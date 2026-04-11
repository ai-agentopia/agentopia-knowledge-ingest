"""HTTP API routes for the Document Ingest Service.

Implements the contract defined in docs/api/ingest-service.yaml.

All upload calls are non-blocking: the caller receives a job_id immediately.
Document processing runs in a background thread via the synchronous pipeline.
"""

import hashlib
import logging
import re
import threading
from typing import Any

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, field_validator

from config import get_settings
from db import registry
from normalizer.base import detect_format
from storage import store

logger = logging.getLogger(__name__)

router = APIRouter()

_SCOPE_RE = re.compile(r"^[a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*$")


# ── Health ────────────────────────────────────────────────────────────────────


@router.get("/health")
async def health() -> dict:
    """Liveness probe — returns ok unconditionally."""
    return {"status": "ok", "service": "knowledge-ingest"}


# ── Scopes ────────────────────────────────────────────────────────────────────


class CreateScopeRequest(BaseModel):
    scope_name: str
    description: str = ""
    owner: str = ""

    @field_validator("scope_name")
    @classmethod
    def validate_scope_name(cls, v: str) -> str:
        if not _SCOPE_RE.match(v):
            raise ValueError(
                f"scope_name must match {{tenant}}/{{domain}} using lowercase letters, "
                f"digits, and hyphens. Got: '{v}'"
            )
        return v


@router.get("/scopes")
async def list_scopes() -> dict:
    """List all registered scopes."""
    try:
        scopes = registry.list_scopes()
        return {"scopes": scopes}
    except RuntimeError as exc:
        _db_unavailable(exc)


@router.post("/scopes", status_code=201)
async def create_scope(body: CreateScopeRequest) -> dict:
    """Register a new scope."""
    try:
        scope = registry.create_scope(
            body.scope_name,
            description=body.description,
            owner=body.owner,
        )
        logger.info("scope created: %s", body.scope_name)
        return scope
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except RuntimeError as exc:
        _db_unavailable(exc)


# ── Upload ────────────────────────────────────────────────────────────────────


@router.post("/documents/upload", status_code=202)
async def upload_document(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    scope: str = Query(..., description="Target scope in {tenant}/{domain} format"),
    owner: str = Query("", description="Operator identifier for audit log"),
) -> dict:
    """Upload a document for ingestion. Returns job_id immediately (non-blocking).

    The document is stored in S3/local immediately. Pipeline stages
    (normalize → extract → index) run in a background thread.
    Poll GET /jobs/{job_id} to track progress.
    """
    # Validate scope
    if not _SCOPE_RE.match(scope):
        raise HTTPException(
            status_code=422,
            detail=f"scope must match {{tenant}}/{{domain}} using lowercase letters, digits, and hyphens",
        )
    try:
        if not registry.scope_exists(scope):
            raise HTTPException(
                status_code=422,
                detail=f"Scope '{scope}' not found. Create it with POST /scopes.",
            )
    except RuntimeError as exc:
        _db_unavailable(exc)

    raw_bytes = await file.read()
    if not raw_bytes:
        raise HTTPException(status_code=422, detail="Uploaded file is empty")

    filename = file.filename or "upload.bin"
    fmt = detect_format(filename)
    source_hash = hashlib.sha256(raw_bytes).hexdigest()

    # Create document record (status=submitted)
    try:
        doc = registry.create_document(
            scope=scope,
            filename=filename,
            format=fmt,
            source_hash=source_hash,
            s3_original_key="",  # updated after S3 write below
            owner=owner,
        )
    except RuntimeError as exc:
        _db_unavailable(exc)

    document_id = doc["document_id"]
    version = doc["version"]

    # Write original to storage
    try:
        s3_original_key = store.write_original(scope, document_id, version, filename, raw_bytes)
        registry.update_document_status(document_id, "submitted",
                                        s3_normalized_key=None, s3_extracted_key=None)
        # Patch s3_original_key into the record (workaround: update via direct SQL in registry)
        _patch_original_key(document_id, s3_original_key)
    except Exception as exc:
        registry.update_document_status(document_id, "failed", error_message=str(exc))
        raise HTTPException(status_code=500, detail=f"Storage write failed: {exc}")

    # Create job record
    job_id = registry.create_job(document_id)

    registry.write_audit_event(
        action="uploaded",
        document_id=document_id,
        job_id=job_id,
        scope=scope,
        actor=owner,
        status="submitted",
        metadata={"filename": filename, "format": fmt, "version": version, "bytes": len(raw_bytes)},
    )

    logger.info(
        "upload accepted: document_id=%s scope=%s version=%d filename=%s job_id=%s",
        document_id, scope, version, filename, job_id,
    )

    # Kick off background pipeline
    background_tasks.add_task(
        _run_pipeline_bg,
        job_id=job_id,
        document_id=document_id,
        scope=scope,
        version=version,
        filename=filename,
        raw_bytes=raw_bytes,
        s3_original_key=s3_original_key,
        actor=owner,
    )

    return {
        "job_id": job_id,
        "document_id": document_id,
        "scope": scope,
        "version": version,
        "skipped": False,
    }


def _run_pipeline_bg(**kwargs) -> None:
    """Wrapper so exceptions in the background task are logged rather than swallowed."""
    from orchestrator import run_pipeline
    try:
        run_pipeline(**kwargs)
    except Exception as exc:
        logger.error("pipeline background task uncaught exception: %s", exc)


def _patch_original_key(document_id: str, s3_original_key: str) -> None:
    """Patch s3_original_key into the documents row after initial creation."""
    from db.connection import transaction
    with transaction() as cur:
        cur.execute(
            "UPDATE documents SET s3_original_key = %s WHERE document_id = %s",
            (s3_original_key, document_id),
        )


# ── Job status ────────────────────────────────────────────────────────────────


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str) -> dict:
    """Return current ingest job status."""
    try:
        job = registry.get_job(job_id)
    except RuntimeError as exc:
        _db_unavailable(exc)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return job


# ── Documents ─────────────────────────────────────────────────────────────────


@router.get("/documents")
async def list_documents(
    scope: str = Query(...),
    status: str = Query("active", enum=["active", "superseded", "deleted", "failed", "all"]),
) -> dict:
    """List documents in a scope."""
    try:
        if not registry.scope_exists(scope):
            raise HTTPException(status_code=422, detail=f"Scope '{scope}' not found")
        docs = registry.list_documents(scope, status=status)
        return {"documents": docs}
    except HTTPException:
        raise
    except RuntimeError as exc:
        _db_unavailable(exc)


@router.get("/documents/{document_id}/versions")
async def get_document_versions(document_id: str) -> dict:
    """Return all versions of a document."""
    try:
        versions = registry.get_document_versions(document_id)
    except RuntimeError as exc:
        _db_unavailable(exc)
    if not versions:
        raise HTTPException(status_code=404, detail=f"Document '{document_id}' not found")
    first = versions[0]
    return {
        "document_id": document_id,
        "scope": first["scope"],
        "filename": first["filename"],
        "versions": [
            {
                "version": v["version"],
                "status": v["status"],
                "source_hash": v["source_hash"],
                "created_at": v["created_at"],
            }
            for v in versions
        ],
    }


class RollbackRequest(BaseModel):
    version: int
    reason: str = ""


@router.post("/documents/{document_id}/rollback", status_code=202)
async def rollback_document(document_id: str, body: RollbackRequest) -> dict:
    """Restore a prior version to active; supersede current active version."""
    try:
        result = registry.rollback_to_version(document_id, body.version)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        _db_unavailable(exc)

    registry.write_audit_event(
        action="rolled_back",
        document_id=document_id,
        scope=None,
        status="success",
        metadata={
            "restored_version": result["restored_version"],
            "superseded_version": result["superseded_version"],
            "reason": body.reason,
        },
    )
    logger.info(
        "rollback: document_id=%s restored_version=%d superseded_version=%s",
        document_id, result["restored_version"], result["superseded_version"],
    )
    return result


# ── Operator UI ───────────────────────────────────────────────────────────────


@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def operator_ui() -> str:
    """Basic operator UI: upload form and job status poller."""
    return _OPERATOR_UI_HTML


_OPERATOR_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Agentopia Knowledge Ingest</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; }
    h1 { font-size: 1.4rem; border-bottom: 1px solid #ddd; padding-bottom: 8px; }
    h2 { font-size: 1.1rem; margin-top: 32px; }
    label { display: block; margin-bottom: 4px; font-weight: 500; font-size: 0.9rem; }
    input, select { width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px;
                    font-size: 0.9rem; margin-bottom: 12px; box-sizing: border-box; }
    button { background: #1a56db; color: white; border: none; padding: 10px 20px;
             border-radius: 4px; cursor: pointer; font-size: 0.9rem; }
    button:hover { background: #1e429f; }
    #status-box { background: #f4f4f4; padding: 16px; border-radius: 6px;
                  font-family: monospace; font-size: 0.85rem; white-space: pre-wrap;
                  min-height: 60px; margin-top: 12px; }
    .label-active   { color: #0f5132; font-weight: bold; }
    .label-failed   { color: #842029; font-weight: bold; }
    .label-indexing { color: #084298; }
    .label-other    { color: #555; }
    #job-poll { margin-top: 24px; }
    #upload-result { margin-top: 12px; font-size: 0.85rem; }
  </style>
</head>
<body>
  <h1>Agentopia — Knowledge Ingest</h1>

  <h2>Upload Document</h2>
  <form id="upload-form">
    <label for="scope">Scope (e.g. joblogic-kb/api-docs)</label>
    <input type="text" id="scope" placeholder="tenant/domain" required>

    <label for="owner">Owner (optional)</label>
    <input type="text" id="owner" placeholder="operator@example.com">

    <label for="file">Document (PDF, DOCX, HTML, Markdown)</label>
    <input type="file" id="file" accept=".pdf,.docx,.doc,.html,.htm,.md,.markdown,.txt" required>

    <button type="submit">Upload</button>
  </form>
  <div id="upload-result"></div>

  <h2>Job Status</h2>
  <div id="job-poll">
    <label for="job-id">Job ID</label>
    <input type="text" id="job-id" placeholder="Paste job_id here">
    <button onclick="pollJob()">Check Status</button>
    <div id="status-box">Status will appear here.</div>
  </div>

  <script>
    const uploadForm = document.getElementById('upload-form');
    const uploadResult = document.getElementById('upload-result');
    const jobIdInput = document.getElementById('job-id');
    const statusBox = document.getElementById('status-box');
    let pollInterval = null;

    uploadForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const scope = document.getElementById('scope').value.trim();
      const owner = document.getElementById('owner').value.trim();
      const file = document.getElementById('file').files[0];
      if (!file) return;

      const fd = new FormData();
      fd.append('file', file);

      uploadResult.textContent = 'Uploading...';
      try {
        const resp = await fetch(
          `/documents/upload?scope=${encodeURIComponent(scope)}&owner=${encodeURIComponent(owner)}`,
          { method: 'POST', body: fd }
        );
        const data = await resp.json();
        if (!resp.ok) {
          uploadResult.textContent = 'Error: ' + (data.detail || resp.statusText);
          return;
        }
        uploadResult.textContent =
          `Accepted. job_id: ${data.job_id}  document_id: ${data.document_id}  version: ${data.version}`;
        jobIdInput.value = data.job_id;
        startPolling(data.job_id);
      } catch (err) {
        uploadResult.textContent = 'Request failed: ' + err;
      }
    });

    async function pollJob() {
      const jobId = jobIdInput.value.trim();
      if (!jobId) return;
      startPolling(jobId);
    }

    function startPolling(jobId) {
      if (pollInterval) clearInterval(pollInterval);
      fetchJobStatus(jobId);
      pollInterval = setInterval(() => fetchJobStatus(jobId), 3000);
    }

    async function fetchJobStatus(jobId) {
      try {
        const resp = await fetch(`/jobs/${jobId}`);
        const data = await resp.json();
        if (!resp.ok) {
          statusBox.textContent = 'Error: ' + (data.detail || resp.statusText);
          clearInterval(pollInterval);
          return;
        }
        const status = data.status || '';
        let cls = 'label-other';
        if (status === 'active') cls = 'label-active';
        else if (status === 'failed') cls = 'label-failed';
        else if (status === 'indexing') cls = 'label-indexing';

        statusBox.innerHTML =
          `<span class="${cls}">${status.toUpperCase()}</span>  (${data.progress_percent ?? 0}%)\n` +
          `scope: ${data.scope}   version: ${data.version}\n` +
          `stage: ${data.stage || '-'}\n` +
          (data.error_message ? `error: ${data.error_message}\n` : '') +
          `\nupdated: ${data.updated_at || '-'}`;

        if (status === 'active' || status === 'failed') {
          clearInterval(pollInterval);
        }
      } catch (err) {
        statusBox.textContent = 'Poll failed: ' + err;
        clearInterval(pollInterval);
      }
    }
  </script>
</body>
</html>
"""


# ── Error helpers ─────────────────────────────────────────────────────────────


def _db_unavailable(exc: Exception):
    logger.error("database unavailable: %s", exc)
    raise HTTPException(
        status_code=503,
        detail="Database unavailable. Set DATABASE_URL to a valid PostgreSQL connection string.",
    )
