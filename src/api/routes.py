"""HTTP API routes for the Document Ingest Service.

Implements the contract defined in docs/api/ingest-service.yaml.

All upload calls are non-blocking: the caller receives a job_id immediately.
Document processing runs in FastAPI BackgroundTasks (synchronous pipeline day-1).
"""

import hashlib
import logging
import re
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
    try:
        return {"scopes": registry.list_scopes()}
    except RuntimeError as exc:
        _db_unavailable(exc)


@router.post("/scopes", status_code=201)
async def create_scope(body: CreateScopeRequest) -> dict:
    try:
        scope = registry.create_scope(body.scope_name, description=body.description, owner=body.owner)
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
    scope: str = Query(...),
    owner: str = Query(""),
) -> dict:
    """Upload a document. Returns {job_id, document_id, version} immediately (non-blocking)."""
    if not _SCOPE_RE.match(scope):
        raise HTTPException(status_code=422, detail="scope must match {tenant}/{domain}")

    try:
        if not registry.scope_exists(scope):
            raise HTTPException(
                status_code=422,
                detail=f"Scope '{scope}' not found. Create it with POST /scopes.",
            )
    except HTTPException:
        raise
    except RuntimeError as exc:
        _db_unavailable(exc)

    raw_bytes = await file.read()
    if not raw_bytes:
        raise HTTPException(status_code=422, detail="Uploaded file is empty")

    filename = file.filename or "upload.bin"
    fmt = detect_format(filename)
    source_hash = hashlib.sha256(raw_bytes).hexdigest()

    # Create document version row (status=submitted)
    # document_id is stable for (scope, filename); version increments
    try:
        doc = registry.create_document(
            scope=scope,
            filename=filename,
            format=fmt,
            source_hash=source_hash,
            s3_original_key="",   # patched after S3 write below
            owner=owner,
        )
    except RuntimeError as exc:
        _db_unavailable(exc)

    row_id = doc["row_id"]
    document_id = doc["document_id"]
    version = doc["version"]

    # Write original to storage
    try:
        s3_original_key = store.write_original(scope, document_id, version, filename, raw_bytes)
        registry.patch_s3_original_key(row_id, s3_original_key)
    except Exception as exc:
        registry.update_document_status(row_id, "failed", error_message=str(exc))
        raise HTTPException(status_code=500, detail=f"Storage write failed: {exc}")

    job_id = registry.create_job(row_id)

    registry.write_audit_event(
        action="uploaded",
        document_id=document_id,
        document_version=version,
        job_id=job_id,
        scope=scope,
        actor=owner,
        status="submitted",
        metadata={"filename": filename, "format": fmt, "bytes": len(raw_bytes)},
    )

    logger.info(
        "upload: doc=%s v%d scope=%s filename=%s job=%s",
        document_id, version, scope, filename, job_id,
    )

    background_tasks.add_task(
        _run_pipeline_bg,
        job_id=job_id,
        row_id=row_id,
        document_id=document_id,
        version=version,
        scope=scope,
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
    from orchestrator import run_pipeline
    try:
        run_pipeline(**kwargs)
    except Exception as exc:
        logger.error("pipeline background task uncaught exception: %s", exc)


# ── Job status ────────────────────────────────────────────────────────────────


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str) -> dict:
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
    status: str = Query("active"),
) -> dict:
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
    try:
        result = registry.rollback_to_version(document_id, body.version)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        _db_unavailable(exc)

    registry.write_audit_event(
        action="rolled_back",
        document_id=document_id,
        document_version=body.version,
        status="success",
        metadata={
            "restored_version": result["restored_version"],
            "superseded_version": result["superseded_version"],
            "reason": body.reason,
        },
    )
    return result


# ── Operator UI ───────────────────────────────────────────────────────────────


@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def operator_ui() -> str:
    return _OPERATOR_UI_HTML


_OPERATOR_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Agentopia Knowledge Ingest — Operator</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 1000px; margin: 0 auto; padding: 20px; background: #f8f9fa; }
    h1 { font-size: 1.3rem; border-bottom: 2px solid #1a56db; padding-bottom: 8px; margin-bottom: 0; color: #1a56db; }
    h2 { font-size: 1.0rem; margin-top: 0; padding: 10px 16px; cursor: pointer; background: #fff; border: 1px solid #ddd; border-radius: 6px 6px 0 0; display: flex; justify-content: space-between; }
    h2.open { background: #1a56db; color: #fff; }
    .section { margin: 12px 0; }
    .section-body { display: none; background: #fff; border: 1px solid #ddd; border-top: none; border-radius: 0 0 6px 6px; padding: 16px; }
    .section-body.visible { display: block; }
    label { display: block; margin-bottom: 3px; font-weight: 500; font-size: 0.85rem; color: #333; }
    input, select, textarea { width: 100%; padding: 7px 10px; border: 1px solid #ccc; border-radius: 4px;
                    font-size: 0.85rem; margin-bottom: 10px; box-sizing: border-box; font-family: inherit; }
    textarea { height: 80px; resize: vertical; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    button { background: #1a56db; color: white; border: none; padding: 8px 16px;
             border-radius: 4px; cursor: pointer; font-size: 0.85rem; margin-right: 6px; }
    button:hover { background: #1e429f; }
    button.danger { background: #dc3545; }
    button.danger:hover { background: #b02a37; }
    button.secondary { background: #6c757d; }
    button.secondary:hover { background: #5a6268; }
    .output { background: #f4f4f4; padding: 12px; border-radius: 6px;
              font-family: monospace; font-size: 0.8rem; white-space: pre-wrap;
              min-height: 40px; margin-top: 8px; max-height: 300px; overflow-y: auto; word-break: break-all; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 0.75rem; font-weight: 600; }
    .badge.active   { background: #d4edda; color: #155724; }
    .badge.failed   { background: #f8d7da; color: #721c24; }
    .badge.indexing { background: #cce5ff; color: #004085; }
    .badge.other    { background: #e2e3e5; color: #383d41; }
    table { width: 100%; border-collapse: collapse; font-size: 0.82rem; margin-top: 8px; }
    th { background: #f0f0f0; padding: 6px 10px; text-align: left; border-bottom: 2px solid #ddd; }
    td { padding: 6px 10px; border-bottom: 1px solid #eee; }
    tr:hover td { background: #fafafa; }
    .verdict.passed  { color: #155724; font-weight: 600; }
    .verdict.blocked { color: #721c24; font-weight: 600; }
    .verdict.warning { color: #856404; font-weight: 600; }
    .actions { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 8px; }
  </style>
</head>
<body>
  <h1>Agentopia Knowledge Ingest &mdash; Operator Console</h1>

  <!-- ── Upload ──────────────────────────────────────────────────── -->
  <div class="section">
    <h2 id="hUpload" onclick="toggle('sUpload','hUpload')">&#x2795; Upload Document</h2>
    <div id="sUpload" class="section-body">
      <div class="row">
        <div>
          <label>Scope (tenant/domain)</label>
          <input id="upScope" placeholder="joblogic-kb/api-docs">
          <label>Owner (optional)</label>
          <input id="upOwner" placeholder="operator@example.com">
          <label>Document (PDF, DOCX, HTML, Markdown)</label>
          <input type="file" id="upFile" accept=".pdf,.docx,.html,.htm,.md,.markdown,.txt">
          <button onclick="doUpload()">Upload</button>
        </div>
        <div>
          <label>Job ID (auto-filled on upload)</label>
          <input id="upJobId" placeholder="Paste job_id to check status">
          <button onclick="pollJob()">Check Status</button>
          <div id="upStatus" class="output">Ready.</div>
        </div>
      </div>
    </div>
  </div>

  <!-- ── Documents ──────────────────────────────────────────────── -->
  <div class="section">
    <h2 id="hDocs" onclick="toggle('sDocs','hDocs')">&#x1F4C4; Documents</h2>
    <div id="sDocs" class="section-body">
      <div style="display:flex;gap:8px;align-items:flex-end;margin-bottom:10px">
        <div style="flex:1"><label>Scope</label><input id="docScope" placeholder="joblogic-kb/api-docs"></div>
        <div><label>Status</label>
          <select id="docStatus">
            <option value="active">active</option>
            <option value="superseded">superseded</option>
            <option value="failed">failed</option>
            <option value="all">all</option>
          </select>
        </div>
        <div><button onclick="listDocs()">List</button></div>
      </div>
      <div id="docTable"></div>
    </div>
  </div>

  <!-- ── Version History ────────────────────────────────────────── -->
  <div class="section">
    <h2 id="hVer" onclick="toggle('sVer','hVer')">&#x1F4CB; Version History &amp; Rollback</h2>
    <div id="sVer" class="section-body">
      <div style="display:flex;gap:8px;align-items:flex-end;margin-bottom:10px">
        <div style="flex:1"><label>Document ID</label><input id="verDocId" placeholder="uuid"></div>
        <button onclick="listVersions()">Load Versions</button>
      </div>
      <div id="verTable"></div>
      <div id="rollbackSection" style="display:none;margin-top:12px">
        <div style="display:flex;gap:8px;align-items:flex-end">
          <div><label>Restore version</label><input id="rollbackVer" type="number" min="1" style="width:80px"></div>
          <div style="flex:1"><label>Reason (optional)</label><input id="rollbackReason" placeholder="Prior version more accurate"></div>
          <button class="danger" onclick="doRollback()">Rollback</button>
        </div>
        <div id="rollbackOut" class="output" style="display:none"></div>
      </div>
    </div>
  </div>

  <!-- ── Scopes ─────────────────────────────────────────────────── -->
  <div class="section">
    <h2 id="hScopes" onclick="toggle('sScopes','hScopes')">&#x1F5C2; Scopes</h2>
    <div id="sScopes" class="section-body">
      <div style="display:flex;gap:8px;align-items:flex-end;margin-bottom:10px">
        <button onclick="listScopes()">List All Scopes</button>
        <span style="flex:1"></span>
        <input id="newScope" placeholder="tenant/domain" style="width:200px">
        <input id="newScopeOwner" placeholder="owner (optional)" style="width:160px">
        <button onclick="createScope()">Create Scope</button>
      </div>
      <div id="scopeTable"></div>
      <div id="scopeOut" class="output" style="display:none"></div>
    </div>
  </div>

  <!-- ── Quality Dashboard ─────────────────────────────────────── -->
  <div class="section">
    <h2 id="hQual" onclick="toggle('sQual','hQual')">&#x1F4CA; Quality Dashboard</h2>
    <div id="sQual" class="section-body">
      <p style="font-size:0.82rem;color:#555;margin-top:0">
        Reads from Super RAG. Enter the Super RAG URL and token below (shared with Retrieval Debugger).
        Auth or backend failures are shown as explicit errors, not empty data.
      </p>

      <!-- Baselines -->
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">
        <button onclick="loadBaselines()">Load All Baselines</button>
        <span id="baselineErr" style="color:#842029;font-size:0.82rem;display:none"></span>
      </div>
      <div id="baselineTable"></div>

      <!-- Regression history for one scope -->
      <div style="margin-top:20px;border-top:1px solid #e0e0e0;padding-top:14px">
        <div style="display:flex;gap:8px;align-items:flex-end;margin-bottom:8px">
          <div style="flex:1">
            <label>Scope — regression history</label>
            <input id="bsScope" placeholder="joblogic-kb/api-docs">
          </div>
          <button onclick="loadResults()" class="secondary">Load History</button>
          <span id="resultsErr" style="color:#842029;font-size:0.82rem;display:none"></span>
        </div>
        <!-- Trend summary: latest nDCG, delta, blocked count -->
        <div id="trendSummary" style="display:none;margin-bottom:10px;padding:10px;background:#f8f9fa;border-radius:6px;font-size:0.82rem"></div>
        <div id="resultsTable"></div>
      </div>
    </div>
  </div>

  <!-- ── Retrieval Debugger ─────────────────────────────────────── -->
  <div class="section">
    <h2 id="hDebug" onclick="toggle('sDebug','hDebug')">&#x1F50D; Retrieval Debugger</h2>
    <div id="sDebug" class="section-body">
      <p style="font-size:0.82rem;color:#555;margin-top:0">
        Requires Super RAG URL and internal token. Results show ranked chunks with text, section path, score, and document lineage.
      </p>
      <div class="row">
        <div>
          <label>Super RAG URL</label>
          <input id="dbgUrl" value="http://localhost:8002">
          <label>Internal Token</label>
          <input id="dbgToken" type="password" placeholder="SUPER_RAG_INTERNAL_TOKEN">
        </div>
        <div>
          <label>Scope</label>
          <input id="dbgScope" placeholder="joblogic-kb/api-docs">
          <label>Query</label>
          <input id="dbgQuery" placeholder="How do I authenticate?">
          <button onclick="doDebug()">Run Query</button>
        </div>
      </div>
      <div id="debugTable" style="margin-top:8px"></div>
    </div>
  </div>

  <!-- ── AWS S3 Sync Trigger (W-C3.5) ──────────────────────────────────── -->
  <div class="section">
    <h2 id="hS3" onclick="toggle('sS3','hS3')">&#x1F5C4; AWS S3 Sync</h2>
    <div id="sS3" class="section-body">
      <p style="font-size:0.82rem;color:#555;margin-top:0">
        Triggers a server-side pull of an S3 bucket into a knowledge scope.
        Credentials are resolved server-side from environment variables — do not enter access keys here.
        Set <code>AWS_ACCESS_KEY_ID</code> / <code>AWS_SECRET_ACCESS_KEY</code> (default),
        or <code>S3_SECRET_&lt;REF&gt;_ACCESS_KEY</code> / <code>S3_SECRET_&lt;REF&gt;_SECRET_KEY</code>
        for a named reference.
      </p>
      <div class="row">
        <div>
          <label>Scope (tenant/domain)</label>
          <input id="s3Scope" placeholder="joblogic-kb/s3-docs">
          <label>Bucket</label>
          <input id="s3Bucket" placeholder="my-docs-bucket">
          <label>Secret Ref (leave blank for default AWS env vars)</label>
          <input id="s3SecretRef" placeholder="default">
        </div>
        <div>
          <label>Prefix (optional key filter, e.g. docs/)</label>
          <input id="s3Prefix" placeholder="">
          <label>Region (optional, default us-east-1)</label>
          <input id="s3Region" placeholder="us-east-1">
          <label>Endpoint URL (optional, for MinIO / R2)</label>
          <input id="s3Endpoint" placeholder="">
          <label>Owner (optional)</label>
          <input id="s3Owner" placeholder="operator@example.com">
        </div>
      </div>
      <div class="actions">
        <button onclick="doS3Sync()">Run S3 Sync</button>
      </div>
      <div id="s3Out" class="output" style="margin-top:8px">Ready.</div>
      <div id="s3Table" style="margin-top:8px"></div>
    </div>
  </div>

  <script>
  // ── Helpers ────────────────────────────────────────────────────────────────

  function toggle(bodyId, headId) {
    const b = document.getElementById(bodyId);
    const h = document.getElementById(headId);
    const open = b.classList.toggle('visible');
    h.classList.toggle('open', open);
    h.textContent = (open ? '▼ ' : '▶ ') + h.textContent.replace(/^[▼▶] /, '');
  }

  function badgeHtml(status) {
    const cls = ['active','failed','indexing'].includes(status) ? status : 'other';
    return `<span class="badge ${cls}">${status}</span>`;
  }

  function isoDate(ts) {
    if (!ts) return '-';
    return ts.replace('T', ' ').slice(0, 16);
  }

  async function api(path, opts = {}) {
    const resp = await fetch(path, opts);
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ detail: resp.statusText }));
      throw new Error(err.detail || resp.statusText);
    }
    return resp.json();
  }

  let pollInterval = null;

  // ── Upload ─────────────────────────────────────────────────────────────────

  async function doUpload() {
    const scope = document.getElementById('upScope').value.trim();
    const owner = document.getElementById('upOwner').value.trim();
    const file = document.getElementById('upFile').files[0];
    const statusEl = document.getElementById('upStatus');
    if (!scope || !file) { statusEl.textContent = 'Scope and file are required.'; return; }
    const fd = new FormData();
    fd.append('file', file);
    statusEl.textContent = 'Uploading...';
    try {
      const data = await fetch(
        `/documents/upload?scope=${encodeURIComponent(scope)}&owner=${encodeURIComponent(owner)}`,
        { method: 'POST', body: fd }
      ).then(r => { if (!r.ok) throw new Error(r.statusText); return r.json(); });
      document.getElementById('upJobId').value = data.job_id;
      statusEl.textContent = `Accepted. job_id: ${data.job_id}\\ndocument_id: ${data.document_id}  v${data.version}`;
      startPolling(data.job_id);
    } catch (err) { statusEl.textContent = 'Error: ' + err.message; }
  }

  function pollJob() {
    const j = document.getElementById('upJobId').value.trim();
    if (j) startPolling(j);
  }

  function startPolling(jobId) {
    if (pollInterval) clearInterval(pollInterval);
    fetchStatus(jobId);
    pollInterval = setInterval(() => fetchStatus(jobId), 3000);
  }

  async function fetchStatus(jobId) {
    const el = document.getElementById('upStatus');
    try {
      const d = await api('/jobs/' + jobId);
      const s = d.status || '';
      el.textContent =
        `${s.toUpperCase()} (${d.progress_percent ?? 0}%)\\n` +
        `scope: ${d.scope}  v${d.version}  stage: ${d.stage || '-'}\\n` +
        (d.error_message ? `error: ${d.error_message}\\n` : '') +
        `updated: ${d.updated_at || '-'}`;
      if (s === 'active' || s === 'failed') clearInterval(pollInterval);
    } catch (e) { clearInterval(pollInterval); }
  }

  // ── Documents ──────────────────────────────────────────────────────────────

  async function listDocs() {
    const scope = document.getElementById('docScope').value.trim();
    const status = document.getElementById('docStatus').value;
    const el = document.getElementById('docTable');
    if (!scope) { el.innerHTML = '<em>Enter a scope.</em>'; return; }
    try {
      const d = await api('/documents?scope=' + encodeURIComponent(scope) + '&status=' + status);
      const docs = d.documents || [];
      if (!docs.length) { el.innerHTML = '<em>No documents found.</em>'; return; }
      el.innerHTML = '<table><thead><tr><th>Filename</th><th>Format</th><th>Version</th><th>Status</th><th>Updated</th><th>Actions</th></tr></thead><tbody>' +
        docs.map(doc => `<tr>
          <td style="font-family:monospace;font-size:0.78rem">${doc.filename}</td>
          <td>${doc.format}</td>
          <td>v${doc.version}</td>
          <td>${badgeHtml(doc.status)}</td>
          <td>${isoDate(doc.updated_at)}</td>
          <td><button class="secondary" onclick="loadVersionsById('${doc.document_id}')">Versions</button></td>
        </tr>`).join('') + '</tbody></table>';
    } catch (e) { el.innerHTML = '<em>Error: ' + e.message + '</em>'; }
  }

  // ── Version History ────────────────────────────────────────────────────────

  function loadVersionsById(docId) {
    document.getElementById('verDocId').value = docId;
    // open section
    const b = document.getElementById('sVer');
    if (!b.classList.contains('visible')) toggle('sVer', 'hVer');
    listVersions();
  }

  async function listVersions() {
    const docId = document.getElementById('verDocId').value.trim();
    const el = document.getElementById('verTable');
    const rs = document.getElementById('rollbackSection');
    if (!docId) { el.innerHTML = '<em>Enter a document ID.</em>'; return; }
    try {
      const d = await api('/documents/' + docId + '/versions');
      const vs = d.versions || [];
      if (!vs.length) { el.innerHTML = '<em>No versions found.</em>'; return; }
      el.innerHTML = '<table><thead><tr><th>Version</th><th>Status</th><th>Source Hash</th><th>Created</th></tr></thead><tbody>' +
        vs.map(v => `<tr>
          <td>v${v.version}</td>
          <td>${badgeHtml(v.status)}</td>
          <td style="font-family:monospace;font-size:0.72rem">${(v.source_hash||'').slice(0,16)}…</td>
          <td>${isoDate(v.created_at)}</td>
        </tr>`).join('') + '</tbody></table>';
      rs.style.display = vs.length > 1 ? 'block' : 'none';
    } catch (e) { el.innerHTML = '<em>Error: ' + e.message + '</em>'; }
  }

  async function doRollback() {
    const docId = document.getElementById('verDocId').value.trim();
    const version = parseInt(document.getElementById('rollbackVer').value);
    const reason = document.getElementById('rollbackReason').value;
    const out = document.getElementById('rollbackOut');
    if (!docId || !version) { out.textContent = 'Document ID and version required.'; out.style.display = 'block'; return; }
    if (!confirm('Roll back document to v' + version + '? Current active version will be superseded.')) return;
    try {
      const d = await api('/documents/' + docId + '/rollback', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ version, reason })
      });
      out.textContent = 'Rollback complete.\\nRestored: v' + d.restored_version + '\\nSuperseded: v' + d.superseded_version;
      out.style.display = 'block';
      listVersions();
    } catch (e) { out.textContent = 'Error: ' + e.message; out.style.display = 'block'; }
  }

  // ── Scopes ─────────────────────────────────────────────────────────────────

  async function listScopes() {
    const el = document.getElementById('scopeTable');
    try {
      const d = await api('/scopes');
      const scopes = d.scopes || [];
      if (!scopes.length) { el.innerHTML = '<em>No scopes found.</em>'; return; }
      el.innerHTML = '<table><thead><tr><th>Scope</th><th>Owner</th><th>Docs</th><th>Created</th></tr></thead><tbody>' +
        scopes.map(s => `<tr>
          <td style="font-family:monospace">${s.scope_name}</td>
          <td>${s.owner || '-'}</td>
          <td>${s.document_count ?? 0}</td>
          <td>${isoDate(s.created_at)}</td>
        </tr>`).join('') + '</tbody></table>';
    } catch (e) { el.innerHTML = '<em>Error: ' + e.message + '</em>'; }
  }

  async function createScope() {
    const name = document.getElementById('newScope').value.trim();
    const owner = document.getElementById('newScopeOwner').value.trim();
    const out = document.getElementById('scopeOut');
    if (!name) { out.textContent = 'Scope name required.'; out.style.display = 'block'; return; }
    try {
      const d = await api('/scopes', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ scope_name: name, owner })
      });
      out.textContent = 'Created: ' + d.scope_name + ' (id: ' + d.scope_id + ')';
      out.style.display = 'block';
      listScopes();
    } catch (e) { out.textContent = 'Error: ' + e.message; out.style.display = 'block'; }
  }

  // ── Quality Dashboard ──────────────────────────────────────────────────────
  // Finding 1 fix: both functions check resp.ok and surface auth/backend errors
  // explicitly to the operator instead of silently showing empty data.

  function superRagConn() {
    return {
      url: (document.getElementById('dbgUrl').value || '').trim() || 'http://localhost:8002',
      token: (document.getElementById('dbgToken').value || '').trim(),
    };
  }

  async function superRagFetch(path) {
    const { url, token } = superRagConn();
    if (!token) throw new Error('Super RAG Internal Token is required. Enter it in the Retrieval Debugger section.');
    const resp = await fetch(url + path, { headers: { 'X-Internal-Token': token } });
    if (!resp.ok) {
      let detail = resp.statusText;
      try { const body = await resp.json(); detail = body.detail || detail; } catch {}
      throw new Error('HTTP ' + resp.status + ' from Super RAG: ' + detail);
    }
    return resp.json();
  }

  async function loadBaselines() {
    const el = document.getElementById('baselineTable');
    const errEl = document.getElementById('baselineErr');
    errEl.style.display = 'none';
    el.innerHTML = '<em>Loading…</em>';
    try {
      const d = await superRagFetch('/api/v1/evaluation/baselines');
      const bs = d.baselines || [];
      if (!bs.length) {
        el.innerHTML = '<em>No baselines established yet. ' +
          'Run <code>POST /api/v1/evaluation/baselines/{scope}</code> in Super RAG to create one.</em>';
        return;
      }
      el.innerHTML = '<table data-testid="baseline-table"><thead><tr>' +
        '<th>Scope</th><th>nDCG@5</th><th>MRR</th><th>P@5</th><th>R@5</th><th>Questions</th><th>Established</th></tr></thead><tbody>' +
        bs.map(b => `<tr>
          <td style="font-family:monospace;font-size:0.78rem">${b.scope}</td>
          <td>${(b.ndcg_5||0).toFixed(4)}</td>
          <td>${(b.mrr||0).toFixed(4)}</td>
          <td>${(b.p_5||0).toFixed(4)}</td>
          <td>${(b.r_5||0).toFixed(4)}</td>
          <td>${b.golden_question_count}</td>
          <td>${isoDate(b.established_at)}</td>
        </tr>`).join('') + '</tbody></table>';
    } catch (e) {
      el.innerHTML = '';
      errEl.textContent = 'Error: ' + e.message;
      errEl.style.display = 'inline';
    }
  }

  async function loadResults() {
    const scope = (document.getElementById('bsScope').value || '').trim();
    const el = document.getElementById('resultsTable');
    const summaryEl = document.getElementById('trendSummary');
    const errEl = document.getElementById('resultsErr');
    errEl.style.display = 'none';
    summaryEl.style.display = 'none';
    if (!scope) { el.innerHTML = '<em>Enter a scope name.</em>'; return; }
    el.innerHTML = '<em>Loading…</em>';
    try {
      const d = await superRagFetch('/api/v1/evaluation/results?scope=' + encodeURIComponent(scope));
      const rs = d.results || [];
      if (!rs.length) {
        el.innerHTML = '<em>No evaluation results for scope <strong>' + scope + '</strong>.<br>' +
          'Results are created automatically on document replacement once a baseline and golden questions exist.</em>';
        return;
      }

      // ── Trend summary (Finding 2) ──────────────────────────────────────────
      // Compute: latest nDCG, min/max delta, blocked count, trend direction
      const withNdcg = rs.filter(r => r.ndcg_5 != null);
      const latest = withNdcg[0];
      const blockedCount = rs.filter(r => r.verdict === 'blocked').length;
      const overrideCount = rs.filter(r => r.operator_override).length;
      const deltas = rs.filter(r => r.delta_ndcg_5 != null).map(r => r.delta_ndcg_5);
      const minDelta = deltas.length ? Math.min(...deltas) : null;
      const maxDelta = deltas.length ? Math.max(...deltas) : null;

      // Trend: compare first and last nDCG with data
      let trendText = '';
      if (withNdcg.length >= 2) {
        const oldest = withNdcg[withNdcg.length - 1];
        const trendDelta = latest.ndcg_5 - oldest.ndcg_5;
        const arrow = trendDelta >= 0.005 ? '↑' : trendDelta <= -0.005 ? '↓' : '→';
        trendText = `Trend vs oldest (${withNdcg.length} runs): ${arrow} ${trendDelta >= 0 ? '+' : ''}${trendDelta.toFixed(4)}`;
      }

      summaryEl.innerHTML =
        '<strong>Scope:</strong> ' + scope + ' &nbsp;|&nbsp; ' +
        '<strong>Runs:</strong> ' + rs.length + ' &nbsp;|&nbsp; ' +
        (latest ? '<strong>Latest nDCG@5:</strong> ' + latest.ndcg_5.toFixed(4) + ' &nbsp;|&nbsp; ' : '') +
        (minDelta != null ? '<strong>Delta range:</strong> ' + (minDelta >= 0 ? '+' : '') + minDelta.toFixed(4) +
          ' to ' + (maxDelta >= 0 ? '+' : '') + maxDelta.toFixed(4) + ' &nbsp;|&nbsp; ' : '') +
        '<strong style="color:' + (blockedCount > 0 ? '#721c24' : '#155724') + '">Blocked:</strong> ' + blockedCount +
        (overrideCount > 0 ? ' <span style="color:#155724">(' + overrideCount + ' overridden)</span>' : '') +
        (trendText ? ' &nbsp;|&nbsp; ' + trendText : '');
      summaryEl.style.display = 'block';

      // ── Full results table ─────────────────────────────────────────────────
      el.innerHTML = '<table data-testid="results-table"><thead><tr>' +
        '<th>Run At</th><th>Trigger</th><th>nDCG@5</th><th>MRR</th><th>Delta</th><th>Verdict</th><th>Override</th>' +
        '</tr></thead><tbody>' +
        rs.map(r => {
          const vc = r.verdict === 'passed' ? 'passed' : r.verdict === 'blocked' || r.verdict === 'eval_error' ? 'blocked' : 'warning';
          const delta = r.delta_ndcg_5 != null ? (r.delta_ndcg_5 >= 0 ? '+' : '') + r.delta_ndcg_5.toFixed(4) : '-';
          const override = r.operator_override ? '✓ ' + (r.operator_identity || '(no id)') : '-';
          return `<tr>
            <td>${isoDate(r.run_at)}</td>
            <td>${r.trigger}</td>
            <td>${r.ndcg_5 != null ? r.ndcg_5.toFixed(4) : '-'}</td>
            <td>${r.mrr != null ? r.mrr.toFixed(4) : '-'}</td>
            <td>${delta}</td>
            <td><span class="verdict ${vc}">${r.verdict}</span></td>
            <td style="font-size:0.78rem">${override}</td>
          </tr>`;
        }).join('') + '</tbody></table>';
    } catch (e) {
      el.innerHTML = '';
      errEl.textContent = 'Error: ' + e.message;
      errEl.style.display = 'inline';
    }
  }

  // ── Retrieval Debugger ─────────────────────────────────────────────────────

  async function doDebug() {
    const url = document.getElementById('dbgUrl').value.trim();
    const token = document.getElementById('dbgToken').value;
    const scope = document.getElementById('dbgScope').value.trim();
    const q = document.getElementById('dbgQuery').value.trim();
    const el = document.getElementById('debugTable');
    if (!url || !scope || !q) { el.innerHTML = '<em>URL, scope, and query are required.</em>'; return; }
    try {
      const resp = await fetch(
        url + '/api/v1/knowledge/debug/query?scope=' + encodeURIComponent(scope) +
        '&q=' + encodeURIComponent(q) + '&limit=10',
        { headers: { 'X-Internal-Token': token } }
      );
      if (!resp.ok) { const e = await resp.json(); throw new Error(e.detail || resp.statusText); }
      const d = await resp.json();
      const results = d.results || [];
      if (!results.length) { el.innerHTML = '<em>No results returned.</em>'; return; }
      el.innerHTML = '<table><thead><tr><th>#</th><th>Score</th><th>Source</th><th>Section</th><th>Chunk</th><th>Text (preview)</th></tr></thead><tbody>' +
        results.map(r => `<tr>
          <td>${r.rank}</td>
          <td style="font-family:monospace">${r.score.toFixed(4)}</td>
          <td style="font-family:monospace;font-size:0.75rem;max-width:160px;overflow:hidden;text-overflow:ellipsis">${r.source}</td>
          <td style="font-size:0.78rem;max-width:140px">${r.section_path || r.section || '-'}</td>
          <td>${r.chunk_index}</td>
          <td style="font-size:0.78rem;max-width:260px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${r.text.replace(/"/g,'&quot;')}">${r.text.slice(0,120)}${r.text.length>120?'…':''}</td>
        </tr>`).join('') + '</tbody></table>';
    } catch (e) { el.innerHTML = '<em>Error: ' + e.message + '</em>'; }
  }

  // ── AWS S3 Sync (W-C3.5) ──────────────────────────────────────────────────

  async function doS3Sync() {
    const scope      = document.getElementById('s3Scope').value.trim();
    const bucket     = document.getElementById('s3Bucket').value.trim();
    const secretRef  = document.getElementById('s3SecretRef').value.trim() || 'default';
    const prefix     = document.getElementById('s3Prefix').value.trim();
    const region     = document.getElementById('s3Region').value.trim();
    const endpoint   = document.getElementById('s3Endpoint').value.trim();
    const owner      = document.getElementById('s3Owner').value.trim();
    const outEl      = document.getElementById('s3Out');
    const tableEl    = document.getElementById('s3Table');
    if (!scope || !bucket) { outEl.textContent = 'Scope and bucket are required.'; return; }
    outEl.textContent = 'Running S3 sync...';
    tableEl.innerHTML = '';
    try {
      const body = { scope, bucket, secret_ref: secretRef };
      if (prefix)   body.prefix       = prefix;
      if (region)   body.region        = region;
      if (endpoint) body.endpoint_url  = endpoint;
      if (owner)    body.owner         = owner;
      const resp = await fetch('/connectors/s3/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await resp.json();
      if (!resp.ok) { outEl.textContent = 'Error ' + resp.status + ': ' + (data.detail || resp.statusText); return; }
      outEl.textContent = `Done. ${data.total} object(s) processed in scope ${data.scope} from bucket ${data.bucket}.`;
      if (!data.results || !data.results.length) { tableEl.innerHTML = '<em>No objects found.</em>'; return; }
      const verdictColor = { fetched_new: '#155724', fetched_updated: '#004085', skipped_unchanged: '#383d41', fetch_failed: '#721c24' };
      tableEl.innerHTML = '<table><thead><tr><th>source_uri</th><th>verdict</th><th>task_id</th><th>error</th></tr></thead><tbody>' +
        data.results.map(r => {
          const color = verdictColor[r.verdict] || '#383d41';
          return `<tr>
            <td style="font-family:monospace;font-size:0.75rem;max-width:300px;overflow:hidden;text-overflow:ellipsis">${r.source_uri || '-'}</td>
            <td style="color:${color};font-weight:600">${r.verdict}</td>
            <td style="font-family:monospace;font-size:0.75rem">${r.task_id || '-'}</td>
            <td style="font-size:0.78rem;color:#721c24">${r.error_message || ''}</td>
          </tr>`;
        }).join('') + '</tbody></table>';
    } catch (e) { outEl.textContent = 'Error: ' + e.message; }
  }
  </script>
</body>
</html>
"""


def _db_unavailable(exc: Exception):
    logger.error("database unavailable: %s", exc)
    raise HTTPException(status_code=503, detail="Database unavailable.")
