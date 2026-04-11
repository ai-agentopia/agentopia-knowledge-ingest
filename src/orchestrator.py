"""Synchronous ingest orchestrator.

Coordinates the day-1 linear pipeline:
  upload → normalize → extract → Super RAG ingest → mark active

This is the synchronous implementation. All stages run in-process,
blocking until completion. The operator receives a job_id immediately
(non-blocking upload), then polls GET /jobs/{job_id} for progress.

Phase-2 transition: each stage call below will become a queue-emit.
No external API changes. No state machine changes.
"""

import hashlib
import json
import logging
import time

import httpx

from config import get_settings
from db import registry
from normalizer.base import NormalizationError, detect_format, normalize
from normalizer.extractor import extract
from storage import store

logger = logging.getLogger(__name__)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run_pipeline(
    *,
    job_id: str,
    document_id: str,
    scope: str,
    version: int,
    filename: str,
    raw_bytes: bytes,
    s3_original_key: str,
    actor: str = "",
) -> None:
    """Execute the full synchronous ingest pipeline for one document.

    Intended to be called from a background thread or task so the
    HTTP upload response returns immediately.

    State transitions:
      submitted -> normalizing -> normalized -> extracting -> extracted
      -> indexing -> active
      OR any stage -> failed (prior active version untouched)

    All state transitions are written to audit_log.
    On any unrecoverable failure: document status = failed, job = failed,
    prior active version (if any) remains active.
    """
    settings = get_settings()
    fmt = detect_format(filename)

    # ── Stage 1: Normalize ────────────────────────────────────────────────────
    _transition(job_id, document_id, scope, "normalizing", 20, actor=actor)
    try:
        normalized = normalize(raw_bytes, fmt)
    except (NormalizationError, ValueError) as exc:
        _fail(job_id, document_id, scope, str(exc), actor=actor)
        return
    except Exception as exc:
        _fail(job_id, document_id, scope, f"Unexpected normalization error: {exc}", actor=actor)
        return

    norm_bytes = json.dumps(
        normalized.to_json_dict(document_id, version, fmt)
    ).encode()
    try:
        s3_normalized_key = store.write_normalized(scope, document_id, version, norm_bytes)
    except Exception as exc:
        _fail(job_id, document_id, scope, f"Storage write failed (normalized): {exc}", actor=actor)
        return

    _transition(
        job_id, document_id, scope, "normalized", 40,
        s3_normalized_key=s3_normalized_key, actor=actor,
    )

    # ── Stage 2: Extract metadata ─────────────────────────────────────────────
    _transition(job_id, document_id, scope, "extracting", 50, actor=actor)
    try:
        normalized_dict = json.loads(norm_bytes)
        extracted = extract(normalized_dict)
    except Exception as exc:
        # Extraction failure is non-fatal: proceed with empty metadata
        logger.warning(
            "orchestrator: extraction failed for %s — proceeding with empty metadata: %s",
            document_id, exc,
        )
        from normalizer.extractor import ExtractedMetadata
        extracted = ExtractedMetadata(partial=True)

    ext_bytes = json.dumps(
        extracted.to_json_dict(document_id, version)
    ).encode()
    try:
        s3_extracted_key = store.write_extracted(scope, document_id, version, ext_bytes)
    except Exception as exc:
        _fail(job_id, document_id, scope, f"Storage write failed (extracted): {exc}", actor=actor)
        return

    _transition(
        job_id, document_id, scope, "extracted", 60,
        s3_extracted_key=s3_extracted_key, actor=actor,
    )

    # ── Stage 3: Super RAG ingest ─────────────────────────────────────────────
    _transition(job_id, document_id, scope, "indexing", 80, actor=actor)

    section_path = extracted.section_path_from_hierarchy()
    hierarchy_dicts = [n.to_dict() for n in extracted.hierarchy]

    # Determine chunking strategy: use markdown_aware for markdown format
    chunking_strategy = "markdown_aware" if fmt == "markdown" else "fixed_size"

    payload = {
        "document_id": document_id,
        "version": version,
        "text": normalized.text,
        "metadata": {
            "title": extracted.title,
            "author": extracted.author,
            "date": extracted.date,
            "language": extracted.language,
            "format": fmt,
            "hierarchy": hierarchy_dicts,
            "section_path": section_path,
            "tags": extracted.tags,
        },
        "chunking_strategy": chunking_strategy,
    }

    max_retries = settings.super_rag_ingest_max_retries
    timeout_secs = settings.super_rag_ingest_timeout_seconds
    ingest_url = (
        f"{settings.super_rag_url.rstrip('/')}"
        f"/api/v1/knowledge/{scope.replace('/', '--')}/ingest-document"
    )

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = httpx.post(
                ingest_url,
                json=payload,
                headers={"X-Internal-Token": settings.super_rag_internal_token},
                timeout=timeout_secs,
            )
            resp.raise_for_status()
            break
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                sleep_secs = min(2.0 * (2 ** (attempt - 1)), 60.0)
                logger.warning(
                    "orchestrator: Super RAG ingest attempt %d/%d failed for %s: %s — retry in %.1fs",
                    attempt, max_retries, document_id, exc, sleep_secs,
                )
                time.sleep(sleep_secs)
            else:
                logger.error(
                    "orchestrator: Super RAG ingest exhausted %d retries for %s: %s",
                    max_retries, document_id, exc,
                )
    else:
        _fail(
            job_id, document_id, scope,
            f"Super RAG ingest failed after {max_retries} retries: {last_exc}",
            actor=actor,
        )
        return

    # ── Stage 4: Mark active ──────────────────────────────────────────────────
    try:
        registry.update_document_status(document_id, "active")
        registry.update_job(
            job_id, status="active", stage="active",
            progress_percent=100, completed=True,
        )
        registry.write_audit_event(
            action="active",
            document_id=document_id,
            job_id=job_id,
            scope=scope,
            actor=actor,
            status="success",
            metadata={"version": version, "format": fmt},
        )
        logger.info(
            "orchestrator: document %s v%d active in scope=%s",
            document_id, version, scope,
        )
    except Exception as exc:
        logger.error("orchestrator: failed to mark document active: %s", exc)
        _fail(job_id, document_id, scope, f"Failed to mark active: {exc}", actor=actor)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _transition(
    job_id: str,
    document_id: str,
    scope: str,
    status: str,
    progress: int,
    *,
    s3_normalized_key: str | None = None,
    s3_extracted_key: str | None = None,
    actor: str = "",
) -> None:
    """Write status transition to document, job, and audit_log."""
    registry.update_document_status(
        document_id, status,
        s3_normalized_key=s3_normalized_key,
        s3_extracted_key=s3_extracted_key,
    )
    registry.update_job(job_id, status=status, stage=status, progress_percent=progress)
    registry.write_audit_event(
        action=status,
        document_id=document_id,
        job_id=job_id,
        scope=scope,
        actor=actor,
        status="in_progress",
    )


def _fail(
    job_id: str,
    document_id: str,
    scope: str,
    error_message: str,
    *,
    actor: str = "",
) -> None:
    """Transition document and job to failed state. Prior active version untouched."""
    logger.error(
        "orchestrator: pipeline failed document=%s scope=%s: %s",
        document_id, scope, error_message,
    )
    try:
        registry.update_document_status(document_id, "failed", error_message=error_message)
        registry.update_job(
            job_id, status="failed", error_message=error_message,
            progress_percent=0, completed=True,
        )
        registry.write_audit_event(
            action="failed",
            document_id=document_id,
            job_id=job_id,
            scope=scope,
            actor=actor,
            status="failed",
            error_message=error_message,
        )
    except Exception as exc:
        logger.error("orchestrator: failed to write failure state: %s", exc)
