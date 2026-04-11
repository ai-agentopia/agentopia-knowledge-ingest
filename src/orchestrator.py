"""Synchronous ingest orchestrator.

Coordinates the day-1 linear pipeline:
  upload → normalize → extract → Super RAG ingest → mark active

All external state is driven through the document's row_id (surrogate PK),
which identifies exactly one (document_id, version) row. This ensures every
state transition touches the correct version even when multiple versions of
the same logical document exist.

Phase-2 transition: each stage call will become a queue-emit.
No external API changes. No state machine changes.
"""

import json
import logging
import time

import httpx

from config import get_settings
from db import registry
from normalizer.base import NormalizationError, detect_format, normalize
from normalizer.extractor import ExtractedMetadata, extract
from storage import store

logger = logging.getLogger(__name__)


def run_pipeline(
    *,
    job_id: str,
    row_id: int,
    document_id: str,
    version: int,
    scope: str,
    filename: str,
    raw_bytes: bytes,
    s3_original_key: str,
    actor: str = "",
) -> None:
    """Execute the full synchronous ingest pipeline for one document version.

    Uses row_id (surrogate PK) to target the exact document version throughout.
    State transitions: submitted → normalizing → normalized → extracting →
                      extracted → indexing → active
                   OR any stage → failed (prior active version of this document untouched)
    """
    settings = get_settings()
    fmt = detect_format(filename)

    # ── Stage 1: Normalize ────────────────────────────────────────────────────
    _transition(job_id, row_id, document_id, version, scope, "normalizing", 20, actor=actor)
    try:
        normalized = normalize(raw_bytes, fmt)
    except (NormalizationError, ValueError) as exc:
        _fail(job_id, row_id, document_id, version, scope, str(exc), actor=actor)
        return
    except Exception as exc:
        _fail(job_id, row_id, document_id, version, scope,
              f"Unexpected normalization error: {exc}", actor=actor)
        return

    norm_bytes = json.dumps(
        normalized.to_json_dict(document_id, version, fmt)
    ).encode()
    try:
        s3_normalized_key = store.write_normalized(scope, document_id, version, norm_bytes)
    except Exception as exc:
        _fail(job_id, row_id, document_id, version, scope,
              f"Storage write failed (normalized): {exc}", actor=actor)
        return

    _transition(job_id, row_id, document_id, version, scope, "normalized", 40,
                s3_normalized_key=s3_normalized_key, actor=actor)

    # ── Stage 2: Extract metadata ─────────────────────────────────────────────
    _transition(job_id, row_id, document_id, version, scope, "extracting", 50, actor=actor)
    try:
        normalized_dict = json.loads(norm_bytes)
        extracted = extract(normalized_dict)
    except Exception as exc:
        # Extraction failure is non-fatal: proceed with empty metadata
        logger.warning(
            "orchestrator: extraction failed for doc=%s v%d — proceeding empty: %s",
            document_id, version, exc,
        )
        extracted = ExtractedMetadata(partial=True)

    ext_bytes = json.dumps(
        extracted.to_json_dict(document_id, version)
    ).encode()
    try:
        s3_extracted_key = store.write_extracted(scope, document_id, version, ext_bytes)
    except Exception as exc:
        _fail(job_id, row_id, document_id, version, scope,
              f"Storage write failed (extracted): {exc}", actor=actor)
        return

    _transition(job_id, row_id, document_id, version, scope, "extracted", 60,
                s3_extracted_key=s3_extracted_key, actor=actor)

    # ── Stage 3: Super RAG ingest ─────────────────────────────────────────────
    _transition(job_id, row_id, document_id, version, scope, "indexing", 80, actor=actor)

    section_path = extracted.section_path_from_hierarchy()
    hierarchy_dicts = [n.to_dict() for n in extracted.hierarchy]
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

    # Scope may contain '/' — Super RAG uses '--' as path separator in the URL
    url_scope = scope.replace("/", "--")
    ingest_url = (
        f"{settings.super_rag_url.rstrip('/')}"
        f"/api/v1/knowledge/{url_scope}/ingest-document"
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
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                sleep_secs = min(2.0 * (2 ** (attempt - 1)), 60.0)
                logger.warning(
                    "orchestrator: Super RAG ingest attempt %d/%d failed doc=%s v%d: %s — retry in %.1fs",
                    attempt, max_retries, document_id, version, exc, sleep_secs,
                )
                time.sleep(sleep_secs)

    if last_exc is not None:
        _fail(
            job_id, row_id, document_id, version, scope,
            f"Super RAG ingest failed after {max_retries} retries: {last_exc}",
            actor=actor,
        )
        return

    # ── Stage 4: Promote to active (atomic swap with prior active version) ────
    # promote_to_active() supersedes the prior active row and sets this row to
    # active in a single transaction. This satisfies the unique partial index
    # idx_documents_one_active (only one active per document_id at any time).
    # Plain update_document_status(row_id, "active") is NOT used here because
    # it would violate the constraint when a prior active version exists.
    try:
        superseded_row_id = registry.promote_to_active(row_id, document_id)
        registry.update_job(job_id, status="active", stage="active",
                            progress_percent=100, completed=True)
        registry.write_audit_event(
            action="active",
            document_id=document_id,
            document_version=version,
            job_id=job_id,
            scope=scope,
            actor=actor,
            status="success",
            metadata={"format": fmt, "superseded_row_id": superseded_row_id},
        )
        logger.info(
            "orchestrator: doc=%s v%d active scope=%s superseded_row=%s",
            document_id, version, scope, superseded_row_id,
        )
    except Exception as exc:
        logger.error("orchestrator: failed to promote active doc=%s v%d: %s", document_id, version, exc)
        _fail(job_id, row_id, document_id, version, scope,
              f"Failed to promote to active: {exc}", actor=actor)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _transition(
    job_id: str,
    row_id: int,
    document_id: str,
    version: int,
    scope: str,
    status: str,
    progress: int,
    *,
    s3_normalized_key: str | None = None,
    s3_extracted_key: str | None = None,
    actor: str = "",
) -> None:
    registry.update_document_status(
        row_id, status,
        s3_normalized_key=s3_normalized_key,
        s3_extracted_key=s3_extracted_key,
    )
    registry.update_job(job_id, status=status, stage=status, progress_percent=progress)
    registry.write_audit_event(
        action=status,
        document_id=document_id,
        document_version=version,
        job_id=job_id,
        scope=scope,
        actor=actor,
        status="in_progress",
    )


def _fail(
    job_id: str,
    row_id: int,
    document_id: str,
    version: int,
    scope: str,
    error_message: str,
    *,
    actor: str = "",
) -> None:
    """Transition document and job to failed. Prior active version untouched."""
    logger.error(
        "orchestrator: pipeline failed doc=%s v%d scope=%s: %s",
        document_id, version, scope, error_message,
    )
    try:
        registry.update_document_status(row_id, "failed", error_message=error_message)
        registry.update_job(job_id, status="failed", error_message=error_message,
                            completed=True)
        registry.write_audit_event(
            action="failed",
            document_id=document_id,
            document_version=version,
            job_id=job_id,
            scope=scope,
            actor=actor,
            status="failed",
            error_message=error_message,
        )
    except Exception as exc:
        logger.error("orchestrator: failed to write failure state: %s", exc)
