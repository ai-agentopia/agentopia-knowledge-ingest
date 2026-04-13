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
from dataclasses import dataclass

import httpx

from config import get_settings
from db import registry
from normalizer.base import NormalizationError, detect_format, normalize
from normalizer.extractor import ExtractedMetadata, extract
from storage import store


@dataclass
class PipelineResult:
    """Outcome of a synchronous ingest pipeline run.

    Returned by run_pipeline() so callers can surface the actual ingest
    outcome — not just the connector sync verdict.
    """

    status: str  # "active" | "failed"
    chunks_created: int = 0
    chunks_skipped: int = 0
    error_message: str | None = None
    stage_failed: str | None = None  # "normalization" | "extraction_storage" | "indexing" | "promotion"

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
) -> PipelineResult:
    """Execute the full synchronous ingest pipeline for one document version.

    Uses row_id (surrogate PK) to target the exact document version throughout.
    State transitions: submitted → normalizing → normalized → extracting →
                      extracted → indexing → active
                   OR any stage → failed (prior active version of this document untouched)

    Returns PipelineResult with the final status, chunk counts, and error details.
    """
    settings = get_settings()
    fmt = detect_format(filename)

    # ── Stage 1: Normalize ────────────────────────────────────────────────────
    _transition(job_id, row_id, document_id, version, scope, "normalizing", 20, actor=actor)
    try:
        normalized = normalize(raw_bytes, fmt)
    except (NormalizationError, ValueError) as exc:
        _fail(job_id, row_id, document_id, version, scope, str(exc), actor=actor)
        return PipelineResult(status="failed", error_message=str(exc), stage_failed="normalization")
    except Exception as exc:
        msg = f"Unexpected normalization error: {exc}"
        _fail(job_id, row_id, document_id, version, scope, msg, actor=actor)
        return PipelineResult(status="failed", error_message=msg, stage_failed="normalization")

    norm_bytes = json.dumps(
        normalized.to_json_dict(document_id, version, fmt)
    ).encode()
    try:
        s3_normalized_key = store.write_normalized(scope, document_id, version, norm_bytes)
    except Exception as exc:
        msg = f"Storage write failed (normalized): {exc}"
        _fail(job_id, row_id, document_id, version, scope, msg, actor=actor)
        return PipelineResult(status="failed", error_message=msg, stage_failed="normalization")

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
        msg = f"Storage write failed (extracted): {exc}"
        _fail(job_id, row_id, document_id, version, scope, msg, actor=actor)
        return PipelineResult(status="failed", error_message=msg, stage_failed="extraction_storage")

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
        msg = f"Super RAG ingest failed after {max_retries} retries: {last_exc}"
        _fail(job_id, row_id, document_id, version, scope, msg, actor=actor)
        return PipelineResult(status="failed", error_message=msg, stage_failed="indexing")

    # ── Stage 4: Promote to active (atomic swap with prior active version) ────
    # promote_to_active() supersedes the prior active row and sets this row to
    # active in a single transaction. This satisfies the unique partial index
    # idx_documents_one_active (only one active per document_id at any time).
    # Plain update_document_status(row_id, "active") is NOT used here because
    # it would violate the constraint when a prior active version exists.
    # Parse chunk counts from the Super RAG response
    chunks_created = 0
    chunks_skipped = 0
    try:
        rag_data = resp.json()
        chunks_created = rag_data.get("chunk_count", 0)
        if rag_data.get("status") == "skipped":
            chunks_skipped = chunks_created
            chunks_created = 0
    except Exception:
        pass

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
            "orchestrator: doc=%s v%d active scope=%s superseded_row=%s chunks=%d",
            document_id, version, scope, superseded_row_id, chunks_created,
        )
    except Exception as exc:
        logger.error("orchestrator: failed to promote active doc=%s v%d: %s", document_id, version, exc)
        msg = f"Failed to promote to active: {exc}"
        _fail(job_id, row_id, document_id, version, scope, msg, actor=actor)
        return PipelineResult(status="failed", error_message=msg, stage_failed="promotion",
                              chunks_created=chunks_created, chunks_skipped=chunks_skipped)

    return PipelineResult(status="active", chunks_created=chunks_created, chunks_skipped=chunks_skipped)


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
