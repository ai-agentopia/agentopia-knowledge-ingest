"""W-C3.1: POST /connectors/ingest — thin HTTP transport wrapper.

Issue:  knowledge-ingest#42
ADR:    docs/adr/002-connector-precondition-audit.md (W-C3.1 trigger condition)
ADR:    docs/adr/003-openrag-integration-ownership.md (in-process-first seam)

This module is intentionally isolated. It imports ONLY:
  - standard library (base64, logging, typing)
  - fastapi / pydantic
  - connectors.adapter (ConnectorEvent, ingest_from_connector)

It does NOT import db.registry, db.connector_sync, orchestrator, storage, or config.
All business logic (scope validation, dedup, provenance, state machine) lives in the
adapter and must NOT be re-implemented here.

Payload-size contract
---------------------
The 50 MiB limit is enforced BEFORE decode by checking the base64 string length.

Mathematical relationship: a base64-encoded payload of N bytes produces exactly
ceil(N / 3) * 4 characters (including padding). Therefore:

  max_b64_chars = ceil(max_decoded_bytes / 3) * 4

If len(raw_bytes_b64) > max_b64_chars, the decoded output would necessarily exceed
max_decoded_bytes, so we reject BEFORE calling b64decode. No oversized allocation
is ever made. After a safe decode we do a final byte-level check to guard against
pathological padding or non-standard encoders.

Auth boundary
-------------
No bearer token required — internal-network-only, matching the operator UI.
"""

import base64
import logging
import math
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from connectors.adapter import ConnectorEvent, ingest_from_connector

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Size constants
# ---------------------------------------------------------------------------

_MAX_DECODED_BYTES: int = 50 * 1024 * 1024  # 50 MiB

# Maximum number of base64 characters that can encode exactly _MAX_DECODED_BYTES.
# ceil(n / 3) * 4 — derived from the RFC 4648 base64 encoding length formula.
_MAX_B64_CHARS: int = math.ceil(_MAX_DECODED_BYTES / 3) * 4  # 69,905,068 chars

_VALID_FORMATS = {"pdf", "docx", "html", "markdown", "txt"}


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class ConnectorIngestRequest(BaseModel):
    """Request body for POST /connectors/ingest (W-C3.1).

    raw_bytes_b64 must be standard base64-encoded (RFC 4648) document content.
    Maximum decoded size: 50 MiB. The size is checked against the base64 string
    length BEFORE decode to avoid unbounded memory allocation (413 is returned
    if len(raw_bytes_b64) > 69,905,068 characters).
    """

    connector_module: str
    scope: str
    source_uri: str
    filename: str
    format: str
    raw_bytes_b64: str
    source_revision: Optional[str] = None
    owner: str = ""
    metadata: Dict[str, Any] = {}

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        if v not in _VALID_FORMATS:
            raise ValueError(
                f"format must be one of {sorted(_VALID_FORMATS)}. Got: '{v}'"
            )
        return v

    @field_validator("connector_module", "scope", "source_uri", "filename")
    @classmethod
    def validate_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("field must not be empty")
        return v


class ConnectorIngestResponse(BaseModel):
    task_id: str
    verdict: str
    document_id: Optional[str] = None
    job_id: Optional[str] = None
    version: Optional[int] = None
    error_message: Optional[str] = None
    document_status: Optional[str] = None
    chunks_created: Optional[int] = None
    chunks_skipped: Optional[int] = None
    pipeline_error: Optional[str] = None
    stage_failed: Optional[str] = None


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post("/connectors/ingest", status_code=200, response_model=ConnectorIngestResponse)
async def connector_ingest(body: ConnectorIngestRequest) -> ConnectorIngestResponse:
    """Accept a connector-sourced document and pass it to ingest_from_connector().

    This is a thin HTTP transport wrapper — it performs no business logic:
    - Scope registry validation: adapter (W-C1.9)
    - source_uri validation: adapter
    - Dedup via source_hash: adapter (W-C1.10)
    - Provenance immutability: adapter (W-C1.6)
    - connector_sync_tasks state machine: adapter (W-C1.8)

    HTTP semantics:
    - 200 OK  for all adapter verdicts including fetch_failed
    - 422     for malformed body, missing required field, invalid format, invalid base64
    - 413     for base64 string length implying decoded size > 50 MiB (checked BEFORE decode)

    Auth: none — internal-network-only (same boundary as operator UI).
    """
    # ── Pre-decode size guard (prevents unbounded allocation) ──────────────
    # If the b64 string length exceeds _MAX_B64_CHARS, the decoded payload
    # would necessarily exceed _MAX_DECODED_BYTES. Reject before allocating.
    if len(body.raw_bytes_b64) > _MAX_B64_CHARS:
        raise HTTPException(
            status_code=413,
            detail=(
                f"raw_bytes_b64 length {len(body.raw_bytes_b64)} characters implies "
                f"decoded size > {_MAX_DECODED_BYTES // (1024 * 1024)} MiB limit"
            ),
        )

    # ── Decode — 422 on invalid base64 ────────────────────────────────────
    try:
        raw_bytes = base64.b64decode(body.raw_bytes_b64, validate=True)
    except Exception:
        raise HTTPException(status_code=422, detail="raw_bytes_b64 is not valid base64")

    # ── Post-decode size guard (defence-in-depth for pathological encoders) ─
    if len(raw_bytes) > _MAX_DECODED_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"Decoded payload size {len(raw_bytes)} bytes exceeds the "
                f"{_MAX_DECODED_BYTES // (1024 * 1024)} MiB limit"
            ),
        )

    event = ConnectorEvent(
        connector_module=body.connector_module,
        scope=body.scope,
        source_uri=body.source_uri,
        filename=body.filename,
        format=body.format,
        raw_bytes=raw_bytes,
        source_revision=body.source_revision,
        owner=body.owner,
        metadata=body.metadata,
    )

    logger.debug(
        "connector_ingest: connector_module=%s source_uri=%s filename=%r format=%s bytes=%d",
        body.connector_module, body.source_uri, body.filename, body.format, len(raw_bytes),
    )

    result = ingest_from_connector(event)

    logger.info(
        "connector_ingest: verdict=%s task_id=%s source_uri=%s",
        result.verdict, result.task_id, body.source_uri,
    )

    return ConnectorIngestResponse(
        task_id=result.task_id,
        verdict=result.verdict,
        document_id=result.document_id,
        job_id=result.job_id,
        version=result.version,
        error_message=result.error_message,
        document_status=result.document_status,
        chunks_created=result.chunks_created,
        chunks_skipped=result.chunks_skipped,
        pipeline_error=result.pipeline_error,
        stage_failed=result.stage_failed,
    )
