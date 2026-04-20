"""Managed-prefix relay — write connector files directly to a managed S3 prefix.

Migration path for push-based connectors (Google Drive, OneDrive) under ADR-001.

Architecture
------------
Under ADR-001, all ingest must converge on object storage → Pathway → Qdrant.
For connectors that cannot expose an S3-compatible endpoint (GDrive, OneDrive),
the relay pattern puts connector-fetched bytes into the scope's `managed_upload`
source prefix. Pathway's existing S3 watcher picks up the new objects
automatically — no Pathway restart, no second ingest pipeline.

              connector fetch
         (google_drive / onedrive)
                    │
                    ▼
           managed_relay.put()
                    │
                    ▼
       S3: {bucket}/{prefix}{filename}
                    │
                    ▼
         Pathway pw.io.s3.read()  (existing watcher)
                    │
                    ▼
                 Qdrant

Calling convention
------------------
Callers (connector wrappers) receive a `ManagedSourceRef` from the control plane
(bot-config-api `knowledge_sources` row) and call `relay_files()` for each batch.

The relay does NOT:
  - Write to the `documents`, `connector_sync_tasks`, or `ingest_jobs` tables.
  - Call the normalizer, extractor, or orchestrator.
  - Track per-file dedup state (Pathway handles idempotent re-ingest via ETag).
  - Accept credentials as positional arguments.
  - Log secret values at any log level.

S3 key contract
---------------
  key = prefix + safe_filename(filename)

`safe_filename()` strips path components and replaces non-safe characters with `_`
so that connector-sourced filenames cannot traverse the prefix boundary.

If the same filename is uploaded again with different content, S3 PutObject
replaces the existing object. Pathway detects the ETag change and re-ingests.
Unchanged content produces the same ETag — Pathway skips (idempotent).
"""

import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ManagedSourceRef:
    """Reference to a `managed_upload` knowledge_sources row.

    All fields come from the `storage_ref` JSONB column of the source row.
    Credentials come separately from Vault (never embedded in storage_ref).
    """

    source_id: str        # UUID from knowledge_sources.source_id
    bucket: str           # S3 bucket (Agentopia-managed, one per env)
    prefix: str           # S3 key prefix: "scopes/{scope_id}/sources/{source_id}/"
    region: str           # AWS region
    access_key: str       # Injected from Vault / K8s secret at call time
    secret_key: str       # Injected from Vault / K8s secret at call time
    endpoint_url: Optional[str] = None  # Non-None for MinIO / S3-compatible


@dataclass
class RelayResult:
    """Result for one file relayed to the managed prefix."""

    filename: str
    s3_key: str
    bytes_written: int
    etag: str
    replaced: bool    # True if an object already existed at this key


@dataclass
class RelayError:
    """Per-file error record when relay fails for one file."""

    filename: str
    error: str


# ---------------------------------------------------------------------------
# Filename safety
# ---------------------------------------------------------------------------

_UNSAFE = re.compile(r"[^\w.\-]")


def safe_filename(filename: str) -> str:
    """Sanitize a connector-sourced filename for use as an S3 key suffix.

    Strips all path components (preserves only the basename), then replaces
    any character that is not alphanumeric, dot, hyphen, or underscore with
    an underscore. Empty names map to "_unnamed".

    Examples:
        "report.pdf"         → "report.pdf"
        "my doc (v2).docx"   → "my_doc__v2_.docx"
        "../../../etc/passwd" → "passwd"
        ""                   → "_unnamed"
    """
    # Strip path components — only keep the last segment
    name = filename.replace("\\", "/").split("/")[-1]
    name = _UNSAFE.sub("_", name)
    return name or "_unnamed"


# ---------------------------------------------------------------------------
# S3 client factory
# ---------------------------------------------------------------------------

def _build_s3_client(ref: ManagedSourceRef):
    try:
        import boto3
    except ImportError as exc:
        raise ImportError("boto3 is required: pip install boto3") from exc

    kwargs = {
        "aws_access_key_id": ref.access_key,
        "aws_secret_access_key": ref.secret_key,
        "region_name": ref.region,
    }
    if ref.endpoint_url:
        kwargs["endpoint_url"] = ref.endpoint_url
    return boto3.client("s3", **kwargs)


# ---------------------------------------------------------------------------
# Core relay functions
# ---------------------------------------------------------------------------

def put_file(
    ref: ManagedSourceRef,
    filename: str,
    content: bytes,
    content_type: str = "application/octet-stream",
) -> RelayResult:
    """Write a single file into the managed S3 prefix.

    The S3 key is `ref.prefix + safe_filename(filename)`. If an object at that
    key already exists, it is replaced (S3 PutObject semantics). Pathway detects
    ETag changes and re-ingests only when content differs.

    Args:
        ref:          ManagedSourceRef identifying the target source.
        filename:     Original filename from the connector. Will be sanitized.
        content:      Raw file bytes.
        content_type: MIME type; stored as S3 ContentType metadata (informational).

    Returns:
        RelayResult with s3_key, bytes_written, etag, and replaced flag.

    Raises:
        RuntimeError: On any S3 error. Caller should log and continue with
                      remaining files rather than aborting the full sync.
    """
    s3_filename = safe_filename(filename)
    key = ref.prefix + s3_filename

    s3 = _build_s3_client(ref)

    # Check if object already exists (to populate RelayResult.replaced)
    replaced = False
    try:
        s3.head_object(Bucket=ref.bucket, Key=key)
        replaced = True
    except Exception:
        pass  # Object does not exist — normal for first upload

    try:
        response = s3.put_object(
            Bucket=ref.bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
        )
    except Exception as exc:
        raise RuntimeError(
            f"managed_relay: failed to write s3://{ref.bucket}/{key}: {type(exc).__name__}: {exc}"
        ) from exc

    etag = (response.get("ETag") or "").strip('"')

    logger.info(
        "managed_relay: put source_id=%s s3://%s/%s bytes=%d etag=%s replaced=%s",
        ref.source_id, ref.bucket, key, len(content), etag, replaced,
    )

    return RelayResult(
        filename=filename,
        s3_key=key,
        bytes_written=len(content),
        etag=etag,
        replaced=replaced,
    )


def relay_files(
    ref: ManagedSourceRef,
    files: list[tuple[str, bytes, str]],
) -> tuple[list[RelayResult], list[RelayError]]:
    """Relay a batch of connector files to the managed S3 prefix.

    Args:
        ref:   Target managed source ref.
        files: List of (filename, content_bytes, content_type) tuples.

    Returns:
        Tuple of (results, errors). Files that fail are recorded in errors;
        successful files appear in results. The caller decides whether
        partial success is acceptable.
    """
    results: list[RelayResult] = []
    errors: list[RelayError] = []

    for filename, content, content_type in files:
        try:
            result = put_file(ref, filename, content, content_type)
            results.append(result)
        except Exception as exc:
            logger.error(
                "managed_relay: failed to relay filename=%r source_id=%s: %s",
                filename, ref.source_id, exc,
            )
            errors.append(RelayError(filename=filename, error=str(exc)))

    logger.info(
        "managed_relay: batch done source_id=%s success=%d errors=%d",
        ref.source_id, len(results), len(errors),
    )
    return results, errors


def delete_file(ref: ManagedSourceRef, filename: str) -> bool:
    """Delete a previously relayed file from the managed S3 prefix.

    Used when the connector reports a file deletion (e.g. GDrive trash event).
    Pathway detects the key removal and retracts the corresponding Qdrant chunks.

    Returns True if the object was deleted, False if it did not exist.
    """
    key = ref.prefix + safe_filename(filename)
    s3 = _build_s3_client(ref)

    try:
        s3.delete_object(Bucket=ref.bucket, Key=key)
        logger.info(
            "managed_relay: delete source_id=%s s3://%s/%s",
            ref.source_id, ref.bucket, key,
        )
        return True
    except Exception as exc:
        logger.error(
            "managed_relay: delete failed source_id=%s key=%s: %s",
            ref.source_id, key, exc,
        )
        return False
