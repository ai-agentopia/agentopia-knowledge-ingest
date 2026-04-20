"""Managed-prefix relay — write connector files directly to a managed S3 prefix.

Push-source connector workflow for Google Drive, OneDrive, and similar sources
under ADR-001. Implements the managed-upload relay pattern.

Architecture
------------
Push-based connectors cannot expose an S3-compatible endpoint. Under ADR-001
these connectors write fetched bytes into the scope's `managed_upload` source
prefix. Pathway's existing S3 watcher picks up the objects automatically —
no Pathway restart, no separate pipeline.

              connector fetch
         (google_drive / onedrive)
                    │
                    ▼
           managed_relay.relay_files()
                    │
                    ▼
       S3: {prefix}{connector_module}/{safe_stable_id(stable_id)}{ext}
                    │
                    ▼
         Pathway pw.io.s3.read()  (existing watcher)
                    │
                    ▼
                 Qdrant

S3 object identity contract
----------------------------
Key = `{prefix}{connector_module}/{safe_stable_id(stable_id)}{ext}`

  prefix           – from ManagedSourceRef (e.g. "scopes/{scope_id}/sources/{src_id}/")
  connector_module – "google_drive", "onedrive", "aws_s3", etc.
  stable_id        – the connector's own stable item identifier (NOT the filename):
                       • Google Drive: fileId (opaque, stable across renames)
                       • OneDrive:     itemId (stable within a drive)
                     The stable_id is sanitised via safe_stable_id() before use.
  ext              – extension derived from the display filename (".pdf", ".docx", ...)

Why NOT basename:
  Two files named "report.pdf" in different Drive folders have different fileIds
  but the same basename → basename-only identity causes silent overwrite/delete.
  stable_id-based identity prevents all such collisions.

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
"""

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

    source_id: str               # UUID from knowledge_sources.source_id
    bucket: str                  # S3 bucket (Agentopia-managed, one per env)
    prefix: str                  # Key prefix: "scopes/{scope_id}/sources/{source_id}/"
    region: str                  # AWS region
    access_key: str              # Injected from Vault / K8s secret at call time
    secret_key: str              # Injected from Vault / K8s secret at call time
    endpoint_url: Optional[str] = None  # Non-None for MinIO / S3-compatible

    @classmethod
    def from_source_row(
        cls,
        source_id: str,
        storage_ref: dict,
        access_key: str,
        secret_key: str,
        endpoint_url: Optional[str] = None,
    ) -> "ManagedSourceRef":
        """Construct from a knowledge_sources row's storage_ref JSONB.

        Args:
            source_id:   knowledge_sources.source_id (UUID string)
            storage_ref: Parsed storage_ref dict {"bucket", "prefix", "region"}
            access_key:  S3 access key — from Vault or K8s Secret at call time
            secret_key:  S3 secret key — from Vault or K8s Secret at call time
            endpoint_url: Optional S3-compatible endpoint override
        """
        return cls(
            source_id=source_id,
            bucket=storage_ref["bucket"],
            prefix=storage_ref.get("prefix", ""),
            region=storage_ref["region"],
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url,
        )


@dataclass
class ConnectorFile:
    """A file from a push-based connector, ready to relay to managed S3 prefix.

    Identity is carried by (connector_module, stable_id), NOT by filename.
    filename is used only for extension extraction.

    Attributes:
        connector_module: Connector identifier — "google_drive", "onedrive", etc.
                          Used as a sub-folder in the S3 key to namespace by source.
        stable_id:        The connector's native stable identifier for this item.
                          Must be stable across file renames, folder moves, and
                          repeated sync runs. Examples:
                            - Google Drive: fileId (opaque, Drive-assigned)
                            - OneDrive: itemId (stable within a drive)
                          Do NOT use the filename or display path as stable_id.
        filename:         Display filename from the connector. Used ONLY to derive
                          the file extension for the S3 key. Not used for identity.
        content:          Raw file bytes.
        content_type:     MIME type (informational; stored as S3 ContentType).
    """

    connector_module: str
    stable_id: str
    filename: str
    content: bytes
    content_type: str = "application/octet-stream"


@dataclass
class RelayResult:
    """Result for one file successfully relayed to the managed prefix."""

    connector_module: str
    stable_id: str
    filename: str
    s3_key: str
    bytes_written: int
    etag: str
    replaced: bool    # True if an object already existed at this key


@dataclass
class RelayError:
    """Per-file error when relay fails."""

    connector_module: str
    stable_id: str
    filename: str
    error: str


# ---------------------------------------------------------------------------
# Key identity helpers
# ---------------------------------------------------------------------------

_STABLE_ID_UNSAFE = re.compile(r"[^\w\-]")


def safe_stable_id(stable_id: str) -> str:
    """Sanitise a connector stable ID for use as an S3 key segment.

    Replaces every character that is not alphanumeric, hyphen, or underscore
    with an underscore. Produces a deterministic 1-to-1 mapping — the same
    input always produces the same output, and different inputs produce different
    outputs (no collapsing of consecutive unsafe chars, preserving injectivity).

    Empty stable_id maps to "_empty".

    Examples:
        "1BxiMVs0XRA5nFMdK"           → "1BxiMVs0XRA5nFMdK"  (GDrive fileId, unchanged)
        "driveId123/itemId456"          → "driveId123_itemId456"
        "onedrive://driveId/itemId"     → "onedrive___driveId_itemId"
        "item with spaces"              → "item_with_spaces"
        ""                             → "_empty"
    """
    if not stable_id:
        return "_empty"
    return _STABLE_ID_UNSAFE.sub("_", stable_id)


def _derive_ext(filename: str) -> str:
    """Extract the lowercase file extension from a filename.

    Returns the extension including the leading dot (e.g. ".pdf", ".docx").
    Returns "" if the filename has no extension or is empty.

    Examples:
        "report.pdf"     → ".pdf"
        "archive.tar.gz" → ".gz"
        "README"         → ""
        ""               → ""
    """
    if not filename or "." not in filename:
        return ""
    return "." + filename.rsplit(".", 1)[-1].lower()


def object_key(ref: ManagedSourceRef, connector_module: str, stable_id: str, filename: str) -> str:
    """Compute the canonical S3 key for a connector file.

    Formula: {prefix}{connector_module}/{safe_stable_id(stable_id)}{ext}

    This function is the single authoritative source of the key formula.
    Both put_file() and delete_file() call this — guaranteeing that upload
    and delete always reference the same key for the same logical item.

    Args:
        ref:              Target managed source ref (provides prefix).
        connector_module: Connector identifier (namespaces keys per connector).
        stable_id:        Connector's stable item identifier.
        filename:         Display filename (extension extraction only).

    Returns:
        Full S3 key string.
    """
    return f"{ref.prefix}{connector_module}/{safe_stable_id(stable_id)}{_derive_ext(filename)}"


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

def put_file(ref: ManagedSourceRef, file: ConnectorFile) -> RelayResult:
    """Write a single connector file into the managed S3 prefix.

    The S3 key is derived from the file's connector_module + stable_id + extension.
    If an object already exists at that key it is replaced (S3 PutObject semantics).
    Pathway detects ETag changes and re-ingests; unchanged content is idempotent.

    Args:
        ref:  ManagedSourceRef identifying the target source.
        file: ConnectorFile carrying stable_id, filename, content, content_type.

    Returns:
        RelayResult.

    Raises:
        RuntimeError: On S3 error. Callers should catch and continue with remaining files.
    """
    key = object_key(ref, file.connector_module, file.stable_id, file.filename)
    s3 = _build_s3_client(ref)

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
            Body=file.content,
            ContentType=file.content_type,
        )
    except Exception as exc:
        raise RuntimeError(
            f"managed_relay: PutObject failed s3://{ref.bucket}/{key}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    etag = (response.get("ETag") or "").strip('"')

    logger.info(
        "managed_relay: put source_id=%s connector=%s stable_id=%s "
        "s3://%s/%s bytes=%d etag=%s replaced=%s",
        ref.source_id, file.connector_module, file.stable_id,
        ref.bucket, key, len(file.content), etag, replaced,
    )

    return RelayResult(
        connector_module=file.connector_module,
        stable_id=file.stable_id,
        filename=file.filename,
        s3_key=key,
        bytes_written=len(file.content),
        etag=etag,
        replaced=replaced,
    )


def relay_files(
    ref: ManagedSourceRef,
    files: list[ConnectorFile],
) -> tuple[list[RelayResult], list[RelayError]]:
    """Relay a batch of connector files to the managed S3 prefix.

    Processes all files; per-file errors do not abort the batch. The caller
    decides whether partial success is acceptable for its sync semantics.

    Args:
        ref:   Target managed source ref.
        files: List of ConnectorFile objects.

    Returns:
        (results, errors) — successful files in results, failed files in errors.
    """
    results: list[RelayResult] = []
    errors: list[RelayError] = []

    for file in files:
        try:
            result = put_file(ref, file)
            results.append(result)
        except Exception as exc:
            logger.error(
                "managed_relay: relay failed connector=%s stable_id=%s source_id=%s: %s",
                file.connector_module, file.stable_id, ref.source_id, exc,
            )
            errors.append(RelayError(
                connector_module=file.connector_module,
                stable_id=file.stable_id,
                filename=file.filename,
                error=str(exc),
            ))

    logger.info(
        "managed_relay: batch done source_id=%s success=%d errors=%d",
        ref.source_id, len(results), len(errors),
    )
    return results, errors


def delete_file(
    ref: ManagedSourceRef,
    connector_module: str,
    stable_id: str,
    filename: str,
) -> bool:
    """Delete a previously relayed file from the managed S3 prefix.

    The key is computed by the same object_key() formula as put_file() so that
    delete always targets the exact object that upload created.

    Used when a connector reports a file deletion (e.g. GDrive trash event).
    Pathway detects the key removal and retracts the corresponding Qdrant chunks.

    Args:
        ref:              Target managed source ref.
        connector_module: Same connector_module used at upload time.
        stable_id:        Same stable_id used at upload time.
        filename:         Same filename used at upload time (for extension matching).

    Returns:
        True if the DeleteObject call succeeded; False on error.
    """
    key = object_key(ref, connector_module, stable_id, filename)
    s3 = _build_s3_client(ref)

    try:
        s3.delete_object(Bucket=ref.bucket, Key=key)
        logger.info(
            "managed_relay: delete source_id=%s connector=%s stable_id=%s s3://%s/%s",
            ref.source_id, connector_module, stable_id, ref.bucket, key,
        )
        return True
    except Exception as exc:
        logger.error(
            "managed_relay: delete failed source_id=%s connector=%s stable_id=%s: %s",
            ref.source_id, connector_module, stable_id, exc,
        )
        return False
