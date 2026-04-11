"""W-C2.1: AWS S3 connector wrapper — bridges S3Connector into ingest_from_connector().

Issue:  knowledge-ingest#37
ADR:    docs/adr/002-connector-precondition-audit.md (GATE-1)
Source: langflow-ai/openrag src/connectors/aws_s3/connector.py (see openrag_s3.py)

Architecture
------------
This module is a pure wrapper. It:

  1. Accepts a config dict (credentials from managed secret storage).
  2. Instantiates S3Connector and authenticates.
  3. Iterates list_files() + get_file_content() for each discovered object.
  4. Derives the stable source_uri and source_revision per W-C1 contracts.
  5. Constructs a ConnectorEvent and calls ingest_from_connector().

It does NOT:
  - Write to documents, connector_sync_tasks, or ingest_jobs tables directly.
  - Call the normalizer, extractor, or orchestrator directly.
  - Bypass the adapter's scope registry validation (W-C1.9).
  - Bypass the adapter's source_hash dedup (W-C1.10).
  - Implement webhook / event notification support (out of scope for #37).
  - Accept credentials as positional arguments.
  - Log secret values at any log level.

Identity contract (W-C1.2, W-C1.3, #37)
-----------------------------------------
  connector_module  = "aws_s3"
  source_uri        = "s3://{bucket}/{key}"
                      Derived from the S3Connector composite file ID "{bucket}::{key}".
                      Stable across re-fetches of the same object. Changes on
                      object rename/move (same semantics as W-C1.4 manual rename).
  source_revision   = doc.modified_time.isoformat()
                      LastModified UTC datetime from boto3, serialised to ISO 8601.
                      Written once on INSERT (W-C1.6 provenance immutability).

Secret handling contract
------------------------
The caller (operator script, scheduled job, or future W-C3.1 HTTP handler) is
responsible for loading credentials from a managed secret store (K8s Secret, Vault,
or equivalent) and passing them in the config dict. This module accepts config only
via the `config` parameter — never via positional arguments or module-level globals.

The config dict keys are:
  access_key   (str)       : AWS access key ID (required unless env var set).
  secret_key   (str)       : AWS secret access key (required unless env var set).
  endpoint_url (str|None)  : Custom endpoint for S3-compatible services. Omit for AWS S3.
  region       (str)       : AWS region. Default: us-east-1.
  bucket_names (list[str]) : Buckets to ingest from. Empty list = auto-discover all.
  prefix       (str)       : Optional key prefix filter (e.g. "docs/").
  scope        (str)       : Target knowledge scope, used as fallback when no
                             CONNECTOR_SCOPE_MAPPINGS rule matches source_uri.
  owner        (str)       : Optional actor identifier for audit log.

Webhook support
---------------
Not implemented. S3 event notifications are out of scope for W-C2.1 (#37).
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from connectors.adapter import ConnectorEvent, SyncResult, ingest_from_connector
from connectors.openrag_s3 import S3Connector, _split_file_id

logger = logging.getLogger(__name__)

CONNECTOR_MODULE = "aws_s3"


# ---------------------------------------------------------------------------
# Public config dataclass (for callers that prefer typed config)
# ---------------------------------------------------------------------------

@dataclass
class S3ConnectorConfig:
    """Typed config for the S3 connector wrapper.

    All credential fields must come from managed secret storage — never from
    hardcoded strings or user input without a secret store intermediary.
    """
    access_key: str = ""
    secret_key: str = ""
    endpoint_url: Optional[str] = None
    region: str = "us-east-1"
    bucket_names: List[str] = field(default_factory=list)
    prefix: str = ""
    scope: str = ""
    owner: str = ""

    def to_connector_dict(self) -> Dict[str, Any]:
        """Return the dict passed to S3Connector(config)."""
        d: Dict[str, Any] = {
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "region": self.region,
            "bucket_names": self.bucket_names,
            "prefix": self.prefix,
        }
        if self.endpoint_url:
            d["endpoint_url"] = self.endpoint_url
        return d


# ---------------------------------------------------------------------------
# source_uri derivation
# ---------------------------------------------------------------------------

def derive_source_uri(file_id: str) -> str:
    """Derive the stable source_uri from an S3 composite file ID.

    Input:  "{bucket}::{key}"  (format produced by S3Connector.list_files())
    Output: "s3://{bucket}/{key}"

    This is the canonical source_uri for all S3-originated documents.
    It is stable across re-fetches of the same object and changes only
    if the object is renamed or moved (W-C1.4 rename semantics).

    Raises:
        ValueError: If file_id does not contain the "::" separator.
    """
    bucket, key = _split_file_id(file_id)
    return f"s3://{bucket}/{key}"


# ---------------------------------------------------------------------------
# Format inference
# ---------------------------------------------------------------------------

_MIME_TO_FORMAT = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "text/html": "html",
    "text/markdown": "markdown",
    "text/x-markdown": "markdown",
    "text/plain": "txt",
}

_EXT_TO_FORMAT = {
    ".pdf": "pdf",
    ".docx": "docx",
    ".html": "html",
    ".htm": "html",
    ".md": "markdown",
    ".markdown": "markdown",
    ".txt": "txt",
}


def _infer_format(filename: str, mimetype: str) -> Optional[str]:
    """Infer a knowledge-ingest document format string from filename/mimetype.

    Returns one of: "pdf", "docx", "html", "markdown", "txt", or None
    if the format cannot be determined. Unsupported formats are skipped by
    the wrapper (not passed to ingest_from_connector).
    """
    fmt = _MIME_TO_FORMAT.get(mimetype.split(";")[0].strip().lower())
    if fmt:
        return fmt
    ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return _EXT_TO_FORMAT.get(ext)


# ---------------------------------------------------------------------------
# Main wrapper function
# ---------------------------------------------------------------------------

async def sync_s3_bucket(config: Dict[str, Any]) -> List[SyncResult]:
    """Run a full pull-based sync of S3 objects into knowledge-ingest.

    This is the primary entry point for W-C2.1. It authenticates, iterates
    all objects, and calls ingest_from_connector() for each supported document.

    Args:
        config: Dict with S3 credentials and options (see module docstring).
                Must be loaded from managed secret storage by the caller.

    Returns:
        List of SyncResult — one per file that was attempted. Files with
        unsupported formats are skipped (logged as DEBUG) and not included.

    Raises:
        RuntimeError: If authentication fails — no files are fetched.
        ValueError:   If required credentials are missing from config.
    """
    scope: str = config.get("scope", "")
    owner: str = config.get("owner", "")

    # Build the connector config dict (excludes wrapper-only keys)
    connector_config: Dict[str, Any] = {
        k: v for k, v in config.items()
        if k not in ("scope", "owner")
    }

    connector = S3Connector(connector_config)

    # Step 1 — Authenticate. Fail closed: no files fetched on auth failure.
    authenticated = await connector.authenticate()
    if not authenticated:
        raise RuntimeError(
            f"S3 authentication failed for connection_id={config.get('connection_id', 'default')}. "
            "Check access_key / secret_key and bucket permissions."
        )

    # Step 2 — List all objects.
    result = await connector.list_files()
    files = result.get("files", [])
    logger.info(
        "s3_wrapper: discovered %d object(s) connector_module=%s",
        len(files), CONNECTOR_MODULE,
    )

    results: List[SyncResult] = []

    # Step 3 — For each object, derive identity, fetch content, ingest.
    for file_meta in files:
        file_id: str = file_meta["id"]
        filename: str = file_meta.get("name", "")

        # Derive stable source_uri (s3://{bucket}/{key})
        try:
            source_uri = derive_source_uri(file_id)
        except ValueError as exc:
            logger.warning(
                "s3_wrapper: skipping file_id=%r — cannot derive source_uri: %s",
                file_id, exc,
            )
            continue

        # Fetch raw content
        try:
            doc = await connector.get_file_content(file_id)
        except Exception as exc:
            logger.error(
                "s3_wrapper: failed to fetch content for source_uri=%s: %s",
                source_uri, type(exc).__name__,
            )
            # Record a synthetic fetch_failed result (no adapter call on content error)
            results.append(SyncResult(
                task_id="",
                verdict="fetch_failed",
                error_message=f"Content fetch error: {type(exc).__name__}: {exc}",
            ))
            continue

        # Infer document format — skip unsupported types
        doc_format = _infer_format(doc.filename, doc.mimetype)
        if doc_format is None:
            logger.debug(
                "s3_wrapper: skipping source_uri=%s — unsupported format "
                "(filename=%r mimetype=%r)",
                source_uri, doc.filename, doc.mimetype,
            )
            continue

        # Derive source_revision from modified_time (ISO 8601 UTC string)
        source_revision: str = doc.modified_time.isoformat()

        # Construct ConnectorEvent
        event = ConnectorEvent(
            connector_module=CONNECTOR_MODULE,
            scope=scope,
            source_uri=source_uri,
            filename=doc.filename or filename,
            format=doc_format,
            raw_bytes=doc.content,
            source_revision=source_revision,
            owner=owner,
            metadata=doc.metadata,
        )

        logger.debug(
            "s3_wrapper: ingesting source_uri=%s filename=%r format=%s",
            source_uri, event.filename, doc_format,
        )

        # Step 4 — Hand off to adapter. All W-C1 invariants enforced there:
        # - scope registry validation (W-C1.9)
        # - source_hash dedup / skipped_unchanged (W-C1.10)
        # - sync verdict independence (W-C1.8)
        # - provenance immutability (W-C1.6)
        sync_result = ingest_from_connector(event)
        results.append(sync_result)

        logger.info(
            "s3_wrapper: source_uri=%s verdict=%s task=%s",
            source_uri, sync_result.verdict, sync_result.task_id,
        )

    return results
