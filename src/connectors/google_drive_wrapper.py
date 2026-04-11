"""W-C2.2: Google Drive connector wrapper — bridges GoogleDriveConnector into ingest_from_connector().

Issue:  knowledge-ingest#38
ADR:    docs/adr/002-connector-precondition-audit.md (GATE-1)
Source: langflow-ai/openrag src/connectors/google_drive/ (see openrag_gdrive.py)

Architecture
------------
This module is a pure wrapper. It:

  1. Accepts a config dict (credentials from managed secret storage).
  2. Instantiates GoogleDriveConnector and authenticates.
  3. Iterates list_files() + get_file_content() for each discovered file.
  4. Derives the stable source_uri and source_revision per W-C1 contracts.
  5. Constructs a ConnectorEvent and calls ingest_from_connector().

It does NOT:
  - Write to documents, connector_sync_tasks, or ingest_jobs tables directly.
  - Call the normalizer, extractor, or orchestrator directly.
  - Bypass the adapter's scope registry validation (W-C1.9).
  - Bypass the adapter's source_hash dedup (W-C1.10).
  - Implement browser OAuth flow (operator must set up credentials out of band).
  - Implement webhook / incremental subscription support (out of scope for #38).
  - Accept credentials as positional arguments.
  - Log secret values (client_id, client_secret, refresh_token) at any log level.

Identity contract (W-C1.2, W-C1.3, #38)
-----------------------------------------
  connector_module  = "google_drive"
  source_uri        = "gdrive://{fileId}"
                      Google Drive fileId is a stable, opaque identifier assigned
                      by Drive. It is stable across file renames, folder moves,
                      and ownership transfers. It changes only if the file is
                      deleted and re-created.
                      This ensures rename stability: two syncs of the same file
                      with different filenames produce the same document_id.
  source_revision   = modifiedTime.isoformat()
                      RFC 3339 modifiedTime returned by the Drive API, serialised
                      to ISO 8601 UTC. Written once on INSERT (W-C1.6 provenance
                      immutability). This is equivalent to ETag for change detection.

Token refresh write-back contract (W-C2.2)
-------------------------------------------
If a credential refresh occurs during authenticate(), the updated token is
atomically persisted to disk BEFORE the sync proceeds. If persistence fails,
a RuntimeError is raised and no files are ingested (fail closed).
See _GDriveOAuth.refresh_if_needed() and save_credentials() in openrag_gdrive.py.

Secret handling contract
------------------------
The caller (operator script, scheduled job, or future HTTP handler) is
responsible for loading credentials from managed secret storage (K8s Secret,
Vault, or equivalent) and passing them in the config dict. This module accepts
config only via the `config` parameter.

The config dict keys are:
  client_id     (str)       : OAuth2 client ID (required).
  client_secret (str)       : OAuth2 client secret (required).
  token_file    (str)       : Path to persisted OAuth token JSON (required).
  folder_ids    (list[str]) : Google Drive folder IDs to sync. Empty = all My Drive.
  file_ids      (list[str]) : Specific file IDs (overrides folder_ids if set).
  scope         (str)       : Target knowledge scope (fallback when no scope mapping matches).
  owner         (str)       : Optional actor identifier for audit log.

Webhook support
---------------
Not implemented. Google Drive push notifications (webhooks / Changes API) are
out of scope for W-C2.2 (#38).
"""

import logging
from typing import Any, Dict, List, Optional

from connectors.adapter import ConnectorEvent, SyncResult, ingest_from_connector
from connectors.openrag_gdrive import GoogleDriveConnector, _DIRECT_MIME_TYPES, _EXPORT_FORMATS

logger = logging.getLogger(__name__)

CONNECTOR_MODULE = "google_drive"

# MIME types natively supported by knowledge-ingest (after export for Workspace docs)
_EXPORTED_MIME_TO_FORMAT: Dict[str, str] = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
}

# Combined format map: direct download formats + exported Workspace formats
_MIME_TO_FORMAT: Dict[str, str] = {**_DIRECT_MIME_TYPES, **_EXPORTED_MIME_TO_FORMAT}


# ---------------------------------------------------------------------------
# source_uri derivation
# ---------------------------------------------------------------------------

def derive_source_uri(file_id: str) -> str:
    """Derive the stable source_uri from a Google Drive file ID.

    Input:  Google Drive fileId (e.g. "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms")
    Output: "gdrive://{fileId}"

    This is the canonical source_uri for all Google Drive-originated documents.
    It is stable across file renames, folder moves, and ownership transfers.

    Raises:
        ValueError: If file_id is empty.
    """
    if not file_id or not file_id.strip():
        raise ValueError("derive_source_uri: file_id must not be empty")
    return f"gdrive://{file_id.strip()}"


# ---------------------------------------------------------------------------
# Format inference
# ---------------------------------------------------------------------------

def _infer_format(mimetype: str) -> Optional[str]:
    """Map a MIME type to a knowledge-ingest format string.

    Returns one of: "pdf", "docx", "html", "markdown", "txt", "xlsx", "pptx",
    or None if the MIME type is not supported.
    """
    return _MIME_TO_FORMAT.get(mimetype.split(";")[0].strip().lower())


# ---------------------------------------------------------------------------
# Main wrapper function
# ---------------------------------------------------------------------------

async def sync_gdrive(config: Dict[str, Any]) -> List[SyncResult]:
    """Run a full pull-based sync of Google Drive files into knowledge-ingest.

    This is the primary entry point for W-C2.2. It authenticates, iterates
    all accessible files, and calls ingest_from_connector() for each supported
    document.

    Args:
        config: Dict with Google Drive credentials and options (see module docstring).
                Must be loaded from managed secret storage by the caller.

    Returns:
        List of SyncResult — one per file that was attempted. Files with
        unsupported formats are skipped (logged as DEBUG) and not included.

    Raises:
        RuntimeError: If authentication fails, or if token refresh occurs and
                      token persistence fails (fail closed). No files are fetched.
        ValueError:   If required credentials are missing from config.
    """
    scope: str = config.get("scope", "")
    owner: str = config.get("owner", "")

    if not config.get("client_id") or not config.get("client_secret"):
        raise ValueError(
            "google_drive_wrapper: config must include 'client_id' and 'client_secret'"
        )
    if not config.get("token_file"):
        raise ValueError(
            "google_drive_wrapper: config must include 'token_file' (path to OAuth token JSON)"
        )

    connector = GoogleDriveConnector(config)

    # Step 1 — Authenticate. Fail closed: no files fetched on auth failure.
    # May raise RuntimeError on token refresh failure or token persistence failure.
    authenticated = await connector.authenticate()
    if not authenticated:
        raise RuntimeError(
            "google_drive_wrapper: Google Drive authentication failed. "
            "Ensure OAuth credentials are configured and the token file is valid. "
            "Run the OAuth setup flow to obtain a refresh token."
        )

    # Step 2 — List all supported files.
    list_result = await connector.list_files()
    files = list_result.get("files", [])
    logger.info(
        "gdrive_wrapper: discovered %d file(s) connector_module=%s",
        len(files), CONNECTOR_MODULE,
    )

    results: List[SyncResult] = []

    # Step 3 — For each file, derive identity, fetch content, ingest.
    for file_meta in files:
        file_id: str = file_meta.get("id", "")
        filename: str = file_meta.get("name", "")

        # Derive stable source_uri ("gdrive://{fileId}")
        try:
            source_uri = derive_source_uri(file_id)
        except ValueError as exc:
            logger.warning(
                "gdrive_wrapper: skipping file_id=%r — cannot derive source_uri: %s",
                file_id, exc,
            )
            continue

        # Fetch raw content + metadata
        try:
            doc = await connector.get_file_content(file_id)
        except Exception as exc:
            logger.error(
                "gdrive_wrapper: failed to fetch content for source_uri=%s: %s",
                source_uri, type(exc).__name__,
            )
            results.append(SyncResult(
                task_id="",
                verdict="fetch_failed",
                error_message=f"Content fetch error: {type(exc).__name__}: {exc}",
                source_uri=source_uri,
            ))
            continue

        # Infer document format — skip unsupported MIME types
        doc_format = _infer_format(doc.mimetype)
        if doc_format is None:
            logger.debug(
                "gdrive_wrapper: skipping source_uri=%s — unsupported format "
                "(mimetype=%r)",
                source_uri, doc.mimetype,
            )
            continue

        # source_revision = modifiedTime ISO 8601 (W-C1.6 provenance immutability)
        source_revision: str = doc.modified_time.isoformat()

        # Build ConnectorEvent
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
            "gdrive_wrapper: ingesting source_uri=%s filename=%r format=%s",
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
            "gdrive_wrapper: source_uri=%s verdict=%s task=%s",
            source_uri, sync_result.verdict, sync_result.task_id,
        )

    return results
