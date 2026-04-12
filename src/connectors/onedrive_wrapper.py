"""W-C2.4: OneDrive connector wrapper — bridges OneDriveConnector into ingest_from_connector().

Issue:  knowledge-ingest#40
ADR:    docs/adr/002-connector-precondition-audit.md (GATE-1)
Source: langflow-ai/openrag src/connectors/onedrive/ (see openrag_onedrive.py)

Architecture
------------
This module is a pure wrapper. It:

  1. Accepts a config dict (credentials from managed secret storage).
  2. Instantiates OneDriveConnector and authenticates (fail closed on error).
  3. Resolves the default driveId via Graph API (fail closed on error).
  4. Paginates list_files() to enumerate all files.
  5. For each file, derives the stable source_uri and item_id using the three
     derivation rules from the W-C2.4 identity contract (Cases 1–3).
  6. Fetches file content via get_file_content(item_id).
  7. Constructs a ConnectorEvent and calls ingest_from_connector().

It does NOT:
  - Write to documents, connector_sync_tasks, or ingest_jobs tables directly.
  - Call the normalizer, extractor, or orchestrator directly.
  - Bypass the adapter's scope registry validation (W-C1.9).
  - Bypass the adapter's source_hash dedup (W-C1.10).
  - Implement browser OAuth flow (operator must set up credentials out of band).
  - Implement webhook / subscription support (out of scope for #40).
  - Log any token value at any log level.
  - Accept credentials as positional arguments.

Identity contract (W-C1.2, W-C1.3, #40)
-----------------------------------------
  connector_module  = "onedrive"
  source_uri        = "onedrive://{driveId}/{itemId}"
                      OneDrive itemId is stable across file renames within
                      the same drive. Cross-drive moves produce a new driveId
                      and therefore a new document_id (as expected).
  source_revision   = lastModifiedDateTime ISO 8601 from Microsoft Graph.
                      Stored once on INSERT (W-C1.6 provenance immutability).
  Sharing IDs:      Items whose itemId starts with "s" (plain form) or whose
                    right-hand component after "!" starts with "s" (composite
                    form) are sharing IDs. They are NOT stable locators — skip
                    and log WARNING. ingest_from_connector() is not called.

Token persistence contract (W-C2.4)
-------------------------------------
Token refresh is handled by openrag_onedrive.py (_MSALTokenCache). If refresh
occurs and file persistence fails, authenticate() raises RuntimeError (fail
closed). This wrapper propagates that error — no files are ingested.

Supported token persistence: writable token_file on disk only.
K8s Secret / Vault write-back is NOT implemented.

Config dict keys
----------------
  client_id     (str)       : Azure AD / Entra application client ID (required).
                              Load from managed secret storage — do not hardcode.
  client_secret (str)       : Application client secret (required).
                              Load from managed secret storage — do not hardcode.
  tenant_id     (str)       : Azure AD / Entra tenant ID (required).
  token_file    (str)       : Path to MSAL SerializableTokenCache JSON (required).
                              Must be writable. Operator populates via initial
                              OAuth flow before first sync.
  scope         (str)       : Target knowledge scope.
  owner         (str)       : Optional actor identifier for audit log.

Webhook support
---------------
Not implemented. Microsoft Graph change notifications (OneDrive subscriptions)
are out of scope for W-C2.4 (#40). Pull-based sync only.

Personal OneDrive note
----------------------
Microsoft Graph change notifications may not be available for personal (MSA)
accounts. This wrapper targets organizational OneDrive (work/school accounts).
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from connectors.adapter import ConnectorEvent, SyncResult, ingest_from_connector
from connectors.openrag_onedrive import OneDriveConnector, _MIME_TO_FORMAT

logger = logging.getLogger(__name__)

CONNECTOR_MODULE = "onedrive"


# ---------------------------------------------------------------------------
# Identity derivation
# ---------------------------------------------------------------------------

def derive_source_uri_and_item(
    file_id: str,
    default_drive_id: str,
) -> Optional[Tuple[str, str]]:
    """Derive (source_uri, item_id) from a Graph API item ID.

    Implements the three cases from the W-C2.4 identity contract:

    Case 1 — Plain itemId (no "!" separator):
        Use default_drive_id resolved at init via /me/drive.
        source_uri = "onedrive://{default_drive_id}/{file_id}"
        item_id    = file_id

    Case 2 — Composite "driveId!itemId":
        Split on "!"; use the embedded driveId directly.
        source_uri = "onedrive://{drive_id}/{item_id}"
        item_id    = right-hand part

    Case 3 — Sharing ID (plain "s..." or composite "driveId!s..."):
        Sharing IDs are ephemeral and NOT stable locators. Skip and log
        WARNING. Returns None — ingest_from_connector() is not called.

    Args:
        file_id:         Raw item ID from Graph API list_files() response.
        default_drive_id: Resolved driveId from /me/drive (used for Case 1).

    Returns:
        (source_uri, item_id) tuple, or None if the item should be skipped.
    """
    if "!" in file_id:
        drive_id, item_id = file_id.split("!", 1)
        if item_id.startswith("s"):
            logger.warning(
                "onedrive_wrapper: skipping sharing ID (composite form) file_id=%r — "
                "sharing IDs are not stable source_uri locators and must not be ingested",
                file_id,
            )
            return None
        return f"onedrive://{drive_id}/{item_id}", item_id

    # Plain itemId
    if file_id.startswith("s"):
        logger.warning(
            "onedrive_wrapper: skipping sharing ID (plain form) file_id=%r — "
            "sharing IDs are not stable source_uri locators and must not be ingested",
            file_id,
        )
        return None

    return f"onedrive://{default_drive_id}/{file_id}", file_id


# ---------------------------------------------------------------------------
# Format inference
# ---------------------------------------------------------------------------

def _infer_format(mimetype: str) -> Optional[str]:
    """Map a MIME type to a knowledge-ingest format string, or None if unsupported."""
    return _MIME_TO_FORMAT.get(mimetype.split(";")[0].strip().lower())


# ---------------------------------------------------------------------------
# Main wrapper function
# ---------------------------------------------------------------------------

async def sync_onedrive(config: Dict[str, Any]) -> List[SyncResult]:
    """Run a full pull-based sync of OneDrive files into knowledge-ingest.

    This is the primary entry point for W-C2.4. It authenticates, resolves
    the default driveId, enumerates all accessible files, and calls
    ingest_from_connector() for each supported document.

    Args:
        config: Dict with OneDrive/MSAL credentials and options (see module
                docstring). Must be loaded from managed secret storage by the
                caller. Do not hardcode credentials.

    Returns:
        List of SyncResult — one per file that was attempted. Files with
        unsupported MIME types or sharing IDs are skipped and not included.

    Raises:
        RuntimeError: If authentication fails, driveId resolution fails, or
                      token cache persistence fails (all fail closed — no files
                      are ingested in these cases).
        ValueError:   If required credentials are missing from config.
    """
    scope: str = config.get("scope", "")
    owner: str = config.get("owner", "")

    # Validate required credentials upfront
    for key in ("client_id", "client_secret", "tenant_id", "token_file"):
        if not config.get(key):
            raise ValueError(f"onedrive_wrapper: config must include '{key}'")

    connector = OneDriveConnector(config)

    # Step 1 — Authenticate (fail closed on any error)
    # RuntimeError raised by authenticate() propagates directly to caller.
    authenticated = await connector.authenticate()
    if not authenticated:
        raise RuntimeError(
            "onedrive_wrapper: OneDrive authentication returned False unexpectedly"
        )

    # Step 2 — Resolve default driveId (fail closed if Graph call fails)
    # Required for Case 1 source_uri derivation.
    default_drive_id = await connector.resolve_default_drive_id()

    # Step 3 — List all files (paginate until next_page_token is None)
    all_files: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        page = await connector.list_files(page_token=page_token)
        all_files.extend(page.get("files", []))
        page_token = page.get("next_page_token")
        if not page_token:
            break

    logger.info(
        "onedrive_wrapper: discovered %d file(s) connector_module=%s",
        len(all_files), CONNECTOR_MODULE,
    )

    results: List[SyncResult] = []

    # Step 4 — For each file: derive identity → fetch content → ingest
    for file_meta in all_files:
        file_id: str = file_meta.get("id", "")
        filename: str = file_meta.get("name", "")

        # Derive source_uri and plain item_id (None = sharing ID, skip)
        derived = derive_source_uri_and_item(file_id, default_drive_id)
        if derived is None:
            continue
        source_uri, item_id = derived

        # Fetch raw content via the plain item_id
        try:
            doc = await connector.get_file_content(item_id)
        except Exception as exc:
            logger.error(
                "onedrive_wrapper: failed to fetch content for source_uri=%s: %s",
                source_uri, type(exc).__name__,
            )
            results.append(SyncResult(
                task_id="",
                verdict="fetch_failed",
                error_message=f"Content fetch error: {type(exc).__name__}: {exc}",
                source_uri=source_uri,
            ))
            continue

        # Infer format — skip unsupported MIME types
        doc_format = _infer_format(doc.mimetype)
        if doc_format is None:
            logger.debug(
                "onedrive_wrapper: skipping source_uri=%s — unsupported mimetype=%r",
                source_uri, doc.mimetype,
            )
            continue

        # source_revision = lastModifiedDateTime ISO 8601 (W-C1.6 provenance immutability)
        # Prefer the value from list_files() metadata; fall back to ConnectorDocument.
        source_revision: str = (
            file_meta.get("modified", "") or doc.modified_time.isoformat()
        )

        # Build ConnectorEvent — carries all W-C1 identity fields
        event = ConnectorEvent(
            connector_module=CONNECTOR_MODULE,
            scope=scope,
            source_uri=source_uri,
            filename=doc.filename or filename,
            format=doc_format,
            raw_bytes=doc.content,
            source_revision=source_revision,
            owner=owner,
        )

        logger.debug(
            "onedrive_wrapper: ingesting source_uri=%s filename=%r format=%s",
            source_uri, event.filename, doc_format,
        )

        # Step 5 — Hand off to adapter. All W-C1 invariants enforced there:
        # - scope registry validation (W-C1.9)
        # - source_hash dedup / skipped_unchanged (W-C1.10)
        # - sync verdict independence (W-C1.8)
        # - provenance immutability (W-C1.6)
        sync_result = ingest_from_connector(event)
        results.append(sync_result)

        logger.info(
            "onedrive_wrapper: source_uri=%s verdict=%s task=%s",
            source_uri, sync_result.verdict, sync_result.task_id,
        )

    return results
