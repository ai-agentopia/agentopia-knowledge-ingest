"""OneDrive connector — adapted from OpenRAG for knowledge-ingest.

Source provenance
-----------------
Derived from ``langflow-ai/openrag`` (Apache 2.0 license).
Original paths:
  ``src/connectors/onedrive/connector.py``
  ``src/connectors/onedrive/oauth.py``
Adaptation date: 2026-04-12 (W-C2.4, knowledge-ingest#40)

What was changed relative to the OpenRAG originals
---------------------------------------------------
- Merged oauth.py credential logic directly into ``_MSALTokenCache`` in this module
  to eliminate the OpenRAG internal package dependency (``from .oauth import ...``).
- Removed browser OAuth flow methods and all interactive auth flows.
  Server-side pull sync only (#40).
- Removed all webhook methods: ``setup_subscription``, ``handle_webhook``,
  ``cleanup_subscription``, ``extract_webhook_channel_id``.
  Webhook support is explicitly out of scope for W-C2.4 (#40).
- Removed OpenRAG-internal ``utils.encryption`` dependency. Token persistence
  uses MSAL SerializableTokenCache with atomic file write (write to temp,
  then os.replace) — same pattern as openrag_gdrive.py.
- Removed OpenRAG-internal ``utils.logging_config`` dependency. Standard
  ``logging`` used instead.
- Removed ACL extraction. ACL is OpenRAG-internal; knowledge-ingest does not use it.
- Removed OpenRAG ``connection_manager`` dependency entirely.
- ``ConnectorDocument`` and ``BaseConnector`` now imported from ``openrag_base``.
- driveId resolution (``resolve_default_drive_id()``) is surfaced as a public
  method called by the wrapper per the W-C2.4 identity contract.
- ``list_files()`` uses Graph API search (``/me/drive/root/search(q='')``) to
  enumerate all file items recursively; folders are excluded by checking for
  the ``"file"`` key in the Graph item response.

What was NOT changed
--------------------
- OAuth authority URL construction: ``https://login.microsoftonline.com/{tenant_id}``.
- Graph API scopes: ``https://graph.microsoft.com/.default``.
- ``list_files()`` returns the same stripped-dict format as other OpenRAG connectors:
  ``{id, name, modified, mimeType, size}``.
- ``get_file_content()`` returns a ``ConnectorDocument`` with ``content`` as bytes.

Token persistence contract (W-C2.4)
-------------------------------------
If a token refresh occurs during ``acquire_token_silent()``, the updated MSAL
SerializableTokenCache is atomically persisted to the token file BEFORE the
caller proceeds. If persistence fails, a ``RuntimeError`` is raised and the
entire sync is aborted (fail closed).

**K8s Secret / Vault write-back is NOT implemented.** The sole supported token
persistence layer is a writable token_file path on disk (e.g. mounted from a
K8s Secret as a file or a PVC path). Operators must ensure the token_file path
is writable. If the path is unwritable after a cache state change, the sync
aborts (fail closed) — no files are ingested with a potentially stale token.

The operator must run an initial OAuth authorization flow (out-of-band, using
device code or auth code grant) to populate the token_file before first use.

Secret handling contract (enforced by callers)
-----------------------------------------------
Credentials are passed via the ``config`` dict only. This module does NOT read
``MICROSOFT_GRAPH_OAUTH_CLIENT_ID`` / ``MICROSOFT_GRAPH_OAUTH_CLIENT_SECRET``
from env vars directly. Callers (``onedrive_wrapper.py``) load from managed
secret storage (K8s Secret, Vault, or equivalent) and inject into config.

client_id, client_secret, and access tokens must NEVER appear in log output
at any log level. Only non-secret field names (filename, source_uri) are logged.
"""

import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import msal
import requests as _req_lib

from connectors.openrag_base import BaseConnector, ConnectorDocument

logger = logging.getLogger(__name__)

_GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Graph scopes for delegated access.
# ".default" works for both delegated (cached refresh token) and client
# credentials flows, letting MSAL negotiate the token type from the cache.
_GRAPH_SCOPES = ["https://graph.microsoft.com/.default"]

# MIME type → knowledge-ingest format string.
# Only formats the ingest normalizer actually supports.
# xlsx and pptx are intentionally absent — no OneDrive export like GDrive.
_MIME_TO_FORMAT: Dict[str, str] = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "text/html": "html",
    "text/markdown": "markdown",
    "text/x-markdown": "markdown",
    "text/plain": "txt",
}


# ---------------------------------------------------------------------------
# MSAL token cache — atomic file persistence, no browser flow
# ---------------------------------------------------------------------------

class _MSALTokenCache:
    """MSAL SerializableTokenCache with atomic file persistence.

    Supports loading and persisting a pre-populated MSAL token cache only.
    No browser or interactive OAuth flow is initiated here.

    Token file path is the sole persistence layer.  K8s Secret / Vault
    write-back is NOT implemented (see module docstring for the supported
    deployment contract).
    """

    def __init__(self, token_file: str) -> None:
        self.token_file = token_file
        self.cache = msal.SerializableTokenCache()

    def load(self) -> None:
        """Load the token cache from the token file.

        Raises:
            RuntimeError: If the file is missing or unreadable. The caller
                          must treat this as an auth failure (fail closed).
        """
        if not os.path.exists(self.token_file):
            raise RuntimeError(
                f"onedrive_oauth: token file not found: {self.token_file}. "
                "Run the initial OAuth authorization flow to populate the token cache."
            )
        try:
            with open(self.token_file, "r", encoding="utf-8") as fh:
                self.cache.deserialize(fh.read())
        except Exception as exc:
            raise RuntimeError(
                f"onedrive_oauth: failed to read token file {self.token_file}: "
                f"{type(exc).__name__}"
            ) from exc

    def save_if_changed(self) -> None:
        """Atomically persist the cache to the token file if state has changed.

        Uses write-to-temp + os.replace to avoid partial writes.

        Raises:
            RuntimeError: If the write fails. Caller must abort sync (fail closed)
                          — do not proceed with a token that cannot be persisted.
        """
        if not self.cache.has_state_changed:
            return

        serialized = self.cache.serialize()
        token_dir = os.path.dirname(os.path.abspath(self.token_file))
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(dir=token_dir, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(serialized)
            except Exception:
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise
            os.replace(tmp_path, self.token_file)
            tmp_path = None
        except Exception as exc:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            raise RuntimeError(
                f"onedrive_oauth: failed to persist token cache to {self.token_file}: "
                f"{type(exc).__name__}. Aborting sync (fail closed)."
            ) from exc


# ---------------------------------------------------------------------------
# OneDrive connector
# ---------------------------------------------------------------------------

class OneDriveConnector(BaseConnector):
    """OneDrive connector for knowledge-ingest pull-based sync.

    Uses MSAL ConfidentialClientApplication with a pre-populated
    SerializableTokenCache (no interactive/browser flow). The operator must run
    the initial OAuth authorization flow out-of-band to populate token_file.

    Config dict keys
    ----------------
    client_id     (str)  : Azure AD / Entra application client ID (required).
    client_secret (str)  : Application client secret (required).
    tenant_id     (str)  : Azure AD / Entra tenant ID (required).
    token_file    (str)  : Path to MSAL SerializableTokenCache JSON (required).
    scope         (str)  : Target knowledge scope (passed through by wrapper).
    owner         (str)  : Optional actor identifier for audit log.
    """

    CONNECTOR_NAME = "onedrive"

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self._access_token: Optional[str] = None
        self._default_drive_id: Optional[str] = None
        self._session: Optional[_req_lib.Session] = None

    def _make_session(self) -> _req_lib.Session:
        """Build a requests.Session with the bearer token pre-set."""
        sess = _req_lib.Session()
        sess.headers.update({
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        })
        return sess

    async def authenticate(self) -> bool:
        """Acquire an access token silently from the MSAL token cache.

        Loads the persisted cache, acquires a token via MSAL (triggering a
        refresh if the cached access token is expired), persists the updated
        cache atomically, and stores the access token for subsequent Graph API
        calls.

        Raises:
            RuntimeError: On missing/unreadable token file, no cached account,
                          failed token acquisition, or failed cache persistence.
                          In all failure cases no files are fetched (fail closed).
        """
        client_id = self.config.get("client_id", "")
        client_secret = self.config.get("client_secret", "")
        tenant_id = self.config.get("tenant_id", "")
        token_file = self.config.get("token_file", "")

        if not client_id:
            raise RuntimeError("onedrive_connector: 'client_id' is required in config")
        if not client_secret:
            raise RuntimeError("onedrive_connector: 'client_secret' is required in config")
        if not tenant_id:
            raise RuntimeError("onedrive_connector: 'tenant_id' is required in config")
        if not token_file:
            raise RuntimeError("onedrive_connector: 'token_file' is required in config")

        # Load persisted token cache — raises RuntimeError if missing/unreadable
        token_cache = _MSALTokenCache(token_file)
        token_cache.load()

        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id,
            client_credential=client_secret,
            authority=authority,
            token_cache=token_cache.cache,
        )

        accounts = app.get_accounts()
        if not accounts:
            raise RuntimeError(
                "onedrive_connector: no cached MSAL account found in token file. "
                "Run the initial OAuth authorization flow to populate the token cache."
            )

        result = app.acquire_token_silent(_GRAPH_SCOPES, account=accounts[0])

        # Persist updated cache BEFORE using the token — fail closed on write error.
        # This prevents operating with a token that cannot be persisted (stale file).
        token_cache.save_if_changed()

        if not result or "access_token" not in result:
            error = (result or {}).get("error_description", "unknown error")
            raise RuntimeError(
                f"onedrive_connector: failed to acquire access token silently: {error}. "
                "The cached token may be expired. Re-run the initial OAuth flow."
            )

        self._access_token = result["access_token"]
        self._session = self._make_session()
        self._authenticated = True
        logger.info("onedrive_connector: authenticated successfully connector=%s", self.CONNECTOR_NAME)
        return True

    async def resolve_default_drive_id(self) -> str:
        """Resolve the authenticated user's default OneDrive drive ID.

        Calls ``GET /me/drive?$select=id`` once per instance and caches the
        result. The driveId is required to construct the canonical
        ``onedrive://{driveId}/{itemId}`` source_uri when ``list_files()``
        returns plain itemIds (Case 1, no ``!`` separator).

        Returns:
            driveId string (opaque, assigned by Microsoft Graph).

        Raises:
            RuntimeError: If not authenticated or if the API call fails.
                          Sync is aborted (fail closed).
        """
        if self._default_drive_id is not None:
            return self._default_drive_id

        if not self._authenticated or not self._session:
            raise RuntimeError("onedrive_connector: resolve_default_drive_id called before authenticate()")

        try:
            resp = self._session.get(
                f"{_GRAPH_BASE}/me/drive",
                params={"$select": "id"},
                timeout=30,
            )
            resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(
                "onedrive_connector: failed to resolve default driveId via /me/drive: "
                f"{type(exc).__name__}. Aborting sync (fail closed)."
            ) from exc

        data = resp.json()
        drive_id = data.get("id", "")
        if not drive_id:
            raise RuntimeError(
                "onedrive_connector: /me/drive response missing 'id' field. "
                "Aborting sync (fail closed)."
            )

        self._default_drive_id = drive_id
        # driveId itself is not secret, but not useful to log — only log that resolution succeeded.
        logger.debug("onedrive_connector: default driveId resolved successfully")
        return drive_id

    async def list_files(
        self,
        page_token: Optional[str] = None,
        max_files: Optional[int] = None,
    ) -> Dict[str, Any]:
        """List all files in the user's OneDrive via Graph API search.

        Uses ``/me/drive/root/search(q='')`` to recursively enumerate all file
        items from the drive root. Folder items are excluded (items without a
        ``"file"`` key in the Graph response).

        Args:
            page_token:  If provided, must be a ``@odata.nextLink`` URL from a
                         previous call. Resumes pagination from that point.
            max_files:   If provided, stop after collecting this many files.

        Returns:
            ``{"files": [...], "next_page_token": str | None}``

            Each file dict:
            ``{"id": str, "name": str, "modified": str, "mimeType": str, "size": int}``

        Raises:
            RuntimeError: If not authenticated or if the Graph API call fails.
        """
        if not self._authenticated or not self._session:
            raise RuntimeError("onedrive_connector: list_files called before authenticate()")

        url = page_token or (
            f"{_GRAPH_BASE}/me/drive/root/search(q='')"
            "?$select=id,name,file,lastModifiedDateTime,size&$top=200"
        )

        try:
            resp = self._session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(
                f"onedrive_connector: list_files failed: {type(exc).__name__}"
            ) from exc

        data = resp.json()
        raw_items = data.get("value", [])
        next_link = data.get("@odata.nextLink")

        files: List[Dict[str, Any]] = []
        for item in raw_items:
            # Only include file items — skip folders and other non-file objects
            if "file" not in item:
                continue
            mime = item.get("file", {}).get("mimeType", "")
            files.append({
                "id": item["id"],
                "name": item.get("name", ""),
                "modified": item.get("lastModifiedDateTime", ""),
                "mimeType": mime,
                "size": item.get("size", 0),
            })
            if max_files and len(files) >= max_files:
                next_link = None
                break

        return {"files": files, "next_page_token": next_link}

    async def get_file_content(self, file_id: str) -> ConnectorDocument:
        """Fetch metadata and raw bytes for the given plain itemId.

        Args:
            file_id: Plain itemId (NOT the driveId!itemId composite form).
                     The wrapper resolves the composite form before calling this.

        Returns:
            ``ConnectorDocument`` with ``content`` as raw bytes.

        Raises:
            RuntimeError: If not authenticated or if the metadata/content API
                          calls fail.
        """
        if not self._authenticated or not self._session:
            raise RuntimeError("onedrive_connector: get_file_content called before authenticate()")

        # Metadata — needed for filename, MIME type, and timestamps
        try:
            meta_resp = self._session.get(
                f"{_GRAPH_BASE}/me/drive/items/{file_id}",
                params={"$select": "id,name,file,lastModifiedDateTime,createdDateTime,size"},
                timeout=30,
            )
            meta_resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(
                f"onedrive_connector: metadata fetch failed for item {file_id!r}: "
                f"{type(exc).__name__}"
            ) from exc

        meta = meta_resp.json()
        mime = meta.get("file", {}).get("mimeType", "application/octet-stream")

        # Content download — follows redirects (Graph returns a 302 to CDN)
        try:
            content_resp = self._session.get(
                f"{_GRAPH_BASE}/me/drive/items/{file_id}/content",
                timeout=60,
                allow_redirects=True,
            )
            content_resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(
                f"onedrive_connector: content download failed for item {file_id!r}: "
                f"{type(exc).__name__}"
            ) from exc

        def _parse_dt(s: str) -> datetime:
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return datetime.now(timezone.utc)

        return ConnectorDocument(
            id=file_id,
            filename=meta.get("name", ""),
            mimetype=mime,
            content=content_resp.content,
            source_url=f"{_GRAPH_BASE}/me/drive/items/{file_id}",
            modified_time=_parse_dt(meta.get("lastModifiedDateTime", "")),
            created_time=_parse_dt(meta.get("createdDateTime", "")),
            metadata={"onedrive_item_id": file_id},
        )
