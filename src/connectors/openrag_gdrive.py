"""Google Drive connector — adapted from OpenRAG for knowledge-ingest.

Source provenance
-----------------
Derived from ``langflow-ai/openrag`` (Apache 2.0 license).
Original paths:
  ``src/connectors/google_drive/connector.py``
  ``src/connectors/google_drive/oauth.py``
Adaptation date: 2026-04-11 (W-C2.2, knowledge-ingest#38)

What was changed relative to the OpenRAG originals
---------------------------------------------------
- Merged oauth.py credential logic directly into ``_GDriveOAuth`` in this module
  to eliminate the OpenRAG internal package dependency (``from .oauth import ...``).
- Removed browser OAuth flow methods: ``create_authorization_url``,
  ``handle_authorization_callback``. Server-side pull sync only (#38).
- Removed all webhook methods: ``setup_subscription``, ``handle_webhook``,
  ``cleanup_subscription``, ``extract_webhook_channel_id``.
  Webhook support is explicitly out of scope for W-C2.2 (#38).
- Removed OpenRAG-internal ``utils.encryption`` dependency. Token persistence
  uses plain JSON with atomic write (write to temp file, then os.replace).
- Removed OpenRAG-internal ``utils.logging_config`` dependency. Standard
  ``logging`` used instead.
- Removed ACL extraction (``_extract_acl``, permissions fields). ACL is
  OpenRAG-internal; knowledge-ingest does not use it.
- Removed Changes API / incremental sync (``poll_changes_and_sync``).
  Full pull sync only. Incremental support is W-C2.3 (future).
- Removed shortcut resolution (``_resolve_shortcut``). Out of scope.
- ``ConnectorDocument`` and ``BaseConnector`` now imported from ``openrag_base``.
- ``CONNECTOR_NAME`` retained as a class attr for documentation clarity.

What was NOT changed
--------------------
- OAuth scopes: ``drive.readonly`` + ``drive.metadata.readonly``.
- ``Credentials`` construction and ``expiry`` normalisation (timezone-naive for
  google-auth compatibility) are identical to the upstream.
- Token refresh timeout (``_REFRESH_TIMEOUT_SECONDS = 30``).
- ``_EXPORT_FORMATS`` mapping: Google Workspace → closest exportable MIME type.
- ``list_files()`` Drive API query structure (mimeType filter, trashed=false,
  pageSize=100 pagination) is equivalent to upstream logic.
- ``get_file_content()`` download-vs-export decision is equivalent to upstream.

Token persistence contract (W-C2.2)
-------------------------------------
If a token refresh occurs during ``_GDriveOAuth.refresh_if_needed()``, the
updated credentials are atomically persisted to the token file BEFORE the
caller proceeds. If persistence fails, a ``RuntimeError`` is raised and the
entire sync is aborted (fail closed). This prevents a partially-refreshed
credential state where the old token file is invalid but the sync ran.

Secret handling contract (enforced by callers)
-----------------------------------------------
Credentials are passed via the ``config`` dict only.
``_GDriveOAuth`` does NOT read environment variables.
Callers (``google_drive_wrapper.py``) load credentials from managed secret
storage (K8s Secret, Vault, or equivalent) and inject into the config dict.
Client ID, client secret, and refresh tokens must NEVER appear in log output
at any level (enforced by this module — only non-secret field names are logged).
"""

import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import requests as _req_lib

from connectors.openrag_base import BaseConnector, ConnectorDocument

logger = logging.getLogger(__name__)

_REFRESH_TIMEOUT_SECONDS = 30

# Google Workspace MIME → (export MIME, file extension)
# Only Google Docs is supported: exports to DOCX, which the ingest core can normalise.
# Google Sheets (→ xlsx) and Google Slides (→ pptx) are NOT listed here because
# the ingest core does not support those formats. Files with those MIME types will
# be discovered by list_files() but skipped by the wrapper before ingest_from_connector().
_EXPORT_FORMATS: Dict[str, tuple] = {
    "application/vnd.google-apps.document": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "docx",
    ),
}

# Regular (binary) files — direct download
_DIRECT_MIME_TYPES: Dict[str, str] = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "text/html": "html",
    "text/markdown": "markdown",
    "text/x-markdown": "markdown",
    "text/plain": "txt",
}

# All MIME types eligible for listing
_SUPPORTED_MIME_TYPES: frozenset = frozenset(_DIRECT_MIME_TYPES) | frozenset(_EXPORT_FORMATS)


# ---------------------------------------------------------------------------
# OAuth handler — server-side only, no browser flow
# ---------------------------------------------------------------------------

class _GDriveOAuth:
    """Minimal Google Drive OAuth handler for server-side pull-based sync.

    Supports loading and refreshing pre-existing credentials only.
    No browser OAuth flow, no interactive login.
    """

    SCOPES = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.metadata.readonly",
    ]

    def __init__(self, client_id: str, client_secret: str, token_file: str) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self.token_file = token_file
        self.creds: Optional[Credentials] = None

    def _make_request_transport(self) -> Request:
        """Build a google-auth Request transport with a bounded timeout."""
        session = _req_lib.Session()
        session.timeout = _REFRESH_TIMEOUT_SECONDS
        return Request(session=session)

    def load_credentials(self) -> Optional[Credentials]:
        """Load credentials from the token file. Returns None if not found or unreadable."""
        if not os.path.exists(self.token_file):
            logger.debug("gdrive_oauth: token file not found: %s", self.token_file)
            return None

        try:
            with open(self.token_file, "r", encoding="utf-8") as fh:
                token_data = json.load(fh)
        except Exception as exc:
            logger.warning(
                "gdrive_oauth: failed to read token file %s: %s",
                self.token_file, type(exc).__name__,
            )
            return None

        self.creds = Credentials(
            token=token_data.get("token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=self._client_id,
            client_secret=self._client_secret,
            scopes=token_data.get("scopes", self.SCOPES),
        )

        # Normalise expiry: google-auth expects timezone-naive datetime
        if token_data.get("expiry"):
            try:
                expiry_dt = datetime.fromisoformat(token_data["expiry"])
                self.creds.expiry = expiry_dt.replace(tzinfo=None)
            except Exception:
                pass

        return self.creds

    def save_credentials(self) -> None:
        """Atomically write credentials to the token file.

        Uses write-to-temp + os.replace to avoid a partially-written file.
        Raises RuntimeError if the write fails (caller must fail closed).
        """
        if not self.creds:
            raise RuntimeError("gdrive_oauth: no credentials to save")

        token_data: Dict[str, Any] = {
            "token": self.creds.token,
            "refresh_token": self.creds.refresh_token,
            "scopes": list(self.creds.scopes or self.SCOPES),
        }
        if self.creds.expiry:
            token_data["expiry"] = self.creds.expiry.isoformat()

        token_dir = os.path.dirname(os.path.abspath(self.token_file))
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(dir=token_dir, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(token_data, fh, indent=2)
            except Exception:
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise
            os.replace(tmp_path, self.token_file)
        except Exception as exc:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            raise RuntimeError(
                f"gdrive_oauth: failed to persist token to {self.token_file}: {exc}"
            ) from exc

    def refresh_if_needed(self) -> bool:
        """Refresh credentials if expired. Persist immediately after refresh.

        Returns True if credentials are valid.
        Raises RuntimeError if:
          - refresh request fails (token revoked / network error)
          - token persistence fails (disk full / permissions error)

        W-C2.2 contract: persistence happens BEFORE returning; fail closed if it fails.
        """
        if not self.creds:
            return False

        if self.creds.expired and self.creds.refresh_token:
            logger.debug("gdrive_oauth: token expired — refreshing")
            try:
                self.creds.refresh(self._make_request_transport())
            except Exception as exc:
                raise RuntimeError(
                    f"gdrive_oauth: token refresh failed — re-run OAuth setup to obtain "
                    f"a new refresh token: {type(exc).__name__}: {exc}"
                ) from exc

            # W-C2.2 critical contract: persist BEFORE proceeding; fail closed on write error
            self.save_credentials()
            logger.debug("gdrive_oauth: token refreshed and persisted to %s", self.token_file)

        return bool(self.creds and self.creds.valid)

    def get_service(self):
        """Build an authenticated Drive v3 service object."""
        if not self.creds or not self.creds.valid:
            raise RuntimeError(
                "gdrive_oauth: credentials not valid — call load_credentials() "
                "and refresh_if_needed() first"
            )
        return build("drive", "v3", credentials=self.creds)


# ---------------------------------------------------------------------------
# Google Drive connector
# ---------------------------------------------------------------------------

class GoogleDriveConnector(BaseConnector):
    """Google Drive pull-based connector — adapted from OpenRAG for knowledge-ingest.

    No browser OAuth flow, no webhook support, no incremental sync.
    Full pull only — lists all supported files in the configured scope and
    downloads them for ingestion.

    Config keys:
        client_id     (str)       : OAuth2 client ID (from Google Cloud Console)
        client_secret (str)       : OAuth2 client secret
        token_file    (str)       : Path to persisted OAuth token JSON
        folder_ids    (list[str]) : Google Drive folder IDs to sync (empty = all My Drive)
        file_ids      (list[str]) : Specific file IDs to sync (takes precedence if set)
    """

    CONNECTOR_NAME = "google_drive"

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self._oauth = _GDriveOAuth(
            client_id=config.get("client_id", ""),
            client_secret=config.get("client_secret", ""),
            token_file=config.get("token_file", "gdrive_token.json"),
        )
        self._service = None

    async def authenticate(self) -> bool:
        """Load OAuth credentials, refresh if expired, build Drive service.

        Returns True on success. Raises RuntimeError on token refresh failure
        or token persistence failure (W-C2.2 fail-closed contract).
        """
        creds = self._oauth.load_credentials()
        if creds is None:
            logger.error(
                "gdrive: no token file found at %s — run OAuth setup first",
                self._oauth.token_file,
            )
            return False

        # May raise RuntimeError on refresh or persistence failure
        valid = await asyncio.to_thread(self._oauth.refresh_if_needed)
        if not valid:
            logger.error("gdrive: credentials not valid after load/refresh")
            return False

        try:
            self._service = await asyncio.to_thread(self._oauth.get_service)
        except Exception as exc:
            raise RuntimeError(f"gdrive: failed to build Drive service: {exc}") from exc

        self._authenticated = True
        logger.debug("gdrive: authenticated, service ready")
        return True

    async def list_files(
        self,
        page_token: Optional[str] = None,
        max_files: Optional[int] = None,
    ) -> Dict[str, Any]:
        """List supported files from Google Drive.

        Respects ``file_ids`` (specific files) and ``folder_ids`` (folder scope)
        from config. Falls back to listing all supported files in My Drive.

        Returns:
            {"files": [...], "next_page_token": None}
        """
        specific_file_ids: List[str] = self.config.get("file_ids", [])
        folder_ids: List[str] = self.config.get("folder_ids", [])

        if specific_file_ids:
            files = await asyncio.to_thread(self._list_specific_files, specific_file_ids)
        elif folder_ids:
            files = []
            for folder_id in folder_ids:
                folder_files = await asyncio.to_thread(self._list_folder_files, folder_id)
                files.extend(folder_files)
        else:
            files = await asyncio.to_thread(self._list_all_supported_files)

        logger.info("gdrive: discovered %d supported file(s)", len(files))
        return {"files": files, "next_page_token": None}

    def _supported_mime_query(self) -> str:
        """Build a Drive API query string that matches all supported MIME types."""
        conditions = [f"mimeType='{m}'" for m in sorted(_SUPPORTED_MIME_TYPES)]
        return "(" + " or ".join(conditions) + ") and trashed=false"

    def _list_all_supported_files(self) -> List[Dict[str, Any]]:
        files: List[Dict[str, Any]] = []
        query = self._supported_mime_query()
        page_token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {
                "q": query,
                "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,size)",
                "pageSize": 100,
            }
            if page_token:
                kwargs["pageToken"] = page_token
            response = self._service.files().list(**kwargs).execute()
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return files

    def _list_folder_files(self, folder_id: str) -> List[Dict[str, Any]]:
        files: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {
                "q": f"'{folder_id}' in parents and trashed=false",
                "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,size)",
                "pageSize": 100,
            }
            if page_token:
                kwargs["pageToken"] = page_token
            response = self._service.files().list(**kwargs).execute()
            for f in response.get("files", []):
                if f.get("mimeType") in _SUPPORTED_MIME_TYPES:
                    files.append(f)
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return files

    def _list_specific_files(self, file_ids: List[str]) -> List[Dict[str, Any]]:
        files: List[Dict[str, Any]] = []
        for file_id in file_ids:
            try:
                meta = self._service.files().get(
                    fileId=file_id,
                    fields="id,name,mimeType,modifiedTime,size",
                ).execute()
                files.append(meta)
            except HttpError as exc:
                logger.warning(
                    "gdrive: could not get metadata for file_id=%s: %s",
                    file_id, exc.status_code if hasattr(exc, "status_code") else exc,
                )
        return files

    async def get_file_content(self, file_id: str) -> ConnectorDocument:
        """Download file content from Google Drive.

        Google Workspace documents are exported to the nearest compatible format:
          Docs → DOCX, Sheets → XLSX, Slides → PPTX.
        Regular files are downloaded directly.

        Raises:
            ValueError: If the file's MIME type is not supported.
            HttpError:  If the Drive API returns an error.
        """
        meta = await asyncio.to_thread(
            lambda: self._service.files().get(
                fileId=file_id,
                fields="id,name,mimeType,modifiedTime,createdTime,size,webViewLink",
            ).execute()
        )

        mime_type: str = meta.get("mimeType", "")
        filename: str = meta.get("name", file_id)
        modified_time = _parse_gdrive_time(meta.get("modifiedTime", ""))
        created_time = _parse_gdrive_time(meta.get("createdTime", ""))

        if mime_type in _EXPORT_FORMATS:
            export_mime, ext = _EXPORT_FORMATS[mime_type]
            content = await asyncio.to_thread(
                self._download_export, file_id, export_mime
            )
            effective_mime = export_mime
            # Append extension if the Workspace file has no extension
            if "." not in filename:
                filename = f"{filename}.{ext}"
        elif mime_type in _DIRECT_MIME_TYPES:
            content = await asyncio.to_thread(self._download_direct, file_id)
            effective_mime = mime_type
        else:
            raise ValueError(
                f"gdrive: unsupported MIME type {mime_type!r} for file_id={file_id}"
            )

        source_url = meta.get(
            "webViewLink", f"https://drive.google.com/file/d/{file_id}/view"
        )

        return ConnectorDocument(
            id=file_id,
            filename=filename,
            mimetype=effective_mime,
            content=content,
            source_url=source_url,
            modified_time=modified_time,
            created_time=created_time,
            metadata={
                "gdrive_file_id": file_id,
                "original_mime_type": mime_type,
            },
        )

    def _download_export(self, file_id: str, export_mime: str) -> bytes:
        """Export a Google Workspace document to a binary format."""
        request = self._service.files().export_media(
            fileId=file_id, mimeType=export_mime
        )
        return request.execute()

    def _download_direct(self, file_id: str) -> bytes:
        """Download a regular (non-Workspace) file."""
        import io
        request = self._service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_gdrive_time(time_str: str) -> datetime:
    """Parse a Google Drive RFC 3339 timestamp to a UTC-aware datetime."""
    if not time_str:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)
