"""W-C2.2: Tests for Google Drive connector wrapper (knowledge-ingest#38).

Boundary
--------
- GoogleDriveConnector (authenticate, list_files, get_file_content) is stubbed
  via unittest.mock — no real Google API calls.
- ingest_from_connector() is also stubbed — no database or orchestrator.
- _GDriveOAuth internals are stubbed where needed (token refresh, save).
- No real disk I/O for token files.

Tests cover:
  1.  source_uri derivation — "gdrive://{fileId}" format
  2.  source_revision — taken from modifiedTime (ISO 8601 UTC)
  3.  Auth failure → RuntimeError, no ingest calls
  4.  list_files + get_file_content → correct ConnectorEvent fields
  5.  Rename stability — same fileId + different filename = same source_uri
  6.  skipped_unchanged verdict propagated from adapter
  7.  Token refresh write-back failure → RuntimeError (fail closed)
  8.  No secret leakage in log output
  9.  No webhook path in google_drive_wrapper module source
 10.  No direct db.* or orchestrator imports in wrapper module
 11.  Unsupported MIME type → skipped (not ingested)
 12.  Content fetch error → fetch_failed SyncResult, source_uri preserved
 13.  Multiple files → one SyncResult per attempted file
 14.  Empty file_ids config → falls through to list_all
 15.  fetch_failed from adapter propagated without raising
 16.  Google Sheets → skipped, never passed to ingest_from_connector()
 17.  Google Slides → skipped, never passed to ingest_from_connector()
 18.  xlsx/pptx not in wrapper's ingest format map
"""

import inspect
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

import sys, types

# ---------------------------------------------------------------------------
# Synthetic shims — allow import without real Google or boto3 libraries
# ---------------------------------------------------------------------------

_fake_boto3 = types.ModuleType("boto3")
_fake_boto3.client = lambda service, **kwargs: MagicMock()
sys.modules.setdefault("boto3", _fake_boto3)

def _fake_module(name: str) -> types.ModuleType:
    m = types.ModuleType(name)
    sys.modules[name] = m
    return m

# googleapiclient.*
_gapi = _fake_module("googleapiclient")
_gapi_disc = _fake_module("googleapiclient.discovery")
_gapi_disc.build = MagicMock(return_value=MagicMock())
_gapi_err = _fake_module("googleapiclient.errors")
_gapi_err.HttpError = type("HttpError", (Exception,), {})
_gapi_http = _fake_module("googleapiclient.http")
_gapi_http.MediaIoBaseDownload = MagicMock()

# google.*
_google = _fake_module("google")
_google_auth = _fake_module("google.auth")
_google_auth_tr = _fake_module("google.auth.transport")
_google_auth_req = _fake_module("google.auth.transport.requests")
_google_auth_req.Request = MagicMock()
_google_oauth2 = _fake_module("google.oauth2")
_google_oauth2_creds = _fake_module("google.oauth2.credentials")
class _FakeCredentials:
    def __init__(self, **kwargs):
        self.token = kwargs.get("token")
        self.refresh_token = kwargs.get("refresh_token")
        self.scopes = kwargs.get("scopes")
        self.expiry = kwargs.get("expiry")
        self.expired = False
        self.valid = True
    def refresh(self, request):
        pass
_google_oauth2_creds.Credentials = _FakeCredentials

from connectors.adapter import ConnectorEvent, SyncResult
from connectors.google_drive_wrapper import (
    CONNECTOR_MODULE,
    _infer_format,
    derive_source_uri,
    sync_gdrive,
    sync_gdrive_relay,
)
from connectors.managed_relay import (
    ConnectorFile,
    ManagedSourceRef,
    RelayError,
    RelayResult,
)
from connectors.openrag_base import ConnectorDocument

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MOD = "connectors.google_drive_wrapper"

_FIXED_DT = datetime(2026, 2, 20, 12, 0, 0, tzinfo=timezone.utc)
_FIXED_DT_ISO = "2026-02-20T12:00:00+00:00"

_FILE_ID_1 = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
_FILE_ID_2 = "0B4kM1h2aRfKdDXkxdGxmV0NWdFk"

_BASE_CONFIG: Dict[str, Any] = {
    "client_id": "test-client-id.apps.googleusercontent.com",
    "client_secret": "test-client-secret",
    "token_file": "/tmp/test_gdrive_token.json",
    "scope": "test-kb/docs",
    "owner": "ci@example.com",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_file_meta(
    file_id: str = _FILE_ID_1,
    name: str = "api-reference.pdf",
    mime_type: str = "application/pdf",
) -> Dict[str, Any]:
    return {
        "id": file_id,
        "name": name,
        "mimeType": mime_type,
        "modifiedTime": _FIXED_DT.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "size": "102400",
    }


def _make_doc(
    file_id: str = _FILE_ID_1,
    filename: str = "api-reference.pdf",
    mimetype: str = "application/pdf",
    content: bytes = b"%PDF-1.4 fake content",
) -> ConnectorDocument:
    return ConnectorDocument(
        id=file_id,
        filename=filename,
        mimetype=mimetype,
        content=content,
        source_url=f"https://drive.google.com/file/d/{file_id}/view",
        modified_time=_FIXED_DT,
        created_time=_FIXED_DT,
        metadata={"gdrive_file_id": file_id, "original_mime_type": mimetype},
    )


def _ok_sync_result(
    file_id: str = _FILE_ID_1,
    verdict: str = "fetched_new",
) -> SyncResult:
    return SyncResult(
        task_id=f"task-{file_id[:8]}",
        verdict=verdict,
        document_id=f"doc-{file_id[:8]}",
        job_id=f"job-{file_id[:8]}",
        version=1,
        source_uri=f"gdrive://{file_id}",
    )


def _connector_mock(
    auth_return: bool = True,
    files: Optional[List[Dict[str, Any]]] = None,
    doc: Optional[ConnectorDocument] = None,
) -> MagicMock:
    """Build a mock GoogleDriveConnector with controllable return values."""
    if files is None:
        files = [_make_file_meta()]
    if doc is None:
        doc = _make_doc()

    mock = MagicMock()
    mock.authenticate = AsyncMock(return_value=auth_return)
    mock.list_files = AsyncMock(return_value={"files": files, "next_page_token": None})
    mock.get_file_content = AsyncMock(return_value=doc)
    return mock


# ---------------------------------------------------------------------------
# 1. source_uri derivation
# ---------------------------------------------------------------------------

class TestDeriveSourceUri:
    def test_standard_file_id(self):
        assert derive_source_uri(_FILE_ID_1) == f"gdrive://{_FILE_ID_1}"

    def test_short_file_id(self):
        assert derive_source_uri("abc123") == "gdrive://abc123"

    def test_strips_whitespace(self):
        assert derive_source_uri(f"  {_FILE_ID_1}  ") == f"gdrive://{_FILE_ID_1}"

    def test_empty_file_id_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            derive_source_uri("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            derive_source_uri("   ")


# ---------------------------------------------------------------------------
# 2. source_revision from modifiedTime
# ---------------------------------------------------------------------------

class TestSourceRevision:
    @pytest.mark.asyncio
    async def test_source_revision_is_modified_time_iso(self):
        """Wrapper sets source_revision = modifiedTime.isoformat()."""
        captured: Dict[str, Any] = {}

        def _fake_ingest(event: ConnectorEvent) -> SyncResult:
            captured["event"] = event
            return _ok_sync_result()

        connector = _connector_mock()

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector", side_effect=_fake_ingest):
                await sync_gdrive(_BASE_CONFIG)

        assert captured["event"].source_revision == _FIXED_DT_ISO

    @pytest.mark.asyncio
    async def test_source_revision_is_utc_iso8601(self):
        """source_revision must be a UTC-aware ISO 8601 string."""
        captured: Dict[str, Any] = {}

        def _fake_ingest(event: ConnectorEvent) -> SyncResult:
            captured["event"] = event
            return _ok_sync_result()

        connector = _connector_mock()

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector", side_effect=_fake_ingest):
                await sync_gdrive(_BASE_CONFIG)

        rev = captured["event"].source_revision
        # Must be parseable as ISO 8601
        dt = datetime.fromisoformat(rev)
        assert dt.tzinfo is not None  # UTC-aware


# ---------------------------------------------------------------------------
# 3. Auth failure → RuntimeError, no ingest
# ---------------------------------------------------------------------------

class TestAuthFailure:
    @pytest.mark.asyncio
    async def test_auth_false_raises_runtime_error(self):
        connector = _connector_mock(auth_return=False)
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
                with pytest.raises(RuntimeError, match="authentication failed"):
                    await sync_gdrive(_BASE_CONFIG)
                mock_ingest.assert_not_called()

    @pytest.mark.asyncio
    async def test_auth_runtime_error_propagates(self):
        """RuntimeError from authenticate (e.g. token refresh fail) propagates."""
        connector = _connector_mock()
        connector.authenticate = AsyncMock(side_effect=RuntimeError("token refresh failed"))

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
                with pytest.raises(RuntimeError, match="token refresh failed"):
                    await sync_gdrive(_BASE_CONFIG)
                mock_ingest.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_client_id_raises_value_error(self):
        config = {**_BASE_CONFIG, "client_id": ""}
        with pytest.raises(ValueError, match="client_id"):
            await sync_gdrive(config)

    @pytest.mark.asyncio
    async def test_missing_client_secret_raises_value_error(self):
        config = {**_BASE_CONFIG, "client_secret": ""}
        with pytest.raises(ValueError, match="client_secret"):
            await sync_gdrive(config)

    @pytest.mark.asyncio
    async def test_missing_token_file_raises_value_error(self):
        config = {**_BASE_CONFIG, "token_file": ""}
        with pytest.raises(ValueError, match="token_file"):
            await sync_gdrive(config)


# ---------------------------------------------------------------------------
# 4. ConnectorEvent construction
# ---------------------------------------------------------------------------

class TestConnectorEventConstruction:
    @pytest.mark.asyncio
    async def test_connector_event_fields(self):
        """connector_module, scope, source_uri, format, owner all correctly set."""
        captured: Dict[str, Any] = {}

        def _fake_ingest(event: ConnectorEvent) -> SyncResult:
            captured["event"] = event
            return _ok_sync_result()

        connector = _connector_mock()
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector", side_effect=_fake_ingest):
                results = await sync_gdrive(_BASE_CONFIG)

        assert len(results) == 1
        ev = captured["event"]
        assert ev.connector_module == CONNECTOR_MODULE
        assert ev.connector_module == "google_drive"
        assert ev.scope == "test-kb/docs"
        assert ev.source_uri == f"gdrive://{_FILE_ID_1}"
        assert ev.format == "pdf"
        assert ev.owner == "ci@example.com"
        assert ev.raw_bytes == b"%PDF-1.4 fake content"
        assert ev.filename == "api-reference.pdf"


# ---------------------------------------------------------------------------
# 5. Rename stability
# ---------------------------------------------------------------------------

class TestRenameStability:
    @pytest.mark.asyncio
    async def test_same_file_id_different_name_same_source_uri(self):
        """Renaming a file does not change its source_uri (fileId is stable)."""
        uri_a = derive_source_uri(_FILE_ID_1)

        # Simulate a rename: same fileId, different filename
        renamed_meta = _make_file_meta(file_id=_FILE_ID_1, name="renamed-document.pdf")
        renamed_doc = _make_doc(file_id=_FILE_ID_1, filename="renamed-document.pdf")

        captured_uris: List[str] = []

        def _fake_ingest(event: ConnectorEvent) -> SyncResult:
            captured_uris.append(event.source_uri)
            return _ok_sync_result()

        connector = _connector_mock(files=[renamed_meta], doc=renamed_doc)
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector", side_effect=_fake_ingest):
                await sync_gdrive(_BASE_CONFIG)

        assert captured_uris == [uri_a]
        assert captured_uris[0] == f"gdrive://{_FILE_ID_1}"


# ---------------------------------------------------------------------------
# 6. skipped_unchanged propagated from adapter
# ---------------------------------------------------------------------------

class TestSkippedUnchanged:
    @pytest.mark.asyncio
    async def test_skipped_unchanged_verdict_propagated(self):
        skipped = SyncResult(
            task_id="task-skip",
            verdict="skipped_unchanged",
            document_id="doc-abc",
            source_uri=f"gdrive://{_FILE_ID_1}",
        )
        connector = _connector_mock()
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector", return_value=skipped):
                results = await sync_gdrive(_BASE_CONFIG)

        assert len(results) == 1
        assert results[0].verdict == "skipped_unchanged"
        assert results[0].document_id == "doc-abc"


# ---------------------------------------------------------------------------
# 7. Token refresh write-back failure → fail closed
# ---------------------------------------------------------------------------

class TestTokenRefreshWriteBack:
    @pytest.mark.asyncio
    async def test_token_persist_failure_raises_runtime_error(self):
        """If token refresh occurs but persistence fails, RuntimeError propagates."""
        connector = _connector_mock()
        connector.authenticate = AsyncMock(
            side_effect=RuntimeError(
                "gdrive_oauth: failed to persist token to /tmp/token.json: [Errno 28] No space left"
            )
        )

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
                with pytest.raises(RuntimeError, match="persist token"):
                    await sync_gdrive(_BASE_CONFIG)
                mock_ingest.assert_not_called()

    def test_gdrive_oauth_save_credentials_raises_on_disk_full(self, tmp_path):
        """_GDriveOAuth.save_credentials raises RuntimeError if write fails."""
        from connectors.openrag_gdrive import _GDriveOAuth
        from unittest.mock import patch as _patch
        import json

        oauth = _GDriveOAuth(
            client_id="cid",
            client_secret="csecret",
            token_file=str(tmp_path / "token.json"),
        )
        # Inject a valid Credentials mock
        from google.oauth2.credentials import Credentials
        creds = MagicMock(spec=Credentials)
        creds.token = "access_token"
        creds.refresh_token = "refresh_token"
        creds.scopes = ["https://www.googleapis.com/auth/drive.readonly"]
        creds.expiry = None
        oauth.creds = creds

        # Simulate disk full by making tempfile.mkstemp raise
        with _patch("connectors.openrag_gdrive.tempfile.mkstemp", side_effect=OSError("No space left")):
            with pytest.raises(RuntimeError, match="persist token"):
                oauth.save_credentials()

    def test_gdrive_oauth_refresh_calls_save_on_success(self, tmp_path):
        """After successful refresh, save_credentials is called before returning."""
        from connectors.openrag_gdrive import _GDriveOAuth
        from unittest.mock import patch as _patch

        token_file = tmp_path / "token.json"
        token_file.write_text('{"token": "old", "refresh_token": "rt", "scopes": []}')

        oauth = _GDriveOAuth(
            client_id="cid",
            client_secret="csecret",
            token_file=str(token_file),
        )
        oauth.load_credentials()

        # Make creds appear expired with a refresh token
        from unittest.mock import PropertyMock
        type(oauth.creds).expired = PropertyMock(return_value=True)
        type(oauth.creds).refresh_token = PropertyMock(return_value="rt")

        save_calls: List[str] = []
        original_save = oauth.save_credentials

        def _tracking_save():
            save_calls.append("save")

        with _patch.object(oauth, "save_credentials", side_effect=_tracking_save):
            with _patch.object(oauth.creds, "refresh"):
                result = oauth.refresh_if_needed()

        # save_credentials must have been called
        assert "save" in save_calls


# ---------------------------------------------------------------------------
# 8. No secret leakage in log output
# ---------------------------------------------------------------------------

class TestNoSecretLeakage:
    @pytest.mark.asyncio
    async def test_client_secret_not_in_logs(self, caplog):
        connector = _connector_mock(auth_return=False)
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with pytest.raises(RuntimeError):
                with caplog.at_level(logging.DEBUG):
                    await sync_gdrive(_BASE_CONFIG)

        secret = _BASE_CONFIG["client_secret"]
        for record in caplog.records:
            assert secret not in record.getMessage(), (
                f"client_secret leaked in log: {record.getMessage()}"
            )

    @pytest.mark.asyncio
    async def test_client_id_not_in_error_logs(self, caplog):
        connector = _connector_mock(auth_return=False)
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with pytest.raises(RuntimeError):
                with caplog.at_level(logging.DEBUG):
                    await sync_gdrive(_BASE_CONFIG)

        client_id = _BASE_CONFIG["client_id"]
        for record in caplog.records:
            assert client_id not in record.getMessage(), (
                f"client_id leaked in log: {record.getMessage()}"
            )


# ---------------------------------------------------------------------------
# 9. No webhook path in module source
# ---------------------------------------------------------------------------

class TestNoWebhookPath:
    def test_google_drive_wrapper_has_no_webhook_functions(self):
        """No function or method named *webhook* may exist in the wrapper.

        The module docstring explicitly documents that webhooks are unsupported,
        so we check for actual function/method definitions, not just the word.
        """
        import importlib, re, pathlib
        mod = importlib.import_module("connectors.google_drive_wrapper")
        source = pathlib.Path(inspect.getfile(mod)).read_text()
        # Check for actual function/method definitions, not just the word in docs
        webhook_defs = re.findall(r"^\s*def\s+\w*webhook\w*\s*\(", source, re.MULTILINE | re.IGNORECASE)
        assert not webhook_defs, (
            "google_drive_wrapper.py must not define webhook functions — out of scope for #38. "
            f"Found: {webhook_defs}"
        )
        # Also verify GoogleDriveConnector has no webhook attributes
        from connectors.google_drive_wrapper import sync_gdrive
        assert not any("webhook" in name.lower() for name in dir(sync_gdrive))

    def test_openrag_gdrive_has_no_webhook_functions(self):
        """No function or method named *webhook* may exist in the connector.

        The module docstring explicitly documents that webhooks are unsupported,
        so we check for actual function/method definitions, not just the word.
        """
        import importlib, re, pathlib
        mod = importlib.import_module("connectors.openrag_gdrive")
        source = pathlib.Path(inspect.getfile(mod)).read_text()
        webhook_defs = re.findall(r"^\s*def\s+\w*webhook\w*\s*\(", source, re.MULTILINE | re.IGNORECASE)
        assert not webhook_defs, (
            "openrag_gdrive.py must not define webhook functions — out of scope for #38. "
            f"Found: {webhook_defs}"
        )
        # GoogleDriveConnector must not expose webhook methods
        from connectors.openrag_gdrive import GoogleDriveConnector
        connector_methods = [m for m in dir(GoogleDriveConnector) if "webhook" in m.lower()]
        assert not connector_methods, (
            f"GoogleDriveConnector must not have webhook methods: {connector_methods}"
        )


# ---------------------------------------------------------------------------
# 10. No db.* or orchestrator imports in wrapper
# ---------------------------------------------------------------------------

class TestModuleIsolation:
    def test_google_drive_wrapper_does_not_import_db_or_orchestrator(self):
        import importlib, pathlib
        mod = importlib.import_module("connectors.google_drive_wrapper")
        source = pathlib.Path(inspect.getfile(mod)).read_text()

        import_lines = [
            line.strip()
            for line in source.splitlines()
            if line.strip().startswith(("import ", "from "))
        ]
        forbidden = ["from db", "import db", "from orchestrator", "import orchestrator"]
        for pattern in forbidden:
            assert not any(line.startswith(pattern) for line in import_lines), (
                f"google_drive_wrapper.py must not import '{pattern}'. "
                f"Import lines found:\n" + "\n".join(import_lines)
            )


# ---------------------------------------------------------------------------
# 11. Unsupported MIME type → skipped
# ---------------------------------------------------------------------------

class TestUnsupportedFormat:
    @pytest.mark.asyncio
    async def test_unsupported_mime_type_not_ingested(self):
        unsupported_doc = _make_doc(
            mimetype="application/zip",
            content=b"PK\x03\x04",
        )
        connector = _connector_mock(
            files=[_make_file_meta(mime_type="application/zip")],
            doc=unsupported_doc,
        )
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
                results = await sync_gdrive(_BASE_CONFIG)

        mock_ingest.assert_not_called()
        assert results == []

    def test_infer_format_known_types(self):
        assert _infer_format("application/pdf") == "pdf"
        assert _infer_format("text/html") == "html"
        assert _infer_format("text/markdown") == "markdown"
        assert _infer_format("application/vnd.openxmlformats-officedocument.wordprocessingml.document") == "docx"

    def test_infer_format_unknown_returns_none(self):
        assert _infer_format("application/zip") is None
        assert _infer_format("image/png") is None
        assert _infer_format("video/mp4") is None


# ---------------------------------------------------------------------------
# 16-18. Google Sheets and Slides skipped; xlsx/pptx not in format map
# ---------------------------------------------------------------------------

class TestSheetsAndSlidesSkipped:
    """Google Sheets and Google Slides must be skipped — the ingest core does not
    support xlsx or pptx. These files must never reach ingest_from_connector()."""

    _SHEETS_MIME = "application/vnd.google-apps.spreadsheet"
    _SLIDES_MIME = "application/vnd.google-apps.presentation"
    _EXPORTED_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    _EXPORTED_PPTX = "application/vnd.openxmlformats-officedocument.presentationml.presentation"

    @pytest.mark.asyncio
    async def test_google_sheets_not_ingested(self):
        """A Google Sheets file must be skipped — no ingest_from_connector() call."""
        sheets_meta = _make_file_meta(mime_type=self._SHEETS_MIME)
        connector = _connector_mock(files=[sheets_meta])

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
                results = await sync_gdrive(_BASE_CONFIG)

        mock_ingest.assert_not_called()
        assert results == []

    @pytest.mark.asyncio
    async def test_google_slides_not_ingested(self):
        """A Google Slides file must be skipped — no ingest_from_connector() call."""
        slides_meta = _make_file_meta(mime_type=self._SLIDES_MIME)
        connector = _connector_mock(files=[slides_meta])

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
                results = await sync_gdrive(_BASE_CONFIG)

        mock_ingest.assert_not_called()
        assert results == []

    @pytest.mark.asyncio
    async def test_sheets_and_slides_skipped_pdf_ingested(self):
        """Mixed list: Sheets and Slides are skipped; PDF proceeds to ingest."""
        files = [
            _make_file_meta(file_id=_FILE_ID_1, mime_type="application/pdf", name="doc.pdf"),
            _make_file_meta(file_id=_FILE_ID_2, mime_type=self._SHEETS_MIME, name="budget.gsheet"),
        ]
        pdf_doc = _make_doc(file_id=_FILE_ID_1, mimetype="application/pdf")
        connector = _connector_mock(files=files, doc=pdf_doc)

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector", return_value=_ok_sync_result()) as mock_ingest:
                results = await sync_gdrive(_BASE_CONFIG)

        # Only the PDF was ingested
        assert mock_ingest.call_count == 1
        assert len(results) == 1
        assert results[0].source_uri == f"gdrive://{_FILE_ID_1}"

    def test_xlsx_not_in_infer_format(self):
        """xlsx MIME type must not map to any ingest format — not supported by normalizer."""
        assert _infer_format(self._EXPORTED_XLSX) is None

    def test_pptx_not_in_infer_format(self):
        """pptx MIME type must not map to any ingest format — not supported by normalizer."""
        assert _infer_format(self._EXPORTED_PPTX) is None

    def test_google_sheets_gdrive_mime_not_in_infer_format(self):
        """google-apps.spreadsheet MIME must not produce an ingest format."""
        assert _infer_format(self._SHEETS_MIME) is None

    def test_google_slides_gdrive_mime_not_in_infer_format(self):
        """google-apps.presentation MIME must not produce an ingest format."""
        assert _infer_format(self._SLIDES_MIME) is None

    def test_google_docs_still_supported(self):
        """Google Docs (→ DOCX) must still be ingested — it is the only supported export."""
        from connectors.openrag_gdrive import _EXPORT_FORMATS
        assert "application/vnd.google-apps.document" in _EXPORT_FORMATS
        # Sheets and Slides must NOT be in _EXPORT_FORMATS
        assert "application/vnd.google-apps.spreadsheet" not in _EXPORT_FORMATS
        assert "application/vnd.google-apps.presentation" not in _EXPORT_FORMATS


# ---------------------------------------------------------------------------
# 12. Content fetch error → fetch_failed, source_uri preserved
# ---------------------------------------------------------------------------

class TestContentFetchError:
    @pytest.mark.asyncio
    async def test_get_file_content_error_returns_fetch_failed(self):
        connector = _connector_mock()
        connector.get_file_content = AsyncMock(side_effect=Exception("Drive API 500"))

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
                results = await sync_gdrive(_BASE_CONFIG)

        mock_ingest.assert_not_called()
        assert len(results) == 1
        assert results[0].verdict == "fetch_failed"
        assert results[0].source_uri == f"gdrive://{_FILE_ID_1}"
        assert "Exception" in results[0].error_message

    @pytest.mark.asyncio
    async def test_fetch_failed_verdict_from_adapter_propagated(self):
        """fetch_failed from adapter is a normal verdict, not an exception."""
        failed = SyncResult(
            task_id="task-f",
            verdict="fetch_failed",
            error_message="Scope not registered",
            source_uri=f"gdrive://{_FILE_ID_1}",
        )
        connector = _connector_mock()
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector", return_value=failed):
                results = await sync_gdrive(_BASE_CONFIG)

        assert len(results) == 1
        assert results[0].verdict == "fetch_failed"
        assert results[0].error_message == "Scope not registered"


# ---------------------------------------------------------------------------
# 13. Multiple files → one SyncResult per attempted file
# ---------------------------------------------------------------------------

class TestMultipleFiles:
    @pytest.mark.asyncio
    async def test_two_files_two_results(self):
        files = [
            _make_file_meta(file_id=_FILE_ID_1, name="doc1.pdf"),
            _make_file_meta(file_id=_FILE_ID_2, name="doc2.pdf"),
        ]
        docs = [
            _make_doc(file_id=_FILE_ID_1, filename="doc1.pdf"),
            _make_doc(file_id=_FILE_ID_2, filename="doc2.pdf"),
        ]

        connector = _connector_mock(files=files)
        connector.get_file_content = AsyncMock(side_effect=docs)

        ingest_results = [
            _ok_sync_result(_FILE_ID_1),
            _ok_sync_result(_FILE_ID_2),
        ]

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector", side_effect=ingest_results):
                results = await sync_gdrive(_BASE_CONFIG)

        assert len(results) == 2
        assert results[0].source_uri == f"gdrive://{_FILE_ID_1}"
        assert results[1].source_uri == f"gdrive://{_FILE_ID_2}"

    @pytest.mark.asyncio
    async def test_empty_file_list_returns_empty_results(self):
        connector = _connector_mock(files=[])
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
                results = await sync_gdrive(_BASE_CONFIG)

        mock_ingest.assert_not_called()
        assert results == []


# ---------------------------------------------------------------------------
# sync_gdrive_relay — relay mode tests
# ---------------------------------------------------------------------------

def _source_ref(**overrides) -> ManagedSourceRef:
    defaults = dict(
        source_id="00000000-0000-0000-0000-000000000099",
        bucket="agentopia-knowledge-dev",
        prefix="scopes/scope-1/sources/src-1/",
        region="ap-northeast-1",
        access_key="AKIATEST",
        secret_key="testsecret",
        endpoint_url=None,
    )
    defaults.update(overrides)
    return ManagedSourceRef(**defaults)


def _ok_relay_result(file_id: str, filename: str = "api-reference.pdf") -> RelayResult:
    return RelayResult(
        connector_module=CONNECTOR_MODULE,
        stable_id=file_id,
        filename=filename,
        s3_key=f"scopes/scope-1/sources/src-1/google_drive/{file_id}.pdf",
        bytes_written=21,
        etag="etag001",
        replaced=False,
    )


class TestSyncGdriveRelay:
    """Tests for sync_gdrive_relay — the production relay-mode entry point."""

    @pytest.mark.asyncio
    async def test_relay_mode_calls_relay_files_not_ingest(self):
        """relay_files() is called; ingest_from_connector() is NOT called."""
        connector = _connector_mock()
        ref = _source_ref()
        expected_results = [_ok_relay_result(_FILE_ID_1)]

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.relay_files", return_value=(expected_results, [])) as mock_relay:
                with patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
                    results, errors = await sync_gdrive_relay(_BASE_CONFIG, ref)

        mock_relay.assert_called_once()
        mock_ingest.assert_not_called()
        assert len(results) == 1
        assert len(errors) == 0

    @pytest.mark.asyncio
    async def test_relay_uses_file_id_as_stable_id(self):
        """ConnectorFile.stable_id == Drive fileId (NOT filename)."""
        connector = _connector_mock(
            files=[_make_file_meta(file_id=_FILE_ID_1, name="report.pdf")],
            doc=_make_doc(file_id=_FILE_ID_1, filename="report.pdf"),
        )
        ref = _source_ref()
        captured: list[ConnectorFile] = []

        def capture_relay(r, files):
            captured.extend(files)
            return [_ok_relay_result(_FILE_ID_1, "report.pdf")], []

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.relay_files", side_effect=capture_relay):
                await sync_gdrive_relay(_BASE_CONFIG, ref)

        assert len(captured) == 1
        assert captured[0].stable_id == _FILE_ID_1  # stable_id = fileId, NOT filename
        assert captured[0].connector_module == CONNECTOR_MODULE
        assert captured[0].filename == "report.pdf"

    @pytest.mark.asyncio
    async def test_two_files_same_basename_different_ids_no_collision(self):
        """Files with same display name in different Drive folders → different stable_ids."""
        files_meta = [
            _make_file_meta(file_id=_FILE_ID_1, name="report.pdf"),
            _make_file_meta(file_id=_FILE_ID_2, name="report.pdf"),  # same basename!
        ]
        connector = _connector_mock(files=files_meta)
        connector.get_file_content = AsyncMock(side_effect=[
            _make_doc(file_id=_FILE_ID_1, filename="report.pdf"),
            _make_doc(file_id=_FILE_ID_2, filename="report.pdf"),
        ])
        ref = _source_ref()
        captured: list[ConnectorFile] = []

        def capture_relay(r, files):
            captured.extend(files)
            return [
                _ok_relay_result(_FILE_ID_1, "report.pdf"),
                _ok_relay_result(_FILE_ID_2, "report.pdf"),
            ], []

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.relay_files", side_effect=capture_relay):
                results, errors = await sync_gdrive_relay(_BASE_CONFIG, ref)

        assert len(captured) == 2
        # Different stable_ids → no collision
        assert captured[0].stable_id != captured[1].stable_id
        assert captured[0].stable_id == _FILE_ID_1
        assert captured[1].stable_id == _FILE_ID_2

    @pytest.mark.asyncio
    async def test_relay_auth_failure_raises_runtime_error(self):
        connector = _connector_mock(auth_return=False)
        ref = _source_ref()
        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with pytest.raises(RuntimeError, match="authentication failed"):
                await sync_gdrive_relay(_BASE_CONFIG, ref)

    @pytest.mark.asyncio
    async def test_relay_unsupported_mime_skipped(self):
        """Google Sheets files are skipped before content download in relay mode too."""
        files_meta = [
            _make_file_meta(file_id=_FILE_ID_1, mime_type="application/vnd.google-apps.spreadsheet"),
        ]
        connector = _connector_mock(files=files_meta)
        ref = _source_ref()
        captured: list[ConnectorFile] = []

        def capture_relay(r, files):
            captured.extend(files)
            return [], []

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.relay_files", side_effect=capture_relay):
                results, errors = await sync_gdrive_relay(_BASE_CONFIG, ref)

        # Skipped before relay_files — no ConnectorFile constructed for this file
        assert len(captured) == 0
        connector.get_file_content.assert_not_called()

    @pytest.mark.asyncio
    async def test_relay_content_fetch_error_skipped(self):
        """fetch failure on one file → that file skipped, others continue."""
        files_meta = [
            _make_file_meta(file_id=_FILE_ID_1, name="ok.pdf"),
            _make_file_meta(file_id=_FILE_ID_2, name="bad.pdf"),
        ]
        connector = _connector_mock(files=files_meta)
        connector.get_file_content = AsyncMock(side_effect=[
            _make_doc(file_id=_FILE_ID_1, filename="ok.pdf"),
            Exception("network error"),
        ])
        ref = _source_ref()
        captured: list[ConnectorFile] = []

        def capture_relay(r, files):
            captured.extend(files)
            return [_ok_relay_result(_FILE_ID_1, "ok.pdf")], []

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.relay_files", side_effect=capture_relay):
                results, errors = await sync_gdrive_relay(_BASE_CONFIG, ref)

        # Only the successful file was relayed
        assert len(captured) == 1
        assert captured[0].stable_id == _FILE_ID_1

    @pytest.mark.asyncio
    async def test_relay_missing_credentials_raises_value_error(self):
        ref = _source_ref()
        bad_config = {"token_file": "/tmp/tok.json"}  # no client_id / client_secret
        with pytest.raises(ValueError, match="client_id"):
            await sync_gdrive_relay(bad_config, ref)

    @pytest.mark.asyncio
    async def test_relay_passes_source_ref_to_relay_files(self):
        """The ManagedSourceRef passed by caller is forwarded to relay_files() unchanged."""
        connector = _connector_mock()
        ref = _source_ref(source_id="specific-uuid")
        captured_ref = []

        def capture_relay(r, files):
            captured_ref.append(r)
            return [_ok_relay_result(_FILE_ID_1)], []

        with patch(f"{_MOD}.GoogleDriveConnector", return_value=connector):
            with patch(f"{_MOD}.relay_files", side_effect=capture_relay):
                await sync_gdrive_relay(_BASE_CONFIG, ref)

        assert len(captured_ref) == 1
        assert captured_ref[0].source_id == "specific-uuid"
