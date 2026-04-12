"""W-C2.4: Tests for OneDrive connector wrapper (knowledge-ingest#40).

Boundary
--------
- OneDriveConnector (authenticate, resolve_default_drive_id, list_files,
  get_file_content) is stubbed via unittest.mock — no real Microsoft Graph API
  calls, no real MSAL token exchange.
- ingest_from_connector() is stubbed — no database, no orchestrator.
- _MSALTokenCache internals are stubbed where needed (file load, save).
- No real disk I/O for token files.

Tests cover the 13 required validation points for W-C2.4:
  1.  Plain itemId → source_uri uses resolved default driveId.
  2.  driveId!itemId composite → source_uri split correctly.
  3.  Sharing IDs (plain "s..." and composite "driveId!s...") → skipped.
  4.  source_revision taken from list_files() "modified" field (ISO 8601).
  5.  Auth failure → RuntimeError, no ingest calls.
  6.  list_files + get_file_content → correct ConnectorEvent fields
      (connector_module, source_uri, filename, format, raw_bytes).
  7.  Same logical item renamed → identity preserved at wrapper boundary
      (same itemId = same source_uri regardless of filename change).
  8.  Identical bytes on second sync → skipped_unchanged propagated.
  9.  Drive resolution failure → fail closed, RuntimeError propagated.
 10.  Token refresh persistence failure → fail closed, RuntimeError propagated.
 11.  No token leakage in logs (client_secret not logged).
 12.  No direct db.* or orchestrator imports in wrapper/connector modules.
 13.  No webhook path in onedrive_wrapper or openrag_onedrive source.
"""

import inspect
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from connectors.adapter import ConnectorEvent, SyncResult
from connectors.onedrive_wrapper import (
    CONNECTOR_MODULE,
    _infer_format,
    derive_source_uri_and_item,
    sync_onedrive,
)
from connectors.openrag_base import ConnectorDocument

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MOD = "connectors.onedrive_wrapper"

_FIXED_DT = datetime(2026, 3, 15, 9, 30, 0, tzinfo=timezone.utc)
_FIXED_DT_ISO = "2026-03-15T09:30:00+00:00"

_DRIVE_ID = "AbCdEfGhIjKlMnOpQrStUv"  # real driveIds use URL-safe base64 without "!"
_ITEM_ID_1 = "01AAAAAAAAAAAAAAAAAAAAAA"
_ITEM_ID_2 = "01BBBBBBBBBBBBBBBBBBBBBB"

_BASE_CONFIG: Dict[str, Any] = {
    "client_id": "test-client-id",
    "client_secret": "test-client-secret",
    "tenant_id": "test-tenant-id",
    "token_file": "/tmp/test_onedrive_token.json",
    "scope": "test-kb/onedrive",
    "owner": "ci@example.com",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_file_meta(
    file_id: str = _ITEM_ID_1,
    name: str = "spec.pdf",
    mime: str = "application/pdf",
    modified: str = _FIXED_DT_ISO,
) -> Dict[str, Any]:
    return {
        "id": file_id,
        "name": name,
        "modified": modified,
        "mimeType": mime,
        "size": 65536,
    }


def _make_doc(
    file_id: str = _ITEM_ID_1,
    filename: str = "spec.pdf",
    mimetype: str = "application/pdf",
    content: bytes = b"%PDF-1.4 fake content",
) -> ConnectorDocument:
    return ConnectorDocument(
        id=file_id,
        filename=filename,
        mimetype=mimetype,
        content=content,
        source_url=f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}",
        modified_time=_FIXED_DT,
        created_time=_FIXED_DT,
        metadata={"onedrive_item_id": file_id},
    )


def _ok_sync_result(
    item_id: str = _ITEM_ID_1,
    drive_id: str = _DRIVE_ID,
    verdict: str = "fetched_new",
) -> SyncResult:
    return SyncResult(
        task_id=f"task-{item_id[:8]}",
        verdict=verdict,
        document_id=f"doc-{item_id[:8]}",
        job_id=f"job-{item_id[:8]}",
        version=1,
        source_uri=f"onedrive://{drive_id}/{item_id}",
    )


def _mock_connector(
    drive_id: str = _DRIVE_ID,
    files: Optional[List[Dict]] = None,
    doc: Optional[ConnectorDocument] = None,
    auth_ok: bool = True,
    auth_exc: Optional[Exception] = None,
    drive_exc: Optional[Exception] = None,
) -> MagicMock:
    """Build a fully-stubbed OneDriveConnector mock."""
    mock = MagicMock()

    if auth_exc:
        mock.authenticate = AsyncMock(side_effect=auth_exc)
    elif not auth_ok:
        mock.authenticate = AsyncMock(return_value=False)
    else:
        mock.authenticate = AsyncMock(return_value=True)

    if drive_exc:
        mock.resolve_default_drive_id = AsyncMock(side_effect=drive_exc)
    else:
        mock.resolve_default_drive_id = AsyncMock(return_value=drive_id)

    file_list = files if files is not None else [_make_file_meta()]
    mock.list_files = AsyncMock(return_value={"files": file_list, "next_page_token": None})

    mock.get_file_content = AsyncMock(return_value=doc or _make_doc())
    return mock


# ---------------------------------------------------------------------------
# 1–3: derive_source_uri_and_item — pure function tests
# ---------------------------------------------------------------------------

class TestDeriveSourceUri:
    """Tests for the W-C2.4 identity derivation function."""

    def test_plain_item_id_uses_default_drive(self):
        """Case 1: plain itemId → source_uri uses default driveId."""
        result = derive_source_uri_and_item(_ITEM_ID_1, _DRIVE_ID)
        assert result is not None
        source_uri, item_id = result
        assert source_uri == f"onedrive://{_DRIVE_ID}/{_ITEM_ID_1}"
        assert item_id == _ITEM_ID_1

    def test_composite_form_split_correctly(self):
        """Case 2: driveId!itemId → split correctly, embedded driveId used."""
        other_drive = "OtherDriveXxx"  # driveIds are base64 — no "!" inside them
        composite = f"{other_drive}!{_ITEM_ID_2}"
        result = derive_source_uri_and_item(composite, _DRIVE_ID)
        assert result is not None
        source_uri, item_id = result
        assert source_uri == f"onedrive://{other_drive}/{_ITEM_ID_2}"
        assert item_id == _ITEM_ID_2
        # Default driveId must NOT appear in the source_uri when composite
        assert _DRIVE_ID not in source_uri

    def test_sharing_id_plain_form_skipped(self):
        """Case 3a: plain sharing ID (starts with 's', no '!') → returns None."""
        sharing_id = "sAABBCCDDEEFF"  # plain form: starts with 's', no '!' separator
        result = derive_source_uri_and_item(sharing_id, _DRIVE_ID)
        assert result is None

    def test_sharing_id_composite_form_skipped(self):
        """Case 3b: composite driveId!s... → right-hand sharing ID → returns None."""
        composite_sharing = f"{_DRIVE_ID}!sABCDEFGH"
        result = derive_source_uri_and_item(composite_sharing, _DRIVE_ID)
        assert result is None

    def test_rename_stability_same_item_id(self):
        """Case 1 rename stability: same itemId with different filename = same source_uri.

        OneDrive itemId is stable across renames within the same drive.
        The wrapper derives source_uri from itemId only — filename is irrelevant
        to identity. This test verifies that derive_source_uri_and_item produces
        the same output for the same itemId regardless of what the caller passes
        as filename (filename is not an input to this function).
        """
        result_v1 = derive_source_uri_and_item(_ITEM_ID_1, _DRIVE_ID)
        result_v2 = derive_source_uri_and_item(_ITEM_ID_1, _DRIVE_ID)
        assert result_v1 == result_v2
        assert result_v1[0] == f"onedrive://{_DRIVE_ID}/{_ITEM_ID_1}"


# ---------------------------------------------------------------------------
# 4: source_revision from modified timestamp
# ---------------------------------------------------------------------------

class TestSourceRevision:
    """source_revision must be the lastModifiedDateTime from list_files()."""

    @pytest.mark.asyncio
    async def test_source_revision_from_list_files_modified(self):
        file_meta = _make_file_meta(modified=_FIXED_DT_ISO)
        captured_events: List[ConnectorEvent] = []

        def _capture(event: ConnectorEvent) -> SyncResult:
            captured_events.append(event)
            return _ok_sync_result()

        connector = _mock_connector(files=[file_meta])
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector", side_effect=_capture):
            await sync_onedrive(_BASE_CONFIG)

        assert len(captured_events) == 1
        assert captured_events[0].source_revision == _FIXED_DT_ISO


# ---------------------------------------------------------------------------
# 5: Auth failure → no ingest
# ---------------------------------------------------------------------------

class TestAuthFailure:
    """Authentication failures must propagate without any ingest calls."""

    @pytest.mark.asyncio
    async def test_auth_runtime_error_propagates(self):
        connector = _mock_connector(
            auth_exc=RuntimeError("MSAL auth failed: expired token")
        )
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
            with pytest.raises(RuntimeError, match="MSAL auth failed"):
                await sync_onedrive(_BASE_CONFIG)
            mock_ingest.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_client_id_raises_value_error(self):
        cfg = {**_BASE_CONFIG, "client_id": ""}
        with pytest.raises(ValueError, match="client_id"):
            await sync_onedrive(cfg)

    @pytest.mark.asyncio
    async def test_missing_client_secret_raises_value_error(self):
        cfg = {**_BASE_CONFIG, "client_secret": ""}
        with pytest.raises(ValueError, match="client_secret"):
            await sync_onedrive(cfg)

    @pytest.mark.asyncio
    async def test_missing_tenant_id_raises_value_error(self):
        cfg = {**_BASE_CONFIG, "tenant_id": ""}
        with pytest.raises(ValueError, match="tenant_id"):
            await sync_onedrive(cfg)

    @pytest.mark.asyncio
    async def test_missing_token_file_raises_value_error(self):
        cfg = {**_BASE_CONFIG, "token_file": ""}
        with pytest.raises(ValueError, match="token_file"):
            await sync_onedrive(cfg)


# ---------------------------------------------------------------------------
# 6: Correct ConnectorEvent fields from list_files + get_file_content
# ---------------------------------------------------------------------------

class TestConnectorEventConstruction:
    """verify that sync_onedrive passes a fully correct ConnectorEvent to the adapter."""

    @pytest.mark.asyncio
    async def test_connector_event_fields(self):
        item_id = _ITEM_ID_1
        file_meta = _make_file_meta(file_id=item_id, name="guide.pdf",
                                    mime="application/pdf", modified=_FIXED_DT_ISO)
        doc = _make_doc(file_id=item_id, filename="guide.pdf",
                        mimetype="application/pdf", content=b"%PDF content")

        captured: List[ConnectorEvent] = []

        def _capture(event: ConnectorEvent) -> SyncResult:
            captured.append(event)
            return _ok_sync_result(item_id=item_id)

        connector = _mock_connector(files=[file_meta], doc=doc)
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector", side_effect=_capture):
            results = await sync_onedrive(_BASE_CONFIG)

        assert len(captured) == 1
        ev = captured[0]
        assert ev.connector_module == CONNECTOR_MODULE
        assert ev.source_uri == f"onedrive://{_DRIVE_ID}/{item_id}"
        assert ev.filename == "guide.pdf"
        assert ev.format == "pdf"
        assert ev.raw_bytes == b"%PDF content"
        assert ev.source_revision == _FIXED_DT_ISO
        assert ev.scope == _BASE_CONFIG["scope"]
        assert ev.owner == _BASE_CONFIG["owner"]
        assert len(results) == 1


# ---------------------------------------------------------------------------
# 7: Rename stability — same itemId, different name → same source_uri
# ---------------------------------------------------------------------------

class TestRenameStability:
    """Same itemId across two syncs with different filenames must yield identical source_uri."""

    @pytest.mark.asyncio
    async def test_same_item_id_different_name_same_source_uri(self):
        item_id = _ITEM_ID_1

        captured: List[ConnectorEvent] = []

        def _capture(event: ConnectorEvent) -> SyncResult:
            captured.append(event)
            return _ok_sync_result(item_id=item_id)

        # Sync 1: filename = "original.pdf"
        file_v1 = _make_file_meta(file_id=item_id, name="original.pdf")
        doc_v1 = _make_doc(file_id=item_id, filename="original.pdf")
        connector_v1 = _mock_connector(files=[file_v1], doc=doc_v1)
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector_v1), \
             patch(f"{_MOD}.ingest_from_connector", side_effect=_capture):
            await sync_onedrive(_BASE_CONFIG)

        # Sync 2: filename = "renamed.pdf" (same itemId)
        file_v2 = _make_file_meta(file_id=item_id, name="renamed.pdf")
        doc_v2 = _make_doc(file_id=item_id, filename="renamed.pdf")
        connector_v2 = _mock_connector(files=[file_v2], doc=doc_v2)
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector_v2), \
             patch(f"{_MOD}.ingest_from_connector", side_effect=_capture):
            await sync_onedrive(_BASE_CONFIG)

        # source_uri must be identical across both syncs
        assert len(captured) == 2
        assert captured[0].source_uri == captured[1].source_uri
        assert captured[0].source_uri == f"onedrive://{_DRIVE_ID}/{item_id}"


# ---------------------------------------------------------------------------
# 8: Identical bytes → skipped_unchanged propagated
# ---------------------------------------------------------------------------

class TestSkippedUnchanged:

    @pytest.mark.asyncio
    async def test_skipped_unchanged_verdict_propagated(self):
        skipped = SyncResult(
            task_id="task-skip",
            verdict="skipped_unchanged",
            source_uri=f"onedrive://{_DRIVE_ID}/{_ITEM_ID_1}",
        )
        connector = _mock_connector()
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector", return_value=skipped):
            results = await sync_onedrive(_BASE_CONFIG)

        assert len(results) == 1
        assert results[0].verdict == "skipped_unchanged"


# ---------------------------------------------------------------------------
# 9: Drive resolution failure → fail closed
# ---------------------------------------------------------------------------

class TestDriveResolutionFailure:

    @pytest.mark.asyncio
    async def test_drive_resolution_failure_aborts_sync(self):
        connector = _mock_connector(
            drive_exc=RuntimeError("onedrive_connector: failed to resolve default driveId")
        )
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
            with pytest.raises(RuntimeError, match="driveId"):
                await sync_onedrive(_BASE_CONFIG)
            mock_ingest.assert_not_called()


# ---------------------------------------------------------------------------
# 10: Token refresh persistence failure → fail closed
# ---------------------------------------------------------------------------

class TestTokenPersistenceFailure:
    """Token cache write failure must abort the sync (fail closed)."""

    @pytest.mark.asyncio
    async def test_token_persist_failure_raises_before_ingest(self):
        """If _MSALTokenCache.save_if_changed() raises, authenticate() propagates
        RuntimeError and ingest_from_connector() must never be called."""
        persist_error = RuntimeError(
            "onedrive_oauth: failed to persist token cache to /tmp/tok.json: "
            "OSError. Aborting sync (fail closed)."
        )
        connector = _mock_connector(auth_exc=persist_error)
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
            with pytest.raises(RuntimeError, match="persist token cache"):
                await sync_onedrive(_BASE_CONFIG)
            mock_ingest.assert_not_called()

    def test_msal_token_cache_save_raises_on_disk_full(self, tmp_path):
        """_MSALTokenCache.save_if_changed() must raise RuntimeError if write fails."""
        import os
        from connectors.openrag_onedrive import _MSALTokenCache

        cache = _MSALTokenCache(str(tmp_path / "tok.json"))

        mock_inner = MagicMock()
        mock_inner.has_state_changed = True
        mock_inner.serialize.return_value = '{"AccessToken": {}}'
        cache.cache = mock_inner

        # Simulate write failure by making tempfile creation fail
        with patch("connectors.openrag_onedrive.tempfile.mkstemp",
                   side_effect=OSError("No space left on device")):
            with pytest.raises(RuntimeError, match="persist token cache"):
                cache.save_if_changed()

    def test_msal_token_cache_load_raises_on_missing_file(self):
        """_MSALTokenCache.load() must raise RuntimeError if token file is missing."""
        from connectors.openrag_onedrive import _MSALTokenCache

        cache = _MSALTokenCache("/nonexistent/path/tok.json")
        with pytest.raises(RuntimeError, match="token file not found"):
            cache.load()


# ---------------------------------------------------------------------------
# 11: No token leakage in logs
# ---------------------------------------------------------------------------

class TestNoSecretLeakage:

    @pytest.mark.asyncio
    async def test_client_secret_not_in_logs(self, caplog):
        """client_secret value must never appear in any log output."""
        connector = _mock_connector()
        with caplog.at_level(logging.DEBUG, logger="connectors"):
            with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
                 patch(f"{_MOD}.ingest_from_connector", return_value=_ok_sync_result()):
                await sync_onedrive(_BASE_CONFIG)

        secret = _BASE_CONFIG["client_secret"]
        for record in caplog.records:
            assert secret not in record.getMessage(), (
                f"client_secret leaked in log: {record.getMessage()!r}"
            )

    @pytest.mark.asyncio
    async def test_no_access_token_in_logs(self, caplog):
        """If authenticate() populates _access_token, it must not appear in logs."""
        fake_token = "fake-access-token-xyzzy"

        connector = _mock_connector()
        # Simulate that authenticate() set an access token on the connector
        connector.authenticate = AsyncMock(
            side_effect=lambda: setattr(connector, "_access_token", fake_token) or True
        )
        connector.resolve_default_drive_id = AsyncMock(return_value=_DRIVE_ID)

        with caplog.at_level(logging.DEBUG, logger="connectors"):
            with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
                 patch(f"{_MOD}.ingest_from_connector", return_value=_ok_sync_result()):
                await sync_onedrive(_BASE_CONFIG)

        for record in caplog.records:
            assert fake_token not in record.getMessage(), (
                f"access token leaked in log: {record.getMessage()!r}"
            )


# ---------------------------------------------------------------------------
# 12: No direct DB or orchestrator imports in wrapper/connector modules
# ---------------------------------------------------------------------------

class TestModuleIsolation:
    """Wrapper and connector must not bypass the adapter by importing db.* or orchestrator."""

    def test_onedrive_wrapper_does_not_import_db_or_orchestrator(self):
        import connectors.onedrive_wrapper as mod
        src = inspect.getsource(mod)
        for forbidden in ("import db", "from db", "import orchestrator", "from orchestrator"):
            assert forbidden not in src, (
                f"onedrive_wrapper.py contains forbidden import: {forbidden!r}"
            )

    def test_openrag_onedrive_does_not_import_db_or_orchestrator(self):
        import connectors.openrag_onedrive as mod
        src = inspect.getsource(mod)
        for forbidden in ("import db", "from db", "import orchestrator", "from orchestrator"):
            assert forbidden not in src, (
                f"openrag_onedrive.py contains forbidden import: {forbidden!r}"
            )

    def test_onedrive_wrapper_does_not_import_connector_service(self):
        import connectors.onedrive_wrapper as mod
        src = inspect.getsource(mod)
        assert "ConnectorService" not in src, (
            "onedrive_wrapper.py must not import ConnectorService (OpenRAG internal)"
        )


# ---------------------------------------------------------------------------
# 13: No webhook path
# ---------------------------------------------------------------------------

class TestNoWebhookPath:
    """No webhook *function definitions* may exist in onedrive_wrapper or openrag_onedrive.

    Docstrings may mention these names (to document that they are out of scope),
    but no ``def`` statement for any of them is allowed.
    """

    _WEBHOOK_DEF_NAMES = (
        "setup_subscription",
        "handle_webhook",
        "cleanup_subscription",
        "extract_webhook_channel_id",
    )

    def test_onedrive_wrapper_has_no_webhook_functions(self):
        import connectors.onedrive_wrapper as mod
        src = inspect.getsource(mod)
        for name in self._WEBHOOK_DEF_NAMES:
            for prefix in ("def ", "async def "):
                assert f"{prefix}{name}" not in src, (
                    f"onedrive_wrapper.py contains webhook function definition: {prefix}{name!r}"
                )

    def test_openrag_onedrive_has_no_webhook_functions(self):
        import connectors.openrag_onedrive as mod
        src = inspect.getsource(mod)
        for name in self._WEBHOOK_DEF_NAMES:
            for prefix in ("def ", "async def "):
                assert f"{prefix}{name}" not in src, (
                    f"openrag_onedrive.py contains webhook function definition: {prefix}{name!r}"
                )


# ---------------------------------------------------------------------------
# Additional coverage: unsupported MIME type, content fetch error, sharing IDs,
# pagination, multiple files
# ---------------------------------------------------------------------------

class TestUnsupportedFormat:

    @pytest.mark.asyncio
    async def test_unsupported_mime_type_not_ingested(self):
        """xlsx files must be skipped without calling ingest_from_connector()."""
        file_meta = _make_file_meta(
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        doc = _make_doc(
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        connector = _mock_connector(files=[file_meta], doc=doc)
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
            results = await sync_onedrive(_BASE_CONFIG)
        mock_ingest.assert_not_called()
        assert results == []

    def test_infer_format_known_types(self):
        assert _infer_format("application/pdf") == "pdf"
        assert _infer_format("application/vnd.openxmlformats-officedocument.wordprocessingml.document") == "docx"
        assert _infer_format("text/html") == "html"
        assert _infer_format("text/markdown") == "markdown"
        assert _infer_format("text/plain") == "txt"

    def test_infer_format_unknown_returns_none(self):
        assert _infer_format("application/vnd.ms-excel") is None
        assert _infer_format("application/zip") is None
        assert _infer_format("video/mp4") is None


class TestSharingIdSkipped:

    @pytest.mark.asyncio
    async def test_plain_sharing_id_not_ingested(self):
        """A file whose itemId starts with 's' (no '!') must be skipped."""
        file_meta = _make_file_meta(file_id="sABCDEFGHIJKLMNOP")  # plain sharing ID
        connector = _mock_connector(files=[file_meta])
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
            results = await sync_onedrive(_BASE_CONFIG)
        mock_ingest.assert_not_called()
        assert results == []

    @pytest.mark.asyncio
    async def test_composite_sharing_id_not_ingested(self):
        """A composite driveId!sXXX file must be skipped."""
        file_meta = _make_file_meta(file_id=f"{_DRIVE_ID}!sABCDEFGH")
        connector = _mock_connector(files=[file_meta])
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
            results = await sync_onedrive(_BASE_CONFIG)
        mock_ingest.assert_not_called()
        assert results == []


class TestContentFetchError:

    @pytest.mark.asyncio
    async def test_content_fetch_error_produces_fetch_failed(self):
        """get_file_content() failure → fetch_failed SyncResult, source_uri preserved."""
        connector = _mock_connector()
        connector.get_file_content = AsyncMock(
            side_effect=RuntimeError("content download failed for item")
        )
        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector") as mock_ingest:
            results = await sync_onedrive(_BASE_CONFIG)
        mock_ingest.assert_not_called()
        assert len(results) == 1
        assert results[0].verdict == "fetch_failed"
        assert f"onedrive://{_DRIVE_ID}/{_ITEM_ID_1}" in results[0].source_uri


class TestPagination:

    @pytest.mark.asyncio
    async def test_multiple_pages_collected(self):
        """list_files() with next_page_token must cause a second call and collect all files."""
        file1 = _make_file_meta(file_id=_ITEM_ID_1, name="a.pdf")
        file2 = _make_file_meta(file_id=_ITEM_ID_2, name="b.txt",
                                mime="text/plain", modified=_FIXED_DT_ISO)

        call_count = 0

        async def _paginated_list_files(page_token=None):
            nonlocal call_count
            call_count += 1
            if page_token is None:
                return {"files": [file1], "next_page_token": "https://graph.microsoft.com/next"}
            return {"files": [file2], "next_page_token": None}

        connector = _mock_connector()
        connector.list_files = _paginated_list_files
        connector.get_file_content = AsyncMock(side_effect=lambda fid: _make_doc(
            file_id=fid,
            filename="a.pdf" if fid == _ITEM_ID_1 else "b.txt",
            mimetype="application/pdf" if fid == _ITEM_ID_1 else "text/plain",
        ))

        captured: List[ConnectorEvent] = []

        def _capture(event):
            captured.append(event)
            return _ok_sync_result(item_id=event.source_uri.split("/")[-1])

        with patch(f"{_MOD}.OneDriveConnector", return_value=connector), \
             patch(f"{_MOD}.ingest_from_connector", side_effect=_capture):
            results = await sync_onedrive(_BASE_CONFIG)

        assert call_count == 2
        assert len(captured) == 2
        uris = {ev.source_uri for ev in captured}
        assert f"onedrive://{_DRIVE_ID}/{_ITEM_ID_1}" in uris
        assert f"onedrive://{_DRIVE_ID}/{_ITEM_ID_2}" in uris
