"""Tests for W-C2.1: AWS S3 connector wrapper (knowledge-ingest#37).

Boundary: S3Connector (authenticate, list_files, get_file_content) is stubbed
via unittest.mock. The adapter (ingest_from_connector) is also stubbed so that
database and orchestrator are not exercised. Tests verify the wrapper's own
responsibilities only:

  - source_uri derivation
  - source_revision derivation
  - ConnectorEvent construction
  - auth failure → no ingest
  - unsupported format → skipped
  - dedup (skipped_unchanged) propagated from adapter
  - scope mapping / registry enforcement delegated to adapter
  - no direct DB writes (adapter stub counts calls)
  - no webhook path exists in the wrapper
  - no secret values appear in log output

What is stubbed
---------------
  S3Connector.authenticate()       → controlled return value
  S3Connector.list_files()         → returns synthetic file list
  S3Connector.get_file_content()   → returns synthetic ConnectorDocument
  ingest_from_connector()          → returns SyncResult; counts calls

Nothing real (boto3, PostgreSQL, orchestrator) is invoked.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from connectors.adapter import SyncResult
from connectors.aws_s3_wrapper import (
    CONNECTOR_MODULE,
    derive_source_uri,
    sync_s3_bucket,
    _infer_format,
)
from connectors.openrag_base import ConnectorDocument

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

_MOD = "connectors.aws_s3_wrapper"

_FIXED_DT = datetime(2026, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
_FIXED_DT_ISO = "2026-01-15T10:30:45+00:00"

_BASE_CONFIG = {
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "bucket_names": ["test-bucket"],
    "scope": "test-scope/docs",
    "owner": "ci@example.com",
}


def _make_file_meta(bucket: str = "test-bucket", key: str = "docs/api.pdf") -> Dict[str, Any]:
    return {
        "id": f"{bucket}::{key}",
        "name": key.rsplit("/", 1)[-1],
        "bucket": bucket,
        "key": key,
        "size": 1024,
        "modified_time": _FIXED_DT,
    }


def _make_doc(
    file_id: str = "test-bucket::docs/api.pdf",
    filename: str = "api.pdf",
    mimetype: str = "application/pdf",
    content: bytes = b"pdf content",
) -> ConnectorDocument:
    return ConnectorDocument(
        id=file_id,
        filename=filename,
        mimetype=mimetype,
        content=content,
        source_url=f"s3://test-bucket/docs/api.pdf",
        modified_time=_FIXED_DT,
        created_time=_FIXED_DT,
        metadata={"s3_bucket": "test-bucket", "s3_key": "docs/api.pdf", "size": len(content)},
    )


def _make_sync_result(verdict: str = "fetched_new") -> SyncResult:
    return SyncResult(
        task_id="task-uuid-1",
        verdict=verdict,
        document_id="doc-uuid-1" if verdict != "fetch_failed" else None,
        job_id="job-uuid-1" if verdict == "fetched_new" else None,
        version=1 if verdict == "fetched_new" else None,
    )


# ---------------------------------------------------------------------------
# Part 1 — Unit: source_uri derivation
# ---------------------------------------------------------------------------

class TestDeriveSourceUri:
    def test_basic_key(self):
        uri = derive_source_uri("my-bucket::path/to/file.pdf")
        assert uri == "s3://my-bucket/path/to/file.pdf"

    def test_key_with_nested_path(self):
        uri = derive_source_uri("bucket::a/b/c/document.docx")
        assert uri == "s3://bucket/a/b/c/document.docx"

    def test_key_at_root(self):
        uri = derive_source_uri("bucket::readme.txt")
        assert uri == "s3://bucket/readme.txt"

    def test_bucket_with_hyphens(self):
        uri = derive_source_uri("my-corp-bucket::docs/api-reference.pdf")
        assert uri == "s3://my-corp-bucket/docs/api-reference.pdf"

    def test_key_containing_double_colon(self):
        # Extra :: in key — first occurrence is the separator; remainder is the key
        uri = derive_source_uri("bucket::path::to::file.pdf")
        assert uri == "s3://bucket/path::to::file.pdf"

    def test_missing_separator_raises(self):
        with pytest.raises(ValueError, match="Invalid S3 file ID"):
            derive_source_uri("bucket-without-separator.pdf")


# ---------------------------------------------------------------------------
# Part 2 — Unit: source_revision derivation
# ---------------------------------------------------------------------------

class TestSourceRevision:
    def test_modified_time_is_iso_string(self):
        """source_revision must be the ISO 8601 string of doc.modified_time."""
        doc = _make_doc()
        revision = doc.modified_time.isoformat()
        assert revision == _FIXED_DT_ISO

    def test_revision_is_utc_aware(self):
        """modified_time must be timezone-aware (UTC)."""
        doc = _make_doc()
        assert doc.modified_time.tzinfo is not None


# ---------------------------------------------------------------------------
# Part 3 — Unit: format inference
# ---------------------------------------------------------------------------

class TestInferFormat:
    @pytest.mark.parametrize("filename,mime,expected", [
        ("doc.pdf",      "application/pdf",   "pdf"),
        ("doc.docx",     "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "docx"),
        ("page.html",    "text/html",         "html"),
        ("notes.md",     "text/markdown",     "markdown"),
        ("notes.txt",    "text/plain",        "txt"),
        ("notes.md",     "application/octet-stream", "markdown"),  # mime fallback to ext
        ("unknown.xyz",  "application/octet-stream", None),
    ])
    def test_format_inference(self, filename, mime, expected):
        assert _infer_format(filename, mime) == expected


# ---------------------------------------------------------------------------
# Part 4 — Integration: auth failure → no ingest
# ---------------------------------------------------------------------------

class TestAuthFailure:
    @pytest.mark.asyncio
    async def test_auth_failure_raises_and_no_ingest(self):
        with (
            patch(f"{_MOD}.S3Connector") as MockConnector,
            patch(f"{_MOD}.ingest_from_connector") as mock_ingest,
        ):
            instance = AsyncMock()
            instance.authenticate = AsyncMock(return_value=False)
            MockConnector.return_value = instance

            with pytest.raises(RuntimeError, match="authentication failed"):
                await sync_s3_bucket(_BASE_CONFIG)

            mock_ingest.assert_not_called()


# ---------------------------------------------------------------------------
# Part 5 — Integration: list_files + get_file_content → correct ConnectorEvent
# ---------------------------------------------------------------------------

class TestCorrectConnectorEvent:
    @pytest.mark.asyncio
    async def test_single_pdf_produces_correct_event(self):
        """list_files returns one PDF → ingest_from_connector called with correct fields."""
        file_meta = _make_file_meta(bucket="test-bucket", key="docs/api.pdf")
        doc = _make_doc()
        sync_result = _make_sync_result("fetched_new")

        with (
            patch(f"{_MOD}.S3Connector") as MockConnector,
            patch(f"{_MOD}.ingest_from_connector") as mock_ingest,
        ):
            instance = AsyncMock()
            instance.authenticate = AsyncMock(return_value=True)
            instance.list_files = AsyncMock(return_value={
                "files": [file_meta], "next_page_token": None
            })
            instance.get_file_content = AsyncMock(return_value=doc)
            MockConnector.return_value = instance
            mock_ingest.return_value = sync_result

            results = await sync_s3_bucket(_BASE_CONFIG)

        assert len(results) == 1
        assert results[0].verdict == "fetched_new"

        called_event = mock_ingest.call_args[0][0]
        assert called_event.connector_module == CONNECTOR_MODULE
        assert called_event.source_uri == "s3://test-bucket/docs/api.pdf"
        assert called_event.filename == "api.pdf"
        assert called_event.format == "pdf"
        assert called_event.raw_bytes == b"pdf content"
        assert called_event.source_revision == _FIXED_DT_ISO
        assert called_event.scope == "test-scope/docs"
        assert called_event.owner == "ci@example.com"

    @pytest.mark.asyncio
    async def test_multiple_files_all_ingested(self):
        """Multiple files → ingest called once per file."""
        files = [
            _make_file_meta(key="docs/a.pdf"),
            _make_file_meta(key="docs/b.md"),
        ]
        docs = [
            _make_doc(file_id="test-bucket::docs/a.pdf", filename="a.pdf", mimetype="application/pdf"),
            _make_doc(file_id="test-bucket::docs/b.md",  filename="b.md",  mimetype="text/markdown",
                      content=b"# heading"),
        ]

        with (
            patch(f"{_MOD}.S3Connector") as MockConnector,
            patch(f"{_MOD}.ingest_from_connector") as mock_ingest,
        ):
            instance = AsyncMock()
            instance.authenticate = AsyncMock(return_value=True)
            instance.list_files = AsyncMock(return_value={
                "files": files, "next_page_token": None
            })
            instance.get_file_content = AsyncMock(side_effect=docs)
            MockConnector.return_value = instance
            mock_ingest.return_value = _make_sync_result("fetched_new")

            results = await sync_s3_bucket(_BASE_CONFIG)

        assert mock_ingest.call_count == 2
        uris = [c[0][0].source_uri for c in mock_ingest.call_args_list]
        assert "s3://test-bucket/docs/a.pdf" in uris
        assert "s3://test-bucket/docs/b.md" in uris


# ---------------------------------------------------------------------------
# Part 6 — Integration: dedup — identical bytes → skipped_unchanged
# ---------------------------------------------------------------------------

class TestDedup:
    @pytest.mark.asyncio
    async def test_skipped_unchanged_propagated_from_adapter(self):
        """When adapter returns skipped_unchanged, wrapper propagates it."""
        file_meta = _make_file_meta()
        doc = _make_doc()
        skipped = SyncResult(
            task_id="task-2",
            verdict="skipped_unchanged",
            document_id="doc-1",
        )

        with (
            patch(f"{_MOD}.S3Connector") as MockConnector,
            patch(f"{_MOD}.ingest_from_connector") as mock_ingest,
        ):
            instance = AsyncMock()
            instance.authenticate = AsyncMock(return_value=True)
            instance.list_files = AsyncMock(return_value={
                "files": [file_meta], "next_page_token": None
            })
            instance.get_file_content = AsyncMock(return_value=doc)
            MockConnector.return_value = instance
            mock_ingest.return_value = skipped

            results = await sync_s3_bucket(_BASE_CONFIG)

        assert results[0].verdict == "skipped_unchanged"
        assert results[0].document_id == "doc-1"
        # Adapter was still called — dedup is the adapter's responsibility
        mock_ingest.assert_called_once()


# ---------------------------------------------------------------------------
# Part 7 — Unsupported format → skipped
# ---------------------------------------------------------------------------

class TestUnsupportedFormat:
    @pytest.mark.asyncio
    async def test_unsupported_format_is_skipped_not_ingested(self):
        """Files with unknown MIME types and extensions are skipped silently."""
        file_meta = _make_file_meta(key="archive.zip")
        doc = ConnectorDocument(
            id="test-bucket::archive.zip",
            filename="archive.zip",
            mimetype="application/zip",
            content=b"PK...",
            source_url="s3://test-bucket/archive.zip",
            modified_time=_FIXED_DT,
            created_time=_FIXED_DT,
            metadata={},
        )

        with (
            patch(f"{_MOD}.S3Connector") as MockConnector,
            patch(f"{_MOD}.ingest_from_connector") as mock_ingest,
        ):
            instance = AsyncMock()
            instance.authenticate = AsyncMock(return_value=True)
            instance.list_files = AsyncMock(return_value={
                "files": [file_meta], "next_page_token": None
            })
            instance.get_file_content = AsyncMock(return_value=doc)
            MockConnector.return_value = instance

            results = await sync_s3_bucket(_BASE_CONFIG)

        mock_ingest.assert_not_called()
        assert results == []  # nothing ingested, nothing returned


# ---------------------------------------------------------------------------
# Part 8 — Scope mapping / registry delegated to adapter
# ---------------------------------------------------------------------------

class TestScopeEnforcement:
    @pytest.mark.asyncio
    async def test_unregistered_scope_propagated_as_fetch_failed(self):
        """When adapter returns fetch_failed for unknown scope, wrapper propagates it."""
        file_meta = _make_file_meta()
        doc = _make_doc()
        failed = SyncResult(
            task_id="task-3",
            verdict="fetch_failed",
            error_message="Scope 'bad-scope/x' is not registered in the knowledge registry.",
        )

        cfg = {**_BASE_CONFIG, "scope": "bad-scope/x"}

        with (
            patch(f"{_MOD}.S3Connector") as MockConnector,
            patch(f"{_MOD}.ingest_from_connector") as mock_ingest,
        ):
            instance = AsyncMock()
            instance.authenticate = AsyncMock(return_value=True)
            instance.list_files = AsyncMock(return_value={
                "files": [file_meta], "next_page_token": None
            })
            instance.get_file_content = AsyncMock(return_value=doc)
            MockConnector.return_value = instance
            mock_ingest.return_value = failed

            results = await sync_s3_bucket(cfg)

        assert results[0].verdict == "fetch_failed"
        assert "not registered" in (results[0].error_message or "")


# ---------------------------------------------------------------------------
# Part 9 — Wrapper never writes DB directly
# ---------------------------------------------------------------------------

class TestNoDatabaseWrites:
    @pytest.mark.asyncio
    async def test_wrapper_does_not_import_or_call_db_modules(self):
        """The wrapper module must not call db.registry or db.connector_sync directly."""
        import connectors.aws_s3_wrapper as wrapper_mod
        import inspect
        source = inspect.getsource(wrapper_mod)

        # The wrapper must not import db modules directly
        assert "from db" not in source, "Wrapper must not import from db directly"
        assert "import db" not in source, "Wrapper must not import db directly"
        assert "registry." not in source, "Wrapper must not call registry directly"
        assert "connector_sync." not in source, "Wrapper must not call connector_sync directly"

    @pytest.mark.asyncio
    async def test_ingest_from_connector_is_sole_entry_point(self):
        """Only ingest_from_connector() may trigger database writes from the wrapper."""
        file_meta = _make_file_meta()
        doc = _make_doc()

        with (
            patch(f"{_MOD}.S3Connector") as MockConnector,
            patch(f"{_MOD}.ingest_from_connector") as mock_ingest,
        ):
            instance = AsyncMock()
            instance.authenticate = AsyncMock(return_value=True)
            instance.list_files = AsyncMock(return_value={
                "files": [file_meta], "next_page_token": None
            })
            instance.get_file_content = AsyncMock(return_value=doc)
            MockConnector.return_value = instance
            mock_ingest.return_value = _make_sync_result("fetched_new")

            await sync_s3_bucket(_BASE_CONFIG)

        # Exactly one call to ingest_from_connector, nothing else
        mock_ingest.assert_called_once()


# ---------------------------------------------------------------------------
# Part 10 — No webhook path
# ---------------------------------------------------------------------------

class TestNoWebhookPath:
    def test_no_webhook_method_on_wrapper(self):
        """The wrapper module does not expose any webhook handler function."""
        import connectors.aws_s3_wrapper as wrapper_mod
        for name in dir(wrapper_mod):
            assert "webhook" not in name.lower(), (
                f"Unexpected webhook symbol found in wrapper: {name!r}"
            )

    def test_s3_connector_has_no_webhook_abstract_methods(self):
        """S3Connector in openrag_s3 does not inherit webhook abstract methods."""
        from connectors.openrag_s3 import S3Connector
        assert not hasattr(S3Connector, "setup_subscription"), (
            "setup_subscription should not be on S3Connector in this repo"
        )
        assert not hasattr(S3Connector, "handle_webhook"), (
            "handle_webhook should not be on S3Connector in this repo"
        )


# ---------------------------------------------------------------------------
# Part 11 — No secret leakage in logs
# ---------------------------------------------------------------------------

class TestSecretNonDisclosure:
    @pytest.mark.asyncio
    async def test_credentials_not_logged_on_auth_failure(self, caplog):
        """Access key and secret key values must not appear in log output."""
        with (
            patch(f"{_MOD}.S3Connector") as MockConnector,
            patch(f"{_MOD}.ingest_from_connector"),
        ):
            instance = AsyncMock()
            instance.authenticate = AsyncMock(return_value=False)
            MockConnector.return_value = instance

            with caplog.at_level(logging.DEBUG, logger="connectors.aws_s3_wrapper"):
                with pytest.raises(RuntimeError):
                    await sync_s3_bucket(_BASE_CONFIG)

        log_text = caplog.text
        assert "AKIAIOSFODNN7EXAMPLE" not in log_text, "Access key leaked in logs"
        assert "wJalrXUtnFEMI" not in log_text, "Secret key leaked in logs"

    @pytest.mark.asyncio
    async def test_credentials_not_logged_on_success(self, caplog):
        """Credentials must not appear in logs during normal ingest."""
        file_meta = _make_file_meta()
        doc = _make_doc()

        with (
            patch(f"{_MOD}.S3Connector") as MockConnector,
            patch(f"{_MOD}.ingest_from_connector") as mock_ingest,
        ):
            instance = AsyncMock()
            instance.authenticate = AsyncMock(return_value=True)
            instance.list_files = AsyncMock(return_value={
                "files": [file_meta], "next_page_token": None
            })
            instance.get_file_content = AsyncMock(return_value=doc)
            MockConnector.return_value = instance
            mock_ingest.return_value = _make_sync_result("fetched_new")

            with caplog.at_level(logging.DEBUG, logger="connectors.aws_s3_wrapper"):
                await sync_s3_bucket(_BASE_CONFIG)

        log_text = caplog.text
        assert "AKIAIOSFODNN7EXAMPLE" not in log_text, "Access key leaked in logs"
        assert "wJalrXUtnFEMI" not in log_text, "Secret key leaked in logs"

    @pytest.mark.asyncio
    async def test_openrag_s3_logs_do_not_contain_credentials(self, caplog):
        """openrag_s3 module must not log credential values on auth failure."""
        from connectors.openrag_s3 import S3Connector

        bad_config = {
            "access_key": "AKIAIOSFODNN7EXAMPLE",
            "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "bucket_names": ["test"],
        }

        # Patch boto3.resource to simulate auth failure without real AWS call
        import boto3
        with (
            patch("boto3.resource") as mock_resource,
            caplog.at_level(logging.DEBUG, logger="connectors.openrag_s3"),
        ):
            mock_resource.return_value.buckets.all.side_effect = Exception("Access Denied")
            connector = S3Connector(bad_config)
            await connector.authenticate()

        log_text = caplog.text
        assert "AKIAIOSFODNN7EXAMPLE" not in log_text, "Access key leaked in openrag_s3 log"
        assert "wJalrXUtnFEMI" not in log_text, "Secret key leaked in openrag_s3 log"
