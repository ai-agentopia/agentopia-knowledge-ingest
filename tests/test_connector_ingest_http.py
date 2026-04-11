"""W-C3.1: Tests for POST /connectors/ingest HTTP endpoint.

Issue:  knowledge-ingest#42
ADR:    docs/adr/002-connector-precondition-audit.md (W-C3.1 trigger condition)

Architecture
------------
This test boundary:
  - ingest_from_connector() is stubbed (no adapter logic runs).
  - FastAPI TestClient is used (no real network).
  - No DB, no orchestrator, no storage.

The route is a thin transport wrapper. Tests verify:
  1. All four adapter verdicts return HTTP 200.
  2. SyncResult fields are serialised correctly to JSON.
  3. Malformed body returns 422.
  4. Invalid base64 returns 422.
  5. Oversized decoded payload returns 413.
  6. Missing required fields return 422.
  7. Invalid format returns 422.
  8. ConnectorEvent constructed with correct field values from request.
"""

import base64
from dataclasses import dataclass
from typing import Optional
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# App import — PYTHONPATH=src required (same as other tests in this project)
# ---------------------------------------------------------------------------

from main import app  # noqa: E402

client = TestClient(app)

_MOD = "api.routes"  # module where ingest_from_connector is imported at call time

_ENDPOINT = "/connectors/ingest"

# ---------------------------------------------------------------------------
# Stub helpers
# ---------------------------------------------------------------------------

@dataclass
class _SyncResult:
    task_id: str
    verdict: str
    document_id: Optional[str] = None
    job_id: Optional[str] = None
    version: Optional[int] = None
    error_message: Optional[str] = None


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _minimal_body(**overrides) -> dict:
    body = {
        "connector_module": "test_connector",
        "scope": "test-kb/docs",
        "source_uri": "test://bucket/doc.pdf",
        "filename": "doc.pdf",
        "format": "pdf",
        "raw_bytes_b64": _b64(b"PDF content bytes"),
    }
    body.update(overrides)
    return body


# ---------------------------------------------------------------------------
# Verdict tests — all four verdicts must produce HTTP 200
# ---------------------------------------------------------------------------

class TestAllVerdicts:
    def test_fetched_new_returns_200(self):
        result = _SyncResult(
            task_id="task-001",
            verdict="fetched_new",
            document_id="doc-abc",
            job_id="job-xyz",
            version=1,
        )
        with patch(f"{_MOD}.ingest_from_connector", return_value=result):
            resp = client.post(_ENDPOINT, json=_minimal_body())
        assert resp.status_code == 200
        data = resp.json()
        assert data["verdict"] == "fetched_new"
        assert data["task_id"] == "task-001"
        assert data["document_id"] == "doc-abc"
        assert data["job_id"] == "job-xyz"
        assert data["version"] == 1
        assert data["error_message"] is None

    def test_fetched_updated_returns_200(self):
        result = _SyncResult(
            task_id="task-002",
            verdict="fetched_updated",
            document_id="doc-abc",
            job_id="job-xyz",
            version=2,
        )
        with patch(f"{_MOD}.ingest_from_connector", return_value=result):
            resp = client.post(_ENDPOINT, json=_minimal_body())
        assert resp.status_code == 200
        assert resp.json()["verdict"] == "fetched_updated"
        assert resp.json()["version"] == 2

    def test_skipped_unchanged_returns_200(self):
        result = _SyncResult(
            task_id="task-003",
            verdict="skipped_unchanged",
        )
        with patch(f"{_MOD}.ingest_from_connector", return_value=result):
            resp = client.post(_ENDPOINT, json=_minimal_body())
        assert resp.status_code == 200
        data = resp.json()
        assert data["verdict"] == "skipped_unchanged"
        assert data["document_id"] is None
        assert data["job_id"] is None
        assert data["version"] is None

    def test_fetch_failed_returns_200_not_4xx(self):
        """fetch_failed is an adapter verdict — must return 200, not an error status."""
        result = _SyncResult(
            task_id="task-004",
            verdict="fetch_failed",
            error_message="Scope 'bad-scope/docs' is not registered",
        )
        with patch(f"{_MOD}.ingest_from_connector", return_value=result):
            resp = client.post(_ENDPOINT, json=_minimal_body())
        assert resp.status_code == 200
        data = resp.json()
        assert data["verdict"] == "fetch_failed"
        assert "not registered" in data["error_message"]
        assert data["document_id"] is None


# ---------------------------------------------------------------------------
# ConnectorEvent construction verification
# ---------------------------------------------------------------------------

class TestEventConstruction:
    def test_event_fields_match_request(self):
        captured = {}

        def _capture(event):
            captured["event"] = event
            return _SyncResult(task_id="t1", verdict="fetched_new",
                               document_id="d1", job_id="j1", version=1)

        raw = b"test document content"
        body = _minimal_body(
            connector_module="aws_s3",
            scope="joblogic-kb/docs",
            source_uri="s3://bucket/path/file.pdf",
            filename="file.pdf",
            format="pdf",
            raw_bytes_b64=_b64(raw),
            source_revision="2026-01-15T10:30:00+00:00",
            owner="operator@example.com",
            metadata={"s3_bucket": "bucket", "s3_key": "path/file.pdf"},
        )

        with patch(f"{_MOD}.ingest_from_connector", side_effect=_capture):
            resp = client.post(_ENDPOINT, json=body)

        assert resp.status_code == 200
        ev = captured["event"]
        assert ev.connector_module == "aws_s3"
        assert ev.scope == "joblogic-kb/docs"
        assert ev.source_uri == "s3://bucket/path/file.pdf"
        assert ev.filename == "file.pdf"
        assert ev.format == "pdf"
        assert ev.raw_bytes == raw
        assert ev.source_revision == "2026-01-15T10:30:00+00:00"
        assert ev.owner == "operator@example.com"
        assert ev.metadata == {"s3_bucket": "bucket", "s3_key": "path/file.pdf"}

    def test_optional_fields_default_correctly(self):
        captured = {}

        def _capture(event):
            captured["event"] = event
            return _SyncResult(task_id="t1", verdict="fetched_new",
                               document_id="d1", job_id="j1", version=1)

        with patch(f"{_MOD}.ingest_from_connector", side_effect=_capture):
            resp = client.post(_ENDPOINT, json=_minimal_body())

        assert resp.status_code == 200
        ev = captured["event"]
        assert ev.source_revision is None
        assert ev.owner == ""
        assert ev.metadata == {}


# ---------------------------------------------------------------------------
# Validation errors — 422
# ---------------------------------------------------------------------------

class TestValidationErrors:
    def test_missing_connector_module_returns_422(self):
        body = _minimal_body()
        del body["connector_module"]
        resp = client.post(_ENDPOINT, json=body)
        assert resp.status_code == 422

    def test_missing_source_uri_returns_422(self):
        body = _minimal_body()
        del body["source_uri"]
        resp = client.post(_ENDPOINT, json=body)
        assert resp.status_code == 422

    def test_missing_raw_bytes_b64_returns_422(self):
        body = _minimal_body()
        del body["raw_bytes_b64"]
        resp = client.post(_ENDPOINT, json=body)
        assert resp.status_code == 422

    def test_invalid_format_returns_422(self):
        body = _minimal_body(format="xlsx")
        resp = client.post(_ENDPOINT, json=body)
        assert resp.status_code == 422

    def test_empty_connector_module_returns_422(self):
        body = _minimal_body(connector_module="")
        resp = client.post(_ENDPOINT, json=body)
        assert resp.status_code == 422

    def test_empty_source_uri_returns_422(self):
        body = _minimal_body(source_uri="")
        resp = client.post(_ENDPOINT, json=body)
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Invalid base64 — 422
# ---------------------------------------------------------------------------

class TestBase64Errors:
    def test_invalid_base64_returns_422(self):
        body = _minimal_body(raw_bytes_b64="not-valid-base64!!!")
        resp = client.post(_ENDPOINT, json=body)
        assert resp.status_code == 422
        assert "base64" in resp.json()["detail"].lower()

    def test_empty_raw_bytes_b64_is_valid_empty_doc(self):
        """Empty string decodes to 0 bytes — valid at transport layer."""
        result = _SyncResult(task_id="t1", verdict="fetched_new",
                             document_id="d1", job_id="j1", version=1)
        body = _minimal_body(raw_bytes_b64=_b64(b""))
        with patch(f"{_MOD}.ingest_from_connector", return_value=result):
            resp = client.post(_ENDPOINT, json=body)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Oversized payload — 413
# ---------------------------------------------------------------------------

class TestSizeLimit:
    def test_oversized_payload_returns_413(self):
        """Decoded payload > 50 MiB must return 413, not 200."""
        # Patch to avoid actually encoding 50 MiB in CI.
        # We mock the b64decode to return an oversized byte string.
        oversized = b"x" * (50 * 1024 * 1024 + 1)
        body = _minimal_body(raw_bytes_b64=_b64(b"small"))

        with patch("api.routes.base64.b64decode", return_value=oversized):
            resp = client.post(_ENDPOINT, json=body)

        assert resp.status_code == 413
        assert "50" in resp.json()["detail"]

    def test_exactly_at_limit_is_accepted(self):
        """Payload exactly at 50 MiB boundary must NOT return 413."""
        at_limit = b"x" * (50 * 1024 * 1024)
        result = _SyncResult(task_id="t1", verdict="fetched_new",
                             document_id="d1", job_id="j1", version=1)
        body = _minimal_body(raw_bytes_b64=_b64(b"small"))

        with patch("api.routes.base64.b64decode", return_value=at_limit):
            with patch(f"{_MOD}.ingest_from_connector", return_value=result):
                resp = client.post(_ENDPOINT, json=body)

        assert resp.status_code == 200
