"""W-C3.1: Tests for POST /connectors/ingest HTTP endpoint.

Issue:  knowledge-ingest#42
ADR:    docs/adr/002-connector-precondition-audit.md (W-C3.1 trigger condition)

Test boundary
-------------
- ingest_from_connector() is stubbed via unittest.mock.patch — no adapter logic runs.
- FastAPI TestClient is used — no real network.
- No DB, no orchestrator, no storage runs in any test in this file.
- Patch target: "api.connector_routes.ingest_from_connector"

Tests cover:
  1. Valid request → adapter called with correct ConnectorEvent fields
  2. Invalid base64 → 422
  3. Oversized base64 string (pre-decode check) → 413 without allocating decoded bytes
  4. Adapter returns fetched_new → 200 with serialised SyncResult
  5. Adapter returns fetched_updated → 200 with serialised SyncResult
  6. Adapter returns skipped_unchanged → 200 with serialised SyncResult
  7. Adapter returns fetch_failed → 200 (NOT 4xx) with serialised SyncResult
  8. Scope/business errors not reimplemented in route (fetch_failed passthrough test)
  9. Route module (connector_routes) does NOT import db.registry, db.connector_sync,
     or orchestrator — verified by static source inspection
 10. Optional fields default correctly
 11. Missing required fields → 422
 12. Invalid format → 422
 13. Empty required string fields → 422
 14. Post-decode size guard (defence-in-depth) → 413
 15. Exactly-at-limit b64 length is accepted
"""

import base64
import inspect
import math
from dataclasses import dataclass
from typing import Optional
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

# PYTHONPATH=src required (same as all other tests in this project)
from main import app  # noqa: E402

client = TestClient(app)

_MOD = "api.connector_routes"   # narrow module; does not import db.* or orchestrator
_ENDPOINT = "/connectors/ingest"

_MAX_DECODED_BYTES = 50 * 1024 * 1024
_MAX_B64_CHARS = math.ceil(_MAX_DECODED_BYTES / 3) * 4


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
# 1. Static isolation test — route module must not import db or orchestrator
# ---------------------------------------------------------------------------

class TestRouteIsolation:
    def test_connector_routes_does_not_import_db_or_orchestrator(self):
        """Verify api.connector_routes has no direct dependency on db.* or orchestrator.

        Reads the source file as text and asserts the forbidden import prefixes are absent.
        This test enforces the W-C3.1 isolation requirement: all business logic must
        remain in ingest_from_connector(), not in the HTTP transport layer.
        """
        import importlib
        import pathlib

        mod = importlib.import_module("api.connector_routes")
        source_path = pathlib.Path(inspect.getfile(mod))
        source = source_path.read_text()

        # Check only non-comment, non-docstring lines for forbidden import statements.
        # A line is an import statement if it starts with "import " or "from "
        # (after stripping leading whitespace). This avoids false positives from
        # docstrings or comments that describe what is NOT imported.
        import_lines = [
            line.strip()
            for line in source.splitlines()
            if line.strip().startswith(("import ", "from "))
        ]
        import_source = "\n".join(import_lines)

        forbidden_prefixes = [
            "from db",
            "import db",
            "from orchestrator",
            "import orchestrator",
        ]
        for pattern in forbidden_prefixes:
            assert not any(line.startswith(pattern) for line in import_lines), (
                f"api/connector_routes.py must not contain the import statement '{pattern}'. "
                f"All DB and orchestrator access must remain in ingest_from_connector().\n"
                f"Import lines found:\n{import_source}"
            )


# ---------------------------------------------------------------------------
# 2. Adapter verdict tests — all four verdicts must produce HTTP 200
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
        """fetch_failed is an adapter verdict — must NOT be converted to an HTTP error."""
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
# 3. ConnectorEvent construction
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
# 4. Validation errors — 422
# ---------------------------------------------------------------------------

class TestValidationErrors:
    def test_missing_connector_module_returns_422(self):
        body = _minimal_body()
        del body["connector_module"]
        assert client.post(_ENDPOINT, json=body).status_code == 422

    def test_missing_source_uri_returns_422(self):
        body = _minimal_body()
        del body["source_uri"]
        assert client.post(_ENDPOINT, json=body).status_code == 422

    def test_missing_raw_bytes_b64_returns_422(self):
        body = _minimal_body()
        del body["raw_bytes_b64"]
        assert client.post(_ENDPOINT, json=body).status_code == 422

    def test_invalid_format_returns_422(self):
        assert client.post(_ENDPOINT, json=_minimal_body(format="xlsx")).status_code == 422

    def test_empty_connector_module_returns_422(self):
        assert client.post(_ENDPOINT, json=_minimal_body(connector_module="")).status_code == 422

    def test_empty_source_uri_returns_422(self):
        assert client.post(_ENDPOINT, json=_minimal_body(source_uri="")).status_code == 422


# ---------------------------------------------------------------------------
# 5. Base64 errors — 422
# ---------------------------------------------------------------------------

class TestBase64Errors:
    def test_invalid_base64_returns_422(self):
        resp = client.post(_ENDPOINT, json=_minimal_body(raw_bytes_b64="not-valid-base64!!!"))
        assert resp.status_code == 422
        assert "base64" in resp.json()["detail"].lower()

    def test_empty_raw_bytes_decodes_to_zero_bytes_passes_transport(self):
        """Empty base64 string decodes to 0 bytes — valid at transport layer."""
        result = _SyncResult(task_id="t1", verdict="fetched_new",
                             document_id="d1", job_id="j1", version=1)
        with patch(f"{_MOD}.ingest_from_connector", return_value=result):
            resp = client.post(_ENDPOINT, json=_minimal_body(raw_bytes_b64=_b64(b"")))
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# 6. Payload size — 413 enforced BEFORE decode
# ---------------------------------------------------------------------------

class TestSizeLimit:
    def test_oversized_b64_string_rejected_before_decode(self):
        """Pre-decode guard: b64 string length > _MAX_B64_CHARS returns 413.

        The oversized b64 string is built in-process but b64decode is NOT called
        by the route — the string-length check fires first. This verifies that
        unbounded memory allocation never occurs.
        """
        # A b64 string one character longer than the allowed maximum.
        # Actual b64decode is never called because the pre-check fires first.
        oversized_b64 = "A" * (_MAX_B64_CHARS + 1)
        resp = client.post(_ENDPOINT, json=_minimal_body(raw_bytes_b64=oversized_b64))
        assert resp.status_code == 413
        assert "50" in resp.json()["detail"]

    def test_b64_string_exactly_at_limit_is_accepted(self):
        """b64 string at exactly _MAX_B64_CHARS characters does not trigger the pre-check.

        We mock b64decode to avoid allocating 50 MiB in CI and mock ingest_from_connector
        to complete the request.
        """
        at_limit_b64 = "A" * _MAX_B64_CHARS

        result = _SyncResult(task_id="t1", verdict="fetched_new",
                             document_id="d1", job_id="j1", version=1)

        # Return bytes that fit within the limit
        safe_decoded = b"x" * 100
        with patch(f"{_MOD}.base64.b64decode", return_value=safe_decoded):
            with patch(f"{_MOD}.ingest_from_connector", return_value=result):
                resp = client.post(_ENDPOINT, json=_minimal_body(raw_bytes_b64=at_limit_b64))

        assert resp.status_code == 200

    def test_post_decode_size_guard_rejects_pathological_payload(self):
        """Defence-in-depth: if b64decode somehow returns > _MAX_DECODED_BYTES,
        the post-decode guard returns 413.

        This guards against non-standard b64 encoders that produce shorter
        strings than the canonical encoding for the same decoded size.
        """
        oversized_decoded = b"x" * (_MAX_DECODED_BYTES + 1)
        # The b64 string itself is short (passes pre-check); only the decoded
        # output exceeds the limit.
        with patch(f"{_MOD}.base64.b64decode", return_value=oversized_decoded):
            resp = client.post(_ENDPOINT, json=_minimal_body())

        assert resp.status_code == 413
