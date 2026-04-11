"""W-C3.5: Tests for POST /connectors/s3/sync HTTP endpoint.

Issue:  knowledge-ingest#49
Depends on: knowledge-ingest#37 (W-C2.1 S3 wrapper), knowledge-ingest#42 (W-C3.1)

Test boundary
-------------
- sync_s3_bucket() is stubbed via unittest.mock.patch — no S3 calls, no adapter calls.
- FastAPI TestClient is used — no real network.
- No DB, no orchestrator, no storage, no actual AWS credentials.
- Patch target: "api.s3_routes.sync_s3_bucket"

Tests cover:
  1. Valid request with named secret_ref → sync called with resolved credentials
  2. Valid request with default secret_ref → sync called with default AWS env vars
  3. Missing secret_ref env vars → 503 (credentials not found)
  4. Successful sync results rendered correctly in response
  5. fetch_failed verdict rendered correctly (not converted to HTTP error)
  6. S3 authentication failure (RuntimeError from wrapper) → 503
  7. Missing required field (scope) → 422
  8. Missing required field (bucket) → 422
  9. Plaintext access_key / secret_key NOT accepted in request body
 10. IBM COS path: no ibm_cos connector_module field exists in this endpoint
 11. s3_routes module does NOT import db.* or orchestrator directly
 12. UI section renders in operator console (GET /ui contains S3 Sync heading)
"""

import inspect
import os
from dataclasses import dataclass
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app  # PYTHONPATH=src required

client = TestClient(app)

_MOD = "api.s3_routes"
_ENDPOINT = "/connectors/s3/sync"

_NAMED_SECRET_REF = "prod"
_NAMED_ACCESS_KEY = "AKIANAMED"
_NAMED_SECRET_KEY = "namedSecretVal"
_DEFAULT_ACCESS_KEY = "AKIADEFAULT"
_DEFAULT_SECRET_KEY = "defaultSecretVal"


# ---------------------------------------------------------------------------
# Stub helpers
# ---------------------------------------------------------------------------

@dataclass
class _SyncResult:
    task_id: str
    verdict: str
    source_uri: str = ""
    document_id: Optional[str] = None
    job_id: Optional[str] = None
    version: Optional[int] = None
    error_message: Optional[str] = None


def _ok_results(n: int = 1, verdict: str = "fetched_new") -> list:
    return [
        _SyncResult(
            task_id=f"task-{i:03d}",
            verdict=verdict,
            source_uri=f"s3://test-bucket/doc{i}.pdf",
            document_id=f"doc-{i:03d}",
            job_id=f"job-{i:03d}",
            version=1,
        )
        for i in range(n)
    ]


def _minimal_body(**overrides) -> dict:
    body = {"scope": "test-kb/docs", "bucket": "my-bucket"}
    body.update(overrides)
    return body


# ---------------------------------------------------------------------------
# 1. Module isolation test
# ---------------------------------------------------------------------------

class TestS3RoutesIsolation:
    def test_s3_routes_does_not_import_db_or_orchestrator(self):
        """api/s3_routes.py must not contain db.* or orchestrator import statements."""
        import importlib
        import pathlib

        mod = importlib.import_module("api.s3_routes")
        source_path = pathlib.Path(inspect.getfile(mod))
        source = source_path.read_text()

        import_lines = [
            line.strip()
            for line in source.splitlines()
            if line.strip().startswith(("import ", "from "))
        ]
        forbidden_prefixes = [
            "from db",
            "import db",
            "from orchestrator",
            "import orchestrator",
        ]
        for pattern in forbidden_prefixes:
            assert not any(line.startswith(pattern) for line in import_lines), (
                f"api/s3_routes.py must not contain the import statement '{pattern}'. "
                f"All DB and orchestrator access must remain in the adapter.\n"
                f"Import lines found:\n" + "\n".join(import_lines)
            )


# ---------------------------------------------------------------------------
# 2. Credential resolution — named secret_ref
# ---------------------------------------------------------------------------

class TestNamedSecretRef:
    def test_named_secret_ref_passes_resolved_credentials_to_wrapper(self):
        """sync_s3_bucket is called with access_key/secret_key from named env vars."""
        captured = {}

        async def _fake_sync(config):
            captured["config"] = config
            return _ok_results(1)

        env_patch = {
            f"S3_SECRET_{_NAMED_SECRET_REF.upper()}_ACCESS_KEY": _NAMED_ACCESS_KEY,
            f"S3_SECRET_{_NAMED_SECRET_REF.upper()}_SECRET_KEY": _NAMED_SECRET_KEY,
        }
        with patch.dict(os.environ, env_patch):
            with patch(f"{_MOD}.sync_s3_bucket", side_effect=_fake_sync):
                resp = client.post(_ENDPOINT, json=_minimal_body(secret_ref=_NAMED_SECRET_REF))

        assert resp.status_code == 200
        assert captured["config"]["access_key"] == _NAMED_ACCESS_KEY
        assert captured["config"]["secret_key"] == _NAMED_SECRET_KEY
        assert captured["config"]["bucket_names"] == ["my-bucket"]
        assert captured["config"]["scope"] == "test-kb/docs"

    def test_named_secret_ref_missing_env_returns_503(self):
        """Missing env vars for a named secret_ref returns 503, not 500 or 422."""
        # Ensure the named env vars are NOT set
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(f"S3_SECRET_{_NAMED_SECRET_REF.upper()}")}
        with patch.dict(os.environ, env, clear=True):
            resp = client.post(_ENDPOINT, json=_minimal_body(secret_ref=_NAMED_SECRET_REF))

        assert resp.status_code == 503
        assert "S3_SECRET_PROD_ACCESS_KEY" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# 3. Credential resolution — default secret_ref
# ---------------------------------------------------------------------------

class TestDefaultSecretRef:
    def test_default_secret_ref_uses_aws_env_vars(self):
        """secret_ref 'default' or omitted uses AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY."""
        captured = {}

        async def _fake_sync(config):
            captured["config"] = config
            return _ok_results(1)

        env_patch = {
            "AWS_ACCESS_KEY_ID": _DEFAULT_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": _DEFAULT_SECRET_KEY,
        }
        with patch.dict(os.environ, env_patch):
            with patch(f"{_MOD}.sync_s3_bucket", side_effect=_fake_sync):
                resp = client.post(_ENDPOINT, json=_minimal_body())  # no secret_ref

        assert resp.status_code == 200
        assert captured["config"]["access_key"] == _DEFAULT_ACCESS_KEY
        assert captured["config"]["secret_key"] == _DEFAULT_SECRET_KEY

    def test_missing_default_aws_env_vars_returns_503(self):
        """Missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY returns 503."""
        env = {k: v for k, v in os.environ.items()
               if k not in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")}
        with patch.dict(os.environ, env, clear=True):
            resp = client.post(_ENDPOINT, json=_minimal_body())

        assert resp.status_code == 503
        assert "AWS_ACCESS_KEY_ID" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# 4. No plaintext credentials accepted in request body
# ---------------------------------------------------------------------------

class TestNoPlaintextCredentials:
    def test_request_body_has_no_access_key_field(self):
        """Sending access_key in the request body is silently ignored (extra field).

        The pydantic model does not define access_key or secret_key, so they
        are rejected or ignored at the schema level. We verify the resolved
        credentials come from env vars only, not from request body.
        """
        captured = {}

        async def _fake_sync(config):
            captured["config"] = config
            return _ok_results(1)

        env_patch = {
            "AWS_ACCESS_KEY_ID": _DEFAULT_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": _DEFAULT_SECRET_KEY,
        }
        # Attempt to inject credentials via request body
        poisoned_body = _minimal_body()
        poisoned_body["access_key"] = "INJECTED_KEY"
        poisoned_body["secret_key"] = "INJECTED_SECRET"

        with patch.dict(os.environ, env_patch):
            with patch(f"{_MOD}.sync_s3_bucket", side_effect=_fake_sync):
                resp = client.post(_ENDPOINT, json=poisoned_body)

        # Request succeeds but credentials come from env, not from body
        assert resp.status_code == 200
        assert captured["config"]["access_key"] == _DEFAULT_ACCESS_KEY
        assert captured["config"]["secret_key"] == _DEFAULT_SECRET_KEY
        assert captured["config"].get("access_key") != "INJECTED_KEY"

    def test_ibm_cos_connector_module_not_present_in_this_endpoint(self):
        """This endpoint handles AWS S3 only. There is no ibm_cos path here."""
        # The S3SyncRequest has no connector_module field and the route is
        # POST /connectors/s3/sync (S3-specific). IBM COS is W-C3.6 (#50).
        import importlib
        import pathlib
        mod = importlib.import_module("api.s3_routes")
        source = pathlib.Path(inspect.getfile(mod)).read_text()
        assert "ibm_cos" not in source.lower(), (
            "IBM COS must not appear in s3_routes.py — it belongs in W-C3.6 (#50)"
        )


# ---------------------------------------------------------------------------
# 5. Sync result rendering
# ---------------------------------------------------------------------------

class TestSyncResultRendering:
    def test_successful_sync_returns_200_with_result_list(self):
        results = _ok_results(3, verdict="fetched_new")

        env_patch = {"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s"}
        with patch.dict(os.environ, env_patch):
            with patch(f"{_MOD}.sync_s3_bucket", return_value=results):
                resp = client.post(_ENDPOINT, json=_minimal_body())

        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert data["scope"] == "test-kb/docs"
        assert data["bucket"] == "my-bucket"
        assert len(data["results"]) == 3
        assert data["results"][0]["verdict"] == "fetched_new"
        assert data["results"][0]["source_uri"].startswith("s3://")

    def test_fetch_failed_verdict_returns_200_not_5xx(self):
        """fetch_failed from the wrapper is an adapter verdict, not an HTTP error."""
        results = [_SyncResult(
            task_id="t1",
            verdict="fetch_failed",
            source_uri="s3://test-bucket/doc.pdf",
            error_message="Scope not registered",
        )]

        env_patch = {"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s"}
        with patch.dict(os.environ, env_patch):
            with patch(f"{_MOD}.sync_s3_bucket", return_value=results):
                resp = client.post(_ENDPOINT, json=_minimal_body())

        assert resp.status_code == 200
        data = resp.json()
        assert data["results"][0]["verdict"] == "fetch_failed"
        assert "not registered" in data["results"][0]["error_message"]

    def test_skipped_unchanged_returns_200(self):
        results = _ok_results(1, verdict="skipped_unchanged")
        results[0].document_id = "doc-abc"

        env_patch = {"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s"}
        with patch.dict(os.environ, env_patch):
            with patch(f"{_MOD}.sync_s3_bucket", return_value=results):
                resp = client.post(_ENDPOINT, json=_minimal_body())

        assert resp.status_code == 200
        assert resp.json()["results"][0]["verdict"] == "skipped_unchanged"

    def test_empty_bucket_returns_200_with_zero_results(self):
        env_patch = {"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s"}
        with patch.dict(os.environ, env_patch):
            with patch(f"{_MOD}.sync_s3_bucket", return_value=[]):
                resp = client.post(_ENDPOINT, json=_minimal_body())

        assert resp.status_code == 200
        assert resp.json()["total"] == 0
        assert resp.json()["results"] == []


# ---------------------------------------------------------------------------
# 6. S3 auth failure (RuntimeError from wrapper) → 503
# ---------------------------------------------------------------------------

class TestS3AuthFailure:
    def test_s3_authentication_failure_returns_503(self):
        env_patch = {"AWS_ACCESS_KEY_ID": "bad", "AWS_SECRET_ACCESS_KEY": "bad"}
        with patch.dict(os.environ, env_patch):
            with patch(f"{_MOD}.sync_s3_bucket",
                       side_effect=RuntimeError("S3 authentication failed")):
                resp = client.post(_ENDPOINT, json=_minimal_body())

        assert resp.status_code == 503
        assert "authentication" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# 7. Validation errors — 422
# ---------------------------------------------------------------------------

class TestValidationErrors:
    def test_missing_scope_returns_422(self):
        body = _minimal_body()
        del body["scope"]
        assert client.post(_ENDPOINT, json=body).status_code == 422

    def test_missing_bucket_returns_422(self):
        body = _minimal_body()
        del body["bucket"]
        assert client.post(_ENDPOINT, json=body).status_code == 422

    def test_empty_scope_returns_422(self):
        assert client.post(_ENDPOINT, json=_minimal_body(scope="")).status_code == 422

    def test_empty_bucket_returns_422(self):
        assert client.post(_ENDPOINT, json=_minimal_body(bucket="")).status_code == 422

    def test_invalid_secret_ref_characters_returns_422(self):
        # secret_ref with spaces or special chars is rejected
        assert client.post(_ENDPOINT, json=_minimal_body(secret_ref="bad ref!")).status_code == 422


# ---------------------------------------------------------------------------
# 8. Operator UI — S3 section renders
# ---------------------------------------------------------------------------

class TestOperatorUI:
    def test_ui_contains_s3_sync_section(self):
        resp = client.get("/ui")
        assert resp.status_code == 200
        assert "AWS S3 Sync" in resp.text

    def test_ui_s3_sync_submits_to_correct_endpoint(self):
        resp = client.get("/ui")
        assert "/connectors/s3/sync" in resp.text

    def test_ui_shows_source_uri_format_hint(self):
        """The S3 form must display a visible s3://{bucket}/{key} format hint."""
        resp = client.get("/ui")
        assert "s3://{bucket}/{key}" in resp.text

    def test_ui_does_not_expose_access_key_input(self):
        """The S3 form must not have an HTML input element for plaintext credentials.

        Checks that no <input> element with id or placeholder referencing access_key
        or secret_key exists in the S3 section. Descriptive prose text (explaining
        what env vars to set) is allowed — only input fields are forbidden.
        """
        resp = client.get("/ui")
        html = resp.text
        s3_section_start = html.find("hS3")
        s3_section_end = html.find("hDebug", s3_section_start)
        if s3_section_start == -1:
            pytest.fail("S3 section not found in operator UI")
        s3_html = html[s3_section_start:s3_section_end]

        # Extract only <input ...> tags from the S3 section
        import re
        input_tags = re.findall(r'<input[^>]*>', s3_html, re.IGNORECASE)
        input_tags_joined = " ".join(input_tags).lower()

        # No input element should reference access_key or secret_key as id/placeholder
        assert "access_key" not in input_tags_joined, (
            f"Found 'access_key' in an <input> element in the S3 form: {input_tags}"
        )
        assert "secret_key" not in input_tags_joined, (
            f"Found 'secret_key' in an <input> element in the S3 form: {input_tags}"
        )
