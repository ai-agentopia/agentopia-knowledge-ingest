"""Operator UI and document management API tests — Finding 3 remediation.

Covers:
1. Dashboard error handling
   - superRagFetch logic: missing token → clear error message
   - HTTP non-2xx from Super RAG → explicit error, not empty data

2. Document management API via TestClient
   - GET /documents?scope= — list documents
   - GET /documents/{id}/versions — version history
   - POST /documents/{id}/rollback — rollback action
   - Invalid inputs return correct status codes

3. Version-history and rollback flow
   - v1 active → v2 active → both appear in version history
   - rollback v1: v1 → active, v2 → superseded (verified via registry)

Tests use the in-memory registry stubs from test_ingest_e2e where needed.
No real database required.
"""

import contextlib
import os
import sys
import uuid
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.testclient import TestClient
import main  # noqa
from main import app

from db.registry import stable_document_id


# ── In-memory registry stubs ──────────────────────────────────────────────────

class _InMemDB:
    def __init__(self):
        self._rows = []
        self._next_row_id = 1

    def add_scope(self, name):
        pass  # scope_exists always returns True in stubs

    def make_doc(self, scope, filename, status="active", version=None):
        doc_id = stable_document_id(scope, filename)
        max_ver = max(
            (r["version"] for r in self._rows if r["document_id"] == doc_id),
            default=0,
        )
        ver = version if version is not None else max_ver + 1
        row_id = self._next_row_id; self._next_row_id += 1
        row = {
            "row_id": row_id, "document_id": doc_id, "version": ver,
            "scope": scope, "filename": filename, "format": "markdown",
            "status": status, "source_hash": f"hash{ver}",
            "s3_original_key": f"docs/{scope}/{doc_id}/v{ver}/orig.md",
            "s3_normalized_key": None, "s3_extracted_key": None,
            "error_message": None, "owner": "test",
            "created_at": "2026-04-11T00:00:00+00:00",
            "updated_at": "2026-04-11T00:00:00+00:00",
        }
        self._rows.append(row)
        return row

    def scope_exists(self, name):
        return True

    def list_documents(self, scope, status="active"):
        return [r for r in self._rows
                if r["scope"] == scope and (status == "all" or r["status"] == status)]

    def get_document_versions(self, document_id):
        return sorted(
            [r for r in self._rows if r["document_id"] == document_id],
            key=lambda r: r["version"], reverse=True,
        )

    def rollback_to_version(self, document_id, target_version):
        active = [r for r in self._rows if r["document_id"] == document_id and r["status"] == "active"]
        target = [r for r in self._rows if r["document_id"] == document_id and r["version"] == target_version]
        if not target:
            raise ValueError(f"Version {target_version} not found for {document_id}")
        if target[0]["status"] == "active":
            raise ValueError(f"Version {target_version} is already active")
        if target[0]["status"] == "deleted":
            raise ValueError(f"Version {target_version} is deleted")
        for r in active:
            r["status"] = "superseded"
        target[0]["status"] = "active"
        return {
            "document_id": document_id,
            "restored_version": target_version,
            "superseded_version": active[0]["version"] if active else None,
        }

    def write_audit_event(self, **kwargs):
        pass  # no-op in tests


_DB = _InMemDB()


def _patches(db: _InMemDB):
    return [
        patch("db.registry.scope_exists", side_effect=db.scope_exists),
        patch("db.registry.list_documents", side_effect=db.list_documents),
        patch("db.registry.get_document_versions", side_effect=db.get_document_versions),
        patch("db.registry.rollback_to_version", side_effect=db.rollback_to_version),
        patch("db.registry.write_audit_event", side_effect=db.write_audit_event),
    ]


def _apply(patches):
    return contextlib.ExitStack().__enter__(), [p.__enter__() for p in patches]


# ── Tests: dashboard error handling logic ─────────────────────────────────────

class TestDashboardErrorHandling(unittest.TestCase):
    """Validates the superRagFetch error-handling contract in the UI JavaScript.

    We can't run JS in Python tests, so we validate the HTML string directly:
    - superRagFetch is defined and checks resp.ok
    - loadBaselines() uses superRagFetch (not raw fetch)
    - loadResults() uses superRagFetch (not raw fetch)
    - error elements exist in DOM (baselineErr, resultsErr)
    """

    def _get_ui_html(self):
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/ui")
        self.assertEqual(resp.status_code, 200)
        return resp.text

    def test_superRagFetch_function_defined(self):
        """superRagFetch must be defined in the UI JavaScript."""
        html = self._get_ui_html()
        self.assertIn("async function superRagFetch", html,
                      "superRagFetch not found — loadBaselines/loadResults will not check resp.ok")

    def test_superRagFetch_checks_resp_ok(self):
        """superRagFetch must check resp.ok and throw on failure."""
        html = self._get_ui_html()
        # The function must contain a resp.ok check
        self.assertIn("resp.ok", html,
                      "superRagFetch must check resp.ok to surface HTTP errors")

    def test_loadBaselines_uses_superRagFetch(self):
        """loadBaselines must call superRagFetch, not raw fetch directly."""
        html = self._get_ui_html()
        # Find the loadBaselines function body
        start = html.find("async function loadBaselines")
        end = html.find("async function", start + 10)
        fn_body = html[start:end]
        self.assertIn("superRagFetch", fn_body,
                      "loadBaselines must use superRagFetch (which checks resp.ok)")
        self.assertNotIn("fetch(superRagUrl", fn_body,
                         "loadBaselines must not call raw fetch — use superRagFetch instead")

    def test_loadResults_uses_superRagFetch(self):
        """loadResults must call superRagFetch, not raw fetch directly."""
        html = self._get_ui_html()
        start = html.find("async function loadResults")
        end = html.find("async function", start + 10)
        fn_body = html[start:end]
        self.assertIn("superRagFetch", fn_body,
                      "loadResults must use superRagFetch (which checks resp.ok)")

    def test_error_elements_exist_in_dom(self):
        """baselineErr and resultsErr elements must exist so errors can be displayed."""
        html = self._get_ui_html()
        self.assertIn('id="baselineErr"', html,
                      "baselineErr element missing — baseline errors cannot be shown")
        self.assertIn('id="resultsErr"', html,
                      "resultsErr element missing — results errors cannot be shown")

    def test_missing_token_message_defined(self):
        """superRagFetch must provide a clear message when token is missing."""
        html = self._get_ui_html()
        self.assertIn("Internal Token is required", html,
                      "Missing token must produce a clear operator-facing error message")

    def test_http_error_message_surfaced(self):
        """superRagFetch must include HTTP status code in the thrown error."""
        html = self._get_ui_html()
        # Check that the error includes HTTP status info
        self.assertIn("HTTP ", html,
                      "superRagFetch error must include HTTP status code")

    def test_trend_summary_element_exists(self):
        """trendSummary element must exist for the regression-history trend view."""
        html = self._get_ui_html()
        self.assertIn('id="trendSummary"', html,
                      "trendSummary element missing — regression trend view incomplete")

    def test_results_table_shows_mrr_column(self):
        """Results table must include MRR column (full metric set per agreed scope)."""
        html = self._get_ui_html()
        # MRR column header appears in the results table template
        self.assertIn("MRR", html,
                      "Results table must show MRR column (full metric set)")


# ── Tests: document management API ───────────────────────────────────────────

class TestDocumentListAPI(unittest.TestCase):

    def setUp(self):
        self._db = _InMemDB()
        self._client = TestClient(app, raise_server_exceptions=False)

    def test_list_active_documents(self):
        """GET /documents?scope= returns active documents."""
        self._db.make_doc("test/scope", "guide.md", status="active")

        with contextlib.ExitStack() as stack:
            for p in _patches(self._db):
                stack.enter_context(p)
            resp = self._client.get("/documents?scope=test/scope&status=active")

        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("documents", data)
        self.assertEqual(len(data["documents"]), 1)
        self.assertEqual(data["documents"][0]["filename"], "guide.md")

    def test_list_all_statuses(self):
        """GET /documents?scope=&status=all returns all version rows."""
        self._db.make_doc("test/scope", "spec.md", status="active")
        self._db.make_doc("test/scope", "spec.md", status="superseded")

        with contextlib.ExitStack() as stack:
            for p in _patches(self._db):
                stack.enter_context(p)
            resp = self._client.get("/documents?scope=test/scope&status=all")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.json()["documents"]), 2)

    def test_missing_scope_param_returns_422(self):
        resp = self._client.get("/documents")
        self.assertEqual(resp.status_code, 422)

    def test_unknown_scope_returns_422(self):
        """Non-existent scope returns 422 (validated before listing)."""
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch("db.registry.scope_exists", return_value=False))
            resp = self._client.get("/documents?scope=unknown/scope")
        self.assertEqual(resp.status_code, 422)


class TestVersionHistoryAPI(unittest.TestCase):

    def setUp(self):
        self._db = _InMemDB()
        self._client = TestClient(app, raise_server_exceptions=False)

    def test_versions_returned_newest_first(self):
        """GET /documents/{id}/versions returns all versions, newest first."""
        doc_id = stable_document_id("test/scope", "api.md")
        self._db.make_doc("test/scope", "api.md", status="superseded", version=1)
        self._db.make_doc("test/scope", "api.md", status="active", version=2)

        with contextlib.ExitStack() as stack:
            for p in _patches(self._db):
                stack.enter_context(p)
            resp = self._client.get(f"/documents/{doc_id}/versions")

        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["document_id"], doc_id)
        versions = data["versions"]
        self.assertEqual(len(versions), 2)
        # Newest first
        self.assertEqual(versions[0]["version"], 2)
        self.assertEqual(versions[0]["status"], "active")
        self.assertEqual(versions[1]["version"], 1)
        self.assertEqual(versions[1]["status"], "superseded")

    def test_unknown_document_returns_404(self):
        with contextlib.ExitStack() as stack:
            for p in _patches(self._db):
                stack.enter_context(p)
            resp = self._client.get("/documents/nonexistent-uuid/versions")
        self.assertEqual(resp.status_code, 404)

    def test_version_fields_include_source_hash(self):
        """Each version entry must include source_hash for provenance."""
        doc_id = stable_document_id("test/scope", "ref.md")
        self._db.make_doc("test/scope", "ref.md", status="active")
        with contextlib.ExitStack() as stack:
            for p in _patches(self._db):
                stack.enter_context(p)
            resp = self._client.get(f"/documents/{doc_id}/versions")
        v = resp.json()["versions"][0]
        self.assertIn("source_hash", v, "version entry must include source_hash")
        self.assertIn("created_at", v, "version entry must include created_at")


class TestRollbackAPIFlow(unittest.TestCase):

    def setUp(self):
        self._db = _InMemDB()
        self._client = TestClient(app, raise_server_exceptions=False)

    def test_rollback_restores_prior_version(self):
        """POST /documents/{id}/rollback: v1 superseded, v2 active → v1 active, v2 superseded."""
        doc_id = stable_document_id("test/scope", "doc.md")
        self._db.make_doc("test/scope", "doc.md", status="superseded", version=1)
        self._db.make_doc("test/scope", "doc.md", status="active", version=2)

        with contextlib.ExitStack() as stack:
            for p in _patches(self._db):
                stack.enter_context(p)
            resp = self._client.post(
                f"/documents/{doc_id}/rollback",
                json={"version": 1, "reason": "v2 regression"},
            )

        self.assertEqual(resp.status_code, 202)
        data = resp.json()
        self.assertEqual(data["restored_version"], 1)
        self.assertEqual(data["superseded_version"], 2)

        # Verify in-memory state
        v1 = next(r for r in self._db._rows if r["document_id"] == doc_id and r["version"] == 1)
        v2 = next(r for r in self._db._rows if r["document_id"] == doc_id and r["version"] == 2)
        self.assertEqual(v1["status"], "active")
        self.assertEqual(v2["status"], "superseded")

    def test_rollback_to_already_active_returns_400(self):
        """Rollback to the currently active version must return 400."""
        doc_id = stable_document_id("test/scope", "spec.md")
        self._db.make_doc("test/scope", "spec.md", status="active", version=1)

        with contextlib.ExitStack() as stack:
            for p in _patches(self._db):
                stack.enter_context(p)
            resp = self._client.post(
                f"/documents/{doc_id}/rollback",
                json={"version": 1},
            )

        self.assertEqual(resp.status_code, 400)
        self.assertIn("already active", resp.json()["detail"])

    def test_rollback_to_nonexistent_version_returns_400(self):
        doc_id = stable_document_id("test/scope", "x.md")
        self._db.make_doc("test/scope", "x.md", status="active", version=1)

        with contextlib.ExitStack() as stack:
            for p in _patches(self._db):
                stack.enter_context(p)
            resp = self._client.post(
                f"/documents/{doc_id}/rollback",
                json={"version": 99},
            )

        self.assertEqual(resp.status_code, 400)
        self.assertIn("not found", resp.json()["detail"])

    def test_rollback_reason_recorded_in_audit(self):
        """Rollback reason must be passed to audit log (write_audit_event called)."""
        doc_id = stable_document_id("test/scope", "logged.md")
        self._db.make_doc("test/scope", "logged.md", status="superseded", version=1)
        self._db.make_doc("test/scope", "logged.md", status="active", version=2)

        audit_calls = []
        def capture_audit(**kwargs):
            audit_calls.append(kwargs)

        db_local = self._db
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch("db.registry.scope_exists", side_effect=db_local.scope_exists))
            stack.enter_context(patch("db.registry.list_documents", side_effect=db_local.list_documents))
            stack.enter_context(patch("db.registry.get_document_versions", side_effect=db_local.get_document_versions))
            stack.enter_context(patch("db.registry.rollback_to_version", side_effect=db_local.rollback_to_version))
            stack.enter_context(patch("db.registry.write_audit_event", side_effect=capture_audit))

            self._client.post(
                f"/documents/{doc_id}/rollback",
                json={"version": 1, "reason": "quality regression"},
            )

        self.assertTrue(len(audit_calls) > 0, "write_audit_event must be called on rollback")
        rollback_events = [c for c in audit_calls if c.get("action") == "rolled_back"]
        self.assertTrue(len(rollback_events) > 0, "rolled_back audit event must be written")
        meta = rollback_events[0].get("metadata", {})
        self.assertIn("reason", meta, "rollback reason must appear in audit metadata")
        self.assertEqual(meta["reason"], "quality regression")


class TestScopeManagementAPI(unittest.TestCase):

    def setUp(self):
        self._client = TestClient(app, raise_server_exceptions=False)

    def test_invalid_scope_name_returns_422(self):
        """POST /scopes with invalid scope_name returns 422."""
        with patch("db.registry.create_scope", side_effect=ValueError("exists")):
            resp = self._client.post("/scopes", json={"scope_name": "INVALID_FORMAT"})
        self.assertEqual(resp.status_code, 422)

    def test_duplicate_scope_returns_409(self):
        """POST /scopes with existing scope_name returns 409."""
        with patch("db.registry.create_scope", side_effect=ValueError("already exists")):
            resp = self._client.post("/scopes", json={"scope_name": "test/scope"})
        self.assertEqual(resp.status_code, 409)

    def test_list_scopes_returns_200(self):
        """GET /scopes returns 200 with scopes list."""
        with patch("db.registry.list_scopes", return_value=[
            {"scope_id": str(uuid.uuid4()), "scope_name": "test/scope",
             "description": None, "owner": None,
             "created_at": "2026-04-11T00:00:00+00:00", "document_count": 0}
        ]):
            resp = self._client.get("/scopes")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("scopes", resp.json())


if __name__ == "__main__":
    unittest.main()
