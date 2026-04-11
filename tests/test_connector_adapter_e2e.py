"""W-C1.12: Connector adapter E2E test.

Tests the full ingest_from_connector() flow:
  1. First sync of a new source_uri → fetched_new, document created, job created
  2. Re-sync with same raw_bytes (same hash) → skipped_unchanged, no new document row
  3. Re-sync with changed raw_bytes (different hash) → fetched_updated, new version created
  4. Orchestrator failure does NOT change the sync verdict (W-C1.8)
  5. source_uri with commit SHA is rejected with fetch_failed verdict

All DB calls are stubbed via unittest.mock.patch — no real PostgreSQL required.
The orchestrator is also stubbed at network boundary (httpx.post) consistent
with the existing test_ingest_e2e.py pattern.

Stub architecture:
  - db.registry: patched to use in-memory dicts
  - db.connector_sync: patched to use in-memory dicts
  - orchestrator.run_pipeline: patched to record calls (simulates success/failure)
"""

import hashlib
import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call
import pathlib

_SRC = pathlib.Path(__file__).parent.parent / "src"
sys.path.insert(0, str(_SRC))


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


# ── In-memory DB stubs ────────────────────────────────────────────────────────

class _InMemoryRegistry:
    """Minimal in-memory stub for db.registry used by adapter tests."""

    def __init__(self):
        self._docs: dict[str, dict] = {}    # document_id → active row dict
        self._rows: dict[int, dict] = {}    # row_id → row dict
        self._next_row_id = 1
        self._jobs: dict[str, dict] = {}    # job_id → job dict
        import uuid as _uuid
        self._uuid = _uuid

    def stable_document_id_connector(self, scope: str, connector_module: str, source_uri: str) -> str:
        from db.registry import stable_document_id_connector as real_fn
        return real_fn(scope, connector_module, source_uri)

    def get_active_version(self, document_id: str) -> dict | None:
        return self._docs.get(document_id)

    def create_document(self, *, scope, filename, format, source_hash,
                        s3_original_key, owner="",
                        connector_module=None, source_uri=None, source_revision=None) -> dict:
        from db.registry import stable_document_id_connector, stable_document_id_manual
        if connector_module:
            doc_id = stable_document_id_connector(scope, connector_module, source_uri)
        else:
            doc_id = stable_document_id_manual(scope, filename)

        existing_versions = [r["version"] for r in self._rows.values()
                             if r["document_id"] == doc_id]
        version = max(existing_versions, default=0) + 1

        row_id = self._next_row_id
        self._next_row_id += 1
        row = {
            "row_id": row_id,
            "document_id": doc_id,
            "version": version,
            "scope": scope,
            "filename": filename,
            "format": format,
            "status": "submitted",
            "source_hash": source_hash,
            "s3_original_key": s3_original_key,
            "connector_module": connector_module,
            "source_uri": source_uri,
            "source_revision": source_revision,
            "created_at": "2026-01-01T00:00:00",
        }
        self._rows[row_id] = row
        return row

    def create_job(self, row_id: int) -> str:
        import uuid as _uuid
        job_id = str(_uuid.uuid4())
        self._jobs[job_id] = {"job_id": job_id, "row_id": row_id, "status": "submitted"}
        return job_id

    def promote_to_active(self, row_id: int, document_id: str) -> int | None:
        old = self._docs.get(document_id)
        old_row_id = old["row_id"] if old else None
        if old_row_id:
            self._rows[old_row_id]["status"] = "superseded"
        self._rows[row_id]["status"] = "active"
        self._docs[document_id] = self._rows[row_id]
        return old_row_id

    def update_document_status(self, row_id: int, status: str, **kwargs) -> None:
        if row_id in self._rows:
            self._rows[row_id]["status"] = status
            for k, v in kwargs.items():
                if v is not None:
                    self._rows[row_id][k] = v

    def update_job(self, job_id: str, *, status: str, **kwargs) -> None:
        if job_id in self._jobs:
            self._jobs[job_id]["status"] = status

    def write_audit_event(self, **kwargs) -> None:
        pass  # No-op in tests


class _InMemoryConnectorSync:
    """Minimal in-memory stub for db.connector_sync used by adapter tests."""

    def __init__(self):
        self._tasks: dict[str, dict] = {}
        import uuid as _uuid
        self._uuid = _uuid

    def create_sync_task(self, *, connector_module, scope, source_uri) -> str:
        task_id = str(self._uuid.uuid4())
        self._tasks[task_id] = {
            "task_id": task_id,
            "connector_module": connector_module,
            "scope": scope,
            "source_uri": source_uri,
            "status": "queued",
            "verdict": None,
            "observed_source_revision": None,
            "document_id": None,
            "job_id": None,
            "error_message": None,
        }
        return task_id

    def mark_fetching(self, task_id: str) -> None:
        self._tasks[task_id]["status"] = "fetching"

    def set_verdict(self, task_id: str, *, verdict, observed_source_revision=None,
                    document_id=None, job_id=None, error_message=None) -> None:
        t = self._tasks[task_id]
        t["verdict"] = verdict
        t["status"] = "failed" if verdict == "fetch_failed" else "fetched"
        if observed_source_revision is not None:
            t["observed_source_revision"] = observed_source_revision
        if document_id is not None:
            t["document_id"] = document_id
        if job_id is not None:
            t["job_id"] = job_id
        if error_message is not None:
            t["error_message"] = error_message

    def get_task(self, task_id: str) -> dict | None:
        return self._tasks.get(task_id)


# ── Test cases ────────────────────────────────────────────────────────────────

class TestConnectorAdapterE2E(unittest.TestCase):
    """E2E tests for ingest_from_connector() with in-memory registry/sync stubs."""

    def _make_stubs(self):
        reg = _InMemoryRegistry()
        sync = _InMemoryConnectorSync()
        return reg, sync

    def _run_with_stubs(self, event, reg, sync, *, orchestrator_raises=None):
        """Patch adapter dependencies and call ingest_from_connector."""
        from connectors import adapter as _mod

        pipeline_mock = MagicMock(return_value=None)
        if orchestrator_raises is not None:
            pipeline_mock.side_effect = orchestrator_raises

        with (
            patch.object(_mod, "registry", reg),
            patch.object(_mod, "create_sync_task", sync.create_sync_task),
            patch.object(_mod, "mark_fetching", sync.mark_fetching),
            patch.object(_mod, "set_verdict", sync.set_verdict),
            patch.object(_mod, "get_task", sync.get_task),
            patch.object(_mod, "run_pipeline", pipeline_mock),
        ):
            result = _mod.ingest_from_connector(event)
        return result

    def _make_event(self, raw_bytes=b"content v1", source_revision="abc123"):
        from connectors.adapter import ConnectorEvent
        return ConnectorEvent(
            connector_module="openrag",
            scope="joblogic-kb/docs",
            source_uri="joblogic/api-reference.pdf",
            filename="api-reference.pdf",
            format="pdf",
            raw_bytes=raw_bytes,
            source_revision=source_revision,
            owner="test-operator",
        )

    # ── Test 1: First sync → fetched_new ─────────────────────────────────────

    def test_first_sync_produces_fetched_new(self):
        """First time a source_uri is synced → verdict is fetched_new."""
        reg, sync = self._make_stubs()
        event = self._make_event()
        result = self._run_with_stubs(event, reg, sync)

        self.assertEqual(result.verdict, "fetched_new")
        self.assertIsNotNone(result.document_id)
        self.assertIsNotNone(result.job_id)
        self.assertEqual(result.version, 1)

    def test_first_sync_creates_document_row(self):
        """First sync must create a document row in registry with connector identity fields."""
        reg, sync = self._make_stubs()
        event = self._make_event()
        result = self._run_with_stubs(event, reg, sync)

        self.assertEqual(result.verdict, "fetched_new")
        # Row must exist in registry (promote_to_active runs inside orchestrator which is mocked;
        # the row is created by create_document and lives in _rows)
        rows_with_doc = [r for r in reg._rows.values() if r["document_id"] == result.document_id]
        self.assertGreater(len(rows_with_doc), 0, "create_document must have been called")
        row = rows_with_doc[0]
        self.assertEqual(row["connector_module"], "openrag")
        self.assertEqual(row["source_uri"], "joblogic/api-reference.pdf")
        self.assertEqual(row["source_revision"], "abc123")

    def test_first_sync_sync_task_has_correct_state(self):
        """First sync task must reach status=fetched with verdict=fetched_new."""
        reg, sync = self._make_stubs()
        event = self._make_event()
        result = self._run_with_stubs(event, reg, sync)

        task = sync.get_task(result.task_id)
        self.assertIsNotNone(task)
        self.assertEqual(task["status"], "fetched")
        self.assertEqual(task["verdict"], "fetched_new")
        self.assertEqual(task["observed_source_revision"], "abc123")
        self.assertEqual(task["document_id"], result.document_id)
        self.assertEqual(task["job_id"], result.job_id)

    # ── Test 2: Re-sync with same hash → skipped_unchanged ───────────────────

    def test_resync_same_content_produces_skipped_unchanged(self):
        """Re-sync with identical raw_bytes → verdict is skipped_unchanged."""
        reg, sync = self._make_stubs()
        raw = b"stable content"
        event = self._make_event(raw_bytes=raw, source_revision="rev1")

        # First sync — creates document
        r1 = self._run_with_stubs(event, reg, sync)
        self.assertEqual(r1.verdict, "fetched_new")

        # Manually put the active document into registry so dedup check works
        doc_id = r1.document_id
        # The stub's promote_to_active is called by run_pipeline stub (which is no-op)
        # Simulate active state manually:
        active_row = [r for r in reg._rows.values() if r["document_id"] == doc_id][0]
        reg._docs[doc_id] = active_row

        # Re-sync — same content
        event2 = self._make_event(raw_bytes=raw, source_revision="rev2")
        r2 = self._run_with_stubs(event2, reg, sync)

        self.assertEqual(r2.verdict, "skipped_unchanged")
        self.assertIsNone(r2.job_id)
        self.assertIsNone(r2.version)

    def test_skipped_unchanged_does_not_create_new_document_row(self):
        """skipped_unchanged must not add a new row to the documents table."""
        reg, sync = self._make_stubs()
        raw = b"stable content"
        event = self._make_event(raw_bytes=raw)

        r1 = self._run_with_stubs(event, reg, sync)
        doc_id = r1.document_id
        active_row = [r for r in reg._rows.values() if r["document_id"] == doc_id][0]
        reg._docs[doc_id] = active_row
        row_count_before = len(reg._rows)

        event2 = self._make_event(raw_bytes=raw, source_revision="rev2")
        self._run_with_stubs(event2, reg, sync)

        self.assertEqual(
            len(reg._rows), row_count_before,
            "skipped_unchanged must not create a new document row",
        )

    def test_skipped_unchanged_records_observed_revision(self):
        """skipped_unchanged sync task must record observed_source_revision."""
        reg, sync = self._make_stubs()
        raw = b"stable content"
        event = self._make_event(raw_bytes=raw, source_revision="rev1")

        r1 = self._run_with_stubs(event, reg, sync)
        doc_id = r1.document_id
        active_row = [r for r in reg._rows.values() if r["document_id"] == doc_id][0]
        reg._docs[doc_id] = active_row

        event2 = self._make_event(raw_bytes=raw, source_revision="rev2-new-stamp")
        r2 = self._run_with_stubs(event2, reg, sync)

        task = sync.get_task(r2.task_id)
        self.assertEqual(task["observed_source_revision"], "rev2-new-stamp")

    # ── Test 3: Changed content → fetched_updated ────────────────────────────

    def test_resync_changed_content_produces_fetched_updated(self):
        """Re-sync with different raw_bytes → verdict is fetched_updated, new version created."""
        reg, sync = self._make_stubs()
        event = self._make_event(raw_bytes=b"version 1", source_revision="rev1")

        r1 = self._run_with_stubs(event, reg, sync)
        doc_id = r1.document_id
        active_row = [r for r in reg._rows.values() if r["document_id"] == doc_id][0]
        reg._docs[doc_id] = active_row

        event2 = self._make_event(raw_bytes=b"version 2 content changed", source_revision="rev2")
        r2 = self._run_with_stubs(event2, reg, sync)

        self.assertEqual(r2.verdict, "fetched_updated")
        self.assertEqual(r2.document_id, doc_id, "Same logical document_id for same source_uri")
        self.assertEqual(r2.version, 2, "Version must increment for updated content")
        self.assertIsNotNone(r2.job_id)

    # ── Test 4: Orchestrator failure does NOT change sync verdict (W-C1.8) ───

    def test_orchestrator_failure_does_not_change_sync_verdict(self):
        """If run_pipeline raises, the sync verdict must remain fetched_new (not fetch_failed)."""
        reg, sync = self._make_stubs()
        event = self._make_event()

        result = self._run_with_stubs(
            event, reg, sync,
            orchestrator_raises=RuntimeError("embedding service unavailable"),
        )

        # Sync verdict must be fetched_new even though orchestrator failed
        task = sync.get_task(result.task_id)
        self.assertIsNotNone(task)
        self.assertIn(
            task["verdict"], ("fetched_new", "fetched_updated"),
            "Orchestrator failure must not revert sync verdict to fetch_failed",
        )
        self.assertEqual(task["status"], "fetched")

    # ── Test 5: SHA in source_uri → fetch_failed ─────────────────────────────

    def test_sha_in_source_uri_produces_fetch_failed(self):
        """source_uri with a 40-char commit SHA must yield fetch_failed verdict."""
        from connectors.adapter import ConnectorEvent
        sha = "a3f9c2d1" + "b" * 32
        event = ConnectorEvent(
            connector_module="openrag",
            scope="scope/test",
            source_uri=f"https://github.com/org/repo/blob/{sha}/docs/api.md",
            filename="api.md",
            format="markdown",
            raw_bytes=b"content",
            source_revision=sha,
        )

        reg, sync = self._make_stubs()
        result = self._run_with_stubs(event, reg, sync)

        self.assertEqual(result.verdict, "fetch_failed")
        self.assertIsNotNone(result.error_message)
        task = sync.get_task(result.task_id)
        self.assertEqual(task["verdict"], "fetch_failed")
        self.assertEqual(task["status"], "failed")

    # ── Test: connector document identity fields are set ─────────────────────

    def test_connector_identity_fields_stored_on_document_row(self):
        """connector_module, source_uri, and source_revision are stored on the document row."""
        reg, sync = self._make_stubs()
        event = self._make_event(source_revision="git-sha-abc123")
        result = self._run_with_stubs(event, reg, sync)

        rows = [r for r in reg._rows.values() if r["document_id"] == result.document_id]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["connector_module"], "openrag")
        self.assertEqual(row["source_uri"], "joblogic/api-reference.pdf")
        self.assertEqual(row["source_revision"], "git-sha-abc123",
                         "source_revision must be set on INSERT from event.source_revision")


if __name__ == "__main__":
    unittest.main()
