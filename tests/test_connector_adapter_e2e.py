"""W-C1.12: Connector adapter E2E tests.

Two test classes:

TestConnectorAdapterStubbed
    Exercises sync state machine logic, verdict assignment, scope resolution,
    and provenance fields using full in-memory stubs for registry and connector_sync.
    Super RAG and orchestrator are also stubbed.

TestConnectorAdapterRealPipeline  (required by W-C1.12)
    Exercises the real adapter → real normalizer → real extractor → real orchestrator
    path, with Super RAG mocked only at the network boundary (httpx.post).
    Registry is stubbed in-memory (no real PostgreSQL required).
    Connector sync state is stubbed in-memory.
    Storage backend is local filesystem (temp dir).

    This mirrors the pattern used in test_ingest_e2e.py:
      "Real orchestrator code paths with httpx mocked at network boundary."

Scope of each test class:
  Stubbed — verdict semantics, scope resolution, schema fields (source_hash_observed,
             resulting_row_id), skipped_unchanged dedup, SHA rejection.
  RealPipeline — full data flow from ConnectorEvent bytes through normalizer, extractor,
                 Super RAG handoff, and promote_to_active.
"""

import hashlib
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch, patch as _patch

import pathlib

_SRC = pathlib.Path(__file__).parent.parent / "src"
sys.path.insert(0, str(_SRC))

# ── Shared env setup for real-pipeline tests ──────────────────────────────────
_TMP_STORE = tempfile.mkdtemp(prefix="ki_connector_test_")
os.environ.setdefault("STORAGE_LOCAL_PATH", _TMP_STORE)
os.environ.setdefault("SUPER_RAG_URL", "http://localhost:8002")
os.environ.setdefault("SUPER_RAG_INTERNAL_TOKEN", "test-token")
os.environ.setdefault("SUPER_RAG_INGEST_MAX_RETRIES", "1")
os.environ.setdefault("DATABASE_URL", "")


# ── In-memory stubs ───────────────────────────────────────────────────────────

class _InMemoryRegistry:
    """In-memory registry stub that mirrors the real versioning contract.

    Extended from test_ingest_e2e.py's _InMemoryRegistry with connector fields.
    """

    def __init__(self, known_scopes: set[str] | None = None):
        self._rows: list[dict] = []
        self._jobs: list[dict] = []
        self._audit: list[dict] = []
        self._next_row_id = 1
        # If None, scope_exists always returns True (permissive default for most tests).
        # Set to a frozenset to enable strict scope validation.
        self._known_scopes = known_scopes

    # Identity functions — use real implementations (no DB needed)
    def stable_document_id_manual(self, scope, filename):
        from db.registry import stable_document_id_manual as real_fn
        return real_fn(scope, filename)

    def stable_document_id_connector(self, scope, connector_module, source_uri):
        from db.registry import stable_document_id_connector as real_fn
        return real_fn(scope, connector_module, source_uri)

    def get_active_version(self, document_id):
        for r in self._rows:
            if r["document_id"] == document_id and r["status"] == "active":
                return r
        return None

    def create_document(self, *, scope, filename, format, source_hash, s3_original_key,
                        owner="", connector_module=None, source_uri=None, source_revision=None):
        from db.registry import stable_document_id_connector, stable_document_id_manual
        if connector_module:
            doc_id = stable_document_id_connector(scope, connector_module, source_uri)
        else:
            doc_id = stable_document_id_manual(scope, filename)

        max_ver = max(
            (r["version"] for r in self._rows if r["document_id"] == doc_id),
            default=0,
        )
        version = max_ver + 1
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
            "s3_normalized_key": None,
            "s3_extracted_key": None,
            "error_message": None,
            "owner": owner,
            "connector_module": connector_module,
            "source_uri": source_uri,
            "source_revision": source_revision,  # IMMUTABLE — set once here
            "created_at": "2026-04-11T00:00:00+00:00",
            "updated_at": "2026-04-11T00:00:00+00:00",
        }
        self._rows.append(row)
        return row

    def update_document_status(self, row_id, status, *, s3_normalized_key=None,
                                s3_extracted_key=None, error_message=None):
        for r in self._rows:
            if r["row_id"] == row_id:
                r["status"] = status
                if s3_normalized_key:
                    r["s3_normalized_key"] = s3_normalized_key
                if s3_extracted_key:
                    r["s3_extracted_key"] = s3_extracted_key
                if error_message:
                    r["error_message"] = error_message
                break

    def promote_to_active(self, row_id, document_id):
        """Atomic swap: supersede prior active, promote new row. Mirrors production contract."""
        active_rows = [r for r in self._rows
                       if r["document_id"] == document_id and r["status"] == "active"]
        superseded_row_id = None
        for r in active_rows:
            if r["row_id"] != row_id:
                r["status"] = "superseded"
                superseded_row_id = r["row_id"]
        for r in self._rows:
            if r["row_id"] == row_id:
                r["status"] = "active"
                break
        return superseded_row_id

    def patch_s3_original_key(self, row_id, key):
        for r in self._rows:
            if r["row_id"] == row_id:
                r["s3_original_key"] = key
                break

    def create_job(self, row_id):
        import uuid as _uuid
        job_id = str(_uuid.uuid4())
        row = next((r for r in self._rows if r["row_id"] == row_id), None)
        self._jobs.append({
            "job_id": job_id,
            "row_id": row_id,
            "status": "submitted",
            "stage": None,
            "progress_percent": 0,
            "error_message": None,
        })
        return job_id

    def update_job(self, job_id, *, status, stage=None, progress_percent=None,
                   error_message=None, completed=False):
        for j in self._jobs:
            if j["job_id"] == job_id:
                j["status"] = status
                if stage:
                    j["stage"] = stage
                if progress_percent is not None:
                    j["progress_percent"] = progress_percent
                if error_message:
                    j["error_message"] = error_message
                break

    def get_job(self, job_id):
        return next((j for j in self._jobs if j["job_id"] == job_id), None)

    def scope_exists(self, scope_name):
        # By default all scopes exist; tests can override by populating _known_scopes
        if self._known_scopes is None:
            return True
        return scope_name in self._known_scopes

    def write_audit_event(self, **kwargs):
        self._audit.append(kwargs)

    def active_row(self, document_id):
        return next(
            (r for r in self._rows if r["document_id"] == document_id and r["status"] == "active"),
            None,
        )


class _InMemoryConnectorSync:
    """In-memory stub for db.connector_sync operations."""

    def __init__(self):
        self._tasks: dict[str, dict] = {}
        import uuid as _uuid
        self._uuid = _uuid

    def create_sync_task(self, *, connector_module, scope, source_uri):
        task_id = str(self._uuid.uuid4())
        self._tasks[task_id] = {
            "task_id": task_id,
            "connector_module": connector_module,
            "scope": scope,
            "source_uri": source_uri,
            "status": "queued",
            "verdict": None,
            "observed_source_revision": None,
            "source_hash_observed": None,
            "document_id": None,
            "resulting_row_id": None,
            "job_id": None,
            "error_message": None,
            "completed_at": None,
        }
        return task_id

    def mark_fetching(self, task_id):
        self._tasks[task_id]["status"] = "fetching"

    def set_verdict(self, task_id, *, verdict, observed_source_revision=None,
                    source_hash_observed=None, document_id=None, resulting_row_id=None,
                    job_id=None, error_message=None):
        t = self._tasks[task_id]
        t["verdict"] = verdict
        t["status"] = "failed" if verdict == "fetch_failed" else "fetched"
        t["completed_at"] = "2026-04-11T00:01:00+00:00"
        if observed_source_revision is not None:
            t["observed_source_revision"] = observed_source_revision
        if source_hash_observed is not None:
            t["source_hash_observed"] = source_hash_observed
        if document_id is not None:
            t["document_id"] = document_id
        if resulting_row_id is not None:
            t["resulting_row_id"] = resulting_row_id
        if job_id is not None:
            t["job_id"] = job_id
        if error_message is not None:
            t["error_message"] = error_message

    def get_task(self, task_id):
        return self._tasks.get(task_id)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _make_event(
    raw_bytes=b"# API Reference\n\nAuthentication section.\n",
    source_revision="rev1",
    scope="acme-kb/docs",
):
    from connectors.adapter import ConnectorEvent
    return ConnectorEvent(
        connector_module="openrag",
        scope=scope,
        source_uri="acme/api-reference.md",
        filename="api-reference.md",
        format="markdown",
        raw_bytes=raw_bytes,
        source_revision=source_revision,
        owner="test-operator",
    )


def _run_stubbed(event, reg, sync, *, orchestrator_raises=None, resolved_scope=None, pipeline_result=None):
    """Run ingest_from_connector with all DB stubs, orchestrator stubbed."""
    from connectors import adapter as _mod
    from orchestrator import PipelineResult

    if pipeline_result is None:
        pipeline_result = PipelineResult(status="active", chunks_created=5)
    pipeline_mock = MagicMock(return_value=pipeline_result)
    if orchestrator_raises is not None:
        pipeline_mock.side_effect = orchestrator_raises

    # resolve_scope returns None by default (event.scope is used as fallback)
    resolve_mock = MagicMock(return_value=resolved_scope)

    with (
        patch.object(_mod, "registry", reg),
        patch.object(_mod, "create_sync_task", sync.create_sync_task),
        patch.object(_mod, "mark_fetching", sync.mark_fetching),
        patch.object(_mod, "set_verdict", sync.set_verdict),
        patch.object(_mod, "get_task", sync.get_task),
        patch.object(_mod, "resolve_scope", resolve_mock),
        patch.object(_mod, "run_pipeline", pipeline_mock),
    ):
        result = _mod.ingest_from_connector(event)
    return result, resolve_mock


# ═══════════════════════════════════════════════════════════════════════════════
# TestConnectorAdapterStubbed: verdict, schema fields, scope resolution, dedup
# ═══════════════════════════════════════════════════════════════════════════════

class TestConnectorAdapterStubbed(unittest.TestCase):
    """Adapter logic tests with all dependencies stubbed."""

    def _reg_sync(self):
        return _InMemoryRegistry(), _InMemoryConnectorSync()

    # ── Verdict: fetched_new ──────────────────────────────────────────────────

    def test_first_sync_produces_fetched_new(self):
        reg, sync = self._reg_sync()
        result, _ = _run_stubbed(_make_event(), reg, sync)
        self.assertEqual(result.verdict, "fetched_new")
        self.assertIsNotNone(result.document_id)
        self.assertIsNotNone(result.job_id)
        self.assertEqual(result.version, 1)

    def test_first_sync_sets_schema_fields_on_task(self):
        """source_hash_observed and resulting_row_id must be set on fetched_new task."""
        reg, sync = self._reg_sync()
        raw = b"# Doc\n\nContent.\n"
        result, _ = _run_stubbed(_make_event(raw_bytes=raw), reg, sync)

        task = sync.get_task(result.task_id)
        self.assertEqual(task["verdict"], "fetched_new")
        self.assertEqual(task["source_hash_observed"], _sha256(raw),
                         "source_hash_observed must be SHA-256 of fetched bytes")
        self.assertIsNotNone(task["resulting_row_id"],
                             "resulting_row_id must be set for fetched_new")
        self.assertIsNotNone(task["document_id"])
        self.assertIsNotNone(task["job_id"])

    def test_first_sync_creates_document_row_with_connector_fields(self):
        reg, sync = self._reg_sync()
        result, _ = _run_stubbed(_make_event(source_revision="git-sha-123"), reg, sync)

        rows = [r for r in reg._rows if r["document_id"] == result.document_id]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["connector_module"], "openrag")
        self.assertEqual(row["source_uri"], "acme/api-reference.md")
        self.assertEqual(row["source_revision"], "git-sha-123",
                         "source_revision must be immutably set from event")

    # ── Verdict: skipped_unchanged ────────────────────────────────────────────

    def test_resync_same_content_skipped_unchanged(self):
        reg, sync = self._reg_sync()
        raw = b"# Stable content\n\nNo changes.\n"
        event = _make_event(raw_bytes=raw, source_revision="rev1")

        # First sync — creates document
        r1, _ = _run_stubbed(event, reg, sync)
        # Promote to active manually (orchestrator is stubbed)
        doc_id = r1.document_id
        active_row = next(r for r in reg._rows if r["document_id"] == doc_id)
        active_row["status"] = "active"

        # Re-sync same content
        event2 = _make_event(raw_bytes=raw, source_revision="rev2")
        r2, _ = _run_stubbed(event2, reg, sync)

        self.assertEqual(r2.verdict, "skipped_unchanged")
        self.assertIsNone(r2.job_id)
        self.assertIsNone(r2.version)

    def test_skipped_unchanged_sets_source_hash_observed(self):
        """skipped_unchanged task must record source_hash_observed (dedup evidence)."""
        reg, sync = self._reg_sync()
        raw = b"# Stable content\n"
        r1, _ = _run_stubbed(_make_event(raw_bytes=raw), reg, sync)
        doc_id = r1.document_id
        active_row = next(r for r in reg._rows if r["document_id"] == doc_id)
        active_row["status"] = "active"

        r2, _ = _run_stubbed(_make_event(raw_bytes=raw, source_revision="rev2"), reg, sync)
        task = sync.get_task(r2.task_id)
        self.assertEqual(task["source_hash_observed"], _sha256(raw))
        self.assertIsNone(task["resulting_row_id"],
                          "resulting_row_id must be NULL for skipped_unchanged")

    def test_skipped_unchanged_document_id_set_on_task(self):
        """skipped_unchanged task must have document_id set (logical doc exists)."""
        reg, sync = self._reg_sync()
        raw = b"# Stable content\n"
        r1, _ = _run_stubbed(_make_event(raw_bytes=raw), reg, sync)
        doc_id = r1.document_id
        next(r for r in reg._rows if r["document_id"] == doc_id)["status"] = "active"

        r2, _ = _run_stubbed(_make_event(raw_bytes=raw, source_revision="rev2"), reg, sync)
        task = sync.get_task(r2.task_id)
        self.assertIsNotNone(task["document_id"],
                             "document_id must be set even for skipped_unchanged")

    def test_skipped_unchanged_no_new_document_row(self):
        reg, sync = self._reg_sync()
        raw = b"# Stable\n"
        r1, _ = _run_stubbed(_make_event(raw_bytes=raw), reg, sync)
        next(r for r in reg._rows if r["document_id"] == r1.document_id)["status"] = "active"
        row_count_before = len(reg._rows)

        _run_stubbed(_make_event(raw_bytes=raw, source_revision="rev2"), reg, sync)
        self.assertEqual(len(reg._rows), row_count_before,
                         "skipped_unchanged must not create a new document row")

    # ── Verdict: fetched_updated ──────────────────────────────────────────────

    def test_resync_changed_content_fetched_updated(self):
        reg, sync = self._reg_sync()
        r1, _ = _run_stubbed(_make_event(raw_bytes=b"version 1"), reg, sync)
        doc_id = r1.document_id
        next(r for r in reg._rows if r["document_id"] == doc_id)["status"] = "active"

        r2, _ = _run_stubbed(_make_event(raw_bytes=b"version 2 changed"), reg, sync)
        self.assertEqual(r2.verdict, "fetched_updated")
        self.assertEqual(r2.document_id, doc_id)
        self.assertEqual(r2.version, 2)
        task = sync.get_task(r2.task_id)
        self.assertIsNotNone(task["resulting_row_id"])

    # ── Verdict: fetch_failed ─────────────────────────────────────────────────

    def test_sha_in_source_uri_produces_fetch_failed(self):
        from connectors.adapter import ConnectorEvent
        sha = "a3f9c2d1" + "b" * 32  # 40-char hex
        event = ConnectorEvent(
            connector_module="openrag",
            scope="scope/test",
            source_uri=f"https://github.com/org/repo/blob/{sha}/docs/api.md",
            filename="api.md",
            format="markdown",
            raw_bytes=b"content",
        )
        reg, sync = self._reg_sync()
        result, _ = _run_stubbed(event, reg, sync)
        self.assertEqual(result.verdict, "fetch_failed")
        task = sync.get_task(result.task_id)
        self.assertEqual(task["verdict"], "fetch_failed")
        self.assertEqual(task["status"], "failed")

    # ── W-C1.9: Scope resolution wired into adapter ───────────────────────────

    def test_resolve_scope_is_called_with_connector_module_and_source_uri(self):
        """resolve_scope() must be called before the sync task is created."""
        reg, sync = self._reg_sync()
        result, resolve_mock = _run_stubbed(_make_event(), reg, sync)

        resolve_mock.assert_called_once_with("openrag", "acme/api-reference.md")

    def test_resolve_scope_result_overrides_event_scope(self):
        """When resolve_scope returns a scope, it must be used instead of event.scope."""
        reg, sync = self._reg_sync()
        event = _make_event(scope="original-scope/docs")
        result, _ = _run_stubbed(event, reg, sync, resolved_scope="mapped-scope/api")

        # The task must have been created with the resolved scope
        task = sync.get_task(result.task_id)
        self.assertEqual(task["scope"], "mapped-scope/api",
                         "Resolved scope must override event.scope")

    def test_event_scope_used_when_no_mapping_matches(self):
        """When resolve_scope returns None, event.scope is the fallback."""
        reg, sync = self._reg_sync()
        event = _make_event(scope="fallback-scope/docs")
        result, _ = _run_stubbed(event, reg, sync, resolved_scope=None)

        task = sync.get_task(result.task_id)
        self.assertEqual(task["scope"], "fallback-scope/docs",
                         "event.scope must be used when no mapping rule matches")

    def test_no_scope_produces_fetch_failed(self):
        """If resolve_scope returns None and event.scope is empty, verdict=fetch_failed."""
        from connectors.adapter import ConnectorEvent
        event = ConnectorEvent(
            connector_module="openrag",
            scope="",          # empty — no fallback
            source_uri="some/doc.md",
            filename="doc.md",
            format="markdown",
            raw_bytes=b"content",
        )
        reg, sync = self._reg_sync()
        result, _ = _run_stubbed(event, reg, sync, resolved_scope=None)
        self.assertEqual(result.verdict, "fetch_failed")
        self.assertIsNotNone(result.error_message)

    # ── W-C1.8: Orchestrator failure does NOT change sync verdict ─────────────

    def test_orchestrator_failure_does_not_change_sync_verdict(self):
        reg, sync = self._reg_sync()
        result, _ = _run_stubbed(
            _make_event(), reg, sync,
            orchestrator_raises=RuntimeError("embedding service unavailable"),
        )
        task = sync.get_task(result.task_id)
        self.assertIn(task["verdict"], ("fetched_new", "fetched_updated"))
        self.assertEqual(task["status"], "fetched",
                         "Sync task must remain fetched even if orchestrator raises")

    # ── W-C1.9: Unregistered scope must fail cleanly ──────────────────────────

    def test_unregistered_scope_produces_fetch_failed(self):
        """If the resolved scope is not in the scope registry, verdict must be fetch_failed.

        This is the acceptance criterion: 'unknown scope not in scope registry fails cleanly'.
        """
        # Registry with strict scope validation — only "registered-scope/docs" is known
        reg = _InMemoryRegistry(known_scopes={"registered-scope/docs"})
        sync = _InMemoryConnectorSync()

        event = _make_event(scope="unregistered-scope/docs")
        result, _ = _run_stubbed(event, reg, sync, resolved_scope=None)

        self.assertEqual(result.verdict, "fetch_failed",
                         "Unregistered scope must yield fetch_failed")
        self.assertIsNotNone(result.error_message)
        self.assertIn("not registered", result.error_message.lower(),
                      "Error message must explain the scope is not registered")
        task = sync.get_task(result.task_id)
        self.assertEqual(task["verdict"], "fetch_failed")
        self.assertEqual(task["status"], "failed")

    def test_registered_scope_proceeds_normally(self):
        """When scope is registered, the pipeline proceeds to fetched_new."""
        reg = _InMemoryRegistry(known_scopes={"acme-kb/docs"})
        sync = _InMemoryConnectorSync()
        event = _make_event(scope="acme-kb/docs")
        result, _ = _run_stubbed(event, reg, sync, resolved_scope=None)
        self.assertEqual(result.verdict, "fetched_new")

    def test_resolved_scope_is_also_validated_against_registry(self):
        """A mapping-resolved scope that is not registered also yields fetch_failed."""
        reg = _InMemoryRegistry(known_scopes={"registered/scope"})
        sync = _InMemoryConnectorSync()
        event = _make_event(scope="original/scope")
        # resolve_scope returns "phantom/scope" which is not in the registry
        result, _ = _run_stubbed(event, reg, sync, resolved_scope="phantom/scope")
        self.assertEqual(result.verdict, "fetch_failed")
        self.assertIn("not registered", result.error_message.lower())

    # ── W-C1.3: completed_at set on terminal verdicts ─────────────────────────

    def test_completed_at_is_set_on_fetched_new_task(self):
        """connector_sync_tasks.completed_at must be set when verdict is finalized."""
        reg, sync = self._reg_sync()
        result, _ = _run_stubbed(_make_event(), reg, sync)
        task = sync.get_task(result.task_id)
        self.assertIsNotNone(task["completed_at"],
                             "completed_at must be set when task reaches a terminal state")

    def test_completed_at_is_set_on_fetch_failed_task(self):
        """completed_at must be set for fetch_failed too (terminal state)."""
        reg, sync = self._reg_sync()
        reg = _InMemoryRegistry(known_scopes={"registered/scope"})
        event = _make_event(scope="unregistered/scope")
        result, _ = _run_stubbed(event, reg, sync, resolved_scope=None)
        self.assertEqual(result.verdict, "fetch_failed")
        task = sync.get_task(result.task_id)
        self.assertIsNotNone(task["completed_at"])

    # ── Pipeline result propagation ──────────────────────────────────────────

    def test_pipeline_result_propagated_to_sync_result(self):
        """SyncResult must carry document_status and chunks from pipeline."""
        from orchestrator import PipelineResult
        reg, sync = self._reg_sync()
        pr = PipelineResult(status="active", chunks_created=12, chunks_skipped=0)
        result, _ = _run_stubbed(_make_event(), reg, sync, pipeline_result=pr)
        self.assertEqual(result.verdict, "fetched_new")
        self.assertEqual(result.document_status, "active")
        self.assertEqual(result.chunks_created, 12)
        self.assertEqual(result.chunks_skipped, 0)
        self.assertIsNone(result.pipeline_error)
        self.assertIsNone(result.stage_failed)

    def test_pipeline_failure_propagated_to_sync_result(self):
        """Pipeline failure must surface document_status=failed and error details."""
        from orchestrator import PipelineResult
        reg, sync = self._reg_sync()
        pr = PipelineResult(status="failed", error_message="parse error", stage_failed="normalization")
        result, _ = _run_stubbed(_make_event(), reg, sync, pipeline_result=pr)
        self.assertEqual(result.verdict, "fetched_new")  # sync verdict unchanged
        self.assertEqual(result.document_status, "failed")
        self.assertEqual(result.pipeline_error, "parse error")
        self.assertEqual(result.stage_failed, "normalization")

    def test_orchestrator_exception_sets_failed_pipeline_result(self):
        """Unhandled orchestrator exception must produce document_status=failed."""
        reg, sync = self._reg_sync()
        result, _ = _run_stubbed(_make_event(), reg, sync,
                                 orchestrator_raises=RuntimeError("boom"))
        self.assertEqual(result.verdict, "fetched_new")
        self.assertEqual(result.document_status, "failed")
        self.assertIn("boom", result.pipeline_error or "")

    def test_fetch_failed_has_no_pipeline_fields(self):
        """fetch_failed verdict should have no pipeline outcome fields."""
        _, sync = self._reg_sync()
        reg = _InMemoryRegistry(known_scopes={"registered/scope"})
        result, _ = _run_stubbed(_make_event(scope="bad/scope"), reg, sync)
        self.assertEqual(result.verdict, "fetch_failed")
        self.assertIsNone(result.document_status)
        self.assertIsNone(result.chunks_created)

    def test_skipped_unchanged_has_no_pipeline_fields(self):
        """skipped_unchanged should have no pipeline outcome fields."""
        reg, sync = self._reg_sync()
        raw = b"# Stable\n"
        r1, _ = _run_stubbed(_make_event(raw_bytes=raw), reg, sync)
        next(r for r in reg._rows if r["document_id"] == r1.document_id)["status"] = "active"
        r2, _ = _run_stubbed(_make_event(raw_bytes=raw), reg, sync)
        self.assertEqual(r2.verdict, "skipped_unchanged")
        self.assertIsNone(r2.document_status)
        self.assertIsNone(r2.chunks_created)


# ═══════════════════════════════════════════════════════════════════════════════
# TestConnectorAdapterRealPipeline: real normalizer + extractor + orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

class TestConnectorAdapterRealPipeline(unittest.TestCase):
    """W-C1.12: Connector adapter with real normalizer/extractor/orchestrator.

    Only httpx.post (Super RAG network call) is mocked.
    Registry and connector_sync use in-memory stubs (no PostgreSQL).
    Storage uses local filesystem at _TMP_STORE.

    This verifies that ConnectorEvent bytes flow through the real pipeline and
    produce a document in 'active' state — the same guarantee test_ingest_e2e.py
    provides for the manual upload path.
    """

    def setUp(self):
        self.reg = _InMemoryRegistry()
        self.sync = _InMemoryConnectorSync()
        # Ensure SUPER_RAG_INGEST_MAX_RETRIES is 1 for fast failure in tests
        os.environ["SUPER_RAG_INGEST_MAX_RETRIES"] = "1"

    def _run_real(self, event, *, super_rag_status=201, resolved_scope=None):
        """Run ingest_from_connector with real pipeline, stubbing only:
        - httpx.post → simulated Super RAG response
        - db.registry in orchestrator → in-memory registry
        - db.registry in adapter → in-memory registry
        - connector_sync operations → in-memory sync stub
        - resolve_scope → return resolved_scope (None = fallback to event.scope)
        """
        from connectors import adapter as _mod
        import orchestrator as _orch

        resolve_mock = MagicMock(return_value=resolved_scope)

        # Build a fake httpx response
        mock_resp = MagicMock()
        mock_resp.status_code = super_rag_status
        mock_resp.raise_for_status = MagicMock(
            side_effect=None if super_rag_status < 400
            else Exception(f"HTTP {super_rag_status}")
        )

        with (
            patch.object(_mod, "registry", self.reg),
            patch.object(_mod, "create_sync_task", self.sync.create_sync_task),
            patch.object(_mod, "mark_fetching", self.sync.mark_fetching),
            patch.object(_mod, "set_verdict", self.sync.set_verdict),
            patch.object(_mod, "get_task", self.sync.get_task),
            patch.object(_mod, "resolve_scope", resolve_mock),
            # Patch registry inside orchestrator too
            patch.object(_orch, "registry", self.reg),
            # Mock only the Super RAG network call
            patch("httpx.post", return_value=mock_resp),
        ):
            result = _mod.ingest_from_connector(event)
        return result

    def test_markdown_connector_event_reaches_active_state(self):
        """Full pipeline: markdown ConnectorEvent → normalizer → extractor → Super RAG → active."""
        event = _make_event(
            raw_bytes=b"# API Reference\n\n## Authentication\n\nUse bearer tokens.\n",
            source_revision="v1-rev",
        )
        result = self._run_real(event)

        self.assertEqual(result.verdict, "fetched_new")
        self.assertIsNotNone(result.document_id)
        self.assertEqual(result.version, 1)
        self.assertIsNotNone(result.job_id)

        # Document must be in active state (orchestrator ran promote_to_active)
        active = self.reg.active_row(result.document_id)
        self.assertIsNotNone(active, "Document must be in active state after successful pipeline")
        self.assertEqual(active["status"], "active")

    def test_real_pipeline_stores_connector_provenance(self):
        """connector_module, source_uri, source_revision must be on the active document row."""
        event = _make_event(
            raw_bytes=b"# Guide\n\nContent here.\n",
            source_revision="git-abc123",
        )
        result = self._run_real(event)

        active = self.reg.active_row(result.document_id)
        self.assertEqual(active["connector_module"], "openrag")
        self.assertEqual(active["source_uri"], "acme/api-reference.md")
        self.assertEqual(active["source_revision"], "git-abc123",
                         "source_revision must be immutable provenance from event")

    def test_real_pipeline_sync_task_has_correct_schema_fields(self):
        """source_hash_observed and resulting_row_id must be set on sync task."""
        raw = b"# API Reference\n\n## Auth\n\nBearers.\n"
        event = _make_event(raw_bytes=raw)
        result = self._run_real(event)

        task = self.sync.get_task(result.task_id)
        self.assertEqual(task["verdict"], "fetched_new")
        self.assertEqual(task["source_hash_observed"], _sha256(raw))
        self.assertIsNotNone(task["resulting_row_id"])

    def test_real_pipeline_super_rag_failure_leaves_document_failed(self):
        """Super RAG HTTP 500 → document status=failed; sync verdict=fetched_new (W-C1.8)."""
        event = _make_event()
        result = self._run_real(event, super_rag_status=500)

        # Sync verdict must still be fetched_new — failure happened in orchestrator
        task = self.sync.get_task(result.task_id)
        self.assertIn(task["verdict"], ("fetched_new", "fetched_updated"),
                      "Sync verdict must not change to fetch_failed on orchestrator failure")

        # Document row must be in failed state
        rows = [r for r in self.reg._rows if r["document_id"] == result.document_id]
        self.assertTrue(len(rows) > 0, "Document row must exist")
        # At least one row should be in failed state
        statuses = {r["status"] for r in rows}
        self.assertTrue(
            "failed" in statuses or "active" not in statuses,
            f"Document should be failed after Super RAG 500, got statuses: {statuses}",
        )

    def test_real_pipeline_scope_mapping_overrides_event_scope(self):
        """When resolve_scope returns a scope, the real pipeline uses it."""
        event = _make_event(scope="original/scope")
        result = self._run_real(event, resolved_scope="mapped/scope")

        # The sync task must be under the resolved scope
        task = self.sync.get_task(result.task_id)
        self.assertEqual(task["scope"], "mapped/scope")

        # The document row must have the resolved scope
        rows = [r for r in self.reg._rows if r["document_id"] == result.document_id]
        self.assertTrue(len(rows) > 0)
        self.assertEqual(rows[0]["scope"], "mapped/scope")

    def test_real_pipeline_skipped_unchanged_does_not_run_orchestrator(self):
        """skipped_unchanged: real normalizer/orchestrator must NOT be called."""
        raw = b"# API Reference\n\nAuthentication section.\n"
        event1 = _make_event(raw_bytes=raw, source_revision="rev1")
        result1 = self._run_real(event1)

        # Promote first document to active
        doc_id = result1.document_id
        active_row = next(r for r in self.reg._rows if r["document_id"] == doc_id)
        active_row["status"] = "active"
        row_count_after_first = len(self.reg._rows)

        # Second sync with same content
        event2 = _make_event(raw_bytes=raw, source_revision="rev2")
        with patch("httpx.post") as mock_post:
            from connectors import adapter as _mod
            import orchestrator as _orch
            with (
                patch.object(_mod, "registry", self.reg),
                patch.object(_mod, "create_sync_task", self.sync.create_sync_task),
                patch.object(_mod, "mark_fetching", self.sync.mark_fetching),
                patch.object(_mod, "set_verdict", self.sync.set_verdict),
                patch.object(_mod, "get_task", self.sync.get_task),
                patch.object(_mod, "resolve_scope", MagicMock(return_value=None)),
                patch.object(_orch, "registry", self.reg),
            ):
                result2 = _mod.ingest_from_connector(event2)

        self.assertEqual(result2.verdict, "skipped_unchanged")
        mock_post.assert_not_called()
        self.assertEqual(len(self.reg._rows), row_count_after_first,
                         "skipped_unchanged must not create a new document row")


if __name__ == "__main__":
    unittest.main()
