"""W-C1.6: Static audit — provenance immutability enforcement.

Contract
--------
documents.source_revision is IMMUTABLE per row. It is set exactly once on INSERT
by the connector adapter (connectors/adapter.py → db/registry.py create_document).
No UPDATE statement anywhere in the codebase may reference source_revision.

This test scans the registry module's source text for any UPDATE that touches
source_revision. It acts as a regression guard: if a developer accidentally adds
an UPDATE that mutates source_revision (e.g. "updating observed revision" or
"refreshing provenance"), this test will catch it before merge.

The canonical location for re-sync revision data is:
  connector_sync_tasks.observed_source_revision  (set on every sync task)
NOT:
  documents.source_revision  (immutable per row)

See docs/adr/003-openrag-integration-ownership.md §4 for the design rationale.
"""

import ast
import os
import pathlib
import re
import sys
import unittest

# Ensure src is importable (tests run from repo root or tests/)
_SRC = pathlib.Path(__file__).parent.parent / "src"
sys.path.insert(0, str(_SRC))


class TestSourceRevisionImmutability(unittest.TestCase):
    """Static audit: no UPDATE touching source_revision in the document registry."""

    def _registry_source(self) -> str:
        registry_path = _SRC / "db" / "registry.py"
        self.assertTrue(
            registry_path.exists(),
            f"registry.py not found at {registry_path}",
        )
        return registry_path.read_text()

    def _connector_sync_source(self) -> str:
        sync_path = _SRC / "db" / "connector_sync.py"
        self.assertTrue(
            sync_path.exists(),
            f"connector_sync.py not found at {sync_path}",
        )
        return sync_path.read_text()

    def test_registry_has_no_update_on_source_revision(self):
        """registry.py must not contain any UPDATE statement that sets source_revision."""
        source = self._registry_source()

        # Find all UPDATE ... SET ... blocks and check for source_revision
        # Normalise whitespace for pattern matching
        normalised = " ".join(source.split())

        # Pattern: UPDATE ... source_revision = (with various whitespace)
        pattern = re.compile(r'UPDATE\b[^;]*\bsource_revision\s*=', re.IGNORECASE)
        match = pattern.search(normalised)

        self.assertIsNone(
            match,
            "registry.py contains an UPDATE that sets source_revision. "
            "source_revision is immutable per row — it must only be set on INSERT. "
            "Use connector_sync_tasks.observed_source_revision for re-sync data. "
            f"Matched context: {normalised[max(0, (match.start() if match else 0) - 50):(match.end() if match else 0) + 100]!r}"
        )

    def test_registry_insert_sets_source_revision(self):
        """registry.py INSERT for connector documents must include source_revision."""
        source = self._registry_source()

        # The create_document function must have source_revision in its INSERT
        self.assertIn(
            "source_revision",
            source,
            "registry.py does not reference source_revision at all — "
            "connector INSERT must set this field",
        )

    def test_connector_sync_does_not_write_to_documents_source_revision(self):
        """connector_sync.py must not write source_revision to the documents table."""
        source = self._connector_sync_source()

        # Any UPDATE on documents table touching source_revision is forbidden
        normalised = " ".join(source.split())
        pattern = re.compile(
            r'UPDATE\s+documents\b[^;]*\bsource_revision\s*=',
            re.IGNORECASE,
        )
        match = pattern.search(normalised)

        self.assertIsNone(
            match,
            "connector_sync.py contains an UPDATE on the documents table that sets "
            "source_revision. Only connector_sync_tasks.observed_source_revision "
            "should hold re-sync revision data.",
        )

    def test_connector_sync_writes_observed_source_revision(self):
        """connector_sync.py must write observed_source_revision (not source_revision) to sync tasks."""
        source = self._connector_sync_source()

        self.assertIn(
            "observed_source_revision",
            source,
            "connector_sync.py does not reference observed_source_revision — "
            "sync tasks must record the observed revision here",
        )

    def test_adapter_sets_source_revision_on_create_document_only(self):
        """adapter.py must pass source_revision to create_document, not to any UPDATE."""
        adapter_path = _SRC / "connectors" / "adapter.py"
        self.assertTrue(adapter_path.exists(), f"adapter.py not found at {adapter_path}")
        source = adapter_path.read_text()

        # Must pass source_revision to create_document (provenance set on INSERT)
        self.assertIn(
            "source_revision",
            source,
            "adapter.py does not pass source_revision to create_document",
        )

        # Must not issue any UPDATE on source_revision via registry
        normalised = " ".join(source.split())
        pattern = re.compile(r'\bupdate_document_status\b[^)]*source_revision', re.IGNORECASE)
        match = pattern.search(normalised)
        self.assertIsNone(
            match,
            "adapter.py calls update_document_status with source_revision — "
            "provenance must only be set on INSERT via create_document",
        )

    def test_sql_migration_source_revision_has_no_update_trigger(self):
        """db/002_connectors.sql comment must state source_revision is immutable."""
        sql_path = pathlib.Path(__file__).parent.parent / "db" / "002_connectors.sql"
        self.assertTrue(sql_path.exists(), f"002_connectors.sql not found")
        sql = sql_path.read_text()

        # The migration comment must document immutability
        self.assertIn(
            "IMMUTABLE",
            sql.upper(),
            "002_connectors.sql does not document source_revision immutability — "
            "add a comment so future developers understand the constraint",
        )


class TestRegistryIdentityFunctions(unittest.TestCase):
    """Unit tests for stable_document_id_manual and stable_document_id_connector."""

    def _registry(self):
        from db import registry
        return registry

    def test_stable_document_id_manual_exists(self):
        r = self._registry()
        self.assertTrue(callable(r.stable_document_id_manual))

    def test_stable_document_id_connector_exists(self):
        r = self._registry()
        self.assertTrue(callable(r.stable_document_id_connector))

    def test_backward_compat_alias(self):
        """stable_document_id is a backward-compat alias for stable_document_id_manual."""
        r = self._registry()
        self.assertIs(r.stable_document_id, r.stable_document_id_manual)

    def test_connector_rejects_40char_hex_sha_in_source_uri(self):
        """stable_document_id_connector must reject a source_uri containing a 40-char hex SHA."""
        r = self._registry()
        sha = "a3f9c2d" + "0" * 33  # 40-char hex
        with self.assertRaises(ValueError) as ctx:
            r.stable_document_id_connector("scope/test", "openrag", f"path/{sha}/doc.pdf")
        self.assertIn("source_revision", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
