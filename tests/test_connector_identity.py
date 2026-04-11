"""W-C1.11: Connector identity coexistence tests.

6 test cases validating:
1. Manual document_id is stable across independent calls.
2. Connector document_id is stable across independent calls.
3. Manual and connector document_ids DIFFER for same scope + same logical filename.
4. Different source_uris produce different connector document_ids.
5. Same source_uri under different connector_modules produces different IDs.
6. source_uri containing a 40-char commit SHA is rejected with a clear error.

These tests run against the pure registry functions — no database required.
They document and enforce the identity coexistence contract described in
docs/adr/003-openrag-integration-ownership.md.
"""

import os
import sys
import unittest

# Ensure src is on the path
import pathlib
_SRC = pathlib.Path(__file__).parent.parent / "src"
sys.path.insert(0, str(_SRC))

from db.registry import (
    stable_document_id_manual,
    stable_document_id_connector,
    stable_document_id,          # backward-compat alias
)


class TestIdentityCoexistence(unittest.TestCase):
    """Identity coexistence: manual and connector document IDs never collide."""

    # ── Test 1: Manual ID stability ──────────────────────────────────────────

    def test_manual_id_is_stable(self):
        """Same (scope, filename) always produces the same manual document_id."""
        id1 = stable_document_id_manual("joblogic-kb/docs", "api-reference.pdf")
        id2 = stable_document_id_manual("joblogic-kb/docs", "api-reference.pdf")
        self.assertEqual(id1, id2)

    # ── Test 2: Connector ID stability ───────────────────────────────────────

    def test_connector_id_is_stable(self):
        """Same (scope, connector_module, source_uri) always produces the same connector document_id."""
        id1 = stable_document_id_connector(
            "joblogic-kb/docs", "openrag", "joblogic/api-reference.pdf"
        )
        id2 = stable_document_id_connector(
            "joblogic-kb/docs", "openrag", "joblogic/api-reference.pdf"
        )
        self.assertEqual(id1, id2)

    # ── Test 3: Manual vs connector never collide ────────────────────────────

    def test_manual_and_connector_ids_differ_for_same_filename(self):
        """Manual upload and connector document with the same scope+filename get different IDs.

        This is the critical coexistence property: the two identity spaces
        are separated by key prefix so collision is impossible even when
        an operator manually uploads the same file that a connector also syncs.
        """
        scope = "joblogic-kb/docs"
        filename = "api-reference.pdf"

        manual_id = stable_document_id_manual(scope, filename)
        connector_id = stable_document_id_connector(scope, "openrag", filename)

        self.assertNotEqual(
            manual_id, connector_id,
            "Manual and connector document_ids must differ even with identical scope+filename",
        )

    # ── Test 4: Different source_uris → different connector IDs ─────────────

    def test_different_source_uris_produce_different_connector_ids(self):
        """Two distinct source_uris under the same connector produce distinct document_ids."""
        id_a = stable_document_id_connector(
            "joblogic-kb/docs", "openrag", "joblogic/api-reference.pdf"
        )
        id_b = stable_document_id_connector(
            "joblogic-kb/docs", "openrag", "joblogic/quickstart-guide.pdf"
        )
        self.assertNotEqual(
            id_a, id_b,
            "Different source_uris must produce different connector document_ids",
        )

    # ── Test 5: Same source_uri, different connector_module → different IDs ──

    def test_same_source_uri_different_connector_module_produces_different_ids(self):
        """The same source_uri synced by two different connectors gets different document_ids.

        This matters if the same URL is accessible by both an OpenRAG connector and
        a direct Confluence connector — they should not silently overwrite each other.
        """
        source_uri = "joblogic/api-reference.pdf"
        id_openrag = stable_document_id_connector("joblogic-kb/docs", "openrag", source_uri)
        id_confluence = stable_document_id_connector("joblogic-kb/docs", "confluence", source_uri)
        self.assertNotEqual(
            id_openrag, id_confluence,
            "Same source_uri under different connector_modules must produce different document_ids",
        )

    # ── Test 6: SHA in source_uri is rejected ────────────────────────────────

    def test_source_uri_with_commit_sha_is_rejected(self):
        """stable_document_id_connector must raise ValueError when source_uri contains a commit SHA.

        A 40-character hex string (git commit SHA, blob SHA, etc.) in source_uri
        would make the URI unstable — two fetches at different commits would produce
        different document_ids instead of a new version of the same document.
        """
        commit_sha = "a3f9c2d1" + "b" * 32  # 40-char hex
        volatile_uri = f"https://github.com/org/repo/blob/{commit_sha}/docs/api.md"

        with self.assertRaises(ValueError) as ctx:
            stable_document_id_connector("joblogic-kb/docs", "openrag", volatile_uri)

        error_msg = str(ctx.exception)
        # Error must mention source_revision so the developer knows the fix
        self.assertIn(
            "source_revision", error_msg,
            "ValueError must mention source_revision so the developer understands the fix",
        )

    # ── Additional: backward-compat alias ────────────────────────────────────

    def test_stable_document_id_alias_matches_manual(self):
        """stable_document_id() (legacy alias) produces the same ID as stable_document_id_manual()."""
        scope = "joblogic-kb/docs"
        filename = "api-reference.pdf"
        self.assertEqual(
            stable_document_id(scope, filename),
            stable_document_id_manual(scope, filename),
        )

    def test_manual_ids_differ_across_scopes(self):
        """Same filename in different scopes produces different document_ids."""
        id_scope_a = stable_document_id_manual("scope-a/docs", "guide.pdf")
        id_scope_b = stable_document_id_manual("scope-b/docs", "guide.pdf")
        self.assertNotEqual(id_scope_a, id_scope_b)

    def test_connector_ids_differ_across_scopes(self):
        """Same source_uri in different scopes produces different connector document_ids."""
        uri = "shared/common-guide.pdf"
        id_scope_a = stable_document_id_connector("scope-a/docs", "openrag", uri)
        id_scope_b = stable_document_id_connector("scope-b/docs", "openrag", uri)
        self.assertNotEqual(id_scope_a, id_scope_b)

    def test_ids_are_valid_uuid_format(self):
        """Both identity functions return valid UUID strings."""
        import uuid
        manual_id = stable_document_id_manual("scope/test", "file.pdf")
        connector_id = stable_document_id_connector("scope/test", "openrag", "path/file.pdf")
        # Should not raise
        uuid.UUID(manual_id)
        uuid.UUID(connector_id)

    def test_source_uri_with_branch_ref_is_accepted(self):
        """Branch name in source_uri (main, develop, feature/x) is valid — not a SHA."""
        # Should not raise
        id_ = stable_document_id_connector(
            "scope/test", "openrag",
            "https://github.com/org/repo/blob/main/docs/api.md"
        )
        self.assertIsNotNone(id_)

    def test_short_hex_string_in_source_uri_is_accepted(self):
        """A hex string shorter than 40 chars is not treated as a commit SHA."""
        short_hex = "a3f9c2d"  # 7-char abbreviated SHA — common in display but not 40-char
        uri = f"https://github.com/org/repo/blob/{short_hex}/docs/api.md"
        # Short SHAs should be accepted (they're ambiguous, but not definitively volatile)
        id_ = stable_document_id_connector("scope/test", "openrag", uri)
        self.assertIsNotNone(id_)


if __name__ == "__main__":
    unittest.main()
