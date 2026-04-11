"""Connector integration layer for agentopia-knowledge-ingest.

W-C1: Internal foundations for syncing external sources into the knowledge base.

Modules:
  adapter.py       — ingest_from_connector() entry point (in-process, W-C1.7)
  scope_mapping.py — source_pattern → scope resolution config (W-C1.9)

The connector layer sits between external source modules (OpenRAG, Confluence, etc.)
and the existing manual-upload pipeline. It owns the connector document identity
model (stable_document_id_connector), dedup logic, and sync/ingest handoff.

Architecture invariants (enforced here):
  - source_uri is the stable logical locator (no volatile revision selectors).
  - source_revision is immutable provenance, written once on INSERT.
  - connector_sync_tasks records ALL sync outcomes including skipped_unchanged.
  - sync verdict is finalized independent of downstream orchestrator outcome.
"""
