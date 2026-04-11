# ADR-003: OpenRAG Integration Ownership and Connector Architecture

**Status:** Accepted
**Date:** 2026-04-11
**Issues:** W-C1 (#20–#31)

---

## Context

Agentopia needs to ingest documents from external sources — the first target being OpenRAG (a document management and annotation tool). Two questions required a firm decision before connector code was written:

1. **Who owns document registry and versioning?** OpenRAG has its own document management features. Should we reuse them, or does knowledge-ingest own the registry?
2. **How do we represent source identity vs. source revision?** Connector documents have both a stable locator (where to find the document) and a mutable revision stamp (which exact version was fetched).

---

## Decision

### 1. knowledge-ingest is the sole owner of document registry, versioning, rollback, and provenance

OpenRAG is a **connector** — a source of raw bytes. It does not own:
- `documents` table rows
- version numbers
- `active` / `superseded` / `rollback` lifecycle
- Qdrant chunk payloads
- audit log entries

knowledge-ingest accepts a `ConnectorEvent` from the OpenRAG connector module, runs the same normalizer → extractor → Super RAG pipeline, and manages all lifecycle state. OpenRAG's internal versioning is irrelevant to the knowledge-ingest document model.

### 2. Two identity spaces: manual documents and connector documents

| Upload type | document_id key space | Function |
|---|---|---|
| Manual upload (operator UI / API) | `manual://{scope}:{filename}` | `stable_document_id_manual(scope, filename)` |
| Connector-originated | `connector://{scope}:{connector_module}:{source_uri}` | `stable_document_id_connector(scope, connector_module, source_uri)` |

The two spaces use different key prefixes so they **never collide** even when a connector document has the same filename as a manually-uploaded document.

### 3. source_uri is the stable logical locator — no volatile revision selectors

`source_uri` identifies **where** a document lives in the external source. It must be stable across fetches. It must NOT contain commit SHAs, blob SHAs, or any volatile revision selector because:

- If `source_uri` contained a SHA, two fetches of the same document at different commits would produce **different document_ids** — creating duplicate documents instead of a new version.
- The correct behaviour is: same logical path → same `document_id` → incremented `version`.

Examples:
- ✅ `joblogic/api-reference.pdf` (path only)
- ✅ `https://wiki.example.com/pages/12345` (stable page ID)
- ✅ `https://github.com/org/repo/blob/main/docs/api.md` (branch ref OK)
- ❌ `https://github.com/org/repo/blob/a3f9c2d.../docs/api.md` (commit SHA — rejected)

`stable_document_id_connector` validates `source_uri` and raises `ValueError` if a 40-character hex SHA is detected.

### 4. source_revision is immutable provenance per version row

`documents.source_revision` carries the exact external revision stamp at the moment of ingest (e.g. git commit SHA, Confluence version ID, S3 ETag). It is:

- Set **once** on INSERT by the connector adapter.
- **Never updated** after INSERT — no UPDATE statement in registry.py touches `source_revision`.
- The `source_revision` of a superseded version remains accessible in version history for provenance inspection.

`connector_sync_tasks.observed_source_revision` is the only field that holds the latest observed revision from a re-sync. It is written on every sync task (including `skipped_unchanged`) but is never propagated back to `documents.source_revision`.

### 5. connector_sync_tasks is a separate state machine from ingest_jobs

The sync state machine tracks connector fetch outcomes independently from the ingest pipeline:

```
connector_sync_tasks:    queued → fetching → fetched (verdict)
                                           → failed

ingest_jobs:             submitted → normalizing → normalized → extracting →
                         extracted → indexing → active
                      OR any stage → failed
```

Handoff: only `fetched_new` and `fetched_updated` verdicts create an `ingest_job`. `skipped_unchanged` and `fetch_failed` do not create document rows or ingest jobs.

The sync verdict is finalized **before** the orchestrator runs. If the orchestrator fails, the sync task remains at `verdict=fetched_new/fetched_updated` — the failure is visible on the document row and ingest_job only. A new sync event (re-fetch) creates a new sync task.

### 6. Rename/move semantics (W-C1.4)

A document rename or move in the external source changes `source_uri`. Because `document_id` is derived from `source_uri`, a rename produces a **new document_id**. This is intentional:

- The old document_id continues to exist (as `superseded` or `active` depending on whether a manual delete was issued).
- The new document_id starts at version 1.
- Reconciliation (detecting renames and linking old→new document_ids) is **deferred** to W-C3 or later.

This matches the manual upload behaviour: renaming a file and re-uploading creates a new document, not a new version of the old one.

---

## Consequences

### Good

- **Clear ownership**: knowledge-ingest operators have a single interface for document lifecycle regardless of how content arrived.
- **No identity collision**: manual and connector documents with identical filenames never produce the same `document_id`.
- **Provenance correctness**: every active document version carries an immutable `source_revision` stamp. Operators can trace which external revision was ingested.
- **Sync visibility**: `connector_sync_tasks` records all sync outcomes including `skipped_unchanged`, giving operators full audit history.
- **Resilient pipeline**: orchestrator failures don't corrupt the sync state. Re-fetch creates a clean new task.

### Trade-offs

- **Rename detection is not automatic**: a renamed source document creates a new knowledge-ingest document. This is acceptable for W-C1 and can be addressed in a future reconciliation pass.
- **source_revision immutability requires discipline**: callers must never issue UPDATE on `documents.source_revision`. Enforced by static audit test (`tests/test_provenance_immutability.py`).

---

## Alternatives Considered

**Reuse OpenRAG versioning**: Rejected. OpenRAG versioning is scoped to the annotation/editing workflow, not the knowledge retrieval lifecycle. Coupling knowledge-ingest lifecycle to OpenRAG's internal state would make rollback and provenance dependent on OpenRAG availability.

**Store source_revision in source_uri**: Rejected. This collapses identity and provenance into one field, breaking the stable-document_id invariant on the first re-fetch.

**Single identity space (no prefix)**: Rejected. Without key-space separation, a manual upload and a connector document with the same scope + filename would silently collide on `document_id`.
