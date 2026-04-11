-- W-C1: Connector integration foundations
-- Idempotent: safe to run multiple times.
--
-- Adds connector identity fields to the documents table and creates the
-- connector_sync_tasks table for the independent sync state machine.
--
-- Design invariant (MUST NOT BREAK):
--   documents.source_revision is IMMUTABLE per row. It is set exactly once
--   on INSERT by the connector adapter. No UPDATE may touch this column.
--   observed_source_revision in connector_sync_tasks is the only column
--   that tracks the latest revision stamp from subsequent re-syncs.

-- ── Connector identity columns (NULL for manually-uploaded documents) ─────────

ALTER TABLE documents ADD COLUMN IF NOT EXISTS connector_module VARCHAR(255);
-- e.g. "openrag", "confluence", "gdrive"

ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_uri TEXT;
-- Stable logical locator for the external source document.
-- Must NOT contain volatile revision selectors (no commit SHAs, no ?ref=<sha>).
-- Example: "joblogic/api-reference.pdf" — branch name OK, commit SHA NOT OK.

ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_revision TEXT;
-- IMMUTABLE per row. Set once on INSERT by the connector adapter.
-- Carries the exact external revision stamp at the moment of ingest
-- (e.g. git commit SHA, Confluence version ID, S3 ETag).
-- NULL for manually-uploaded documents.
-- NEVER updated after INSERT — see provenance immutability contract in
-- docs/adr/003-openrag-integration-ownership.md.

-- Fast lookup of active connector-originated documents by (module, source_uri)
CREATE INDEX IF NOT EXISTS idx_documents_connector_source
    ON documents(connector_module, source_uri) WHERE connector_module IS NOT NULL;


-- ── connector_sync_tasks ──────────────────────────────────────────────────────
-- Independent state machine for connector fetch runs.
-- Exists for ALL sync outcomes, including skipped_unchanged where no document
-- row is written. This is intentional: the sync state machine is decoupled from
-- the ingest pipeline state machine (ingest_jobs).
--
-- State transitions:
--   queued → fetching → fetched (verdict set)
--                     → failed
--
-- Verdict values:
--   fetched_new       — source_uri seen for first time; document + ingest_job created
--   fetched_updated   — content changed (source_hash differs); new version created
--   skipped_unchanged — content unchanged; NO document row written, NO ingest_job created
--   fetch_failed      — remote fetch error; NO document row written

CREATE TABLE IF NOT EXISTS connector_sync_tasks (
    task_id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    connector_module         VARCHAR(255) NOT NULL,
    scope                    VARCHAR(255) NOT NULL,
    source_uri               TEXT         NOT NULL,
    status                   VARCHAR(50)  NOT NULL DEFAULT 'queued',
    -- queued | fetching | fetched | failed
    verdict                  VARCHAR(50),
    -- fetched_new | fetched_updated | skipped_unchanged | fetch_failed
    observed_source_revision TEXT,
    -- Latest revision stamp observed at fetch time.
    -- Written here ONLY — never propagated back to documents.source_revision.
    -- A skipped_unchanged task still records observed_source_revision here
    -- so operators can see that the external source was checked.
    document_id              UUID,
    -- Populated for fetched_new and fetched_updated verdicts.
    -- NULL for skipped_unchanged (no document row written).
    job_id                   UUID,
    -- Populated when an ingest_job is created (fetched_new / fetched_updated only).
    -- NULL for skipped_unchanged and fetch_failed.
    error_message            TEXT,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_connector_sync_module_scope
    ON connector_sync_tasks(connector_module, scope);

CREATE INDEX IF NOT EXISTS idx_connector_sync_source_uri
    ON connector_sync_tasks(source_uri);

CREATE INDEX IF NOT EXISTS idx_connector_sync_document_id
    ON connector_sync_tasks(document_id);

CREATE INDEX IF NOT EXISTS idx_connector_sync_status
    ON connector_sync_tasks(status);

-- Latest task per (connector_module, source_uri) — used by adapter for dedup
CREATE INDEX IF NOT EXISTS idx_connector_sync_module_uri_created
    ON connector_sync_tasks(connector_module, source_uri, created_at DESC);
