-- 003_scope_ingest_mode.sql
--
-- Adds scope_ingest_mode to the local scopes registry to enforce the
-- single-publisher invariant during the Pathway migration.
--
-- Values:
--   'legacy'   — default. This service is the active ingest path.
--                Upload and orchestrator proceed normally.
--   'pathway'  — agentopia-rag-platform Pathway pipeline is the sole writer.
--                POST /documents/upload returns 409 Conflict for this scope.
--                Orchestrator tasks are blocked for this scope.
--
-- Source of truth: bot-config-api knowledge_bases.scope_ingest_mode.
-- This column is a local mirror, synced via PATCH /scopes/{scope}/ingest-mode.
-- Bot-config-api calls this endpoint when the mode changes.

ALTER TABLE scopes
    ADD COLUMN IF NOT EXISTS scope_ingest_mode VARCHAR(20) NOT NULL DEFAULT 'legacy';

ALTER TABLE scopes
    DROP CONSTRAINT IF EXISTS chk_scopes_ingest_mode;

ALTER TABLE scopes
    ADD CONSTRAINT chk_scopes_ingest_mode
    CHECK (scope_ingest_mode IN ('legacy', 'pathway'));
