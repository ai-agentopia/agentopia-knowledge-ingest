-- Agentopia Knowledge Ingest — initial schema
-- Idempotent: safe to run multiple times

-- Scope registry: valid scopes that documents can be uploaded to
CREATE TABLE IF NOT EXISTS scopes (
    scope_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    scope_name  VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    owner       VARCHAR(255),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scopes_scope_name ON scopes(scope_name);

-- Document registry: metadata and lifecycle state for every document version.
--
-- document_id is the STABLE logical identity of a document: it is derived
-- deterministically from (scope, filename) and SHARED across all versions
-- of the same logical document.
--
-- version increments on each new upload of the same logical document.
-- (document_id, version) is the unique composite key per row.
--
-- Primary key is a surrogate row_id so ingest_jobs can FK to a single column.
CREATE TABLE IF NOT EXISTS documents (
    row_id             BIGSERIAL    NOT NULL PRIMARY KEY,
    document_id        UUID         NOT NULL,
    version            INT          NOT NULL DEFAULT 1,
    scope              VARCHAR(255) NOT NULL,
    owner              VARCHAR(255) NOT NULL DEFAULT '',
    filename           VARCHAR(500) NOT NULL,
    format             VARCHAR(50)  NOT NULL,
    -- pdf | docx | html | markdown | txt
    status             VARCHAR(50)  NOT NULL DEFAULT 'submitted',
    -- submitted | normalizing | normalized | extracting | extracted | indexing | active | superseded | deleted | failed
    source_hash        VARCHAR(64)  NOT NULL,
    -- SHA-256 hex of original file bytes
    s3_original_key    VARCHAR(500) NOT NULL DEFAULT '',
    s3_normalized_key  VARCHAR(500),
    s3_extracted_key   VARCHAR(500),
    error_message      TEXT,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    metadata           JSONB        NOT NULL DEFAULT '{}'
);

-- One row per (document_id, version): the composite version identity
CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_id_version
    ON documents(document_id, version);

-- At most one active version per logical document
CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_one_active
    ON documents(document_id) WHERE status = 'active';

-- Deduplication: same file bytes should not be re-ingested to same scope+version
CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_scope_hash_version
    ON documents(scope, source_hash, version);

-- Fast listing: all documents in a scope by status
CREATE INDEX IF NOT EXISTS idx_documents_scope_status ON documents(scope, status);

-- Fast lookup by document_id (all versions of a logical document)
CREATE INDEX IF NOT EXISTS idx_documents_document_id  ON documents(document_id);

-- Fast lookup by scope+filename (for stable document_id resolution)
CREATE INDEX IF NOT EXISTS idx_documents_scope_filename ON documents(scope, filename);


-- Ingest jobs: read model for operator; one job per upload attempt.
-- Tracks the current pipeline stage for the specific (document_id, version).
CREATE TABLE IF NOT EXISTS ingest_jobs (
    job_id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    row_id           BIGINT      NOT NULL REFERENCES documents(row_id),
    -- FK to the specific document version this job covers
    status           VARCHAR(50) NOT NULL DEFAULT 'submitted',
    stage            VARCHAR(100),
    progress_percent INT         NOT NULL DEFAULT 0 CHECK (progress_percent BETWEEN 0 AND 100),
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ingest_jobs_row_id ON ingest_jobs(row_id);

-- Audit log: append-only record of all lifecycle events.
-- The application role must have INSERT-only permission on this table.
CREATE TABLE IF NOT EXISTS audit_log (
    id            BIGSERIAL    PRIMARY KEY,
    timestamp     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    document_id   UUID,
    document_version INT,
    job_id        UUID,
    scope         VARCHAR(255),
    action        VARCHAR(100) NOT NULL,
    actor         VARCHAR(255),
    status        VARCHAR(50),
    error_message TEXT,
    metadata      JSONB        NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_audit_log_document_id ON audit_log(document_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_scope       ON audit_log(scope, timestamp DESC);
