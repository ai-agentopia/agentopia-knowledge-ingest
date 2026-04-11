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

-- Document registry: metadata and lifecycle state for every document
CREATE TABLE IF NOT EXISTS documents (
    document_id        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    scope              VARCHAR(255) NOT NULL,
    owner              VARCHAR(255) NOT NULL DEFAULT '',
    filename           VARCHAR(500) NOT NULL,
    format             VARCHAR(50)  NOT NULL,
    -- pdf | docx | html | markdown | txt
    version            INT          NOT NULL DEFAULT 1,
    status             VARCHAR(50)  NOT NULL DEFAULT 'submitted',
    -- submitted | normalizing | normalized | extracting | extracted | indexing | active | superseded | deleted | failed
    source_hash        VARCHAR(64)  NOT NULL,
    -- SHA-256 hex of original file bytes; used for duplicate detection
    s3_original_key    VARCHAR(500) NOT NULL,
    s3_normalized_key  VARCHAR(500),
    s3_extracted_key   VARCHAR(500),
    error_message      TEXT,
    -- populated when status = failed
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    metadata           JSONB        NOT NULL DEFAULT '{}'
);

-- At most one active version per document_id (partial unique index)
CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_one_active
    ON documents(document_id) WHERE status = 'active';

-- Duplicate detection: same scope + hash + version must not be re-ingested
CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_scope_hash_version
    ON documents(scope, source_hash, version);

CREATE INDEX IF NOT EXISTS idx_documents_scope_status ON documents(scope, status);
CREATE INDEX IF NOT EXISTS idx_documents_document_id  ON documents(document_id, version);

-- Ingest jobs: read model for operator; mirrors document status transitions
CREATE TABLE IF NOT EXISTS ingest_jobs (
    job_id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id      UUID        NOT NULL REFERENCES documents(document_id),
    status           VARCHAR(50) NOT NULL DEFAULT 'submitted',
    stage            VARCHAR(100),
    progress_percent INT         NOT NULL DEFAULT 0 CHECK (progress_percent BETWEEN 0 AND 100),
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ingest_jobs_document_id ON ingest_jobs(document_id);

-- Audit log: append-only record of all lifecycle events
-- Application role must have INSERT-only permission on this table.
CREATE TABLE IF NOT EXISTS audit_log (
    id            BIGSERIAL    PRIMARY KEY,
    timestamp     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    document_id   UUID,
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
