# Agentopia Knowledge Ingest — Data Model

This document covers data models owned by `agentopia-knowledge-ingest`: the PostgreSQL tables managed by the Document Ingest Service, and the S3 object layout and artifact schemas used across the ingest pipeline.

For Qdrant chunk payload schema and Super RAG evaluation tables, see `agentopia-super-rag/docs/data-model.md`.

---

## PostgreSQL Tables

### documents

Source of truth for document metadata, version, and lifecycle state.

```sql
CREATE TABLE documents (
  document_id        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  scope              VARCHAR(255) NOT NULL,
  owner              VARCHAR(255) NOT NULL DEFAULT '',
  filename           VARCHAR(500) NOT NULL,
  format             VARCHAR(50)  NOT NULL,
  -- pdf | docx | html | markdown | txt
  version            INT          NOT NULL DEFAULT 1,
  status             VARCHAR(50)  NOT NULL DEFAULT 'submitted',
  -- submitted | normalizing | normalized | extracting | extracted |
  -- indexing | active | superseded | deleted | failed
  source_hash        VARCHAR(64)  NOT NULL,
  -- SHA-256 hex of original file; used for duplicate detection
  s3_original_key    VARCHAR(500) NOT NULL,
  s3_normalized_key  VARCHAR(500),
  -- null until Normalizer completes
  s3_extracted_key   VARCHAR(500),
  -- null until Extractor completes
  created_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMP    NOT NULL DEFAULT NOW(),
  metadata           JSONB        NOT NULL DEFAULT '{}'
);

-- Only one active version per document_id at any time (enforced in application layer)
CREATE UNIQUE INDEX idx_documents_active_version
  ON documents(document_id)
  WHERE status = 'active';

-- Duplicate detection: same scope + hash + version must not be re-ingested
CREATE UNIQUE INDEX idx_documents_scope_hash_version
  ON documents(scope, source_hash, version);

CREATE INDEX idx_documents_scope_status ON documents(scope, status);
CREATE INDEX idx_documents_document_id  ON documents(document_id, version);
```

**Constraints:**
- At most one row per `document_id` may have `status = 'active'` at any time (partial unique index above)
- `source_hash` is the SHA-256 of the raw uploaded file bytes; used for deduplication
- `version` increments monotonically per `document_id`; gaps are not allowed
- `scope` must match a row in the `scopes` table (enforced in application layer, not FK, to avoid cross-table lock on ingest)

---

### ingest_jobs

Read model for operators: tracks the current pipeline position of each ingest job. Jobs are created at upload time and updated as the document moves through pipeline stages.

```sql
CREATE TABLE ingest_jobs (
  job_id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id      UUID         NOT NULL REFERENCES documents(document_id),
  status           VARCHAR(50)  NOT NULL DEFAULT 'submitted',
  -- submitted | normalizing | normalized | extracting | extracted |
  -- indexing | active | failed
  stage            VARCHAR(100),
  -- Human-readable current stage detail; e.g. "parsing pdf: page 12/42"
  progress_percent INT          NOT NULL DEFAULT 0 CHECK (progress_percent BETWEEN 0 AND 100),
  error_message    TEXT,
  -- Present when status = failed
  created_at       TIMESTAMP    NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMP    NOT NULL DEFAULT NOW(),
  completed_at     TIMESTAMP
  -- Set when status reaches active or failed
);

CREATE INDEX idx_ingest_jobs_document_id ON ingest_jobs(document_id);
CREATE INDEX idx_ingest_jobs_status      ON ingest_jobs(status) WHERE status NOT IN ('active', 'failed');
```

**Notes:**
- `ingest_jobs.status` mirrors `documents.status` — they are kept in sync as pipeline stages complete
- `job_id` is stable and returned to the operator on upload; it is the polling key
- Completed jobs (status = `active` or `failed`) are retained for audit history; they are not deleted

---

### scopes

Registry of valid scopes. A scope must exist here before any document can be uploaded to it.

```sql
CREATE TABLE scopes (
  scope_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  scope_name  VARCHAR(255) NOT NULL UNIQUE,
  -- Format: {tenant}/{domain}; validated by application layer
  description TEXT,
  owner       VARCHAR(255),
  created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_scopes_scope_name ON scopes(scope_name);
```

**Format constraint:** `scope_name` must match `^[a-z0-9-]+/[a-z0-9-]+$`. Validated by the application layer before insert.

---

### audit_log

Append-only record of all document lifecycle events. Rows are never updated or deleted.

```sql
CREATE TABLE audit_log (
  id            BIGSERIAL    PRIMARY KEY,
  timestamp     TIMESTAMP    NOT NULL DEFAULT NOW(),
  document_id   UUID,
  job_id        UUID,
  scope         VARCHAR(255),
  action        VARCHAR(100) NOT NULL,
  -- uploaded | normalizing | normalized | extracting | extracted |
  -- indexing | active | superseded | deleted | rolled_back | failed
  actor         VARCHAR(255),
  -- operator identifier from upload request; or service name for pipeline events
  status        VARCHAR(50),
  error_message TEXT,
  metadata      JSONB        NOT NULL DEFAULT '{}'
);

-- Audit log is insert-only. No UPDATE or DELETE permissions granted to the application role.
CREATE INDEX idx_audit_log_document_id ON audit_log(document_id, timestamp DESC);
CREATE INDEX idx_audit_log_scope       ON audit_log(scope, timestamp DESC);
```

**Enforcement:** The PostgreSQL application role must have INSERT-only permission on `audit_log`. No UPDATE or DELETE. Enforced at the database permission level, not only in application code.

---

## S3 Object Layout

All ingest artifacts are stored in a single S3 bucket under a consistent key prefix.

### Key structure

```
{bucket}/
  documents/
    {scope}/
      {document_id}/
        v{version}/
          original.{pdf|docx|html|md|txt}     <- immutable; written once at upload
          metadata.json                         <- immutable; written at upload
          normalized.json                       <- immutable; written by Normalizer
          extracted.json                        <- immutable; written by Extractor
```

**Immutability rule:** Once an object is written under a `v{version}/` prefix, it is never overwritten. A new version always creates a new prefix. Existing prefixes are read-only after initial write.

**Scope in key:** `{scope}` uses `/` as a path separator, so `joblogic-kb/api-docs` becomes `documents/joblogic-kb/api-docs/{document_id}/...`. Both AWS S3 and compatible stores (MinIO, GCS) treat `/` as a delimiter, producing a browse-friendly layout.

---

### metadata.json

Written by the Document Ingest Service immediately on upload, before any parsing begins.

```json
{
  "document_id":  "3f7a2b1c-...",
  "scope":        "joblogic-kb/api-docs",
  "version":      1,
  "filename":     "api-reference.pdf",
  "format":       "pdf",
  "owner":        "operator@agentopia.io",
  "source_hash":  "sha256:a3f4b2...",
  "uploaded_at":  "2026-04-11T10:30:00Z",
  "s3_prefix":    "documents/joblogic-kb/api-docs/3f7a2b1c-.../v1/"
}
```

| Field | Type | Description |
|---|---|---|
| `document_id` | UUID string | Stable document identifier |
| `scope` | string | Target scope in `{tenant}/{domain}` format |
| `version` | integer | Version number for this upload |
| `filename` | string | Original filename as provided by operator |
| `format` | string | Detected format: `pdf`, `docx`, `html`, `markdown`, `txt` |
| `owner` | string | Operator identifier from upload request |
| `source_hash` | string | `sha256:{hex}` of the raw uploaded file bytes |
| `uploaded_at` | ISO8601 | Upload timestamp |
| `s3_prefix` | string | Full S3 key prefix for this version's artifacts |

---

### normalized.json

Written by the Normalizer Service after successfully parsing the original file.

```json
{
  "document_id":        "3f7a2b1c-...",
  "version":            1,
  "format":             "pdf",
  "text":               "Full extracted plain text content of the document...",
  "headings":           ["Introduction", "Authentication", "Token Format", "Endpoints"],
  "structure_hints": {
    "has_tables":        true,
    "has_code_blocks":   false,
    "has_numbered_lists": true,
    "heading_count":     12,
    "estimated_tokens":  8400
  },
  "normalized_at":      "2026-04-11T10:30:45Z",
  "normalizer_version": "1.0.0"
}
```

| Field | Type | Description |
|---|---|---|
| `document_id` | UUID string | Matches `metadata.json` |
| `version` | integer | Matches `metadata.json` |
| `format` | string | Format that was parsed |
| `text` | string | Full extracted plain text. Preserves paragraph breaks with newlines. Does not include images or binary content. |
| `headings` | string array | Detected headings in document order. Used by Extractor to build hierarchy. Empty array if no headings detected. |
| `structure_hints` | object | Flags and counts to inform downstream extraction and chunking decisions |
| `normalized_at` | ISO8601 | Normalization completion timestamp |
| `normalizer_version` | string | Normalizer service version for reproducibility |

---

### extracted.json

Written by the Metadata Extractor Service after processing `normalized.json`.

```json
{
  "document_id":        "3f7a2b1c-...",
  "version":            1,
  "title":              "API Reference",
  "author":             "Engineering Team",
  "date":               "2026-01-15",
  "language":           "en",
  "hierarchy": [
    {
      "level":    1,
      "heading":  "Authentication",
      "children": [
        {
          "level":    2,
          "heading":  "Token Format",
          "children": []
        },
        {
          "level":    2,
          "heading":  "Bearer Tokens",
          "children": []
        }
      ]
    },
    {
      "level":    1,
      "heading":  "Endpoints",
      "children": []
    }
  ],
  "tags":               ["api", "authentication", "reference"],
  "extraction_method":  "heuristic",
  "extracted_at":       "2026-04-11T10:31:00Z",
  "extractor_version":  "1.0.0"
}
```

| Field | Type | Description |
|---|---|---|
| `document_id` | UUID string | Matches `metadata.json` |
| `version` | integer | Matches `metadata.json` |
| `title` | string | Document title. Source: first `#` heading, frontmatter `title` field, or PDF metadata title. Empty string if not detected. |
| `author` | string | Author name. Source: markdown frontmatter `author` field or PDF metadata. Empty string if not detected. |
| `date` | string | Publication or modification date in `YYYY-MM-DD` format. Source: frontmatter `date` field or filename date pattern. Empty string if not detected. |
| `language` | string | ISO 639-1 language code detected from text. Defaults to `"en"` if detection is inconclusive. |
| `hierarchy` | array | Tree structure of document sections. Each node has `level` (1=H1, 2=H2, etc.), `heading` (string), and `children` (array of same shape). Used to populate `section_path` in Qdrant chunk payload. |
| `tags` | string array | Keywords or tags extracted from content or frontmatter. May be empty. |
| `extraction_method` | string | `"heuristic"` (day-1) or `"llm"` (phase-2, if LLM extraction is promoted). |
| `extracted_at` | ISO8601 | Extraction completion timestamp |
| `extractor_version` | string | Extractor service version for reproducibility |

**Partial extraction:** If extraction partially fails (e.g., title not found, hierarchy not detected), missing fields are set to their empty defaults (`""`, `[]`). Extraction never fails completely; it always produces a valid `extracted.json`. The pipeline proceeds to indexing with partial metadata rather than blocking.
