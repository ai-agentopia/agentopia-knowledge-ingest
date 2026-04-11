# Operator Knowledge Runbook

This runbook covers the full document lifecycle for Agentopia knowledge operators: uploading, managing, replacing, and rolling back documents across scopes.

---

## Prerequisites

- `agentopia-knowledge-ingest` service running at `http://localhost:8003` (local) or internal cluster URL
- `agentopia-super-rag` service running at `http://localhost:8002` (local) or internal cluster URL
- `SUPER_RAG_INTERNAL_TOKEN` for authenticated Super RAG API calls
- Access to the operator console at `GET http://localhost:8003/ui`

---

## Operator Console

Navigate to `http://<ingest-service-host>/ui` for the full operator console. The console provides:

- **Upload**: upload a document and track its ingest job
- **Documents**: list documents in a scope by status
- **Version History & Rollback**: view all versions of a document; rollback to any prior version
- **Scopes**: list and create scopes
- **Quality Dashboard**: view per-scope evaluation baselines and recent evaluation results (requires Super RAG URL and token)
- **Retrieval Debugger**: query a scope and inspect ranked results with chunk text, section path, score, and document lineage

---

## 1. Create a Scope

Before uploading documents, a scope must exist.

**Via API:**
```bash
curl -X POST http://localhost:8003/scopes \
  -H "Content-Type: application/json" \
  -d '{"scope_name": "joblogic-kb/api-docs", "description": "Joblogic API documentation", "owner": "operator@example.com"}'
```

**Via UI:** Use the Scopes section.

Scope names must match `{tenant}/{domain}` — lowercase letters, digits, and hyphens only. Example: `joblogic-kb/api-docs`, `customer-portal/onboarding`.

---

## 2. Upload a Document

**Via API:**
```bash
curl -X POST "http://localhost:8003/documents/upload?scope=joblogic-kb/api-docs&owner=operator@example.com" \
  -F "file=@/path/to/api-reference.pdf"
```

**Response:**
```json
{"job_id": "...", "document_id": "...", "scope": "joblogic-kb/api-docs", "version": 1, "skipped": false}
```

The upload returns immediately. The document is not yet visible to retrieval — it must pass through normalization, extraction, and indexing before reaching `active` state.

**Track progress:**
```bash
curl http://localhost:8003/jobs/<job_id>
```

States: `submitted` → `normalizing` → `normalized` → `extracting` → `extracted` → `indexing` → `active` (or `failed`).

**Supported formats:** PDF, DOCX, HTML, Markdown, TXT.

---

## 3. Verify a Document is Indexed

Once the job reaches `active`, the document is visible to retrieval queries.

```bash
curl "http://localhost:8003/documents?scope=joblogic-kb/api-docs&status=active"
```

Each active document shows: `filename`, `format`, `version`, `status`, `created_at`, `updated_at`.

Use the retrieval debugger to confirm the document appears in search results:
```bash
curl "http://localhost:8002/api/v1/knowledge/debug/query?scope=joblogic-kb/api-docs&q=authentication" \
  -H "X-Internal-Token: $SUPER_RAG_INTERNAL_TOKEN"
```

---

## 4. Replace a Document (New Version)

To replace a document, upload the new version using the same filename. The `document_id` is stable — derived from `(scope, filename)`.

```bash
curl -X POST "http://localhost:8003/documents/upload?scope=joblogic-kb/api-docs" \
  -F "file=@/path/to/api-reference-v2.pdf"
```

On successful ingest:
- Old version (v1) transitions from `active` → `superseded`
- New version (v2) becomes `active`
- Retrieval immediately queries the new version

After replacement, the evaluation service automatically runs the scope's golden questions and compares against the baseline. If nDCG@5 drops by more than 0.02, a `blocked` verdict is recorded and a WARNING is logged. The document remains active — the operator must decide to accept or rollback.

---

## 5. Roll Back to a Prior Version

If a replacement degrades quality or introduces errors, roll back to the prior version.

**Check version history:**
```bash
curl http://localhost:8003/documents/<document_id>/versions
```

**Rollback:**
```bash
curl -X POST "http://localhost:8003/documents/<document_id>/rollback" \
  -H "Content-Type: application/json" \
  -d '{"version": 1, "reason": "v2 broke authentication section"}'
```

On rollback:
- Target version transitions `superseded` → `active`
- Current active transitions `active` → `superseded`
- Retrieval immediately queries the restored version
- Rollback is recorded in the audit log

**Note:** Only `active` and `superseded` versions can be restored. `deleted` versions cannot be restored.

---

## 6. Handle a Failed Ingest

If a document fails normalization or indexing, the job shows `status: failed` with an `error_message`.

**Common failure reasons:**
- PDF is scanned/image-only (no extractable text)
- DOCX is password-protected
- File is empty
- Super RAG embedding API unavailable (check `SUPER_RAG_URL`)

**Recovery:**
1. Fix the source file
2. Re-upload using the same filename — this creates a new version (v2, v3, etc.)
3. If you need to clean up a failed version row, contact the platform team — failed versions do not appear in active listings and do not affect retrieval

The prior active version (if any) remains unaffected when ingest fails — retrieval continues serving the prior version.

---

## 7. View Evaluation Quality

**Check per-scope baseline:**
```bash
curl "http://localhost:8002/api/v1/evaluation/baselines/joblogic-kb/api-docs" \
  -H "X-Internal-Token: $SUPER_RAG_INTERNAL_TOKEN"
```

**View recent evaluation runs:**
```bash
curl "http://localhost:8002/api/v1/evaluation/results?scope=joblogic-kb/api-docs" \
  -H "X-Internal-Token: $SUPER_RAG_INTERNAL_TOKEN"
```

**Accept a blocked regression:**

If a replacement was blocked (nDCG@5 dropped >0.02) but you've reviewed the results and accept the change:
```bash
curl -X POST "http://localhost:8002/api/v1/evaluation/results/<result_id>/override" \
  -H "X-Internal-Token: $SUPER_RAG_INTERNAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"operator_note": "New API reference is more complete; known nDCG regression", "operator_identity": "operator@example.com"}'
```

This records the override in the evaluation audit trail but does not change the document status.

---

## 8. Add Golden Questions (for Evaluation)

Golden questions define the expected quality bar for a scope. They must be authored before a baseline can be established.

```bash
# Add a golden question
curl -X POST "http://localhost:8002/api/v1/evaluation/questions/joblogic-kb/api-docs" \
  -H "X-Internal-Token: $SUPER_RAG_INTERNAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "How do I authenticate with the API?",
    "expected_sources": [
      {"source": "api-reference.pdf", "relevance": 2},
      {"source": "quickstart-guide.pdf", "relevance": 1}
    ],
    "weight": 1.0,
    "created_by": "operator@example.com"
  }'

# Establish baseline
curl -X POST "http://localhost:8002/api/v1/evaluation/baselines/joblogic-kb/api-docs" \
  -H "X-Internal-Token: $SUPER_RAG_INTERNAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"notes": "Initial baseline after pilot documents indexed"}'
```

Relevance grades: 2 = fully relevant, 1 = partially relevant, 0 = not relevant.

---

## 9. Debug Retrieval Quality

When search results are unexpected, use the retrieval debugger to inspect ranked chunks:

```bash
curl "http://localhost:8002/api/v1/knowledge/debug/query?scope=joblogic-kb/api-docs&q=authentication&limit=5" \
  -H "X-Internal-Token: $SUPER_RAG_INTERNAL_TOKEN"
```

**Response fields:**
- `rank`: result position (1 = highest score)
- `score`: cosine similarity score
- `source`: document filename/path that produced this chunk
- `section_path`: heading hierarchy path (e.g., "Authentication > Token Format")
- `section`: immediate section heading
- `chunk_index`: position within the source document
- `document_hash`: SHA-256 of source document (provenance)
- `ingested_at`: Unix timestamp when chunk was indexed

If a document you expect in results is not appearing:
1. Check it is in `active` status (`GET /documents?scope=...&status=active`)
2. Check the job reached `active` state (`GET /jobs/<job_id>`)
3. If chunks are present but ranked low, consider adding golden questions and checking the baseline

---

## 10. Configure Connector Scope Mapping

Connector integrations (OpenRAG, Confluence, etc.) automatically route documents into
knowledge scopes based on mapping rules you configure. Without a mapping, the
connector must supply the scope explicitly on each event, or ingestion fails.

### Why this matters

Each connector module pushes documents from an external source. The scope mapping
determines **which knowledge scope** receives those documents. Misconfigured or
missing mappings cause ingest to fail with `fetch_failed` verdict and the error:

```
Scope 'X' is not registered in the knowledge registry.
Create the scope via POST /scopes before connector ingestion proceeds.
```

### Configuration options (pick one)

**Option A — Environment variable** (recommended for Kubernetes):

```bash
export CONNECTOR_SCOPE_MAPPINGS='[
  {"connector_module": "openrag", "source_pattern": "joblogic/*",   "scope": "joblogic-kb/docs"},
  {"connector_module": "openrag", "source_pattern": "portal/*",     "scope": "portal-kb/docs"},
  {"connector_module": "openrag", "source_pattern": "*",            "scope": "default-kb/general"}
]'
```

**Option B — Config file** (recommended for many rules):

```bash
export CONNECTOR_SCOPE_MAPPINGS_FILE=/etc/agentopia/connector-scopes.json
```

```json
[
  {"connector_module": "openrag", "source_pattern": "joblogic/*",   "scope": "joblogic-kb/docs"},
  {"connector_module": "openrag", "source_pattern": "portal/*",     "scope": "portal-kb/docs"},
  {"connector_module": "openrag", "source_pattern": "*",            "scope": "default-kb/general"}
]
```

If both are set, the env var takes precedence. If neither is set, connectors must
supply `event.scope` directly (usable from code; not the normal operator flow).

### Rule format

| Field | Description |
|---|---|
| `connector_module` | Exact connector identifier (e.g. `"openrag"`, `"confluence"`) |
| `source_pattern` | fnmatch glob applied to the document's `source_uri` |
| `scope` | Target scope name — must be registered via `POST /scopes` first |

Rules are evaluated in order. **First match wins.** Add a wildcard catch-all (`*`)
as the last rule to ensure every document lands somewhere.

### Setup checklist

1. Create the target scope(s) first:
   ```bash
   curl -X POST http://localhost:8003/scopes \
     -H "Content-Type: application/json" \
     -d '{"scope_name": "joblogic-kb/docs", "description": "Joblogic docs", "owner": "operator@example.com"}'
   ```

2. Set the mapping configuration (env var or file).

3. Restart or reload the ingest service to pick up new rules.
   To reload without restart (in a running process), the service exposes no reload endpoint
   in Phase 1 — restart is required for config changes.

4. Verify a sync task reaches `fetched` status:
   ```bash
   # connector_sync_tasks are viewable via operator console at /ui
   # or check the ingest job for the document_id
   curl http://localhost:8003/jobs/<job_id>
   ```

### Troubleshooting scope mapping failures

| Symptom | Likely cause | Fix |
|---|---|---|
| `fetch_failed` with "Scope not registered" | Scope name in mapping doesn't match a registered scope | Create scope via `POST /scopes` |
| `fetch_failed` with "No scope could be resolved" | No mapping matches and `event.scope` is empty | Add a wildcard catch-all rule or set `event.scope` |
| Documents land in wrong scope | Rule order wrong | Move more-specific rules before the wildcard |

---

## 11. Trigger Ingestion via HTTP (W-C3.1)

`POST /connectors/ingest` is the HTTP transport entry point for out-of-process connectors
or any caller that cannot use the in-process `ingest_from_connector()` adapter directly.

**All W-C1 invariants are enforced by the adapter — this endpoint adds no business logic.**

### Request

```bash
curl -X POST http://localhost:8003/connectors/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "connector_module": "aws_s3",
    "scope": "joblogic-kb/docs",
    "source_uri": "s3://my-bucket/docs/api-reference.pdf",
    "filename": "api-reference.pdf",
    "format": "pdf",
    "raw_bytes_b64": "<base64-encoded file content>",
    "source_revision": "2026-01-15T10:30:00+00:00",
    "owner": "operator@example.com",
    "metadata": {"s3_bucket": "my-bucket", "s3_key": "docs/api-reference.pdf"}
  }'
```

| Field | Required | Description |
|---|---|---|
| `connector_module` | yes | Connector identifier (e.g. `"aws_s3"`, `"confluence"`) |
| `scope` | yes | Target knowledge scope (must be registered via `POST /scopes`) |
| `source_uri` | yes | Stable logical locator for the source document |
| `filename` | yes | Display filename (e.g. `"api-reference.pdf"`) |
| `format` | yes | One of: `pdf`, `docx`, `html`, `markdown`, `txt` |
| `raw_bytes_b64` | yes | Base64-encoded (RFC 4648) raw document bytes |
| `source_revision` | no | Revision stamp for provenance (e.g. ETag, LastModified) |
| `owner` | no | Actor identifier for audit log |
| `metadata` | no | Extra key/value pairs passed through to document metadata |

### Response

HTTP 200 for all adapter verdicts (including `fetch_failed`):

```json
{
  "task_id": "...",
  "verdict": "fetched_new",
  "document_id": "...",
  "job_id": "...",
  "version": 1,
  "error_message": null
}
```

| `verdict` | Meaning |
|---|---|
| `fetched_new` | Document created for the first time |
| `fetched_updated` | New version created (content changed) |
| `skipped_unchanged` | SHA-256 matches active version — no write |
| `fetch_failed` | Scope not registered or adapter-level error |

### Error responses

| Status | Cause |
|---|---|
| 422 | Malformed body, missing required field, invalid format, or invalid base64 |
| 413 | Decoded payload exceeds 50 MiB |

### Auth

No bearer token required — same internal-network-only boundary as the operator UI.

---

## 12. Common Operations Quick Reference

| Task | Command |
|---|---|
| Create scope | `POST /scopes` |
| Upload document | `POST /documents/upload?scope=...` |
| Check job status | `GET /jobs/{job_id}` |
| List active documents | `GET /documents?scope=...&status=active` |
| View version history | `GET /documents/{id}/versions` |
| Rollback to prior version | `POST /documents/{id}/rollback` |
| List evaluation results | `GET /evaluation/results?scope=...` (Super RAG) |
| Add golden question | `POST /evaluation/questions/{scope}` (Super RAG) |
| Establish baseline | `POST /evaluation/baselines/{scope}` (Super RAG) |
| Override regression block | `POST /evaluation/results/{id}/override` (Super RAG) |
| Debug retrieval | `GET /knowledge/debug/query?scope=...&q=...` (Super RAG) |
| Configure scope mapping (env) | `CONNECTOR_SCOPE_MAPPINGS='[...]'` |
| Configure scope mapping (file) | `CONNECTOR_SCOPE_MAPPINGS_FILE=/path/to/rules.json` |
| HTTP connector ingest | `POST /connectors/ingest` |

---

## 12. Configure and Run the AWS S3 Connector (W-C2.1)

The AWS S3 connector pulls documents from S3-compatible object storage into a
knowledge scope. It uses the `ingest_from_connector()` adapter (W-C1) — all scope
validation, dedup, versioning, and provenance rules apply unchanged.

**Source:** `langflow-ai/openrag` `src/connectors/aws_s3/connector.py` (adapted).
**Implementation:** `src/connectors/openrag_s3.py`, `src/connectors/aws_s3_wrapper.py`.
**Issue:** knowledge-ingest#37.

### Prerequisites

1. Create the target scope (if not already done):
   ```bash
   curl -X POST http://localhost:8003/scopes \
     -H "Content-Type: application/json" \
     -d '{"scope_name": "joblogic-kb/s3-docs", "description": "S3 documents", "owner": "operator@example.com"}'
   ```

2. Configure scope mapping for the S3 connector:
   ```bash
   export CONNECTOR_SCOPE_MAPPINGS='[
     {"connector_module": "aws_s3", "source_pattern": "s3://my-bucket/*", "scope": "joblogic-kb/s3-docs"}
   ]'
   ```

### Secret handling requirements

Credentials must come from managed secret storage — **never hardcode them**.

Minimum required secret fields:

| Field | Description |
|---|---|
| `access_key` | AWS Access Key ID (IAM user with `s3:GetObject`, `s3:ListBucket`) |
| `secret_key` | AWS Secret Access Key |
| `endpoint_url` | Optional; set for MinIO / Cloudflare R2 / custom S3-compatible endpoints |
| `region` | Optional; default is `us-east-1` |
| `bucket_names` | List of buckets to ingest from; empty list = auto-discover all accessible buckets |
| `prefix` | Optional key prefix filter (e.g. `"docs/"`) |

**K8s Secret example:**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: s3-connector-creds
type: Opaque
stringData:
  access_key: "<AWS_ACCESS_KEY_ID>"
  secret_key: "<AWS_SECRET_ACCESS_KEY>"
  bucket_names: '["my-docs-bucket"]'
```

### Running a sync

```python
import asyncio
from connectors.aws_s3_wrapper import sync_s3_bucket

config = {
    "access_key": "<from secret store>",
    "secret_key": "<from secret store>",
    "bucket_names": ["my-docs-bucket"],
    "prefix": "docs/",
    "scope": "joblogic-kb/s3-docs",   # fallback if CONNECTOR_SCOPE_MAPPINGS not set
    "owner": "operator@example.com",
}

results = asyncio.run(sync_s3_bucket(config))
for r in results:
    print(r.verdict, r.source_uri if hasattr(r, "source_uri") else "")
```

### Identity contract

| Field | Value |
|---|---|
| `connector_module` | `"aws_s3"` |
| `source_uri` | `s3://{bucket}/{key}` |
| `source_revision` | `LastModified` ISO 8601 UTC string from boto3 |
| Rename behaviour | Object rename = new `document_id` (new document, v1). Same semantics as manual upload rename. |
| Unchanged detection | SHA-256 of raw bytes. Same object re-fetched with identical content → `skipped_unchanged`. |

### Webhook support

Not implemented in W-C2.1. The connector uses pull-based sync only. Run
`sync_s3_bucket()` on a schedule or trigger it manually. S3 event notification
integration is a future issue.

---

## 12. Configure and Run the AWS S3 Connector (W-C2.1)

The AWS S3 connector pulls documents from S3-compatible object storage into a
knowledge scope. It uses the `ingest_from_connector()` adapter (W-C1) — all scope
validation, dedup, versioning, and provenance rules apply unchanged.

**Source:** `langflow-ai/openrag` `src/connectors/aws_s3/connector.py` (adapted).
**Implementation:** `src/connectors/openrag_s3.py`, `src/connectors/aws_s3_wrapper.py`.
**Issue:** knowledge-ingest#37.

### Prerequisites

1. Create the target scope (if not already done):
   ```bash
   curl -X POST http://localhost:8003/scopes \
     -H "Content-Type: application/json" \
     -d '{"scope_name": "joblogic-kb/s3-docs", "description": "S3 documents", "owner": "operator@example.com"}'
   ```

2. Configure scope mapping for the S3 connector:
   ```bash
   export CONNECTOR_SCOPE_MAPPINGS='[
     {"connector_module": "aws_s3", "source_pattern": "s3://my-bucket/*", "scope": "joblogic-kb/s3-docs"}
   ]'
   ```

### Secret handling requirements

Credentials must come from managed secret storage — **never hardcode them**.

Minimum required secret fields:

| Field | Description |
|---|---|
| `access_key` | AWS Access Key ID (IAM user with `s3:GetObject`, `s3:ListBucket`) |
| `secret_key` | AWS Secret Access Key |
| `endpoint_url` | Optional; set for MinIO / Cloudflare R2 / custom S3-compatible endpoints |
| `region` | Optional; default is `us-east-1` |
| `bucket_names` | List of buckets to ingest from; empty list = auto-discover all accessible buckets |
| `prefix` | Optional key prefix filter (e.g. `"docs/"`) |

**K8s Secret example:**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: s3-connector-creds
type: Opaque
stringData:
  access_key: "<AWS_ACCESS_KEY_ID>"
  secret_key: "<AWS_SECRET_ACCESS_KEY>"
  bucket_names: '["my-docs-bucket"]'
```

### Running a sync

```python
import asyncio
from connectors.aws_s3_wrapper import sync_s3_bucket

config = {
    "access_key": "<from secret store>",
    "secret_key": "<from secret store>",
    "bucket_names": ["my-docs-bucket"],
    "prefix": "docs/",
    "scope": "joblogic-kb/s3-docs",   # fallback if CONNECTOR_SCOPE_MAPPINGS not set
    "owner": "operator@example.com",
}

results = asyncio.run(sync_s3_bucket(config))
for r in results:
    print(r.verdict, r.source_uri if hasattr(r, "source_uri") else "")
```

### Identity contract

| Field | Value |
|---|---|
| `connector_module` | `"aws_s3"` |
| `source_uri` | `s3://{bucket}/{key}` |
| `source_revision` | `LastModified` ISO 8601 UTC string from boto3 |
| Rename behaviour | Object rename = new `document_id` (new document, v1). Same semantics as manual upload rename. |
| Unchanged detection | SHA-256 of raw bytes. Same object re-fetched with identical content → `skipped_unchanged`. |

### Webhook support

Not implemented in W-C2.1. The connector uses pull-based sync only. Run
`sync_s3_bucket()` on a schedule or trigger it manually. S3 event notification
integration is a future issue.

---

## Service URLs

| Service | Default Port | Auth |
|---|---|---|
| Document Ingest Service | 8003 | None (internal network only) |
| Super RAG | 8002 | `X-Internal-Token` for write/debug; bearer token for bot queries |
| Operator Console | 8003/ui | Browser, no auth |
