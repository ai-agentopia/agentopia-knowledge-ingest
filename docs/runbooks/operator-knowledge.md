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

## 12. Trigger an AWS S3 Sync (W-C3.5)

`POST /connectors/s3/sync` is the server-side S3 sync trigger. It calls the existing
W-C2.1 wrapper (`sync_s3_bucket()`) which routes through `ingest_from_connector()`.

**Credentials are never sent through the browser or request body.** They are resolved
server-side from environment variables using a `secret_ref` key.

### Configure S3 credentials

**Default credentials** (no `secret_ref` or `secret_ref: "default"`):

```bash
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
```

**Named credentials** (`secret_ref: "prod"` → looks up `S3_SECRET_PROD_*`):

```bash
export S3_SECRET_PROD_ACCESS_KEY=<your-access-key>
export S3_SECRET_PROD_SECRET_KEY=<your-secret-key>
```

For Kubernetes, store these as a K8s Secret and inject into the pod env. Never hardcode credentials.

### Trigger via API

```bash
curl -X POST http://localhost:8003/connectors/s3/sync \
  -H "Content-Type: application/json" \
  -d '{
    "scope": "joblogic-kb/s3-docs",
    "bucket": "my-docs-bucket",
    "secret_ref": "prod",
    "prefix": "docs/",
    "region": "us-east-1",
    "owner": "operator@example.com"
  }'
```

### Trigger via Operator Console

Navigate to `GET /ui` → **AWS S3 Sync** section. Enter scope, bucket, and `secret_ref`.
Credentials must already be set in the service environment — do not enter access keys in the form.

### Response

```json
{
  "scope": "joblogic-kb/s3-docs",
  "bucket": "my-docs-bucket",
  "total": 3,
  "results": [
    {
      "source_uri": "s3://my-docs-bucket/docs/api-reference.pdf",
      "verdict": "fetched_new",
      "task_id": "...",
      "error_message": null
    }
  ]
}
```

| `verdict` | Meaning |
|---|---|
| `fetched_new` | Document ingested for the first time |
| `fetched_updated` | New version created (content changed) |
| `skipped_unchanged` | SHA-256 matches active version — no write |
| `fetch_failed` | Scope not registered, auth failure, or object-level error |

### Error responses

| Status | Cause |
|---|---|
| 422 | Malformed body, missing `scope` or `bucket`, invalid `secret_ref` characters |
| 503 | S3 credentials not found in environment, or S3 authentication rejected |

### IBM COS

IBM COS trigger UI is W-C3.6 (knowledge-ingest#50), which depends on the IBM COS connector
wrapper (knowledge-ingest#41). Do not use this endpoint for IBM COS.

---

## 13. Common Operations Quick Reference

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
| HTTP connector ingest (generic) | `POST /connectors/ingest` |
| AWS S3 sync trigger (UI or API) | `POST /connectors/s3/sync` |
| Google Drive sync (script) | `sync_gdrive(config)` |

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

## 14. Configure and Run the Google Drive Connector (W-C2.2)

The Google Drive connector pulls documents from Google Drive into a knowledge scope.
It uses the `ingest_from_connector()` adapter (W-C1) — all scope validation, dedup,
versioning, and provenance rules apply unchanged.

**Source:** `langflow-ai/openrag` `src/connectors/google_drive/` (adapted).
**Implementation:** `src/connectors/openrag_gdrive.py`, `src/connectors/google_drive_wrapper.py`.
**Issue:** knowledge-ingest#38.

### Prerequisites

1. Create a Google Cloud project and enable the Google Drive API.

2. Create an OAuth 2.0 client ID (Desktop or Web application type).
   Download the client credentials JSON from the Google Cloud Console.

3. Run the OAuth setup flow **once** to obtain a refresh token and store it in the
   token file. The service account or operator must complete this browser flow
   out-of-band. The token file is then stored in managed secret storage.
   ```bash
   # Example using google-auth-oauthlib (outside this service):
   python -c "
   from google_auth_oauthlib.flow import InstalledAppFlow
   import json
   flow = InstalledAppFlow.from_client_secrets_file(
       'credentials.json',
       scopes=['https://www.googleapis.com/auth/drive.readonly',
               'https://www.googleapis.com/auth/drive.metadata.readonly']
   )
   creds = flow.run_local_server()
   token_data = {'token': creds.token, 'refresh_token': creds.refresh_token, 'scopes': list(creds.scopes)}
   open('gdrive_token.json', 'w').write(json.dumps(token_data, indent=2))
   print('Token saved to gdrive_token.json')
   "
   ```

4. Store the token file on a **writable volume** (for example a PVC) and mount it
   at the configured `token_file` path. Do not use a K8s Secret volume for the
   token file — K8s Secrets are mounted read-only and token refresh write-back
   will fail. See "Token refresh write-back" below.

5. Create the target scope:
   ```bash
   curl -X POST http://localhost:8003/scopes \
     -H "Content-Type: application/json" \
     -d '{"scope_name": "joblogic-kb/gdrive-docs", "description": "Google Drive documents", "owner": "operator@example.com"}'
   ```

6. Configure scope mapping for the Google Drive connector:
   ```bash
   export CONNECTOR_SCOPE_MAPPINGS='[
     {"connector_module": "google_drive", "source_pattern": "gdrive://*", "scope": "joblogic-kb/gdrive-docs"}
   ]'
   ```

### Secret handling requirements

Credentials must come from managed secret storage — **never hardcode them**.

Required config fields:

| Field | Description |
|---|---|
| `client_id` | OAuth2 client ID from Google Cloud Console |
| `client_secret` | OAuth2 client secret |
| `token_file` | Absolute path to the stored OAuth token JSON (contains refresh_token) |

Optional config fields:

| Field | Description |
|---|---|
| `folder_ids` | List of Google Drive folder IDs to sync. Empty = all of My Drive |
| `file_ids` | Specific file IDs to sync. Takes precedence over `folder_ids` if set |
| `scope` | Target knowledge scope (fallback when no scope mapping rule matches) |
| `owner` | Actor identifier for audit log |

**K8s Secret example:**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: gdrive-connector-creds
type: Opaque
stringData:
  client_id: "<GOOGLE_CLIENT_ID>"
  client_secret: "<GOOGLE_CLIENT_SECRET>"
```

### Token refresh write-back

When the stored access token is expired, the connector refreshes it automatically
using the `refresh_token` and immediately writes the new token back to `token_file`
before the sync proceeds. If the write fails, the sync is aborted with a
`RuntimeError` — **fail closed**.

**What this requires operationally:**

- `token_file` must be on a **writable filesystem path** at runtime (for example
  a PVC or a writable `emptyDir`).
- **Do not use a K8s Secret volume for the token file.** K8s Secrets are mounted
  read-only by default; token refresh write-back will always fail with a
  `RuntimeError` if the token file is not writable.
- **Vault write-back is not implemented.** If the initial token originated from
  Vault, copy it to a writable volume before the service starts and point
  `token_file` at that path. Refreshed tokens are persisted only to the local
  writable file — Vault is not updated.

**Recommended setup:**

Mount a PVC at `/var/secrets/gdrive/` and place the initial `token.json` there.
The service can refresh and rewrite the token file freely. Back up the PVC if you
need durability across PVC deletion.

### OAuth scopes required

```
https://www.googleapis.com/auth/drive.readonly
https://www.googleapis.com/auth/drive.metadata.readonly
```

### Supported file types

| Google Drive type | Action | Format ingested |
|---|---|---|
| PDF | Direct download | `pdf` |
| DOCX | Direct download | `docx` |
| HTML | Direct download | `html` |
| Markdown / plain text | Direct download | `markdown` / `txt` |
| Google Docs | Export as DOCX | `docx` |
| Google Sheets | **Skipped** | — (xlsx not supported by normalizer) |
| Google Slides | **Skipped** | — (pptx not supported by normalizer) |

All other MIME types are also skipped (logged as DEBUG).

> **Note:** Google Sheets and Google Slides would export to xlsx and pptx respectively,
> but the ingest normalizer does not support those formats. They are skipped before any
> content download is attempted. Support for xlsx/pptx is a future issue.

### Running a sync

```python
import asyncio
from connectors.google_drive_wrapper import sync_gdrive

config = {
    "client_id": "<from secret store>",
    "client_secret": "<from secret store>",
    "token_file": "/var/secrets/gdrive/token.json",
    "folder_ids": ["1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"],  # optional
    "scope": "joblogic-kb/gdrive-docs",  # fallback if CONNECTOR_SCOPE_MAPPINGS not set
    "owner": "operator@example.com",
}

results = asyncio.run(sync_gdrive(config))
for r in results:
    print(r.verdict, r.source_uri)
```

### Identity contract

| Field | Value |
|---|---|
| `connector_module` | `"google_drive"` |
| `source_uri` | `gdrive://{fileId}` — Google Drive stable file identifier |
| `source_revision` | `modifiedTime` ISO 8601 UTC string from the Drive API |
| Rename behaviour | File rename does **not** change `document_id` — the fileId is stable. The new filename appears on the next version row. |
| Move behaviour | Moving a file between folders does **not** change `document_id` — fileId is stable. |
| Unchanged detection | SHA-256 of raw bytes. Same file re-fetched with identical content → `skipped_unchanged`. |
| Incremental sync | Not implemented (W-C2.2). Full pull only. Changes API integration is a future issue. |

### Webhook / push notification support

Not implemented in W-C2.2. The connector uses pull-based sync only. Run
`sync_gdrive()` on a schedule or trigger it manually. Google Drive push notification
integration is a future issue.

---

## 15. Generic Browser-File Connector Trigger (W-C3.2)

**Issue:** knowledge-ingest#46.

Adds a **Generic Connector Ingest** section to the operator console at `GET /ui`.
Use this when you have a document file on disk and want to submit it to any connector
module via `POST /connectors/ingest` — without writing a script or using curl.

The form is connector-agnostic: `connector_module` is a free-text field so any module
name can be used (`aws_s3`, `openrag`, `custom_connector`, etc.).

### When to use this form vs other paths

| Situation | Use |
|---|---|
| File is on the operator's local disk, any connector module | **This form (W-C3.2)** |
| File is in Google Drive, browser-side selection | Google Drive Picker section (W-C3.4) |
| Files are in an S3 bucket, server should pull | AWS S3 Sync section (W-C3.5) |
| Server-side Google Drive pull (scheduled/scripted) | `sync_gdrive(config)` — server-side wrapper (W-C2.2) |
| Server-side S3 pull (scripted) | `sync_s3_bucket(config)` — server-side wrapper (W-C2.1) |

### How the browser flow works

1. Navigate to `GET /ui` → expand the **Generic Connector Ingest** section.
2. Fill in:
   - `connector_module` — the connector identifier (e.g. `aws_s3`)
   - `scope` — target knowledge scope (must be registered via `POST /scopes`)
   - `source_uri` — stable logical locator for this document (e.g. `s3://bucket/path.pdf`)
   - `format` — select from the dropdown: `pdf`, `docx`, `html`, `markdown`, `txt`
   - `source_revision` (optional) — external revision stamp (ETag, ISO 8601 timestamp, etc.)
   - `owner` (optional) — actor identifier for audit log
3. Click the **File** input and select a local file. The browser reads the file and:
   - Auto-populates the `filename` field from the selected file name.
   - Encodes the file bytes to base64 (`FileReader.readAsDataURL`, prefix stripped).
4. Click **Submit to /connectors/ingest**. The browser POSTs JSON with `raw_bytes_b64`.
5. The `SyncResult` verdict is displayed with colour:
   - `fetched_new` (green) — new document version ingested
   - `fetched_updated` (blue) — existing document updated
   - `skipped_unchanged` (grey) — content unchanged (dedup match)
   - `fetch_failed` (red) — adapter-level failure (see `error_message`)
6. HTTP errors 413 (file > 50 MiB) and 422 (validation failure) display the detail text.

### Identity contract

The operator supplies all identity fields manually. There is no auto-derivation:

| Field | Operator responsibility |
|---|---|
| `connector_module` | Must match the connector identifier expected by scope mappings |
| `source_uri` | Must be the stable logical locator — no commit SHAs, no volatile query strings |
| `source_revision` | Optional; pass the external revision stamp if available |
| `filename` | Auto-populated from file selection; can be edited before submit |
| `format` | Select from dropdown; must match the file content |

### What this form does NOT do

- No connector-specific auth, picker SDK, or OAuth flow.
- No server-side pull — bytes come from the operator's local machine.
- No async polling — the verdict is returned synchronously in the `POST /connectors/ingest` response.
- No direct DB writes — all adapter invariants (scope validation, dedup, provenance) are enforced server-side.

---

## 16. Google Drive Browser Picker (W-C3.4)

**Issue:** knowledge-ingest#48.

This feature adds a **browser-side** Google Drive picker to the operator console at `GET /ui`.
It is a completely separate path from the server-side `sync_gdrive()` wrapper (W-C2.2/#38).
No OAuth tokens are sent to or stored on the server — the browser obtains a session-scoped
access token directly from Google and fetches the file bytes client-side before submitting
them to `POST /connectors/ingest`.

### Required environment variables

Set these on the `agentopia-knowledge-ingest` service before starting:

| Variable | Description |
|---|---|
| `GOOGLE_PICKER_API_KEY` | Browser-restricted API key with Google Picker API enabled (Google Cloud Console → APIs & Services → Credentials) |
| `GOOGLE_PICKER_CLIENT_ID` | OAuth 2.0 client ID (same project, `drive.readonly` scope, authorized JavaScript origins must include the console URL) |
| `GOOGLE_PICKER_APP_ID` | Google Cloud project number — required for Shared Drive visibility (optional for My Drive only) |

If `GOOGLE_PICKER_API_KEY` or `GOOGLE_PICKER_CLIENT_ID` are not set, the picker button is
disabled and a configuration warning is displayed in the console. No picker functionality
is available until both are set.

### How the browser flow works

1. Operator opens `GET /ui` → navigates to the **Google Drive Ingest** section.
2. Clicks **Open Google Drive Picker** — this triggers the GIS implicit grant flow,
   requesting a session-scoped `access_token` with `drive.readonly` scope.
   The access token is never sent to or logged by the server.
3. The Google Picker dialog opens. The operator selects a file.
4. **Rejection at selection:** Google Sheets and Google Slides are rejected with a
   clear message. Selecting them does not enable the submit button.
5. On selection of a supported file, the UI auto-populates:
   - `source_uri = gdrive://{fileId}` — stable across renames and folder moves
   - `filename` — display name from Drive API metadata
   - `format` — inferred from MIME type (PDF, DOCX, HTML, Markdown, plain text;
     Google Docs exported as DOCX)
6. Operator enters the target **scope** and optional **owner**, then clicks
   **Submit to /connectors/ingest**.
7. The browser fetches file bytes from the Drive API (direct download for native
   formats; export endpoint for Google Docs → DOCX).
8. Bytes are base64-encoded in-browser and POSTed to `POST /connectors/ingest`
   as `raw_bytes_b64`, with `connector_module = "google_drive"`.
9. Response verdicts are displayed with colour coding:
   `fetched_new` (green), `fetched_updated` (blue),
   `skipped_unchanged` (grey), `fetch_failed` (red).
10. Error codes 413 (file too large, limit 50 MiB) and 422 (validation error)
    are shown with detail text.

### Supported and rejected file types

| Google Drive type | Browser action | Format submitted |
|---|---|---|
| PDF | Direct download | `pdf` |
| DOCX (Office) | Direct download | `docx` |
| HTML | Direct download | `html` |
| Markdown / plain text | Direct download | `markdown` / `txt` |
| Google Docs | Export → DOCX | `docx` |
| **Google Sheets** | **Rejected at selection** | — |
| **Google Slides** | **Rejected at selection** | — |

Google Sheets and Google Slides are rejected because the ingest normalizer does not
support XLSX or PPTX formats. Rejection happens at picker selection time — no content
is downloaded for these types.

### Identity contract (aligned with W-C2.2/#38)

| Field | Value |
|---|---|
| `connector_module` | `"google_drive"` |
| `source_uri` | `gdrive://{fileId}` — stable across renames |
| `source_revision` | Not submitted via the browser picker path. The Google Picker API response does not include `modifiedTime`. The browser does not send `source_revision` in the `POST /connectors/ingest` payload. The adapter stores it as `NULL` — no auto-assignment occurs. If `modifiedTime` provenance is required, it must be fetched separately from the Drive Files API and submitted explicitly by the caller. |

### Difference from W-C2.2 (server-side sync wrapper)

| Dimension | W-C3.4 browser picker | W-C2.2 server-side wrapper |
|---|---|---|
| Trigger | Operator selects a file in-browser | Scheduled or manual `sync_gdrive()` call |
| OAuth token | Browser GIS implicit grant (session-scoped, never sent to server) | Refresh token persisted to a PVC/token_file; server refreshes as needed |
| Files | One file per operator action | All files in configured folders |
| Scope entry | Entered manually in the UI | Configured via `CONNECTOR_SCOPE_MAPPINGS` |
| Server-side token storage | None — browser handles tokens entirely | Required — token_file on PVC |

### Google Cloud Console setup (operator pre-requisite)

This is a one-time setup, outside the scope of the picker code:

1. Create or select a Google Cloud project.
2. Enable the **Google Picker API** and **Google Drive API**.
3. Create a **browser-restricted API key** → restrict to Google Picker API → set HTTP referrers to the console origin.
4. Create an **OAuth 2.0 client ID** (Web application type) → add the operator console origin to **Authorized JavaScript origins**.
5. Note the **project number** for `GOOGLE_PICKER_APP_ID` (Shared Drive support).
6. Set the three env vars on the service and restart.

---

## Service URLs

| Service | Default Port | Auth |
|---|---|---|
| Document Ingest Service | 8003 | None (internal network only) |
| Super RAG | 8002 | `X-Internal-Token` for write/debug; bearer token for bot queries |
| Operator Console | 8003/ui | Browser, no auth |
