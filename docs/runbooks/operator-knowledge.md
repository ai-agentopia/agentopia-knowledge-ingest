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

## 10. Common Operations Quick Reference

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

---

## Service URLs

| Service | Default Port | Auth |
|---|---|---|
| Document Ingest Service | 8003 | None (internal network only) |
| Super RAG | 8002 | `X-Internal-Token` for write/debug; bearer token for bot queries |
| Operator Console | 8003/ui | Browser, no auth |
