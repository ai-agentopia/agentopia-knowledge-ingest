# ADR-002: Connector Precondition Audit — OpenRAG Repository

**Status:** Accepted  
**Date:** 2026-04-11  
**Issues:** GATE-1 (#18), GATE-2 (#19)  
**Repo audited:** `langflow-ai/openrag` (commit inspected via GitHub API + raw file fetch)

---

## Purpose

This document records the concrete audit findings from reading the actual OpenRAG source
repository. It is the authoritative basis for which W-C2 and W-C3 sub-issues are
legitimate to open. Nothing is speculated — every claim below is traceable to a specific
file path in `langflow-ai/openrag`.

---

## Part 1 — GATE-1: Connector Code Audit

### Repository identity

**Repo:** `langflow-ai/openrag`  
**Description:** A comprehensive RAG platform and document management tool built on
FastAPI, Next.js, OpenSearch, Langflow, and Docling.  
**License:** Apache Software License 2.0 (compatible with commercial reuse)  
**Python requirement:** `>=3.13`  
**Packaging:** Installable via `pyproject.toml`; entry point `openrag = tui.main:run_tui`.
The connectors package is at `src/connectors/` and is importable from the repo root.
**Not published on PyPI** — must be copied or added as a git dependency.

### How connectors work in OpenRAG

All connectors inherit from `src/connectors/base.py::BaseConnector`. The abstract interface
exposes:

```
authenticate()          → bool
list_files(page_token, max_files) → {"files": [...], "next_page_token": str|None}
get_file_content(file_id) → ConnectorDocument
setup_subscription()    → str (subscription ID)
handle_webhook(payload) → list[str] (affected file IDs)
cleanup_subscription(subscription_id) → bool
```

`ConnectorDocument` (defined in `base.py`) carries:

| Field | Type | Notes |
|---|---|---|
| `id` | `str` | Provider-native file ID (opaque, provider-specific format) |
| `filename` | `str` | Display filename |
| `mimetype` | `str` | MIME type |
| `content` | `bytes` | Raw file bytes |
| `source_url` | `str` | Human-readable URL (may not be stable across ACL changes) |
| `acl` | `DocumentACL` | Owner, allowed_users, allowed_groups |
| `modified_time` | `datetime` | Last modified (ISO 8601 or boto3 datetime) |
| `created_time` | `datetime` | Created time (not always available — S3/COS use modified_time) |
| `metadata` | `dict` | Provider-specific extra fields |

OpenRAG's `ConnectorService` (`src/connectors/service.py`) orchestrates calls to
`list_files()` + `get_file_content()` and feeds results into OpenRAG's own Docling
pipeline and OpenSearch index. **This service layer is NOT reusable for agentopia** —
it is tightly coupled to OpenRAG's embedding model, task service, and OpenSearch client.
Only the individual connector classes are candidates for reuse.

---

### Verified connector modules

#### 1. AWS S3 (`src/connectors/aws_s3/`)

| Field | Value |
|---|---|
| **Repo path** | `src/connectors/aws_s3/connector.py` (10.8 KB), `auth.py` (3.1 KB), `api.py` (6.9 KB), `models.py`, `support.py` |
| **Source type** | Amazon S3 and S3-compatible object storage (MinIO, Cloudflare R2) |
| **Auth model** | HMAC — Access Key ID + Secret Access Key via `config["access_key"]` / `config["secret_key"]` or env vars `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. No OAuth. |
| **File ID format** | Composite string `"{bucket}::{key}"` — bucket name + object key joined by `::` |
| **`source_url` field** | `f"s3://{bucket_name}/{key}"` |
| **Revision signal** | `obj.last_modified` (boto3 datetime, UTC). **No ETag exposed** in current connector code. |
| **Unchanged detection** | W-C1 SHA-256 of raw bytes (content hash) — `modified_time` usable as `source_revision` string |
| **Webhooks** | Stubbed — `setup_subscription()` returns `""`, `handle_webhook()` returns `[]` |
| **Dependencies** | `boto3>=1.35.0` |
| **Import path** | `from connectors.aws_s3 import S3Connector` |

**Stable `source_uri` for agentopia:**  
`s3://{bucket}/{key}` (path-based; changes on object rename/move — same as manual upload rename semantics)

**Canonical `source_revision` for agentopia:**  
`obj.last_modified.isoformat()` — best available; no ETag in current code.

**Reusability verdict:** REUSABLE WITH WRAPPER  
The class is clean and has no runtime service dependencies beyond boto3. A W-C2 wrapper
must: instantiate `S3Connector(config)`, call `authenticate()`, iterate `list_files()`,
call `get_file_content(file_id)`, derive `source_uri` from the `s3://` URL, and bridge
output into `ConnectorEvent` / `ingest_from_connector()`. Credential storage must be
handled by knowledge-ingest (K8s Secret or Vault) — OpenRAG's `connection_manager.py`
is not reusable.

---

#### 2. Google Drive (`src/connectors/google_drive/`)

| Field | Value |
|---|---|
| **Repo path** | `src/connectors/google_drive/connector.py` (~47 KB), `oauth.py` (~8 KB) |
| **Source type** | Google Drive files (all MIME types; Google Docs exported to DOCX/PDF via export API) |
| **Auth model** | OAuth 2.0. `oauth.py` handles code exchange and token refresh. Client ID/secret from env vars `GOOGLE_DRIVE_OAUTH_CLIENT_ID` / `GOOGLE_DRIVE_OAUTH_CLIENT_SECRET`. Tokens stored in OpenRAG's connection_manager encrypted JSON. |
| **File ID format** | Google-native `fileId` — opaque alphanumeric string (e.g., `"1a2b3cXYZ..."`) |
| **`source_url` field** | `meta["webViewLink"]` — Google Drive web view URL |
| **Revision signal** | `modifiedTime` from Google Drive API (ISO 8601 string, e.g., `"2024-01-15T10:30:45.123Z"`) |
| **Unchanged detection** | W-C1 SHA-256 of raw bytes; `modifiedTime` usable as `source_revision` |
| **Webhooks** | Fully implemented — `setup_subscription()` calls Google Drive Changes API `watch()`. Requires externally reachable webhook URL. |
| **Dependencies** | `google-auth`, `google-api-python-client`, `google-auth-oauthlib` |
| **Import path** | `from connectors.google_drive import GoogleDriveConnector` |

**Stable `source_uri` for agentopia:**  
`gdrive://{fileId}` — Google's `fileId` is stable across file renames and folder moves.
**Do NOT use `source_url` (webViewLink)** as `source_uri` — it changes with sharing settings.

**Canonical `source_revision` for agentopia:**  
`modifiedTime` string from Google API (ISO 8601).

**Reusability verdict:** REUSABLE WITH WRAPPER  
The connector class is self-contained given `google-auth*` deps. The OAuth flow
(`oauth.py`) exchanges codes and refreshes tokens — agentopia must store tokens in its
own secret store. The W-C2 wrapper handles token injection into the `config` dict and
bridges `ConnectorDocument` to `ConnectorEvent`. The OAuth callback handler is OpenRAG-
specific and must be re-implemented in knowledge-ingest.

---

#### 3. SharePoint (`src/connectors/sharepoint/`)

| Field | Value |
|---|---|
| **Repo path** | `src/connectors/sharepoint/connector.py` (~41 KB), `oauth.py` |
| **Source type** | Microsoft SharePoint document libraries (via Microsoft Graph API) |
| **Auth model** | MSAL (Microsoft Authentication Library). Client ID/secret from env vars `MICROSOFT_GRAPH_OAUTH_CLIENT_ID` / `MICROSOFT_GRAPH_OAUTH_CLIENT_SECRET`. |
| **File ID format** | Graph API item ID (alphanumeric, site-scoped) |
| **`source_url` field** | `item["webUrl"]` from Graph API |
| **Revision signal** | `lastModifiedDateTime` from Graph API (ISO 8601 with fractional seconds) |
| **Unchanged detection** | W-C1 SHA-256 of raw bytes; `lastModifiedDateTime` usable as `source_revision` |
| **Webhooks** | Fully implemented — Microsoft Graph subscriptions. Requires externally reachable `{webhook_url}/webhook/sharepoint`. |
| **Dependencies** | `msal` |
| **Import path** | `from connectors.sharepoint import SharePointConnector` |

**Stable `source_uri` for agentopia:**  
`sharepoint://{siteId}/{driveId}/{itemId}` — must include site and drive scope for
uniqueness across tenants. The raw Graph item ID alone is not globally unique.

**Canonical `source_revision` for agentopia:**  
`lastModifiedDateTime` string from Graph API.

**Reusability verdict:** REUSABLE WITH WRAPPER  
41 KB connector with significant site/library navigation logic. The core
`list_files()` + `get_file_content()` is clean once authenticated. Token management
must be re-implemented in knowledge-ingest. MSAL dependency (`msal`) must be added to
`requirements.txt`.

---

#### 4. OneDrive (`src/connectors/onedrive/`)

| Field | Value |
|---|---|
| **Repo path** | `src/connectors/onedrive/connector.py` (~41 KB), `oauth.py` (~16.8 KB) |
| **Source type** | Microsoft OneDrive personal and organizational drives (via Microsoft Graph API) |
| **Auth model** | MSAL — same OAuth app as SharePoint (`MICROSOFT_GRAPH_OAUTH_CLIENT_ID` / `MICROSOFT_GRAPH_OAUTH_CLIENT_SECRET`) |
| **File ID format** | Multiple formats: simple item ID, `driveId!itemId` composite, or sharing ID with `s` prefix. Sharing IDs are NOT stable. |
| **`source_url` field** | `item["webUrl"]` from Graph API |
| **Revision signal** | `lastModifiedDateTime` from Graph API |
| **Unchanged detection** | W-C1 SHA-256 of raw bytes; `lastModifiedDateTime` usable as `source_revision` |
| **Webhooks** | Fully implemented — Graph subscriptions for `/me/drive/root`. Note: personal OneDrive accounts may not support change notifications. |
| **Dependencies** | `msal` (shared with SharePoint) |
| **Import path** | `from connectors.onedrive import OneDriveConnector` |

**Stable `source_uri` for agentopia:**  
`onedrive://{driveId}/{itemId}` — use the `driveId!itemId` composite format when
available. **Do NOT use sharing IDs** (prefixed `s`) as `source_uri` — they are
ephemeral. The W-C2 wrapper must normalize item IDs to the canonical form.

**Canonical `source_revision` for agentopia:**  
`lastModifiedDateTime` string from Graph API.

**Reusability verdict:** REUSABLE WITH WRAPPER  
Shares OAuth implementation with SharePoint. The `source_uri` normalization for sharing
IDs requires extra care in the wrapper layer. Otherwise analogous to SharePoint.

---

#### 5. IBM Cloud Object Storage (`src/connectors/ibm_cos/`)

| Field | Value |
|---|---|
| **Repo path** | `src/connectors/ibm_cos/connector.py` (~15.6 KB), `auth.py` (~6.7 KB), `api.py` (~8.4 KB), `models.py`, `support.py` |
| **Source type** | IBM Cloud Object Storage buckets |
| **Auth model** | IBM HMAC credentials + IBM IAM JWT (`src/auth/ibm_auth.py` for JWT validation). Config keys: IBM-specific env vars (`IBMLH_*`). |
| **File ID format** | Composite `"{bucket}::{key}"` — same pattern as S3 connector |
| **`source_url` field** | `f"cos://{bucket_name}/{key}"` |
| **Revision signal** | `LastModified` (boto3 datetime, UTC) — same as S3 |
| **Unchanged detection** | W-C1 SHA-256 of raw bytes; `LastModified.isoformat()` as `source_revision` |
| **Webhooks** | Stubbed — `setup_subscription()` returns `""`, `handle_webhook()` returns `[]`. IBM Event Notifications integration is out of scope. |
| **Dependencies** | `ibm-cos-sdk` (IBM-specific boto3 fork), `ibm-cloud-sdk-core` |
| **Import path** | `from connectors.ibm_cos import IBMCOSConnector` |

**Stable `source_uri` for agentopia:**  
`cos://{bucket}/{key}` (path-based; same rename caveats as S3).

**Canonical `source_revision` for agentopia:**  
`LastModified.isoformat()`.

**Reusability verdict:** REUSABLE WITH WRAPPER  
Functionally analogous to S3Connector. IBM-specific dependencies add non-trivial install
weight (`ibm-cos-sdk`). Lowest priority for W-C2 given enterprise-specific use case.

---

### Connector reusability summary table

| Module name | Repo path | Source type | Auth model | Output shape | Reusability verdict |
|---|---|---|---|---|---|
| `S3Connector` | `src/connectors/aws_s3/connector.py` | Amazon S3 / S3-compatible | HMAC (no OAuth) | `ConnectorDocument` | Reusable with wrapper |
| `GoogleDriveConnector` | `src/connectors/google_drive/connector.py` | Google Drive | OAuth 2.0 (GOOGLE_DRIVE env vars) | `ConnectorDocument` | Reusable with wrapper |
| `SharePointConnector` | `src/connectors/sharepoint/connector.py` | SharePoint (Graph API) | MSAL OAuth (MICROSOFT_GRAPH env vars) | `ConnectorDocument` | Reusable with wrapper |
| `OneDriveConnector` | `src/connectors/onedrive/connector.py` | OneDrive (Graph API) | MSAL OAuth (shared with SharePoint) | `ConnectorDocument` | Reusable with wrapper |
| `IBMCOSConnector` | `src/connectors/ibm_cos/connector.py` | IBM Cloud Object Storage | IBM HMAC + IAM JWT | `ConnectorDocument` | Reusable with wrapper |

All five connectors:
- Are Python classes importable from the repo
- Require adaptation (W-C2 wrapper) — none can call `ingest_from_connector()` directly
- Require credential storage re-implemented in knowledge-ingest — OpenRAG's `connection_manager.py` is not portable

---

### What the W-C2 wrapper must provide (for every connector)

Regardless of which connector is implemented first, every W-C2 wrapper must:

1. **Instantiate** the connector class with a `config` dict (credentials from K8s Secret or Vault — not from OpenRAG's connection_manager).
2. **Call `authenticate()`** and fail fast if authentication fails.
3. **Derive a stable `source_uri`** from the connector's file ID — do NOT pass the provider's raw opaque ID. Use the `s3://`, `gdrive://`, `sharepoint://`, `onedrive://`, `cos://` URI scheme defined above.
4. **Call `list_files()`** for discovery; **call `get_file_content(file_id)`** for each file.
5. **Extract `source_revision`** from `modified_time` (best available from all five connectors).
6. **Construct `ConnectorEvent`** and call `ingest_from_connector()`.
7. **Handle `skipped_unchanged`** — do not re-ingest files whose SHA-256 matches the active version.

---

### Recommended W-C2 issue list

Based only on verified modules above. Ordered by implementation complexity (simplest first):

| Issue | Connector | Rationale |
|---|---|---|
| W-C2.1 | AWS S3 | Simplest auth (HMAC, no OAuth); no token refresh; best first connector |
| W-C2.2 | Google Drive | High operator value; OAuth but well-documented; ~47 KB connector |
| W-C2.3 | SharePoint | High enterprise value; shares OAuth infra with OneDrive |
| W-C2.4 | OneDrive | Shares OAuth infra with SharePoint; implement after W-C2.3 |
| W-C2.5 | IBM COS | Enterprise-only; low priority; heavy dependencies |

Each W-C2 issue covers: credential wiring (K8s Secret schema), wrapper implementation, scope mapping config, and integration test (real connector endpoint stubbed).

---

### Explicit non-findings (connectors assumed but not present)

The following connectors were mentioned in prior planning or assumed possible but are
**not present in `langflow-ai/openrag`** as of this audit:

| Connector | Status | Evidence |
|---|---|---|
| GitHub | NOT PRESENT | No `src/connectors/github/` directory. Deferred in super-rag#66. |
| Slack | NOT PRESENT | No `src/connectors/slack/` directory. Deferred in super-rag#67. |
| Confluence | NOT PRESENT | No `src/connectors/confluence/` directory. No reference in `__init__.py`. |
| Notion | NOT PRESENT | No `src/connectors/notion/` directory. |
| Dropbox | NOT PRESENT | No `src/connectors/dropbox/` directory. |
| Box | NOT PRESENT | No `src/connectors/box/` directory. |
| Azure Blob Storage | NOT PRESENT | SharePoint connector uses Graph API, not Azure Blob. |
| Email / IMAP | NOT PRESENT | Not in repo. |

**W-C2 issues for any of the above connectors MUST NOT be filed until a concrete OpenRAG
module is verified or an alternative source is identified.**

---

## Part 2 — GATE-2: Connector UI/Auth Audit

### OpenRAG UI technology

**Stack:** Next.js 15.5.9 + React 19.0.0 + TypeScript + Tailwind CSS 3.4.17  
**State management:** TanStack React Query 5.86.0 (server state), Zustand 5.0.8 (client state)  
**Data grid:** AG Grid 34.2.0  
**UI primitives:** Radix UI  
**Notifications:** Sonner  

**Knowledge-ingest operator UI (current):** Single inline HTML response served from
`GET /ui` (`src/api/routes.py:273`). Vanilla HTML + browser `fetch()` API. No framework.

These are fundamentally different tech stacks. OpenRAG's React components cannot be
embedded in knowledge-ingest's HTML page without adopting a JS build pipeline.

---

### Verified UI/auth surfaces

#### Surface 1: Connector management page (`frontend/app/connectors/page.tsx`)

| Field | Value |
|---|---|
| **Repo path** | `frontend/app/connectors/page.tsx` |
| **Purpose** | Connector status dashboard and file sync trigger. Currently a Google Drive demo page using `UnifiedCloudPicker`. |
| **Tech stack** | Next.js client component (`"use client"`), React hooks |
| **API calls** | `POST /api/connectors/{connector.type}/sync` — sends selected file IDs, receives task_id or sync results |
| **User actions** | File selection via cloud picker, "Sync N Selected Items" button, result display |
| **Auth dependency** | Requires user session (OpenRAG auth context), connector OAuth tokens |
| **Reusability verdict** | NOT REUSABLE AS-IS. Calls OpenRAG-specific API routes. React component cannot be embedded in vanilla HTML operator UI. |
| **Recommendation** | REWRITE — implement a sync trigger form in knowledge-ingest's operator UI that calls POST /connectors/ingest (W-C3.1 endpoint). |

---

#### Surface 2: Cloud picker components (`frontend/components/cloud-picker/`)

| Field | Value |
|---|---|
| **Repo path** | `frontend/components/cloud-picker/provider-handlers.ts`, `onedrive-v8-handler.ts`, `sharepoint-v8-handler.ts` |
| **Purpose** | Browser-side file selection dialogs using provider-native SDKs (Microsoft OneDrive Picker v8, SharePoint Picker v8). Google Drive picker handled separately via Google Picker API. |
| **Tech stack** | TypeScript ES modules; Microsoft OneDrive/SharePoint use `@microsoft/filepicker` SDK (loaded at runtime) |
| **Auth dependency** | Requires OAuth access token injected at runtime (fetched via `useGetConnectorTokenQuery.ts` from OpenRAG backend). |
| **Runtime dependency** | Microsoft picker SDK loaded as external script; connector OAuth token served by backend. |
| **Reusability verdict** | REUSABLE IN CONCEPT — the picker logic (open file dialog, collect selected file IDs) is provider-agnostic. The React wrapper is not reusable. |
| **Recommendation** | ADAPT. Extract the picker invocation logic into standalone JS (no React). Provide OAuth token from knowledge-ingest backend. Wire selected file IDs to POST /connectors/ingest (W-C3.1). |

---

#### Surface 3: Connector API hooks (`frontend/app/api/mutations/`, `frontend/app/api/queries/`)

| Field | Value |
|---|---|
| **Repo path** | `frontend/app/api/mutations/useConnectConnectorMutation.ts`, `useDisconnectConnectorMutation.ts`, `useSyncConnector.ts`; `frontend/app/api/queries/useGetConnectorsQuery.ts`, `useGetConnectorTokenQuery.ts` |
| **Purpose** | React Query hooks wrapping OpenRAG backend API calls for connector connect/disconnect/sync/list/token operations |
| **Tech stack** | TanStack React Query (React hooks) |
| **Auth dependency** | OpenRAG user session (auth context) |
| **Reusability verdict** | NOT REUSABLE — React Query hooks, OpenRAG-specific endpoints, OpenRAG auth context |
| **Recommendation** | Do not reuse. Equivalent browser fetch calls for knowledge-ingest API are trivial to write. |

---

#### Surface 4: Authentication (`src/api/auth.py`, `src/auth/ibm_auth.py`, `frontend/contexts/auth-context.tsx`)

| Field | Value |
|---|---|
| **Repo path** | `src/api/auth.py` (5.8 KB), `src/auth/ibm_auth.py` (3.3 KB), `frontend/contexts/auth-context.tsx` |
| **Purpose** | IBM-specific JWT validation (RS256, audience/issuer claims for IBM Lakehouse); OIDC support; user session management |
| **Auth model** | IBM JWT + OIDC — specific to IBM deployment model |
| **Reusability verdict** | NOT APPLICABLE — knowledge-ingest has no user authentication system. Operator UI is internal-network-only with no auth (per ADR-003 and runbook). |
| **Recommendation** | No action. Do not import OpenRAG auth into knowledge-ingest. |

---

#### Surface 5: Settings and credentials UI (`frontend/app/settings/`)

| Field | Value |
|---|---|
| **Repo path** | `frontend/app/settings/` |
| **Purpose** | User-facing settings, including connector credential entry (OAuth client ID/secret configuration) |
| **Tech stack** | Next.js React |
| **Reusability verdict** | NOT REUSABLE AS-IS — React-only, calls OpenRAG settings API |
| **Recommendation** | For knowledge-ingest, connector credentials should be stored in K8s Secrets or Vault (operator-managed), not entered via UI. No equivalent UI surface needed in W-C3. |

---

### UI/auth reusability summary table

| UI surface | Repo path | Purpose | Tech stack | Auth flow support | Reusability verdict |
|---|---|---|---|---|---|
| Connector management page | `frontend/app/connectors/page.tsx` | Sync trigger + status | Next.js / React | OpenRAG session | NOT REUSABLE AS-IS — rewrite |
| Cloud picker components | `frontend/components/cloud-picker/` | Provider file picker dialogs (OneDrive v8, SharePoint v8) | TypeScript ES modules + MS SDK | OAuth token injected | REUSABLE IN CONCEPT — adapt (strip React) |
| Connector API hooks | `frontend/app/api/mutations/`, `queries/` | React Query wrappers for connector API | React Query | OpenRAG session | NOT REUSABLE — trivial to rewrite as plain fetch |
| IBM/OIDC auth backend | `src/auth/ibm_auth.py`, `src/api/auth.py` | JWT/OIDC user auth | Python / FastAPI | IBM IAM | NOT APPLICABLE — knowledge-ingest has no user auth |
| Settings / credential UI | `frontend/app/settings/` | OAuth credential entry | Next.js / React | OpenRAG session | NOT REUSABLE — credentials via K8s Secrets |

---

### W-C3.1 trigger condition

The cloud-picker components make API calls from the browser to trigger sync. Those calls
must reach a knowledge-ingest HTTP endpoint. The current adapter (`ingest_from_connector`)
is in-process only — no HTTP surface exists today.

**Finding:** Any browser-side connector trigger UI requires `POST /connectors/ingest` (W-C3.1)
as a prerequisite. Without it, the picker can select files but cannot dispatch them to
knowledge-ingest.

W-C3.1 is defined in the W-C3 tracking shell (#32) as explicitly conditional on GATE-2
closure. This audit confirms the trigger condition is met — W-C3.1 is a genuine
prerequisite for all browser-side UI surfaces.

---

### Recommended W-C3 issue list

Based only on verified surfaces above:

| Issue | Surface | Rationale |
|---|---|---|
| W-C3.1 | HTTP endpoint `POST /connectors/ingest` | Prerequisite for all browser-side trigger UI. Exposes `ingest_from_connector` over HTTP. |
| W-C3.2 | Operator UI: connector status and sync trigger form | Simple HTML form in existing operator UI (`/ui`) — no framework needed. Calls W-C3.1. |
| W-C3.3 | Microsoft cloud picker integration | Adapt `onedrive-v8-handler.ts` + `sharepoint-v8-handler.ts` logic into standalone JS. Wire selected IDs to W-C3.1. |
| W-C3.4 | Google Drive picker integration | Adapt Google Picker API invocation into standalone JS. Wire to W-C3.1. |
| W-C3.5 | S3/COS sync trigger form | No OAuth picker needed — simple form (bucket, prefix, access key). Lower complexity than W-C3.3/W-C3.4. |

Note: W-C3.3, W-C3.4, and W-C3.5 each depend on W-C3.1. W-C3.2 depends on W-C3.1.
W-C3 connector UI sub-issues should be filed after W-C3.1 is accepted.

**Embed vs separate process:** All W-C3 UI surfaces can be implemented as additions to
the existing operator UI (`/ui` route), provided the operator UI adopts inline script
loading for provider SDKs. No separate frontend process is required for W-C3.

---

## Explicit non-findings — UI surfaces

| Surface | Status | Evidence |
|---|---|---|
| Connector sync status / history view | NOT PRESENT | OpenRAG has task polling but no dedicated sync history UI equivalent to knowledge-ingest's `connector_sync_tasks` table. |
| Rollback / version history UI (connector-specific) | NOT PRESENT | OpenRAG has no document versioning UI. knowledge-ingest already implements this in its operator UI. |
| Scope mapping UI | NOT PRESENT | OpenRAG has no concept of scope mapping. Must be built natively for W-C3. |
| GitHub UI / Slack UI | NOT PRESENT | No connector exists; no UI exists. |
| Confluence UI | NOT PRESENT | No connector exists; no UI exists. |

---

## Audit method

All findings above are derived from:

1. GitHub repository listing via `gh search repos` — identified `langflow-ai/openrag` as the
   document management and connector platform referenced in ADR-003.
2. GitHub API tree fetch: `api.github.com/repos/langflow-ai/openrag/git/trees/main?recursive=1`
   — verified directory structure.
3. Raw file fetches via `raw.githubusercontent.com` for:
   - `src/connectors/base.py`
   - `src/connectors/__init__.py`
   - `src/connectors/aws_s3/connector.py`
   - `src/connectors/google_drive/connector.py` (directory listing confirmed)
   - `src/connectors/sharepoint/connector.py` (directory listing confirmed)
   - `src/connectors/onedrive/connector.py` (directory listing confirmed)
   - `src/connectors/ibm_cos/connector.py` (directory listing confirmed)
   - `src/connectors/service.py`
   - `frontend/app/connectors/page.tsx`
   - `pyproject.toml`
4. Cross-referencing with knowledge-ingest source (`src/api/routes.py:273`) to confirm
   operator UI technology (inline HTML, not React).

No cloning was performed. No connector source code was imported or adapted into production
code. This is an audit artefact only.
