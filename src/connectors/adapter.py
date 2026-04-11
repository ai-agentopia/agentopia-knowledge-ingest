"""Connector adapter — in-process integration point for external source connectors.

W-C1.7: ingest_from_connector(ConnectorEvent) in-process handoff.

Architecture
------------
External connector modules (OpenRAG, Confluence, gdrive, etc.) call
ingest_from_connector() with a ConnectorEvent. The adapter handles the full
sync-to-ingest handoff:

  1. Create connector_sync_task (queued)
  2. Transition to fetching
  3. Compute document_id via stable_document_id_connector
  4. Dedup check: compare event.source_hash against the active version's hash
       → skipped_unchanged if hash matches (sync task finalized, no document write)
  5. fetched_new (first time) or fetched_updated (content changed):
       → create_document() → create_job() → orchestrator.run_pipeline()
  6. set_verdict() is called BEFORE orchestrator — sync state is final once
     fetched_new/fetched_updated is determined (W-C1.8 handoff rule).

W-C1.8: sync verdict independence
----------------------------------
The sync verdict (fetched_new, fetched_updated, skipped_unchanged, fetch_failed)
is finalized on the connector_sync_task BEFORE the orchestrator runs. If the
orchestrator subsequently fails, the document row enters 'failed' status but
the sync task verdict is NOT revised. The operator sees:
  - connector_sync_tasks.verdict = 'fetched_new' (fetch succeeded)
  - documents.status = 'failed' (ingest failed)
  - ingest_jobs.status = 'failed' (with error_message)
A new sync event (re-fetch) creates a new sync task.

W-C1.10: dedup via source_hash
--------------------------------
source_hash is SHA-256 of the raw file bytes from the connector. The adapter
looks up the active version's source_hash from the documents table. If it
matches, the sync is skipped_unchanged — no document row is written, no ingest
job is created.

Identity
--------
document_id = stable_document_id_connector(scope, connector_module, source_uri)
source_uri must be the stable logical locator (no commit SHAs — validated by
stable_document_id_connector). source_revision carries the exact external
revision stamp (e.g. git SHA, Confluence version, S3 ETag) as immutable
provenance per version row.
"""

import hashlib
import logging
import os
from dataclasses import dataclass, field

from connectors.scope_mapping import resolve_scope
from db import registry
from db.connector_sync import create_sync_task, get_task, mark_fetching, set_verdict
from orchestrator import run_pipeline

logger = logging.getLogger(__name__)


@dataclass
class ConnectorEvent:
    """Represents a document event emitted by an external connector module.

    Attributes:
        connector_module:   Identifier of the connector (e.g. "openrag", "confluence").
        scope:              Target knowledge scope (must be registered).
        source_uri:         Stable logical locator for the source document.
                            Must NOT contain volatile revision selectors (commit SHAs).
        filename:           Display filename for operator UI (e.g. "api-reference.pdf").
        format:             Document format: "pdf" | "docx" | "html" | "markdown" | "txt".
        raw_bytes:          Raw file content bytes.
        source_revision:    Exact external revision stamp at time of fetch
                            (e.g. git commit SHA, Confluence version ID, S3 ETag).
                            Stored as immutable provenance on the document row.
                            May be None if the connector does not track revisions.
        owner:              Optional owner/actor identifier for audit log.
        metadata:           Optional extra key/value data passed through to document metadata.
    """

    connector_module: str
    scope: str
    source_uri: str
    filename: str
    format: str
    raw_bytes: bytes
    source_revision: str | None = None
    owner: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class SyncResult:
    """Result returned by ingest_from_connector().

    Attributes:
        task_id:    connector_sync_task UUID.
        verdict:    fetched_new | fetched_updated | skipped_unchanged | fetch_failed
        document_id: Set for fetched_new / fetched_updated. None for skipped/failed.
        job_id:     Set when an ingest job was created. None otherwise.
        version:    Document version number. None for skipped/failed.
        error_message: Set on fetch_failed verdict.
    """

    task_id: str
    verdict: str
    document_id: str | None = None
    job_id: str | None = None
    version: int | None = None
    error_message: str | None = None
    source_uri: str = ""


def ingest_from_connector(event: ConnectorEvent) -> SyncResult:
    """Process a connector document event through the sync → ingest pipeline.

    This is the single entry point for all connector-originated documents.
    It is in-process: no HTTP calls, no queue. The orchestrator runs synchronously.

    Returns a SyncResult with the verdict and identifiers.
    """
    connector_module = event.connector_module
    source_uri = event.source_uri

    # ── Step 0: Resolve scope via scope mapping (W-C1.9) ─────────────────────
    # resolve_scope() checks CONNECTOR_SCOPE_MAPPINGS for a matching rule.
    # Falls back to event.scope when no mapping matches (e.g. scope supplied
    # explicitly by callers in tests or CLI tools).
    resolved = resolve_scope(connector_module, source_uri)
    scope = resolved if resolved is not None else event.scope

    if not scope:
        # Neither mapping nor event provided a scope — cannot proceed.
        logger.error(
            "connector_adapter: no scope resolved for connector=%s uri=%s "
            "— set CONNECTOR_SCOPE_MAPPINGS or provide event.scope",
            connector_module, source_uri,
        )
        # Create a minimal task record so the failure is visible in audit
        task_id = create_sync_task(
            connector_module=connector_module,
            scope="__unresolved__",
            source_uri=source_uri,
        )
        mark_fetching(task_id)
        set_verdict(task_id, verdict="fetch_failed",
                    error_message="No scope could be resolved for this connector event")
        return SyncResult(
            task_id=task_id,
            verdict="fetch_failed",
            error_message="No scope could be resolved for this connector event",
            source_uri=source_uri,
        )

    logger.debug(
        "connector_adapter: scope resolved connector=%s uri=%s scope=%s (via_mapping=%s)",
        connector_module, source_uri, scope, resolved is not None,
    )

    # ── Step 0b: Validate scope is registered ─────────────────────────────────
    # The resolved scope must exist in the scope registry. An unregistered scope
    # would make all document rows and chunk payloads land in a phantom scope that
    # Super RAG has no record of. Fail early with a clear operator-actionable error.
    if not registry.scope_exists(scope):
        error_msg = (
            f"Scope '{scope}' is not registered in the knowledge registry. "
            f"Create the scope via POST /scopes before connector ingestion proceeds."
        )
        logger.error(
            "connector_adapter: fetch_failed — unregistered scope=%s connector=%s uri=%s",
            scope, connector_module, source_uri,
        )
        task_id = create_sync_task(
            connector_module=connector_module,
            scope=scope,
            source_uri=source_uri,
        )
        mark_fetching(task_id)
        set_verdict(task_id, verdict="fetch_failed", error_message=error_msg)
        return SyncResult(
            task_id=task_id,
            verdict="fetch_failed",
            error_message=error_msg,
            source_uri=source_uri,
        )

    # ── Step 1: Create sync task (queued) ─────────────────────────────────────
    task_id = create_sync_task(
        connector_module=connector_module,
        scope=scope,
        source_uri=source_uri,
    )
    logger.info(
        "connector_adapter: task=%s connector=%s scope=%s uri=%s",
        task_id, connector_module, scope, source_uri,
    )

    # ── Step 2: Transition to fetching ────────────────────────────────────────
    mark_fetching(task_id)

    # ── Step 3: Compute source_hash and document_id ───────────────────────────
    try:
        source_hash = hashlib.sha256(event.raw_bytes).hexdigest()
        doc_id = registry.stable_document_id_connector(scope, connector_module, source_uri)
    except ValueError as exc:
        # source_uri validation failed (e.g. contains commit SHA)
        _finalize_failed(task_id, str(exc), event)
        return SyncResult(
            task_id=task_id,
            verdict="fetch_failed",
            error_message=str(exc),
            source_uri=source_uri,
        )

    # ── Step 4: Dedup check — compare against active version ─────────────────
    active = registry.get_active_version(doc_id)
    if active is not None and active["source_hash"] == source_hash:
        # Content unchanged — finalize sync task, do NOT write document row
        set_verdict(
            task_id,
            verdict="skipped_unchanged",
            observed_source_revision=event.source_revision,
            source_hash_observed=source_hash,
            document_id=doc_id,
            # resulting_row_id is NULL — no new document row was written
        )
        logger.info(
            "connector_adapter: task=%s verdict=skipped_unchanged doc=%s scope=%s",
            task_id, doc_id, scope,
        )
        return SyncResult(
            task_id=task_id,
            verdict="skipped_unchanged",
            document_id=doc_id,
            source_uri=source_uri,
        )

    # ── Step 5: Determine verdict (new vs updated) ────────────────────────────
    verdict = "fetched_new" if active is None else "fetched_updated"

    # ── Step 6: Create document row ───────────────────────────────────────────
    try:
        doc_row = registry.create_document(
            scope=scope,
            filename=event.filename,
            format=event.format,
            source_hash=source_hash,
            s3_original_key="",  # Will be patched by storage write below
            owner=event.owner,
            connector_module=connector_module,
            source_uri=source_uri,
            source_revision=event.source_revision,  # IMMUTABLE — set once here
        )
    except Exception as exc:
        _finalize_failed(task_id, f"Failed to create document row: {exc}", event)
        return SyncResult(
            task_id=task_id,
            verdict="fetch_failed",
            error_message=str(exc),
            source_uri=source_uri,
        )

    row_id = doc_row["row_id"]
    document_id = doc_row["document_id"]
    version = doc_row["version"]

    # ── Step 7: Finalize sync verdict BEFORE orchestrator runs ────────────────
    # W-C1.8: sync verdict is independent of orchestrator outcome.
    # The verdict is written here. If orchestrator fails, the sync task
    # remains at verdict=fetched_new/fetched_updated — only the document row
    # and ingest_job will reflect the failure.
    try:
        job_id = registry.create_job(row_id)
    except Exception as exc:
        _finalize_failed(task_id, f"Failed to create ingest job: {exc}", event)
        registry.update_document_status(row_id, "failed", error_message=str(exc))
        return SyncResult(
            task_id=task_id,
            verdict="fetch_failed",
            error_message=str(exc),
            source_uri=source_uri,
        )

    set_verdict(
        task_id,
        verdict=verdict,
        observed_source_revision=event.source_revision,
        source_hash_observed=source_hash,
        document_id=document_id,
        resulting_row_id=row_id,
        job_id=job_id,
    )
    logger.info(
        "connector_adapter: task=%s verdict=%s doc=%s v%d job=%s scope=%s",
        task_id, verdict, document_id, version, job_id, scope,
    )

    # ── Step 8: Run ingest pipeline (synchronous) ─────────────────────────────
    # Sync verdict is already finalized above. Orchestrator failure only affects
    # the document row and ingest_job, not the sync task.
    try:
        run_pipeline(
            job_id=job_id,
            row_id=row_id,
            document_id=document_id,
            version=version,
            scope=scope,
            filename=event.filename,
            raw_bytes=event.raw_bytes,
            s3_original_key="",
            actor=event.owner or connector_module,
        )
    except Exception as exc:
        # Orchestrator failure does NOT change the sync verdict — it is already set.
        # The document row and job will reflect the failure via _fail() inside run_pipeline.
        logger.error(
            "connector_adapter: task=%s orchestrator raised unexpectedly doc=%s: %s",
            task_id, document_id, exc,
        )
        # Ensure document and job are in failed state
        try:
            registry.update_document_status(row_id, "failed", error_message=str(exc))
            registry.update_job(job_id, status="failed", error_message=str(exc), completed=True)
        except Exception:
            pass

    return SyncResult(
        task_id=task_id,
        verdict=verdict,
        document_id=document_id,
        job_id=job_id,
        version=version,
        source_uri=source_uri,
    )


def _finalize_failed(task_id: str, error_message: str, event: ConnectorEvent) -> None:
    """Set fetch_failed verdict on a sync task."""
    try:
        set_verdict(
            task_id,
            verdict="fetch_failed",
            observed_source_revision=event.source_revision,
            error_message=error_message,
        )
    except Exception as exc:
        logger.error(
            "connector_adapter: failed to write fetch_failed verdict task=%s: %s",
            task_id, exc,
        )
    logger.warning(
        "connector_adapter: task=%s verdict=fetch_failed connector=%s uri=%s: %s",
        task_id, event.connector_module, event.source_uri, error_message,
    )
