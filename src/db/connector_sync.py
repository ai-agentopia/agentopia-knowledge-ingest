"""Connector sync task state machine operations.

W-C1.5: connector_sync_tasks state machine.

State transitions:
  queued → fetching → fetched (verdict set)
                    → failed

Verdict values (set when status transitions to 'fetched' or 'failed'):
  fetched_new       — source_uri seen for first time; document + ingest_job created
  fetched_updated   — content changed (source_hash differs); new version created
  skipped_unchanged — content unchanged (source_hash matches); NO document row written
  fetch_failed      — remote fetch error; NO document row written

Source revision tracking (W-C1.6 provenance immutability contract):
  observed_source_revision is written to connector_sync_tasks ONLY.
  It is NEVER propagated to documents.source_revision.
  documents.source_revision is immutable per row, set on INSERT by the adapter.
  A skipped_unchanged task still records observed_source_revision so operators
  can verify that the external source was checked.

Relationship to ingest pipeline:
  connector_sync_tasks has its own state machine, separate from ingest_jobs.
  The sync verdict is finalized here. If the downstream orchestrator subsequently
  fails, the sync task remains at status='fetched', verdict='fetched_new' or
  'fetched_updated'. The orchestrator failure is visible on the document row
  (status='failed') and the ingest_job — not on the sync task.
"""

import logging
import uuid

from db.connection import transaction

logger = logging.getLogger(__name__)


def create_sync_task(
    *,
    connector_module: str,
    scope: str,
    source_uri: str,
) -> str:
    """Create a new sync task in 'queued' state. Returns task_id."""
    task_id = str(uuid.uuid4())
    with transaction() as cur:
        cur.execute(
            """
            INSERT INTO connector_sync_tasks
                (task_id, connector_module, scope, source_uri, status)
            VALUES (%s, %s, %s, %s, 'queued')
            """,
            (task_id, connector_module, scope, source_uri),
        )
    return task_id


def mark_fetching(task_id: str) -> None:
    """Transition task from queued → fetching."""
    with transaction() as cur:
        cur.execute(
            """
            UPDATE connector_sync_tasks
            SET status = 'fetching', updated_at = NOW()
            WHERE task_id = %s
            """,
            (task_id,),
        )


def set_verdict(
    task_id: str,
    *,
    verdict: str,
    observed_source_revision: str | None = None,
    document_id: str | None = None,
    job_id: str | None = None,
    error_message: str | None = None,
) -> None:
    """Finalize a sync task with a verdict.

    For fetched_new / fetched_updated: status → 'fetched'; document_id and
    job_id are populated after the ingest handoff.

    For skipped_unchanged: status → 'fetched'; document_id and job_id are NULL
    (no document row was written, no ingest job created).

    For fetch_failed: status → 'failed'; document_id and job_id are NULL.

    observed_source_revision is always recorded here (even for skipped_unchanged).
    It is NEVER written to documents.source_revision.

    Valid verdicts: fetched_new | fetched_updated | skipped_unchanged | fetch_failed
    """
    valid_verdicts = {"fetched_new", "fetched_updated", "skipped_unchanged", "fetch_failed"}
    if verdict not in valid_verdicts:
        raise ValueError(f"Invalid verdict {verdict!r}. Must be one of {valid_verdicts}")

    final_status = "failed" if verdict == "fetch_failed" else "fetched"

    with transaction() as cur:
        cur.execute(
            """
            UPDATE connector_sync_tasks SET
                status                   = %s,
                verdict                  = %s,
                observed_source_revision = COALESCE(%s, observed_source_revision),
                document_id              = COALESCE(%s, document_id),
                job_id                   = COALESCE(%s, job_id),
                error_message            = COALESCE(%s, error_message),
                updated_at               = NOW()
            WHERE task_id = %s
            """,
            (
                final_status, verdict,
                observed_source_revision,
                document_id,
                job_id,
                error_message,
                task_id,
            ),
        )


def get_task(task_id: str) -> dict | None:
    """Return a sync task by task_id, or None if not found."""
    with transaction() as cur:
        cur.execute(
            """
            SELECT task_id, connector_module, scope, source_uri,
                   status, verdict, observed_source_revision,
                   document_id, job_id, error_message,
                   created_at, updated_at
            FROM connector_sync_tasks
            WHERE task_id = %s
            """,
            (task_id,),
        )
        row = cur.fetchone()
        return _row_to_dict(row) if row else None


def get_latest_task(
    connector_module: str,
    source_uri: str,
) -> dict | None:
    """Return the most recent sync task for a (connector_module, source_uri) pair.

    Used by the adapter to look up the prior active document hash for dedup.
    Returns None if this source_uri has never been synced.
    """
    with transaction() as cur:
        cur.execute(
            """
            SELECT task_id, connector_module, scope, source_uri,
                   status, verdict, observed_source_revision,
                   document_id, job_id, error_message,
                   created_at, updated_at
            FROM connector_sync_tasks
            WHERE connector_module = %s AND source_uri = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (connector_module, source_uri),
        )
        row = cur.fetchone()
        return _row_to_dict(row) if row else None


def list_tasks(
    connector_module: str,
    scope: str,
    *,
    limit: int = 50,
    status: str | None = None,
) -> list[dict]:
    """Return sync tasks for a (connector_module, scope) pair, newest first."""
    with transaction() as cur:
        if status is not None:
            cur.execute(
                """
                SELECT task_id, connector_module, scope, source_uri,
                       status, verdict, observed_source_revision,
                       document_id, job_id, error_message,
                       created_at, updated_at
                FROM connector_sync_tasks
                WHERE connector_module = %s AND scope = %s AND status = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (connector_module, scope, status, limit),
            )
        else:
            cur.execute(
                """
                SELECT task_id, connector_module, scope, source_uri,
                       status, verdict, observed_source_revision,
                       document_id, job_id, error_message,
                       created_at, updated_at
                FROM connector_sync_tasks
                WHERE connector_module = %s AND scope = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (connector_module, scope, limit),
            )
        return [_row_to_dict(r) for r in cur.fetchall()]


def _row_to_dict(row) -> dict:
    def _iso(dt) -> str | None:
        return dt.isoformat() if dt is not None else None

    return {
        "task_id": str(row[0]),
        "connector_module": row[1],
        "scope": row[2],
        "source_uri": row[3],
        "status": row[4],
        "verdict": row[5],
        "observed_source_revision": row[6],
        "document_id": str(row[7]) if row[7] else None,
        "job_id": str(row[8]) if row[8] else None,
        "error_message": row[9],
        "created_at": _iso(row[10]),
        "updated_at": _iso(row[11]),
    }
