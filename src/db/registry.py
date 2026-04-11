"""Document registry, job tracking, and audit log operations.

All writes to audit_log must go through write_audit_event — never via raw SQL elsewhere.
The audit_log table is append-only: no UPDATE or DELETE operations are permitted.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from db.connection import transaction

logger = logging.getLogger(__name__)


# ── Scope operations ──────────────────────────────────────────────────────────


def scope_exists(scope_name: str) -> bool:
    """Return True if scope_name is registered."""
    with transaction() as cur:
        cur.execute(
            "SELECT 1 FROM scopes WHERE scope_name = %s LIMIT 1",
            (scope_name,),
        )
        return cur.fetchone() is not None


def create_scope(scope_name: str, description: str = "", owner: str = "") -> dict:
    """Create a new scope entry. Raises ValueError if already exists."""
    scope_id = str(uuid.uuid4())
    with transaction() as cur:
        cur.execute(
            """
            INSERT INTO scopes (scope_id, scope_name, description, owner)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (scope_name) DO NOTHING
            RETURNING scope_id, scope_name, description, owner, created_at
            """,
            (scope_id, scope_name, description or None, owner or None),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Scope '{scope_name}' already exists")
        return {
            "scope_id": str(row[0]),
            "scope_name": row[1],
            "description": row[2],
            "owner": row[3],
            "created_at": row[4].isoformat(),
        }


def list_scopes() -> list[dict]:
    """Return all registered scopes with document counts."""
    with transaction() as cur:
        cur.execute(
            """
            SELECT s.scope_id, s.scope_name, s.description, s.owner, s.created_at,
                   COUNT(d.document_id) AS document_count
            FROM scopes s
            LEFT JOIN documents d ON d.scope = s.scope_name AND d.status = 'active'
            GROUP BY s.scope_id, s.scope_name, s.description, s.owner, s.created_at
            ORDER BY s.scope_name
            """,
        )
        rows = cur.fetchall()
        return [
            {
                "scope_id": str(r[0]),
                "scope_name": r[1],
                "description": r[2],
                "owner": r[3],
                "created_at": r[4].isoformat(),
                "document_count": r[5],
            }
            for r in rows
        ]


# ── Document operations ───────────────────────────────────────────────────────


def create_document(
    *,
    scope: str,
    filename: str,
    format: str,
    source_hash: str,
    s3_original_key: str,
    owner: str = "",
) -> dict:
    """Create a new document entry with status=submitted.

    Determines version by incrementing the highest existing version for
    (scope, source_hash) — so identical files re-uploaded get a new version.
    Returns the created document dict.
    """
    document_id = str(uuid.uuid4())
    # Version = max existing version + 1, or 1 for first upload
    with transaction() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(version), 0) + 1
            FROM documents
            WHERE scope = %s AND source_hash = %s
            """,
            (scope, source_hash),
        )
        version = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO documents
                (document_id, scope, owner, filename, format, version, status,
                 source_hash, s3_original_key)
            VALUES (%s, %s, %s, %s, %s, %s, 'submitted', %s, %s)
            RETURNING document_id, version, created_at
            """,
            (document_id, scope, owner, filename, format, version, source_hash, s3_original_key),
        )
        row = cur.fetchone()
        return {
            "document_id": str(row[0]),
            "version": row[1],
            "scope": scope,
            "filename": filename,
            "format": format,
            "status": "submitted",
            "source_hash": source_hash,
            "s3_original_key": s3_original_key,
            "created_at": row[2].isoformat(),
        }


def update_document_status(
    document_id: str,
    status: str,
    *,
    s3_normalized_key: str | None = None,
    s3_extracted_key: str | None = None,
    error_message: str | None = None,
) -> None:
    """Transition document to a new status and update S3 keys if provided."""
    with transaction() as cur:
        cur.execute(
            """
            UPDATE documents SET
                status = %s,
                s3_normalized_key = COALESCE(%s, s3_normalized_key),
                s3_extracted_key  = COALESCE(%s, s3_extracted_key),
                error_message     = COALESCE(%s, error_message),
                updated_at        = NOW()
            WHERE document_id = %s
            """,
            (status, s3_normalized_key, s3_extracted_key, error_message, document_id),
        )


def get_document(document_id: str) -> dict | None:
    """Return document dict or None if not found."""
    with transaction() as cur:
        cur.execute(
            """
            SELECT document_id, scope, owner, filename, format, version, status,
                   source_hash, s3_original_key, s3_normalized_key, s3_extracted_key,
                   error_message, created_at, updated_at
            FROM documents WHERE document_id = %s
            """,
            (document_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return _doc_row_to_dict(row)


def list_documents(scope: str, status: str = "active") -> list[dict]:
    """Return all documents for scope filtered by status."""
    with transaction() as cur:
        if status == "all":
            cur.execute(
                """
                SELECT document_id, scope, owner, filename, format, version, status,
                       source_hash, s3_original_key, s3_normalized_key, s3_extracted_key,
                       error_message, created_at, updated_at
                FROM documents WHERE scope = %s ORDER BY filename, version DESC
                """,
                (scope,),
            )
        else:
            cur.execute(
                """
                SELECT document_id, scope, owner, filename, format, version, status,
                       source_hash, s3_original_key, s3_normalized_key, s3_extracted_key,
                       error_message, created_at, updated_at
                FROM documents WHERE scope = %s AND status = %s ORDER BY filename, version DESC
                """,
                (scope, status),
            )
        return [_doc_row_to_dict(r) for r in cur.fetchall()]


def get_document_versions(document_id: str) -> list[dict]:
    """Return all versions of a document ordered by version desc."""
    with transaction() as cur:
        cur.execute(
            """
            SELECT document_id, scope, owner, filename, format, version, status,
                   source_hash, s3_original_key, s3_normalized_key, s3_extracted_key,
                   error_message, created_at, updated_at
            FROM documents WHERE document_id = %s ORDER BY version DESC
            """,
            (document_id,),
        )
        return [_doc_row_to_dict(r) for r in cur.fetchall()]


def rollback_to_version(document_id: str, target_version: int) -> dict:
    """Restore target_version to active; set current active to superseded.

    Raises ValueError if target_version doesn't exist, is already active,
    or has status=deleted.
    """
    with transaction() as cur:
        # Find current active version
        cur.execute(
            "SELECT version FROM documents WHERE document_id = %s AND status = 'active'",
            (document_id,),
        )
        active_row = cur.fetchone()

        # Find target version
        cur.execute(
            "SELECT version, status FROM documents WHERE document_id = %s AND version = %s",
            (document_id, target_version),
        )
        target_row = cur.fetchone()
        if target_row is None:
            raise ValueError(f"Version {target_version} not found for document {document_id}")
        if target_row[1] == "active":
            raise ValueError(f"Version {target_version} is already active")
        if target_row[1] == "deleted":
            raise ValueError(f"Version {target_version} is deleted and cannot be restored")

        # Supersede current active version (if any)
        if active_row is not None:
            cur.execute(
                "UPDATE documents SET status = 'superseded', updated_at = NOW() "
                "WHERE document_id = %s AND status = 'active'",
                (document_id,),
            )

        # Restore target version to active
        cur.execute(
            "UPDATE documents SET status = 'active', updated_at = NOW(), error_message = NULL "
            "WHERE document_id = %s AND version = %s",
            (document_id, target_version),
        )
        return {
            "document_id": document_id,
            "restored_version": target_version,
            "superseded_version": active_row[0] if active_row else None,
        }


def _doc_row_to_dict(row) -> dict:
    def _iso(dt) -> str | None:
        return dt.isoformat() if dt is not None else None
    return {
        "document_id": str(row[0]),
        "scope": row[1],
        "owner": row[2],
        "filename": row[3],
        "format": row[4],
        "version": row[5],
        "status": row[6],
        "source_hash": row[7],
        "s3_original_key": row[8],
        "s3_normalized_key": row[9],
        "s3_extracted_key": row[10],
        "error_message": row[11],
        "created_at": _iso(row[12]),
        "updated_at": _iso(row[13]),
    }


# ── Job operations ────────────────────────────────────────────────────────────


def create_job(document_id: str) -> str:
    """Create an ingest job for document_id. Returns job_id."""
    job_id = str(uuid.uuid4())
    with transaction() as cur:
        cur.execute(
            """
            INSERT INTO ingest_jobs (job_id, document_id, status, progress_percent)
            VALUES (%s, %s, 'submitted', 0)
            """,
            (job_id, document_id),
        )
    return job_id


def update_job(
    job_id: str,
    *,
    status: str,
    stage: str | None = None,
    progress_percent: int | None = None,
    error_message: str | None = None,
    completed: bool = False,
) -> None:
    """Update job status, stage, progress, and error message."""
    with transaction() as cur:
        cur.execute(
            """
            UPDATE ingest_jobs SET
                status           = %s,
                stage            = COALESCE(%s, stage),
                progress_percent = COALESCE(%s, progress_percent),
                error_message    = COALESCE(%s, error_message),
                updated_at       = NOW(),
                completed_at     = CASE WHEN %s THEN NOW() ELSE completed_at END
            WHERE job_id = %s
            """,
            (status, stage, progress_percent, error_message, completed, job_id),
        )


def get_job(job_id: str) -> dict | None:
    """Return job dict or None if not found."""
    with transaction() as cur:
        cur.execute(
            """
            SELECT j.job_id, j.document_id, d.scope, d.version, j.status, j.stage,
                   j.progress_percent, j.error_message,
                   j.created_at, j.updated_at, j.completed_at
            FROM ingest_jobs j
            JOIN documents d ON d.document_id = j.document_id
            WHERE j.job_id = %s
            """,
            (job_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        def _iso(dt) -> str | None:
            return dt.isoformat() if dt is not None else None
        return {
            "job_id": str(row[0]),
            "document_id": str(row[1]),
            "scope": row[2],
            "version": row[3],
            "status": row[4],
            "stage": row[5],
            "progress_percent": row[6],
            "error_message": row[7],
            "created_at": _iso(row[8]),
            "updated_at": _iso(row[9]),
            "completed_at": _iso(row[10]),
        }


# ── Audit log ────────────────────────────────────────────────────────────────


def write_audit_event(
    *,
    action: str,
    document_id: str | None = None,
    job_id: str | None = None,
    scope: str | None = None,
    actor: str | None = None,
    status: str | None = None,
    error_message: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Append an event to the audit_log. Never raises — logs errors instead."""
    try:
        with transaction() as cur:
            cur.execute(
                """
                INSERT INTO audit_log
                    (document_id, job_id, scope, action, actor, status, error_message, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    document_id,
                    job_id,
                    scope,
                    action,
                    actor,
                    status,
                    error_message,
                    json.dumps(metadata or {}),
                ),
            )
    except Exception as exc:
        logger.error("audit_log write failed action=%s doc=%s: %s", action, document_id, exc)
