"""Document registry, job tracking, and audit log operations.

Versioning model
----------------
document_id is the STABLE logical identity of a document. It is derived
deterministically from (scope, filename) using a MD5-seeded UUID so the same
logical document always gets the same ID, even across service restarts.

version increments on each upload of the same logical document:
  - Upload 1  → document_id=X, version=1, status=submitted
  - Upload 2  → document_id=X, version=2, status=submitted  (prior v1 → superseded on active)

The documents table primary key is a surrogate (row_id BIGSERIAL). The composite
(document_id, version) has a UNIQUE constraint. ingest_jobs.row_id FK points to
the specific document version row, not just the logical document.

Audit log
---------
All writes to audit_log go through write_audit_event — never raw SQL elsewhere.
The table is append-only: no UPDATE or DELETE permitted on audit_log.
"""

import hashlib
import json
import logging
import uuid

from db.connection import transaction

logger = logging.getLogger(__name__)


# ── Stable document_id derivation ────────────────────────────────────────────


def stable_document_id(scope: str, filename: str) -> str:
    """Derive a deterministic UUID from (scope, filename).

    The same logical document (same scope + filename) always resolves to the
    same UUID, regardless of upload count or service restart. This makes
    document_id a stable logical identity rather than a per-upload handle.
    """
    key = f"{scope}:{filename}"
    digest = hashlib.md5(key.encode()).hexdigest()
    return str(uuid.UUID(digest))


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
    """Return all registered scopes with active document counts."""
    with transaction() as cur:
        cur.execute(
            """
            SELECT s.scope_id, s.scope_name, s.description, s.owner, s.created_at,
                   COUNT(DISTINCT d.document_id) AS document_count
            FROM scopes s
            LEFT JOIN documents d ON d.scope = s.scope_name AND d.status = 'active'
            GROUP BY s.scope_id, s.scope_name, s.description, s.owner, s.created_at
            ORDER BY s.scope_name
            """,
        )
        return [
            {
                "scope_id": str(r[0]),
                "scope_name": r[1],
                "description": r[2],
                "owner": r[3],
                "created_at": r[4].isoformat(),
                "document_count": r[5],
            }
            for r in cur.fetchall()
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
    """Create a new document version row and return it.

    document_id is derived deterministically from (scope, filename) so the same
    logical document always shares one document_id across all its versions.

    version = max existing version for this document_id + 1, or 1 for first upload.

    Returns the created row as a dict including row_id (used for ingest_jobs FK).
    """
    doc_id = stable_document_id(scope, filename)

    with transaction() as cur:
        # Determine next version for this logical document
        cur.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM documents WHERE document_id = %s",
            (doc_id,),
        )
        version = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO documents
                (document_id, version, scope, owner, filename, format,
                 status, source_hash, s3_original_key)
            VALUES (%s, %s, %s, %s, %s, %s, 'submitted', %s, %s)
            RETURNING row_id, document_id, version, created_at
            """,
            (doc_id, version, scope, owner, filename, format, source_hash, s3_original_key),
        )
        row = cur.fetchone()
        return {
            "row_id": row[0],
            "document_id": str(row[1]),
            "version": row[2],
            "scope": scope,
            "filename": filename,
            "format": format,
            "status": "submitted",
            "source_hash": source_hash,
            "s3_original_key": s3_original_key,
            "created_at": row[3].isoformat(),
        }


def update_document_status(
    row_id: int,
    status: str,
    *,
    s3_normalized_key: str | None = None,
    s3_extracted_key: str | None = None,
    error_message: str | None = None,
) -> None:
    """Transition a document row to a new status.

    Uses row_id (surrogate PK) to target exactly one version row.
    """
    with transaction() as cur:
        cur.execute(
            """
            UPDATE documents SET
                status            = %s,
                s3_normalized_key = COALESCE(%s, s3_normalized_key),
                s3_extracted_key  = COALESCE(%s, s3_extracted_key),
                error_message     = COALESCE(%s, error_message),
                updated_at        = NOW()
            WHERE row_id = %s
            """,
            (status, s3_normalized_key, s3_extracted_key, error_message, row_id),
        )


def patch_s3_original_key(row_id: int, s3_original_key: str) -> None:
    """Update s3_original_key after initial row creation."""
    with transaction() as cur:
        cur.execute(
            "UPDATE documents SET s3_original_key = %s, updated_at = NOW() WHERE row_id = %s",
            (s3_original_key, row_id),
        )


def get_document_row(row_id: int) -> dict | None:
    """Return a single document row by surrogate row_id."""
    with transaction() as cur:
        cur.execute(_DOC_SELECT + " WHERE d.row_id = %s", (row_id,))
        row = cur.fetchone()
        return _doc_row_to_dict(row) if row else None


def get_active_version(document_id: str) -> dict | None:
    """Return the active version row for a logical document, or None."""
    with transaction() as cur:
        cur.execute(_DOC_SELECT + " WHERE d.document_id = %s AND d.status = 'active'", (document_id,))
        row = cur.fetchone()
        return _doc_row_to_dict(row) if row else None


def list_documents(scope: str, status: str = "active") -> list[dict]:
    """Return documents for scope filtered by status.

    When status='active', returns one row per logical document (the active version).
    When status='all', returns all version rows.
    """
    with transaction() as cur:
        if status == "all":
            cur.execute(
                _DOC_SELECT + " WHERE d.scope = %s ORDER BY d.filename, d.version DESC",
                (scope,),
            )
        else:
            cur.execute(
                _DOC_SELECT + " WHERE d.scope = %s AND d.status = %s ORDER BY d.filename, d.version DESC",
                (scope, status),
            )
        return [_doc_row_to_dict(r) for r in cur.fetchall()]


def get_document_versions(document_id: str) -> list[dict]:
    """Return all version rows for a logical document, newest first.

    Relies on the fact that multiple rows share the same document_id across versions.
    Returns [] if document_id does not exist.
    """
    with transaction() as cur:
        cur.execute(
            _DOC_SELECT + " WHERE d.document_id = %s ORDER BY d.version DESC",
            (document_id,),
        )
        return [_doc_row_to_dict(r) for r in cur.fetchall()]


def rollback_to_version(document_id: str, target_version: int) -> dict:
    """Restore target_version to active; supersede current active version.

    Both updates happen in one transaction. Rolls back atomically on error.

    Raises ValueError on:
    - target_version not found for document_id
    - target_version is already active
    - target_version has status=deleted
    """
    with transaction() as cur:
        # Find current active version (may be None if document is fully superseded/failed)
        cur.execute(
            "SELECT row_id, version FROM documents WHERE document_id = %s AND status = 'active'",
            (document_id,),
        )
        active_row = cur.fetchone()

        # Find target version row
        cur.execute(
            "SELECT row_id, version, status FROM documents WHERE document_id = %s AND version = %s",
            (document_id, target_version),
        )
        target_row = cur.fetchone()

        if target_row is None:
            raise ValueError(
                f"Version {target_version} not found for document {document_id}"
            )
        if target_row[2] == "active":
            raise ValueError(
                f"Version {target_version} is already active"
            )
        if target_row[2] == "deleted":
            raise ValueError(
                f"Version {target_version} is deleted and cannot be restored"
            )

        # Supersede current active (if any)
        if active_row is not None:
            cur.execute(
                "UPDATE documents SET status = 'superseded', updated_at = NOW() WHERE row_id = %s",
                (active_row[0],),
            )

        # Restore target to active
        cur.execute(
            "UPDATE documents SET status = 'active', updated_at = NOW(), error_message = NULL "
            "WHERE row_id = %s",
            (target_row[0],),
        )

        return {
            "document_id": document_id,
            "restored_version": target_version,
            "superseded_version": active_row[1] if active_row else None,
        }


# ── SQL fragment shared across document queries ───────────────────────────────

_DOC_SELECT = """
SELECT d.row_id, d.document_id, d.version, d.scope, d.owner, d.filename,
       d.format, d.status, d.source_hash, d.s3_original_key,
       d.s3_normalized_key, d.s3_extracted_key, d.error_message,
       d.created_at, d.updated_at
FROM documents d
"""


def _doc_row_to_dict(row) -> dict:
    def _iso(dt) -> str | None:
        return dt.isoformat() if dt is not None else None
    return {
        "row_id": row[0],
        "document_id": str(row[1]),
        "version": row[2],
        "scope": row[3],
        "owner": row[4],
        "filename": row[5],
        "format": row[6],
        "status": row[7],
        "source_hash": row[8],
        "s3_original_key": row[9],
        "s3_normalized_key": row[10],
        "s3_extracted_key": row[11],
        "error_message": row[12],
        "created_at": _iso(row[13]),
        "updated_at": _iso(row[14]),
    }


# ── Job operations ────────────────────────────────────────────────────────────


def create_job(row_id: int) -> str:
    """Create an ingest job for the specific document version (row_id). Returns job_id."""
    job_id = str(uuid.uuid4())
    with transaction() as cur:
        cur.execute(
            """
            INSERT INTO ingest_jobs (job_id, row_id, status, progress_percent)
            VALUES (%s, %s, 'submitted', 0)
            """,
            (job_id, row_id),
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
    """Return job dict with joined document fields, or None if not found."""
    with transaction() as cur:
        cur.execute(
            """
            SELECT j.job_id, d.document_id, d.scope, d.version, j.status, j.stage,
                   j.progress_percent, j.error_message,
                   j.created_at, j.updated_at, j.completed_at
            FROM ingest_jobs j
            JOIN documents d ON d.row_id = j.row_id
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


# ── Audit log ─────────────────────────────────────────────────────────────────


def write_audit_event(
    *,
    action: str,
    document_id: str | None = None,
    document_version: int | None = None,
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
                    (document_id, document_version, job_id, scope, action,
                     actor, status, error_message, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    document_id,
                    document_version,
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
