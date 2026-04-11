"""PostgreSQL connection management for agentopia-knowledge-ingest."""

import logging
import os
from contextlib import contextmanager
from typing import Generator

import psycopg

logger = logging.getLogger(__name__)

_pool: psycopg.Connection | None = None


def get_connection() -> psycopg.Connection:
    """Return a synchronous psycopg connection.

    On first call, establishes a persistent connection from DATABASE_URL.
    Raises RuntimeError if DATABASE_URL is not set.
    """
    global _pool
    if _pool is None or _pool.closed:
        database_url = os.getenv("DATABASE_URL", "")
        if not database_url:
            raise RuntimeError(
                "DATABASE_URL is not configured. "
                "Set it to a valid PostgreSQL connection string."
            )
        _pool = psycopg.connect(database_url, autocommit=False)
        logger.info("db: connected to PostgreSQL")
    return _pool


@contextmanager
def transaction() -> Generator[psycopg.Cursor, None, None]:
    """Context manager that yields a cursor inside a transaction.

    Commits on clean exit, rolls back on exception.
    """
    conn = get_connection()
    with conn.cursor() as cur:
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def apply_schema() -> None:
    """Apply all schema migrations in db/NNN_*.sql order, idempotently.

    Scans db/ for files matching the NNN_*.sql pattern and applies them in
    sorted order. Each file uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS so
    re-running is safe.
    """
    import pathlib

    db_dir = pathlib.Path(__file__).parent.parent.parent / "db"
    sql_files = sorted(db_dir.glob("[0-9][0-9][0-9]_*.sql"))

    if not sql_files:
        logger.warning("db: no schema files found in %s — skipping migration", db_dir)
        return

    for sql_path in sql_files:
        sql = sql_path.read_text()
        try:
            with transaction() as cur:
                cur.execute(sql)
            logger.info("db: applied migration %s", sql_path.name)
        except Exception as exc:
            logger.error("db: migration %s failed: %s", sql_path.name, exc)
            raise


def close_connection() -> None:
    """Close the database connection on service shutdown."""
    global _pool
    if _pool and not _pool.closed:
        _pool.close()
        _pool = None
        logger.info("db: connection closed")
