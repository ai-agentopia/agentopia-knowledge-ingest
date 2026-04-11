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
    """Apply the initial schema migration (db/001_initial.sql) idempotently."""
    import pathlib

    sql_path = pathlib.Path(__file__).parent.parent.parent / "db" / "001_initial.sql"
    if not sql_path.exists():
        logger.warning("db: schema file not found at %s — skipping migration", sql_path)
        return

    sql = sql_path.read_text()
    try:
        with transaction() as cur:
            cur.execute(sql)
        logger.info("db: schema migration applied successfully")
    except Exception as exc:
        logger.error("db: schema migration failed: %s", exc)
        raise


def close_connection() -> None:
    """Close the database connection on service shutdown."""
    global _pool
    if _pool and not _pool.closed:
        _pool.close()
        _pool = None
        logger.info("db: connection closed")
