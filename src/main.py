"""Agentopia Knowledge Ingest Service — entry point.

Starts the FastAPI application. All configuration is loaded from
environment variables via src/config.py.

Dev start:
    cd src && uvicorn main:app --port 8003 --reload

Production:
    uvicorn main:app --host 0.0.0.0 --port 8003
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.connector_routes import router as connector_router
from api.routes import router
from config import get_settings

# ── Logging setup ─────────────────────────────────────────────────────────────

settings = get_settings()

_log_format = (
    "%(asctime)s %(levelname)s %(name)s %(message)s"
    if settings.log_format != "json"
    else None
)
logging.basicConfig(
    level=settings.log_level.upper(),
    format=_log_format,
)

if settings.log_format == "json":
    try:
        import json_log_formatter
        handler = logging.StreamHandler()
        handler.setFormatter(json_log_formatter.JSONFormatter())
        logging.root.handlers = [handler]
    except ImportError:
        pass  # Fall back to plain text if json_log_formatter not installed

logger = logging.getLogger(__name__)


# ── Lifespan ──────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown hooks."""
    # Apply schema migration on startup (idempotent)
    if settings.database_url:
        try:
            from db.connection import apply_schema
            apply_schema()
        except Exception as exc:
            logger.error("Startup: schema migration failed: %s", exc)
            # Service continues — operators can fix and restart

    logger.info(
        "knowledge-ingest started: port=%d storage=%s super_rag=%s",
        settings.port,
        "s3" if settings.use_s3() else "local",
        settings.super_rag_url,
    )
    yield

    # Shutdown
    from db.connection import close_connection
    close_connection()
    logger.info("knowledge-ingest stopped")


# ── Application ───────────────────────────────────────────────────────────────


app = FastAPI(
    title="Agentopia Knowledge Ingest Service",
    description=(
        "Upstream document ingestion pipeline for Agentopia. "
        "Accepts PDF, DOCX, HTML, and Markdown documents, normalizes them, "
        "extracts metadata, and hands off to Super RAG for retrieval indexing."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)
app.include_router(connector_router)
