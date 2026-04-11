"""Runtime configuration for agentopia-knowledge-ingest.

All configuration is loaded from environment variables. No config file is read at runtime.
In development, source .env.local before starting the service.
"""

import os
from functools import lru_cache


class Settings:
    """Configuration loaded from environment variables."""

    # ── Database ──────────────────────────────────────────────────────────────
    database_url: str  # postgresql://user:pass@host:5432/db
    # Unset → service starts but document registry operations fail with a clear error.

    # ── Storage ───────────────────────────────────────────────────────────────
    s3_bucket: str
    # When set, use S3 for raw document storage.
    # When empty, fall back to local filesystem at storage_local_path.

    s3_region: str
    s3_endpoint_url: str
    # For MinIO or other S3-compatible stores. Empty → AWS S3.

    aws_access_key_id: str
    aws_secret_access_key: str

    storage_local_path: str
    # Local filesystem path used when S3_BUCKET is not configured.
    # Useful for local dev and CI testing.

    # ── Super RAG integration ─────────────────────────────────────────────────
    super_rag_url: str
    # Base URL of the agentopia-super-rag service.
    # Example: http://knowledge-api.agentopia-dev.svc.cluster.local:8002

    super_rag_internal_token: str
    # Shared token sent as X-Internal-Token to Super RAG /ingest-document endpoint.

    super_rag_ingest_timeout_seconds: int
    # Timeout in seconds for the POST /api/v1/knowledge/{scope}/ingest-document call.
    # Default: 120 (large PDFs can take time to embed).

    super_rag_ingest_max_retries: int
    # Number of retry attempts for the Super RAG ingest call before marking failed.
    # Default: 5.

    # ── Service ───────────────────────────────────────────────────────────────
    log_level: str       # DEBUG | INFO | WARNING | ERROR. Default: INFO
    log_format: str      # text | json. Default: text; set json for structured output.
    port: int            # Default: 8003

    def __init__(self) -> None:
        self.database_url = os.getenv("DATABASE_URL", "")

        self.s3_bucket = os.getenv("S3_BUCKET", "")
        self.s3_region = os.getenv("S3_REGION", "us-east-1")
        self.s3_endpoint_url = os.getenv("S3_ENDPOINT_URL", "")
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        self.storage_local_path = os.getenv("STORAGE_LOCAL_PATH", "/tmp/knowledge-store")

        self.super_rag_url = os.getenv("SUPER_RAG_URL", "http://localhost:8002")
        self.super_rag_internal_token = os.getenv("SUPER_RAG_INTERNAL_TOKEN", "")
        self.super_rag_ingest_timeout_seconds = int(
            os.getenv("SUPER_RAG_INGEST_TIMEOUT_SECONDS", "120")
        )
        self.super_rag_ingest_max_retries = int(
            os.getenv("SUPER_RAG_INGEST_MAX_RETRIES", "5")
        )

        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.log_format = os.getenv("LOG_FORMAT", "text")
        self.port = int(os.getenv("PORT", "8003"))

    def use_s3(self) -> bool:
        """Return True if S3 is configured as the storage backend."""
        return bool(self.s3_bucket)

    def super_rag_configured(self) -> bool:
        """Return True if Super RAG integration is configured."""
        return bool(self.super_rag_url and self.super_rag_internal_token)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance."""
    return Settings()
