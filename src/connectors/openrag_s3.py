"""AWS S3 / S3-compatible connector — adapted from OpenRAG for knowledge-ingest.

Source provenance
-----------------
Derived from ``langflow-ai/openrag`` (Apache 2.0 license).
Original paths:
  ``src/connectors/aws_s3/connector.py``
  ``src/connectors/aws_s3/auth.py``
Adaptation date: 2026-04-11 (W-C2.1, knowledge-ingest#37)

What was changed relative to the OpenRAG originals
---------------------------------------------------
- Merged ``auth.py`` credential/client-factory logic directly into this module
  to eliminate the OpenRAG internal package dependency (``from .auth import ...``).
- Removed ACL extraction (``_extract_acl``, ``get_object_acl`` call) — ACL is
  OpenRAG-internal; knowledge-ingest does not use it. ``DocumentACL`` is gone.
- Removed all webhook stubs (``setup_subscription``, ``handle_webhook``,
  ``cleanup_subscription``, ``extract_webhook_channel_id``) — webhook support is
  out of scope for W-C2.1 (#37). These methods are not present on ``BaseConnector``
  in this repo's minimal base.
- Removed ``utils.logging_config`` import — standard ``logging`` used instead.
- ``get_logger`` replaced with ``logging.getLogger(__name__)``.
- ``ConnectorDocument`` and ``BaseConnector`` now imported from ``openrag_base``.
- ``CONNECTOR_NAME`` / ``CONNECTOR_DESCRIPTION`` / ``CONNECTOR_ICON`` class attrs
  retained for documentation clarity; they have no runtime effect here.

What was NOT changed
--------------------
- ``_make_file_id`` / ``_split_file_id`` / ``_ID_SEPARATOR`` logic is identical.
- ``authenticate()`` bucket listing approach is unchanged.
- ``list_files()`` pagination and prefix filtering logic is unchanged.
- ``get_file_content()`` boto3 download and MIME type inference is unchanged.
- Config key names (``access_key``, ``secret_key``, ``endpoint_url``, ``region``,
  ``bucket_names``, ``prefix``, ``connection_id``) are unchanged.

Secret handling contract (enforced by callers, not this class)
--------------------------------------------------------------
- This class accepts credentials via the ``config`` dict only.
- It reads ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` as a fallback only
  when the config dict does not supply them (boto3 default chain behaviour).
- It must NEVER receive raw secret values as positional arguments or log them.
- Callers (aws_s3_wrapper.py) are responsible for loading from managed secret
  storage and injecting into the config dict.
"""

import logging
import mimetypes
from datetime import datetime, timezone
from posixpath import basename
from typing import Any, Dict, List, Optional

from connectors.openrag_base import BaseConnector, ConnectorDocument

logger = logging.getLogger(__name__)

# Separator used in composite file IDs: "<bucket>::<key>"
_ID_SEPARATOR = "::"
_DEFAULT_REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Credential / client helpers (inlined from OpenRAG auth.py)
# ---------------------------------------------------------------------------

def _resolve_credentials(config: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve S3 credentials from config dict with environment-variable fallback.

    Resolution order: config dict → AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars.
    Raises ValueError if access_key or secret_key cannot be resolved.
    """
    import os

    access_key: Optional[str] = config.get("access_key") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key: Optional[str] = config.get("secret_key") or os.getenv("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "S3 credentials are required. Provide 'access_key' and 'secret_key' in the "
            "connector config dict, or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
        )

    endpoint_url: Optional[str] = (
        config.get("endpoint_url") or os.getenv("AWS_S3_ENDPOINT") or None
    )
    region: str = config.get("region") or os.getenv("AWS_REGION") or _DEFAULT_REGION

    return {
        "access_key": access_key,
        "secret_key": secret_key,
        "endpoint_url": endpoint_url,
        "region": region,
    }


def _build_boto3_kwargs(creds: Dict[str, Any]) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "aws_access_key_id": creds["access_key"],
        "aws_secret_access_key": creds["secret_key"],
        "region_name": creds["region"],
    }
    if creds["endpoint_url"]:
        kwargs["endpoint_url"] = creds["endpoint_url"]
    return kwargs


def _create_s3_resource(config: Dict[str, Any]):
    try:
        import boto3
    except ImportError as exc:
        raise ImportError("boto3 is required. Install it with: pip install boto3") from exc
    creds = _resolve_credentials(config)
    return boto3.resource("s3", **_build_boto3_kwargs(creds))


def _create_s3_client(config: Dict[str, Any]):
    try:
        import boto3
    except ImportError as exc:
        raise ImportError("boto3 is required. Install it with: pip install boto3") from exc
    creds = _resolve_credentials(config)
    return boto3.client("s3", **_build_boto3_kwargs(creds))


# ---------------------------------------------------------------------------
# Composite file ID helpers
# ---------------------------------------------------------------------------

def _make_file_id(bucket: str, key: str) -> str:
    return f"{bucket}{_ID_SEPARATOR}{key}"


def _split_file_id(file_id: str):
    """Split a composite file ID into (bucket, key). Raises ValueError if invalid."""
    if _ID_SEPARATOR not in file_id:
        raise ValueError(f"Invalid S3 file ID (missing separator): {file_id!r}")
    bucket, key = file_id.split(_ID_SEPARATOR, 1)
    return bucket, key


# ---------------------------------------------------------------------------
# Connector class
# ---------------------------------------------------------------------------

class S3Connector(BaseConnector):
    """Connector for Amazon S3 and S3-compatible object storage.

    Supports AWS S3, MinIO, Cloudflare R2, and any S3-API-compatible service.
    Uses HMAC (Access Key + Secret Key) authentication — no OAuth, no token refresh.

    Config dict keys
    ----------------
    access_key (str)        : AWS access key ID. Falls back to AWS_ACCESS_KEY_ID env var.
    secret_key (str)        : AWS secret access key. Falls back to AWS_SECRET_ACCESS_KEY.
    endpoint_url (str|None) : Custom endpoint for S3-compatible services. Omit for AWS.
    region (str)            : AWS region. Default: us-east-1.
    bucket_names (list[str]): Buckets to ingest from. If empty, all accessible buckets.
    prefix (str)            : Optional key prefix filter (e.g. "docs/").
    connection_id (str)     : Optional identifier for log correlation.

    Webhook support
    ---------------
    Not implemented in this class. S3 event notification integration is out of scope
    for W-C2.1 (#37). This connector covers pull-based sync only.
    """

    CONNECTOR_NAME = "Amazon S3"
    CONNECTOR_DESCRIPTION = "Ingest documents from Amazon S3 or S3-compatible object storage"

    def __init__(self, config: Dict[str, Any]) -> None:
        if config is None:
            config = {}
        super().__init__(config)
        self.bucket_names: List[str] = config.get("bucket_names") or []
        self.prefix: str = config.get("prefix", "")
        self.connection_id: str = config.get("connection_id", "default")
        self._resource = None  # lazy-initialized on first use
        self._client = None

    def _get_resource(self):
        if self._resource is None:
            self._resource = _create_s3_resource(self.config)
        return self._resource

    def _get_client(self):
        if self._client is None:
            self._client = _create_s3_client(self.config)
        return self._client

    # ------------------------------------------------------------------
    # BaseConnector abstract implementations
    # ------------------------------------------------------------------

    async def authenticate(self) -> bool:
        """Validate credentials by listing accessible buckets."""
        try:
            resource = self._get_resource()
            list(resource.buckets.all())
            self._authenticated = True
            logger.debug("S3 authenticated for connection %s", self.connection_id)
            return True
        except Exception as exc:
            # Log the exception class and message only — never log credentials.
            logger.warning(
                "S3 authentication failed for connection %s: %s",
                self.connection_id, type(exc).__name__,
            )
            self._authenticated = False
            return False

    def _resolve_bucket_names(self) -> List[str]:
        """Return configured bucket names, or auto-discover all accessible buckets."""
        if self.bucket_names:
            return self.bucket_names
        try:
            resource = self._get_resource()
            buckets = [b.name for b in resource.buckets.all()]
            logger.debug("S3 auto-discovered %d bucket(s) for connection %s",
                         len(buckets), self.connection_id)
            return buckets
        except Exception as exc:
            logger.warning("S3 could not auto-discover buckets: %s", type(exc).__name__)
            return []

    async def list_files(
        self,
        page_token: Optional[str] = None,
        max_files: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """List objects across all configured (or auto-discovered) buckets.

        Uses boto3 resource API — pagination handled internally by SDK.

        Returns:
            {"files": [...], "next_page_token": None}
            Each file dict: {id, name, bucket, key, size, modified_time}
        """
        resource = self._get_resource()
        files: List[Dict[str, Any]] = []
        bucket_names = self._resolve_bucket_names()

        for bucket_name in bucket_names:
            try:
                bucket = resource.Bucket(bucket_name)
                objects = (
                    bucket.objects.filter(Prefix=self.prefix)
                    if self.prefix
                    else bucket.objects.all()
                )
                for obj in objects:
                    if obj.key.endswith("/"):
                        continue
                    files.append({
                        "id": _make_file_id(bucket_name, obj.key),
                        "name": basename(obj.key) or obj.key,
                        "bucket": bucket_name,
                        "key": obj.key,
                        "size": obj.size,
                        "modified_time": obj.last_modified,  # boto3 datetime (UTC-aware)
                    })
                    if max_files and len(files) >= max_files:
                        return {"files": files, "next_page_token": None}
            except Exception as exc:
                logger.error(
                    "S3 failed to list objects in bucket %r: %s",
                    bucket_name, type(exc).__name__,
                )
                continue

        return {"files": files, "next_page_token": None}

    async def get_file_content(self, file_id: str) -> ConnectorDocument:
        """Download an S3 object and return a ConnectorDocument.

        Args:
            file_id: Composite ID in the form "<bucket>::<key>".
        """
        bucket_name, key = _split_file_id(file_id)
        resource = self._get_resource()

        response = resource.Object(bucket_name, key).get()
        content: bytes = response["Body"].read()

        last_modified: datetime = response.get("LastModified") or datetime.now(timezone.utc)
        size: int = response.get("ContentLength", len(content))

        raw_content_type = response.get("ContentType", "")
        if raw_content_type and raw_content_type != "application/octet-stream":
            mime_type: str = raw_content_type
        else:
            mime_type = mimetypes.guess_type(key)[0] or "application/octet-stream"

        filename = basename(key) or key

        return ConnectorDocument(
            id=file_id,
            filename=filename,
            mimetype=mime_type,
            content=content,
            source_url=f"s3://{bucket_name}/{key}",
            modified_time=last_modified,
            created_time=last_modified,  # S3 does not expose object creation time
            metadata={
                "s3_bucket": bucket_name,
                "s3_key": key,
                "size": size,
            },
        )
