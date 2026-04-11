"""W-C3.5: POST /connectors/s3/sync — server-side AWS S3 sync trigger.

Issue:  knowledge-ingest#49
Depends on: knowledge-ingest#37 (W-C2.1 aws_s3_wrapper), knowledge-ingest#42 (W-C3.1)

Architecture
------------
This module is a thin server-side trigger for the existing S3 connector wrapper
(connectors/aws_s3_wrapper.py). It:

  1. Accepts a scope + bucket reference + optional config overrides from the operator.
  2. Resolves S3 credentials server-side from environment variables using a secret_ref
     key — credentials are NEVER accepted from the browser or request body.
  3. Calls sync_s3_bucket() from the existing W-C2.1 wrapper.
  4. Returns the list of SyncResults.

All ingest logic flows through sync_s3_bucket() → ingest_from_connector() unchanged.
This route adds no business logic, no dedup, no scope validation, no DB writes.

Isolation contract
------------------
This module imports ONLY:
  - standard library (logging, os, typing)
  - fastapi / pydantic
  - connectors.aws_s3_wrapper (sync_s3_bucket)

It does NOT import db.registry, db.connector_sync, orchestrator, or storage.

Credential resolution
---------------------
Credentials must be pre-loaded into server-side environment variables.
The request supplies a `secret_ref` — a short alphanumeric key used to look up
the named credential pair. Resolution order:

  named:   S3_SECRET_<SECRET_REF>_ACCESS_KEY / S3_SECRET_<SECRET_REF>_SECRET_KEY
  default: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  (when secret_ref is "default"
           or empty, matching boto3 default chain behaviour)

Plaintext access_key or secret_key are never accepted in the request body.
"""

import logging
import os
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from connectors.aws_s3_wrapper import sync_s3_bucket

logger = logging.getLogger(__name__)

router = APIRouter()

_VALID_SECRET_REF = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
)


# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------

def _resolve_s3_credentials(secret_ref: str) -> dict:
    """Resolve S3 credentials from server-side environment variables.

    Args:
        secret_ref: Short alphanumeric key (e.g. "prod", "staging", "default").
                    "default" or "" uses the standard AWS_ACCESS_KEY_ID /
                    AWS_SECRET_ACCESS_KEY env vars (boto3 default chain).

    Returns:
        Dict with "access_key" and "secret_key" entries (both non-empty).

    Raises:
        HTTPException 503: If the credentials cannot be resolved from env.
    """
    ref = secret_ref.strip()
    if ref and ref.lower() != "default":
        env_prefix = f"S3_SECRET_{ref.upper()}"
        access_key = os.environ.get(f"{env_prefix}_ACCESS_KEY", "")
        secret_key = os.environ.get(f"{env_prefix}_SECRET_KEY", "")
        if not access_key or not secret_key:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"S3 credentials for secret_ref='{ref}' not found. "
                    f"Expected env vars: {env_prefix}_ACCESS_KEY and {env_prefix}_SECRET_KEY. "
                    f"Set them in the service environment before triggering a sync."
                ),
            )
    else:
        # Default: rely on boto3 default chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if not access_key or not secret_key:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Default S3 credentials not found. "
                    "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the service environment, "
                    "or provide a named secret_ref with S3_SECRET_<REF>_ACCESS_KEY / "
                    "S3_SECRET_<REF>_SECRET_KEY env vars."
                ),
            )
    return {"access_key": access_key, "secret_key": secret_key}


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class S3SyncRequest(BaseModel):
    """Request body for POST /connectors/s3/sync (W-C3.5).

    Credentials are NOT accepted in this request. Supply a secret_ref that
    maps to server-side environment variables (see module docstring).
    """

    scope: str
    bucket: str
    secret_ref: str = "default"
    prefix: str = ""
    region: str = ""
    endpoint_url: Optional[str] = None
    owner: str = ""

    @field_validator("scope", "bucket")
    @classmethod
    def validate_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("field must not be empty")
        return v

    @field_validator("secret_ref")
    @classmethod
    def validate_secret_ref(cls, v: str) -> str:
        ref = v.strip()
        if ref and ref.lower() != "default":
            invalid = set(ref) - _VALID_SECRET_REF
            if invalid:
                raise ValueError(
                    f"secret_ref must contain only letters, digits, underscores, or hyphens. "
                    f"Invalid characters: {''.join(sorted(invalid))}"
                )
        return ref or "default"


class S3SyncResultItem(BaseModel):
    source_uri: str
    verdict: str
    task_id: str
    error_message: Optional[str] = None


class S3SyncResponse(BaseModel):
    scope: str
    bucket: str
    total: int
    results: List[S3SyncResultItem]


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post("/connectors/s3/sync", status_code=200, response_model=S3SyncResponse)
async def s3_sync(body: S3SyncRequest) -> S3SyncResponse:
    """Trigger a server-side AWS S3 bucket sync into a knowledge scope.

    Resolves S3 credentials from server-side env vars using `secret_ref`.
    Calls sync_s3_bucket() from the W-C2.1 wrapper. All ingest logic flows
    through ingest_from_connector() — this route adds no business logic.

    Returns 200 with a list of SyncResult entries, one per S3 object attempted.
    Returns 503 if credentials cannot be resolved from env.
    Returns 422 for malformed request body.

    Auth: none — internal-network-only (same boundary as operator UI).
    """
    creds = _resolve_s3_credentials(body.secret_ref)

    connector_config = {
        "access_key": creds["access_key"],
        "secret_key": creds["secret_key"],
        "bucket_names": [body.bucket],
        "prefix": body.prefix,
        "scope": body.scope,
        "owner": body.owner,
    }
    if body.region:
        connector_config["region"] = body.region
    if body.endpoint_url:
        connector_config["endpoint_url"] = body.endpoint_url

    logger.info(
        "s3_sync: scope=%s bucket=%s prefix=%r secret_ref=%r",
        body.scope, body.bucket, body.prefix, body.secret_ref,
    )

    try:
        sync_results = await sync_s3_bucket(connector_config)
    except RuntimeError as exc:
        # Authentication failure from the wrapper — surface as 503
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        logger.error("s3_sync: unexpected error: %s: %s", type(exc).__name__, exc)
        raise HTTPException(status_code=500, detail=f"S3 sync error: {type(exc).__name__}")

    items = [
        S3SyncResultItem(
            source_uri=r.source_uri,
            verdict=r.verdict,
            task_id=r.task_id,
            error_message=r.error_message,
        )
        for r in sync_results
    ]

    logger.info(
        "s3_sync: completed scope=%s bucket=%s total=%d verdicts=%s",
        body.scope, body.bucket, len(items),
        {v: sum(1 for i in items if i.verdict == v) for v in {i.verdict for i in items}},
    )

    return S3SyncResponse(
        scope=body.scope,
        bucket=body.bucket,
        total=len(items),
        results=items,
    )
