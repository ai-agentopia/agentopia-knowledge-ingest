"""Raw document storage.

Uses S3 when S3_BUCKET is configured; falls back to local filesystem otherwise.
Both paths implement the same interface: write and read artifact files under
the canonical key prefix  documents/{scope}/{document_id}/v{version}/.

S3 objects are written once and never overwritten (immutable per version).
Local filesystem files are also written once per run.
"""

import logging
import os
import pathlib

logger = logging.getLogger(__name__)


def _s3_prefix(scope: str, document_id: str, version: int) -> str:
    """Return the S3 key prefix for a document version.

    Scope may contain '/' (e.g. 'joblogic-kb/api-docs'), which S3 treats
    as a path delimiter — producing a human-browsable layout.
    """
    return f"documents/{scope}/{document_id}/v{version}/"


def write_original(
    scope: str,
    document_id: str,
    version: int,
    filename: str,
    data: bytes,
) -> str:
    """Store the original uploaded file and metadata.json. Returns S3/local key."""
    prefix = _s3_prefix(scope, document_id, version)
    # Determine extension from filename; default to .bin for unknown types
    ext = pathlib.Path(filename).suffix.lower() or ".bin"
    original_key = f"{prefix}original{ext}"

    _write(original_key, data)
    logger.info("store: wrote original %s (%d bytes)", original_key, len(data))
    return original_key


def write_normalized(
    scope: str,
    document_id: str,
    version: int,
    content: bytes,
) -> str:
    """Store normalized.json artifact. Returns key."""
    prefix = _s3_prefix(scope, document_id, version)
    key = f"{prefix}normalized.json"
    _write(key, content)
    logger.info("store: wrote normalized %s", key)
    return key


def write_extracted(
    scope: str,
    document_id: str,
    version: int,
    content: bytes,
) -> str:
    """Store extracted.json artifact. Returns key."""
    prefix = _s3_prefix(scope, document_id, version)
    key = f"{prefix}extracted.json"
    _write(key, content)
    logger.info("store: wrote extracted %s", key)
    return key


def read(key: str) -> bytes:
    """Read an artifact by key. Raises FileNotFoundError if not found."""
    from config import get_settings
    settings = get_settings()

    if settings.use_s3():
        return _s3_read(key, settings)
    return _local_read(key, settings.storage_local_path)


# ── Internal write dispatch ───────────────────────────────────────────────────


def _write(key: str, data: bytes) -> None:
    from config import get_settings
    settings = get_settings()
    if settings.use_s3():
        _s3_write(key, data, settings)
    else:
        _local_write(key, data, settings.storage_local_path)


# ── S3 backend ────────────────────────────────────────────────────────────────


def _s3_write(key: str, data: bytes, settings) -> None:
    import boto3

    kwargs: dict = {
        "region_name": settings.s3_region,
    }
    if settings.s3_endpoint_url:
        kwargs["endpoint_url"] = settings.s3_endpoint_url
    if settings.aws_access_key_id:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key

    s3 = boto3.client("s3", **kwargs)
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=data,
    )


def _s3_read(key: str, settings) -> bytes:
    import boto3

    kwargs: dict = {"region_name": settings.s3_region}
    if settings.s3_endpoint_url:
        kwargs["endpoint_url"] = settings.s3_endpoint_url
    if settings.aws_access_key_id:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key

    s3 = boto3.client("s3", **kwargs)
    response = s3.get_object(Bucket=settings.s3_bucket, Key=key)
    return response["Body"].read()


# ── Local filesystem backend ──────────────────────────────────────────────────


def _local_write(key: str, data: bytes, base_path: str) -> None:
    path = pathlib.Path(base_path) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _local_read(key: str, base_path: str) -> bytes:
    path = pathlib.Path(base_path) / key
    if not path.exists():
        raise FileNotFoundError(f"Artifact not found: {key}")
    return path.read_bytes()
