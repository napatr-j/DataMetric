"""
Generic MinIO uploader for social-media pipeline data.

Object path convention:
    {bucket}/{platform}/{account_name}/{YYYY-MM-DD}.json

This module is intentionally platform-agnostic so that facebook/instagram/
tiktok scrapers can call upload_json() without any changes here.

Required environment variables
───────────────────────────────
    MINIO_ENDPOINT      e.g. "minio:9000"  (no scheme)
    MINIO_ACCESS_KEY    e.g. "admin"
    MINIO_SECRET_KEY    e.g. "password123"

Optional environment variables
───────────────────────────────
    MINIO_BUCKET              default "social-media-data"
    MINIO_SECURE              "true" / "false"  (default "false")
    MINIO_AUTO_CREATE_BUCKET  "true" / "false"  (default "true")
"""
from __future__ import annotations

import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "")
_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
_DEFAULT_BUCKET = os.environ.get("MINIO_BUCKET", "social-media-data")
_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
_AUTO_CREATE_BUCKET = os.environ.get("MINIO_AUTO_CREATE_BUCKET", "true").lower() == "true"


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class MinIOUploadError(Exception):
    """Raised when a MinIO upload fails."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_client() -> "Minio":  # noqa: F821  (Minio imported lazily)
    """
    Validate env vars and return a configured Minio client.

    Raises:
        MinIOUploadError: If any required variable is missing.
    """
    from minio import Minio  # local import so module loads without minio installed

    missing = [
        name
        for name, val in [
            ("MINIO_ENDPOINT", _ENDPOINT),
            ("MINIO_ACCESS_KEY", _ACCESS_KEY),
            ("MINIO_SECRET_KEY", _SECRET_KEY),
        ]
        if not val
    ]
    if missing:
        raise MinIOUploadError(
            f"Missing required MinIO environment variable(s): {', '.join(missing)}"
        )

    logger.debug("Creating MinIO client | endpoint=%s | secure=%s", _ENDPOINT, _SECURE)
    return Minio(
        _ENDPOINT,
        access_key=_ACCESS_KEY,
        secret_key=_SECRET_KEY,
        secure=_SECURE,
    )


def _ensure_bucket(client: "Minio", bucket: str) -> None:  # noqa: F821
    """
    Create *bucket* if it does not already exist.

    Raises:
        MinIOUploadError: If the bucket cannot be created.
    """
    from minio.error import S3Error

    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            logger.info("Created MinIO bucket: %s", bucket)
        else:
            logger.debug("Bucket already exists: %s", bucket)
    except S3Error as exc:
        raise MinIOUploadError(
            f"Failed to ensure bucket '{bucket}' exists: {exc}"
        ) from exc


def _sanitize_account(account_name: str) -> str:
    """Strip leading @ and surrounding whitespace from an account identifier."""
    return account_name.lstrip("@").strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def upload_json(
    platform: str,
    account_name: str,
    json_data: dict,
    bucket: Optional[str] = None,
    date_str: Optional[str] = None,
) -> dict:
    """
    Serialize *json_data* and upload it to MinIO.

    Object path: ``{platform}/{account_name}/{date_str}.json``

    Args:
        platform:     Social media platform identifier ("youtube", "instagram", …).
        account_name: Channel / account identifier used as the folder name.
                      A leading ``@`` is stripped automatically.
        json_data:    Python dict to serialise and upload.
        bucket:       MinIO bucket name.  Defaults to ``MINIO_BUCKET`` env var
                      (fallback: ``"social-media-data"``).
        date_str:     Date partition string (``YYYY-MM-DD``).
                      Defaults to today's UTC date.

    Returns:
        Dict with keys:
          - bucket, object_path, full_path
          - size_bytes, uploaded_at
          - platform, account_name

    Raises:
        MinIOUploadError: If validation fails or the upload cannot complete.
    """
    from minio.error import S3Error

    # ── Input validation ────────────────────────────────────────────────────
    if not platform:
        raise MinIOUploadError("'platform' must not be empty")
    if not account_name:
        raise MinIOUploadError("'account_name' must not be empty")
    if not json_data:
        raise MinIOUploadError("'json_data' must not be empty")

    bucket = bucket or _DEFAULT_BUCKET
    date_str = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    safe_account = _sanitize_account(account_name)
    object_path = f"{platform}/{safe_account}/{date_str}.json"

    logger.info(
        "Preparing upload | bucket=%s | object=%s", bucket, object_path
    )

    t0 = time.monotonic()

    try:
        client = _build_client()

        if _AUTO_CREATE_BUCKET:
            _ensure_bucket(client, bucket)

        payload = json.dumps(json_data, ensure_ascii=False, indent=2).encode("utf-8")
        size_bytes = len(payload)
        logger.info(
            "Uploading %d bytes → s3://%s/%s", size_bytes, bucket, object_path
        )

        client.put_object(
            bucket_name=bucket,
            object_name=object_path,
            data=io.BytesIO(payload),
            length=size_bytes,
            content_type="application/json",
        )

        # ── Validate by stat-ing the uploaded object ────────────────────────
        stat = client.stat_object(bucket, object_path)
        if stat.size != size_bytes:
            raise MinIOUploadError(
                f"Upload size mismatch: sent {size_bytes} B, server reports {stat.size} B"
            )

        elapsed = time.monotonic() - t0
        uploaded_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Upload complete in %.2fs | path=s3://%s/%s | size=%d B | etag=%s",
            elapsed,
            bucket,
            object_path,
            size_bytes,
            stat.etag,
        )

        return {
            "bucket": bucket,
            "object_path": object_path,
            "full_path": f"s3://{bucket}/{object_path}",
            "size_bytes": size_bytes,
            "uploaded_at": uploaded_at,
            "platform": platform,
            "account_name": safe_account,
        }

    except MinIOUploadError:
        raise
    except S3Error as exc:
        elapsed = time.monotonic() - t0
        logger.exception(
            "S3Error after %.2fs | bucket=%s | object=%s", elapsed, bucket, object_path
        )
        raise MinIOUploadError(
            f"MinIO S3 error uploading '{object_path}': {exc}"
        ) from exc
    except Exception as exc:
        elapsed = time.monotonic() - t0
        logger.exception(
            "Unexpected error after %.2fs | bucket=%s | object=%s",
            elapsed,
            bucket,
            object_path,
        )
        raise MinIOUploadError(
            f"Unexpected error uploading '{object_path}': {exc}"
        ) from exc
