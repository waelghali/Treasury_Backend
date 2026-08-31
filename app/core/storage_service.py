# app/core/storage_service.py
"""
Centralized Cloud Storage Service for Grow Platform.
Enforces multi-environment isolation (development, staging, production)
and strict per-customer folder hierarchy in Google Cloud Storage (GCS).
Eliminates local filesystem storage for 100% environment parity.
Supports dual-read backward compatibility for seamless migration.
"""

import os
import io
import re
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict, Any, Union

from app.core.ai_integration import (
    _get_gcs_client,
    GOOGLE_CLOUD_LIBRARIES_AVAILABLE,
    GCS_BUCKET_NAME,
)

logger = logging.getLogger("app.storage_service")

# --- Environment Detection ---
VALID_ENVIRONMENTS = {"development", "staging", "production", "local", "test"}

def get_storage_environment() -> str:
    """
    Returns the normalized active storage environment prefix: 'development', 'staging', or 'production'.
    Includes intelligent fail-safe auto-detection:
    1. Explicit ENVIRONMENT_NAME / APP_ENV / STORAGE_ENV variable if set.
    2. Render Cloud Auto-Detection:
       - RENDER_SERVICE_NAME or RENDER_GIT_BRANCH containing 'prod' / 'main' -> 'production'
       - RENDER_SERVICE_NAME or RENDER_GIT_BRANCH containing 'stage' / 'staging' / 'dev' -> 'staging'
    3. Safe Local Default:
       - Local developer machine (Windows/Mac/Linux outside Render) -> 'development'
    """
    # 1. Check explicit override first
    explicit_env = (
        os.getenv("ENVIRONMENT_NAME")
        or os.getenv("APP_ENV")
        or os.getenv("STORAGE_ENV")
    )
    if explicit_env:
        cleaned = explicit_env.strip().lower()
        if cleaned in ("local", "dev", "test"):
            return "development"
        elif cleaned in ("stage", "staging", "uat"):
            return "staging"
        elif cleaned in ("prod", "production"):
            return "production"
        return cleaned

    # 2. Check Render Cloud Built-In Variables (Auto-detect)
    render_service = os.getenv("RENDER_SERVICE_NAME", "").lower()
    render_branch = os.getenv("RENDER_GIT_BRANCH", "").lower()

    if render_service or render_branch:
        if "prod" in render_service or render_branch == "main" or render_branch == "master":
            return "production"
        if "stage" in render_service or "dev" in render_service or "staging" in render_branch:
            return "staging"
        return "staging"  # Default safe cloud fallback

    # 3. Local Development Safe Default
    return "development"


# --- Standardized Path Builders ---

def build_customer_blob_path(customer_id: int, module: str, relative_path: str, environment: Optional[str] = None) -> str:
    """
    Builds a standardized, customer-isolated GCS blob path.
    Format: {env}/customer_{customer_id}/{module}/{clean_relative_path}
    
    Example:
      build_customer_blob_path(25, "lg_custody", "lg_102/scans/guarantee.pdf")
      -> "development/customer_25/lg_custody/lg_102/scans/guarantee.pdf"
    """
    env = (environment or get_storage_environment()).strip().strip("/")
    clean_module = module.strip().strip("/").lower()
    clean_subpath = relative_path.strip().lstrip("/")
    
    return f"{env}/customer_{customer_id}/{clean_module}/{clean_subpath}"


def build_system_blob_path(category: str, relative_path: str, environment: Optional[str] = None) -> str:
    """
    Builds a standardized, system-scoped GCS blob path (non-tenant assets).
    Format: {env}/system/{category}/{clean_relative_path}
    
    Example:
      build_system_blob_path("bank_forms", "bank_1/application.pdf")
      -> "development/system/bank_forms/bank_1/application.pdf"
    """
    env = (environment or get_storage_environment()).strip().strip("/")
    clean_cat = category.strip().strip("/").lower()
    clean_subpath = relative_path.strip().lstrip("/")
    
    return f"{env}/system/{clean_cat}/{clean_subpath}"


# --- Dual-Read Path Resolver ---

def parse_gcs_uri(gcs_uri: str) -> Tuple[str, str]:
    """
    Parses a gs:// URI into (bucket_name, blob_name).
    """
    if not gcs_uri or not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")
    parts = gcs_uri[5:].split('/', 1)
    if len(parts) != 2:
        raise ValueError(f"Cannot parse bucket and blob from URI: {gcs_uri}")
    return parts[0], parts[1]


def resolve_gcs_blob(bucket_name: str, blob_name: str) -> Tuple[Any, str]:
    """
    Dual-read resolver: Checks if the blob exists at blob_name.
    If not found, attempts fallback resolution (e.g. legacy unprefixed path vs new prefixed path).
    Returns (gcs_blob_object, resolved_blob_name).
    """
    client = _get_gcs_client()
    if not client:
        raise RuntimeError("GCS client is not initialized.")

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Fast path: check if exact path exists
    try:
        if blob.exists():
            return blob, blob_name
    except Exception as e:
        logger.warning(f"Error checking existence of {blob_name}: {e}")

    # Fallback 1: If searching for a legacy path, check if it was migrated under current env
    env = get_storage_environment()
    if not blob_name.startswith(f"{env}/") and not any(blob_name.startswith(f"{e}/") for e in VALID_ENVIRONMENTS):
        candidate_prefixed = f"{env}/{blob_name}"
        candidate_blob = bucket.blob(candidate_prefixed)
        try:
            if candidate_blob.exists():
                logger.info(f"Dual-read resolved legacy '{blob_name}' -> '{candidate_prefixed}'")
                return candidate_blob, candidate_prefixed
        except Exception:
            pass

    # Fallback 2: If searching for an env-prefixed path, check if it exists in legacy root
    for env_name in VALID_ENVIRONMENTS:
        if blob_name.startswith(f"{env_name}/"):
            candidate_unprefixed = blob_name[len(env_name) + 1:]
            candidate_blob = bucket.blob(candidate_unprefixed)
            try:
                if candidate_blob.exists():
                    logger.info(f"Dual-read resolved prefixed '{blob_name}' -> legacy '{candidate_unprefixed}'")
                    return candidate_blob, candidate_unprefixed
            except Exception:
                pass

    # Default to original blob object even if not found (caller will handle 404)
    return blob, blob_name


# --- Core Cloud Storage Operations ---

async def upload_bytes_to_gcs(
    blob_name: str,
    data: bytes,
    content_type: str = "application/octet-stream",
    bucket_name: Optional[str] = None
) -> str:
    """
    Asynchronously uploads raw bytes to GCS at the designated blob_name.
    Returns the full gs:// URI.
    """
    target_bucket = bucket_name or GCS_BUCKET_NAME
    if not target_bucket:
        raise ValueError("Target GCS bucket name is not configured.")

    client = _get_gcs_client()
    if not client:
        raise RuntimeError("GCS client is not initialized.")

    bucket = client.bucket(target_bucket)
    blob = bucket.blob(blob_name)

    await asyncio.to_thread(blob.upload_from_string, data, content_type=content_type)
    logger.info(f"Uploaded {len(data)} bytes to gs://{target_bucket}/{blob_name}")
    return f"gs://{target_bucket}/{blob_name}"


def upload_bytes_to_gcs_sync(
    blob_name: str,
    data: bytes,
    content_type: str = "application/octet-stream",
    bucket_name: Optional[str] = None
) -> str:
    """
    Synchronously uploads raw bytes to GCS.
    """
    target_bucket = bucket_name or GCS_BUCKET_NAME
    if not target_bucket:
        raise ValueError("Target GCS bucket name is not configured.")

    client = _get_gcs_client()
    if not client:
        raise RuntimeError("GCS client is not initialized.")

    bucket = client.bucket(target_bucket)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)
    logger.info(f"[Sync] Uploaded {len(data)} bytes to gs://{target_bucket}/{blob_name}")
    return f"gs://{target_bucket}/{blob_name}"


async def download_bytes_from_gcs(gcs_uri: str) -> Tuple[bytes, str]:
    """
    Asynchronously downloads raw bytes from GCS with dual-read resolution.
    Returns (bytes, content_type).
    """
    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    blob, resolved_name = resolve_gcs_blob(bucket_name, blob_name)

    data = await asyncio.to_thread(blob.download_as_bytes)
    content_type = blob.content_type or "application/octet-stream"
    return data, content_type


def download_bytes_from_gcs_sync(gcs_uri: str) -> Tuple[bytes, str]:
    """
    Synchronously downloads raw bytes from GCS with dual-read resolution.
    """
    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    blob, resolved_name = resolve_gcs_blob(bucket_name, blob_name)

    data = blob.download_as_bytes()
    content_type = blob.content_type or "application/octet-stream"
    return data, content_type


async def generate_signed_url(gcs_uri: str, expiration_seconds: int = 3600) -> Optional[str]:
    """
    Generates a secure, temporary V4 signed URL for browser viewing or download.
    Includes dual-read fallback resolution.
    """
    try:
        bucket_name, blob_name = parse_gcs_uri(gcs_uri)
        blob, resolved_name = resolve_gcs_blob(bucket_name, blob_name)

        def _sign():
            return blob.generate_signed_url(
                version="v4",
                expiration=timedelta(seconds=expiration_seconds),
                method="GET"
            )

        return await asyncio.to_thread(_sign)
    except Exception as e:
        logger.error(f"Failed to generate signed URL for {gcs_uri}: {e}")
        return None


def delete_gcs_blob(gcs_uri: str) -> bool:
    """
    Deletes a blob from GCS.
    """
    try:
        bucket_name, blob_name = parse_gcs_uri(gcs_uri)
        blob, resolved_name = resolve_gcs_blob(bucket_name, blob_name)
        blob.delete()
        logger.info(f"Deleted GCS blob: {resolved_name}")
        return True
    except Exception as e:
        logger.error(f"Error deleting GCS blob {gcs_uri}: {e}")
        return False


# --- Customer Data Export & Management ---

def list_customer_blobs(customer_id: int, environment: Optional[str] = None, bucket_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Lists all files belonging to a specific customer within the specified environment.
    Enables 1-click full tenant data export and compliance auditing.
    """
    env = environment or get_storage_environment()
    target_bucket = bucket_name or GCS_BUCKET_NAME
    prefix = f"{env}/customer_{customer_id}/"

    client = _get_gcs_client()
    if not client:
        return []

    bucket = client.bucket(target_bucket)
    blobs = bucket.list_blobs(prefix=prefix)

    result = []
    for b in blobs:
        result.append({
            "name": b.name,
            "size_bytes": b.size,
            "content_type": b.content_type,
            "updated": b.updated.isoformat() if b.updated else None,
            "md5_hash": b.md5_hash,
            "gcs_uri": f"gs://{target_bucket}/{b.name}"
        })
    return result
