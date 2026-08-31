# scripts/cleanup_legacy_storage.py
"""
Safe Cleanup Script for Legacy Storage Artifacts.
1. Cleans up local disk uploads/ directory.
2. Cleans up legacy un-prefixed GCS blobs in lg_custody_bucket.
"""

import os
import sys
import shutil
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from app.core.ai_integration import _get_gcs_client, GCS_BUCKET_NAME

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("storage_cleanup")


def cleanup_local_uploads():
    print("=" * 80)
    print("🧹 STEP 1: CLEANING LOCAL DISK uploads/ DIRECTORY")
    print("=" * 80)

    uploads_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "uploads"))
    if not os.path.exists(uploads_dir):
        print("  uploads/ directory does not exist. Nothing to clean.")
        return

    deleted_count = 0
    for root, dirs, files in os.walk(uploads_dir, topdown=False):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                os.remove(file_path)
                deleted_count += 1
                print(f"  🗑️ Deleted local file: {os.path.relpath(file_path, uploads_dir)}")
            except Exception as e:
                print(f"  ❌ Failed to delete {file_path}: {e}")

        for d in dirs:
            dir_path = os.path.join(root, d)
            try:
                os.rmdir(dir_path)
            except Exception:
                pass

    print(f"\n  ✅ Local disk cleanup finished: {deleted_count} files removed from uploads/.")


def cleanup_legacy_gcs_blobs():
    print("\n" + "=" * 80)
    print("🧹 STEP 2: CLEANING LEGACY UN-PREFIXED GCS BLOBS")
    print(f"Target GCS Bucket: {GCS_BUCKET_NAME}")
    print("=" * 80)

    client = _get_gcs_client()
    if not client:
        print("❌ GCS Client unavailable. Aborting GCS cleanup.")
        return

    bucket = client.bucket(GCS_BUCKET_NAME)

    # Legacy un-prefixed prefixes at the bucket root
    legacy_prefixes = [
        "customer_1/",
        "customer_2/",
        "customer_11/",
        "customer_21/",
        "customer_22/",
        "bank_forms/",
        "quotations/",
        "issuance/",
        "delivery_proofs/",
    ]

    deleted_gcs_count = 0
    skipped_count = 0

    for prefix in legacy_prefixes:
        print(f"\n--- Scanning legacy prefix: {prefix} ---")
        blobs = list(bucket.list_blobs(prefix=prefix))
        for blob in blobs:
            # Extra safety: ensure it's not under development/, staging/, or production/
            if blob.name.startswith(("development/", "staging/", "production/")):
                skipped_count += 1
                continue

            try:
                blob.delete()
                deleted_gcs_count += 1
                print(f"  🗑️ Deleted legacy blob: {blob.name}")
            except Exception as e:
                print(f"  ❌ Failed to delete blob {blob.name}: {e}")

    print("\n" + "=" * 80)
    print("📊 CLEANUP SUMMARY")
    print(f"Legacy GCS Blobs Deleted: {deleted_gcs_count}")
    print(f"Preserved Environment Blobs: {skipped_count}")
    print("=" * 80)


if __name__ == "__main__":
    cleanup_local_uploads()
    cleanup_legacy_gcs_blobs()
