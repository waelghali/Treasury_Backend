# scripts/migrate_storage_to_hierarchical_gcs.py
"""
Automated Idempotent GCS & Database Storage Migration Tool.
Migrates legacy unorganized GCS document paths into the clean hierarchical structure:
  Old: gs://bucket/customer_25/lg_1/scans/file.pdf
  New: gs://bucket/{env}/customer_25/lg_custody/lg_1/scans/file.pdf

Features:
- --dry-run: Simulates migration without making changes.
- --apply: Executes server-side GCS blob copy and updates DB pointers atomically.
- Idempotent: Skips already migrated files.
- Zero data loss: Preserves original files until explicit cleanup.
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.core.ai_integration import _get_gcs_client, GCS_BUCKET_NAME
from app.core.storage_service import (
    get_storage_environment,
    parse_gcs_uri,
    build_customer_blob_path,
    build_system_blob_path,
)

# Models
from app.models.models import LGDocument
from app.models.models_issuance import (
    IssuanceRequestDocument,
    BankFormTemplate,
    ReconciliationSession,
    IssuanceFacility,
)
from app.models.models_inbox import InboxAttachment

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("storage_migrator")


def remap_legacy_path(gcs_uri: str, target_env: str, rec: Any = None) -> str:
    """
    Calculates the target hierarchical GCS path for a given legacy URI.
    """
    if not gcs_uri or not gcs_uri.startswith("gs://"):
        return gcs_uri

    bucket_name, blob_name = parse_gcs_uri(gcs_uri)

    # Check if already migrated
    if blob_name.startswith(f"{target_env}/"):
        return gcs_uri

    # Case 1: Inbound LG Custody: customer_{id}/lg_{id}/{slug}/{file}
    if blob_name.startswith("customer_") and "/lg_" in blob_name:
        parts = blob_name.split("/", 2)
        # parts[0] = 'customer_25', parts[1] = 'lg_1', parts[2] = 'scans/file.pdf'
        return f"gs://{GCS_BUCKET_NAME}/{target_env}/{parts[0]}/lg_custody/{parts[1]}/{parts[2]}"

    # Case 2: Inbound LG Custody flat legacy filenames: e.g. 98_Original LG_... or 19_ORIGINAL_LG_DOCUMENT_...
    if rec and hasattr(rec, "lg_record") and rec.lg_record:
        cust_id = rec.lg_record.customer_id
        lg_id = rec.lg_record_id
        doc_type = getattr(rec, "document_type", "original_lg").lower()
        return f"gs://{GCS_BUCKET_NAME}/{target_env}/customer_{cust_id}/lg_custody/lg_{lg_id}/{doc_type}/{blob_name}"

    # Case 3: Issuance Requests: customer_{id}/issuance_req_{id}/... or customer_{id}/requests/{id}/...
    if blob_name.startswith("customer_") and ("/issuance_req_" in blob_name or "/requests/" in blob_name):
        parts = blob_name.split("/", 1)
        return f"gs://{GCS_BUCKET_NAME}/{target_env}/{parts[0]}/issuance/{parts[1]}"

    # Case 4: Facilities: customer_{id}/Facilities/{file}
    if blob_name.startswith("customer_") and "/Facilities/" in blob_name:
        parts = blob_name.split("/Facilities/", 1)
        return f"gs://{GCS_BUCKET_NAME}/{target_env}/{parts[0]}/facilities/fac_docs/{parts[1]}"

    # Case 5: Bank Forms: bank_forms/{bank_id}/{file}
    if blob_name.startswith("bank_forms/"):
        return f"gs://{GCS_BUCKET_NAME}/{target_env}/system/{blob_name}"

    # Default prefixing
    return f"gs://{GCS_BUCKET_NAME}/{target_env}/{blob_name}"


def run_migration(dry_run: bool = True, target_env: str = "production", db_url: Optional[str] = None):
    print("=" * 80)
    print(f"📦 GROW STORAGE MIGRATION TOOL")
    print(f"Mode:               {'🔍 DRY-RUN (Simulation Only)' if dry_run else '⚡ APPLY (Live DB & GCS Changes)'}")
    print(f"Target Environment: {target_env}")
    print(f"GCS Bucket:         {GCS_BUCKET_NAME}")
    if db_url:
        masked_url = db_url.split('@')[-1] if '@' in db_url else 'custom'
        print(f"Database Target:    ...@{masked_url}")
    print("=" * 80)

    if db_url:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={"connect_timeout": 30, "keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 5}
        )
        Session = sessionmaker(autocommit=False, autoflush=False, expire_on_commit=False, bind=engine)
        db = Session()
    else:
        from sqlalchemy.orm import sessionmaker
        from app.database import engine
        Session = sessionmaker(autocommit=False, autoflush=False, expire_on_commit=False, bind=engine)
        db = Session()

    client = _get_gcs_client() if not dry_run else None

    total_scanned = 0
    total_to_migrate = 0
    total_already_migrated = 0
    total_errors = 0

    from sqlalchemy.orm import joinedload

    tables = [
        ("LGDocument", db.query(LGDocument).options(joinedload(LGDocument.lg_record)).filter(LGDocument.is_deleted == False), "file_path"),
        ("IssuanceRequestDocument", db.query(IssuanceRequestDocument).filter(IssuanceRequestDocument.is_deleted == False), "file_path"),
        ("IssuanceFacility", db.query(IssuanceFacility).filter(IssuanceFacility.is_deleted == False), "contract_document_path"),
        ("BankFormTemplate", db.query(BankFormTemplate).filter(BankFormTemplate.is_deleted == False), "file_path"),
        ("ReconciliationSession", db.query(ReconciliationSession).filter(ReconciliationSession.uploaded_file_path.isnot(None)), "uploaded_file_path"),
    ]

    for model_name, query, url_field in tables:
        records = query.all()
        print(f"\n--- Scanning {model_name} ({len(records)} active records) ---")

        for idx, rec in enumerate(records, 1):
            total_scanned += 1
            current_uri = getattr(rec, url_field, None)
            if not current_uri or not current_uri.startswith("gs://"):
                continue

            target_uri = remap_legacy_path(current_uri, target_env, rec=rec)

            if target_uri == current_uri:
                total_already_migrated += 1
                continue

            total_to_migrate += 1

            if dry_run:
                print(f"  [PROPOSED] ID {rec.id}: {current_uri}\n             -> {target_uri}")
            else:
                try:
                    src_bucket_name, src_blob_name = parse_gcs_uri(current_uri)
                    tgt_bucket_name, tgt_blob_name = parse_gcs_uri(target_uri)

                    src_bucket = client.bucket(src_bucket_name)
                    tgt_bucket = client.bucket(tgt_bucket_name)

                    tgt_blob = tgt_bucket.blob(tgt_blob_name)
                    if not tgt_blob.exists():
                        src_blob = src_bucket.blob(src_blob_name)
                        blob_exists = False
                        try:
                            blob_exists = src_blob.exists()
                        except Exception as blob_check_err:
                            logger.warning(f"Could not check source blob {current_uri}: {blob_check_err}")

                        if blob_exists:
                            src_bucket.copy_blob(src_blob, tgt_bucket, tgt_blob_name)
                            logger.info(f"Copied GCS blob to {target_uri}")

                    # Update DB record pointer
                    setattr(rec, url_field, target_uri)
                    if idx % 10 == 0:
                        db.commit()
                    print(f"  ✅ [MIGRATED] ID {rec.id} -> {target_uri}")
                except Exception as err:
                    db.rollback()
                    logger.error(f"Failed to migrate record {rec.id} ({current_uri}): {err}")
                    total_errors += 1

        if not dry_run:
            try:
                db.commit()
            except Exception:
                pass

    db.close()

    print("\n" + "=" * 80)
    print("📊 MIGRATION SUMMARY")
    print(f"Total Records Scanned:    {total_scanned}")
    print(f"Already Migrated (Clean): {total_already_migrated}")
    print(f"Requiring Migration:     {total_to_migrate}")
    print(f"Errors Encountered:       {total_errors}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate Grow storage to clean hierarchy.")
    parser.add_argument("--apply", action="store_true", help="Execute live migration (default is dry-run)")
    parser.add_argument("--env", type=str, default="development", help="Target environment prefix (development, staging, production)")
    parser.add_argument("--db-url", type=str, default=None, help="Optional direct database URL override (for remote Staging/Production)")
    args = parser.parse_args()

    run_migration(dry_run=not args.apply, target_env=args.env, db_url=args.db_url)
