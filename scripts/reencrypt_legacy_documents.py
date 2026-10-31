# scripts/reencrypt_legacy_documents.py
"""
Grow Treasury — Zero-Downtime Legacy Document Re-Encryption Utility.
Sub-Phase 8.7B: Cross-Module Document Encryption & Memory-Safe Zero-Buffer Streaming.

Scans historical unencrypted documents stored in Google Cloud Storage across:
1. LG Custody (`LGDocument`) -> Encrypted with Tenant DEK (`context="lg_custody_doc"`)
2. LG Issuance (`IssuanceRequestDocument`) -> Encrypted with Tenant DEK (`context="lg_issuance_doc"`)
3. Issued LG Records (`IssuedLGRecord.handover_signed_copy_path`, `soft_copy_path`) -> Encrypted with Tenant DEK (`context="lg_issuance_doc"`)

Usage:
  python scripts/reencrypt_legacy_documents.py --dry-run
  python scripts/reencrypt_legacy_documents.py --module all --batch-size 50
  python scripts/reencrypt_legacy_documents.py --module custody
  python scripts/reencrypt_legacy_documents.py --module issuance
"""

import os
import sys
import io
import argparse
import logging
from typing import Optional, List, Dict, Any

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy.orm import Session
from app.database import engine
from app.models.models import LGDocument, LGRecord, Customer
from app.models.models_issuance import IssuanceRequestDocument, IssuanceRequest, IssuedLGRecord
from app.services.tenant_key_service import tenant_key_service
from app.core.security_crypto import STREAM_MAGIC
from app.core.storage_service import (
    _get_gcs_client,
    open_gcs_blob_stream,
    upload_stream_to_gcs_encrypted,
    GCS_BUCKET_NAME,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("reencrypt_legacy_documents")


def parse_gcs_uri(gcs_uri: str):
    """Splits 'gs://bucket/blob_name' into (bucket_name, blob_name)."""
    if not gcs_uri or not gcs_uri.startswith("gs://"):
        return None, None
    clean = gcs_uri[5:]
    parts = clean.split("/", 1)
    if len(parts) != 2:
        return None, None
    return parts[0], parts[1]


def check_is_encrypted(bucket_name: str, blob_name: str, client) -> bool:
    """Checks the first 14 bytes of a GCS blob to determine if it is STREAM V2 encrypted."""
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return True  # Skip missing blobs
        # Read first 14 bytes
        header = blob.download_as_bytes(start=0, end=len(STREAM_MAGIC) - 1)
        return header == STREAM_MAGIC
    except Exception as e:
        logger.warning(f"Could not inspect header for gs://{bucket_name}/{blob_name}: {e}")
        return True  # Skip on error to avoid double encryption


def reencrypt_blob(
    db: Session,
    client,
    gcs_uri: str,
    customer_id: int,
    context: str,
    content_type: str = "application/pdf",
    dry_run: bool = False
) -> bool:
    """Streams unencrypted blob through upload_stream_to_gcs_encrypted, replacing original."""
    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    if not bucket_name or not blob_name:
        return False

    if check_is_encrypted(bucket_name, blob_name, client):
        logger.debug(f"Skipping already encrypted or missing blob: {gcs_uri}")
        return False

    if dry_run:
        logger.info(f"[DRY RUN] Would re-encrypt: {gcs_uri} (Customer {customer_id}, Context {context})")
        return True

    try:
        tenant_dek = tenant_key_service.get_or_create_tenant_dek(db, customer_id)
        bucket = client.bucket(bucket_name)
        src_blob = bucket.blob(blob_name)

        # 1. Open streaming reader on unencrypted source blob
        source_stream, detected_mime, size_bytes = open_gcs_blob_stream(gcs_uri)
        eff_mime = detected_mime or content_type or "application/pdf"

        # 2. Upload to temporary encrypted blob
        temp_blob_name = f"{blob_name}.enc_temp"
        upload_stream_to_gcs_encrypted(
            source_stream=source_stream,
            destination_blob_name=temp_blob_name,
            tenant_dek=tenant_dek,
            content_type=eff_mime,
            bucket_name=bucket_name,
            context=context
        )

        # 3. Atomically overwrite original blob with encrypted version
        temp_blob = bucket.blob(temp_blob_name)
        bucket.copy_blob(temp_blob, bucket, blob_name)
        temp_blob.delete()

        logger.info(f"Successfully re-encrypted: {gcs_uri} ({size_bytes} plain bytes)")
        return True

    except Exception as e:
        logger.error(f"Failed to re-encrypt {gcs_uri}: {e}", exc_info=True)
        return False


def process_custody_documents(db: Session, client, batch_size: int, dry_run: bool) -> int:
    """Scans and re-encrypts LG Custody documents."""
    logger.info("Scanning LG Custody documents (LGDocument)...")
    docs = db.query(
        LGDocument.file_path,
        LGRecord.customer_id
    ).join(
        LGRecord, LGRecord.id == LGDocument.lg_record_id
    ).filter(
        LGDocument.file_path.like("gs://%"),
        LGDocument.is_deleted == False
    ).all()

    logger.info(f"Found {len(docs)} LG Custody documents in GCS.")
    reencrypted = 0

    for file_path, cust_id in docs:
        if not cust_id or not file_path:
            continue
        success = reencrypt_blob(
            db=db,
            client=client,
            gcs_uri=file_path,
            customer_id=cust_id,
            context="lg_custody_doc",
            dry_run=dry_run
        )
        if success:
            reencrypted += 1

    return reencrypted


def process_issuance_documents(db: Session, client, batch_size: int, dry_run: bool) -> int:
    """Scans and re-encrypts LG Issuance documents and issued LG copies."""
    logger.info("Scanning LG Issuance request documents (IssuanceRequestDocument)...")
    reencrypted = 0

    # 1. Request Documents
    req_docs = db.query(
        IssuanceRequestDocument.file_path,
        IssuanceRequest.customer_id
    ).join(
        IssuanceRequest, IssuanceRequest.id == IssuanceRequestDocument.request_id
    ).filter(
        IssuanceRequestDocument.file_path.like("gs://%"),
        IssuanceRequestDocument.is_deleted == False
    ).all()

    logger.info(f"Found {len(req_docs)} Issuance Request documents in GCS.")
    for file_path, cust_id in req_docs:
        if not cust_id or not file_path:
            continue
        if reencrypt_blob(db, client, file_path, cust_id, "lg_issuance_doc", dry_run=dry_run):
            reencrypted += 1

    # 2. Issued LG Records (Signed delivery copies, bank copies, and cancellation notices)
    issued_records = db.query(IssuedLGRecord).all()

    logger.info(f"Scanning {len(issued_records)} Issued LG records for GCS attachments...")
    for rec in issued_records:
        if rec.handover_signed_copy_path and rec.handover_signed_copy_path.startswith("gs://"):
            if reencrypt_blob(db, client, rec.handover_signed_copy_path, rec.customer_id, "lg_issuance_doc", dry_run=dry_run):
                reencrypted += 1
        if rec.soft_copy_path and rec.soft_copy_path.startswith("gs://"):
            if reencrypt_blob(db, client, rec.soft_copy_path, rec.customer_id, "lg_issuance_doc", dry_run=dry_run):
                reencrypted += 1
        if rec.cancellation_notice and isinstance(rec.cancellation_notice, dict):
            cancel_path = rec.cancellation_notice.get("pdf_path")
            if cancel_path and cancel_path.startswith("gs://"):
                if reencrypt_blob(db, client, cancel_path, rec.customer_id, "lg_issuance_doc", dry_run=dry_run):
                    reencrypted += 1

    return reencrypted


def process_facility_documents(db: Session, client, batch_size: int, dry_run: bool) -> int:
    """Scans and re-encrypts Credit Facility agreement documents."""
    from app.models.models_issuance import IssuanceFacility
    logger.info("Scanning Credit Facility agreements (IssuanceFacility)...")
    reencrypted = 0

    facs = db.query(IssuanceFacility).filter(
        IssuanceFacility.contract_document_path.like("gs://%")
    ).all()

    logger.info(f"Found {len(facs)} Facility agreements in GCS.")
    for fac in facs:
        if not fac.customer_id or not fac.contract_document_path:
            continue
        if reencrypt_blob(db, client, fac.contract_document_path, fac.customer_id, "facility_doc", dry_run=dry_run):
            reencrypted += 1

    return reencrypted


def main():
    parser = argparse.ArgumentParser(description="Zero-Downtime Document Re-Encryption Utility")
    parser.add_argument("--dry-run", action="store_true", help="Preview documents without encrypting")
    parser.add_argument("--module", choices=["custody", "issuance", "facilities", "all"], default="all", help="Target module")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for processing")
    args = parser.parse_args()

    client = _get_gcs_client()
    if not client:
        logger.error("Could not initialize Google Cloud Storage client. Please check credentials.")
        sys.exit(1)

    with Session(engine) as db:
        total_reencrypted = 0
        if args.module in ("custody", "all"):
            total_reencrypted += process_custody_documents(db, client, args.batch_size, args.dry_run)
        if args.module in ("issuance", "all"):
            total_reencrypted += process_issuance_documents(db, client, args.batch_size, args.dry_run)
        if args.module in ("facilities", "all"):
            total_reencrypted += process_facility_documents(db, client, args.batch_size, args.dry_run)

        mode_str = "DRY RUN COMPLETE: Would re-encrypt" if args.dry_run else "RE-ENCRYPTION COMPLETE: Processed"
        logger.info(f"============================================================")
        logger.info(f"{mode_str} {total_reencrypted} document(s).")
        logger.info(f"============================================================")


if __name__ == "__main__":
    main()

