"""
Update GCS paths in the staging database to add the 'staging/' prefix.

Run this script AFTER (or in tandem with) copying/moving the physical GCS files
from 'gs://lg_custody_bucket/<path>' to 'gs://lg_custody_bucket/staging/<path>'.

Usage:
  set STAGING_DATABASE_URL=postgresql://...
  python scripts/migrate_staging_gcs_paths_db.py --dry-run
  python scripts/migrate_staging_gcs_paths_db.py --apply
"""
import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text

def main():
    parser = argparse.ArgumentParser(description="Migrate staging GCS DB paths to staging/ prefix.")
    parser.add_argument("--apply", action="store_true", help="Execute changes in DB (default is dry-run)")
    args = parser.parse_args()

    db_url = os.environ.get("STAGING_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: Please set STAGING_DATABASE_URL or DATABASE_URL environment variable.")
        sys.exit(1)

    engine = create_engine(db_url)

    # Tables and columns where gs:// paths need 'staging/' prepended to the blob path
    # Example transformation:
    # gs://lg_custody_bucket/customer_1/... -> gs://lg_custody_bucket/staging/customer_1/...
    
    updates = [
        ("lg_documents", "file_path"),
        ("lg_records", "generated_content_path"),
        ("customers", "commercial_register_document_path"),
        ("notification_templates", "image_url"),
        ("issued_lg_records", "soft_copy_path"),
        ("issued_lg_records", "handover_signed_copy_path"),
        ("issuance_maintenance_actions", "letter_generated_path"),
        ("issuance_maintenance_actions", "delivery_document_path"),
        ("issuance_maintenance_actions", "bank_reply_document_path"),
        ("bank_form_templates", "file_path"),
        ("issuance_request_documents", "file_path"),
        ("reconciliation_sessions", "uploaded_file_path"),
        ("facilities", "contract_document_path"),
        ("bank_form_issue_reports", "attachment_path"),
        ("quotation_rfqs", "document_path"),
        ("inbox_items", "primary_attachment_path"),
        ("inbox_attachments", "storage_path"),
        ("bank_statements", "raw_file_path"),
    ]

    with engine.begin() as conn:
        print(f"=== Staging GCS Path DB Migration ({'APPLY MODE' if args.apply else 'DRY RUN'}) ===")
        total_updated = 0

        for table, col in updates:
            try:
                # Check if table and column exist and count eligible rows
                count_query = text(f"""
                    SELECT count(*) FROM {table}
                    WHERE {col} IS NOT NULL 
                      AND {col} LIKE 'gs://%'
                      AND {col} NOT LIKE 'gs://%/staging/%'
                """)
                count = conn.execute(count_query).scalar()
                print(f"- {table}.{col}: {count} row(s) to update")

                if count > 0 and args.apply:
                    # Regex replacement to insert staging/ right after bucket name
                    # gs://bucket_name/path -> gs://bucket_name/staging/path
                    update_query = text(f"""
                        UPDATE {table}
                        SET {col} = regexp_replace({col}, '^gs://([^/]+)/', 'gs://\\1/staging/')
                        WHERE {col} IS NOT NULL 
                          AND {col} LIKE 'gs://%'
                          AND {col} NOT LIKE 'gs://%/staging/%'
                    """)
                    res = conn.execute(update_query)
                    total_updated += res.rowcount
                elif count > 0:
                    total_updated += count
            except Exception as e:
                print(f"  [SKIPPED] {table}.{col}: {e}")

        # JSONB field in issued_lg_records: cancellation_notice->pdf_path
        try:
            json_count_query = text("""
                SELECT count(*) FROM issued_lg_records
                WHERE cancellation_notice IS NOT NULL
                  AND cancellation_notice->>'pdf_path' LIKE 'gs://%'
                  AND cancellation_notice->>'pdf_path' NOT LIKE 'gs://%/staging/%'
            """)
            json_count = conn.execute(json_count_query).scalar()
            print(f"- issued_lg_records.cancellation_notice (JSONB pdf_path): {json_count} row(s) to update")

            if json_count > 0 and args.apply:
                json_update_query = text("""
                    UPDATE issued_lg_records
                    SET cancellation_notice = jsonb_set(
                        cancellation_notice,
                        '{pdf_path}',
                        to_jsonb(regexp_replace(cancellation_notice->>'pdf_path', '^gs://([^/]+)/', 'gs://\\1/staging/'))
                    )
                    WHERE cancellation_notice IS NOT NULL
                      AND cancellation_notice->>'pdf_path' LIKE 'gs://%'
                      AND cancellation_notice->>'pdf_path' NOT LIKE 'gs://%/staging/%'
                """)
                res = conn.execute(json_update_query)
                total_updated += res.rowcount
            elif json_count > 0:
                total_updated += json_count
        except Exception as e:
            print(f"  [SKIPPED] issued_lg_records.cancellation_notice: {e}")

        print(f"\nTotal rows impacted: {total_updated}")
        if not args.apply:
            print("\nRun with --apply to commit these changes to the database.")

if __name__ == "__main__":
    main()
