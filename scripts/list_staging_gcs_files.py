"""
Extract all GCS file paths from the staging database.
Run this script locally with STAGING_DATABASE_URL set in environment.

Usage:
  set STAGING_DATABASE_URL=postgresql://...
  python scripts/list_staging_gcs_files.py
"""
import os, sys, json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("STAGING_DATABASE_URL") or os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: Set STAGING_DATABASE_URL or DATABASE_URL env var")
    sys.exit(1)

engine = create_engine(DATABASE_URL)

# Every table.column that can hold a GCS gs:// path
QUERIES = [
    # --- models.py ---
    ("lg_documents",                "file_path",                    "SELECT id, file_path FROM lg_documents WHERE file_path IS NOT NULL AND file_path LIKE 'gs://%' AND is_deleted = false"),
    ("lg_records",                  "generated_content_path",       "SELECT id, generated_content_path FROM lg_records WHERE generated_content_path IS NOT NULL AND generated_content_path LIKE 'gs://%' AND is_deleted = false"),
    ("customers",                   "commercial_register_document_path", "SELECT id, commercial_register_document_path FROM customers WHERE commercial_register_document_path IS NOT NULL AND commercial_register_document_path LIKE 'gs://%' AND is_deleted = false"),
    ("notification_templates_sys",  "image_url",                    "SELECT id, image_url FROM notification_templates WHERE image_url IS NOT NULL AND image_url LIKE 'gs://%' AND is_deleted = false"),

    # --- models_issuance.py ---
    ("issued_lg_records",           "soft_copy_path",               "SELECT id, soft_copy_path FROM issued_lg_records WHERE soft_copy_path IS NOT NULL AND soft_copy_path LIKE 'gs://%' AND is_deleted = false"),
    ("issued_lg_records",           "handover_signed_copy_path",    "SELECT id, handover_signed_copy_path FROM issued_lg_records WHERE handover_signed_copy_path IS NOT NULL AND handover_signed_copy_path LIKE 'gs://%' AND is_deleted = false"),
    ("issued_lg_records",           "cancellation_notice->pdf_path","SELECT id, cancellation_notice->>'pdf_path' AS path FROM issued_lg_records WHERE cancellation_notice IS NOT NULL AND cancellation_notice->>'pdf_path' LIKE 'gs://%' AND is_deleted = false"),
    ("issuance_maintenance_actions","letter_generated_path",        "SELECT id, letter_generated_path FROM issuance_maintenance_actions WHERE letter_generated_path IS NOT NULL AND letter_generated_path LIKE 'gs://%' AND is_deleted = false"),
    ("issuance_maintenance_actions","delivery_document_path",       "SELECT id, delivery_document_path FROM issuance_maintenance_actions WHERE delivery_document_path IS NOT NULL AND delivery_document_path LIKE 'gs://%' AND is_deleted = false"),
    ("issuance_maintenance_actions","bank_reply_document_path",     "SELECT id, bank_reply_document_path FROM issuance_maintenance_actions WHERE bank_reply_document_path IS NOT NULL AND bank_reply_document_path LIKE 'gs://%' AND is_deleted = false"),
    ("bank_form_templates",         "file_path",                    "SELECT id, file_path FROM bank_form_templates WHERE file_path IS NOT NULL AND file_path LIKE 'gs://%' AND is_deleted = false"),
    ("issuance_request_documents",  "file_path",                    "SELECT id, file_path FROM issuance_request_documents WHERE file_path IS NOT NULL AND file_path LIKE 'gs://%' AND is_deleted = false"),
    ("reconciliation_sessions",     "uploaded_file_path",           "SELECT id, uploaded_file_path FROM reconciliation_sessions WHERE uploaded_file_path IS NOT NULL AND uploaded_file_path LIKE 'gs://%' AND is_deleted = false"),
    ("facilities",                  "contract_document_path",       "SELECT id, contract_document_path FROM facilities WHERE contract_document_path IS NOT NULL AND contract_document_path LIKE 'gs://%' AND is_deleted = false"),
    ("bank_form_issue_reports",     "attachment_path",              "SELECT id, attachment_path FROM bank_form_issue_reports WHERE attachment_path IS NOT NULL AND attachment_path LIKE 'gs://%' AND is_deleted = false"),

    # --- models_quotation.py ---
    ("quotation_rfqs",              "document_path",                "SELECT id, document_path FROM quotation_rfqs WHERE document_path IS NOT NULL AND document_path LIKE 'gs://%' AND is_deleted = false"),

    # --- models_inbox.py ---
    ("inbox_items",                 "primary_attachment_path",      "SELECT id, primary_attachment_path FROM inbox_items WHERE primary_attachment_path IS NOT NULL AND primary_attachment_path LIKE 'gs://%' AND is_deleted = false"),
    ("inbox_attachments",           "storage_path",                 "SELECT id, storage_path FROM inbox_attachments WHERE storage_path IS NOT NULL AND storage_path LIKE 'gs://%'"),

    # --- models_reconciliation_v2.py ---
    ("bank_statements",             "raw_file_path",                "SELECT id, raw_file_path FROM bank_statements WHERE raw_file_path IS NOT NULL AND raw_file_path LIKE 'gs://%'"),
]

all_paths = []
total = 0

with engine.connect() as conn:
    for table, column, query in QUERIES:
        try:
            rows = conn.execute(text(query)).fetchall()
            count = len(rows)
            if count > 0:
                print(f"\n{'='*60}")
                print(f"  {table}.{column}  ({count} file(s))")
                print(f"{'='*60}")
                for row in rows:
                    path = row[1]  # the path column
                    # Extract just the blob path (strip gs://bucket_name/)
                    blob = path
                    if path.startswith("gs://"):
                        parts = path[5:].split("/", 1)
                        if len(parts) == 2:
                            blob = parts[1]
                    print(f"  [{row[0]:>5}]  {blob}")
                    all_paths.append({
                        "table": table,
                        "column": column,
                        "record_id": row[0],
                        "full_gcs_uri": path,
                        "blob_path": blob
                    })
                total += count
        except Exception as e:
            print(f"  SKIP {table}.{column}: {e}")

print(f"\n{'='*60}")
print(f"  TOTAL: {total} GCS file(s) across all tables")
print(f"{'='*60}")

# Also output unique blob paths (the actual files to move)
unique_blobs = sorted(set(p["blob_path"] for p in all_paths))
print(f"\n  UNIQUE FILES TO MOVE: {len(unique_blobs)}")
print(f"  (Copy each from root to staging/ prefix)\n")
for b in unique_blobs:
    print(f"  {b}  -->  staging/{b}")

# Save to JSON for reference
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "staging_gcs_files.json")
with open(output_path, "w") as f:
    json.dump({"total": total, "unique_files": len(unique_blobs), "files": all_paths, "unique_blobs": unique_blobs}, f, indent=2)
print(f"\n  Full list saved to: {output_path}")
