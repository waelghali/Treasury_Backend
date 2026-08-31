# scripts/verify_storage_architecture.py
"""
Automated Verification Suite for Cloud-Only Multi-Environment Storage Architecture.
Tests:
1. Hierarchy and path builder formatting (customer isolation & environment prefix).
2. Live GCS upload to development namespace.
3. Live GCS download and signed URL generation with HTTP verification.
4. Dual-read backward compatibility (resolution of legacy root paths).
5. Single-command customer data listing & extraction.
6. Verification that zero local files are written to uploads/.
7. Safe cleanup of test blobs.
"""

import os
import sys
import uuid
import asyncio
import urllib.request
import logging

# Ensure app root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("storage_verifier")

from app.core.storage_service import (
    get_storage_environment,
    build_customer_blob_path,
    build_system_blob_path,
    upload_bytes_to_gcs,
    download_bytes_from_gcs,
    generate_signed_url,
    delete_gcs_blob,
    list_customer_blobs,
    resolve_gcs_blob,
)
from app.core.ai_integration import GCS_BUCKET_NAME, _get_gcs_client

TEST_CUSTOMER_ID = 99999
TEST_FILE_UUID = uuid.uuid4().hex[:8]


async def run_verification_suite():
    print("=" * 80)
    print("🚀 GROW PLATFORM CLOUD STORAGE ARCHITECTURE VERIFICATION SUITE")
    print(f"Active Environment: {get_storage_environment()}")
    print(f"Target GCS Bucket:  {GCS_BUCKET_NAME}")
    print("=" * 80)

    passed_tests = 0
    total_tests = 6

    # -------------------------------------------------------------------------
    # TEST 1: Path Builder Formatting & Isolation
    # -------------------------------------------------------------------------
    print("\n[TEST 1] Testing Hierarchical Path Builders...")
    cust_path = build_customer_blob_path(TEST_CUSTOMER_ID, "lg_custody", "lg_101/scans/contract.pdf")
    expected_cust = f"development/customer_{TEST_CUSTOMER_ID}/lg_custody/lg_101/scans/contract.pdf"
    assert cust_path == expected_cust, f"Expected {expected_cust}, got {cust_path}"

    recon_pos_path = build_customer_blob_path(TEST_CUSTOMER_ID, "reconciliation/lg_positions", "session_5/report.xlsx")
    expected_pos = f"development/customer_{TEST_CUSTOMER_ID}/reconciliation/lg_positions/session_5/report.xlsx"
    assert recon_pos_path == expected_pos, f"Expected {expected_pos}, got {recon_pos_path}"

    recon_stmt_path = build_customer_blob_path(TEST_CUSTOMER_ID, "reconciliation/bank_statements", "stmt_12/stmt.mt940")
    expected_stmt = f"development/customer_{TEST_CUSTOMER_ID}/reconciliation/bank_statements/stmt_12/stmt.mt940"
    assert recon_stmt_path == expected_stmt, f"Expected {expected_stmt}, got {recon_stmt_path}"

    req_path = build_customer_blob_path(TEST_CUSTOMER_ID, "issuance/requests", "req_42/tender/specs.pdf")
    expected_req = f"development/customer_{TEST_CUSTOMER_ID}/issuance/requests/req_42/tender/specs.pdf"
    assert req_path == expected_req, f"Expected {expected_req}, got {req_path}"

    sys_path = build_system_blob_path("bank_forms", "bank_1/form.pdf")
    expected_sys = f"development/system/bank_forms/bank_1/form.pdf"
    assert sys_path == expected_sys, f"Expected {expected_sys}, got {sys_path}"

    print("  ✅ All customer & system path builders generated strictly isolated hierarchical URIs.")
    passed_tests += 1

    # -------------------------------------------------------------------------
    # TEST 2: Live GCS Upload to Development Hierarchy
    # -------------------------------------------------------------------------
    print("\n[TEST 2] Testing Live GCS Upload into Development Hierarchy...")
    test_data = f"GROW_STORAGE_TEST_PAYLOAD_{TEST_FILE_UUID}".encode("utf-8")
    test_blob = build_customer_blob_path(TEST_CUSTOMER_ID, "lg_custody", f"lg_1/scans/test_{TEST_FILE_UUID}.txt")
    
    gcs_uri = await upload_bytes_to_gcs(test_blob, test_data, content_type="text/plain")
    print(f"  Uploaded to: {gcs_uri}")
    assert gcs_uri == f"gs://{GCS_BUCKET_NAME}/{test_blob}", f"URI mismatch: {gcs_uri}"
    print("  ✅ Live upload into development namespace succeeded.")
    passed_tests += 1

    # -------------------------------------------------------------------------
    # TEST 3: Download & Signed URL Verification
    # -------------------------------------------------------------------------
    print("\n[TEST 3] Testing Download & Signed URL Generation...")
    downloaded_bytes, mime = await download_bytes_from_gcs(gcs_uri)
    assert downloaded_bytes == test_data, "Downloaded bytes do not match original payload!"
    print(f"  Direct GCS byte download verified ({len(downloaded_bytes)} bytes, mime: {mime})")

    signed_url = await generate_signed_url(gcs_uri, expiration_seconds=300)
    assert signed_url and signed_url.startswith("https://storage.googleapis.com"), f"Invalid signed URL: {signed_url}"
    print(f"  V4 Signed URL successfully generated.")

    # Test HTTP GET on the signed URL
    req = urllib.request.Request(signed_url, headers={"User-Agent": "GrowPlatformTest"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        http_data = resp.read()
        assert http_data == test_data, "HTTP signed URL download payload mismatch!"
        assert resp.status == 200, f"Expected HTTP 200, got {resp.status}"
    print("  ✅ HTTP GET on Signed URL returned 200 OK with identical payload.")
    passed_tests += 1

    # -------------------------------------------------------------------------
    # TEST 4: Dual-Read Backward Compatibility (Legacy Path Resolution)
    # -------------------------------------------------------------------------
    print("\n[TEST 4] Testing Dual-Read Backward Compatibility on Legacy Paths...")
    # Simulate a legacy un-prefixed file existing in the bucket root
    legacy_blob_name = f"customer_{TEST_CUSTOMER_ID}/legacy_lg/scan_{TEST_FILE_UUID}.txt"
    legacy_data = f"LEGACY_DATA_PAYLOAD_{TEST_FILE_UUID}".encode("utf-8")
    
    client = _get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    bucket.blob(legacy_blob_name).upload_from_string(legacy_data, content_type="text/plain")
    legacy_uri = f"gs://{GCS_BUCKET_NAME}/{legacy_blob_name}"
    print(f"  Created mock legacy un-prefixed blob: {legacy_uri}")

    # Read the legacy URI via our new download and signed URL methods
    read_legacy_bytes, _ = await download_bytes_from_gcs(legacy_uri)
    assert read_legacy_bytes == legacy_data, "Dual-read failed on legacy path!"
    print("  Dual-read direct download on legacy URI succeeded.")

    legacy_signed_url = await generate_signed_url(legacy_uri, expiration_seconds=300)
    assert legacy_signed_url and "storage.googleapis.com" in legacy_signed_url
    print("  Dual-read V4 signed URL for legacy URI generated successfully.")
    
    # Clean up mock legacy file
    delete_gcs_blob(legacy_uri)
    print("  ✅ Dual-read backward compatibility successfully verified.")
    passed_tests += 1

    # -------------------------------------------------------------------------
    # TEST 5: Customer-Isolated Data Listing & Extraction
    # -------------------------------------------------------------------------
    print("\n[TEST 5] Testing Customer-Isolated Data Listing...")
    # Upload a second file for the same customer
    test_blob_2 = build_customer_blob_path(TEST_CUSTOMER_ID, "inbox", f"msg_100/advice_{TEST_FILE_UUID}.txt")
    await upload_bytes_to_gcs(test_blob_2, b"INBOX_ADVICE_CONTENT", content_type="text/plain")

    customer_files = list_customer_blobs(TEST_CUSTOMER_ID)
    print(f"  Found {len(customer_files)} files under {get_storage_environment()}/customer_{TEST_CUSTOMER_ID}/:")
    for f in customer_files:
        print(f"    - {f['name']} ({f['size_bytes']} bytes)")
    assert len(customer_files) >= 2, f"Expected at least 2 customer files, found {len(customer_files)}"
    print("  ✅ Single-command tenant data extraction listing verified.")
    passed_tests += 1

    # -------------------------------------------------------------------------
    # TEST 6: Cleanup & Zero Local Disk Writes Confirmation
    # -------------------------------------------------------------------------
    print("\n[TEST 6] Cleaning up test blobs and verifying zero local filesystem writes...")
    delete_gcs_blob(gcs_uri)
    delete_gcs_blob(f"gs://{GCS_BUCKET_NAME}/{test_blob_2}")
    
    # Verify uploads/ has no stray test files
    uploads_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "uploads"))
    if os.path.exists(uploads_dir):
        stray_test_files = []
        for root, dirs, files in os.walk(uploads_dir):
            for file in files:
                if TEST_FILE_UUID in file:
                    stray_test_files.append(os.path.join(root, file))
        assert len(stray_test_files) == 0, f"Found stray local test files: {stray_test_files}"
    print("  Zero local test files created in uploads/.")
    print("  ✅ Cleanup completed.")
    passed_tests += 1

    print("\n" + "=" * 80)
    print(f"🎯 RESULTS: {passed_tests}/{total_tests} TESTS PASSED (100% SUCCESS)")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(run_verification_suite())
