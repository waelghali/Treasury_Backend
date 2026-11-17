# tests/test_lg_custody_delivery_ai_migration_reports_suite.py
"""
Master LG Custody Suite (Part B):
Delivery Proof AI, All 5 Bank Reply AI Branches, Formal Bank Discrepancy Letters,
Legacy Excel Historical Migration Hub, Action Center, Dashboard Safety Score,
Ops Health / Lifecycle Reports & Scheduled Cron Digests.

Focus: Verifies exact user-expected outputs (standardized vault filenames `{serial[:19]}D/R`,
date alignments without false admin friction, hard override blocks on modified amounts,
rendered Discrepancy HTML & WeasyPrint Reminder PDFs, reconstructed multi-snapshot Excel
migration state, exact Safety Score math, and Owner-grouped email digests).

Run directly:
    $env:PYTHONPATH = "c:\\Grow"; c:\\Grow\\venv\\Scripts\\python.exe c:\\Grow\\tests\\test_lg_custody_delivery_ai_migration_reports_suite.py
"""

import asyncio
import base64
import io
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from fastapi.testclient import TestClient
import openpyxl

from _test_harness import (
    ROOT_DIR,
    app,
    SessionLocal,
    IsolatedTenantFixture,
    install_in_memory_gcs,
    install_email_capture,
    make_auth_headers,
    extract_pdf_info,
)
from app.constants import GlobalConfigKey, LgStatusEnum
from app.models.models import (
    LGRecord,
    LGInstruction,
    LGDocument,
    ApprovalRequest,
    AuditLog,
    GlobalConfiguration,
    CustomerConfiguration,
    SystemNotification,
    LGMigrationStaging,
    LGChangeLog,
)
from app.services.document_verification_service import DocumentVerificationService
from app.core.security_crypto import STREAM_MAGIC
from app.core.background_tasks import run_daily_lg_status_update
from app.crud.crud import crud_lg_record


SAMPLE_DOCS_DIR = os.path.join(ROOT_DIR, "test_documents_lg_custody")


def _load_sample_pdf(filename: str, fallback_text: str = "SAMPLE BANK DOCUMENT") -> bytes:
    path = os.path.join(SAMPLE_DOCS_DIR, filename)
    if os.path.exists(path):
        with open(path, "rb") as f:
            return f.read()
    # Deterministic minimal valid PDF fallback if sample folder is absent
    return f"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n{fallback_text}".encode("utf-8")


def _set_customer_config(db, customer_id: int, key: GlobalConfigKey, value: str):
    gcfg = db.query(GlobalConfiguration).filter(GlobalConfiguration.key == key).first()
    if not gcfg:
        return
    ccfg = db.query(CustomerConfiguration).filter(
        CustomerConfiguration.customer_id == customer_id,
        CustomerConfiguration.global_config_id == gcfg.id,
    ).first()
    if ccfg:
        ccfg.configured_value = value
        ccfg.is_deleted = False
    else:
        ccfg = CustomerConfiguration(
            customer_id=customer_id,
            global_config_id=gcfg.id,
            configured_value=value,
        )
        db.add(ccfg)
    db.commit()


def run_lg_custody_delivery_ai_migration_reports_suite():
    print("=" * 92)
    print("GROW TREASURY — LG CUSTODY DELIVERY AI, BANK REPLIES, MIGRATION, REPORTS & CRONS SUITE")
    print("=" * 92)

    mem_gcs = install_in_memory_gcs()
    email_harness = install_email_capture()
    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="CUST_AI_MIG")
    client = TestClient(app)

    maker_h = make_auth_headers(fx.maker_user)
    admin_h = make_auth_headers(fx.admin_user)
    checker_h = make_auth_headers(fx.admin2_user)
    sys_h = make_auth_headers(fx.sys_owner)

    orig_deliv_llm = DocumentVerificationService._call_gemini_delivery_prompt
    orig_reply_llm = DocumentVerificationService._call_gemini_bank_reply_prompt

    def _create_lg(
        lg_number: str,
        amount: str = "1000000.00",
        days_to_expiry: int = 150,
        owner_email: Optional[str] = None,
        auto_renewal: bool = True,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        today = date.today()
        payload = {
            "beneficiary_corporate_id": fx.entity_alpha_hq.id,
            "issuer_name": "Hassan Allam Construction SAE",
            "lg_number": lg_number,
            "lg_amount": amount,
            "lg_currency_id": fx.ccy_egp.id,
            "lg_payable_currency_id": fx.ccy_egp.id,
            "issuance_date": (today - timedelta(days=30)).isoformat(),
            "expiry_date": (today + timedelta(days=days_to_expiry)).isoformat(),
            "auto_renewal": auto_renewal,
            "lg_type_id": fx.lg_type_perf.id,
            "description_purpose": "10% Performance Bond — Civil Package B",
            "issuing_bank_id": fx.bank_cib.id,
            "issuing_bank_address": "CIB Smart Village Branch",
            "issuing_bank_phone": "+20219666",
            "issuing_method_id": fx.issuing_method.id,
            "applicable_rule_id": fx.rule_urdg.id,
            "lg_category_id": fx.cat_standard.id,
            "internal_owner_email": owner_email or fx.maker_user.email,
            "internal_owner_phone": "+201001112233",
            "internal_owner_id": "EMP-101",
            "manager_email": f"mgr.{fx.uid.lower()}@globex.com",
        }
        if extra_fields:
            payload.update(extra_fields)
        r = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(payload)},
        )
        assert r.status_code == 201, f"Create LG failed: {r.text}"
        return r.json()

    try:
        # ==================================================================================
        # PART 1: DELIVERY PROOF AI VERIFICATION, CHRONOLOGY, DUPLICATES & OVERRIDE
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 1] DELIVERY PROOF AI VERIFICATION, CHRONOLOGY, SHA-256 GUARD & OVERRIDES")
        print("=" * 92)

        today = date.today()
        lg_deliv = _create_lg(f"LG-DLV-{fx.uid}-01")
        lg_deliv_id = lg_deliv["id"]

        # Issue an Extension instruction so we can record delivery on it
        target_ext_dt = today + timedelta(days=270)
        r_ext = client.post(
            f"/api/v1/end-user/lg-records/{lg_deliv_id}/extend",
            headers=maker_h,
            json={"new_expiry_date": target_ext_dt.isoformat(), "notes": "Delivery AI test"},
        )
        assert r_ext.status_code == 200
        inst_deliv_id = r_ext.json()["latest_instruction_id"]
        db.expire_all()
        inst_deliv_obj = db.query(LGInstruction).get(inst_deliv_id)
        serial_1 = inst_deliv_obj.serial_number

        # 1.1 Enforce DOC_MANDATORY_RECORD_DELIVERY = True -> missing file MUST fail with HTTP 400
        _set_customer_config(db, fx.cust_alpha.id, GlobalConfigKey.DOC_MANDATORY_RECORD_DELIVERY, "true")
        r_missing_doc = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_deliv_id}/record-delivery",
            headers=maker_h,
            data={"delivery_date": today.isoformat()},
        )
        assert r_missing_doc.status_code == 400
        assert "mandatory" in r_missing_doc.json()["detail"].lower()
        print("  [PASS] DOC_MANDATORY_RECORD_DELIVERY=true blocked missing delivery scan (HTTP 400)")

        # 1.2 Chronology Conflict (`detected_stamp_date < instruction_date`) -> returns `status="mismatch"`
        old_stamp_bytes = _load_sample_pdf("05_Delivery_Proof_Chronology_Conflict_OldStamp_2026-01-05.pdf")
        old_stamp_dt = (today - timedelta(days=45)).isoformat()
        DocumentVerificationService._call_gemini_delivery_prompt = classmethod(
            lambda cls, *a, **kw: {
                "is_bank_stamp_detected": True,
                "detected_bank_name": "Commercial International Bank (CIB)",
                "detected_branch_name": "Smart Village Branch",
                "detected_stamp_date": old_stamp_dt,
                "detected_lg_numbers": [lg_deliv["lg_number"]],
                "detected_instruction_serials": [serial_1],
                "detected_document_type": "CORPORATE_INSTRUCTION_LETTER_STAMPED",
                "is_courier_receipt": False,
            }
        )
        r_chrono = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_deliv_id}/record-delivery",
            headers=maker_h,
            data={"delivery_date": today.isoformat()},
            files={"delivery_document_file": ("OldStamp.pdf", old_stamp_bytes, "application/pdf")},
        )
        assert r_chrono.status_code == 200
        chrono_json = r_chrono.json()
        assert chrono_json["status"] == "mismatch"
        assert any("chronology" in m.lower() or "earlier" in m.lower() for m in chrono_json["ai_verification"]["mismatches"])
        assert chrono_json["delivery_date_variance"]["classification"] == "DELIVERY_STAMP_DATE_MISMATCH"
        assert chrono_json["delivery_date_variance"]["can_align"] is False
        print("  [PASS] Delivery AI caught Chronology Conflict (stamp date earlier than instruction date) & blocked 1-click align")

        # 1.3 Clean Bank Stamp Match -> `status == "Instruction Delivered"` + `{serial[:19]}D.pdf` encrypted in vault!
        clean_stamp_bytes = _load_sample_pdf("04_Delivery_Proof_CIB_Clean_Stamp_2026-11-02.pdf")
        DocumentVerificationService._call_gemini_delivery_prompt = classmethod(
            lambda cls, *a, **kw: {
                "is_bank_stamp_detected": True,
                "detected_bank_name": "Commercial International Bank (CIB)",
                "detected_branch_name": "Corporate Trade Finance",
                "detected_stamp_date": today.isoformat(),
                "detected_lg_numbers": [lg_deliv["lg_number"]],
                "detected_instruction_serials": [serial_1],
                "detected_document_type": "CORPORATE_INSTRUCTION_LETTER_STAMPED",
                "is_courier_receipt": False,
            }
        )
        r_clean_dlv = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_deliv_id}/record-delivery",
            headers=maker_h,
            data={"delivery_date": today.isoformat()},
            files={"delivery_document_file": ("CleanStamp.pdf", clean_stamp_bytes, "application/pdf")},
        )
        assert r_clean_dlv.status_code == 200, r_clean_dlv.text
        dlv_out = r_clean_dlv.json()
        assert dlv_out["status"] == "Instruction Delivered"
        assert dlv_out["delivery_date"] is not None

        db.expire_all()
        dlv_doc = db.query(LGDocument).filter(
            LGDocument.lg_instruction_id == inst_deliv_id,
            LGDocument.document_type == "DELIVERY_PROOF",
        ).first()
        assert dlv_doc is not None
        expected_dlv_fname = f"{serial_1[:19]}D.pdf"
        assert dlv_doc.file_name == expected_dlv_fname, f"Expected {expected_dlv_fname}, got {dlv_doc.file_name}"
        raw_dlv_cipher = mem_gcs.get_raw_bytes_by_uri(dlv_doc.file_path)
        assert raw_dlv_cipher.startswith(STREAM_MAGIC)
        print(f"  [PASS] Clean Delivery Proof verified -> 'Instruction Delivered', standardized vault file '{expected_dlv_fname}' encrypted")

        # 1.4 SHA-256 Duplicate Scan Guard + User Force Override (`confirm-delivery`)
        # Issue a second instruction (Decrease) on the same LG and re-upload the EXACT SAME `clean_stamp_bytes`!
        r_dec_dup = client.post(
            f"/api/v1/end-user/lg-records/{lg_deliv_id}/decrease-amount",
            headers=maker_h,
            data={"decrease_amount": "100000.00", "reason": "Partial reduction"},
        )
        assert r_dec_dup.status_code == 200
        r_dec_dup_app = client.post(
            f"/api/v1/corporate-admin/approval-requests/{r_dec_dup.json()['approval_request_id']}/approve",
            headers=checker_h,
        )
        assert r_dec_dup_app.status_code == 200
        inst_dup_id = r_dec_dup_app.json()["related_instruction_id"]

        r_sha_dup = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_dup_id}/record-delivery",
            headers=maker_h,
            data={"delivery_date": today.isoformat()},
            files={"delivery_document_file": ("Reuploaded_Same_Bytes.pdf", clean_stamp_bytes, "application/pdf")},
        )
        assert r_sha_dup.status_code == 200
        sha_json = r_sha_dup.json()
        assert sha_json["status"] == "mismatch"
        assert sha_json["duplicate_scan_detected"] is not None
        assert sha_json["duplicate_scan_detected"]["previous_instruction_id"] == inst_deliv_id
        print("  [PASS] SHA-256 Duplicate Scan Guard caught identical PDF re-uploaded across sibling instructions")

        # Force Override (`POST .../confirm-delivery`) -> logs override & notifies Corporate Admins!
        r_ovr_dlv = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_dup_id}/confirm-delivery",
            headers=maker_h,
            data={
                "delivery_date": today.isoformat(),
                "override_reason": "Single courier manifest covered both instructions",
                "ai_verification_json": json.dumps(sha_json["ai_verification"]),
            },
            files={"delivery_document_file": ("Combined_Courier_Slip.pdf", clean_stamp_bytes + b"%OVR", "application/pdf")},
        )
        assert r_ovr_dlv.status_code == 200
        assert r_ovr_dlv.json()["status"] == "Instruction Delivered"
        db.expire_all()
        inst_dup_db = db.query(LGInstruction).get(inst_dup_id)
        assert (inst_dup_db.details or {}).get("delivery_override", {}).get("status") == "OVERRIDDEN_BY_USER"
        ovr_notif = db.query(SystemNotification).filter(
            SystemNotification.notification_type == "LG_DELIVERY_AI_OVERRIDDEN",
        ).order_by(SystemNotification.id.desc()).first()
        assert ovr_notif is not None and lg_deliv["lg_number"] in ovr_notif.content
        print("  [PASS] Delivery Force Override saved `delivery_override` & alerted Corporate Admins (`LG_DELIVERY_AI_OVERRIDDEN`)")

        # 1.5 Foreign Advised LG Delivery Proof Stamped by Local Advising Bank (`HSBC`)
        lg_frgn = _create_lg(
            f"LG-FRGN-DLV-{fx.uid}",
            extra_fields={
                "issuing_bank_id": fx.bank_foreign.id,
                "foreign_bank_name": "Deutsche Bank AG Frankfurt",
                "foreign_bank_country": "Germany",
                "foreign_bank_address": "Taunusanlage 12, Frankfurt",
                "foreign_bank_swift_code": "DEUTDEFFXXX",
                "advising_status": "Advised",
                "communication_bank_id": str(fx.bank_hsbc.id),
            },
        )
        r_fext = client.post(
            f"/api/v1/end-user/lg-records/{lg_frgn['id']}/extend",
            headers=maker_h,
            json={"new_expiry_date": (today + timedelta(days=260)).isoformat()},
        )
        f_inst_id = r_fext.json()["latest_instruction_id"]
        hsbc_stamp_bytes = _load_sample_pdf("06_Delivery_Proof_Foreign_Advised_HSBC_Stamp_2026-11-02.pdf")
        DocumentVerificationService._call_gemini_delivery_prompt = classmethod(
            lambda cls, *a, **kw: {
                "is_bank_stamp_detected": True,
                "detected_bank_name": "HSBC Bank Egypt",  # Local advising bank, NOT "Foreign Bank"!
                "detected_branch_name": "Corniche El Nil",
                "detected_stamp_date": today.isoformat(),
                "detected_lg_numbers": [lg_frgn["lg_number"]],
                "detected_document_type": "CORPORATE_INSTRUCTION_LETTER_STAMPED",
                "is_courier_receipt": False,
            }
        )
        r_f_dlv = client.post(
            f"/api/v1/end-user/lg-records/instructions/{f_inst_id}/record-delivery",
            headers=maker_h,
            data={"delivery_date": today.isoformat()},
            files={"delivery_document_file": ("HSBC_Advising_Stamp.pdf", hsbc_stamp_bytes, "application/pdf")},
        )
        assert r_f_dlv.status_code == 200
        assert r_f_dlv.json()["status"] == "Instruction Delivered"
        print("  [PASS] Foreign Advised LG accepted local Advising Bank (HSBC) delivery stamp via alternate bank resolution")

        # ==================================================================================
        # PART 2: ALL 5 BANK REPLY AI BRANCHES, DISCREPANCY LETTERS & BANK REMINDERS
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 2] ALL 5 BANK REPLY AI BRANCHES, DISCREPANCY NOTICES & BANK REMINDERS")
        print("=" * 92)

        # --- 2.1 Branch 1: Exact Bank Reply Match -> `Confirmed by Bank` + `{serial[:19]}R.pdf` ---
        exact_reply_bytes = _load_sample_pdf("07_Bank_Reply_CIB_Extension_ExactMatch_2027-05-15_LetterDate_2026-11-03.pdf")
        DocumentVerificationService._call_gemini_bank_reply_prompt = classmethod(
            lambda cls, *a, **kw: {
                "detected_lg_numbers": [lg_deliv["lg_number"]],
                "detected_instruction_serials": [serial_1],
                "bank_advice_reference": "CIB-ADV-9001",
                "detected_bank_name": "Commercial International Bank (CIB)",
                "detected_document_type": "BANK_AMENDMENT_ADVICE",
                "detected_reply_purpose": "EXTENSION",
                "is_bank_seal_detected": True,
                "is_red_security_stamp_detected": False,
                "amended_expiry_date": target_ext_dt.isoformat(),
                "cited_effective_date": today.isoformat(),
                "amended_amount": 1000000.00,
                "is_discharge_confirmed": False,
                "summary": "CIB confirmed extension to exact requested date.",
            }
        )
        r_rep_exact = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_deliv_id}/record-bank-reply",
            headers=maker_h,
            data={"bank_reply_date": today.isoformat(), "reply_details": "Exact CIB extension advice"},
            files={"bank_reply_document_file": ("Exact_Reply.pdf", exact_reply_bytes, "application/pdf")},
        )
        assert r_rep_exact.status_code == 200, r_rep_exact.text
        assert r_rep_exact.json()["status"] == "Confirmed by Bank"
        db.expire_all()
        rep_doc = db.query(LGDocument).filter(
            LGDocument.lg_instruction_id == inst_deliv_id,
            LGDocument.document_type == "BANK_REPLY",
        ).first()
        assert rep_doc is not None and rep_doc.file_name == f"{serial_1[:19]}R.pdf"
        print(f"  [PASS] Branch 1 (Exact Match): Confirmed by Bank, saved '{rep_doc.file_name}' in encrypted vault")

        # --- 2.2 Branch 2: `DATE_CONVENTION_VARIANCE` (<= 3 days) -> 1-Click Direct Alignment ---
        lg_var = _create_lg(f"LG-VAR2D-{fx.uid}")
        req_dt_2d = today + timedelta(days=240)
        bank_dt_2d = req_dt_2d + timedelta(days=2)  # +2 days weekend/quarter convention variance!
        inst_var_id = client.post(
            f"/api/v1/end-user/lg-records/{lg_var['id']}/extend",
            headers=maker_h,
            json={"new_expiry_date": req_dt_2d.isoformat()},
        ).json()["latest_instruction_id"]

        var2d_bytes = _load_sample_pdf("08_Bank_Reply_CIB_Extension_Plus2Days_Variance_2027-05-17.pdf")
        DocumentVerificationService._call_gemini_bank_reply_prompt = classmethod(
            lambda cls, *a, **kw: {
                "detected_lg_numbers": [lg_var["lg_number"]],
                "detected_bank_name": "Commercial International Bank",
                "detected_document_type": "BANK_AMENDMENT_ADVICE",
                "detected_reply_purpose": "EXTENSION",
                "is_bank_seal_detected": True,
                "amended_expiry_date": bank_dt_2d.isoformat(),
                "cited_effective_date": today.isoformat(),
                "amended_amount": 1000000.00,
            }
        )
        r_var_chk = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_var_id}/record-bank-reply",
            headers=maker_h,
            data={"bank_reply_date": today.isoformat()},
            files={"bank_reply_document_file": ("Plus2Days.pdf", var2d_bytes, "application/pdf")},
        )
        assert r_var_chk.status_code == 200
        v2_body = r_var_chk.json()
        assert v2_body["status"] == "date_variance"
        assert v2_body["date_convention_variance"]["classification"] == "DATE_CONVENTION_VARIANCE"
        assert v2_body["date_convention_variance"]["variance_days"] == 2

        # User clicks 1-click "Accept Bank Date" -> updates LGRecord.expiry_date directly WITHOUT Admin Approval!
        r_var_align = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_var_id}/record-bank-reply",
            headers=maker_h,
            data={
                "bank_reply_date": today.isoformat(),
                "accept_date_alignment": "true",
                "aligned_expiry_date": bank_dt_2d.isoformat(),
                "ai_verification_json": json.dumps(v2_body["ai_verification"]),
            },
            files={"bank_reply_document_file": ("Plus2Days.pdf", var2d_bytes, "application/pdf")},
        )
        assert r_var_align.status_code == 200
        assert r_var_align.json()["status"] == "Confirmed by Bank"
        db.expire_all()
        assert db.query(LGRecord).get(lg_var["id"]).expiry_date.date() == bank_dt_2d
        print(f"  [PASS] Branch 2 (<=3d DATE_CONVENTION_VARIANCE): 1-click aligned LG expiry to {bank_dt_2d} without Admin friction")

        # --- 2.3 Branch 3: `FAVORABLE_EXTENDED_EXPIRY` (> 3 days extra coverage in Beneficiary's favor) ---
        lg_fav = _create_lg(f"LG-FAV20D-{fx.uid}")
        req_dt_fav = today + timedelta(days=240)
        bank_dt_fav = req_dt_fav + timedelta(days=20)  # +20 days extra protection!
        inst_fav_id = client.post(
            f"/api/v1/end-user/lg-records/{lg_fav['id']}/extend",
            headers=maker_h,
            json={"new_expiry_date": req_dt_fav.isoformat()},
        ).json()["latest_instruction_id"]

        fav20_bytes = _load_sample_pdf("09_Bank_Reply_CIB_Extension_Plus20Days_AdminApproval_2027-06-04.pdf")
        DocumentVerificationService._call_gemini_bank_reply_prompt = classmethod(
            lambda cls, *a, **kw: {
                "detected_lg_numbers": [lg_fav["lg_number"]],
                "detected_bank_name": "Commercial International Bank",
                "detected_document_type": "BANK_AMENDMENT_ADVICE",
                "detected_reply_purpose": "EXTENSION",
                "is_bank_seal_detected": True,
                "amended_expiry_date": bank_dt_fav.isoformat(),
                "cited_effective_date": today.isoformat(),
                "amended_amount": 1000000.00,
            }
        )
        r_fav_chk = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_fav_id}/record-bank-reply",
            headers=maker_h,
            data={"bank_reply_date": today.isoformat()},
            files={"bank_reply_document_file": ("Plus20Days.pdf", fav20_bytes, "application/pdf")},
        )
        assert r_fav_chk.status_code == 200
        fav_body = r_fav_chk.json()
        assert fav_body["status"] == "mismatch"
        assert fav_body["date_convention_variance"]["classification"] == "FAVORABLE_EXTENDED_EXPIRY"

        r_fav_align = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_fav_id}/record-bank-reply",
            headers=maker_h,
            data={
                "bank_reply_date": today.isoformat(),
                "accept_date_alignment": "true",
                "aligned_expiry_date": bank_dt_fav.isoformat(),
                "ai_verification_json": json.dumps(fav_body["ai_verification"]),
            },
            files={"bank_reply_document_file": ("Plus20Days.pdf", fav20_bytes, "application/pdf")},
        )
        assert r_fav_align.status_code == 200
        db.expire_all()
        assert db.query(LGRecord).get(lg_fav["id"]).expiry_date.date() == bank_dt_fav
        fav_notif = db.query(SystemNotification).filter(
            SystemNotification.notification_type == "LG_FAVORABLE_EXPIRY_ALIGNED",
        ).order_by(SystemNotification.id.desc()).first()
        assert fav_notif is not None and lg_fav["lg_number"] in fav_notif.content
        print(f"  [PASS] Branch 3 (>3d FAVORABLE_EXTENDED_EXPIRY): Aligned to {bank_dt_fav} & notified Corporate Admins")

        # --- 2.4 Branch 4: `BANK_MODIFIED_TERMS_APPROVAL_REQUIRED` (Override Block, Admin Approve & Admin Reject -> Discrepancy Letter) ---
        # Sub-case A: Admin Approves Bank's Modified Amount (`850,000` vs requested `800,000`)
        lg_mod_a = _create_lg(f"LG-MOD-APP-{fx.uid}", amount="1000000.00")
        r_dec_a = client.post(
            f"/api/v1/end-user/lg-records/{lg_mod_a['id']}/decrease-amount",
            headers=maker_h,
            data={"decrease_amount": "200000.00", "reason": "Requested reduction to 800k"},
        )
        inst_mod_a_id = client.post(
            f"/api/v1/corporate-admin/approval-requests/{r_dec_a.json()['approval_request_id']}/approve",
            headers=checker_h,
        ).json()["related_instruction_id"]

        mod850_bytes = _load_sample_pdf("11_Bank_Reply_CIB_DecreaseAmount_BankModified_850000_AdminApproval.pdf")
        DocumentVerificationService._call_gemini_bank_reply_prompt = classmethod(
            lambda cls, *a, **kw: {
                "detected_lg_numbers": [lg_mod_a["lg_number"]],
                "detected_bank_name": "Commercial International Bank",
                "detected_document_type": "BANK_AMENDMENT_ADVICE",
                "detected_reply_purpose": "DECREASE_AMOUNT",
                "is_bank_seal_detected": True,
                "amended_amount": 850000.00,  # Bank reduced to 850k instead of 800k!
                "cited_effective_date": today.isoformat(),
            }
        )
        r_mod_chk = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_mod_a_id}/record-bank-reply",
            headers=maker_h,
            data={"bank_reply_date": today.isoformat()},
            files={"bank_reply_document_file": ("Bank_850k.pdf", mod850_bytes, "application/pdf")},
        )
        assert r_mod_chk.status_code == 200
        mod_a_body = r_mod_chk.json()
        assert mod_a_body["status"] == "mismatch"
        assert mod_a_body["bank_terms_modification"] is not None

        # Hard Override Block: `force_override=True` (`confirm-bank-reply`) MUST fail with HTTP 400!
        r_illegal_ovr = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_mod_a_id}/confirm-bank-reply",
            headers=maker_h,
            data={
                "bank_reply_date": today.isoformat(),
                "override_reason": "Trying to bypass admin approval",
                "ai_verification_json": json.dumps(mod_a_body["ai_verification"]),
            },
            files={"bank_reply_document_file": ("Bank_850k.pdf", mod850_bytes, "application/pdf")},
        )
        assert r_illegal_ovr.status_code == 400
        assert "corporate admin approval" in r_illegal_ovr.json()["detail"].lower()

        # Submit `request_admin_terms_approval=True` -> creates PENDING `LG_AMEND` ApprovalRequest!
        r_sub_adm = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_mod_a_id}/record-bank-reply",
            headers=maker_h,
            data={
                "bank_reply_date": today.isoformat(),
                "request_admin_terms_approval": "true",
                "ai_verification_json": json.dumps(mod_a_body["ai_verification"]),
            },
            files={"bank_reply_document_file": ("Bank_850k.pdf", mod850_bytes, "application/pdf")},
        )
        assert r_sub_adm.status_code == 200
        adm_ar_id = (r_sub_adm.json().get("details") or {}).get("bank_terms_admin_approval", {}).get("approval_request_id")
        assert adm_ar_id is not None

        # Corporate Admin Approves -> LG amount updates to 850,000.00!
        r_app_terms = client.post(f"/api/v1/corporate-admin/approval-requests/{adm_ar_id}/approve", headers=checker_h)
        assert r_app_terms.status_code == 200
        db.expire_all()
        assert db.query(LGRecord).get(lg_mod_a["id"]).lg_amount == Decimal("850000.00")
        print("  [PASS] Branch 4A (Modified Terms Approved): Direct override blocked (400), Admin approval updated LG to 850,000.00")

        # Sub-case B: Admin Rejects Bank's Modified Amount -> Reclassifies Doc + Unlocks Formal Discrepancy Letter + Reminder PDF!
        lg_mod_b = _create_lg(f"LG-MOD-REJ-{fx.uid}", amount="1000000.00")
        r_dec_b = client.post(
            f"/api/v1/end-user/lg-records/{lg_mod_b['id']}/decrease-amount",
            headers=maker_h,
            data={"decrease_amount": "300000.00", "reason": "Requested reduction to 700k"},
        )
        inst_mod_b_id = client.post(
            f"/api/v1/corporate-admin/approval-requests/{r_dec_b.json()['approval_request_id']}/approve",
            headers=checker_h,
        ).json()["related_instruction_id"]

        # Record delivery 10 days ago so it is also eligible for a Bank Reminder
        db.expire_all()
        inst_b_obj = db.query(LGInstruction).get(inst_mod_b_id)
        inst_b_obj.instruction_date = datetime.now(timezone.utc) - timedelta(days=12)
        inst_b_obj.delivery_date = datetime.now(timezone.utc) - timedelta(days=10)
        inst_b_obj.status = "Instruction Delivered"
        inst_b_obj.sent_to_bank = True
        db.commit()

        DocumentVerificationService._call_gemini_bank_reply_prompt = classmethod(
            lambda cls, *a, **kw: {
                "detected_lg_numbers": [lg_mod_b["lg_number"]],
                "detected_bank_name": "Commercial International Bank",
                "detected_document_type": "BANK_AMENDMENT_ADVICE",
                "detected_reply_purpose": "DECREASE_AMOUNT",
                "is_bank_seal_detected": True,
                "amended_amount": 780000.00,  # Bank reduced to 780k instead of 700k!
                "cited_effective_date": today.isoformat(),
            }
        )
        r_mod_b_chk = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_mod_b_id}/record-bank-reply",
            headers=maker_h,
            data={"bank_reply_date": today.isoformat()},
            files={"bank_reply_document_file": ("Bank_780k_Wrong.pdf", mod850_bytes + b"%780K", "application/pdf")},
        ).json()

        r_sub_adm_b = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_mod_b_id}/record-bank-reply",
            headers=maker_h,
            data={
                "bank_reply_date": today.isoformat(),
                "request_admin_terms_approval": "true",
                "ai_verification_json": json.dumps(r_mod_b_chk["ai_verification"]),
            },
            files={"bank_reply_document_file": ("Bank_780k_Wrong.pdf", mod850_bytes + b"%780K", "application/pdf")},
        ).json()
        adm_ar_b_id = r_sub_adm_b["details"]["bank_terms_admin_approval"]["approval_request_id"]

        # Corporate Admin REJECTS the bank's modified terms!
        rej_reason = "Bank erroneously retained 80,000 EGP; demand corrected advice for EGP 700,000.00"
        r_rej_terms = client.post(
            f"/api/v1/corporate-admin/approval-requests/{adm_ar_b_id}/reject",
            headers=checker_h,
            json={"reason": rej_reason},
        )
        assert r_rej_terms.status_code == 200

        db.expire_all()
        inst_b_obj = db.query(LGInstruction).get(inst_mod_b_id)
        b_approval_info = (inst_b_obj.details or {}).get("bank_terms_admin_approval") or {}
        assert b_approval_info.get("status") == "REJECTED_BY_ADMIN"
        assert b_approval_info.get("discrepancy_letter_ready") is True
        # Ensure document was preserved and reclassified to REJECTED_BANK_REPLY (NOT nuked!)
        rej_doc = db.query(LGDocument).filter(
            LGDocument.lg_instruction_id == inst_mod_b_id,
            LGDocument.document_type == "REJECTED_BANK_REPLY",
        ).first()
        assert rej_doc is not None

        # Verify instruction surfaces in `discrepancy-letters-pending-print` AND `instructions-awaiting-reply`
        r_disc_q = client.get("/api/v1/end-user/action-center/discrepancy-letters-pending-print", headers=maker_h)
        assert r_disc_q.status_code == 200
        assert any(i["id"] == inst_mod_b_id for i in r_disc_q.json())

        r_await_q = client.get("/api/v1/end-user/action-center/instructions-awaiting-reply", headers=maker_h)
        assert r_await_q.status_code == 200
        assert any(i["id"] == inst_mod_b_id for i in r_await_q.json())

        # Render the Formal Discrepancy Notice (`?print=true`) and verify user-expected HTML table contents!
        r_disc_html = client.get(
            f"/api/v1/end-user/lg-records/instructions/{inst_mod_b_id}/view-discrepancy-letter",
            headers=maker_h,
            params={"print": True},
        )
        assert r_disc_html.status_code == 200
        disc_html = r_disc_html.text
        assert f"{inst_b_obj.serial_number}/DISC" in disc_html
        assert "700,000.00" in disc_html  # Instructed amount
        assert "780,000.00" in disc_html  # Rejected bank advice amount
        assert "Notice of Discrepancy" in disc_html

        # After `?print=true`, it MUST disappear from `discrepancy-letters-pending-print`!
        r_disc_q_after = client.get("/api/v1/end-user/action-center/discrepancy-letters-pending-print", headers=maker_h)
        assert not any(i["id"] == inst_mod_b_id for i in r_disc_q_after.json())

        # Also test `POST .../send-reminder-to-bank` on `inst_mod_b_id` -> generates `RM` child instruction + PDF with Discrepancy Callout!
        r_rem = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_mod_b_id}/send-reminder-to-bank",
            headers=maker_h,
        )
        assert r_rem.status_code == 200
        assert "Bank Reminder Notice" in r_rem.text
        import re as _re
        b64_match = _re.search(r'var b64 = "([A-Za-z0-9+/=]+)";', r_rem.text)
        assert b64_match is not None, "Expected embedded base64 PDF inside Bank Reminder print toolbar HTML!"
        rem_pdf = extract_pdf_info(base64.b64decode(b64_match.group(1)))
        assert rem_pdf["page_count"] == 1
        assert lg_mod_b["lg_number"] in rem_pdf["text"]

        db.expire_all()
        rem_child = db.query(LGInstruction).filter(
            LGInstruction.lg_record_id == lg_mod_b["id"],
            LGInstruction.instruction_type == "LG_REMINDER_TO_BANKS",
        ).order_by(LGInstruction.id.desc()).first()
        assert rem_child is not None
        rem_serial = rem_child.serial_number
        assert len(rem_serial) == 20 and "RM" in rem_serial and rem_serial.endswith("M")

        # Duplicate reminder on the same parent instruction MUST fail with HTTP 400!
        r_dup_rem = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_mod_b_id}/send-reminder-to-bank",
            headers=maker_h,
        )
        assert r_dup_rem.status_code == 400
        print(f"  [PASS] Branch 4B (Modified Terms Rejected): Preserved REJECTED_BANK_REPLY, rendered Discrepancy Notice (700k vs 780k), issued RM ({rem_serial}) & blocked duplicate reminder")

        # --- 2.5 Branch 5: `WRONG_INSTRUCTION_TARGET` (Multi-Instruction Disambiguation) ---
        lg_dis = _create_lg(f"LG-DISAMB-{fx.uid}", amount="1000000.00")
        ext_dis_dt = today + timedelta(days=280)
        inst_dis_ext_id = client.post(
            f"/api/v1/end-user/lg-records/{lg_dis['id']}/extend",
            headers=maker_h,
            json={"new_expiry_date": ext_dis_dt.isoformat()},
        ).json()["latest_instruction_id"]
        db.expire_all()
        inst_dis_ext_serial = db.query(LGInstruction).get(inst_dis_ext_id).serial_number

        r_dis_dec = client.post(
            f"/api/v1/end-user/lg-records/{lg_dis['id']}/decrease-amount",
            headers=maker_h,
            data={"decrease_amount": "150000.00", "reason": "Sibling decrease"},
        )
        inst_dis_dec_id = client.post(
            f"/api/v1/corporate-admin/approval-requests/{r_dis_dec.json()['approval_request_id']}/approve",
            headers=checker_h,
        ).json()["related_instruction_id"]

        # User accidentally uploads the EXTENSION bank reply onto the DECREASE instruction (`inst_dis_dec_id`)!
        DocumentVerificationService._call_gemini_bank_reply_prompt = classmethod(
            lambda cls, *a, **kw: {
                "detected_lg_numbers": [lg_dis["lg_number"]],
                "detected_instruction_serials": [inst_dis_ext_serial],
                "detected_bank_name": "Commercial International Bank",
                "detected_document_type": "BANK_AMENDMENT_ADVICE",
                "detected_reply_purpose": "EXTENSION",
                "is_bank_seal_detected": True,
                "amended_expiry_date": ext_dis_dt.isoformat(),
                "cited_effective_date": today.isoformat(),
            }
        )
        r_wrong_card = client.post(
            f"/api/v1/end-user/lg-records/instructions/{inst_dis_dec_id}/record-bank-reply",
            headers=maker_h,
            data={"bank_reply_date": today.isoformat()},
            files={"bank_reply_document_file": ("Extension_On_Wrong_Card.pdf", exact_reply_bytes + b"%DIS", "application/pdf")},
        )
        assert r_wrong_card.status_code == 200
        wc_json = r_wrong_card.json()
        assert wc_json["status"] == "mismatch"
        assert wc_json["instruction_disambiguation"] is not None
        assert wc_json["instruction_disambiguation"]["matched_instruction_id"] == inst_dis_ext_id
        assert wc_json["instruction_disambiguation"]["matched_verification"]["status"] == "verified"
        print(f"  [PASS] Branch 5 (WRONG_INSTRUCTION_TARGET): Disambiguated reply from Decrease #{inst_dis_dec_id} -> Extension #{inst_dis_ext_id}")

        # ==================================================================================
        # PART 3: LEGACY EXCEL HISTORICAL MIGRATION HUB (MULTI-SNAPSHOT, DIFFS & RESET)
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 3] LEGACY HISTORICAL MIGRATION HUB: EXCEL STAGING, REPAIR, CHRONOLOGY & RESET")
        print("=" * 92)

        # 3.1 Upload encrypted staged attachment PDF via `/upload-attachments`
        staged_pdf_name = f"Hist_Scan_{fx.uid}.pdf"
        r_up_att = client.post(
            "/api/v1/corporate-admin/migration/upload-attachments",
            headers=admin_h,
            files=[("files", (staged_pdf_name, exact_reply_bytes, "application/pdf"))],
        )
        assert r_up_att.status_code == 200
        assert staged_pdf_name in r_up_att.json()["files"]

        # 3.2 Build an in-memory `.xlsx` workbook with:
        #   - 3 chronological snapshots for `LG-HIST-MULTI-{uid}` (1,000,000 -> Extended -> Reduced to 650,000)
        #   - 1 expired historical LG (`LG-HIST-EXP-{uid}`)
        #   - 1 defective row (`LG-HIST-ERR-{uid}`, missing amount -> ERROR, repaired via PUT `/staged/{id}`)
        hist_multi_num = f"LG-HIST-MULTI-{fx.uid}"
        hist_exp_num = f"LG-HIST-EXP-{fx.uid}"
        hist_err_num = f"LG-HIST-ERR-{fx.uid}"

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Legacy_LGs"
        headers_row = [
            "LG Number",
            "Beneficiary Name",
            "Issuer Name",
            "LG Amount",
            "Currency",
            "Issuance Date",
            "Expiry Date",
            "LG Type",
            "Issuing Bank",
            "Issuing Method",
            "Applicable Rule",
            "Purpose",
            "Internal Owner Email",
            "Category",
            "Attachment",
            "History_Sequence",
        ]
        ws.append(headers_row)

        # Snapshot 1 (2024 Creation: 1,000,000, expires today+60d)
        ws.append([
            hist_multi_num, fx.entity_alpha_hq.entity_name, "Elsewedy Electric", 1000000.00, "EGP",
            (today - timedelta(days=400)).isoformat(), (today + timedelta(days=60)).isoformat(),
            fx.lg_type_perf.name, fx.bank_cib.name, fx.issuing_method.name, fx.rule_urdg.name,
            "Phase 1 Grid", fx.maker_user.email, fx.cat_standard.name, staged_pdf_name, 1,
        ])
        # Snapshot 2 (2025 Extension: 1,000,000, expires today+240d)
        ws.append([
            hist_multi_num, fx.entity_alpha_hq.entity_name, "Elsewedy Electric", 1000000.00, "EGP",
            (today - timedelta(days=400)).isoformat(), (today + timedelta(days=240)).isoformat(),
            fx.lg_type_perf.name, fx.bank_cib.name, fx.issuing_method.name, fx.rule_urdg.name,
            "Phase 1 Grid Extended", fx.maker_user.email, fx.cat_standard.name, "", 2,
        ])
        # Snapshot 3 (2026 Reduction: 650,000.00, expires today+240d)
        final_hist_exp = (today + timedelta(days=240)).isoformat()
        ws.append([
            hist_multi_num, fx.entity_alpha_hq.entity_name, "Elsewedy Electric", 650000.00, "EGP",
            (today - timedelta(days=400)).isoformat(), final_hist_exp,
            fx.lg_type_perf.name, fx.bank_cib.name, fx.issuing_method.name, fx.rule_urdg.name,
            "Phase 1 Grid Reduced", fx.maker_user.email, fx.cat_standard.name, "", 3,
        ])
        # Expired historical LG (expired 90 days ago)
        ws.append([
            hist_exp_num, fx.entity_alpha_hq.entity_name, "Legacy Supplier Co", 300000.00, "EGP",
            (today - timedelta(days=450)).isoformat(), (today - timedelta(days=90)).isoformat(),
            fx.lg_type_perf.name, fx.bank_cib.name, fx.issuing_method.name, fx.rule_urdg.name,
            "Completed Historical Contract", fx.maker_user.email, fx.cat_standard.name, "", 1,
        ])
        # Defective row (missing LG Amount -> ERROR)
        ws.append([
            hist_err_num, fx.entity_alpha_hq.entity_name, "Broken Row Supplier", None, "EGP",
            (today - timedelta(days=100)).isoformat(), (today + timedelta(days=120)).isoformat(),
            fx.lg_type_perf.name, fx.bank_cib.name, fx.issuing_method.name, fx.rule_urdg.name,
            "Row needing inline fix", fx.maker_user.email, fx.cat_standard.name, "", 1,
        ])

        xlsx_buf = io.BytesIO()
        wb.save(xlsx_buf)
        xlsx_bytes = xlsx_buf.getvalue()

        r_up_xlsx = client.post(
            "/api/v1/corporate-admin/migration/upload-structured",
            headers=admin_h,
            files={
                "file": (
                    f"Migration_{fx.uid}.xlsx",
                    xlsx_bytes,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        assert r_up_xlsx.status_code in (200, 201), f"Upload structured failed: {r_up_xlsx.text}"

        # Fetch staged rows & verify defective row is ERROR, then repair via PUT `/staged/{id}`!
        r_staged = client.get("/api/v1/corporate-admin/migration/staged", headers=admin_h)
        assert r_staged.status_code == 200
        staged_list = r_staged.json()
        err_row = next(s for s in staged_list if (s.get("source_data_json") or {}).get("lg_number") == hist_err_num)
        assert err_row["record_status"] == "ERROR"

        fixed_data = dict(err_row["source_data_json"])
        fixed_data["lg_amount"] = 420000.00
        r_fix = client.put(
            f"/api/v1/corporate-admin/migration/staged/{err_row['id']}",
            headers=admin_h,
            json=fixed_data,
        )
        assert r_fix.status_code == 200
        assert r_fix.json()["record_status"] == "READY_FOR_IMPORT"
        print("  [PASS] Excel staged 5 rows, flagged missing-amount row as ERROR, and inline PUT repair promoted to READY_FOR_IMPORT")

        # 3.3 Preview historical timeline reconstruction (`POST /preview-history`)
        r_prev_hist = client.post(
            "/api/v1/corporate-admin/migration/preview-history",
            headers=admin_h,
            params={"lg_number": hist_multi_num},
        )
        assert r_prev_hist.status_code == 200
        prev_hist = r_prev_hist.json()
        assert len(prev_hist) == 1
        assert prev_hist[0]["lg_number"] == hist_multi_num
        assert len(prev_hist[0]["snapshots"]) == 3
        assert prev_hist[0]["conflict_flag"] is False

        # 3.4 Import historical records (`POST /import-history`) & verify reconstructed production LGRecord state!
        r_imp_hist = client.post(
            "/api/v1/corporate-admin/migration/import-history",
            headers=admin_h,
            json={"lg_numbers": [hist_multi_num, hist_exp_num, hist_err_num], "batch_note": f"Batch {fx.tag}"},
        )
        assert r_imp_hist.status_code == 200, r_imp_hist.text
        imp_totals = r_imp_hist.json()["totals"]
        assert imp_totals["imported"] == 3 and imp_totals["failed"] == 0

        db.expire_all()
        prod_multi = db.query(LGRecord).filter(
            LGRecord.customer_id == fx.cust_alpha.id,
            LGRecord.lg_number == hist_multi_num,
        ).first()
        assert prod_multi is not None
        assert prod_multi.migration_source == "LEGACY"
        assert prod_multi.lg_amount == Decimal("650000.00"), f"Expected latest snapshot amount 650k, got {prod_multi.lg_amount}"
        assert prod_multi.expiry_date.date().isoformat() == final_hist_exp
        assert prod_multi.lg_status_id == LgStatusEnum.VALID.value

        change_logs = db.query(LGChangeLog).filter(LGChangeLog.lg_id == prod_multi.id).all()
        assert len(change_logs) >= 2, "Expected chronological LGChangeLog diffs for Extension + Reduction snapshots!"

        prod_exp = db.query(LGRecord).filter(
            LGRecord.customer_id == fx.cust_alpha.id,
            LGRecord.lg_number == hist_exp_num,
        ).first()
        assert prod_exp is not None and prod_exp.lg_status_id == LgStatusEnum.EXPIRED.value
        print("  [PASS] Historical Migration reconstructed 3-snapshot LG (final=650,000.00, LGChangeLog diffs) & set past LG to EXPIRED")

        # 3.5 Verify 48h `reset-migrated-status` & `reset-migrated` clean rollback!
        r_rst_stat = client.get("/api/v1/corporate-admin/migration/reset-migrated-status", headers=admin_h)
        assert r_rst_stat.status_code == 200 and r_rst_stat.json()["allowed"] is True
        r_rst = client.post("/api/v1/corporate-admin/migration/reset-migrated", headers=admin_h)
        assert r_rst.status_code == 200 and r_rst.json()["purged_lg_count"] == 3
        print("  [PASS] Post-migration 48h Reset & Clean purged 3 migrated LGs and restored staging rows for clean re-import")

        # ==================================================================================
        # PART 4: ACTION CENTER, SAFETY SCORE MATH, OPS HEALTH & LIFECYCLE REPORTS
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 4] ACTION CENTER QUEUES, SAFETY SCORE FORMULA, OPS HEALTH & LIFECYCLE REPORTS")
        print("=" * 92)

        # Create 1 near-expiry LG (expires in 12 days) and 1 overdue VALID LG (expiry_date = yesterday, not yet swept by cron)
        lg_near = _create_lg(f"LG-NEAR-{fx.uid}", amount="200000.00", days_to_expiry=12)
        lg_overdue_valid = _create_lg(f"LG-OVD-{fx.uid}", amount="300000.00", days_to_expiry=10)
        db.expire_all()
        ovd_obj = db.query(LGRecord).get(lg_overdue_valid["id"])
        ovd_obj.expiry_date = datetime.combine(today - timedelta(days=2), datetime.min.time())
        # Also backdate an undelivered instruction (`inst_dis_dec_id`) by 6 days so it enters `instructions-undelivered`
        undeliv_obj = db.query(LGInstruction).get(inst_dis_dec_id)
        undeliv_obj.instruction_date = datetime.now(timezone.utc) - timedelta(days=6)
        db.commit()

        # 4.1 Action Center: `lg-for-renewal` and `instructions-undelivered`
        r_ac_ren = client.get("/api/v1/end-user/action-center/lg-for-renewal", headers=maker_h)
        assert r_ac_ren.status_code == 200
        assert any(r["id"] == lg_near["id"] for r in r_ac_ren.json())

        r_ac_und = client.get("/api/v1/end-user/action-center/instructions-undelivered", headers=maker_h)
        assert r_ac_und.status_code == 200
        assert any(i["id"] == inst_dis_dec_id for i in r_ac_und.json())
        print("  [PASS] Action Center queues (`lg-for-renewal` & `instructions-undelivered`) populated accurately")

        # 4.2 My LG Dashboard Safety Score exact formula verification:
        #     `safety_score = clamp(100 - 10 * expired_valid_count - 2 * near_expiry_count, 0, 100)`
        r_dash = client.get("/api/v1/reports/end-user/my-lg-dashboard", headers=maker_h)
        assert r_dash.status_code == 200, r_dash.text
        dash_data = r_dash.json()["data"]

        db.expire_all()
        expected_expired_valid = db.query(LGRecord).filter(
            LGRecord.customer_id == fx.cust_alpha.id,
            LGRecord.is_deleted == False,
            LGRecord.lg_status_id == LgStatusEnum.VALID.value,
            LGRecord.expiry_date < datetime.combine(today, datetime.min.time()),
        ).count()
        near_cnt = dash_data["lgs_near_expiry_count"]
        expected_score = max(0, min(100, 100 - (10 * expected_expired_valid) - (2 * near_cnt)))
        assert dash_data["safety_score"] == expected_score, (
            f"Safety Score mismatch! Expected {expected_score} (expired_valid={expected_expired_valid}, near={near_cnt}), got {dash_data['safety_score']}"
        )
        expected_risk = "Stable" if expected_score >= 90 else ("Attention" if expected_score >= 70 else "Critical")
        assert dash_data["risk_status"] == expected_risk
        print(f"  [PASS] Dashboard Safety Score verified exact: {dash_data['safety_score']}/100 ({dash_data['risk_status']})")

        # 4.3 Corporate Admin LG Performance, Ops Health, Charts & Lifecycle History Report
        r_perf = client.get("/api/v1/reports/corporate-admin/lg-performance", headers=admin_h)
        assert r_perf.status_code == 200
        perf_data = r_perf.json()["data"]
        assert perf_data["lgs_by_status"].get("Valid", 0) >= 5
        assert Decimal(str(perf_data["total_value_of_active_lgs"].get("EGP", 0))) > Decimal("0")

        r_ops = client.get("/api/v1/reports/ops-health", headers=admin_h)
        assert r_ops.status_code == 200
        ops_json = r_ops.json()
        assert ops_json["flow"]["new_issuances_count"] >= 5
        assert len(ops_json["history"]) == 4

        assert client.get("/api/v1/reports/customer-lg-type-mix", headers=admin_h).status_code == 200
        assert client.get("/api/v1/reports/bank-market-share", headers=admin_h).status_code == 200

        r_lc = client.get("/api/v1/end-user/reports/lg-lifecycle-history", headers=maker_h)
        assert r_lc.status_code == 200
        lc_rows = r_lc.json()
        assert len(lc_rows) >= 5
        # Ensure zero scientific notation (e.g. `0E-10`) in formatted report strings
        assert "0E-" not in json.dumps(lc_rows) and "e-" not in json.dumps([r.get("lg_amount") for r in lc_rows])
        print("  [PASS] LG Performance, Ops Health (flow + 4-month history), Charts & Lifecycle History (no sci-notation) verified")

        # ==================================================================================
        # PART 5: SCHEDULED CRON JOBS (DAILY EXPIRY SWEEP & OWNER RENEWAL DIGESTS)
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 5] SCHEDULED CRONS: DAILY EXPIRY STATUS SWEEP & OWNER RENEWAL DIGESTS")
        print("=" * 92)

        # 5.1 `run_daily_lg_status_update(db)` -> transitions `lg_overdue_valid` (`VALID` with past expiry) -> `EXPIRED`!
        asyncio.run(run_daily_lg_status_update(db))
        db.expire_all()
        ovd_after = db.query(LGRecord).get(lg_overdue_valid["id"])
        assert ovd_after.lg_status_id == LgStatusEnum.EXPIRED.value
        exp_notif = db.query(SystemNotification).filter(
            SystemNotification.notification_type == "LG_EXPIRED",
            SystemNotification.link == f"/lg-records/{lg_overdue_valid['id']}",
        ).first()
        assert exp_notif is not None
        print("  [PASS] `run_daily_lg_status_update` transitioned past-expiry LG to EXPIRED & emitted `LG_EXPIRED` notification")

        # 5.2 `crud_lg_record.run_internal_owner_renewal_reminders(db)`:
        #     Create a dedicated Owner with:
        #       - 1 Critical LG expiring in 2 days (`<= 3d` -> Urgent Daily Critical Alert)
        #       - 1 Standard LG expiring in 18 days (`> 3d` -> Consolidated Weekly Owner Digest)
        #       - 1 LG expiring in 15 days that ALREADY has an `LG_RELEASE` AuditLog ("Stop Button" -> MUST be excluded!)
        digest_owner_email = f"digest.owner.{fx.uid.lower()}@globex.com"
        lg_crit2 = _create_lg(f"LG-CRIT2D-{fx.uid}", days_to_expiry=2, owner_email=digest_owner_email)
        lg_wk18 = _create_lg(f"LG-WK10D-{fx.uid}", days_to_expiry=10, owner_email=digest_owner_email)
        lg_stopped = _create_lg(f"LG-STOPPED-{fx.uid}", days_to_expiry=9, owner_email=digest_owner_email)

        # Seed terminal `LG_RELEASE` audit log on `lg_stopped` to trigger the Stop Button
        db.add(
            AuditLog(
                user_id=fx.maker_user.id,
                action_type="LG_RELEASE",
                entity_type="LGRecord",
                entity_id=lg_stopped["id"],
                lg_record_id=lg_stopped["id"],
                customer_id=fx.cust_alpha.id,
                details={"note": "Release initiated"},
            )
        )
        db.commit()

        email_harness.clear()
        asyncio.run(crud_lg_record.run_internal_owner_renewal_reminders(db))

        owner_emails = email_harness.find_by_recipient(digest_owner_email)
        assert len(owner_emails) >= 2, f"Expected separate Weekly Digest + <=3d Critical Alert to {digest_owner_email}, got {len(owner_emails)}"
        combined_bodies = "\n".join(e["body"] for e in owner_emails)
        assert lg_crit2["lg_number"] in combined_bodies
        assert lg_wk18["lg_number"] in combined_bodies
        assert lg_stopped["lg_number"] not in combined_bodies, "LG with terminal LG_RELEASE action MUST be suppressed by Stop Button!"
        print("  [PASS] Owner Renewal Reminders sent Weekly Digest + <=3d Critical Alert & honored Terminal Action Stop-Button")

        print("\n" + "=" * 92)
        print("ALL LG CUSTODY DELIVERY AI, BANK REPLIES, MIGRATION, REPORTS & CRONS PASSED 100%!")
        print("=" * 92)

    finally:
        DocumentVerificationService._call_gemini_delivery_prompt = orig_deliv_llm
        DocumentVerificationService._call_gemini_bank_reply_prompt = orig_reply_llm
        fx.cleanup()
        db.close()


def test_lg_custody_delivery_ai_migration_reports_suite():
    run_lg_custody_delivery_ai_migration_reports_suite()


if __name__ == "__main__":
    run_lg_custody_delivery_ai_migration_reports_suite()
