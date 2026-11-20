# tests/test_bulk_print_action_center.py
"""
End-to-End Verification Suite for Action Center Bulk Print & Batch Re-Print:
- Tests all 3 pending-print sources together in one batch:
  1) Custody Approved Request: LG_DECREASE_AMOUNT
  2) Custody Approved Request: LG_RELEASE
  3) Custody Discrepancy Notice: BANK_DISCREPANCY_NOTICE (REJECTED_BY_ADMIN)
  4) Issuance Maintenance Action: EXTEND (status=EXECUTED, instruction_status="Instruction Issued")
- Verifies:
  - Merged PDF page count == 4 (without duplex padding), zero unresolved `{{` template tags
  - Action-specific placeholders (`decrease_amount_formatted`, `enclosed_documents_clause`) populated
  - Valid QR verification tokens across all 4 letters (including `M2` on Issuance Maintenance)
  - DB print flags updated (`is_printed=True`, `print_notification_status="PRINTED"`,
    `discrepancy_letter_printed=True`, `instruction_status="Printed"`) and items cleared from pending queues
  - Background Print Confirmation emails dispatched cleanly
  - `GET /action-center/bulk-print-history` returns the batch & live statuses
  - `POST /action-center/bulk-print-reprint` with `pad_for_duplex=True` inserts blank back-pages
    strictly between letters -> 7 pages total (4 letters + 3 blank pages, none after final letter)
  - Role guard (Corporate Admin blocked 403 from executing bulk print)
"""

import json
import os
import re
import sys
import base64
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from sqlalchemy.orm.attributes import flag_modified

from _test_harness import (
    app,
    SessionLocal,
    IsolatedTenantFixture,
    install_in_memory_gcs,
    install_email_capture,
    make_auth_headers,
    extract_pdf_info,
)
from app.constants import LgStatusEnum
from app.core.instruction_verification import (
    _to_epoch_seconds,
    generate_verification_token_and_code,
    _COOLDOWN_MEMORY_STORE,
)
from app.models.models import (
    InternalOwnerContact,
    LGRecord,
    LGInstruction,
    ApprovalRequest,
    Template,
)
from app.models.models_issuance import IssuedLGRecord, IssuanceMaintenanceAction


def run_suite():
    install_in_memory_gcs()
    email_harness = install_email_capture()
    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="BULKPRT")
    client = TestClient(app)

    try:
        today = date.today()
        maker_h = make_auth_headers(fx.maker_user)
        checker_h = make_auth_headers(fx.checker_user)
        admin_h = make_auth_headers(fx.admin_user)
        custody_approver_h = make_auth_headers(fx.admin2_user)

        owner_email = f"owner.{fx.uid.lower()}@globex.com"
        mgr_email = f"mgr.{fx.uid.lower()}@globex.com"

        # Seed modern release template override if present so {{enclosed_documents_clause}} is exercised
        modern_rel_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "app", "templates", "CustomerID_1_LG_RELEASE_template.html")
        )
        if os.path.exists(modern_rel_path):
            with open(modern_rel_path, "r", encoding="utf-8") as tf:
                db.add(
                    Template(
                        name=f"Modern Release {fx.uid}",
                        template_type="LG_INSTRUCTION",
                        action_type="LG_RELEASE",
                        content=tf.read(),
                        is_global=False,
                        is_notification_template=False,
                        customer_id=fx.cust_alpha.id,
                    )
                )
                db.commit()

        def _create_custody_lg(
            suffix: str,
            amount: str,
            bank_id: int,
            entity_id: int,
            ccy_id: int,
            expiry_days: int = 180,
            auto_renewal: bool = False,
        ) -> int:
            payload = {
                "lg_number": f"LG-{suffix}-{fx.uid}",
                "lg_amount": amount,
                "lg_currency_id": ccy_id,
                "lg_payable_currency_id": ccy_id,
                "issuance_date": (today - timedelta(days=30)).isoformat(),
                "expiry_date": (today + timedelta(days=expiry_days)).isoformat(),
                "auto_renewal": auto_renewal,
                "lg_type_id": fx.lg_type_perf.id,
                "lg_operational_status_id": fx.op_operative.id if fx.op_operative else None,
                "description_purpose": f"Bulk Print Verification {suffix}",
                "issuer_name": f"Counterparty {suffix}",
                "issuer_id": f"CR-{suffix}-{fx.uid}",
                "beneficiary_corporate_id": entity_id,
                "issuing_bank_id": bank_id,
                "issuing_bank_address": "Cairo Banking District",
                "issuing_bank_phone": "+20219666",
                "issuing_method_id": fx.issuing_method.id,
                "applicable_rule_id": fx.rule_urdg.id,
                "internal_owner_email": owner_email,
                "internal_owner_phone": "+201001112233",
                "internal_owner_id": f"EMP-{fx.uid}",
                "manager_email": mgr_email,
                "lg_category_id": fx.cat_standard.id,
                "notes": f"Notes for {suffix}",
            }
            res = client.post(
                "/api/v1/end-user/lg-records/",
                headers=maker_h,
                data={"lg_record_in": json.dumps(payload)},
            )
            assert res.status_code == 201, f"Failed creating LG {suffix}: {res.text}"
            return res.json()["id"]

        # 1a) Custody LG #1 -> LG_DECREASE_AMOUNT #1 (global template, approved, pending print)
        lg_dec_id = _create_custody_lg("DEC", "800000.00", fx.bank_cib.id, fx.entity_alpha_hq.id, fx.ccy_egp.id)
        res_dec = client.post(
            f"/api/v1/end-user/lg-records/{lg_dec_id}/decrease-amount",
            headers=maker_h,
            data={"decrease_amount": "150000.00", "reason": "Scope reduction 1"},
        )
        assert res_dec.status_code == 200, res_dec.text
        dec_ar_id = res_dec.json()["approval_request_id"]
        res_dec_app = client.post(f"/api/v1/corporate-admin/approval-requests/{dec_ar_id}/approve", headers=custody_approver_h)
        assert res_dec_app.status_code == 200, res_dec_app.text
        dec_inst_id = res_dec_app.json()["related_instruction_id"]

        # 1b) Second LG_DECREASE_AMOUNT #2 on the SAME LG (reproduces BOT/2025/888 multi-decrease scenario)
        res_dec2 = client.post(
            f"/api/v1/end-user/lg-records/{lg_dec_id}/decrease-amount",
            headers=maker_h,
            data={"decrease_amount": "50000.00", "reason": "Scope reduction 2"},
        )
        assert res_dec2.status_code == 200, res_dec2.text
        dec2_ar_id = res_dec2.json()["approval_request_id"]
        res_dec2_app = client.post(f"/api/v1/corporate-admin/approval-requests/{dec2_ar_id}/approve", headers=custody_approver_h)
        assert res_dec2_app.status_code == 200, res_dec2_app.text
        dec2_inst_id = res_dec2_app.json()["related_instruction_id"]

        # 2) Custody LG #2 -> LG_RELEASE (approved, pending print)
        lg_rel_id = _create_custody_lg("REL", "450000.00", fx.bank_hsbc.id, fx.entity_alpha_sub.id, fx.ccy_usd.id)
        res_rel = client.post(
            f"/api/v1/end-user/lg-records/{lg_rel_id}/release",
            headers=maker_h,
            data={"reason": "Final completion"},
        )
        assert res_rel.status_code == 200, res_rel.text
        rel_ar_id = res_rel.json()["approval_request_id"]
        res_rel_app = client.post(f"/api/v1/corporate-admin/approval-requests/{rel_ar_id}/approve", headers=custody_approver_h)
        assert res_rel_app.status_code == 200, res_rel_app.text
        rel_inst_id = res_rel_app.json()["related_instruction_id"]

        # 3) Custody LG #3 -> Discrepancy Notice pending print (REJECTED_BY_ADMIN)
        lg_disc_id = _create_custody_lg("DISC", "300000.00", fx.bank_qnb.id, fx.entity_alpha_hq.id, fx.ccy_egp.id)
        res_ext_disc = client.post(
            f"/api/v1/end-user/lg-records/{lg_disc_id}/extend",
            headers=maker_h,
            json={"new_expiry_date": (today + timedelta(days=365)).isoformat()},
        )
        assert res_ext_disc.status_code == 200, res_ext_disc.text
        disc_inst_id = res_ext_disc.json()["latest_instruction_id"]
        db.expire_all()
        disc_inst = db.get(LGInstruction, disc_inst_id)
        disc_details = dict(disc_inst.details or {})
        disc_details["bank_terms_admin_approval"] = {
            "status": "REJECTED_BY_ADMIN",
            "rejected_at": datetime.utcnow().isoformat(),
            "rejected_bank_reply_date": today.isoformat(),
            "amendment_details": {"expiry_date": (today + timedelta(days=270)).isoformat()},
        }
        disc_inst.details = disc_details
        disc_inst.is_printed = True
        flag_modified(disc_inst, "details")
        db.add(disc_inst)
        db.commit()

        # 4) Issuance Maintenance Action -> EXTEND pending print
        iss_ref = f"ISS-BP-{fx.uid}"
        bank_lg_num = f"CIB-ISS-{fx.uid}-88"
        issued_lg = IssuedLGRecord(
            lg_ref_number=iss_ref,
            internal_serial=f"2026-SUB-{fx.uid}-000",
            bank_lg_number=bank_lg_num,
            customer_id=fx.cust_alpha.id,
            bank_id=fx.bank_cib.id,
            issuing_entity_id=fx.entity_alpha_sub.id,
            beneficiary_name="National Grid Co.",
            current_amount=Decimal("250000.00"),
            currency_id=fx.ccy_usd.id,
            issue_date=today - timedelta(days=40),
            expiry_date=today + timedelta(days=60),
            status="ACTIVE",
            issued_by_user_id=fx.maker_user.id,
        )
        db.add(issued_lg)
        db.commit()
        db.refresh(issued_lg)

        r_maint = client.post(
            f"/api/v1/issuance/issued-lgs/{issued_lg.id}/maintenance",
            headers=maker_h,
            json={
                "action_type": "EXTEND",
                "action_data": {"new_expiry_date": (today + timedelta(days=180)).isoformat()},
                "notes": "Extend issued LG for Bulk Print test",
            },
        )
        assert r_maint.status_code == 200, f"Maintenance create failed: {r_maint.text}"
        maint_data = r_maint.json()
        maint_id = maint_data["id"]
        while maint_data.get("status") == "PENDING_APPROVAL":
            approver_ids = [int(x) for x in (maint_data.get("pending_approver_users") or [])]
            hdr = checker_h if fx.checker_user.id in approver_ids else admin_h
            r_ma = client.post(f"/api/v1/issuance/maintenance/{maint_id}/approve", headers=hdr)
            assert r_ma.status_code == 200, r_ma.text
            maint_data = r_ma.json()
        assert maint_data["status"] == "EXECUTED"
        assert maint_data["instruction_status"] == "Instruction Issued"

        # Verify all 5 appear in their pending-print endpoints with enriched fields
        r_p_cust = client.get("/api/v1/end-user/action-center/approved-requests-pending-print", headers=maker_h)
        assert r_p_cust.status_code == 200
        cust_ids = {p["related_instruction"]["id"] for p in r_p_cust.json() if p.get("related_instruction")}
        assert dec_inst_id in cust_ids and dec2_inst_id in cust_ids and rel_inst_id in cust_ids
        sample_cust_row = next(p for p in r_p_cust.json() if p["related_instruction"]["id"] == dec_inst_id)
        assert sample_cust_row["lg_record"]["beneficiary_corporate"]["entity_name"]
        assert sample_cust_row["lg_record"]["issuing_bank"]["name"]

        r_p_disc = client.get("/api/v1/end-user/action-center/discrepancy-letters-pending-print", headers=maker_h)
        assert r_p_disc.status_code == 200
        assert disc_inst_id in {d["id"] for d in r_p_disc.json()}

        r_p_maint = client.get("/api/v1/issuance/maintenance/pending-print", headers=maker_h)
        assert r_p_maint.status_code == 200
        maint_row = next((m for m in r_p_maint.json() if m["id"] == maint_id), None)
        assert maint_row is not None
        assert maint_row["bank_name"]
        assert maint_row["entity_name"] == fx.entity_alpha_sub.entity_name
        assert maint_row["currency_code"] == "USD"
        assert maint_row["amount"] == 250000.0
        print("  [PASS] Seeded all 5 unrelated letters (incl. 2 global-template decreases on same LG)")

        # Corporate Admin cannot execute bulk print (403)
        r_admin_blk = client.post(
            "/api/v1/end-user/action-center/bulk-print",
            headers=admin_h,
            json={"items": [{"source": "custody", "id": dec_inst_id}], "pad_for_duplex": False},
        )
        assert r_admin_blk.status_code == 403

        # Execute Bulk Print for all 5 items (pad_for_duplex=False)
        email_harness.clear()
        bulk_payload = {
            "items": [
                {"source": "custody", "id": dec_inst_id},
                {"source": "custody", "id": dec2_inst_id},
                {"source": "custody", "id": rel_inst_id},
                {"source": "custody_discrepancy", "id": disc_inst_id},
                {"source": "issuance_maintenance", "id": maint_id},
            ],
            "pad_for_duplex": False,
        }
        r_bulk = client.post("/api/v1/end-user/action-center/bulk-print", headers=maker_h, json=bulk_payload)
        assert r_bulk.status_code == 200, f"Bulk print failed: {r_bulk.text}"
        bulk_out = r_bulk.json()

        assert bulk_out["printed_count"] == 5
        assert bulk_out["batch_id"].startswith("BULK-PRT-")
        assert len(bulk_out["printed_items"]) == 5

        pdf_bytes = base64.b64decode(bulk_out["combined_pdf_base64"])
        pdf_info = extract_pdf_info(pdf_bytes)
        assert pdf_info["page_count"] == 5, f"Expected 5 pages, got {pdf_info['page_count']}"
        assert "{{" not in pdf_info["text"], f"Unresolved placeholder found in bulk PDF:\n{pdf_info['text']}"

        # Verify action-specific placeholders & content per page
        page0, page1, page2, page3, page4 = pdf_info["pages_text"]
        assert "150,000.00" in page0 and "650,000.00" in page0, f"Decrease 1 amounts missing on page 0:\n{page0}"
        assert "50,000.00" in page1 and "600,000.00" in page1, f"Decrease 2 amounts missing on page 1:\n{page1}"
        assert "attached herewith" in page2.lower(), f"enclosed_documents_clause missing on page 2:\n{page2}"
        assert "Discrepancy" in page3 and f"LG-DISC-{fx.uid}" in page3, f"Discrepancy content missing on page 3:\n{page3}"
        assert bank_lg_num in page4 or iss_ref in page4, f"Issuance maintenance content missing on page 4:\n{page4}"

        # Verify UNIQUE authentic QR tokens & Security Codes on EVERY page (never preview 6957!)
        for idx, ptext in enumerate(pdf_info["pages_text"]):
            assert "Security Code:" in ptext, f"Missing security verification footer on page {idx}"

        _COOLDOWN_MEMORY_STORE.clear()
        db.expire_all()
        inst_dec1 = db.get(LGInstruction, dec_inst_id)
        inst_dec2 = db.get(LGInstruction, dec2_inst_id)
        inst_rel = db.get(LGInstruction, rel_inst_id)
        inst_disc = db.get(LGInstruction, disc_inst_id)
        db_ma = db.get(IssuanceMaintenanceAction, maint_id)

        dec_lg_num = inst_dec1.lg_record.lg_number
        rel_lg_num = inst_rel.lg_record.lg_number
        disc_lg_num = inst_disc.lg_record.lg_number

        dec1_tok, dec1_pin = generate_verification_token_and_code(
            "I", inst_dec1.id, inst_dec1.serial_number, dec_lg_num,
            _to_epoch_seconds(inst_dec1.created_at or inst_dec1.instruction_date),
        )
        dec2_tok, dec2_pin = generate_verification_token_and_code(
            "I", inst_dec2.id, inst_dec2.serial_number, dec_lg_num,
            _to_epoch_seconds(inst_dec2.created_at or inst_dec2.instruction_date),
        )
        rel_tok, rel_pin = generate_verification_token_and_code(
            "I", inst_rel.id, inst_rel.serial_number, rel_lg_num,
            _to_epoch_seconds(inst_rel.created_at or inst_rel.instruction_date),
        )
        disc_tok, disc_pin = generate_verification_token_and_code(
            "I", inst_disc.id, inst_disc.serial_number, disc_lg_num,
            _to_epoch_seconds(inst_disc.created_at or inst_disc.instruction_date),
        )
        m2_tok, m2_pin = generate_verification_token_and_code(
            "M", db_ma.id, db_ma.letter_serial_number, bank_lg_num,
            _to_epoch_seconds(db_ma.created_at),
        )
        db.commit()

        assert dec1_tok != dec2_tok, "Two instructions on the same LG must produce distinct QR tokens"
        assert dec1_pin in page0, f"Expected real pin {dec1_pin} on Decrease #1 page 0, got:\n{page0}"
        assert dec2_pin in page1, f"Expected real pin {dec2_pin} on Decrease #2 page 1, got:\n{page1}"
        assert rel_pin in page2, f"Expected real pin {rel_pin} on Release page 2, got:\n{page2}"
        assert disc_pin in page3, f"Expected real pin {disc_pin} on Discrepancy page 3, got:\n{page3}"
        assert m2_pin in page4, f"Expected real M2 pin {m2_pin} on Maintenance page 4, got:\n{page4}"

        for tok, pin, lg_num_val in [
            (dec1_tok, dec1_pin, dec_lg_num),
            (dec2_tok, dec2_pin, dec_lg_num),
            (rel_tok, rel_pin, rel_lg_num),
            (disc_tok, disc_pin, disc_lg_num),
            (m2_tok, m2_pin, bank_lg_num),
        ]:
            r_insp = client.get("/api/v1/public/verify-instruction/inspect", params={"t": tok})
            assert r_insp.status_code == 200 and r_insp.json()["valid_token"] is True
            assert r_insp.json()["is_preview"] is False
            r_conf = client.post(
                "/api/v1/public/verify-instruction/confirm",
                json={"token": tok, "code": pin, "lg_tail": lg_num_val[-2:], "elapsed_ms": 2000},
            )
            assert r_conf.status_code == 200 and r_conf.json()["verified"] is True, f"Confirm failed for {tok}: {r_conf.text}"
        print("  [PASS] Merged PDF has 5 pages, zero unresolved placeholders, and unique verified QR seals on every page")

        # Verify DB state transitions & removal from pending-print queues
        db.expire_all()
        assert db.get(LGInstruction, dec_inst_id).is_printed is True
        assert db.get(LGInstruction, dec2_inst_id).is_printed is True
        assert db.get(LGInstruction, rel_inst_id).is_printed is True
        assert db.get(ApprovalRequest, dec_ar_id).request_details.get("print_notification_status") == "PRINTED"
        assert db.get(ApprovalRequest, dec2_ar_id).request_details.get("print_notification_status") == "PRINTED"
        assert db.get(ApprovalRequest, rel_ar_id).request_details.get("print_notification_status") == "PRINTED"
        assert db.get(LGInstruction, disc_inst_id).details["bank_terms_admin_approval"]["discrepancy_letter_printed"] is True
        db_ma = db.get(IssuanceMaintenanceAction, maint_id)
        assert db_ma.instruction_status == "Printed"
        assert db_ma.is_printed is True
        db.commit()

        assert len(client.get("/api/v1/end-user/action-center/approved-requests-pending-print", headers=maker_h).json()) == 0
        assert disc_inst_id not in {d["id"] for d in client.get("/api/v1/end-user/action-center/discrepancy-letters-pending-print", headers=maker_h).json()}
        assert maint_id not in {m["id"] for m in client.get("/api/v1/issuance/maintenance/pending-print", headers=maker_h).json()}

        # Verify background Print Confirmation emails captured for the 3 custody instructions
        owner_emails = email_harness.find_by_recipient(owner_email)
        assert len(owner_emails) == 3, f"Expected 3 background print confirmation emails, got {len(owner_emails)}"
        print("  [PASS] All 5 items marked printed, cleared from queues, and 3 background emails dispatched")

        # Verify History endpoint
        r_hist = client.get("/api/v1/end-user/action-center/bulk-print-history", headers=maker_h)
        assert r_hist.status_code == 200, r_hist.text
        batches = r_hist.json()
        assert len(batches) >= 1
        latest_batch = next((b for b in batches if b["batch_id"] == bulk_out["batch_id"]), None)
        assert latest_batch is not None
        assert latest_batch["printed_count"] == 5
        assert len(latest_batch["items"]) == 5

        # Verify Reprint with pad_for_duplex=True -> 9 pages (5 letters + 4 blank separators, none after final letter)
        r_reprint = client.post(
            "/api/v1/end-user/action-center/bulk-print-reprint",
            headers=maker_h,
            json={
                "items": [{"source": i["source"], "id": i["id"]} for i in latest_batch["items"]],
                "pad_for_duplex": True,
            },
        )
        assert r_reprint.status_code == 200, r_reprint.text
        reprint_pdf = extract_pdf_info(base64.b64decode(r_reprint.json()["combined_pdf_base64"]))
        assert reprint_pdf["page_count"] == 9, f"Expected 9 pages with duplex padding, got {reprint_pdf['page_count']}"
        for blank_idx in (1, 3, 5, 7):
            assert reprint_pdf["pages_text"][blank_idx].strip() == ""
        assert reprint_pdf["pages_text"][8].strip() != ""
        assert dec1_pin in reprint_pdf["pages_text"][0] and dec2_pin in reprint_pdf["pages_text"][2]
        print("  [PASS] History & Duplex-Safe Reprint (9 pages + authentic QR seals preserved) verified")

        # Verify Auto-Renewal Reprint endpoint also supports pad_for_duplex=True
        r_ar_reprint = client.post(
            "/api/v1/end-user/lg-records/auto-renewal-reprint",
            headers=maker_h,
            json={"instruction_ids": [dec_inst_id, dec2_inst_id], "pad_for_duplex": True},
        )
        assert r_ar_reprint.status_code == 200, r_ar_reprint.text
        ar_pdf = extract_pdf_info(base64.b64decode(r_ar_reprint.json()["combined_pdf_base64"]))
        assert ar_pdf["page_count"] == 3, f"Expected 3 pages on Auto-Renewal duplex reprint, got {ar_pdf['page_count']}"
        assert ar_pdf["pages_text"][1].strip() == ""
        assert dec1_pin in ar_pdf["pages_text"][0] and dec2_pin in ar_pdf["pages_text"][2]
        print("  [PASS] Auto-Renewal Duplex-Safe Reprint (3 pages + authentic QR seals) verified")

        # Verify Bulk Bank Reminders: pad_for_duplex=False (2 pages) vs pad_for_duplex=True (3 pages)
        overdue_dt = datetime.now() - timedelta(days=10)
        db.expire_all()
        for iid in (dec_inst_id, dec2_inst_id):
            row = db.get(LGInstruction, iid)
            row.instruction_date = overdue_dt
            row.delivery_date = None
            row.bank_reply_date = None
            db.add(row)
        db.commit()

        r_rem_single = client.get(
            "/api/v1/end-user/lg-records/instructions/generate-all-bank-reminders-pdf",
            headers=maker_h,
            params={"pad_for_duplex": "false"},
        )
        assert r_rem_single.status_code == 200, f"Bulk reminder single failed: {r_rem_single.text}"
        rem_single_out = r_rem_single.json()
        assert rem_single_out["reminders_generated_count"] == 2
        assert rem_single_out["pad_for_duplex"] is False
        rem_single_pdf = extract_pdf_info(base64.b64decode(rem_single_out["combined_pdf_base64"]))
        assert rem_single_pdf["page_count"] == 2, f"Expected 2 pages, got {rem_single_pdf['page_count']}"
        assert "{{" not in rem_single_pdf["text"]

        # Prepare 2 more overdue instructions (inst_rel + a newly approved release LG) for pad_for_duplex=True
        lg_rem2_id = _create_custody_lg("REM2", "190000.00", fx.bank_cib.id, fx.entity_alpha_hq.id, fx.ccy_egp.id)
        r_rem2_rel = client.post(
            f"/api/v1/end-user/lg-records/{lg_rem2_id}/release",
            headers=maker_h,
            data={"reason": "Reminder duplex test"},
        )
        assert r_rem2_rel.status_code == 200, r_rem2_rel.text
        rem2_ar_id = r_rem2_rel.json()["approval_request_id"]
        r_rem2_app = client.post(f"/api/v1/corporate-admin/approval-requests/{rem2_ar_id}/approve", headers=custody_approver_h)
        assert r_rem2_app.status_code == 200, r_rem2_app.text
        rem2_inst_id = r_rem2_app.json()["related_instruction_id"]

        db.expire_all()
        for iid in (rel_inst_id, rem2_inst_id):
            row = db.get(LGInstruction, iid)
            row.instruction_date = overdue_dt
            row.delivery_date = None
            row.bank_reply_date = None
            db.add(row)
        db.commit()

        r_rem_duplex = client.get(
            "/api/v1/end-user/lg-records/instructions/generate-all-bank-reminders-pdf",
            headers=maker_h,
            params={"pad_for_duplex": "true"},
        )
        assert r_rem_duplex.status_code == 200, f"Bulk reminder duplex failed: {r_rem_duplex.text}"
        rem_duplex_out = r_rem_duplex.json()
        assert rem_duplex_out["reminders_generated_count"] == 2
        assert rem_duplex_out["pad_for_duplex"] is True
        rem_duplex_pdf = extract_pdf_info(base64.b64decode(rem_duplex_out["combined_pdf_base64"]))
        assert rem_duplex_pdf["page_count"] == 3, f"Expected 3 duplex-padded pages, got {rem_duplex_pdf['page_count']}"
        assert rem_duplex_pdf["pages_text"][1].strip() == ""
        assert "{{" not in rem_duplex_pdf["text"]
        assert "Security Code:" in rem_duplex_pdf["pages_text"][0]
        assert "Security Code:" in rem_duplex_pdf["pages_text"][2]

        # Confirm the newly generated reminder instructions verify cryptographically
        rem_rows = (
            db.query(LGInstruction)
            .filter(
                LGInstruction.lg_record_id.in_([lg_rel_id, lg_rem2_id]),
                LGInstruction.instruction_type == "LG_REMINDER_TO_BANKS",
                LGInstruction.is_deleted == False,
            )
            .all()
        )
        assert len(rem_rows) == 2
        for rem_inst in rem_rows:
            r_tok, r_pin = generate_verification_token_and_code(
                "I",
                rem_inst.id,
                rem_inst.serial_number,
                rem_inst.lg_record.lg_number,
                _to_epoch_seconds(rem_inst.created_at or rem_inst.instruction_date),
            )
            assert r_pin in rem_duplex_pdf["text"], f"Missing reminder PIN {r_pin} in duplex PDF"
            r_conf = client.post(
                "/api/v1/public/verify-instruction/confirm",
                json={"token": r_tok, "code": r_pin, "lg_tail": rem_inst.lg_record.lg_number[-2:], "elapsed_ms": 2000},
            )
            assert r_conf.status_code == 200 and r_conf.json()["verified"] is True, r_conf.text
        db.commit()
        print("  [PASS] Bulk Bank Reminders single-sided (2p) & pad_for_duplex=True (3p + unique QR seals) verified")

        # Verify Auto-Renewal run with pad_for_duplex=True & background extension emails
        _create_custody_lg("AR1", "210000.00", fx.bank_cib.id, fx.entity_alpha_hq.id, fx.ccy_egp.id, expiry_days=5, auto_renewal=True)
        _create_custody_lg("AR2", "320000.00", fx.bank_hsbc.id, fx.entity_alpha_sub.id, fx.ccy_usd.id, expiry_days=6, auto_renewal=True)
        email_harness.clear()
        r_ar_run = client.post(
            "/api/v1/end-user/lg-records/run-auto-renewal",
            headers=maker_h,
            json={"pad_for_duplex": True},
        )
        assert r_ar_run.status_code == 200, f"Auto-renewal run failed: {r_ar_run.text}"
        ar_run_out = r_ar_run.json()
        assert ar_run_out["renewed_count"] == 2, f"Expected 2 renewed LGs, got {ar_run_out}"
        ar_run_pdf = extract_pdf_info(base64.b64decode(ar_run_out["combined_pdf_base64"]))
        assert ar_run_pdf["page_count"] == 3, f"Expected 3 duplex-padded pages, got {ar_run_pdf['page_count']}"
        assert ar_run_pdf["pages_text"][1].strip() == ""
        assert "{{" not in ar_run_pdf["text"]

        ar_emails = email_harness.find_by_recipient(owner_email)
        assert len(ar_emails) == 2, f"Expected 2 background extension emails, got {len(ar_emails)}"

        ar_inst_ids = [it["instruction_id"] for it in ar_run_out["renewed_items"]]
        db.expire_all()
        ar_insts = db.query(LGInstruction).filter(LGInstruction.id.in_(ar_inst_ids)).all()
        for idx_ar, ari in enumerate(ar_insts):
            a_tok, a_pin = generate_verification_token_and_code(
                "I",
                ari.id,
                ari.serial_number,
                ari.lg_record.lg_number,
                _to_epoch_seconds(ari.created_at or ari.instruction_date),
            )
            assert a_pin in ar_run_pdf["text"], f"Missing AR pin {a_pin} in PDF"
            r_conf = client.post(
                "/api/v1/public/verify-instruction/confirm",
                json={"token": a_tok, "code": a_pin, "lg_tail": ari.lg_record.lg_number[-2:], "elapsed_ms": 2000},
            )
            assert r_conf.status_code == 200 and r_conf.json()["verified"] is True, r_conf.text
        db.commit()
        print("  [PASS] Auto-Renewal E2E (pad_for_duplex=True 3p + background emails + verified QR seals) verified")

        print("\nALL BULK PRINT, REMINDER & AUTO-RENEWAL E2E CHECKS PASSED!")
    finally:
        fx.cleanup()
        db.close()


if __name__ == "__main__":
    run_suite()
