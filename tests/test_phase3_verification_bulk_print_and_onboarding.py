# tests/test_phase3_verification_bulk_print_and_onboarding.py
"""
Phase 3 Verification Suite:
1. Instruction Verification: canonical cooldown keys (v1/v2/4-part/whitespace), thread-safe atomic
   cooldown persistence, and soft-delete enforcement (IssuanceMaintenanceAction, LGRecord, IssuedLGRecord).
2. Single Bank Reminder (send_bank_reminder): PDF QR code & 4-digit PIN bind strictly to the reminder
   LGInstruction rather than the original instruction serial in the body.
3. Maintenance Print & Reply Lifecycle: mark-printed guards (EXECUTED + Instruction Issued + not GRACE +
   not deleted) and record_bank_reply accepting 'Printed' instruction_status.
4. Action Center Bulk Print Guards: module gates (has_custody_module / has_issuance_module),
   EXECUTED/status/reprint filtering, and issuing_entity_id entity-level authorization.
5. LGRecordMinimalOut Serialization: foreign_bank_name, advising_status, communication_bank_id,
   communication_bank on /discrepancy-letters-pending-print.
6. Trial Registration & Onboarding Activation Emails: missing/failed PDF attachments omit attachment
   claims, HTML-escape user/tenant strings, and complete cleanly without 500s.
"""

import asyncio
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm.attributes import flag_modified

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
for p in (TESTS_DIR, ROOT_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

from _test_harness import (  # noqa: E402
    IsolatedTenantFixture,
    extract_pdf_info,
    install_email_capture,
    install_in_memory_gcs,
    make_auth_headers,
)
import app.api.v1.endpoints.public as public_ep  # noqa: E402
from app.constants import SubscriptionStatus  # noqa: E402
from app.core.instruction_verification import (  # noqa: E402
    _COOLDOWN_LOCK,
    _COOLDOWN_MEMORY_STORE,
    _RECENT_INSTRUCTIONS_CACHE,
    _canonical_token_key,
    _check_cooldown_status,
    _get_cooldown_file_path,
    _record_failed_attempt,
    _resolve_instruction_meta,
    _save_cooldown_store,
    _to_epoch_seconds,
    generate_verification_token_and_code,
    inspect_verification_token,
    verify_instruction_with_code,
)
from app.crud.crud import crud_lg_instruction  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.main import app  # noqa: E402
from app.models.models import LGInstruction, LGRecord, Template  # noqa: E402
from app.models.models_issuance import IssuanceMaintenanceAction, IssuedLGRecord  # noqa: E402
from app.services.customer_onboarding_service import send_corporate_admin_activation_email  # noqa: E402
from app.services.issuance_maintenance_service import maintenance_service  # noqa: E402


def _create_custody_lg(
    client: TestClient,
    fx: IsolatedTenantFixture,
    headers: dict,
    suffix: str,
    today: date,
    foreign: bool = False,
) -> int:
    payload = {
        "lg_number": f"LG-P3-{suffix}-{fx.uid}",
        "lg_amount": "500000.00",
        "lg_currency_id": fx.ccy_egp.id,
        "lg_payable_currency_id": fx.ccy_egp.id,
        "issuance_date": (today - timedelta(days=30)).isoformat(),
        "expiry_date": (today + timedelta(days=180)).isoformat(),
        "auto_renewal": False,
        "lg_type_id": fx.lg_type_perf.id,
        "lg_operational_status_id": fx.op_operative.id if fx.op_operative else None,
        "description_purpose": f"Phase 3 Test LG {suffix}",
        "issuer_name": f"Beneficiary {suffix}",
        "issuer_id": f"CR-{suffix}-{fx.uid}",
        "beneficiary_corporate_id": fx.entity_alpha_hq.id,
        "issuing_bank_id": fx.bank_foreign.id if foreign else fx.bank_cib.id,
        "issuing_bank_address": "Cairo Banking District",
        "issuing_bank_phone": "+20219666",
        "issuing_method_id": fx.issuing_method.id,
        "applicable_rule_id": fx.rule_urdg.id,
        "internal_owner_email": f"owner.p3.{fx.uid.lower()}@globex.com",
        "internal_owner_phone": "+201001112233",
        "internal_owner_id": f"EMP-{fx.uid}",
        "manager_email": f"mgr.p3.{fx.uid.lower()}@globex.com",
        "lg_category_id": fx.cat_standard.id,
    }
    if foreign:
        payload["foreign_bank_name"] = "Deutsche Bank AG"
        payload["foreign_bank_country"] = "Germany"
        payload["foreign_bank_address"] = "Taunusanlage 12, Frankfurt"
        payload["foreign_bank_swift_code"] = "DEUTDEFF"
        payload["advising_status"] = "Advised"
        payload["communication_bank_id"] = str(fx.bank_hsbc.id)

    res = client.post(
        "/api/v1/end-user/lg-records/",
        headers=headers,
        data={"lg_record_in": json.dumps(payload)},
    )
    assert res.status_code == 201, f"Failed creating LG {suffix}: {res.text}"
    return res.json()["id"]


def run_all_tests():
    install_in_memory_gcs()
    email_harness = install_email_capture()
    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="P3_SUITE")
    client = TestClient(app)

    orig_pub_pdf = public_ep.generate_multipage_pdf_from_html
    orig_pub_send = public_ep.send_email
    public_ep.send_email = email_harness.mock_send_email
    trial_email = f"trial.lead.{fx.uid.lower()}@acme-treasury-sandbox.com"
    test_ips = []

    try:
        today = date.today()
        maker_h = make_auth_headers(fx.maker_user)
        restricted_h = make_auth_headers(fx.restricted_maker)
        checker_h = make_auth_headers(fx.checker_user)
        admin_h = make_auth_headers(fx.admin_user)
        admin2_h = make_auth_headers(fx.admin2_user)

        # ------------------------------------------------------------------
        # 1. Canonical cooldown key, thread safety & soft-delete verification
        # ------------------------------------------------------------------
        print("[1/7] Testing instruction_verification canonical cooldown, thread safety & soft-delete...")
        synthetic_id = 987654321
        tok_v1 = f"I.{synthetic_id}.fakesig111111111111"
        tok_v2 = f"  I2.{synthetic_id}.fakesig222222222222 \n"
        tok_legacy = f"I.{synthetic_id}.1760000000.fakesig3333333333"
        assert _canonical_token_key(tok_v1) == f"I:{synthetic_id}"
        assert _canonical_token_key(tok_v2) == f"I:{synthetic_id}"
        assert _canonical_token_key(tok_legacy) == f"I:{synthetic_id}"

        long_garbage_a = "X." * 20 + "alpha"
        long_garbage_b = "X." * 20 + "bravo"
        assert _canonical_token_key(long_garbage_a) != _canonical_token_key(long_garbage_b)

        cd_ip = f"10.210.1.{os.getpid() % 200 + 10}"
        test_ips.append(cd_ip)

        # Attempt 1 with v1, Attempt 2 with whitespace-padded v2, Attempt 3 with 4-part legacy -> triggers cooldown!
        f1, cd1, _ = _record_failed_attempt(cd_ip, tok_v1)
        f2, cd2, _ = _record_failed_attempt(cd_ip, tok_v2)
        f3, cd3, _ = _record_failed_attempt(cd_ip, tok_legacy)
        assert (f1, cd1) == (1, 0)
        assert (f2, cd2) == (2, 0)
        assert f3 == 3 and cd3 >= 60
        assert _check_cooldown_status(cd_ip, tok_v1) > 0
        assert _check_cooldown_status(cd_ip, tok_v2) > 0

        # Concurrent multi-threaded failed attempts -> exact total count and intact JSON store
        race_ip = f"10.210.2.{os.getpid() % 200 + 10}"
        test_ips.append(race_ip)
        race_tok = f"M2.{synthetic_id + 1}.racesig111111111111"

        def _hit(_idx):
            _record_failed_attempt(race_ip, race_tok)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(_hit, range(16)))

        with _COOLDOWN_LOCK:
            canon_entry = _COOLDOWN_MEMORY_STORE.get(f"ip_tok:{race_ip}:{_canonical_token_key(race_tok)}")
        assert canon_entry and int(canon_entry["fails"]) == 16, f"Lost update under concurrency: {canon_entry}"
        assert os.path.exists(_get_cooldown_file_path())
        with open(_get_cooldown_file_path(), "r", encoding="utf-8") as cf:
            parsed_cd = json.load(cf)
            assert isinstance(parsed_cd, dict)

        # Soft-delete tests on custody LG + IssuanceMaintenanceAction
        lg_soft_id = _create_custody_lg(client, fx, maker_h, "SOFT", today)
        r_ext = client.post(
            f"/api/v1/end-user/lg-records/{lg_soft_id}/extend",
            headers=maker_h,
            json={"new_expiry_date": (today + timedelta(days=240)).isoformat()},
        )
        assert r_ext.status_code == 200, r_ext.text
        soft_inst_id = r_ext.json()["latest_instruction_id"]
        soft_inst = db.get(LGInstruction, soft_inst_id)
        soft_lg = db.get(LGRecord, lg_soft_id)
        ts_i = _to_epoch_seconds(getattr(soft_inst, "created_at", None) or getattr(soft_inst, "instruction_date", None))
        tok_i, pin_i = generate_verification_token_and_code("I", soft_inst.id, soft_inst.serial_number, soft_lg.lg_number, ts_i)

        # Verify valid first, then soft-delete parent LGRecord
        assert inspect_verification_token(db, f"  {tok_i}  ", client_ip="10.210.3.1")["valid_token"] is True
        soft_lg.is_deleted = True
        db.commit()
        _RECENT_INSTRUCTIONS_CACHE.clear()
        meta_del_lg = _resolve_instruction_meta(f"<!-- GROW_VERIFY_META:kind=I|id={soft_inst.id}|serial={soft_inst.serial_number} -->")
        assert meta_del_lg["kind"] == "P", f"Deleted parent LGRecord must not resolve: {meta_del_lg}"
        assert inspect_verification_token(db, tok_i, client_ip="10.210.3.2")["valid_token"] is False
        assert verify_instruction_with_code(
            db, tok_i, pin_i, lg_tail=soft_lg.lg_number[-2:], client_ip="10.210.3.3"
        )["verified"] is False
        # Restore custody LG so uq_lg_sequence_per_entity advances cleanly for later custody LGs
        soft_lg.is_deleted = False
        db.commit()

        # IssuanceMaintenanceAction soft-delete + parent IssuedLGRecord soft-delete
        issued_soft = IssuedLGRecord(
            lg_ref_number=f"ISS-P3-SOFT-{fx.uid}",
            internal_serial=f"2026-P3S-{fx.uid}",
            bank_lg_number=f"CIB-P3S-{fx.uid}-77",
            customer_id=fx.cust_alpha.id,
            bank_id=fx.bank_cib.id,
            issuing_entity_id=fx.entity_alpha_hq.id,
            beneficiary_name="Soft Delete Beneficiary",
            current_amount=Decimal("100000.00"),
            currency_id=fx.ccy_egp.id,
            issue_date=today - timedelta(days=10),
            expiry_date=today + timedelta(days=90),
            status="ACTIVE",
            issued_by_user_id=fx.maker_user.id,
        )
        db.add(issued_soft)
        db.flush()
        maint_soft = IssuanceMaintenanceAction(
            issued_lg_id=issued_soft.id,
            action_type="EXTEND",
            action_data={"new_expiry_date": (today + timedelta(days=150)).isoformat()},
            status="EXECUTED",
            instruction_status="Instruction Issued",
            letter_serial_number=f"MNT-P3S-{fx.uid}",
            initiated_by_user_id=fx.maker_user.id,
            is_deleted=False,
        )
        db.add(maint_soft)
        db.commit()
        db.refresh(maint_soft)
        ts_m = _to_epoch_seconds(maint_soft.created_at)
        tok_m, pin_m = generate_verification_token_and_code("M", maint_soft.id, maint_soft.letter_serial_number, issued_soft.bank_lg_number, ts_m)
        assert inspect_verification_token(db, tok_m, client_ip="10.210.3.4")["valid_token"] is True

        # Soft-delete action itself
        maint_soft.is_deleted = True
        db.commit()
        _RECENT_INSTRUCTIONS_CACHE.clear()
        meta_del_m = _resolve_instruction_meta(f"Ref: <strong>{maint_soft.letter_serial_number}</strong>")
        assert meta_del_m["kind"] == "P"
        assert inspect_verification_token(db, tok_m, client_ip="10.210.3.5")["valid_token"] is False
        assert verify_instruction_with_code(
            db, tok_m, pin_m, lg_tail=issued_soft.bank_lg_number[-2:], client_ip="10.210.3.6"
        )["verified"] is False

        # Restore action and mark status="CANCELLED" -> verify_instruction_with_code reports CANCELED category
        maint_soft.is_deleted = False
        maint_soft.status = "CANCELLED"
        db.commit()
        _RECENT_INSTRUCTIONS_CACHE.clear()
        res_cancel_m = verify_instruction_with_code(
            db, tok_m, pin_m, lg_tail=issued_soft.bank_lg_number[-2:], client_ip="10.210.3.7"
        )
        assert res_cancel_m["verified"] is True and res_cancel_m["status_category"] == "CANCELED"

        # ------------------------------------------------------------------
        # 2. Single Bank Reminder: QR & Security Code bind to reminder serial
        # ------------------------------------------------------------------
        print("[2/7] Testing send_bank_reminder verification marker & QR binding...")
        rem_tmpl = db.query(Template).filter(
            Template.action_type == "LG_REMINDER_TO_BANKS",
            Template.is_notification_template == False,
            Template.is_deleted == False,
        ).first()
        if not rem_tmpl:
            rem_tmpl = Template(
                name=f"Phase3 Reminder {fx.uid}",
                template_type="LG_INSTRUCTION",
                action_type="LG_REMINDER_TO_BANKS",
                content="<html><body><p>Ref: <strong>{{serial_number}}</strong></p><p>Follow-up on {{original_instruction_serial}} for {{lg_number}}</p></body></html>",
                is_global=False,
                customer_id=fx.cust_alpha.id,
                is_notification_template=False,
            )
            db.add(rem_tmpl)
            db.commit()
        db.commit()

        lg_rem_id = _create_custody_lg(client, fx, maker_h, "REM", today)
        r_ext_rem = client.post(
            f"/api/v1/end-user/lg-records/{lg_rem_id}/extend",
            headers=maker_h,
            json={"new_expiry_date": (today + timedelta(days=270)).isoformat()},
        )
        assert r_ext_rem.status_code == 200, r_ext_rem.text
        orig_inst_id = r_ext_rem.json()["latest_instruction_id"]
        orig_inst = db.get(LGInstruction, orig_inst_id)
        lg_rem = db.get(LGRecord, lg_rem_id)

        _RECENT_INSTRUCTIONS_CACHE.clear()
        _, rem_inst_id, rem_pdf_bytes = asyncio.run(
            crud_lg_instruction.send_bank_reminder(
                db=db,
                original_instruction_id=orig_inst.id,
                user_id=fx.maker_user.id,
                customer_id=fx.cust_alpha.id,
            )
        )
        db.commit()
        assert rem_pdf_bytes, "send_bank_reminder should produce PDF bytes"
        rem_inst = db.get(LGInstruction, rem_inst_id)
        assert rem_inst and rem_inst.serial_number != orig_inst.serial_number

        pdf_info = extract_pdf_info(rem_pdf_bytes)
        pin_m = re.search(r"Security Code:\s*([0-9]{4})", pdf_info["text"])
        assert pin_m, f"Expected 4-digit footer PIN in reminder PDF text:\n{pdf_info['text']}"
        printed_pin = pin_m.group(1)

        rem_ts = _to_epoch_seconds(getattr(rem_inst, "created_at", None) or getattr(rem_inst, "instruction_date", None))
        rem_tok, _ = generate_verification_token_and_code("I", rem_inst.id, rem_inst.serial_number, lg_rem.lg_number, rem_ts)
        orig_ts = _to_epoch_seconds(getattr(orig_inst, "created_at", None) or getattr(orig_inst, "instruction_date", None))
        orig_tok, _ = generate_verification_token_and_code("I", orig_inst.id, orig_inst.serial_number, lg_rem.lg_number, orig_ts)

        r_ver_rem = verify_instruction_with_code(
            db, rem_tok, printed_pin, lg_tail=lg_rem.lg_number[-2:], client_ip="10.210.4.1"
        )
        assert r_ver_rem["verified"] is True and r_ver_rem["instruction_serial"] == rem_inst.serial_number
        r_ver_orig = verify_instruction_with_code(
            db, orig_tok, printed_pin, lg_tail=lg_rem.lg_number[-2:], client_ip="10.210.4.2"
        )
        assert r_ver_orig["verified"] is False

        # ------------------------------------------------------------------
        # 3. Maintenance mark-printed & record_bank_reply on Printed
        # ------------------------------------------------------------------
        print("[3/7] Testing maintenance mark-printed guards & bank reply after Printed...")
        issued_hq = IssuedLGRecord(
            lg_ref_number=f"ISS-HQ-{fx.uid}",
            internal_serial=f"2026-HQ-{fx.uid}",
            bank_lg_number=f"CIB-HQ-{fx.uid}-11",
            customer_id=fx.cust_alpha.id,
            bank_id=fx.bank_cib.id,
            issuing_entity_id=fx.entity_alpha_hq.id,
            beneficiary_name="HQ Power Grid",
            current_amount=Decimal("200000.00"),
            currency_id=fx.ccy_egp.id,
            issue_date=today - timedelta(days=20),
            expiry_date=today + timedelta(days=60),
            status="ACTIVE",
            issued_by_user_id=fx.maker_user.id,
        )
        issued_sub = IssuedLGRecord(
            lg_ref_number=f"ISS-SUB-{fx.uid}",
            internal_serial=f"2026-SUB-{fx.uid}",
            bank_lg_number=f"HSBC-SUB-{fx.uid}-22",
            customer_id=fx.cust_alpha.id,
            bank_id=fx.bank_hsbc.id,
            issuing_entity_id=fx.entity_alpha_sub.id,
            beneficiary_name="Subsidiary Grid",
            current_amount=Decimal("300000.00"),
            currency_id=fx.ccy_usd.id,
            issue_date=today - timedelta(days=20),
            expiry_date=today + timedelta(days=90),
            status="ACTIVE",
            issued_by_user_id=fx.maker_user.id,
        )
        db.add_all([issued_hq, issued_sub])
        db.flush()

        maint_pending = IssuanceMaintenanceAction(
            issued_lg_id=issued_hq.id,
            action_type="EXTEND",
            action_data={"new_expiry_date": (today + timedelta(days=120)).isoformat()},
            status="PENDING_APPROVAL",
            instruction_status="Instruction Issued",
            letter_serial_number=f"MNT-PEND-{fx.uid}",
            initiated_by_user_id=fx.maker_user.id,
        )
        maint_rejected = IssuanceMaintenanceAction(
            issued_lg_id=issued_hq.id,
            action_type="EXTEND",
            action_data={"new_expiry_date": (today + timedelta(days=120)).isoformat()},
            status="REJECTED",
            instruction_status="Instruction Issued",
            letter_serial_number=f"MNT-REJ-{fx.uid}",
            initiated_by_user_id=fx.maker_user.id,
        )
        maint_exec_hq = IssuanceMaintenanceAction(
            issued_lg_id=issued_hq.id,
            action_type="EXTEND",
            action_data={"new_expiry_date": (today + timedelta(days=150)).isoformat()},
            status="EXECUTED",
            instruction_status="Instruction Issued",
            letter_serial_number=f"MNT-HQ-{fx.uid}",
            initiated_by_user_id=fx.maker_user.id,
        )
        maint_exec_sub = IssuanceMaintenanceAction(
            issued_lg_id=issued_sub.id,
            action_type="EXTEND",
            action_data={"new_expiry_date": (today + timedelta(days=150)).isoformat()},
            status="EXECUTED",
            instruction_status="Instruction Issued",
            letter_serial_number=f"MNT-SUB-{fx.uid}",
            initiated_by_user_id=fx.maker_user.id,
        )
        maint_reply_after_print = IssuanceMaintenanceAction(
            issued_lg_id=issued_hq.id,
            action_type="CLOSE",
            action_data={"close_reason": "Completed"},
            status="EXECUTED",
            instruction_status="Instruction Issued",
            letter_serial_number=f"MNT-CLS-{fx.uid}",
            initiated_by_user_id=fx.maker_user.id,
        )
        db.add_all([maint_pending, maint_rejected, maint_exec_hq, maint_exec_sub, maint_reply_after_print])
        db.commit()
        for row in (maint_pending, maint_rejected, maint_exec_hq, maint_exec_sub, maint_reply_after_print):
            db.refresh(row)
        db.commit()

        # Non-EXECUTED cannot be marked printed (400)
        assert client.post(
            f"/api/v1/issuance/maintenance/{maint_pending.id}/mark-printed",
            headers=maker_h,
        ).status_code == 400

        # GRACE blocked from mark-printed (403)
        fx.cust_alpha.status = SubscriptionStatus.GRACE
        db.commit()
        assert client.post(
            f"/api/v1/issuance/maintenance/{maint_reply_after_print.id}/mark-printed",
            headers=maker_h,
        ).status_code == 403
        fx.cust_alpha.status = SubscriptionStatus.ACTIVE
        db.commit()

        # Mark printed sets both instruction_status='Printed' and is_printed=True; then record_bank_reply succeeds
        r_mp = client.post(
            f"/api/v1/issuance/maintenance/{maint_reply_after_print.id}/mark-printed",
            headers=maker_h,
        )
        assert r_mp.status_code == 200, r_mp.text
        db.refresh(maint_reply_after_print)
        assert maint_reply_after_print.instruction_status == "Printed"
        assert maint_reply_after_print.is_printed is True

        replied_action = maintenance_service.record_bank_reply(
            db=db,
            action_id=maint_reply_after_print.id,
            user_id=fx.maker_user.id,
            bank_reply_notes="Bank confirmed Printed close notice",
            customer_id=fx.cust_alpha.id,
        )
        assert replied_action.instruction_status == "Confirmed by Bank"
        db.commit()

        # ------------------------------------------------------------------
        # 4. Bulk Print Guards (PENDING/REJECTED, Custody Module Gate, Entity Auth)
        # ------------------------------------------------------------------
        print("[4/7] Testing Bulk Print status, module-gate & issuing_entity_id authorization...")
        assert client.post(
            "/api/v1/end-user/action-center/bulk-print",
            headers=maker_h,
            json={"items": [
                {"source": "issuance_maintenance", "id": maint_pending.id},
                {"source": "issuance_maintenance", "id": maint_rejected.id},
            ]},
        ).status_code == 404
        assert client.post(
            "/api/v1/end-user/action-center/bulk-print-reprint",
            headers=maker_h,
            json={"items": [{"source": "issuance_maintenance", "id": maint_rejected.id}]},
        ).status_code == 404

        # Restricted user (only entity_alpha_hq) cannot bulk-print or reprint entity_alpha_sub's maintenance action
        assert client.post(
            "/api/v1/end-user/action-center/bulk-print",
            headers=restricted_h,
            json={"items": [{"source": "issuance_maintenance", "id": maint_exec_sub.id}]},
        ).status_code == 404

        # Execute valid bulk print by full maker including maint_exec_sub + custody orig_inst
        r_bp_full = client.post(
            "/api/v1/end-user/action-center/bulk-print",
            headers=maker_h,
            json={"items": [
                {"source": "custody", "id": orig_inst.id},
                {"source": "issuance_maintenance", "id": maint_exec_hq.id},
                {"source": "issuance_maintenance", "id": maint_exec_sub.id},
            ]},
        )
        assert r_bp_full.status_code == 200, r_bp_full.text

        # Restricted user calling bulk-print-history only sees maint_exec_hq and orig_inst (entity_alpha_hq), NOT maint_exec_sub
        r_hist_rest = client.get("/api/v1/end-user/action-center/bulk-print-history", headers=restricted_h)
        assert r_hist_rest.status_code == 200
        rest_ids = {(i["source"], i["id"]) for b in r_hist_rest.json() for i in b["items"]}
        assert ("issuance_maintenance", maint_exec_hq.id) in rest_ids
        assert ("issuance_maintenance", maint_exec_sub.id) not in rest_ids

        # Restricted user reprinting mixed batch skips maint_exec_sub
        r_rep_rest = client.post(
            "/api/v1/end-user/action-center/bulk-print-reprint",
            headers=restricted_h,
            json={"items": [{"source": "issuance_maintenance", "id": maint_exec_sub.id}]},
        )
        assert r_rep_rest.status_code == 404

        # Plan with has_custody_module=False (Issuance-only) cannot bulk-print or list custody items in history
        fx.plan.has_custody_module = False
        db.commit()
        try:
            assert client.post(
                "/api/v1/end-user/action-center/bulk-print-reprint",
                headers=maker_h,
                json={"items": [{"source": "custody", "id": orig_inst.id}]},
            ).status_code == 404
            r_hist_nocust = client.get("/api/v1/end-user/action-center/bulk-print-history", headers=maker_h)
            assert r_hist_nocust.status_code == 200, r_hist_nocust.text
            nocust_sources = {i["source"] for b in r_hist_nocust.json() for i in b["items"]}
            assert "custody" not in nocust_sources and "custody_discrepancy" not in nocust_sources
        finally:
            fx.plan.has_custody_module = True
            db.commit()

        # ------------------------------------------------------------------
        # 5. LGRecordMinimalOut Foreign/Advising Bank Fields on Discrepancy List
        # ------------------------------------------------------------------
        print("[5/7] Testing LGRecordMinimalOut advising/foreign bank fields...")
        lg_for_id = _create_custody_lg(client, fx, maker_h, "FOR", today, foreign=True)
        r_ext_for = client.post(
            f"/api/v1/end-user/lg-records/{lg_for_id}/extend",
            headers=maker_h,
            json={"new_expiry_date": (today + timedelta(days=300)).isoformat()},
        )
        assert r_ext_for.status_code == 200, r_ext_for.text
        for_inst = db.get(LGInstruction, r_ext_for.json()["latest_instruction_id"])
        det = dict(for_inst.details or {})
        det["bank_terms_admin_approval"] = {
            "status": "REJECTED_BY_ADMIN",
            "rejected_at": datetime.utcnow().isoformat(),
            "rejected_bank_reply_date": today.isoformat(),
        }
        for_inst.details = det
        flag_modified(for_inst, "details")
        db.add(for_inst)
        db.commit()

        r_disc = client.get("/api/v1/end-user/action-center/discrepancy-letters-pending-print", headers=maker_h)
        assert r_disc.status_code == 200, r_disc.text
        disc_match = next((x for x in r_disc.json() if x["id"] == for_inst.id), None)
        assert disc_match and disc_match.get("lg_record"), f"Expected discrepancy item {for_inst.id}"
        rec_out = disc_match["lg_record"]
        assert rec_out["foreign_bank_name"] == "Deutsche Bank AG"
        assert rec_out["advising_status"] == "Advised"
        assert rec_out["communication_bank_id"] == fx.bank_hsbc.id
        assert rec_out["communication_bank"]["id"] == fx.bank_hsbc.id

        # ------------------------------------------------------------------
        # 6. Public Trial Registration Email Guard & HTML Escaping
        # ------------------------------------------------------------------
        print("[6/7] Testing register_free_trial missing PDF guard & HTML escaping...")
        async def _mock_none_pdf(*args, **kwargs):
            return None

        public_ep.generate_multipage_pdf_from_html = _mock_none_pdf
        email_harness.clear()
        org_xss = '<script>alert("xss")</script> & Co'
        contact_xss = '<img src=x onerror=1> Lead'
        r_trial = client.post(
            "/api/v1/public/register-free-trial/",
            data={
                "organization_name": org_xss,
                "organization_address": "1 Nile Tower",
                "contact_admin_name": contact_xss,
                "contact_phone": "+201001234567",
                "admin_email": trial_email,
                "entities_count": "2",
                "requested_modules": "custody,issuance",
                "accepted_terms": "true",
            },
        )
        assert r_trial.status_code == 201, r_trial.text
        sent_to_trial = email_harness.find_by_recipient(trial_email)
        assert len(sent_to_trial) == 1
        msg_trial = sent_to_trial[0]
        assert msg_trial["attachments_count"] == 0, "Must not attach empty/missing Terms PDF"
        assert "attached for your records" not in msg_trial["body"]
        assert "<script>" not in msg_trial["body"] and "&lt;script&gt;" in msg_trial["body"]

        # ------------------------------------------------------------------
        # 7. Onboarding Activation Email Attachment Guard & HTML Escaping
        # ------------------------------------------------------------------
        print("[7/7] Testing onboarding activation email attach_guide=False & HTML escaping...")
        email_harness.clear()
        ok_send, err_send = asyncio.run(
            send_corporate_admin_activation_email(
                db=db,
                user=fx.maker_user,
                customer_name='<b>Alpha & "Quoted"</b>',
                custom_role_label="<Treasury>",
                is_trial=True,
                recipient_name="<Maker>",
                module_labels=["LG Custody & Issuance", "<script>Mod</script>"],
                attach_guide=False,
            )
        )
        assert ok_send is True and err_send is None
        sent_onboard = email_harness.find_by_recipient(fx.maker_user.email)
        assert len(sent_onboard) == 1
        msg_onboard = sent_onboard[0]
        assert msg_onboard["attachments_count"] == 0
        assert "Attached as a PDF" not in msg_onboard["body"]
        assert "<b>Alpha" not in msg_onboard["body"] and "&lt;b&gt;Alpha &amp;" in msg_onboard["body"]
        assert "<script>Mod</script>" not in msg_onboard["body"]

        # unused vars keep pyflakes clean
        _ = (checker_h, admin_h, admin2_h)

        print("\nALL PHASE 3 VERIFICATION, BULK PRINT & ONBOARDING TESTS PASSED!")

    finally:
        public_ep.generate_multipage_pdf_from_html = orig_pub_pdf
        public_ep.send_email = orig_pub_send

        try:
            from app.models.models import TrialRegistration, AuditLog
            from app.api.v1.endpoints.system_owner import _suspend_and_revoke_trial_registration_sync
            treg_cleanup = db.query(TrialRegistration).filter(TrialRegistration.admin_email == trial_email).first()
            if treg_cleanup:
                treg_cid = treg_cleanup.id
                if treg_cleanup.customer_id:
                    _suspend_and_revoke_trial_registration_sync(db, treg_cleanup, caller_user_id=None, reason="Phase 3 test cleanup")
                db.query(AuditLog).filter(AuditLog.entity_type == "TrialRegistration", AuditLog.entity_id == treg_cid).delete(synchronize_session=False)
            db.execute(text("DELETE FROM trial_registrations WHERE admin_email = :em"), {"em": trial_email})
            db.commit()
        except Exception:
            db.rollback()

        with _COOLDOWN_LOCK:
            for ip in test_ips:
                for k in [k for k in list(_COOLDOWN_MEMORY_STORE.keys()) if ip in k]:
                    _COOLDOWN_MEMORY_STORE.pop(k, None)
            _COOLDOWN_MEMORY_STORE.pop(f"letter:I:{987654321}", None)
            _COOLDOWN_MEMORY_STORE.pop(f"letter:M:{987654322}", None)
            _save_cooldown_store(_COOLDOWN_MEMORY_STORE)

        fx.cleanup()
        db.close()


if __name__ == "__main__":
    run_all_tests()
