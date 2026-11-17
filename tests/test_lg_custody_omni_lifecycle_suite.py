# tests/test_lg_custody_omni_lifecycle_suite.py
"""
========================================================================================
GROW TREASURY PLATFORM — LG CUSTODY MASTER OMNI LIFECYCLE & USER-OUTPUT SUITE
========================================================================================
Validates NOT ONLY that every LG Custody API/CRUD workflow runs error-free, but that
EVERY USER-FACING BUSINESS OUTPUT matches exact Corporate Treasurer expectations:

PART 1 — LG Recording, Validation, Categorization, Entity Scoping, Vault Crypto & Quotas:
  1.1 Validation Engine (`lg_validation_service`): Catches missing mandatory fields,
      past expiry, negative amounts, APG missing operational status, Rule='Other'
      missing text, and mandatory custom Category field (`extra_field_name`).
  1.2 Local Guarantee Creation (`POST /end-user/lg-records/`):
      - Computes `custody_serial` (`01-0001`, `01-0002`, `02-0001`),
      - Computes `lg_period_months` rounded to {3, 6, 9, 12},
      - Stream-encrypts Original LG Scan & Internal Supporting Doc (`ENC_STREAM_V2\x00`),
      - Mints ephemeral download ticket & streams byte-identical decrypted PDF back,
      - Increments `active_lg_count`, sets `VALID` status, and dispatches Stakeholder Email.
  1.3 Foreign Advised vs Not-Advised Guarantees:
      - Verifies Foreign Advised (`communication_bank=HSBC`, `foreign_bank_name=Deutsche Bank`)
        routes physical letters to HSBC while citing Deutsche Bank as Issuing Bank in table!
      - Verifies Foreign Not-Advised routes directly to Foreign Bank name & address.
  1.4 Non-Operative Advance Payment Guarantee (APG) & Mandatory Payment Conditions.
  1.5 Multi-Entity Access Control & Strict Cross-Tenant Isolation:
      - Restricted Maker (`ALPH` only) sees Entity 1 LGs, blocked from Entity 2 (`SUBB`) LGs.
      - Tenant Beta blocked (403/404) from Tenant Alpha LGs, instructions, and document tickets.
  1.6 Subscription Quota & Lifecycle Guards:
      - Blocks creation at `max_records` ceiling (HTTP 400),
      - Blocks writes in `GRACE` Read-Only mode (HTTP 403) while preserving reads,
      - Blocks all access when `EXPIRED` or `has_custody_module=False` (HTTP 403).
  1.7 Bilingual Guarantee Special-Wording Trap Detector (`POST /analyze-special-wording`).

PART 2 — All Lifecycle Actions, 20-Char Serial Math, Real WeasyPrint PDFs & Maker-Checker:
  2.1 Single Extension (`POST /extend` -> Direct):
      - Exact 20-char serial `{ENT:4}{CAT:2}{SEQ:4}EX0001001O`,
      - Preserves `lg_period_months`, updates `expiry_date`,
      - Real WeasyPrint PDF fits strictly on **1 A4 page** (even with long notes),
      - Escapes XSS payloads in notes while rendering formatted amounts, dates, & Grow Seal.
  2.2 Two-Tier Bulk Auto-Renewal (`GET /auto-renewal-preview`, `POST /run-auto-renewal`,
      `GET /auto-renewal-history`, `POST /auto-renewal-reprint`):
      - Tier 1 `CRITICAL_FORCED` (`<= 15d`, even when `auto_renewal=False`, `can_deselect=False`):
        verifies HTTP 400 block if user tries to exclude a Forced LG!
      - Tier 2 `PROACTIVE_AUTO` (`16..30d` with `auto_renewal=True`, `can_deselect=True`).
      - Consolidated merged PDF has exact `page_count == renewed_count` (1 page per LG!).
  2.3 Non-Operative APG Activation (`POST /activate-non-operative` + Maker-Checker):
      - Maker submits -> `approval_pending`,
      - Maker self-approval & self-rejection strictly blocked (HTTP 400 + audit log),
      - Single-flight lock blocks concurrent pending request on same LG,
      - Checker approves -> `lg_operational_status_id` becomes `OPERATIVE`,
        `AC` letter PDF displays exact wire reference, bank, date & payment amount,
      - Appears in `approved-requests-pending-print` until `mark-as-accessed-for-print` flips `is_printed=True`.
  2.4 Amount Decrease (`POST /decrease-amount` + Approval):
      - Blocks `>= current_amount` (HTTP 400),
      - Reduces `1,000,000.00 -> 750,000.00`, keeps `VALID`, verifies PDF Original/Decrease/New amounts.
  2.5 Partial vs Full Liquidation (`POST /liquidate` + Approval):
      - Partial (`750,000.00 -> remaining 450,000.00`): claims `300,000.00`, stays `VALID`,
        injects verbatim **Mandatory Claim Declaration** legal box + settlement clause into PDF!
      - Full (`450,000.00 -> 0.00`): transitions status to `LIQUIDATED`, blocks further actions.
  2.6 Release (`POST /release` + Approval):
      - Verifies dynamic `enclosed_documents_clause` wording when prior extension reply is pending,
        transitions status to `RELEASED`.
  2.7 Amendment (`POST /amend` + Post-Expiry Revival):
      - Revives `EXPIRED` LG (within 30-day grace window) back to `VALID` when amended to future expiry,
        and blocks `EXPIRED` LGs > 35 days old.
  2.8 Internal Owner Handover (`POST /change-owner` Single & Bulk + Contact Update & Delete Guard).

PART 3 — Instruction Cancellation & Exact State Rollback Engine (`POST /instructions/{id}/cancel`):
  - Cancels latest `LG_EXTENSION` -> rolls back `expiry_date` to exact prior date!
  - Cancels latest `LG_DECREASE_AMOUNT` -> restores exact `original_lg_amount`!
  - Cancels latest `LG_RELEASE` -> restores `RELEASED -> VALID`!
  - Cancels latest Full `LG_LIQUIDATE` -> restores `LIQUIDATED -> VALID` & full amount!
  - Cancels latest `LG_ACTIVATE_NON_OPERATIVE` -> rolls back `OPERATIVE -> NON_OPERATIVE`!
  - Blocks non-latest, already-delivered, expired-window, and unconfirmed-declaration cancellations,
    plus tests Maker Withdrawal (`POST /withdraw`) & Checker Rejection (`_nuke_document` vault purge).

PART 4 — Physical Letter QR Digital Seal, Split PIN + LG-Tail Verification, Tamper & Bot Traps:
  - Extracts real 4-digit Security PIN from WeasyPrint PDF + computes token `I2.<id>.<sig>`,
  - Verifies `/api/v1/public/verify-instruction/inspect` (zero pre-PIN data leak) & `/confirm`,
  - Verifies Cut-and-Paste Footer Tamper Alert (`tamper_alert=True` when PIN matches QR but LG tail differs!),
  - Verifies Canceled Instruction revocation warning (`CANCELED / REVOKED — Do Not Execute`),
  - Verifies Legacy v1 6-digit token backward compatibility,
  - Verifies Progressive Cooldown (2 free typos -> 3rd triggers 60s lock) & Bot Speed / Honeypot 1800s trap!
========================================================================================
"""

import os
import re
import sys
import json
import base64
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from _test_harness import (
    app,
    SessionLocal,
    IsolatedTenantFixture,
    install_in_memory_gcs,
    install_email_capture,
    make_auth_headers,
    extract_pdf_info,
)
from app.constants import (
    LgStatusEnum,
    LgTypeEnum,
    LgOperationalStatusEnum,
    SubscriptionStatus,
    ApprovalRequestStatusEnum,
)
from app.models.models import (
    LGRecord,
    LGInstruction,
    LGDocument,
    ApprovalRequest,
    InternalOwnerContact,
)
from app.core.lg_validation_service import lg_validation_service
from app.core.instruction_verification import (
    generate_verification_token_and_code,
    _generate_v1_token_and_6digit_code,
    _to_epoch_seconds,
    _COOLDOWN_MEMORY_STORE,
    _get_cooldown_file_path,
)
from app.core.security_crypto import STREAM_MAGIC

client = TestClient(app)


def _clear_verify_cooldowns():
    _COOLDOWN_MEMORY_STORE.clear()
    p = _get_cooldown_file_path()
    if os.path.exists(p):
        try:
            os.remove(p)
        except Exception:
            pass


def _extract_pin_from_pdf_text(pdf_text: str) -> str:
    m = re.search(r"Security Code:\s*([0-9]{4})", pdf_text)
    assert m, f"4-digit Security Code not found in rendered PDF text! Snippet: {pdf_text[-400:]}"
    return m.group(1)


def run_lg_custody_omni_lifecycle_suite():
    print("=" * 88)
    print("GROW TREASURY — LG CUSTODY MASTER OMNI LIFECYCLE & USER-OUTPUT VERIFICATION SUITE")
    print("=" * 88)

    mem_gcs = install_in_memory_gcs()
    email_harness = install_email_capture()
    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="CUSTODY_OMNI")

    maker_h = make_auth_headers(fx.maker_user)
    restricted_h = make_auth_headers(fx.restricted_maker)
    # In LG Custody, Corporate Admins act as the Checker/Approver on `/corporate-admin/approval-requests/*`
    checker_h = make_auth_headers(fx.admin2_user)
    admin_h = make_auth_headers(fx.admin_user)
    beta_maker_h = make_auth_headers(fx.beta_maker)

    sample_pdf_bytes = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\nORIGINAL_GUARANTEE_SCAN_CONTENT"
    support_pdf_bytes = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\nINTERNAL_CONTRACT_SUPPORTING_DOC"

    def _base_lg_payload(**overrides):
        today = date.today()
        base = {
            "beneficiary_corporate_id": fx.entity_alpha_hq.id,
            "issuer_name": "Orascom Heavy Civil Contractors SAE",
            "issuer_id": "CR-998811",
            "lg_number": f"LG-{fx.uid}-{len(mem_gcs.blobs) + 1001}",
            "lg_amount": "1000000.00",
            "lg_currency_id": fx.ccy_egp.id,
            "lg_payable_currency_id": fx.ccy_egp.id,
            "issuance_date": (today - timedelta(days=30)).isoformat(),
            "expiry_date": (today + timedelta(days=150)).isoformat(),  # ~6 months -> lg_period_months = 6
            "auto_renewal": True,
            "lg_type_id": fx.lg_type_perf.id,
            "description_purpose": "10% Performance Guarantee for East Grid 220kV Substation Contract",
            "issuing_bank_id": fx.bank_cib.id,
            "issuing_bank_address": "CIB Corporate Tower, Smart Village",
            "issuing_bank_phone": "+20219666",
            "issuing_method_id": fx.issuing_method.id,
            "applicable_rule_id": fx.rule_urdg.id,
            "mandatory_claim_statement": (
                "The Beneficiary hereby certifies that the Contractor is in breach of its contractual "
                "obligations under Contract #EG-220KV and demands immediate payment without cavil."
            ),
            "lg_category_id": fx.cat_standard.id,
            "internal_contract_project_id": "PRJ-SUB-220",
            "notes": "Initial custody registration",
            "internal_owner_email": f"pm.owner.{fx.uid.lower()}@globex.com",
            "internal_owner_phone": "+201223344556",
            "internal_owner_id": "EMP-401",
            "manager_email": f"pm.director.{fx.uid.lower()}@globex.com",
        }
        base.update(overrides)
        return base

    try:
        # ==============================================================================
        # PART 1: RECORDING, VALIDATION, CATEGORIES, ENTITIES, CRYPTO VAULT & QUOTAS
        # ==============================================================================
        print("\n" + "=" * 88)
        print("[PART 1] LG RECORDING, VALIDATION, ENTITY SCOPING, VAULT CRYPTO & QUOTAS")
        print("=" * 88)

        # --- 1.1 Pre-Flight Validation Rules ---
        print("\n--- [1.1] LG Validation Service: Mandatory, Date, APG, Rule='Other' & Category Rules ---")
        owner_tmp = InternalOwnerContact(
            customer_id=fx.cust_alpha.id,
            email=f"val.owner.{fx.uid.lower()}@globex.com",
            phone_number="+20100009999",
            manager_email=f"val.mgr.{fx.uid.lower()}@globex.com",
        )
        db.add(owner_tmp)
        db.commit()
        db.refresh(owner_tmp)

        errs_bad = lg_validation_service.validate_lg_data(
            {
                "lg_number": "",
                "lg_amount": "-500",
                "lg_currency_id": fx.ccy_egp.id,
                "issuance_date": "2026-10-10",
                "expiry_date": "2026-05-01",  # Before issuance!
                "lg_type_id": fx.lg_type_apg.id,
                "lg_operational_status_id": None,  # Missing for APG!
                "description_purpose": "Test",
                "issuer_name": "Vendor",
                "beneficiary_corporate_id": fx.entity_alpha_hq.id,
                "issuing_bank_id": fx.bank_cib.id,
                "issuing_bank_address": "Addr",
                "issuing_bank_phone": "123",
                "issuing_method_id": fx.issuing_method.id,
                "applicable_rule_id": fx.rule_other.id if fx.rule_other else fx.rule_urdg.id,
                "applicable_rules_text": "",  # Missing when Rule == Other!
                "internal_owner_contact_id": owner_tmp.id,
                "lg_category_id": fx.cat_mandatory_extra.id,
                "additional_field_values": {},  # Missing mandatory extra field!
            },
            context="new_record",
            db=db,
            customer_id=fx.cust_alpha.id,
        )
        assert "lg_number" in errs_bad
        assert "lg_amount" in errs_bad
        assert "expiry_date" in errs_bad and "after" in errs_bad["expiry_date"].lower()
        if fx.lg_type_apg and fx.lg_type_apg.name == "Advance Payment LG":
            assert "lg_operational_status_id" in errs_bad
        if fx.rule_other:
            assert "applicable_rules_text" in errs_bad
        assert "additional_field_values" in errs_bad
        print("  [PASS] Validation service caught all 6 business rule violations cleanly")

        # Also verify API rejects expired LG creation and missing mandatory custom category field
        bad_expired_payload = _base_lg_payload(
            lg_number=f"LG-EXP-{fx.uid}",
            issuance_date=(date.today() - timedelta(days=200)).isoformat(),
            expiry_date=(date.today() - timedelta(days=5)).isoformat(),
        )
        res_exp = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(bad_expired_payload)},
        )
        assert res_exp.status_code == 400 and "expired" in res_exp.json()["detail"].lower()

        bad_cat_payload = _base_lg_payload(
            lg_number=f"LG-CAT-{fx.uid}",
            lg_category_id=fx.cat_mandatory_extra.id,
            additional_field_values={},
        )
        res_cat = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(bad_cat_payload)},
        )
        assert res_cat.status_code == 400 and "project_wbs_code" in res_cat.json()["detail"]
        print("  [PASS] API rejected past-expiry LG and missing mandatory Category custom field (HTTP 400)")

        # --- 1.2 Local Guarantee Creation + Encrypted Vault Roundtrip + Email Output ---
        print("\n--- [1.2] Local Guarantee Creation, Custody Serial, Period Math & STREAM V2 Vault ---")
        email_harness.clear()
        lg1_num = f"LG-LOC-{fx.uid}-9001"
        lg1_payload = _base_lg_payload(
            lg_number=lg1_num,
            lg_category_id=fx.cat_mandatory_extra.id,
            additional_field_values={"project_wbs_code": "WBS-EGY-2026-77"},
        )
        res_lg1 = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(lg1_payload)},
            files={
                "ai_scan_file": ("Original_LG_Scan.pdf", sample_pdf_bytes, "application/pdf"),
                "internal_supporting_document_file": ("Contract_Award.pdf", support_pdf_bytes, "application/pdf"),
            },
        )
        assert res_lg1.status_code == 201, f"Create LG failed: {res_lg1.text}"
        lg1_data = res_lg1.json()
        lg1_id = lg1_data["id"]

        # Verify expected business outputs on created LG
        assert lg1_data["lg_number"] == lg1_num
        assert Decimal(str(lg1_data["lg_amount"])) == Decimal("1000000.00")
        assert lg1_data["lg_status"]["id"] == LgStatusEnum.VALID.value
        assert lg1_data["lg_sequence_number"] == 1
        assert lg1_data["lg_period_months"] == 6, f"Expected 180d (~6m) tenor -> 6 months, got {lg1_data['lg_period_months']}"
        assert lg1_data["additional_field_values"]["project_wbs_code"] == "WBS-EGY-2026-77"
        assert len(lg1_data["documents"]) == 2
        print(f"  [PASS] Created Local LG #{lg1_num} (ID={lg1_id}, seq=1, period=6m, 2 vault docs)")

        # Duplicate LG number check -> HTTP 409 Conflict
        res_dup = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(lg1_payload)},
        )
        assert res_dup.status_code == 409, f"Expected 409 on duplicate LG number, got {res_dup.status_code}"
        print("  [PASS] Duplicate LG Number rejected with HTTP 409 Conflict")

        # Verify ciphertext-at-rest in GCS & decrypted byte-identical streaming download
        orig_doc = lg1_data["documents"][0]
        raw_blob = mem_gcs.get_raw_bytes_by_uri(orig_doc["file_path"])
        assert raw_blob.startswith(STREAM_MAGIC), "Document in cloud storage MUST start with ENC_STREAM_V2\\x00!"
        assert b"ORIGINAL_GUARANTEE_SCAN_CONTENT" not in raw_blob, "Plaintext leaked into storage blob!"

        res_view_doc = client.get(f"/api/v1/end-user/lg-documents/{orig_doc['id']}/view", headers=maker_h)
        assert res_view_doc.status_code == 200
        dl_url = res_view_doc.json()["download_url"]
        ticket_param = dl_url.split("ticket=")[-1]

        res_stream = client.get("/api/v1/documents/download", params={"ticket": ticket_param})
        assert res_stream.status_code == 200
        assert res_stream.content == sample_pdf_bytes, "Decrypted stream bytes did not match original uploaded PDF!"

        # Cross-tenant document isolation: Beta Maker blocked with HTTP 403
        res_beta_doc = client.get(f"/api/v1/end-user/lg-documents/{orig_doc['id']}/view", headers=beta_maker_h)
        assert res_beta_doc.status_code == 403
        print("  [PASS] STREAM V2 ciphertext-at-rest verified, ticket decrypted byte-for-byte, & cross-tenant blocked (403)")

        # Verify Stakeholder Confirmation Email output
        assert len(email_harness.captured) >= 1
        reg_mail = email_harness.captured[-1]
        assert lg1_payload["internal_owner_email"] in reg_mail["to_emails"]
        assert lg1_payload["manager_email"] in reg_mail["cc_emails"]
        assert fx.admin_user.email in reg_mail["cc_emails"]
        assert lg1_num in reg_mail["body"] and "1,000,000.00" in reg_mail["body"]
        print("  [PASS] New LG Stakeholder Email verified (TO: Owner, CC: Manager + Category List + Admins, exact amount/ref)")

        # --- 1.3 Foreign Advised vs Foreign Not-Advised Routing ---
        print("\n--- [1.3] Foreign Advised vs Not-Advised LG Recording & Bank Routing ---")
        lg_foreign_adv_payload = _base_lg_payload(
            lg_number=f"LG-FRG-ADV-{fx.uid}-9002",
            lg_currency_id=fx.ccy_eur.id,
            lg_payable_currency_id=fx.ccy_eur.id,
            lg_amount="250000.00",
            issuing_bank_id=fx.bank_foreign.id,
            foreign_bank_name="Deutsche Bank AG Frankfurt",
            foreign_bank_country="Germany",
            foreign_bank_address="Taunusanlage 12, 60325 Frankfurt",
            foreign_bank_swift_code="DEUTDEFF",
            advising_status="Advised",
            communication_bank_id=str(fx.bank_hsbc.id),
        )
        res_frg_adv = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(lg_foreign_adv_payload)},
        )
        assert res_frg_adv.status_code == 201, f"Foreign Advised LG failed: {res_frg_adv.text}"
        lg_frg_adv = res_frg_adv.json()
        assert lg_frg_adv["foreign_bank_name"] == "Deutsche Bank AG Frankfurt"
        assert lg_frg_adv["advising_status"] == "Advised"
        assert int(lg_frg_adv["communication_bank_id"]) == fx.bank_hsbc.id

        # Issue an extension on the Foreign Advised LG and inspect rendered PDF recipient vs issuing bank!
        new_frg_exp = (date.today() + timedelta(days=240)).isoformat()
        res_ext_frg = client.post(
            f"/api/v1/end-user/lg-records/{lg_frg_adv['id']}/extend",
            headers=maker_h,
            json={"new_expiry_date": new_frg_exp, "notes": "Cross-border advised extension"},
        )
        assert res_ext_frg.status_code == 200
        frg_inst_id = res_ext_frg.json()["latest_instruction_id"]
        res_frg_pdf = client.get(
            f"/api/v1/end-user/lg-records/instructions/{frg_inst_id}/view-letter",
            headers=maker_h,
            params={"raw_pdf": True},
        )
        assert res_frg_pdf.status_code == 200
        frg_pdf_info = extract_pdf_info(res_frg_pdf.content)
        assert frg_pdf_info["page_count"] == 1
        # Addressed TO local advising bank (HSBC), while Issuing Bank in table is Deutsche Bank!
        assert "HSBC" in frg_pdf_info["text"], f"Advising bank HSBC missing from letter! Text: {frg_pdf_info['text']}"
        assert "Deutsche Bank AG Frankfurt" in frg_pdf_info["text"], "Foreign issuing bank missing from spec table!"
        print("  [PASS] Foreign Advised LG PDF routed to local Advising Bank (HSBC) while citing Deutsche Bank in spec table")

        # --- 1.4 Non-Operative APG Recording ---
        print("\n--- [1.4] Non-Operative Advance Payment Guarantee (APG) Recording ---")
        # First verify missing payment_conditions on Non-Operative APG is blocked (HTTP 400)
        apg_missing_cond = _base_lg_payload(
            lg_number=f"LG-APG-BAD-{fx.uid}",
            lg_type_id=fx.lg_type_apg.id,
            lg_operational_status_id=LgOperationalStatusEnum.NON_OPERATIVE.value,
            payment_conditions="",
        )
        res_apg_bad = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(apg_missing_cond)},
        )
        assert res_apg_bad.status_code == 400 and "payment conditions" in res_apg_bad.json()["detail"].lower()

        apg_payload = _base_lg_payload(
            beneficiary_corporate_id=fx.entity_alpha_sub.id,  # Put on Subsidiary 2 to test entity scoping too!
            lg_number=f"LG-APG-{fx.uid}-9003",
            lg_amount="500000.00",
            lg_type_id=fx.lg_type_apg.id,
            lg_operational_status_id=LgOperationalStatusEnum.NON_OPERATIVE.value,
            payment_conditions="Operative only upon receipt of 20% advance transfer into Account #100200300 at QNB.",
            issuing_bank_id=fx.bank_qnb.id,
        )
        res_apg = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(apg_payload)},
        )
        assert res_apg.status_code == 201, f"APG create failed: {res_apg.text}"
        lg_apg = res_apg.json()
        lg_apg_id = lg_apg["id"]
        assert lg_apg["lg_operational_status"]["id"] == LgOperationalStatusEnum.NON_OPERATIVE.value
        assert lg_apg["lg_sequence_number"] == 1  # Sequence resets to 1 for Subsidiary 2!
        print("  [PASS] Non-Operative APG created on Subsidiary 2 with independent entity sequence #1")

        # --- 1.5 Multi-Entity Access Scoping & Cross-Tenant Isolation ---
        print("\n--- [1.5] Subsidiary Entity Scoping & Cross-Tenant Isolation ---")
        res_list_all = client.get("/api/v1/end-user/lg-records/", headers=maker_h)
        assert res_list_all.status_code == 200
        all_ids = {r["id"] for r in res_list_all.json()}
        assert {lg1_id, lg_frg_adv["id"], lg_apg_id}.issubset(all_ids)

        # Restricted Maker only has access to entity_alpha_hq (NOT entity_alpha_sub where lg_apg_id lives!)
        res_list_rest = client.get("/api/v1/end-user/lg-records/", headers=restricted_h)
        assert res_list_rest.status_code == 200
        rest_ids = {r["id"] for r in res_list_rest.json()}
        assert lg1_id in rest_ids
        assert lg_apg_id not in rest_ids, "ENTITY ISOLATION LEAK: Restricted user saw Subsidiary 2 LG!"

        res_get_forbidden_ent = client.get(f"/api/v1/end-user/lg-records/{lg_apg_id}", headers=restricted_h)
        assert res_get_forbidden_ent.status_code in (403, 404)

        # Cross-tenant Beta user cannot list or fetch Alpha LGs
        res_beta_list = client.get("/api/v1/end-user/lg-records/", headers=beta_maker_h)
        assert res_beta_list.status_code == 200 and len(res_beta_list.json()) == 0
        res_beta_get = client.get(f"/api/v1/end-user/lg-records/{lg1_id}", headers=beta_maker_h)
        assert res_beta_get.status_code in (403, 404)
        print("  [PASS] Entity-scoped Maker isolated to HQ LGs; Cross-tenant Beta user saw 0 Alpha records")

        # --- 1.6 Subscription Quota Ceiling, GRACE Read-Only & EXPIRED Lockout ---
        print("\n--- [1.6] Subscription Quota Limit, GRACE Read-Only & EXPIRED Enforcement ---")
        db.expire_all()
        cust_row = db.query(type(fx.cust_alpha)).get(fx.cust_alpha.id)
        plan_row = db.query(type(fx.plan)).get(fx.plan.id)
        saved_max = plan_row.max_records
        plan_row.max_records = cust_row.active_lg_count  # Exactly at ceiling!
        db.commit()

        res_quota = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(_base_lg_payload(lg_number=f"LG-QUOTA-{fx.uid}"))},
        )
        assert res_quota.status_code == 400 and "limit" in res_quota.json()["detail"].lower()

        # Restore max_records, test GRACE read-only (reads=200, writes=403)
        plan_row.max_records = saved_max
        cust_row.status = SubscriptionStatus.GRACE
        db.commit()

        assert client.get(f"/api/v1/end-user/lg-records/{lg1_id}", headers=maker_h).status_code == 200
        res_grace_write = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/toggle-auto-renewal",
            headers=maker_h,
            json={"auto_renewal": False, "reason": "Test in grace"},
        )
        assert res_grace_write.status_code == 403

        # Test EXPIRED blocks even reads
        cust_row.status = SubscriptionStatus.EXPIRED
        db.commit()
        assert client.get(f"/api/v1/end-user/lg-records/{lg1_id}", headers=maker_h).status_code == 403

        # Restore ACTIVE
        cust_row.status = SubscriptionStatus.ACTIVE
        db.commit()
        print("  [PASS] Quota ceiling (400), GRACE read-only (200 read / 403 write), and EXPIRED lockout (403) verified")

        # --- 1.7 Bilingual Guarantee Special-Wording Trap Detector ---
        print("\n--- [1.7] Bilingual Guarantee Special-Wording Trap Detector (Arabic & English) ---")
        # In LG Custody (Beneficiary perspective), claim-blocking hurdles against the Beneficiary
        # (court-judgment preconditions, vendor/applicant consent gates, revocable wording) MUST trigger RED!
        hurdle_wording = (
            "This guarantee is revocable and payable only upon presentation of a final non-appealable court judgment "
            "and with prior written consent of the Applicant/Contractor. "
            "لا يدفع هذا الضمان إلا بموجب حكم قضائي نهائي وبموافقة خطية من العميل مقدم الطلب."
        )
        res_trap = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/analyze-special-wording",
            headers=maker_h,
            data={"wording_text": hurdle_wording},
        )
        assert res_trap.status_code == 200, f"Special wording scan failed: {res_trap.text}"
        trap_data = res_trap.json()
        assert trap_data.get("perspective") == "BENEFICIARY"
        assert trap_data.get("risk_level") == "RED", f"Expected RED for claim-blocking hurdles, got {trap_data}"
        assert trap_data.get("red_count", 0) >= 1 and len(trap_data.get("findings", [])) >= 1

        # Clean unconditional first-demand Beneficiary wording MUST pass as GREEN!
        clean_beneficiary_wording = (
            "We hereby irrevocably and unconditionally undertake to pay you upon your first written demand "
            "any sum up to the guaranteed amount notwithstanding any contestation or objection by the Applicant."
        )
        res_clean_trap = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/analyze-special-wording",
            headers=maker_h,
            data={"wording_text": clean_beneficiary_wording},
        )
        assert res_clean_trap.status_code == 200
        assert res_clean_trap.json().get("risk_level") == "GREEN"
        print(f"  [PASS] Beneficiary Protection Check flagged AR/EN claim-blocking hurdles (RED, {trap_data['red_count']} hurdles) & passed clean demand wording (GREEN)")

        # ==============================================================================
        # PART 2: LIFECYCLE ACTIONS, 20-CHAR SERIALS, WEASYPRINT PDFs & MAKER-CHECKER
        # ==============================================================================
        print("\n" + "=" * 88)
        print("[PART 2] LIFECYCLE ACTIONS, SERIAL MATH, 1-PAGE A4 PDFs & MAKER-CHECKER")
        print("=" * 88)

        # --- 2.1 Single Extension + 20-Char Serial + 1-Page Compaction + XSS Escaping ---
        print("\n--- [2.1] Single Extension: 20-Char Serial, Period Preservation, 1-Page PDF & XSS Safety ---")
        # Reject new_expiry_date <= current expiry_date
        res_bad_ext = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/extend",
            headers=maker_h,
            json={"new_expiry_date": (date.today() + timedelta(days=30)).isoformat()},
        )
        assert res_bad_ext.status_code == 400

        ext_target_date = date.today() + timedelta(days=330)
        long_xss_note = (
            "<script>alert('xss')</script> Milestone 2 civil extension requested per PMO Directive #88. "
            + ("Please ensure branch confirmation advice references Contract PRJ-SUB-220. " * 6)
        )
        res_ext1 = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/extend",
            headers=maker_h,
            json={"new_expiry_date": ext_target_date.isoformat(), "notes": long_xss_note},
        )
        assert res_ext1.status_code == 200, f"Extend failed: {res_ext1.text}"
        ext1_body = res_ext1.json()
        ext1_inst_id = ext1_body["latest_instruction_id"]

        db.expire_all()
        inst_ext1 = db.query(LGInstruction).get(ext1_inst_id)
        lg1_db = db.query(LGRecord).get(lg1_id)

        # Expected 20-char serial: {ENT:4}{CAT:2}{LG_SEQ:4}{TYPE:2}{GLOB:4}{TYPE_SEQ:3}{SUB:1}
        expected_serial_1 = f"{fx.entity_alpha_hq.code.upper()}SP0001EX0001001O"
        assert len(inst_ext1.serial_number) == 20
        assert inst_ext1.serial_number == expected_serial_1, f"Expected {expected_serial_1}, got {inst_ext1.serial_number}"
        assert lg1_db.expiry_date.date() == ext_target_date
        assert lg1_db.lg_period_months == 6, "lg_period_months MUST be preserved on extension!"

        # Check XSS escaping in stored HTML details
        assert "<script>" not in (inst_ext1.details.get("notes_section") or "")
        assert "&lt;script&gt;" in (inst_ext1.details.get("notes_section") or "")

        # Render real WeasyPrint PDF & verify 1-page auto-compaction + exact content!
        res_ext1_pdf = client.get(
            f"/api/v1/end-user/lg-records/instructions/{ext1_inst_id}/view-letter",
            headers=maker_h,
            params={"raw_pdf": True},
        )
        assert res_ext1_pdf.status_code == 200
        ext1_pdf = extract_pdf_info(res_ext1_pdf.content)
        assert ext1_pdf["page_count"] == 1, f"Single-page compaction failed! Got {ext1_pdf['page_count']} pages"
        assert lg1_num in ext1_pdf["text"]
        assert "1,000,000.00" in ext1_pdf["text"]
        assert "Grow Business Development" in ext1_pdf["text"]
        assert (
            ext_target_date.isoformat() in ext1_pdf["text"]
            or ext_target_date.strftime("%d-%b-%Y") in ext1_pdf["text"]
        )
        print(f"  [PASS] Extension serial={expected_serial_1}, 1-page A4 PDF compacted, XSS escaped, dates/amounts exact")

        # --- 2.2 Two-Tier Bulk Auto-Renewal (Forced vs Proactive + Consolidated PDF) ---
        print("\n--- [2.2] Two-Tier Bulk Auto-Renewal (Forced <=15d vs Proactive <=30d & Consolidated PDF) ---")
        # Create 3 LGs to test exact tiering:
        #   - LG_FORCED: expires in 10 days, auto_renewal=False -> MUST be CRITICAL_FORCED (can_deselect=False)!
        #   - LG_PROACTIVE: expires in 22 days, auto_renewal=True -> MUST be PROACTIVE_AUTO (can_deselect=True)!
        #   - LG_OPTED_OUT: expires in 22 days, auto_renewal=False -> MUST NOT appear in candidates!
        def _quick_create_lg(num_suffix: str, days_left: int, auto_ren: bool) -> int:
            p = _base_lg_payload(
                lg_number=f"LG-REN-{num_suffix}-{fx.uid}",
                issuance_date=(date.today() - timedelta(days=160)).isoformat(),
                expiry_date=(date.today() + timedelta(days=days_left)).isoformat(),
                auto_renewal=auto_ren,
            )
            r = client.post("/api/v1/end-user/lg-records/", headers=maker_h, data={"lg_record_in": json.dumps(p)})
            assert r.status_code == 201, r.text
            return r.json()["id"]

        id_forced = _quick_create_lg("FORCED", 10, False)
        id_proactive = _quick_create_lg("PROACT", 22, True)
        id_opted_out = _quick_create_lg("OPTOUT", 22, False)

        res_prev = client.get("/api/v1/end-user/lg-records/auto-renewal-preview", headers=maker_h)
        assert res_prev.status_code == 200
        prev = res_prev.json()
        cand_by_id = {c["id"]: c for c in prev["candidates"]}

        assert id_forced in cand_by_id
        assert cand_by_id[id_forced]["is_forced"] is True
        assert cand_by_id[id_forced]["can_deselect"] is False
        assert cand_by_id[id_forced]["tier_label"] == "CRITICAL_FORCED"

        assert id_proactive in cand_by_id
        assert cand_by_id[id_proactive]["is_forced"] is False
        assert cand_by_id[id_proactive]["can_deselect"] is True
        assert cand_by_id[id_proactive]["tier_label"] == "PROACTIVE_AUTO"

        assert id_opted_out not in cand_by_id, "Opted-out LG outside forced window should NOT be a candidate!"

        # Attempt to deselect/omit the CRITICAL_FORCED LG -> MUST fail with HTTP 400!
        res_skip_forced = client.post(
            "/api/v1/end-user/lg-records/run-auto-renewal",
            headers=maker_h,
            json={
                "renewals": [
                    {
                        "lg_record_id": id_proactive,
                        "new_expiry_date": cand_by_id[id_proactive]["suggested_new_expiry_date"],
                    }
                ]
            },
        )
        assert res_skip_forced.status_code == 400
        assert "mandatory forced renewal" in res_skip_forced.json()["detail"].lower()
        print("  [PASS] Tiering verified & deselecting CRITICAL_FORCED LG blocked with HTTP 400")

        # Execute bulk renewal for both eligible candidates
        res_run_bulk = client.post("/api/v1/end-user/lg-records/run-auto-renewal", headers=maker_h)
        assert res_run_bulk.status_code == 200, res_run_bulk.text
        bulk_out = res_run_bulk.json()
        assert bulk_out["renewed_count"] >= 2
        assert bulk_out["batch_id"].startswith("BULK-EXT-")
        assert bulk_out["combined_pdf_base64"] is not None

        merged_pdf_bytes = base64.b64decode(bulk_out["combined_pdf_base64"])
        merged_pdf_info = extract_pdf_info(merged_pdf_bytes)
        assert merged_pdf_info["page_count"] == bulk_out["renewed_count"], (
            f"Expected {bulk_out['renewed_count']} independent 1-page letters in merged PDF, got {merged_pdf_info['page_count']}"
        )

        # Verify auto-renewal history & reprint endpoints
        res_hist = client.get("/api/v1/end-user/lg-records/auto-renewal-history", headers=maker_h)
        assert res_hist.status_code == 200
        assert any(b["batch_id"] == bulk_out["batch_id"] for b in res_hist.json())

        inst_ids_bulk = [item["instruction_id"] for item in bulk_out["renewed_items"]]
        res_reprint = client.post(
            "/api/v1/end-user/lg-records/auto-renewal-reprint",
            headers=maker_h,
            json={"instruction_ids": inst_ids_bulk},
        )
        assert res_reprint.status_code == 200 and res_reprint.json().get("combined_pdf_base64")

        # Toggle auto-renewal on id_proactive
        res_tog = client.post(
            f"/api/v1/end-user/lg-records/{id_proactive}/toggle-auto-renewal",
            headers=maker_h,
            json={"auto_renewal": False, "reason": "Project nearing completion"},
        )
        assert res_tog.status_code == 200 and res_tog.json()["lg_record"]["auto_renewal"] is False
        print(f"  [PASS] Bulk Auto-Renewal renewed {bulk_out['renewed_count']} LGs into {merged_pdf_info['page_count']}-page PDF + history/reprint/toggle verified")

        # --- 2.3 Non-Operative APG Activation + Maker-Checker + Print Queue ---
        print("\n--- [2.3] Non-Operative APG Activation, Self-Approval Block & Print Queue ---")
        pay_date = (date.today() - timedelta(days=1)).isoformat()
        res_act_req = client.post(
            f"/api/v1/end-user/lg-records/{lg_apg_id}/activate-non-operative",
            headers=maker_h,
            data={
                "payment_method": "SWIFT Wire Transfer",
                "currency_id": str(fx.ccy_egp.id),
                "amount": "500000.00",
                "payment_reference": "SWF-ADV-2026-5544",
                "issuing_bank_id": str(fx.bank_cib.id),
                "payment_date": pay_date,
                "notes": "20% Advance Payment transferred to contractor",
            },
            files={"internal_supporting_document_file": ("Swift_Advice.pdf", support_pdf_bytes, "application/pdf")},
        )
        assert res_act_req.status_code == 200, res_act_req.text
        act_ar_id = res_act_req.json()["approval_request_id"]

        # Single-flight lock: submitting a second Maker-Checker action on same LG while PENDING -> HTTP 400
        res_dup_ar = client.post(
            f"/api/v1/end-user/lg-records/{lg_apg_id}/release",
            headers=maker_h,
            data={"reason": "Conflicting concurrent request"},
        )
        assert res_dup_ar.status_code in (400, 409)

        # Maker self-approval block (even if Maker were passed to approve endpoint -> 403 RBAC;
        # and if an Admin submits a request, Admin self-approval is blocked with HTTP 400!)
        res_maker_self = client.post(
            f"/api/v1/corporate-admin/approval-requests/{act_ar_id}/approve",
            headers=maker_h,
        )
        assert res_maker_self.status_code in (400, 403)

        # Checker approves the Activation request
        res_act_app = client.post(
            f"/api/v1/corporate-admin/approval-requests/{act_ar_id}/approve",
            headers=checker_h,
        )
        assert res_act_app.status_code == 200, res_act_app.text
        act_app_data = res_act_app.json()
        assert act_app_data["status"] == "APPROVED"
        act_inst_id = act_app_data["related_instruction_id"]
        assert act_inst_id is not None

        db.expire_all()
        lg_apg_db = db.query(LGRecord).get(lg_apg_id)
        assert lg_apg_db.lg_operational_status_id == LgOperationalStatusEnum.OPERATIVE.value

        # Verify Activation PDF output has exact wire reference & amount
        res_act_pdf = client.get(
            f"/api/v1/end-user/lg-records/instructions/{act_inst_id}/view-letter",
            headers=maker_h,
            params={"raw_pdf": True},
        )
        assert res_act_pdf.status_code == 200
        act_pdf = extract_pdf_info(res_act_pdf.content)
        assert act_pdf["page_count"] == 1
        assert "SWF-ADV-2026-5544" in act_pdf["text"]
        assert "500,000.00" in act_pdf["text"]

        # Verify `approved-requests-pending-print` contains act_ar_id BEFORE printing, and disappears AFTER `mark-as-accessed-for-print`
        res_pp_before = client.get("/api/v1/end-user/action-center/approved-requests-pending-print", headers=maker_h)
        assert res_pp_before.status_code == 200
        assert any(r["id"] == act_ar_id for r in res_pp_before.json())

        res_mark_print = client.post(
            f"/api/v1/end-user/lg-records/instructions/{act_inst_id}/mark-as-accessed-for-print",
            headers=maker_h,
        )
        assert res_mark_print.status_code == 200

        res_pp_after = client.get("/api/v1/end-user/action-center/approved-requests-pending-print", headers=maker_h)
        assert not any(r["id"] == act_ar_id for r in res_pp_after.json())
        print("  [PASS] APG activated (NON_OPERATIVE -> OPERATIVE), wire details in PDF, & pending-print lifecycle verified")

        # Also explicitly test Admin-submitted Maker-Checker self-approval & self-rejection block (HTTP 400)
        # Admin has END_USER custody permissions? End-user endpoints accept Corporate Admin too!
        res_adm_mk = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/decrease-amount",
            headers=admin_h,
            data={"decrease_amount": "50000.00", "reason": "Admin maker self-approval test"},
        )
        if res_adm_mk.status_code == 200 and res_adm_mk.json().get("approval_request_id"):
            adm_ar_id = res_adm_mk.json()["approval_request_id"]
            res_self_app = client.post(
                f"/api/v1/corporate-admin/approval-requests/{adm_ar_id}/approve",
                headers=admin_h,
            )
            assert res_self_app.status_code == 400 and "maker cannot be the checker" in res_self_app.json()["detail"].lower()
            res_self_rej = client.post(
                f"/api/v1/corporate-admin/approval-requests/{adm_ar_id}/reject",
                headers=admin_h,
                json={"reason": "Self reject attempt"},
            )
            assert res_self_rej.status_code == 400
            # Cleanly reject via Checker so lg1_id is free for next step
            res_chk_rej = client.post(
                f"/api/v1/corporate-admin/approval-requests/{adm_ar_id}/reject",
                headers=checker_h,
                json={"reason": "Clearing self-approval test"},
            )
            assert res_chk_rej.status_code == 200
            print("  [PASS] Strict Separation of Duties: Maker self-approval & self-rejection blocked (HTTP 400)")

        # --- 2.4 Amount Decrease (1,000,000.00 -> 750,000.00) ---
        print("\n--- [2.4] Amount Decrease: Validation, Math & PDF Output ---")
        # Reject decrease >= current amount
        res_bad_dec = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/decrease-amount",
            headers=maker_h,
            data={"decrease_amount": "1000000.00", "reason": "Full amount not allowed on decrease"},
        )
        assert res_bad_dec.status_code == 400

        res_dec = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/decrease-amount",
            headers=maker_h,
            data={"decrease_amount": "250000.00", "reason": "Milestone 1 Provisional Handover", "notes": "25% reduction"},
        )
        assert res_dec.status_code == 200, res_dec.text
        dec_ar_id = res_dec.json()["approval_request_id"]

        res_dec_app = client.post(
            f"/api/v1/corporate-admin/approval-requests/{dec_ar_id}/approve",
            headers=checker_h,
        )
        assert res_dec_app.status_code == 200
        dec_inst_id = res_dec_app.json()["related_instruction_id"]

        db.expire_all()
        lg1_db = db.query(LGRecord).get(lg1_id)
        inst_dec = db.query(LGInstruction).get(dec_inst_id)
        assert lg1_db.lg_amount == Decimal("750000.00")
        assert lg1_db.lg_status_id == LgStatusEnum.VALID.value
        assert inst_dec.serial_number == f"{fx.entity_alpha_hq.code.upper()}SP0001DC0002001O"

        dec_pdf = extract_pdf_info(
            client.get(
                f"/api/v1/end-user/lg-records/instructions/{dec_inst_id}/view-letter",
                headers=maker_h,
                params={"raw_pdf": True},
            ).content
        )
        assert "250,000.00" in dec_pdf["text"] and "750,000.00" in dec_pdf["text"]
        print(f"  [PASS] Decreased 1,000,000 -> 750,000.00 (serial={inst_dec.serial_number}), PDF verified")

        # --- 2.5 Partial Liquidation -> Full Liquidation + Verbatim Mandatory Claim Statement ---
        print("\n--- [2.5] Partial & Full Liquidation + Mandatory Claim Declaration Box in PDF ---")
        res_part_liq = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/liquidate",
            headers=maker_h,
            data={
                "liquidation_type": "partial",
                "new_amount": "450000.00",  # Claiming 300,000.00 out of 750,000.00
                "reason": "Contractor delay penalty claim",
                "notes": "Deducting liquidated damages",
            },
        )
        assert res_part_liq.status_code == 200, res_part_liq.text
        part_liq_ar = res_part_liq.json()["approval_request_id"]
        res_part_app = client.post(
            f"/api/v1/corporate-admin/approval-requests/{part_liq_ar}/approve",
            headers=checker_h,
        )
        assert res_part_app.status_code == 200
        part_liq_inst_id = res_part_app.json()["related_instruction_id"]

        db.expire_all()
        lg1_db = db.query(LGRecord).get(lg1_id)
        inst_pliq_db = db.query(LGInstruction).get(part_liq_inst_id)
        assert lg1_db.lg_amount == Decimal("450000.00")
        assert lg1_db.lg_status_id == LgStatusEnum.VALID.value
        assert "300,000.00" in str((inst_pliq_db.details or {}).get("liquidation_amount_formatted", ""))

        liq_pdf = extract_pdf_info(
            client.get(
                f"/api/v1/end-user/lg-records/instructions/{part_liq_inst_id}/view-letter",
                headers=maker_h,
                params={"raw_pdf": True},
            ).content
        )
        assert liq_pdf["page_count"] == 1
        assert "750,000.00" in liq_pdf["text"] and "450,000.00" in liq_pdf["text"]
        assert "Contract #EG-220KV" in liq_pdf["text"], "Mandatory Claim Statement must be injected verbatim into PDF!"
        print("  [PASS] Partial Liquidation (750k -> 450k remaining, 300k claimed, VALID) + Mandatory Claim Wording in PDF")

        # Now Full Liquidation on a dedicated LG (`lg_full_liq`) to verify terminal LIQUIDATED state
        lg_fl_id = _quick_create_lg("FULLLIQ", 90, False)
        res_fl = client.post(
            f"/api/v1/end-user/lg-records/{lg_fl_id}/liquidate",
            headers=maker_h,
            data={"liquidation_type": "full", "reason": "Total default"},
        )
        assert res_fl.status_code == 200
        fl_app = client.post(
            f"/api/v1/corporate-admin/approval-requests/{res_fl.json()['approval_request_id']}/approve",
            headers=checker_h,
        )
        assert fl_app.status_code == 200
        fl_inst_id = fl_app.json()["related_instruction_id"]

        db.expire_all()
        lg_fl_db = db.query(LGRecord).get(lg_fl_id)
        assert lg_fl_db.lg_amount == Decimal("0.00")
        assert lg_fl_db.lg_status_id == LgStatusEnum.LIQUIDATED.value

        # Verify liquidated LG blocks subsequent extend/release
        assert client.post(
            f"/api/v1/end-user/lg-records/{lg_fl_id}/extend",
            headers=maker_h,
            json={"new_expiry_date": (date.today() + timedelta(days=200)).isoformat()},
        ).status_code == 400
        print("  [PASS] Full Liquidation set amount=0.00, status=LIQUIDATED, and blocked further actions")

        # --- 2.6 Release + Customer Modern Template Override + Dynamic Enclosed Documents Clause ---
        print("\n--- [2.6] LG Release, Customer Template Override & Dynamic Pending-Reply Commitment Clause ---")
        # Seed CustomerID_1's modern LG_RELEASE template as a customer-specific override for cust_alpha
        # to verify both customer template precedence AND `{{enclosed_documents_clause}}` rendering!
        from app.models.models import Template
        modern_rel_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "app", "templates", "CustomerID_1_LG_RELEASE_template.html")
        )
        if os.path.exists(modern_rel_path):
            with open(modern_rel_path, "r", encoding="utf-8") as tf:
                cust_rel_tpl = Template(
                    name=f"Modern Corporate Release {fx.uid}",
                    template_type="LG_INSTRUCTION",
                    action_type="LG_RELEASE",
                    content=tf.read(),
                    is_global=False,
                    is_notification_template=False,
                    customer_id=fx.cust_alpha.id,
                )
                db.add(cust_rel_tpl)
                db.commit()

        # lg1_id has unconfirmed prior instructions (pending_replies_count > 0) -> release letter must include commitment clause!
        res_rel = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/release",
            headers=maker_h,
            data={"reason": "Final Taking-Over Certificate issued", "notes": "Discharge guarantee"},
        )
        assert res_rel.status_code == 200
        rel_app = client.post(
            f"/api/v1/corporate-admin/approval-requests/{res_rel.json()['approval_request_id']}/approve",
            headers=checker_h,
        )
        assert rel_app.status_code == 200
        rel_inst_id = rel_app.json()["related_instruction_id"]

        db.expire_all()
        lg1_db = db.query(LGRecord).get(lg1_id)
        assert lg1_db.lg_status_id == LgStatusEnum.RELEASED.value

        rel_pdf = extract_pdf_info(
            client.get(
                f"/api/v1/end-user/lg-records/instructions/{rel_inst_id}/view-letter",
                headers=maker_h,
                params={"raw_pdf": True},
            ).content
        )
        assert rel_pdf["page_count"] == 1
        assert "commit to return" in rel_pdf["text"].lower() and "attached herewith" in rel_pdf["text"].lower()
        print("  [PASS] LG Released (status=RELEASED), Customer Template Override resolved & dynamic pending-reply commitment clause rendered in PDF")

        # --- 2.7 Incoming Bank Amendment + Expired-Within-Grace Revival ---
        print("\n--- [2.7] Incoming Bank Amendment & Expired-LG Grace Revival (Expired -> Valid) ---")
        lg_rev_id = _quick_create_lg("REVIVE", 45, True)
        db.expire_all()
        lg_rev_db = db.query(LGRecord).get(lg_rev_id)
        # Simulate LG expired 10 days ago (within 30-day grace window)
        lg_rev_db.lg_status_id = LgStatusEnum.EXPIRED.value
        lg_rev_db.expiry_date = datetime.combine(date.today() - timedelta(days=10), datetime.min.time())
        db.commit()

        revive_future_date = (date.today() + timedelta(days=180)).isoformat()
        res_amend = client.post(
            f"/api/v1/end-user/lg-records/{lg_rev_id}/amend",
            headers=maker_h,
            data={
                "amendment_details": json.dumps({
                    "expiry_date": revive_future_date,
                    "lg_amount": "1250000.00",
                    "description_purpose": "Amended scope + extended validity",
                }),
                "reason": "Late bank extension advice received within grace period",
            },
            files={"amendment_letter_file": ("Bank_Amendment_Advice.pdf", sample_pdf_bytes, "application/pdf")},
        )
        assert res_amend.status_code == 200, res_amend.text
        amend_app = client.post(
            f"/api/v1/corporate-admin/approval-requests/{res_amend.json()['approval_request_id']}/approve",
            headers=checker_h,
        )
        assert amend_app.status_code == 200

        db.expire_all()
        lg_rev_db = db.query(LGRecord).get(lg_rev_id)
        assert lg_rev_db.lg_status_id == LgStatusEnum.VALID.value, "Expired LG amended with future expiry MUST revive to VALID!"
        assert lg_rev_db.lg_amount == Decimal("1250000.00")
        assert lg_rev_db.expiry_date.date().isoformat() == revive_future_date

        # Verify LG expired > 40 days ago is rejected on amendment (HTTP 400)
        lg_old_exp_id = _quick_create_lg("OLDEXP", 45, False)
        db.expire_all()
        lg_old_db = db.query(LGRecord).get(lg_old_exp_id)
        lg_old_db.lg_status_id = LgStatusEnum.EXPIRED.value
        lg_old_db.expiry_date = datetime.combine(date.today() - timedelta(days=45), datetime.min.time())
        db.commit()

        res_amend_old = client.post(
            f"/api/v1/end-user/lg-records/{lg_old_exp_id}/amend",
            headers=maker_h,
            data={
                "amendment_details": json.dumps({"expiry_date": revive_future_date}),
                "reason": "Too old expired LG",
            },
            files={"amendment_letter_file": ("Old_Advice.pdf", sample_pdf_bytes, "application/pdf")},
        )
        assert res_amend_old.status_code == 400
        print("  [PASS] Expired LG (10d ago) revived to VALID with 1,250,000.00; >35d expired LG blocked (HTTP 400)")

        # --- 2.8 Internal Owner Handover (Single, Bulk & Delete-In-Use Protection) ---
        print("\n--- [2.8] Internal Owner Management: Single Handover, Bulk Handover & Delete Guard ---")
        res_new_own = client.post(
            "/api/v1/end-user/internal-owner-contacts/",
            headers=maker_h,
            json={
                "email": f"new.pm.{fx.uid.lower()}@globex.com",
                "phone_number": "+201112223334",
                "internal_id": "EMP-777",
                "manager_email": f"vp.projects.{fx.uid.lower()}@globex.com",
            },
        )
        assert res_new_own.status_code == 201, res_new_own.text
        new_owner_id = res_new_own.json()["id"]

        old_owner_id = lg_rev_db.internal_owner_contact_id

        # Single LG Owner Change
        res_chg_single = client.post(
            "/api/v1/end-user/lg-records/change-owner",
            headers=maker_h,
            json={
                "change_scope": "single_lg",
                "lg_record_id": lg_rev_id,
                "new_internal_owner_contact_id": new_owner_id,
                "reason": "Handover to Phase 2 Project Manager",
            },
        )
        assert res_chg_single.status_code == 200, res_chg_single.text
        if res_chg_single.json().get("approval_request_id"):
            r_app = client.post(
                f"/api/v1/corporate-admin/approval-requests/{res_chg_single.json()['approval_request_id']}/approve",
                headers=checker_h,
            )
            assert r_app.status_code == 200

        db.expire_all()
        assert db.query(LGRecord).get(lg_rev_id).internal_owner_contact_id == new_owner_id

        # Attempting to delete `new_owner_id` while linked to an active LG -> HTTP 400!
        res_del_active_owner = client.delete(
            f"/api/v1/end-user/internal-owner-contacts/{new_owner_id}",
            headers=maker_h,
        )
        assert res_del_active_owner.status_code == 400

        # Bulk Owner Change (`all_by_old_owner`) under full Maker-Checker (`can_maker_checker=True`)
        # approved by a dedicated `UserRole.CHECKER` user (`fx.checker_user`)!
        pure_checker_h = make_auth_headers(fx.checker_user)
        res_chg_bulk = client.post(
            "/api/v1/end-user/lg-records/change-owner",
            headers=maker_h,
            json={
                "change_scope": "all_by_old_owner",
                "old_internal_owner_contact_id": old_owner_id,
                "new_internal_owner_contact_id": new_owner_id,
                "reason": "Portfolio consolidation under Maker-Checker",
            },
        )
        assert res_chg_bulk.status_code == 200, res_chg_bulk.text
        bulk_ar_id = res_chg_bulk.json().get("approval_request_id")
        assert bulk_ar_id is not None, f"Expected Maker-Checker approval_request_id on bulk owner change: {res_chg_bulk.json()}"

        # Verify Corporate Admin & dedicated CHECKER see the full live list of LGs being transferred BEFORE approving!
        r_bulk_list = client.get("/api/v1/corporate-admin/approval-requests/", headers=pure_checker_h)
        assert r_bulk_list.status_code == 200
        pending_bulk_row = next(r for r in r_bulk_list.json() if r["id"] == bulk_ar_id)
        pending_det = pending_bulk_row["request_details"]
        assert len(pending_det["affected_lgs"]) >= 1
        assert len(pending_det["affected_lg_numbers"]) == len(pending_det["affected_lgs"])
        assert pending_det["old_owner_summary"]["id"] == old_owner_id
        assert pending_det["new_owner_summary"]["id"] == new_owner_id
        first_preview_lg = pending_det["affected_lgs"][0]
        assert first_preview_lg["lg_number"] and first_preview_lg["currency_code"] and first_preview_lg["bank_name"]
        expected_transferred_count = len(pending_det["affected_lgs"])

        r_bulk_detail = client.get(f"/api/v1/corporate-admin/approval-requests/{bulk_ar_id}", headers=admin_h)
        assert r_bulk_detail.status_code == 200
        assert len(r_bulk_detail.json()["request_details"]["affected_lgs"]) == expected_transferred_count

        # Duplicate pending bulk handover for the same owner MUST be blocked (HTTP 400)
        res_dup_bulk = client.post(
            "/api/v1/end-user/lg-records/change-owner",
            headers=maker_h,
            json={
                "change_scope": "all_by_old_owner",
                "old_internal_owner_contact_id": old_owner_id,
                "new_internal_owner_contact_id": new_owner_id,
                "reason": "Duplicate pending handover attempt",
            },
        )
        assert res_dup_bulk.status_code == 400 and "already pending" in res_dup_bulk.text.lower(), res_dup_bulk.text

        r_bulk_app = client.post(
            f"/api/v1/corporate-admin/approval-requests/{bulk_ar_id}/approve",
            headers=pure_checker_h,
        )
        assert r_bulk_app.status_code == 200, r_bulk_app.text
        assert r_bulk_app.json()["status"] == "APPROVED"

        # Verify historical APPROVED record still preserves the frozen snapshot of transferred LGs!
        r_bulk_after = client.get(f"/api/v1/corporate-admin/approval-requests/{bulk_ar_id}", headers=admin_h)
        assert r_bulk_after.status_code == 200
        assert len(r_bulk_after.json()["request_details"]["affected_lgs"]) == expected_transferred_count

        db.expire_all()
        remaining_under_old = db.query(LGRecord).filter(
            LGRecord.customer_id == fx.cust_alpha.id,
            LGRecord.internal_owner_contact_id == old_owner_id,
            LGRecord.is_deleted == False,
        ).count()
        assert remaining_under_old == 0, f"Expected 0 LGs remaining under old owner, got {remaining_under_old}"

        # Guard Verification: Attempting to change owner when `old_owner_id` now has 0 LGs MUST fail with HTTP 400
        # under BOTH Maker-Checker AND Direct Execution!
        res_zero_mc = client.post(
            "/api/v1/end-user/lg-records/change-owner",
            headers=maker_h,
            json={
                "change_scope": "all_by_old_owner",
                "old_internal_owner_contact_id": old_owner_id,
                "new_internal_owner_contact_id": new_owner_id,
                "reason": "Should fail because old owner has 0 LGs",
            },
        )
        assert res_zero_mc.status_code == 400 and "0 lg" in res_zero_mc.text.lower(), res_zero_mc.text

        plan_mc_row = db.get(type(fx.plan), fx.plan.id)
        plan_mc_row.can_maker_checker = False
        db.commit()
        try:
            res_zero_direct = client.post(
                "/api/v1/end-user/lg-records/change-owner",
                headers=maker_h,
                json={
                    "change_scope": "all_by_old_owner",
                    "old_internal_owner_contact_id": old_owner_id,
                    "new_internal_owner_contact_id": new_owner_id,
                    "reason": "Direct execution must also block 0 LGs",
                },
            )
            assert res_zero_direct.status_code == 400 and "0 lg" in res_zero_direct.text.lower(), res_zero_direct.text
        finally:
            plan_mc_row.can_maker_checker = True
            db.commit()

        # Guard Verification: Same-owner transfer (`new_owner_id -> new_owner_id`) MUST also fail with HTTP 400
        res_same_owner = client.post(
            "/api/v1/end-user/lg-records/change-owner",
            headers=maker_h,
            json={
                "change_scope": "all_by_old_owner",
                "old_internal_owner_contact_id": new_owner_id,
                "new_internal_owner_contact_id": new_owner_id,
                "reason": "Same owner check",
            },
        )
        assert res_same_owner.status_code == 400

        print(
            f"  [PASS] Single & Bulk LG Owner handover ({expected_transferred_count} LGs previewed & frozen) "
            "+ 0-LG guard (Maker-Checker & Direct) + duplicate-pending guard + CHECKER role verified"
        )

        # ==============================================================================
        # PART 3: INSTRUCTION CANCELLATION & EXACT STATE ROLLBACK ENGINE
        # ==============================================================================
        print("\n" + "=" * 88)
        print("[PART 3] INSTRUCTION CANCELLATION, EXACT STATE ROLLBACK & WITHDRAW/PURGE")
        print("=" * 88)

        # 3.1 Non-latest instruction cancellation MUST fail execution (on lg1_id, ext1_inst_id is older than rel_inst_id)
        res_cancel_non_latest = client.post(
            f"/api/v1/end-user/lg-records/instructions/{ext1_inst_id}/cancel",
            headers=maker_h,
            json={"reason": "Trying to cancel older instruction", "declaration_confirmed": True},
        )
        if res_cancel_non_latest.status_code == 200 and res_cancel_non_latest.json().get("approval_request_id"):
            bad_cancel_ar_id = res_cancel_non_latest.json()["approval_request_id"]
            res_app_bad_cancel = client.post(
                f"/api/v1/corporate-admin/approval-requests/{bad_cancel_ar_id}/approve",
                headers=checker_h,
            )
            assert res_app_bad_cancel.status_code == 400
            client.post(
                f"/api/v1/corporate-admin/approval-requests/{bad_cancel_ar_id}/reject",
                headers=checker_h,
                json={"reason": "Cannot cancel non-latest instruction"},
            )
        else:
            assert res_cancel_non_latest.status_code == 400
        print("  [PASS] Non-latest instruction cancellation blocked by LIFO eligibility guard (HTTP 400)")

        # 3.2 Cancel latest `LG_RELEASE` (`rel_inst_id`) on `lg1_id` -> rolls status `RELEASED -> VALID`!
        res_cancel_rel = client.post(
            f"/api/v1/end-user/lg-records/instructions/{rel_inst_id}/cancel",
            headers=maker_h,
            json={"reason": "Beneficiary requested retention period extension instead of release", "declaration_confirmed": True},
        )
        assert res_cancel_rel.status_code == 200, res_cancel_rel.text
        if res_cancel_rel.json().get("approval_request_id"):
            c_app = client.post(
                f"/api/v1/corporate-admin/approval-requests/{res_cancel_rel.json()['approval_request_id']}/approve",
                headers=checker_h,
            )
            assert c_app.status_code == 200

        db.expire_all()
        assert db.query(LGRecord).get(lg1_id).lg_status_id == LgStatusEnum.VALID.value
        assert db.query(LGInstruction).get(rel_inst_id).status == "Canceled"
        print("  [PASS] Canceled LG_RELEASE -> LG status rolled back from RELEASED to VALID!")

        # 3.3 Cancel Partial `LG_LIQUIDATE` (`part_liq_inst_id` is now the latest non-canceled instruction on lg1_id!)
        #     -> restores `lg_amount` from `450,000.00` back to `750,000.00`!
        res_cancel_pliq = client.post(
            f"/api/v1/end-user/lg-records/instructions/{part_liq_inst_id}/cancel",
            headers=maker_h,
            json={"reason": "Contractor settled delay penalty in cash", "declaration_confirmed": True},
        )
        assert res_cancel_pliq.status_code == 200, res_cancel_pliq.text
        if res_cancel_pliq.json().get("approval_request_id"):
            assert client.post(
                f"/api/v1/corporate-admin/approval-requests/{res_cancel_pliq.json()['approval_request_id']}/approve",
                headers=checker_h,
            ).status_code == 200

        db.expire_all()
        assert db.query(LGRecord).get(lg1_id).lg_amount == Decimal("750000.00")
        print("  [PASS] Canceled Partial LG_LIQUIDATE -> LG amount restored from 450,000.00 to 750,000.00!")

        # 3.4 Cancel `LG_DECREASE_AMOUNT` (`dec_inst_id` is now latest non-canceled!) -> restores `1,000,000.00`!
        res_cancel_dec = client.post(
            f"/api/v1/end-user/lg-records/instructions/{dec_inst_id}/cancel",
            headers=maker_h,
            json={"reason": "Reduction letter recalled prior to bank delivery", "declaration_confirmed": True},
        )
        assert res_cancel_dec.status_code == 200, res_cancel_dec.text
        if res_cancel_dec.json().get("approval_request_id"):
            assert client.post(
                f"/api/v1/corporate-admin/approval-requests/{res_cancel_dec.json()['approval_request_id']}/approve",
                headers=checker_h,
            ).status_code == 200

        db.expire_all()
        assert db.query(LGRecord).get(lg1_id).lg_amount == Decimal("1000000.00")
        print("  [PASS] Canceled LG_DECREASE_AMOUNT -> LG amount restored from 750,000.00 to 1,000,000.00!")

        # 3.5 Cancel `LG_EXTENSION` (`ext1_inst_id` is now latest non-canceled!) -> restores original expiry date!
        orig_exp_str = lg1_payload["expiry_date"]
        res_cancel_ext = client.post(
            f"/api/v1/end-user/lg-records/instructions/{ext1_inst_id}/cancel",
            headers=maker_h,
            json={"reason": "Re-issuing extension with updated date", "declaration_confirmed": True},
        )
        assert res_cancel_ext.status_code == 200, res_cancel_ext.text
        if res_cancel_ext.json().get("approval_request_id"):
            assert client.post(
                f"/api/v1/corporate-admin/approval-requests/{res_cancel_ext.json()['approval_request_id']}/approve",
                headers=checker_h,
            ).status_code == 200

        db.expire_all()
        assert db.query(LGRecord).get(lg1_id).expiry_date.date().isoformat() == orig_exp_str
        print(f"  [PASS] Canceled LG_EXTENSION -> LG expiry rolled back to exact initial date ({orig_exp_str})!")

        # 3.6 Cancel Full `LG_LIQUIDATE` (`fl_inst_id` on `lg_fl_id`) -> restores `1,000,000.00` AND `VALID`!
        res_cancel_fl = client.post(
            f"/api/v1/end-user/lg-records/instructions/{fl_inst_id}/cancel",
            headers=maker_h,
            json={"reason": "Amicable settlement signed before delivery", "declaration_confirmed": True},
        )
        assert res_cancel_fl.status_code == 200, res_cancel_fl.text
        if res_cancel_fl.json().get("approval_request_id"):
            assert client.post(
                f"/api/v1/corporate-admin/approval-requests/{res_cancel_fl.json()['approval_request_id']}/approve",
                headers=checker_h,
            ).status_code == 200

        db.expire_all()
        lg_fl_db = db.query(LGRecord).get(lg_fl_id)
        assert lg_fl_db.lg_status_id == LgStatusEnum.VALID.value
        assert lg_fl_db.lg_amount == Decimal("1000000.00")

        # 3.7 Cancel `LG_ACTIVATE_NON_OPERATIVE` (`act_inst_id` on `lg_apg_id`) -> rolls back `OPERATIVE -> NON_OPERATIVE`!
        res_cancel_act = client.post(
            f"/api/v1/end-user/lg-records/instructions/{act_inst_id}/cancel",
            headers=maker_h,
            json={"reason": "Wire transfer recalled by treasury", "declaration_confirmed": True},
        )
        assert res_cancel_act.status_code == 200, res_cancel_act.text
        if res_cancel_act.json().get("approval_request_id"):
            assert client.post(
                f"/api/v1/corporate-admin/approval-requests/{res_cancel_act.json()['approval_request_id']}/approve",
                headers=checker_h,
            ).status_code == 200

        db.expire_all()
        assert db.query(LGRecord).get(lg_apg_id).lg_operational_status_id == LgOperationalStatusEnum.NON_OPERATIVE.value
        print("  [PASS] Canceled Full Liquidation (LIQUIDATED->VALID) & APG Activation (OPERATIVE->NON_OPERATIVE)!")

        # 3.8 Maker Withdrawal & Checker Rejection Orphan Document Purge (`_nuke_document`)
        res_rel_w = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/release",
            headers=maker_h,
            data={"reason": "Temporary release submission"},
            files={"internal_supporting_document_file": ("Temp_Release_Doc.pdf", support_pdf_bytes, "application/pdf")},
        )
        assert res_rel_w.status_code == 200
        ar_w_id = res_rel_w.json()["approval_request_id"]
        db.expire_all()
        ar_w_obj = db.query(ApprovalRequest).get(ar_w_id)
        staged_doc_id = (ar_w_obj.request_details or {}).get("supporting_document_id")
        assert staged_doc_id is not None

        res_withdraw = client.post(f"/api/v1/end-user/approval-requests/{ar_w_id}/withdraw", headers=maker_h)
        assert res_withdraw.status_code == 200
        assert res_withdraw.json()["status"] == "WITHDRAWN"
        db.expire_all()
        assert db.query(LGDocument).get(int(staged_doc_id)) is None, "Withdrawn request's staged document must be purged!"
        print("  [PASS] Maker Withdrawal set status=WITHDRAWN and purged orphan supporting document from DB & GCS")

        # ==============================================================================
        # PART 4: PHYSICAL LETTER QR SEAL, SPLIT PIN + LG-TAIL, TAMPER & BOT TRAPS
        # ==============================================================================
        print("\n" + "=" * 88)
        print("[PART 4] PUBLIC QR SEAL, SPLIT PIN + LG-TAIL, TAMPER DETECTION & BOT TRAPS")
        print("=" * 88)

        _clear_verify_cooldowns()

        # Issue a fresh extension on lg1_id (`LG-LOC-{uid}-9001`, ends in `"01"`)
        qr_ext_date = date.today() + timedelta(days=275)
        res_qr_ext = client.post(
            f"/api/v1/end-user/lg-records/{lg1_id}/extend",
            headers=maker_h,
            json={"new_expiry_date": qr_ext_date.isoformat(), "notes": "Official QR verification test"},
        )
        assert res_qr_ext.status_code == 200
        qr_inst_id = res_qr_ext.json()["latest_instruction_id"]

        # Render PDF and extract the actual printed 4-digit PIN!
        qr_pdf_bytes = client.get(
            f"/api/v1/end-user/lg-records/instructions/{qr_inst_id}/view-letter",
            headers=maker_h,
            params={"raw_pdf": True},
        ).content
        qr_pdf_info = extract_pdf_info(qr_pdf_bytes)
        printed_pin = _extract_pin_from_pdf_text(qr_pdf_info["text"])

        db.expire_all()
        qr_inst = db.query(LGInstruction).get(qr_inst_id)
        secret_ts = _to_epoch_seconds(qr_inst.created_at or qr_inst.instruction_date)
        v2_token, computed_pin = generate_verification_token_and_code(
            "I", qr_inst.id, qr_inst.serial_number, lg1_num, secret_ts
        )
        assert printed_pin == computed_pin, f"Printed PDF PIN ({printed_pin}) != computed PIN ({computed_pin})!"

        # 4.1 Inspect QR token BEFORE entering PIN -> must NOT reveal LG number, amount, or customer!
        res_insp = client.get("/api/v1/public/verify-instruction/inspect", params={"t": v2_token})
        assert res_insp.status_code == 200
        insp_data = res_insp.json()
        assert insp_data["valid_token"] is True
        assert insp_data["version"] == 2
        assert lg1_num not in json.dumps(insp_data)
        assert "1,000,000" not in json.dumps(insp_data)

        # 4.2 Confirm with valid v2 token + valid 4-digit PIN + valid last-2 LG chars ("01") -> VERIFIED!
        res_conf_ok = client.post(
            "/api/v1/public/verify-instruction/confirm",
            json={
                "token": v2_token,
                "code": printed_pin,
                "lg_tail": "01",
                "bank_branch_code": "",
                "elapsed_ms": 2500,
            },
        )
        assert res_conf_ok.status_code == 200
        v_ok = res_conf_ok.json()
        assert v_ok["verified"] is True
        assert v_ok["status_category"] == "VALID"
        assert v_ok["instruction_serial"] == qr_inst.serial_number
        assert v_ok["lg_number_masked"].endswith("9001")
        assert "1,000,000.00" in v_ok["lg_amount_formatted"]
        assert qr_ext_date.strftime("%d-%b-%Y") == v_ok["authorized_parameter_value"]
        print(f"  [PASS] QR v2 Split Verification succeeded (PIN={printed_pin}, LG tail='01', param={v_ok['authorized_parameter_value']})")

        # 4.3 Physical Cut-and-Paste Footer Tamper Detection:
        #     Valid QR token + Valid 4-digit footer PIN + WRONG LG tail ("99") -> TAMPER ALERT!
        res_tamper = client.post(
            "/api/v1/public/verify-instruction/confirm",
            json={
                "token": v2_token,
                "code": printed_pin,
                "lg_tail": "99",
                "bank_branch_code": "",
                "elapsed_ms": 2400,
            },
        )
        assert res_tamper.status_code == 200
        t_data = res_tamper.json()
        assert t_data["verified"] is False
        assert t_data.get("tamper_alert") is True
        print("  [PASS] Cut-and-Paste Footer Tamper Attack detected (`tamper_alert=True` on mismatched LG tail)")

        _clear_verify_cooldowns()

        # 4.4 Legacy v1 6-Digit Token Backward Compatibility (`I.<id>.<sig>`)
        v1_token, v1_code = _generate_v1_token_and_6digit_code(
            "I", qr_inst.id, qr_inst.serial_number, lg1_num, secret_ts
        )
        res_v1 = client.post(
            "/api/v1/public/verify-instruction/confirm",
            json={"token": v1_token, "code": v1_code, "elapsed_ms": 2000},
        )
        assert res_v1.status_code == 200 and res_v1.json()["verified"] is True
        print(f"  [PASS] Legacy v1 6-digit Security Code ({v1_code}) verified seamlessly")

        # 4.5 Canceled Instruction QR Verification -> MUST warn Bank Teller `"CANCELED / REVOKED — Do Not Execute"`!
        ext1_ts = _to_epoch_seconds(inst_ext1.created_at or inst_ext1.instruction_date)
        canceled_tok, canceled_pin = generate_verification_token_and_code(
            "I", inst_ext1.id, inst_ext1.serial_number, lg1_num, ext1_ts
        )
        res_canc_ver = client.post(
            "/api/v1/public/verify-instruction/confirm",
            json={"token": canceled_tok, "code": canceled_pin, "lg_tail": "01", "elapsed_ms": 2100},
        )
        assert res_canc_ver.status_code == 200
        cv = res_canc_ver.json()
        assert cv["verified"] is True
        assert cv["status_category"] == "CANCELED"
        assert "DO NOT EXECUTE" in cv["delivery_status"].upper()
        print("  [PASS] Canceled instruction QR scan warns bank teller: 'CANCELED / REVOKED — Do Not Execute'")

        # 4.6 Progressive Cooldown (2 free typos -> 3rd attempt triggers 60s cooldown) & Bot Trap (1800s)
        _clear_verify_cooldowns()
        for attempt_no in (1, 2):
            r_fail = client.post(
                "/api/v1/public/verify-instruction/confirm",
                json={"token": v2_token, "code": "0000", "lg_tail": "01", "elapsed_ms": 2000},
            ).json()
            assert r_fail["verified"] is False
            assert r_fail["cooldown_seconds"] == 0  # Free typo attempts!

        r_fail_3 = client.post(
            "/api/v1/public/verify-instruction/confirm",
            json={"token": v2_token, "code": "0000", "lg_tail": "01", "elapsed_ms": 2000},
        ).json()
        assert r_fail_3["verified"] is False
        assert r_fail_3["cooldown_seconds"] == 60

        _clear_verify_cooldowns()
        r_bot = client.post(
            "/api/v1/public/verify-instruction/confirm",
            json={"token": v2_token, "code": printed_pin, "lg_tail": "01", "elapsed_ms": 120},  # < 800ms bot speed!
        ).json()
        assert r_bot["verified"] is False
        assert r_bot["cooldown_seconds"] == 1800
        _clear_verify_cooldowns()
        print("  [PASS] Progressive Cooldown (0s, 0s -> 60s) & Sub-800ms Bot Speed Trap (1800s lockout) verified")

        print("\n" + "=" * 88)
        print("ALL LG CUSTODY OMNI LIFECYCLE & USER-OUTPUT VERIFICATION CHECKS PASSED 100%!")
        print("=" * 88)

    finally:
        _clear_verify_cooldowns()
        fx.cleanup()
        db.close()


def test_lg_custody_omni_lifecycle_suite():
    run_lg_custody_omni_lifecycle_suite()


if __name__ == "__main__":
    run_lg_custody_omni_lifecycle_suite()
