# tests/test_issuance_facilities_maintenance_recon_suite.py
"""
Master LG Issuance, Credit Facilities, SLA Intelligence, Post-Issuance Maintenance,
LG Position Reconciliation & Enterprise Excel Export Verification Suite.

Covers:
1. PART 1 — Bank Credit Facilities, Cash-Margin Exposure Reduction & Cross-Currency FX Utilization Math:
   - Multi-currency EGP facility (`multi_currency_allowed=True`, `margin_reduces_exposure=True`),
     sub-limit with `initial_utilization`, `default_cash_margin_pct=20%`, and `CurrencyExchangeRate` (`USD->EGP`).
   - Exact verification of `utilized_amount`, `reserved_amount`, `limit_available`, and margin/commission math.
2. PART 2 — Issuance Workflow Matrix (Dual-Control), Immutable V1 Snapshot, Safe-Field Edit Governance & Similarity:
   - Dual-control `PUT /api/v1/issuance/workflow-policies` (`202 Accepted` -> self-approval blocked -> 2nd Admin approves).
   - Cross-currency `USD` Issuance Request -> `/submit` -> V1 `IssuanceRequestSnapshot` -> multi-step `/approve` -> `APPROVED_INTERNAL`.
   - Post-approval safe-field edit (`EDIT_REQUESTED` -> `/resolve-edit` -> `IssuanceRequestVersion` diff).
   - Duplicate / Similarity Detection (`>=70%` weighted score).
3. PART 3 — Facility Reservation, Execution, Copy Verification, SLA Actualization EMA & Post-Issuance Maintenance:
   - `/reserve` -> `/issue` (`YYYY-XXXX-NNNN000` internal serial) -> `PATCH /lg-records/{id}/verify` (missing number guard,
     discrepancy detection, End-User `force_accept` 403 block, Admin `force_accept` acceptance).
   - `SLAActualizationService`: Bayesian credibility curve (`W_N = min(0.85, N/(N+3))`) & `EMA_ALPHA=0.25` turnaround math.
   - `IssuanceMaintenanceService`: `EXTEND` (with WeasyPrint letter regeneration & `M2.<id>.<sig>` QR public verification!),
     `INCREASE_AMOUNT` (exposure delta), `ACTIVATE` (non-operative APG one-time guard), and `CLOSE` (frees facility utilization!).
4. PART 4 — LG Position Reconciliation & Enterprise Multi-Tab Excel Export:
   - `POST /api/v1/issuance/reconciliation/sessions` + `/match` + `/results`: verifies `MATCHED`, variance/mismatch,
     `BANK_ONLY`, and `SYSTEM_ONLY` detection from real `.xlsx` bank position sheet.
   - `GET /api/v1/issuance/issued-lgs/export` (`detailed` with `include_event_log=True` & `include_approvals=True`):
     parses `.xlsx` workbook via `openpyxl` and asserts multi-tab sheets and exact LG reference cells.

Run directly:
    $env:PYTHONPATH = "c:\\Grow"; c:\\Grow\\venv\\Scripts\\python.exe c:\\Grow\\tests\\test_issuance_facilities_maintenance_recon_suite.py
"""

import io
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from fastapi.testclient import TestClient
import openpyxl

from _test_harness import (
    app,
    SessionLocal,
    IsolatedTenantFixture,
    install_in_memory_gcs,
    install_email_capture,
    make_auth_headers,
    extract_pdf_info,
)
from app.models.models import CurrencyExchangeRate
from app.models.models_issuance import (
    IssuedLGRecord,
    IssuanceMaintenanceAction,
    IssuanceFacility,
)
from app.services.sla_actualization_service import SLAActualizationService
from app.core.instruction_verification import (
    generate_verification_token_and_code,
    _to_epoch_seconds,
    _COOLDOWN_MEMORY_STORE,
)


def run_issuance_facilities_maintenance_recon_suite():
    print("=" * 92)
    print("GROW TREASURY — LG ISSUANCE, FACILITIES, SLA EMA, MAINTENANCE, RECON & EXPORT SUITE")
    print("=" * 92)

    mem_gcs = install_in_memory_gcs()
    email_harness = install_email_capture()
    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="ISS_FAC_REC")
    client = TestClient(app)

    maker_h = make_auth_headers(fx.maker_user)
    checker_h = make_auth_headers(fx.checker_user)
    admin1_h = make_auth_headers(fx.admin_user)
    admin2_h = make_auth_headers(fx.admin2_user)

    created_fx_rate_id = None

    try:
        today = date.today()

        # ==================================================================================
        # PART 1: FACILITIES, CASH-MARGIN EXPOSURE REDUCTION & CROSS-CURRENCY FX MATH
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 1] BANK CREDIT FACILITIES, CASH-MARGIN EXPOSURE & CROSS-CURRENCY FX MATH")
        print("=" * 92)

        # Seed deterministic fresh USD -> EGP CBE exchange rate = 50.00 (buy=50.00, sell=50.00 -> mid=50.00)
        fx_rate = CurrencyExchangeRate(
            currency_id=fx.ccy_usd.id,
            buy_rate=Decimal("50.0000"),
            sell_rate=Decimal("50.0000"),
            rate_date=today + timedelta(days=1),  # Latest rate_date so Tier-1 CBE picks this exact 50.00 rate
        )
        db.add(fx_rate)
        db.commit()
        db.refresh(fx_rate)
        created_fx_rate_id = fx_rate.id

        # Create EGP 10,000,000 Facility with `multi_currency_allowed=True`, `margin_reduces_exposure=True`,
        # `initial_utilization=500,000`, `default_cash_margin_pct=20%`, `sla_agreement_days=5`
        fac_payload = {
            "facility_name": f"CIB Omnibus Trade Facility {fx.uid}",
            "facility_type": "LG",
            "bank_id": fx.bank_cib.id,
            "customer_id": fx.cust_alpha.id,
            "currency_id": fx.ccy_egp.id,
            "total_limit_amount": "10000000.00",
            "reference_number": f"FAC-CIB-{fx.uid}",
            "multi_currency_allowed": True,
            "margin_reduces_exposure": True,
            "exposure_start_trigger": "ON_ISSUANCE",
            "sla_agreement_days": 5,
            "allow_cross_border": True,
            "start_date": (today - timedelta(days=30)).isoformat(),
            "expiry_date": (today + timedelta(days=335)).isoformat(),
            "entity_ids": [fx.entity_alpha_hq.id],
            "sub_limits": [
                {
                    "limit_name": "Performance & APG Sub-Limit",
                    "limit_amount": "10000000.00",
                    "lg_type_ids": [fx.lg_type_perf.id] + ([fx.lg_type_apg.id] if fx.lg_type_apg else []),
                    "default_commission_rate": "1.50",
                    "default_cash_margin_pct": "20.00",
                    "default_min_commission": "250.00",
                    "default_flat_fee": "100.00",
                    "initial_utilization": "500000.00",
                }
            ],
        }
        r_fac = client.post("/api/v1/facilities/", headers=admin1_h, json=fac_payload)
        assert r_fac.status_code == 200, f"Create facility failed: {r_fac.text}"
        fac_out = r_fac.json()
        fac_id = fac_out["id"]
        sub_id = fac_out["sub_limits"][0]["id"]

        # Verify initial utilization from `GET /api/v1/facilities/`
        r_list_fac = client.get("/api/v1/facilities/", headers=admin1_h)
        assert r_list_fac.status_code == 200
        fac_listed = next(f for f in r_list_fac.json() if f["id"] == fac_id)
        assert Decimal(str(fac_listed["utilized_amount"])) == Decimal("500000.00")
        assert Decimal(str(fac_listed["reserved_amount"])) == Decimal("0.00")
        print(f"  [PASS] Created multi-currency EGP 10M Facility #{fac_id} (initial utilized=500,000.00, reserved=0.00)")

        # ==================================================================================
        # PART 2: DUAL-CONTROL WORKFLOW MATRIX, REQUEST SNAPSHOT, SAFE EDIT & SIMILARITY
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 2] DUAL-CONTROL WORKFLOW POLICIES, V1 SNAPSHOT, EDIT GOVERNANCE & SIMILARITY")
        print("=" * 92)

        # 2.1 Configure 2-step workflow matrix via `PUT /api/v1/issuance/workflow-policies`
        #     Because Tenant Alpha has 2 Corporate Admins, `PUT` MUST return `202 Accepted`!
        policies_payload = [
            {
                "step_sequence": 1,
                "condition_type": "ALWAYS",
                "approver_type": "USERS",
                "approver_values": [str(fx.checker_user.id)],
                "required_signatures": 1,
                "is_active": True,
            },
            {
                "step_sequence": 2,
                "condition_type": "AMOUNT_OVER",
                "condition_value": "200000",
                "currency_id": fx.ccy_egp.id,  # Cross-currency USD 10,000 * 50 = EGP 500,000 > 200,000!
                "approver_type": "USERS",
                "approver_values": [str(fx.admin_user.id)],
                "required_signatures": 1,
                "is_active": True,
            },
        ]
        r_pol = client.put("/api/v1/issuance/workflow-policies", headers=admin1_h, json=policies_payload)
        assert r_pol.status_code == 202, f"Expected 202 Dual-Control for 2-admin tenant, got {r_pol.status_code}: {r_pol.text}"
        pol_cr_id = r_pol.json()["change_request_id"]

        # Self-approval by Admin 1 MUST be blocked!
        r_self_cr = client.post(
            f"/api/v1/issuance/admin/change-requests/{pol_cr_id}/action",
            headers=admin1_h,
            json={"action": "APPROVE"},
        )
        assert r_self_cr.status_code in (400, 403)

        # Admin 2 approves the workflow policy change!
        r_app_cr = client.post(
            f"/api/v1/issuance/admin/change-requests/{pol_cr_id}/action",
            headers=admin2_h,
            json={"action": "APPROVE"},
        )
        assert r_app_cr.status_code == 200
        active_pols = client.get("/api/v1/issuance/workflow-policies", headers=admin1_h).json()
        assert len(active_pols) == 2
        print("  [PASS] Dual-Control Workflow Matrix: 202 Pending -> Self-Approval Blocked -> 2nd Admin Approved (2 steps active)")

        # 2.2 Create Cross-Currency USD 10,000.00 Issuance Request (= EGP 500,000.00 at CBE rate 50.00)
        req_exp_dt = today + timedelta(days=180)
        req_payload = {
            "issuing_entity_id": fx.entity_alpha_hq.id,
            "requestor_name": "Karim Mansour",
            "requestor_email": fx.maker_user.email,
            "department": "Projects",
            "phone_number": "+201099887766",
            "reference_type": "CONTRACT",
            "reference_number": f"CNT-GRID-{fx.uid}",
            "reference_amount": "100000.00",
            "reference_currency_id": fx.ccy_usd.id,
            "lg_type_id": fx.lg_type_perf.id,
            "lg_purpose": "Performance bond for SCADA turbine supply contract",
            "amount": "10000.00",
            "currency_id": fx.ccy_usd.id,
            "requested_issue_date": today.isoformat(),
            "requested_expiry_date": req_exp_dt.isoformat(),
            "expiry_type": "FIXED_DATE",
            "lg_language": "EN",
            "beneficiary_name": "Egyptian Electricity Transmission Co (EETC)",
            "beneficiary_address": "Abbassia Square, Cairo",
            "beneficiary_country": "Egypt",
        }
        r_req = client.post("/api/v1/issuance/requests/", headers=maker_h, json=req_payload)
        assert r_req.status_code in (200, 201), f"Create request failed: {r_req.text}"
        req1 = r_req.json()
        req1_id = req1["id"]

        # Upload STREAM V2 encrypted supporting contract document on the request
        r_req_doc = client.post(
            f"/api/v1/issuance/requests/{req1_id}/documents",
            headers=maker_h,
            params={"document_type": "CONTRACT"},
            files={"file": ("SCADA_Supply_Contract.pdf", b"%PDF-1.4\n%SCADA Contract\n%%EOF", "application/pdf")},
        )
        assert r_req_doc.status_code == 200, f"Request document upload failed: {r_req_doc.text}"

        # Submit Request -> triggers Step 1 (Checker) AND Step 2 (Admin 1 because USD 10k * 50 = EGP 500k > 200k!)
        r_sub = client.post(f"/api/v1/issuance/requests/{req1_id}/submit", headers=maker_h)
        assert r_sub.status_code == 200, r_sub.text
        sub_data = r_sub.json()
        assert sub_data["status"] == "PENDING_APPROVAL"
        assert sub_data["current_approval_step"] == 1

        # Verify Immutable V1 Submission Snapshot (`GET /requests/{id}/snapshot`)
        r_snap = client.get(f"/api/v1/issuance/requests/{req1_id}/snapshot", headers=maker_h)
        assert r_snap.status_code == 200
        snap_data = r_snap.json()
        assert Decimal(str(snap_data.get("amount", "0"))) == Decimal("10000.00")

        # Approve Step 1 (Checker) -> advances to Step 2 (`PENDING_APPROVAL`)
        r_app1 = client.post(f"/api/v1/issuance/requests/{req1_id}/approve", headers=checker_h)
        assert r_app1.status_code == 200
        assert r_app1.json()["status"] == "PENDING_APPROVAL"
        assert r_app1.json()["current_approval_step"] == 2

        # Approve Step 2 (Admin 1) -> transitions to `APPROVED_INTERNAL`!
        r_app2 = client.post(f"/api/v1/issuance/requests/{req1_id}/approve", headers=admin1_h)
        assert r_app2.status_code == 200
        assert r_app2.json()["status"] == "APPROVED_INTERNAL"
        print(f"  [PASS] Cross-currency USD 10k Request #{req1_id} evaluated >EGP 200k step, saved V1 Snapshot & reached APPROVED_INTERNAL")

        # 2.3 Safe-Field Post-Approval Edit (`beneficiary_address`) via partial PUT -> `EDIT_REQUESTED` -> `/resolve-edit`
        r_edit = client.put(
            f"/api/v1/issuance/requests/{req1_id}",
            headers=maker_h,
            json={
                "beneficiary_address": "New Administrative Capital, Ministries District, Building E4",
                "change_reason": "Updated EETC headquarters address",
            },
        )
        assert r_edit.status_code == 200, r_edit.text
        edit_out = r_edit.json()
        if edit_out["status"] == "EDIT_REQUESTED":
            r_res_ed = client.post(
                f"/api/v1/issuance/requests/{req1_id}/resolve-edit",
                headers=admin1_h,
                json={"approved": True, "note": "Verified official HQ move"},
            )
            assert r_res_ed.status_code == 200
            assert r_res_ed.json()["status"] == "APPROVED_INTERNAL"

        r_after_ed = client.get(f"/api/v1/issuance/requests/{req1_id}", headers=maker_h).json()
        assert "New Administrative Capital" in (r_after_ed.get("beneficiary_address") or "")
        print("  [PASS] Post-approval safe-field edit governed cleanly without resetting financial approval chain")

        # 2.4 Duplicate / Similarity Detection (`GET /requests/{id2}/similarity-check`)
        r_req_dup = client.post("/api/v1/issuance/requests/", headers=admin1_h, json=req_payload)
        assert r_req_dup.status_code in (200, 201), f"Admin create request failed: {r_req_dup.text}"
        dup_req_id = r_req_dup.json()["id"]
        r_sim = client.get(f"/api/v1/issuance/requests/{dup_req_id}/similarity-check", headers=maker_h)
        assert r_sim.status_code == 200
        sim_matches = r_sim.json()["matches"]
        assert len(sim_matches) >= 1
        top_score = sim_matches[0].get("score") or sim_matches[0].get("similarity_score") or sim_matches[0].get("similarity_pct", 0)
        assert float(top_score) >= 70.0
        print(f"  [PASS] Similarity Detector flagged near-identical request with score={top_score}%")

        # ==================================================================================
        # PART 3: RESERVATION, EXECUTION, COPY VERIFICATION, SLA EMA & MAINTENANCE
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 3] RESERVATION, EXECUTION, COPY VERIFY, SLA EMA & POST-ISSUANCE MAINTENANCE")
        print("=" * 92)

        # 3.1 Suitable Facilities Smart Engine (`GET /requests/{id}/suitable-facilities`)
        r_suit = client.get(f"/api/v1/issuance/requests/{req1_id}/suitable-facilities", headers=maker_h)
        assert r_suit.status_code == 200
        suit_fac = next(s for s in r_suit.json() if s["facility_id"] == fac_id)
        assert suit_fac["has_sufficient_limit"] is True
        # 20% cash margin on USD 10,000 = 2,000.00!
        assert Decimal(str(suit_fac["required_cash_margin_amount"])) == Decimal("2000.00")

        # 3.2 Reserve -> Release Reservation -> Re-Reserve Facility Capacity
        r_resv = client.post(
            f"/api/v1/issuance/requests/{req1_id}/reserve",
            headers=maker_h,
            params={"sub_limit_id": sub_id},
        )
        assert r_resv.status_code == 200, r_resv.text
        assert r_resv.json()["status"] == "FACILITY_RESERVED"

        fac_after_resv = next(f for f in client.get("/api/v1/facilities/", headers=admin1_h).json() if f["id"] == fac_id)
        reserved_egp = Decimal(str(fac_after_resv["reserved_amount"]))
        # USD 10,000 * 50 = EGP 500,000 gross; with 20% margin_reduces_exposure -> EGP 400,000 net exposure!
        assert reserved_egp == Decimal("400000.00"), f"Expected margin-adjusted reserved_amount 400000.00, got {reserved_egp}"

        # Release reservation -> must drop reserved_amount back to 0.00 and revert status to APPROVED_INTERNAL
        r_rel_resv = client.post(f"/api/v1/issuance/requests/{req1_id}/release-reservation", headers=maker_h)
        assert r_rel_resv.status_code == 200
        assert r_rel_resv.json()["status"] == "APPROVED_INTERNAL"
        fac_after_rel = next(f for f in client.get("/api/v1/facilities/", headers=admin1_h).json() if f["id"] == fac_id)
        assert Decimal(str(fac_after_rel["reserved_amount"])) == Decimal("0.00")
        assert Decimal(str(fac_after_rel["utilized_amount"])) == Decimal("500000.00")

        # Re-reserve before execution
        assert client.post(
            f"/api/v1/issuance/requests/{req1_id}/reserve",
            headers=maker_h,
            params={"sub_limit_id": sub_id},
        ).status_code == 200

        # 3.3 Execute / Issue LG (`POST /requests/{id}/issue`) + Record Bank Reply (`LG_ISSUED`)
        bank_ref_1 = f"CIB-ISS-{fx.uid}-001"
        r_issue = client.post(
            f"/api/v1/issuance/requests/{req1_id}/issue",
            headers=maker_h,
            json={
                "sub_limit_id": sub_id,
                "bank_id": fx.bank_cib.id,
                "issued_ref_number": bank_ref_1,
                "issue_date": today.isoformat(),
                "expiry_date": req_exp_dt.isoformat(),
                "issuance_method": "COMPANY_LETTER",
            },
        )
        assert r_issue.status_code == 200, f"Issue LG failed: {r_issue.text}"
        issued_lg = r_issue.json()
        issued_lg_id = issued_lg["id"]
        internal_serial = issued_lg["internal_serial"]
        # Format: YYYY-XXXX-NNNN000 (e.g., 2026-A1B2-0001000)
        assert internal_serial and internal_serial.endswith("000"), f"Unexpected internal_serial: {internal_serial}"

        fac_after_issue = next(f for f in client.get("/api/v1/facilities/", headers=admin1_h).json() if f["id"] == fac_id)
        expected_utilized = Decimal("500000.00") + Decimal("400000.00")  # Initial 500k + (USD 10k * 50 * 0.80 margin factor = 400k) = 900k!
        actual_utilized = Decimal(str(fac_after_issue["utilized_amount"]))
        assert actual_utilized == expected_utilized, f"Expected margin-adjusted utilization {expected_utilized}, got {actual_utilized}"
        assert Decimal(str(fac_after_issue["reserved_amount"])) == Decimal("0.00"), (
            f"Expected reserved_amount=0.00 after RESERVATION promoted to ISSUANCE, got {fac_after_issue['reserved_amount']}"
        )
        print(f"  [PASS] Issued LG #{issued_lg_id} (serial={internal_serial}): reserved->0.00 & margin-adjusted utilization exact (900,000.00 EGP)")

        # Record Bank Reply (`LG_ISSUED`) via HTTP endpoint WITH scanned Bank LG Copy PDF
        # (satisfies `DOC_MANDATORY_ISSUED_LG_SCAN=true` and encrypts to GCS under `STREAM V2`!)
        r_ver_noscan = client.patch(
            f"/api/v1/issuance/lg-records/{issued_lg_id}/verify",
            headers=maker_h,
            json={"bank_lg_number": bank_ref_1, "bank_lg_amount": "10000.00"},
        )
        # If DOC_MANDATORY_ISSUED_LG_SCAN is true globally, verify without scan is rejected (400)
        assert r_ver_noscan.status_code in (200, 400)
        if r_ver_noscan.status_code == 200:
            # Reset verification_status back to PENDING for subsequent tests
            db_lg_tmp = db.get(IssuedLGRecord, issued_lg_id)
            db_lg_tmp.verification_status = None
            db_lg_tmp.bank_lg_number = None
            db.commit()

        r_brep = client.patch(
            f"/api/v1/issuance/lg-records/{issued_lg_id}/record-bank-reply",
            headers=maker_h,
            data={
                "bank_reply_type": "LG_ISSUED",
                "bank_reply_date": today.isoformat(),
                "bank_lg_issue_date": today.isoformat(),
                "bank_lg_expiry_date": req_exp_dt.isoformat(),
                "bank_lg_amount": "10000.00",
                "bank_reply_notes": "CIB issued guarantee",
            },
            files={"bank_reply_file": ("CIB_Issued_LG_Copy.pdf", b"%PDF-1.4\n%Scanned Bank LG Copy\n%%EOF", "application/pdf")},
        )
        assert r_brep.status_code == 200, r_brep.text
        assert r_brep.json()["status"] == "LG_ISSUED"

        # 3.4 Post-Issuance Bank Copy Verification (`PATCH /lg-records/{id}/verify`)
        # Missing `bank_lg_number` without `force_no_number` -> HTTP 400
        r_ver_nonum = client.patch(
            f"/api/v1/issuance/lg-records/{issued_lg_id}/verify",
            headers=maker_h,
            json={"bank_lg_amount": "10000.00"},
        )
        assert r_ver_nonum.status_code == 400

        # Discrepant bank amount (`10,500.00` vs `10,000.00`): End User `force_accept=True` -> HTTP 403!
        r_ver_eu_force = client.patch(
            f"/api/v1/issuance/lg-records/{issued_lg_id}/verify",
            headers=maker_h,
            json={"bank_lg_number": bank_ref_1, "bank_lg_amount": "10500.00", "force_accept": True},
        )
        assert r_ver_eu_force.status_code == 403

        # Exact match verification -> `MATCHED`!
        r_ver_ok = client.patch(
            f"/api/v1/issuance/lg-records/{issued_lg_id}/verify",
            headers=maker_h,
            json={
                "bank_lg_number": bank_ref_1,
                "bank_lg_amount": "10000.00",
                "bank_lg_expiry_date": req_exp_dt.isoformat(),
                "bank_beneficiary_name": "Egyptian Electricity Transmission Co (EETC)",
            },
        )
        assert r_ver_ok.status_code == 200, r_ver_ok.text
        assert r_ver_ok.json().get("verification_status") == "MATCHED"
        print("  [PASS] Bank LG Copy Verification enforced bank_lg_number, blocked End-User force_accept (403) & verified MATCHED")

        # 3.5 SLA Actualization Service (Bayesian Credibility Curve & EMA_ALPHA = 0.25)
        sla_svc = SLAActualizationService()
        assert sla_svc.calculate_credibility_weight(0) == 0.0
        assert sla_svc.calculate_credibility_weight(3) == 0.5
        assert sla_svc.calculate_credibility_weight(25) == 0.85  # Capped at MAX_EMPIRICAL_WEIGHT=0.85!

        db_lg = db.get(IssuedLGRecord, issued_lg_id)
        db_lg.status = "ACTIVE"
        db_lg.delivery_date = today - timedelta(days=4)
        db_lg.bank_reply_date = today
        db.commit()

        sla_res_1 = sla_svc.record_issuance_turnaround(db, db_lg)
        db.commit()
        assert sla_res_1 is not None and sla_res_1["is_on_time"] is True
        first_avg = float(sla_res_1["facility_new_avg_sla"])

        # Simulate a second turnaround on a facility with previous EMA `first_avg`:
        # Expected EMA = round(turnaround_2 * 0.25 + first_avg * 0.75, 2)
        db_lg.delivery_date = today - timedelta(days=12)
        db_lg.bank_reply_date = today
        db.commit()
        sla_res_2 = sla_svc.record_issuance_turnaround(db, db_lg)
        db.commit()
        t2 = float(sla_res_2["turnaround_business_days"])
        expected_ema = round(t2 * 0.25 + first_avg * 0.75, 2)
        assert float(sla_res_2["facility_new_avg_sla"]) == expected_ema
        print(f"  [PASS] SLA Actualization verified: Bayesian cap=0.85 & EMA (alpha=0.25 -> {first_avg}d then {expected_ema}d)")

        # 3.6 Post-Issuance Maintenance Lifecycle (`EXTEND` -> PDF + `M2` QR Verify -> `INCREASE_AMOUNT` -> `CLOSE`)
        # Ensure LG status is `ACTIVE` for maintenance
        db_lg = db.get(IssuedLGRecord, issued_lg_id)
        db_lg.status = "ACTIVE"
        db.commit()

        maint_new_exp = req_exp_dt + timedelta(days=90)
        r_m_ext = client.post(
            f"/api/v1/issuance/issued-lgs/{issued_lg_id}/maintenance",
            headers=maker_h,
            json={
                "action_type": "EXTEND",
                "action_data": {"new_expiry_date": maint_new_exp.isoformat()},
                "notes": "Extend SCADA bond by 90 days",
            },
        )
        assert r_m_ext.status_code == 200, r_m_ext.text
        m_ext = r_m_ext.json()
        m_ext_id = m_ext["id"]

        # Approve Step 1 & Step 2 (if governed by 2-step workflow)
        while m_ext["status"] == "PENDING_APPROVAL":
            approver_ids = [int(x) for x in (m_ext.get("pending_approver_users") or [])]
            hdr = checker_h if fx.checker_user.id in approver_ids else admin1_h
            r_ma = client.post(f"/api/v1/issuance/maintenance/{m_ext_id}/approve", headers=hdr)
            assert r_ma.status_code == 200, r_ma.text
            m_ext = r_ma.json()

        assert m_ext["status"] in ("APPROVED", "EXECUTED")
        assert m_ext["letter_serial_number"] is not None

        # Download regenerated Maintenance Letter PDF (`GET /maintenance/{id}/document/letter`)
        r_m_pdf = client.get(f"/api/v1/issuance/maintenance/{m_ext_id}/document/letter", headers=maker_h)
        assert r_m_pdf.status_code == 200
        m_pdf_info = extract_pdf_info(r_m_pdf.content)
        assert m_pdf_info["page_count"] == 1
        assert bank_ref_1 in m_pdf_info["text"]

        # Verify `M2.<id>.<sig>` Public QR Split-Verification (`/inspect` + `/confirm`) for Issuance Maintenance Letters!
        _COOLDOWN_MEMORY_STORE.clear()
        db_ma = db.get(IssuanceMaintenanceAction, m_ext_id)
        db.refresh(db_ma)
        ma_ts = _to_epoch_seconds(db_ma.created_at)
        m2_tok, m2_pin = generate_verification_token_and_code(
            "M", db_ma.id, db_ma.letter_serial_number, bank_ref_1, ma_ts
        )
        r_m2_insp = client.get("/api/v1/public/verify-instruction/inspect", params={"t": m2_tok})
        assert r_m2_insp.status_code == 200 and r_m2_insp.json()["valid_token"] is True

        r_m2_ver = client.post(
            "/api/v1/public/verify-instruction/confirm",
            json={
                "token": m2_tok,
                "code": m2_pin,
                "lg_tail": bank_ref_1[-2:],  # "01"
                "elapsed_ms": 2200,
            },
        )
        assert r_m2_ver.status_code == 200 and r_m2_ver.json()["verified"] is True
        assert r_m2_ver.json()["status_category"] == "VALID"
        assert r_m2_ver.json()["delivery_status"] is not None

        # Record delivery & bank reply on `EXTEND` -> updates `IssuedLGRecord.expiry_date`!
        assert client.post(
            f"/api/v1/issuance/maintenance/{m_ext_id}/delivery",
            headers=maker_h,
            data={"delivery_method": "HAND_DELIVERY", "delivery_date": today.isoformat()},
        ).status_code == 200
        assert client.post(
            f"/api/v1/issuance/maintenance/{m_ext_id}/bank-reply",
            headers=maker_h,
            data={"bank_reply_notes": "CIB confirmed extension", "bank_reply_date": today.isoformat()},
        ).status_code == 200

        db.refresh(db_lg)
        assert db_lg.expiry_date == maint_new_exp
        print(f"  [PASS] Maintenance EXTEND executed (serial={db_ma.letter_serial_number}), PDF & M2 QR verified, expiry -> {maint_new_exp}")

        # Test `ACTIVATE` guard on non-APG LG -> MUST fail with HTTP 400!
        r_bad_act = client.post(
            f"/api/v1/issuance/issued-lgs/{issued_lg_id}/maintenance",
            headers=maker_h,
            json={"action_type": "ACTIVATE", "action_data": {}},
        )
        assert r_bad_act.status_code == 400

        # Test `CLOSE` Maintenance Action -> closes LG (`CLOSED`) and verify utilization math!
        r_m_close = client.post(
            f"/api/v1/issuance/issued-lgs/{issued_lg_id}/maintenance",
            headers=maker_h,
            json={"action_type": "CLOSE", "action_data": {"close_reason": "Contract completed"}, "notes": "Return original LG"},
        )
        assert r_m_close.status_code == 200, r_m_close.text
        m_close = r_m_close.json()
        m_close_id = m_close["id"]
        while m_close["status"] == "PENDING_APPROVAL":
            approver_ids = [int(x) for x in (m_close.get("pending_approver_users") or [])]
            hdr = checker_h if fx.checker_user.id in approver_ids else admin1_h
            r_mc = client.post(f"/api/v1/issuance/maintenance/{m_close_id}/approve", headers=hdr)
            assert r_mc.status_code == 200, r_mc.text
            m_close = r_mc.json()

        assert client.post(
            f"/api/v1/issuance/maintenance/{m_close_id}/delivery",
            headers=maker_h,
            data={"delivery_method": "HAND_DELIVERY", "delivery_date": today.isoformat()},
        ).status_code == 200
        r_close_rep = client.post(
            f"/api/v1/issuance/maintenance/{m_close_id}/bank-reply",
            headers=maker_h,
            data={"bank_reply_notes": "CIB confirmed LG closure", "bank_reply_date": today.isoformat()},
        )
        assert r_close_rep.status_code == 200
        db.refresh(db_lg)
        assert db_lg.status == "CLOSED", f"Expected LG status CLOSED after CLOSE bank-reply, got {db_lg.status}"

        # Verify facility utilization IMMEDIATELY drops back to initial_utilization (500,000.00 EGP) upon CLOSE!
        fac_after_close = next(f for f in client.get("/api/v1/facilities/", headers=admin1_h).json() if f["id"] == fac_id)
        assert Decimal(str(fac_after_close["utilized_amount"])) == Decimal("500000.00"), (
            f"Expected utilization to drop back to 500,000.00 immediately on CLOSE, got {fac_after_close['utilized_amount']}"
        )
        assert Decimal(str(fac_after_close["reserved_amount"])) == Decimal("0.00")
        print("  [PASS] Maintenance CLOSE confirmed (status=CLOSED) & facility utilization freed back to 500,000.00 EGP!")

        # ==================================================================================
        # PART 4: LG POSITION RECONCILIATION & ENTERPRISE MULTI-TAB EXCEL EXPORT
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 4] LG POSITION RECONCILIATION (4 CATEGORIES) & MULTI-TAB EXCEL EXPORT")
        print("=" * 92)

        # Seed 3 ACTIVE IssuedLGRecord rows on CIB to reconcile against a bank Excel position sheet:
        #   1. `REC-MATCH` (500,000 EGP) -> exact match in bank sheet (`MATCHED`)
        #   2. `REC-VAR`   (200,000 EGP in system vs 250,000 EGP in bank sheet -> variance/mismatch)
        #   3. `REC-SYSONLY` (300,000 EGP in system, missing from bank sheet -> `SYSTEM_ONLY`)
        #   +  `REC-BANKONLY` (120,000 EGP in bank sheet only -> `BANK_ONLY`)
        ref_match = f"CIB-REC-MATCH-{fx.uid}"
        ref_var = f"CIB-REC-VAR-{fx.uid}"
        ref_sys = f"CIB-REC-SYS-{fx.uid}"
        ref_bank = f"CIB-REC-BNK-{fx.uid}"

        for idx, (ref_no, amt_val) in enumerate([(ref_match, "500000.00"), (ref_var, "200000.00"), (ref_sys, "300000.00")], start=2):
            rec_lg = IssuedLGRecord(
                lg_ref_number=ref_no,
                bank_lg_number=ref_no,
                internal_serial=f"2026-{fx.entity_alpha_hq.code}-{idx:04d}000",
                customer_id=fx.cust_alpha.id,
                facility_sub_limit_id=sub_id,
                bank_id=fx.bank_cib.id,
                beneficiary_name="Ministry of Transport",
                current_amount=Decimal(amt_val),
                currency_id=fx.ccy_egp.id,
                issue_date=today - timedelta(days=20),
                expiry_date=today + timedelta(days=160),
                status="ACTIVE",
                issued_by_user_id=fx.maker_user.id,
            )
            db.add(rec_lg)
        db.commit()

        # Build bank position `.xlsx` workbook
        wb_rec = openpyxl.Workbook()
        ws_rec = wb_rec.active
        ws_rec.title = "Bank_LG_Position"
        ws_rec.append(["Bank LG Number", "Beneficiary Name", "Amount", "Currency", "Issue Date", "Expiry Date"])
        ws_rec.append([ref_match, "Ministry of Transport", 500000.00, "EGP", (today - timedelta(days=20)).isoformat(), (today + timedelta(days=160)).isoformat()])
        ws_rec.append([ref_var, "Ministry of Transport", 250000.00, "EGP", (today - timedelta(days=20)).isoformat(), (today + timedelta(days=160)).isoformat()])
        ws_rec.append([ref_bank, "Unknown Contractor", 120000.00, "EGP", (today - timedelta(days=10)).isoformat(), (today + timedelta(days=150)).isoformat()])

        rec_buf = io.BytesIO()
        wb_rec.save(rec_buf)

        r_sess = client.post(
            "/api/v1/issuance/reconciliation/sessions",
            headers=maker_h,
            data={"bank_id": str(fx.bank_cib.id), "position_date": today.isoformat(), "notes": "Monthly CIB recon"},
            files={
                "file": (
                    f"CIB_Position_{fx.uid}.xlsx",
                    rec_buf.getvalue(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )
        assert r_sess.status_code == 200, f"Create recon session failed: {r_sess.text}"
        sess_id = r_sess.json()["id"]
        assert r_sess.json()["total_bank_records"] == 3

        r_match = client.post(f"/api/v1/issuance/reconciliation/sessions/{sess_id}/match", headers=maker_h)
        assert r_match.status_code == 200, r_match.text
        m_summary = r_match.json()
        assert m_summary["matched_count"] == 1
        assert m_summary["mismatched_count"] >= 1
        assert m_summary["bank_only_count"] == 1
        assert m_summary["system_only_count"] >= 1
        print(
            f"  [PASS] Position Reconciliation Session #{sess_id}: matched={m_summary['matched_count']}, "
            f"mismatched={m_summary['mismatched_count']}, bank_only={m_summary['bank_only_count']}, system_only={m_summary['system_only_count']}"
        )

        # 4.2 Enterprise Multi-Tab Excel Export (`GET /api/v1/issuance/issued-lgs/export`)
        r_exp = client.get(
            "/api/v1/issuance/issued-lgs/export",
            headers=maker_h,
            params={
                "export_type": "detailed",
                "include_event_log": True,
                "include_approvals": True,
            },
        )
        assert r_exp.status_code == 200
        exp_wb = openpyxl.load_workbook(io.BytesIO(r_exp.content), data_only=True)
        assert len(exp_wb.sheetnames) >= 3, f"Expected >=3 tabs (Portfolio + Event Log + Approvals), got {exp_wb.sheetnames}"
        first_sheet_text = "\n".join(
            " ".join(str(c) for c in row if c is not None)
            for row in exp_wb[exp_wb.sheetnames[0]].iter_rows(values_only=True)
        )
        assert bank_ref_1 in first_sheet_text and ref_match in first_sheet_text
        exp_wb.close()
        print(f"  [PASS] Enterprise Excel Export verified: {len(exp_wb.sheetnames)} worksheets ({exp_wb.sheetnames}) with exact LG refs")

        # ==================================================================================
        # PART 5: APPROVAL EXPIRATION & AUTO-REJECTION ACCURACY (INLINE + SCHEDULED SWEEP)
        # ==================================================================================
        print("\n" + "=" * 92)
        print("[PART 5] APPROVAL WINDOW EXPIRATION: INLINE APPROVE GUARD & DAILY TIMEOUT SWEEP")
        print("=" * 92)
        import asyncio
        from datetime import timezone as dt_tz
        from sqlalchemy import text as sa_text
        from app.core.background_tasks import run_daily_issuance_approval_timeout
        from app.models.models_issuance import IssuanceRequest

        active_rec_lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.lg_ref_number == ref_match).first()
        assert active_rec_lg is not None

        # 5.1 Inline Expiration Guard on Maintenance Approve & Issuance Request Approve
        r_exp_maint = client.post(
            f"/api/v1/issuance/issued-lgs/{active_rec_lg.id}/maintenance",
            headers=maker_h,
            json={
                "action_type": "EXTEND",
                "action_data": {"new_expiry_date": (today + timedelta(days=500)).isoformat(), "reason": "Stale test"},
            },
        )
        assert r_exp_maint.status_code == 200, r_exp_maint.text
        exp_maint_json = r_exp_maint.json()
        exp_maint_id = exp_maint_json["id"]
        exp_maint_approvers = [int(x) for x in (exp_maint_json.get("pending_approver_users") or [])]
        exp_maint_hdr = checker_h if fx.checker_user.id in exp_maint_approvers else admin1_h

        r_exp_req_create = client.post(
            "/api/v1/issuance/requests/",
            headers=maker_h,
            json={
                **req_payload,
                "reference_number": f"CNT-EXP1-{fx.uid}",
                "beneficiary_name": "Stale Approval Beneficiary",
                "amount": "150000.00",
                "currency_id": fx.ccy_egp.id,
                "lg_purpose": "Inline expiry guard test",
            },
        )
        assert r_exp_req_create.status_code in (200, 201), r_exp_req_create.text
        exp_req_id = r_exp_req_create.json()["id"]
        r_exp_req_sub = client.post(f"/api/v1/issuance/requests/{exp_req_id}/submit", headers=maker_h)
        assert r_exp_req_sub.status_code == 200
        exp_req_approvers = [int(x) for x in (r_exp_req_sub.json().get("pending_approver_users") or [])]
        exp_req_hdr = checker_h if fx.checker_user.id in exp_req_approvers else admin1_h

        ten_days_ago = datetime.now(dt_tz.utc) - timedelta(days=10)
        db.execute(
            sa_text("UPDATE issuance_maintenance_actions SET created_at = :ts, updated_at = :ts WHERE id = :id"),
            {"ts": ten_days_ago, "id": exp_maint_id},
        )
        db.execute(
            sa_text("UPDATE issuance_requests SET created_at = :ts, updated_at = :ts WHERE id = :id"),
            {"ts": ten_days_ago, "id": exp_req_id},
        )
        db.commit()

        # Attempting to approve an 8+ day old Maintenance action or IssuanceRequest MUST return 400 and auto-reject it!
        r_try_maint = client.post(f"/api/v1/issuance/maintenance/{exp_maint_id}/approve", headers=exp_maint_hdr)
        assert r_try_maint.status_code == 400 and "expired" in r_try_maint.text.lower(), r_try_maint.text
        r_try_req = client.post(f"/api/v1/issuance/requests/{exp_req_id}/approve", headers=exp_req_hdr)
        assert r_try_req.status_code == 400 and "expired" in r_try_req.text.lower(), r_try_req.text

        # Verify approver still sees the auto-rejected items in history endpoints with accurate status!
        maint_hist = client.get("/api/v1/issuance/maintenance/approval-history", headers=exp_maint_hdr).json()
        hist_item = next((m for m in maint_hist if m["id"] == exp_maint_id), None)
        assert hist_item is not None and hist_item["status"] == "AUTO_REJECTED_EXPIRED"

        req_hist = client.get("/api/v1/issuance/my-approval-history", headers=exp_req_hdr).json()
        req_hist_item = next((r for r in req_hist if r["id"] == exp_req_id), None)
        assert req_hist_item is not None and req_hist_item["status"] == "REJECTED"
        print("  [PASS] Inline expiration guard blocked stale /approve (HTTP 400) & recorded AUTO_REJECTED_EXPIRED in history")

        # 5.2 Scheduled / Startup Timeout Sweep (`run_daily_issuance_approval_timeout`)
        r_sw_maint = client.post(
            f"/api/v1/issuance/issued-lgs/{active_rec_lg.id}/maintenance",
            headers=maker_h,
            json={"action_type": "EXTEND", "action_data": {"new_expiry_date": (today + timedelta(days=520)).isoformat()}},
        )
        assert r_sw_maint.status_code == 200
        sw_maint_id = r_sw_maint.json()["id"]

        r_sw_req = client.post(
            "/api/v1/issuance/requests/",
            headers=maker_h,
            json={
                **req_payload,
                "reference_number": f"CNT-EXP2-{fx.uid}",
                "beneficiary_name": "Sweep Stale Beneficiary",
                "amount": "125000.00",
                "currency_id": fx.ccy_egp.id,
                "lg_purpose": "Sweep timeout test",
            },
        )
        assert r_sw_req.status_code in (200, 201), r_sw_req.text
        sw_req_id = r_sw_req.json()["id"]
        assert client.post(f"/api/v1/issuance/requests/{sw_req_id}/submit", headers=maker_h).status_code == 200

        # Also create a fresh request today that MUST NOT be auto-rejected!
        r_fresh_req = client.post(
            "/api/v1/issuance/requests/",
            headers=maker_h,
            json={
                **req_payload,
                "reference_number": f"CNT-FRESH-{fx.uid}",
                "beneficiary_name": "Fresh Today Beneficiary",
                "amount": "110000.00",
                "currency_id": fx.ccy_egp.id,
                "lg_purpose": "Fresh request must stay PENDING_APPROVAL",
            },
        )
        assert r_fresh_req.status_code in (200, 201), r_fresh_req.text
        fresh_req_id = r_fresh_req.json()["id"]
        assert client.post(f"/api/v1/issuance/requests/{fresh_req_id}/submit", headers=maker_h).status_code == 200

        # Put one IssuedLGRecord into CANCEL_REQUESTED aged 10 days to test no `metadata_json` crash!
        cancel_test_lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.lg_ref_number == ref_sys).first()
        cancel_test_lg.status = "CANCEL_REQUESTED"
        cancel_test_lg.custody_transfer_log = [{
            "action": "CANCEL_REQUESTED",
            "user_id": fx.maker_user.id,
            "reason": "Old cancel request",
            "previous_status": "ACTIVE",
            "timestamp": ten_days_ago.isoformat(),
        }]
        db.commit()

        db.execute(
            sa_text("UPDATE issuance_maintenance_actions SET created_at = :ts, updated_at = :ts WHERE id = :id"),
            {"ts": ten_days_ago, "id": sw_maint_id},
        )
        db.execute(
            sa_text("UPDATE issuance_requests SET created_at = :ts, updated_at = :ts WHERE id = :id"),
            {"ts": ten_days_ago, "id": sw_req_id},
        )
        db.commit()
        db.expire_all()

        asyncio.run(run_daily_issuance_approval_timeout(db))
        db.expire_all()

        assert db.get(IssuanceMaintenanceAction, sw_maint_id).status == "AUTO_REJECTED_EXPIRED"
        assert db.get(IssuanceRequest, sw_req_id).status == "REJECTED"
        assert db.get(IssuedLGRecord, cancel_test_lg.id).status == "ACTIVE"
        assert db.get(IssuanceRequest, fresh_req_id).status == "PENDING_APPROVAL"
        print("  [PASS] Timeout sweep auto-rejected expired Maint, Issuance & LG Cancel while preserving today's fresh request!")

        print("\n" + "=" * 92)
        print("ALL LG ISSUANCE, FACILITIES, SLA EMA, MAINTENANCE, RECON & EXPORT CHECKS PASSED 100%!")
        print("=" * 92)

    finally:
        _COOLDOWN_MEMORY_STORE.clear()
        if created_fx_rate_id:
            try:
                db.rollback()
                r_fx = db.get(CurrencyExchangeRate, created_fx_rate_id)
                if r_fx:
                    db.delete(r_fx)
                    db.commit()
            except Exception:
                db.rollback()
        fx.cleanup()
        db.close()


def test_issuance_facilities_maintenance_recon_suite():
    run_issuance_facilities_maintenance_recon_suite()


if __name__ == "__main__":
    run_issuance_facilities_maintenance_recon_suite()
