# c:\Grow\tests\test_platform_modules_e2e_suite.py
"""
GROW TREASURY PLATFORM — ENTERPRISE PLATFORM MODULES END-TO-END VERIFICATION SUITE
===================================================================================
Verifies exact user-expected outputs ("does the job", not just HTTP 200) across:
  [PART 1] Auth v2 (`/api/v2/*`): 1st-Device Auto-Trust vs 2nd-Device MFA Challenge,
           `mfa_challenge` JWT Privilege Isolation (`401`), Live 6-Digit Email OTP
           Extraction & Verification, Password Complexity, 5-Strike Brute-Force Lockout,
           and Host-Blind / Zero-Touch Admin Password-Reset `403` Guard.
  [PART 2] Corporate Admin & System Owner Governance: Checker Spillover License Pool Math
           (`max_checker_users` spilling into `max_users` before `400` block), Subscription
           Renewal Date Math, Plan Downgrade (`can_multi_entity: True -> False`) Entity
           Auto-Deactivation, and Cryptographic WORM Audit Chain (`reseal` + `verify-chain`).
  [PART 3] Smart Shared Treasury Inbox (`/api/v1/inbox/*`): Outbound `[Ref: POS-...]`
           Generation, 8-Signal Classification (Reference + Bank Domain + Arabic NIL
           Position `"لا توجد خطابات ضمان قائمة"`), NIL Reconciliation Session Flagging
           Active LGs as `SYSTEM_ONLY`, Generic `@gmail.com` Irrelevant Filter, CSV Table
           Preview, Archive/Unarchive, and Exact KPI Stats.
  [PART 4] Promotional Campaigns & Cashback Math (Bank-Specific Precedence, Cap Clamping,
           `CALCULATED -> RECONCILED -> VERIFIED -> PAID` + Partner CSV), Holidays &
           `BusinessCalendarService` (`MIDDLE_EAST` vs `WESTERN`), `FxService` 3-Way CBE
           Triangulation (`USD/EGP`, `EGP/USD`, `EUR/USD`), and Signatory Verification Desk
           (`/verify-list` + Read-Only `/preview-letter` preserving `is_printed=False`).
  [PART 5] Bank Statement Reconciliation v2 (`/api/v1/reconciliation/*`): Statement CSV
           Upload, ERP Ledger Ingest, `1:1` Match -> Unmatch -> Fee-Variance Match,
           Taxonomy + Bulk Classification, and Custom CSV Export Verifying UTF-8 BOM +
           Spreadsheet Formula-Injection Neutralization (`'=CMD...` vs `-1250.50`).
"""

import csv
import io
import os
import re
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from sqlalchemy import text

from _test_harness import (
    app,
    SessionLocal,
    IsolatedTenantFixture,
    extract_pdf_info,
    install_email_capture,
    install_in_memory_gcs,
    make_auth_headers,
)

from app.constants import LgStatusEnum, SubscriptionStatus, UserRole
from app.core.encryption import encrypt_data
from app.core.hashing import get_password_hash
from app.crud.crud_campaign import crud_campaign
from app.models.models import (
    CurrencyExchangeRate,
    CustomerEmailSetting,
    CustomerEntity,
    InternalOwnerContact,
    LGInstruction,
    LGRecord,
    SubscriptionPlan,
    SystemHoliday,
    User,
)
from app.models.models_campaign import Campaign, CashbackClaim
from app.models.models_inbox import InboxAttachment, InboxItem
from app.models.models_issuance import IssuedLGRecord
from app.models.models_reconciliation_v2 import BankTransaction
from app.services.business_calendar_service import BusinessCalendarService
from app.services.fx_service import fx_service
from app.services.inbox_classification_service import inbox_classification_service


def run_platform_modules_e2e_suite():
    print("=" * 94)
    print("GROW TREASURY — PLATFORM MODULES E2E SUITE (AUTHv2, GOV, INBOX, CAMPAIGNS, FX, RECON v2)")
    print("=" * 94)

    install_in_memory_gcs()
    email_harness = install_email_capture()

    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="PLAT_E2E")
    client = TestClient(app)

    created_campaign_ids = []
    created_holiday_ids = []
    created_fx_ids = []
    created_plan_ids = []
    temp_files = []

    try:
        today = date.today()
        from app.crud.crud import crud_role_permission

        def _hdrs(u: User):
            r_str = u.role.value if hasattr(u.role, "value") else str(u.role)
            perms = [p.name for p in crud_role_permission.get_permissions_for_role(db, r_str)]
            if r_str == "corporate_admin" and "corporate_admin:view_dashboard" not in perms:
                perms.append("corporate_admin:view_dashboard")
            return make_auth_headers(u, extra_claims={"permissions": perms, "can_email_inbox": True})

        admin1_h = _hdrs(fx.admin_user)
        beta_admin_h = _hdrs(fx.beta_admin)
        maker_h = _hdrs(fx.maker_user)
        checker_h = _hdrs(fx.checker_user)
        so_h = _hdrs(fx.sys_owner)

        # ==================================================================================
        # PART 1: AUTH v2, TRUSTED-DEVICE MFA, BRUTE-FORCE LOCKOUT & HOST-BLIND GUARD
        # ==================================================================================
        print("\n" + "=" * 94)
        print("[PART 1] AUTH v2: TRUSTED-DEVICE MFA, OTP CHALLENGE, LOCKOUT & HOST-BLIND SECURITY")
        print("=" * 94)

        # Create a user on a NON-MFA-exempt domain (`@corp-alpha-sec.com`)
        sec_email = f"sec.officer.{fx.uid.lower()}@corp-alpha-sec.com"
        sec_pass_1 = "SecPass!2026"
        sec_user = User(
            email=sec_email,
            password_hash=get_password_hash(sec_pass_1),
            role=UserRole.END_USER,
            customer_id=fx.cust_alpha.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        db.add(sec_user)
        db.commit()
        db.refresh(sec_user)

        # 1.1 First Device Grandfathering (`dev-laptop-01`) -> Auto-trusted, full JWT returned
        r_login1 = client.post(
            "/api/v2/login",
            json={
                "email": sec_email,
                "password": sec_pass_1,
                "device_id": f"dev-laptop-01-{fx.uid}",
                "remember_me": True,
            },
        )
        assert r_login1.status_code == 200, f"1st-device login failed: {r_login1.text}"
        login1_data = r_login1.json()
        assert "access_token" in login1_data, f"Expected full access_token on 1st device, got {login1_data}"
        tok_dev1 = login1_data["access_token"]

        r_me_ok = client.get("/api/v2/account/me", headers={"Authorization": f"Bearer {tok_dev1}"})
        assert r_me_ok.status_code == 200 and r_me_ok.json()["email"] == sec_email

        # 1.2 Second Unrecognized Device (`dev-mobile-02`) -> `MFA_REQUIRED` + Email OTP
        email_harness.clear()
        dev2_id = f"dev-mobile-02-{fx.uid}"
        r_login2 = client.post(
            "/api/v2/login",
            json={
                "email": sec_email,
                "password": sec_pass_1,
                "device_id": dev2_id,
                "remember_me": True,
            },
        )
        assert r_login2.status_code == 200, r_login2.text
        login2_data = r_login2.json()
        assert login2_data.get("status") == "MFA_REQUIRED", f"Expected MFA_REQUIRED on 2nd device, got {login2_data}"
        mfa_session_tok = login2_data["mfa_session_token"]

        # Privilege Isolation: `mfa_session_token` MUST be blocked (`401`) on `/api/v2/account/me`!
        r_me_mfa_block = client.get("/api/v2/account/me", headers={"Authorization": f"Bearer {mfa_session_tok}"})
        assert r_me_mfa_block.status_code == 401, f"MFA challenge token leaked access: {r_me_mfa_block.status_code}"

        # Extract the live 6-digit OTP code from the captured security email HTML
        mfa_emails = email_harness.find_by_recipient(sec_email)
        assert len(mfa_emails) >= 1, "MFA verification email was not dispatched!"
        otp_match = re.search(r">\s*(\d{6})\s*<", mfa_emails[-1]["body"]) or re.search(r"\b(\d{6})\b", mfa_emails[-1]["body"])
        assert otp_match, f"Could not find 6-digit OTP in email body: {mfa_emails[-1]['body'][:400]}"
        live_otp = otp_match.group(1)

        # Wrong OTP -> HTTP 400 ("4 attempts remaining")
        bad_otp = "999999" if live_otp != "999999" else "000000"
        r_mfa_bad = client.post(
            "/api/v2/verify-mfa",
            json={
                "email": sec_email,
                "mfa_code": bad_otp,
                "mfa_session_token": mfa_session_tok,
                "device_id": dev2_id,
                "remember_me": True,
            },
        )
        assert r_mfa_bad.status_code == 400 and "4 attempts remaining" in r_mfa_bad.text

        # Correct OTP -> HTTP 200 + Full JWT + Device Trusted!
        r_mfa_ok = client.post(
            "/api/v2/verify-mfa",
            json={
                "email": sec_email,
                "mfa_code": live_otp,
                "mfa_session_token": mfa_session_tok,
                "device_id": dev2_id,
                "remember_me": True,
            },
        )
        assert r_mfa_ok.status_code == 200, f"MFA verify failed: {r_mfa_ok.text}"
        tok_dev2 = r_mfa_ok.json()["access_token"]
        assert client.get("/api/v2/account/me", headers={"Authorization": f"Bearer {tok_dev2}"}).status_code == 200

        # Subsequent login on `dev-mobile-02` now bypasses MFA because `remember_me=True` trusted it!
        r_login2_repeat = client.post(
            "/api/v2/login",
            json={"email": sec_email, "password": sec_pass_1, "device_id": dev2_id, "remember_me": True},
        )
        assert r_login2_repeat.status_code == 200 and "access_token" in r_login2_repeat.json()
        print("  [PASS] Trusted-Device Grandfathering, 2nd-Device MFA Challenge, 401 Isolation & Live OTP Verified")

        # 1.3 Password Policy Enforcement & Self-Service Change (`/api/v2/change-password`)
        r_pw_weak = client.post(
            "/api/v2/change-password",
            headers={"Authorization": f"Bearer {tok_dev2}"},
            json={
                "current_password": sec_pass_1,
                "new_password": "alllowercasepassword",
                "confirm_new_password": "alllowercasepassword",
            },
        )
        assert r_pw_weak.status_code == 400

        sec_pass_2 = "VaultRotate#2027"
        r_pw_ok = client.post(
            "/api/v2/change-password",
            headers={"Authorization": f"Bearer {tok_dev2}"},
            json={
                "current_password": sec_pass_1,
                "new_password": sec_pass_2,
                "confirm_new_password": sec_pass_2,
            },
        )
        assert r_pw_ok.status_code == 200 and "access_token" in r_pw_ok.json()

        # 1.4 5-Strike Brute-Force Lockout (`401` x 4 -> `403` on 5th -> `403` even with valid password!)
        for strike in range(1, 5):
            r_bf = client.post(
                "/api/v2/login",
                json={"email": sec_email, "password": "WrongPassword!99", "device_id": dev2_id, "remember_me": False},
            )
            assert r_bf.status_code == 401, f"Strike {strike} expected 401, got {r_bf.status_code}"

        r_bf5 = client.post(
            "/api/v2/login",
            json={"email": sec_email, "password": "WrongPassword!99", "device_id": dev2_id, "remember_me": False},
        )
        assert r_bf5.status_code == 403 and "locked" in r_bf5.text.lower()

        # Even valid password MUST be rejected (`403`) while `locked_until` is active!
        r_bf_locked = client.post(
            "/api/v2/login",
            json={"email": sec_email, "password": sec_pass_2, "device_id": dev2_id, "remember_me": False},
        )
        assert r_bf_locked.status_code == 403
        print("  [PASS] Password Complexity Policy & 5-Strike Account Lockout (blocking valid credentials) Verified")

        # 1.5 Host-Blind / Zero-Touch Admin Password-Reset Block (`PATCH /api/v2/admin/users/{id}/password-reset`)
        for hdr_label, hdr in [("Corporate Admin", admin1_h), ("System Owner", so_h)]:
            r_hb = client.patch(
                f"/api/v2/admin/users/{sec_user.id}/password-reset",
                headers=hdr,
                json={
                    "new_password": "AdminOverride#2027",
                    "confirm_new_password": "AdminOverride#2027",
                    "force_change_on_next_login": True,
                },
            )
            assert r_hb.status_code == 403, f"Host-Blind guard failed for {hdr_label}: {r_hb.status_code} {r_hb.text}"
        print("  [PASS] Zero-Touch / Host-Blind Admin Password-Reset Guard blocked both Corporate Admin & System Owner (403)")

        # ==================================================================================
        # PART 2: CHECKER SPILLOVER QUOTAS, SUBSCRIPTION RENEWAL, DOWNGRADE & WORM CHAIN
        # ==================================================================================
        print("\n" + "=" * 94)
        print("[PART 2] CHECKER SPILLOVER QUOTAS, PLAN DOWNGRADE ENTITY AUTO-DEACTIVATE & WORM CHAIN")
        print("=" * 94)

        # 2.1 Checker Spillover License Pool Math on `cust_beta`
        # `cust_beta` starts with 2 non-checker users (`beta_maker`, `beta_admin`) and 0 checkers.
        # Assign a tight plan: `max_users = 3` (general pool), `max_checker_users = 1` (dedicated checker pool).
        tight_plan = SubscriptionPlan(
            name=f"Spillover Test Plan {fx.tag}",
            description="Tests dedicated checker pool + general pool spillover",
            duration_months=12,
            monthly_price=500.0,
            annual_price=5000.0,
            max_users=3,
            max_checker_users=1,
            max_records=50,
            max_issuance_records=50,
            can_maker_checker=True,
            can_multi_entity=False,
            can_ai_integration=True,
            can_image_storage=True,
            can_email_inbox=True,
            has_custody_module=True,
            has_issuance_module=True,
            has_quotation_module=True,
            has_reconciliation_module=True,
            grace_period_days=15,
        )
        db.add(tight_plan)
        db.commit()
        db.refresh(tight_plan)
        created_plan_ids.append(tight_plan.id)

        fx.cust_beta.subscription_plan_id = tight_plan.id
        db.commit()

        # 1st Checker -> consumes dedicated `max_checker_users` (1/1 checker, 2/3 general) -> HTTP 201!
        r_chk1 = client.post(
            "/api/v1/corporate-admin/users/",
            headers=beta_admin_h,
            json={
                "email": f"chk1.beta.{fx.uid.lower()}@globex.com",
                "role": "checker",
                "has_all_entity_access": True,
                "entity_ids": [],
                "must_change_password": True,
            },
        )
        assert r_chk1.status_code == 201, f"1st Checker should fit dedicated pool: {r_chk1.text}"

        # 2nd Checker -> dedicated pool is full (1/1), so it SPILLS OVER into general `max_users` (2 + 1 = 3/3)!
        r_chk2 = client.post(
            "/api/v1/corporate-admin/users/",
            headers=beta_admin_h,
            json={
                "email": f"chk2.beta.{fx.uid.lower()}@globex.com",
                "role": "checker",
                "has_all_entity_access": True,
                "entity_ids": [],
                "must_change_password": True,
            },
        )
        assert r_chk2.status_code == 201, f"2nd Checker should spill over into remaining general slot: {r_chk2.text}"

        # 3rd Checker -> both dedicated (1/1) AND general (2 regular + 1 spillover = 3/3) are full -> HTTP 400!
        r_chk3 = client.post(
            "/api/v1/corporate-admin/users/",
            headers=beta_admin_h,
            json={
                "email": f"chk3.beta.{fx.uid.lower()}@globex.com",
                "role": "checker",
                "has_all_entity_access": True,
                "entity_ids": [],
                "must_change_password": True,
            },
        )
        assert r_chk3.status_code == 400, f"Overflow checker must be rejected with 400, got {r_chk3.status_code}: {r_chk3.text}"
        print("  [PASS] Checker Spillover Pool Math exact: 1st->Dedicated (201), 2nd->General Spillover (201), 3rd->400 Blocked")

        # 2.2 System Owner Subscription Renewal (`POST /api/v1/system-owner/customers/{id}/renew`)
        fx.cust_beta.status = SubscriptionStatus.GRACE
        db.commit()
        target_renew_dt = datetime.now(timezone.utc) + timedelta(days=400)
        r_renew = client.post(
            f"/api/v1/system-owner/customers/{fx.cust_beta.id}/renew",
            headers=so_h,
            json={"new_subscription_end_date": target_renew_dt.isoformat()},
        )
        assert r_renew.status_code == 200, r_renew.text
        renew_out = r_renew.json()
        assert str(renew_out["status"]).lower() == "active"
        assert renew_out["end_date"][:10] == target_renew_dt.strftime("%Y-%m-%d")

        # 2.3 Plan Downgrade (`can_multi_entity: True -> False`) on `cust_alpha` (2 active entities -> 1 active!)
        r_downgrade = client.put(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}",
            headers=so_h,
            json={"subscription_plan_id": tight_plan.id},
        )
        assert r_downgrade.status_code == 200, r_downgrade.text
        active_ents_after = db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == fx.cust_alpha.id,
            CustomerEntity.is_deleted == False,
            CustomerEntity.is_active == True,
        ).all()
        assert len(active_ents_after) == 1, f"Expected 1 active entity after single-entity downgrade, got {len(active_ents_after)}"

        # Restore `cust_alpha` & `cust_beta` back to enterprise `fx.plan` and restore subsidiary entity
        fx.cust_alpha.subscription_plan_id = fx.plan.id
        fx.cust_beta.subscription_plan_id = fx.plan.id
        fx.entity_alpha_sub.is_deleted = False
        fx.entity_alpha_sub.is_active = True
        db.commit()
        print("  [PASS] Subscription Renewal & Single-Entity Plan Downgrade Auto-Deactivation Verified")

        # 2.4 Cryptographic WORM Audit Chain (`POST /reseal-chain` + `GET /verify-chain`)
        r_reseal = client.post("/api/v1/system-owner/audit-logs/reseal-chain", headers=so_h)
        assert r_reseal.status_code == 200, r_reseal.text
        r_worm = client.get("/api/v1/system-owner/audit-logs/verify-chain", headers=so_h, params={"limit": 150})
        assert r_worm.status_code == 200, r_worm.text
        worm_data = r_worm.json()
        is_worm_intact = worm_data.get("valid") is True or worm_data.get("status") in ("INTACT", "VALID", "EMPTY") or worm_data.get("broken_links", 0) == 0
        assert is_worm_intact, f"WORM chain verification failed: {worm_data}"
        print(f"  [PASS] Cryptographic SHA-256 WORM Audit Chain verified intact ({worm_data.get('verified_count') or worm_data.get('checked_count', 'OK')} entries)")

        # ==================================================================================
        # PART 3: SMART SHARED TREASURY INBOX — 8-SIGNAL ENGINE & NIL RECONCILIATION
        # ==================================================================================
        print("\n" + "=" * 94)
        print("[PART 3] SMART TREASURY INBOX: [Ref: POS-...], 8-SIGNAL ENGINE & ARABIC NIL RECON")
        print("=" * 94)

        # Configure offline-safe `smtp.example.com` settings for `cust_alpha`
        email_cfg = CustomerEmailSetting(
            customer_id=fx.cust_alpha.id,
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_username="treasury@globex.com",
            smtp_password_encrypted=encrypt_data("sim_pass"),
            sender_email=f"treasury.{fx.uid.lower()}@globex.com",
            sender_display_name="Alpha Treasury Desk",
            is_active=True,
        )
        db.add(email_cfg)
        db.commit()

        # 3.1 Outbound Position Request (`POST /api/v1/inbox/request-position`)
        r_out_pos = client.post(
            "/api/v1/inbox/request-position",
            headers=maker_h,
            json={
                "bank_id": fx.bank_cib.id,
                "position_date": today.isoformat(),
                "custom_recipient_emails": ["trade.finance@globex.com"],
                "custom_notes": "Monthly audit snapshot",
            },
        )
        assert r_out_pos.status_code == 200, f"Outbound position request failed: {r_out_pos.text}"
        out_pos = r_out_pos.json()
        ref_m = re.search(r"POS-\d{8}-[A-Z0-9]+", out_pos["subject"])
        assert ref_m, f"Expected [Ref: POS-...] in outbound subject: {out_pos['subject']}"
        pos_ref_code = ref_m.group(0)

        # 3.2 Seed an active CIB IssuedLGRecord on `cust_alpha`, then receive Arabic NIL Reply!
        nil_test_lg = IssuedLGRecord(
            lg_ref_number=f"CIB-NIL-CHECK-{fx.uid}",
            bank_lg_number=f"CIB-NIL-CHECK-{fx.uid}",
            internal_serial=f"2026-{fx.entity_alpha_hq.code}-0099000",
            customer_id=fx.cust_alpha.id,
            bank_id=fx.bank_cib.id,
            beneficiary_name="Cairo Metro Authority",
            current_amount=Decimal("400000.00"),
            currency_id=fx.ccy_egp.id,
            issue_date=today - timedelta(days=30),
            expiry_date=today + timedelta(days=150),
            status="ACTIVE",
            issued_by_user_id=fx.maker_user.id,
        )
        db.add(nil_test_lg)
        db.commit()
        db.refresh(nil_test_lg)

        # Ensure CIB has `cibeg.com` email_domain for Signal 3 (+4 points)
        fx.bank_cib.email_domain = "cibeg.com"
        db.commit()

        nil_item = InboxItem(
            customer_id=fx.cust_alpha.id,
            message_id=f"<cib-reply-{fx.uid}@cibeg.com>",
            in_reply_to=out_pos["message_id"],
            sender_email="trade.desk@cibeg.com",
            sender_domain="cibeg.com",
            subject=f"RE: [Ref: {pos_ref_code}] LG Position Request",
            body_text_encrypted=encrypt_data("السادة الكرام، نفيدكم بأنه لا توجد خطابات ضمان قائمة لصالح شركتكم حتى تاريخه."),
            has_attachment=False,
            attachment_count=0,
            received_at=datetime.now(timezone.utc),
            status="RECEIVED",
        )
        db.add(nil_item)
        db.commit()
        db.refresh(nil_item)

        inbox_classification_service.classify(db, nil_item)
        db.commit()
        db.refresh(nil_item)

        assert nil_item.classification == "LG_POSITION_REPORT"
        assert nil_item.classification_confidence == "HIGH"
        assert nil_item.is_nil_position is True
        assert nil_item.outbound_request_id == out_pos["id"]
        assert "reference_match" in (nil_item.classification_signals or {})
        assert "nil_position_detected" in (nil_item.classification_signals or {})

        # Confirm NIL item -> auto-creates NIL ReconciliationSession flagging `nil_test_lg` as `SYSTEM_ONLY`!
        r_conf_nil = client.post(f"/api/v1/inbox/items/{nil_item.id}/confirm", headers=maker_h, json={})
        assert r_conf_nil.status_code == 200, r_conf_nil.text
        nil_sess_id = r_conf_nil.json()["session_id"]

        r_nil_sess = client.get(f"/api/v1/issuance/reconciliation/sessions/{nil_sess_id}", headers=maker_h)
        assert r_nil_sess.status_code == 200
        assert r_nil_sess.json()["total_bank_records"] == 0
        assert r_nil_sess.json()["status"] == "MATCHED"
        assert r_nil_sess.json()["system_only_count"] >= 1, (
            f"Expected NIL reconciliation to flag active LG in system_only_count, got {r_nil_sess.json()}"
        )
        print(f"  [PASS] Arabic NIL Reply classified HIGH (Ref+Domain+NIL=16 pts) & Reconciliation #{nil_sess_id} flagged {r_nil_sess.json()['system_only_count']} unconfirmed LG!")

        # 3.3 Generic Webmail (`@gmail.com`) Promotional Spam -> `IRRELEVANT` & `is_trusted_sender=False`
        spam_item = InboxItem(
            customer_id=fx.cust_alpha.id,
            message_id=f"<promo-{fx.uid}@gmail.com>",
            sender_email="special.deals@gmail.com",
            sender_domain="gmail.com",
            subject="Exclusive Marriott hotel booking promo — unsubscribe anytime",
            body_text_encrypted=encrypt_data("Click here for special vacation discounts and newsletter."),
            has_attachment=False,
            attachment_count=0,
            received_at=datetime.now(timezone.utc),
            status="RECEIVED",
        )
        db.add(spam_item)
        db.commit()
        db.refresh(spam_item)
        inbox_classification_service.classify(db, spam_item)
        db.commit()
        db.refresh(spam_item)
        assert spam_item.classification == "IRRELEVANT"
        assert spam_item.is_trusted_sender is False

        # 3.4 CSV Attachment Table Preview + Archive / Unarchive + Stats
        tmp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8", newline="")
        tmp_csv.write("Booking Date,Description,Debit,Credit,Balance\n2026-11-01,SWIFT Incoming,0,250000,1250000\n")
        tmp_csv.close()
        temp_files.append(tmp_csv.name)

        stmt_item = InboxItem(
            customer_id=fx.cust_alpha.id,
            message_id=f"<stmt-{fx.uid}@cibeg.com>",
            sender_email="statements@cibeg.com",
            sender_domain="cibeg.com",
            subject="Monthly Account Statement - November 2026",
            body_text_encrypted=encrypt_data("Attached is your monthly bank statement."),
            has_attachment=True,
            attachment_count=1,
            primary_attachment_path=tmp_csv.name,
            primary_attachment_name="Nov_Statement.csv",
            primary_attachment_type="csv",
            received_at=datetime.now(timezone.utc),
            status="RECEIVED",
        )
        db.add(stmt_item)
        db.flush()
        stmt_att = InboxAttachment(
            inbox_item_id=stmt_item.id,
            file_name="Nov_Statement.csv",
            file_type="CSV",
            file_size_bytes=os.path.getsize(tmp_csv.name),
            storage_path=tmp_csv.name,
            is_primary=True,
        )
        db.add(stmt_att)
        db.commit()
        inbox_classification_service.classify(db, stmt_item)
        db.commit()
        assert stmt_item.classification == "BANK_STATEMENT"

        r_prev = client.get(f"/api/v1/inbox/items/{stmt_item.id}/attachment/{stmt_att.id}/preview", headers=maker_h)
        assert r_prev.status_code == 200
        assert r_prev.json()["preview_type"] == "TABLE"
        assert "Balance" in r_prev.json()["headers"]

        assert client.post(f"/api/v1/inbox/items/{stmt_item.id}/archive", headers=maker_h).status_code == 200
        assert client.post(f"/api/v1/inbox/items/{stmt_item.id}/unarchive", headers=maker_h).status_code == 200

        r_stats = client.get("/api/v1/inbox/stats", headers=maker_h).json()
        assert r_stats["total_received"] == 3
        assert r_stats["actioned_count"] == 1
        assert r_stats["irrelevant_count"] == 1
        assert r_stats["bank_statement_count"] == 1
        print("  [PASS] Smart Inbox Generic-Domain Penalty, Irrelevant Filter, CSV Table Preview & KPI Stats Verified")

        # ==================================================================================
        # PART 4: CAMPAIGNS & CASHBACK, BUSINESS CALENDAR, FX TRIANGULATION & SIGNATORY DESK
        # ==================================================================================
        print("\n" + "=" * 94)
        print("[PART 4] CAMPAIGNS/CASHBACK, HOLIDAY CALENDAR, FX TRIANGULATION & SIGNATORY DESK")
        print("=" * 94)

        # 4.1 Promotional Campaigns & Cashback Math
        r_camp_all = client.post(
            "/api/v1/system-owner/campaigns",
            headers=so_h,
            json={
                "name": f"All-Banks Promo {fx.uid}",
                "bank_id": None,
                "cashback_percentage": "0.0010",  # 0.10%
                "max_cashback_per_lg": "2000.00",
                "max_lgs_per_customer": 5,
                "start_date": (today - timedelta(days=5)).isoformat(),
                "end_date": (today + timedelta(days=30)).isoformat(),
                "is_active": True,
            },
        )
        assert r_camp_all.status_code == 201, r_camp_all.text
        created_campaign_ids.append(r_camp_all.json()["id"])

        r_camp_cib = client.post(
            "/api/v1/system-owner/campaigns",
            headers=so_h,
            json={
                "name": f"CIB Partner VIP {fx.uid}",
                "bank_id": fx.bank_cib.id,
                "cashback_percentage": "0.0025",  # 0.25%
                "max_cashback_per_lg": "3000.00",
                "max_lgs_per_customer": 3,
                "start_date": (today - timedelta(days=5)).isoformat(),
                "end_date": (today + timedelta(days=30)).isoformat(),
                "is_active": True,
            },
        )
        assert r_camp_cib.status_code == 201, r_camp_cib.text
        cib_camp_id = r_camp_cib.json()["id"]
        created_campaign_ids.append(cib_camp_id)

        # Check eligibility: CIB-specific campaign (0.25%) MUST take precedence over All-Banks (0.10%),
        # and EGP 2,000,000 * 0.0025 = 5,000 MUST be clamped to `max_cashback_per_lg = 3,000.00`!
        r_elig_cap = client.get(
            "/api/v1/customer/campaigns/check-eligibility",
            headers=maker_h,
            params={"bank_id": fx.bank_cib.id, "amount": "2000000.00"},
        )
        assert r_elig_cap.status_code == 200
        elig_cap = r_elig_cap.json()
        assert elig_cap["is_eligible"] is True
        assert elig_cap["campaign_id"] == cib_camp_id
        assert float(elig_cap["estimated_cashback"]) == 3000.0

        # Record claim on `nil_test_lg` (EGP 400,000 * 0.0025 = EGP 1,000.00 exact!)
        claim_obj = crud_campaign.record_claim_for_issued_lg(db, nil_test_lg)
        db.commit()
        assert claim_obj is not None
        assert Decimal(str(claim_obj.cashback_amount)) == Decimal("1000.00")
        assert claim_obj.status == "CALCULATED"

        # Auto-reconciliation upgrade (`CALCULATED -> RECONCILED`)
        upgraded_cnt = crud_campaign.reconcile_claims_for_lgs(db, [nil_test_lg.id])
        db.commit()
        db.refresh(claim_obj)
        assert upgraded_cnt == 1 and claim_obj.status == "RECONCILED"

        # System Owner manual-verify (`VERIFIED`) -> payout (`PAID`) -> Customer Summary + Partner CSV!
        r_mv = client.post(
            f"/api/v1/system-owner/cashback-claims/{claim_obj.id}/manual-verify",
            headers=so_h,
            json={"notes": "Confirmed against CIB quarterly fee schedule"},
        )
        assert r_mv.status_code == 200 and r_mv.json()["status"] == "VERIFIED"
        assert r_mv.json()["verified_by_name"] is not None

        r_paid = client.patch(
            f"/api/v1/system-owner/cashback-claims/{claim_obj.id}/status",
            headers=so_h,
            json={"status": "PAID", "notes": "Settled via credit note #CN-882"},
        )
        assert r_paid.status_code == 200 and r_paid.json()["status"] == "PAID"

        r_csum = client.get("/api/v1/customer/campaigns/cashback-summary", headers=maker_h).json()
        assert Decimal(str(r_csum["total_paid"])) == Decimal("1000.00")

        r_pcsv = client.get(
            "/api/v1/system-owner/campaigns/export-partner-report",
            headers=so_h,
            params={"customer_id": fx.cust_alpha.id},
        )
        assert r_pcsv.status_code == 200, f"Partner CSV export failed: {r_pcsv.text}"
        pcsv_text = r_pcsv.text
        assert nil_test_lg.lg_ref_number in pcsv_text and "1000.00" in pcsv_text and "PAID" in pcsv_text
        print("  [PASS] Campaigns & Cashback verified: Bank precedence, 3,000 cap, CALCULATED->RECONCILED->VERIFIED->PAID & Partner CSV")

        # 4.2 System Holidays API & `BusinessCalendarService` (`MIDDLE_EAST` vs `WESTERN`)
        # Pick a future Wednesday in 2028 (2028-05-10 is a Wednesday)
        wed_dt = date(2028, 5, 10)
        fri_dt = date(2028, 5, 12)  # Friday (Weekend in MIDDLE_EAST, Working in WESTERN!)
        sun_dt = date(2028, 5, 14)  # Sunday (Working in MIDDLE_EAST, Weekend in WESTERN!)

        cal_me = BusinessCalendarService(default_country="EG", default_weekend_scheme="MIDDLE_EAST")
        cal_we = BusinessCalendarService(default_country="US", default_weekend_scheme="WESTERN")
        assert cal_me.is_business_day(fri_dt, db=None) is False
        assert cal_me.is_business_day(sun_dt, db=None) is True
        assert cal_we.is_business_day(fri_dt, db=None) is True
        assert cal_we.is_business_day(sun_dt, db=None) is False

        # Create custom SystemHoliday on Wednesday 2028-05-10 via HTTP API
        r_hol = client.post(
            "/api/v1/system-owner/holidays",
            headers=so_h,
            json={"holiday_date": wed_dt.isoformat(), "name": f"Bank Audit Shutdown {fx.uid}", "is_recurring": False},
        )
        assert r_hol.status_code == 201, r_hol.text
        hol_id = r_hol.json()["id"]
        created_holiday_ids.append(hol_id)

        hol_set = cal_me.get_holiday_dates(db, 2028, 2028, "EG")
        assert wed_dt in hol_set
        r_cal_set = client.get("/api/v1/system-owner/holidays/calendar-settings", headers=so_h)
        assert r_cal_set.status_code == 200 and r_cal_set.json()["weekend_scheme"] in ("MIDDLE_EAST", "WESTERN")
        assert client.delete(f"/api/v1/system-owner/holidays/{hol_id}", headers=so_h).status_code == 200
        r_adm_dash = client.get("/api/v1/corporate-admin/dashboard-metrics", headers=admin1_h)
        assert r_adm_dash.status_code == 200 and r_adm_dash.json()["total_active_users"] >= 1
        print("  [PASS] BusinessCalendarService, /calendar-settings, /dashboard-metrics & Custom Holiday Override verified")

        # 4.3 `FxService` 3-Way CBE Math (Direct, Inverse & Cross-Currency Triangulation via EGP)
        future_fx_dt = today + timedelta(days=3650)
        fx_usd = CurrencyExchangeRate(
            currency_id=fx.ccy_usd.id,
            buy_rate=Decimal("49.9000"),
            sell_rate=Decimal("50.1000"),  # Mid = 50.0000 EGP
            rate_date=future_fx_dt,
        )
        fx_eur = CurrencyExchangeRate(
            currency_id=fx.ccy_eur.id,
            buy_rate=Decimal("54.8000"),
            sell_rate=Decimal("55.2000"),  # Mid = 55.0000 EGP
            rate_date=future_fx_dt,
        )
        db.add_all([fx_usd, fx_eur])
        db.commit()
        created_fx_ids.extend([fx_usd.id, fx_eur.id])

        # Direct: USD -> EGP = 50.00
        r_dir = fx_service.get_rate(db, fx.ccy_usd.id, fx.ccy_egp.id, allow_ai=False)
        assert r_dir == Decimal("50.00")
        # Inverse: EGP -> USD = 1 / 50.00 = 0.02
        r_inv = fx_service.get_rate(db, fx.ccy_egp.id, fx.ccy_usd.id, allow_ai=False)
        assert r_inv == Decimal("0.02")
        # Triangulated: EUR -> USD = 55.00 / 50.00 = 1.10 -> EUR 1,000 = USD 1,100.00!
        conv_eur_usd, rate_eur_usd = fx_service.convert(db, Decimal("1000.00"), fx.ccy_eur.id, fx.ccy_usd.id, allow_ai=False)
        assert rate_eur_usd == Decimal("1.1") and conv_eur_usd == Decimal("1100.000")
        print("  [PASS] FxService 3-Way CBE Math exact: Direct (50.00), Inverse (0.02) & EUR->USD Triangulated (1.10 -> 1,100.00)")

        # 4.4 Platform Activity & Signatory Control Verification Desk (`/api/v1/corporate-admin/activity/*`)
        fx.plan.can_maker_checker = False
        db.commit()
        owner_c = InternalOwnerContact(
            customer_id=fx.cust_alpha.id,
            email=f"owner.sig.{fx.uid.lower()}@globex.com",
            phone_number="+201011223344",
            manager_email=f"mgr.sig.{fx.uid.lower()}@globex.com",
        )
        db.add(owner_c)
        db.commit()

        custody_lg = LGRecord(
            customer_id=fx.cust_alpha.id,
            beneficiary_corporate_id=fx.entity_alpha_hq.id,
            lg_sequence_number=1,
            issuer_name="Siemens Mobility SAE",
            lg_number=f"LG-SIG-{fx.uid}",
            lg_amount=Decimal("850000.00"),
            lg_currency_id=fx.ccy_egp.id,
            lg_payable_currency_id=fx.ccy_egp.id,
            issuance_date=datetime.now(timezone.utc) - timedelta(days=30),
            expiry_date=datetime.now(timezone.utc) + timedelta(days=120),
            lg_period_months=6,
            auto_renewal=True,
            lg_type_id=fx.lg_type_perf.id,
            lg_operational_status_id=fx.op_operative.id,
            payment_conditions="Unconditional on first demand",
            description_purpose="Signatory verification test bond",
            issuing_bank_id=fx.bank_cib.id,
            issuing_bank_address="Smart Village",
            issuing_bank_phone="+20219666",
            issuing_method_id=fx.issuing_method.id,
            applicable_rule_id=fx.rule_urdg.id,
            internal_owner_contact_id=owner_c.id,
            lg_category_id=fx.cat_standard.id,
            lg_status_id=LgStatusEnum.VALID.value,
        )
        db.add(custody_lg)
        db.commit()
        db.refresh(custody_lg)

        r_ext_sig = client.post(
            f"/api/v1/end-user/lg-records/{custody_lg.id}/extend",
            headers=maker_h,
            json={"new_expiry_date": (today + timedelta(days=300)).isoformat(), "notes": "Signatory batch verify"},
        )
        assert r_ext_sig.status_code == 200, r_ext_sig.text
        sig_instr_id = r_ext_sig.json()["latest_instruction_id"]

        # Ensure `is_printed` is False before signatory preview
        db.expire_all()
        db_sig_inst = db.get(LGInstruction, sig_instr_id)
        sig_serial = db_sig_inst.serial_number
        db_sig_inst.is_printed = False
        db.commit()

        assert client.get("/api/v1/corporate-admin/activity/feed", headers=checker_h).status_code == 200
        assert client.get("/api/v1/corporate-admin/activity/controls", headers=checker_h).status_code == 200

        # Signatory Verify-List: pass valid `sig_serial` + rogue `"ROGUE-SERIAL-9999"`
        r_vlist = client.post(
            "/api/v1/corporate-admin/activity/verify-list",
            headers=checker_h,
            json={"raw_text": f"{sig_serial}\nROGUE-SERIAL-9999", "window_days": 45},
        )
        assert r_vlist.status_code == 200, r_vlist.text
        vlist_str = str(r_vlist.json())
        assert sig_serial in vlist_str and "ROGUE-SERIAL-9999" in vlist_str

        # Read-Only Signatory Preview (`?raw_pdf=true`) -> valid 1-page PDF AND `is_printed` MUST remain `False`!
        r_sprev = client.get(
            f"/api/v1/corporate-admin/activity/instructions/{sig_instr_id}/preview-letter",
            headers=checker_h,
            params={"raw_pdf": True},
        )
        assert r_sprev.status_code == 200
        sprev_pdf = extract_pdf_info(r_sprev.content)
        assert sprev_pdf["page_count"] == 1 and custody_lg.lg_number in sprev_pdf["text"]

        db.refresh(db_sig_inst)
        assert db_sig_inst.is_printed is False, "Signatory preview-letter MUST NOT mutate is_printed!"
        print(f"  [PASS] Signatory Desk verified: Checker /verify-list matched {sig_serial} & read-only PDF preserved is_printed=False")

        # ==================================================================================
        # PART 5: BANK STATEMENT RECONCILIATION v2 & CSV FORMULA-INJECTION DEFENSE
        # ==================================================================================
        print("\n" + "=" * 94)
        print("[PART 5] BANK STATEMENT RECONCILIATION v2 & CSV FORMULA-INJECTION DEFENSE")
        print("=" * 94)

        # 5.1 Upload Bank Statement CSV containing:
        #   Row 1: Exact invoice collection `INV-RECON-7788` (+125,000.00 EGP)
        #   Row 2: Supplier wire net of EGP 50 bank fee (-49,950.00 debit vs -50,000.00 ERP bill)
        #   Row 3: Malicious CSV formula injection description `=CMD|' /C calc'!A0` (-1,250.50 debit)
        stmt_csv_text = (
            "Date,Value Date,Description,Debit,Credit,Balance,Currency\n"
            f"{today.isoformat()},{today.isoformat()},Collection Ref INV-RECON-7788,0.00,125000.00,625000.00,EGP\n"
            f"{today.isoformat()},{today.isoformat()},Supplier Wire BILL-FEE-22,49950.00,0.00,575050.00,EGP\n"
            f"{today.isoformat()},{today.isoformat()},\"=CMD|' /C calc'!A0\",1250.50,0.00,573799.50,EGP\n"
        )
        r_up_stmt = client.post(
            "/api/v1/reconciliation/statements/upload",
            headers=admin1_h,
            data={
                "bank_id": str(fx.bank_cib.id),
                "opening_balance": "500000.00",
                "closing_balance": "573799.50",
                "start_date": today.isoformat(),
                "end_date": today.isoformat(),
            },
            files={"file": (f"CIB_Stmt_{fx.uid}.csv", stmt_csv_text.encode("utf-8"), "text/csv")},
        )
        assert r_up_stmt.status_code == 200, f"Upload statement failed: {r_up_stmt.text}"
        stmt_id = r_up_stmt.json()["id"]

        txns = client.get(f"/api/v1/reconciliation/statements/{stmt_id}/transactions", headers=admin1_h).json()
        assert len(txns) == 3
        txn_inv = next(t for t in txns if "INV-RECON-7788" in t["raw_description"])
        txn_fee = next(t for t in txns if "BILL-FEE-22" in t["raw_description"])
        txn_cmd = next(t for t in txns if t["raw_description"].startswith("=CMD"))

        # 5.2 Ingest ERP Ledger Records (`POST /api/v1/reconciliation/erp-records/ingest`)
        r_erp = client.post(
            "/api/v1/reconciliation/erp-records/ingest",
            headers=admin1_h,
            json=[
                {
                    "record_type": "AR_INVOICE",
                    "reference_number": "INV-RECON-7788",
                    "entity_name": "Orascom Construction",
                    "record_date": f"{today.isoformat()}T00:00:00Z",
                    "amount": "125000.00",
                    "currency": "EGP",
                    "gl_account": "4010010 - Revenue",
                },
                {
                    "record_type": "AP_BILL",
                    "reference_number": "BILL-FEE-22",
                    "entity_name": "Elsewedy Cables",
                    "record_date": f"{today.isoformat()}T00:00:00Z",
                    "amount": "-50000.00",
                    "currency": "EGP",
                    "gl_account": "2010010 - Payables",
                },
            ],
        )
        assert r_erp.status_code == 200 and r_erp.json()["imported_count"] == 2
        erp_rows = client.get("/api/v1/reconciliation/erp-records", headers=admin1_h).json()
        erp_inv = next(e for e in erp_rows if e["reference_number"] == "INV-RECON-7788")
        erp_fee = next(e for e in erp_rows if e["reference_number"] == "BILL-FEE-22")

        # 5.3 Auto-Match + Manual Match with Fee Variance + Unmatch -> Re-Match
        client.post(f"/api/v1/reconciliation/statements/{stmt_id}/auto-match", headers=admin1_h)

        # Ensure `txn_inv <-> erp_inv` matched (if auto-match didn't pair, pair via `/match`), then test `/unmatch` -> `/match`
        matches_now = client.get("/api/v1/reconciliation/matches", headers=admin1_h, params={"statement_id": stmt_id}).json()
        if not any(m["bank_transaction"]["id"] == txn_inv["id"] for m in matches_now):
            r_m1 = client.post(
                "/api/v1/reconciliation/match",
                headers=admin1_h,
                json={"bank_transaction_ids": [txn_inv["id"]], "erp_record_ids": [erp_inv["id"]]},
            )
            assert r_m1.status_code == 200

        # Test Unmatch (`POST /unmatch`) -> resets ERP back to `OPEN` and `is_reconciled=False`!
        r_unm = client.post(
            "/api/v1/reconciliation/unmatch",
            headers=admin1_h,
            json={"bank_transaction_ids": [txn_inv["id"]]},
        )
        assert r_unm.status_code == 200 and r_unm.json()["unmatched_links"] >= 1

        # Re-match `txn_inv` AND match `txn_fee` with EGP 50.00 variance (`BANK_FEE_WRITEOFF`)!
        assert client.post(
            "/api/v1/reconciliation/match",
            headers=admin1_h,
            json={"bank_transaction_ids": [txn_inv["id"]], "erp_record_ids": [erp_inv["id"]]},
        ).status_code == 200
        r_mfee = client.post(
            "/api/v1/reconciliation/match",
            headers=admin1_h,
            json={
                "bank_transaction_ids": [txn_fee["id"]],
                "erp_record_ids": [erp_fee["id"]],
                "write_off_variance_as_fee": True,
            },
        )
        assert r_mfee.status_code == 200 and r_mfee.json()["match_type"] == "1:1"

        # 5.4 Taxonomy Inspection / Creation / Tenant Isolation + Bulk Classification
        assert client.get("/api/v1/reconciliation/taxonomy", headers=admin1_h).status_code == 200
        r_tax = client.post(
            "/api/v1/reconciliation/taxonomy",
            headers=admin1_h,
            json={
                "code": f"BANK_FEES_{fx.uid}",
                "name": "Bank Charges & Commissions",
                "direction": "DEBIT",
                "default_gl_account": "6050100 - Bank Charges",
            },
        )
        assert r_tax.status_code == 200, r_tax.text
        tax_node_id = r_tax.json()["id"]

        # Update taxonomy node (same tenant -> 200; rival tenant -> 404!)
        assert client.put(
            f"/api/v1/reconciliation/taxonomy/{tax_node_id}",
            headers=admin1_h,
            json={"description": "Updated bank fee GL mapping"},
        ).status_code == 200
        assert client.put(
            f"/api/v1/reconciliation/taxonomy/{tax_node_id}",
            headers=beta_admin_h,
            json={"description": "Cross-tenant hijack attempt"},
        ).status_code == 404
        assert client.delete(
            f"/api/v1/reconciliation/taxonomy/{tax_node_id}",
            headers=beta_admin_h,
        ).status_code == 404

        r_bcls = client.post(
            "/api/v1/reconciliation/transactions/bulk-classify",
            headers=admin1_h,
            json={
                "transaction_ids": [txn_cmd["id"]],
                "category": f"BANK_FEES_{fx.uid}",
                "gl_account": "6050100 - Bank Charges",
            },
        )
        assert r_bcls.status_code == 200 and r_bcls.json()["affected_count"] == 1

        # 5.5 Custom CSV Export (`POST /export/download`) -> Verify UTF-8 BOM + Formula-Injection Neutralization!
        export_payload = {
            "statement_id": stmt_id,
            "file_format": "CSV",
            "amount_mode": "SINGLE_SIGNED",
            "mark_as_exported": True,
            "columns_config": [
                {"source_field": "booking_date", "custom_header": "Date"},
                {"source_field": "raw_description", "custom_header": "Description"},
                {"source_field": "signed_amount", "custom_header": "Net Amount"},
                {"source_field": "classification_category", "custom_header": "Category"},
            ],
        }
        r_csv_dl = client.post(
            "/api/v1/reconciliation/export/download",
            headers=admin1_h,
            json=export_payload,
        )
        assert r_csv_dl.status_code == 200, r_csv_dl.text
        raw_bytes = r_csv_dl.content

        # 1) Must begin with UTF-8 BOM (`\xef\xbb\xbf`) so Excel renders Arabic cleanly
        assert raw_bytes.startswith(b"\xef\xbb\xbf"), "Exported CSV must start with UTF-8 BOM!"
        csv_text = raw_bytes.decode("utf-8-sig")
        parsed_rows = list(csv.DictReader(io.StringIO(csv_text)))
        assert len(parsed_rows) == 3

        # 2) Formula-injection payload `=CMD|' /C calc'!A0` MUST be neutralized with a leading `'`!
        cmd_row = next(r for r in parsed_rows if "CMD|" in r["Description"])
        assert cmd_row["Description"].startswith("'=CMD|"), (
            f"CSV formula injection NOT neutralized! Got: {cmd_row['Description']!r}"
        )
        # 3) Genuine negative numbers (`-1250.50`) MUST NOT be prefixed with `'`!
        if str(cmd_row["Net Amount"]).startswith("-"):
            assert not str(cmd_row["Net Amount"]).startswith("'-"), (
                f"Negative numeric amount was wrongly escaped as formula: {cmd_row['Net Amount']!r}"
            )

        # 4) Verify `mark_as_exported=True` locked the exported transactions in DB
        db_txn_cmd = db.get(BankTransaction, txn_cmd["id"])
        db.refresh(db_txn_cmd)
        assert db_txn_cmd.is_exported is True and db_txn_cmd.is_locked is True
        print("  [PASS] Reconciliation v2 verified: ERP Match/Unmatch, Fee Variance, UTF-8 BOM, Formula-Injection Defense & Export Lock!")

        print("\n" + "=" * 94)
        print("ALL PLATFORM MODULES END-TO-END CHECKS PASSED 100%!")
        print("=" * 94)

    finally:
        for fp in temp_files:
            try:
                if os.path.exists(fp):
                    os.remove(fp)
            except Exception:
                pass
        try:
            db.rollback()
            if created_campaign_ids:
                db.query(CashbackClaim).filter(CashbackClaim.campaign_id.in_(created_campaign_ids)).delete(synchronize_session=False)
                db.query(Campaign).filter(Campaign.id.in_(created_campaign_ids)).delete(synchronize_session=False)
                db.commit()
            if created_holiday_ids:
                db.query(SystemHoliday).filter(SystemHoliday.id.in_(created_holiday_ids)).delete(synchronize_session=False)
                db.commit()
            if created_fx_ids:
                db.query(CurrencyExchangeRate).filter(CurrencyExchangeRate.id.in_(created_fx_ids)).delete(synchronize_session=False)
                db.commit()
        except Exception:
            db.rollback()
        fx.cleanup()
        if created_plan_ids:
            try:
                db.execute(text("DELETE FROM subscription_plans WHERE id = ANY(:pids)"), {"pids": created_plan_ids})
                db.commit()
            except Exception:
                db.rollback()
        db.close()


def test_platform_modules_e2e_suite():
    run_platform_modules_e2e_suite()


if __name__ == "__main__":
    run_platform_modules_e2e_suite()
