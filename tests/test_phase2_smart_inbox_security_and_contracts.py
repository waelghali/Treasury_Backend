# tests/test_phase2_smart_inbox_security_and_contracts.py
"""
Phase 2 Verification Suite: Smart Inbox Security, RBAC, Account Matching,
Classification Guardrails, Scheduler Isolation, and Frontend/Backend Contracts.
"""

import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

import fitz
from fastapi.testclient import TestClient

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
for p in (TESTS_DIR, ROOT_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

from _test_harness import (  # noqa: E402
    IsolatedTenantFixture,
    install_email_capture,
    install_in_memory_gcs,
    make_auth_headers,
    purge_test_customers_by_ids,
)
from app.constants import SubscriptionStatus, UserRole  # noqa: E402
from app.core.encryption import decrypt_data, encrypt_data  # noqa: E402
from app.core.hashing import get_password_hash  # noqa: E402
from app.core.storage_service import build_customer_blob_path, upload_bytes_to_gcs_sync  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.main import SensitiveDataFilter, app  # noqa: E402
from app.models.models import Customer, SubscriptionPlan, User  # noqa: E402
from app.models.models_inbox import (  # noqa: E402
    EmailClassificationHistory,
    InboxAttachment,
    InboxItem,
    InboxOutboundRequest,
    InboxScheduleConfig,
)
from app.models.models_issuance import CustomerBankAccount  # noqa: E402
from app.services.inbox_action_service import inbox_action_service  # noqa: E402
from app.services.inbox_classification_service import inbox_classification_service  # noqa: E402
from app.services.inbox_outbound_service import inbox_outbound_service  # noqa: E402


def _make_encrypted_summary_pdf(password: str, text_lines: list[str]) -> bytes:
    doc = fitz.open()
    page = doc.new_page()
    y = 72
    for line in text_lines:
        page.insert_text((72, y), line, fontsize=11)
        y += 18
    pdf_bytes = doc.tobytes(
        encryption=fitz.PDF_ENCRYPT_AES_256,
        owner_pw=f"owner_{password}",
        user_pw=password,
        permissions=fitz.PDF_PERM_PRINT,
    )
    doc.close()
    return pdf_bytes


def run_all_tests():
    mem_gcs = install_in_memory_gcs()
    install_email_capture()
    client = TestClient(app)

    db = SessionLocal()
    fix = IsolatedTenantFixture(db, tag="P2_INBOX")
    orig_cib_domain = fix.bank_cib.email_domain
    extra_plan_ids = []
    extra_cust_ids = []

    try:
        # Create a VIEWER user under Tenant Alpha
        viewer_user = User(
            email=f"viewer.{fix.uid.lower()}@globex.com",
            password_hash=get_password_hash("StrongPass!2026"),
            role=UserRole.VIEWER,
            customer_id=fix.cust_alpha.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        db.add(viewer_user)
        db.commit()
        db.refresh(viewer_user)

        admin_hdrs = make_auth_headers(fix.admin_user)
        maker_hdrs = make_auth_headers(fix.maker_user)
        viewer_hdrs = make_auth_headers(viewer_user)
        beta_hdrs = make_auth_headers(fix.beta_admin)
        sys_hdrs = make_auth_headers(fix.sys_owner)

        # ------------------------------------------------------------------
        # 1. PUT /banks/{bank_id}/domain restricted strictly to SYSTEM_OWNER
        # ------------------------------------------------------------------
        print("[1/12] Testing PUT /inbox/banks/{id}/domain System Owner restriction...")
        assert client.put(
            f"/api/v1/inbox/banks/{fix.bank_cib.id}/domain",
            headers=admin_hdrs,
            json={"email_domain": "evil.example.com"},
        ).status_code == 403
        assert client.put(
            f"/api/v1/inbox/banks/{fix.bank_cib.id}/domain",
            headers=maker_hdrs,
            json={"email_domain": "evil.example.com"},
        ).status_code == 403
        r_sys_dom = client.put(
            f"/api/v1/inbox/banks/{fix.bank_cib.id}/domain",
            headers=sys_hdrs,
            json={"email_domain": "cibeg.com"},
        )
        assert r_sys_dom.status_code == 200, r_sys_dom.text
        assert r_sys_dom.json()["email_domain"] == "cibeg.com"

        # Seed a baseline classified inbox item + attachment for Alpha
        sample_csv = b"Date,Description,Debit,Credit,Balance\n2026-11-18,Wire,0,5000,15000\n"
        blob_path = build_customer_blob_path(fix.cust_alpha.id, "inbox", f"p2_seed_{fix.uid}.csv")
        uri = upload_bytes_to_gcs_sync(blob_path, sample_csv, content_type="text/csv")
        assert uri in f"gs://lg_custody_bucket/{blob_path}"

        seed_item = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"seed-{fix.uid}@cibeg.com",
            sender_email="statements@cibeg.com",
            sender_domain="cibeg.com",
            subject="Daily Balance",
            matched_bank_id=fix.bank_cib.id,
            is_trusted_sender=True,
            classification="CASH_POSITION_SNAPSHOT",
            classification_confidence="HIGH",
            confidence_score=9,
            has_attachment=True,
            attachment_count=1,
            primary_attachment_path=uri,
            primary_attachment_name=f"seed_{fix.uid}.csv",
            primary_attachment_type="CSV",
            extracted_balances_json=[
                {"account_number": "100111222", "currency": "EGP", "available_balance": 10000.0}
            ],
            received_at=datetime.now(timezone.utc),
            status="CLASSIFIED",
        )
        db.add(seed_item)
        db.flush()
        seed_att = InboxAttachment(
            inbox_item_id=seed_item.id,
            file_name=seed_item.primary_attachment_name,
            file_type="CSV",
            file_size_bytes=len(sample_csv),
            storage_path=uri,
            is_primary=True,
        )
        db.add(seed_att)
        db.commit()
        db.refresh(seed_item)
        db.refresh(seed_att)

        # ------------------------------------------------------------------
        # 2. VIEWER role: reads permitted, ALL mutations rejected with 403
        # ------------------------------------------------------------------
        print("[2/12] Testing VIEWER read-only vs write enforcement & SYSTEM_OWNER guard...")
        assert client.get("/api/v1/inbox/items", headers=viewer_hdrs).status_code == 200
        assert client.get("/api/v1/inbox/banks", headers=viewer_hdrs).status_code == 200
        assert client.get("/api/v1/inbox/stats", headers=viewer_hdrs).status_code == 200
        assert client.get("/api/v1/inbox/cash-positions", headers=viewer_hdrs).status_code == 200

        assert client.put(
            f"/api/v1/inbox/items/{seed_item.id}/reclassify",
            headers=viewer_hdrs,
            json={"classification": "BANK_STATEMENT"},
        ).status_code == 403
        assert client.post(
            f"/api/v1/inbox/items/{seed_item.id}/confirm",
            headers=viewer_hdrs,
            json={},
        ).status_code == 403
        assert client.post(
            f"/api/v1/inbox/items/{seed_item.id}/archive",
            headers=viewer_hdrs,
        ).status_code == 403
        assert client.post(
            f"/api/v1/inbox/items/{seed_item.id}/unarchive",
            headers=viewer_hdrs,
        ).status_code == 403
        assert client.post(
            "/api/v1/inbox/sync-tokens",
            headers=viewer_hdrs,
            json={"label": "Viewer Token"},
        ).status_code == 403
        assert client.delete("/api/v1/inbox/sync-tokens/999999", headers=viewer_hdrs).status_code == 403
        assert client.get("/api/v1/inbox/sync-watcher-script", headers=viewer_hdrs).status_code == 403
        assert client.post(
            "/api/v1/inbox/sync-watcher-script",
            headers=viewer_hdrs,
            json={"token_hint": "abc"},
        ).status_code == 403
        assert client.post(
            "/api/v1/inbox/sync-drop",
            headers=viewer_hdrs,
            files={"file": ("cib_viewer.csv", sample_csv, "text/csv")},
        ).status_code == 403

        # SYSTEM_OWNER has customer_id=None -> cleanly blocked (403, never 500) on tenant sync-tokens
        assert client.post(
            "/api/v1/inbox/sync-tokens",
            headers=sys_hdrs,
            json={"label": "SysOwner Token"},
        ).status_code == 403

        # Create a valid sync token & schedule config for subsequent GRACE / plan tests
        r_tok = client.post("/api/v1/inbox/sync-tokens", headers=maker_hdrs, json={"label": "Phase2 PC"})
        assert r_tok.status_code == 200, r_tok.text
        sync_tok_id = r_tok.json()["id"]
        raw_sync_tok = r_tok.json()["plaintext_token"]

        r_sched = client.post(
            "/api/v1/inbox/schedule-configs",
            headers=admin_hdrs,
            json={
                "bank_id": fix.bank_cib.id,
                "request_type": "CASH_POSITION",
                "is_active": True,
                "frequency": "DAILY",
            },
        )
        assert r_sched.status_code == 200, r_sched.text
        sched_id = r_sched.json()["id"]

        # ------------------------------------------------------------------
        # 3. GRACE status & can_email_inbox=False enforcement + GET /banks
        # ------------------------------------------------------------------
        print("[3/12] Testing GRACE read-only, plan gates, and GET /inbox/banks without Issuance module...")
        fix.cust_alpha.status = SubscriptionStatus.GRACE
        db.commit()

        assert client.get("/api/v1/inbox/items", headers=admin_hdrs).status_code == 200
        assert client.post(
            "/api/v1/inbox/schedule-configs",
            headers=admin_hdrs,
            json={"bank_id": fix.bank_hsbc.id, "request_type": "LG_POSITION", "frequency": "DAILY"},
        ).status_code == 403
        assert client.post(
            "/api/v1/inbox/schedule-configs/bulk",
            headers=admin_hdrs,
            json={"bank_ids": [fix.bank_hsbc.id], "request_type": "LG_POSITION", "frequency": "DAILY"},
        ).status_code == 403
        assert client.delete(f"/api/v1/inbox/schedule-configs/{sched_id}", headers=admin_hdrs).status_code == 403
        assert client.post(f"/api/v1/inbox/items/{seed_item.id}/archive", headers=maker_hdrs).status_code == 403
        assert client.post(
            "/api/v1/inbox/sync-tokens",
            headers=maker_hdrs,
            json={"label": "Grace Token"},
        ).status_code == 403
        assert client.post(
            "/api/v1/inbox/sync-drop",
            headers=maker_hdrs,
            files={"file": ("cib_grace_jwt.csv", sample_csv, "text/csv")},
        ).status_code == 403
        assert client.post(
            "/api/v1/inbox/sync-drop",
            headers={"X-Grow-Sync-Token": raw_sync_tok},
            files={"file": ("cib_grace_tok.csv", sample_csv, "text/csv")},
        ).status_code == 403

        # Restore ACTIVE, toggle can_email_inbox=False
        fix.cust_alpha.status = SubscriptionStatus.ACTIVE
        fix.plan.can_email_inbox = False
        db.commit()

        assert client.get(
            f"/api/v1/inbox/items/{seed_item.id}/attachment/{seed_att.id}",
            headers=admin_hdrs,
        ).status_code == 403
        assert client.get(
            f"/api/v1/inbox/items/{seed_item.id}/attachment/{seed_att.id}/preview",
            headers=admin_hdrs,
        ).status_code == 403
        assert client.get("/api/v1/inbox/banks-summary", headers=admin_hdrs).status_code == 403
        assert client.get("/api/v1/inbox/schedule-configs", headers=admin_hdrs).status_code == 403
        assert client.post(
            "/api/v1/inbox/sync-drop",
            headers={"X-Grow-Sync-Token": raw_sync_tok},
            files={"file": ("cib_no_plan.csv", sample_csv, "text/csv")},
        ).status_code == 403

        # Restore can_email_inbox=True, toggle has_issuance_module=False -> GET /inbox/banks STILL works
        fix.plan.can_email_inbox = True
        fix.plan.has_issuance_module = False
        db.commit()
        r_banks = client.get("/api/v1/inbox/banks", headers=maker_hdrs)
        assert r_banks.status_code == 200, r_banks.text
        assert any(b["id"] == fix.bank_cib.id for b in r_banks.json())
        fix.plan.has_issuance_module = True
        db.commit()

        # ------------------------------------------------------------------
        # 4. Reclassify tenant isolation, validation & unarchive state integrity
        # ------------------------------------------------------------------
        print("[4/12] Testing reclassify isolation, Literal validation, ACTIONED guard, and unarchive...")
        assert client.put(
            f"/api/v1/inbox/items/{seed_item.id}/reclassify",
            headers=beta_hdrs,
            json={"classification": "BANK_STATEMENT"},
        ).status_code == 404
        assert client.put(
            f"/api/v1/inbox/items/{seed_item.id}/reclassify",
            headers=maker_hdrs,
            json={"classification": "BOGUS_CATEGORY"},
        ).status_code == 422
        assert client.post(
            f"/api/v1/inbox/items/{seed_item.id}/unarchive",
            headers=maker_hdrs,
        ).status_code == 400

        # Confirm item -> becomes ACTIONED; reclassify must fail 400
        # First test invalid override_balances rejected with 422
        for bad_bal in [
            [{"currency": "EGP", "available_balance": "NaN"}],
            [{"currency": "EGP", "available_balance": "Infinity"}],
            [{"currency": "EGP", "available_balance": "1,234.50"}],
            [{"currency": "EGP", "available_balance": ""}],
            [{"currency": "EGP", "available_balance": True}],
            [{"currency": "EGP"}],
        ]:
            r_bad = client.post(
                f"/api/v1/inbox/items/{seed_item.id}/confirm",
                headers=maker_hdrs,
                json={"override_balances": bad_bal},
            )
            assert r_bad.status_code == 422, f"Expected 422 for {bad_bal}, got {r_bad.status_code}: {r_bad.text}"

        r_ok_conf = client.post(
            f"/api/v1/inbox/items/{seed_item.id}/confirm",
            headers=maker_hdrs,
            json={"override_balances": [{"account_number": "100111222", "currency": "EGP", "available_balance": "12345.67"}]},
        )
        assert r_ok_conf.status_code == 200, r_ok_conf.text

        assert client.put(
            f"/api/v1/inbox/items/{seed_item.id}/reclassify",
            headers=maker_hdrs,
            json={"classification": "BANK_STATEMENT"},
        ).status_code == 400

        # Archive then unarchive an ACTIONED item -> restores to ACTIONED (not CLASSIFIED!)
        assert client.post(f"/api/v1/inbox/items/{seed_item.id}/archive", headers=maker_hdrs).status_code == 200
        assert client.post(f"/api/v1/inbox/items/{seed_item.id}/unarchive", headers=maker_hdrs).status_code == 200
        db.refresh(seed_item)
        assert seed_item.status == "ACTIONED"

        # record_cash_balance_snapshot string truncation check (>64 account, >40 source_channel)
        long_snap = inbox_action_service.record_cash_balance_snapshot(
            db=db,
            customer_id=fix.cust_alpha.id,
            bank_id=fix.bank_cib.id,
            available_balance=Decimal("99.50"),
            currency_code="USDX",
            account_number="A" * 85,
            source_channel="SOURCE_" + ("X" * 60),
            continuity_status="CONT_" + ("Y" * 50),
            auto_commit=True,
        )
        assert len(long_snap.account_number) == 64
        assert long_snap.currency_code == "USD"
        assert len(long_snap.source_channel) == 40
        assert len(long_snap.continuity_status) == 40

        # ------------------------------------------------------------------
        # 5. _match_customer_bank_account precision & currency filtering
        # ------------------------------------------------------------------
        print("[5/12] Testing _match_customer_bank_account currency & suffix rules...")
        acct_egp = CustomerBankAccount(
            customer_id=fix.cust_alpha.id,
            bank_id=fix.bank_hsbc.id,
            account_name="HSBC EGP Default",
            account_number="1110001234",
            currency_code="EGP",
            is_default=True,
            is_active=True,
        )
        acct_egp2 = CustomerBankAccount(
            customer_id=fix.cust_alpha.id,
            bank_id=fix.bank_hsbc.id,
            account_name="HSBC EGP Ops",
            account_number="2220001234",
            currency_code="EGP",
            is_default=False,
            is_active=True,
        )
        acct_usd = CustomerBankAccount(
            customer_id=fix.cust_alpha.id,
            bank_id=fix.bank_hsbc.id,
            account_name="HSBC USD Treasury",
            account_number="3339995678",
            currency_code="USD",
            is_default=False,
            is_active=True,
        )
        db.add_all([acct_egp, acct_egp2, acct_usd])
        db.commit()

        assert inbox_action_service._match_customer_bank_account(
            db, fix.cust_alpha.id, fix.bank_hsbc.id, account_number="999888777"
        ) is None, "Explicit non-matching account must return None"
        assert inbox_action_service._match_customer_bank_account(
            db, fix.cust_alpha.id, fix.bank_hsbc.id, account_number="****1234"
        ) is None, "Ambiguous 4-digit tail must return None"
        m_tail6 = inbox_action_service._match_customer_bank_account(
            db, fix.cust_alpha.id, fix.bank_hsbc.id, account_number="******995678"
        )
        assert m_tail6 and m_tail6.id == acct_usd.id
        m_usd = inbox_action_service._match_customer_bank_account(
            db, fix.cust_alpha.id, fix.bank_hsbc.id, account_number=None, currency_code="USD"
        )
        assert m_usd and m_usd.id == acct_usd.id

        # ------------------------------------------------------------------
        # 6. Encrypted summary-only PDF save_pdf_password + GET /items flags
        # ------------------------------------------------------------------
        print("[6/12] Testing encrypted PDF save_pdf_password deferral & GET /items flags...")
        pdf_pw = "SecRet2026!"
        enc_summary_pdf = _make_encrypted_summary_pdf(
            pdf_pw,
            [
                "HSBC Corporate Cash Summary",
                "Account Number: 3339995678",
                "Available Balance: USD 88,500.25",
                "Closing Balance: USD 88,500.25",
            ],
        )
        pdf_blob = build_customer_blob_path(fix.cust_alpha.id, "inbox", f"hsbc_summary_{fix.uid}.pdf")
        pdf_uri = upload_bytes_to_gcs_sync(pdf_blob, enc_summary_pdf, content_type="application/pdf")

        locked_item = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"locked-{fix.uid}@hsbc.com",
            sender_email="statements@hsbc.com",
            sender_domain="hsbc.com",
            subject="Your HSBC e-Statement Summary",
            matched_bank_id=fix.bank_hsbc.id,
            is_trusted_sender=True,
            classification="BANK_STATEMENT",
            classification_confidence="HIGH",
            classification_signals={"pdf_password_required": {"points": 3}},
            confidence_score=7,
            has_attachment=True,
            attachment_count=1,
            primary_attachment_path=pdf_uri,
            primary_attachment_name=f"hsbc_summary_{fix.uid}.pdf",
            primary_attachment_type="PDF",
            received_at=datetime.now(timezone.utc),
            status="CLASSIFIED",
        )
        db.add(locked_item)
        db.commit()
        db.refresh(locked_item)

        r_items = client.get("/api/v1/inbox/items", headers=maker_hdrs)
        assert r_items.status_code == 200
        match_row = next(i for i in r_items.json() if i["id"] == locked_item.id)
        assert match_row["primary_attachment_type"] == "PDF"
        assert match_row["pdf_password_required"] is True

        # Wrong password -> 400 PDF_PASSWORD_REQUIRED, password NOT saved
        r_wrong = client.post(
            f"/api/v1/inbox/items/{locked_item.id}/confirm",
            headers=maker_hdrs,
            json={"pdf_password": "wrong", "save_pdf_password": True},
        )
        assert r_wrong.status_code == 400
        assert "PDF_PASSWORD_REQUIRED" in r_wrong.text
        db.refresh(acct_usd)
        db.refresh(acct_egp)
        assert acct_usd.statement_pdf_password_encrypted is None
        assert acct_egp.statement_pdf_password_encrypted is None

        # Correct password -> BANK_STATEMENT falls back to summary snapshot & saves password on USD account!
        r_right = client.post(
            f"/api/v1/inbox/items/{locked_item.id}/confirm",
            headers=maker_hdrs,
            json={"pdf_password": pdf_pw, "save_pdf_password": True},
        )
        assert r_right.status_code == 200, r_right.text
        db.refresh(acct_usd)
        db.refresh(acct_egp)
        assert acct_usd.statement_pdf_password_encrypted is not None
        assert decrypt_data(acct_usd.statement_pdf_password_encrypted) == pdf_pw
        assert acct_egp.statement_pdf_password_encrypted is None

        # Also verify direct CASH_POSITION_SNAPSHOT save_pdf_password=True
        acct_usd.statement_pdf_password_encrypted = None
        db.commit()
        cash_pdf_item = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"cashpdf-{fix.uid}@hsbc.com",
            sender_email="statements@hsbc.com",
            sender_domain="hsbc.com",
            subject="Daily Cash Summary PDF",
            matched_bank_id=fix.bank_hsbc.id,
            is_trusted_sender=True,
            classification="CASH_POSITION_SNAPSHOT",
            classification_confidence="HIGH",
            confidence_score=7,
            has_attachment=True,
            attachment_count=1,
            primary_attachment_path=pdf_uri,
            primary_attachment_name=f"hsbc_cash_{fix.uid}.pdf",
            primary_attachment_type="PDF",
            received_at=datetime.now(timezone.utc),
            status="CLASSIFIED",
        )
        db.add(cash_pdf_item)
        db.commit()
        r_cash_pw = client.post(
            f"/api/v1/inbox/items/{cash_pdf_item.id}/confirm",
            headers=maker_hdrs,
            json={"pdf_password": pdf_pw, "save_pdf_password": True},
        )
        assert r_cash_pw.status_code == 200, r_cash_pw.text
        db.refresh(acct_usd)
        assert decrypt_data(acct_usd.statement_pdf_password_encrypted) == pdf_pw

        # ------------------------------------------------------------------
        # 7. try_auto_execute_item keeps CLASSIFIED on parse error
        # ------------------------------------------------------------------
        print("[7/12] Testing try_auto_execute_item does not commit PARSE_ERROR on failure...")
        acct_usd.auto_ingest_statements = True
        db.commit()

        bad_csv_blob = build_customer_blob_path(fix.cust_alpha.id, "inbox", f"bad_auto_{fix.uid}.csv")
        bad_csv_uri = upload_bytes_to_gcs_sync(bad_csv_blob, b"not,a,valid\ncsv", content_type="text/csv")
        auto_bad_item = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"autobad-{fix.uid}@hsbc.com",
            sender_email="statements@hsbc.com",
            sender_domain="hsbc.com",
            subject="Statement",
            matched_bank_id=fix.bank_hsbc.id,
            is_trusted_sender=True,
            classification="BANK_STATEMENT",
            classification_confidence="HIGH",
            confidence_score=8,
            has_attachment=True,
            attachment_count=1,
            primary_attachment_path=bad_csv_uri,
            primary_attachment_name=f"bad_auto_{fix.uid}.csv",
            primary_attachment_type="CSV",
            received_at=datetime.now(timezone.utc),
            status="CLASSIFIED",
        )
        db.add(auto_bad_item)
        db.commit()

        assert inbox_action_service.try_auto_execute_item(db, auto_bad_item) is False
        db.expire_all()
        reloaded_auto = db.query(InboxItem).filter(InboxItem.id == auto_bad_item.id).first()
        assert reloaded_auto.status == "CLASSIFIED", f"Expected CLASSIFIED, got {reloaded_auto.status}"

        # ------------------------------------------------------------------
        # 8. Classification guardrails: NIL_POSITION vs inline zero + null domain
        # ------------------------------------------------------------------
        print("[8/12] Testing classification NIL guard and Signal 7 null sender_domain...")
        zero_cash_item = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"zerobal-{fix.uid}@cibeg.com",
            sender_email="treasury@cibeg.com",
            sender_domain="cibeg.com",
            subject="Daily Cash Balance",
            body_text_encrypted=encrypt_data("Available Balance: EGP 250,000.00\nEUR account zero balance"),
            has_attachment=False,
            received_at=datetime.now(timezone.utc),
            status="RECEIVED",
        )
        db.add(zero_cash_item)
        db.flush()
        c_zero = inbox_classification_service.classify(db, zero_cash_item)
        assert c_zero.classification == "CASH_POSITION_SNAPSHOT"
        assert bool(c_zero.is_nil_position) is False

        # Signal 7 with sender_domain=None must NOT match unrelated sender_domain=None history row
        null_hist = EmailClassificationHistory(
            customer_id=fix.cust_alpha.id,
            sender_email="other-person@example.org",
            sender_domain=None,
            classification="PROGRESS_REPORT",
            was_user_corrected=False,
            original_classification="PROGRESS_REPORT",
            confidence_score=5,
            confirmed_by_user_id=fix.maker_user.id,
        )
        null_item = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"nulldom-{fix.uid}@local",
            sender_email="unrelated-sender@local",
            sender_domain=None,
            subject="General note",
            has_attachment=False,
            received_at=datetime.now(timezone.utc),
            status="RECEIVED",
        )
        db.add_all([null_hist, null_item])
        db.flush()
        c_null = inbox_classification_service.classify(db, null_item)
        assert "history_match" not in (c_null.classification_signals or {})

        # ------------------------------------------------------------------
        # 9. send_scheduled_requests tenant gate & per-request commit isolation
        # ------------------------------------------------------------------
        print("[9/12] Testing send_scheduled_requests tenant filtering and tx isolation...")
        no_inbox_plan = SubscriptionPlan(
            name=f"NoInboxPlan {fix.uid}",
            duration_months=12,
            monthly_price=100.0,
            annual_price=1000.0,
            max_users=5,
            max_checker_users=1,
            max_records=10,
            max_issuance_records=10,
            can_email_inbox=False,
        )
        db.add(no_inbox_plan)
        db.flush()
        extra_plan_ids.append(no_inbox_plan.id)

        grace_cust = Customer(
            name=f"Grace Scheduled Corp {fix.uid}",
            address="Cairo",
            contact_email=f"grace.sched.{fix.uid.lower()}@globex.com",
            subscription_plan_id=fix.plan.id,
            start_date=datetime.now(timezone.utc),
            end_date=datetime.now(timezone.utc),
            status=SubscriptionStatus.GRACE,
        )
        db.add(grace_cust)
        db.flush()
        extra_cust_ids.append(grace_cust.id)

        fix.cust_beta.subscription_plan_id = no_inbox_plan.id
        cfg_alpha_ok = (
            db.query(InboxScheduleConfig)
            .filter(InboxScheduleConfig.id == sched_id)
            .first()
        )
        cfg_alpha_ok.last_sent_at = None
        cfg_alpha_fail = InboxScheduleConfig(
            customer_id=fix.cust_alpha.id,
            bank_id=fix.bank_hsbc.id,
            request_type="BANK_STATEMENT",
            is_active=True,
            frequency="DAILY",
        )
        cfg_beta_no_plan = InboxScheduleConfig(
            customer_id=fix.cust_beta.id,
            bank_id=fix.bank_cib.id,
            request_type="LG_POSITION",
            is_active=True,
            frequency="DAILY",
        )
        cfg_grace = InboxScheduleConfig(
            customer_id=grace_cust.id,
            bank_id=fix.bank_cib.id,
            request_type="LG_POSITION",
            is_active=True,
            frequency="DAILY",
        )
        db.add_all([cfg_alpha_fail, cfg_beta_no_plan, cfg_grace])
        db.commit()

        orig_send = inbox_outbound_service.send_position_request
        called_pairs = []

        def _mock_send_position_request(
            db,
            customer_id,
            bank_id,
            position_date,
            **kwargs,
        ):
            called_pairs.append((customer_id, bank_id))
            if bank_id == fix.bank_hsbc.id:
                req_partial = InboxOutboundRequest(
                    customer_id=customer_id,
                    bank_id=bank_id,
                    request_type="BANK_STATEMENT",
                    sent_to_emails=["rm@hsbc.com"],
                    subject="Will Rollback",
                    sent_at=datetime.now(timezone.utc),
                )
                db.add(req_partial)
                db.flush()
                raise RuntimeError("Simulated SMTP outage")
            req_ok = InboxOutboundRequest(
                customer_id=customer_id,
                bank_id=bank_id,
                request_type="CASH_POSITION",
                sent_to_emails=["rm@cibeg.com"],
                subject=f"OK_{fix.uid}",
                sent_at=datetime.now(timezone.utc),
            )
            db.add(req_ok)
            db.flush()
            return req_ok

        inbox_outbound_service.send_position_request = _mock_send_position_request
        try:
            sent_list = inbox_outbound_service.send_scheduled_requests(db)
        finally:
            inbox_outbound_service.send_position_request = orig_send

        assert all(cid == fix.cust_alpha.id for cid, _ in called_pairs), f"Unexpected tenants: {called_pairs}"
        assert len(sent_list) == 1 and sent_list[0].bank_id == fix.bank_cib.id
        assert (
            db.query(InboxOutboundRequest)
            .filter(
                InboxOutboundRequest.customer_id == fix.cust_alpha.id,
                InboxOutboundRequest.bank_id == fix.bank_hsbc.id,
                InboxOutboundRequest.subject == "Will Rollback",
            )
            .count()
            == 0
        ), "Failed request must roll back cleanly without leaking"
        assert (
            db.query(InboxOutboundRequest)
            .filter(
                InboxOutboundRequest.customer_id == fix.cust_alpha.id,
                InboxOutboundRequest.bank_id == fix.bank_cib.id,
                InboxOutboundRequest.subject == f"OK_{fix.uid}",
            )
            .count()
            == 1
        )

        # Restore beta plan before remaining tests
        fix.cust_beta.subscription_plan_id = fix.plan.id
        db.commit()

        # ------------------------------------------------------------------
        # 10. POST /sync-drop cap (413), basename sanitization, rollback recovery
        # ------------------------------------------------------------------
        print("[10/12] Testing POST /sync-drop size cap, filename basename, and error recovery...")
        oversized = b"X" * (20 * 1024 * 1024 + 64)
        r_big = client.post(
            "/api/v1/inbox/sync-drop",
            headers={"X-Grow-Sync-Token": raw_sync_tok},
            files={"file": ("huge.xlsx", oversized, "application/octet-stream")},
        )
        assert r_big.status_code == 413, f"Expected 413, got {r_big.status_code}: {r_big.text}"

        # Path traversal filename sanitized to basename + failed auto_execute recovers cleanly (200)
        trav_bytes = f"bad-content-{fix.uid}".encode("utf-8")
        r_trav = client.post(
            "/api/v1/inbox/sync-drop",
            headers={"X-Grow-Sync-Token": raw_sync_tok},
            files={"file": ("..\\..\\etc\\evil_CIB.csv", trav_bytes, "text/csv")},
            data={"bank_id": str(fix.bank_cib.id), "auto_execute": "true"},
        )
        assert r_trav.status_code == 200, r_trav.text
        trav_item = db.query(InboxItem).filter(InboxItem.id == r_trav.json()["item_id"]).first()
        assert trav_item and trav_item.primary_attachment_name == "evil_CIB.csv"
        assert ".." not in trav_item.primary_attachment_path and "\\" not in trav_item.primary_attachment_name

        # ------------------------------------------------------------------
        # 11. /sync-watcher-script Host injection & PowerShell quoting
        # ------------------------------------------------------------------
        print("[11/12] Testing /sync-watcher-script Host header defense and PS single quotes...")
        r_ps_post = client.post(
            "/api/v1/inbox/sync-watcher-script",
            headers={**maker_hdrs, "Host": "evil.attacker.com"},
            json={"token_hint": 'gws_$(calc.exe)"inj'},
        )
        assert r_ps_post.status_code == 200, r_ps_post.text
        body_ps = r_ps_post.text
        assert "evil.attacker.com" not in body_ps
        assert "[string]$SyncToken   = 'gws_calc_exeinj'" in body_ps or "[string]$SyncToken   = '" in body_ps
        assert '$(calc.exe)' not in body_ps

        r_ps_get = client.get(
            "/api/v1/inbox/sync-watcher-script",
            headers={**maker_hdrs, "Host": "evil.attacker.com"},
            params={"token_hint": "gws_1234567890abcdef"},
        )
        assert r_ps_get.status_code == 200
        assert "evil.attacker.com" not in r_ps_get.text
        assert "'gws_1234567890abcdef'" in r_ps_get.text

        # ------------------------------------------------------------------
        # 12. SensitiveDataFilter token_hint & gws_* scrubbing
        # ------------------------------------------------------------------
        print("[12/12] Testing SensitiveDataFilter scrubbing for token_hint and gws_* tokens...")
        sdf = SensitiveDataFilter(preserve_args=False)
        secret_tok = "gws_0123456789abcdef0123456789abcdef"
        rec = logging.LogRecord(
            name="uvicorn.access",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="GET /api/v1/inbox/sync-watcher-script?token_hint=%s Header=%s",
            args=(secret_tok, secret_tok),
            exc_info=None,
        )
        sdf.filter(rec)
        assert secret_tok not in str(rec.msg)
        assert "token_hint=[REDACTED]" in str(rec.msg)
        assert "gws_[REDACTED]" in str(rec.msg)

        # Clean up temporary sync token reference cleanly
        assert client.delete(f"/api/v1/inbox/sync-tokens/{sync_tok_id}", headers=maker_hdrs).status_code == 200

        print("\nALL PHASE 2 SMART INBOX SECURITY & CONTRACT TESTS PASSED!")

    finally:
        try:
            fix.bank_cib.email_domain = orig_cib_domain
            fix.cust_alpha.status = SubscriptionStatus.ACTIVE
            fix.cust_beta.subscription_plan_id = fix.plan.id
            fix.plan.can_email_inbox = True
            fix.plan.has_issuance_module = True
            db.commit()
        except Exception:
            db.rollback()

        if extra_cust_ids:
            purge_test_customers_by_ids(db, cust_ids=extra_cust_ids)

        fix.cleanup()

        if extra_plan_ids:
            purge_test_customers_by_ids(db, cust_ids=[], plan_ids=extra_plan_ids)
        db.close()
        _ = mem_gcs


if __name__ == "__main__":
    run_all_tests()
