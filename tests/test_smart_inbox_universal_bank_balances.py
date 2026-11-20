# tests/test_smart_inbox_universal_bank_balances.py
"""
End-to-End Verification Suite for Universal Multi-Bank Balance & Statement Ingestion via Smart Inbox:
1. Excel Statement via Smart Inbox (GCS Dual-Read Preview/Download -> Statement Ingestion -> CashBalanceSnapshot)
2. Encrypted Password-Protected PDF Statement Unlock & Saved Bank Account PDF Password
3. Bilingual (Arabic + English) Inline Email-Body Cash Balances (CASH_POSITION_SNAPSHOT)
4. Scheduled Outbound BANK_STATEMENT & CASH_POSITION + Thread Reply Matching + Zero-Touch Auto-Ingest
5. Watched-Folder Sync Token + /sync-drop Bank Auto-Detection & Deduplication + Standing Instruction Letter
"""

import io
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from fastapi.testclient import TestClient
from pypdf import PdfReader, PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

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
)
from app.core.encryption import encrypt_data  # noqa: E402
from app.core.storage_service import build_customer_blob_path, upload_bytes_to_gcs_sync  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.main import app  # noqa: E402
from app.models.models import CustomerEmailSetting  # noqa: E402
from app.models.models_inbox import (  # noqa: E402
    CashBalanceSnapshot,
    InboxAttachment,
    InboxItem,
    InboxOutboundRequest,
    InboxScheduleConfig,
)
from app.models.models_reconciliation_v2 import BankStatement, BankTransaction  # noqa: E402
from app.services.inbox_action_service import inbox_action_service  # noqa: E402
from app.services.inbox_classification_service import inbox_classification_service  # noqa: E402
from app.services.inbox_outbound_service import inbox_outbound_service  # noqa: E402


def _make_excel_statement_bytes(rows: list, opening_bal: float = 1000000.00) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Statement")
    return buf.getvalue()


def _make_encrypted_pdf_statement_bytes(password: str) -> bytes:
    raw_buf = io.BytesIO()
    c = canvas.Canvas(raw_buf, pagesize=letter)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, 750, "HSBC Bank Egypt - Corporate Account Statement")
    c.setFont("Helvetica", 10)
    c.drawString(50, 730, "Account No: 777-888999-001   Currency: EGP")
    c.drawString(50, 715, "Opening Balance: 500,000.00 EGP")
    c.drawString(50, 685, "Date          Description                        Debit        Credit       Balance")
    c.drawString(50, 665, "2026-11-10    INWARD TRANSFER CLIENT A           0.00         150,000.00   650,000.00")
    c.drawString(50, 648, "2026-11-11    BANK SERVICE CHARGE                10,000.00    0.00         640,000.00")
    c.drawString(50, 620, "Closing Balance: 640,000.00 EGP")
    c.save()

    reader = PdfReader(io.BytesIO(raw_buf.getvalue()))
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    writer.encrypt(password)
    enc_buf = io.BytesIO()
    writer.write(enc_buf)
    return enc_buf.getvalue()


def run_suite() -> None:
    install_in_memory_gcs()
    install_email_capture()
    db = SessionLocal()
    fix = IsolatedTenantFixture(db, tag="INBOX_BAL")
    client = TestClient(app)

    try:
        admin_hdrs = make_auth_headers(fix.admin_user)
        maker_hdrs = make_auth_headers(fix.maker_user)

        # Ensure CustomerEmailSetting exists with dummy SMTP so outbound emails log cleanly without network calls
        email_setting = CustomerEmailSetting(
            customer_id=fix.cust_alpha.id,
            smtp_host="localhost",
            smtp_port=587,
            smtp_username="treasury@globex.com",
            smtp_password_encrypted=encrypt_data("dummy-smtp-pass"),
            sender_email=f"treasury.{fix.uid.lower()}@globex.com",
            sender_display_name=fix.cust_alpha.name,
            imap_host="imap.example.com",
            imap_port=993,
            imap_username=f"bankfeeds.{fix.uid.lower()}@globex.com",
            imap_password_encrypted=encrypt_data("dummy-imap-pass"),
            imap_is_active=True,
            is_active=True,
        )
        db.add(email_setting)
        db.commit()

        # ------------------------------------------------------------------
        # TEST 1: Excel Statement via Smart Inbox + GCS Dual-Read + Live Cash
        # ------------------------------------------------------------------
        print("[1/5] Testing Excel statement via Smart Inbox + GCS Dual-Read...")
        r_acct = client.post(
            "/api/v1/issuance/bank-accounts",
            headers=admin_hdrs,
            json={
                "bank_id": fix.bank_cib.id,
                "account_name": "CIB Main EGP Operating",
                "account_number": "001-987654-002",
                "currency_code": "EGP",
                "is_default": True,
                "auto_ingest_statements": False,
            },
        )
        assert r_acct.status_code == 200, r_acct.text
        cib_acct_id = r_acct.json()["id"]

        xlsx_bytes = _make_excel_statement_bytes([
            {"Date": "2026-11-12", "Description": "INWARD ACH CLIENT X", "Debit": 0.0, "Credit": 260000.50, "Balance": 1260000.50},
            {"Date": "2026-11-13", "Description": "BANK TRANSFER FEE", "Debit": 10000.0, "Credit": 0.0, "Balance": 1250000.50},
        ])
        cib_gcs_uri = upload_bytes_to_gcs_sync(
            build_customer_blob_path(fix.cust_alpha.id, "inbox", f"msg_cib_{fix.uid}/CIB_Daily_Statement.xlsx"),
            xlsx_bytes,
            content_type="application/octet-stream",
        )
        item_cib = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"cib-stmt-{fix.uid}@cibeg.com",
            sender_email="statements@cibeg.com",
            sender_domain="cibeg.com",
            subject="Daily Account Statement - EGP",
            received_at=datetime.now(timezone.utc),
            has_attachment=True,
            attachment_count=1,
            primary_attachment_path=cib_gcs_uri,
            primary_attachment_name="CIB_Daily_Statement.xlsx",
            primary_attachment_type="XLSX",
            status="RECEIVED",
        )
        db.add(item_cib)
        db.flush()
        att_cib = InboxAttachment(
            inbox_item_id=item_cib.id,
            file_name="CIB_Daily_Statement.xlsx",
            file_type="XLSX",
            file_size_bytes=len(xlsx_bytes),
            storage_path=cib_gcs_uri,
            is_primary=True,
        )
        db.add(att_cib)
        db.commit()

        classified_cib = inbox_classification_service.classify(db, item_cib)
        assert classified_cib.classification == "BANK_STATEMENT", classified_cib.classification
        assert classified_cib.classification_confidence == "HIGH", classified_cib.classification_confidence

        # Verify GCS preview & download work without local file
        r_prev = client.get(f"/api/v1/inbox/items/{item_cib.id}/attachment/{att_cib.id}/preview", headers=maker_hdrs)
        assert r_prev.status_code == 200 and r_prev.json()["preview_type"] == "TABLE", r_prev.text
        r_dl = client.get(f"/api/v1/inbox/items/{item_cib.id}/attachment/{att_cib.id}", headers=maker_hdrs)
        assert r_dl.status_code == 200 and r_dl.content == xlsx_bytes

        # Confirm statement ingestion
        r_conf1 = client.post(
            f"/api/v1/inbox/items/{item_cib.id}/confirm",
            headers=maker_hdrs,
            json={"override_bank_id": fix.bank_cib.id},
        )
        assert r_conf1.status_code == 200, r_conf1.text
        conf1_data = r_conf1.json()
        assert conf1_data["status"] == "ACTIONED"
        assert conf1_data["closing_balance"] == 1250000.5
        assert conf1_data["statement_id"] is not None

        # Verify Live Cash Positions
        r_pos1 = client.get("/api/v1/inbox/cash-positions", headers=maker_hdrs)
        assert r_pos1.status_code == 200, r_pos1.text
        pos1 = r_pos1.json()
        assert pos1["totals_by_currency"]["EGP"]["available_balance"] == 1250000.5

        # ------------------------------------------------------------------
        # TEST 2: Password-Protected PDF Statement Unlock & Saved Password
        # ------------------------------------------------------------------
        print("[2/5] Testing Password-Protected PDF Statement Unlock & Saved Bank Account Password...")
        r_hsbc = client.post(
            "/api/v1/issuance/bank-accounts",
            headers=admin_hdrs,
            json={
                "bank_id": fix.bank_hsbc.id,
                "account_name": "HSBC Corporate EGP",
                "account_number": "777-888999-001",
                "currency_code": "EGP",
                "is_default": True,
            },
        )
        assert r_hsbc.status_code == 200, r_hsbc.text
        hsbc_acct_id = r_hsbc.json()["id"]

        enc_pdf_bytes = _make_encrypted_pdf_statement_bytes("CIB9988")
        hsbc_gcs_uri = upload_bytes_to_gcs_sync(
            build_customer_blob_path(fix.cust_alpha.id, "inbox", f"msg_hsbc_{fix.uid}/HSBC_Statement.pdf"),
            enc_pdf_bytes,
            content_type="application/pdf",
        )
        item_hsbc = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"hsbc-pdf-{fix.uid}@hsbc.com",
            sender_email="estatement@hsbc.com",
            sender_domain="hsbc.com",
            subject="Your HSBC e-Statement PDF",
            received_at=datetime.now(timezone.utc),
            has_attachment=True,
            attachment_count=1,
            primary_attachment_path=hsbc_gcs_uri,
            primary_attachment_name="HSBC_Statement.pdf",
            primary_attachment_type="PDF",
            status="RECEIVED",
        )
        db.add(item_hsbc)
        db.flush()
        db.add(InboxAttachment(
            inbox_item_id=item_hsbc.id,
            file_name="HSBC_Statement.pdf",
            file_type="PDF",
            file_size_bytes=len(enc_pdf_bytes),
            storage_path=hsbc_gcs_uri,
            is_primary=True,
        ))
        db.commit()

        classified_hsbc = inbox_classification_service.classify(db, item_hsbc)
        assert classified_hsbc.classification == "BANK_STATEMENT", classified_hsbc.classification
        assert "pdf_password_required" in (classified_hsbc.classification_signals or {})

        # Without password -> must reject cleanly with PDF_PASSWORD_REQUIRED without locking item in PARSE_ERROR
        r_fail_pdf = client.post(f"/api/v1/inbox/items/{item_hsbc.id}/confirm", headers=maker_hdrs, json={})
        assert r_fail_pdf.status_code == 400
        assert "PDF_PASSWORD_REQUIRED" in r_fail_pdf.json()["detail"]
        db.refresh(item_hsbc)
        assert item_hsbc.status == "CLASSIFIED"

        # Save static PDF password on Bank Account via Corporate Admin API
        r_upd_acct = client.put(
            f"/api/v1/issuance/bank-accounts/{hsbc_acct_id}",
            headers=admin_hdrs,
            json={"statement_pdf_password": "CIB9988"},
        )
        assert r_upd_acct.status_code == 200, r_upd_acct.text
        r_list_accts = client.get("/api/v1/issuance/bank-accounts", headers=admin_hdrs)
        hsbc_listed = [a for a in r_list_accts.json() if a["id"] == hsbc_acct_id][0]
        assert hsbc_listed["has_statement_pdf_password"] is True
        assert "CIB9988" not in str(hsbc_listed)

        # Retry confirm WITHOUT sending password -> auto-unlocked via saved Bank Account password!
        r_ok_pdf = client.post(f"/api/v1/inbox/items/{item_hsbc.id}/confirm", headers=maker_hdrs, json={})
        assert r_ok_pdf.status_code == 200, r_ok_pdf.text
        pdf_res = r_ok_pdf.json()
        assert pdf_res["status"] == "ACTIONED"
        assert pdf_res["closing_balance"] == 640000.0

        # ------------------------------------------------------------------
        # TEST 3: Bilingual Inline Arabic & English Email-Body Balances
        # ------------------------------------------------------------------
        print("[3/5] Testing Inline Bilingual Arabic + English Cash Balances...")
        bilingual_body = (
            "السادة الأفاضل، مرفق موقف الأرصدة اليومي:\n"
            "حساب رقم: 1122334455\n"
            "الرصيد المتاح: 2,450,000.00 ج.م\n"
            "الرصيد الدفتري: 2,475,000.00 ج.م\n"
            "Account No: 99887766\n"
            "USD Available Balance: USD 85,200.50\n"
            "USD Ledger Balance: USD 85,200.50\n"
        )
        item_bal = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"cib-bal-{fix.uid}@cibeg.com",
            sender_email="rm@cibeg.com",
            sender_domain="cibeg.com",
            subject="Daily Cash Position / موقف الأرصدة",
            body_text_encrypted=encrypt_data(bilingual_body),
            received_at=datetime.now(timezone.utc),
            has_attachment=False,
            attachment_count=0,
            status="RECEIVED",
        )
        db.add(item_bal)
        db.commit()

        classified_bal = inbox_classification_service.classify(db, item_bal)
        assert classified_bal.classification == "CASH_POSITION_SNAPSHOT", classified_bal.classification
        assert len(classified_bal.extracted_balances_json or []) == 2, classified_bal.extracted_balances_json

        r_conf_bal = client.post(f"/api/v1/inbox/items/{item_bal.id}/confirm", headers=maker_hdrs, json={})
        assert r_conf_bal.status_code == 200, r_conf_bal.text
        assert r_conf_bal.json()["snapshots_created"] == 2

        r_recon_pos = client.get("/api/v1/reconciliation/cash-positions", headers=maker_hdrs)
        assert r_recon_pos.status_code == 200
        totals = r_recon_pos.json()["totals_by_currency"]
        assert "USD" in totals and totals["USD"]["available_balance"] == 85200.5

        # ------------------------------------------------------------------
        # TEST 4: Scheduled Outbound BANK_STATEMENT & CASH_POSITION + Auto-Ingest
        # ------------------------------------------------------------------
        print("[4/5] Testing Scheduled Outbound Statement/Balance + Reply Threading + Zero-Touch Auto-Ingest...")
        r_qnb = client.post(
            "/api/v1/issuance/bank-accounts",
            headers=admin_hdrs,
            json={
                "bank_id": fix.bank_qnb.id,
                "account_name": "QNB Zero-Touch Account",
                "account_number": "2030405060",
                "currency_code": "EGP",
                "is_default": True,
                "auto_ingest_statements": True,
            },
        )
        assert r_qnb.status_code == 200

        r_sched_stmt = client.post(
            "/api/v1/inbox/schedule-configs",
            headers=admin_hdrs,
            json={
                "bank_id": fix.bank_qnb.id,
                "request_type": "BANK_STATEMENT",
                "is_active": True,
                "frequency": "DAILY",
                "recipient_emails": ["cash-ops@qnb.com.eg"],
            },
        )
        assert r_sched_stmt.status_code == 200, r_sched_stmt.text

        r_sched_bal = client.post(
            "/api/v1/inbox/schedule-configs",
            headers=admin_hdrs,
            json={
                "bank_id": fix.bank_cib.id,
                "request_type": "CASH_POSITION",
                "is_active": True,
                "frequency": "DAILY",
                "recipient_emails": ["rm@cibeg.com"],
            },
        )
        assert r_sched_bal.status_code == 200, r_sched_bal.text

        sent_reqs = inbox_outbound_service.send_scheduled_requests(db)
        assert len(sent_reqs) >= 2
        qnb_req = [r for r in sent_reqs if r.bank_id == fix.bank_qnb.id and r.request_type == "BANK_STATEMENT"][0]
        cib_bal_req = [r for r in sent_reqs if r.bank_id == fix.bank_cib.id and r.request_type == "CASH_POSITION"][0]
        assert qnb_req.request_reference.startswith("STM-")
        assert cib_bal_req.request_reference.startswith("BAL-")

        # Running scheduler again on same day must NOT duplicate sends
        dup_reqs = inbox_outbound_service.send_scheduled_requests(db)
        assert len(dup_reqs) == 0

        # Simulate QNB reply with thread match -> Zero-Touch Auto-Ingest triggers automatically!
        qnb_xlsx = _make_excel_statement_bytes([
            {"Date": "2026-11-15", "Description": "COLLECTION INSTAPAY", "Debit": 0.0, "Credit": 910000.00, "Balance": 910000.00},
        ])
        qnb_uri = upload_bytes_to_gcs_sync(
            build_customer_blob_path(fix.cust_alpha.id, "inbox", f"msg_qnb_{fix.uid}/QNB_Stmt.xlsx"),
            qnb_xlsx,
        )
        item_qnb = InboxItem(
            customer_id=fix.cust_alpha.id,
            message_id=f"qnb-reply-{fix.uid}@qnb.com.eg",
            in_reply_to=qnb_req.message_id,
            sender_email="cash-ops@qnb.com.eg",
            sender_domain="qnb.com.eg",
            subject=f"Re: {qnb_req.subject}",
            body_text_encrypted=encrypt_data(f"Please find attached statement for {qnb_req.request_reference}."),
            received_at=datetime.now(timezone.utc),
            has_attachment=True,
            attachment_count=1,
            primary_attachment_path=qnb_uri,
            primary_attachment_name="QNB_Stmt.xlsx",
            primary_attachment_type="XLSX",
            status="RECEIVED",
        )
        db.add(item_qnb)
        db.flush()
        db.add(InboxAttachment(
            inbox_item_id=item_qnb.id,
            file_name="QNB_Stmt.xlsx",
            file_type="XLSX",
            file_size_bytes=len(qnb_xlsx),
            storage_path=qnb_uri,
            is_primary=True,
        ))
        db.commit()

        classified_qnb = inbox_classification_service.classify(db, item_qnb)
        auto_ok = inbox_action_service.try_auto_execute_item(db, classified_qnb)
        assert auto_ok is True
        db.refresh(classified_qnb)
        db.refresh(qnb_req)
        assert classified_qnb.status == "ACTIONED"
        assert classified_qnb.action_reference_type == "BankStatement"
        assert qnb_req.is_replied is True

        # ------------------------------------------------------------------
        # TEST 5: Watched-Folder Sync Token, /sync-drop & Standing Letter
        # ------------------------------------------------------------------
        print("[5/5] Testing Watched-Folder Sync Token, /sync-drop Dedup & Standing Instruction Letter...")
        r_tok = client.post(
            "/api/v1/inbox/sync-tokens",
            headers=maker_hdrs,
            json={"label": "Treasurer Office PC"},
        )
        assert r_tok.status_code == 200, r_tok.text
        raw_sync_tok = r_tok.json()["plaintext_token"]
        assert raw_sync_tok.startswith("gws_")

        csv_payload = (
            "Date,Description,Debit,Credit,Balance\n"
            "2026-11-18,PORTAL EXPORT RECEIPT,0.00,50000.00,1300000.00\n"
        ).encode("utf-8")

        r_drop1 = client.post(
            "/api/v1/inbox/sync-drop",
            headers={"X-Grow-Sync-Token": raw_sync_tok},
            files={"file": ("CIB_Portal_Nov18.csv", csv_payload, "text/csv")},
            data={"auto_execute": "true"},
        )
        assert r_drop1.status_code == 200, r_drop1.text
        drop1 = r_drop1.json()
        assert drop1["status"] == "ACTIONED", drop1
        assert drop1["matched_bank_id"] == fix.bank_cib.id

        # Dropping exact same file again must be deduplicated cleanly
        r_drop2 = client.post(
            "/api/v1/inbox/sync-drop",
            headers={"X-Grow-Sync-Token": raw_sync_tok},
            files={"file": ("CIB_Portal_Nov18.csv", csv_payload, "text/csv")},
        )
        assert r_drop2.status_code == 200
        assert r_drop2.json()["status"] == "DUPLICATE_SKIPPED"

        # Watcher PowerShell script endpoint
        r_ps1 = client.get("/api/v1/inbox/sync-watcher-script", headers=maker_hdrs)
        assert r_ps1.status_code == 200
        assert "X-Grow-Sync-Token" in r_ps1.text and "GrowSync" in r_ps1.text

        # Standing Instruction Letter endpoint
        r_let = client.get(f"/api/v1/inbox/standing-instruction-letter/{fix.bank_cib.id}", headers=admin_hdrs)
        assert r_let.status_code == 200, r_let.text
        letter_json = r_let.json()
        assert "001-987654-002" in letter_json["html_letter"]
        assert email_setting.imap_username in letter_json["html_letter"]

        # Verify stats endpoint includes cash_position_count
        r_stats = client.get("/api/v1/inbox/stats", headers=maker_hdrs)
        assert r_stats.status_code == 200
        assert r_stats.json()["cash_position_count"] >= 1
        assert r_stats.json()["actioned_count"] >= 5

        print("ALL_UNIVERSAL_INBOX_BALANCE_TESTS_PASSED")
    finally:
        fix.cleanup()
        db.close()


if __name__ == "__main__":
    run_suite()
