"""
Phase 2 Verification Suite: Official Month-End Bank Reconciliation Statement (BRS) Audit Pack
& 4 Departmental Exception / Action Packs.

Verifies:
1. Two-Way Mathematical Proof-of-Cash Bridge:
   - Bank Closing Balance + Deposits in Transit - Unpresented Payments == Adjusted True Cash Position
   - Implied Book Balance reconciles to 0.00 Unexplained Variance
   - Explicit Trial Balance override accurately detects exact unexplained discrepancy when books differ!
2. BRS Period Sign-Off & Cryptographic WORM Audit Chain Hash (`brs_worm_hash`) + Transaction Lock + Admin Reopen.
3. Live Multi-Page WeasyPrint BRS Audit PDF rendering (extracting PDF text to verify Adjusted Cash & WORM Hash)
   + 5-Sheet Excel Audit Workbook (`.xlsx`) generation.
4. All 4 Departmental Action Packs (`AR_COLLECTIONS_AND_MYSTERY_CASH`, `WHT_TAX_CERTIFICATES_REGISTER`,
   `AP_STALE_PAYMENTS_AND_BOUNCED_RISK`, `BANK_FEES_AND_FLOAT_AUDIT`), formula-injection neutralization,
   and multi-tenant HTTP isolation!
"""

import csv
import io
import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.models_reconciliation_v2 import (
    Base,
    BankStatement,
    BankTransaction,
    InternalLedgerRecord,
    ReconciliationMatch,
    ClassificationRule,
    ClassificationTaxonomy,
    Counterparty,
    CollaborativePattern,
    CollaborativeTenantVote,
    BankAccountProfile,
    MatchingToleranceRule,
    StatementInputMappingProfile,
    ReconciliationExportProfile,
    MultiReference,
)
from app.services.reconciliation_brs_and_packs_service import reconciliation_brs_service


def _make_sqlite_db():
    eng = create_engine("sqlite:///:memory:", echo=False)
    SessionCls = sessionmaker(bind=eng, autoflush=False, autocommit=False)
    tables = [
        ClassificationTaxonomy.__table__,
        Counterparty.__table__,
        ClassificationRule.__table__,
        BankStatement.__table__,
        BankTransaction.__table__,
        MultiReference.__table__,
        InternalLedgerRecord.__table__,
        ReconciliationMatch.__table__,
        CollaborativePattern.__table__,
        CollaborativeTenantVote.__table__,
        BankAccountProfile.__table__,
        MatchingToleranceRule.__table__,
        StatementInputMappingProfile.__table__,
        ReconciliationExportProfile.__table__,
    ]
    Base.metadata.create_all(bind=eng, tables=tables)
    return SessionCls()


def _seed_brs_and_packs_scenario(db, company_id: int = 88, bank_id: int = 1, created_by: int = 1):
    stmt = BankStatement(
        company_id=company_id,
        bank_id=bank_id,
        account_number="ACC-BRS-001",
        file_name="CIB_MonthEnd_Aug2026.csv",
        opening_balance=Decimal("500000.00"),
        closing_balance=Decimal("578500.00"),
        statement_start_date=datetime(2026, 8, 1),
        statement_end_date=datetime(2026, 8, 31),
        status="VALIDATED",
    )
    db.add(stmt)
    db.flush()

    # 1) Matched AR Collection with 1,000 EGP Form-41 WHT Deduction (Invoice 100,000 -> Bank +99,000)
    t_ar = BankTransaction(
        statement_id=stmt.id,
        account_number="ACC-BRS-001",
        booking_date=datetime(2026, 8, 10),
        value_date=datetime(2026, 8, 10),
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("99000.00"),
        currency="EGP",
        running_balance=Decimal("599000.00"),
        raw_description="COLLECTION ORANGE EGYPT WHT DEDUCTED",
        counterparty_name="Orange Egypt",
        is_reconciled=True,
        is_classified=True,
        classification_category="COLLECTION_FROM_CUSTOMER",
        internal_category="1020100 - Trade Receivables",
        variance_amount=Decimal("1000.00"),
    )

    # 2) Unidentified Mystery Credit (+25,000) with formula-injection probe in narrative!
    t_mystery = BankTransaction(
        statement_id=stmt.id,
        account_number="ACC-BRS-001",
        booking_date=datetime(2026, 8, 14),
        value_date=datetime(2026, 8, 17),  # 3-day clearing float lag!
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("25000.00"),
        currency="EGP",
        running_balance=Decimal("624000.00"),
        raw_description="=HYPERLINK('http://evil','INSTAPAY DEPOSIT UNKNOWN')",
        is_reconciled=False,
        is_classified=False,
    )

    # 3) Direct Bank Fee (-500)
    t_fee = BankTransaction(
        statement_id=stmt.id,
        account_number="ACC-BRS-001",
        booking_date=datetime(2026, 8, 20),
        value_date=datetime(2026, 8, 20),
        debit_amount=Decimal("500.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("623500.00"),
        raw_description="MONTHLY CORPORATE TARIFF FEE",
        is_reconciled=False,
        is_classified=True,
        classification_category="BANK_CHARGES",
        internal_category="5040100 - Bank Charges",
    )

    # 4) Bounced Customer Cheque (-45,000)
    t_bounced = BankTransaction(
        statement_id=stmt.id,
        account_number="ACC-BRS-001",
        booking_date=datetime(2026, 8, 25),
        value_date=datetime(2026, 8, 25),
        debit_amount=Decimal("45000.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("578500.00"),
        raw_description="RETURNED BOUNCED CHEQUE CHQ 88412",
        counterparty_name="Nile Distributors",
        cheque_number="88412",
        is_reconciled=False,
        is_classified=True,
        classification_category="REVERSED_COLLECTION",
        internal_category="1020100 - Trade Receivables",
    )

    db.add_all([t_ar, t_mystery, t_fee, t_bounced])
    db.flush()

    # Reconciled ERP AR Invoice (+100,000)
    erp_cleared = InternalLedgerRecord(
        company_id=company_id,
        record_type="AR_INVOICE",
        reference_number="INV-8801",
        entity_name="Orange Egypt",
        record_date=datetime(2026, 8, 5),
        amount=Decimal("100000.00"),
        currency="EGP",
        gl_account="1020100 - Trade Receivables",
        bank_id=bank_id,
        bank_account_number="ACC-BRS-001",
        status="RECONCILED",
        matched_bank_txn_id=t_ar.id,
    )
    # Open Deposit in Transit (+15,000 recorded Aug 30 in ERP, not yet in bank)
    erp_dit = InternalLedgerRecord(
        company_id=company_id,
        record_type="AR_INVOICE",
        reference_number="REC-DIT-01",
        entity_name="Vodafone Egypt",
        record_date=datetime(2026, 8, 30),
        amount=Decimal("15000.00"),
        currency="EGP",
        bank_id=bank_id,
        bank_account_number="ACC-BRS-001",
        status="OPEN",
    )
    # Open Stale Unpresented Payment (-32,000 recorded 18 days before month-end, still OPEN!)
    erp_up = InternalLedgerRecord(
        company_id=company_id,
        record_type="AP_BILL",
        reference_number="PAY-CHQ-902",
        entity_name="Elsewedy Cables",
        record_date=datetime(2026, 8, 12),
        amount=Decimal("-32000.00"),
        currency="EGP",
        bank_id=bank_id,
        bank_account_number="ACC-BRS-001",
        status="OPEN",
    )
    db.add_all([erp_cleared, erp_dit, erp_up])
    db.flush()

    db.add(
        ReconciliationMatch(
            bank_txn_id=t_ar.id,
            source_type="AR_INVOICE",
            source_record_id=erp_cleared.id,
            match_type="1:1",
            match_logic="TOLERANCE",
            variance_amount=Decimal("1000.00"),
            variance_disposition="WHT_DEDUCTION",
            created_by=created_by,
        )
    )
    db.commit()
    return stmt


def test_brs_proof_of_cash_math_signoff_and_excel():
    db = _make_sqlite_db()
    try:
        stmt = _seed_brs_and_packs_scenario(db)
        brs = reconciliation_brs_service.build_statement_brs_report(db, customer_id=88, statement_id=stmt.id)
        b = brs["bridge"]

        # Bank Closing (578,500) + DIT (15,000) - Unpresented (32,000) == 561,500.00 Adjusted Cash!
        assert Decimal(str(b["bank_closing_balance"])) == Decimal("578500.00")
        assert Decimal(str(b["plus_deposits_in_transit"])) == Decimal("15000.00")
        assert Decimal(str(b["less_unpresented_payments"])) == Decimal("32000.00")
        assert Decimal(str(b["adjusted_cash_balance"])) == Decimal("561500.00")

        # Unbooked Credits = 25,000 (Mystery) | Unbooked Debits = 45,500 (500 fee + 45,000 bounced) | Match WHT = 1,000
        # Implied Book = 561,500 - 25,000 + 45,500 + 1,000 = 583,000.00!
        assert Decimal(str(b["implied_erp_book_balance"])) == Decimal("583000.00")
        assert b["is_proof_of_cash_balanced"] is True
        assert Decimal(str(b["unexplained_variance"])) == Decimal("0.00")

        # If Chief Accountant enters an off-by-250 Trial Balance figure (583,250.00), BRS flags -250.00 variance!
        brs_diff = reconciliation_brs_service.build_statement_brs_report(
            db, customer_id=88, statement_id=stmt.id, erp_book_balance_override=Decimal("583250.00")
        )
        assert brs_diff["bridge"]["is_proof_of_cash_balanced"] is False
        assert Decimal(str(brs_diff["bridge"]["unexplained_variance"])) == Decimal("-250.00")

        # Sign Off with exact book balance 583,000.00 -> locks lines & stamps WORM hash!
        signed = reconciliation_brs_service.sign_off_statement_brs(
            db=db,
            customer_id=88,
            statement_id=stmt.id,
            user_id=1,
            erp_book_closing_balance=Decimal("583000.00"),
            notes="Reconciled to SAP Trial Balance Aug 2026",
            lock_transactions=True,
        )
        assert signed["statement"]["is_signed_off"] is True
        assert len(signed["statement"]["brs_worm_hash"]) == 64
        locked_cnt = db.query(BankTransaction).filter(
            BankTransaction.statement_id == stmt.id, BankTransaction.is_locked == True
        ).count()
        assert locked_cnt == 4

        # Verify 5-sheet Excel export
        xlsx_bytes = reconciliation_brs_service.render_brs_excel_bytes(signed)
        assert len(xlsx_bytes) > 2000
        print("[PASS] test_brs_proof_of_cash_math_signoff_and_excel passed")
    finally:
        db.close()


def test_all_four_departmental_action_packs_and_formula_defense():
    db = _make_sqlite_db()
    try:
        stmt = _seed_brs_and_packs_scenario(db)

        # Pack 1: AR Collections & Mystery Cash
        p1 = reconciliation_brs_service.build_action_packs(
            db, customer_id=88, selected_pack="AR_COLLECTIONS_AND_MYSTERY_CASH", statement_id=stmt.id
        )
        assert len(p1["rows"]) == 2  # 1 Mystery + 1 Verified Collection
        csv_bytes, _, _ = reconciliation_brs_service.export_action_pack_bytes(p1, file_format="CSV")
        assert csv_bytes.startswith(b"\xef\xbb\xbf")
        csv_rows = list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig"))))
        mystery_row = next(r for r in csv_rows if r["Section"] == "UNIDENTIFIED_MYSTERY_CREDIT")
        assert mystery_row["Raw_Bank_Narrative"].startswith("'=HYPERLINK"), "Formula injection must be neutralized!"

        # Pack 2: WHT Tax Certificates Register
        p2 = reconciliation_brs_service.build_action_packs(
            db, customer_id=88, selected_pack="WHT_TAX_CERTIFICATES_REGISTER", statement_id=stmt.id
        )
        assert len(p2["rows"]) == 1
        assert p2["rows"][0]["Invoice_Reference"] == "INV-8801"
        assert Decimal(str(p2["rows"][0]["Tax_Deducted_Amount"])) == Decimal("1000.00")

        # Pack 3: AP Stale Payments & Bounced Cheques
        p3 = reconciliation_brs_service.build_action_packs(
            db, customer_id=88, selected_pack="AP_STALE_PAYMENTS_AND_BOUNCED_RISK", statement_id=stmt.id
        )
        alert_types = {r["Alert_Type"] for r in p3["rows"]}
        assert "CRITICAL_BOUNCED_COLLECTION" in alert_types
        assert "STALE_UNCLEARED_ERP_PAYMENT" in alert_types

        # Pack 4: Bank Fees & Clearing Float Audit
        p4 = reconciliation_brs_service.build_action_packs(
            db, customer_id=88, selected_pack="BANK_FEES_AND_FLOAT_AUDIT", statement_id=stmt.id
        )
        finding_types = {r["Finding_Type"] for r in p4["rows"]}
        assert "DIRECT_BANK_CHARGE" in finding_types
        assert "CLEARING_FLOAT_DELAY" in finding_types
        print("[PASS] test_all_four_departmental_action_packs_and_formula_defense passed")
    finally:
        db.close()


def test_phase2_http_pdf_and_worm_audit_e2e():
    from _test_harness import (
        app,
        SessionLocal,
        IsolatedTenantFixture,
        make_auth_headers,
        extract_pdf_info,
    )

    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="P2_BRS")
    client = TestClient(app)
    try:
        admin_h = make_auth_headers(fx.admin_user, extra_claims={"has_reconciliation_module": True})
        beta_h = make_auth_headers(fx.beta_admin, extra_claims={"has_reconciliation_module": True})

        stmt = _seed_brs_and_packs_scenario(
            db, company_id=fx.cust_alpha.id, bank_id=fx.bank_cib.id, created_by=fx.admin_user.id
        )

        # 1. GET BRS Report via HTTP
        r_brs = client.get(f"/api/v1/reconciliation/statements/{stmt.id}/brs", headers=admin_h)
        assert r_brs.status_code == 200, r_brs.text
        assert r_brs.json()["bridge"]["adjusted_cash_balance"] == 561500.0

        # Cross-tenant block check
        assert client.get(f"/api/v1/reconciliation/statements/{stmt.id}/brs", headers=beta_h).status_code == 404

        # 2. Sign-Off BRS -> Verify real PostgreSQL WORM Audit Chain SHA-256 Hash!
        r_sign = client.post(
            f"/api/v1/reconciliation/statements/{stmt.id}/brs/sign-off",
            headers=admin_h,
            json={"erp_book_closing_balance": 583000.00, "notes": "External Audit Approved", "lock_transactions": True},
        )
        assert r_sign.status_code == 200, r_sign.text
        worm_hash = r_sign.json()["statement"]["brs_worm_hash"]
        assert worm_hash and len(worm_hash) == 64

        # 3. Export Official Multi-Page BRS Certificate PDF & inspect real PDF text!
        r_pdf = client.get(f"/api/v1/reconciliation/statements/{stmt.id}/brs/export?format=PDF", headers=admin_h)
        assert r_pdf.status_code == 200, r_pdf.text
        assert r_pdf.content.startswith(b"%PDF")
        pdf_info = extract_pdf_info(r_pdf.content)
        assert pdf_info["page_count"] >= 1
        assert "561,500.00" in pdf_info["text"]
        assert worm_hash[:16] in pdf_info["text"]

        # 4. Export Departmental Action Pack XLSX & PDF via HTTP
        r_pack = client.get("/api/v1/reconciliation/action-packs?pack_key=WHT_TAX_CERTIFICATES_REGISTER", headers=admin_h)
        assert r_pack.status_code == 200
        assert r_pack.json()["total_rows"] >= 1

        r_pack_pdf = client.get(
            "/api/v1/reconciliation/action-packs/WHT_TAX_CERTIFICATES_REGISTER/export?format=PDF",
            headers=admin_h,
        )
        assert r_pack_pdf.status_code == 200 and r_pack_pdf.content.startswith(b"%PDF")

        # 5. Reopen BRS as Corporate Admin
        r_reopen = client.post(f"/api/v1/reconciliation/statements/{stmt.id}/brs/reopen", headers=admin_h)
        assert r_reopen.status_code == 200
        assert r_reopen.json()["statement"]["is_signed_off"] is False
        print("[PASS] test_phase2_http_pdf_and_worm_audit_e2e passed")
    finally:
        fx.cleanup()
        db.close()


if __name__ == "__main__":
    test_brs_proof_of_cash_math_signoff_and_excel()
    test_all_four_departmental_action_packs_and_formula_defense()
    test_phase2_http_pdf_and_worm_audit_e2e()
    print("ALL_PHASE2_BRS_AND_ACTION_PACKS_TESTS_PASSED")
