"""
Phase 3 Verification Suite: CFO Direct-Method Cash Flow (IAS 7), 7/14/30-Day Rolling Cash Liquidity
Forecast (with Learned Counterparty Lag AI & CashBalanceSnapshot Live Override), and Bank Fee/Float Scorecard.
"""

import io
import os
import sys
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.models_reconciliation_v2 import (
    Base,
    BankAccountProfile,
    BankStatement,
    BankTransaction,
    ClassificationRule,
    ClassificationTaxonomy,
    CollaborativePattern,
    CollaborativeTenantVote,
    Counterparty,
    InternalLedgerRecord,
    MatchingToleranceRule,
    MultiReference,
    ReconciliationExportProfile,
    ReconciliationMatch,
    StatementInputMappingProfile,
)
from app.services.reconciliation_cash_intelligence_service import (
    reconciliation_cash_intelligence_service,
)


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


def _seed_phase3_scenario(db, company_id: int = 99, bank1_id: int = 1, bank2_id: int = 2, user_id: int = 1):
    # Statement 1: Bank 1 Operating EGP (Closing 500,000 on 2026-08-31)
    s1 = BankStatement(
        company_id=company_id,
        bank_id=bank1_id,
        account_number="ACC-B1-EGP",
        file_name="B1_Aug2026.csv",
        opening_balance=Decimal("421100.00"),
        closing_balance=Decimal("500000.00"),
        statement_start_date=datetime(2026, 8, 1),
        statement_end_date=datetime(2026, 8, 31),
        status="VALIDATED",
    )
    # Statement 2: Bank 2 Liquidity EGP (Closing 260,000 on 2026-08-31)
    s2 = BankStatement(
        company_id=company_id,
        bank_id=bank2_id,
        account_number="ACC-B2-EGP",
        file_name="B2_Aug2026.csv",
        opening_balance=Decimal("200000.00"),
        closing_balance=Decimal("260000.00"),
        statement_start_date=datetime(2026, 8, 1),
        statement_end_date=datetime(2026, 8, 31),
        status="VALIDATED",
    )
    # Statement 3: Bank 1 USD Account (Closing 50,000 USD — tests multi-currency non-contamination)
    s_usd = BankStatement(
        company_id=company_id,
        bank_id=bank1_id,
        account_number="ACC-B1-USD",
        file_name="B1_USD_Aug2026.csv",
        opening_balance=Decimal("50000.00"),
        closing_balance=Decimal("50000.00"),
        statement_start_date=datetime(2026, 8, 1),
        statement_end_date=datetime(2026, 8, 31),
        status="VALIDATED",
    )
    db.add_all([s1, s2, s_usd])
    db.flush()

    # Dummy USD transaction so s_usd resolves to USD in SQLite
    db.add(
        BankTransaction(
            statement_id=s_usd.id,
            account_number="ACC-B1-USD",
            booking_date=datetime(2026, 8, 5),
            value_date=datetime(2026, 8, 5),
            debit_amount=Decimal("0.00"),
            credit_amount=Decimal("1000.00"),
            currency="USD",
            running_balance=Decimal("50000.00"),
            raw_description="EXPORT USD COLLECTION",
            is_classified=True,
            classification_category="COLLECTION_FROM_CUSTOMER",
        )
    )

    # 1) Two Historical AR Collections for "Orange Egypt" on Bank 1:
    #    - Invoice 1 recorded Aug 1, cleared Aug 12 (11d lag), 4-day value-date float lag (Aug 16)!
    #    - Invoice 2 recorded Aug 5, cleared Aug 18 (13d lag), 2-day value-date float lag (Aug 20)!
    #    => Learned avg lag for "Orange Egypt" = 12 days!
    t_ar1 = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 12),
        value_date=datetime(2026, 8, 16),  # 4d float lag!
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("120000.00"),
        currency="EGP",
        running_balance=Decimal("541100.00"),
        raw_description="COLLECTION ORANGE EGYPT INV-H1",
        counterparty_name="Orange Egypt",
        is_reconciled=True,
        is_classified=True,
        classification_category="COLLECTION_FROM_CUSTOMER",
    )
    t_ar2 = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 18),
        value_date=datetime(2026, 8, 20),  # 2d float lag!
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("80000.00"),
        currency="EGP",
        running_balance=Decimal("621100.00"),
        raw_description="COLLECTION ORANGE EGYPT INV-H2",
        counterparty_name="Orange Egypt",
        is_reconciled=True,
        is_classified=True,
        classification_category="COLLECTION_FROM_CUSTOMER",
    )

    # 2) Operating Outflows: Supplier AP (-80,000), Payroll (-50,000), Direct Bank Fee (-400)
    t_ap = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 20),
        value_date=datetime(2026, 8, 20),
        debit_amount=Decimal("80000.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("541100.00"),
        raw_description="ACH SUPPLIER ELSEWEDY",
        is_classified=True,
        classification_category="PAYMENT_TO_SUPPLIER",
    )
    t_pay = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 25),
        value_date=datetime(2026, 8, 25),
        debit_amount=Decimal("50000.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("491100.00"),
        raw_description="AUGUST PAYROLL BATCH",
        is_classified=True,
        classification_category="SALARY_PAYROLL",
    )
    t_fee = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 28),
        value_date=datetime(2026, 8, 28),
        debit_amount=Decimal("400.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("490700.00"),
        raw_description="ACCOUNT MAINTENANCE COMMISSION",
        is_classified=True,
        classification_category="BANK_CHARGES",
    )

    # 3) Investing (-30,000 CapEx) & Financing (+100,000 Loan Drawdown)
    t_capex = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 26),
        value_date=datetime(2026, 8, 26),
        debit_amount=Decimal("30000.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("460700.00"),
        raw_description="CAPEX HEAVY MACHINERY PURCHASE",
        is_classified=True,
        classification_category="CAPEX_EQUIPMENT",
    )
    t_loan = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 27),
        value_date=datetime(2026, 8, 27),
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("100000.00"),
        currency="EGP",
        running_balance=Decimal("560700.00"),
        raw_description="WORKING CAPITAL LOAN_DRAWDOWN TRANCHE",
        is_classified=True,
        classification_category="LOAN_DRAWDOWN",
    )

    # 4) Cross-Bank Liquidity Sweep: -60,150 from Bank 1 (150 corridor fee) -> +60,000 into Bank 2 (0 float!)
    t_swp_out = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 29),
        value_date=datetime(2026, 8, 29),
        debit_amount=Decimal("60150.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("500550.00"),
        raw_description="RTGS LIQUIDITY SWEEP TO BANK 2",
        is_reconciled=True,
        is_classified=True,
        classification_category="INTER_BANK_SWEEP",
        variance_amount=Decimal("150.00"),
    )
    t_swp_in = BankTransaction(
        statement_id=s2.id,
        account_number="ACC-B2-EGP",
        booking_date=datetime(2026, 8, 29),
        value_date=datetime(2026, 8, 29),
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("60000.00"),
        currency="EGP",
        running_balance=Decimal("260000.00"),
        raw_description="RTGS LIQUIDITY SWEEP FROM BANK 1",
        is_reconciled=True,
        is_classified=True,
        classification_category="INTER_BANK_SWEEP",
    )
    # Clean 0-fee, 0-float customer collection on Bank 2 so Bank 2 earns Grade A
    t_b2_clean = BankTransaction(
        statement_id=s2.id,
        account_number="ACC-B2-EGP",
        booking_date=datetime(2026, 8, 15),
        value_date=datetime(2026, 8, 15),
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("50000.00"),
        currency="EGP",
        running_balance=Decimal("250000.00"),
        raw_description="INSTAPAY COLLECTION VODAFONE",
        is_classified=True,
        classification_category="COLLECTION_FROM_CUSTOMER",
    )

    # 5) Paired Same-Day Error Reversal on Bank 1 (-19,500 and +19,500) -> 0.00 Cash Flow Distortion
    t_rev_dr = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 30),
        value_date=datetime(2026, 8, 30),
        debit_amount=Decimal("19500.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("481050.00"),
        raw_description="FAILED WIRE IBAN CLOSED",
        is_reversal=False,  # Deliberately test mixed is_reversal=False paired with True!
    )
    t_rev_cr = BankTransaction(
        statement_id=s1.id,
        account_number="ACC-B1-EGP",
        booking_date=datetime(2026, 8, 30),
        value_date=datetime(2026, 8, 30),
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("19500.00"),
        currency="EGP",
        running_balance=Decimal("500000.00"),
        raw_description="RETURN REVERSAL FAILED WIRE",
        is_reversal=True,
    )

    db.add_all([t_ar1, t_ar2, t_ap, t_pay, t_fee, t_capex, t_loan, t_swp_out, t_swp_in, t_b2_clean, t_rev_dr, t_rev_cr])
    db.flush()
    t_swp_out.linked_txn_id = t_swp_in.id
    t_swp_in.linked_txn_id = t_swp_out.id
    t_rev_dr.linked_txn_id = t_rev_cr.id
    t_rev_cr.linked_txn_id = t_rev_dr.id

    # Historical Reconciled ERP Records for "Orange Egypt" (lags: 11d and 13d -> avg 12d)
    erp_h1 = InternalLedgerRecord(
        company_id=company_id,
        record_type="AR_INVOICE",
        reference_number="INV-H1",
        entity_name="Orange Egypt",
        record_date=datetime(2026, 8, 1),
        amount=Decimal("120000.00"),
        currency="EGP",
        status="RECONCILED",
        matched_bank_txn_id=t_ar1.id,
    )
    erp_h2 = InternalLedgerRecord(
        company_id=company_id,
        record_type="AR_INVOICE",
        reference_number="INV-H2",
        entity_name="Orange Egypt",
        record_date=datetime(2026, 8, 5),
        amount=Decimal("80000.00"),
        currency="EGP",
        status="RECONCILED",
        matched_bank_txn_id=t_ar2.id,
    )
    db.add_all([erp_h1, erp_h2])
    db.flush()

    db.add_all([
        ReconciliationMatch(
            bank_txn_id=t_ar1.id,
            source_type="AR_INVOICE",
            source_record_id=erp_h1.id,
            match_type="1:1",
            match_logic="EXACT",
            created_by=user_id,
        ),
        ReconciliationMatch(
            bank_txn_id=t_ar2.id,
            source_type="AR_INVOICE",
            source_record_id=erp_h2.id,
            match_type="1:1",
            match_logic="EXACT",
            created_by=user_id,
        ),
    ])

    # OPEN ERP Items for 30-Day Forecast (Anchor Date = 2026-08-31):
    # A) OPEN Invoice for "Orange Egypt" recorded 2026-08-29 (+90,000 EGP):
    #    With learned 12d lag -> predicted clearing = 2026-09-10 (+10d, lands in 14d bucket, NOT 7d!)
    erp_open_orange = InternalLedgerRecord(
        company_id=company_id,
        record_type="AR_INVOICE",
        reference_number="INV-OPEN-ORANGE",
        entity_name="Orange Egypt",
        record_date=datetime(2026, 8, 29),
        amount=Decimal("90000.00"),
        currency="EGP",
        status="OPEN",
    )
    # B) OPEN AP Bill with explicit ERP due_date = 2026-09-05 (+5d -> lands in 7d bucket!) & formula-injection entity name
    erp_open_ap = InternalLedgerRecord(
        company_id=company_id,
        record_type="AP_BILL",
        reference_number="BILL-DUE-05",
        entity_name="=CMD|' /C calc'!A0 Supplier",
        record_date=datetime(2026, 8, 28),
        amount=Decimal("-40000.00"),
        currency="EGP",
        custom_fields_json={"due_date": "2026-09-05"},
        status="OPEN",
    )
    # C) Aged Overdue OPEN AR Invoice recorded 45 days before anchor (+20,000 EGP -> 50% haircut in baseline, 0% in stress!)
    erp_overdue_ar = InternalLedgerRecord(
        company_id=company_id,
        record_type="AR_INVOICE",
        reference_number="INV-OLD-45D",
        entity_name="Slow Payer Co",
        record_date=datetime(2026, 7, 15),
        amount=Decimal("20000.00"),
        currency="EGP",
        status="OPEN",
    )
    db.add_all([erp_open_orange, erp_open_ap, erp_overdue_ar])
    db.commit()
    return s1, s2


def test_ias7_cash_flow_learned_forecast_and_scorecard_sqlite():
    db = _make_sqlite_db()
    try:
        _seed_phase3_scenario(db)
        rep = reconciliation_cash_intelligence_service.build_cash_intelligence_report(
            db=db,
            customer_id=99,
            currency="EGP",
            minimum_cash_buffer=750000.0,
            benchmark_annual_rate_pct=20.0,
        )

        # 1) Multi-currency strip check (EGP = 760,000 across 2 accts; USD = 50,000)
        strip_map = {x["currency"]: x["verified_cash_balance"] for x in rep["multi_currency_cash_strip"]}
        assert strip_map["EGP"] == 760000.0
        assert strip_map["USD"] == 50000.0

        # 2) Pillar 1: IAS 7 Direct Cash Flow Math
        cf = rep["direct_cash_flow"]
        # Operating Inflows = 120,000 + 80,000 + 50,000 (Vodafone on B2) = 250,000.00
        assert Decimal(str(cf["operating_inflows_total"])) == Decimal("250000.00")
        # Operating Outflows = 80,000 (AP) + 50,000 (Payroll) + 400 (Direct Fee) + 150 (Sweep Corridor Fee) = 130,550.00
        assert Decimal(str(cf["operating_outflows_total"])) == Decimal("130550.00")
        assert Decimal(str(cf["net_operating_cash_flow"])) == Decimal("119450.00")
        assert Decimal(str(cf["net_investing_cash_flow"])) == Decimal("-30000.00")
        assert Decimal(str(cf["net_financing_cash_flow"])) == Decimal("100000.00")
        # Principal sweeps (+60,000 and -60,000) net to 0.00!
        assert Decimal(str(cf["net_interbank_sweeps"])) == Decimal("0.00")
        assert cf["excluded_paired_reversals_count"] == 2

        # 3) Pillar 2: 7/14/30-Day Liquidity Forecast & Learned Lag AI
        lf = rep["liquidity_forecast"]
        assert Decimal(str(lf["verified_starting_cash"])) == Decimal("760000.00")
        orange_prof = next(p for p in lf["counterparty_lag_profiles"] if p["entity_name"] == "Orange Egypt")
        assert orange_prof["avg_clearing_lag_days"] == 12

        # Day +7: AP Bill (-40,000) clears on Day +5; Orange (+90,000) is NOT due until Day +10!
        # => Day +7 Baseline Cash = 760,000 - 40,000 = 720,000.00 (< 750,000 buffer -> BUFFER_BREACH_WARNING!)
        assert Decimal(str(lf["horizons"]["7d"]["expected_inflows"])) == Decimal("0.00")
        assert Decimal(str(lf["horizons"]["7d"]["committed_outflows"])) == Decimal("40000.00")
        assert Decimal(str(lf["horizons"]["7d"]["projected_ending_cash"])) == Decimal("720000.00")
        assert lf["liquidity_status"] == "BUFFER_BREACH_WARNING"

        # Day +14: Orange (+90,000 on Day +10) + Overdue AR (+20,000 * 50% haircut = +10,000 on Day +14) = +100,000 inflows!
        # => Day +14 Baseline Cash = 760,000 + 100,000 - 40,000 = 820,000.00!
        assert Decimal(str(lf["horizons"]["14d"]["expected_inflows"])) == Decimal("100000.00")
        assert Decimal(str(lf["horizons"]["14d"]["projected_ending_cash"])) == Decimal("820000.00")
        # Stress Case at Day +30: Orange gets 85% weight (+76,500) and Overdue AR gets 0% (0.00) -> 760,000 + 76,500 - 40,000 = 796,500.00
        assert Decimal(str(lf["horizons"]["30d"]["stress_ending_cash"])) == Decimal("796500.00")

        # 4) Pillar 3: Bank Fee & Float Scorecard
        banks_by_id = {b["bank_id"]: b for b in rep["bank_scorecard"]["banks"]}
        b1 = banks_by_id[1]
        b2 = banks_by_id[2]
        assert Decimal(str(b1["direct_fees_amount"])) == Decimal("400.00")
        assert Decimal(str(b1["embedded_fees_amount"])) == Decimal("150.00")
        assert b1["avg_inflow_float_days"] > 0
        assert b1["imputed_float_cost"] > 0
        assert b2["efficiency_grade"] == "A"
        assert b2["efficiency_score"] > b1["efficiency_score"]

        # 5) 4-Sheet Excel Export + Formula-Injection Neutralization
        xlsx_bytes = reconciliation_cash_intelligence_service.render_excel_bytes(rep)
        df_queue = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name="3_Upcoming_AR_AP_Lags")
        ap_entity_cell = str(df_queue[df_queue["reference_number"] == "BILL-DUE-05"]["entity_name"].iloc[0])
        assert ap_entity_cell.startswith("'=CMD"), f"Formula injection must be neutralized, got: {ap_entity_cell}"
        print("[PASS] test_ias7_cash_flow_learned_forecast_and_scorecard_sqlite passed")
    finally:
        db.close()


def test_phase3_http_live_snapshot_override_and_pdf_e2e():
    from _test_harness import (
        IsolatedTenantFixture,
        SessionLocal,
        app,
        extract_pdf_info,
        make_auth_headers,
    )
    from app.models.models_inbox import CashBalanceSnapshot

    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="P3_CASH")
    client = TestClient(app)
    created_snap_id = None
    try:
        admin_h = make_auth_headers(fx.admin_user, extra_claims={"has_reconciliation_module": True})
        beta_h = make_auth_headers(fx.beta_admin, extra_claims={"has_reconciliation_module": True})

        _seed_phase3_scenario(
            db,
            company_id=fx.cust_alpha.id,
            bank1_id=fx.bank_cib.id,
            bank2_id=fx.bank_hsbc.id,
            user_id=fx.admin_user.id,
        )

        # Insert a newer live Smart Inbox CashBalanceSnapshot for Bank 1 EGP on 2026-09-02 (540,000 vs Aug 31 500,000!)
        snap = CashBalanceSnapshot(
            customer_id=fx.cust_alpha.id,
            bank_id=fx.bank_cib.id,
            account_number="ACC-B1-EGP",
            currency_code="EGP",
            available_balance=Decimal("540000.00"),
            ledger_balance=Decimal("540000.00"),
            as_of_datetime=datetime(2026, 9, 2, 10, 0, 0),
            source_channel="EMAIL_BODY",
        )
        db.add(snap)
        db.commit()
        created_snap_id = snap.id

        # 1. GET /cash-intelligence -> Starting cash must reflect 540,000 (live snapshot) + 260,000 (Bank 2) = 800,000.00!
        r = client.get("/api/v1/reconciliation/cash-intelligence?currency=EGP", headers=admin_h)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["liquidity_forecast"]["verified_starting_cash"] == 800000.0

        # 2. Cross-tenant isolation check (Beta sees 0.00 starting cash)
        r_beta = client.get("/api/v1/reconciliation/cash-intelligence?currency=EGP", headers=beta_h)
        assert r_beta.status_code == 200
        assert r_beta.json()["liquidity_forecast"]["verified_starting_cash"] == 0.0

        # 3. Export WeasyPrint CFO Executive Briefing PDF & verify rendered text
        r_pdf = client.get("/api/v1/reconciliation/cash-intelligence/export?format=PDF&currency=EGP", headers=admin_h)
        assert r_pdf.status_code == 200, r_pdf.text
        assert r_pdf.content.startswith(b"%PDF")
        pdf_info = extract_pdf_info(r_pdf.content)
        assert pdf_info["page_count"] >= 1
        assert "800,000.00" in pdf_info["text"]
        assert "119,450.00" in pdf_info["text"]

        # 4. Export 4-Sheet Excel Workbook via HTTP
        r_xlsx = client.get("/api/v1/reconciliation/cash-intelligence/export?format=XLSX&currency=EGP", headers=admin_h)
        assert r_xlsx.status_code == 200
        xl = pd.ExcelFile(io.BytesIO(r_xlsx.content))
        assert set(xl.sheet_names) == {
            "1_Direct_Cash_Flow_IAS7",
            "2_30Day_Liquidity_Forecast",
            "3_Upcoming_AR_AP_Lags",
            "4_Bank_Fee_Float_Scorecard",
        }
        print("[PASS] test_phase3_http_live_snapshot_override_and_pdf_e2e passed")
    finally:
        if created_snap_id:
            try:
                db.query(CashBalanceSnapshot).filter(CashBalanceSnapshot.id == created_snap_id).delete()
                db.commit()
            except Exception:
                db.rollback()
        fx.cleanup()
        db.close()


if __name__ == "__main__":
    test_ias7_cash_flow_learned_forecast_and_scorecard_sqlite()
    test_phase3_http_live_snapshot_override_and_pdf_e2e()
    print("ALL_PHASE3_CASH_INTELLIGENCE_TESTS_PASSED")
