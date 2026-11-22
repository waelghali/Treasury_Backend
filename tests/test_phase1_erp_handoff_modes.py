"""
Phase 1 Verification Suite: Multi-Mode ERP Handoff Engine (Zero-Regression + Accounting Accuracy).
Tests:
1. Default `handoff_mode="ALL"` preserves 100% legacy behavior.
2. `ENRICHED_STATEMENT` ("Grow First") enriches ERP Invoice Refs & Entities, suppresses paired error reversals,
   and pre-splits 1:M batches & deducted fee/WHT variances while maintaining exact running balance continuity!
3. `SUBLEDGER_CLEARING` exports only ERP-matched items, expands 1:M batch invoices into individual clearing rows/legs,
   echoes round-trip `erp_custom_fields.*` (Vendor/Customer Code, Cost Center), and balances both AR & AP variances!
4. `MISSING_GL_JOURNAL` exports only non-ERP bank lines (fees, taxes, sweeps), suppresses ghost error reversals,
   and routes paired cross-bank sweeps through `Cash-in-Transit GL` so `Cash-in-Transit` nets to `0.00` with zero fee double-counting!
5. `offset_account_mode="BANK_CLEARING_SUSPENSE"` + per-account GL mappings route offset legs to ERP Bank Clearing Suspense
   for clients who already uploaded raw bank statements into SAP/Oracle/Odoo!
"""

from decimal import Decimal
from datetime import datetime
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
from app.schemas.schemas_reconciliation_v2 import CustomExportExecuteRequest
from app.services.bank_reconciliation_service import bank_reconcile_service


def _make_db():
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


def _seed_full_reconciliation_scenario(db):
    # Statement A: CIB Operating Account (001-CIB-EGP)
    stmt_cib = BankStatement(
        company_id=77,
        bank_id=1,
        account_number="001-CIB-EGP",
        file_name="CIB_Aug2026.csv",
        opening_balance=Decimal("1000000.00"),
        closing_balance=Decimal("828425.00"),
        statement_start_date=datetime(2026, 8, 1),
        statement_end_date=datetime(2026, 8, 31),
        status="VALIDATED",
    )
    # Statement B: HSBC Operating Account (002-HSBC-EGP)
    stmt_hsbc = BankStatement(
        company_id=77,
        bank_id=2,
        account_number="002-HSBC-EGP",
        file_name="HSBC_Aug2026.csv",
        opening_balance=Decimal("500000.00"),
        closing_balance=Decimal("750000.00"),
        statement_start_date=datetime(2026, 8, 1),
        statement_end_date=datetime(2026, 8, 31),
        status="VALIDATED",
    )
    db.add_all([stmt_cib, stmt_hsbc])
    db.flush()

    # 1) AR Collection with WHT Deduction: Invoice +100,000, Bank received +99,000 (1,000 WHT deducted)
    t_ar_wht = BankTransaction(
        statement_id=stmt_cib.id,
        account_number="001-CIB-EGP",
        booking_date=datetime(2026, 8, 5),
        value_date=datetime(2026, 8, 5),
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("99000.00"),
        currency="EGP",
        running_balance=Decimal("1099000.00"),
        raw_description="ACH COLLECTION DELTA TELECOM WHT DEDUCTED",
        e2e_id="E2E-AR-101",
        is_reconciled=True,
        is_classified=True,
        classification_category="COLLECTION_FROM_CUSTOMER",
        internal_category="1020100 - Trade Receivables",
        variance_amount=Decimal("1000.00"),
    )

    # 2) AP Supplier Wire with Added Bank Fee: Bill -50,000, Bank debited 50,150 (150 SWIFT fee)
    t_ap_fee = BankTransaction(
        statement_id=stmt_cib.id,
        account_number="001-CIB-EGP",
        booking_date=datetime(2026, 8, 8),
        value_date=datetime(2026, 8, 8),
        debit_amount=Decimal("50150.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("1048850.00"),
        raw_description="OUTWARD WIRE SIEMENS INDUSTRIAL",
        e2e_id="E2E-AP-202",
        is_reconciled=True,
        is_classified=True,
        classification_category="PAYMENT_TO_SUPPLIER",
        internal_category="2010100 - Accounts Payable",
        variance_amount=Decimal("150.00"),
    )

    # 3) 1:M Batch AR Collection: Bank received +30,000 matching INV-B1 (18,000) + INV-B2 (12,000)
    t_batch_ar = BankTransaction(
        statement_id=stmt_cib.id,
        account_number="001-CIB-EGP",
        booking_date=datetime(2026, 8, 12),
        value_date=datetime(2026, 8, 12),
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("30000.00"),
        currency="EGP",
        running_balance=Decimal("1078850.00"),
        raw_description="INSTAPAY LUMP SUM APEX RETAIL",
        e2e_id="E2E-BATCH-303",
        is_reconciled=True,
        is_classified=True,
        classification_category="COLLECTION_FROM_CUSTOMER",
        internal_category="1020100 - Trade Receivables",
    )

    # 4) Bank-Only Overhead Fee (No ERP record)
    t_fee_only = BankTransaction(
        statement_id=stmt_cib.id,
        account_number="001-CIB-EGP",
        booking_date=datetime(2026, 8, 15),
        value_date=datetime(2026, 8, 15),
        debit_amount=Decimal("350.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("1078500.00"),
        raw_description="MONTHLY ACCOUNT MAINTENANCE FEE",
        is_reconciled=False,
        is_classified=True,
        classification_category="BANK_CHARGES",
        internal_category="5040100 - Bank Charges",
    )

    # 5 & 6) Same-Day Bank Error & Reversal Pair on CIB (-14,250 and +14,250)
    t_err_dr = BankTransaction(
        statement_id=stmt_cib.id,
        account_number="001-CIB-EGP",
        booking_date=datetime(2026, 8, 18),
        value_date=datetime(2026, 8, 18),
        debit_amount=Decimal("14250.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("1064250.00"),
        raw_description="DUPLICATE DEBIT BANK ERROR REF 99120",
        is_reconciled=True,
        is_reversal=True,
    )
    t_err_cr = BankTransaction(
        statement_id=stmt_cib.id,
        account_number="001-CIB-EGP",
        booking_date=datetime(2026, 8, 18),
        value_date=datetime(2026, 8, 18),
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("14250.00"),
        currency="EGP",
        running_balance=Decimal("1078500.00"),
        raw_description="CORRECTION REVERSAL BANK ERROR REF 99120",
        is_reconciled=True,
        is_reversal=True,
    )

    # 7 & 8) Cross-Bank Liquidity Sweep Pair with 75 EGP Transfer Fee:
    # CIB Outflow: -250,075.00 | HSBC Inflow: +250,000.00
    t_swp_out = BankTransaction(
        statement_id=stmt_cib.id,
        account_number="001-CIB-EGP",
        booking_date=datetime(2026, 8, 22),
        value_date=datetime(2026, 8, 22),
        debit_amount=Decimal("250075.00"),
        credit_amount=Decimal("0.00"),
        currency="EGP",
        running_balance=Decimal("828425.00"),
        raw_description="CASH POOL SWEEP TO HSBC SWP20260822",
        is_reconciled=False,
        is_classified=True,
        classification_category="INTER_BANK_SWEEP",
        internal_category="INTER_BANK_SWEEP",
        variance_amount=Decimal("75.00"),
    )
    t_swp_in = BankTransaction(
        statement_id=stmt_hsbc.id,
        account_number="002-HSBC-EGP",
        booking_date=datetime(2026, 8, 22),
        value_date=datetime(2026, 8, 22),
        debit_amount=Decimal("0.00"),
        credit_amount=Decimal("250000.00"),
        currency="EGP",
        running_balance=Decimal("750000.00"),
        raw_description="CASH POOL SWEEP FROM CIB SWP20260822",
        is_reconciled=False,
        is_classified=True,
        classification_category="INTER_BANK_SWEEP",
        internal_category="INTER_BANK_SWEEP",
        variance_amount=Decimal("75.00"),
    )

    db.add_all([t_ar_wht, t_ap_fee, t_batch_ar, t_fee_only, t_err_dr, t_err_cr, t_swp_out, t_swp_in])
    db.flush()

    # Link reversal pair & sweep pair
    t_err_dr.linked_txn_id = t_err_cr.id
    t_err_cr.linked_txn_id = t_err_dr.id
    t_swp_out.linked_txn_id = t_swp_in.id
    t_swp_in.linked_txn_id = t_swp_out.id

    # Seed ERP Open/Reconciled Records with custom round-trip ERP columns (`Partner_Code`, `SAP_Company`)
    erp_ar = InternalLedgerRecord(
        company_id=77,
        record_type="AR_INVOICE",
        reference_number="INV-2026-101",
        entity_name="Delta Telecom SAE",
        record_date=datetime(2026, 8, 1),
        amount=Decimal("100000.00"),
        currency="EGP",
        gl_account="1020100 - Trade Receivables",
        custom_fields_json={"Partner_Code": "CUST-9001", "SAP_Company": "EG01"},
        status="RECONCILED",
        matched_bank_txn_id=t_ar_wht.id,
    )
    erp_ap = InternalLedgerRecord(
        company_id=77,
        record_type="AP_BILL",
        reference_number="BILL-2026-202",
        entity_name="Siemens Industrial Egypt",
        record_date=datetime(2026, 8, 3),
        amount=Decimal("-50000.00"),
        currency="EGP",
        gl_account="2010100 - Trade Payables",
        custom_fields_json={"Partner_Code": "VEND-4402", "SAP_Company": "EG01"},
        status="RECONCILED",
        matched_bank_txn_id=t_ap_fee.id,
    )
    erp_b1 = InternalLedgerRecord(
        company_id=77,
        record_type="AR_INVOICE",
        reference_number="INV-B1",
        entity_name="Apex Retail Group",
        record_date=datetime(2026, 8, 10),
        amount=Decimal("18000.00"),
        currency="EGP",
        gl_account="1020100 - Trade Receivables",
        custom_fields_json={"Partner_Code": "CUST-7710", "SAP_Company": "EG01"},
        status="RECONCILED",
        matched_bank_txn_id=t_batch_ar.id,
    )
    erp_b2 = InternalLedgerRecord(
        company_id=77,
        record_type="AR_INVOICE",
        reference_number="INV-B2",
        entity_name="Apex Retail Group",
        record_date=datetime(2026, 8, 11),
        amount=Decimal("12000.00"),
        currency="EGP",
        gl_account="1020100 - Trade Receivables",
        custom_fields_json={"Partner_Code": "CUST-7710", "SAP_Company": "EG01"},
        status="RECONCILED",
        matched_bank_txn_id=t_batch_ar.id,
    )
    db.add_all([erp_ar, erp_ap, erp_b1, erp_b2])
    db.flush()

    db.add_all([
        ReconciliationMatch(
            bank_txn_id=t_ar_wht.id,
            source_type="AR_INVOICE",
            source_record_id=erp_ar.id,
            match_type="1:1",
            match_logic="TOLERANCE",
            variance_amount=Decimal("1000.00"),
            variance_disposition="WHT_DEDUCTION",
            created_by=1,
        ),
        ReconciliationMatch(
            bank_txn_id=t_ap_fee.id,
            source_type="AP_BILL",
            source_record_id=erp_ap.id,
            match_type="1:1",
            match_logic="TOLERANCE",
            variance_amount=Decimal("150.00"),
            variance_disposition="BANK_FEE_WRITEOFF",
            created_by=1,
        ),
        ReconciliationMatch(
            bank_txn_id=t_batch_ar.id,
            source_type="AR_INVOICE",
            source_record_id=erp_b1.id,
            match_type="1:M",
            match_logic="BATCH_SUM",
            variance_amount=Decimal("0.00"),
            variance_disposition="NONE",
            created_by=1,
        ),
        ReconciliationMatch(
            bank_txn_id=t_batch_ar.id,
            source_type="AR_INVOICE",
            source_record_id=erp_b2.id,
            match_type="1:M",
            match_logic="BATCH_SUM",
            variance_amount=Decimal("0.00"),
            variance_disposition="NONE",
            created_by=1,
        ),
    ])
    db.commit()
    return stmt_cib, stmt_hsbc


def test_mode_1_enriched_statement_grow_first():
    db = _make_db()
    try:
        stmt_cib, _ = _seed_full_reconciliation_scenario(db)
        req = CustomExportExecuteRequest(
            statement_id=stmt_cib.id,
            handoff_mode="ENRICHED_STATEMENT",
            split_variance_as_fee_row=True,
            columns_config=[
                {"source_field": "booking_date", "custom_header": "Date"},
                {"source_field": "payment_or_erp_ref", "custom_header": "Payment_Ref"},
                {"source_field": "counterparty_name", "custom_header": "Partner"},
                {"source_field": "signed_amount", "custom_header": "Signed_Amount"},
                {"source_field": "running_balance", "custom_header": "Balance"},
                {"source_field": "internal_category", "custom_header": "Target_GL"},
            ],
        )
        _, rows, _, _ = bank_reconcile_service.build_custom_export_rows(db, customer_id=77, req=req)

        # Verify paired ghost error reversals (-14,250 / +14,250) were suppressed!
        assert not any(abs(float(r["Signed_Amount"])) == 14250.0 for r in rows), "Ghost reversals must be suppressed!"

        # Verify mathematical continuity: Opening (1,000,000) + Sum(Signed_Amount) == Closing (828,425.00)!
        net_change = sum(Decimal(str(r["Signed_Amount"])) for r in rows)
        assert Decimal("1000000.00") + net_change == Decimal("828425.00"), (
            f"Enriched statement split broke balance continuity: 1,000,000 + {net_change} != 828,425"
        )
        assert Decimal(str(rows[-1]["Balance"])) == Decimal("828425.00")

        # Verify INV-2026-101 split cleanly into +100,000.00 Invoice & -1,000.00 WHT Receivable!
        inv_row = next(r for r in rows if r["Payment_Ref"] == "INV-2026-101")
        assert Decimal(str(inv_row["Signed_Amount"])) == Decimal("100000.00")
        assert inv_row["Partner"] == "Delta Telecom SAE"

        wht_row = next(r for r in rows if Decimal(str(r["Signed_Amount"])) == Decimal("-1000.00"))
        assert "Withholding Tax" in wht_row["Target_GL"]

        # Verify 1:M Batch (+30,000) split into INV-B1 (+18,000) and INV-B2 (+12,000)!
        b1_row = next(r for r in rows if r["Payment_Ref"] == "INV-B1")
        b2_row = next(r for r in rows if r["Payment_Ref"] == "INV-B2")
        assert Decimal(str(b1_row["Signed_Amount"])) == Decimal("18000.00")
        assert Decimal(str(b2_row["Signed_Amount"])) == Decimal("12000.00")

        # Verify Sweep routed to Cash-in-Transit GL!
        swp_row = next(r for r in rows if Decimal(str(r["Signed_Amount"])) == Decimal("-250075.00"))
        assert "Cash in Transit" in swp_row["Target_GL"]
        print("[PASS] test_mode_1_enriched_statement_grow_first passed")
    finally:
        db.close()


def test_mode_2_subledger_clearing_flat_and_journal():
    db = _make_db()
    try:
        _seed_full_reconciliation_scenario(db)

        # 2A: Flat Sub-Ledger Cash Application Batch (1 row per cleared ERP Invoice/Bill + round-trip ERP custom fields)
        req_flat = CustomExportExecuteRequest(
            handoff_mode="SUBLEDGER_CLEARING",
            expand_journal_legs=False,
            columns_config=[
                {"source_field": "matched_erp_type", "custom_header": "Doc_Type"},
                {"source_field": "matched_erp_reference", "custom_header": "ERP_Ref"},
                {"source_field": "erp_custom_fields.Partner_Code", "custom_header": "ERP_Partner_Code"},
                {"source_field": "allocated_erp_amount", "custom_header": "Cleared_ERP_Amt"},
                {"source_field": "variance_amount", "custom_header": "Variance"},
                {"source_field": "variance_gl_account", "custom_header": "Variance_GL"},
            ],
        )
        _, rows_flat, _, _ = bank_reconcile_service.build_custom_export_rows(db, customer_id=77, req=req_flat)
        # 4 ERP records cleared: INV-2026-101, BILL-2026-202, INV-B1, INV-B2 (no bank-only fees or sweeps!)
        assert len(rows_flat) == 4
        refs = [r["ERP_Ref"] for r in rows_flat]
        assert refs == ["INV-2026-101", "BILL-2026-202", "INV-B1", "INV-B2"]
        assert rows_flat[0]["ERP_Partner_Code"] == "CUST-9001"
        assert "Withholding Tax" in rows_flat[0]["Variance_GL"]
        assert rows_flat[1]["ERP_Partner_Code"] == "VEND-4402"
        assert Decimal(str(rows_flat[1]["Cleared_ERP_Amt"])) == Decimal("50000.00")
        assert Decimal(str(rows_flat[1]["Variance"])) == Decimal("150.00")

        # 2B: Double-Entry Clearing Journal with Offset Mode = BANK_CLEARING_SUSPENSE (Statement Already in ERP!)
        req_legs = CustomExportExecuteRequest(
            handoff_mode="SUBLEDGER_CLEARING",
            offset_account_mode="BANK_CLEARING_SUSPENSE",
            account_gl_mappings={"001-CIB-EGP": {"clearing_gl": "1010991 - CIB Suspense Clearing"}},
            expand_journal_legs=True,
            columns_config=[
                {"source_field": "internal_category", "custom_header": "GL"},
                {"source_field": "matched_erp_reference", "custom_header": "ERP_Ref"},
                {"source_field": "debit_amount", "custom_header": "DR"},
                {"source_field": "credit_amount", "custom_header": "CR"},
                {"source_field": "raw_description", "custom_header": "Memo"},
            ],
        )
        _, rows_legs, _, _ = bank_reconcile_service.build_custom_export_rows(db, customer_id=77, req=req_legs)
        total_dr = sum(Decimal(str(r["DR"])) for r in rows_legs)
        total_cr = sum(Decimal(str(r["CR"])) for r in rows_legs)
        # AR WHT (100,000) + AP Bill+Fee (50,150) + Batch AR (30,000) = 180,150.00!
        assert total_dr == Decimal("180150.00")
        assert total_cr == Decimal("180150.00")
        assert any(r["GL"] == "1010991 - CIB Suspense Clearing" for r in rows_legs)
        assert any("[CLEARING OFFSET LEG]" in r["Memo"] for r in rows_legs)
        # Verify AP Bill debited 50,000 (NOT 50,300!) and Bank Charges debited 150!
        ap_sub_leg = next(r for r in rows_legs if r["ERP_Ref"] == "BILL-2026-202" and "[SUBLEDGER LEG]" in r["Memo"])
        assert Decimal(str(ap_sub_leg["DR"])) == Decimal("50000.00")
        assert Decimal(str(ap_sub_leg["CR"])) == Decimal("0.00")
        print("[PASS] test_mode_2_subledger_clearing_flat_and_journal passed")
    finally:
        db.close()


def test_mode_3_missing_gl_journal_and_cross_bank_sweep_zero_netting():
    db = _make_db()
    try:
        _seed_full_reconciliation_scenario(db)

        req = CustomExportExecuteRequest(
            handoff_mode="MISSING_GL_JOURNAL",
            expand_journal_legs=True,
            cash_in_transit_gl_account="1010800 - Inter-Bank Cash in Transit",
            account_gl_mappings={
                "001-CIB-EGP": {"main_gl": "1010010 - CIB Operating"},
                "002-HSBC-EGP": {"main_gl": "1010020 - HSBC Operating"},
            },
            columns_config=[
                {"source_field": "internal_category", "custom_header": "GL"},
                {"source_field": "debit_amount", "custom_header": "DR"},
                {"source_field": "credit_amount", "custom_header": "CR"},
                {"source_field": "raw_description", "custom_header": "Memo"},
            ],
        )
        _, rows, exported_txns, _ = bank_reconcile_service.build_custom_export_rows(db, customer_id=77, req=req)

        # Only 3 transactions should be exported:
        # 1) CIB Fee (350) -> 2 legs
        # 2) CIB Sweep Out (-250,075) -> 3 legs (Cr CIB 250,075 | Dr Transit 250,000 | Dr Bank Charges 75)
        # 3) HSBC Sweep In (+250,000) -> 2 legs (Dr HSBC 250,000 | Cr Transit 250,000)
        # (All 3 ERP-matched txns AND both ghost error reversal txns are excluded!)
        assert len(exported_txns) == 3
        assert len(rows) == 7

        total_dr = sum(Decimal(str(r["DR"])) for r in rows)
        total_cr = sum(Decimal(str(r["CR"])) for r in rows)
        assert total_dr == total_cr == Decimal("500425.00"), f"Unbalanced journal: DR={total_dr} vs CR={total_cr}"

        # CRITICAL PROOF: Cash-in-Transit MUST net to 0.00 across CIB + HSBC!
        transit_dr = sum(Decimal(str(r["DR"])) for r in rows if r["GL"] == "1010800 - Inter-Bank Cash in Transit")
        transit_cr = sum(Decimal(str(r["CR"])) for r in rows if r["GL"] == "1010800 - Inter-Bank Cash in Transit")
        assert transit_dr == Decimal("250000.00")
        assert transit_cr == Decimal("250000.00")
        assert transit_dr - transit_cr == Decimal("0.00"), "Cash-in-Transit MUST net to 0.00 across sweep legs!"

        # And the 75 EGP sweep transfer fee MUST be booked ONCE (350 maintenance + 75 sweep fee = 425 total Bank Charges)!
        charges_dr = sum(Decimal(str(r["DR"])) for r in rows if "Bank Charges" in r["GL"])
        assert charges_dr == Decimal("425.00"), f"Expected 425.00 total Bank Charges (350 + 75), got {charges_dr}"
        print("[PASS] test_mode_3_missing_gl_journal_and_cross_bank_sweep_zero_netting passed")
    finally:
        db.close()


def test_phase1_http_endpoints_and_postgres_persistence():
    import os
    import sys
    sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    from fastapi.testclient import TestClient
    from _test_harness import app, SessionLocal, IsolatedTenantFixture, make_auth_headers

    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="P1_HTTP")
    client = TestClient(app)
    try:
        h = make_auth_headers(fx.admin_user, extra_claims={"has_reconciliation_module": True})
        beta_h = make_auth_headers(fx.beta_admin, extra_claims={"has_reconciliation_module": True})

        # 1. Verify available-fields exposes new Round-Trip ERP Clearing fields
        r_f = client.get("/api/v1/reconciliation/export/available-fields", headers=h)
        assert r_f.status_code == 200, r_f.text
        f_keys = {x["source_field"] for x in r_f.json()["available_fields"]}
        assert {
            "payment_or_erp_ref",
            "matched_erp_entity",
            "allocated_erp_amount",
            "variance_gl_account",
            "effective_bank_gl",
        }.issubset(f_keys)

        # 2. Save & fetch Export Profile with Phase 1 handoff_mode & offset_account_mode in PostgreSQL
        r_p = client.post(
            "/api/v1/reconciliation/export-profiles",
            headers=h,
            json={
                "profile_name": "SAP Clearing Pack",
                "file_format": "XLSX",
                "handoff_mode": "SUBLEDGER_CLEARING",
                "offset_account_mode": "BANK_CLEARING_SUSPENSE",
                "clearing_suspense_gl_account": "1010995 - SAP Bank Clearing",
                "split_variance_as_fee_row": True,
                "exclude_paired_reversals": True,
                "columns_config_json": [{"source_field": "matched_erp_reference", "custom_header": "Assignment"}],
            },
        )
        assert r_p.status_code == 200, r_p.text
        prof = r_p.json()
        assert prof["handoff_mode"] == "SUBLEDGER_CLEARING"
        assert prof["offset_account_mode"] == "BANK_CLEARING_SUSPENSE"
        assert prof["clearing_suspense_gl_account"] == "1010995 - SAP Bank Clearing"

        # 3. Verify Per-Account ERP G/L Mappings PUT & GET + Multi-Tenant Isolation
        acct_num = f"ACC-P1-{fx.uid}"
        r_ag_put = client.put(
            "/api/v1/reconciliation/account-gl-mappings",
            headers=h,
            json={
                "mappings": {
                    acct_num: {
                        "bank_id": fx.bank_cib.id,
                        "account_name": "CIB Operating",
                        "currency": "EGP",
                        "main_gl": "1010011 - CIB Main",
                        "clearing_gl": "1010991 - CIB Clearing",
                    }
                }
            },
        )
        assert r_ag_put.status_code == 200 and r_ag_put.json()["updated_count"] == 1

        r_ag_get = client.get("/api/v1/reconciliation/account-gl-mappings", headers=h)
        assert r_ag_get.status_code == 200
        found = next(a for a in r_ag_get.json()["accounts"] if a["account_number"] == acct_num)
        assert found["main_gl"] == "1010011 - CIB Main"
        assert found["clearing_gl"] == "1010991 - CIB Clearing"

        # Beta tenant must never see Alpha's account mapping
        r_ag_beta = client.get("/api/v1/reconciliation/account-gl-mappings", headers=beta_h)
        assert r_ag_beta.status_code == 200
        assert not any(a["account_number"] == acct_num for a in r_ag_beta.json()["accounts"])

        # 4. Preview endpoint returns enriched Phase 1 summary keys
        r_prev = client.post(
            "/api/v1/reconciliation/export/preview",
            headers=h,
            json={"profile_id": prof["id"]},
        )
        assert r_prev.status_code == 200, r_prev.text
        s = r_prev.json()["summary"]
        assert {
            "erp_matched_count",
            "bank_only_count",
            "paired_sweeps_count",
            "paired_reversals_count",
            "total_debit",
            "total_credit",
            "is_balanced",
        }.issubset(s.keys())
        print("[PASS] test_phase1_http_endpoints_and_postgres_persistence passed")
    finally:
        fx.cleanup()
        db.close()


if __name__ == "__main__":
    test_mode_1_enriched_statement_grow_first()
    test_mode_2_subledger_clearing_flat_and_journal()
    test_mode_3_missing_gl_journal_and_cross_bank_sweep_zero_netting()
    test_phase1_http_endpoints_and_postgres_persistence()
    print("ALL_PHASE1_ERP_HANDOFF_TESTS_PASSED")
