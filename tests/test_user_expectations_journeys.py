"""
Customer & Accountant Expectation Acceptance Suite
==================================================
Tests the Bank Statement Classification & Reconciliation module from the perspective
of a real Corporate Treasurer / Senior Accountant across 5 end-to-end business journeys:

1. Journey 1: "The Messy Bank File Reality"
   - 6 junk header rows, Arabic columns, accounting parentheses `(12,500.00)`,
     multi-line wrapped narratives, Day-First `DD/MM/YYYY` dates, and running balance gap detection.
2. Journey 2: "Month 1 Teaching -> Month 2 Zero-Touch Autonomy & Self-Healing"
   - User teaches custom categories/GLs in Month 1 and corrects a tricky "SALES COMMISSION" trap;
     verifies Month 2 statement is classified 100% automatically as the user expects.
3. Journey 3: "Standalone Multi-Bank Treasury Classification (No ERP File)"
   - Cross-bank liquidity sweep between CIB and NBE with 2-day Value Date clearing lag and
     100 EGP correspondent fee deducted, plus companion wire fee and a bounced cheque reversal pair.
4. Journey 4: "Real-World ERP Reconciliation Traps"
   - Identical-amount disambiguation (Hungarian assignment), net-of-fee wire settlement across
     Posting/Value date lag, and 3 ERP invoices paid in 1 lump-sum bank deposit.
5. Journey 5: "The Accountant's Balanced Double-Entry Export"
   - Custom column order, Arabic UTF-8 support, preserved custom columns, and verifying that
     expanded GL journal legs balance to the exact piastre (Total DR == Total CR).
"""

from datetime import datetime
from decimal import Decimal
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql import JSONB

compiles(JSONB, "sqlite")(lambda element, compiler, **kw: "JSON")

from app.database import Base
from app.models.models_reconciliation_v2 import (
    BankStatement,
    BankTransaction,
    ClassificationRule,
    Counterparty,
    InternalLedgerRecord,
    ReconciliationMatch,
)
from app.schemas.schemas_reconciliation_v2 import CustomExportExecuteRequest
from app.services.bank_reconciliation_service import bank_reconcile_service


def _create_fresh_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def test_journey_1_messy_arabic_and_multiline_statement_expectations():
    """
    Accountant Expectation:
    - Real bank exports have title rows at the top, Arabic headers, parenthesized negatives,
      multi-line descriptions where continuation rows have empty Date/Amount, and DD/MM/YYYY dates.
    - If a bank statement skips a 500 EGP row in the middle, the user expects a clear warning
      with the exact 500 EGP variance, NOT a crash and NOT silent ignorance.
    """
    db = _create_fresh_db()

    messy_csv = (
        "NATIONAL BANK OF EGYPT - CORPORATE TREASURY PORTAL\n"
        "Statement Generated: 2026-08-31,Branch: Cairo Main\n"
        "Customer: AL-AHRAM MANUFACTURING CO\n"
        "Account Number: 100200300400\n"
        "Opening Balance: \"100,000.00\"\n"
        "\n"
        "تاريخ القيد,تاريخ القيمة,مدين,دائن,الرصيد,بيان الحركة,مركز التكلفة\n"
        "05/08/2026,07/08/2026,\"(15,000.00)\",0.00,\"85,000.00\",تحويل سويفت للمورد شركة النيل للمعدات,CC-101\n"
        ",,,,,دفعة أولى عقد التوريد رقم 8821,\n"
        "06/08/2026,06/08/2026,0.00,\"25,000.00\",\"109,500.00\",تحصيل من العميل شركة الدلتا للتجارة,CC-202\n"
    ).encode("utf-8")

    stmt = bank_reconcile_service.process_ingestion(
        db=db,
        content=messy_csv,
        file_type="csv",
        company_id=1,
        user_id=1,
        overrides={"bank_id": 1, "file_name": "nbe_arabic_messy.csv"},
    )

    txns = db.query(BankTransaction).filter(BankTransaction.statement_id == stmt.id).order_by(BankTransaction.id.asc()).all()

    # 1. Must extract exactly 2 transactions (merging the continuation line into txn 1)
    assert len(txns) == 2, f"Expected 2 transactions after merging continuation row, got {len(txns)}"

    # 2. Multi-line narrative must be merged cleanly
    assert "شركة النيل للمعدات" in txns[0].raw_description
    assert "عقد التوريد رقم 8821" in txns[0].raw_description

    # 3. Parenthesized (15,000.00) in Debit column must be positive 15,000.00 debit_amount
    assert txns[0].debit_amount == Decimal("15000.00")
    assert txns[0].credit_amount == Decimal("0.00")

    # 4. DD/MM/YYYY (05/08/2026) must be August 5, 2026 (NOT May 8) and Value Date August 7, 2026
    assert txns[0].booking_date.strftime("%Y-%m-%d") == "2026-08-05"
    assert txns[0].value_date.strftime("%Y-%m-%d") == "2026-08-07"

    # 5. Custom Arabic column 'مركز التكلفة' must be preserved in custom_fields_json
    assert txns[0].custom_fields_json.get("مركز التكلفة") == "CC-101"

    # 6. Running balance gap (85,000 + 25,000 = 110,000 expected vs 109,500 reported -> -500 delta) must be flagged!
    assert stmt.balance_continuity_status == "CONTINUITY_WARNING"
    assert abs(stmt.balance_variance_amount) == Decimal("500.00")
    assert len(stmt.continuity_warnings_json or []) >= 1
    db.close()


def test_journey_2_month1_teaching_to_month2_autonomy_and_trap_avoidance():
    """
    Accountant Expectation:
    - In Month 1, the statement has:
      (a) A recurring warehouse rent wire: "ACH TRF OCTOBER LOGISTICS WAREHOUSE" (45,000 EGP)
      (b) A tricky 185,000 EGP payment: "DISTRIBUTOR SALES COMMISSION BATCH Q3" -> contains the word
          "COMMISSION", which a naive system would misclassify as BANK_CHARGES!
    - The accountant classifies (a) as "WAREHOUSE_RENT" (GL 5040100) and (b) as "SELLING_COMMISSION" (GL 5030200).
    - In Month 2, a new statement arrives with those same patterns on new dates and slightly different amounts.
    - Expectation: Month 2 must classify BOTH lines automatically into the exact customer categories & GLs!
    """
    db = _create_fresh_db()

    month1_csv = (
        "Posting Date,Value Date,Debit,Credit,Balance,Narrative\n"
        "2026-08-05,2026-08-05,45000.00,0.00,500000.00,ACH TRF OCTOBER LOGISTICS WAREHOUSE AUG\n"
        "2026-08-10,2026-08-10,185000.00,0.00,315000.00,DISTRIBUTOR SALES COMMISSION BATCH Q3\n"
        "2026-08-20,2026-08-20,45000.00,0.00,270000.00,ACH TRF OCTOBER LOGISTICS WAREHOUSE ANNEX\n"
    ).encode("utf-8")

    stmt_m1 = bank_reconcile_service.process_ingestion(
        db=db,
        content=month1_csv,
        file_type="csv",
        company_id=2,
        user_id=20,
        overrides={"bank_id": 1, "account_number": "ACC-M1", "file_name": "month1.csv"},
    )
    m1_txns = db.query(BankTransaction).filter(BankTransaction.statement_id == stmt_m1.id).order_by(BankTransaction.id.asc()).all()

    # Verify the 185,000 EGP "DISTRIBUTOR SALES COMMISSION" was NOT auto-confirmed as BANK_CHARGES!
    assert not (m1_txns[1].classification_category == "BANK_CHARGES" and m1_txns[1].auto_confirmed), (
        "185,000 EGP distributor commission should NEVER be auto-confirmed as small BANK_CHARGES!"
    )

    # Accountant teaches the system in Month 1:
    bank_reconcile_service.confirm_and_learn(
        db=db,
        transaction_id=m1_txns[0].id,
        customer_id=2,
        user_id=20,
        counterparty_name="OCTOBER LOGISTICS WAREHOUSE",
        entity_type="SUPPLIER",
        category="WAREHOUSE_RENT",
        gl_account="5040100 - Rent Expense",
    )
    bank_reconcile_service.confirm_and_learn(
        db=db,
        transaction_id=m1_txns[2].id,
        customer_id=2,
        user_id=20,
        counterparty_name="OCTOBER LOGISTICS WAREHOUSE",
        entity_type="SUPPLIER",
        category="WAREHOUSE_RENT",
        gl_account="5040100 - Rent Expense",
    )
    bank_reconcile_service.confirm_and_learn(
        db=db,
        transaction_id=m1_txns[1].id,
        customer_id=2,
        user_id=20,
        category="SELLING_COMMISSION",
        gl_account="5030200 - Selling & Distribution Commissions",
    )

    # Now Month 2 arrives!
    month2_csv = (
        "Posting Date,Value Date,Debit,Credit,Balance,Narrative\n"
        "2026-09-05,2026-09-05,47000.00,0.00,223000.00,ACH TRF OCTOBER LOGISTICS WAREHOUSE SEP\n"
        "2026-09-12,2026-09-12,195000.00,0.00,28000.00,DISTRIBUTOR SALES COMMISSION BATCH Q4\n"
    ).encode("utf-8")

    stmt_m2 = bank_reconcile_service.process_ingestion(
        db=db,
        content=month2_csv,
        file_type="csv",
        company_id=2,
        user_id=20,
        overrides={"bank_id": 1, "account_number": "ACC-M1", "file_name": "month2.csv"},
    )
    m2_txns = db.query(BankTransaction).filter(BankTransaction.statement_id == stmt_m2.id).order_by(BankTransaction.id.asc()).all()

    # Expectation: Month 2 automatically classifies BOTH into the user's taught categories & GLs!
    assert m2_txns[0].classification_category == "WAREHOUSE_RENT"
    assert m2_txns[0].internal_category == "5040100 - Rent Expense"
    assert m2_txns[0].auto_confirmed is True

    assert m2_txns[1].classification_category == "SELLING_COMMISSION"
    assert m2_txns[1].internal_category == "5030200 - Selling & Distribution Commissions"
    db.close()


def test_journey_3_standalone_multibank_sweeps_and_bounced_cheques_without_erp():
    """
    Treasurer Expectation (Standalone Mode — Zero ERP Records):
    - Company uploads Bank 1 (CIB) statement and Bank 2 (NBE) statement.
    - 500,000 EGP leaves CIB on Posting Date Aug 10 (Value Date Aug 12) as a transfer to NBE,
      with an adjacent 85 EGP SWIFT fee on CIB.
    - 499,900 EGP arrives in NBE on Posting Date Aug 12 (100 EGP correspondent fee deducted).
    - Also on CIB, a 40,000 EGP cheque is deposited on Aug 14 and bounces on Aug 17.
    - Expectation:
      1. The 500,000 CIB outflow and 499,900 NBE inflow are paired (`linked_txn_id`), classified as
         `INTER_BANK_SWEEP`, with `variance_amount = 100.00`.
      2. The 85 EGP CIB fee is attached to the 500,000 outflow (`companion_parent_id`).
      3. The 40,000 cheque deposit and its 40,000 bounced debit are paired (`linked_txn_id`),
         marked `is_reversal = True` and `is_reconciled = True`.
    """
    db = _create_fresh_db()

    cib_csv = (
        "Posting Date,Value Date,Debit,Credit,Balance,Description\n"
        "2026-08-10,2026-08-12,500000.00,0.00,1000000.00,SWEEP TRANSFER TO OWN ACCOUNT NBE TRF998877\n"
        "2026-08-10,2026-08-10,85.00,0.00,999915.00,CORRESPONDENT TRANSFER FEE\n"
        "2026-08-14,2026-08-14,0.00,40000.00,1039915.00,CHEQUE DEPOSIT CHQ991122 AL-AMAL CO\n"
        "2026-08-17,2026-08-17,40000.00,0.00,999915.00,RETURNED BOUNCED CHEQUE CHQ991122 AL-AMAL CO\n"
    ).encode("utf-8")

    nbe_csv = (
        "Posting Date,Value Date,Debit,Credit,Balance,Description\n"
        "2026-08-12,2026-08-12,0.00,499900.00,750000.00,SWEEP TRANSFER FROM CIB OWN ACCOUNT TRF998877\n"
    ).encode("utf-8")

    stmt_cib = bank_reconcile_service.process_ingestion(
        db=db,
        content=cib_csv,
        file_type="csv",
        company_id=3,
        user_id=30,
        overrides={"bank_id": 1, "account_number": "CIB-001", "file_name": "cib.csv"},
    )
    stmt_nbe = bank_reconcile_service.process_ingestion(
        db=db,
        content=nbe_csv,
        file_type="csv",
        company_id=3,
        user_id=30,
        overrides={"bank_id": 2, "account_number": "NBE-002", "file_name": "nbe.csv"},
    )

    cib_txns = db.query(BankTransaction).filter(BankTransaction.statement_id == stmt_cib.id).order_by(BankTransaction.id.asc()).all()
    nbe_txns = db.query(BankTransaction).filter(BankTransaction.statement_id == stmt_nbe.id).order_by(BankTransaction.id.asc()).all()

    # 1. Cross-bank sweep paired with 100.00 transit fee variance
    assert cib_txns[0].linked_txn_id == nbe_txns[0].id
    assert nbe_txns[0].linked_txn_id == cib_txns[0].id
    assert cib_txns[0].classification_category == "INTER_BANK_SWEEP"
    assert nbe_txns[0].classification_category == "INTER_BANK_SWEEP"
    assert cib_txns[0].variance_amount == Decimal("100.00")

    # 2. Companion 85 EGP fee attached to parent 500,000 sweep
    assert cib_txns[1].companion_parent_id == cib_txns[0].id
    assert cib_txns[1].classification_category == "BANK_CHARGES"

    # 3. Bounced cheque paired with original deposit and auto-reconciled
    assert cib_txns[2].linked_txn_id == cib_txns[3].id
    assert cib_txns[2].is_reversal is True
    assert cib_txns[3].is_reversal is True
    assert cib_txns[2].is_reconciled is True
    assert cib_txns[3].is_reconciled is True
    db.close()


def test_journey_4_erp_reconciliation_disambiguation_tolerance_and_batch():
    """
    Accountant Expectation when matching against ERP Ledger:
    1. Disambiguation: Two 25,000 EGP payments on the same day to 'MODERN PLASTICS' and 'GLOBAL PACKAGING'.
       Even if ERP lists GLOBAL PACKAGING first and MODERN PLASTICS second, the engine must never cross-match them!
    2. Deducted Bank Fee + Clearing Lag: ERP Invoice INV-700 for 100,000 EGP on Aug 15 for 'SINAI CEMENT'.
       Bank statement shows 99,910 EGP (90 EGP fee deducted) on Posting Date Aug 13, Value Date Aug 15.
       Must reconcile via Tolerance Pass with `variance_amount = 90.00`!
    3. Batch Deposit: 3 ERP invoices (10,000 + 15,000 + 5,000 = 30,000 EGP) paid in 1 bank wire of 30,000 EGP.
       Must reconcile all 3 ERP invoices (`1:M`, `BATCH_SUM`)!
    """
    db = _create_fresh_db()

    bank_csv = (
        "Posting Date,Value Date,Debit,Credit,Balance,Description\n"
        "2026-08-10,2026-08-10,25000.00,0.00,975000.00,WIRE PAYMENT TO MODERN PLASTICS CO\n"
        "2026-08-10,2026-08-10,25000.00,0.00,950000.00,WIRE PAYMENT TO GLOBAL PACKAGING LTD\n"
        "2026-08-13,2026-08-15,0.00,99910.00,1049910.00,COLLECTION FROM SINAI CEMENT INV-700\n"
        "2026-08-18,2026-08-18,0.00,30000.00,1079910.00,BATCH COLLECTION CAIRO RETAILERS\n"
    ).encode("utf-8")

    stmt = bank_reconcile_service.process_ingestion(
        db=db,
        content=bank_csv,
        file_type="csv",
        company_id=4,
        user_id=40,
        overrides={"bank_id": 1, "account_number": "ACC-ERP-4", "file_name": "erp_test.csv"},
    )

    # Ingest ERP records (note reverse order for the two 25,000 bills to test Hungarian disambiguation!)
    bank_reconcile_service.ingest_erp_records_bulk(
        db=db,
        records_data=[
            {"reference_number": "BILL-GP-01", "entity_name": "GLOBAL PACKAGING LTD", "amount": -25000.00, "record_date": "2026-08-10", "record_type": "AP_BILL"},
            {"reference_number": "BILL-MP-02", "entity_name": "MODERN PLASTICS CO", "amount": -25000.00, "record_date": "2026-08-10", "record_type": "AP_BILL"},
            {"reference_number": "INV-700", "entity_name": "SINAI CEMENT", "amount": 100000.00, "record_date": "2026-08-15", "record_type": "AR_INVOICE"},
            {"reference_number": "INV-B1", "entity_name": "CAIRO RETAILERS", "amount": 10000.00, "record_date": "2026-08-18", "record_type": "AR_INVOICE"},
            {"reference_number": "INV-B2", "entity_name": "CAIRO RETAILERS", "amount": 15000.00, "record_date": "2026-08-18", "record_type": "AR_INVOICE"},
            {"reference_number": "INV-B3", "entity_name": "CAIRO RETAILERS", "amount": 5000.00, "record_date": "2026-08-18", "record_type": "AR_INVOICE"},
        ],
        company_id=4,
        user_id=40,
    )

    res = bank_reconcile_service.run_matching_engine(db=db, customer_id=4, user_id=40, statement_id=stmt.id)
    assert res["pass1_optimal_1to1"] == 2
    assert res["pass2_tolerance_matched"] == 1
    assert res["pass3_batch_grouped"] == 1

    txns = db.query(BankTransaction).filter(BankTransaction.statement_id == stmt.id).order_by(BankTransaction.id.asc()).all()
    erps = {e.reference_number: e for e in db.query(InternalLedgerRecord).filter(InternalLedgerRecord.company_id == 4).all()}

    # Verify MODERN PLASTICS (txns[0]) matched BILL-MP-02 and GLOBAL PACKAGING (txns[1]) matched BILL-GP-01
    assert erps["BILL-MP-02"].matched_bank_txn_id == txns[0].id
    assert erps["BILL-GP-01"].matched_bank_txn_id == txns[1].id

    # Verify INV-700 matched txns[2] with 90.00 fee variance
    assert erps["INV-700"].matched_bank_txn_id == txns[2].id
    assert txns[2].variance_amount == Decimal("90.00")

    # Verify all 3 batch invoices matched txns[3]
    assert erps["INV-B1"].matched_bank_txn_id == txns[3].id
    assert erps["INV-B2"].matched_bank_txn_id == txns[3].id
    assert erps["INV-B3"].matched_bank_txn_id == txns[3].id
    db.close()


def test_journey_5_balanced_double_entry_journal_export():
    """
    Accountant Expectation on Final Export:
    - When exporting with `expand_journal_legs=True`, every bank transaction produces
      a balanced pair of GL journal lines (Bank GL leg + Counterpart Category/GL leg),
      and the grand total of Debit == Credit across the entire export file (`0.00` out-of-balance).
    """
    db = _create_fresh_db()

    csv_stmt = (
        "Posting Date,Value Date,Debit,Credit,Balance,Description,Project_Code\n"
        "2026-08-01,2026-08-01,12500.50,0.00,87499.50,MONTHLY PAYROLL SALARIES,PRJ-HQ\n"
        "2026-08-02,2026-08-02,150.25,0.00,87349.25,SWIFT TRANSFER COMMISSION,PRJ-HQ\n"
        "2026-08-03,2026-08-03,0.00,45000.75,132350.00,POS SETTLEMENT BATCH,PRJ-RETAIL\n"
    ).encode("utf-8")

    stmt = bank_reconcile_service.process_ingestion(
        db=db,
        content=csv_stmt,
        file_type="csv",
        company_id=5,
        user_id=50,
        overrides={"bank_id": 1, "account_number": "ACC-EXP-5", "file_name": "export_test.csv"},
    )

    exp_req = CustomExportExecuteRequest(
        statement_id=stmt.id,
        columns_config=[
            {"source_field": "booking_date", "custom_header": "Journal_Date"},
            {"source_field": "internal_category", "custom_header": "GL_Account"},
            {"source_field": "debit_amount", "custom_header": "Debit_EGP"},
            {"source_field": "credit_amount", "custom_header": "Credit_EGP"},
            {"source_field": "custom_fields.Project_Code", "custom_header": "Project"},
        ],
        date_format="%d/%m/%Y",
        expand_journal_legs=True,
        bank_gl_account="1010010 - Cash at Bank",
    )

    headers, rows, _, _ = bank_reconcile_service.build_custom_export_rows(db, customer_id=5, req=exp_req)
    assert headers == ["Journal_Date", "GL_Account", "Debit_EGP", "Credit_EGP", "Project"]
    assert len(rows) == 6  # 3 transactions * 2 legs

    total_dr = sum(Decimal(str(r["Debit_EGP"] or "0.00")) for r in rows)
    total_cr = sum(Decimal(str(r["Credit_EGP"] or "0.00")) for r in rows)
    expected_sum = Decimal("12500.50") + Decimal("150.25") + Decimal("45000.75")

    assert total_dr == expected_sum, f"Expected Total DR {expected_sum}, got {total_dr}"
    assert total_cr == expected_sum, f"Expected Total CR {expected_sum}, got {total_cr}"
    assert total_dr == total_cr, "Double-entry journal export MUST be 100% balanced!"
    assert rows[0]["Journal_Date"] == "01/08/2026"
    assert rows[0]["Project"] == "PRJ-HQ"
    db.close()


if __name__ == "__main__":
    test_journey_1_messy_arabic_and_multiline_statement_expectations()
    test_journey_2_month1_teaching_to_month2_autonomy_and_trap_avoidance()
    test_journey_3_standalone_multibank_sweeps_and_bounced_cheques_without_erp()
    test_journey_4_erp_reconciliation_disambiguation_tolerance_and_batch()
    test_journey_5_balanced_double_entry_journal_export()
    print("ALL_5_CUSTOMER_EXPECTATION_USER_JOURNEYS_PASSED")
