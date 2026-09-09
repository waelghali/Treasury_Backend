import os
import sys
from decimal import Decimal
from datetime import datetime

# Ensure project root in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.database import SessionLocal
from app.services.bank_reconciliation_service import bank_reconcile_service
from app.models.models_reconciliation_v2 import BankStatement, BankTransaction, InternalLedgerRecord, ReconciliationMatch
from app.models import Customer

def test_ingest_and_reconcile():
    db = SessionLocal()
    
    customer = db.query(Customer).first()
    customer_id = customer.id if customer else 1
    
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'demo_statements', 'Customer_Demo_01_HSBC.xlsx'))
    with open(file_path, "rb") as f:
        content = f.read()

    print(f"Ingesting {file_path} for customer {customer_id}...")
    # Clean any prior statement with this account to allow re-test
    old_stmt = db.query(BankStatement).filter(BankStatement.account_number == "001-987654-001").first()
    if old_stmt:
        old_txn_ids = [t.id for t in db.query(BankTransaction).filter(BankTransaction.statement_id == old_stmt.id).all()]
        if old_txn_ids:
            db.query(InternalLedgerRecord).filter(InternalLedgerRecord.matched_bank_txn_id.in_(old_txn_ids)).update({"matched_bank_txn_id": None, "status": "OPEN"}, synchronize_session=False)
            db.query(ReconciliationMatch).filter(ReconciliationMatch.bank_txn_id.in_(old_txn_ids)).delete(synchronize_session=False)
            db.query(BankTransaction).filter(BankTransaction.statement_id == old_stmt.id).delete(synchronize_session=False)
        db.delete(old_stmt)
        db.commit()

    overrides = {
        "bank_id": 1,
        "file_name": "Customer_Demo_01_HSBC.xlsx",
        "account_number": "001-987654-001"
    }

    db_stmt = bank_reconcile_service.process_ingestion(
        db, content, "xlsx",
        company_id=customer_id,
        user_id=1,
        overrides=overrides
    )

    print(f"Statement Ingested successfully! ID: {db_stmt.id}, Period: {db_stmt.statement_start_date.date()} to {db_stmt.statement_end_date.date()}")

    txns = db.query(BankTransaction).filter(BankTransaction.statement_id == db_stmt.id).all()
    print(f"Total Transactions parsed: {len(txns)}")
    assert len(txns) == 20, f"Expected 20 transactions, got {len(txns)}"

    # Check built-in classifications
    classified = [t for t in txns if t.is_classified]
    print(f"Auto-Classified Lines: {len(classified)}/{len(txns)} ({len(classified)/len(txns)*100:.1f}%)")
    for t in classified[:5]:
        print(f"  - {t.raw_description[:40]}: {t.classification_category} ({t.classification_source})")

    # Check sweep pairing
    paired_sweeps = [t for t in txns if t.linked_txn_id is not None]
    print(f"Paired Sweep Lines: {len(paired_sweeps)}")
    for t in paired_sweeps:
        print(f"  - Sweep {t.id} <-> {t.linked_txn_id}: {t.raw_description[:45]} (Variance: {t.variance_amount})")

    # Run auto-matching against ERP records
    print("\nRunning Auto-Matching Engine...")
    match_res = bank_reconcile_service.run_matching_engine(
        db, customer_id=customer_id, user_id=1, statement_id=db_stmt.id
    )
    print(f"Matching Result: {match_res}")

    matches = db.query(ReconciliationMatch).filter(
        ReconciliationMatch.bank_txn_id.in_([t.id for t in txns])
    ).all()
    print(f"Reconciliation Matches Created: {len(matches)}")
    for m in matches:
        b_txn = db.get(BankTransaction, m.bank_txn_id)
        print(f"  [OK] Bank Txn #{b_txn.id} ({b_txn.raw_description[:35]}) matched with {m.source_type} #{m.source_record_id} via {m.match_logic}")

    reconciled_count = db.query(BankTransaction).filter(
        BankTransaction.statement_id == db_stmt.id,
        BankTransaction.is_reconciled == True
    ).count()
    print(f"Total Reconciled Bank Txns: {reconciled_count}/{len(txns)} ({reconciled_count/len(txns)*100:.1f}%)")

    db.close()
    print("\nALL_DEMO_TESTS_PASSED")

if __name__ == "__main__":
    test_ingest_and_reconcile()
