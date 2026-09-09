import io
from decimal import Decimal
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.models_reconciliation_v2 import (
    Base, BankStatement, BankTransaction, InternalLedgerRecord,
    ClassificationTaxonomy, Counterparty, CollaborativePattern, CollaborativeTenantVote
)
from app.services.bank_reconciliation_service import bank_reconcile_service

# In-memory SQLite for fast testing
engine = create_engine("sqlite:///:memory:", echo=False)
TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def setup_db():
    tables = [
        ClassificationTaxonomy.__table__,
        Counterparty.__table__,
        BankStatement.__table__,
        BankTransaction.__table__,
        InternalLedgerRecord.__table__,
        CollaborativePattern.__table__,
        CollaborativeTenantVote.__table__,
    ]
    Base.metadata.create_all(bind=engine, tables=tables)
    return TestingSessionLocal()


def test_taxonomy_hierarchy_and_subclasses():
    db = setup_db()
    try:
        # 1. Parent Class: Operating Expenses
        opex_class = ClassificationTaxonomy(
            company_id=None,
            name="Operating Expenses (OPEX)",
            name_ar="المصروفات التشغيلية",
            code="OPEX",
            direction="DEBIT",
            is_active=True
        )
        db.add(opex_class)
        db.commit()
        db.refresh(opex_class)

        assert opex_class.id is not None
        assert opex_class.code == "OPEX"

        # 2. Add Child Subclasses with Default G/L Accounts
        sub_cloud = ClassificationTaxonomy(
            company_id=1,
            parent_id=opex_class.id,
            name="Cloud & Hosting Services",
            code="OPEX_CLOUD",
            direction="DEBIT",
            default_gl_account="5020100 - Cloud Infrastructure",
            is_active=True
        )
        sub_rent = ClassificationTaxonomy(
            company_id=1,
            parent_id=opex_class.id,
            name="Office Rent",
            code="OPEX_RENT",
            direction="DEBIT",
            default_gl_account="5010200 - Head Office Rent",
            is_active=True
        )
        db.add_all([sub_cloud, sub_rent])
        db.commit()

        # Query parent with subclasses
        parent = db.query(ClassificationTaxonomy).filter(ClassificationTaxonomy.id == opex_class.id).first()
        assert len(parent.subclasses) == 2
        assert any(s.code == "OPEX_CLOUD" for s in parent.subclasses)
        assert parent.subclasses[0].default_gl_account is not None
        print("[PASS] test_taxonomy_hierarchy_and_subclasses passed")
    finally:
        db.close()


def test_erp_csv_file_ingestion():
    db = setup_db()
    try:
        csv_content = (
            b"Date,Reference,Entity,Amount,Type,GL Account\n"
            b"2026-02-01,INV-2026-001,ORANGE DATA TELECOM,92400.00,AR_INVOICE,1020100 - Trade Receivables\n"
            b"2026-02-03,BILL-2026-010,SCHNEIDER ELECTRIC,-145000.00,AP_BILL,2010100 - Trade Payables\n"
            b"2026-02-05,LG-COM-2026-01,COMMERCIAL INTL BANK,-4500.00,LG_COMMISSION,6020300 - LG Commissions\n"
        )

        res = bank_reconcile_service.process_erp_file_ingestion(
            db=db,
            file_content=csv_content,
            file_type="csv",
            company_id=1,
            user_id=42
        )

        assert res["imported_count"] == 3
        db_records = db.query(InternalLedgerRecord).filter(InternalLedgerRecord.company_id == 1).all()
        assert len(db_records) == 3

        inv = next(r for r in db_records if r.reference_number == "INV-2026-001")
        assert inv.entity_name == "ORANGE DATA TELECOM"
        assert inv.amount == Decimal("92400.00")
        assert inv.record_type == "AR_INVOICE"
        assert inv.gl_account == "1020100 - Trade Receivables"
        assert inv.created_by == 42

        bill = next(r for r in db_records if r.reference_number == "BILL-2026-010")
        assert bill.amount == Decimal("-145000.00")
        assert bill.record_type == "AP_BILL"
        print("[PASS] test_erp_csv_file_ingestion passed")
    finally:
        db.close()


def test_erp_bulk_json_ingest():
    db = setup_db()
    try:
        records = [
            {
                "record_type": "AR_INVOICE",
                "reference_number": "ORACLE-INV-990",
                "entity_name": "VODAFONE EGYPT",
                "amount": 75000.50,
                "record_date": "2026-02-01",
                "gl_account": "1020100 - Trade Receivables"
            },
            {
                "record_type": "AP_BILL",
                "reference_number": "SAP-PO-882",
                "entity_name": "BAKER HUGHES EG",
                "amount": -32000.00,
                "record_date": "2026-02-02",
                "gl_account": "2010100 - Trade Payables"
            }
        ]

        res = bank_reconcile_service.ingest_erp_records_bulk(
            db=db,
            records_data=records,
            company_id=2,
            user_id=10
        )

        assert res["imported_count"] == 2
        recs = db.query(InternalLedgerRecord).filter(InternalLedgerRecord.company_id == 2).all()
        assert len(recs) == 2
        assert recs[0].currency == "EGP"
        print("[PASS] test_erp_bulk_json_ingest passed")
    finally:
        db.close()


def test_manual_classify_with_counterparty_learning():
    db = setup_db()
    try:
        # Create statement
        stmt = BankStatement(
            company_id=1,
            bank_id=1,
            account_number="EG12000340001",
            file_name="test_stmt.csv",
            opening_balance=Decimal("500000.0"),
            closing_balance=Decimal("592400.0"),
            statement_start_date=datetime(2026, 2, 1),
            statement_end_date=datetime(2026, 2, 28),
            status="PROCESSED"
        )
        db.add(stmt)
        db.commit()
        db.refresh(stmt)

        txn = BankTransaction(
            statement_id=stmt.id,
            booking_date=datetime(2026, 2, 10),
            value_date=datetime(2026, 2, 10),
            debit_amount=Decimal("1450.00"),
            credit_amount=Decimal("0.00"),
            running_balance=Decimal("498550.00"),
            currency="EGP",
            raw_description="AMAZON WEB SERVICES AWS EMEA CHARGE REF AWS-98213",
            cleaned_description="AMAZON WEB SERVICES AWS EMEA CHARGE REF AWS-98213",
            is_classified=False
        )
        db.add(txn)
        db.commit()
        db.refresh(txn)

        # Confirm and learn
        bank_reconcile_service.confirm_and_learn(
            db=db,
            transaction_id=txn.id,
            customer_id=1,
            user_id=1,
            counterparty_name="AMAZON WEB SERVICES",
            category="Operating Expenses (OPEX)",
            gl_account="5020100 - Cloud Infrastructure"
        )

        # Verify counterparty was learned in registry
        cp = db.query(Counterparty).filter(
            Counterparty.company_id == 1,
            Counterparty.name == "AMAZON WEB SERVICES"
        ).first()
        assert cp is not None
        assert cp.default_category == "Operating Expenses (OPEX)"
        assert cp.learned_count >= 1
        print("[PASS] test_manual_classify_with_counterparty_learning passed")
    finally:
        db.close()


if __name__ == "__main__":
    test_taxonomy_hierarchy_and_subclasses()
    test_erp_csv_file_ingestion()
    test_erp_bulk_json_ingest()
    test_manual_classify_with_counterparty_learning()
    print("ALL_ERP_INGESTION_AND_TAXONOMY_TESTS_PASSED")
