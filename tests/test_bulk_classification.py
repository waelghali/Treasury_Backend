from decimal import Decimal
from datetime import date, datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from types import SimpleNamespace

from app.models.models_reconciliation_v2 import Base, BankStatement, BankTransaction, Counterparty
from app.crud.crud_reconciliation_v2 import crud_counterparty
from app.api.v1.endpoints.reconciliation_endpoints import bulk_classify_transactions, bulk_clear_classification
from app.schemas.schemas_reconciliation_v2 import BulkClassifyRequest, BulkClearClassificationRequest

# Use in-memory SQLite for high-speed test verification
engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def setup_db():
    tables = [
        BankStatement.__table__,
        BankTransaction.__table__,
        Counterparty.__table__,
    ]
    Base.metadata.create_all(bind=engine, tables=tables)
    return TestingSessionLocal()

def test_bulk_operations():
    db = setup_db()
    company_id = 42
    current_user = SimpleNamespace(customer_id=company_id, user_id=101)

    # 1. Create a parent BankStatement
    stmt = BankStatement(
        company_id=company_id,
        bank_id=1,
        account_number="EG1234567890",
        file_name="test_stmt.csv",
        opening_balance=Decimal("100000.00"),
        closing_balance=Decimal("94850.00"),
        statement_start_date=date(2026, 9, 1),
        statement_end_date=date(2026, 9, 30),
        status="PROCESSED"
    )
    db.add(stmt)
    db.commit()
    db.refresh(stmt)

    # 2. Create 3 test bank transactions linked to the statement
    t1 = BankTransaction(
        statement_id=stmt.id,
        booking_date=datetime(2026, 9, 1),
        value_date=datetime(2026, 9, 1),
        debit_amount=Decimal("450.00"),
        credit_amount=Decimal("0.00"),
        running_balance=Decimal("99550.00"),
        currency="EGP",
        raw_description="AWS CLOUD EMEA SERVICES",
        cleaned_description="AWS CLOUD EMEA SERVICES",
        is_classified=False
    )
    t2 = BankTransaction(
        statement_id=stmt.id,
        booking_date=datetime(2026, 9, 2),
        value_date=datetime(2026, 9, 2),
        debit_amount=Decimal("1200.00"),
        credit_amount=Decimal("0.00"),
        running_balance=Decimal("98350.00"),
        currency="EGP",
        raw_description="AWS SERVICES CHARGE MONTHLY",
        cleaned_description="AWS SERVICES CHARGE MONTHLY",
        is_classified=False
    )
    t3 = BankTransaction(
        statement_id=stmt.id,
        booking_date=datetime(2026, 9, 3),
        value_date=datetime(2026, 9, 3),
        debit_amount=Decimal("3500.00"),
        credit_amount=Decimal("0.00"),
        running_balance=Decimal("94850.00"),
        currency="EGP",
        raw_description="AWS STORAGE SYNC",
        cleaned_description="AWS STORAGE SYNC",
        is_classified=False
    )
    db.add_all([t1, t2, t3])
    db.commit()

    # 3. Bulk classify all 3 transactions as Cloud Expenses
    req = BulkClassifyRequest(
        transaction_ids=[t1.id, t2.id, t3.id],
        category="Operating Expenses (OPEX)",
        sub_category="Cloud & Hosting Services",
        gl_account="5020100 - Cloud Infrastructure",
        remember_for_counterparty=True,
        counterparty_name="AMAZON WEB SERVICES"
    )

    result = bulk_classify_transactions(req, db=db, current_user=current_user)
    assert result.status == "success"
    assert result.affected_count == 3

    # Verify db records updated
    db.refresh(t1)
    db.refresh(t2)
    db.refresh(t3)
    assert t1.classification_category == "Operating Expenses (OPEX)"
    assert t1.sub_category == "Cloud & Hosting Services"
    assert t1.internal_category == "5020100 - Cloud Infrastructure"
    assert t1.classification_source == "MANUAL"

    # Verify counterparty was learned in registry
    cps = crud_counterparty.get_by_customer(db, company_id)
    assert any(c.name == "AMAZON WEB SERVICES" for c in cps)

    # 4. Bulk clear classification on t1 and t2
    clear_req = BulkClearClassificationRequest(transaction_ids=[t1.id, t2.id])
    clear_res = bulk_clear_classification(clear_req, db=db, current_user=current_user)
    assert clear_res.affected_count == 2

    db.refresh(t1)
    db.refresh(t2)
    db.refresh(t3)
    assert t1.classification_category is None
    assert t1.classification_source is None
    assert t3.classification_category == "Operating Expenses (OPEX)"

    print("ALL_BULK_CLASSIFICATION_TESTS_PASSED")

if __name__ == "__main__":
    test_bulk_operations()
