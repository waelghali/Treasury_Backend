from decimal import Decimal
from app.services.bank_reconciliation_service import bank_reconcile_service

def test_builtin_interest_credit():
    txn = {
        "raw_description": "MONTHLY INT CREDIT ON SAVINGS",
        "description_line2": "",
        "debit_amount": Decimal("0.00"),
        "credit_amount": Decimal("1500.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_INTEREST"
    assert result["sub_category"] == "INTEREST_EARNED"
    assert result["confidence"] == 90

def test_builtin_interest_arabic():
    txn = {
        "raw_description": "فائدة دائنة عن شهر أغسطس",
        "description_line2": None,
        "debit_amount": Decimal("0.00"),
        "credit_amount": Decimal("850.50"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_INTEREST"
    assert result["sub_category"] == "INTEREST_EARNED"

def test_builtin_interest_debit():
    txn = {
        "raw_description": "OVERDRAFT INTEREST CHARGE",
        "description_line2": None,
        "debit_amount": Decimal("320.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_INTEREST"
    assert result["sub_category"] == "INTEREST_CHARGED"

def test_builtin_bank_charges():
    txn = {
        "raw_description": "COMMISSION ON SWIFT TRANSFER REF 12345",
        "description_line2": "SERVICE CHARGE",
        "debit_amount": Decimal("45.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_CHARGES"
    assert result["confidence"] == 85

def test_builtin_bank_charges_arabic():
    txn = {
        "raw_description": "عمولة تحويل ومصاريف بنكية",
        "description_line2": "",
        "debit_amount": Decimal("120.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "BANK_CHARGES"

def test_builtin_tax_deduction():
    txn = {
        "raw_description": "WHT DEDUCTION ON ACCRUED INTEREST",
        "description_line2": "",
        "debit_amount": Decimal("150.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "TAX_DEDUCTION"

def test_builtin_tax_arabic():
    txn = {
        "raw_description": "ضريبة استقطاع / دمغة نسبية",
        "description_line2": "",
        "debit_amount": Decimal("75.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "TAX_DEDUCTION"

def test_builtin_salary_payroll():
    txn = {
        "raw_description": "MONTHLY SALARY BATCH 2026-08",
        "description_line2": "",
        "debit_amount": Decimal("450000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "SALARY_PAYROLL"

def test_builtin_salary_arabic():
    txn = {
        "raw_description": "صرف مرتبات وأجور العاملين",
        "description_line2": "",
        "debit_amount": Decimal("320000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "SALARY_PAYROLL"

def test_builtin_loan_repayment():
    txn = {
        "raw_description": "LOAN REPAYMENT INSTALLMENT #12",
        "description_line2": "",
        "debit_amount": Decimal("50000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is not None
    assert result["category"] == "LOAN_REPAYMENT"

def test_builtin_unrelated_not_classified():
    txn = {
        "raw_description": "SUPPLIER PAYMENT INVOICE 99482 AL MOASSRON",
        "description_line2": "",
        "debit_amount": Decimal("25000.00"),
        "credit_amount": Decimal("0.00"),
    }
    result = bank_reconcile_service._run_builtin_classifiers(txn)
    assert result is None

def test_concept_collection_rule():
    from app.models.models_reconciliation_v2 import BankTransaction
    txn = BankTransaction(
        raw_description="TRANSFER FROM ACME HOLDINGS CORP REF 9921",
        credit_amount=Decimal("75000.00"),
        debit_amount=Decimal("0.00"),
        counterparty_name="ACME HOLDINGS CORP"
    )
    # Test concept rule evaluation
    rule_condition = {"concept": "COLLECTION_FROM_CUSTOMER"}
    assert bank_reconcile_service._evaluate_group(txn, rule_condition) is True

    # Test counterparty condition
    cp_condition = {"conditions": [
        {"field": "counterparty_name", "operator": "contains", "value": "ACME"},
        {"field": "credit_amount", "operator": "gt", "value": 0, "joiner": "AND"}
    ]}
    assert bank_reconcile_service._evaluate_group(txn, cp_condition) is True

def test_concept_supplier_payment_rule():
    from app.models.models_reconciliation_v2 import BankTransaction
    txn = BankTransaction(
        raw_description="PAYMENT TO VENDOR ORASCOM CONSTRUCTION",
        credit_amount=Decimal("0.00"),
        debit_amount=Decimal("120000.00"),
        counterparty_name="ORASCOM CONSTRUCTION"
    )
    rule_condition = {"concept": "PAYMENT_TO_SUPPLIER"}
    assert bank_reconcile_service._evaluate_group(txn, rule_condition) is True

    # Opposite concept should NOT match
    assert bank_reconcile_service._evaluate_group(txn, {"concept": "COLLECTION_FROM_CUSTOMER"}) is False

if __name__ == "__main__":
    test_builtin_interest_credit()
    test_builtin_interest_arabic()
    test_builtin_interest_debit()
    test_builtin_bank_charges()
    test_builtin_bank_charges_arabic()
    test_builtin_tax_deduction()
    test_builtin_tax_arabic()
    test_builtin_salary_payroll()
    test_builtin_salary_arabic()
    test_builtin_loan_repayment()
    test_builtin_unrelated_not_classified()
    test_concept_collection_rule()
    test_concept_supplier_payment_rule()
    print("ALL_CLASSIFIER_AND_CONCEPT_TESTS_PASSED")

