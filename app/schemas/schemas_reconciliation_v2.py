from pydantic import BaseModel, ConfigDict, Field
from typing import List, Optional, Any
from datetime import datetime
from decimal import Decimal

class BankTransactionBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    external_txn_id: Optional[str] = None
    bank_txn_code: Optional[str] = None
    booking_date: datetime
    value_date: datetime
    debit_amount: Decimal = Decimal("0.00")
    credit_amount: Decimal = Decimal("0.00")
    currency: str
    running_balance: Decimal
    raw_description: str
    counterparty_name: Optional[str] = None
    counterparty_iban: Optional[str] = None
    e2e_id: Optional[str] = None

    # New expanded fields
    company_name: Optional[str] = None
    account_number: Optional[str] = None
    back_office_ref: Optional[str] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    net_amount: Optional[Decimal] = None
    is_positive: Optional[bool] = None
    classification_category: Optional[str] = None
    source_system: Optional[str] = None
    exchange_rate_egp: Optional[Decimal] = None
    exchange_rate_eur_usd: Optional[Decimal] = None
    beneficiary_name: Optional[str] = None
    purpose_of_payment: Optional[str] = None
    amount_in_currency: Optional[Decimal] = None
    amount_in_egp: Optional[Decimal] = None
    transfer_type: Optional[str] = None
    applied_rule_id: Optional[int] = None
    linked_txn_id: Optional[int] = None

class BankTransactionOut(BankTransactionBase):
    id: int
    statement_id: int
    is_reconciled: bool
    is_classified: bool
    is_reversal: bool
    is_duplicate: bool
    created_at: datetime
    applied_rule_name: Optional[str] = None

class BankStatementBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    bank_id: int
    company_id: int
    file_name: str
    opening_balance: Decimal
    closing_balance: Decimal
    statement_start_date: datetime
    statement_end_date: datetime
    account_number: Optional[str] = None
    currency_id: Optional[int] = None

class BankStatementCreate(BankStatementBase):
    pass

class BankStatementOut(BankStatementBase):
    id: int
    status: str
    created_at: datetime
    transactions: List[BankTransactionOut] = []

class ReconciliationMatchCreate(BaseModel):
    bank_txn_id: int
    source_type: str
    source_record_id: int
    match_type: str
    match_logic: str

class ClassificationRuleBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    priority: int = 100
    rule_name: Optional[str] = None
    stop_after_match: bool = True
    conditions_json: Any
    assigned_gl_account: str
    is_active: bool = True

class ClassificationRuleCreate(ClassificationRuleBase):
    pass

class ClassificationRuleUpdate(BaseModel):
    rule_name: Optional[str] = None
    priority: Optional[int] = None
    stop_after_match: Optional[bool] = None
    conditions_json: Optional[Any] = None
    assigned_gl_account: Optional[str] = None
    is_active: Optional[bool] = None

class ClassificationRuleOut(ClassificationRuleBase):
    id: int
    company_id: int
    usage_count: int
    created_at: datetime
