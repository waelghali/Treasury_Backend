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
    internal_category: Optional[str] = None
    classification_source: Optional[str] = None
    classification_confidence: Optional[int] = None
    suggested_category: Optional[str] = None
    variance_amount: Optional[Decimal] = None


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
    rule_type: Optional[str] = "LITERAL"  # LITERAL or CONCEPT
    stop_after_match: bool = True
    conditions_json: Any
    assigned_gl_account: str
    is_active: bool = True

class ClassificationRuleCreate(ClassificationRuleBase):
    pass

class ClassificationRuleUpdate(BaseModel):
    rule_name: Optional[str] = None
    rule_type: Optional[str] = None
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


class CounterpartyBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    name: str
    aliases: Optional[List[str]] = None
    entity_type: str = "CUSTOMER"  # CUSTOMER, SUPPLIER, BANK, GOVERNMENT, OTHER
    default_gl_account: Optional[str] = None
    default_category: Optional[str] = None
    is_active: bool = True

class CounterpartyCreate(CounterpartyBase):
    company_id: Optional[int] = None  # None for global

class CounterpartyUpdate(BaseModel):
    name: Optional[str] = None
    aliases: Optional[List[str]] = None
    entity_type: Optional[str] = None
    default_gl_account: Optional[str] = None
    default_category: Optional[str] = None
    is_active: Optional[bool] = None
    is_verified: Optional[bool] = None

class CounterpartyOut(CounterpartyBase):
    id: int
    company_id: Optional[int] = None
    learned_count: int = 0
    is_verified: bool = False
    created_at: datetime


class InternalLedgerRecordBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    record_type: str
    reference_number: str
    entity_name: str
    record_date: datetime
    amount: Decimal
    currency: Optional[str] = "EGP"
    gl_account: Optional[str] = None
    bank_id: Optional[int] = None
    bank_name: Optional[str] = None
    bank_account_number: Optional[str] = None
    status: Optional[str] = "OPEN"

class InternalLedgerRecordCreate(InternalLedgerRecordBase):
    company_id: int

class InternalLedgerRecordOut(InternalLedgerRecordBase):
    id: int
    company_id: int
    matched_bank_txn_id: Optional[int] = None
    created_at: datetime


class TaxonomyNodeBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    code: str
    name: str
    name_ar: Optional[str] = None
    parent_id: Optional[int] = None
    direction: Optional[str] = "EITHER" # DEBIT, CREDIT, EITHER
    default_gl_account: Optional[str] = None
    description: Optional[str] = None
    is_active: bool = True
    order_index: int = 0

class TaxonomyNodeCreate(TaxonomyNodeBase):
    company_id: Optional[int] = None

class TaxonomyNodeUpdate(BaseModel):
    name: Optional[str] = None
    name_ar: Optional[str] = None
    code: Optional[str] = None
    parent_id: Optional[int] = None
    direction: Optional[str] = None
    default_gl_account: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None
    order_index: Optional[int] = None

class TaxonomyNodeOut(TaxonomyNodeBase):
    id: int
    company_id: Optional[int] = None
    subclasses: Optional[List['TaxonomyNodeOut']] = []
    created_at: datetime

class ClassifyTransactionRequest(BaseModel):
    category: str # Class or Subclass name/code
    sub_category: Optional[str] = None
    gl_account: Optional[str] = None
    remember_for_counterparty: bool = True
    counterparty_name: Optional[str] = None

class ERPUploadResult(BaseModel):
    imported_count: int
    total_rows_processed: int
    errors: List[str] = []
    status: str

class BulkClassifyRequest(BaseModel):
    transaction_ids: List[int]
    category: str
    sub_category: Optional[str] = None
    gl_account: Optional[str] = None
    remember_for_counterparty: bool = False
    counterparty_name: Optional[str] = None

class BulkClearClassificationRequest(BaseModel):
    transaction_ids: List[int]

class BulkOperationResult(BaseModel):
    status: str
    affected_count: int
    message: str

class MatchActionRequest(BaseModel):
    bank_transaction_ids: List[int]
    erp_record_ids: List[int]
    type: Optional[str] = "MANUAL"
    write_off_variance_as_fee: Optional[bool] = False
    variance_amount: Optional[Decimal] = None
    variance_disposition: Optional[str] = None # 'BANK_FEE_WRITEOFF', 'ROUNDING', 'FX_DIFFERENCE'

class UnmatchActionRequest(BaseModel):
    match_id: Optional[int] = None
    bank_transaction_ids: Optional[List[int]] = None
    erp_record_ids: Optional[List[int]] = None




