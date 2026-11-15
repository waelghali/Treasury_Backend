from pydantic import BaseModel, ConfigDict, Field
from typing import List, Optional, Any, Dict
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
    tokenized_description: Optional[str] = None
    counterparty_name: Optional[str] = None
    counterparty_iban: Optional[str] = None
    iban_bank_code: Optional[str] = None
    e2e_id: Optional[str] = None

    # Expanded & Schema-Agnostic fields
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
    amount_tier: Optional[str] = None
    custom_fields_json: Optional[Dict[str, Any]] = None
    applied_rule_id: Optional[int] = None
    linked_txn_id: Optional[int] = None
    companion_parent_id: Optional[int] = None

class BankTransactionOut(BankTransactionBase):
    id: int
    statement_id: int
    is_reconciled: bool
    is_classified: bool
    auto_confirmed: Optional[bool] = False
    is_reversal: bool
    is_duplicate: bool
    is_exported: Optional[bool] = False
    is_locked: Optional[bool] = False
    created_at: datetime
    applied_rule_name: Optional[str] = None
    internal_category: Optional[str] = None
    classification_source: Optional[str] = None
    classification_confidence: Optional[int] = None
    suggested_category: Optional[str] = None
    classification_reasoning: Optional[str] = None
    top_suggestions_json: Optional[List[Dict[str, Any]]] = None
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
    balance_continuity_status: Optional[str] = "VALIDATED"
    balance_variance_amount: Optional[Decimal] = Decimal("0.00")
    continuity_warnings_json: Optional[List[str]] = None
    detected_mapping_json: Optional[Dict[str, Any]] = None

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
    bank_id: Optional[int] = None
    rule_name: Optional[str] = None
    rule_type: Optional[str] = "LITERAL"  # LITERAL, CONCEPT, LEARNED_COMPOSITE
    rule_origin: Optional[str] = "MANUAL" # MANUAL, LEARNED_ONLINE, COLLABORATIVE, SYSTEM_ADAPTED
    lifecycle_state: Optional[str] = "AUTO_CONFIRMED" # CANDIDATE, SUGGESTED, AUTO_CONFIRMED, DEMOTED
    stop_after_match: bool = True
    conditions_json: Any
    assigned_gl_account: str
    assigned_category: Optional[str] = None
    assigned_sub_category: Optional[str] = None
    is_active: bool = True

class ClassificationRuleCreate(ClassificationRuleBase):
    pass

class ClassificationRuleUpdate(BaseModel):
    rule_name: Optional[str] = None
    rule_type: Optional[str] = None
    lifecycle_state: Optional[str] = None
    bank_id: Optional[int] = None
    priority: Optional[int] = None
    stop_after_match: Optional[bool] = None
    conditions_json: Optional[Any] = None
    assigned_gl_account: Optional[str] = None
    assigned_category: Optional[str] = None
    assigned_sub_category: Optional[str] = None
    is_active: Optional[bool] = None

class ClassificationRuleOut(ClassificationRuleBase):
    id: int
    company_id: int
    usage_count: int = 0
    confirmations_count: Optional[int] = 1
    overrides_count: Optional[int] = 0
    empirical_precision: Optional[Decimal] = Decimal("100.00")
    calibrated_confidence: Optional[int] = 85
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
    blind_token: Optional[str] = None
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
    custom_fields_json: Optional[Dict[str, Any]] = None
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


# ── Learned Matching Tolerance Rules & Customizable Export Schemas ──────────

class MatchingToleranceRuleBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    bank_id: Optional[int] = None
    rule_name: str
    pass_type: str = "FEE_TOLERANCE"
    direction: str = "EITHER"
    record_type: Optional[str] = None
    max_date_window_days: int = 7
    max_fixed_fee_variance: Decimal = Decimal("50.00")
    max_percentage_variance: Decimal = Decimal("0.0200")
    default_variance_disposition: str = "BANK_FEE_WRITEOFF"
    variance_gl_account: Optional[str] = "BANK_CHARGES"
    lifecycle_state: str = "AUTO_CONFIRMED"
    is_active: bool = True

class MatchingToleranceRuleCreate(MatchingToleranceRuleBase):
    pass

class MatchingToleranceRuleOut(MatchingToleranceRuleBase):
    id: int
    company_id: int
    confirmations_count: int = 1
    overrides_count: int = 0
    created_at: datetime


class ExportColumnSpec(BaseModel):
    source_field: str # e.g. "booking_date", "raw_description", "debit_amount", "custom_fields.Branch"
    custom_header: str # User-chosen column header name
    default_value: Optional[str] = ""

class ReconciliationExportProfileBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    profile_name: str
    description: Optional[str] = None
    file_format: str = "CSV" # CSV or XLSX
    columns_config_json: List[Dict[str, Any]]
    amount_mode: str = "SPLIT_DEBIT_CREDIT" # SPLIT_DEBIT_CREDIT, SINGLE_SIGNED, ABSOLUTE_WITH_DC_FLAG
    date_format: str = "%Y-%m-%d"
    filter_config_json: Optional[Dict[str, Any]] = None
    expand_journal_legs: bool = False
    bank_gl_account: Optional[str] = "1010010 - Cash at Bank"
    is_default: bool = False
    is_active: bool = True

class ReconciliationExportProfileCreate(ReconciliationExportProfileBase):
    pass

class ReconciliationExportProfileUpdate(BaseModel):
    profile_name: Optional[str] = None
    description: Optional[str] = None
    file_format: Optional[str] = None
    columns_config_json: Optional[List[Dict[str, Any]]] = None
    amount_mode: Optional[str] = None
    date_format: Optional[str] = None
    filter_config_json: Optional[Dict[str, Any]] = None
    expand_journal_legs: Optional[bool] = None
    bank_gl_account: Optional[str] = None
    is_default: Optional[bool] = None
    is_active: Optional[bool] = None

class ReconciliationExportProfileOut(ReconciliationExportProfileBase):
    id: int
    company_id: int
    created_at: datetime

class CustomExportExecuteRequest(BaseModel):
    profile_id: Optional[int] = None
    statement_id: Optional[int] = None
    bank_id: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    only_reconciled: Optional[bool] = None
    only_classified: Optional[bool] = None
    exclude_already_exported: bool = False
    mark_as_exported: bool = False
    file_format: Optional[str] = None # Override CSV or XLSX
    columns_config: Optional[List[Dict[str, Any]]] = None # Ad-hoc custom column list & order
    amount_mode: Optional[str] = None
    date_format: Optional[str] = None
    expand_journal_legs: Optional[bool] = None
    bank_gl_account: Optional[str] = None
