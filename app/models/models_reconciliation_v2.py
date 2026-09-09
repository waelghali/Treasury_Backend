# app/models_reconciliation_v2.py

from sqlalchemy import Column, Integer, String, Numeric, ForeignKey, DateTime, Boolean, Text, JSON, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base
from app.models import BaseModel

class BankStatement(BaseModel):
    """
    Header-level tracking for an uploaded bank statement file.
    """
    __tablename__ = 'bank_statements'

    company_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    account_id = Column(Integer, nullable=True) # Future: Link to BankAccount model
    account_number = Column(String, nullable=True)
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=True)
    
    file_name = Column(String, nullable=False)
    raw_file_path = Column(String, nullable=True) # Path to secure storage
    json_snapshot = Column(JSON, nullable=True) # Full parsed structure
    
    opening_balance = Column(Numeric(precision=18, scale=2), nullable=False)
    closing_balance = Column(Numeric(precision=18, scale=2), nullable=False)
    
    statement_start_date = Column(DateTime(timezone=True), nullable=False)
    statement_end_date = Column(DateTime(timezone=True), nullable=False)
    
    status = Column(String, default="PENDING") # PENDING, VALIDATED, ERROR, RECONCILED
    
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    
    # Relationships
    company = relationship("Customer")
    bank = relationship("Bank")
    currency = relationship("Currency")
    transactions = relationship("BankTransaction", back_populates="statement", cascade="all, delete-orphan")

class BankTransaction(BaseModel):
    """
    Unified model for all bank transaction lines.
    """
    __tablename__ = 'bank_transactions'

    statement_id = Column(Integer, ForeignKey("bank_statements.id"), nullable=False, index=True)
    external_txn_id = Column(String, nullable=True, index=True)
    bank_txn_code = Column(String, nullable=True) # ISO20022 or Bank Specific
    internal_category = Column(String, nullable=True)
    
    # User-requested fields for multi-entity/expanded reporting
    company_name = Column(String, nullable=True) # For statements containing multiple companies
    account_number = Column(String, nullable=True)
    back_office_ref = Column(String, nullable=True)
    category = Column(String, nullable=True)
    sub_category = Column(String, nullable=True)
    net_amount = Column(Numeric(precision=18, scale=2), nullable=True)
    is_positive = Column(Boolean, nullable=True)
    classification_category = Column(String, nullable=True)
    source_system = Column(String, nullable=True)
    exchange_rate_egp = Column(Numeric(precision=18, scale=6), nullable=True)
    exchange_rate_eur_usd = Column(Numeric(precision=18, scale=6), nullable=True)
    beneficiary_name = Column(String, nullable=True)
    purpose_of_payment = Column(String, nullable=True)
    amount_in_currency = Column(Numeric(precision=18, scale=2), nullable=True)
    amount_in_egp = Column(Numeric(precision=18, scale=2), nullable=True)
    transfer_type = Column(String, nullable=True)

    # Dates
    booking_date = Column(DateTime(timezone=True), nullable=False, index=True)
    value_date = Column(DateTime(timezone=True), nullable=False, index=True)
    transaction_date = Column(DateTime(timezone=True), nullable=True)
    system_posting_date = Column(DateTime(timezone=True), nullable=True)
    
    # Money (Account Currency)
    debit_amount = Column(Numeric(precision=18, scale=2), default=0)
    credit_amount = Column(Numeric(precision=18, scale=2), default=0)
    currency = Column(String(3), nullable=False)
    running_balance = Column(Numeric(precision=18, scale=2), nullable=False)
    
    # Foreign Currency (Optional)
    txn_amount_foreign = Column(Numeric(precision=18, scale=2), nullable=True)
    txn_currency_foreign = Column(String(3), nullable=True)
    ex_rate = Column(Numeric(precision=18, scale=6), nullable=True)
    ex_rate_type = Column(String, nullable=True)
    
    # Descriptions
    raw_description = Column(Text, nullable=False)
    cleaned_description = Column(Text, nullable=True)
    description_line1 = Column(String, nullable=True)
    description_line2 = Column(String, nullable=True)
    
    # Counterparty
    counterparty_name = Column(String, nullable=True, index=True)
    counterparty_iban = Column(String, nullable=True)
    counterparty_bank = Column(String, nullable=True)
    
    # References
    cheque_number = Column(String, nullable=True)
    swift_ref = Column(String, nullable=True)
    e2e_id = Column(String, nullable=True, index=True)
    
    # Flags
    is_reconciled = Column(Boolean, default=False, index=True)
    is_classified = Column(Boolean, default=False, index=True)
    is_reversal = Column(Boolean, default=False)
    is_duplicate = Column(Boolean, default=False)
    manual_override = Column(Boolean, default=False)
    is_locked = Column(Boolean, default=False)
    is_exported = Column(Boolean, default=False)
    applied_rule_id = Column(Integer, ForeignKey("classification_rules.id"), nullable=True)
    linked_txn_id = Column(Integer, ForeignKey("bank_transactions.id"), nullable=True)
    
    # Smart Classification Metadata
    classification_source = Column(String, nullable=True)  # BUILTIN, RULE, MANUAL, LEARNED
    classification_confidence = Column(Integer, nullable=True)  # 0-100
    suggested_category = Column(String, nullable=True)  # Pending suggestion, not yet confirmed
    variance_amount = Column(Numeric(precision=18, scale=2), nullable=True)  # Bank fee or FX variance on paired sweeps/reversals

    
    # Relationships
    statement = relationship("BankStatement", back_populates="transactions")
    multi_references = relationship("MultiReference", back_populates="transaction", cascade="all, delete-orphan")
    matches = relationship("ReconciliationMatch", back_populates="bank_txn")
    applied_rule = relationship("ClassificationRule")
    linked_txn = relationship("BankTransaction", remote_side="BankTransaction.id")

    @property
    def applied_rule_name(self):
        return self.applied_rule.rule_name if self.applied_rule else None

class MultiReference(BaseModel):
    """
    Allows multiple references (UETR, Internal, SWIFT) per transaction.
    """
    __tablename__ = 'multi_references'

    transaction_id = Column(Integer, ForeignKey("bank_transactions.id"), nullable=False, index=True)
    ref_type = Column(String, nullable=False) # UETR, Internal, Batch, SWIFT
    ref_value = Column(String, nullable=False, index=True)
    
    transaction = relationship("BankTransaction", back_populates="multi_references")

class ReconciliationMatch(BaseModel):
    """
    Links a bank transaction to one or more internal records (AP, AR, Treasury).
    """
    __tablename__ = 'reconciliation_matches'

    bank_txn_id = Column(Integer, ForeignKey("bank_transactions.id"), nullable=False, index=True)
    source_type = Column(String, nullable=False) # AP, AR, Treasury
    source_record_id = Column(Integer, nullable=False) # ID in the source system
    
    match_type = Column(String, nullable=False) # 1:1, 1:M, M:1, PARTIAL
    match_logic = Column(String, nullable=False) # REFERENCE, EXACT, TOLERANCE, MANUAL
    variance_amount = Column(Numeric(precision=18, scale=2), default=0.00, nullable=True) # Intermediary fee / correspondent variance
    variance_disposition = Column(String(50), default="NONE", nullable=True) # 'NONE', 'BANK_FEE_WRITEOFF', 'FX_DIFFERENCE', 'ROUNDING'
    
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    # created_at is in BaseModel
    
    bank_txn = relationship("BankTransaction", back_populates="matches")

class ClassificationRule(BaseModel):
    """
    User-defined logic for automated GL mapping.
    """
    __tablename__ = 'classification_rules'

    company_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    rule_name = Column(String, nullable=True) # Optional user-friendly name
    rule_type = Column(String, default="LITERAL") # LITERAL or CONCEPT
    priority = Column(Integer, default=100)
    stop_after_match = Column(Boolean, default=True)
    
    conditions_json = Column(JSON, nullable=False) # { "field": "cleaned_description", "op": "contains", "val": "TAX" } or concept dict
    assigned_gl_account = Column(String, nullable=False)
    
    usage_count = Column(Integer, default=0)
    last_triggered_date = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True)
    
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Composite Index for performance
    __table_args__ = (
        Index('idx_classification_rules_priority', 'company_id', 'priority'),
    )

class Counterparty(BaseModel):
    """
    Registry of known counterparties (customers, suppliers, banks, government bodies).
    Supports both tenant-specific entries (company_id) and system-wide global entries (company_id is None).
    Enables concept-based rules:
      - Credit + Customer entity in description -> Collection from Customer (AR)
      - Debit + Supplier entity in description -> Payment to Supplier (AP)
      - Transfer + Bank entity in description -> Inter-bank sweep / Internal transfer
    Can be manually defined or automatically learned and reinforced from customer transactions.
    """
    __tablename__ = 'reconciliation_counterparties'

    company_id = Column(Integer, ForeignKey("customers.id"), nullable=True, index=True) # None = Global for all companies
    name = Column(String, nullable=False, index=True)
    aliases = Column(JSON, nullable=True) # List of alias strings / search keywords e.g. ["ACME", "ACME LLC"]
    entity_type = Column(String, nullable=False, default="CUSTOMER") # CUSTOMER, SUPPLIER, BANK, GOVERNMENT, OTHER
    default_gl_account = Column(String, nullable=True) # e.g. "ACCOUNTS_RECEIVABLE", "ACCOUNTS_PAYABLE"
    default_category = Column(String, nullable=True) # e.g. "COLLECTION_FROM_CUSTOMER", "PAYMENT_TO_SUPPLIER"
    
    learned_count = Column(Integer, default=0) # Times confirmed/learned
    is_verified = Column(Boolean, default=False) # Manually verified vs auto-learned
    is_active = Column(Boolean, default=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    
    company = relationship("Customer")


class CollaborativePattern(BaseModel):
    """
    Dynamic, self-evolving collaborative learning pattern.
    Stores anonymized consensus signatures across tenants.
    Zero customer PII, zero amounts, zero account numbers.
    """
    __tablename__ = 'collaborative_patterns'

    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=True, index=True) # Null = Universal cross-bank, Set = Bank-specific
    signature_token = Column(String, nullable=False, index=True) # Anonymized normalized n-gram e.g. "INSTAPAY COMMISSION"
    direction = Column(String, nullable=False, default="DEBIT") # CREDIT, DEBIT, EITHER
    
    suggested_category = Column(String, nullable=False) # e.g. "BANK_CHARGES", "TAX_DEDUCTION", "COLLECTION"
    suggested_gl_account = Column(String, nullable=True) # e.g. "BANK_CHARGES", "ACCOUNTS_RECEIVABLE"
    
    distinct_tenants_count = Column(Integer, default=1) # Count of distinct companies that validated this pattern
    total_confirmations = Column(Integer, default=1)    # Total times confirmed platform-wide
    agreement_score = Column(Numeric(precision=5, scale=2), default=100.00) # % agreement across tenants
    confidence_score = Column(Integer, default=50)      # Computed 0-100 confidence
    
    is_promoted = Column(Boolean, default=False, index=True) # Promoted to live active prediction when distinct_tenants >= K
    last_confirmed_at = Column(DateTime(timezone=True), server_default=func.now())
    
    bank = relationship("Bank")
    votes = relationship("CollaborativeTenantVote", back_populates="pattern", cascade="all, delete-orphan")


class CollaborativeTenantVote(BaseModel):
    """
    Internal ledger of distinct tenant votes for K-anonymity validation.
    Used exclusively to ensure 1 company = 1 vote and prevent gaming or leakage.
    Never exposed through any client API.
    """
    __tablename__ = 'collaborative_tenant_votes'

    pattern_id = Column(Integer, ForeignKey("collaborative_patterns.id", ondelete="CASCADE"), nullable=False, index=True)
    company_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)
    category_voted = Column(String, nullable=False)
    last_voted_at = Column(DateTime(timezone=True), server_default=func.now())
    
    pattern = relationship("CollaborativePattern", back_populates="votes")
    company = relationship("Customer")


class InternalLedgerRecord(BaseModel):
    """
    Simulated or integrated ERP records (AP Bills, AR Invoices, Payroll Batches, LG Commissions).
    Used for automated dual-pane matching against bank statement lines.
    """
    __tablename__ = 'internal_ledger_records'

    company_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)
    record_type = Column(String(50), nullable=False) # 'AR_INVOICE', 'AP_BILL', 'PAYROLL_BATCH', 'LG_COMMISSION'
    reference_number = Column(String(100), nullable=False, index=True) # e.g. "INV-2026-001", "BILL-2026-042"
    entity_name = Column(String(255), nullable=False, index=True) # Customer or Supplier name
    record_date = Column(DateTime(timezone=True), nullable=False)
    amount = Column(Numeric(precision=18, scale=2), nullable=False) # Positive for AR/Inflow, Negative for AP/Outflow
    currency = Column(String(3), default="EGP")
    gl_account = Column(String(100), nullable=True) # e.g. "4010010 - Revenue", "2010010 - Payables"
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=True) # Associated bank for payment/receipt
    bank_name = Column(String(100), nullable=True) # Cached bank name e.g. "CIB", "HSBC"
    bank_account_number = Column(String(50), nullable=True) # Target bank account number
    status = Column(String(20), default="OPEN", index=True) # 'OPEN', 'RECONCILED'
    matched_bank_txn_id = Column(Integer, ForeignKey("bank_transactions.id"), nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    
    company = relationship("Customer")
    bank = relationship("Bank")
    matched_txn = relationship("BankTransaction")


class ClassificationTaxonomy(BaseModel):
    """
    Hierarchical Classification Taxonomy (Classes & Subclasses).
    Allows corporate admins to define and manage endless categories tailored to their business,
    with default GL account mappings and cash flow directions.
    Supports both global system templates (company_id is None) and company-specific overrides.
    """
    __tablename__ = 'classification_taxonomy'

    company_id = Column(Integer, ForeignKey("customers.id"), nullable=True, index=True) # None = Global Template
    parent_id = Column(Integer, ForeignKey("classification_taxonomy.id", ondelete="CASCADE"), nullable=True, index=True) # None = Class (Level 1), Set = Subclass (Level 2)
    
    code = Column(String(100), nullable=False, index=True) # e.g. "OPEX_SOFTWARE", "TREASURY_SWEEP"
    name = Column(String(255), nullable=False) # e.g. "Software Subscriptions"
    name_ar = Column(String(255), nullable=True) # e.g. "اشتراكات برمجيات"
    
    direction = Column(String(20), default="EITHER") # 'DEBIT', 'CREDIT', 'EITHER'
    default_gl_account = Column(String(100), nullable=True) # e.g. "5020100 - Software Licenses"
    description = Column(String(500), nullable=True)
    is_active = Column(Boolean, default=True)
    order_index = Column(Integer, default=0)

    company = relationship("Customer")
    parent = relationship("ClassificationTaxonomy", remote_side="ClassificationTaxonomy.id", backref="subclasses")


