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
    priority = Column(Integer, default=100)
    stop_after_match = Column(Boolean, default=True)
    
    conditions_json = Column(JSON, nullable=False) # { "field": "cleaned_description", "op": "contains", "val": "TAX" }
    assigned_gl_account = Column(String, nullable=False)
    
    usage_count = Column(Integer, default=0)
    last_triggered_date = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True)
    
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Composite Index for performance
    __table_args__ = (
        Index('idx_classification_rules_priority', 'company_id', 'priority'),
    )
