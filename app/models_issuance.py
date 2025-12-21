# app/models/models_issuance.py

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey, Text, Enum as SQLEnum, Numeric, Date
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base
from app.models import BaseModel # Inherit from your base model

# ==============================================================================
# 1. FACILITY MANAGEMENT (The Limits)
# ==============================================================================

class IssuanceFacility(BaseModel):
    __tablename__ = 'issuance_facilities'

    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    
    reference_number = Column(String, nullable=True, comment="Bank's facility reference number")
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=False)
    total_limit_amount = Column(Numeric(precision=20, scale=2), nullable=False)
    
    start_date = Column(Date, nullable=True)
    expiry_date = Column(Date, nullable=True) # The facility expiry, not LG expiry
    review_date = Column(Date, nullable=True)
    
    is_active = Column(Boolean, default=True)
    
    # SLA & Boundaries
    sla_agreement_days = Column(Integer, nullable=True, comment="Agreed Service Level Agreement in days")
    allow_cross_border = Column(Boolean, default=False, comment="Allows issuance to beneficiaries in other countries")
    allow_third_party_issuance = Column(Boolean, default=False, comment="Allows issuance on behalf of other entities/subsidiaries")
    required_cash_margin_days = Column(Integer, default=0, comment="Days required to deposit cash margin before issuance")
    
    contract_document_path = Column(String, nullable=True)

    # Relationships
    bank = relationship("Bank")
    customer = relationship("Customer")
    currency = relationship("Currency")
    sub_limits = relationship("IssuanceFacilitySubLimit", back_populates="facility")


class IssuanceFacilitySubLimit(BaseModel):
    __tablename__ = 'issuance_facility_sub_limits'

    facility_id = Column(Integer, ForeignKey("issuance_facilities.id"), nullable=False)
    lg_type_id = Column(Integer, ForeignKey("lg_types.id"), nullable=True) # Null means "General" or "All Types"
    
    limit_name = Column(String, nullable=False, comment="e.g. 'Bid Bonds Line' or 'Short Term Line'")
    limit_amount = Column(Numeric(precision=20, scale=2), nullable=False)
    
    # Pricing & Margin defaults for this line
    default_commission_rate = Column(Float, nullable=True, comment="Annual percentage")
    default_cash_margin_pct = Column(Float, nullable=True, comment="Percentage check required")
    default_min_commission = Column(Numeric(precision=10, scale=2), nullable=True)
    
    facility = relationship("IssuanceFacility", back_populates="sub_limits")
    lg_type = relationship("LgType")

class IssuanceWorkflowPolicy(BaseModel):
    """
    Defines the 'Org Chart' approval logic per customer.
    e.g., If Amount between 0-50k -> Require 'Finance Manager'.
    """
    __tablename__ = 'issuance_workflow_policies'

    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    
    min_amount = Column(Numeric(precision=20, scale=2), default=0)
    max_amount = Column(Numeric(precision=20, scale=2), nullable=True, comment="Null means infinite/no cap")
    
    step_sequence = Column(Integer, default=1, comment="Order of approval (1=First, 2=Second)")
    approver_role_name = Column(String, nullable=True, comment="Dynamic role name e.g., 'FINANCE_MANAGER'")
    specific_approver_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, comment="If assigned to a specific person")

    customer = relationship("Customer")
    specific_approver = relationship("User")

# ==============================================================================
# 2. TRANSACTIONS (Requests & Records)
# ==============================================================================

class IssuedLGRecord(BaseModel):
    """
    Represents the LIVE state of an issued LG.
    Created after the first request is executed. 
    Updated by subsequent amendment requests.
    """
    __tablename__ = 'issued_lg_records'

    lg_ref_number = Column(String, unique=True, index=True, nullable=False, comment="The Bank's LG Number")
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    
    # Link to the facility used
    facility_sub_limit_id = Column(Integer, ForeignKey("issuance_facility_sub_limits.id"), nullable=True)
    
    # Current State Data
    beneficiary_name = Column(String, nullable=False)
    current_amount = Column(Numeric(precision=20, scale=2), nullable=False)
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=False)
    
    issue_date = Column(Date, nullable=False)
    expiry_date = Column(Date, nullable=True)
    
    status = Column(String, default="ACTIVE", comment="ACTIVE, EXPIRED, RELEASED, CANCELLED")
    
    # Relationships
    customer = relationship("Customer")
    currency = relationship("Currency")
    sub_limit = relationship("IssuanceFacilitySubLimit")
    
    # History of requests (Original + Amendments)
    requests = relationship("IssuanceRequest", back_populates="lg_record")


class IssuanceRequest(BaseModel):
    """
    Represents a specific TRANSACTION request.
    Can be type: NEW_ISSUANCE, AMENDMENT_INCREASE, AMENDMENT_EXTEND, RELEASE, etc.
    """
    __tablename__ = 'issuance_requests'

    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    requestor_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, comment="Internal user if logged in")
    requestor_name = Column(String, nullable=True, comment="For non-user employees")
    
    # Transaction Type Definition
    transaction_type = Column(String, nullable=False, default="NEW_ISSUANCE", 
                              comment="NEW_ISSUANCE, AMEND_AMOUNT, AMEND_DATE, AMEND_TEXT, RELEASE")
    
    # If this is an amendment, link to the parent record
    lg_record_id = Column(Integer, ForeignKey("issued_lg_records.id"), nullable=True)
    
    # Workflow Status
    status = Column(String, default="DRAFT", index=True, comment="DRAFT, SUBMITTED, APPROVED_INTERNAL, PROCESSING_BANK, COMPLETED, REJECTED")
    current_approval_step = Column(Integer, default=0, comment="Current step in the approval chain")
    pending_approver_role = Column(String, nullable=True, comment="Role currently required to approve (for UI display)")
    approval_chain_audit = Column(JSONB, nullable=True, comment="Log of who approved and when")
    
    # Core Data
    amount = Column(Numeric(precision=20, scale=2), nullable=False)
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=False)
    beneficiary_name = Column(String, nullable=True)
    
    requested_issue_date = Column(Date, nullable=True)
    requested_expiry_date = Column(Date, nullable=True)
    
    selected_issuance_option_id = Column(Integer, ForeignKey("bank_issuance_options.id"), nullable=True)
    selected_issuance_option = relationship("BankIssuanceOption")
    
    # Complex Business Logic Field (The "Smart" JSON)
    # Stores: project_name, tender_ref, delivery_method, specific_clauses, etc.
    business_details = Column(JSONB, nullable=True)
    
    # Relationships
    customer = relationship("Customer")
    currency = relationship("Currency")
    lg_record = relationship("IssuedLGRecord", back_populates="requests")

class BankIssuanceOption(BaseModel):
    """
    Defines available execution paths for a Bank.
    Managed by System Owner.
    """
    __tablename__ = 'bank_issuance_options'

    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    
    # The technical engine to use (matches the Strategy Factory)
    # e.g. "MANUAL_PDF", "BANK_API_V1", "SWIFT_MT760"
    strategy_code = Column(String, nullable=False) 
    
    # The label shown to the user
    # e.g. "Instant Digital Issuance", "Download Paper Form", "Swift Network"
    display_name = Column(String, nullable=False)
    
    # Technical Config (Templates, API URLs, Keys)
    configuration = Column(JSONB, default={}, nullable=False)
    
    is_active = Column(Boolean, default=True)

    bank = relationship("Bank")