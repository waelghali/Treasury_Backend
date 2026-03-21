# app/models/models_issuance.py

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, 
    ForeignKey, Text, Numeric, Date, UniqueConstraint, Index, Table
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base
from app.models import BaseModel # Inherit from your base model

# ==============================================================================
# 0. CORPORATE PROJECTS (Shared entity for facility dedication)
# ==============================================================================

class CorporateProject(Base):
    __tablename__ = 'corporate_projects'

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    name = Column(String, nullable=False, comment="e.g. 'Cairo Metro Phase 3'")
    project_type = Column(String, nullable=False, default='PROJECT', 
                          comment="CONTRACT / PROJECT / PURCHASE_ORDER / TENDER / OTHER")
    reference_number = Column(String, nullable=True, comment="External reference (optional)")
    status = Column(String, default='ACTIVE', comment="ACTIVE / COMPLETED / CANCELLED")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    customer = relationship("Customer", backref="projects")

# ==============================================================================
# 0.5. CUSTOMER BANK ACCOUNTS (How a customer identifies at a bank)
# ==============================================================================

class CustomerBankAccount(Base):
    __tablename__ = 'customer_bank_accounts'

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    entity_id = Column(Integer, ForeignKey("customer_entities.id"), nullable=True,
                       comment="Optional: entity-specific account. NULL = company-level default")
    account_name = Column(String, nullable=False, comment="Name on the bank account")
    account_number = Column(String, nullable=False, comment="Bank account number")
    customer_number = Column(String, nullable=True, comment="Bank CIF / customer number (optional)")
    branch_name = Column(String, nullable=True, comment="Bank branch name (optional)")
    iban = Column(String, nullable=True, comment="IBAN (optional)")
    is_default = Column(Boolean, default=False, comment="Default account for this customer+bank pair")
    is_active = Column(Boolean, default=True)
    is_deleted = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    customer = relationship("Customer")
    bank = relationship("Bank")
    entity = relationship("CustomerEntity")

    __table_args__ = (
        Index('idx_cba_customer_bank', 'customer_id', 'bank_id'),
    )


# ==============================================================================
# 1. FACILITY MANAGEMENT (The Limits)
# ==============================================================================

facility_entities = Table(
    'facility_entities',
    Base.metadata,
    Column('facility_id', Integer, ForeignKey('facilities.id', ondelete="CASCADE"), primary_key=True),
    Column('entity_id', Integer, ForeignKey('customer_entities.id', ondelete="CASCADE"), primary_key=True)
)

class IssuanceFacility(Base):
    __tablename__ = 'facilities'

    # Identity & Core
    id = Column(Integer, primary_key=True, index=True)
    facility_name = Column(String, nullable=False, info={"description": "Readable name for UI"})
    reference_number = Column(String, nullable=True, comment="Bank's facility reference number")
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    entities = relationship("CustomerEntity", secondary=facility_entities, backref="facilities")
    facility_type = Column(String, default="LG", nullable=False) # Future proofing (LC, etc.)
    
    # Foreign Bank specific fields
    foreign_bank_name = Column(String(255), nullable=True)
    foreign_bank_country = Column(String(100), nullable=True)
    foreign_bank_address = Column(Text, nullable=True)
    foreign_bank_swift_code = Column(String(20), nullable=True)

    # Limits & Currency
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=False)
    total_limit_amount = Column(Numeric(precision=20, scale=2), nullable=False)
    tenor_months = Column(Integer, nullable=True, default=1)
    multi_currency_allowed = Column(Boolean, default=False)
    
    # Lifecycle & Governance
    status = Column(String, default="ACTIVE", nullable=False) # ACTIVE, SUSPENDED, EXPIRED, ARCHIVED
    start_date = Column(Date, nullable=True)
    expiry_date = Column(Date, nullable=True) 
    review_date = Column(Date, nullable=True)
    review_required_flag = Column(Boolean, default=False)
    
    # Soft Delete Support
    deleted_at = Column(DateTime, nullable=True)
    is_deleted = Column(Boolean, default=False)
    # Advanced Risk Controls
    fx_breach_auto_suspend = Column(Boolean, default=False, comment="Suspend if FX movement causes limit breach")
    margin_reduces_exposure = Column(Boolean, default=False, comment="If True, cash margin amount is deducted from utilized limit")
    exposure_start_trigger = Column(String, default="ON_ISSUANCE", comment="ON_APPROVAL or ON_ISSUANCE")
    facility_default_margin_pct = Column(Numeric(precision=5, scale=2), nullable=True)
    
    # SLA & Boundaries
    sla_agreement_days = Column(Integer, nullable=True, comment="Agreed Service Level Agreement in days")
    allow_cross_border = Column(Boolean, default=False, comment="Allows issuance to beneficiaries in other countries")
    allow_third_party_issuance = Column(Boolean, default=False, comment="Allows issuance on behalf of other entities")
    required_cash_margin_days = Column(Integer, default=0, comment="Days required to deposit cash margin before issuance")
    
    internal_notes = Column(Text, nullable=True)
    contract_document_path = Column(String, nullable=True)
    agreement_analysis = Column(JSONB, nullable=True, comment="H1: AI-extracted facility agreement terms")
    
    # Bank Account Link
    bank_account_id = Column(Integer, ForeignKey("customer_bank_accounts.id"), nullable=True,
                             comment="The customer's bank account used for this facility")
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    # Indexes
    __table_args__ = (
        Index('idx_facility_customer_bank_status', 'customer_id', 'bank_id', 'status'),
    )

    # Relationships
    bank = relationship("Bank")
    customer = relationship("Customer")
    currency = relationship("Currency")
    bank_account = relationship("CustomerBankAccount")
    sub_limits = relationship(
        "IssuanceFacilitySubLimit", 
        backref="facility_parent",  # Changed from "facility" to "facility_parent"
        cascade="all, delete-orphan",
        primaryjoin="IssuanceFacility.id == IssuanceFacilitySubLimit.facility_id"
    )
    exposure_entries = relationship("IssuanceExposureEntry", back_populates="facility")


class IssuanceFacilitySubLimit(Base):
    __tablename__ = 'issuance_facility_sub_limits'

    id = Column(Integer, primary_key=True, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id", ondelete="CASCADE"))
    lg_type_ids = Column(JSONB, nullable=False, default=list)    
    limit_name = Column(String, nullable=False, comment="e.g. 'Bid Bonds Line'")
    limit_amount = Column(Numeric(precision=20, scale=2), nullable=False)
    
    # Validation Rules
    max_amount_per_lg = Column(Numeric(precision=20, scale=2), nullable=True)
    max_tenor_days = Column(Integer, nullable=True)
    allowed_countries = Column(
        JSONB, 
        nullable=True, 
        comment="Structure: {'type': 'ALLOW'|'EXCLUDE', 'countries': ['US', 'AE']}"
    )
    allows_confirmation = Column(Boolean, default=False)
    
    # Initial utilization for onboarding existing facilities (not new ones)
    initial_utilization = Column(Numeric(precision=20, scale=2), default=0, nullable=False,
                                 comment="Pre-existing utilization when facility is first registered in the system")
    
    # Pricing & Margin (Using Numeric for financial integrity)
    default_commission_rate = Column(Numeric(precision=8, scale=6), nullable=True)
    default_cash_margin_pct = Column(Numeric(precision=5, scale=2), nullable=True)
    default_min_commission = Column(Numeric(precision=10, scale=2), nullable=True)
    default_flat_fee = Column(Numeric(10, 2), default=0.0, nullable=True)
    
    # Dedication: restrict this sub-limit to specific projects/contracts
    dedicated_project_ids = Column(
        JSONB, nullable=True,
        comment="Array of CorporateProject IDs this sub-limit is earmarked for"
    )

    facility = relationship("IssuanceFacility", back_populates="sub_limits", overlaps="facility_parent")

class IssuanceFacilityAuditLog(BaseModel):
    """Immutable log of all changes to facilities"""
    __tablename__ = 'issuance_facility_audit_logs'
    
    id = Column(Integer, primary_key=True, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=False)
    performed_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    action = Column(String, nullable=False) # CREATE, UPDATE, SUSPEND, DELETE, RESTORE
    old_values = Column(JSONB, nullable=True)
    new_values = Column(JSONB, nullable=True)
    timestamp = Column(DateTime, server_default=func.now())


class IssuanceWorkflowPolicy(BaseModel):
    __tablename__ = 'issuance_workflow_policies'

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    step_sequence = Column(Integer, default=1)
    
    # 1. Trigger Condition
    condition_type = Column(String, default="ALWAYS", nullable=False) # ALWAYS, AMOUNT_OVER, DEPT_MATCH, CROSS_BORDER, THIRD_PARTY
    condition_value = Column(String, nullable=True) # Holds amount threshold, dept_id, etc.
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=True) # For AMOUNT_OVER
    
    # 2. Approver Pool
    approver_type = Column(String, default="ROLE", nullable=False) # ROLE, USERS, DEPT_HEAD
    approver_values = Column(JSONB, default=list, nullable=False) # List of roles or specific user IDs
    
    # 3. Signatures
    required_signatures = Column(Integer, default=1, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    customer = relationship("Customer")
    currency = relationship("Currency")

# ==============================================================================
# 2. EXPOSURE ENGINE (The Ledger)
# ==============================================================================

class IssuanceExposureEntry(Base):
    __tablename__ = 'issuance_exposure_entries'
    
    id = Column(Integer, primary_key=True, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=False)
    sub_limit_id = Column(Integer, ForeignKey("issuance_facility_sub_limits.id"), nullable=False)
    lg_record_id = Column(Integer, ForeignKey("issued_lg_records.id"), nullable=True)
    request_id = Column(Integer, ForeignKey("issuance_requests.id"), nullable=False)
    
    entry_type = Column(String, nullable=False) # ISSUANCE, AMEND_INCREASE, RELEASE, etc.
    original_amount_delta = Column(Numeric(precision=20, scale=2), nullable=False)
    original_currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=False)
    
    fx_rate_used = Column(Numeric(precision=15, scale=6), default=1.0)
    facility_equivalent_delta = Column(Numeric(precision=20, scale=2), nullable=False)
    
    is_active = Column(Boolean, default=True, index=True)
    effective_date = Column(Date, server_default=func.current_date())

    facility = relationship("IssuanceFacility", back_populates="exposure_entries")
    request = relationship("IssuanceRequest")
    
# ==============================================================================
# 3. TRANSACTIONS (Requests & Records)
# ==============================================================================

class IssuedLGRecord(Base):
    __tablename__ = 'issued_lg_records'

    id = Column(Integer, primary_key=True, index=True)
    lg_ref_number = Column(String, unique=True, index=True, nullable=False)
    internal_serial = Column(String, unique=True, index=True, nullable=True,
                             comment="Auto-generated: YYYY-XXXX-NNNNSSS where XXXX=entity code, NNNN=seq, SSS=sub-serial")
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    facility_sub_limit_id = Column(Integer, ForeignKey("issuance_facility_sub_limits.id"), nullable=True)
    request_id = Column(Integer, ForeignKey("issuance_requests.id"), nullable=True, comment="Link back to originating request")
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=True, comment="Bank this LG was issued to")
    
    beneficiary_name = Column(String, nullable=False)
    current_amount = Column(Numeric(precision=20, scale=2), nullable=False)
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=False)
    
    issue_date = Column(Date, nullable=True, comment="Set from bank reply, NOT at execution")
    expiry_date = Column(Date, nullable=True)
    status = Column(String, default="PENDING_CONFIRMATION", comment="PENDING_CONFIRMATION, ACTIVE, EXPIRED, CANCELLED")
    
    # Accountability & Issuance Method
    issued_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    issuance_method = Column(String, nullable=True, comment="MANUAL_PDF, BANK_API, PRE_PRINTED_FORM")
    
    # Step 5.5a: Delivery Tracking
    delivery_date = Column(Date, nullable=True, comment="When bank form was delivered to bank")
    delivery_method = Column(String, nullable=True, comment="HAND_DELIVERY, COURIER, EMAIL, OTHER")
    delivery_notes = Column(Text, nullable=True, comment="Free-text delivery notes")

    # Step 5.5b: Bank Reply Tracking
    bank_reply_type = Column(String, nullable=True, comment="LG_ISSUED, INQUIRY, REJECTED, NO_RESPONSE")
    bank_reply_date = Column(Date, nullable=True, comment="When bank reply was received")
    bank_reply_notes = Column(Text, nullable=True, comment="Bank reply details / inquiry questions")
    bank_inquiry_log = Column(JSONB, nullable=True, default=list, comment="[{date, notes, type, logged_by_user_id, created_at}] — intermediate inquiry/correction notes")
    bank_lg_number = Column(String, nullable=True, comment="LG number assigned by bank")
    bank_lg_issue_date = Column(Date, nullable=True, comment="Actual issue date from bank")
    bank_lg_expiry_date = Column(Date, nullable=True, comment="Expiry date from bank")
    bank_lg_amount = Column(Numeric(precision=20, scale=2), nullable=True, comment="Amount confirmed by bank")

    # Step 5.6: LG Copy Verification
    verification_status = Column(String, nullable=True, comment="PENDING, MATCHED, DISCREPANCY, ACCEPTED")
    verification_notes = Column(Text, nullable=True, comment="Discrepancy details or acceptance notes")
    verified_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    verified_at = Column(DateTime, nullable=True)

    # Step 5.7: LG Handover (always physical delivery)
    handover_date = Column(Date, nullable=True, comment="When LG was handed over to recipient")
    handover_notes = Column(Text, nullable=True, comment="Free-text handover notes")
    handover_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, comment="User who performed handover")
    recipient_name = Column(String, nullable=True, comment="Name of person receiving the LG")
    recipient_email = Column(String, nullable=True, comment="Recipient's email")
    recipient_department = Column(String, nullable=True, comment="Recipient's department (configurable)")
    recipient_job_title = Column(String, nullable=True, comment="Recipient's job title (configurable)")
    recipient_phone = Column(String, nullable=True, comment="Recipient's phone (configurable)")
    recipient_employee_id = Column(String, nullable=True, comment="Recipient's employee ID (configurable)")
    recipient_manager_email = Column(String, nullable=True, comment="Recipient's manager email (configurable)")
    recipient_second_line_manager_email = Column(String, nullable=True, comment="Recipient's 2nd line manager (configurable)")
    handover_signed_copy_path = Column(String, nullable=True, comment="GCS path to signed receiving document")

    # Bank Confirmation Tracking (legacy — kept for backward compat)
    bank_confirmation_date = Column(Date, nullable=True)
    bank_confirmation_ref = Column(String, nullable=True, comment="Ref number returned by bank upon confirmation")
    
    # Custody Chain
    original_copy_collected_by = Column(String, nullable=True)
    original_copy_collected_date = Column(Date, nullable=True)
    soft_copy_path = Column(String, nullable=True, comment="GCS path or local path to scanned LG")
    custody_holder = Column(String, nullable=True, comment="Current holder of the original LG")
    custody_transfer_log = Column(JSONB, nullable=True, default=list, comment="[{from, to, date, notes}]")
    
    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    
    # Maintenance history — full audit trail of all changes
    action_history = Column(JSONB, nullable=True, default=list,
                           comment="[{action_type, before, after, user_id, timestamp, notes}]")
    
    # A4: Ownership tracking (mirrors custody internal_owner concept)
    current_owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True,
                                   comment="Current owner — initially the requestor, can be transferred")
    
    # A3: Reference validity flag (set by background task)
    reference_validity_flag = Column(String, nullable=True,
                                     comment="VALID / EXCEEDED — set when LG expiry > reference end date")

    # D1: Complete field copy from request (self-contained record — no joins needed)
    issuing_entity_id = Column(Integer, ForeignKey("customer_entities.id"), nullable=True,
                               comment="Entity that requested the LG")
    lg_type_id = Column(Integer, ForeignKey("lg_types.id"), nullable=True,
                        comment="LG type from the issuance request")
    beneficiary_address = Column(String, nullable=True)
    beneficiary_country = Column(String, nullable=True)
    department = Column(String, nullable=True, comment="Department of the requestor")
    project_id = Column(Integer, ForeignKey("corporate_projects.id"), nullable=True)
    is_cross_border = Column(Boolean, default=False)
    is_third_party = Column(Boolean, default=False)
    reference_type = Column(String, nullable=True, comment="Contract / Project / PO / Other")
    lg_purpose = Column(Text, nullable=True)
    lg_payable_currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=True,
                                    comment="Payable currency (may differ from LG denomination currency)")
    requested_issue_date = Column(Date, nullable=True, comment="Original requested issue date from the request")

    # D3: Manual pricing for "other bank" (no facility) issuances
    manual_pricing = Column(JSONB, nullable=True,
                           comment="{commission_rate, flat_fee, margin_pct, margin_amount, agreed_sla, notes}")

    customer = relationship("Customer")
    currency = relationship("Currency", foreign_keys=[currency_id])
    bank = relationship("Bank")
    sub_limit = relationship("IssuanceFacilitySubLimit")
    issued_by = relationship("User", foreign_keys=[issued_by_user_id])
    verified_by = relationship("User", foreign_keys=[verified_by_user_id])
    handover_by = relationship("User", foreign_keys=[handover_by_user_id])
    current_owner = relationship("User", foreign_keys=[current_owner_user_id])
    issuing_entity = relationship("CustomerEntity", foreign_keys=[issuing_entity_id])
    lg_type = relationship("LgType")
    project = relationship("CorporateProject", foreign_keys=[project_id])
    payable_currency = relationship("Currency", foreign_keys=[lg_payable_currency_id])
    requests = relationship("IssuanceRequest", back_populates="lg_record", foreign_keys="IssuanceRequest.lg_record_id")
    maintenance_actions = relationship("IssuanceMaintenanceAction", back_populates="issued_lg", cascade="all, delete-orphan")

class BankIssuanceOption(Base):
    __tablename__ = 'bank_issuance_options'

    id = Column(Integer, primary_key=True, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    strategy_code = Column(String, nullable=False) 
    display_name = Column(String, nullable=False)
    configuration = Column(JSONB, default={}, nullable=False)
    is_active = Column(Boolean, default=True)

    bank = relationship("Bank")


# ==============================================================================
# ISSUANCE MAINTENANCE ACTIONS
# ==============================================================================

class IssuanceMaintenanceAction(BaseModel):
    """
    Tracks maintenance actions on issued LGs: Extend, Increase Amount,
    Close/Return, Liquidation, Amendment, Activate Non-Operative.
    Mirrors custody LGInstruction lifecycle but from the issuer side.
    """
    __tablename__ = 'issuance_maintenance_actions'

    # Core link
    issued_lg_id = Column(Integer, ForeignKey("issued_lg_records.id"), nullable=False, index=True)
    action_type = Column(String, nullable=False, index=True,
                         comment="EXTEND, INCREASE_AMOUNT, CLOSE, LIQUIDATION, AMENDMENT, ACTIVATE")
    status = Column(String, nullable=False, default="PENDING_APPROVAL",
                    comment="PENDING_APPROVAL, APPROVED, EXECUTED, REJECTED, CANCELLED")

    # Action-specific data (flexible JSON)
    action_data = Column(JSONB, nullable=True,
                         comment="{new_expiry_date, new_amount, amendment_text, liquidation_type, liquidation_amount, ...}")

    # Approval matrix integration (same pattern as IssuanceRequest)
    pending_approver_users = Column(JSONB, nullable=True, default=list,
                                    comment="JSON list of user IDs for current pending step")
    current_step_number = Column(Integer, nullable=True, default=0)
    approval_history = Column(JSONB, nullable=True, default=list,
                              comment="[{step, user_id, decision, timestamp, notes}]")

    # Letter / instruction lifecycle (mirrors LGInstruction)
    letter_template_id = Column(Integer, ForeignKey("templates.id"), nullable=True)
    letter_generated_path = Column(String, nullable=True, comment="GCS path to generated PDF")
    letter_serial_number = Column(String, nullable=True, unique=True,
                                  comment="e.g., ISS-EXT-2026-001")
    instruction_status = Column(String, nullable=True,
                                comment="Instruction Issued, Instruction Delivered, Confirmed by Bank")
    is_printed = Column(Boolean, default=False, nullable=False)

    # Delivery tracking
    delivery_date = Column(DateTime(timezone=True), nullable=True)
    delivery_method = Column(String, nullable=True, comment="HAND_DELIVERY, COURIER, EMAIL")
    delivery_notes = Column(Text, nullable=True)
    delivery_document_path = Column(String, nullable=True, comment="GCS path to delivery proof document")

    # Bank reply tracking
    bank_reply_date = Column(DateTime(timezone=True), nullable=True)
    bank_reply_notes = Column(Text, nullable=True)
    bank_reply_document_path = Column(String, nullable=True, comment="GCS path to bank reply letter/document")

    # Initiation tracking
    initiation_source = Column(String, nullable=True, default="INTERNAL_USER",
                               comment="INTERNAL_USER, REQUESTOR_PORTAL, BANK_INITIATED")

    # Tracking
    initiated_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    executed_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    notes = Column(Text, nullable=True)

    # Relationships
    issued_lg = relationship("IssuedLGRecord", back_populates="maintenance_actions")
    letter_template = relationship("Template")
    initiated_by = relationship("User", foreign_keys=[initiated_by_user_id])
    executed_by = relationship("User", foreign_keys=[executed_by_user_id])

    def __repr__(self):
        return f"<IssuanceMaintenanceAction(id={self.id}, type='{self.action_type}', status='{self.status}')>"


class BankFormTemplate(Base):
    """
    Stores uploaded bank PDF forms and their AI-generated field mappings.
    Each bank can have multiple form templates (different form versions).
    AI analyzes the PDF once; the cached field_mapping is used on every fill.
    """
    __tablename__ = 'bank_form_templates'

    id = Column(Integer, primary_key=True, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False, index=True)
    name = Column(String, nullable=False)                    # e.g., "ENBD LG Request Form v3"
    version = Column(Integer, default=1, nullable=False)     # Increments on re-upload
    form_type = Column(String, nullable=False, default="FILLABLE_PDF")  # FILLABLE_PDF | PHYSICAL_OVERLAY | SCANNED_FILL

    # Storage
    file_path = Column(String, nullable=True)                # File path to the original uploaded PDF
    original_filename = Column(String, nullable=True)        # Original upload filename
    
    # LG Type targeting: null/empty = universal (any LG type), list = specific types only
    # e.g. [1, 2, 3] = only for LG types with IDs 1, 2, 3.  NULL/[] = works for all types.
    lg_type_ids = Column(JSONB, nullable=True, default=None,
                         comment="JSON array of lg_type IDs this form covers. NULL/empty = universal.")

    # AI-generated mapping (cached — AI runs once, fills use cache)
    # Format: [{"pdf_field_name": "...", "label": "...", "mapped_to": "beneficiary_name", "source": "request_data|customer_data", "confidence": 0.95}, ...]
    field_mapping = Column(JSONB, nullable=True)

    # Full AI analysis result for reference/debugging
    ai_analysis = Column(JSONB, nullable=True)
    ai_analysis_status = Column(String, default="PENDING")   # PENDING | ANALYZING | COMPLETED | FAILED

    # Language
    form_language = Column(String, default="BILINGUAL", nullable=False, comment="AR / EN / BILINGUAL — language of this form template")

    # Status
    is_active = Column(Boolean, default=True)
    priority = Column(Integer, default=0, nullable=False, comment="Higher = preferred. Used to rank forms for auto-selection.")
    is_deleted = Column(Boolean, default=False)

    # Audit
    uploaded_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    bank = relationship("Bank")
    uploader = relationship("User", foreign_keys=[uploaded_by])


class FormFieldUserValue(Base):
    """
    Stores user-provided values for bank form fields that couldn't be 
    auto-filled from system data. Values persist per customer+form template
    and are pre-populated on subsequent fills with the same form.
    """
    __tablename__ = 'form_field_user_values'

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    form_template_id = Column(Integer, ForeignKey("bank_form_templates.id"), nullable=False)
    pdf_field_name = Column(String, nullable=False)
    saved_value = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    customer = relationship("Customer")
    form_template = relationship("BankFormTemplate")

    __table_args__ = (
        Index('idx_ffuv_cust_form', 'customer_id', 'form_template_id'),
    )


# ==============================================================================
# ISSUANCE REQUEST MODULE (PHASE 1)
# ==============================================================================

class IssuanceRequest(BaseModel):
    __tablename__ = 'issuance_requests'

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    
    # SECTION 1: SYSTEM
    serial_number = Column(String, unique=True, index=True, nullable=False, comment="e.g., REQ-YYYY-XXXX")
    status = Column(String, default="DRAFT", index=True, nullable=False)
    transaction_type = Column(String, nullable=False, default="NEW_ISSUANCE")
    
    # SECTION 2: ISSUING ENTITY
    issuing_entity_id = Column(Integer, ForeignKey("customer_entities.id"), nullable=True)
    
    # SECTION 3: REQUESTOR INFO
    requestor_user_id = Column(Integer, ForeignKey("users.id"), nullable=True) # Linked if internal user
    requestor_name = Column(String, nullable=True)
    requestor_email = Column(String, nullable=True)
    department = Column(String, nullable=True, default="General")
    job_title = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)
    employee_id = Column(String, nullable=True)
    manager_email = Column(String, nullable=True)
    second_line_manager_email = Column(String, nullable=True)
    
    # SECTION 4: UNDERLYING REFERENCE
    reference_type = Column(String, nullable=True) # Contract / Project / PO / Other
    reference_number = Column(String, nullable=True)
    reference_amount = Column(Numeric(precision=20, scale=2), nullable=True)
    reference_currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=True)
    reference_start_date = Column(Date, nullable=True)
    reference_end_date = Column(Date, nullable=True)
    project_id = Column(Integer, ForeignKey("corporate_projects.id"), nullable=True,
                        comment="Link to CorporateProject for facility dedication matching")
    
    # SECTION 5: LG CORE INFO
    lg_type_id = Column(Integer, ForeignKey("lg_types.id"), nullable=True)
    lg_purpose = Column(Text, nullable=True)
    amount = Column(Numeric(precision=20, scale=2), nullable=True) # Maps to LG Amount
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=True) # Maps to LG Currency
    requested_issue_date = Column(Date, nullable=True, default=func.current_date()) # Suggested Start
    requested_expiry_date = Column(Date, nullable=True) # Maturity Date
    payable_currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=True) # New field
    operational_status = Column(String, nullable=True) # Operative / Non-Operative (Rule bound)
    lg_language = Column(String, nullable=False, default="AR", comment="AR / EN — language for LG issuance")
    is_auto_reducing = Column(Boolean, default=False)
    reduction_trigger = Column(Text, nullable=True)  # Describes the reduction trigger condition
    
    # SECTION 6: BENEFICIARY INFO
    beneficiary_id_number = Column(String, nullable=True, index=True)  # Unique beneficiary identifier
    beneficiary_name = Column(String, nullable=True)
    beneficiary_address = Column(String, nullable=True)
    beneficiary_contact_person = Column(String, nullable=True)
    beneficiary_phone = Column(String, nullable=True)
    beneficiary_email = Column(String, nullable=True)
    beneficiary_country = Column(String, nullable=True)
    
    # SECTION 7: CONDITIONAL SECTIONS
    is_third_party = Column(Boolean, default=False)
    third_party_name = Column(String, nullable=True)
    third_party_address = Column(String, nullable=True)
    third_party_relationship = Column(String, nullable=True)
    
    is_cross_border = Column(Boolean, default=False)
    issuance_country = Column(String, nullable=True)
    
    requires_special_wording = Column(Boolean, default=False)
    is_urgent = Column(Boolean, default=False)
    urgency_justification = Column(Text, nullable=True)
    
    # SECTION 8 & 10: COMMENTS, CONDITIONS & CUSTOM FIELDS
    other_conditions = Column(Text, nullable=True)  # Free-text other conditions/requirements
    comments = Column(Text, nullable=True)
    custom_field_1_value = Column(String, nullable=True)
    custom_field_2_value = Column(String, nullable=True)
    
    # GOVERNANCE & VERSIONING
    current_version_number = Column(Integer, default=1, nullable=False)
    locked_for_issuance = Column(Boolean, default=False, nullable=False)
    
    # APPROVAL TRACKING (Existing fields maintained)
    current_approval_step = Column(Integer, default=0)
    pending_approver_role = Column(String, nullable=True) 
    pending_approver_users = Column(JSONB, nullable=True) 
    signatures_collected = Column(Integer, default=0)     
    approval_chain_audit = Column(JSONB, nullable=True)   

    # RETURN-FOR-REVISION TRACKING
    revision_notes = Column(Text, nullable=True, comment="Notes from approver when returning for revision")
    returned_from_step = Column(Integer, nullable=True, comment="Step number that returned the request — resubmit resumes here")

    # FACILITY SELECTION (Set when user picks the facility/sub-limit, before execution)
    selected_sub_limit_id = Column(Integer, ForeignKey("issuance_facility_sub_limits.id"), nullable=True)

    # C1: Applicable Rules (URDG 758, ISP98, Local Law, None)
    applicable_rules = Column(String, nullable=True,
                              comment="URDG_758, ISP_98, LOCAL_LAW, or NULL for no specific rules")

    # C2: Cross-border enriched details (JSONB — only populated when is_cross_border=True)
    cross_border_details = Column(JSONB, nullable=True,
                                  comment="""JSON: {advising_bank_name, advising_bank_country, advising_bank_swift,
                                  requires_counter_guarantee, counter_guarantee_bank_name, counter_guarantee_ref,
                                  governing_law_country, place_of_jurisdiction,
                                  delivery_channel, beneficiary_bank_name, beneficiary_bank_swift,
                                  sanctions_screening_required, sanctions_screening_notes}""")

    # C3: Treasury Enrichment (flexible JSONB for technical input by treasury team)
    treasury_enrichment = Column(JSONB, nullable=True,
                                 comment="""JSON: {applicable_rules override, advising_bank, margin_instructions,
                                 internal_notes, enriched_by_user_id, enriched_at, ...}""")

    # EXECUTION LINK (Populated post-issuance)
    lg_record_id = Column(Integer, ForeignKey("issued_lg_records.id"), nullable=True)
    cancellation_reason = Column(Text, nullable=True)

    # SYSTEM METADATA (flexible JSON for TTL tracking, flags, etc.)
    metadata_json = Column(JSONB, nullable=True, default=dict, comment="Flexible system metadata: reservation_ttl_status, auto_released_at, etc.")

    # Relationships
    customer = relationship("Customer")
    issuing_entity = relationship("CustomerEntity", foreign_keys=[issuing_entity_id])
    lg_type = relationship("LgType")
    currency = relationship("Currency", foreign_keys=[currency_id])
    payable_currency = relationship("Currency", foreign_keys=[payable_currency_id])
    reference_currency = relationship("Currency", foreign_keys=[reference_currency_id])
    project = relationship("CorporateProject", foreign_keys=[project_id], lazy='joined')
    lg_record = relationship("IssuedLGRecord", back_populates="requests", foreign_keys=[lg_record_id])
    versions = relationship("IssuanceRequestVersion", back_populates="request", cascade="all, delete-orphan")
    documents = relationship("IssuanceRequestDocument", back_populates="request", cascade="all, delete-orphan")


class IssuanceRequestSnapshot(BaseModel):
    """Immutable V1 copy captured at the exact moment of 'Submit'."""
    __tablename__ = 'issuance_request_snapshots'
    
    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(Integer, ForeignKey('issuance_requests.id'), unique=True, nullable=False)
    snapshot_data = Column(JSONB, nullable=False) # Full JSON dump of V1

    request = relationship("IssuanceRequest")


class IssuanceRequestVersion(BaseModel):
    """Linear version history for Treasury edits (N+1)."""
    __tablename__ = 'issuance_request_versions'
    
    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(Integer, ForeignKey('issuance_requests.id'), nullable=False)
    version_number = Column(Integer, nullable=False)
    edited_by_user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    
    change_reason = Column(Text, nullable=True) # Mandatory if post-approval
    changed_fields = Column(JSONB, nullable=False) # e.g., {"amount": {"old": 100, "new": 200}}

    request = relationship("IssuanceRequest", back_populates="versions")


class IssuanceRequestDocument(BaseModel):
    """Documents uploaded to the request (Contract, PO, Special Wording)."""
    __tablename__ = 'issuance_request_documents'
    
    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(Integer, ForeignKey('issuance_requests.id'), nullable=False)
    document_type = Column(String, nullable=False) # 'CONTRACT', 'SPECIAL_WORDING', 'FORMAL_REQUEST'
    file_name = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    uploaded_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    ai_verification_result = Column(JSONB, nullable=True,
                                     comment="AI verification comparison result: {status, comparison, mismatches, summary}")

    request = relationship("IssuanceRequest", back_populates="documents")


class CustomerFormConfiguration(BaseModel):
    """Admin configuration for standardizing form visibility and requirements."""
    __tablename__ = 'customer_form_configurations'
    
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey('customers.id'), unique=True, nullable=False)
    
    # Only stores toggles for fields that are allowed to be configured. 
    # e.g., {"department": {"is_visible": true, "is_mandatory": false}}
    field_configurations = Column(JSONB, nullable=False, default={}) 
    
    # Custom Field configurations
    custom_field_1_config = Column(JSONB, nullable=True) # {"label": "Cost Center", "type": "TEXT", "is_visible": True, "is_mandatory": False}
    custom_field_2_config = Column(JSONB, nullable=True)
    
    mandatory_document_types = Column(JSONB, nullable=False, default=["FORMAL_REQUEST"])
    
    # Configurable reference types (defaults in schema if empty)
    reference_types = Column(JSONB, nullable=True, comment="[{id, name}] — configurable list of reference types")
    
    # Document upload configuration per type
    document_config = Column(JSONB, nullable=True, comment="{DOC_TYPE: {is_visible, is_mandatory}}")

    # Recipient field configurations (mirrors requestor field config for LG handover)
    recipient_field_configurations = Column(JSONB, nullable=True, default={}, comment="Same format as field_configurations for handover recipient fields")

    # C6: Reservation TTL configuration
    reservation_ttl_days = Column(Integer, nullable=True, default=14, comment="Days before unused reservations are auto-released. Default: 14")


# ==============================================================================
# 7. LG POSITION RECONCILIATION
# ==============================================================================

class ReconciliationSession(BaseModel):
    """A single reconciliation run — one bank position report uploaded & processed."""
    __tablename__ = 'reconciliation_sessions'

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    position_date = Column(Date, nullable=False, comment="Snapshot date of the bank position report")

    uploaded_file_path = Column(String, nullable=True, comment="GCS path to original uploaded file")
    original_file_name = Column(String, nullable=True, comment="Original filename for display")
    file_format = Column(String, nullable=True, comment="EXCEL, CSV, PDF, TEXT, MANUAL")
    parsing_method = Column(String, nullable=True, comment="TABULAR, AI, MANUAL")

    status = Column(String, default="CREATED",
                    comment="CREATED, PARSING, PARSED, MATCHING, MATCHED, REVIEW, COMPLETED, FAILED")

    # Summary statistics (populated after matching)
    total_bank_records = Column(Integer, default=0)
    bank_reported_total = Column(Numeric(precision=20, scale=2), nullable=True,
                                  comment="Total amount reported by bank (for completeness check)")
    bank_reported_count = Column(Integer, nullable=True,
                                  comment="Record count reported by bank (for completeness check)")
    matched_count = Column(Integer, default=0)
    mismatched_count = Column(Integer, default=0)
    bank_only_count = Column(Integer, default=0)
    system_only_count = Column(Integer, default=0)

    # G3: Completeness check visibility
    completeness_status = Column(String, nullable=True,
                                  comment="OK, COUNT_MISMATCH, NOT_CHECKED — result of count validation")
    completeness_note = Column(Text, nullable=True,
                                comment="e.g. 'Bank reported 45 records but parsed 42'")

    ai_usage_log = Column(JSONB, nullable=True, comment="Token usage if AI was used for parsing")
    error_message = Column(Text, nullable=True, comment="Error details if status=FAILED")

    reviewed_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    customer = relationship("Customer")
    bank = relationship("Bank")
    created_by = relationship("User", foreign_keys=[created_by_user_id])
    reviewed_by = relationship("User", foreign_keys=[reviewed_by_user_id])
    bank_rows = relationship("ReconciliationBankRow", back_populates="session", cascade="all, delete-orphan")
    results = relationship("ReconciliationResult", back_populates="session", cascade="all, delete-orphan")


class ReconciliationBankRow(Base):
    """A single parsed row from the bank's position report."""
    __tablename__ = 'reconciliation_bank_rows'

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey("reconciliation_sessions.id", ondelete="CASCADE"), nullable=False)

    bank_lg_number = Column(String, nullable=True, comment="Bank's LG reference number")
    beneficiary_name = Column(String, nullable=True)
    amount = Column(Numeric(precision=20, scale=2), nullable=True)
    currency_code = Column(String, nullable=True)
    issue_date = Column(Date, nullable=True)
    expiry_date = Column(Date, nullable=True)

    raw_data = Column(JSONB, nullable=True, comment="Original pre-normalization values from the file")

    match_status = Column(String, default="UNMATCHED",
                          comment="MATCHED, PARTIAL_MATCH, BANK_ONLY, UNMATCHED")
    matched_lg_id = Column(Integer, ForeignKey("issued_lg_records.id"), nullable=True)
    variances = Column(JSONB, nullable=True,
                       comment="[{field, bank_value, system_value, severity}]")

    created_at = Column(DateTime, server_default=func.now())

    session = relationship("ReconciliationSession", back_populates="bank_rows")
    matched_lg = relationship("IssuedLGRecord")


class ReconciliationResult(Base):
    """Individual mismatch/flag from reconciliation — one per variance field."""
    __tablename__ = 'reconciliation_results'

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey("reconciliation_sessions.id", ondelete="CASCADE"), nullable=False)
    bank_row_id = Column(Integer, ForeignKey("reconciliation_bank_rows.id", ondelete="CASCADE"), nullable=True,
                         comment="NULL for SYSTEM_ONLY mismatches")
    issued_lg_id = Column(Integer, ForeignKey("issued_lg_records.id"), nullable=True,
                          comment="NULL for BANK_ONLY mismatches")

    mismatch_type = Column(String, nullable=False,
                           comment="AMOUNT, EXPIRY, INITIAL_DATA, BANK_ONLY, SYSTEM_ONLY")
    severity = Column(String, nullable=False, comment="HIGH, MEDIUM, LOW, INFO")
    field_name = Column(String, nullable=True, comment="Which field differs")
    bank_value = Column(String, nullable=True)
    system_value = Column(String, nullable=True)

    # User resolution
    user_resolution = Column(String, nullable=True,
                             comment="ADJUSTED, DISPUTE, IGNORE")
    resolution_notes = Column(Text, nullable=True)
    resolved_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    resolved_at = Column(DateTime, nullable=True)

    # Approval workflow (for ADJUSTED resolutions — requires corp admin)
    approval_status = Column(String, nullable=True,
                             comment="PENDING_APPROVAL, APPROVED, REJECTED — only for ADJUSTED resolution")
    approved_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    approved_at = Column(DateTime, nullable=True)
    record_updated = Column(Boolean, default=False,
                            comment="Whether the IssuedLGRecord was actually updated")

    created_at = Column(DateTime, server_default=func.now())

    session = relationship("ReconciliationSession", back_populates="results")
    bank_row = relationship("ReconciliationBankRow")
    issued_lg = relationship("IssuedLGRecord")
    resolved_by = relationship("User", foreign_keys=[resolved_by_user_id])
    approved_by = relationship("User", foreign_keys=[approved_by_user_id])


class BankColumnMapping(Base):
    """Cached column mapping for a bank's position report format — avoids repeat AI/manual work."""
    __tablename__ = 'bank_column_mappings'

    id = Column(Integer, primary_key=True, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False,
                         comment="Scoped per customer — same bank may send different formats to different customers")

    source_column = Column(String, nullable=False, comment="Original header from bank file")
    mapped_field = Column(String, nullable=False,
                          comment="Internal field: bank_lg_number, beneficiary_name, amount, currency_code, issue_date, expiry_date")
    mapping_source = Column(String, default="AUTO", comment="AUTO, AI, MANUAL")
    confidence = Column(Numeric(precision=3, scale=2), nullable=True, comment="0.00-1.00")

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    bank = relationship("Bank")
    customer = relationship("Customer")

    __table_args__ = (
        Index('idx_bcm_bank_customer', 'bank_id', 'customer_id'),
    )


# ==============================================================================
# 8. ADMIN DUAL-CONTROL (Maker-Checker for Config Changes)
# ==============================================================================

class AdminChangeRequest(BaseModel):
    """
    Tracks pending admin configuration changes that require approval
    from a second corporate admin before being applied.
    """
    __tablename__ = 'admin_change_requests'

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    requested_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    approved_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    change_type = Column(String, nullable=False,
                         comment="CONFIG_UPDATE, APPROVAL_MATRIX_UPDATE, DEPARTMENT_CREATE, DEPARTMENT_UPDATE, GROUP_CREATE, GROUP_UPDATE, etc.")
    change_payload = Column(JSONB, nullable=False,
                            comment='{"config_key": "...", "old_value": "...", "new_value": "..."}')
    status = Column(String, default="PENDING",
                    comment="PENDING, APPROVED, REJECTED")
    
    rejection_reason = Column(Text, nullable=True)
    applied_at = Column(DateTime, nullable=True, comment="When the change was actually applied")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    customer = relationship("Customer")
    requested_by = relationship("User", foreign_keys=[requested_by_user_id])
    approved_by = relationship("User", foreign_keys=[approved_by_user_id])


# ==============================================================================
# 9. BANK FORM ISSUE REPORTING
# ==============================================================================

class BankFormIssueReport(BaseModel):
    """
    Tracks issues reported by users about bank forms — missing fields,
    incorrect layouts, formatting problems, or outdated templates.
    """
    __tablename__ = 'bank_form_issue_reports'

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    reported_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    form_config_id = Column(Integer, nullable=True, comment="FK to CustomerFormConfiguration if applicable")
    
    issue_type = Column(String, nullable=False,
                        comment="MISSING_FIELD, INCORRECT_FORMAT, OUTDATED_TEMPLATE, LAYOUT_ERROR, OTHER")
    description = Column(Text, nullable=False, comment="Detailed description of the issue")
    field_name = Column(String, nullable=True, comment="Specific field with the issue, if applicable")
    severity = Column(String, default="MEDIUM", comment="LOW, MEDIUM, HIGH, CRITICAL")
    status = Column(String, default="OPEN", comment="OPEN, IN_PROGRESS, RESOLVED, CLOSED, WONT_FIX")
    resolution_notes = Column(Text, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    resolved_at = Column(DateTime, nullable=True)

    customer = relationship("Customer")
    reported_by = relationship("User", foreign_keys=[reported_by_user_id])
    bank = relationship("Bank")


# ==============================================================================
# AI FX RATE CACHE (for Tier 2 FX resolution)
# ==============================================================================

class AiFxRateCache(Base):
    """
    Caches FX rates fetched from AI (Gemini) to avoid repeat API calls.
    Entries expire after 24 hours (enforced by FxService, not DB).
    """
    __tablename__ = 'ai_fx_rate_cache'

    id = Column(Integer, primary_key=True, index=True)
    from_currency_code = Column(String(10), nullable=False, comment="ISO code of source currency (e.g., 'USD')")
    to_currency_code = Column(String(10), nullable=False, comment="ISO code of target currency (e.g., 'EGP')")
    rate = Column(Numeric(precision=15, scale=6), nullable=False, comment="1 from_currency = rate × to_currency")
    cached_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index('idx_ai_fx_cache_pair', 'from_currency_code', 'to_currency_code'),
        Index('idx_ai_fx_cache_time', 'cached_at'),
    )