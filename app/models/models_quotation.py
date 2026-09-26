# app/models_quotation.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey, Text, Index, Date
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.models import BaseModel, Base

class QuotationBankEntity(Base):
    """Junction table mapping a QuotationBank to specific CustomerEntities."""
    __tablename__ = "quotation_bank_entities"
    id = Column(Integer, primary_key=True, index=True)
    quotation_bank_id = Column(Integer, ForeignKey("quotation_banks.id", ondelete="CASCADE"), nullable=False, index=True)
    entity_id = Column(Integer, ForeignKey("customer_entities.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    quotation_bank = relationship("QuotationBank", back_populates="entity_associations")
    entity = relationship("CustomerEntity")

class QuotationBank(BaseModel):
    """Link table allowing a Customer to configure which core Banks they want to receive quotations, and adding custom emails per bank."""
    __tablename__ = "quotation_banks"
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="CASCADE"), nullable=False)
    trade_type = Column(String, default="BOTH", comment="'FX_SPOT', 'TBILL', or 'BOTH'")
    entity_scope = Column(String(30), default="ALL_ENTITIES", nullable=False, comment="'ALL_ENTITIES' or 'SPECIFIC_ENTITIES'")
    emails = Column(Text, nullable=False, comment="Comma-separated emails for this specific customer's counterparty list")
    contacts = Column(JSONB, default=list, nullable=True, comment="Structured contacts list: [{'email': '...', 'name': '...', 'role': 'EXECUTION'|'VIEW_ONLY'}]")

    customer = relationship("Customer")
    bank = relationship("Bank")
    entity_associations = relationship("QuotationBankEntity", back_populates="quotation_bank", cascade="all, delete-orphan")

class QuotationRequest(BaseModel):
    """Core RFQ configuration for FX Spot and T-Bills."""
    __tablename__ = "quotation_rfqs"
    id = Column(String, primary_key=True, comment="UUID string")
    ref_no = Column(String, unique=True, index=True, nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    
    type = Column(String, default="FX_SPOT", comment="'FX_SPOT' or 'TBILL'")
    direction = Column(String, nullable=True, comment="'Buy' or 'Sell'")
    value_date = Column(String, nullable=True)
    amount = Column(Float, nullable=True)
    min_ticket_amount = Column(Float, nullable=True)
    buy_currency = Column(String, nullable=True)
    sell_currency = Column(String, nullable=True)
    
    settlement_date_start = Column(String, nullable=True)
    settlement_date_end = Column(String, nullable=True)
    maturity_date_start = Column(String, nullable=True)
    maturity_date_end = Column(String, nullable=True)
    eval_rate = Column(Float, nullable=True)
    
    window_start = Column(DateTime(timezone=True), nullable=False)
    window_end = Column(DateTime(timezone=True), nullable=False)
    quotation_base = Column(String, nullable=True, comment="'Execution' or 'Indicative'")
    max_tolerance_percent = Column(Float, nullable=True, comment="Max allowed % deviation for Execution rates vs Indicative benchmark")
    allow_alternative_value_date = Column(Boolean, default=False, nullable=False, comment="Whether counterparties are permitted to propose an alternative value date")
    document_path = Column(Text, nullable=True)
    status = Column(String, default="PENDING", comment="'PENDING_APPROVAL', 'NEEDS_REVISION', 'PENDING', 'OPEN', 'EVALUATING', 'COMPLETED', 'REJECTED', 'CANCEL_REQUESTED', 'CANCELLED'")
    token_validity_hours = Column(Integer, default=24, comment="Hours the bank link remains valid after window_end")
    parent_rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="SET NULL"), nullable=True, index=True, comment="Original RFQ if this is a re-tender")
    entity_id = Column(Integer, ForeignKey("customer_entities.id", ondelete="SET NULL"), nullable=True, index=True, comment="Customer entity/subsidiary requesting the RFQ")
    admin_revision_notes = Column(Text, nullable=True, comment="Notes from Corporate Admin when returned for revision")
    admin_reviewed_at = Column(DateTime(timezone=True), nullable=True)
    cancellation_reason = Column(String(255), nullable=True, comment="Internal reason selected by end user for cancellation")
    cancellation_notes = Column(Text, nullable=True, comment="Additional context notes provided by requestor")
    cancellation_requested_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    cancellation_requested_at = Column(DateTime(timezone=True), nullable=True)
    cancelled_at = Column(DateTime(timezone=True), nullable=True)
    internal_notes = Column(Text, nullable=True, comment="Internal notes from requestor (e.g. related payments, invoices)")
    scheduled_release_at = Column(DateTime(timezone=True), nullable=True, comment="Future scheduled time for bank email dispatch")
    scheduled_release_job_id = Column(String(100), nullable=True, comment="APScheduler job ID for scheduled release")
    is_dispatched = Column(Boolean, default=False, nullable=False, comment="Whether quotation invitation emails have been dispatched to banks")
    dispatched_at = Column(DateTime(timezone=True), nullable=True, comment="Timestamp when quotation invitation emails were dispatched")

    customer = relationship("Customer")
    entity = relationship("CustomerEntity")
    creator = relationship("User", foreign_keys=[created_by_user_id])
    cancellation_requestor = relationship("User", foreign_keys=[cancellation_requested_by])
    assignments = relationship("QuotationBankAssignment", back_populates="rfq", cascade="all, delete-orphan")
    legs = relationship("QuotationLeg", back_populates="rfq", cascade="all, delete-orphan", order_by="QuotationLeg.leg_index")
    parent_rfq = relationship("QuotationRequest", remote_side=[id], backref="re_tenders")

class QuotationLeg(BaseModel):
    """Individual currency pair or trade leg within a QuotationRequest session."""
    __tablename__ = "quotation_legs"
    id = Column(String, primary_key=True)
    rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="CASCADE"), nullable=False, index=True)
    leg_index = Column(Integer, default=1, nullable=False)
    
    type = Column(String, default="FX_SPOT", comment="'FX_SPOT' or 'TBILL'")
    direction = Column(String, nullable=True, comment="'Buy' or 'Sell'")
    buy_currency = Column(String, nullable=True)
    sell_currency = Column(String, nullable=True)
    amount = Column(Float, nullable=True)
    min_ticket_amount = Column(Float, nullable=True)
    value_date = Column(String, nullable=True)
    allow_alternative_value_date = Column(Boolean, default=False, nullable=False)
    quotation_base = Column(String, nullable=True, comment="'Execution' or 'Indicative'")
    max_tolerance_percent = Column(Float, nullable=True)
    
    settlement_date_start = Column(String, nullable=True)
    settlement_date_end = Column(String, nullable=True)
    maturity_date_start = Column(String, nullable=True)
    maturity_date_end = Column(String, nullable=True)
    eval_rate = Column(Float, nullable=True)
    
    status = Column(String, default="PENDING", comment="'PENDING', 'OPEN', 'EVALUATING', 'COMPLETED', 'ACCEPTED', 'REJECTED', 'INCONCLUSIVE', 'EXPIRED'")
    winner_bank_id = Column(Integer, nullable=True)
    winner_bank_name = Column(String(255), nullable=True)
    winner_rate = Column(Float, nullable=True)
    saved_vs_avg = Column(Float, nullable=True)
    execution_reference = Column(String(50), nullable=True)
    deal_slip_pdf_path = Column(String(500), nullable=True)
    rejection_reason = Column(Text, nullable=True)
    document_path = Column(Text, nullable=True)
    entity_id = Column(Integer, ForeignKey("customer_entities.id", ondelete="SET NULL"), nullable=True)

    rfq = relationship("QuotationRequest", back_populates="legs")
    offers = relationship("QuotationOffer", back_populates="leg", cascade="all, delete-orphan")
    tbill_offers = relationship("QuotationTBillOffer", back_populates="leg", cascade="all, delete-orphan")
    bank_configs = relationship("QuotationBankLegConfig", back_populates="leg", cascade="all, delete-orphan")

class QuotationBankAssignment(BaseModel):
    """Junction table connecting an RFQ strictly to a QuotationBank (1 token per bank per RFQ session)."""
    __tablename__ = "quotation_bank_assignments"
    id = Column(String, primary_key=True)
    rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="CASCADE"), nullable=False)
    quotation_bank_id = Column(Integer, ForeignKey("quotation_banks.id", ondelete="CASCADE"), nullable=False)
    token = Column(String, unique=True, index=True, nullable=False)
    
    cost_min = Column(Float, default=0.0)
    cost_percent = Column(Float, default=0.0)
    cost_max = Column(Float, default=0.0)
    cost_flat = Column(Float, default=0.0)
    quotation_base = Column(String, nullable=True, comment="'Execution' or 'Indicative' override per bank")
    is_document_visible = Column(Boolean, default=True, comment="Controls document attachment visibility for this bank")
    value_date = Column(Date, nullable=True, comment="Custom value date target for this specific bank; falls back to RFQ master value_date")
    allow_alternative_value_date = Column(Boolean, nullable=True, comment="Per-bank override: True/False, or NULL to inherit from RFQ master")
    
    # Bank Approval Layer (optional, per-assignment)
    approval_status = Column(String, default=None, nullable=True, comment="NULL=no approval needed, 'PENDING', 'APPROVED', 'DECLINED', 'EXPIRED'")
    approved_by_email = Column(String, nullable=True, comment="Email of the approver who approved/declined")
    approved_at = Column(DateTime(timezone=True), nullable=True)
    approval_notes = Column(Text, nullable=True, comment="Optional notes from the approver")

    rfq = relationship("QuotationRequest", back_populates="assignments")
    quotation_bank = relationship("QuotationBank")
    offers = relationship("QuotationOffer", back_populates="assignment", cascade="all, delete-orphan")
    tbill_offers = relationship("QuotationTBillOffer", back_populates="assignment", cascade="all, delete-orphan")
    otps = relationship("QuotationAccessOTP", back_populates="assignment", cascade="all, delete-orphan")
    leg_configs = relationship("QuotationBankLegConfig", back_populates="assignment", cascade="all, delete-orphan")

    def get_config_for_leg(self, leg_id: str):
        """Returns the specific configuration for a leg, or falls back to assignment level."""
        if self.leg_configs:
            for cfg in self.leg_configs:
                if cfg.leg_id == leg_id:
                    return cfg
        return self

class QuotationBankLegConfig(BaseModel):
    """Per-bank configuration for a specific currency pair leg within a quotation session."""
    __tablename__ = "quotation_bank_leg_configs"
    id = Column(String, primary_key=True)
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False, index=True)
    leg_id = Column(String, ForeignKey("quotation_legs.id", ondelete="CASCADE"), nullable=False, index=True)
    
    is_invited = Column(Boolean, default=True, nullable=False)
    cost_min = Column(Float, default=0.0)
    cost_percent = Column(Float, default=0.0)
    cost_max = Column(Float, default=0.0)
    cost_flat = Column(Float, default=0.0)
    quotation_base = Column(String, nullable=True, comment="'Execution' or 'Indicative' override per bank for this leg")
    is_document_visible = Column(Boolean, default=True)
    value_date = Column(Date, nullable=True, comment="Custom value date target for this specific bank on this leg")
    allow_alternative_value_date = Column(Boolean, nullable=True, comment="Per-bank override for this leg")

    assignment = relationship("QuotationBankAssignment", back_populates="leg_configs")
    leg = relationship("QuotationLeg", back_populates="bank_configs")

class QuotationOffer(BaseModel):
    """FX Spot Offers from banks."""
    __tablename__ = "quotation_offers"
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False)
    leg_id = Column(String, ForeignKey("quotation_legs.id", ondelete="CASCADE"), nullable=True, index=True)
    price = Column(Float, nullable=False)
    offered_value_date = Column(Date, nullable=True, comment="Alternative settlement date proposed by counterparty")
    notes = Column(Text, nullable=True, comment="Optional notes or comments from the submitting trader")
    submitted_by_email = Column(String, nullable=True, comment="Email of the authenticated trader who submitted this quote")
    submitted_at = Column(DateTime(timezone=True), server_default=func.now())

    assignment = relationship("QuotationBankAssignment", back_populates="offers")
    leg = relationship("QuotationLeg", back_populates="offers")

class QuotationTBillOffer(BaseModel):
    """T-Bill specific quotation lines."""
    __tablename__ = "quotation_tbill_offers"
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False)
    leg_id = Column(String, ForeignKey("quotation_legs.id", ondelete="CASCADE"), nullable=True, index=True)
    settlement_date = Column(String, nullable=False)
    maturity_date = Column(String, nullable=False)
    discount_rate = Column(Float, nullable=False)
    max_amount = Column(Float, nullable=False)
    notes = Column(Text, nullable=True, comment="Optional notes or comments from the submitting trader")
    submitted_by_email = Column(String, nullable=True, comment="Email of the authenticated trader who submitted this quote")
    submitted_at = Column(DateTime(timezone=True), server_default=func.now())

    assignment = relationship("QuotationBankAssignment", back_populates="tbill_offers")
    leg = relationship("QuotationLeg", back_populates="tbill_offers")

class QuotationAccessOTP(BaseModel):
    """Stores OTPs and magic access tokens for bank desk authentication."""
    __tablename__ = "quotation_access_otps"
    id = Column(Integer, primary_key=True, index=True)
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False, index=True)
    email = Column(String, nullable=False, index=True)
    role = Column(String, default="EXECUTION", comment="'EXECUTION', 'VIEW_ONLY', or 'APPROVER'")
    otp_code = Column(String, nullable=False)
    magic_token = Column(String, unique=True, index=True, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_used = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    assignment = relationship("QuotationBankAssignment", back_populates="otps")

class QuotationAnalytics(BaseModel):
    """Stores definite factual computation facts for closed RFQs."""
    __tablename__ = "quotation_analytics"
    id = Column(Integer, primary_key=True, index=True)
    rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="CASCADE"), nullable=False, unique=True)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False, index=True)
    winner_quotation_bank_id = Column(Integer, ForeignKey("quotation_banks.id", ondelete="SET NULL"), nullable=True)
    
    winner_price = Column(Float, nullable=True)
    total_participated = Column(Integer, default=0)
    avg_price_spread = Column(Float, default=0.0)
    results_json = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    rfq = relationship("QuotationRequest")
    customer = relationship("Customer")
    winner_bank = relationship("QuotationBank")

class QuotationNotification(BaseModel):
    """Per-user notifications for RFQ status changes and live updates."""
    __tablename__ = "quotation_notifications"
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    type = Column(String, nullable=False, comment="'RFQ_PENDING_APPROVAL', 'RFQ_APPROVED', 'RFQ_REJECTED', 'NEW_OFFER'")
    title = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    link = Column(String, nullable=True)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User")

class QuotationAnonymousBenchmark(Base):
    """
    Zero-Knowledge Anonymous Aggregation Mart.
    Stores stripped, normalized macro metrics across all tenders with 0% confidentiality risk.
    Contains NO customer IDs, NO user IDs, NO RFQ refs, and NO raw trade amounts.
    """
    __tablename__ = "quotation_anonymous_benchmarks"
    
    id = Column(Integer, primary_key=True, index=True)
    tenant_cohort_hash = Column(String, nullable=False, index=True, comment="One-way anonymous tenant hash for k-anonymity cardinality verification")
    currency_pair = Column(String(10), nullable=False, index=True, comment="e.g. 'USD/EGP', 'EUR/EGP'")
    trade_type = Column(String(20), nullable=False, default="FX_SPOT", comment="'FX_SPOT' or 'TBILL'")
    deal_tier = Column(String(10), nullable=False, comment="'TIER_1' (<250k), 'TIER_2' (250k-1M), 'TIER_3' (>1M)")
    cbe_benchmark_rate = Column(Float, nullable=True, comment="Reference rate from CBE at window start")
    winning_spread_bps = Column(Float, nullable=True, comment="Winning execution rate spread in basis points over CBE benchmark")
    avg_spread_bps = Column(Float, nullable=True, comment="Market average spread in basis points over CBE benchmark")
    num_participating_banks = Column(Integer, default=0)
    num_quotes_submitted = Column(Integer, default=0)
    response_duration_seconds = Column(Integer, nullable=True, comment="Seconds from tender opening to first winning quote")
    window_time_slot = Column(String(20), nullable=True, comment="e.g. 'TUE_10_12', 'WED_14_16' for liquidity timing analysis")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

class BankLiveRankingConfig(BaseModel):
    """
    Configuration table allowing System Owner to enable or disable live ranking per Bank,
    filtered by Service (FX_SPOT, TBILL, or BOTH), Customer Scope (ALL or SPECIFIC),
    and Entity Scope (ALL or SPECIFIC).
    """
    __tablename__ = "bank_live_ranking_configs"

    id = Column(Integer, primary_key=True, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="CASCADE"), nullable=False, index=True)
    trade_type = Column(String(20), default="BOTH", nullable=False, comment="'FX_SPOT', 'TBILL', or 'BOTH'")
    
    # Customer scope
    scope_type = Column(String(30), default="ALL_CUSTOMERS", nullable=False, comment="'ALL_CUSTOMERS' or 'SPECIFIC_CUSTOMER'")
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=True, index=True)
    
    # Entity scope (applicable when scope_type == 'SPECIFIC_CUSTOMER')
    entity_scope_type = Column(String(30), default="ALL_ENTITIES", nullable=False, comment="'ALL_ENTITIES' or 'SPECIFIC_ENTITY'")
    entity_id = Column(Integer, ForeignKey("customer_entities.id", ondelete="CASCADE"), nullable=True, index=True)
    
    is_enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    bank = relationship("Bank")
    customer = relationship("Customer")
    entity = relationship("CustomerEntity")
