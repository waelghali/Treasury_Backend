# app/models_quotation.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey, Text, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.models import BaseModel

class QuotationBank(BaseModel):
    """Link table allowing a Customer to configure which core Banks they want to receive quotations, and adding custom emails per bank."""
    __tablename__ = "quotation_banks"
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="CASCADE"), nullable=False)
    trade_type = Column(String, default="BOTH", comment="'FX_SPOT', 'TBILL', or 'BOTH'")
    emails = Column(Text, nullable=False, comment="Comma-separated emails for this specific customer's counterparty list")

    customer = relationship("Customer")
    bank = relationship("Bank")

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
    document_path = Column(Text, nullable=True)
    status = Column(String, default="PENDING", comment="'PENDING_APPROVAL', 'PENDING', 'OPEN', 'EVALUATING', 'COMPLETED', 'REJECTED'")
    token_validity_hours = Column(Integer, default=24, comment="Hours the bank link remains valid after window_end")

    customer = relationship("Customer")
    creator = relationship("User")
    assignments = relationship("QuotationBankAssignment", back_populates="rfq", cascade="all, delete-orphan")

class QuotationBankAssignment(BaseModel):
    """Junction table connecting an RFQ strictly to a QuotationBank."""
    __tablename__ = "quotation_bank_assignments"
    id = Column(String, primary_key=True)
    rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="CASCADE"), nullable=False)
    quotation_bank_id = Column(Integer, ForeignKey("quotation_banks.id", ondelete="CASCADE"), nullable=False)
    token = Column(String, unique=True, index=True, nullable=False)
    
    cost_min = Column(Float, default=0.0)
    cost_percent = Column(Float, default=0.0)
    cost_max = Column(Float, default=0.0)
    cost_flat = Column(Float, default=0.0)

    rfq = relationship("QuotationRequest", back_populates="assignments")
    quotation_bank = relationship("QuotationBank")
    offers = relationship("QuotationOffer", back_populates="assignment", cascade="all, delete-orphan")
    tbill_offers = relationship("QuotationTBillOffer", back_populates="assignment", cascade="all, delete-orphan")

class QuotationOffer(BaseModel):
    """FX Spot Offers from banks."""
    __tablename__ = "quotation_offers"
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False)
    price = Column(Float, nullable=False)
    submitted_at = Column(DateTime(timezone=True), server_default=func.now())

    assignment = relationship("QuotationBankAssignment", back_populates="offers")

class QuotationTBillOffer(BaseModel):
    """T-Bill specific quotation lines."""
    __tablename__ = "quotation_tbill_offers"
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False)
    settlement_date = Column(String, nullable=False)
    maturity_date = Column(String, nullable=False)
    discount_rate = Column(Float, nullable=False)
    max_amount = Column(Float, nullable=False)
    submitted_at = Column(DateTime(timezone=True), server_default=func.now())

    assignment = relationship("QuotationBankAssignment", back_populates="tbill_offers")

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
