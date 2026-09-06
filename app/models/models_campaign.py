# app/models/models_campaign.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Numeric, Date, Text
from sqlalchemy.orm import relationship
from app.models.models import BaseModel

class Campaign(BaseModel):
    """
    Promotional partner campaign (e.g. Emirates NBD Exclusive Offer or Cross-Bank Adoption Promotion).
    """
    __tablename__ = "campaigns"

    name = Column(String(255), nullable=False, comment="Campaign name (e.g. Emirates NBD Launch Adoption Offer)")
    description = Column(Text, nullable=True, comment="Detailed description of the offer terms")
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=True, comment="Target partner bank. Null indicates offer applies across ALL banks.")
    cashback_percentage = Column(Numeric(6, 4), nullable=False, default=0.0010, comment="Cashback rate, e.g. 0.0010 for 0.10%")
    max_cashback_per_lg = Column(Numeric(14, 2), nullable=False, default=3000.00, comment="Maximum cashback payout cap per LG in EGP")
    max_lgs_per_customer = Column(Integer, nullable=False, default=10, comment="Maximum number of eligible LGs per corporate customer")
    start_date = Column(Date, nullable=False, comment="Campaign active start date")
    end_date = Column(Date, nullable=False, comment="Campaign active end date")
    is_active = Column(Boolean, default=True, nullable=False, comment="Whether campaign is currently active")

    # Relationships
    bank = relationship("Bank", foreign_keys=[bank_id])
    claims = relationship("CashbackClaim", back_populates="campaign", cascade="all, delete-orphan")


class CashbackClaim(BaseModel):
    """
    Individual cashback claim recorded for an eligible LG under a Campaign.
    Lifecycle:
      CALCULATED: Computed upon LG creation/issuance and document scan attachment.
      RECONCILED: Verified automatically when LG matches during bank statement reconciliation.
      VERIFIED: Manually verified by System Owner audit.
      PAID: System Owner confirms payout disbursement.
      REJECTED: Flagged/rejected by System Owner.
    """
    __tablename__ = "cashback_claims"

    campaign_id = Column(Integer, ForeignKey("campaigns.id"), nullable=False, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)
    issued_lg_record_id = Column(Integer, ForeignKey("issued_lg_records.id"), nullable=True, index=True)
    lg_record_id = Column(Integer, ForeignKey("lg_records.id"), nullable=True, index=True)
    lg_amount = Column(Numeric(28, 2), nullable=False, comment="Original LG amount at time of calculation")
    cashback_amount = Column(Numeric(14, 2), nullable=False, comment="Calculated cashback amount capped at max_cashback_per_lg")
    currency_symbol = Column(String(10), default="EGP", nullable=False)
    status = Column(String(30), default="CALCULATED", nullable=False, index=True, comment="CALCULATED, RECONCILED, VERIFIED, PAID, REJECTED")
    verified_at = Column(DateTime(timezone=True), nullable=True, comment="Timestamp when reconciliation or audit verified the LG")
    verified_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    verification_source = Column(String(30), nullable=True, comment="AUTO_RECONCILIATION, MANUAL_AUDIT, SCAN_VERIFIED")
    paid_at = Column(DateTime(timezone=True), nullable=True, comment="Timestamp when payout was settled")
    notes = Column(Text, nullable=True, comment="Audit or settlement notes")

    # Relationships
    campaign = relationship("Campaign", back_populates="claims")
    customer = relationship("Customer")
    issued_lg_record = relationship("IssuedLGRecord")
    lg_record = relationship("LGRecord")
    verified_by_user = relationship("User", foreign_keys=[verified_by_user_id])
