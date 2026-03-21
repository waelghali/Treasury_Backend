# app/models_reconciliation.py

from sqlalchemy import Column, Integer, String, Date, Numeric, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base
from app.models import BaseModel # Inherit from your root models.py

class BankPositionBatch(BaseModel):
    """
    Represents one 'Upload Event' (e.g. HSBC Position Report - Jan 2025)
    """
    __tablename__ = 'bank_position_batches'

    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    as_of_date = Column(Date, nullable=False)
    uploaded_by_user_id = Column(Integer, nullable=True) # Admin ID
    
    total_records = Column(Integer, default=0)
    matched_records = Column(Integer, default=0)
    
    # Audit
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    bank = relationship("Bank")
    rows = relationship("BankPositionRow", back_populates="batch")


class BankPositionRow(BaseModel):
    """
    Raw data rows from the Excel file
    """
    __tablename__ = 'bank_position_rows'

    batch_id = Column(Integer, ForeignKey("bank_position_batches.id"), nullable=False)
    
    ref_number = Column(String, nullable=False)
    amount = Column(Numeric(precision=20, scale=2), nullable=False)
    currency_code = Column(String, nullable=True)
    status_in_bank = Column(String, nullable=True) # e.g. "Alive", "Expired"
    
    # Reconciliation Result for this specific row
    recon_status = Column(String, default="PENDING") # MATCHED, MISMATCH, MISSING_IN_SYSTEM
    recon_note = Column(String, nullable=True)

    batch = relationship("BankPositionBatch", back_populates="rows")