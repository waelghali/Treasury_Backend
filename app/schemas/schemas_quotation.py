# app/schemas_quotation.py
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List, Any
from datetime import datetime

# --- Quotation Bank Schemas ---
class QuotationBankBase(BaseModel):
    bank_id: int
    trade_type: str = "BOTH"
    emails: str

class QuotationBankCreate(QuotationBankBase):
    pass

class BankSimpleOut(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True

class QuotationBankOut(QuotationBankBase):
    id: int
    customer_id: int
    created_at: datetime
    bank: Optional[BankSimpleOut] = None
    
    class Config:
        from_attributes = True

# --- Quotation Request (RFQ) Schemas ---
class BankSelection(BaseModel):
    id: int
    costMin: Optional[float] = 0.0
    costPercent: Optional[float] = 0.0
    costMax: Optional[float] = 0.0
    costFlat: Optional[float] = 0.0

class QuotationRequestCreate(BaseModel):
    type: str = "FX_SPOT"
    direction: Optional[str] = None
    valueDate: Optional[str] = None
    amount: Optional[float] = None
    minTicketAmount: Optional[float] = None
    buyCurrency: Optional[str] = None
    sellCurrency: Optional[str] = None
    settlementDateStart: Optional[str] = None
    settlementDateEnd: Optional[str] = None
    maturityDateStart: Optional[str] = None
    maturityDateEnd: Optional[str] = None
    evalRate: Optional[float] = None
    windowStart: datetime
    windowEnd: datetime
    quotationBase: Optional[str] = None
    selectedBanks: str # JSON string matching Node module format, or we can parse it in FastAPI
    token_validity_hours: Optional[int] = 24

class QuotationRequestOut(BaseModel):
    id: str
    ref_no: str
    type: str
    direction: Optional[str]
    value_date: Optional[str]
    amount: Optional[float]
    min_ticket_amount: Optional[float]
    buy_currency: Optional[str]
    sell_currency: Optional[str]
    window_start: datetime
    window_end: datetime
    status: str
    token_validity_hours: Optional[int]
    created_at: datetime

    class Config:
        from_attributes = True

# --- Bank Offers Schemas (Public) ---
class FXSpotOfferCreate(BaseModel):
    token: str
    price: float

class TBillLineItem(BaseModel):
    settlementDate: str
    maturityDate: str
    discountRate: float
    maxAmount: float

class TBillOfferCreate(BaseModel):
    token: str
    lines: List[TBillLineItem]

# --- Results Schemas ---
class QuotationResultItem(BaseModel):
    bank_id: int
    bank_name: str
    bank_emails: str
    price: Optional[float] = None
    finalPrice: Optional[float] = None
    submitted_at: Optional[datetime] = None
    token: Optional[str] = None
    offers: Optional[List[dict]] = None # For T-Bills

class QuotationResultsOut(BaseModel):
    rfq: QuotationRequestOut
    results: List[QuotationResultItem]
