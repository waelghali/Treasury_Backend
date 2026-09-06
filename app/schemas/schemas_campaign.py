# app/schemas/schemas_campaign.py
from datetime import date, datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from decimal import Decimal

# --- Campaign Schemas ---

class CampaignBase(BaseModel):
    name: str = Field(..., max_length=255)
    description: Optional[str] = None
    bank_id: Optional[int] = None
    cashback_percentage: Decimal = Field(default=Decimal("0.0010"))
    max_cashback_per_lg: Decimal = Field(default=Decimal("3000.00"))
    max_lgs_per_customer: int = Field(default=10, ge=1)
    start_date: date
    end_date: date
    is_active: bool = True

class CampaignCreate(CampaignBase):
    pass

class CampaignUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    bank_id: Optional[int] = None
    cashback_percentage: Optional[Decimal] = None
    max_cashback_per_lg: Optional[Decimal] = None
    max_lgs_per_customer: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    is_active: Optional[bool] = None

class BankBrief(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True

class CampaignOut(CampaignBase):
    id: int
    created_at: Optional[datetime] = None
    bank: Optional[BankBrief] = None
    total_claims_count: Optional[int] = 0
    total_cashback_accrued: Optional[Decimal] = Decimal("0.00")

    class Config:
        from_attributes = True


# --- Cashback Claim Schemas ---

class CashbackClaimOut(BaseModel):
    id: int
    campaign_id: int
    campaign_name: Optional[str] = None
    customer_id: int
    customer_name: Optional[str] = None
    issued_lg_record_id: Optional[int] = None
    lg_record_id: Optional[int] = None
    lg_number: Optional[str] = None
    bank_name: Optional[str] = None
    lg_amount: Decimal
    cashback_amount: Decimal
    currency_symbol: str
    status: str
    created_at: Optional[datetime] = None
    verified_at: Optional[datetime] = None
    verified_by_user_id: Optional[int] = None
    verified_by_name: Optional[str] = None
    verification_source: Optional[str] = None
    paid_at: Optional[datetime] = None
    notes: Optional[str] = None

    class Config:
        from_attributes = True

class CashbackClaimStatusUpdate(BaseModel):
    status: str = Field(..., description="PAID, REJECTED, RECONCILED, or VERIFIED")
    notes: Optional[str] = None

class ManualVerifyRequest(BaseModel):
    notes: Optional[str] = Field(None, description="Audit note describing the manual verification rationale")


# --- Analytics & Summary Schemas ---

class CampaignPerformanceStats(BaseModel):
    total_active_campaigns: int
    total_claims: int
    total_accrued_cashback: Decimal
    total_paid_cashback: Decimal

class CustomerCashbackSummary(BaseModel):
    active_campaign: Optional[CampaignOut] = None
    claims_used: int
    claims_limit: int
    total_earned: Decimal
    total_paid: Decimal
    claims: List[CashbackClaimOut]

class ActiveBankPromotionOut(BaseModel):
    bank_id: Optional[int] = None
    campaign_name: str
    cashback_rate_pct: float
    max_per_lg: float
    max_lgs_per_customer: int
    claims_used: int = 0
