from typing import List, Optional, Dict, Any
from datetime import date, datetime
from pydantic import BaseModel

# Import base schemas
from app.schemas.all_schemas import CurrencyOut, BankOut, LGRecordOut

# --- 1. SUB LIMITS ---
class IssuanceFacilitySubLimitBase(BaseModel):
    limit_name: str
    limit_amount: float
    lg_type_id: int
    default_commission_rate: Optional[float] = 0.0
    default_cash_margin_pct: Optional[float] = 0.0
    default_min_commission: Optional[float] = 0.0

class IssuanceFacilitySubLimitCreate(IssuanceFacilitySubLimitBase):
    pass

class IssuanceFacilitySubLimitOut(IssuanceFacilitySubLimitBase):
    id: int
    facility_id: int

    class Config:
        from_attributes = True

# --- 2. FACILITIES ---
class IssuanceFacilityBase(BaseModel):
    bank_id: int
    currency_id: int
    total_limit_amount: float
    reference_number: str
    start_date: Optional[date] = None
    expiry_date: Optional[date] = None
    review_date: Optional[date] = None
    is_active: bool = True

class IssuanceFacilityCreate(IssuanceFacilityBase):
    sub_limits: List[IssuanceFacilitySubLimitCreate] = []

class IssuanceFacilityUpdate(BaseModel):
    total_limit_amount: Optional[float] = None
    is_active: Optional[bool] = None

class IssuanceFacilityOut(IssuanceFacilityBase):
    id: int
    customer_id: int
    created_at: datetime
    # Nested relationships
    bank: Optional[BankOut] = None
    currency: Optional[CurrencyOut] = None
    sub_limits: List[IssuanceFacilitySubLimitOut] = []

    class Config:
        from_attributes = True

# --- 3. SUITABLE FACILITY ---
class SuitableFacilityOut(BaseModel):
    facility_id: int
    facility_bank: str
    sub_limit_id: int
    sub_limit_name: str
    limit_available: float
    price_commission: float

# --- 4. ISSUANCE REQUESTS ---
class BusinessDetails(BaseModel):
    project_name: Optional[str] = None
    contract_ref: Optional[str] = None
    department: Optional[str] = None

class IssuanceRequestBase(BaseModel):
    # FIX: Made Optional to prevent validation error when DB has null
    requestor_name: Optional[str] = None 
    transaction_type: str = "NEW_ISSUANCE" 
    lg_record_id: Optional[int] = None
    amount: float
    currency_id: int
    beneficiary_name: str
    requested_issue_date: Optional[date] = None
    requested_expiry_date: Optional[date] = None
    business_details: Optional[BusinessDetails] = None

class IssuanceRequestCreate(IssuanceRequestBase):
    pass

class IssuanceRequestUpdate(BaseModel):
    status: Optional[str] = None
    rejection_reason: Optional[str] = None


class IssuanceRequestContentUpdate(BaseModel):
    amount: Optional[float] = None
    beneficiary_name: Optional[str] = None
    currency_id: Optional[int] = None
    requested_issue_date: Optional[date] = None
    requested_expiry_date: Optional[date] = None
    business_details: Optional[dict] = None

class IssuanceRequestOut(IssuanceRequestBase):
    id: int
    customer_id: int
    requestor_user_id: Optional[int]
    status: str
    created_at: datetime
    
    # Relationships
    currency: Optional[CurrencyOut] = None
    lg_record: Optional[LGRecordOut] = None

    class Config:
        from_attributes = True

# --- 5. EXECUTED RECORD WRAPPER ---
class IssuedLGRecordOut(BaseModel):
    message: str
    lg_record_id: Optional[int] = None