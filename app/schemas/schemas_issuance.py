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
    
    # New Boundaries
    sla_agreement_days: Optional[int] = None
    allow_cross_border: bool = False
    allow_third_party_issuance: bool = False
    required_cash_margin_days: int = 0

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

class BankIssuanceOptionOut(BaseModel):
    id: int
    display_name: str
    strategy_code: str # Frontend might need to know if it's "INSTANT" or "MANUAL" to show icons
    
    class Config:
        from_attributes = True

# --- 3. SMART DECISION SUPPORT ---
class SuitableFacilityOut(BaseModel):
    facility_id: int
    facility_bank: str
    sub_limit_id: int
    sub_limit_name: str
    limit_available: float
    available_methods: List[BankIssuanceOptionOut] = []
    # NEW INTELLIGENCE FIELDS
    price_commission_rate: float
    price_cash_margin_pct: float
    estimated_commission_cost: float
    required_cash_margin_amount: float
    
    # Recommendation Tags (e.g. "BEST_PRICE", "NO_MARGIN")
    recommendation_tags: List[str] = []

# --- 4. ISSUANCE REQUESTS ---
class BusinessDetails(BaseModel):
    project_name: Optional[str] = None
    contract_ref: Optional[str] = None
    department: Optional[str] = None

class IssuanceRequestBase(BaseModel):
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
    current_approval_step: Optional[int] = 0
    pending_approver_role: Optional[str] = None
    approval_chain_audit: Optional[List[Dict[str, Any]]] = None
    created_at: datetime
    currency: Optional[CurrencyOut] = None
    lg_record: Optional[LGRecordOut] = None

    class Config:
        from_attributes = True

class IssuedLGRecordOut(BaseModel):
    message: str
    lg_record_id: Optional[int] = None

# --- 5. RECONCILIATION SCHEMAS ---
class BankPositionRow(BaseModel):
    """Represents one row from the Bank's Excel Sheet"""
    ref_number: str
    amount: float
    currency: str
    status: str = "ACTIVE"

class ReconciliationRequest(BaseModel):
    bank_id: int
    as_of_date: date
    rows: List[BankPositionRow]

class ReconciliationResult(BaseModel):
    total_bank_records: int
    matched_count: int
    mismatched_amount_count: int
    missing_in_system_count: int
    
    # Detailed discrepancies
    discrepancies: List[Dict[str, Any]]

# --- 6. WORKFLOW POLICY SCHEMAS ---
class IssuanceWorkflowPolicyBase(BaseModel):
    min_amount: float = 0.0
    max_amount: Optional[float] = None
    step_sequence: int = 1
    approver_role_name: str
    specific_approver_user_id: Optional[int] = None

class IssuanceWorkflowPolicyCreate(IssuanceWorkflowPolicyBase):
    pass

class IssuanceWorkflowPolicyOut(IssuanceWorkflowPolicyBase):
    id: int
    customer_id: int

    class Config:
        from_attributes = True
        
