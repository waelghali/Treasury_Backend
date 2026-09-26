# app/schemas_quotation.py
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List, Any, Union
from datetime import datetime, date

# --- Quotation Bank Schemas ---
class QuotationContactItem(BaseModel):
    email: str
    name: Optional[str] = None
    role: str = "EXECUTION" # "EXECUTION", "VIEW_ONLY", or "APPROVER"

class BankApprovalActionCreate(BaseModel):
    action: str  # "APPROVE" or "DECLINE"
    session_token: str
    notes: Optional[str] = None

class QuotationBankBase(BaseModel):
    bank_id: int
    trade_type: str = "BOTH"
    entity_scope: Optional[str] = "ALL_ENTITIES"
    entity_ids: Optional[List[int]] = []
    emails: Optional[str] = None
    contacts: Optional[List[QuotationContactItem]] = None

class QuotationBankCreate(QuotationBankBase):
    pass

class BankSimpleOut(BaseModel):
    id: int
    name: str
    email_domain: Optional[str] = None

    class Config:
        from_attributes = True

class QuotationBankOut(QuotationBankBase):
    id: int
    customer_id: int
    created_at: datetime
    bank: Optional[BankSimpleOut] = None
    contacts: Optional[List[dict]] = None
    entity_scope: Optional[str] = "ALL_ENTITIES"
    entity_ids: Optional[List[int]] = []
    is_cross_entity: Optional[bool] = False
    
    class Config:
        from_attributes = True

# --- Quotation Request (RFQ) Schemas ---
class BankSelection(BaseModel):
    id: int
    costMin: Optional[float] = 0.0
    costPercent: Optional[float] = 0.0
    costMax: Optional[float] = 0.0
    costFlat: Optional[float] = 0.0
    quotationBase: Optional[str] = None
    isDocumentVisible: Optional[bool] = True
    valueDate: Optional[str] = None
    allowAlternativeValueDate: Optional[bool] = None

class QuotationLegCreate(BaseModel):
    direction: Optional[str] = "Buy"
    buyCurrency: Optional[str] = "USD"
    sellCurrency: Optional[str] = "EGP"
    amount: Optional[float] = None
    minTicketAmount: Optional[float] = None
    valueDate: Optional[Union[str, date]] = None
    allowAlternativeValueDate: Optional[bool] = False
    quotationBase: Optional[str] = None
    maxTolerancePercent: Optional[float] = None
    selectedBanks: Optional[Union[str, List[dict]]] = None

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
    maxTolerancePercent: Optional[float] = None
    allowAlternativeValueDate: Optional[bool] = False
    documentPath: Optional[str] = None
    selectedBanks: Optional[str] = "[]" # JSON string matching Node module format, or we can parse it in FastAPI
    token_validity_hours: Optional[int] = 24
    entity_id: Optional[int] = None
    parent_rfq_id: Optional[str] = None
    internal_notes: Optional[str] = None
    internalNotes: Optional[str] = None
    legal_disclaimer_accepted: Optional[bool] = False
    legalDisclaimerAccepted: Optional[bool] = False
    pairs: Optional[List[QuotationLegCreate]] = None
    legs: Optional[List[QuotationLegCreate]] = None

class QuotationBankLegConfigOut(BaseModel):
    id: str
    assignment_id: str
    leg_id: str
    is_invited: bool = True
    cost_min: float = 0.0
    cost_percent: float = 0.0
    cost_max: float = 0.0
    cost_flat: float = 0.0
    quotation_base: Optional[str] = None
    is_document_visible: bool = True
    value_date: Optional[Union[str, date]] = None
    allow_alternative_value_date: Optional[bool] = None

    class Config:
        from_attributes = True

class QuotationLegOut(BaseModel):
    id: str
    rfq_id: str
    leg_index: int = 1
    type: str = "FX_SPOT"
    direction: Optional[str] = None
    buy_currency: Optional[str] = None
    sell_currency: Optional[str] = None
    amount: Optional[float] = None
    min_ticket_amount: Optional[float] = None
    value_date: Optional[Union[str, date]] = None
    allow_alternative_value_date: Optional[bool] = False
    quotation_base: Optional[str] = None
    max_tolerance_percent: Optional[float] = None
    status: str = "PENDING"
    winner_bank_id: Optional[int] = None
    winner_bank_name: Optional[str] = None
    winner_rate: Optional[float] = None
    saved_vs_avg: Optional[float] = None
    execution_reference: Optional[str] = None
    deal_slip_pdf_path: Optional[str] = None
    rejection_reason: Optional[str] = None
    document_path: Optional[str] = None
    bank_configs: Optional[List[QuotationBankLegConfigOut]] = []

    class Config:
        from_attributes = True

class QuotationRequestOut(BaseModel):
    id: str
    ref_no: str
    type: str
    direction: Optional[str] = None
    value_date: Optional[Union[str, date]] = None
    amount: Optional[float] = None
    min_ticket_amount: Optional[float] = None
    buy_currency: Optional[str] = None
    sell_currency: Optional[str] = None
    settlement_date_start: Optional[str] = None
    settlement_date_end: Optional[str] = None
    maturity_date_start: Optional[str] = None
    maturity_date_end: Optional[str] = None
    eval_rate: Optional[float] = None
    window_start: datetime
    window_end: datetime
    status: str
    token_validity_hours: Optional[int] = 24
    entity_id: Optional[int] = None
    entity_name: Optional[str] = None
    entity_code: Optional[str] = None
    created_at: datetime
    creator_name: Optional[str] = None
    quotation_base: Optional[str] = None
    max_tolerance_percent: Optional[float] = None
    document_path: Optional[str] = None
    internal_notes: Optional[str] = None
    parent_rfq_id: Optional[str] = None
    parent_rfq_ref: Optional[str] = None
    admin_revision_notes: Optional[str] = None
    admin_reviewed_at: Optional[datetime] = None
    winner_bank_name: Optional[str] = None
    winner_rate: Optional[float] = None
    saved_vs_avg: Optional[float] = None
    allow_alternative_value_date: Optional[bool] = False
    re_tender_count: Optional[int] = 0
    cancellation_reason: Optional[str] = None
    cancellation_notes: Optional[str] = None
    cancellation_requested_by: Optional[int] = None
    cancellation_requested_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    scheduled_release_at: Optional[datetime] = None
    is_dispatched: Optional[bool] = False
    dispatched_at: Optional[datetime] = None
    acceptance_timeout_seconds: Optional[int] = None
    acceptance_timeout_action: Optional[str] = None
    acceptance_deadline: Optional[datetime] = None
    acceptance_status: Optional[str] = None
    acceptance_resolved_at: Optional[datetime] = None
    assigned_banks: Optional[List[dict]] = None
    legs: Optional[List[QuotationLegOut]] = []

    class Config:
        from_attributes = True

class QuotationApprovalRequest(BaseModel):
    legal_disclaimer_accepted: Optional[bool] = True
    scheduled_release_at: Optional[datetime] = None

class QuotationRescheduleRequest(BaseModel):
    scheduled_release_at: Optional[datetime] = None
    release_now: Optional[bool] = False

class QuotationCancellationRequest(BaseModel):
    reason: str
    notes: Optional[str] = None

class ReTenderRequest(BaseModel):
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    amount: Optional[float] = None
    selected_bank_ids: Optional[List[int]] = None
    token_validity_hours: Optional[int] = 24
    entity_id: Optional[int] = None

class QuotationResubmitRequest(BaseModel):
    type: Optional[str] = None
    direction: Optional[str] = None
    value_date: Optional[str] = None
    amount: Optional[float] = None
    min_ticket_amount: Optional[float] = None
    buy_currency: Optional[str] = None
    sell_currency: Optional[str] = None
    settlement_date_start: Optional[str] = None
    settlement_date_end: Optional[str] = None
    maturity_date_start: Optional[str] = None
    maturity_date_end: Optional[str] = None
    eval_rate: Optional[float] = None
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    quotation_base: Optional[str] = None
    max_tolerance_percent: Optional[float] = None
    allow_alternative_value_date: Optional[bool] = None
    document_path: Optional[str] = None
    selected_banks: Optional[str] = None
    selected_bank_ids: Optional[List[int]] = None
    token_validity_hours: Optional[int] = 24
    entity_id: Optional[int] = None
    user_notes: Optional[str] = None
    internal_notes: Optional[str] = None
    internalNotes: Optional[str] = None
    legal_disclaimer_accepted: Optional[bool] = False
    legalDisclaimerAccepted: Optional[bool] = False
    pairs: Optional[List[QuotationLegCreate]] = None
    legs: Optional[List[QuotationLegCreate]] = None

# --- Bank Offers & OTP Schemas (Public) ---
class OTPRequestCreate(BaseModel):
    token: str
    email: str

class OTPVerifyCreate(BaseModel):
    token: str
    email: Optional[str] = None
    otp_code: Optional[str] = None
    magic_token: Optional[str] = None

class DeskSessionActionRequest(BaseModel):
    email: str
    name: Optional[str] = None
    role: Optional[str] = "EXECUTION"
    session_token: Optional[str] = None

class FXSpotOfferCreate(BaseModel):
    token: str
    price: float
    offered_value_date: Optional[Union[str, date]] = None
    notes: Optional[str] = None
    session_token: Optional[str] = None
    email: Optional[str] = None
    leg_id: Optional[str] = None

class FXSpotOfferItem(BaseModel):
    leg_id: str
    price: float
    offered_value_date: Optional[Union[str, date]] = None
    notes: Optional[str] = None

class FXSpotMultiOfferCreate(BaseModel):
    token: str
    quotes: List[FXSpotOfferItem]
    session_token: Optional[str] = None
    email: Optional[str] = None

class TBillLineItem(BaseModel):
    settlementDate: str
    maturityDate: str
    discountRate: float
    maxAmount: float
    notes: Optional[str] = None

class TBillOfferCreate(BaseModel):
    token: str
    lines: List[TBillLineItem]
    notes: Optional[str] = None
    session_token: Optional[str] = None
    email: Optional[str] = None

# --- Results Schemas ---
class QuotationResultItem(BaseModel):
    bank_id: int
    bank_name: str
    bank_emails: str
    price: Optional[float] = None
    finalPrice: Optional[float] = None
    notes: Optional[str] = None
    submitted_at: Optional[datetime] = None
    submitted_by_email: Optional[str] = None
    token: Optional[str] = None
    offers: Optional[List[dict]] = None # For T-Bills
    quotation_base: Optional[str] = None
    is_document_visible: Optional[bool] = True
    contacts: Optional[List[dict]] = None
    approval_status: Optional[str] = None
    approved_by_email: Optional[str] = None
    approved_at: Optional[datetime] = None
    approval_notes: Optional[str] = None
    cost_min: Optional[float] = 0.0
    cost_percent: Optional[float] = 0.0
    cost_max: Optional[float] = 0.0
    cost_flat: Optional[float] = 0.0
    assigned_value_date: Optional[Union[str, date]] = None
    offered_value_date: Optional[Union[str, date]] = None
    allow_alternative_value_date: Optional[bool] = False
    is_alternative_value_date: Optional[bool] = False
    time_value_adjustment: Optional[float] = 0.0
    normalized_price: Optional[float] = None

class QuotationResultsOut(BaseModel):
    rfq: QuotationRequestOut
    results: List[QuotationResultItem]
    legs: Optional[List[dict]] = None
    winner_bank_id: Optional[int] = None
    is_inconclusive: bool = False
    inconclusive_reason: Optional[str] = None
    best_indicative_rate: Optional[float] = None
    best_execution_rate: Optional[float] = None
    deviation_percent: Optional[float] = None
    has_execution_banks: bool = True

# --- Bank Live Ranking Schemas ---
class BankLiveRankingConfigCreate(BaseModel):
    bank_id: int
    trade_type: str = "BOTH"
    scope_type: str = "ALL_CUSTOMERS"
    customer_id: Optional[int] = None
    entity_scope_type: str = "ALL_ENTITIES"
    entity_id: Optional[int] = None
    is_enabled: bool = True

class BankLiveRankingConfigUpdate(BaseModel):
    trade_type: Optional[str] = None
    scope_type: Optional[str] = None
    customer_id: Optional[int] = None
    entity_scope_type: Optional[str] = None
    entity_id: Optional[int] = None
    is_enabled: Optional[bool] = None

class BankLiveRankingConfigOut(BaseModel):
    id: int
    bank_id: int
    bank_name: Optional[str] = None
    trade_type: str
    scope_type: str
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    entity_scope_type: str
    entity_id: Optional[int] = None
    entity_name: Optional[str] = None
    is_enabled: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
