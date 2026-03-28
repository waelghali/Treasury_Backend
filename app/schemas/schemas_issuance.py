# app/schemas/schemas_issuance.py

from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Optional, Dict, List, Any
from datetime import date, datetime
from enum import Enum
from decimal import Decimal

from app.schemas.all_schemas import CurrencyOut, BankOut, CustomerEntityOut, LgTypeOut

# ==============================================================================
# 1. ADMIN CONFIGURATION (STRICT GOVERNANCE)
# ==============================================================================

class CustomFieldType(str, Enum):
    TEXT = "TEXT"
    NUMBER = "NUMBER"
    DATE = "DATE"
    LIST = "LIST"

class StandardFieldConfig(BaseModel):
    is_visible: bool = True
    is_mandatory: bool = False
    model_config = ConfigDict(extra='forbid') # STRICT: Rejects injected keys

class CustomFieldConfig(BaseModel):
    label: str = Field(..., max_length=100)
    type: CustomFieldType
    is_visible: bool = True
    is_mandatory: bool = False
    options: Optional[List[str]] = None  # Only used when type=LIST, e.g. ["Option A", "Option B"]
    model_config = ConfigDict(extra='forbid') # STRICT

    @field_validator('options')
    @classmethod
    def validate_list_options(cls, v, info):
        if info.data.get('type') == CustomFieldType.LIST:
            if not v or len(v) < 1:
                raise ValueError('LIST type requires at least one option')
        return v

class CustomerFormConfigurationCreateUpdate(BaseModel):
    field_configurations: Dict[str, StandardFieldConfig] = Field(default_factory=dict)
    custom_field_1_config: Optional[CustomFieldConfig] = None
    custom_field_2_config: Optional[CustomFieldConfig] = None
    mandatory_document_types: List[str] = Field(default=["FORMAL_REQUEST"])
    reference_types: Optional[List[Dict[str, str]]] = None  # [{id, name}] — defaults applied in frontend
    document_config: Optional[Dict[str, Dict[str, bool]]] = None  # {DOC_TYPE: {is_visible, is_mandatory}}

    @field_validator('field_configurations')
    @classmethod
    def restrict_unhideable_fields(cls, v: Dict[str, StandardFieldConfig]) -> Dict[str, StandardFieldConfig]:
        """Hard-coded governance: Prevents admins from hiding critical LG fields."""
        unhideable_fields = [
            'amount', 'currency_id', 'requested_expiry_date', 
            'beneficiary_name', 'lg_type_id', 'issuing_entity_id'
        ]
        for field in unhideable_fields:
            if field in v and not v[field].is_visible:
                raise ValueError(f"System constraint: Field '{field}' cannot be hidden by admin configuration.")
        return v

# ==============================================================================
# 2. ISSUANCE REQUESTS (PHASE 1)
# ==============================================================================

class IssuanceRequestBase(BaseModel):
    issuing_entity_id: int
    requestor_name: str
    requestor_email: str
    department: Optional[str] = "General"
    job_title: Optional[str] = None
    phone_number: Optional[str] = None
    employee_id: Optional[str] = None
    manager_email: Optional[str] = None
    second_line_manager_email: Optional[str] = None
    reference_type: Optional[str] = None
    reference_number: Optional[str] = None
    reference_amount: Optional[Decimal] = None
    reference_currency_id: Optional[int] = None
    reference_start_date: Optional[date] = None
    reference_end_date: Optional[date] = None
    project_id: Optional[int] = None
    lg_type_id: int
    lg_purpose: str
    amount: Decimal
    currency_id: int
    payable_currency_id: Optional[int] = None
    requested_issue_date: Optional[date] = None
    requested_expiry_date: date
    operational_status: Optional[str] = None
    lg_language: Optional[str] = "AR"
    is_auto_reducing: bool = False
    reduction_trigger: Optional[str] = None
    other_conditions: Optional[str] = None
    beneficiary_id_number: Optional[str] = None
    beneficiary_name: str
    beneficiary_address: Optional[str] = None
    beneficiary_contact_person: Optional[str] = None
    beneficiary_phone: Optional[str] = None
    beneficiary_email: Optional[str] = None
    beneficiary_country: str
    is_third_party: bool = False
    third_party_name: Optional[str] = None
    third_party_address: Optional[str] = None
    third_party_relationship: Optional[str] = None
    is_cross_border: bool = False
    issuance_country: Optional[str] = None
    applicable_rules: Optional[str] = None
    cross_border_details: Optional[Dict[str, Any]] = None
    requires_special_wording: bool = False
    is_urgent: bool = False
    urgency_justification: Optional[str] = None
    comments: Optional[str] = None
    custom_field_1_value: Optional[str] = None
    custom_field_2_value: Optional[str] = None

    @field_validator('phone_number', 'beneficiary_phone')
    @classmethod
    def validate_phone(cls, v: Optional[str]) -> Optional[str]:
        if v:
            # Simple numeric or + format check
            import re
            if not re.match(r'^\+?[0-9]*$', v):
                raise ValueError("Phone number must contain only digits and an optional leading '+'")
        return v

    @field_validator('amount', 'reference_amount')
    @classmethod
    def validate_positive_amount(cls, v):
        if v is not None and v <= 0:
            raise ValueError("Amount must be greater than 0")
        return v

class IssuanceRequestCreate(IssuanceRequestBase):
    pass 

class IssuanceRequestDraftCreate(BaseModel):
    """Permissive schema for saving drafts — all fields optional."""
    issuing_entity_id: Optional[int] = None
    requestor_name: Optional[str] = None
    requestor_email: Optional[str] = None
    department: Optional[str] = "General"
    job_title: Optional[str] = None
    phone_number: Optional[str] = None
    employee_id: Optional[str] = None
    manager_email: Optional[str] = None
    second_line_manager_email: Optional[str] = None
    reference_type: Optional[str] = None
    reference_number: Optional[str] = None
    reference_amount: Optional[Decimal] = None
    reference_currency_id: Optional[int] = None
    reference_start_date: Optional[date] = None
    reference_end_date: Optional[date] = None
    project_id: Optional[int] = None
    lg_type_id: Optional[int] = None
    lg_purpose: Optional[str] = None
    amount: Optional[Decimal] = None
    currency_id: Optional[int] = None
    payable_currency_id: Optional[int] = None
    requested_issue_date: Optional[date] = None
    requested_expiry_date: Optional[date] = None
    operational_status: Optional[str] = None
    lg_language: Optional[str] = "AR"
    is_auto_reducing: bool = False
    reduction_trigger: Optional[str] = None
    other_conditions: Optional[str] = None
    beneficiary_id_number: Optional[str] = None
    beneficiary_name: Optional[str] = None
    beneficiary_address: Optional[str] = None
    beneficiary_contact_person: Optional[str] = None
    beneficiary_phone: Optional[str] = None
    beneficiary_email: Optional[str] = None
    beneficiary_country: Optional[str] = None
    is_third_party: bool = False
    third_party_name: Optional[str] = None
    third_party_address: Optional[str] = None
    third_party_relationship: Optional[str] = None
    is_cross_border: bool = False
    issuance_country: Optional[str] = None
    applicable_rules: Optional[str] = None
    cross_border_details: Optional[Dict[str, Any]] = None
    requires_special_wording: bool = False
    is_urgent: bool = False
    urgency_justification: Optional[str] = None
    comments: Optional[str] = None
    custom_field_1_value: Optional[str] = None
    custom_field_2_value: Optional[str] = None

class IssuanceRequestUpdate(IssuanceRequestBase):
    __annotations__ = {k: Optional[v] for k, v in IssuanceRequestBase.__annotations__.items()}
    __annotations__['change_reason'] = Optional[str]
    change_reason: Optional[str] = None

class CorporateProjectOut(BaseModel):
    id: int
    name: str
    project_type: str
    reference_number: Optional[str] = None
    status: str
    model_config = ConfigDict(from_attributes=True)

class IssuanceRequestOut(IssuanceRequestBase):
    # Override required base fields to Optional for draft compatibility
    issuing_entity_id: Optional[int] = None
    requestor_name: Optional[str] = None
    requestor_email: Optional[str] = None
    lg_type_id: Optional[int] = None
    lg_purpose: Optional[str] = None
    amount: Optional[Decimal] = None
    currency_id: Optional[int] = None
    requested_expiry_date: Optional[date] = None
    beneficiary_name: Optional[str] = None
    beneficiary_country: Optional[str] = None

    id: int
    customer_id: int
    serial_number: str
    status: str
    transaction_type: str
    current_version_number: int
    locked_for_issuance: bool
    lg_record_id: Optional[int] = None
    selected_sub_limit_id: Optional[int] = None
    cancellation_reason: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    # Approval tracking fields
    current_approval_step: Optional[int] = None
    signatures_collected: Optional[int] = None
    pending_approver_users: Optional[List[Any]] = None
    approval_chain_audit: Optional[List[Dict[str, Any]]] = None
    requestor_user_id: Optional[int] = None
    returned_from_step: Optional[int] = None
    revision_notes: Optional[str] = None
    change_reason: Optional[str] = None
    # Nested currency object
    currency: Optional[CurrencyOut] = None
    payable_currency: Optional[CurrencyOut] = None
    reference_currency: Optional[CurrencyOut] = None
    issuing_entity: Optional[CustomerEntityOut] = None
    lg_type: Optional[LgTypeOut] = None
    project: Optional[CorporateProjectOut] = None
    applicable_rules: Optional[str] = None
    cross_border_details: Optional[Dict[str, Any]] = None
    treasury_enrichment: Optional[Dict[str, Any]] = None
    model_config = ConfigDict(from_attributes=True)

class IssuanceRequestVersionOut(BaseModel):
    id: int
    request_id: int
    version_number: int
    edited_by_user_id: int
    change_reason: Optional[str] = None
    changed_fields: Dict[str, Any]
    model_config = ConfigDict(from_attributes=True)

class IssuedLGRecordOut(BaseModel):
    message: str
    lg_record_id: Optional[int] = None

class IssuanceExecuteRequest(BaseModel):
    """Request body for the unified /issue endpoint."""
    sub_limit_id: Optional[int] = None
    bank_id: Optional[int] = None
    bank_method_id: Optional[int] = None
    issued_ref_number: str
    issue_date: Optional[date] = None
    expiry_date: Optional[date] = None
    issuance_method: Optional[str] = "MANUAL"
    manual_pricing: Optional[Dict[str, Any]] = None  # Cost/margin data for no-facility LGs

class IssuanceCancelRequest(BaseModel):
    """Request body for cancellation."""
    reason: str

class CancellationResolveIn(BaseModel):
    """Admin resolves (approve/reject) a cancellation request."""
    approved: bool
    note: Optional[str] = None

class IssuedLGRecordDetailOut(BaseModel):
    """Full LG record response with all tracking fields."""
    id: int
    lg_ref_number: str
    internal_serial: Optional[str] = None
    customer_id: int
    facility_sub_limit_id: Optional[int] = None
    request_id: Optional[int] = None
    beneficiary_name: str
    current_amount: Decimal
    currency_id: int
    issue_date: Optional[date] = None
    expiry_date: Optional[date] = None
    status: str
    issued_by_user_id: Optional[int] = None
    issuance_method: Optional[str] = None
    bank_confirmation_date: Optional[date] = None
    bank_confirmation_ref: Optional[str] = None
    original_copy_collected_by: Optional[str] = None
    original_copy_collected_date: Optional[date] = None
    soft_copy_path: Optional[str] = None
    custody_holder: Optional[str] = None
    custody_transfer_log: Optional[List[Dict[str, Any]]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)

# ==============================================================================
# 3. FACILITIES & EXPOSURE (EXISTING)
# ==============================================================================

class CountryRule(BaseModel):
    type: str 
    list: List[str] = Field(default_factory=list)

class IssuanceFacilitySubLimitBase(BaseModel):
    limit_name: str
    limit_amount: Decimal
    lg_type_ids: List[int] = []
    max_amount_per_lg: Optional[Decimal] = None
    max_tenor_days: Optional[int] = None
    allowed_countries: Optional[CountryRule] = None
    allows_confirmation: bool = False
    default_commission_rate: Optional[Decimal] = Decimal("0.0")
    default_cash_margin_pct: Optional[Decimal] = Decimal("0.0")
    default_min_commission: Optional[Decimal] = Decimal("0.0")
    default_flat_fee: Optional[Decimal] = Decimal("0.0")
    dedicated_project_ids: Optional[List[int]] = None
    initial_utilization: Optional[Decimal] = Decimal("0")

class IssuanceFacilitySubLimitCreate(IssuanceFacilitySubLimitBase):
    pass

class IssuanceFacilitySubLimitOut(IssuanceFacilitySubLimitBase):
    id: int
    facility_id: int
    model_config = ConfigDict(from_attributes=True)

class IssuanceFacilityAuditLogOut(BaseModel):
    id: int
    facility_id: int
    performed_by_user_id: int
    action: str
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    timestamp: datetime
    model_config = ConfigDict(from_attributes=True)

class IssuanceExposureEntryOut(BaseModel):
    id: int
    facility_id: int
    sub_limit_id: int
    entry_type: str 
    original_amount_delta: Decimal
    original_currency_id: int
    fx_rate_used: Decimal
    facility_equivalent_delta: Decimal
    is_active: bool
    effective_date: date
    request_id: int
    lg_record_id: Optional[int] = None
    model_config = ConfigDict(from_attributes=True)

class IssuanceFacilityBase(BaseModel):
    facility_name: str 
    facility_type: str = "LG" 
    bank_id: int
    customer_id: int
    currency_id: int
    total_limit_amount: Decimal 
    reference_number: Optional[str] = None
    foreign_bank_name: Optional[str] = None
    foreign_bank_country: Optional[str] = None
    foreign_bank_address: Optional[str] = None
    foreign_bank_swift_code: Optional[str] = None
    tenor_months: Optional[int] = 12
    multi_currency_allowed: bool = False
    fx_breach_auto_suspend: bool = False
    margin_reduces_exposure: bool = False
    exposure_start_trigger: str = "ON_ISSUANCE"
    facility_default_margin_pct: Optional[Decimal] = None
    sla_agreement_days: Optional[int] = None
    allow_cross_border: bool = False
    allow_third_party_issuance: bool = False
    required_cash_margin_days: int = 0
    start_date: Optional[date] = None
    expiry_date: Optional[date] = None
    review_date: Optional[date] = None
    review_required_flag: bool = False
    internal_notes: Optional[str] = None
    bank_account_id: Optional[int] = None

class IssuanceFacilityCreate(IssuanceFacilityBase):
    sub_limits: List[IssuanceFacilitySubLimitCreate] = []
    entity_ids: Optional[List[int]] = []

    @field_validator('sub_limits')
    @classmethod
    def validate_at_least_one_sub_limit(cls, v):
        if not v or len(v) < 1:
            raise ValueError('Each facility must have at least one sub-limit.')
        return v

class IssuanceFacilityUpdate(BaseModel):
    __annotations__ = {k: Optional[v] for k, v in IssuanceFacilityBase.__annotations__.items()}
    __annotations__['entity_ids'] = Optional[List[int]]
    __annotations__['sub_limits'] = Optional[List[IssuanceFacilitySubLimitCreate]]

    entity_ids: Optional[List[int]] = None
    sub_limits: Optional[List[IssuanceFacilitySubLimitCreate]] = None

class IssuanceFacilityOut(IssuanceFacilityBase):
    id: int
    customer_id: int
    status: str
    created_at: datetime
    is_deleted: bool
    utilized_amount: Optional[Decimal] = Decimal("0")
    reserved_amount: Optional[Decimal] = Decimal("0")
    bank: Optional[BankOut] = None
    currency: Optional[CurrencyOut] = None
    entities: List[CustomerEntityOut] = []
    sub_limits: List[IssuanceFacilitySubLimitOut] = []
    model_config = ConfigDict(from_attributes=True)

class BankIssuanceOptionOut(BaseModel):
    id: int
    bank_id: int
    display_name: str
    strategy_code: str
    configuration: Dict[str, Any]
    is_active: bool
    model_config = ConfigDict(from_attributes=True)

class BankIssuanceOptionCreateUpdate(BaseModel):
    display_name: str
    strategy_code: str
    configuration: Dict[str, Any] = {}
    is_active: bool = True

class SuitableFacilityOut(BaseModel):
    facility_id: int
    facility_bank: str
    bank_id: int
    sub_limit_id: int
    sub_limit_name: str
    limit_available: Decimal
    total_limit: Decimal = Decimal(0)
    total_used: Decimal = Decimal(0)
    utilization_pct: float = 0.0
    has_sufficient_limit: bool = True
    available_methods: List[BankIssuanceOptionOut] = []
    price_commission_rate: Decimal
    price_cash_margin_pct: Decimal
    estimated_commission_cost: Decimal
    required_cash_margin_amount: Decimal
    facility_score: float = 0.0
    recommendation_tags: List[str] = []

# ==============================================================================
# 4. RECONCILIATION & WORKFLOW (EXISTING)
# ==============================================================================

class BankPositionRow(BaseModel):
    ref_number: str
    amount: Decimal
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
    discrepancies: List[Dict[str, Any]]

class IssuanceWorkflowPolicyBase(BaseModel):
    step_sequence: int = 1
    condition_type: str = "ALWAYS"
    condition_value: Optional[str] = None
    currency_id: Optional[int] = None
    approver_type: str = "ROLE"
    approver_values: List[str] = []
    required_signatures: int = 1
    is_active: bool = True

class IssuanceWorkflowPolicyCreate(IssuanceWorkflowPolicyBase):
    pass 

class IssuanceWorkflowPolicyOut(IssuanceWorkflowPolicyBase):
    id: int
    customer_id: int
    model_config = ConfigDict(from_attributes=True)

IssuanceFacilityOut.model_rebuild()

IssuanceRequestContentUpdate = IssuanceRequestUpdate


# ==============================================================================
# ADMIN DUAL-CONTROL SCHEMAS
# ==============================================================================

class AdminChangeRequestCreate(BaseModel):
    change_type: str = Field(..., description="CONFIG_UPDATE, APPROVAL_MATRIX_UPDATE, DEPARTMENT_CREATE, DEPARTMENT_UPDATE, GROUP_CREATE, GROUP_UPDATE")
    change_payload: Dict[str, Any] = Field(..., description="JSON payload with the change details")

class AdminChangeRequestOut(BaseModel):
    id: int
    customer_id: int
    requested_by_user_id: int
    approved_by_user_id: Optional[int] = None
    change_type: str
    change_payload: Dict[str, Any]
    status: str
    rejection_reason: Optional[str] = None
    applied_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    requested_by_email: Optional[str] = None
    approved_by_email: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)

class AdminChangeRequestAction(BaseModel):
    action: str = Field(..., description="APPROVE or REJECT")
    rejection_reason: Optional[str] = None


# ==============================================================================
# BANK FORM ISSUE REPORT SCHEMAS
# ==============================================================================

class BankFormIssueReportCreate(BaseModel):
    bank_id: int
    form_config_id: Optional[int] = None
    issue_type: str = Field(..., description="MISSING_FIELD, INCORRECT_FORMAT, OUTDATED_TEMPLATE, LAYOUT_ERROR, MISSING_BANK_FORM, OTHER")
    description: str = Field(..., min_length=3)
    field_name: Optional[str] = None
    severity: str = "MEDIUM"

class BankFormIssueReportOut(BaseModel):
    id: int
    customer_id: int
    reported_by_user_id: int
    bank_id: int
    form_config_id: Optional[int] = None
    issue_type: str
    description: str
    field_name: Optional[str] = None
    severity: str
    status: str
    resolution_notes: Optional[str] = None
    attachment_path: Optional[str] = None
    created_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    reported_by_email: Optional[str] = None
    bank_name: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)

class BankFormIssueReportUpdate(BaseModel):
    status: Optional[str] = None
    resolution_notes: Optional[str] = None
    severity: Optional[str] = None

# ==============================================================================
# ISSUANCE OWNERSHIP MANAGEMENT (HANDOVER)
# ==============================================================================

class RequestorProfile(BaseModel):
    email: str
    name: Optional[str] = None
    department: Optional[str] = None
    job_title: Optional[str] = None
    phone_number: Optional[str] = None
    employee_id: Optional[str] = None
    manager_email: Optional[str] = None
    second_line_manager_email: Optional[str] = None

class InitiateHandoverPayload(BaseModel):
    lg_ids: List[int]
    new_requestor: RequestorProfile

class ForceHandoverPayload(BaseModel):
    lg_ids: List[int]
    new_requestor: RequestorProfile

class EditRequestorProfilePayload(BaseModel):
    old_email: str
    updated_profile: RequestorProfile
    update_all_lgs: bool = True