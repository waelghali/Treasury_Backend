# app/schemas/schemas_inbox.py
"""
Pydantic Schemas for the Smart Inbox Module
"""

from typing import List, Optional, Any, Dict, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime, date
import math


# ==============================================================================
# ATTACHMENTS
# ==============================================================================

class InboxAttachmentOut(BaseModel):
    id: int
    inbox_item_id: int
    file_name: str
    file_type: Optional[str] = None
    file_size_bytes: Optional[int] = None
    storage_path: Optional[str] = None
    is_primary: bool = False
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ==============================================================================
# INBOX ITEMS
# ==============================================================================

class InboxItemListOut(BaseModel):
    id: int
    customer_id: int
    sender_email: str
    sender_domain: Optional[str] = None
    subject: Optional[str] = None
    received_at: datetime
    matched_bank_id: Optional[int] = None
    matched_bank_name: Optional[str] = None
    is_trusted_sender: bool = True
    classification: str
    classification_confidence: str
    confidence_score: int = 0
    has_attachment: bool = False
    attachment_count: int = 0
    primary_attachment_name: Optional[str] = None
    primary_attachment_type: Optional[str] = None
    pdf_password_required: bool = False
    status: str
    action_summary: Optional[str] = None
    error_message: Optional[str] = None
    actioned_at: Optional[datetime] = None
    action_reference_type: Optional[str] = None
    action_reference_id: Optional[int] = None
    is_duplicate: bool = False
    is_nil_position: bool = False
    extracted_balances_json: Optional[Any] = None
    ingestion_channel: Optional[str] = "IMAP_EMAIL"
    outbound_request_id: Optional[int] = None

    class Config:
        from_attributes = True


class InboxItemDetailOut(BaseModel):
    id: int
    customer_id: int
    message_id: str
    sender_email: str
    sender_domain: Optional[str] = None
    subject: Optional[str] = None
    body_preview: Optional[str] = None
    received_at: datetime
    matched_bank_id: Optional[int] = None
    matched_bank_name: Optional[str] = None
    is_trusted_sender: bool = True
    in_reply_to: Optional[str] = None
    outbound_request_id: Optional[int] = None
    classification: str
    classification_confidence: str
    confidence_score: int = 0
    classification_signals: Optional[Dict[str, Any]] = None
    user_override_classification: Optional[str] = None
    has_attachment: bool = False
    attachment_count: int = 0
    primary_attachment_name: Optional[str] = None
    primary_attachment_type: Optional[str] = None
    status: str
    action_summary: Optional[str] = None
    error_message: Optional[str] = None
    actioned_at: Optional[datetime] = None
    actioned_by_user_id: Optional[int] = None
    action_reference_type: Optional[str] = None
    action_reference_id: Optional[int] = None
    is_duplicate: bool = False
    is_nil_position: bool = False
    extracted_balances_json: Optional[Any] = None
    ingestion_channel: Optional[str] = "IMAP_EMAIL"
    duplicate_of_id: Optional[int] = None
    attachments: List[InboxAttachmentOut] = []
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


InboxClassificationLiteral = Literal[
    "LG_POSITION_REPORT",
    "BANK_STATEMENT",
    "CASH_POSITION_SNAPSHOT",
    "PROGRESS_REPORT",
    "IRRELEVANT",
    "UNCLASSIFIED",
]


class InboxItemReclassify(BaseModel):
    classification: InboxClassificationLiteral = Field(
        ...,
        description="Target classification: LG_POSITION_REPORT, BANK_STATEMENT, CASH_POSITION_SNAPSHOT, PROGRESS_REPORT, IRRELEVANT, or UNCLASSIFIED",
    )


class InboxStatsOut(BaseModel):
    total_received: int = 0
    pending_action: int = 0
    lg_position_count: int = 0
    bank_statement_count: int = 0
    cash_position_count: int = 0
    progress_report_count: int = 0
    irrelevant_count: int = 0
    unclassified_count: int = 0
    actioned_count: int = 0
    archived_count: int = 0
    parse_error_count: int = 0


# ==============================================================================
# OUTBOUND DATA REQUESTS
# ==============================================================================

class OutboundRequestCreate(BaseModel):
    bank_id: int
    request_type: str = "LG_POSITION"
    position_date: Optional[date] = None
    statement_period_start: Optional[date] = None
    statement_period_end: Optional[date] = None
    custom_recipient_emails: Optional[List[str]] = None
    custom_subject: Optional[str] = None
    custom_notes: Optional[str] = None


class OutboundRequestOut(BaseModel):
    id: int
    customer_id: int
    bank_id: Optional[int] = None
    bank_name: Optional[str] = None
    request_type: str
    request_reference: Optional[str] = None
    sent_to_emails: Any
    subject: str
    message_id: Optional[str] = None
    position_date: Optional[date] = None
    statement_period_start: Optional[date] = None
    statement_period_end: Optional[date] = None
    sent_at: datetime
    sent_by_user_id: Optional[int] = None
    is_replied: bool = False
    reply_received_at: Optional[datetime] = None
    is_scheduled: bool = False

    class Config:
        from_attributes = True


# ==============================================================================
# SCHEDULE CONFIGURATIONS
# ==============================================================================

class ScheduleConfigCreate(BaseModel):
    bank_id: int
    request_type: str = "LG_POSITION"
    is_active: bool = True
    frequency: str = "MONTHLY"  # DAILY, WEEKLY, MONTHLY
    day_of_month: Optional[int] = 1
    day_of_week: Optional[int] = None
    recipient_emails: Optional[List[str]] = None
    custom_subject: Optional[str] = None
    custom_body: Optional[str] = None


class BulkScheduleConfigCreate(BaseModel):
    bank_ids: List[int]
    request_type: str = "LG_POSITION"
    is_active: bool = True
    frequency: str = "MONTHLY"
    day_of_month: Optional[int] = 1
    day_of_week: Optional[int] = None
    custom_subject: Optional[str] = None
    custom_body: Optional[str] = None


class ScheduleConfigUpdate(BaseModel):
    is_active: Optional[bool] = None
    frequency: Optional[str] = None
    day_of_month: Optional[int] = None
    day_of_week: Optional[int] = None
    recipient_emails: Optional[List[str]] = None
    custom_subject: Optional[str] = None
    custom_body: Optional[str] = None


class ScheduleConfigOut(BaseModel):
    id: int
    customer_id: int
    bank_id: int
    bank_name: Optional[str] = None
    request_type: str
    is_active: bool
    frequency: str
    day_of_month: Optional[int] = None
    day_of_week: Optional[int] = None
    recipient_emails: Optional[Any] = None
    custom_subject: Optional[str] = None
    custom_body: Optional[str] = None
    last_sent_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    class Config:
        orm_mode = True


# ==============================================================================
# BANK DOMAIN UPDATE
# ==============================================================================

class BankDomainUpdate(BaseModel):
    email_domain: Optional[str] = Field(None, description="Domain like 'cibeg.com' or 'qnb.com.eg'")


# ==============================================================================
# WATCHED-FOLDER SYNC TOKENS
# ==============================================================================

class SyncTokenCreate(BaseModel):
    label: str = Field("Treasury Workstation Sync", min_length=2, max_length=120)


class SyncTokenOut(BaseModel):
    id: int
    label: str
    token_prefix: str
    is_active: bool
    last_used_at: Optional[datetime] = None
    files_synced_count: int = 0
    created_at: Optional[datetime] = None
    plaintext_token: Optional[str] = None

    class Config:
        from_attributes = True


class OverrideBalanceEntry(BaseModel):
    account_number: Optional[str] = Field(None, max_length=100)
    currency: str = Field("EGP", max_length=10)
    available_balance: Optional[float] = None
    ledger_balance: Optional[float] = None
    as_of_date: Optional[Any] = None

    @field_validator("currency", mode="before")
    @classmethod
    def _normalize_currency(cls, v: Any) -> str:
        if not v or not str(v).strip():
            return "EGP"
        return str(v).strip().upper()[:3]

    @field_validator("available_balance", "ledger_balance", mode="before")
    @classmethod
    def _validate_finite_balance(cls, v: Any) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, bool):
            raise ValueError("Balance must be a finite number")
        if isinstance(v, str):
            s = v.strip()
            if not s:
                raise ValueError("Balance cannot be empty")
            if "," in s:
                raise ValueError("Commas are not allowed in numeric balance values")
            num = float(s)
        else:
            num = float(v)
        if not math.isfinite(num):
            raise ValueError("Balance must be a finite number")
        return num

    @model_validator(mode="after")
    def _require_at_least_one_balance(self) -> "OverrideBalanceEntry":
        if self.available_balance is None and self.ledger_balance is None:
            raise ValueError("At least one of available_balance or ledger_balance must be provided")
        return self


class WatcherScriptRequest(BaseModel):
    token_hint: str = Field("", max_length=256)
