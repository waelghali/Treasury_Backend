# app/services/notifications/catalog.py
"""
Notification event catalog (single source of truth for event definitions).

Each EventSpec defines *what* an event says and *who* receives it by default.
Text is deliberately limited to non-sensitive fields (RFQ reference + action)
so the same rendering is safe for phone lock screens: no amounts, rates,
currency pairs, bank names or client names.

Phase N6 will let the System Owner override wording, channels, urgency and the
role audiences per event; these specs remain the defaults and the fallback.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Tuple

from app.services.notifications.audiences import AudienceKey


class EventCode(str, Enum):
    # Quotation — instant workflow events (Phase Q1)
    RFQ_APPROVAL_REQUIRED = "RFQ_APPROVAL_REQUIRED"
    RFQ_APPROVED = "RFQ_APPROVED"
    RFQ_REJECTED = "RFQ_REJECTED"
    RFQ_REVISION_REQUESTED = "RFQ_REVISION_REQUESTED"
    RFQ_CANCELLATION_REQUESTED = "RFQ_CANCELLATION_REQUESTED"
    RFQ_CANCELLATION_DECIDED = "RFQ_CANCELLATION_DECIDED"

    # Quotation — timed events raised by the window sweeper (Phase Q4)
    RFQ_STARTING_SOON = "RFQ_STARTING_SOON"
    RFQ_NO_DEALERS_CHECKED_IN = "RFQ_NO_DEALERS_CHECKED_IN"
    RFQ_CLOSING_SOON = "RFQ_CLOSING_SOON"
    RFQ_QUOTES_READY = "RFQ_QUOTES_READY"
    RFQ_RESULTS_READY = "RFQ_RESULTS_READY"
    RFQ_CLOSED_NO_QUOTES = "RFQ_CLOSED_NO_QUOTES"
    RFQ_DEAL_AUTO_RESOLVED = "RFQ_DEAL_AUTO_RESOLVED"

    # LG Custody (Inbound Guarantees)
    LG_CUSTODY_INACTION_ESCALATION = "LG_CUSTODY_INACTION_ESCALATION"
    LG_CUSTODY_UNDELIVERED_LETTER = "LG_CUSTODY_UNDELIVERED_LETTER"
    LG_CUSTODY_APPROVAL_REQUIRED = "LG_CUSTODY_APPROVAL_REQUIRED"
    LG_CUSTODY_DAY_ZERO = "LG_CUSTODY_DAY_ZERO"
    LG_CUSTODY_BANK_DEFECT = "LG_CUSTODY_BANK_DEFECT"
    LG_USER_EXPIRY_ACTION_NOTICE = "LG_USER_EXPIRY_ACTION_NOTICE"
    LG_USER_PRE_ESCALATION_WARNING = "LG_USER_PRE_ESCALATION_WARNING"
    LG_USER_REQUEST_DECIDED = "LG_USER_REQUEST_DECIDED"
    LG_CUSTODY_PORTFOLIO_BURST_DIGEST = "LG_CUSTODY_PORTFOLIO_BURST_DIGEST"


class Urgency(str, Enum):
    # Values mirror the RFC 8030 Web Push "Urgency" header (used from Phase Q3).
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


class LinkTarget(str, Enum):
    RFQ_RESULTS = "RFQ_RESULTS"          # /{role}/quotations/results/{rfq_id}
    RFQ_APPROVALS = "RFQ_APPROVALS"      # /corporate-admin/quotations/approvals
    RFQ_ACTIVE = "RFQ_ACTIVE"            # maker's active/editable RFQs
    RFQ_HISTORY = "RFQ_HISTORY"          # maker's RFQ history
    LG_CUSTODY_DETAIL = "LG_CUSTODY_DETAIL"          # /{role}/lg-records/{lg_id}
    LG_CUSTODY_LIST = "LG_CUSTODY_LIST"              # /{role}/lg-records
    LG_CUSTODY_APPROVALS = "LG_CUSTODY_APPROVALS"    # /corporate-admin/approval-requests


class ExpiryPolicy(str, Enum):
    UNTIL_WINDOW_START = "UNTIL_WINDOW_START"  # falls back to window_end when start has passed
    UNTIL_WINDOW_END = "UNTIL_WINDOW_END"
    UNTIL_ACCEPTANCE_DEADLINE = "UNTIL_ACCEPTANCE_DEADLINE"
    FIXED_24H = "FIXED_24H"


@dataclass(frozen=True)
class EventSpec:
    code: EventCode
    module: str
    category: str                      # user-facing preference bucket (Phase Q5)
    audiences: Tuple[AudienceKey, ...]
    title: str                         # str.format() over the safe render context
    body: str
    link: LinkTarget
    urgency: Urgency = Urgency.NORMAL
    expiry: ExpiryPolicy = ExpiryPolicy.FIXED_24H
    # Placeholders allowed in title/body. Anything else is a programming error.
    allowed_fields: Tuple[str, ...] = field(default=("ref_no",))


QUOTATIONS = "QUOTATIONS"
LG_CUSTODY = "LG_CUSTODY"

CATALOG: Dict[EventCode, EventSpec] = {
    EventCode.RFQ_APPROVAL_REQUIRED: EventSpec(
        code=EventCode.RFQ_APPROVAL_REQUIRED,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.CUSTOMER_CORPORATE_ADMINS,),
        title="RFQ {ref_no} · Approval required",
        body="A quotation request is waiting for your approval before it can be sent to banks.",
        link=LinkTarget.RFQ_APPROVALS,
        urgency=Urgency.HIGH,
        expiry=ExpiryPolicy.UNTIL_WINDOW_START,
    ),
    EventCode.RFQ_APPROVED: EventSpec(
        code=EventCode.RFQ_APPROVED,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.RFQ_CREATOR,),
        title="RFQ {ref_no} · Approved",
        body="Your quotation request was approved and will be sent to the selected banks.",
        link=LinkTarget.RFQ_RESULTS,
    ),
    EventCode.RFQ_REJECTED: EventSpec(
        code=EventCode.RFQ_REJECTED,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.RFQ_CREATOR,),
        title="RFQ {ref_no} · Rejected",
        body="Your quotation request was rejected by a corporate admin.",
        link=LinkTarget.RFQ_HISTORY,
    ),
    EventCode.RFQ_REVISION_REQUESTED: EventSpec(
        code=EventCode.RFQ_REVISION_REQUESTED,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.RFQ_CREATOR,),
        title="RFQ {ref_no} · Revision requested",
        body="A corporate admin returned your quotation request for changes.",
        link=LinkTarget.RFQ_ACTIVE,
    ),
    EventCode.RFQ_CANCELLATION_REQUESTED: EventSpec(
        code=EventCode.RFQ_CANCELLATION_REQUESTED,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.CUSTOMER_CORPORATE_ADMINS,),
        title="RFQ {ref_no} · Cancellation requested",
        body="A cancellation request is waiting for your decision.",
        link=LinkTarget.RFQ_APPROVALS,
        urgency=Urgency.HIGH,
    ),
    EventCode.RFQ_CANCELLATION_DECIDED: EventSpec(
        code=EventCode.RFQ_CANCELLATION_DECIDED,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.CANCELLATION_REQUESTER,),
        title="RFQ {ref_no} · Cancellation {decision}",
        body="Your cancellation request was {decision} by a corporate admin.",
        link=LinkTarget.RFQ_HISTORY,
        allowed_fields=("ref_no", "decision"),
    ),

    # ── Timed events (Phase Q4) ──────────────────────────────────────────────────────
    EventCode.RFQ_STARTING_SOON: EventSpec(
        code=EventCode.RFQ_STARTING_SOON,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.RFQ_CREATOR, AudienceKey.RFQ_DELEGATE),
        title="RFQ {ref_no} · Starting soon",
        body="The quotation window opens in 5 minutes. Get ready to monitor incoming quotes.",
        link=LinkTarget.RFQ_RESULTS,
        urgency=Urgency.NORMAL,
        expiry=ExpiryPolicy.UNTIL_WINDOW_START,
    ),
    EventCode.RFQ_NO_DEALERS_CHECKED_IN: EventSpec(
        code=EventCode.RFQ_NO_DEALERS_CHECKED_IN,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.RFQ_CREATOR, AudienceKey.RFQ_DELEGATE),
        title="RFQ {ref_no} · Low attendance warning",
        body="0 of {invited_count} invited banks are currently checked in for the upcoming window.",
        link=LinkTarget.RFQ_RESULTS,
        urgency=Urgency.HIGH,
        expiry=ExpiryPolicy.UNTIL_WINDOW_START,
        allowed_fields=("ref_no", "invited_count"),
    ),
    # Who can accept/reject a deal: the maker, their delegate, and corporate admins.
    EventCode.RFQ_CLOSING_SOON: EventSpec(
        code=EventCode.RFQ_CLOSING_SOON,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(
            AudienceKey.RFQ_CREATOR,
            AudienceKey.RFQ_DELEGATE,
            AudienceKey.CUSTOMER_CORPORATE_ADMINS,
        ),
        title="RFQ {ref_no} · Closing soon",
        body="Be ready: the quotation window closes in about 2 minutes and you will need to decide quickly.",
        link=LinkTarget.RFQ_RESULTS,
        urgency=Urgency.HIGH,
        expiry=ExpiryPolicy.UNTIL_WINDOW_END,
    ),
    EventCode.RFQ_QUOTES_READY: EventSpec(
        code=EventCode.RFQ_QUOTES_READY,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(
            AudienceKey.RFQ_CREATOR,
            AudienceKey.RFQ_DELEGATE,
            AudienceKey.CUSTOMER_CORPORATE_ADMINS,
        ),
        title="RFQ {ref_no} · Quotes ready – decide now",
        body="Quotes are ready for acceptance. Accept or reject before the acceptance time runs out.",
        link=LinkTarget.RFQ_RESULTS,
        urgency=Urgency.HIGH,
        expiry=ExpiryPolicy.UNTIL_ACCEPTANCE_DEADLINE,
    ),
    EventCode.RFQ_RESULTS_READY: EventSpec(
        code=EventCode.RFQ_RESULTS_READY,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.RFQ_CREATOR, AudienceKey.RFQ_DELEGATE),
        title="RFQ {ref_no} · Results ready",
        body="The quotation window has closed. Open Grow to review the indicative results.",
        link=LinkTarget.RFQ_RESULTS,
    ),
    EventCode.RFQ_CLOSED_NO_QUOTES: EventSpec(
        code=EventCode.RFQ_CLOSED_NO_QUOTES,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(AudienceKey.RFQ_CREATOR, AudienceKey.RFQ_DELEGATE),
        title="RFQ {ref_no} · Closed with no quotes",
        body="The quotation window closed without any executable quotes.",
        link=LinkTarget.RFQ_RESULTS,
    ),
    EventCode.RFQ_DEAL_AUTO_RESOLVED: EventSpec(
        code=EventCode.RFQ_DEAL_AUTO_RESOLVED,
        module="QUOTATION",
        category=QUOTATIONS,
        audiences=(
            AudienceKey.RFQ_CREATOR,
            AudienceKey.RFQ_DELEGATE,
            AudienceKey.CUSTOMER_CORPORATE_ADMINS,
        ),
        title="RFQ {ref_no} · Deal {decision}",
        body="The acceptance time ran out and the deal was {decision} by your company policy.",
        link=LinkTarget.RFQ_RESULTS,
        allowed_fields=("ref_no", "decision"),
    ),

    # ── LG Custody events ──────────────────────────────────────────────────────────
    EventCode.LG_CUSTODY_INACTION_ESCALATION: EventSpec(
        code=EventCode.LG_CUSTODY_INACTION_ESCALATION,
        module="CUSTODY",
        category=LG_CUSTODY,
        audiences=(AudienceKey.CUSTOMER_CORPORATE_ADMINS,),
        title="Action Overdue · Custody Escalation",
        body="LG {masked_lg_no} ({masked_issuer}) expires in {days}d. No extension or release instruction initiated by treasury user.",
        link=LinkTarget.LG_CUSTODY_DETAIL,
        urgency=Urgency.HIGH,
        allowed_fields=("masked_lg_no", "masked_issuer", "days"),
    ),
    EventCode.LG_CUSTODY_UNDELIVERED_LETTER: EventSpec(
        code=EventCode.LG_CUSTODY_UNDELIVERED_LETTER,
        module="CUSTODY",
        category=LG_CUSTODY,
        audiences=(AudienceKey.CUSTOMER_CORPORATE_ADMINS,),
        title="Risk Alert · Undelivered Bank Letter",
        body="Instruction for LG {masked_lg_no} ({masked_issuer}) was generated but not marked delivered to the bank. Expiry in {days}d.",
        link=LinkTarget.LG_CUSTODY_DETAIL,
        urgency=Urgency.HIGH,
        allowed_fields=("masked_lg_no", "masked_issuer", "days"),
    ),
    EventCode.LG_CUSTODY_APPROVAL_REQUIRED: EventSpec(
        code=EventCode.LG_CUSTODY_APPROVAL_REQUIRED,
        module="CUSTODY",
        category=LG_CUSTODY,
        audiences=(AudienceKey.CUSTOMER_CORPORATE_ADMINS,),
        title="Authorization Required · Custody Action",
        body="User requested {action_type} for LG {masked_lg_no} ({masked_issuer}). Open desktop terminal to review.",
        link=LinkTarget.LG_CUSTODY_APPROVALS,
        urgency=Urgency.HIGH,
        allowed_fields=("masked_lg_no", "masked_issuer", "action_type"),
    ),
    EventCode.LG_CUSTODY_DAY_ZERO: EventSpec(
        code=EventCode.LG_CUSTODY_DAY_ZERO,
        module="CUSTODY",
        category=LG_CUSTODY,
        audiences=(AudienceKey.CUSTOMER_CORPORATE_ADMINS,),
        title="Final Cutoff · Guarantee Expiry Today",
        body="LG {masked_lg_no} ({masked_issuer}) expires today with no extension or claim on file.",
        link=LinkTarget.LG_CUSTODY_DETAIL,
        urgency=Urgency.HIGH,
        allowed_fields=("masked_lg_no", "masked_issuer"),
    ),
    EventCode.LG_CUSTODY_BANK_DEFECT: EventSpec(
        code=EventCode.LG_CUSTODY_BANK_DEFECT,
        module="CUSTODY",
        category=LG_CUSTODY,
        audiences=(AudienceKey.CUSTOMER_CORPORATE_ADMINS,),
        title="Notice · Bank Objection / Defect",
        body="Bank reply recorded for LG {masked_lg_no} ({masked_issuer}) indicates an objection or defect. Review required.",
        link=LinkTarget.LG_CUSTODY_DETAIL,
        urgency=Urgency.HIGH,
        allowed_fields=("masked_lg_no", "masked_issuer"),
    ),
    EventCode.LG_USER_EXPIRY_ACTION_NOTICE: EventSpec(
        code=EventCode.LG_USER_EXPIRY_ACTION_NOTICE,
        module="CUSTODY",
        category=LG_CUSTODY,
        audiences=(AudienceKey.CUSTOMER_END_USERS,),
        title="Advisory · Guarantee Action Required",
        body="LG {masked_lg_no} ({masked_issuer}) expires in 30 days. Please log into desktop terminal to prepare formal instruction.",
        link=LinkTarget.LG_CUSTODY_DETAIL,
        urgency=Urgency.NORMAL,
        allowed_fields=("masked_lg_no", "masked_issuer"),
    ),
    EventCode.LG_USER_PRE_ESCALATION_WARNING: EventSpec(
        code=EventCode.LG_USER_PRE_ESCALATION_WARNING,
        module="CUSTODY",
        category=LG_CUSTODY,
        audiences=(AudienceKey.CUSTOMER_END_USERS,),
        title="Notice · Pending Management Escalation",
        body="LG {masked_lg_no} ({masked_issuer}) requires action. Case will escalate to Corporate Treasury in 48h if unaddressed.",
        link=LinkTarget.LG_CUSTODY_DETAIL,
        urgency=Urgency.HIGH,
        allowed_fields=("masked_lg_no", "masked_issuer"),
    ),
    EventCode.LG_USER_REQUEST_DECIDED: EventSpec(
        code=EventCode.LG_USER_REQUEST_DECIDED,
        module="CUSTODY",
        category=LG_CUSTODY,
        audiences=(AudienceKey.LG_ACTION_MAKER,),
        title="Update · Request {status}",
        body="Your request for LG {masked_lg_no} ({masked_issuer}) has been {status} by Corporate Treasury.",
        link=LinkTarget.LG_CUSTODY_DETAIL,
        urgency=Urgency.NORMAL,
        allowed_fields=("masked_lg_no", "masked_issuer", "status"),
    ),
    EventCode.LG_CUSTODY_PORTFOLIO_BURST_DIGEST: EventSpec(
        code=EventCode.LG_CUSTODY_PORTFOLIO_BURST_DIGEST,
        module="CUSTODY",
        category=LG_CUSTODY,
        audiences=(AudienceKey.CUSTOMER_CORPORATE_ADMINS,),
        title="Portfolio Alert · {count} Guarantees Require Attention",
        body="{count} guarantees in your custody portfolio triggered '{summary_type}' today (including LG {masked_lg_no}). Open Grow to review all.",
        link=LinkTarget.LG_CUSTODY_LIST,
        urgency=Urgency.HIGH,
        allowed_fields=("count", "summary_type", "masked_lg_no"),
    ),
}


def get_spec(code) -> EventSpec:
    """Returns the spec for an event code. Raises ValueError for unknown codes."""
    try:
        return CATALOG[EventCode(code)]
    except (ValueError, KeyError):
        raise ValueError(f"Unknown notification event code: {code!r}")
