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


class ExpiryPolicy(str, Enum):
    UNTIL_WINDOW_START = "UNTIL_WINDOW_START"  # falls back to window_end when start has passed
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
}


def get_spec(code) -> EventSpec:
    """Returns the spec for an event code. Raises ValueError for unknown codes."""
    try:
        return CATALOG[EventCode(code)]
    except (ValueError, KeyError):
        raise ValueError(f"Unknown notification event code: {code!r}")
