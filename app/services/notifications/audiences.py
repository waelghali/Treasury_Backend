# app/services/notifications/audiences.py
"""
Role-based audience resolution for the notification core.

Decision (2026-10-26): audiences are configurable *by role*, never by individual
user. Each AudienceKey is resolved here, in code, and every resolver is scoped
to the RFQ's customer. A final defence-in-depth filter re-checks tenancy on the
loaded User rows, so a bad foreign key can never leak an alert across customers.
"""

import logging
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.constants import UserRole
from app.models.models import User, UserCustomerEntityAssociation
from app.models.models_quotation import QuotationRequest

logger = logging.getLogger(__name__)


class AudienceKey(str, Enum):
    RFQ_CREATOR = "RFQ_CREATOR"
    RFQ_DELEGATE = "RFQ_DELEGATE"
    CUSTOMER_CORPORATE_ADMINS = "CUSTOMER_CORPORATE_ADMINS"
    CANCELLATION_REQUESTER = "CANCELLATION_REQUESTER"
    CUSTOMER_END_USERS = "CUSTOMER_END_USERS"
    LG_INTERNAL_OWNER = "LG_INTERNAL_OWNER"  # Alias kept for backward compatibility -> resolves to CUSTOMER_END_USERS
    LG_ACTION_MAKER = "LG_ACTION_MAKER"


def _creator(db: Session, rfq: QuotationRequest) -> List[int]:
    return [rfq.created_by_user_id] if rfq.created_by_user_id else []


def _delegate(db: Session, rfq: QuotationRequest) -> List[int]:
    return [rfq.delegated_to_user_id] if rfq.delegated_to_user_id else []


def _customer_corporate_admins(db: Session, rfq: QuotationRequest) -> List[int]:
    rows = db.query(User.id).filter(
        User.customer_id == rfq.customer_id,
        User.role == UserRole.CORPORATE_ADMIN,
        User.is_deleted == False,  # noqa: E712
    ).all()
    return [r[0] for r in rows]


def _cancellation_requester(db: Session, rfq: QuotationRequest) -> List[int]:
    return [rfq.cancellation_requested_by] if rfq.cancellation_requested_by else []


_RESOLVERS = {
    AudienceKey.RFQ_CREATOR: _creator,
    AudienceKey.RFQ_DELEGATE: _delegate,
    AudienceKey.CUSTOMER_CORPORATE_ADMINS: _customer_corporate_admins,
    AudienceKey.CANCELLATION_REQUESTER: _cancellation_requester,
}


def resolve_rfq_audience(
    db: Session,
    rfq: QuotationRequest,
    audiences: Iterable[AudienceKey],
    actor_user_id: Optional[int] = None,
) -> Dict[int, UserRole]:
    """
    Returns {user_id: role} for all recipients of the given audience keys.

    Guarantees:
      - every recipient belongs to rfq.customer_id and is not deleted
      - the actor who triggered the event is never notified about their own action
      - duplicates across audience keys are collapsed
    """
    candidate_ids = set()
    for key in audiences:
        resolver = _RESOLVERS.get(AudienceKey(key))
        if resolver is None:
            raise ValueError(f"No resolver registered for audience '{key}'")
        candidate_ids.update(uid for uid in resolver(db, rfq) if uid)

    if actor_user_id:
        candidate_ids.discard(actor_user_id)
    if not candidate_ids:
        return {}

    # Defence in depth: re-verify tenancy and liveness on the actual User rows.
    users = db.query(User.id, User.role).filter(
        User.id.in_(candidate_ids),
        User.customer_id == rfq.customer_id,
        User.is_deleted == False,  # noqa: E712
    ).all()

    dropped = candidate_ids - {u.id for u in users}
    if dropped:
        logger.warning(
            "Notification audience: dropped %d out-of-scope/deleted recipient(s) for RFQ %s",
            len(dropped), rfq.id,
        )
    return {u.id: u.role for u in users}


def resolve_lg_audience(
    db: Session,
    customer_id: int,
    audiences: Iterable[AudienceKey],
    lg_record: Optional[Any] = None,
    target_user_id: Optional[int] = None,
    actor_user_id: Optional[int] = None,
) -> Dict[int, UserRole]:
    """
    Returns {user_id: role} for all recipients of the given audience keys for an LG Custody event.
    Scoped strictly to customer_id, non-deleted users, and excludes actor_user_id.
    """
    candidate_ids = set()
    for key in audiences:
        k = AudienceKey(key)
        if k == AudienceKey.CUSTOMER_CORPORATE_ADMINS:
            admin_rows = db.query(User.id).filter(
                User.customer_id == customer_id,
                User.role == UserRole.CORPORATE_ADMIN,
                User.is_deleted == False,  # noqa: E712
            ).all()
            candidate_ids.update(r[0] for r in admin_rows)
        elif k in (AudienceKey.CUSTOMER_END_USERS, AudienceKey.LG_INTERNAL_OWNER):
            entity_id = getattr(lg_record, "beneficiary_corporate_id", None) if lg_record else None
            base_query = db.query(User.id).filter(
                User.customer_id == customer_id,
                User.role == UserRole.END_USER,
                User.is_deleted == False,  # noqa: E712
            )
            if entity_id:
                entity_user_ids = db.query(UserCustomerEntityAssociation.user_id).filter(
                    UserCustomerEntityAssociation.customer_entity_id == entity_id,
                    UserCustomerEntityAssociation.is_deleted == False,  # noqa: E712
                )
                scoped_rows = base_query.filter(
                    or_(
                        User.has_all_entity_access == True,  # noqa: E712
                        User.id.in_(entity_user_ids),
                    )
                ).all()
                if scoped_rows:
                    candidate_ids.update(r[0] for r in scoped_rows)
                else:
                    # Fallback to all active end users of the customer if none matched entity
                    all_end_users = base_query.all()
                    candidate_ids.update(r[0] for r in all_end_users)
            else:
                all_end_users = base_query.all()
                candidate_ids.update(r[0] for r in all_end_users)
        elif k == AudienceKey.LG_ACTION_MAKER:
            if target_user_id:
                candidate_ids.add(target_user_id)
        else:
            logger.warning("resolve_lg_audience: unsupported audience key %s for LG event", key)

    if actor_user_id:
        candidate_ids.discard(actor_user_id)
    if not candidate_ids:
        return {}

    users = db.query(User.id, User.role).filter(
        User.id.in_(candidate_ids),
        User.customer_id == customer_id,
        User.is_deleted == False,  # noqa: E712
    ).all()

    return {u.id: u.role for u in users}

