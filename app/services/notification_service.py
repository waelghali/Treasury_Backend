# app/services/notification_service.py
"""
Centralized notification service.
Phase A: In-app notifications only (issuance module).
Phase B (future): Add dual-write to custody, email integration.
"""

import logging
from typing import List, Optional
from sqlalchemy.orm import Session

from app.models.models_notification import UserNotification

logger = logging.getLogger(__name__)


def notify(
    db: Session,
    user_ids: List[int],
    module: str,
    event_type: str,
    title: str,
    message: str,
    link: Optional[str] = None,
    actor_user_id: Optional[int] = None,
    reference_id: Optional[int] = None,
    reference_type: Optional[str] = None,
):
    """
    Creates in-app notifications for the given recipients.
    
    Args:
        db: Database session
        user_ids: List of user IDs to notify
        module: ISSUANCE, CUSTODY, QUOTATION, SYSTEM
        event_type: e.g. REQUEST_SUBMITTED, REQUEST_APPROVED
        title: Short notification title
        message: Detail text
        link: Optional deep link (e.g., /corporate-admin/issuance/requests)
        actor_user_id: User who triggered the event
        reference_id: ID of related object
        reference_type: Type of related object (IssuanceRequest, etc.)
    """
    if not user_ids:
        logger.warning(f"notify() called with empty user_ids for {module}:{event_type}")
        return

    # Deduplicate and filter out None/invalid
    unique_ids = list(set(uid for uid in user_ids if uid))
    if not unique_ids:
        return

    # Query user roles to tailor the link path between /corporate-admin/ and /end-user/
    from app.models import User
    user_roles = {}
    try:
        user_rows = db.query(User.id, User.role).filter(User.id.in_(unique_ids)).all()
        user_roles = {u[0]: (u[1] or "").lower() for u in user_rows}
    except Exception:
        pass
    
    notifications = []
    for uid in unique_ids:
        role = user_roles.get(uid, "end_user")
        role_path = "corporate-admin" if role in ("corporate_admin", "admin", "checker") else "end-user"
        
        user_link = link
        if user_link:
            if role_path == "end-user" and user_link.startswith("/corporate-admin/"):
                user_link = user_link.replace("/corporate-admin/", "/end-user/", 1)
            elif role_path == "corporate-admin" and user_link.startswith("/end-user/"):
                user_link = user_link.replace("/end-user/", "/corporate-admin/", 1)

        notif = UserNotification(
            user_id=uid,
            module=module,
            event_type=event_type,
            title=title,
            message=message,
            link=user_link,
            actor_user_id=actor_user_id,
            reference_id=reference_id,
            reference_type=reference_type,
        )
        notifications.append(notif)

    try:
        db.add_all(notifications)
        db.flush()
        logger.info(f"Created {len(notifications)} notifications: {module}:{event_type} → users {unique_ids}")
    except Exception as e:
        logger.error(f"Failed to create notifications: {e}", exc_info=True)
        # Don't raise — notifications should never block the main action


# --- Convenience Functions for Issuance ---

def notify_request_submitted(db: Session, approver_user_ids: List[int], request_serial: str,
                              requestor_name: str, actor_user_id: int, request_id: int):
    notify(
        db, approver_user_ids,
        module="ISSUANCE",
        event_type="REQUEST_SUBMITTED",
        title="New Issuance Request",
        message=f"{requestor_name} submitted issuance request {request_serial} for your approval.",
        link=f"/corporate-admin/issuance/requests?request_id={request_id}",
        actor_user_id=actor_user_id,
        reference_id=request_id,
        reference_type="IssuanceRequest",
    )


def notify_request_approved(db: Session, recipient_user_ids: List[int], request_serial: str,
                              step_number: int, approver_name: str, actor_user_id: int,
                              request_id: int, is_fully_approved: bool = False):
    if is_fully_approved:
        title = "Request Fully Approved"
        message = f"Issuance request {request_serial} has been fully approved and is ready for execution."
    else:
        title = f"Approval Step {step_number} Complete"
        message = f"{approver_name} approved step {step_number} of request {request_serial}. Your approval is now required."

    notify(
        db, recipient_user_ids,
        module="ISSUANCE",
        event_type="REQUEST_FULLY_APPROVED" if is_fully_approved else "REQUEST_STEP_APPROVED",
        title=title,
        message=message,
        link=f"/corporate-admin/issuance/requests?request_id={request_id}",
        actor_user_id=actor_user_id,
        reference_id=request_id,
        reference_type="IssuanceRequest",
    )


def notify_request_rejected(db: Session, recipient_user_ids: List[int], request_serial: str,
                              rejector_name: str, reason: str, actor_user_id: int, request_id: int):
    notify(
        db, recipient_user_ids,
        module="ISSUANCE",
        event_type="REQUEST_REJECTED",
        title="Request Rejected",
        message=f"{rejector_name} rejected issuance request {request_serial}. Reason: {reason}",
        link=f"/corporate-admin/issuance/requests?request_id={request_id}",
        actor_user_id=actor_user_id,
        reference_id=request_id,
        reference_type="IssuanceRequest",
    )


def notify_lg_issued(db: Session, recipient_user_ids: List[int], request_serial: str,
                      bank_name: str, actor_user_id: int, request_id: int):
    notify(
        db, recipient_user_ids,
        module="ISSUANCE",
        event_type="LG_ISSUED",
        title="LG Issued to Bank",
        message=f"LG for request {request_serial} has been issued to {bank_name}.",
        link=f"/corporate-admin/issuance/issued-lgs",
        actor_user_id=actor_user_id,
        reference_id=request_id,
        reference_type="IssuanceRequest",
    )
