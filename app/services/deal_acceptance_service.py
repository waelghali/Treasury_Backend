import logging
from datetime import datetime, timezone
from typing import Optional, List, Any
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.models import User
from app.models.models_quotation import QuotationRequest
from app.crud.crud import log_action

logger = logging.getLogger(__name__)


def check_deal_authority(rfq: QuotationRequest, user: User, action_name: str = "accept"):
    """
    Validates whether the given user has authority to accept, decline, or delegate the RFQ.
    Authorized users are:
    1. Corporate Admin / Super Admin
    2. Maker (creator of the RFQ)
    3. Designated Delegate (assigned by Maker or Corporate Admin)
    """
    user_role = user.role.value if hasattr(user.role, 'value') else str(user.role)
    is_admin = user_role in ["corporate_admin", "super_admin"]
    is_maker = (user.id == rfq.created_by_user_id)
    is_delegate = (rfq.delegated_to_user_id is not None and user.id == rfq.delegated_to_user_id)

    if action_name == "delegate":
        if not (is_admin or is_maker):
            raise HTTPException(
                status_code=403,
                detail="Only the Maker (creator) or a Corporate Admin can delegate deal acceptance authority."
            )
        return True

    if not (is_admin or is_maker or is_delegate):
        raise HTTPException(
            status_code=403,
            detail=f"You are not authorized to {action_name} this deal. Only the Maker, a Corporate Admin, or the designated Delegate can execute this action."
        )
    return True


def execute_deal_delegation(
    rfq_id: str,
    delegator_user_id: int,
    delegatee_user_id: int,
    db: Session
) -> dict:
    """
    Delegates acceptance/rejection authority to another authorized corporate user.
    """
    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="Quotation request not found.")

    delegator = db.query(User).filter(User.id == delegator_user_id).first()
    if not delegator:
        raise HTTPException(status_code=401, detail="Delegating user not found.")

    check_deal_authority(rfq, delegator, action_name="delegate")

    delegatee = db.query(User).filter(
        User.id == delegatee_user_id,
        User.customer_id == rfq.customer_id,
        User.is_deleted == False
    ).first()
    if not delegatee:
        raise HTTPException(
            status_code=404,
            detail="Designated delegate user not found within your corporate organization."
        )

    now = datetime.now(timezone.utc)
    rfq.delegated_to_user_id = delegatee.id
    rfq.delegated_by_user_id = delegator.id
    rfq.delegated_at = now
    first_n = getattr(delegatee, 'first_name', '') or ''
    last_n = getattr(delegatee, 'last_name', '') or ''
    delegatee_display = f"{first_n} {last_n}".strip() or delegatee.email

    log_action(
        db,
        user_id=delegator.id,
        action_type="DEAL_ACCEPTANCE_DELEGATED",
        entity_type="QuotationRequest",
        entity_id=None,
        details={
            "rfq_id": str(rfq.id),
            "ref_no": rfq.ref_no,
            "delegated_to_user_id": delegatee.id,
            "delegated_to_name": delegatee_display,
            "delegated_by_user_id": delegator.id
        },
        customer_id=rfq.customer_id
    )
    db.commit()

    return {
        "status": "success",
        "message": f"Deal acceptance authority successfully delegated to {delegatee_display}.",
        "delegated_to_user_id": delegatee.id,
        "delegated_to_name": delegatee_display,
        "delegated_at": now.isoformat()
    }


async def execute_shared_deal_acceptance(
    rfq_id: str,
    user_id: int,
    accepted_leg_ids: Optional[List[Any]] = None,
    declined_leg_ids: Optional[List[Any]] = None,
    db: Session = None
) -> dict:
    """
    Executes binding acceptance of winning quotation.
    Supports Maker, Corporate Admin, and Delegated Colleague.
    Supports both Multi-Leg basket RFQs and Single-Leg RFQs.
    """
    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="Quotation not found.")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not authenticated.")

    check_deal_authority(rfq, user, action_name="accept")

    now_utc = datetime.now(timezone.utc)
    rfq.acceptance_resolved_at = now_utc
    rfq.acceptance_resolved_by_user_id = user.id

    if accepted_leg_ids is not None:
        accepted_set = {str(x) for x in accepted_leg_ids}
        declined_set = {str(x) for x in declined_leg_ids} if declined_leg_ids is not None else set()
        for leg in (rfq.legs or []):
            leg_key = str(leg.id)
            if leg_key in accepted_set:
                if leg.winner_bank_id:
                    leg.status = 'ACCEPTED'
                else:
                    leg.status = 'INCONCLUSIVE'
            elif leg_key in declined_set or (declined_leg_ids is not None and len(declined_set) > 0 and leg_key not in accepted_set):
                leg.status = 'REJECTED'
                leg.winner_bank_id = None
                leg.winner_bank_name = None
                leg.winner_rate = None
                leg.saved_vs_avg = None
                leg.rejection_reason = "Declined by authorized corporate user during deal acceptance"
            elif leg.winner_bank_id and leg.status not in ('REJECTED', 'CANCELLED'):
                leg.status = 'ACCEPTED'
            elif leg.status in ('PENDING', 'PENDING_APPROVAL', 'EVALUATING', 'APPROVED_SCHEDULED'):
                leg.status = 'INCONCLUSIVE' if not leg.winner_bank_id else 'COMPLETED'

        any_accepted = any(l.status == 'ACCEPTED' for l in (rfq.legs or []))
        if any_accepted:
            rfq.status = 'COMPLETED'
            rfq.acceptance_status = 'ACCEPTED'
        else:
            rfq.status = 'REJECTED'
            rfq.acceptance_status = 'REJECTED'
            rfq.admin_revision_notes = "All legs declined by authorized corporate user"
    else:
        rfq.status = 'COMPLETED'
        rfq.acceptance_status = 'ACCEPTED'
        for leg in (rfq.legs or []):
            if leg.winner_bank_id and leg.status not in ('REJECTED', 'CANCELLED'):
                leg.status = 'ACCEPTED'
            elif leg.status in ('PENDING', 'PENDING_APPROVAL', 'EVALUATING', 'APPROVED_SCHEDULED'):
                leg.status = 'INCONCLUSIVE' if not leg.winner_bank_id else 'COMPLETED'

    rfq.acceptance_resolved_at = datetime.now(timezone.utc)
    rfq.acceptance_resolved_by_user_id = user.id

    db.commit()

    log_action(
        db=db,
        user_id=user.id,
        action_type="QUOTATION_DEAL_ACCEPTED",
        entity_type="QuotationRequest",
        entity_id=None,
        details={
            "rfq_id": rfq.id,
            "ref_no": rfq.ref_no,
            "message": f"Deal accepted by authorized user {user.email} (Maker/Admin/Delegate) for RFQ {rfq.ref_no}."
        },
        customer_id=rfq.customer_id
    )

    try:
        from app.api.v1.endpoints.quotations_endpoints import dispatch_rfq_result_emails
        await dispatch_rfq_result_emails(rfq.id, db, force=False)
    except Exception as email_err:
        logger.warning(f"Result emails auto-dispatch encountered an issue for RFQ {rfq.id}: {email_err}")

    return {
        "status": "success",
        "message": f"Deal accepted successfully for RFQ {rfq.ref_no}! Trade execution confirmed and counterparties notified.",
        "rfq_id": rfq.id,
        "rfq_status": rfq.status,
        "acceptance_status": rfq.acceptance_status
    }


async def execute_shared_deal_decline(
    rfq_id: str,
    user_id: int,
    reason: Optional[str] = "Declined by corporate treasury desk",
    db: Session = None
) -> dict:
    """
    Manually declines/rejects the deal outcome.
    Supports Maker, Corporate Admin, and Delegated Colleague.
    """
    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="Quotation not found.")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not authenticated.")

    check_deal_authority(rfq, user, action_name="decline")

    now_utc = datetime.now(timezone.utc)
    rfq.status = 'REJECTED'
    rfq.acceptance_status = 'REJECTED'
    rfq.admin_revision_notes = reason
    rfq.acceptance_resolved_at = now_utc
    rfq.acceptance_resolved_by_user_id = user.id

    for leg in (rfq.legs or []):
        leg.status = 'REJECTED'
        leg.rejection_reason = reason

    db.commit()

    log_action(
        db=db,
        user_id=user.id,
        action_type="QUOTATION_DEAL_DECLINED",
        entity_type="QuotationRequest",
        entity_id=None,
        details={
            "rfq_id": rfq.id,
            "ref_no": rfq.ref_no,
            "reason": reason,
            "message": f"Deal declined by authorized user {user.email} (Maker/Admin/Delegate) for RFQ {rfq.ref_no}."
        },
        customer_id=rfq.customer_id
    )

    try:
        from app.api.v1.endpoints.quotations_endpoints import dispatch_rfq_result_emails
        await dispatch_rfq_result_emails(rfq.id, db, force=False)
    except Exception as email_err:
        logger.warning(f"Rejection result emails auto-dispatch encountered an issue for RFQ {rfq.id}: {email_err}")

    return {
        "status": "success",
        "message": f"Deal declined for RFQ {rfq.ref_no}. Participating counterparties notified.",
        "rfq_id": rfq.id,
        "rfq_status": rfq.status,
        "acceptance_status": rfq.acceptance_status
    }
