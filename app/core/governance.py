# app/core/governance.py
"""
Shared dual-control governance utility.

Provides the `create_governed_change` function for enforcing the "no single
admin acts alone" rule across all modules.  Change-type-specific application
logic is supplied by the caller via the `apply_fn` callback so that this
module stays module-agnostic.
"""
from sqlalchemy.orm import Session
from datetime import datetime as _dt

from app.models.models_issuance import AdminChangeRequest
from app.models.models import User
from app.constants import UserRole


def create_governed_change(
    db: Session,
    customer_id: int,
    user_id: int,
    change_type: str,
    change_payload: dict,
    apply_fn=None,
) -> tuple:
    """
    Create an AdminChangeRequest for dual-control.

    If only 1 corp-admin exists the change is auto-approved and the
    optional ``apply_fn(db, change_req)`` callback is invoked immediately.
    Otherwise the change stays PENDING until a *different* corp-admin
    approves it.

    Returns ``(change_req, auto_approved: bool)``.
    """

    new_req = AdminChangeRequest(
        customer_id=customer_id,
        requested_by_user_id=user_id,
        change_type=change_type,
        change_payload=change_payload,
        status="PENDING",
    )

    # Single-admin exception: auto-approve
    corp_admin_count = (
        db.query(User)
        .filter(
            User.customer_id == customer_id,
            User.role == UserRole.CORPORATE_ADMIN,
            User.is_deleted == False,
        )
        .count()
    )

    auto_approved = corp_admin_count <= 1
    if auto_approved:
        new_req.status = "APPROVED"
        new_req.approved_by_user_id = user_id
        new_req.applied_at = _dt.utcnow()

    db.add(new_req)
    db.flush()

    if auto_approved and apply_fn is not None:
        apply_fn(db, new_req)

    db.commit()
    db.refresh(new_req)
    return new_req, auto_approved
