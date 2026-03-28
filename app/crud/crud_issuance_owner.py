import logging
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from app.models.models_issuance import IssuedLGRecord, IssuanceRequest
from app.models.models import ApprovalRequest
from app.schemas.schemas_issuance import RequestorProfile
from app.constants import ApprovalRequestStatusEnum
from fastapi import HTTPException
from app.models.models_issuance import CustomerFormConfiguration

logger = logging.getLogger(__name__)

def _validate_requestor_profile(db: Session, customer_id: int, profile: RequestorProfile):
    """
    Validates the provided RequestorProfile against the customer's form configuration.
    Raises HTTPException if any mandatory fields are missing.
    """
    config = db.query(CustomerFormConfiguration).filter(
        CustomerFormConfiguration.customer_id == customer_id
    ).first()
    
    if not config or not config.field_configurations:
        return # No specific constraints
        
    fc = config.field_configurations
    errors = []
    
    for field_key, field_name in [
        ("name", "Name"),
        ("department", "Department"),
        ("job_title", "Job Title"),
        ("phone_number", "Phone Number"),
        ("employee_id", "Employee ID"),
        ("manager_email", "Manager Email"),
        ("second_line_manager_email", "Second Line Manager Email")
    ]:
        field_config = fc.get(field_key, {})
        if field_config.get("is_mandatory", False) and field_config.get("is_visible", True):
            val = getattr(profile, field_key, None)
            if not val or not str(val).strip():
                errors.append(field_name)
                
    if errors:
        raise HTTPException(status_code=400, detail=f"The following fields are mandatory based on your organization's configuration: {', '.join(errors)}")

def get_unique_requestors(db: Session, customer_id: int) -> List[RequestorProfile]:
    """
    Fetches a unique list of historical requestors for a given customer based on IssuanceRequests.
    Returns the most recent data for each requestor email.
    """
    # Find the max ID for each requestor_email to get their most recent profile
    subquery = db.query(
        IssuanceRequest.requestor_email,
        func.max(IssuanceRequest.id).label('max_id')
    ).filter(
        IssuanceRequest.customer_id == customer_id,
        IssuanceRequest.requestor_email.isnot(None)
    ).group_by(IssuanceRequest.requestor_email).subquery()

    recent_requests = db.query(IssuanceRequest).join(
        subquery,
        (IssuanceRequest.requestor_email == subquery.c.requestor_email) &
        (IssuanceRequest.id == subquery.c.max_id)
    ).all()

    profiles = []
    for req in recent_requests:
        profiles.append(RequestorProfile(
            email=req.requestor_email,
            name=req.requestor_name,
            department=req.department,
            job_title=req.job_title,
            phone_number=req.phone_number,
            employee_id=req.employee_id,
            manager_email=req.manager_email,
            second_line_manager_email=req.second_line_manager_email
        ))
    return profiles

def initiate_peer_handover(db: Session, customer_id: int, lg_id: int, new_profile: RequestorProfile, initiator_email: str) -> IssuedLGRecord:
    """
    Called by a Requestor to hand over an LG to another Requestor.
    """
    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == customer_id,
        IssuedLGRecord.status.not_in(["CANCELLED", "LIQUIDATED", "EXPIRED"])
    ).first()

    if not lg:
        raise HTTPException(status_code=404, detail="LG not found or cannot be modified.")

    _validate_requestor_profile(db, customer_id, new_profile)

    lg.pending_handover_data = new_profile.model_dump()
    lg.handover_state = "PENDING_ACCEPTANCE"
    lg.handover_initiated_by = "REQUESTOR"

    db.commit()
    db.refresh(lg)
    return lg

def resolve_peer_handover(db: Session, customer_id: int, lg_id: int, action: str, returning_email: str) -> IssuedLGRecord:
    """
    Called by the new Requestor (via OTP login) to ACCEPT or REJECT an incoming handover.
    """
    lg = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id == lg_id,
        IssuedLGRecord.customer_id == customer_id,
        IssuedLGRecord.handover_state == "PENDING_ACCEPTANCE"
    ).first()

    if not lg:
        raise HTTPException(status_code=404, detail="Pending handover not found.")

    pending_email = lg.pending_handover_data.get("email")
    if not pending_email or pending_email.lower() != returning_email.lower():
        raise HTTPException(status_code=403, detail="Not authorized to resolve this handover.")

    if action == "ACCEPT":
        # 1. We must find the originating Request to update its requestor data so it displays correctly
        # Alternatively, we just update the IssuanceRequest fields.
        req = db.query(IssuanceRequest).filter(IssuanceRequest.id == lg.request_id).first()
        if req:
            req.requestor_email = pending_email
            req.requestor_name = lg.pending_handover_data.get("name")
            req.department = lg.pending_handover_data.get("department")
            req.job_title = lg.pending_handover_data.get("job_title")
            req.phone_number = lg.pending_handover_data.get("phone_number")
            req.employee_id = lg.pending_handover_data.get("employee_id")
            req.manager_email = lg.pending_handover_data.get("manager_email")
            req.second_line_manager_email = lg.pending_handover_data.get("second_line_manager_email")
        
        # 2. Clear pending state
        lg.pending_handover_data = None
        lg.handover_state = None
        lg.handover_initiated_by = None
    elif action == "REJECT":
        lg.pending_handover_data = None
        lg.handover_state = None
        lg.handover_initiated_by = None
    else:
        raise HTTPException(status_code=400, detail="Invalid action.")

    db.commit()
    db.refresh(lg)
    return lg

def update_requestor_profile(db: Session, customer_id: int, old_email: str, updated_profile: RequestorProfile, update_all_lgs: bool = True):
    """
    Called by Corporate Admin to update requestor metadata (like department, job title).
    """
    _validate_requestor_profile(db, customer_id, updated_profile)
    
    requests = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == customer_id,
        func.lower(IssuanceRequest.requestor_email) == old_email.lower()
    ).all()

    for req in requests:
        req.requestor_email = updated_profile.email
        if updated_profile.name: req.requestor_name = updated_profile.name
        if updated_profile.department: req.department = updated_profile.department
        if updated_profile.job_title: req.job_title = updated_profile.job_title
        if updated_profile.phone_number: req.phone_number = updated_profile.phone_number
        if updated_profile.employee_id: req.employee_id = updated_profile.employee_id
        if updated_profile.manager_email: req.manager_email = updated_profile.manager_email
        if updated_profile.second_line_manager_email: req.second_line_manager_email = updated_profile.second_line_manager_email

    db.commit()
    return True

def execute_force_handover(db: Session, customer_id: int, lg_ids: List[int], new_profile: RequestorProfile, user_id: int) -> List[IssuedLGRecord]:
    """
    Called by Corporate Admin to instantly transfer ownership (subject to Maker-Checker at API level).
    """
    from app.crud.crud import log_action

    _validate_requestor_profile(db, customer_id, new_profile)

    lgs = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.id.in_(lg_ids),
        IssuedLGRecord.customer_id == customer_id,
        IssuedLGRecord.status.not_in(["CANCELLED", "LIQUIDATED", "EXPIRED"])
    ).all()

    for lg in lgs:
        req = db.query(IssuanceRequest).filter(IssuanceRequest.id == lg.request_id).first()
        if req:
            req.requestor_email = new_profile.email
            if new_profile.name: req.requestor_name = new_profile.name
            if new_profile.department: req.department = new_profile.department
            if new_profile.job_title: req.job_title = new_profile.job_title
            if new_profile.phone_number: req.phone_number = new_profile.phone_number
            if new_profile.employee_id: req.employee_id = new_profile.employee_id
            if new_profile.manager_email: req.manager_email = new_profile.manager_email
            if new_profile.second_line_manager_email: req.second_line_manager_email = new_profile.second_line_manager_email

        lg.pending_handover_data = None
        lg.handover_state = None
        lg.handover_initiated_by = None

        log_action(
            db,
            user_id=user_id,
            action_type="ISSUANCE_OWNER_CHANGED",
            entity_type="IssuedLGRecord",
            entity_id=lg.id,
            details={
                "lg_ref_number": lg.lg_ref_number,
                "new_owner_email": new_profile.email,
                "reason": "Admin Force Handover"
            },
            customer_id=customer_id,
            lg_record_id=lg.id,
        )

    db.commit()
    return lgs

