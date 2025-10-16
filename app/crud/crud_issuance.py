# app/crud/crud_issuance.py

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
import secrets
from typing import List, Optional, Dict, Any

from app.models import IssuanceRequestToken, FixedApprover, LGIssuanceRequest, User
from app.schemas.all_schemas import LGIssuanceRequestCreate, LGIssuanceRequestUpdate
from app.constants import LGIssuanceRequestStatusMVP
from app.crud.crud import log_action

def create_issuance_request_token(db: Session, created_by_user_id: int, requester_email: str, expiry_hours: int) -> IssuanceRequestToken:
    """Creates a secure token for a magic link, saves it, and returns the token object."""
    token_value = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(hours=expiry_hours)
    
    db_token = IssuanceRequestToken(
        token=token_value,
        created_by_user_id=created_by_user_id,
        requester_email=requester_email,
        expires_at=expires_at,
        is_used=False
    )
    db.add(db_token)
    db.commit()
    db.refresh(db_token)
    return db_token

def validate_issuance_request_token(db: Session, token: str) -> Optional[IssuanceRequestToken]:
    """Validates a token. Returns the token object if valid, otherwise None."""
    db_token = db.query(IssuanceRequestToken).filter(IssuanceRequestToken.token == token).first()
    if not db_token or db_token.is_used or db_token.expires_at < datetime.utcnow():
        return None
    return db_token

def create_lg_issuance_request(db: Session, obj_in: LGIssuanceRequestCreate, customer_id: int) -> LGIssuanceRequest:
    """Creates the initial LG Issuance Request from a public form submission."""
    db_request = LGIssuanceRequest(
        customer_id=customer_id,
        status=LGIssuanceRequestStatusMVP.AWAITING_INITIAL_APPROVAL,
        requester_name=obj_in.requester_name,
        requester_email=obj_in.requester_email,
        beneficiary_name=obj_in.beneficiary_name,
        lg_amount=obj_in.lg_amount,
        lg_currency_id=obj_in.lg_currency_id,
        expiry_date=obj_in.expiry_date,
        purpose=obj_in.purpose,
        current_approver_email=obj_in.initial_approver_email,
        approval_history=[{
            "action": "SUBMITTED",
            "actor_email": obj_in.requester_email,
            "timestamp": datetime.utcnow().isoformat(),
            "notes": "Initial request submitted."
        }]
    )
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request

def get_fixed_approvers(db: Session, customer_id: int) -> List[str]:
    """Gets the list of fixed approver emails for a customer."""
    approvers = db.query(FixedApprover).filter(FixedApprover.customer_id == customer_id, FixedApprover.is_deleted == False).all()
    return [approver.approver_email for approver in approvers]

def get_issuance_request(db: Session, request_id: int, customer_id: int) -> Optional[LGIssuanceRequest]:
    """Gets a single issuance request for a specific customer."""
    return db.query(LGIssuanceRequest).filter(
        LGIssuanceRequest.id == request_id,
        LGIssuanceRequest.customer_id == customer_id
    ).first()

def get_all_issuance_requests(db: Session, customer_id: int) -> List[LGIssuanceRequest]:
    """Gets all issuance requests for a customer."""
    return db.query(LGIssuanceRequest).filter(LGIssuanceRequest.customer_id == customer_id).order_by(LGIssuanceRequest.created_at.desc()).all()

def update_issuance_request(db: Session, db_request: LGIssuanceRequest, update_data: LGIssuanceRequestUpdate) -> LGIssuanceRequest:
    """Updates an issuance request with new data."""
    update_dict = update_data.model_dump(exclude_unset=True)
    for key, value in update_dict.items():
        setattr(db_request, key, value)
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request

def add_ad_hoc_approver_to_request(db: Session, db_request: LGIssuanceRequest, new_approver_email: str, admin_user: User) -> LGIssuanceRequest:
    """Injects an ad-hoc approver into the workflow."""
    if not db_request.approval_history:
        db_request.approval_history = []
    
    # Store the current state to resume later
    original_next_approver = db_request.current_approver_email
    original_status = db_request.status

    db_request.approval_history.append({
        "action": "AD_HOC_APPROVAL_ADDED",
        "actor_email": admin_user.email,
        "timestamp": datetime.utcnow().isoformat(),
        "notes": f"Ad-hoc approval step added for {new_approver_email}. Original next approver was {original_next_approver}.",
        "resumption_state": {
            "status": original_status.value,
            "next_approver": original_next_approver
        }
    })

    db_request.status = LGIssuanceRequestStatusMVP.PENDING_AD_HOC_APPROVAL
    db_request.current_approver_email = new_approver_email

    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request
