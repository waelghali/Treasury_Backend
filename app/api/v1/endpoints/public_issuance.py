# app/api/v1/endpoints/public_issuance.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_db

router = APIRouter(prefix="/public/issuance", tags=["Public Issuance"])

@router.get("/request/{token}")
def get_issuance_request_form(token: str, db: Session = Depends(get_db)):
    """
    Frontend will hit this endpoint. It validates the token.
    If valid, the frontend renders the request submission form.
    """
    # 1. Validate token from IssuanceRequestToken table
    # 2. If valid, return success. If invalid/expired, raise 404/403.
    # The actual form is rendered by the frontend framework.
    # This endpoint just serves as the secure entry point.
    raise HTTPException(status_code=501, detail="Not Implemented")


@router.post("/request/{token}")
def submit_issuance_request(token: str, db: Session = Depends(get_db)):
    """
    Receives the submitted form data from the magic link.
    """
    # 1. Validate token.
    # 2. Create LGIssuanceRequest with status AWAITING_INITIAL_APPROVAL.
    # 3. Store requester's details and the initial approver's email.
    # 4. Invalidate the token.
    # 5. Trigger an email to the initial approver with a new secure approval token.
    raise HTTPException(status_code=501, detail="Not Implemented")


@router.get("/approval/{approval_token}")
def get_approval_form(approval_token: str, db: Session = Depends(get_db)):
    """
    Approver clicks a link in their email, leading here.
    The system validates the token and shows the request details.
    """
    # 1. Validate the approval token.
    # 2. Fetch the LGIssuanceRequest and its details.
    # 3. Return the data for the frontend to render.
    raise HTTPException(status_code=501, detail="Not Implemented")


@router.post("/approval/{approval_token}")
def process_approval_action(approval_token: str, db: Session = Depends(get_db)):
    """
    Receives the Approve/Reject action from an approver.
    """
    # 1. Validate token and ensure the user is the current approver.
    # 2. Update approval_history in the LGIssuanceRequest.
    # 3. Determine the next approver (from FixedApprover list or if flow is complete).
    # 4. Update current_approver_email and send the next notification.
    # 5. If final approval, update status to APPROVED.
    raise HTTPException(status_code=501, detail="Not Implemented")
