# app/api/v1/endpoints/issuance.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from app.database import get_db
from app.core.security import TokenData, HasPermission, get_current_corporate_admin_context

router = APIRouter(prefix="/issuance", tags=["Issuance Management"])


@router.post("/requests/generate-link")
def generate_issuance_request_link(
    requester_email: str,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("issuance_request:initiate"))
):
    """
    (For Treasury/Corporate Admin)
    Generates a secure, timed magic link and emails it to an internal employee to start an LG request.
    """
    # 1. Create a record in the IssuanceRequestToken table.
    # 2. Get link expiry from customer/global config.
    # 3. Send an email to `requester_email` with the generated link.
    raise HTTPException(status_code=501, detail="Not Implemented")


@router.get("/requests/")
def list_all_issuance_requests(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("issuance_request:view_all"))
):
    """
    (For Corporate Admin)
    Dashboard to view all issuance requests, their current status, and who they are pending with.
    """
    # Query LGIssuanceRequest table for the customer and return the list.
    raise HTTPException(status_code=501, detail="Not Implemented")


@router.post("/requests/{request_id}/add-approver")
def add_ad_hoc_approver(
    request_id: int,
    approver_email: str,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("issuance_request:manage"))
):
    """
    (For Corporate Admin)
    Injects an additional, ad-hoc approver into the current workflow.
    """
    # 1. Find the LGIssuanceRequest.
    # 2. Store the current approver.
    # 3. Set the current_approver_email to the new ad-hoc approver.
    # 4. Log this action in the approval_history.
    # 5. Send notification to the new ad-hoc approver.
    # The approval processing logic will handle resuming the original flow.
    raise HTTPException(status_code=501, detail="Not Implemented")
