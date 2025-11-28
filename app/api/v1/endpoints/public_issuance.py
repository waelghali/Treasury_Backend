# app/api/v1/endpoints/public_issuance.py

from typing import Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.database import get_db
from app.core.encryption import decrypt_data, encrypt_data # Reuse your existing encryption
from app.crud.crud_issuance import crud_issuance_request
from app.schemas.schemas_issuance import IssuanceRequestCreate, IssuanceRequestOut

router = APIRouter()

# --- SECURITY HELPER FOR PORTAL ---
def verify_portal_token(token: str) -> dict:
    """
    Decrypts and validates the portal access token.
    Token Format (Encrypted): "customer_id:department_name:expiry_timestamp"
    """
    try:
        decrypted_str = decrypt_data(token)
        parts = decrypted_str.split(":")
        
        if len(parts) < 2:
            raise ValueError("Invalid token format")
            
        customer_id = int(parts[0])
        department = parts[1]
        
        # Future: Add timestamp check here for expiry
        
        return {"customer_id": customer_id, "department": department}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Invalid or expired access token."
        )

# --- ENDPOINTS ---

@router.get("/validate-access")
def validate_portal_access(token: str = Query(...)):
    """
    Called by Frontend when loading the page to ensure the link is valid.
    Returns context (Company Name, Department) to display on the form.
    """
    data = verify_portal_token(token)
    return {
        "valid": True, 
        "department": data["department"],
        "message": "Access granted"
    }

@router.post("/submit", response_model=IssuanceRequestOut)
def public_submit_request(
    request_in: IssuanceRequestCreate,
    token: str = Query(...),
    db: Session = Depends(get_db)
):
    """
    The Public Endpoint for employees to submit LG requests.
    No User Login required. Validated by Token.
    """
    # 1. Verify Access
    access_data = verify_portal_token(token)
    customer_id = access_data["customer_id"]
    department = access_data["department"]

    # 2. Enrich Request Data
    # We auto-fill the department from the token so they can't fake it
    if request_in.business_details:
        request_in.business_details["department"] = department
    else:
        request_in.business_details = {"department": department}

    # 3. Create Request
    # Note: user_id is None because it's an employee
    request = crud_issuance_request.create_request(
        db, 
        obj_in=request_in, 
        customer_id=customer_id, 
        user_id=None 
    )
    
    return request