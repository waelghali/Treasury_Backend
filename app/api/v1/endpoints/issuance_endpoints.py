from typing import List, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import date

from app.database import get_db
# CORRECT IMPORT: Use app.core.security
from app.core.security import get_current_active_user, get_current_corporate_admin_context, TokenData

from app.crud.crud_issuance import crud_issuance_facility, crud_issuance_request
from app.schemas.schemas_issuance import (
    IssuanceRequestCreate, IssuanceRequestOut, IssuanceRequestUpdate,
    IssuanceFacilityCreate, IssuanceFacilityOut, SuitableFacilityOut, IssuanceRequestContentUpdate, IssuanceFacilityUpdate,
    IssuedLGRecordOut
)
from app.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceFacilitySubLimit, IssuanceFacility
from fastapi.responses import StreamingResponse
import io
from app.core.document_generator import generate_pdf_from_html
from app.core.encryption import encrypt_data 
from app.schemas.all_schemas import BankOut, CurrencyOut 
from app.models import Bank, Currency 

router = APIRouter()

# ==============================================================================
# 1. DICTIONARIES (Banks & Currencies)
# ==============================================================================

@router.get("/banks", response_model=List[BankOut])
def get_issuance_banks(db: Session = Depends(get_db)):
    """Fetch all banks for dropdowns."""
    return db.query(Bank).all()

@router.get("/currencies", response_model=List[CurrencyOut])
def get_issuance_currencies(db: Session = Depends(get_db)):
    """Fetch all currencies for dropdowns."""
    return db.query(Currency).all()

# ==============================================================================
# 2. FACILITIES MANAGEMENT
# ==============================================================================

@router.get("/facilities/", response_model=List[IssuanceFacilityOut])
def get_facilities(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    return crud_issuance_facility.get_multi_by_customer(db, customer_id=current_user.customer_id)

@router.post("/facilities/", response_model=IssuanceFacilityOut)
def create_facility(
    facility_in: IssuanceFacilityCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    return crud_issuance_facility.create_with_limits(db, obj_in=facility_in, customer_id=current_user.customer_id)

# ==============================================================================
# 3. REQUESTS MANAGEMENT
# ==============================================================================

@router.post("/requests/", response_model=IssuanceRequestOut)
def create_issuance_request_internal(
    request_in: IssuanceRequestCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Internal Endpoint: Allows the Admin to manually create a request 
    (bypassing the public portal).
    """
    # We pass the current_user.user_id because this is an internal creation
    return crud_issuance_request.create_request(
        db, 
        obj_in=request_in, 
        customer_id=current_user.customer_id, 
        user_id=current_user.user_id 
    )

@router.get("/requests/", response_model=List[IssuanceRequestOut])
def get_issuance_requests(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    return crud_issuance_request.get_by_customer(db, customer_id=current_user.customer_id)

@router.get("/requests/{request_id}/suitable-facilities", response_model=List[SuitableFacilityOut])
def get_suitable_facilities(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Finds facilities that match the currency.
    CALCULATES UTILIZATION: Limit Amount - Sum(Active LGs).
    """
    req = crud_issuance_request.get(db, id=request_id)
    if not req: raise HTTPException(404, "Request not found")
    
    # Get all facilities for this customer
    facilities = crud_issuance_facility.get_multi_by_customer(db, customer_id=current_user.customer_id)
    
    suitable = []
    
    for fac in facilities:
        # Filter 1: Must match Currency and be Active
        if fac.currency_id == req.currency_id and fac.is_active:
            
            # Loop through Sub-Limits (e.g., "Bid Bond Line")
            for sl in fac.sub_limits:
                
                # --- THE MISSING MATH ---
                # Query: Sum of all 'current_amount' where sub_limit_id matches 
                # and status is 'ACTIVE'
                used_amount = db.query(func.sum(IssuedLGRecord.current_amount))\
                    .filter(IssuedLGRecord.facility_sub_limit_id == sl.id)\
                    .filter(IssuedLGRecord.status == 'ACTIVE')\
                    .scalar() or 0.0 # scalar() returns None if no records, so default to 0.0
                
                available = float(sl.limit_amount) - float(used_amount)
                
                # Only show if there is room (or show generic logic)
                suitable.append({
                    "facility_id": fac.id,
                    "facility_bank": fac.bank.name if fac.bank else "Unknown Bank",
                    "sub_limit_id": sl.id,
                    "sub_limit_name": sl.limit_name,
                    "limit_available": available, # NOW DYNAMIC
                    "price_commission": sl.default_commission_rate or 0.0
                })
                
    return suitable

@router.post("/requests/{request_id}/execute")
def execute_issuance_request(
    request_id: int,
    sub_limit_id: int,
    issued_ref_number: str,
    issue_date: str, 
    expiry_date: str = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Revised Execution Bridge with INTELLIGENCE (Guardrails).
    Checks if limit is available before executing.
    """
    # 1. Fetch Request
    req = db.query(IssuanceRequest).filter(IssuanceRequest.id == request_id).first()
    if not req: raise HTTPException(404, "Request not found")
    
    if req.status == "ISSUED":
        raise HTTPException(400, "Request already executed")

    # 2. Fetch the Sub-Limit to check money
    sub_limit = db.query(IssuanceFacilitySubLimit).filter(IssuanceFacilitySubLimit.id == sub_limit_id).first()
    if not sub_limit:
        raise HTTPException(404, "Selected Facility Sub-Limit not found")

    # 3. GUARDRAIL: Calculate Utilization
    # (Reuse the logic from suitable_facilities to be safe)
    used_amount = db.query(func.sum(IssuedLGRecord.current_amount))\
        .filter(IssuedLGRecord.facility_sub_limit_id == sub_limit_id)\
        .filter(IssuedLGRecord.status == 'ACTIVE')\
        .scalar() or 0.0
    
    available_amount = float(sub_limit.limit_amount) - float(used_amount)

    # 4. The Block
    if req.amount > available_amount:
        raise HTTPException(
            status_code=400, 
            detail=f"Insufficient Limits. Available: {available_amount:,.2f}, Requested: {req.amount:,.2f}"
        )

    # 5. Proceed if Safe
    new_record = IssuedLGRecord(
        lg_ref_number=issued_ref_number,
        customer_id=req.customer_id,
        facility_sub_limit_id=sub_limit_id,
        beneficiary_name=req.beneficiary_name,
        current_amount=req.amount,
        currency_id=req.currency_id,
        issue_date=issue_date,
        expiry_date=expiry_date, 
        status="ACTIVE"
    )
    
    try:
        db.add(new_record)
        db.flush() 
        
        req.lg_record_id = new_record.id
        req.status = "ISSUED" 
        db.add(req)
        
        db.commit()
        return {"message": "LG Executed Successfully", "lg_record_id": new_record.id}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(500, detail=f"Execution Failed: {str(e)}")

@router.get("/requests/{request_id}/print-form")
async def print_issuance_application_form(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    request = crud_issuance_request.get(db, id=request_id)
    if not request: raise HTTPException(404, "Request not found")

    html_content = f"""
    <html><body>
        <h1>LG Issuance Application</h1>
        <p><strong>Ref:</strong> {request.id}</p>
        <p><strong>Beneficiary:</strong> {request.beneficiary_name}</p>
        <p><strong>Amount:</strong> {request.amount}</p>
    </body></html>
    """
    pdf_bytes = await generate_pdf_from_html(html_content, f"application_{request.id}")
    if not pdf_bytes:
        raise HTTPException(500, "PDF generation failed")
        
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=application_{request.id}.pdf"}
    )

# ==============================================================================
# 4. SECURITY GENERATOR (NEW)
# ==============================================================================

@router.post("/generate-portal-link")
def generate_portal_link(
    department_name: str,
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Generates a safe, encrypted link for the Public Portal.
    Format: customer_id:department_name:timestamp
    """
    # Create the raw string
    raw_data = f"{current_user.customer_id}:{department_name}"
    
    # Encrypt it
    token = encrypt_data(raw_data)
    
    # Return the full URL (Adjust base URL as needed)
    return {
        "link": f"https://growbusinessdevelopment.com/public-issuance?token={token}",
        "token": token
    }

# ==============================================================================
# 5. EDIT & DELETE ENDPOINTS (INTEGRITY PHASE)
# ==============================================================================

@router.put("/facilities/{facility_id}", response_model=IssuanceFacilityOut)
def update_facility(
    facility_id: int,
    facility_in: IssuanceFacilityUpdate, # Imports from schemas_issuance
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Update Facility Limits or Status.
    """
    facility = crud_issuance_facility.get(db, id=facility_id)
    if not facility or facility.customer_id != current_user.customer_id:
        raise HTTPException(404, "Facility not found")
        
    # Use the standard CRUD update method
    facility = crud_issuance_facility.update(db, db_obj=facility, obj_in=facility_in)
    return facility

@router.delete("/facilities/{facility_id}")
def delete_facility(
    facility_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Soft-delete a facility (mark as inactive) so we don't lose historical data.
    """
    facility = crud_issuance_facility.get(db, id=facility_id)
    if not facility or facility.customer_id != current_user.customer_id:
        raise HTTPException(404, "Facility not found")
    
    # Soft Delete Logic
    facility.is_active = False
    db.add(facility)
    db.commit()
    return {"message": "Facility deactivated successfully"}

@router.put("/requests/{request_id}/details", response_model=IssuanceRequestOut)
def update_request_details(
    request_id: int,
    request_in: IssuanceRequestContentUpdate, # The new schema we defined above
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Allows Admin to fix typos in the request (Amount, Beneficiary, etc.) 
    BEFORE it is executed.
    """
    req = crud_issuance_request.get(db, id=request_id)
    if not req:
        raise HTTPException(404, "Request not found")
    
    # Block editing if already issued
    if req.status in ["ISSUED", "COMPLETED"]:
        raise HTTPException(400, "Cannot edit a request that has already been executed.")

    # Update fields manually or via CRUD
    update_data = request_in.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(req, field, value)

    db.add(req)
    db.commit()
    db.refresh(req)
    return req