from typing import List, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import date
import io

from app.database import get_db
from app.core.security import get_current_corporate_admin_context, TokenData
from app.core.document_generator import generate_pdf_from_html
from app.core.encryption import encrypt_data 

# Models
from app.models import Bank, Currency 
from app.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceFacilitySubLimit, IssuanceFacility, IssuanceWorkflowPolicy
# NOTE: Ensure you created app/models_reconciliation.py first!
from app.models_reconciliation import BankPositionBatch, BankPositionRow 

# Schemas
from app.schemas.all_schemas import BankOut, CurrencyOut 
from app.schemas.schemas_issuance import (
    IssuanceRequestCreate, IssuanceRequestOut, IssuanceRequestUpdate,
    IssuanceFacilityCreate, IssuanceFacilityOut, SuitableFacilityOut, 
    IssuanceRequestContentUpdate, IssuanceFacilityUpdate,
    IssuedLGRecordOut,
    ReconciliationRequest, ReconciliationResult,
    IssuanceWorkflowPolicyCreate, IssuanceWorkflowPolicyOut
)
from app.services.issuance_service import issuance_service

# CRUD
from app.crud.crud_issuance import crud_issuance_facility, crud_issuance_request

from fastapi.responses import StreamingResponse

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

@router.put("/facilities/{facility_id}", response_model=IssuanceFacilityOut)
def update_facility(
    facility_id: int,
    facility_in: IssuanceFacilityUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    facility = crud_issuance_facility.get(db, id=facility_id)
    if not facility or facility.customer_id != current_user.customer_id:
        raise HTTPException(404, "Facility not found")
        
    return crud_issuance_facility.update(db, db_obj=facility, obj_in=facility_in)

@router.delete("/facilities/{facility_id}")
def delete_facility(
    facility_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    facility = crud_issuance_facility.get(db, id=facility_id)
    if not facility or facility.customer_id != current_user.customer_id:
        raise HTTPException(404, "Facility not found")
    
    facility.is_active = False
    db.add(facility)
    db.commit()
    return {"message": "Facility deactivated successfully"}

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
    Internal Endpoint: Allows the Admin to manually create a request.
    """
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

@router.put("/requests/{request_id}/details", response_model=IssuanceRequestOut)
def update_request_details(
    request_id: int,
    request_in: IssuanceRequestContentUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    req = crud_issuance_request.get(db, id=request_id)
    if not req:
        raise HTTPException(404, "Request not found")
    
    if req.status in ["ISSUED", "COMPLETED"]:
        raise HTTPException(400, "Cannot edit a request that has already been executed.")

    update_data = request_in.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(req, field, value)

    db.add(req)
    db.commit()
    db.refresh(req)
    return req

# ==============================================================================
# 4. INTELLIGENT DECISION SUPPORT
# ==============================================================================

@router.get("/requests/{request_id}/suitable-facilities", response_model=List[SuitableFacilityOut])
def get_suitable_facilities(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Returns facilities with:
    1. Utilization Check (Math)
    2. Cost Calculation (Commissions & Margins)
    3. Recommendations (Tags)
    """
    req = crud_issuance_request.get(db, id=request_id)
    if not req: 
        raise HTTPException(404, "Request not found")
    
    facilities = crud_issuance_facility.get_multi_by_customer(db, customer_id=current_user.customer_id)
    suitable = []
    
    for fac in facilities:
        if fac.currency_id == req.currency_id and fac.is_active:
            for sl in fac.sub_limits:
                
                # 1. Math: Utilization
                used_amount = db.query(func.sum(IssuedLGRecord.current_amount))\
                    .filter(IssuedLGRecord.facility_sub_limit_id == sl.id)\
                    .filter(IssuedLGRecord.status == 'ACTIVE')\
                    .scalar() or 0.0
                
                available = float(sl.limit_amount) - float(used_amount)
                
                # 2. Math: Costs
                comm_rate = sl.default_commission_rate or 0.0
                margin_pct = sl.default_cash_margin_pct or 0.0
                
                estimated_comm = float(req.amount) * (comm_rate / 100.0)
                required_margin = float(req.amount) * (margin_pct / 100.0)
                
                # 3. Logic: Tags
                tags = []
                if margin_pct == 0:
                    tags.append("NO_MARGIN")
                
                if available >= float(req.amount):
                    suitable.append({
                        "facility_id": fac.id,
                        "facility_bank": fac.bank.name if fac.bank else "Unknown Bank",
                        "sub_limit_id": sl.id,
                        "sub_limit_name": sl.limit_name,
                        "limit_available": available,
                        
                        "price_commission_rate": comm_rate,
                        "price_cash_margin_pct": margin_pct,
                        "estimated_commission_cost": estimated_comm,
                        "required_cash_margin_amount": required_margin,
                        "recommendation_tags": tags
                    })
                
    # 4. Sorting Intelligence: Cheapest Margin first, then Cheapest Commission
    suitable.sort(key=lambda x: (x['required_cash_margin_amount'], x['estimated_commission_cost']))
    
    # Tag the top result as BEST_OPTION
    if suitable:
        suitable[0]['recommendation_tags'].insert(0, "BEST_OPTION")
                
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
    """
    req = db.query(IssuanceRequest).filter(IssuanceRequest.id == request_id).first()
    if not req: raise HTTPException(404, "Request not found")
    
    if req.status == "ISSUED":
        raise HTTPException(400, "Request already executed")

    sub_limit = db.query(IssuanceFacilitySubLimit).filter(IssuanceFacilitySubLimit.id == sub_limit_id).first()
    if not sub_limit:
        raise HTTPException(404, "Selected Facility Sub-Limit not found")

    used_amount = db.query(func.sum(IssuedLGRecord.current_amount))\
        .filter(IssuedLGRecord.facility_sub_limit_id == sub_limit_id)\
        .filter(IssuedLGRecord.status == 'ACTIVE')\
        .scalar() or 0.0
    
    available_amount = float(sub_limit.limit_amount) - float(used_amount)

    if req.amount > available_amount:
        raise HTTPException(
            status_code=400, 
            detail=f"Insufficient Limits. Available: {available_amount:,.2f}, Requested: {req.amount:,.2f}"
        )

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

# ==============================================================================
# 5. UTILITIES (PDF, SECURITY)
# ==============================================================================

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
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=application_{request.id}.pdf"}
    )

@router.post("/generate-portal-link")
def generate_portal_link(
    department_name: str,
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Generates a safe, encrypted link for the Public Portal.
    """
    raw_data = f"{current_user.customer_id}:{department_name}"
    token = encrypt_data(raw_data)
    
    return {
        "link": f"http://localhost:3000/public-issuance?token={token}",
        "token": token
    }

# ==============================================================================
# 6. RECONCILIATION ENGINE (NEW)
# ==============================================================================

@router.post("/reconciliation/run", response_model=ReconciliationResult)
def run_bank_position_reconciliation(
    recon_data: ReconciliationRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Matches Excel rows against DB and SAVES a historical batch.
    """
    discrepancies = []
    matched = 0
    mismatched_amount = 0
    missing_system = 0
    
    # A. FETCH SYSTEM RECORDS
    bank_facilities = db.query(IssuanceFacility).filter(
        IssuanceFacility.bank_id == recon_data.bank_id, 
        IssuanceFacility.customer_id == current_user.customer_id
    ).all()
    
    fac_ids = [f.id for f in bank_facilities]
    
    system_records = db.query(IssuedLGRecord)\
        .join(IssuanceFacilitySubLimit)\
        .filter(IssuanceFacilitySubLimit.facility_id.in_(fac_ids))\
        .filter(IssuedLGRecord.status == 'ACTIVE')\
        .all()
    
    system_map = {rec.lg_ref_number: rec for rec in system_records}
    bank_refs_processed = set()

    # B. CREATE DB BATCH (History)
    # Using 'models_reconciliation.py' models
    batch = BankPositionBatch(
        bank_id=recon_data.bank_id,
        as_of_date=recon_data.as_of_date,
        uploaded_by_user_id=current_user.user_id
    )
    db.add(batch)
    db.flush() # Get ID
    
    # C. ITERATE & COMPARE
    for row in recon_data.rows:
        bank_refs_processed.add(row.ref_number)
        
        # Save raw row to DB
        db_row = BankPositionRow(
            batch_id=batch.id,
            ref_number=row.ref_number,
            amount=row.amount,
            currency_code=row.currency,
            status_in_bank=row.status
        )
        
        if row.ref_number in system_map:
            sys_rec = system_map[row.ref_number]
            if abs(float(sys_rec.current_amount) - row.amount) > 1.0:
                mismatched_amount += 1
                db_row.recon_status = "MISMATCH"
                db_row.recon_note = f"Amount mismatch: Bank={row.amount}, Sys={sys_rec.current_amount}"
                discrepancies.append({
                    "type": "AMOUNT_MISMATCH",
                    "ref": row.ref_number,
                    "bank_amount": row.amount,
                    "system_amount": float(sys_rec.current_amount),
                    "diff": row.amount - float(sys_rec.current_amount)
                })
            else:
                matched += 1
                db_row.recon_status = "MATCHED"
        else:
            missing_system += 1
            db_row.recon_status = "MISSING_IN_SYSTEM"
            db_row.recon_note = "Found in bank, missing in system"
            discrepancies.append({
                "type": "MISSING_IN_SYSTEM",
                "ref": row.ref_number,
                "bank_amount": row.amount,
                "note": "Bank has this LG, but we do not."
            })
            
        db.add(db_row)
            
    # D. REVERSE CHECK
    for sys_ref, sys_rec in system_map.items():
        if sys_ref not in bank_refs_processed:
            discrepancies.append({
                "type": "MISSING_IN_BANK",
                "ref": sys_ref,
                "system_amount": float(sys_rec.current_amount),
                "note": "We show this as ACTIVE, but it is not in Bank Position."
            })

    # E. COMMIT
    batch.total_records = len(recon_data.rows)
    batch.matched_records = matched
    db.commit()

    return {
        "total_bank_records": len(recon_data.rows),
        "matched_count": matched,
        "mismatched_amount_count": mismatched_amount,
        "missing_in_system_count": missing_system,
        "discrepancies": discrepancies
    }

# ==============================================================================
# 5. WORKFLOW CONFIGURATION (Corporate Admin Only)
# ==============================================================================

@router.post("/workflow-policies", response_model=IssuanceWorkflowPolicyOut)
def create_workflow_policy(
    policy_in: IssuanceWorkflowPolicyCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """ Define an approval rule (e.g., Amount > 50k requires 'MANAGER'). """
    # Simple CRUD creation
    db_obj = IssuanceWorkflowPolicy(
        customer_id=current_user.customer_id,
        **policy_in.dict()
    )
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    return db_obj

@router.get("/workflow-policies", response_model=List[IssuanceWorkflowPolicyOut])
def list_workflow_policies(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """ List all active approval rules for this customer. """
    return db.query(IssuanceWorkflowPolicy).filter(
        IssuanceWorkflowPolicy.customer_id == current_user.customer_id
    ).order_by(IssuanceWorkflowPolicy.step_sequence.asc()).all()

# ==============================================================================
# 6. APPROVAL ACTIONS
# ==============================================================================

@router.post("/requests/{request_id}/submit", response_model=IssuanceRequestOut)
def submit_request_for_approval(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context) # Or EndUser
):
    """ User submits DRAFT -> PENDING_APPROVAL """
    # TODO: Add logic to verify current_user owns the request or is allowed to submit
    return issuance_service.submit_for_approval(db, request_id, current_user.user_id)

@router.post("/requests/{request_id}/approve", response_model=IssuanceRequestOut)
def approve_request_action(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """ 
    Approver (Manager) approves the request. 
    Moves to Next Step OR 'APPROVED_INTERNAL'.
    """
    # Security Check: In a real app, check if current_user.role == request.pending_approver_role
    return issuance_service.approve_request(db, request_id, current_user.user_id)

@router.post("/requests/{request_id}/reject", response_model=IssuanceRequestOut)
def reject_request_action(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """ Approver rejects the request. """
    return issuance_service.reject_request(db, request_id, current_user.user_id)

# ==============================================================================
# 7. SMART ISSUANCE SUPPORT
# ==============================================================================

@router.get("/requests/{request_id}/recommendations", response_model=List[SuitableFacilityOut])
def get_facility_recommendations(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Returns a list of facilities that CAN issue this LG, sorted by SLA.
    """
    return issuance_service.get_suitable_facilities(db, request_id)

from fastapi.responses import StreamingResponse
import io

@router.post("/requests/{request_id}/execute/{facility_id}")
async def execute_issuance(
    request_id: int,
    facility_id: int,
    method_id: int = Query(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    # Call Service
    result_data = await issuance_service.issue_lg_from_request(
        db, request_id, facility_id, method_id, current_user.user_id
    )
    
    execution_result = result_data["execution_result"]
    
    # If the strategy produced a File (Bytes), stream it to user
    if execution_result.get("output_type") == "BYTES":
        pdf_bytes = execution_result["output_data"]
        filename = execution_result.get("filename", "document.pdf")
        
        # Return as File Download
        return StreamingResponse(
            io.BytesIO(pdf_bytes), 
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    # Otherwise (e.g. API-based issuance), return JSON
    return {
        "message": execution_result.get("message"), 
        "lg_record_id": result_data["lg_record"].id
    }