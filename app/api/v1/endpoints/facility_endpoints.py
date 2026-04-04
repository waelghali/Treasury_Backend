# app/endpoints/facility_endpoints.py
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.orm import Session
from app.database import get_db
from app.core.security import get_current_corporate_admin_context, TokenData
from app.schemas.schemas_issuance import IssuanceFacilityCreate, IssuanceFacilityOut, IssuanceFacilityUpdate
from app.crud.crud_facility import crud_facility
from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME

router = APIRouter()

@router.get("/", response_model=List[IssuanceFacilityOut])
def list_facilities(
    include_archived: bool = Query(False),
    db: Session = Depends(get_db), 
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """List all facilities for the logged-in customer, with computed utilization."""
    from app.models.models_issuance import IssuedLGRecord, IssuanceExposureEntry
    from sqlalchemy import func
    from decimal import Decimal

    facilities = crud_facility.get_multi_by_customer(
        db, 
        customer_id=current_user.customer_id, 
        include_deleted=include_archived
    )

    for fac in facilities:
        total_used = Decimal("0")
        total_reserved = Decimal("0")
        for sub in fac.sub_limits:
            # Active LG amounts
            used_amount = db.query(func.coalesce(func.sum(IssuedLGRecord.current_amount), 0)).filter(
                IssuedLGRecord.facility_sub_limit_id == sub.id,
                IssuedLGRecord.status.in_(["ACTIVE", "INTERNAL_PROCESSING"])
            ).scalar()
            # Pending exposure
            pending = db.query(func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0)).filter(
                IssuanceExposureEntry.sub_limit_id == sub.id,
                IssuanceExposureEntry.is_active == True
            ).scalar()
            sub_used = max(Decimal(str(used_amount)), Decimal(str(pending)))
            # Add initial utilization
            sub_used += Decimal(str(getattr(sub, 'initial_utilization', 0) or 0))
            total_used += sub_used

            # Reserved amount (RESERVATION entries only)
            reserved = db.query(func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0)).filter(
                IssuanceExposureEntry.sub_limit_id == sub.id,
                IssuanceExposureEntry.entry_type == "RESERVATION",
                IssuanceExposureEntry.is_active == True
            ).scalar()
            total_reserved += Decimal(str(reserved))

        fac.utilized_amount = total_used
        fac.reserved_amount = total_reserved

    return facilities

@router.post("/", response_model=IssuanceFacilityOut)
def create_new_facility(
    facility_in: IssuanceFacilityCreate, 
    db: Session = Depends(get_db), 
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Create a new bank facility with sub-limits."""
    return crud_facility.create_facility(
        db=db,                      # Name the db argument
        obj_in=facility_in, 
        customer_id=current_user.customer_id, 
        user_id=current_user.user_id
    )

@router.put("/{facility_id}", response_model=IssuanceFacilityOut)
def update_facility(
    facility_id: int,
    facility_in: IssuanceFacilityUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Update facility details and sync sub-limits."""
    facility = crud_facility.get(db, id=facility_id)
    if not facility or facility.customer_id != current_user.customer_id:
        raise HTTPException(404, "Facility not found")
    
    return crud_facility.update_facility(
        db, 
        db_obj=facility, 
        obj_in=facility_in, 
        user_id=current_user.user_id
    )

@router.delete("/{facility_id}", response_model=IssuanceFacilityOut)
def archive_facility(
    facility_id: int, 
    db: Session = Depends(get_db), 
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Soft-delete (Archive) a facility."""
    facility = crud_facility.get(db, id=facility_id)
    if not facility or facility.customer_id != current_user.customer_id:
        raise HTTPException(404, "Facility not found")
        
    return crud_facility.soft_delete(db, facility_id=facility_id, user_id=current_user.user_id)

@router.post("/{facility_id}/restore", response_model=IssuanceFacilityOut)
def restore_facility(
    facility_id: int, 
    db: Session = Depends(get_db), 
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Restore an archived facility to ACTIVE status."""
    facility = crud_facility.get(db, id=facility_id)
    if not facility or facility.customer_id != current_user.customer_id:
        raise HTTPException(404, "Facility not found")
        
    return crud_facility.restore(db, facility_id=facility_id, user_id=current_user.user_id)

@router.post("/upload-attachment")
async def upload_facility_attachment(
    file: UploadFile = File(...),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    # 1. Read the file content
    file_content = await file.read()
    
    # 2. Define the path using your system's existing pattern:
    # customer_{id}/Facilities/{filename}
    blob_path = f"customer_{current_user.customer_id}/Facilities/{file.filename}"
    
    # 3. Use your existing _upload_to_gcs function from ai_integration.py
    # This function already handles the technical upload logic
    stored_uri = await _upload_to_gcs(
        bucket_name=GCS_BUCKET_NAME, 
        blob_name=blob_path, 
        data=file_content, 
        content_type=file.content_type
    )
    
    if not stored_uri:
        raise HTTPException(status_code=500, detail="Failed to upload to cloud storage")

    return {"url": stored_uri, "filename": file.filename}


# ==============================================================================
# H1: Facility Agreement AI Verification
# ==============================================================================

@router.post("/{facility_id}/analyze-agreement")
async def analyze_facility_agreement_endpoint(
    facility_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """
    H1: Upload a bank facility agreement PDF for AI verification.
    Extracts key terms and compares against current facility fields.
    This is ADVISORY only — it highlights potential gaps, never blocks.
    """
    from app.core.ai_integration import analyze_facility_agreement, FACILITY_DOC_MAX_SIZE_BYTES
    from app.models.models_issuance import IssuanceFacility

    # Validate facility
    facility = db.query(IssuanceFacility).filter(
        IssuanceFacility.id == facility_id,
        IssuanceFacility.customer_id == current_user.customer_id,
        IssuanceFacility.is_deleted == False,
    ).first()
    if not facility:
        raise HTTPException(404, "Facility not found")

    # Read file
    pdf_bytes = await file.read()

    # File size guard (friendly message, not an error)
    if len(pdf_bytes) > FACILITY_DOC_MAX_SIZE_BYTES:
        return {
            "status": "TOO_LARGE",
            "message": f"Document is too large for AI analysis ({len(pdf_bytes) / (1024*1024):.1f} MB). Maximum is 5 MB.",
            "comparison": None,
        }

    # Run AI analysis
    ai_result = await analyze_facility_agreement(
        pdf_bytes, file.filename,
        db=db, customer_id=current_user.customer_id, user_id=current_user.user_id,
    )

    if ai_result["status"] != "OK":
        return ai_result

    extracted = ai_result["extracted_terms"]

    # Store in facility
    facility.agreement_analysis = extracted
    db.commit()

    # Build advisory comparison
    from decimal import Decimal
    comparison = []

    def _compare(field_name, current_val, extracted_val, label):
        if extracted_val is None:
            return
        match = False
        if current_val is not None:
            if isinstance(current_val, (int, float, Decimal)):
                match = abs(float(current_val) - float(extracted_val)) < 0.01
            else:
                match = str(current_val).strip().lower() == str(extracted_val).strip().lower()
        comparison.append({
            "field": field_name,
            "label": label,
            "current_value": str(current_val) if current_val is not None else None,
            "agreement_value": str(extracted_val),
            "match": match,
            "severity": "info" if match else ("warning" if current_val is not None else "suggestion"),
        })

    _compare("total_limit_amount", facility.total_limit_amount, extracted.get("total_limit"), "Total Limit")
    _compare("expiry_date", str(facility.expiry_date) if facility.expiry_date else None, extracted.get("expiry_date"), "Expiry Date")
    _compare("start_date", str(facility.start_date) if facility.start_date else None, extracted.get("start_date"), "Start Date")
    _compare("sla_agreement_days", facility.sla_agreement_days, extracted.get("sla_days"), "SLA Days")
    _compare("tenor_months", facility.tenor_months, extracted.get("tenor_months"), "Tenor (Months)")

    # Currency comparison
    if extracted.get("currency_code") and facility.currency:
        currency_match = facility.currency.iso_code == extracted.get("currency_code")
        comparison.append({
            "field": "currency",
            "label": "Currency",
            "current_value": facility.currency.iso_code,
            "agreement_value": extracted.get("currency_code"),
            "match": currency_match,
            "severity": "info" if currency_match else "warning",
        })

    # Advisory items from special terms
    special_terms = extracted.get("special_terms", [])

    return {
        "status": "OK",
        "message": None,
        "comparison": comparison,
        "special_terms": special_terms,
        "sub_limits_detected": extracted.get("sub_limits", []),
        "mismatches": len([c for c in comparison if not c["match"]]),
        "total_fields_compared": len(comparison),
    }
