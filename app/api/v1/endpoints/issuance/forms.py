from typing import List, Any, Optional, Dict
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks, Body, Request, UploadFile, File, Form
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from datetime import date
import io
import json
import logging

logger = logging.getLogger(__name__)

from app.database import get_db
from app.core.security import get_current_corporate_admin_context, get_current_approver_context, get_current_treasury_context, get_issuance_read_context, check_subscription_status, TokenData
from app.core.document_generator import generate_pdf_from_html
from app.core.encryption import encrypt_data 

# Models
from app.models.models import Bank, Currency 
from app.models.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceFacilitySubLimit, IssuanceFacility, IssuanceWorkflowPolicy, CustomerFormConfiguration, IssuanceRequestSnapshot, IssuanceRequestVersion, AdminChangeRequest, BankFormIssueReport, BankFormTemplate
# NOTE: Ensure you created app/models/models_reconciliation.py first!
from app.models.models_reconciliation import BankPositionBatch, BankPositionRow 
# Schemas
from app.schemas.all_schemas import BankOut, CurrencyOut 
from app.schemas.schemas_issuance import (
    IssuanceRequestCreate, IssuanceRequestOut, IssuanceRequestUpdate, IssuanceRequestDraftCreate,
    CustomerFormConfigurationCreateUpdate, IssuanceRequestVersionOut,
    IssuanceFacilityCreate, IssuanceFacilityOut, SuitableFacilityOut, 
    IssuanceRequestContentUpdate, IssuanceFacilityUpdate,
    IssuedLGRecordOut, IssuedLGRecordDetailOut,
    IssuanceExecuteRequest, IssuanceCancelRequest,
    ReconciliationRequest, ReconciliationResult,
    IssuanceWorkflowPolicyCreate, IssuanceWorkflowPolicyOut,
    BankIssuanceOptionOut, BankIssuanceOptionCreateUpdate,
    AdminChangeRequestCreate, AdminChangeRequestOut, AdminChangeRequestAction,
    BankFormIssueReportCreate, BankFormIssueReportOut, BankFormIssueReportUpdate
)
from app.services.issuance_service import issuance_service

# CRUD
from app.crud.crud_issuance import crud_issuance_request
from app.crud.crud_facility import crud_facility
from app.crud.crud_bank_methods import crud_bank_methods
from fastapi.responses import StreamingResponse

router = APIRouter()

from .base import *
from .base import _read_bank_form_pdf_bytes, _send_edit_notifications, _detect_coverage_gaps, _make_doc_filename, _get_lg_copy_docs, _send_requestor_status_notification, _serialize_action, _serialize_recon_session, _serialize_recon_result, _apply_admin_change, _create_governed_change

@router.post("/bank-forms/upload")
async def upload_bank_form(
    bank_id: int = Query(...),
    form_name: str = Query(...),
    form_type: str = Query("FILLABLE_PDF"),
    form_language: str = Query("BILINGUAL", description="AR / EN / BILINGUAL"),
    lg_type_ids: str = Query(None, description="Optional: comma-separated LG type IDs this form covers. NULL = universal."),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    # Allow System Owner and Corporate Admin
    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges: Requires System Owner or Corporate Admin role.")
    """
    Upload a bank's PDF form template. Creates a BankFormTemplate record
    and stores the file locally. Does NOT trigger AI analysis automatically.
    """
    import os
    
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(400, "Only PDF files are accepted.")
    
    pdf_bytes = await file.read()
    
    # Check for existing active forms for this bank to determine version
    existing = db.query(BankFormTemplate).filter(
        BankFormTemplate.bank_id == bank_id,
        BankFormTemplate.is_active == True,
        BankFormTemplate.is_deleted == False,
    ).order_by(BankFormTemplate.version.desc()).first()
    
    new_version = (existing.version + 1) if existing else 1
    safe_filename = f"v{new_version}_{file.filename}"
    
    # Upload to Google Cloud Storage
    from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME
    blob_path = f"bank_forms/{bank_id}/{safe_filename}"
    file_path = None
    try:
        gcs_uri = await _upload_to_gcs(GCS_BUCKET_NAME, blob_path, pdf_bytes, "application/pdf")
        if gcs_uri:
            file_path = gcs_uri  # Store the gs:// URI
        else:
            raise Exception("GCS upload returned None")
    except Exception as gcs_err:
        import logging
        logging.getLogger(__name__).warning(f"GCS upload failed, saving locally: {gcs_err}")
        # Fallback: store locally
        upload_dir = os.path.join("uploads", "bank_forms", str(bank_id))
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, safe_filename)
        with open(file_path, "wb") as f:
            f.write(pdf_bytes)
    
    # Try to extract interactive form fields
    try:
        from app.core.pdf_form_filler import get_pdf_form_fields
        detected_fields = get_pdf_form_fields(pdf_bytes)
    except Exception:
        detected_fields = []
    
    # Create DB record
    form_template = BankFormTemplate(
        bank_id=bank_id,
        name=form_name,
        version=new_version,
        form_type=form_type,
        form_language=form_language if form_language in ('AR', 'EN', 'BILINGUAL') else 'BILINGUAL',
        lg_type_ids=[int(x.strip()) for x in lg_type_ids.split(',') if x.strip()] if lg_type_ids else None,
        file_path=file_path,
        original_filename=file.filename,
        ai_analysis_status="PENDING",
        is_active=True,
        uploaded_by=current_user.user_id if hasattr(current_user, 'user_id') else None,
    )
    
    # If we detected interactive fields, store them in the AI analysis
    if detected_fields:
        form_template.ai_analysis = {"detected_interactive_fields": detected_fields}
    
    db.add(form_template)
    db.commit()
    db.refresh(form_template)
    
    return {
        "id": form_template.id,
        "name": form_template.name,
        "version": form_template.version,
        "form_type": form_template.form_type,
        "bank_id": form_template.bank_id,
        "original_filename": form_template.original_filename,
        "ai_analysis_status": form_template.ai_analysis_status,
        "detected_fields_count": len(detected_fields),
        "message": f"Form uploaded successfully (v{new_version}). Run AI analysis to auto-map fields."
    }


@router.get("/bank-forms/{form_id}/download")
async def download_bank_form(
    form_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Returns a signed URL to view the uploaded bank form PDF."""
    import os

    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges.")

    form_template = db.query(BankFormTemplate).filter(BankFormTemplate.id == form_id).first()
    if not form_template:
        raise HTTPException(404, "Form template not found.")
    if not form_template.file_path:
        raise HTTPException(404, "No file associated with this form template.")

    # GCS path (gs://...)
    if form_template.file_path.startswith("gs://"):
        from app.core.ai_integration import generate_signed_gcs_url
        signed_url = await generate_signed_gcs_url(form_template.file_path, expiration=3600)
        if not signed_url:
            raise HTTPException(500, "Failed to generate download URL.")
        return {"download_url": signed_url, "filename": form_template.original_filename}

    # Legacy local file: lazy-migrate to GCS, then return signed URL
    if os.path.exists(form_template.file_path):
        with open(form_template.file_path, "rb") as f:
            pdf_bytes = f.read()
        from app.core.ai_integration import _upload_to_gcs, generate_signed_gcs_url, GCS_BUCKET_NAME
        safe_name = os.path.basename(form_template.file_path)
        blob_path = f"bank_forms/{form_template.bank_id}/{safe_name}"
        try:
            gcs_uri = await _upload_to_gcs(GCS_BUCKET_NAME, blob_path, pdf_bytes, "application/pdf")
            if gcs_uri:
                form_template.file_path = gcs_uri
                db.commit()
                import logging
                logging.getLogger(__name__).info(f"Lazy-migrated bank form {form_id} to GCS: {gcs_uri}")
                signed_url = await generate_signed_gcs_url(gcs_uri, expiration=3600)
                if signed_url:
                    return {"download_url": signed_url, "filename": form_template.original_filename}
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Lazy migration to GCS failed for form {form_id}: {e}")
        raise HTTPException(500, "Failed to generate download URL.")

    raise HTTPException(404, "Uploaded PDF file not found.")


@router.post("/bank-forms/{form_id}/analyze")
async def analyze_bank_form(
    form_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    # Allow System Owner and Corporate Admin
    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges: Requires System Owner or Corporate Admin role.")
    """
    Triggers AI analysis on an uploaded bank form to auto-map fields.
    This is called ONCE per form upload. The result is cached and reused.
    """
    form_template = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
        BankFormTemplate.is_deleted == False,
    ).first()
    
    if not form_template:
        raise HTTPException(404, "Bank form template not found.")
    
    pdf_bytes = _read_bank_form_pdf_bytes(form_template)
    
    # Update status
    form_template.ai_analysis_status = "ANALYZING"
    db.commit()
    
    try:
        from app.core.ai_integration import analyze_bank_form_pdf
        from app.core.pdf_form_filler import get_pdf_form_fields
        
        # Get detected fields
        try:
            detected_fields = get_pdf_form_fields(pdf_bytes)
        except Exception:
            detected_fields = []
        
        # Run AI analysis
        result = await analyze_bank_form_pdf(
            pdf_bytes=pdf_bytes,
            filename=form_template.original_filename or "bank_form.pdf",
            detected_fields=detected_fields,
            form_type=form_template.form_type or "FILLABLE_PDF",
        )
        
        # Cache the results
        form_template.field_mapping = result.get("field_mapping", [])
        form_template.ai_analysis = result
        form_template.ai_analysis_status = "COMPLETED"
        
        # Update name if AI detected a better one
        if result.get("form_title") and not form_template.name:
            form_template.name = result["form_title"]
        
        db.commit()
        db.refresh(form_template)
        
        return {
            "id": form_template.id,
            "status": "COMPLETED",
            "form_title": result.get("form_title", ""),
            "bank_name_detected": result.get("bank_name_detected", ""),
            "total_fields": result.get("total_fields", 0),
            "mapped_fields": result.get("mapped_fields", 0),
            "unmapped_fields": result.get("unmapped_fields", []),
            "field_mapping": result.get("field_mapping", []),
            "form_notes": result.get("form_notes", ""),
        }
        
    except Exception as e:
        form_template.ai_analysis_status = "FAILED"
        form_template.ai_analysis = {"error": str(e)}
        db.commit()
        raise HTTPException(500, f"AI analysis failed: {str(e)}")


@router.post("/bank-forms/{form_id}/preview")
def preview_bank_form(
    form_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """
    Generate a test-filled PDF using realistic dummy data.
    Allows admins to verify that AI-analyzed field positions are correct
    before using the form in actual issuance.
    """
    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges.")
    
    form_template = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
        BankFormTemplate.is_deleted == False,
    ).first()
    
    if not form_template:
        raise HTTPException(404, "Bank form template not found.")
    
    if form_template.ai_analysis_status != "COMPLETED":
        raise HTTPException(400, "Form must be analyzed before preview. Run AI analysis first.")
    
    if not form_template.field_mapping:
        raise HTTPException(400, "No field mapping found. Run AI analysis first.")
    
    # PREVIEW MODE: Comprehensive dummy data with ALL fields populated.
    # Every field is given a visible value so that admins can verify
    # the mapping accuracy of ALL positions — including optionals,
    # checkboxes, and multi-choice fields.
    from datetime import date as d, timedelta
    today = d.today()
    dummy_data = {
        # ── Beneficiary ──
        "beneficiary_name": "ACME Construction Co. LLC",
        "beneficiary_address": "12 Nile Avenue, Giza, Egypt",
        "beneficiary_contact_person": "Ahmed Hassan",
        "beneficiary_phone": "+20 100 123 4567",
        "beneficiary_email": "ahmed@acme-construct.com",
        "beneficiary_country": "Egypt",
        "beneficiary_id_number": "ID-0987654321",
        # ── Amount & Currency ──
        "amount": 250000.00,
        "amount_in_words": "Two Hundred Fifty Thousand Egyptian Pounds Only",
        "currency_code": "EGP",
        "currency_name": "Egyptian Pounds",
        "amount_with_currency": "EGP 250,000.00",
        # ── LG Type (ALL set to True for preview so every checkbox position is tested) ──
        "lg_type": "Performance Guarantee",
        "guarantee_type": "Performance Guarantee",
        "lg_type_is_bid_bond": True,
        "lg_type_is_performance": True,
        "lg_type_is_advance_payment": True,
        "lg_type_is_advance_conditioned": True,
        "lg_type_is_advance_unconditioned": True,
        "lg_type_is_payment_guarantee": True,
        "lg_type_is_financial_guarantee": True,
        # ── Purpose ──
        "lg_purpose": "Supply and installation of HVAC systems - Phase 2",
        "purpose": "Supply and installation of HVAC systems - Phase 2",
        "operational_status": "Operative",
        # ── Dates ──
        "current_date": today,
        "requested_issue_date": today,
        "requested_expiry_date": today + timedelta(days=365),
        "expiry_date": today + timedelta(days=365),
        "effective_date": today,
        "tender_date": today - timedelta(days=30),
        # ── Reference ──
        "reference_type": "Contract",
        "reference_number": "CON-2026-A100",
        "reference_date": today - timedelta(days=60),
        "reference_amount": 2500000.00,
        "tender_number": "TNR-2026-555",
        # ── Customer / Entity ──
        "entity_name": "Pilot Test Corp Main Entity",
        "entity_address": "45 Smart Village, 6th October City, Giza, Egypt",
        "customer_name": "Pilot Test Corp",
        "company_name": "Pilot Test Corp",
        "customer_address": "45 Smart Village, 6th October City, Giza, Egypt",
        "customer_phone": "+20 2 3456 7890",
        "customer_email": "treasury@pilottest.com",
        "customer_name_for_bank": "Pilot Test Corp Main Entity",
        # ── Bank Account ──
        "bank_branch": "New Cairo Branch",
        "bank_account_number": "1234567890123",
        "customer_cif_number": "CIF-98765",
        "iban": "EG30 0000 1234 5678 9012 3456 7",
        "account_name": "Pilot Test Corp Treasury Account",
        "bank_name": "Al Ahli Bank of Kuwait - Egypt",
        # ── Indicators (ALL True for preview — tests every checkbox) ──
        "is_local_lg": True,
        "is_cross_border": True,
        "lg_format_is_bank_standard": True,
        "lg_format_is_special": True,
        "is_third_party": True,
        "is_in_own_name": True,
        "third_party_name": "Delta Subcontractors Ltd.",
        "third_party_address": "78 Industrial Zone, 10th of Ramadan, Egypt",
        "lg_language_is_arabic": True,
        "lg_language_is_english": True,
        "has_facility_at_bank": True,
        "facility_reference": "FAC-2025-001",
        "requires_special_wording": True,
        # ── Requestor ──
        "requestor_name": "Wael Ghali",
        "requestor_email": "wael@pilottest.com",
        "department": "Treasury Department",
        "project_name": "Highway Construction Phase 2",
        "serial_number": "ISS-2026-PREVIEW",
        # ── Additional / Optional Fields (all populated for preview) ──
        "additional_conditions": "As per attached special wording / Cross-border Letter of Guarantee",
        "special_conditions": "Subject to ICC URDG 758",
        "custom_field_1_value": "Custom Value 1 (Preview)",
        "custom_field_2_value": "Custom Value 2 (Preview)",
        "margin_percentage": "10%",
        "commission_rate": "0.15%",
        "notes": "Preview mode — all fields populated for testing",
    }
    
    pdf_bytes = _read_bank_form_pdf_bytes(form_template)
    
    # Choose fill mode
    effective_form_type = form_template.form_type or "FILLABLE_PDF"
    
    # Auto-detect: if FILLABLE_PDF but no interactive fields, switch to SCANNED_FILL
    if effective_form_type == "FILLABLE_PDF":
        from app.core.pdf_form_filler import get_pdf_form_fields
        pdf_fields_check = get_pdf_form_fields(pdf_bytes)
        if len(pdf_fields_check) == 0:
            effective_form_type = "SCANNED_FILL"
    
    if effective_form_type == "PHYSICAL_OVERLAY":
        from app.core.pdf_form_filler import generate_overlay_pdf
        filled_pdf = generate_overlay_pdf(
            template_pdf_bytes=pdf_bytes,
            field_mapping=form_template.field_mapping,
            request_data=dummy_data,
        )
    elif effective_form_type == "SCANNED_FILL":
        from app.core.pdf_form_filler import generate_scanned_fill_pdf
        filled_pdf = generate_scanned_fill_pdf(
            template_pdf_bytes=pdf_bytes,
            field_mapping=form_template.field_mapping,
            request_data=dummy_data,
        )
    else:
        from app.core.pdf_form_filler import fill_pdf_form
        filled_pdf = fill_pdf_form(
            template_pdf_bytes=pdf_bytes,
            field_mapping=form_template.field_mapping,
            request_data=dummy_data,
        )
    
    filename = f"PREVIEW_{form_template.name or 'form'}.pdf"
    # Sanitize filename for HTTP headers (latin-1 only)
    filename = filename.encode('ascii', 'replace').decode('ascii')
    
    return StreamingResponse(
        io.BytesIO(filled_pdf),
        media_type="application/pdf",
        headers={
            'Content-Disposition': f'inline; filename="{filename}"',
            'X-Preview-Mode': 'true',
        }
    )


@router.post("/bank-forms/{form_id}/enhance")
def enhance_bank_form_endpoint(
    form_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Visual feedback loop: generates preview, sends to Gemini for visual correction, applies fixes."""
    from app.core.ai_integration import enhance_bank_form_mapping
    from app.constants import UserRole
    import asyncio
    
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges.")
    
    form_template = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id, BankFormTemplate.is_deleted == False,
    ).first()
    if not form_template:
        raise HTTPException(404, "Bank form template not found.")
    if not form_template.field_mapping:
        raise HTTPException(400, "No field mapping to enhance. Analyze first.")
    if form_template.form_type not in ("PHYSICAL_OVERLAY", "SCANNED_FILL"):
        raise HTTPException(400, "Enhance only available for overlay/scanned forms.")
    
    # Load template PDF
    pdf_bytes = _read_bank_form_pdf_bytes(form_template)
    
    # Build preview dummy data (same as preview endpoint)
    from datetime import date as d, timedelta
    today = d.today()
    dummy_data = {
        "beneficiary_name": "ACME Construction Co. LLC",
        "beneficiary_address": "12 Nile Avenue, Giza, Egypt",
        "amount": 250000.00,
        "amount_in_words": "Two Hundred Fifty Thousand Egyptian Pounds Only",
        "currency_code": "EGP",
        "lg_type": "Performance Guarantee",
        "lg_purpose": "Supply and installation of HVAC systems - Phase 2",
        "current_date": today,
        "requested_issue_date": today,
        "requested_expiry_date": today + timedelta(days=365),
        "entity_name": "Pilot Test Corp Main Entity",
        "entity_address": "45 Smart Village, 6th October City, Giza, Egypt",
        "bank_branch": "New Cairo Branch",
        "bank_account_number": "1234567890123",
        "customer_cif_number": "CIF-98765",
        "customer_phone": "+20 2 3456 7890",
        "customer_email": "treasury@pilottest.com",
        "customer_name_for_bank": "Pilot Test Corp Main Entity",
        "reference_number": "CON-2026-A100",
        "additional_conditions": "As per attached special wording",
        "third_party_name": "Delta Subcontractors Ltd.",
        "third_party_address": "78 Industrial Zone, 10th of Ramadan, Egypt",
        "lg_type_is_bid_bond": True, "lg_type_is_performance": True,
        "lg_type_is_advance_payment": True, "is_cross_border": True,
    }
    
    # Generate the current preview
    effective_type = form_template.form_type or "SCANNED_FILL"
    if effective_type == "SCANNED_FILL":
        from app.core.pdf_form_filler import generate_scanned_fill_pdf
        filled_pdf = generate_scanned_fill_pdf(pdf_bytes, form_template.field_mapping, dummy_data)
    else:
        from app.core.pdf_form_filler import generate_overlay_pdf
        filled_pdf = generate_overlay_pdf(pdf_bytes, form_template.field_mapping, dummy_data)
    
    # Store backup BEFORE enhancement
    form_template.field_mapping_backup = list(form_template.field_mapping)
    
    # Run enhance
    try:
        loop = asyncio.new_event_loop()
        enhanced = loop.run_until_complete(enhance_bank_form_mapping(
            template_pdf_bytes=pdf_bytes, filled_pdf_bytes=filled_pdf,
            current_mapping=form_template.field_mapping,
            form_type=effective_type, filename=form_template.name or "form.pdf",
        ))
        loop.close()
    except Exception as e:
        logger.error(f"Enhancement failed: {e}", exc_info=True)
        raise HTTPException(500, f"Enhancement failed: {str(e)}")
    
    form_template.field_mapping = enhanced
    db.commit()
    return {"status": "enhanced", "form_id": form_id, "fields_count": len(enhanced),
            "message": "Enhancement applied. Use undo to revert if needed.", "can_undo": True}


@router.post("/bank-forms/{form_id}/undo-enhance")
def undo_enhance_bank_form_endpoint(
    form_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Reverts the last enhancement by restoring the backup mapping."""
    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges.")
    
    form_template = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id, BankFormTemplate.is_deleted == False,
    ).first()
    if not form_template:
        raise HTTPException(404, "Bank form template not found.")
    if not form_template.field_mapping_backup:
        raise HTTPException(400, "No enhancement backup found. Nothing to undo.")
    
    form_template.field_mapping = list(form_template.field_mapping_backup)
    form_template.field_mapping_backup = None
    db.commit()
    return {"status": "reverted", "form_id": form_id,
            "fields_count": len(form_template.field_mapping),
            "message": "Enhancement undone. Original mapping restored."}


@router.get("/bank-forms")
def list_bank_forms(
    bank_id: int = Query(None),
    include_archived: bool = Query(False, description="Include suspended and deleted forms"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """List bank form templates, optionally filtered by bank. Use include_archived=true to see deleted/suspended forms."""
    if include_archived:
        query = db.query(BankFormTemplate)  # Show all including deleted/suspended
    else:
        query = db.query(BankFormTemplate).filter(
            BankFormTemplate.is_active == True,
            BankFormTemplate.is_deleted == False,
        )
    if bank_id:
        query = query.filter(BankFormTemplate.bank_id == bank_id)
    
    forms = query.order_by(BankFormTemplate.bank_id, BankFormTemplate.priority.desc(), BankFormTemplate.version.desc()).all()
    
    return [
        {
            "id": f.id,
            "bank_id": f.bank_id,
            "bank_name": f.bank.name if f.bank else "Unknown",
            "name": f.name,
            "version": f.version,
            "form_type": f.form_type,
            "form_language": getattr(f, 'form_language', 'BILINGUAL') or 'BILINGUAL',
            "original_filename": f.original_filename,
            "ai_analysis_status": f.ai_analysis_status,
            "lg_type_ids": f.lg_type_ids or [],
            "mapped_fields_count": len(f.field_mapping) if f.field_mapping else 0,
            "is_active": f.is_active,
            "is_deleted": f.is_deleted,
            "priority": f.priority or 0,
            "created_at": f.created_at.isoformat() if f.created_at else None,
        }
        for f in forms
    ]


@router.get("/bank-forms/{form_id}")
def get_bank_form(
    form_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Get full details of a bank form template including field mapping."""
    form_template = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
        BankFormTemplate.is_deleted == False,
    ).first()
    
    if not form_template:
        raise HTTPException(404, "Bank form template not found.")
    
    return {
        "id": form_template.id,
        "bank_id": form_template.bank_id,
        "bank_name": form_template.bank.name if form_template.bank else "Unknown",
        "name": form_template.name,
        "version": form_template.version,
        "form_type": form_template.form_type,
        "form_language": getattr(form_template, 'form_language', 'BILINGUAL') or 'BILINGUAL',
        "original_filename": form_template.original_filename,
        "file_path": form_template.file_path,
        "ai_analysis_status": form_template.ai_analysis_status,
        "lg_type_ids": form_template.lg_type_ids or [],
        "field_mapping": form_template.field_mapping,
        "field_mapping_backup": form_template.field_mapping_backup,
        "ai_analysis": form_template.ai_analysis,
        "is_active": form_template.is_active,
        "priority": form_template.priority or 0,
        "created_at": form_template.created_at.isoformat() if form_template.created_at else None,
        "updated_at": form_template.updated_at.isoformat() if form_template.updated_at else None,
    }


@router.put("/bank-forms/{form_id}/mapping")
def update_bank_form_mapping(
    form_id: int,
    mapping: List[dict] = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges.")
    """Manually override/fine-tune the AI-generated field mapping."""
    form_template = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
        BankFormTemplate.is_deleted == False,
    ).first()
    
    if not form_template:
        raise HTTPException(404, "Bank form template not found.")
    
    form_template.field_mapping = mapping
    db.commit()
    
    return {"message": "Field mapping updated", "id": form_template.id}


@router.delete("/bank-forms/{form_id}")
def delete_bank_form(
    form_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Soft-deletes a bank form template."""
    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges.")
    
    form = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
        BankFormTemplate.is_deleted == False,
    ).first()
    if not form:
        raise HTTPException(404, "Bank form template not found.")
    
    form.is_deleted = True
    form.is_active = False
    db.commit()
    return {"message": f"Form '{form.name}' deleted.", "id": form_id}


@router.patch("/bank-forms/{form_id}/restore")
def restore_bank_form(
    form_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Restores a soft-deleted bank form template."""
    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges.")
    
    form = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
    ).first()
    if not form:
        raise HTTPException(404, "Bank form template not found.")
    
    form.is_deleted = False
    form.is_active = True
    db.commit()
    return {"message": f"Form '{form.name}' restored.", "id": form_id, "is_active": True, "is_deleted": False}


@router.patch("/bank-forms/{form_id}/toggle-active")
def toggle_bank_form_active(
    form_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Toggles a bank form between active and suspended."""
    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges.")
    
    form = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
        BankFormTemplate.is_deleted == False,
    ).first()
    if not form:
        raise HTTPException(404, "Bank form template not found.")
    
    form.is_active = not form.is_active
    db.commit()
    status = "active" if form.is_active else "suspended"
    return {"message": f"Form '{form.name}' is now {status}.", "id": form_id, "is_active": form.is_active}


@router.patch("/bank-forms/{form_id}/priority")
def set_bank_form_priority(
    form_id: int,
    priority: int = Query(..., ge=0, le=100),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(check_subscription_status),
):
    """Sets the priority ranking of a bank form (0-100, higher = preferred)."""
    from app.constants import UserRole
    if current_user.role not in [UserRole.SYSTEM_OWNER, UserRole.CORPORATE_ADMIN]:
        raise HTTPException(403, "Not enough privileges.")
    
    form = db.query(BankFormTemplate).filter(
        BankFormTemplate.id == form_id,
        BankFormTemplate.is_deleted == False,
    ).first()
    if not form:
        raise HTTPException(404, "Bank form template not found.")
    
    form.priority = priority
    db.commit()
    return {"message": f"Form '{form.name}' priority set to {priority}.", "id": form_id, "priority": priority}


@router.get("/form-dictionary")
def get_form_dictionary(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Returns entities, departments, lg-types, and currencies for the issuance form.
    Accessible by end users, corporate admins, and checkers."""
    from app.models.models import CustomerEntity, Currency
    from app.models.models_issuance import IssuanceFacility
    from app.crud.crud import crud_customer_entity, crud_lg_type, crud_currency

    # Entities
    entities = db.query(CustomerEntity).filter(
        CustomerEntity.customer_id == current_user.customer_id,
        CustomerEntity.is_deleted == False
    ).all()

    # Departments from org structure
    from app.models.models import Department
    departments = db.query(Department).filter(
        Department.customer_id == current_user.customer_id,
        Department.is_deleted == False
    ).all()

    # LG Types
    lg_types = crud_lg_type.get_all(db, limit=1000)

    # Currencies
    currencies = crud_currency.get_all(db, limit=1000)

    return {
        "entities": [{"id": e.id, "name": e.entity_name, "code": e.code} for e in entities],
        "departments": [{"id": d.id, "name": d.name} for d in departments],
        "lgTypes": [{"id": t.id, "name": t.name} for t in lg_types],
        "currencies": [{"id": c.id, "name": c.name, "iso_code": c.iso_code} for c in currencies],
    }


# ==============================================================================
# ADMIN FORM CONFIGURATION
# ==============================================================================

@router.get("/form-config", response_model=CustomerFormConfigurationCreateUpdate)
def get_form_configuration(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context)
):
    """Fetch the custom form layout for the customer. Returns defaults if none exist."""
    config = db.query(CustomerFormConfiguration).filter(
        CustomerFormConfiguration.customer_id == current_user.customer_id
    ).first()
    
    if not config:
        # Return default empty config
        return CustomerFormConfigurationCreateUpdate()
        
    return CustomerFormConfigurationCreateUpdate(
        field_configurations=config.field_configurations,
        custom_field_1_config=config.custom_field_1_config,
        custom_field_2_config=config.custom_field_2_config,
        mandatory_document_types=config.mandatory_document_types,
        reference_types=config.reference_types,
        document_config=config.document_config
    )

@router.put("/form-config", response_model=CustomerFormConfigurationCreateUpdate)
def update_form_configuration(
    config_in: CustomerFormConfigurationCreateUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Updates the form layout. Validates against hiding critical fields.
    Multi-admin: requires dual-control approval via AdminChangeRequest."""
    
    # Check if department is being made optional/hidden while an active DEPT_MATCH policy exists
    dept_config = config_in.field_configurations.get("department")
    if not dept_config or not dept_config.is_visible or not dept_config.is_mandatory:
        has_dept_policy = db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == current_user.customer_id,
            IssuanceWorkflowPolicy.condition_type == "DEPT_MATCH",
            IssuanceWorkflowPolicy.is_active == True
        ).first()
        if has_dept_policy:
            from app.schemas.schemas_issuance import FieldConfiguration
            config_in.field_configurations["department"] = FieldConfiguration(
                is_visible=True, is_mandatory=True
            )
            
    # Capture new value as serializable dict
    new_val = {
        "field_configurations": {k: v.model_dump() for k, v in config_in.field_configurations.items()},
        "mandatory_document_types": config_in.mandatory_document_types,
        "reference_types": config_in.reference_types,
        "document_config": config_in.document_config,
    }
    if config_in.custom_field_1_config:
        new_val["custom_field_1_config"] = config_in.custom_field_1_config.model_dump()
    if config_in.custom_field_2_config:
        new_val["custom_field_2_config"] = config_in.custom_field_2_config.model_dump()

    # Capture old value for audit
    existing = db.query(CustomerFormConfiguration).filter(
        CustomerFormConfiguration.customer_id == current_user.customer_id
    ).first()
    old_val = {}
    if existing:
        old_val = {
            "field_configurations": existing.field_configurations,
            "mandatory_document_types": existing.mandatory_document_types,
        }

    change_req, auto_approved = _create_governed_change(
        db, current_user.customer_id, current_user.user_id,
        "FORM_CONFIG_UPDATE", {"old_value": old_val, "new_value": new_val}
    )

    if auto_approved:
        # Single-admin: already applied by _apply_admin_change inside _create_governed_change
        return config_in

    # Multi-admin: return 202 — change is pending approval
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=202,
        content={
            "message": "Configuration change submitted for approval by a second administrator.",
            "change_request_id": change_req.id,
            "status": "PENDING"
        }
    )


# ==============================================================================
# DUPLICATE & SIMILARITY CHECKS
# ==============================================================================

@router.post("/bank-forms/auto-fill/{request_id}")
async def auto_fill_bank_form(
    request_id: int,
    bank_id: int = Query(..., description="The bank to find forms for"),
    user_values: Optional[Dict[str, str]] = Body(None, description="User-provided values for missing fields"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Two-phase bank form auto-fill:
    
    Phase 1 (user_values=null): Build data, detect missing fields.
      - If ALL fields have values → return filled PDF immediately.
      - If some fields are empty → return JSON with missing_fields + saved_values.
    
    Phase 2 (user_values={...}): Merge user values into data, generate PDF, save values for reuse.
    """
    from sqlalchemy.orm import selectinload
    from app.models.models_issuance import FormFieldUserValue
    
    # Load request
    request = db.query(IssuanceRequest).options(
        selectinload(IssuanceRequest.currency),
        selectinload(IssuanceRequest.lg_type),
        selectinload(IssuanceRequest.issuing_entity),
        selectinload(IssuanceRequest.customer),
        selectinload(IssuanceRequest.project),
    ).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == current_user.customer_id,
    ).first()
    
    if not request:
        raise HTTPException(404, "Issuance request not found.")
    
    # --- ATOMIC BANK LOCK: Prevent form generation for multiple banks ---
    # Only allow form generation for requests in issuance-eligible statuses
    if request.status not in ("APPROVED_INTERNAL", "FACILITY_RESERVED", "INTERNAL_PROCESSING"):
        raise HTTPException(400, f"Cannot generate bank form for request in status '{request.status}'.")
    
    # If already issued (has an active LG record), reject
    if request.lg_record_id:
        existing_lg = db.query(IssuedLGRecord).filter(
            IssuedLGRecord.id == request.lg_record_id,
            IssuedLGRecord.status.notin_(("SLA_EXCEEDED", "BANK_REJECTED", "CANCELLED"))
        ).first()
        if existing_lg:
            raise HTTPException(409, f"This request has already been issued (LG #{existing_lg.id}). Cannot generate new forms.")
    
    # --- SUBSCRIPTION LIMIT CHECK: Fail early before locking or generating anything ---
    from app.models import Customer as _Customer
    _customer = db.query(_Customer).filter(_Customer.id == current_user.customer_id).first()
    if _customer and _customer.subscription_plan and _customer.active_issuance_lg_count >= _customer.subscription_plan.max_issuance_records:
        raise HTTPException(
            status_code=400,
            detail=f"Issuance LG limit ({_customer.subscription_plan.max_issuance_records}) exceeded for this customer's subscription plan. "
                   f"Cannot proceed with issuance — you have reached the maximum number of active LGs allowed."
        )

    # Check bank lock: if request is locked to a DIFFERENT bank, reject

    meta = dict(request.metadata_json or {})
    locked_bank_id = meta.get("locked_bank_id")
    if locked_bank_id and int(locked_bank_id) != int(bank_id):
        raise HTTPException(
            409,
            f"This request is locked to bank #{locked_bank_id}. "
            f"You cannot generate forms for a different bank. "
            f"Complete the issuance with the current bank or cancel first."
        )
    
    # Lock to this bank on first form generation
    if not locked_bank_id:
        meta["locked_bank_id"] = bank_id
        request.metadata_json = meta
        request.locked_for_issuance = True
        db.flush()  # Push lock immediately
    
    # Find best matching form: type-specific first, then universal, language-aware
    req_lang = getattr(request, 'lg_language', 'AR') or 'AR'  # AR or EN
    
    all_candidates = db.query(BankFormTemplate).filter(
        BankFormTemplate.bank_id == bank_id,
        BankFormTemplate.is_active == True,
        BankFormTemplate.is_deleted == False,
        BankFormTemplate.ai_analysis_status == "COMPLETED",
    ).order_by(BankFormTemplate.version.desc()).all()
    
    # 4-tier priority selection:
    # 1. Type-specific + matching language (AR/EN)
    # 2. Type-specific + BILINGUAL
    # 3. Universal + matching language
    # 4. Universal + BILINGUAL
    form_template = None
    
    # P1: type match + exact language
    for f in all_candidates:
        if f.lg_type_ids and request.lg_type_id in f.lg_type_ids:
            if getattr(f, 'form_language', 'BILINGUAL') == req_lang:
                form_template = f
                break
    
    # P2: type match + bilingual
    if not form_template:
        for f in all_candidates:
            if f.lg_type_ids and request.lg_type_id in f.lg_type_ids:
                if getattr(f, 'form_language', 'BILINGUAL') == 'BILINGUAL':
                    form_template = f
                    break
    
    # P3: universal + exact language
    if not form_template:
        for f in all_candidates:
            if not f.lg_type_ids:
                if getattr(f, 'form_language', 'BILINGUAL') == req_lang:
                    form_template = f
                    break
    
    # P4: universal + bilingual
    if not form_template:
        for f in all_candidates:
            if not f.lg_type_ids:
                if getattr(f, 'form_language', 'BILINGUAL') == 'BILINGUAL':
                    form_template = f
                    break
    
    if not form_template and all_candidates:
        # Last resort: any active analyzed form for this bank
        form_template = all_candidates[0]
    
    if not form_template:
        raise HTTPException(404, f"No analyzed bank form template found for this bank. Please upload and analyze a form first.")
    
    if not form_template.field_mapping:
        raise HTTPException(400, "Form has no field mapping. Run AI analysis first.")
    
    # Normalize field_mapping: if AI stored it as a dict, convert to list
    import logging as _logging
    _logger = _logging.getLogger(__name__)
    field_mapping = form_template.field_mapping
    if isinstance(field_mapping, dict):
        _logger.warning(f"field_mapping for form {form_template.id} is a dict — normalizing to list")
        field_mapping = [{"pdf_field_name": k, **v} if isinstance(v, dict) else {"pdf_field_name": k, "mapped_to": v} for k, v in field_mapping.items()]
        form_template.field_mapping = field_mapping
        db.commit()
    
    # ── PRE-CHECK: Sub-limit capacity ──
    # Verify facility has enough capacity BEFORE generating the bank form
    if request.selected_sub_limit_id:
        from sqlalchemy import func as sa_func
        from decimal import Decimal
        sub_limit = db.query(IssuanceFacilitySubLimit).filter(
            IssuanceFacilitySubLimit.id == request.selected_sub_limit_id
        ).first()
        if sub_limit:
            facility = db.query(IssuanceFacility).filter(
                IssuanceFacility.id == sub_limit.facility_id
            ).first()
            if facility:
                from app.services.fx_service import fx_service
                fac_amount, _ = fx_service.convert(
                    db,
                    Decimal(str(request.amount)),
                    request.currency_id,
                    facility.currency_id if facility else request.currency_id,
                    allow_ai=False,
                )
                if fac_amount is not None:
                    used = float(db.query(sa_func.coalesce(sa_func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0)).filter(
                        IssuanceExposureEntry.sub_limit_id == request.selected_sub_limit_id,
                        IssuanceExposureEntry.is_active == True
                    ).scalar())
                    used += float(getattr(sub_limit, 'initial_utilization', 0) or 0)
                    available = float(sub_limit.limit_amount) - used
                    if float(fac_amount) > available:
                        raise HTTPException(
                            400,
                            f"Insufficient sub-limit capacity. Available: {available:,.2f}, "
                            f"Required: {float(fac_amount):,.2f}. "
                            f"Please select a different facility or adjust the amount before generating the bank form."
                        )
    
    # Build data dict (auto-fills from system data + bank account)
    from app.core.pdf_form_filler import fill_pdf_form, build_request_data_dict
    request_data = build_request_data_dict(request, db, bank_id=bank_id)
    _logger.info(f"Auto-fill: form_type={form_template.form_type}, field_mapping has {len(field_mapping)} entries, request_data has {len(request_data)} keys")
    _logger.info(f"Auto-fill: non-empty request_data keys: {[k for k,v in request_data.items() if v]}")
    
    # Look up special wording attachment (for auto-open after form download)
    special_wording_doc_id = None
    if request.requires_special_wording:
        from app.models.models_issuance import IssuanceRequestDocument
        sw_doc = db.query(IssuanceRequestDocument).filter(
            IssuanceRequestDocument.request_id == request.id,
            IssuanceRequestDocument.document_type == "SPECIAL_WORDING",
            IssuanceRequestDocument.is_deleted == False,
        ).first()
        if sw_doc:
            special_wording_doc_id = sw_doc.id
    
    # Load saved user values for this customer + form
    saved_rows = db.query(FormFieldUserValue).filter(
        FormFieldUserValue.customer_id == current_user.customer_id,
        FormFieldUserValue.form_template_id == form_template.id,
    ).all()
    saved_values = {row.pdf_field_name: row.saved_value for row in saved_rows}
    
    # ── PHASE 1: Detect missing fields ──
    if user_values is None:
        # --- Currency auto-embed: currency_code / currency_name can NEVER be missing ---
        # If the form has no dedicated currency_code field, prepend the currency ISO code
        # before the amount so it is always visible on the printed form.
        has_currency_field = any(
            me.get("mapped_to") in ("currency_code", "currency_name")
            for me in form_template.field_mapping
        )
        _currency_code = request_data.get("currency_code") or ""
        if not has_currency_field and _currency_code:
            # Prepend currency to amount: "EGP 250,000.00"
            request_data["amount"] = request_data.get("amount_with_currency") or f"{_currency_code} {float(request.amount):,.2f}"
            _logger.info(f"No currency field in form → amount auto-set to '{request_data['amount']}'")
        # Harden amount_in_words — always include currency prefix + spelled-out currency name
        # e.g. "EGP 100,000.00 — One Hundred Thousand Egyptian Pounds Only"
        _amount_val = float(request.amount) if request.amount else 0
        from app.core.pdf_form_filler import _number_to_words as _ntw
        _words = _ntw(_amount_val, _currency_code or "EGP")
        request_data["amount_in_words"] = (
            f"{_currency_code} {_amount_val:,.2f} — {_words}" if _currency_code else _words
        )
        _logger.info(f"amount_in_words set to: '{request_data['amount_in_words'][:80]}'")
        
        # --- Build filtering sets ---
        # Currency fields: ALWAYS auto-filled — never surface as missing or unmapped
        CURRENCY_ALWAYS_FILLED = {"currency_code", "currency_name", "amount_with_currency", "amount_in_words"}
        # Third-party fields to skip when request is NOT third-party
        is_third_party = request_data.get("is_third_party", False)
        third_party_keys = {"third_party_name", "third_party_address", "third_party_relationship"}
        # custom_field_1_value is used for "Other Commercial Registration" only in third-party context
        if not is_third_party:
            third_party_keys.add("custom_field_1_value")
        
        # Track seen mapped_to keys to deduplicate (Issue 2: same field in EN + AR)
        seen_mapped_to = set()
        
        # Diagnostic: log each field mapping and what it resolves to
        for _me in form_template.field_mapping:
            _mt = _me.get("mapped_to", "")
            _pf = _me.get("pdf_field_name", "")
            _v = request_data.get(_mt, "__NOT_FOUND__")
            _logger.info(f"  MAPPING: pdf_field='{_pf}' → mapped_to='{_mt}' → value='{str(_v)[:60]}' (type={type(_v).__name__})")
        
        missing_fields = []
        for mapping_entry in form_template.field_mapping:
            mapped_to = mapping_entry.get("mapped_to", "")
            pdf_field = mapping_entry.get("pdf_field_name", "")
            field_type = mapping_entry.get("field_type", "text").lower()
            
            # Skip checkboxes — they always have a boolean value
            if field_type == "checkbox":
                continue

            # Currency fields are ALWAYS auto-filled — never report as missing
            if mapped_to in CURRENCY_ALWAYS_FILLED:
                _logger.info(f"  SKIP currency field '{pdf_field}' ({mapped_to}) — always auto-filled")
                continue

            # Issue 4/5: Skip third-party fields when not a third-party issuance
            if not is_third_party and mapped_to in third_party_keys:
                _logger.info(f"  SKIP third-party field '{pdf_field}' ({mapped_to}) — not a third-party issuance")
                continue
            
            # Issue 2: Skip duplicate mapped_to (e.g. same field appearing as _en, _ar, _shared)
            if mapped_to in seen_mapped_to:
                _logger.info(f"  SKIP duplicate '{pdf_field}' ({mapped_to}) — already seen")
                continue
            seen_mapped_to.add(mapped_to)
            
            # Check if the system data has a value
            val = request_data.get(mapped_to, "")
            if isinstance(val, (bool, int, float)):
                continue  # These always have values
            
            is_empty = not val or str(val).strip() == ""
            
            if is_empty:
                missing_fields.append({
                    "pdf_field_name": pdf_field,
                    "label": mapping_entry.get("label", pdf_field),
                    "mapped_to": mapped_to,
                    "field_type": field_type,
                    "saved_value": saved_values.get(pdf_field, ""),
                })
        
        # If nothing is missing, generate PDF directly
        if not missing_fields:
            # Merge any saved values that might map to unmapped fields
            for pdf_field, sv in saved_values.items():
                if sv:
                    request_data[pdf_field] = sv
            
            template_pdf_bytes = _read_bank_form_pdf_bytes(form_template)
            
            # Auto-detect: if FILLABLE_PDF but no interactive fields, switch to SCANNED_FILL
            effective_form_type = form_template.form_type or "FILLABLE_PDF"
            if effective_form_type == "FILLABLE_PDF":
                from app.core.pdf_form_filler import get_pdf_form_fields
                pdf_fields = get_pdf_form_fields(template_pdf_bytes)
                if len(pdf_fields) == 0:
                    _logger.warning(f"Form '{form_template.name}' (id={form_template.id}) has no interactive PDF fields — auto-switching to SCANNED_FILL mode")
                    effective_form_type = "SCANNED_FILL"
            
            if effective_form_type == "PHYSICAL_OVERLAY":
                from app.core.pdf_form_filler import generate_overlay_pdf
                fill_lang = req_lang if getattr(form_template, 'form_language', 'BILINGUAL') == 'BILINGUAL' else None
                filled_pdf = generate_overlay_pdf(
                    template_pdf_bytes=template_pdf_bytes,
                    field_mapping=form_template.field_mapping,
                    request_data=request_data,
                    lg_language=fill_lang,
                )
            elif effective_form_type == "SCANNED_FILL":
                from app.core.pdf_form_filler import generate_scanned_fill_pdf
                fill_lang = req_lang if getattr(form_template, 'form_language', 'BILINGUAL') == 'BILINGUAL' else None
                filled_pdf = generate_scanned_fill_pdf(
                    template_pdf_bytes=template_pdf_bytes,
                    field_mapping=form_template.field_mapping,
                    request_data=request_data,
                    lg_language=fill_lang,
                )
            else:
                fill_lang = req_lang if getattr(form_template, 'form_language', 'BILINGUAL') == 'BILINGUAL' else None
                filled_pdf = fill_pdf_form(
                    template_pdf_bytes=template_pdf_bytes,
                    field_mapping=form_template.field_mapping,
                    request_data=request_data,
                    lg_language=fill_lang,
                )
            
            filename = f"Filled_{form_template.name}_{request.serial_number}.pdf"
            return StreamingResponse(
                io.BytesIO(filled_pdf),
                media_type="application/pdf",
                headers={
                    'Content-Disposition': f'inline; filename="{filename}"',
                    'X-Form-Template-Id': str(form_template.id),
                    'X-Form-Template-Name': form_template.name,
                    'X-Form-Type': form_template.form_type or 'FILLABLE_PDF',
                    'X-Special-Wording-Doc-Id': str(special_wording_doc_id) if special_wording_doc_id else '',
                    'Access-Control-Expose-Headers': 'X-Form-Type, X-Special-Wording-Doc-Id',
                }
            )
        
        # Some fields are missing — return them for user input
        return {
            "status": "missing_fields",
            "form_template_id": form_template.id,
            "form_template_name": form_template.name,
            "form_type": form_template.form_type or "FILLABLE_PDF",
            "missing_fields": missing_fields,
            "total_fields": len(form_template.field_mapping),
            "auto_filled_fields": len(form_template.field_mapping) - len(missing_fields),
            "special_wording_doc_id": special_wording_doc_id,
        }
    
    # ── PHASE 2: Merge user values and generate PDF ──
    # Merge user-provided values into request_data
    for mapping_entry in form_template.field_mapping:
        pdf_field = mapping_entry.get("pdf_field_name", "")
        mapped_to = mapping_entry.get("mapped_to", "")
        
        if pdf_field in user_values and user_values[pdf_field]:
            request_data[mapped_to] = user_values[pdf_field]
    
    # Also inject saved values for any still-empty fields
    for pdf_field, sv in saved_values.items():
        if sv and pdf_field not in user_values:
            # Find the mapped_to for this pdf_field
            for m in form_template.field_mapping:
                if m.get("pdf_field_name") == pdf_field:
                    mapped_key = m.get("mapped_to", "")
                    if mapped_key and not request_data.get(mapped_key):
                        request_data[mapped_key] = sv
                    break

    # Save user values for future use
    for pdf_field, value in user_values.items():
        if value is not None:
            existing = db.query(FormFieldUserValue).filter(
                FormFieldUserValue.customer_id == current_user.customer_id,
                FormFieldUserValue.form_template_id == form_template.id,
                FormFieldUserValue.pdf_field_name == pdf_field,
            ).first()
            
            if existing:
                existing.saved_value = value
            else:
                db.add(FormFieldUserValue(
                    customer_id=current_user.customer_id,
                    form_template_id=form_template.id,
                    pdf_field_name=pdf_field,
                    saved_value=value,
                ))
    db.commit()

    # Generate filled PDF
    template_pdf_bytes = _read_bank_form_pdf_bytes(form_template)
    
    # Auto-detect: if FILLABLE_PDF but no interactive fields, switch to SCANNED_FILL
    effective_form_type = form_template.form_type or "FILLABLE_PDF"
    if effective_form_type == "FILLABLE_PDF":
        from app.core.pdf_form_filler import get_pdf_form_fields
        pdf_fields_check = get_pdf_form_fields(template_pdf_bytes)
        if len(pdf_fields_check) == 0:
            _logger.warning(f"Phase 2: Form '{form_template.name}' has no interactive PDF fields — auto-switching to SCANNED_FILL")
            effective_form_type = "SCANNED_FILL"
    
    if effective_form_type == "PHYSICAL_OVERLAY":
        from app.core.pdf_form_filler import generate_overlay_pdf
        fill_lang = req_lang if getattr(form_template, 'form_language', 'BILINGUAL') == 'BILINGUAL' else None
        filled_pdf = generate_overlay_pdf(
            template_pdf_bytes=template_pdf_bytes,
            field_mapping=form_template.field_mapping,
            request_data=request_data,
            lg_language=fill_lang,
        )
    elif effective_form_type == "SCANNED_FILL":
        from app.core.pdf_form_filler import generate_scanned_fill_pdf
        fill_lang = req_lang if getattr(form_template, 'form_language', 'BILINGUAL') == 'BILINGUAL' else None
        filled_pdf = generate_scanned_fill_pdf(
            template_pdf_bytes=template_pdf_bytes,
            field_mapping=form_template.field_mapping,
            request_data=request_data,
            lg_language=fill_lang,
        )
    else:
        fill_lang = req_lang if getattr(form_template, 'form_language', 'BILINGUAL') == 'BILINGUAL' else None
        filled_pdf = fill_pdf_form(
            template_pdf_bytes=template_pdf_bytes,
            field_mapping=form_template.field_mapping,
            request_data=request_data,
            lg_language=fill_lang,
        )
    
    filename = f"Filled_{form_template.name}_{request.serial_number}.pdf"
    
    return StreamingResponse(
        io.BytesIO(filled_pdf),
        media_type="application/pdf",
        headers={
            'Content-Disposition': f'inline; filename="{filename}"',
            'X-Form-Template-Id': str(form_template.id),
            'X-Form-Template-Name': form_template.name,
        }
    )


@router.post("/bank-forms/unlock/{request_id}")
def unlock_bank_form(
    request_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Release the bank lock when the issuance wizard is cancelled without issuing.
    Clears locked_bank_id from metadata and resets locked_for_issuance.
    """
    request = db.query(IssuanceRequest).filter(
        IssuanceRequest.id == request_id,
        IssuanceRequest.customer_id == current_user.customer_id,
    ).first()
    
    if not request:
        raise HTTPException(404, "Request not found.")
    
    # Don't unlock if already issued (has active LG record)
    if request.lg_record_id:
        existing_lg = db.query(IssuedLGRecord).filter(
            IssuedLGRecord.id == request.lg_record_id,
            IssuedLGRecord.status.notin_(("SLA_EXCEEDED", "BANK_REJECTED", "CANCELLED"))
        ).first()
        if existing_lg:
            raise HTTPException(409, "Cannot unlock — this request has already been issued.")
    
    # Clear the bank lock
    meta = dict(request.metadata_json or {})
    meta.pop("locked_bank_id", None)
    request.metadata_json = meta
    request.locked_for_issuance = False
    db.commit()
    
    return {"status": "unlocked", "request_id": request_id}



# ==============================================================================
# 8. POST-ISSUANCE TRACKING (Steps 5.5 + 5.6)
# ==============================================================================

