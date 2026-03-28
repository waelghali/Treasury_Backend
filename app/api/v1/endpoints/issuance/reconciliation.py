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
from app.models.models_issuance import IssuedLGRecord, IssuanceRequest, IssuanceFacilitySubLimit, IssuanceFacility, IssuanceWorkflowPolicy, CustomerFormConfiguration, IssuanceRequestSnapshot, IssuanceRequestVersion, AdminChangeRequest, BankFormIssueReport
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
# 5. WORKFLOW CONFIGURATION (The Matrix Engine)
# ==============================================================================

@router.get("/maintenance/{action_id}/document/{doc_type}")
async def get_maintenance_document_url(
    action_id: int,
    doc_type: str,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Generate a signed URL for maintenance action documents.
    doc_type: 'delivery', 'bank_reply', 'bank_initiated', or 'letter'
    For 'letter': regenerates the instruction letter on-the-fly from template + action data.
    """
    action = db.query(IssuanceMaintenanceAction).filter(
        IssuanceMaintenanceAction.id == action_id
    ).first()
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    # ── LETTER: Regenerate on-the-fly (matches LG custody pattern) ──
    if doc_type == "letter":
        if not action.instruction_status:
            raise HTTPException(status_code=404, detail="No instruction letter for this action")

        lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
        if not lg:
            raise HTTPException(status_code=404, detail="LG record not found")

        # Regenerate HTML from template + action_data
        html_content = maintenance_service.regenerate_maintenance_letter_html(db, action, lg)
        if not html_content:
            raise HTTPException(status_code=404, detail="Could not generate instruction letter — template not found")

        from app.core.document_generator import generate_pdf_from_html
        pdf_bytes = await generate_pdf_from_html(
            html_content,
            filename_hint=f"maint_{action.letter_serial_number or action.id}"
        )
        if not pdf_bytes:
            raise HTTPException(status_code=500, detail="Failed to generate PDF")

        from starlette.responses import Response
        filename = f"Maintenance_{action.action_type}_{action.letter_serial_number or action.id}.pdf"
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{filename}"'}
        )

    # ── Other doc types: serve from stored paths ──
    gcs_path = None
    if doc_type == "delivery":
        gcs_path = action.delivery_document_path
    elif doc_type == "bank_reply":
        gcs_path = action.bank_reply_document_path
    elif doc_type == "bank_initiated":
        data = action.action_data or {}
        gcs_path = data.get("bank_document_gcs")
    else:
        raise HTTPException(status_code=400, detail="Invalid document type. Use 'delivery', 'bank_reply', 'bank_initiated', or 'letter'.")

    if not gcs_path:
        raise HTTPException(status_code=404, detail=f"No {doc_type.replace('_', ' ')} document found for this action")

    # Check if it's a local file path (not GCS)
    import os
    if not gcs_path.startswith("gs://") and os.path.isfile(gcs_path):
        with open(gcs_path, "rb") as f:
            content = f.read()
        ext = gcs_path.rsplit(".", 1)[-1].lower()
        media_types = {"pdf": "application/pdf", "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png"}
        from starlette.responses import Response
        return Response(
            content=content,
            media_type=media_types.get(ext, "application/octet-stream"),
            headers={"Content-Disposition": f'inline; filename="{doc_type}_{action_id}.{ext}"'}
        )

    # GCS path — generate signed URL
    from app.core.ai_integration import generate_signed_gcs_url
    signed_url = await generate_signed_gcs_url(gcs_path, expiration=3600)
    if not signed_url:
        raise HTTPException(status_code=500, detail="Could not generate download URL")

    return {"download_url": signed_url, "doc_type": doc_type}


@router.get("/maintenance/{action_id}/serve-letter")
async def serve_maintenance_letter(
    action_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Serve a locally-stored maintenance letter PDF directly."""
    action = db.query(IssuanceMaintenanceAction).filter(
        IssuanceMaintenanceAction.id == action_id
    ).first()
    if not action or not action.letter_generated_path:
        raise HTTPException(status_code=404, detail="Letter not found")

    import os
    path = action.letter_generated_path
    if path.startswith("gs://"):
        raise HTTPException(status_code=400, detail="Use the document endpoint for GCS files")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Local letter file not found on disk")

    from starlette.responses import FileResponse
    return FileResponse(
        path,
        media_type="application/pdf",
        filename=os.path.basename(path),
    )

# ==============================================================================
# LG POSITION RECONCILIATION
# ==============================================================================

from app.services.reconciliation_service import reconciliation_service
from app.models.models_issuance import (
    ReconciliationSession as ReconSession,
    ReconciliationBankRow as ReconBankRow,
    ReconciliationResult as ReconResultModel,
    BankColumnMapping,
)


@router.post("/reconciliation/sessions")
async def create_reconciliation_session(
    bank_id: int = Form(...),
    position_date: str = Form(...),
    notes: Optional[str] = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Create a reconciliation session and upload the bank position report."""
    from datetime import date as date_type
    try:
        pd = date_type.fromisoformat(position_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid position_date format. Use YYYY-MM-DD.")

    import re as _re
    from datetime import date as _date
    _today = _date.today().strftime('%Y%m%d')
    _safe_fname = _re.sub(r'[^\w\-.]', '_', file.filename or 'report')[:60]
    systematic_fname = f"RECON_BANK-{bank_id}_{position_date}_{_today}_{_safe_fname}"
    session = reconciliation_service.create_session(
        db, customer_id=current_user.customer_id, bank_id=bank_id,
        position_date=pd, user_id=current_user.user_id,
        file_name=systematic_fname, notes=notes,
    )

    # Parse the file
    file_bytes = await file.read()
    session = await reconciliation_service.parse_file(
        db, session.id, file_bytes, file.filename,
        customer_id=current_user.customer_id, user_id=current_user.user_id,
    )

    return _serialize_recon_session(session, db)


@router.get("/reconciliation/sessions")
def list_reconciliation_sessions(
    bank_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """List reconciliation sessions for the customer."""
    q = db.query(ReconSession).filter(
        ReconSession.customer_id == current_user.customer_id,
    )
    if bank_id:
        q = q.filter(ReconSession.bank_id == bank_id)
    sessions = q.order_by(ReconSession.created_at.desc()).all()
    return [_serialize_recon_session(s, db, brief=True) for s in sessions]


@router.get("/reconciliation/sessions/{session_id}")
def get_reconciliation_session(
    session_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Get detailed reconciliation session."""
    session = db.query(ReconSession).filter(
        ReconSession.id == session_id,
        ReconSession.customer_id == current_user.customer_id,
    ).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return _serialize_recon_session(session, db)


@router.post("/reconciliation/sessions/{session_id}/match")
def run_reconciliation_matching(
    session_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Run the matching engine on a parsed session."""
    session = reconciliation_service.run_matching(
        db, session_id,
        customer_id=current_user.customer_id,
        user_id=current_user.user_id,
    )
    return _serialize_recon_session(session, db)


@router.get("/reconciliation/sessions/{session_id}/results")
def get_reconciliation_results(
    session_id: int,
    severity: Optional[str] = Query(None),
    mismatch_type: Optional[str] = Query(None),
    resolved: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Get reconciliation results with optional filters."""
    # Verify session access
    session = db.query(ReconSession).filter(
        ReconSession.id == session_id,
        ReconSession.customer_id == current_user.customer_id,
    ).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    q = db.query(ReconResultModel).filter(
        ReconResultModel.session_id == session_id
    )
    if severity:
        q = q.filter(ReconResultModel.severity == severity)
    if mismatch_type:
        q = q.filter(ReconResultModel.mismatch_type == mismatch_type)
    if resolved is not None:
        if resolved:
            q = q.filter(ReconResultModel.user_resolution.isnot(None))
        else:
            q = q.filter(ReconResultModel.user_resolution.is_(None))

    results = q.order_by(
        # HIGH first, then MEDIUM, LOW, INFO
        func.array_position(func.cast(['HIGH', 'MEDIUM', 'LOW', 'INFO'], type_=None),
                            ReconResultModel.severity) if False else ReconResultModel.id
    ).all()

    # Sort by severity manually
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "INFO": 3}
    results.sort(key=lambda r: severity_order.get(r.severity, 99))

    return [_serialize_recon_result(r, db) for r in results]


@router.get("/reconciliation/sessions/{session_id}/bank-rows")
def get_reconciliation_bank_rows(
    session_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Get all parsed bank rows for a session."""
    session = db.query(ReconSession).filter(
        ReconSession.id == session_id,
        ReconSession.customer_id == current_user.customer_id,
    ).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    rows = db.query(ReconBankRow).filter(
        ReconBankRow.session_id == session_id
    ).all()

    return [{
        "id": r.id,
        "bank_lg_number": r.bank_lg_number,
        "beneficiary_name": r.beneficiary_name,
        "amount": str(r.amount) if r.amount else None,
        "currency_code": r.currency_code,
        "issue_date": str(r.issue_date) if r.issue_date else None,
        "expiry_date": str(r.expiry_date) if r.expiry_date else None,
        "match_status": r.match_status,
        "matched_lg_id": r.matched_lg_id,
        "variances": r.variances,
    } for r in rows]


@router.post("/reconciliation/results/{result_id}/resolve")
def resolve_reconciliation_result(
    result_id: int,
    payload: Dict = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Resolve a reconciliation mismatch: ADJUSTED, DISPUTE, or IGNORE."""
    result = reconciliation_service.resolve_result(
        db, result_id,
        resolution=payload.get("resolution", ""),
        notes=payload.get("notes"),
        user_id=current_user.user_id,
        customer_id=current_user.customer_id,
    )
    return _serialize_recon_result(result, db)


@router.post("/reconciliation/results/{result_id}/approve")
def approve_reconciliation_adjustment(
    result_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """Corporate admin approves a reconciliation adjustment — updates the LG record."""
    result = reconciliation_service.approve_adjustment(
        db, result_id,
        admin_user_id=current_user.user_id,
        customer_id=current_user.customer_id,
    )
    return _serialize_recon_result(result, db)


@router.post("/reconciliation/results/{result_id}/reject-approval")
def reject_reconciliation_adjustment(
    result_id: int,
    payload: Dict = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """Corporate admin rejects a reconciliation adjustment."""
    result = reconciliation_service.reject_adjustment(
        db, result_id,
        admin_user_id=current_user.user_id,
        customer_id=current_user.customer_id,
        reason=payload.get("reason"),
    )
    return _serialize_recon_result(result, db)


@router.post("/reconciliation/sessions/{session_id}/complete")
def complete_reconciliation_session(
    session_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Mark a reconciliation session as complete (all items must be resolved)."""
    session = reconciliation_service.complete_session(
        db, session_id,
        user_id=current_user.user_id,
        customer_id=current_user.customer_id,
    )
    serialized = _serialize_recon_session(session, db)
    
    stats = {
        'total': serialized.get('total_bank_records', 0),
        'matched': serialized.get('matched_count', 0),
        'mismatched': serialized.get('mismatched_count', 0),
        'bankOnly': serialized.get('bank_only_count', 0),
        'systemOnly': serialized.get('system_only_count', 0),
    }

    from app.services.issuance_notifications import notify_reconciliation_summary
    background_tasks.add_task(
        notify_reconciliation_summary,
        db=db,
        session_id=session_id,
        bank_name=serialized.get("bank_name", "Unknown Bank"),
        position_date=serialized.get("position_date", ""),
        stats=stats,
        submitter_user_id=current_user.user_id,
        customer_id=current_user.customer_id
    )
    
    return serialized


@router.delete("/reconciliation/sessions/{session_id}")
def delete_reconciliation_session(
    session_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """Delete a reconciliation session and all its associated rows and results."""
    from app.models.models_issuance import ReconciliationSession, ReconciliationBankRow, ReconciliationResult
    session = db.query(ReconciliationSession).filter(
        ReconciliationSession.id == session_id,
        ReconciliationSession.customer_id == current_user.customer_id,
    ).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Cascade delete is usually not set up for these safely, so manual delete
    db.query(ReconciliationResult).filter(ReconciliationResult.session_id == session_id).delete()
    db.query(ReconciliationBankRow).filter(ReconciliationBankRow.session_id == session_id).delete()
    db.delete(session)
    db.commit()
    return {"message": "Session deleted"}


@router.get("/reconciliation/pending-approvals")
def get_pending_reconciliation_approvals(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context),
):
    """Get all reconciliation results pending corporate admin approval."""
    results = db.query(ReconResultModel).join(
        ReconSession,
        ReconResultModel.session_id == ReconSession.id,
    ).filter(
        ReconSession.customer_id == current_user.customer_id,
        ReconResultModel.approval_status == "PENDING_APPROVAL",
    ).all()
    return [_serialize_recon_result(r, db) for r in results]


# ── Serialization helpers ──

@router.post("/reconciliation/check-headers")
def check_reconciliation_headers(
    bank_id: int = Body(...),
    headers: List[str] = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """
    3.3: Compare uploaded file headers against cached bank column mapping.
    Detect column drift (new/missing columns) and warn user before parsing.
    """
    return recon_service.detect_header_drift(
        db, bank_id, current_user.customer_id, headers
    )


@router.post("/reconciliation/re-analyze-mapping")
def re_analyze_reconciliation_mapping(
    bank_id: int = Body(...),
    headers: List[str] = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_treasury_context),
):
    """
    3.3: Clear cached column mapping and re-run keyword analysis on new headers.
    Called after user confirms re-analysis when header drift is detected.
    """
    return recon_service.re_analyze_mapping(
        db, bank_id, current_user.customer_id, headers
    )


# ==============================================================================
# 4.2 TREASURY DASHBOARD STATS
# ==============================================================================

