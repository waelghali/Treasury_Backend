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

@router.get("/action-center/maintenance-pending-delivery")
def issuance_action_center_pending_delivery(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Maintenance letters that have been issued (PDF generated) but not yet
    delivered to the bank — mirrors custody 'undelivered instructions'.
    """
    from app.models.models_issuance import IssuanceMaintenanceAction, IssuedLGRecord

    actions = db.query(IssuanceMaintenanceAction).join(
        IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id
    ).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        IssuanceMaintenanceAction.is_deleted == False,
        IssuanceMaintenanceAction.letter_generated_path.isnot(None),
        IssuanceMaintenanceAction.delivery_date.is_(None),
        IssuanceMaintenanceAction.status.in_(["APPROVED", "EXECUTED"]),
    ).order_by(IssuanceMaintenanceAction.created_at.desc()).all()

    return [
        {
            "id": a.id,
            "action_type": a.action_type,
            "status": a.status,
            "instruction_status": a.instruction_status,
            "serial_number": a.letter_serial_number,
            "issued_lg_id": a.issued_lg_id,
            "lg_number": a.issued_lg.lg_number if a.issued_lg else None,
            "beneficiary": a.issued_lg.beneficiary_name if a.issued_lg else None,
            "created_at": a.created_at.isoformat() if a.created_at else None,
        }
        for a in actions
    ]


@router.get("/action-center/maintenance-awaiting-reply")
def issuance_action_center_awaiting_reply(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Maintenance letters that have been delivered to the bank but are still
    awaiting a bank reply — mirrors custody 'awaiting bank reply'.
    """
    from app.models.models_issuance import IssuanceMaintenanceAction, IssuedLGRecord

    actions = db.query(IssuanceMaintenanceAction).join(
        IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id
    ).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        IssuanceMaintenanceAction.is_deleted == False,
        IssuanceMaintenanceAction.delivery_date.isnot(None),
        IssuanceMaintenanceAction.bank_reply_date.is_(None),
        IssuanceMaintenanceAction.status.in_(["APPROVED", "EXECUTED"]),
    ).order_by(IssuanceMaintenanceAction.delivery_date.desc()).all()

    return [
        {
            "id": a.id,
            "action_type": a.action_type,
            "status": a.status,
            "instruction_status": a.instruction_status,
            "serial_number": a.letter_serial_number,
            "issued_lg_id": a.issued_lg_id,
            "lg_number": a.issued_lg.lg_number if a.issued_lg else None,
            "beneficiary": a.issued_lg.beneficiary_name if a.issued_lg else None,
            "delivery_date": a.delivery_date.isoformat() if a.delivery_date else None,
            "created_at": a.created_at.isoformat() if a.created_at else None,
        }
        for a in actions
    ]


@router.get("/action-center/approaching-expiry")
def issuance_action_center_approaching_expiry(
    days_threshold: int = Query(30, description="Number of days before expiry to flag"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Issued LGs that are approaching expiry within the given threshold —
    prompts the user to consider initiating a CLOSE action.
    """
    from app.models.models_issuance import IssuedLGRecord
    from datetime import date as date_cls, timedelta

    today = date_cls.today()
    horizon = today + timedelta(days=days_threshold)

    lgs = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        IssuedLGRecord.status.in_(["ACTIVE", "LG_ISSUED"]),
        IssuedLGRecord.bank_lg_expiry_date.isnot(None),
        IssuedLGRecord.bank_lg_expiry_date >= today,
        IssuedLGRecord.bank_lg_expiry_date <= horizon,
    ).order_by(IssuedLGRecord.bank_lg_expiry_date.asc()).all()

    return [
        {
            "id": lg.id,
            "lg_number": lg.bank_lg_number or lg.lg_ref_number,
            "beneficiary_name": lg.beneficiary_name,
            "amount": str(lg.current_amount) if lg.current_amount else None,
            "currency_id": lg.currency_id,
            "bank_lg_expiry_date": str(lg.bank_lg_expiry_date) if lg.bank_lg_expiry_date else None,
            "days_to_expiry": (lg.bank_lg_expiry_date - today).days if lg.bank_lg_expiry_date else None,
            "lg_status": lg.status,
            "suggestion": "Consider initiating a CLOSE action for this LG",
        }
        for lg in lgs
    ]

# ── NEW: Unified Issuance Action Center endpoints ──

@router.get("/action-center/approved-requests")
def issuance_action_center_approved_requests(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Issuance requests that have been approved internally but not yet processed
    (no IssuedLGRecord created yet). End-user needs to generate instruction & issue.
    """
    requests = db.query(IssuanceRequest).filter(
        IssuanceRequest.customer_id == current_user.customer_id,
        IssuanceRequest.status == "APPROVED_INTERNAL",
        IssuanceRequest.is_deleted == False,
    ).order_by(IssuanceRequest.updated_at.desc()).all()

    return [
        {
            "id": r.id,
            "serial_number": r.serial_number,
            "beneficiary_name": r.beneficiary_name,
            "amount": str(r.amount) if r.amount else None,
            "currency_id": r.currency_id,
            "lg_type_id": r.lg_type_id,
            "department": r.department,
            "approved_at": r.updated_at.isoformat() if r.updated_at else None,
            "type": "issuance_request",
        }
        for r in requests
    ]


@router.get("/action-center/approved-maintenance")
def issuance_action_center_approved_maintenance(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    Maintenance actions that have been approved but not yet executed.
    End-user needs to generate instruction letter & execute.
    """
    from app.models.models_issuance import IssuanceMaintenanceAction

    actions = db.query(IssuanceMaintenanceAction).join(
        IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id
    ).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        IssuanceMaintenanceAction.status == "APPROVED",
        IssuanceMaintenanceAction.is_deleted == False,
    ).order_by(IssuanceMaintenanceAction.updated_at.desc()).all()

    return [
        {
            "id": a.id,
            "action_type": a.action_type,
            "serial_number": a.letter_serial_number,
            "issued_lg_id": a.issued_lg_id,
            "lg_number": (a.issued_lg.bank_lg_number or a.issued_lg.lg_ref_number) if a.issued_lg else None,
            "beneficiary": a.issued_lg.beneficiary_name if a.issued_lg else None,
            "approved_at": a.updated_at.isoformat() if a.updated_at else None,
            "type": "maintenance",
        }
        for a in actions
    ]


@router.get("/action-center/unified-pending-delivery")
def issuance_action_center_unified_pending_delivery(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    UNIFIED: Items with instructions generated but not yet delivered to bank.
    Combines IssuedLGRecords (INTERNAL_PROCESSING, no delivery_date)
    + Maintenance actions (letter generated, no delivery_date).
    """
    from app.models.models_issuance import IssuanceMaintenanceAction

    # 1. Issuance LGs pending delivery
    lgs = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        IssuedLGRecord.status == "INTERNAL_PROCESSING",
        IssuedLGRecord.delivery_date.is_(None),
    ).order_by(IssuedLGRecord.created_at.desc()).all()

    # 2. Maintenance actions pending delivery
    maint = db.query(IssuanceMaintenanceAction).join(
        IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id
    ).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        IssuanceMaintenanceAction.is_deleted == False,
        or_(
            IssuanceMaintenanceAction.letter_generated_path.isnot(None),
            IssuanceMaintenanceAction.is_printed == True,
            IssuanceMaintenanceAction.instruction_status.in_(["Instruction Issued", "Printed"]),
        ),
        IssuanceMaintenanceAction.delivery_date.is_(None),
        IssuanceMaintenanceAction.status.in_(["APPROVED", "EXECUTED"]),
    ).order_by(IssuanceMaintenanceAction.created_at.desc()).all()

    results = []
    for lg in lgs:
        results.append({
            "id": lg.id,
            "source": "issuance",
            "lg_number": lg.bank_lg_number or lg.lg_ref_number,
            "beneficiary": lg.beneficiary_name,
            "amount": str(lg.current_amount) if lg.current_amount else None,
            "currency_id": lg.currency_id,
            "created_at": lg.created_at.isoformat() if lg.created_at else None,
            "status": lg.status,
            "action_type": "NEW_ISSUANCE",
        })
    for a in maint:
        results.append({
            "id": a.id,
            "source": "maintenance",
            "issued_lg_id": a.issued_lg_id,
            "lg_number": (a.issued_lg.bank_lg_number or a.issued_lg.lg_ref_number) if a.issued_lg else None,
            "beneficiary": a.issued_lg.beneficiary_name if a.issued_lg else None,
            "amount": None,
            "currency_id": None,
            "created_at": a.created_at.isoformat() if a.created_at else None,
            "status": a.status,
            "action_type": a.action_type,
            "serial_number": a.letter_serial_number,
        })
    return results


@router.get("/action-center/unified-pending-bank-reply")
def issuance_action_center_unified_pending_bank_reply(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_issuance_read_context),
):
    """
    UNIFIED: Items delivered to bank but awaiting reply.
    Combines IssuedLGRecords (delivered, no bank_reply)
    + Maintenance actions (delivered, no bank_reply).
    """
    from app.models.models_issuance import IssuanceMaintenanceAction

    # 1. Issuance LGs awaiting bank reply
    lgs = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        IssuedLGRecord.delivery_date.isnot(None),
        IssuedLGRecord.bank_reply_date.is_(None),
        IssuedLGRecord.status.in_(["INTERNAL_PROCESSING", "DELIVERED_TO_BANK"]),
    ).order_by(IssuedLGRecord.delivery_date.desc()).all()

    # 2. Maintenance actions awaiting bank reply
    maint = db.query(IssuanceMaintenanceAction).join(
        IssuedLGRecord, IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id
    ).filter(
        IssuedLGRecord.customer_id == current_user.customer_id,
        IssuanceMaintenanceAction.is_deleted == False,
        IssuanceMaintenanceAction.delivery_date.isnot(None),
        IssuanceMaintenanceAction.bank_reply_date.is_(None),
        IssuanceMaintenanceAction.status.in_(["APPROVED", "EXECUTED"]),
    ).order_by(IssuanceMaintenanceAction.delivery_date.desc()).all()

    results = []
    for lg in lgs:
        days_waiting = (date.today() - lg.delivery_date).days if lg.delivery_date else 0
        results.append({
            "id": lg.id,
            "source": "issuance",
            "lg_number": lg.bank_lg_number or lg.lg_ref_number,
            "beneficiary": lg.beneficiary_name,
            "delivery_date": lg.delivery_date.isoformat() if lg.delivery_date else None,
            "days_waiting": days_waiting,
            "action_type": "NEW_ISSUANCE",
        })
    for a in maint:
        days_waiting = (date.today() - a.delivery_date.date()).days if a.delivery_date else 0
        results.append({
            "id": a.id,
            "source": "maintenance",
            "lg_number": (a.issued_lg.bank_lg_number or a.issued_lg.lg_ref_number) if a.issued_lg else None,
            "beneficiary": a.issued_lg.beneficiary_name if a.issued_lg else None,
            "delivery_date": a.delivery_date.isoformat() if a.delivery_date else None,
            "days_waiting": days_waiting,
            "action_type": a.action_type,
            "serial_number": a.letter_serial_number,
        })
    return results


