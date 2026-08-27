# app/api/v1/endpoints/inbox_endpoints.py
"""
Smart Inbox API Endpoints
Endpoints for viewing received emails, reviewing multi-signal classifications,
confirming actions, downloading attachments, triggering outbound requests,
and managing per-bank schedules.
"""

import os
import logging
from typing import List, Optional
from datetime import date
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks, UploadFile, File, Form, Body
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, and_, desc, func

from app.database import get_db
from app.core.security import (
    TokenData, require_email_inbox_feature, get_current_user,
    get_current_corporate_admin_context, UserRole
)
from app.core.encryption import decrypt_data
from app.models.models import Bank, CustomerEmailSetting
from app.models.models_inbox import (
    InboxItem, InboxAttachment, InboxOutboundRequest, InboxScheduleConfig
)
from app.schemas.schemas_inbox import (
    InboxItemListOut, InboxItemDetailOut, InboxItemReclassify, InboxStatsOut,
    OutboundRequestCreate, OutboundRequestOut,
    ScheduleConfigCreate, ScheduleConfigUpdate, ScheduleConfigOut,
    BankDomainUpdate
)
from app.services.inbox_polling_service import inbox_polling_service
from app.services.inbox_classification_service import inbox_classification_service
from app.services.inbox_action_service import inbox_action_service
from app.services.inbox_outbound_service import inbox_outbound_service

logger = logging.getLogger("app.inbox_api")
router = APIRouter()


# ==============================================================================
# 1. INBOX ITEMS
# ==============================================================================

@router.get("/items", response_model=List[InboxItemListOut])
def get_inbox_items(
    classification: Optional[str] = Query(None, description="Filter by classification"),
    status: Optional[str] = Query(None, description="Filter by status (RECEIVED, CLASSIFIED, ACTIONED, ARCHIVED, PARSE_ERROR)"),
    bank_id: Optional[int] = Query(None),
    search: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Retrieves paginated list of inbox items for the current customer."""
    query = db.query(InboxItem).options(
        joinedload(InboxItem.matched_bank)
    ).filter(
        InboxItem.customer_id == current_user.customer_id,
        InboxItem.is_deleted == False
    )

    if classification and classification != "ALL":
        query = query.filter(InboxItem.classification == classification)

    if status and status != "ALL":
        query = query.filter(InboxItem.status == status)

    if bank_id:
        query = query.filter(InboxItem.matched_bank_id == bank_id)

    if search:
        search_pattern = f"%{search}%"
        query = query.filter(
            or_(
                InboxItem.sender_email.ilike(search_pattern),
                InboxItem.subject.ilike(search_pattern),
                InboxItem.primary_attachment_name.ilike(search_pattern)
            )
        )

    items = query.order_by(desc(InboxItem.received_at)).offset(skip).limit(limit).all()

    # Format output with matched_bank_name
    result = []
    for item in items:
        out = InboxItemListOut(
            id=item.id,
            customer_id=item.customer_id,
            sender_email=item.sender_email,
            sender_domain=item.sender_domain,
            subject=item.subject,
            received_at=item.received_at,
            matched_bank_id=item.matched_bank_id,
            matched_bank_name=item.matched_bank.name if item.matched_bank else None,
            is_trusted_sender=item.is_trusted_sender,
            classification=item.classification,
            classification_confidence=item.classification_confidence,
            confidence_score=item.confidence_score,
            has_attachment=item.has_attachment,
            attachment_count=item.attachment_count,
            primary_attachment_name=item.primary_attachment_name,
            status=item.status,
            action_summary=item.action_summary,
            error_message=item.error_message,
            actioned_at=item.actioned_at,
            action_reference_type=item.action_reference_type,
            action_reference_id=item.action_reference_id,
            is_duplicate=item.is_duplicate,
            outbound_request_id=item.outbound_request_id
        )
        result.append(out)

    return result


@router.get("/items/{item_id}", response_model=InboxItemDetailOut)
def get_inbox_item_detail(
    item_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Retrieves full details for a single inbox item, including attachments and preview."""
    item = db.query(InboxItem).options(
        joinedload(InboxItem.matched_bank),
        joinedload(InboxItem.attachments)
    ).filter(
        InboxItem.id == item_id,
        InboxItem.customer_id == current_user.customer_id,
        InboxItem.is_deleted == False
    ).first()

    if not item:
        raise HTTPException(status_code=404, detail="Inbox item not found")

    # Decrypt body preview if available
    body_preview = None
    if item.body_text_encrypted:
        try:
            decrypted = decrypt_data(item.body_text_encrypted)
            body_preview = decrypted[:1000]  # First 1000 characters
        except Exception:
            body_preview = None

    attachments_out = []
    for att in item.attachments:
        if not att.is_deleted:
            attachments_out.append({
                "id": att.id,
                "inbox_item_id": att.inbox_item_id,
                "file_name": att.file_name,
                "file_type": att.file_type,
                "file_size_bytes": att.file_size_bytes,
                "storage_path": att.storage_path,
                "is_primary": att.is_primary,
                "created_at": att.created_at
            })

    return InboxItemDetailOut(
        id=item.id,
        customer_id=item.customer_id,
        message_id=item.message_id,
        sender_email=item.sender_email,
        sender_domain=item.sender_domain,
        subject=item.subject,
        body_preview=body_preview,
        received_at=item.received_at,
        matched_bank_id=item.matched_bank_id,
        matched_bank_name=item.matched_bank.name if item.matched_bank else None,
        is_trusted_sender=item.is_trusted_sender,
        in_reply_to=item.in_reply_to,
        outbound_request_id=item.outbound_request_id,
        classification=item.classification,
        classification_confidence=item.classification_confidence,
        confidence_score=item.confidence_score,
        classification_signals=item.classification_signals,
        user_override_classification=item.user_override_classification,
        has_attachment=item.has_attachment,
        attachment_count=item.attachment_count,
        primary_attachment_name=item.primary_attachment_name,
        primary_attachment_type=item.primary_attachment_type,
        status=item.status,
        action_summary=item.action_summary,
        error_message=item.error_message,
        actioned_at=item.actioned_at,
        actioned_by_user_id=item.actioned_by_user_id,
        action_reference_type=item.action_reference_type,
        action_reference_id=item.action_reference_id,
        is_duplicate=item.is_duplicate,
        duplicate_of_id=item.duplicate_of_id,
        attachments=attachments_out,
        created_at=item.created_at
    )


@router.get("/items/{item_id}/attachment/{attachment_id}")
def download_inbox_attachment(
    item_id: int,
    attachment_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """Downloads an attachment file belonging to an inbox item."""
    # Find item
    item = db.query(InboxItem).filter(
        InboxItem.id == item_id,
        InboxItem.customer_id == current_user.customer_id,
        InboxItem.is_deleted == False
    ).first()

    if not item:
        raise HTTPException(status_code=404, detail="Inbox item not found")

    # Find specific attachment or fallback to primary/first
    att = db.query(InboxAttachment).filter(
        InboxAttachment.inbox_item_id == item_id,
        InboxAttachment.id == attachment_id,
        InboxAttachment.is_deleted == False
    ).first()

    if not att:
        # Fallback to primary or first attachment
        att = db.query(InboxAttachment).filter(
            InboxAttachment.inbox_item_id == item_id,
            InboxAttachment.is_deleted == False
        ).order_by(desc(InboxAttachment.is_primary)).first()

    if att and att.storage_path and os.path.exists(att.storage_path):
        return FileResponse(
            path=att.storage_path,
            filename=att.file_name,
            media_type="application/octet-stream"
        )

    if item.primary_attachment_path and os.path.exists(item.primary_attachment_path):
        filename = item.primary_attachment_name or os.path.basename(item.primary_attachment_path)
        return FileResponse(
            path=item.primary_attachment_path,
            filename=filename,
            media_type="application/octet-stream"
        )

    raise HTTPException(status_code=404, detail="Attachment file not found on disk")


@router.get("/items/{item_id}/attachment/{attachment_id}/preview")
def preview_inbox_attachment(
    item_id: int,
    attachment_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """
    Parses and returns lightweight table preview data for Excel/CSV attachments,
    or metadata for PDF attachments.
    """
    item = db.query(InboxItem).filter(
        InboxItem.id == item_id,
        InboxItem.customer_id == current_user.customer_id,
        InboxItem.is_deleted == False
    ).first()

    if not item:
        raise HTTPException(status_code=404, detail="Inbox item not found")

    att = db.query(InboxAttachment).filter(
        InboxAttachment.inbox_item_id == item_id,
        InboxAttachment.id == attachment_id,
        InboxAttachment.is_deleted == False
    ).first()

    if not att and item.primary_attachment_path:
        file_path = item.primary_attachment_path
        filename = item.primary_attachment_name or "attachment"
        file_type = (item.primary_attachment_type or "unknown").lower()
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
    elif att:
        file_path = att.storage_path
        filename = att.file_name
        file_type = (att.file_type or "").lower()
        file_size = att.file_size_bytes or (os.path.getsize(file_path) if file_path and os.path.exists(file_path) else 0)
    else:
        raise HTTPException(status_code=404, detail="Attachment not found")

    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Attachment file not found on disk")

    ext = os.path.splitext(filename)[1].lower()

    # Handle Excel / CSV previews
    if ext in [".xlsx", ".xls", ".csv"] or file_type in ["xlsx", "xls", "csv"]:
        try:
            import pandas as pd
            if ext == ".csv" or file_type == "csv":
                df = pd.read_csv(file_path, nrows=25)
                sheet_names = ["CSV Data"]
                active_sheet = "CSV Data"
            else:
                xl = pd.ExcelFile(file_path)
                sheet_names = xl.sheet_names
                active_sheet = sheet_names[0] if sheet_names else "Sheet1"
                df = pd.read_excel(file_path, sheet_name=active_sheet, nrows=25)

            # Clean and sanitize for JSON
            df = df.fillna("")
            headers = [str(col) for col in df.columns]
            rows = [[str(val) for val in row] for row in df.values.tolist()]

            return {
                "status": "SUCCESS",
                "preview_type": "TABLE",
                "filename": filename,
                "file_type": ext.lstrip(".").upper(),
                "file_size_bytes": file_size,
                "sheet_names": sheet_names,
                "active_sheet": active_sheet,
                "headers": headers,
                "rows": rows,
                "row_count_preview": len(rows),
                "total_columns": len(headers)
            }
        except Exception as err:
            logger.warning(f"Could not parse preview for file {filename}: {err}")
            return {
                "status": "ERROR",
                "preview_type": "UNSUPPORTED",
                "filename": filename,
                "error": f"Preview unavailable: {str(err)}"
            }

    # Handle PDF or other files
    is_pdf = ext == ".pdf" or file_type == "pdf"
    return {
        "status": "SUCCESS",
        "preview_type": "PDF" if is_pdf else "RAW",
        "filename": filename,
        "file_type": ext.lstrip(".").upper(),
        "file_size_bytes": file_size,
        "is_pdf": is_pdf,
        "download_url": f"/api/v1/inbox/items/{item_id}/attachment/{attachment_id}"
    }


@router.put("/items/{item_id}/reclassify", response_model=InboxItemDetailOut)
def reclassify_inbox_item(
    item_id: int,
    payload: InboxItemReclassify,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Allows user to override or correct the classification of an inbox item."""
    try:
        updated_item = inbox_classification_service.reclassify(
            db=db,
            inbox_item_id=item_id,
            new_classification=payload.classification,
            user_id=current_user.user_id
        )
        return get_inbox_item_detail(item_id=updated_item.id, db=db, current_user=current_user)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/items/{item_id}/confirm")
async def confirm_and_action_item(
    item_id: int,
    override_bank_id: Optional[int] = Body(None, embed=True),
    override_position_date: Optional[date] = Body(None, embed=True),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Confirms an inbox item and executes downstream action (e.g. Reconciliation Session)."""
    return await inbox_action_service.confirm_and_execute(
        db=db,
        inbox_item_id=item_id,
        user_id=current_user.user_id,
        customer_id=current_user.customer_id,
        override_bank_id=override_bank_id,
        override_position_date=override_position_date
    )


@router.post("/items/{item_id}/archive")
def archive_inbox_item(
    item_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Archives/dismisses an inbox item."""
    item = db.query(InboxItem).filter(
        InboxItem.id == item_id,
        InboxItem.customer_id == current_user.customer_id,
        InboxItem.is_deleted == False
    ).first()

    if not item:
        raise HTTPException(status_code=404, detail="Inbox item not found")

    item.status = "ARCHIVED"
    db.commit()
    return {"message": "Inbox item archived successfully", "id": item_id}


@router.post("/items/{item_id}/unarchive")
def unarchive_inbox_item(
    item_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Restores an archived inbox item back to active classified status."""
    item = db.query(InboxItem).filter(
        InboxItem.id == item_id,
        InboxItem.customer_id == current_user.customer_id,
        InboxItem.is_deleted == False
    ).first()

    if not item:
        raise HTTPException(status_code=404, detail="Inbox item not found")

    item.status = "CLASSIFIED"
    db.commit()
    return {"message": "Inbox item unarchived and restored successfully", "id": item_id}


@router.get("/stats", response_model=InboxStatsOut)
def get_inbox_stats(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Returns summary statistics for the Smart Inbox tabs/KPIs."""
    cid = current_user.customer_id
    base_q = db.query(InboxItem).filter(InboxItem.customer_id == cid, InboxItem.is_deleted == False)

    total_received = base_q.count()
    pending_action = base_q.filter(InboxItem.status.in_(["RECEIVED", "CLASSIFIED"]), InboxItem.classification != "IRRELEVANT").count()
    lg_pos = base_q.filter(InboxItem.classification == "LG_POSITION_REPORT", InboxItem.status != "ARCHIVED").count()
    stmt = base_q.filter(InboxItem.classification == "BANK_STATEMENT", InboxItem.status != "ARCHIVED").count()
    prog = base_q.filter(InboxItem.classification == "PROGRESS_REPORT", InboxItem.status != "ARCHIVED").count()
    irrel = base_q.filter(InboxItem.classification == "IRRELEVANT", InboxItem.status != "ARCHIVED").count()
    unclass = base_q.filter(InboxItem.classification == "UNCLASSIFIED", InboxItem.status != "ARCHIVED").count()
    actioned = base_q.filter(InboxItem.status == "ACTIONED").count()
    archived = base_q.filter(InboxItem.status == "ARCHIVED").count()
    errors = base_q.filter(InboxItem.status == "PARSE_ERROR").count()

    return InboxStatsOut(
        total_received=total_received,
        pending_action=pending_action,
        lg_position_count=lg_pos,
        bank_statement_count=stmt,
        progress_report_count=prog,
        irrelevant_count=irrel,
        unclassified_count=unclass,
        actioned_count=actioned,
        archived_count=archived,
        parse_error_count=errors
    )


@router.post("/poll-now")
def trigger_manual_poll(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Triggers an on-demand IMAP poll for the current customer's mailbox."""
    try:
        new_items = inbox_polling_service.poll_customer_mailbox(db, current_user.customer_id)
        return {
            "status": "SUCCESS",
            "new_items_count": len(new_items),
            "message": f"Polled mailbox successfully. Ingested {len(new_items)} new emails." if new_items else "Mailbox polled. No new incoming messages found."
        }
    except Exception as e:
        logger.error(f"Error during on-demand poll: {e}")
        raise HTTPException(status_code=502, detail="Mailbox connection timed out or failed. Please check your credentials or retry in a moment.")


# ==============================================================================
# 2. OUTBOUND DATA REQUESTS
# ==============================================================================

@router.post("/request-position", response_model=OutboundRequestOut)
def send_manual_position_request(
    payload: OutboundRequestCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Manually triggers an LG position request email to a specific bank."""
    try:
        outbound = inbox_outbound_service.send_position_request(
            db=db,
            customer_id=current_user.customer_id,
            bank_id=payload.bank_id,
            position_date=payload.position_date,
            custom_recipients=payload.custom_recipient_emails,
            custom_subject=payload.custom_subject,
            custom_notes=payload.custom_notes,
            user_id=current_user.user_id
        )

        bank = db.query(Bank).filter(Bank.id == payload.bank_id).first()
        return OutboundRequestOut(
            id=outbound.id,
            customer_id=outbound.customer_id,
            bank_id=outbound.bank_id,
            bank_name=bank.name if bank else None,
            request_type=outbound.request_type,
            sent_to_emails=outbound.sent_to_emails,
            subject=outbound.subject,
            message_id=outbound.message_id,
            position_date=outbound.position_date,
            statement_period_start=outbound.statement_period_start,
            statement_period_end=outbound.statement_period_end,
            sent_at=outbound.sent_at,
            sent_by_user_id=outbound.sent_by_user_id,
            is_replied=outbound.is_replied,
            reply_received_at=outbound.reply_received_at,
            is_scheduled=outbound.is_scheduled
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/outbound-requests", response_model=List[OutboundRequestOut])
def get_outbound_requests(
    bank_id: Optional[int] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Lists system-initiated outbound requests sent by the customer."""
    query = db.query(InboxOutboundRequest).options(
        joinedload(InboxOutboundRequest.bank)
    ).filter(
        InboxOutboundRequest.customer_id == current_user.customer_id,
        InboxOutboundRequest.is_deleted == False
    )

    if bank_id:
        query = query.filter(InboxOutboundRequest.bank_id == bank_id)

    reqs = query.order_by(desc(InboxOutboundRequest.sent_at)).offset(skip).limit(limit).all()

    result = []
    for r in reqs:
        result.append(OutboundRequestOut(
            id=r.id,
            customer_id=r.customer_id,
            bank_id=r.bank_id,
            bank_name=r.bank.name if r.bank else None,
            request_type=r.request_type,
            sent_to_emails=r.sent_to_emails,
            subject=r.subject,
            message_id=r.message_id,
            position_date=r.position_date,
            statement_period_start=r.statement_period_start,
            statement_period_end=r.statement_period_end,
            sent_at=r.sent_at,
            sent_by_user_id=r.sent_by_user_id,
            is_replied=r.is_replied,
            reply_received_at=r.reply_received_at,
            is_scheduled=r.is_scheduled
        ))
    return result


from app.schemas.schemas_inbox import (
    InboxItemListOut, InboxItemDetailOut, InboxItemReclassify, InboxStatsOut,
    OutboundRequestCreate, OutboundRequestOut,
    ScheduleConfigCreate, BulkScheduleConfigCreate, ScheduleConfigUpdate, ScheduleConfigOut,
    BankDomainUpdate
)


# ==============================================================================
# 3. SCHEDULE CONFIGURATIONS (Corporate Admin)
# ==============================================================================

@router.get("/banks-summary")
def get_banks_relationship_summary(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Returns smart bank directory with relationship metrics (active LG counts, facilities, accounts)
    and schedule statuses for the current customer.
    """
    from app.models.models import LGRecord
    from app.models.models_issuance import IssuedLGRecord, IssuanceFacility

    cid = current_user.customer_id
    banks = db.query(Bank).filter(Bank.is_deleted == False).order_by(Bank.name.asc()).all()
    schedules = db.query(InboxScheduleConfig).filter(
        InboxScheduleConfig.customer_id == cid,
        InboxScheduleConfig.is_deleted == False
    ).all()
    schedule_map = {s.bank_id: s for s in schedules}

    # Aggregate active LG issuance counts (where customer is applicant)
    issuance_counts = dict(
        db.query(IssuedLGRecord.bank_id, func.count(IssuedLGRecord.id))
        .filter(IssuedLGRecord.customer_id == cid, IssuedLGRecord.status != "CANCELLED")
        .group_by(IssuedLGRecord.bank_id).all()
    )

    # Aggregate credit facility counts
    facility_counts = dict(
        db.query(IssuanceFacility.bank_id, func.count(IssuanceFacility.id))
        .filter(IssuanceFacility.customer_id == cid, IssuanceFacility.is_deleted == False)
        .group_by(IssuanceFacility.bank_id).all()
    )

    result = []
    for b in banks:
        i_count = issuance_counts.get(b.id, 0)
        f_count = facility_counts.get(b.id, 0)
        total_rel = i_count + f_count
        sched = schedule_map.get(b.id)

        result.append({
            "id": b.id,
            "name": b.name,
            "email_domain": b.email_domain,
            "lg_issuance_count": i_count,
            "facility_count": f_count,
            "total_active_records": total_rel,
            "has_relationship": total_rel > 0 or sched is not None,
            "schedule": {
                "id": sched.id,
                "is_active": sched.is_active,
                "frequency": sched.frequency,
                "day_of_month": sched.day_of_month,
                "day_of_week": sched.day_of_week,
                "recipient_emails": sched.recipient_emails,
                "last_sent_at": sched.last_sent_at.isoformat() if sched.last_sent_at else None
            } if sched else None
        })

    return result


@router.post("/schedule-configs/bulk")
def bulk_create_or_update_schedules(
    payload: BulkScheduleConfigCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Applies a recurring schedule to multiple banks in a single action."""
    cid = current_user.customer_id
    updated_ids = []

    for b_id in payload.bank_ids:
        existing = db.query(InboxScheduleConfig).filter(
            InboxScheduleConfig.customer_id == cid,
            InboxScheduleConfig.bank_id == b_id,
            InboxScheduleConfig.request_type == payload.request_type,
            InboxScheduleConfig.is_deleted == False
        ).first()

        if existing:
            existing.is_active = payload.is_active
            existing.frequency = payload.frequency
            existing.day_of_month = payload.day_of_month
            existing.day_of_week = payload.day_of_week
            if payload.custom_subject:
                existing.custom_subject = payload.custom_subject
            if payload.custom_body:
                existing.custom_body = payload.custom_body
            updated_ids.append(existing.id)
        else:
            new_cfg = InboxScheduleConfig(
                customer_id=cid,
                bank_id=b_id,
                request_type=payload.request_type,
                is_active=payload.is_active,
                frequency=payload.frequency,
                day_of_month=payload.day_of_month,
                day_of_week=payload.day_of_week,
                custom_subject=payload.custom_subject,
                custom_body=payload.custom_body
            )
            db.add(new_cfg)
            db.flush()
            updated_ids.append(new_cfg.id)

    db.commit()
    return {
        "status": "SUCCESS",
        "message": f"Applied schedule to {len(payload.bank_ids)} banks successfully.",
        "affected_bank_count": len(payload.bank_ids),
        "config_ids": updated_ids
    }


@router.get("/schedule-configs", response_model=List[ScheduleConfigOut])
def get_schedule_configs(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Lists per-bank outbound request schedules for the customer."""
    configs = db.query(InboxScheduleConfig).options(
        joinedload(InboxScheduleConfig.bank)
    ).filter(
        InboxScheduleConfig.customer_id == current_user.customer_id,
        InboxScheduleConfig.is_deleted == False
    ).all()

    result = []
    for c in configs:
        result.append(ScheduleConfigOut(
            id=c.id,
            customer_id=c.customer_id,
            bank_id=c.bank_id,
            bank_name=c.bank.name if c.bank else None,
            request_type=c.request_type,
            is_active=c.is_active,
            frequency=c.frequency,
            day_of_month=c.day_of_month,
            day_of_week=c.day_of_week,
            recipient_emails=c.recipient_emails,
            custom_subject=c.custom_subject,
            custom_body=c.custom_body,
            last_sent_at=c.last_sent_at,
            created_at=c.created_at
        ))
    return result


@router.post("/schedule-configs", response_model=ScheduleConfigOut)
def create_or_update_schedule_config(
    payload: ScheduleConfigCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Creates or updates a recurring schedule config for a bank."""
    existing = db.query(InboxScheduleConfig).filter(
        InboxScheduleConfig.customer_id == current_user.customer_id,
        InboxScheduleConfig.bank_id == payload.bank_id,
        InboxScheduleConfig.request_type == payload.request_type,
        InboxScheduleConfig.is_deleted == False
    ).first()

    if existing:
        existing.is_active = payload.is_active
        existing.frequency = payload.frequency
        existing.day_of_month = payload.day_of_month
        existing.day_of_week = payload.day_of_week
        existing.recipient_emails = payload.recipient_emails
        existing.custom_subject = payload.custom_subject
        existing.custom_body = payload.custom_body
        config_obj = existing
    else:
        config_obj = InboxScheduleConfig(
            customer_id=current_user.customer_id,
            bank_id=payload.bank_id,
            request_type=payload.request_type,
            is_active=payload.is_active,
            frequency=payload.frequency,
            day_of_month=payload.day_of_month,
            day_of_week=payload.day_of_week,
            recipient_emails=payload.recipient_emails,
            custom_subject=payload.custom_subject,
            custom_body=payload.custom_body
        )
        db.add(config_obj)

    db.commit()
    db.refresh(config_obj)

    bank = db.query(Bank).filter(Bank.id == config_obj.bank_id).first()
    return ScheduleConfigOut(
        id=config_obj.id,
        customer_id=config_obj.customer_id,
        bank_id=config_obj.bank_id,
        bank_name=bank.name if bank else None,
        request_type=config_obj.request_type,
        is_active=config_obj.is_active,
        frequency=config_obj.frequency,
        day_of_month=config_obj.day_of_month,
        day_of_week=config_obj.day_of_week,
        recipient_emails=config_obj.recipient_emails,
        custom_subject=config_obj.custom_subject,
        custom_body=config_obj.custom_body,
        last_sent_at=config_obj.last_sent_at,
        created_at=config_obj.created_at
    )


@router.delete("/schedule-configs/{config_id}")
def delete_schedule_config(
    config_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Soft-deletes a schedule configuration."""
    cfg = db.query(InboxScheduleConfig).filter(
        InboxScheduleConfig.id == config_id,
        InboxScheduleConfig.customer_id == current_user.customer_id,
        InboxScheduleConfig.is_deleted == False
    ).first()

    if not cfg:
        raise HTTPException(status_code=404, detail="Schedule configuration not found")

    cfg.is_deleted = True
    db.commit()
    return {"message": "Schedule configuration deleted successfully", "id": config_id}


# ==============================================================================
# 4. BANK DOMAIN MANAGEMENT
# ==============================================================================

@router.put("/banks/{bank_id}/domain")
def update_bank_domain(
    bank_id: int,
    payload: BankDomainUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """Updates the recognized corporate email domain for a bank."""
    bank = db.query(Bank).filter(Bank.id == bank_id, Bank.is_deleted == False).first()
    if not bank:
        raise HTTPException(status_code=404, detail="Bank not found")

    clean_domain = payload.email_domain.strip().lower().lstrip("@") if payload.email_domain else None
    bank.email_domain = clean_domain
    db.commit()
    return {
        "status": "SUCCESS",
        "bank_id": bank.id,
        "bank_name": bank.name,
        "email_domain": bank.email_domain
    }
