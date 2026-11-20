# app/api/v1/endpoints/inbox_endpoints.py
"""
Smart Inbox API Endpoints
Endpoints for viewing received emails, reviewing multi-signal classifications,
confirming actions (LG positions, bank statements, cash balances),
downloading/previewing GCS or local attachments, triggering outbound requests,
managing per-bank schedules, generating standing instruction letters, and
syncing downloaded statements from a local Watched Folder.
"""

import hashlib
import io
import logging
import os
import re
import secrets
from datetime import date, datetime, timezone
from html import escape as html_escape
from typing import Any, Dict, List, Optional, Tuple

from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    Form,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
)
from fastapi.responses import StreamingResponse
from sqlalchemy import desc, func, or_
from sqlalchemy.orm import Session, joinedload

from app.core.encryption import decrypt_data
from app.core.security import (
    TokenData,
    get_current_corporate_admin_context,
    get_current_user,
    require_email_inbox_feature,
    verify_access_token,
)
from app.core.storage_service import (
    build_customer_blob_path,
    read_attachment_bytes,
    upload_bytes_to_gcs_sync,
)
from app.database import get_db
from app.models.models import Bank, Customer, CustomerEmailSetting, User
from app.models.models_inbox import (
    InboxAttachment,
    InboxItem,
    InboxOutboundRequest,
    InboxScheduleConfig,
    InboxSyncToken,
)
from app.models.models_issuance import CustomerBankAccount, IssuanceFacility, IssuedLGRecord
from app.schemas.schemas_inbox import (
    BankDomainUpdate,
    BulkScheduleConfigCreate,
    InboxItemDetailOut,
    InboxItemListOut,
    InboxItemReclassify,
    InboxStatsOut,
    OutboundRequestCreate,
    OutboundRequestOut,
    ScheduleConfigCreate,
    ScheduleConfigOut,
    SyncTokenCreate,
    SyncTokenOut,
)
from app.services.inbox_action_service import inbox_action_service
from app.services.inbox_classification_service import inbox_classification_service
from app.services.inbox_outbound_service import inbox_outbound_service
from app.services.inbox_polling_service import (
    ALLOWED_ATTACHMENT_EXTENSIONS,
    inbox_polling_service,
)

logger = logging.getLogger("app.inbox_api")
router = APIRouter()

EGYPTIAN_BANK_FILENAME_ALIASES: List[Tuple[str, List[str]]] = [
    ("cib", ["commercial international bank", "cib"]),
    ("nbe", ["national bank of egypt", "nbe"]),
    ("misr", ["banque misr", "bank misr"]),
    ("qnb", ["qnb", "qatar national"]),
    ("hsbc", ["hsbc"]),
    ("fab", ["first abu dhabi", "fab misr", "fab"]),
    ("citi", ["citibank", "citi"]),
    ("aaib", ["arab african", "aaib"]),
    ("alex", ["bank of alexandria", "alexbank"]),
    ("adcb", ["abu dhabi commercial", "adcb"]),
    ("enbd", ["emirates nbd", "enbd"]),
    ("scb", ["standard chartered"]),
    ("credit agricole", ["credit agricole", "cae"]),
    ("faisal", ["faisal islamic"]),
    ("abk", ["al ahli bank of kuwait", "abk"]),
    ("midbank", ["midbank"]),
    ("suez", ["suez canal bank"]),
    ("arab bank", ["arab bank"]),
    ("hdb", ["housing and development", "hdb"]),
    ("caire", ["banque du caire", "bdc"]),
    ("adib", ["abu dhabi islamic", "adib"]),
    ("attijari", ["attijariwafa"]),
    ("egbank", ["egyptian gulf", "egbank", "egb"]),
    ("saib", ["saib"]),
]


def _detect_bank_id_from_filename_or_bytes(
    db: Session,
    filename: str,
    raw_bytes: bytes,
) -> Optional[int]:
    """Best-effort Egyptian bank auto-detection from filename tokens or SWIFT/IBAN hints."""
    banks = db.query(Bank).filter(Bank.is_deleted == False).all()
    fn_low = re.sub(r"[^a-z0-9]+", " ", (filename or "").lower())

    for b in banks:
        if b.short_name and re.search(rf"\b{re.escape(b.short_name.lower())}\b", fn_low):
            return b.id
        if b.name and b.name.lower() in fn_low:
            return b.id

    for token, name_hints in EGYPTIAN_BANK_FILENAME_ALIASES:
        if re.search(rf"\b{re.escape(token)}\b", fn_low):
            for b in banks:
                b_hay = f"{b.name or ''} {b.short_name or ''}".lower()
                if any(h in b_hay for h in name_hints):
                    return b.id

    # Fallback: check IBAN / SWIFT code inside first 4KB of text-based files
    try:
        head_txt = raw_bytes[:4096].decode("utf-8", errors="ignore").upper()
        for b in banks:
            if b.swift_code and len(b.swift_code) >= 6 and b.swift_code[:6].upper() in head_txt:
                return b.id
    except Exception:
        pass

    return None


def _resolve_sync_actor(
    request: Request,
    x_grow_sync_token: Optional[str],
    db: Session,
) -> Tuple[int, int, Optional[InboxSyncToken]]:
    """
    Authenticates `/sync-drop` using either a scoped `X-Grow-Sync-Token` header
    (for local Windows/PowerShell folder watchers) or a standard user Bearer JWT.
    """
    if x_grow_sync_token:
        tok_hash = hashlib.sha256(x_grow_sync_token.strip().encode("utf-8")).hexdigest()
        sync_tok = (
            db.query(InboxSyncToken)
            .filter(
                InboxSyncToken.token_hash == tok_hash,
                InboxSyncToken.is_active == True,
                InboxSyncToken.is_deleted == False,
            )
            .first()
        )
        if not sync_tok:
            raise HTTPException(status_code=401, detail="Invalid or revoked sync token.")
        sync_tok.last_used_at = datetime.now(timezone.utc)
        return sync_tok.customer_id, sync_tok.created_by_user_id, sync_tok

    auth_hdr = request.headers.get("authorization") or ""
    if auth_hdr.lower().startswith("bearer "):
        raw_jwt = auth_hdr.split(" ", 1)[1].strip()
        payload = verify_access_token(raw_jwt)
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid access token.")
        user = db.query(User).filter(User.id == user_id, User.is_deleted == False).first()
        if not user or not user.customer_id:
            raise HTTPException(status_code=403, detail="User is not associated with a corporate tenant.")
        return user.customer_id, user.id, None

    raise HTTPException(
        status_code=401,
        detail="Authentication required: supply X-Grow-Sync-Token or Bearer token.",
    )


# ==============================================================================
# 1. INBOX ITEMS & LIVE CASH POSITIONS
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
            is_nil_position=bool(item.is_nil_position),
            extracted_balances_json=item.extracted_balances_json,
            ingestion_channel=item.ingestion_channel or "IMAP_EMAIL",
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

    body_preview = None
    if item.body_text_encrypted:
        try:
            decrypted = decrypt_data(item.body_text_encrypted)
            body_preview = decrypted[:2000]
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
        is_nil_position=bool(item.is_nil_position),
        extracted_balances_json=item.extracted_balances_json,
        ingestion_channel=item.ingestion_channel or "IMAP_EMAIL",
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
    """Downloads an attachment file belonging to an inbox item (supports GCS and local storage)."""
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

    if not att:
        att = db.query(InboxAttachment).filter(
            InboxAttachment.inbox_item_id == item_id,
            InboxAttachment.is_deleted == False
        ).order_by(desc(InboxAttachment.is_primary)).first()

    target_path = att.storage_path if att else item.primary_attachment_path
    filename = (att.file_name if att else item.primary_attachment_name) or "attachment"
    safe_dl_name = re.sub(r'["\\\r\n]', "_", filename)

    raw_bytes = read_attachment_bytes(target_path)
    if not raw_bytes:
        raise HTTPException(status_code=404, detail="Attachment file not found in storage")

    return StreamingResponse(
        io.BytesIO(raw_bytes),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{safe_dl_name}"'},
    )


@router.get("/items/{item_id}/attachment/{attachment_id}/preview")
def preview_inbox_attachment(
    item_id: int,
    attachment_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
):
    """
    Parses and returns lightweight table preview data for Excel/CSV attachments,
    or metadata for PDF/SWIFT attachments (supports GCS and local storage).
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
    elif att:
        file_path = att.storage_path
        filename = att.file_name
        file_type = (att.file_type or "").lower()
    else:
        raise HTTPException(status_code=404, detail="Attachment not found")

    raw_bytes = read_attachment_bytes(file_path)
    if not raw_bytes:
        raise HTTPException(status_code=404, detail="Attachment file not found in storage")

    file_size = len(raw_bytes)
    ext = os.path.splitext(filename)[1].lower()

    if ext in [".xlsx", ".xls", ".xlsb", ".csv"] or file_type in ["xlsx", "xls", "xlsb", "csv"]:
        try:
            import pandas as pd
            if ext == ".csv" or file_type == "csv":
                df = pd.read_csv(io.BytesIO(raw_bytes), nrows=25)
                sheet_names = ["CSV Data"]
                active_sheet = "CSV Data"
            else:
                xl = pd.ExcelFile(io.BytesIO(raw_bytes))
                sheet_names = xl.sheet_names
                active_sheet = sheet_names[0] if sheet_names else "Sheet1"
                df = xl.parse(active_sheet, nrows=25)

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
    pdf_password: Optional[str] = Body(None, embed=True),
    save_pdf_password: bool = Body(False, embed=True),
    override_balances: Optional[List[Dict[str, Any]]] = Body(None, embed=True),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """
    Confirms an inbox item and executes downstream action
    (LG Reconciliation, Bank Statement Ingestion + Cash Balance Snapshot, or Inline Cash Position update).
    """
    return await inbox_action_service.confirm_and_execute(
        db=db,
        inbox_item_id=item_id,
        user_id=current_user.user_id,
        customer_id=current_user.customer_id,
        override_bank_id=override_bank_id,
        override_position_date=override_position_date,
        pdf_password=pdf_password,
        save_pdf_password=save_pdf_password,
        override_balances=override_balances,
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

    return InboxStatsOut(
        total_received=base_q.count(),
        pending_action=base_q.filter(
            InboxItem.status.in_(["RECEIVED", "CLASSIFIED"]),
            InboxItem.classification != "IRRELEVANT",
        ).count(),
        lg_position_count=base_q.filter(InboxItem.classification == "LG_POSITION_REPORT", InboxItem.status != "ARCHIVED").count(),
        bank_statement_count=base_q.filter(InboxItem.classification == "BANK_STATEMENT", InboxItem.status != "ARCHIVED").count(),
        cash_position_count=base_q.filter(InboxItem.classification == "CASH_POSITION_SNAPSHOT", InboxItem.status != "ARCHIVED").count(),
        progress_report_count=base_q.filter(InboxItem.classification == "PROGRESS_REPORT", InboxItem.status != "ARCHIVED").count(),
        irrelevant_count=base_q.filter(InboxItem.classification == "IRRELEVANT", InboxItem.status != "ARCHIVED").count(),
        unclassified_count=base_q.filter(InboxItem.classification == "UNCLASSIFIED", InboxItem.status != "ARCHIVED").count(),
        actioned_count=base_q.filter(InboxItem.status == "ACTIONED").count(),
        archived_count=base_q.filter(InboxItem.status == "ARCHIVED").count(),
        parse_error_count=base_q.filter(InboxItem.status == "PARSE_ERROR").count(),
    )


@router.get("/cash-positions")
def get_inbox_cash_positions(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature),
):
    """Returns consolidated live multi-bank cash balances across all ingestion channels."""
    return inbox_action_service.get_live_cash_positions(db, current_user.customer_id)


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
# 2. OUTBOUND DATA REQUESTS & STANDING INSTRUCTION LETTERS
# ==============================================================================

@router.post("/request-position", response_model=OutboundRequestOut)
def send_manual_position_request(
    payload: OutboundRequestCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature)
):
    """Manually triggers an LG Position, Bank Statement, or Cash Balance request email to a specific bank."""
    try:
        outbound = inbox_outbound_service.send_position_request(
            db=db,
            customer_id=current_user.customer_id,
            bank_id=payload.bank_id,
            position_date=payload.position_date,
            custom_recipients=payload.custom_recipient_emails,
            custom_subject=payload.custom_subject,
            custom_notes=payload.custom_notes,
            user_id=current_user.user_id,
            request_type=payload.request_type or "LG_POSITION",
            statement_period_start=payload.statement_period_start,
            statement_period_end=payload.statement_period_end,
        )

        bank = db.query(Bank).filter(Bank.id == payload.bank_id).first()
        return OutboundRequestOut(
            id=outbound.id,
            customer_id=outbound.customer_id,
            bank_id=outbound.bank_id,
            bank_name=bank.name if bank else None,
            request_type=outbound.request_type,
            request_reference=outbound.request_reference,
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

    return [
        OutboundRequestOut(
            id=r.id,
            customer_id=r.customer_id,
            bank_id=r.bank_id,
            bank_name=r.bank.name if r.bank else None,
            request_type=r.request_type,
            request_reference=r.request_reference,
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
        )
        for r in reqs
    ]


@router.get("/standing-instruction-letter/{bank_id}")
def get_standing_instruction_letter(
    bank_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature),
):
    """
    Generates a printable bilingual (English + Arabic) Corporate Standing Instruction Letter
    for the customer's Relationship Manager at any Egyptian bank to enable automated daily
    email statement / MT940 / balance delivery to the customer's Smart Inbox email.
    """
    customer = db.query(Customer).filter(
        Customer.id == current_user.customer_id,
        Customer.is_deleted == False,
    ).first()
    bank = db.query(Bank).filter(Bank.id == bank_id, Bank.is_deleted == False).first()
    if not customer or not bank:
        raise HTTPException(status_code=404, detail="Customer or Bank not found")

    email_setting = db.query(CustomerEmailSetting).filter(
        CustomerEmailSetting.customer_id == current_user.customer_id,
        CustomerEmailSetting.is_deleted == False,
    ).first()
    target_inbox_email = (
        (email_setting.imap_username if email_setting and email_setting.imap_username and "@" in email_setting.imap_username else None)
        or (email_setting.sender_email if email_setting and email_setting.sender_email else None)
        or "treasury@company.com"
    )

    accounts = db.query(CustomerBankAccount).filter(
        CustomerBankAccount.customer_id == current_user.customer_id,
        CustomerBankAccount.bank_id == bank_id,
        CustomerBankAccount.is_active == True,
        CustomerBankAccount.is_deleted == False,
    ).all()

    account_rows_html = "".join(
        f"<tr>"
        f"<td style='padding:8px;border:1px solid #cbd5e1;'>{html_escape(a.account_name)}</td>"
        f"<td style='padding:8px;border:1px solid #cbd5e1;font-family:monospace;'>{html_escape(a.account_number)}</td>"
        f"<td style='padding:8px;border:1px solid #cbd5e1;font-family:monospace;'>{html_escape(a.iban or '—')}</td>"
        f"<td style='padding:8px;border:1px solid #cbd5e1;'>{html_escape(a.currency_code or 'EGP')}</td>"
        f"<td style='padding:8px;border:1px solid #cbd5e1;font-family:monospace;'>{html_escape(a.customer_number or '—')}</td>"
        f"</tr>"
        for a in accounts
    ) or (
        "<tr><td colspan='5' style='padding:8px;border:1px solid #cbd5e1;color:#64748b;'>"
        "All active corporate accounts held under our CIF at your bank / كافة الحسابات القائمة للشركة طرفكم</td></tr>"
    )

    today_str = date.today().strftime("%d %B %Y")
    html_letter = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Standing Instruction - Automated Bank Statement Delivery ({html_escape(bank.name)})</title>
<style>
  body {{ font-family: 'Segoe UI', Tahoma, Arial, sans-serif; color: #0f172a; line-height: 1.55; margin: 28px; }}
  .doc {{ max-width: 820px; margin: auto; }}
  .hdr {{ border-bottom: 2px solid #1e3a8a; padding-bottom: 10px; margin-bottom: 18px; display:flex; justify-content:space-between; align-items:flex-end; }}
  h1 {{ font-size: 18px; margin: 0; color: #1e3a8a; }}
  .meta {{ font-size: 12px; color: #475569; }}
  table {{ width: 100%; border-collapse: collapse; margin: 14px 0; font-size: 13px; }}
  th {{ background: #f1f5f9; text-align: left; padding: 8px; border: 1px solid #cbd5e1; }}
  .box {{ background: #eff6ff; border-left: 4px solid #2563eb; padding: 10px 14px; margin: 12px 0; font-size: 13px; }}
  .ar {{ direction: rtl; text-align: right; border-top: 2px dashed #cbd5e1; margin-top: 24px; padding-top: 18px; }}
  .sigs {{ display: flex; justify-content: space-between; margin-top: 32px; font-size: 13px; }}
  .sig-line {{ border-top: 1px solid #334155; width: 220px; padding-top: 6px; }}
  @media print {{ body {{ margin: 12px; }} }}
</style>
</head>
<body>
<div class="doc">
  <div class="hdr">
    <div>
      <h1>STANDING INSTRUCTION: AUTOMATED DAILY STATEMENT &amp; BALANCE DELIVERY</h1>
      <div class="meta">Company: <strong>{html_escape(customer.name)}</strong> · Date: {today_str}</div>
    </div>
    <div class="meta">To: <strong>{html_escape(bank.name)}</strong></div>
  </div>

  <p>Dear Corporate Relationship Manager &amp; Cash Management Operations Team at <strong>{html_escape(bank.name)}</strong>,</p>
  <p>We kindly instruct your bank to enable automated daily End-of-Day (and intraday, where available) electronic statement and balance delivery for our company's accounts listed below:</p>

  <table>
    <thead>
      <tr><th>Account Name</th><th>Account Number</th><th>IBAN</th><th>Currency</th><th>CIF #</th></tr>
    </thead>
    <tbody>{account_rows_html}</tbody>
  </table>

  <div class="box">
    <div><strong>Authorized Delivery Email (Treasury Inbox):</strong> <span style="font-family:monospace;">{html_escape(target_inbox_email)}</span></div>
    <div><strong>Preferred Formats:</strong> SWIFT MT940 (.sta / .mt940), Excel (.xlsx / .csv), or PDF Balance &amp; Statement Report</div>
    <div><strong>Frequency:</strong> Automated Daily (by 08:30 AM Cairo Time)</div>
  </div>

  <p>Please ensure that any static encryption password required for PDF e-statements is shared separately with our Authorized Signatory so our Treasury Management System can ingest statements automatically.</p>

  <div class="ar">
    <h2 style="font-size:17px; color:#1e3a8a; margin-top:0;">تعليمات مستديمة: إرسال كشوف الحساب والأرصدة اليومية آلياً</h2>
    <p>السادة / مدير علاقات الشركات وإدارة عمليات الخزانة بـ <strong>{html_escape(bank.name)}</strong>، تحية طيبة وبعد،،،</p>
    <p>يرجى التكرم بتفعيل خدمة الإرسال الآلي اليومي لكشوف الحسابات والأرصدة الختامية الخاصة بشركة <strong>{html_escape(customer.name)}</strong> للحسابات الموضحة في الجدول أعلاه، وذلك على البريد الإلكتروني المعتمد لإدارة الخزانة:</p>
    <p style="font-weight:bold; font-family:monospace; direction:ltr; text-align:center; background:#f8fafc; padding:8px; border:1px solid #cbd5e1;">{html_escape(target_inbox_email)}</p>
    <p>بصيغة (MT940 / Excel / CSV / PDF) يومياً في تمام الساعة الثامنة والنصف صباحاً بتوقيت القاهرة.</p>
  </div>

  <div class="sigs">
    <div class="sig-line">Authorized Signatory / المفوض بالتوقيع<br/>Name / الاسم:<br/>Date / التاريخ:</div>
    <div class="sig-line">Company Stamp / ختم الشركة</div>
  </div>
</div>
</body>
</html>"""

    return {
        "bank_id": bank.id,
        "bank_name": bank.name,
        "customer_name": customer.name,
        "target_inbox_email": target_inbox_email,
        "accounts": [
            {
                "id": a.id,
                "account_name": a.account_name,
                "account_number": a.account_number,
                "iban": a.iban,
                "currency_code": a.currency_code or "EGP",
                "customer_number": a.customer_number,
            }
            for a in accounts
        ],
        "html_letter": html_letter,
    }


# ==============================================================================
# 3. SCHEDULE CONFIGURATIONS (Corporate Admin)
# ==============================================================================

@router.get("/banks-summary")
def get_banks_relationship_summary(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Returns smart bank directory with relationship metrics (active LG counts, facilities, bank accounts)
    and schedule statuses per request_type for the current customer.
    """
    cid = current_user.customer_id
    banks = db.query(Bank).filter(Bank.is_deleted == False).order_by(Bank.name.asc()).all()
    schedules = db.query(InboxScheduleConfig).filter(
        InboxScheduleConfig.customer_id == cid,
        InboxScheduleConfig.is_deleted == False
    ).all()

    schedules_by_bank: Dict[int, Dict[str, Any]] = {}
    for s in schedules:
        sched_dict = {
            "id": s.id,
            "request_type": s.request_type,
            "is_active": s.is_active,
            "frequency": s.frequency,
            "day_of_month": s.day_of_month,
            "day_of_week": s.day_of_week,
            "recipient_emails": s.recipient_emails,
            "custom_subject": s.custom_subject,
            "custom_body": s.custom_body,
            "last_sent_at": s.last_sent_at.isoformat() if s.last_sent_at else None,
        }
        schedules_by_bank.setdefault(s.bank_id, {})[s.request_type] = sched_dict

    issuance_counts = dict(
        db.query(IssuedLGRecord.bank_id, func.count(IssuedLGRecord.id))
        .filter(IssuedLGRecord.customer_id == cid, IssuedLGRecord.status != "CANCELLED")
        .group_by(IssuedLGRecord.bank_id).all()
    )

    facility_counts = dict(
        db.query(IssuanceFacility.bank_id, func.count(IssuanceFacility.id))
        .filter(IssuanceFacility.customer_id == cid, IssuanceFacility.is_deleted == False)
        .group_by(IssuanceFacility.bank_id).all()
    )

    account_counts = dict(
        db.query(CustomerBankAccount.bank_id, func.count(CustomerBankAccount.id))
        .filter(
            CustomerBankAccount.customer_id == cid,
            CustomerBankAccount.is_deleted == False,
            CustomerBankAccount.is_active == True,
        )
        .group_by(CustomerBankAccount.bank_id).all()
    )

    result = []
    for b in banks:
        i_count = issuance_counts.get(b.id, 0)
        f_count = facility_counts.get(b.id, 0)
        a_count = account_counts.get(b.id, 0)
        total_rel = i_count + f_count + a_count
        bank_scheds = schedules_by_bank.get(b.id, {})
        primary_sched = (
            bank_scheds.get("LG_POSITION")
            or bank_scheds.get("BANK_STATEMENT")
            or bank_scheds.get("CASH_POSITION")
        )

        result.append({
            "id": b.id,
            "name": b.name,
            "email_domain": b.email_domain,
            "lg_issuance_count": i_count,
            "facility_count": f_count,
            "bank_account_count": a_count,
            "total_active_records": total_rel,
            "has_relationship": total_rel > 0 or bool(bank_scheds),
            "schedule": primary_sched,
            "schedules_by_type": bank_scheds,
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
        "message": f"Applied {payload.request_type} schedule to {len(payload.bank_ids)} banks successfully.",
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

    return [
        ScheduleConfigOut(
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
        )
        for c in configs
    ]


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


# ==============================================================================
# 5. WATCHED-FOLDER AUTO-SYNC (PORTAL-ONLY EGYPTIAN BANKS FALLBACK)
# ==============================================================================

@router.get("/sync-tokens", response_model=List[SyncTokenOut])
def list_sync_tokens(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature),
):
    """Lists active Watched-Folder sync tokens for the current customer."""
    tokens = (
        db.query(InboxSyncToken)
        .filter(
            InboxSyncToken.customer_id == current_user.customer_id,
            InboxSyncToken.is_deleted == False,
        )
        .order_by(desc(InboxSyncToken.id))
        .all()
    )
    return tokens


@router.post("/sync-tokens", response_model=SyncTokenOut)
def create_sync_token(
    payload: SyncTokenCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature),
):
    """Generates a new scoped Watched-Folder sync token (plaintext returned once)."""
    raw_token = f"gws_{secrets.token_hex(20)}"
    token_hash = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
    tok = InboxSyncToken(
        customer_id=current_user.customer_id,
        created_by_user_id=current_user.user_id,
        label=payload.label.strip() or "Treasury Workstation Sync",
        token_prefix=raw_token[:12],
        token_hash=token_hash,
        is_active=True,
    )
    db.add(tok)
    db.commit()
    db.refresh(tok)

    return SyncTokenOut(
        id=tok.id,
        label=tok.label,
        token_prefix=tok.token_prefix,
        is_active=tok.is_active,
        last_used_at=tok.last_used_at,
        files_synced_count=tok.files_synced_count,
        created_at=tok.created_at,
        plaintext_token=raw_token,
    )


@router.delete("/sync-tokens/{token_id}")
def revoke_sync_token(
    token_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(require_email_inbox_feature),
):
    """Revokes and soft-deletes a Watched-Folder sync token."""
    tok = db.query(InboxSyncToken).filter(
        InboxSyncToken.id == token_id,
        InboxSyncToken.customer_id == current_user.customer_id,
        InboxSyncToken.is_deleted == False,
    ).first()
    if not tok:
        raise HTTPException(status_code=404, detail="Sync token not found")
    tok.is_active = False
    tok.is_deleted = True
    db.commit()
    return {"status": "REVOKED", "id": token_id}


@router.post("/sync-drop")
async def sync_drop_file(
    request: Request,
    file: UploadFile = File(...),
    bank_id: Optional[int] = Form(None),
    pdf_password: Optional[str] = Form(None),
    auto_execute: bool = Form(True),
    x_grow_sync_token: Optional[str] = Header(None, alias="X-Grow-Sync-Token"),
    db: Session = Depends(get_db),
):
    """
    Universal Watched-Folder / Drag-and-Drop sync ingestion endpoint.
    Accepts files downloaded manually from portal-only Egyptian banks (e.g., NBE, Banque Misr, HDB),
    deduplicates by SHA-256 content hash, classifies via Smart Inbox, and auto-executes when bank is known.
    Authenticates via `X-Grow-Sync-Token` or standard user Bearer JWT.
    """
    customer_id, actor_user_id, sync_tok = _resolve_sync_actor(request, x_grow_sync_token, db)

    filename = file.filename or "statement.xlsx"
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_ATTACHMENT_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file extension '{ext}'. Allowed: {sorted(ALLOWED_ATTACHMENT_EXTENSIONS)}",
        )

    raw_bytes = await file.read()
    if not raw_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    content_hash = hashlib.sha256(raw_bytes).hexdigest()

    # Content-hash deduplication per customer
    existing_dup = db.query(InboxItem).filter(
        InboxItem.customer_id == customer_id,
        InboxItem.attachment_content_hash == content_hash,
        InboxItem.is_deleted == False,
    ).first()
    if existing_dup:
        return {
            "status": "DUPLICATE_SKIPPED",
            "existing_item_id": existing_dup.id,
            "message": f"File '{filename}' was already synced (Inbox Item #{existing_dup.id}).",
        }

    resolved_bank_id = bank_id or _detect_bank_id_from_filename_or_bytes(db, filename, raw_bytes)
    if resolved_bank_id:
        valid_bank = db.query(Bank).filter(Bank.id == resolved_bank_id, Bank.is_deleted == False).first()
        if not valid_bank:
            resolved_bank_id = None

    clean_hash_id = f"sync-{content_hash[:16]}"
    blob_path = build_customer_blob_path(customer_id, "inbox", f"sync_{clean_hash_id}/{filename}")
    mime_type = "application/pdf" if ext == ".pdf" else "application/octet-stream"
    try:
        storage_path = upload_bytes_to_gcs_sync(blob_path, raw_bytes, content_type=mime_type, timeout=25)
    except Exception:
        storage_path, _, _ = inbox_polling_service._save_attachment(
            customer_id, clean_hash_id, filename, raw_bytes
        )

    now_utc = datetime.now(timezone.utc)
    item = InboxItem(
        customer_id=customer_id,
        message_id=f"sync-{content_hash[:24]}-{int(now_utc.timestamp())}@grow.sync",
        sender_email="folder-sync@local-workstation",
        sender_domain=None,
        subject=f"[Folder Sync] {filename}",
        matched_bank_id=resolved_bank_id,
        is_trusted_sender=True,
        has_attachment=True,
        attachment_count=1,
        primary_attachment_path=storage_path,
        primary_attachment_name=filename,
        primary_attachment_type=ext.lstrip(".").upper(),
        attachment_content_hash=content_hash,
        received_at=now_utc,
        status="RECEIVED",
        ingestion_channel="WATCHED_FOLDER",
    )
    db.add(item)
    db.flush()

    att = InboxAttachment(
        inbox_item_id=item.id,
        file_name=filename,
        file_type=ext.lstrip(".").upper(),
        file_size_bytes=len(raw_bytes),
        storage_path=storage_path,
        content_hash=content_hash,
        is_primary=True,
    )
    db.add(att)
    if sync_tok:
        sync_tok.files_synced_count = (sync_tok.files_synced_count or 0) + 1
    db.commit()
    db.refresh(item)

    classified_item = inbox_classification_service.classify(db, item)
    if classified_item.classification == "UNCLASSIFIED":
        classified_item.classification = "BANK_STATEMENT"
        classified_item.classification_confidence = "HIGH" if resolved_bank_id else "MEDIUM"
        db.commit()
        db.refresh(classified_item)

    action_result = None
    if auto_execute and resolved_bank_id and classified_item.classification in ("BANK_STATEMENT", "CASH_POSITION_SNAPSHOT"):
        try:
            action_result = await inbox_action_service.confirm_and_execute(
                db=db,
                inbox_item_id=classified_item.id,
                user_id=actor_user_id,
                customer_id=customer_id,
                override_bank_id=resolved_bank_id,
                pdf_password=pdf_password,
            )
        except HTTPException as exec_err:
            logger.info(f"Watched-folder item #{classified_item.id} queued for manual confirmation: {exec_err.detail}")
        except Exception as exec_err:
            logger.info(f"Watched-folder item #{classified_item.id} queued for review: {exec_err}")

    db.refresh(classified_item)
    return {
        "status": classified_item.status,
        "item_id": classified_item.id,
        "classification": classified_item.classification,
        "matched_bank_id": classified_item.matched_bank_id,
        "action_result": action_result,
        "message": (
            action_result["message"]
            if action_result
            else f"Synced '{filename}' into Smart Inbox (#{classified_item.id}) for review."
        ),
    }


@router.get("/sync-watcher-script")
def download_watcher_script(
    request: Request,
    token_hint: Optional[str] = Query("PASTE_YOUR_GWS_SYNC_TOKEN_HERE"),
    current_user: TokenData = Depends(require_email_inbox_feature),
):
    """Downloads a ready-to-run Windows PowerShell Watched-Folder Sync script."""
    base_url = str(request.base_url).rstrip("/")
    api_url = f"{base_url}/api/v1/inbox/sync-drop"
    safe_token = re.sub(r"[^A-Za-z0-9_\-]", "", token_hint or "PASTE_YOUR_GWS_SYNC_TOKEN_HERE")

    ps1_content = f"""# Grow Treasury - Watched Folder Auto-Sync Script (Windows PowerShell)
# Drop any downloaded bank statement (.xlsx, .xls, .csv, .pdf, .mt940, .sta, .xml) into $WatchFolder
# Tip: Include the bank short name in the file name (e.g. NBE_Statement_Nov.xlsx) for instant auto-routing.

param(
    [string]$WatchFolder = "$HOME\\GrowSync",
    [string]$SyncToken   = "{safe_token}",
    [string]$EndpointUrl = "{api_url}"
)

$SyncedFolder = Join-Path $WatchFolder "Synced"
$FailedFolder = Join-Path $WatchFolder "Failed"
New-Item -ItemType Directory -Force -Path $WatchFolder, $SyncedFolder, $FailedFolder | Out-Null

$AllowedExt = @(".xlsx", ".xls", ".xlsb", ".csv", ".pdf", ".mt940", ".sta", ".940", ".xml", ".txt")
Write-Host "Grow Watched-Folder Sync active on: $WatchFolder" -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop." -ForegroundColor Gray

while ($true) {{
    $files = Get-ChildItem -Path $WatchFolder -File | Where-Object {{ $AllowedExt -contains $_.Extension.ToLower() }}
    foreach ($f in $files) {{
        try {{
            $boundary = [System.Guid]::NewGuid().ToString()
            $LF = "`r`n"
            $fileBytes = [System.IO.File]::ReadAllBytes($f.FullName)
            $enc = [System.Text.Encoding]::UTF8
            $header = "--$boundary$LF" +
                      "Content-Disposition: form-data; name=`"file`"; filename=`"$($f.Name)`"$LF" +
                      "Content-Type: application/octet-stream$LF$LF"
            $footer = "$LF--$boundary--$LF"
            $ms = New-Object System.IO.MemoryStream
            $hdrBytes = $enc.GetBytes($header)
            $ftrBytes = $enc.GetBytes($footer)
            $ms.Write($hdrBytes, 0, $hdrBytes.Length)
            $ms.Write($fileBytes, 0, $fileBytes.Length)
            $ms.Write($ftrBytes, 0, $ftrBytes.Length)

            $headers = @{{ "X-Grow-Sync-Token" = $SyncToken }}
            $resp = Invoke-RestMethod -Uri $EndpointUrl -Method Post -Headers $headers -ContentType "multipart/form-data; boundary=$boundary" -Body $ms.ToArray()
            Write-Host "[SYNCED] $($f.Name) -> $($resp.status): $($resp.message)" -ForegroundColor Green
            Move-Item -Path $f.FullName -Destination (Join-Path $SyncedFolder $f.Name) -Force
        }} catch {{
            Write-Warning "[FAILED] $($f.Name): $_"
            Move-Item -Path $f.FullName -Destination (Join-Path $FailedFolder $f.Name) -Force
        }}
    }}
    Start-Sleep -Seconds 5
}}
"""
    return Response(
        content=ps1_content,
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="Start-GrowFolderSync.ps1"'},
    )
