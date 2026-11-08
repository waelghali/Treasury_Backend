# c:\Grow\app\api/v1/endpoints\end_user.py

import os
import sys
import io
import base64
from datetime import date, datetime, timedelta
import logging
import decimal
import hashlib
import json
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query, UploadFile, File, Body, Form, BackgroundTasks
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from app.services.document_verification_service import document_verification_service
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import func, and_
from typing import List, Optional, Any, Dict, Tuple
import asyncio

from app.database import get_db

from app.schemas.all_schemas import (
    LGRecordCreate, LGRecordOut, LGDocumentOut,
    CustomerEntityOut, CurrencyOut, LgTypeOut, RuleOut, IssuingMethodOut,
    LgStatusOut, LgOperationalStatusOut, BankOut, LGCategoryOut, UserOut,
    InternalOwnerContactCreate,
    InternalOwnerContactOut,
    LGInstructionOut,
    LGLifecycleEventOut,
    ApprovalRequestCreate,
    ApprovalRequestOut,
    LGRecordRelease,
    LGRecordLiquidation,
    LGRecordDecreaseAmount,
    ApprovalRequestStatusEnum,
    InternalOwnerChangeScope,
    InternalOwnerContactUpdateDetails,
    LGRecordChangeOwner,
    LGInstructionRecordDelivery,
    LGInstructionRecordBankReply,
    LGRecordAmendRequest,
    LGActivateNonOperativeRequest,
    LGDocumentCreate,
    LGRecordToggleAutoRenewalRequest,
    AutoRenewalRunSummaryOut,
    AutoRenewalBatchItemOut,
    AutoRenewalBatchOut,
    BulkReprintInstructionsIn,
    AutoRenewalPreviewOut,
    RunAutoRenewalRequestIn,
    CustomerConfigurationOut,
    # NEW: Import SystemNotificationOut schema
    SystemNotificationOut,
    # New schema
    LGInstructionCancelRequest,
    LGLifecycleHistoryReportItem,
)
from app.crud import crud_reports
from app.crud.crud import (
    log_action,
    crud_audit_log,
    crud_approval_request,
    crud_customer,
    crud_customer_configuration,
    crud_lg_owner,
    crud_template,
    crud_lg_record,
    crud_internal_owner_contact,
    crud_lg_instruction,
    crud_lg_document,
    crud_lg_category,
    crud_currency,
    crud_bank,
    crud_lg_type,
    crud_rule,
    crud_issuing_method,
    crud_lg_status,
    crud_lg_operational_status,
    # MODIFIED: Removed crud_universal_category
    crud_user,
    crud_customer_entity, # Added missing import for crud_customer_entity
    # NEW: Import crud_system_notification
    crud_system_notification,
    crud_system_notification_view_log,
    # New CRUD instance
    crud_lg_cancellation,
)

import app.models as models

from app.constants import (
    ACTION_TYPE_LG_DECREASE_AMOUNT,
    ACTION_TYPE_LG_CHANGE_OWNER_DETAILS,
    ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER,
    ACTION_TYPE_LG_CHANGE_BULK_LG_OWNER,
    ACTION_TYPE_LG_RECORD_DELIVERY,
    ACTION_TYPE_LG_RECORD_BANK_REPLY,
    ACTION_TYPE_LG_REMINDER_TO_BANKS,
    ACTION_TYPE_LG_BULK_REMINDER_TO_BANKS,
    AUDIT_ACTION_TYPE_LG_BULK_REMINDER_INITIATED,
    GlobalConfigKey,
    ACTION_TYPE_LG_EXTEND,
    ACTION_TYPE_LG_LIQUIDATE,
    ACTION_TYPE_LG_RELEASE,
    ACTION_TYPE_LG_AMEND,
    ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
    ACTION_TYPES_REQUIRING_APPROVAL,
    AUDIT_ACTION_TYPE_LG_AMENDED,
    AUDIT_ACTION_TYPE_LG_ACTIVATED,
    LgStatusEnum,
    LgTypeEnum,
    LgOperationalStatusEnum,
    NOTIFICATION_PRINT_CONFIRMATION, # NEW: Import the constant for print confirmation email
    # New constant
    ACTION_TYPE_LG_CANCEL_LAST_INSTRUCTION,
    # New constants for audit log
    AUDIT_ACTION_TYPE_LG_INSTRUCTION_CANCELLATION_FAILED,
)
from app.core.ai_integration import generate_signed_gcs_url

# Initialize logger at the module level
logger = logging.getLogger(__name__)

try:
    # NEW: Import the subscription security checks
    from app.core.security import (
        TokenData,
        HasPermission,
        get_current_active_user,
        get_current_user,
        check_subscription_status,
        check_for_read_only_mode, get_client_ip
    )
    from app.core.ai_integration import process_lg_document_with_ai, generate_signed_gcs_url, process_amendment_with_ai

    from app.core.document_generator import generate_pdf_from_html, generate_merged_pdf_from_html_list
    from app.core.email_service import EmailSettings, get_global_email_settings, send_email, get_customer_email_settings

except Exception as e:
    # Now logger is defined, so this line won't cause a NameError
    logger.critical(f"FATAL ERROR (end_user.py): Could not import core modules using standard absolute imports. Error: {e}", exc_info=True)
    raise

async def check_and_handle_document(
    db: Session,
    customer_id: int,
    config_key: GlobalConfigKey,
    document_file: Optional[UploadFile]
) -> Optional[int]:
    """
    Checks if a document is mandatory based on config_key.
    If mandatory and missing, raises 400.
    If provided, creates the document record and returns its ID.
    """
    is_doc_mandatory_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id, config_key
    )
    is_doc_mandatory = is_doc_mandatory_config.get('effective_value', 'false').lower() == 'true'

    if is_doc_mandatory and (document_file is None or document_file.filename == ''):
        action_name = config_key.value.replace('DOC_MANDATORY_', '').replace('_', ' ').title()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Supporting document is mandatory for {action_name} as per Corporate Admin settings."
        )

    # If document is provided (optional or mandatory)
    if document_file and document_file.filename != '':
        # Use existing document creation logic (e.g., crud_lg_document.create_document)
        document_in = LGDocumentCreate(
            filename=document_file.filename,
            file_type=document_file.content_type,
            # document_type and description can be set based on the action, or left generic
            document_type=config_key.value, # Using config key as a unique type identifier
            is_supporting_document=True
        )
        lg_document = await crud_lg_document.create_document(
            db, obj_in=document_in, file_data=await document_file.read(), commit=False
        )
        return lg_document.id
    
    return None

# --- UPDATED: get_current_end_user_context dependency now checks subscription status ---

def get_current_end_user_context(
    current_user: TokenData = Depends(check_subscription_status)
) -> TokenData:
    """
    Dependency that ensures the current user is an active user (END_USER or CORPORATE_ADMIN).
    
    CRITICAL FIX: Allow SYSTEM_OWNER role to pass through without a customer_id, 
    as they may be accessing global notifications or debugging.
    """
    
    # Allow System Owners to proceed, they will get a customer_id of None,
    # which the notification fetching logic must safely handle.
    if current_user.role == "system_owner":
        return current_user
        
    # For all other non-system-owner roles (End User, Corporate Admin, etc.), 
    # a customer_id is mandatory for the 'end-user' context.
    if current_user.customer_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User is not associated with a customer. Data integrity error."
        )
        
    return current_user


def get_fresh_entity_permissions(db: Session, user_id: int) -> Tuple[bool, List[int]]:
    """
    Fetches the user from the DB to ensure we have real-time entity access rights.
    """
    user = crud_user.get(db, user_id)
    if not user:
        return False, []
    
    has_all = user.has_all_entity_access
    allowed_ids = [assoc.customer_entity_id for assoc in user.entity_associations]
    return has_all, allowed_ids

# --- Helper for Fuzzy Matching ---
def find_best_match(extracted_name: str, valid_entities: List[Dict[str, Any]], threshold: int = 70) -> Optional[int]:
    """
    Finds the best fuzzy match for an extracted name against a list of valid entities (name and ID).
    Returns the ID of the best match if the similarity score is above the threshold.
    """
    from fuzzywuzzy import fuzz
    if not extracted_name:
        logger.debug(f"Fuzzy Match Debug: Extracted name is empty.")
        return None

    best_score = -1
    best_match_id = None
    best_match_name = None

    logger.debug(f"Fuzzy Match Debug: Searching among {len(valid_entities)} entities.")
    for entity in valid_entities:
        entity_name = entity.get('name')
        entity_id = entity.get('id')
        if not entity_name or entity_id is None:
            continue
        # FIX: Correct indentation for score calculation
        score = fuzz.token_set_ratio(extracted_name.lower(), entity_name.lower())
        logger.debug(f"Fuzzy Match Debug: Comparing with entity ID {entity_id} - Score: {score}")

        if score > best_score and score >= threshold:
            best_score = score
            best_match_id = entity_id
            best_match_name = entity_name

    if best_match_id:
        logger.debug(f"Fuzzy Match Debug: Best match is ID {best_match_id} with score {best_score}.")
    else:
        logger.debug(f"Fuzzy Match Debug: No good fuzzy match found above threshold {threshold}.")

    return best_match_id


router = APIRouter()

INSTRUCTION_TYPES_REQUIRING_PRINTING = [
    ACTION_TYPE_LG_RELEASE,
    ACTION_TYPE_LG_LIQUIDATE,
    ACTION_TYPE_LG_DECREASE_AMOUNT,
    ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
]
@router.get(
    "/lg-records/auto-renewal-preview",
    response_model=AutoRenewalPreviewOut,
    dependencies=[Depends(HasPermission("lg_record:extend")), Depends(check_subscription_status)],
    summary="Get candidates and suggested dates for auto and forced renewal preview"
)
def get_auto_renewal_preview_endpoint(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Retrieves eligible LG candidates for auto and forced renewal preview,
    including calculated suggested dates, risk tiers (Forced vs Proactive), and control flags.
    """
    has_all, allowed_ids = get_fresh_entity_permissions(db, end_user_context.user_id)
    preview = crud_lg_record.get_auto_renewal_candidates(
        db,
        customer_id=end_user_context.customer_id,
        user_has_all_access=has_all,
        user_allowed_entity_ids=allowed_ids
    )
    return preview


@router.post(
    "/lg-records/run-auto-renewal",
    response_model=AutoRenewalRunSummaryOut,
    dependencies=[Depends(HasPermission("lg_record:extend")), Depends(check_for_read_only_mode)],
    summary="Run automated and forced LG renewal for eligible records"
)
async def run_auto_renewal_endpoint(
    request_payload: Optional[RunAutoRenewalRequestIn] = None,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Triggers the bulk auto-renewal and force-renewal process for eligible LG records.
    Optionally accepts a list of selected LG IDs and custom new expiry dates.
    Enforces that mandatory forced renewal LGs cannot be bypassed.
    Returns a summary of renewed LGs and the combined instruction PDF.
    """
    client_host = get_client_ip(request) if request else None

    try:
        has_all, allowed_ids = get_fresh_entity_permissions(db, end_user_context.user_id)
        selected_items = request_payload.renewals if request_payload and request_payload.renewals is not None else None

        renewed_count, combined_pdf_bytes, batch_id, renewed_items = await crud_lg_record.run_auto_renewal_process(
            db,
            user_id=end_user_context.user_id,
            customer_id=end_user_context.customer_id,
            selected_items=selected_items,
            user_has_all_access=has_all,
            user_allowed_entity_ids=allowed_ids
        )

        if combined_pdf_bytes:
            pdf_base64 = base64.b64encode(combined_pdf_bytes).decode('utf-8')
            return AutoRenewalRunSummaryOut(
                renewed_count=renewed_count,
                message=f"Successfully renewed {renewed_count} eligible LG(s). Consolidated instruction letter generated.",
                combined_pdf_base64=pdf_base64,
                batch_id=batch_id,
                renewed_items=renewed_items
            )
        else:
            return AutoRenewalRunSummaryOut(
                renewed_count=renewed_count,
                message="No eligible LGs were renewed.",
                combined_pdf_base64=None,
                batch_id=batch_id,
                renewed_items=renewed_items
            )
    except HTTPException as e:
        logger.error(f"HTTPException during bulk renewal for customer {end_user_context.customer_id}: {e.detail}", exc_info=True)
        raise
    except Exception as e:
        logger.exception(f"Unexpected error during bulk renewal for customer {end_user_context.customer_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred during bulk renewal: {e}"
        )


def _render_instruction_letter_html(db: Session, db_instruction: models.LGInstruction) -> str:
    """Helper to render the populated HTML for a single LGInstruction letter."""
    template = crud_template.get(db, db_instruction.template_id)
    if not template:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Template not found for instruction {db_instruction.id}.")

    generated_html = template.content
    instruction_details = dict(db_instruction.details or {})
    lg_record_for_template = db_instruction.lg_record
    if lg_record_for_template:
        recipient_name, recipient_address = crud_lg_record._get_recipient_details(db, lg_record_for_template)
        issuance_date_str = lg_record_for_template.issuance_date.strftime("%Y-%m-%d") if getattr(lg_record_for_template, "issuance_date", None) else "N/A"
        expiry_date_str = lg_record_for_template.expiry_date.strftime("%Y-%m-%d") if getattr(lg_record_for_template, "expiry_date", None) else "N/A"
        customer_obj = lg_record_for_template.customer
        entity_obj = lg_record_for_template.beneficiary_corporate
        customer_address_val = (entity_obj.address if entity_obj and entity_obj.address else (customer_obj.address if customer_obj else "")) or ""
        customer_email_val = (entity_obj.contact_email if entity_obj and entity_obj.contact_email else (customer_obj.contact_email if customer_obj else "")) or ""

        instruction_details.update({
            "lg_number": lg_record_for_template.lg_number,
            "lg_serial_number": lg_record_for_template.lg_number,
            "instruction_serial": db_instruction.serial_number or "",
            "serial_number": db_instruction.serial_number or "",
            "lg_amount": float(lg_record_for_template.lg_amount),
            "lg_currency": lg_record_for_template.lg_currency.iso_code if lg_record_for_template.lg_currency else "",
            "issuing_bank_name": crud_lg_record._get_effective_issuing_bank_name(lg_record_for_template),
            "lg_issuer_name": lg_record_for_template.issuer_name,
            "lg_beneficiary_name": lg_record_for_template.beneficiary_corporate.entity_name if lg_record_for_template.beneficiary_corporate else "",
            "customer_name": lg_record_for_template.customer.name if lg_record_for_template.customer else "",
            "customer_address": customer_address_val,
            "customer_contact_email": customer_email_val,
            "internal_owner_email": lg_record_for_template.internal_owner_contact.email if lg_record_for_template.internal_owner_contact else "",
            "current_date": (db_instruction.instruction_date.strftime("%Y-%m-%d") if getattr(db_instruction, "instruction_date", None) else date.today().strftime("%Y-%m-%d")),
            "platform_name": "Treasury Management Platform",
            "recipient_name": recipient_name,
            "recipient_address": recipient_address,
        })
        instruction_details.setdefault("issuance_date", issuance_date_str)
        instruction_details.setdefault("issue_date", issuance_date_str)
        instruction_details.setdefault("expiry_date", instruction_details.get("old_expiry_date") or expiry_date_str)
        instruction_details.setdefault("maturity_date", instruction_details.get("old_expiry_date") or expiry_date_str)
        instruction_details.setdefault("notes_section", "")

        if "old_expiry_date" in instruction_details and "new_expiry_date" in instruction_details and "extension_period" not in instruction_details:
            try:
                old_d = datetime.strptime(str(instruction_details["old_expiry_date"])[:10], "%Y-%m-%d").date()
                new_d = datetime.strptime(str(instruction_details["new_expiry_date"])[:10], "%Y-%m-%d").date()
                instruction_details["extension_period"] = crud_lg_record._format_extension_period(old_d, new_d)
            except Exception:
                instruction_details["extension_period"] = "N/A"

        symbol = lg_record_for_template.lg_currency.symbol if lg_record_for_template.lg_currency else ""
        instruction_details["lg_amount_formatted"] = f"{symbol} {float(lg_record_for_template.lg_amount):,.2f}"

    for key, value in instruction_details.items():
        str_value = str(value) if value is not None else ""
        generated_html = generated_html.replace(f"{{{{{key}}}}}", str_value)

    return generated_html


@router.get(
    "/lg-records/auto-renewal-history",
    response_model=List[AutoRenewalBatchOut],
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)],
    summary="Get history of bulk auto-renewal runs and their extended LGs"
)
def get_auto_renewal_history_endpoint(
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    has_all, allowed_ids = get_fresh_entity_permissions(db, end_user_context.user_id)
    allowed_ids_set = set(allowed_ids) if not has_all else set()

    logs = (
        db.query(models.AuditLog)
        .options(selectinload(models.AuditLog.user))
        .filter(
            models.AuditLog.customer_id == end_user_context.customer_id,
            models.AuditLog.action_type == AUDIT_ACTION_TYPE_LG_BULK_REMINDER_INITIATED,
        )
        .order_by(models.AuditLog.timestamp.desc())
        .limit(100)
        .all()
    )

    batches: List[AutoRenewalBatchOut] = []
    for log_entry in logs:
        details = log_entry.details if isinstance(log_entry.details, dict) else {}
        if details.get("action") != "Bulk LG Renewal (Auto & Forced)":
            continue

        batch_id = details.get("batch_id") or f"RUN-{log_entry.id}"
        inst_ids = details.get("renewed_instruction_ids") or []
        lg_numbers = details.get("renewed_lg_numbers") or []

        instructions: List[models.LGInstruction] = []
        if inst_ids:
            instructions = (
                db.query(models.LGInstruction)
                .options(
                    selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_currency),
                    selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.issuing_bank),
                    selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
                    selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.communication_bank),
                )
                .filter(
                    models.LGInstruction.id.in_(inst_ids),
                    models.LGInstruction.is_deleted == False,
                    models.LGInstruction.lg_record.has(models.LGRecord.customer_id == end_user_context.customer_id),
                )
                .all()
            )
        elif lg_numbers and log_entry.timestamp:
            # Fallback for legacy runs created before renewed_instruction_ids was stored
            log_date = log_entry.timestamp.date()
            instructions = (
                db.query(models.LGInstruction)
                .join(models.LGRecord, models.LGInstruction.lg_record_id == models.LGRecord.id)
                .options(
                    selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_currency),
                    selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.issuing_bank),
                    selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
                    selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.communication_bank),
                )
                .filter(
                    models.LGRecord.customer_id == end_user_context.customer_id,
                    models.LGRecord.lg_number.in_(lg_numbers),
                    models.LGInstruction.is_deleted == False,
                    models.LGInstruction.instruction_type == "LG_EXTENSION",
                    func.date(models.LGInstruction.instruction_date) == log_date,
                )
                .order_by(models.LGInstruction.id.desc())
                .all()
            )
            # Deduplicate per LG number (closest to log timestamp)
            seen_lg = set()
            deduped = []
            for inst in instructions:
                if inst.lg_record_id not in seen_lg:
                    seen_lg.add(inst.lg_record_id)
                    deduped.append(inst)
            instructions = deduped

        inst_by_id = {inst.id: inst for inst in instructions}
        ordered_insts = [inst_by_id[i] for i in inst_ids if i in inst_by_id] if inst_ids else instructions

        items_out: List[AutoRenewalBatchItemOut] = []
        for inst in ordered_insts:
            lg_rec = inst.lg_record
            if not lg_rec:
                continue
            if not has_all and lg_rec.beneficiary_corporate_id not in allowed_ids_set:
                continue
            inst_det = inst.details if isinstance(inst.details, dict) else {}
            items_out.append(
                AutoRenewalBatchItemOut(
                    lg_record_id=lg_rec.id,
                    lg_number=lg_rec.lg_number,
                    beneficiary_name=lg_rec.beneficiary_corporate.entity_name if lg_rec.beneficiary_corporate else None,
                    bank_name=crud_lg_record._get_effective_issuing_bank_name(lg_rec),
                    amount=float(lg_rec.lg_amount) if lg_rec.lg_amount is not None else None,
                    currency_code=lg_rec.lg_currency.iso_code if lg_rec.lg_currency else None,
                    old_expiry_date=inst_det.get("old_expiry_date"),
                    new_expiry_date=inst_det.get("new_expiry_date") or (lg_rec.expiry_date.strftime("%Y-%m-%d") if lg_rec.expiry_date else None),
                    instruction_id=inst.id,
                    instruction_serial=inst.serial_number,
                    instruction_status=inst.status,
                    delivery_date=inst.delivery_date,
                    bank_reply_date=inst.bank_reply_date,
                    is_printed=bool(inst.is_printed),
                )
            )

        if not items_out and not has_all:
            continue

        batches.append(
            AutoRenewalBatchOut(
                batch_id=batch_id,
                audit_log_id=log_entry.id,
                created_at=log_entry.timestamp,
                triggered_by_email=log_entry.user.email if log_entry.user else None,
                renewed_count=len(items_out) if items_out else int(details.get("renewed_lg_count") or 0),
                items=items_out,
            )
        )
        if len(batches) >= limit:
            break

    return batches


@router.post(
    "/lg-records/auto-renewal-reprint",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)],
    summary="Regenerate and merge extension instruction PDFs for a previous auto-renewal batch or selected instructions"
)
async def reprint_auto_renewal_batch_endpoint(
    payload: BulkReprintInstructionsIn,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    if not payload.instruction_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No instruction IDs provided.")

    has_all, allowed_ids = get_fresh_entity_permissions(db, end_user_context.user_id)
    allowed_ids_set = set(allowed_ids) if not has_all else set()

    instructions = (
        db.query(models.LGInstruction)
        .options(
            selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_currency),
            selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.issuing_bank),
            selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
            selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.internal_owner_contact),
            selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.customer),
            selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.communication_bank),
        )
        .filter(
            models.LGInstruction.id.in_(payload.instruction_ids),
            models.LGInstruction.is_deleted == False,
            models.LGInstruction.lg_record.has(models.LGRecord.customer_id == end_user_context.customer_id),
        )
        .all()
    )

    inst_by_id = {inst.id: inst for inst in instructions}
    ordered_insts = [inst_by_id[i] for i in payload.instruction_ids if i in inst_by_id]

    html_list: List[str] = []
    for inst in ordered_insts:
        if not has_all and inst.lg_record and inst.lg_record.beneficiary_corporate_id not in allowed_ids_set:
            continue
        html_list.append(_render_instruction_letter_html(db, inst))

    if not html_list:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No accessible instructions found to reprint.")

    combined_pdf_bytes = await generate_merged_pdf_from_html_list(
        html_list,
        f"reprint_bulk_lg_renewal_customer_{end_user_context.customer_id}_{date.today().isoformat()}"
    )
    if not combined_pdf_bytes:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to generate combined PDF.")

    pdf_base64 = base64.b64encode(combined_pdf_bytes).decode("utf-8")
    return {
        "count": len(html_list),
        "message": f"Successfully regenerated consolidated PDF for {len(html_list)} instruction letter(s).",
        "combined_pdf_base64": pdf_base64,
    }


@router.get("/status", dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def get_end_user_status(
    current_user: TokenData = Depends(get_current_end_user_context)
):
    """
    Checks the status of the End User API and returns basic user info.
    """
    return {"message": f"End User API is up and running for {current_user.email} (Customer ID: {current_user.customer_id})!"}


# --- LG Record Management Endpoints ---
@router.post("/lg-records/", response_model=LGRecordOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(check_for_read_only_mode)]) # ADDED dependency
async def create_lg_record(
    lg_record_in_json: str = Form(..., alias="lg_record_in", description="JSON string of LGRecordCreate data"),
    ai_scan_file: Optional[UploadFile] = File(None, description="Optional LG Copy file (image or PDF)"),
    internal_supporting_document_file: Optional[UploadFile] = File(None, description="Optional Internal Supporting Document (PDF)"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Allows an End User (or Corporate Admin acting as End User) to create a new LG record.
    Performs comprehensive validation and links associated documents.
    Now accepts JSON data as a form field and files as UploadFile objects.
    """
    client_host = get_client_ip(request) if request else None

    # Parse the JSON string back into a Pydantic model
    try:
        # Step 1: Load the raw JSON string into a Python dictionary
        lg_record_data = json.loads(lg_record_in_json)

        # Step 2: Pre-validation fix. Convert the empty string to None.
        # Pydantic can handle None for an optional integer field, but not "".
        if lg_record_data.get('lg_payable_currency_id') == '':
            lg_record_data['lg_payable_currency_id'] = None
        
        # Step 3: Now validate the cleaned dictionary with the Pydantic model
        lg_record_in = LGRecordCreate.model_validate(lg_record_data)

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON format for LG record data."
        )
    except Exception as e:
        # This will now correctly catch any remaining Pydantic validation errors
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Error validating LG record data: {e}"
        )
    
    # Reject expired LGs during operational recording
    if lg_record_in.expiry_date and lg_record_in.expiry_date < date.today():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot record an expired LG. The Expiry Date must be today or in the future. For historical data or LGs that have been extended, please record them through the Migration Hub."
        )

    # NEW LOGIC: Conditional validation for Foreign Bank details and Advising Status
    foreign_bank = db.query(models.Bank).filter(models.Bank.name == "Foreign Bank", models.Bank.is_deleted == False).first()
    if foreign_bank and lg_record_in.issuing_bank_id == foreign_bank.id:
        if not lg_record_in.foreign_bank_name or not lg_record_in.foreign_bank_name.strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Foreign bank name is mandatory when 'Foreign Bank' is selected.")
        if not lg_record_in.foreign_bank_country or not lg_record_in.foreign_bank_country.strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Foreign bank country is mandatory when 'Foreign Bank' is selected.")
        if not lg_record_in.foreign_bank_address or not lg_record_in.foreign_bank_address.strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Foreign bank address is mandatory when 'Foreign Bank' is selected.")
        if not lg_record_in.foreign_bank_swift_code or not lg_record_in.foreign_bank_swift_code.strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Foreign bank SWIFT code is mandatory when 'Foreign Bank' is selected.")
        
        if lg_record_in.advising_status in [models.AdvisingStatus.ADVISED, models.AdvisingStatus.CONFIRMED]:
            if not lg_record_in.communication_bank_id:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"A Communication Bank is mandatory when Advising Status is '{lg_record_in.advising_status.value}'.")    # Read file contents if files are provided
    
    ai_scan_file_bytes = None
    if ai_scan_file:
        ai_scan_file_bytes = await ai_scan_file.read()

    internal_supporting_document_file_bytes = None
    if internal_supporting_document_file:
        internal_supporting_document_file_bytes = await internal_supporting_document_file.read()

    # Update lg_record_in's embedded LGDocumentCreate schemas with file metadata
    # These will then be passed down to crud_lg_record.create
    if ai_scan_file:
        lg_record_in.ai_scan_file = LGDocumentCreate(
            document_type="AI_SCAN", # This is the type submitted by frontend for AI scan.
                                     # The crud layer will override to ORIGINAL_LG_DOCUMENT for saving.
            file_name=ai_scan_file.filename,
            mime_type=ai_scan_file.content_type,
            file_path="", # This will be populated by crud_lg_document.create_document
        )
    else:
        lg_record_in.ai_scan_file = None

    if internal_supporting_document_file:
        lg_record_in.internal_supporting_document_file = LGDocumentCreate(
            document_type="INTERNAL_SUPPORTING",
            file_name=internal_supporting_document_file.filename,
            mime_type=internal_supporting_document_file.content_type,
            file_path="", # This will be populated by crud_lg_document.create_document
        )
    else:
        lg_record_in.internal_supporting_document_file = None

    try:
        # Call the crud method with the parsed Pydantic model and the file bytes
        db_lg_record = await crud_lg_record.create( # Use 'await' here
            db,
            obj_in=lg_record_in,
            customer_id=end_user_context.customer_id,
            user_id=end_user_context.user_id,
            ai_scan_file_content=ai_scan_file_bytes,
            internal_supporting_document_file_content=internal_supporting_document_file_bytes
        )
        return db_lg_record
    except HTTPException as e:
        log_action(db, user_id=end_user_context.user_id, action_type="CREATE_FAILED", entity_type="LGRecord", entity_id=None, details={"lg_number": lg_record_in.lg_number if lg_record_in else "N/A", "customer_id": end_user_context.customer_id, "reason": str(e.detail)}, customer_id=end_user_context.customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=end_user_context.user_id, action_type="CREATE_FAILED", entity_type="LGRecord", entity_id=None, details={"lg_number": lg_record_in.lg_number if lg_record_in else "N/A", "customer_id": end_user_context.customer_id, "reason": str(e)}, customer_id=end_user_context.customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

# AI Scan File Endpoint (Now uses real AI integration and fuzzy matching)
@router.post("/lg-records/scan-file/", response_model=Dict[str, Any], dependencies=[Depends(check_for_read_only_mode)]) # ADDED dependency
async def scan_lg_file(
    file: UploadFile = File(..., description="Image or PDF file of the LG to scan"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:ai_scan")),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Performs AI scanning of an LG file (image/PDF) to extract structured data.
    Returns a dictionary of extracted fields for form auto-population.
    This endpoint checks if the customer's plan supports AI integration.
    """
    from fuzzywuzzy import fuzz
    client_host = get_client_ip(request) if request else None
    ai_usage_metadata = {}
    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan or not customer.subscription_plan.can_ai_integration:
        log_action(db, user_id=end_user_context.user_id, action_type="AI_SCAN_FAILED", entity_type="LGRecord", entity_id=None, details={"reason": "AI integration not enabled for plan", "ai_token_usage": ai_usage_metadata}, customer_id=end_user_context.customer_id)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your subscription plan does not support AI integration for file scanning."
        )

    supported_mime_types = ["image/jpeg", "image/png", "application/pdf"]
    if file.content_type not in supported_mime_types:
        log_action(db, user_id=end_user_context.user_id, action_type="AI_SCAN_FAILED", entity_type="LGRecord", entity_id=None, details={"reason": f"Unsupported file type: {file.content_type}", "ai_token_usage": ai_usage_metadata}, customer_id=end_user_context.customer_id)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only {', '.join(supported_mime_types)} files are supported for AI scanning."
        )

    try:
        # 1. Fetch Customer Bucket Preference
        bucket_config = crud_customer_configuration.get_customer_config_or_global_fallback(
            db, end_user_context.customer_id, GlobalConfigKey.STORAGE_BUCKET_NAME
        )
        # If no config exists, this returns None, handling the fallback automatically
        customer_bucket = bucket_config.get("effective_value") if bucket_config else None
        # 2. Call AI with the specific bucket
        file_bytes = await file.read()
        # 1. FETCH THE USER OBJECT
        from app.models import User
        user_obj = db.query(User).filter(User.id == end_user_context.user_id).first()
        
        # 2. VALIDATE USER EXISTS
        if not user_obj:
            logger.error(f"User ID {end_user_context.user_id} not found in database.")
            raise HTTPException(status_code=404, detail="User context not found.")

        # 3. CALL AI FUNCTION (Ensure variables match perfectly)
        extracted_ai_data, ai_usage_metadata = await process_lg_document_with_ai(
            file_bytes=file_bytes, 
            mime_type=file.content_type, 
            db=db,                         # Pass with name
            current_user=user_obj,         # Pass with name
            file_name=file.filename,       # Pass with name
            customer_bucket_name=customer_bucket
        )

        if not extracted_ai_data:
            log_action(db, user_id=end_user_context.user_id, action_type="AI_SCAN_FAILED", entity_type="LGRecord", entity_id=None, details={"file_name": file.filename, "reason": "AI extraction failed or returned no data", "ai_token_usage": ai_usage_metadata}, customer_id=end_user_context.customer_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to extract data from the document using AI. Please try again or enter details manually."
            )

        all_customer_entities = crud_customer_entity.get_all_for_customer(db, end_user_context.customer_id)
        valid_beneficiary_names_with_ids = [{"name": entity.entity_name, "id": entity.id} for entity in all_customer_entities]
        all_banks = crud_bank.get_all(db)
        all_lg_types = crud_lg_type.get_all(db)
        valid_lg_types_with_ids = [{"name": lg_type.name, "id": lg_type.id} for lg_type in all_lg_types]
        all_rules = crud_rule.get_all(db)
        valid_rules_with_ids = [{"name": rule.name, "id": rule.id} for rule in all_rules]
        all_op_statuses = crud_lg_operational_status.get_all(db)

        import re

        def _normalize_bank_str(val: str) -> str:
            if not val:
                return ""
            s = val.lower().replace("alahli", "al ahli").replace("al-ahli", "al ahli")
            s = re.sub(r"[(),.\-/]", " ", s)
            return re.sub(r"\s+", " ", s).strip()

        def _match_local_bank(raw_name: str):
            """Matches a bank name against local banks using name, short_name, former_names, and common aliases."""
            if not raw_name or not raw_name.strip():
                return None
            norm_input = _normalize_bank_str(raw_name)
            best_bank = None
            best_score = -1

            # Common full-name aliases for Egyptian banks whose DB name uses the abbreviation
            bank_aliases = {
                "QNB": ["qatar national bank", "qatar national bank al ahli", "qnb al ahli", "qnb bebasata", "qnb egypt"],
                "CIB": ["commercial international bank"],
                "NBE": ["national bank of egypt", "al ahly bank"],
                "AAIB": ["arab african international bank"],
                "HSBC": ["hsbc bank egypt", "hongkong and shanghai banking corporation"],
                "FAB": ["first abu dhabi bank", "fabmisr", "fab misr"],
                "NBK": ["national bank of kuwait"],
                "ADIB": ["abu dhabi islamic bank"],
                "ADCB": ["abu dhabi commercial bank"],
                "CAE": ["credit agricole", "crédit agricole"],
                "SAIB": ["societe arabe internationale de banque", "société arabe"],
            }

            for bank in all_banks:
                if not bank.name or bank.name.strip().lower() == "foreign bank":
                    continue
                score = 0
                norm_db_name = _normalize_bank_str(bank.name)
                if norm_input == norm_db_name:
                    score = 100
                else:
                    score = fuzz.token_set_ratio(norm_input, norm_db_name)

                # Check short_name as a distinct token or exact match
                if bank.short_name and bank.short_name.strip().upper() != "OTHER":
                    sn = bank.short_name.strip().lower()
                    if norm_input == sn:
                        score = max(score, 100)
                    elif len(sn) >= 3 and re.search(r"\b" + re.escape(sn) + r"\b", norm_input):
                        score = max(score, 95)
                    elif len(sn) == 2 and re.search(r"\b" + re.escape(sn) + r"\b", norm_input):
                        score = max(score, 85)

                # Check former_names
                former_list = []
                if isinstance(bank.former_names, list):
                    former_list = bank.former_names
                elif isinstance(bank.former_names, str) and bank.former_names.strip():
                    former_list = [x.strip() for x in bank.former_names.split("-") if x.strip()]
                for fn in former_list:
                    fn_norm = _normalize_bank_str(str(fn))
                    if fn_norm:
                        score = max(score, fuzz.token_set_ratio(norm_input, fn_norm))

                # Check known aliases by short_name
                if bank.short_name and bank.short_name.strip().upper() in bank_aliases:
                    for alias in bank_aliases[bank.short_name.strip().upper()]:
                        score = max(score, fuzz.token_set_ratio(norm_input, alias))

                if score > best_score and score >= 70:
                    best_score = score
                    best_bank = bank

            return best_bank

        mapped_data = {}
        extracted_beneficiary_name = extracted_ai_data.get("beneficiaryName")
        if extracted_beneficiary_name:
            beneficiary_id = find_best_match(extracted_beneficiary_name, valid_beneficiary_names_with_ids)
            if beneficiary_id:
                mapped_data["beneficiary_corporate_id"] = beneficiary_id
            else:
                logger.warning("No good fuzzy match found for extracted Beneficiary Corporate.")

        if extracted_ai_data.get("currency"):
            currency = crud_currency.get_by_iso_code(db, extracted_ai_data["currency"])
            if currency:
                mapped_data["lg_currency_id"] = currency.id
                mapped_data["lg_payable_currency_id"] = currency.id
            else:
                logger.warning(f"LG Currency '{extracted_ai_data['currency']}' not found in DB.")

        extracted_lg_type_name = (extracted_ai_data.get("lgType") or "").strip()
        if extracted_lg_type_name:
            lg_type_lower = extracted_lg_type_name.lower()
            if "advance" in lg_type_lower or "apg" in lg_type_lower or "دفعة مقدمة" in lg_type_lower:
                mapped_data["lg_type_id"] = LgTypeEnum.ADVANCE_PAYMENT_GUARANTEE.value
            elif "performance" in lg_type_lower or "final" in lg_type_lower or "نهائي" in lg_type_lower or "أداء" in lg_type_lower:
                mapped_data["lg_type_id"] = LgTypeEnum.PERFORMANCE_GUARANTEE.value
            elif "bid" in lg_type_lower or "tender" in lg_type_lower or "ابتدائي" in lg_type_lower:
                mapped_data["lg_type_id"] = LgTypeEnum.BID_BOND.value
            else:
                lg_type_id = find_best_match(extracted_lg_type_name, valid_lg_types_with_ids)
                if lg_type_id:
                    mapped_data["lg_type_id"] = lg_type_id
                else:
                    logger.warning(f"No good fuzzy match found for LG Type '{extracted_lg_type_name}'.")

        # Map Operational Status & Payment Conditions (only applicable for Advance Payment LG)
        if mapped_data.get("lg_type_id") == LgTypeEnum.ADVANCE_PAYMENT_GUARANTEE.value:
            raw_op_status = (extracted_ai_data.get("operationalStatus") or "").strip().lower()
            raw_pay_cond = (extracted_ai_data.get("paymentConditions") or "").strip()
            other_conds = extracted_ai_data.get("otherConditions") or []
            other_conds_text = " ".join(other_conds) if isinstance(other_conds, list) else str(other_conds)
            combined_cond_check = f"{raw_op_status} {raw_pay_cond} {other_conds_text}".lower()

            is_non_operative = (
                "non-operative" in raw_op_status
                or "non operative" in raw_op_status
                or "none operative" in raw_op_status
                or ("conditional" in raw_op_status and "unconditional" not in raw_op_status)
                or ("مشروط" in raw_op_status and "غير مشروط" not in raw_op_status)
                or "non-operative" in combined_cond_check
                or "non operative" in combined_cond_check
            )
            if is_non_operative:
                non_op_obj = next((s for s in all_op_statuses if "non" in s.name.lower() or "none" in s.name.lower()), None)
                mapped_data["lg_operational_status_id"] = non_op_obj.id if non_op_obj else LgOperationalStatusEnum.NON_OPERATIVE.value
                if raw_pay_cond:
                    mapped_data["payment_conditions"] = raw_pay_cond
                elif other_conds_text.strip():
                    mapped_data["payment_conditions"] = other_conds_text.strip()[:1024]
                else:
                    mapped_data["payment_conditions"] = (extracted_ai_data.get("purpose") or "Subject to receipt of advance payment.")[:1024]
            else:
                op_obj = next((s for s in all_op_statuses if s.name.strip().lower() == "operative"), None)
                mapped_data["lg_operational_status_id"] = op_obj.id if op_obj else LgOperationalStatusEnum.OPERATIVE.value
                mapped_data["payment_conditions"] = None

        # Map Applicable Rule
        extracted_rule_name = (extracted_ai_data.get("applicableRule") or "").strip()
        if extracted_rule_name:
            rule_lower = extracted_rule_name.lower()
            if "758" in rule_lower or "urdg" in rule_lower:
                urdg = next((r for r in all_rules if "758" in r.name.lower() or "urdg" in r.name.lower()), None)
                if urdg:
                    mapped_data["applicable_rule_id"] = urdg.id
            elif "isp" in rule_lower or "98" in rule_lower:
                isp = next((r for r in all_rules if "isp" in r.name.lower()), None)
                if isp:
                    mapped_data["applicable_rule_id"] = isp.id
            elif "522" in rule_lower or "urc" in rule_lower:
                urc = next((r for r in all_rules if "522" in r.name.lower() or "urc" in r.name.lower()), None)
                if urc:
                    mapped_data["applicable_rule_id"] = urc.id
            else:
                rule_id = find_best_match(extracted_rule_name, valid_rules_with_ids)
                if rule_id:
                    mapped_data["applicable_rule_id"] = rule_id
                else:
                    other_rule = next((r for r in all_rules if r.name.strip().lower() == "other"), None)
                    if other_rule:
                        mapped_data["applicable_rule_id"] = other_rule.id
                        mapped_data["applicable_rules_text"] = extracted_rule_name[:64]

        # Map Issuing Bank (Local vs Foreign / Advised)
        extracted_issuing_bank_name = (extracted_ai_data.get("issuingBankName") or "").strip()
        extracted_foreign_bank_name = (extracted_ai_data.get("foreign_bank_name") or "").strip()
        extracted_bank_country = (extracted_ai_data.get("foreign_bank_country") or extracted_ai_data.get("issuingBankCountry") or "").strip()
        extracted_advising_bank_name = (extracted_ai_data.get("advisingBankName") or "").strip()

        foreign_bank_record = next((b for b in all_banks if b.name.strip().lower() == "foreign bank"), None)
        matched_issuing_local_bank = _match_local_bank(extracted_issuing_bank_name) if extracted_issuing_bank_name else None
        matched_foreign_field_local_bank = _match_local_bank(extracted_foreign_bank_name) if extracted_foreign_bank_name else None

        is_country_foreign = bool(
            extracted_bank_country
            and extracted_bank_country.lower() not in ["egypt", "مصر", "arab republic of egypt", "are", "eg"]
        )
        has_true_foreign_bank_name = bool(extracted_foreign_bank_name and not matched_foreign_field_local_bank)

        if has_true_foreign_bank_name or is_country_foreign or (extracted_issuing_bank_name and not matched_issuing_local_bank):
            # Foreign Bank Guarantee!
            if foreign_bank_record:
                mapped_data["issuing_bank_id"] = foreign_bank_record.id
            effective_foreign_name = (
                extracted_foreign_bank_name
                if has_true_foreign_bank_name
                else extracted_issuing_bank_name
            )
            mapped_data["foreign_bank_name"] = effective_foreign_name
            mapped_data["foreign_bank_country"] = extracted_bank_country if is_country_foreign else (extracted_ai_data.get("foreign_bank_country") or "")
            mapped_data["foreign_bank_address"] = (extracted_ai_data.get("foreign_bank_address") or "").strip()
            mapped_data["foreign_bank_swift_code"] = (extracted_ai_data.get("foreign_bank_swift_code") or "").strip()

            # Determine Advising / Confirming Bank if present
            advising_candidate = extracted_advising_bank_name
            if not advising_candidate and matched_issuing_local_bank and has_true_foreign_bank_name:
                # In case AI placed the local advising bank letterhead in issuingBankName and the foreign bank in foreign_bank_name
                advising_candidate = extracted_issuing_bank_name

            if advising_candidate:
                matched_comm_bank = _match_local_bank(advising_candidate)
                raw_adv_status = (extracted_ai_data.get("advisingStatus") or "").strip().lower()
                if "confirm" in raw_adv_status and "without" not in raw_adv_status and "not" not in raw_adv_status and "unconfirm" not in raw_adv_status:
                    mapped_data["advising_status"] = "Confirmed"
                else:
                    mapped_data["advising_status"] = "Advised"
                if matched_comm_bank:
                    mapped_data["communication_bank_id"] = matched_comm_bank.id
            else:
                mapped_data["advising_status"] = "Not Advised"
                mapped_data["communication_bank_id"] = None
        elif matched_issuing_local_bank:
            mapped_data["issuing_bank_id"] = matched_issuing_local_bank.id
            mapped_data["issuing_bank_address"] = matched_issuing_local_bank.address or ""
            mapped_data["issuing_bank_phone"] = matched_issuing_local_bank.phone_number or ""
            mapped_data["issuing_bank_fax"] = matched_issuing_local_bank.fax or ""

        response_data = {
            "issuer_name": extracted_ai_data.get("issuerName"),
            "lg_number": extracted_ai_data.get("lgNumber"),
            "lg_amount": extracted_ai_data.get("lgAmount"),
            "description_purpose": extracted_ai_data.get("purpose"),
            "other_conditions": extracted_ai_data.get("otherConditions"),
            "mandatory_claim_statement": extracted_ai_data.get("mandatoryClaimStatement"),
            "issuance_date": extracted_ai_data.get("issuanceDate"),
            "expiry_date": extracted_ai_data.get("expiryDate"),
            "issuer_id": None,
            "lg_payable_currency_id": None,
            "auto_renewal": False,
            "lg_operational_status_id": None,
            "payment_conditions": None,
            "issuing_bank_address": None,
            "issuing_bank_phone": None,
            "issuing_bank_fax": None,
            "foreign_bank_name": None,
            "foreign_bank_country": None,
            "foreign_bank_address": None,
            "foreign_bank_swift_code": None,
            "advising_status": "Not Advised",
            "communication_bank_id": None,
            "issuing_method_id": None,
            "applicable_rule_id": None,
            "applicable_rules_text": None,
            "internal_owner_email": None,
            "internal_owner_phone": None,
            "internal_owner_id": None,
            "manager_email": None,
            "lg_category_id": None,
            "additional_field_values": {},
        }

        response_data.update(mapped_data)

        log_action(db, user_id=end_user_context.user_id, action_type="AI_SCAN_SUCCESS", entity_type="LGRecord", entity_id=None, details={"file_name": file.filename, "extracted_fields": list(response_data.keys()), "ai_token_usage": ai_usage_metadata}, customer_id=end_user_context.customer_id)
        return response_data
    except Exception as e:
        db.rollback()
        log_action(db, user_id=end_user_context.user_id, action_type="AI_SCAN_FAILED", entity_type="LGRecord", entity_id=None, details={"file_name": file.filename, "reason": f"Processing error: {e}", "ai_token_usage": ai_usage_metadata}, customer_id=end_user_context.customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.post(
    "/lg-records/{lg_record_id}/amend/scan-file",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:amend")), Depends(check_for_read_only_mode)],
    summary="Scans an amendment letter file and returns AI-extracted details."
)
async def scan_amendment_letter_file(
    lg_record_id: int,
    amendment_letter_file: UploadFile = File(..., description="Scanned bank amendment letter (PDF or image)"),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Performs AI scanning of a bank amendment letter to extract structured data
    and confirms its relevance to the specified LG record.
    """
    client_host = get_client_ip(request) if request else None

    lg_record = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
    if not lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")

    # --- SURGERY: Fetch User object for AI Logging ---
    from app.models import User
    user_obj = db.query(User).filter(User.id == end_user_context.user_id).first()
    if not user_obj:
        raise HTTPException(status_code=404, detail="User context not found.")
    # ------------------------------------------------

    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan or not customer.subscription_plan.can_ai_integration:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your subscription plan does not support AI integration for file scanning."
        )

    supported_mime_types = ["image/jpeg", "image/png", "application/pdf"]
    if amendment_letter_file.content_type not in supported_mime_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only {', '.join(supported_mime_types)} files are supported for AI scanning."
        )

    file_bytes = await amendment_letter_file.read()
    lg_record_details_for_ai = {
        "lgNumber": lg_record.lg_number,
        "lgAmount": float(lg_record.lg_amount),
        "expiryDate": lg_record.expiry_date.isoformat() if lg_record.expiry_date else None,
        "issuingBankName": lg_record.issuing_bank.name if lg_record.issuing_bank else None,
        "beneficiaryName": lg_record.beneficiary_corporate.entity_name if lg_record.beneficiary_corporate else None,
        "issuerName": lg_record.issuer_name
    }
    
    try:
        # --- SURGERY: Call with required DB and User arguments ---
        extracted_ai_data, _ = await process_amendment_with_ai(
            file_bytes=file_bytes, 
            mime_type=amendment_letter_file.content_type, 
            db=db,                         # Required for logging
            current_user=user_obj,         # Required for customer_id/user_id link
            lg_record_details=lg_record_details_for_ai,
            file_name=amendment_letter_file.filename
        )

        if not extracted_ai_data:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="AI could not determine the amendment type. Please complete the form manually."
            )

        if not extracted_ai_data.get('is_relevant_amendment', False):
            return {"message": "AI could not confirm amendment is related to this specific LG. Please make sure you are amending the correct LG."}

        ai_suggested_details = extracted_ai_data.get('amendedFields', {})
        
        if not ai_suggested_details:
            return {"message": "AI analysis complete, but no amendments were detected. Please fill the details manually."}

        return {"ai_suggested_details": ai_suggested_details}
        
    except HTTPException as e:
        logger.error(f"AI processing failed for amendment letter on LG id={lg_record.id}: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"AI processing failed for amendment letter on LG id={lg_record.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during AI analysis. Please try again or proceed manually."
        )
@router.get("/internal-owner-contacts/lookup-by-email/", response_model=Optional[InternalOwnerContactOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def lookup_internal_owner_by_email(
    email: str = Query(..., description="Email of the internal owner contact to lookup"),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context)
):
    """
    Looks up an InternalOwnerContact by email within the current user's customer organization.
    Returns contact details if found. If not found, returns null (not a 404 error),
    indicating it's a new contact.
    """
    contact = crud_internal_owner_contact.get_by_email_for_customer(
        db, end_user_context.customer_id, email
    )
    if not contact:
        return None
    return contact

@router.get(
    "/internal-owner-contacts/with-lg-count",
    response_model=List[InternalOwnerContactOut],
    dependencies=[Depends(check_subscription_status)],
    summary="Retrieve all internal owner contacts for the current customer with LG count"
)
def list_internal_owner_contacts_with_lg_count(
    db: Session = Depends(get_db),
    current_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Retrieves a list of all active internal owner contacts associated with the authenticated user's customer,
    including a count of active LGs assigned to each contact.
    """
    internal_owners = crud_internal_owner_contact.get_all_for_customer_with_lg_count(
        db,
        customer_id=current_user_context.customer_id,
    )
    return internal_owners

@router.post(
    "/internal-owner-contacts",
    response_model=InternalOwnerContactOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(check_for_read_only_mode)],
    summary="Create a new Internal Owner Contact for the current customer"
)
@router.post(
    "/internal-owner-contacts/",
    response_model=InternalOwnerContactOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(check_for_read_only_mode)],
    include_in_schema=False,
)
def create_internal_owner_contact_api(
    owner_in: InternalOwnerContactCreate,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Creates a new Internal Owner Contact (or restores a soft-deleted one) for the current customer.
    """
    existing_active = crud_internal_owner_contact.get_by_email_for_customer(
        db, end_user_context.customer_id, owner_in.email.strip()
    )
    if existing_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"An internal owner contact with email '{owner_in.email.strip()}' already exists."
        )
    db_owner = crud_internal_owner_contact.create_or_get(
        db,
        obj_in=owner_in,
        customer_id=end_user_context.customer_id,
        user_id=end_user_context.user_id,
    )
    db.commit()
    db.refresh(db_owner)
    return db_owner

@router.delete(
    "/internal-owner-contacts/{owner_id}",
    response_model=Dict[str, str],
    dependencies=[Depends(check_for_read_only_mode)],
    summary="Soft-delete an Internal Owner Contact if not assigned to active LGs"
)
def delete_internal_owner_contact_api(
    owner_id: int,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Soft-deletes an Internal Owner Contact if they do not own any active LG records.
    """
    db_owner_contact = crud_internal_owner_contact.get(db, owner_id)
    if not db_owner_contact or db_owner_contact.customer_id != end_user_context.customer_id or db_owner_contact.is_deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Internal Owner Contact not found or not accessible."
        )
    crud_internal_owner_contact.soft_delete(db, db_owner_contact, end_user_context.user_id)
    db.commit()
    return {"message": f"Internal Owner Contact '{db_owner_contact.email}' deleted successfully."}

@router.get(
    "/lg-documents/{document_id}/view",
    response_model=Dict[str, str], # Will return a JSON with the signed URL
    dependencies=[Depends(HasPermission("lg_document:view")), Depends(check_subscription_status)], # ADDED dependency
    summary="Retrieve a time-limited signed URL for viewing a private LG document"
)
def view_lg_document_securely(
    document_id: int,
    request: Request = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_end_user_context),
    valid_for_seconds: int = Query(900, description="Duration in seconds the signed URL is valid (default 15 mins)")
):
    """
    Generates and returns a time-limited signed URL for viewing a private LG document.
    The user must be authenticated, authorized, and the document must belong to their customer.
    """
    logger.info(f"Attempting to generate signed URL for document ID: {document_id} for user {current_user.email}")

    # 1. Retrieve the LGDocument from the database
    db_document = db.query(models.LGDocument).options(
        selectinload(models.LGDocument.lg_record).selectinload(models.LGRecord.customer)
    ).filter(
        models.LGDocument.id == document_id,
        models.LGDocument.is_deleted == False
    ).first()

    if not db_document:
        logger.warning(f"Document ID {document_id} not found or is deleted.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found or not accessible.")

    # 2. Authorization: Ensure document belongs to the user's customer
    if not db_document.lg_record or db_document.lg_record.customer_id != current_user.customer_id:
        logger.warning(f"Unauthorized access attempt for document ID {document_id} by user from customer {current_user.customer_id}. Document belongs to customer {db_document.lg_record.customer_id if db_document.lg_record else 'N/A'}.")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Document not accessible to your organization.")

    # 3. Mint an ephemeral 15-minute cryptographically signed download ticket
    import time
    from app.core.security_crypto import generate_download_ticket
    from app.core.routing import get_backend_base_url

    backend_base = get_backend_base_url(request)
    ticket_payload = {
        "gcs_uri": db_document.file_path,
        "customer_id": current_user.customer_id,
        "user_id": current_user.user_id,
        "caller_desc": f"Corporate User {current_user.email} (Custody View)",
        "module": "lg_custody",
        "doc_id": document_id,
        "context": "lg_custody_doc",
        "original_filename": db_document.file_name,
        "created_at": int(time.time())
    }
    expiry_mins = max(1, valid_for_seconds // 60)
    ticket = generate_download_ticket(ticket_payload, expiry_minutes=expiry_mins)
    streaming_url = f"{backend_base}/api/v1/documents/download?ticket={ticket}"

    logger.info(f"Successfully generated streaming ticket for document ID {document_id}.")
    log_action(
        db,
        user_id=current_user.user_id,
        action_type="DOCUMENT_VIEWED_SECURELY",
        entity_type="LGDocument",
        entity_id=document_id,
        details={"file_name": db_document.file_name, "signed_url_valid_for_seconds": valid_for_seconds, "is_streaming": True},
        customer_id=current_user.customer_id,
        lg_record_id=db_document.lg_record_id
    )
    return {"signed_url": streaming_url, "download_url": streaming_url}


@router.post(
    "/lg-records/{lg_record_id}/documents",
    response_model=List[LGDocumentOut],
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(check_subscription_status), Depends(check_for_read_only_mode)],
    summary="Upload one or more supporting documents to an LG record and optionally link to a lifecycle event",
)
async def upload_lg_supporting_documents(
    lg_record_id: int,
    files: List[UploadFile] = File(...),
    document_type: str = Form("INTERNAL_SUPPORTING"),
    audit_log_id: Optional[int] = Form(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_end_user_context),
):
    """
    Uploads and encrypts one or more supporting documents (e.g., Original LG Copy, Amendment Letter,
    Internal Supporting Document) directly onto an existing LG record, and optionally binds the
    uploaded document ID to a specific historical AuditLog event (MIGRATION_IMPORT_RECORD or LG_AMENDED).
    """
    lg_rec = (
        db.query(models.LGRecord)
        .options(
            selectinload(models.LGRecord.beneficiary_corporate),
            selectinload(models.LGRecord.lg_category),
        )
        .filter(
            models.LGRecord.id == lg_record_id,
            models.LGRecord.customer_id == current_user.customer_id,
            models.LGRecord.is_deleted == False,
        )
        .first()
    )
    if not lg_rec:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="LG Record not found or not accessible.",
        )

    lg_record_details = {
        "beneficiary_corporate_code": getattr(lg_rec.beneficiary_corporate, "code", "ENTITY") if lg_rec.beneficiary_corporate else "ENTITY",
        "lg_category_code": getattr(lg_rec.lg_category, "code", "CAT") if lg_rec.lg_category else "CAT",
        "lg_number": lg_rec.lg_number,
    }

    created_docs: List[models.LGDocument] = []
    for upload in files:
        if not upload or not upload.filename:
            continue
        file_bytes = await upload.read()
        if not file_bytes:
            continue
        doc_in = LGDocumentCreate(
            document_type=document_type or "INTERNAL_SUPPORTING",
            file_name=upload.filename,
            mime_type=upload.content_type or "application/pdf",
            file_path="",
            lg_instruction_id=None,
        )
        db_doc = await crud_lg_document.create_document(
            db=db,
            obj_in=doc_in,
            file_content=file_bytes,
            lg_record_id=lg_rec.id,
            uploaded_by_user_id=current_user.user_id,
            lg_record_details=lg_record_details,
        )
        created_docs.append(db_doc)

    if not created_docs:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid document files were provided.",
        )

    reseal_needed = False
    if audit_log_id is not None:
        audit_row = (
            db.query(models.AuditLog)
            .filter(
                models.AuditLog.id == audit_log_id,
                models.AuditLog.lg_record_id == lg_rec.id,
                models.AuditLog.customer_id == current_user.customer_id,
            )
            .first()
        )
        if audit_row:
            details_dict = dict(audit_row.details or {})
            details_dict["amendment_document_id"] = created_docs[0].id
            details_dict["document_id"] = created_docs[0].id
            existing_ids = list(details_dict.get("attached_document_ids") or [])
            for d in created_docs:
                if d.id not in existing_ids:
                    existing_ids.append(d.id)
            details_dict["attached_document_ids"] = existing_ids
            audit_row.details = details_dict
            flag_modified(audit_row, "details")
            if audit_row.entry_hash:
                from app.core.audit_crypto import compute_audit_hash
                audit_row.entry_hash = compute_audit_hash(
                    previous_hash=audit_row.previous_hash,
                    timestamp=audit_row.timestamp,
                    user_id=audit_row.user_id,
                    action_type=audit_row.action_type,
                    entity_type=audit_row.entity_type,
                    entity_id=audit_row.entity_id,
                    customer_id=audit_row.customer_id,
                    lg_record_id=audit_row.lg_record_id,
                    details=audit_row.details,
                    ip_address=audit_row.ip_address,
                )
                reseal_needed = True
            db.add(audit_row)

    db.commit()
    if reseal_needed:
        try:
            from app.core.audit_crypto import reseal_audit_log_chain
            reseal_audit_log_chain(db)
        except Exception as reseal_err:
            logger.warning(f"Failed to auto-reseal WORM audit chain after linking document to AuditLog {audit_log_id}: {reseal_err}")
    for d in created_docs:
        db.refresh(d)
    return created_docs


@router.get("/lg-records/", response_model=List[LGRecordOut], dependencies=[Depends(check_subscription_status)])
def list_lg_records(
    skip: int = 0,
    limit: Optional[int] = Query(None, description="Filter limit. Defaults to None (fetches all records)."),
    internal_owner_contact_id: Optional[int] = Query(None, description="Filter LG records by the ID of the internal owner contact"),
    lg_record_ids: Optional[str] = Query(None, description="Optional comma-separated list of LG record IDs"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:view_own")),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Retrieves a list of LG records belonging to the authenticated End User's customer.
    Includes an optional filter for internal owner contact ID.
    If limit is omitted, all records are returned without pagination cap.
    """
    # FIX: Get fresh permissions from DB instead of stale Token
    fresh_has_all_access, fresh_entity_ids = get_fresh_entity_permissions(db, end_user_context.user_id)

    parsed_lg_ids = None
    if lg_record_ids:
        try:
            parsed_lg_ids = [int(x.strip()) for x in lg_record_ids.split(",") if x.strip()]
        except ValueError:
            parsed_lg_ids = None

    lg_records = crud_lg_record.get_all_lg_records_for_customer(
        db,
        customer_id=end_user_context.customer_id,
        internal_owner_contact_id=internal_owner_contact_id,
        skip=skip,
        limit=limit,
        # NEW: Pass the FRESH security parameters
        user_has_all_access=fresh_has_all_access,
        user_allowed_entity_ids=fresh_entity_ids,
        lg_record_ids=parsed_lg_ids
    )
    return lg_records

@router.get(
    "/lg-records/{lg_record_id}",
    response_model=LGRecordOut,
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)],
    summary="Retrieve a single LG record by ID"
)
def get_lg_record_by_id(
    lg_record_id: int,
    db: Session = Depends(get_db),
    current_user_context: TokenData = Depends(get_current_end_user_context)
):
    """
    Fetches a single LG record by its ID, ensuring it belongs to the current user's customer.
    """
    # FIX: Get fresh permissions from DB instead of stale Token
    fresh_has_all_access, fresh_entity_ids = get_fresh_entity_permissions(db, current_user_context.user_id)

    lg_record = crud_lg_record.get_lg_record_with_relations(
        db, 
        lg_record_id, 
        current_user_context.customer_id,
        # NEW: Pass the FRESH security parameters
        user_has_all_access=fresh_has_all_access,
        user_allowed_entity_ids=fresh_entity_ids
    )
    if not lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")
    return lg_record

@router.post(
    "/lg-records/{lg_record_id}/extend",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:extend")), Depends(check_for_read_only_mode)],
    summary="Extend an LG record and return new instruction ID"
)
async def extend_lg_record(
    lg_record_id: int,
    # NEW: Expect the request body to be a JSON object containing the new_expiry_date and optional notes
    request_body: Dict[str, Any] = Body(
        ...,
        example={
            "new_expiry_date": "2026-12-31",
            "notes": "Extending due to a new project milestone.",
        }
    ),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    background_tasks: BackgroundTasks = None,
    request: Request = None
):
    """
    Extends the expiry date of an existing LG record.
    Returns the updated LG record and the ID of the newly created instruction.
    Requires 'lg_record:extend' permission.
    """
    client_host = get_client_ip(request) if request else None
    
    new_expiry_date = request_body.get("new_expiry_date")
    notes = request_body.get("notes") # NEW: Extract notes from the request body

    if not new_expiry_date:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="new_expiry_date is required in the request body."
        )
    
    try:
        new_expiry_date_obj = datetime.strptime(new_expiry_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid date format for new_expiry_date. Please use YYYY-MM-DD."
        )

    db_lg_record = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
    if not db_lg_record:
        log_action(db, user_id=end_user_context.user_id, action_type="LG_EXTEND_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"reason": "LG Record not found or not accessible to user's customer", "new_expiry_date": new_expiry_date}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="LG Record not found or you do not have access to it.")

    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer or Subscription Plan not found.")

    action_type = ACTION_TYPE_LG_EXTEND

    current_action_requires_maker_checker = action_type in ACTION_TYPES_REQUIRING_APPROVAL

    if current_action_requires_maker_checker:
        existing_pending_requests = crud_approval_request.get_pending_requests_for_lg(db, lg_record_id, end_user_context.customer_id)
        if existing_pending_requests:
            first_pending_req = existing_pending_requests[0]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Another action ({first_pending_req.action_type}) is already pending approval for this LG Record (Request ID: {first_pending_req.id}, Status: {first_pending_req.status.value}). This Maker-Checker action cannot be submitted until the pending action is resolved."
            )

    try:
        updated_lg_record_db, latest_instruction_id, _ = await crud_lg_record.extend_lg(
            db,
            lg_record_id=lg_record_id,
            new_expiry_date=new_expiry_date_obj,
            user_id=end_user_context.user_id,
            notes=notes,
            background_tasks=background_tasks
        )
        return {
            "lg_record": LGRecordOut.model_validate(updated_lg_record_db),
            "latest_instruction_id": latest_instruction_id
        }

    except HTTPException as e:
        log_action(db, user_id=end_user_context.user_id, action_type="LG_EXTEND_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"reason": str(e.detail), "new_expiry_date": new_expiry_date}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
        raise
    except Exception as e:
        log_action(db, user_id=end_user_context.user_id, action_type="LG_EXTEND_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"reason": f"An unexpected error occurred: {e}", "new_expiry_date": new_expiry_date}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

# --------------------------------------------------------------------------------------
# 1. /lg-records/{lg_record_id}/release
# --------------------------------------------------------------------------------------
@router.post(
    "/lg-records/{lg_record_id}/release",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:release")), Depends(check_for_read_only_mode)],
    summary="Release an LG record (potentially via Maker-Checker workflow)"
)
async def release_lg_record(
    lg_record_id: int,
    reason: str = Form(..., description="Reason for releasing the LG."),
    notes: Optional[str] = Form(None, description="Additional notes for the action."),
    internal_supporting_document_file: Optional[UploadFile] = File(None, description="Optional internal supporting document."),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Initiates the release of an LG record.
    If Maker-Checker is enabled for the customer, an ApprovalRequest is created.
    Otherwise, the LG is released directly.
    """
    client_host = get_client_ip(request) if request else None
    
    release_in = LGRecordRelease(reason=reason, notes=notes)
    
    db_lg_record = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
    if not db_lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")

    if db_lg_record.lg_status_id != LgStatusEnum.VALID.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"LG record must be in 'Valid' status to be released. Current status: {db_lg_record.lg_status.name}."
        )

    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer or Subscription Plan not found.")

    action_type = ACTION_TYPE_LG_RELEASE

    # --- NEW ENFORCEMENT LOGIC: Check if supporting document is mandatory ---
    is_doc_mandatory_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.DOC_MANDATORY_RELEASE
    )
    is_doc_mandatory = is_doc_mandatory_config.get('effective_value', 'False').lower() == 'true'

    # The file is considered 'missing' if it's None OR if it's an empty upload (e.g., from form data without file selection)
    file_provided = internal_supporting_document_file is not None and internal_supporting_document_file.filename != ''
    
    if is_doc_mandatory and not file_provided:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Supporting document is mandatory for LG Release as per Corporate Admin settings."
        )
    # --- END NEW ENFORCEMENT LOGIC ---

    current_action_requires_maker_checker = action_type in ACTION_TYPES_REQUIRING_APPROVAL

    document_id_for_approval_request = None
    
    # --- DOCUMENT UPLOAD LOGIC (Executed only if file was provided, mandatory or optional) ---
    if file_provided:
        if not customer.subscription_plan.can_image_storage:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Your subscription plan does not support document storage.")
        
        # NOTE: File content must be read *after* the mandatory check to ensure it's only read once.
        file_bytes = await internal_supporting_document_file.read()
        
        document_metadata = LGDocumentCreate(
            document_type="INTERNAL_SUPPORTING",
            file_name=internal_supporting_document_file.filename,
            file_path="",
            mime_type=internal_supporting_document_file.content_type
        )
        try:
            db_document = await crud_lg_document.create_document(
                db,
                obj_in=document_metadata,
                file_content=file_bytes,
                lg_record_id=db_lg_record.id,
                uploaded_by_user_id=end_user_context.user_id
            )
            document_id_for_approval_request = db_document.id
        except Exception as e:
            logger.error(f"Failed to store supporting document for LG {lg_record_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to store supporting document: {e}")
    # --- END DOCUMENT UPLOAD LOGIC ---

    if current_action_requires_maker_checker:
        existing_pending_requests = crud_approval_request.get_pending_requests_for_lg(db, lg_record_id, end_user_context.customer_id)
        if existing_pending_requests:
            first_pending_req = existing_pending_requests[0]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Another action ({first_pending_req.action_type}) is already pending approval for this LG Record (Request ID: {first_pending_req.id}, Status: {first_pending_req.status.value}). This Maker-Checker action cannot be submitted until the pending action is resolved."
            )

    if customer.subscription_plan.can_maker_checker:
        # Maker-Checker flow: Create Approval Request
        try:
            lg_snapshot = crud_approval_request._get_lg_record_snapshot(db_lg_record)
            
            request_details = release_in.model_dump()
            if document_id_for_approval_request:
                # Store the document ID in the request_details for the corporate admin to review
                request_details["supporting_document_id"] = document_id_for_approval_request
            
            approval_request_in = ApprovalRequestCreate(
                entity_type="LGRecord",
                entity_id=lg_record_id,
                action_type=action_type,
                request_details=request_details,
                lg_record_snapshot=lg_snapshot
            )
            
            # **AWAIT** the asynchronous CRUD method call
            db_approval_request = await crud_approval_request.create_approval_request(
                db,
                approval_request_in,
                end_user_context.user_id,
                end_user_context.customer_id,
                lg_record=db_lg_record
            )

            log_action(
                db,
                user_id=end_user_context.user_id,
                action_type="APPROVAL_REQUEST_SUBMITTED",
                entity_type="ApprovalRequest",
                entity_id=db_approval_request.id,
                details={
                    "lg_record_id": lg_record_id,
                    "action_type": action_type,
                    "status": db_approval_request.status.value,
                    "maker_email": end_user_context.email,
                    "reason_for_request": release_in.reason,
                    "notes_for_request": release_in.notes,
                    "supporting_document_id": document_id_for_approval_request,
                },
                customer_id=end_user_context.customer_id,
                lg_record_id=lg_record_id,
            )
            return {
                "message": f"LG Release request for LG '{db_lg_record.lg_number}' submitted for approval.",
                "approval_request_id": db_approval_request.id,
                "status": db_approval_request.status.value,
                "lg_record": LGRecordOut.model_validate(db_lg_record)
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_RELEASE_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_RELEASE_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during approval request creation: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during approval request creation: {e}"
            )
    else:
        # Direct Execution flow
        try:
            released_lg, latest_instruction_id = await crud_lg_record.release_lg(
                db,
                lg_record=db_lg_record,
                user_id=end_user_context.user_id,
                approval_request_id=None,
                supporting_document_id=document_id_for_approval_request,
                notes=release_in.notes
            )

            return {
                "message": f"LG '{released_lg.lg_number}' released directly.",
                "lg_record": LGRecordOut.model_validate(released_lg),
                "latest_instruction_id": latest_instruction_id
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_RELEASE_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_RELEASE_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during direct release: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during direct release: {e}"
            )
# --------------------------------------------------------------------------------------
# 2. /lg-records/{lg_record_id}/liquidate
# --------------------------------------------------------------------------------------
@router.post(
    "/lg-records/{lg_record_id}/liquidate",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:liquidate")), Depends(check_for_read_only_mode)],
    summary="Liquidate an LG record (potentially via Maker-Checker workflow)"
)
async def liquidate_lg_record(
    lg_record_id: int,
    # FIX: Change to Form parameters to handle file uploads
    liquidation_type: str = Form(..., description="Type of liquidation: 'full' or 'partial'."),
    new_amount: Optional[float] = Form(None, description="The new amount of the LG if partial liquidation."),
    reason: str = Form(..., description="Reason for liquidating the LG."),
    notes: Optional[str] = Form(None, description="Additional notes for the action."), # ADDED THIS LINE
    internal_supporting_document_file: Optional[UploadFile] = File(None, description="Optional supporting document."),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Initiates the liquidation of an LG record (full or partial).
    If Maker-Checker is enabled for the customer, an ApprovalRequest is created.
    Otherwise, the LG is liquidated directly.
    """
    client_host = get_client_ip(request) if request else None

    liquidation_in = LGRecordLiquidation(
        liquidation_type=liquidation_type,
        new_amount=new_amount,
        reason=reason,
        notes=notes
    )

    db_lg_record = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
    if not db_lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")

    if db_lg_record.lg_status_id != LgStatusEnum.VALID.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"LG record must be in 'Valid' status to be released. Current status: {db_lg_record.lg_status.name}."
        )
        
    if liquidation_in.liquidation_type == "partial" and (liquidation_in.new_amount is None or liquidation_in.new_amount <= 0 or liquidation_in.new_amount >= db_lg_record.lg_amount):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="For partial liquidation, 'new_amount' must be a positive value less than the current LG amount.")
    elif liquidation_in.liquidation_type == "full" and liquidation_in.new_amount is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="For full liquidation, 'new_amount' should not be provided.")


    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer or Subscription Plan not found.")

    action_type = ACTION_TYPE_LG_LIQUIDATE
    
    # --- NEW ENFORCEMENT LOGIC: Check if supporting document is mandatory ---
    is_doc_mandatory_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.DOC_MANDATORY_LIQUIDATION # <--- NEW KEY
    )
    is_doc_mandatory = is_doc_mandatory_config.get('effective_value', 'False').lower() == 'true'

    if is_doc_mandatory and internal_supporting_document_file is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Supporting document is mandatory for LG Liquidation as per Corporate Admin settings."
        )
    # --- END NEW ENFORCEMENT LOGIC ---
    
    current_action_requires_maker_checker = action_type in ACTION_TYPES_REQUIRING_APPROVAL
    
    document_id_for_approval_request = None
    if internal_supporting_document_file:
        if not customer.subscription_plan.can_image_storage:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Your subscription plan does not support document storage.")
        
        file_bytes = await internal_supporting_document_file.read()
        document_metadata = LGDocumentCreate(
            document_type="INTERNAL_SUPPORTING",
            file_name=internal_supporting_document_file.filename,
            file_path="",
            mime_type=internal_supporting_document_file.content_type
        )
        try:
            db_document = await crud_lg_document.create_document(
                db,
                obj_in=document_metadata,
                file_content=file_bytes,
                lg_record_id=db_lg_record.id,
                uploaded_by_user_id=end_user_context.user_id
            )
            document_id_for_approval_request = db_document.id
        except Exception as e:
            logger.error(f"Failed to store supporting document for LG {lg_record_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to store supporting document: {e}")

    if current_action_requires_maker_checker:
        existing_pending_requests = crud_approval_request.get_pending_requests_for_lg(db, lg_record_id, end_user_context.customer_id)
        if existing_pending_requests:
            first_pending_req = existing_pending_requests[0]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Another action ({first_pending_req.action_type}) is already pending approval for this LG Record (Request ID: {first_pending_req.id}, Status: {first_pending_req.status.value}). This Maker-Checker action cannot be submitted until the pending action is resolved."
            )


    if customer.subscription_plan.can_maker_checker:
        # Maker-Checker flow: Create Approval Request
        try:
            lg_snapshot = crud_approval_request._get_lg_record_snapshot(db_lg_record)

            request_details = liquidation_in.model_dump()
            if document_id_for_approval_request:
                request_details["supporting_document_id"] = document_id_for_approval_request
                
            approval_request_in = ApprovalRequestCreate(
                entity_type="LGRecord",
                entity_id=lg_record_id,
                action_type=action_type,
                request_details=request_details,
                lg_record_snapshot=lg_snapshot
            )
            
            # **AWAIT** the asynchronous CRUD method call
            db_approval_request = await crud_approval_request.create_approval_request(
                db,
                approval_request_in,
                end_user_context.user_id,
                end_user_context.customer_id,
                lg_record=db_lg_record
            )

            log_action(
                db,
                user_id=end_user_context.user_id,
                action_type="APPROVAL_REQUEST_SUBMITTED",
                entity_type="ApprovalRequest",
                entity_id=db_approval_request.id,
                details={
                    "lg_record_id": lg_record_id,
                    "action_type": action_type,
                    "status": db_approval_request.status.value,
                    "maker_email": end_user_context.email,
                    "liquidation_type": liquidation_in.liquidation_type,
                    "new_amount": liquidation_in.new_amount,
                    "reason_for_request": liquidation_in.reason,
                    "supporting_document_id": document_id_for_approval_request,
                    "notes": liquidation_in.notes,
                },
                customer_id=end_user_context.customer_id,
                lg_record_id=lg_record_id,
            )
            return {
                "message": f"LG Liquidation ({liquidation_in.liquidation_type}) request for LG '{db_lg_record.lg_number}' submitted for approval.",
                "approval_request_id": db_approval_request.id,
                "status": db_approval_request.status.value,
                "lg_record": LGRecordOut.model_validate(db_lg_record)
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_LIQUIDATE_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_LIQUIDATE_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during approval request creation: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during approval request creation: {e}"
            )
    else:
        # Direct Execution flow
        try:
            liquidated_lg, latest_instruction_id = await crud_lg_record.liquidate_lg(
                db,
                lg_record=db_lg_record,
                liquidation_type=liquidation_in.liquidation_type,
                new_amount=liquidation_in.new_amount,
                user_id=end_user_context.user_id,
                approval_request_id=None,
                supporting_document_id=document_id_for_approval_request,
                notes=liquidation_in.notes
            )

            return {
                "message": f"LG '{liquidated_lg.lg_number}' liquidated ({liquidation_in.liquidation_type}) directly.",
                "lg_record": LGRecordOut.model_validate(liquidated_lg),
                "latest_instruction_id": latest_instruction_id
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_LIQUIDATE_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_LIQUIDATE_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during direct liquidation: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during direct liquidation: {e}"
            )

# --------------------------------------------------------------------------------------
# 3. /lg-records/{lg_record_id}/decrease-amount
# --------------------------------------------------------------------------------------
@router.post(
    "/lg-records/{lg_record_id}/decrease-amount",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:decrease_amount")), Depends(check_for_read_only_mode)],
    summary="Decrease the amount of an LG record (potentially via Maker-Checker workflow)"
)
async def decrease_lg_amount_record(
    lg_record_id: int,
    decrease_amount: float = Form(..., gt=0, description="The amount to decrease the LG by."),
    reason: str = Form(..., description="Reason for decreasing the LG amount."),
    notes: Optional[str] = Form(None, description="Additional notes for the action."), # NEW: Add notes field
    internal_supporting_document_file: Optional[UploadFile] = File(None, description="Optional internal supporting document."),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Initiates the decrease of an LG record's amount.
    If Maker-Checker is enabled for the customer, an ApprovalRequest is created.
    Otherwise, the LG amount is decreased directly.
    """
    client_host = get_client_ip(request) if request else None

    # NEW: Include notes in the decrease_in object
    decrease_in = LGRecordDecreaseAmount(decrease_amount=decrease_amount, reason=reason, notes=notes)

    db_lg_record = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
    if not db_lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")

    if db_lg_record.lg_status_id not in [LgStatusEnum.VALID.value]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"LG record must be in 'Valid' status to decrease amount. Current status: {db_lg_record.lg_status.name}."
        )

    if decrease_in.decrease_amount <= 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Decrease amount must be greater than 0.")
    if decrease_in.decrease_amount >= db_lg_record.lg_amount:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Decrease amount must be less than the current LG amount. Use liquidation for full reduction.")

    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer or Subscription Plan not found.")

    action_type = ACTION_TYPE_LG_DECREASE_AMOUNT

    # --- NEW ENFORCEMENT LOGIC: Check if supporting document is mandatory ---
    is_doc_mandatory_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.DOC_MANDATORY_DECREASE_AMOUNT # <--- NEW KEY
    )
    is_doc_mandatory = is_doc_mandatory_config.get('effective_value', 'False').lower() == 'true'

    if is_doc_mandatory and internal_supporting_document_file is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Supporting document is mandatory for LG Decrease Amount as per Corporate Admin settings."
        )
    # --- END NEW ENFORCEMENT LOGIC ---

    current_action_requires_maker_checker = action_type in ACTION_TYPES_REQUIRING_APPROVAL
    
    document_id_for_approval_request = None
    if internal_supporting_document_file:
        if not customer.subscription_plan.can_image_storage:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Your subscription plan does not support document storage.")
        
        file_bytes = await internal_supporting_document_file.read()
        document_metadata = LGDocumentCreate(
            document_type="INTERNAL_SUPPORTING",
            file_name=internal_supporting_document_file.filename,
            file_path="",
            mime_type=internal_supporting_document_file.content_type
        )
        try:
            db_document = await crud_lg_document.create_document(
                db,
                obj_in=document_metadata,
                file_content=file_bytes,
                lg_record_id=db_lg_record.id,
                uploaded_by_user_id=end_user_context.user_id
            )
            document_id_for_approval_request = db_document.id
        except Exception as e:
            logger.error(f"Failed to store supporting document for LG {lg_record_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to store supporting document: {e}")

    if current_action_requires_maker_checker:
        existing_pending_requests = crud_approval_request.get_pending_requests_for_lg(db, lg_record_id, end_user_context.customer_id)
        if existing_pending_requests:
            first_pending_req = existing_pending_requests[0]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Another action ({first_pending_req.action_type}) is already pending approval for this LG Record (Request ID: {first_pending_req.id}, Status: {first_pending_req.status.value}). This Maker-Checker action cannot be submitted until the pending action is resolved."
            )

    if customer.subscription_plan.can_maker_checker:
        try:
            lg_snapshot = crud_approval_request._get_lg_record_snapshot(db_lg_record)

            # NEW: Include notes in the request_details
            request_details = decrease_in.model_dump()
            if document_id_for_approval_request:
                request_details["supporting_document_id"] = document_id_for_approval_request
                
            approval_request_in = ApprovalRequestCreate(
                entity_type="LGRecord",
                entity_id=lg_record_id,
                action_type=action_type,
                request_details=request_details,
                lg_record_snapshot=lg_snapshot
            )
            
            # **AWAIT** the asynchronous CRUD method call
            db_approval_request = await crud_approval_request.create_approval_request(
                db,
                approval_request_in,
                end_user_context.user_id,
                end_user_context.customer_id,
                lg_record=db_lg_record
            )
            
            log_action(
                db,
                user_id=end_user_context.user_id,
                action_type="APPROVAL_REQUEST_SUBMITTED",
                entity_type="ApprovalRequest",
                entity_id=db_approval_request.id,
                details={
                    "lg_record_id": lg_record_id,
                    "action_type": action_type,
                    "status": db_approval_request.status.value,
                    "maker_email": end_user_context.email,
                    "decrease_amount": decrease_in.decrease_amount,
                    "reason_for_request": decrease_in.reason,
                    "notes": decrease_in.notes,
                    "supporting_document_id": document_id_for_approval_request
                },
                customer_id=end_user_context.customer_id,
                lg_record_id=lg_record_id,
            )
            return {
                "message": f"LG Decrease Amount request for LG '{db_lg_record.lg_number}' submitted for approval.",
                "approval_request_id": db_approval_request.id,
                "status": db_approval_request.status.value,
                "lg_record": LGRecordOut.model_validate(db_lg_record)
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_DECREASE_AMOUNT_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_DECREASE_AMOUNT_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during approval request creation: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during approval request creation: {e}"
            )
    else:
        try:
            updated_lg, latest_instruction_id = await crud_lg_record.decrease_lg_amount(
                db,
                lg_record=db_lg_record,
                decrease_amount=decrease_in.decrease_amount,
                user_id=end_user_context.user_id,
                approval_request_id=None,
                supporting_document_id=document_id_for_approval_request,
                notes=decrease_in.notes
            )
            return {
                "message": f"LG '{updated_lg.lg_number}' amount decreased directly.",
                "lg_record": LGRecordOut.model_validate(updated_lg),
                "latest_instruction_id": latest_instruction_id
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_DECREASE_AMOUNT_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_DECREASE_AMOUNT_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during direct decrease: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during direct decrease: {e}"
            )

# --------------------------------------------------------------------------------------
# 4. /lg-records/{lg_record_id}/activate-non-operative
# --------------------------------------------------------------------------------------
@router.post(
    "/lg-records/{lg_record_id}/activate-non-operative",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:activate_non_operative")), Depends(check_for_read_only_mode)],
    summary="Activate a Non-Operative LG (potentially via Maker-Checker workflow)"
)
async def activate_non_operative_lg_record(
    lg_record_id: int,
    payment_method: str = Form(..., description="Payment method used (e.g., 'Wire', 'Check')"),
    currency_id: int = Form(..., description="ID of the Currency for the payment."),
    amount: float = Form(..., gt=0, description="The payment amount."),
    payment_reference: Optional[str] = Form("N/A", max_length=100, description="Wire reference or check number."),
    issuing_bank_id: int = Form(..., description="ID of the Issuing Bank related to the payment."), # NEW: Add this line
    payment_date: date = Form(..., description="Date the payment was made (DD/MM/YYYY)."),
    notes: Optional[str] = Form(None, description="Additional notes for the action."),
    internal_supporting_document_file: Optional[UploadFile] = File(None, description="Optional internal supporting document."),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Activates a non-operative Advance Payment Guarantee by processing payment details.
    If Maker-Checker is enabled, an ApprovalRequest is created. Otherwise, the LG is activated directly.
    """
    client_host = get_client_ip(request) if request else None

    activate_in = LGActivateNonOperativeRequest(
        payment_method=payment_method,
        currency_id=currency_id,
        amount=amount,
        payment_reference=payment_reference or "N/A",
        issuing_bank_id=issuing_bank_id,
        payment_date=payment_date,
        notes=notes
    )

    db_lg_record = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
    if not db_lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")

    if db_lg_record.lg_type_id != LgTypeEnum.ADVANCE_PAYMENT_GUARANTEE.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only 'Advance Payment Guarantee' LG types can be activated via this process."
        )

    if db_lg_record.lg_status_id != LgStatusEnum.VALID.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"LG record must be in 'Valid' status to be activated. Current status: {db_lg_record.lg_status.name}."
        )

    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer or Subscription Plan not found.")

    action_type = ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE

    # --- NEW ENFORCEMENT LOGIC: Check if supporting document is mandatory ---
    is_doc_mandatory_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.DOC_MANDATORY_ACTIVATE # <--- NEW KEY
    )
    is_doc_mandatory = is_doc_mandatory_config.get('effective_value', 'False').lower() == 'true'

    if is_doc_mandatory and internal_supporting_document_file is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Supporting document is mandatory for LG Activation as per Corporate Admin settings."
        )
    # --- END NEW ENFORCEMENT LOGIC ---
    
    document_id_for_approval_request = None
    if internal_supporting_document_file:
        if not customer.subscription_plan.can_image_storage:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Your subscription plan does not support document storage.")
        
        file_bytes = await internal_supporting_document_file.read()
        document_metadata = LGDocumentCreate(
            document_type="INTERNAL_SUPPORTING",
            file_name=internal_supporting_document_file.filename,
            file_path="",
            mime_type=internal_supporting_document_file.content_type
        )
        try:
            db_document = await crud_lg_document.create_document(
                db,
                obj_in=document_metadata,
                file_content=file_bytes,
                lg_record_id=db_lg_record.id,
                uploaded_by_user_id=end_user_context.user_id
            )
            document_id_for_approval_request = db_document.id
        except Exception as e:
            logger.error(f"Failed to store supporting document for LG {lg_record_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to store supporting document: {e}")

    serialized_payment_details = activate_in.model_dump_for_json()
    if document_id_for_approval_request:
        serialized_payment_details["supporting_document_id"] = document_id_for_approval_request

    current_action_requires_maker_checker = action_type in ACTION_TYPES_REQUIRING_APPROVAL
    
    if current_action_requires_maker_checker:
        existing_pending_requests = crud_approval_request.get_pending_requests_for_lg(db, lg_record_id, end_user_context.customer_id)
        if existing_pending_requests:
            first_pending_req = existing_pending_requests[0]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Another action ({first_pending_req.action_type}) is already pending approval for this LG Record (Request ID: {first_pending_req.id}, Status: {first_pending_req.status.value}). This Maker-Checker action cannot be submitted until the pending action is resolved."
            )

    if customer.subscription_plan.can_maker_checker:
        try:
            lg_snapshot = crud_approval_request._get_lg_record_snapshot(db_lg_record)

            approval_request_in = ApprovalRequestCreate(
                entity_type="LGRecord",
                entity_id=lg_record_id,
                action_type=action_type,
                request_details=serialized_payment_details,
                lg_record_snapshot=lg_snapshot
            )
            
            # **AWAIT** the asynchronous CRUD method call
            db_approval_request = await crud_approval_request.create_approval_request(
                db,
                approval_request_in,
                end_user_context.user_id,
                end_user_context.customer_id,
                lg_record=db_lg_record
            )
            
            log_action(
                db,
                user_id=end_user_context.user_id,
                action_type="APPROVAL_REQUEST_SUBMITTED",
                entity_type="ApprovalRequest",
                entity_id=db_approval_request.id,
                details={
                    "lg_record_id": lg_record_id,
                    "action_type": action_type,
                    "status": db_approval_request.status.value,
                    "maker_email": end_user_context.email,
                    "payment_details": serialized_payment_details,
                },
                customer_id=end_user_context.customer_id,
                lg_record_id=lg_record_id,
            )
            return {
                "message": f"LG Activation request for LG '{db_lg_record.lg_number}' submitted for approval.",
                "approval_request_id": db_approval_request.id,
                "status": db_approval_request.status.value,
                "lg_record": LGRecordOut.model_validate(db_lg_record)
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_ACTIVATED_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail), "payment_details": serialized_payment_details}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_ACTIVATED_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during approval request creation: {e}", "payment_details": serialized_payment_details}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during approval request creation: {e}"
            )
    else:
        try:
            activated_lg, latest_instruction_id = await crud_lg_record.activate_non_operative_lg(
                db,
                lg_record=db_lg_record,
                payment_details=activate_in,
                user_id=end_user_context.user_id,
                customer_id=end_user_context.customer_id,
                approval_request_id=None,
                supporting_document_id=document_id_for_approval_request,
                notes=activate_in.notes
            )
            return {
                "message": f"LG '{activated_lg.lg_number}' activated directly.",
                "lg_record": LGRecordOut.model_validate(activated_lg),
                "latest_instruction_id": latest_instruction_id
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_ACTIVATED_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail), "payment_details": serialized_payment_details}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_ACTIVATED_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during direct activation: {e}", "payment_details": serialized_payment_details}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during direct activation: {e}"
            )

# --------------------------------------------------------------------------------------
# 5. /lg-records/{lg_record_id}/amend
# --------------------------------------------------------------------------------------
@router.post(
    "/lg-records/{lg_record_id}/amend",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:amend")), Depends(check_for_read_only_mode)],
    summary="Initiate an amendment for an LG record (potentially via Maker-Checker workflow)"
)
async def amend_lg_record(
    lg_record_id: int,
    # Use standard dependencies to let FastAPI handle the parsing
    amendment_details_json_str: str = Form(..., alias="amendment_details", description="JSON string of LGRecord fields to be amended"),
    reason: Optional[str] = Form(None, alias="reason", description="Reason for amending the LG."),
    amendment_letter_file: UploadFile = File(..., description="Scanned bank amendment letter (PDF or image)"),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Initiates an amendment process for an LG record based on a bank amendment letter.
    If Maker-Checker is enabled, an ApprovalRequest is created. Otherwise, the LG is amended directly.
    The `amendment_details` is provided as a JSON string.
    """
    client_host = get_client_ip(request) if request else None

    try:
        parsed_amendment_details = json.loads(amendment_details_json_str)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON format for 'amendment_details'."
        )

    amendment_in = LGRecordAmendRequest(
        amendment_details=parsed_amendment_details,
        reason=reason
    )

    db_lg_record = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
    if not db_lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")

    # Apply new business logic for amendment eligibility (expired LGs within 35 days)
    current_date = date.today()
    thirty_five_days_ago = current_date - timedelta(days=35)
    
    is_valid = db_lg_record.lg_status_id == LgStatusEnum.VALID.value
    is_expired_within_grace = (db_lg_record.lg_status_id == LgStatusEnum.EXPIRED.value and db_lg_record.expiry_date.date() >= thirty_five_days_ago)

    if not is_valid and not is_expired_within_grace:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"LG record cannot be amended. It is expired more than 35 days ago or in a non-amendable status. Current status: {db_lg_record.lg_status.name}."
        )

    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer or Subscription Plan not found.")

    action_type = ACTION_TYPE_LG_AMEND
    current_action_requires_maker_checker = action_type in ACTION_TYPES_REQUIRING_APPROVAL

    if current_action_requires_maker_checker:
        existing_pending_requests = crud_approval_request.get_pending_requests_for_lg(db, lg_record_id, end_user_context.customer_id)
        if existing_pending_requests:
            first_pending_req = existing_pending_requests[0]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Another action ({first_pending_req.action_type}) is already pending approval for this LG Record (Request ID: {first_pending_req.id}, Status: {first_pending_req.status.value}). This Maker-Checker action cannot be submitted until the pending action is resolved."
            )

    document_id_for_approval_request = None

    if customer.subscription_plan.can_image_storage:
        try:
            file_bytes = await amendment_letter_file.read()
            amendment_document_metadata = LGDocumentCreate(
                document_type="AMENDMENT_LETTER",
                file_name=amendment_letter_file.filename,
                file_path="",
                mime_type=amendment_letter_file.content_type
            )
            db_amendment_document = await crud_lg_document.create_document(
                db,
                obj_in=amendment_document_metadata,
                file_content=file_bytes,
                lg_record_id=db_lg_record.id,
                uploaded_by_user_id=end_user_context.user_id
            )
            document_id_for_approval_request = db_amendment_document.id
            logger.debug(f"[end_user.py] Amendment letter document stored by maker: {db_amendment_document.file_path}, ID: {document_id_for_approval_request}")
        except Exception as e:
            logger.error(f"[end_user.py] Failed to store amendment letter for LG {lg_record_id} at maker submission: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to store amendment letter: {e}")
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your subscription plan does not support document storage for amendment letters."
        )

    if customer.subscription_plan.can_maker_checker:
        try:
            lg_snapshot = crud_approval_request._get_lg_record_snapshot(db_lg_record)

            approval_request_in = ApprovalRequestCreate(
                entity_type="LGRecord",
                entity_id=lg_record_id,
                action_type=action_type,
                request_details={
                    "amendment_details": parsed_amendment_details,
                    "reason": reason,
                    "amendment_document_id": document_id_for_approval_request,
                },
                lg_record_snapshot=lg_snapshot
            )
            
            # **AWAIT** the asynchronous CRUD method call
            db_approval_request = await crud_approval_request.create_approval_request(
                db,
                approval_request_in,
                end_user_context.user_id,
                end_user_context.customer_id,
                lg_record=db_lg_record
            )

            log_action(
                db,
                user_id=end_user_context.user_id,
                action_type="APPROVAL_REQUEST_SUBMITTED",
                entity_type="ApprovalRequest",
                entity_id=db_approval_request.id,
                details={
                    "lg_record_id": lg_record_id,
                    "action_type": action_type,
                    "status": db_approval_request.status.value,
                    "maker_email": end_user_context.email,
                    "requested_amendments": amendment_in.amendment_details,
                    "reason_for_request": amendment_in.reason,
                    "amendment_document_id": document_id_for_approval_request,
                },
                customer_id=end_user_context.customer_id,
                lg_record_id=lg_record_id,
            )
            return {
                "message": f"LG Amendment request for LG '{db_lg_record.lg_number}' submitted for approval.",
                "approval_request_id": db_approval_request.id,
                "status": db_approval_request.status.value,
                "lg_record": LGRecordOut.model_validate(db_lg_record)
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_AMEND_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_AMEND_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during approval request creation: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during approval request creation: {e}"
            )
    else:
        try:
            updated_lg = await crud_lg_record.amend_lg(
                db,
                lg_record_id=lg_record_id,
                amendment_letter_file=amendment_letter_file,
                amendment_document_metadata=None,
                amendment_details=amendment_in.amendment_details,
                user_id=end_user_context.user_id,
                customer_id=end_user_context.customer_id,
                approval_request_id=None,
                existing_document_id=document_id_for_approval_request
            )

            return {
                "message": f"LG '{updated_lg.lg_number}' amended directly.",
                "lg_record": LGRecordOut.model_validate(updated_lg),
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_AMEND_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_AMEND_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred during direct amendment: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during direct amendment: {e}"
            )

@router.post(
    "/lg-records/{lg_record_id}/toggle-auto-renewal",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:amend")), Depends(check_for_read_only_mode)], # ADDED dependency
    summary="Toggle the auto-renewal status of an LG record directly"
)
async def toggle_lg_auto_renewal_api(
    lg_record_id: int,
    toggle_in: LGRecordToggleAutoRenewalRequest,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Toggles the auto-renewal status of an LG record directly, without using the Maker-Checker workflow.
    No approval request is created, and no notification email is sent.
    """
    client_host = get_client_ip(request) if request else None

    db_lg_record = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
    if not db_lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")

    try:
        updated_lg = await crud_lg_record.toggle_lg_auto_renewal(
            db,
            lg_record=db_lg_record,
            new_auto_renewal_status=toggle_in.auto_renewal,
            user_id=end_user_context.user_id,
            customer_id=end_user_context.customer_id,
            reason=toggle_in.reason,
            approval_request_id=None
        )

        return {
            "message": f"LG '{updated_lg.lg_number}' auto-renewal toggled successfully to {'ON' if updated_lg.auto_renewal else 'OFF'}.",
            "lg_record": LGRecordOut.model_validate(updated_lg),
        }
    except HTTPException as e:
        log_action(db, user_id=end_user_context.user_id, action_type="LG_TOGGLE_AUTO_RENEWAL_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
        raise
    except Exception as e:
        log_action(db, user_id=end_user_context.user_id, action_type="LG_TOGGLE_AUTO_RENEWAL_FAILED", entity_type="LGRecord", entity_id=lg_record_id, details={"lg_number": db_lg_record.lg_number, "reason": f"An unexpected error occurred: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )
# NEW ENDPOINT: Update Internal Owner Contact Details (Scenario 1)
# --------------------------------------------------------------------------------------
# 6. /internal-owner-contacts/{owner_id} (Update Details)
# --------------------------------------------------------------------------------------
@router.put(
    "/internal-owner-contacts/{owner_id}",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:amend")), Depends(check_for_read_only_mode)],
    summary="Update details of an existing Internal Owner Contact (via Maker-Checker workflow)"
)
async def update_internal_owner_contact_details_api(
    owner_id: int,
    owner_details_in: InternalOwnerContactUpdateDetails,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Allows updating details (email, phone, etc.) of an existing internal owner contact.
    This action goes through the Maker-Checker workflow if enabled.
    """
    db_owner_contact = crud_internal_owner_contact.get(db, owner_id)
    if not db_owner_contact or db_owner_contact.customer_id != end_user_context.customer_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Internal Owner Contact not found or not accessible.")

    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer or Subscription Plan not found.")

    action_type = ACTION_TYPE_LG_CHANGE_OWNER_DETAILS
    
    current_action_requires_maker_checker = action_type in ACTION_TYPES_REQUIRING_APPROVAL

    if current_action_requires_maker_checker:
        # NOTE: Using a general placeholder for pending requests here as a specific entity type
        # is hard to validate against in a generic way for non-LG objects. Assuming `entity_type` and `entity_id` works for ApprovalRequest model.
        existing_pending_requests = crud_approval_request.get_pending_requests_for_lg(db, owner_id, end_user_context.customer_id)
        if existing_pending_requests:
            first_pending_req = existing_pending_requests[0]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Another action ({first_pending_req.action_type}) is already pending approval for this entity (Request ID: {first_pending_req.id}, Status: {first_pending_req.status.value})."
            )

    if customer.subscription_plan.can_maker_checker:
        try:
            owner_snapshot = crud_approval_request._get_internal_owner_contact_snapshot(db_owner_contact)

            approval_request_in = ApprovalRequestCreate(
                entity_type="InternalOwnerContact",
                entity_id=owner_id,
                action_type=action_type,
                request_details=owner_details_in.model_dump(),
                lg_record_snapshot=owner_snapshot,
            )
            
            # **AWAIT** the asynchronous CRUD method call
            db_approval_request = await crud_approval_request.create_approval_request(
                db,
                approval_request_in,
                end_user_context.user_id,
                end_user_context.customer_id,
                internal_owner_contact=db_owner_contact,
            )

            log_action(
                db,
                user_id=end_user_context.user_id,
                action_type="APPROVAL_REQUEST_SUBMITTED",
                entity_type="ApprovalRequest",
                entity_id=db_approval_request.id,
                details={
                    "owner_contact_id": owner_id,
                    "action_type": action_type,
                    "status": db_approval_request.status.value,
                    "maker_email": end_user_context.email,
                    "requested_changes": owner_details_in.model_dump(),
                },
                customer_id=end_user_context.customer_id,
                lg_record_id=None,
            )
            return {
                "message": f"Internal Owner Contact details update request for '{db_owner_contact.email}' submitted for approval.",
                "approval_request_id": db_approval_request.id,
                "status": db_approval_request.status.value,
                "internal_owner_contact": InternalOwnerContactOut.model_validate(db_owner_contact)
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_OWNER_DETAILS_CHANGE_FAILED", entity_type="InternalOwnerContact", entity_id=owner_id, details={"owner_email": db_owner_contact.email, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=None)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_OWNER_DETAILS_CHANGE_FAILED", entity_type="InternalOwnerContact", entity_id=owner_id, details={"owner_email": db_owner_contact.email, "reason": f"An unexpected error occurred during approval request creation: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=None)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during approval request creation: {e}"
            )
    else:
        # Direct Execution flow
        try:
            updated_owner = await crud_lg_owner.update_internal_owner_details(
                db,
                old_internal_owner_contact_id=owner_id,
                obj_in=owner_details_in,
                user_id=end_user_context.user_id,
                customer_id=end_user_context.customer_id,
            )
            return {
                "message": f"Internal Owner Contact '{updated_owner.email}' details updated directly.",
                "internal_owner_contact": InternalOwnerContactOut.model_validate(updated_owner),
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_OWNER_DETAILS_CHANGE_FAILED", entity_type="InternalOwnerContact", entity_id=owner_id, details={"owner_email": db_owner_contact.email, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=None)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_OWNER_DETAILS_CHANGE_FAILED", entity_type="InternalOwnerContact", entity_id=owner_id, details={"owner_email": db_owner_contact.email, "reason": f"An unexpected error occurred during direct update: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=None)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during direct update: {e}"
            )

# --------------------------------------------------------------------------------------
# 7. /lg-records/change-owner (Single or Bulk)
# --------------------------------------------------------------------------------------
@router.post(
    "/lg-records/change-owner",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_record:amend")), Depends(check_for_read_only_mode)],
    summary="Change internal owner for LG records (single or bulk, via Maker-Checker workflow)"
)
async def change_lg_owner_api(
    change_in: LGRecordChangeOwner,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Changes the internal owner for a single LG record or all LG records associated with an old owner.
    This action goes through the Maker-Checker workflow if enabled.
    """
    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer or Subscription Plan not found.")

    action_type = ""
    entity_id = None

    if change_in.change_scope == InternalOwnerChangeScope.SINGLE_LG:
        action_type = ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER
        entity_id = change_in.lg_record_id
        existing_pending_requests = crud_approval_request.get_pending_requests_for_lg(db, change_in.lg_record_id, end_user_context.customer_id)
        if existing_pending_requests:
            first_pending_req = existing_pending_requests[0]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Another action ({first_pending_req.action_type}) is already pending approval for LG Record ID {change_in.lg_record_id} (Request ID: {first_pending_req.id})."
            )
    elif change_in.change_scope == InternalOwnerChangeScope.ALL_BY_OLD_OWNER:
        action_type = ACTION_TYPE_LG_CHANGE_BULK_LG_OWNER
        entity_id = change_in.old_internal_owner_contact_id # Use the owner ID as the entity ID for the AR
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid change scope.")

    if customer.subscription_plan.can_maker_checker:
        try:
            lg_snapshot = None
            db_lg_record = None
            if change_in.change_scope == InternalOwnerChangeScope.SINGLE_LG and change_in.lg_record_id:
                db_lg_record = crud_lg_record.get_lg_record_with_relations(db, change_in.lg_record_id, end_user_context.customer_id)
                if not db_lg_record:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found for snapshot.")
                lg_snapshot = crud_approval_request._get_lg_record_snapshot(db_lg_record)

            approval_request_in = ApprovalRequestCreate(
                entity_type="LGRecord",
                entity_id=entity_id,
                action_type=action_type,
                request_details=change_in.model_dump(),
                lg_record_snapshot=lg_snapshot,
            )
            
            # **AWAIT** the asynchronous CRUD method call
            db_approval_request = await crud_approval_request.create_approval_request(
                db,
                approval_request_in,
                end_user_context.user_id,
                end_user_context.customer_id,
                lg_record=db_lg_record,
            )

            log_action(
                db,
                user_id=end_user_context.user_id,
                action_type="APPROVAL_REQUEST_SUBMITTED",
                entity_type="ApprovalRequest",
                entity_id=db_approval_request.id,
                details={
                    "scope": change_in.change_scope.value,
                    "lg_record_id": change_in.lg_record_id,
                    "old_owner_id": change_in.old_internal_owner_contact_id,
                    "new_owner_id": change_in.new_internal_owner_contact_id,
                    "action_type": action_type,
                    "status": db_approval_request.status.value,
                    "maker_email": end_user_context.email,
                    "reason_for_request": change_in.reason,
                    "new_owner_details_if_created": change_in.new_internal_owner_contact_details.model_dump() if change_in.new_internal_owner_contact_details else None,
                },
                customer_id=end_user_context.customer_id,
                lg_record_id=change_in.lg_record_id if change_in.change_scope == InternalOwnerChangeScope.SINGLE_LG else None,
            )
            return {
                "message": f"LG Owner Change ({change_in.change_scope.value}) request submitted for approval.",
                "approval_request_id": db_approval_request.id,
                "status": db_approval_request.status.value,
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_OWNER_CHANGE_FAILED", entity_type="LGRecord", entity_id=change_in.lg_record_id, details={"scope": change_in.change_scope.value, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=change_in.lg_record_id if change_in.change_scope == InternalOwnerChangeScope.SINGLE_LG else None)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_OWNER_CHANGE_FAILED", entity_type="LGRecord", entity_id=change_in.lg_record_id, details={"scope": change_in.change_scope.value, "reason": f"An unexpected error occurred during approval request creation: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=change_in.lg_record_id if change_in.change_scope == InternalOwnerChangeScope.SINGLE_LG else None)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during approval request creation: {e}"
            )
    else:
        # Direct Execution flow
        try:
            affected_lgs = await crud_lg_owner.change_lg_internal_owner_single_or_bulk(
                db,
                change_in=change_in,
                user_id=end_user_context.user_id,
                customer_id=end_user_context.customer_id,
                approval_request_id=None,
            )
            message_suffix = f"for LG '{affected_lgs[0].lg_number}'" if affected_lgs and change_in.change_scope == InternalOwnerChangeScope.SINGLE_LG else f"for {len(affected_lgs)} LGs"
            return {
                "message": f"LG Internal Owner changed directly {message_suffix}.",
                "affected_lgs_count": len(affected_lgs),
                "affected_lg_numbers": [lg.lg_number for lg in affected_lgs] if affected_lgs else [],
            }
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_OWNER_CHANGE_FAILED", entity_type="LGRecord", entity_id=change_in.lg_record_id, details={"scope": change_in.change_scope.value, "reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=change_in.lg_record_id if change_in.change_scope == InternalOwnerChangeScope.SINGLE_LG else None)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_OWNER_CHANGE_FAILED", entity_type="LGRecord", entity_id=change_in.lg_record_id, details={"scope": change_in.change_scope.value, "reason": f"An unexpected error occurred during direct change: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=change_in.lg_record_id if change_in.change_scope == InternalOwnerChangeScope.SINGLE_LG else None)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred during direct change: {e}"
            )
            
def _collect_alternate_bank_names(lg_record: Optional[models.LGRecord]) -> List[str]:
    """Return advising/communication bank and foreign bank names for AI counterparty matching."""
    if not lg_record:
        return []
    names: List[str] = []
    comm_bank = getattr(lg_record, "communication_bank", None)
    if comm_bank:
        if getattr(comm_bank, "name", None):
            names.append(comm_bank.name)
        if getattr(comm_bank, "short_name", None):
            names.append(comm_bank.short_name)
    if getattr(lg_record, "foreign_bank_name", None):
        names.append(lg_record.foreign_bank_name)
    return [n for n in names if n]


def _collect_lg_document_hashes(db: Session, lg_record: Optional[models.LGRecord]) -> List[Dict[str, Any]]:
    """Collect existing SHA-256 document hashes across all instructions and uploaded documents of an LG."""
    if not lg_record:
        return []
    hashes: List[Dict[str, Any]] = []
    seen = set()

    inst_map = {getattr(i, "id", None): i for i in (getattr(lg_record, "instructions", None) or [])}
    for inst in (getattr(lg_record, "instructions", None) or []):
        if getattr(inst, "is_deleted", False):
            continue
        inst_details = inst.details or {}
        inst_type_str = getattr(inst, "instruction_type", "") or ""
        deliv_ai = inst_details.get("delivery_ai_verification")
        if isinstance(deliv_ai, dict) and deliv_ai.get("file_sha256"):
            h = deliv_ai["file_sha256"]
            key = (h, inst.id, "DELIVERY_PROOF")
            if key not in seen:
                seen.add(key)
                hashes.append({
                    "file_sha256": h,
                    "document_type": "DELIVERY_PROOF",
                    "document_type_label": "Delivery Proof",
                    "instruction_id": inst.id,
                    "instruction_serial": inst.serial_number,
                    "instruction_type": inst_type_str,
                    "instruction_seq": inst.global_seq_per_lg or inst.id,
                })
        reply_ai = inst_details.get("bank_reply_ai_verification")
        if isinstance(reply_ai, dict) and reply_ai.get("file_sha256"):
            h = reply_ai["file_sha256"]
            key = (h, inst.id, "BANK_REPLY")
            if key not in seen:
                seen.add(key)
                hashes.append({
                    "file_sha256": h,
                    "document_type": "BANK_REPLY",
                    "document_type_label": "Bank Reply",
                    "instruction_id": inst.id,
                    "instruction_serial": inst.serial_number,
                    "instruction_type": inst_type_str,
                    "instruction_seq": inst.global_seq_per_lg or inst.id,
                })

    try:
        upload_logs = db.query(models.AuditLog).filter(
            models.AuditLog.lg_record_id == lg_record.id,
            models.AuditLog.action_type == "DOCUMENT_UPLOADED",
        ).all()
        for log_row in upload_logs:
            log_details = log_row.details or {}
            h = log_details.get("file_sha256")
            if h:
                doc_type = log_details.get("document_type") or "LGDocument"
                inst_id = log_details.get("lg_instruction_id")
                matched_inst = inst_map.get(inst_id) if inst_id else None
                key = (h, inst_id, doc_type)
                if key not in seen:
                    seen.add(key)
                    hashes.append({
                        "file_sha256": h,
                        "document_type": doc_type,
                        "document_type_label": doc_type.replace("_", " ").title(),
                        "instruction_id": inst_id,
                        "instruction_serial": getattr(matched_inst, "serial_number", None),
                        "instruction_type": getattr(matched_inst, "instruction_type", None),
                        "instruction_seq": getattr(matched_inst, "global_seq_per_lg", None) or inst_id,
                    })
    except Exception:
        pass

    return hashes


def _notify_corporate_admins_of_custody_event(
    db: Session,
    customer_id: int,
    lg_record: Optional[models.LGRecord],
    content: str,
    notification_type: str = "LG_CUSTODY_ADMIN_ALERT",
    actor_user_id: Optional[int] = None,
    emit_mobile_defect: bool = False,
) -> None:
    """
    Sends an in-app SystemNotification to Corporate Admins of the customer (visible in the
    Corporate Admin Dashboard) and optionally emits mobile push/smart alert.
    """
    try:
        from app.schemas.all_schemas import SystemNotificationCreate
        from app.crud.crud import crud_system_notification

        now_utc = datetime.utcnow()
        link_path = f"/corporate-admin/lg-records/{lg_record.id}" if lg_record else "/corporate-admin/lg-records"
        notif_in = SystemNotificationCreate(
            content=content,
            notification_type=notification_type,
            target_roles=["CORPORATE_ADMIN"],
            target_customer_ids=[customer_id],
            start_date=now_utc - timedelta(minutes=1),
            end_date=now_utc + timedelta(days=14),
            is_active=True,
            link=link_path,
        )
        crud_system_notification.create(db, obj_in=notif_in, user_id=actor_user_id or 1)
    except Exception as sys_notif_err:
        logger.warning(f"Failed to create SystemNotification for Corporate Admin: {sys_notif_err}")

    if emit_mobile_defect and lg_record:
        try:
            from app.services.notifications.lg_custody_service import notify_custody_bank_defect
            notify_custody_bank_defect(db, lg_record, actor_user_id=actor_user_id)
        except Exception as mob_err:
            logger.warning(f"Failed to emit LG_CUSTODY_BANK_DEFECT notification: {mob_err}")


@router.post(
    "/lg-records/instructions/{instruction_id}/record-delivery",
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_for_read_only_mode)],
    summary="Record the delivery of an LG instruction to the bank with AI verification"
)
async def record_instruction_delivery_api(
    instruction_id: int,
    delivery_date: date = Form(..., description="The date the instruction was physically delivered to the bank."),
    delivery_document_metadata: Optional[str] = Form(None, description="JSON string of optional document metadata."),
    delivery_document_file: Optional[UploadFile] = File(None, description="Optional document proving delivery."),
    force_override: bool = Form(False, description="Whether to proceed anyway despite AI verification mismatches."),
    override_reason: Optional[str] = Form(None, description="Reason for proceeding anyway if overridden."),
    ai_verification_json: Optional[str] = Form(None, description="Cached AI verification result from previous check."),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Records the delivery details for a specific LG instruction.
    Runs AI verification on the uploaded delivery evidence (detecting bank receiving stamp, chronology, bank match, stamp date alignment, and duplicate scan SHA-256).
    """
    client_host = get_client_ip(request) if request else None

    # --- ENFORCEMENT LOGIC: Check if supporting document is mandatory ---
    is_doc_mandatory_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.DOC_MANDATORY_RECORD_DELIVERY
    )
    is_doc_mandatory = is_doc_mandatory_config.get('effective_value', 'False').lower() == 'true'

    if is_doc_mandatory and delivery_document_file is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Supporting delivery document is mandatory for Recording Delivery as per Corporate Admin settings."
        )

    db_instruction = db.query(models.LGInstruction).options(
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.issuing_bank),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.communication_bank),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.instructions),
    ).filter(
        models.LGInstruction.id == instruction_id,
        models.LGInstruction.is_deleted == False
    ).first()

    if not db_instruction or not db_instruction.lg_record or db_instruction.lg_record.customer_id != end_user_context.customer_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Instruction not found or not accessible.")

    file_bytes = None
    file_sha256 = None
    delivery_document_data = None
    ai_verification = None

    if ai_verification_json:
        try:
            parsed_ai_ver = json.loads(ai_verification_json)
            if force_override or (isinstance(parsed_ai_ver, dict) and parsed_ai_ver.get("status") == "verified"):
                ai_verification = parsed_ai_ver
        except Exception:
            pass

    try:
        if delivery_document_file:
            file_bytes = await delivery_document_file.read()
            if file_bytes:
                file_sha256 = hashlib.sha256(file_bytes).hexdigest()

            if delivery_document_metadata:
                try:
                    parsed_metadata = json.loads(delivery_document_metadata)
                    delivery_document_data = LGDocumentCreate(
                        document_type=parsed_metadata.get("document_type", "DELIVERY_PROOF"),
                        file_name=delivery_document_file.filename,
                        file_path="",
                        mime_type=delivery_document_file.content_type,
                        lg_instruction_id=instruction_id
                    )
                except json.JSONDecodeError:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON for delivery document metadata.")
            else:
                delivery_document_data = LGDocumentCreate(
                    document_type="DELIVERY_PROOF",
                    file_name=delivery_document_file.filename,
                    file_path="",
                    mime_type=delivery_document_file.content_type,
                    lg_instruction_id=instruction_id
                )

            # Run AI Verification on Delivery Proof (unless already cached or force_override)
            if not ai_verification and not force_override:
                try:
                    raw_text = document_verification_service.extract_text(
                        file_bytes, delivery_document_file.content_type, delivery_document_file.filename
                    )
                    issuing_bank = db_instruction.lg_record.issuing_bank if db_instruction.lg_record else None
                    expected_bank = issuing_bank.name if issuing_bank else ""
                    expected_short = issuing_bank.short_name if issuing_bank else None
                    inst_date = db_instruction.instruction_date.date() if db_instruction.instruction_date else date.today()
                    existing_hashes = _collect_lg_document_hashes(db, db_instruction.lg_record)
                    alt_bank_names = _collect_alternate_bank_names(db_instruction.lg_record)

                    ai_verification = document_verification_service.verify_delivery_evidence(
                        raw_text=raw_text,
                        expected_bank_name=expected_bank,
                        expected_bank_short_name=expected_short,
                        instruction_issue_date=inst_date,
                        instruction_serial=db_instruction.serial_number,
                        lg_number=db_instruction.lg_record.lg_number if db_instruction.lg_record else "",
                        user_delivery_date=delivery_date,
                        file_sha256=file_sha256,
                        existing_doc_hashes=existing_hashes,
                        alternate_bank_names=alt_bank_names,
                    )

                    if ai_verification.get("status") == "mismatch":
                        logger.info(f"AI Delivery Verification flagged mismatch for instruction {instruction_id}: {ai_verification['mismatches']}")
                        return JSONResponse(
                            status_code=status.HTTP_200_OK,
                            content={
                                "status": "mismatch",
                                "ai_verification": ai_verification,
                                "delivery_date_variance": ai_verification.get("delivery_date_variance"),
                                "detected_stamp_date": ai_verification.get("detected_stamp_date"),
                                "duplicate_scan_detected": ai_verification.get("duplicate_scan_detected"),
                                "instruction_id": instruction_id,
                                "message": "AI verification detected discrepancies in the delivery document. Please review."
                            }
                        )
                except Exception as ai_err:
                    logger.warning(f"Delivery AI verification exception (bypassing): {ai_err}")

        try:
            delivery_in = LGInstructionRecordDelivery(
                delivery_date=delivery_date,
                delivery_document_file=delivery_document_data
            )
        except ValueError as ve:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid delivery date ({delivery_date}): Delivery date cannot be in the future."
            )

        try:
            updated_instruction = await crud_lg_instruction.record_instruction_delivery(
                db,
                instruction_id=instruction_id,
                obj_in=delivery_in,
                user_id=end_user_context.user_id,
                customer_id=end_user_context.customer_id,
                file_content=file_bytes
            )

            details = dict(updated_instruction.details or {})
            if ai_verification:
                if file_sha256 and not ai_verification.get("file_sha256"):
                    ai_verification["file_sha256"] = file_sha256
                details["delivery_ai_verification"] = ai_verification
                if ai_verification.get("detected_stamp_date"):
                    details["detected_stamp_date"] = ai_verification["detected_stamp_date"]
            elif file_sha256:
                details["delivery_ai_verification"] = {"file_sha256": file_sha256}
            if force_override:
                details["delivery_override"] = {
                    "status": "OVERRIDDEN_BY_USER",
                    "reason": override_reason or "User proceeded despite AI mismatch",
                    "user_id": end_user_context.user_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
                log_action(
                    db,
                    user_id=end_user_context.user_id,
                    action_type="LG_RECORD_DELIVERY_OVERRIDDEN",
                    entity_type="LGInstruction",
                    entity_id=instruction_id,
                    details={
                        "lg_number": db_instruction.lg_record.lg_number,
                        "serial_number": db_instruction.serial_number,
                        "override_reason": override_reason or "User proceeded despite AI mismatch",
                        "delivery_date": str(delivery_date)
                    },
                    customer_id=end_user_context.customer_id,
                    lg_record_id=db_instruction.lg_record.id,
                )
                _notify_corporate_admins_of_custody_event(
                    db=db,
                    customer_id=end_user_context.customer_id,
                    lg_record=db_instruction.lg_record,
                    content=(
                        f"AI Verification Override on Delivery: End User recorded delivery for LG "
                        f"{db_instruction.lg_record.lg_number} (Instruction {db_instruction.serial_number}) "
                        f"with override note: '{override_reason or 'User proceeded despite AI mismatch'}'."
                    ),
                    notification_type="LG_DELIVERY_AI_OVERRIDDEN",
                    actor_user_id=end_user_context.user_id,
                    emit_mobile_defect=True,
                )
            updated_instruction.details = details
            db.add(updated_instruction)
            db.commit()
            db.refresh(updated_instruction)

            return LGInstructionOut.model_validate(updated_instruction)
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_RECORD_DELIVERY_FAILED", entity_type="LGInstruction", entity_id=instruction_id, details={"reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=db_instruction.lg_record_id if db_instruction else None)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred in record_instruction_delivery_api: {e}", exc_info=True)
            log_action(db, user_id=end_user_context.user_id, action_type="LG_RECORD_DELIVERY_FAILED", entity_type="LGInstruction", entity_id=instruction_id, details={"reason": f"An unexpected error occurred: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=db_instruction.lg_record_id if db_instruction else None)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred: {e}"
            )
    finally:
        del file_bytes
        import gc
        gc.collect()


@router.post(
    "/lg-records/instructions/{instruction_id}/confirm-delivery",
    response_model=LGInstructionOut,
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_for_read_only_mode)],
    summary="Confirm delivery of an LG instruction following AI verification review (Proceed Anyway)"
)
async def confirm_instruction_delivery_api(
    instruction_id: int,
    override_reason: Optional[str] = Form(None, description="Reason for proceeding despite AI discrepancies."),
    delivery_date: Optional[date] = Form(None),
    delivery_document_metadata: Optional[str] = Form(None),
    delivery_document_file: Optional[UploadFile] = File(None),
    ai_verification_json: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """Maker/Checker confirms delivery despite AI verification mismatches (Proceed Anyway) - saves attached file."""
    return await record_instruction_delivery_api(
        instruction_id=instruction_id,
        delivery_date=delivery_date or date.today(),
        delivery_document_metadata=delivery_document_metadata,
        delivery_document_file=delivery_document_file,
        force_override=True,
        override_reason=override_reason or "User confirmed delivery despite AI discrepancies",
        ai_verification_json=ai_verification_json,
        db=db,
        end_user_context=end_user_context,
        request=request
    )


@router.post(
    "/lg-records/instructions/{instruction_id}/cancel-delivery",
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_for_read_only_mode)],
    summary="Cancel pending delivery recording to re-upload document"
)
def cancel_instruction_delivery_api(
    instruction_id: int,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    return {"status": "cancelled", "instruction_id": instruction_id, "message": "Delivery recording cancelled. You can re-upload."}

# IMPORTANT: Apply similar changes to record_bank_reply_api as well
@router.post(
    "/lg-records/instructions/{instruction_id}/record-bank-reply",
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_for_read_only_mode)],
    summary="Record the bank's reply to an LG instruction with AI verification, calendar tolerance alignment, multi-instruction disambiguation, and Maker-Checker admin terms approval"
)
async def record_bank_reply_api(
    instruction_id: int,
    bank_reply_date: date = Form(..., description="The date the bank's reply was received."),
    reply_details: Optional[str] = Form(None, description="Details or notes from the bank's reply."),
    bank_reply_document_metadata: Optional[str] = Form(None, description="JSON string of optional document metadata."),
    bank_reply_document_file: Optional[UploadFile] = File(None, description="Optional document proving the bank's reply."),
    force_override: bool = Form(False, description="Whether to proceed anyway despite AI verification mismatches."),
    override_reason: Optional[str] = Form(None, description="Reason for proceeding anyway if overridden."),
    ai_verification_json: Optional[str] = Form(None, description="Cached AI verification result from previous check."),
    accept_date_alignment: bool = Form(False, description="Whether the user accepted aligning LG expiry date to the bank's confirmed date (DATE_CONVENTION_VARIANCE)."),
    aligned_expiry_date: Optional[date] = Form(None, description="The bank-confirmed expiry date accepted by the user."),
    switched_from_instruction_id: Optional[int] = Form(None, description="Original instruction ID if redirected via 1-click multi-instruction disambiguation."),
    request_admin_terms_approval: bool = Form(False, description="Whether the user is submitting bank-modified terms (>3 days date variance or amount difference) for Corporate Admin Maker-Checker approval."),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    client_host = get_client_ip(request) if request else None

    # --- ENFORCEMENT LOGIC: Check if supporting document is mandatory ---
    is_doc_mandatory_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.DOC_MANDATORY_RECORD_BANK_REPLY
    )
    is_doc_mandatory = is_doc_mandatory_config.get('effective_value', 'False').lower() == 'true'

    if is_doc_mandatory and bank_reply_document_file is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Supporting bank reply document is mandatory for Recording Bank Reply as per Corporate Admin settings."
        )

    db_instruction = db.query(models.LGInstruction).options(
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.issuing_bank),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.communication_bank),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.instructions),
    ).filter(
        models.LGInstruction.id == instruction_id,
        models.LGInstruction.is_deleted == False
    ).first()

    if not db_instruction or not db_instruction.lg_record or db_instruction.lg_record.customer_id != end_user_context.customer_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Instruction not found or not accessible.")

    file_bytes = None
    file_sha256 = None
    bank_reply_document_data = None
    ai_verification = None

    if ai_verification_json:
        try:
            parsed_ai = json.loads(ai_verification_json)
            if (
                force_override
                or accept_date_alignment
                or request_admin_terms_approval
                or switched_from_instruction_id is not None
                or parsed_ai.get("status") == "verified"
            ):
                ai_verification = parsed_ai
        except Exception:
            pass

    try:
        if bank_reply_document_file:
            file_bytes = await bank_reply_document_file.read()
            if file_bytes:
                file_sha256 = hashlib.sha256(file_bytes).hexdigest()

            if bank_reply_document_metadata:
                try:
                    parsed_metadata = json.loads(bank_reply_document_metadata)
                    bank_reply_document_data = LGDocumentCreate(
                        document_type=parsed_metadata.get("document_type", "BANK_REPLY"),
                        file_name=bank_reply_document_file.filename,
                        file_path="",
                        mime_type=bank_reply_document_file.content_type,
                        lg_instruction_id=instruction_id
                    )
                except json.JSONDecodeError:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON for bank reply document metadata.")
            else:
                bank_reply_document_data = LGDocumentCreate(
                    document_type="BANK_REPLY",
                    file_name=bank_reply_document_file.filename,
                    file_path="",
                    mime_type=bank_reply_document_file.content_type,
                    lg_instruction_id=instruction_id
                )

            # Run AI Verification on Bank Reply (unless already cached via override, date alignment, or admin terms approval submission)
            if not ai_verification and not force_override and not accept_date_alignment and not request_admin_terms_approval:
                try:
                    raw_text = document_verification_service.extract_text(
                        file_bytes, bank_reply_document_file.content_type, bank_reply_document_file.filename
                    )
                    issuing_bank = db_instruction.lg_record.issuing_bank if db_instruction.lg_record else None
                    expected_bank = issuing_bank.name if issuing_bank else ""
                    expected_short = issuing_bank.short_name if issuing_bank else None

                    inst_details = db_instruction.details or {}
                    exp_new_expiry = (
                        inst_details.get("new_expiry_date")
                        or inst_details.get("requested_expiry_date")
                        or (
                            db_instruction.lg_record.expiry_date.date().isoformat()
                            if db_instruction.lg_record and db_instruction.lg_record.expiry_date
                            else None
                        )
                    )
                    exp_new_amount = (
                        inst_details.get("new_amount")
                        or inst_details.get("new_lg_amount")
                        or inst_details.get("requested_amount")
                    )
                    if exp_new_amount is not None:
                        try:
                            exp_new_amount = float(exp_new_amount)
                        except Exception:
                            exp_new_amount = None

                    # Gather all pending/open instructions on this LG for Multi-Instruction Disambiguation (Item 2)
                    def _serialize_inst_for_ai(inst_obj: models.LGInstruction) -> Dict[str, Any]:
                        return {
                            "id": inst_obj.id,
                            "serial_number": inst_obj.serial_number,
                            "instruction_type": inst_obj.instruction_type,
                            "global_seq_per_lg": inst_obj.global_seq_per_lg,
                            "type_seq_per_lg": inst_obj.type_seq_per_lg,
                            "status": inst_obj.status,
                            "instruction_date": inst_obj.instruction_date.date().isoformat() if inst_obj.instruction_date else None,
                            "delivery_date": inst_obj.delivery_date.date().isoformat() if inst_obj.delivery_date else None,
                            "details": inst_obj.details or {},
                        }

                    open_instructions = [
                        i for i in (db_instruction.lg_record.instructions or [])
                        if not i.is_deleted
                        and i.status not in ("Canceled", "Confirmed by Bank")
                        and i.bank_reply_date is None
                    ]
                    open_instructions.sort(key=lambda x: (x.global_seq_per_lg or 0, x.id or 0))
                    pending_inst_dicts = [_serialize_inst_for_ai(i) for i in open_instructions]
                    target_inst_dict = _serialize_inst_for_ai(db_instruction)
                    existing_hashes = _collect_lg_document_hashes(db, db_instruction.lg_record)
                    alt_bank_names = _collect_alternate_bank_names(db_instruction.lg_record)

                    ai_verification = document_verification_service.verify_bank_reply(
                        raw_text=raw_text,
                        expected_bank_name=expected_bank,
                        expected_bank_short_name=expected_short,
                        action_type=db_instruction.instruction_type,
                        expected_new_expiry=exp_new_expiry,
                        expected_new_amount=exp_new_amount,
                        lg_number=db_instruction.lg_record.lg_number if db_instruction.lg_record else "",
                        target_instruction=target_inst_dict,
                        pending_instructions=pending_inst_dicts,
                        user_reply_date=bank_reply_date,
                        file_sha256=file_sha256,
                        existing_doc_hashes=existing_hashes,
                        alternate_bank_names=alt_bank_names,
                    )

                    if ai_verification.get("status") == "date_variance":
                        variance_info = ai_verification.get("date_convention_variance") or {}
                        logger.info(
                            f"AI Bank Reply Verification detected DATE_CONVENTION_VARIANCE for instruction {instruction_id}: "
                            f"{variance_info.get('prompt_message')}"
                        )
                        return JSONResponse(
                            status_code=status.HTTP_200_OK,
                            content={
                                "status": "date_variance",
                                "ai_verification": ai_verification,
                                "date_convention_variance": variance_info,
                                "bank_reply_letter_date": ai_verification.get("bank_reply_letter_date"),
                                "instruction_id": instruction_id,
                                "message": variance_info.get("prompt_message") or "Bank confirmed expiry date with minor calendar convention variance.",
                            }
                        )

                    if ai_verification.get("status") == "mismatch":
                        disambig_info = ai_verification.get("instruction_disambiguation")
                        mod_info = ai_verification.get("bank_terms_modification")
                        logger.info(f"AI Bank Reply Verification flagged mismatch for instruction {instruction_id}: {ai_verification['mismatches']}")
                        return JSONResponse(
                            status_code=status.HTTP_200_OK,
                            content={
                                "status": "mismatch",
                                "ai_verification": ai_verification,
                                "date_convention_variance": ai_verification.get("date_convention_variance"),
                                "instruction_disambiguation": disambig_info,
                                "bank_terms_modification": mod_info,
                                "bank_reply_letter_date": ai_verification.get("bank_reply_letter_date"),
                                "duplicate_scan_detected": ai_verification.get("duplicate_scan_detected"),
                                "instruction_id": instruction_id,
                                "message": (
                                    disambig_info["prompt_message"]
                                    if disambig_info
                                    else "AI verification detected discrepancies in the bank reply document. Please review."
                                )
                            }
                        )
                except Exception as ai_err:
                    logger.warning(f"Bank reply AI verification exception (bypassing): {ai_err}")

        if (
            ai_verification
            and ai_verification.get("bank_terms_modification")
            and not ai_verification.get("instruction_disambiguation")
            and force_override
            and not request_admin_terms_approval
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Bank-modified core terms require Corporate Admin approval and cannot be overridden directly."
            )

        try:
            bank_reply_in = LGInstructionRecordBankReply(
                bank_reply_date=bank_reply_date,
                reply_details=reply_details,
                bank_reply_document_file=bank_reply_document_data
            )
        except ValueError as ve:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid bank reply date ({bank_reply_date}): Bank reply date cannot be in the future."
            )

        try:
            updated_instruction = await crud_lg_instruction.record_bank_reply(
                db,
                instruction_id=instruction_id,
                obj_in=bank_reply_in,
                user_id=end_user_context.user_id,
                customer_id=end_user_context.customer_id,
                file_content=file_bytes
            )

            details = dict(updated_instruction.details or {})
            if ai_verification:
                if file_sha256 and not ai_verification.get("file_sha256"):
                    ai_verification["file_sha256"] = file_sha256
                details["bank_reply_ai_verification"] = ai_verification
                letter_dt = (
                    ai_verification.get("bank_reply_letter_date")
                    or (ai_verification.get("extracted_data") or {}).get("cited_effective_date")
                )
                if letter_dt:
                    details["bank_reply_letter_date"] = letter_dt
            elif file_sha256:
                details["bank_reply_ai_verification"] = {"file_sha256": file_sha256}

            # Item 1: User Accepted Bank Expiry Date Tolerance Alignment
            if accept_date_alignment:
                variance_obj = (ai_verification or {}).get("date_convention_variance") or {}
                confirmed_date_val = aligned_expiry_date
                if not confirmed_date_val:
                    raw_confirmed = (
                        variance_obj.get("bank_confirmed_expiry_date")
                        or (ai_verification or {}).get("extracted_data", {}).get("amended_expiry_date")
                    )
                    if raw_confirmed:
                        try:
                            confirmed_date_val = datetime.strptime(str(raw_confirmed)[:10], "%Y-%m-%d").date()
                        except Exception:
                            confirmed_date_val = None

                if confirmed_date_val and updated_instruction.lg_record:
                    lg_rec = updated_instruction.lg_record
                    requested_expiry_str = (
                        variance_obj.get("requested_expiry_date")
                        or details.get("new_expiry_date")
                        or (lg_rec.expiry_date.date().isoformat() if lg_rec.expiry_date else None)
                    )
                    lg_rec.expiry_date = datetime.combine(confirmed_date_val, datetime.min.time())
                    db.add(lg_rec)

                    var_days = variance_obj.get("variance_days")
                    is_favorable_further = bool(var_days is not None and int(var_days) > 3)
                    classification_str = "FAVORABLE_EXTENDED_EXPIRY" if is_favorable_further else "DATE_CONVENTION_VARIANCE"

                    details["date_convention_alignment"] = {
                        "classification": classification_str,
                        "status": "ACCEPTED_BY_USER",
                        "requested_expiry_date": requested_expiry_str,
                        "previous_expiry_date": requested_expiry_str,
                        "bank_confirmed_expiry_date": confirmed_date_val.isoformat(),
                        "aligned_expiry_date": confirmed_date_val.isoformat(),
                        "variance_days": var_days,
                        "user_id": end_user_context.user_id,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    details["new_expiry_date"] = confirmed_date_val.isoformat()
                    if isinstance(details.get("bank_reply_ai_verification"), dict):
                        details["bank_reply_ai_verification"]["status"] = "verified"

                    log_action(
                        db,
                        user_id=end_user_context.user_id,
                        action_type="LG_EXPIRY_ALIGNED_TO_BANK_CONVENTION",
                        entity_type="LGRecord",
                        entity_id=lg_rec.id,
                        details={
                            "lg_number": lg_rec.lg_number,
                            "instruction_id": instruction_id,
                            "instruction_serial": updated_instruction.serial_number,
                            "requested_expiry_date": requested_expiry_str,
                            "bank_confirmed_expiry_date": confirmed_date_val.isoformat(),
                            "variance_days": var_days,
                            "classification": classification_str,
                            "user_accepted_alignment": True,
                        },
                        customer_id=end_user_context.customer_id,
                        lg_record_id=lg_rec.id,
                    )
                    if is_favorable_further:
                        _notify_corporate_admins_of_custody_event(
                            db=db,
                            customer_id=end_user_context.customer_id,
                            lg_record=lg_rec,
                            content=(
                                f"Favorable Extended Expiry Accepted: End User aligned LG {lg_rec.lg_number} "
                                f"(Instruction {updated_instruction.serial_number}) to bank's further expiry date "
                                f"{confirmed_date_val.isoformat()} (+{var_days} days extra coverage beyond {requested_expiry_str})."
                            ),
                            notification_type="LG_FAVORABLE_EXPIRY_ALIGNED",
                            actor_user_id=end_user_context.user_id,
                            emit_mobile_defect=False,
                        )

            # Enhancement A: Submit Bank-Modified Terms (>3 days date variance or amount difference) for Corporate Admin Maker-Checker Approval
            if request_admin_terms_approval and updated_instruction.lg_record:
                mod_obj = (ai_verification or {}).get("bank_terms_modification") or {}
                amendment_details = dict(mod_obj.get("amendment_details") or {})
                if not amendment_details:
                    extracted = (ai_verification or {}).get("extracted_data") or {}
                    if extracted.get("amended_expiry_date"):
                        amendment_details["expiry_date"] = str(extracted["amended_expiry_date"])[:10]
                    if extracted.get("amended_amount") is not None:
                        try:
                            amendment_details["lg_amount"] = float(extracted["amended_amount"])
                        except Exception:
                            pass

                if amendment_details:
                    reply_doc = db.query(models.LGDocument).filter(
                        models.LGDocument.lg_instruction_id == instruction_id,
                        models.LGDocument.document_type == "BANK_REPLY",
                        models.LGDocument.is_deleted == False,
                    ).order_by(models.LGDocument.id.desc()).first()

                    lg_rec_full = (
                        crud_lg_record.get_lg_record_with_relations(
                            db, db_instruction.lg_record_id, end_user_context.customer_id
                        )
                        or updated_instruction.lg_record
                    )
                    lg_snapshot = crud_approval_request._get_lg_record_snapshot(lg_rec_full)
                    approval_reason = (
                        override_reason
                        or mod_obj.get("prompt_message")
                        or f"Sync LG terms to bank's actual reply on Instruction {updated_instruction.serial_number}"
                    )
                    approval_request_in = ApprovalRequestCreate(
                        entity_type="LGRecord",
                        entity_id=db_instruction.lg_record_id,
                        action_type=ACTION_TYPE_LG_AMEND,
                        request_details={
                            "amendment_details": amendment_details,
                            "reason": approval_reason,
                            "amendment_document_id": reply_doc.id if reply_doc else None,
                            "source_instruction_id": instruction_id,
                            "source_instruction_serial": updated_instruction.serial_number,
                            "classification": "BANK_MODIFIED_TERMS_APPROVAL_REQUIRED",
                        },
                        lg_record_snapshot=lg_snapshot,
                    )
                    db_approval_request = await crud_approval_request.create_approval_request(
                        db,
                        approval_request_in,
                        end_user_context.user_id,
                        end_user_context.customer_id,
                        lg_record=lg_rec_full,
                    )
                    details["bank_terms_admin_approval"] = {
                        "status": "PENDING_ADMIN_APPROVAL",
                        "approval_request_id": db_approval_request.id,
                        "amendment_details": amendment_details,
                        "reason": approval_reason,
                        "requested_by_user_id": end_user_context.user_id,
                        "timestamp": datetime.utcnow().isoformat(),
                    }

            # Item 2: Audit Trail for 1-Click Multi-Instruction Disambiguation Redirection
            if switched_from_instruction_id and switched_from_instruction_id != instruction_id:
                details["disambiguation_redirect"] = {
                    "switched_from_instruction_id": switched_from_instruction_id,
                    "matched_instruction_id": instruction_id,
                    "user_id": end_user_context.user_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
                log_action(
                    db,
                    user_id=end_user_context.user_id,
                    action_type="LG_BANK_REPLY_REDIRECTED_BY_AI",
                    entity_type="LGInstruction",
                    entity_id=instruction_id,
                    details={
                        "lg_number": updated_instruction.lg_record.lg_number if updated_instruction.lg_record else None,
                        "switched_from_instruction_id": switched_from_instruction_id,
                        "target_instruction_id": instruction_id,
                        "instruction_serial": updated_instruction.serial_number,
                    },
                    customer_id=end_user_context.customer_id,
                    lg_record_id=updated_instruction.lg_record.id if updated_instruction.lg_record else None,
                )

            if force_override:
                details["bank_reply_override"] = {
                    "status": "OVERRIDDEN_BY_USER",
                    "reason": override_reason or "User proceeded despite AI mismatch",
                    "user_id": end_user_context.user_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
                log_action(
                    db,
                    user_id=end_user_context.user_id,
                    action_type="LG_BANK_REPLY_OVERRIDDEN",
                    entity_type="LGInstruction",
                    entity_id=instruction_id,
                    details={
                        "lg_number": updated_instruction.lg_record.lg_number if updated_instruction.lg_record else None,
                        "serial_number": updated_instruction.serial_number,
                        "override_reason": override_reason or "User proceeded despite AI mismatch",
                        "bank_reply_date": str(bank_reply_date)
                    },
                    customer_id=end_user_context.customer_id,
                    lg_record_id=updated_instruction.lg_record.id if updated_instruction.lg_record else None,
                )
                _notify_corporate_admins_of_custody_event(
                    db=db,
                    customer_id=end_user_context.customer_id,
                    lg_record=updated_instruction.lg_record,
                    content=(
                        f"AI Verification Override on Bank Reply: End User recorded bank reply for LG "
                        f"{updated_instruction.lg_record.lg_number if updated_instruction.lg_record else 'N/A'} "
                        f"(Instruction {updated_instruction.serial_number}) with override note: "
                        f"'{override_reason or 'User proceeded despite AI mismatch'}'."
                    ),
                    notification_type="LG_BANK_REPLY_AI_OVERRIDDEN",
                    actor_user_id=end_user_context.user_id,
                    emit_mobile_defect=True,
                )

            if not request_admin_terms_approval and isinstance(details.get("bank_terms_admin_approval"), dict):
                prev_admin_approval = details["bank_terms_admin_approval"]
                if prev_admin_approval.get("status") == "REJECTED_BY_ADMIN":
                    details["bank_terms_admin_approval"] = {
                        **prev_admin_approval,
                        "status": "RESOLVED_BY_CORRECTED_REPLY",
                        "resolved_at": datetime.utcnow().isoformat(),
                        "discrepancy_letter_ready": False,
                    }

            updated_instruction.details = details
            db.add(updated_instruction)
            db.commit()
            db.refresh(updated_instruction)

            return LGInstructionOut.model_validate(updated_instruction)
        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type="LG_BANK_REPLY_RECORDED_FAILED", entity_type="LGInstruction", entity_id=instruction_id, details={"reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=db_instruction.lg_record_id if db_instruction else None)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred in record_bank_reply_api: {e}", exc_info=True)
            log_action(db, user_id=end_user_context.user_id, action_type="LG_BANK_REPLY_RECORDED_FAILED", entity_type="LGInstruction", entity_id=instruction_id, details={"reason": f"An unexpected error occurred: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=db_instruction.lg_record_id if db_instruction else None)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred: {e}"
            )
    finally:
        del file_bytes
        import gc
        gc.collect()


@router.post(
    "/lg-records/instructions/{instruction_id}/confirm-bank-reply",
    response_model=LGInstructionOut,
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_for_read_only_mode)],
    summary="Confirm bank reply for an LG instruction following AI verification review (Proceed Anyway)"
)
async def confirm_bank_reply_api(
    instruction_id: int,
    override_reason: Optional[str] = Form(None, description="Reason for proceeding despite AI discrepancies."),
    bank_reply_date: Optional[date] = Form(None),
    reply_details: Optional[str] = Form(None),
    bank_reply_document_metadata: Optional[str] = Form(None),
    bank_reply_document_file: Optional[UploadFile] = File(None),
    ai_verification_json: Optional[str] = Form(None),
    accept_date_alignment: bool = Form(False),
    aligned_expiry_date: Optional[date] = Form(None),
    switched_from_instruction_id: Optional[int] = Form(None),
    request_admin_terms_approval: bool = Form(False),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """Maker/Checker confirms bank reply despite AI verification mismatches (Proceed Anyway) - saves attached file."""
    return await record_bank_reply_api(
        instruction_id=instruction_id,
        bank_reply_date=bank_reply_date or date.today(),
        reply_details=reply_details,
        bank_reply_document_metadata=bank_reply_document_metadata,
        bank_reply_document_file=bank_reply_document_file,
        force_override=not accept_date_alignment and not request_admin_terms_approval,
        override_reason=override_reason or "User confirmed bank reply despite AI discrepancies",
        ai_verification_json=ai_verification_json,
        accept_date_alignment=accept_date_alignment,
        aligned_expiry_date=aligned_expiry_date,
        switched_from_instruction_id=switched_from_instruction_id,
        request_admin_terms_approval=request_admin_terms_approval,
        db=db,
        end_user_context=end_user_context,
        request=request
    )


@router.post(
    "/lg-records/instructions/{instruction_id}/cancel-bank-reply",
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_for_read_only_mode)],
    summary="Cancel pending bank reply recording to re-upload document"
)
def cancel_bank_reply_api(
    instruction_id: int,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    return {"status": "cancelled", "instruction_id": instruction_id, "message": "Bank reply recording cancelled. You can re-upload."}

# NEW ENDPOINT: Maker withdraws pending approval request
@router.post(
    "/approval-requests/{request_id}/withdraw",
    response_model=ApprovalRequestOut,
    dependencies=[Depends(HasPermission("lg_record:create")), Depends(check_for_read_only_mode)], # ADDED dependency
    summary="Maker withdraws a pending approval request"
)
def withdraw_approval_request(
    request_id: int,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request_info: Request = None
):
    """
    Allows a Maker to withdraw their own pending approval request.
    The request must be in PENDING status and initiated by the current user.
    """
    client_host = request_info.client.host if request_info else None

    db_approval_request = crud_approval_request.get_approval_request_by_id(db, request_id, end_user_context.customer_id)

    if not db_approval_request:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval request not found or not accessible.")

    try:
        withdrawn_request = crud_approval_request.withdraw_request(
            db,
            db_approval_request,
            end_user_context.user_id,
            end_user_context.customer_id
        )

        return withdrawn_request
    except HTTPException as e:
        log_action(db, user_id=end_user_context.user_id, action_type="APPROVAL_REQUEST_WITHDRAWN_FAILED", entity_type="ApprovalRequest", entity_id=request_id, details={"reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=db_approval_request.entity_id if db_approval_request.entity_type == "LGRecord" else None)
        raise
    except Exception as e:
        log_action(db, user_id=end_user_context.user_id, action_type="APPROVAL_REQUEST_WITHDRAWN_FAILED", entity_type="ApprovalRequest", entity_id=request_id, details={"reason": f"An unexpected error occurred: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=db_approval_request.entity_id if db_approval_request.entity_type == "LGRecord" else None)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

# NEW ENDPOINT: Maker views their own pending approval requests
@router.get(
    "/approval-requests/my-pending",
    response_model=List[ApprovalRequestOut],
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)], # ADDED dependency
    summary="Retrieve current user's pending approval requests"
)
def get_my_pending_approval_requests(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieves a list of approval requests submitted by the current user (Maker)
    that are currently in a PENDING status.
    """
    pending_requests = crud_approval_request.get_maker_pending_requests(
        db,
        maker_user_id=end_user_context.user_id,
        customer_id=end_user_context.customer_id
    )
    return pending_requests

@router.get(
    "/lg-records/instructions/{instruction_id}/view-letter",
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)],
)
async def view_lg_instruction_letter(
    instruction_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:view_own")),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    print_flag: bool = Query(False, alias="print", description="If true, attempts to trigger print dialog."),
    raw_pdf: bool = Query(False, description="If true, streams raw PDF bytes without HTML print toolbar.")
):
    """
    Generates and returns an LG Instruction letter as a PDF for viewing or printing.
    If 'print=true' query parameter is present, it attempts to trigger the browser's print dialog.
    """
    logger.info(f"Attempting to view letter for instruction ID: {instruction_id}. Print flag: {print_flag}")
    
    # CRITICAL FIX: Eager load the required relationships
    db_instruction = db.query(models.LGInstruction).options(
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_currency),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.issuing_bank),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.internal_owner_contact),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.customer),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.communication_bank), # NEW: Eager load communication_bank
    ).filter(
        models.LGInstruction.id == instruction_id,
        models.LGInstruction.lg_record.has(models.LGRecord.customer_id == end_user_context.customer_id),
    ).first()

    if not db_instruction:
        logger.warning(f"Instruction ID {instruction_id} not found.")
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="LG Instruction not found.")
    
    template = crud_template.get(db, db_instruction.template_id)
    if not template:
        logger.error(f"Template ID {db_instruction.template_id} not found for instruction ID {instruction_id}.")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Template not found.")

    generated_html = template.content
    logger.debug(f"Template content fetched for instruction ID {instruction_id}. Length: {len(generated_html)}.")

    instruction_details = db_instruction.details if db_instruction.details is not None else {}
    
    # Use the already loaded lg_record from db_instruction to avoid another DB call.
    lg_record_for_template = db_instruction.lg_record
    if lg_record_for_template:
        
        # Determine the correct recipient details using the new helper
        recipient_name, recipient_address = crud_lg_record._get_recipient_details(db, lg_record_for_template)
        issuance_date_str = lg_record_for_template.issuance_date.strftime("%Y-%m-%d") if getattr(lg_record_for_template, "issuance_date", None) else "N/A"
        expiry_date_str = lg_record_for_template.expiry_date.strftime("%Y-%m-%d") if getattr(lg_record_for_template, "expiry_date", None) else "N/A"
        customer_obj = lg_record_for_template.customer
        entity_obj = lg_record_for_template.beneficiary_corporate
        customer_address_val = (entity_obj.address if entity_obj and entity_obj.address else (customer_obj.address if customer_obj else "")) or ""
        customer_email_val = (entity_obj.contact_email if entity_obj and entity_obj.contact_email else (customer_obj.contact_email if customer_obj else "")) or ""

        instruction_details.update({
            "lg_number": lg_record_for_template.lg_number,
            "lg_serial_number": lg_record_for_template.lg_number,
            "instruction_serial": db_instruction.serial_number or "",
            "serial_number": db_instruction.serial_number or "",
            "lg_amount": float(lg_record_for_template.lg_amount),
            "lg_currency": lg_record_for_template.lg_currency.iso_code,
            "issuing_bank_name": crud_lg_record._get_effective_issuing_bank_name(lg_record_for_template),
            "lg_issuer_name": lg_record_for_template.issuer_name,
            "lg_beneficiary_name": lg_record_for_template.beneficiary_corporate.entity_name,
            "customer_name": lg_record_for_template.customer.name,
            "customer_address": customer_address_val,
            "customer_contact_email": customer_email_val,
            "internal_owner_email": lg_record_for_template.internal_owner_contact.email,
            "current_date": date.today().strftime("%Y-%m-%d"),
            "platform_name": "Treasury Management Platform",
            "recipient_name": recipient_name,
            "recipient_address": recipient_address,
        })
        instruction_details.setdefault("issuance_date", issuance_date_str)
        instruction_details.setdefault("issue_date", issuance_date_str)
        instruction_details.setdefault("expiry_date", instruction_details.get("old_expiry_date") or expiry_date_str)
        instruction_details.setdefault("maturity_date", instruction_details.get("old_expiry_date") or expiry_date_str)
        instruction_details.setdefault("notes_section", "")

        if "old_expiry_date" in instruction_details and "new_expiry_date" in instruction_details and "extension_period" not in instruction_details:
            try:
                old_d = datetime.strptime(str(instruction_details["old_expiry_date"])[:10], "%Y-%m-%d").date()
                new_d = datetime.strptime(str(instruction_details["new_expiry_date"])[:10], "%Y-%m-%d").date()
                instruction_details["extension_period"] = crud_lg_record._format_extension_period(old_d, new_d)
            except Exception:
                instruction_details["extension_period"] = "N/A"

        if "enclosed_documents_clause" not in instruction_details:
            pending_cnt = instruction_details.get("pending_replies_count", 0)
            instruction_details["enclosed_documents_clause"] = (
                "Kindly find the original Letter of Guarantee attached herewith along with all its extension confirmation(s) received from your esteemed bank for your records and cancellation."
                if not pending_cnt
                else "Kindly find the original Letter of Guarantee attached herewith; however, as we have not yet received all LG extension confirmation(s) from your esteemed bank, we hereby commit to return them back to the bank immediately upon receipt."
            )

        if "settlement_instruction" not in instruction_details:
            instruction_details["settlement_instruction"] = (
                f"Kindly arrange to settle the proceeds of this guarantee by bank transfer or issuing a bank draft/cashier order payable to {lg_record_for_template.beneficiary_corporate.entity_name} ({lg_record_for_template.customer.name}), in accordance with our standing settlement instructions."
            )

        instruction_details["lg_amount_formatted"] = f"{lg_record_for_template.lg_currency.symbol} {float(lg_record_for_template.lg_amount):,.2f}"
        if "original_lg_amount" in instruction_details:
             instruction_details["original_lg_amount_formatted"] = f"{lg_record_for_template.lg_currency.symbol} {float(instruction_details['original_lg_amount']):,.2f}"
        if "new_lg_amount" in instruction_details:
             instruction_details["new_lg_amount_formatted"] = f"{lg_record_for_template.lg_currency.symbol} {float(instruction_details['new_lg_amount']):,.2f}"
        if "decrease_amount" in instruction_details:
             instruction_details["decrease_amount_formatted"] = f"{lg_record_for_template.lg_currency.symbol} {float(instruction_details['decrease_amount']):,.2f}"
        if "original_lg_amount" in instruction_details and "new_lg_amount" in instruction_details and "liquidation_amount_formatted" not in instruction_details:
             liq_amt = float(instruction_details["original_lg_amount"]) - float(instruction_details["new_lg_amount"])
             instruction_details["liquidation_amount"] = liq_amt
             instruction_details["liquidation_amount_formatted"] = f"{lg_record_for_template.lg_currency.symbol} {liq_amt:,.2f}"

        if db_instruction.instruction_type == ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE and "payment_details" in instruction_details:
            payment_details = instruction_details["payment_details"]
            instruction_details["payment_method"] = payment_details.get("payment_method")
            instruction_details["payment_amount"] = float(payment_details.get("amount"))
            instruction_details["payment_currency_code"] = crud_currency.get(db, payment_details.get("currency_id")).iso_code if payment_details.get("currency_id") else ""
            instruction_details["payment_reference"] = payment_details.get("payment_reference")
            instruction_details["payment_issuing_bank_name"] = crud_bank.get(db, payment_details.get("issuing_bank_id")).name if payment_details.get("issuing_bank_id") else ""
            instruction_details["payment_date"] = payment_details.get("payment_date")
            instruction_details["payment_amount_formatted"] = f"{instruction_details['payment_currency_code']} {instruction_details['payment_amount']:,.2f}"


    for key, value in instruction_details.items():
        str_value = str(value) if value is not None else ""
        generated_html = generated_html.replace(f"{{{{{key}}}}}", str_value)
    logger.debug(f"Placeholders populated for instruction ID {instruction_id}.")


    try:
        generated_pdf_bytes = await generate_pdf_from_html(generated_html, f"lg_instruction_{db_instruction.serial_number}")
        if not generated_pdf_bytes:
            logger.error(f"generate_pdf_from_html returned None for instruction ID {instruction_id}.")
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to generate PDF document.")
    except Exception as e:
        logger.error(f"Exception during PDF generation for instruction ID {instruction_id}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to generate PDF document due to an internal error: {e}")

    if raw_pdf:
        headers = {'Content-Disposition': f'inline; filename="{db_instruction.serial_number}.pdf"'}
        logger.info(f"Successfully generated and streaming raw PDF for instruction ID {instruction_id}.")
        return StreamingResponse(io.BytesIO(generated_pdf_bytes), media_type="application/pdf", headers=headers)

    import base64
    pdf_base64 = base64.b64encode(generated_pdf_bytes).decode('utf-8')
    lg_num_display = lg_record_for_template.lg_number if lg_record_for_template else "N/A"
    action_label = (db_instruction.instruction_type or "Instruction").replace("LG_", "").replace("_", " ").title()

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Instruction Letter - {db_instruction.serial_number}</title>
        <style>
            body {{ margin: 0; padding: 0; overflow: hidden; font-family: 'Segoe UI', Arial, sans-serif; background: #1e293b; }}
            .toolbar {{ background: #1e293b; color: white; height: 54px; padding: 0 24px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; box-sizing: border-box; }}
            .toolbar-actions {{ display: flex; gap: 10px; align-items: center; }}
            .btn-print {{ background: #e11d48; color: white; border: none; padding: 8px 16px; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 13px; }}
            .btn-print:hover {{ background: #be123c; }}
            .btn-download {{ background: #334155; color: #f8fafc; border: 1px solid #475569; padding: 8px 14px; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 13px; text-decoration: none; }}
            .btn-download:hover {{ background: #475569; }}
            #pdfFrame {{ width: 100vw; height: calc(100vh - 54px); border: none; background: #f3f4f6; display: block; }}
            @media print {{
                .toolbar {{ display: none !important; }}
                #pdfFrame {{ height: 100vh; }}
            }}
        </style>
    </head>
    <body>
        <div class="toolbar">
            <div style="font-size: 14px;">
                <strong>Bank Instruction Letter ({action_label})</strong> — LG: {lg_num_display} (Instruction: {db_instruction.serial_number})
            </div>
            <div class="toolbar-actions">
                <a id="downloadLink" class="btn-download" download="{db_instruction.serial_number}.pdf">⬇️ Download PDF</a>
                <button class="btn-print" onclick="triggerPrint()">🖨️ Print Instruction Letter</button>
            </div>
        </div>
        <iframe id="pdfFrame" title="LG Instruction PDF"></iframe>
        <script>
            var b64 = "{pdf_base64}";
            var bin = atob(b64);
            var len = bin.length;
            var bytes = new Uint8Array(len);
            for (var i = 0; i < len; i++) {{
                bytes[i] = bin.charCodeAt(i);
            }}
            var blob = new Blob([bytes], {{ type: "application/pdf" }});
            var blobUrl = URL.createObjectURL(blob);
            var frame = document.getElementById("pdfFrame");
            frame.src = blobUrl;
            document.getElementById("downloadLink").href = blobUrl;

            function triggerPrint() {{
                try {{
                    if (frame && frame.contentWindow) {{
                        frame.contentWindow.focus();
                        frame.contentWindow.print();
                        return;
                    }}
                }} catch (e) {{}}
                window.print();
            }}
            {"window.onload = function() { setTimeout(function() { triggerPrint(); }, 600); };" if print_flag else ""}
        </script>
    </body>
    </html>
    """
    logger.info(f"Returning HTML viewer with print toolbar for instruction ID {instruction_id}.")
    return HTMLResponse(content=html_content, status_code=status.HTTP_200_OK)


@router.get(
    "/lg-records/instructions/{instruction_id}/view-discrepancy-letter",
    response_class=HTMLResponse,
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)],
    summary="Generate and view/print a formal Bank Reply Discrepancy Notice letter after Corporate Admin rejects bank-modified terms"
)
def view_bank_reply_discrepancy_letter(
    instruction_id: int,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    print_flag: bool = Query(False, alias="print", description="If true, triggers print dialog."),
):
    db_instruction = db.query(models.LGInstruction).options(
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.customer),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.issuing_bank),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_currency),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_type),
    ).filter(
        models.LGInstruction.id == instruction_id,
        models.LGInstruction.is_deleted == False,
    ).first()

    if not db_instruction or not db_instruction.lg_record or db_instruction.lg_record.customer_id != end_user_context.customer_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Instruction not found or not accessible.")

    lg = db_instruction.lg_record
    inst_details = db_instruction.details or {}
    approval_info = inst_details.get("bank_terms_admin_approval") or {}
    amendment_details = approval_info.get("amendment_details") or {}
    rejection_reason = approval_info.get("rejection_reason") or "Bank-modified terms do not match the corporate instruction."

    bank_name = lg.issuing_bank.name if lg.issuing_bank else (lg.foreign_bank_name or "Issuing Bank")
    bank_address = lg.issuing_bank.address if lg.issuing_bank and getattr(lg.issuing_bank, "address", None) else "Trade Finance / Letter of Guarantee Department"
    customer_name = (
        lg.beneficiary_corporate.entity_name
        if lg.beneficiary_corporate and getattr(lg.beneficiary_corporate, "entity_name", None)
        else (lg.customer.name if lg.customer else "Corporate Treasury")
    )
    def _fmt_full_date(val: Any, fallback: str = "N/A") -> str:
        if not val:
            return fallback
        if hasattr(val, "strftime"):
            return val.strftime("%B %d, %Y")
        s = str(val).strip()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(s[:10] if fmt.startswith("%Y") or fmt.startswith("%d") else s, fmt).strftime("%B %d, %Y")
            except Exception:
                continue
        return s

    currency_code = lg.lg_currency.iso_code if lg.lg_currency else "EGP"
    current_date_str = _fmt_full_date(datetime.now())
    inst_date_str = _fmt_full_date(db_instruction.instruction_date, current_date_str)
    delivery_date_str = _fmt_full_date(db_instruction.delivery_date, "N/A")
    raw_bank_reply_date = (
        inst_details.get("bank_reply_letter_date")
        or approval_info.get("rejected_bank_reply_date")
        or db_instruction.bank_reply_date
    )
    bank_reply_date_str = _fmt_full_date(raw_bank_reply_date, "Recent Advice")
    action_label = (db_instruction.instruction_type or "Instruction").replace("LG_", "").replace("_", " ").title()

    rows_html = ""
    if "expiry_date" in amendment_details:
        raw_req_exp = (
            inst_details.get("new_expiry_date")
            or inst_details.get("requested_expiry_date")
            or lg.expiry_date
        )
        req_exp = _fmt_full_date(raw_req_exp, "As Instructed")
        bank_exp = _fmt_full_date(amendment_details.get("expiry_date"), "As Advised")
        rows_html += f"""
        <tr>
            <td><strong>Amended Expiry Date</strong></td>
            <td>{req_exp}</td>
            <td style="color: #b91c1c; font-weight: 700;">{bank_exp}</td>
        </tr>
        """
    if "lg_amount" in amendment_details:
        req_amt = inst_details.get("new_amount") or inst_details.get("new_lg_amount") or float(lg.lg_amount or 0)
        try:
            req_amt_fmt = f"{currency_code} {float(req_amt):,.2f}"
        except Exception:
            req_amt_fmt = str(req_amt)
        try:
            bank_amt_fmt = f"{currency_code} {float(amendment_details['lg_amount']):,.2f}"
        except Exception:
            bank_amt_fmt = str(amendment_details["lg_amount"])
        rows_html += f"""
        <tr>
            <td><strong>Amended LG Amount</strong></td>
            <td>{req_amt_fmt}</td>
            <td style="color: #b91c1c; font-weight: 700;">{bank_amt_fmt}</td>
        </tr>
        """
    if not rows_html:
        rows_html = f"""
        <tr>
            <td><strong>Instruction Terms ({action_label})</strong></td>
            <td>Per Instruction {db_instruction.serial_number}</td>
            <td style="color: #b91c1c; font-weight: 700;">{approval_info.get('reason') or 'Modified Terms in Bank Advice'}</td>
        </tr>
        """

    discrepancy_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Discrepancy Notice - {db_instruction.serial_number}</title>
        <style>
            @page {{ size: A4; margin: 20mm; }}
            body {{ font-family: 'Segoe UI', Arial, sans-serif; color: #111827; line-height: 1.6; margin: 0; padding: 0; background: #f3f4f6; }}
            .toolbar {{ background: #1e293b; color: white; padding: 12px 24px; display: flex; justify-content: space-between; align-items: center; position: sticky; top: 0; z-index: 10; }}
            .toolbar button {{ background: #e11d48; color: white; border: none; padding: 8px 16px; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 13px; }}
            .toolbar button:hover {{ background: #be123c; }}
            .page {{ max-width: 210mm; margin: 24px auto; background: white; padding: 22mm; box-shadow: 0 4px 20px rgba(0,0,0,0.08); border-radius: 4px; box-sizing: border-box; }}
            .header {{ border-bottom: 2px solid #1e3a8a; padding-bottom: 14px; margin-bottom: 24px; display: flex; justify-content: space-between; align-items: flex-start; }}
            .corp-title {{ font-size: 18px; font-weight: 700; color: #1e3a8a; }}
            .doc-badge {{ background: #fef2f2; color: #b91c1c; border: 1px solid #fecaca; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-weight: 700; text-transform: uppercase; }}
            .meta-block {{ margin-bottom: 20px; font-size: 14px; }}
            .subject-box {{ background: #f8fafc; border-left: 4px solid #b91c1c; padding: 12px 16px; margin: 20px 0; font-weight: 700; font-size: 14px; color: #111827; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 13.5px; }}
            th, td {{ border: 1px solid #d1d5db; padding: 10px 12px; text-align: left; }}
            th {{ background: #f1f5f9; font-weight: 700; color: #334155; }}
            .signatures {{ margin-top: 48px; display: flex; justify-content: space-between; }}
            .sig-line {{ width: 42%; border-top: 1px solid #374151; padding-top: 8px; font-size: 13px; color: #4b5563; }}
            @media print {{
                body {{ background: white; }}
                .toolbar {{ display: none !important; }}
                .page {{ margin: 0; padding: 0; box-shadow: none; max-width: 100%; }}
            }}
        </style>
    </head>
    <body>
        <div class="toolbar">
            <div>
                <strong>Bank Reply Discrepancy Notice</strong> — LG: {lg.lg_number} (Instruction: {db_instruction.serial_number})
            </div>
            <button onclick="window.print()">🖨️ Print Discrepancy Letter</button>
        </div>
        <div class="page">
            <div class="header">
                <div>
                    <div class="corp-title">{customer_name}</div>
                    <div style="font-size: 12px; color: #6b7280;">Letter of Guarantee Operations</div>
                </div>
                <div style="text-align: right;">
                    <div class="doc-badge">Discrepancy &amp; Correction Notice</div>
                    <div style="font-size: 12px; color: #4b5563; margin-top: 6px;"><strong>Date:</strong> {current_date_str}</div>
                    <div style="font-size: 12px; color: #4b5563;"><strong>Ref:</strong> {db_instruction.serial_number}/DISC</div>
                </div>
            </div>

            <div class="meta-block">
                <div><strong>To:</strong> {bank_name}</div>
                <div><strong>Address:</strong> {bank_address}</div>
            </div>

            <div class="subject-box">
                SUBJECT: Notice of Discrepancy &amp; Non-Acceptance of Bank Amendment — Letter of Guarantee No. {lg.lg_number}<br/>
                <span style="font-weight: 500; font-size: 13px; color: #4b5563;">
                    Our Instruction Ref: <strong>{db_instruction.serial_number}</strong> ({action_label}) dated {inst_date_str} (Delivered: {delivery_date_str})
                </span>
            </div>

            <p style="font-size: 14px;">Dear Trade Finance Team,</p>
            <p style="font-size: 14px;">
                We refer to our official corporate instruction ref. <strong>{db_instruction.serial_number}</strong> dated <strong>{inst_date_str}</strong> regarding
                Letter of Guarantee No. <strong>{lg.lg_number}</strong>, and to your subsequent bank advice dated <strong>{bank_reply_date_str}</strong>.
            </p>
            <p style="font-size: 14px;">
                Upon review of your advice, we noted a discrepancy between the terms requested in our instruction
                and the terms stated in your bank advice, as detailed below:
            </p>

            <table>
                <thead>
                    <tr>
                        <th>Parameter</th>
                        <th>Our Instructed Terms ({db_instruction.serial_number})</th>
                        <th>Terms Stated in Bank Advice ({bank_reply_date_str})</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>

            <p style="font-size: 14px;">
                Please be formally advised that <strong>we do not accept the modified amendment terms</strong> stated in your bank advice dated <strong>{bank_reply_date_str}</strong>,
                as they deviate from our authorized instruction ref. <strong>{db_instruction.serial_number}</strong>.
            </p>

            <p style="font-size: 14px;">
                Accordingly, we request that you rectify this discrepancy immediately and issue a <strong>corrected Amendment Advice</strong> for Letter of Guarantee No. <strong>{lg.lg_number}</strong>
                strictly reflecting the exact terms instructed in our reference <strong>{db_instruction.serial_number}</strong>, and provide us with the revised confirmation at your earliest convenience.
            </p>

            <p style="font-size: 14px;">Yours faithfully,<br/><strong>For and on behalf of {customer_name}</strong></p>

            <div class="signatures">
                <div class="sig-line">Authorized Signatory (1)<br/>Name &amp; Title:</div>
                <div class="sig-line">Authorized Signatory (2)<br/>Name &amp; Title:</div>
            </div>
        </div>
        {"<script>window.onload = function() { setTimeout(function() { window.print(); }, 400); };</script>" if print_flag else ""}
    </body>
    </html>
    """
    if print_flag and isinstance(inst_details.get("bank_terms_admin_approval"), dict):
        updated_inst_details = dict(inst_details)
        updated_approval_info = dict(updated_inst_details.get("bank_terms_admin_approval") or {})
        updated_approval_info["discrepancy_letter_printed"] = True
        updated_approval_info["discrepancy_letter_printed_at"] = datetime.utcnow().isoformat()
        updated_inst_details["bank_terms_admin_approval"] = updated_approval_info
        db_instruction.details = updated_inst_details
        flag_modified(db_instruction, "details")
        db.add(db_instruction)
        db.commit()

    log_action(
        db,
        user_id=end_user_context.user_id,
        action_type="DISCREPANCY_LETTER_GENERATED",
        entity_type="LGInstruction",
        entity_id=db_instruction.id,
        details={"serial_number": db_instruction.serial_number, "lg_record_id": lg.id},
        customer_id=end_user_context.customer_id,
        lg_record_id=lg.id,
    )
    return HTMLResponse(content=discrepancy_html, status_code=status.HTTP_200_OK)

        
@router.post(
    "/lg-records/instructions/{instruction_id}/mark-as-accessed-for-print",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_for_read_only_mode)], # ADDED dependency
    summary="Marks an LG instruction as accessed for print."
)
async def mark_instruction_as_accessed_for_print(
    instruction_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_instruction:update_status")),
    end_user_context: TokenData = Depends(get_current_end_user_context)
):
    """
    Marks an LG instruction as accessed for print.
    This will stop any pending reminders/escalations for this instruction.
    It will also send a "Print Confirmation" email notification.
    """
    # Eager load necessary relations for email sending
    db_instruction = db.query(models.LGInstruction).options(
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.customer),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_currency),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.issuing_bank),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.internal_owner_contact),
        selectinload(models.LGInstruction.maker_user),
        selectinload(models.LGInstruction.checker_user),
    ).filter(
        models.LGInstruction.id == instruction_id,
        models.LGInstruction.lg_record.has(models.LGRecord.customer_id == end_user_context.customer_id),
    ).first()

    if not db_instruction or not db_instruction.lg_record:
        logger.warning(f"Mark as printed: Instruction {instruction_id} not found or not accessible for customer {end_user_context.customer_id}.")
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="LG Instruction not found or not accessible.")

    if db_instruction.is_printed:
        logger.info(f"Instruction {instruction_id} already marked as printed. Skipping update and email.")
        return {"message": "Instruction already marked as accessed for print."}

    # Update is_printed flag
    db_instruction.is_printed = True
    db.add(db_instruction)
    db.flush() # Flush to update is_printed before committing the overall transaction

    # Find the related ApprovalRequest and update its print_notification_status in details
    related_approval_request = db.query(models.ApprovalRequest).filter(
        models.ApprovalRequest.related_instruction_id == instruction_id,
        models.ApprovalRequest.status == ApprovalRequestStatusEnum.APPROVED
    ).first()

    if related_approval_request:
        related_approval_request.request_details = related_approval_request.request_details if related_approval_request.request_details else {}
        related_approval_request.request_details["print_notification_status"] = "PRINTED" # Set status to stop future reminders/escalations
        db.add(related_approval_request)
        db.flush()
        logger.info(f"ApprovalRequest {related_approval_request.id} updated to PRINTED status for print reminders.")

    # Log the action
    log_action(
        db,
        user_id=end_user_context.user_id,
        action_type="INSTRUCTION_ACCESSED_FOR_PRINT",
        entity_type="LGInstruction",
        entity_id=db_instruction.id,
        details={"serial_number": db_instruction.serial_number, "lg_record_id": db_instruction.lg_record.id},
        customer_id=end_user_context.customer_id,
        lg_record_id=db_instruction.lg_record.id,
    )


    # --- NEW: Send "Print Confirmation" Email (Email 2) ---

    logger.info(f"Attempting to send 'Print Confirmation' notification for Instruction ID: {instruction_id}.")
    if db_instruction.instruction_type == ACTION_TYPE_LG_EXTEND:
        logger.info(f"Skipping 'Print Confirmation' email for LG Extension Instruction ID: {instruction_id}.")
        # Log the action that the notification was skipped
        log_action(
            db,
            user_id=end_user_context.user_id,
            action_type="NOTIFICATION_SKIPPED",
            entity_type="LGInstruction",
            entity_id=instruction_id,
            details={"reason": "Skipped 'Print Confirmation' for LG_EXTEND instruction type."},
            customer_id=end_user_context.customer_id,
            lg_record_id=db_instruction.lg_record.id,
        )
        # Immediately exit this part of the function to prevent email sending
        return {"message": "Instruction marked as accessed for print, but print confirmation email skipped for LG Extension."}
    
    # The rest of the email sending logic follows this block:
    email_settings_to_use: EmailSettings
    email_method_for_log: str
    try:
        email_settings_to_use, email_method_for_log = get_customer_email_settings(db, end_user_context.customer_id)

    except Exception as e:
        email_settings_to_use = get_global_email_settings()
        email_method_for_log = "global_fallback_due_to_error"
        logger.warning(f"Failed to retrieve customer-specific email settings for customer ID {end_user_context.customer_id}: {e}. Falling back to global settings for print confirmation notification.")

    # *** CRITICAL FIX: Initialize to_emails and cc_emails HERE ***
    email_to_send_to = [db_instruction.lg_record.internal_owner_contact.email]
    cc_emails = []
    if db_instruction.lg_record.internal_owner_contact.manager_email:
        cc_emails.append(db_instruction.lg_record.internal_owner_contact.manager_email)
    if db_instruction.lg_record.lg_category and db_instruction.lg_record.lg_category.communication_list:
        cc_emails.extend(db_instruction.lg_record.lg_category.communication_list)

    common_comm_list_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
    )
    if common_comm_list_config and common_comm_list_config.get('effective_value'):
        try:
            parsed_common_list = json.loads(common_comm_list_config['effective_value'])
            if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                cc_emails.extend(parsed_common_list)
        except json.JSONDecodeError:
            logger.warning(f"COMMON_COMMUNICATION_LIST for customer {end_user_context.customer_id} is not a valid JSON list of emails. Skipping for print confirmation notification.")
    cc_emails = list(set(cc_emails))

    # Fetch the template for "Print Confirmation"
    notification_template = db.query(models.Template).filter(
        models.Template.action_type == NOTIFICATION_PRINT_CONFIRMATION, # Use the new constant
        models.Template.is_global == True,
        models.Template.is_notification_template == True,
        models.Template.is_deleted == False
    ).first()

    if not notification_template:
        logger.error(f"Email notification template for '{NOTIFICATION_PRINT_CONFIRMATION}' not found. Cannot send print confirmation notification for Instruction {instruction_id}.")
        log_action(
            db,
            user_id=end_user_context.user_id,
            action_type="NOTIFICATION_FAILED",
            entity_type="LGInstruction",
            entity_id=instruction_id,
            # to_emails and cc_emails are now guaranteed to be defined
            details={"reason": f"'{NOTIFICATION_PRINT_CONFIRMATION}' template missing for print confirmation", "recipient": email_to_send_to, "cc_recipients": cc_emails}, # Include cc_recipients
            customer_id=end_user_context.customer_id,
            lg_record_id=db_instruction.lg_record.id,
        )
    else:
        template_data = {
            "maker_email": db_instruction.maker_user.email if db_instruction.maker_user else "N/A",
            "lg_number": db_instruction.lg_record.lg_number,
            "instruction_serial_number": db_instruction.serial_number,
            "action_type": db_instruction.instruction_type.replace('_', ' ').title(),
            "current_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "platform_name": "Treasury Management Platform",
            "customer_name": db_instruction.lg_record.customer.name,
            "lg_amount": f"{db_instruction.lg_record.lg_currency.symbol} {float(db_instruction.lg_record.lg_amount):,.2f}" if db_instruction.lg_record.lg_currency else "N/A",
        }

        email_subject = notification_template.subject if notification_template.subject else f"Confirmed: LG {{lg_number}} Instruction {{instruction_serial_number}} Printed"
        email_body_html = notification_template.content
        for key, value in template_data.items():
            str_value = str(value) if value is not None else ""
            email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
            email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

        try:
            email_sent_successfully = await send_email(
                db=db,
                to_emails=email_to_send_to,
                cc_emails=cc_emails,
                subject_template=email_subject,
                body_template=email_body_html,
                template_data=template_data,
                email_settings=email_settings_to_use,
            )

            if email_sent_successfully:
                log_action(
                    db,
                    user_id=end_user_context.user_id,
                    action_type="NOTIFICATION_SENT",
                    entity_type="LGInstruction",
                    entity_id=instruction_id,
                    details={
                        "recipient": email_to_send_to,
                        "cc_recipients": cc_emails,
                        "subject": email_subject,
                        "method": email_method_for_log,
                        "notification_type": "Print Confirmation"
                    },
                    customer_id=end_user_context.customer_id,
                    lg_record_id=db_instruction.lg_record.id,
                )
                logger.info(f"Print confirmation notification sent successfully for Instruction ID: {instruction_id}.")
            else:
                logger.error(f"send_email returned False for 'Print Confirmation' notification for Instruction ID: {instruction_id}.")
                log_action(
                    db,
                    user_id=end_user_context.user_id,
                    action_type="NOTIFICATION_FAILED",
                    entity_type="LGInstruction",
                    entity_id=instruction_id,
                    details={"reason": "Email service failed to send print confirmation notification (send_email returned False)", "recipient": email_to_send_to, "subject": email_subject, "method": email_method_for_log}, # Use correct variable names
                    customer_id=end_user_context.customer_id,
                    lg_record_id=db_instruction.lg_record.id,
                )
        except Exception as e:
            logger.exception(f"Exception occurred while sending 'Print Confirmation' notification for Instruction ID: {instruction_id}: {e}")
            log_action(
                db,
                user_id=end_user_context.user_id,
                action_type="NOTIFICATION_FAILED",
                entity_type="LGInstruction",
                entity_id=instruction_id,
                details={"reason": f"Exception during email send for 'Print Confirmation': {e}", "recipient": email_to_send_to, "subject": email_subject, "method": email_method_for_log}, # Use correct variable names
                customer_id=end_user_context.customer_id,
                lg_record_id=db_instruction.lg_record.id,
            )

    db.refresh(db_instruction)
    return {"message": "Instruction marked as accessed for print successfully."}

@router.get(
    "/lg-records/{lg_record_id}/lifecycle-history",
    response_model=List[LGLifecycleEventOut],
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)], # ADDED dependency
    summary="Retrieve lifecycle history for a specific LG record"
)
def get_lg_record_lifecycle_history(
    lg_record_id: int,
    db: Session = Depends(get_db),
    current_user_context: TokenData = Depends(get_current_end_user_context),
    action_type: Optional[str] = Query(None, description="Filter by a specific action type (e.g., 'LG_EXTENDED', 'NOTIFICATION_SENT')")
):
    """
    Fetches the chronological list of all audit log events associated with a given LG record,
    with an optional filter for action type.
    """
    lg_record = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, current_user_context.customer_id)
    if not lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")

    events = crud_audit_log.get_lg_lifecycle_events(db, lg_record_id, current_user_context.customer_id, action_type=action_type)

    lifecycle_events_out = []
    for event in events:
        user_email = event.user.email if event.user else None
        lifecycle_events_out.append(
            LGLifecycleEventOut(
                id=event.id,
                timestamp=event.timestamp,
                action_type=event.action_type,
                user_email=user_email,
                entity_type=event.entity_type,
                entity_id=event.entity_id,
                details=event.details if event.details else {},
            )
        )
    return lifecycle_events_out

@router.get(
    "/customer-configurations/{config_key}",
    response_model=CustomerConfigurationOut,
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)], # ADDED dependency
    summary="Retrieve a specific effective customer configuration by key"
)
def get_customer_configuration_for_end_user(
    config_key: GlobalConfigKey, # Use the Enum for validation
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Retrieves the effective value of a specific customer configuration setting (customer override or global default).
    Accessible by end-users for dynamic frontend behavior.
    """
    config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, config_key
    )
    
    if not config: # Handle case where global_config_crud.get_by_key might return None
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Configuration key '{config_key}' not found or accessible.")
    
    # The crud function now returns a dict that matches CustomerConfigurationOut
    # We use **config to unpack the dictionary into keyword arguments for the Pydantic model
    return CustomerConfigurationOut(**config)

@router.get("/users/lookup-by-email/", response_model=Optional[UserOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def lookup_system_user_by_email(
    email: str = Query(..., description="Email of the system user to lookup"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    end_user_context: TokenData = Depends(get_current_end_user_context)
):
    """
    Looks up a system user (User model) by email within the current user's customer organization.
    Returns user details if found. This endpoint is for system user management/reference,
    NOT for populating internal LG owner fields (use /internal-owner-contacts/lookup-by-email/ for that).
    """
    user = crud_user.get_by_email(db, email)

    if not user or user.customer_id != end_user_context.customer_id or user.is_deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="System user not found in your organization or is deactivated.")

    return user

@router.get("/customer-entities/", response_model=List[CustomerEntityOut], dependencies=[Depends(check_subscription_status)])
def get_customer_entities_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieves a list of active customer entities accessible by the current user.
    Filtered by real-time DB permissions.
    """
    # FIX: Get fresh permissions
    fresh_has_all_access, fresh_entity_ids = get_fresh_entity_permissions(db, end_user_context.user_id)

    if fresh_has_all_access:
        entities = crud_customer_entity.get_all_for_customer(db, end_user_context.customer_id, skip=skip, limit=limit)
    else:
        # Only return entities the user is explicitly linked to in the DB
        entities = db.query(crud_customer_entity.model).filter(
            crud_customer_entity.model.id.in_(fresh_entity_ids),
            crud_customer_entity.model.customer_id == end_user_context.customer_id,
            crud_customer_entity.model.is_deleted == False
        ).offset(skip).limit(limit).all()
    return entities


@router.get("/currencies/", response_model=List[CurrencyOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def get_currencies_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active currencies."""
    currencies = crud_currency.get_all(db, skip=skip, limit=limit)
    return currencies

@router.get("/lg-types/", response_model=List[LgTypeOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def get_lg_types_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active LG types."""
    lg_types = crud_lg_type.get_all(db, skip=skip, limit=limit)
    return lg_types

@router.get("/banks/", response_model=List[BankOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def get_banks_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active banks."""
    banks = crud_bank.get_all(db, skip=skip, limit=limit)
    return banks

@router.get("/issuing-methods/", response_model=List[IssuingMethodOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def get_issuing_methods_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active issuing methods."""
    issuing_methods = crud_issuing_method.get_all(db, skip=skip, limit=limit)
    return issuing_methods

@router.get("/rules/", response_model=List[RuleOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def get_rules_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active rules."""
    rules = crud_rule.get_all(db, skip=skip, limit=limit)
    return rules

@router.get("/lg-statuses/", response_model=List[LgStatusOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def get_lg_statuses_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active LG statuses."""
    lg_statuses = crud_lg_status.get_all(db, skip=skip, limit=limit)
    return lg_statuses

@router.get("/lg-operational-statuses/", response_model=List[LgOperationalStatusOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
def get_lg_operational_statuses_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active LG operational statuses."""
    lg_operational_statuses = crud_lg_operational_status.get_all(db, skip=skip, limit=limit)
    return lg_operational_statuses

@router.get("/lg-categories/", response_model=List[LGCategoryOut], dependencies=[Depends(check_subscription_status)])
def get_lg_categories_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieves a list of all active LG categories relevant to the authenticated End User's customer,
    including Universal Categories (managed by the system) as default options.
    """
    # CRITICAL CHANGE: Use the new unified CRUD method that handles both customer and universal categories
    all_categories_from_db = crud_lg_category.get_all_for_customer(db, end_user_context.customer_id, skip=skip, limit=limit)
    
    all_categories = []
    customer = crud_customer.get(db, end_user_context.customer_id)
    customer_name = customer.name if customer else "Unknown Customer"

    for cat in all_categories_from_db:
        # Check if the user has access to this category's entities
        has_access = False
        if cat.customer_id is None or cat.has_all_entity_access:
            has_access = True
        else:
            category_entity_ids = {assoc.customer_entity_id for assoc in cat.entity_associations}
            if end_user_context.has_all_entity_access or not end_user_context.entity_ids:
                has_access = True
            elif category_entity_ids.intersection(set(end_user_context.entity_ids)):
                has_access = True

        if not has_access:
            continue

        entities_with_access_for_cat = []
        if cat.customer_id is not None and not cat.has_all_entity_access:
            entity_ids_with_access = set(end_user_context.entity_ids).intersection(
                {assoc.customer_entity.id for assoc in cat.entity_associations if assoc.customer_entity}
            )
            entities_with_access_for_cat = [
                CustomerEntityOut.model_validate(db.query(models.CustomerEntity).get(entity_id))
                for entity_id in entity_ids_with_access
                if db.query(models.CustomerEntity).get(entity_id) is not None and not db.query(models.CustomerEntity).get(entity_id).is_deleted
            ]
        elif cat.customer_id is not None and cat.has_all_entity_access:
             entities_with_access_for_cat = [
                CustomerEntityOut.model_validate(entity)
                for entity in db.query(models.CustomerEntity).filter(models.CustomerEntity.customer_id == end_user_context.customer_id, models.CustomerEntity.is_deleted == False).all()
            ]

        # CRITICAL FIX: Pass the ORM object directly to Pydantic's model_validate.
        # We pass extra arguments as a dictionary to handle the computed fields.
        category_out = LGCategoryOut.model_validate(
            cat,
            context={
                'name': cat.name,
                'customer_name': customer_name if cat.customer_id is not None else "System Default",
                'entities_with_access': entities_with_access_for_cat
            }
        )
        all_categories.append(category_out)

    all_categories.sort(key=lambda x: (0 if x.customer_id is None else 1, x.name.lower()))
    return all_categories

@router.post(
    "/lg-records/instructions/{original_instruction_id}/send-reminder-to-bank",
    response_class=HTMLResponse,
    dependencies=[Depends(HasPermission("lg_instruction:send_reminder")), Depends(check_for_read_only_mode)],
    summary="Send a reminder instruction letter to the bank for an LG instruction awaiting reply"
)
async def send_reminder_to_bank_api(
    original_instruction_id: int,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Allows an End User to manually send a reminder instruction to the bank.
    """
    # 1. Fetch instruction first to get valid IDs for logging
    db_instruction = crud_lg_instruction.get(db, original_instruction_id)
    if not db_instruction:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instruction not found.")
    
    # Store the valid LG Record ID for logging
    valid_lg_record_id = db_instruction.lg_record_id

    try:
        # 2. Call the CRUD function (PDF generation happens INSIDE here)
        lg_record, new_instruction_id, generated_pdf_bytes = await crud_lg_instruction.send_bank_reminder(
            db,
            original_instruction_id=original_instruction_id,
            user_id=end_user_context.user_id,
            customer_id=end_user_context.customer_id
        )

        if not generated_pdf_bytes:
            logger.error(f"PDF bytes were not generated for reminder for original instruction ID {original_instruction_id}.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to generate reminder PDF.")

        # 3. Return the PDF for printing with the unified top toolbar
        pdf_base64 = base64.b64encode(generated_pdf_bytes).decode('utf-8')
        lg_num_display = lg_record.lg_number if lg_record else "N/A"
        serial_display = db_instruction.serial_number or f"INST-{original_instruction_id}"

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8" />
            <title>Print LG Bank Reminder - {serial_display}</title>
            <style>
                body {{ margin: 0; padding: 0; overflow: hidden; font-family: 'Segoe UI', Arial, sans-serif; background: #1e293b; }}
                .toolbar {{ background: #1e293b; color: white; height: 54px; padding: 0 24px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; box-sizing: border-box; }}
                .toolbar-actions {{ display: flex; gap: 10px; align-items: center; }}
                .btn-print {{ background: #e11d48; color: white; border: none; padding: 8px 16px; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 13px; }}
                .btn-print:hover {{ background: #be123c; }}
                .btn-download {{ background: #334155; color: #f8fafc; border: 1px solid #475569; padding: 8px 14px; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 13px; text-decoration: none; }}
                .btn-download:hover {{ background: #475569; }}
                #pdfFrame {{ width: 100vw; height: calc(100vh - 54px); border: none; background: #f3f4f6; display: block; }}
                @media print {{
                    .toolbar {{ display: none !important; }}
                    #pdfFrame {{ height: 100vh; }}
                }}
            </style>
        </head>
        <body>
            <div class="toolbar">
                <div style="font-size: 14px;">
                    <strong>Bank Reminder Notice</strong> — LG: {lg_num_display} (Original Instruction: {serial_display})
                </div>
                <div class="toolbar-actions">
                    <a id="downloadLink" class="btn-download" download="Reminder_{serial_display}.pdf">⬇️ Download PDF</a>
                    <button class="btn-print" onclick="triggerPrint()">🖨️ Print Reminder Letter</button>
                </div>
            </div>
            <iframe id="pdfFrame" title="LG Bank Reminder PDF"></iframe>
            <script>
                var b64 = "{pdf_base64}";
                var bin = atob(b64);
                var len = bin.length;
                var bytes = new Uint8Array(len);
                for (var i = 0; i < len; i++) {{
                    bytes[i] = bin.charCodeAt(i);
                }}
                var blob = new Blob([bytes], {{ type: "application/pdf" }});
                var blobUrl = URL.createObjectURL(blob);
                var frame = document.getElementById("pdfFrame");
                frame.src = blobUrl;
                document.getElementById("downloadLink").href = blobUrl;

                function triggerPrint() {{
                    try {{
                        if (frame && frame.contentWindow) {{
                            frame.contentWindow.focus();
                            frame.contentWindow.print();
                            return;
                        }}
                    }} catch (e) {{}}
                    window.print();
                }}
                window.onload = function() {{
                    setTimeout(function() {{ triggerPrint(); }}, 600);
                }};
            </script>
        </body>
        </html>
        """
        logger.info(f"Generated reminder PDF for instruction {original_instruction_id}.")
        return HTMLResponse(content=html_content, status_code=status.HTTP_200_OK)

    except HTTPException as e:
        # FIX: Use valid_lg_record_id to avoid IntegrityError
        log_action(db, user_id=end_user_context.user_id, action_type="LG_REMINDER_SENT_TO_BANK_FAILED", entity_type="LGInstruction", entity_id=original_instruction_id, details={"reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=valid_lg_record_id)
        raise
    except Exception as e:
        # FIX: Use valid_lg_record_id to avoid IntegrityError
        logger.exception(f"Unexpected error in send_reminder_to_bank_api for instruction {original_instruction_id}: {e}")
        log_action(db, user_id=end_user_context.user_id, action_type="LG_REMINDER_SENT_TO_BANK_FAILED", entity_type="LGInstruction", entity_id=original_instruction_id, details={"reason": f"An unexpected error occurred: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=valid_lg_record_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.get(
    "/lg-records/instructions/generate-all-bank-reminders-pdf",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_instruction:send_reminder")), Depends(check_for_read_only_mode)],
    summary="Generate a consolidated PDF of all eligible LG bank reminder instructions for printing"
)
async def generate_all_eligible_bank_reminders_pdf_api(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Automatically identifies all LG instructions for the current customer that are eligible
    for a bank reminder (based on predefined conditions), generates a single PDF document
    containing all such reminders, and returns a JSON object with the base64-encoded PDF.
    No instruction IDs are passed by the user; the system determines the list automatically.
    """
    client_host = get_client_ip(request) if request else None

    try:
        consolidated_pdf_bytes, generated_reminder_count, eligible_instruction_ids = await crud_lg_instruction.generate_all_eligible_bank_reminders_pdf(
            db,
            customer_id=end_user_context.customer_id,
            user_id=end_user_context.user_id
        )

        if not consolidated_pdf_bytes:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No eligible LG instructions found for a bank reminder to generate a consolidated PDF."
            )

        pdf_base64 = base64.b64encode(consolidated_pdf_bytes).decode('utf-8')

        log_action(db, user_id=end_user_context.user_id, action_type="GENERATE_BULK_REMINDER_PDF_SUCCESS", entity_type="LGInstruction", entity_id=None, details={"reminders_generated_count": generated_reminder_count, "eligible_instruction_ids": eligible_instruction_ids}, customer_id=end_user_context.customer_id)
        
        return {
            "message": f"Successfully generated a consolidated PDF for {generated_reminder_count} reminders.",
            "combined_pdf_base64": pdf_base64
        }

    except HTTPException as e:
        log_action(db, user_id=end_user_context.user_id, action_type="GENERATE_BULK_REMINDER_PDF_FAILED", entity_type="LGInstruction", entity_id=None, details={"reason": str(e.detail), "customer_id": end_user_context.customer_id}, customer_id=end_user_context.customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=end_user_context.user_id, action_type="GENERATE_BULK_REMINDER_PDF_FAILED", entity_type="LGInstruction", entity_id=None, details={"reason": f"An unexpected error occurred: {e}", "customer_id": end_user_context.customer_id}, customer_id=end_user_context.customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

# --- Action Center Specific Endpoints ---

@router.get(
    "/action-center/lg-for-renewal",
    response_model=List[LGRecordOut],
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)],
    summary="Get LG Records approaching expiry/renewal for Action Center"
)
def get_action_center_lg_for_renewal(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    # 1. Get Fresh Permissions
    has_all, allowed_ids = get_fresh_entity_permissions(db, end_user_context.user_id)

    # 2. Pass them to the updated CRUD method
    lg_records = crud_lg_record.get_lg_records_for_renewal_reminder(
        db, 
        end_user_context.customer_id,
        user_has_all_access=has_all,
        user_allowed_entity_ids=allowed_ids
    )
    return lg_records

@router.get(
    "/action-center/instructions-undelivered",
    response_model=List[LGInstructionOut],
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_subscription_status)],
    summary="Get LG Instructions awaiting delivery confirmation for Action Center"
)
def get_action_center_instructions_undelivered(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    # 1. Get Fresh Permissions
    has_all, allowed_ids = get_fresh_entity_permissions(db, end_user_context.user_id)

    # 2. Fetch Data (This gets ALL undelivered instructions for the customer)
    # ... (existing config logic) ...
    report_start_days_config = 0
    report_stop_days_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_STOP_REPORTING_UNDELIVERED
    )
    report_stop_days = int(report_stop_days_config['effective_value']) if report_stop_days_config else 60
    
    instructions = crud_lg_instruction.get_undelivered_instructions_for_reporting(
        db, end_user_context.customer_id, 0, report_stop_days
    )

    # 3. FILTER RESULTS IN PYTHON (Since we can't modify crud_lg_instruction easily right now)
    if not has_all:
        allowed_ids_set = set(allowed_ids)
        instructions = [
            inst for inst in instructions 
            if inst.lg_record and inst.lg_record.beneficiary_corporate_id in allowed_ids_set
        ]

    return instructions

@router.get(
    "/action-center/instructions-awaiting-reply",
    response_model=List[LGInstructionOut],
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_subscription_status)],
    summary="Get LG Instructions awaiting bank reply for Action Center"
)
def get_action_center_instructions_awaiting_reply(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    # 1. Get Fresh Permissions
    has_all, allowed_ids = get_fresh_entity_permissions(db, end_user_context.user_id)

    # 2. Fetch Data
    # ... (existing config logic) ...
    days_since_delivery_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_DELIVERY
    )
    days_since_issuance_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE
    )
    max_days_since_issuance_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.REMINDER_TO_BANKS_MAX_DAYS_SINCE_ISSUANCE
    )

    days_since_delivery = int(days_since_delivery_config['effective_value']) if days_since_delivery_config else 0
    days_since_issuance = int(days_since_issuance_config['effective_value']) if days_since_issuance_config else 0
    max_days_since_issuance = int(max_days_since_issuance_config['effective_value']) if max_days_since_issuance_config else 90

    instructions = crud_lg_instruction.get_instructions_for_bank_reminders(
        db, end_user_context.customer_id, days_since_delivery, days_since_issuance, max_days_since_issuance
    )

    # 3. FILTER RESULTS IN PYTHON
    if not has_all:
        allowed_ids_set = set(allowed_ids)
        instructions = [
            inst for inst in instructions 
            if inst.lg_record and inst.lg_record.beneficiary_corporate_id in allowed_ids_set
        ]

    return instructions


@router.get(
    "/action-center/approved-requests-pending-print",
    response_model=List[ApprovalRequestOut],
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)], # ADDED dependency
    summary="Get approved LG record actions that require printing and are not yet printed"
)
def get_approved_requests_pending_print(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieves a list of approved Approval Requests for the current user's customer
    where the associated LG Instruction requires printing (e.g., Release, Liquidation,
    Decrease Amount, Activate Non-Operative) and has not yet been marked as printed.
    These are tasks for the maker (End User) to follow up on.
    """
    has_all, allowed_ids = get_fresh_entity_permissions(db, end_user_context.user_id)

    # Define instruction types that require printing (consistent with background_tasks)
    INSTRUCTION_TYPES_REQUIRING_PRINTING = [
        ACTION_TYPE_LG_RELEASE,
        ACTION_TYPE_LG_LIQUIDATE,
        ACTION_TYPE_LG_DECREASE_AMOUNT,
        ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
        # ACTION_TYPE_LG_AMEND - Excluded based on clarification
    ]

    # Query for ApprovalRequests that are:
    # 1. APPROVED
    # 2. Related to an LGRecord (entity_type='LGRecord')
    # 3. Have a linked instruction (related_instruction_id IS NOT NULL)
    # 4. The action_type is one that generates a bank letter.
    # 5. The related LGInstruction is NOT yet printed (is_printed = False).
    # 6. The maker_user_id matches the current user (only show maker's own pending prints).

    pending_print_requests = db.query(models.ApprovalRequest).filter(
        models.ApprovalRequest.customer_id == end_user_context.customer_id,
        models.ApprovalRequest.status == ApprovalRequestStatusEnum.APPROVED,
        models.ApprovalRequest.entity_type == "LGRecord",
        models.ApprovalRequest.maker_user_id == end_user_context.user_id, # Only show for the current maker
        models.ApprovalRequest.related_instruction_id.isnot(None),
        models.ApprovalRequest.action_type.in_(INSTRUCTION_TYPES_REQUIRING_PRINTING),
        models.ApprovalRequest.related_instruction.has(models.LGInstruction.is_printed == False) # Check the linked instruction's printed status
    ).options(
        selectinload(models.ApprovalRequest.maker_user),
        selectinload(models.ApprovalRequest.lg_record).selectinload(models.LGRecord.lg_status),
        selectinload(models.ApprovalRequest.lg_record).selectinload(models.LGRecord.internal_owner_contact),
        selectinload(models.ApprovalRequest.related_instruction).selectinload(models.LGInstruction.template)
    ).order_by(models.ApprovalRequest.created_at.asc()).offset(skip).limit(limit).all()

    if not has_all:
        allowed_ids_set = set(allowed_ids)
        pending_print_requests = [
            req for req in pending_print_requests
            if req.lg_record and req.lg_record.beneficiary_corporate_id in allowed_ids_set
        ]

    return pending_print_requests[skip : skip + limit]


@router.get(
    "/action-center/discrepancy-letters-pending-print",
    response_model=List[LGInstructionOut],
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)],
    summary="Get LG instructions where bank-modified terms were rejected by Corporate Admin and a Discrepancy Notice letter is ready to print"
)
def get_discrepancy_letters_pending_print(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    has_all, allowed_ids = get_fresh_entity_permissions(db, end_user_context.user_id)
    instructions = db.query(models.LGInstruction).join(
        models.LGRecord, models.LGInstruction.lg_record_id == models.LGRecord.id
    ).options(
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_status),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_currency),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.issuing_bank),
        selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.instructions),
        selectinload(models.LGInstruction.maker_user),
    ).filter(
        models.LGRecord.customer_id == end_user_context.customer_id,
        models.LGRecord.is_deleted == False,
        models.LGRecord.lg_status_id == LgStatusEnum.VALID.value,
        models.LGInstruction.is_deleted == False,
        models.LGInstruction.status != "Canceled",
        models.LGInstruction.bank_reply_date.is_(None),
        models.LGInstruction.details['bank_terms_admin_approval']['status'].astext == 'REJECTED_BY_ADMIN',
    ).order_by(models.LGInstruction.updated_at.desc()).all()

    results = []
    allowed_ids_set = set(allowed_ids) if not has_all else set()
    for inst in instructions:
        if not has_all and inst.lg_record and inst.lg_record.beneficiary_corporate_id not in allowed_ids_set:
            continue
        approval_info = (inst.details or {}).get("bank_terms_admin_approval") or {}
        if approval_info.get("discrepancy_letter_printed") is True:
            continue
        # Drop if a newer operational instruction has already been created on the same LG
        sibling_insts = (inst.lg_record.instructions or []) if inst.lg_record else []
        has_newer_operational_inst = any(
            s.id > inst.id
            and not s.is_deleted
            and s.status != "Canceled"
            and s.instruction_type != ACTION_TYPE_LG_REMINDER_TO_BANKS
            for s in sibling_insts
        )
        if has_newer_operational_inst:
            continue
        results.append(inst)
    return results

@router.get(
    "/users/me_dashboard_info",
    response_model=Dict[str, Any],
    dependencies=[Depends(check_subscription_status)],
    summary="Get current user's dashboard information"
)
def get_current_user_dashboard_info(
    db: Session = Depends(get_db),
    current_user_token: TokenData = Depends(get_current_active_user),
):
    # 1. Get Fresh Permissions
    has_all, allowed_ids = get_fresh_entity_permissions(db, current_user_token.user_id)
    
    db_user = crud_user.get(db, current_user_token.user_id)
    user_info = UserOut.model_validate(db_user)

    # 2. Pending Print Count (Filtered)
    pending_print_requests = db.query(models.ApprovalRequest).filter(
        models.ApprovalRequest.customer_id == current_user_token.customer_id,
        models.ApprovalRequest.status == ApprovalRequestStatusEnum.APPROVED,
        models.ApprovalRequest.entity_type == "LGRecord",
        models.ApprovalRequest.maker_user_id == current_user_token.user_id,
        models.ApprovalRequest.related_instruction_id.isnot(None),
        models.ApprovalRequest.related_instruction.has(models.LGInstruction.is_printed == False)
    ).options(selectinload(models.ApprovalRequest.lg_record)).all()

    if not has_all:
        allowed_ids_set = set(allowed_ids)
        pending_print_count = sum(1 for req in pending_print_requests if req.lg_record and req.lg_record.beneficiary_corporate_id in allowed_ids_set)
    else:
        pending_print_count = len(pending_print_requests)

    # 3. Renewal Reminders (Updated Call)
    lg_for_renewal_list = crud_lg_record.get_lg_records_for_renewal_reminder(
        db, 
        current_user_token.customer_id,
        user_has_all_access=has_all,
        user_allowed_entity_ids=allowed_ids
    )
    lg_for_renewal_count = len(lg_for_renewal_list)

    # 4. Active LG Count (Filtered)
    active_lg_query = db.query(models.LGRecord).filter(
        models.LGRecord.customer_id == current_user_token.customer_id,
        models.LGRecord.lg_status_id == LgStatusEnum.VALID.value,
        models.LGRecord.is_deleted == False
    )

    if not has_all:
        if not allowed_ids:
             active_lg_count = 0
        else:
             active_lg_count = active_lg_query.filter(
                 models.LGRecord.beneficiary_corporate_id.in_(allowed_ids)
             ).count()
    else:
        active_lg_count = active_lg_query.count()

    return {
        "user_details": user_info.model_dump(),
        "has_pending_prints": pending_print_count > 0,
        "pending_prints_count": pending_print_count,
        "has_lgs_for_renewal": lg_for_renewal_count > 0,
        "lgs_for_renewal_count": lg_for_renewal_count,
        "active_lgs_count": active_lg_count,
    }

# --- HELPER: Async Wrapper for URL Signing ---
# Note: This definition needs to be placed somewhere globally in end_user.py
# (outside of any route function) and requires 'import asyncio' and 
# 'from app.core.ai_integration import generate_signed_gcs_url'
async def _safe_generate_signed_url(gcs_uri: str) -> Optional[str]:
    """
    Safely generates a signed URL. This is necessary because generate_signed_gcs_url
    might return a coroutine that needs to be awaited.
    """
    try:
        if not gcs_uri: return None
        
        # Assuming generate_signed_gcs_url is imported from app.core.ai_integration
        result = generate_signed_gcs_url(gcs_uri)
        
        # This is the crucial logic to await if the result is a coroutine
        if asyncio.iscoroutine(result):
            return await result
        return result
    except Exception as e:
        # Assuming logger is imported globally in end_user.py
        logger.error(f"Error signing URL for end-user: {e}")
        return None

# =============================================================================
# REPLACEMENT ENDPOINTS
# =============================================================================

@router.get(
    "/system-notifications/",
    response_model=List[SystemNotificationOut],
    dependencies=[Depends(check_subscription_status)],
    summary="Get active system notifications for the current user's customer"
)
async def get_active_system_notifications(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    db.expire_all()
    notifications = crud_system_notification.get_active_notifications_for_user(
        db, user_id=end_user_context.user_id, customer_id=end_user_context.customer_id
    )

    results = []
    for n in notifications:
        # CRITICAL FIX: Explicitly detach the ORM object from the session
        # This prevents the subsequent mutation from being written to the DB.
        db.expunge(n) 
        
        if n.image_url and n.image_url.startswith("gs://"):
            # Use the safe async wrapper defined previously
            signed = await _safe_generate_signed_url(n.image_url) 
            if signed:
                n.image_url = signed
        
        results.append(n)

    return results
        
@router.post(
    "/system-notifications/{notification_id}/view", 
    response_model=dict,
    dependencies=[Depends(check_for_read_only_mode)],
    summary="Logs an automatic view for a system notification banner."
)
def log_system_notification_view(
    notification_id: int,
    db: Session = Depends(get_db),
    end_user_context = Depends(get_current_end_user_context),
):
    """
    Records a user's view of a system notification, incrementing the view count for display limits.
    """
    notification = crud_system_notification.get(db, id=notification_id)
    if not notification:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="System notification not found."
        )

    log = crud_system_notification.log_notification_display( 
        db,
        user_id=end_user_context.user_id,
        notification_id=notification_id
    )
    
    return {"status": "success", "view_count": log.view_count}


@router.post(
    "/system-notifications/{notification_id}/acknowledge", 
    response_model=dict,
    dependencies=[Depends(check_for_read_only_mode)],
    summary="Logs an acknowledgment for a system notification."
)
def acknowledge_system_notification(
    notification_id: int,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Records a user's acknowledgment of a system notification, incrementing the view count.
    """
    notification = crud_system_notification.get(db, id=notification_id)
    if not notification:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="System notification not found."
        )

    log = crud_system_notification.acknowledge_notification(
        db,
        user_id=end_user_context.user_id,
        notification_id=notification_id
    )
    
    return {"status": "success", "view_count": log.view_count}
# --------------------------------------------------------------------------------------
# 8. /lg-records/instructions/{instruction_id}/cancel
# --------------------------------------------------------------------------------------
@router.post(
    "/lg-records/instructions/{instruction_id}/cancel",
    response_model=Dict[str, Any],
    dependencies=[Depends(HasPermission("lg_instruction:cancel")), Depends(check_for_read_only_mode)], # ADDED dependency
    summary="Cancel the most recent eligible LG instruction."
)
async def cancel_lg_instruction(
    instruction_id: int,
    cancel_in: LGInstructionCancelRequest,
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    client_host = get_client_ip(request) if request else None
    
    # 1. Fetch the instruction to get the LG Record ID
    db_instruction = crud_lg_instruction.get(db, instruction_id)
    if not db_instruction or not db_instruction.lg_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instruction not found or not accessible.")
    
    lg_record_id = db_instruction.lg_record_id
    
    # 2. Check if Maker-Checker is enabled for the customer
    customer = crud_customer.get_with_relations(db, end_user_context.customer_id)
    if not customer or not customer.subscription_plan:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer or Subscription Plan not found.")

    is_maker_checker_enabled = customer.subscription_plan.can_maker_checker
    action_type = ACTION_TYPE_LG_CANCEL_LAST_INSTRUCTION

    if is_maker_checker_enabled:
        # Guard against duplicate pending cancellation requests for the same LG / instruction
        existing_pending_cancel = db.query(models.ApprovalRequest).filter(
            models.ApprovalRequest.customer_id == end_user_context.customer_id,
            models.ApprovalRequest.entity_type == "LGRecord",
            models.ApprovalRequest.entity_id == lg_record_id,
            models.ApprovalRequest.action_type == action_type,
            models.ApprovalRequest.status == ApprovalRequestStatusEnum.PENDING,
        ).first()
        if existing_pending_cancel:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"A cancellation request (#{existing_pending_cancel.id}) for instruction '{db_instruction.serial_number}' is already pending Corporate Admin approval."
            )

        # Maker-Checker flow: Create an ApprovalRequest
        try:
            lg_record_for_snapshot = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
            if not lg_record_for_snapshot:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Associated LG Record not found.")
            
            lg_snapshot = crud_approval_request._get_lg_record_snapshot(lg_record_for_snapshot)
            
            inst_meta = db_instruction.details if isinstance(db_instruction.details, dict) else {}
            approval_request_in = ApprovalRequestCreate(
                entity_type="LGRecord",
                entity_id=lg_record_id,
                action_type=action_type,
                request_details={
                    "instruction_id": instruction_id,
                    "instruction_serial": db_instruction.serial_number,
                    "instruction_type": db_instruction.instruction_type,
                    "instruction_status": db_instruction.status,
                    "old_expiry_date": inst_meta.get("old_expiry_date"),
                    "new_expiry_date": inst_meta.get("new_expiry_date"),
                    "reason": (cancel_in.reason or "").strip(),
                    "declaration_confirmed": cancel_in.declaration_confirmed,
                },
                lg_record_snapshot=lg_snapshot,
            )
            
            # **AWAIT** the asynchronous CRUD method call
            db_approval_request = await crud_approval_request.create_approval_request(
                db,
                approval_request_in,
                end_user_context.user_id,
                end_user_context.customer_id,
                lg_record=lg_record_for_snapshot,
            )

            inst_details = dict(db_instruction.details or {})
            inst_details["pending_cancellation_request_id"] = db_approval_request.id
            db_instruction.details = inst_details
            flag_modified(db_instruction, "details")
            db.add(db_instruction)
            db.commit()
            db.refresh(db_instruction)

            return {
                "message": f"Cancellation request for instruction '{db_instruction.serial_number}' submitted for approval.",
                "approval_request_id": db_approval_request.id,
                "status": db_approval_request.status.value,
                "instruction": LGInstructionOut.model_validate(db_instruction),
            }

        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type=AUDIT_ACTION_TYPE_LG_INSTRUCTION_CANCELLATION_FAILED, entity_type="LGInstruction", entity_id=instruction_id, details={"reason": str(e.detail), "lg_record_id": lg_record_id}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type=AUDIT_ACTION_TYPE_LG_INSTRUCTION_CANCELLATION_FAILED, entity_type="LGInstruction", entity_id=instruction_id, details={"reason": f"An unexpected error occurred during approval request creation: {e}", "lg_record_id": lg_record_id}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during approval request creation: {e}")

    else:
        # Direct Execution flow (Maker-Checker disabled)
        try:
            canceled_instruction, updated_lg_record = await crud_lg_cancellation.cancel_instruction(
                db,
                instruction_id=instruction_id,
                cancel_in=cancel_in,
                user_id=end_user_context.user_id,
                customer_id=end_user_context.customer_id,
            )

            return {
                "message": f"Instruction '{canceled_instruction.serial_number}' canceled successfully.",
                "instruction": LGInstructionOut.model_validate(canceled_instruction),
                "lg_record": LGRecordOut.model_validate(updated_lg_record),
            }

        except HTTPException as e:
            log_action(db, user_id=end_user_context.user_id, action_type=AUDIT_ACTION_TYPE_LG_INSTRUCTION_CANCELLATION_FAILED, entity_type="LGInstruction", entity_id=instruction_id, details={"reason": str(e.detail), "lg_record_id": lg_record_id}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise
        except Exception as e:
            log_action(db, user_id=end_user_context.user_id, action_type=AUDIT_ACTION_TYPE_LG_INSTRUCTION_CANCELLATION_FAILED, entity_type="LGInstruction", entity_id=instruction_id, details={"reason": f"An unexpected error occurred during direct cancellation: {e}", "lg_record_id": lg_record_id}, customer_id=end_user_context.customer_id, lg_record_id=lg_record_id)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during direct cancellation: {e}")

# --- NEW REPORT ENDPOINT ---
@router.get("/reports/lg-lifecycle-history", response_model=List[LGLifecycleHistoryReportItem])
def get_lg_lifecycle_history_report(
    # FIXED: Importing directly from app.database
    db: Session = Depends(get_db),
    # FIXED: Importing directly from app.core.security
    end_user_context = Depends(get_current_end_user_context),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    action_types: Optional[str] = Query(None, description="Comma-separated list of action types"),
    lg_record_ids: Optional[str] = Query(None, description="Comma-separated list of LG Record IDs")
):
    """
    Retrieve full lifecycle history report for export.
    """
    # Parse comma-separated strings into lists
    action_type_list = action_types.split(",") if action_types else None
    
    lg_record_id_list = None
    if lg_record_ids:
        try:
            lg_record_id_list = [int(id_str) for id_str in lg_record_ids.split(",")]
        except ValueError:
            pass 

    history = crud_reports.get_all_lg_lifecycle_history(
        db=db,
        customer_id=end_user_context.customer_id,
        user_id=end_user_context.user_id,
        start_date=start_date,
        end_date=end_date,
        action_types=action_type_list,
        lg_record_ids=lg_record_id_list
    )
    return history

# ==============================================================================
# ISSUANCE OWNERSHIP MANAGEMENT (END USER / ADMIN)
# ==============================================================================

@router.get("/issuance/requestors/directory")
def get_issuance_requestors_directory(
    db: Session = Depends(get_db),
    end_user_context = Depends(get_current_end_user_context),
):
    """
    Returns a unified directory of all requestors for the customer.
    """
    from app.crud.crud_issuance_owner import get_unique_requestors
    return get_unique_requestors(db, end_user_context.customer_id)

@router.put("/issuance/requestors/profile")
def update_issuance_requestor_profile(
    payload: dict,
    db: Session = Depends(get_db),
    end_user_context = Depends(get_current_end_user_context),
):
    """
    Corporate Admin edits a Requestor's profile data across all their active LG requests.
    payload: { old_email: str, updated_profile: RequestorProfile }
    """
    from app.crud.crud_issuance_owner import update_requestor_profile
    from app.schemas.schemas_issuance import RequestorProfile
    
    old_email = payload.get("old_email")
    prof_data = payload.get("updated_profile")
    if not old_email or not prof_data:
        raise HTTPException(status_code=400, detail="old_email and updated_profile required")

    profile = RequestorProfile(**prof_data)
    update_requestor_profile(db, end_user_context.customer_id, old_email, profile)
    return {"message": "Requestor profile updated successfully"}


@router.post("/issuance/handover/force")
async def force_issuance_handover(
    payload: dict,
    db: Session = Depends(get_db),
    end_user_context = Depends(get_current_end_user_context),
):
    """
    Corporate Admin initiates a force transfer of LGs to a new requestor.
    Subject to Maker-Checker approval.
    """
    from app.schemas.all_schemas import ApprovalRequestCreate
    from app.crud.crud import crud_approval_request
    from app.constants import ApprovalRequestStatusEnum

    # Validate payload through schema if needed, but doing directly here
    if not payload.get("lg_ids") or not payload.get("new_requestor"):
        raise HTTPException(status_code=400, detail="lg_ids and new_requestor are required")
        
    action_payload = {
        "lg_ids": payload["lg_ids"],
        "new_requestor": payload["new_requestor"]
    }

    approval_request_in = ApprovalRequestCreate(
        entity_type="IssuedLGRecord",
        entity_id=payload["lg_ids"][0] if payload["lg_ids"] else None,
        action_type="ISSUANCE_CHANGE_REQUESTOR",
        request_details=action_payload,
        # Default status is PENDING inside the schema
    )

    approval_request = await crud_approval_request.create_approval_request(
        db=db,
        obj_in=approval_request_in,
        maker_user_id=end_user_context.user_id,
        customer_id=end_user_context.customer_id
    )

    db.commit()

    return {"message": "Force handover request submitted for approval", "approval_request_id": approval_request.id}
