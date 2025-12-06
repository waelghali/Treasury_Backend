# c:\Grow\app\api/v1/endpoints\end_user.py

import os
import sys
import io
import base64
from datetime import date, datetime, timedelta
import logging
import decimal
import json
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query, UploadFile, File, Body, Form
from fastapi.responses import StreamingResponse, HTMLResponse
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import func, and_
from typing import List, Optional, Any, Dict, Tuple

from fuzzywuzzy import fuzz
from app.database import get_db

from app.schemas.all_schemas import (
    LGRecordCreate, LGRecordOut, LGDocumentOut,
    CustomerEntityOut, CurrencyOut, LgTypeOut, RuleOut, IssuingMethodOut,
    LgStatusOut, LgOperationalStatusOut, BankOut, LGCategoryOut, UserOut,
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

    from app.core.document_generator import generate_pdf_from_html
    import app.core.hashing
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

async def get_current_end_user_context(
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
def find_best_match(extracted_name: str, valid_entities: List[Dict[str, Any]], threshold: int = 75) -> Optional[int]:
    """
    Finds the best fuzzy match for an extracted name against a list of valid entities (name and ID).
    Returns the ID of the best match if the similarity score is above the threshold.
    """
    if not extracted_name:
        logger.debug(f"Fuzzy Match Debug: Extracted name is empty.")
        return None

    best_score = -1
    best_match_id = None
    best_match_name = None

    logger.debug(f"Fuzzy Match Debug: Searching for '{extracted_name}' among {len(valid_entities)} entities.")
    for entity in valid_entities:
        entity_name = entity.get('name')
        entity_id = entity.get('id')
        if not entity_name or entity_id is None:
            continue
        # FIX: Correct indentation for score calculation
        score = fuzz.token_set_ratio(extracted_name.lower(), entity_name.lower())
        logger.debug(f"Fuzzy Match Debug: Comparing '{extracted_name}' with '{entity_name}' (ID: {entity_id}) - Score: {score}")

        if score > best_score and score >= threshold:
            best_score = score
            best_match_id = entity_id
            best_match_name = entity_name

    if best_match_id:
        logger.debug(f"Fuzzy Match Debug: Best match found for '{extracted_name}' is '{best_match_name}' (ID: {best_match_id}) with score {best_score}.")
    else:
        logger.debug(f"Fuzzy Match Debug: No good fuzzy match found for '{extracted_name}' above threshold {threshold}.")

    return best_match_id


router = APIRouter()

INSTRUCTION_TYPES_REQUIRING_PRINTING = [
    ACTION_TYPE_LG_RELEASE,
    ACTION_TYPE_LG_LIQUIDATE,
    ACTION_TYPE_LG_DECREASE_AMOUNT,
    ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
]
@router.post(
    "/lg-records/run-auto-renewal",
    response_model=AutoRenewalRunSummaryOut, # Or StreamingResponse if sending PDF directly, or Dict[str, Any] with base64
    dependencies=[Depends(HasPermission("lg_record:extend")), Depends(check_for_read_only_mode)], # ADDED dependency
    summary="Run automated and forced LG renewal for eligible records"
)
async def run_auto_renewal_endpoint(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None # For client host logging
):
    """
    Triggers the bulk auto-renewal and force-renewal process for eligible LG records.
    Returns a summary of renewed LGs and the combined instruction PDF.
    """
    client_host = get_client_ip(request) if request else None

    try:
        renewed_count, combined_pdf_bytes = await crud_lg_record.run_auto_renewal_process(
            db,
            user_id=end_user_context.user_id,
            customer_id=end_user_context.customer_id
        )

        if combined_pdf_bytes:
            # Encode PDF to base64 for JSON response
            pdf_base64 = base64.b64encode(combined_pdf_bytes).decode('utf-8')
            return AutoRenewalRunSummaryOut(
                renewed_count=renewed_count,
                message=f"Successfully renewed {renewed_count} eligible LGs. Consolidated instruction letter generated.",
                combined_pdf_base64=pdf_base64
            )
        else:
            return AutoRenewalRunSummaryOut(
                renewed_count=renewed_count,
                message="No eligible LGs found or renewed.",
                combined_pdf_base64=None
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


@router.get("/status", dependencies=[Depends(check_subscription_status)]) # ADDED dependency
async def get_end_user_status(
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
        extracted_ai_data, ai_usage_metadata = await process_lg_document_with_ai(await file.read(), file.content_type, file.filename)
        if not extracted_ai_data:
            log_action(db, user_id=end_user_context.user_id, action_type="AI_SCAN_FAILED", entity_type="LGRecord", entity_id=None, details={"file_name": file.filename, "reason": "AI extraction failed or returned no data", "ai_token_usage": ai_usage_metadata}, customer_id=end_user_context.customer_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to extract data from the document using AI. Please try again or enter details manually."
            )

        all_customer_entities = crud_customer_entity.get_all_for_customer(db, end_user_context.customer_id)
        valid_beneficiary_names_with_ids = [{"name": entity.entity_name, "id": entity.id} for entity in all_customer_entities]
        all_banks = crud_bank.get_all(db)
        valid_bank_names_with_ids = [{"name": bank.name, "id": bank.id} for bank in all_banks]
        all_lg_types = crud_lg_type.get_all(db)
        valid_lg_types_with_ids = [{"name": lg_type.name, "id": lg_type.id} for lg_type in all_lg_types]

        mapped_data = {}
        extracted_beneficiary_name = extracted_ai_data.get("beneficiaryName")
        if extracted_beneficiary_name:
            beneficiary_id = find_best_match(extracted_beneficiary_name, valid_beneficiary_names_with_ids)
            if beneficiary_id:
                mapped_data["beneficiary_corporate_id"] = beneficiary_id
            else:
                logger.warning(f"No good fuzzy match found for Beneficiary Corporate '{extracted_beneficiary_name}'.")

        if extracted_ai_data.get("currency"):
            currency = crud_currency.get_by_iso_code(db, extracted_ai_data["currency"])
            if currency:
                mapped_data["lg_currency_id"] = currency.id
            else:
                logger.warning(f"LG Currency '{extracted_ai_data['currency']}' not found in DB.")

        extracted_lg_type_name = extracted_ai_data.get("lgType")
        if extracted_lg_type_name:
            lg_type_id = find_best_match(extracted_lg_type_name, valid_lg_types_with_ids)
            if lg_type_id:
                mapped_data["lg_type_id"] = lg_type_id
            else:
                logger.warning(f"No good fuzzy match found for LG Type '{extracted_lg_type_name}'.")

        extracted_issuing_bank_name = extracted_ai_data.get("issuingBankName")
        if extracted_issuing_bank_name:
            issuing_bank_id = find_best_match(extracted_issuing_bank_name, valid_bank_names_with_ids)
            if issuing_bank_id:
                mapped_data["issuing_bank_id"] = issuing_bank_id
            else:
                logger.warning(f"No good fuzzy match found for Issuing Bank '{extracted_issuing_bank_name}'.")

        response_data = {
            "issuer_name": extracted_ai_data.get("issuerName"),
            "lg_number": extracted_ai_data.get("lgNumber"),
            "lg_amount": extracted_ai_data.get("lgAmount"),
            "description_purpose": extracted_ai_data.get("purpose"),
            "other_conditions": extracted_ai_data.get("otherConditions"),
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
        "expiryDate": lg_record.expiry_date.isoformat(),
        "issuingBankName": lg_record.issuing_bank.name,
        "beneficiaryName": lg_record.beneficiary_corporate.entity_name,
        "issuerName": lg_record.issuer_name
    }
    
    try:
        # Use the new, dedicated AI function for amendments
        extracted_ai_data, _ = await process_amendment_with_ai(file_bytes, amendment_letter_file.content_type, lg_record_details=lg_record_details_for_ai)

        if not extracted_ai_data:
            # If AI returns no data, we raise a generic error for the frontend.
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="AI could not determine the amendment type. Please complete the form manually."
            )

        # New logic to confirm relevance
        # FIX: Check the 'is_relevant_amendment' flag returned by the AI.
        if not extracted_ai_data.get('is_relevant_amendment', False):
            return {"message": "AI could not confirm amendment is related to this specific LG. Please make sure you are amending the correct LG."}

        # Filter out fields that didn't change according to AI
        ai_suggested_details = extracted_ai_data.get('amendedFields', {})
        
        # Check if the AI successfully extracted any fields
        if not ai_suggested_details:
            return {"message": "AI analysis complete, but no amendments were detected. Please fill the details manually."}

        return {"ai_suggested_details": ai_suggested_details}
        
    except HTTPException as e:
        logger.error(f"AI processing failed for amendment letter on LG {lg_record.lg_number}: {e.detail}", exc_info=True)
        raise e
    except Exception as e:
        logger.error(f"AI processing failed for amendment letter on LG {lg_record.lg_number}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during AI analysis. Please try again or proceed manually."
        )

@router.get("/internal-owner-contacts/lookup-by-email/", response_model=Optional[InternalOwnerContactOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
async def lookup_internal_owner_by_email(
    email: str = Query(..., description="Email of the internal owner contact to lookup"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
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
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)], # ADDED dependency
    summary="Retrieve all internal owner contacts for the current customer with LG count"
)
async def list_internal_owner_contacts_with_lg_count(
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

@router.get(
    "/lg-documents/{document_id}/view",
    response_model=Dict[str, str], # Will return a JSON with the signed URL
    dependencies=[Depends(HasPermission("lg_document:view")), Depends(check_subscription_status)], # ADDED dependency
    summary="Retrieve a time-limited signed URL for viewing a private LG document"
)
async def view_lg_document_securely(
    document_id: int,
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

    # 3. Generate the signed URL using the helper from ai_integration.py
    signed_url = await generate_signed_gcs_url(db_document.file_path, valid_for_seconds)

    if not signed_url:
        logger.error(f"Failed to generate signed URL for document ID {document_id} (GCS path: {db_document.file_path}).")
        log_action(
            db,
            user_id=current_user.user_id,
            action_type="DOCUMENT_VIEW_FAILED",
            entity_type="LGDocument",
            entity_id=document_id,
            details={"file_name": db_document.file_name, "reason": "Failed to generate signed URL"},
            customer_id=current_user.customer_id,
            lg_record_id=db_document.lg_record_id
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate a secure viewing link for the document. Please try again later."
        )

    logger.info(f"Successfully generated signed URL for document ID {document_id}.")
    log_action(
        db,
        user_id=current_user.user_id,
        action_type="DOCUMENT_VIEWED_SECURELY",
        entity_type="LGDocument",
        entity_id=document_id,
        details={"file_name": db_document.file_name, "signed_url_valid_for_seconds": valid_for_seconds},
        customer_id=current_user.customer_id,
        lg_record_id=db_document.lg_record_id
    )
    return {"signed_url": signed_url}

@router.get("/lg-records/", response_model=List[LGRecordOut], dependencies=[Depends(check_subscription_status)])
async def list_lg_records(
    skip: int = 0,
    limit: int = 100,
    internal_owner_contact_id: Optional[int] = Query(None, description="Filter LG records by the ID of the internal owner contact"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:view_own")),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Retrieves a list of LG records belonging to the authenticated End User's customer.
    Includes an optional filter for internal owner contact ID.
    """
    # FIX: Get fresh permissions from DB instead of stale Token
    fresh_has_all_access, fresh_entity_ids = get_fresh_entity_permissions(db, end_user_context.user_id)

    lg_records = crud_lg_record.get_all_lg_records_for_customer(
        db,
        customer_id=end_user_context.customer_id,
        internal_owner_contact_id=internal_owner_contact_id,
        skip=skip,
        limit=limit,
        # NEW: Pass the FRESH security parameters
        user_has_all_access=fresh_has_all_access,
        user_allowed_entity_ids=fresh_entity_ids
    )
    return lg_records

@router.get(
    "/lg-records/{lg_record_id}",
    response_model=LGRecordOut,
    dependencies=[Depends(HasPermission("lg_record:view_own")), Depends(check_subscription_status)],
    summary="Retrieve a single LG record by ID"
)
async def get_lg_record_by_id(
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
            notes=notes # NEW: Pass notes to the crud function
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
    payment_reference: str = Form(..., max_length=100, description="Wire reference or check number."),
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
        payment_reference=payment_reference,
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
            
@router.post(
    "/lg-records/instructions/{instruction_id}/record-delivery",
    response_model=LGInstructionOut,
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_for_read_only_mode)], # ADDED dependency
    summary="Record the delivery of an LG instruction to the bank"
)
async def record_instruction_delivery_api(
    instruction_id: int,
    # FIX: Expect delivery_document_metadata as a Form string, and file as a File
    delivery_date: date = Form(..., description="The date the instruction was physically delivered to the bank."),
    delivery_document_metadata: Optional[str] = Form(None, description="JSON string of optional document metadata."),
    delivery_document_file: Optional[UploadFile] = File(None, description="Optional document proving delivery."),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    """
    Records the delivery details for a specific LG instruction.
    Allows specifying delivery date and an optional supporting document.
    """
    client_host = get_client_ip(request) if request else None

    # --- NEW ENFORCEMENT LOGIC: Check if supporting document is mandatory ---
    is_doc_mandatory_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.DOC_MANDATORY_RECORD_DELIVERY # <--- NEW KEY
    )
    is_doc_mandatory = is_doc_mandatory_config.get('effective_value', 'False').lower() == 'true'

    if is_doc_mandatory and delivery_document_file is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Supporting delivery document is mandatory for Recording Delivery as per Corporate Admin settings."
        )
    # --- END NEW ENFORCEMENT LOGIC ---

    # Manually create the LGInstructionRecordDelivery object
    file_bytes = None
    delivery_document_data = None

    if delivery_document_file:
        file_bytes = await delivery_document_file.read()

        if delivery_document_metadata:
            try:
                parsed_metadata = json.loads(delivery_document_metadata)
                # Ensure LGDocumentCreate fields are correctly parsed, and overwrite if needed
                # CRITICAL FIX: Ensure lg_instruction_id is set from path param, not from parsed metadata
                delivery_document_data = LGDocumentCreate(
                    document_type=parsed_metadata.get("document_type", "DELIVERY_PROOF"),
                    file_name=delivery_document_file.filename,
                    file_path="", # This will be populated by crud function
                    mime_type=delivery_document_file.content_type,
                    lg_instruction_id=instruction_id # FIX: Set lg_instruction_id here from path parameter
                )
            except json.JSONDecodeError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON for delivery document metadata.")
        else:
            # If no metadata string, create default metadata with instruction_id
            delivery_document_data = LGDocumentCreate(
                document_type="DELIVERY_PROOF",
                file_name=delivery_document_file.filename,
                file_path="",
                mime_type=delivery_document_file.content_type,
                lg_instruction_id=instruction_id # FIX: Set lg_instruction_id here from path parameter
            )
    
    delivery_in = LGInstructionRecordDelivery(
        delivery_date=delivery_date,
        delivery_document_file=delivery_document_data
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
        return updated_instruction
    except HTTPException as e:
        log_action(db, user_id=end_user_context.user_id, action_type="LG_RECORD_DELIVERY_FAILED", entity_type="LGInstruction", entity_id=instruction_id, details={"reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=instruction_id)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred in record_instruction_delivery_api: {e}", exc_info=True)
        log_action(db, user_id=end_user_context.user_id, action_type="LG_RECORD_DELIVERY_FAILED", entity_type="LGInstruction", entity_id=instruction_id, details={"reason": f"An unexpected error occurred: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=instruction_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

# IMPORTANT: Apply similar changes to record_bank_reply_api as well
@router.post(
    "/lg-records/instructions/{instruction_id}/record-bank-reply",
    response_model=LGInstructionOut,
    dependencies=[Depends(HasPermission("lg_instruction:update_status")), Depends(check_for_read_only_mode)], # ADDED dependency
    summary="Record the bank's reply to an LG instruction"
)
async def record_bank_reply_api(
    instruction_id: int,
    bank_reply_date: date = Form(..., description="The date the bank's reply was received."),
    reply_details: Optional[str] = Form(None, description="Details or notes from the bank's reply."),
    bank_reply_document_metadata: Optional[str] = Form(None, description="JSON string of optional document metadata."), # NEW PARAM
    bank_reply_document_file: Optional[UploadFile] = File(None, description="Optional document proving the bank's reply."),
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
    request: Request = None
):
    client_host = get_client_ip(request) if request else None


    # --- NEW ENFORCEMENT LOGIC: Check if supporting document is mandatory ---
    is_doc_mandatory_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, end_user_context.customer_id, GlobalConfigKey.DOC_MANDATORY_RECORD_BANK_REPLY # <--- NEW KEY
    )
    is_doc_mandatory = is_doc_mandatory_config.get('effective_value', 'False').lower() == 'true'

    if is_doc_mandatory and bank_reply_document_file is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Supporting bank reply document is mandatory for Recording Bank Reply as per Corporate Admin settings."
        )
    # --- END NEW ENFORCEMENT LOGIC ---

    file_bytes = None
    bank_reply_document_data = None

    if bank_reply_document_file:
        file_bytes = await bank_reply_document_file.read()

        if bank_reply_document_metadata:
            try:
                parsed_metadata = json.loads(bank_reply_document_metadata)
                bank_reply_document_data = LGDocumentCreate(
                    document_type=parsed_metadata.get("document_type", "BANK_REPLY"),
                    file_name=bank_reply_document_file.filename,
                    file_path="",
                    mime_type=bank_reply_document_file.content_type,
                    lg_instruction_id=instruction_id # FIX: Set lg_instruction_id here from path parameter
                )
            except json.JSONDecodeError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON for bank reply document metadata.")
        else:
            bank_reply_document_data = LGDocumentCreate(
                document_type="BANK_REPLY",
                file_name=bank_reply_document_file.filename,
                file_path="",
                mime_type=bank_reply_document_file.content_type,
                lg_instruction_id=instruction_id # FIX: Set lg_instruction_id here from path parameter
            )

    bank_reply_in = LGInstructionRecordBankReply(
        bank_reply_date=bank_reply_date,
        reply_details=reply_details,
        bank_reply_document_file=bank_reply_document_data
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
        return updated_instruction
    except HTTPException as e:
        log_action(db, user_id=end_user_context.user_id, action_type="LG_BANK_REPLY_RECORDED_FAILED", entity_type="LGInstruction", entity_id=instruction_id, details={"reason": str(e.detail)}, customer_id=end_user_context.customer_id, lg_record_id=instruction_id)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred in record_bank_reply_api: {e}", exc_info=True)
        log_action(db, user_id=end_user_context.user_id, action_type="LG_BANK_REPLY_RECORDED_FAILED", entity_type="LGInstruction", entity_id=instruction_id, details={"reason": f"An unexpected error occurred: {e}"}, customer_id=end_user_context.customer_id, lg_record_id=instruction_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

# NEW ENDPOINT: Maker withdraws pending approval request
@router.post(
    "/approval-requests/{request_id}/withdraw",
    response_model=ApprovalRequestOut,
    dependencies=[Depends(HasPermission("lg_record:create")), Depends(check_for_read_only_mode)], # ADDED dependency
    summary="Maker withdraws a pending approval request"
)
async def withdraw_approval_request(
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
async def get_my_pending_approval_requests(
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
    print_flag: bool = Query(False, alias="print", description="If true, attempts to trigger print dialog.")
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
        
        instruction_details.update({
            "lg_number": lg_record_for_template.lg_number,
            "lg_amount": float(lg_record_for_template.lg_amount),
            "lg_currency": lg_record_for_template.lg_currency.iso_code,
            "issuing_bank_name": lg_record_for_template.issuing_bank.name,
            "lg_beneficiary_name": lg_record_for_template.beneficiary_corporate.entity_name,
            "customer_name": lg_record_for_template.customer.name,
            "internal_owner_email": lg_record_for_template.internal_owner_contact.email,
            "current_date": date.today().strftime("%Y-%m-%d"),
            "platform_name": "Treasury Management Platform",
            "recipient_name": recipient_name,
            "recipient_address": recipient_address,
        })
        instruction_details["lg_amount_formatted"] = f"{lg_record_for_template.lg_currency.symbol} {float(lg_record_for_template.lg_amount):,.2f}"
        if "original_lg_amount" in instruction_details:
             instruction_details["original_lg_amount_formatted"] = f"{lg_record_for_template.lg_currency.symbol} {float(instruction_details['original_lg_amount']):,.2f}"
        if "new_lg_amount" in instruction_details:
             instruction_details["new_lg_amount_formatted"] = f"{lg_record_for_template.lg_currency.symbol} {float(instruction_details['new_lg_amount']):,.2f}"
        if "decrease_amount" in instruction_details:
             instruction_details["decrease_amount_formatted"] = f"{lg_record_for_template.lg_currency.symbol} {float(instruction_details['decrease_amount']):,.2f}"

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

    if print_flag:
        import base64
        pdf_base64 = base64.b64encode(generated_pdf_bytes).decode('utf-8')

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Print LG Instruction</title>
            <style>
                body {{ margin: 0; overflow: hidden; }}
                embed {{ width: 100vw; height: 100vh; border: none; }}
            </style>
        </head>
        <body>
            <embed src="data:application/pdf;base64,{pdf_base64}" type="application/pdf" width="100%" height="100%">
            <script>
                window.onload = function() {{
                    setTimeout(function() {{
                        window.print();
                        # window.close(); // Automatically close the new window/tab after printing
                    }}, 500);
                }};
            </script>
        </body>
        </html>
        """
        logger.info(f"Returning HTML response to trigger print dialog for instruction ID {instruction_id}.")
        return HTMLResponse(content=html_content, status_code=status.HTTP_200_OK)
    else:
        headers = {'Content-Disposition': f'inline; filename="{db_instruction.serial_number}.pdf"'}
        logger.info(f"Successfully generated and streaming PDF for instruction ID {instruction_id}.")
        return StreamingResponse(io.BytesIO(generated_pdf_bytes), media_type="application/pdf", headers=headers)
        
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
async def get_lg_record_lifecycle_history(
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
async def get_customer_configuration_for_end_user(
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
async def lookup_system_user_by_email(
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
async def get_customer_entities_for_end_user(
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
async def get_currencies_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active currencies."""
    currencies = crud_currency.get_all(db, skip=skip, limit=limit)
    return currencies

@router.get("/lg-types/", response_model=List[LgTypeOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
async def get_lg_types_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active LG types."""
    lg_types = crud_lg_type.get_all(db, skip=skip, limit=limit)
    return lg_types

@router.get("/banks/", response_model=List[BankOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
async def get_banks_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active banks."""
    banks = crud_bank.get_all(db, skip=skip, limit=limit)
    return banks

@router.get("/issuing-methods/", response_model=List[IssuingMethodOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
async def get_issuing_methods_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active issuing methods."""
    issuing_methods = crud_issuing_method.get_all(db, skip=skip, limit=limit)
    return issuing_methods

@router.get("/rules/", response_model=List[RuleOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
async def get_rules_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active rules."""
    rules = crud_rule.get_all(db, skip=skip, limit=limit)
    return rules

@router.get("/lg-statuses/", response_model=List[LgStatusOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
async def get_lg_statuses_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active LG statuses."""
    lg_statuses = crud_lg_status.get_all(db, skip=skip, limit=limit)
    return lg_statuses

@router.get("/lg-operational-statuses/", response_model=List[LgOperationalStatusOut], dependencies=[Depends(check_subscription_status)]) # ADDED dependency
async def get_lg_operational_statuses_for_end_user(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_record:create")),
    skip: int = 0,
    limit: int = 100
):
    """Retrieves a list of all active LG operational statuses."""
    lg_operational_statuses = crud_lg_operational_status.get_all(db, skip=skip, limit=limit)
    return lg_operational_statuses

@router.get("/lg-categories/", response_model=List[LGCategoryOut], dependencies=[Depends(check_subscription_status)])
async def get_lg_categories_for_end_user(
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

        # 3. Return the PDF for printing
        pdf_base64 = base64.b64encode(generated_pdf_bytes).decode('utf-8')

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Print LG Bank Reminder</title>
            <style>
                body {{ margin: 0; overflow: hidden; }}
                embed {{ width: 100vw; height: 100vh; border: none; }}
            </style>
        </head>
        <body>
            <embed src="data:application/pdf;base64,{pdf_base64}" type="application/pdf" width="100%" height="100%">
            <script>
                window.onload = function() {{
                    setTimeout(function() {{
                        window.print();
                    }}, 500);
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
async def get_action_center_lg_for_renewal(
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
async def get_action_center_instructions_undelivered(
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
async def get_action_center_instructions_awaiting_reply(
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
async def get_approved_requests_pending_print(
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
    "/users/me_dashboard_info",
    response_model=Dict[str, Any],
    dependencies=[Depends(check_subscription_status)],
    summary="Get current user's dashboard information"
)
async def get_current_user_dashboard_info(
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


@router.get(
    "/system-notifications/",
    response_model=List[SystemNotificationOut],
    dependencies=[Depends(check_subscription_status)],
    summary="Get active system notifications for the current user's customer"
)
def get_active_system_notifications(
    db: Session = Depends(get_db),
    end_user_context: TokenData = Depends(get_current_end_user_context),
):
    """
    Retrieves all active system notifications relevant to the authenticated user.
    """
    
    # CRITICAL FIX: Ensure the session reads committed data from the database.
    db.expire_all() 

    customer_id = end_user_context.customer_id
    user_id = end_user_context.user_id
    
    notifications = crud_system_notification.get_active_notifications_for_user(
        db, user_id=user_id, customer_id=customer_id
    )

    return notifications

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

    # Call the new CRUD method for display logging
    log = crud_system_notification.log_notification_display( 
        db,
        user_id=end_user_context.user_id,
        notification_id=notification_id
    )
    
    return {"status": "success", "view_count": log.view_count}


@router.post(
    "/system-notifications/{notification_id}/acknowledge", # EXISTING ENDPOINT (Dismiss action)
    # RESPONSE MODEL CHANGED TO A SIMPLE STATUS OBJECT
    response_model=dict,
    dependencies=[Depends(check_for_read_only_mode)],
    summary="Logs an acknowledgment for a system notification."
)
def acknowledge_system_notification( # FUNCTION RENAME
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

    # Use the dedicated CRUD method for explicit acknowledgment
    log = crud_system_notification.acknowledge_notification(
        db,
        user_id=end_user_context.user_id,
        notification_id=notification_id
    )
    
    # Return a simple success object
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
        # Maker-Checker flow: Create an ApprovalRequest
        try:
            lg_record_for_snapshot = crud_lg_record.get_lg_record_with_relations(db, lg_record_id, end_user_context.customer_id)
            if not lg_record_for_snapshot:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Associated LG Record not found.")
            
            lg_snapshot = crud_approval_request._get_lg_record_snapshot(lg_record_for_snapshot)
            
            approval_request_in = ApprovalRequestCreate(
                entity_type="LGRecord",
                entity_id=lg_record_id,
                action_type=action_type,
                request_details={
                    "instruction_id": instruction_id,
                    "reason": cancel_in.reason,
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
            
            # Log submission
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
                    "reason_for_cancellation": cancel_in.reason,
                },
                customer_id=end_user_context.customer_id,
                lg_record_id=lg_record_id,
            )

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
        user_id=end_user_context.user_id,
        start_date=start_date,
        end_date=end_date,
        action_types=action_type_list,
        lg_record_ids=lg_record_id_list
    )
    return history