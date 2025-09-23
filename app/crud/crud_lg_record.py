# app/crud/crud_lg_record.py
import json
import os
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Type, Tuple
from fastapi import HTTPException, status, UploadFile
from sqlalchemy import func, desc, exists, and_
from sqlalchemy.orm import Session, selectinload
import decimal
from dateutil.relativedelta import relativedelta

import pytz # Ensure pytz is imported at the top of crud_lg_record.py if not already.
EEST_TIMEZONE = pytz.timezone('Africa/Cairo') # Define or import EEST_TIMEZONE

from app.crud.crud import CRUDBase, log_action
import app.models as models
from app.models import (
    Customer, CustomerEmailSetting, CustomerEntity, InternalOwnerContact,
    LGCategory, LGCategoryCustomerEntityAssociation, LGDocument, LGInstruction,
    LGRecord, LgType, LgOperationalStatus, LgStatus, Rule,
    Template, GlobalConfiguration, CustomerConfiguration, User, ApprovalRequest
)
from app.schemas.all_schemas import (
    LGInstructionUpdate, LGInstructionRecordDelivery, LGInstructionRecordBankReply,
    InternalOwnerContactCreate, LGDocumentCreate, LGCategoryCreate, LGCategoryUpdate,
    LGInstructionCreate, LGRecordCreate, LGRecordUpdate, LGActivateNonOperativeRequest,
    LGRecordAmendRequest # New schema for amendment
)
from app.constants import (
    GlobalConfigKey, ApprovalRequestStatusEnum, ACTION_TYPE_LG_DECREASE_AMOUNT, AUDIT_ACTION_TYPE_LG_DECREASED_AMOUNT,
    ACTION_TYPE_LG_RECORD_DELIVERY, AUDIT_ACTION_TYPE_LG_INSTRUCTION_DELIVERED,
    ACTION_TYPE_LG_RECORD_BANK_REPLY, AUDIT_ACTION_TYPE_LG_BANK_REPLY_RECORDED,
    ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT, AUDIT_ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT_SENT,
    ACTION_TYPE_LG_REMINDER_TO_BANKS, AUDIT_ACTION_TYPE_LG_REMINDER_SENT_TO_BANK,
    ACTION_TYPE_LG_EXTEND, ACTION_TYPE_LG_RELEASE, ACTION_TYPE_LG_LIQUIDATE,
    ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE, ACTION_TYPE_LG_AMEND,
    ACTION_TYPE_LG_CANCEL_LAST_INSTRUCTION,
    ACTION_TYPE_LG_CHANGE_OWNER_DETAILS, ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER, ACTION_TYPE_LG_CHANGE_BULK_LG_OWNER,
    AUDIT_ACTION_TYPE_LG_BULK_REMINDER_INITIATED,
    AUDIT_ACTION_TYPE_LG_AMENDED, AUDIT_ACTION_TYPE_LG_ACTIVATED,
    LgStatusEnum, LgTypeEnum, LgOperationalStatusEnum, # Corrected to models.LgOperationalStatusEnum
    ACTION_TYPE_LG_TOGGLE_AUTO_RENEWAL, AUDIT_ACTION_TYPE_LG_AUTO_RENEWAL_TOGGLED, # Added toggle constants
    # NEW constants for renewal reminders
    ACTION_TYPE_LG_RENEWAL_REMINDER_FIRST, AUDIT_ACTION_TYPE_LG_RENEWAL_REMINDER_FIRST_SENT,
    ACTION_TYPE_LG_RENEWAL_REMINDER_SECOND, AUDIT_ACTION_TYPE_LG_RENEWAL_REMINDER_SECOND_SENT,
    ACTION_TYPE_LG_REMINDER_TO_INTERNAL_OWNER, AUDIT_ACTION_TYPE_LG_OWNER_RENEWAL_REMINDER_SENT,
    AUDIT_ACTION_TYPE_LG_OWNER_RENEWAL_REMINDER_SKIPPED_RECENTLY_SENT,
    # NEW: Import DOCUMENT_TYPE_ORIGINAL_LG
    DOCUMENT_TYPE_ORIGINAL_LG,
    # New constants for new LG record email confirmation
    ACTION_TYPE_LG_RECORDED,
    # New constants for instruction serial generation
    InstructionTypeCode, SubInstructionCode,
    UserRole # ADDED in the previous turn
)
from app.core.email_service import EmailSettings, get_global_email_settings, send_email, get_customer_email_settings
from app.core.document_generator import generate_pdf_from_html
from app.core.ai_integration import process_lg_document_with_ai, GCS_BUCKET_NAME

# --- REMOVED tenacity imports from here as retry logic is moved to crud_lg_instruction.create ---
# from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
# from sqlalchemy.exc import IntegrityError
# --- END REMOVED ---

import logging
logger = logging.getLogger(__name__)

class CRUDLGRecord(CRUDBase):
    # CRITICAL CHANGE: Accept other CRUD instances needed in its methods
    def __init__(self, model: Type[LGRecord], crud_internal_owner_contact_instance: Any, crud_lg_instruction_instance: Any, crud_lg_document_instance: Any, crud_customer_configuration_instance: Any):
        super().__init__(model)
        self.crud_internal_owner_contact_instance = crud_internal_owner_contact_instance
        self.crud_lg_instruction_instance = crud_lg_instruction_instance
        self.crud_lg_document_instance = crud_lg_document_instance
        self.crud_customer_configuration_instance = crud_customer_configuration_instance

    # --- MODIFIED: _create_instruction_with_retry method (no retry decorator here) ---
    async def _create_instruction_with_retry(self, db: Session, instruction_create_payload: dict, lg_record_id: int, instruction_type_code: InstructionTypeCode, sub_instruction_code: SubInstructionCode, user_id: int):
        """
        Helper to prepare instruction payload and call the instruction creation.
        The retry logic for unique constraint violations is now handled inside crud_lg_instruction.create.
        """
        lg_record = db.query(self.model).filter(self.model.id == lg_record_id).first() # No with_for_update here
        if not lg_record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found for instruction creation.")

        beneficiary_entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.id == lg_record.beneficiary_corporate_id).first()
        lg_category = db.query(models.LGCategory).filter(models.LGCategory.id == lg_record.lg_category_id).first()

        if not beneficiary_entity or not lg_category:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing related data for serial generation.")

        # These parameters are needed by crud_lg_instruction.create for serial generation.
        # They will be passed as separate keyword arguments, not part of the obj_in payload.
        serial_generation_kwargs = {
            'entity_code': beneficiary_entity.code,
            'lg_category_code': lg_category.code,
            'lg_sequence_number_str': str(lg_record.lg_sequence_number).zfill(4),
            'instruction_type_code_enum': instruction_type_code,
            'sub_instruction_code_enum': sub_instruction_code
        }

        # Call the instruction's create method, which will handle the retryable serial generation
        # We pass the instruction_create_payload (which forms the obj_in for LGInstructionCreate)
        # and then unpack the serial_generation_kwargs as additional keyword arguments.
        db_lg_instruction = await self.crud_lg_instruction_instance.create(
            db, # Pass the original session
            obj_in=LGInstructionCreate(**instruction_create_payload), # This creates the Pydantic model
            **serial_generation_kwargs # Unpack the serial generation parameters as kwargs
        )
        # No db.flush() or db.commit() here; the main transaction will handle it.
        return db_lg_instruction.id

    def get_by_lg_number(self, db: Session, lg_number: str) -> Optional[models.LGRecord]:
        return (
            db.query(self.model)
            .filter(
                func.lower(self.model.lg_number) == func.lower(lg_number),
                self.model.is_deleted == False,
            )
            .first()
        )

    async def create(self, db: Session, obj_in: LGRecordCreate, customer_id: int, user_id: int,
                 ai_scan_file_content: Optional[bytes] = None,
                 internal_supporting_document_file_content: Optional[bytes] = None) -> models.LGRecord:
        existing_lg = self.get_by_lg_number(db, obj_in.lg_number)
        if existing_lg:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"LG with number '{obj_in.lg_number}' already exists.",
            )

        internal_owner_contact_data = InternalOwnerContactCreate(
            email=obj_in.internal_owner_email,
            phone_number=obj_in.internal_owner_phone,
            internal_id=obj_in.internal_owner_id,
            manager_email=obj_in.manager_email,
        )
        db_internal_owner_contact = self.crud_internal_owner_contact_instance.create_or_get(
            db, obj_in=internal_owner_contact_data, customer_id=customer_id, user_id=user_id
        )

        lg_record_data = obj_in.model_dump(
            exclude_unset=True,
            exclude={
                "ai_scan_file",
                "internal_supporting_document_file",
                "internal_owner_email",
                "internal_owner_phone",
                "internal_owner_id",
                "manager_email",
            },
        )
        
        # CRITICAL FIX: Convert empty strings to None for integer fields
        # This resolves the `invalid input syntax for type integer: ""` database error.
        for field in ["communication_bank_id"]:
            if lg_record_data.get(field) == "":
                lg_record_data[field] = None

        lg_record_data["internal_owner_contact_id"] = db_internal_owner_contact.id

        # Calculate lg_period_months
        delta = obj_in.expiry_date - obj_in.issuance_date
        total_days = delta.days
        lg_period_months = max(
            3, min(12, round(total_days / 30.44 / 3) * 3
        ))

        lg_record_data["customer_id"] = customer_id
        lg_record_data["lg_period_months"] = lg_period_months

        valid_lg_status = db.query(models.LgStatus).filter(models.LgStatus.id == models.LgStatusEnum.VALID.value).first()
        if not valid_lg_status:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="'Valid' LG Status not found in master data. Please configure.",
            )
        lg_record_data["lg_status_id"] = valid_lg_status.id

        if lg_record_data.get("lg_payable_currency_id") is None:
            lg_record_data["lg_payable_currency_id"] = lg_record_data["lg_currency_id"]

        lg_type = db.query(models.LgType).filter(models.LgType.id == obj_in.lg_type_id).first()
        if not lg_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid LG Type ID provided."
            )

        applicable_rule = db.query(models.Rule).filter(models.Rule.id == obj_in.applicable_rule_id).first()
        if not applicable_rule:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Applicable Rule ID provided."
            )

        lg_category = db.query(models.LGCategory).filter(models.LGCategory.id == obj_in.lg_category_id, models.LGCategory.is_deleted == False).first()
        if not lg_category or (lg_category.customer_id is not None and lg_category.customer_id != customer_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid LG Category ID provided or category not accessible for your customer.",
            )

        # --- MODIFIED LOGIC START ---
        if lg_type.id == models.LgTypeEnum.ADVANCE_PAYMENT_GUARANTEE.value:
            if obj_in.lg_operational_status_id is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Operational Status is mandatory for 'Advance Payment Guarantee' type.",
                )
            op_status = db.query(models.LgOperationalStatus).filter(models.LgOperationalStatus.id == obj_in.lg_operational_status_id).first()
            if not op_status:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid LG Operational Status ID provided."
                )
            lg_record_data["lg_operational_status_id"] = obj_in.lg_operational_status_id
        else:
            lg_record_data["lg_operational_status_id"] = None
        # --- MODIFIED LOGIC END ---

        if lg_type.id == models.LgTypeEnum.ADVANCE_PAYMENT_GUARANTEE.value and (
            obj_in.lg_operational_status_id is not None
            and db.query(models.LgOperationalStatus).filter(models.LgOperationalStatus.id == obj_in.lg_operational_status_id).first().id
            == models.LgOperationalStatusEnum.NON_OPERATIVE.value
        ):
            if not obj_in.payment_conditions or not obj_in.payment_conditions.strip():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Payment Conditions are mandatory when LG Type is 'Advance Payment Guarantee' and Operational Status is 'Non-Operative'.",
                )
            lg_record_data["payment_conditions"] = obj_in.payment_conditions
        else:
            lg_record_data["payment_conditions"] = None

        if applicable_rule.name == "Other":
            if not obj_in.applicable_rules_text or not obj_in.applicable_rules_text.strip():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Applicable Rules Text is mandatory when Applicable Rule is 'Other'.",
                )
            lg_record_data["applicable_rules_text"] = obj_in.applicable_rules_text
        else:
            lg_record_data["applicable_rules_text"] = None

        if lg_category.extra_field_name:
            if lg_category.is_mandatory:
                if (
                    not obj_in.additional_field_values
                    or lg_category.extra_field_name not in obj_in.additional_field_values
                    or not obj_in.additional_field_values[lg_category.extra_field_name]
                ):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Custom field '{lg_category.extra_field_name}' is mandatory for the selected LG Category.",
                    )
            lg_record_data["additional_field_values"] = obj_in.additional_field_values
        else:
            lg_record_data["additional_field_values"] = None
        
        # NEW LOGIC: Handle Foreign Bank and Advising Status details
        foreign_bank = db.query(models.Bank).filter(models.Bank.name == "Foreign Bank", models.Bank.is_deleted == False).first()
        if foreign_bank and obj_in.issuing_bank_id == foreign_bank.id:
            # For a foreign bank, save the manual details
            lg_record_data["foreign_bank_name"] = obj_in.foreign_bank_name
            lg_record_data["foreign_bank_country"] = obj_in.foreign_bank_country
            lg_record_data["foreign_bank_address"] = obj_in.foreign_bank_address
            lg_record_data["foreign_bank_swift_code"] = obj_in.foreign_bank_swift_code
            
            # FIXED: Provide dummy data instead of None for required fields
            lg_record_data["issuing_bank_address"] = "Foreign Bank - See foreign_bank_address field"
            lg_record_data["issuing_bank_phone"] = "N/A - Foreign Bank"
            lg_record_data["issuing_bank_fax"] = "N/A - Foreign Bank"
            
            # Also save the new advising status and communication bank
            lg_record_data["advising_status"] = obj_in.advising_status
            lg_record_data["communication_bank_id"] = obj_in.communication_bank_id
        else:
            # For a local bank, ensure the new foreign bank and advising fields are null
            lg_record_data["foreign_bank_name"] = None
            lg_record_data["foreign_bank_country"] = None
            lg_record_data["foreign_bank_address"] = None
            lg_record_data["foreign_bank_swift_code"] = None
            lg_record_data["advising_status"] = None
            lg_record_data["communication_bank_id"] = None

        # --- NEW LOGIC: Assign lg_sequence_number ---
        # Get the highest lg_sequence_number for the beneficiary corporate (customer entity)
        # and assign the next one.
        # This assumes beneficiary_corporate_id (customer entity) is selected from user's available entities.
        last_lg_sequence = db.query(func.max(self.model.lg_sequence_number)).filter(
            self.model.beneficiary_corporate_id == obj_in.beneficiary_corporate_id,
            self.model.is_deleted == False
        ).scalar()
        next_lg_sequence = (last_lg_sequence if last_lg_sequence is not None else 0) + 1
        lg_record_data["lg_sequence_number"] = next_lg_sequence

        db_lg_record = self.model(**lg_record_data)
        db.add(db_lg_record)
        db.flush() # Flush to assign ID and lg_sequence_number for further use

        # Replaced Document Handling block:
        # Handle AI Scan File as the ORIGINAL_LG_DOCUMENT if provided
        if obj_in.ai_scan_file and ai_scan_file_content:
            # Check subscription plan here, as the actual storage happens now
            customer_obj = db.query(models.Customer).options(selectinload(models.Customer.subscription_plan)).filter(models.Customer.id == customer_id).first()
            if not customer_obj or not customer_obj.subscription_plan or not customer_obj.subscription_plan.can_image_storage:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Your subscription plan does not support document storage for original LG documents."
                )
            try:
                # We explicitly set document_type to ORIGINAL_LG_DOCUMENT for this primary file
                obj_in.ai_scan_file.document_type = DOCUMENT_TYPE_ORIGINAL_LG # Use the new constant
                db_original_lg_document = await self.crud_lg_document_instance.create_document(
                    db,
                    obj_in=obj_in.ai_scan_file, # Contains metadata (name, mime_type)
                    file_content=ai_scan_file_content, # The actual bytes from the UploadFile
                    lg_record_id=db_lg_record.id,
                    uploaded_by_user_id=user_id,
                    # No original_instruction_serial here, as this is the primary LG doc
                )
                logger.info(f"Original LG Document saved: {db_original_lg_document.file_path}")
            except Exception as e:
                logger.error(f"Failed to store original LG document for LG {db_lg_record.lg_number}: {e}", exc_info=True)
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to store original LG document: {e}")

        # Handle Internal Supporting Document if provided
        if obj_in.internal_supporting_document_file and internal_supporting_document_file_content:
            customer_obj = db.query(models.Customer).options(selectinload(models.Customer.subscription_plan)).filter(models.Customer.id == customer_id).first()
            if not customer_obj or not customer_obj.subscription_plan or not customer_obj.subscription_plan.can_image_storage:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Your subscription plan does not support document storage for internal supporting documents."
                )
            try:
                # The document_type is already set in the frontend LGDocumentCreate for this
                db_internal_supporting_document = await self.crud_lg_document_instance.create_document(
                    db,
                    obj_in=obj_in.internal_supporting_document_file, # Contains metadata (name, mime_type, document_type='INTERNAL_SUPPORTING')
                    file_content=internal_supporting_document_file_content, # The actual bytes
                    lg_record_id=db_lg_record.id,
                    uploaded_by_user_id=user_id,
                    # No original_instruction_serial here
                )
                logger.info(f"Internal Supporting Document saved: {db_internal_supporting_document.file_path}")
            except Exception as e:
                logger.error(f"Failed to store internal supporting document for LG {db_lg_record.lg_number}: {e}", exc_info=True)
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to store internal supporting document: {e}")

        customer = db.query(models.Customer).filter(models.Customer.id == customer_id).first()
        if customer:
            if customer.active_lg_count >= customer.subscription_plan.max_records:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Record limit ({customer.subscription_plan.max_records}) exceeded for this customer's subscription plan. Cannot create new LG record.",
                )
            customer.active_lg_count += 1
            db.add(customer)
            db.flush()
            db.refresh(customer)

        db.refresh(db_lg_record)

        try:
            # 1. Retrieve all email recipients
            to_emails = [db_lg_record.internal_owner_contact.email]
            cc_emails = []
            if db_lg_record.internal_owner_contact.manager_email:
                cc_emails.append(db_lg_record.internal_owner_contact.manager_email)

            if db_lg_record.lg_category and db_lg_record.lg_category.communication_list:
                cc_emails.extend(db_lg_record.lg_category.communication_list)

            # Get the common communication list from customer configuration
            common_comm_list_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
            )
            if common_comm_list_config and common_comm_list_config.get('effective_value'):
                try:
                    parsed_list = json.loads(common_comm_list_config['effective_value'])
                    if isinstance(parsed_list, list):
                        cc_emails.extend([email for email in parsed_list if isinstance(email, str)])
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON for common communication list for customer {customer_id}. Skipping.")

            # Get all corporate admins for this customer to CC them
            corporate_admins = db.query(models.User).filter(
                models.User.customer_id == customer_id,
                models.User.role == UserRole.CORPORATE_ADMIN,
                models.User.is_deleted == False
            ).all()
            for admin in corporate_admins:
                if admin.email:
                    cc_emails.append(admin.email)

            # Remove duplicates and self from CC lists
            cc_emails = list(set(cc_emails) - set(to_emails))

            # 2. Find the notification template for a new LG
            notification_template = db.query(models.Template).filter(
                models.Template.action_type == ACTION_TYPE_LG_RECORDED,
                models.Template.is_notification_template == True,
                models.Template.is_global == True, # Assuming a global template for this
                models.Template.is_deleted == False
            ).first()

            if notification_template:
                # 3. Prepare template data
                template_data = {
                    "lg_number": db_lg_record.lg_number,
                    "lg_amount": float(db_lg_record.lg_amount),
                    "lg_currency": db_lg_record.lg_currency.iso_code,
                    "issuing_bank_name": db_lg_record.issuing_bank.name,
                    "lg_beneficiary_name": db_lg_record.beneficiary_corporate.entity_name,
                    "lg_owner_email": db_lg_record.internal_owner_contact.email,
                    "internal_owner_email": db_lg_record.internal_owner_contact.email, # NEW
                    "lg_type": db_lg_record.lg_type.name,
                    "lg_category": db_lg_record.lg_category.name,
                    "customer_name": db_lg_record.customer.name,
                    "user_email": db.query(models.User.email).filter(models.User.id == user_id).scalar(),
                    "current_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "platform_name": "Grow BD Treasury Platform",
                }

                # Add formatted currency for display
                template_data['lg_amount_formatted'] = f"{db_lg_record.lg_currency.symbol} {template_data['lg_amount']:,.2f}"

                # 4. Handle attachment
                attachments = []
                lg_document = db.query(models.LGDocument).filter(
                    models.LGDocument.lg_record_id == db_lg_record.id,
                    models.LGDocument.document_type == DOCUMENT_TYPE_ORIGINAL_LG,
                    models.LGDocument.is_deleted == False
                ).first()

                if lg_document:
                    # In a real-world scenario, you would need to download the file from GCS
                    # using the `lg_document.file_path` and store it in memory.
                    # For this example, we'll simulate the file content.
                    try:
                        # Placeholder for GCS download function
                        # from app.core.ai_integration import _download_from_gcs
                        # file_content = await _download_from_gcs(lg_document.file_path)
                        file_content = b"This is a placeholder for the LG document content."
                        attachments.append(EmailAttachment(
                            filename=lg_document.file_name,
                            content=file_content,
                            mime_type=lg_document.mime_type
                        ))
                    except Exception as e:
                        logger.error(f"Failed to retrieve LG document from GCS for email attachment: {e}")

                # 5. Get email settings and send the email
                email_settings_to_use, email_method_for_log = get_customer_email_settings(db, customer_id)
                
                # Replace placeholders in subject/body templates
                subject = notification_template.subject
                body_html = notification_template.content
                for key, value in template_data.items():
                    str_value = str(value) if value is not None else ""
                    subject = subject.replace(f"{{{{{key}}}}}", str_value)
                    body_html = body_html.replace(f"{{{{{key}}}}}", str_value)

                email_sent_successfully = await send_email(
                    db=db,
                    to_emails=to_emails,
                    cc_emails=cc_emails,
                    subject_template=subject,
                    body_template=body_html,
                    template_data=template_data,
                    email_settings=email_settings_to_use,
                    attachments=attachments
                )

                # 6. Log the notification action
                if email_sent_successfully:
                    log_action(
                        db,
                        user_id=user_id,
                        action_type="NOTIFICATION_SENT",
                        entity_type="LGRecord",
                        entity_id=db_lg_record.id,
                        details={
                            "recipient": to_emails,
                            "cc_recipients": cc_emails,
                            "subject": subject,
                            "method": email_method_for_log,
                            "notification_type": "LG Recorded Confirmation"
                        },
                        customer_id=customer_id,
                        lg_record_id=db_lg_record.id,
                    )
                else:
                     log_action(
                        db,
                        user_id=user_id,
                        action_type="NOTIFICATION_FAILED",
                        entity_type="LGRecord",
                        entity_id=db_lg_record.id,
                        details={
                            "recipient": to_emails,
                            "cc_recipients": cc_emails,
                            "subject": subject,
                            "method": email_method_for_log,
                            "notification_type": "LG Recorded Confirmation"
                        },
                        customer_id=customer_id,
                        lg_record_id=db_lg_record.id,
                    )

            else:
                logger.warning(f"Notification template for 'LG_RECORDED' not found for customer {customer_id}.")
                log_action(
                    db, user_id=user_id, action_type="NOTIFICATION_SKIPPED", entity_type="LGRecord", entity_id=db_lg_record.id,
                    details={"reason": "Template for 'LG_RECORDED' notification not found."},
                    customer_id=customer_id, lg_record_id=db_lg_record.id
                )

        except Exception as e:
            logger.error(f"An error occurred while sending LG Recorded confirmation email for LG {db_lg_record.lg_number}: {e}", exc_info=True)
            db.rollback() # Rollback the email part of the transaction if it fails
            log_action(
                db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=db_lg_record.id,
                details={"reason": f"Unhandled exception during email send: {e}"},
                customer_id=customer_id, lg_record_id=db_lg_record.id
            )
        # --- END NEW CODE BLOCK: Send LG Recorded Confirmation Email ---

        log_action(
            db,
            user_id=user_id,
            action_type="CREATE",
            entity_type="LGRecord",
            entity_id=db_lg_record.id,
            details={"lg_number": db_lg_record.lg_number, "customer_id": customer_id, "lg_sequence_number": db_lg_record.lg_sequence_number},
            customer_id=customer_id,
            lg_record_id=db_lg_record.id,
        )
        return db_lg_record
        
    async def extend_lg(self, db: Session, lg_record_id: int, new_expiry_date: date, user_id: int, notes: Optional[str] = None) -> Tuple[models.LGRecord, int, str]: # NEW: Add notes parameter
        db_lg_record = self.get_lg_record_with_relations(db, lg_record_id, None)
        recipient_name = "To Whom It May Concern"
        recipient_address = "N/A"
        if not db_lg_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or is deleted."
            )

        if db_lg_record.lg_status_id != models.LgStatusEnum.VALID.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only LGs with status 'Valid' can be extended. Current status: {db_lg_record.lg_status.name}.",
            )

        if new_expiry_date <= db_lg_record.expiry_date.date():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"New expiry date ({new_expiry_date}) must be strictly after the current expiry date ({db_lg_record.expiry_date.date()}).",
            )
        old_expiry_date_for_log = db_lg_record.expiry_date.date().isoformat()

        issue_date_from_db = db_lg_record.issuance_date.date()
        delta_days = (new_expiry_date - issue_date_from_db).days
        new_lg_period_months = max(1, round(delta_days / 30.44))

        update_dict = {
            "expiry_date": datetime.combine(new_expiry_date, datetime.min.time()),
        }
        updated_lg_record = super().update(db, db_lg_record, obj_in=update_dict)

        instruction_template = db.query(models.Template).filter(models.Template.action_type == "LG_EXTENSION", models.Template.is_global == True, models.Template.is_notification_template == False, models.Template.is_deleted == False).first()

        if not instruction_template:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="LG Extension Instruction template not found. Please ensure a global 'LG_EXTENSION' template (non-notification) exists."
            )
        
        customer = db.query(models.Customer).filter(models.Customer.id == db_lg_record.customer_id).first()
        entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.id == db_lg_record.beneficiary_corporate_id).first()
        
        if not customer or not entity:
             raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Customer or entity record not found for LG.")

        customer_address = entity.address if entity.address else customer.address
        customer_contact_email = entity.contact_email if entity.contact_email else customer.contact_email
        customer_name = customer.name if customer else "N/A"
        current_date_str = datetime.now().strftime("%Y-%m-%d")

        instruction_details = {
            "old_expiry_date": old_expiry_date_for_log,
            "new_expiry_date": new_expiry_date.isoformat(),
            "lg_number": db_lg_record.lg_number,
            "lg_amount": float(
                db_lg_record.lg_amount
            ),
            "lg_currency": db_lg_record.lg_currency.iso_code,
            "issuing_bank_name": db_lg_record.issuing_bank.name,
            "internal_owner_email": db_lg_record.internal_owner_contact.email,
            "internal_owner_phone": db_lg_record.internal_owner_contact.phone_number,
            "internal_owner_id": db_lg_record.internal_owner_contact.internal_id,
            "manager_email": db_lg_record.internal_owner_contact.manager_email,
            "lg_issuer_name": updated_lg_record.issuer_name,
            "lg_beneficiary_name": updated_lg_record.beneficiary_corporate.entity_name,
            "customer_name": customer_name,
            "customer_address": customer_address,
            "customer_contact_email": customer_contact_email,
            "current_date": current_date_str,
            "platform_name": "Grow BD Treasury Management Platform",
            "recipient_name": recipient_name,
            "recipient_address": recipient_address,
        }
        
        notes_html = ""
        if notes:
            notes_html = f"""
            <h3>Additional Notes</h3>
            <p>{notes}</p>
            """
        instruction_details["notes_section"] = notes_html # NEW: Use the notes_section key

        instruction_details["lg_amount_formatted"] = f"{db_lg_record.lg_currency.symbol} {float(db_lg_record.lg_amount):,.2f}"

        generated_instruction_html = instruction_template.content
        for key, value in instruction_details.items():
            str_value = str(value) if value is not None else ""
            generated_instruction_html = generated_instruction_html.replace(f"{{{{{key}}}}}", str_value)

        try:
            instruction_create_payload_for_schema = {
                "lg_record_id": db_lg_record.id,
                "instruction_type": "EXTENSION",
                "template_id": instruction_template.id,
                "status": "Instruction Issued",
                "details": instruction_details,
                "maker_user_id": user_id,
                "serial_number": None,
            }

            instruction_type_code_enum = InstructionTypeCode.EXT
            sub_instruction_code_enum = SubInstructionCode.ORIGINAL

            serial_generation_params = {
                'lg_record_id': db_lg_record.id,
                'instruction_type_code': instruction_type_code_enum,
                'sub_instruction_code': sub_instruction_code_enum,
                'user_id': user_id
            }

            instruction_id = await self._create_instruction_with_retry(
                db,
                instruction_create_payload=instruction_create_payload_for_schema,
                **serial_generation_params
            )
            
            db_lg_instruction = db.query(models.LGInstruction).filter(models.LGInstruction.id == instruction_id).first()
            if not db_lg_instruction:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve newly created instruction after creation.")

            filename_for_pdf = f"lg_extension_{db_lg_record.lg_number}_instruction_{db_lg_instruction.serial_number}"
            generated_pdf_bytes = await generate_pdf_from_html(generated_instruction_html, filename_for_pdf)
            
            generated_content_path = f"gs://your-gcs-bucket/generated_instructions/{filename_for_pdf}.pdf"
            
            db_lg_instruction_in_current_session = db.merge(db_lg_instruction)
            db_lg_instruction_in_current_session.generated_content_path = generated_content_path
            db.add(db_lg_instruction_in_current_session)
            db.flush()

            db.refresh(updated_lg_record)
            db.refresh(db_lg_instruction_in_current_session)

        except Exception as e:
            db.rollback()
            logger.exception(f"An unexpected error occurred during LG extension for LG {db_lg_record.lg_number}: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during LG extension: {e}")

        customer_with_email_settings = db.query(models.Customer).options(selectinload(models.Customer.customer_email_settings)).filter(models.Customer.id == db_lg_record.customer_id).first()

        email_settings_to_use: EmailSettings
        email_method_for_log: str
        try:
            email_settings_to_use, email_method_for_log = get_customer_email_settings(db, db_lg_record.customer_id)
        except Exception as e:
            email_settings_to_use = get_global_email_settings()
            email_method_for_log = "global_fallback_due_to_error"
            logger.warning(f"Failed to retrieve customer-specific email settings for customer ID {db_lg_record.customer_id}: {e}. Falling back to global settings.")

        email_to_send_to = [db_lg_record.internal_owner_contact.email]
        cc_emails = []
        if db_lg_record.internal_owner_contact.manager_email:
            cc_emails.append(db_lg_record.internal_owner_contact.manager_email)
        if db_lg_record.lg_category and db_lg_record.lg_category.communication_list:
            cc_emails.extend(db_lg_record.lg_category.communication_list)

        common_comm_list_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
            db, db_lg_record.customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
        )
        if common_comm_list_config and common_comm_list_config.get('effective_value'):
            try:
                parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                if isinstance(parsed_common_list, list) and all(
                    isinstance(e, str) and "@" in e for e in parsed_common_list
                ):
                    cc_emails.extend(parsed_common_list)
            except json.JSONDecodeError:
                logger.warning(
                    f"COMMON_COMMUNICATION_LIST for customer {db_lg_record.customer_id} is not a valid JSON list of emails. Skipping."
                )
        cc_emails = list(set(cc_emails))

        notification_template = db.query(models.Template).filter(models.Template.action_type == "LG_EXTENSION", models.Template.is_global == True, models.Template.is_notification_template == True, models.Template.is_deleted == False).first()

        if not notification_template:
            log_action(
                db,
                user_id=user_id,
                action_type="NOTIFICATION_FAILED",
                entity_type="LGRecord",
                entity_id=updated_lg_record.id,
                details={
                    "recipient": email_to_send_to,
                    "subject": "N/A",
                    "reason": "LG_EXTENSION notification template (is_notification_template=True) not found",
                    "method": "none",
                },
                customer_id=updated_lg_record.customer_id,
                lg_record_id=updated_lg_record.id,
            )
            logger.error(f"LG extended (ID: {updated_lg_record.id}), but failed to send email notification due to missing template.")
        else:
            template_data = {
                "lg_number": updated_lg_record.lg_number,
                "old_expiry_date": instruction_details["old_expiry_date"],
                "new_expiry_date": instruction_details["new_expiry_date"],
                "lg_amount": float(db_lg_record.lg_amount),
                "lg_currency": db_lg_record.lg_currency.iso_code,
                "issuing_bank_name": updated_lg_record.issuing_bank.name,
                "internal_owner_email": db_lg_record.internal_owner_contact.email,
                "internal_owner_phone": db_lg_record.internal_owner_contact.phone_number,
                "internal_owner_id": db_lg_record.internal_owner_contact.internal_id,
                "manager_email": db_lg_record.internal_owner_contact.manager_email,
                "lg_issuer_name": updated_lg_record.issuer_name,
                "lg_beneficiary_name": updated_lg_record.beneficiary_corporate.entity_name,
                "customer_name": customer.name if customer else "N/A",
                "platform_name": "Grow BD Treasury Management Platform",
                "current_date": current_date_str,
                "action_type": "LG Extension",
                "instruction_serial": db_lg_instruction.serial_number,
                "issue_date": db_lg_record.issuance_date.date().isoformat(),
                "lg_serial_number": updated_lg_record.lg_number,
                "notes": notes, # NEW: Add notes to the email template data
             }

            template_data["lg_amount_formatted"] = f"{db_lg_record.lg_currency.symbol} {template_data['lg_amount']:,.2f}"
            email_subject = notification_template.subject if notification_template.subject else f"{{action_type}} LG #{{lg_number}} - Instruction #{{instruction_serial}}"
            email_body_html = notification_template.content
            for key, value in template_data.items():
                str_value = str(value) if value is not None else ""
                email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
                email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

            email_sent_successfully = await send_email(
                db=db,
                to_emails=email_to_send_to,
                cc_emails=cc_emails,
                subject_template=email_subject,
                body_template=email_body_html,
                template_data=template_data,
                email_settings=email_settings_to_use,
                sender_name=customer.name
            )

            if not email_sent_successfully:
                log_action(
                    db,
                    user_id=user_id,
                    action_type="NOTIFICATION_FAILED",
                    entity_type="LGRecord",
                    entity_id=updated_lg_record.id,
                    details={
                        "recipient": email_to_send_to,
                        "cc_recipients": cc_emails,
                        "subject": email_subject,
                        "reason": "Email service failed to send notification",
                        "method": email_method_for_log,
                    },
                    customer_id=updated_lg_record.customer_id,
                    lg_record_id=updated_lg_record.id,
                )
                logger.error(f"LG extended (ID: {updated_lg_record.id}), but failed to send email notification.")
            else:
                log_action(
                    db,
                    user_id=user_id,
                    action_type="NOTIFICATION_SENT",
                    entity_type="LGRecord",
                    entity_id=updated_lg_record.id,
                    details={
                        "recipient": email_to_send_to,
                        "cc_recipients": cc_emails,
                        "subject": email_subject,
                        "method": email_method_for_log,
                    },
                    customer_id=updated_lg_record.customer_id,
                    lg_record_id=updated_lg_record.id,
                )

        log_action(
            db,
            user_id=user_id,
            action_type="LG_EXTENDED",
            entity_type="LGRecord",
            entity_id=updated_lg_record.id,
            details={
                "lg_number": updated_lg_record.lg_number,
                "old_expiry_date": instruction_details["old_expiry_date"],
                "new_expiry_date": instruction_details["new_expiry_date"],
                "instruction_serial": db_lg_instruction.serial_number,
                "generated_instruction_id": db_lg_instruction.id,
                "notes": notes, # NEW: Add notes to the audit log
            },
            customer_id=updated_lg_record.customer_id,
            lg_record_id=updated_lg_record.id,
        )

        db.flush()
        db.refresh(updated_lg_record)
        return updated_lg_record, db_lg_instruction.id, generated_instruction_html
  
    async def release_lg(self, db: Session, lg_record: models.LGRecord, user_id: int, approval_request_id: Optional[int], supporting_document_id: Optional[int] = None, notes: Optional[str] = None) -> Tuple[models.LGRecord, int]: # NEW: Add notes parameter
        """
        Releases an LG record. Updates status to "Released", issues a bank letter,
        notifies stakeholders, and marks it as un-actionable.
        user_id here refers to the actual actor (maker if direct, checker if approved).
        """

        instruction_maker_user_id = user_id
        recipient_name = "To Whom It May Concern"
        recipient_address = "N/A"
        if approval_request_id:
            approval_request = db.query(models.ApprovalRequest).filter(models.ApprovalRequest.id == approval_request_id).first()
            if approval_request:
                instruction_maker_user_id = approval_request.maker_user_id
            else:
                logger.warning(f"ApprovalRequest with ID {approval_request_id} not found when creating LGInstruction for release. Using checker_user_id as maker for instruction.")

        if lg_record.lg_status_id in [models.LgStatusEnum.RELEASED.value, models.LgStatusEnum.LIQUIDATED.value, models.LgStatusEnum.EXPIRED.value]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"LG with status '{lg_record.lg_status.name}' cannot be released.",
            )
        if lg_record.lg_status_id != models.LgStatusEnum.VALID.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only LGs with status 'Valid' can be released. Current status: {lg_record.lg_status.name}.",
            )


        released_status = db.query(models.LgStatus).filter(models.LgStatus.id == models.LgStatusEnum.RELEASED.value).first()
        if not released_status:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="System misconfiguration: 'Released' status not found.")

        lg_record.lg_status_id = released_status.id
        db.add(lg_record)
        db.flush()
        logger.debug(f"DEBUG: LG record {lg_record.id} status flushed to {lg_record.lg_status.name}.")


        instruction_template = db.query(models.Template).filter(models.Template.action_type == "LG_RELEASE", models.Template.is_global == True, models.Template.is_notification_template == False, models.Template.is_deleted == False).first()
        if not instruction_template:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="LG Release Instruction template not found. Please ensure a global 'LG_RELEASE' template (non-notification) exists.")
        
        customer = db.query(models.Customer).filter(models.Customer.id == lg_record.customer_id).first()
        entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.id == lg_record.beneficiary_corporate_id).first()

        if not customer or not entity:
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Customer or entity record not found for LG.")

        customer_address = entity.address if entity.address else customer.address
        customer_contact_email = entity.contact_email if entity.contact_email else customer.contact_email

        total_original_documents = db.query(models.LGDocument).filter(
            models.LGDocument.lg_record_id == lg_record.id,
            models.LGDocument.document_type == "ORIGINAL_BANK_DOCUMENT",
            models.LGDocument.is_deleted == False
        ).count()
        pending_replies_count = db.query(models.LGInstruction).filter(
            models.LGInstruction.lg_record_id == lg_record.id,
            models.LGInstruction.status.in_(["Instruction Issued", "Instruction Delivered"]),
            models.LGInstruction.is_deleted == False
        ).count()

        instruction_wording_documents = f"documents (Total: {total_original_documents})" if total_original_documents > 0 else "no documents"
        instruction_wording_replies = f"and there are {pending_replies_count} pending replies." if pending_replies_count > 0 else "and no pending replies."


        instruction_details = {
            "lg_number": lg_record.lg_number,
            "lg_amount": float(lg_record.lg_amount),
            "lg_currency": lg_record.lg_currency.iso_code,
            "issuing_bank_name": lg_record.issuing_bank.name,
            "lg_issuer_name": lg_record.issuer_name,
            "lg_beneficiary_name": lg_record.beneficiary_corporate.entity_name,
            "current_date": datetime.now().strftime("%Y-%m-%d"),
            "customer_name": lg_record.customer.name,
            "customer_address": customer_address,
            "customer_contact_email": customer_contact_email,
            "internal_owner_email": lg_record.internal_owner_contact.email,
            "instruction_wording_documents": instruction_wording_documents,
            "instruction_wording_replies": instruction_wording_replies,
            "lg_serial_number": lg_record.lg_number,
            "recipient_name": recipient_name,
            "recipient_address": recipient_address,
        }
        
        # NEW LOGIC: Add a notes_section with HTML to be replaced in the template
        notes_html = ""
        if notes:
            notes_html = f"""
            <h3>Additional Notes</h3>
            <p>{notes}</p>
            """
        instruction_details["notes_section"] = notes_html # NEW: Use this placeholder key
        
        instruction_details["lg_amount_formatted"] = f"{lg_record.lg_currency.symbol} {float(lg_record.lg_amount):,.2f}"
        generated_instruction_html = instruction_template.content
        for key, value in instruction_details.items():
            str_value = str(value) if value is not None else ""
            generated_instruction_html = generated_instruction_html.replace(f"{{{{{key}}}}}", str_value)

        try:
            instruction_create_payload_for_schema = {
                "lg_record_id": lg_record.id,
                "instruction_type": "RELEASE",
                "template_id": instruction_template.id,
                "status": "Instruction Issued",
                "details": instruction_details,
                "maker_user_id": instruction_maker_user_id,
                "checker_user_id": user_id if approval_request_id else None,
                "approval_request_id": approval_request_id,
                "serial_number": None,
            }

            instruction_type_code_enum = InstructionTypeCode.REL
            sub_instruction_code_enum = SubInstructionCode.ORIGINAL

            serial_generation_params = {
                'lg_record_id': lg_record.id,
                'instruction_type_code': instruction_type_code_enum,
                'sub_instruction_code': sub_instruction_code_enum,
                'user_id': user_id
            }

            instruction_id = await self._create_instruction_with_retry(
                db,
                instruction_create_payload=instruction_create_payload_for_schema,
                **serial_generation_params
            )
            
            db_lg_instruction = db.query(models.LGInstruction).filter(models.LGInstruction.id == instruction_id).first()
            if not db_lg_instruction:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve newly created instruction after creation.")

            filename_for_pdf = f"lg_release_{lg_record.lg_number}_instruction_{db_lg_instruction.serial_number}"
            generated_pdf_bytes = await generate_pdf_from_html(generated_instruction_html, filename_for_pdf)
            
            generated_content_path = f"gs://your-gcs-bucket/generated_instructions/{filename_for_pdf}.pdf"
            
            db_lg_instruction_in_current_session = db.merge(db_lg_instruction)
            db_lg_instruction_in_current_session.generated_content_path = generated_content_path
            db.add(db_lg_instruction_in_current_session)
            db.flush()

            if supporting_document_id:
                db_document = db.query(models.LGDocument).filter(models.LGDocument.id == supporting_document_id, models.LGDocument.is_deleted == False).first()
                if db_document and not db_document.lg_instruction_id:
                    db_document.lg_instruction_id = db_lg_instruction_in_current_session.id
                    db.add(db_document)
                    db.flush()
                    logger.debug(f"Successfully linked supporting document ID {supporting_document_id} to new instruction ID {db_lg_instruction_in_current_session.id}.")


            db.refresh(lg_record)
            db.refresh(db_lg_instruction_in_current_session)

        except Exception as e:
            db.rollback()
            logger.exception(f"An unexpected error occurred during LG release for LG {lg_record.lg_number}: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during LG release: {e}")

        if approval_request_id is None:
            email_settings_to_use: EmailSettings
            email_method_for_log: str
            try:
                email_settings_to_use, email_method_for_log = get_customer_email_settings(db, lg_record.customer_id)
            except Exception as e:
                email_settings_to_use = get_global_email_settings()
                email_method_for_log = "global_fallback_due_to_error"
                logger.warning(f"Failed to retrieve customer-specific email settings for customer ID {lg_record.customer_id}: {e}. Falling back to global settings.")

            email_to_send_to = [lg_record.internal_owner_contact.email]
            cc_emails = []
            if lg_record.internal_owner_contact.manager_email:
                cc_emails.append(lg_record.internal_owner_contact.manager_email)
            if lg_record.lg_category and lg_record.lg_category.communication_list:
                cc_emails.extend(lg_record.lg_category.communication_list)

            common_comm_list_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, lg_record.customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
            )
            if common_comm_list_config and common_comm_list_config.get('effective_value'):
                try:
                    parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                    if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                        cc_emails.extend(parsed_common_list)
                except json.JSONDecodeError:
                    logger.warning(f"COMMON_COMMUNICATION_LIST for customer {lg_record.customer_id} is not a valid JSON list of emails. Skipping.")
            cc_emails = list(set(cc_emails))

            notification_template = db.query(models.Template).filter(models.Template.action_type == "LG_RELEASE", models.Template.is_global == True, models.Template.is_notification_template == True, models.Template.is_deleted == False).first()

            if not notification_template:
                log_action(
                    db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=lg_record.id,
                    details={"recipient": email_to_send_to, "subject": "N/A", "reason": "LG_RELEASE notification template (is_notification_template=True) not found", "method": "none"},
                    customer_id=lg_record.customer_id, lg_record_id=lg_record.id,
                )
                logger.error(f"LG released (ID: {lg_record.id}), but failed to send email notification due to missing template.")
            else:
                template_data = {
                    "lg_number": lg_record.lg_number,
                    "lg_amount": f"{lg_record.lg_currency.symbol} {float(lg_record.lg_amount):,.2f}",
                    "lg_currency": lg_record.lg_currency.iso_code,
                    "issuing_bank_name": lg_record.issuing_bank.name,
                    "lg_beneficiary_name": lg_record.beneficiary_corporate.entity_name,
                    "lg_issuer_name": lg_record.issuer_name,
                    "current_date": datetime.now().strftime("%Y-%m-%d"),
                    "customer_name": lg_record.customer.name,
                    "action_type": "LG Release",
                    "instruction_serial": db_lg_instruction.serial_number,
                    "internal_owner_email": lg_record.internal_owner_contact.email,
                    "total_original_documents": total_original_documents,
                    "pending_replies_count": pending_replies_count,
                    "notes": notes, # NEW: Add notes to the email template data
                }
                email_subject = notification_template.subject if notification_template.subject else f"{{action_type}} LG #{{lg_number}} - Instruction #{{instruction_serial}}"
                email_body_html = notification_template.content
                for key, value in template_data.items():
                    str_value = str(value) if value is not None else ""
                    email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
                    email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

                email_sent_successfully = await send_email(
                    db=db,
                    to_emails=email_to_send_to,
                    cc_emails=cc_emails,
                    subject_template=email_subject,
                    body_template=email_body_html,
                    template_data=template_data,
                    email_settings=email_settings_to_use,
                    sender_name=lg_record.customer.name
                )
                if not email_sent_successfully:
                    log_action(
                        db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=lg_record.id,
                        details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "reason": "Email service failed to send notification", "method": email_method_for_log},
                        customer_id=lg_record.customer_id, lg_record_id=lg_record.id,
                    )
                    logger.error(f"LG released (ID: {lg_record.id}), but failed to send email notification.")
                else:
                    log_action(
                        db, user_id=user_id, action_type="NOTIFICATION_SENT", entity_type="LGRecord", entity_id=lg_record.id,
                        details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "method": email_method_for_log},
                        customer_id=lg_record.customer_id, lg_record_id=lg_record.id,
                    )
            logger.debug("DEBUG: Email notification logic completed.")

        try:
            log_action(
                db, user_id, "LG_RELEASED", "LGRecord", lg_record.id,
                {"lg_number": lg_record.lg_number, "status": "Released", "instruction_id": db_lg_instruction.id, "approved_by_user_id": user_id, "notes": notes}, # NEW: Add notes to the audit log
                lg_record.customer_id, lg_record.id
            )
            logger.debug(f"DEBUG: Successfully logged LG_RELEASED action for LG ID: {lg_record.id}")

            db.refresh(lg_record)
            logger.debug(f"DEBUG: Successfully refreshed LG record for LG ID: {lg_record.id}")

            return lg_record, db_lg_instruction.id


        except Exception as e:
            logger.exception(f"ERROR: An unexpected error occurred during final logging or refresh for LG Release (LG ID: {lg_record.id}). Full traceback:")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected internal error occurred after LG release. Please check server logs for details."
            )

    async def liquidate_lg(self, db: Session, lg_record: LGRecord, liquidation_type: str, new_amount: Optional[float], user_id: int, approval_request_id: Optional[int], supporting_document_id: Optional[int] = None, notes: Optional[str] = None) -> Tuple[models.LGRecord, int]:
        """
        Liquidates an LG record (full or partial). Updates status, adjusts amount,
        issues a bank letter, and notifies stakeholders.
        user_id here refers to the actual actor (maker if direct, checker if approved).
        """

        instruction_maker_user_id = user_id
        recipient_name = "To Whom It May Concern"
        recipient_address = "N/A"
        if approval_request_id:
            approval_request = db.query(models.ApprovalRequest).filter(models.ApprovalRequest.id == approval_request_id).first()
            if approval_request:
                instruction_maker_user_id = approval_request.maker_user_id
            else:
                logger.warning(f"ApprovalRequest with ID {approval_request_id} not found when creating LGInstruction for liquidation. Using checker_user_id as maker for instruction.")

        if lg_record.lg_status_id in [models.LgStatusEnum.RELEASED.value, models.LgStatusEnum.LIQUIDATED.value, models.LgStatusEnum.EXPIRED.value]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"LG with status '{lg_record.lg_status.name}' cannot be liquidated.",
            )
        if lg_record.lg_status_id != models.LgStatusEnum.VALID.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only LGs with status 'Valid' can be liquidated. Current status: {lg_record.lg_status.name}.",
            )

        liquidated_status = db.query(models.LgStatus).filter(models.LgStatus.id == models.LgStatusEnum.LIQUIDATED.value).first()
        if not liquidated_status:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="System misconfiguration: 'Liquidated' status not found.")

        original_amount = lg_record.lg_amount
        log_details = {"liquidation_type": liquidation_type}

        if liquidation_type == "full":
            lg_record.lg_status_id = liquidated_status.id
            lg_record.lg_amount = decimal.Decimal('0.0')
            log_details["status"] = "Liquidated"
            log_details["old_amount"] = float(original_amount)
            log_details["new_amount"] = 0.0

        elif liquidation_type == "partial":
            if new_amount is None or not (0 < new_amount < original_amount):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Partial liquidation amount is invalid. Must be greater than 0 and less than current LG amount.")

            lg_record.lg_amount = decimal.Decimal(str(new_amount))
            log_details["status"] = "Valid"
            log_details["old_amount"] = float(original_amount)
            log_details["new_amount"] = float(new_amount)
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid liquidation_type. Must be 'full' or 'partial'.")

        db.add(lg_record)
        db.flush()

        instruction_template = db.query(models.Template).filter(models.Template.action_type == "LG_LIQUIDATE", models.Template.is_global == True, models.Template.is_notification_template == False, models.Template.is_deleted == False).first()
        if not instruction_template:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="LG Liquidation Instruction template not found. Please ensure a global 'LG_LIQUIDATE' template (non-notification) exists.")

        customer = db.query(models.Customer).filter(models.Customer.id == lg_record.customer_id).first()
        entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.id == lg_record.beneficiary_corporate_id).first()

        if not customer or not entity:
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Customer or entity record not found for LG.")

        customer_address = entity.address if entity.address else customer.address
        customer_contact_email = entity.contact_email if entity.contact_email else customer.contact_email

        total_original_documents = db.query(models.LGDocument).filter(
            models.LGDocument.lg_record_id == lg_record.id,
            models.LGDocument.document_type == "ORIGINAL_BANK_DOCUMENT",
            models.LGDocument.is_deleted == False
        ).count()
        pending_replies_count = db.query(models.LGInstruction).filter(
            models.LGInstruction.lg_record_id == lg_record.id,
            models.LGInstruction.status.in_(["Instruction Issued", "Instruction Delivered"]),
            models.LGInstruction.is_deleted == False
        ).count()

        instruction_wording_documents = f"documents (Total: {total_original_documents})" if total_original_documents > 0 else "no documents"
        instruction_wording_replies = f"and there are {pending_replies_count} pending replies." if pending_replies_count > 0 else "and no pending replies."

        instruction_details = {
            "lg_number": lg_record.lg_number,
            "liquidation_type": liquidation_type.capitalize(),
            "original_lg_amount": float(original_amount),
            "new_lg_amount": float(lg_record.lg_amount),
            "lg_currency": lg_record.lg_currency.iso_code,
            "issuing_bank_name": lg_record.issuing_bank.name,
            "lg_beneficiary_name": lg_record.beneficiary_corporate.entity_name,
            "lg_issuer_name": lg_record.issuer_name,
            "current_date": datetime.now().strftime("%Y-%m-%d"),
            "customer_name": lg_record.customer.name,
            "customer_address": customer_address,
            "customer_contact_email": customer_contact_email,
            "internal_owner_email": lg_record.internal_owner_contact.email,
            "instruction_wording_documents": instruction_wording_documents,
            "instruction_wording_replies": instruction_wording_replies,
            "lg_serial_number": lg_record.lg_number,
            "recipient_name": recipient_name,
            "recipient_address": recipient_address,
        }

        notes_html = ""
        if notes:
            notes_html = f"""
            <h3>Additional Notes</h3>
            <p>{notes}</p>
            """
        instruction_details["notes_section"] = notes_html
        
        # --- NEW LOGGING FOR DEBUGGING ---
        logger.info(f"DEBUG: Liquidate LG function received 'notes': '{notes}'")
        logger.info(f"DEBUG: Generated HTML snippet for notes_section: '{notes_html}'")
        # --- END NEW LOGGING ---

        instruction_details["original_lg_amount_formatted"] = f"{lg_record.lg_currency.symbol} {float(original_amount):,.2f}"
        instruction_details["new_lg_amount_formatted"] = f"{lg_record.lg_currency.symbol} {float(lg_record.lg_amount):,.2f}"
        
        generated_instruction_html = instruction_template.content
        for key, value in instruction_details.items():
            str_value = str(value) if value is not None else ""
            generated_instruction_html = generated_instruction_html.replace(f"{{{{{key}}}}}", str_value)

        if "notes_section" in generated_instruction_html:
            logger.error("DEBUG: notes_section placeholder was NOT replaced.")
        else:
            logger.info("DEBUG: notes_section placeholder WAS successfully replaced.")

        try:
            instruction_create_payload_for_schema = {
                "lg_record_id": lg_record.id,
                "instruction_type": "LIQUIDATION",
                "template_id": instruction_template.id,
                "status": "Instruction Issued",
                "details": instruction_details,
                "maker_user_id": instruction_maker_user_id,
                "checker_user_id": user_id if approval_request_id else None,
                "approval_request_id": approval_request_id,
                "serial_number": None,
            }

            instruction_type_code_enum = InstructionTypeCode.LIQ
            sub_instruction_code_enum = SubInstructionCode.ORIGINAL

            serial_generation_params = {
                'lg_record_id': lg_record.id,
                'instruction_type_code': instruction_type_code_enum,
                'sub_instruction_code': sub_instruction_code_enum,
                'user_id': user_id
            }

            instruction_id = await self._create_instruction_with_retry(
                db,
                instruction_create_payload=instruction_create_payload_for_schema,
                **serial_generation_params
            )
            
            db_lg_instruction = db.query(models.LGInstruction).filter(models.LGInstruction.id == instruction_id).first()
            if not db_lg_instruction:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve newly created instruction after creation.")

            filename_for_pdf = f"lg_liquidation_{lg_record.lg_number}_instruction_{db_lg_instruction.serial_number}"
            generated_pdf_bytes = await generate_pdf_from_html(generated_instruction_html, filename_for_pdf)
            
            generated_content_path = f"gs://your-gcs-bucket/generated_instructions/{filename_for_pdf}.pdf"
            
            db_lg_instruction_in_current_session = db.merge(db_lg_instruction)
            db_lg_instruction_in_current_session.generated_content_path = generated_content_path
            db.add(db_lg_instruction_in_current_session)
            db.flush()

            if supporting_document_id:
                db_document = db.query(models.LGDocument).filter(models.LGDocument.id == supporting_document_id, models.LGDocument.is_deleted == False).first()
                if db_document and not db_document.lg_instruction_id:
                    db_document.lg_instruction_id = db_lg_instruction_in_current_session.id
                    db.add(db_document)
                    db.flush()
                    logger.debug(f"Successfully linked supporting document ID {supporting_document_id} to new instruction ID {db_lg_instruction_in_current_session.id}.")

            db.refresh(lg_record)
            db.refresh(db_lg_instruction_in_current_session)

        except Exception as e:
            db.rollback()
            logger.exception(f"An unexpected error occurred during LG liquidation for LG {lg_record.lg_number}: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during LG liquidation: {e}")

        log_action(
            db, user_id, f"LG_LIQUIDATED_{liquidation_type.upper()}", "LGRecord", lg_record.id,
            {**log_details, "instruction_id": db_lg_instruction.id, "approved_by_user_id": user_id},
            lg_record.customer_id, lg_record.id
        )

        if approval_request_id is None:
            email_settings_to_use: EmailSettings
            email_method_for_log: str
            try:
                email_settings_to_use, email_method_for_log = get_customer_email_settings(db, lg_record.customer_id)
            except Exception as e:
                email_settings_to_use = get_global_email_settings()
                email_method_for_log = "global_fallback_due_to_error"
                logger.warning(f"Failed to retrieve customer-specific email settings for customer ID {lg_record.customer_id}: {e}. Falling back to global settings.")

            email_to_send_to = [lg_record.internal_owner_contact.email]
            cc_emails = []
            if lg_record.internal_owner_contact.manager_email:
                cc_emails.append(lg_record.internal_owner_contact.manager_email)
            if lg_record.lg_category and lg_record.lg_category.communication_list:
                cc_emails.extend(lg_record.lg_category.communication_list)

            common_comm_list_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, lg_record.customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
            )
            if common_comm_list_config and common_comm_list_config.get('effective_value'):
                try:
                    parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                    if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                        cc_emails.extend(parsed_common_list)
                except json.JSONDecodeError:
                    logger.warning(f"COMMON_COMMUNICATION_LIST for customer {lg_record.customer_id} is not a valid JSON list of emails. Skipping.")
            cc_emails = list(set(cc_emails))

            notification_template = db.query(models.Template).filter(models.Template.action_type == "LG_LIQUIDATE", models.Template.is_global == True, models.Template.is_notification_template == True, models.Template.is_deleted == False).first()

            if not notification_template:
                log_action(
                    db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=lg_record.id,
                    details={"recipient": email_to_send_to, "subject": "N/A", "reason": "LG_LIQUIDATE notification template (is_notification_template=True) not found", "method": "none"},
                    customer_id=lg_record.customer_id, lg_record_id=lg_record.id,
                )
                logger.error(f"LG liquidated (ID: {lg_record.id}), but failed to send email notification.")
            else:
                template_data = {
                    "lg_number": lg_record.lg_number,
                    "liquidation_type": liquidation_type.capitalize(),
                    "original_lg_amount": float(original_amount),
                    "new_lg_amount": float(lg_record.lg_amount),
                    "lg_currency": lg_record.lg_currency.iso_code,
                    "issuing_bank_name": lg_record.issuing_bank.name,
                    "lg_issuer_name": lg_record.issuer_name,
                    "lg_beneficiary_name": lg_record.beneficiary_corporate.entity_name,
                    "current_date": datetime.now().strftime("%Y-%m-%d"),
                    "customer_name": lg_record.customer.name,
                    "action_type": f"LG Liquidation ({liquidation_type.capitalize()})",
                    "instruction_serial": db_lg_instruction.serial_number,
                    "internal_owner_email": lg_record.internal_owner_contact.email,
                    "total_original_documents": total_original_documents,
                    "pending_replies_count": pending_replies_count,
                    "notes": notes,
                }
                email_subject = notification_template.subject if notification_template.subject else f"{{action_type}} LG #{{lg_number}} - Instruction #{{instruction_serial}}"
                email_body_html = notification_template.content
                for key, value in template_data.items():
                    str_value = str(value) if value is not None else ""
                    email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
                    email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

                email_sent_successfully = await send_email(
                    db=db,
                    to_emails=email_to_send_to,
                    cc_emails=cc_emails,
                    subject_template=email_subject,
                    body_template=email_body_html,
                    template_data=template_data,
                    email_settings=email_settings_to_use,
                    sender_name=lg_record.customer.name
                )
                if not email_sent_successfully:
                    log_action(
                        db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=lg_record.id,
                        details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "reason": "Email service failed to send notification", "method": email_method_for_log},
                        customer_id=lg_record.customer_id, lg_record_id=lg_record.id,
                    )
                    logger.error(f"LG liquidated (ID: {lg_record.id}), but failed to send email notification.")
                else:
                    log_action(
                        db, user_id=user_id, action_type="NOTIFICATION_SENT", entity_type="LGRecord", entity_id=lg_record.id,
                        details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "method": email_method_for_log},
                        customer_id=lg_record.customer_id, lg_record_id=lg_record.id,
                    )

        db.refresh(lg_record)
        return lg_record, db_lg_instruction.id    

    async def decrease_lg_amount(self, db: Session, lg_record: LGRecord, decrease_amount: float, user_id: int, approval_request_id: Optional[int], supporting_document_id: Optional[int] = None, notes: Optional[str] = None) -> Tuple[models.LGRecord, int]:
        """
        Decreases the amount of an LG record. Updates the amount, issues a bank letter,
        and notifies stakeholders. LG status remains 'Valid'.
        """
        instruction_maker_user_id = user_id
        recipient_name = "To Whom It May Concern"
        recipient_address = "N/A"
        if approval_request_id:
            approval_request = db.query(models.ApprovalRequest).filter(models.ApprovalRequest.id == approval_request_id).first()
            if approval_request:
                instruction_maker_user_id = approval_request.maker_user_id
            else:
                logger.warning(f"ApprovalRequest with ID {approval_request_id} not found when creating LGInstruction for decrease amount. Using checker_user_id as maker for instruction.")

        try:
            if lg_record.lg_status_id != models.LgStatusEnum.VALID.value:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Only LGs with status 'Valid' can have their amount decreased. Current status: {lg_record.lg_status.name}.",
                )

            if decrease_amount <= 0:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Decrease amount must be greater than 0.")

            if decimal.Decimal(str(decrease_amount)) >= lg_record.lg_amount:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Decrease amount must be less than the current LG amount. Use liquidation for full reduction.")

            original_amount = lg_record.lg_amount

            new_amount = original_amount - decimal.Decimal(str(decrease_amount))

            logger.debug(f"LG {lg_record.id}: Original amount: {original_amount}, Decrease amount: {decrease_amount}, New calculated amount: {new_amount}")

            lg_record.lg_amount = new_amount
            db.add(lg_record)
            db.flush()

            logger.debug(f"LG {lg_record.id} amount updated in DB session to: {lg_record.lg_amount}. Proceeding with instruction and notification.")

            instruction_template = db.query(models.Template).filter(models.Template.action_type == ACTION_TYPE_LG_DECREASE_AMOUNT, models.Template.is_global == True, models.Template.is_notification_template == False, models.Template.is_deleted == False).first()
            if not instruction_template:
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"LG Decrease Amount Instruction template not found. Please ensure a global '{ACTION_TYPE_LG_DECREASE_AMOUNT}' template (non-notification) exists.")
            
            customer = db.query(models.Customer).filter(models.Customer.id == lg_record.customer_id).first()
            entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.id == lg_record.beneficiary_corporate_id).first()

            if not customer or not entity:
                    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Customer or entity record not found for LG.")

            customer_address = entity.address if entity.address else customer.address
            customer_contact_email = entity.contact_email if entity.contact_email else customer.contact_email

            instruction_details = {
                "lg_number": lg_record.lg_number,
                "original_lg_amount": float(original_amount),
                "decrease_amount": float(decrease_amount),
                "new_lg_amount": float(new_amount),
                "lg_currency": lg_record.lg_currency.iso_code,
                "issuing_bank_name": lg_record.issuing_bank.name,
                "lg_issuer_name": lg_record.issuer_name,
                "lg_beneficiary_name": lg_record.beneficiary_corporate.entity_name,
                "current_date": datetime.now().strftime("%Y-%m-%d"),
                "customer_name": lg_record.customer.name,
                "customer_address": customer_address,
                "customer_contact_email": customer_contact_email,
                "action_type": "LG Amount Decrease",
                "internal_owner_email": lg_record.internal_owner_contact.email,
                "recipient_name": recipient_name,
                "recipient_address": recipient_address,
            }

            notes_html = ""
            if notes:
                notes_html = f"""
                <h3>Additional Notes</h3>
                <p>{notes}</p>
                """
            instruction_details["notes_section"] = notes_html # NEW: Use the notes_section key
            
            instruction_details["original_lg_amount_formatted"] = f"{lg_record.lg_currency.symbol} {float(original_amount):,.2f}"
            instruction_details["decrease_amount_formatted"] = f"{lg_record.lg_currency.symbol} {float(decrease_amount):,.2f}"
            instruction_details["new_lg_amount_formatted"] = f"{lg_record.lg_currency.symbol} {float(new_amount):,.2f}"

            generated_instruction_html = instruction_template.content
            for key, value in instruction_details.items():
                str_value = str(value) if value is not None else ""
                generated_instruction_html = generated_instruction_html.replace(f"{{{{{key}}}}}", str_value)

            try:
                instruction_create_payload_for_schema = {
                    "lg_record_id": lg_record.id,
                    "instruction_type": ACTION_TYPE_LG_DECREASE_AMOUNT,
                    "template_id": instruction_template.id,
                    "status": "Instruction Issued",
                    "details": instruction_details,
                    "maker_user_id": instruction_maker_user_id,
                    "checker_user_id": user_id if approval_request_id else None,
                    "approval_request_id": approval_request_id,
                    "serial_number": None,
                }

                instruction_type_code_enum = InstructionTypeCode.DEC
                sub_instruction_code_enum = SubInstructionCode.ORIGINAL

                serial_generation_params = {
                    'lg_record_id': lg_record.id,
                    'instruction_type_code': instruction_type_code_enum,
                    'sub_instruction_code': sub_instruction_code_enum,
                    'user_id': user_id
                }

                instruction_id = await self._create_instruction_with_retry(
                    db,
                    instruction_create_payload=instruction_create_payload_for_schema,
                    **serial_generation_params
                )
                
                db_lg_instruction = db.query(models.LGInstruction).filter(models.LGInstruction.id == instruction_id).first()
                if not db_lg_instruction:
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve newly created instruction after creation.")

                filename_for_pdf = f"{db_lg_instruction.serial_number}"
                generated_pdf_bytes = await generate_pdf_from_html(generated_instruction_html, filename_for_pdf)
                
                generated_content_path = f"gs://{GCS_BUCKET_NAME}/generated_instructions/{filename_for_pdf}"
                
                db_lg_instruction_in_current_session = db.merge(db_lg_instruction)
                db_lg_instruction_in_current_session.generated_content_path = generated_content_path
                db.add(db_lg_instruction_in_current_session)
                db.flush()
                
                if supporting_document_id:
                    db_document = db.query(models.LGDocument).filter(models.LGDocument.id == supporting_document_id, models.LGDocument.is_deleted == False).first()
                    if db_document and not db_document.lg_instruction_id:
                        db_document.lg_instruction_id = db_lg_instruction_in_current_session.id
                        db.add(db_document)
                        db.flush()
                        logger.debug(f"Successfully linked supporting document ID {supporting_document_id} to new instruction ID {db_lg_instruction_in_current_session.id}.")

                db.refresh(lg_record)
                db.refresh(db_lg_instruction_in_current_session)

            except Exception as e:
                db.rollback()
                logger.exception(f"An unexpected error occurred during LG amount decrease for LG {lg_record.lg_number}: {e}")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during LG amount decrease: {e}")
            
            log_action(
                db, user_id, AUDIT_ACTION_TYPE_LG_DECREASED_AMOUNT, "LGRecord", lg_record.id,
                {
                    "lg_number": lg_record.lg_number, 
                    "old_amount": float(original_amount), 
                    "new_amount": float(new_amount), 
                    "instruction_id": db_lg_instruction.id, 
                    "approved_by_user_id": user_id, 
                    "notes": notes # NEW: Log the notes
                },
                lg_record.customer_id, lg_record.id
            )

            if approval_request_id is None:
                email_settings_to_use: EmailSettings
                email_method_for_log: str
                try:
                    email_settings_to_use, email_method_for_log = get_customer_email_settings(db, lg_record.customer_id)
                except Exception as e:
                    email_settings_to_use = get_global_email_settings()
                    email_method_for_log = "global_fallback_due_to_error"
                    logger.warning(f"Failed to retrieve customer-specific email settings for customer ID {lg_record.customer_id}: {e}. Falling back to global settings.")

                email_to_send_to = [lg_record.internal_owner_contact.email]
                cc_emails = []
                if lg_record.internal_owner_contact.manager_email:
                    cc_emails.append(lg_record.internal_owner_contact.manager_email)
                if lg_record.lg_category and lg_record.lg_category.communication_list:
                    cc_emails.extend(lg_record.lg_category.communication_list)

                common_comm_list_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                    db, lg_record.customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
                )
                if common_comm_list_config and common_comm_list_config.get('effective_value'):
                    try:
                        parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                        if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                            cc_emails.extend(parsed_common_list)
                    except json.JSONDecodeError:
                        logger.warning(f"COMMON_COMMUNICATION_LIST for customer {lg_record.customer_id} is not a valid JSON list of emails. Skipping.")
                cc_emails = list(set(cc_emails))

                notification_template = db.query(models.Template).filter(models.Template.action_type == ACTION_TYPE_LG_DECREASE_AMOUNT, models.Template.is_global == True, models.Template.is_notification_template == True, models.Template.is_deleted == False).first()

                if not notification_template:
                    log_action(
                        db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=lg_record.id,
                        details={"recipient": email_to_send_to, "subject": "N/A", "reason": f"{ACTION_TYPE_LG_DECREASE_AMOUNT} notification template (is_notification_template=True) not found", "method": "none"},
                        customer_id=lg_record.customer_id, lg_record_id=lg_record.id,
                    )
                    logger.error(f"LG amount decreased (ID: {lg_record.id}), but failed to send email notification.")
                else:
                    template_data = {
                        "lg_number": lg_record.lg_number,
                        "original_lg_amount": float(original_amount),
                        "original_lg_amount_formatted": f"{lg_record.lg_currency.symbol} {float(original_amount):,.2f}" if lg_record.lg_currency else f"{float(original_amount):,.2f}",
                        "decrease_amount": float(decrease_amount),
                        "decrease_amount_formatted": f"{lg_record.lg_currency.symbol} {float(decrease_amount):,.2f}" if lg_record.lg_currency else f"{float(decrease_amount):,.2f}",
                        "new_lg_amount": float(new_amount),
                        "new_lg_amount_formatted": f"{lg_record.lg_currency.symbol} {float(new_amount):,.2f}" if lg_record.lg_currency else f"{float(new_amount):,.2f}",
                        "lg_currency": lg_record.lg_currency.iso_code if lg_record.lg_currency else "N/A",
                        "issuing_bank_name": lg_record.issuing_bank.name,
                        "lg_issuer_name": lg_record.issuer_name,
                        "lg_beneficiary_name": lg_record.beneficiary_corporate.entity_name,
                        "current_date": datetime.now().strftime("%Y-%m-%d"),
                        "customer_name": lg_record.customer.name,
                        "action_type": "LG Amount Decrease",
                        "instruction_serial": db_lg_instruction.serial_number,
                        "internal_owner_email": lg_record.internal_owner_contact.email,
                        "notes": notes, # NEW: Add notes to the email template
                    }
                    email_subject = notification_template.subject if notification_template.subject else f"{{action_type}} LG #{{lg_number}} - Instruction #{{instruction_serial}}"
                    email_body_html = notification_template.content
                    for key, value in template_data.items():
                        str_value = str(value) if value is not None else ""
                        email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
                        email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

                    email_sent_successfully = await send_email(
                        db=db,
                        to_emails=email_to_send_to,
                        cc_emails=cc_emails,
                        subject_template=email_subject,
                        body_template=email_body_html,
                        template_data=template_data,
                        email_settings=email_settings_to_use,
                        sender_name=lg_record.customer.name
                    )
                    if not email_sent_successfully:
                        log_action(
                            db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=lg_record.id,
                            details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "reason": "Email service failed to send notification", "method": email_method_for_log},
                            customer_id=lg_record.customer_id, lg_record_id=lg_record.id,
                        )
                        logger.error(f"LG amount decreased (ID: {lg_record.id}), but failed to send email notification.")
                    else:
                        log_action(
                            db, user_id=user_id, action_type="NOTIFICATION_SENT", entity_type="LGRecord", entity_id=lg_record.id,
                            details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "method": email_method_for_log},
                            customer_id=lg_record.customer_id, lg_record_id=lg_record.id,
                        )

            db.refresh(lg_record)
            return lg_record, db_lg_instruction.id
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred in decrease_lg_amount for LG {lg_record.id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected internal error occurred during amount decrease: {e}")

    async def activate_non_operative_lg(self, db: Session, lg_record: LGRecord, payment_details: LGActivateNonOperativeRequest, user_id: int, customer_id: int, approval_request_id: Optional[int], supporting_document_id: Optional[int] = None, notes: Optional[str] = None) -> Tuple[models.LGRecord, int]: # NEW: Add notes parameter
        """
        Activates a non-operative Advance Payment Guarantee.
        Updates operational status to "Operative", creates an activation instruction, and notifies stakeholders.
        user_id here refers to the actual actor (maker if direct, checker if approved).
        """
        logger.debug(f"[CRUDLGRecord.activate_non_operative_lg] Initiating activation for LG ID: {lg_record.id}")

        instruction_maker_user_id = user_id
        recipient_name = "To Whom It May Concern"
        recipient_address = "N/A"
        if approval_request_id:
            approval_request = db.query(models.ApprovalRequest).filter(models.ApprovalRequest.id == approval_request_id).first()
            if approval_request:
                instruction_maker_user_id = approval_request.maker_user_id
            else:
                logger.warning(f"ApprovalRequest with ID {approval_request_id} not found when creating LGInstruction for activation. Using checker_user_id as maker for instruction.")

        db_lg_record = self.get_lg_record_with_relations(db, lg_record.id, customer_id)
        if not db_lg_record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")


        if db_lg_record.lg_status_id != models.LgStatusEnum.VALID.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"LG record must be in 'Valid' status to be activated. Current status: {db_lg_record.lg_status.name}."
            )

        if db_lg_record.lg_type_id != models.LgTypeEnum.ADVANCE_PAYMENT_GUARANTEE.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only 'Advance Payment Guarantee' LG types can be activated via this process."
            )

        if db_lg_record.lg_operational_status_id != models.LgOperationalStatusEnum.NON_OPERATIVE.value:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"LG record must be in 'Non-Operative' operational status to be activated. Current operational status: {db_lg_record.lg_operational_status.name if db_lg_record.lg_operational_status else 'N/A'}."
                )

        operative_status = db.query(models.LgOperationalStatus).filter(models.LgOperationalStatus.id == models.LgOperationalStatusEnum.OPERATIVE.value).first()
        if not operative_status:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="System misconfiguration: 'Operative' operational status not found.")

        db_lg_record.lg_operational_status_id = operative_status.id
        db.add(db_lg_record)
        db.flush()

        instruction_template = db.query(models.Template).filter(models.Template.action_type == ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE, models.Template.is_global == True, models.Template.is_notification_template == False, models.Template.is_deleted == False).first()
        if not instruction_template:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"LG Activation Instruction template not found. Please ensure a global '{ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE}' template (non-notification) exists.")

        payment_currency = db.query(models.Currency).filter(models.Currency.id == payment_details.currency_id).first()
        payment_bank = db.query(models.Bank).filter(models.Bank.id == payment_details.issuing_bank_id).first()

        customer = db.query(models.Customer).filter(models.Customer.id == db_lg_record.customer_id).first()
        entity = db.query(models.CustomerEntity).filter(models.CustomerEntity.id == db_lg_record.beneficiary_corporate_id).first()

        if not customer or not entity:
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Customer or entity record not found for LG.")

        customer_address = entity.address if entity.address else customer.address
        customer_contact_email = entity.contact_email if entity.contact_email else customer.contact_email

        instruction_details = {
            "lg_number": db_lg_record.lg_number,
            "lg_amount": float(db_lg_record.lg_amount),
            "lg_currency": db_lg_record.lg_currency.iso_code,
            "issuing_bank_name": db_lg_record.issuing_bank.name,
            "lg_beneficiary_name": db_lg_record.beneficiary_corporate.entity_name,
            "lg_issuer_name": db_lg_record.issuer_name,
            "current_date": datetime.now().strftime("%Y-%m-%d"),
            "customer_name": db_lg_record.customer.name,
            "customer_address": customer_address,
            "customer_contact_email": customer_contact_email,
            "internal_owner_email": db_lg_record.internal_owner_contact.email,
            "lg_serial_number": db_lg_record.lg_number,
            "payment_method": payment_details.payment_method,
            "payment_amount": float(payment_details.amount),
            "payment_currency_code": payment_currency.iso_code if payment_currency else "N/A",
            "payment_reference": payment_details.payment_reference,
            "payment_issuing_bank_name": payment_bank.name if payment_bank else "N/A",
            "payment_date": payment_details.payment_date.isoformat(),
            "original_lg_status_id": models.LgOperationalStatusEnum.NON_OPERATIVE.value,
            "new_lg_status_id": models.LgOperationalStatusEnum.OPERATIVE.value,
            "lg_type_id": models.LgTypeEnum.ADVANCE_PAYMENT_GUARANTEE.value,
            "recipient_name": recipient_name,
            "recipient_address": recipient_address,
        }

        notes_html = ""
        if notes:
            notes_html = f"""
            <h3>Additional Notes</h3>
            <p>{notes}</p>
            """
        instruction_details["notes_section"] = notes_html # NEW: Use the notes_section key

        instruction_details["lg_amount_formatted"] = f"{db_lg_record.lg_currency.symbol} {float(db_lg_record.lg_amount):,.2f}"
        instruction_details["payment_amount_formatted"] = f"{payment_currency.iso_code if payment_currency else 'N/A'} {float(payment_details.amount):,.2f}"

        generated_instruction_html = instruction_template.content
        for key, value in instruction_details.items():
            str_value = str(value) if value is not None else ""
            generated_instruction_html = generated_instruction_html.replace(f"{{{{{key}}}}}", str_value)

        try:
            instruction_create_payload_for_schema = {
                "lg_record_id": db_lg_record.id,
                "instruction_type": ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
                "template_id": instruction_template.id,
                "status": "Instruction Issued",
                "details": instruction_details,
                "maker_user_id": instruction_maker_user_id,
                "checker_user_id": user_id if approval_request_id else None,
                "approval_request_id": approval_request_id,
                "serial_number": None,
            }

            instruction_type_code_enum = InstructionTypeCode.ACT
            sub_instruction_code_enum = SubInstructionCode.ORIGINAL

            serial_generation_params = {
                'lg_record_id': db_lg_record.id,
                'instruction_type_code': instruction_type_code_enum,
                'sub_instruction_code': sub_instruction_code_enum,
                'user_id': user_id
            }

            instruction_id = await self._create_instruction_with_retry(
                db,
                instruction_create_payload=instruction_create_payload_for_schema,
                **serial_generation_params
            )
            
            db_lg_instruction = db.query(models.LGInstruction).filter(models.LGInstruction.id == instruction_id).first()
            if not db_lg_instruction:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve newly created instruction after creation.")

            filename_for_pdf = f"lg_activation_{db_lg_record.lg_number}_instruction_{db_lg_instruction.serial_number}"
            generated_pdf_bytes = await generate_pdf_from_html(generated_instruction_html, filename_for_pdf)
            
            generated_content_path = f"gs://your-gcs-bucket/generated_instructions/{filename_for_pdf}.pdf"
            
            db_lg_instruction_in_current_session = db.merge(db_lg_instruction)
            db_lg_instruction_in_current_session.generated_content_path = generated_content_path
            db.add(db_lg_instruction_in_current_session)
            db.flush()

            if supporting_document_id:
                db_document = db.query(models.LGDocument).filter(models.LGDocument.id == supporting_document_id, models.LGDocument.is_deleted == False).first()
                if db_document and not db_document.lg_instruction_id:
                    db_document.lg_instruction_id = db_lg_instruction_in_current_session.id
                    db.add(db_document)
                    db.flush()
                    logger.debug(f"Successfully linked supporting document ID {supporting_document_id} to new instruction ID {db_lg_instruction_in_current_session.id}.")

            db.refresh(db_lg_record)
            db.refresh(db_lg_instruction_in_current_session)

        except Exception as e:
            db.rollback()
            logger.exception(f"An unexpected error occurred during LG activation for LG {db_lg_record.lg_number}: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during LG activation: {e}")

        payment_details_for_log = payment_details.model_dump()
        if 'payment_date' in payment_details_for_log and isinstance(payment_details_for_log['payment_date'], date):
            payment_details_for_log['payment_date'] = payment_details_for_log['payment_date'].isoformat()
        
        log_action(
            db, user_id, AUDIT_ACTION_TYPE_LG_ACTIVATED, "LGRecord", db_lg_record.id,
            {"lg_number": db_lg_record.lg_number, "old_status": models.LgOperationalStatusEnum.NON_OPERATIVE.name, "new_status": models.LgOperationalStatusEnum.OPERATIVE.name, "payment_details": payment_details_for_log, "instruction_id": db_lg_instruction.id, "approved_by_user_id": user_id, "notes": notes}, # NEW: Log the notes
            db_lg_record.customer_id, db_lg_record.id
        )

        if approval_request_id is None:
            email_settings_to_use: EmailSettings
            email_method_for_log: str
            try:
                email_settings_to_use, email_method_for_log = get_customer_email_settings(db, db_lg_record.customer_id)
            except Exception as e:
                email_settings_to_use = get_global_email_settings()
                email_method_for_log = "global_fallback_due_to_error"
                logger.warning(f"Failed to retrieve customer-specific email settings for customer ID {db_lg_record.customer_id}: {e}. Falling back to global settings.")

            email_to_send_to = [db_lg_record.internal_owner_contact.email]
            cc_emails = []
            if db_lg_record.internal_owner_contact.manager_email:
                cc_emails.append(db_lg_record.internal_owner_contact.manager_email)
            if db_lg_record.lg_category and db_lg_record.lg_category.communication_list:
                cc_emails.extend(db_lg_record.lg_category.communication_list)

            common_comm_list_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, db_lg_record.customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
            )
            if common_comm_list_config and common_comm_list_config.get('effective_value'):
                try:
                    parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                    if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                        cc_emails.extend(parsed_common_list)
                except json.JSONDecodeError:
                    logger.warning(f"COMMON_COMMUNICATION_LIST for customer {db_lg_record.customer_id} is not a valid JSON list of emails. Skipping.")
            cc_emails = list(set(cc_emails))

            notification_template = db.query(models.Template).filter(models.Template.action_type == ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE, models.Template.is_global == True, models.Template.is_notification_template == True, models.Template.is_deleted == False).first()

            if not notification_template:
                log_action(
                    db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=db_lg_record.id,
                    details={"recipient": email_to_send_to, "subject": "N/A", "reason": f"{ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE} notification template (is_notification_template=True) not found", "method": "none"},
                    customer_id=db_lg_record.customer_id, lg_record_id=db_lg_record.id,
                )
                logger.error(f"LG activated (ID: {db_lg_record.id}), but failed to send email notification.")
            else:
                template_data = {
                    "lg_number": db_lg_record.lg_number,
                    "lg_amount": float(db_lg_record.lg_amount),
                    "lg_currency": db_lg_record.lg_currency.iso_code,
                    "issuing_bank_name": db_lg_record.issuing_bank.name,
                    "lg_beneficiary_name": db_lg_record.beneficiary_corporate.entity_name,
                    "lg_issuer_name": db_lg_record.issuer_name,
                    "current_date": datetime.now().strftime("%Y-%m-%d"),
                    "customer_name": db_lg_record.customer.name,
                    "action_type": "LG Activation",
                    "instruction_serial": db_lg_instruction.serial_number,
                    "internal_owner_email": db_lg_record.internal_owner_contact.email,
                    "payment_method": payment_details.payment_method,
                    "payment_amount": float(payment_details.amount),
                    "payment_currency_code": payment_currency.iso_code,
                    "payment_reference": payment_details.payment_reference,
                    "payment_issuing_bank_name": payment_bank.name if payment_bank else "N/A",
                    "payment_date": payment_details.payment_date.isoformat(),
                    "notes": notes, # NEW: Add notes to email template data
                }
                template_data["lg_amount_formatted"] = f"{db_lg_record.lg_currency.symbol} {float(db_lg_record.lg_amount):,.2f}"
                template_data["payment_amount_formatted"] = f"{template_data['payment_currency_code']} {float(payment_details.amount):,.2f}"

                email_subject = notification_template.subject if notification_template.subject else f"{{action_type}} LG #{{lg_number}} - Instruction #{{instruction_serial}}"
                email_body_html = notification_template.content
                for key, value in template_data.items():
                    str_value = str(value) if value is not None else ""
                    email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
                    email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

                email_sent_successfully = await send_email(
                    db=db,
                    to_emails=email_to_send_to,
                    cc_emails=cc_emails,
                    subject_template=email_subject,
                    body_template=email_body_html,
                    template_data=template_data,
                    email_settings=email_settings_to_use,
                    sender_name=db_lg_record.customer.name
                )
                if not email_sent_successfully:
                    log_action(
                        db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=db_lg_record.id,
                        details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "reason": "Email service failed to send notification", "method": email_method_for_log},
                        customer_id=db_lg_record.customer_id, lg_record_id=db_lg_record.id,
                    )
                    logger.error(f"LG activated (ID: {db_lg_record.id}), but failed to send email notification.")
                else:
                    log_action(
                        db, user_id=user_id, action_type="NOTIFICATION_SENT", entity_type="LGRecord", entity_id=db_lg_record.id,
                        details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "method": email_method_for_log},
                        customer_id=db_lg_record.customer_id, lg_record_id=db_lg_record.id,
                    )

        db.refresh(db_lg_record)
        return db_lg_record, db_lg_instruction.id
     
    async def toggle_lg_auto_renewal(self, db: Session, lg_record: models.LGRecord, new_auto_renewal_status: bool, user_id: int, customer_id: int, reason: Optional[str], approval_request_id: Optional[int]) -> models.LGRecord:
        """
        Toggles the auto_renewal status of an LG record directly, without an approval process
        and without sending a notification email.
        """
        logger.debug(f"[CRUDLGRecord.toggle_lg_auto_renewal] Toggling auto-renewal for LG ID: {lg_record.id} to {new_auto_renewal_status}.")

        if lg_record.lg_status_id != models.LgStatusEnum.VALID.value: # Corrected to models.LgStatusEnum
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Auto-renewal cannot be toggled for LGs in status '{lg_record.lg_status.name}'. Must be 'Valid'."
            )

        if lg_record.auto_renewal == new_auto_renewal_status:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"LG auto-renewal is already set to {new_auto_renewal_status}."
            )

        old_auto_renewal_status = lg_record.auto_renewal

        lg_record.auto_renewal = new_auto_renewal_status
        db.add(lg_record)
        db.flush()

        logger.info(f"Auto-renewal for LG {lg_record.lg_number} toggled from {old_auto_renewal_status} to {new_auto_renewal_status}. No notification email sent as per requirement.")

        log_action(
            db, user_id, AUDIT_ACTION_TYPE_LG_AUTO_RENEWAL_TOGGLED, "LGRecord", lg_record.id,
            {"lg_number": lg_record.lg_number, "old_status": old_auto_renewal_status, "new_status": new_auto_renewal_status, "reason": reason},
            customer_id, lg_record.id
        )

        db.refresh(lg_record)
        return lg_record

    async def amend_lg(self,
                       db: Session,
                       lg_record_id: int,
                       amendment_letter_file: Optional[UploadFile],
                       amendment_document_metadata: Optional[LGDocumentCreate],
                       amendment_details: Dict[str, Any],
                       user_id: int,
                       customer_id: int,
                       approval_request_id: Optional[int],
                       existing_document_id: Optional[int] = None
                       ) -> models.LGRecord:
        """
        Applies amendments to an LG record based on bank amendment letter.
        This updates the LGRecord fields and logs the changes. No instruction letter is generated.
        user_id here refers to the actual actor (maker if direct, checker if approved).
        """
        logger.debug(f"[CRUDLGRecord.amend_lg] Initiating amendment for LG ID: {lg_record_id}")

        db_lg_record = self.get_lg_record_with_relations(db, lg_record_id, customer_id)
        if not db_lg_record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")

        # MODIFIED LOGIC START: Allow amendment for expired LGs within 30 days.
        current_date = date.today()
        thirty_days_ago = current_date - timedelta(days=30)
        
        is_expired_within_grace_period = (
            db_lg_record.lg_status_id == models.LgStatusEnum.EXPIRED.value
            and db_lg_record.expiry_date.date() >= thirty_days_ago
        )

        if db_lg_record.is_deleted or (db_lg_record.lg_status_id not in [models.LgStatusEnum.VALID.value, models.LgStatusEnum.EXPIRED.value]) or (
            db_lg_record.lg_status_id == models.LgStatusEnum.EXPIRED.value and not is_expired_within_grace_period
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"LG record cannot be amended. It is either released, liquidated, or expired more than 30 days ago. Current status: {db_lg_record.lg_status.name}."
            )
        # MODIFIED LOGIC END

        acting_user_id = user_id

        updated_lg_record = db_lg_record

        document_id_for_log = None

        if existing_document_id:
            document_id_for_log = existing_document_id
            logger.debug(f"[CRUDLGRecord.amend_lg] Using existing amendment document ID from approval request: {document_id_for_log}")
        elif amendment_letter_file and amendment_document_metadata:
            customer_obj = db.query(models.Customer).options(selectinload(models.Customer.subscription_plan)).filter(models.Customer.id == customer_id).first()
            if not customer_obj or not customer_obj.subscription_plan or not customer_obj.subscription_plan.can_image_storage:
                logger.warning(f"[CRUDLGRecord.amend_lg] Customer's plan '{customer_obj.subscription_plan.name}' does not support image storage. Amendment document will not be stored for direct call.")
            else:
                try:
                    file_bytes = await amendment_letter_file.read()
                    db_amendment_document = await self.crud_lg_document_instance.create_document(
                        db,
                        obj_in=amendment_document_metadata,
                        file_content=file_bytes,
                        lg_record_id=db_lg_record.id,
                        uploaded_by_user_id=user_id
                    )
                    document_id_for_log = db_amendment_document.id
                    logger.debug(f"[CRUDLGRecord.amend_lg] Amendment letter document stored: {db_amendment_document.file_path} for direct call.")
                except Exception as e:
                    logger.error(f"[CRUDLGRecord.amend_lg] Failed to store amendment letter for LG {lg_record_id} at maker submission: {e}", exc_info=True)
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to store amendment letter: {e}")
        else:
            logger.debug(f"No amendment letter file provided or existing document ID found for LG {lg_record_id}. Proceeding without document link for log.")

        updatable_fields = [
            "lg_amount", "lg_currency_id", "lg_payable_currency_id", "issuance_date",
            "expiry_date", "auto_renewal", "lg_type_id", "lg_status_id",
            "lg_operational_status_id", "payment_conditions", "description_purpose",
            "issuing_bank_id", "issuing_bank_address", "issuing_bank_phone",
            "issuing_bank_fax", "issuing_method_id", "applicable_rule_id",
            "applicable_rules_text", "other_conditions", "internal_owner_contact_id",
            "lg_category_id", "additional_field_values", "internal_contract_project_id",
            "notes", "lg_number", "beneficiary_corporate_id", "issuer_name", "issuer_id"
        ]

        updates_to_apply = {}
        for key, value in amendment_details.items():
            if key in updatable_fields:
                if key in ["issuance_date", "expiry_date"] and isinstance(value, str):
                    try:
                        # FIX: Use date.fromisoformat to handle ISO 8601 dates correctly.
                        updates_to_apply[key] = date.fromisoformat(value.split("T")[0])
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid date format for {key}. Expected YYYY-MM-DD.")
                elif key in ["lg_amount"] and isinstance(value, (int, float, str)):
                    try:
                        updates_to_apply[key] = decimal.Decimal(str(value))
                    except (decimal.InvalidOperation, ValueError):
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid numeric format for {key}.")
                else:
                    updates_to_apply[key] = value
            else:
                logger.warning(f"Attempted to amend non-updatable field: {key}. Skipping.")

        if not updates_to_apply:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid LG fields provided for amendment.")

        old_values_for_log = {key: getattr(db_lg_record, key) for key in updates_to_apply.keys()}
        for key, value in old_values_for_log.items():
            if isinstance(value, (date, datetime)):
                old_values_for_log[key] = value.isoformat()
            elif isinstance(value, decimal.Decimal):
                old_values_for_log[key] = float(value)

        updated_lg_record = super().update(db, db_lg_record, obj_in=updates_to_apply)

        # NEW LOGIC START: Check for expired status and future expiry date
        is_currently_expired = db_lg_record.lg_status_id == models.LgStatusEnum.EXPIRED.value
        has_future_expiry_date = "expiry_date" in updates_to_apply and updates_to_apply["expiry_date"] > date.today()

        if is_currently_expired and has_future_expiry_date:
            valid_status = db.query(models.LgStatus).filter(models.LgStatus.id == models.LgStatusEnum.VALID.value).first()
            if not valid_status:
                logger.error("System misconfiguration: 'Valid' LG status not found. Cannot automatically update status.")
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="System misconfiguration: 'Valid' status not found.")
            
            updated_lg_record.lg_status_id = valid_status.id
            db.add(updated_lg_record)
            db.flush()
            logger.info(f"LG {updated_lg_record.lg_number} status automatically updated from 'Expired' to 'Valid' due to expiry date amendment.")
        # NEW LOGIC END


        if "issuance_date" in updates_to_apply or "expiry_date" in updates_to_apply:
            if updated_lg_record.issuance_date and updated_lg_record.expiry_date:
                delta = updated_lg_record.expiry_date.date() - updated_lg_record.issuance_date.date()
                new_lg_period_months = max(1, round(delta.days / 30.44))
                updated_lg_record.lg_period_months = new_lg_period_months
                db.add(updated_lg_record)
                db.flush()
                db.refresh(updated_lg_record)

        json_serializable_amended_fields = {}
        for k, v in updates_to_apply.items():
            if isinstance(v, decimal.Decimal):
                json_serializable_amended_fields[k] = float(v)
            elif isinstance(v, (date, datetime)):
                json_serializable_amended_fields[k] = v.isoformat()
            else:
                json_serializable_amended_fields[k] = v

        log_action(
            db, acting_user_id, AUDIT_ACTION_TYPE_LG_AMENDED, "LGRecord", updated_lg_record.id,
            {
                "lg_number": updated_lg_record.lg_number,
                "amended_fields": json_serializable_amended_fields,
                "old_values": old_values_for_log,
                "amendment_document_id": document_id_for_log,
                "approved_by_user_id": acting_user_id
            },
            updated_lg_record.customer_id, updated_lg_record.id
        )
        if approval_request_id is None:
            email_settings_to_use: EmailSettings
            email_method_for_log: str
            try:
                email_settings_to_use, email_method_for_log = get_customer_email_settings(db, updated_lg_record.customer_id)
            except Exception as e:
                email_settings_to_use = get_global_email_settings()
                email_method_for_log = "global_fallback_due_to_error"
                logger.warning(f"Failed to retrieve customer-specific email settings for customer ID {updated_lg_record.customer_id}: {e}. Falling back to global settings.")

            email_to_send_to = [updated_lg_record.internal_owner_contact.email]
            cc_emails = []
            if updated_lg_record.internal_owner_contact.manager_email:
                cc_emails.append(updated_lg_record.internal_owner_contact.manager_email)
            if updated_lg_record.lg_category and updated_lg_record.lg_category.communication_list:
                cc_emails.extend(updated_lg_record.lg_category.communication_list)

            common_comm_list_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, updated_lg_record.customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
            )
            if common_comm_list_config and common_comm_list_config.get('effective_value'):
                try:
                    parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                    if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                        cc_emails.extend(parsed_common_list)
                except json.JSONDecodeError:
                    logger.warning(f"COMMON_COMMUNICATION_LIST for customer {updated_lg_record.customer_id} is not a valid JSON list of emails. Skipping.")
            cc_emails = list(set(cc_emails))

            notification_template = db.query(models.Template).filter(models.Template.action_type == ACTION_TYPE_LG_AMEND, models.Template.is_global == True, models.Template.is_notification_template == True, models.Template.is_deleted == False).first()

            if not notification_template:
                log_action(
                    db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=updated_lg_record.id,
                    details={"recipient": email_to_send_to, "subject": "N/A", "reason": f"{ACTION_TYPE_LG_AMEND} notification template (is_notification_template=True) not found", "method": "none"},
                    customer_id=updated_lg_record.customer_id, lg_record_id=updated_lg_record.id,
                )
                logger.error(f"LG amended (ID: {updated_lg_record.id}), but failed to send email notification.")
            else:
                template_data = {
                    "lg_number": updated_lg_record.lg_number,
                    "amended_fields_summary": ", ".join(json_serializable_amended_fields.keys()),
                    "lg_amount": float(updated_lg_record.lg_amount),
                    "lg_currency": updated_lg_record.lg_currency.iso_code,
                    "issuing_bank_name": updated_lg_record.issuing_bank.name,
                    "lg_beneficiary_name": updated_lg_record.beneficiary_corporate.entity_name,
                    "current_date": datetime.now().strftime("%Y-%m-%d"),
                    "customer_name": updated_lg_record.customer.name,
                    "action_type": "LG Amendment",
                    "internal_owner_email": updated_lg_record.internal_owner_contact.email,
                    "amendment_document_id": document_id_for_log,
                    "full_amendment_details": json.dumps(json_serializable_amended_fields, indent=2),
                    "lg_amount_formatted": f"{updated_lg_record.lg_currency.symbol} {float(updated_lg_record.lg_amount):,.2f}",
                }
                email_subject = notification_template.subject if notification_template.subject else f"{{action_type}} LG #{{lg_number}}"
                email_body_html = notification_template.content
                for key, value in template_data.items():
                    str_value = str(value) if value is not None else ""
                    email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
                    email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

                email_sent_successfully = await send_email(
                    db=db,
                    to_emails=email_to_send_to,
                    cc_emails=cc_emails,
                    subject_template=email_subject,
                    body_template=email_body_html,
                    template_data=template_data,
                    email_settings=email_settings_to_use,
                    sender_name=updated_lg_record.customer.name
                )
                if not email_sent_successfully:
                    log_action(
                        db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord", entity_id=updated_lg_record.id,
                        details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "reason": "Email service failed to send notification", "method": email_method_for_log},
                        customer_id=updated_lg_record.customer_id, lg_record_id=updated_lg_record.id,
                    )
                    logger.error(f"LG amended (ID: {updated_lg_record.id}), but failed to send email notification.")
                else:
                    log_action(
                        db, user_id=user_id, action_type="NOTIFICATION_SENT", entity_type="LGRecord", entity_id=updated_lg_record.id,
                        details={"recipient": email_to_send_to, "cc_recipients": cc_emails, "subject": email_subject, "method": email_method_for_log},
                        customer_id=updated_lg_record.customer_id, lg_record_id=updated_lg_record.id,
                    )
        db.refresh(updated_lg_record)
        return updated_lg_record
        
    def get_lg_record_with_relations(
        self, db: Session, lg_record_id: int, customer_id: Optional[int]
    ) -> Optional[models.LGRecord]:
        query = db.query(self.model).filter(self.model.id == lg_record_id, self.model.is_deleted == False)
        if customer_id is not None:
            query = query.filter(self.model.customer_id == customer_id)

        return (
            query.options(
                selectinload(models.LGRecord.beneficiary_corporate),
                selectinload(models.LGRecord.lg_currency),
                selectinload(models.LGRecord.lg_payable_currency),
                selectinload(models.LGRecord.lg_type),
                selectinload(models.LGRecord.lg_status),
                selectinload(models.LGRecord.lg_operational_status),
                selectinload(models.LGRecord.issuing_bank),
                selectinload(models.LGRecord.issuing_method),
                selectinload(models.LGRecord.applicable_rule),
                selectinload(models.LGRecord.internal_owner_contact),
                selectinload(models.LGRecord.lg_category),
                selectinload(models.LGRecord.documents),
                selectinload(models.LGRecord.instructions).selectinload(models.LGInstruction.documents), # NEW: Eager load documents for instructions
                selectinload(models.LGRecord.customer), # Eager load customer for name and email settings access
            )
            .first()
        )

    def get_all_lg_records_for_customer(
        self, db: Session, customer_id: int, skip: int = 0, limit: int = 100
    ) -> List[LGRecord]:
        return (
            db.query(self.model)
            .filter(self.model.customer_id == customer_id, self.model.is_deleted == False)
            .options(
                selectinload(models.LGRecord.beneficiary_corporate),
                selectinload(models.LGRecord.lg_currency),
                selectinload(models.LGRecord.lg_type),
                selectinload(models.LGRecord.lg_status),
                selectinload(models.LGRecord.issuing_bank),
                selectinload(models.LGRecord.internal_owner_contact),
                selectinload(models.LGRecord.lg_category),
                selectinload(models.LGRecord.documents),
                selectinload(models.LGRecord.instructions).selectinload(models.LGInstruction.documents), # NEW: Eager load documents for instructions
            )
            .offset(skip)
            .limit(limit)
            .all()
        )

    def create_document_model(self, obj_in: LGDocumentCreate, lg_record_id: int, uploaded_by_user_id: int) -> LGDocument:
        document_data = obj_in.model_dump()
        # Extract and remove lg_instruction_id from document_data before unpacking
        lg_instruction_id_from_obj_in = document_data.pop("lg_instruction_id", None)
        db_document = LGDocument(
            lg_record_id=lg_record_id,
            uploaded_by_user_id=uploaded_by_user_id,
            lg_instruction_id=lg_instruction_id_from_obj_in,
            **document_data,
        )
        return db_document

    def get_lg_records_for_renewal_reminder(
        self, db: Session, customer_id: int
    ) -> List[models.LGRecord]:
        logger.debug(f"[CRUDLGRecord.get_lg_records_for_renewal_reminder] Checking for renewal reminders for customer {customer_id}.")


        auto_renewal_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
            db, customer_id, GlobalConfigKey.AUTO_RENEWAL_DAYS_BEFORE_EXPIRY
        )
        force_renew_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
            db, customer_id, GlobalConfigKey.FORCED_RENEW_DAYS_BEFORE_EXPIRY
        )
        auto_renew_reminder_start_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
            db, customer_id, GlobalConfigKey.AUTO_RENEW_REMINDER_START_DAYS_BEFORE_EXPIRY
        )

        auto_renewal_days = int(auto_renewal_days_config.get('effective_value', 30)) if auto_renewal_days_config else 30
        force_renew_days = int(force_renew_days_config.get('effective_value', 15)) if force_renew_days_config else 15
        auto_renew_reminder_start_days = int(auto_renew_reminder_start_days_config.get('effective_value', 10)) if auto_renew_reminder_start_days_config else 10

        current_date = datetime.now(EEST_TIMEZONE).date() # Still returns a date object, but from an aware datetime

        max_lookahead_days = max(auto_renewal_days, force_renew_days, auto_renew_reminder_start_days)

        lg_records = db.query(self.model).filter(
            self.model.customer_id == customer_id,
            self.model.is_deleted == False,
            self.model.lg_status_id == models.LgStatusEnum.VALID.value,
            self.model.expiry_date >= current_date, # Changed from models.LGRecord.expiry_date
            self.model.expiry_date <= (current_date + timedelta(days=max_lookahead_days)) # Changed from models.LGRecord.expiry_date
        ).options(
            selectinload(models.LGRecord.beneficiary_corporate), # Change here
            selectinload(models.LGRecord.lg_currency),          # Change here
            selectinload(models.LGRecord.lg_payable_currency),  # Change here
            selectinload(models.LGRecord.lg_type),              # Change here
            selectinload(models.LGRecord.lg_status),            # Change here
            selectinload(models.LGRecord.lg_operational_status),# Change here
            selectinload(models.LGRecord.issuing_bank),         # Change here
            selectinload(models.LGRecord.issuing_method),       # Change here
            selectinload(models.LGRecord.applicable_rule),      # Change here
            selectinload(models.LGRecord.internal_owner_contact),# Change here
            selectinload(models.LGRecord.lg_category),          # Change here
            selectinload(models.LGRecord.customer)              # Change here
        ).order_by(self.model.expiry_date.asc()).all() # Changed from models.LGRecord.expiry_date

        logger.debug(f"[CRUDLGRecord.get_lg_records_for_renewal_reminder] Found {len(lg_records)} LGs approaching expiry for customer {customer_id}.")
        return lg_records

    # NEW METHOD: Run Auto Renewal / Bulk Renewal
    async def run_auto_renewal_process(self, db: Session, user_id: int, customer_id: int) -> Tuple[int, Optional[bytes]]:
        """
        Identifies eligible LGs for auto-renewal and force-renewal,
        executes the extension process for each, generates individual instruction letters,
        sends individual email notifications, and finally produces a single consolidated PDF
        of all generated instruction letters for physical printing by the user.
        Bypasses Maker-Checker for individual extensions as it's a bulk operation.
        """
        logger.info(f"[CRUDLGRecord.run_auto_renewal_process] Initiating auto/bulk renewal process for customer {customer_id} by user {user_id}.")

        try:
            # Retrieve configurable thresholds for renewal eligibility 
            auto_renewal_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, customer_id, GlobalConfigKey.AUTO_RENEWAL_DAYS_BEFORE_EXPIRY
            )
            force_renewal_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, customer_id, GlobalConfigKey.FORCED_RENEW_DAYS_BEFORE_EXPIRY
            )

            auto_renewal_days = int(auto_renewal_days_config.get('effective_value', 30)) if auto_renewal_days_config else 30
            force_renewal_days = int(force_renewal_days_config.get('effective_value', 60)) if force_renewal_days_config else 60

            if auto_renewal_days <= 0 or force_renewal_days <= 0:
                logger.error(f"Invalid auto/force renewal days configuration for customer {customer_id}.")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Renewal configuration invalid. Please contact support.")

        except (ValueError, AttributeError, TypeError) as e:
            logger.error(f"Error retrieving or parsing renewal configuration for customer {customer_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve renewal configurations.")

        current_date = date.today()
        auto_renewal_cutoff_date = current_date + timedelta(days=auto_renewal_days)
        force_renewal_cutoff_date = current_date + timedelta(days=force_renewal_days)

        logger.info(f"DEBUG: Auto-renewal run for customer {customer_id}.")
        logger.info(f"DEBUG: Current Server Date (today): {current_date}")
        logger.info(f"DEBUG: Auto Renewal Days Before Expiry: {auto_renewal_days}")
        logger.info(f"DEBUG: Auto Renewal Cutoff Date (inclusive): {auto_renewal_cutoff_date}")
        logger.info(f"DEBUG: Force Renewal Days Before Expiry: {force_renewal_days}")
        logger.info(f"DEBUG: Force Renewal Cutoff Date (inclusive): {force_renewal_cutoff_date}")


        eligible_lgs: List[models.LGRecord] = []

        # 1. Identify Auto-Renewal LGs
        auto_renewal_lgs = db.query(self.model).filter(
            self.model.customer_id == customer_id,
            self.model.is_deleted == False,
            self.model.lg_status.has(models.LgStatus.id == models.LgStatusEnum.VALID.value), # Corrected to models.LgStatusEnum
            self.model.auto_renewal == True,
            # LGs for auto-renewal that are nearing their expiry
            models.LGRecord.expiry_date >= current_date,
            models.LGRecord.expiry_date <= auto_renewal_cutoff_date
        ).options(
            selectinload(models.LGRecord.beneficiary_corporate),
            selectinload(models.LGRecord.lg_currency),
            selectinload(models.LGRecord.issuing_bank),
            selectinload(models.LGRecord.internal_owner_contact),
            selectinload(models.LGRecord.lg_category),
            selectinload(models.LGRecord.customer),
            selectinload(models.LGRecord.lg_status)
        ).all()
        
        eligible_lgs.extend(auto_renewal_lgs)
        logger.debug(f"Found {len(auto_renewal_lgs)} auto-renewal eligible LGs for customer {customer_id}.")

        # 2. Identify Force-Renewal LGs (not auto-renewal, but nearing expiry for forced action)
        force_renewal_lgs = db.query(self.model).filter(
            self.model.customer_id == customer_id,
            self.model.is_deleted == False,
            self.model.lg_status.has(models.LgStatus.id == models.LgStatusEnum.VALID.value), # Corrected to models.LgStatusEnum
            self.model.auto_renewal == False, # Explicitly not auto-renewal
            # LGs for force-renewal that are nearing their expiry
            models.LGRecord.expiry_date >= current_date,
            models.LGRecord.expiry_date <= force_renewal_cutoff_date
        ).options(
            selectinload(models.LGRecord.beneficiary_corporate),
            selectinload(models.LGRecord.lg_currency),
            selectinload(models.LGRecord.issuing_bank),
            selectinload(models.LGRecord.internal_owner_contact),
            selectinload(models.LGRecord.lg_category),
            selectinload(models.LGRecord.customer),
            selectinload(models.LGRecord.lg_status)
        ).all()

        # Filter out any duplicates if an LG somehow meets both (though criteria should prevent this)
        # Or more likely, an LG could be in auto_renewal_lgs and also picked up here due to broad cutoff.
        # Ensure distinct LGs. Using a set for IDs is efficient.
        processed_lg_ids = {lg.id for lg in eligible_lgs}
        for lg in force_renewal_lgs:
            if lg.id not in processed_lg_ids:
                eligible_lgs.append(lg)
                processed_lg_ids.add(lg.id)

        logger.debug(f"Found {len(force_renewal_lgs)} force-renewal eligible LGs. Total distinct eligible LGs: {len(eligible_lgs)}.")


        if not eligible_lgs:
            logger.info(f"[CRUDLGRecord.run_auto_renewal_process] No eligible LGs found for auto/bulk renewal for customer {customer_id}.")
            return 0, None

        renewed_lg_count = 0
        all_generated_instruction_htmls = []
        renewed_lg_numbers = []

        for lg_record_to_renew in eligible_lgs:
            try:
                # Calculate new expiry date based on lg_period_months using relativedelta
                current_expiry_dt = lg_record_to_renew.expiry_date # This is already a datetime object from DB

                # Use relativedelta for accurate month addition, handling year rollovers and end-of-month correctly.
                # Convert the resulting datetime object back to a date object, as required by extend_lg.
                new_expiry_date_as_date = (current_expiry_dt + relativedelta(months=lg_record_to_renew.lg_period_months)).date()

                # Call extend_lg, which handles LG update, instruction creation, and email
                # CRITICAL: Pass None for approval_request_id to bypass Maker-Checker
                updated_lg, instruction_id, generated_html = await self.extend_lg( # MODIFIED TO CAPTURE HTML
                    db,
                    lg_record_to_renew.id,
                    new_expiry_date_as_date,
                    user_id # The user initiating the bulk process is the 'maker' for this automated extension
                )
                all_generated_instruction_htmls.append(generated_html)
                renewed_lg_count += 1
                renewed_lg_numbers.append(updated_lg.lg_number)
                logger.info(f"Successfully renewed LG {updated_lg.lg_number} (ID: {updated_lg.id}) to new expiry date {updated_lg.expiry_date.date()}. Instruction ID: {instruction_id}.")

            except HTTPException as e:
                logger.error(f"Skipping LG {lg_record_to_renew.lg_number} (ID: {lg_record_to_renew.id}) due to specific error during extension: {e.detail}", exc_info=True)
                # Do not re-raise, continue to process other LGs
            except Exception as e:
                # Ensure the traceback is properly logged for all unexpected errors
                logger.error(f"Skipping LG {lg_record_to_renew.lg_number} (ID: {lg_record_to_renew.id}) due to unexpected error during extension: {e}", exc_info=True)
                # Do not re-raise, continue to process other LGs


        if not all_generated_instruction_htmls:
            logger.info(f"[CRUDLGRecord.run_auto_renewal_process] No LGs were successfully renewed to generate a combined PDF for customer {customer_id}.")
            return 0, None

        # Combine all generated instruction HTMLs into a single document with page breaks
        consolidated_html_content = []
        for html_segment in all_generated_instruction_htmls:
            consolidated_html_content.append(html_segment)
            consolidated_html_content.append('<div style="page-break-after: always;"></div>')
        
        # Remove the last page break to avoid an empty page at the end
        if consolidated_html_content and consolidated_html_content[-1].startswith('<div style="page-break-after:'):
            consolidated_html_content.pop()

        final_consolidated_html = "".join(consolidated_html_content)

        # Generate the combined PDF
        combined_pdf_bytes: Optional[bytes] = None
        try:
            combined_pdf_bytes = await generate_pdf_from_html(
                final_consolidated_html,
                f"bulk_lg_renewal_customer_{customer_id}_{date.today().isoformat()}"
            )
            if not combined_pdf_bytes:
                raise Exception("generate_pdf_from_html returned None for combined PDF.")
            logger.info(f"Successfully generated combined PDF for {renewed_lg_count} renewed LGs.")
        except Exception as e:
            logger.error(f"Failed to generate consolidated PDF for bulk renewal for customer {customer_id}: {e}", exc_info=True)
            # Do not re-raise, proceed with logging the overall action

        # Log the overall bulk action 
        log_action(
            db,
            user_id=user_id,
            action_type=AUDIT_ACTION_TYPE_LG_BULK_REMINDER_INITIATED, # Reusing for bulk renewal as a placeholder, ideally a new constant for auto renewal
            entity_type="Customer",
            entity_id=customer_id,
            details={
                "action": "Bulk LG Renewal (Auto & Forced)",
                "renewed_lg_count": renewed_lg_count,
                "renewed_lg_numbers": renewed_lg_numbers,
                "auto_renewal_threshold_days": auto_renewal_days,
                "force_renewal_threshold_days": force_renewal_days,
                "combined_pdf_generated": combined_pdf_bytes is not None,
                "triggered_by_user": user_id
            },
            customer_id=customer_id,
            lg_record_id=None,
        )
        logger.info(f"Bulk LG Renewal process completed for customer {customer_id}. {renewed_lg_count} LGs renewed.")

        return renewed_lg_count, combined_pdf_bytes
        
    def get_active_lg_records_count_for_customer(self, db: Session, customer_id: int) -> int:
        """
        Retrieves the count of active LG records for a given customer.
        Active LGs are defined as those with a status of 'VALID'.
        """
        return db.query(models.LGRecord).filter(
            models.LGRecord.customer_id == customer_id,
            models.LGRecord.lg_status_id == models.LgStatusEnum.VALID.value, # Corrected to models.LgStatusEnum
            models.LGRecord.is_deleted == False
        ).count()

    async def _send_renewal_reminder_email(self,
                                           db: Session,
                                           lg_record: models.LGRecord,
                                           reminder_type: str, # "first" or "second"
                                           lg_type_context: str, # "auto-renew" or "non-auto-renew"
                                           days_until_expiry: int,
                                           subject_prefix: str, # e.g., "URGENT: "
                                           template_name: str,
                                           audit_action_type: str,
                                           is_urgent: bool = False # For styling like red font
                                           ):
        """
        Helper function to send renewal reminder emails to relevant recipients.
        """
        logger.info(f"Sending {reminder_type} renewal reminder for LG {lg_record.lg_number} (Type: {lg_type_context}).")

        email_settings_to_use, email_method_for_log = get_customer_email_settings(db, lg_record.customer_id)

        to_emails = [lg_record.internal_owner_contact.email] if lg_record.internal_owner_contact else []
        cc_emails = []

        # Add internal owner manager if exists
        if lg_record.internal_owner_contact and lg_record.internal_owner_contact.manager_email:
            cc_emails.append(lg_record.internal_owner_contact.manager_email)

        # Add all end users linked to the LG's customer
        end_users = db.query(models.User).filter(
            models.User.customer_id == lg_record.customer_id,
            models.User.role == models.UserRole.END_USER, # Corrected to models.UserRole
            models.User.is_deleted == False
        ).all()
        for user in end_users:
            if user.email:
                to_emails.append(user.email)

        # Add all corporate admins linked to the LG's customer
        corporate_admins = db.query(models.User).filter(
            models.User.customer_id == lg_record.customer_id,
            models.User.role == models.UserRole.CORPORATE_ADMIN, # Corrected to models.UserRole
            models.User.is_deleted == False
        ).all()
        for admin in corporate_admins:
            if admin.email:
                cc_emails.append(admin.email)

        to_emails = list(set(to_emails)) # Remove duplicates
        cc_emails = list(set(cc_emails) - set(to_emails)) # Remove duplicates and emails already in 'to' list

        if not to_emails and not cc_emails:
            logger.warning(f"No valid recipients found for LG {lg_record.lg_number} renewal reminder (type: {reminder_type}). Skipping email.")
            log_action(
                db,
                user_id=None,
                action_type="NOTIFICATION_FAILED",
                entity_type="LGRecord",
                entity_id=lg_record.id,
                details={
                    "reason": "No valid recipients for renewal reminder",
                    "reminder_type": reminder_type,
                    "lg_type_context": lg_type_context,
                },
                customer_id=lg_record.customer_id,
                lg_record_id=lg_record.id,
            )
            return

        notification_template = db.query(models.Template).filter(
            models.Template.action_type == template_name,
            models.Template.is_notification_template == True,
            models.Template.is_deleted == False,
            (models.Template.customer_id == lg_record.customer_id) | (models.Template.is_global == True)
        ).order_by(models.Template.is_global.desc(), models.Template.is_default.desc()).first() # Prefer customer-specific, then global, then default.

        if not notification_template:
            logger.error(f"Renewal reminder template '{template_name}' not found for customer {lg_record.customer.name} or globally. Cannot send renewal reminder for LG {lg_record.lg_number}.")
            log_action(
                db,
                user_id=None,
                action_type="NOTIFICATION_FAILED",
                entity_type="LGRecord",
                entity_id=lg_record.id,
                details={
                    "reason": f"Missing renewal reminder email template: '{template_name}'",
                    "reminder_type": reminder_type,
                    "lg_type_context": lg_type_context,
                },
                customer_id=lg_record.customer_id,
                lg_record_id=lg_record.id,
            )
            return

        template_data = {
            "lg_number": lg_record.lg_number,
            "lg_amount": float(lg_record.lg_amount),
            "lg_currency": lg_record.lg_currency.iso_code,
            "expiry_date": lg_record.expiry_date.strftime('%Y-%m-%d'),
            "days_until_expiry": days_until_expiry,
            "auto_renewal_status": "Enabled" if lg_record.auto_renewal else "Disabled",
            "lg_type": lg_record.lg_type.name,
            "lg_status": lg_record.lg_status.name,
            "customer_name": lg_record.customer.name,
            "platform_name": "Grow BD Treasury Management Platform",
            "current_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "internal_owner_email": lg_record.internal_owner_contact.email if lg_record.internal_owner_contact else "N/A",
            "is_urgent_style": "color: red; font-weight: bold;" if is_urgent else "", # For urgent styling
            "subject_prefix": subject_prefix # Pass prefix for potential use in template subject
        }
        template_data["lg_amount_formatted"] = f"{lg_record.lg_currency.symbol} {template_data['lg_amount']:,.2f}"

        email_subject = notification_template.subject
        email_body_html = notification_template.content

        for key, value in template_data.items():
            str_value = str(value) if value is not None else ""
            email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)
            email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)

        email_sent_successfully = await send_email(
            db=db,
            to_emails=to_emails,
            cc_emails=cc_emails,
            subject_template=email_subject,
            body_template=email_body_html,
            template_data=template_data,
            email_settings=email_settings_to_use,
            sender_name=lg_record.customer.name # Use customer name as sender
        )

        if email_sent_successfully:
            log_action(
                db,
                user_id=None,
                action_type=audit_action_type,
                entity_type="LGRecord",
                entity_id=lg_record.id,
                details={
                    "lg_number": lg_record.lg_number,
                    "reminder_type": reminder_type,
                    "lg_type_context": lg_type_context,
                    "days_until_expiry": days_until_expiry,
                    "recipients": to_emails,
                    "cc_recipients": cc_emails,
                    "email_subject": email_subject,
                    "email_method": email_method_for_log,
                    "is_urgent_email": is_urgent
                },
                customer_id=lg_record.customer_id,
                lg_record_id=lg_record.id,
            )
            logger.info(f"{reminder_type.capitalize()} renewal reminder email sent successfully for LG {lg_record.lg_number}.")
        else:
            log_action(
                db,
                user_id=None,
                action_type="NOTIFICATION_FAILED",
                entity_type="LGRecord",
                entity_id=lg_record.id,
                details={
                    "reason": "Email sending failed for renewal reminder",
                    "reminder_type": reminder_type,
                    "lg_type_context": lg_type_context,
                    "days_until_expiry": days_until_expiry,
                    "recipients": to_emails,
                    "cc_recipients": cc_emails,
                    "email_subject": email_subject,
                    "email_method": email_method_for_log,
                },
                customer_id=lg_record.customer_id,
                lg_record_id=lg_record.id,
            )
            logger.error(f"Failed to send {reminder_type} renewal reminder email for LG {lg_record.lg_number}.")

    async def run_renewal_reminders_to_users_and_admins(self, db: Session):
        """
        Feature 1: Sends renewal reminders to End Users and Corporate Admins.
        Checks for auto-renew and non-auto-renew LGs nearing expiry based on configurable thresholds.
        """
        logger.info("Starting renewal reminders to End Users & Corporate Admins background task.")
        current_datetime_aware = datetime.now(EEST_TIMEZONE)

        customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()
        if not customers:
            logger.info("No active customers found. Skipping renewal reminders.")
            return

        for customer in customers:
            try:
                # Fetch customer-specific or global configurations
                auto_renewal_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.AUTO_RENEWAL_DAYS_BEFORE_EXPIRY
                )
                forced_renew_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.FORCED_RENEW_DAYS_BEFORE_EXPIRY
                )
                first_reminder_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.RENEWAL_REMINDER_FIRST_THRESHOLD_DAYS
                )
                second_reminder_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.RENEWAL_REMINDER_SECOND_THRESHOLD_DAYS
                )

                auto_renewal_days = int(auto_renewal_days_config.get('effective_value', 30))
                forced_renew_days = int(forced_renew_days_config.get('effective_value', 15))
                first_reminder_offset_days = int(first_reminder_days_config.get('effective_value', 7)) # e.g., 7 days before auto_renewal_days
                second_reminder_offset_days = int(second_reminder_days_config.get('effective_value', 14)) # e.g., 14 days before auto_renewal_days

                current_date = date.today()

                # Get all relevant LGs for this customer that are VALID and not deleted
                # Eager load necessary relations for email content
                eligible_lgs = db.query(models.LGRecord).filter(
                    models.LGRecord.customer_id == customer.id,
                    models.LGRecord.is_deleted == False,
                    models.LGRecord.lg_status_id == models.LgStatusEnum.VALID.value, # Corrected to models.LgStatusEnum
                    models.LGRecord.expiry_date >= current_date # Only future expiring LGs
                ).options(
                    selectinload(models.LGRecord.internal_owner_contact),
                    selectinload(models.LGRecord.lg_category),
                    selectinload(models.LGRecord.customer),
                    selectinload(models.LGRecord.lg_currency),
                    selectinload(models.LGRecord.lg_type),
                    selectinload(models.LGRecord.lg_status)
                ).all()

                for lg in eligible_lgs:
                    days_until_expiry = (lg.expiry_date.date() - current_date).days

                    # Determine if a first reminder is due
                    is_first_reminder_due = False
                    if lg.auto_renewal and (days_until_expiry <= (auto_renewal_days - first_reminder_offset_days)):
                        is_first_reminder_due = True
                    elif not lg.auto_renewal and (days_until_expiry <= (forced_renew_days - first_reminder_offset_days)):
                        is_first_reminder_due = True

                    # Determine if a second (escalation) reminder is due
                    is_second_reminder_due = False
                    if lg.auto_renewal and (days_until_expiry <= (auto_renewal_days - second_reminder_offset_days)):
                        is_second_reminder_due = True
                    elif not lg.auto_renewal and (days_until_expiry <= (forced_renew_days - second_reminder_offset_days)):
                        is_second_reminder_due = True

                    # Retrieve the last sent reminder timestamp from AuditLog
                    # We need to query audit logs to avoid sending duplicate reminders within the same reminder window.
                    # This approach assumes that a renewal reminder is logged with the lg_record_id and specific audit_action_type
                    # and that we only need to check the most recent reminder of that type.
                    last_first_reminder = db.query(models.AuditLog).filter(
                        models.AuditLog.lg_record_id == lg.id,
                        models.AuditLog.action_type == AUDIT_ACTION_TYPE_LG_RENEWAL_REMINDER_FIRST_SENT
                    ).order_by(models.AuditLog.timestamp.desc()).first()

                    last_second_reminder = db.query(models.AuditLog).filter(
                        models.AuditLog.lg_record_id == lg.id,
                        models.AuditLog.action_type == AUDIT_ACTION_TYPE_LG_RENEWAL_REMINDER_SECOND_SENT
                    ).order_by(models.AuditLog.timestamp.desc()).first()

                    # Logic to send reminders:
                    # Send second reminder only if it's due and a second reminder hasn't been sent,
                    # or if the last second reminder was more than 7 days ago (to allow periodic re-reminders if needed, though usually one is enough for escalation).
                    # For this task, we will consider it sent if recorded once.
                    if is_second_reminder_due and (not last_second_reminder or (current_datetime_aware - last_second_reminder.timestamp).days >= 7):
                        await self._send_renewal_reminder_email(
                            db, lg, "second", "auto-renew" if lg.auto_renewal else "non-auto-renew",
                            days_until_expiry, "URGENT: ", ACTION_TYPE_LG_RENEWAL_REMINDER_SECOND,
                            AUDIT_ACTION_TYPE_LG_RENEWAL_REMINDER_SECOND_SENT, is_urgent=True
                        )
                    # Send first reminder only if it's due, and a second reminder is NOT due yet,
                    # and a first reminder hasn't been sent, or if the last first reminder was more than 7 days ago.
                    elif is_first_reminder_due and not is_second_reminder_due and \
                         (not last_first_reminder or (current_datetime_aware - last_first_reminder.timestamp).days >= 7):
                        await self._send_renewal_reminder_email(
                            db, lg, "first", "auto-renew" if lg.auto_renewal else "non-auto-renew",
                            days_until_expiry, "", ACTION_TYPE_LG_RENEWAL_REMINDER_FIRST,
                            AUDIT_ACTION_TYPE_LG_RENEWAL_REMINDER_FIRST_SENT, is_urgent=False
                        )

            except Exception as e:
                db.rollback()
                logger.error(f"Error processing renewal reminders for customer {customer.id} ({customer.name}): {e}", exc_info=True)
                log_action(
                    db,
                    user_id=None,
                    action_type="TASK_PROCESSING_FAILED",
                    entity_type="Customer",
                    entity_id=customer.id,
                    details={"reason": f"Unhandled error in renewal reminders task: {e}", "task": "Renewal Reminders to Users/Admins"},
                    customer_id=customer.id,
                    lg_record_id=None
                )
            finally:
                db.commit() # Commit after each customer's reminders are processed

        logger.info("Finished renewal reminders to End Users & Corporate Admins background task.")

    async def run_internal_owner_renewal_reminders(self, db: Session):
        """
        Feature 2: Sends renewal reminders to Internal Owners and related contacts for NON-AUTO-RENEW LGs.
        Sends follow-up reminders until action is recorded.
        """
        logger.info("Starting internal owner renewal reminders background task for non-auto-renew LGs.")
        current_date_aware = datetime.now(EEST_TIMEZONE) # Use EEST_TIMEZONE or import from shared constant

        customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()
        if not customers:
            logger.info("No active customers found. Skipping internal owner renewal reminders.")
            return

        for customer in customers:
            try:
                auto_renew_reminder_start_days_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.AUTO_RENEW_REMINDER_START_DAYS_BEFORE_EXPIRY
                )
                number_of_days_for_next_reminder_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.NUMBER_OF_DAYS_FOR_NEXT_REMINDER
                )

                initial_reminder_days_before_expiry = int(auto_renew_reminder_start_days_config.get('effective_value', 60))
                follow_up_reminder_interval_days = int(number_of_days_for_next_reminder_config.get('effective_value', 7))

                current_date = datetime.now() # Use datetime for precise comparison with last_renewal_reminder_sent_at

                eligible_lgs = db.query(models.LGRecord).filter(
                    models.LGRecord.customer_id == customer.id,
                    models.LGRecord.is_deleted == False,
                    models.LGRecord.lg_status_id == models.LgStatusEnum.VALID.value, # Corrected to models.LgStatusEnum
                    models.LGRecord.auto_renewal == False, # Only non-auto-renew LGs
                    models.LGRecord.expiry_date >= current_date.date(), # Only future expiring LGs
                    models.LGRecord.expiry_date <= (current_date.date() + timedelta(days=initial_reminder_days_before_expiry))
                ).options(
                    selectinload(models.LGRecord.internal_owner_contact),
                    selectinload(models.LGRecord.lg_category),
                    selectinload(models.LGRecord.customer),
                    selectinload(models.LGRecord.lg_currency),
                    selectinload(models.LGRecord.lg_type),
                    selectinload(models.LGRecord.lg_status),
                    selectinload(models.LGRecord.instructions) # Load instructions to check for relevant actions
                ).all()

                for lg in eligible_lgs:
                    days_until_expiry = (lg.expiry_date.date() - current_date.date()).days

                    # Check if a relevant action (Extend, Release, Liquidate) has been recorded since last reminder
                    # Or, more simply, check if such an action exists at all.
                    # For Feature 2, the reminders *stop* if these actions are recorded.
                    relevant_actions_recorded = db.query(models.AuditLog).filter(
                        models.AuditLog.lg_record_id == lg.id,
                        models.AuditLog.action_type.in_([
                            ACTION_TYPE_LG_EXTEND, # Changed from AUDIT_ACTION_TYPE_LG_EXTENDED
                            ACTION_TYPE_LG_RELEASE, # Changed from AUDIT_ACTION_TYPE_LG_RELEASED
                            ACTION_TYPE_LG_LIQUIDATE # Covers both full/partial
                        ])
                    ).order_by(models.AuditLog.timestamp.desc()).first()

                    if relevant_actions_recorded:
                        logger.info(f"LG {lg.lg_number} has recorded a relevant action ({relevant_actions_recorded.action_type}). Skipping further internal owner renewal reminders.")
                        continue # Skip sending reminders if action has been taken

                    # Retrieve the last internal owner renewal reminder sent via ApprovalRequest.last_renewal_reminder_sent_at
                    # or from AuditLog if ApprovalRequest isn't the right place (which it isn't for *this* reminder type based on doc).
                    # Instead of ApprovalRequest, we query AuditLog directly for this specific reminder type.
                    last_owner_reminder_log = db.query(models.AuditLog).filter(
                        models.AuditLog.lg_record_id == lg.id,
                        models.AuditLog.action_type == AUDIT_ACTION_TYPE_LG_OWNER_RENEWAL_REMINDER_SENT
                    ).order_by(models.AuditLog.timestamp.desc()).first()

                    should_send_reminder = False
                    if not last_owner_reminder_log:
                        # Send initial reminder if within the window and no reminder sent yet
                        if days_until_expiry <= initial_reminder_days_before_expiry:
                            should_send_reminder = True
                    else:
                        # Send follow-up reminder if enough days have passed since the last one
                        if (current_date_aware - last_owner_reminder_log.timestamp).days >= follow_up_reminder_interval_days:
                            should_send_reminder = True
                        else:
                            logger.info(f"LG {lg.lg_number}: Internal owner reminder sent recently ({last_owner_reminder_log.timestamp.strftime('%Y-%m-%d %H:%M:%S')}). Skipping this run.")
                            log_action( # Log that a reminder was skipped due to recent sending
                                db, user_id=None, action_type=AUDIT_ACTION_TYPE_LG_OWNER_RENEWAL_REMINDER_SKIPPED_RECENTLY_SENT,
                                entity_type="LGRecord", entity_id=lg.id,
                                details={"lg_number": lg.lg_number, "reason": "Reminder sent recently", "last_sent_at": last_owner_reminder_log.timestamp.isoformat()},
                                customer_id=lg.customer_id, lg_record_id=lg.id
                            )


                    if should_send_reminder:
                        await self._send_internal_owner_renewal_reminder_email(
                            db, lg, days_until_expiry,
                            ACTION_TYPE_LG_REMINDER_TO_INTERNAL_OWNER,
                            AUDIT_ACTION_TYPE_LG_OWNER_RENEWAL_REMINDER_SENT
                        )
                        # Update the last_renewal_reminder_sent_at on the LGRecord itself for simpler tracking
                        # or on the ApprovalRequest if it was tied to the LG lifecycle, which it's not directly here.
                        # For now, just logging the audit action is enough for "sent" status.

            except Exception as e:
                db.rollback()
                logger.error(f"Error processing internal owner renewal reminders for customer {customer.id} ({customer.name}): {e}", exc_info=True)
                log_action(
                    db,
                    user_id=None,
                    action_type="TASK_PROCESSING_FAILED",
                    entity_type="Customer",
                    entity_id=customer.id,
                    details={"reason": f"Unhandled error in internal owner renewal reminders task: {e}", "task": "Internal Owner Renewal Reminders"},
                    customer_id=customer.id,
                    lg_record_id=None
                )
            finally:
                db.commit() # Commit after each customer's reminders are processed

        logger.info("Finished internal owner renewal reminders background task.")

    async def _send_internal_owner_renewal_reminder_email(self,
                                                           db: Session,
                                                           lg_record: models.LGRecord,
                                                           days_until_expiry: int,
                                                           template_name: str,
                                                           audit_action_type: str):
        """
        Helper function to send internal owner renewal reminder emails.
        """
        logger.info(f"Sending internal owner renewal reminder for LG {lg_record.lg_number}.")

        email_settings_to_use, email_method_for_log = get_customer_email_settings(db, lg_record.customer_id)

        to_emails = []
        if lg_record.internal_owner_contact and lg_record.internal_owner_contact.email:
            to_emails.append(lg_record.internal_owner_contact.email)
        
        cc_emails = []
        if lg_record.internal_owner_contact and lg_record.internal_owner_contact.manager_email:
            cc_emails.append(lg_record.internal_owner_contact.manager_email)

        # Add common communication list
        common_comm_list_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
            db, lg_record.customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
        )
        if common_comm_list_config and common_comm_list_config.get('effective_value'):
            try:
                parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                    cc_emails.extend(parsed_common_list)
            except json.JSONDecodeError:
                logger.warning(f"COMMON_COMMUNICATION_LIST for customer {lg_record.customer_id} is not a valid JSON list of emails. Skipping.")

        # Add category communication list
        if lg_record.lg_category and lg_record.lg_category.communication_list:
            cc_emails.extend(lg_record.lg_category.communication_list)

        # Add end users and corporate admins (as per Feature 2 requirements for non-auto-renew LGs)
        end_users = db.query(models.User).filter(
            models.User.customer_id == lg_record.customer_id,
            models.User.role == models.UserRole.END_USER, # Corrected to models.UserRole
            models.User.is_deleted == False
        ).all()
        for user in end_users:
            if user.email:
                to_emails.append(user.email)

        corporate_admins = db.query(models.User).filter(
            models.User.customer_id == lg_record.customer_id,
            models.User.role == models.UserRole.CORPORATE_ADMIN, # Corrected to models.UserRole
            models.User.is_deleted == False
        ).all()
        for admin in corporate_admins:
            if admin.email:
                cc_emails.append(admin.email)

        to_emails = list(set(to_emails)) # Remove duplicates
        cc_emails = list(set(cc_emails) - set(to_emails)) # Remove duplicates and emails already in 'to' list

        if not to_emails and not cc_emails:
            logger.warning(f"No valid recipients found for LG {lg_record.lg_number} internal owner renewal reminder. Skipping email.")
            log_action(
                db,
                user_id=None,
                action_type="NOTIFICATION_FAILED",
                entity_type="LGRecord",
                entity_id=lg_record.id,
                details={
                    "reason": "No valid recipients for internal owner renewal reminder",
                    "lg_type_context": "non-auto-renew",
                },
                customer_id=lg_record.customer_id,
                lg_record_id=lg_record.id,
            )
            return

        notification_template = db.query(models.Template).filter(
            models.Template.action_type == template_name,
            models.Template.is_notification_template == True,
            models.Template.is_deleted == False,
            (models.Template.customer_id == lg_record.customer_id) | (models.Template.is_global == True)
        ).order_by(models.Template.is_global.desc(), models.Template.is_default.desc()).first()

        if not notification_template:
            logger.error(f"Internal owner renewal reminder template '{template_name}' not found for customer {lg_record.customer.name} or globally. Cannot send reminder for LG {lg_record.lg_number}.")
            log_action(
                db,
                user_id=None,
                action_type="NOTIFICATION_FAILED",
                entity_type="LGRecord",
                entity_id=lg_record.id,
                details={
                    "reason": f"Missing internal owner renewal reminder email template: '{template_name}'",
                    "lg_type_context": "non-auto-renew",
                },
                customer_id=lg_record.customer_id,
                lg_record_id=lg_record.id,
            )
            return

        template_data = {
            "lg_number": lg_record.lg_number,
            "lg_amount": float(lg_record.lg_amount),
            "lg_currency": lg_record.lg_currency.iso_code,
            "expiry_date": lg_record.expiry_date.strftime('%Y-%m-%d'),
            "days_until_expiry": days_until_expiry,
            "lg_type": lg_record.lg_type.name,
            "lg_status": lg_record.lg_status.name,
            "customer_name": lg_record.customer.name,
            "platform_name": "Grow BD Treasury Management Platform",
            "current_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "internal_owner_email": lg_record.internal_owner_contact.email if lg_record.internal_owner_contact else "N/A",
            "manager_email": lg_record.internal_owner_contact.manager_email if lg_record.internal_owner_contact else "N/A",
            "is_urgent_style": "" # Not urgent by default for this reminder type
        }
        template_data["lg_amount_formatted"] = f"{lg_record.lg_currency.symbol} {template_data['lg_amount']:,.2f}"

        email_subject = notification_template.subject
        email_body_html = notification_template.content

        for key, value in template_data.items():
            str_value = str(value) if value is not None else ""
            email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)
            email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)

        email_sent_successfully = await send_email(
            db=db,
            to_emails=to_emails,
            cc_emails=cc_emails,
            subject_template=email_subject,
            body_template=email_body_html,
            template_data=template_data,
            email_settings=email_settings_to_use,
            sender_name=lg_record.customer.name # Use customer name as sender
        )

        if email_sent_successfully:
            log_action(
                db,
                user_id=None,
                action_type=audit_action_type,
                entity_type="LGRecord",
                entity_id=lg_record.id,
                details={
                    "lg_number": lg_record.lg_number,
                    "reminder_type": "internal_owner",
                    "days_until_expiry": days_until_expiry,
                    "recipients": to_emails,
                    "cc_recipients": cc_emails,
                    "email_subject": email_subject,
                    "email_method": email_method_for_log,
                },
                customer_id=lg_record.customer_id,
                lg_record_id=lg_record.id,
            )
            logger.info(f"Internal owner renewal reminder email sent successfully for LG {lg_record.lg_number}.")
        else:
            log_action(
                db,
                user_id=None,
                action_type="NOTIFICATION_FAILED",
                entity_type="LGRecord",
                entity_id=lg_record.id,
                details={
                    "reason": "Email sending failed for internal owner renewal reminder",
                    "reminder_type": "internal_owner",
                    "days_until_expiry": days_until_expiry,
                    "recipients": to_emails,
                    "cc_recipients": cc_emails,
                    "email_subject": email_subject,
                    "email_method": email_method_for_log,
                },
                customer_id=lg_record.customer_id,
                lg_record_id=lg_record.id,
            )
            logger.error(f"Failed to send internal owner renewal reminder email for LG {lg_record.lg_number}.")


    def get_all_lg_records_for_customer(
        self,
        db: Session,
        customer_id: int,
        internal_owner_contact_id: Optional[int] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[LGRecord]:
        """
        Retrieves all LG records for a given customer, with optional filtering
        by internal owner contact ID.
        """
        query = (
            db.query(self.model)
            .filter(self.model.customer_id == customer_id, self.model.is_deleted == False)
        )
        
        # NEW: Apply the filter if the parameter is provided
        if internal_owner_contact_id is not None:
            query = query.filter(self.model.internal_owner_contact_id == internal_owner_contact_id)

        # Apply eager loading to the filtered query
        query = query.options(
            selectinload(self.model.beneficiary_corporate),
            selectinload(self.model.lg_currency),
            selectinload(self.model.lg_payable_currency),
            selectinload(self.model.lg_type),
            selectinload(self.model.lg_status),
            selectinload(self.model.lg_operational_status),
            selectinload(self.model.issuing_bank),
            selectinload(self.model.issuing_method),
            selectinload(self.model.applicable_rule),
            selectinload(self.model.internal_owner_contact),
            selectinload(self.model.lg_category),
        )

        return query.offset(skip).limit(limit).all()

    async def create_from_migration(self, db: Session, obj_in: LGRecordCreate, customer_id: int, user_id: int, migration_source: str, migrated_from_staging_id: int) -> models.LGRecord:
        # Check for existing LG in production before creating a new one (proactive check)
        existing_lg = self.get_by_lg_number(db, obj_in.lg_number)
        if existing_lg:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"LG with number '{obj_in.lg_number}' already exists in production. Cannot migrate."
            )

        # Get the next LG sequence number
        last_lg_sequence = db.query(func.max(self.model.lg_sequence_number)).filter(
            self.model.beneficiary_corporate_id == obj_in.beneficiary_corporate_id,
            self.model.is_deleted == False
        ).scalar()
        next_lg_sequence = (last_lg_sequence if last_lg_sequence is not None else 0) + 1
        
        # We assume the obj_in already has the correct IDs for owner, bank, etc. from the staging process.
        lg_record_data = obj_in.model_dump(exclude_unset=True)
        lg_record_data["customer_id"] = customer_id
        lg_record_data["lg_sequence_number"] = next_lg_sequence
        
        # Set the migration-specific flags
        lg_record_data["migration_source"] = migration_source
        lg_record_data["migrated_from_staging_id"] = migrated_from_staging_id
        
        db_lg_record = self.model(**lg_record_data)
        db.add(db_lg_record)
        db.flush()
        
        db.refresh(db_lg_record)
        
        # Log the migration action for this specific record
        log_action(
            db,
            user_id=user_id,
            action_type="MIGRATION_IMPORT_RECORD",
            entity_type="LGRecord",
            entity_id=db_lg_record.id,
            details={
                "lg_number": db_lg_record.lg_number,
                "customer_id": customer_id,
                "staged_record_id": migrated_from_staging_id,
                "migration_source": migration_source,
            },
            customer_id=customer_id,
            lg_record_id=db_lg_record.id,
        )
        return db_lg_record

    def _get_recipient_details(self, db: Session, lg_record: models.LGRecord) -> Tuple[str, str]:
        """
        Determines the correct recipient name and address based on the LG's type and advising status.
        """
        # Debug print to confirm incoming data. This line can be removed once the issue is fixed.
        print(f"DEBUG: LG {lg_record.lg_number} - Advising Status: '{lg_record.advising_status}', Communication Bank ID: {lg_record.communication_bank_id}")

        # Case 1: Local Guarantee (issuing_bank is NOT "Foreign Bank")
        # This check is now robust and case-insensitive.
        if lg_record.issuing_bank.name and lg_record.issuing_bank.name.strip().upper() != "FOREIGN BANK":
            return lg_record.issuing_bank.name, lg_record.issuing_bank.address

        # Case 2: Counter Guarantee
        # Sub-case 2.1: Advised or Confirmed Counter Guarantee
        # Directly compare the AdvisingStatus enum object with its members.
        if lg_record.advising_status in [models.AdvisingStatus.ADVISED, models.AdvisingStatus.CONFIRMED]:
            if lg_record.communication_bank_id:
                try:
                    # Use the provided 'db' session to query the database.
                    communication_bank = db.query(models.Bank).get(lg_record.communication_bank_id)
                    if communication_bank:
                        recipient_name = communication_bank.name
                        recipient_address = communication_bank.address
                        return recipient_name, recipient_address
                    else:
                        # Fallback if the communication bank record is missing.
                        logger.error(f"LG {lg_record.lg_number} has advising status '{lg_record.advising_status}' but communication_bank_id {lg_record.communication_bank_id} does not match any bank record. Falling back to foreign bank details.")
                        return lg_record.foreign_bank_name, lg_record.foreign_bank_address
                except Exception as e:
                    # Handle potential database errors during the query.
                    logger.error(f"Error fetching communication bank for LG {lg_record.lg_number}: {e}. Falling back.")
                    return lg_record.foreign_bank_name, lg_record.foreign_bank_address
            else:
                # Fallback if communication bank ID is missing.
                logger.error(f"LG {lg_record.lg_number} has advising status '{lg_record.advising_status}' but no communication_bank_id defined. Falling back to foreign bank details.")
                return lg_record.foreign_bank_name, lg_record.foreign_bank_address

        # Sub-case 2.2: Unadvised or Unconfirmed Counter Guarantee
        else:  # Assumes AdvisingStatus.NOT_ADVISED or any other value
            recipient_name = lg_record.foreign_bank_name
            recipient_address = lg_record.foreign_bank_address
            return recipient_name, recipient_address
            
        return "To Whom It May Concern", "N/A" # Final default fallback