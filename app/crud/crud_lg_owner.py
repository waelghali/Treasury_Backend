# c:\Grow\app\crud\crud_lg_owner.py

from typing import List, Optional, Type, Dict, Any, Tuple
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func
from fastapi import HTTPException, status
from datetime import datetime
import json

# Corrected: Import app.models as models consistently
import app.models as models

# Corrected: Only import CRUDBase and log_action from app.crud.crud here
from app.crud.crud import CRUDBase, log_action

# Removed direct imports from old crud_lg_custody.py.
# All CRUD instance calls will now go through `crud_instances` late import.
# Only import models for type hints if needed, or if accessing attributes directly.
from app.models import (
    User, # Keep if used for type hints or specific queries outside models.py alias
    Customer,
    CustomerConfiguration,
    Template,
    ApprovalRequest,
    LGRecord, # Explicitly import LGRecord for type hinting in __init__
    InternalOwnerContact, # Explicitly import InternalOwnerContact for type hinting in __init__
)
from app.schemas.all_schemas import (
    InternalOwnerContactCreate,
    InternalOwnerContactUpdateDetails,
    LGRecordChangeOwner,
    LGInstructionCreate, # If used in _handle_lg_owner_change_notifications_and_instructions
)
from app.constants import (
    ACTION_TYPE_LG_CHANGE_OWNER_DETAILS,
    ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER,
    ACTION_TYPE_LG_CHANGE_BULK_LG_OWNER,
    AUDIT_ACTION_TYPE_LG_OWNER_DETAILS_UPDATED,
    AUDIT_ACTION_TYPE_LG_SINGLE_OWNER_CHANGED,
    AUDIT_ACTION_TYPE_LG_BULK_OWNER_CHANGED,
    GlobalConfigKey
)
from app.core.email_service import EmailSettings, get_global_email_settings, send_email, get_customer_email_settings
from app.core.document_generator import generate_pdf_from_html


# Configure logging for this module
import logging
logger = logging.getLogger(__name__)


class CRUDLGOwner(CRUDBase):
    # CRITICAL: Constructor for CRUDLGOwner should take the specific models it needs
    def __init__(self, lg_record_model: Type[models.LGRecord], internal_owner_contact_model: Type[models.InternalOwnerContact]):
        super().__init__(lg_record_model) # Inherit from CRUDBase using LGRecord model
        self.internal_owner_contact_model = internal_owner_contact_model # Store internal owner model

    async def update_internal_owner_details(
        self, db: Session, old_internal_owner_contact_id: int,
        obj_in: InternalOwnerContactUpdateDetails, user_id: int, customer_id: int
    ) -> models.InternalOwnerContact: # Corrected return type
        """
        Updates the details of an existing InternalOwnerContact record.
        This is Scenario 1: changing details of the contact itself.
        """
        logger.debug(f"[{self.__class__.__name__}.update_internal_owner_details] Attempting to update owner contact {old_internal_owner_contact_id} for customer {customer_id}.")
        try:
            # CRITICAL: Late import of app.crud.crud for instances
            import app.crud.crud as crud_instances

            db_owner = crud_instances.crud_internal_owner_contact.get(db, old_internal_owner_contact_id)
            if not db_owner or db_owner.customer_id != customer_id:
                logger.warning(f"[{self.__class__.__name__}.update_internal_owner_details] Owner contact {old_internal_owner_contact_id} not found or not accessible for customer {customer_id}.")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Internal owner contact not found or not accessible.")
            
            if obj_in.email and obj_in.email.lower() != db_owner.email.lower():
                existing_email_contact = crud_instances.crud_internal_owner_contact.get_by_email_for_customer(db, customer_id, obj_in.email)
                if existing_email_contact and existing_email_contact.id != db_owner.id:
                    logger.warning(f"[{self.__class__.__name__}.update_internal_owner_details] Conflict: Email '{obj_in.email}' already exists for another contact (ID: {existing_email_contact.id}).")
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"An internal owner with email '{obj_in.email}' already exists.")

            logger.debug(f"[{self.__class__.__name__}.update_internal_owner_details] Calling crud_internal_owner_contact.update for owner ID {db_owner.id}.")
            updated_owner = crud_instances.crud_internal_owner_contact.update(db, db_owner, obj_in, user_id=user_id) # Call update on the instance
            logger.debug(f"[{self.__class__.__name__}.update_internal_owner_details] crud_internal_owner_contact.update completed. Updated owner email: {updated_owner.email}.")

            log_action(
                db,
                user_id=user_id,
                action_type=AUDIT_ACTION_TYPE_LG_OWNER_DETAILS_UPDATED,
                entity_type="InternalOwnerContact",
                entity_id=updated_owner.id,
                details={
                    "owner_id": updated_owner.id,
                    "owner_email": updated_owner.email,
                    "changes": getattr(updated_owner, '_changed_fields_for_log', {})
                },
                customer_id=customer_id,
                lg_record_id=None,
            )
            logger.debug(f"[{self.__class__.__name__}.update_internal_owner_details] Audit log recorded for owner update.")
            return updated_owner
        except HTTPException:
            logger.error(f"[{self.__class__.__name__}.update_internal_owner_details] HTTPException occurred.", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"[{self.__class__.__name__}.update_internal_owner_details] An unexpected error occurred: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during internal owner details update: {e}")

    async def _handle_lg_owner_change_notifications_and_instructions(
        self, db: Session, lg_records: List[models.LGRecord], new_owner: models.InternalOwnerContact,
        old_owner: Optional[models.InternalOwnerContact], action_type_constant: str,
        audit_action_type_constant: str, user_id: int, approval_request_id: Optional[int],
        scope: str,
    ) -> List[int]:
        """
        Helper function to handle instruction generation and email notifications
        for LG internal owner changes (single or bulk).
        """
        logger.debug(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Handling notifications for {len(lg_records)} LGs, scope: {scope}.")
        instruction_ids = []
        
        # CRITICAL: Late import of app.crud.crud for instances
        import app.crud.crud as crud_instances

        instruction_maker_user_id = user_id
        if approval_request_id:
            approval_request = db.query(models.ApprovalRequest).filter(models.ApprovalRequest.id == approval_request_id).first()
            if approval_request:
                instruction_maker_user_id = approval_request.maker_user_id
            else:
                logger.warning(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] ApprovalRequest with ID {approval_request_id} not found when creating LGInstruction for owner change. Using checker_user_id as maker for instruction.")

        customer_id = lg_records[0].customer_id if lg_records else None
        if not customer_id:
            logger.error(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Cannot send notifications: No customer ID found for LG records.")
            return []

        email_settings_to_use: EmailSettings
        email_method_for_log: str
        try:
            email_settings_to_use, email_method_for_log = get_customer_email_settings(db, customer_id)
            logger.debug(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Email settings method: {email_method_for_log}")
        except Exception as e:
            email_settings_to_use = get_global_email_settings()
            email_method_for_log = "global_fallback_due_to_error"
            logger.warning(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Failed to retrieve customer-specific email settings for customer ID {customer_id}: {e}. Falling back to global settings.", exc_info=True)

        to_emails = [new_owner.email]
        cc_emails = []
        if new_owner.manager_email:
            cc_emails.append(new_owner.manager_email)
        if old_owner and old_owner.email != new_owner.email:
            cc_emails.append(old_owner.email)
            if old_owner.manager_email and old_owner.manager_email != new_owner.manager_email:
                cc_emails.append(old_owner.manager_email)

        # CRITICAL: Use `crud_instances.crud_customer_configuration` for getting config
        common_comm_list_config = crud_instances.crud_customer_configuration.get_customer_config_or_global_fallback(
            db, customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
        )
        if common_comm_list_config and common_comm_list_config.get('effective_value'):
            try:
                parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                    cc_emails.extend(parsed_common_list)
            except json.JSONDecodeError:
                logger.warning(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] COMMON_COMMUNICATION_LIST for customer {customer_id} is not a valid JSON list of emails. Skipping.", exc_info=True)
        cc_emails = list(set(cc_emails))

        notification_template = db.query(models.Template).filter(
            models.Template.action_type == action_type_constant,
            models.Template.is_global == True,
            models.Template.is_notification_template == True,
            models.Template.is_deleted == False
        ).first()

        if not notification_template:
            logger.error(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Notification template for action '{action_type_constant}' not found. Skipping email notification.")
            log_action(
                db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord",
                entity_id=lg_records[0].id if lg_records else None,
                details={"recipient": to_emails, "subject": "N/A", "reason": f"Notification template for {action_type_constant} not found", "method": "none"},
                customer_id=customer_id, lg_record_id=lg_records[0].id if lg_records else None,
            )
        else:
            customer_obj = db.query(models.Customer).filter(models.Customer.id == customer_id).first()
            customer_name = customer_obj.name if customer_obj else "N/A"
            lg_numbers = ", ".join([lg.lg_number for lg in lg_records])

            template_data = {
                "new_owner_email": new_owner.email,
                "new_owner_name": new_owner.email.split('@')[0],
                "new_owner_phone": new_owner.phone_number,
                "old_owner_email": old_owner.email if old_owner else "N/A",
                "old_owner_name": old_owner.email.split('@')[0] if old_owner else "N/A",
                "lg_numbers": lg_numbers,
                "number_of_lgs_affected": len(lg_records),
                "customer_name": customer_name,
                "current_date": datetime.now().strftime("%Y-%m-%d"),
                "action_type": "LG Owner Change",
                "platform_name": "Treasury Management Platform",
                "scope": scope.replace('_', ' ').title(),
            }
            if scope == "single_lg" and lg_records:
                template_data["lg_amount"] = float(lg_records[0].lg_amount)
                template_data["lg_currency"] = lg_records[0].lg_currency.iso_code
                template_data["issuing_bank_name"] = lg_records[0].issuing_bank.name
                template_data["lg_beneficiary_name"] = lg_records[0].beneficiary_corporate.entity_name
                template_data["lg_number_single"] = lg_records[0].lg_number
                template_data["lg_amount_formatted"] = f"{lg_records[0].lg_currency.symbol} {float(lg_records[0].lg_amount):,.2f}"

            email_subject = notification_template.subject if notification_template.subject else f"LG Owner Change Notification - {{scope}} for {{lg_numbers}}"
            email_body_html = notification_template.content
            
            for key, value in template_data.items():
                str_value = str(value) if value is not None else ""
                email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
                email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

            logger.debug(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Attempting to send email to: {to_emails}, CC: {cc_emails}, Subject: {email_subject}")
            email_sent_successfully = send_email(
                db=db,
                to_emails=to_emails,
                cc_emails=cc_emails,
                subject_template=email_subject,
                body_template=email_body_html,
                template_data=template_data,
                email_settings=email_settings_to_use,
            )

            if email_sent_successfully:
                log_action(
                    db, user_id=user_id, action_type="NOTIFICATION_SENT", entity_type="LGRecord",
                    entity_id=lg_records[0].id if lg_records else None,
                    details={"recipient": to_emails, "cc_recipients": cc_emails, "subject": email_subject, "method": email_method_for_log, "scope": scope},
                    customer_id=customer_id, lg_record_id=lg_records[0].id if lg_records else None,
                )
                logger.debug(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Email notification sent successfully.")
            else:
                log_action(
                    db, user_id=user_id, action_type="NOTIFICATION_FAILED", entity_type="LGRecord",
                    entity_id=lg_records[0].id if lg_records else None,
                    details={"recipient": to_emails, "cc_recipients": cc_emails, "subject": email_subject, "reason": "Email service failed to send notification", "method": email_method_for_log, "scope": scope},
                    customer_id=customer_id, lg_record_id=lg_records[0].id if lg_records else None,
                )
                logger.error(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Failed to send owner change notification for scope '{scope}' for LGs: {lg_numbers}.", exc_info=True)

        instruction_template = db.query(models.Template).filter(
            models.Template.action_type == action_type_constant,
            models.Template.is_global == True,
            models.Template.is_notification_template == False,
            models.Template.is_deleted == False
        ).first()

        if instruction_template:
            for lg_record in lg_records:
                logger.debug(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Generating instruction for LG: {lg_record.lg_number}")
                
                # CRITICAL: Late import for crud_lg_instruction instance
                import app.crud.crud as crud_instances
                
                instruction_serial_number = crud_instances.crud_lg_instruction.get_next_serial_number(db, lg_record.id)
                
                instruction_details = {
                    "lg_number": lg_record.lg_number,
                    "old_owner_email": old_owner.email if old_owner else "N/A",
                    "new_owner_email": new_owner.email,
                    "new_owner_phone": new_owner.phone_number,
                    "new_owner_internal_id": new_owner.internal_id,
                    "new_owner_manager_email": new_owner.manager_email,
                    "current_date": datetime.now().strftime("%Y-%m-%d"),
                    "customer_name": lg_record.customer.name,
                    "platform_name": "Treasury Management Platform",
                    "instruction_serial": instruction_serial_number,
                    "lg_amount": float(lg_record.lg_amount),
                    "lg_currency": lg_record.lg_currency.iso_code,
                    "issuing_bank_name": lg_record.issuing_bank.name,
                }
                instruction_details["lg_amount_formatted"] = f"{lg_record.lg_currency.symbol} {float(lg_record.lg_amount):,.2f}"

                generated_instruction_html = instruction_template.content
                for key, value in instruction_details.items():
                    str_value = str(value) if value is not None else ""
                    generated_instruction_html = generated_instruction_html.replace(f"{{{{{key}}}}}", str_value)

                try:
                    generated_pdf_bytes = await generate_pdf_from_html(generated_instruction_html, f"lg_owner_change_{lg_record.lg_number}_instruction")
                    generated_content_path = None
                    
                    db_lg_instruction = crud_instances.crud_lg_instruction.create(
                        db,
                        obj_in=LGInstructionCreate(
                            lg_record_id=lg_record.id,
                            instruction_type=action_type_constant,
                            serial_number=instruction_serial_number,
                            template_id=instruction_template.id,
                            status="Instruction Issued",
                            details=instruction_details,
                            maker_user_id=instruction_maker_user_id,
                            checker_user_id=user_id if approval_request_id else None,
                            approval_request_id=approval_request_id,
                            generated_content_path=generated_content_path,
                        )
                    )
                    db.flush()
                    instruction_ids.append(db_lg_instruction.id)
                    logger.debug(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Instruction {db_lg_instruction.serial_number} created and flushed.")
                except Exception as e:
                    logger.error(f"[{self.__class__.__name__}._handle_lg_owner_change_notifications_and_instructions] Error generating PDF or creating instruction for LG {lg_record.lg_number} owner change: {e}", exc_info=True)

        return instruction_ids


    async def change_lg_internal_owner_single_or_bulk(
        self, db: Session, change_in: LGRecordChangeOwner, user_id: int, customer_id: int, approval_request_id: Optional[int]
    ) -> List[models.LGRecord]: # Corrected return type
        """
        Handles changing the internal owner for a single LG or multiple LGs based on scope.
        This is Scenario 2 & 3.
        """
        logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Starting change for scope: {change_in.change_scope}, user_id: {user_id}, customer_id: {customer_id}.")
        new_owner_contact: Optional[models.InternalOwnerContact] = None
        
        # CRITICAL: Late import for crud_internal_owner_contact instance
        import app.crud.crud as crud_instances

        if change_in.new_internal_owner_contact_id:
            logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] New owner specified by ID: {change_in.new_internal_owner_contact_id}")
            new_owner_contact = crud_instances.crud_internal_owner_contact.get(db, change_in.new_internal_owner_contact_id)
            if not new_owner_contact or new_owner_contact.customer_id != customer_id:
                logger.warning(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] New owner contact {change_in.new_internal_owner_contact_id} not found or not accessible.")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="New internal owner contact not found or not accessible.")
        elif change_in.new_internal_owner_contact_details:
            logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] New owner specified by details: {change_in.new_internal_owner_contact_details.email}")
            new_owner_contact = crud_instances.crud_internal_owner_contact.create_or_get(
                db, obj_in=change_in.new_internal_owner_contact_details, customer_id=customer_id, user_id=user_id
            )
        
        if not new_owner_contact:
            logger.error(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] New internal owner contact could not be determined from input.")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="New internal owner contact could not be determined.")

        affected_lgs: List[models.LGRecord] = []
        old_owner_contact: Optional[models.InternalOwnerContact] = None

        try:
            if change_in.change_scope == "single_lg" and change_in.lg_record_id:
                logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Processing single LG change for record ID: {change_in.lg_record_id}")
                
                # Using crud_instances.crud_lg_record for consistency
                lg_record = crud_instances.crud_lg_record.get_lg_record_with_relations(db, change_in.lg_record_id, customer_id) # Access via late import
                if not lg_record or lg_record.customer_id != customer_id or lg_record.is_deleted:
                    logger.warning(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] LG Record {change_in.lg_record_id} not found or not accessible.")
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found or not accessible.")
                
                old_owner_contact = lg_record.internal_owner_contact # This is an ORM object, fine to access
                
                if lg_record.internal_owner_contact_id == new_owner_contact.id:
                    logger.warning(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] LG record {lg_record.id} already assigned to this internal owner {new_owner_contact.email}.")
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="LG record already assigned to this internal owner.")

                logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Updating LG {lg_record.lg_number} owner from {old_owner_contact.email if old_owner_contact else 'N/A'} to {new_owner_contact.email}.")
                lg_record.internal_owner_contact_id = new_owner_contact.id
                lg_record.updated_at = func.now()
                db.add(lg_record)
                db.flush()
                db.refresh(lg_record)
                affected_lgs.append(lg_record)
                logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] LG {lg_record.id} owner updated and flushed. Affected LGs count: {len(affected_lgs)}")


                log_action(
                    db,
                    user_id=user_id,
                    action_type=AUDIT_ACTION_TYPE_LG_SINGLE_OWNER_CHANGED,
                    entity_type="LGRecord",
                    entity_id=lg_record.id,
                    details={
                        "lg_number": lg_record.lg_number,
                        "old_owner_id": old_owner_contact.id if old_owner_contact else None,
                        "old_owner_email": old_owner_contact.email if old_owner_contact else "N/A",
                        "new_owner_id": new_owner_contact.id,
                        "new_owner_email": new_owner_contact.email,
                        "reason": change_in.reason,
                    },
                    customer_id=customer_id,
                    lg_record_id=lg_record.id,
                )
                await self._handle_lg_owner_change_notifications_and_instructions(
                    db,
                    lg_records=[lg_record],
                    new_owner=new_owner_contact,
                    old_owner=old_owner_contact,
                    action_type_constant=ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER,
                    audit_action_type_constant=AUDIT_ACTION_TYPE_LG_SINGLE_OWNER_CHANGED,
                    user_id=user_id,
                    approval_request_id=approval_request_id,
                    scope="single_lg"
                )
                logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Notifications/instructions handled for single LG change.")


            elif change_in.change_scope == "all_by_old_owner" and change_in.old_internal_owner_contact_id:
                logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Processing bulk LG change from old owner ID: {change_in.old_internal_owner_contact_id}")
                old_owner_contact = crud_instances.crud_internal_owner_contact.get(db, change_in.old_internal_owner_contact_id) # Access via late import
                if not old_owner_contact or old_owner_contact.customer_id != customer_id:
                    logger.warning(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Old internal owner contact {change_in.old_internal_owner_contact_id} not found or not accessible.")
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Old internal owner contact not found or not accessible.")
                
                if old_owner_contact.id == new_owner_contact.id:
                    logger.warning(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Attempted to transfer LGs to the same internal owner: {old_owner_contact.email}.")
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot transfer LGs to the same internal owner.")

                lgs_to_update = db.query(models.LGRecord).filter(
                    models.LGRecord.internal_owner_contact_id == old_owner_contact.id,
                    models.LGRecord.customer_id == customer_id,
                    models.LGRecord.is_deleted == False
                ).all()

                if not lgs_to_update:
                    logger.warning(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] No active LG Records found for the specified old owner to perform bulk change.")
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No active LG records found for old internal owner '{old_owner_contact.email}'.")

                logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Found {len(lgs_to_update)} LGs to update for bulk change.")
                for lg_record in lgs_to_update:
                    lg_record.internal_owner_contact_id = new_owner_contact.id
                    lg_record.updated_at = func.now()
                    db.add(lg_record)
                    affected_lgs.append(lg_record)
                
                db.flush()
                for lg_record in affected_lgs:
                    db.refresh(lg_record)
                logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Bulk LG owner update completed and flushed. Affected LGs count: {len(affected_lgs)}")


                log_action(
                    db,
                    user_id=user_id,
                    action_type=AUDIT_ACTION_TYPE_LG_BULK_OWNER_CHANGED,
                    entity_type="LGRecord",
                    entity_id=None,
                    details={
                        "old_owner_id": old_owner_contact.id,
                        "old_owner_email": old_owner_contact.email,
                        "new_owner_id": new_owner_contact.id,
                        "new_owner_email": new_owner_contact.email,
                        "num_lgs_affected": len(affected_lgs),
                        "affected_lg_numbers": [lg.lg_number for lg in affected_lgs],
                        "reason": change_in.reason,
                    },
                    customer_id=customer_id,
                    lg_record_id=None,
                )
                await self._handle_lg_owner_change_notifications_and_instructions(
                    db,
                    lg_records=affected_lgs,
                    new_owner=new_owner_contact,
                    old_owner=old_owner_contact,
                    action_type_constant=ACTION_TYPE_LG_CHANGE_BULK_LG_OWNER,
                    audit_action_type_constant=AUDIT_ACTION_TYPE_LG_BULK_OWNER_CHANGED,
                    user_id=user_id,
                    approval_request_id=approval_request_id,
                    scope="bulk"
                )
                logger.debug(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Notifications/instructions handled for bulk LG change.")


            else:
                logger.error(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] Invalid change scope or missing required parameters: {change_in.change_scope}, lg_record_id: {change_in.lg_record_id}, old_internal_owner_contact_id: {change_in.old_internal_owner_contact_id}")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid change scope or missing required parameters.")
            
            return affected_lgs
        except HTTPException:
            logger.error(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] HTTPException occurred during owner change.", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"[{self.__class__.__name__}.change_lg_internal_owner_single_or_bulk] An unexpected error occurred during LG owner change: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during LG owner change: {e}")


# CRITICAL: This file should NOT instantiate crud_lg_owner at its end.
# crud_lg_owner = CRUDLGOwner(app.models.LGRecord) # THIS LINE MUST BE DELETED