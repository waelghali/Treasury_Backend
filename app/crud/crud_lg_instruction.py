# app/crud/crud_lg_instruction.py
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Type, Tuple
from fastapi import HTTPException, status, UploadFile
from sqlalchemy import func, desc, exists, and_, Integer, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload
import decimal

from app.crud.crud import CRUDBase, log_action
import app.models as models
from app.schemas.all_schemas import LGInstructionCreate, LGInstructionUpdate, LGInstructionRecordDelivery, LGInstructionRecordBankReply
from app.constants import (
    GlobalConfigKey, ACTION_TYPE_LG_DECREASE_AMOUNT, AUDIT_ACTION_TYPE_LG_DECREASED_AMOUNT,
    ACTION_TYPE_LG_RECORD_DELIVERY, AUDIT_ACTION_TYPE_LG_INSTRUCTION_DELIVERED,
    ACTION_TYPE_LG_RECORD_BANK_REPLY, AUDIT_ACTION_TYPE_LG_BANK_REPLY_RECORDED,
    ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT, AUDIT_ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT_SENT,
    ACTION_TYPE_LG_EXTEND, ACTION_TYPE_LG_RELEASE, ACTION_TYPE_LG_LIQUIDATE,
    ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE, ACTION_TYPE_LG_AMEND, ACTION_TYPE_LG_REMINDER_TO_BANKS,
    ACTION_TYPE_LG_CHANGE_OWNER_DETAILS, ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER, ACTION_TYPE_LG_CHANGE_BULK_LG_OWNER,
    AUDIT_ACTION_TYPE_LG_BULK_REMINDER_INITIATED,
    AUDIT_ACTION_TYPE_LG_AMENDED, AUDIT_ACTION_TYPE_LG_ACTIVATED,
    LgStatusEnum,
    AUDIT_ACTION_TYPE_LG_REMINDER_SENT_TO_BANK,
    InstructionTypeCode, SubInstructionCode, INSTRUCTION_TYPE_CODE_TO_FULL_ACTION_MAP
)

from app.core.email_service import EmailSettings, get_global_email_settings, send_email, get_customer_email_settings
from app.core.document_generator import generate_pdf_from_html

import logging
logger = logging.getLogger(__name__)

class CRUDLGInstruction(CRUDBase):
    def __init__(self, model: Type[models.LGInstruction], crud_lg_document_instance: Any, crud_customer_configuration_instance: Any):
        super().__init__(model)
        self.crud_lg_document_instance = crud_lg_document_instance
        self.crud_customer_configuration_instance = crud_customer_configuration_instance

    async def get_next_serial_number(
        self,
        db: Session,
        lg_record_id: int,
        entity_code: str,
        lg_category_code: str,
        lg_sequence_number: str,
        instruction_type_code: InstructionTypeCode,
        sub_instruction_code: SubInstructionCode
    ) -> Tuple[str, int, int]:
        """
        Generates the next unique instruction serial number and its sequence numbers.
        Returns: Tuple[serial_number_str, global_seq_int, type_seq_int]
        """
        entity_code = entity_code.upper()
        lg_category_code = lg_category_code.upper()
        instruction_type_code_val = instruction_type_code.value.upper()
        sub_instruction_code_val = sub_instruction_code.value.upper()

        lg_record = db.query(models.LGRecord).filter(models.LGRecord.id == lg_record_id, models.LGRecord.is_deleted == False).first()
        if not lg_record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Record not found for serial generation.")

        current_max_global_seq = db.query(func.max(self.model.global_seq_per_lg)).filter(
            self.model.lg_record_id == lg_record_id
        ).scalar()
        next_global_seq = (current_max_global_seq if current_max_global_seq is not None else 0) + 1
        global_seq_padded = str(next_global_seq).zfill(4)

        current_max_type_seq = db.query(func.max(self.model.type_seq_per_lg)).filter(
            self.model.lg_record_id == lg_record_id,
            self.model.instruction_type == INSTRUCTION_TYPE_CODE_TO_FULL_ACTION_MAP.get(instruction_type_code)
        ).scalar()
        next_type_seq = (current_max_type_seq if current_max_type_seq is not None else 0) + 1
        type_seq_padded = str(next_type_seq).zfill(3)

        logger.debug(f"DEBUG: get_next_serial_number for LG {lg_record_id}, Type {INSTRUCTION_TYPE_CODE_TO_FULL_ACTION_MAP.get(instruction_type_code)}:")
        logger.debug(f"  current_max_global_seq: {current_max_global_seq}, next_global_seq: {next_global_seq}")
        logger.debug(f"  current_max_type_seq: {current_max_type_seq}, next_type_seq: {next_type_seq}")

        new_serial = (
            f"{entity_code}"
            f"{lg_category_code.ljust(2, '_')}"
            f"{lg_sequence_number}"
            f"{instruction_type_code_val}"
            f"{global_seq_padded}"
            f"{type_seq_padded}"
            f"{sub_instruction_code_val}"
        )

        if db.query(exists().where(self.model.serial_number == new_serial)).scalar():
            logger.warning(f"Race condition detected: Generated serial '{new_serial}' already exists during get_next_serial_number. This should be handled by the retry loop.")
        
        return new_serial, next_global_seq, next_type_seq

    async def create(self, db: Session, obj_in: LGInstructionCreate, **kwargs: Any) -> models.LGInstruction:
        lg_record_id = obj_in.lg_record_id
        entity_code = kwargs.pop('entity_code')
        lg_category_code = kwargs.pop('lg_category_code')
        lg_sequence_number_str = kwargs.pop('lg_sequence_number_str')
        instruction_type_code_enum = kwargs.pop('instruction_type_code_enum')
        sub_instruction_code_enum = kwargs.pop('sub_instruction_code_enum')
        
        full_action_type_for_db = INSTRUCTION_TYPE_CODE_TO_FULL_ACTION_MAP.get(instruction_type_code_enum)
        if not full_action_type_for_db:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Internal error: No full action type mapping found for InstructionTypeCode: {instruction_type_code_enum.value}")

        for attempt in range(5):
            try:
                serial_number_generated, global_seq_val, type_seq_val = await self.get_next_serial_number(
                    db,
                    lg_record_id=lg_record_id,
                    entity_code=entity_code,
                    lg_category_code=lg_category_code,
                    lg_sequence_number=lg_sequence_number_str,
                    instruction_type_code=instruction_type_code_enum,
                    sub_instruction_code=sub_instruction_code_enum
                )

                obj_in.serial_number = serial_number_generated
                
                db_obj = self.model(
                    lg_record_id=lg_record_id,
                    instruction_type=full_action_type_for_db,
                    serial_number=obj_in.serial_number,
                    global_seq_per_lg=global_seq_val,
                    type_seq_per_lg=type_seq_val,
                    template_id=obj_in.template_id,
                    status=obj_in.status,
                    instruction_date=obj_in.instruction_date if obj_in.instruction_date else func.now(),
                    delivery_date=obj_in.delivery_date,
                    bank_reply_date=obj_in.bank_reply_date,
                    details=obj_in.details,
                    generated_content_path=obj_in.generated_content_path,
                    sent_to_bank=obj_in.sent_to_bank,
                    is_printed=obj_in.is_printed,
                    maker_user_id=obj_in.maker_user_id,
                    checker_user_id=obj_in.checker_user_id,
                    approval_request_id=obj_in.approval_request_id,
                    bank_reply_details=obj_in.bank_reply_details
                )
                db.add(db_obj)
                db.flush()
                db.refresh(db_obj)
                return db_obj

            except IntegrityError as e:
                db.rollback()
                logger.warning(f"UniqueViolation during instruction creation (Attempt {attempt+1}/5). Retrying serial generation. Error: {e.orig}")
                if attempt == 4:
                    logger.error(f"Failed to generate unique instruction serial after 5 attempts for LG {lg_record_id}, type {full_action_type_for_db}.")
                    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to generate a unique instruction serial number after multiple attempts. Please try again.")
            except Exception as e:
                db.rollback()
                logger.error(f"Unexpected error during instruction creation: {e}", exc_info=True)
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred during instruction creation: {e}")

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create instruction after multiple retries.")
        
    def update(self, db: Session, db_obj: models.LGInstruction, obj_in: LGInstructionUpdate) -> models.LGInstruction:
        update_data = obj_in.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_obj, field, value)
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)
        return db_obj

    async def record_instruction_delivery(
        self, db: Session, instruction_id: int, obj_in: LGInstructionRecordDelivery, user_id: int, customer_id: int, file_content: Optional[bytes] = None
    ) -> models.LGInstruction:
        logger.debug(f"[CRUDLGInstruction.record_instruction_delivery] Starting record delivery for instruction ID: {instruction_id}")
        db_instruction = db.query(self.model).options(
            selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.customer).selectinload(models.Customer.subscription_plan)
        ).filter(
            self.model.id == instruction_id,
            self.model.is_deleted == False
        ).first()
        if not db_instruction or not db_instruction.lg_record or db_instruction.lg_record.customer_id != customer_id:
            logger.warning(f"[CRUDLGInstruction.record_instruction_delivery] Instruction {instruction_id} not found or not accessible for customer {customer_id}.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Instruction not found or not accessible.")
        if db_instruction.status == "Instruction Delivered":
            logger.warning(f"[CRUDLGInstruction.record_instruction_delivery] Instruction {instruction_id} is already marked as delivered.")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instruction is already marked as delivered.")
        if db_instruction.delivery_date is not None:
             logger.warning(f"[CRUDLGInstruction.record_instruction_delivery] Delivery date already set for instruction {instruction_id}. Overwriting.")
        db_instruction.delivery_date = obj_in.delivery_date
        db_instruction.status = "Instruction Delivered"
        db.add(db_instruction)
        db.flush()

        if obj_in.delivery_document_file:
            customer_subscription_plan = db_instruction.lg_record.customer.subscription_plan
            if not customer_subscription_plan.can_image_storage:
                logger.warning(f"[CRUDLGInstruction.record_instruction_delivery] Customer's plan '{customer_subscription_plan.name}' does not support image storage. Delivery document will not be stored for instruction {instruction_id}.")
                log_action(
                    db,
                    user_id=user_id,
                    action_type="DOCUMENT_STORAGE_SKIPPED",
                    entity_type="LGInstruction",
                    entity_id=instruction_id,
                    details={"reason": "Subscription plan does not support image storage", "file_name": obj_in.delivery_document_file.file_name},
                    customer_id=customer_id,
                    lg_record_id=db_instruction.lg_record.id,
                )
            else:
                try:
                    db_document = await self.crud_lg_document_instance.create_document(
                        db,
                        obj_in=obj_in.delivery_document_file,
                        file_content=file_content,
                        lg_record_id=db_instruction.lg_record.id,
                        uploaded_by_user_id=user_id,
                        original_instruction_serial=db_instruction.serial_number
                    )
                    logger.debug(f"[CRUDLGInstruction.record_instruction_delivery] Delivery document '{db_document.file_name}' stored for instruction {instruction_id}.")
                except Exception as e:
                    logger.error(f"[CRUDLGInstruction.record_instruction_delivery] Failed to store delivery document for instruction {instruction_id}: {e}", exc_info=True)
                    log_action(
                        db,
                        user_id=user_id,
                        action_type="DOCUMENT_STORAGE_FAILED",
                        entity_type="LGInstruction",
                        entity_id=instruction_id,
                        details={"reason": f"Failed to store document: {e}", "file_name": obj_in.delivery_document_file.file_name, "document_type": "DELIVERY_PROOF"},
                        customer_id=customer_id,
                        lg_record_id=db_instruction.lg_record.id,
                    )
        log_action(
            db,
            user_id=user_id,
            action_type=ACTION_TYPE_LG_RECORD_DELIVERY,
            entity_type="LGInstruction",
            entity_id=instruction_id,
            details={
                "lg_number": db_instruction.lg_record.lg_number,
                "serial_number": db_instruction.serial_number,
                "instruction_serial": db_instruction.serial_number,
                "delivery_date": obj_in.delivery_date.isoformat(),
                "document_stored": obj_in.delivery_document_file.file_name if obj_in.delivery_document_file else False,
            },
            customer_id=customer_id,
            lg_record_id=db_instruction.lg_record.id,
        )
        logger.debug(f"[CRUDLGInstruction.record_instruction_delivery] Audit log recorded for instruction delivery.")
        db.refresh(db_instruction)
        return db_instruction


    async def record_bank_reply(
        self, db: Session, instruction_id: int, obj_in: LGInstructionRecordBankReply, user_id: int, customer_id: int, file_content: Optional[bytes] = None
    ) -> models.LGInstruction:
        logger.debug(f"[CRUDLGInstruction.record_bank_reply] Starting record bank reply for instruction ID: {instruction_id}")
        db_instruction = db.query(self.model).options(
            selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.customer).selectinload(models.Customer.subscription_plan)
        ).filter(
            self.model.id == instruction_id,
            self.model.is_deleted == False
        ).first()
        if not db_instruction or not db_instruction.lg_record or db_instruction.lg_record.customer_id != customer_id:
            logger.warning(f"[CRUDLGInstruction.record_bank_reply] Instruction {instruction_id} not found or not accessible for customer {customer_id}.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Instruction not found or not accessible.")
        if db_instruction.bank_reply_date is not None:
            logger.warning(f"[CRUDLGInstruction.record_bank_reply] Bank reply date already set for instruction {instruction_id}. Overwriting existing reply details.")
        db_instruction.bank_reply_date = obj_in.bank_reply_date
        db_instruction.bank_reply_details = obj_in.reply_details
        db_instruction.status = "Confirmed by Bank"
        db.add(db_instruction)
        db.flush()

        if obj_in.bank_reply_document_file:
            customer_subscription_plan = db_instruction.lg_record.customer.subscription_plan
            if not customer_subscription_plan.can_image_storage:
                logger.warning(f"[CRUDLGInstruction.record_bank_reply] Customer's plan '{customer_subscription_plan.name}' does not support image storage. Bank reply document will not be stored for instruction {instruction_id}.")
                log_action(
                    db,
                    user_id=user_id,
                    action_type="DOCUMENT_STORAGE_SKIPPED",
                    entity_type="LGInstruction",
                    entity_id=instruction_id,
                    details={"reason": "Subscription plan does not support image storage", "file_name": obj_in.bank_reply_document_file.file_name},
                    customer_id=customer_id,
                    lg_record_id=db_instruction.lg_record.id,
                )
            else:
                try:
                    db_document = await self.crud_lg_document_instance.create_document(
                        db,
                        obj_in=obj_in.bank_reply_document_file,
                        file_content=file_content,
                        lg_record_id=db_instruction.lg_record.id,
                        uploaded_by_user_id=user_id,
                        original_instruction_serial=db_instruction.serial_number
                    )
                    logger.debug(f"[CRUDLGInstruction.record_bank_reply] Bank reply document '{db_document.file_name}' stored for instruction {instruction_id}.")
                except Exception as e:
                    logger.error(f"[CRUDLGInstruction.record_bank_reply] Failed to store bank reply document for instruction {instruction_id}: {e}", exc_info=True)
                    log_action(
                        db,
                        user_id=user_id,
                        action_type="DOCUMENT_STORAGE_FAILED",
                        entity_type="LGInstruction",
                        entity_id=instruction_id,
                        details={"reason": f"Failed to store document: {e}", "file_name": obj_in.bank_reply_document_file.file_name, "document_type": "BANK_REPLY"},
                        customer_id=customer_id,
                        lg_record_id=db_instruction.lg_record.id,
                    )
        log_action(
            db,
            user_id=user_id,
            action_type=ACTION_TYPE_LG_RECORD_BANK_REPLY,
            entity_type="LGInstruction",
            entity_id=instruction_id,
            details={
                "lg_number": db_instruction.lg_record.lg_number,
                "serial_number": db_instruction.serial_number,
                "instruction_serial": db_instruction.serial_number,
                "bank_reply_date": obj_in.bank_reply_date.isoformat(),
                "reply_details": obj_in.reply_details,
                "document_stored": obj_in.bank_reply_document_file.file_name if obj_in.bank_reply_document_file else False,
                "new_instruction_status": db_instruction.status
            },
            customer_id=customer_id,
            lg_record_id=db_instruction.lg_record.id,
        )
        logger.debug(f"[CRUDLGInstruction.record_bank_reply] Audit log recorded for bank reply.")
        db.refresh(db_instruction)
        return db_instruction

    def get_undelivered_instructions_for_reporting(
        self, db: Session, customer_id: int, report_start_days: int, report_stop_days: int
    ) -> List[models.LGInstruction]:
        logger.debug(f"[CRUDLGInstruction.get_undelivered_instructions_for_reporting] Checking for undelivered instructions for customer {customer_id}.")
        logger.debug(f"Reporting window: instructions issued on or before today, and on or after {report_stop_days} days ago.")

        current_date = date.today()
        min_age_cutoff_date = current_date - timedelta(days=report_start_days)
        max_age_cutoff_date = current_date - timedelta(days=report_stop_days)
        date_for_oldest_to_include = current_date - timedelta(days=report_start_days)
        date_for_newest_to_exclude = current_date - timedelta(days=report_stop_days)

        logger.debug(f"Calculated date_for_oldest_to_include (start_days): {date_for_oldest_to_include}")
        logger.debug(f"Calculated date_for_newest_to_exclude (stop_days): {date_for_newest_to_exclude}")

        deliverable_instruction_types = [
            "LG_DECREASE_AMOUNT",
            "LG_EXTENSION",
            "LG_LIQUIDATE",
            "LG_RELEASE",
            "LG_ACTIVATE_NON_OPERATIVE",
            "LG_AMEND",
        ]

        query = db.query(self.model).options(
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_currency),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.issuing_bank),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.customer),
            selectinload(self.model.maker_user),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.internal_owner_contact),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_category),
            selectinload(self.model.template),
            selectinload(self.model.documents)
        ).filter(
            self.model.lg_record.has(models.LGRecord.customer_id == customer_id),
            self.model.is_deleted == False,
            self.model.delivery_date.is_(None),
            self.model.instruction_type.in_(deliverable_instruction_types),
            func.date(self.model.instruction_date) <= date_for_oldest_to_include,
            func.date(self.model.instruction_date) > date_for_newest_to_exclude,
            self.model.status == "Instruction Issued"
        ).order_by(self.model.instruction_date)

        logger.debug(f"Generated SQL Query (get_undelivered_instructions_for_reporting): {query.statement.compile(dialect=db.bind.dialect, compile_kwargs={'literal_binds': True})}")

        results = query.all()

        logger.debug(f"[CRUDLGInstruction.get_undelivered_instructions_for_reporting] Found {len(results)} undelivered instructions for customer {customer_id} based on refined date logic.")
        return results

    def get_instructions_for_bank_reminders(
        self,
        db: Session,
        customer_id: int,
        days_since_delivery: int,
        days_since_issuance: int,
        max_days_since_issuance: int
    ) -> List[models.LGInstruction]:
        logger.debug(f"[CRUDLGInstruction.get_instructions_for_bank_reminders] Checking for bank reminders for customer {customer_id}.")

        current_date_obj = date.today()

        remindable_instruction_types = [
            "LG_DECREASE_AMOUNT",
            "LG_EXTENSION",
            "LG_LIQUIDATE",
            "LG_RELEASE",
        ]

        reminder_exists_subquery = db.query(exists().where(and_(
            models.LGInstruction.instruction_type == ACTION_TYPE_LG_REMINDER_TO_BANKS,
            models.LGInstruction.lg_record_id == self.model.lg_record_id,
            models.LGInstruction.is_deleted == False,
            models.LGInstruction.details['original_instruction_id'].astext == func.cast(self.model.id, String)
        ))).scalar_subquery()

        query = db.query(self.model, reminder_exists_subquery.label("has_reminder_sent")).join(
            models.LGRecord,
            self.model.lg_record_id == models.LGRecord.id
        ).filter(
            models.LGRecord.customer_id == customer_id,
            self.model.is_deleted == False,
            self.model.bank_reply_date.is_(None),
            self.model.instruction_type.in_(remindable_instruction_types),
            func.date(self.model.instruction_date) >= (current_date_obj - timedelta(days=max_days_since_issuance))
        )

        condition_delivered_and_overdue = and_(
            self.model.delivery_date.isnot(None),
            func.date(self.model.delivery_date) <= (current_date_obj - timedelta(days=days_since_delivery))
        )

        condition_undelivered_and_overdue_by_issuance = and_(
            self.model.delivery_date.is_(None),
            func.date(self.model.instruction_date) <= (current_date_obj - timedelta(days=days_since_issuance))
        )

        final_query = query.filter(
            condition_delivered_and_overdue | condition_undelivered_and_overdue_by_issuance
        )

        logger.info(f"Generated SQL Query (get_instructions_for_bank_reminders): {final_query.statement.compile(dialect=db.bind.dialect, compile_kwargs={'literal_binds': True})}")

        result_tuples = final_query.order_by(self.model.instruction_date).all()

        instructions_with_flags = []
        for instruction_obj, has_reminder_sent_flag in result_tuples:
            instruction_obj.has_reminder_sent = has_reminder_sent_flag
            instructions_with_flags.append(instruction_obj)

        logger.debug(f"[CRUDLGInstruction.get_instructions_for_bank_reminders] Found {len(instructions_with_flags)} instructions meeting reminder criteria for customer {customer_id}.")
        return instructions_with_flags


    async def send_bank_reminder(
        self, db: Session, original_instruction_id: int, user_id: int, customer_id: int
    ) -> Tuple[models.LGRecord, int, bytes]:
        logger.debug(f"[CRUDLGInstruction.send_bank_reminder] Initiating bank reminder for original instruction ID: {original_instruction_id}")
        original_instruction = db.query(self.model).options(
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_currency),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.issuing_bank),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.customer),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.internal_owner_contact),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_category),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.communication_bank),
            selectinload(self.model.template)
        ).filter(
            self.model.id == original_instruction_id,
            self.model.lg_record.has(models.LGRecord.customer_id == customer_id),
            self.model.is_deleted == False
        ).first()
        
        # --- Logic to calculate the delivery clause text ---
        delivery_date_val = getattr(original_instruction, 'delivery_date', None) 
        if delivery_date_val:
            formatted_date = delivery_date_val.strftime("%Y-%m-%d") if hasattr(delivery_date_val, 'strftime') else str(delivery_date_val)
            delivery_clause_text = f"(or delivered on {formatted_date})"
        else:
            delivery_clause_text = ""
        # --------------------------------------------------
        
        if not original_instruction or not original_instruction.lg_record:
            logger.warning(f"[CRUDLGInstruction.send_bank_reminder] Original instruction {original_instruction_id} not found or not accessible for customer {customer_id}.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Original LG Instruction not found or not accessible.")
        lg_record = original_instruction.lg_record
        if original_instruction.bank_reply_date is not None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot send reminder: Bank reply has already been recorded for this instruction.")
        existing_reminder = db.query(self.model).filter(
            self.model.instruction_type == ACTION_TYPE_LG_REMINDER_TO_BANKS,
            self.model.lg_record_id == lg_record.id,
            self.model.is_deleted == False,
            self.model.details['original_instruction_id'].astext == func.cast(original_instruction_id, String)
        ).first()

        if existing_reminder:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"A reminder has already been sent for original instruction '{original_instruction.serial_number}' (Reminder Serial: {existing_reminder.serial_number}).")
        days_overdue = 0
        if original_instruction.delivery_date:
            days_overdue = (date.today() - original_instruction.delivery_date.date()).days
            logger.debug(f"Instruction {original_instruction_id} is overdue by {days_overdue} days since delivery.")
        else:
            days_overdue = (date.today() - original_instruction.instruction_date.date()).days
            logger.debug(f"Instruction {original_instruction_id} is overdue by {days_overdue} days since issuance (no delivery date).")
        reminder_template = db.query(models.Template).filter(
            models.Template.action_type == ACTION_TYPE_LG_REMINDER_TO_BANKS,
            models.Template.is_global == True,
            models.Template.is_notification_template == False,
            models.Template.is_deleted == False
        ).first()
        if not reminder_template:
            logger.error(f"LG Reminder to Banks template for action_type '{ACTION_TYPE_LG_REMINDER_TO_BANKS}' not found.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"System configuration error: Reminder to Banks template not found.")
        
        beneficiary_entity_code = lg_record.beneficiary_corporate.code
        lg_category_code = lg_record.lg_category.code
        lg_sequence_number_str = str(lg_record.lg_sequence_number).zfill(4)
        
        instruction_type_code = InstructionTypeCode.REM
        sub_instruction_code = SubInstructionCode.BANK_REMINDER
        
        reminder_serial_number, global_seq_val, type_seq_val = await self.get_next_serial_number(
            db,
            lg_record_id=lg_record.id,
            entity_code=beneficiary_entity_code,
            lg_category_code=lg_category_code,
            lg_sequence_number=lg_sequence_number_str,
            instruction_type_code=instruction_type_code,
            sub_instruction_code=sub_instruction_code
        )
        
        # Determine the correct recipient using the helper from crud_lg_record
        from app.crud.crud import crud_lg_record
        recipient_name, recipient_address = crud_lg_record._get_recipient_details(db, lg_record)

        # Get entity and customer details with fallback
        customer = lg_record.customer
        entity = lg_record.beneficiary_corporate
        customer_address = entity.address if entity.address else customer.address
        customer_contact_email = entity.contact_email if entity.contact_email else customer.contact_email

        template_data = {
            "lg_number": lg_record.lg_number,
            "lg_serial_number": lg_record.lg_number,
            "lg_amount": float(lg_record.lg_amount),
            "lg_amount_formatted": f"{lg_record.lg_currency.symbol} {float(lg_record.lg_amount):,.2f}",
            "lg_currency": lg_record.lg_currency.iso_code,
            "issuing_bank_name": lg_record.issuing_bank.name,
            "lg_beneficiary_name": lg_record.beneficiary_corporate.entity_name,
            "customer_name": lg_record.customer.name,
            "customer_address": customer_address,
            "customer_contact_email": customer_contact_email,
            "current_date": date.today().strftime("%Y-%m-%d"),
            "platform_name": "Treasury Management Platform",
            "original_instruction_type": original_instruction.instruction_type,
            "original_instruction_serial": original_instruction.serial_number,
            "original_instruction_date": original_instruction.instruction_date.strftime("%Y-%m-%d"),
            "original_instruction_delivery_date": original_instruction.delivery_date.strftime("%Y-%m-%d") if original_instruction.delivery_date else "N/A",
            "days_overdue": days_overdue,
            "original_instruction_details_summary": str(original_instruction.details)[:200] if original_instruction.details else "No specific details.",
            "original_instruction_template_name": original_instruction.template.name if original_instruction.template else "N/A",
            "recipient_name": recipient_name,
            "recipient_address": recipient_address,
            "delivery_clause": delivery_clause_text, # Correctly added to template data
        }
        generated_html = reminder_template.content
        for key, value in template_data.items():
            str_value = str(value) if value is not None else ""
            generated_html = generated_html.replace(f"{{{{{key}}}}}", str_value)
        try:
            filename_for_pdf = f"lg_reminder_{lg_record.lg_number}_original_{original_instruction.serial_number}_{reminder_serial_number}"
            generated_pdf_bytes = await generate_pdf_from_html(
                generated_html,
                filename_for_pdf
            )
        except Exception as e:
            logger.error(f"[CRUDLGInstruction.send_bank_reminder] Failed to generate PDF for reminder for LG {lg_record.id}, original instruction {original_instruction_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to generate reminder PDF: {e}")
        
        generated_content_path = None
        new_reminder_instruction = await self.create(
            db,
            obj_in=LGInstructionCreate(
                lg_record_id=lg_record.id,
                instruction_type=ACTION_TYPE_LG_REMINDER_TO_BANKS,
                serial_number=None,
                template_id=reminder_template.id,
                status="Instruction Issued",
                # Syntax Error Removed Here
                details={
                    "original_instruction_id": str(original_instruction.id),
                    "original_instruction_serial": original_instruction.serial_number,
                    "original_instruction_type": original_instruction.instruction_type,
                    "days_overdue": days_overdue,
                    "reminder_count": (original_instruction.details.get("reminder_count", 0) + 1) if original_instruction.details and isinstance(original_instruction.details, dict) else 1,
                    "original_instruction_date": original_instruction.instruction_date.strftime("%Y-%m-%d"),
                    "bulk_generated": False
                },
                maker_user_id=user_id,
                checker_user_id=None,
                approval_request_id=None,
                generated_content_path=generated_content_path,
            ),
            entity_code=beneficiary_entity_code,
            lg_category_code=lg_category_code,
            lg_sequence_number_str=lg_sequence_number_str,
            instruction_type_code_enum=instruction_type_code,
            sub_instruction_code_enum=sub_instruction_code
        )
        db.flush()
        log_action(
            db,
            user_id=user_id,
            action_type=AUDIT_ACTION_TYPE_LG_REMINDER_SENT_TO_BANK,
            entity_type="LGInstruction",
            entity_id=new_reminder_instruction.id,
            details={
                "lg_number": lg_record.lg_number,
                "original_instruction_id": original_instruction_id,
                "original_instruction_serial": original_instruction.serial_number,
                "reminder_serial": new_reminder_instruction.serial_number,
                "days_overdue": days_overdue
            },
            customer_id=customer_id,
            lg_record_id=lg_record.id,
        )
        logger.info(f"Bank reminder issued successfully for original instruction '{original_instruction.serial_number}' (Reminder Serial: {new_reminder_instruction.serial_number}).")
        db.refresh(new_reminder_instruction)
        return lg_record, new_reminder_instruction.id, generated_pdf_bytes
        
    async def generate_all_eligible_bank_reminders_pdf(
        self, db: Session, customer_id: int, user_id: int
        ) -> Tuple[Optional[bytes], int, List[int]]:
        logger.info(f"[CRUDLGInstruction.generate_all_eligible_bank_reminders_pdf] Initiating bulk bank reminder PDF generation for customer {customer_id}.")
        
        # 1. Validation: Time Window Check
        try:
            current_time = datetime.now()
            start_hour = 0
            end_hour = 23
            if current_time.hour < start_hour or current_time.hour > end_hour:
                logger.warning(f"Bulk reminder generation is outside the allowed time window ({start_hour}-{end_hour}h). Skipping for customer {customer_id}.")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Bulk reminder generation is outside the allowed time window.")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error checking time window for customer {customer_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to check time window for bulk reminder generation.")
        
        # 2. Configuration Retrieval
        try:
            days_since_delivery_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, customer_id, GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_DELIVERY
            )
            days_since_issuance_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, customer_id, GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE
            )
            max_days_since_issuance_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
                db, customer_id, GlobalConfigKey.REMINDER_TO_BANKS_MAX_DAYS_SINCE_ISSUANCE
            )
            
            # Use defaults if config is missing
            days_since_delivery = int(days_since_delivery_config.get('effective_value', 7)) if days_since_delivery_config else 7
            days_since_issuance = int(days_since_issuance_config.get('effective_value', 3)) if days_since_issuance_config else 3
            max_days_since_issuance = int(max_days_since_issuance_config.get('effective_value', 90)) if max_days_since_issuance_config else 90

            # FIX: Removed the strict exception raising logic here. 
            # We just log a warning if values seem odd, but we proceed.
            if not (0 < days_since_delivery < max_days_since_issuance and 0 < days_since_issuance < max_days_since_issuance):
                logger.warning(f"Reminder configuration thresholds for customer {customer_id} might be illogical (Delivery: {days_since_delivery}, Issuance: {days_since_issuance}, Max: {max_days_since_issuance}), but proceeding.")

        except (ValueError, AttributeError, TypeError) as e:
            logger.error(f"Error retrieving or parsing reminder configuration for customer {customer_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve reminder configurations.")
        
        # 3. Fetch Eligible Instructions
        current_datetime = datetime.now()
        remindable_instruction_types = [
            "LG_DECREASE_AMOUNT",
            "LG_EXTENSION",
            "LG_LIQUIDATE",
            "LG_RELEASE",
        ]

        eligible_instructions = db.query(self.model).options(
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_currency),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.issuing_bank),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.customer),
            selectinload(self.model.maker_user),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.internal_owner_contact),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_category),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.communication_bank),
            selectinload(self.model.template),
            selectinload(self.model.documents)
        ).filter(
            models.LGInstruction.lg_record.has(models.LGRecord.customer_id == customer_id),
            self.model.is_deleted == False,
            self.model.bank_reply_date.is_(None),
            self.model.instruction_type.in_(remindable_instruction_types),
            func.date(self.model.instruction_date) > (current_datetime - timedelta(days=max_days_since_issuance)).date()
        ).order_by(self.model.instruction_date).all()

        if not eligible_instructions:
            logger.info(f"[CRUDLGInstruction.generate_all_eligible_bank_reminders_pdf] No eligible instructions found for bank reminders for customer {customer_id}.")
            return None, 0, []

        # 4. Generate Content
        consolidated_html_content = []
        generated_reminder_count = 0
        generated_instruction_ids = []
        
        reminder_template = db.query(models.Template).filter(
            models.Template.action_type == ACTION_TYPE_LG_REMINDER_TO_BANKS,
            models.Template.is_global == True,
            models.Template.is_notification_template == False,
            models.Template.is_deleted == False
        ).first()
        
        if not reminder_template:
            logger.error(f"LG Reminder to Banks template for action_type '{ACTION_TYPE_LG_REMINDER_TO_BANKS}' not found.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"System configuration error: Reminder to Banks template not found.")
        
        customer_obj = db.query(models.Customer).filter(models.Customer.id == customer_id).first()
        customer_name = customer_obj.name if customer_obj else "N/A"
        customer_address = customer_obj.address
        customer_contact_email = customer_obj.contact_email
        
        from app.crud.crud import crud_lg_record

        for original_instruction in eligible_instructions:
            # Check for existing reminders
            existing_reminder = db.query(self.model).filter(
                self.model.instruction_type == ACTION_TYPE_LG_REMINDER_TO_BANKS,
                self.model.lg_record_id == original_instruction.lg_record.id,
                self.model.is_deleted == False,
                self.model.details['original_instruction_id'].astext == func.cast(original_instruction.id, String)
            ).first()
            if existing_reminder:
                continue

            lg_record = original_instruction.lg_record
            
            # Determine days overdue
            if original_instruction.delivery_date:
                days_overdue = (date.today() - original_instruction.delivery_date.date()).days
                # Skip if not yet overdue based on DELIVERY config
                if days_overdue < days_since_delivery:
                    continue
            else:
                days_overdue = (date.today() - original_instruction.instruction_date.date()).days
                # Skip if not yet overdue based on ISSUANCE config
                if days_overdue < days_since_issuance:
                    continue

            # Fallback logic for contact details
            entity = lg_record.beneficiary_corporate
            customer_address_final = entity.address if entity.address else customer_address
            customer_contact_email_final = entity.contact_email if entity.contact_email else customer_contact_email
            
            # Recipient details
            recipient_name, recipient_address = crud_lg_record._get_recipient_details(db, lg_record)

            # --- Calculate the delivery clause text ---
            delivery_date_val = getattr(original_instruction, 'delivery_date', None) 
            if delivery_date_val:
                formatted_date = delivery_date_val.strftime("%Y-%m-%d") if hasattr(delivery_date_val, 'strftime') else str(delivery_date_val)
                delivery_clause_text = f"(or delivered on {formatted_date})"
            else:
                delivery_clause_text = ""
            # ------------------------------------------

            # Serial Number Generation
            beneficiary_entity_code = lg_record.beneficiary_corporate.code
            lg_category_code = lg_record.lg_category.code
            lg_sequence_number_str = str(lg_record.lg_sequence_number).zfill(4)
            
            reminder_serial_number, global_seq_val, type_seq_val = await self.get_next_serial_number(
                db,
                lg_record_id=lg_record.id,
                entity_code=beneficiary_entity_code,
                lg_category_code=lg_category_code,
                lg_sequence_number=lg_sequence_number_str,
                instruction_type_code=InstructionTypeCode.REM,
                sub_instruction_code=SubInstructionCode.BANK_REMINDER
            )

            template_data = {
                "lg_number": lg_record.lg_number,
                "lg_serial_number": lg_record.lg_number,
                "lg_amount": float(lg_record.lg_amount),
                "lg_amount_formatted": f"{lg_record.lg_currency.symbol} {float(lg_record.lg_amount):,.2f}",
                "lg_currency": lg_record.lg_currency.iso_code,
                "issuing_bank_name": lg_record.issuing_bank.name,
                "lg_beneficiary_name": lg_record.beneficiary_corporate.entity_name,
                "customer_name": lg_record.customer.name,
                "customer_address": customer_address_final,
                "customer_contact_email": customer_contact_email_final,
                "current_date": date.today().strftime("%Y-%m-%d"),
                "platform_name": "Treasury Management Platform",
                "original_instruction_type": original_instruction.instruction_type,
                "original_instruction_serial": original_instruction.serial_number,
                "original_instruction_date": original_instruction.instruction_date.strftime("%Y-%m-%d"),
                "original_instruction_delivery_date": original_instruction.delivery_date.strftime("%Y-%m-%d") if original_instruction.delivery_date else "N/A",
                "days_overdue": days_overdue,
                "original_instruction_details_summary": str(original_instruction.details)[:200] if original_instruction.details else "No specific details.",
                "original_instruction_template_name": original_instruction.template.name if original_instruction.template else "N/A",
                "recipient_name": recipient_name,
                "recipient_address": recipient_address,
                "delivery_clause": delivery_clause_text, # Passed to template
            }
            
            generated_html = reminder_template.content
            for key, value in template_data.items():
                str_value = str(value) if value is not None else ""
                generated_html = generated_html.replace(f"{{{{{key}}}}}", str_value)
            
            consolidated_html_content.append(generated_html)
            consolidated_html_content.append('<div style="page-break-after: always;"></div>')
            
            new_reminder_instruction = await self.create(
                db,
                obj_in=LGInstructionCreate(
                    lg_record_id=lg_record.id,
                    instruction_type=ACTION_TYPE_LG_REMINDER_TO_BANKS,
                    serial_number=None,
                    template_id=reminder_template.id,
                    status="Instruction Issued",
                    details={
                        "original_instruction_id": str(original_instruction.id),
                        "original_instruction_serial": original_instruction.serial_number,
                        "original_instruction_type": original_instruction.instruction_type,
                        "days_overdue": days_overdue,
                        "reminder_count": (original_instruction.details.get("reminder_count", 0) + 1) if original_instruction.details and isinstance(original_instruction.details, dict) else 1,
                        "original_instruction_date": original_instruction.instruction_date.strftime("%Y-%m-%d"),
                        "bulk_generated": True
                    },
                    maker_user_id=user_id,
                    checker_user_id=None,
                    approval_request_id=None,
                    generated_content_path=None,
                ),
                entity_code=beneficiary_entity_code,
                lg_category_code=lg_category_code,
                lg_sequence_number_str=lg_sequence_number_str,
                instruction_type_code_enum=InstructionTypeCode.REM,
                sub_instruction_code_enum=SubInstructionCode.BANK_REMINDER
            )
            db.flush()
            log_action(
                db,
                user_id=user_id,
                action_type=AUDIT_ACTION_TYPE_LG_REMINDER_SENT_TO_BANK,
                entity_type="LGInstruction",
                entity_id=new_reminder_instruction.id,
                details={
                    "lg_number": lg_record.lg_number,
                    "original_instruction_id": original_instruction.id,
                    "original_instruction_serial": original_instruction.serial_number,
                    "reminder_serial": new_reminder_instruction.serial_number,
                    "days_overdue": days_overdue
                },
                customer_id=customer_id,
                lg_record_id=lg_record.id,
            )
            generated_reminder_count += 1
            generated_instruction_ids.append(new_reminder_instruction.id)
            logger.debug(f"Generated reminder {new_reminder_instruction.serial_number} for original instruction {original_instruction.serial_number}.")

        # 5. Final PDF Assembly
        if consolidated_html_content and consolidated_html_content[-1].startswith('<div style="page-break-after:'):
            consolidated_html_content.pop()
        
        if not consolidated_html_content:
            logger.info(f"No actual reminders generated after filtering for customer {customer_id}.")
            return None, 0, []
        
        final_consolidated_html = "".join(consolidated_html_content)
        try:
            consolidated_pdf_bytes = await generate_pdf_from_html(
                final_consolidated_html,
                f"consolidated_bank_reminders_customer_{customer_id}_{date.today().isoformat()}"
            )
        except Exception as e:
            logger.error(f"[CRUDLGInstruction.generate_all_eligible_bank_reminders_pdf] Failed to generate consolidated PDF: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to generate consolidated PDF: {e}")
        
        log_action(
            db,
            user_id=user_id,
            action_type=AUDIT_ACTION_TYPE_LG_BULK_REMINDER_INITIATED,
            entity_type="Customer",
            entity_id=customer_id,
            details={
                "total_eligible_instructions": len(eligible_instructions),
                "reminders_actually_generated": generated_reminder_count,
                "generated_instruction_ids": generated_instruction_ids,
                "source_api": "generate_all_bank_reminders_pdf",
                "remind_since_delivery_days": days_since_delivery,
                "remind_since_issuance_days": days_since_issuance,
                "max_days_since_issuance": max_days_since_issuance
            },
            customer_id=customer_id,
            lg_record_id=None,
        )
        logger.info(f"Successfully generated {generated_reminder_count} consolidated bank reminders for customer {customer_id}.")
        return consolidated_pdf_bytes, generated_reminder_count, generated_instruction_ids