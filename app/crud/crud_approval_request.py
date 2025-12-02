# c:\Grow\app\crud\crud_approval_request.py

from typing import List, Optional, Type, Dict, Any, Union # <-- ADD Union
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func, desc
from fastapi import HTTPException, status
from datetime import datetime, date, timedelta
import json
import decimal
from app.core.ai_integration import delete_file_from_gcs
from app.crud.crud import CRUDBase, log_action
import app.models as models
from app.schemas.all_schemas import (
    ApprovalRequestCreate, ApprovalRequestUpdate, LGRecordUpdate, LGInstructionUpdate,
    LGRecordChangeOwner, InternalOwnerContactUpdateDetails,
    LGActivateNonOperativeRequest,
    LGDocumentCreate,
    LGInstructionCancelRequest, # <-- ADD this import for the new action type
)
from app.constants import (
    ApprovalRequestStatusEnum,
    ACTION_TYPE_LG_DECREASE_AMOUNT,
    ACTION_TYPE_LG_CHANGE_OWNER_DETAILS,
    ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER,
    ACTION_TYPE_LG_CHANGE_BULK_LG_OWNER,
    ACTION_TYPE_LG_AMEND,
    ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
    ACTION_TYPE_LG_CANCEL_LAST_INSTRUCTION, # <-- ADD this new constant
    GlobalConfigKey,
    AUDIT_ACTION_TYPE_APPROVAL_REJECTED_SELF_APPROVAL,
    AUDIT_ACTION_TYPE_APPROVAL_INVALIDATED_LG_MISSING,
    AUDIT_ACTION_TYPE_APPROVAL_LG_STATE_CHANGED,
    AUDIT_ACTION_TYPE_APPROVAL_INVALIDATED_ENTITY_MISSING,
    AUDIT_ACTION_TYPE_APPROVAL_ENTITY_STATE_CHANGED,
    AUDIT_ACTION_TYPE_APPROVAL_INVALIDATED_BY_OTHER_APPROVAL,
    AUDIT_ACTION_TYPE_APPROVAL_REQUEST_APPROVED,
    AUDIT_ACTION_TYPE_APPROVAL_REQUEST_REJECTED,
    AUDIT_ACTION_TYPE_APPROVAL_REQUEST_WITHDRAWN,
    AUDIT_ACTION_TYPE_APPROVAL_REQUEST_AUTO_REJECTED,
    NOTIFICATION_PRINT_CONFIRMATION,
    ACTION_TYPE_APPROVAL_REQUEST_PENDING,
)

from app.core.email_service import EmailSettings, get_global_email_settings, send_email, get_customer_email_settings

import logging
logger = logging.getLogger(__name__)

def _nuke_document(db: Session, request_details: dict):
    """Finds document ID in details, deletes file from Cloud, deletes record from DB."""
    doc_id = request_details.get("supporting_document_id") or request_details.get("lg_document_id")
    
    if doc_id:
        try:
            # 1. Find the document directly
            doc = db.query(models.LGDocument).get(int(doc_id))
            if doc:
                # 2. Delete from GCS
                if doc.file_path:
                    delete_file_from_gcs(doc.file_path)
                
                # 3. Delete from DB
                db.delete(doc)
                # Note: The main transaction will commit this change
        except Exception as e:
            logger.error(f"Failed to delete document {doc_id}: {e}")

class CRUDApprovalRequest(CRUDBase):
    def __init__(self, model: Type[models.ApprovalRequest]):
        # FIX: Remove the super().__init__(model) call from the original file's version
        # because it tries to access `is_deleted` which doesn't exist on this model.
        # Instead, just set the model attribute directly.
        self.model = model

    def _get_lg_record_snapshot(self, lg_record: models.LGRecord) -> Dict[str, Any]:
        if not lg_record:
            return {}
        return {
            "lg_number": lg_record.lg_number,
            "expiry_date": lg_record.expiry_date.isoformat() if lg_record.expiry_date else None,
            "lg_amount": float(lg_record.lg_amount),
            "lg_status_id": lg_record.lg_status_id,
            "lg_type_id": lg_record.lg_type_id,
            "issuing_bank_id": lg_record.issuing_bank_id,
            "beneficiary_corporate_id": lg_record.beneficiary_corporate_id,
            "internal_owner_contact_id": lg_record.internal_owner_contact_id,
            "issuance_date": lg_record.issuance_date.isoformat() if lg_record.issuance_date else None,
            "payment_conditions": lg_record.payment_conditions,
            "description_purpose": lg_record.description_purpose,
            "other_conditions": lg_record.other_conditions,
        }

    def _get_internal_owner_contact_snapshot(self, owner_contact: models.InternalOwnerContact) -> Dict[str, Any]:
        if not owner_contact:
            return {}
        return {
            "id": owner_contact.id,
            "email": owner_contact.email,
            "phone_number": owner_contact.phone_number,
            "internal_id": owner_contact.internal_id,
            "manager_email": owner_contact.manager_email,
        }

    # CONVERTED TO ASYNC METHOD
    async def create_approval_request(
        self,
        db: Session,
        obj_in: ApprovalRequestCreate,
        maker_user_id: int,
        customer_id: int,
        lg_record: Optional[models.LGRecord] = None,
        internal_owner_contact: Optional[models.InternalOwnerContact] = None
    ) -> models.ApprovalRequest:
        create_data = obj_in.model_dump()
        create_data["maker_user_id"] = maker_user_id
        create_data["customer_id"] = customer_id
        create_data["status"] = ApprovalRequestStatusEnum.PENDING

        if lg_record:
            create_data["lg_record_snapshot"] = self._get_lg_record_snapshot(lg_record)
        elif internal_owner_contact:
            create_data["lg_record_snapshot"] = self._get_internal_owner_contact_snapshot(internal_owner_contact)
        else:
            create_data["lg_record_snapshot"] = {}

        db_obj = self.model(**create_data)
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)

        log_action(
            db,
            user_id=maker_user_id,
            action_type="APPROVAL_REQUEST_SUBMITTED",
            entity_type="ApprovalRequest",
            entity_id=db_obj.id,
            details={
                "entity_type_requested": db_obj.entity_type,
                "entity_id_requested": db_obj.entity_id,
                "action_type_requested": db_obj.action_type,
                "request_details_summary": str(db_obj.request_details)[:200],
                "snapshot_at_request": db_obj.lg_record_snapshot
            },
            customer_id=customer_id,
            lg_record_id=db_obj.entity_id if db_obj.entity_type == "LGRecord" else None,
        )

        # --- FIX START: Await the async notification function ---
        try:
            # The function signature of _send_pending_approval_notification should remain async
            await self._send_pending_approval_notification(db, db_obj) # <--- ADDED AWAIT HERE
        except Exception as e:
            logger.error(f"Failed to send pending approval notification for request ID {db_obj.id}: {e}", exc_info=True)
            log_action(
                db,
                user_id=maker_user_id,
                action_type="NOTIFICATION_FAILED",
                entity_type="ApprovalRequest",
                entity_id=db_obj.id,
                details={
                    "reason": f"Failed to notify checkers about pending request: {e}",
                    "notification_type": ACTION_TYPE_APPROVAL_REQUEST_PENDING,
                },
                customer_id=customer_id,
                lg_record_id=db_obj.entity_id if db_obj.entity_type == "LGRecord" else None,
            )
        # --- FIX END ---

        return db_obj

    # The _send_pending_approval_notification helper function remains ASYNC:
    async def _send_pending_approval_notification(self, db: Session, approval_request: models.ApprovalRequest):
        """
        Sends an email notification to all Checkers and Corporate Admins of the customer
        about a newly submitted approval request.
        """
        logger.info(f"Attempting to send 'Pending Approval' notification for Approval Request ID: {approval_request.id}.")

        # 1. Identify all Checkers and Corporate Admins for the customer
        checker_and_admin_emails = db.query(models.User.email).filter(
            models.User.customer_id == approval_request.customer_id,
            models.User.is_deleted == False,  # <--- CORRECTED: Using is_deleted == False for active users
            models.User.email.isnot(None),
            models.User.email != "",
            models.User.role.in_(['corporate_admin', 'checker']) 
        ).distinct().all()
        
        # Filter out the maker user to prevent self-notification
        # Note: We must ensure maker_user relationship is eagerly loaded or fetched, 
        # but for simplicity and safety, we'll rely on the DB object refresh done prior to this call.
        maker_email = approval_request.maker_user.email if approval_request.maker_user else None
        
        to_emails = [email[0] for email in checker_and_admin_emails if email[0] != maker_email]
        cc_emails = [] # No CC list needed for the primary checker alert

        if not to_emails:
            logger.warning(f"No valid recipients found for pending approval request ID {approval_request.id}. Skipping email.")
            return

        # 2. Fetch template
        import app.crud.crud as crud_instances # Import crud instance helper inside the function scope if needed
        notification_template = db.query(models.Template).filter(
            models.Template.action_type == ACTION_TYPE_APPROVAL_REQUEST_PENDING,
            models.Template.is_notification_template == True,
            models.Template.is_deleted == False,
            models.Template.is_global == True
        ).first()

        if not notification_template:
            logger.error(f"Email notification template for '{ACTION_TYPE_APPROVAL_REQUEST_PENDING}' not found. Cannot send checker alert.")
            return

        # 3. Prepare template data and email settings
        email_settings_to_use, email_method_for_log = get_customer_email_settings(db, approval_request.customer_id)
        
        lg_record = approval_request.lg_record if approval_request.lg_record else None
        
        lg_number = lg_record.lg_number if lg_record else f"Owner Contact ID: {approval_request.entity_id}"
        lg_currency = lg_record.lg_currency.iso_code if lg_record and lg_record.lg_currency else "N/A"
        lg_amount = float(lg_record.lg_amount) if lg_record and lg_record.lg_amount is not None else "N/A"
        
        template_data = {
            "maker_email": maker_email if maker_email else "N/A",
            "maker_name": approval_request.maker_user.email.split('@')[0] if approval_request.maker_user and approval_request.maker_user.email else "N/A",
            "approval_request_id": approval_request.id,
            "action_type": approval_request.action_type.replace('_', ' ').title(),
            "entity_type": approval_request.entity_type,
            "lg_number": lg_number,
            "lg_amount": lg_amount,
            "lg_currency_code": lg_currency,
            "current_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "platform_name": "Treasury Management Platform",
            "action_center_link": "/checker/action-center"
        }

        template_data["lg_amount_formatted"] = f"{lg_currency} {lg_amount:,.2f}" if isinstance(lg_amount, (float, int, decimal.Decimal)) else lg_amount


        email_subject = notification_template.subject if notification_template.subject else f"ACTION REQUIRED: Approval Pending for {{action_type}} on {{lg_number}}"
        email_body_html = notification_template.content
        for key, value in template_data.items():
            str_value = str(value) if value is not None else ""
            email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
            email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

        # 4. Send the email
        email_sent_successfully = await send_email(
            db=db,
            to_emails=to_emails,
            cc_emails=cc_emails,
            subject_template=email_subject,
            body_template=email_body_html,
            template_data=template_data,
            email_settings=email_settings_to_use,
        )

        # 5. Log the notification attempt/success
        if email_sent_successfully:
            log_action(
                db,
                user_id=approval_request.maker_user_id,
                action_type="NOTIFICATION_SENT",
                entity_type="ApprovalRequest",
                entity_id=approval_request.id,
                details={
                    "recipient": to_emails,
                    "subject": email_subject,
                    "method": email_method_for_log,
                    "notification_type": "Pending Approval Alert"
                },
                customer_id=approval_request.customer_id,
                lg_record_id=lg_record.id if lg_record else None,
            )
            logger.info(f"Pending approval alert sent successfully for AR {approval_request.id} to {len(to_emails)} checker(s).")
        else:
            log_action(
                db,
                user_id=approval_request.maker_user_id,
                action_type="NOTIFICATION_FAILED",
                entity_type="ApprovalRequest",
                entity_id=approval_request.id,
                details={"reason": "Email service failed to send pending approval alert", "recipient": to_emails, "subject": email_subject, "method": email_method_for_log},
                customer_id=approval_request.customer_id,
                lg_record_id=lg_record.id if lg_record else None,
            )
            logger.error(f"Failed to send pending approval alert for Approval Request ID: {approval_request.id}.")
    # --- UPDATED METHOD END: _send_pending_approval_notification ---
    
    
    def get_pending_requests_for_customer(
        self, db: Session, customer_id: int, skip: int = 0, limit: int = 100
    ) -> List[models.ApprovalRequest]:
        return db.query(self.model).filter(
            self.model.customer_id == customer_id,
            self.model.status == ApprovalRequestStatusEnum.PENDING,
        ).options(
            selectinload(self.model.maker_user),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_status),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.internal_owner_contact),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.customer),
        ).order_by(desc(self.model.created_at)).offset(skip).limit(limit).all()

    def get_maker_pending_requests(
        self, db: Session, maker_user_id: int, customer_id: int, skip: int = 0, limit: int = 100
    ) -> List[models.ApprovalRequest]:
        return db.query(self.model).filter(
            self.model.maker_user_id == maker_user_id,
            self.model.customer_id == customer_id,
            self.model.status == ApprovalRequestStatusEnum.PENDING,
        ).options(
            selectinload(self.model.maker_user),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_status),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.internal_owner_contact),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.customer),
        ).order_by(desc(self.model.created_at)).offset(skip).limit(limit).all()


    def get_pending_requests_for_lg(
        self, db: Session, lg_record_id: int, customer_id: int
    ) -> List[models.ApprovalRequest]:
        return db.query(self.model).filter(
            self.model.entity_id == lg_record_id,
            self.model.customer_id == customer_id,
            self.model.status == ApprovalRequestStatusEnum.PENDING,
            self.model.entity_type == "LGRecord"
        ).all()

    def get_approval_request_by_id(
        self, db: Session, request_id: int, customer_id: int
    ) -> Optional[models.ApprovalRequest]:
        query = db.query(self.model).filter(
            self.model.id == request_id,
            self.model.customer_id == customer_id,
        ).options(
            selectinload(self.model.maker_user),
            selectinload(self.model.checker_user),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_status),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_type),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.issuing_bank),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.beneficiary_corporate),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.internal_owner_contact),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.customer),
            selectinload(self.model.internal_owner_contact),
            selectinload(self.model.related_instruction).selectinload(models.LGInstruction.template)
        )
        return query.first()

    def get_all_for_customer(
        self,
        db: Session,
        customer_id: int,
        status_filter: Optional[ApprovalRequestStatusEnum | List[ApprovalRequestStatusEnum]] = None, # <-- FIX
        action_type_filter: Optional[Union[str, List[str]]] = None, # <-- FIX
        skip: int = 0,
        limit: int = 100,
        pending_only: bool = False, # <-- ADD this parameter to handle the special case of pending-only requests
    ) -> List[models.ApprovalRequest]:
        """
        Retrieves approval requests for a given customer, with optional status and action_type filters.
        """
        # FIX: The `is_deleted` filter is removed from this method, as the ApprovalRequest model does not have this column.
        query = db.query(self.model).filter(
            self.model.customer_id == customer_id,
        )
        
        # New conditional logic to handle `pending_only` filter from the new endpoints
        if pending_only:
            query = query.filter(self.model.status == ApprovalRequestStatusEnum.PENDING)

        if status_filter:
            # FIX: Check if status_filter is a list and use the appropriate operator
            if isinstance(status_filter, list):
                query = query.filter(self.model.status.in_(status_filter))
            else:
                query = query.filter(self.model.status == status_filter)

        if action_type_filter:
            # FIX: Check if action_type_filter is a list and use the appropriate operator
            if isinstance(action_type_filter, list):
                query = query.filter(self.model.action_type.in_(action_type_filter))
            else:
                query = query.filter(self.model.action_type == action_type_filter)

        query = query.options(
            selectinload(self.model.maker_user),
            selectinload(self.model.checker_user),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.lg_status),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.internal_owner_contact),
            selectinload(self.model.lg_record).selectinload(models.LGRecord.customer),
            selectinload(self.model.internal_owner_contact),
            selectinload(self.model.related_instruction).selectinload(models.LGInstruction.template)
        ).order_by(desc(self.model.created_at)).offset(skip).limit(limit)
        return query.all()


    async def approve_request(
        self, db: Session, request_id: int, checker_user_id: int, customer_id: int
    ) -> models.ApprovalRequest:
        db_request = self.get_approval_request_by_id(db, request_id, customer_id)
        if not db_request:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval request not found or not accessible.")
        if db_request.status != ApprovalRequestStatusEnum.PENDING:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Request is already {db_request.status.value}.")

        if db_request.maker_user_id == checker_user_id:
            log_action(
                db,
                user_id=checker_user_id,
                action_type=AUDIT_ACTION_TYPE_APPROVAL_REJECTED_SELF_APPROVAL,
                entity_type="ApprovalRequest",
                entity_id=db_request.id,
                details={"reason": "Maker attempted to approve own request", "maker_user_id": db_request.maker_user_id},
                customer_id=customer_id,
                lg_record_id=db_request.entity_id if db_request.entity_type == "LGRecord" else None,
            )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Maker cannot be the Checker for the same transaction.")

        current_entity_data = {}
        state_changed_details = {}

        if db_request.entity_type == "LGRecord" and db_request.entity_id:
            import app.crud.crud as crud_instances
            current_lg_record = crud_instances.crud_lg_record.get_lg_record_with_relations(db, db_request.entity_id, customer_id)
            if not current_lg_record:
                db_request.status = ApprovalRequestStatusEnum.INVALIDATED_BY_APPROVAL
                db_request.checker_user_id = checker_user_id
                db_request.updated_at = func.now()
                db_request.reason = "Associated LGRecord not found during approval execution."
                db.add(db_request)
                db.flush()
                log_action(
                    db,
                    user_id=checker_user_id,
                    action_type=AUDIT_ACTION_TYPE_APPROVAL_INVALIDATED_LG_MISSING,
                    entity_type="ApprovalRequest",
                    entity_id=db_request.id,
                    details={"reason": db_request.reason, "entity_id": db_request.entity_id, "action_type_approved": db_request.action_type},
                    customer_id=customer_id,
                    lg_record_id=db_request.entity_id,
                )
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Associated LG Record not found or has been removed. Request invalidated.")

            snapshot = db_request.lg_record_snapshot or {}
            current_entity_data = self._get_lg_record_snapshot(current_lg_record)

            for key in snapshot.keys():
                snapshot_value = snapshot.get(key)
                current_value = current_entity_data.get(key)

                if key == "lg_amount":
                    if not isinstance(snapshot_value, decimal.Decimal):
                        snapshot_value = decimal.Decimal(str(snapshot_value)) if snapshot_value is not None else None
                    if not isinstance(current_value, decimal.Decimal):
                        current_value = decimal.Decimal(str(current_value)) if current_value is not None else None

                    if snapshot_value != current_value:
                        state_changed_details[key] = {"old": float(snapshot_value) if snapshot_value is not None else None,
                                                      "new": float(current_value) if current_value is not None else None}
                elif key == "internal_owner_contact_id":
                    if snapshot_value != current_value:
                        state_changed_details[key] = {"old": snapshot_value, "new": current_value}
                elif key in ["issuance_date", "expiry_date"]:
                    if (snapshot_value and current_value and snapshot_value != current_value) or (bool(snapshot_value) != bool(current_value)):
                        state_changed_details[key] = {"old": snapshot_value, "new": current_value}
                else:
                    if snapshot_value != current_value:
                        state_changed_details[key] = {"old": snapshot_value, "new": current_value}

            if state_changed_details:
                log_action(
                    db,
                    user_id=checker_user_id,
                    action_type=AUDIT_ACTION_TYPE_APPROVAL_LG_STATE_CHANGED,
                    entity_type="ApprovalRequest",
                    entity_id=db_request.id,
                    details={"entity_id": db_request.entity_id, "changes": state_changed_details, "action_type_approved": db_request.action_type},
                    customer_id=customer_id,
                    lg_record_id=db_request.entity_id,
                )
                logger.debug(f"LG Record state changed since request was made for approval {db_request.id}. Changes: {state_changed_details}")

        elif db_request.entity_type == "InternalOwnerContact" and db_request.entity_id:
            import app.crud.crud as crud_instances
            current_owner_contact = crud_instances.crud_internal_owner_contact.get(db, db_request.entity_id)
            if not current_owner_contact:
                db_request.status = ApprovalRequestStatusEnum.INVALIDATED_BY_APPROVAL
                db_request.checker_user_id = checker_user_id
                db_request.updated_at = func.now()
                db_request.reason = "Associated InternalOwnerContact not found during approval execution."
                db.add(db_request)
                db.flush()
                log_action(
                    db,
                    user_id=checker_user_id,
                    action_type=AUDIT_ACTION_TYPE_APPROVAL_INVALIDATED_ENTITY_MISSING,
                    entity_type="ApprovalRequest",
                    entity_id=db_request.id,
                    details={"reason": db_request.reason, "entity_id": db_request.entity_id, "action_type_approved": db_request.action_type},
                    customer_id=customer_id,
                    lg_record_id=None,
                )
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Associated Internal Owner Contact not found or has been removed. Request invalidated.")

            snapshot = db_request.lg_record_snapshot or {}
            current_entity_data = self._get_internal_owner_contact_snapshot(current_owner_contact)

            for key in snapshot.keys():
                if snapshot.get(key) != current_entity_data.get(key):
                    state_changed_details[key] = {"old": snapshot.get(key), "new": current_entity_data.get(key)}

            if state_changed_details:
                log_action(
                    db,
                    user_id=checker_user_id,
                    action_type=AUDIT_ACTION_TYPE_APPROVAL_ENTITY_STATE_CHANGED,
                    entity_type="ApprovalRequest",
                    entity_id=db_request.id,
                    details={"entity_type": db_request.entity_type, "entity_id": db_request.entity_id, "changes": state_changed_details, "action_type_approved": db_request.action_type},
                    customer_id=customer_id,
                    lg_record_id=None,
                )
                logger.debug(f"InternalOwnerContact state changed since request was made for approval {db_request.id}. Changes: {state_changed_details}")

        db_request.status = ApprovalRequestStatusEnum.APPROVED
        db_request.checker_user_id = checker_user_id
        db_request.updated_at = func.now()
        db.add(db_request)
        db.flush()
        db.refresh(db_request)

        generated_instruction_id: Optional[int] = None

        try:
            import app.crud.crud as crud_instances

            if db_request.entity_type == "LGRecord" and db_request.action_type == "LG_RELEASE":
                logger.debug(f"Approval Request {db_request.id}: Calling crud_instances.crud_lg_record.release_lg.")
                if not db_request.lg_record:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Associated LG Record not loaded for execution.")
                supporting_document_id = db_request.request_details.get("supporting_document_id")
                notes = db_request.request_details.get("notes")
                _, instruction_id = await crud_instances.crud_lg_record.release_lg(
                    db,
                    lg_record=db_request.lg_record,
                    user_id=db_request.maker_user_id,
                    approval_request_id=db_request.id,
                    supporting_document_id=supporting_document_id,
                    notes=notes
                )
                generated_instruction_id = instruction_id
                logger.debug(f"DEBUG: Approval Request {db_request.id}: crud_instances.crud_lg_record.release_lg call completed successfully. Instruction ID: {generated_instruction_id}.")

            elif db_request.entity_type == "LGRecord" and db_request.action_type == "LG_LIQUIDATE":
                if "liquidation_type" not in db_request.request_details:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing liquidation_type in request_details for LG_LIQUIDATE approval.")
                if not db_request.lg_record:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Associated LG Record not loaded for execution.")

                liquidation_type = db_request.request_details["liquidation_type"]
                new_amount = db_request.request_details.get("new_amount")
                logger.debug(f"Approval Request {db_request.id}: Calling crud_instances.crud_lg_record.liquidate_lg with type {liquidation_type}.")
                supporting_document_id = db_request.request_details.get("supporting_document_id")
                notes = db_request.request_details.get("notes")
                _, instruction_id = await crud_instances.crud_lg_record.liquidate_lg(
                    db,
                    lg_record=db_request.lg_record,
                    liquidation_type=db_request.request_details["liquidation_type"],
                    new_amount=db_request.request_details.get("new_amount"),
                    user_id=db_request.maker_user_id,
                    approval_request_id=db_request.id,
                    supporting_document_id=supporting_document_id,
                    notes=notes
                )
                generated_instruction_id = instruction_id
                logger.debug(f"Approval Request {db_request.id}: crud_instances.crud_lg_record.liquidate_lg completed. Instruction ID: {generated_instruction_id}.")


            elif db_request.entity_type == "LGRecord" and db_request.action_type == ACTION_TYPE_LG_DECREASE_AMOUNT:
                logger.debug(f"Approval Request {db_request.id}: Detected '{ACTION_TYPE_LG_DECREASE_AMOUNT}' action. Preparing to call crud_instances.crud_lg_record.decrease_lg_amount.")
                if "decrease_amount" not in db_request.request_details:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing decrease_amount in request_details for LG Decrease Amount approval.")
                if not db_request.lg_record:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Associated LG Record not loaded for execution.")

                decrease_amount = db_request.request_details["decrease_amount"]
                supporting_document_id = db_request.request_details.get("supporting_document_id")
                logger.debug(f"Approval Request {db_request.id}: Decrease amount from request_details: {decrease_amount}. Passing to crud_instances.crud_lg_record.decrease_lg_amount.")
                notes = db_request.request_details.get("notes")
                _, instruction_id = await crud_instances.crud_lg_record.decrease_lg_amount(
                    db,
                    lg_record=db_request.lg_record,
                    decrease_amount=decrease_amount,
                    user_id=db_request.maker_user_id,
                    approval_request_id=db_request.id,
                    supporting_document_id=supporting_document_id,
                    notes=notes
                )
                generated_instruction_id = instruction_id
                logger.debug(f"Approval Request {db_request.id}: crud_instances.crud_lg_record.decrease_lg_amount call successfully awaited. Instruction ID: {generated_instruction_id}.")

            elif db_request.entity_type == "LGRecord" and db_request.action_type == ACTION_TYPE_LG_AMEND:
                logger.debug(f"Approval Request {db_request.id}: Detected '{ACTION_TYPE_LG_AMEND}' action. Preparing to call crud_instances.crud_lg_record.amend_lg. No instruction will be generated.")
                if "amendment_details" not in db_request.request_details:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing amendment_details in request_details for LG Amendment approval.")
                if not db_request.lg_record:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Associated LG Record not loaded for execution.")

                amendment_details = db_request.request_details["amendment_details"]
                existing_document_id_from_request = db_request.request_details.get("amendment_document_id")

                await crud_instances.crud_lg_record.amend_lg(
                    db,
                    lg_record_id=db_request.entity_id,
                    amendment_letter_file=None,
                    amendment_document_metadata=None,
                    amendment_details=amendment_details,
                    user_id=db_request.maker_user_id,
                    customer_id=customer_id,
                    approval_request_id=db_request.id,
                    existing_document_id=existing_document_id_from_request
                )
                generated_instruction_id = None
                logger.debug(f"Approval Request {db_request.id}: crud_instances.crud_lg_record.amend_lg call completed.")

            elif db_request.entity_type == "LGRecord" and db_request.action_type == ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE:
                logger.debug(f"Approval Request {db_request.id}: Detected '{ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE}' action. Preparing to call crud_instances.crud_lg_record.activate_non_operative_lg.")
                if "payment_method" not in db_request.request_details:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing payment details in request_details for LG Activation approval.")
                if not db_request.lg_record:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Associated LG Record not loaded for execution.")

                payment_details = LGActivateNonOperativeRequest(**db_request.request_details)
                supporting_document_id = db_request.request_details.get("supporting_document_id")
                notes = db_request.request_details.get("notes")
                _, instruction_id = await crud_instances.crud_lg_record.activate_non_operative_lg(
                    db,
                    lg_record=db_request.lg_record,
                    payment_details=payment_details,
                    user_id=db_request.maker_user_id,
                    customer_id=customer_id,
                    approval_request_id=db_request.id,
                    supporting_document_id=supporting_document_id,
                    notes=notes
                )
                generated_instruction_id = instruction_id
                logger.debug(f"Approval Request {db_request.id}: crud_instances.crud_lg_record.activate_non_operative_lg call completed. Instruction ID: {generated_instruction_id}.")

            elif db_request.entity_type == "LGRecord" and db_request.action_type in [ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER, ACTION_TYPE_LG_CHANGE_BULK_LG_OWNER]:
                logger.debug(f"Approval Request {db_request.id}: Detected '{db_request.action_type}' action. Preparing to call crud_instances.crud_lg_owner.change_lg_internal_owner_single_or_bulk. No instruction will be generated.")

                change_in_data = LGRecordChangeOwner(**db_request.request_details)

                if db_request.action_type == ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER and not change_in_data.lg_record_id and db_request.entity_id:
                    change_in_data.lg_record_id = db_request.entity_id

                await crud_instances.crud_lg_owner.change_lg_internal_owner_single_or_bulk(
                    db,
                    change_in=change_in_data,
                    user_id=db_request.maker_user_id,
                    customer_id=customer_id,
                    approval_request_id=db_request.id
                )
                generated_instruction_id = None
                logger.debug(f"Approval Request {db_request.id}: crud_instances.crud_lg_owner.change_lg_internal_owner_single_or_bulk call for '{db_request.action_type}' completed.")

            elif db_request.entity_type == "InternalOwnerContact" and db_request.action_type == ACTION_TYPE_LG_CHANGE_OWNER_DETAILS:
                logger.debug(f"Approval Request {db_request.id}: Detected '{ACTION_TYPE_LG_CHANGE_OWNER_DETAILS}' action. Preparing to call crud_instances.crud_lg_owner.update_internal_owner_details.")

                owner_details_in = InternalOwnerContactUpdateDetails(**db_request.request_details)

                await crud_instances.crud_lg_owner.update_internal_owner_details(
                    db,
                    old_internal_owner_contact_id=db_request.entity_id,
                    obj_in=owner_details_in,
                    user_id=db_request.maker_user_id,
                    customer_id=customer_id
                )
                generated_instruction_id = None
                logger.debug(f"Approval Request {db_request.id}: crud_instances.crud_lg_owner.update_internal_owner_details call completed.")

            elif db_request.entity_type == "LGRecord" and db_request.action_type == ACTION_TYPE_LG_CANCEL_LAST_INSTRUCTION:
                logger.debug(f"Approval Request {db_request.id}: Detected '{ACTION_TYPE_LG_CANCEL_LAST_INSTRUCTION}' action. Preparing to call crud_instances.crud_lg_cancellation.cancel_instruction.")
                
                request_details = db_request.request_details or {}
                instruction_id_to_cancel = request_details.get("instruction_id")
                
                if not instruction_id_to_cancel:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing instruction_id in request_details for cancellation approval.")
                
                # Use the new LGInstructionCancelRequest Pydantic model for validation
                cancel_in = LGInstructionCancelRequest(**request_details)
                
                # The cancel_instruction method handles all the core logic, including rollback
                canceled_instruction, _ = await crud_instances.crud_lg_cancellation.cancel_instruction(
                    db,
                    instruction_id=instruction_id_to_cancel,
                    cancel_in=cancel_in,
                    user_id=db_request.maker_user_id, # User who initiated the request
                    customer_id=customer_id,
                    approval_request_id=db_request.id,
                )
                
                db_request.related_instruction_id = canceled_instruction.id
                generated_instruction_id = canceled_instruction.id
                
                logger.debug(f"Approval Request {db_request.id}: crud_instances.crud_lg_cancellation.cancel_instruction call completed.")

            else:
                logger.warning(f"Approval Request {db_request.id}: Action type '{db_request.action_type}' not recognized for entity type '{db_request.entity_type}'. No underlying action executed.")


            if generated_instruction_id is not None:
                db_request.related_instruction_id = generated_instruction_id
                db.add(db_request)
                db.flush()
                logger.debug(f"DEBUG: Approval Request {db_request.id} related_instruction_id set to: {generated_instruction_id} and flushed.")

                db.refresh(db_request)
                logger.debug(f"DEBUG: Approval Request {db_request.id} refreshed again to load related_instruction.")

            INSTRUCTION_TYPES_REQUIRING_PRINTING = [
                "LG_RELEASE", "LG_LIQUIDATE", "LG_DECREASE_AMOUNT", "LG_ACTIVATE_NON_OPERATIVE"
            ]
            if generated_instruction_id is not None and db_request.entity_type == "LGRecord" and db_request.action_type in INSTRUCTION_TYPES_REQUIRING_PRINTING:
                await self._send_approval_for_processing_notification(db, db_request)

        except HTTPException as e:
            db.rollback()
            logger.error(f"Approval Request {db_request.id}: HTTPException during action execution. Rolled back. Details: {e.detail}", exc_info=True)
            raise e
        except Exception as e:
            db.rollback()
            logger.exception(f"FATAL ERROR: Approval Request {db_request.id}: Unexpected critical error during underlying action. Rolled back. Traceback:")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected internal error occurred during approval execution. Please check server logs.")

        if db_request.entity_type == "LGRecord" and db_request.entity_id:
            other_pending_requests = self.get_pending_requests_for_lg(db, db_request.entity_id, customer_id)
            for req in other_pending_requests:
                if req.id != db_request.id:
                    req.status = ApprovalRequestStatusEnum.INVALIDATED_BY_APPROVAL
                    req.updated_at = func.now()
                    req.reason = f"This request was invalidated because another approval (ID: {db_request.id}, Type: {db_request.action_type}) for the same LG was approved."
                    db.add(req)
                    db.flush()
                    log_action(
                        db,
                        user_id=checker_user_id,
                        action_type=AUDIT_ACTION_TYPE_APPROVAL_INVALIDATED_BY_OTHER_APPROVAL,
                        entity_type="ApprovalRequest",
                        entity_id=req.id,
                        details={"entity_id": req.entity_id, "invalidated_by_approval_id": db_request.id, "invalidated_action_type": req.action_type},
                        customer_id=customer_id,
                        lg_record_id=req.entity_id,
                    )
        logger.debug(f"DEBUG: Conflicting requests invalidation logic completed for approval request {db_request.id}.")

        try:
            log_action(
                db,
                user_id=checker_user_id,
                action_type=AUDIT_ACTION_TYPE_APPROVAL_REQUEST_APPROVED,
                entity_type="ApprovalRequest",
                entity_id=db_request.id,
                details={
                    "entity_type_approved": db_request.entity_type,
                    "entity_id_approved": db_request.entity_id,
                    "action_type_approved": db_request.action_type,
                    "maker_user_id": db_request.maker_user_id,
                    "executed_details": db_request.request_details,
                    "snapshot_at_request": db_request.lg_record_snapshot,
                    "state_changes_since_request": state_changed_details if state_changed_details else None,
                    "related_instruction_id": db_request.related_instruction_id
                },
                customer_id=customer_id,
                lg_record_id=db_request.entity_id if db_request.entity_type == "LGRecord" else None,
            )
            logger.debug(f"DEBUG: Final audit log for approval request {db_request.id} completed.")

            db.refresh(db_request)
            logger.debug(f"DEBUG: Approval request {db_request.id} refreshed successfully.")
            return db_request

        except Exception as e:
            logger.exception(f"FATAL ERROR: Approval Request {db_request.id}: An unexpected error occurred during final audit logging or object refresh. Transaction will be rolled back. Full traceback:")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected internal error occurred during finalization of approval. Please check server logs for full traceback. Error: {str(e)}"
            )

    def reject_request(
        self, db: Session, request_id: int, checker_user_id: int, customer_id: int, reason: Optional[str] = None
    ) -> models.ApprovalRequest:
        db_request = self.get_approval_request_by_id(db, request_id, customer_id)
        if not db_request:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval request not found or not accessible.")
        if db_request.status != ApprovalRequestStatusEnum.PENDING:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Request is already {db_request.status.value}.")

        if db_request.maker_user_id == checker_user_id:
            log_action(
                db,
                user_id=checker_user_id,
                action_type=AUDIT_ACTION_TYPE_APPROVAL_REJECTED_SELF_APPROVAL,
                entity_type="ApprovalRequest",
                entity_id=db_request.id,
                details={"reason": "Maker attempted to reject own request", "maker_user_id": db_request.maker_user_id},
                customer_id=customer_id,
                lg_record_id=db_request.entity_id if db_request.entity_type == "LGRecord" else None,
            )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Maker cannot be the Checker for the same transaction.")

        db_request.status = ApprovalRequestStatusEnum.REJECTED
        db_request.checker_user_id = checker_user_id
        db_request.updated_at = func.now()
        db_request.reason = reason
        db.add(db_request)
        db.flush()

        log_action(
            db,
            user_id=checker_user_id,
            action_type=AUDIT_ACTION_TYPE_APPROVAL_REQUEST_REJECTED,
            entity_type="ApprovalRequest",
            entity_id=db_request.id,
            details={
                "entity_type_rejected": db_request.entity_type,
                "entity_id_rejected": db_request.entity_id,
                "action_type_rejected": db_request.action_type,
                "maker_user_id": db_request.maker_user_id,
                "rejection_reason": reason
            },
            customer_id=customer_id,
            lg_record_id=db_request.entity_id if db_request.entity_type == "LGRecord" else None,
        )
        _nuke_document(db, db_request.request_details or {})
        db.refresh(db_request)
        return db_request

    def withdraw_request(
        self, db: Session, approval_request: models.ApprovalRequest, maker_user_id: int, customer_id: int
    ) -> models.ApprovalRequest:
        if approval_request.maker_user_id != maker_user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You can only withdraw your own requests.")

        if approval_request.status != ApprovalRequestStatusEnum.PENDING:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Request is already {approval_request.status.value} and cannot be withdrawn.")

        import app.crud.crud as crud_instances
        max_pending_days_config = crud_instances.crud_global_configuration.get_by_key(db, GlobalConfigKey.APPROVAL_REQUEST_MAX_PENDING_DAYS)
        max_withdrawal_days = None
        if max_pending_days_config and max_pending_days_config.value_default:
            try:
                max_withdrawal_days = int(max_pending_days_config.value_default)
            except ValueError:
                logger.warning(f"Invalid value_default for {GlobalConfigKey.APPROVAL_REQUEST_MAX_PENDING_DAYS.value}. Expected integer, got '{max_pending_days_config.value_default}'. Withdrawal time limit will not be enforced.")

        if max_withdrawal_days is not None:
            time_since_submission = datetime.now(datetime.utcnow().astimezone().tzinfo) - approval_request.created_at if approval_request.created_at.tzinfo else datetime.now() - approval_request.created_at
            if time_since_submission.days > max_withdrawal_days:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Withdrawal window has expired. Requests can only be withdrawn within {max_withdrawal_days} days of submission.")

        approval_request.status = ApprovalRequestStatusEnum.WITHDRAWN
        approval_request.withdrawn_at = func.now()
        approval_request.updated_at = func.now()
        approval_request.reason = "Maker withdrew the request."

        db.add(approval_request)
        db.flush()

        log_action(
            db,
            user_id=maker_user_id,
            action_type=AUDIT_ACTION_TYPE_APPROVAL_REQUEST_WITHDRAWN,
            entity_type="ApprovalRequest",
            entity_id=approval_request.id,
            details={
                "entity_type_withdrawn": approval_request.entity_type,
                "entity_id_withdrawn": approval_request.entity_id,
                "action_type_withdrawn": approval_request.action_type,
                "maker_user_id": approval_request.maker_user_id,
            },
            customer_id=customer_id,
            lg_record_id=approval_request.entity_id if approval_request.entity_type == "LGRecord" else None,
        )
        _nuke_document(db, approval_request.request_details or {})
        db.refresh(approval_request)
        return approval_request

    def auto_reject_expired_requests(self, db: Session) -> List[models.ApprovalRequest]:
        auto_rejected_requests = []
        import app.crud.crud as crud_instances
        max_pending_days_config = crud_instances.crud_global_configuration.get_by_key(db, GlobalConfigKey.APPROVAL_REQUEST_MAX_PENDING_DAYS)

        if not max_pending_days_config or not max_pending_days_config.value_default:
            logger.warning("APPROVAL_REQUEST_MAX_PENDING_DAYS not configured or has no default value. Auto-rejection skipped.")
            return []

        try:
            max_pending_days = int(max_pending_days_config.value_default)
        except ValueError:
            logger.error(f"Invalid value_default for {GlobalConfigKey.APPROVAL_REQUEST_MAX_PENDING_DAYS.value}. Expected integer. Auto-rejection skipped.", exc_info=True)
            return []

        cutoff_date = datetime.now(datetime.utcnow().astimezone().tzinfo) - timedelta(days=max_pending_days)


        expired_requests = db.query(self.model).filter(
            self.model.status == ApprovalRequestStatusEnum.PENDING,
            self.model.created_at < cutoff_date
        ).all()

        for req in expired_requests:
            req.status = ApprovalRequestStatusEnum.AUTO_REJECTED_EXPIRED
            req.updated_at = func.now()
            req.reason = f"Request automatically rejected as it exceeded the maximum pending duration of {max_pending_days} days."
            db.add(req)
            auto_rejected_requests.append(req)

            log_action(
                db,
                user_id=None,
                action_type=AUDIT_ACTION_TYPE_APPROVAL_REQUEST_AUTO_REJECTED,
                entity_type="ApprovalRequest",
                entity_id=req.id,
                details={
                    "entity_type_auto_rejected": req.entity_type,
                    "entity_id_auto_rejected": req.entity_id,
                    "action_type_auto_rejected": req.action_type,
                    "maker_user_id": req.maker_user_id,
                    "reason": req.reason,
                    "max_pending_days_configured": max_pending_days
                },
                customer_id=req.customer_id,
                lg_record_id=req.entity_id if req.entity_type == "LGRecord" else None,
            )
            _nuke_document(db, req.request_details or {})
        if auto_rejected_requests:
            db.commit()
            for req in auto_rejected_requests:
                db.refresh(req)
            logger.info(f"Auto-rejected {len(auto_rejected_requests)} expired approval requests.")

        return auto_rejected_requests

    async def _send_approval_for_processing_notification(self, db: Session, approval_request: models.ApprovalRequest):
        logger.info(f"Attempting to send 'Approval for Processing' notification for Approval Request ID: {approval_request.id}.")

        if not approval_request.maker_user:
            logger.error(f"Cannot send 'Approval for Processing' notification for AR {approval_request.id}: Maker user is missing or not loaded.")
            log_action(
                db,
                user_id=approval_request.checker_user_id,
                action_type="NOTIFICATION_FAILED",
                entity_type="ApprovalRequest",
                entity_id=approval_request.id,
                details={"reason": "Maker user missing for approval for processing notification", "recipient": "N/A"},
                customer_id=approval_request.customer_id,
                lg_record_id=approval_request.entity_id if approval_request.entity_type == "LGRecord" else None,
            )
            return
        
        if not approval_request.related_instruction or not approval_request.lg_record:
            logger.error(f"Cannot send 'Approval for Processing' notification for AR {approval_request.id}: Related instruction or LG Record missing/not loaded. Instruction ID: {approval_request.related_instruction_id}.")
            log_action(
                db,
                user_id=approval_request.checker_user_id,
                action_type="NOTIFICATION_FAILED",
                entity_type="ApprovalRequest",
                entity_id=approval_request.id,
                details={"reason": "Related instruction or LG Record missing for approval for processing notification", "recipient": approval_request.maker_user.email},
                customer_id=approval_request.customer_id,
                lg_record_id=approval_request.entity_id if approval_request.entity_type == "LGRecord" else None,
            )
            return

        maker_email = approval_request.maker_user.email
        to_emails = [maker_email]
        cc_emails = []

        email_settings_to_use: EmailSettings
        email_method_for_log: str
        try:
            email_settings_to_use, email_method_for_log = get_customer_email_settings(db, approval_request.customer_id)
        except Exception as e:
            email_settings_to_use = get_global_email_settings()
            email_method_for_log = "global_fallback_due_to_error"
            logger.warning(f"Failed to retrieve customer-specific email settings for customer ID {approval_request.customer_id}: {e}. Falling back to global settings for 'Approval for Processing' notification.")

        import app.crud.crud as crud_instances
        common_comm_list_config = crud_instances.crud_customer_configuration.get_customer_config_or_global_fallback(
            db, approval_request.customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
        )
        if common_comm_list_config and common_comm_list_config.get('effective_value'):
            try:
                parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                    cc_emails.extend(parsed_common_list)
            except json.JSONDecodeError:
                logger.warning(f"COMMON_COMMUNICATION_LIST for customer {approval_request.customer_id} is not a valid JSON list of emails. Skipping for 'Approval for Processing' notification.")
        cc_emails = list(set(cc_emails))

        notification_template = db.query(models.Template).filter(
            models.Template.action_type == "APPROVAL_READY_FOR_PRINT",
            models.Template.is_global == True,
            models.Template.is_notification_template == True,
            models.Template.is_deleted == False
        ).first()

        if not notification_template:
            logger.error(f"Email notification template with name 'Approval Ready for Print' not found. Cannot send 'Approval for Processing' notification for Approval Request {approval_request.id}.")
            log_action(
                db,
                user_id=approval_request.checker_user_id,
                action_type="NOTIFICATION_FAILED",
                entity_type="ApprovalRequest",
                entity_id=approval_request.id,
                details={"reason": "'APPROVAL_READY_FOR_PRINT' template missing for approval for processing notification", "recipient": to_emails},
                customer_id=approval_request.customer_id,
                lg_record_id=approval_request.entity_id if approval_request.entity_type == "LGRecord" else None,
            )
            return

        lg_record = approval_request.lg_record
        instruction = approval_request.related_instruction

        template_data = {
            "maker_email": maker_email,
            "maker_name": approval_request.maker_user.email.split('@')[0],
            "checker_email": approval_request.checker_user.email if approval_request.checker_user else "N/A",
            "approval_request_id": approval_request.id,
            "action_type": approval_request.action_type.replace('_', ' ').title(),
            "lg_number": lg_record.lg_number,
            "lg_amount": float(lg_record.lg_amount),
            "lg_currency_code": lg_record.lg_currency.iso_code,
            "instruction_serial_number": instruction.serial_number,
            "current_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "platform_name": "Treasury Management Platform",
            "print_link": f"/api/v1/end-user/lg-records/instructions/{instruction.id}/view-letter?print=true",
            "action_center_link": "/end-user/action-center"
        }

        template_data["lg_amount_formatted"] = f"{template_data['lg_currency_code']} {template_data['lg_amount']:,.2f}" if isinstance(template_data['lg_amount'], (float, int, decimal.Decimal)) else template_data['lg_amount']

        email_subject = notification_template.subject if notification_template.subject else f"Approved: Action on LG #{{lg_number}} - Ready for Processing"
        email_body_html = notification_template.content
        for key, value in template_data.items():
            str_value = str(value) if value is not None else ""
            email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
            email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

        try:
            email_sent_successfully = await send_email(
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
                    db,
                    user_id=approval_request.checker_user_id,
                    action_type="NOTIFICATION_SENT",
                    entity_type="ApprovalRequest",
                    entity_id=approval_request.id,
                    details={
                        "recipient": to_emails,
                        "cc_recipients": cc_emails,
                        "subject": email_subject,
                        "method": email_method_for_log,
                        "notification_type": "Approval For Processing"
                    },
                    customer_id=approval_request.customer_id,
                    lg_record_id=approval_request.entity_id if approval_request.entity_type == "LGRecord" else None,
                )
                logger.info(f"'Approval for Processing' notification sent successfully for Approval Request ID: {approval_request.id} to maker {maker_email}.")
            else:
                logger.error(f"send_email returned False for 'Approval for Processing' notification for Approval Request ID: {approval_request.id}.")
                log_action(
                    db,
                    user_id=approval_request.checker_user_id,
                    action_type="NOTIFICATION_FAILED",
                    entity_type="ApprovalRequest",
                    entity_id=approval_request.id,
                    details={"reason": "Email service failed to send print-ready notification (send_email returned False)", "recipient": to_emails, "subject": email_subject, "method": email_method_for_log},
                    customer_id=approval_request.customer_id,
                    lg_record_id=approval_request.entity_id if approval_request.entity_type == "LGRecord" else None,
                )
        except Exception as e:
            logger.exception(f"Exception occurred while sending 'Approval for Processing' notification for Approval Request ID: {approval_request.id}: {e}")
            log_action(
                db,
                user_id=approval_request.checker_user_id,
                action_type="NOTIFICATION_FAILED",
                entity_type="ApprovalRequest",
                entity_id=approval_request.id,
                details={"reason": f"Exception during email send: {e}", "recipient": to_emails, "subject": email_subject, "method": email_method_for_log},
                customer_id=approval_request.customer_id,
                lg_record_id=approval_request.entity_id if approval_request.entity_type == "LGRecord" else None,
            )
crud_approval_request = CRUDApprovalRequest(models.ApprovalRequest)
