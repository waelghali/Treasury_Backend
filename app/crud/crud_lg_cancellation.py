# app/crud/crud_lg_cancellation.py

from typing import Any, List, Optional, Type, Dict, Tuple

from fastapi import HTTPException, status
from sqlalchemy.orm import Session, selectinload
from datetime import datetime, timedelta, date
from decimal import Decimal

from app.crud.crud import CRUDBase, log_action
import app.models as models
from app.schemas.all_schemas import LGInstructionCancelRequest
from app.constants import (
    GlobalConfigKey,
    ACTION_TYPE_LG_CANCEL_LAST_INSTRUCTION,
    AUDIT_ACTION_TYPE_LG_INSTRUCTION_CANCELED,
    AUDIT_ACTION_TYPE_LG_INSTRUCTION_CANCELLATION_FAILED,
    INSTRUCTION_TYPE_CODE_TO_FULL_ACTION_MAP,
    InstructionTypeCode,
    LgStatusEnum,
    LgTypeEnum,
)
# NEW: Import timezone from the datetime module
from datetime import timezone

import logging
logger = logging.getLogger(__name__)

# This class will be instantiated in app/crud/crud.py and passed dependencies
class CRUDLGCancellation(CRUDBase):
    def __init__(self, model: Type[models.LGInstruction], crud_lg_record_instance: Any, crud_customer_configuration_instance: Any, crud_user_instance: Any):
        self.model = model
        self.crud_lg_record_instance = crud_lg_record_instance
        self.crud_customer_configuration_instance = crud_customer_configuration_instance
        self.crud_user_instance = crud_user_instance

    async def cancel_instruction(
        self,
        db: Session,
        instruction_id: int,
        cancel_in: LGInstructionCancelRequest,
        user_id: int,
        customer_id: int,
        approval_request_id: Optional[int] = None,
    ) -> Tuple[models.LGInstruction, models.LGRecord]:
        
        # 1. Fetch the instruction to cancel
        db_instruction = self.get(db, instruction_id)
        if not db_instruction or db_instruction.is_deleted:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instruction not found or not accessible.")
        
        # 2. Check LG Record ownership and existence
        db_lg_record = self.crud_lg_record_instance.get_lg_record_with_relations(db, db_instruction.lg_record_id, customer_id)
        if not db_lg_record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Associated LG Record not found or not accessible.")

        # 3. Check if this is the last instruction for the LG
        last_instruction = db.query(self.model).filter(
            self.model.lg_record_id == db_lg_record.id,
            self.model.is_deleted == False
        ).order_by(self.model.created_at.desc()).first()
        
        if not last_instruction or last_instruction.id != db_instruction.id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only the most recent instruction can be canceled.")

        # 4. Check eligibility rules
        # Only instructions that generate bank letters are cancellable
        cancellable_instruction_types = list(INSTRUCTION_TYPE_CODE_TO_FULL_ACTION_MAP.values())
        if db_instruction.instruction_type not in cancellable_instruction_types:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Instruction type '{db_instruction.instruction_type}' is not eligible for cancellation.")
            
        if db_instruction.status not in ["Instruction Issued", "Reminder Issued"]:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Instruction status '{db_instruction.status}' is not eligible for cancellation. It must be 'Instruction Issued' or 'Reminder Issued'.")
        
        # 5. Check time window
        cancellation_window_config = self.crud_customer_configuration_instance.get_customer_config_or_global_fallback(
            db, customer_id, GlobalConfigKey.MAX_DAYS_FOR_LAST_INSTRUCTION_CANCELLATION
        )
        cancellation_window_days = int(cancellation_window_config.get('effective_value', 7)) # Use days from the config

        # FIX: Make datetime.now() timezone-aware by using timezone.utc
        time_since_creation = datetime.now(timezone.utc) - db_instruction.created_at
        if time_since_creation.days > cancellation_window_days:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"The cancellation window of {cancellation_window_days} days has expired for this instruction.")

        # 6. Apply rollback logic
        # This needs a specific helper method to reverse the action.
        updated_lg_record = await self._rollback_lg_state(db, db_lg_record, db_instruction)

        # 7. Update instruction status and details
        db_instruction.status = "Canceled"
        db_instruction.details = {
            "cancellation_details": {
                "reason": cancel_in.reason,
                "declaration_confirmed": cancel_in.declaration_confirmed,
                "canceled_by_user_id": user_id,
                # FIX: Make datetime.now() timezone-aware
                "canceled_at": datetime.now(timezone.utc).isoformat(),
            },
            **(db_instruction.details or {})
        }
        db.add(db_instruction)
        db.flush()
        db.refresh(db_instruction)

        # 8. Log the action
        log_action(
            db,
            user_id=user_id,
            action_type=AUDIT_ACTION_TYPE_LG_INSTRUCTION_CANCELED,
            entity_type="LGInstruction",
            entity_id=db_instruction.id,
            details={
                "lg_record_id": db_lg_record.id,
                "lg_number": db_lg_record.lg_number,
                "canceled_instruction_type": db_instruction.instruction_type,
                "canceled_instruction_serial": db_instruction.serial_number,
                "reason": cancel_in.reason,
                "maker_checker_involved": approval_request_id is not None,
                # Add rollback details to the log
                "rolled_back_fields": "LG state has been rolled back to pre-instruction values."
            },
            customer_id=customer_id,
            lg_record_id=db_lg_record.id,
        )

        db.refresh(updated_lg_record)
        return db_instruction, updated_lg_record

    async def _rollback_lg_state(self, db: Session, lg_record: models.LGRecord, instruction: models.LGInstruction) -> models.LGRecord:
        """
        Helper method to reverse the changes made by a specific instruction.
        This is the most complex part of the implementation.
        """
        # Load the snapshot from the instruction's details if it exists
        # In this implementation, the `lg_record_snapshot` is stored in the `ApprovalRequest` not the instruction itself.
        # So we need to fetch the related approval request.
        
        # A simplified approach for direct cancellation is to reconstruct the old state from the instruction details.
        # This assumes the instruction.details field contains all necessary information to reverse the action.
        
        if instruction.instruction_type == "LG_EXTENSION":
            # Reverse extension: change expiry date back to old_expiry_date
            old_expiry_date = instruction.details.get("old_expiry_date")
            if old_expiry_date:
                # FIX: Change from datetime.strptime to date.fromisoformat, it's safer
                lg_record.expiry_date = datetime.combine(date.fromisoformat(old_expiry_date), datetime.min.time())
                db.add(lg_record)
                db.flush()
        elif instruction.instruction_type == "LG_RELEASE":
            # Reverse release: change status back to VALID
            lg_record.lg_status_id = LgStatusEnum.VALID.value
            db.add(lg_record)
            db.flush()
        elif instruction.instruction_type == "LG_LIQUIDATION":
            liquidation_type = instruction.details.get("liquidation_type")
            if liquidation_type == "full":
                # Reverse full liquidation: change amount back to old_amount and status to VALID
                old_amount = instruction.details.get("original_lg_amount")
                if old_amount:
                    lg_record.lg_amount = Decimal(str(old_amount))
                    lg_record.lg_status_id = LgStatusEnum.VALID.value
                    db.add(lg_record)
                    db.flush()
            elif liquidation_type == "partial":
                # Reverse partial liquidation: change amount back to old_amount and status remains VALID
                old_amount = instruction.details.get("original_lg_amount")
                if old_amount:
                    lg_record.lg_amount = Decimal(str(old_amount))
                    db.add(lg_record)
                    db.flush()
        elif instruction.instruction_type == "LG_DECREASE_AMOUNT":
            # Reverse decrease amount: change amount back to old_amount
            old_amount = instruction.details.get("original_lg_amount")
            if old_amount:
                lg_record.lg_amount = Decimal(str(old_amount))
                db.add(lg_record)
                db.flush()
        elif instruction.instruction_type == "LG_ACTIVATE_NON_OPERATIVE":
            # Reverse activation: change operational status back to NON_OPERATIVE
            lg_record.lg_operational_status_id = models.LgOperationalStatusEnum.NON_OPERATIVE.value
            db.add(lg_record)
            db.flush()
        # Add other types as needed
        db.refresh(lg_record)
        return lg_record