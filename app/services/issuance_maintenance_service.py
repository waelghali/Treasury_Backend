                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        # app/services/issuance_maintenance_service.py
"""
Service layer for Issuance LG Maintenance Actions:
  EXTEND, INCREASE_AMOUNT, CLOSE, LIQUIDATION, AMENDMENT, ACTIVATE

Mirrors custody patterns (crud_lg_record.py) but from the issuer side.
Phase A: No custody code is touched.
"""

import logging
from datetime import date, datetime
from typing import Optional, List, Dict, Any
from decimal import Decimal

from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.models.models_issuance import (
    IssuedLGRecord, IssuanceMaintenanceAction,
    IssuanceWorkflowPolicy, IssuanceExposureEntry,
    IssuanceFacility, IssuanceFacilitySubLimit
)
from app.crud.crud import log_action

logger = logging.getLogger(__name__)

# Action types that require approval matrix
ACTIONS_REQUIRING_APPROVAL = {"EXTEND", "INCREASE_AMOUNT", "AMENDMENT", "ACTIVATE"}
# Action types that generate a bank letter
ACTIONS_WITH_LETTER = {"EXTEND", "INCREASE_AMOUNT", "CLOSE", "AMENDMENT", "ACTIVATE"}
# Action types that can be executed directly (no approval, no letter for some)
ACTIONS_DIRECT_EXECUTE = {"CLOSE", "LIQUIDATION", "CHANGE_OWNERSHIP"}


class IssuanceMaintenanceService:

    # ──────────────────────────────────────────────────
    # 1. CREATE ACTION
    # ──────────────────────────────────────────────────
    def create_action(
        self,
        db: Session,
        issued_lg_id: int,
        action_type: str,
        action_data: Dict[str, Any],
        user_id: int,
        customer_id: int,
        notes: Optional[str] = None,
        initiation_source: str = "INTERNAL_USER",
    ) -> IssuanceMaintenanceAction:
        """
        Creates a maintenance action. Routes to approval matrix if needed,
        otherwise sets status to APPROVED (ready for execution).
        """
        # Validate LG exists and belongs to customer
        lg = db.query(IssuedLGRecord).filter(
            IssuedLGRecord.id == issued_lg_id,
            IssuedLGRecord.customer_id == customer_id
        ).first()
        if not lg:
            raise HTTPException(status_code=404, detail="Issued LG not found")

        # Validate LG is in a state that allows maintenance
        if lg.status != "ACTIVE":
            raise HTTPException(status_code=400,
                detail=f"Cannot perform {action_type} on LG with status {lg.status}. Only ACTIVE LGs allow maintenance actions.")

        # Validate action type
        valid_types = {"EXTEND", "INCREASE_AMOUNT", "CLOSE", "LIQUIDATION", "AMENDMENT", "ACTIVATE", "CHANGE_OWNERSHIP"}
        if action_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Invalid action type: {action_type}")

        # ACTIVATE: server-side enforcement — advance payment + non-operative + one-time
        if action_type == "ACTIVATE":
            from app.constants import LgTypeEnum
            if lg.lg_type_id != LgTypeEnum.ADVANCE_PAYMENT_GUARANTEE:
                raise HTTPException(status_code=400, detail="ACTIVATE is only available for Advance Payment LGs.")
            # Check operational_status: prefer lg field, fallback to request join
            op_status = lg.operational_status
            if not op_status and lg.request_id:
                from app.models.models_issuance import IssuanceRequest
                req = db.query(IssuanceRequest.operational_status).filter(
                    IssuanceRequest.id == lg.request_id
                ).first()
                op_status = req.operational_status if req else None

            if not op_status or op_status.strip().lower() not in (
                "non-operative", "none operative", "non_operative"
            ):
                raise HTTPException(status_code=400, detail="ACTIVATE is only available for Non-Operative LGs.")
            # One-time check
            existing_activation = db.query(IssuanceMaintenanceAction).filter(
                IssuanceMaintenanceAction.issued_lg_id == issued_lg_id,
                IssuanceMaintenanceAction.action_type == "ACTIVATE",
                IssuanceMaintenanceAction.status.in_(["APPROVED", "EXECUTED", "COMPLETED"]),
            ).first()
            if existing_activation:
                raise HTTPException(status_code=400, detail="This LG has already been activated. ACTIVATE is a one-time action.")

        # ── Guard: block duplicate / simultaneous maintenance actions ──
        # 1) Same-type always blocked
        same_type_pending = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.issued_lg_id == issued_lg_id,
            IssuanceMaintenanceAction.action_type == action_type,
            IssuanceMaintenanceAction.status == "PENDING_APPROVAL",
        ).first()
        if same_type_pending:
            raise HTTPException(
                status_code=409,
                detail=f"A {action_type.replace('_', ' ')} action is already pending approval for this LG. "
                       f"Please wait for it to be approved or rejected before submitting another."
            )

        # 2) Different-type blocked if config says so
        from app.crud.crud_config import crud_customer_configuration
        from app.constants import GlobalConfigKey
        config_result = crud_customer_configuration.get_customer_config_or_global_fallback(
            db, customer_id, GlobalConfigKey.ALLOW_SIMULTANEOUS_MAINTENANCE
        )
        # Default to true (allow) if config not found
        allow_simultaneous = True
        if config_result:
            effective = str(config_result.get("effective_value", "true")).lower()
            allow_simultaneous = effective == "true"
        if not allow_simultaneous:
            any_pending = db.query(IssuanceMaintenanceAction).filter(
                IssuanceMaintenanceAction.issued_lg_id == issued_lg_id,
                IssuanceMaintenanceAction.status == "PENDING_APPROVAL",
            ).first()
            if any_pending:
                pending_label = any_pending.action_type.replace("_", " ")
                raise HTTPException(
                    status_code=409,
                    detail=f"Another maintenance action ({pending_label}) is already pending approval for this LG. "
                           f"Simultaneous maintenance actions are not allowed. "
                           f"Please wait for it to be resolved before submitting a new one."
                )

        # Action-specific validation
        self._validate_action_data(db, action_type, action_data, lg)

        # Create the action record
        action = IssuanceMaintenanceAction(
            issued_lg_id=issued_lg_id,
            action_type=action_type,
            action_data=action_data,
            initiated_by_user_id=user_id,
            notes=notes,
            initiation_source=initiation_source,
        )

        # Route through approval matrix or execute directly
        if action_type in ACTIONS_REQUIRING_APPROVAL:
            next_policy, approver_ids = self._find_next_approval_step(
                db, customer_id, start_sequence=0,
                lg=lg, action_data=action_data, initiator_user_id=user_id
            )
            
            if next_policy:
                action.status = "PENDING_APPROVAL"
                action.current_step_number = next_policy.step_sequence
                action.pending_approver_users = approver_ids
                action.approval_history = [{
                    "action": "SUBMITTED",
                    "user_id": user_id,
                    "timestamp": str(datetime.utcnow())
                }]
            else:
                # Distinguish: "no policies configured" vs "policies exist but none match"
                has_any_policies = db.query(IssuanceWorkflowPolicy).filter(
                    IssuanceWorkflowPolicy.customer_id == customer_id,
                    IssuanceWorkflowPolicy.is_active == True,
                ).count() > 0

                if has_any_policies:
                    # Policies exist but _find_next_approval_step found no matching step
                    # This could be a coverage gap — block to prevent silent auto-approve
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"This {action_type} action does not match any configured approval "
                            "workflow rule. Please contact your Corporate Admin to review "
                            "the approval matrix configuration."
                        )
                    )

                # No approval steps configured at all — auto-approve
                action.status = "APPROVED"
                action.approval_history = [{
                    "action": "AUTO_APPROVED",
                    "reason": "No approval steps configured",
                    "timestamp": str(datetime.utcnow())
                }]
        else:
            # Close, Liquidation, Activate — no approval needed
            action.status = "APPROVED"
            action.approval_history = [{
                "action": "DIRECT_ACTION",
                "user_id": user_id,
                "timestamp": str(datetime.utcnow())
            }]

        db.add(action)
        db.flush()

        # Generate sub-serial from the LG's internal serial
        # Format: YYYY-XXXX-NNNNSSS where SSS increments for each maintenance action
        if lg.internal_serial:
            try:
                base_prefix = lg.internal_serial[:-3]  # "2026-ACME-0001"
                # Query max existing sub-serial for this LG
                last_sub = db.query(IssuanceMaintenanceAction.letter_serial_number).filter(
                    IssuanceMaintenanceAction.issued_lg_id == issued_lg_id,
                    IssuanceMaintenanceAction.letter_serial_number.like(f"{base_prefix}%"),
                ).order_by(IssuanceMaintenanceAction.letter_serial_number.desc()).first()

                if last_sub and last_sub[0]:
                    try:
                        last_sub_num = int(last_sub[0][-3:])
                        next_sub = last_sub_num + 1
                    except ValueError:
                        next_sub = 1
                else:
                    next_sub = 1

                action.letter_serial_number = f"{base_prefix}{next_sub:03d}"
                db.flush()
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"Failed to generate maintenance sub-serial: {e}")

        # If auto-approved or direct, execute immediately
        if action.status == "APPROVED":
            self._execute_action(db, action, lg, user_id)

        db.commit()
        db.refresh(action)

        log_action(
            db, user_id=user_id,
            action_type=f"ISSUANCE_MAINTENANCE_{action_type}_CREATED",
            entity_type="IssuanceMaintenanceAction",
            entity_id=action.id,
            details={
                "lg_ref": lg.lg_ref_number,
                "internal_serial": lg.internal_serial,
                "letter_serial_number": action.letter_serial_number,
                "action_type": action_type,
                "status": action.status,
                "action_data": action_data,
            },
            customer_id=customer_id
        )

        return action

    # ──────────────────────────────────────────────────
    # 2. APPROVE ACTION
    # ──────────────────────────────────────────────────
    def approve_action(
        self,
        db: Session,
        action_id: int,
        approver_user_id: int,
        customer_id: int,
    ) -> IssuanceMaintenanceAction:
        """Processes an approval step. Same logic as issuance request approval."""
        action = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.id == action_id
        ).first()
        if not action or action.status != "PENDING_APPROVAL":
            raise HTTPException(status_code=400, detail="Action is not pending approval")

        lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
        if not lg or lg.customer_id != customer_id:
            raise HTTPException(status_code=404, detail="LG not found")

        # Verify authorization
        allowed_users = [int(uid) for uid in (action.pending_approver_users or [])]
        if approver_user_id not in allowed_users:
            raise HTTPException(status_code=403, detail="You are not authorized to approve this step.")

        # Block self-approval
        if approver_user_id == action.initiated_by_user_id:
            raise HTTPException(status_code=403, detail="You cannot approve your own action.")

        # Check for duplicate approval
        history = action.approval_history or []
        for entry in history:
            if (entry.get("step") == action.current_step_number and 
                entry.get("user_id") == approver_user_id):
                raise HTTPException(status_code=400, detail="You have already approved this step.")

        # Record approval
        history.append({
            "action": "APPROVED_STEP",
            "step": action.current_step_number,
            "user_id": approver_user_id,
            "timestamp": str(datetime.utcnow())
        })
        action.approval_history = list(history)

        # Check if step is complete (simplified: 1 signature per step)
        current_policy = db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == customer_id,
            IssuanceWorkflowPolicy.step_sequence == action.current_step_number
        ).first()
        required_sigs = current_policy.required_signatures if current_policy else 1

        # Count signatures for current step
        step_sigs = sum(
            1 for e in history 
            if e.get("action") == "APPROVED_STEP" and e.get("step") == action.current_step_number
        )

        if step_sigs >= required_sigs:
            # Step complete — find next
            next_policy, approver_ids = self._find_next_approval_step(
                db, customer_id, start_sequence=action.current_step_number,
                lg=lg, action_data=action.action_data, initiator_user_id=action.initiated_by_user_id
            )
            if next_policy:
                action.current_step_number = next_policy.step_sequence
                action.pending_approver_users = approver_ids
            else:
                # Fully approved — execute
                action.status = "APPROVED"
                action.pending_approver_users = []
                history.append({
                    "action": "FULLY_APPROVED",
                    "timestamp": str(datetime.utcnow())
                })
                action.approval_history = list(history)
                self._execute_action(db, action, lg, approver_user_id)

        db.add(action)
        db.commit()
        db.refresh(action)

        log_action(
            db, user_id=approver_user_id,
            action_type=f"ISSUANCE_MAINTENANCE_{action.action_type}_APPROVED",
            entity_type="IssuanceMaintenanceAction",
            entity_id=action.id,
            details={"step": action.current_step_number, "status": action.status},
            customer_id=customer_id
        )

        return action

    # ──────────────────────────────────────────────────
    # 3. REJECT ACTION
    # ──────────────────────────────────────────────────
    def reject_action(
        self,
        db: Session,
        action_id: int,
        user_id: int,
        customer_id: int,
        reason: Optional[str] = None,
    ) -> IssuanceMaintenanceAction:
        action = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.id == action_id
        ).first()
        if not action or action.status != "PENDING_APPROVAL":
            raise HTTPException(status_code=400, detail="Action is not pending approval")

        action.status = "REJECTED"
        action.pending_approver_users = []
        history = action.approval_history or []
        history.append({
            "action": "REJECTED",
            "user_id": user_id,
            "reason": reason,
            "timestamp": str(datetime.utcnow())
        })
        action.approval_history = list(history)

        db.add(action)
        db.commit()
        db.refresh(action)

        log_action(
            db, user_id=user_id,
            action_type=f"ISSUANCE_MAINTENANCE_{action.action_type}_REJECTED",
            entity_type="IssuanceMaintenanceAction",
            entity_id=action.id,
            details={"reason": reason},
            customer_id=customer_id
        )

        return action

    # ──────────────────────────────────────────────────
    # 4. RECORD DELIVERY
    # ──────────────────────────────────────────────────
    def record_delivery(
        self,
        db: Session,
        action_id: int,
        user_id: int,
        delivery_method: str,
        delivery_notes: Optional[str] = None,
        delivery_date_str: Optional[str] = None,
        delivery_document_bytes: Optional[bytes] = None,
        delivery_document_mime_type: Optional[str] = None,
        customer_id: Optional[int] = None,
    ) -> IssuanceMaintenanceAction:
        action = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.id == action_id
        ).first()
        if not action:
            raise HTTPException(status_code=404, detail="Action not found")
        if action.instruction_status not in ("Instruction Issued", "Printed"):
            raise HTTPException(status_code=400, detail="Action letter has not been issued yet")

        # Parse delivery date or default to now
        if delivery_date_str:
            try:
                from dateutil import parser as dateparser
                action.delivery_date = dateparser.parse(delivery_date_str)
            except Exception:
                action.delivery_date = datetime.utcnow()
        else:
            action.delivery_date = datetime.utcnow()

        action.instruction_status = "Instruction Delivered"
        action.delivery_method = delivery_method
        action.delivery_notes = delivery_notes

        # Upload delivery proof document to GCS if provided
        if delivery_document_bytes and len(delivery_document_bytes) > 0:
            try:
                import asyncio, uuid
                from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME

                lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
                cust_id = customer_id or (lg.customer_id if lg else 0)
                ext = (delivery_document_mime_type or "").split("/")[-1] or "pdf"
                import re as _re, datetime as _dt
                today = _dt.date.today().strftime('%Y%m%d')
                lg_ref_clean = _re.sub(r'[^\w\-]', '-', (lg.lg_ref_number or str(action.issued_lg_id))) if lg else str(action.issued_lg_id)
                unique_name = f"DELIVERY_{lg_ref_clean}_{action.action_type}_{today}_{uuid.uuid4().hex[:8]}.{ext}"
                # Organized: customer / requests / request_id / lg_id / maintenance / delivery
                req_id = lg.request_id if lg and lg.request_id else "no_request"
                blob_path = f"customer_{cust_id}/requests/{req_id}/lg_{action.issued_lg_id}/maintenance/delivery/{unique_name}"

                bucket = GCS_BUCKET_NAME
                if cust_id:
                    from app.crud import crud_customer_configuration
                    bucket_cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
                        db, cust_id, "STORAGE_BUCKET_NAME"
                    )
                    if bucket_cfg and bucket_cfg.get("effective_value"):
                        bucket = bucket_cfg["effective_value"]

                loop = asyncio.new_event_loop()
                try:
                    gcs_uri = loop.run_until_complete(
                        _upload_to_gcs(bucket, blob_path, delivery_document_bytes, delivery_document_mime_type)
                    )
                finally:
                    loop.close()
                if gcs_uri:
                    action.delivery_document_path = gcs_uri
                    logger.info(f"Delivery document uploaded for action {action.id}: {gcs_uri}")
            except Exception as e:
                logger.error(f"Failed to upload delivery document for action {action.id}: {e}", exc_info=True)
                # Non-fatal — delivery is still recorded without the document

        db.add(action)
        db.commit()
        db.refresh(action)
        return action

    # ──────────────────────────────────────────────────
    # 5. RECORD BANK REPLY
    # ──────────────────────────────────────────────────
    def record_bank_reply(
        self,
        db: Session,
        action_id: int,
        user_id: int,
        customer_id: int,
        bank_reply_notes: Optional[str] = None,
        bank_reply_file_bytes: Optional[bytes] = None,
        bank_reply_mime_type: Optional[str] = None,
        bank_reply_date_str: Optional[str] = None,
    ) -> IssuanceMaintenanceAction:
        """
        Records bank's reply and applies confirmed changes to the IssuedLGRecord.
        F3: Optionally accepts a bank reply document for AI verification.
        For CLOSE actions, this is what moves the LG to CLOSED status.
        """
        logger.info(f"record_bank_reply: START action_id={action_id}")
        action = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.id == action_id
        ).first()
        if not action:
            raise HTTPException(status_code=404, detail="Action not found")
        if action.instruction_status not in ("Instruction Issued", "Instruction Delivered"):
            raise HTTPException(status_code=400, detail="Cannot record bank reply for this action")

        # F3: AI verification on bank reply document (optional)
        ai_verification_result = None
        if bank_reply_file_bytes and bank_reply_mime_type:
            ai_verification_result = self._verify_bank_reply_with_ai(
                db, action, bank_reply_file_bytes, bank_reply_mime_type, user_id
            )
            # Store AI verification result in action_data
            updated_data = dict(action.action_data or {})
            updated_data["ai_verification"] = ai_verification_result
            action.action_data = updated_data

        logger.info(f"record_bank_reply: Setting instruction_status")
        action.bank_reply_notes = bank_reply_notes

        # Parse bank reply date or default to now
        if bank_reply_date_str:
            try:
                from dateutil import parser as dateparser
                action.bank_reply_date = dateparser.parse(bank_reply_date_str)
            except Exception:
                action.bank_reply_date = datetime.utcnow()
        else:
            action.bank_reply_date = datetime.utcnow()

        # Upload bank reply document to GCS for later access (separate from AI verification)
        if bank_reply_file_bytes and len(bank_reply_file_bytes) > 0:
            try:
                import asyncio, uuid
                from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME

                lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
                cust_id = customer_id or (lg.customer_id if lg else 0)
                ext = (bank_reply_mime_type or "").split("/")[-1] or "pdf"
                import re as _re, datetime as _dt
                today = _dt.date.today().strftime('%Y%m%d')
                lg_ref_clean = _re.sub(r'[^\w\-]', '-', (lg.lg_ref_number or str(action.issued_lg_id))) if lg else str(action.issued_lg_id)
                unique_name = f"BANK_REPLY_{lg_ref_clean}_{action.action_type}_{today}_{uuid.uuid4().hex[:8]}.{ext}"
                # Organized: customer / requests / request_id / lg_id / maintenance / bank_reply
                req_id = lg.request_id if lg and lg.request_id else "no_request"
                blob_path = f"customer_{cust_id}/requests/{req_id}/lg_{action.issued_lg_id}/maintenance/bank_reply/{unique_name}"

                bucket = GCS_BUCKET_NAME
                if cust_id:
                    from app.crud import crud_customer_configuration
                    bucket_cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
                        db, cust_id, "STORAGE_BUCKET_NAME"
                    )
                    if bucket_cfg and bucket_cfg.get("effective_value"):
                        bucket = bucket_cfg["effective_value"]

                loop = asyncio.new_event_loop()
                try:
                    gcs_uri = loop.run_until_complete(
                        _upload_to_gcs(bucket, blob_path, bank_reply_file_bytes, bank_reply_mime_type)
                    )
                finally:
                    loop.close()
                if gcs_uri:
                    action.bank_reply_document_path = gcs_uri
                    logger.info(f"Bank reply document uploaded for action {action.id}: {gcs_uri}")
            except Exception as e:
                logger.error(f"Failed to upload bank reply document for action {action.id}: {e}", exc_info=True)

        # Decision: if AI found mismatches, pause for user confirmation
        # If no file uploaded or AI verified clean → apply changes directly
        needs_user_confirmation = (
            ai_verification_result is not None
            and ai_verification_result.get("status") in ("mismatch", "ocr_failed", "ai_failed", "error")
        )

        if needs_user_confirmation:
            # Phase 1: Pause — user must review AI result and decide
            action.instruction_status = "Awaiting Confirmation"
            logger.info(f"record_bank_reply: AI found issues, pausing for user confirmation")
        else:
            # No file, or AI verified clean → apply changes immediately
            action.instruction_status = "Confirmed by Bank"
            lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
            if lg:
                logger.info(f"record_bank_reply: Applying confirmed changes to LG {lg.id}")
                self._apply_confirmed_changes(db, action, lg, user_id)
                logger.info(f"record_bank_reply: Changes applied")

        db.add(action)
        db.commit()
        db.refresh(action)

        log_action(
            db, user_id=user_id,
            action_type=f"ISSUANCE_MAINTENANCE_{action.action_type}_BANK_REPLY_RECORDED",
            entity_type="IssuanceMaintenanceAction",
            entity_id=action.id,
            details={
                "lg_ref": lg.lg_ref_number if lg else None,
                "notes": bank_reply_notes,
                "ai_verified": ai_verification_result is not None,
                "needs_confirmation": needs_user_confirmation,
            },
            customer_id=customer_id
        )
        logger.info(f"record_bank_reply: DONE")

        return action

    # ──────────────────────────────────────────────────
    # 5b. CONFIRM BANK REPLY (Phase 2 of two-phase flow)
    # ──────────────────────────────────────────────────
    def confirm_bank_reply(
        self,
        db: Session,
        action_id: int,
        user_id: int,
        customer_id: int,
    ) -> IssuanceMaintenanceAction:
        """
        Phase 2: User reviewed AI results and chose to proceed.
        Applies the confirmed changes to the LG record.
        """
        action = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.id == action_id
        ).first()
        if not action:
            raise HTTPException(status_code=404, detail="Action not found")
        if action.instruction_status != "Awaiting Confirmation":
            raise HTTPException(status_code=400, detail="Action is not awaiting confirmation")

        # Apply changes
        action.instruction_status = "Confirmed by Bank"
        lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
        if lg:
            self._apply_confirmed_changes(db, action, lg, user_id)

        # Record user decision in action_data
        updated_data = dict(action.action_data or {})
        if "ai_verification" in updated_data:
            updated_data["ai_verification"]["user_decision"] = "PROCEED_DESPITE_MISMATCH"
            updated_data["ai_verification"]["decided_by"] = user_id
            updated_data["ai_verification"]["decided_at"] = str(datetime.utcnow())
        action.action_data = updated_data

        db.add(action)
        db.commit()
        db.refresh(action)

        log_action(
            db, user_id=user_id,
            action_type=f"ISSUANCE_MAINTENANCE_{action.action_type}_BANK_CONFIRMED",
            entity_type="IssuanceMaintenanceAction",
            entity_id=action.id,
            details={
                "lg_ref": lg.lg_ref_number if lg else None,
                "user_decision": "PROCEED_DESPITE_MISMATCH",
            },
            customer_id=customer_id
        )

        return action

    # ──────────────────────────────────────────────────
    # 5c. CANCEL PENDING BANK REPLY (User chose not to proceed)
    # ──────────────────────────────────────────────────
    def cancel_pending_bank_reply(
        self,
        db: Session,
        action_id: int,
        user_id: int,
        customer_id: int,
    ) -> IssuanceMaintenanceAction:
        """
        User reviewed AI results and chose NOT to proceed.
        Reverts instruction_status back to Instruction Delivered (or Issued).
        """
        action = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.id == action_id
        ).first()
        if not action:
            raise HTTPException(status_code=404, detail="Action not found")
        if action.instruction_status != "Awaiting Confirmation":
            raise HTTPException(status_code=400, detail="Action is not awaiting confirmation")

        # Revert to previous state so user can try again
        action.instruction_status = "Instruction Delivered"
        action.bank_reply_date = None
        action.bank_reply_notes = None

        # Record cancellation in action_data
        updated_data = dict(action.action_data or {})
        if "ai_verification" in updated_data:
            updated_data["ai_verification"]["user_decision"] = "CANCELLED"
            updated_data["ai_verification"]["decided_by"] = user_id
            updated_data["ai_verification"]["decided_at"] = str(datetime.utcnow())
        action.action_data = updated_data

        db.add(action)
        db.commit()
        db.refresh(action)

        log_action(
            db, user_id=user_id,
            action_type=f"ISSUANCE_MAINTENANCE_{action.action_type}_BANK_REPLY_CANCELLED",
            entity_type="IssuanceMaintenanceAction",
            entity_id=action.id,
            details={"user_decision": "CANCELLED"},
            customer_id=customer_id
        )

        return action

    # ──────────────────────────────────────────────────
    # 6. CANCEL ACTION (within cancellation window)
    # ──────────────────────────────────────────────────
    def cancel_action(
        self,
        db: Session,
        action_id: int,
        user_id: int,
        customer_id: int,
        reason: str = "",
    ) -> IssuanceMaintenanceAction:
        """
        Cancel a recently executed maintenance action within the cancellation window.
        Rules:
        - Only the most recent action on the LG can be cancelled
        - Action must be in EXECUTED status with instruction_status = 'Instruction Issued'
        - Must be within the configurable cancellation window (default 24h)
        """
        action = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.id == action_id
        ).first()
        if not action:
            raise HTTPException(status_code=404, detail="Action not found")

        # Must be EXECUTED and not yet delivered/confirmed
        if action.status != "EXECUTED":
            raise HTTPException(status_code=400, detail="Only executed actions can be cancelled")
        if action.instruction_status not in ("Instruction Issued", None):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot cancel — instruction is already '{action.instruction_status}'"
            )

        # Must be the most recent action on this LG
        latest = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.issued_lg_id == action.issued_lg_id,
            IssuanceMaintenanceAction.status != "CANCELLED",
        ).order_by(IssuanceMaintenanceAction.created_at.desc()).first()
        if latest and latest.id != action.id:
            raise HTTPException(
                status_code=400,
                detail="Only the most recent action on this LG can be cancelled"
            )

        # Check cancellation window (configurable, default 24 hours)
        from app.crud import crud_customer_configuration
        window_hours = 24
        try:
            cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
                db, customer_id, "MAINTENANCE_CANCELLATION_WINDOW_HOURS"
            )
            if cfg and cfg.get("effective_value"):
                window_hours = int(cfg["effective_value"])
        except Exception:
            pass

        if action.created_at:
            elapsed = (datetime.utcnow() - action.created_at).total_seconds() / 3600
            if elapsed > window_hours:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cancellation window has expired ({window_hours}h). "
                           f"This action was created {elapsed:.1f} hours ago."
                )

        # Revert LG changes if applicable
        lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
        if lg and action.action_type == "LIQUIDATION":
            data = action.action_data or {}
            liq_type = data.get("liquidation_type", "FULL")
            if liq_type == "FULL":
                lg.status = "ACTIVE"  # Revert from LIQUIDATED
            # Partial liquidation reversal would need amount tracking — skip for now
            db.add(lg)

        # Mark cancelled
        action.status = "CANCELLED"
        action.instruction_status = None
        updated_data = dict(action.action_data or {})
        updated_data["cancellation"] = {
            "reason": reason,
            "cancelled_by": user_id,
            "cancelled_at": str(datetime.utcnow()),
        }
        action.action_data = updated_data

        db.add(action)
        db.commit()
        db.refresh(action)

        log_action(
            db, user_id=user_id,
            action_type=f"ISSUANCE_MAINTENANCE_{action.action_type}_CANCELLED",
            entity_type="IssuanceMaintenanceAction",
            entity_id=action.id,
            details={"reason": reason},
            customer_id=customer_id
        )

        return action

    # ──────────────────────────────────────────────────
    # PRIVATE: Execute action (apply changes + generate letter)
    # ──────────────────────────────────────────────────
    def _execute_action(
        self, db: Session, action: IssuanceMaintenanceAction,
        lg: IssuedLGRecord, user_id: int
    ):
        """
        Called when action is approved. For most actions, this generates the
        bank letter but does NOT apply the change yet (waits for bank confirmation).
        Exception: LIQUIDATION applies immediately (no letter).
        """
        action.status = "EXECUTED"
        action.executed_by_user_id = user_id
        data = action.action_data or {}

        # Snapshot before-state for history
        before = self._snapshot_lg(lg)

        # ── Capture "before" values into action_data for letter regeneration ──
        # This ensures the letter always shows the correct historical
        # data, even though the LG record has been updated.
        snapshot_for_letter = {
            "snapshot_expiry_date": str(lg.expiry_date) if lg.expiry_date else None,
            "snapshot_amount": str(lg.current_amount) if lg.current_amount else None,
            "snapshot_beneficiary_name": lg.beneficiary_name,
            "snapshot_beneficiary_address": getattr(lg, 'beneficiary_address', None),
            "snapshot_status": lg.status,
        }
        updated_data = dict(data)
        updated_data.update(snapshot_for_letter)
        action.action_data = updated_data
        data = updated_data  # refresh local reference

        if action.action_type == "LIQUIDATION":
            # Liquidation applies immediately (no bank letter)
            liq_type = data.get("liquidation_type", "FULL")
            if liq_type == "FULL":
                lg.status = "LIQUIDATED"
            else:
                # Partial — reduce amount
                reduction = Decimal(str(data.get("liquidation_amount", 0)))
                if reduction > 0 and reduction < lg.current_amount:
                    lg.current_amount = lg.current_amount - reduction
                else:
                    lg.status = "LIQUIDATED"  # If full amount, liquidate

            after = self._snapshot_lg(lg)
            self._record_history(lg, action.action_type, before, after, user_id, action.notes)
        else:
            # For actions with letters, mark instruction status (letter is regenerated on-the-fly when viewed)
            if action.action_type in ACTIONS_WITH_LETTER:
                action.instruction_status = "Instruction Issued"
                logger.info(f"Maintenance action {action.id} ({action.action_type}) marked as Instruction Issued")

            # For CLOSE: set to PENDING_CLOSE (not CLOSED yet)
            if action.action_type == "CLOSE":
                lg.status = "PENDING_CLOSE"
                after = self._snapshot_lg(lg)
                self._record_history(lg, action.action_type, before, after, user_id, action.notes)

            # CHANGE_OWNERSHIP: update the current_owner_user_id
            elif action.action_type == "CHANGE_OWNERSHIP":
                from app.models import User
                new_owner_id = data.get("new_owner_user_id")
                if new_owner_id:
                    new_owner = db.query(User).filter(User.id == new_owner_id).first()
                    if not new_owner:
                        logger.error(f"New owner user {new_owner_id} not found")
                    else:
                        lg.current_owner_user_id = new_owner_id
                        after = self._snapshot_lg(lg)
                        self._record_history(lg, action.action_type, before, after, user_id, 
                                            f"Ownership changed to user {new_owner.email or new_owner_id}")

            # EXTEND: DO NOT apply yet — wait for bank confirmation
            # Changes applied in _apply_confirmed_changes() after bank reply
            elif action.action_type == "EXTEND":
                logger.info(f"EXTEND action {action.id} approved for LG {lg.id}. "
                           f"New expiry: {data.get('new_expiry_date')}. Awaiting bank confirmation.")

            # INCREASE_AMOUNT: DO NOT apply yet — wait for bank confirmation
            # Changes applied in _apply_confirmed_changes() after bank reply
            elif action.action_type == "INCREASE_AMOUNT":
                logger.info(f"INCREASE_AMOUNT action {action.id} approved for LG {lg.id}. "
                           f"New amount: {data.get('new_amount')}. Awaiting bank confirmation.")

            # AMENDMENT: DO NOT apply yet — wait for bank confirmation
            # Changes applied in _apply_confirmed_changes() after bank reply
            elif action.action_type == "AMENDMENT":
                logger.info(f"AMENDMENT action {action.id} approved for LG {lg.id}. "
                           f"Awaiting bank confirmation.")

            # ACTIVATE: set LG operational_status to Operative
            elif action.action_type == "ACTIVATE":
                lg.operational_status = "Operative"
                payment_info = {
                    "payment_method": data.get("payment_method"),
                    "payment_amount": data.get("payment_amount"),
                    "payment_reference": data.get("payment_reference"),
                    "payment_date": data.get("payment_date"),
                }
                updated_data = dict(data)
                updated_data["payment_confirmed"] = True
                action.action_data = updated_data
                after = self._snapshot_lg(lg)
                self._record_history(lg, action.action_type, before, after, user_id, action.notes)
                logger.info(f"Activated LG {lg.id} with payment: {payment_info}")

        db.add(lg)

    # ──────────────────────────────────────────────────
    # PRIVATE: Apply confirmed changes (after bank reply)
    # ──────────────────────────────────────────────────
    def _apply_confirmed_changes(
        self, db: Session, action: IssuanceMaintenanceAction,
        lg: IssuedLGRecord, user_id: int
    ):
        """Applies the actual field changes after bank confirms."""
        data = action.action_data or {}
        before = self._snapshot_lg(lg)

        if action.action_type == "EXTEND":
            new_date = data.get("new_expiry_date")
            if new_date:
                lg.expiry_date = date.fromisoformat(new_date) if isinstance(new_date, str) else new_date

        elif action.action_type == "INCREASE_AMOUNT":
            new_amount = data.get("new_amount")
            if new_amount:
                old_amount = lg.current_amount
                lg.current_amount = Decimal(str(new_amount))

                # F4: Create exposure entry for the increase delta
                self._create_increase_exposure_entry(
                    db, lg, old_amount, Decimal(str(new_amount))
                )

        elif action.action_type == "CLOSE":
            lg.status = "CLOSED"

        elif action.action_type == "AMENDMENT":
            # Amendment: apply approved amendable fields from action_data
            AMENDABLE_FIELDS = {
                "new_beneficiary_name": "beneficiary_name",
                "new_beneficiary_address": "beneficiary_address",
                "new_lg_purpose": "lg_purpose",
            }

            for data_key, lg_field in AMENDABLE_FIELDS.items():
                new_value = data.get(data_key)
                if new_value is not None and new_value != "":
                    setattr(lg, lg_field, new_value)

        elif action.action_type == "ACTIVATE":
            # Mark as operative — apply payment details from action_data
            lg.operational_status = "Operative"

            # Record payment details in the action history (stored in action_data)
            payment_info = {
                "payment_method": data.get("payment_method"),
                "payment_amount": data.get("payment_amount"),
                "payment_reference": data.get("payment_reference"),
                "payment_date": data.get("payment_date"),
                "payment_currency_id": data.get("payment_currency_id"),
                "payment_bank_id": data.get("payment_bank_id"),
            }
            # Store payment info back in action_data for audit
            updated_data = dict(data)
            updated_data["payment_confirmed"] = True
            action.action_data = updated_data

            logger.info(f"Activated LG {lg.id} with payment: {payment_info}")

        elif action.action_type == "CHANGE_OWNERSHIP":
            new_owner_id = data.get("new_owner_user_id")
            if new_owner_id:
                lg.current_owner_user_id = int(new_owner_id)

        after = self._snapshot_lg(lg)
        self._record_history(lg, f"{action.action_type}_CONFIRMED", before, after, user_id, 
                            action.bank_reply_notes)
        db.add(lg)

    # ──────────────────────────────────────────────────
    # PRIVATE: Approval matrix helper
    # ──────────────────────────────────────────────────
    def _find_next_approval_step(self, db: Session, customer_id: int, start_sequence: int,
                                    lg: IssuedLGRecord = None, action_data: dict = None,
                                    initiator_user_id: int = None):
        """
        Finds the next applicable approval step after start_sequence.

        Context-aware: builds a pseudo-request from the LG record so that
        the issuance service's _evaluate_condition() can properly evaluate
        AMOUNT_OVER, AMOUNT_RANGE, DEPT_MATCH, CROSS_BORDER, etc.

        Falls back to the old behavior (accept any step with approvers)
        if no LG is provided (backward compatibility).
        """
        from app.services.issuance_service import IssuanceService
        from app.models.models_issuance import IssuanceRequest

        issuance_svc = IssuanceService()

        policies = db.query(IssuanceWorkflowPolicy).filter(
            IssuanceWorkflowPolicy.customer_id == customer_id,
            IssuanceWorkflowPolicy.step_sequence > start_sequence,
            IssuanceWorkflowPolicy.is_active == True
        ).order_by(IssuanceWorkflowPolicy.step_sequence).all()

        # Build a pseudo-request context from the LG record
        pseudo_request = None
        if lg:
            pseudo_request = self._build_pseudo_request(db, lg, action_data)

        for policy in policies:
            # Condition evaluation (context-aware if LG is provided)
            if pseudo_request:
                condition_met = issuance_svc._evaluate_condition(db, pseudo_request, policy)
                if not condition_met:
                    continue  # Skip this step — condition doesn't apply
            # else: no context → accept any step (legacy behavior)

            # Approver resolution (reuse issuance service's resolver)
            if pseudo_request:
                approver_ids = issuance_svc._resolve_approvers(
                    db, pseudo_request, policy, initiator_user_id
                )
            else:
                # Fallback: inline resolution (legacy)
                approver_ids = self._resolve_approvers_legacy(db, customer_id, policy)

            if approver_ids:
                return policy, list(set(approver_ids))

        return None, []

    def _build_pseudo_request(self, db: Session, lg: IssuedLGRecord, action_data: dict = None):
        """
        Builds a lightweight object that looks like an IssuanceRequest
        for condition evaluation purposes.

        Maps LG record fields to the attributes that _evaluate_condition() reads:
        - amount, currency_id, department, is_cross_border, is_third_party, reference_type
        """
        # Try to get original request for fields not on the LG record
        original_request = None
        if lg.request_id:
            from app.models.models_issuance import IssuanceRequest
            original_request = db.query(IssuanceRequest).filter(
                IssuanceRequest.id == lg.request_id
            ).first()

        # For INCREASE_AMOUNT, use the new amount for threshold comparison
        amount = lg.current_amount
        if action_data and action_data.get("new_amount"):
            try:
                amount = Decimal(str(action_data["new_amount"]))
            except Exception:
                pass

        # Build a simple namespace object (duck-typing for _evaluate_condition)
        class _PseudoRequest:
            pass

        pseudo = _PseudoRequest()
        pseudo.id = lg.id
        pseudo.customer_id = lg.customer_id
        pseudo.amount = amount
        pseudo.currency_id = lg.currency_id

        # Fields from original request (if available)
        pseudo.department = getattr(original_request, 'department', None)
        pseudo.is_cross_border = getattr(original_request, 'is_cross_border', False)
        pseudo.is_third_party = getattr(original_request, 'is_third_party', False)
        pseudo.reference_type = getattr(original_request, 'reference_type', None)
        pseudo.requestor_user_id = getattr(original_request, 'requestor_user_id', None)
        pseudo.approval_chain_audit = []  # No audit chain for maintenance pseudo-requests

        return pseudo

    def _resolve_approvers_legacy(self, db: Session, customer_id: int, policy):
        """Legacy approver resolution — used when no LG context is available."""
        from app.models.models import User, ApprovalGroup

        approver_ids = []

        if policy.approver_type == "USERS":
            approver_ids = [int(uid) for uid in (policy.approver_values or []) if str(uid).isdigit()]
        elif policy.approver_type == "ROLE":
            roles = [str(r).lower() for r in (policy.approver_values or [])]
            if roles:
                users = db.query(User.id).filter(
                    User.customer_id == customer_id,
                    User.role.in_(roles),
                    User.is_deleted == False
                ).all()
                approver_ids = [u[0] for u in users]
        elif policy.approver_type == "GROUP":
            group_ids = [int(gid) for gid in (policy.approver_values or []) if str(gid).isdigit()]
            if group_ids:
                groups = db.query(ApprovalGroup).filter(
                    ApprovalGroup.id.in_(group_ids),
                    ApprovalGroup.customer_id == customer_id,
                    ApprovalGroup.is_deleted == False
                ).all()
                for grp in groups:
                    for u in grp.users:
                        if not u.is_deleted:
                            approver_ids.append(u.id)
        elif policy.approver_type == "DEPT_HEAD":
            pass  # No department context in legacy mode

        return approver_ids

    # ──────────────────────────────────────────────────
    # PRIVATE: Validation helpers
    # ──────────────────────────────────────────────────
    def _validate_action_data(self, db: Session, action_type: str, data: dict, lg: IssuedLGRecord):
        if action_type == "EXTEND":
            new_date = data.get("new_expiry_date")
            if not new_date:
                raise HTTPException(status_code=400, detail="new_expiry_date is required for EXTEND")
            parsed = date.fromisoformat(new_date) if isinstance(new_date, str) else new_date
            if lg.expiry_date and parsed <= lg.expiry_date:
                raise HTTPException(status_code=400, detail="New expiry date must be after current expiry")

        elif action_type == "INCREASE_AMOUNT":
            new_amount = data.get("new_amount")
            if not new_amount:
                raise HTTPException(status_code=400, detail="new_amount is required for INCREASE_AMOUNT")
            if Decimal(str(new_amount)) <= lg.current_amount:
                raise HTTPException(status_code=400, detail="New amount must be greater than current amount")

            # F5: Capacity check — ensure facility has headroom for the increase
            self._check_increase_capacity(db, lg, Decimal(str(new_amount)))

        elif action_type == "LIQUIDATION":
            liq_type = data.get("liquidation_type")
            if liq_type not in ("FULL", "PARTIAL"):
                raise HTTPException(status_code=400, detail="liquidation_type must be FULL or PARTIAL")
            if liq_type == "PARTIAL":
                amount = data.get("liquidation_amount")
                if not amount or Decimal(str(amount)) <= 0:
                    raise HTTPException(status_code=400, detail="liquidation_amount required for partial")
                if Decimal(str(amount)) >= lg.current_amount:
                    raise HTTPException(status_code=400, detail="Partial amount must be less than current amount")

        elif action_type == "ACTIVATE":
            # Requires payment details — mirrors custody LGActivateNonOperativeRequest
            required_fields = ["payment_method", "payment_amount", "payment_reference", "payment_date"]
            missing = [f for f in required_fields if not data.get(f)]
            if missing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required payment fields for ACTIVATE: {', '.join(missing)}"
                )
            if float(data["payment_amount"]) <= 0:
                raise HTTPException(status_code=400, detail="Payment amount must be positive")

        elif action_type == "AMENDMENT":
            # At least one amendment field must be provided
            amendment_fields = [
                "new_beneficiary_name", "new_beneficiary_address", "new_lg_purpose",
            ]
            if not any(data.get(f) for f in amendment_fields):
                raise HTTPException(
                    status_code=400,
                    detail="At least one amendment field is required: new_beneficiary_name, new_beneficiary_address, or new_lg_purpose"
                )

    def _snapshot_lg(self, lg: IssuedLGRecord) -> dict:
        """Creates a snapshot of key LG fields for history."""
        return {
            "status": lg.status,
            "current_amount": str(lg.current_amount) if lg.current_amount else None,
            "expiry_date": str(lg.expiry_date) if lg.expiry_date else None,
            "beneficiary_name": lg.beneficiary_name,
            "beneficiary_address": getattr(lg, 'beneficiary_address', None),
            "beneficiary_country": getattr(lg, 'beneficiary_country', None),
            "lg_purpose": getattr(lg, 'lg_purpose', None),
            "department": getattr(lg, 'department', None),
            "reference_type": getattr(lg, 'reference_type', None),
            "current_owner_user_id": lg.current_owner_user_id,
        }

    def _record_history(self, lg: IssuedLGRecord, action_type: str, before: dict, 
                        after: dict, user_id: int, notes: Optional[str]):
        """Appends to the action_history JSON array on the LG record."""
        history = lg.action_history or []
        history.append({
            "action_type": action_type,
            "before": before,
            "after": after,
            "user_id": user_id,
            "timestamp": str(datetime.utcnow()),
            "notes": notes,
        })
        lg.action_history = list(history)

    # ──────────────────────────────────────────────────
    # 7. BANK-INITIATED CHANGES
    # ──────────────────────────────────────────────────
    def process_bank_initiated_change(
        self,
        db: Session,
        lg_id: int,
        file_bytes: bytes,
        mime_type: str,
        user_id: int,
        customer_id: int,
    ) -> dict:
        """
        Upload bank letter → AI extracts what changed → compare with LG → return diff.
        Creates a PENDING action with initiation_source='BANK_INITIATED'.
        Does NOT apply changes yet — returns diff for user confirmation.
        """
        import asyncio
        import uuid

        lg = db.query(IssuedLGRecord).filter(
            IssuedLGRecord.id == lg_id,
        ).first()
        if not lg:
            raise HTTPException(status_code=404, detail="LG record not found")

        # OCR the bank letter (reuse F3 infrastructure)
        from app.core.ai_integration import (
            extract_structured_data_with_gemini,
            perform_ocr_with_google_vision,
            _convert_pdf_to_images_and_upload_to_gcs,
            _cleanup_gcs_files,
            _upload_to_gcs,
            GCS_BUCKET_NAME,
        )

        target_bucket = GCS_BUCKET_NAME
        session_id = uuid.uuid4().hex
        unique_file_id = f"bank_initiated_{lg.lg_ref_number}_{session_id}"

        raw_text = ""
        if mime_type and mime_type.startswith("image/"):
            blob_name = f"lg_scans_temp/{unique_file_id}/image_{uuid.uuid4().hex}.{mime_type.split('/')[-1]}"
            gcs_uri = asyncio.get_event_loop().run_until_complete(
                _upload_to_gcs(target_bucket, blob_name, file_bytes, mime_type)
            )
            if gcs_uri:
                raw_text = asyncio.get_event_loop().run_until_complete(
                    perform_ocr_with_google_vision(gcs_uri, unique_file_id)
                ) or ""
        elif mime_type == "application/pdf":
            image_uris = asyncio.get_event_loop().run_until_complete(
                _convert_pdf_to_images_and_upload_to_gcs(file_bytes, target_bucket, unique_file_id)
            )
            texts = []
            for uri in (image_uris or []):
                page_text = asyncio.get_event_loop().run_until_complete(
                    perform_ocr_with_google_vision(uri, unique_file_id)
                )
                if page_text:
                    texts.append(page_text)
            raw_text = "\n".join(texts)

        if not raw_text:
            raise HTTPException(status_code=400, detail="Could not extract text from bank letter. Please try a clearer scan.")

        # Upload original to GCS for permanent storage
        doc_blob = f"issuance/bank_initiated/{lg.lg_ref_number}_{session_id}.{mime_type.split('/')[-1]}"
        doc_gcs_uri = asyncio.get_event_loop().run_until_complete(
            _upload_to_gcs(target_bucket, doc_blob, file_bytes, mime_type)
        )

        # AI extraction — tell it this is a bank-initiated change
        context = {
            "lg_record_details": {
                "lgNumber": lg.lg_ref_number or lg.bank_lg_number or "",
                "currentAmount": str(lg.current_amount) if lg.current_amount else None,
                "currentExpiryDate": str(lg.expiry_date) if lg.expiry_date else None,
                "beneficiaryName": lg.beneficiary_name,
                "status": lg.status,
            },
            "extraction_mode": "bank_initiated_change",
        }

        extracted_data, usage = asyncio.get_event_loop().run_until_complete(
            extract_structured_data_with_gemini(raw_text, unique_file_id, context=context)
        )

        # Cleanup temp files
        try:
            asyncio.get_event_loop().run_until_complete(
                _cleanup_gcs_files(target_bucket, f"lg_scans_temp/{unique_file_id}/")
            )
        except Exception:
            pass

        if not extracted_data:
            raise HTTPException(status_code=400, detail="AI could not extract structured data from bank letter")

        # Compare extracted data against current LG state to produce diff
        changes = []
        amended = extracted_data.get("amendedFields", {})
        detected_type = "AMENDMENT"  # default

        if amended.get("expiryDate"):
            new_date = amended["expiryDate"]
            old_date = str(lg.expiry_date) if lg.expiry_date else "N/A"
            if new_date != old_date:
                changes.append({"field": "expiry_date", "old": old_date, "new": new_date})
                detected_type = "EXTEND"

        if amended.get("lgAmount"):
            try:
                new_amount = float(amended["lgAmount"])
                old_amount = float(lg.current_amount or 0)
                if abs(new_amount - old_amount) > 0.01:
                    changes.append({"field": "current_amount", "old": str(old_amount), "new": str(new_amount)})
                    if new_amount > old_amount:
                        detected_type = "INCREASE_AMOUNT"
                    else:
                        detected_type = "DECREASE_AMOUNT"
            except (ValueError, TypeError):
                pass

        if amended.get("beneficiaryName"):
            if amended["beneficiaryName"] != (lg.beneficiary_name or ""):
                changes.append({"field": "beneficiary_name", "old": lg.beneficiary_name or "N/A", "new": amended["beneficiaryName"]})

        if amended.get("lgPurpose"):
            old_purpose = getattr(lg, 'lg_purpose', None) or "N/A"
            if amended["lgPurpose"] != old_purpose:
                changes.append({"field": "lg_purpose", "old": old_purpose, "new": amended["lgPurpose"]})

        # Check for liquidation indicators
        if extracted_data.get("is_liquidation") or "liquidat" in raw_text.lower():
            detected_type = "LIQUIDATION"
            changes.append({"field": "status", "old": lg.status, "new": "LIQUIDATED"})

        # Create a pending action for user review
        action_data = {
            "detected_type": detected_type,
            "extracted_data": extracted_data,
            "changes": changes,
            "raw_text_preview": raw_text[:500],
            "bank_document_gcs": doc_gcs_uri,
        }

        action = IssuanceMaintenanceAction(
            issued_lg_id=lg_id,
            action_type=detected_type,
            action_data=action_data,
            initiated_by_user_id=user_id,
            initiation_source="BANK_INITIATED",
            status="PENDING_BANK_CHANGE_REVIEW",
            bank_reply_document_path=doc_gcs_uri,
        )
        db.add(action)
        db.commit()
        db.refresh(action)

        log_action(
            db, user_id=user_id,
            action_type=f"ISSUANCE_MAINTENANCE_BANK_INITIATED_UPLOADED",
            entity_type="IssuanceMaintenanceAction",
            entity_id=action.id,
            details={"detected_type": detected_type, "changes_count": len(changes)},
            customer_id=customer_id,
        )

        return {
            "action_id": action.id,
            "detected_type": detected_type,
            "changes": changes,
            "extracted_data": extracted_data,
            "message": f"AI detected a {detected_type.replace('_', ' ').lower()} from the bank letter. Please review the changes and confirm.",
        }

    def confirm_bank_initiated_change(
        self,
        db: Session,
        action_id: int,
        user_id: int,
        customer_id: int,
    ) -> IssuanceMaintenanceAction:
        """
        Apply the AI-detected bank-initiated changes to the LG record.
        Called after user reviews the diff and confirms.
        """
        action = db.query(IssuanceMaintenanceAction).filter(
            IssuanceMaintenanceAction.id == action_id,
        ).first()
        if not action:
            raise HTTPException(status_code=404, detail="Action not found")
        if action.status != "PENDING_BANK_CHANGE_REVIEW":
            raise HTTPException(status_code=400, detail="Action is not pending review")

        lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
        if not lg:
            raise HTTPException(status_code=404, detail="LG record not found")

        before = self._snapshot_lg(lg)

        # Apply changes from the extracted diff
        data = action.action_data or {}
        changes = data.get("changes", [])

        for change in changes:
            field = change.get("field")
            new_val = change.get("new")

            if field == "expiry_date" and new_val:
                try:
                    lg.expiry_date = date.fromisoformat(new_val) if isinstance(new_val, str) else new_val
                except Exception:
                    pass

            elif field == "current_amount" and new_val:
                try:
                    lg.current_amount = Decimal(str(new_val))
                except Exception:
                    pass

            elif field == "beneficiary_name" and new_val:
                lg.beneficiary_name = new_val

            elif field == "lg_purpose" and new_val:
                if hasattr(lg, 'lg_purpose'):
                    lg.lg_purpose = new_val

            elif field == "status" and new_val == "LIQUIDATED":
                lg.status = "LIQUIDATED"

        after = self._snapshot_lg(lg)
        self._record_history(lg, f"BANK_INITIATED_{action.action_type}", before, after, user_id,
                             "Bank-initiated change applied after user review")

        # Update action status
        action.status = "COMPLETED"
        action.instruction_status = "Confirmed by Bank"

        db.add(lg)
        db.add(action)
        db.commit()
        db.refresh(action)

        log_action(
            db, user_id=user_id,
            action_type=f"ISSUANCE_MAINTENANCE_BANK_INITIATED_CONFIRMED",
            entity_type="IssuanceMaintenanceAction",
            entity_id=action.id,
            details={"changes_applied": len(changes)},
            customer_id=customer_id,
        )

        return action

    # ──────────────────────────────────────────────────
    # F3: AI Verification on Bank Reply
    # ──────────────────────────────────────────────────
    def _verify_bank_reply_with_ai(
        self, db: Session, action: IssuanceMaintenanceAction,
        file_bytes: bytes, mime_type: str, user_id: int
    ) -> dict:
        """
        F3: Optionally verifies a bank reply document using AI extraction.
        Reuses the existing extract_structured_data_with_gemini() with
        maintenance-specific context.
        Returns a verification result dict.
        """
        import asyncio
        try:
            from app.core.ai_integration import extract_structured_data_with_gemini
            from app.core.ai_integration import perform_ocr_with_google_vision
            from app.core.ai_integration import _convert_pdf_to_images_and_upload_to_gcs
            from app.core.ai_integration import _cleanup_gcs_files, GCS_BUCKET_NAME
            import uuid

            # Helper: run async function from sync context (FastAPI worker thread has no event loop)
            def _run_async(coro):
                loop = asyncio.new_event_loop()
                try:
                    return loop.run_until_complete(coro)
                finally:
                    loop.close()

            lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
            if not lg:
                return {"status": "error", "message": "LG record not found"}

            target_bucket = GCS_BUCKET_NAME
            session_id = uuid.uuid4().hex
            unique_file_id = f"maint_reply_{lg.lg_ref_number}_{session_id}"

            # OCR the bank reply document
            raw_text = ""
            if mime_type.startswith("image/"):
                blob_name = f"lg_scans_temp/{unique_file_id}/image_{uuid.uuid4().hex}.{mime_type.split('/')[-1]}"
                from app.core.ai_integration import _upload_to_gcs
                gcs_uri = _run_async(
                    _upload_to_gcs(target_bucket, blob_name, file_bytes, mime_type)
                )
                if gcs_uri:
                    raw_text = _run_async(
                        perform_ocr_with_google_vision(gcs_uri, unique_file_id)
                    ) or ""
            elif mime_type == "application/pdf":
                image_uris = _run_async(
                    _convert_pdf_to_images_and_upload_to_gcs(file_bytes, target_bucket, unique_file_id)
                )
                texts = []
                for uri in (image_uris or []):
                    page_text = _run_async(
                        perform_ocr_with_google_vision(uri, unique_file_id)
                    )
                    if page_text:
                        texts.append(page_text)
                raw_text = "\n".join(texts)

            if not raw_text:
                return {"status": "ocr_failed", "message": "Could not extract text from bank reply document"}

            # Build context for AI extraction — tell it what we expect
            data = action.action_data or {}
            # Prioritize bank_lg_number (the real bank reference) over lg_ref_number
            # (system temp reference like LG-TEMP-...). Bank reply documents always
            # reference the bank's own LG number.
            primary_lg_number = lg.bank_lg_number or lg.lg_ref_number or ""
            alt_lg_number = lg.lg_ref_number if lg.bank_lg_number else ""
            context = {
                "lg_record_details": {
                    "lgNumber": primary_lg_number,
                    "alternativeLgNumber": alt_lg_number,
                    "expected_action": action.action_type,
                    "expected_changes": data,
                }
            }

            extracted_data, usage = _run_async(
                extract_structured_data_with_gemini(raw_text, unique_file_id, context=context)
            )

            # Cleanup temp files
            try:
                _run_async(
                    _cleanup_gcs_files(target_bucket, f"lg_scans_temp/{unique_file_id}/")
                )
            except Exception:
                pass

            if not extracted_data:
                return {"status": "ai_failed", "message": "AI could not extract structured data from document"}

            # Compare extracted vs expected
            verification = {
                "status": "verified",
                "extracted_data": extracted_data,
                "matches": [],
                "mismatches": [],
            }

            # Check if the AI confirmed this is a relevant document
            if extracted_data.get("is_relevant_amendment") is False:
                verification["status"] = "mismatch"
                verification["mismatches"].append("AI indicates document is not relevant to this LG")

            # Compare key fields based on action type
            if action.action_type == "EXTEND":
                expected_date = data.get("new_expiry_date")
                ai_date = extracted_data.get("amendedFields", {}).get("expiryDate")
                if expected_date and ai_date:
                    # Normalize both to YYYY-MM-DD (AI may return "2027-01-02_00:00:00Z" or similar)
                    def _normalize_date(d):
                        """Extract just the YYYY-MM-DD portion from any date/datetime string."""
                        s = str(d).replace("_", "T").replace("Z", "").strip()
                        return s[:10]  # first 10 chars = YYYY-MM-DD
                    norm_expected = _normalize_date(expected_date)
                    norm_ai = _normalize_date(ai_date)
                    if norm_expected == norm_ai:
                        verification["matches"].append(f"Expiry date matches: {norm_expected}")
                    else:
                        verification["mismatches"].append(
                            f"Expiry date mismatch: expected={norm_expected}, extracted={norm_ai}"
                        )
                elif ai_date:
                    verification["mismatches"].append(
                        f"Expiry date mismatch: expected={expected_date}, extracted={ai_date}"
                    )

            elif action.action_type == "INCREASE_AMOUNT":
                expected_amount = data.get("new_amount")
                ai_amount = extracted_data.get("amendedFields", {}).get("lgAmount")
                if expected_amount and ai_amount:
                    if abs(float(expected_amount) - float(ai_amount)) < 0.01:
                        verification["matches"].append(f"Amount matches: {expected_amount}")
                    else:
                        verification["mismatches"].append(
                            f"Amount mismatch: expected={expected_amount}, extracted={ai_amount}"
                        )

            if verification["mismatches"]:
                verification["status"] = "mismatch"

            logger.info(f"F3 AI verification result for action {action.id}: {verification['status']}")
            return verification

        except Exception as e:
            logger.error(f"F3 AI verification failed for action {action.id}: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    # ──────────────────────────────────────────────────
    # F4: Create Exposure Entry on INCREASE_AMOUNT
    # ──────────────────────────────────────────────────
    def _create_increase_exposure_entry(
        self, db: Session, lg: IssuedLGRecord,
        old_amount: Decimal, new_amount: Decimal
    ):
        """
        F4: Creates an AMEND_INCREASE exposure entry for the increase delta.
        Mirrors the pattern from reserve_facility() in issuance_service.py.
        The delta is converted to facility currency using FX.
        """
        delta = new_amount - old_amount
        if delta <= 0:
            return

        # Find the linked request and its sub-limit
        from app.models.models_issuance import IssuanceRequest
        request = None
        if lg.request_id:
            request = db.query(IssuanceRequest).filter(IssuanceRequest.id == lg.request_id).first()

        if not request or not request.selected_sub_limit_id:
            logger.warning(
                f"F4: Cannot create exposure entry for LG {lg.id} — "
                f"no linked request or sub-limit. Increase delta={delta} not tracked."
            )
            return

        sub_limit = db.query(IssuanceFacilitySubLimit).filter(
            IssuanceFacilitySubLimit.id == request.selected_sub_limit_id
        ).first()
        if not sub_limit:
            logger.warning(f"F4: Sub-limit {request.selected_sub_limit_id} not found for LG {lg.id}")
            return

        facility = db.query(IssuanceFacility).filter(
            IssuanceFacility.id == sub_limit.facility_id
        ).first()
        if not facility:
            logger.warning(f"F4: Facility not found for sub-limit {sub_limit.id}")
            return

        # Convert delta to facility currency
        fx_rate = Decimal("1.0")
        facility_equivalent = delta

        if lg.currency_id != facility.currency_id:
            try:
                from app.services.fx_service import fx_service
                converted_amount, rate = fx_service.convert(
                    db, delta, lg.currency_id, facility.currency_id, allow_ai=False
                )
                if converted_amount is not None:
                    facility_equivalent = converted_amount
                    fx_rate = Decimal(str(rate))
                else:
                    logger.warning(f"F4: FX conversion failed for LG {lg.id}, using 1:1 rate")
            except Exception as e:
                logger.error(f"F4: FX conversion error: {e}")

        exposure_entry = IssuanceExposureEntry(
            facility_id=sub_limit.facility_id,
            sub_limit_id=sub_limit.id,
            lg_record_id=lg.id,
            request_id=request.id,
            entry_type="AMEND_INCREASE",
            original_amount_delta=delta,
            original_currency_id=lg.currency_id,
            fx_rate_used=fx_rate,
            facility_equivalent_delta=facility_equivalent,
            is_active=True,
            effective_date=date.today(),
        )
        db.add(exposure_entry)
        logger.info(
            f"F4: Created AMEND_INCREASE exposure entry for LG {lg.id}: "
            f"delta={delta} → facility_equivalent={facility_equivalent}"
        )

    # ──────────────────────────────────────────────────
    # F5: Facility Capacity Check on INCREASE_AMOUNT
    # ──────────────────────────────────────────────────
    def _check_increase_capacity(
        self, db: Session, lg: IssuedLGRecord, new_amount: Decimal
    ):
        """
        F5: Validates that the facility has enough headroom for the increase.
        Mirrors the capacity check in reserve_facility() from issuance_service.py.
        Checks both sub-limit and facility-level availability.
        """
        delta = new_amount - lg.current_amount
        if delta <= 0:
            return  # Decrease or same — no capacity issue

        from app.models.models_issuance import IssuanceRequest
        from sqlalchemy import func

        request = None
        if lg.request_id:
            request = db.query(IssuanceRequest).filter(IssuanceRequest.id == lg.request_id).first()

        if not request or not request.selected_sub_limit_id:
            # No facility tracking — skip capacity check (e.g., "other bank" LGs)
            logger.info(f"F5: No sub-limit linked to LG {lg.id} — skipping capacity check")
            return

        sub_limit = db.query(IssuanceFacilitySubLimit).filter(
            IssuanceFacilitySubLimit.id == request.selected_sub_limit_id
        ).first()
        if not sub_limit:
            return

        facility = db.query(IssuanceFacility).filter(
            IssuanceFacility.id == sub_limit.facility_id
        ).first()
        if not facility:
            return

        # Convert delta to facility currency
        facility_equivalent_delta = delta
        if lg.currency_id != facility.currency_id:
            try:
                from app.services.fx_service import fx_service
                converted, _ = fx_service.convert(
                    db, delta, lg.currency_id, facility.currency_id, allow_ai=False
                )
                if converted is not None:
                    facility_equivalent_delta = converted
                else:
                    raise HTTPException(
                        status_code=400,
                        detail="Cannot determine FX rate for capacity check. "
                               "Please ensure exchange rates are up to date."
                    )
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"F5: FX conversion error: {e}")
                raise HTTPException(
                    status_code=400,
                    detail="FX conversion failed during capacity check."
                )

        # Check sub-limit availability
        used_amount = db.query(
            func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0)
        ).filter(
            IssuanceExposureEntry.sub_limit_id == sub_limit.id,
            IssuanceExposureEntry.is_active == True
        ).scalar()
        used_amount = float(used_amount) + float(getattr(sub_limit, 'initial_utilization', 0) or 0)

        available = float(sub_limit.limit_amount) - used_amount
        if float(facility_equivalent_delta) > available:
            curr_code = facility.currency.iso_code if facility.currency else "N/A"
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient sub-limit capacity for increase. "
                       f"Available: {available:,.2f} {curr_code}, "
                       f"Required increase: {float(facility_equivalent_delta):,.2f} {curr_code}"
            )

        # Check facility-level total cap
        facility_total_used = db.query(
            func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0)
        ).filter(
            IssuanceExposureEntry.facility_id == facility.id,
            IssuanceExposureEntry.is_active == True
        ).scalar()
        facility_total_used = float(facility_total_used)
        for sl in facility.sub_limits:
            facility_total_used += float(getattr(sl, 'initial_utilization', 0) or 0)
        facility_available = float(facility.total_limit_amount) - facility_total_used

        if float(facility_equivalent_delta) > facility_available:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient facility total limit for increase. "
                       f"Facility available: {facility_available:,.2f}, "
                       f"Required increase: {float(facility_equivalent_delta):,.2f}"
            )

        logger.info(
            f"F5: Capacity check passed for LG {lg.id}: "
            f"delta={delta}, sub-limit available={available}, "
            f"facility available={facility_available}"
        )

    # ──────────────────────────────────────────────────
    # F1a: Regenerate Maintenance Letter HTML (on-the-fly)
    # ──────────────────────────────────────────────────
    def regenerate_maintenance_letter_html(
        self, db: Session, action: IssuanceMaintenanceAction, lg: IssuedLGRecord
    ) -> str:
        """
        Regenerates the instruction letter HTML from template + action_data.
        Mirrors the LG custody pattern: no file storage, regenerate on demand.
        Returns rendered HTML string, or None if template not found.
        """
        from app.crud.crud import crud_template
        from sqlalchemy.orm import selectinload

        TEMPLATE_MAP = {
            "EXTEND": "LG_EXTEND_REQUEST",
            "INCREASE_AMOUNT": "LG_INCREASE_REQUEST",
            "CLOSE": "LG_CLOSE_REQUEST",
            "AMENDMENT": "LG_AMENDMENT_REQUEST",
            "ACTIVATE": "LG_ACTIVATE_REQUEST",
        }

        specific_action_type = TEMPLATE_MAP.get(action.action_type, f"LG_{action.action_type}_REQUEST")

        template = None
        for action_type_key in [specific_action_type, "LG_MAINTENANCE_REQUEST"]:
            template = crud_template.get_single_template(
                db, action_type=action_type_key, is_global=False,
                customer_id=lg.customer_id, is_notification_template=False,
            )
            if not template:
                template = crud_template.get_single_template(
                    db, action_type=action_type_key, is_global=True,
                    is_notification_template=False,
                )
            if template:
                break

        if not template:
            logger.warning(f"No template found for maintenance letter regeneration: {action.action_type}")
            return None

        lg_with_rels = db.query(IssuedLGRecord).options(
            selectinload(IssuedLGRecord.currency),
            selectinload(IssuedLGRecord.bank),
            selectinload(IssuedLGRecord.customer),
            selectinload(IssuedLGRecord.issuing_entity),
            selectinload(IssuedLGRecord.lg_type),
        ).filter(IssuedLGRecord.id == lg.id).first()

        data = action.action_data or {}

        def amount_to_words(amount) -> str:
            try:
                from num2words import num2words
                amount = float(amount)
                integer_part = int(amount)
                decimal_part = round((amount - integer_part) * 100)
                words = num2words(integer_part).title()
                if decimal_part > 0:
                    words += f" and {num2words(decimal_part).title()} Cents"
                return words
            except (ImportError, Exception):
                return f"{float(amount):,.2f}"

        current_amount = float(lg_with_rels.current_amount) if lg_with_rels.current_amount else 0
        currency_code = lg_with_rels.currency.iso_code if lg_with_rels.currency else "N/A"
        currency_name = lg_with_rels.currency.name if lg_with_rels.currency else "N/A"

        placeholder_data = {
            "lg_ref_number": lg_with_rels.lg_ref_number or "",
            "bank_lg_number": lg_with_rels.bank_lg_number or "N/A",
            "internal_serial": lg_with_rels.internal_serial or "",
            "letter_serial_number": action.letter_serial_number or "",
            "beneficiary_name": lg_with_rels.beneficiary_name or "",
            "beneficiary_address": lg_with_rels.beneficiary_address or "",
            "current_amount": f"{current_amount:,.2f}",
            "current_amount_in_words": amount_to_words(current_amount),
            "currency_code": currency_code,
            "currency_name": currency_name,
            "currency_symbol": lg_with_rels.currency.symbol if lg_with_rels.currency and hasattr(lg_with_rels.currency, 'symbol') else currency_code,
            "current_amount_formatted": f"{lg_with_rels.currency.symbol if lg_with_rels.currency and hasattr(lg_with_rels.currency, 'symbol') else currency_code} {current_amount:,.2f}",
            "lg_amount_formatted": f"{lg_with_rels.currency.symbol if lg_with_rels.currency and hasattr(lg_with_rels.currency, 'symbol') else currency_code} {current_amount:,.2f}",
            "lg_type": lg_with_rels.lg_type.name if lg_with_rels.lg_type else "N/A",
            "issue_date": str(lg_with_rels.issue_date) if lg_with_rels.issue_date else "N/A",
            "expiry_date": str(lg_with_rels.expiry_date) if lg_with_rels.expiry_date else "N/A",
            "bank_name": lg_with_rels.bank.name if lg_with_rels.bank else "N/A",
            "bank_address": lg_with_rels.bank.address if lg_with_rels.bank and hasattr(lg_with_rels.bank, 'address') else "N/A",
            "recipient_name": lg_with_rels.bank.name if lg_with_rels.bank else "To Whom It May Concern",
            "recipient_address": lg_with_rels.bank.address if lg_with_rels.bank and hasattr(lg_with_rels.bank, 'address') else "N/A",
            "customer_name": lg_with_rels.customer.name if lg_with_rels.customer else "N/A",
            "company_name": lg_with_rels.customer.name if lg_with_rels.customer else "N/A",
            "customer_address": lg_with_rels.customer.address if lg_with_rels.customer and hasattr(lg_with_rels.customer, 'address') else "N/A",
            "customer_contact_email": lg_with_rels.customer.contact_email if lg_with_rels.customer and hasattr(lg_with_rels.customer, 'contact_email') else "N/A",
            "entity_name": lg_with_rels.issuing_entity.entity_name if lg_with_rels.issuing_entity else "",
            "entity_address": lg_with_rels.issuing_entity.address if lg_with_rels.issuing_entity and hasattr(lg_with_rels.issuing_entity, 'address') else "",
            "action_type": action.action_type.replace("_", " ").title(),
            "action_notes": action.notes or "",
            "current_date": date.today().strftime("%d-%b-%Y"),
            "serial_number": action.letter_serial_number or "",
            "platform_name": "Treasury Management Platform",
        }

        # Notes section as HTML block (matches custody pattern)
        notes_html = ""
        if action.notes:
            notes_html = f"<h3>Additional Notes</h3><p>{action.notes}</p>"
        placeholder_data["notes_section"] = notes_html

        # Action-specific extra placeholders
        # NOTE: Use snapshot_* values from action_data (captured at execution time)
        # to ensure the letter shows the correct "before" values, not the current LG state.
        if action.action_type == "EXTEND":
            placeholder_data["new_expiry_date"] = data.get("new_expiry_date", "N/A")
            # Use snapshot (before extension was applied), fallback to current LG data for old actions
            old_expiry = data.get("snapshot_expiry_date") or (str(lg_with_rels.expiry_date) if lg_with_rels.expiry_date else "N/A")
            placeholder_data["old_expiry_date"] = old_expiry
        elif action.action_type == "INCREASE_AMOUNT":
            new_amount = data.get("new_amount", 0)
            new_amount_val = float(new_amount) if new_amount else 0
            # Use snapshot amount (before increase), fallback to current for old actions
            old_amount_str = data.get("snapshot_amount")
            old_amount_val = float(old_amount_str) if old_amount_str else current_amount
            delta = new_amount_val - old_amount_val
            placeholder_data["new_amount"] = f"{new_amount_val:,.2f}"
            placeholder_data["new_amount_in_words"] = amount_to_words(new_amount_val)
            placeholder_data["increase_delta"] = f"{delta:,.2f}"
            placeholder_data["old_amount"] = f"{old_amount_val:,.2f}"
        elif action.action_type == "AMENDMENT":
            if data.get("new_beneficiary_name"):
                placeholder_data["new_beneficiary_name"] = data["new_beneficiary_name"]
            placeholder_data["amendment_details"] = data.get("amendment_text", action.notes or "")
        elif action.action_type == "CLOSE":
            placeholder_data["close_reason"] = data.get("close_reason", "")
        elif action.action_type == "ACTIVATE":
            placeholder_data["payment_method"] = data.get("payment_method", "N/A")
            amount_val = float(data.get("payment_amount", 0))
            placeholder_data["payment_amount"] = f"{amount_val:,.2f}"
            placeholder_data["payment_amount_formatted"] = f"{amount_val:,.2f}"
            placeholder_data["payment_reference"] = data.get("payment_reference", "N/A")
            placeholder_data["payment_date"] = data.get("payment_date", "N/A")
            payment_bank_id = data.get("payment_bank_id")
            if payment_bank_id:
                from app.models.models import Bank
                payment_bank = db.query(Bank).filter(Bank.id == payment_bank_id).first()
                placeholder_data["payment_issuing_bank_name"] = payment_bank.name if payment_bank else "N/A"
            else:
                placeholder_data["payment_issuing_bank_name"] = placeholder_data.get("bank_name", "N/A")

        # Fill template
        generated_html = template.content
        for key, value in placeholder_data.items():
            str_value = str(value) if value is not None else ""
            generated_html = generated_html.replace(f"{{{{{key}}}}}", str_value)

        return generated_html

    # ──────────────────────────────────────────────────
    # F1b: Maintenance Letter PDF Generation (used during _execute_action)
    # ──────────────────────────────────────────────────
    def _generate_maintenance_letter(
        self, db: Session, action: IssuanceMaintenanceAction, lg: IssuedLGRecord
    ):
        """
        Generates a maintenance letter PDF using the template system.
        Mirrors generate_issuance_letter() from issuance_service.py.
        
        Template lookup order:
          1. Customer-specific template for the action type (e.g., LG_EXTEND_REQUEST)
          2. Global template for the action type
          3. Customer-specific generic LG_MAINTENANCE_REQUEST template
          4. Global generic LG_MAINTENANCE_REQUEST template
        
        Returns: (pdf_bytes, filename) or (None, None) if template not found.
        """
        from app.crud.crud import crud_template
        from app.core.document_generator import generate_pdf_from_html
        from sqlalchemy.orm import selectinload

        # Map maintenance action types to template action_types
        TEMPLATE_MAP = {
            "EXTEND": "LG_EXTEND_REQUEST",
            "INCREASE_AMOUNT": "LG_INCREASE_REQUEST",
            "CLOSE": "LG_CLOSE_REQUEST",
            "AMENDMENT": "LG_AMENDMENT_REQUEST",
            "ACTIVATE": "LG_ACTIVATE_REQUEST",
        }

        specific_action_type = TEMPLATE_MAP.get(action.action_type, f"LG_{action.action_type}_REQUEST")

        # Template resolution: action-specific customer → action-specific global → generic customer → generic global
        template = None
        for action_type_key in [specific_action_type, "LG_MAINTENANCE_REQUEST"]:
            template = crud_template.get_single_template(
                db,
                action_type=action_type_key,
                is_global=False,
                customer_id=lg.customer_id,
                is_notification_template=False,
            )
            if not template:
                template = crud_template.get_single_template(
                    db,
                    action_type=action_type_key,
                    is_global=True,
                    is_notification_template=False,
                )
            if template:
                break

        if not template:
            logger.warning(
                f"No template found for maintenance action {action.action_type} "
                f"(tried {specific_action_type} and LG_MAINTENANCE_REQUEST)"
            )
            return None, None

        # Load relationships for placeholder data
        lg_with_rels = db.query(IssuedLGRecord).options(
            selectinload(IssuedLGRecord.currency),
            selectinload(IssuedLGRecord.bank),
            selectinload(IssuedLGRecord.customer),
            selectinload(IssuedLGRecord.issuing_entity),
            selectinload(IssuedLGRecord.lg_type),
        ).filter(IssuedLGRecord.id == lg.id).first()

        data = action.action_data or {}

        # Amount in words helper
        def amount_to_words(amount) -> str:
            try:
                from num2words import num2words
                amount = float(amount)
                integer_part = int(amount)
                decimal_part = round((amount - integer_part) * 100)
                words = num2words(integer_part).title()
                if decimal_part > 0:
                    words += f" and {num2words(decimal_part).title()} Cents"
                return words
            except (ImportError, Exception):
                return f"{float(amount):,.2f}"

        current_amount = float(lg_with_rels.current_amount) if lg_with_rels.current_amount else 0
        currency_code = lg_with_rels.currency.iso_code if lg_with_rels.currency else "N/A"
        currency_name = lg_with_rels.currency.name if lg_with_rels.currency else "N/A"

        # Build placeholder data — works for any action type
        placeholder_data = {
            # LG Core
            "lg_ref_number": lg_with_rels.lg_ref_number or "",
            "bank_lg_number": lg_with_rels.bank_lg_number or "N/A",
            "internal_serial": lg_with_rels.internal_serial or "",
            "letter_serial_number": action.letter_serial_number or "",
            "beneficiary_name": lg_with_rels.beneficiary_name or "",
            "beneficiary_address": lg_with_rels.beneficiary_address or "",
            "current_amount": f"{current_amount:,.2f}",
            "current_amount_in_words": amount_to_words(current_amount),
            "currency_code": currency_code,
            "currency_name": currency_name,
            "lg_type": lg_with_rels.lg_type.name if lg_with_rels.lg_type else "N/A",
            "issue_date": str(lg_with_rels.issue_date) if lg_with_rels.issue_date else "N/A",
            "expiry_date": str(lg_with_rels.expiry_date) if lg_with_rels.expiry_date else "N/A",

            # Bank
            "bank_name": lg_with_rels.bank.name if lg_with_rels.bank else "N/A",

            # Customer / Company
            "customer_name": lg_with_rels.customer.name if lg_with_rels.customer else "N/A",
            "company_name": lg_with_rels.customer.name if lg_with_rels.customer else "N/A",
            "entity_name": lg_with_rels.issuing_entity.entity_name if lg_with_rels.issuing_entity else "",

            # Action-specific
            "action_type": action.action_type.replace("_", " ").title(),
            "action_notes": action.notes or "",

            # Date
            "current_date": date.today().strftime("%d-%b-%Y"),
            "serial_number": action.letter_serial_number or "",
            "platform_name": "Treasury Management Platform",
        }

        # Action-specific extra placeholders
        if action.action_type == "EXTEND":
            new_date = data.get("new_expiry_date", "N/A")
            placeholder_data["new_expiry_date"] = new_date
            placeholder_data["old_expiry_date"] = str(lg_with_rels.expiry_date) if lg_with_rels.expiry_date else "N/A"

        elif action.action_type == "INCREASE_AMOUNT":
            new_amount = data.get("new_amount", 0)
            new_amount_val = float(new_amount) if new_amount else 0
            delta = new_amount_val - current_amount
            placeholder_data["new_amount"] = f"{new_amount_val:,.2f}"
            placeholder_data["new_amount_in_words"] = amount_to_words(new_amount_val)
            placeholder_data["increase_delta"] = f"{delta:,.2f}"
            placeholder_data["old_amount"] = f"{current_amount:,.2f}"

        elif action.action_type == "AMENDMENT":
            if data.get("new_beneficiary_name"):
                placeholder_data["new_beneficiary_name"] = data["new_beneficiary_name"]
            placeholder_data["amendment_details"] = data.get("amendment_text", action.notes or "")

        elif action.action_type == "CLOSE":
            placeholder_data["close_reason"] = data.get("close_reason", "")

        elif action.action_type == "ACTIVATE":
            # Payment details for activation letter
            placeholder_data["payment_method"] = data.get("payment_method", "N/A")
            amount_val = float(data.get("payment_amount", 0))
            placeholder_data["payment_amount"] = f"{amount_val:,.2f}"
            placeholder_data["payment_amount_formatted"] = f"{amount_val:,.2f}"
            placeholder_data["payment_reference"] = data.get("payment_reference", "N/A")
            placeholder_data["payment_date"] = data.get("payment_date", "N/A")
            # Resolve payment bank name if bank_id provided
            payment_bank_id = data.get("payment_bank_id")
            if payment_bank_id:
                from app.models.models import Bank
                payment_bank = db.query(Bank).filter(Bank.id == payment_bank_id).first()
                placeholder_data["payment_issuing_bank_name"] = payment_bank.name if payment_bank else "N/A"
            else:
                placeholder_data["payment_issuing_bank_name"] = placeholder_data.get("bank_name", "N/A")

        # Fill template
        generated_html = template.content
        for key, value in placeholder_data.items():
            str_value = str(value) if value is not None else ""
            generated_html = generated_html.replace(f"{{{{{key}}}}}", str_value)

        # Generate PDF (synchronous call via asyncio for background context)
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're inside an async context — create a task
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    pdf_bytes = pool.submit(
                        asyncio.run,
                        generate_pdf_from_html(generated_html, filename_hint=f"maint_{action.letter_serial_number}")
                    ).result()
            else:
                pdf_bytes = loop.run_until_complete(
                    generate_pdf_from_html(generated_html, filename_hint=f"maint_{action.letter_serial_number}")
                )
        except RuntimeError:
            pdf_bytes = asyncio.run(
                generate_pdf_from_html(generated_html, filename_hint=f"maint_{action.letter_serial_number}")
            )

        if not pdf_bytes:
            logger.warning(f"PDF generation returned empty bytes for action {action.id}")
            return None, None

        filename = f"Maintenance_{action.action_type}_{action.letter_serial_number or action.id}.pdf"
        logger.info(f"Generated maintenance letter: {filename} ({len(pdf_bytes)} bytes)")
        return pdf_bytes, filename


# Singleton instance
maintenance_service = IssuanceMaintenanceService()

