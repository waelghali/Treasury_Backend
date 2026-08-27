# app/services/inbox_action_service.py
"""
Action Execution Service for Smart Inbox
Executes confirmed inbox items by bridging them into the appropriate target module:
- LG_POSITION_REPORT -> ReconciliationSession creation + file parsing + matching
- BANK_STATEMENT -> BankStatement ingestion (Phase 2)
- PROGRESS_REPORT -> Milestone action review (Phase 3)
"""

import os
import logging
from datetime import datetime, date, timezone
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.models.models import Bank
from app.models.models_inbox import InboxItem, EmailClassificationHistory
from app.services.reconciliation_service import reconciliation_service
from app.crud.crud import log_action

logger = logging.getLogger("app.inbox_action")


class InboxActionService:
    """
    Executes confirmed inbox items and logs results.
    """

    async def confirm_and_execute(
        self,
        db: Session,
        inbox_item_id: int,
        user_id: int,
        customer_id: int,
        override_bank_id: Optional[int] = None,
        override_position_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Confirms an InboxItem and triggers its respective downstream action.
        """
        item = db.query(InboxItem).filter(
            InboxItem.id == inbox_item_id,
            InboxItem.customer_id == customer_id,
            InboxItem.is_deleted == False
        ).first()

        if not item:
            raise HTTPException(status_code=404, detail="Inbox item not found")

        if item.status in ("ACTIONED", "CONFIRMED"):
            raise HTTPException(
                status_code=400,
                detail=f"Item has already been actioned (Ref: {item.action_reference_type} #{item.action_reference_id})"
            )

        # ----------------------------------------------------------------------
        # ACTION TYPE 1: LG POSITION REPORT
        # ----------------------------------------------------------------------
        if item.classification == "LG_POSITION_REPORT":
            return await self._execute_lg_position(
                db, item, user_id, customer_id, override_bank_id, override_position_date
            )

        # ----------------------------------------------------------------------
        # ACTION TYPE 2: BANK STATEMENT (Phase 2 Placeholder / Bridge)
        # ----------------------------------------------------------------------
        elif item.classification == "BANK_STATEMENT":
            # For Phase 1, mark as confirmed and ready for workspace
            item.status = "CONFIRMED"
            item.actioned_at = datetime.now(timezone.utc)
            item.actioned_by_user_id = user_id
            item.action_reference_type = "BankStatement"
            db.commit()
            return {
                "status": "CONFIRMED",
                "message": "Bank statement confirmed. Ready for reconciliation workspace.",
                "item_id": item.id
            }

        # ----------------------------------------------------------------------
        # ACTION TYPE 3: PROGRESS REPORT (Phase 3 Placeholder / Bridge)
        # ----------------------------------------------------------------------
        elif item.classification == "PROGRESS_REPORT":
            item.status = "CONFIRMED"
            item.actioned_at = datetime.now(timezone.utc)
            item.actioned_by_user_id = user_id
            item.action_reference_type = "ProgressReport"
            db.commit()
            return {
                "status": "CONFIRMED",
                "message": "Progress report confirmed and saved.",
                "item_id": item.id
            }

        elif item.classification == "IRRELEVANT":
            item.status = "ARCHIVED"
            item.actioned_at = datetime.now(timezone.utc)
            item.actioned_by_user_id = user_id
            item.action_reference_type = "Archived"
            db.commit()
            return {
                "status": "ARCHIVED",
                "message": "Item marked as irrelevant and archived.",
                "item_id": item.id
            }

        else:
            raise HTTPException(
                status_code=400,
                detail="Item must be classified as a valid actionable category before confirmation."
            )

    async def _execute_lg_position(
        self,
        db: Session,
        item: InboxItem,
        user_id: int,
        customer_id: int,
        override_bank_id: Optional[int] = None,
        override_position_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Executes LG Position Report:
        1. Validates Bank and Position Date.
        2. Creates ReconciliationSession.
        3. Parses file into bank rows.
        4. Runs matching engine against active IssuedLGRecords.
        """
        bank_id = override_bank_id or item.matched_bank_id
        if not bank_id:
            raise HTTPException(
                status_code=400,
                detail="Please specify the Bank associated with this LG Position Report."
            )

        # Verify bank exists
        bank = db.query(Bank).filter(Bank.id == bank_id, Bank.is_deleted == False).first()
        if not bank:
            raise HTTPException(status_code=400, detail=f"Bank ID {bank_id} not found.")

        # Determine Position Date
        pos_date = override_position_date or (item.received_at.date() if item.received_at else date.today())

        # Handle NIL / Zero Position Reconciliation (No attachment file required)
        if item.is_nil_position or not item.has_attachment:
            # 1. Create ReconciliationSession with 0 bank records
            session = reconciliation_service.create_session(
                db=db,
                customer_id=customer_id,
                bank_id=bank_id,
                position_date=pos_date,
                user_id=user_id,
                file_name="NIL_POSITION_CONFIRMATION",
                notes=f"NIL/Zero position confirmed via Smart Inbox email: '{item.subject}' (Sender: {item.sender_email})"
            )

            # 2. Run matching against active system LGs
            try:
                session = reconciliation_service.run_matching(
                    db=db,
                    session_id=session.id,
                    customer_id=customer_id,
                    user_id=user_id
                )
            except Exception as match_err:
                logger.warning(f"Reconciliation matching warning for nil-position InboxItem {item.id}: {match_err}")

            # 3. Update InboxItem status
            item.status = "ACTIONED"
            item.actioned_at = datetime.now(timezone.utc)
            item.actioned_by_user_id = user_id
            item.action_reference_type = "ReconciliationSession"
            item.action_reference_id = session.id
            item.matched_bank_id = bank_id
            db.commit()

            return {
                "status": "ACTIONED",
                "message": f"NIL Position Reconciliation Session #{session.id} created and matched against system records.",
                "session_id": session.id,
                "item_id": item.id
            }

        if not item.primary_attachment_path or not os.path.exists(item.primary_attachment_path):
            raise HTTPException(
                status_code=400,
                detail="No readable attachment file found for this position report."
            )

        filename = item.primary_attachment_name or os.path.basename(item.primary_attachment_path)

        try:
            with open(item.primary_attachment_path, "rb") as f:
                file_bytes = f.read()
        except Exception as e:
            logger.error(f"Failed to read attachment for InboxItem {item.id}: {e}")
            item.status = "PARSE_ERROR"
            item.error_message = f"File read error: {str(e)}"
            db.commit()
            raise HTTPException(status_code=500, detail="Could not read stored attachment file.")

        # 1. Create ReconciliationSession
        session = reconciliation_service.create_session(
            db=db,
            customer_id=customer_id,
            bank_id=bank_id,
            position_date=pos_date,
            user_id=user_id,
            file_name=filename,
            notes=f"Auto-generated via Smart Inbox from email: '{item.subject}' (Sender: {item.sender_email})"
        )

        # 2. Parse file into bank rows
        try:
            session = await reconciliation_service.parse_file(
                db=db,
                session_id=session.id,
                file_bytes=file_bytes,
                file_name=filename,
                customer_id=customer_id,
                user_id=user_id
            )
        except Exception as parse_err:
            logger.error(f"Reconciliation parse failed for InboxItem {item.id}: {parse_err}")
            item.status = "PARSE_ERROR"
            item.error_message = str(parse_err)
            db.commit()
            raise HTTPException(status_code=400, detail=f"File parsing error: {str(parse_err)}")

        # 3. Run matching
        try:
            session = reconciliation_service.run_matching(
                db=db,
                session_id=session.id,
                customer_id=customer_id,
                user_id=user_id
            )
        except Exception as match_err:
            logger.warning(f"Reconciliation matching warning for InboxItem {item.id}: {match_err}")
            # Session is still created & parsed

        # 4. Update InboxItem status
        item.status = "ACTIONED"
        item.actioned_at = datetime.now(timezone.utc)
        item.actioned_by_user_id = user_id
        item.action_reference_type = "ReconciliationSession"
        item.action_reference_id = session.id
        item.matched_bank_id = bank_id

        # 5. Record Confirmation in History
        history = EmailClassificationHistory(
            customer_id=customer_id,
            sender_email=item.sender_email,
            sender_domain=item.sender_domain,
            classification="LG_POSITION_REPORT",
            was_user_corrected=(item.user_override_classification is not None),
            original_classification=item.classification,
            confidence_score=item.confidence_score,
            confirmed_by_user_id=user_id
        )
        db.add(history)

        log_action(
            db=db,
            user_id=user_id,
            action_type="INBOX_ITEM_ACTIONED",
            entity_type="InboxItem",
            entity_id=item.id,
            details={
                "classification": "LG_POSITION_REPORT",
                "session_id": session.id,
                "bank_id": bank_id,
                "position_date": pos_date.isoformat(),
                "total_records": session.total_bank_records,
                "matched_count": session.matched_count,
                "mismatched_count": session.mismatched_count
            },
            customer_id=customer_id
        )

        db.commit()
        db.refresh(item)

        return {
            "status": "SUCCESS",
            "message": f"Reconciliation Session #{session.id} successfully created and matched with {session.total_bank_records} bank records.",
            "session_id": session.id,
            "bank_id": bank_id,
            "bank_name": bank.name,
            "position_date": pos_date.isoformat(),
            "total_records": session.total_bank_records,
            "matched_count": session.matched_count,
            "mismatched_count": session.mismatched_count,
            "item_id": item.id
        }


inbox_action_service = InboxActionService()
