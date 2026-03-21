# app/crud_quotation.py
from sqlalchemy.orm import Session
from datetime import datetime, timezone
import uuid
import json

from app.models.models_quotation import (
    QuotationBank, QuotationRequest, QuotationBankAssignment, 
    QuotationOffer, QuotationTBillOffer, QuotationAnalytics
)
from app.schemas.schemas_quotation import QuotationRequestCreate, QuotationBankCreate

class CRUDQuotation:
    
    # --- Quotation Banks ---
    def create_quotation_bank(self, db: Session, customer_id: int, obj_in: QuotationBankCreate):
        # Check if already exists for this customer and trade_type
        existing = db.query(QuotationBank).filter(
            QuotationBank.customer_id == customer_id,
            QuotationBank.bank_id == obj_in.bank_id,
            QuotationBank.trade_type == (obj_in.trade_type or "BOTH")
        ).first()
        
        if existing:
            # Update emails if changed
            existing.emails = obj_in.emails
            db.commit()
            db.refresh(existing)
            return existing

        db_obj = QuotationBank(
            customer_id=customer_id,
            bank_id=obj_in.bank_id,
            emails=obj_in.emails,
            trade_type=obj_in.trade_type or "BOTH"
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def delete_quotation_bank(self, db: Session, customer_id: int, bank_id: int):
        db_obj = db.query(QuotationBank).filter(
            QuotationBank.id == bank_id,
            QuotationBank.customer_id == customer_id
        ).first()
        if db_obj:
            db.delete(db_obj)
            db.commit()
            return True
        return False

    def get_quotation_banks(self, db: Session, customer_id: int, trade_type: str = None):
        query = db.query(QuotationBank).filter(QuotationBank.customer_id == customer_id)
        if trade_type:
            # If trade_type is specified, return banks matching the specific type OR "BOTH"
            query = query.filter(QuotationBank.trade_type.in_([trade_type, "BOTH"]))
        return query.all()

    # --- Requests ---
    def create_request(self, db: Session, customer_id: int, user_id: int, requires_approval: bool, obj_in: QuotationRequestCreate, document_path: str = None):
        rfq_id = str(uuid.uuid4())
        date_str = datetime.now().strftime("%Y%m%d")
        
        prefix = "TB" if obj_in.type == "TBILL" else "RFQ"
        ref_no = f"{prefix}-{date_str}-{uuid.uuid4().hex[:4].upper()}"

        initial_status = "PENDING_APPROVAL" if requires_approval else "PENDING"

        db_rfq = QuotationRequest(
            id=rfq_id,
            ref_no=ref_no,
            customer_id=customer_id,
            created_by_user_id=user_id,
            type=obj_in.type,
            direction=obj_in.direction,
            value_date=obj_in.valueDate,
            amount=obj_in.amount,
            min_ticket_amount=obj_in.minTicketAmount,
            buy_currency=obj_in.buyCurrency,
            sell_currency=obj_in.sellCurrency,
            settlement_date_start=obj_in.settlementDateStart,
            settlement_date_end=obj_in.settlementDateEnd,
            maturity_date_start=obj_in.maturityDateStart,
            maturity_date_end=obj_in.maturityDateEnd,
            eval_rate=obj_in.evalRate,
            window_start=obj_in.windowStart,
            window_end=obj_in.windowEnd,
            quotation_base=obj_in.quotationBase,
            document_path=document_path,
            status=initial_status
        )
        db.add(db_rfq)
        
        # Parse assigned banks
        assignments = []
        try:
            banks_data = json.loads(obj_in.selectedBanks)
            for b_data in banks_data:
                assignment_id = str(uuid.uuid4())
                token = str(uuid.uuid4())
                
                # Fetch the quotation bank id based on the standard bank id and the rfq trade type
                q_bank = db.query(QuotationBank).filter(
                    QuotationBank.customer_id == customer_id,
                    QuotationBank.bank_id == b_data.get('id'),
                    QuotationBank.trade_type.in_([obj_in.type, "BOTH"])
                ).first()
                
                if q_bank:
                    db_assignment = QuotationBankAssignment(
                        id=assignment_id,
                        rfq_id=rfq_id,
                        quotation_bank_id=q_bank.id,
                        token=token,
                        cost_min=b_data.get('costMin', 0.0),
                        cost_percent=b_data.get('costPercent', 0.0),
                        cost_max=b_data.get('costMax', 0.0),
                        cost_flat=b_data.get('costFlat', 0.0)
                    )
                    db.add(db_assignment)
                    assignments.append({"bankId": b_data.get('id'), "token": token})
        except Exception as e:
            # Re-raise or handle JSON parsing failure
            raise ValueError(f"Failed to parse selected banks: {e}")

        db.commit()
        db.refresh(db_rfq)
        return db_rfq, assignments

    def get_requests(self, db: Session, customer_id: int):
        return db.query(QuotationRequest).filter(
            QuotationRequest.customer_id == customer_id
        ).order_by(QuotationRequest.created_at.desc()).all()

    def get_request(self, db: Session, rfq_id: str, customer_id: int):
        return db.query(QuotationRequest).filter(
            QuotationRequest.id == rfq_id,
            QuotationRequest.customer_id == customer_id
        ).first()

    # --- Background Processing ---
    async def process_quotation_timeouts(self, db: Session):
        """
        Background job to process RFQs that have expired before being approved.
        """
        now = datetime.now(timezone.utc)
        
        # We need to find PENDING_APPROVAL RFQs where the current time is past their window_end
        # Note: window_end is currently stored as a string (e.g. "15:30"). 
        # A proper implementation would convert window_end to a UTC datetime for reliable comparison.
        # For this prototype, we'll do a basic check if window_end is parseable into today's date.
        
        expired_rfqs = db.query(QuotationRequest).filter(
            QuotationRequest.status == 'PENDING_APPROVAL'
        ).all()
        
        # Simple evaluation loop
        expired_count = 0
        from app.crud.crud import log_action
        
        for rfq in expired_rfqs:
            # Safely try to parse window_end as HH:MM against today's date
            try:
                # Assuming window_end is HH:MM in local time
                # In a robust system, window_end would be a proper TIMESTAMP WITH TIMEZONE
                if rfq.window_end:
                     target_datetime = rfq.window_end
                     
                     # Ensure awareness
                     if target_datetime.tzinfo is None:
                         target_datetime = target_datetime.replace(tzinfo=timezone.utc)
                         
                     if now > target_datetime:
                         # Expired!
                         rfq.status = 'REJECTED'
                         expired_count += 1
                         
                         log_action(
                             db,
                             user_id=rfq.created_by_user_id,
                             action_type="QUOTATION_AUTO_REJECTED",
                             entity_type="QuotationRequest",
                             entity_id=None,
                             details={"rfq_id": rfq.id, "ref_no": rfq.ref_no, "reason": "Time window expired before Corporate Admin approval"},
                             customer_id=rfq.customer_id
                         )
            except Exception as e:
                print(f"Error evaluating timeout for RFQ {rfq.id}: {e}")
                
        if expired_count > 0:
            db.commit()

crud_quotation = CRUDQuotation()
