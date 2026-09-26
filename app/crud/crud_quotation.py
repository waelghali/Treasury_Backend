from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime, timezone, date
import uuid
import json

from app.models.models_quotation import (
    QuotationBank, QuotationRequest, QuotationBankAssignment, 
    QuotationOffer, QuotationTBillOffer, QuotationAnalytics,
    QuotationLeg, QuotationBankLegConfig
)
from app.schemas.schemas_quotation import QuotationRequestCreate, QuotationBankCreate

def _parse_date_only(val):
    if not val:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    try:
        clean_str = str(val).strip().split('T')[0]
        return datetime.strptime(clean_str, "%Y-%m-%d").date()
    except Exception:
        return None

def _parse_banks_payload(raw_data):
    if not raw_data:
        return []
    if isinstance(raw_data, list):
        return [b.dict() if hasattr(b, 'dict') else b for b in raw_data]
    if isinstance(raw_data, str):
        try:
            parsed = json.loads(raw_data)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return []
    return []

class CRUDQuotation:
    
    # --- Quotation Banks ---
    def create_quotation_bank(self, db: Session, customer_id: int, obj_in: QuotationBankCreate):
        contacts_data = []
        if obj_in.contacts:
            contacts_data = [c.dict() if hasattr(c, 'dict') else c for c in obj_in.contacts]
            emails_str = ", ".join([c["email"].strip() for c in contacts_data if c.get("email")])
        elif obj_in.emails:
            emails_list = [e.strip() for e in obj_in.emails.split(",") if e.strip()]
            contacts_data = [{"email": e, "name": "", "role": "EXECUTION"} for e in emails_list]
            emails_str = ", ".join(emails_list)
        else:
            emails_str = ""

        # Check if already exists for this customer and trade_type
        existing = db.query(QuotationBank).filter(
            QuotationBank.customer_id == customer_id,
            QuotationBank.bank_id == obj_in.bank_id,
            QuotationBank.trade_type == (obj_in.trade_type or "BOTH")
        ).first()
        
        entity_scope = getattr(obj_in, "entity_scope", "ALL_ENTITIES") or "ALL_ENTITIES"
        entity_ids = getattr(obj_in, "entity_ids", []) or []

        if existing:
            # Update emails and contacts if changed
            existing.emails = emails_str
            existing.contacts = contacts_data
            existing.entity_scope = entity_scope

            from app.models.models_quotation import QuotationBankEntity
            db.query(QuotationBankEntity).filter(QuotationBankEntity.quotation_bank_id == existing.id).delete()
            if entity_scope == "SPECIFIC_ENTITIES":
                for eid in entity_ids:
                    db.add(QuotationBankEntity(quotation_bank_id=existing.id, entity_id=eid))

            db.commit()
            db.refresh(existing)
            existing.entity_ids = entity_ids if entity_scope == "SPECIFIC_ENTITIES" else []
            return existing

        db_obj = QuotationBank(
            customer_id=customer_id,
            bank_id=obj_in.bank_id,
            emails=emails_str,
            contacts=contacts_data,
            trade_type=obj_in.trade_type or "BOTH",
            entity_scope=entity_scope
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)

        if entity_scope == "SPECIFIC_ENTITIES":
            from app.models.models_quotation import QuotationBankEntity
            for eid in entity_ids:
                db.add(QuotationBankEntity(quotation_bank_id=db_obj.id, entity_id=eid))
            db.commit()

        db_obj.entity_ids = entity_ids if entity_scope == "SPECIFIC_ENTITIES" else []
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

    def get_quotation_banks(self, db: Session, customer_id: int, trade_type: str = None, entity_id: int = None):
        from app.models.models_quotation import QuotationBankEntity
        query = db.query(QuotationBank).filter(QuotationBank.customer_id == customer_id)
        if trade_type:
            # If trade_type is specified, return banks matching the specific type OR "BOTH"
            query = query.filter(QuotationBank.trade_type.in_([trade_type, "BOTH"]))

        if entity_id:
            from sqlalchemy import or_
            query = query.filter(
                or_(
                    QuotationBank.entity_scope == 'ALL_ENTITIES',
                    QuotationBank.id.in_(
                        db.query(QuotationBankEntity.quotation_bank_id).filter(QuotationBankEntity.entity_id == entity_id)
                    )
                )
            )

        banks = query.all()
        for b in banks:
            if not b.contacts:
                emails_list = [e.strip() for e in (b.emails or "").split(",") if e.strip()]
                b.contacts = [{"email": e, "name": "", "role": "EXECUTION"} for e in emails_list]
            b.entity_ids = [assoc.entity_id for assoc in b.entity_associations] if b.entity_associations else []
        return banks

    def get_unique_retender_ref_no(self, db: Session, parent_rfq: QuotationRequest):
        """
        Traverses to the root parent, inspects all existing re-tender references in the database,
        and generates the next available unique sequential ref_no (e.g. RFQ-...-R1, -R2, -R3).
        """
        root_parent = parent_rfq
        while root_parent.parent_rfq_id:
            ancestor = db.query(QuotationRequest).filter(QuotationRequest.id == root_parent.parent_rfq_id).first()
            if ancestor:
                root_parent = ancestor
            else:
                break

        base_ref = root_parent.ref_no.split("-R")[0]
        similar_refs = db.query(QuotationRequest.ref_no).filter(
            QuotationRequest.ref_no.like(f"{base_ref}%")
        ).all()

        max_idx = 0
        for (r_val,) in similar_refs:
            if "-R" in r_val:
                try:
                    suffix = r_val.split("-R")[-1]
                    idx = int(suffix)
                    if idx > max_idx:
                        max_idx = idx
                except ValueError:
                    pass

        next_idx = max_idx + 1
        new_ref = f"{base_ref}-R{next_idx}"

        # Final guarantee: verify non-existence
        while db.query(QuotationRequest.id).filter(QuotationRequest.ref_no == new_ref).first():
            next_idx += 1
            new_ref = f"{base_ref}-R{next_idx}"

        return new_ref, root_parent.id

    # --- Requests ---
    def create_request(self, db: Session, customer_id: int, user_id: int, requires_approval: bool, obj_in: QuotationRequestCreate, document_path: str = None):
        rfq_id = str(uuid.uuid4())
        date_str = datetime.now().strftime("%Y%m%d")
        
        parent_rfq = None
        if getattr(obj_in, 'parent_rfq_id', None):
            parent_rfq = db.query(QuotationRequest).filter(
                QuotationRequest.id == obj_in.parent_rfq_id,
                QuotationRequest.customer_id == customer_id
            ).first()

        if parent_rfq:
            ref_no, parent_id = self.get_unique_retender_ref_no(db, parent_rfq)
        else:
            prefix = "TB" if obj_in.type == "TBILL" else "RFQ"
            ref_no = f"{prefix}-{date_str}-{uuid.uuid4().hex[:4].upper()}"
            while db.query(QuotationRequest.id).filter(QuotationRequest.ref_no == ref_no).first():
                ref_no = f"{prefix}-{date_str}-{uuid.uuid4().hex[:4].upper()}"
            parent_id = None

        initial_status = "PENDING_APPROVAL" if requires_approval else "PENDING"
        allow_alt_master = getattr(obj_in, 'allowAlternativeValueDate', False) or False
        effective_eval_rate = obj_in.evalRate
        if effective_eval_rate is None or effective_eval_rate <= 0:
            try:
                from app.core.background_tasks import get_effective_quotation_eval_rate
                effective_eval_rate = get_effective_quotation_eval_rate(db, customer_id)
            except Exception:
                effective_eval_rate = 20.25

        # --- Strict Market Rule: Settlement / Value date cannot precede quotation trade date ---
        w_date = _parse_date_only(obj_in.windowStart)
        if w_date:
            if (obj_in.type == 'FX_SPOT' or not obj_in.type) and obj_in.valueDate:
                val_d = _parse_date_only(obj_in.valueDate)
                if val_d and val_d < w_date:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Master Value Date ({val_d}) cannot be earlier than quotation window date ({w_date}). Settlement date can be the same day or later, but never earlier."
                    )
            elif obj_in.type == 'TBILL' and obj_in.settlementDateStart:
                settle_d = _parse_date_only(obj_in.settlementDateStart)
                if settle_d and settle_d < w_date:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"T-Bill Settlement Date ({settle_d}) cannot be earlier than quotation window date ({w_date})."
                    )

        # Determine multi-pair vs single-ticket
        pairs_list = getattr(obj_in, 'pairs', None) or getattr(obj_in, 'legs', None) or []
        is_multi_pair = len(pairs_list) > 0

        if is_multi_pair:
            if len(pairs_list) > 4:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="A maximum of 4 currency pairs can be submitted in a single quotation request."
                )

            # Validate against similar/duplicate pairs (same currencies, same value date, and same quotation base)
            seen_pair_keys = set()
            for idx, p_item in enumerate(pairs_list, start=1):
                b_curr = (getattr(p_item, 'buyCurrency', None) or getattr(p_item, 'buy_currency', None) or obj_in.buyCurrency or 'USD').strip().upper()
                s_curr = (getattr(p_item, 'sellCurrency', None) or getattr(p_item, 'sell_currency', None) or obj_in.sellCurrency or 'EGP').strip().upper()
                v_date = str(getattr(p_item, 'valueDate', None) or getattr(p_item, 'value_date', None) or obj_in.valueDate or '').strip()
                q_b = (getattr(p_item, 'quotationBase', None) or getattr(p_item, 'quotation_base', None) or obj_in.quotationBase or 'Execution').strip().lower()

                # Currencies set handles both matching and reversed currencies (e.g., USD/EGP vs EGP/USD)
                pair_key = (frozenset([b_curr, s_curr]), v_date, q_b)
                if pair_key in seen_pair_keys:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Duplicate currency pair detected: Multiple legs requested for {b_curr}/{s_curr} with the same settlement date ({v_date}) and quotation base ({q_b.capitalize()}). Similar pairs with matching settlement date and quotation base are not permitted."
                    )
                seen_pair_keys.add(pair_key)

        first_pair = pairs_list[0] if is_multi_pair else None
        p_type = obj_in.type or "FX_SPOT"
        p_direction = (first_pair.direction if is_multi_pair and first_pair.direction else obj_in.direction) or "Buy"
        p_val_date = (first_pair.valueDate if is_multi_pair and first_pair.valueDate else obj_in.valueDate)
        p_amount = (first_pair.amount if is_multi_pair and first_pair.amount is not None else obj_in.amount)
        p_min_ticket = (first_pair.minTicketAmount if is_multi_pair and first_pair.minTicketAmount is not None else obj_in.minTicketAmount)
        p_buy_curr = (first_pair.buyCurrency if is_multi_pair and first_pair.buyCurrency else obj_in.buyCurrency) or "USD"
        p_sell_curr = (first_pair.sellCurrency if is_multi_pair and first_pair.sellCurrency else obj_in.sellCurrency) or "EGP"
        p_base = (first_pair.quotationBase if is_multi_pair and first_pair.quotationBase else obj_in.quotationBase) or "Execution"
        p_tol = (first_pair.maxTolerancePercent if is_multi_pair and first_pair.maxTolerancePercent is not None else obj_in.maxTolerancePercent)
        p_alt_val = bool(first_pair.allowAlternativeValueDate if is_multi_pair else allow_alt_master)

        db_rfq = QuotationRequest(
            id=rfq_id,
            ref_no=ref_no,
            customer_id=customer_id,
            entity_id=getattr(obj_in, 'entity_id', None),
            created_by_user_id=user_id,
            type=p_type,
            direction=p_direction,
            value_date=str(p_val_date) if p_val_date else None,
            amount=p_amount,
            min_ticket_amount=p_min_ticket,
            buy_currency=p_buy_curr,
            sell_currency=p_sell_curr,
            settlement_date_start=obj_in.settlementDateStart,
            settlement_date_end=obj_in.settlementDateEnd,
            maturity_date_start=obj_in.maturityDateStart,
            maturity_date_end=obj_in.maturityDateEnd,
            eval_rate=effective_eval_rate,
            window_start=obj_in.windowStart,
            window_end=obj_in.windowEnd,
            quotation_base=p_base,
            max_tolerance_percent=p_tol,
            allow_alternative_value_date=p_alt_val,
            document_path=document_path or obj_in.documentPath,
            status=initial_status,
            token_validity_hours=getattr(obj_in, 'token_validity_hours', 24) or 24,
            parent_rfq_id=parent_id,
            internal_notes=getattr(obj_in, 'internal_notes', None) or getattr(obj_in, 'internalNotes', None)
        )
        db.add(db_rfq)
        db.flush()

        # Create Legs
        created_legs = []
        if is_multi_pair:
            for idx, p_item in enumerate(pairs_list, start=1):
                leg_id = f"{rfq_id}-leg-{idx}"
                leg_val_d = getattr(p_item, 'valueDate', None)
                leg_obj = QuotationLeg(
                    id=leg_id,
                    rfq_id=rfq_id,
                    leg_index=idx,
                    type=p_type,
                    direction=getattr(p_item, 'direction', None) or p_direction,
                    buy_currency=getattr(p_item, 'buyCurrency', None) or p_buy_curr,
                    sell_currency=getattr(p_item, 'sellCurrency', None) or p_sell_curr,
                    amount=getattr(p_item, 'amount', None),
                    min_ticket_amount=getattr(p_item, 'minTicketAmount', None),
                    value_date=str(leg_val_d) if leg_val_d else None,
                    allow_alternative_value_date=bool(getattr(p_item, 'allowAlternativeValueDate', False)),
                    quotation_base=getattr(p_item, 'quotationBase', None) or p_base,
                    max_tolerance_percent=getattr(p_item, 'maxTolerancePercent', None) or p_tol,
                    status=initial_status,
                    entity_id=getattr(obj_in, 'entity_id', None)
                )
                db.add(leg_obj)
                created_legs.append((leg_obj, p_item))
        else:
            leg_id = f"{rfq_id}-leg-1"
            leg_obj = QuotationLeg(
                id=leg_id,
                rfq_id=rfq_id,
                leg_index=1,
                type=obj_in.type,
                direction=obj_in.direction,
                buy_currency=obj_in.buyCurrency,
                sell_currency=obj_in.sellCurrency,
                amount=obj_in.amount,
                min_ticket_amount=obj_in.minTicketAmount,
                value_date=str(obj_in.valueDate) if obj_in.valueDate else None,
                allow_alternative_value_date=allow_alt_master,
                quotation_base=obj_in.quotationBase,
                max_tolerance_percent=obj_in.maxTolerancePercent,
                status=initial_status,
                entity_id=getattr(obj_in, 'entity_id', None)
            )
            db.add(leg_obj)
            created_legs.append((leg_obj, obj_in))

        db.flush()

        # Parse assigned banks across all pairs (Single token per bank per RFQ session)
        assignments = []
        assignment_by_bank_id = {} # bank_id -> QuotationBankAssignment

        root_banks_data = _parse_banks_payload(getattr(obj_in, 'selectedBanks', None))

        for leg_obj, p_source in created_legs:
            leg_banks = _parse_banks_payload(getattr(p_source, 'selectedBanks', None))
            # Fall back to root banks if pair didn't specify banks
            if not leg_banks:
                leg_banks = root_banks_data

            for b_data in leg_banks:
                raw_bank_id = b_data.get('id')
                if not raw_bank_id:
                    continue

                q_bank = db.query(QuotationBank).filter(
                    QuotationBank.customer_id == customer_id,
                    QuotationBank.bank_id == raw_bank_id,
                    QuotationBank.trade_type.in_([p_type, "BOTH"])
                ).first()

                if not q_bank:
                    continue

                # Ensure single QuotationBankAssignment per bank per RFQ session
                if raw_bank_id not in assignment_by_bank_id:
                    assign_id = str(uuid.uuid4())
                    token = str(uuid.uuid4())

                    q_base_override = b_data.get('quotationBase') or p_base
                    contacts = q_bank.contacts if isinstance(q_bank.contacts, list) else []
                    has_approver = any(c.get('role') == 'APPROVER' for c in contacts)
                    has_execution = any(c.get('role') == 'EXECUTION' for c in contacts)
                    is_exec = (p_base or '').lower() == 'execution' or (q_base_override or '').lower() == 'execution'
                    bank_approval_status = 'PENDING' if (has_approver and has_execution and is_exec) else None

                    b_val_d = _parse_date_only(b_data.get('valueDate') or leg_obj.value_date)
                    b_allow_alt = b_data.get('allowAlternativeValueDate')

                    db_assignment = QuotationBankAssignment(
                        id=assign_id,
                        rfq_id=rfq_id,
                        quotation_bank_id=q_bank.id,
                        token=token,
                        cost_min=b_data.get('costMin', 0.0),
                        cost_percent=b_data.get('costPercent', 0.0),
                        cost_max=b_data.get('costMax', 0.0),
                        cost_flat=b_data.get('costFlat', 0.0),
                        quotation_base=q_base_override,
                        is_document_visible=b_data.get('isDocumentVisible', True),
                        value_date=b_val_d,
                        allow_alternative_value_date=b_allow_alt,
                        approval_status=bank_approval_status
                    )
                    db.add(db_assignment)
                    db.flush()
                    assignment_by_bank_id[raw_bank_id] = db_assignment
                    assignments.append({
                        "id": assign_id,
                        "bankId": raw_bank_id,
                        "quotation_bank_id": q_bank.id,
                        "token": token,
                        "approval_status": bank_approval_status
                    })

                db_assign = assignment_by_bank_id[raw_bank_id]

                # Create pair-level bank config
                cfg_id = str(uuid.uuid4())
                leg_cfg_val_d = _parse_date_only(b_data.get('valueDate') or leg_obj.value_date)
                leg_bank_cfg = QuotationBankLegConfig(
                    id=cfg_id,
                    assignment_id=db_assign.id,
                    leg_id=leg_obj.id,
                    is_invited=True,
                    cost_min=b_data.get('costMin', 0.0),
                    cost_percent=b_data.get('costPercent', 0.0),
                    cost_max=b_data.get('costMax', 0.0),
                    cost_flat=b_data.get('costFlat', 0.0),
                    quotation_base=b_data.get('quotationBase') or leg_obj.quotation_base,
                    is_document_visible=b_data.get('isDocumentVisible', True),
                    value_date=leg_cfg_val_d,
                    allow_alternative_value_date=b_data.get('allowAlternativeValueDate')
                )
                db.add(leg_bank_cfg)

        db.commit()
        db.refresh(db_rfq)
        return db_rfq, assignments

    def get_requests(self, db: Session, customer_id: int = None, allowed_entity_ids: list = None):
        query = db.query(QuotationRequest)
        if customer_id is not None:
            query = query.filter(QuotationRequest.customer_id == customer_id)
        if allowed_entity_ids is not None:
            query = query.filter(QuotationRequest.entity_id.in_(allowed_entity_ids))
        reqs = query.order_by(QuotationRequest.created_at.desc()).all()
        for r in reqs:
            if r.creator:
                r.creator_name = r.creator.email.split('@')[0] if r.creator.email else "End User"
            else:
                r.creator_name = "End User"
            r.entity_name = r.entity.entity_name if r.entity else None
            r.entity_code = r.entity.code if r.entity else None
        return reqs

    def get_request(self, db: Session, rfq_id: str, customer_id: int):
        r = db.query(QuotationRequest).filter(
            QuotationRequest.id == rfq_id,
            QuotationRequest.customer_id == customer_id
        ).first()
        if r:
            if r.creator:
                r.creator_name = r.creator.email.split('@')[0] if r.creator.email else "End User"
            else:
                r.creator_name = "End User"
            r.entity_name = r.entity.entity_name if r.entity else None
            r.entity_code = r.entity.code if r.entity else None
        return r

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
