from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Request
from sqlalchemy.orm import Session
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict
import os
import secrets
import uuid

from app.database import get_db
from app.crud.base import log_action
from app.models.models_quotation import (
    QuotationBankAssignment, QuotationRequest, QuotationOffer, 
    QuotationTBillOffer, QuotationBank, QuotationBankContactInvitation, QuotationAccessOTP, QuotationAnalytics,
    QuotationNotification
)
from app.schemas.schemas_quotation import (
    FXSpotOfferCreate, FXSpotMultiOfferCreate, TBillOfferCreate, OTPRequestCreate, OTPVerifyCreate,
    BankApprovalActionCreate, DeskSessionActionRequest
)
from app.services.desk_session_service import desk_session_service
from app.core.email_service import send_email, get_customer_email_settings, get_global_email_settings
from app.core.routing import get_frontend_base_url
from app.core.rate_limiter import quotation_rate_limiter
from app.core.otp_security import hash_otp_code, verify_otp_code, MAX_OTP_FAILED_ATTEMPTS

router = APIRouter()

_RECENT_ACCESS_LOGS: Dict[str, datetime] = {}

def _should_debounce_access_log(key: str, now: datetime, cooldown_seconds: int = 60) -> bool:
    last_time = _RECENT_ACCESS_LOGS.get(key)
    if last_time and (now - last_time).total_seconds() < cooldown_seconds:
        return True
    _RECENT_ACCESS_LOGS[key] = now
    if len(_RECENT_ACCESS_LOGS) > 1000:
        cutoff = now - timedelta(minutes=10)
        to_del = [k for k, v in _RECENT_ACCESS_LOGS.items() if v < cutoff]
        for k in to_del:
            _RECENT_ACCESS_LOGS.pop(k, None)
    return False

def _format_time_diff(seconds: float) -> str:
    sec = max(1, int(abs(seconds)))
    mins, s = divmod(sec, 60)
    hours, mins = divmod(mins, 60)
    if hours > 0:
        return f"{hours}h {mins}m {s}s"
    if mins > 0:
        return f"{mins}m {s}s"
    return f"{s}s"

def _get_bank_contacts_list(q_bank: QuotationBank):
    """Helper to extract normalized contacts list with roles."""
    if q_bank.contacts and isinstance(q_bank.contacts, list) and len(q_bank.contacts) > 0:
        return q_bank.contacts
    emails_list = [e.strip() for e in (q_bank.emails or "").split(",") if e.strip()]
    return [{"email": e, "name": "", "role": "EXECUTION"} for e in emails_list]

def _clean_magic_token(token_str: Optional[str]) -> Optional[str]:
    """Extracts raw magic token if appended with tab-specific concurrency salt."""
    if not token_str:
        return None
    return token_str.split('_tab_')[0] if '_tab_' in token_str else token_str

@router.get("/{token}")
async def get_rfq_by_token(token: str, request: Request, db: Session = Depends(get_db)):
    """Fetch RFQ details securely using token."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    bank_name = q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank"
    
    # Send detailed customer name as requested! 
    customer_name = rfq.customer.name if rfq.customer else "Unknown Customer"

    # Process Gap Closure: Block access if awaiting internal approval
    if rfq.status == 'PENDING_APPROVAL':
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="This quotation is awaiting internal corporate approval and is not yet open for bidding."
        )

    if rfq.status == 'CANCELLED':
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="This quotation request was officially withdrawn by the corporate treasury desk. No quotation is required."
        )

    now = datetime.now(timezone.utc)
    
    # Handle naive vs aware datetimes safely
    window_start = rfq.window_start
    window_end = rfq.window_end
    if window_start and window_start.tzinfo is None:
        window_start = window_start.replace(tzinfo=timezone.utc)
    if window_end and window_end.tzinfo is None:
        window_end = window_end.replace(tzinfo=timezone.utc)

    # Calculate is_open boolean
    is_open = False
    if window_start and window_end:
        is_open = (window_start <= now <= window_end)

    # Clean audit logging of portal visits (debounced per 60s per assignment/action in-memory)
    client_host = request.client.host if request and request.client else None
    if window_end and now > window_end:
        diff_sec = (now - window_end).total_seconds()
        diff_str = _format_time_diff(diff_sec)
        debounce_key = f"{assignment.id}:LATE"
        if not _should_debounce_access_log(debounce_key, now, cooldown_seconds=60):
            log_action(
                db=db,
                user_id=None,
                action_type="QUOTATION_BANK_LATE_ACCESS_ATTEMPT",
                entity_type="QuotationRequest",
                entity_id=None,
                details={
                    "rfq_id": str(rfq.id),
                    "ref_no": rfq.ref_no,
                    "bank_name": bank_name,
                    "action_description": f"Bank counterparty accessed portal {diff_str} after bidding window closed. Inputs locked/dimmed.",
                    "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S UTC") if window_start else None,
                    "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S UTC") if window_end else None,
                    "attempted_at": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "seconds_past_deadline": int(diff_sec),
                    "status": "CLOSED"
                },
                customer_id=rfq.customer_id,
                ip_address=client_host
            )
            db.commit()
    elif window_start and now < window_start:
        diff_sec = (window_start - now).total_seconds()
        diff_str = _format_time_diff(diff_sec)
        debounce_key = f"{assignment.id}:EARLY"
        if not _should_debounce_access_log(debounce_key, now, cooldown_seconds=60):
            log_action(
                db=db,
                user_id=None,
                action_type="QUOTATION_BANK_EARLY_ACCESS_ATTEMPT",
                entity_type="QuotationRequest",
                entity_id=None,
                details={
                    "rfq_id": str(rfq.id),
                    "ref_no": rfq.ref_no,
                    "bank_name": bank_name,
                    "action_description": f"Bank counterparty accessed portal {diff_str} before bidding window opened. Inputs locked until window opens.",
                    "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S UTC") if window_start else None,
                    "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S UTC") if window_end else None,
                    "attempted_at": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "seconds_until_open": int(diff_sec),
                    "status": "PRE_WINDOW"
                },
                customer_id=rfq.customer_id,
                ip_address=client_host
            )
            db.commit()

    # Process Token Validity Expiry
    validity_hours = rfq.token_validity_hours or 24
    if window_end and now > (window_end + timedelta(hours=validity_hours)):
        raise HTTPException(
            status_code=status.HTTP_410_GONE, 
            detail="The validity of this quotation link has expired."
        )

    offers = []
    if rfq.type == 'TBILL':
        tbill_records = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == assignment.id).all()
        offers = [{
            "settlement_date": o.settlement_date,
            "maturity_date": o.maturity_date,
            "discount_rate": o.discount_rate,
            "max_amount": o.max_amount,
            "notes": o.notes,
            "submitted_by_email": o.submitted_by_email,
            "submitted_at": o.submitted_at
        } for o in tbill_records]
    else:
        # FX_SPOT
        offer = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == assignment.id).order_by(QuotationOffer.submitted_at.desc()).first()
        if offer:
            offers = [{
                "price": offer.price, 
                "offered_value_date": offer.offered_value_date,
                "notes": offer.notes,
                "submitted_by_email": offer.submitted_by_email,
                "submitted_at": offer.submitted_at
            }]

    effective_base = (assignment.quotation_base or rfq.quotation_base or 'Execution').lower()

    parsed_docs = []
    # Indicative RFQs never share documents, and document visibility flag is respected.
    # If release_docs_to_winner_only is active, documents are strictly withheld during the bidding window.
    if (
        effective_base != 'indicative' 
        and (assignment.is_document_visible is not False) 
        and rfq.document_path 
        and not getattr(rfq, 'release_docs_to_winner_only', False)
    ):
        raw_docs = []
        try:
            import json
            loaded = json.loads(rfq.document_path)
            if isinstance(loaded, dict):
                raw_docs = loaded.get("documents", [])
            elif isinstance(loaded, list):
                raw_docs = loaded
            elif isinstance(loaded, str):
                raw_docs = [{"name": os.path.basename(loaded), "path": loaded}]
        except Exception:
            raw_paths = [p.strip() for p in rfq.document_path.split(',') if p.strip()]
            raw_docs = [{"name": os.path.basename(p), "path": p} for p in raw_paths]

        from app.core.ai_integration import generate_signed_gcs_url
        for d in raw_docs:
            p_str = d.get("path", "")
            if p_str.startswith("gs://"):
                try:
                    signed_url = await generate_signed_gcs_url(p_str, expiration=604800)
                    p_str = signed_url or p_str
                except Exception:
                    pass
            parsed_docs.append({"name": d.get("name") or "Document", "path": p_str})

    contacts = _get_bank_contacts_list(q_bank) if q_bank else []

    cbe_benchmark_rate = None
    if rfq.type == 'FX_SPOT' and rfq.buy_currency and rfq.sell_currency:
        try:
            from app.services.fx_service import fx_service
            bm = fx_service.get_rate_by_code(db, from_code=rfq.buy_currency, to_code=rfq.sell_currency, allow_ai=False)
            if bm is not None:
                cbe_benchmark_rate = float(bm)
        except Exception:
            pass

    # Live Ranking Evaluation
    is_live_ranking_enabled = False
    live_rank = None
    total_quotes = 0
    try:
        from app.services.live_ranking_service import live_ranking_service
        actual_bank_id = q_bank.bank_id if q_bank else None
        rfq_entity_id = getattr(rfq, 'entity_id', None)
        is_live_ranking_enabled = live_ranking_service.evaluate_live_ranking_eligibility(
            db, bank_id=actual_bank_id, customer_id=rfq.customer_id,
            entity_id=rfq_entity_id, trade_type=rfq.type or 'FX_SPOT'
        )
        if is_live_ranking_enabled and offers:
            rank_result = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment.id)
            if rank_result is not None:
                live_rank = rank_result
                # Count total banks that have submitted
                all_assignments = db.query(QuotationBankAssignment).filter(
                    QuotationBankAssignment.rfq_id == rfq.id
                ).all()
                if rfq.type == 'TBILL':
                    submitted_ids = set(
                        o.assignment_id for o in db.query(QuotationTBillOffer).filter(
                            QuotationTBillOffer.assignment_id.in_([a.id for a in all_assignments])
                        ).all()
                    )
                else:
                    submitted_ids = set(
                        o.assignment_id for o in db.query(QuotationOffer).filter(
                            QuotationOffer.assignment_id.in_([a.id for a in all_assignments])
                        ).all()
                    )
                total_quotes = len(submitted_ids)
    except Exception:
        pass

    effective_value_date = assignment.value_date or rfq.value_date
    effective_value_date_str = str(effective_value_date).split('T')[0] if effective_value_date else None
    effective_allow_alt = assignment.allow_alternative_value_date if assignment.allow_alternative_value_date is not None else (rfq.allow_alternative_value_date or False)

    # Build legs structure for multi-pair RFQ
    portal_legs = []
    rfq_legs = rfq.legs if rfq.legs else []
    from app.services.fx_service import fx_service

    for leg in rfq_legs:
        leg_cfg = assignment.get_config_for_leg(leg.id)
        cfg_val_date = leg_cfg.value_date if leg_cfg and leg_cfg.value_date else (assignment.value_date or rfq.value_date)
        cfg_allow_alt = (leg_cfg.allow_alternative_value_date if leg_cfg and leg_cfg.allow_alternative_value_date is not None 
                         else (assignment.allow_alternative_value_date if assignment.allow_alternative_value_date is not None 
                               else (rfq.allow_alternative_value_date or False)))
        cfg_base = (leg_cfg.quotation_base if leg_cfg and leg_cfg.quotation_base 
                    else (assignment.quotation_base or rfq.quotation_base or 'Execution'))
        cfg_doc_vis = (leg_cfg.is_document_visible if leg_cfg and leg_cfg.is_document_visible is not None 
                       else (assignment.is_document_visible is not False))
        cfg_cost_pct = leg_cfg.cost_percent if leg_cfg else (assignment.cost_percent or 0.0)
        cfg_cost_flat = leg_cfg.cost_flat if leg_cfg else (assignment.cost_flat or 0.0)
        cfg_cost_min = leg_cfg.cost_min if leg_cfg else (assignment.cost_min or 0.0)
        cfg_cost_max = leg_cfg.cost_max if leg_cfg else (assignment.cost_max or 0.0)

        # Submitted offer for this leg by this bank
        leg_offer = db.query(QuotationOffer).filter(
            QuotationOffer.assignment_id == assignment.id,
            QuotationOffer.leg_id == leg.id
        ).order_by(QuotationOffer.submitted_at.desc()).first()

        if not leg_offer and len(rfq_legs) == 1:
            leg_offer = db.query(QuotationOffer).filter(
                QuotationOffer.assignment_id == assignment.id
            ).order_by(QuotationOffer.submitted_at.desc()).first()

        leg_offers_list = []
        if leg_offer:
            leg_offers_list.append({
                "price": leg_offer.price,
                "offered_value_date": str(leg_offer.offered_value_date).split('T')[0] if leg_offer.offered_value_date else None,
                "notes": leg_offer.notes,
                "submitted_by_email": leg_offer.submitted_by_email,
                "submitted_at": leg_offer.submitted_at
            })

        # Leg CBE benchmark
        leg_bm = None
        if rfq.type == 'FX_SPOT' and leg.buy_currency and leg.sell_currency:
            try:
                bm_val = fx_service.get_rate_by_code(db, from_code=leg.buy_currency, to_code=leg.sell_currency, allow_ai=False)
                if bm_val is not None:
                    leg_bm = float(bm_val)
            except Exception:
                pass

        # Leg Live Rank
        leg_rank_info = None
        if is_live_ranking_enabled and leg_offers_list:
            try:
                from app.services.live_ranking_service import live_ranking_service
                leg_ranks_map = live_ranking_service.calculate_bank_live_ranks_by_leg(db, rfq.id, assignment.id)
                leg_rank_info = leg_ranks_map.get(leg.id) or leg_ranks_map.get(str(leg.id))
            except Exception:
                pass

        portal_legs.append({
            "id": leg.id,
            "pair_order": leg.pair_order,
            "currency_pair": leg.currency_pair,
            "buy_currency": leg.buy_currency,
            "sell_currency": leg.sell_currency,
            "direction": leg.direction,
            "amount": leg.amount,
            "min_ticket_amount": leg.min_ticket_amount,
            "eval_rate": leg.eval_rate,
            "tolerance_percent": leg.tolerance_percent,
            "value_date": str(cfg_val_date).split('T')[0] if cfg_val_date else None,
            "allow_alternative_value_date": cfg_allow_alt,
            "quotation_base": cfg_base,
            "is_document_visible": cfg_doc_vis,
            "cost_percent": cfg_cost_pct,
            "cost_flat": cfg_cost_flat,
            "cost_min": cfg_cost_min,
            "cost_max": cfg_cost_max,
            "offers": leg_offers_list,
            "cbe_benchmark_rate": leg_bm,
            "live_rank": leg_rank_info
        })

    # Acceptance timeout countdown configured by Corporate Admin (default: 120s for TBILL, 30s for FX_SPOT)
    acceptance_timeout_seconds = 120 if rfq.type == "TBILL" else 30
    try:
        from app.crud.crud_config import crud_customer_configuration
        from app.constants import GlobalConfigKey
        cfg_key = GlobalConfigKey.QUOTATION_ACCEPTANCE_TIMEOUT_TBILL if rfq.type == "TBILL" else GlobalConfigKey.QUOTATION_ACCEPTANCE_TIMEOUT_FX_SPOT
        cfg = crud_customer_configuration.get_customer_config_or_global_fallback(db, rfq.customer_id, cfg_key)
        if cfg and cfg.get("effective_value"):
            acceptance_timeout_seconds = int(cfg["effective_value"])
    except Exception:
        pass

    # Check if this multi-pair package is mixed (contains both Execution and Indicative legs)
    bank_assigned_base = (assignment.quotation_base or rfq.quotation_base or "Execution").capitalize()
    leg_bases = list(set((l.get("quotation_base") or bank_assigned_base).capitalize() for l in portal_legs)) if portal_legs else [bank_assigned_base]
    has_exec_leg = any(b.lower() == "execution" for b in leg_bases)
    has_indic_leg = any(b.lower() == "indicative" for b in leg_bases)
    is_mixed_rfq = bool(portal_legs and len(leg_bases) > 1 and has_exec_leg and has_indic_leg)

    overall_quotation_base = "Mixed" if is_mixed_rfq else (leg_bases[0] if (portal_legs and len(leg_bases) == 1) else bank_assigned_base)

    # Bank approval is required ONLY if ANY leg is execution (or overall execution) and bank has appropriate roles
    requires_bank_approval = has_exec_leg and any(c.get("role") == "EXECUTION" for c in (_get_bank_contacts_list(q_bank) if q_bank else [])) and any(c.get("role") == "APPROVER" for c in (_get_bank_contacts_list(q_bank) if q_bank else []))

    return {
        "id": rfq.id,
        "ref_no": rfq.ref_no,
        "type": rfq.type,
        "direction": rfq.direction,
        "value_date": effective_value_date_str,
        "allow_alternative_value_date": effective_allow_alt,
        "amount": rfq.amount,
        "min_ticket_amount": rfq.min_ticket_amount,
        "buy_currency": rfq.buy_currency,
        "sell_currency": rfq.sell_currency,
        "settlement_date_start": rfq.settlement_date_start,
        "settlement_date_end": rfq.settlement_date_end,
        "maturity_date_start": rfq.maturity_date_start,
        "maturity_date_end": rfq.maturity_date_end,
        "eval_rate": rfq.eval_rate,
        "window_start": rfq.window_start,
        "window_end": rfq.window_end,
        "acceptance_timeout_seconds": acceptance_timeout_seconds,
        "quotation_base": overall_quotation_base,
        "is_mixed": is_mixed_rfq,
        "has_execution_legs": has_exec_leg,
        "is_all_indicative": not has_exec_leg,
        "has_execution_dealers": any(c.get("role") == "EXECUTION" for c in (_get_bank_contacts_list(q_bank) if q_bank else [])),
        "document_path": rfq.document_path if (has_exec_leg and assignment.is_document_visible is not False and not getattr(rfq, 'release_docs_to_winner_only', False)) else None,
        "documents": parsed_docs,
        "status": rfq.status,
        "comments_to_banks": rfq.comments_to_banks,
        "assignment_id": assignment.id,
        "is_cross_entity": bool(assignment.is_cross_entity),
        "bank_name": bank_name,
        "customer_name": customer_name,
        "entity_name": customer_name if assignment.is_cross_entity else (rfq.entity.entity_name if rfq.entity else customer_name),
        "entity_tax_id": None if assignment.is_cross_entity else (rfq.entity.tax_id if rfq.entity else None),
        "entity_cr_number": None if assignment.is_cross_entity else (rfq.entity.commercial_register_number if rfq.entity else None),
        "entity_code": None if assignment.is_cross_entity else (rfq.entity.code if rfq.entity else None),
        "serverTime": now.isoformat(),
        "isWindowOpen": is_open,
        "offers": offers,
        "legs": portal_legs,
        "approval_status": assignment.approval_status if has_exec_leg else None,
        "approved_by_email": assignment.approved_by_email,
        "approved_at": assignment.approved_at.isoformat() if assignment.approved_at else None,
        "approval_notes": assignment.approval_notes,
        "has_execution_dealers": any(c.get("role") == "EXECUTION" for c in (_get_bank_contacts_list(q_bank) if q_bank else [])),
        "total_execution_dealers": sum(1 for c in (_get_bank_contacts_list(q_bank) if q_bank else []) if c.get("role") == "EXECUTION"),
        "requires_bank_approval": requires_bank_approval,
        "cbe_benchmark_rate": cbe_benchmark_rate,
        "is_live_ranking_enabled": is_live_ranking_enabled,
        "live_rank": live_rank,
        "total_quotes": total_quotes,
        "ranks_by_leg": (live_ranking_service.calculate_bank_live_ranks_by_leg(db, rfq.id, assignment.id) if is_live_ranking_enabled else {})
    }

@router.post("/request-otp")
async def request_quotation_otp(
    req: OTPRequestCreate,
    background_tasks: BackgroundTasks,
    request: Request,
    db: Session = Depends(get_db)
):
    """Generates and emails a 6-digit OTP code + 1-Click Magic Link to the bank representative."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == req.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    # Rate Limiting: Max 3 requests per 5 minutes (300 seconds) per client IP + assignment token
    client_ip = request.client.host if request and request.client else "unknown"
    rate_key = f"otp_req:{client_ip}:{assignment.token}"
    is_limited, retry_after = quotation_rate_limiter.is_rate_limited(rate_key, max_requests=3, window_seconds=300)
    if is_limited:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many verification code requests. Please wait {retry_after} seconds before requesting a new code.",
            headers={"Retry-After": str(retry_after)}
        )

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    if rfq.status == 'CANCELLED':
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="This quotation request was officially withdrawn by the corporate treasury desk. No quotation is required."
        )
    if rfq.status == 'PENDING_APPROVAL':
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This quotation is not currently open for bidding."
        )
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    if not q_bank:
        raise HTTPException(status_code=404, detail="Bank configuration not found")

    contacts = _get_bank_contacts_list(q_bank)
    target_email = req.email.strip().lower()
    
    # Match email in bank contact roster
    matched_contact = next((c for c in contacts if c.get("email", "").strip().lower() == target_email), None)
    if not matched_contact:
        raise HTTPException(
            status_code=400, 
            detail="The specified email is not registered for this bank counterparty roster."
        )

    role = matched_contact.get("role", "EXECUTION")
    contact_name = matched_contact.get("name") or target_email.split("@")[0]

    # Check if bank approval is required for this quotation
    leg_cfgs = getattr(assignment, "leg_configs", []) or []
    if leg_cfgs:
        has_exec_leg = any((c.quotation_base or "").lower() == "execution" for c in leg_cfgs)
    else:
        has_exec_leg = (getattr(assignment, "quotation_base", "") or getattr(rfq, "quotation_base", "") or "Execution").lower() in ("execution", "mixed")
    if getattr(assignment, "is_cross_entity", False):
        has_exec_leg = False
    has_approver = any(c.get("role") == "APPROVER" for c in contacts)
    has_execution = any(c.get("role") == "EXECUTION" for c in contacts)
    requires_approval = has_exec_leg and has_approver and has_execution

    # If approval is not required, heal any stale PENDING status
    if not requires_approval and assignment.approval_status == 'PENDING':
        assignment.approval_status = None
        db.commit()

    # Non-approvers are blocked if bank-level approval is required and not granted
    if role in ("EXECUTION", "VIEW_ONLY") and requires_approval:
        if assignment.approval_status == 'PENDING':
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail="This quotation is awaiting internal bank approval from your authorized approver."
            )
        elif assignment.approval_status == 'EXPIRED':
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail="Quotation window closed before your bank approved participation."
            )
        elif assignment.approval_status == 'DECLINED':
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail="Your bank's approver declined participation in this quotation."
            )

    # Invalidate any prior unverified OTPs for this assignment and email so only 1 OTP is active at a time
    now_utc = datetime.now(timezone.utc)
    db.query(QuotationAccessOTP).filter(
        QuotationAccessOTP.assignment_id == assignment.id,
        QuotationAccessOTP.email == target_email,
        QuotationAccessOTP.is_used == False
    ).update({
        QuotationAccessOTP.is_used: True,
        QuotationAccessOTP.expires_at: now_utc
    }, synchronize_session=False)

    # Generate 6-digit OTP & Magic Token
    otp_code = f"{secrets.randbelow(900000) + 100000}"
    magic_token = uuid.uuid4().hex
    expires_at = now_utc + timedelta(minutes=15)
    hashed_otp = hash_otp_code(otp_code)

    otp_record = QuotationAccessOTP(
        assignment_id=assignment.id,
        email=target_email,
        role=role,
        otp_code=hashed_otp,
        magic_token=magic_token,
        expires_at=expires_at,
        is_used=False,
        failed_attempts=0
    )
    db.add(otp_record)
    db.commit()

    # Build Email with 1-Click Magic Link
    base_url = get_frontend_base_url(request=request)
    magic_link = f"{base_url}/public-quotation/{req.token}?magic_token={magic_token}"
    customer_name = rfq.customer.name if rfq.customer else "Treasury Client"
    
    if role == "APPROVER":
        role_badge = "🛡️ Authorized Bank Approver"
    elif role == "VIEW_ONLY":
        role_badge = "👁️ View-Only Observer"
    else:
        role_badge = "⚡ Execution Trader"
    
    action_button_html = f"""
                <div style="text-align: center; margin-bottom: 24px;">
                    <a href="{magic_link}" style="display: inline-block; background-color: #2563eb; color: #ffffff; font-size: 14px; font-weight: 600; text-decoration: none; padding: 12px 28px; border-radius: 10px; box-shadow: 0 4px 6px -1px rgba(37,99,235,0.2);">
                        ⚡ 1-Click Instant Direct Access
                    </a>
                </div>
    """ if role != "APPROVER" else f"""
                <div style="background-color: #fffbeb; border: 1px solid #fde68a; border-radius: 10px; padding: 14px 18px; margin-bottom: 24px; text-align: center;">
                    <p style="margin: 0; font-size: 12px; color: #92400e; font-weight: 700;">
                        🛡️ 2FA Verification Code Required
                    </p>
                    <p style="margin: 4px 0 0 0; font-size: 11px; color: #b45309; line-height: 1.4;">
                        As an authorized Bank Approver, enter the 6-digit access code above in the verification window to authenticate your identity and review this deal.
                    </p>
                </div>
    """

    bank_display = q_bank.bank.name if q_bank and q_bank.bank else "Treasury Portal"
    subject = f"RFQ {rfq.ref_no} - Portal Verification Code {otp_code} - {bank_display}"
    body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f8fafc; margin: 0; padding: 24px; color: #1e293b;">
        <div style="max-width: 540px; margin: 0 auto; background: #ffffff; border-radius: 16px; border: 1px solid #e2e8f0; overflow: hidden; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);">
            <div style="background: #0f172a; padding: 28px 32px; text-align: center;">
                <h1 style="color: #ffffff; font-size: 20px; font-weight: 700; margin: 0; letter-spacing: -0.025em;">Treasury Quotation Verification</h1>
                <p style="color: #94a3b8; font-size: 13px; margin: 6px 0 0 0;">Request for Quotation Portal Access</p>
            </div>
            
            <div style="padding: 32px;">
                <p style="margin: 0 0 16px 0; font-size: 15px; line-height: 1.5;">Dear <strong>{contact_name}</strong>,</p>
                <p style="margin: 0 0 20px 0; font-size: 14px; line-height: 1.6; color: #475569;">
                    Use the verification code below to access the live RFQ (<strong>{rfq.ref_no}</strong>) for <strong>{customer_name}</strong>.
                </p>

                <div style="background: #f1f5f9; border-radius: 12px; padding: 20px; text-align: center; margin-bottom: 24px;">
                    <span style="display: block; font-size: 11px; text-transform: uppercase; font-weight: 700; color: #64748b; letter-spacing: 0.05em; margin-bottom: 6px;">Your 6-Digit Access Code</span>
                    <span style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 36px; font-weight: 800; letter-spacing: 0.25em; color: #0f172a; margin-left: 0.25em;">{otp_code}</span>
                    <span style="display: block; font-size: 12px; color: #94a3b8; margin-top: 8px;">Valid for 15 minutes</span>
                </div>

                {action_button_html}

                <div style="border-top: 1px solid #f1f5f9; padding-top: 16px; font-size: 12px; color: #64748b;">
                    <p style="margin: 0 0 4px 0;"><strong>Assigned Role:</strong> {role_badge}</p>
                    <p style="margin: 0;">If you did not request this verification code, please ignore this message.</p>
                </div>
            </div>
            
            <div style="background: #f8fafc; padding: 16px 32px; border-top: 1px solid #e2e8f0; font-size: 11px; color: #94a3b8; text-align: center;">
                Grow Treasury Portal &bull; Secure RFQ Bidding System
            </div>
        </div>
    </body>
    </html>
    """

    email_settings, source = get_customer_email_settings(db, rfq.customer_id)
    sender_name = "Treasury Quotations" if source != "customer_specific" else email_settings.sender_display_name

    background_tasks.add_task(
        send_email,
        db=db,
        to_emails=[target_email],
        subject_template=subject,
        body_template=body,
        template_data={},
        email_settings=email_settings,
        sender_name=sender_name,
        save_copy=False
    )

    # Audit log the dealer's verification request (Zero-Knowledge: strictly NO otp_code or magic_token logged)
    w_start = rfq.window_start
    w_end = rfq.window_end
    if w_start and w_start.tzinfo is None:
        w_start = w_start.replace(tzinfo=timezone.utc)
    if w_end and w_end.tzinfo is None:
        w_end = w_end.replace(tzinfo=timezone.utc)

    now_utc = datetime.now(timezone.utc)
    client_ip = request.client.host if request and request.client else None

    if w_end and now_utc > w_end:
        diff_sec = (now_utc - w_end).total_seconds()
        diff_str = _format_time_diff(diff_sec)
        log_action(
            db=db,
            user_id=None,
            action_type="QUOTATION_BANK_LATE_ACCESS_ATTEMPT",
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": str(rfq.id),
                "bank_name": bank_display,
                "dealer_email": target_email,
                "dealer_role": role,
                "action_description": f"Dealer requested verification code {diff_str} after bidding window closed. Inputs locked.",
                "ref_no": rfq.ref_no,
                "window_start": w_start.strftime("%Y-%m-%d %H:%M:%S UTC") if w_start else None,
                "window_end": w_end.strftime("%Y-%m-%d %H:%M:%S UTC") if w_end else None,
                "attempted_at": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "seconds_past_deadline": int(diff_sec),
                "status": "CLOSED"
            },
            customer_id=rfq.customer_id,
            ip_address=client_ip
        )
    elif w_start and now_utc < w_start:
        diff_sec = (w_start - now_utc).total_seconds()
        diff_str = _format_time_diff(diff_sec)
        log_action(
            db=db,
            user_id=None,
            action_type="QUOTATION_BANK_EARLY_ACCESS_ATTEMPT",
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": str(rfq.id),
                "bank_name": bank_display,
                "dealer_email": target_email,
                "dealer_role": role,
                "action_description": f"Dealer requested verification code {diff_str} before bidding window opened.",
                "ref_no": rfq.ref_no,
                "window_start": w_start.strftime("%Y-%m-%d %H:%M:%S UTC") if w_start else None,
                "window_end": w_end.strftime("%Y-%m-%d %H:%M:%S UTC") if w_end else None,
                "attempted_at": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "seconds_until_open": int(diff_sec),
                "status": "PRE_WINDOW"
            },
            customer_id=rfq.customer_id,
            ip_address=client_ip
        )
    else:
        log_action(
            db=db,
            user_id=None,
            action_type="QUOTATION_PORTAL_ACCESSED",
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": str(rfq.id),
                "bank_name": bank_display,
                "dealer_email": target_email,
                "dealer_role": role,
                "action_description": "Dealer requested verification code during active bidding window.",
                "ref_no": rfq.ref_no,
                "window_start": w_start.strftime("%Y-%m-%d %H:%M:%S UTC") if w_start else None,
                "window_end": w_end.strftime("%Y-%m-%d %H:%M:%S UTC") if w_end else None,
                "attempted_at": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "status": "OPEN"
            },
            customer_id=rfq.customer_id,
            ip_address=client_ip
        )
    db.commit()

    return {
        "message": f"Verification code sent to {target_email}",
        "email": target_email,
        "role": role,
        "name": contact_name
    }

@router.post("/verify-otp")
def verify_quotation_otp(
    req: OTPVerifyCreate,
    request: Request,
    db: Session = Depends(get_db)
):
    """Verifies 6-digit OTP code or Magic Link token and establishes authenticated portal session."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == req.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    # 1. Rate limiting (max 5 verification attempts per 60 seconds per IP + assignment token)
    client_ip = request.client.host if request and request.client else "unknown"
    rate_key = f"otp_verify:{client_ip}:{assignment.token}"
    is_limited, retry_after = quotation_rate_limiter.is_rate_limited(rate_key, max_requests=5, window_seconds=60)
    if is_limited:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many verification attempts. Please wait {retry_after} seconds before trying again.",
            headers={"Retry-After": str(retry_after)}
        )

    now = datetime.now(timezone.utc)

    if req.magic_token:
        otp_record = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == req.magic_token,
            QuotationAccessOTP.expires_at > now,
            QuotationAccessOTP.is_used == False
        ).order_by(QuotationAccessOTP.created_at.desc()).first()

        if not otp_record:
            raise HTTPException(status_code=400, detail="Invalid or expired verification link.")

        # Bank Approvers MUST verify with 6-digit OTP code - magic token bypass is strictly forbidden
        if otp_record.role == "APPROVER":
            raise HTTPException(
                status_code=400, 
                detail="Bank Approvers must verify identity using a 6-digit OTP code."
            )
        quotation_rate_limiter.reset_key(rate_key)

    elif req.email and req.otp_code:
        clean_email = req.email.strip().lower()
        clean_otp = req.otp_code.strip()

        # Find the latest unexpired, unused OTP record for this assignment and email
        otp_record = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.email == clean_email,
            QuotationAccessOTP.expires_at > now,
            QuotationAccessOTP.is_used == False
        ).order_by(QuotationAccessOTP.created_at.desc()).first()

        if not otp_record:
            raise HTTPException(status_code=400, detail="Invalid or expired verification code. Please request a new code.")

        # Check if already locked (3 failed attempts reached)
        if otp_record.failed_attempts is not None and otp_record.failed_attempts >= MAX_OTP_FAILED_ATTEMPTS:
            otp_record.is_used = True
            db.commit()
            raise HTTPException(
                status_code=400,
                detail="This verification code was locked due to too many failed attempts. Please request a new code."
            )

        # Constant-time cryptographic verification
        if not verify_otp_code(clean_otp, otp_record.otp_code):
            otp_record.failed_attempts = (otp_record.failed_attempts or 0) + 1
            remaining = MAX_OTP_FAILED_ATTEMPTS - otp_record.failed_attempts
            if remaining <= 0:
                otp_record.is_used = True
                db.commit()
                raise HTTPException(
                    status_code=400,
                    detail="Too many failed attempts. This code is no longer valid. Please request a new verification code."
                )
            else:
                db.commit()
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid verification code. {remaining} attempt{'s' if remaining > 1 else ''} remaining."
                )

        # Successful verification: reset rate limiting bucket for this key
        quotation_rate_limiter.reset_key(rate_key)

    else:
        raise HTTPException(status_code=400, detail="Must provide either magic_token or email + otp_code")

    # Check if bank approval is required for this quotation
    leg_cfgs = getattr(assignment, "leg_configs", []) or []
    if leg_cfgs:
        has_exec_leg = any((c.quotation_base or "").lower() == "execution" for c in leg_cfgs)
    else:
        has_exec_leg = (getattr(assignment, "quotation_base", "") or getattr(assignment.rfq, "quotation_base", "") if assignment.rfq else "Execution").lower() in ("execution", "mixed")
    if getattr(assignment, "is_cross_entity", False):
        has_exec_leg = False
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    contacts = _get_bank_contacts_list(q_bank) if q_bank else []
    has_approver = any(c.get("role") == "APPROVER" for c in contacts)
    has_execution = any(c.get("role") == "EXECUTION" for c in contacts)
    requires_approval = has_exec_leg and has_approver and has_execution

    # If approval is not required, heal any stale PENDING status
    if not requires_approval and assignment.approval_status == 'PENDING':
        assignment.approval_status = None
        db.commit()

    # Non-approvers cannot authenticate while approval is still pending, expired, or declined
    if otp_record.role in ("EXECUTION", "VIEW_ONLY") and requires_approval:
        if assignment.approval_status == 'PENDING':
            raise HTTPException(status_code=403, detail="This quotation is awaiting internal bank approval from your authorized approver.")
        elif assignment.approval_status == 'EXPIRED':
            raise HTTPException(status_code=403, detail="Quotation window closed before your bank approved participation.")
        elif assignment.approval_status == 'DECLINED':
            raise HTTPException(status_code=403, detail="Your bank's approver declined participation in this quotation.")

    otp_record.is_used = True
    db.commit()

    rfq = assignment.rfq or db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    w_start = rfq.window_start if rfq else None
    w_end = rfq.window_end if rfq else None
    if w_start and w_start.tzinfo is None:
        w_start = w_start.replace(tzinfo=timezone.utc)
    if w_end and w_end.tzinfo is None:
        w_end = w_end.replace(tzinfo=timezone.utc)
    
    w_status = "OPEN"
    if w_end and now > w_end:
        w_status = "CLOSED"
    elif w_start and now < w_start:
        w_status = "PRE_WINDOW"

    client_ip = request.client.host if request and request.client else None
    if rfq:
        bank_name = q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank"
        log_action(
            db=db,
            user_id=None,
            action_type="QUOTATION_PORTAL_AUTHENTICATED",
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": str(rfq.id),
                "bank_name": bank_name,
                "dealer_email": otp_record.email,
                "dealer_role": otp_record.role,
                "auth_method": "6_DIGIT_OTP" if req.otp_code else "1_CLICK_MAGIC_LINK",
                "action_description": "Bank dealer successfully authenticated and entered the quotation portal.",
                "ref_no": rfq.ref_no,
                "authenticated_at": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "window_status": w_status
            },
            customer_id=rfq.customer_id,
            ip_address=client_ip
        )
        db.commit()

    contacts = _get_bank_contacts_list(q_bank) if q_bank else []
    matched = next((c for c in contacts if c.get("email", "").strip().lower() == otp_record.email.lower()), None)
    contact_name = matched.get("name") if matched else otp_record.email.split("@")[0]

    return {
        "authenticated": True,
        "email": otp_record.email,
        "role": otp_record.role,
        "name": contact_name,
        "session_token": otp_record.magic_token
    }

# --- Multi-Dealer Concurrency & Active Trader Desk Session Endpoints ---
@router.get("/{token}/desk-session")
def get_desk_session(
    token: str,
    email: str = None,
    name: str = None,
    db: Session = Depends(get_db)
):
    """Retrieves current desk lock state, active controller, and online colleagues."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    if not email:
        desk = desk_session_service._get_or_create(assignment.id)
        return desk_session_service._build_status(desk, caller_email="", is_active=False)

    return desk_session_service.get_or_claim_desk(
        assignment_id=assignment.id,
        email=email,
        name=name
    )

@router.post("/{token}/desk-heartbeat")
def desk_heartbeat(
    token: str,
    payload: DeskSessionActionRequest,
    db: Session = Depends(get_db)
):
    """Refreshes active dealer presence and returns latest desk state."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    # Authoritatively resolve user role from database session if session_token provided
    resolved_role = (payload.role or "EXECUTION").strip().upper()
    if payload.session_token:
        clean_magic = payload.session_token.split('_tab_')[0] if '_tab_' in payload.session_token else payload.session_token
        otp_rec = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == clean_magic
        ).first()
        if otp_rec and otp_rec.role:
            resolved_role = otp_rec.role.strip().upper()

    rfq = assignment.rfq
    if rfq and rfq.status == 'CANCELLED':
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="This quotation request was officially withdrawn by the corporate treasury desk. No quotation is required."
        )

    # Check if quotation bidding window or corporate acceptance period is currently active
    now = datetime.now(timezone.utc)
    def _to_utc_dt(dt):
        if not dt:
            return None
        if isinstance(dt, str):
            try:
                from dateutil import parser
                dt = parser.parse(dt)
            except Exception:
                return None
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

    w_start = _to_utc_dt(rfq.window_start) if rfq else None
    w_end = _to_utc_dt(rfq.window_end) if rfq else None
    
    # Retrieve customer-configured corporate acceptance timeout (default: 120s for TBILL, 30s for FX_SPOT)
    acceptance_timeout_seconds = 120 if (rfq and rfq.type == "TBILL") else 30
    if rfq and rfq.customer_id:
        try:
            from app.crud.crud_config import crud_customer_configuration
            from app.constants import GlobalConfigKey
            cfg_key = GlobalConfigKey.QUOTATION_ACCEPTANCE_TIMEOUT_TBILL if rfq.type == "TBILL" else GlobalConfigKey.QUOTATION_ACCEPTANCE_TIMEOUT_FX_SPOT
            cfg = crud_customer_configuration.get_customer_config_or_global_fallback(db, rfq.customer_id, cfg_key)
            if cfg and cfg.get("effective_value"):
                acceptance_timeout_seconds = int(cfg["effective_value"])
        except Exception:
            pass

    # Desk session coordination allowed from 10 minutes before window_start
    # until (Corporate Acceptance Timeout + 1 minute buffer) after window_end,
    # as counterparties must remain connected at their desk during the corporate acceptance period.
    from datetime import timedelta
    post_buffer = timedelta(seconds=acceptance_timeout_seconds + 60)
    is_session_active = bool(
        w_start and w_end and 
        (w_start - timedelta(minutes=10)) <= now <= (w_end + post_buffer) and 
        rfq.status not in ('CANCELLED', 'REJECTED')
    )

    if not is_session_active:
        return {
            "active_trader_email": None,
            "active_trader_name": None,
            "is_active_trader": False,
            "can_takeover": False,
            "spectators": [],
            "rfq_status": rfq.status if rfq else "UNKNOWN",
            "is_window_open": False
        }

    # Determine if approver is authorized to quote (only when bank has no execution dealers)
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    contacts = _get_bank_contacts_list(q_bank) if q_bank else []
    has_execution_dealers = any(c.get("role") == "EXECUTION" for c in contacts)
    can_approver_execute = not has_execution_dealers

    desk_role = "EXECUTION" if (resolved_role == "EXECUTION" or (resolved_role == "APPROVER" and can_approver_execute)) else resolved_role

    res = desk_session_service.heartbeat(
        assignment_id=assignment.id,
        email=payload.email,
        name=payload.name,
        role=desk_role,
        session_token=payload.session_token
    )
    if isinstance(res, dict) and rfq:
        res["rfq_status"] = rfq.status
    return res

@router.post("/{token}/desk-takeover")
def desk_takeover(
    token: str,
    payload: DeskSessionActionRequest,
    db: Session = Depends(get_db)
):
    """Transfers active quoting control to the requesting dealer with audit trail."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = assignment.rfq
    if rfq and rfq.status == 'CANCELLED':
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="This quotation request was officially withdrawn by the corporate treasury desk. No quotation is required."
        )

    # Authoritatively resolve user role
    resolved_role = (payload.role or "EXECUTION").strip().upper()
    if payload.session_token:
        clean_magic = payload.session_token.split('_tab_')[0] if '_tab_' in payload.session_token else payload.session_token
        otp_rec = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == clean_magic
        ).first()
        if otp_rec and otp_rec.role:
            resolved_role = otp_rec.role.strip().upper()

    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    contacts = _get_bank_contacts_list(q_bank) if q_bank else []
    has_execution_dealers = any(c.get("role") == "EXECUTION" for c in contacts)
    can_approver_execute = not has_execution_dealers

    is_allowed = (resolved_role == "EXECUTION") or (resolved_role == "APPROVER" and can_approver_execute)
    if not is_allowed:
        raise HTTPException(status_code=403, detail="Only authorized Execution dealers can take over desk quoting control.")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()

    status_res = desk_session_service.takeover_desk(
        assignment_id=assignment.id,
        email=payload.email,
        name=payload.name,
        role="EXECUTION",
        session_token=payload.session_token
    )
    if isinstance(status_res, dict) and rfq:
        status_res["rfq_status"] = rfq.status

    # Audit Log the takeover event
    from app.crud.crud import log_action
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    bank_name = q_bank.bank.name if q_bank and q_bank.bank else "Bank Desk"
    log_action(
        db,
        user_id=None,
        action_type="DESK_CONTROL_TAKEOVER",
        entity_type="QuotationBankAssignment",
        entity_id=None,
        details={
            "assignment_id": str(assignment.id),
            "rfq_ref": rfq.ref_no if rfq else None,
            "bank_name": bank_name,
            "new_active_trader": payload.email,
            "superseded_trader": status_res.get("superseded_trader"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        },
        customer_id=rfq.customer_id if rfq else None
    )
    db.commit()

    return status_res

@router.post("/offer")
def submit_fx_offer(
    offer_in: FXSpotOfferCreate,
    request: Request,
    db: Session = Depends(get_db)
):
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == offer_in.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    if rfq.status in ('PENDING_APPROVAL', 'CANCELLED'):
        raise HTTPException(status_code=403, detail="Quotation is cancelled or not currently open for bidding.")
    if rfq.status in ('COMPLETED', 'REJECTED'):
        raise HTTPException(status_code=403, detail="Quotation is closed and no longer accepting bids.")
    
    # Verify quotation approval status
    if assignment.approval_status == 'PENDING':
        raise HTTPException(status_code=403, detail="Quotation is pending approval from your bank's authorized approver.")
    if assignment.approval_status in ('DECLINED', 'EXPIRED'):
        raise HTTPException(status_code=403, detail="Your bank is not participating in this quotation.")

    # Strict bidding window check (with 3 seconds network latency buffer)
    now = datetime.now(timezone.utc)
    w_start = rfq.window_start if (rfq.window_start and rfq.window_start.tzinfo) else (rfq.window_start.replace(tzinfo=timezone.utc) if rfq.window_start else None)
    w_end = rfq.window_end if (rfq.window_end and rfq.window_end.tzinfo) else (rfq.window_end.replace(tzinfo=timezone.utc) if rfq.window_end else None)

    if w_start and now < (w_start - timedelta(seconds=3)):
        raise HTTPException(status_code=403, detail="Bidding window has not opened yet.")
    if w_end and now > (w_end + timedelta(seconds=3)):
        raise HTTPException(status_code=403, detail="Bidding window is closed.")

    # Verify submitter authorization & role if session provided
    submitted_by = offer_in.email
    if offer_in.session_token:
        clean_token = _clean_magic_token(offer_in.session_token)
        otp_rec = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == clean_token
        ).first()
        if otp_rec:
            if otp_rec.role == "VIEW_ONLY":
                raise HTTPException(status_code=403, detail="View-only contacts are not authorized to submit bids.")
            elif otp_rec.role == "APPROVER":
                contacts = _get_bank_contacts_list(q_bank) if q_bank else []
                has_execution_dealers = any(c.get("role") == "EXECUTION" for c in contacts)
                is_indicative = (assignment.quotation_base or rfq.quotation_base or "").lower() == "indicative"
                if not is_indicative and has_execution_dealers:
                    raise HTTPException(status_code=403, detail="Quotes can only be submitted by authorized Execution dealers.")
            submitted_by = otp_rec.email

    # Verify active trader session lock
    if submitted_by:
        can_submit, block_reason = desk_session_service.can_submit_quote(
            assignment.id, 
            submitted_by, 
            session_token=offer_in.session_token
        )
        if not can_submit:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=block_reason)

    # Determine target leg
    target_leg_id = offer_in.leg_id
    if not target_leg_id and rfq.legs:
        target_leg_id = rfq.legs[0].id

    # Retrieve leg-specific or assignment config for value date
    leg_cfg = assignment.get_config_for_leg(target_leg_id) if target_leg_id else None
    if leg_cfg and leg_cfg.value_date:
        effective_target_value_date = leg_cfg.value_date
    else:
        effective_target_value_date = assignment.value_date or rfq.value_date

    if leg_cfg and leg_cfg.allow_alternative_value_date is not None:
        is_alt_allowed = leg_cfg.allow_alternative_value_date
    elif assignment.allow_alternative_value_date is not None:
        is_alt_allowed = assignment.allow_alternative_value_date
    else:
        is_alt_allowed = rfq.allow_alternative_value_date or False

    def _clean_date_str(d):
        if not d:
            return None
        return str(d).strip().split('T')[0]

    target_date_clean = _clean_date_str(effective_target_value_date)
    proposed_date_clean = _clean_date_str(offer_in.offered_value_date)

    if not is_alt_allowed or not proposed_date_clean:
        # Bank is fixed to target settlement date
        final_offered_value_date = target_date_clean
    else:
        # Bank is permitted to propose an alternative date
        if target_date_clean and proposed_date_clean == target_date_clean:
            final_offered_value_date = target_date_clean
        else:
            # Date cannot be earlier than quotation trade date or submission date
            try:
                p_date = datetime.strptime(proposed_date_clean, "%Y-%m-%d").date()
                today_date = datetime.now(timezone.utc).date()
                w_trade_date = rfq.window_start.date() if (rfq.window_start and hasattr(rfq.window_start, 'date')) else today_date
                min_valid_date = max(today_date, w_trade_date)
                if p_date < min_valid_date:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Proposed value date ({proposed_date_clean}) cannot be earlier than quotation trade date ({min_valid_date})."
                    )
                final_offered_value_date = proposed_date_clean
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid proposed value date format. Expected YYYY-MM-DD.")

    offer = QuotationOffer(
        assignment_id=assignment.id,
        price=offer_in.price,
        offered_value_date=final_offered_value_date,
        notes=offer_in.notes,
        submitted_by_email=submitted_by,
        leg_id=target_leg_id
    )
    db.add(offer)
    db.commit()

    # Notify Creator
    from app.models.models_quotation import QuotationNotification
    by_text = f" by {submitted_by}" if submitted_by else ""
    db.add(QuotationNotification(
        user_id=rfq.created_by_user_id,
        type="NEW_OFFER",
        title=f"New Quote: {rfq.ref_no}",
        message=f"A quote of {offer_in.price:.4f} was submitted{by_text} for your {rfq.type} request.",
        link=f"/end-user/quotations/history?rfq_id={rfq.id}",
        is_read=False
    ))
    db.commit()

    # Calculate live rank if enabled
    live_rank_data = None
    try:
        from app.services.live_ranking_service import live_ranking_service
        q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
        actual_bank_id = q_bank.bank_id if q_bank else None
        rfq_entity_id = getattr(rfq, 'entity_id', None)
        is_enabled = live_ranking_service.evaluate_live_ranking_eligibility(
            db, bank_id=actual_bank_id, customer_id=rfq.customer_id,
            entity_id=rfq_entity_id, trade_type=rfq.type or 'FX_SPOT'
        )
        if is_enabled:
            rank = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment.id, leg_id=target_leg_id)
            all_assignments = db.query(QuotationBankAssignment).filter(
                QuotationBankAssignment.rfq_id == rfq.id
            ).all()
            q_filter = [QuotationOffer.assignment_id.in_([a.id for a in all_assignments])]
            if target_leg_id:
                q_filter.append(QuotationOffer.leg_id == target_leg_id)
            submitted_ids = set(
                o.assignment_id for o in db.query(QuotationOffer).filter(*q_filter).all()
            )
            live_rank_data = {
                "rank": rank,
                "total_quotes": len(submitted_ids),
                "is_leading": rank == 1 if rank else False,
                "leg_id": target_leg_id
            }
    except Exception:
        pass

    ranks_by_leg = {}
    try:
        if is_enabled:
            ranks_by_leg = live_ranking_service.calculate_bank_live_ranks_by_leg(db, rfq.id, assignment.id)
    except Exception:
        pass

    # Clean non-sensitive audit log (Zero-Knowledge: strictly NO prices or bid numbers)
    bank_name = q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank"
    client_ip = request.client.host if request and request.client else None
    log_action(
        db=db,
        user_id=None,
        action_type="QUOTATION_OFFER_SUBMITTED",
        entity_type="QuotationRequest",
        entity_id=None,
        details={
            "rfq_id": str(rfq.id),
            "bank_name": bank_name,
            "dealer_email": submitted_by or "Authorized Bank Dealer",
            "action_description": f"Bank submitted quotation offer for {rfq.type} ({rfq.ref_no}).",
            "ref_no": rfq.ref_no,
            "trade_type": rfq.type,
            "legs_quoted_count": 1,
            "submitted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        },
        customer_id=rfq.customer_id,
        ip_address=client_ip
    )
    db.commit()

    desk_session_service.record_quote_submission(assignment.id, submitted_by or "Dealer", offer_in.price)

    return {"success": True, "submitted_by": submitted_by, "live_rank": live_rank_data, "ranks_by_leg": ranks_by_leg}


@router.post("/offers-batch")
def submit_fx_offers_batch(
    payload: FXSpotMultiOfferCreate,
    request: Request,
    db: Session = Depends(get_db)
):
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == payload.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    if rfq.status in ('PENDING_APPROVAL', 'CANCELLED'):
        raise HTTPException(status_code=403, detail="Quotation is cancelled or not currently open for bidding.")
    if rfq.status in ('COMPLETED', 'REJECTED'):
        raise HTTPException(status_code=403, detail="Quotation is closed and no longer accepting bids.")
    
    if assignment.approval_status == 'PENDING':
        raise HTTPException(status_code=403, detail="Quotation is pending approval from your bank's authorized approver.")
    if assignment.approval_status in ('DECLINED', 'EXPIRED'):
        raise HTTPException(status_code=403, detail="Your bank is not participating in this quotation.")

    # Strict bidding window check (with 3 seconds network latency buffer)
    now = datetime.now(timezone.utc)
    w_start = rfq.window_start if (rfq.window_start and rfq.window_start.tzinfo) else (rfq.window_start.replace(tzinfo=timezone.utc) if rfq.window_start else None)
    w_end = rfq.window_end if (rfq.window_end and rfq.window_end.tzinfo) else (rfq.window_end.replace(tzinfo=timezone.utc) if rfq.window_end else None)

    if w_start and now < (w_start - timedelta(seconds=3)):
        raise HTTPException(status_code=403, detail="Bidding window has not opened yet.")
    if w_end and now > (w_end + timedelta(seconds=3)):
        raise HTTPException(status_code=403, detail="Bidding window is closed.")

    # Authorize submitter
    submitted_by = payload.email
    if payload.session_token:
        clean_token = _clean_magic_token(payload.session_token)
        otp_rec = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == clean_token
        ).first()
        if otp_rec:
            if otp_rec.role == "VIEW_ONLY":
                raise HTTPException(status_code=403, detail="View-only contacts are not authorized to submit bids.")
            elif otp_rec.role == "APPROVER":
                contacts = _get_bank_contacts_list(q_bank) if q_bank else []
                has_execution_dealers = any(c.get("role") == "EXECUTION" for c in contacts)
                if has_execution_dealers:
                    raise HTTPException(status_code=403, detail="Quotes can only be submitted by authorized Execution dealers.")
            submitted_by = otp_rec.email

    if submitted_by:
        can_submit, block_reason = desk_session_service.can_submit_quote(
            assignment.id, 
            submitted_by, 
            session_token=payload.session_token
        )
        if not can_submit:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=block_reason)

    def _clean_date_str(d):
        if not d:
            return None
        return str(d).strip().split('T')[0]

    submitted_offers = []
    for item in payload.quotes:
        leg_cfg = assignment.get_config_for_leg(item.leg_id) if item.leg_id else None
        eff_target_val_date = (leg_cfg.value_date if leg_cfg and leg_cfg.value_date 
                               else (assignment.value_date or rfq.value_date))
        is_alt_allowed = (leg_cfg.allow_alternative_value_date if leg_cfg and leg_cfg.allow_alternative_value_date is not None
                          else (assignment.allow_alternative_value_date if assignment.allow_alternative_value_date is not None 
                                else (rfq.allow_alternative_value_date or False)))

        target_date_clean = _clean_date_str(eff_target_val_date)
        prop_date_clean = _clean_date_str(item.offered_value_date)

        if not is_alt_allowed or not prop_date_clean:
            final_val_date = target_date_clean
        else:
            if target_date_clean and prop_date_clean == target_date_clean:
                final_val_date = target_date_clean
            else:
                try:
                    p_date = datetime.strptime(prop_date_clean, "%Y-%m-%d").date()
                    today_date = datetime.now(timezone.utc).date()
                    w_trade_date = rfq.window_start.date() if (rfq.window_start and hasattr(rfq.window_start, 'date')) else today_date
                    min_valid = max(today_date, w_trade_date)
                    if p_date < min_valid:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Proposed value date ({prop_date_clean}) cannot be earlier than quotation trade date ({min_valid})."
                        )
                    final_val_date = prop_date_clean
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid proposed value date format. Expected YYYY-MM-DD.")

        offer = QuotationOffer(
            assignment_id=assignment.id,
            price=item.price,
            offered_value_date=final_val_date,
            notes=item.notes,
            submitted_by_email=submitted_by,
            leg_id=item.leg_id
        )
        db.add(offer)
        submitted_offers.append(offer)

    db.commit()

    # Notify Creator
    from app.models.models_quotation import QuotationNotification
    by_text = f" by {submitted_by}" if submitted_by else ""
    db.add(QuotationNotification(
        user_id=rfq.created_by_user_id,
        type="NEW_OFFER",
        title=f"New Quotes: {rfq.ref_no}",
        message=f"{len(submitted_offers)} quote(s) were submitted{by_text} for your {rfq.type} request.",
        link=f"/end-user/quotations/history?rfq_id={rfq.id}",
        is_read=False
    ))
    db.commit()

    # Calculate live ranks per leg only if bank is eligible in system-owner live ranking
    ranks_by_leg = {}
    try:
        from app.services.live_ranking_service import live_ranking_service
        q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
        actual_bank_id = q_bank.bank_id if q_bank else None
        rfq_entity_id = getattr(rfq, 'entity_id', None)
        is_live_ranking_enabled = live_ranking_service.evaluate_live_ranking_eligibility(
            db, bank_id=actual_bank_id, customer_id=rfq.customer_id,
            entity_id=rfq_entity_id, trade_type=rfq.type or 'FX_SPOT'
        )
        if is_live_ranking_enabled:
            ranks_by_leg = live_ranking_service.calculate_bank_live_ranks_by_leg(db, rfq.id, assignment.id)
    except Exception:
        pass

    # Clean non-sensitive audit log (Zero-Knowledge: strictly NO prices or bid numbers)
    bank_name = q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank"
    client_ip = request.client.host if request and request.client else None
    log_action(
        db=db,
        user_id=None,
        action_type="QUOTATION_OFFER_SUBMITTED",
        entity_type="QuotationRequest",
        entity_id=None,
        details={
            "rfq_id": str(rfq.id),
            "bank_name": bank_name,
            "dealer_email": submitted_by or "Authorized Bank Dealer",
            "action_description": f"Bank submitted multi-leg quotation package ({len(submitted_offers)} leg(s)) for {rfq.ref_no}.",
            "ref_no": rfq.ref_no,
            "trade_type": rfq.type,
            "legs_quoted_count": len(submitted_offers),
            "submitted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        },
        customer_id=rfq.customer_id,
        ip_address=client_ip
    )
    db.commit()

    if submitted_offers:
        best_price = submitted_offers[0].price
        legs_payload = {
            str(item.leg_id or "default"): {
                "price": item.price,
                "offered_value_date": getattr(item, "offered_value_date", None),
                "notes": getattr(item, "notes", None)
            }
            for item in payload.quotes
        }
        desk_session_service.record_quote_submission(
            assignment.id,
            submitted_by or "Dealer",
            best_price,
            legs_quotes=legs_payload
        )

    return {
        "success": True,
        "submitted_by": submitted_by,
        "quotes_count": len(submitted_offers),
        "ranks_by_leg": ranks_by_leg
    }

@router.post("/tbill-offer")
def submit_tbill_offer(
    offer_in: TBillOfferCreate,
    request: Request,
    db: Session = Depends(get_db)
):
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == offer_in.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    if rfq.status in ('PENDING_APPROVAL', 'CANCELLED'):
        raise HTTPException(status_code=403, detail="Quotation is cancelled or not currently open for bidding.")
    if rfq.status in ('COMPLETED', 'REJECTED'):
        raise HTTPException(status_code=403, detail="Quotation is closed and no longer accepting bids.")
    
    # Verify quotation approval status
    if assignment.approval_status == 'PENDING':
        raise HTTPException(status_code=403, detail="Quotation is pending approval from your bank's authorized approver.")
    if assignment.approval_status in ('DECLINED', 'EXPIRED'):
        raise HTTPException(status_code=403, detail="Your bank is not participating in this quotation.")

    # Strict bidding window check (with 3 seconds network latency buffer)
    now = datetime.now(timezone.utc)
    w_start = rfq.window_start if (rfq.window_start and rfq.window_start.tzinfo) else (rfq.window_start.replace(tzinfo=timezone.utc) if rfq.window_start else None)
    w_end = rfq.window_end if (rfq.window_end and rfq.window_end.tzinfo) else (rfq.window_end.replace(tzinfo=timezone.utc) if rfq.window_end else None)

    if w_start and now < (w_start - timedelta(seconds=3)):
        raise HTTPException(status_code=403, detail="Bidding window has not opened yet.")
    if w_end and now > (w_end + timedelta(seconds=3)):
        raise HTTPException(status_code=403, detail="Bidding window is closed.")
    
    # Verify submitter authorization & role if session provided
    submitted_by = offer_in.email
    if offer_in.session_token:
        clean_token = _clean_magic_token(offer_in.session_token)
        otp_rec = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == clean_token
        ).first()
        if otp_rec:
            if otp_rec.role == "VIEW_ONLY":
                raise HTTPException(status_code=403, detail="View-only contacts are not authorized to submit bids.")
            elif otp_rec.role == "APPROVER":
                contacts = _get_bank_contacts_list(q_bank) if q_bank else []
                has_execution_dealers = any(c.get("role") == "EXECUTION" for c in contacts)
                if has_execution_dealers:
                    raise HTTPException(status_code=403, detail="Quotes can only be submitted by authorized Execution dealers.")
            submitted_by = otp_rec.email

    # Verify active trader session lock
    if submitted_by:
        can_submit, block_reason = desk_session_service.can_submit_quote(
            assignment.id, 
            submitted_by, 
            session_token=offer_in.session_token
        )
        if not can_submit:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=block_reason)

    w_trade_date = rfq.window_start.date() if (rfq.window_start and hasattr(rfq.window_start, 'date')) else datetime.now(timezone.utc).date()
    for line in offer_in.lines:
        if line.settlementDate:
            try:
                s_date = datetime.strptime(str(line.settlementDate).split('T')[0], "%Y-%m-%d").date()
                if s_date < w_trade_date:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Offered settlement date ({s_date}) cannot be earlier than quotation trade date ({w_trade_date})."
                    )
            except ValueError:
                pass

    # Delete existing lines for this exact assignment entirely before repopulating
    db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == assignment.id).delete()
    
    for line in offer_in.lines:
        o = QuotationTBillOffer(
            assignment_id=assignment.id,
            settlement_date=line.settlementDate,
            maturity_date=line.maturityDate,
            discount_rate=line.discountRate,
            max_amount=line.maxAmount,
            notes=line.notes or offer_in.notes,
            submitted_by_email=submitted_by
        )
        db.add(o)
    
    db.commit()

    # Notify Creator
    from app.models.models_quotation import QuotationNotification
    by_text = f" by {submitted_by}" if submitted_by else ""
    db.add(QuotationNotification(
        user_id=rfq.created_by_user_id,
        type="NEW_OFFER",
        title=f"New T-Bill Quote: {rfq.ref_no}",
        message=f"A multi-line T-Bill quote was submitted{by_text} for your request {rfq.ref_no}.",
        link=f"/end-user/quotations/history?rfq_id={rfq.id}",
        is_read=False
    ))
    db.commit()

    # Calculate live rank if enabled
    live_rank_data = None
    try:
        from app.services.live_ranking_service import live_ranking_service
        q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
        actual_bank_id = q_bank.bank_id if q_bank else None
        rfq_entity_id = getattr(rfq, 'entity_id', None)
        is_enabled = live_ranking_service.evaluate_live_ranking_eligibility(
            db, bank_id=actual_bank_id, customer_id=rfq.customer_id,
            entity_id=rfq_entity_id, trade_type=rfq.type or 'TBILL'
        )
        if is_enabled:
            rank = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment.id)
            all_assignments = db.query(QuotationBankAssignment).filter(
                QuotationBankAssignment.rfq_id == rfq.id
            ).all()
            submitted_ids = set(
                o.assignment_id for o in db.query(QuotationTBillOffer).filter(
                    QuotationTBillOffer.assignment_id.in_([a.id for a in all_assignments])
                ).all()
            )
            live_rank_data = {
                "rank": rank,
                "total_quotes": len(submitted_ids),
                "is_leading": rank == 1
            }
    except Exception:
        pass

    # Clean non-sensitive audit log (Zero-Knowledge: strictly NO rates or bid numbers)
    bank_name = q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank"
    client_ip = request.client.host if request and request.client else None
    log_action(
        db=db,
        user_id=None,
        action_type="QUOTATION_OFFER_SUBMITTED",
        entity_type="QuotationRequest",
        entity_id=None,
        details={
            "rfq_id": str(rfq.id),
            "bank_name": bank_name,
            "dealer_email": submitted_by or "Authorized Bank Dealer",
            "action_description": f"Bank submitted T-Bill quotation ({len(offer_in.lines)} line(s)) for {rfq.ref_no}.",
            "ref_no": rfq.ref_no,
            "trade_type": "TBILL",
            "lines_quoted_count": len(offer_in.lines),
            "submitted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        },
        customer_id=rfq.customer_id,
        ip_address=client_ip
    )
    db.commit()

    best_rate = max([line.discountRate for line in offer_in.lines]) if offer_in.lines else 0.0
    desk_session_service.record_quote_submission(assignment.id, submitted_by or "Dealer", best_rate)

    return {"success": True, "submitted_by": submitted_by, "live_rank": live_rank_data}

@router.get("/{token}/live-rank")
def get_live_rank(token: str, leg_id: Optional[str] = None, db: Session = Depends(get_db)):
    """Lightweight polling endpoint: returns the bank's current rank among submitted quotes."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="RFQ not found")

    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()

    # Evaluate eligibility
    from app.services.live_ranking_service import live_ranking_service
    actual_bank_id = q_bank.bank_id if q_bank else None
    rfq_entity_id = getattr(rfq, 'entity_id', None)
    is_enabled = live_ranking_service.evaluate_live_ranking_eligibility(
        db, bank_id=actual_bank_id, customer_id=rfq.customer_id,
        entity_id=rfq_entity_id, trade_type=rfq.type or 'FX_SPOT'
    )

    if not is_enabled:
        return {"is_live_ranking_enabled": False, "rank": None, "ranks_by_leg": {}, "total_quotes": 0}

    rank = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment.id, leg_id=leg_id)
    ranks_by_leg = live_ranking_service.calculate_bank_live_ranks_by_leg(db, rfq.id, assignment.id)

    # Count total submitted banks
    all_assignments = db.query(QuotationBankAssignment).filter(
        QuotationBankAssignment.rfq_id == rfq.id
    ).all()
    if rfq.type == 'TBILL':
        submitted_ids = set(
            o.assignment_id for o in db.query(QuotationTBillOffer).filter(
                QuotationTBillOffer.assignment_id.in_([a.id for a in all_assignments])
            ).all()
        )
    else:
        q_filter = [QuotationOffer.assignment_id.in_([a.id for a in all_assignments])]
        if leg_id:
            q_filter.append(QuotationOffer.leg_id == leg_id)
        submitted_ids = set(
            o.assignment_id for o in db.query(QuotationOffer).filter(*q_filter).all()
        )

    # Check if window is still open
    now = datetime.now(timezone.utc)
    window_end = rfq.window_end
    if window_end and window_end.tzinfo is None:
        window_end = window_end.replace(tzinfo=timezone.utc)
    is_open = False
    if rfq.window_start and window_end:
        ws = rfq.window_start
        if ws.tzinfo is None:
            ws = ws.replace(tzinfo=timezone.utc)
        is_open = (ws <= now <= window_end)

    return {
        "is_live_ranking_enabled": True,
        "rank": rank,
        "ranks_by_leg": ranks_by_leg,
        "leg_id": leg_id,
        "total_quotes": len(submitted_ids),
        "is_leading": rank == 1 if rank else False,
        "isWindowOpen": is_open
    }

@router.post("/{token}/approve")
async def approve_rfq_for_bank(
    token: str,
    action_in: BankApprovalActionCreate,
    background_tasks: BackgroundTasks,
    request: Request,
    db: Session = Depends(get_db)
):
    """Approver authorizes or declines bank participation for this RFQ."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="RFQ not found")

    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()

    # Authenticate session & verify role
    clean_token = _clean_magic_token(action_in.session_token)
    otp_rec = db.query(QuotationAccessOTP).filter(
        QuotationAccessOTP.assignment_id == assignment.id,
        QuotationAccessOTP.magic_token == clean_token
    ).first()
    if not otp_rec:
        raise HTTPException(status_code=401, detail="Invalid or expired session. Please log in again.")
    if otp_rec.role != "APPROVER":
        raise HTTPException(status_code=403, detail="Only authorized Bank Approvers can approve or decline participation.")

    now = datetime.now(timezone.utc)
    window_end = rfq.window_end
    if window_end and window_end.tzinfo is None:
        window_end = window_end.replace(tzinfo=timezone.utc)

    # Check if window already ended
    if window_end and now > window_end:
        if assignment.approval_status == 'PENDING':
            assignment.approval_status = 'EXPIRED'
            db.commit()
        raise HTTPException(
            status_code=403, 
            detail="The quotation window has closed. Your bank has been excluded due to late response."
        )

    if assignment.approval_status == 'EXPIRED':
        raise HTTPException(
            status_code=403, 
            detail="The quotation window has closed. Your bank has been excluded due to late response."
        )

    if assignment.approval_status in ('APPROVED', 'DECLINED'):
        raise HTTPException(
            status_code=400, 
            detail=f"Quotation participation has already been {assignment.approval_status.lower()} by {assignment.approved_by_email or 'another approver'}."
        )

    action = action_in.action.strip().upper()
    if action not in ("APPROVE", "DECLINE"):
        raise HTTPException(status_code=400, detail="Action must be either APPROVE or DECLINE.")

    approver_email = otp_rec.email
    assignment.approved_by_email = approver_email
    assignment.approved_at = now
    assignment.approval_notes = action_in.notes

    bank_name = q_bank.bank.name if q_bank and q_bank.bank else "Bank Partner"
    customer_name = (rfq.entity.entity_name if rfq and rfq.entity else None) or (rfq.customer.name if rfq and rfq.customer else "Treasury Client")
    base_url = get_frontend_base_url(request=request)
    email_settings, source = get_customer_email_settings(db, rfq.customer_id)

    contacts = _get_bank_contacts_list(q_bank) if q_bank else []
    approver_email_set = {approver_email.lower()} if approver_email else set()
    non_approver_emails = list(dict.fromkeys(
        c.get("email", "").strip() for c in contacts 
        if c.get("role") != "APPROVER" and c.get("email") and c.get("email").strip().lower() not in approver_email_set
    ))

    if action == "APPROVE":
        assignment.approval_status = "APPROVED"
        
        # Phase 2: Email EXECUTION + VIEW_ONLY contacts WITH active link (ALL TOGETHER in ONE email, NEVER to APPROVER)
        if non_approver_emails:
            link = f"{base_url}/public-quotation/{assignment.token}"
            from app.services.unified_email_builder import build_quotation_rfq_bank_email
            subject, body = build_quotation_rfq_bank_email(
                rfq=rfq,
                assignment=assignment,
                bank_name=bank_name,
                customer_branding=customer_name,
                link=link,
                email_purpose="APPROVED_BY_BANK"
            )
            background_tasks.add_task(send_email, db, non_approver_emails, subject, body, {}, email_settings)

        # Notify Corporate Admin / Creator
        db.add(QuotationNotification(
            user_id=rfq.created_by_user_id,
            type="BANK_APPROVED",
            title=f"Bank Approved: {bank_name}",
            message=f"{bank_name} approver ({approver_email}) approved participation for {rfq.ref_no} ({customer_name}).",
            link=f"/end-user/quotations/history?rfq_id={rfq.id}",
            is_read=False
        ))

    else:
        # DECLINE
        assignment.approval_status = "DECLINED"
        
        # Phase 2 (declined): Email EXECUTION + VIEW_ONLY contacts
        if non_approver_emails:
            from app.services.unified_email_builder import build_alert_email_html
            subject = f"RFQ {rfq.ref_no} ({customer_name}) - Bank Participation Declined"
            reason_part = f"<br/><br/><strong>Approver Notes / Justification:</strong> {action_in.notes}" if action_in.notes else ""
            msg = (
                f"Your bank's authorized approver (<strong>{approver_email}</strong>) has <strong>declined participation</strong> "
                f"for RFQ <strong>{rfq.ref_no}</strong> on behalf of <strong>{customer_name}</strong>.{reason_part}<br/><br/>"
                f"No further action is required from your execution desk for this quotation request."
            )
            body = build_alert_email_html(
                customer_name=customer_name,
                title=f"Participation Declined &bull; RFQ {rfq.ref_no}",
                alert_type="warning",
                message=msg,
                recipient_name=f"{bank_name} FX &amp; Treasury Desk",
                platform_name="Grow Treasury Platform"
            )
            background_tasks.add_task(send_email, db, non_approver_emails, subject, body, {}, email_settings)

        # Notify Corporate Admin / Creator
        db.add(QuotationNotification(
            user_id=rfq.created_by_user_id,
            type="BANK_DECLINED",
            title=f"Bank Declined: {bank_name}",
            message=f"{bank_name} approver ({approver_email}) declined participation for {rfq.ref_no}." + (f" Notes: {action_in.notes}" if action_in.notes else ""),
            link=f"/end-user/quotations/history?rfq_id={rfq.id}",
            is_read=False
        ))

    db.commit()

    # Clean non-sensitive audit log
    client_ip = request.client.host if request and request.client else None
    action_type = "QUOTATION_BANK_APPROVED" if action == "APPROVE" else "QUOTATION_BANK_DECLINED"
    log_action(
        db=db,
        user_id=None,
        action_type=action_type,
        entity_type="QuotationRequest",
        entity_id=None,
        details={
            "rfq_id": str(rfq.id),
            "bank_name": bank_name,
            "approver_email": approver_email,
            "decision": action,
            "action_description": f"Bank Approver {action.lower()}d participation for {rfq.ref_no}.",
            "ref_no": rfq.ref_no,
            "notes": action_in.notes
        },
        customer_id=rfq.customer_id,
        ip_address=client_ip
    )
    db.commit()

    return {
        "success": True,
        "approval_status": assignment.approval_status,
        "approved_by_email": approver_email,
        "approved_at": assignment.approved_at.isoformat(),
        "notes": action_in.notes
    }

@router.get("/{token}/history")
def get_bank_quotation_history(
    token: str,
    db: Session = Depends(get_db)
):
    """Returns past quotation requests, submitted quotes, and execution outcomes for this bank counterparty."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    if not q_bank:
        raise HTTPException(status_code=404, detail="Bank counterparty configuration not found")

    q_bank_id = q_bank.id
    customer_id = q_bank.customer_id

    # Find all assignments for this specific bank under this customer
    all_assignments = db.query(QuotationBankAssignment).join(
        QuotationRequest, QuotationRequest.id == QuotationBankAssignment.rfq_id
    ).filter(
        QuotationBankAssignment.quotation_bank_id == q_bank_id,
        QuotationRequest.customer_id == customer_id,
        QuotationRequest.status.in_(["OPEN", "PENDING", "COMPLETED", "CANCELLED", "EVALUATING"])
    ).order_by(QuotationRequest.created_at.desc()).limit(100).all()

    history_items = []
    for a in all_assignments:
        rfq = a.rfq
        if not rfq:
            continue

        best_price = None
        submitted_by = None
        submitted_at = None
        notes = None
        offers_info = []

        if rfq.type == "TBILL":
            tb_offers = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == a.id).all()
            if tb_offers:
                best_price = min(o.discount_rate for o in tb_offers)
                submitted_by = tb_offers[0].submitted_by_email
                submitted_at = tb_offers[0].submitted_at
                notes = tb_offers[0].notes
                offers_info = [{
                    "settlement_date": o.settlement_date,
                    "maturity_date": o.maturity_date,
                    "discount_rate": o.discount_rate,
                    "max_amount": o.max_amount,
                    "notes": o.notes
                } for o in tb_offers]
        else:
            fx_offer = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == a.id).order_by(QuotationOffer.submitted_at.desc()).first()
            if fx_offer:
                best_price = fx_offer.price
                submitted_by = fx_offer.submitted_by_email
                submitted_at = fx_offer.submitted_at
                notes = fx_offer.notes

        # Determine trade outcome
        bank_system_id = q_bank.bank_id
        rfq_legs = rfq.legs or []
        won_legs = [l for l in rfq_legs if l.winner_bank_id == bank_system_id and (l.status not in ('REJECTED', 'CANCELLED', 'DECLINED'))]
        is_multi_leg = len(rfq_legs) > 1

        legs_info = []
        if is_multi_leg:
            for l in rfq_legs:
                l_offer = db.query(QuotationOffer).filter(
                    QuotationOffer.assignment_id == a.id,
                    QuotationOffer.leg_id == l.id
                ).order_by(QuotationOffer.submitted_at.desc()).first()

                is_l_won = (l.winner_bank_id == bank_system_id and l.status not in ('REJECTED', 'CANCELLED', 'DECLINED'))
                if is_l_won:
                    l_outcome = "WON"
                elif l.status in ('REJECTED', 'CANCELLED'):
                    l_outcome = "REJECTED"
                elif l.status == 'INCONCLUSIVE':
                    l_outcome = "INCONCLUSIVE"
                elif l_offer and l_offer.price is not None:
                    l_outcome = "NOT_SELECTED" if rfq.status == "COMPLETED" else "SUBMITTED"
                else:
                    l_outcome = "NO_QUOTE"

                legs_info.append({
                    "leg_id": l.id,
                    "leg_index": l.leg_index,
                    "pair": f"{l.buy_currency}/{l.sell_currency}",
                    "direction": l.direction,
                    "amount": l.amount,
                    "buy_currency": l.buy_currency,
                    "sell_currency": l.sell_currency,
                    "value_date": l.value_date,
                    "submitted_price": l_offer.price if l_offer else None,
                    "outcome": l_outcome,
                    "is_winner": is_l_won
                })

        outcome = "NO_QUOTE"
        if a.approval_status == 'DECLINED':
            outcome = "PARTICIPATION_DECLINED"
        elif a.approval_status == 'EXPIRED':
            outcome = "EXPIRED"
        elif rfq.status == "COMPLETED":
            if won_legs:
                if is_multi_leg and len(won_legs) < len(rfq_legs):
                    outcome = "PARTIALLY_WON"
                else:
                    outcome = "WON"
            else:
                # Fallback check for single-ticket or T-Bills without persisted legs
                winner_id = None
                try:
                    from app.api.v1.endpoints.quotations_endpoints import compute_rfq_standings
                    standings = compute_rfq_standings(rfq, db, dispatch_emails=False)
                    winner_id = standings.get("winner_bank_id")
                except Exception:
                    pass

                if winner_id and winner_id == bank_system_id:
                    outcome = "WON"
                elif best_price is not None or any(l.get("submitted_price") is not None for l in legs_info):
                    outcome = "NOT_SELECTED"
                else:
                    outcome = "NO_QUOTE"
        elif rfq.status in ["REJECTED", "CANCELLED"]:
            outcome = rfq.status
        elif rfq.status in ["OPEN", "PENDING", "EVALUATING", "APPROVED_SCHEDULED"]:
            outcome = "SUBMITTED" if (best_price is not None or any(l.get("submitted_price") is not None for l in legs_info)) else "NO_QUOTE"
        else:
            outcome = rfq.status

        display_pair = (
            f"Package ({len(rfq_legs)} Pairs)" if is_multi_leg 
            else (f"{rfq.buy_currency}/{rfq.sell_currency}" if rfq.buy_currency else None)
        )

        history_items.append({
            "rfq_id": rfq.id,
            "ref_no": rfq.ref_no,
            "entity_name": rfq.entity.entity_name if rfq.entity else None,
            "entity_code": rfq.entity.code if rfq.entity else None,
            "type": rfq.type,
            "direction": rfq.direction,
            "amount": rfq.amount,
            "currency_pair": display_pair,
            "value_date": rfq.value_date,
            "window_end": rfq.window_end,
            "status": rfq.status,
            "quotation_base": a.quotation_base or rfq.quotation_base,
            "best_quote": best_price,
            "notes": notes,
            "submitted_by": submitted_by,
            "submitted_at": submitted_at,
            "outcome": outcome,
            "offers_count": len(offers_info) if rfq.type == "TBILL" else (len([l for l in legs_info if l.get('submitted_price') is not None]) if is_multi_leg else (1 if best_price else 0)),
            "created_at": rfq.created_at,
            "is_multi_leg": is_multi_leg,
            "legs_count": len(rfq_legs),
            "won_legs_count": len(won_legs),
            "legs": legs_info
        })

    return {
        "bank_name": q_bank.bank.name if q_bank.bank else "Bank",
        "total_deals": len(history_items),
        "history": history_items
    }

@router.get("/{token}/result")
async def get_public_rfq_result(token: str, db: Session = Depends(get_db)):
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")
        
    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    
    # If the bank declined participation, strictly return PARTICIPATION_DECLINED.
    # Counterparties that declined participation must not know if the deal concluded, executed, or closed without a winner.
    if assignment.approval_status == 'DECLINED':
        return {"status": "PARTICIPATION_DECLINED"}

    # Lazy evaluation in case history hasn't been fetched
    now = datetime.now(timezone.utc)

    def _to_utc_dt(dt):
        if not dt:
            return None
        if isinstance(dt, str):
            try:
                from dateutil import parser
                dt = parser.parse(dt)
            except Exception:
                return None
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

    w_start = _to_utc_dt(rfq.window_start)
    w_end = _to_utc_dt(rfq.window_end)

    # If quotation bidding window has not opened yet, it can NEVER be closed or completed
    if w_start and now < w_start:
        return {"status": "SCHEDULED"}

    # Align with 3-second network latency buffer
    is_closed = bool(w_end and now > (w_end + timedelta(seconds=3)))

    if not is_closed and rfq.status not in ('COMPLETED', 'CANCELLED', 'REJECTED'):
        return {"status": "OPEN" if (w_start and now >= w_start) else "PENDING"}

    # Calculate results using central endpoint evaluation logic
    from app.api.v1.endpoints.quotations_endpoints import get_rfq_results
    try:
        res_data = get_rfq_results(rfq.id, db, current_user=None)
        db.refresh(rfq)

        # Check if ALL legs requested from this counterparty are Indicative (non-binding)
        legs_data = res_data.get("legs", [])
        bank_id = assignment.quotation_bank.bank_id if assignment.quotation_bank else None

        has_any_execution_for_bank = False
        if legs_data:
            for l in legs_data:
                cfg = assignment.get_config_for_leg(l.get("leg_id"))
                assigned_base = (getattr(cfg, 'quotation_base', None) or assignment.quotation_base or l.get('quotation_base') or rfq.quotation_base or 'Execution').lower()
                if assigned_base == 'execution':
                    has_any_execution_for_bank = True
                    break
        else:
            q_base = (assignment.quotation_base or rfq.quotation_base or 'Execution').lower()
            has_any_execution_for_bank = (q_base == 'execution')

        # If everything requested from this counterparty is Indicative:
        # Counterparty should immediately see thank you note and NEVER see "Selection in Progress"!
        if not has_any_execution_for_bank:
            return {
                "status": "INDICATIVE_ONLY",
                "detail": "Indicative pricing received. Thank you for your quote."
            }

        # If acceptance is currently pending corporate decision (ONLY for Execution deals!)
        if getattr(rfq, 'acceptance_status', None) == 'PENDING':
            return {
                "status": "AWAITING_MANUAL_SELECTION",
                "message": "Quotation window closed. Awaiting corporate treasury acceptance.",
                "acceptance_deadline": rfq.acceptance_deadline.isoformat() if getattr(rfq, 'acceptance_deadline', None) else None
            }

        # If deal was rejected or auto-rejected
        if rfq.status == 'REJECTED' or getattr(rfq, 'acceptance_status', None) in ('REJECTED', 'AUTO_REJECTED'):
            return {
                "status": "UNEXECUTED",
                "detail": "This quotation request has concluded and your offer was not selected for trade execution on this occasion."
            }

        bank_id = assignment.quotation_bank.bank_id if assignment.quotation_bank else None
        legs_data = res_data.get("legs", [])

        if legs_data and len(legs_data) > 1:
            won_legs = []
            lost_legs = []
            inconclusive_legs = []
            indicative_legs = []
            legs_breakdown = {}

            for l in legs_data:
                leg_id = l.get("leg_id")
                leg_base = (l.get("quotation_base") or "").lower()
                pair_name = l.get("currency_pair") or f"{l.get('buy_currency')}/{l.get('sell_currency')}"
                l_winner_id = l.get("winner_bank_id")
                l_inconclusive = l.get("is_inconclusive", False)
                l_status = (l.get("status") or "").upper()

                if leg_base == "indicative":
                    indicative_legs.append(l)
                    leg_status = "INDICATIVE"
                    is_leg_win = False
                elif l_status in ("REJECTED", "CANCELLED", "DECLINED"):
                    lost_legs.append(l)
                    leg_status = "NOT_SELECTED"
                    is_leg_win = False
                elif l_inconclusive or not l_winner_id:
                    inconclusive_legs.append(l)
                    leg_status = "INCONCLUSIVE"
                    is_leg_win = False
                elif bank_id and l_winner_id == bank_id:
                    won_legs.append(l)
                    leg_status = "WON"
                    is_leg_win = True
                else:
                    lost_legs.append(l)
                    leg_status = "LOST"
                    is_leg_win = False

                leg_item = {
                    "leg_id": str(leg_id) if leg_id is not None else None,
                    "leg_index": l.get("leg_index"),
                    "pair": pair_name,
                    "currency_pair": pair_name,
                    "quotation_base": l.get("quotation_base", "Execution"),
                    "is_winner": is_leg_win,
                    "won": is_leg_win,
                    "status": "WINNER" if is_leg_win else ("NOT_SELECTED" if leg_status in ("LOST", "NOT_SELECTED") else leg_status),
                    "raw_status": leg_status,
                    "winner_rate": l.get("winner_rate") if is_leg_win else None
                }
                legs_breakdown[leg_id] = leg_item
                if leg_id is not None:
                    legs_breakdown[str(leg_id)] = leg_item
                if l.get("leg_index") is not None:
                    legs_breakdown[l.get("leg_index") - 1] = leg_item
                    legs_breakdown[str(l.get("leg_index") - 1)] = leg_item
                legs_breakdown[pair_name] = leg_item

            if len(won_legs) == len(legs_data):
                overall_status = "WINNER"
            elif len(won_legs) > 0:
                overall_status = "PARTIALLY_WON"
            elif len(won_legs) == 0 and len(lost_legs) > 0:
                overall_status = "NOT_SELECTED"
            elif len(indicative_legs) == len(legs_data):
                overall_status = "INDICATIVE_ONLY"
            else:
                overall_status = "INCONCLUSIVE"

            receipt = None
            if won_legs and assignment.quotation_bank and bank_id:
                try:
                    customer_name = (rfq.entity.entity_name if rfq.entity else None) or (rfq.customer.name if rfq.customer else "Treasury Customer")
                    bank_display = assignment.quotation_bank.bank.name if assignment.quotation_bank.bank else "Bank Partner"
                    exec_legs_data = []
                    for wl in won_legs:
                        exec_legs_data.append({
                            "leg_id": str(wl.get("leg_id", "")),
                            "pair": wl.get("currency_pair") or f"{wl.get('buy_currency')}/{wl.get('sell_currency')}",
                            "direction": wl.get("direction", "BUY"),
                            "amount": float(wl.get("amount", 0)),
                            "currency": wl.get("buy_currency", ""),
                            "rate": float(wl.get("winner_rate") or 0.0),
                            "value_date": str(wl.get("value_date") or 'Standard Spot')
                        })
                    exec_time = rfq.updated_at.strftime("%Y-%m-%d %H:%M:%S UTC") if rfq.updated_at else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    from app.core.otp_security import generate_scoped_deal_receipt
                    receipt = generate_scoped_deal_receipt(
                        rfq_id=str(rfq.id),
                        ref_no=rfq.ref_no,
                        customer_name=customer_name,
                        bank_id=bank_id,
                        bank_name=bank_display,
                        executed_legs=exec_legs_data,
                        executed_at=exec_time
                    )
                except Exception:
                    receipt = None

            released_docs = []
            if won_legs:
                seen_paths = set()
                from app.core.ai_integration import generate_signed_gcs_url
                for wl in won_legs:
                    wl_idx = wl.get("leg_index")
                    wl_id = str(wl.get("leg_id") or "")
                    leg_docs = rfq.get_documents_for_leg(leg_index=wl_idx, leg_id=wl_id)
                    for d in leg_docs:
                        p_val = d.get("path")
                        if p_val and p_val not in seen_paths:
                            seen_paths.add(p_val)
                            p_signed = p_val
                            if str(p_val).startswith("gs://"):
                                try:
                                    signed = await generate_signed_gcs_url(p_val, expiration=604800)
                                    p_signed = signed or p_val
                                except Exception:
                                    pass
                            released_docs.append({
                                "name": d.get("name") or "Document",
                                "path": p_signed,
                                "pair": d.get("pair") or wl.get("currency_pair")
                            })

            return {
                "status": overall_status,
                "won_legs_count": len(won_legs),
                "lost_legs_count": len(lost_legs),
                "inconclusive_legs_count": len(inconclusive_legs),
                "total_legs_count": len(legs_data),
                "won_pairs": [l.get("currency_pair") or f"{l.get('buy_currency')}/{l.get('sell_currency')}" for l in won_legs],
                "lost_pairs": [l.get("currency_pair") or f"{l.get('buy_currency')}/{l.get('sell_currency')}" for l in lost_legs],
                "inconclusive_pairs": [l.get("currency_pair") or f"{l.get('buy_currency')}/{l.get('sell_currency')}" for l in inconclusive_legs],
                "legs_breakdown": legs_breakdown,
                "receipt": receipt,
                "released_documents": released_docs
            }

        # Single-leg or master RFQ evaluation
        q_base = (assignment.quotation_base or rfq.quotation_base or 'Execution').lower()
        if q_base == 'indicative':
            return {"status": "INDICATIVE_ONLY", "released_documents": []}

        winner_bank_id = res_data.get("winner_bank_id")
        is_inconclusive = res_data.get("is_inconclusive", False)
        rfq_res_status = (res_data.get("status") or (rfq.status if rfq else "")).upper()

        if rfq_res_status in ("REJECTED", "CANCELLED", "DECLINED"):
            return {"status": "NOT_SELECTED", "released_documents": []}

        if is_inconclusive or not winner_bank_id:
            return {"status": "INCONCLUSIVE", "released_documents": []}

        if assignment.quotation_bank and assignment.quotation_bank.bank_id == winner_bank_id:
            receipt = None
            try:
                customer_name = (rfq.entity.entity_name if rfq.entity else None) or (rfq.customer.name if rfq.customer else "Treasury Customer")
                bank_display = assignment.quotation_bank.bank.name if assignment.quotation_bank.bank else "Bank Partner"
                single_leg_data = [{
                    "pair": f"{rfq.buy_currency}/{rfq.sell_currency}",
                    "direction": rfq.direction,
                    "amount": float(rfq.amount),
                    "currency": rfq.buy_currency,
                    "rate": float(res_data.get("winner_rate") or 0.0),
                    "value_date": str(rfq.value_date or 'Standard Spot')
                }]
                exec_time = rfq.updated_at.strftime("%Y-%m-%d %H:%M:%S UTC") if rfq.updated_at else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                from app.core.otp_security import generate_scoped_deal_receipt
                receipt = generate_scoped_deal_receipt(
                    rfq_id=str(rfq.id),
                    ref_no=rfq.ref_no,
                    customer_name=customer_name,
                    bank_id=assignment.quotation_bank.bank_id,
                    bank_name=bank_display,
                    executed_legs=single_leg_data,
                    executed_at=exec_time
                )
            except Exception:
                receipt = None

            released_docs = []
            all_docs = rfq.get_parsed_documents()
            from app.core.ai_integration import generate_signed_gcs_url
            for d in all_docs:
                p_val = d.get("path")
                p_signed = p_val
                if str(p_val).startswith("gs://"):
                    try:
                        signed = await generate_signed_gcs_url(p_val, expiration=604800)
                        p_signed = signed or p_val
                    except Exception:
                        pass
                released_docs.append({
                    "name": d.get("name") or "Document",
                    "path": p_signed,
                    "pair": d.get("pair") or f"{rfq.buy_currency}/{rfq.sell_currency}"
                })

            return {"status": "WINNER", "receipt": receipt, "released_documents": released_docs}
        else:
            return {"status": "NOT_SELECTED", "released_documents": []}
    except Exception:
        return {"status": "COMPLETED", "released_documents": []}


@router.get("/bank-handshake/{token}")
def get_bank_handshake_details(token: str, db: Session = Depends(get_db)):
    """Validates the bank handshake token and returns bank info and contacts for verification."""
    from app.core.security import SECRET_KEY, ALGORITHM
    from jose import jwt
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("sub") != "bank_handshake":
            raise ValueError("Invalid token type")
    except Exception:
        raise HTTPException(status_code=400, detail="The handshake verification link is invalid or has expired.")

    bank_config_id = payload.get("bank_config_id")
    customer_id = payload.get("customer_id")
    
    qb = db.query(QuotationBank).filter(
        QuotationBank.id == bank_config_id,
        QuotationBank.customer_id == customer_id
    ).first()

    if not qb:
        raise HTTPException(status_code=404, detail="Bank counterparty configuration not found.")

    from app.models.models import Customer
    cust = db.query(Customer).filter(Customer.id == customer_id).first()
    customer_name = cust.name if cust else "Corporate Treasury"
    bank_name = qb.bank.name if qb.bank else f"Bank {qb.bank_id}"

    return {
        "status": "success",
        "bank_name": bank_name,
        "customer_name": customer_name,
        "authorized_contact_email": qb.authorized_contact_email,
        "authorized_contact_name": qb.authorized_contact_name,
        "contacts": list(qb.contacts) if qb.contacts else [],
        "handshake_confirmed_at": qb.handshake_confirmed_at.isoformat() if qb.handshake_confirmed_at else None,
        "already_confirmed": bool(qb.handshake_confirmed_at)
    }


@router.post("/bank-handshake/{token}/confirm")
def confirm_bank_handshake(token: str, request: Request, db: Session = Depends(get_db)):
    """Confirms the counterparty governance handshake by the bank officer."""
    from app.core.security import SECRET_KEY, ALGORITHM
    from jose import jwt
    from datetime import datetime, timezone
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("sub") != "bank_handshake":
            raise ValueError("Invalid token type")
    except Exception:
        raise HTTPException(status_code=400, detail="The handshake verification link is invalid or has expired.")

    bank_config_id = payload.get("bank_config_id")
    customer_id = payload.get("customer_id")

    qb = db.query(QuotationBank).filter(
        QuotationBank.id == bank_config_id,
        QuotationBank.customer_id == customer_id
    ).first()

    if not qb:
        raise HTTPException(status_code=404, detail="Bank counterparty configuration not found.")

    now_utc = datetime.now(timezone.utc)
    qb.handshake_confirmed_at = now_utc
    db.commit()

    # Log audit action
    client_ip = request.client.host if request and request.client else "unknown"
    log_action(
        db,
        user_id=None,
        action_type="QUOTATION_BANK_HANDSHAKE_CONFIRMED",
        entity_type="QuotationBank",
        entity_id=qb.id,
        details={
            "bank_id": qb.bank_id,
            "bank_name": qb.bank.name if qb.bank else f"Bank {qb.bank_id}",
            "confirmed_by_email": payload.get("email"),
            "confirmed_at": now_utc.isoformat(),
            "ip_address": client_ip
        },
        customer_id=customer_id
    )

    return {
        "status": "success",
        "message": f"Counterparty trading roster confirmed successfully for {qb.bank.name if qb.bank else 'your bank'}.",
        "confirmed_at": now_utc.isoformat()
    }


# --- Public Dealer Handshake & Self-Activation Endpoints ---

@router.get("/dealer-handshake/{token}")
def get_dealer_handshake_details(token: str, db: Session = Depends(get_db)):
    """Validates the dealer invitation token and returns invitation details for verification."""
    inv = db.query(QuotationBankContactInvitation).filter(
        QuotationBankContactInvitation.token == token
    ).first()

    if not inv:
        raise HTTPException(status_code=404, detail="The invitation link is invalid or has expired.")

    from app.models.models import Customer, Bank
    cust = db.query(Customer).filter(Customer.id == inv.customer_id).first()
    bank = db.query(Bank).filter(Bank.id == inv.bank_id).first()

    now_utc = datetime.now(timezone.utc)
    is_expired = inv.expires_at < now_utc if inv.expires_at else False

    return {
        "status": inv.status,
        "is_expired": is_expired,
        "email": inv.email,
        "title": inv.title,
        "role": inv.role,
        "bank_name": bank.name if bank else f"Bank {inv.bank_id}",
        "customer_name": cust.name if cust else "Corporate Treasury",
        "created_at": inv.created_at.isoformat() if inv.created_at else None,
        "accepted_at": inv.accepted_at.isoformat() if inv.accepted_at else None,
        "already_accepted": (inv.status == "ACCEPTED")
    }


@router.post("/dealer-handshake/{token}/accept")
def accept_dealer_handshake(token: str, request: Request, db: Session = Depends(get_db)):
    """Accepts the dealer invitation and promotes the contact into quotation_banks.contacts."""
    inv = db.query(QuotationBankContactInvitation).filter(
        QuotationBankContactInvitation.token == token
    ).first()

    if not inv:
        raise HTTPException(status_code=404, detail="The invitation link is invalid or has expired.")

    if inv.status == "ACCEPTED":
        return {
            "status": "already_accepted",
            "message": "This invitation has already been accepted and your trading access is active.",
            "accepted_at": inv.accepted_at.isoformat() if inv.accepted_at else None
        }

    if inv.status == "REVOKED":
        raise HTTPException(status_code=400, detail="This invitation has been revoked by the corporate treasury administrator.")

    now_utc = datetime.now(timezone.utc)
    if inv.expires_at and inv.expires_at < now_utc:
        raise HTTPException(status_code=400, detail="This invitation has expired. Please request a new invitation link from your corporate treasury administrator.")

    # Promote contact to QuotationBank
    qb = db.query(QuotationBank).filter(
        QuotationBank.customer_id == inv.customer_id,
        QuotationBank.bank_id == inv.bank_id
    ).first()

    from app.models.models import Bank, Customer
    bank = db.query(Bank).filter(Bank.id == inv.bank_id).first()
    cust = db.query(Customer).filter(Customer.id == inv.customer_id).first()

    new_contact_entry = {
        "email": inv.email.strip().lower(),
        "title": inv.title or "",
        "name": inv.title or "",
        "role": (inv.role or "EXECUTION").upper()
    }

    if qb:
        existing_contacts = list(qb.contacts) if qb.contacts else []
        idx = next((i for i, c in enumerate(existing_contacts) if c.get("email", "").strip().lower() == new_contact_entry["email"]), -1)
        if idx >= 0:
            existing_contacts[idx] = new_contact_entry
        else:
            existing_contacts.append(new_contact_entry)

        qb.contacts = existing_contacts
        qb.emails = ",".join(c.get("email", "").strip() for c in existing_contacts if c.get("email"))
    else:
        qb = QuotationBank(
            customer_id=inv.customer_id,
            bank_id=inv.bank_id,
            trade_type="BOTH",
            entity_scope="ALL_ENTITIES",
            emails=new_contact_entry["email"],
            contacts=[new_contact_entry]
        )
        db.add(qb)
        db.flush()

    inv.status = "ACCEPTED"
    inv.accepted_at = now_utc
    inv.quotation_bank_id = qb.id

    db.commit()

    client_ip = request.client.host if request and request.client else "unknown"
    log_action(
        db,
        user_id=None,
        action_type="QUOTATION_BANK_DEALER_HANDSHAKE_ACCEPTED",
        entity_type="QuotationBankContactInvitation",
        entity_id=inv.id,
        details={
            "bank_id": inv.bank_id,
            "bank_name": bank.name if bank else f"Bank {inv.bank_id}",
            "dealer_email": inv.email,
            "role": inv.role,
            "ip_address": client_ip
        },
        customer_id=inv.customer_id
    )

    return {
        "status": "success",
        "message": f"Welcome aboard! Your trading access for {bank.name if bank else 'your bank'} is now active.",
        "accepted_at": now_utc.isoformat(),
        "bank_name": bank.name if bank else "",
        "customer_name": cust.name if cust else ""
    }
