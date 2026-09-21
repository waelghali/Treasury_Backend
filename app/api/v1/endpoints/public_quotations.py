# app/api/v1/endpoints/public_quotations.py
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Request
from sqlalchemy.orm import Session
from datetime import datetime, timezone, timedelta
import os
import secrets
import uuid

from app.database import get_db
from app.models.models_quotation import (
    QuotationBankAssignment, QuotationRequest, QuotationOffer, 
    QuotationTBillOffer, QuotationBank, QuotationAccessOTP, QuotationAnalytics,
    QuotationNotification
)
from app.schemas.schemas_quotation import (
    FXSpotOfferCreate, TBillOfferCreate, OTPRequestCreate, OTPVerifyCreate,
    BankApprovalActionCreate, DeskSessionActionRequest
)
from app.services.desk_session_service import desk_session_service
from app.core.email_service import send_email, get_customer_email_settings, get_global_email_settings
from app.core.routing import get_frontend_base_url

router = APIRouter()

def _get_bank_contacts_list(q_bank: QuotationBank):
    """Helper to extract normalized contacts list with roles."""
    if q_bank.contacts and isinstance(q_bank.contacts, list) and len(q_bank.contacts) > 0:
        return q_bank.contacts
    emails_list = [e.strip() for e in (q_bank.emails or "").split(",") if e.strip()]
    return [{"email": e, "name": "", "role": "EXECUTION"} for e in emails_list]

@router.get("/{token}")
async def get_rfq_by_token(token: str, db: Session = Depends(get_db)):
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
    # Indicative RFQs never share documents, and document visibility flag is respected
    if effective_base != 'indicative' and (assignment.is_document_visible is not False) and rfq.document_path:
        raw_docs = []
        try:
            import json
            loaded = json.loads(rfq.document_path)
            if isinstance(loaded, list):
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
        "quotation_base": assignment.quotation_base or rfq.quotation_base,
        "document_path": rfq.document_path if (effective_base != 'indicative' and assignment.is_document_visible is not False) else None,
        "documents": parsed_docs,
        "status": rfq.status,
        "assignment_id": assignment.id,
        "bank_name": bank_name,
        "customer_name": customer_name,
        "entity_name": rfq.entity.entity_name if rfq.entity else customer_name,
        "entity_tax_id": rfq.entity.tax_id if rfq.entity else None,
        "entity_cr_number": rfq.entity.commercial_register_number if rfq.entity else None,
        "entity_code": rfq.entity.code if rfq.entity else None,
        "serverTime": now.isoformat(),
        "isWindowOpen": is_open,
        "offers": offers,
        "approval_status": assignment.approval_status,
        "approved_by_email": assignment.approved_by_email,
        "approved_at": assignment.approved_at.isoformat() if assignment.approved_at else None,
        "approval_notes": assignment.approval_notes,
        "has_execution_dealers": any(c.get("role") == "EXECUTION" for c in (_get_bank_contacts_list(q_bank) if q_bank else [])),
        "total_execution_dealers": sum(1 for c in (_get_bank_contacts_list(q_bank) if q_bank else []) if c.get("role") == "EXECUTION"),
        "requires_bank_approval": (effective_base != "indicative") and any(c.get("role") == "EXECUTION" for c in (_get_bank_contacts_list(q_bank) if q_bank else [])) and any(c.get("role") == "APPROVER" for c in (_get_bank_contacts_list(q_bank) if q_bank else [])),
        "cbe_benchmark_rate": cbe_benchmark_rate,
        "is_live_ranking_enabled": is_live_ranking_enabled,
        "live_rank": live_rank,
        "total_quotes": total_quotes
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
    effective_base = (getattr(assignment, "quotation_base", "") or getattr(rfq, "quotation_base", "") or "Execution").lower()
    is_indicative = effective_base == "indicative"
    has_approver = any(c.get("role") == "APPROVER" for c in contacts)
    has_execution = any(c.get("role") == "EXECUTION" for c in contacts)
    requires_approval = (not is_indicative) and has_approver and has_execution

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

    # Generate 6-digit OTP & Magic Token
    otp_code = f"{secrets.randbelow(900000) + 100000}"
    magic_token = uuid.uuid4().hex
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

    otp_record = QuotationAccessOTP(
        assignment_id=assignment.id,
        email=target_email,
        role=role,
        otp_code=otp_code,
        magic_token=magic_token,
        expires_at=expires_at,
        is_used=False
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

    return {
        "message": f"Verification code sent to {target_email}",
        "email": target_email,
        "role": role,
        "name": contact_name
    }

@router.post("/verify-otp")
def verify_quotation_otp(
    req: OTPVerifyCreate,
    db: Session = Depends(get_db)
):
    """Verifies 6-digit OTP code or Magic Link token and establishes authenticated portal session."""
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == req.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    now = datetime.now(timezone.utc)

    query = db.query(QuotationAccessOTP).filter(
        QuotationAccessOTP.assignment_id == assignment.id,
        QuotationAccessOTP.expires_at > now
    )

    if req.magic_token:
        query = query.filter(QuotationAccessOTP.magic_token == req.magic_token)
    elif req.email and req.otp_code:
        query = query.filter(
            QuotationAccessOTP.email == req.email.strip().lower(),
            QuotationAccessOTP.otp_code == req.otp_code.strip()
        )
    else:
        raise HTTPException(status_code=400, detail="Must provide either magic_token or email + otp_code")

    otp_record = query.order_by(QuotationAccessOTP.created_at.desc()).first()
    if not otp_record:
        raise HTTPException(status_code=400, detail="Invalid or expired verification code.")

    # Bank Approvers MUST verify with 6-digit OTP code - magic token bypass is strictly forbidden
    if otp_record.role == "APPROVER" and not req.otp_code:
        raise HTTPException(
            status_code=400, 
            detail="Bank Approvers must verify identity using a 6-digit OTP code."
        )

    # Check if bank approval is required for this quotation
    effective_base = (getattr(assignment, "quotation_base", "") or getattr(assignment.rfq, "quotation_base", "") if assignment.rfq else "Execution").lower()
    is_indicative = effective_base == "indicative"
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    contacts = _get_bank_contacts_list(q_bank) if q_bank else []
    has_approver = any(c.get("role") == "APPROVER" for c in contacts)
    has_execution = any(c.get("role") == "EXECUTION" for c in contacts)
    requires_approval = (not is_indicative) and has_approver and has_execution

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

    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
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
        otp_rec = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == payload.session_token
        ).first()
        if otp_rec and otp_rec.role:
            resolved_role = otp_rec.role.strip().upper()

    rfq = assignment.rfq
    if rfq and rfq.status == 'CANCELLED':
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="This quotation request was officially withdrawn by the corporate treasury desk. No quotation is required."
        )

    # Check if quotation bidding window is currently active
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
    
    # Desk session coordination allowed from 10 minutes before window_start until 1 minute after window_end
    from datetime import timedelta
    is_session_active = bool(
        w_start and w_end and 
        (w_start - timedelta(minutes=10)) <= now <= (w_end + timedelta(minutes=1)) and 
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

    # Determine if approver is authorized to quote (indicative or solo bank contact)
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    contacts = _get_bank_contacts_list(q_bank) if q_bank else []
    has_execution_dealers = any(c.get("role") == "EXECUTION" for c in contacts)
    effective_base = (getattr(assignment, "quotation_base", "") or getattr(rfq, "quotation_base", "") if rfq else "Execution").lower()
    is_indicative = effective_base == "indicative"
    can_approver_execute = is_indicative or not has_execution_dealers

    desk_role = "EXECUTION" if (resolved_role == "EXECUTION" or (resolved_role == "APPROVER" and can_approver_execute)) else resolved_role

    res = desk_session_service.heartbeat(
        assignment_id=assignment.id,
        email=payload.email,
        name=payload.name,
        role=desk_role
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
        otp_rec = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == payload.session_token
        ).first()
        if otp_rec and otp_rec.role:
            resolved_role = otp_rec.role.strip().upper()

    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    contacts = _get_bank_contacts_list(q_bank) if q_bank else []
    has_execution_dealers = any(c.get("role") == "EXECUTION" for c in contacts)
    effective_base = (getattr(assignment, "quotation_base", "") or getattr(rfq, "quotation_base", "") if rfq else "Execution").lower()
    is_indicative = effective_base == "indicative"
    can_approver_execute = is_indicative or not has_execution_dealers

    is_allowed = (resolved_role == "EXECUTION") or (resolved_role == "APPROVER" and can_approver_execute)
    if not is_allowed:
        raise HTTPException(status_code=403, detail="Only authorized Execution dealers can take over desk quoting control.")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()

    status_res = desk_session_service.takeover_desk(
        assignment_id=assignment.id,
        email=payload.email,
        name=payload.name,
        role="EXECUTION"
    )

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
    db: Session = Depends(get_db)
):
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == offer_in.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    if rfq.status in ('PENDING_APPROVAL', 'CANCELLED'):
        raise HTTPException(status_code=403, detail="Quotation is cancelled or not currently open for bidding.")
    
    # Verify quotation approval status
    if assignment.approval_status == 'PENDING':
        raise HTTPException(status_code=403, detail="Quotation is pending approval from your bank's authorized approver.")
    if assignment.approval_status in ('DECLINED', 'EXPIRED'):
        raise HTTPException(status_code=403, detail="Your bank is not participating in this quotation.")

    # 3 seconds buffer check for network latency
    now = datetime.now(timezone.utc)
    try:
        start_ts = rfq.window_start.timestamp() - 3
        end_ts = rfq.window_end.timestamp() + 3
        if now.timestamp() < start_ts or now.timestamp() > end_ts:
            raise HTTPException(status_code=403, detail="Window is closed.")
    except Exception:
        pass

    # Verify submitter authorization & role if session provided
    submitted_by = offer_in.email
    if offer_in.session_token:
        otp_rec = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == offer_in.session_token
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
        can_submit, block_reason = desk_session_service.can_submit_quote(assignment.id, submitted_by)
        if not can_submit:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=block_reason)

    # Alternative Value Date Validation
    effective_target_value_date = assignment.value_date or rfq.value_date
    is_alt_allowed = assignment.allow_alternative_value_date if assignment.allow_alternative_value_date is not None else (rfq.allow_alternative_value_date or False)

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
        submitted_by_email=submitted_by
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
            rank = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment.id)
            all_assignments = db.query(QuotationBankAssignment).filter(
                QuotationBankAssignment.rfq_id == rfq.id
            ).all()
            submitted_ids = set(
                o.assignment_id for o in db.query(QuotationOffer).filter(
                    QuotationOffer.assignment_id.in_([a.id for a in all_assignments])
                ).all()
            )
            live_rank_data = {
                "rank": rank,
                "total_quotes": len(submitted_ids),
                "is_leading": rank == 1
            }
    except Exception:
        pass

    desk_session_service.record_quote_submission(assignment.id, submitted_by or "Dealer", offer_in.price)

    return {"success": True, "submitted_by": submitted_by, "live_rank": live_rank_data}

@router.post("/tbill-offer")
def submit_tbill_offer(
    offer_in: TBillOfferCreate,
    db: Session = Depends(get_db)
):
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == offer_in.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    if rfq.status in ('PENDING_APPROVAL', 'CANCELLED'):
        raise HTTPException(status_code=403, detail="Quotation is cancelled or not currently open for bidding.")
    
    # Verify quotation approval status
    if assignment.approval_status == 'PENDING':
        raise HTTPException(status_code=403, detail="Quotation is pending approval from your bank's authorized approver.")
    if assignment.approval_status in ('DECLINED', 'EXPIRED'):
        raise HTTPException(status_code=403, detail="Your bank is not participating in this quotation.")

    # 3 seconds buffer check for network latency
    now = datetime.now(timezone.utc)
    try:
        start_ts = rfq.window_start.timestamp() - 3
        end_ts = rfq.window_end.timestamp() + 3
        if now.timestamp() < start_ts or now.timestamp() > end_ts:
            raise HTTPException(status_code=403, detail="Window is closed.")
    except Exception:
        pass
    
    # Verify submitter authorization & role if session provided
    submitted_by = offer_in.email
    if offer_in.session_token:
        otp_rec = db.query(QuotationAccessOTP).filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.magic_token == offer_in.session_token
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
        can_submit, block_reason = desk_session_service.can_submit_quote(assignment.id, submitted_by)
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

    best_rate = max([line.discountRate for line in offer_in.lines]) if offer_in.lines else 0.0
    desk_session_service.record_quote_submission(assignment.id, submitted_by or "Dealer", best_rate)

    return {"success": True, "submitted_by": submitted_by, "live_rank": live_rank_data}

@router.get("/{token}/live-rank")
def get_live_rank(token: str, db: Session = Depends(get_db)):
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
        return {"is_live_ranking_enabled": False, "rank": None, "total_quotes": 0}

    rank = live_ranking_service.calculate_bank_live_rank(db, rfq.id, assignment.id)

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
        submitted_ids = set(
            o.assignment_id for o in db.query(QuotationOffer).filter(
                QuotationOffer.assignment_id.in_([a.id for a in all_assignments])
            ).all()
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
    otp_rec = db.query(QuotationAccessOTP).filter(
        QuotationAccessOTP.assignment_id == assignment.id,
        QuotationAccessOTP.magic_token == action_in.session_token
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
            subject = f"RFQ {rfq.ref_no} ({customer_name}) - Bank Participation Declined"
            notes_html = f"<p><strong>Reason / Notes:</strong> {action_in.notes}</p>" if action_in.notes else ""
            body = f"""
            <html>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 20px; color: #1e293b;">
                <p>Dear {bank_name} FX Desk,</p>
                <p>Your bank's authorized approver (<strong>{approver_email}</strong>) has <strong>declined participation</strong> for RFQ <strong>{rfq.ref_no}</strong> on behalf of <strong>{customer_name}</strong>.</p>
                {notes_html}
                <p>No further action is required from your desk.</p>
                <br/>
                <p>Best Regards,</p>
                <p>Treasury Team</p>
            </body>
            </html>
            """
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
        outcome = "NO_QUOTE"
        if a.approval_status == 'DECLINED':
            outcome = "PARTICIPATION_DECLINED"
        elif best_price is not None:
            if rfq.status == "COMPLETED":
                # Check analytics
                analytics = db.query(QuotationAnalytics).filter(QuotationAnalytics.rfq_id == rfq.id).first()
                if analytics and analytics.winner_quotation_bank_id == q_bank_id:
                    outcome = "WON"
                else:
                    outcome = "NOT_SELECTED"
            elif rfq.status in ["OPEN", "PENDING", "EVALUATING"]:
                outcome = "SUBMITTED"
            else:
                outcome = rfq.status

        history_items.append({
            "rfq_id": rfq.id,
            "ref_no": rfq.ref_no,
            "entity_name": rfq.entity.entity_name if rfq.entity else None,
            "entity_code": rfq.entity.code if rfq.entity else None,
            "type": rfq.type,
            "direction": rfq.direction,
            "amount": rfq.amount,
            "currency_pair": f"{rfq.buy_currency}/{rfq.sell_currency}" if rfq.buy_currency else None,
            "value_date": rfq.value_date,
            "window_end": rfq.window_end,
            "status": rfq.status,
            "quotation_base": a.quotation_base or rfq.quotation_base,
            "best_quote": best_price,
            "notes": notes,
            "submitted_by": submitted_by,
            "submitted_at": submitted_at,
            "outcome": outcome,
            "offers_count": len(offers_info) if rfq.type == "TBILL" else (1 if best_price else 0),
            "created_at": rfq.created_at
        })

    return {
        "bank_name": q_bank.bank.name if q_bank.bank else "Bank",
        "total_deals": len(history_items),
        "history": history_items
    }

@router.get("/{token}/result")
def get_public_rfq_result(token: str, db: Session = Depends(get_db)):
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
    if is_closed and rfq.status in ('PENDING', 'OPEN'):
        rfq.status = 'COMPLETED'
        db.commit()

    if not is_closed and rfq.status not in ('COMPLETED', 'CANCELLED', 'REJECTED'):
        return {"status": "OPEN" if (w_start and now >= w_start) else "PENDING"}

    # Indicative banks are for market sounding only and are never declared winners or sent regret statuses
    q_base = (assignment.quotation_base or rfq.quotation_base or 'Execution').lower()
    if q_base == 'indicative':
        return {"status": "INDICATIVE_ONLY"}

    # Calculate results using central endpoint evaluation logic
    from app.api.v1.endpoints.quotations_endpoints import get_rfq_results
    try:
        res_data = get_rfq_results(rfq.id, db, current_user=None)
        winner_bank_id = res_data.get("winner_bank_id")
        is_inconclusive = res_data.get("is_inconclusive", False)

        if is_inconclusive or not winner_bank_id:
            return {"status": "INCONCLUSIVE"}

        if assignment.quotation_bank and assignment.quotation_bank.bank_id == winner_bank_id:
            return {"status": "WINNER"}
        else:
            return {"status": "NOT_SELECTED"}
    except Exception:
        return {"status": "COMPLETED"}
