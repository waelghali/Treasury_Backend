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
    QuotationTBillOffer, QuotationBank, QuotationAccessOTP, QuotationAnalytics
)
from app.schemas.schemas_quotation import (
    FXSpotOfferCreate, TBillOfferCreate, OTPRequestCreate, OTPVerifyCreate
)
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
        raise HTTPException(status_code=403, detail="The validity of this link has expired.")

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
                "notes": offer.notes,
                "submitted_by_email": offer.submitted_by_email,
                "submitted_at": offer.submitted_at
            }]

    parsed_docs = []
    if (assignment.is_document_visible is not False) and rfq.document_path:
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

    return {
        "id": rfq.id,
        "ref_no": rfq.ref_no,
        "type": rfq.type,
        "direction": rfq.direction,
        "value_date": rfq.value_date,
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
        "document_path": rfq.document_path if (assignment.is_document_visible is not False) else None,
        "documents": parsed_docs,
        "status": rfq.status,
        "assignment_id": assignment.id,
        "bank_name": bank_name,
        "customer_name": customer_name,
        "serverTime": now.isoformat(),
        "isWindowOpen": is_open,
        "offers": offers,
        "contacts": contacts
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
    role_badge = "⚡ Execution Trader" if role == "EXECUTION" else "👁️ View-Only Observer"
    
    subject = f"🔐 Access Code {otp_code} for RFQ {rfq.ref_no} - {q_bank.bank.name if q_bank.bank else 'Treasury Portal'}"
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

                <div style="text-align: center; margin-bottom: 24px;">
                    <a href="{magic_link}" style="display: inline-block; background-color: #2563eb; color: #ffffff; font-size: 14px; font-weight: 600; text-decoration: none; padding: 12px 28px; border-radius: 10px; box-shadow: 0 4px 6px -1px rgba(37,99,235,0.2);">
                        ⚡ 1-Click Instant Direct Access
                    </a>
                </div>

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
        sender_name=sender_name
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

@router.post("/offer")
def submit_fx_offer(
    offer_in: FXSpotOfferCreate,
    db: Session = Depends(get_db)
):
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == offer_in.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    if rfq.status == 'PENDING_APPROVAL':
        raise HTTPException(status_code=403, detail="Quotation is not yet approved.")
    
    # 5 seconds buffer check
    now = datetime.now(timezone.utc)
    try:
        start_ts = rfq.window_start.timestamp() - 5
        end_ts = rfq.window_end.timestamp() + 5
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
                raise HTTPException(status_code=403, detail="View-Only contacts are not authorized to submit bids.")
            submitted_by = otp_rec.email

    offer = QuotationOffer(
        assignment_id=assignment.id,
        price=offer_in.price,
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

    return {"success": True, "submitted_by": submitted_by}

@router.post("/tbill-offer")
def submit_tbill_offer(
    offer_in: TBillOfferCreate,
    db: Session = Depends(get_db)
):
    assignment = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.token == offer_in.token).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Invalid token")

    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == assignment.rfq_id).first()
    if rfq.status == 'PENDING_APPROVAL':
        raise HTTPException(status_code=403, detail="Quotation is not yet approved.")
    
    # Window check logic
    now = datetime.now(timezone.utc)
    try:
        start_ts = rfq.window_start.timestamp() - 5
        end_ts = rfq.window_end.timestamp() + 5
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
                raise HTTPException(status_code=403, detail="View-Only contacts are not authorized to submit bids.")
            submitted_by = otp_rec.email

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

    return {"success": True, "submitted_by": submitted_by}

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
        if best_price is not None:
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
    
    # Lazy evaluation in case history hasn't been fetched
    now = datetime.now(timezone.utc)
    try:
        is_closed = now > rfq.window_end
    except TypeError:
        is_closed = datetime.now() > rfq.window_end
        
    if is_closed and rfq.status == 'PENDING':
        rfq.status = 'COMPLETED'
        db.commit()

    if rfq.status != 'COMPLETED':
        return {"status": "PENDING"}

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
