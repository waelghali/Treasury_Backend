# app/api/v1/endpoints/public_quotations.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime, timezone

from app.database import get_db
from app.models.models_quotation import QuotationBankAssignment, QuotationRequest, QuotationOffer, QuotationTBillOffer, QuotationBank
from app.schemas.schemas_quotation import FXSpotOfferCreate, TBillOfferCreate

router = APIRouter()

@router.get("/{token}")
def get_rfq_by_token(token: str, db: Session = Depends(get_db)):
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
    
    # If TimeZone aware datetimes require strict matching, handle offsets
    try:
        window_start = rfq.window_start
        window_end = rfq.window_end
        is_open = window_start <= now <= window_end
    except TypeError:
        # Fallback if naive
        now_naive = datetime.now()
        is_open = rfq.window_start <= now_naive <= rfq.window_end

    # Process Token Validity Expiry
    validity_hours = rfq.token_validity_hours or 24
    from datetime import timedelta
    try:
        if now > (rfq.window_end + timedelta(hours=validity_hours)):
            raise HTTPException(status_code=403, detail="The validity of this link has expired.")
    except TypeError:
        now_naive = datetime.now()
        if now_naive > (rfq.window_end + timedelta(hours=validity_hours)):
            raise HTTPException(status_code=403, detail="The validity of this link has expired.")

    offers = []
    if rfq.type == 'TBILL':
        tbill_records = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == assignment.id).all()
        offers = [{
            "settlement_date": o.settlement_date,
            "maturity_date": o.maturity_date,
            "discount_rate": o.discount_rate,
            "max_amount": o.max_amount,
            "submitted_at": o.submitted_at
        } for o in tbill_records]
    else:
        # FX_SPOT
        offer = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == assignment.id).order_by(QuotationOffer.submitted_at.desc()).first()
        if offer:
            offers = [{"price": offer.price, "submitted_at": offer.submitted_at}]

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
        "quotation_base": rfq.quotation_base,
        "document_path": rfq.document_path,
        "status": rfq.status,
        "assignment_id": assignment.id,
        "bank_name": bank_name,
        "customer_name": customer_name, # Critical branding detail
        "serverTime": now.isoformat(),
        "isWindowOpen": is_open,
        "offers": offers
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
        # Use timezone aware buffer
        buffer_ms = 5000
        start_ts = rfq.window_start.timestamp() - 5
        end_ts = rfq.window_end.timestamp() + 5
        if now.timestamp() < start_ts or now.timestamp() > end_ts:
            raise HTTPException(status_code=403, detail="Window is closed.")
    except Exception:
        pass

    offer = QuotationOffer(
        assignment_id=assignment.id,
        price=offer_in.price,
    )
    db.add(offer)
    db.commit()

    # Notify Creator
    from app.models.models_quotation import QuotationNotification
    db.add(QuotationNotification(
        user_id=rfq.created_by_user_id,
        type="NEW_OFFER",
        title=f"New Quote: {rfq.ref_no}",
        message=f"A bank has just submitted a quote for your {rfq.type} request.",
        link=f"/end-user/quotations/history?rfq_id={rfq.id}",
        is_read=False
    ))
    db.commit()

    return {"success": True}

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
        buffer_ms = 5000
        start_ts = rfq.window_start.timestamp() - 5
        end_ts = rfq.window_end.timestamp() + 5
        if now.timestamp() < start_ts or now.timestamp() > end_ts:
            raise HTTPException(status_code=403, detail="Window is closed.")
    except Exception:
        pass
    
    # Delete existing lines for this exact assignment entirely before repopulating
    db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == assignment.id).delete()
    
    for line in offer_in.lines:
        o = QuotationTBillOffer(
            assignment_id=assignment.id,
            settlement_date=line.settlementDate,
            maturity_date=line.maturityDate,
            discount_rate=line.discountRate,
            max_amount=line.maxAmount
        )
        db.add(o)
    
    db.commit()

    # Notify Creator
    from app.models.models_quotation import QuotationNotification
    db.add(QuotationNotification(
        user_id=rfq.created_by_user_id,
        type="NEW_OFFER",
        title=f"New T-Bill Quote: {rfq.ref_no}",
        message=f"A bank has just submitted a multi-line quote for your T-Bill request.",
        link=f"/end-user/quotations/history?rfq_id={rfq.id}",
        is_read=False
    ))
    db.commit()

    return {"success": True}

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

    # Calculate results
    if rfq.type == 'TBILL':
        # For T-Bills, we need to see if ANY winner was selected in Analytics
        from app.models.models_quotation import QuotationAnalytics
        analytics = db.query(QuotationAnalytics).filter(QuotationAnalytics.rfq_id == rfq.id).first()
        if not analytics:
            return {"status": "AWAITING_MANUAL_SELECTION"}
        
        # In T-Bills, we could have multiple winners, but usually, we store one or a list.
        # For now, let's check if this bank is the winner_quotation_bank_id
        if analytics.winner_quotation_bank_id == assignment.quotation_bank_id:
            return {"status": "WINNER"}
        else:
            return {"status": "NOT_SELECTED"}
        
    # FX_SPOT
    all_assignments = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == rfq.id).all()
    best_assignment_id = None
    best_score = float('inf')
    
    for a in all_assignments:
        offer = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == a.id).order_by(QuotationOffer.submitted_at.desc()).first()
        if offer:
            price = offer.price
            variableCost = (price * (a.cost_percent / 100)) + float(a.cost_flat or 0)
            adjustedCost = variableCost
            if a.cost_min > 0:
                adjustedCost = max(adjustedCost, a.cost_min)
            if a.cost_max > 0:
                adjustedCost = min(adjustedCost, a.cost_max)
            final_price = price + adjustedCost
            
            if final_price < best_score:
                best_score = final_price
                best_assignment_id = a.id
                
    if best_assignment_id == assignment.id:
        return {"status": "WINNER"}
    elif best_assignment_id is not None:
        return {"status": "NOT_SELECTED"}
    else:
        return {"status": "NO_BIDS"}
