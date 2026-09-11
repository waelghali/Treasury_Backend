from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, UploadFile, File, Response, Request
from sqlalchemy.orm import Session
from typing import List, Any
from datetime import datetime, timezone
import logging
import csv
import io
import os
import uuid
import shutil

from app.database import get_db
from app.core.security import get_current_active_user, TokenData
from app.crud.crud import log_action
from app.core.email_service import send_email, get_global_email_settings
from app.services.unified_email_builder import build_transaction_email_html, build_standard_email_html


from app.schemas.schemas_quotation import (
    QuotationBankCreate, QuotationBankOut,
    QuotationRequestCreate, QuotationRequestOut,
    QuotationResultsOut, QuotationResultItem,
    ReTenderRequest, QuotationResubmitRequest
)
from app.crud.crud_quotation import crud_quotation
from app.models.models_quotation import QuotationRequest, QuotationBankAssignment, QuotationOffer, QuotationTBillOffer, QuotationBank, QuotationAnalytics

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/upload-documents")
async def upload_quotation_documents(
    files: List[UploadFile] = File(...),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Uploads supporting documents for a quotation request to GCS bucket (or local storage fallback)."""
    from app.core.ai_integration import _upload_to_gcs, generate_signed_gcs_url, GCS_BUCKET_NAME
    
    uploaded_files = []
    for file in files:
        file_bytes = await file.read()
        safe_filename = f"{uuid.uuid4().hex[:8]}_{file.filename.replace(' ', '_')}"
        from app.core.storage_service import build_customer_blob_path
        blob_path = build_customer_blob_path(current_user.customer_id, "quotations", f"rfq_docs/{safe_filename}")
        
        gcs_uri = await _upload_to_gcs(GCS_BUCKET_NAME, blob_path, file_bytes, file.content_type or "application/octet-stream")
        if not gcs_uri:
            raise HTTPException(status_code=500, detail=f"Failed to upload document {file.filename} to cloud storage")
        
        signed = await generate_signed_gcs_url(gcs_uri, expiration=604800)
        doc_url = signed or gcs_uri

        uploaded_files.append({
            "name": file.filename,
            "path": doc_url
        })
        
    return {"documents": uploaded_files}

@router.post("/banks", response_model=QuotationBankOut)
def create_quotation_bank(
    bank_in: QuotationBankCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Adds a Bank to a specific Customer's Quotation Roster."""
    bank = crud_quotation.create_quotation_bank(db, customer_id=current_user.customer_id, obj_in=bank_in)
    
    # Audit log
    # entity_id is Int in audit_logs, so we safely put the string ID in details instead.
    log_action(
        db,
        user_id=current_user.user_id,
        action_type="QUOTATION_BANK_ADDED",
        entity_type="QuotationBank",
        entity_id=bank.id, # QuotationBank.id is Integer, so this is fine
        details={"bank_id": bank_in.bank_id, "emails": bank_in.emails},
        customer_id=current_user.customer_id
    )
    return bank

@router.delete("/banks/{bank_id}")
def delete_quotation_bank(
    bank_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Removes a Bank from the Customer's Quotation Roster."""
    success = crud_quotation.delete_quotation_bank(db, customer_id=current_user.customer_id, bank_id=bank_id)
    if not success:
        raise HTTPException(status_code=404, detail="Bank configuration not found.")
    
    # Audit log
    log_action(
        db,
        user_id=current_user.user_id,
        action_type="QUOTATION_BANK_REMOVED",
        entity_type="QuotationBank",
        entity_id=bank_id,
        details={"bank_id": bank_id},
        customer_id=current_user.customer_id
    )
    return {"message": "Bank configuration removed."}

@router.get("/banks", response_model=List[QuotationBankOut])
def get_quotation_banks(
    trade_type: str = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    return crud_quotation.get_quotation_banks(db, customer_id=current_user.customer_id, trade_type=trade_type)

@router.get("/banks/latest-costs")
def get_latest_bank_costs(
    bank_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Retrieves cost settings from the most recent approved RFQ for a given bank."""
    q_banks = db.query(QuotationBank).filter(
        QuotationBank.customer_id == current_user.customer_id,
        QuotationBank.bank_id == bank_id
    ).all()
    q_bank_ids = [qb.id for qb in q_banks]
    
    if not q_bank_ids:
        return {"cost_min": 0.0, "cost_percent": 0.0, "cost_max": 0.0, "cost_flat": 0.0}
        
    latest_assignment = db.query(QuotationBankAssignment).join(
        QuotationRequest, QuotationBankAssignment.rfq_id == QuotationRequest.id
    ).filter(
        QuotationBankAssignment.quotation_bank_id.in_(q_bank_ids),
        QuotationRequest.customer_id == current_user.customer_id,
        QuotationRequest.status.in_(['PENDING', 'EVALUATING', 'COMPLETED'])
    ).order_by(QuotationRequest.created_at.desc()).first()
    
    if latest_assignment:
        return {
            "cost_min": latest_assignment.cost_min or 0.0,
            "cost_percent": latest_assignment.cost_percent or 0.0,
            "cost_max": latest_assignment.cost_max or 0.0,
            "cost_flat": latest_assignment.cost_flat or 0.0,
            "quotation_base": latest_assignment.quotation_base
        }
        
    return {"cost_min": 0.0, "cost_percent": 0.0, "cost_max": 0.0, "cost_flat": 0.0}

@router.get("/recommendations")
def get_bank_recommendations(
    trade_type: str = "FX_SPOT",
    buy_currency: str = None,
    sell_currency: str = None,
    amount: float = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """
    Analyzes historical quotation outcomes for this customer to recommend the top 3 best-performing counterparties.
    Calculates win rates, response times, and spread performance.
    """
    from app.models.models_quotation import QuotationBank, QuotationRequest, QuotationBankAssignment, QuotationAnalytics, QuotationOffer

    q = db.query(QuotationBank).filter(
        QuotationBank.customer_id == current_user.customer_id
    )
    if trade_type and trade_type != 'BOTH':
        q = q.filter(QuotationBank.trade_type.in_([trade_type, 'BOTH']))
    customer_banks = q.all()

    if not customer_banks:
        return {"recommended_bank_ids": [], "recommendations": []}

    bank_stats = []
    for qb in customer_banks:
        assignments = db.query(QuotationBankAssignment).join(
            QuotationRequest, QuotationBankAssignment.rfq_id == QuotationRequest.id
        ).filter(
            QuotationBankAssignment.quotation_bank_id == qb.id,
            QuotationRequest.status.in_(['COMPLETED', 'EXPIRED'])
        )

        # Pair-specific filter if provided and enough history exists
        if buy_currency and sell_currency:
            pair_filtered = assignments.filter(
                QuotationRequest.buy_currency == buy_currency,
                QuotationRequest.sell_currency == sell_currency
            ).all()
            if len(pair_filtered) >= 1:
                assignments = pair_filtered
            else:
                assignments = assignments.all()
        else:
            assignments = assignments.all()

        total_invited = len(assignments)
        total_responded = 0
        total_won = 0
        response_times_sec = []

        for a in assignments:
            offers = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == a.id).all()
            if offers:
                total_responded += 1
                first_offer = sorted(offers, key=lambda o: o.submitted_at)[0]
                rfq = a.rfq
                ref_time = rfq.window_start or rfq.created_at
                if first_offer.submitted_at and ref_time:
                    try:
                        delta = (first_offer.submitted_at - ref_time).total_seconds()
                        if 0 < delta < 86400:
                            response_times_sec.append(delta)
                    except Exception:
                        pass

            analytics = db.query(QuotationAnalytics).filter(QuotationAnalytics.rfq_id == a.rfq_id).first()
            if analytics and analytics.winner_quotation_bank_id == qb.id:
                total_won += 1

        win_rate = (total_won / total_responded * 100) if total_responded > 0 else 0.0
        response_rate = (total_responded / total_invited * 100) if total_invited > 0 else 100.0
        avg_resp_min = (sum(response_times_sec) / len(response_times_sec) / 60) if response_times_sec else None

        score = (win_rate * 0.6) + (response_rate * 0.3) + (10 if total_won > 0 else 0)

        # Smart contextual highlight
        pair_label = f" in {buy_currency}/{sell_currency}" if (buy_currency and sell_currency) else ""
        if total_won > 0 and avg_resp_min:
            highlight = f"{win_rate:.0f}% Win Rate{pair_label} • Avg {avg_resp_min:.1f}m"
        elif total_won > 0:
            highlight = f"{win_rate:.0f}% Win Rate ({total_won} deals won)"
        elif total_responded > 0:
            highlight = f"Active Counterparty ({total_responded} quotes)"
        else:
            highlight = "Roster Bank • Ready to Quote"

        bank_name = qb.bank.name if qb.bank else f"Bank {qb.bank_id}"
        bank_stats.append({
            "bank_id": qb.bank_id,
            "quotation_bank_id": qb.id,
            "bank_name": bank_name,
            "win_rate": round(win_rate, 1),
            "total_won": total_won,
            "total_participated": total_responded,
            "avg_response_minutes": round(avg_resp_min, 1) if avg_resp_min else None,
            "highlight": highlight,
            "score": score
        })

    bank_stats.sort(key=lambda x: x["score"], reverse=True)
    top_recommendations = bank_stats[:3]
    rec_bank_ids = [r["bank_id"] for r in top_recommendations]

    return {
        "recommended_bank_ids": rec_bank_ids,
        "recommendations": top_recommendations
    }

@router.post("/", response_model=Any)
def create_rfq(
    rfq_in: QuotationRequestCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
    request: Request = None
):
    """Creates a new RFQ and generates secure tokens for external Banks."""
    # Add file path parsing here if files are uploaded.
    # For now, it accepts JSON. If files are needed, this endpoint will need to use Form/File Fastapi constructs.
    
    try:
        from app.crud.crud_config import crud_customer_configuration
        from app.constants import GlobalConfigKey
        
        config = crud_customer_configuration.get_customer_config_or_global_fallback(
            db, customer_id=current_user.customer_id, config_key=GlobalConfigKey.QUOTATION_APPROVAL_REQUIRED
        )
        requires_approval = False
        if config and config.get("effective_value"):
            requires_approval = str(config.get("effective_value")).lower() == 'true'

        # --- T-Bill Directional Validation ---
        if rfq_in.type == 'TBILL':
            if rfq_in.direction == 'Sell':
                # No ranges allowed for Sell
                if (rfq_in.settlementDateEnd and rfq_in.settlementDateEnd != rfq_in.settlementDateStart) or \
                   (rfq_in.maturityDateEnd and rfq_in.maturityDateEnd != rfq_in.maturityDateStart):
                    raise HTTPException(status_code=400, detail="Date ranges are not allowed for T-Bill Sell quotations.")
                # Eval rate not allowed / irrelevant for Sell
                if rfq_in.evalRate is not None:
                     rfq_in.evalRate = None # Silently clear or could raise error. Let's clear it.
            elif rfq_in.direction == 'Buy':
                # Eval rate required if ranges are present
                has_range = (rfq_in.settlementDateEnd and rfq_in.settlementDateEnd != rfq_in.settlementDateStart) or \
                            (rfq_in.maturityDateEnd and rfq_in.maturityDateEnd != rfq_in.maturityDateStart)
                if has_range and (rfq_in.evalRate is None or rfq_in.evalRate <= 0):
                    raise HTTPException(status_code=400, detail="Evaluation Interest Rate (%) is required for T-Bill Buy quotations with date ranges.")

        rfq, assignments = crud_quotation.create_request(
            db, 
            customer_id=current_user.customer_id, 
            user_id=current_user.user_id, 
            requires_approval=requires_approval,
            obj_in=rfq_in
        )
        
        log_action(
            db,
            user_id=current_user.user_id,
            action_type="QUOTATION_RFQ_CREATED",
            entity_type="QuotationRequest",
            entity_id=None, # UUID string cannot fit into Integer column
            details={"rfq_id": rfq.id, "ref_no": rfq.ref_no, "type": rfq.type},
            customer_id=current_user.customer_id
        )
        
        # Trigger immediate email dispatch if not requiring corporate-level approval
        if not requires_approval:
            email_settings = get_global_email_settings()
            from app.core.routing import get_frontend_base_url
            base_url = get_frontend_base_url(request=request)
            
            for assignment in assignments:
                q_bank_id = assignment.get("quotation_bank_id")
                bank_row = db.query(QuotationBank).filter(QuotationBank.id == q_bank_id).first() if q_bank_id else None
                if not bank_row:
                    continue
                    
                contacts = bank_row.contacts if isinstance(bank_row.contacts, list) and len(bank_row.contacts) > 0 else []
                if not contacts and bank_row.emails:
                    contacts = [{"email": e.strip(), "name": "", "role": "EXECUTION"} for e in bank_row.emails.split(',') if e.strip()]
                if not contacts:
                    continue
                
                bank_name = bank_row.bank.name if bank_row.bank else 'Bank Partner'
                link = f"{base_url}/public-quotation/{assignment['token']}"
                
                if assignment.get("approval_status") == "PENDING":
                    # --- BANK APPROVAL FLOW (Execution RFQ + bank has APPROVER contacts) ---
                    
                    # Phase 1a: Email APPROVER contacts with action link
                    approver_emails = [c.get("email", "").strip() for c in contacts if c.get("role") == "APPROVER" and c.get("email")]
                    if approver_emails:
                        approver_subject = f"APPROVAL REQUIRED: RFQ {rfq.ref_no} - {rfq.buy_currency}/{rfq.sell_currency}"
                        approver_body = f"""
                        <html>
                        <body>
                            <p>Dear {bank_name} Authorized Approver,</p>
                            <p>Your bank has been invited to participate in a new <strong>Execution</strong> Request for Quotation (RFQ).</p>
                            <br/>
                            <ul>
                                <li><strong>Reference:</strong> {rfq.ref_no}</li>
                                <li><strong>Product:</strong> {rfq.type}</li>
                                <li><strong>Pair:</strong> {rfq.buy_currency}/{rfq.sell_currency}</li>
                            </ul>
                            <p>Please review the RFQ details and approve your bank's participation. Once approved, your execution desk will receive the secure link to submit their binding quote.</p>
                            <a href="{link}" style="padding: 10px 20px; background-color: #000; color: #fff; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 10px;">Review & Approve</a>
                            <br/><br/>
                            <p>Best Regards,</p>
                            <p>Treasury Team</p>
                        </body>
                        </html>
                        """
                        background_tasks.add_task(send_email, db, approver_emails, approver_subject, approver_body, {}, email_settings)
                    
                    # Phase 1b: Email EXECUTION + VIEW_ONLY contacts with heads-up (NO link)
                    non_approver_emails = [c.get("email", "").strip() for c in contacts if c.get("role") != "APPROVER" and c.get("email")]
                    if non_approver_emails:
                        headsup_subject = f"HEADS UP: New RFQ Pending Bank Approval - {rfq.ref_no}"
                        headsup_body = f"""
                        <html>
                        <body>
                            <p>Dear {bank_name} FX Desk,</p>
                            <p>A new Request for Quotation (RFQ) has been received by your bank and is currently <strong>pending approval</strong> from your authorized approver.</p>
                            <br/>
                            <ul>
                                <li><strong>Reference:</strong> {rfq.ref_no}</li>
                                <li><strong>Product:</strong> {rfq.type}</li>
                                <li><strong>Pair:</strong> {rfq.buy_currency}/{rfq.sell_currency}</li>
                            </ul>
                            <p>You will receive a follow-up notification with a secure access link once your bank's approver has authorized participation.</p>
                            <br/>
                            <p>Best Regards,</p>
                            <p>Treasury Team</p>
                        </body>
                        </html>
                        """
                        background_tasks.add_task(send_email, db, non_approver_emails, headsup_subject, headsup_body, {}, email_settings)
                else:
                    # --- STANDARD FLOW (no approval needed) ---
                    bank_emails = [c.get("email", "").strip() for c in contacts if c.get("email")]
                    if bank_emails:
                        subject = f"ACTION REQUIRED: New RFQ Request - {rfq.type} - {rfq.ref_no}"
                        body = f"""
                        <html>
                        <body>
                            <p>Dear {bank_name} FX Desk,</p>
                            <p>You have received a new Request for Quotation (RFQ) on our Treasury Platform.</p>
                            <br/>
                            <ul>
                                <li><strong>Reference:</strong> {rfq.ref_no}</li>
                                <li><strong>Product:</strong> {rfq.type}</li>
                            </ul>
                            <p>To view full terms and submit your quote (or observe in View-Only mode), please click the secure link below. This link is unique to your institution and will expire automatically.</p>
                            <a href="{link}" style="padding: 10px 20px; background-color: #000; color: #fff; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 10px;">Access Quotation Portal</a>
                            <br/><br/>
                            <p>Best Regards,</p>
                            <p>Treasury Team</p>
                        </body>
                        </html>
                        """
                        background_tasks.add_task(send_email, db, bank_emails, subject, body, {}, email_settings)
        else:
            # Notify Corporate Admins
            from app.models import User, UserRole
            from app.models.models_quotation import QuotationNotification
            admins = db.query(User).filter(
                User.customer_id == current_user.customer_id,
                User.role == UserRole.CORPORATE_ADMIN
            ).all()
            for admin in admins:
                db.add(QuotationNotification(
                    user_id=admin.id,
                    type="RFQ_PENDING_APPROVAL",
                    title="Action Required: New RFQ Pending Approval",
                    message=f"A new {rfq.type} RFQ ({rfq.ref_no}) has been created by {current_user.user_id} and requires your approval.",
                    link=f"/corporate-admin/quotations/history?rfq_id={rfq.id}",
                    is_read=False
                ))
            db.commit()
        
        return {"rfq_id": rfq.id, "ref_no": rfq.ref_no, "assignments": assignments}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/", response_model=List[QuotationRequestOut])
def get_rfq_history(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Returns the history of quotations for this customer."""
    reqs = crud_quotation.get_requests(db, customer_id=current_user.customer_id)
    now = datetime.now(timezone.utc)
    changed = False
    
    for r in reqs:
        try:
            is_closed = now > r.window_end
        except TypeError:
            is_closed = datetime.now() > r.window_end
            
        if is_closed:
            if r.status == 'PENDING':
                r.status = 'COMPLETED'
                changed = True
            elif r.status == 'PENDING_APPROVAL':
                r.status = 'REJECTED'
                changed = True
            
            # Auto-expire any bank-level approvals that were still PENDING when window closed
            pending_assignments = db.query(QuotationBankAssignment).filter(
                QuotationBankAssignment.rfq_id == r.id,
                QuotationBankAssignment.approval_status == 'PENDING'
            ).all()
            for pa in pending_assignments:
                pa.approval_status = 'EXPIRED'
                changed = True
        
        # Attach winner and analytics data if available
        analytics = db.query(QuotationAnalytics).filter(QuotationAnalytics.rfq_id == r.id).first()
        if analytics:
            if analytics.winner_bank and analytics.winner_bank.bank:
                r.winner_bank_name = analytics.winner_bank.bank.name
            r.winner_rate = analytics.winner_price
            r.saved_vs_avg = analytics.avg_price_spread
            
        if r.parent_rfq_id:
            parent = db.query(QuotationRequest).filter(QuotationRequest.id == r.parent_rfq_id).first()
            if parent:
                r.parent_rfq_ref = parent.ref_no
                
        r.re_tender_count = db.query(QuotationRequest).filter(QuotationRequest.parent_rfq_id == r.id).count()
            
    if changed:
        db.commit()
        
    return reqs

@router.post("/{rfq_id}/re-tender")
def retender_quotation(
    rfq_id: str,
    payload: ReTenderRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
    request: Request = None
):
    """Clones an existing quotation (e.g. Inconclusive, Expired, or Completed) into a clean, audited re-tender."""
    parent = db.query(QuotationRequest).filter(
        QuotationRequest.id == rfq_id,
        QuotationRequest.customer_id == current_user.customer_id
    ).first()
    if not parent:
        raise HTTPException(status_code=404, detail="Quotation not found.")

    # Calculate re-tender suffix
    existing_count = db.query(QuotationRequest).filter(QuotationRequest.parent_rfq_id == parent.id).count()
    base_ref = parent.ref_no.split("-R")[0]
    new_ref_no = f"{base_ref}-R{existing_count + 1}"

    # Determine window times
    now = datetime.now(timezone.utc)
    w_start = payload.window_start if payload.window_start else now
    w_end = payload.window_end

    if w_end <= w_start:
        raise HTTPException(status_code=400, detail="Window close time must be after the opening time.")

    # Check customer approval policy
    from app.crud.crud_config import crud_customer_configuration
    from app.constants import GlobalConfigKey
    config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id=current_user.customer_id, config_key=GlobalConfigKey.QUOTATION_APPROVAL_REQUIRED
    )
    requires_approval = False
    if config and config.get("effective_value"):
        requires_approval = str(config.get("effective_value")).lower() == 'true'

    initial_status = "PENDING_APPROVAL" if requires_approval else "PENDING"

    new_id = str(uuid.uuid4())
    new_rfq = QuotationRequest(
        id=new_id,
        ref_no=new_ref_no,
        customer_id=parent.customer_id,
        created_by_user_id=current_user.user_id,
        type=parent.type,
        direction=parent.direction,
        value_date=parent.value_date,
        amount=payload.amount if payload.amount is not None else parent.amount,
        min_ticket_amount=parent.min_ticket_amount,
        buy_currency=parent.buy_currency,
        sell_currency=parent.sell_currency,
        settlement_date_start=parent.settlement_date_start,
        settlement_date_end=parent.settlement_date_end,
        maturity_date_start=parent.maturity_date_start,
        maturity_date_end=parent.maturity_date_end,
        eval_rate=parent.eval_rate,
        window_start=w_start,
        window_end=w_end,
        quotation_base=parent.quotation_base,
        max_tolerance_percent=parent.max_tolerance_percent,
        document_path=parent.document_path,
        status=initial_status,
        token_validity_hours=payload.token_validity_hours or parent.token_validity_hours or 24,
        parent_rfq_id=parent.id
    )
    db.add(new_rfq)
    db.flush()

    # Replicate Bank Assignments
    parent_assignments = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == parent.id).all()
    assigned_records = []
    
    for pa in parent_assignments:
        if payload.selected_bank_ids is not None and pa.quotation_bank.bank_id not in payload.selected_bank_ids:
            continue
        
        token = str(uuid.uuid4())
        bank_row = pa.quotation_bank
        bank_contacts = bank_row.contacts if (bank_row and isinstance(bank_row.contacts, list)) else []
        has_approvers = any(c.get("role") == "APPROVER" for c in bank_contacts)
        assignment_approval_status = "PENDING" if (has_approvers and parent.quotation_base == "Execution") else None

        new_assignment = QuotationBankAssignment(
            id=str(uuid.uuid4()),
            rfq_id=new_rfq.id,
            quotation_bank_id=pa.quotation_bank_id,
            token=token,
            cost_min=pa.cost_min,
            cost_percent=pa.cost_percent,
            cost_max=pa.cost_max,
            cost_flat=pa.cost_flat,
            quotation_base=pa.quotation_base,
            is_document_visible=pa.is_document_visible,
            approval_status=assignment_approval_status
        )
        db.add(new_assignment)
        assigned_records.append({"assignment": new_assignment, "bank_row": bank_row, "token": token})

    db.commit()

    # If no internal approval required, dispatch bank notification emails
    if not requires_approval:
        email_settings = get_global_email_settings()
        from app.core.routing import get_frontend_base_url
        base_url = get_frontend_base_url(request=request)
        customer_branding = new_rfq.customer.name if new_rfq.customer else "Treasury Customer"

        for item in assigned_records:
            bank_row = item["bank_row"]
            if not bank_row: continue
            contacts = bank_row.contacts if isinstance(bank_row.contacts, list) and len(bank_row.contacts) > 0 else []
            if not contacts and bank_row.emails:
                contacts = [{"email": e.strip(), "name": "", "role": "EXECUTION"} for e in bank_row.emails.split(',') if e.strip()]
            
            bank_emails = [c.get("email", "").strip() for c in contacts if c.get("email")]
            if bank_emails:
                link = f"{base_url}/public-quotation/{item['token']}"
                subject = f"ACTION REQUIRED: Re-Tender RFQ Request from {customer_branding} - {new_rfq.type} - {new_rfq.ref_no}"
                body = f"""
                <html>
                <body>
                    <p>Dear {bank_row.bank.name if bank_row.bank else 'Bank Partner'} FX Desk,</p>
                    <p>You have received a new re-tendered Request for Quotation (RFQ) on behalf of <strong>{customer_branding}</strong>.</p>
                    <ul>
                        <li><strong>Reference:</strong> {new_rfq.ref_no} (Re-tender of {parent.ref_no})</li>
                        <li><strong>Product:</strong> {new_rfq.type}</li>
                        <li><strong>Amount:</strong> {new_rfq.amount} {new_rfq.buy_currency or ''}</li>
                    </ul>
                    <a href="{link}" style="padding: 10px 20px; background-color: #000; color: #fff; text-decoration: none; border-radius: 5px; display: inline-block;">Access Quotation Portal</a>
                </body>
                </html>
                """
                background_tasks.add_task(send_email, db, bank_emails, subject, body, {}, email_settings)
    else:
        # Notify Corporate Admins
        from app.models import User, UserRole
        from app.models.models_quotation import QuotationNotification
        admins = db.query(User).filter(
            User.customer_id == current_user.customer_id,
            User.role == UserRole.CORPORATE_ADMIN
        ).all()
        for admin in admins:
            db.add(QuotationNotification(
                user_id=admin.id,
                type="RFQ_PENDING_APPROVAL",
                title=f"Action Required: Re-Tender RFQ {new_rfq.ref_no} Pending Approval",
                message=f"Re-tender {new_rfq.ref_no} (of {parent.ref_no}) has been created and requires your approval.",
                link=f"/corporate-admin/quotations/history?rfq_id={new_rfq.id}",
                is_read=False
            ))
        db.commit()

    return {
        "message": "Quotation re-tendered successfully.",
        "rfq_id": new_rfq.id,
        "ref_no": new_rfq.ref_no,
        "parent_ref_no": parent.ref_no,
        "status": new_rfq.status
    }

@router.post("/{rfq_id}/resubmit")
def resubmit_quotation(
    rfq_id: str,
    payload: QuotationResubmitRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Allows the maker to update an RFQ that was returned with status NEEDS_REVISION and resubmit for approval."""
    rfq = db.query(QuotationRequest).filter(
        QuotationRequest.id == rfq_id,
        QuotationRequest.customer_id == current_user.customer_id
    ).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="Quotation not found.")

    if rfq.status != 'NEEDS_REVISION':
        raise HTTPException(status_code=400, detail=f"Quotation is in {rfq.status} status and cannot be resubmitted.")

    if payload.amount is not None:
        rfq.amount = payload.amount
    if payload.window_start is not None:
        rfq.window_start = payload.window_start
    if payload.window_end is not None:
        rfq.window_end = payload.window_end
    if payload.quotation_base is not None:
        rfq.quotation_base = payload.quotation_base
    if payload.max_tolerance_percent is not None:
        rfq.max_tolerance_percent = payload.max_tolerance_percent

    rfq.status = 'PENDING_APPROVAL'
    db.commit()

    # Notify Corporate Admins
    from app.models import User, UserRole
    from app.models.models_quotation import QuotationNotification
    admins = db.query(User).filter(
        User.customer_id == current_user.customer_id,
        User.role == UserRole.CORPORATE_ADMIN
    ).all()
    for admin in admins:
        db.add(QuotationNotification(
            user_id=admin.id,
            type="RFQ_RESUBMITTED",
            title=f"Revised RFQ {rfq.ref_no} Resubmitted for Approval",
            message=f"Maker has addressed your notes and resubmitted RFQ {rfq.ref_no}.",
            link=f"/corporate-admin/quotations/history?rfq_id={rfq.id}",
            is_read=False
        ))
    db.commit()

    return {"message": "Quotation revised and resubmitted for approval.", "rfq_id": rfq.id, "status": rfq.status}

@router.get("/stats")
def get_quotation_stats(
    trade_type: str = None,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Calculates bank performance statistics for the Market Insights dashboard."""
    query = db.query(QuotationRequest).filter(
        QuotationRequest.customer_id == current_user.customer_id,
        QuotationRequest.status == 'COMPLETED'
    )
    if trade_type:
        query = query.filter(QuotationRequest.type == trade_type)
    reqs = query.all()
    
    bank_stats = {}
    
    for r in reqs:
        # For each RFQ, find all offers and determine ranks
        assignments = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == r.id).all()
        
        offers = []
        if r.type == 'FX_SPOT':
            for a in assignments:
                best_offer = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == a.id).order_by(QuotationOffer.price.asc()).first()
                if best_offer:
                    offers.append({'bank_id': a.quotation_bank_id, 'price': best_offer.price, 'name': a.quotation_bank.bank.name if a.quotation_bank.bank else 'Unknown'})
            
            is_sell = (r.direction and r.direction.lower() == 'sell')
            sorted_offers = sorted(offers, key=lambda x: x['price'], reverse=is_sell)
        else: # TBILL
            # We must normalize all offers for this specific RFQ to determine the correct winner and ranks
            rfq_offers = []
            for a in assignments:
                best_tb_offer = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == a.id).order_by(QuotationTBillOffer.discount_rate.asc()).first()
                if best_tb_offer:
                    rfq_offers.append({
                        'bank_id': a.quotation_bank_id, 
                        'name': a.quotation_bank.bank.name if a.quotation_bank.bank else 'Unknown',
                        'settlement_date': best_tb_offer.settlement_date,
                        'maturity_date': best_tb_offer.maturity_date,
                        'discount_rate': best_tb_offer.discount_rate
                    })
            
            if not rfq_offers:
                continue

            is_buy = (r.direction and r.direction.lower() == 'buy')
            eval_rate = (r.eval_rate or 0) / 100.0

            s_min = None
            m_max = None
            
            for o in rfq_offers:
                try:
                    o['s_dt'] = datetime.strptime(o['settlement_date'], "%Y-%m-%d")
                    o['m_dt'] = datetime.strptime(o['maturity_date'], "%Y-%m-%d")
                    if s_min is None or o['s_dt'] < s_min: s_min = o['s_dt']
                    if m_max is None or o['m_dt'] > m_max: m_max = o['m_dt']
                except Exception: continue

            for o in rfq_offers:
                days = (o['m_dt'] - o['s_dt']).days
                price = 100.0 * (1.0 - (o['discount_rate'] / 100.0) * (days / 360.0))
                if is_buy:
                    delta_s = (o['s_dt'] - s_min).days
                    delta_m = (m_max - o['m_dt']).days
                    m_accrual = 1.0 + (eval_rate * (delta_m / 360.0))
                    normalized_price = (price / m_accrual) * (1.0 - (eval_rate * (delta_s / 360.0)))
                    o['final_price'] = normalized_price
                else:
                    o['final_price'] = o['discount_rate'] # Lowest DR wins for Sell

            # Normalized "Price" (Score) determining the rank
            sorted_offers = sorted(rfq_offers, key=lambda x: x['final_price'])
            # Remap to the format expected by the stats loop
            sorted_offers = [{'bank_id': o['bank_id'], 'price': o['final_price'], 'name': o['name']} for o in sorted_offers]
        
        for i, offer in enumerate(sorted_offers):
            bid_id = offer['bank_id']
            if bid_id not in bank_stats:
                bank_stats[bid_id] = {
                    'bank_id': bid_id,
                    'bank_name': offer['name'],
                    'total_participated': 0,
                    'total_won': 0,
                    'ranks': {1: 0, 2: 0, 3: 0},
                    'total_spread': 0.0,
                    'spread_count': 0
                }
            
            stats = bank_stats[bid_id]
            stats['total_participated'] += 1
            rank = i + 1
            if rank <= 3:
                stats['ranks'][rank] += 1
            if rank == 1:
                stats['total_won'] += 1
            
            winner_price = sorted_offers[0]['price']
            if winner_price > 0:
                spread = abs(offer['price'] - winner_price) / winner_price * 100
                stats['total_spread'] += spread
                stats['spread_count'] += 1

    results = []
    for bid, s in bank_stats.items():
        results.append({
            'bank_id': s['bank_id'],
            'bank_name': s['bank_name'],
            'win_rate': (s['total_won'] / s['total_participated'] * 100) if s['total_participated'] > 0 else 0,
            'total_won': s['total_won'],
            'total_participated': s['total_participated'],
            'ranks': s['ranks'],
            'avg_spread': (s['total_spread'] / s['spread_count']) if s['spread_count'] > 0 else 0
        })
        
    return sorted(results, key=lambda x: x['win_rate'], reverse=True)

@router.get("/{rfq_id}/results", response_model=QuotationResultsOut)
def get_rfq_results(
    rfq_id: str,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Calculates active Quotation standings/results for a given RFQ."""
    if current_user:
        rfq = crud_quotation.get_request(db, rfq_id=rfq_id, customer_id=current_user.customer_id)
    else:
        rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()

    if not rfq:
        raise HTTPException(status_code=404, detail="RFQ not found")
        
    now = datetime.now(timezone.utc)
    try:
        is_closed = now > rfq.window_end
    except TypeError:
        is_closed = datetime.now() > rfq.window_end
        
    if is_closed and rfq.status == 'PENDING':
        rfq.status = 'COMPLETED'
        db.commit()
        
    assignments = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == rfq_id).all()
    
    results = []
    winner_bank_id = None
    is_inconclusive = False
    inconclusive_reason = None
    best_indicative_rate = None
    best_execution_rate = None
    deviation_percent = None
    has_execution_banks = False
    
    if rfq.type == 'TBILL':
        all_tbill_offers = []
        for a in assignments:
            offers_db = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == a.id).all()
            q_bank = db.query(QuotationBank).filter(QuotationBank.id == a.quotation_bank_id).first()
            for o in offers_db:
                all_tbill_offers.append({
                    "bank_id": q_bank.bank_id if q_bank else 0,
                    "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                    "bank_emails": q_bank.emails if q_bank else "",
                    "settlement_date": o.settlement_date,
                    "maturity_date": o.maturity_date,
                    "discount_rate": o.discount_rate,
                    "max_amount": o.max_amount,
                    "submitted_at": o.submitted_at
                })

        if not all_tbill_offers:
            # Return empty structure if no offers
            for a in assignments:
                q_bank = db.query(QuotationBank).filter(QuotationBank.id == a.quotation_bank_id).first()
                results.append({
                    "bank_id": q_bank.bank_id if q_bank else 0,
                    "quotation_bank_id": a.quotation_bank_id,
                    "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                    "bank_emails": q_bank.emails if q_bank else "",
                    "offers": [],
                    "best_score": None,
                    "token": a.token,
                    "quotation_base": a.quotation_base or rfq.quotation_base,
                    "is_document_visible": a.is_document_visible if a.is_document_visible is not None else True,
                    "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                    "approval_status": a.approval_status,
                    "approved_by_email": a.approved_by_email,
                    "approved_at": a.approved_at,
                    "approval_notes": a.approval_notes
                })
            return {
                "rfq": rfq,
                "results": results,
                "winner_bank_id": None,
                "is_inconclusive": is_closed,
                "inconclusive_reason": "Quotation window closed without receiving any offers from counterparties." if is_closed else None,
                "best_indicative_rate": None,
                "best_execution_rate": None,
                "deviation_percent": None,
                "has_execution_banks": True
            }

        # --- T-Bill Normalization Logic ---
        is_buy = (rfq.direction and rfq.direction.lower() == 'buy')
        eval_rate = (rfq.eval_rate or 0) / 100.0

        s_min = None
        m_max = None
        
        parsed_offers = []
        for o in all_tbill_offers:
            try:
                s_dt = datetime.strptime(o['settlement_date'], "%Y-%m-%d")
                m_dt = datetime.strptime(o['maturity_date'], "%Y-%m-%d")
                o['s_dt'] = s_dt
                o['m_dt'] = m_dt
                parsed_offers.append(o)
                
                if s_min is None or s_dt < s_min: s_min = s_dt
                if m_max is None or m_dt > m_max: m_max = m_dt
            except Exception:
                continue

        # Calculate scores
        for o in parsed_offers:
            days = (o['m_dt'] - o['s_dt']).days
            price = 100.0 * (1.0 - (o['discount_rate'] / 100.0) * (days / 360.0))
            
            if is_buy:
                # Normalize to S_min and M_max
                delta_s = (o['s_dt'] - s_min).days
                delta_m = (m_max - o['m_dt']).days
                
                # Accrue maturity gap: reinvest the FV until m_max
                # Scale the price so it represents a value of exactly 100 at m_max
                m_accrual_factor = 1.0 + (eval_rate * (delta_m / 360.0))
                scaled_price = price / m_accrual_factor
                
                # Discount back for settlement delay
                s_discount_factor = 1.0 - (eval_rate * (delta_s / 360.0))
                normalized_price = scaled_price * s_discount_factor
                o['score'] = normalized_price
            else:
                # Sell: Lower Discount Rate wins (Higher proceeds)
                o['score'] = o['discount_rate']

        # Group by bank and take the best offer
        bank_best = {}
        for o in parsed_offers:
            bid = o['bank_id']
            if bid not in bank_best or o['score'] < bank_best[bid]['score']:
                bank_best[bid] = o

        # Format Final Results
        for a in assignments:
            q_bank = db.query(QuotationBank).filter(QuotationBank.id == a.quotation_bank_id).first()
            bank_id = q_bank.bank_id if q_bank else 0
            
            # All offers from this specific bank
            bank_offers = [o for o in parsed_offers if o['bank_id'] == bank_id]
            best_offer = bank_best.get(bank_id)
            
            results.append({
                "bank_id": bank_id,
                "quotation_bank_id": a.quotation_bank_id,
                "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                "bank_emails": q_bank.emails if q_bank else "",
                "offers": bank_offers,
                "best_score": best_offer['score'] if best_offer else None,
                "submitted_by_email": best_offer.get('submitted_by_email') if best_offer else (bank_offers[0].get('submitted_by_email') if bank_offers else None),
                "notes": best_offer.get('notes') if best_offer else (bank_offers[0].get('notes') if bank_offers else None),
                "token": a.token,
                "quotation_base": a.quotation_base or rfq.quotation_base,
                "is_document_visible": a.is_document_visible if a.is_document_visible is not None else True,
                "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                "approval_status": a.approval_status,
                "approved_by_email": a.approved_by_email,
                "approved_at": a.approved_at,
                "approval_notes": a.approval_notes
            })

        # Sort results: Lowest score wins (Lowest price for buy, Lowest DR for sell)
        results.sort(key=lambda x: (x['best_score'] is None, x['best_score']))
        if results and results[0].get('best_score') is not None:
            winner_bank_id = results[0]['bank_id']

    else:
        # FX_SPOT
        for a in assignments:
            offer_db = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == a.id).order_by(QuotationOffer.submitted_at.desc()).first()
            q_bank = db.query(QuotationBank).filter(QuotationBank.id == a.quotation_bank_id).first()
            
            if not offer_db:
                results.append({
                    "bank_id": q_bank.bank_id if q_bank else 0,
                    "quotation_bank_id": a.quotation_bank_id,
                    "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                    "bank_emails": q_bank.emails if q_bank else "",
                    "price": None,
                    "finalPrice": None,
                    "notes": None,
                    "submitted_at": None,
                    "submitted_by_email": None,
                    "token": a.token,
                    "quotation_base": a.quotation_base or rfq.quotation_base,
                    "is_document_visible": a.is_document_visible if a.is_document_visible is not None else True,
                    "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                    "approval_status": a.approval_status,
                    "approved_by_email": a.approved_by_email,
                    "approved_at": a.approved_at,
                    "approval_notes": a.approval_notes
                })
                continue
            
            price = offer_db.price
            deal_amount = float(rfq.amount or 1.0)
            base_deal_volume = deal_amount * price
            raw_fee = (base_deal_volume * (float(a.cost_percent or 0) / 100.0)) + float(a.cost_flat or 0)
            clamped_fee = raw_fee
            if a.cost_min and a.cost_min > 0:
                clamped_fee = max(clamped_fee, float(a.cost_min))
            if a.cost_max and a.cost_max > 0:
                clamped_fee = min(clamped_fee, float(a.cost_max))
                
            fee_per_unit = clamped_fee / deal_amount if deal_amount > 0 else 0.0
            is_sell_dir = (rfq.direction and rfq.direction.lower() == 'sell')
            final_all_in_price = (price - fee_per_unit) if is_sell_dir else (price + fee_per_unit)
                
            results.append({
                "bank_id": q_bank.bank_id if q_bank else 0,
                "quotation_bank_id": a.quotation_bank_id,
                "bank_name": q_bank.bank.name if q_bank and q_bank.bank else "Unknown Bank",
                "bank_emails": q_bank.emails if q_bank else "",
                "price": price,
                "finalPrice": final_all_in_price,
                "bank_fee_total": clamped_fee,
                "fee_per_unit": fee_per_unit,
                "notes": offer_db.notes,
                "submitted_at": offer_db.submitted_at,
                "submitted_by_email": offer_db.submitted_by_email,
                "token": a.token,
                "quotation_base": a.quotation_base or rfq.quotation_base,
                "is_document_visible": a.is_document_visible if a.is_document_visible is not None else True,
                "contacts": q_bank.contacts if (q_bank and q_bank.contacts) else [],
                "approval_status": a.approval_status,
                "approved_by_email": a.approved_by_email,
                "approved_at": a.approved_at,
                "approval_notes": a.approval_notes
            })
            
        # Filter nulls and sort by direction
        is_sell = (rfq.direction and rfq.direction.lower() == 'sell')
        valid_results = [r for r in results if r.get('finalPrice') is not None]
        valid_results.sort(key=lambda x: x['finalPrice'], reverse=is_sell)
        results = valid_results + [r for r in results if r.get('finalPrice') is None]
        
        # --- Evaluation Logic: Execution vs Indicative & Max Tolerance Check ---
        has_execution_banks = any((r.get('quotation_base') or rfq.quotation_base or 'Execution').lower() == 'execution' for r in results)
        
        winner_bank_id = None
        is_inconclusive = False
        inconclusive_reason = None
        best_indicative_rate = None
        best_execution_rate = None
        deviation_percent = None

        if not valid_results:
            if is_closed:
                is_inconclusive = True
                inconclusive_reason = "Quotation window closed without receiving any quotes from assigned counterparties."
        elif not has_execution_banks:
            # Scenario B: All banks are Indicative -> pure market sounding
            is_inconclusive = True
            inconclusive_reason = "All counterparties were requested on an Indicative basis. No binding winner is selected."
        else:
            # Separate valid bids into Indicative and Execution pools
            indicative_bids = [r for r in valid_results if (r.get('quotation_base') or rfq.quotation_base or 'Execution').lower() == 'indicative']
            execution_bids = [r for r in valid_results if (r.get('quotation_base') or rfq.quotation_base or 'Execution').lower() == 'execution']
            
            if not execution_bids and is_closed:
                is_inconclusive = True
                inconclusive_reason = "No Execution quotes were submitted before the window closed. Only Indicative quotes were received."
            
            if indicative_bids:
                best_indicative_rate = indicative_bids[0]['finalPrice'] # Already sorted by direction
            if execution_bids:
                best_execution_rate = execution_bids[0]['finalPrice'] # Already sorted by direction
                
            if execution_bids:
                best_exec_item = execution_bids[0]
                if best_indicative_rate is not None and best_execution_rate is not None:
                    # Scenario C: Mixed -> Calculate one-directional deviation
                    if is_sell:
                        # Sell: Higher rate is better. Deviation is how much lower execution is vs indicative benchmark
                        deviation_percent = ((best_indicative_rate - best_execution_rate) / best_indicative_rate) * 100.0
                    else:
                        # Buy: Lower rate is better. Deviation is how much higher execution is vs indicative benchmark
                        deviation_percent = ((best_execution_rate - best_indicative_rate) / best_indicative_rate) * 100.0
                    
                    if deviation_percent <= 0:
                        # Execution rate is equal to or better than Indicative benchmark
                        winner_bank_id = best_exec_item['bank_id']
                    else:
                        max_tol = rfq.max_tolerance_percent if rfq.max_tolerance_percent is not None else 0.0
                        if deviation_percent > max_tol:
                            is_inconclusive = True
                            inconclusive_reason = (
                                f"The best Execution rate ({best_execution_rate:.4f}) exceeded the Indicative benchmark "
                                f"({best_indicative_rate:.4f}) by {deviation_percent:.2f}%, which is higher than the allowed tolerance of {max_tol:.2f}%."
                            )
                        else:
                            winner_bank_id = best_exec_item['bank_id']
                else:
                    # Scenario A: All Execution or no Indicative quotes submitted
                    winner_bank_id = best_exec_item['bank_id']

    # --- 1. Live Trading Floor Presence Telemetry ---
    from app.models.models_quotation import QuotationAccessOTP
    total_invited = len(assignments)
    desks_active = 0
    quotes_locked = 0
    approvals_pending = 0
    approvals_cleared = 0

    for a in assignments:
        if a.approval_status == 'PENDING':
            approvals_pending += 1
        elif a.approval_status == 'APPROVED':
            approvals_cleared += 1

        otp_exists = db.query(QuotationAccessOTP).filter(QuotationAccessOTP.assignment_id == a.id).first()
        if otp_exists:
            desks_active += 1

        if rfq.type == 'TBILL':
            has_q = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == a.id).first()
        else:
            has_q = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == a.id).first()
        if has_q:
            quotes_locked += 1

    live_telemetry = {
        "total_invited": total_invited,
        "desks_active": desks_active,
        "quotes_locked": quotes_locked,
        "approvals_pending": approvals_pending,
        "approvals_cleared": approvals_cleared,
        "summary_text": f"{desks_active} of {total_invited} Desks Active • {quotes_locked} Quote{'s' if quotes_locked != 1 else ''} Locked In"
    }

    # --- 2. Best Execution & Monetary Savings Summary ---
    savings_summary = None
    if winner_bank_id and not is_inconclusive:
        winner_res = next((r for r in results if r['bank_id'] == winner_bank_id), None)
        if winner_res:
            if rfq.type == 'FX_SPOT' and valid_results:
                rates = [r['finalPrice'] for r in valid_results if r.get('finalPrice') is not None]
                if len(rates) >= 1:
                    win_rate = winner_res.get('finalPrice') or rates[0]
                    avg_rate = sum(rates) / len(rates)
                    worst_rate = max(rates) if not is_sell else min(rates)
                    amount = float(rfq.amount or 1.0)
                    
                    if not is_sell:
                        # Buy: lower price is better
                        saved_vs_avg = max(0.0, (avg_rate - win_rate) * amount)
                        saved_vs_worst = max(0.0, (worst_rate - win_rate) * amount)
                    else:
                        # Sell: higher price is better
                        saved_vs_avg = max(0.0, (win_rate - avg_rate) * amount)
                        saved_vs_worst = max(0.0, (win_rate - worst_rate) * amount)

                    savings_summary = {
                        "winner_bank_name": winner_res.get('bank_name'),
                        "winner_rate": round(win_rate, 4),
                        "avg_rate": round(avg_rate, 4),
                        "worst_rate": round(worst_rate, 4),
                        "currency": rfq.sell_currency,
                        "saved_vs_avg": round(saved_vs_avg, 2),
                        "saved_vs_worst": round(saved_vs_worst, 2),
                        "total_quotes": len(rates)
                    }
            elif rfq.type == 'TBILL' and valid_results:
                scores = [r['best_score'] for r in valid_results if r.get('best_score') is not None]
                if len(scores) >= 1:
                    win_score = winner_res.get('best_score') or scores[0]
                    avg_score = sum(scores) / len(scores)
                    savings_summary = {
                        "winner_bank_name": winner_res.get('bank_name'),
                        "winner_rate": round(win_score, 4),
                        "avg_rate": round(avg_score, 4),
                        "worst_rate": round(max(scores) if is_buy else min(scores), 4),
                        "currency": "EGP",
                        "saved_vs_avg": round(abs(avg_score - win_score) * 1000, 2),
                        "saved_vs_worst": round(abs(max(scores) - min(scores)) * 1000, 2),
                        "total_quotes": len(scores)
                    }

    return {
        "rfq": rfq,
        "results": results,
        "winner_bank_id": winner_bank_id,
        "is_inconclusive": is_inconclusive,
        "inconclusive_reason": inconclusive_reason,
        "best_indicative_rate": best_indicative_rate,
        "best_execution_rate": best_execution_rate,
        "deviation_percent": deviation_percent,
        "has_execution_banks": has_execution_banks,
        "live_telemetry": live_telemetry,
        "savings_summary": savings_summary
    }

@router.post("/{rfq_id}/send-results")
async def send_rfq_results(
    rfq_id: str,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Sends winner/regret emails to assigned Execution banks for a completed RFQ."""
    rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="RFQ not found")
    
    # Process results retrieval
    res_data = get_rfq_results(rfq_id, db, current_user)
    results = res_data["results"]
    is_inconclusive = res_data.get("is_inconclusive", False)
    inconclusive_reason = res_data.get("inconclusive_reason")
    winner_bank_id = res_data.get("winner_bank_id")

    if is_inconclusive:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot send deal result emails: {inconclusive_reason or 'RFQ ended without a conclusive winner.'}"
        )
        
    if not winner_bank_id:
        raise HTTPException(status_code=400, detail="Cannot send result emails: No winning Execution quote found.")

    from app.core.email_service import get_customer_email_settings, send_email
    email_settings, source = get_customer_email_settings(db, rfq.customer_id)

    for bank_res in results:
        # Indicative banks do not receive execution win/regret emails
        q_base = (bank_res.get('quotation_base') or rfq.quotation_base or 'Execution').lower()
        if q_base == 'indicative':
            continue

        bank_emails = []
        if bank_res.get('contacts') and isinstance(bank_res['contacts'], list):
            bank_emails = [c.get('email', '').strip() for c in bank_res['contacts'] if c.get('email')]
        if not bank_emails and bank_res.get('bank_emails'):
            bank_emails = [e.strip() for e in bank_res['bank_emails'].split(',') if e.strip()]

        if not bank_emails:
            continue
        
        is_winner = (bank_res['bank_id'] == winner_bank_id)
        
        ref_no = rfq.ref_no
        customer_name = rfq.customer.name if rfq.customer else "Treasury Client"
        if is_winner:
            subject = f"TRADE EXECUTION CONFIRMED: RFQ {ref_no} - {rfq.buy_currency}/{rfq.sell_currency}"
            body = build_transaction_email_html(
                customer_name=customer_name,
                title="📈 Trade Execution Confirmation",
                transaction_ref=ref_no,
                transaction_type="RFQ Execution",
                key_value_dict={
                    "Pair": f"{rfq.buy_currency}/{rfq.sell_currency}",
                    "Direction": rfq.direction,
                    "Amount": f"{rfq.amount:,.2f} {rfq.buy_currency}",
                    "Executed Rate": f"<span style='color: #16a34a; font-weight: 700;'>{bank_res['price']:.5f}</span>",
                    "All-In Effective Rate": f"{bank_res['finalPrice']:.5f}",
                    "Value Date": rfq.value_date or 'Standard Spot'
                },
                summary_text=f"We are pleased to confirm the execution of the trade with <strong>{customer_name}</strong> based on your winning quote.",
                recipient_name=f"{bank_res['bank_name']} Treasury Desk"
            )
        else:
            subject = f"RFQ Result Notification: RFQ {ref_no} - {rfq.buy_currency}/{rfq.sell_currency}"
            body = build_standard_email_html(
                customer_name=customer_name,
                title="RFQ Result Notification",
                content_html=f"""
                <p>Thank you for participating in the Request for Quotation (RFQ) for <strong>{rfq.buy_currency}/{rfq.sell_currency}</strong> with <strong>{customer_name}</strong>.</p>
                <p>We are writing to inform you that your quote was not selected for this specific deal as we executed with another counterparty at a more competitive all-in rate.</p>
                <p>We appreciate your prompt participation and look forward to receiving your quotes on future requests.</p>
                """,
                recipient_name=f"{bank_res['bank_name']} Treasury Desk"
            )

        
        # Override sender name to "Treasury Quotations" if using system default
        sender_name = "Treasury Quotations" if source != "customer_specific" else email_settings.sender_display_name
        
        await send_email(
            db=db,
            to_emails=bank_emails,
            subject_template=subject,
            body_template=body,
            template_data={},
            email_settings=email_settings,
            sender_name=sender_name
        )

    return {"message": "Result emails sent to all participating banks."}

@router.post("/{rfq_id}/resend-invite/{quotation_bank_id}")
async def resend_rfq_bank_invite(
    rfq_id: str,
    quotation_bank_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user),
    request: Request = None
):
    """Resends the secure invite email to a specific bank for an RFQ."""
    rfq = db.query(QuotationRequest).filter(
        QuotationRequest.id == rfq_id,
        QuotationRequest.customer_id == current_user.customer_id
    ).first()
    if not rfq:
        raise HTTPException(status_code=404, detail="RFQ not found")
        
    assignment = db.query(QuotationBankAssignment).filter(
        QuotationBankAssignment.rfq_id == rfq.id,
        QuotationBankAssignment.quotation_bank_id == quotation_bank_id
    ).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Bank assignment not found for this RFQ")
        
    q_bank = db.query(QuotationBank).filter(QuotationBank.id == quotation_bank_id).first()
    if not q_bank or not q_bank.emails:
        raise HTTPException(status_code=400, detail="No email address configured for this bank")
        
    email_settings = get_global_email_settings()
    from app.core.routing import get_frontend_base_url
    base_url = get_frontend_base_url(request=request)
    bank_emails = [e.strip() for e in q_bank.emails.split(',') if e.strip()]
    link = f"{base_url}/public-quotation/{assignment.token}"
    
    subject = f"REMINDER: RFQ Request - {rfq.type} - {rfq.ref_no}"
    body = f"""
    <html>
    <body>
        <p>Dear {q_bank.bank.name if q_bank.bank else 'Bank Partner'} FX Desk,</p>
        <p>This is a reminder regarding the Request for Quotation (RFQ) on our Treasury Platform.</p>
        <br/>
        <ul>
            <li><strong>Reference:</strong> {rfq.ref_no}</li>
            <li><strong>Product:</strong> {rfq.type}</li>
        </ul>
        <p>To submit your quote, please click the secure link below. This link is unique to your institution and will expire automatically.</p>
        <a href="{link}" style="padding: 10px 20px; background-color: #000; color: #fff; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 10px;">Submit Quote Now</a>
        <br/><br/>
        <p>Best Regards,</p>
        <p>Treasury Team</p>
    </body>
    </html>
    """
    background_tasks.add_task(
        send_email,
        db,
        bank_emails,
        subject,
        body,
        {},
        email_settings,
    )
    return {"message": f"Invitation email resent to {q_bank.bank.name if q_bank.bank else 'Bank'}"}

@router.get("/notifications")
def get_my_notifications(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Fetches the 20 most recent notifications for the logged-in user."""
    from app.models.models_quotation import QuotationNotification
    return db.query(QuotationNotification).filter(
        QuotationNotification.user_id == current_user.user_id
    ).order_by(QuotationNotification.created_at.desc()).limit(20).all()

@router.patch("/notifications/{notification_id}/read")
def mark_notification_as_read(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Marks a specific notification as read."""
    from app.models.models_quotation import QuotationNotification
    notif = db.query(QuotationNotification).filter(
        QuotationNotification.id == notification_id,
        QuotationNotification.user_id == current_user.user_id
    ).first()
    if not notif:
        raise HTTPException(status_code=404, detail="Notification not found")
    notif.is_read = True
    db.commit()
    return {"message": "Notification marked as read"}

@router.get("/export-csv")
def export_quotations_csv(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Exports a detailed CSV report containing all RFQs and every bank quote submitted."""
    rfqs = crud_quotation.get_requests(db, customer_id=current_user.customer_id)
    
    output = io.BytesIO()
    output.write(b'\xef\xbb\xbf')
    text_wrapper = io.TextIOWrapper(output, encoding='utf-8', newline='')
    writer = csv.writer(text_wrapper, lineterminator='\r\n')
    
    # Write detailed headers
    writer.writerow([
        "RFQ Reference", "Type", "Direction", "Amount", "Buy Currency", "Sell Currency",
        "Value Date", "RFQ Status", "Master Quotation Base", "Max Tolerance %",
        "Bank Name", "Bank Base Type", "Document Visible", "Submitted Base Rate",
        "Bank Fee Total", "All-In Effective Rate", "Submission Time", "Is Winner",
        "Created By", "Created At"
    ])
    
    for rfq in rfqs:
        try:
            res_data = get_rfq_results(rfq.id, db, current_user)
            results = res_data.get("results", [])
            winner_bank_id = res_data.get("winner_bank_id")
            creator_name = (rfq.creator.email.split('@')[0] if (rfq.creator and rfq.creator.email) else getattr(rfq, 'creator_name', 'End User')) or "End User"
            
            if not results:
                writer.writerow([
                    rfq.ref_no, rfq.type, rfq.direction or "", rfq.amount or 0,
                    rfq.buy_currency or "", rfq.sell_currency or "", rfq.value_date or "",
                    rfq.status, rfq.quotation_base or "Execution", rfq.max_tolerance_percent or "",
                    "No Banks Assigned", "", "", "", "", "", "", "NO",
                    creator_name, rfq.created_at.isoformat() if (rfq.created_at and hasattr(rfq.created_at, 'isoformat')) else str(rfq.created_at or "")
                ])
            else:
                for b in results:
                    is_win = "YES" if (winner_bank_id and b.get("bank_id") == winner_bank_id) else "NO"
                    sub_time = b.get("submitted_at")
                    if sub_time and hasattr(sub_time, 'isoformat'):
                        sub_time = sub_time.isoformat()
                    else:
                        sub_time = str(sub_time or "")

                    created_at_str = rfq.created_at
                    if created_at_str and hasattr(created_at_str, 'isoformat'):
                        created_at_str = created_at_str.isoformat()
                    else:
                        created_at_str = str(created_at_str or "")

                    writer.writerow([
                        rfq.ref_no, rfq.type, rfq.direction or "", rfq.amount or 0,
                        rfq.buy_currency or "", rfq.sell_currency or "", rfq.value_date or "",
                        rfq.status, rfq.quotation_base or "Execution", rfq.max_tolerance_percent or "",
                        b.get("bank_name", ""), b.get("quotation_base", ""),
                        "YES" if b.get("is_document_visible") else "NO",
                        b.get("price") if b.get("price") is not None else "No Quote",
                        b.get("bank_fee_total") if b.get("bank_fee_total") is not None else "",
                        b.get("finalPrice") if b.get("finalPrice") is not None else "",
                        sub_time,
                        is_win, creator_name, created_at_str
                    ])
        except Exception as rfq_err:
            logger.warning(f"Skipping RFQ {rfq.id} in CSV export due to error: {rfq_err}")
            continue

    text_wrapper.flush()
    filename = f"quotation_detailed_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        content=output.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )
