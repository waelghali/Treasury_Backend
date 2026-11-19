# app/api/v1/endpoints/user_feedback.py
"""
User Feedback API Endpoints for System Owners and Corporate Admins.
Provides review, filtering, and status updates on user feedback and feature requests.
"""

from typing import Any, List, Optional
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime
import os

from app.database import get_db
from app.models.models import Customer, User
from app.models.models_feedback import UserFeedback, FeedbackType, FeedbackSentiment, FeedbackStatus
from app.core.security import get_current_user, TokenData

router = APIRouter()


class FeedbackUpdateRequest(BaseModel):
    status: Optional[str] = None
    resolution_notes: Optional[str] = None


class FeedbackResponse(BaseModel):
    id: int
    customer_id: int
    user_id: int
    user_email: Optional[str]
    feedback_type: str
    sentiment: str
    message: str
    ai_summary: Optional[str]
    status: str
    resolution_notes: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]



from app.core.honeypot import verify_honeypot_sync

class FeedbackCreateRequest(BaseModel):
    message: str
    feedback_type: Optional[str] = "GENERAL_FEEDBACK"
    sentiment: Optional[str] = "NEUTRAL"
    ai_summary: Optional[str] = None
    website_url: Optional[str] = None


@router.post("/", response_model=FeedbackResponse)
async def create_user_feedback(
    payload: FeedbackCreateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
) -> Any:
    """
    Submit a new user feedback, issue report, or AI response evaluation.
    Tags Free Trial feedback automatically and alerts System Owner if a trial user hits friction.
    """
    from app.core.trial_guard import is_trial_customer

    user = db.query(User).filter(User.id == current_user.user_id).first()
    u_email = user.email if user else (current_user.email if hasattr(current_user, 'email') else None)

    # Perimeter Anti-Bot Honeypot Defense
    verify_honeypot_sync(payload.website_url, context="user_feedback", legitimate_values=[u_email])
    
    f_type = payload.feedback_type.upper() if payload.feedback_type else "GENERAL_FEEDBACK"
    sentiment_val = payload.sentiment.upper() if payload.sentiment else "NEUTRAL"
    in_trial = bool(current_user.customer_id and is_trial_customer(db, current_user.customer_id))

    summary_val = payload.ai_summary
    if in_trial:
        cust = db.query(Customer).filter(Customer.id == current_user.customer_id).first()
        co_label = cust.name if cust else f"Customer #{current_user.customer_id}"
        prefix = f"[FREE TRIAL · {co_label}]"
        if not summary_val:
            summary_val = prefix
        elif "[FREE TRIAL" not in summary_val:
            summary_val = f"{prefix} {summary_val}"

    entry = UserFeedback(
        customer_id=current_user.customer_id,
        user_id=current_user.user_id,
        user_email=u_email,
        feedback_type=f_type,
        sentiment=sentiment_val,
        message=payload.message,
        ai_summary=summary_val,
        status="NEW"
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)

    # Instant Rescue Alert to System Owner if a Free Trial evaluator reports confusion or friction
    if in_trial and (sentiment_val == "NEGATIVE" or f_type in ("USABILITY_PAIN_POINT", "BUG_REPORT")):
        try:
            from app.core.email_service import get_global_email_settings, send_email
            from app.services.unified_email_builder import build_standard_email_html

            email_settings = get_global_email_settings()
            alert_to = (
                os.getenv("ADMIN_LEAD_ALERT_EMAIL")
                or getattr(email_settings, "sender_email", None)
                or os.getenv("SMTP_FROM_EMAIL")
            )
            if alert_to and "@" in alert_to:
                cust = db.query(Customer).filter(Customer.id == current_user.customer_id).first()
                co_name = cust.name if cust else f"Trial #{current_user.customer_id}"
                subj = f"[Trial Rescue Alert] {co_name} shared feedback ({sentiment_val})"
                html_body = build_standard_email_html(
                    title="Free Trial Evaluator Feedback Alert",
                    body_html=(
                        f"<p>A Free Trial evaluator just reported feedback that may need a quick personal check-in:</p>"
                        f"<ul>"
                        f"<li><strong>Company:</strong> {co_name}</li>"
                        f"<li><strong>User Email:</strong> {u_email or 'N/A'}</li>"
                        f"<li><strong>Category:</strong> {f_type} ({sentiment_val})</li>"
                        f"</ul>"
                        f"<blockquote style='padding:12px 16px; background:#f8fafc; border-left:3px solid #f59e0b;'>"
                        f"{payload.message}"
                        f"</blockquote>"
                    ),
                )
                background_tasks.add_task(
                    send_email,
                    None,
                    [alert_to],
                    subj,
                    html_body,
                    {},
                    email_settings,
                )
        except Exception:
            pass

    return entry.to_dict()


@router.get("/", response_model=List[FeedbackResponse])
def get_user_feedbacks(
    status: Optional[str] = Query(None, description="Filter by status (NEW, IN_REVIEW, RESOLVED, ARCHIVED)"),
    feedback_type: Optional[str] = Query(None, description="Filter by type (FEATURE_REQUEST, BUG_REPORT, USABILITY_PAIN_POINT, GENERAL_FEEDBACK)"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
) -> Any:
    """
    Retrieve user feedback entries. System Owners can view all; Corporate Admins view their customer's.
    """
    query = db.query(UserFeedback)
    
    role_val = str(getattr(current_user.role, 'value', current_user.role)).upper()
    if role_val != "SYSTEM_OWNER":
        query = query.filter(UserFeedback.customer_id == current_user.customer_id)
        
    if status:
        query = query.filter(UserFeedback.status == status.upper())
    if feedback_type:
        query = query.filter(UserFeedback.feedback_type == feedback_type.upper())
        
    entries = query.order_by(UserFeedback.created_at.desc()).limit(limit).all()
    return [e.to_dict() for e in entries]


@router.patch("/{feedback_id}", response_model=FeedbackResponse)
def update_feedback_status(
    feedback_id: int,
    payload: FeedbackUpdateRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
) -> Any:
    """
    Update feedback status and resolution notes.
    """
    entry = db.query(UserFeedback).filter(UserFeedback.id == feedback_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="Feedback entry not found.")
        
    role_val = str(getattr(current_user.role, 'value', current_user.role)).upper()
    if role_val != "SYSTEM_OWNER" and entry.customer_id != current_user.customer_id:
        raise HTTPException(status_code=403, detail="Not authorized to update this feedback entry.")
        
    if payload.status:
        entry.status = payload.status.upper()
    if payload.resolution_notes is not None:
        entry.resolution_notes = payload.resolution_notes
        
    entry.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(entry)
    return entry.to_dict()
