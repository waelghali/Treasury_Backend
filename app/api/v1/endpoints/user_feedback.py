# app/api/v1/endpoints/user_feedback.py
"""
User Feedback API Endpoints for System Owners and Corporate Admins.
Provides review, filtering, and status updates on user feedback and feature requests.
"""

from typing import Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime

from app.database import get_db
from app.models.models import User
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



class FeedbackCreateRequest(BaseModel):
    message: str
    feedback_type: Optional[str] = "GENERAL_FEEDBACK"
    sentiment: Optional[str] = "NEUTRAL"
    ai_summary: Optional[str] = None


@router.post("/", response_model=FeedbackResponse)
def create_user_feedback(
    payload: FeedbackCreateRequest,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_user)
) -> Any:
    """
    Submit a new user feedback, issue report, or AI response evaluation.
    """
    user = db.query(User).filter(User.id == current_user.user_id).first()
    u_email = user.email if user else (current_user.email if hasattr(current_user, 'email') else None)
    
    f_type = payload.feedback_type.upper() if payload.feedback_type else "GENERAL_FEEDBACK"
    sentiment_val = payload.sentiment.upper() if payload.sentiment else "NEUTRAL"
    
    entry = UserFeedback(
        customer_id=current_user.customer_id,
        user_id=current_user.user_id,
        user_email=u_email,
        feedback_type=f_type,
        sentiment=sentiment_val,
        message=payload.message,
        ai_summary=payload.ai_summary,
        status="NEW"
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
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
