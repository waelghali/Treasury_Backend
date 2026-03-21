# app/api/v1/endpoints/notification_endpoints.py
"""
General-purpose notification API.
Serves UserNotification records for the bell icon across all modules.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.database import get_db
from app.core.security import get_current_active_user, TokenData
from app.models.models_notification import UserNotification

router = APIRouter()


@router.get("/")
def get_my_notifications(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Fetches the 30 most recent notifications for the logged-in user."""
    return db.query(UserNotification).filter(
        UserNotification.user_id == current_user.user_id
    ).order_by(desc(UserNotification.created_at)).limit(30).all()


@router.get("/unread-count")
def get_unread_count(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Returns the count of unread notifications for badge display."""
    count = db.query(UserNotification).filter(
        UserNotification.user_id == current_user.user_id,
        UserNotification.is_read == False
    ).count()
    return {"count": count}


@router.patch("/{notification_id}/read")
def mark_notification_as_read(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Marks a specific notification as read."""
    notif = db.query(UserNotification).filter(
        UserNotification.id == notification_id,
        UserNotification.user_id == current_user.user_id
    ).first()
    if not notif:
        raise HTTPException(status_code=404, detail="Notification not found")
    notif.is_read = True
    db.commit()
    return {"message": "Notification marked as read"}


@router.patch("/mark-all-read")
def mark_all_as_read(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_active_user)
):
    """Marks all notifications as read for the current user."""
    db.query(UserNotification).filter(
        UserNotification.user_id == current_user.user_id,
        UserNotification.is_read == False
    ).update({"is_read": True})
    db.commit()
    return {"message": "All notifications marked as read"}
