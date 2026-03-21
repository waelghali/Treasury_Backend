# app/models/models_notification.py
# General-purpose user notification model (cross-module)

from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.models import BaseModel


class UserNotification(BaseModel):
    """Per-user, event-driven notification. Supports both in-app bell + optional email."""
    __tablename__ = "user_notifications"

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    module = Column(String, nullable=False, index=True,
                    comment="ISSUANCE, CUSTODY, QUOTATION, SYSTEM")
    event_type = Column(String, nullable=False, index=True,
                        comment="e.g. REQUEST_SUBMITTED, REQUEST_APPROVED, LG_EXPIRY_WARNING")
    title = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    link = Column(String, nullable=True, comment="Deep link to relevant page")
    is_read = Column(Boolean, default=False, index=True)

    # Optional: who/what triggered this
    actor_user_id = Column(Integer, ForeignKey("users.id"), nullable=True,
                           comment="User who performed the action that triggered this notification")
    reference_id = Column(Integer, nullable=True,
                          comment="ID of the related object (request, LG record, etc.)")
    reference_type = Column(String, nullable=True,
                            comment="Type of related object: IssuanceRequest, LGRecord, etc.")

    # Relationships
    user = relationship("User", foreign_keys=[user_id])
    actor = relationship("User", foreign_keys=[actor_user_id])
