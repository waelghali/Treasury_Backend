# app/models/models_feedback.py
"""
User Feedback & AI Problem Listener Model
Captures user feedback, feature requests, bug reports, and usability pain points
submitted via the AI Assistant, with full transparency and System Owner visibility.
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import relationship
import enum

from app.models.models import Base


class FeedbackType(str, enum.Enum):
    FEATURE_REQUEST = "FEATURE_REQUEST"
    BUG_REPORT = "BUG_REPORT"
    USABILITY_PAIN_POINT = "USABILITY_PAIN_POINT"
    GENERAL_FEEDBACK = "GENERAL_FEEDBACK"


class FeedbackSentiment(str, enum.Enum):
    POSITIVE = "POSITIVE"
    NEUTRAL = "NEUTRAL"
    NEGATIVE = "NEGATIVE"


class FeedbackStatus(str, enum.Enum):
    NEW = "NEW"
    IN_REVIEW = "IN_REVIEW"
    RESOLVED = "RESOLVED"
    ARCHIVED = "ARCHIVED"


class UserFeedback(Base):
    __tablename__ = "user_feedbacks"

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user_email = Column(String(255), nullable=True)
    
    feedback_type = Column(String(50), default="GENERAL_FEEDBACK", nullable=False, index=True)
    sentiment = Column(String(20), default="NEUTRAL", nullable=False)
    message = Column(Text, nullable=False)
    ai_summary = Column(Text, nullable=True)
    
    status = Column(String(30), default="NEW", nullable=False, index=True)
    resolution_notes = Column(Text, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "customer_id": self.customer_id,
            "user_id": self.user_id,
            "user_email": self.user_email,
            "feedback_type": self.feedback_type,
            "sentiment": self.sentiment,
            "message": self.message,
            "ai_summary": self.ai_summary,
            "status": self.status,
            "resolution_notes": self.resolution_notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
