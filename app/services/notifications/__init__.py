# app/services/notifications/__init__.py
"""
Notification core (Notification Programme, quotation-first track).

Usage:
    from app.services.notifications import emit, EventCode
    emit(db, EventCode.RFQ_APPROVED, rfq, actor_user_id=current_user.user_id)

The legacy app.services.notification_service.notify() remains in use by the
Issuance module until it is migrated onto emit().
"""

from app.services.notifications.catalog import EventCode, Urgency
from app.services.notifications.emit import emit

__all__ = ["emit", "EventCode", "Urgency"]
