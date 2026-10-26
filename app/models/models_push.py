# app/models/models_push.py
"""
Web Push channel tables (Notifications Q3).

push_subscriptions       - one row per browser/phone that granted permission
                           for a given user. The endpoint URL is the device's
                           mailbox at Apple/Google/Mozilla; it is unique.
notification_deliveries  - outbox: one row per (notification, subscription).
                           A background dispatcher sends rows and records the
                           result, so request handlers never call the network.
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Index, Text
from sqlalchemy.orm import relationship

from app.models import BaseModel


class PushSubscription(BaseModel):
    # NOTE: this table already exists since 2026-10-14 (ledger "Phase 9 / 9.2").
    # Q3 only ADDS columns; legacy columns are mapped so nothing is lost.
    __tablename__ = "push_subscriptions"

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True,
                     comment="Internal user owner (NULL for bank-dealer rows, reserved for N7)")
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=True, index=True,
                         comment="Owner's customer at subscribe time (lets admins revoke devices later)")
    device_id = Column(String(64), nullable=True, index=True,
                       comment="Frontend device UUID (same value as user_devices.device_id)")
    device_label = Column(String(120), nullable=True, comment="e.g. 'iPhone/iPad - Safari'")
    platform = Column(String(20), nullable=True, comment="ios | android | desktop")

    endpoint = Column(Text, nullable=False, unique=True, index=True)
    p256dh = Column(String(255), nullable=False)
    auth = Column(String(255), nullable=False)

    is_active = Column(Boolean, nullable=False, default=True, server_default="true", index=True)
    revoked_reason = Column(String(32), nullable=True,
                            comment="USER_REMOVED | SIGNED_OUT | GONE | REASSIGNED | ADMIN_REVOKED | LEGACY_KEYS")
    failure_count = Column(Integer, nullable=False, default=0, server_default="0")
    last_success_at = Column(DateTime(timezone=True), nullable=True)
    last_failure_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(String(255), nullable=True)

    # Legacy (Phase 9 / 9.2) columns
    user_agent = Column(String(500), nullable=True)
    device_type = Column(String(50), nullable=False, default="web")
    last_notified_at = Column(DateTime, nullable=True)
    dealer_id = Column(Integer, ForeignKey("quotation_bank_dealers.id"), nullable=True, index=True)
    dealer_email = Column(String(255), nullable=True, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=True, index=True)

    user = relationship("User", foreign_keys=[user_id])


class NotificationDelivery(BaseModel):
    __tablename__ = "notification_deliveries"

    notification_id = Column(Integer, ForeignKey("user_notifications.id", ondelete="CASCADE"), nullable=False, index=True)
    subscription_id = Column(Integer, ForeignKey("push_subscriptions.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    channel = Column(String(16), nullable=False, default="PUSH", server_default="PUSH")

    status = Column(String(16), nullable=False, default="PENDING", server_default="PENDING",
                    comment="PENDING | SENDING | SENT | FAILED | EXPIRED")
    attempts = Column(Integer, nullable=False, default=0, server_default="0")
    next_attempt_at = Column(DateTime(timezone=True), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(String(255), nullable=True)

    notification = relationship("UserNotification")
    subscription = relationship("PushSubscription")

    __table_args__ = (
        Index("uq_notification_delivery_pair", "notification_id", "subscription_id", unique=True),
        Index("ix_notification_deliveries_due", "status", "next_attempt_at"),
    )
