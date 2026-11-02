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

    notification_id = Column(Integer, ForeignKey("user_notifications.id", ondelete="CASCADE"), nullable=True, index=True)
    subscription_id = Column(Integer, ForeignKey("push_subscriptions.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    dealer_email = Column(String(255), nullable=True, index=True)
    dealer_id = Column(Integer, ForeignKey("quotation_bank_dealers.id", ondelete="CASCADE"), nullable=True, index=True)
    payload_json = Column(Text, nullable=True, comment="Direct JSON payload for dealer alerts (no user_notifications row)")
    dedupe_key = Column(String(128), unique=True, nullable=True, index=True)
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
    dealer = relationship("QuotationBankDealer")

    __table_args__ = (
        Index("uq_notification_delivery_pair", "notification_id", "subscription_id", unique=True),
        Index("ix_notification_deliveries_due", "status", "next_attempt_at"),
    )


class DealerAlertReminder(BaseModel):
    """
    N7b: Tracks standing reminder status and setup links sent to bank dealers
    inviting them to enable mobile RFQ push alerts.
    """
    __tablename__ = "dealer_alert_reminders"

    email = Column(String(255), nullable=False, unique=True, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="SET NULL"), nullable=True, index=True)
    setup_token = Column(String(64), nullable=False, unique=True, index=True)
    reminders_sent = Column(Integer, nullable=False, default=0, server_default="0")
    last_reminded_at = Column(DateTime(timezone=True), nullable=True)
    link_opened_at = Column(DateTime(timezone=True), nullable=True)
    opted_out = Column(Boolean, nullable=False, default=False, server_default="false")
    otp_code = Column(String(255), nullable=True)
    otp_expires_at = Column(DateTime(timezone=True), nullable=True)
    otp_failed_attempts = Column(Integer, nullable=False, default=0, server_default="0")

    bank = relationship("Bank")


class UserMobilePairToken(BaseModel):
    """
    Tracks one-time QR pairing tokens used to link a mobile device
    to an End User or Corporate Admin account with password verification.
    """
    __tablename__ = "user_mobile_pair_tokens"

    pairing_token = Column(String(64), nullable=False, unique=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_paired = Column(Boolean, nullable=False, default=False, server_default="false")
    paired_at = Column(DateTime(timezone=True), nullable=True)
    device_label = Column(String(120), nullable=True)
    platform = Column(String(20), nullable=True)


class DealerMobilePairToken(BaseModel):
    """
    N7: Ephemeral 15-minute QR pairing sessions for Bank Dealers.
    Generated dynamically on the Trading Desk screen. Zero persistent attack surface.
    """
    __tablename__ = "dealer_mobile_pair_tokens"

    pairing_token = Column(String(64), nullable=False, unique=True, index=True)
    email = Column(String(255), nullable=False, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="SET NULL"), nullable=True, index=True)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_paired = Column(Boolean, nullable=False, default=False, server_default="false")
    paired_at = Column(DateTime(timezone=True), nullable=True)
    device_label = Column(String(120), nullable=True)
    platform = Column(String(20), nullable=True)
    otp_code = Column(String(255), nullable=True)
    otp_expires_at = Column(DateTime(timezone=True), nullable=True)
    otp_failed_attempts = Column(Integer, nullable=False, default=0, server_default="0")
    last_otp_sent_at = Column(DateTime(timezone=True), nullable=True)

    bank = relationship("Bank")


class UserWebAuthnCredential(BaseModel):
    """
    Stores WebAuthn / Passkey public keys (Face ID, Touch ID, Android Fingerprint)
    for permanent platform users (End Users, Corporate Admins, Checkers, System Owners).
    Enables instant biometric login when opening mobile push notifications.
    """
    __tablename__ = "user_webauthn_credentials"

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=True, index=True)
    credential_id = Column(String(512), nullable=False, unique=True, index=True,
                           comment="Base64URL-encoded WebAuthn credential ID")
    public_key_spki = Column(Text, nullable=False,
                             comment="Base64URL-encoded SubjectPublicKeyInfo (DER) public key")
    algorithm = Column(Integer, nullable=False, default=-7, server_default="-7",
                       comment="COSE algorithm (-7 for ES256/P-256, -257 for RS256)")
    sign_count = Column(Integer, nullable=False, default=0, server_default="0")
    device_id = Column(String(64), nullable=True, index=True,
                       comment="Frontend device UUID matching user_devices.device_id")
    device_label = Column(String(120), nullable=True)
    platform = Column(String(20), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True, server_default="true", index=True)
    last_used_at = Column(DateTime(timezone=True), nullable=True)

    user = relationship("User", foreign_keys=[user_id])



