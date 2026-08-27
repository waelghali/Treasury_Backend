# app/models/models_inbox.py
"""
Smart Inbox Models
Data structures for email-based ingestion, classification, multi-signal scoring,
system-initiated outbound requests, and per-bank scheduling.
"""

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime,
    ForeignKey, Date, Numeric, JSON
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.models.models import BaseModel, Base


class InboxItem(BaseModel):
    """
    A single email received in the customer's mailbox.
    Core entity of the Smart Inbox feature.
    """
    __tablename__ = 'inbox_items'

    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)
    
    # Email metadata
    message_id = Column(String, nullable=False, index=True,
                        comment="Email Message-ID header — primary dedup key")
    sender_email = Column(String, nullable=False, index=True)
    sender_domain = Column(String, nullable=True, index=True,
                           comment="Extracted domain from sender email")
    subject = Column(String, nullable=True)
    body_text_encrypted = Column(Text, nullable=True,
                                 comment="Email body (plain text), encrypted at rest")
    received_at = Column(DateTime(timezone=True), nullable=False,
                         comment="Email Date header")
    
    # Sender identification
    matched_bank_id = Column(Integer, ForeignKey("banks.id"), nullable=True,
                             comment="Bank matched via email_domain lookup")
    is_trusted_sender = Column(Boolean, default=True,
                               comment="False if sender domain is generic/blacklisted")
    
    # Thread tracking
    in_reply_to = Column(String, nullable=True,
                         comment="In-Reply-To header — links to outbound request")
    outbound_request_id = Column(Integer, ForeignKey("inbox_outbound_requests.id"), nullable=True,
                                 comment="If this is a reply to a system-initiated request")
    
    # Classification
    classification = Column(String, default="UNCLASSIFIED", nullable=False, index=True,
                            comment="LG_POSITION_REPORT, BANK_STATEMENT, PROGRESS_REPORT, UNCLASSIFIED, etc.")
    classification_confidence = Column(String, default="LOW", nullable=False,
                                       comment="HIGH, MEDIUM, LOW")
    confidence_score = Column(Integer, default=0,
                              comment="Numeric score from classification engine")
    classification_signals = Column(JSON, nullable=True,
                                    comment="Breakdown of scoring signals for transparency")
    user_override_classification = Column(String, nullable=True,
                                          comment="If user reclassified, stores their choice")
    
    # Attachment info
    has_attachment = Column(Boolean, default=False)
    attachment_count = Column(Integer, default=0)
    primary_attachment_path = Column(String, nullable=True,
                                     comment="Storage path to primary attachment")
    primary_attachment_name = Column(String, nullable=True)
    primary_attachment_type = Column(String, nullable=True,
                                     comment="xlsx, csv, pdf, etc.")
    attachment_content_hash = Column(String, nullable=True, index=True,
                                     comment="SHA-256 hash of primary attachment for dedup")
    
    # Processing status
    status = Column(String, default="RECEIVED", nullable=False, index=True,
                    comment="RECEIVED, CLASSIFIED, PARSE_ERROR, CONFIRMED, ACTIONED, ARCHIVED")
    action_summary = Column(Text, nullable=True,
                            comment="Human-readable summary of what the system proposes to do")
    error_message = Column(Text, nullable=True,
                           comment="Error details if status=PARSE_ERROR")
    
    # Action tracking
    actioned_at = Column(DateTime(timezone=True), nullable=True)
    actioned_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    action_reference_type = Column(String, nullable=True,
                                   comment="ReconciliationSession, BankStatement, etc.")
    action_reference_id = Column(Integer, nullable=True,
                                 comment="ID of the created entity after confirmation")
    
    # Dedup & Nil flags
    is_duplicate = Column(Boolean, default=False)
    duplicate_of_id = Column(Integer, ForeignKey("inbox_items.id"), nullable=True)
    is_nil_position = Column(Boolean, default=False, comment="True if email body states zero/NIL outstanding position")
    
    # Relationships
    customer = relationship("Customer")
    matched_bank = relationship("Bank")
    actioned_by = relationship("User")
    outbound_request = relationship("InboxOutboundRequest", back_populates="replies", foreign_keys=[outbound_request_id])
    attachments = relationship("InboxAttachment", back_populates="inbox_item",
                               cascade="all, delete-orphan")

    def __repr__(self):
        return f"<InboxItem(id={self.id}, sender='{self.sender_email}', class='{self.classification}', status='{self.status}')>"


class InboxAttachment(BaseModel):
    """Individual attachment on an inbox item (supports multi-attachment emails)."""
    __tablename__ = 'inbox_attachments'

    inbox_item_id = Column(Integer, ForeignKey("inbox_items.id"), nullable=False, index=True)
    file_name = Column(String, nullable=False)
    file_type = Column(String, nullable=True, comment="Extension: xlsx, csv, pdf")
    file_size_bytes = Column(Integer, nullable=True)
    storage_path = Column(String, nullable=True, comment="Storage path (local/GCS)")
    content_hash = Column(String, nullable=True, comment="SHA-256 for dedup")
    is_primary = Column(Boolean, default=False,
                        comment="True if this is the attachment selected for processing")
    
    inbox_item = relationship("InboxItem", back_populates="attachments")

    def __repr__(self):
        return f"<InboxAttachment(id={self.id}, name='{self.file_name}', item_id={self.inbox_item_id})>"


class InboxOutboundRequest(BaseModel):
    """
    A system-initiated email requesting data from an external party.
    Used for thread-matching when the reply arrives.
    """
    __tablename__ = 'inbox_outbound_requests'

    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=True)
    
    request_type = Column(String, nullable=False,
                          comment="LG_POSITION, BANK_STATEMENT, PROGRESS, etc.")
    request_reference = Column(String, nullable=True, index=True,
                               comment="Unique human-readable reference number (e.g. POS-20260825-0042)")
    
    # Email details
    sent_to_emails = Column(JSON, nullable=False, comment="List of recipient emails")
    subject = Column(String, nullable=False)
    message_id = Column(String, nullable=True, index=True,
                        comment="Outbound email Message-ID — used for In-Reply-To matching")
    
    # Context
    position_date = Column(Date, nullable=True, comment="For LG_POSITION: the requested position date")
    statement_period_start = Column(Date, nullable=True)
    statement_period_end = Column(Date, nullable=True)
    
    sent_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    sent_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True,
                             comment="Null if sent by scheduler (automatic)")
    
    # Response tracking
    is_replied = Column(Boolean, default=False)
    reply_received_at = Column(DateTime(timezone=True), nullable=True)
    
    # Schedule info
    is_scheduled = Column(Boolean, default=False,
                          comment="True if this was auto-generated by the scheduler")
    schedule_config_id = Column(Integer, ForeignKey("inbox_schedule_configs.id"), nullable=True)
    
    # Relationships
    customer = relationship("Customer")
    bank = relationship("Bank")
    sent_by = relationship("User")
    replies = relationship("InboxItem", back_populates="outbound_request", foreign_keys=[InboxItem.outbound_request_id])

    def __repr__(self):
        return f"<InboxOutboundRequest(id={self.id}, type='{self.request_type}', bank_id={self.bank_id}, replied={self.is_replied})>"


class InboxScheduleConfig(BaseModel):
    """
    Per-customer, per-bank scheduling configuration for outbound data requests.
    Corporate Admin configures these. Overrides global defaults.
    """
    __tablename__ = 'inbox_schedule_configs'

    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False)
    
    request_type = Column(String, nullable=False,
                          comment="LG_POSITION, BANK_STATEMENT")
    
    is_active = Column(Boolean, default=True)
    frequency = Column(String, default="MONTHLY", nullable=False,
                       comment="DAILY, WEEKLY, MONTHLY")
    day_of_month = Column(Integer, nullable=True,
                          comment="For MONTHLY: day to send (1-28)")
    day_of_week = Column(Integer, nullable=True,
                         comment="For WEEKLY: day (0=Monday, 6=Sunday)")
    
    recipient_emails = Column(JSON, nullable=True,
                              comment="Override recipient emails. If null, uses bank's default contacts.")
    custom_subject = Column(String, nullable=True,
                            comment="Custom subject line. If null, uses system default.")
    custom_body = Column(Text, nullable=True,
                         comment="Custom email body. If null, uses system template.")
    
    last_sent_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    customer = relationship("Customer")
    bank = relationship("Bank")

    def __repr__(self):
        return f"<InboxScheduleConfig(id={self.id}, customer_id={self.customer_id}, bank_id={self.bank_id}, freq='{self.frequency}')>"


class EmailClassificationHistory(BaseModel):
    """
    Learning table — stores confirmed classifications for future scoring.
    """
    __tablename__ = 'email_classification_history'

    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)
    sender_email = Column(String, nullable=False, index=True)
    sender_domain = Column(String, nullable=True, index=True)
    
    classification = Column(String, nullable=False)
    was_user_corrected = Column(Boolean, default=False,
                                comment="True if user changed the system's initial classification")
    original_classification = Column(String, nullable=True,
                                     comment="System's initial classification before user correction")
    
    attachment_column_signature = Column(String, nullable=True,
                                         comment="Hash of detected column names — for pattern matching")
    confidence_score = Column(Integer, nullable=True)
    confirmed_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    def __repr__(self):
        return f"<EmailClassificationHistory(id={self.id}, sender='{self.sender_email}', class='{self.classification}', corrected={self.was_user_corrected})>"
