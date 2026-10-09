# app/models_quotation.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey, Text, Index, Date
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.models import BaseModel, Base

class QuotationBankEntity(Base):
    """Junction table mapping a QuotationBank to specific CustomerEntities."""
    __tablename__ = "quotation_bank_entities"
    id = Column(Integer, primary_key=True, index=True)
    quotation_bank_id = Column(Integer, ForeignKey("quotation_banks.id", ondelete="CASCADE"), nullable=False, index=True)
    entity_id = Column(Integer, ForeignKey("customer_entities.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    quotation_bank = relationship("QuotationBank", back_populates="entity_associations")
    entity = relationship("CustomerEntity")

class QuotationBank(BaseModel):
    """Link table allowing a Customer to configure which core Banks they want to receive quotations, and adding custom emails per bank."""
    __tablename__ = "quotation_banks"
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="CASCADE"), nullable=False)
    trade_type = Column(String, default="BOTH", comment="'FX_SPOT', 'TBILL', or 'BOTH'")
    entity_scope = Column(String(30), default="ALL_ENTITIES", nullable=False, comment="'ALL_ENTITIES' or 'SPECIFIC_ENTITIES'")
    emails = Column(Text, nullable=False, comment="Comma-separated emails for this specific customer's counterparty list")
    contacts = Column(JSONB, default=list, nullable=True, comment="Structured contacts list: [{'email': '...', 'name': '...', 'role': 'EXECUTION'|'VIEW_ONLY'}]")
    authorized_contact_email = Column(String(255), nullable=True, comment="Designated bank officer email authorized to request roster updates")
    authorized_contact_name = Column(String(255), nullable=True, comment="Name or title of authorized bank governance officer")
    handshake_confirmed_at = Column(DateTime(timezone=True), nullable=True, comment="Timestamp when bank governance handshake was confirmed")

    customer = relationship("Customer")
    bank = relationship("Bank")
    entity_associations = relationship("QuotationBankEntity", back_populates="quotation_bank", cascade="all, delete-orphan")


class QuotationBankContactInvitation(Base):
    """Staging table for pending counterparty dealer invitations and handshakes."""
    __tablename__ = "quotation_bank_contact_invitations"
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="CASCADE"), nullable=False, index=True)
    quotation_bank_id = Column(Integer, ForeignKey("quotation_banks.id", ondelete="CASCADE"), nullable=True)
    email = Column(String(255), nullable=False)
    title = Column(String(255), nullable=True)
    role = Column(String(50), default="EXECUTION")
    token = Column(String(255), nullable=False, unique=True, index=True)
    status = Column(String(30), default="PENDING")  # 'PENDING', 'ACCEPTED', 'REVOKED'
    invited_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    accepted_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    customer = relationship("Customer")
    bank = relationship("Bank")
    quotation_bank = relationship("QuotationBank")


class QuotationRequest(BaseModel):
    """Core RFQ configuration for FX Spot and T-Bills."""
    __tablename__ = "quotation_rfqs"
    id = Column(String, primary_key=True, comment="UUID string")
    ref_no = Column(String, unique=True, index=True, nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    
    type = Column(String, default="FX_SPOT", comment="'FX_SPOT' or 'TBILL'")
    direction = Column(String, nullable=True, comment="'Buy' or 'Sell'")
    value_date = Column(String, nullable=True)
    amount = Column(Float, nullable=True)
    min_ticket_amount = Column(Float, nullable=True)
    buy_currency = Column(String, nullable=True)
    sell_currency = Column(String, nullable=True)
    
    settlement_date_start = Column(String, nullable=True)
    settlement_date_end = Column(String, nullable=True)
    maturity_date_start = Column(String, nullable=True)
    maturity_date_end = Column(String, nullable=True)
    eval_rate = Column(Float, nullable=True)
    
    window_start = Column(DateTime(timezone=True), nullable=False)
    window_end = Column(DateTime(timezone=True), nullable=False)
    quotation_base = Column(String, nullable=True, comment="'Execution' or 'Indicative'")
    max_tolerance_percent = Column(Float, nullable=True, comment="Max allowed % deviation for Execution rates vs Indicative benchmark")
    allow_alternative_value_date = Column(Boolean, default=False, nullable=False, comment="Whether counterparties are permitted to propose an alternative value date")
    document_path = Column(Text, nullable=True)
    status = Column(String, default="PENDING", comment="'PENDING_APPROVAL', 'NEEDS_REVISION', 'PENDING', 'OPEN', 'EVALUATING', 'COMPLETED', 'REJECTED', 'CANCEL_REQUESTED', 'CANCELLED'")
    token_validity_hours = Column(Integer, default=24, comment="Hours the bank link remains valid after window_end")
    parent_rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="SET NULL"), nullable=True, index=True, comment="Original RFQ if this is a re-tender")
    entity_id = Column(Integer, ForeignKey("customer_entities.id", ondelete="SET NULL"), nullable=True, index=True, comment="Customer entity/subsidiary requesting the RFQ")
    admin_revision_notes = Column(Text, nullable=True, comment="Notes from Corporate Admin when returned for revision")
    user_revision_notes = Column(Text, nullable=True, comment="Notes from requestor when revising RFQ")
    admin_reviewed_at = Column(DateTime(timezone=True), nullable=True)
    cancellation_reason = Column(String(255), nullable=True, comment="Internal reason selected by end user for cancellation")
    cancellation_notes = Column(Text, nullable=True, comment="Additional context notes provided by requestor")
    cancellation_requested_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    cancellation_requested_at = Column(DateTime(timezone=True), nullable=True)
    cancelled_at = Column(DateTime(timezone=True), nullable=True)
    internal_notes = Column(Text, nullable=True, comment="Internal notes from requestor (e.g. related payments, invoices)")
    comments_to_banks = Column(Text, nullable=True, comment="Special instructions or comments visible to participating banks")
    scheduled_release_at = Column(DateTime(timezone=True), nullable=True, comment="Future scheduled time for bank email dispatch")
    scheduled_release_job_id = Column(String(100), nullable=True, comment="APScheduler job ID for scheduled release")
    is_dispatched = Column(Boolean, default=False, nullable=False, comment="Whether quotation invitation emails have been dispatched to banks")
    dispatched_at = Column(DateTime(timezone=True), nullable=True, comment="Timestamp when quotation invitation emails were dispatched")
    acceptance_timeout_seconds = Column(Integer, nullable=True)
    acceptance_timeout_action = Column(String(50), nullable=True, comment="'AUTO_ACCEPT' or 'AUTO_REJECT'")
    acceptance_deadline = Column(DateTime(timezone=True), nullable=True)
    acceptance_status = Column(String(50), nullable=True, comment="'PENDING', 'ACCEPTED', 'REJECTED', 'AUTO_ACCEPTED', 'AUTO_REJECTED', 'INDICATIVE_COMPLETED'")
    acceptance_resolved_at = Column(DateTime(timezone=True), nullable=True)
    acceptance_resolved_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    market_benchmark_snapshot = Column(JSONB, nullable=True, comment="Frozen market benchmark snapshot at trade acceptance")

    # Acceptance Delegation
    delegated_to_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, comment="Corporate colleague authorized to accept/reject deal on behalf of maker")
    delegated_at = Column(DateTime(timezone=True), nullable=True)
    delegated_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    # Winning terms & Zero-Knowledge encryption
    winner_bank_id = Column(Integer, nullable=True)
    winner_bank_name = Column(String(255), nullable=True)
    _db_winner_rate = Column("winner_rate", Float, nullable=True)
    encrypted_winner_rate = Column(String(255), nullable=True)
    _db_saved_vs_avg = Column("saved_vs_avg", Float, nullable=True)
    encrypted_saved_vs_avg = Column(String(255), nullable=True)

    customer = relationship("Customer")
    entity = relationship("CustomerEntity")
    creator = relationship("User", foreign_keys=[created_by_user_id])
    cancellation_requestor = relationship("User", foreign_keys=[cancellation_requested_by])
    delegate = relationship("User", foreign_keys=[delegated_to_user_id])
    delegator = relationship("User", foreign_keys=[delegated_by_user_id])
    acceptance_resolved_by = relationship("User", foreign_keys=[acceptance_resolved_by_user_id])
    assignments = relationship("QuotationBankAssignment", back_populates="rfq", cascade="all, delete-orphan")
    legs = relationship("QuotationLeg", back_populates="rfq", cascade="all, delete-orphan", order_by="QuotationLeg.leg_index")
    parent_rfq = relationship("QuotationRequest", remote_side=[id], backref="re_tenders")

    @property
    def winner_rate(self):
        if hasattr(self, '_resolved_winner_rate') and self._resolved_winner_rate is not None:
            return self._resolved_winner_rate
        if self._db_winner_rate is not None:
            return self._db_winner_rate
        if not self.encrypted_winner_rate:
            return None
        try:
            from sqlalchemy.orm import object_session
            session = object_session(self)
            cust_id = self.customer_id
            if session and cust_id:
                from app.services.tenant_key_service import tenant_key_service
                dek = tenant_key_service.get_or_create_tenant_dek(session, cust_id)
                from app.core.security_crypto import decrypt_field
                val = decrypt_field(self.encrypted_winner_rate, dek, field_context="winner_rate")
                if val is not None:
                    self._resolved_winner_rate = float(val)
                    return self._resolved_winner_rate
        except Exception:
            pass
        return None

    @winner_rate.setter
    def winner_rate(self, value):
        self._resolved_winner_rate = float(value) if value is not None else None
        if value is None:
            self._db_winner_rate = None
            self.encrypted_winner_rate = None

    @property
    def saved_vs_avg(self):
        if hasattr(self, '_resolved_saved_vs_avg') and self._resolved_saved_vs_avg is not None:
            return self._resolved_saved_vs_avg
        if self._db_saved_vs_avg is not None:
            return self._db_saved_vs_avg
        if not self.encrypted_saved_vs_avg:
            return None
        try:
            from sqlalchemy.orm import object_session
            session = object_session(self)
            cust_id = self.customer_id
            if session and cust_id:
                from app.services.tenant_key_service import tenant_key_service
                dek = tenant_key_service.get_or_create_tenant_dek(session, cust_id)
                from app.core.security_crypto import decrypt_field
                val = decrypt_field(self.encrypted_saved_vs_avg, dek, field_context="saved_vs_avg")
                if val is not None:
                    self._resolved_saved_vs_avg = float(val)
                    return self._resolved_saved_vs_avg
        except Exception:
            pass
        return None

    @saved_vs_avg.setter
    def saved_vs_avg(self, value):
        self._resolved_saved_vs_avg = float(value) if value is not None else None
        if value is None:
            self._db_saved_vs_avg = None
            self.encrypted_saved_vs_avg = None

    @property
    def delegated_to_name(self):
        if self.delegate:
            first_n = getattr(self.delegate, 'first_name', '') or ''
            last_n = getattr(self.delegate, 'last_name', '') or ''
            name = f"{first_n} {last_n}".strip()
            return name or self.delegate.email
        return None

    @property
    def acceptance_resolved_by_name(self):
        if self.acceptance_resolved_by:
            first_n = getattr(self.acceptance_resolved_by, 'first_name', '') or ''
            last_n = getattr(self.acceptance_resolved_by, 'last_name', '') or ''
            name = f"{first_n} {last_n}".strip()
            return name or self.acceptance_resolved_by.email
        return None

    @property
    def acceptance_resolved_by_email(self):
        if self.acceptance_resolved_by:
            return self.acceptance_resolved_by.email
        return None

    @property
    def approved_by_name(self):
        if hasattr(self, '_approved_by_name'):
            return self._approved_by_name
        from sqlalchemy.orm import object_session
        session = object_session(self)
        if not session or not self.admin_reviewed_at:
            return None
        try:
            from app.models.models import AuditLog
            logs = session.query(AuditLog).filter(
                AuditLog.action_type.in_(["QUOTATION_RFQ_APPROVED", "QUOTATION_RFQ_APPROVED_SCHEDULED"]),
                AuditLog.customer_id == self.customer_id
            ).order_by(AuditLog.id.desc()).all()
            for l in logs:
                d = l.details or {}
                if str(d.get("rfq_id")) == str(self.id):
                    if l.user:
                        first_n = getattr(l.user, 'first_name', '') or ''
                        last_n = getattr(l.user, 'last_name', '') or ''
                        name = f"{first_n} {last_n}".strip()
                        return name or l.user.email
                    app_email = d.get("approved_by_email")
                    if app_email:
                        return app_email
        except Exception:
            pass
        return None

    @approved_by_name.setter
    def approved_by_name(self, value):
        self._approved_by_name = value

    @property
    def approved_by_email(self):
        if hasattr(self, '_approved_by_email'):
            return self._approved_by_email
        from sqlalchemy.orm import object_session
        session = object_session(self)
        if not session or not self.admin_reviewed_at:
            return None
        try:
            from app.models.models import AuditLog
            logs = session.query(AuditLog).filter(
                AuditLog.action_type.in_(["QUOTATION_RFQ_APPROVED", "QUOTATION_RFQ_APPROVED_SCHEDULED"]),
                AuditLog.customer_id == self.customer_id
            ).order_by(AuditLog.id.desc()).all()
            for l in logs:
                d = l.details or {}
                if str(d.get("rfq_id")) == str(self.id):
                    return d.get("approved_by_email")
        except Exception:
            pass
        return None

    @approved_by_email.setter
    def approved_by_email(self, value):
        self._approved_by_email = value

    @property
    def release_docs_to_winner_only(self) -> bool:
        if not self.document_path:
            return False
        try:
            import json
            data = json.loads(self.document_path)
            if isinstance(data, dict):
                return bool(data.get("release_to_winner_only"))
            elif isinstance(data, list) and data and isinstance(data[0], dict):
                return any(bool(d.get("release_to_winner_only")) for d in data)
        except Exception:
            pass
        return False

    def get_parsed_documents(self) -> list:
        if not self.document_path:
            return []
        try:
            import json, os
            data = json.loads(self.document_path)
            if isinstance(data, dict):
                return data.get("documents", [])
            elif isinstance(data, list):
                return data
        except Exception:
            import os
            return [{"name": os.path.basename(p.strip()), "path": p.strip()} for p in self.document_path.split(',') if p.strip()]
        return []

    def get_documents_for_leg(self, leg_index: int = None, leg_id: str = None, pair: str = None) -> list:
        """Returns documents associated with a specific leg (or global docs applicable to all legs)."""
        docs = self.get_parsed_documents()
        filtered = []

        # Check if 0-based indexing is used in the uploaded doc set (e.g. frontend pIdx 0, 1...)
        has_zero_indexed = any(d.get("leg_index") == 0 for d in docs if d.get("leg_index") is not None)

        clean_pair = pair.strip().upper() if pair else None
        clean_leg_id = str(leg_id).strip() if leg_id else None

        for d in docs:
            d_idx = d.get("leg_index")
            d_id = str(d.get("leg_id") or "").strip()
            d_pair = str(d.get("pair") or "").strip().upper()

            # Global documents (no leg_index, no leg_id, no pair specified) apply to all legs
            if d_idx is None and not d_id and not d_pair:
                filtered.append(d)
                continue

            # Critical Safeguard: If a document has an explicit pair and clean_pair is provided,
            # it must NEVER be assigned to a different currency pair!
            if d_pair and clean_pair and d_pair != clean_pair:
                continue

            # Exact leg_id match
            if clean_leg_id and d_id and d_id == clean_leg_id:
                filtered.append(d)
                continue

            # Exact pair match if provided
            if clean_pair and d_pair and d_pair == clean_pair:
                filtered.append(d)
                continue

            # Accurate index matching (ONLY if the document does not specify a conflicting pair)
            if leg_index is not None and d_idx is not None and not d_pair:
                expected_idx = (leg_index - 1) if (has_zero_indexed and leg_index >= 1) else leg_index
                if d_idx == expected_idx:
                    filtered.append(d)
                    continue

        return filtered

class QuotationLeg(BaseModel):
    """Individual currency pair or trade leg within a QuotationRequest session."""
    __tablename__ = "quotation_legs"
    id = Column(String, primary_key=True)
    rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="CASCADE"), nullable=False, index=True)
    leg_index = Column(Integer, default=1, nullable=False)
    
    type = Column(String, default="FX_SPOT", comment="'FX_SPOT' or 'TBILL'")
    direction = Column(String, nullable=True, comment="'Buy' or 'Sell'")
    buy_currency = Column(String, nullable=True)
    sell_currency = Column(String, nullable=True)
    amount = Column(Float, nullable=True)
    min_ticket_amount = Column(Float, nullable=True)
    value_date = Column(String, nullable=True)
    allow_alternative_value_date = Column(Boolean, default=False, nullable=False)
    quotation_base = Column(String, nullable=True, comment="'Execution' or 'Indicative'")
    max_tolerance_percent = Column(Float, nullable=True)
    
    settlement_date_start = Column(String, nullable=True)
    settlement_date_end = Column(String, nullable=True)
    maturity_date_start = Column(String, nullable=True)
    maturity_date_end = Column(String, nullable=True)
    eval_rate = Column(Float, nullable=True)
    
    status = Column(String, default="PENDING", comment="'PENDING', 'OPEN', 'EVALUATING', 'COMPLETED', 'ACCEPTED', 'REJECTED', 'INCONCLUSIVE', 'EXPIRED'")
    winner_bank_id = Column(Integer, nullable=True)
    winner_bank_name = Column(String(255), nullable=True)
    _db_winner_rate = Column("winner_rate", Float, nullable=True)
    encrypted_winner_rate = Column(String(255), nullable=True)
    _db_saved_vs_avg = Column("saved_vs_avg", Float, nullable=True)
    encrypted_saved_vs_avg = Column(String(255), nullable=True)
    execution_reference = Column(String(50), nullable=True)
    deal_slip_pdf_path = Column(String(500), nullable=True)
    rejection_reason = Column(Text, nullable=True)
    document_path = Column(Text, nullable=True)
    entity_id = Column(Integer, ForeignKey("customer_entities.id", ondelete="SET NULL"), nullable=True)
    market_benchmark_snapshot = Column(JSONB, nullable=True, comment="Frozen market benchmark snapshot at leg acceptance")

    rfq = relationship("QuotationRequest", back_populates="legs")

    @property
    def winner_rate(self):
        if hasattr(self, '_resolved_winner_rate') and self._resolved_winner_rate is not None:
            return self._resolved_winner_rate
        if self._db_winner_rate is not None:
            return self._db_winner_rate
        if not self.encrypted_winner_rate:
            return None
        try:
            from sqlalchemy.orm import object_session
            session = object_session(self)
            cust_id = self.rfq.customer_id if (self.rfq and getattr(self.rfq, 'customer_id', None)) else None
            if session and cust_id:
                from app.services.tenant_key_service import tenant_key_service
                dek = tenant_key_service.get_or_create_tenant_dek(session, cust_id)
                from app.core.security_crypto import decrypt_field
                val = decrypt_field(self.encrypted_winner_rate, dek, field_context="winner_rate")
                if val is not None:
                    self._resolved_winner_rate = float(val)
                    return self._resolved_winner_rate
        except Exception:
            pass
        return None

    @winner_rate.setter
    def winner_rate(self, value):
        self._resolved_winner_rate = float(value) if value is not None else None
        if value is None:
            self._db_winner_rate = None
            self.encrypted_winner_rate = None

    @property
    def saved_vs_avg(self):
        if hasattr(self, '_resolved_saved_vs_avg') and self._resolved_saved_vs_avg is not None:
            return self._resolved_saved_vs_avg
        if self._db_saved_vs_avg is not None:
            return self._db_saved_vs_avg
        if not self.encrypted_saved_vs_avg:
            return None
        try:
            from sqlalchemy.orm import object_session
            session = object_session(self)
            cust_id = self.rfq.customer_id if (self.rfq and getattr(self.rfq, 'customer_id', None)) else None
            if session and cust_id:
                from app.services.tenant_key_service import tenant_key_service
                dek = tenant_key_service.get_or_create_tenant_dek(session, cust_id)
                from app.core.security_crypto import decrypt_field
                val = decrypt_field(self.encrypted_saved_vs_avg, dek, field_context="saved_vs_avg")
                if val is not None:
                    self._resolved_saved_vs_avg = float(val)
                    return self._resolved_saved_vs_avg
        except Exception:
            pass
        return None

    @saved_vs_avg.setter
    def saved_vs_avg(self, value):
        self._resolved_saved_vs_avg = float(value) if value is not None else None
        if value is None:
            self._db_saved_vs_avg = None
            self.encrypted_saved_vs_avg = None
    offers = relationship("QuotationOffer", back_populates="leg", cascade="all, delete-orphan")
    tbill_offers = relationship("QuotationTBillOffer", back_populates="leg", cascade="all, delete-orphan")
    bank_configs = relationship("QuotationBankLegConfig", back_populates="leg", cascade="all, delete-orphan")

    @property
    def currency_pair(self) -> str:
        if self.buy_currency and self.sell_currency:
            return f"{self.buy_currency}/{self.sell_currency}"
        return ""

    @property
    def pair_order(self) -> int:
        return self.leg_index

    @property
    def tolerance_percent(self) -> float:
        return self.max_tolerance_percent

    def get_parsed_documents(self) -> list:
        if not self.document_path:
            return []
        try:
            import json, os
            data = json.loads(self.document_path)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("documents", [])
        except Exception:
            import os
            return [{"name": os.path.basename(p.strip()), "path": p.strip()} for p in self.document_path.split(',') if p.strip()]
        return []

class QuotationBankAssignment(BaseModel):
    """Junction table connecting an RFQ strictly to a QuotationBank (1 token per bank per RFQ session)."""
    __tablename__ = "quotation_bank_assignments"
    id = Column(String, primary_key=True)
    rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="CASCADE"), nullable=False)
    quotation_bank_id = Column(Integer, ForeignKey("quotation_banks.id", ondelete="CASCADE"), nullable=False)
    token = Column(String, unique=True, index=True, nullable=False)
    
    cost_min = Column(Float, default=0.0)
    cost_percent = Column(Float, default=0.0)
    cost_max = Column(Float, default=0.0)
    cost_flat = Column(Float, default=0.0)
    quotation_base = Column(String, nullable=True, comment="'Execution' or 'Indicative' override per bank")
    is_document_visible = Column(Boolean, default=True, comment="Controls document attachment visibility for this bank")
    value_date = Column(Date, nullable=True, comment="Custom value date target for this specific bank; falls back to RFQ master value_date")
    allow_alternative_value_date = Column(Boolean, nullable=True, comment="Per-bank override: True/False, or NULL to inherit from RFQ master")
    
    # Bank Approval Layer (optional, per-assignment)
    approval_status = Column(String, default=None, nullable=True, comment="NULL=no approval needed, 'PENDING', 'APPROVED', 'DECLINED', 'EXPIRED'")
    approved_by_email = Column(String, nullable=True, comment="Email of the approver who approved/declined")
    approved_at = Column(DateTime(timezone=True), nullable=True)
    approval_notes = Column(Text, nullable=True, comment="Optional notes from the approver")

    rfq = relationship("QuotationRequest", back_populates="assignments")
    quotation_bank = relationship("QuotationBank")
    offers = relationship("QuotationOffer", back_populates="assignment", cascade="all, delete-orphan")
    tbill_offers = relationship("QuotationTBillOffer", back_populates="assignment", cascade="all, delete-orphan")
    otps = relationship("QuotationAccessOTP", back_populates="assignment", cascade="all, delete-orphan")
    leg_configs = relationship("QuotationBankLegConfig", back_populates="assignment", cascade="all, delete-orphan")

    def get_config_for_leg(self, leg_id: str):
        """Returns the specific configuration for a leg, or falls back to assignment level."""
        if self.leg_configs:
            for cfg in self.leg_configs:
                if cfg.leg_id == leg_id:
                    return cfg
        return self

    @property
    def is_cross_entity(self) -> bool:
        """Determines if this bank assignment is a cross-entity benchmark (not directly scoped to the RFQ entity)."""
        if not self.rfq or not self.rfq.entity_id or not self.quotation_bank:
            return False
        if self.quotation_bank.entity_scope == 'ALL_ENTITIES':
            return False
        assoc_ids = [assoc.entity_id for assoc in self.quotation_bank.entity_associations] if self.quotation_bank.entity_associations else []
        return self.rfq.entity_id not in assoc_ids

class QuotationBankLegConfig(BaseModel):
    """Per-bank configuration for a specific currency pair leg within a quotation session."""
    __tablename__ = "quotation_bank_leg_configs"
    id = Column(String, primary_key=True)
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False, index=True)
    leg_id = Column(String, ForeignKey("quotation_legs.id", ondelete="CASCADE"), nullable=False, index=True)
    
    is_invited = Column(Boolean, default=True, nullable=False)
    is_passed = Column(Boolean, default=False, nullable=False, comment="True if dealer explicitly passed/declined to quote this leg")
    cost_min = Column(Float, default=0.0)
    cost_percent = Column(Float, default=0.0)
    cost_max = Column(Float, default=0.0)
    cost_flat = Column(Float, default=0.0)
    quotation_base = Column(String, nullable=True, comment="'Execution' or 'Indicative' override per bank for this leg")
    is_document_visible = Column(Boolean, default=True)
    value_date = Column(Date, nullable=True, comment="Custom value date target for this specific bank on this leg")
    allow_alternative_value_date = Column(Boolean, nullable=True, comment="Per-bank override for this leg")

    assignment = relationship("QuotationBankAssignment", back_populates="leg_configs")
    leg = relationship("QuotationLeg", back_populates="bank_configs")

class QuotationOffer(BaseModel):
    """FX Spot Offers from banks."""
    __tablename__ = "quotation_offers"
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False)
    leg_id = Column(String, ForeignKey("quotation_legs.id", ondelete="CASCADE"), nullable=True, index=True)
    price = Column(Float, nullable=False)
    encrypted_price = Column(String(255), nullable=True, comment="Phase 8 Zero-Knowledge AES-256-GCM encrypted quote")
    encrypted_spread = Column(String(255), nullable=True, comment="Phase 8 Zero-Knowledge AES-256-GCM encrypted spread")
    offered_value_date = Column(Date, nullable=True, comment="Alternative settlement date proposed by counterparty")
    notes = Column(Text, nullable=True, comment="Optional notes or comments from the submitting trader")
    submitted_by_email = Column(String, nullable=True, comment="Email of the authenticated trader who submitted this quote")
    submitted_at = Column(DateTime(timezone=True), server_default=func.now())

    assignment = relationship("QuotationBankAssignment", back_populates="offers")
    leg = relationship("QuotationLeg", back_populates="offers")

class QuotationTBillOffer(BaseModel):
    """T-Bill specific quotation lines."""
    __tablename__ = "quotation_tbill_offers"
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False)
    leg_id = Column(String, ForeignKey("quotation_legs.id", ondelete="CASCADE"), nullable=True, index=True)
    settlement_date = Column(String, nullable=False)
    maturity_date = Column(String, nullable=False)
    discount_rate = Column(Float, nullable=False)
    max_amount = Column(Float, nullable=False)
    encrypted_discount_rate = Column(String(255), nullable=True, comment="Phase 8 Zero-Knowledge AES-256-GCM encrypted discount rate")
    encrypted_max_amount = Column(String(255), nullable=True, comment="Phase 8 Zero-Knowledge AES-256-GCM encrypted max amount")
    notes = Column(Text, nullable=True, comment="Optional notes or comments from the submitting trader")
    submitted_by_email = Column(String, nullable=True, comment="Email of the authenticated trader who submitted this quote")
    submitted_at = Column(DateTime(timezone=True), server_default=func.now())

    assignment = relationship("QuotationBankAssignment", back_populates="tbill_offers")
    leg = relationship("QuotationLeg", back_populates="tbill_offers")

class QuotationTenantKey(BaseModel):
    """Phase 8: Stores wrapped Tenant Data Encryption Keys (DEKs) per Corporate Customer."""
    __tablename__ = "quotation_tenant_keys"
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    key_id = Column(String(64), nullable=False, unique=True)
    wrapped_dek = Column(Text, nullable=False, comment="AES-256-GCM wrapped DEK sealed by Master KEK")
    key_version = Column(Integer, default=1, nullable=False)
    status = Column(String(20), default="ACTIVE", nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    rotated_at = Column(DateTime(timezone=True), nullable=True)

    customer = relationship("Customer")


class QuotationAccessOTP(BaseModel):
    """Stores OTPs and magic access tokens for bank desk authentication."""
    __tablename__ = "quotation_access_otps"
    id = Column(Integer, primary_key=True, index=True)
    assignment_id = Column(String, ForeignKey("quotation_bank_assignments.id", ondelete="CASCADE"), nullable=False, index=True)
    email = Column(String, nullable=False, index=True)
    role = Column(String, default="EXECUTION", comment="'EXECUTION', 'VIEW_ONLY', or 'APPROVER'")
    otp_code = Column(String, nullable=False)
    magic_token = Column(String, unique=True, index=True, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_used = Column(Boolean, default=False)
    failed_attempts = Column(Integer, default=0, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    assignment = relationship("QuotationBankAssignment", back_populates="otps")

class QuotationAnalytics(BaseModel):
    """Stores definite factual computation facts for closed RFQs."""
    __tablename__ = "quotation_analytics"
    id = Column(Integer, primary_key=True, index=True)
    rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="CASCADE"), nullable=False, unique=True)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False, index=True)
    winner_quotation_bank_id = Column(Integer, ForeignKey("quotation_banks.id", ondelete="SET NULL"), nullable=True)
    
    winner_price = Column(Float, nullable=True)
    total_participated = Column(Integer, default=0)
    avg_price_spread = Column(Float, default=0.0)
    results_json = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    rfq = relationship("QuotationRequest")
    customer = relationship("Customer")
    winner_bank = relationship("QuotationBank")

class QuotationNotification(BaseModel):
    """Per-user notifications for RFQ status changes and live updates."""
    __tablename__ = "quotation_notifications"
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    type = Column(String, nullable=False, comment="'RFQ_PENDING_APPROVAL', 'RFQ_APPROVED', 'RFQ_REJECTED', 'NEW_OFFER'")
    title = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    link = Column(String, nullable=True)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User")

class QuotationAnonymousBenchmark(Base):
    """
    Zero-Knowledge Anonymous Aggregation Mart.
    Stores stripped, normalized macro metrics across all tenders with 0% confidentiality risk.
    Contains NO customer IDs, NO user IDs, NO RFQ refs, and NO raw trade amounts.
    """
    __tablename__ = "quotation_anonymous_benchmarks"
    
    id = Column(Integer, primary_key=True, index=True)
    tenant_cohort_hash = Column(String, nullable=False, index=True, comment="One-way anonymous tenant hash for k-anonymity cardinality verification")
    currency_pair = Column(String(10), nullable=False, index=True, comment="e.g. 'USD/EGP', 'EUR/EGP'")
    trade_type = Column(String(20), nullable=False, default="FX_SPOT", comment="'FX_SPOT' or 'TBILL'")
    deal_tier = Column(String(10), nullable=False, comment="'TIER_1' (<250k), 'TIER_2' (250k-1M), 'TIER_3' (>1M)")
    cbe_benchmark_rate = Column(Float, nullable=True, comment="Reference rate from CBE at window start")
    winning_spread_bps = Column(Float, nullable=True, comment="Winning execution rate spread in basis points over CBE benchmark")
    avg_spread_bps = Column(Float, nullable=True, comment="Market average spread in basis points over CBE benchmark")
    num_participating_banks = Column(Integer, default=0)
    num_quotes_submitted = Column(Integer, default=0)
    response_duration_seconds = Column(Integer, nullable=True, comment="Seconds from tender opening to first winning quote")
    window_time_slot = Column(String(20), nullable=True, comment="e.g. 'TUE_10_12', 'WED_14_16' for liquidity timing analysis")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

class BankLiveRankingConfig(BaseModel):
    """
    Configuration table allowing System Owner to enable or disable live ranking per Bank,
    filtered by Service (FX_SPOT, TBILL, or BOTH), Customer Scope (ALL or SPECIFIC),
    and Entity Scope (ALL or SPECIFIC).
    """
    __tablename__ = "bank_live_ranking_configs"

    id = Column(Integer, primary_key=True, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="CASCADE"), nullable=False, index=True)
    trade_type = Column(String(20), default="BOTH", nullable=False, comment="'FX_SPOT', 'TBILL', or 'BOTH'")
    
    # Customer scope
    scope_type = Column(String(30), default="ALL_CUSTOMERS", nullable=False, comment="'ALL_CUSTOMERS' or 'SPECIFIC_CUSTOMER'")
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=True, index=True)
    
    # Entity scope (applicable when scope_type == 'SPECIFIC_CUSTOMER')
    entity_scope_type = Column(String(30), default="ALL_ENTITIES", nullable=False, comment="'ALL_ENTITIES' or 'SPECIFIC_ENTITY'")
    entity_id = Column(Integer, ForeignKey("customer_entities.id", ondelete="CASCADE"), nullable=True, index=True)
    
    is_enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    bank = relationship("Bank")
    customer = relationship("Customer")
    entity = relationship("CustomerEntity")


class QuotationMarketRateHistory(Base):
    """
    Time-Series Market Spot Rate Archive.
    Stores historical live spot rates fetched from interbank feeds to build
    a proprietary time-series market database for rate modeling and audit reconstruction.
    """
    __tablename__ = "quotation_market_rate_history"

    id = Column(Integer, primary_key=True, index=True)
    currency_pair = Column(String(10), nullable=False, index=True, comment="e.g. 'USD/EGP', 'EUR/EGP'")
    base_currency = Column(String(5), nullable=False)
    quote_currency = Column(String(5), nullable=False)
    rate = Column(Float, nullable=False)
    source = Column(String(50), nullable=False)
    cbe_official_mid = Column(Float, nullable=True)
    cbe_gap_bps = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)


class QuotationDealerFeedback(Base):
    """
    Phase 7.2 & 7.3: Institutional Dealer Voice & Feedback Mechanism.
    Stores direct feedback, ratings, usability issues, and feature requests
    submitted by bank dealers quoting via public terminal links.
    """
    __tablename__ = "quotation_dealer_feedbacks"

    id = Column(Integer, primary_key=True, index=True)
    rfq_id = Column(String, ForeignKey("quotation_rfqs.id", ondelete="SET NULL"), nullable=True, index=True)
    quotation_bank_id = Column(Integer, ForeignKey("quotation_banks.id", ondelete="SET NULL"), nullable=True, index=True)
    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="SET NULL"), nullable=True, index=True)
    bank_name = Column(String(255), nullable=True)
    
    dealer_email = Column(String(255), nullable=True, index=True)
    dealer_name = Column(String(255), nullable=True)
    is_anonymous = Column(Boolean, default=False, nullable=False)
    
    star_rating = Column(Integer, nullable=False, default=5)
    category = Column(String(100), nullable=False, default="GENERAL_FEEDBACK")
    comment = Column(Text, nullable=True)
    
    status = Column(String(50), default="NEW", nullable=False, index=True)
    admin_notes = Column(Text, nullable=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_by = Column(String(255), nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    rfq = relationship("QuotationRequest")
    quotation_bank = relationship("QuotationBank")
    bank = relationship("Bank")

    def to_dict(self):
        is_anon = bool(self.is_anonymous)
        return {
            "id": self.id,
            "rfq_id": None if is_anon else self.rfq_id,
            "rfq_ref_no": None if is_anon else (getattr(self.rfq, 'ref_no', None) if self.rfq else None),
            "quotation_bank_id": None if is_anon else self.quotation_bank_id,
            "bank_id": None if is_anon else self.bank_id,
            "bank_name": "Verified Bank Partner" if is_anon else (self.bank_name or (self.bank.name if self.bank else "Unknown Bank")),
            "dealer_email": "Anonymous Dealer" if is_anon else self.dealer_email,
            "dealer_name": "Anonymous Trader" if is_anon else (self.dealer_name or (self.dealer_email.split('@')[0] if self.dealer_email else "Trader")),
            "is_anonymous": is_anon,
            "star_rating": self.star_rating,
            "category": self.category,
            "comment": self.comment,
            "status": self.status,
            "admin_notes": self.admin_notes,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolved_by": self.resolved_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }


class QuotationBankDealer(BaseModel):
    """
    Permanent Bank Dealer user entity for institutional multi-customer quotation desk access.
    Enables unified multi-customer RFQ participation without expiring link dependency, secured by
    RFC 6238 TOTP dual-factor authentication (Microsoft Authenticator / Google Authenticator).
    """
    __tablename__ = "quotation_bank_dealers"

    bank_id = Column(Integer, ForeignKey("banks.id", ondelete="CASCADE"), nullable=False, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False, comment="Official corporate bank email (@bank.com)")
    full_name = Column(String(255), nullable=False)
    phone_number = Column(String(50), nullable=True)
    title = Column(String(100), nullable=True, comment="e.g. Senior FX Dealer, Head of Treasury Sales")
    role = Column(String(50), default="EXECUTION", nullable=False, comment="'EXECUTION', 'APPROVER', or 'VIEW_ONLY'")

    hashed_password = Column(String(255), nullable=True)
    totp_secret = Column(String(128), nullable=True, comment="Base32 RFC 6238 secret key for Microsoft/Google Authenticator")
    is_totp_enrolled = Column(Boolean, default=False, nullable=False)

    is_active = Column(Boolean, default=True, nullable=False)
    email_verified_at = Column(DateTime(timezone=True), nullable=True)
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    failed_login_attempts = Column(Integer, default=0, nullable=False)
    locked_until = Column(DateTime(timezone=True), nullable=True)

    # Staging fields for 2-Factor Enrolment Handshake
    pending_email_otp = Column(String(64), nullable=True, comment="HMAC-SHA256 hashed 6-digit email verification code")
    pending_email_otp_expires_at = Column(DateTime(timezone=True), nullable=True)
    pending_otp_failed_attempts = Column(Integer, default=0, nullable=False)
    enrollment_token = Column(String(255), unique=True, nullable=True, index=True, comment="Gate 1 to Gate 2 cryptographic handover token")
    enrollment_token_expires_at = Column(DateTime(timezone=True), nullable=True)

    bank = relationship("Bank")

    def to_dict(self):
        return {
            "id": self.id,
            "bank_id": self.bank_id,
            "bank_name": self.bank.name if self.bank else None,
            "bank_short_name": getattr(self.bank, 'short_name', None) if self.bank else None,
            "email": self.email,
            "full_name": self.full_name,
            "phone_number": self.phone_number,
            "title": self.title,
            "role": self.role,
            "is_totp_enrolled": bool(self.is_totp_enrolled),
            "is_active": bool(self.is_active),
            "locked_until": self.locked_until.isoformat() if self.locked_until else None,
            "failed_login_attempts": self.failed_login_attempts,
            "last_login_at": self.last_login_at.isoformat() if self.last_login_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }


# Aliases for convenience
BankDealerUser = QuotationBankDealer


