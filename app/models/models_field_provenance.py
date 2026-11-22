# app/models/models_field_provenance.py
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from app.database import Base


class LGFieldProvenance(Base):
    """
    Additive sidecar table tracking per-field origin (AI scan vs. manual user entry / edit)
    for Custody LG records without modifying the core lg_records schema or endpoints.
    """
    __tablename__ = "lg_field_provenance"

    id = Column(Integer, primary_key=True, index=True)
    lg_record_id = Column(
        Integer,
        ForeignKey("lg_records.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
        comment="Target LGRecord ID",
    )
    customer_id = Column(
        Integer,
        ForeignKey("customers.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Tenant customer ID",
    )
    created_by_user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="User who recorded the LG",
    )
    record_origin = Column(
        String(40),
        nullable=False,
        default="MANUAL",
        comment="Overall creation origin: AI_SCAN or MANUAL",
    )
    field_sources_json = Column(
        JSONB,
        nullable=False,
        default=dict,
        comment="Mapping of field_name -> origin status (AI_SCAN, SYSTEM_DEFAULT, USER_MANUAL, USER_EDITED_AI, USER_EDITED_DEFAULT)",
    )
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<LGFieldProvenance(lg_record_id={self.lg_record_id}, origin='{self.record_origin}')>"
