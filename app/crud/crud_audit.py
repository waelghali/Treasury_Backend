# crud_audit.py
from typing import List, Optional, Type
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func, desc
from fastapi import HTTPException, status

from app.crud.crud import CRUDBase, log_action
from app.models import AuditLog, User, LGRecord, CustomerEntity, LGCategory, ApprovalRequest, LGInstruction, Customer
from app.schemas.all_schemas import AuditLogCreate

# =====================================================================================
# Audit Logs
# =====================================================================================
class CRUDAuditLog(CRUDBase):
    def __init__(self, model: Type[AuditLog]):
        self.model = model

    def create_log(self, db: Session, log_in: AuditLogCreate) -> AuditLog:
        log_data = log_in.model_dump()
        db_log = self.model(**log_data)
        db.add(db_log)
        db.commit()
        db.refresh(db_log)
        return db_log

    def get(self, db: Session, id: int) -> Optional[AuditLog]:
        return db.query(self.model).filter(self.model.id == id).first()

    def get_all(self, db: Session, skip: int = 0, limit: int = 100) -> List[AuditLog]:
        return db.query(self.model).offset(skip).limit(limit).all()

    def get_all_logs(
        self,
        db: Session,
        skip: int = 0,
        limit: int = 100,
        user_id: Optional[int] = None,
        action_type: Optional[str] = None,
        entity_type: Optional[str] = None,
        entity_id: Optional[int] = None,
        customer_id: Optional[int] = None,
        lg_record_id: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        search: Optional[str] = None,
    ) -> List[AuditLog]:
        from sqlalchemy import cast, String, or_
        from datetime import datetime

        query = db.query(self.model).options(
            selectinload(self.model.user),
            selectinload(self.model.lg_record),
            selectinload(self.model.customer)
        )
        if user_id:
            query = query.filter(self.model.user_id == user_id)
        if action_type and action_type.strip() and action_type.upper() != "ALL":
            query = query.filter(func.lower(self.model.action_type) == func.lower(action_type.strip()))
        if entity_type and entity_type.strip() and entity_type.upper() != "ALL":
            query = query.filter(func.lower(self.model.entity_type) == func.lower(entity_type.strip()))
        if entity_id:
            query = query.filter(self.model.entity_id == entity_id)
        if customer_id:
            query = query.filter(self.model.customer_id == customer_id)
        if lg_record_id:
            query = query.filter(self.model.lg_record_id == lg_record_id)

        # Date range filtering
        if start_date:
            try:
                dt_start = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                query = query.filter(self.model.timestamp >= dt_start)
            except Exception:
                try:
                    dt_start = datetime.strptime(start_date, "%Y-%m-%d")
                    query = query.filter(self.model.timestamp >= dt_start)
                except Exception:
                    pass

        if end_date:
            try:
                dt_end = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                query = query.filter(self.model.timestamp <= dt_end)
            except Exception:
                try:
                    dt_end = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
                    query = query.filter(self.model.timestamp <= dt_end)
                except Exception:
                    pass

        # Text search across action, entity, details JSON, and IP address
        if search and search.strip():
            s = f"%{search.strip().lower()}%"
            query = query.filter(
                or_(
                    func.lower(self.model.action_type).like(s),
                    func.lower(self.model.entity_type).like(s),
                    func.lower(self.model.ip_address).like(s),
                    func.lower(cast(self.model.details, String)).like(s)
                )
            )

        query = query.order_by(self.model.timestamp.desc())

        return query.offset(skip).limit(limit).all()

    def get_lg_lifecycle_events(self, db: Session, lg_record_id: int, customer_id: int, action_type: Optional[str] = None) -> List[AuditLog]:
        """
        Retrieves all audit log entries related to a specific LG record for a given customer,
        ordered chronologically (most recent first), with optional action_type filter.
        """
        query = db.query(self.model).filter(
            self.model.lg_record_id == lg_record_id,
            self.model.customer_id == customer_id
        )

        if action_type: # NEW: Apply action_type filter if provided
            query = query.filter(self.model.action_type == action_type)

        # MODIFIED: Add eager loading for user and lg_record to optimize performance
        return query.options(
            selectinload(AuditLog.user),
            selectinload(AuditLog.lg_record)
        ).order_by(desc(self.model.timestamp)).all()


def enrich_audit_log(db: Session, log: AuditLog) -> "AuditLogOut":
    """
    Enriches a raw AuditLog database object into AuditLogOut with human-readable
    customer_name, user_name, entity_name, and lg_number.
    """
    from app.models import (
        User, LGRecord, CustomerEntity, LGCategory, ApprovalRequest,
        LGInstruction, Customer, Template, SubscriptionPlan, IssuanceFacility
    )
    from app.schemas.all_schemas import AuditLogOut

    user_name = log.user.email if (log.user and getattr(log.user, 'email', None)) else ("System" if not log.user_id else f"User #{log.user_id}")
    customer_name = log.customer.name if (log.customer and getattr(log.customer, 'name', None)) else None
    lg_number = None

    entity_name = None
    try:
        if log.lg_record:
            lg_number = log.lg_record.lg_number
            entity_name = lg_number
        elif log.entity_type == "User" and log.entity_id:
            u = db.query(User).filter(User.id == log.entity_id).first()
            entity_name = u.email if u else f"User #{log.entity_id}"
        elif log.entity_type == "Customer" and log.entity_id:
            c = db.query(Customer).filter(Customer.id == log.entity_id).first()
            entity_name = c.name if c else f"Customer #{log.entity_id}"
        elif log.entity_type in ("CustomerEntity", "Entity") and log.entity_id:
            ce = db.query(CustomerEntity).filter(CustomerEntity.id == log.entity_id).first()
            entity_name = ce.entity_name if ce else f"Entity #{log.entity_id}"
        elif log.entity_type == "Template" and log.entity_id:
            t = db.query(Template).filter(Template.id == log.entity_id).first()
            entity_name = t.name if t else f"Template #{log.entity_id}"
        elif log.entity_type == "Facility" and log.entity_id:
            f = db.query(IssuanceFacility).filter(IssuanceFacility.id == log.entity_id).first()
            entity_name = f.facility_name if f else f"Facility #{log.entity_id}"
        elif log.entity_type == "SubscriptionPlan" and log.entity_id:
            p = db.query(SubscriptionPlan).filter(SubscriptionPlan.id == log.entity_id).first()
            entity_name = p.name if p else f"Plan #{log.entity_id}"
        elif log.entity_type == "LGCategory" and log.entity_id:
            cat = db.query(LGCategory).filter(LGCategory.id == log.entity_id).first()
            entity_name = cat.name if cat else f"Category #{log.entity_id}"
        elif log.entity_type == "LGInstruction" and log.entity_id:
            inst = db.query(LGInstruction).filter(LGInstruction.id == log.entity_id).first()
            entity_name = inst.serial_number if inst else f"Instruction #{log.entity_id}"
            if log.lg_record:
                lg_number = log.lg_record.lg_number
        elif log.entity_type == "ApprovalRequest" and log.entity_id:
            entity_name = f"{log.action_type.replace('_', ' ').title()}"
            if log.lg_record:
                lg_number = log.lg_record.lg_number
        else:
            entity_name = log.entity_type or "System"
    except Exception:
        entity_name = log.entity_type or "System"

    if not entity_name:
        entity_name = log.entity_type or "System"

    return AuditLogOut(
        id=log.id,
        timestamp=log.timestamp,
        user_id=log.user_id,
        user_name=user_name,
        action_type=log.action_type,
        entity_type=log.entity_type,
        entity_id=log.entity_id,
        entity_name=entity_name,
        lg_number=lg_number,
        details=log.details,
        customer_id=log.customer_id,
        customer_name=customer_name,
        lg_record_id=log.lg_record_id,
        ip_address=log.ip_address
    )


def enrich_audit_logs(db: Session, logs: List[AuditLog]) -> List["AuditLogOut"]:
    return [enrich_audit_log(db, log) for log in logs]