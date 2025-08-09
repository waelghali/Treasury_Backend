# crud_audit.py
from typing import List, Optional, Type
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func, desc
from fastapi import HTTPException, status

from app.crud.crud import CRUDBase, log_action
from app.models import AuditLog, User, LGRecord
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
        db.commit() # Note: This `commit` is unusual for a log function if the caller is managing a broader transaction.
                    # It means every log_action will commit its own transaction. Usually, logs are flushed and committed by the outer transaction.
                    # However, this is existing behavior, so I'm retaining it.
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
    ) -> List[AuditLog]:
        query = db.query(self.model)
        if user_id:
            query = query.filter(self.model.user_id == user_id)
        if action_type:
            query = query.filter(self.model.action_type == action_type)
        if entity_type:
            query = query.filter(self.model.entity_type == entity_type)
        if entity_id:
            query = query.filter(self.model.entity_id == entity_id)
        if customer_id:
            query = query.filter(self.model.customer_id == customer_id)
        if lg_record_id:
            query = query.filter(self.model.lg_record_id == lg_record_id)

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

        return query.options(
            selectinload(AuditLog.user)
        ).order_by(desc(self.model.timestamp)).all()

# Removed local instantiation: crud_audit_log = CRUDAuditLog(AuditLog)