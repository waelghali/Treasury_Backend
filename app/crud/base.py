# app/crud/base.py
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type, TypeVar
from fastapi import HTTPException, status, UploadFile
import decimal
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import relationship, selectinload
from sqlalchemy.orm import Session
from app.models import BaseModel as SQLBaseModel, AuditLog
from app.schemas.all_schemas import AuditLogCreate

# Define ModelType for generic CRUDBase typing
ModelType = TypeVar("ModelType", bound=SQLBaseModel)

# =====================================================================================
# Base CRUD Class Definition (CRUDBase)
# =====================================================================================
class CRUDBase:
    def __init__(self, model: Type[ModelType]):
        self.model = model

    def get(self, db: Session, id: Any) -> Optional[ModelType]:
        
        return db.query(self.model).filter(self.model.id == id, self.model.is_deleted == False).first() 

    def get_all(self, db: Session, skip: int = 0, limit: int = 100) -> List[ModelType]:
        
        return db.query(self.model).filter(self.model.is_deleted == False).offset(skip).limit(limit).all() 

    def create(self, db: Session, obj_in: Any, **kwargs: Any) -> ModelType:
        
        obj_data = obj_in.model_dump(exclude_unset=True) 
        create_data = {**obj_data, **kwargs}
        db_obj = self.model(**create_data)
        db.add(db_obj)
        db.flush() 
        db.refresh(db_obj)
        return db_obj

    def update(self, db: Session, db_obj: ModelType, obj_in: Any, **kwargs: Any) -> ModelType:
        
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True) 

        old_values_for_log = {
            k: getattr(db_obj, k)
            for k in update_data.keys()
            if hasattr(db_obj, k)
        }

        update_data_merged = {**update_data, **kwargs}
        for field, value in update_data_merged.items():
            setattr(db_obj, field, value)
        db_obj.updated_at = func.now() 
        db.add(db_obj)
        db.flush() 
        db.refresh(db_obj)

        new_values_for_log = {
            k: getattr(db_obj, k)
            for k in update_data.keys()
            if hasattr(db_obj, k)
        }

        changed_fields = {}
        for key in update_data.keys():
            old_val = old_values_for_log.get(key)
            new_val = new_values_for_log.get(key)

            if isinstance(old_val, (datetime, date)):
                old_val = old_val.isoformat()
            if isinstance(new_val, (datetime, date)):
                new_val = new_val.isoformat()
            if isinstance(old_val, decimal.Decimal):
                old_val = float(old_val)
            if isinstance(new_val, decimal.Decimal):
                new_val = float(new_val)

            if old_val != new_val:
                changed_fields[key] = {"old": old_val, "new": new_val}

        db_obj._changed_fields_for_log = changed_fields 
        return db_obj

    def soft_delete(self, db: Session, db_obj: ModelType) -> ModelType:
        
        db_obj.is_deleted = True 
        db_obj.deleted_at = func.now() 
        db.add(db_obj)
        db.flush() 
        db.refresh(db_obj)
        return db_obj

    def restore(self, db: Session, db_obj: ModelType) -> ModelType:
        
        db_obj.is_deleted = False 
        db_obj.deleted_at = None 
        db.add(db_obj)
        db.flush() 
        db.refresh(db_obj)
        return db_obj

# =====================================================================================
# Log Action Utility
# =====================================================================================
def log_action(
    db: Session,
    user_id: Optional[int],
    action_type: str,
    entity_type: str,
    entity_id: Optional[int],
    details: Optional[Dict[str, Any]] = None,
    customer_id: Optional[int] = None,
    lg_record_id: Optional[int] = None,
    ip_address: Optional[str] = None,
):
    """
    Logs an action to the AuditLog.
    """
    try:
        audit_log_entry = AuditLog(
            user_id=user_id,
            action_type=action_type,
            entity_type=entity_type,
            entity_id=entity_id,
            details=details,
            customer_id=customer_id,
            lg_record_id=lg_record_id,
            ip_address=ip_address,
            timestamp=func.now(),
        )
        db.add(audit_log_entry)
        db.flush()
        db.refresh(audit_log_entry)
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error creating audit log entry: {e}", exc_info=True)