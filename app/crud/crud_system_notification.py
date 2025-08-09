# c:\Grow\app\crud\crud_system_notification.py
import json
import logging
from datetime import datetime, date
from typing import List, Optional, Any, Dict, Tuple

from sqlalchemy import or_, and_, cast, inspect, text, literal_column
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB

from app.crud.crud import CRUDBase, log_action
from app.models import SystemNotification, User, SystemNotificationViewLog
from app.schemas.all_schemas import SystemNotificationCreate, SystemNotificationUpdate
from app.constants import UserRole # Import UserRole enum

logger = logging.getLogger(__name__)

class CRUDSystemNotification(CRUDBase):
    def get_all_active(self, db: Session, skip: int = 0, limit: int = 100) -> List[SystemNotification]:
        """
        Retrieves all active system notifications, regardless of targeting.
        This is primarily for use by the System Owner for administration.
        """
        now = datetime.now()
        return db.query(self.model).filter(
            self.model.is_deleted == False,
            self.model.is_active == True,
            self.model.start_date <= now,
            self.model.end_date >= now
        ).order_by(self.model.created_at.desc()).offset(skip).limit(limit).all()

    def get_all(self, db: Session, skip: int = 0, limit: int = 100) -> List[SystemNotification]:
        """
        Retrieves all system notifications, including inactive ones.
        This is for use by the System Owner for administration.
        """
        return db.query(self.model).filter(
            self.model.is_deleted == False
        ).order_by(self.model.created_at.desc()).offset(skip).limit(limit).all()

    # In crud_system_notification.py, replace the get_active_notifications_for_user function
    def get_active_notifications_for_user(self, db: Session, user_id: int, customer_id: int) -> List[SystemNotification]:
        """
        Retrieves all currently active system notifications relevant to a specific user.
        This function performs initial filtering based on customer, role, and user ID,
        but it does NOT filter out dismissed notifications. That logic is now handled on the client-side.
        """
        now = datetime.now()

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return []

        query = db.query(self.model).filter(
            self.model.is_deleted == False,
            self.model.is_active == True,
            self.model.start_date <= now,
            self.model.end_date >= now,
            or_(
                and_(
                    or_(self.model.target_customer_ids.is_(None), self.model.target_customer_ids == []),
                    or_(self.model.target_roles.is_(None), self.model.target_roles == []),
                    or_(self.model.target_user_ids.is_(None), self.model.target_user_ids == [])
                ),
                self.model.target_customer_ids.contains([customer_id]),
                self.model.target_roles.contains([user.role.value]),
                self.model.target_user_ids.contains([user_id])
            )
        ).order_by(self.model.created_at.desc())
        
        return query.all()
        
    def create(self, db: Session, obj_in: SystemNotificationCreate, created_by_user_id: int) -> SystemNotification:
        """
        Creates a new system notification, allowing for optional targeting.
        """
        target_customer_ids = obj_in.target_customer_ids if obj_in.target_customer_ids is not None else []
        target_roles = obj_in.target_roles if obj_in.target_roles is not None else []
        target_user_ids = obj_in.target_user_ids if obj_in.target_user_ids is not None else []

        db_obj = self.model(
            content=obj_in.content,
            link=obj_in.link,
            start_date=obj_in.start_date,
            end_date=obj_in.end_date,
            is_active=obj_in.is_active,
            created_by_user_id=created_by_user_id,
            target_customer_ids=target_customer_ids, 
            animation_type=obj_in.animation_type,
            display_frequency=obj_in.display_frequency,
            max_display_count=obj_in.max_display_count,
            target_user_ids=target_user_ids,
            target_roles=target_roles,
        )
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)

        return db_obj

    def update(self, db: Session, db_obj: SystemNotification, obj_in: SystemNotificationUpdate, user_id: int) -> SystemNotification:
        """
        Updates a system notification and logs the action.
        """
        obj_data = obj_in.model_dump(exclude_unset=True)
        changed_fields = {}
        for field, value in obj_data.items():
            current_value = getattr(db_obj, field)
            if field in ["target_customer_ids", "target_user_ids", "target_roles"]:
                current_list = current_value if current_value is not None else []
                new_list = value if value is not None else []
                if sorted(current_list) != sorted(new_list):
                    changed_fields[field] = {"old": current_list, "new": new_list}
                    setattr(db_obj, field, value)
            elif current_value != value:
                changed_fields[field] = {"old": current_value, "new": value}
                setattr(db_obj, field, value)

        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)
        for field in changed_fields:
            for key in changed_fields[field]:
                if isinstance(changed_fields[field][key], datetime):
                    changed_fields[field][key] = changed_fields[field][key].isoformat()
        log_action(
            db,
            user_id=user_id,
            action_type="UPDATE",
            entity_type="SystemNotification",
            entity_id=db_obj.id,
            details={
                "content_preview": db_obj.content[:50] + "...",
                "updated_fields": changed_fields
            }
        )
        return db_obj

    def soft_delete(self, db: Session, db_obj: SystemNotification, user_id: int) -> SystemNotification:
        """
        Soft-deletes a system notification and logs the action.
        """
        db_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=user_id,
            action_type="SOFT_DELETE",
            entity_type="SystemNotification",
            entity_id=db_obj.id,
            details={
                "content_preview": db_obj.content[:50] + "..."
            }
        )
        return db_obj

    def restore(self, db: Session, db_obj: SystemNotification, user_id: int) -> SystemNotification:
        """
        Restores a soft-deleted system notification and logs the action.
        """
        db_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=user_id,
            action_type="RESTORE",
            entity_type="SystemNotification",
            entity_id=db_obj.id,
            details={
                "content_preview": db_obj.content[:50] + "..."
            }
        )
        return db_obj

    def get_active_universal_notifications(self, db: Session) -> List[SystemNotification]:
        """
        Retrieves all currently active universal notifications (i.e., those not targeted
        to any specific customer, user, or role).
        """
        now = datetime.now()
        return db.query(self.model).filter(
            self.model.is_deleted == False,
            self.model.is_active == True,
            self.model.start_date <= now,
            self.model.end_date >= now,
            or_(
                and_(
                    self.model.target_customer_ids.is_(None),
                    self.model.target_roles.is_(None),
                    self.model.target_user_ids.is_(None)
                ),
                and_(
                    self.model.target_customer_ids == [],
                    self.model.target_roles == [],
                    self.model.target_user_ids == []
                )
            )
        ).all()