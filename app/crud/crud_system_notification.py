# c:\Grow\app\crud\crud_system_notification.py
import json
import logging
from datetime import datetime, date
from typing import List, Optional, Any, Dict, Tuple

from sqlalchemy import or_, and_, cast, inspect, text, literal_column
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func # func is needed for coalesce
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError 

from app.crud.crud import CRUDBase, log_action
from app.models import SystemNotification, User, SystemNotificationViewLog
from app.schemas.all_schemas import SystemNotificationCreate, SystemNotificationUpdate
from app.constants import UserRole

logger = logging.getLogger(__name__)

class CRUDSystemNotification(CRUDBase):
    def get(self, db: Session, id: Any, include_deleted: bool = False) -> Optional[SystemNotification]:
        """
        Retrieve a SystemNotification record by its ID. 
        Overrides CRUDBase to allow fetching soft-deleted records.
        """
        query = db.query(self.model).filter(self.model.id == id)
        
        # Only apply the soft-delete filter if we are NOT asked to include deleted records
        if not include_deleted:
            query = query.filter(self.model.is_deleted == False) 
            
        return query.first()
    
    def get_all_active(self, db: Session, skip: int = 0, limit: int = 100) -> List[SystemNotification]:
        """
        Retrieves all active system notifications.
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
        Retrieves ALL system notifications, INCLUDING deleted ones.
        This enables the 'Restore' functionality in the admin panel.
        """
        return db.query(self.model).order_by(self.model.created_at.desc()).offset(skip).limit(limit).all()

    def get_active_notifications_for_user(self, db: Session, user_id: int, customer_id: Optional[int]) -> List[SystemNotification]:
        """
        Retrieves active notifications, filtering out those the user has already seen/exhausted,
        and applying correct customer, role, and user targeting logic.
        """
        now = datetime.now()

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return []

        user_role = user.role
        user_role_str = str(user_role.value) if hasattr(user_role, 'value') else str(user_role)
        
        # CRITICAL FIX: Convert user role string to UPPERCASE to match database entries (e.g., "END_USER")
        user_role_str = user_role_str.upper()
        
        # --- 1. Customer Targeting Clause ---
        # If the user belongs to a customer, the notification must either be untargeted OR target that customer.
        if customer_id:
            customer_clause = or_(
                self.model.target_customer_ids.is_(None),
                self.model.target_customer_ids == [],
                self.model.target_customer_ids.contains([customer_id])
            )
        # If the user is System Owner (no customer_id), the notification must be untargeted.
        else:
            customer_clause = or_(
                self.model.target_customer_ids.is_(None),
                self.model.target_customer_ids == []
            )

        # --- 2. Role Targeting Clause ---
        # The notification must either be untargeted OR target the user's role.
        role_clause = or_(
            self.model.target_roles.is_(None),
            self.model.target_roles == [],
            self.model.target_roles.contains([user_role_str]) # Uses the now-UPPERCASE role
        )

        # --- 3. User Targeting Clause ---
        # The notification must either be untargeted OR target the user's ID.
        user_clause = or_(
            self.model.target_user_ids.is_(None),
            self.model.target_user_ids == [],
            self.model.target_user_ids.contains([user_id])
        )

        # Final Targeting Condition: ALL three target types must be satisfied (AND)
        final_targeting_condition = and_(
            customer_clause,
            role_clause,
            user_clause
        )
        
        # --- Query Setup ---
        query = db.query(self.model).outerjoin(
            SystemNotificationViewLog,
            and_(
                SystemNotificationViewLog.notification_id == self.model.id,
                SystemNotificationViewLog.user_id == user_id
            )
        ).filter(
            # Basic active status checks
            self.model.is_deleted == False,
            self.model.is_active == True,
            self.model.start_date <= now,
            self.model.end_date >= now,
            
            # Targeting condition
            final_targeting_condition,
            
            # View Frequency Logic
            or_(
                # A. Always show 'once-per-login' (relies on client sessionStorage)
                self.model.display_frequency == 'once-per-login',
                
                # B. Handle all count-based rules ('once', 'repeat-x-times')
                and_(
                    self.model.display_frequency.in_(['repeat-x-times', 'once']),
                    or_(
                        # Condition 1: Log does NOT exist for this user (user hasn't seen it). 
                        SystemNotificationViewLog.id.is_(None),
                        
                        # Condition 2: Log exists, AND the count is less than the max.
                        SystemNotificationViewLog.view_count < func.coalesce(self.model.max_display_count, 1)
                    )
                )
            )
        ).order_by(self.model.created_at.desc())
        
        return query.all()

    # --- NEW METHOD: Log Display (Counts against view limit) ---
    def log_notification_display(self, db: Session, user_id: int, notification_id: int) -> SystemNotificationViewLog:
        """
        Increments the view count for a user/notification pair upon automatic display.
        Handles race condition using try-insert/update fallback pattern.
        """
        view_log = SystemNotificationViewLog(
            user_id=user_id,
            notification_id=notification_id,
            view_count=1, # Start at 1 for the first view
            last_viewed_at=datetime.now()
        )
        db.add(view_log)
        
        try:
            db.commit()
        except IntegrityError:
            # Race condition: Log was inserted by a concurrent request.
            db.rollback() 
            
            # Now, safely retrieve and update the existing log.
            view_log = db.query(SystemNotificationViewLog).filter(
                SystemNotificationViewLog.user_id == user_id,
                SystemNotificationViewLog.notification_id == notification_id
            ).first() 

            if view_log:
                view_log.view_count += 1
                view_log.last_viewed_at = datetime.now()
                db.commit()
            else:
                logger.error(f"Integrity error occurred but existing log for user {user_id}, notif {notification_id} not found after rollback.")
                raise 
        
        db.refresh(view_log)
        return view_log


    # --- EXISTING METHOD: Acknowledge (The Dismiss Action) ---
    def acknowledge_notification(self, db: Session, user_id: int, notification_id: int) -> SystemNotificationViewLog:
        """
        Increments the view count for a user/notification pair, serving as the explicit acknowledgment action.
        Handles race condition using try-insert/update fallback pattern.
        """
        view_log = SystemNotificationViewLog(
            user_id=user_id,
            notification_id=notification_id,
            view_count=1, # Start at 1 for the first view
            last_viewed_at=datetime.now()
        )
        db.add(view_log)

        try:
            db.commit()
        except IntegrityError:
            # Race condition: Log was inserted by a concurrent request.
            db.rollback() 
            
            # Now, safely retrieve and update the existing log.
            view_log = db.query(SystemNotificationViewLog).filter(
                SystemNotificationViewLog.user_id == user_id,
                SystemNotificationViewLog.notification_id == notification_id
            ).first()

            if view_log:
                view_log.view_count += 1
                view_log.last_viewed_at = datetime.now()
                db.commit()
            else:
                logger.error(f"Integrity error occurred but existing log for user {user_id}, notif {notification_id} not found after rollback.")
                raise 

        db.refresh(view_log)
        return view_log
        
        
    def create(self, db: Session, *, obj_in: SystemNotificationCreate, user_id: int) -> SystemNotification:
        """
        Creates a new SystemNotification instance, ensuring all fields including image_url are set.
        Overrides CRUDBase.create to explicitly handle SystemNotificationCreate fields.
        """
        # Convert Pydantic object to a dictionary
        # We use .model_dump() (or .dict() if using older Pydantic) to get the fields
        obj_in_data = obj_in.model_dump(exclude_unset=True)
        
        # 1. Create the database object
        db_obj = self.model(**obj_in_data)
        
        # 2. Add system audit fields
        db_obj.created_by_user_id = user_id
        
        # 3. Save to database
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)

        # 4. Log the action (based on your existing structure)
        log_action(
            db,
            user_id=user_id,
            action_type="CREATE",
            entity_type="SystemNotification",
            entity_id=db_obj.id,
            details={
                "content_preview": db_obj.content[:50] + "...",
                "image_url": db_obj.image_url # Include new field in audit log
            }
        )
        return db_obj

    def update(self, db: Session, db_obj: SystemNotification, obj_in: SystemNotificationUpdate, user_id: int) -> SystemNotification:
        """
        Updates a system notification and logs the action.
        """
        obj_data = obj_in.model_dump(exclude_unset=True)
        changed_fields = {}
        
        # Check if target_roles is being updated and convert to uppercase
        if 'target_roles' in obj_data and obj_data['target_roles'] is not None:
            new_target_roles = [role.upper() for role in obj_data['target_roles']]
            # Replace the value in obj_data with the uppercase version for processing below
            obj_data['target_roles'] = new_target_roles


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
        Soft-deletes a system notification.
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
        Restores a soft-deleted system notification.
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
        Retrieves active universal notifications (no targeting).
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