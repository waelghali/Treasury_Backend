# crud_subscription.py
from typing import Any, Optional, Type
from sqlalchemy.orm import Session
from app.crud.crud import CRUDBase, log_action
from app.models import SubscriptionPlan
from app.schemas.all_schemas import SubscriptionPlanCreate, SubscriptionPlanOut, SubscriptionPlanUpdate
from app.constants import SubscriptionStatus


# =====================================================================================
# Subscription Plan
# =====================================================================================
class CRUDSubscriptionPlan(CRUDBase):
    def get_by_name(self, db: Session, name: str) -> Optional[SubscriptionPlan]:
        return (
            db.query(self.model)
            .filter(self.model.name == name, self.model.is_deleted == False)
            .first()
        )

    def create(self, db: Session, obj_in: SubscriptionPlanCreate, **kwargs: Any) -> SubscriptionPlan:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="SubscriptionPlan",
            entity_id=db_obj.id,
            details={"name": db_obj.name, "grace_period_days": db_obj.grace_period_days},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: SubscriptionPlan, obj_in: SubscriptionPlanUpdate, **kwargs: Any
    ) -> SubscriptionPlan:
        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="SubscriptionPlan",
                entity_id=updated_obj.id,
                details={"name": updated_obj.name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj

    def soft_delete(self, db: Session, db_obj: SubscriptionPlan) -> SubscriptionPlan:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="SubscriptionPlan",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: SubscriptionPlan) -> SubscriptionPlan:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="SubscriptionPlan",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj