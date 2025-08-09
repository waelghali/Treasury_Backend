# app/crud/crud_internal_owner_contact.py
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type, Tuple
from fastapi import HTTPException, status
from sqlalchemy import func
from sqlalchemy.orm import Session, selectinload

from app.crud.crud import CRUDBase, log_action
import app.models as models
from app.models import InternalOwnerContact, LGRecord
from app.schemas.all_schemas import InternalOwnerContactCreate
from app.constants import LgStatusEnum

import logging
logger = logging.getLogger(__name__)

class CRUDInternalOwnerContact(CRUDBase):
    def get_by_email_for_customer(
        self, db: Session, customer_id: int, email: str
    ) -> Optional[InternalOwnerContact]:
        return (
            db.query(self.model)
            .filter(
                self.model.customer_id == customer_id,
                func.lower(self.model.email) == func.lower(email),
                self.model.is_deleted == False,
            )
            .first()
        )

    def get_all_for_customer(
        self, db: Session, customer_id: int, skip: int = 0, limit: int = 100
    ) -> List[InternalOwnerContact]:
        """
        Retrieves all active internal owner contacts for a specific customer.
        """
        return (
            db.query(self.model)
            .filter(self.model.customer_id == customer_id, self.model.is_deleted == False)
            .offset(skip)
            .limit(limit)
            .all()
        )

    def create_or_get(self, db: Session, obj_in: InternalOwnerContactCreate, customer_id: int, user_id: int) -> InternalOwnerContact:
        existing_contact = self.get_by_email_for_customer(
            db, customer_id, obj_in.email
        )
        if existing_contact:
            # If already exists and is active, return it.
            if not existing_contact.is_deleted:
                return existing_contact
            # If it exists but is deleted, restore and update.
            restored_obj = self.restore(db, existing_contact) # Call restore on the CRUDBase instance
            for field, value in obj_in.model_dump(exclude_unset=True).items():
                setattr(restored_obj, field, value)
            db.add(restored_obj)
            db.flush()
            db.refresh(restored_obj)
            log_action(
                db,
                user_id=user_id,
                action_type="RESTORE_AND_UPDATE",
                entity_type="InternalOwnerContact",
                entity_id=restored_obj.id,
                details={"email": restored_obj.email, "customer_id": customer_id, "restored_from_deleted": True},
                customer_id=customer_id,
            )
            return restored_obj

        # No active or deleted contact, create a new one.
        internal_owner_data = obj_in.model_dump()
        db_obj = self.model(customer_id=customer_id, **internal_owner_data)
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)
        log_action(
            db,
            user_id=user_id,
            action_type="CREATE",
            entity_type="InternalOwnerContact",
            entity_id=db_obj.id,
            details={"email": db_obj.email, "customer_id": customer_id},
            customer_id=customer_id,
        )
        return db_obj

    def soft_delete(self, db: Session, db_obj: InternalOwnerContact, user_id: int) -> InternalOwnerContact:
        """
        Soft-deletes an InternalOwnerContact, but only if they are not
        associated with any active LG records.
        """
        # Check for active LG records associated with this owner
        active_lgs_count = db.query(LGRecord).filter(
            LGRecord.internal_owner_contact_id == db_obj.id,
            LGRecord.customer_id == db_obj.customer_id,
            LGRecord.is_deleted == False,
            LGRecord.lg_status_id == LgStatusEnum.VALID.value
        ).count()

        if active_lgs_count > 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot delete contact '{db_obj.email}' as they are currently the owner of {active_lgs_count} active LG records. Please reassign those records first."
            )

        # Proceed with soft deletion if no active LGs are found
        deleted_obj = super().soft_delete(db, db_obj)
        
        log_action(
            db,
            user_id=user_id,
            action_type="SOFT_DELETE",
            entity_type="InternalOwnerContact",
            entity_id=deleted_obj.id,
            details={"email": deleted_obj.email, "customer_id": deleted_obj.customer_id},
            customer_id=deleted_obj.customer_id,
        )
        return deleted_obj
        
        
    def get_all_for_customer_with_lg_count(
        self, db: Session, customer_id: int
    ) -> List[models.InternalOwnerContact]:
        """
        Retrieves all active internal owner contacts for a specific customer,
        including a count of active (Valid) LG records and a total count
        of all non-deleted LGs for each.
        """
        from app.models import LGRecord, LgStatusEnum
        
        # Build a subquery to count ALL non-deleted LGs per owner
        lg_total_count_subquery = (
            db.query(
                LGRecord.internal_owner_contact_id,
                func.count(LGRecord.id).label("owned_lgs_total_count")
            )
            .filter(
                LGRecord.is_deleted == False
            )
            .group_by(LGRecord.internal_owner_contact_id)
            .subquery()
        )

        # Build a subquery to count active LGs (Status = Valid) per owner
        lg_valid_count_subquery = (
            db.query(
                LGRecord.internal_owner_contact_id,
                func.count(LGRecord.id).label("owned_lgs_valid_count")
            )
            .filter(
                LGRecord.is_deleted == False,
                LGRecord.lg_status_id == LgStatusEnum.VALID.value
            )
            .group_by(LGRecord.internal_owner_contact_id)
            .subquery()
        )

        # Join the main query with both subqueries
        owners_with_counts = (
            db.query(
                models.InternalOwnerContact,
                lg_valid_count_subquery.c.owned_lgs_valid_count,
                lg_total_count_subquery.c.owned_lgs_total_count,
            )
            .outerjoin(
                lg_valid_count_subquery,
                models.InternalOwnerContact.id == lg_valid_count_subquery.c.internal_owner_contact_id
            )
            .outerjoin(
                lg_total_count_subquery,
                models.InternalOwnerContact.id == lg_total_count_subquery.c.internal_owner_contact_id
            )
            .filter(
                models.InternalOwnerContact.customer_id == customer_id,
                models.InternalOwnerContact.is_deleted == False
            )
            .all()
        )
        
        result = []
        for owner, valid_count, total_count in owners_with_counts:
            setattr(owner, 'owned_lgs_count', valid_count or 0) # Keep the old field for compatibility
            setattr(owner, 'owned_lgs_total_count', total_count or 0)
            result.append(owner)
            
        return result