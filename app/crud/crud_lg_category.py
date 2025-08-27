import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type, Tuple
from fastapi import HTTPException, status
from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session, selectinload

from app.crud.crud import CRUDBase, log_action
from app.models import (
    LGCategory,
    CustomerEntity,
    LGRecord,
    LGCategoryCustomerEntityAssociation,
)
from app.schemas.all_schemas import LGCategoryCreate, LGCategoryUpdate

import logging
logger = logging.getLogger(__name__)

class CRUDLGCategory(CRUDBase):
    def get_by_name(
        self, db: Session, category_name: str, customer_id: Optional[int]
    ) -> Optional[LGCategory]:
        """
        Retrieves a category by its name and customer scope (customer_id).
        customer_id=None is used for universal categories.
        """
        return (
            db.query(self.model)
            .filter(
                self.model.customer_id == customer_id,
                func.lower(self.model.name) == func.lower(category_name),
                self.model.is_deleted == False
            )
            .first()
        )

    def get_by_code(
        self, db: Session, code: str, customer_id: Optional[int]
    ) -> Optional[LGCategory]:
        """
        Retrieves a category by its code and customer scope (customer_id).
        customer_id=None is used for universal categories.
        """
        return (
            db.query(self.model)
            .filter(
                self.model.customer_id == customer_id,
                func.lower(self.model.code) == func.lower(code),
                self.model.is_deleted == False
            )
            .first()
        )
        
    def get_all_for_customer(
        self, db: Session, customer_id: int, skip: int = 0, limit: int = 100
    ) -> List[LGCategory]:
        """
        Retrieves all active categories for a specific customer, including universal categories.
        """
        return (
            db.query(self.model)
            .filter(
                (self.model.customer_id == customer_id) | (self.model.customer_id.is_(None)),
                self.model.is_deleted == False
            )
            .order_by(self.model.customer_id.desc().nulls_first(), self.model.name) # Show universal first, then customer-specific
            .options(
                selectinload(self.model.entity_associations).selectinload(
                    LGCategoryCustomerEntityAssociation.customer_entity
                )
            )
            .offset(skip)
            .limit(limit)
            .all()
        )

    def get_default_category(self, db: Session, customer_id: Optional[int]) -> Optional[LGCategory]:
        """
        Retrieves the default category for a specific scope (customer or universal).
        """
        return (
            db.query(self.model)
            .filter(
                self.model.is_default == True,
                self.model.customer_id == customer_id, # Use == for customer, is_(None) for universal
                self.model.is_deleted == False
            )
            .first()
        )
        
    def get_for_customer(
        self, db: Session, lg_category_id: int, customer_id: int
    ) -> Optional[LGCategory]:
        """
        Retrieves a category by ID, ensuring it belongs to the customer or is a universal category.
        """
        return (
            db.query(self.model)
            .filter(
                self.model.id == lg_category_id,
                self.model.is_deleted == False,
                or_(
                    self.model.customer_id == customer_id,
                    self.model.customer_id.is_(None)
                )
            )
            .options(
                selectinload(self.model.entity_associations).selectinload(
                    LGCategoryCustomerEntityAssociation.customer_entity
                )
            )
            .first()
        )

    def create(self, db: Session, obj_in: LGCategoryCreate, user_id: int) -> LGCategory:
        target_customer_id = obj_in.customer_id
        
        # Check for existing categories with the same name or code in the target scope.
        existing_name = self.get_by_name(db, obj_in.name, target_customer_id)
        if existing_name:
             raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Category with name '{obj_in.name}' already exists for this scope.",
            )

        if obj_in.code:
            existing_code = self.get_by_code(db, obj_in.code, target_customer_id)
            if existing_code:
                 raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with code '{obj_in.code}' already exists for this scope.",
                )

        # Ensure only one default category per scope.
        if obj_in.is_default:
            existing_default = self.get_default_category(db, target_customer_id)
            if existing_default:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="A default category for this scope already exists. Please set the 'is_default' flag to false or update the existing default category."
                )

        # Create new object
        lg_category_data = obj_in.model_dump(exclude_unset=True, exclude={"entity_ids"})
        entity_ids = obj_in.entity_ids if obj_in.entity_ids is not None else []
        
        # Add the customer_id to the dictionary
        lg_category_data["customer_id"] = target_customer_id
        
        db_obj = self.model(**lg_category_data)

        db.add(db_obj)
        db.flush()

        if db_obj.customer_id is not None and not db_obj.has_all_entity_access:
            customer_entities = (
                db.query(CustomerEntity)
                .filter(
                    CustomerEntity.id.in_(entity_ids),
                    CustomerEntity.customer_id == db_obj.customer_id,
                    CustomerEntity.is_deleted == False,
                )
                .all()
            )
            if len(customer_entities) != len(entity_ids):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="One or more provided entity IDs are invalid or do not belong to this customer.",
                )

            for entity_id in entity_ids:
                association = LGCategoryCustomerEntityAssociation(
                    lg_category_id=db_obj.id, customer_entity_id=entity_id
                )
                db.add(association)

        db.refresh(db_obj)
        
        log_action(
            db,
            user_id=user_id,
            action_type="CREATE",
            entity_type="LGCategory",
            entity_id=db_obj.id,
            details={
                "name": db_obj.name,
                "code": db_obj.code,
                "customer_id": db_obj.customer_id,
                "is_default": db_obj.is_default,
            },
            customer_id=db_obj.customer_id,
        )
        return db_obj

    def update(self, db: Session, db_obj: LGCategory, obj_in: LGCategoryUpdate, user_id: int) -> LGCategory:
        # Check if the name or code is being changed to an existing one
        update_data = obj_in.model_dump(exclude_unset=True)
        target_customer_id = db_obj.customer_id
        
        if obj_in.name and obj_in.name.lower() != db_obj.name.lower():
            existing_name = self.get_by_name(db, obj_in.name, target_customer_id)
            if existing_name and existing_name.id != db_obj.id:
                 raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with name '{obj_in.name}' already exists for this scope.",
                )

        if obj_in.code and obj_in.code.lower() != db_obj.code.lower():
            existing_code = self.get_by_code(db, obj_in.code, target_customer_id)
            if existing_code and existing_code.id != db_obj.id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with code '{obj_in.code}' already exists for this scope.",
                )
        
        if obj_in.is_default is True and not db_obj.is_default:
            existing_default = self.get_default_category(db, target_customer_id)
            if existing_default and existing_default.id != db_obj.id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="A default category for this scope already exists. Please update the existing one first."
                )

        # Handle entity access changes
        if "has_all_entity_access" in update_data:
            new_has_all_entity_access = update_data.pop("has_all_entity_access")
            
            if new_has_all_entity_access != db_obj.has_all_entity_access:
                db.query(LGCategoryCustomerEntityAssociation).filter(
                    LGCategoryCustomerEntityAssociation.lg_category_id == db_obj.id
                ).delete()
                db_obj.has_all_entity_access = new_has_all_entity_access
                db.flush()
                db.refresh(db_obj, attribute_names=['entity_associations'])
            
            if db_obj.customer_id is not None and not new_has_all_entity_access:
                new_entity_ids = update_data.pop("entity_ids", [])
                customer_entities = (
                    db.query(CustomerEntity)
                    .filter(
                        CustomerEntity.id.in_(new_entity_ids),
                        CustomerEntity.customer_id == db_obj.customer_id,
                        CustomerEntity.is_deleted == False,
                    )
                    .all()
                )
                if len(customer_entities) != len(new_entity_ids):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="One or more provided entity IDs are invalid or do not belong to this customer.",
                    )

                current_entity_ids = {
                    assoc.customer_entity_id for assoc in db_obj.entity_associations
                }

                for entity_id in new_entity_ids:
                    if entity_id not in current_entity_ids:
                        association = LGCategoryCustomerEntityAssociation(
                            lg_category_id=db_obj.id, customer_entity_id=entity_id
                        )
                        db.add(association)

                for entity_id in current_entity_ids:
                    if entity_id not in new_entity_ids:
                        db.query(LGCategoryCustomerEntityAssociation).filter(
                            LGCategoryCustomerEntityAssociation.lg_category_id
                            == db_obj.id,
                            LGCategoryCustomerEntityAssociation.customer_entity_id
                            == entity_id,
                        ).delete()
        
        db.flush()

        updated_category = super().update(db, db_obj, update_data)
        
        log_action(
            db,
            user_id=user_id,
            action_type="UPDATE",
            entity_type="LGCategory",
            entity_id=db_obj.id,
            details={"name": db_obj.name, "code": db_obj.code, "customer_id": db_obj.customer_id, "changes": update_data},
            customer_id=db_obj.customer_id,
        )

        return updated_category

    def soft_delete(self, db: Session, db_obj: LGCategory, user_id: int) -> LGCategory:
        if db_obj.is_default:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete a default category. Please set a new default first.")
        
        active_lg_records = db.query(LGRecord).filter(
            LGRecord.lg_category_id == db_obj.id,
            LGRecord.is_deleted == False
        ).first()

        if active_lg_records:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete category: active LG Records are still associated with it.")

        deleted_category = super().soft_delete(db, db_obj)

        log_action(
            db,
            user_id=user_id,
            action_type="SOFT_DELETE",
            entity_type="LGCategory",
            entity_id=deleted_category.id,
            details={
                "name": deleted_category.name,
                "code": deleted_category.code,
                "customer_id": deleted_category.customer_id,
            },
            customer_id=deleted_category.customer_id,
        )
        return deleted_category

    def restore(self, db: Session, db_obj: LGCategory, user_id: int) -> LGCategory:
        restored_category = super().restore(db, db_obj)

        log_action(
            db,
            user_id=user_id,
            action_type="RESTORE",
            entity_type="LGCategory",
            entity_id=restored_category.id,
            details={
                "name": restored_category.name,
                "code": restored_category.code,
                "customer_id": restored_category.customer_id,
            },
            customer_id=restored_category.customer_id,
        )
        return restored_category