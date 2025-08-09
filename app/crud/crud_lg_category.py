# app/crud/crud_lg_category.py
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type, Tuple
from fastapi import HTTPException, status
from sqlalchemy import func
from sqlalchemy.orm import Session, selectinload

from app.crud.crud import CRUDBase, log_action
import app.models as models
from app.models import LGCategory, CustomerEntity, LGRecord, LGCategoryCustomerEntityAssociation
from app.schemas.all_schemas import LGCategoryCreate, LGCategoryUpdate

import logging
logger = logging.getLogger(__name__)

class CRUDLGCategory(CRUDBase):
    def get_by_name_for_customer(
        self, db: Session, customer_id: int, category_name: str
    ) -> Optional[LGCategory]:
        return (
            db.query(self.model)
            .filter(
                self.model.customer_id == customer_id,
                func.lower(self.model.category_name) == func.lower(category_name),
            )
            .first()
        )

    def get_by_code_for_customer(
        self, db: Session, customer_id: int, code: str
    ) -> Optional[LGCategory]:
        return (
            db.query(self.model)
            .filter(
                self.model.customer_id == customer_id, func.lower(self.model.code) == func.lower(code)
            )
            .first()
        )

    def get_all_for_customer(
        self, db: Session, customer_id: int, skip: int = 0, limit: int = 100
    ) -> List[LGCategory]:
        return (
            db.query(self.model)
            .filter(self.model.customer_id == customer_id, self.model.is_deleted == False)
            .options(
                selectinload(LGCategory.entity_associations).selectinload(
                    LGCategoryCustomerEntityAssociation.customer_entity
                )
            )
            .offset(skip)
            .limit(limit)
            .all()
        )

    def get_for_customer(
        self, db: Session, lg_category_id: int, customer_id: int
    ) -> Optional[LGCategory]:
        return (
            db.query(self.model)
            .filter(
                self.model.id == lg_category_id,
                self.model.customer_id == customer_id,
                self.model.is_deleted == False,
            )
            .options(
                selectinload(LGCategory.entity_associations).selectinload(
                    LGCategoryCustomerEntityAssociation.customer_entity
                )
            )
            .first()
        )

    def create(self, db: Session, obj_in: LGCategoryCreate, customer_id: int, user_id: int) -> LGCategory:
        existing_name = self.get_by_name_for_customer(
            db, customer_id, obj_in.category_name
        )
        if existing_name:
            if existing_name.is_deleted:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with name '{obj_in.category_name}' already exists for this customer but is deleted. Please restore it if needed.",
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with name '{obj_in.category_name}' already exists for this customer.",
                )

        existing_code = self.get_by_code_for_customer(db, customer_id, obj_in.code)
        if existing_code:
            if existing_code.is_deleted:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with code '{obj_in.code}' already exists for this customer but is deleted. Please restore it if needed.",
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with code '{obj_in.code}' already exists for this customer.",
                )

        lg_category_data = obj_in.model_dump(exclude_unset=True)
        entity_ids = lg_category_data.pop("entity_ids", [])

        db_obj = self.model(customer_id=customer_id, **lg_category_data)

        db.add(db_obj)
        db.flush()

        if not db_obj.has_all_entity_access and entity_ids:
            customer_entities = (
                db.query(CustomerEntity)
                .filter(
                    CustomerEntity.id.in_(entity_ids),
                    CustomerEntity.customer_id == customer_id,
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
            details={"category_name": db_obj.category_name, "code": db_obj.code, "customer_id": db_obj.customer_id, "has_all_entity_access": db_obj.has_all_entity_access, "entity_ids": entity_ids,},
            customer_id=customer_id,
        )
        return db_obj

    def update(self, db: Session, db_obj: LGCategory, obj_in: LGCategoryUpdate, user_id: int) -> LGCategory:
        if obj_in.category_name is not None and obj_in.category_name.lower() != db_obj.category_name.lower():
            existing_name = self.get_by_name_for_customer(
                db, db_obj.customer_id, obj_in.category_name
            )
            if existing_name and existing_name.id != db_obj.id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with name '{obj_in.category_name}' already exists for this customer.",
                )

        if obj_in.code is not None and obj_in.code.lower() != db_obj.code.lower():
            existing_code = self.get_by_code_for_customer(
                db, db_obj.customer_id, obj_in.code
            )
            if existing_code and existing_code.id != db_obj.id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with code '{obj_in.code}' already exists for this customer.",
                )

        old_data_for_log = obj_in.model_dump(exclude_unset=True)

        if "has_all_entity_access" in old_data_for_log:
            new_has_all_entity_access = old_data_for_log.pop("has_all_entity_access")
            new_entity_ids = old_data_for_log.pop("entity_ids", [])

            if new_has_all_entity_access != db_obj.has_all_entity_access:
                db.query(LGCategoryCustomerEntityAssociation).filter(
                    LGCategoryCustomerEntityAssociation.lg_category_id == db_obj.id
                ).delete()

                db_obj.has_all_entity_access = new_has_all_entity_access
                db.flush()

                db.refresh(db_obj, attribute_names=['entity_associations'])


            if not db_obj.has_all_entity_access:
                customer_entities = (
                    db.query(CustomerEntity)
                    .filter(
                        CustomerEntity.id.in_(new_entity_ids), # Use new_entity_ids here
                        CustomerEntity.customer_id == db_obj.customer_id,
                        CustomerEntity.is_deleted == False,
                    )
                    .all()
                )
                if len(customer_entities) != len(new_entity_ids): # Compare with new_entity_ids
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

                # Corrected logic to remove deselected entities
                for entity_id in current_entity_ids:
                    if entity_id not in new_entity_ids:
                        db.query(LGCategoryCustomerEntityAssociation).filter(
                            LGCategoryCustomerEntityAssociation.lg_category_id
                            == db_obj.id,
                            LGCategoryCustomerEntityAssociation.customer_entity_id
                            == entity_id,
                        ).delete()
            db.flush()

        # Update other fields from obj_in
        for field, value in old_data_for_log.items():
            setattr(db_obj, field, value)

        db_obj.updated_at = func.now()
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)

        log_action(
            db,
            user_id=user_id,
            action_type="UPDATE",
            entity_type="LGCategory",
            entity_id=db_obj.id,
            details={"category_name": db_obj.category_name, "code": db_obj.code, "customer_id": db_obj.customer_id, "changes": old_data_for_log},
            customer_id=db_obj.customer_id,
        )

        return db_obj

    def soft_delete(self, db: Session, db_obj: LGCategory, user_id: int) -> LGCategory:
        if db_obj.customer_id is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Universal categories cannot be deleted by Corporate Admin.")

        active_lg_records = db.query(LGRecord).filter(
            LGRecord.lg_category_id == db_obj.id,
            LGRecord.is_deleted == False
        ).first()

        if active_lg_records:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete category: active LG Records are still associated with it.")

        deleted_category = super().soft_delete(db, db_obj)

        # Soft delete associated entity access entries
        db.query(LGCategoryCustomerEntityAssociation).filter(
            LGCategoryCustomerEntityAssociation.lg_category_id == deleted_category.id
        ).update(
            {"is_deleted": True, "deleted_at": func.now()}, synchronize_session=False
        )
        db.flush()

        log_action(
            db,
            user_id=user_id,
            action_type="SOFT_DELETE",
            entity_type="LGCategory",
            entity_id=deleted_category.id,
            details={
                "category_name": deleted_category.category_name,
                "code": deleted_category.code,
                "customer_id": deleted_category.customer_id,
            },
            customer_id=deleted_category.customer_id,
        )
        return deleted_category

    def restore(self, db: Session, db_obj: LGCategory, user_id: int) -> LGCategory:
        if db_obj.customer_id is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Universal categories cannot be restored by Corporate Admin.")

        restored_category = super().restore(db, db_obj)

        # Restore associated entity access entries
        db.query(LGCategoryCustomerEntityAssociation).filter(
            LGCategoryCustomerEntityAssociation.lg_category_id == restored_category.id
        ).update(
            {"is_deleted": False, "deleted_at": None}, synchronize_session=False
        )
        db.flush()

        log_action(
            db,
            user_id=user_id,
            action_type="RESTORE",
            entity_type="LGCategory",
            entity_id=restored_category.id,
            details={
                "category_name": restored_category.category_name,
                "code": restored_category.code,
                "customer_id": restored_category.customer_id,
            },
            customer_id=restored_category.customer_id,
        )
        return restored_category