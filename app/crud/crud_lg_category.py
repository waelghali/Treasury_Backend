import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type, Tuple
from fastapi import HTTPException, status
from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.orm.attributes import flag_modified

from app.crud.crud import CRUDBase, log_action
from app.models import (
    LGCategory,
    Customer,
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
        scope_filter = (
            self.model.customer_id.is_(None)
            if customer_id is None
            else self.model.customer_id == customer_id
        )
        return (
            db.query(self.model)
            .filter(
                self.model.is_default == True,
                scope_filter,
                self.model.is_deleted == False
            )
            .first()
        )

    def get_effective_default_category(
        self, db: Session, customer_id: Optional[int]
    ) -> Optional[LGCategory]:
        """
        Resolves the effective default category for a customer.
        Precedence:
        1. Customer.default_lg_category_id (can reference a customer-specific OR universal category)
        2. Active customer-specific LGCategory with is_default=True
        3. Active Universal LGCategory (customer_id IS NULL) with is_default=True
        """
        if customer_id is not None:
            customer = (
                db.query(Customer)
                .filter(Customer.id == customer_id, Customer.is_deleted == False)
                .first()
            )
            if customer and customer.default_lg_category_id:
                cat = self.get_for_customer(db, customer.default_lg_category_id, customer_id)
                if cat and cat.has_all_entity_access:
                    return cat

            cust_default = (
                db.query(self.model)
                .filter(
                    self.model.customer_id == customer_id,
                    self.model.is_default == True,
                    self.model.is_deleted == False,
                    self.model.has_all_entity_access == True,
                )
                .first()
            )
            if cust_default:
                return cust_default

        return (
            db.query(self.model)
            .filter(
                self.model.customer_id.is_(None),
                self.model.is_default == True,
                self.model.is_deleted == False,
            )
            .first()
        )

    def _unset_scope_defaults(
        self, db: Session, target_customer_id: Optional[int], exclude_id: Optional[int] = None
    ) -> None:
        scope_filter = (
            self.model.customer_id.is_(None)
            if target_customer_id is None
            else self.model.customer_id == target_customer_id
        )
        q = db.query(self.model).filter(scope_filter, self.model.is_default == True)
        if exclude_id is not None:
            q = q.filter(self.model.id != exclude_id)
        q.update({"is_default": False}, synchronize_session="fetch")
        
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

        if target_customer_id is None:
            if not obj_in.has_all_entity_access:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Universal categories must have 'has_all_entity_access' set to True.",
                )
            if obj_in.entity_ids:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Universal categories cannot have 'entity_ids'.",
                )
        
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

        if obj_in.is_default and not obj_in.has_all_entity_access:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A default category must apply to all entities ('has_all_entity_access' must be True).",
            )

        # Auto-unset any existing default category in the same scope (one default per scope).
        if obj_in.is_default:
            self._unset_scope_defaults(db, target_customer_id)

        # Create new object
        lg_category_data = obj_in.model_dump(exclude_unset=True, exclude={"entity_ids"})
        entity_ids = obj_in.entity_ids if obj_in.entity_ids is not None else []
        
        # Add the customer_id to the dictionary
        lg_category_data["customer_id"] = target_customer_id
        if "extra_field_name" in lg_category_data:
            cleaned_extra = (lg_category_data.get("extra_field_name") or "").strip()
            lg_category_data["extra_field_name"] = cleaned_extra if cleaned_extra else None
        if not lg_category_data.get("extra_field_name"):
            lg_category_data["is_mandatory"] = False
        
        db_obj = self.model(**lg_category_data)

        db.add(db_obj)
        db.flush()

        if db_obj.customer_id is not None and db_obj.is_default:
            customer = db.query(Customer).filter(Customer.id == db_obj.customer_id).first()
            if customer:
                customer.default_lg_category_id = db_obj.id
                db.add(customer)
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

        if obj_in.code and obj_in.code.lower() != (db_obj.code or "").lower():
            existing_code = self.get_by_code(db, obj_in.code, target_customer_id)
            if existing_code and existing_code.id != db_obj.id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Category with code '{obj_in.code}' already exists for this scope.",
                )

        customer = (
            db.query(Customer).filter(Customer.id == db_obj.customer_id).first()
            if db_obj.customer_id is not None
            else None
        )
        currently_customer_default = bool(
            db_obj.is_default or (customer and customer.default_lg_category_id == db_obj.id)
        )
        effective_is_default = (
            update_data["is_default"]
            if "is_default" in update_data and update_data["is_default"] is not None
            else currently_customer_default
        )
        effective_all_access = (
            update_data["has_all_entity_access"]
            if "has_all_entity_access" in update_data and update_data["has_all_entity_access"] is not None
            else db_obj.has_all_entity_access
        )
        if effective_is_default and not effective_all_access:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A default category must apply to all entities. Please unset default first or keep 'Applies to All Entities' enabled.",
            )

        old_extra_name = (db_obj.extra_field_name or "").strip() or None

        if "extra_field_name" in update_data:
            cleaned_extra = (update_data["extra_field_name"] or "").strip()
            update_data["extra_field_name"] = cleaned_extra if cleaned_extra else None
            if not update_data["extra_field_name"]:
                update_data["is_mandatory"] = False
        elif update_data.get("is_mandatory") is True and not db_obj.extra_field_name:
            update_data["is_mandatory"] = False

        if update_data.get("is_default") is True:
            self._unset_scope_defaults(db, target_customer_id, exclude_id=db_obj.id)
            if customer:
                customer.default_lg_category_id = db_obj.id
                db.add(customer)
        elif update_data.get("is_default") is False:
            if customer and customer.default_lg_category_id == db_obj.id:
                customer.default_lg_category_id = None
                db.add(customer)

        # Handle entity access changes
        if "has_all_entity_access" in update_data or (
            "entity_ids" in update_data and not effective_all_access
        ):
            new_has_all_entity_access = update_data.pop(
                "has_all_entity_access", db_obj.has_all_entity_access
            )
            
            if new_has_all_entity_access != db_obj.has_all_entity_access:
                db.query(LGCategoryCustomerEntityAssociation).filter(
                    LGCategoryCustomerEntityAssociation.lg_category_id == db_obj.id
                ).delete()
                db_obj.has_all_entity_access = new_has_all_entity_access
                db.flush()
                db.refresh(db_obj, attribute_names=['entity_associations'])
            
            if db_obj.customer_id is not None and not new_has_all_entity_access:
                new_entity_ids = update_data.pop("entity_ids", None)
                if new_entity_ids is None:
                    new_entity_ids = []
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

                conflict_query = db.query(LGRecord.lg_number).filter(
                    LGRecord.lg_category_id == db_obj.id,
                    LGRecord.is_deleted == False,
                )
                if new_entity_ids:
                    conflict_query = conflict_query.filter(
                        ~LGRecord.beneficiary_corporate_id.in_(new_entity_ids)
                    )
                conflicting_lgs = conflict_query.all()
                if conflicting_lgs:
                    sample = ", ".join(r[0] for r in conflicting_lgs[:5])
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=(
                            f"Cannot restrict entity access: {len(conflicting_lgs)} existing LG(s) in this category "
                            f"belong to excluded entities ({sample}). Please migrate those LGs to another category first."
                        ),
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
            elif "entity_ids" in update_data:
                update_data.pop("entity_ids", None)
        elif "entity_ids" in update_data:
            update_data.pop("entity_ids", None)
        
        db.flush()

        updated_category = super().update(db, db_obj, update_data)

        new_extra_name = (updated_category.extra_field_name or "").strip() or None
        propagated_lg_count = 0
        if old_extra_name != new_extra_name and (old_extra_name is not None):
            linked_lgs = (
                db.query(LGRecord)
                .filter(
                    LGRecord.lg_category_id == updated_category.id,
                    LGRecord.is_deleted == False,
                )
                .all()
            )
            for lg in linked_lgs:
                if not isinstance(lg.additional_field_values, dict) or not lg.additional_field_values:
                    continue
                prev_dict = dict(lg.additional_field_values)
                if new_extra_name is not None:
                    # In-place rename: move existing value under the new key immediately
                    raw_val = prev_dict.get(old_extra_name)
                    if raw_val in (None, ""):
                        for v in prev_dict.values():
                            if v not in (None, "") and str(v).strip():
                                raw_val = v
                                break
                    cleaned_val = str(raw_val).strip() if raw_val not in (None, "") else ""
                    lg.additional_field_values = {new_extra_name: cleaned_val} if cleaned_val else None
                    flag_modified(lg, "additional_field_values")
                    db.add(lg)
                    propagated_lg_count += 1
                else:
                    # Extra field removed: clear active JSON and archive old value in AuditLog
                    lg.additional_field_values = None
                    flag_modified(lg, "additional_field_values")
                    db.add(lg)
                    propagated_lg_count += 1
                    log_action(
                        db,
                        user_id=user_id,
                        action_type="LG_CATEGORY_EXTRA_FIELD_CLEARED",
                        entity_type="LGRecord",
                        entity_id=lg.id,
                        details={
                            "lg_number": lg.lg_number,
                            "category_id": updated_category.id,
                            "category_name": updated_category.name,
                            "removed_extra_field_name": old_extra_name,
                            "old_additional_field_values": prev_dict,
                        },
                        customer_id=lg.customer_id,
                        lg_record_id=lg.id,
                    )
            if propagated_lg_count > 0:
                db.flush()

        log_action(
            db,
            user_id=user_id,
            action_type="UPDATE",
            entity_type="LGCategory",
            entity_id=db_obj.id,
            details={
                "name": db_obj.name,
                "code": db_obj.code,
                "customer_id": db_obj.customer_id,
                "changes": update_data,
                "propagated_lg_records_count": propagated_lg_count,
            },
            customer_id=db_obj.customer_id,
        )

        return updated_category

    def set_customer_default_category(
        self, db: Session, customer_id: int, category_id: Optional[int], user_id: int
    ) -> Optional[LGCategory]:
        """
        Sets (or clears when category_id is None) the default LG Category for a customer.
        Supports pointing to either a customer-specific category or a universal category.
        """
        customer = (
            db.query(Customer)
            .filter(Customer.id == customer_id, Customer.is_deleted == False)
            .first()
        )
        if not customer:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found."
            )

        prev_default_id = customer.default_lg_category_id

        if category_id is None:
            self._unset_scope_defaults(db, customer_id)
            customer.default_lg_category_id = None
            db.add(customer)
            db.flush()
            log_action(
                db,
                user_id=user_id,
                action_type="CLEAR_DEFAULT_CATEGORY",
                entity_type="LGCategory",
                entity_id=prev_default_id,
                details={
                    "customer_id": customer_id,
                    "previous_default_category_id": prev_default_id,
                },
                customer_id=customer_id,
            )
            return None

        cat = self.get_for_customer(db, category_id, customer_id)
        if not cat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="LG Category not found or not accessible.",
            )
        if not cat.has_all_entity_access:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A default category must apply to all entities ('Applies to All Entities' must be enabled).",
            )

        self._unset_scope_defaults(
            db, customer_id, exclude_id=cat.id if cat.customer_id == customer_id else None
        )
        if cat.customer_id == customer_id:
            cat.is_default = True
            db.add(cat)

        customer.default_lg_category_id = cat.id
        db.add(customer)
        db.flush()
        db.refresh(cat)

        log_action(
            db,
            user_id=user_id,
            action_type="SET_DEFAULT_CATEGORY",
            entity_type="LGCategory",
            entity_id=cat.id,
            details={
                "name": cat.name,
                "code": cat.code,
                "customer_id": customer_id,
                "category_scope": "universal" if cat.customer_id is None else "customer",
                "previous_default_category_id": prev_default_id,
            },
            customer_id=customer_id,
        )
        return cat

    def soft_delete(self, db: Session, db_obj: LGCategory, user_id: int) -> LGCategory:
        is_customer_default_ref = (
            db_obj.customer_id is not None
            and db.query(Customer)
            .filter(
                Customer.id == db_obj.customer_id,
                Customer.default_lg_category_id == db_obj.id,
            )
            .first()
            is not None
        )
        if db_obj.is_default or is_customer_default_ref:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete a default category. Please set a new default first.")
        
        active_lg_records = db.query(LGRecord).filter(
            LGRecord.lg_category_id == db_obj.id,
            LGRecord.is_deleted == False
        ).first()

        if active_lg_records:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete category: active LG Records are still associated with it.")

        deleted_category = super().soft_delete(db, db_obj)

        db.query(Customer).filter(
            Customer.default_lg_category_id == deleted_category.id
        ).update({"default_lg_category_id": None}, synchronize_session="fetch")

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