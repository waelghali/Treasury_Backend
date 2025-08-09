# app/crud/crud_user.py
from typing import Any, List, Optional, Type
from fastapi import HTTPException, status
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func
from app.crud.crud import CRUDBase, log_action
from app.models import ( # Ensure all necessary models are imported directly
    Customer,
    CustomerEntity,
    User,
    UserCustomerEntityAssociation,
)
from app.schemas.all_schemas import UserCreate, UserCreateCorporateAdmin, UserUpdate, UserUpdateCorporateAdmin
from app.constants import UserRole


# =====================================================================================
# User Management
# =====================================================================================
class CRUDUser(CRUDBase):
    def get_by_email(self, db: Session, email: str) -> Optional[User]:
        return (
            db.query(self.model)
            .filter(self.model.email == email, self.model.is_deleted == False)
            .first()
        )

    # NEW METHOD: Get users by role for a specific customer
    def get_users_by_role_for_customer(self, db: Session, customer_id: int, role: UserRole) -> List[User]:
        """
        Retrieves active users of a specific role for a given customer.
        """
        return (
            db.query(self.model)
            .filter(
                self.model.customer_id == customer_id,
                self.model.role == role,
                self.model.is_deleted == False
            )
            .all()
        )

    def create_user(self, db: Session, user_in: UserCreate, user_id_caller: Optional[int] = None) -> User:
        customer = (
            db.query(Customer)
            .options(selectinload(Customer.subscription_plan))
            .filter(Customer.id == user_in.customer_id)
            .first()
        )
        if not customer:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found.")

        if customer.active_user_count >= customer.subscription_plan.max_users:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"User limit ({customer.subscription_plan.max_users}) exceeded for this customer's subscription plan. Cannot create new user.",
            )

        user_data = user_in.model_dump(exclude_unset=True)
        password = user_data.pop("password")
        entity_ids = user_data.pop("entity_ids", [])

        db_user = self.model(**user_data)
        db_user.set_password(password)

        db.add(db_user)
        db.flush()

        if not db_user.has_all_entity_access and entity_ids:
            customer_entities = (
                db.query(CustomerEntity)
                .filter(
                    CustomerEntity.id.in_(entity_ids),
                    CustomerEntity.customer_id == db_user.customer_id,
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
                association = UserCustomerEntityAssociation(
                    user_id=db_user.id, customer_entity_id=entity_id
                )
                db.add(association)

        customer.active_user_count += 1
        db.add(customer)
        db.flush()
        db.refresh(db_user)
        db.refresh(customer)

        log_action(
            db,
            user_id=user_id_caller,
            action_type="CREATE",
            entity_type="User",
            entity_id=db_user.id,
            details={
                "email": db_user.email,
                "role": db_user.role,
                "customer_id": db_user.customer_id,
                "has_all_entity_access": db_user.has_all_entity_access,
                "entity_ids": entity_ids,
            },
            customer_id=db_user.customer_id,
        )
        return db_user

    def create_user_by_corporate_admin(self, db: Session, user_in: UserCreateCorporateAdmin, customer_id: int, user_id_caller: int) -> User:
        customer = (
            db.query(Customer)
            .options(selectinload(Customer.subscription_plan))
            .filter(Customer.id == customer_id)
            .first()
        )
        if not customer:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found.")

        if customer.active_user_count >= customer.subscription_plan.max_users:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"User limit ({customer.subscription_plan.max_users}) exceeded for this customer's subscription plan. Cannot create new user.",
            )

        if user_in.role == UserRole.SYSTEM_OWNER:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Corporate Admins cannot create users with 'SYSTEM_OWNER' role.",
            )

        user_data = user_in.model_dump(exclude_unset=True)
        password = user_data.pop("password")
        entity_ids = user_data.pop("entity_ids", [])

        user_data.pop("must_change_password", None)

        db_user = self.model(must_change_password=True, customer_id=customer_id, **user_data)
        db_user.set_password(password)

        db.add(db_user)
        db.flush()

        if not db_user.has_all_entity_access and entity_ids:
            # REMOVED: Redundant late import, CustomerEntity is directly imported at top
            # import app.crud.crud as crud_instances
            customer_entities = (
                db.query(CustomerEntity)
                .filter(
                    CustomerEntity.id.in_(entity_ids),
                    CustomerEntity.customer_id == db_user.customer_id,
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
                association = UserCustomerEntityAssociation(
                    user_id=db_user.id, customer_entity_id=entity_id
                )
                db.add(association)

        customer.active_user_count += 1
        db.add(customer)
        db.flush()
        db.refresh(db_user)
        db.refresh(customer)

        log_action(
            db,
            user_id=user_id_caller,
            action_type="CREATE_BY_CA",
            entity_type="User",
            entity_id=db_user.id,
            details={
                "email": db_user.email,
                "role": db_user.role,
                "customer_id": db_user.customer_id,
                "has_all_entity_access": db_user.has_all_entity_access,
                "entity_ids": entity_ids,
            },
            customer_id=db_user.customer_id,
        )
        return db_user

    def update_user(self, db: Session, db_user: User, user_in: UserUpdate, user_id_caller: Optional[int] = None) -> User:
        old_data_for_log = user_in.model_dump(exclude_unset=True)

        if "password" in old_data_for_log and old_data_for_log["password"]:
            db_user.set_password(old_data_for_log.pop("password"))

        if "has_all_entity_access" in old_data_for_log:
            new_has_all_entity_access = old_data_for_log.pop("has_all_entity_access")
            new_entity_ids = old_data_for_log.pop("entity_ids", [])

            if new_has_all_entity_access != db_user.has_all_entity_access:
                db.query(UserCustomerEntityAssociation).filter(
                    UserCustomerEntityAssociation.user_id == db_user.id
                ).delete()
                db_user.has_all_entity_access = new_has_all_entity_access
                db.flush()
            if not db_user.has_all_entity_access:
                # REMOVED: Redundant late import, CustomerEntity is directly imported at top
                # import app.crud.crud as crud_instances
                customer_entities = (
                    db.query(CustomerEntity)
                    .filter(
                        CustomerEntity.id.in_(new_entity_ids),
                        CustomerEntity.customer_id == db_user.customer_id,
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
                    assoc.customer_entity_id for assoc in db_user.entity_associations
                }

                for entity_id in new_entity_ids:
                    if entity_id not in current_entity_ids:
                        association = UserCustomerEntityAssociation(
                            user_id=db_user.id, customer_entity_id=entity_id
                        )
                        db.add(association)

                for entity_id in current_entity_ids:
                    if entity_id not in new_entity_ids:
                        db.query(UserCustomerEntityAssociation).filter(
                            UserCustomerEntityAssociation.user_id == db_user.id,
                            UserCustomerEntityAssociation.customer_entity_id == entity_id,
                        ).delete()
            db.flush()

        for field, value in old_data_for_log.items():
            setattr(db_user, field, value)

        db_user.updated_at = func.now()
        db.add(db_user)
        db.flush()
        db.refresh(db_user)

        new_data_for_log = {
            k: getattr(db_user, k)
            for k in user_in.model_dump(exclude_unset=True).keys()
            if hasattr(db_user, k)
        }

        changed_fields = {}
        for key in user_in.model_dump(exclude_unset=True).keys():
            if key == "password":
                if user_in.password:
                    changed_fields["password"] = {"old": "[HIDDEN]", "new": "[SET]"}
            elif key == "entity_ids":
                old_entities = sorted(
                    [
                        assoc.customer_entity_id
                        for assoc in db.query(UserCustomerEntityAssociation)
                        .filter(UserCustomerEntityAssociation.user_id == db_user.id)
                        .all()
                    ]
                )
                new_entities = sorted(new_entity_ids)
                if old_entities != new_entities:
                    changed_fields["entities_with_access"] = {
                        "old": old_entities,
                        "new": new_entities,
                    }
            elif key == "has_all_entity_access":
                if (
                    user_in.has_all_entity_access is not None
                    and db_user.has_all_entity_access != user_in.has_all_entity_access
                ):
                    changed_fields["has_all_entity_access"] = {
                        "old": not user_in.has_all_entity_access,
                        "new": user_in.has_all_entity_access,
                    }
            elif key in new_data_for_log and new_data_for_log.get(key) != old_data_for_log.get(key):
                changed_fields[key] = {"old": old_data_for_log.get(key), "new": new_data_for_log.get(key)}

        if changed_fields:
            log_action(
                db,
                user_id=user_id_caller,
                action_type="UPDATE",
                entity_type="User",
                entity_id=db_user.id,
                details={"email": db_user.email, "changes": changed_fields},
                customer_id=db_user.customer_id,
            )
        return db_user

    # NEW METHOD: Update user by Corporate Admin
    def update_user_by_corporate_admin(self, db: Session, db_user: User, user_in: UserUpdateCorporateAdmin, customer_id: int, user_id_caller: int) -> User:
        # 1. Basic validation: Ensure user belongs to the current corporate admin's customer
        if db_user.customer_id != customer_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User does not belong to your organization."
            )

        # 2. Prevent Corporate Admin from changing to SYSTEM_OWNER role
        if user_in.role == UserRole.SYSTEM_OWNER:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Corporate Admins cannot assign 'SYSTEM_OWNER' role."
            )

        # 3. Prevent Corporate Admin from changing other CAs' roles (or their own)
        if db_user.role == UserRole.CORPORATE_ADMIN and user_in.role != UserRole.CORPORATE_ADMIN:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Corporate Admins cannot change the role of other Corporate Admins."
            )
        if db_user.id == user_id_caller and user_in.role != UserRole.CORPORATE_ADMIN:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Corporate Admin cannot change their own role from 'CORPORATE_ADMIN'."
            )

        # Proceed with updating common fields
        update_data = user_in.model_dump(exclude_unset=True)
        changed_fields = {} # To track changes for audit logging

        # Handle password update if provided
        if "password" in update_data and update_data["password"]:
            db_user.set_password(update_data.pop("password"))
            changed_fields["password"] = {"old": "[HIDDEN]", "new": "[SET]"}
            # If password is changed, must_change_password flag might be relevant (e.g., reset)
            if user_in.must_change_password is not None:
                if db_user.must_change_password != user_in.must_change_password:
                    changed_fields["must_change_password"] = {
                        "old": db_user.must_change_password,
                        "new": user_in.must_change_password
                    }
                db_user.must_change_password = user_in.must_change_password
            else: # If password is set but must_change_password is not explicitly provided, default to False
                if db_user.must_change_password: # Only change if currently True
                    changed_fields["must_change_password"] = {
                        "old": db_user.must_change_password,
                        "new": False
                    }
                db_user.must_change_password = False # Assume setting password means they don't have to change immediately


        # Handle entity access
        if "has_all_entity_access" in update_data:
            new_has_all_entity_access = update_data.pop("has_all_entity_access")
            new_entity_ids = update_data.pop("entity_ids", []) # Will be empty list if not provided

            old_has_all_entity_access = db_user.has_all_entity_access
            old_entity_ids_set = {assoc.customer_entity_id for assoc in db_user.entity_associations}


            if new_has_all_entity_access != old_has_all_entity_access:
                # If access type changed, clear existing associations
                db.query(UserCustomerEntityAssociation).filter(
                    UserCustomerEntityAssociation.user_id == db_user.id
                ).delete()
                db_user.has_all_entity_access = new_has_all_entity_access
                db.flush() # Flush to ensure deletions are processed before new additions

                changed_fields["has_all_entity_access"] = {
                    "old": old_has_all_entity_access,
                    "new": new_has_all_entity_access
                }
                
                # If changing from all access to specific access, and new_entity_ids are provided
                if not new_has_all_entity_access and new_entity_ids:
                     # Validate new entity IDs
                    # REMOVED: Redundant late import, CustomerEntity is directly imported at top
                    # import app.crud.crud as crud_instances
                    customer_entities = (
                        db.query(CustomerEntity)
                        .filter(
                            CustomerEntity.id.in_(new_entity_ids),
                            CustomerEntity.customer_id == db_user.customer_id,
                            CustomerEntity.is_deleted == False,
                        )
                        .all()
                    )
                    if len(customer_entities) != len(new_entity_ids):
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="One or more provided entity IDs for granular access are invalid or do not belong to this customer."
                        )
                    # Add new associations
                    for entity_id in new_entity_ids:
                        association = UserCustomerEntityAssociation(
                            user_id=db_user.id, customer_entity_id=entity_id
                        )
                        db.add(association)
                    
                    changed_fields["entities_with_access"] = {"old": sorted(list(old_entity_ids_set)), "new": sorted(new_entity_ids)}

            elif not new_has_all_entity_access: # Access type did not change, still granular, but entities changed
                # Validate new entity IDs even if type didn't change
                if new_entity_ids: # Only validate if list is not empty
                    # REMOVED: Redundant late import, CustomerEntity is directly imported at top
                    # import app.crud.crud as crud_instances
                    customer_entities = (
                        db.query(CustomerEntity)
                        .filter(
                            CustomerEntity.id.in_(new_entity_ids),
                            CustomerEntity.customer_id == db_user.customer_id,
                            CustomerEntity.is_deleted == False,
                        )
                        .all()
                    )
                    if len(customer_entities) != len(new_entity_ids):
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="One or more provided entity IDs for granular access are invalid or do not belong to this customer."
                        )

                current_entity_ids_set = {assoc.customer_entity_id for assoc in db_user.entity_associations}
                new_entity_ids_set = set(new_entity_ids)

                if current_entity_ids_set != new_entity_ids_set:
                    # Add new associations
                    for entity_id in new_entity_ids_set - current_entity_ids_set:
                        association = UserCustomerEntityAssociation(
                            user_id=db_user.id, customer_entity_id=entity_id
                        )
                        db.add(association)
                    # Remove old associations
                    for entity_id in current_entity_ids_set - new_entity_ids_set:
                        db.query(UserCustomerEntityAssociation).filter(
                            UserCustomerEntityAssociation.user_id == db_user.id,
                            UserCustomerEntityAssociation.customer_entity_id == entity_id,
                        ).delete()
                    
                    changed_fields["entities_with_access"] = {
                        "old": sorted(list(old_entity_ids_set)),
                        "new": sorted(list(new_entity_ids_set))
                    }
            db.flush() # Flush changes to associations

        # Update other fields
        for field, value in update_data.items():
            if field != "password" and field != "has_all_entity_access" and field != "entity_ids": # Already handled these
                if hasattr(db_user, field) and getattr(db_user, field) != value:
                    setattr(db_user, field, value)
                    changed_fields[field] = {"old": getattr(db_user, field), "new": value}

        db_user.updated_at = func.now()
        db.add(db_user)
        db.flush()
        db.refresh(db_user) # Refresh after all changes are flushed

        # Log action based on actual changes
        if changed_fields:
            log_action(
                db,
                user_id=user_id_caller,
                action_type="UPDATE_BY_CA",
                entity_type="User",
                entity_id=db_user.id,
                details={"email": db_user.email, "changes": changed_fields},
                customer_id=db_user.customer_id,
            )
        return db_user


    def soft_delete(self, db: Session, db_obj: User, user_id: Optional[int] = None) -> User:
        deleted_obj = super().soft_delete(db, db_obj)

        if deleted_obj.customer and not deleted_obj.is_deleted:
            deleted_obj.customer.active_user_count -= 1
            db.add(deleted_obj.customer)
            db.flush()

        # CRITICAL FIX: Hard delete UserCustomerEntityAssociation records
        # as they do not support soft deletion (no is_deleted/deleted_at columns).
        # This will lead to loss of granular entity access data upon user restoration.
        db.query(UserCustomerEntityAssociation).filter(
            UserCustomerEntityAssociation.user_id == deleted_obj.id
        ).delete(synchronize_session=False) # Perform hard delete
        db.flush()

        log_action(
            db,
            user_id=user_id,
            action_type="SOFT_DELETE",
            entity_type="User",
            entity_id=deleted_obj.id,
            details={"email": deleted_obj.email},
            customer_id=deleted_obj.customer_id,
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: User, user_id: Optional[int] = None) -> User:
        customer = (
            db.query(Customer)
            .options(selectinload(Customer.subscription_plan))
            .filter(Customer.id == db_obj.customer_id)
            .first()
        )
        if not customer:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Customer not found for user restoration.",
            )

        if customer.active_user_count >= customer.subscription_plan.max_users:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"User limit ({customer.subscription_plan.max_users}) exceeded for this customer's subscription plan. Cannot restore user.",
            )

        restored_obj = super().restore(db, db_obj)

        if restored_obj.customer and not restored_obj.is_deleted:
            restored_obj.customer.active_user_count += 1
            db.add(restored_obj.customer)
            db.flush()

        # IMPORTANT: UserCustomerEntityAssociation records were hard-deleted during soft_delete.
        # They are NOT automatically restored here. If the user had granular entity access
        # (has_all_entity_access=False), that data is lost and must be reconfigured manually by an admin
        # after restoration. This is a known limitation with the current model design for associations.
        # Do NOT attempt to update 'is_deleted' on associations here as they don't have that column.

        log_action(
            db,
            user_id=user_id,
            action_type="RESTORE",
            entity_type="User",
            entity_id=restored_obj.id,
            details={"email": restored_obj.email},
            customer_id=restored_obj.customer_id,
        )
        return restored_obj

    def get_users_by_customer_id(self, db: Session, customer_id: int, skip: int = 0, limit: int = 100) -> List[User]:
        return (
            db.query(self.model)
            .filter(self.model.customer_id == customer_id, self.model.is_deleted == False)
            .options(
                selectinload(User.entity_associations).selectinload(
                    UserCustomerEntityAssociation.customer_entity
                )
            )
            .offset(skip)
            .limit(limit)
            .all()
        )

    def change_password_on_first_login(self, db: Session, db_user: User, new_password: str, user_id_caller: Optional[int] = None) -> User:
        db_user.set_password(new_password)
        db_user.must_change_password = False
        db_user.updated_at = func.now()
        db.add(db_user)
        db.flush()
        db.refresh(db_user)
        log_action(
            db,
            user_id=user_id_caller if user_id_caller else db_user.id,
            action_type="PASSWORD_CHANGE", # Consider using a more specific audit type from constants if available
            entity_type="User",
            entity_id=db_user.id,
            details={"email": db_user.email, "reason": "First login password change"},
            customer_id=db_user.customer_id,
        )
        return db_user