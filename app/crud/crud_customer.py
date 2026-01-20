# app/crud/crud_customer.py
import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Type, Tuple
from fastapi import HTTPException, status
from sqlalchemy import func, and_
from sqlalchemy.orm import Session, selectinload, aliased
from app.core.encryption import encrypt_data, decrypt_data
from app.crud.crud import CRUDBase, log_action
import app.models as models
from app.models import Customer, CustomerEntity, User, SubscriptionPlan, CustomerEmailSetting
from app.schemas.all_schemas import (
    CustomerCreate, CustomerUpdate, CustomerEntityCreate, CustomerEntityUpdate,
    UserCreateCorporateAdmin, CustomerEmailSettingCreate, CustomerEmailSettingUpdate
)
from app.constants import UserRole, AUDIT_ACTION_TYPE_CUSTOMER_ONBOARD, AUDIT_ACTION_TYPE_CREATE, AUDIT_ACTION_TYPE_UPDATE, AUDIT_ACTION_TYPE_SOFT_DELETE, AUDIT_ACTION_TYPE_RESTORE, SubscriptionStatus

import logging
logger = logging.getLogger(__name__)


class CRUDCustomer(CRUDBase):
    def __init__(self, model: Type[Customer], crud_customer_entity_instance: Any, crud_user_instance: Any):
        super().__init__(model)
        self.crud_customer_entity_instance = crud_customer_entity_instance
        self.crud_user_instance = crud_user_instance

    def get_by_name(self, db: Session, name: str) -> Optional[models.Customer]:
        return db.query(self.model).filter(func.lower(self.model.name) == func.lower(name), self.model.is_deleted == False).first()

    def get_by_contact_email(self, db: Session, email: str) -> Optional[models.Customer]:
        return db.query(self.model).filter(func.lower(self.model.contact_email) == func.lower(email), self.model.is_deleted == False).first()

    def get_all_with_relations(self, db: Session, skip: int = 0, limit: int = 100) -> List[models.Customer]:
        return db.query(self.model).options(
            selectinload(self.model.subscription_plan),
            selectinload(self.model.entities),
            selectinload(self.model.users),
            selectinload(self.model.customer_email_settings),
            selectinload(self.model.templates)
        ).offset(skip).limit(limit).all()

    def get_with_relations(self, db: Session, customer_id: int) -> Optional[models.Customer]:
        return db.query(self.model).filter(self.model.id == customer_id, self.model.is_deleted == False).options(
            selectinload(self.model.subscription_plan),
            selectinload(self.model.entities),
            selectinload(self.model.users),
            selectinload(self.model.lg_categories), # Eager load for access checks in UI
            selectinload(self.model.internal_owner_contacts),
            selectinload(self.model.customer_configurations),
            selectinload(self.model.customer_email_settings),
            selectinload(self.model.templates)
        ).first()

    def onboard_customer(self, db: Session, customer_in: CustomerCreate, user_id_caller: int) -> models.Customer:
        # Validate uniqueness of customer name and email
        if self.get_by_name(db, customer_in.name):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Customer with this name already exists.")
        if self.get_by_contact_email(db, customer_in.contact_email):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Customer with this contact email already exists.")

        # Validate subscription plan
        subscription_plan = db.query(models.SubscriptionPlan).filter(models.SubscriptionPlan.id == customer_in.subscription_plan_id, models.SubscriptionPlan.is_deleted == False).first()
        if not subscription_plan:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid subscription plan ID.")

        # Check if can_multi_entity is false but multiple entities are provided
        if not subscription_plan.can_multi_entity and len(customer_in.initial_entities) > 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Subscription plan '{subscription_plan.name}' does not support multiple entities. Please provide only one initial entity or upgrade your plan."
            )

        # NEW LOGIC: Calculate end date based on plan duration
        start_date = datetime.now()
        end_date = start_date + timedelta(days=30 * subscription_plan.duration_months)

        # Create customer record with new fields
        db_customer = models.Customer(
            name=customer_in.name,
            address=customer_in.address,
            contact_email=customer_in.contact_email,
            contact_phone=customer_in.contact_phone,
            subscription_plan_id=customer_in.subscription_plan_id,
            start_date=start_date, # NEW
            end_date=end_date,      # NEW
            status=SubscriptionStatus.ACTIVE # NEW
        )
        db.add(db_customer)
        db.flush()

        # Create initial entities
        if not customer_in.initial_entities:
            customer_prefix = customer_in.name.replace(" ", "").upper()[:3].ljust(3, 'X')
            default_entity_code = f"{customer_prefix}1"
            main_entity_in = CustomerEntityCreate(
                entity_name="Main Entity",
                code=default_entity_code,
                contact_person=customer_in.name,
                contact_email=customer_in.contact_email,
            )
            self.crud_customer_entity_instance.create(db, main_entity_in, db_customer.id, user_id_caller)
        else:
            for entity_in in customer_in.initial_entities:
                self.crud_customer_entity_instance.create(db, entity_in, db_customer.id, user_id_caller)

        # Create initial Corporate Admin user
        initial_admin_user_data = UserCreateCorporateAdmin(
            email=customer_in.initial_corporate_admin.email,
            password=customer_in.initial_corporate_admin.password,
            role=UserRole.CORPORATE_ADMIN,
            has_all_entity_access=customer_in.initial_corporate_admin.has_all_entity_access,
            entity_ids=customer_in.initial_corporate_admin.entity_ids,
            must_change_password=True,
        )
        if db_customer.active_user_count >= subscription_plan.max_users:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"User limit ({subscription_plan.max_users}) exceeded for this plan. Cannot create initial Corporate Admin."
            )
        self.crud_user_instance.create_user_by_corporate_admin(db, initial_admin_user_data, db_customer.id, user_id_caller)

        # Log onboarding action
        log_action(
            db,
            user_id=user_id_caller,
            action_type=AUDIT_ACTION_TYPE_CUSTOMER_ONBOARD,
            entity_type="Customer",
            entity_id=db_customer.id,
            details={
                "customer_name": db_customer.name,
                "subscription_plan": subscription_plan.name,
                "initial_admin_email": customer_in.initial_corporate_admin.email,
                "initial_entities_count": len(customer_in.initial_entities) if customer_in.initial_entities else 1,
            }
        )

        db.refresh(db_customer)
        return db_customer

    def create(self, db: Session, obj_in: CustomerCreate, **kwargs: Any) -> models.Customer:
        # Overriding to enforce customer_id presence if needed, but the onboard_customer handles creation for now.
        # This generic create might not be used directly for customers if onboarding is always the entry point.
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(db, user_id=kwargs.get('user_id'), action_type=AUDIT_ACTION_TYPE_CREATE, entity_type="Customer", entity_id=db_obj.id, details={"name": db_obj.name})
        return db_obj

    def update(self, db: Session, db_obj: models.Customer, obj_in: CustomerUpdate, user_id: Optional[int] = None) -> models.Customer:
        # Check for changes in subscription plan
        old_plan_id = db_obj.subscription_plan_id
        new_plan_id = obj_in.subscription_plan_id if obj_in.subscription_plan_id is not None else old_plan_id

        if old_plan_id != new_plan_id:
            old_plan = db_obj.subscription_plan
            new_plan = db.query(models.SubscriptionPlan).filter(models.SubscriptionPlan.id == new_plan_id, models.SubscriptionPlan.is_deleted == False).first()
            
            if not new_plan:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="New subscription plan not found or is deleted.")

            # Downgrade scenario: from multi-entity to single-entity plan
            if old_plan.can_multi_entity and not new_plan.can_multi_entity:
                active_entities = db.query(models.CustomerEntity).filter(
                    models.CustomerEntity.customer_id == db_obj.id,
                    models.CustomerEntity.is_deleted == False,
                    models.CustomerEntity.is_active == True
                ).all()

                if len(active_entities) > 1:
                    # Soft-delete all but one active entity
                    for i, entity in enumerate(active_entities):
                        if i == 0: # Keep the first one active
                            continue
                        logger.warning(f"Customer {db_obj.name} downgraded to single-entity plan. Deactivating excess entity: {entity.entity_name} (ID: {entity.id})")
                        self.crud_customer_entity_instance.soft_delete(db, entity, user_id)
                        # We must flush here to ensure log_action is committed before next entity and to ensure DB state
                        # is updated for other operations in this loop. No db.commit() as it's part of a larger transaction.
                        db.flush() # Ensure soft-delete action is persisted to session

            # NEW LOGIC: When a plan is changed, reset the subscription start date, end date, and status.
            db_obj.start_date = datetime.now()
            db_obj.end_date = db_obj.start_date + timedelta(days=30 * new_plan.duration_months)
            db_obj.status = SubscriptionStatus.ACTIVE
            db.add(db_obj)

        updated_customer = super().update(db, db_obj, obj_in)
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_UPDATE, entity_type="Customer", entity_id=db_obj.id, details={"name": db_obj.name, "changes": updated_customer._changed_fields_for_log}, customer_id=db_obj.id)
        return updated_customer
    
    def soft_delete(self, db: Session, db_obj: models.Customer, user_id: Optional[int] = None) -> models.Customer:
        db_obj.soft_delete()
        db.add(db_obj)
        # Soft delete related entities and users
        for entity in db_obj.entities:
            if not entity.is_deleted:
                self.crud_customer_entity_instance.soft_delete(db, entity, user_id)
        for user in db_obj.users:
            if not user.is_deleted:
                self.crud_user_instance.soft_delete(db, user, user_id)
        
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_SOFT_DELETE, entity_type="Customer", entity_id=db_obj.id, details={"name": db_obj.name}, customer_id=db_obj.id)
        db.flush() # Ensure related soft deletes are flushed
        return db_obj

    def restore(self, db: Session, db_obj: models.Customer, user_id: Optional[int] = None) -> models.Customer:
        db_obj.restore()
        db.add(db_obj)

        # Restore related entities, checking subscription plan limits
        active_entities_count = 0
        if db_obj.subscription_plan and not db_obj.subscription_plan.can_multi_entity:
            # For single-entity plans, we can only reactivate one entity
            # We will prioritize reactivating the one that was 'Main Entity' or the first one found
            restored_one_entity = False
            for entity in sorted(db_obj.entities, key=lambda e: e.created_at): # Restore by creation order
                if entity.is_deleted:
                    if not restored_one_entity:
                        logger.debug(f"DEBUG: Restoring single entity {entity.entity_name} for customer {db_obj.name}")
                        self.crud_customer_entity_instance.restore(db, entity, user_id)
                        active_entities_count += 1
                        restored_one_entity = True
                        db.flush() # Flush to ensure it's counted
                    else:
                        logger.debug(f"DEBUG: Not reactivating entity {entity.entity_name} as customer {db_obj.name} is on a single-entity plan.")
                        # Log that other entities were skipped from restoration due to plan limitations
                        log_action(db, user_id=user_id, action_type="ENTITY_RESTORE_SKIPPED", entity_type="CustomerEntity", entity_id=entity.id, details={"reason": "Customer on single-entity plan", "customer_name": db_obj.name, "entity_name": entity.entity_name}, customer_id=db_obj.id)
            if not restored_one_entity and db_obj.entities:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot restore customer. No eligible entity found for single-entity plan.")
        else: # Multi-entity plan, restore all
            for entity in db_obj.entities:
                if entity.is_deleted:
                    self.crud_customer_entity_instance.restore(db, entity, user_id)
                    active_entities_count += 1
            db.flush() # Flush after entity restorations

        # Restore related users, checking subscription plan limits
        active_users_after_restore = db.query(models.User).filter(
            models.User.customer_id == db_obj.id,
            models.User.is_deleted == False
        ).count()

        for user in db_obj.users:
            if user.is_deleted:
                if active_users_after_restore < db_obj.subscription_plan.max_users:
                    self.crud_user_instance.restore(db, user, user_id)
                    active_users_after_restore += 1
                else:
                    logger.warning(f"Skipping user restore for {user.email} (ID: {user.id}) due to subscription plan user limit for customer {db_obj.name}.")
                    log_action(db, user_id=user_id, action_type="USER_RESTORE_SKIPPED", entity_type="User", entity_id=user.id, details={"reason": "Subscription plan user limit exceeded", "customer_name": db_obj.name, "user_email": user.email}, customer_id=db_obj.id)
        
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_RESTORE, entity_type="Customer", entity_id=db_obj.id, details={"name": db_obj.name}, customer_id=db_obj.id)
        db.flush() # Final flush for customer and user restorations
        return db_obj
        
    def renew_subscription(self, db: Session, customer_id: int, user_id_caller: int) -> models.Customer:
        """
        Renews the customer's subscription based on their CURRENT plan.
        Logic:
        - If Active: Extends the end_date by the plan duration.
        - If Expired: Restarts the subscription (Start=Now, End=Now+Duration).
        """
        customer = self.get_with_relations(db, customer_id)
        if not customer:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found.")
        
        plan = customer.subscription_plan
        if not plan:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Customer has no valid subscription plan.")

        now = datetime.now()
        
        # Ensure we compare timezone-naive datetimes if necessary (depending on your DB setup)
        current_end_date = customer.end_date.replace(tzinfo=None) if customer.end_date else now

        # LOGIC: New End Date = Max(Today, Current End) + Duration
        reference_date = max(now, current_end_date)
        duration_days = 30 * plan.duration_months # Approximate month as 30 days
        new_end_date = reference_date + timedelta(days=duration_days)

        # State Change Logic
        previous_end_date = customer.end_date
        customer.end_date = new_end_date
        customer.status = SubscriptionStatus.ACTIVE # Always reactivate on renewal

        # If subscription was expired (Reference was Today), reset the Start Date to represent a "New Term"
        if now > current_end_date:
            customer.start_date = now

        db.add(customer)
        db.flush()
        db.refresh(customer)

        # Log the action
        log_action(
            db,
            user_id=user_id_caller,
            action_type="SUBSCRIPTION_RENEWED",
            entity_type="Customer",
            entity_id=customer.id,
            details={
                "previous_end_date": str(previous_end_date),
                "new_end_date": str(new_end_date),
                "plan_name": plan.name,
                "is_restart": (now > current_end_date) # True if it was expired
            },
            customer_id=customer.id
        )

        return customer


class CRUDCustomerEntity(CRUDBase):
    def __init__(self, model: Type[CustomerEntity]):
        super().__init__(model)

    def get_by_name_for_customer(self, db: Session, customer_id: int, entity_name: str) -> Optional[models.CustomerEntity]:
        return db.query(self.model).filter(
            self.model.customer_id == customer_id,
            func.lower(self.model.entity_name) == func.lower(entity_name),
            self.model.is_deleted == False
        ).first()

    def get_by_code_for_customer(self, db: Session, customer_id: int, code: str) -> Optional[models.CustomerEntity]:
        return db.query(self.model).filter(
            self.model.customer_id == customer_id,
            func.upper(self.model.code) == func.upper(code), # Use upper for case-insensitive comparison for code
            self.model.is_deleted == False
        ).first()

    def get_all_for_customer(self, db: Session, customer_id: int, skip: int = 0, limit: int = 100) -> List[models.CustomerEntity]:
        return db.query(self.model).filter(self.model.customer_id == customer_id, self.model.is_deleted == False).offset(skip).limit(limit).all()
    
    def create(self, db: Session, obj_in: CustomerEntityCreate, customer_id: int, user_id: Optional[int] = None) -> models.CustomerEntity:
        # Check customer's subscription plan for multi-entity support
        customer = db.query(models.Customer).options(selectinload(models.Customer.subscription_plan)).filter(models.Customer.id == customer_id).first()
        if not customer:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found.")
        
        if not customer.subscription_plan.can_multi_entity:
            active_entities_count = db.query(self.model).filter(
                self.model.customer_id == customer_id,
                self.model.is_deleted == False,
                self.model.is_active == True
            ).count()
            if active_entities_count >= 1: # Already has an active entity
                 raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Subscription plan '{customer.subscription_plan.name}' does not support multiple active entities. Cannot create another entity."
                )

        # Validate uniqueness of entity name and code within the customer scope
        if self.get_by_name_for_customer(db, customer_id, obj_in.entity_name):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Entity with this name already exists for this customer.")
        
        # Check for existing code (even soft-deleted)
        existing_code_entity = db.query(self.model).filter(
            self.model.customer_id == customer_id,
            func.upper(self.model.code) == func.upper(obj_in.code) # Ensure case-insensitive check
        ).first()
        if existing_code_entity and not existing_code_entity.is_deleted:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Entity with code '{obj_in.code}' already exists for this customer.")
        elif existing_code_entity and existing_code_entity.is_deleted:
            # If a soft-deleted entity with the same code exists, it means the code is taken.
            # You might want to prevent creation and ask to restore, or auto-restore and update.
            # For now, we treat it as a conflict preventing new creation with that code.
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Entity with code '{obj_in.code}' exists but is deleted. Please restore it or use a different code.")
            
        # If code is not provided in obj_in, it will be generated after getting ID
        db_obj = self.model(
            customer_id=customer_id,
            entity_name=obj_in.entity_name,
            code=obj_in.code.upper() if obj_in.code else None, # Store code in uppercase if provided
            contact_person=obj_in.contact_person,
            contact_email=obj_in.contact_email,
            is_active=obj_in.is_active,
            address=obj_in.address,
            commercial_register_number=obj_in.commercial_register_number,
            tax_id=obj_in.tax_id
        )
        db.add(db_obj)
        db.flush() # Flush to assign ID to db_obj

        # Auto-generate code if not provided
        if not db_obj.code:
            db_obj.code = str(db_obj.id).zfill(4) # Pad with leading zeros to 4 digits
            # Re-check uniqueness after auto-generation, though ID-based generation should be unique
            # In a highly concurrent system, this might still face rare conflicts if IDs are not perfectly sequential.
            # For this scenario, assuming ID-based uniqueness is sufficient.
            db.add(db_obj) # Add again to mark as modified
            db.flush() # Flush again to persist generated code

        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_CREATE, entity_type="CustomerEntity", entity_id=db_obj.id, details={"name": db_obj.entity_name, "code": db_obj.code}, customer_id=customer_id)
        db.refresh(db_obj)
        return db_obj

    def update(self, db: Session, db_obj: models.CustomerEntity, obj_in: CustomerEntityUpdate, user_id: Optional[int] = None) -> models.CustomerEntity:
        # Check for uniqueness if entity_name is changed
        if obj_in.entity_name is not None and obj_in.entity_name.lower() != db_obj.entity_name.lower():
            if self.get_by_name_for_customer(db, db_obj.customer_id, obj_in.entity_name):
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Entity with this name already exists for this customer.")
        
        # Check for uniqueness if code is changed
        if obj_in.code is not None and obj_in.code.upper() != db_obj.code.upper():
            # Check if new code conflicts with existing active entity
            existing_code_entity = self.get_by_code_for_customer(db, db_obj.customer_id, obj_in.code)
            if existing_code_entity and existing_code_entity.id != db_obj.id:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Entity with code '{obj_in.code}' already exists for this customer.")
            
            # Check for soft-deleted entity with the new code
            soft_deleted_code_entity = db.query(self.model).filter(
                self.model.customer_id == db_obj.customer_id,
                func.upper(self.model.code) == func.upper(obj_in.code),
                self.model.is_deleted == True
            ).first()
            if soft_deleted_code_entity and soft_deleted_code_entity.id != db_obj.id:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Entity with code '{obj_in.code}' exists but is deleted. Cannot use this code.")

        update_data = obj_in.model_dump(exclude_unset=True)
        # Ensure code is stored in uppercase
        if 'code' in update_data and update_data['code'] is not None:
            update_data['code'] = update_data['code'].upper()

        updated_entity = super().update(db, db_obj, update_data)
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_UPDATE, entity_type="CustomerEntity", entity_id=db_obj.id, details={"name": db_obj.entity_name, "changes": updated_entity._changed_fields_for_log}, customer_id=db_obj.customer_id)
        return updated_entity

    def soft_delete(self, db: Session, db_obj: models.CustomerEntity, user_id: Optional[int] = None) -> models.CustomerEntity:
        # Check if it's the last active entity on a single-entity plan
        customer = db.query(models.Customer).options(selectinload(models.Customer.subscription_plan)).filter(models.Customer.id == db_obj.customer_id).first()
        if customer and not customer.subscription_plan.can_multi_entity:
            active_entities_count = db.query(self.model).filter(
                self.model.customer_id == db_obj.customer_id,
                self.model.is_deleted == False,
                self.model.is_active == True,
                self.model.id != db_obj.id # Exclude the current entity being soft-deleted
            ).count()
            if active_entities_count == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot delete the last active entity for a customer on a single-entity plan ({customer.subscription_plan.name})."
                )

        db_obj.soft_delete()
        db.add(db_obj)
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_SOFT_DELETE, entity_type="CustomerEntity", entity_id=db_obj.id, details={"name": db_obj.entity_name, "code": db_obj.code}, customer_id=db_obj.customer_id)
        db.flush() # Flush changes to ensure they are available for related operations
        return db_obj

    def restore(self, db: Session, db_obj: models.CustomerEntity, user_id: Optional[int] = None) -> models.CustomerEntity:
        customer = db.query(models.Customer).options(selectinload(models.Customer.subscription_plan)).filter(models.Customer.id == db_obj.customer_id).first()
        if not customer:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found for entity restoration.")
            
        # Check subscription plan limit
        if not customer.subscription_plan.can_multi_entity:
            active_entities_count = db.query(self.model).filter(
                self.model.customer_id == db_obj.customer_id,
                self.model.is_deleted == False,
                self.model.is_active == True
            ).count()
            if active_entities_count >= 1: # If there's already one active entity and plan is single-entity
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Subscription plan '{customer.subscription_plan.name}' only supports one active entity. Cannot restore '{db_obj.entity_name}'."
                )

        db_obj.restore()
        db.add(db_obj)
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_RESTORE, entity_type="CustomerEntity", entity_id=db_obj.id, details={"name": db_obj.entity_name, "code": db_obj.code}, customer_id=db_obj.customer_id)
        db.flush() # Flush changes
        return db_obj


class CRUDCustomerEmailSetting(CRUDBase):
    def __init__(self, model: Type[CustomerEmailSetting]):
        super().__init__(model)

    def get_by_customer_id(self, db: Session, customer_id: int) -> Optional[models.CustomerEmailSetting]:
        return db.query(self.model).filter(self.model.customer_id == customer_id, self.model.is_deleted == False).first()

    def create(self, db: Session, obj_in: CustomerEmailSettingCreate, customer_id: int, user_id: Optional[int] = None) -> models.CustomerEmailSetting:
        existing_settings = self.get_by_customer_id(db, customer_id)
        if existing_settings:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email settings for this customer already exist. Please update instead.")

        db_obj = self.model(
            customer_id=customer_id,
            smtp_host=obj_in.smtp_host,
            smtp_port=obj_in.smtp_port,
            smtp_username=obj_in.smtp_username,
            smtp_password_encrypted=encrypt_data(obj_in.smtp_password),
            sender_email=obj_in.sender_email,
            sender_display_name=obj_in.sender_display_name,
            is_active=obj_in.is_active
        )
        db.add(db_obj)
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_CREATE, entity_type="CustomerEmailSetting", entity_id=db_obj.id, details={"customer_id": customer_id, "sender_email": obj_in.sender_email}, customer_id=customer_id)
        db.flush()
        return db_obj

    def update(self, db: Session, db_obj: models.CustomerEmailSetting, obj_in: CustomerEmailSettingUpdate, user_id: Optional[int] = None) -> models.CustomerEmailSetting:
        update_data = obj_in.model_dump(exclude_unset=True)
        
        # Handle password encryption if provided
        if "smtp_password" in update_data and update_data["smtp_password"] is not None:
            update_data["smtp_password_encrypted"] = encrypt_data(update_data["smtp_password"])
            del update_data["smtp_password"] # Remove plaintext password from update_data

        # Manually update fields to track changes for logging
        changes = {}
        for field, value in update_data.items():
            current_value = getattr(db_obj, field, None)
            if current_value != value:
                changes[field] = {"old": current_value, "new": value}
                setattr(db_obj, field, value)
        
        db.add(db_obj)
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_UPDATE, entity_type="CustomerEmailSetting", entity_id=db_obj.id, details={"customer_id": db_obj.customer_id, "sender_email": db_obj.sender_email, "changes": changes}, customer_id=db_obj.customer_id)
        db.flush()
        db.refresh(db_obj)
        return db_obj

    def soft_delete(self, db: Session, db_obj: models.CustomerEmailSetting, user_id: Optional[int] = None) -> models.CustomerEmailSetting:
        db_obj.soft_delete()
        db.add(db_obj)
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_SOFT_DELETE, entity_type="CustomerEmailSetting", entity_id=db_obj.id, details={"customer_id": db_obj.customer_id, "sender_email": db_obj.sender_email}, customer_id=db_obj.customer_id)
        db.flush()
        return db_obj

    def restore(self, db: Session, db_obj: models.CustomerEmailSetting, user_id: Optional[int] = None) -> models.CustomerEmailSetting:
        db_obj.restore()
        db.add(db_obj)
        log_action(db, user_id=user_id, action_type=AUDIT_ACTION_TYPE_RESTORE, entity_type="CustomerEmailSetting", entity_id=db_obj.id, details={"customer_id": db_obj.customer_id, "sender_email": db_obj.sender_email}, customer_id=db_obj.customer_id)
        db.flush()
        return db_obj

