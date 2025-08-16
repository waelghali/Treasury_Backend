# app/api/v1/endpoints/corporate_admin.py

import os
import sys
import importlib.util
from datetime import datetime, timedelta
from typing import List, Optional, Any, Dict

from fastapi import APIRouter, Depends, HTTPException, status, Request, Query, Body

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import func

from app.database import get_db
from app.schemas.all_schemas import (
    CustomerEntityCreate, CustomerEntityUpdate, CustomerEntityOut,
    UserCreateCorporateAdmin, UserUpdateCorporateAdmin, UserOut,
    LGCategoryCreate, LGCategoryUpdate, LGCategoryOut,
    CustomerConfigurationCreate, CustomerConfigurationUpdate, CustomerConfigurationOut,
    CustomerEmailSettingCreate, CustomerEmailSettingUpdate, CustomerEmailSettingOut,
    Token,
    ApprovalRequestOut,
    ApprovalRequestUpdate,
    InternalOwnerContactOut, AuditLogOut, LGRecordOut, LGInstructionOut,
    SystemNotificationOut,
)
from app.crud.crud import (
    crud_customer, crud_customer_entity, crud_user,
    crud_lg_category, crud_universal_category,
    crud_global_configuration, crud_customer_configuration,
    crud_audit_log, log_action,
    crud_permission, crud_role_permission,
    crud_approval_request,
    crud_customer_email_setting,
    crud_lg_record,
    crud_internal_owner_contact, crud_lg_instruction,
    crud_system_notification,
    crud_system_notification_view_log,
)
from app.models import (
    User, Customer, CustomerEntity,
    LGCategory, UniversalCategory, GlobalConfiguration,
    CustomerEmailSetting, UserCustomerEntityAssociation,
    ApprovalRequest, InternalOwnerContact,
)
from app.constants import UserRole, GlobalConfigKey, ApprovalRequestStatusEnum, SubscriptionStatus

# --- Explicitly load core.security using importlib ---
try:
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_file_dir, '..', '..', '..'))
    
    security_module_path = os.path.join(project_root, 'core', 'security.py')

    if not os.path.exists(security_module_path):
        raise FileNotFoundError(f"Expected core/security.py at {security_module_path} but it was not found.")
    
    spec = importlib.util.spec_from_file_location("core.security", security_module_path)
    core_security = importlib.util.module_from_spec(spec)
    sys.modules["core.security"] = core_security
    spec.loader.exec_module(core_security)
    from core.security import (
        TokenData,
        HasPermission,
        get_current_active_user,
        get_current_corporate_admin_context,
        get_current_user
    )
except Exception as e:
    print(f"FATAL ERROR (corporate_admin.py): Could not import core.security module directly. Error: {e}")
    raise

import logging
logger = logging.getLogger(__name__)
router = APIRouter()

# NEW DEPENDENCY: Check subscription status
def check_subscription_status(
    current_user: TokenData = Depends(get_current_corporate_admin_context)
):
    if current_user.subscription_status == SubscriptionStatus.EXPIRED:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Subscription is expired. Access is denied. Please contact the system owner to renew."
        )
    # Allows 'active' and 'grace' statuses to pass
    return current_user

# WRAPPER DEPENDENCY for write operations during grace period
def check_for_read_only_mode(
    current_user: TokenData = Depends(check_subscription_status)
):
    if current_user.subscription_status == SubscriptionStatus.GRACE:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Subscription is in grace period. Write operations are disabled. Access is read-only."
        )
    return current_user

# --- Corporate Admin API Status ---
@router.get("/status", dependencies=[Depends(check_subscription_status)])
async def get_corporate_admin_status(
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Checks the status of the Corporate Admin API and returns basic user info.
    """
    return {"message": f"Corporate Admin API is up and running for {corporate_admin_context.email} (Customer ID: {corporate_admin_context.customer_id})!"}


# --- Dashboard Metrics ---
@router.get("/dashboard-metrics", response_model=Dict[str, Any], dependencies=[Depends(check_subscription_status)])
def get_dashboard_metrics(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("corporate_admin:view_dashboard"))
):
    """
    Retrieves key metrics for the Corporate Admin dashboard, providing an overview of their organization's status.
    """
    customer_id = corporate_admin_context.customer_id

    # Total Active Users for this customer
    total_active_users = db.query(User).filter(
        User.customer_id == customer_id,
        User.is_deleted == False
    ).count()

    # Total LG Categories (customer-specific + universal)
    total_customer_categories = db.query(LGCategory).filter(
        LGCategory.customer_id == customer_id,
        LGCategory.is_deleted == False
    ).count()
    total_universal_categories = db.query(UniversalCategory).filter(
        UniversalCategory.is_deleted == False
    ).count()
    total_lg_categories = total_customer_categories + total_universal_categories

    # Pending Approvals Count
    pending_approvals_count = db.query(ApprovalRequest).filter(
        ApprovalRequest.customer_id == customer_id,
        ApprovalRequest.status == ApprovalRequestStatusEnum.PENDING
    ).count()

    customer_obj = crud_customer.get(db, customer_id)
    customer_name = customer_obj.name if customer_obj else "Unknown Customer"

    return {
        "total_active_users": total_active_users,
        "total_lg_categories": total_lg_categories,
        "pending_approvals_count": pending_approvals_count,
        "customer_name": customer_name
    }

# --- Password Change Endpoint (Forced Login) ---
# This endpoint should NOT be protected by subscription status checks
@router.post("/users/change-password-on-first-login", response_model=Token)
def change_password_on_first_login(
    new_password: str = Body(..., embed=True, min_length=8),
    db: Session = Depends(get_db),
    current_user_token_data: TokenData = Depends(get_current_user),
    request: Request = None
):
    """
    Allows a user whose must_change_password flag is True to set a new password.
    After successful change, issues a new JWT token with must_change_password set to False.
    This endpoint explicitly bypasses the get_current_active_user check.
    """
    client_host = request.client.host if request else None

    db_user = db.query(User).filter(User.id == current_user_token_data.user_id, User.is_deleted == False).first()
    if not db_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")

    if not db_user.must_change_password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password change not required for this user. You can change your password from your profile settings."
        )

    try:
        updated_user = crud_user.change_password_on_first_login(db, db_user, new_password, current_user_token_data.user_id)

        db_permissions = crud_role_permission.get_permissions_for_role(db, updated_user.role.value)
        permission_names = [p.name for p in db_permissions]

        new_token_data = {
            "sub": updated_user.email,
            "user_id": updated_user.id,
            "role": updated_user.role.value,
            "permissions": permission_names,
            "customer_id": updated_user.customer_id,
            "has_all_entity_access": updated_user.has_all_entity_access,
            "entity_ids": [assoc.customer_entity.id for assoc in updated_user.entity_associations] if not updated_user.has_all_entity_access else [],
            "must_change_password": False,
        }
        access_token_expires = timedelta(minutes=core_security.ACCESS_TOKEN_EXPIRE_MINUTES)
        new_access_token = core_security.create_access_token(
            data=new_token_data, expires_delta=access_token_expires
        )

        log_action(db, user_id=updated_user.id, action_type="PASSWORD_CHANGE_FIRST_LOGIN_SUCCESS", entity_type="User", entity_id=updated_user.id, details={"email": updated_user.email}, customer_id=updated_user.customer_id)
        
        return Token(access_token=new_access_token, token_type="bearer")

    except HTTPException as e:
        log_action(db, user_id=current_user_token_data.user_id, action_type="PASSWORD_CHANGE_FIRST_LOGIN_FAILED", entity_type="User", entity_id=current_user_token_data.user_id, details={"email": db_user.email, "reason": str(e.detail)}, customer_id=current_user_token_data.customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=current_user_token_data.user_id, action_type="PASSWORD_CHANGE_FIRST_LOGIN_FAILED", entity_type="User", entity_id=current_user_token_data.user_id, details={"email": db_user.email, "customer_id": current_user_token_data.customer_id, "reason": str(e)}, customer_id=current_user_token_data.customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


# --- Customer Entity Management (within Corporate Admin's customer scope) ---
# All write operations must use check_for_read_only_mode
@router.post("/customer-entities/", response_model=CustomerEntityOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(check_for_read_only_mode)])
def create_customer_entity(
    entity_in: CustomerEntityCreate, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("customer_entity:create")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        # FIX: Call get_with_relations with positional argument for customer_id
        customer_check = crud_customer.get_with_relations(db, customer_id)
        if not customer_check:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found or is deleted.")
        
        active_entities_count = db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == customer_id,
            CustomerEntity.is_deleted == False,
            CustomerEntity.is_active == True
        ).count()

        if not customer_check.subscription_plan.can_multi_entity and active_entities_count >= 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Customer's subscription plan '{customer_check.subscription_plan.name}' does not support adding more entities. Max 1 active entity allowed."
            )

        existing_entity_name = crud_customer_entity.get_by_name_for_customer(db, customer_id, entity_in.entity_name)
        if existing_entity_name:
            if existing_entity_name.is_deleted:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Entity with name '{entity_in.entity_name}' already exists for this customer but is deleted. Please restore it if needed."
                        )
            else:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Entity with name '{entity_in.entity_name}' already exists for this customer."
                )
        
        if entity_in.code:
            existing_entity_code = crud_customer_entity.get_by_code_for_customer(db, customer_id, entity_in.code)
            if existing_entity_code and existing_entity_code.is_deleted == False:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Entity with code '{entity_in.code}' already exists for customer '{customer_check.name}'."
                )
                
        db_entity = crud_customer_entity.create(db, obj_in=entity_in, customer_id=customer_id, user_id=corporate_admin_context.user_id)
        
        return db_entity
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="CREATE_FAILED", entity_type="CustomerEntity", entity_id=None, details={"entity_name": entity_in.entity_name, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="CREATE_FAILED", entity_type="CustomerEntity", entity_id=None, details={"entity_name": entity_in.entity_name, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


@router.get("/customer-entities/", response_model=List[CustomerEntityOut], dependencies=[Depends(check_subscription_status)])
def list_customer_entities(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("customer_entity:view"))
):
    customer_id = corporate_admin_context.customer_id
    entities = crud_customer_entity.get_all_for_customer(db, customer_id)
    return entities

@router.get("/customer-entities/{entity_id}", response_model=CustomerEntityOut, dependencies=[Depends(check_subscription_status)])
def read_customer_entity(
    entity_id: int, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("customer_entity:view"))
):
    db_entity = crud_customer_entity.get(db, entity_id)
    if not db_entity or db_entity.customer_id != corporate_admin_context.customer_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer entity not found or does not belong to your customer.")
    return db_entity

@router.put("/customer-entities/{entity_id}", response_model=CustomerEntityOut, dependencies=[Depends(check_for_read_only_mode)])
def update_customer_entity(
    entity_id: int, 
    entity_in: CustomerEntityUpdate, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("customer_entity:edit")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_entity = crud_customer_entity.get(db, entity_id)
        if not db_entity or db_entity.customer_id != customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer entity not found or does not belong to your customer.")
        
        if entity_in.entity_name is not None and entity_in.entity_name.lower() != db_entity.entity_name.lower():
            existing_entity = crud_customer_entity.get_by_name_for_customer(db, customer_id, entity_in.entity_name)
            if existing_entity and existing_entity.id != entity_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Entity with name '{entity_in.entity_name}' already exists for this customer."
                )
        
        if entity_in.code is not None and entity_in.code.lower() != db_entity.code.lower():
            existing_entity_code = crud_customer_entity.get_by_code_for_customer(db, customer_id, entity_in.code)
            if existing_entity_code and existing_entity_code.id != entity_id and existing_entity_code.is_deleted == False:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Entity with code '{entity_in.code}' already exists for this customer."
                )
                
        updated_entity = crud_customer_entity.update(db, db_obj=db_entity, obj_in=entity_in, user_id=corporate_admin_context.user_id)
        
        return updated_entity
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="CustomerEntity", entity_id=entity_id, details={"entity_name": entity_in.entity_name, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="CustomerEntity", entity_id=entity_id, details={"entity_name": entity_in.entity_name, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.delete("/customer-entities/{entity_id}", response_model=CustomerEntityOut, dependencies=[Depends(check_for_read_only_mode)])
def delete_customer_entity(
    entity_id: int, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("customer_entity:delete")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_entity = crud_customer_entity.get(db, entity_id)
        if not db_entity or db_entity.customer_id != customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer entity not found or does not belong to your customer.")
        
        # FIX: Call get_with_relations with positional argument for customer_id
        customer_of_entity = crud_customer.get_with_relations(db, customer_id)
        if customer_of_entity and not customer_of_entity.subscription_plan.can_multi_entity:
            active_entities_count = db.query(CustomerEntity).filter(
                CustomerEntity.customer_id == customer_id,
                CustomerEntity.is_deleted == False,
                CustomerEntity.is_active == True,
                CustomerEntity.id != entity_id
            ).count()
            if active_entities_count == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cannot delete or deactivate the last active entity for a customer on a single-entity plan."
                )

        deleted_entity = crud_customer_entity.soft_delete(db, db_entity, user_id=corporate_admin_context.user_id)
        
        return deleted_entity
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="SOFT_DELETE_FAILED", entity_type="CustomerEntity", entity_id=entity_id, details={"entity_name": db_entity.entity_name, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="SOFT_DELETE_FAILED", entity_type="CustomerEntity", entity_id=entity_id, details={"entity_name": db_entity.entity_name, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.post("/customer-entities/{entity_id}/restore", response_model=CustomerEntityOut, dependencies=[Depends(check_for_read_only_mode)])
def restore_customer_entity(
    entity_id: int, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("customer_entity:edit")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_entity = db.query(CustomerEntity).filter(
            CustomerEntity.id == entity_id, 
            CustomerEntity.customer_id == customer_id,
            CustomerEntity.is_deleted == True 
        ).first()

        if not db_entity:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="Customer entity not found or not in a soft-deleted state, or does not belong to your customer."
            )
        
        # FIX: Call get_with_relations with positional argument for customer_id
        customer_of_entity = crud_customer.get_with_relations(db, customer_id)
        if customer_of_entity and not customer_of_entity.subscription_plan.can_multi_entity:
            active_entities_count = db.query(CustomerEntity).filter(
                CustomerEntity.customer_id == customer_id,
                CustomerEntity.is_deleted == False,
                CustomerEntity.is_active == True,
                CustomerEntity.id != entity_id
            ).count()
            if active_entities_count >= 1:
                print(f"DEBUG: Not reactivating entity {db_entity.entity_name} during restore due to single-entity plan limit.")
                restored_entity = crud_customer_entity.restore(db, db_entity, user_id=corporate_admin_context.user_id)
                restored_entity.is_active = False
                db.add(restored_entity)
                
                log_action(db, user_id=corporate_admin_context.user_id, action_type="RESTORE_INACTIVE", entity_type="CustomerEntity", entity_id=restored_entity.id, details={"entity_name": restored_entity.entity_name, "reason": "Single-entity plan limit met"}, customer_id=customer_id)
                return restored_entity
                
        restored_entity = crud_customer_entity.restore(db, db_entity, user_id=corporate_admin_context.user_id)
        
        return restored_entity
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="RESTORE_FAILED", entity_type="CustomerEntity", entity_id=entity_id, details={"entity_name": db_entity.entity_name, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="RESTORE_FAILED", entity_type="CustomerEntity", entity_id=entity_id, details={"entity_name": db_entity.entity_name, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

# --- User Management (within Corporate Admin's customer scope) ---
# NEW ENDPOINT: Create User (was missing)
@router.post("/users/", response_model=UserOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(check_for_read_only_mode)])
def create_user(
    user_in: UserCreateCorporateAdmin,
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("user:create")),
    request: Request = None
):
    """
    Allows a Corporate Admin to create a new user under their customer organization.
    Enforces subscription plan limits and prevents assignment of the SYSTEM_OWNER role.
    """
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        # FIX: Call get_with_relations with positional argument for customer_id
        customer_check = crud_customer.get_with_relations(db, customer_id)
        if not customer_check:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found or is deleted.")
        
        # Check user limit for subscription plan
        active_users_count = db.query(User).filter(
            User.customer_id == customer_id,
            User.is_deleted == False
        ).count()
        if active_users_count >= customer_check.subscription_plan.max_users:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Customer's subscription plan '{customer_check.subscription_plan.name}' limit of {customer_check.subscription_plan.max_users} users reached."
            )

        # Ensure role is not SYSTEM_OWNER (already in schema validator, but double check)
        if user_in.role == UserRole.SYSTEM_OWNER:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Corporate Admins cannot create users with 'SYSTEM_OWNER' role.")

        db_user = crud_user.create_user_by_corporate_admin(db, user_in, customer_id, corporate_admin_context.user_id)
        return db_user
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="CREATE_FAILED", entity_type="User", entity_id=None, details={"email": user_in.email, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="CREATE_FAILED", entity_type="User", entity_id=None, details={"email": user_in.email, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.get("/users/", response_model=List[UserOut], dependencies=[Depends(check_subscription_status)])
def list_users(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("user:view"))
):
    customer_id = corporate_admin_context.customer_id
    users = crud_user.get_users_by_customer_id(db, customer_id)
    return users

@router.get("/users/{user_id}", response_model=UserOut, dependencies=[Depends(check_subscription_status)])
def read_user(
    user_id: int, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("user:view"))
):
    db_user = db.query(User).options(
        selectinload(User.entity_associations).selectinload(UserCustomerEntityAssociation.customer_entity)
    ).filter(
        User.id == user_id,
        User.customer_id == corporate_admin_context.customer_id,
        User.is_deleted == False
    ).first()

    if not db_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found or does not belong to your organization.")
    return db_user

@router.put("/users/{user_id}", response_model=UserOut, dependencies=[Depends(check_for_read_only_mode)])
def update_user(
    user_id: int, 
    user_in: UserUpdateCorporateAdmin, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("user:edit")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_user = db.query(User).filter(
            User.id == user_id, 
            User.is_deleted == False
        ).first()
        if not db_user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found or is deleted.")

        updated_user = crud_user.update_user_by_corporate_admin(db, db_user, user_in, customer_id, corporate_admin_context.user_id)
        
        return updated_user
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="User", entity_id=user_id, details={"email": user_in.email, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="User", entity_id=user_id, details={"email": user_in.email, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.delete("/users/{user_id}", response_model=UserOut, dependencies=[Depends(check_for_read_only_mode)])
def delete_user(
    user_id: int, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("user:delete")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_user = db.query(User).filter(
            User.id == user_id, 
            User.is_deleted == False
        ).first()
        if not db_user or db_user.customer_id != customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found or does not belong to your organization.")
        
        if db_user.role == UserRole.CORPORATE_ADMIN and db_user.id != corporate_admin_context.user_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete other Corporate Admins.")
        if db_user.id == corporate_admin_context.user_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete your own user account.")

        deleted_user = crud_user.soft_delete(db, db_user, user_id=corporate_admin_context.user_id)
        
        return deleted_user
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="SOFT_DELETE_FAILED", entity_type="User", entity_id=user_id, details={"email": db_user.email, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="SOFT_DELETE_FAILED", entity_type="User", entity_id=user_id, details={"email": db_user.email, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


@router.post("/users/{user_id}/restore", response_model=UserOut, dependencies=[Depends(check_for_read_only_mode)])
def restore_user(
    user_id: int, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("user:edit")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_user = db.query(User).filter(
            User.id == user_id, 
            User.customer_id == customer_id,
            User.is_deleted == True 
        ).first()

        if not db_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="User not found or not in a soft-deleted state, or does not belong to your organization."
            )

        restored_user = crud_user.restore(db, db_user, user_id=corporate_admin_context.user_id)
        
        return restored_user
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="RESTORE_FAILED", entity_type="User", entity_id=user_id, details={"email": db_user.email, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="RESTORE_FAILED", entity_type="User", entity_id=user_id, details={"email": db_user.email, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


# --- LG Category Management (within Corporate Admin's customer scope) ---
# All write operations must use check_for_read_only_mode
@router.post("/lg-categories/", response_model=LGCategoryOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(check_for_read_only_mode)])
def create_lg_category(
    lg_category_in: LGCategoryCreate, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("corporate_category:create")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_lg_category = crud_lg_category.create(db, obj_in=lg_category_in, customer_id=customer_id, user_id=corporate_admin_context.user_id)
        
        return db_lg_category
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="CREATE_FAILED", entity_type="LGCategory", entity_id=None, details={"category_name": lg_category_in.category_name, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="CREATE_FAILED", entity_type="LGCategory", entity_id=None, details={"category_name": lg_category_in.category_name, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.get("/lg-categories/", response_model=List[LGCategoryOut], dependencies=[Depends(check_subscription_status)])
def list_lg_categories(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("corporate_category:view"))
):
    customer_id = corporate_admin_context.customer_id
    all_categories = []

    universal_categories_db = crud_universal_category.get_all(db)
    for uc in universal_categories_db:
        all_categories.append(
            LGCategoryOut(
                id=uc.id,
                category_name=uc.category_name,
                code=uc.code,
                extra_field_name=uc.extra_field_name,
                is_mandatory=uc.is_mandatory if uc.is_mandatory is not None else False,
                communication_list=uc.communication_list,
                customer_id=None,
                customer_name="System Default",
                is_deleted=False,
                created_at=uc.created_at,
                updated_at=uc.updated_at,
                type="universal",
                has_all_entity_access=True,
                entities_with_access=[]
            )
        )

    customer_lg_categories = crud_lg_category.get_all_for_customer(
        db,
        customer_id=customer_id
    )

    customer = crud_customer.get(db, customer_id)
    customer_name = customer.name if customer else None

    for cat in customer_lg_categories:
        entities_out = [
            CustomerEntityOut(
                id=assoc.customer_entity.id,
                entity_name=assoc.customer_entity.entity_name,
                code=assoc.customer_entity.code,
                contact_person=assoc.customer_entity.contact_person,
                contact_email=assoc.customer_entity.contact_email,
                is_active=assoc.customer_entity.is_active,
                customer_id=assoc.customer_entity.customer_id,
                created_at=assoc.customer_entity.created_at,
                updated_at=assoc.customer_entity.updated_at,
                is_deleted=assoc.customer_entity.is_deleted,
                deleted_at=assoc.customer_entity.deleted_at
            ) for assoc in cat.entity_associations if not assoc.customer_entity.is_deleted
        ]
        all_categories.append(
            LGCategoryOut(
                id=cat.id,
                category_name=cat.category_name,
                code=cat.code,
                extra_field_name=cat.extra_field_name,
                is_mandatory=cat.is_mandatory,
                communication_list=cat.communication_list,
                customer_id=cat.customer_id,
                customer_name=customer_name,
                is_deleted=cat.is_deleted,
                created_at=cat.created_at,
                updated_at=cat.updated_at,
                type="customer",
                has_all_entity_access=cat.has_all_entity_access,
                entities_with_access=entities_out
            )
        )

    all_categories.sort(key=lambda x: (0 if x.type == 'universal' else 1, x.category_name.lower()))
    return all_categories

@router.get("/lg-categories/{category_id}", response_model=LGCategoryOut, dependencies=[Depends(check_subscription_status)])
def read_lg_category(
    category_id: int,
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("corporate_category:view"))
):
    customer_id = corporate_admin_context.customer_id
    
    db_lg_category = crud_lg_category.get_for_customer(db, category_id, customer_id)

    if not db_lg_category:
        db_lg_category = crud_universal_category.get(db, category_id)
        if not db_lg_category:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Category not found.")
        
        return LGCategoryOut(
            id=db_lg_category.id,
            category_name=db_lg_category.category_name,
            code=db_lg_category.code,
            extra_field_name=db_lg_category.extra_field_name,
            is_mandatory=db_lg_category.is_mandatory if db_lg_category.is_mandatory is not None else False,
            communication_list=db_lg_category.communication_list,
            customer_id=None,
            customer_name="System Default",
            is_deleted=False,
            created_at=db_lg_category.created_at,
            updated_at=db_lg_category.updated_at,
            type="universal",
            has_all_entity_access=True,
            entities_with_access=[]
        )

    customer = crud_customer.get(db, customer_id)
    customer_name = customer.name if customer else None
    entities_out = [
        CustomerEntityOut(
            id=assoc.customer_entity.id,
            entity_name=assoc.customer_entity.entity_name,
            code=assoc.customer_entity.code,
            contact_person=assoc.customer_entity.contact_person,
            contact_email=assoc.customer_entity.contact_email,
            is_active=assoc.customer_entity.is_active,
            customer_id=assoc.customer_entity.customer_id,
            created_at=assoc.customer_entity.created_at,
            updated_at=assoc.customer_entity.updated_at,
            is_deleted=assoc.customer_entity.is_deleted,
            deleted_at=assoc.customer_entity.deleted_at
        ) for assoc in db_lg_category.entity_associations if not assoc.customer_entity.is_deleted
    ]
    return LGCategoryOut(
        id=db_lg_category.id,
        category_name=db_lg_category.category_name,
        code=db_lg_category.code,
        extra_field_name=db_lg_category.extra_field_name,
        is_mandatory=db_lg_category.is_mandatory,
        is_deleted=db_lg_category.is_deleted,
        created_at=db_lg_category.created_at,
        updated_at=db_lg_category.updated_at,
        communication_list=db_lg_category.communication_list,
        customer_id=db_lg_category.customer_id,
        customer_name=customer_name,
        type="customer",
        has_all_entity_access=db_lg_category.has_all_entity_access,
        entities_with_access=entities_out
    )


@router.put("/lg-categories/{category_id}", response_model=LGCategoryOut, dependencies=[Depends(check_for_read_only_mode)])
def update_lg_category(
    category_id: int, 
    lg_category_in: LGCategoryUpdate, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("corporate_category:edit")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_lg_category = crud_lg_category.get_for_customer(db, category_id, customer_id)
        if not db_lg_category:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Category not found or does not belong to your customer.")
        
        if db_lg_category.customer_id is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Universal categories can only be updated by a System Owner.")

        updated_lg_category = crud_lg_category.update(db, db_lg_category, lg_category_in, corporate_admin_context.user_id)
        
        return updated_lg_category
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": lg_category_in.category_name, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": lg_category_in.category_name, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.delete("/lg-categories/{category_id}", response_model=LGCategoryOut, dependencies=[Depends(check_for_read_only_mode)])
def delete_lg_category(
    category_id: int, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("corporate_category:delete")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_lg_category = crud_lg_category.get_for_customer(db, category_id, customer_id)
        if not db_lg_category:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Category not found or does not belong to your customer.")
        
        if db_lg_category.customer_id is None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Universal categories can only be deleted by a System Owner.")

        deleted_lg_category = crud_lg_category.soft_delete(db, db_lg_category, corporate_admin_context.user_id)
        
        return deleted_lg_category
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="SOFT_DELETE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": db_lg_category.category_name, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="SOFT_DELETE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": db_lg_category.category_name, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.post("/lg-categories/{category_id}/restore", response_model=LGCategoryOut, dependencies=[Depends(check_for_read_only_mode)])
def restore_lg_category(
    category_id: int, 
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("corporate_category:edit")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_lg_category = db.query(LGCategory).filter(
            LGCategory.id == category_id, 
            LGCategory.customer_id == customer_id,
            LGCategory.is_deleted == True 
        ).first()

        if not db_lg_category:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="LG Category not found or not in a soft-deleted state, or does not belong to your customer."
            )
        
        if db_lg_category.customer_id is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Universal categories can only be restored by a System Owner.")

        restored_lg_category = crud_lg_category.restore(db, db_lg_category, corporate_admin_context.user_id)
        
        return restored_lg_category
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="RESTORE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": db_lg_category.category_name, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="RESTORE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": db_lg_category.category_name, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


# --- Customer Configuration Management (Corporate Admin) ---
# All write operations must use check_for_read_only_mode
@router.get("/customer-configurations/", response_model=List[CustomerConfigurationOut], dependencies=[Depends(check_subscription_status)])
def read_customer_configurations(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("customer_config:view"))
):
    customer_id = corporate_admin_context.customer_id
    configs = crud_customer_configuration.get_all_customer_configs_for_customer(db, customer_id)
    return configs

@router.put("/customer-configurations/{global_config_key}", response_model=CustomerConfigurationOut, dependencies=[Depends(check_for_read_only_mode)])
def update_customer_configuration(
    global_config_key: str,
    config_in: CustomerConfigurationUpdate,
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("customer_config:edit")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        config_key_enum = GlobalConfigKey(global_config_key.upper())
        
        global_config = crud_global_configuration.get_by_key(db, config_key_enum)
        if not global_config:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Global configuration key not found.")

        db_customer_config = crud_customer_configuration.set_customer_config(
            db, 
            customer_id=customer_id, 
            global_config_id=global_config.id,
            configured_value=config_in.configured_value,
            user_id=corporate_admin_context.user_id
        )
        
        response_data = CustomerConfigurationOut(
            id=db_customer_config.id,
            customer_id=db_customer_config.customer_id,
            global_config_id=db_customer_config.global_config_id,
            configured_value=db_customer_config.configured_value,
            
            global_config_key=db_customer_config.global_configuration.key.value,
            effective_value=db_customer_config.configured_value,
            global_value_default=db_customer_config.global_configuration.value_default,
            global_value_min=db_customer_config.global_configuration.value_min,
            global_value_max=db_customer_config.global_configuration.value_max,
            unit=db_customer_config.global_configuration.unit,
            description=db_customer_config.global_configuration.description,
            
            created_at=db_customer_config.created_at,
            updated_at=db_customer_config.updated_at,
            is_deleted=db_customer_config.is_deleted,
            deleted_at=db_customer_config.deleted_at
        )
        
        return response_data
    except ValueError as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="CustomerConfiguration", entity_id=None, details={"key": global_config_key, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="CustomerConfiguration", entity_id=None, details={"key": global_config_key, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="CustomerConfiguration", entity_id=None, details={"key": global_config_key, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


# --- Customer Email Settings Management (Corporate Admin) (NEW SECTION) ---
@router.post("/email-settings/", response_model=CustomerEmailSettingOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(check_for_read_only_mode)])
@router.post("/email-settings/", response_model=CustomerEmailSettingOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(check_for_read_only_mode)])
def create_customer_email_settings(
    settings_in: CustomerEmailSettingCreate,
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("email_setting:manage")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        # NEW LOGIC: Check for existing settings first, including soft-deleted ones.
        existing_settings = db.query(CustomerEmailSetting).filter(
            CustomerEmailSetting.customer_id == customer_id
        ).first()

        if existing_settings:
            # If a record already exists (even if deleted), update it instead of creating a new one.
            settings_update_payload = CustomerEmailSettingUpdate(
                smtp_host=settings_in.smtp_host,
                smtp_port=settings_in.smtp_port,
                smtp_username=settings_in.smtp_username,
                smtp_password=settings_in.smtp_password,
                sender_email=settings_in.sender_email,
                sender_display_name=settings_in.sender_display_name,
                is_active=settings_in.is_active,
            )
            
            # Use the update method and explicitly set is_deleted to False to restore it
            db_settings = crud_customer_email_setting.update(db, existing_settings, settings_update_payload, corporate_admin_context.user_id)
            db_settings.is_deleted = False # Restore the soft-deleted record
            db.add(db_settings)
            
            # The update method handles flushing and logging internally.
            # We explicitly commit here to finalize the transaction for this endpoint.
            db.commit()
            db.refresh(db_settings)
            
            return db_settings
        else:
            # If no settings exist at all, proceed with a new creation.
            db_settings = crud_customer_email_setting.create(db, settings_in, customer_id, corporate_admin_context.user_id)
            db.commit()
            db.refresh(db_settings)
            return db_settings
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="CREATE/UPDATE_FAILED", entity_type="CustomerEmailSetting", entity_id=None, details={"customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="CREATE/UPDATE_FAILED", entity_type="CustomerEmailSetting", entity_id=None, details={"customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.get("/email-settings/", response_model=Optional[CustomerEmailSettingOut], dependencies=[Depends(check_subscription_status)])
def get_customer_email_settings(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("email_setting:manage"))
):
    customer_id = corporate_admin_context.customer_id
    settings = crud_customer_email_setting.get_by_customer_id(db, customer_id)

    # NEW: Add a check to ensure the returned settings are valid and not soft-deleted.
    if settings and not settings.is_deleted and settings.smtp_host and settings.sender_email and settings.smtp_username and settings.smtp_password_encrypted:
        return settings
    
    return None

@router.put("/email-settings/{setting_id}", response_model=CustomerEmailSettingOut, dependencies=[Depends(check_for_read_only_mode)])
def update_customer_email_settings(
    setting_id: int,
    settings_in: CustomerEmailSettingUpdate,
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("email_setting:manage")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_settings = crud_customer_email_setting.get(db, setting_id)
        if not db_settings or db_settings.customer_id != customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Email settings not found or do not belong to your customer.")
        
        updated_settings = crud_customer_email_setting.update(db, db_settings, settings_in, corporate_admin_context.user_id)
        return updated_settings
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="CustomerEmailSetting", entity_id=setting_id, details={"customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="UPDATE_FAILED", entity_type="CustomerEmailSetting", entity_id=setting_id, details={"customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.delete("/email-settings/{setting_id}", response_model=CustomerEmailSettingOut, dependencies=[Depends(check_for_read_only_mode)])
def delete_customer_email_settings(
    setting_id: int,
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("email_setting:manage")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_settings = crud_customer_email_setting.get(db, setting_id)
        if not db_settings or db_settings.customer_id != customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Email settings not found or do not belong to your customer.")
        
        deleted_settings = crud_customer_email_setting.soft_delete(db, db_settings, corporate_admin_context.user_id)
        return deleted_settings
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="SOFT_DELETE_FAILED", entity_type="CustomerEmailSetting", entity_id=setting_id, details={"customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="SOFT_DELETE_FAILED", entity_type="CustomerEmailSetting", entity_id=setting_id, details={"customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.post("/email-settings/{setting_id}/restore", response_model=CustomerEmailSettingOut, dependencies=[Depends(check_for_read_only_mode)])
def restore_customer_email_settings(
    setting_id: int,
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("email_setting:manage")),
    request: Request = None
):
    client_host = request.client.host if request else None
    customer_id = corporate_admin_context.customer_id

    try:
        db_settings = db.query(CustomerEmailSetting).filter(
            CustomerEmailSetting.id == setting_id,
            CustomerEmailSetting.customer_id == customer_id,
            CustomerEmailSetting.is_deleted == True
        ).first()

        if not db_settings:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Email settings not found or not in a soft-deleted state, or do not belong to your customer."
            )
        
        restored_settings = crud_customer_email_setting.restore(db, db_settings, corporate_admin_context.user_id)
        return restored_settings
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="RESTORE_FAILED", entity_type="CustomerEmailSetting", entity_id=setting_id, details={"customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="RESTORE_FAILED", entity_type="CustomerEmailSetting", entity_id=setting_id, details={"customer_id": customer_id, "reason": str(e)}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


# --- NEW: Approval Request Management for Corporate Admin (Checker) ---
# All write operations must use check_for_read_only_mode

@router.get(
    "/approval-requests/",
    response_model=List[ApprovalRequestOut],
    dependencies=[Depends(HasPermission("approval_request:view_all")), Depends(check_subscription_status)],
    summary="List all approval requests for the Corporate Admin's customer"
)
def list_approval_requests(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context),
    status: Optional[ApprovalRequestStatusEnum] = Query(None, description="Filter by status (e.g., PENDING, APPROVED, REJECTED)"),
    action_type: Optional[str] = Query(None, description="Filter by action type (e.g., LG_RELEASE, LG_LIQUIDATE)"),
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieves a list of approval requests for the authenticated Corporate Admin's customer.
    Allows filtering by status and action type. Eager-loads related LGRecord and User data.
    """
    customer_id = corporate_admin_context.customer_id
    
    requests = crud_approval_request.get_all_for_customer(
        db,
        customer_id=customer_id,
        status_filter=status,
        action_type_filter=action_type,
        skip=skip,
        limit=limit
    )
    
    return [ApprovalRequestOut.model_validate(req) for req in requests]

@router.get(
    "/approval-requests/{request_id}",
    response_model=ApprovalRequestOut,
    dependencies=[Depends(HasPermission("approval_request:view_all")), Depends(check_subscription_status)],
    summary="Get details of a specific approval request"
)
def get_approval_request_details(
    request_id: int,
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context)
):
    """
    Retrieves the details of a specific approval request, including the LG record snapshot
    and current LG record details for comparison.
    """
    customer_id = corporate_admin_context.customer_id

    db_request = crud_approval_request.get_approval_request_by_id(db, request_id, customer_id)
    
    if not db_request:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval request not found or not accessible.")
    
    return ApprovalRequestOut.model_validate(db_request)


@router.post(
    "/approval-requests/{request_id}/approve",
    response_model=ApprovalRequestOut,
    dependencies=[Depends(HasPermission("approval_request:approve")), Depends(check_for_read_only_mode)],
    summary="Approve a pending approval request"
)
async def approve_approval_request(
    request_id: int,
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context),
    request_info: Request = None
):
    """
    Allows a Corporate Admin (Checker) to approve a pending approval request.
    This executes the underlying action (Release, Liquidation, etc.) and invalidates other pending requests.
    """
    client_host = request_info.client.host if request_info else None
    customer_id = corporate_admin_context.customer_id

    try:
        approved_request = await crud_approval_request.approve_request(
            db, 
            request_id=request_id,
            checker_user_id=corporate_admin_context.user_id,
            customer_id=customer_id
        )
        
        log_action(
            db,
            user_id=corporate_admin_context.user_id,
            action_type="APPROVAL_REQUEST_APPROVED",
            entity_type="ApprovalRequest",
            entity_id=approved_request.id,
            details={
                "lg_record_id": approved_request.entity_id if approved_request.entity_type == "LGRecord" else None,
                "action_type": approved_request.action_type,
                "status": approved_request.status.value,
                "checker_email": corporate_admin_context.email,
            },
            customer_id=customer_id,
            lg_record_id=approved_request.entity_id if approved_request.entity_type == "LGRecord" else None,
        )
        
        return ApprovalRequestOut.model_validate(approved_request)
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="APPROVAL_REQUEST_APPROVED_FAILED", entity_type="ApprovalRequest", entity_id=request_id, details={"reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="APPROVAL_REQUEST_APPROVED_FAILED", entity_type="ApprovalRequest", entity_id=request_id, details={"reason": f"An unexpected error occurred: {e}"}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )


@router.post(
    "/approval-requests/{request_id}/reject",
    response_model=ApprovalRequestOut,
    dependencies=[Depends(HasPermission("approval_request:reject")), Depends(check_for_read_only_mode)],
    summary="Reject a pending approval request"
)
async def reject_approval_request(
    request_id: int,
    reason: str = Body(..., embed=True, description="Reason for rejecting the approval request"),
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context),
    request_info: Request = None
):
    """
    Allows a Corporate Admin (Checker) to reject a pending approval request.
    A reason for rejection is mandatory.
    """
    client_host = request_info.client.host if request_info else None
    customer_id = corporate_admin_context.customer_id

    try:
        rejected_request = crud_approval_request.reject_request(
            db, 
            request_id=request_id,
            checker_user_id=corporate_admin_context.user_id,
            customer_id=customer_id,
            reason=reason
        )
        
        log_action(
            db,
            user_id=corporate_admin_context.user_id,
            action_type="APPROVAL_REQUEST_REJECTED",
            entity_type="ApprovalRequest",
            entity_id=rejected_request.id,
            details={
                "lg_record_id": rejected_request.entity_id if rejected_request.entity_type == "LGRecord" else None,
                "action_type": rejected_request.action_type,
                "status": rejected_request.status.value,
                "checker_email": corporate_admin_context.email,
                "rejection_reason": reason,
            },
            customer_id=customer_id,
            lg_record_id=rejected_request.entity_id if rejected_request.entity_type == "LGRecord" else None,
        )
        return ApprovalRequestOut.model_validate(rejected_request)
    except HTTPException as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="APPROVAL_REQUEST_REJECTED_FAILED", entity_type="ApprovalRequest", entity_id=request_id, details={"reason": str(e.detail)}, customer_id=customer_id)
        raise
    except Exception as e:
        log_action(db, user_id=corporate_admin_context.user_id, action_type="APPROVAL_REQUEST_REJECTED_FAILED", entity_type="ApprovalRequest", entity_id=request_id, details={"reason": f"An unexpected error occurred: {e}"}, customer_id=customer_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.get("/audit-logs/", response_model=List[AuditLogOut], dependencies=[Depends(check_subscription_status)])
def read_corporate_admin_audit_logs(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(HasPermission("audit_log:view")),
    skip: int = 0,
    limit: int = 100,
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    action_type: Optional[str] = Query(None, description="Filter by type of action (e.g., CREATE, UPDATE)"),
    entity_type: Optional[str] = Query(None, description="Filter by type of entity (e.g., User, CustomerEntity)"),
    entity_id: Optional[int] = Query(None, description="Filter by ID of the entity"),
    lg_record_id: Optional[int] = Query(None, description="Filter by ID of the LG Record (if applicable)")
):
    """
    Retrieves a list of audit log entries for the authenticated Corporate Admin's customer.
    Filters are applied on top of the customer scope.
    """
    customer_id = corporate_admin_context.customer_id
    
    # Assuming a new function is created in crud_audit.py
    # that correctly handles timestamp retrieval.
    logs = crud_audit_log.get_all_logs(
        db,
        skip=skip,
        limit=limit,
        customer_id=customer_id,
        user_id=user_id,
        action_type=action_type,
        entity_type=entity_type,
        entity_id=entity_id,
        lg_record_id=lg_record_id
    )
    
    return logs

# --- NEW: Action Center Endpoints for Corporate Admin ---
# All read operations must use check_subscription_status
@router.get(
    "/action-center/lg-for-renewal",
    response_model=List[LGRecordOut],
    dependencies=[Depends(HasPermission("action_center:view")), Depends(check_subscription_status)],
    summary="Get LGs for renewal for the Corporate Admin's customer"
)
def get_action_center_lg_for_renewal(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context),
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieves LG records approaching expiry/renewal based on configured thresholds.
    """
    customer_id = corporate_admin_context.customer_id
    renewal_list = crud_lg_record.get_lg_records_for_renewal_reminder(db, customer_id)
    return renewal_list

@router.get(
    "/action-center/instructions/undelivered",
    response_model=List[LGInstructionOut],
    dependencies=[Depends(HasPermission("action_center:view")), Depends(check_subscription_status)],
    summary="Get undelivered instructions for the Corporate Admin's customer"
)
def get_action_center_instructions_undelivered(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context),
    skip: int = 0,
    limit: int = 100
):
    customer_id = corporate_admin_context.customer_id
    
    # Safely retrieve configuration for the start days
    report_start_days_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id, GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_REPORT_UNDELIVERED
    )
    # Safely retrieve configuration for the stop days
    report_stop_days_config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id, GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_STOP_REPORTING_UNDELIVERED
    )
    
    # Extract values with a default if the config is missing
    report_start_days = int(report_start_days_config.get('effective_value', 3)) if report_start_days_config else 3
    report_stop_days = int(report_stop_days_config.get('effective_value', 60)) if report_stop_days_config else 60

    if not (0 <= report_start_days < report_stop_days):
        logger.warning(f"Invalid report start/stop days config for customer {customer_id}. Using default range.")
        report_start_days = 3
        report_stop_days = 60
    
    instructions = crud_lg_instruction.get_undelivered_instructions_for_reporting(
        db, customer_id, report_start_days, report_stop_days
    )
    return instructions[skip:skip+limit]

@router.get(
    "/action-center/instructions/awaiting-reply",
    response_model=List[LGInstructionOut],
    dependencies=[Depends(HasPermission("action_center:view")), Depends(check_subscription_status)],
    summary="Get instructions awaiting a bank reply for the Corporate Admin's customer"
)
def get_action_center_instructions_awaiting_reply(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context),
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieves instructions that have been delivered but are awaiting a bank reply, based on configurable thresholds.
    """
    customer_id = corporate_admin_context.customer_id
    
    days_since_delivery = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id, GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_DELIVERY
    )['effective_value']
    
    days_since_issuance = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id, GlobalConfigKey.REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE
    )['effective_value']
    
    max_days_since_issuance = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id, GlobalConfigKey.REMINDER_TO_BANKS_MAX_DAYS_SINCE_ISSUANCE
    )['effective_value']

    awaiting_reply_instructions = crud_lg_instruction.get_instructions_for_bank_reminders(
        db, customer_id, int(days_since_delivery), int(days_since_issuance), int(max_days_since_issuance)
    )

    return awaiting_reply_instructions


@router.get(
    "/action-center/requests/pending-print",
    response_model=List[ApprovalRequestOut],
    dependencies=[Depends(HasPermission("action_center:view")), Depends(check_subscription_status)],
    summary="Get approved requests pending printing for the Corporate Admin's customer"
)
def get_approved_requests_pending_print(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context),
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieves approved approval requests that have generated a letter and are awaiting printing.
    """
    customer_id = corporate_admin_context.customer_id
    
    # Define which action types require printing (this is also defined in background tasks)
    INSTRUCTION_TYPES_REQUIRING_PRINTING = [
        "LG_RELEASE",
        "LG_LIQUIDATE",
        "LG_DECREASE_AMOUNT",
        "LG_AMENDMENT",
        "LG_ACTIVATE_NON_OPERATIVE",
        "LG_REMINDER_TO_BANKS",
        "LG_OWNER_CHANGE"
    ]
    
    pending_print_requests = crud_approval_request.get_all_for_customer(
        db,
        customer_id=customer_id,
        status_filter=ApprovalRequestStatusEnum.APPROVED,
        action_type_filter=INSTRUCTION_TYPES_REQUIRING_PRINTING
    )
    
    # Filter out requests that are already marked as printed in the instruction
    result = [
        req for req in pending_print_requests
        if req.related_instruction and not req.related_instruction.is_printed
    ]
    
    return result[skip:skip + limit]

@router.get(
    "/system-notifications/",
    response_model=List[SystemNotificationOut],
    dependencies=[Depends(check_subscription_status)],
    summary="Get active system notifications for the Corporate Admin's customer"
)
def get_active_system_notifications(
    db: Session = Depends(get_db),
    corporate_admin_context: TokenData = Depends(get_current_corporate_admin_context),
):
    """
    Retrieves all active system notifications relevant to the authenticated Corporate Admin.
    """
    customer_id = corporate_admin_context.customer_id
    user_id = corporate_admin_context.user_id
    
    # Use the updated CRUD function which handles all filtering logic
    notifications = crud_system_notification.get_active_notifications_for_user(
        db, user_id=user_id, customer_id=customer_id
    )

    # Log views for all notifications returned to the user
    for notification in notifications:
        try:
            # This will either create a new log or increment an existing one
            crud_system_notification_view_log.increment_view_count(db, user_id, notification.id)
        except Exception as e:
            # Log the error but don't fail the API call
            logger.error(f"Failed to log view for notification ID {notification.id} for user {user_id}: {e}")
            db.rollback() # Rollback any changes in this specific logging action
    
    return notifications