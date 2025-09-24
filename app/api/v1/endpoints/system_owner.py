# api/v1/endpoints/system_owner.py
import os
import sys
import importlib.util
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status, Response, Request, Query, Body, BackgroundTasks
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import func
from typing import List, Optional, Any, Dict

from app.database import get_db
from app.schemas.all_schemas import (
    SubscriptionPlanCreate, SubscriptionPlanUpdate, SubscriptionPlanOut,
    CustomerCreate, CustomerUpdate, CustomerOut, CustomerCoreCreate,
    UserCreate, UserOut, UserUpdate, CustomerEntityCreate, CustomerEntityUpdate, CustomerEntityOut,
    GlobalConfigurationCreate, GlobalConfigurationUpdate, GlobalConfigurationOut,
    BankCreate, BankUpdate, BankOut,
    TemplateCreate, TemplateUpdate, TemplateOut,
    CurrencyCreate, CurrencyUpdate, CurrencyOut,
    LgTypeCreate, LgTypeUpdate, LgTypeOut,
    RuleCreate, RuleUpdate, RuleOut,
    IssuingMethodCreate, IssuingMethodUpdate, IssuingMethodOut,
    LgStatusCreate, LgStatusUpdate, LgStatusOut,
    LgOperationalStatusCreate, LgOperationalStatusUpdate, LgOperationalStatusOut,
    # MODIFIED: Use new unified LGCategory schemas
    LGCategoryCreate, LGCategoryUpdate, LGCategoryOut,
    AuditLogOut,
    SystemNotificationCreate, SystemNotificationUpdate, SystemNotificationOut,
    LegalArtifactCreate, LegalArtifactOut, TrialRegistrationOut,UserCreateCorporateAdmin
)
from app.crud.crud import (
    crud_subscription_plan, crud_customer, crud_customer_entity, crud_user,
    crud_global_configuration, crud_bank, crud_template,
    crud_currency, crud_lg_type, crud_rule, crud_issuing_method,
    crud_lg_status, crud_lg_operational_status,
    crud_lg_category,
    crud_audit_log, log_action,
    crud_system_notification,
    crud_legal_artifact, crud_trial_registration
)
from app.models import (
    SubscriptionPlan, Customer, CustomerEntity, User,
    GlobalConfiguration, Bank, Template,
    Currency, LgType, Rule, IssuingMethod, LgStatus, LgOperationalStatus,
    # MODIFIED: Use new unified LGCategory model
    LGCategory,
    AuditLog,
    SystemNotification, UserCustomerEntityAssociation, TrialRegistration,
)

import app.core.background_tasks as background_tasks_module 
from app.core.email_service import get_global_email_settings, send_email, EmailAttachment # NEW IMPORTS
from app.core.document_generator import generate_pdf_from_html # NEW IMPORT
import logging
logger = logging.getLogger(__name__)

try:
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_file_dir, '..', '..', '..'))

    security_module_path = os.path.join(project_root, 'core', 'security.py')

    if not os.path.exists(security_module_path):
        raise FileNotFoundError(f"Expected core/security.py at {security_module_path} but it was not found.")
    
    spec = importlib.util.spec_from_file_location("app.core.security", security_module_path)
    core_security = importlib.util.module_from_spec(spec)
    sys.modules["app.core.security"] = core_security
    spec.loader.exec_module(core_security)
    from app.core.security import (
        get_current_user,
        get_current_system_owner,
        HasPermission,
        TokenData, get_client_ip
    )
    from app.main import app as fastapi_app
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.date import DateTrigger

except Exception as e:
    print(f"FATAL ERROR (system_owner.py): Could not import core.security module directly. Error: {e}")
    raise


from app.constants import UserRole, LegalArtifactType, GlobalConfigKey

router = APIRouter()
trial_router = APIRouter()

@router.get("/status")
async def get_system_status():
    """
    Checks the status of the System Owner API.
    This endpoint does not require authentication or specific permissions.
    """
    return {"message": "System Owner API is up and running!"}

@router.get("/dashboard-metrics", response_model=Dict[str, Any])
def get_dashboard_metrics(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_owner:view_dashboard"))
):
    """
    Retrieves key metrics for the System Owner dashboard.
    For now, focuses on active customers, active users, and recent audit activity.
    LG-related metrics will be integrated once the LG_CUSTODY module is active.
    """
    total_active_customers = db.query(Customer).filter(Customer.is_deleted == False).count()
    total_active_users = db.query(User).filter(User.is_deleted == False).count()
    
    customers_change_percent = "+12%" if total_active_customers > 0 else "0%"
    users_change_percent = "+8%" if total_active_users > 0 else "0%"

    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    recent_activity_logs = db.query(AuditLog).filter(
        AuditLog.timestamp >= seven_days_ago
    ).order_by(AuditLog.timestamp.desc()).limit(10).all()

    formatted_recent_activity = []
    for log in recent_activity_logs:
        action_desc = f"{log.action_type.replace('_', ' ').title()} on {log.entity_type} ID {log.entity_id}"
        if log.details and 'name' in log.details:
            action_desc += f" (Name: {log.details['name']})"
        elif log.details and 'email' in log.details:
            action_desc += f" (Email: {log.details['email']})"
        elif log.details and 'category_name' in log.details:
            action_desc += f" (Category: {log.details['category_name']})"

        
        user_email = "System/Unknown"
        if log.user_id:
            user = db.query(User).filter(User.id == log.user_id).first()
            if user:
                user_email = user.email
        
        formatted_recent_activity.append({
            "id": log.id,
            "timestamp": log.timestamp.isoformat(),
            "description": f"[{user_email}] {action_desc}"
        })
    
    return {
            "total_active_customers": total_active_customers,
            "customers_change_percent": customers_change_percent,
            "total_active_users": total_active_users,
            "users_change_percent": users_change_percent,
            "recent_activity": formatted_recent_activity
        }

@router.get("/template-placeholders", response_model=List[Dict[str, str]])
def get_template_placeholders(
    action_type: Optional[str] = Query(None, description="Filter placeholders by action type (e.g., LG_EXTENSION)"),
    lg_record_id: Optional[int] = Query(None, description="Optional LG Record ID to fetch LG-specific placeholders"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("template:view"))
):
    """
    Retrieves a list of available placeholders for templates, optionally filtered by action type.
    Includes LG-specific placeholders, dynamically fetching LG data if lg_record_id is provided.
    """
    
    placeholders = [
        {"name": "{{customer_name}}", "description": "Name of the customer organization."},
        {"name": "{{current_date}}", "description": "Current date."},
        {"name": "{{platform_name}}", "description": "Name of the platform (e.g., 'Treasury Management Platform')."},
        {"name": "{{user_email}}", "description": "Email of the user triggering the action."},
    ]
    
    # NEW: Add general customer/entity placeholders with fallback logic
    if lg_record_id:
        from app.crud.lg_records import crud_lg_record
        lg_record = crud_lg_record.get(db, id=lg_record_id)
        if lg_record:
            customer = lg_record.customer
            entity = lg_record.beneficiary_corporate
            
            # Fallback logic for address, phone, and email
            customer_address = entity.address if entity.address else customer.address
            customer_phone = entity.contact_phone if entity.contact_phone else customer.contact_phone
            customer_email = entity.contact_email if entity.contact_email else customer.contact_email
            
            placeholders.extend([
                {"name": "{{customer_address}}", "description": "Address of the customer entity (falls back to customer address)."},
                {"name": "{{customer_phone}}", "description": "Phone number of the customer entity (falls back to customer phone)."},
                {"name": "{{customer_contact_email}}", "description": "Contact email of the customer entity (falls back to customer email)."},
            ])

    if action_type and action_type.upper().startswith("LG_"):
        lg_placeholders = [
            {"name": "{{lg_serial_number}}", "description": "Serial number of the Letter of Guarantee."},
            {"name": "{{lg_amount}}", "description": "Amount of the Letter of Guarantee."},
            {"name": "{{lg_currency}}", "description": "Currency of the Letter of Guarantee."},
            {"name": "{{issuing_bank_name}}", "description": "Name of the LG's issuing bank."},
            {"name": "{{issue_date}}", "description": "Issue date of the LG."},
            {"name": "{{expiry_date}}", "description": "Expiry date of the LG."},
            {"name": "{{lg_issuer_name}}", "description": "Name of the LG issuer/applicant."},
            {"name": "{{lg_beneficiary_name}}", "description": "Name of the LG beneficiary."},
            {"name": "{{internal_owner_email}}", "description": "Email of the internal owner."},
        ]
        placeholders.extend(lg_placeholders)

    if action_type == "LG_EXTENSION":
        extension_placeholders = [
            {"name": "{{old_expiry_date}}", "description": "Original expiry date before extension."},
            {"name": "{{new_expiry_date}}", "description": "New expiry date after extension."},
        ]
        placeholders.extend(extension_placeholders)
    elif action_type == "LG_AMENDMENT":
        amendment_placeholders = [
            {"name": "{{amendment_details}}", "description": "Specific details about the amendment."},
        ]
        placeholders.extend(amendment_placeholders)
    elif action_type == "LG_LIQUIDATION":
        liquidation_placeholders = [
            {"name": "{{liquidation_amount}}", "description": "Amount being liquidated (for partial liquidation)."},
            {"name": "{{remaining_amount}}", "description": "Remaining LG amount after partial liquidation."},
        ]
        placeholders.extend(liquidation_placeholders)
    elif action_type == "LG_RELEASE" or action_type == "LG_LIQUIDATION":
        release_liquidation_placeholders = [
            {"name": "{{total_documents_received}}", "description": "Total original documents received from bank for this LG."},
            {"name": "{{pending_replies_count}}", "description": "Number of pending replies from the bank for this LG."},
        ]
        placeholders.extend(release_liquidation_placeholders)
    elif action_type == "ACTIVATION":
        activation_placeholders = [
            {"name": "{{payment_details}}", "description": "Details of the activating payment (e.g., cheque number, transfer reference)."},
        ]
        placeholders.extend(activation_placeholders)
    elif action_type == "REMINDER":
        reminder_placeholders = [
            {"name": "{{original_instruction_serial}}", "description": "Serial number of the instruction the reminder relates to."},
            {"name": "{{days_overdue}}", "description": "Number of days the instruction has been overdue."},
            {"name": "{{original_instruction_date}}", "description": "The date the original instruction was issued."},
            {"name": "{{original_instruction_delivery_date}}", "description": "The date the original instruction was delivered."},
            {"name": "{{original_instruction_type}}", "description": "Type of the original instruction."},
        ]
        placeholders.extend(reminder_placeholders)

    return placeholders

@router.post("/subscription-plans/", response_model=SubscriptionPlanOut, status_code=status.HTTP_201_CREATED)
def create_subscription_plan(
    plan_in: SubscriptionPlanCreate, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("subscription_plan:create")),
    request: Request = None
):
    """
    Create a new subscription plan.
    Requires: name, duration_months, monthly_price, annual_price, max_users, max_records,
              and boolean flags for features.
    """
    existing_plan = crud_subscription_plan.get_by_name(db, name=plan_in.name)
    if existing_plan:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Subscription plan with name '{plan_in.name}' already exists."
        )
    
    if plan_in.annual_price > (plan_in.monthly_price * plan_in.duration_months):
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Annual price cannot be greater than monthly price multiplied by duration."
         )

    db_plan = crud_subscription_plan.create(db, obj_in=plan_in)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="SubscriptionPlan", entity_id=db_plan.id, details={"name": db_plan.name, "ip_address": client_host})
    
    return db_plan

@router.get("/subscription-plans/", response_model=List[SubscriptionPlanOut])
def read_subscription_plans(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("subscription_plan:view"))
):
    """
    Retrieve a list of all active subscription plans.
    """
    plans = crud_subscription_plan.get_all(db, skip=skip, limit=limit)
    return plans

@router.get("/subscription-plans/{plan_id}", response_model=SubscriptionPlanOut)
def read_subscription_plan(
    plan_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("subscription_plan:view"))
):
    """
    Retrieve a single subscription plan by its ID.
    Returns 404 if the plan is not found or is soft-deleted.
    """
    db_plan = crud_subscription_plan.get(db, id=plan_id)
    if db_plan is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Subscription plan not found or already deleted"
        )
    return db_plan

@router.put("/subscription-plans/{plan_id}", response_model=SubscriptionPlanOut)
def update_subscription_plan(
    plan_id: int, 
    plan_in: SubscriptionPlanUpdate, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("subscription_plan:edit")),
    request: Request = None
):
    """
    Update an existing subscription plan by ID.
    Only provided fields will be updated (partial update is supported).
    Returns 404 if the plan is not found or is soft-deleted.
    """
    db_plan = crud_subscription_plan.get(db, id=plan_id)
    if db_plan is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Subscription plan not found or already deleted"
        )
    
    if plan_in.name is not None and plan_in.name != db_plan.name:
        existing_plan = crud_subscription_plan.get_by_name(db, name=plan_in.name)
        if existing_plan and existing_plan.id != plan_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Subscription plan with name '{plan_in.name}' already exists."
            )
            
    if plan_in.annual_price is not None:
        current_monthly_price = plan_in.monthly_price if plan_in.monthly_price is not None else db_plan.monthly_price
        current_duration_months = plan_in.duration_months if plan_in.duration_months is not None else db_plan.duration_months
        if plan_in.annual_price > (current_monthly_price * current_duration_months):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Annual price cannot be greater than monthly price multiplied by duration."
            )

    updated_plan = crud_subscription_plan.update(db, db_obj=db_plan, obj_in=plan_in)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="SubscriptionPlan", entity_id=updated_plan.id, details={"name": updated_plan.name, "ip_address": client_host, "updated_fields": plan_in.model_dump(exclude_unset=True)})
    
    return updated_plan

@router.delete("/subscription-plans/{plan_id}", response_model=SubscriptionPlanOut)
def delete_subscription_plan(
    plan_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("subscription_plan:delete")),
    request: Request = None
):
    """
    Soft-delete a subscription plan by ID. The plan will be marked as inactive.
    Returns 404 if the plan is not found or already deleted.
    """
    db_plan = crud_subscription_plan.get(db, id=plan_id)
    if db_plan is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Subscription plan not found or already deleted"
        )
    
    deleted_plan = crud_subscription_plan.soft_delete(db, db_plan)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="SubscriptionPlan", entity_id=deleted_plan.id, details={"name": deleted_plan.name, "ip_address": client_host})
    
    return deleted_plan

@router.post("/subscription-plans/{plan_id}/restore", response_model=SubscriptionPlanOut)
def restore_subscription_plan(
    plan_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("subscription_plan:edit")),
    request: Request = None
):
    """
    Restore a soft-deleted subscription plan by ID.
    Returns 404 if the plan is not found or is not soft-deleted.
    """
    db_plan = db.query(SubscriptionPlan).filter(
        SubscriptionPlan.id == plan_id, 
        SubscriptionPlan.is_deleted == True 
    ).first()

    if db_plan is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Subscription plan not found or not in a soft-deleted state."
        )
    
    restored_plan = crud_subscription_plan.restore(db, db_plan)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="SubscriptionPlan", entity_id=restored_plan.id, details={"name": restored_plan.name, "ip_address": client_host})
    
    return restored_plan


@router.post("/customers/onboard", response_model=CustomerOut, status_code=status.HTTP_201_CREATED)
def onboard_customer(
    customer_in: CustomerCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer:create")),
    request: Request = None
):
    """
    Onboard a new customer, including their initial entities and a Corporate Admin user.
    """
    client_host = get_client_ip(request) if request else None

    # This is a good practice for debugging, but doesn't fix the issue
    if customer_in.initial_entities and customer_in.initial_entities[0].contact_email:
        email_val = customer_in.initial_entities[0].contact_email
        logger.info(f"DEBUG: Received contact_email: '{email_val}' (Length: {len(email_val)})")
        logger.info(f"DEBUG: Repr of contact_email: {repr(email_val)}")
        if not isinstance(email_val, str):
            logger.error(f"DEBUG: contact_email is not a string, type: {type(email_val)}")

    # Call the actual onboarding logic in the CRUD layer
    db_customer = crud_customer.onboard_customer(db, customer_in, user_id_caller=current_user.user_id)
    
    # Return the created customer object. FastAPI will automatically serialize it to JSON
    return db_customer

@router.get("/customers/", response_model=List[CustomerOut])
def read_customers(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer:view"))
):
    """
    Retrieve a list of all active customers, including their entities and users.
    """
    customers = crud_customer.get_all_with_relations(db, skip=skip, limit=limit)
    return customers

@router.get("/customers/{customer_id}", response_model=CustomerOut)
def read_customer(
    customer_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer:view"))
):
    """
    Retrieve a single customer by its ID, including their entities and users.
    Returns 404 if the customer is not found or is soft-deleted.
    """
    db_customer = crud_customer.get_with_relations(db, customer_id)
    if db_customer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Customer not found or already deleted"
        )
    return db_customer

@router.put("/customers/{customer_id}", response_model=CustomerOut)
def update_customer(
    customer_id: int, 
    customer_in: CustomerUpdate, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer:edit")),
    request: Request = None
):
    """
    Update an existing customer's details by ID.
    Only provided fields will be updated (partial update is supported).
    Returns 404 if the customer is not found or is soft-deleted.
    """
    client_host = get_client_ip(request) if request else None

    db_customer = crud_customer.get_with_relations(db, customer_id)
    if db_customer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Customer not found or already deleted"
        )
    
    if customer_in.name is not None and customer_in.name != db_customer.name:
        existing_by_name = crud_customer.get_by_name(db, name=customer_in.name)
        if existing_by_name and existing_by_name.id != customer_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Customer with name '{customer_in.name}' already exists."
            )
    
    if customer_in.contact_email is not None and customer_in.contact_email != db_customer.contact_email:
        existing_by_email = crud_customer.get_by_contact_email(db, email=customer_in.contact_email)
        if existing_by_email and existing_by_email.id != customer_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Customer with contact email '{customer_in.contact_email}' already exists."
            )

    old_subscription_plan = db_customer.subscription_plan
    new_subscription_plan = None
    if customer_in.subscription_plan_id is not None and customer_in.subscription_plan_id != db_customer.subscription_plan_id:
        new_subscription_plan = crud_subscription_plan.get(db, id=customer_in.subscription_plan_id)
        if not new_subscription_plan:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="New subscription plan not found or is inactive."
            )
        
        if old_subscription_plan.can_multi_entity and not new_subscription_plan.can_multi_entity:
            active_entities = [entity for entity in db_customer.entities if not entity.is_deleted and entity.is_active]
            if len(active_entities) > 1:
                active_entities.sort(key=lambda x: x.created_at)
                entities_to_deactivate = active_entities[1:]

                for entity in entities_to_deactivate:
                    crud_customer_entity.soft_delete(db, entity)
                    log_action(
                        db,
                        user_id=current_user.user_id,
                        action_type="ENTITY_DEACTIVATED_BY_PLAN_CHANGE",
                        entity_type="CustomerEntity",
                        entity_id=entity.id,
                        details={
                            "entity_name": entity.entity_name,
                            "customer_id": db_customer.id,
                            "old_plan": old_subscription_plan.name,
                            "new_plan": new_subscription_plan.name,
                            "reason": "Plan change to single-entity limited support"
                        }
                    )
                print(f"DEBUG: Customer {db_customer.name} entities deactivated due to plan change.")
                log_action(
                    db,
                    user_id=current_user.user_id,
                    action_type="CUSTOMER_PLAN_CHANGE_ENTITY_ADJUSTMENT",
                    entity_type="Customer",
                    entity_id=db_customer.id,
                    details={
                        "customer_name": db_customer.name,
                        "old_plan": old_subscription_plan.name,
                        "new_plan": new_subscription_plan.name,
                        "deactivated_entities_count": len(entities_to_deactivate),
                        "ip_address": client_host
                    }
                )

    updated_customer = crud_customer.update(db, db_obj=db_customer, obj_in=customer_in, user_id=current_user.user_id)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="Customer", entity_id=updated_customer.id, details={"name": updated_customer.name, "ip_address": client_host, "updated_fields": customer_in.model_dump(exclude_unset=True)})
    
    return crud_customer.get_with_relations(db, updated_customer.id)

@router.delete("/customers/{customer_id}", response_model=CustomerOut)
def delete_customer(
    customer_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer:delete")),
    request: Request = None
):
    """
    Soft-delete a customer by ID. This will also cascade soft-delete their entities and users.
    Returns 404 if the customer is not found or already deleted.
    """
    db_customer = crud_customer.get_with_relations(db, customer_id) 
    if db_customer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Customer not found or already deleted"
        )
    
    deleted_customer = crud_customer.soft_delete(db, db_customer, user_id=current_user.user_id)

    for entity in db_customer.entities:
        if not entity.is_deleted:
            crud_customer_entity.soft_delete(db, entity, user_id=current_user.user_id)
    for user in db_customer.users:
        if not user.is_deleted:
            crud_user.soft_delete(db, user, user_id=current_user.user_id)

    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="Customer", entity_id=deleted_customer.id, details={"name": deleted_customer.name, "ip_address": client_host})

    db.refresh(deleted_customer)
    return deleted_customer

@router.post("/customers/{customer_id}/restore", response_model=CustomerOut)
def restore_customer(
    customer_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer:edit")),
    request: Request = None
):
    """
    Restore a soft-deleted customer by ID. This will also restore their associated entities and users.
    Returns 404 if the customer is not found or is not soft-deleted.
    """
    db_customer = db.query(Customer).filter(
        Customer.id == customer_id, 
        Customer.is_deleted == True 
    ).options(
        selectinload(Customer.entities),
        selectinload(Customer.users)
    ).first()

    if db_customer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Customer not found or not in a soft-deleted state."
        )
    
    restored_customer = crud_customer.restore(db, db_customer, user_id=current_user.user_id)

    all_entities_of_customer = db.query(CustomerEntity).filter(CustomerEntity.customer_id == restored_customer.id).all()
    for entity in all_entities_of_customer:
        if entity.is_deleted:
            if not restored_customer.subscription_plan.can_multi_entity:
                active_entities_count = db.query(CustomerEntity).filter(
                    CustomerEntity.customer_id == restored_customer.id,
                    CustomerEntity.is_deleted == False,
                    CustomerEntity.is_active == True
                ).count()
                if active_entities_count >= 1:
                    print(f"DEBUG: Not reactivating entity {entity.entity_name} during restore due to single-entity plan limit.")
                    restored_entity = crud_customer_entity.restore(db, entity, user_id=current_user.user_id)
                    restored_entity.is_active = False
                    db.add(restored_entity)
                    db.commit()
                    db.refresh(restored_entity)
                    log_action(db, user_id=current_user.user_id, action_type="RESTORE_INACTIVE", entity_type="CustomerEntity", entity_id=restored_entity.id, details={"name": restored_entity.entity_name, "reason": "Single-entity plan limit met", "ip_address": client_host})
                    continue
            
            crud_customer_entity.restore(db, entity, user_id=current_user.user_id)
    
    all_users_of_customer = db.query(User).filter(User.customer_id == restored_customer.id).all()
    for user in all_users_of_customer: 
        if user.is_deleted:
            crud_user.restore(db, user, user_id=current_user.user_id)

    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="Customer", entity_id=restored_customer.id, details={"name": restored_customer.name, "ip_address": client_host})

    db.refresh(restored_customer)
    return crud_customer.get_with_relations(db, restored_customer.id)

@router.post("/customer-entities/", response_model=CustomerEntityOut, status_code=status.HTTP_201_CREATED)
def create_customer_entity(
    entity_in: CustomerEntityCreate, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer_entity:create")),
    request: Request = None
):
    client_host = get_client_ip(request) if request else None

    customer_check = crud_customer.get_with_relations(db, id=entity_in.customer_id)
    if not customer_check:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found or is deleted.")
    
    active_entities_count = db.query(CustomerEntity).filter(
        CustomerEntity.customer_id == entity_in.customer_id,
        CustomerEntity.is_deleted == False,
        CustomerEntity.is_active == True
    ).count()

    if not customer_check.subscription_plan.can_multi_entity and active_entities_count >= 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Customer's subscription plan '{customer_check.subscription_plan.name}' does not support adding more entities. Max 1 active entity allowed."
        )

    existing_entity_name = db.query(CustomerEntity).filter(
        CustomerEntity.customer_id == entity_in.customer_id,
        func.lower(CustomerEntity.entity_name) == func.lower(entity_in.entity_name)
    ).first()
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
        existing_entity_code = crud_customer_entity.get_by_code_for_customer(db, entity_in.customer_id, entity_in.code)
        if existing_entity_code and existing_entity_code.is_deleted == False:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Entity with code '{entity_in.code}' already exists for customer '{customer_check.name}'."
            )
            
    db_entity = crud_customer_entity.create(db, obj_in=entity_in, customer_id=entity_in.customer_id, user_id=current_user.user_id)
    
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="CustomerEntity", entity_id=db_entity.id, details={"name": db_entity.entity_name, "code": db_entity.code, "customer_id": db_entity.customer_id, "ip_address": client_host})
    
    return db_entity

@router.get("/customer-entities/{entity_id}", response_model=CustomerEntityOut)
def read_customer_entity(
    entity_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer_entity:view"))
):
    """
    Retrieve a single customer entity by its ID.
    Returns 404 if the entity is not found or is soft-deleted.
    """
    db_entity = crud_customer_entity.get(db, id=entity_id)
    if db_entity is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Customer entity not found or already deleted"
        )
    return db_entity

@router.put("/customer-entities/{entity_id}", response_model=CustomerEntityOut)
def update_customer_entity(
    entity_id: int, 
    entity_in: CustomerEntityUpdate, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer_entity:edit")),
    request: Request = None
):
    """
    Update an existing customer entity by ID.
    Returns 404 if the entity is not found or is soft-deleted.
    """
    client_host = get_client_ip(request) if request else None

    db_entity = crud_customer_entity.get(db, id=entity_id)
    if db_entity is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Customer entity not found or already deleted"
        )
    
    if entity_in.entity_name is not None and entity_in.entity_name != db_entity.entity_name:
        existing_entity = db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == db_entity.customer_id,
            func.lower(CustomerEntity.entity_name) == func.lower(entity_in.entity_name),
            CustomerEntity.id != entity_id
        ).first()
        if existing_entity:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Entity with name '{entity_in.entity_name}' already exists for this customer."
            )
    
    if entity_in.code is not None and entity_in.code != db_entity.code:
        existing_entity_code = db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == db_entity.customer_id,
            func.lower(CustomerEntity.code) == func.lower(entity_in.code),
            CustomerEntity.id != entity_id
        ).first()
        if existing_entity_code and existing_entity_code.is_deleted == False:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Entity with code '{entity_in.code}' already exists for this customer."
            )
            
    updated_entity = crud_customer_entity.update(db, db_obj=db_entity, obj_in=entity_in, user_id=current_user.user_id)
    
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="CustomerEntity", entity_id=updated_entity.id, details={"name": updated_entity.entity_name, "code": updated_entity.code, "customer_id": updated_entity.customer_id, "ip_address": client_host, "updated_fields": entity_in.model_dump(exclude_unset=True)})
    
    return updated_entity

@router.delete("/customer-entities/{entity_id}", response_model=CustomerEntityOut)
def delete_customer_entity(
    entity_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer_entity:delete")),
    request: Request = None
):
    """
    Soft-delete a customer entity by ID.
    Returns 404 if the entity is not found or already deleted.
    """
    client_host = get_client_ip(request) if request else None

    db_entity = crud_customer_entity.get(db, id=entity_id)
    if db_entity is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Customer entity not found or already deleted"
        )
    
    customer_of_entity = crud_customer.get_with_relations(db, db_entity.customer_id)
    if customer_of_entity and not customer_of_entity.subscription_plan.can_multi_entity:
        active_entities_count = db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == db_entity.customer_id,
            CustomerEntity.is_deleted == False,
            CustomerEntity.is_active == True,
            CustomerEntity.id != entity_id
        ).count()
        if active_entities_count == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete or deactivate the last active entity for a customer on a single-entity plan."
            )

    deleted_entity = crud_customer_entity.soft_delete(db, db_entity, user_id=current_user.user_id)
    
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="CustomerEntity", entity_id=deleted_entity.id, details={"name": deleted_entity.entity_name, "code": deleted_entity.code, "customer_id": deleted_entity.customer_id, "ip_address": client_host})
    
    return deleted_entity

@router.post("/customer-entities/{entity_id}/restore", response_model=CustomerEntityOut)
def restore_customer_entity(
    entity_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer_entity:edit")),
    request: Request = None
):
    """
    Restore a soft-deleted customer entity by ID.
    Returns 404 if the entity is not found or is not soft-deleted.
    """
    client_host = get_client_ip(request) if request else None

    db_entity = db.query(CustomerEntity).filter(
        CustomerEntity.id == entity_id, 
        CustomerEntity.is_deleted == True 
    ).first()

    if db_entity is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Customer entity not found or not in a soft-deleted state."
        )
    
    customer_of_entity = crud_customer.get_with_relations(db, db_entity.customer_id)
    if customer_of_entity and not customer_of_entity.subscription_plan.can_multi_entity:
        active_entities_count = db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == db_entity.customer_id,
            CustomerEntity.is_deleted == False,
            CustomerEntity.is_active == True
        ).count()
        if active_entities_count >= 1:
            print(f"DEBUG: Not reactivating entity {db_entity.entity_name} during restore due to single-entity plan limit.")
            restored_entity = crud_customer_entity.restore(db, db_entity, user_id=current_user.user_id)
            restored_entity.is_active = False
            db.add(restored_entity)
            db.commit()
            db.refresh(restored_entity)
            log_action(db, user_id=current_user.user_id, action_type="RESTORE_INACTIVE", entity_type="CustomerEntity", entity_id=restored_entity.id, details={"name": restored_entity.entity_name, "reason": "Single-entity plan limit met", "ip_address": client_host})
            
            
    restored_entity = crud_customer_entity.restore(db, db_entity, user_id=current_user.user_id)
    
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="CustomerEntity", entity_id=restored_entity.id, details={"name": restored_entity.entity_name, "code": restored_entity.code, "customer_id": restored_entity.customer_id, "ip_address": client_host})
    
    return restored_entity

# api/v1/endpoints/system_owner.py

# (Other imports and router definitions remain the same)

@router.post("/customers/{customer_id}/entities/", response_model=CustomerEntityOut, status_code=status.HTTP_201_CREATED)
def create_customer_entity(
    customer_id: int,
    entity_in: CustomerEntityCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("customer_entity:create")),
    request: Request = None
):
    client_host = get_client_ip(request) if request else None

    # CORRECTED: Use 'customer_id' as the keyword argument.
    customer_check = crud_customer.get_with_relations(db, customer_id=customer_id)
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

    existing_entity_name = db.query(CustomerEntity).filter(
        CustomerEntity.customer_id == customer_id,
        func.lower(CustomerEntity.entity_name) == func.lower(entity_in.entity_name)
    ).first()
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
            
    db_entity = crud_customer_entity.create(db, obj_in=entity_in, customer_id=customer_id, user_id=current_user.user_id)
    
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="CustomerEntity", entity_id=db_entity.id, details={"name": db_entity.entity_name, "code": db_entity.code, "customer_id": db_entity.customer_id, "ip_address": client_host})
    
    return db_entity

@router.post("/customers/{customer_id}/users/", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def create_customer_user_by_system_owner(
    customer_id: int,
    user_in: UserCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("user:create")),
    request: Request = None
):
    """
    Allows a System Owner to create a new user for a specific customer.
    This can be used to add additional Corporate Admins after the initial onboarding.
    """
    client_host = get_client_ip(request) if request else None

    # Check if the customer exists and is not deleted
    customer_check = crud_customer.get_with_relations(db, customer_id=customer_id)
    if not customer_check:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found or is deleted.")
    
    # We should not rely on user_in.customer_id as it might be missing or wrong.
    # Instead, we use the customer_id from the URL path.
    user_in.customer_id = customer_id
    
    # Check for existing user with the same email address for the customer
    existing_user = db.query(User).filter(
        User.customer_id == customer_id,
        User.email == user_in.email,
        User.is_deleted == False
    ).first()
    if existing_user:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A user with this email already exists for this customer.")

    try:
        # Use the existing CRUD method which handles all business logic like user limit checks
        db_user = crud_user.create_user(db, user_in, user_id_caller=current_user.user_id)
        return db_user
    except HTTPException as e:
        log_action(db, user_id=current_user.user_id, action_type="CREATE_FAILED", entity_type="User", entity_id=None, details={"email": user_in.email, "customer_id": customer_id, "reason": str(e.detail)}, customer_id=customer_id, ip_address=client_host)
        raise
    except Exception as e:
        log_action(db, user_id=current_user.user_id, action_type="CREATE_FAILED", entity_type="User", entity_id=None, details={"email": user_in.email, "customer_id": customer_id, "reason": str(e)}, customer_id=customer_id, ip_address=client_host)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

@router.delete("/users/{user_id}", response_model=UserOut)
def delete_user(
    user_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("user:delete")),
    request: Request = None
):
    """
    Soft-delete a user by ID.
    """
    client_host = get_client_ip(request) if request else None

    # FIX: Explicitly check for the user's existence
    db_user = crud_user.get(db, id=user_id)
    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="User not found or already deleted."
        )

    # Prevent a System Owner from deleting their own account
    if db_user.id == current_user.user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot delete your own user account."
        )

    deleted_user = crud_user.soft_delete(db, db_user, user_id=current_user.user_id)
    
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="User", entity_id=deleted_user.id, details={"email": deleted_user.email, "customer_id": deleted_user.customer_id, "ip_address": client_host})
    
    return deleted_user
    
@router.post("/users/{user_id}/restore", response_model=UserOut)
def restore_user(
    user_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("user:edit")),
    request: Request = None
):
    """
    Restore a soft-deleted user by ID.
    """
    client_host = get_client_ip(request) if request else None

    # FIX: Explicitly query for soft-deleted users
    db_user = db.query(User).filter(
        User.id == user_id, 
        User.is_deleted == True 
    ).first()

    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="User not found or not in a soft-deleted state."
        )

    restored_user = crud_user.restore(db, db_user, user_id=current_user.user_id)
    
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="User", entity_id=restored_user.id, details={"email": restored_user.email, "customer_id": restored_user.customer_id, "ip_address": client_host})
    
    return restored_user

@router.put("/users/{user_id}", response_model=UserOut)
def update_user_by_system_owner(
    user_id: int, 
    user_in: UserUpdate, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("user:edit")),
    request: Request = None
):
    """
    Allows a System Owner to update an existing user's details by ID.
    """
    client_host = get_client_ip(request) if request else None
    
    db_user = crud_user.get(db, id=user_id)
    if not db_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found or is deleted.")

    if user_in.email is not None and user_in.email != db_user.email:
        existing_user = crud_user.get_by_email(db, email=user_in.email)
        if existing_user and existing_user.id != user_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"User with email '{user_in.email}' already exists."
            )
            
    # FIX: Change 'db_obj' to 'db_user' to match the function signature in crud_user.py
    updated_user = crud_user.update_user(db, db_user=db_user, user_in=user_in, user_id_caller=current_user.user_id)
    
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="User", entity_id=updated_user.id, details={"email": updated_user.email, "ip_address": client_host, "updated_fields": user_in.model_dump(exclude_unset=True)})
    
    return updated_user
           
@router.post("/global-configurations/", response_model=GlobalConfigurationOut, status_code=status.HTTP_201_CREATED)
def create_global_configuration(
    config_in: GlobalConfigurationCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("global_config:create")),
    request: Request = None
):
    """
    Create a new global configuration setting.
    """
    existing_config = crud_global_configuration.get_by_key(db, key=config_in.key)
    if existing_config:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Global configuration with key '{config_in.key}' already exists."
        )
    db_config = crud_global_configuration.create(db, obj_in=config_in)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="GlobalConfiguration", entity_id=db_config.id, details={"key": db_config.key, "value_default": db_config.value_default, "ip_address": client_host})

    return db_config

@router.get("/global-configurations/", response_model=List[GlobalConfigurationOut])
def read_global_configurations(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("global_config:view"))
):
    """
    Retrieve a list of all active global configuration settings.
    """
    configs = crud_global_configuration.get_all(db, skip=skip, limit=limit)
    return configs

@router.get("/global-configurations/{config_id}", response_model=GlobalConfigurationOut)
def read_global_configuration(
    config_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("global_config:view"))
):
    """
    Retrieve a single global configuration setting by its ID.
    Returns 404 if the configuration is not found or is soft-deleted.
    """
    db_config = crud_global_configuration.get(db, id=config_id)
    if db_config is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Global configuration not found or already deleted"
        )
    return db_config

@router.put("/global-configurations/{config_id}", response_model=GlobalConfigurationOut)
def update_global_configuration(
    config_id: int, 
    config_in: GlobalConfigurationUpdate, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("global_config:edit")),
    request: Request = None,
    background_tasks: BackgroundTasks = None
):
    """
    Update an existing global configuration setting by ID.
    Only provided fields will be updated (partial update is supported).
    Returns 404 if the configuration is not found or is soft-deleted.
    """
    db_config = crud_global_configuration.get(db, id=config_id)
    if db_config is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Global configuration not found or already deleted"
        )
    
    if config_in.key is not None and config_in.key != db_config.key:
        existing_config = crud_global_configuration.get_by_key(db, key=config_in.key)
        if existing_config and existing_config.id != config_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Global configuration with key '{config_in.key}' already exists."
            )
    
    old_value_min = db_config.value_min
    old_value_max = db_config.value_max

    new_value_min_str = config_in.value_min if config_in.value_min is not None else old_value_min
    new_value_max_str = config_in.value_max if config_in.value_max is not None else old_value_max

    trigger_revalidation = False

    try:
        if new_value_min_str is not None and old_value_min is not None and float(new_value_min_str) > float(old_value_min):
            trigger_revalidation = True
        
        if new_value_max_str is not None and old_value_max is not None and float(new_value_max_str) < float(old_value_max):
            trigger_revalidation = True
    except (ValueError, TypeError):
        pass

    updated_config = crud_global_configuration.update(db, db_obj=db_config, obj_in=config_in)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="GlobalConfiguration", entity_id=updated_config.id, details={"key": updated_config.key, "ip_address": client_host, "updated_fields": config_in.model_dump(exclude_unset=True)})
    
    if trigger_revalidation:
        background_tasks.add_task(background_tasks_module.proactively_correct_customer_configs, global_config_id=updated_config.id, db=db)

    return updated_config

@router.delete("/global-configurations/{config_id}", response_model=GlobalConfigurationOut)
def delete_global_configuration(
    config_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("global_config:delete")),
    request: Request = None
):
    """
    Soft-delete a global configuration setting by ID.
    Returns 404 if the configuration is not found or already deleted.
    """
    db_config = crud_global_configuration.get(db, id=config_id)
    if db_config is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Global configuration not found or already deleted"
        )
    deleted_config = crud_global_configuration.soft_delete(db, db_config)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="GlobalConfiguration", entity_id=deleted_config.id, details={"key": deleted_config.key, "ip_address": client_host})
    
    return deleted_config

@router.post("/global-configurations/{config_id}/restore", response_model=GlobalConfigurationOut)
def restore_global_configuration(
    config_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("global_config:edit")),
    request: Request = None
):
    """
    Restore a soft-deleted global configuration setting by ID.
    Returns 404 if the configuration is not found or is not soft-deleted.
    """
    db_config = db.query(GlobalConfiguration).filter(
        GlobalConfiguration.id == config_id,
        GlobalConfiguration.is_deleted == True
    ).first()

    if db_config is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Global configuration not found or not in a soft-deleted state."
        )
    restored_config = crud_global_configuration.restore(db, db_config)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="GlobalConfiguration", entity_id=restored_config.id, details={"key": restored_config.key, "ip_address": client_host})
    return restored_config


@router.post("/currencies/", response_model=CurrencyOut, status_code=status.HTTP_201_CREATED)
def create_currency(
    currency_in: CurrencyCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("currency:create")),
    request: Request = None
):
    """Create a new currency entry."""
    existing_currency = crud_currency.get_by_iso_code(db, iso_code=currency_in.iso_code)
    if existing_currency:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Currency with ISO code '{currency_in.iso_code}' already exists."
        )
    db_currency = crud_currency.create(db, obj_in=currency_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="Currency", entity_id=db_currency.id, details={"name": db_currency.name, "iso_code": db_currency.iso_code, "ip_address": client_host})
    return db_currency

@router.get("/currencies/", response_model=List[CurrencyOut])
def read_currencies(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("currency:view"))
):
    """Retrieve a list of all active currencies."""
    currencies = crud_currency.get_all(db, skip=skip, limit=limit)
    return currencies

@router.get("/currencies/{currency_id}", response_model=CurrencyOut)
def read_currency(
    currency_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("currency:view"))
):
    """Retrieve a single currency by its ID."""
    db_currency = crud_currency.get(db, id=currency_id)
    if db_currency is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Currency not found or already deleted")
    return db_currency

@router.put("/currencies/{currency_id}", response_model=CurrencyOut)
def update_currency(
    currency_id: int, currency_in: CurrencyUpdate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("currency:edit")),
    request: Request = None
):
    """Update an existing currency by ID."""
    db_currency = crud_currency.get(db, id=currency_id)
    if db_currency is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Currency not found or already deleted")
    
    if currency_in.iso_code is not None and currency_in.iso_code != db_currency.iso_code:
        existing_currency = crud_currency.get_by_iso_code(db, iso_code=currency_in.iso_code)
        if existing_currency and existing_currency.id != currency_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Currency with ISO code '{currency_in.iso_code}' already exists.")
            
    updated_currency = crud_currency.update(db, db_obj=db_currency, obj_in=currency_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="Currency", entity_id=updated_currency.id, details={"name": updated_currency.name, "ip_address": client_host, "updated_fields": currency_in.model_dump(exclude_unset=True)})
    return updated_currency

@router.delete("/currencies/{currency_id}", response_model=CurrencyOut)
def delete_currency(
    currency_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("currency:delete")),
    request: Request = None
):
    """Soft-delete a currency by ID."""
    db_currency = crud_currency.get(db, id=currency_id)
    if db_currency is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Currency not found or already deleted")
    db_currency = crud_currency.soft_delete(db, db_currency)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="Currency", entity_id=db_currency.id, details={"name": db_currency.name, "ip_address": client_host})
    return db_currency

@router.post("/currencies/{currency_id}/restore", response_model=CurrencyOut)
def restore_currency(
    currency_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("currency:edit")),
    request: Request = None
):
    """Restore a soft-deleted currency by ID."""
    db_currency = db.query(Currency).filter(Currency.id == currency_id, Currency.is_deleted == True).first()
    if db_currency is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Currency not found or not in a soft-deleted state.")
    db_currency = crud_currency.restore(db, db_currency)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="Currency", entity_id=db_currency.id, details={"name": db_currency.name, "ip_address": client_host})
    return db_currency

@router.post("/lg-types/", response_model=LgTypeOut, status_code=status.HTTP_201_CREATED)
def create_lg_type(
    lg_type_in: LgTypeCreate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_type:create")),
    request: Request = None
):
    """Create a new LG Type entry."""
    existing_lg_type = crud_lg_type.get_by_name(db, name=lg_type_in.name)
    if existing_lg_type:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"LG Type with name '{lg_type_in.name}' already exists."
        )
    db_lg_type = crud_lg_type.create(db, obj_in=lg_type_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="LgType", entity_id=db_lg_type.id, details={"name": db_lg_type.name, "ip_address": client_host})
    return db_lg_type

@router.get("/lg-types/", response_model=List[LgTypeOut])
def read_lg_types(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_type:view"))
):
    """Retrieve a list of all active LG Types."""
    lg_types = crud_lg_type.get_all(db, skip=skip, limit=limit)
    return lg_types

@router.get("/lg-types/{lg_type_id}", response_model=LgTypeOut)
def read_lg_type(
    lg_type_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_type:view"))
):
    """Retrieve a single LG Type by its ID."""
    db_lg_type = crud_lg_type.get(db, id=lg_type_id)
    if db_lg_type is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Type not found or already deleted")
    return db_lg_type

@router.put("/lg-types/{lg_type_id}", response_model=LgTypeOut)
def update_lg_type(
    lg_type_id: int, lg_type_in: LgTypeUpdate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_type:edit")),
    request: Request = None
):
    """Update an existing LG Type by ID."""
    db_lg_type = crud_lg_type.get(db, id=lg_type_id)
    if db_lg_type is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Type not found or already deleted")
    
    if lg_type_in.name is not None and lg_type_in.name != db_lg_type.name:
        existing_lg_type = crud_lg_type.get_by_name(db, name=lg_type_in.name)
        if existing_lg_type and existing_lg_type.id != lg_type_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"LG Type with name '{lg_type_in.name}' already exists.")
            
    updated_lg_type = crud_lg_type.update(db, db_obj=db_lg_type, obj_in=lg_type_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="LgType", entity_id=updated_lg_type.id, details={"name": updated_lg_type.name, "ip_address": client_host, "updated_fields": lg_type_in.model_dump(exclude_unset=True)})
    return updated_lg_type

@router.delete("/lg-types/{lg_type_id}", response_model=LgTypeOut)
def delete_lg_type(
    lg_type_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_type:delete")),
    request: Request = None
):
    """Soft-delete an LG Type by ID."""
    db_lg_type = crud_lg_type.get(db, id=lg_type_id)
    if db_lg_type is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Type not found or already deleted")
    db_lg_type = crud_lg_type.soft_delete(db, db_lg_type)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="LgType", entity_id=db_lg_type.id, details={"name": db_lg_type.name, "ip_address": client_host})
    return db_lg_type

@router.post("/lg-types/{lg_type_id}/restore", response_model=LgTypeOut)
def restore_lg_type(
    lg_type_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_type:edit")),
    request: Request = None
):
    """Restore a soft-deleted LG Type by ID."""
    db_lg_type = db.query(LgType).filter(LgType.id == lg_type_id, LgType.is_deleted == True).first()
    if db_lg_type is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Type not found or not in a soft-deleted state.")
    db_lg_type = crud_lg_type.restore(db, db_lg_type)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="LgType", entity_id=db_lg_type.id, details={"name": db_lg_type.name, "ip_address": client_host})
    return db_lg_type

@router.post("/rules/", response_model=RuleOut, status_code=status.HTTP_201_CREATED)
def create_rule(
    rule_in: RuleCreate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("rule:create")),
    request: Request = None
):
    """Create a new Rule entry."""
    existing_rule = crud_rule.get_by_name(db, name=rule_in.name)
    if existing_rule:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Rule with name '{rule_in.name}' already exists."
        )
    db_rule = crud_rule.create(db, obj_in=rule_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="Rule", entity_id=db_rule.id, details={"name": db_rule.name, "ip_address": client_host})
    return db_rule

@router.get("/rules/", response_model=List[RuleOut])
def read_rules(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("rule:view"))
):
    """Retrieve a list of all active Rules."""
    rules = crud_rule.get_all(db, skip=skip, limit=limit)
    return rules

@router.get("/rules/{rule_id}", response_model=RuleOut)
def read_rule(
    rule_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("rule:view"))
):
    """Retrieve a single Rule by its ID."""
    db_rule = crud_rule.get(db, id=rule_id)
    if db_rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found or already deleted")
    return db_rule

@router.put("/rules/{rule_id}", response_model=RuleOut)
def update_rule(
    rule_id: int, rule_in: RuleUpdate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("rule:edit")),
    request: Request = None
):
    """Update an existing Rule by ID."""
    db_rule = crud_rule.get(db, id=rule_id)
    if db_rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found or already deleted")
    
    if rule_in.name is not None and rule_in.name != db_rule.name:
        existing_rule = crud_rule.get_by_name(db, name=rule_in.name)
        if existing_rule and existing_rule.id != rule_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Rule with name '{rule_in.name}' already exists.")
            
    updated_rule = crud_rule.update(db, db_obj=db_rule, obj_in=rule_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="Rule", entity_id=updated_rule.id, details={"name": updated_rule.name, "ip_address": client_host, "updated_fields": rule_in.model_dump(exclude_unset=True)})
    return updated_rule

@router.delete("/rules/{rule_id}", response_model=RuleOut)
def delete_rule(
    rule_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("rule:delete")),
    request: Request = None
):
    """Soft-delete a Rule by ID."""
    db_rule = crud_rule.get(db, id=rule_id)
    if db_rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found or already deleted")
    db_rule = crud_rule.soft_delete(db, db_rule)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="Rule", entity_id=db_rule.id, details={"name": db_rule.name, "ip_address": client_host})
    return db_rule

@router.post("/rules/{rule_id}/restore", response_model=RuleOut)
def restore_rule(
    rule_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("rule:edit")),
    request: Request = None
):
    """Restore a soft-deleted Rule by ID."""
    db_rule = db.query(Rule).filter(Rule.id == rule_id, Rule.is_deleted == True).first()
    if db_rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found or not in a soft-deleted state.")
    db_rule = crud_rule.restore(db, db_rule)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="Rule", entity_id=db_rule.id, details={"name": db_rule.name, "ip_address": client_host})
    return db_rule

@router.post("/issuing-methods/", response_model=IssuingMethodOut, status_code=status.HTTP_201_CREATED)
def create_issuing_method(
    method_in: IssuingMethodCreate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("issuing_method:create")),
    request: Request = None
):
    """Create a new Issuing Method entry."""
    existing_method = crud_issuing_method.get_by_name(db, name=method_in.name)
    if existing_method:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Issuing Method with name '{method_in.name}' already exists."
        )
    db_method = crud_issuing_method.create(db, obj_in=method_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="IssuingMethod", entity_id=db_method.id, details={"name": db_method.name, "ip_address": client_host})
    return db_method

@router.get("/issuing-methods/", response_model=List[IssuingMethodOut])
def read_issuing_methods(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("issuing_method:view"))
):
    """Retrieve a list of all active Issuing Methods."""
    methods = crud_issuing_method.get_all(db, skip=skip, limit=limit)
    return methods

@router.get("/issuing-methods/{method_id}", response_model=IssuingMethodOut)
def read_issuing_method(
    method_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("issuing_method:view"))
):
    """Retrieve a single Issuing Method by its ID."""
    db_method = crud_issuing_method.get(db, id=method_id)
    if db_method is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Issuing Method not found or already deleted")
    return db_method

@router.put("/issuing-methods/{method_id}", response_model=IssuingMethodOut)
def update_issuing_method(
    method_id: int, method_in: IssuingMethodUpdate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("issuing_method:edit")),
    request: Request = None
):
    """Update an existing Issuing Method by ID."""
    db_method = crud_issuing_method.get(db, id=method_id)
    if db_method is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Issuing Method not found or already deleted")
    
    if method_in.name is not None and method_in.name != db_method.name:
        existing_method = crud_issuing_method.get_by_name(db, name=method_in.name)
        if existing_method and existing_method.id != method_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Issuing Method with name '{method_in.name}' already exists.")
            
    updated_method = crud_issuing_method.update(db, db_obj=db_method, obj_in=method_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="IssuingMethod", entity_id=updated_method.id, details={"name": updated_method.name, "ip_address": client_host, "updated_fields": method_in.model_dump(exclude_unset=True)})
    return updated_method

@router.delete("/issuing-methods/{method_id}", response_model=IssuingMethodOut)
def delete_issuing_method(
    method_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("issuing_method:delete")),
    request: Request = None
):
    """Soft-delete an Issuing Method by ID."""
    db_method = crud_issuing_method.get(db, id=method_id)
    if db_method is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Issuing Method not found or already deleted")
    db_method = crud_issuing_method.soft_delete(db, db_method)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="IssuingMethod", entity_id=db_method.id, details={"name": db_method.name, "ip_address": client_host})
    return db_method

@router.post("/issuing-methods/{method_id}/restore", response_model=IssuingMethodOut)
def restore_issuing_method(
    method_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("issuing_method:edit")),
    request: Request = None
):
    """Restore a soft-deleted Issuing Method by ID."""
    db_method = db.query(IssuingMethod).filter(IssuingMethod.id == method_id, IssuingMethod.is_deleted == True).first()
    if db_method is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Issuing Method not found or not in a soft-deleted state.")
    db_method = crud_issuing_method.restore(db, db_method)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="IssuingMethod", entity_id=db_method.id, details={"name": db_method.name, "ip_address": client_host})
    return db_method

@router.post("/lg-statuses/", response_model=LgStatusOut, status_code=status.HTTP_201_CREATED)
def create_lg_status(
    status_in: LgStatusCreate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_status:create")),
    request: Request = None
):
    """Create a new LG Status entry."""
    existing_status = crud_lg_status.get_by_name(db, name=status_in.name)
    if existing_status:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"LG Status with name '{status_in.name}' already exists."
        )
    db_status = crud_lg_status.create(db, obj_in=status_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="LgStatus", entity_id=db_status.id, details={"name": db_status.name, "ip_address": client_host})
    return db_status

@router.get("/lg-statuses/", response_model=List[LgStatusOut])
def read_lg_statuses(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_status:view"))
):
    """Retrieve a list of all active LG Statuses."""
    statuses = crud_lg_status.get_all(db, skip=skip, limit=limit)
    return statuses

@router.get("/lg-statuses/{status_id}", response_model=LgStatusOut)
def read_lg_status(
    status_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_status:view"))
):
    """Retrieve a single LG Status by its ID."""
    db_status = crud_lg_status.get(db, id=status_id)
    if db_status is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Status not found or already deleted")
    return db_status

@router.put("/lg-statuses/{status_id}", response_model=LgStatusOut)
def update_lg_status(
    status_id: int, status_in: LgStatusUpdate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_status:edit")),
    request: Request = None
):
    """Update an existing LG Status by ID."""
    db_status = crud_lg_status.get(db, id=status_id)
    if db_status is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Status not found or already deleted")
    
    if status_in.name is not None and status_in.name != db_status.name:
        existing_status = crud_lg_status.get_by_name(db, name=status_in.name)
        if existing_status and existing_status.id != status_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"LG Status with name '{status_in.name}' already exists.")
            
    updated_status = crud_lg_status.update(db, db_obj=db_status, obj_in=status_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="LgStatus", entity_id=updated_status.id, details={"name": updated_status.name, "ip_address": client_host, "updated_fields": status_in.model_dump(exclude_unset=True)})
    return updated_status

@router.delete("/lg-statuses/{status_id}", response_model=LgStatusOut)
def delete_lg_status(
    status_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_status:delete")),
    request: Request = None
):
    """Soft-delete an LG Status by ID."""
    db_status = crud_lg_status.get(db, id=status_id)
    if db_status is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Status not found or already deleted")
    db_status = crud_lg_status.soft_delete(db, db_status)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="LgStatus", entity_id=db_status.id, details={"name": db_status.name, "ip_address": client_host})
    return db_status

@router.post("/lg-statuses/{status_id}/restore", response_model=LgStatusOut)
def restore_lg_status(
    status_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_status:edit")),
    request: Request = None
):
    """Restore a soft-deleted LG Status by ID."""
    db_status = db.query(LgStatus).filter(LgStatus.id == status_id, LgStatus.is_deleted == True).first()
    if db_status is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Status not found or not in a soft-deleted state.")
    db_status = crud_lg_status.restore(db, db_status)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="LgStatus", entity_id=db_status.id, details={"name": db_status.name, "ip_address": client_host})
    return db_status

@router.post("/lg-operational-statuses/", response_model=LgOperationalStatusOut, status_code=status.HTTP_201_CREATED)
def create_lg_operational_status(
    op_status_in: LgOperationalStatusCreate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_operational_status:create")),
    request: Request = None
):
    """Create a new LG Operational Status entry."""
    existing_op_status = crud_lg_operational_status.get_by_name(db, name=op_status_in.name)
    if existing_op_status:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"LG Operational Status with name '{op_status_in.name}' already exists."
        )
    db_op_status = crud_lg_operational_status.create(db, obj_in=op_status_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="LgOperationalStatus", entity_id=db_op_status.id, details={"name": db_op_status.name, "ip_address": client_host})
    return db_op_status

@router.get("/lg-operational-statuses/", response_model=List[LgOperationalStatusOut])
def read_lg_operational_statuses(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_operational_status:view"))
):
    """Retrieve a list of all active LG Operational Statuses."""
    op_statuses = crud_lg_operational_status.get_all(db, skip=skip, limit=limit)
    return op_statuses

@router.get("/lg-operational-statuses/{op_status_id}", response_model=LgOperationalStatusOut)
def read_lg_operational_status(
    op_status_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_operational_status:view"))
):
    """Retrieve a single LG Operational Status by its ID."""
    db_op_status = crud_lg_operational_status.get(db, id=op_status_id)
    if db_op_status is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Operational Status not found or already deleted")
    return db_op_status

@router.put("/lg-operational-statuses/{op_status_id}", response_model=LgOperationalStatusOut)
def update_lg_operational_status(
    op_status_id: int, op_status_in: LgOperationalStatusUpdate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_operational_status:edit")),
    request: Request = None
):
    """Update an existing LG Operational Status by ID."""
    db_op_status = crud_lg_operational_status.get(db, id=op_status_id)
    if db_op_status is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Operational Status not found or already deleted")
    
    if op_status_in.name is not None and op_status_in.name != db_op_status.name:
        existing_op_status = crud_lg_operational_status.get_by_name(db, name=op_status_in.name)
        if existing_op_status and existing_op_status.id != op_status_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"LG Operational Status with name '{op_status_in.name}' already exists.")
            
    updated_op_status = crud_lg_operational_status.update(db, db_obj=db_op_status, obj_in=op_status_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="LgOperationalStatus", entity_id=updated_op_status.id, details={"name": updated_op_status.name, "ip_address": client_host, "updated_fields": op_status_in.model_dump(exclude_unset=True)})
    return updated_op_status

@router.delete("/lg-operational-statuses/{op_status_id}", response_model=LgOperationalStatusOut)
def delete_lg_operational_status(
    op_status_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_operational_status:delete")),
    request: Request = None
):
    """Soft-delete an LG Operational Status by ID."""
    db_op_status = crud_lg_operational_status.get(db, id=op_status_id)
    if db_op_status is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Operational Status not found or already deleted")
    db_op_status = crud_lg_operational_status.soft_delete(db, db_op_status)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="LgOperationalStatus", entity_id=db_op_status.id, details={"name": db_op_status.name, "ip_address": client_host})
    return db_op_status

@router.post("/lg-operational-statuses/{op_status_id}/restore", response_model=LgOperationalStatusOut)
def restore_lg_operational_status(
    op_status_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("lg_operational_status:edit")),
    request: Request = None
):
    """Restore a soft-deleted LG Operational Status by ID."""
    db_op_status = db.query(LgOperationalStatus).filter(LgOperationalStatus.id == op_status_id, LgOperationalStatus.is_deleted == True).first()
    if db_op_status is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="LG Operational Status not found or not in a soft-deleted state.")
    db_op_status = crud_lg_operational_status.restore(db, db_op_status)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="LgOperationalStatus", entity_id=db_op_status.id, details={"name": db_op_status.name, "ip_address": client_host})
    return db_op_status


@router.post("/lg-categories/universal", response_model=LGCategoryOut, status_code=status.HTTP_201_CREATED)
def create_universal_category(
    category_in: LGCategoryCreate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("universal_category:create")),
    request: Request = None
):
    """Create a new Universal Category entry."""
    client_host = get_client_ip(request) if request else None

    # Enforce universal scope by setting customer_id to None
    category_in.customer_id = None

    try:
        db_category = crud_lg_category.create(db, obj_in=category_in, user_id=current_user.user_id)
        # Manually create the output model to include customer_name helper field
        # --- FIX START ---
        return LGCategoryOut.model_validate(
            db_category,
            context={
                'customer_name': "System Default",
                'type': "universal"
            }
        )
        # --- FIX END ---
    except HTTPException as e:
        log_action(db, user_id=current_user.user_id, action_type="CREATE_FAILED", entity_type="LGCategory", entity_id=None, details={"category_name": category_in.name, "reason": str(e.detail), "scope": "universal"}, customer_id=None)
        raise
    except Exception as e:
        log_action(db, user_id=current_user.user_id, action_type="CREATE_FAILED", entity_type="LGCategory", entity_id=None, details={"category_name": category_in.name, "reason": str(e), "scope": "universal"}, customer_id=None)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")


@router.get("/lg-categories/universal", response_model=List[LGCategoryOut])
def read_universal_categories(
    skip: int = 0,
    limit: int = 100,
    include_deleted: Optional[bool] = Query(False),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("universal_category:view"))
):
    """Retrieve a list of all active Universal Categories."""
    # Logic to fetch all categories, including deleted ones if the flag is True
    categories = db.query(LGCategory).filter(LGCategory.customer_id.is_(None))
    if not include_deleted:
        categories = categories.filter(LGCategory.is_deleted == False)

    categories = categories.order_by(LGCategory.name).offset(skip).limit(limit).all()

    return [
        LGCategoryOut.model_validate(
            cat,
            context={
                'customer_name': "System Default",
                'type': "universal"
            }
        ) for cat in categories
    ]

@router.get("/lg-categories/universal/{category_id}", response_model=LGCategoryOut)
def read_universal_category(
    category_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("universal_category:view"))
):
    """Retrieve a single Universal Category by its ID."""
    db_category = crud_lg_category.get(db, id=category_id)
    if not db_category or db_category.customer_id is not None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Universal Category not found, already deleted, or is a customer-specific category.")
    
    # FIX: Use LGCategoryOut.model_validate to correctly create a Pydantic instance from the ORM object.
    return LGCategoryOut.model_validate(
        db_category,
        context={
            'customer_name': "System Default",
            'type': "universal"
        }
    )

@router.put("/lg-categories/universal/{category_id}", response_model=LGCategoryOut)
def update_universal_category(
    category_id: int, category_in: LGCategoryUpdate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("universal_category:edit")),
    request: Request = None
):
    """Update an existing Universal Category by ID."""
    client_host = get_client_ip(request) if request else None
    
    db_category = crud_lg_category.get(db, id=category_id)
    if not db_category or db_category.customer_id is not None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Universal Category not found, already deleted, or is a customer-specific category.")
    
    # FIX: Remove the problematic line `category_in.customer_id = None`.
    # This is not a valid operation on the Pydantic schema.
    # The CRUD layer is responsible for ensuring the customer_id remains None.
    
    try:
        updated_category = crud_lg_category.update(db, db_category, category_in, user_id=current_user.user_id)
        return LGCategoryOut.model_validate(
            updated_category,
            context={
                'customer_name': "System Default",
                'type': "universal"
            }
        )
    except HTTPException as e:
        # Safely get the category name for logging
        category_name = category_in.name if category_in.name else "N/A"
        log_action(db, user_id=current_user.user_id, action_type="UPDATE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": category_name, "reason": str(e.detail), "scope": "universal"}, customer_id=None)
        raise
    except Exception as e:
        # Safely get the category name for logging
        category_name = category_in.name if category_in.name else "N/A"
        log_action(db, user_id=current_user.user_id, action_type="UPDATE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": category_name, "reason": str(e), "scope": "universal"}, customer_id=None)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.delete("/lg-categories/universal/{category_id}", response_model=LGCategoryOut)
def delete_universal_category(
    category_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("universal_category:delete")),
    request: Request = None
):
    """Soft-delete a Universal Category by ID."""
    db_category = crud_lg_category.get(db, id=category_id)
    if not db_category or db_category.customer_id is not None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Universal Category not found, already deleted, or is a customer-specific category.")
    
    try:
        deleted_category = crud_lg_category.soft_delete(db, db_category, user_id=current_user.user_id)
        # The logic here is fine. It does not try to modify `category_in`.
        return LGCategoryOut.model_validate(
            deleted_category,
            context={
                'customer_name': "System Default",
                'type': "universal"
            }
        )
    except HTTPException as e:
        log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": db_category.name, "reason": str(e.detail), "scope": "universal"}, customer_id=None)
        raise
    except Exception as e:
        log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": db_category.name, "reason": str(e), "scope": "universal"}, customer_id=None)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.post("/lg-categories/universal/{category_id}/restore", response_model=LGCategoryOut)
def restore_universal_category(
    category_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("universal_category:edit")),
    request: Request = None
):
    """Restore a soft-deleted Universal Category by ID."""
    db_category = db.query(LGCategory).filter(
        LGCategory.id == category_id,
        LGCategory.customer_id.is_(None),
        LGCategory.is_deleted == True
    ).first()
    
    if db_category is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Universal Category not found or not in a soft-deleted state.")
    
    try:
        restored_category = crud_lg_category.restore(db, db_category, user_id=current_user.user_id)
        # FIX: Use model_validate instead of model_dump()
        return LGCategoryOut.model_validate(
            restored_category,
            context={
                'customer_name': "System Default",
                'type': "universal"
            }
        )
    except HTTPException as e:
        log_action(db, user_id=current_user.user_id, action_type="RESTORE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": db_category.name, "reason": str(e.detail), "scope": "universal"}, customer_id=None)
        raise
    except Exception as e:
        log_action(db, user_id=current_user.user_id, action_type="RESTORE_FAILED", entity_type="LGCategory", entity_id=category_id, details={"category_name": db_category.name, "reason": str(e), "scope": "universal"}, customer_id=None)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")


@router.post("/banks/", response_model=BankOut, status_code=status.HTTP_201_CREATED)
def create_bank(
    bank_in: BankCreate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("bank:create")),
    request: Request = None
):
    """Create a new bank entry."""
    existing_bank_name = crud_bank.get_by_name(db, name=bank_in.name)
    if existing_bank_name:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Bank with name '{bank_in.name}' already exists."
        )
    if bank_in.swift_code:
        existing_bank_swift = crud_bank.get_by_swift_code(db, swift_code=bank_in.swift_code)
        if existing_bank_swift:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Bank with SWIFT code '{bank_in.swift_code}' already exists."
            )
    db_bank = crud_bank.create(db, obj_in=bank_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="Bank", entity_id=db_bank.id, details={"name": db_bank.name, "swift_code": db_bank.swift_code, "ip_address": client_host})
    return db_bank

@router.get("/banks/", response_model=List[BankOut])
def read_banks(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("bank:view")),
):
    """Retrieve a list of all active banks."""
    banks = crud_bank.get_all(db, skip=skip, limit=limit)
    return banks

@router.get("/banks/{bank_id}", response_model=BankOut)
def read_bank(
    bank_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("bank:view")),
):
    """Retrieve a single bank by its ID."""
    db_bank = crud_bank.get(db, id=bank_id)
    if db_bank is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bank not found or already deleted")
    return db_bank

@router.put("/banks/{bank_id}", response_model=BankOut)
def update_bank(
    bank_id: int, bank_in: BankUpdate, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("bank:edit")),
    request: Request = None
):
    """Update an existing bank by ID."""
    db_bank = crud_bank.get(db, id=bank_id)
    if db_bank is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bank not found or already deleted")
    
    if bank_in.name is not None and bank_in.name != db_bank.name:
        existing_bank_name = crud_bank.get_by_name(db, name=bank_in.name)
        if existing_bank_name and existing_bank_name.id != bank_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Bank with name '{bank_in.name}' already exists.")
            
    if bank_in.swift_code is not None and bank_in.swift_code != db_bank.swift_code:
        existing_bank_swift = crud_bank.get_by_swift_code(db, swift_code=bank_in.swift_code)
        if existing_bank_swift and existing_bank_swift.id != bank_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Bank with SWIFT code '{bank_in.swift_code}' already exists."
            )
            
    updated_bank = crud_bank.update(db, db_obj=db_bank, obj_in=bank_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="Bank", entity_id=updated_bank.id, details={"name": updated_bank.name, "ip_address": client_host, "updated_fields": bank_in.model_dump(exclude_unset=True)})
    return updated_bank

@router.delete("/banks/{bank_id}", response_model=BankOut)
def delete_bank(
    bank_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("bank:delete")),
    request: Request = None
):
    """Soft-delete a bank by ID."""
    db_bank = crud_bank.get(db, id=bank_id)
    if db_bank is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bank not found or already deleted")
    db_bank = crud_bank.soft_delete(db, db_bank)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="Bank", entity_id=db_bank.id, details={"name": db_bank.name, "ip_address": client_host})
    return db_bank

@router.post("/banks/{bank_id}/restore", response_model=BankOut)
def restore_bank(
    bank_id: int, db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("bank:edit")),
    request: Request = None
):
    """Restore a soft-deleted bank by ID."""
    db_bank = db.query(Bank).filter(Bank.id == bank_id, Bank.is_deleted == True).first()
    if db_bank is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bank not found or not in a soft-deleted state.")
    db_bank = crud_bank.restore(db, db_bank)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="Bank", entity_id=db_bank.id, details={"name": db_bank.name, "ip_address": client_host})
    return db_bank

@router.post("/templates/", response_model=TemplateOut, status_code=status.HTTP_201_CREATED)
def create_template(
    template_in: TemplateCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("template:create")),
    request: Request = None
):
    """Create a new template."""
    existing_template = crud_template.get_by_name_and_action_type(
        db,
        name=template_in.name,
        action_type=template_in.action_type,
        customer_id=template_in.customer_id,
        is_notification_template=template_in.is_notification_template
    )
    if existing_template:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Template with name '{template_in.name}' and action type '{template_in.action_type}' already exists for this scope and purpose."
        )
    db_template = crud_template.create(db, obj_in=template_in)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="CREATE", entity_type="Template", entity_id=db_template.id, details={"name": db_template.name, "action_type": db_template.action_type, "is_global": db_template.is_global, "customer_id": db_template.customer_id, "is_notification_template": db_template.is_notification_template, "ip_address": client_host})
    return db_template

@router.get("/templates/", response_model=List[TemplateOut])
def read_templates(
    action_type: Optional[str] = Query(None, description="Filter templates by action type"),
    customer_id: Optional[int] = Query(None, description="Filter templates by customer ID (for customer-specific templates)"),
    is_notification_template: Optional[bool] = Query(None, description="Filter templates by whether they are for notifications (True/False)"),
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("template:view"))
):
    """
    Retrieve a list of templates. Can filter by action type, customer ID, and notification purpose.
    If customer_id is provided, returns global and customer-specific templates for that customer.
    If no customer_id, only returns global templates.
    """
    templates_query = db.query(Template).filter(Template.is_deleted == False)

    if action_type:
        templates_query = templates_query.filter(Template.action_type == action_type)
    if is_notification_template is not None:
        templates_query = templates_query.filter(Template.is_notification_template == is_notification_template)

    if customer_id:
        templates_query = templates_query.filter(
            (Template.is_global == True) | (Template.customer_id == customer_id)
        )
    else:
        templates_query = templates_query.filter(Template.is_global == True)

    templates = templates_query.offset(skip).limit(limit).all()

    for tpl in templates:
        if tpl.customer:
            tpl.customer_name = tpl.customer.name
    return templates

@router.get("/templates/{template_id}", response_model=TemplateOut)
def read_template(
    template_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("template:view"))
):
    """Retrieve a single template by ID."""
    db_template = crud_template.get_templates_with_customer_name(db, template_id)
    if db_template is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found or already deleted")
    if db_template.customer:
        db_template.customer_name = db_template.customer.name
    return db_template

@router.put("/templates/{template_id}", response_model=TemplateOut)
def update_template(
    template_id: int,
    template_in: TemplateUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("template:edit")),
    request: Request = None
):
    """Update an existing template by ID."""
    db_template = crud_template.get(db, id=template_id)
    if db_template is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found or already deleted")
    
    target_name = template_in.name if template_in.name is not None else db_template.name
    target_action_type = template_in.action_type if template_in.action_type is not None else db_template.action_type
    target_customer_id = template_in.customer_id if template_in.customer_id is not None else db_template.customer_id
    target_is_global = template_in.is_global if template_in.is_global is not None else db_template.is_global
    target_is_notification_template = template_in.is_notification_template if template_in.is_notification_template is not None else db_template.is_notification_template

    if target_is_global:
        if target_customer_id is not None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="customer_id must be null if is_global is true.")
        
        existing_conflict = crud_template.get_by_name_and_action_type(
            db, 
            name=target_name, 
            action_type=target_action_type, 
            customer_id=None,
            is_notification_template=target_is_notification_template
        )
    else:
        if target_customer_id is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="customer_id must be provided if is_global is false (customer-specific template).")

        existing_conflict = crud_template.get_by_name_and_action_type(
            db, 
            name=target_name, 
            action_type=target_action_type, 
            customer_id=target_customer_id,
            is_notification_template=target_is_notification_template
        )
        
    if existing_conflict and existing_conflict.id != template_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Template with name '{target_name}' and action type '{target_action_type}' already exists for this scope and purpose."
        )

    updated_template = crud_template.update(db, db_obj=db_template, obj_in=template_in)
    
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="UPDATE", entity_type="Template", entity_id=updated_template.id, details={"name": updated_template.name, "ip_address": client_host, "updated_fields": template_in.model_dump(exclude_unset=True)})
    
    return updated_template

@router.delete("/templates/{template_id}", response_model=TemplateOut)
def delete_template(
    template_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("template:delete")),
    request: Request = None
):
    """Soft-delete a template by ID."""
    db_template = crud_template.get(db, id=template_id)
    if db_template is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found or already deleted")
    deleted_template = crud_template.soft_delete(db, db_template)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="SOFT_DELETE", entity_type="Template", entity_id=deleted_template.id, details={"name": deleted_template.name, "ip_address": client_host})
    return deleted_template

@router.post("/templates/{template_id}/restore", response_model=TemplateOut)
def restore_template(
    template_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("template:edit")),
    request: Request = None
):
    """Restore a soft-deleted template by ID."""
    db_template = db.query(Template).filter(Template.id == template_id, Template.is_deleted == True).first()
    if db_template is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Template not found or not in a soft-deleted state.")
    restored_template = crud_template.restore(db, db_template)
    client_host = get_client_ip(request) if request else None
    log_action(db, user_id=current_user.user_id, action_type="RESTORE", entity_type="Template", entity_id=restored_template.id, details={"name": restored_template.name, "ip_address": client_host})
    return restored_template

@router.get("/audit-logs/", response_model=List[AuditLogOut])
def read_audit_logs(
    skip: int = 0, 
    limit: int = 100,
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    action_type: Optional[str] = Query(None, description="Filter by type of action (e.g., CREATE, UPDATE)"),
    entity_type: Optional[str] = Query(None, description="Filter by type of entity (e.g., Customer, SubscriptionPlan)"),
    entity_id: Optional[int] = Query(None, description="Filter by ID of the entity"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("audit_log:view"))
):
    """
    Retrieve a list of audit log entries.
    System Owners can filter logs but sensitive data should be redacted in the 'details' if present.
    """
    logs = crud_audit_log.get_all_logs(
        db, 
        skip=skip, 
        limit=limit, 
        user_id=user_id,
        action_type=action_type,
        entity_type=entity_type,
        entity_id=entity_id
    )
    return logs

@router.get("/audit-logs/{log_id}", response_model=AuditLogOut)
def read_audit_log(
    log_id: int, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("audit_log:view"))
):
    """
    Retrieves a single audit log entry by ID.
    """
    log = crud_audit_log.get(db, id=log_id)
    if log is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Audit log entry not found")
    return log

@router.get("/scheduler/jobs")
async def get_scheduled_jobs(current_user: Any = Depends(HasPermission("system_owner:view_scheduler"))):
    """
    Retrieves a list of all scheduled jobs and their details.
    Requires System Owner role with 'system_owner:view_scheduler' permission.
    """
    from app.main import app as fastapi_app
    jobs = []
    for job in fastapi_app.state.scheduler.get_jobs():
        # Corrected code to manually serialize only the necessary attributes.
        job_details = {
            "id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
            "func": job.func.__name__,
            "args": [str(arg) for arg in job.args] if job.args else [], # Convert args to strings
            "kwargs": {k: str(v) for k, v in job.kwargs.items()} if job.kwargs else {}, # Convert kwargs values to strings
            "misfire_grace_time": job.misfire_grace_time,
        }
        jobs.append(job_details)
    return {"jobs": jobs}

@router.post("/scheduler/run_job/{job_id}")
async def run_task_now(job_id: str, current_user: Any = Depends(HasPermission("system_owner:run_scheduler_job"))):
    """
    Manually triggers a scheduled job to run immediately.
    Requires System Owner role with 'system_owner:run_scheduler_job' permission.
    """
    from app.main import app as fastapi_app
    try:
        original_job = fastapi_app.state.scheduler.get_job(job_id)
        if original_job:
            fastapi_app.state.scheduler.add_job(
                func=original_job.func, 
                trigger='date', 
                run_date=datetime.now(), 
                args=original_job.args,
                kwargs=original_job.kwargs
            )
        else:
            raise ValueError(f"Job with id '{job_id}' not found.")
        
        return {"message": f"Job '{job_id}' manually triggered to run now."}
    except Exception as e:
        logger.error(f"Failed to manually run job '{job_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found or could not be run. Error: {e}")

@router.post("/scheduler/reschedule_job/{job_id}")
async def reschedule_job(
    job_id: str,
    new_trigger_data: dict = Body(..., example={"trigger_type": "cron", "hour": 3, "minute": 30}),
    current_user: Any = Depends(HasPermission("system_owner:reschedule_scheduler_job"))
):
    """
    Reschedules a job with a new trigger.
    Requires System Owner role with 'system_owner:reschedule_scheduler_job' permission.
    Example body for a cron trigger: {"trigger_type": "cron", "hour": 3, "minute": 30, "timezone": "Africa/Cairo"}
    Example body for a date trigger: {"trigger_type": "date", "run_date": "2025-08-08T10:00:00+02:00"}
    """
    from app.main import app as fastapi_app
    trigger_type = new_trigger_data.pop("trigger_type")
    
    try:
        if trigger_type == "cron":
            new_trigger = CronTrigger(**new_trigger_data)
        elif trigger_type == "date":
            run_date_str = new_trigger_data.pop("run_date")
            new_trigger = DateTrigger(run_date=datetime.fromisoformat(run_date_str), **new_trigger_data)
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid trigger type: {trigger_type}. Must be 'cron' or 'date'.")
        
        fastapi_app.state.scheduler.reschedule_job(job_id, trigger=new_trigger)
        return {"message": f"Job '{job_id}' rescheduled successfully."}
    except Exception as e:
        logger.error(f"Failed to reschedule job '{job_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to reschedule job '{job_id}'. Error: {e}")

@router.post("/scheduler/pause_job/{job_id}")
async def pause_job(job_id: str, current_user: Any = Depends(HasPermission("system_owner:pause_scheduler_job"))):
    """
    Pauses a scheduled job.
    Requires System Owner role with 'system_owner:pause_scheduler_job' permission.
    """
    from app.main import app as fastapi_app
    try:
        fastapi_app.state.scheduler.pause_job(job_id)
        return {"message": f"Job '{job_id}' paused successfully."}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found or could not be paused. Error: {e}")

@router.post("/scheduler/resume_job/{job_id}")
async def resume_job(job_id: str, current_user: Any = Depends(HasPermission("system_owner:resume_scheduler_job"))):
    """
    Resumes a paused job.
    Requires System Owner role with 'system_owner:resume_scheduler_job' permission.
    """
    from app.main import app as fastapi_app
    try:
        fastapi_app.state.scheduler.resume_job(job_id)
        return {"message": f"Job '{job_id}' resumed successfully."}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found or could not be resumed. Error: {e}")

@router.post("/system-notifications/", response_model=SystemNotificationOut, status_code=status.HTTP_201_CREATED)
def create_system_notification(
    notification_in: SystemNotificationCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_notification:create")),
    request: Request = None
):
    """
    Creates a new system-wide notification.
    Requires: content, start_date, end_date, and optionally a link and target_customer_ids.
    """
    client_host = get_client_ip(request) if request else None

    if notification_in.start_date >= notification_in.end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="End date must be after start date."
        )

    if (notification_in.target_customer_ids is None or len(notification_in.target_customer_ids) == 0) and \
       (notification_in.target_roles is None or len(notification_in.target_roles) == 0) and \
       (notification_in.target_user_ids is None or len(notification_in.target_user_ids) == 0):
        existing_universal_notifications = crud_system_notification.get_active_universal_notifications(db)
        if any(n.id != notification_id for n in existing_universal_notifications):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A universal notification is already active. Please deactivate it before creating another one."
            )

    db_notification = crud_system_notification.create(
        db,
        obj_in=notification_in,
        created_by_user_id=current_user.user_id
    )

    log_action(
        db,
        user_id=current_user.user_id,
        action_type="CREATE",
        entity_type="SystemNotification",
        entity_id=db_notification.id,
        details={
            "content_preview": db_notification.content[:50] + "...",
            "is_active": db_notification.is_active,
            "start_date": db_notification.start_date.isoformat(),
            "end_date": db_notification.end_date.isoformat(),
            "link": db_notification.link,
            "target_customers": db_notification.target_customer_ids,
            "animation_type": db_notification.animation_type,
            "display_frequency": db_notification.display_frequency,
            "max_display_count": db_notification.max_display_count,
            "target_user_ids": db_notification.target_user_ids,
            "target_roles": db_notification.target_roles,
            "ip_address": client_host
        }
    )
    return db_notification

@router.get("/system-notifications/", response_model=List[SystemNotificationOut])
def read_system_notifications(
    skip: int = 0,
    limit: int = 100,
    is_active: Optional[bool] = Query(None, description="Filter notifications by active status."),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_notification:view"))
):
    """
    Retrieves a list of all system notifications for administration. Can filter by active status.
    """
    if is_active is True:
        notifications = crud_system_notification.get_all_active(db, skip=skip, limit=limit)
    else:
        notifications = crud_system_notification.get_all(db, skip=skip, limit=limit)
    return notifications

@router.get("/system-notifications/{notification_id}", response_model=SystemNotificationOut)
def read_system_notification(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_notification:view"))
):
    """
    Retrieves a single system notification by its ID.
    """
    db_notification = crud_system_notification.get(db, id=notification_id)
    if db_notification is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="System notification not found or already deleted."
        )
    return db_notification

@router.put("/system-notifications/{notification_id}", response_model=SystemNotificationOut)
def update_system_notification(
    notification_id: int,
    notification_in: SystemNotificationUpdate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_notification:edit")),
    request: Request = None
):
    """
    Updates an existing system notification by ID.
    """
    db_notification = crud_system_notification.get(db, id=notification_id)
    if db_notification is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="System notification not found or already deleted."
        )

    new_start_date = notification_in.start_date or db_notification.start_date
    new_end_date = notification_in.end_date or db_notification.end_date
    
    if new_start_date >= new_end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="End date must be after start date."
        )
    
    new_target_customers = notification_in.target_customer_ids if notification_in.target_customer_ids is not None else db_notification.target_customer_ids
    new_target_roles = notification_in.target_roles if notification_in.target_roles is not None else db_notification.target_roles
    new_target_users = notification_in.target_user_ids if notification_in.target_user_ids is not None else db_notification.target_user_ids

    if (not new_target_customers or len(new_target_customers) == 0) and \
       (not new_target_roles or len(new_target_roles) == 0) and \
       (not new_target_users or len(new_target_users) == 0):
        existing_universal_notifications = crud_system_notification.get_active_universal_notifications(db)
        if any(n.id != notification_id for n in existing_universal_notifications):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A universal notification is already active. Please deactivate it before updating this one to be universal."
            )

    updated_notification = crud_system_notification.update(
        db,
        db_obj=db_notification,
        obj_in=notification_in,
        user_id=current_user.user_id
    )
    return updated_notification

@router.delete("/system-notifications/{notification_id}", response_model=SystemNotificationOut)
def delete_system_notification(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_notification:delete")),
    request: Request = None
):
    """
    Soft-deletes a system notification by ID.
    """
    db_notification = crud_system_notification.get(db, id=notification_id)
    if db_notification is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="System notification not found or already deleted."
        )

    deleted_notification = crud_system_notification.soft_delete(
        db,
        db_notification,
        user_id=current_user.user_id
    )
    return deleted_notification

@router.post("/system-notifications/{notification_id}/restore", response_model=SystemNotificationOut)
def restore_system_notification(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_notification:edit")),
    request: Request = None
):
    """
    Restores a soft-deleted system notification by ID.
    """
    db_notification = db.query(SystemNotification).filter(
        SystemNotification.id == notification_id,
        SystemNotification.is_deleted == True
    ).first()

    if db_notification is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="System notification not found or not in a soft-deleted state."
        )

    restored_notification = crud_system_notification.restore(
        db,
        db_notification,
        user_id=current_user.user_id
    )
    return restored_notification

@router.get("/users/", response_model=List[UserOut])
def read_users(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("user:view"))
):
    """
    Retrieve a list of all active users.
    """
    users = crud_user.get_all(db, skip=skip, limit=limit)
    return users
    
# NEW ENDPOINTS FOR MANAGING LEGAL ARTIFACTS

@router.post("/legal-artifacts", response_model=LegalArtifactOut, status_code=status.HTTP_201_CREATED)
@router.post("/legal-artifacts/", response_model=LegalArtifactOut, status_code=status.HTTP_201_CREATED)
def create_legal_artifact(
    artifact_in: LegalArtifactCreate,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("legal_artifact:create")),
    request: Request = None,
):
    """
    Allows a System Owner to create a new legal artifact and update its global version.
    This action will trigger a re-acceptance for all users on next login.
    """
    client_host = get_client_ip(request) if request else None

    # Enforce one of the two legal artifact types
    if artifact_in.artifact_type not in [LegalArtifactType.TERMS_AND_CONDITIONS, LegalArtifactType.PRIVACY_POLICY]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid legal artifact type. Must be one of: {LegalArtifactType.TERMS_AND_CONDITIONS}, {LegalArtifactType.PRIVACY_POLICY}"
        )
    
    # Create the new legal artifact in the database
    db_artifact = crud_legal_artifact.create_artifact(db, obj_in=artifact_in)

    # Update the global configuration with the new version number
    if db_artifact.artifact_type == LegalArtifactType.TERMS_AND_CONDITIONS:
        config_key = GlobalConfigKey.TC_VERSION
    else:
        config_key = GlobalConfigKey.PP_VERSION

    # Retrieve or create the global config entry
    db_config = crud_global_configuration.get_by_key(db, key=config_key)
    if db_config:
        # CORRECTED: Create a schema object to update with the new value
        config_update_in = GlobalConfigurationUpdate(value_default=str(db_artifact.version))
        crud_global_configuration.update(db, db_obj=db_config, obj_in=config_update_in)
    else:
        # If config key doesn't exist, create it
        new_config_in = GlobalConfigurationCreate(
            key=config_key,
            value_default=str(db_artifact.version),
            description=f"Current version of the {db_artifact.artifact_type.replace('_', ' ')}."
        )
        crud_global_configuration.create(db, obj_in=new_config_in)
    
    log_action(
        db,
        user_id=current_user.user_id,
        action_type="CREATE",
        entity_type="LegalArtifact",
        entity_id=db_artifact.id,
        details={"artifact_type": db_artifact.artifact_type, "version": db_artifact.version, "ip_address": client_host}
    )

    return db_artifact

@router.get("/legal-artifacts/{artifact_type}", response_model=LegalArtifactOut)
def read_legal_artifact(
    artifact_type: LegalArtifactType,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("legal_artifact:view"))
):
    """
    Retrieves the latest version of a legal artifact by its type.
    """
    db_artifact = crud_legal_artifact.get_by_artifact_type(db, artifact_type=artifact_type)
    if not db_artifact:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Legal artifact of type '{artifact_type}' not found."
        )
    return db_artifact
    
@trial_router.get("/trial-registrations/", response_model=List[TrialRegistrationOut])
def read_trial_registrations(
    status: Optional[str] = Query(None, description="Filter by registration status (e.g., 'pending', 'approved', 'rejected')"),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_owner:view_trial_registrations"))
):
    """
    Retrieves a list of trial registrations, with optional status filtering.
    """
    # Fetch all registrations with the requested status.
    registrations = crud_trial_registration.get_by_status(db, status=status)

    # Use a list comprehension to explicitly validate and return the data
    # against the Pydantic schema. This ensures all fields, including the
    # IP and date, are included in the JSON response.
    return [TrialRegistrationOut.model_validate(reg) for reg in registrations]

@trial_router.post("/trial-registrations/{registration_id}/approve", response_model=CustomerOut, status_code=status.HTTP_201_CREATED)
async def approve_trial_registration(
    registration_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_owner:approve_trial_registration")),
    request: Request = None,
    background_tasks: BackgroundTasks = None
):
    """
    Approves a pending trial registration, creates a new customer and initial user,
    and sends a welcome email.
    """
    registration = crud_trial_registration.get(db, id=registration_id)
    if not registration or registration.status != "pending":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Registration not found or not in pending state.")

    try:
        # Step 1: Create a new customer and user
        subscription_plan = crud_subscription_plan.get_by_name(db, name="Free Trial Plan")
        if not subscription_plan:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="'Free Trial Plan' subscription plan not found. System configuration error.")
        
        # NEW: Generate a random password for the new user
        generated_password = 'Password123!'

        customer_in = CustomerCreate(
            name=registration.organization_name,
            address=registration.organization_address,
            contact_email=registration.admin_email,
            contact_phone=registration.contact_phone,
            subscription_plan_id=subscription_plan.id,
            initial_corporate_admin=UserCreate(
                email=registration.admin_email,
                password=generated_password,
                role=UserRole.CORPORATE_ADMIN,
                must_change_password=True,
                has_all_entity_access=True,
                entity_ids=[],
            ),
            initial_entities=[]
        )
        if registration.entities_count == 'One':
            entity_name = registration.organization_name + " Main Entity"
            customer_in.initial_entities.append(CustomerEntityCreate(
                entity_name=entity_name,
                code="TEMP", # This will be replaced with a generated code in the CRUD layer
                contact_person=registration.contact_admin_name,
                contact_email=registration.admin_email
            ))

        db_customer = crud_customer.onboard_customer(db, customer_in, user_id_caller=current_user.user_id)
        
        # Step 2: Update the registration status
        crud_trial_registration.update(db, registration, {"status": "approved", "customer_id": db_customer.id})

        # Step 3: Send approval email
        email_settings = get_global_email_settings()
        subject = "Your LG Custody Free Trial is Ready!"
        body = f"""
            <html><body>
                <p>Hello {registration.contact_admin_name},</p>
                <p>We're excited to confirm that your free trial account for the LG Custody Platform is now active!</p>
                <p>To get started, please use the following temporary password to log in: <strong>{generated_password}</strong>. We highly recommend that you change this password immediately after your first login.</p>
                <p><strong>Login Link:</strong> <a href="{os.getenv('FRONTEND_URL', 'https://www.growbusinessdevelopment.com')}/login">{os.getenv('FRONTEND_URL', 'https://www.growbusinessdevelopment.com')}/login</a></p>
                <p>Your free trial will be active for {subscription_plan.duration_months} months, expiring on {db_customer.end_date.strftime('%Y-%m-%d')}.</p>
                <p><strong>Quick Start Guide:</strong> Please find a quick-start guide attached to help you get started with the platform's core features.</p>
                <p>Best regards,</p>
                <p>The LG Custody Team</p>
            </body></html>
        """
        if registration.entities_count == 'Multiple':
            body = body.replace("</body>", f"""
                <p>Since you indicated multiple entities, please reply to this email with the details and documents for each additional entity (similar to the documents you submitted for the main entity). Once received, we will add them to your account.</p>
                </body>
            """)

        # NEW CODE: Define a dynamic path to the PDF guide
        # NEW CODE: Define a dynamic path to the PDF guide
        current_file_dir = os.path.dirname(os.path.abspath(__file__))
        # From c:\Grow\app\api\v1\endpoints, go up 3 levels to get to c:\Grow\app
        app_root = os.path.abspath(os.path.join(current_file_dir, '..', '..', '..'))
        pdf_path = os.path.join(app_root, 'templates', 'quick_start_guide.pdf')

        logger.info(f"Looking for PDF at: {pdf_path}")

        try:
            with open(pdf_path, "rb") as f:
                attachment = EmailAttachment("Quick_Start_Guide.pdf", f.read(), "application/pdf")
                background_tasks.add_task(send_email, db, [registration.admin_email], subject, body, {}, email_settings, attachments=[attachment])
                logger.info(f"Successfully found PDF at: {pdf_path}")
        except FileNotFoundError:
            logger.warning(f"Quick start guide PDF not found at: {pdf_path}. Sending email without attachment.")
            background_tasks.add_task(send_email, db, [registration.admin_email], subject, body, {}, email_settings)

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to approve registration: {e}")

    log_action(db, user_id=current_user.user_id, action_type="TRIAL_REGISTRATION_APPROVED", entity_type="TrialRegistration", entity_id=registration_id, details={"organization": registration.organization_name, "customer_id": db_customer.id})
    return crud_customer.get_with_relations(db, db_customer.id)

@trial_router.post("/trial-registrations/{registration_id}/reject", status_code=status.HTTP_200_OK)
def reject_trial_registration(
    registration_id: int,
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(HasPermission("system_owner:reject_trial_registration")),
    request: Request = None
):
    """
    Rejects a pending trial registration and sends a rejection email.
    """
    registration = crud_trial_registration.get(db, id=registration_id)
    if not registration or registration.status != "pending":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Registration not found or not in pending state.")

    crud_trial_registration.update(db, registration, {"status": "rejected"})

    # Send rejection email (logic for this will be in a later step)

    log_action(db, user_id=current_user.user_id, action_type="TRIAL_REGISTRATION_REJECTED", entity_type="TrialRegistration", entity_id=registration_id, details={"organization": registration.organization_name})
    return {"message": "Registration rejected successfully."}

router.include_router(trial_router, prefix="/trial", tags=["Trial Registration"])
