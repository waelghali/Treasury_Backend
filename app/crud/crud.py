# c:\Grow\app\crud\crud.py
import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Type, TypeVar
from fastapi import HTTPException, status, UploadFile
import decimal
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import relationship, selectinload
from sqlalchemy.orm import Session

# --- Core backend services & utilities (used by log_action or other CRUDs) ---
from app.core.email_service import EmailSettings, get_global_email_settings, send_email
from app.core.encryption import decrypt_data, encrypt_data
from app.core.document_generator import generate_pdf_from_html
from app.core.ai_integration import _upload_to_gcs # Assuming this utility exists

# --- Import Models and Schemas ---
import app.models as models
from app.models import (
    ApprovalRequest,
    AuditLog,
    Bank,
    Currency,
    Customer,
    CustomerConfiguration,
    CustomerEmailSetting,
    CustomerEntity,
    GlobalConfiguration,
    InternalOwnerContact,
    IssuingMethod,
    LGCategory,
    LGCategoryCustomerEntityAssociation,
    LGDocument,
    LGInstruction,
    LgOperationalStatus,
    LGRecord,
    LgStatus,
    LgType,
    Permission,
    RolePermission,
    SubscriptionPlan,
    Template,
    UniversalCategory,
    User,
    UserCustomerEntityAssociation,
    SystemNotification,
    SystemNotificationViewLog,
)
# NEW: Import SubscriptionStatus for checking against models and schemas
from app.constants import SubscriptionStatus

from app.schemas.all_schemas import (
    ApprovalRequestCreate,
    ApprovalRequestOut,
    ApprovalRequestUpdate,
    AuditLogCreate,
    AuditLogOut,
    BankCreate,
    BankOut,
    BankUpdate,
    CurrencyCreate,
    CurrencyOut,
    CurrencyUpdate,
    CustomerConfigurationCreate,
    CustomerConfigurationOut,
    CustomerConfigurationUpdate,
    CustomerCoreCreate,
    CustomerCreate,
    CustomerEmailSettingCreate,
    CustomerEmailSettingOut,
    CustomerEmailSettingUpdate,
    CustomerEntityCreate,
    CustomerEntityOut,
    CustomerEntityUpdate,
    CustomerOut,
    CustomerUpdate,
    GlobalConfigurationCreate,
    GlobalConfigurationOut,
    GlobalConfigurationUpdate,
    InternalOwnerContactCreate,
    InternalOwnerContactOut,
    InternalOwnerContactUpdateDetails,
    IssuingMethodCreate,
    IssuingMethodOut,
    IssuingMethodUpdate,
    LGCategoryCreate,
    LGCategoryOut,
    LGCategoryUpdate,
    LGDocumentCreate,
    LGDocumentOut,
    LGInstructionCreate,
    LGInstructionOut,
    LGInstructionUpdate,
    LgOperationalStatusCreate,
    LgOperationalStatusOut,
    LgOperationalStatusUpdate,
    LGRecordCreate,
    LGRecordOut,
    LGRecordUpdate,
    LgStatusCreate,
    LgStatusOut,
    LgStatusUpdate,
    LgTypeCreate,
    LgTypeOut,
    LgTypeUpdate,
    PermissionCreate,
    PermissionOut,
    RolePermissionCreate,
    RolePermissionOut,
    RuleCreate,
    RuleOut,
    RuleUpdate,
    SubscriptionPlanCreate,
    SubscriptionPlanOut,
    SubscriptionPlanUpdate,
    TemplateCreate,
    TemplateOut,
    TemplateUpdate,
    UniversalCategoryCreate,
    UniversalCategoryOut,
    UniversalCategoryUpdate,
    UserCreate,
    UserCreateCorporateAdmin,
    UserOut,
    UserUpdate,
    UserUpdateCorporateAdmin,
    LGRecordChangeOwner,
    InternalOwnerChangeScope,
    # NEW: Import new report schemas
    SystemUsageOverviewReportOut,
    CustomerLGPerformanceReportOut,
    MyLGDashboardReportOut,
    SystemNotificationCreate,
    SystemNotificationUpdate,
    SystemNotificationOut,
    # NEW: Import cancellation request schema
    LGInstructionCancelRequest,
)

# --- Import Constants ---
from app.constants import (
    GlobalConfigKey,
    UserRole,
    ACTION_TYPE_LG_DECREASE_AMOUNT,
    AUDIT_ACTION_TYPE_LG_DECREASED_AMOUNT,
    ACTION_TYPE_LG_RECORD_DELIVERY,
    AUDIT_ACTION_TYPE_LG_INSTRUCTION_DELIVERED,
    ACTION_TYPE_LG_RECORD_BANK_REPLY,
    AUDIT_ACTION_TYPE_LG_BANK_REPLY_RECORDED,
    ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT,
    AUDIT_ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT_SENT,
    ACTION_TYPE_LG_REMINDER_TO_BANKS,
    AUDIT_ACTION_TYPE_LG_REMINDER_SENT_TO_BANK,
    ACTION_TYPE_LG_EXTEND,
    ACTION_TYPE_LG_RELEASE,
    ACTION_TYPE_LG_LIQUIDATE,
    ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
    ACTION_TYPE_LG_AMEND,
    ACTION_TYPE_LG_CANCEL_LAST_INSTRUCTION,
    ACTION_TYPE_LG_CHANGE_OWNER_DETAILS,
    ACTION_TYPE_LG_CHANGE_SINGLE_LG_OWNER,
    ACTION_TYPE_LG_CHANGE_BULK_LG_OWNER,
    AUDIT_ACTION_TYPE_LG_BULK_REMINDER_INITIATED,
    AUDIT_ACTION_TYPE_LG_AMENDED,
    AUDIT_ACTION_TYPE_LG_ACTIVATED,
    # NEW: Import subscription notification types
    SubscriptionNotificationType,
    ACTION_TYPE_LG_RECORDED,
)


# Define ModelType for generic CRUDBase typing
ModelType = TypeVar("ModelType", bound=models.Base)

# =====================================================================================
# Base CRUD Class Definition (CRUDBase)
# =====================================================================================
class CRUDBase:
    def __init__(self, model: Type[ModelType]):
        self.model = model

    def get(self, db: Session, id: Any) -> Optional[ModelType]:
        
        return db.query(self.model).filter(self.model.id == id, self.model.is_deleted == False).first() 

    def get_all(self, db: Session, skip: int = 0, limit: int = 100) -> List[ModelType]:
        
        return db.query(self.model).filter(self.model.is_deleted == False).offset(skip).limit(limit).all() 

    def create(self, db: Session, obj_in: Any, **kwargs: Any) -> ModelType:
        
        obj_data = obj_in.model_dump(exclude_unset=True) 
        create_data = {**obj_data, **kwargs}
        db_obj = self.model(**create_data)
        db.add(db_obj)
        db.flush() 
        db.refresh(db_obj)
        return db_obj

    def update(self, db: Session, db_obj: ModelType, obj_in: Any, **kwargs: Any) -> ModelType:
        
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True) 

        old_values_for_log = {
            k: getattr(db_obj, k)
            for k in update_data.keys()
            if hasattr(db_obj, k)
        }

        update_data_merged = {**update_data, **kwargs}
        for field, value in update_data_merged.items():
            setattr(db_obj, field, value)
        db_obj.updated_at = func.now() #
        db.add(db_obj)
        db.flush() 
        db.refresh(db_obj)

        new_values_for_log = {
            k: getattr(db_obj, k)
            for k in update_data.keys()
            if hasattr(db_obj, k)
        }

        changed_fields = {}
        for key in update_data.keys():
            old_val = old_values_for_log.get(key)
            new_val = new_values_for_log.get(key)

            if isinstance(old_val, (datetime, date)):
                old_val = old_val.isoformat()
            if isinstance(new_val, (datetime, date)):
                new_val = new_val.isoformat()
            if isinstance(old_val, decimal.Decimal):
                old_val = float(old_val)
            if isinstance(new_val, decimal.Decimal):
                new_val = float(new_val)

            if old_val != new_val:
                changed_fields[key] = {"old": old_val, "new": new_val}

        db_obj._changed_fields_for_log = changed_fields 
        return db_obj

    def soft_delete(self, db: Session, db_obj: ModelType) -> ModelType:
        
        db_obj.is_deleted = True 
        db_obj.deleted_at = func.now() 
        db.add(db_obj)
        db.flush() 
        db.refresh(db_obj)
        return db_obj

    def restore(self, db: Session, db_obj: ModelType) -> ModelType:
        #
        db_obj.is_deleted = False 
        db_obj.deleted_at = None 
        db.add(db_obj)
        db.flush() 
        db.refresh(db_obj)
        return db_obj


# =====================================================================================
# Log Action Utility (Defined directly in this central crud.py)
# =====================================================================================
def log_action(
    db: Session,
    user_id: Optional[int],
    action_type: str,
    entity_type: str,
    entity_id: Optional[int],
    details: Optional[Dict[str, Any]] = None,
    customer_id: Optional[int] = None,
    lg_record_id: Optional[int] = None,
    ip_address: Optional[str] = None,
):
    """
    Logs an action to the AuditLog.
    Uses db.flush() instead of db.commit() to allow the calling function to manage the overall transaction.
    """
    try:
        audit_log_entry = models.AuditLog(
            user_id=user_id,
            action_type=action_type,
            entity_type=entity_type,
            entity_id=entity_id,
            details=details,
            customer_id=customer_id,
            lg_record_id=lg_record_id,
            ip_address=ip_address,
            timestamp=func.now(),
        )
        db.add(audit_log_entry)
        db.flush() #
        db.refresh(audit_log_entry)
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error creating audit log entry: {e}", exc_info=True)


# =====================================================================================
# NEW CRUD FOR SYSTEM NOTIFICATION VIEW LOG
# =====================================================================================
class CRUDSystemNotificationViewLog(CRUDBase):
    def get_by_user_and_notification(self, db: Session, user_id: int, notification_id: int) -> Optional[models.SystemNotificationViewLog]:
        return db.query(self.model).filter(
            self.model.user_id == user_id,
            self.model.notification_id == notification_id
        ).first()

    def increment_view_count(self, db: Session, user_id: int, notification_id: int):
        db_log = self.get_by_user_and_notification(db, user_id, notification_id)
        if db_log:
            db_log.view_count += 1
            db.add(db_log)
        else:
            db_log = self.model(user_id=user_id, notification_id=notification_id, view_count=1)
            db.add(db_log)
        db.flush()
        db.refresh(db_log)
        return db_log


# =====================================================================================
# Specific CRUD Class Imports (These files define the classes, not instances)
# =====================================================================================
from .crud_audit import CRUDAuditLog
from .crud_config import CRUDGlobalConfiguration, CRUDCustomerConfiguration
from .crud_customer import CRUDCustomer, CRUDCustomerEntity, CRUDCustomerEmailSetting
from .crud_master_data import (
    CRUDBank, CRUDTemplate, CRUDCurrency, CRUDLgType, CRUDRule, CRUDIssuingMethod,
    CRUDLgStatus, CRUDLgOperationalStatus, CRUDUniversalCategory
)
from .crud_permissions import CRUDPermission, CRUDRolePermission
from .crud_subscription import CRUDSubscriptionPlan
from .crud_user import CRUDUser
from .crud_approval_request import CRUDApprovalRequest
from .crud_lg_owner import CRUDLGOwner

# NEW IMPORTS for the split CRUDs
from .crud_internal_owner_contact import CRUDInternalOwnerContact
from .crud_lg_category import CRUDLGCategory
from .crud_lg_document import CRUDLGDocument
from .crud_lg_instruction import CRUDLGInstruction
from .crud_lg_record import CRUDLGRecord
from .crud_lg_cancellation import CRUDLGCancellation # New import
from .crud_reports import CRUDReports
from .crud_system_notification import CRUDSystemNotification
from . import subscription_tasks


# =====================================================================================
# Centralized CRUD Instances Instantiation and Re-export
# =====================================================================================

crud_audit_log = CRUDAuditLog(models.AuditLog)
crud_global_configuration = CRUDGlobalConfiguration(models.GlobalConfiguration)
crud_customer_configuration = CRUDCustomerConfiguration(models.CustomerConfiguration, crud_global_configuration)

# Order matters for dependency injection! 
# Core foundational CRUDs first
crud_customer_entity = CRUDCustomerEntity(models.CustomerEntity)
crud_user = CRUDUser(models.User)
crud_internal_owner_contact = CRUDInternalOwnerContact(models.InternalOwnerContact)

# Master Data CRUDs
crud_bank = CRUDBank(models.Bank)
crud_template = CRUDTemplate(models.Template)
crud_currency = CRUDCurrency(models.Currency)
crud_lg_type = CRUDLgType(models.LgType)
crud_rule = CRUDRule(models.Rule)
crud_issuing_method = CRUDIssuingMethod(models.IssuingMethod)
crud_lg_status = CRUDLgStatus(models.LgStatus)
crud_lg_operational_status = CRUDLgOperationalStatus(models.LgOperationalStatus)
crud_universal_category = CRUDUniversalCategory(models.UniversalCategory)
crud_permission = CRUDPermission(models.Permission)
crud_role_permission = CRUDRolePermission(models.RolePermission)
crud_subscription_plan = CRUDSubscriptionPlan(models.SubscriptionPlan)

# Dependent CRUDs
crud_customer = CRUDCustomer(
    models.Customer,
    crud_customer_entity_instance=crud_customer_entity,
    crud_user_instance=crud_user
)
crud_customer_email_setting = CRUDCustomerEmailSetting(models.CustomerEmailSetting)
crud_lg_category = CRUDLGCategory(models.LGCategory)

# LG Document depends on customer configuration
crud_lg_document = CRUDLGDocument(models.LGDocument, crud_customer_configuration_instance=crud_customer_configuration)

# LG Instruction depends on LG Document and Customer Configuration
crud_lg_instruction = CRUDLGInstruction(
    models.LGInstruction,
    crud_lg_document_instance=crud_lg_document,
    crud_customer_configuration_instance=crud_customer_configuration
)

# LG Record depends on Internal Owner Contact, LG Instruction, LG Document, Customer Configuration
crud_lg_record = CRUDLGRecord(
    models.LGRecord,
    crud_internal_owner_contact_instance=crud_internal_owner_contact,
    crud_lg_instruction_instance=crud_lg_instruction,
    crud_lg_document_instance=crud_lg_document,
    crud_customer_configuration_instance=crud_customer_configuration
)

# New instantiation for cancellation CRUD
crud_lg_cancellation = CRUDLGCancellation(
    models.LGInstruction,
    crud_lg_record_instance=crud_lg_record,
    crud_customer_configuration_instance=crud_customer_configuration,
    crud_user_instance=crud_user,
)

# LG Owner depends on LG Record and Internal Owner Contact
crud_lg_owner = CRUDLGOwner(models.LGRecord, models.InternalOwnerContact)

# Approval Request depends on LG Record, Internal Owner Contact, User (via maker/checker fields)
crud_approval_request = CRUDApprovalRequest(models.ApprovalRequest)

# NEW: Instantiate CRUDReports
crud_reports = CRUDReports(
    models.LGRecord, # Primary model for reports, but it accesses others
    crud_customer_configuration_instance=crud_customer_configuration,
    crud_user_instance=crud_user
)

# NEW: Instantiate CRUDSystemNotification and its view log
crud_system_notification = CRUDSystemNotification(models.SystemNotification)
crud_system_notification_view_log = CRUDSystemNotificationViewLog(models.SystemNotificationViewLog)

__all__ = [ 
    "CRUDBase", 
    "log_action", 
    "crud_audit_log", 
    "crud_global_configuration", 
    "crud_customer_configuration", 
    "crud_customer", 
    "crud_customer_entity", 
    "crud_customer_email_setting", 
    # Updated to new specific CRUDs
    "crud_lg_record", 
    "crud_lg_category", 
    "crud_lg_instruction", 
    "crud_lg_document", 
    "crud_internal_owner_contact", 
    "crud_bank", 
    "crud_template", 
    "crud_currency", 
    "crud_lg_type", 
    "crud_rule", 
    "crud_issuing_method", 
    "crud_lg_status", 
    "crud_lg_operational_status", 
    "crud_universal_category", 
    "crud_permission", 
    "crud_role_permission", 
    "crud_subscription_plan", 
    "crud_user", 
    "crud_approval_request", 
    "crud_lg_owner", 
    "crud_reports",
    "crud_system_notification",
    "crud_system_notification_view_log",
    "crud_lg_cancellation", # New instance
]
