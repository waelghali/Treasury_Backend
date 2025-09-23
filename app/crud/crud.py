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
    User,
    UserCustomerEntityAssociation,
    SystemNotification,
    SystemNotificationViewLog,
    LGMigrationStaging,
    MigrationBatch,
    LGChangeLog,
    LegalArtifact,
    UserLegalAcceptance,
    TrialRegistration
)
# NEW: Import SubscriptionStatus for checking against models and schemas
from app.constants import SubscriptionStatus
# NEW: Import migration schemas
from app.schemas.migration_schemas import LGMigrationStagingIn, LGMigrationStagingOut, MigrationReportSummary
from app.schemas.migration_history_schemas import MigrationBatchOut

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
    # MODIFIED: Removed UniversalCategory schemas
    UserCreate,
    UserCreateCorporateAdmin,
    UserOut,
    UserUpdate,
    UserUpdateCorporateAdmin,
    LGRecordChangeOwner,
    InternalOwnerChangeScope,
    SystemUsageOverviewReportOut,
    CustomerLGPerformanceReportOut,
    MyLGDashboardReportOut,
    SystemNotificationCreate,
    SystemNotificationUpdate,
    SystemNotificationOut,
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

# NEW: Import CRUDBase and log_action from the new base.py file
from .base import CRUDBase, log_action
# Define ModelType for generic CRUDBase typing
ModelType = TypeVar("ModelType", bound=models.Base)


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
# NEW CRUD FOR TRIAL REGISTRATION
# =====================================================================================
class CRUDTrialRegistration(CRUDBase):
    def get_by_email_and_status(self, db: Session, email: str, status: str) -> Optional[models.TrialRegistration]:
        return db.query(self.model).filter(
            self.model.admin_email == email,
            self.model.status == status
        ).first()

    def get_by_status(self, db: Session, status: Optional[str] = None) -> List[models.TrialRegistration]:
        query = db.query(self.model)
        if status:
            query = query.filter(self.model.status == status)
        return query.order_by(self.model.accepted_terms_at.desc()).all()

# =====================================================================================
# Specific CRUD Class Imports (These files define the classes, not instances)
# =====================================================================================
from .crud_audit import CRUDAuditLog
from .crud_config import CRUDGlobalConfiguration, CRUDCustomerConfiguration
from .crud_customer import CRUDCustomer, CRUDCustomerEntity, CRUDCustomerEmailSetting
from .crud_master_data import (
    CRUDBank, CRUDTemplate, CRUDCurrency, CRUDLgType, CRUDRule, CRUDIssuingMethod,
    CRUDLgStatus, CRUDLgOperationalStatus
)
from .crud_permissions import CRUDPermission, CRUDRolePermission
from .crud_subscription import CRUDSubscriptionPlan
from .crud_user import CRUDUser
from .crud_approval_request import CRUDApprovalRequest
from .crud_lg_owner import CRUDLGOwner
from .crud_internal_owner_contact import CRUDInternalOwnerContact
from .crud_lg_category import CRUDLGCategory
from .crud_lg_document import CRUDLGDocument
from .crud_lg_instruction import CRUDLGInstruction
from .crud_lg_record import CRUDLGRecord
from .crud_lg_cancellation import CRUDLGCancellation
from .crud_reports import CRUDReports
from .crud_system_notification import CRUDSystemNotification
from . import subscription_tasks
from .crud_migration import CRUDLGMigration
from .crud_legal import CRUDLegalArtifact, CRUDUserLegalAcceptance # NEW IMPORT

# NEW IMPORTS for historical reconstruction
from .crud_migration_history import CRUDMigrationBatch, CRUDLGChangeLog
from app.core.migration_history import MigrationHistoryService

# =====================================================================================
# Centralized CRUD Instances Instantiation and Re-export
# =====================================================================================

crud_audit_log = CRUDAuditLog(models.AuditLog)
crud_global_configuration = CRUDGlobalConfiguration(models.GlobalConfiguration)
crud_customer_configuration = CRUDCustomerConfiguration(models.CustomerConfiguration, crud_global_configuration)

crud_customer_entity = CRUDCustomerEntity(models.CustomerEntity)
crud_user = CRUDUser(models.User)
crud_internal_owner_contact = CRUDInternalOwnerContact(models.InternalOwnerContact)

crud_bank = CRUDBank(models.Bank)
crud_template = CRUDTemplate(models.Template)
crud_currency = CRUDCurrency(models.Currency)
crud_lg_type = CRUDLgType(models.LgType)
crud_rule = CRUDRule(models.Rule)
crud_issuing_method = CRUDIssuingMethod(models.IssuingMethod)
crud_lg_status = CRUDLgStatus(models.LgStatus)
crud_lg_operational_status = CRUDLgOperationalStatus(models.LgOperationalStatus)
crud_permission = CRUDPermission(models.Permission)
crud_role_permission = CRUDRolePermission(models.RolePermission)
crud_subscription_plan = CRUDSubscriptionPlan(models.SubscriptionPlan)

crud_customer = CRUDCustomer(
    models.Customer,
    crud_customer_entity_instance=crud_customer_entity,
    crud_user_instance=crud_user
)
crud_customer_email_setting = CRUDCustomerEmailSetting(models.CustomerEmailSetting)
crud_lg_category = CRUDLGCategory(models.LGCategory)

crud_lg_document = CRUDLGDocument(
    models.LGDocument,
    crud_customer_configuration_instance=crud_customer_configuration
)

crud_lg_instruction = CRUDLGInstruction(
    models.LGInstruction,
    crud_lg_document_instance=crud_lg_document,
    crud_customer_configuration_instance=crud_customer_configuration
)

crud_lg_record = CRUDLGRecord(
    models.LGRecord,
    crud_internal_owner_contact_instance=crud_internal_owner_contact,
    crud_lg_instruction_instance=crud_lg_instruction,
    crud_lg_document_instance=crud_lg_document,
    crud_customer_configuration_instance=crud_customer_configuration
)

crud_lg_cancellation = CRUDLGCancellation(
    models.LGInstruction,
    crud_lg_record_instance=crud_lg_record,
    crud_customer_configuration_instance=crud_customer_configuration,
    crud_user_instance=crud_user,
)

crud_lg_owner = CRUDLGOwner(models.LGRecord, models.InternalOwnerContact)

crud_approval_request = CRUDApprovalRequest(models.ApprovalRequest)

crud_reports = CRUDReports(
    models.LGRecord,
    crud_customer_configuration_instance=crud_customer_configuration,
    crud_user_instance=crud_user
)

crud_system_notification = CRUDSystemNotification(models.SystemNotification)
crud_system_notification_view_log = CRUDSystemNotificationViewLog(models.SystemNotificationViewLog)
crud_trial_registration = CRUDTrialRegistration(models.TrialRegistration)
crud_legal_artifact = CRUDLegalArtifact(models.LegalArtifact)

crud_lg_migration = CRUDLGMigration(models.LGMigrationStaging)

# NEW INSTANTIATIONS
crud_migration_batch = CRUDMigrationBatch(models.MigrationBatch)
crud_lg_change_log = CRUDLGChangeLog(models.LGChangeLog)
migration_history_service = MigrationHistoryService()

# NEW CRUDS for legal artifacts
crud_legal_artifact = CRUDLegalArtifact(models.LegalArtifact)
crud_user_legal_acceptance = CRUDUserLegalAcceptance(models.UserLegalAcceptance)


__all__ = [ 
    "CRUDBase", 
    "log_action", 
    "crud_audit_log", 
    "crud_global_configuration", 
    "crud_customer_configuration", 
    "crud_customer", 
    "crud_customer_entity", 
    "crud_customer_email_setting", 
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
    "crud_permission", 
    "crud_role_permission", 
    "crud_subscription_plan", 
    "crud_user", 
    "crud_approval_request", 
    "crud_lg_owner", 
    "crud_reports",
    "crud_system_notification",
    "crud_system_notification_view_log",
    "crud_lg_cancellation",
    "crud_lg_migration",
    # NEW EXPORTS
    "crud_migration_batch",
    "crud_lg_change_log",
    "migration_history_service",
    "crud_legal_artifact", # NEW EXPORT
    "crud_user_legal_acceptance", # NEW EXPORT
    "crud_trial_registration",
]