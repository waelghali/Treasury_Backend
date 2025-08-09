# app/schemas/all_schemas.py
from pydantic import BaseModel, EmailStr, Field, model_validator, computed_field
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from enum import Enum
from uuid import UUID
import os
import logging
from decimal import Decimal
import re

logger = logging.getLogger(__name__) # Get logger instance for the module

from app.constants import UserRole, GlobalConfigKey, ApprovalRequestStatusEnum, LgStatusEnum, LgTypeEnum, LgOperationalStatusEnum, SubscriptionStatus

class BaseSchema(BaseModel):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    is_deleted: bool
    deleted_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class SubscriptionPlanBase(BaseModel):
    name: str = Field(..., min_length=3, max_length=100, description="Name of the subscription plan")
    description: Optional[str] = Field(None, max_length=500, description="Description of the subscription plan")
    duration_months: int = Field(..., ge=1, description="Duration of the plan in months (e.g., 1 for monthly, 12 for annual)")
    monthly_price: float = Field(..., ge=0, description="Monthly price of the plan")
    annual_price: float = Field(..., ge=0, description="Annual price of the plan (should be less than monthly_price * duration_months)")
    max_users: int = Field(..., ge=1, description="Maximum number of users allowed under this plan")
    max_records: int = Field(..., ge=0, description="Maximum number of records (e.g., LGs) allowed under this plan")
    can_maker_checker: bool = Field(False, description="Enables maker-checker workflow for customers on this plan")
    can_multi_entity: bool = Field(False, description="Enables multi-entity support for customers on this plan")
    can_ai_integration: bool = Field(False, description="Enables AI integration features for customers on this plan")
    can_image_storage: bool = Field(False, description="Enables image storage features for customers on this plan")
    # NEW: Grace period in days
    grace_period_days: int = Field(30, ge=0, description="Grace period in days after subscription end date.")

class SubscriptionPlanCreate(SubscriptionPlanBase):
    pass

class SubscriptionPlanUpdate(SubscriptionPlanBase):
    name: Optional[str] = Field(None, min_length=3, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    duration_months: Optional[int] = Field(None, ge=1)
    monthly_price: Optional[float] = Field(None, ge=0)
    annual_price: Optional[float] = Field(None, ge=0)
    max_users: Optional[int] = Field(None, ge=1)
    max_records: Optional[int] = Field(None, ge=0)
    can_maker_checker: Optional[bool] = None
    can_multi_entity: Optional[bool] = None
    can_ai_integration: Optional[bool] = None
    can_image_storage: Optional[bool] = None
    # NEW: Grace period update
    grace_period_days: Optional[int] = Field(None, ge=0)

class SubscriptionPlanOut(SubscriptionPlanBase, BaseSchema):
    pass

class CustomerEntityBase(BaseModel):
    entity_name: str = Field(..., min_length=1, max_length=100, description="Name of the customer entity")
    # Updated: code is now mandatory and fixed length 4 in the models, reflected in schemas
    address: Optional[str] = Field(None, max_length=250, description="The physical address of the customer entity")
    commercial_register_number: Optional[str] = Field(None, max_length=50, description="The commercial register number of the entity")
    tax_id: Optional[str] = Field(None, max_length=50, description="The tax identification number of the entity")
    code: str = Field(..., min_length=4, max_length=4, pattern=r"^[A-Z0-9]{4}$", description="Unique 4-character code for the entity, used in LG instruction serialization")
    contact_person: Optional[str] = Field(None, max_length=100, description="Main contact person for this entity")
    contact_email: Optional[EmailStr] = Field(None, description="Contact email for this entity")
    is_active: bool = Field(True, description="Whether the entity is currently active")

class CustomerEntityCreate(CustomerEntityBase):
    pass

class CustomerEntityUpdate(CustomerEntityBase):
    entity_name: Optional[str] = Field(None, min_length=1, max_length=100)
    # Updated: code field for update
    code: Optional[str] = Field(None, min_length=4, max_length=4, pattern=r"^[A-Z0-9]{4}$", description="Unique 4-character code for the entity, used in LG instruction serialization (optional on creation, auto-generated)")
    contact_person: Optional[str] = Field(None, max_length=100)
    contact_email: Optional[EmailStr] = None
    is_active: Optional[bool] = None

class CustomerEntityOut(CustomerEntityBase, BaseSchema):
    customer_id: int

class LoginRequest(BaseModel):
    email: EmailStr = Field(..., description="User's email address")
    password: str = Field(..., description="User's password")

class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., description="User's current password")
    new_password: str = Field(..., min_length=8, description="User's new password, must be at least 8 characters long.")
    confirm_new_password: str = Field(..., description="Confirmation of the new password.")

    @model_validator(mode='after')
    def passwords_match(self) -> 'ChangePasswordRequest':
        if self.new_password != self.confirm_new_password:
            raise ValueError("New password and confirmation do not match.")
        return self

    @model_validator(mode='after')
    def validate_password_policy(self) -> 'ChangePasswordRequest':
        # These values will ideally come from GlobalConfiguration,
        # but for schema validation, we can assume typical defaults or fetch from a config service.
        min_length = 8 # Default, to be overridden by config
        require_uppercase = True # Default, to be overridden by config
        require_lowercase = True # Default, to be overridden by config
        require_digit = True # Default, to be overridden by config

        if len(self.new_password) < min_length:
            raise ValueError(f"Password must be at least {min_length} characters long.")
        if require_uppercase and not re.search(r'[A-Z]', self.new_password):
            raise ValueError("Password must contain at least one uppercase letter.")
        if require_lowercase and not re.search(r'[a-z]', self.new_password):
            raise ValueError("Password must contain at least one lowercase letter.")
        if require_digit and not re.search(r'\d', self.new_password):
            raise ValueError("Password must contain at least one digit.")

        return self

class ForgotPasswordRequest(BaseModel):
    email: EmailStr = Field(..., description="Email address associated with the account to reset.")

class ResetPasswordRequest(BaseModel):
    token: str = Field(..., description="Password reset token received via email.")
    new_password: str = Field(..., min_length=8, description="New password for the account.")
    confirm_new_password: str = Field(..., description="Confirmation of the new password.")

    @model_validator(mode='after')
    def passwords_match(self) -> 'ResetPasswordRequest':
        if self.new_password != self.confirm_new_password:
            raise ValueError("New password and confirmation do not match.")
        return self

    @model_validator(mode='after')
    def validate_password_policy(self) -> 'ResetPasswordRequest':
        # Same password policy validation as ChangePasswordRequest
        min_length = 8
        require_uppercase = True
        require_lowercase = True
        require_digit = True

        if len(self.new_password) < min_length:
            raise ValueError(f"Password must be at least {min_length} characters long.")
        if require_uppercase and not re.search(r'[A-Z]', self.new_password):
            raise ValueError("Password must contain at least one uppercase letter.")
        if require_lowercase and not re.search(r'[a-z]', self.new_password):
            raise ValueError("Password must contain at least one lowercase letter.")
        if require_digit and not re.search(r'\d', self.new_password):
            raise ValueError("Password must contain at least one digit.")

        return self

class UserAccountOut(BaseModel):
    id: int
    email: EmailStr
    role: UserRole
    customer_id: Optional[int] = None
    has_all_entity_access: bool
    must_change_password: bool
    permissions: List[str] = Field([], description="List of permission names associated with the user's role.")
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True # Allow mapping from ORM models

class AdminUserUpdate(BaseModel):
    new_password: str = Field(..., min_length=8, description="New password for the user.")
    confirm_new_password: str = Field(..., description="Confirmation of the new password.")
    force_change_on_next_login: bool = Field(True, description="If true, user must change password on next login.")

    @model_validator(mode='after')
    def passwords_match(self) -> 'AdminUserUpdate':
        if self.new_password != self.confirm_new_password:
            raise ValueError("New password and confirmation do not match.")
        return self

    @model_validator(mode='after')
    def validate_password_policy(self) -> 'AdminUserUpdate':
        # Same password policy validation as ChangePasswordRequest
        min_length = 8
        require_uppercase = True
        require_lowercase = True
        require_digit = True

        if len(self.new_password) < min_length:
            raise ValueError(f"Password must be at least {min_length} characters long.")
        if require_uppercase and not re.search(r'[A-Z]', self.new_password):
            raise ValueError("Password must contain at least one uppercase letter.")
        if require_lowercase and not re.search(r'[a-z]', self.new_password):
            raise ValueError("Password must contain at least one lowercase letter.")
        if require_digit and not re.search(r'\d', self.new_password):
            raise ValueError("Password must contain at least one digit.")

        return self

class UserBase(BaseModel):
    email: EmailStr = Field(..., description="User's email address")
    role: UserRole = Field(UserRole.END_USER, description="Role of the user within the system")

class UserCreate(UserBase):
    password: str = Field(..., min_length=8, description="User's password")
    customer_id: Optional[int] = Field(None, description="Customer ID for the user (required for non-SYSTEM_OWNER)")
    has_all_entity_access: bool = Field(True, description="True if user has access to all entities under their customer, False if restricted to specific entities")
    entity_ids: Optional[List[int]] = Field(None, description="List of customer entity IDs this user has access to (if has_all_entity_access is False)")
    must_change_password: bool = Field(True, description="True if user must change password on next login")

    @model_validator(mode='after')
    def validate_entity_access_and_customer_id(self):
        if self.has_all_entity_access and self.entity_ids:
            raise ValueError("Cannot provide specific entity_ids when has_all_entity_access is True.")
        if not self.has_all_entity_access and not self.entity_ids:
            raise ValueError("Must provide specific entity_ids when has_all_entity_access is False.")
        return self

class UserUpdate(UserBase):
    email: Optional[EmailStr] = Field(None, description="User's email address")
    role: Optional[UserRole] = Field(None, description="Role of the user within the system")
    password: Optional[str] = Field(None, min_length=8, description="User's new password (optional for update)")
    customer_id: Optional[int] = Field(None, description="Customer ID for the user (required for non-SYSTEM_OWNER)")
    has_all_entity_access: Optional[bool] = Field(None, description="True if user has access to all entities under their customer, False if restricted to specific entities")
    entity_ids: Optional[List[int]] = Field(None, description="List of customer entity IDs this user has access to (if has_all_entity_access is False)")
    must_change_password: Optional[bool] = Field(None, description="True if user must change password on next login")

    @model_validator(mode='after')
    def validate_entity_access(self):
        if self.has_all_entity_access is not None:
            if self.has_all_entity_access and self.entity_ids:
                raise ValueError("Cannot provide specific entity_ids when has_all_entity_access is True.")
            if not self.has_all_entity_access and not self.entity_ids:
                raise ValueError("Must provide specific entity_ids when has_all_entity_access is False.")
        return self

class UserCreateCorporateAdmin(UserBase):
    password: str = Field(..., min_length=8, description="User's password")
    role: UserRole = Field(..., description="Role of the user within the system (cannot be SYSTEM_OWNER)")
    has_all_entity_access: bool = Field(True, description="True if user has access to all entities under their customer, False if restricted to specific entities")
    entity_ids: Optional[List[int]] = Field(None, description="List of customer entity IDs this user has access to (if has_all_entity_access is False)")
    must_change_password: bool = Field(True, description="True if user must change password on next login")

    @model_validator(mode='after')
    def validate_corporate_admin_create(self):
        if self.role == UserRole.SYSTEM_OWNER:
            raise ValueError("Corporate Admins cannot create users with 'SYSTEM_OWNER' role.")
        if self.has_all_entity_access and self.entity_ids:
            raise ValueError("Cannot provide specific entity_ids when has_all_entity_access is True.")
        if not self.has_all_entity_access and not self.entity_ids:
            raise ValueError("Must provide specific entity_ids when has_all_entity_access is False.")
        return self

class UserUpdateCorporateAdmin(UserBase):
    email: Optional[EmailStr] = Field(None, description="User's email address")
    role: Optional[UserRole] = Field(None, description="Role of the user within the system (cannot be SYSTEM_OWNER)")
    password: Optional[str] = Field(None, min_length=8, description="User's new password (optional for update)")
    has_all_entity_access: Optional[bool] = Field(None, description="True if user has access to all entities under their customer, False if restricted to specific entities")
    entity_ids: Optional[List[int]] = Field(None, description="List of customer entity IDs this user has access to (if has_all_entity_access is False)")
    must_change_password: Optional[bool] = Field(None, description="True if user must change password on next login")

    @model_validator(mode='after')
    def validate_entity_access(self):
        if self.has_all_entity_access is not None:
            if self.has_all_entity_access and self.entity_ids:
                raise ValueError("Cannot provide specific entity_ids when has_all_entity_access is True.")
            if not self.has_all_entity_access and not self.entity_ids:
                raise ValueError("Must provide specific entity_ids when has_all_entity_access is False.")
        return self

class UserOut(UserBase, BaseSchema):
    customer_id: Optional[int] = None
    has_all_entity_access: bool
    must_change_password: bool
    entities_with_access: List[CustomerEntityOut] = []

    maker_user_email: Optional[EmailStr] = None
    checker_user_email: Optional[EmailStr] = None

class CustomerCoreCreate(BaseModel):
    name: str = Field(..., min_length=3, max_length=150, description="Customer organization name")
    address: Optional[str] = Field(None, max_length=250, description="Customer organization address")
    contact_email: EmailStr = Field(..., description="Primary contact email for the customer")
    contact_phone: Optional[str] = Field(None, max_length=50, description="Primary contact phone for the customer")
    subscription_plan_id: int = Field(..., description="ID of the subscription plan assigned to this customer")

class CustomerBase(BaseModel):
    name: str = Field(..., min_length=3, max_length=150, description="Customer organization name")
    address: Optional[str] = Field(None, max_length=250, description="Customer organization address")
    contact_email: EmailStr = Field(..., description="Primary contact email for the customer")
    contact_phone: Optional[str] = Field(None, max_length=50, description="Primary contact phone for the customer")
    subscription_plan_id: int = Field(..., description="ID of the subscription plan assigned to this customer")
    # NEW: Add subscription lifecycle fields
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: Optional[SubscriptionStatus] = None

class CustomerCreate(CustomerBase):
    # Remove subscription fields from creation schema as they are set by the backend
    start_date: Any = Field(None, exclude=True)
    end_date: Any = Field(None, exclude=True)
    status: Any = Field(None, exclude=True)
    initial_corporate_admin: UserCreate = Field(..., description="Details for the initial Corporate Admin user")
    initial_entities: Optional[List[CustomerEntityCreate]] = Field([], description="Optional list of initial entities for the customer")

class CustomerUpdate(CustomerBase):
    name: Optional[str] = Field(None, min_length=3, max_length=150)
    address: Optional[str] = Field(None, max_length=250)
    contact_email: Optional[EmailStr] = None
    contact_phone: Optional[str] = Field(None, max_length=50)
    subscription_plan_id: Optional[int] = None
    # NEW: Prevent updating these fields via API as they are managed by the system
    start_date: Any = Field(None, exclude=True)
    end_date: Any = Field(None, exclude=True)
    status: Any = Field(None, exclude=True)

class CustomerOut(CustomerBase, BaseSchema):
    subscription_plan: SubscriptionPlanOut
    entities: List[CustomerEntityOut] = []
    users: List[UserOut] = []
    customer_email_settings: Optional["CustomerEmailSettingOut"] = None
    templates: List["TemplateOut"] = []
    # NEW: Expose subscription lifecycle fields
    start_date: datetime
    end_date: datetime
    status: SubscriptionStatus

class GlobalConfigurationBase(BaseModel):
    key: GlobalConfigKey = Field(..., description="Unique key for the configuration setting")
    value_min: Optional[str] = Field(None, description="Minimum allowed value for this setting (as string)")
    value_max: Optional[str] = Field(None, description="Maximum allowed value for this setting (as string)")
    value_default: Optional[str] = Field(None, description="Default value for this setting (as string)")
    unit: Optional[str] = Field(None, max_length=50, description="Unit of the setting (e.g., 'days', 'percentage', 'boolean')")
    description: Optional[str] = Field(None, max_length=500, description="Description of the configuration setting")

class GlobalConfigurationCreate(GlobalConfigurationBase):
    pass

class GlobalConfigurationUpdate(GlobalConfigurationBase):
    key: Optional[GlobalConfigKey] = Field(None, description="Unique key for the configuration setting")
    value_min: Optional[str] = None
    value_max: Optional[str] = None
    value_default: Optional[str] = None
    unit: Optional[str] = None
    description: Optional[str] = None

class GlobalConfigurationOut(GlobalConfigurationBase, BaseSchema):
    pass

class CustomerConfigurationBase(BaseModel):
    global_config_id: int = Field(..., description="ID of the GlobalConfiguration being overridden")
    configured_value: str = Field(..., description="The value set by the customer, overriding the global default")

class CustomerConfigurationCreate(CustomerConfigurationBase):
    pass

class CustomerConfigurationUpdate(BaseModel):
    configured_value: str = Field(..., description="The new value for the customer's configuration")

class CustomerConfigurationOut(BaseSchema):
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    is_deleted: Optional[bool] = None
    deleted_at: Optional[datetime] = None
    
    customer_id: int
    global_config_id: int
    configured_value: Optional[str] = None
    
    global_config_key: GlobalConfigKey = Field(..., description="Key of the global configuration")
    global_value_min: Optional[str] = Field(None, description="Minimum allowed value from global config")
    global_value_max: Optional[str] = Field(None, description="Maximum allowed value from global config")
    global_value_default: Optional[str] = Field(None, description="Default value from global config")
    global_unit: Optional[str] = Field(None, description="Unit from global config")
    global_description: Optional[str] = Field(None, description="Description from global config")
    
    effective_value: str = Field(..., description="The effective value for the customer (configured_value if present, else global_value_default)")

class BankBase(BaseModel):
    name: str = Field(..., min_length=3, max_length=150, description="Name of the bank")
    address: Optional[str] = Field(None, max_length=250, description="Address of the bank")
    phone_number: Optional[str] = Field(None, max_length=50, description="Phone number of the bank")
    fax: Optional[str] = Field(None, max_length=50, description="Fax number of the bank")
    former_names: Optional[List[str]] = Field(None, description="Former names of the bank (list of strings)")
    swift_code: Optional[str] = Field(None, min_length=8, max_length=11, description="SWIFT/BIC code of the bank")
    short_name: Optional[str] = Field(None, max_length=50, description="Short name of the bank")

class BankCreate(BankBase):
    pass

class BankUpdate(BankBase):
    name: Optional[str] = Field(None, min_length=3, max_length=150)
    address: Optional[str] = None
    phone_number: Optional[str] = None
    fax: Optional[str] = None
    former_names: Optional[List[str]] = None
    swift_code: Optional[str] = Field(None, min_length=8, max_length=11)
    short_name: Optional[str] = None

class BankOut(BankBase, BaseSchema):
    pass

class TemplateBase(BaseModel):
    name: str = Field(..., min_length=3, max_length=100, description="Name of the template")
    template_type: str = Field(..., description="Type of template (e.g., 'EMAIL', 'LETTER', 'PDF_ATTACHMENT')")
    action_type: str = Field(..., description="Action it relates to (e.g., 'LG_EXTENSION', 'LG_RELEASE')")
    content: str = Field(..., description="The actual content of the template (e.g., HTML for email, Markdown for letter)")
    is_global: bool = Field(True, description="True if universal, False if customer-specific")
    customer_id: Optional[int] = Field(None, description="Null if is_global is True, links to Customer if customer-specific")
    is_notification_template: bool = Field(False, description="True if this template is for email notifications.")
    is_default: bool = Field(False, description="True if this is the default template for its scope and action type.")
    subject: Optional[str] = Field(None, description="Subject line of the template (for emails).")
    
    @model_validator(mode='after')
    def validate_global_customer_id(self):
        if self.is_global and self.customer_id is not None:
            raise ValueError("customer_id must be null if is_global is true.")
        if not self.is_global and self.customer_id is None:
            raise ValueError("customer_id must be provided if is_global is false (customer-specific template).")
        return self

class TemplateCreate(TemplateBase):
    pass

class TemplateUpdate(TemplateBase):
    name: Optional[str] = Field(None, min_length=3, max_length=100)
    template_type: Optional[str] = None
    action_type: Optional[str] = None
    content: Optional[str] = None
    is_global: Optional[bool] = None
    customer_id: Optional[int] = None
    is_notification_template: Optional[bool] = None
    is_default: Optional[bool] = None
    subject: Optional[str] = None

class TemplateOut(TemplateBase, BaseSchema):
    customer_name: Optional[str] = None

class CurrencyBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Full name of the currency")
    iso_code: str = Field(..., min_length=3, max_length=3, description="ISO 4217 currency code (e.g., 'USD')")
    symbol: Optional[str] = Field(None, max_length=10, description="Currency symbol (e.g., '$')")

class CurrencyCreate(CurrencyBase):
    pass

class CurrencyUpdate(CurrencyBase):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    iso_code: Optional[str] = Field(None, min_length=3, max_length=3)
    symbol: Optional[str] = None

class CurrencyOut(CurrencyBase, BaseSchema):
    pass

class LgTypeBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Name of the LG Type")
    description: Optional[str] = Field(None, max_length=250, description="Description of the LG Type")

class LgTypeCreate(LgTypeBase):
    pass

class LgTypeUpdate(LgTypeBase):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None

class LgTypeOut(LgTypeBase, BaseSchema):
    pass

class RuleBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Name of the rule set")
    description: Optional[str] = Field(None, max_length=250, description="Description of the rule set")

class RuleCreate(RuleBase):
    pass

class RuleUpdate(RuleBase):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None

class RuleOut(RuleBase, BaseSchema):
    pass

class IssuingMethodBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Name of the issuing method")
    description: Optional[str] = Field(None, max_length=250, description="Description of the issuing method")

class IssuingMethodCreate(IssuingMethodBase):
    pass

class IssuingMethodUpdate(IssuingMethodBase):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None

class IssuingMethodOut(IssuingMethodBase, BaseSchema):
    pass

class LgStatusBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Name of the LG Status")
    description: Optional[str] = Field(None, max_length=250, description="Description of the LG Status")

class LgStatusCreate(LgStatusBase):
    pass

class LgStatusUpdate(LgStatusBase):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None

class LgStatusOut(LgStatusBase, BaseSchema):
    pass

class LgOperationalStatusBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Name of the LG Operational Status")
    description: Optional[str] = Field(None, max_length=250, description="Description of the LG Operational Status")

class LgOperationalStatusCreate(LgOperationalStatusBase):
    pass

class LgOperationalStatusUpdate(LgOperationalStatusBase):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None

class LgOperationalStatusOut(LgOperationalStatusBase, BaseSchema):
    pass

class UniversalCategoryBase(BaseModel):
    category_name: str = Field(..., min_length=1, max_length=100, description="Name of the universal category")
    code: Optional[str] = Field(None, max_length=2, description="Unique 1-2 character code for the universal category")
    extra_field_name: Optional[str] = Field(None, max_length=100, description="Name for an optional extra field (e.g., 'Project Code')")
    is_mandatory: Optional[bool] = Field(None, description="Whether the extra field is mandatory for LGs in this category")
    communication_list: Optional[List[str]] = Field(None, description="List of email addresses for communication (JSON array of strings)")

class UniversalCategoryCreate(UniversalCategoryBase):
    pass

class UniversalCategoryUpdate(UniversalCategoryBase):
    category_name: Optional[str] = Field(None, min_length=1, max_length=100)
    code: Optional[str] = Field(None, max_length=2)
    extra_field_name: Optional[str] = None
    is_mandatory: Optional[bool] = None
    communication_list: Optional[List[str]] = None

class UniversalCategoryOut(UniversalCategoryBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class LGCategoryBase(BaseModel):
    category_name: str = Field(..., min_length=1, max_length=100, description="Name of the LG category specific to a customer")
    code: str = Field(..., min_length=1, max_length=2, description="Unique 1-2 character code for the category, used in LG serialization")
    extra_field_name: Optional[str] = Field(None, max_length=100, description="Name for an optional extra field for LGs in this category")
    is_mandatory: bool = Field(False, description="Whether the extra field is mandatory for LGs in this category")
    communication_list: Optional[List[EmailStr]] = Field(None, description="List of email addresses for communication specific to this category")
    
    has_all_entity_access: bool = Field(True, description="True if category applies to all entities under their customer, False if restricted to specific entities")
    entity_ids: Optional[List[int]] = Field(None, description="List of customer entity IDs this category applies to (if has_all_entity_access is False)")

    @model_validator(mode='after')
    def validate_code_format(self):
        if self.code and (not self.code.isalnum() or not (1 <= len(self.code) <= 2)):
            raise ValueError("Category code must be 1 or 2 alphanumeric characters.")
        if self.code:
            self.code = self.code.upper()
        return self

    @model_validator(mode='after')
    def validate_entity_access(self):
        if self.has_all_entity_access and self.entity_ids:
            raise ValueError("Cannot provide specific entity_ids when has_all_entity_access is True.")
        if not self.has_all_entity_access and not self.entity_ids:
            raise ValueError("Must provide specific entity_ids when has_all_entity_access is False.")
        return self

class LGCategoryCreate(LGCategoryBase):
    pass

class LGCategoryUpdate(BaseModel):
    category_name: Optional[str] = Field(None, min_length=1, max_length=100)
    code: Optional[str] = Field(None, min_length=1, max_length=2)
    extra_field_name: Optional[str] = None
    is_mandatory: Optional[bool] = None
    communication_list: Optional[List[EmailStr]] = None
    
    has_all_entity_access: Optional[bool] = Field(None, description="True if category applies to all entities under their customer, False if restricted to specific entities")
    entity_ids: Optional[List[int]] = Field(None, description="List of customer entity IDs this category applies to (if has_all_entity_access is False)")

    @model_validator(mode='after')
    def validate_code_format(self):
        if self.code:
            if not self.code.isalnum() or not (1 <= len(self.code) <= 2):
                raise ValueError("Category code must be 1 or 2 alphanumeric characters.")
            self.code = self.code.upper()
        return self

    @model_validator(mode='after')
    def validate_entity_access(self):
        if self.has_all_entity_access is not None:
            if self.has_all_entity_access and self.entity_ids:
                raise ValueError("Cannot provide specific entity_ids when has_all_entity_access is True.")
            if not self.has_all_entity_access and not self.entity_ids:
                raise ValueError("Must provide specific entity_ids when has_all_entity_access is False.")
        return self

class LGCategoryOut(BaseModel):
    id: int
    category_name: str
    code: Optional[str] = None
    extra_field_name: Optional[str] = None
    is_mandatory: bool = False
    communication_list: Optional[List[EmailStr]] = None
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    is_deleted: bool = False
    created_at: datetime
    updated_at: Optional[datetime] = None
    type: Optional[str] = None
    
    has_all_entity_access: bool
    entities_with_access: List[CustomerEntityOut] = []

    class Config:
        from_attributes = True

class InternalOwnerContactBase(BaseModel):
    email: EmailStr = Field(..., description="Email of the internal owner contact person")
    phone_number: str = Field(..., description="Phone number of the internal owner")
    internal_id: Optional[str] = Field(None, max_length=10, description="Optional internal ID for the owner")
    manager_email: EmailStr = Field(..., description="Manager's email of the internal owner")

class InternalOwnerContactCreate(InternalOwnerContactBase):
    pass

class InternalOwnerContactOut(InternalOwnerContactBase, BaseSchema):
    customer_id: int
    # NEW: Add the dynamically computed fields to the schema so Pydantic will include them
    owned_lgs_count: int = Field(0, description="The number of active LG records assigned to this owner")
    owned_lgs_total_count: int = Field(0, description="The total number of non-deleted LG records assigned to this owner")

class PermissionBase(BaseModel):
    name: str = Field(..., description="Unique name of the permission (e.g., 'customer:create')")
    description: Optional[str] = Field(None, description="Description of what the permission allows")

class PermissionCreate(PermissionBase):
    pass

class PermissionOut(BaseModel):
    id: int
    name: str
    description: Optional[str] = None

    class Config:
        from_attributes = True

class RolePermissionBase(BaseModel):
    role: str = Field(..., description="The role name (e.g., 'system_owner')")
    permission_id: int = Field(..., description="ID of the permission granted to the role")

class RolePermissionCreate(RolePermissionBase):
    pass

class RolePermissionOut(RolePermissionBase):
    id: int
    permission: PermissionOut

    class Config:
        from_attributes = True

class ApprovalRequestBase(BaseModel):
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    entity_type: str = Field(..., description="Type of entity requiring approval (e.g., 'LGRecord', 'User', 'InternalOwnerContact')")
    entity_id: Optional[int] = Field(None, description="ID of the entity requiring approval (can be null for bulk/general actions)")
    action_type: str = Field(..., description="Type of action requiring approval (e.g., 'LG_AMEND', 'USER_CREATE')")
    status: ApprovalRequestStatusEnum = Field(ApprovalRequestStatusEnum.PENDING, description="Status of the approval request (PENDING, APPROVED, REJECTED, AUTO_REJECTED_EXPIRED, INVALIDATED_BY_APPROVAL, WITHDRAWN)")
    maker_user_id: int = Field(..., description="ID of the user who initiated the action (Maker)")
    checker_user_id: Optional[int] = Field(None, description="ID of the user who approved/rejected the action (Checker)")
    request_details: Optional[Dict[str, Any]] = Field(None, description="JSON object with details of the requested change")
    lg_record_snapshot: Optional[Dict[str, Any]] = Field(None, description="Snapshot of LGRecord's critical fields at time of request submission (JSON). Reused for other entity snapshots.")
    customer_id: int = Field(..., description="ID of the customer this approval request belongs to")
    reason: Optional[str] = Field(None, description="Reason for rejection or auto-rejection/invalidation")
    withdrawn_at: Optional[datetime] = None 

    class Config:
        from_attributes = True

class ApprovalRequestCreate(ApprovalRequestBase):
    id: Any = Field(None, exclude=True)
    created_at: Any = Field(None, exclude=True)
    updated_at: Any = Field(None, exclude=True)
    status: Any = Field(None, exclude=True)
    maker_user_id: Any = Field(None, exclude=True)
    customer_id: Any = Field(None, exclude=True)

class ApprovalRequestUpdate(BaseModel):
    status: ApprovalRequestStatusEnum = Field(..., description="New status for the approval request (APPROVED, REJECTED)")
    checker_user_id: int = Field(..., description="ID of the user who is approving/rejection")
    reason: Optional[str] = Field(None, description="Reason for rejection")

class AuditLogBase(BaseModel):
    user_id: Optional[int] = Field(None, description="ID of the user who performed the action")
    action_type: str = Field(..., description="Type of action (e.g., CREATE, UPDATE, DELETE, LOGIN)")
    entity_type: str = Field(..., description="Type of entity affected (e.g., Customer, User)")
    entity_id: Optional[int] = Field(None, description="ID of the entity affected")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details about the action (JSON)")
    ip_address: Optional[str] = Field(None, description="IP address from where the action was performed")
    customer_id: Optional[int] = Field(None, description="ID of the customer associated with the action/entity (for filtering)")
    lg_record_id: Optional[int] = Field(None, description="ID of the LG record associated with the action (for LG lifecycle tracking)")

class AuditLogCreate(AuditLogBase):
    pass

class AuditLogOut(BaseModel):
    id: int

    user_id: Optional[int]
    action_type: str
    entity_type: str
    entity_id: Optional[int]
    details: Optional[Dict[str, Any]]
    timestamp: datetime
    ip_address: Optional[str]
    customer_id: Optional[int] = None
    lg_record_id: Optional[int] = None

    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class TokenData(BaseModel):
    email: Optional[EmailStr] = None
    user_id: Optional[int] = None
    role: Optional[UserRole] = None
    permissions: List[str] = Field([], description="List of permission names associated with the user's role.")
    customer_id: Optional[int] = Field(None, description="NEW: Include customer_id in token data")
    has_all_entity_access: Optional[bool] = Field(True, description="True if user has access to all entities under their customer, False if restricted to specific entities")
    entity_ids: List[int] = Field([], description="List of customer entity IDs this user has access to.")
    must_change_password: Optional[bool] = Field(False, description="True if user must change password on next login.")
    # NEW: Add subscription status to the token payload
    subscription_status: Optional[SubscriptionStatus] = Field(None, description="Current subscription status of the customer.")

class LGDocumentBase(BaseModel):
    document_type: str = Field(..., description="Type of document (e.g., 'AI_SCAN', 'INTERNAL_SUPPORTING', 'BANK_REPLY', 'AMENDMENT_LETTER', 'DELIVERY_PROOF')") # Added 'AMENDMENT_LETTER', 'DELIVERY_PROOF'
    file_name: str = Field(..., description="Original file name of the document")
    file_path: str = Field(..., description="Path or URL to the stored file (e.g., cloud storage URL)")
    mime_type: Optional[str] = Field(None, description="MIME type of the file (e.g., 'application/pdf', 'image/jpeg')")
    lg_instruction_id: Optional[int] = Field(None, description="ID of the LG instruction this document belongs to (e.g., delivery proof, bank reply)")

class LGDocumentCreate(LGDocumentBase):
    pass

class LGDocumentOut(LGDocumentBase, BaseSchema):
    lg_record_id: int
    uploaded_by_user_id: int

    # NEW: Computed field to provide the public URL
    # @computed_field
    # @property
    # def public_file_url(self) -> Optional[str]:
    #     # Ensure os.environ is accessed directly for GCS_BUCKET_NAME within the method
    #     gcs_bucket_name = os.environ.get('GCS_BUCKET_NAME')
    #
    #     if self.file_path and self.file_path.startswith("gs://"):
    #         if not gcs_bucket_name:
    #             logger.warning("GCS_BUCKET_NAME not set in environment. Cannot generate public URL for %s", self.file_path)
    #             return self.file_path # Fallback to gs:// if bucket name not set
    #
    #         # Extract blob name from gs:// URI
    #         parts = self.file_path[len("gs://"):].split('/', 1)
    #         if len(parts) < 2:
    #             logger.warning(f"Invalid gs_uri format for public URL generation: {self.file_path}")
    #             return self.file_path # Return original if malformed
    #
    #         # Use the actual bucket name from env, and the blob path from the URI
    #         blob_path_in_bucket = parts[1]
    #         return f"https://storage.googleapis.com/{gcs_bucket_name}/{blob_path_in_bucket}"
    #     return self.file_path


class LGRecordBase(BaseModel):
    beneficiary_corporate_id: int = Field(..., description="ID of the entity benefiting from the LG")
    # New: lg_sequence_number for new serial format
    lg_sequence_number: Optional[int] = Field(None, ge=1, description="Unique sequential number for LG within a customer entity (auto-generated for new LGs)")
    issuer_name: str = Field(..., description="Name of the LG issuer (person/company bank guarantees)")
    issuer_id: Optional[str] = Field(None, max_length=15, description="Optional identifier for the issuer")
    lg_number: str = Field(..., max_length=64, description="Unique identifier for the Letter of Guarantee")
    lg_amount: Decimal = Field(..., ge=0, description="Original total amount of the LG (can be 0 for liquidated LGs)")
    lg_currency_id: int = Field(..., description="ID of the currency of the LG amount")
    lg_payable_currency_id: Optional[int] = Field(None, description="ID of the currency in which LG is payable (defaults to LG Currency)")
    issuance_date: date = Field(..., description="Date the LG was issued (DD/MM/YYYY)")
    expiry_date: date = Field(..., description="Date the LG expires")
    auto_renewal: bool = Field(True, description="Indicates if the LG is set to automatically renew")
    # MODIFICATION START: lg_status_id is now Optional for creation, as backend sets default
    lg_status_id: Optional[int] = Field(None, description="Current status of the LG (defaults to 'Valid'). Expected to be set by backend for new LGs.")
    # MODIFICATION END
    lg_operational_status_id: Optional[int] = Field(None, description="ID of LG's operational state (conditional)")
    payment_conditions: Optional[str] = Field(None, max_length=1024, description="Specific conditions related to payment (conditional)")
    description_purpose: str = Field(..., max_length=516, description="General description or purpose of the LG")

    issuing_bank_id: int = Field(..., description="ID of the bank that issued the LG")
    issuing_bank_address: str = Field(..., description="Address of the issuing bank")
    issuing_bank_phone: str = Field(..., description="Phone number of the issuing bank")
    issuing_bank_fax: Optional[str] = Field(None, max_length=18, description="Fax number of the issuing bank")
    issuing_method_id: int = Field(..., description="ID of the method by which LG was issued")
    applicable_rule_id: int = Field(..., description="ID of the set of rules governing the LG")
    applicable_rules_text: Optional[str] = Field(None, max_length=64, description="Free text for rules (conditional)")
    other_conditions: Optional[str] = Field(None, max_length=2048, description="Any other specific conditions not covered elsewhere")

    internal_owner_contact_id: int = Field(..., description="ID of the internal owner contact person")

    lg_category_id: int = Field(..., description="ID of the LG Category for internal classification")
    additional_field_values: Optional[Dict[str, Any]] = Field(None, description="Dynamic fields based on selected LGCategory's extra_field_name (JSONB)")
    internal_contract_project_id: Optional[str] = Field(None, max_length=64, description="Internal reference ID for contract/project")
    notes: Optional[str] = Field(None, max_length=1024, description="Free-form notes related to the LG")

    @model_validator(mode='after')
    def validate_lg_dates(self):
        if self.issuance_date and self.expiry_date:
            if self.expiry_date <= self.issuance_date:
                raise ValueError("Expiry Date must be after Issuance Date.")
        return self

    @model_validator(mode='after')
    def validate_conditional_fields(self):
        return self

class LGRecordCreate(LGRecordBase):
    lg_type_id: int = Field(..., description="ID of the LG Type.")  # Add this line
    internal_owner_email: EmailStr = Field(..., description="Email of the internal owner contact person")
    internal_owner_phone: str = Field(..., description="Phone number of the internal owner")
    internal_owner_id: Optional[str] = Field(None, max_length=10, description="Optional internal ID for the owner")
    manager_email: EmailStr = Field(..., description="Manager's email of the internal owner")

    internal_owner_contact_id: Any = Field(None, exclude=True)
    lg_sequence_number: Any = Field(None, exclude=True) # Exclude lg_sequence_number from create input as it's auto-generated

    ai_scan_file: Optional[LGDocumentCreate] = Field(None, description="Metadata for the AI Scan File (optional)")
    internal_supporting_document_file: Optional[LGDocumentCreate] = Field(None, description="Metadata for the Internal Supporting Document (optional)")

class LGRecordUpdate(BaseModel):
    expiry_date: Optional[date] = Field(None, description="New expiry date for the LG")
    lg_period_months: Optional[int] = Field(None, description="Recalculated duration of the LG in months")
    lg_amount: Optional[float] = Field(None, ge=0, description="New amount for partial liquidation or decrease")
    lg_status_id: Optional[int] = Field(None, description="New status for release/liquidation")
    pass

class LGRecordAmendRequest(BaseModel):
    amendment_details: Dict[str, Any] = Field(
        ...,
        description="Dictionary of LGRecord fields to be amended and their new values. "
                    "Keys should match LGRecord model attributes. Example: {'expiry_date': '2025-12-31', 'lg_amount': 50000.0}"
    )
    ai_extracted_data: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional: Data extracted by AI, presented to the user for review. Not directly used for updating LG."
    )
    reason: Optional[str] = Field(None, description="Reason for amending the LG.")

class LGActivateNonOperativeRequest(BaseModel):
    payment_method: str = Field(..., description="Payment method used (e.g., 'Wire', 'Check')")
    currency_id: int = Field(..., description="ID of the Currency for the payment.")
    amount: float = Field(..., gt=0, description="The payment amount.")
    payment_reference: str = Field(..., max_length=100, description="Wire reference or check number.")
    issuing_bank_id: int = Field(..., description="ID of the Issuing Bank related to the payment.")
    payment_date: date = Field(..., description="Date the payment was made (DD/MM/YYYY).")

    @model_validator(mode='after')
    def validate_payment_date(self):
        if self.payment_date > date.today():
            raise ValueError("Payment date cannot be in the future.")
        return self

class LGRecordRelease(BaseModel):
    reason: Optional[str] = Field(None, description="Reason for releasing the LG (e.g., contract fulfilled).")

class LGRecordLiquidation(BaseModel):
    liquidation_type: str = Field(..., description="Type of liquidation: 'full' or 'partial'.")
    new_amount: Optional[float] = Field(None, description="The new amount of the LG if partial liquidation. Required for partial, ignored for full.")
    reason: Optional[str] = Field(None, description="Reason for liquidating the LG.")

class LGRecordDecreaseAmount(BaseModel):
    decrease_amount: float = Field(..., gt=0, description="The amount to decrease the LG by.")
    reason: Optional[str] = Field(None, description="Reason for decreasing the LG amount.")

class LGInstructionCancelRequest(BaseModel):
    reason: str = Field(
        ...,
        description="Reason for canceling the instruction.",
        min_length=1,
    )
    declaration_confirmed: bool = Field(
        ...,
        description="Confirmation that the user accepts full responsibility for the cancellation.",
    )

    @model_validator(mode='after')
    def validate_declaration_confirmed(self) -> 'LGInstructionCancelRequest':
        if not self.declaration_confirmed:
            raise ValueError("Declaration must be confirmed to proceed with cancellation.")
        return self

class LGInstructionBase(BaseModel):
    lg_record_id: int = Field(..., description="ID of the LG record this instruction pertains to")
    instruction_type: str = Field(..., description="Type of instruction (e.g., 'EXTENSION', 'RELEASE', 'AMENDMENT', 'ACTIVATION')") # Removed 'REMINDER' as it's a sub-type
    serial_number: str = Field(..., description="Unique serial number for the instruction")
    template_id: int = Field(..., description="The template used to generate this instruction")
    status: str = Field("Instruction Issued", description="Current status of the instruction (e.g., 'Instruction Issued', 'Instruction Delivered', 'Confirmed by Bank', 'Reminder Issued', 'Canceled')")
    instruction_date: datetime = Field(None, description="Date the instruction was issued/generated (defaults to now)")
    delivery_date: Optional[datetime] = Field(None, description="Date the instruction was physically delivered to the bank")
    bank_reply_date: Optional[datetime] = Field(None, description="Date the bank's reply was received")
    details: Optional[Dict[str, Any]] = Field(None, description="JSON object for instruction-specific details")
    generated_content_path: Optional[str] = Field(None, description="Path or URL to the generated instruction document")
    sent_to_bank: bool = Field(False, description="Indicates if this instruction has been marked as sent to the bank")
    is_printed: bool = Field(False, description="True if this instruction letter has been printed by a user")
    maker_user_id: int = Field(..., description="User who initiated this instruction (Maker)")
    checker_user_id: Optional[int] = Field(None, description="User who approved this instruction (Checker)")
    approval_request_id: Optional[int] = Field(None, description="Link to related approval request if Maker-Checker is enabled")
    bank_reply_details: Optional[str] = Field(None, description="Detailed text of the bank's reply or notes on it.")

class LGInstructionCreate(LGInstructionBase):
    instruction_date: Optional[datetime] = None
    # --- CRITICAL CHANGE: Make serial_number optional for creation ---
    serial_number: Optional[str] = Field(None, description="Unique serial number for the instruction (generated automatically if None)")
    # --- NEW: Exclude sequence numbers from input payload as they are generated by the backend ---
    global_seq_per_lg: Any = Field(None, exclude=True)
    type_seq_per_lg: Any = Field(None, exclude=True)
    # --- END CRITICAL CHANGE ---

class LGInstructionUpdate(BaseModel):
    status: Optional[str] = None
    delivery_date: Optional[datetime] = None
    bank_reply_date: Optional[str] = None # Change type to string
    sent_to_bank: Optional[bool] = None
    checker_user_id: Optional[int] = None

class LGRecordMinimalOut(BaseSchema):
    lg_number: str
    lg_amount: float
    lg_currency_id: int
    beneficiary_corporate_id: int
    lg_sequence_number: int # New: Expose lg_sequence_number
    issuing_bank_id: int
    lg_type_id: int
    lg_status_id: int
    lg_operational_status_id: Optional[int] = None

    lg_currency: 'CurrencyOut'
    beneficiary_corporate: 'CustomerEntityOut'
    issuing_bank: 'BankOut'
    lg_type: 'LgTypeOut'
    lg_status: 'LgStatusOut'
    lg_operational_status: Optional['LgOperationalStatusOut'] = None

    class Config:
        from_attributes = True

class LGInstructionOut(LGInstructionBase, BaseSchema):
    lg_record_id: int
    lg_record: Optional['LGRecordMinimalOut'] = None
    template: Optional['TemplateOut'] = None
    maker_user: Optional['UserOut'] = None
    checker_user: Optional['UserOut'] = None
    approval_request_id: Optional[int] = None
    approval_request_status: Optional[ApprovalRequestStatusEnum] = None
    documents: List['LGDocumentOut'] = []
    # Expose new sequence numbers for potential UI display or debugging
    global_seq_per_lg: int # NEW field
    type_seq_per_lg: int # NEW field

class LGRecordOut(LGRecordBase, BaseSchema):
    customer_id: int
    lg_period_months: int
    lg_sequence_number: int # New: Expose lg_sequence_number in LGRecordOut

    beneficiary_corporate: 'CustomerEntityOut'
    lg_currency: 'CurrencyOut'
    lg_payable_currency: Optional['CurrencyOut'] = None
    lg_type: 'LgTypeOut'
    lg_status: 'LgStatusOut'
    lg_operational_status: Optional['LgOperationalStatusOut'] = None
    issuing_bank: 'BankOut'
    issuing_method: 'IssuingMethodOut'
    applicable_rule: 'RuleOut'
    internal_owner_contact: 'InternalOwnerContactOut'
    lg_category: 'LGCategoryOut'
    documents: List['LGDocumentOut'] = []
    instructions: List['LGInstructionOut'] = []

class LGRecordToggleAutoRenewalRequest(BaseModel):
    auto_renewal: bool = Field(..., description="The new auto_renewal status (True/False).")
    reason: Optional[str] = Field(None, description="Reason for toggling auto-renewal.")

class ApprovalRequestOut(BaseModel): # Inherits from BaseModel for base fields
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    entity_type: str
    entity_id: Optional[int]
    action_type: str
    status: ApprovalRequestStatusEnum
    maker_user_id: int
    checker_user_id: Optional[int]
    request_details: Optional[Dict[str, Any]]
    lg_record_snapshot: Optional[Dict[str, Any]]
    customer_id: int
    reason: Optional[str]
    withdrawn_at: Optional[datetime]
    internal_owner_contact_id: Optional[int]
    related_instruction_id: Optional[int]

    lg_record: Optional[LGRecordOut] = None
    maker_user: Optional[UserOut] = None
    checker_user: Optional[UserOut] = None
    internal_owner_contact: Optional[InternalOwnerContactOut] = None
    related_instruction: Optional['LGInstructionOut'] = None

    class Config:
        from_attributes = True

class LGLifecycleEventOut(BaseModel):
    id: int
    timestamp: datetime
    action_type: str
    user_email: Optional[EmailStr] = None
    details: Dict[str, Any]

    class Config:
        from_attributes = True

class CustomerEmailSettingBase(BaseModel):
    smtp_host: str = Field(..., description="SMTP server host (e.g., smtp.sendgrid.net)")
    smtp_port: int = Field(..., gt=0, description="SMTP server port (e.g., 587 or 465)")
    smtp_username: str = Field(..., description="SMTP authentication username")
    smtp_password: str = Field(..., description="SMTP password (will be encrypted)")
    sender_email: EmailStr = Field(..., description="Email address to use as the sender (e.g., no-reply@customer.com)")
    sender_display_name: Optional[str] = Field(None, description="Optional display name for the sender (e.g., 'Customer Name Treasury')")
    is_active: bool = Field(True, description="Whether these custom settings are active (False means fallback to global)")

class CustomerEmailSettingCreate(CustomerEmailSettingBase):
    pass

class CustomerEmailSettingUpdate(BaseModel):
    smtp_host: Optional[str] = Field(None, description="SMTP server host")
    smtp_port: Optional[int] = Field(None, gt=0, description="SMTP server port")
    smtp_username: Optional[str] = Field(None, description="SMTP authentication username")
    smtp_password: Optional[str] = Field(None, description="New SMTP password (will be encrypted if provided)")
    sender_email: Optional[EmailStr] = Field(None, description="Email address to use as the sender")
    sender_display_name: Optional[str] = Field(None, description="Optional display name for the sender")
    is_active: Optional[bool] = Field(None, description="Whether these custom settings are active")

class CustomerEmailSettingOut(BaseSchema):
    customer_id: int
    smtp_host: str
    smtp_port: int
    smtp_username: str
    sender_email: EmailStr
    sender_display_name: Optional[str] = None
    is_active: bool

class InternalOwnerChangeScope(str, Enum):
    SINGLE_LG = "single_lg"
    ALL_BY_OLD_OWNER = "all_by_old_owner"

class InternalOwnerContactUpdateDetails(BaseModel):
    email: Optional[EmailStr] = Field(None, description="New email of the internal owner contact person")
    phone_number: Optional[str] = Field(None, description="New phone number of the internal owner")
    internal_id: Optional[str] = Field(None, max_length=10, description="New optional internal ID for the owner")
    manager_email: Optional[EmailStr] = Field(None, description="New manager's email of the internal owner")

class LGRecordChangeOwner(BaseModel):
    change_scope: InternalOwnerChangeScope = Field(..., description="Scope of the change: 'single_lg' or 'all_by_old_owner'.")
    
    lg_record_id: Optional[int] = Field(None, description="ID of the specific LG record to change owner for (required for 'single_lg' scope).")

    old_internal_owner_contact_id: Optional[int] = Field(None, description="ID of the current internal owner contact whose LGs will be reassigned (required for 'all_by_old_owner' scope).")

    new_internal_owner_contact_id: Optional[int] = Field(None, description="ID of an *existing* internal owner contact to assign to (mutually exclusive with new_internal_owner_contact_details).")
    new_internal_owner_contact_details: Optional[InternalOwnerContactCreate] = Field(None, description="Details to create a *new* internal owner contact and assign to (mutually exclusive with new_internal_owner_contact_id).")
    
    reason: Optional[str] = Field(None, description="Reason for changing the LG owner(s).")

    @model_validator(mode='after')
    def validate_change_scope_fields(self):
        if self.change_scope == InternalOwnerChangeScope.SINGLE_LG:
            if self.lg_record_id is None:
                raise ValueError("lg_record_id is required for 'single_lg' change scope.")
            if self.old_internal_owner_contact_id is not None:
                raise ValueError("old_internal_owner_contact_id should not be provided for 'single_lg' change scope.")
        elif self.change_scope == InternalOwnerChangeScope.ALL_BY_OLD_OWNER:
            if self.old_internal_owner_contact_id is None:
                raise ValueError("old_internal_owner_contact_id is required for 'all_by_old_owner' change scope.")
            if self.lg_record_id is not None:
                raise ValueError("lg_record_id should not be provided for 'all_by_old_owner' change scope.")
        
        if (self.new_internal_owner_contact_id is None and self.new_internal_owner_contact_details is None) or \
           (self.new_internal_owner_contact_id is not None and self.new_internal_owner_contact_details is not None):
            raise ValueError("Exactly one of 'new_internal_owner_contact_id' or 'new_internal_owner_contact_details' must be provided.")
        
        return self

print("schemas.py has been loaded and InternalOwnerContactUpdateDetails should be defined.")

class LGInstructionRecordDelivery(BaseModel):
    delivery_date: date = Field(..., description="The date the instruction was physically delivered to the bank.")
    delivery_document_file: Optional[LGDocumentCreate] = Field(None, description="Optional metadata for the document proving delivery.")

    @model_validator(mode='after')
    def validate_delivery_date(self):
        if self.delivery_date > date.today():
            raise ValueError("Delivery date cannot be in the future.")
        return self

class LGInstructionRecordBankReply(BaseModel):
    bank_reply_date: date = Field(..., description="The date the bank's reply was received.")
    reply_details: Optional[str] = Field(None, description="Details or notes from the bank's reply.")
    bank_reply_document_file: Optional[LGDocumentCreate] = Field(None, description="Optional metadata for the document proving the bank's reply.")

    @model_validator(mode='after')
    def validate_bank_reply_date(self):
        if self.bank_reply_date > date.today():
            raise ValueError("Bank reply date cannot be in the future.")
        return self

class AutoRenewalRunSummaryOut(BaseModel):
    renewed_count: int
    message: str
    combined_pdf_base64: Optional[str] = None

# New Schemas for Reporting Module
class ReportFilterBase(BaseModel):
    as_of_date: Optional[date] = Field(None, description="Report data as of this specific date.")
    from_date: Optional[date] = Field(None, description="Start date for data filtering (inclusive).")
    to_date: Optional[date] = Field(None, description="End date for data filtering (inclusive).")
    customer_id: Optional[int] = Field(None, description="Filter by specific customer ID (System Owner only).")
    entity_id: Optional[int] = Field(None, description="Filter by specific customer entity ID.")
    lg_category_id: Optional[int] = Field(None, description="Filter by specific LG category ID.")
    issuing_bank_id: Optional[int] = Field(None, description="Filter by specific issuing bank ID.")
    lg_type_id: Optional[int] = Field(None, description="Filter by specific LG type ID.")
    lg_status_id: Optional[int] = Field(None, description="Filter by specific LG status ID.")
    internal_owner_contact_id: Optional[int] = Field(None, description="Filter by specific internal owner contact ID.")
    user_id: Optional[int] = Field(None, description="Filter by specific user ID (for user activity report).")

    @model_validator(mode='after')
    def validate_date_filters(self):
        if self.from_date and self.to_date and self.from_date > self.to_date:
            raise ValueError("From Date cannot be after To Date.")
        if self.as_of_date and (self.from_date or self.to_date):
            raise ValueError("Cannot use 'As of Date' with 'From Date' or 'To Date'. Choose one date filtering method.")
        return self

# --- NEW: SystemNotification Schemas (for announcement banner) ---
class SystemNotificationBase(BaseModel):
    content: str = Field(..., description="The plain text message of the notification.")
    link: Optional[str] = Field(None, description="Optional URL to attach to the message.")
    start_date: datetime = Field(..., description="The date and time the notification becomes active.")
    end_date: datetime = Field(..., description="The date and time the notification expires.")
    is_active: bool = Field(True, description="Whether the notification is currently active or disabled.")
    target_customer_ids: Optional[List[int]] = Field(None, description="List of customer IDs to target. Null or empty list means all customers.")
    animation_type: Optional[str] = Field(None, description="CSS animation class to apply (e.g., 'fade', 'slide-left')")
    display_frequency: str = Field("once-per-login", description="Frequency of display (e.g., 'once', 'once-per-login', 'repeat')")
    max_display_count: Optional[int] = Field(None, description="Max times to display for a repeating notification")
    target_user_ids: Optional[List[int]] = Field(None, description="List of specific user IDs to target")
    target_roles: Optional[List[str]] = Field(None, description="List of user roles to target")

class SystemNotificationCreate(SystemNotificationBase):
    pass

class SystemNotificationUpdate(SystemNotificationBase):
    content: Optional[str] = None
    link: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    is_active: Optional[bool] = None

class SystemNotificationOut(SystemNotificationBase, BaseSchema):
    created_by_user_id: int
    animation_type: Optional[str]
    display_frequency: str
    max_display_count: Optional[int]
    target_user_ids: Optional[List[int]]
    target_roles: Optional[List[str]]

class SystemNotificationViewLogBase(BaseModel):
    user_id: int
    notification_id: int
    view_count: int = Field(1)

class SystemNotificationViewLogOut(SystemNotificationViewLogBase, BaseSchema):
    pass

# --- NEW: Report Schemas (MINIMALIST) ---
class SystemUsageOverviewReportItemOut(BaseModel):
    total_customers: int
    total_lgs_managed: int
    total_users: int
    total_instructions_issued: int
    total_emails_sent: int

class SystemUsageOverviewReportOut(BaseModel):
    report_date: date
    data: SystemUsageOverviewReportItemOut

class CustomerLGPerformanceReportItemOut(BaseModel):
    lgs_by_status: Dict[str, int]
    instructions_by_type: Dict[str, int]
    total_value_of_active_lgs: Dict[str, Decimal]
    users_with_action_counts: Dict[str, int]

class CustomerLGPerformanceReportOut(BaseModel):
    report_date: date
    data: CustomerLGPerformanceReportItemOut

class MyLGDashboardReportItemOut(BaseModel):
    my_lgs_count: int
    lgs_near_expiry_count: int
    undelivered_instructions_count: int
    recent_actions: List[str]

class MyLGDashboardReportOut(BaseModel):
    report_date: date
    data: MyLGDashboardReportItemOut

LGRecordMinimalOut.model_rebuild()
ApprovalRequestOut.model_rebuild()
LGRecordOut.model_rebuild()
LGInstructionOut.model_rebuild()