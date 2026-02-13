# app/models.py
from __future__ import annotations
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey, UniqueConstraint, Enum as SQLEnum, Text, Index, Numeric, and_, CheckConstraint, Date
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func
from app.database import Base
from app.constants import UserRole, GlobalConfigKey, ApprovalRequestStatusEnum, LgStatusEnum, LgTypeEnum, LgOperationalStatusEnum, DOCUMENT_TYPE_ORIGINAL_LG, SubscriptionStatus, AdvisingStatus
from app.schemas.migration_schemas import MigrationRecordStatusEnum, MigrationTypeEnum
from datetime import datetime

class BaseModel(Base):
    __abstract__ = True

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    is_deleted = Column(Boolean, default=False, index=True)
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    def soft_delete(self):
        self.is_deleted = True
        self.deleted_at = func.now()

    def restore(self):
        self.is_deleted = False
        self.deleted_at = None

# --- ASSOCIATION TABLES ---
class UserCustomerEntityAssociation(Base):
    __tablename__ = 'user_customer_entity_association'
    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)
    customer_entity_id = Column(Integer, ForeignKey('customer_entities.id'), primary_key=True)

    user = relationship("User", back_populates="entity_associations")
    customer_entity = relationship("CustomerEntity", back_populates="users_with_access")

class LGCategoryCustomerEntityAssociation(Base):
    __tablename__ = 'lg_category_customer_entity_association'
    lg_category_id = Column(Integer, ForeignKey('lg_categories.id'), primary_key=True)
    customer_entity_id = Column(Integer, ForeignKey('customer_entities.id'), primary_key=True)

    lg_category = relationship("LGCategory", back_populates="entity_associations")
    customer_entity = relationship("CustomerEntity", back_populates="lg_categories_with_access")


# --- CORE MODELS (alphabetical, with dependencies defined first) ---

class SubscriptionPlan(BaseModel):
    __tablename__ = "subscription_plans"
    name = Column(String, unique=True, index=True, nullable=False, comment="Name of the subscription plan")
    description = Column(String, nullable=True, comment="Description of the subscription plan")
    duration_months = Column(Integer, nullable=False, comment="Duration of the plan in months (e.g., 1 for monthly, 12 for annual)")
    monthly_price = Column(Float, nullable=False, comment="Monthly price of the plan")
    annual_price = Column(Float, nullable=False, comment="Annual price of the plan (should be less than monthly_price * duration_months)")
    max_users = Column(Integer, nullable=False, comment="Maximum number of users allowed under this plan")
    max_records = Column(Integer, nullable=False, comment="Maximum number of records (e.g., LGs) allowed under this plan")
    can_maker_checker = Column(Boolean, default=False, nullable=False, comment="Allows maker-checker workflow")
    can_multi_entity = Column(Boolean, default=False, nullable=False, comment="Allows multiple entities under one customer")
    can_ai_integration = Column(Boolean, default=False, nullable=False, comment="Allows AI integration features")
    can_image_storage = Column(Boolean, default=False, nullable=False, comment="Allows image storage features")
    grace_period_days = Column(Integer, default=30, nullable=False, comment="Grace period in days after subscription end date")

    customers = relationship("Customer", back_populates="subscription_plan")

    def __repr__(self: SubscriptionPlan):
        return f"<SubscriptionPlan(id={self.id}, name='{self.name}', duration_months={self.duration_months})>"

class Customer(BaseModel):
    __tablename__ = "customers"
    name = Column(String, unique=True, index=True, nullable=False, comment="Customer organization name")
    address = Column(String, nullable=True)
    contact_email = Column(String, nullable=False, unique=True, index=True)
    contact_phone = Column(String, nullable=True)
    subscription_plan_id = Column(Integer, ForeignKey("subscription_plans.id"), nullable=False)
    start_date = Column(DateTime(timezone=True), server_default=func.now(), comment="Date the current subscription period started")
    end_date = Column(DateTime(timezone=True), nullable=False, comment="Date the current subscription period ends")
    status = Column(SQLEnum(SubscriptionStatus), default=SubscriptionStatus.ACTIVE, nullable=False, comment="Current subscription status (active, grace, expired)")

    active_user_count = Column(Integer, default=0, nullable=False, comment="Current count of active (non-deleted) users for this customer")
    active_lg_count = Column(Integer, default=0, nullable=False, comment="Current count of active LG records for this customer")

    subscription_plan = relationship("SubscriptionPlan", back_populates="customers")
    entities = relationship("CustomerEntity", back_populates="customer", cascade="all, delete-orphan")
    users = relationship("User", back_populates="customer", cascade="all, delete-orphan")
    lg_categories = relationship("LGCategory", back_populates="customer", cascade="all, delete-orphan")
    internal_owner_contacts = relationship("InternalOwnerContact", back_populates="customer", cascade="all, delete-orphan")
    customer_configurations = relationship("CustomerConfiguration", back_populates="customer", cascade="all, delete-orphan")
    customer_email_settings = relationship("CustomerEmailSetting", back_populates="customer", uselist=False, cascade="all, delete-orphan")
    templates = relationship("Template", back_populates="customer")

    def __repr__(self: Customer):
        return f"<Customer(id={self.id}, name='{self.name}')>"

class CustomerEntity(BaseModel):
    __tablename__ = "customer_entities"
    entity_name = Column(String, nullable=False, comment="Name of the entity under the customer")
    address = Column(String, nullable=True, comment="The physical address of the customer entity")
    commercial_register_number = Column(String, nullable=True, comment="The commercial register number of the entity")
    tax_id = Column(String, nullable=True, comment="The tax identification number of the entity")
    code = Column(String(4), nullable=False, comment="Unique 4-character code for the entity, used in LG instruction serialization")
    contact_person = Column(String, nullable=True, comment="Main contact person for this entity")
    contact_email = Column(String, nullable=True, comment="Contact email for this entity")
    is_active = Column(Boolean, default=True, nullable=False, comment="Whether the entity is currently active")

    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    customer = relationship("Customer", back_populates="entities")

    users_with_access = relationship("UserCustomerEntityAssociation", back_populates="customer_entity")
    lg_categories_with_access = relationship("LGCategoryCustomerEntityAssociation", back_populates="customer_entity")

    __table_args__ = (
        UniqueConstraint('customer_id', 'entity_name', name='_customer_entity_name_uc'),
        UniqueConstraint('customer_id', 'code', name='_customer_entity_code_uc'),
    )

    def __repr__(self: CustomerEntity):
        return f"<CustomerEntity(id={self.id}, name='{self.entity_name}', code='{self.code}', customer_id={self.customer_id})>"


class User(BaseModel):
    __tablename__ = "users"
    email = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)
    role = Column(SQLEnum(UserRole), nullable=False, default=UserRole.END_USER)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)
    customer = relationship("Customer", back_populates="users")

    must_change_password = Column(Boolean, default=True, nullable=False, comment="True if user must change password on next login")
    has_all_entity_access = Column(Boolean, default=True, nullable=False, comment="True if user has access to all entities under their customer, False if restricted to specific entities")
    
    # NEW FIELDS: Account Lockout
    failed_login_attempts = Column(Integer, default=0, nullable=False, comment="Consecutive failed login attempts")
    locked_until = Column(DateTime(timezone=True), nullable=True, comment="Timestamp until which the account is locked")
    
    # NEW FIELD: Tracks the version of the last accepted legal artifacts.
    last_accepted_legal_version = Column(Float, nullable=True, comment="The last version of the legal artifacts (T&C and PP) accepted by the user.")
    
    entity_associations = relationship("UserCustomerEntityAssociation", back_populates="user", cascade="all, delete-orphan")
    entities_with_access = relationship("CustomerEntity", secondary="user_customer_entity_association", viewonly=True)
    password_reset_tokens = relationship("PasswordResetToken", back_populates="user", cascade="all, delete-orphan")
    system_notifications_created = relationship("SystemNotification", back_populates="created_by_user")

    def set_password(self, password: str):
        from app.core.hashing import get_password_hash
        self.password_hash = get_password_hash(password)

    def check_password(self, password: str) -> bool:
        from app.core.hashing import verify_password_direct
        return verify_password_direct(password, self.password_hash)

    def __repr__(self: User):
        return f"<User(id={self.id}, email='{self.email}', role='{self.role}')>"

    mfa_code_hashed = Column(String, nullable=True)       # The hashed 6-digit code
    mfa_code_expires_at = Column(DateTime(timezone=True), nullable=True)
    mfa_attempts = Column(Integer, default=0)             # To prevent brute force

class TrialRegistration(BaseModel): # CHANGE THIS LINE from 'Base' to 'BaseModel'
    __tablename__ = "trial_registrations"
    # id, created_at, updated_at, is_deleted, deleted_at are inherited from BaseModel
    organization_name = Column(String, nullable=False, index=True)
    organization_address = Column(String, nullable=False)
    contact_admin_name = Column(String, nullable=False)
    contact_phone = Column(String, nullable=False)
    admin_email = Column(String, nullable=False, unique=True)
    commercial_register_document_path = Column(String, nullable=True)
    entities_count = Column(String, nullable=False)
    accepted_terms_version = Column(Float, nullable=False)
    accepted_terms_at = Column(DateTime(timezone=True), server_default=func.now())
    accepted_terms_ip = Column(String, nullable=True)
    status = Column(String, nullable=False, default="pending", index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True)
    
    customer = relationship("Customer")

    __table_args__ = (
        Index('idx_trial_registrations_email_status', 'admin_email', 'status'),
    )
    
    def __repr__(self):
        return f"<TrialRegistration(id={self.id}, org_name='{self.organization_name}', status='{self.status}')>"

# NEW MODEL: PasswordResetToken
class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    token_hash = Column(String, nullable=False, unique=True, comment="Hashed token string sent to the user")
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_used = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    user = relationship("User", back_populates="password_reset_tokens")

    __table_args__ = (
        UniqueConstraint('user_id', 'token_hash', name='_user_token_hash_uc'),
        Index('idx_password_reset_tokens_user_id', 'user_id'),
    )

    def __repr__(self: PasswordResetToken):
        return f"<PasswordResetToken(id={self.id}, user_id={self.user_id}, expires_at='{self.expires_at}', is_used={self.is_used})>"


class GlobalConfiguration(BaseModel):
    __tablename__ = "global_configurations"
    key = Column(SQLEnum(GlobalConfigKey), unique=True, index=True, nullable=False, comment="Unique key for the configuration setting")
    value_min = Column(String, nullable=True, comment="Minimum allowed value for this setting (as string)")
    value_max = Column(String, nullable=True, comment="Maximum allowed value for this setting (as string)")
    value_default = Column(String, nullable=True, comment="Default value for this setting (as string)")
    unit = Column(String, nullable=True, comment="Unit of the setting (e.g., 'days', 'percentage', 'boolean')")
    description = Column(String, nullable=True, comment="Description of the configuration setting")

    customer_configurations = relationship("CustomerConfiguration", back_populates="global_configuration")


    def __repr__(self: GlobalConfiguration):
        return f"<GlobalConfiguration(id={self.id}, key='{self.key}')>"

class CustomerConfiguration(BaseModel):
    __tablename__ = "customer_configurations"
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    global_config_id = Column(Integer, ForeignKey("global_configurations.id"), nullable=False)
    configured_value = Column(String, nullable=False, comment="The value set by the customer, overriding the global default")

    customer = relationship("Customer", back_populates="customer_configurations")
    global_configuration = relationship("GlobalConfiguration", back_populates="customer_configurations")

    __table_args__ = (
        UniqueConstraint('customer_id', 'global_config_id', name='_customer_global_config_uc'),
        Index('idx_customer_config_customer_id', 'customer_id'),
        Index('idx_customer_config_global_config_id', 'global_config_id'),
    )

    def __repr__(self: CustomerConfiguration):
        return f"<CustomerConfiguration(id={self.id}, customer_id={self.customer_id}, global_config_id={self.global_config_id}, configured_value='{self.configured_value}')>"

class Bank(BaseModel):
    __tablename__ = "banks"
    name = Column(String, unique=True, index=True, nullable=False, comment="Name of the bank")
    address = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)
    fax = Column(String, nullable=True)
    former_names = Column(JSON, nullable=True, comment="Former names of the bank (JSON array of strings)")
    swift_code = Column(String, nullable=True, unique=True, index=True, comment="SWIFT/BIC code of the bank")
    short_name = Column(String, nullable=True, comment="Short name of the bank")

    def __repr__(self: Bank):
        return f"<Bank(id={self.id}, name='{self.name}')>"

class Template(BaseModel):
    __tablename__ = "templates"
    name = Column(String, nullable=False, comment="Name of the template")
    template_type = Column(String, nullable=False, comment="Type of template (e.g., 'EMAIL', 'LETTER', 'PDF_ATTACHMENT')")
    action_type = Column(String, nullable=False, comment="Action it relates to (e.g., 'LG_EXTENSION', 'LG_RELEASE')")
    content = Column(Text, nullable=False, comment="The actual content of the template (e.g., HTML for email, Markdown for letter)")
    is_global = Column(Boolean, default=True, nullable=False, comment="True if universal, False if customer-specific")
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True, comment="Null if is_global is True, links to Customer if customer-specific")
    customer = relationship("Customer", back_populates="templates")
    is_notification_template = Column(Boolean, default=False, nullable=False, comment="True if this template is used for sending email notifications.")
    subject = Column(String, nullable=True, comment="Subject line of the template (for emails).")
    is_default = Column(Boolean, default=False, nullable=False, comment="True if this is the default template for its scope and action type.")

    __table_args__ = (
        UniqueConstraint('name', 'action_type', 'customer_id', 'is_notification_template', name='_template_unique_per_scope_and_purpose_and_name'),
    )

    def __repr__(self: Template):
        return f"<Template(id={self.id}, name='{self.name}', type='{self.template_type}')>"

class Currency(BaseModel):
    __tablename__ = "currencies"
    name = Column(String, nullable=False, comment="Full name of the currency (e.g., 'US Dollar')")
    iso_code = Column(String, unique=True, index=True, nullable=False, comment="ISO 4217 currency code (e.g., 'USD', 'EUR')")
    symbol = Column(String, nullable=True, comment="Currency symbol (e.g., '$', '€')")

    def __repr__(self: Currency):
        return f"<Currency(id={self.id}, name='{self.name}', iso_code='{self.iso_code}')>"

class CurrencyExchangeRate(Base):
    __tablename__ = "currency_exchange_rates"

    id = Column(Integer, primary_key=True, index=True)
    currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=False)
    buy_rate = Column(Float, nullable=False)
    sell_rate = Column(Float, nullable=False)
    rate_date = Column(Date, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Optional relationship to your existing Currency model
    currency = relationship("Currency")

class LgType(BaseModel):
    __tablename__ = "lg_types"
    name = Column(String, unique=True, nullable=False, comment="Name of the LG Type (e.g., 'Performance LG', 'Advance Payment LG')")
    description = Column(String, nullable=True)

    def __repr__(self: LgType):
        return f"<LgType(id={self.id}, name='{self.name}')>"

class Rule(BaseModel):
    __tablename__ = "rules"
    name = Column(String, unique=True, nullable=False, comment="Name of the rule set (e.g., 'URDG 758')")
    description = Column(String, nullable=True)

    def __repr__(self: Rule):
        return f"<Rule(id={self.id}, name='{self.name}')>"

class IssuingMethod(BaseModel):
    __tablename__ = "issuing_methods"
    name = Column(String, unique=True, nullable=False, comment="Name of the issuing method (e.g., 'SWIFT', 'Manual', 'Online Portal')")
    description = Column(String, nullable=True)

    def __repr__(self: IssuingMethod):
        return f"<IssuingMethod(id={self.id}, name='{self.name}')>"

class LgStatus(BaseModel):
    __tablename__ = "lg_statuses"
    name = Column(String, unique=True, nullable=False, comment="Name of the LG Status (e.g., 'Issued', 'Active', 'Released', 'Liquidated')")
    description = Column(String, nullable=True)

    def __repr__(self: LgStatus):
        return f"<LgStatus(id={self.id}, name='{self.name}')>"

class LgOperationalStatus(BaseModel):
    __tablename__ = "lg_operational_statuses"
    name = Column(String, unique=True, nullable=False, comment="Name of the LG Operational Status (e.g., 'Operative', 'Non-Operative')")
    description = Column(String, nullable=True)

    def __repr__(self: LgOperationalStatus):
        return f"<LgOperationalStatus(id={self.id}, name='{self.name}')>"


class LGCategory(BaseModel):
    __tablename__ = "lg_categories"
    name = Column(String, nullable=False, comment="Name of the LG category")
    code = Column(String(2), nullable=True, comment="Unique 1-2 character code for the category")
    extra_field_name = Column(String, nullable=True, comment="Name for an optional extra field for LGs in this category")
    is_mandatory = Column(Boolean, default=False, nullable=False, comment="Whether the extra field is mandatory for LGs in this category")
    communication_list = Column(JSON, nullable=True, comment="List of email addresses for communication (JSON array of strings)")
    
    # CRITICAL CHANGE: customer_id is now nullable. NULL means it is a universal category.
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True, comment="The ID of the customer. NULL for universal categories.")
    customer = relationship("Customer", back_populates="lg_categories")
    
    # NEW FIELD: to mark a category as the default for its scope (global or customer-specific)
    is_default = Column(Boolean, default=False, nullable=False, comment="Whether this is the default category for its scope (global or customer-specific).")

    has_all_entity_access = Column(Boolean, default=True, nullable=False, comment="True if category applies to all entities under their customer, False if restricted to specific entities")
    entity_associations = relationship("LGCategoryCustomerEntityAssociation", back_populates="lg_category", cascade="all, delete-orphan")
    entities_with_access = relationship("CustomerEntity", secondary="lg_category_customer_entity_association", viewonly=True)

    __table_args__ = (
        UniqueConstraint('customer_id', 'name', name='_customer_category_name_uc'),
        UniqueConstraint('customer_id', 'code', name='_customer_category_code_uc'),
        Index('idx_lg_category_customer_id_code', 'customer_id', 'code'),
    )

    def __repr__(self: LGCategory):
        scope = "Universal" if self.customer_id is None else f"Customer {self.customer_id}"
        return f"<LGCategory(id={self.id}, name='{self.name}', code='{self.code}', scope='{scope}')>"


class InternalOwnerContact(BaseModel):
    __tablename__ = "internal_owner_contacts"
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    email = Column(String, nullable=False, index=True)
    phone_number = Column(String, nullable=False)
    internal_id = Column(String, nullable=True)
    manager_email = Column(String, nullable=False)

    customer = relationship("Customer", back_populates="internal_owner_contacts")

    __table_args__ = (
        UniqueConstraint('customer_id', 'email', name='_customer_internal_owner_email_uc'),
        Index('idx_internal_owner_contact_customer_id_email', 'customer_id', 'email'),
    )

    def __repr__(self: InternalOwnerContact):
        return f"<InternalOwnerContact(id={self.id}, email='{self.email}', customer_id={self.customer_id})>"


class Permission(Base):
    __tablename__ = "permissions"
    id = Column(Integer, primary_key=True, index=True)
    role_permissions = relationship("RolePermission", back_populates="permission")
    name = Column(String, unique=True, index=True, nullable=False, comment="Unique name of the permission (e.g., 'customer:create', 'lg:view_all')")
    description = Column(String, nullable=True, comment="Description of what the permission allows")

    def __repr__(self: Permission):
        return f"<Permission(id={self.id}, name='{self.name}')>"

class RolePermission(Base):
    __tablename__ = "role_permissions"
    id = Column(Integer, primary_key=True, index=True)
    role = Column(String, nullable=False, comment="The role name (e.g., 'system_owner', 'corporate_admin')")
    permission_id = Column(Integer, ForeignKey("permissions.id"), nullable=False)

    permission = relationship("Permission", back_populates="role_permissions")

    __table_args__ = (
        UniqueConstraint('role', 'permission_id', name='_role_permission_uc'),
        Index('idx_role_permission_role', 'role'),
    )

    def __repr__(self: RolePermission):
        return f"<RolePermission(id={self.id}, role='{self.role}', permission_id={self.permission_id})>"


# --- LG CUSTODY MODULE MODELS (Moved to appear before ApprovalRequest for dependency resolution) ---

class LGRecord(BaseModel):
    __tablename__ = "lg_records"
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, comment="Customer this LG belongs to")
    beneficiary_corporate_id = Column(Integer, ForeignKey("customer_entities.id"), nullable=False, comment="Entity benefiting from the LG")
    lg_sequence_number = Column(Integer, nullable=False, default=1, comment="Unique sequential number for LG within a customer entity for new serial format")
    issuer_name = Column(String, nullable=False, comment="Name of the LG issuer (person/company bank guarantees)")
    issuer_id = Column(String, nullable=True, comment="Optional identifier for the issuer")
    lg_number = Column(String, unique=True, nullable=False, comment="Unique identifier for the Letter of Guarantee")
    lg_amount = Column(Numeric(28, 2), nullable=False, comment="Original total amount of the LG (can be 0 for liquidated LGs)")
    lg_currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=False, comment="Currency of the LG amount")
    lg_payable_currency_id = Column(Integer, ForeignKey("currencies.id"), nullable=True, comment="Currency in which LG is payable (defaults to LG Currency)")
    issuance_date = Column(DateTime(timezone=True), nullable=False, comment="Date the LG was issued (DD/MM/YYYY)")
    expiry_date = Column(DateTime(timezone=True), nullable=False, comment="Date the LG expires")
    lg_period_months = Column(Integer, nullable=False, comment="Duration of the LG in months (auto-calculated)")
    auto_renewal = Column(Boolean, default=True, nullable=False, comment="Indicates if the LG is set to automatically renew")
    lg_type_id = Column(Integer, ForeignKey("lg_types.id"), nullable=False, comment="Categorization of the LG")
    lg_status_id = Column(Integer, ForeignKey("lg_statuses.id"), nullable=False, default=LgStatusEnum.VALID.value, comment="Current status of the LG (defaults to 'Valid')")
    lg_operational_status_id = Column(Integer, ForeignKey("lg_operational_statuses.id"), nullable=True, comment="LG's operational state (conditional)")
    payment_conditions = Column(String, nullable=True, comment="Specific conditions related to payment (conditional)")
    description_purpose = Column(String, nullable=False, comment="General description or purpose of the LG")
    issuing_bank_id = Column(Integer, ForeignKey("banks.id"), nullable=False, comment="ID of the bank that issued the LG")
    issuing_bank_address = Column(String, nullable=False, comment="Address of the issuing bank")
    issuing_bank_phone = Column(String, nullable=False, comment="Phone number of the issuing bank")
    issuing_bank_fax = Column(String, nullable=True, comment="Fax number of the issuing bank")
    issuing_method_id = Column(Integer, ForeignKey("issuing_methods.id"), nullable=False, comment="ID of the method by which LG was issued")
    applicable_rule_id = Column(Integer, ForeignKey("rules.id"), nullable=False, comment="ID of the set of rules governing the LG")
    applicable_rules_text = Column(String, nullable=True, comment="Free text for rules (conditional)")
    other_conditions = Column(String(8000), nullable=True, comment="Any other specific conditions not covered elsewhere")
    internal_owner_contact_id = Column(Integer, ForeignKey("internal_owner_contacts.id"), nullable=False, comment="ID of the internal owner contact person")
    lg_category_id = Column(Integer, ForeignKey("lg_categories.id"), nullable=False, comment="LG Category for internal classification")
    additional_field_values = Column(JSON, nullable=True, comment="Dynamic fields based on selected LGCategory's extra_field_name (JSONB)")
    internal_contract_project_id = Column(String, nullable=True, comment="Internal reference ID for contract/project")
    notes = Column(Text, nullable=True, comment="Free-form notes related to the LG")
    migration_source = Column(String, nullable=True, comment="Indicates the source of the LG (e.g., 'LEGACY' for migrated records).")
    migrated_from_staging_id = Column(Integer, ForeignKey('lg_migration_staging.id'), nullable=True, comment="Foreign key to the last staged record used for this LG.")
    customer = relationship("Customer")
    beneficiary_corporate = relationship("CustomerEntity", foreign_keys=[beneficiary_corporate_id])
    lg_currency = relationship("Currency", foreign_keys=[lg_currency_id])
    lg_payable_currency = relationship("Currency", foreign_keys=[lg_payable_currency_id])
    lg_type = relationship("LgType")
    lg_status = relationship("LgStatus")
    lg_operational_status = relationship("LgOperationalStatus")
    issuing_bank = relationship("Bank", foreign_keys=[issuing_bank_id])
    
    # NEW FIELDS for Foreign Banks
    foreign_bank_name = Column(String, nullable=True, comment="Manually entered bank name for foreign banks")
    foreign_bank_country = Column(String, nullable=True, comment="Manually entered country for foreign banks")
    foreign_bank_address = Column(String, nullable=True, comment="Manually entered address for foreign banks")
    foreign_bank_swift_code = Column(String, nullable=True, comment="Manually entered SWIFT code for foreign banks")
    advising_status = Column(SQLEnum(AdvisingStatus), nullable=True, comment="Status of the LG regarding advising/confirmation")
    communication_bank_id = Column(Integer, ForeignKey("banks.id"), nullable=True, comment="The Advising or Confirming bank for the LG")
    issuing_method = relationship("IssuingMethod")
    applicable_rule = relationship("Rule")
    internal_owner_contact = relationship("InternalOwnerContact")
    lg_category = relationship("LGCategory")
    communication_bank = relationship("Bank", foreign_keys=[communication_bank_id])
    documents = relationship("LGDocument", back_populates="lg_record", cascade="all, delete-orphan")
    instructions = relationship("LGInstruction", back_populates="lg_record")
    change_logs = relationship("LGChangeLog", back_populates="lg_record")
    __table_args__ = (
        UniqueConstraint('lg_number', name='uq_lg_record_number'),
        UniqueConstraint('beneficiary_corporate_id', 'lg_sequence_number', name='uq_lg_sequence_per_entity'),
        Index('idx_lg_record_customer_id', 'customer_id'),
        Index('idx_lg_record_lg_number', 'lg_number'),
        Index('idx_lg_record_expiry_date', 'expiry_date'),
        Index('ix_lg_records_migrated_from_staging_id', 'migrated_from_staging_id'),

    )
    def __repr__(self: LGRecord):
        return f"<LGRecord(id={self.id}, lg_number='{self.lg_number}', customer_id={self.customer_id})>"

class LGDocument(BaseModel):
    __tablename__ = "lg_documents"

    lg_record_id = Column(Integer, ForeignKey("lg_records.id"), nullable=False, comment="The LG record this document belongs to")
    lg_instruction_id = Column(Integer, ForeignKey("lg_instructions.id"), nullable=True, comment="The LG instruction this document belongs to (e.g., delivery proof, bank reply)")
    document_type = Column(String, nullable=False, comment="Type of document (e.g., 'AI_SCAN', 'INTERNAL_SUPPORTING', 'BANK_REPLY', 'AMENDMENT_LETTER', 'DELIVERY_PROOF')")
    file_name = Column(String, nullable=False, comment="Original file name of the document")
    file_path = Column(String, nullable=False, comment="Path or URL to the stored file (e.g., cloud storage URL)")
    mime_type = Column(String, nullable=True, comment="MIME type of the file (e.g., 'application/pdf', 'image/jpeg')")
    uploaded_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, comment="User who uploaded the document")

    # Relationships
    lg_record = relationship("LGRecord", back_populates="documents")
    lg_instruction = relationship("LGInstruction", back_populates="documents")
    status = Column(String(50), default="PENDING", nullable=False, index=True)
    uploaded_by_user = relationship("User")

    __table_args__ = (
        Index('idx_lg_document_lg_record_id', 'lg_record_id'),
        Index('idx_lg_document_lg_instruction_id', 'lg_instruction_id'),
    )

    def __repr__(self: LGDocument):
        return f"<LGDocument(id={self.id}, file_name='{self.file_name}', lg_record_id={self.lg_record_id})>"


class LGInstruction(BaseModel):
    __tablename__ = "lg_instructions"

    lg_record_id = Column(Integer, ForeignKey("lg_records.id"), nullable=False, comment="The LG record this instruction pertains to")
    instruction_type = Column(String, nullable=False, comment="Type of instruction (e.g., 'EXTENSION', 'RELEASE', 'AMENDMENT', 'ACTIVATION')") # Removed 'REMINDER' as it's a sub-type
    serial_number = Column(String, unique=True, nullable=False, comment="Unique serial number for the instruction")
    global_seq_per_lg = Column(Integer, nullable=False, default=1, comment="Global sequential number for all instructions related to this LG record")
    type_seq_per_lg = Column(Integer, nullable=False, default=1, comment="Sequential number per instruction type for this LG record")
    template_id = Column(Integer, ForeignKey("templates.id"), nullable=False, comment="The template used to generate this instruction")
    status = Column(String, nullable=False, default="Instruction Issued", comment="Current status of the instruction (e.g., 'Instruction Issued', 'Instruction Delivered', 'Confirmed by Bank', 'Reminder Issued', 'Canceled')")
    instruction_date = Column(DateTime(timezone=True), server_default=func.now(), comment="Date the instruction was issued/generated (defaults to now)")
    delivery_date = Column(DateTime(timezone=True), nullable=True, comment="Date the instruction was physically delivered to the bank")
    bank_reply_date = Column(DateTime(timezone=True), nullable=True, comment="Date the bank's reply was received")
    details = Column(JSON, nullable=True, comment="JSON object for instruction-specific details")
    generated_content_path = Column(String, nullable=True, comment="Path or URL to the generated instruction document")
    sent_to_bank = Column(Boolean, default=False, nullable=False, comment="Indicates if this instruction has been marked as sent to the bank")
    is_printed = Column(Boolean, default=False, nullable=False, comment="True if this instruction letter has been printed by a user")
    maker_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, comment="User who initiated this instruction (Maker)")
    checker_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, comment="User who approved this instruction (Checker)")
    approval_request_id = Column(Integer, ForeignKey("approval_requests.id"), nullable=True, comment="Link to related approval request if Maker-Checker is enabled")
    bank_reply_details = Column(Text, nullable=True, comment="Detailed text of the bank's reply or notes on it.")

    # Relationships
    lg_record = relationship("LGRecord", back_populates="instructions")
    template = relationship("Template")
    maker_user = relationship("User", foreign_keys=[maker_user_id])
    checker_user = relationship("User", foreign_keys=[checker_user_id])
    documents = relationship("LGDocument", back_populates="lg_instruction", cascade="all, delete-orphan") # New relationship

    __table_args__ = (
        UniqueConstraint('lg_record_id', 'serial_number', name='uq_lg_instruction_serial'),
        UniqueConstraint('lg_record_id', 'global_seq_per_lg', name='uq_lg_instruction_global_seq'),
        UniqueConstraint('lg_record_id', 'instruction_type', 'type_seq_per_lg', name='uq_lg_instruction_type_seq'),
        Index('idx_lg_instruction_lg_record_id', 'lg_record_id'),
        Index('idx_lg_instruction_status', 'status'),
        Index('idx_lg_instruction_type', 'instruction_type'),
    )

    def __repr__(self: LGInstruction):
        return f"<LGInstruction(id={self.id}, serial='{self.serial_number}', type='{self.instruction_type}', status='{self.status}')>"

class ApprovalRequest(Base):
    __tablename__ = "approval_requests"
    id = Column(Integer, primary_key=True, index=True)
    entity_type = Column(String, nullable=False, comment="Type of entity requiring approval (e.g., 'LGRecord', 'User')")
    entity_id = Column(Integer, nullable=True, comment="ID of the entity requiring approval (can be null for bulk/general actions)")
    action_type = Column(String, nullable=False, comment="Type of action requiring approval (e.g., 'LG_AMEND', 'USER_CREATE')")
    status = Column(SQLEnum(ApprovalRequestStatusEnum), nullable=False, default=ApprovalRequestStatusEnum.PENDING, comment="Status of the approval request (PENDING, APPROVED, REJECTED, AUTO_REJECTED_EXPIRED, INVALIDATED_BY_APPROVAL, WITHDRAWN)")
    maker_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, comment="ID of the user who initiated the action (Maker)")
    checker_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, comment="ID of the user who approved/rejected the action (Checker)")
    request_details = Column(JSON, nullable=True, comment="JSON object with details of the requested change")
    lg_record_snapshot = Column(JSON, nullable=True, comment="Snapshot of LGRecord's critical fields at time of request submission (JSON). Reused for other entity snapshots.")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, comment="ID of the customer this approval request belongs to")
    reason = Column(Text, nullable=True, comment="Reason for rejection or auto-rejection/invalidation")
    withdrawn_at = Column(DateTime(timezone=True), nullable=True, comment="Timestamp when the request was withdrawn by the maker")
    internal_owner_contact_id = Column(Integer, ForeignKey("internal_owner_contacts.id"), nullable=True, comment="ID of the InternalOwnerContact if this request pertains to one directly.")
    related_instruction_id = Column(
        Integer,
        ForeignKey(
            "lg_instructions.id",
            use_alter=True,
            name="fk_approval_request_instruction_id"
        ),
        nullable=True,
        comment="ID of the LGInstruction generated upon approval if applicable."
    )
    last_renewal_reminder_sent_at = Column(DateTime(timezone=True), nullable=True, comment="Timestamp of the last renewal reminder sent for this LG (for non-auto-renew LGs)")

    maker_user = relationship("User", foreign_keys=[maker_user_id])
    checker_user = relationship("User", foreign_keys=[checker_user_id])
    customer = relationship("Customer")

    lg_record = relationship("LGRecord",
                             primaryjoin="and_(ApprovalRequest.entity_id == LGRecord.id, ApprovalRequest.entity_type == 'LGRecord')",
                             foreign_keys=[entity_id],
                             remote_side=[LGRecord.id],
                             overlaps="lg_record")

    internal_owner_contact = relationship("InternalOwnerContact",
                                          primaryjoin="and_(ApprovalRequest.entity_id == InternalOwnerContact.id, ApprovalRequest.entity_type == 'InternalOwnerContact')",
                                          foreign_keys=[entity_id],
                                          remote_side=[InternalOwnerContact.id],
                                          overlaps="lg_record, internal_owner_contact")

    related_instruction = relationship(
        "LGInstruction",
        foreign_keys=[related_instruction_id],
        post_update=True,
        overlaps="approval_request"
    )

    def __repr__(self: ApprovalRequest):
        return f"<ApprovalRequest(id={self.id}, entity_type='{self.entity_type}', action_type='{self.action_type}', status='{self.status}')>"


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, comment="ID of the user who performed the action (can be null for system actions or unauthenticated users)")
    action_type = Column(String, nullable=False, comment="Type of action (e.g., CREATE, UPDATE, DELETE, LOGIN, LOGOUT, CUSTOMER_ONBOARD, PLAN_UPDATE)")
    entity_type = Column(String, nullable=False, comment="Type of entity affected (e.g., Customer, User)")
    entity_id = Column(Integer, nullable=True, comment="ID of the entity affected (can be null if action is system-wide or non-entity specific)")
    details = Column(JSON, nullable=True, comment="JSON object with additional details, including changed fields or specific action context (sensitive data should be excluded/redacted)")
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), comment="Timestamp of when the action occurred")
    ip_address = Column(String, nullable=True, comment="IP address from where the action was performed (optional)")
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True, comment="ID of the customer this audit log belongs to (for filtering)")
    lg_record_id = Column(Integer, ForeignKey("lg_records.id"), nullable=True, comment="ID of the LG record associated with the action (for LG lifecycle tracking)")

    user = relationship("User")
    customer = relationship("Customer")
    lg_record = relationship("LGRecord")

    def __repr__(self: AuditLog):
        return f"<AuditLog(id={self.id}, action='{self.action_type}', entity='{self.entity_type}:{self.entity_id}', user_id={self.user_id})>"


class CustomerEmailSetting(BaseModel):
    __tablename__ = "customer_email_settings"

    customer_id = Column(Integer, ForeignKey("customers.id"), unique=True, nullable=False, comment="Customer this email setting belongs to (one-to-one)")
    smtp_host = Column(String, nullable=False, comment="SMTP server host (e.g., smtp.sendgrid.net)")
    smtp_port = Column(Integer, nullable=False, comment="SMTP server port (e.g., 587 or 465)")
    smtp_username = Column(String, nullable=False, comment="SMTP authentication username")
    smtp_password_encrypted = Column(String, nullable=False, comment="Encrypted SMTP password")
    sender_email = Column(String, nullable=False, comment="Email address to use as the sender (e.g., no-reply@customer.com)")
    sender_display_name = Column(String, nullable=True, comment="Optional display name for the sender (e.g., 'Customer Name Treasury')")
    is_active = Column(Boolean, default=True, nullable=False, comment="Whether these custom settings are active (False means fallback to global)")

    customer = relationship("Customer", back_populates="customer_email_settings")

    def __repr__(self: CustomerEmailSetting):
        return f"<CustomerEmailSetting(id={self.id}, customer_id={self.customer_id}, sender_email='{self.sender_email}')>"

class SystemNotification(BaseModel):
    __tablename__ = "system_notifications"
    notification_type = Column(String, default="system_info")
    content = Column(String, nullable=False, comment="The plain text message of the notification")
    link = Column(String, nullable=True, comment="Optional URL to attach to the message")
    start_date = Column(DateTime(timezone=True), nullable=False, comment="Date/time the notification becomes active")
    end_date = Column(DateTime(timezone=True), nullable=False, comment="Date/time the notification expires")
    is_active = Column(Boolean, default=True, nullable=False, comment="Whether the notification is currently active or disabled by the System Owner")

    animation_type = Column(String, nullable=True, comment="CSS animation class to apply (e.g., 'fade', 'slide-left')")
    display_frequency = Column(String, default="once-per-login", nullable=False, comment="Frequency of display (e.g., 'once', 'once-per-login', 'repeat')")
    max_display_count = Column(Integer, nullable=True, comment="Max times to display for a repeating notification")
    target_user_ids = Column(JSONB, nullable=True, comment="List of specific user IDs to target")
    target_roles = Column(JSONB, nullable=True, comment="List of user roles to target")
    target_customer_ids = Column(JSONB, nullable=True, comment="List of customer IDs to target. Null or empty list means all customers.")

    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, comment="The System Owner who created this notification")
    created_by_user = relationship("User", back_populates="system_notifications_created")
    is_popup = Column(Boolean, default=False)
    popup_action_label = Column(String, nullable=True)    
    image_url = Column(String, nullable=True, doc="GCS URI (gs://...) or public URL for the notification image")
    def __repr__(self: SystemNotification):
        return f"<SystemNotification(id={self.id}, content='{self.content[:30]}...', is_active={self.is_active}, targets={self.target_customer_ids})>"

# Add this new model after the SystemNotification model
class SystemNotificationViewLog(Base):
    __tablename__ = "system_notification_view_logs"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    notification_id = Column(Integer, ForeignKey("system_notifications.id"), nullable=False)
    view_count = Column(Integer, default=1, nullable=False)
    last_viewed_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    user = relationship("User")
    notification = relationship("SystemNotification")

    __table_args__ = (
        UniqueConstraint('user_id', 'notification_id', name='_user_notification_uc'),
    )

    def __repr__(self: SystemNotificationViewLog):
        return f"<SystemNotificationViewLog(id={self.id}, user_id={self.user_id}, notification_id={self.notification_id}, view_count={self.view_count})>"
        
# NEW MIGRATION MODELS - Added to this file
class LGMigrationStaging(BaseModel):
    __tablename__ = 'lg_migration_staging'
    
    # The BaseModel already provides: id, created_at, updated_at, is_deleted, deleted_at
    file_name = Column(String, nullable=True, comment="Original name of the uploaded file.")
    record_status = Column(SQLEnum(MigrationRecordStatusEnum), default=MigrationRecordStatusEnum.PENDING, nullable=False, index=True)
    validation_log = Column(JSON, nullable=True, comment="Details of validation errors or warnings.")
    internal_notes = Column(Text, nullable=True, comment="Internal notes from a reviewer.")
    file_content_hash = Column(String, nullable=True, comment="SHA256 hash of the uploaded file for duplicate detection.")
    source_data_json = Column(JSON, nullable=True, comment="The raw extracted data from the document.")
    structured_data_json = Column(JSON, nullable=True, comment="The cleaned, validated, and normalized data ready for import.")
    
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)
    # NEW: Add migration_type column for records vs instructions
    migration_type = Column(SQLEnum(MigrationTypeEnum), default=MigrationTypeEnum.RECORD, nullable=False, index=True, comment="Distinguishes between a full LG record or a subsequent instruction.")
    
    # NEW: Historical Reconstruction columns
    history_sequence = Column(Integer, nullable=True, comment="User-provided sequence number for timeline ordering.")
    history_timestamp = Column(DateTime(timezone=True), nullable=True, comment="User-provided timestamp for timeline ordering.")
    production_lg_id = Column(Integer, ForeignKey('lg_records.id'), nullable=True, comment="The ID of the final LG record this staged record was used to create/update.")

    __table_args__ = (
        Index('idx_lg_migration_staging_customer_id_status', 'customer_id', 'record_status'),
        Index('ix_lg_migration_staging_production_lg_id', 'production_lg_id'),
        # CORRECTED: Functional Index for faster grouping by lg_number for history reconstruction
        Index('ix_lg_migration_staging_lg_number_lower', func.lower((source_data_json['lg_number'].astext))),
    )
    
    def __repr__(self):
        return f"<LGMigrationStaging(id={self.id}, file_name='{self.file_name}', status='{self.record_status}')>"


class MigrationBatch(BaseModel):
    __tablename__ = "migration_batches"
    
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    source_files = Column(JSONB, nullable=True)
    totals = Column(JSONB, nullable=True)
    notes = Column(Text, nullable=True)
    # NEW: Add file_hash to the batch for duplicate file detection
    file_hash = Column(String, nullable=True, comment="SHA256 hash of the uploaded file for duplicate detection.")
    
    user = relationship("User")
    
    __table_args__ = (
        Index('ix_migration_batches_user_id', 'user_id'),
    )

class LGChangeLog(BaseModel):
    __tablename__ = "lg_change_log"
    
    lg_id = Column(Integer, ForeignKey('lg_records.id'), nullable=False)
    staging_id = Column(Integer, ForeignKey('lg_migration_staging.id'), nullable=True)
    change_index = Column(Integer, nullable=False)
    applied_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    diff_json = Column(JSONB, nullable=False)
    note = Column(Text, nullable=True)

    lg_record = relationship("LGRecord", back_populates="change_logs")
    staging_record = relationship("LGMigrationStaging")

    __table_args__ = (
        UniqueConstraint('lg_id', 'change_index', name='uq_lg_change_log_index'),
        Index('ix_lg_change_log_lg_id', 'lg_id'),
    )

# =====================================================================================
# NEW MODELS FOR T&C AND PRIVACY POLICY
# =====================================================================================

class LegalArtifact(BaseModel):
    __tablename__ = "legal_artifacts"
    artifact_type = Column(String, nullable=False, index=True, comment="e.g. 'privacy_policy', 'terms_and_conditions'")
    version = Column(Float, nullable=False, comment="Version number of the artifact")
    content = Column(Text, nullable=False, comment="Full content of the artifact")
    url = Column(String, nullable=True, comment="External URL to the artifact if available")

    user_acceptances = relationship("UserLegalAcceptance", back_populates="artifact")

    __table_args__ = (
        UniqueConstraint('artifact_type', 'version', name='_artifact_type_version_uc'),
        Index('idx_legal_artifacts_type_version', 'artifact_type', 'version'),
    )

class UserLegalAcceptance(Base):
    __tablename__ = "user_legal_acceptance"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    artifact_id = Column(Integer, ForeignKey("legal_artifacts.id"), nullable=False)
    accepted_at = Column(DateTime(timezone=True), server_default=func.now())
    ip_address = Column(String, nullable=True)

    user = relationship("User", back_populates="legal_acceptances")
    artifact = relationship("LegalArtifact", back_populates="user_acceptances")

    __table_args__ = (
        UniqueConstraint('user_id', 'artifact_id', name='_user_artifact_uc'),
        Index('idx_user_legal_acceptance_user_id', 'user_id'),
    )

User.legal_acceptances = relationship("UserLegalAcceptance", back_populates="user", cascade="all, delete-orphan")

class UserDevice(Base):
    __tablename__ = "user_devices"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    device_id = Column(String, index=True, nullable=False)  # UUID from frontend
    device_name = Column(String, nullable=True)           # e.g., "Chrome on Windows"
    is_trusted = Column(Boolean, default=False)          # Becomes True after MFA
    last_login_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    last_ip = Column(String, nullable=True)               # Captured via get_client_ip
    
    # Optional: For extra security
    browser_fingerprint = Column(String, nullable=True)