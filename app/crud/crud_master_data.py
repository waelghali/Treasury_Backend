# crud_master_data.py
from typing import Any, List, Optional, Type
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func, and_

from app.crud.crud import CRUDBase, log_action
from app.models import (
    Bank,
    Currency,
    IssuingMethod,
    LgOperationalStatus,
    LgStatus,
    LgType,
    Rule,
    Template,
    UniversalCategory,
    BaseModel,
    Customer, # Explicitly imported for Template.customer relation
)
from app.schemas.all_schemas import (
    BankCreate,
    BankOut,
    BankUpdate,
    CurrencyCreate,
    CurrencyOut,
    CurrencyUpdate,
    IssuingMethodCreate,
    IssuingMethodOut,
    IssuingMethodUpdate,
    LgOperationalStatusCreate,
    LgOperationalStatusOut,
    LgOperationalStatusUpdate,
    LgStatusCreate,
    LgStatusOut,
    LgStatusUpdate,
    LgTypeCreate,
    LgTypeOut,
    LgTypeUpdate,
    RuleCreate,
    RuleOut,
    RuleUpdate,
    TemplateCreate,
    TemplateOut,
    TemplateUpdate,
    UniversalCategoryCreate,
    UniversalCategoryOut,
    UniversalCategoryUpdate,
)


# =====================================================================================
# Master Data Management (Global Scope)
# =====================================================================================
class CRUDBank(CRUDBase):
    def get_by_name(self, db: Session, name: str) -> Optional[Bank]:
        return (
            db.query(self.model)
            .filter(self.model.name == name, self.model.is_deleted == False)
            .first()
        )

    def get_by_swift_code(self, db: Session, swift_code: str) -> Optional[Bank]:
        return (
            db.query(self.model)
            .filter(
                self.model.swift_code == swift_code, self.model.is_deleted == False
            )
            .first()
        )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="Bank",
            entity_id=db_obj.id,
            details={"name": db_obj.name, "swift_code": db_obj.swift_code},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="Bank",
                entity_id=updated_obj.id,
                details={"name": updated_obj.name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj

    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="Bank",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="Bank",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj


class CRUDTemplate(CRUDBase):
    def get_by_name_and_action_type(
        self, db: Session, name: str, action_type: str, customer_id: Optional[int] = None, is_notification_template: bool = False, is_default: Optional[bool] = None
    ) -> Optional[Template]:
        query = db.query(self.model).filter(
            self.model.name == name,
            self.model.action_type == action_type,
            self.model.is_notification_template == is_notification_template,
            self.model.is_deleted == False,
        )
        if customer_id is None:
            query = query.filter(self.model.customer_id.is_(None))
        else:
            query = query.filter(self.model.customer_id == customer_id)
        
        if is_default is not None:
            query = query.filter(self.model.is_default == is_default)

        return query.first()

    def get_single_template(
        self, db: Session, action_type: str, is_global: bool, customer_id: Optional[int] = None, is_notification_template: bool = False
    ) -> Optional[Template]:
        query = db.query(self.model).filter(
            self.model.action_type == action_type,
            self.model.is_global == is_global,
            self.model.is_notification_template == is_notification_template,
            self.model.is_deleted == False,
        )
        if is_global:
            query = query.filter(self.model.customer_id.is_(None))
        else:
            query = query.filter(self.model.customer_id == customer_id)

        default_template = query.filter(self.model.is_default == True).first()
        if default_template:
            return default_template
        
        return query.first()
        
    def get_all_by_action_type(
        self, db: Session, action_type: Optional[str] = None, customer_id: Optional[int] = None, skip: int = 0, limit: int = 100, is_notification_template: Optional[bool] = None
    ) -> List[Template]:
        query = db.query(self.model).filter(self.model.is_deleted == False)
        if action_type:
            query = query.filter(self.model.action_type == action_type)
        if is_notification_template is not None:
            query = query.filter(self.model.is_notification_template == is_notification_template)

        if customer_id:
            query = query.filter(
                (self.model.is_global == True) | (self.model.customer_id == customer_id)
            )
        else:
            query = query.filter(self.model.is_global == True)

        return query.offset(skip).limit(limit).all()

    def get_templates_with_customer_name(
        self, db: Session, template_id: int
    ) -> Optional[Template]:
        # Removed local import, using the explicitly imported Customer model at the top
        return (
            db.query(self.model)
            .options(selectinload(Template.customer))
            .filter(self.model.id == template_id, self.model.is_deleted == False)
            .first()
        )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        if obj_in.is_default:
            existing_default = self.get_by_name_and_action_type(
                db,
                name=None,
                action_type=obj_in.action_type,
                customer_id=obj_in.customer_id,
                is_notification_template=obj_in.is_notification_template,
                is_default=True
            )
            if existing_default:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"A default template for action type '{obj_in.action_type}' (notification: {obj_in.is_notification_template}) already exists for this scope. Only one default allowed."
                )

        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="Template",
            entity_id=db_obj.id,
            details={
                "name": db_obj.name,
                "action_type": db_obj.action_type,
                "is_global": db_obj.is_global,
                "customer_id": db_obj.customer_id,
                "is_notification_template": db_obj.is_notification_template,
                "is_default": db_obj.is_default,
            },
        )
        return db_obj
    
    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        if obj_in.is_default is True:
            existing_default = self.get_by_name_and_action_type(
                db,
                name=None,
                action_type=obj_in.action_type if obj_in.action_type is not None else db_obj.action_type,
                customer_id=obj_in.customer_id if obj_in.customer_id is not None else db_obj.customer_id,
                is_notification_template=obj_in.is_notification_template if obj_in.is_notification_template is not None else db_obj.is_notification_template,
                is_default=True
            )
            if existing_default and existing_default.id != db_obj.id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"A default template for action type '{obj_in.action_type if obj_in.action_type is not None else db_obj.action_type}' (notification: {obj_in.is_notification_template if obj_in.is_notification_template is not None else db_obj.is_notification_template}) already exists for this scope. Only one default allowed."
                )

        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="Template",
                entity_id=updated_obj.id,
                details={"name": updated_obj.name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj
    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="Template",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="Template",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj


class CRUDCurrency(CRUDBase):
    def get_by_iso_code(self, db: Session, iso_code: str) -> Optional[Currency]:
        return (
            db.query(self.model)
            .filter(self.model.iso_code == iso_code, self.model.is_deleted == False)
            .first()
        )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="Currency",
            entity_id=db_obj.id,
            details={"name": db_obj.name, "iso_code": db_obj.iso_code},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="Currency",
                entity_id=updated_obj.id,
                details={"name": updated_obj.name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj

    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="Currency",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="Currency",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj


class CRUDLgType(CRUDBase):
    def get_by_name(self, db: Session, name: str) -> Optional[LgType]:
        return (
            db.query(self.model)
            .filter(self.model.name == name, self.model.is_deleted == False)
            .first()
        )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="LgType",
            entity_id=db_obj.id,
            details={"name": db_obj.name},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="LgType",
                entity_id=updated_obj.id,
                details={"name": updated_obj.name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj

    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="LgType",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="LgType",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj


class CRUDRule(CRUDBase):
    def get_by_name(self, db: Session, name: str) -> Optional[Rule]:
        return (
            db.query(self.model)
            .filter(self.model.name == name, self.model.is_deleted == False)
            .first()
        )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="Rule",
            entity_id=db_obj.id,
            details={"name": db_obj.name},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="Rule",
                entity_id=updated_obj.id,
                details={"name": updated_obj.name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj

    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="Rule",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="Rule",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj


class CRUDIssuingMethod(CRUDBase):
    def get_by_name(self, db: Session, name: str) -> Optional[IssuingMethod]:
        return (
            db.query(self.model)
            .filter(self.model.name == name, self.model.is_deleted == False)
            .first()
        )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="IssuingMethod",
            entity_id=db_obj.id,
            details={"name": db_obj.name},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="IssuingMethod",
                entity_id=updated_obj.id,
                details={"name": updated_obj.name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj

    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="IssuingMethod",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="IssuingMethod",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj


class CRUDLgStatus(CRUDBase):
    def get_by_name(self, db: Session, name: str) -> Optional[LgStatus]:
        return (
            db.query(self.model)
            .filter(self.model.name == name, self.model.is_deleted == False)
            .first()
        )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="LgStatus",
            entity_id=db_obj.id,
            details={"name": db_obj.name},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="LgStatus",
                entity_id=updated_obj.id,
                details={"name": updated_obj.name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj

    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="LgStatus",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="LgStatus",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj


class CRUDLgOperationalStatus(CRUDBase):
    def get_by_name(self, db: Session, name: str) -> Optional[LgOperationalStatus]:
        return (
            db.query(self.model)
            .filter(self.model.name == name, self.model.is_deleted == False)
            .first()
        )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="LgOperationalStatus",
            entity_id=db_obj.id,
            details={"name": db_obj.name},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="LgOperationalStatus",
                entity_id=updated_obj.id,
                details={"name": updated_obj.name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj

    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="LgOperationalStatus",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="LgOperationalStatus",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj


class CRUDUniversalCategory(CRUDBase):
    def get_by_category_name(
        self, db: Session, category_name: str
    ) -> Optional[UniversalCategory]:
        return (
            db.query(self.model)
            .filter(
                self.model.category_name == category_name, self.model.is_deleted == False
            )
            .first()
        )

    def get_by_code(self, db: Session, code: str) -> Optional[UniversalCategory]:
        return (
            db.query(self.model)
            .filter(self.model.code == code, self.model.is_deleted == False)
            .first()
        )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="UniversalCategory",
            entity_id=db_obj.id,
            details={"category_name": db_obj.category_name},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="UniversalCategory",
                entity_id=updated_obj.id,
                details={"category_name": updated_obj.category_name, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log
        return updated_obj

    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="UniversalCategory",
            entity_id=deleted_obj.id,
            details={"name": deleted_obj.name},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="UniversalCategory",
            entity_id=restored_obj.id,
            details={"name": restored_obj.name},
        )
        return restored_obj

# Removed local instantiations
# crud_bank = CRUDBank(Bank)
# crud_template = CRUDTemplate(Template)
# crud_currency = CRUDCurrency(Currency)
# crud_lg_type = CRUDLgType(LgType)
# crud_rule = CRUDRule(Rule)
# crud_issuing_method = CRUDIssuingMethod(IssuingMethod)
# crud_lg_status = CRUDLgStatus(LgStatus)
# crud_lg_operational_status = CRUDLgOperationalStatus(LgOperationalStatus)
# crud_universal_category = CRUDUniversalCategory(UniversalCategory)