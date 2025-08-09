# crud_permissions.py
from typing import List, Optional, Type
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func
from app.crud.crud import CRUDBase
from app.models import ApprovalRequest, Permission, RolePermission, BaseModel
from app.schemas.all_schemas import ApprovalRequestCreate, RolePermissionCreate, PermissionCreate


# =====================================================================================
# Permissions & Approvals
# =====================================================================================
class CRUDPermission(CRUDBase):
    def __init__(self, model: Type[Permission]):
        self.model = model

    def get_by_name(self, db: Session, name: str) -> Optional[Permission]:
        return db.query(self.model).filter(self.model.name == name).first()

    def create(self, db: Session, obj_in: PermissionCreate) -> Permission:
        db_obj = super().create(db, obj_in)
        return db_obj

    def get_all(self, db: Session, skip: int = 0, limit: int = 100) -> List[Permission]:
        return db.query(self.model).offset(skip).limit(limit).all()

    def update(
        self, db: Session, db_obj: Permission, obj_in: PermissionCreate
    ) -> Permission:
        if obj_in.description is not None:
            db_obj.description = obj_in.description
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)
        return db_obj


class CRUDRolePermission(CRUDBase):
    def __init__(self, model: Type[RolePermission]):
        self.model = model

    def get_by_role_and_permission(
        self, db: Session, role: str, permission_id: int
    ) -> Optional[RolePermission]:
        return (
            db.query(self.model)
            .filter(self.model.role == role, self.model.permission_id == permission_id)
            .first()
        )

    def get_permissions_for_role(self, db: Session, role: str) -> List[Permission]:
        return (
            db.query(Permission)
            .join(RolePermission, RolePermission.permission_id == Permission.id)
            .filter(RolePermission.role == role)
            .all()
        )

    def create(self, db: Session, obj_in: RolePermissionCreate) -> RolePermission:
        db_obj = super().create(db, obj_in)
        return db_obj

    def delete(self, db: Session, db_obj: RolePermission):
        db.delete(db_obj)
        db.flush()
        return {"message": "Role permission deleted"}

# Removed local instantiations
# crud_permission = CRUDPermission(Permission)
# crud_role_permission = CRUDRolePermission(RolePermission)