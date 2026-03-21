# app/crud/crud_org.py
from typing import List
from sqlalchemy.orm import Session, selectinload
from fastapi import HTTPException, status
from app.crud.crud import CRUDBase, log_action
from app.models import Department, ApprovalGroup, User
from app.schemas.all_schemas import DepartmentCreate, DepartmentUpdate, ApprovalGroupCreate, ApprovalGroupUpdate

class CRUDDepartment(CRUDBase):
    def get_all_for_customer(self, db: Session, customer_id: int) -> List[Department]:
        return db.query(self.model).options(selectinload(Department.manager)).filter(
            self.model.customer_id == customer_id,
            self.model.is_deleted == False
        ).all()

    def _validate_manager_role(self, db: Session, manager_id: int, customer_id: int):
        """Ensures the department manager has an approval-capable role (corporate_admin or checker)."""
        if not manager_id:
            return
        manager = db.query(User).filter(User.id == manager_id, User.customer_id == customer_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager user not found.")
        allowed_roles = ["corporate_admin", "checker"]
        if manager.role not in allowed_roles:
            raise HTTPException(
                status_code=400,
                detail=f"Department manager must have 'Corporate Admin' or 'Checker' role. "
                       f"User '{manager.email}' has role '{manager.role}' which cannot approve requests."
            )

    def create_dept(self, db: Session, obj_in: DepartmentCreate, customer_id: int, user_id: int) -> Department:
        existing = db.query(self.model).filter(
            self.model.customer_id == customer_id, 
            self.model.name == obj_in.name, 
            self.model.is_deleted == False
        ).first()
        if existing:
            raise HTTPException(status_code=409, detail="Department name already exists.")
        
        self._validate_manager_role(db, obj_in.manager_id, customer_id)
        
        db_obj = self.model(name=obj_in.name, manager_id=obj_in.manager_id, customer_id=customer_id)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        log_action(db, user_id=user_id, action_type="CREATE", entity_type="Department", entity_id=db_obj.id, details={"name": db_obj.name}, customer_id=customer_id)
        return db_obj

    def update_dept(self, db: Session, db_obj: Department, obj_in: DepartmentUpdate, user_id: int) -> Department:
        if obj_in.name and obj_in.name != db_obj.name:
            existing = db.query(self.model).filter(
                self.model.customer_id == db_obj.customer_id, 
                self.model.name == obj_in.name, 
                self.model.is_deleted == False
            ).first()
            if existing:
                raise HTTPException(status_code=409, detail="Department name already exists.")
        
        if obj_in.manager_id is not None:
            self._validate_manager_role(db, obj_in.manager_id, db_obj.customer_id)
        
        update_data = obj_in.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_obj, field, value)
        db.commit()
        db.refresh(db_obj)
        log_action(db, user_id=user_id, action_type="UPDATE", entity_type="Department", entity_id=db_obj.id, details=update_data, customer_id=db_obj.customer_id)
        return db_obj

class CRUDApprovalGroup(CRUDBase):
    def get_all_for_customer(self, db: Session, customer_id: int) -> List[ApprovalGroup]:
        return db.query(self.model).options(selectinload(ApprovalGroup.users)).filter(
            self.model.customer_id == customer_id,
            self.model.is_deleted == False
        ).all()

    def create_group(self, db: Session, obj_in: ApprovalGroupCreate, customer_id: int, user_id: int) -> ApprovalGroup:
        existing = db.query(self.model).filter(
            self.model.customer_id == customer_id, 
            self.model.name == obj_in.name, 
            self.model.is_deleted == False
        ).first()
        if existing:
            raise HTTPException(status_code=409, detail="Group name already exists.")
        
        db_obj = self.model(name=obj_in.name, customer_id=customer_id)
        
        if obj_in.user_ids:
            users = db.query(User).filter(User.id.in_(obj_in.user_ids), User.customer_id == customer_id).all()
            db_obj.users = users

        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        log_action(db, user_id=user_id, action_type="CREATE", entity_type="ApprovalGroup", entity_id=db_obj.id, details={"name": db_obj.name}, customer_id=customer_id)
        return db_obj

    def update_group(self, db: Session, db_obj: ApprovalGroup, obj_in: ApprovalGroupUpdate, user_id: int) -> ApprovalGroup:
        if obj_in.name and obj_in.name != db_obj.name:
            existing = db.query(self.model).filter(
                self.model.customer_id == db_obj.customer_id, 
                self.model.name == obj_in.name, 
                self.model.is_deleted == False
            ).first()
            if existing:
                raise HTTPException(status_code=409, detail="Group name already exists.")
        
        if obj_in.name:
            db_obj.name = obj_in.name
        
        if obj_in.user_ids is not None:
            users = db.query(User).filter(User.id.in_(obj_in.user_ids), User.customer_id == db_obj.customer_id).all()
            db_obj.users = users

        db.commit()
        db.refresh(db_obj)
        log_action(db, user_id=user_id, action_type="UPDATE", entity_type="ApprovalGroup", entity_id=db_obj.id, details={"name": db_obj.name}, customer_id=db_obj.customer_id)
        return db_obj

crud_department = CRUDDepartment(Department)
crud_approval_group = CRUDApprovalGroup(ApprovalGroup)