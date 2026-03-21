from typing import List, Optional, Dict, Any, Union
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, or_
from datetime import datetime
from fastapi import HTTPException
from app.crud.crud import CRUDBase, log_action
from app.models.models_reconciliation_v2 import BankStatement, BankTransaction, MultiReference, ReconciliationMatch, ClassificationRule
from app.schemas.schemas_reconciliation_v2 import BankStatementCreate, ClassificationRuleCreate

class CRUDBankStatement(CRUDBase):
    def get(self, db: Session, id: Any) -> Optional[BankStatement]:
        return db.query(self.model).filter(
            self.model.id == id,
            self.model.is_deleted == False
        ).first()

    def get_by_customer(self, db: Session, customer_id: int, skip: int = 0, limit: int = 100) -> List[BankStatement]:
        return db.query(self.model).filter(
            self.model.company_id == customer_id,
            self.model.is_deleted == False
        ).order_by(desc(self.model.created_at)).offset(skip).limit(limit).all()

    def create_statement(self, db: Session, obj_in: BankStatementCreate, user_id: int) -> BankStatement:
        db_obj = BankStatement(
            **obj_in.model_dump(),
            created_by=user_id
        )
        db.add(db_obj)
        db.flush()
        
        log_action(db, user_id, "STATEMENT_IMPORTED", "BankStatement", db_obj.id, {"file": db_obj.file_name}, obj_in.company_id)
        
        db.commit()
        db.refresh(db_obj)
        return db_obj

class CRUDBankTransaction(CRUDBase):
    def get_by_statement(self, db: Session, statement_id: int) -> List[BankTransaction]:
        return db.query(self.model).filter(self.model.statement_id == statement_id).all()

    def get_unreconciled(self, db: Session, customer_id: int) -> List[BankTransaction]:
        return db.query(self.model).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            self.model.is_reconciled == False
        ).all()

    def get_filtered(self, db: Session, customer_id: int, 
                    bank_id: Optional[int] = None, 
                    account_number: Optional[str] = None,
                    start_date: Optional[Union[str, datetime]] = None,
                    end_date: Optional[Union[str, datetime]] = None,
                    is_reconciled: Optional[bool] = None,
                    is_classified: Optional[bool] = None,
                    search: Optional[str] = None,
                    skip: int = 0, limit: int = 1000) -> List[BankTransaction]:
        """
        Fetches transactions across multiple statements with advanced filtering.
        """
        query = db.query(self.model).join(BankStatement).filter(BankStatement.company_id == customer_id)
        
        if bank_id:
            query = query.filter(BankStatement.bank_id == bank_id)
        if account_number:
            query = query.filter(BankStatement.account_number == account_number)
        
        if start_date:
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            query = query.filter(self.model.booking_date >= start_date)
            
        if end_date:
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            query = query.filter(self.model.booking_date <= end_date)
            
        if is_reconciled is not None:
            query = query.filter(self.model.is_reconciled == is_reconciled)
        if is_classified is not None:
            query = query.filter(self.model.is_classified == is_classified)
            
        if search:
            query = query.filter(or_(
                self.model.raw_description.ilike(f"%{search}%"),
                self.model.e2e_id.ilike(f"%{search}%")
            ))
            
        from sqlalchemy.orm import joinedload
        # Default order: Ledger style (Newest first) unless user sorts otherwise in frontend
        return query.options(joinedload(self.model.applied_rule)).order_by(self.model.booking_date.desc(), self.model.id.desc()).offset(skip).limit(limit).all()

class CRUDClassificationRule(CRUDBase):
    def get_by_customer(self, db: Session, customer_id: int) -> List[ClassificationRule]:
        return db.query(self.model).filter(
            self.model.company_id == customer_id,
            self.model.is_deleted == False
        ).order_by(self.model.priority.asc()).all()

    def create_rule(self, db: Session, obj_in: ClassificationRuleCreate, customer_id: int, user_id: int) -> ClassificationRule:
        db_obj = ClassificationRule(
            **obj_in.model_dump(),
            company_id=customer_id,
            created_by=user_id
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def update_rule(self, db: Session, rule_id: int, obj_in: Any, customer_id: int) -> ClassificationRule:
        db_obj = db.query(self.model).filter(
            self.model.id == rule_id, 
            self.model.company_id == customer_id,
            self.model.is_deleted == False
        ).first()
        if not db_obj:
            raise HTTPException(status_code=404, detail="Rule not found")
        
        updated_obj = self.update(db, db_obj=db_obj, obj_in=obj_in)
        db.commit()
        db.refresh(updated_obj)
        return updated_obj

    def delete_rule(self, db: Session, rule_id: int, customer_id: int) -> bool:
        db_obj = db.query(self.model).filter(
            self.model.id == rule_id, 
            self.model.company_id == customer_id,
            self.model.is_deleted == False
        ).first()
        if not db_obj:
            raise HTTPException(status_code=404, detail="Rule not found")
        
        self.soft_delete(db, db_obj=db_obj)
        db.commit()
        return True

crud_bank_statement = CRUDBankStatement(BankStatement)
crud_bank_transaction = CRUDBankTransaction(BankTransaction)
crud_classification_rule = CRUDClassificationRule(ClassificationRule)
