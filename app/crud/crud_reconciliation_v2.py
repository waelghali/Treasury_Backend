from typing import List, Optional, Dict, Any, Union
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, or_
from datetime import datetime
from fastapi import HTTPException
from app.crud.crud import CRUDBase, log_action
from app.models.models_reconciliation_v2 import (
    BankStatement, BankTransaction, MultiReference, 
    ReconciliationMatch, ClassificationRule, Counterparty, InternalLedgerRecord,
    ClassificationTaxonomy
)
from app.schemas.schemas_reconciliation_v2 import (
    BankStatementCreate, ClassificationRuleCreate,
    CounterpartyCreate, CounterpartyUpdate, TaxonomyNodeCreate, TaxonomyNodeUpdate
)

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


class CRUDCounterparty(CRUDBase):
    def get(self, db: Session, id: Any) -> Optional[Counterparty]:
        return db.query(self.model).filter(
            self.model.id == id,
            self.model.is_deleted == False
        ).first()

    def get_by_customer(self, db: Session, customer_id: int, 
                        entity_type: Optional[str] = None,
                        search: Optional[str] = None,
                        skip: int = 0, limit: int = 100) -> List[Counterparty]:
        """
        Returns counterparties visible to this customer (customer-specific + global system-wide ones).
        """
        query = db.query(self.model).filter(
            or_(
                self.model.company_id == customer_id,
                self.model.company_id == None
            ),
            self.model.is_deleted == False
        )
        if entity_type:
            query = query.filter(self.model.entity_type == entity_type)
        if search:
            query = query.filter(self.model.name.ilike(f"%{search}%"))
            
        return query.order_by(
            self.model.is_verified.desc(),
            self.model.learned_count.desc(),
            self.model.name.asc()
        ).offset(skip).limit(limit).all()

    def create_counterparty(self, db: Session, obj_in: CounterpartyCreate, 
                            customer_id: Optional[int], user_id: int) -> Counterparty:
        company_id = obj_in.company_id if obj_in.company_id is not None else customer_id
        db_obj = Counterparty(
            name=obj_in.name.strip(),
            aliases=obj_in.aliases or [],
            entity_type=obj_in.entity_type,
            default_gl_account=obj_in.default_gl_account,
            default_category=obj_in.default_category,
            company_id=company_id,
            is_verified=True,
            is_active=obj_in.is_active,
            created_by=user_id
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def update_counterparty(self, db: Session, counterparty_id: int, 
                            obj_in: CounterpartyUpdate, customer_id: int) -> Counterparty:
        db_obj = db.query(self.model).filter(
            self.model.id == counterparty_id,
            or_(
                self.model.company_id == customer_id,
                self.model.company_id == None
            ),
            self.model.is_deleted == False
        ).first()
        if not db_obj:
            raise HTTPException(status_code=404, detail="Counterparty not found")
            
        update_data = obj_in.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_obj, field, value)
            
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def delete_counterparty(self, db: Session, counterparty_id: int, customer_id: int) -> bool:
        db_obj = db.query(self.model).filter(
            self.model.id == counterparty_id,
            self.model.company_id == customer_id,
            self.model.is_deleted == False
        ).first()
        if not db_obj:
            raise HTTPException(status_code=404, detail="Counterparty not found or cannot delete global entry")
            
        self.soft_delete(db, db_obj=db_obj)
        db.commit()
        return True

    def find_all_active(self, db: Session, customer_id: int) -> List[Counterparty]:
        """
        Fetches all active counterparties visible to customer for fast in-memory matching.
        """
        return db.query(self.model).filter(
            or_(
                self.model.company_id == customer_id,
                self.model.company_id == None
            ),
            self.model.is_active == True,
            self.model.is_deleted == False
        ).all()

    def learn_or_increment(self, db: Session, name: str, customer_id: int, 
                           entity_type: str = "CUSTOMER", 
                           default_category: Optional[str] = None,
                           default_gl: Optional[str] = None,
                           user_id: Optional[int] = None) -> Counterparty:
        """
        Learning feedback loop: If counterparty exists, increments learned_count.
        If new, auto-registers it as unverified so it works for future transactions.
        """
        clean_name = name.strip()
        if not clean_name:
            return None

        existing = db.query(self.model).filter(
            or_(
                self.model.company_id == customer_id,
                self.model.company_id == None
            ),
            self.model.name.ilike(clean_name),
            self.model.is_deleted == False
        ).first()

        if existing:
            existing.learned_count = (existing.learned_count or 0) + 1
            if default_category and not existing.default_category:
                existing.default_category = default_category
            if default_gl and not existing.default_gl_account:
                existing.default_gl_account = default_gl
            db.commit()
            db.refresh(existing)
            return existing
        else:
            new_cp = Counterparty(
                name=clean_name,
                aliases=[clean_name],
                entity_type=entity_type,
                default_category=default_category,
                default_gl_account=default_gl,
                company_id=customer_id,
                learned_count=1,
                is_verified=False,
                is_active=True,
                created_by=user_id
            )
            db.add(new_cp)
            db.commit()
            db.refresh(new_cp)
            return new_cp


crud_counterparty = CRUDCounterparty(Counterparty)


class CRUDInternalLedgerRecord(CRUDBase):
    def get_by_customer(self, db: Session, customer_id: int, status: Optional[str] = None, limit: int = 200) -> List[InternalLedgerRecord]:
        query = db.query(self.model).filter(
            self.model.company_id == customer_id,
            self.model.is_deleted == False
        )
        if status:
            query = query.filter(self.model.status == status)
        return query.order_by(self.model.record_date.desc()).limit(limit).all()

crud_internal_ledger = CRUDInternalLedgerRecord(InternalLedgerRecord)


class CRUDClassificationTaxonomy(CRUDBase):
    def get_tree(self, db: Session, customer_id: int) -> List[ClassificationTaxonomy]:
        """
        Returns full hierarchy of Classes (parent_id is None) with their Subclasses.
        Combines company-specific entries and global enterprise template entries.
        """
        classes = db.query(self.model).filter(
            or_(self.model.company_id == customer_id, self.model.company_id == None),
            self.model.parent_id == None,
            self.model.is_active == True,
            self.model.is_deleted == False
        ).order_by(self.model.order_index, self.model.name).all()

        subclasses = db.query(self.model).filter(
            or_(self.model.company_id == customer_id, self.model.company_id == None),
            self.model.parent_id != None,
            self.model.is_active == True,
            self.model.is_deleted == False
        ).order_by(self.model.order_index, self.model.name).all()

        sub_by_parent = {}
        for s in subclasses:
            sub_by_parent.setdefault(s.parent_id, []).append(s)

        for c in classes:
            c.subclasses = sub_by_parent.get(c.id, [])

        return classes

    def create_node(self, db: Session, node_in: TaxonomyNodeCreate, customer_id: int, user_id: int) -> ClassificationTaxonomy:
        db_obj = ClassificationTaxonomy(
            company_id=customer_id,
            parent_id=node_in.parent_id,
            code=node_in.code.strip().upper(),
            name=node_in.name.strip(),
            name_ar=node_in.name_ar.strip() if node_in.name_ar else None,
            direction=node_in.direction or "EITHER",
            default_gl_account=node_in.default_gl_account,
            description=node_in.description,
            order_index=node_in.order_index,
            is_active=True,
            created_by=user_id
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def update_node(self, db: Session, db_obj: ClassificationTaxonomy, node_in: TaxonomyNodeUpdate, user_id: int) -> ClassificationTaxonomy:
        for field, value in node_in.model_dump(exclude_unset=True).items():
            setattr(db_obj, field, value)
        db_obj.updated_by = user_id
        db.commit()
        db.refresh(db_obj)
        return db_obj

crud_taxonomy = CRUDClassificationTaxonomy(ClassificationTaxonomy)


