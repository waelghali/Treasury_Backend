# app/crud/crud_bank_methods.py

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_
from fastapi import HTTPException

from app.crud.crud import CRUDBase, log_action
from app.models.models_issuance import BankIssuanceOption

class CRUDBankIssuanceOption(CRUDBase):
    
    def get_by_bank(self, db: Session, bank_id: int, only_active: bool = True) -> List[BankIssuanceOption]:
        query = db.query(self.model).filter(self.model.bank_id == bank_id)
        if only_active:
            query = query.filter(self.model.is_active == True)
        return query.all()

    def create_method(self, db: Session, bank_id: int, obj_in: Dict[str, Any], user_id: int) -> BankIssuanceOption:
        db_obj = BankIssuanceOption(
            bank_id=bank_id,
            strategy_code=obj_in.get("strategy_code"),
            display_name=obj_in.get("display_name"),
            configuration=obj_in.get("configuration", {}),
            is_active=obj_in.get("is_active", True)
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        
        log_action(db, user_id, "BANK_METHOD_CREATED", "BankIssuanceOption", db_obj.id, {"bank_id": bank_id, "code": db_obj.strategy_code})
        return db_obj

    def update_method(self, db: Session, method_id: int, obj_in: Dict[str, Any], user_id: int) -> BankIssuanceOption:
        db_obj = db.query(self.model).filter(self.model.id == method_id).first()
        if not db_obj:
            raise HTTPException(status_code=404, detail="Bank issuance method not found")
        
        for field in ["display_name", "strategy_code", "configuration", "is_active"]:
            if field in obj_in:
                setattr(db_obj, field, obj_in[field])
        
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        
        log_action(db, user_id, "BANK_METHOD_UPDATED", "BankIssuanceOption", db_obj.id, {"bank_id": db_obj.bank_id})
        return db_obj

crud_bank_methods = CRUDBankIssuanceOption(BankIssuanceOption)
