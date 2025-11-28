# app/crud/crud_issuance.py

from typing import List, Optional
from sqlalchemy.orm import Session, selectinload 
from sqlalchemy import desc

from app.crud.crud import CRUDBase
from app.models_issuance import IssuanceFacility, IssuanceFacilitySubLimit, IssuanceRequest, IssuedLGRecord
from app.schemas.schemas_issuance import IssuanceFacilityCreate, IssuanceRequestCreate

class CRUDIssuanceFacility(CRUDBase):
    def get_multi_by_customer(self, db: Session, customer_id: int, skip: int = 0, limit: int = 100) -> List[IssuanceFacility]:
        return (
            db.query(self.model)
            .filter(self.model.customer_id == customer_id, self.model.is_active == True)
            .options(
                selectinload(IssuanceFacility.sub_limits).selectinload(IssuanceFacilitySubLimit.lg_type),
                selectinload(IssuanceFacility.bank),      
                selectinload(IssuanceFacility.currency)   
            )
            .offset(skip)
            .limit(limit)
            .all()
        )

    def create_with_limits(self, db: Session, obj_in: IssuanceFacilityCreate, customer_id: int) -> IssuanceFacility:
        db_obj = IssuanceFacility(
            bank_id=obj_in.bank_id,
            customer_id=customer_id,
            currency_id=obj_in.currency_id,
            total_limit_amount=obj_in.total_limit_amount,
            reference_number=obj_in.reference_number,
            start_date=obj_in.start_date,
            expiry_date=obj_in.expiry_date,
            review_date=obj_in.review_date,
            is_active=obj_in.is_active
        )
        db.add(db_obj)
        db.flush() 

        if obj_in.sub_limits:
            for limit_in in obj_in.sub_limits:
                db_limit = IssuanceFacilitySubLimit(
                    facility_id=db_obj.id,
                    limit_name=limit_in.limit_name,
                    limit_amount=limit_in.limit_amount,
                    lg_type_id=limit_in.lg_type_id,
                    default_commission_rate=limit_in.default_commission_rate,
                    default_cash_margin_pct=limit_in.default_cash_margin_pct,
                    default_min_commission=limit_in.default_min_commission
                )
                db.add(db_limit)
        
        db.commit()
        db.refresh(db_obj)
        return db_obj

class CRUDIssuanceRequest(CRUDBase):
    def get_by_customer(self, db: Session, customer_id: int, skip: int = 0, limit: int = 100) -> List[IssuanceRequest]:
        return (
            db.query(self.model)
            .filter(self.model.customer_id == customer_id)
            .options(
                selectinload(IssuanceRequest.currency),
                selectinload(IssuanceRequest.lg_record)    
            )
            .order_by(desc(self.model.created_at))
            .offset(skip)
            .limit(limit)
            .all()
        )

    def create_request(self, db: Session, obj_in: IssuanceRequestCreate, customer_id: int, user_id: Optional[int] = None) -> IssuanceRequest:
        if obj_in.transaction_type == "NEW_ISSUANCE":
            lg_record_id = None
        else:
            lg_record_id = obj_in.lg_record_id if obj_in.lg_record_id and obj_in.lg_record_id > 0 else None

        # --- FIX: Convert Pydantic model to Dict for JSON Column ---
        # The database cannot read the 'BusinessDetails' object directly.
        # We must convert it to a standard dictionary using .dict()
        business_details_data = None
        if obj_in.business_details:
             # Check if it has a .dict() method (Pydantic v1/v2 compat)
            if hasattr(obj_in.business_details, 'dict'):
                business_details_data = obj_in.business_details.dict()
            elif hasattr(obj_in.business_details, 'model_dump'): # Pydantic v2 specific
                business_details_data = obj_in.business_details.model_dump()
            else:
                business_details_data = obj_in.business_details

        db_obj = IssuanceRequest(
            customer_id=customer_id,
            requestor_user_id=user_id,
            requestor_name=obj_in.requestor_name,
            transaction_type=obj_in.transaction_type,
            lg_record_id=lg_record_id,
            status="DRAFT", 
            amount=obj_in.amount,
            currency_id=obj_in.currency_id,
            beneficiary_name=obj_in.beneficiary_name,
            requested_issue_date=obj_in.requested_issue_date,
            requested_expiry_date=obj_in.requested_expiry_date,
            business_details=business_details_data # <--- USING THE DICT
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

# Instantiate the CRUD objects
crud_issuance_facility = CRUDIssuanceFacility(IssuanceFacility)
crud_issuance_request = CRUDIssuanceRequest(IssuanceRequest)