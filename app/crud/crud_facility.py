# app/crud/crud_facility.py

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session, selectinload 
from datetime import datetime
from decimal import Decimal
from fastapi.encoders import jsonable_encoder

from app.crud.crud import CRUDBase
from app.models.models_issuance import (
    IssuanceFacility, 
    IssuanceFacilitySubLimit, 
    IssuanceFacilityAuditLog
)
from app.schemas.schemas_issuance import IssuanceFacilityCreate, IssuanceFacilityUpdate

class CRUDFacility(CRUDBase):
    
    def get_multi_by_customer(self, db: Session, customer_id: int, include_deleted: bool = False) -> List[IssuanceFacility]:
        """Fetch facilities, excluding archived ones by default."""
        query = db.query(self.model).filter(self.model.customer_id == customer_id)
        
        if not include_deleted:
            query = query.filter(self.model.deleted_at == None)
            
        return query.options(
            selectinload(IssuanceFacility.sub_limits),
            selectinload(IssuanceFacility.entities),
            selectinload(IssuanceFacility.bank),      
            selectinload(IssuanceFacility.currency)   
        ).all()

    def create_facility(self, db: Session, *, obj_in: IssuanceFacilityCreate, customer_id: int, user_id: int) -> IssuanceFacility:
        # 1. Convert schema to dict and separate sub_limits
        # Use exclude_unset=True to avoid sending default None values for missing fields
        full_data = obj_in.dict(exclude={"sub_limits", "entity_ids"})
        sub_limits_data = obj_in.sub_limits or []
        entity_ids = getattr(obj_in, "entity_ids", []) # Get entity_ids if provided
        # 2. Remove customer_id from data to avoid the 'multiple values' error
        full_data.pop("customer_id", None) 

        # 3. Create Main Facility
        db_obj = IssuanceFacility(
            **full_data,
            customer_id=customer_id,
            status="ACTIVE"
        )
        db.add(db_obj)
        db.flush()  # Push to DB to get db_obj.id

        if entity_ids:
            from app.models import CustomerEntity  # Ensure this import works
            # Fetch the actual entity objects from the DB
            db_entities = db.query(CustomerEntity).filter(CustomerEntity.id.in_(entity_ids)).all()
            # Attach them to the facility relationship
            db_obj.entities = db_entities

        db.add(db_obj)
        db.flush()  # Push to DB to generate db_obj.id

        # 4. Create Sub-Limits (with safety check)
        for sl_schema in sub_limits_data:
            # If sl_schema is a Pydantic object, convert it to dict
            sl_dict = sl_schema.dict() if hasattr(sl_schema, "dict") else sl_schema
            
            # Only attempt to save if lg_type_ids is provided (not None/empty)
            if sl_dict.get("lg_type_ids") is not None:
                db_limit = IssuanceFacilitySubLimit(
                    **sl_dict,
                    facility_id=db_obj.id
                )
                db.add(db_limit)
        
        # 6. Log Audit Trail
        audit = IssuanceFacilityAuditLog(
            facility_id=db_obj.id,
            performed_by_user_id=user_id,
            action="CREATE",
            new_values=jsonable_encoder(obj_in.dict()) # Use encoder here
        )
        db.add(audit)
        
        db.commit()
        db.refresh(db_obj)
        return db_obj
    def update_facility(self, db: Session, db_obj: IssuanceFacility, obj_in: IssuanceFacilityUpdate, user_id: int) -> IssuanceFacility:
        # Snapshot for audit log
        old_values = {
            "facility_name": db_obj.facility_name,
            "total_limit_amount": float(db_obj.total_limit_amount),
            "status": db_obj.status
        }

        # 1. Update Basic Fields - Use exclude_unset to only update what was in the payload
        update_data = obj_in.dict(exclude={"sub_limits", "entity_ids"}, exclude_unset=True) # Add entity_ids to exclude        
        for field, value in update_data.items():
            # Using hasattr and checking against None ensures booleans (False) are saved
            if hasattr(db_obj, field):
                setattr(db_obj, field, value)

        # 2. Update Entities (The Fix)
        if obj_in.entity_ids is not None:
            # Assuming your model has a many-to-many relationship called 'entities'
            from app.models import CustomerEntity # Import your entity model
            new_entities = db.query(CustomerEntity).filter(CustomerEntity.id.in_(obj_in.entity_ids)).all()
            db_obj.entities = new_entities

        # 2. Reconcile Sub-limits (safe: don't delete sub-limits referenced by exposure entries)
        if obj_in.sub_limits is not None:
            from app.models.models_issuance import IssuanceExposureEntry
            
            existing_sls = db.query(IssuanceFacilitySubLimit).filter(
                IssuanceFacilitySubLimit.facility_id == db_obj.id
            ).all()
            existing_by_id = {sl.id: sl for sl in existing_sls}
            
            # Collect IDs from incoming payload (existing ones being kept/updated)
            incoming_ids = set()
            for sl in obj_in.sub_limits:
                sl_data = sl.dict()
                sl_id = sl_data.pop("id", None)
                
                if sl_id and sl_id in existing_by_id:
                    # Update existing sub-limit in place
                    incoming_ids.add(sl_id)
                    existing_sl = existing_by_id[sl_id]
                    for field, value in sl_data.items():
                        if hasattr(existing_sl, field):
                            setattr(existing_sl, field, value)
                else:
                    # Create new sub-limit
                    new_sl = IssuanceFacilitySubLimit(
                        facility_id=db_obj.id,
                        **sl_data
                    )
                    db.add(new_sl)
            
            # Delete sub-limits that are no longer in the payload, but only if they have no exposure entries
            for old_id, old_sl in existing_by_id.items():
                if old_id not in incoming_ids:
                    has_refs = db.query(IssuanceExposureEntry).filter(
                        IssuanceExposureEntry.sub_limit_id == old_id
                    ).first()
                    if not has_refs:
                        db.delete(old_sl)
                    # If has references, keep it (don't break FK integrity)

        # 3. Log Audit
        audit = IssuanceFacilityAuditLog(
            facility_id=db_obj.id,
            performed_by_user_id=user_id,
            action="UPDATE",
            old_values=old_values,
            new_values=obj_in.dict(exclude_unset=True)
        )
        db.add(audit)

        db.commit()
        db.refresh(db_obj)
        return db_obj

    def soft_delete(self, db: Session, facility_id: int, user_id: int) -> Optional[IssuanceFacility]:
        """Mark as deleted and ARCHIVED to keep historical referential integrity."""
        facility = db.query(IssuanceFacility).get(facility_id)
        if facility:
            facility.deleted_at = datetime.now()
            facility.status = "ARCHIVED"
            
            db.add(IssuanceFacilityAuditLog(
                facility_id=facility.id,
                performed_by_user_id=user_id,
                action="DELETE"
            ))
            db.commit()
        return facility

    def restore(self, db: Session, facility_id: int, user_id: int) -> Optional[IssuanceFacility]:
        """Bring back an archived facility."""
        facility = db.query(IssuanceFacility).get(facility_id)
        if facility:
            facility.deleted_at = None
            facility.status = "ACTIVE"
            
            db.add(IssuanceFacilityAuditLog(
                facility_id=facility.id,
                performed_by_user_id=user_id,
                action="RESTORE"
            ))
            db.commit()
        return facility

crud_facility = CRUDFacility(IssuanceFacility)