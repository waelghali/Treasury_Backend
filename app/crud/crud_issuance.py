# app/crud/crud_issuance.py

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session, selectinload 
from sqlalchemy import desc
from fastapi import HTTPException
from datetime import datetime
from decimal import Decimal

from app.crud.crud import CRUDBase, log_action
from app.models.models_issuance import (
    IssuanceRequest, IssuedLGRecord, IssuanceRequestSnapshot, 
    IssuanceRequestVersion, CustomerFormConfiguration
)
from app.models import Customer
from app.schemas.schemas_issuance import IssuanceRequestCreate, IssuanceRequestUpdate, IssuanceRequestDraftCreate

# Fields that trigger full re-approval when edited after submission.
# All other field edits are "safe" — logged + notified, no re-approval needed.
RE_APPROVAL_TRIGGER_FIELDS = {
    'issuing_entity_id',
    'lg_type_id',
    'amount',
    'currency_id',
    'payable_currency_id',
    'requested_expiry_date',
    'operational_status',
    'beneficiary_name',
    'is_third_party',
    'third_party_name',
    'is_cross_border',
}

class CRUDIssuanceRequest(CRUDBase):
    
    def _generate_serial(self, db: Session, customer_id: int, entity_code: str = None) -> str:
        """Generates a sequential {PREFIX}-YYYY-XXXX number.
        PREFIX = entity code if available, otherwise customer name abbreviation."""
        current_year = datetime.now().year
        if entity_code:
            prefix = entity_code.upper()
        else:
            # Fallback: use first 3 letters of customer name
            customer = db.query(Customer).filter(Customer.id == customer_id).first()
            prefix = (customer.name.replace(" ", "").upper()[:3].ljust(3, 'X')) if customer else "REQ"
        count = db.query(IssuanceRequest).filter(
            IssuanceRequest.customer_id == customer_id,
            IssuanceRequest.serial_number.like(f"{prefix}-{current_year}-%")
        ).count()
        return f"{prefix}-{current_year}-{(count + 1):04d}"

    def get_by_customer(self, db: Session, customer_id: int, skip: int = 0, limit: int = 100) -> List[IssuanceRequest]:
        return db.query(self.model).filter(self.model.customer_id == customer_id)\
            .options(selectinload(IssuanceRequest.currency), selectinload(IssuanceRequest.lg_record))\
            .order_by(desc(self.model.created_at)).offset(skip).limit(limit).all()

    def create_request(self, db: Session, obj_in, customer_id: int, user_id: Optional[int] = None) -> IssuanceRequest:
        # 1. Fetch Customer
        customer = db.query(Customer).filter(Customer.id == customer_id).first()
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        # 2. Generate Serial (with entity code prefix if available)
        data = obj_in.model_dump(exclude_unset=True)
        entity_code = None
        if data.get("issuing_entity_id"):
            from app.models import CustomerEntity
            entity = db.query(CustomerEntity).filter(CustomerEntity.id == data["issuing_entity_id"]).first()
            if entity and entity.code:
                entity_code = entity.code
        serial = self._generate_serial(db, customer_id, entity_code)
        
        # 3. Create Draft
        db_obj = IssuanceRequest(
            **data,
            customer_id=customer_id,
            requestor_user_id=user_id,
            serial_number=serial,
            status="DRAFT",
            current_version_number=1,
            locked_for_issuance=False
        )
        
        db.add(db_obj)
        db.flush()
        
        # Log Creation
        log_action(
            db, user_id, "REQUEST_CREATED", "IssuanceRequest", 
            db_obj.id, {"serial": serial}, customer_id
        )
        
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def submit_request(self, db: Session, request_id: int, customer_id: int, user_id: Optional[int] = None) -> IssuanceRequest:
        """Transitions DRAFT -> SUBMITTED and captures immutable V1 Snapshot."""
        req = db.query(IssuanceRequest).filter(IssuanceRequest.id == request_id, IssuanceRequest.customer_id == customer_id).first()
        if not req:
            raise HTTPException(status_code=404, detail="Request not found")
            
        if req.status != "DRAFT":
            raise HTTPException(status_code=400, detail="Only DRAFT requests can be submitted")

        # Validation 1: Hard Business Rules
        if req.lg_type_id == 3 and not req.operational_status: # Assuming 3 is Advance Payment
            raise HTTPException(status_code=400, detail="Advance Payment LGs require operational_status")
        
        # (Optional Future Step): Validate against CustomerFormConfiguration here
        
        # (Optional Future Step): Validate Documents here

        # 1. Update Status
        req.status = "SUBMITTED"
        
        # 2. Create Immutable Snapshot (V1)
        # Convert model to dict safely handling Decimals and Dates
        snapshot_data = {
            col.name: str(getattr(req, col.name)) if getattr(req, col.name) is not None else None 
            for col in req.__table__.columns
        }
        
        snapshot = IssuanceRequestSnapshot(
            request_id=req.id,
            snapshot_data=snapshot_data
        )
        db.add(snapshot)
        db.flush()

        log_action(db, user_id, "REQUEST_SUBMITTED", "IssuanceRequest", req.id, {"version": req.current_version_number}, customer_id)
        db.commit()
        db.refresh(req)
        return req

    def update_request(self, db: Session, request_id: int, obj_in: IssuanceRequestUpdate, customer_id: int, user_id: int) -> IssuanceRequest:
        """Edit a request. Applies conditional re-approval governance for post-submission edits."""
        req = db.query(IssuanceRequest).filter(IssuanceRequest.id == request_id, IssuanceRequest.customer_id == customer_id).first()
        if not req:
            raise HTTPException(status_code=404, detail="Request not found")
            
        if req.locked_for_issuance or req.status == "ISSUED":
            raise HTTPException(status_code=403, detail="Request is locked for execution and cannot be edited.")

        if req.status in ("CANCELLED",):
            raise HTTPException(status_code=400, detail="Cancelled requests cannot be edited.")

        update_data = obj_in.model_dump(exclude_unset=True)
        change_reason = update_data.pop("change_reason", None)
        
        # 1. Calculate Diff
        changed_fields = {}
        has_risky_changes = False
        risky_fields_changed = []
        
        for field, new_value in update_data.items():
            old_value = getattr(req, field, None)
            # Normalize for comparison (Decimal vs float, etc.)
            if isinstance(old_value, Decimal) and isinstance(new_value, (int, float)):
                new_value = Decimal(str(new_value))
            if old_value != new_value:
                changed_fields[field] = {
                    "old": str(old_value) if old_value is not None else None, 
                    "new": str(new_value) if new_value is not None else None
                }
                if field in RE_APPROVAL_TRIGGER_FIELDS:
                    has_risky_changes = True
                    risky_fields_changed.append(field)

        if not changed_fields:
            return req  # No changes made

        # 2. Post-Submission Governance
        re_approval_triggered = False
        if req.status in ("SUBMITTED", "PENDING_APPROVAL", "APPROVED"):
            if not change_reason:
                raise HTTPException(
                    status_code=400, 
                    detail="A change_reason is mandatory when editing a request that has been submitted."
                )
            
            if has_risky_changes:
                # === RE-APPROVAL REQUIRED ===
                # Reset approval chain so it restarts from step 1
                req.status = "PENDING_APPROVAL"
                req.current_approval_step = 1
                req.signatures_collected = 0
                re_approval_triggered = True
                log_action(
                    db, user_id, "RE_APPROVAL_TRIGGERED", "IssuanceRequest", req.id, 
                    {"risky_fields": risky_fields_changed, "all_changes": changed_fields, "reason": change_reason},
                    customer_id
                )
            else:
                # === SAFE EDIT — notify only ===
                log_action(
                    db, user_id, "SAFE_EDIT_FYI", "IssuanceRequest", req.id, 
                    {"changes": changed_fields, "reason": change_reason},
                    customer_id
                )

        # 3. Apply Updates to Record
        for field, new_value in update_data.items():
            setattr(req, field, new_value)
            
        # 4. Create Linear Version (N+1)
        req.current_version_number += 1
        
        version_log = IssuanceRequestVersion(
            request_id=req.id,
            version_number=req.current_version_number,
            edited_by_user_id=user_id,
            change_reason=change_reason,
            changed_fields=changed_fields
        )
        db.add(version_log)
        
        log_action(
            db, user_id, "REQUEST_EDITED", "IssuanceRequest", req.id, 
            {"version": req.current_version_number, "fields": list(changed_fields.keys())}, 
            customer_id
        )
        
        db.commit()
        db.refresh(req)

        # Attach metadata for the caller (not persisted)
        req._edit_metadata = {
            "re_approval_triggered": re_approval_triggered,
            "risky_fields_changed": risky_fields_changed,
            "safe_fields_changed": [f for f in changed_fields if f not in RE_APPROVAL_TRIGGER_FIELDS],
            "change_reason": change_reason,
        }
        return req

    def get_single(self, db: Session, request_id: int, customer_id: int) -> IssuanceRequest:
        """Get a single request by ID with eager-loaded relationships."""
        req = db.query(IssuanceRequest).filter(
            IssuanceRequest.id == request_id,
            IssuanceRequest.customer_id == customer_id
        ).options(
            selectinload(IssuanceRequest.currency),
            selectinload(IssuanceRequest.lg_type),
            selectinload(IssuanceRequest.lg_record),
            selectinload(IssuanceRequest.issuing_entity),
        ).first()
        if not req:
            raise HTTPException(status_code=404, detail="Request not found")
        return req

    def delete_draft(self, db: Session, request_id: int, customer_id: int, user_id: int) -> dict:
        """Hard-delete a DRAFT request. Only drafts can be deleted."""
        req = db.query(IssuanceRequest).filter(
            IssuanceRequest.id == request_id,
            IssuanceRequest.customer_id == customer_id
        ).first()
        if not req:
            raise HTTPException(status_code=404, detail="Request not found")
        if req.status != "DRAFT":
            raise HTTPException(status_code=400, detail="Only DRAFT requests can be deleted")
        
        # Clean up related records
        from app.models.models_issuance import IssuanceRequestDocument, IssuanceRequestVersion, IssuanceRequestSnapshot
        db.query(IssuanceRequestDocument).filter(IssuanceRequestDocument.request_id == request_id).delete()
        db.query(IssuanceRequestVersion).filter(IssuanceRequestVersion.request_id == request_id).delete()
        db.query(IssuanceRequestSnapshot).filter(IssuanceRequestSnapshot.request_id == request_id).delete()
        
        serial = req.serial_number
        db.delete(req)
        
        log_action(db, user_id, "DRAFT_DELETED", "IssuanceRequest", request_id, {"serial": serial}, customer_id)
        db.commit()
        return {"detail": f"Draft {serial} deleted successfully"}

crud_issuance_request = CRUDIssuanceRequest(IssuanceRequest)