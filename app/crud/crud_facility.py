# app/crud/crud_facility.py

from sqlalchemy.orm import Session
from typing import List, Optional

from app.models import Facility, FacilitySelectionRule, FacilityCustomerEntityAssociation, CustomerEntity
from app.schemas.all_schemas import FacilityCreate, FacilityUpdate, FacilitySelectionRuleCreate

# This file will contain the database interaction logic for the new Facility models.

def get_facility(db: Session, facility_id: int, customer_id: int) -> Optional[Facility]:
    """
    Retrieves a single facility, ensuring it belongs to the correct customer.
    """
    return db.query(Facility).filter(Facility.id == facility_id, Facility.customer_id == customer_id, Facility.is_deleted == False).first()

def get_all_facilities_for_customer(db: Session, customer_id: int) -> List[Facility]:
    """
    Retrieves all non-deleted facilities for a specific customer.
    """
    return db.query(Facility).filter(Facility.customer_id == customer_id, Facility.is_deleted == False).all()

def create_facility_with_entities(db: Session, obj_in: FacilityCreate, customer_id: int) -> Facility:
    """
    Creates a new facility and associates it with the specified entities.
    """
    # Create the facility object from the input schema
    db_facility = Facility(
        customer_id=customer_id,
        bank_id=obj_in.bank_id,
        facility_type=obj_in.facility_type,
        total_limit=obj_in.total_limit,
        currency_id=obj_in.currency_id,
        expiry_date=obj_in.expiry_date,
        pricing_details=obj_in.pricing_details,
        bank_sla_days=obj_in.bank_sla_days,
        special_conditions=obj_in.special_conditions,
        parent_facility_id=obj_in.parent_facility_id,
        has_all_entity_access=obj_in.has_all_entity_access
    )
    db.add(db_facility)
    db.flush() # Flush to get the ID for the new facility

    # Handle entity associations
    if not obj_in.has_all_entity_access and obj_in.entity_ids:
        for entity_id in obj_in.entity_ids:
            # Verify the entity belongs to the customer
            entity = db.query(CustomerEntity).filter(CustomerEntity.id == entity_id, CustomerEntity.customer_id == customer_id).first()
            if entity:
                association = FacilityCustomerEntityAssociation(facility_id=db_facility.id, customer_entity_id=entity_id)
                db.add(association)

    db.commit()
    db.refresh(db_facility)
    return db_facility

# ... Other placeholder CRUD functions ...
def update_facility(db: Session, facility_id: int, obj_in: FacilityUpdate, customer_id: int) -> Optional[Facility]:
    return None

def soft_delete_facility(db: Session, facility_id: int, customer_id: int) -> Optional[Facility]:
    return None

def get_facility_selection_rules(db: Session, customer_id: int) -> List[FacilitySelectionRule]:
    return []

def set_facility_selection_rules(db: Session, customer_id: int, rules_in: List[FacilitySelectionRuleCreate]) -> List[FacilitySelectionRule]:
    return []
