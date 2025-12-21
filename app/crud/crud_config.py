# app/crud/crud_config.py

import json
from typing import Any, Dict, List, Optional, Type, TypeVar
from fastapi import HTTPException, status
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.sql import func

from app.crud.crud import CRUDBase, log_action
from app.models import GlobalConfiguration, CustomerConfiguration, BaseModel
from app.schemas.all_schemas import (
    CustomerConfigurationCreate,
    CustomerConfigurationOut,
    CustomerConfigurationUpdate,
    GlobalConfigurationCreate,
    GlobalConfigurationOut,
    GlobalConfigurationUpdate,
)
from app.constants import GlobalConfigKey, AUDIT_ACTION_TYPE_UPDATE

from sqlalchemy import cast, String 


class CRUDGlobalConfiguration(CRUDBase):
    def get_by_key(
        self, db: Session, key: GlobalConfigKey
    ) -> Optional[GlobalConfiguration]:
        # REVERT TO ORIGINAL: We rely on the developer to ensure the database value 
        # exactly matches the GlobalConfigKey enum value string.
        return (
            db.query(self.model)
            .filter(self.model.key == key, self.model.is_deleted == False)
            .first()
        )

    def _validate_config_value(global_config, configured_value):
        if global_config.unit and global_config.unit.lower() == 'boolean':
            if configured_value.lower() not in ['true', 'false']:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Value for '{global_config.key.value}' must be 'true' or 'false'."
                )

    def create(self, db: Session, obj_in: BaseModel, **kwargs: Any) -> BaseModel:
        db_obj = super().create(db, obj_in, **kwargs)
        log_action(
            db,
            user_id=None,
            action_type="CREATE",
            entity_type="GlobalConfiguration",
            entity_id=db_obj.id,
            details={"key": db_obj.key.value, "value_default": db_obj.value_default},
        )
        return db_obj

    def update(
        self, db: Session, db_obj: BaseModel, obj_in: BaseModel, **kwargs: Any
    ) -> BaseModel:
        # Save old values to check for narrowing ranges
        old_value_min = db_obj.value_min
        old_value_max = db_obj.value_max

        updated_obj = super().update(db, db_obj, obj_in, **kwargs)
        if hasattr(updated_obj, "_changed_fields_for_log") and updated_obj._changed_fields_for_log:
            log_action(
                db,
                user_id=None,
                action_type="UPDATE",
                entity_type="GlobalConfiguration",
                entity_id=updated_obj.id,
                details={"key": updated_obj.key.value, "changes": updated_obj._changed_fields_for_log},
            )
            del updated_obj._changed_fields_for_log

        # Return old values for the API endpoint to decide whether to trigger the background task
        updated_obj.old_value_min = old_value_min
        updated_obj.old_value_max = old_value_max
        
        return updated_obj

    def soft_delete(self, db: Session, db_obj: BaseModel) -> BaseModel:
        deleted_obj = super().soft_delete(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="SOFT_DELETE",
            entity_type="GlobalConfiguration",
            entity_id=deleted_obj.id,
            details={"key": deleted_obj.key.value},
        )
        return deleted_obj

    def restore(self, db: Session, db_obj: BaseModel) -> BaseModel:
        restored_obj = super().restore(db, db_obj)
        log_action(
            db,
            user_id=None,
            action_type="RESTORE",
            entity_type="GlobalConfiguration",
            entity_id=restored_obj.id,
            details={"key": restored_obj.key.value},
        )
        return restored_obj


class CRUDCustomerConfiguration(CRUDBase):
    def __init__(self, model: Type[CustomerConfiguration], global_config_crud: 'CRUDGlobalConfiguration'):
        super().__init__(model)
        self.global_config_crud = global_config_crud

    def get_by_customer_and_global_config_id(
        self, db: Session, customer_id: int, global_config_id: int
    ) -> Optional[CustomerConfiguration]:
        return (
            db.query(self.model)
            .filter(
                self.model.customer_id == customer_id,
                self.model.global_config_id == global_config_id,
                self.model.is_deleted == False,
            )
            .first()
        )

    def get_customer_config_or_global_fallback(
        self, db: Session, customer_id: int, config_key: GlobalConfigKey
    ) -> Optional[Dict[str, Any]]:
        global_config = self.global_config_crud.get_by_key(db, config_key)
        if not global_config:
            # If global config key itself doesn't exist, return None or raise error
            return None 

        customer_config = self.get_by_customer_and_global_config_id(
            db, customer_id, global_config.id
        )

        effective_value = global_config.value_default
        source = "global"
        configured_value_for_output = None # Default to None if not overridden by customer

        if customer_config and customer_config.configured_value is not None:
            effective_value = customer_config.configured_value
            source = "customer"
            configured_value_for_output = customer_config.configured_value

        # MODIFIED RETURN: Include all required fields for CustomerConfigurationOut
        return {
            "id": customer_config.id if customer_config else None,
            "created_at": customer_config.created_at if customer_config else global_config.created_at, # Fallback for base fields
            "updated_at": customer_config.updated_at if customer_config else global_config.updated_at,
            "is_deleted": customer_config.is_deleted if customer_config else global_config.is_deleted,
            "deleted_at": customer_config.deleted_at if customer_config else global_config.deleted_at,
            "customer_id": customer_id, 
            "global_config_id": global_config.id, 
            "configured_value": configured_value_for_output, # Explicitly pass the configured_value if exists
            "global_config_key": global_config.key.value, # Correctly retrieve the string value of the enum
            "global_value_min": global_config.value_min,
            "global_value_max": global_config.value_max,
            "global_value_default": global_config.value_default,
            "global_unit": global_config.unit,
            "global_description": global_config.description,
            "effective_value": effective_value,
            "source": source, # Not part of schema, but useful for debugging
        }

    def get_all_customer_configs_for_customer(self, db: Session, customer_id: int) -> List[CustomerConfigurationOut]:
        all_global_configs = self.global_config_crud.get_all(db)
        customer_configs_map = {
            cc.global_config_id: cc
            for cc in db.query(CustomerConfiguration)
            .filter(CustomerConfiguration.customer_id == customer_id, CustomerConfiguration.is_deleted == False)
            .all()
        }

        result = []
        for gc in all_global_configs:
            customer_config = customer_configs_map.get(gc.id)
            effective_value = (
                customer_config.configured_value
                if customer_config and customer_config.configured_value is not None
                else gc.value_default
            )

            result.append(
                CustomerConfigurationOut(
                    id=customer_config.id if customer_config else None,
                    created_at=customer_config.created_at
                    if customer_config
                    else gc.created_at,
                    updated_at=customer_config.updated_at
                    if customer_config
                    else gc.updated_at,
                    is_deleted=customer_config.is_deleted
                    if customer_config
                    else gc.is_deleted,
                    deleted_at=customer_config.deleted_at
                    if customer_config
                    else gc.deleted_at,
                    customer_id=customer_id,
                    global_config_id=gc.id,
                    configured_value=customer_config.configured_value
                    if customer_config
                    else None, # Ensure configured_value is None if it's falling back
                    global_config_key=gc.key.value, # FIX: Correctly retrieve the string value of the enum
                    global_value_min=gc.value_min,
                    global_value_max=gc.value_max,
                    global_value_default=gc.value_default,
                    global_unit=gc.unit,
                    global_description=gc.description,
                    effective_value=effective_value,
                )
            )
        result.sort(key=lambda x: x.global_config_key) # Sort by the string value
        return result

    def set_customer_config(self, db: Session, customer_id: int, global_config_id: int, configured_value: str, user_id: int) -> CustomerConfiguration:
        # CRITICAL CHANGE: Use the stored instance
        global_config = self.global_config_crud.get(db, global_config_id)
        if not global_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Global configuration not found.",
            )

        try:
            if global_config.unit in ["days", "percentage"] or global_config.key == GlobalConfigKey.GRACE_PERIOD_DAYS:
                # Corrected logic for parsing numeric values, handling both int and float
                if "." in configured_value or "e" in configured_value.lower():
                    val = float(configured_value)
                else:
                    val = int(configured_value)
                
                min_val = float(global_config.value_min) if global_config.value_min is not None else None
                max_val = float(global_config.value_max) if global_config.value_max is not None else None

                if min_val is not None and val < min_val:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Configured value {configured_value} is below the minimum allowed value of {global_config.value_min} {global_config.unit}.",
                    )
                if max_val is not None and val > max_val:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Configured value {configured_value} exceeds the maximum allowed value of {global_config.value_max} {global_config.unit}.",
                    )
            elif global_config.unit == "boolean":
                if configured_value.lower() not in ['true', 'false']:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Value must be 'true' or 'false'.")
            elif global_config.key == GlobalConfigKey.COMMON_COMMUNICATION_LIST:
                try:
                    parsed_list = json.loads(configured_value)
                    if not isinstance(parsed_list, list) or not all(
                        isinstance(i, str) and "@" in i for i in parsed_list
                    ):
                        raise ValueError("Value must be a JSON array of valid email strings.")
                except json.JSONDecodeError:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Value must be a valid JSON array for communication list.")

        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Configured value '{configured_value}' is not valid for type '{global_config.unit}' of config '{global_config.key.value}'. Error: {e}",
            )
        
        customer_config = self.get_by_customer_and_global_config_id(
            db, customer_id, global_config_id
        )

        if customer_config:
            updated_config = super().update(
                db,
                db_obj=customer_config,
                obj_in=CustomerConfigurationUpdate(configured_value=configured_value),
            )
            log_action(
                db,
                user_id=user_id,
                action_type="UPDATE",
                entity_type="CustomerConfiguration",
                entity_id=updated_config.id,
                details={
                    "global_config_key": global_config.key.value,
                    "old_value": customer_config.configured_value,
                    "new_value": configured_value,
                },
                customer_id=customer_id,
            )
            return updated_config
        else:
            new_config = super().create(
                db,
                obj_in=CustomerConfigurationCreate(
                    global_config_id=global_config_id, configured_value=configured_value
                ),
                customer_id=customer_id,
            )
            log_action(
                db,
                user_id=user_id,
                action_type="CREATE",
                entity_type="CustomerConfiguration",
                entity_id=new_config.id,
                details={
                    "global_config_key": global_config.key.value,
                    "configured_value": configured_value,
                },
                customer_id=customer_id,
            )
            return new_config

    def revalidate_customer_configs_for_global_change(
        self, db: Session, global_config_id: int
    ) -> List[Dict[str, Any]]:
        """
        Revalidates and corrects customer configuration values when a global setting's
        range has been narrowed. Returns a list of corrected configurations.
        """
        global_config = self.global_config_crud.get(db, global_config_id)
        if not global_config:
            return []
        
        customer_configs_to_check = db.query(self.model).filter(
            self.model.global_config_id == global_config_id,
            self.model.is_deleted == False
        ).all()
        
        corrected_configs = []

        # 1. Determine validation limits
        new_min_val = float(global_config.value_min) if global_config.value_min is not None else None
        new_max_val = float(global_config.value_max) if global_config.value_max is not None else None
        
        # 2. Check if this config has numeric constraints
        # We assume if min/max exist, we must enforce them, regardless of unit name.
        # We explicitly skip 'boolean' as it doesn't use numeric ranges.
        is_boolean = global_config.unit and global_config.unit.lower() == 'boolean'
        has_range_constraints = (new_min_val is not None or new_max_val is not None)

        if not has_range_constraints or is_boolean:
            return []

        for cust_config in customer_configs_to_check:
            original_value = cust_config.configured_value
            corrected_value = None
            is_corrected = False

            # Only validate if the customer actually has a custom value set
            if original_value:
                try:
                    # Clean the string to handle " 5 " or "5 "
                    clean_val = str(original_value).strip()
                    current_val = float(clean_val)

                    # Check Min Constraint
                    if new_min_val is not None and current_val < new_min_val:
                        corrected_value = new_min_val
                        is_corrected = True
                    
                    # Check Max Constraint
                    elif new_max_val is not None and current_val > new_max_val:
                        corrected_value = new_max_val
                        is_corrected = True
                        
                except (ValueError, TypeError):
                    # LOGGING ADDED: This will tell you if data corruption is the cause
                    print(f"WARNING: Validation failed. Config ID {cust_config.id} has invalid value '{original_value}' not convertible to float.")
                    continue

            if is_corrected:
                # Format correction: e.g. 5.0 -> "5", 5.5 -> "5.5"
                if corrected_value.is_integer():
                    cust_config.configured_value = str(int(corrected_value))
                else:
                    cust_config.configured_value = str(corrected_value)
                
                db.add(cust_config)
                
                # Use a safe way to get the key string
                key_str = global_config.key.value if hasattr(global_config.key, 'value') else str(global_config.key)

                corrected_configs.append({
                    "customer_id": cust_config.customer_id,
                    "global_config_key": key_str,
                    "old_value": original_value,
                    "new_value": cust_config.configured_value
                })
        
        db.flush() 
        return corrected_configs
