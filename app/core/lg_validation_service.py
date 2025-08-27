# app/core/lg_validation_service.py
import re
from datetime import date, datetime
from typing import Dict, Any, List, Optional
import logging
from sqlalchemy import func, or_
from app.models import InternalOwnerContact, LGCategory, LgType, LgOperationalStatus, Currency, IssuingMethod, Bank, CustomerEntity, Rule
# NEW: Import necessary models to check for existence of related data
from app.schemas.migration_schemas import MigrationRecordStatusEnum

logger = logging.getLogger(__name__)

# UPDATED: Define a comprehensive error message mapping layer for all fields
ERROR_MAPPING = {
    "lg_number": {
        "Missing or empty field.": "Missing or empty LG number. Please provide a unique LG number (e.g., LG12345).",
        "Invalid format.": "LG number has an invalid format. Please use a valid format (e.g., alphanumeric).",
        "Duplicate entry. A newer version with the same LG number exists.": "Duplicate entry. A newer version with the same LG number exists.",
        "Instruction LG number does not exist in production.": "The provided LG number for this instruction does not exist in the main LG records table."
    },
    "lg_amount": {
        "Missing or empty field.": "Missing or empty amount. Must be a positive number greater than 0.",
        "Invalid number format.": "Invalid amount. Must be a number (e.g., 100000.00)."
    },
    "lg_currency_id": {
        "Missing or empty field.": "Missing or empty currency. Please provide a valid currency code or ID.",
        "Currency not found.": "The provided currency code or ID does not exist in the system."
    },
    "lg_payable_currency_id": {
        "Missing or empty field.": "Missing or empty payable currency. Please provide a valid currency code or ID.",
        "Currency not found.": "The provided payable currency ID does not exist."
    },
    "issuance_date": {
        "Missing or empty field.": "Missing or empty issuance date. Please provide a valid date.",
        "Invalid date format.": "Invalid issuance date format. Expected YYYY-MM-DD, e.g., 2024-05-20."
    },
    "expiry_date": {
        "Missing or empty field.": "Missing or empty expiry date. Please provide a valid date.",
        "Invalid date format.": "Invalid expiry date format. Expected YYYY-MM-DD, e.g., 2025-12-31.",
        "Must be after issuance date.": "Expiry date must be after the issuance date.",
        "Cannot be in the past.": "Expiry date cannot be in the past for new records."
    },
    "lg_type_id": {
        "Missing or empty field.": "Missing or empty LG type. Please select or provide a valid LG type name or ID.",
        "LG Type not found.": "The provided LG type name or ID does not exist."
    },
    "lg_operational_status_id": {
        "Missing or empty field.": "Operational Status is mandatory for this LG type.",
        "Field not applicable.": "Operational Status is only applicable for 'Advance Payment LG' type.",
        "Operational status not found.": "The provided operational status ID does not exist."
    },
    "payment_conditions": {
        "Missing or empty field.": "Payment Conditions are mandatory for this LG type and operational status.",
        "Field not applicable.": "Payment Conditions are only applicable for 'Advance Payment LG' with 'Non-Operative' status."
    },
    "description_purpose": {
        "Missing or empty field.": "Missing or empty description or purpose."
    },
    "issuer_name": {
        "Missing or empty field.": "Missing or empty issuer name."
    },
    "beneficiary_corporate_id": {
        "Missing or empty field.": "Missing or empty beneficiary corporate. Please select a beneficiary.",
        "Beneficiary not found.": "The provided beneficiary corporate ID does not exist for this customer."
    },
    "issuing_bank_id": {
        "Missing or empty field.": "Missing or empty issuing bank. Please select or provide a valid bank name or ID.",
        "Bank not found.": "The provided issuing bank name or ID does not exist."
    },
    "issuing_bank_address": {
        "Missing or empty field.": "Missing or empty issuing bank address."
    },
    "issuing_bank_phone": {
        "Missing or empty field.": "Missing or empty issuing bank phone number."
    },
    "issuing_method_id": {
        "Missing or empty field.": "Missing or empty issuing method. Please select an issuing method.",
        "Issuing method not found.": "The provided issuing method ID does not exist."
    },
    "applicable_rule_id": {
        "Missing or empty field.": "Missing or empty applicable rule. Please select a rule.",
        "Rule not found.": "The provided rule ID does not exist."
    },
    "applicable_rules_text": {
        "Missing or empty field.": "Applicable Rules Text is mandatory when Applicable Rule is 'Other'.",
        "Field not applicable.": "Applicable Rules Text is only applicable when Applicable Rule is 'Other'."
    },
    "internal_owner_contact_id": {
        "Missing or empty field.": "Missing or empty internal owner. Please select a valid internal owner or provide a valid email.",
        "Internal owner not found.": "The provided internal owner contact ID does not exist."
    },
    "internal_owner_email": {
        "Invalid email format.": "Invalid email format for internal owner."
    },
    "lg_category_id": {
        "Missing or empty field.": "Missing or empty category. Please select a category or provide a category code.",
        "Category not found.": "The provided category name, code, or ID does not exist for this customer or globally."
    }
}

class LGValidationService:
    def __init__(self):
        pass

    def _get_enhanced_error(self, field: str, message: str) -> str:
        """Translates a raw validation message into a user-friendly one."""
        return ERROR_MAPPING.get(field, {}).get(message, message)

    def validate_lg_data(self, record_data: Dict[str, Any], context: str = 'new_record', db: Optional[Any] = None, customer_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Performs comprehensive validation on LG data based on the provided context.
        Returns a dictionary of user-friendly validation errors.
        """
        raw_errors = {}
        
        # --- Mandatory Field Presence and Basic Format Checks ---
        mandatory_fields = [
            "lg_number", "lg_amount", "lg_currency_id", "issuance_date", "expiry_date",
            "lg_type_id", "description_purpose", "issuer_name", "beneficiary_corporate_id",
            "issuing_bank_id", "issuing_bank_address", "issuing_bank_phone",
            "issuing_method_id", "applicable_rule_id", "internal_owner_contact_id",
            "lg_category_id"
        ]
        
        for field in mandatory_fields:
            if not record_data.get(field):
                raw_errors[field] = "Missing or empty field."

        # --- Specific Field Format & Logic Checks ---
        if record_data.get("lg_amount"):
            try:
                if float(record_data["lg_amount"]) <= 0:
                    raw_errors["lg_amount"] = "Invalid number format."
            except (ValueError, TypeError):
                raw_errors["lg_amount"] = "Invalid number format."
        
        # Date Format and Consistency
        issuance_date_obj = None
        expiry_date_obj = None
        
        if record_data.get("issuance_date"):
            try:
                issuance_date_obj = datetime.strptime(str(record_data["issuance_date"]), "%Y-%m-%d").date()
            except (ValueError, TypeError):
                if not raw_errors.get("issuance_date"):
                    raw_errors["issuance_date"] = "Invalid date format."

        if record_data.get("expiry_date"):
            try:
                expiry_date_obj = datetime.strptime(str(record_data["expiry_date"]), "%Y-%m-%d").date()
            except (ValueError, TypeError):
                if not raw_errors.get("expiry_date"):
                    raw_errors["expiry_date"] = "Invalid date format."
        
        if issuance_date_obj and expiry_date_obj and expiry_date_obj <= issuance_date_obj:
            raw_errors["expiry_date"] = "Must be after issuance date."
        
        # --- Conditional Validation Rules ---
        
        # LG Operational Status
        lg_type = db.query(LgType).filter(LgType.id == record_data.get("lg_type_id")).first() if db and isinstance(record_data.get("lg_type_id"), int) else None
        if lg_type and lg_type.name == "Advance Payment LG":
            if not record_data.get("lg_operational_status_id"):
                raw_errors["lg_operational_status_id"] = "Missing or empty field."
        
        # Applicable Rules Text
        applicable_rule = db.query(Rule).filter(Rule.id == record_data.get("applicable_rule_id")).first() if db and isinstance(record_data.get("applicable_rule_id"), int) else None
        if applicable_rule and applicable_rule.name == "Other" and not record_data.get("applicable_rules_text"):
            raw_errors["applicable_rules_text"] = "Missing or empty field."
        
        # NEW: Consolidated Category Lookup and Validation
        lg_category_input = record_data.get("lg_category_id")
        resolved_category = None
        
        if lg_category_input:
            if isinstance(lg_category_input, int):
                # If it's an ID, try to find the category
                resolved_category = db.query(LGCategory).filter(
                    LGCategory.id == lg_category_input,
                    or_(
                        LGCategory.customer_id == customer_id,
                        LGCategory.customer_id.is_(None)
                    ),
                    LGCategory.is_deleted == False
                ).first()
            elif isinstance(lg_category_input, str):
                # If it's a string, perform a lookup by code or name, prioritizing customer-specific categories
                resolved_category = db.query(LGCategory).filter(
                    LGCategory.customer_id == customer_id,
                    LGCategory.is_deleted == False,
                    or_(
                        func.lower(LGCategory.code) == func.lower(lg_category_input),
                        func.lower(LGCategory.name) == func.lower(lg_category_input),
                    )
                ).first()
                if not resolved_category:
                    # Fallback to universal categories
                    resolved_category = db.query(LGCategory).filter(
                        LGCategory.customer_id.is_(None),
                        LGCategory.is_deleted == False,
                        or_(
                            func.lower(LGCategory.code) == func.lower(lg_category_input),
                            func.lower(LGCategory.name) == func.lower(lg_category_input),
                        )
                    ).first()
            
            # If a category was found and it's mandatory, validate the additional fields
            if resolved_category and resolved_category.is_mandatory:
                if not record_data.get("additional_field_values"):
                    raw_errors["additional_field_values"] = "Missing or empty field."
            elif not resolved_category:
                raw_errors["lg_category_id"] = "Category not found."


        # --- Database Existence Checks (if DB context provided) ---
        if db and customer_id:
            # Check for each ID against the database
            lookup_fields = {
                "lg_currency_id": Currency,
                "lg_payable_currency_id": Currency,
                "issuing_method_id": IssuingMethod,
                "applicable_rule_id": Rule,
                "lg_type_id": LgType,
                "lg_operational_status_id": LgOperationalStatus,
                "beneficiary_corporate_id": CustomerEntity,
                "issuing_bank_id": Bank,
            }
            # Note: lg_category_id is handled by the new logic above.

            for field, model in lookup_fields.items():
                field_value = record_data.get(field)
                if field_value and isinstance(field_value, int):
                    query = db.query(model).filter(model.id == field_value)
                    # Apply customer filter where applicable
                    if hasattr(model, 'customer_id'):
                        query = query.filter(model.customer_id == customer_id)
                    
                    if not query.first():
                        raw_errors[field] = f"{model.__name__} not found."
            
            # --- CRITICAL FIX: Add a more specific validation for internal_owner_contact_id ---
            if record_data.get("internal_owner_contact_id") and isinstance(record_data["internal_owner_contact_id"], int):
                owner_id = record_data["internal_owner_contact_id"]
                # Query for the owner, ensuring it belongs to the same customer_id
                owner = db.query(InternalOwnerContact).filter_by(id=owner_id, customer_id=customer_id).first()
                if not owner:
                    logger.warning(f"Validation failed for internal owner. ID {owner_id} does not exist or does not belong to customer {customer_id}.")
                    raw_errors["internal_owner_contact_id"] = "Invalid internal owner ID: must exist and belong to this customer."
                else:
                    logger.info(f"Internal owner validation passed for ID {owner_id} and customer {customer_id}.")

        # --- Final Error Generation ---
        if raw_errors:
            enhanced_errors = {}
            for field, message in raw_errors.items():
                enhanced_message = self._get_enhanced_error(field, message)
                enhanced_errors[field] = enhanced_message
            return enhanced_errors
        
        return {} # Return empty dict for no errors
        
    def validate_lg_instruction_data(self, record_data: Dict[str, Any], context: str = 'migration', db: Optional[Any] = None, customer_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Performs focused validation on LG instruction data.
        Assumes an LG record already exists in the production database.
        """
        raw_errors = {}
        
        # Instruction-specific validation rules
        if not record_data.get("lg_number"):
            raw_errors["lg_number"] = "Missing or empty field."

        # Check if the LG number exists in the production database
        if db and record_data.get("lg_number"):
            from app.models import LGRecord
            existing_lg = db.query(LGRecord).filter(
                func.lower(LGRecord.lg_number) == func.lower(record_data["lg_number"]),
                LGRecord.customer_id == customer_id,
                LGRecord.is_deleted == False
            ).first()
            if not existing_lg:
                raw_errors["lg_number"] = "Instruction LG number does not exist in production."

        if raw_errors:
            return {field: self._get_enhanced_error(field, msg) for field, msg in raw_errors.items()}
            
        return {} # Return empty dict for no errors


lg_validation_service = LGValidationService()