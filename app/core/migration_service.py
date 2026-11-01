# c:\Grow\app\core\migration_service.py

import os
import re
import sys
import io
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple, Type
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from pydantic import ValidationError
import logging
import requests
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, or_, cast
from sqlalchemy.dialects.postgresql import JSONB

from app.schemas.all_schemas import LGRecordCreate, LGDocumentCreate, LGInstructionCreate, LGRecordAmendRequest
from app.schemas.migration_schemas import MigrationRecordStatusEnum, MigrationTypeEnum

from app.models import LGRecord, LGInstruction, Bank, IssuingMethod, Rule, CustomerEntity, LgType, LgOperationalStatus, LGCategory, Currency, InternalOwnerContact

from app.constants import (
    DOCUMENT_TYPE_ORIGINAL_LG,
    DOCUMENT_TYPE_INTERNAL_SUPPORTING,
    ACTION_TYPE_LG_AMEND,
    InstructionTypeCode,
    SubInstructionCode,
)

logger = logging.getLogger(__name__)

# =====================================================================================
# MOVED FROM endpoints/migration.py TO BREAK CIRCULAR IMPORT
# =====================================================================================
def _get_id_by_name(db: Session, model: Any, name: str) -> Optional[int]:
    """Finds an ID by a unique name, short name, or former name in a given table."""
    if model == Bank:
        obj = db.query(model).filter(
            or_(
                func.lower(model.name) == func.lower(name),
                func.lower(model.short_name) == func.lower(name),
                cast(model.former_names, JSONB).op('?')(name)
            )
        ).first()
        return obj.id if obj else None
        
    obj = db.query(model).filter(func.lower(model.name) == func.lower(name)).first()
    return obj.id if obj else None
    
def calculate_lg_period_months(issuance_date: date, expiry_date: date) -> Optional[int]:
    """
    Calculates the LG period in months based on business rules.
    """
    if not issuance_date or not expiry_date:
        return None

    delta = relativedelta(expiry_date, issuance_date)
    total_months = delta.years * 12 + delta.months
    if delta.days > 0:
        total_months += 1

    rounded_months = int(round(total_months / 3)) * 3

    clamped_months = max(3, min(12, rounded_months))

    return clamped_months

def _apply_defaults_and_autofill(
    db: Session, 
    record_data: Dict[str, Any], 
    customer_id: int,
    # Inject dependencies here
    crud_internal_owner_contact: Any,
    crud_lg_type: Any,
    crud_issuing_method: Any,
    crud_rule: Any,
    crud_bank: Any,
    crud_customer_entity: Any,
    crud_lg_category: Any,
    crud_currency: Any,
) -> Dict[str, Any]:
    """
    Applies defaults and attempts to autofill data for a single record.
    Returns the updated record_data dictionary.
    """
    logger.debug("Autofill started for record.")
    try:
        from app.api.v1.endpoints.migration import normalize_record_keys
        normalize_record_keys(record_data)
    except Exception:
        pass
    
    # --- Step 1: Autofill Internal Owner Contact (FIXED) ---
    existing_owner_id = record_data.get("internal_owner_contact_id")
    email_input = record_data.get("internal_owner_email")

    # FIX 1: If ID is a digit string (e.g. "5"), convert it to int immediately
    if isinstance(existing_owner_id, str) and existing_owner_id.isdigit():
        existing_owner_id = int(existing_owner_id)
        record_data["internal_owner_contact_id"] = existing_owner_id

    # FIX 2: If ID looks like an email, move it to email_input
    elif isinstance(existing_owner_id, str) and "@" in existing_owner_id:
        email_input = existing_owner_id
        record_data["internal_owner_contact_id"] = None # Clear invalid ID
    
    # Now run the lookup or auto-create if we have an email
    if email_input:
        clean_email = str(email_input).strip().lower()
        owner = crud_internal_owner_contact.get_by_email_for_customer(db, customer_id, clean_email)
        if not owner and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", clean_email):
            from app.schemas.all_schemas import InternalOwnerContactCreate
            raw_phone = record_data.get("internal_owner_phone")
            if raw_phone is not None and not isinstance(raw_phone, str):
                phone_str = str(int(raw_phone)).strip() if isinstance(raw_phone, float) and raw_phone.is_integer() else str(raw_phone).strip()
            else:
                phone_str = str(raw_phone).strip() if raw_phone else ""
            if not phone_str or phone_str.upper() in ("NAN", "NONE", "N/A"):
                phone_str = "0000000000"

            raw_mgr = record_data.get("manager_email") or record_data.get("internal_owner_manager_email")
            mgr_str = str(raw_mgr).strip().lower() if raw_mgr else ""
            if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", mgr_str):
                mgr_str = clean_email

            raw_int_id = record_data.get("internal_owner_id")
            if raw_int_id is not None and not isinstance(raw_int_id, str):
                int_id_str = str(int(raw_int_id)).strip() if isinstance(raw_int_id, float) and raw_int_id.is_integer() else str(raw_int_id).strip()
            else:
                int_id_str = str(raw_int_id).strip() if raw_int_id else None
            if int_id_str and int_id_str.upper() in ("NAN", "NONE", "N/A"):
                int_id_str = None
            if int_id_str:
                int_id_str = int_id_str[:10]

            try:
                owner = crud_internal_owner_contact.create_or_get(
                    db,
                    obj_in=InternalOwnerContactCreate(
                        email=clean_email,
                        phone_number=phone_str,
                        internal_id=int_id_str,
                        manager_email=mgr_str,
                    ),
                    customer_id=customer_id,
                    user_id=None,
                )
                logger.info(f"Auto-created internal_owner_contact_id: {owner.id} for {clean_email}")
            except Exception as e:
                logger.warning(f"Failed to auto-create internal owner for '{clean_email}': {e}")

        if owner:
            record_data["internal_owner_contact_id"] = owner.id
            record_data["internal_owner_email"] = owner.email 
            if not record_data.get("internal_owner_phone"):
                record_data["internal_owner_phone"] = owner.phone_number
            if not record_data.get("manager_email"):
                record_data["manager_email"] = owner.manager_email
            logger.debug(f"Autofilled internal_owner_contact_id: {owner.id}")
        else:
            record_data["internal_owner_contact_id"] = None

    # --- Step 2: Resolve Operational Status (NEW FIX) ---
    op_status_val = record_data.get("lg_operational_status_id")
    if isinstance(op_status_val, str):
        # Map common string names to your specific database IDs
        # Update these integer IDs (1, 2, 3...) to match your LGOperationalStatus table
        status_map = {
            "OPERATIVE": 1,
            "NON-OPERATIVE": 2
        }
        clean_status = op_status_val.strip().upper()
        if clean_status in status_map:
            record_data["lg_operational_status_id"] = status_map[clean_status]
            logger.debug(f"Resolved operational status '{op_status_val}' to ID: {status_map[clean_status]}")
        else:
            # Default to Operative (ID 1) if unknown string provided
            record_data["lg_operational_status_id"] = 1

    # Normalize attachment alias keys if present
    if not record_data.get("attachment_url"):
        for alias in ("attachement", "attachment", "file_path", "document", "pdf", "scan"):
            if record_data.get(alias):
                record_data["attachment_url"] = str(record_data.pop(alias)).strip()
                break

    # Normalize lg_number and string fields if parsed as numeric by Excel/JSON
    if record_data.get("lg_number") is not None and not isinstance(record_data.get("lg_number"), str):
        val = record_data["lg_number"]
        record_data["lg_number"] = str(int(val)).strip() if isinstance(val, float) and val.is_integer() else str(val).strip()
    elif isinstance(record_data.get("lg_number"), str):
        record_data["lg_number"] = record_data["lg_number"].strip()

    for str_field in ["issuing_bank_phone", "issuing_bank_fax", "internal_contract_project_id", "notes"]:
        if record_data.get(str_field) is not None and not isinstance(record_data.get(str_field), str):
            val = record_data[str_field]
            record_data[str_field] = str(int(val)).strip() if isinstance(val, float) and val.is_integer() else str(val).strip()

    # --- Step 3: Generic Lookups (LG Type, Issuing Method, Rules) ---
    lookup_map = {
        "lg_type_id": (crud_lg_type, "lg_type"),
        "issuing_method_id": (crud_issuing_method, "issuing_method"),
        "applicable_rule_id": (crud_rule, "applicable_rule"),
    }
    
    for id_field, (crud_obj, name_field) in lookup_map.items():
        val = record_data.get(id_field)
        # If ID is missing or is a string name, look it up
        if not isinstance(val, int):
            lookup_val = val if val else record_data.get(name_field)
            if lookup_val and isinstance(lookup_val, str):
                clean_val = lookup_val.strip()
                obj = crud_obj.get_by_name(db, clean_val)
                if not obj and id_field == "lg_type_id" and clean_val:
                    obj = (
                        db.query(LgType)
                        .filter(LgType.is_deleted == False, func.lower(LgType.name) == clean_val.lower())
                        .first()
                        or db.query(LgType)
                        .filter(LgType.is_deleted == False, func.lower(LgType.name).like(f"{clean_val.lower()}%"))
                        .first()
                        or db.query(LgType)
                        .filter(LgType.is_deleted == False, func.lower(LgType.name).like(f"%{clean_val.lower()}%"))
                        .first()
                    )
                if obj:
                    record_data[id_field] = obj.id

    # --- Step 4: Handle Issuing Bank (Fuzzy/Cleaned lookup) ---
    issuing_bank_val = record_data.get("issuing_bank_id")
    if isinstance(issuing_bank_val, str):
        # This uses the helper you likely have in migration_service or core
        from app.models import Bank
        from sqlalchemy import func, or_, cast
        from sqlalchemy.dialects.postgresql import JSONB

        bank_name_clean = issuing_bank_val.strip()
        bank = db.query(Bank).filter(
            or_(
                func.lower(Bank.name) == func.lower(bank_name_clean),
                func.lower(Bank.short_name) == func.lower(bank_name_clean),
                cast(Bank.former_names, JSONB).op('?')(bank_name_clean)
            )
        ).first()
        
        if bank:
            record_data["issuing_bank_id"] = bank.id
            # Fill secondary bank details
            record_data["issuing_bank_address"] = record_data.get("issuing_bank_address") or bank.address
            record_data["issuing_bank_phone"] = record_data.get("issuing_bank_phone") or bank.phone_number
            record_data["issuing_bank_fax"] = record_data.get("issuing_bank_fax") or bank.fax
        else:
            record_data["issuing_bank_id"] = None

    # --- Step 5: Handle Beneficiary Corporate (Customer Specific) ---
    beneficiary_val = record_data.get("beneficiary_corporate_id")
    if isinstance(beneficiary_val, str):
        entity = crud_customer_entity.get_by_name_for_customer(db, customer_id, beneficiary_val.strip())
        if entity:
            record_data["beneficiary_corporate_id"] = entity.id
        else:
            # Fallback: if name lookup fails, check if customer has exactly one entity
            from app.models import CustomerEntity
            entities = db.query(CustomerEntity).filter(
                CustomerEntity.customer_id == customer_id, 
                CustomerEntity.is_deleted == False
            ).all()
            if len(entities) == 1:
                record_data["beneficiary_corporate_id"] = entities[0].id

    # --- Step 6: LG Category (Customer Specific vs Universal) ---
    cat_val = record_data.get("lg_category_id")
    cat_obj = None
    if isinstance(cat_val, str) and cat_val.strip():
        clean_cat = cat_val.strip()
        # Try Customer Category -> Try Universal Category
        cat_obj = (
            crud_lg_category.get_by_code(db, clean_cat, customer_id)
            or crud_lg_category.get_by_name(db, clean_cat, customer_id)
            or crud_lg_category.get_by_code(db, clean_cat, None)
            or crud_lg_category.get_by_name(db, clean_cat, None)
        )
    elif isinstance(cat_val, int):
        cat_obj = crud_lg_category.get(db, cat_val)

    if cat_obj:
        record_data["lg_category_id"] = cat_obj.id
    else:
        default_cat = (
            crud_lg_category.get_default_category(db, None)
            or crud_lg_category.get_default_category(db, customer_id)
            or db.query(LGCategory).filter(LGCategory.customer_id.is_(None), LGCategory.is_deleted == False).order_by(LGCategory.id.asc()).first()
            or db.query(LGCategory).filter(LGCategory.customer_id == customer_id, LGCategory.is_deleted == False).order_by(LGCategory.id.asc()).first()
        )
        record_data["lg_category_id"] = default_cat.id if default_cat else None

    # --- Step 7: Currencies ---
    for cur_field in ["lg_currency_id", "lg_payable_currency_id"]:
        cur_val = record_data.get(cur_field)
        if isinstance(cur_val, str):
            currency = crud_currency.get_by_iso_code(db, cur_val.strip().upper())
            if currency:
                record_data[cur_field] = currency.id

    # --- Step 8: Dynamic Calculations (Dates and Periods) ---
    issuance_date_str = record_data.get("issuance_date")
    expiry_date_str = record_data.get("expiry_date")
    if issuance_date_str and expiry_date_str:
        try:
            from datetime import datetime
            # Ensure we are working with date objects for the calculation
            i_date = datetime.strptime(str(issuance_date_str).split(' ')[0], "%Y-%m-%d").date()
            e_date = datetime.strptime(str(expiry_date_str).split(' ')[0], "%Y-%m-%d").date()
            
            # Assuming calculate_lg_period_months is imported or available in scope
            from app.api.v1.endpoints.migration import calculate_lg_period_months
            record_data['lg_period_months'] = calculate_lg_period_months(i_date, e_date)
        except Exception as e:
            logger.warning(f"Period calculation failed: {e}")

    # Set hard defaults if still missing
    record_data["auto_renewal"] = record_data.get("auto_renewal", True)
    
    # Ensure payable currency matches if not specified
    if not record_data.get("lg_payable_currency_id"):
        record_data["lg_payable_currency_id"] = record_data.get("lg_currency_id")

    return record_data
    
class MigrationService:
    def __init__(self):
        pass

    async def _fetch_file_content(
        self,
        url: str,
        customer_id: Optional[int] = None,
        db: Optional[Session] = None,
    ) -> Tuple[Optional[bytes], Optional[str]]:
        """
        Fetches and decrypts (if encrypted in staging) file content from:
        1. Encrypted Server Staging (/tmp/migration_attachments/customer_{id}/{file}.enc)
        2. Local Disk (if running locally)
        3. HTTP/HTTPS URL (if shared web URL)
        4. Google Cloud Storage (encrypted migration_staging or legacy migration folder)
        """
        import mimetypes
        import os
        import tempfile
        import glob
        from app.core.security_crypto import decrypt_stream_generator
        from app.services.tenant_key_service import tenant_key_service
        from app.core.storage_service import build_customer_blob_path

        clean_url = str(url).strip().strip("'").strip('"').replace("\u202a", "").replace("\u202c", "")
        normalized_url = clean_url.replace("\\", "/")
        file_name = os.path.basename(normalized_url)
        mime_type, _ = mimetypes.guess_type(file_name)
        resolved_mime = mime_type or "application/pdf"

        tenant_dek = None
        if db is not None and customer_id is not None:
            try:
                tenant_dek = tenant_key_service.get_or_create_tenant_dek(db, customer_id)
            except Exception as dek_err:
                logger.warning(f"Could not load tenant DEK for customer {customer_id}: {dek_err}")

        # --- STRATEGY 1: Check Encrypted Staged Uploads (.enc) & Local Disk ---
        candidate_paths = []
        temp_base = os.path.join(tempfile.gettempdir(), "migration_attachments")
        if customer_id is not None:
            candidate_paths.append(os.path.join(temp_base, f"customer_{customer_id}", f"{file_name}.enc"))
            candidate_paths.append(os.path.join(temp_base, f"customer_{customer_id}", file_name))
        if os.path.exists(temp_base):
            for match in glob.glob(os.path.join(temp_base, "*", f"{file_name}.enc")) + glob.glob(os.path.join(temp_base, "*", file_name)):
                if match not in candidate_paths:
                    candidate_paths.append(match)
        candidate_paths.extend([clean_url, file_name])

        final_path = None
        for p in candidate_paths:
            if p and os.path.exists(p) and os.path.isfile(p):
                final_path = p
                break

        if final_path:
            try:
                with open(final_path, "rb") as f:
                    if tenant_dek:
                        # decrypt_stream_generator is dual-read safe (decrypts STREAM V2 or passes plaintext through)
                        decrypted_bytes = b"".join(decrypt_stream_generator(f, tenant_dek, context="migration_staging"))
                    else:
                        decrypted_bytes = f.read()
                logger.info(f"Loaded encrypted migration attachment ({len(decrypted_bytes)} bytes) from local/staged storage for customer {customer_id}")
                return decrypted_bytes, resolved_mime
            except Exception as e:
                logger.warning(f"Local/staged read failed for customer {customer_id}: {e}")

        # --- STRATEGY 2: Direct HTTP/HTTPS URL ---
        if clean_url.lower().startswith(("http://", "https://")):
            try:
                import asyncio
                resp = await asyncio.to_thread(requests.get, clean_url, timeout=20)
                if resp.status_code == 200 and resp.content:
                    content_type = resp.headers.get("Content-Type") or resolved_mime
                    logger.info(f"Downloaded migration attachment ({len(resp.content)} bytes) via HTTPS for customer {customer_id}")
                    return resp.content, content_type
            except Exception as http_err:
                logger.warning(f"HTTP fetch failed for customer {customer_id}: {http_err}")

        # --- STRATEGY 3: Check Google Cloud Storage (GCS) ---
        try:
            bucket_name = os.getenv("GCS_BUCKET_NAME")
            if not bucket_name and db and customer_id:
                try:
                    from app.crud.crud import crud_customer_configuration
                    from app.constants import GlobalConfigKey
                    cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
                        db, customer_id, GlobalConfigKey.STORAGE_BUCKET_NAME
                    )
                    if cfg and cfg.get("effective_value"):
                        bucket_name = cfg["effective_value"]
                except Exception:
                    pass

            if bucket_name:
                def fetch_from_gcs():
                    from app.core.ai_integration import _get_gcs_client
                    client = _get_gcs_client()
                    bucket = client.bucket(bucket_name)

                    candidate_blobs = []
                    if customer_id is not None:
                        candidate_blobs.append(build_customer_blob_path(customer_id, "migration_staging", file_name))
                        candidate_blobs.append(f"customer_{customer_id}/migration/{file_name}")
                    candidate_blobs.extend([f"migration/{file_name}", file_name])

                    for blob_path in candidate_blobs:
                        blob = bucket.blob(blob_path)
                        if blob.exists():
                            logger.info(f"Found migration attachment in cloud staging for customer {customer_id}.")
                            raw_bytes = blob.download_as_bytes()
                            if tenant_dek:
                                try:
                                    return b"".join(
                                        decrypt_stream_generator(io.BytesIO(raw_bytes), tenant_dek, context="migration_staging")
                                    )
                                except Exception:
                                    return raw_bytes
                            return raw_bytes
                    return None

                import asyncio
                content = await asyncio.to_thread(fetch_from_gcs)

                if content:
                    return content, resolved_mime

        except ImportError:
            logger.error("google-cloud-storage library not installed.")
        except Exception as e:
            logger.error(f"GCS Fetch Error: {e}")

        logger.warning(f"Migration attachment not found in staged uploads, local disk, or cloud for customer {customer_id}")
        return None, None

    def _cleanup_migration_staging_file(
        self,
        url: str,
        customer_id: Optional[int],
        db: Optional[Session] = None,
    ) -> None:
        """
        Permanently removes temporary staged migration copies (local .enc and GCS migration_staging)
        once the file has been encrypted and moved into its final lg_custody cloud folder.
        """
        import os
        import tempfile
        from app.core.storage_service import build_customer_blob_path

        try:
            clean_url = str(url).strip().strip("'").strip('"').replace("\u202a", "").replace("\u202c", "")
            file_name = os.path.basename(clean_url.replace("\\", "/"))
            if not file_name or customer_id is None:
                return

            # 1. Remove local temporary staged files (.enc and raw)
            base_dir = os.path.join(tempfile.gettempdir(), "migration_attachments", f"customer_{customer_id}")
            for candidate in (os.path.join(base_dir, f"{file_name}.enc"), os.path.join(base_dir, file_name)):
                if os.path.exists(candidate):
                    try:
                        os.remove(candidate)
                        logger.info(f"Cleaned up temporary staged migration file for customer {customer_id}")
                    except Exception as rm_err:
                        logger.warning(f"Failed to remove temporary staged file for customer {customer_id}: {rm_err}")

            # 2. Remove temporary GCS migration_staging blob if present
            bucket_name = os.getenv("GCS_BUCKET_NAME")
            if not bucket_name and db:
                try:
                    from app.crud.crud import crud_customer_configuration
                    from app.constants import GlobalConfigKey
                    cfg = crud_customer_configuration.get_customer_config_or_global_fallback(
                        db, customer_id, GlobalConfigKey.STORAGE_BUCKET_NAME
                    )
                    if cfg and cfg.get("effective_value"):
                        bucket_name = cfg["effective_value"]
                except Exception:
                    pass

            if bucket_name:
                from app.core.ai_integration import _get_gcs_client
                client = _get_gcs_client()
                if client:
                    bucket = client.bucket(bucket_name)
                    for blob_path in (
                        build_customer_blob_path(customer_id, "migration_staging", file_name),
                        f"customer_{customer_id}/migration/{file_name}",
                        f"migration/{file_name}",
                    ):
                        try:
                            blob = bucket.blob(blob_path)
                            if blob.exists():
                                blob.delete()
                                logger.info(f"Cleaned up temporary GCS migration staging blob for customer {customer_id}")
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(f"Non-fatal error during migration staging cleanup for customer {customer_id}: {e}")

    async def _create_document_from_url(
        self,
        db: Session,
        lg_record_id: int,
        url: str,
        document_type: str,
        uploaded_by_user_id: int,
        original_instruction_serial: Optional[str] = None
    ) -> Optional[int]:
        """
        Fetches file content from staging/URL, calls crud_lg_document.create_document
        to encrypt and store it in the permanent lg_custody cloud folder, and cleans up
        the temporary migration staging copy.
        """
        try:
            from app.crud.crud import crud_lg_document
            from app.models import LGRecord
            from sqlalchemy.orm import selectinload

            lg_rec = (
                db.query(LGRecord)
                .options(selectinload(LGRecord.beneficiary_corporate), selectinload(LGRecord.lg_category))
                .filter(LGRecord.id == lg_record_id)
                .first()
            )
            cust_id = lg_rec.customer_id if lg_rec else None
            file_content, mime_type = await self._fetch_file_content(url, customer_id=cust_id, db=db)
            if not file_content:
                logger.error(f"Failed to fetch file content for lg_record_id={lg_record_id}")
                return None

            file_name = os.path.basename(str(url).replace("\\", "/"))

            doc_metadata = LGDocumentCreate(
                document_type=document_type,
                file_name=file_name,
                mime_type=mime_type,
                file_path=url,
                lg_instruction_id=None
            )

            lg_record_details = None
            if lg_rec:
                lg_record_details = {
                    "beneficiary_corporate_code": getattr(lg_rec.beneficiary_corporate, "code", "ENTITY") if lg_rec.beneficiary_corporate else "ENTITY",
                    "lg_category_code": getattr(lg_rec.lg_category, "code", "CAT") if lg_rec.lg_category else "CAT",
                    "lg_number": lg_rec.lg_number,
                }

            db_document = await crud_lg_document.create_document(
                db=db,
                obj_in=doc_metadata,
                file_content=file_content,
                lg_record_id=lg_record_id,
                uploaded_by_user_id=uploaded_by_user_id,
                original_instruction_serial=original_instruction_serial,
                lg_record_details=lg_record_details,
            )

            # Once encrypted and stored in the permanent lg_custody cloud folder, clean up temporary migration files
            if db_document and db_document.id:
                self._cleanup_migration_staging_file(url, cust_id, db)

            return db_document.id
        except Exception as e:
            logger.error(f"Error creating document for lg_record_id={lg_record_id}: {e}", exc_info=True)
            return None

    async def _apply_migration_amendment(self, db: Session, lg_record_id: int, updates: Dict[str, Any], user_id: int, staged_record_id: int):
        """
        Applies a single amendment to an existing LG record and logs the change.
        """
        try:
            from app.crud.crud import crud_lg_record, crud_lg_change_log
            from app.models import LGRecord, LGChangeLog
            from sqlalchemy import func
            from app.constants import DOCUMENT_TYPE_INTERNAL_SUPPORTING

            # --- Flatten the diff dictionary ---
            updates_to_apply = {}
            for key, val in updates.items():
                if isinstance(val, dict) and 'new' in val:
                    updates_to_apply[key] = val['new']
                else:
                    updates_to_apply[key] = val
            
            # --- Intercept Attachment URL ---
            if "attachment_url" in updates_to_apply:
                new_url = updates_to_apply["attachment_url"]
                
                if new_url:
                    clean_url = str(new_url).strip().strip("'").strip('"')
                    await self._create_document_from_url(
                        db=db,
                        lg_record_id=lg_record_id,
                        url=clean_url,
                        document_type=DOCUMENT_TYPE_INTERNAL_SUPPORTING,
                        uploaded_by_user_id=user_id,
                        original_instruction_serial=f"MIG-HIST-{staged_record_id}"
                    )
                    logger.info(f"Created amendment document from {clean_url}")

                del updates_to_apply["attachment_url"]

            lg_obj = db.query(LGRecord).filter(LGRecord.id == lg_record_id).first()
            if not lg_obj:
                raise ValueError(f"LG Record {lg_record_id} not found.")

            updated_lg_record = lg_obj
            if updates_to_apply:
                updated_lg_record = await crud_lg_record.amend_lg(
                    db=db,
                    lg_record_id=lg_record_id,
                    amendment_letter_file=None, 
                    amendment_document_metadata=None,
                    amendment_details=updates_to_apply,
                    user_id=user_id,
                    customer_id=lg_obj.customer_id,
                    approval_request_id=None,
                )

            # Calculate Next Change Index
            max_index = db.query(func.max(LGChangeLog.change_index))\
                          .filter(LGChangeLog.lg_id == lg_record_id)\
                          .scalar()
            next_index = (max_index or 0) + 1

            crud_lg_change_log.create_change_log_entry(
                db=db,
                lg_id=updated_lg_record.id,
                staging_id=staged_record_id,
                change_index=next_index,
                diff_json=updates, 
                note="Applied historical amendment"
            )

        except Exception as e:
            logger.error(f"Failed to apply historical amendment to LG {lg_record_id} from staged record {staged_record_id}: {e}", exc_info=True)
            raise

    async def migrate_record(
        self,
        db: Session,
        staged_record: Dict[str, Any],
        user_id: int,
        customer_id: int,
    ) -> LGRecord:
        """
        Migrates a single staged LG record into the production LGRecord table.
        """
        from app.crud.crud import crud_lg_record, crud_internal_owner_contact
        from app.schemas.all_schemas import LGRecordCreate 
        import json 
        
        # --- DEBUG PRINT ---
        logger.debug("============================================")
        logger.debug(">>> ENTERING MIGRATE_RECORD FUNCTION <<<")
        source_data = staged_record.source_data_json or {}
        raw_url = source_data.get("attachment_url")
        logger.debug(f">>> RAW ATTACHMENT URL: {raw_url}")
        logger.debug("============================================")

        lg_number = source_data.get("lg_number")
        if lg_number is not None and not isinstance(lg_number, str):
            lg_number = str(int(lg_number)).strip() if isinstance(lg_number, float) and lg_number.is_integer() else str(lg_number).strip()
        elif isinstance(lg_number, str):
            lg_number = lg_number.strip()

        if lg_number and crud_lg_record.get_by_lg_number(db, lg_number):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"LG number '{lg_number}' already exists in production."
            )

        owner_id = source_data.get("internal_owner_contact_id")
        owner_phone = source_data.get("internal_owner_phone")
        manager_email = source_data.get("manager_email")
        owner_email = source_data.get("internal_owner_email")
        
        if owner_id and (not owner_phone or not manager_email):
            owner_obj = None
            if isinstance(owner_id, int):
                owner_obj = crud_internal_owner_contact.get(db, id=owner_id)
            elif isinstance(owner_id, str):
                clean_owner_email = owner_id.strip()
                if clean_owner_email:
                    owner_obj = crud_internal_owner_contact.get_by_email_for_customer(
                        db, 
                        customer_id=staged_record.customer_id, 
                        email=clean_owner_email
                    )
            if owner_obj:
                owner_phone = owner_obj.phone_number
                manager_email = owner_obj.manager_email
                if not owner_email:
                    owner_email = owner_obj.email

        add_fields = source_data.get("additional_field_values")
        if isinstance(add_fields, str):
            if add_fields.strip().upper() in ['N/A', '0', '']:
                add_fields = None 
            else:
                try:
                    add_fields = json.loads(add_fields)
                except Exception:
                    add_fields = None 

        try:
            payload_data = {
                "beneficiary_corporate_id": source_data.get("beneficiary_corporate_id"),
                "issuer_name": source_data.get("issuer_name"),
                "lg_number": lg_number,
                "lg_amount": source_data.get("lg_amount"),
                "lg_currency_id": source_data.get("lg_currency_id"),
                "lg_payable_currency_id": source_data.get("lg_payable_currency_id"),
                "issuance_date": source_data.get("issuance_date"),
                "expiry_date": source_data.get("expiry_date"),
                "auto_renewal": source_data.get("auto_renewal", True),
                "lg_type_id": source_data.get("lg_type_id"),
                "lg_operational_status_id": source_data.get("lg_operational_status_id"),
                "payment_conditions": source_data.get("payment_conditions"),
                "description_purpose": source_data.get("description_purpose"),
                "issuing_bank_id": source_data.get("issuing_bank_id"),
                "issuing_bank_address": source_data.get("issuing_bank_address"),
                "issuing_bank_phone": source_data.get("issuing_bank_phone"),
                "issuing_bank_fax": source_data.get("issuing_bank_fax"),
                "issuing_method_id": source_data.get("issuing_method_id"),
                "applicable_rule_id": source_data.get("applicable_rule_id"),
                "applicable_rules_text": source_data.get("applicable_rules_text"),
                "other_conditions": source_data.get("other_conditions"),
                "internal_owner_contact_id": owner_id,
                "internal_owner_phone": owner_phone, 
                "internal_owner_email": owner_email,
                "manager_email": manager_email,
                "additional_field_values": add_fields,
                "lg_category_id": source_data.get("lg_category_id"),
                "internal_contract_project_id": source_data.get("internal_contract_project_id"),
                "notes": source_data.get("notes"),
                "ai_scan_file": None,
                "internal_supporting_document_file": None,
            }
            lg_record_create_payload = LGRecordCreate(**payload_data)
            
            new_lg_record = await crud_lg_record.create(
                db=db,
                obj_in=lg_record_create_payload,
                customer_id=customer_id,
                user_id=user_id,
                ai_scan_file_content=None,
                internal_supporting_document_file_content=None,
                allow_past_expiry=True,
            )
            
            new_lg_record.migration_source = 'LEGACY'
            new_lg_record.migrated_from_staging_id = staged_record.id
            db.add(new_lg_record)
            db.flush() 
            
        except Exception as e:
            logger.error(f"Error creating LG Record: {e}", exc_info=True)
            raise

        if raw_url:
            clean_url = str(raw_url).strip().strip("'").strip('"')
            logger.debug(f">>> UPLOADING INITIAL DOCUMENT: {clean_url}")
            try:
                doc_id = await self._create_document_from_url(
                    db=db,
                    lg_record_id=new_lg_record.id,
                    url=clean_url,
                    document_type=DOCUMENT_TYPE_ORIGINAL_LG,
                    uploaded_by_user_id=user_id
                )
                if doc_id:
                    logger.info(f"Successfully attached document ID {doc_id} to LG id={new_lg_record.id}")
                    logger.info(f">>> UPLOAD SUCCESS. ID: {doc_id}")
                else:
                    logger.error(f"Failed to attach document to LG id={new_lg_record.id}. _create_document_from_url returned None.")
                    logger.error(f">>> UPLOAD RETURNED NONE for LG id={new_lg_record.id}")
            except Exception as e:
                logger.error(f"Exception during initial attachment upload for LG id={new_lg_record.id}: {e}", exc_info=True)
                logger.error(f">>> UPLOAD EXCEPTION for LG id={new_lg_record.id}: {e}", exc_info=True)
        else:
             logger.debug(f">>> NO URL FOUND IN RAW DATA for LG id={new_lg_record.id}")
        
        db.refresh(new_lg_record)
        return new_lg_record

    async def migrate_instruction(
        self,
        db: Session,
        staged_instruction: Dict[str, Any],
        user_id: int,
        customer_id: int,
    ) -> LGInstruction:
        """
        Migrates a single staged LG instruction into the production LGInstruction table.
        """
        from app.crud.crud import crud_lg_record, crud_lg_instruction, crud_internal_owner_contact
        source_data = staged_instruction.source_data_json or {}

        lg_number = source_data.get("lg_number")
        if lg_number is not None and not isinstance(lg_number, str):
            lg_number = str(int(lg_number)).strip() if isinstance(lg_number, float) and lg_number.is_integer() else str(lg_number).strip()
        elif isinstance(lg_number, str):
            lg_number = lg_number.strip()

        if not lg_number:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="LG Number missing for instruction record.")

        production_lg_record = crud_lg_record.get_by_lg_number(db, lg_number)
        if not production_lg_record or production_lg_record.customer_id != customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"LG Record '{lg_number}' not found in production for instruction import.")

        staged_id = getattr(staged_instruction, 'id', None)
        try:
            instruction_type = source_data.get('instruction_type', 'LG_AMEND')
            maker_user_email = source_data.get('maker_user_email') or 'migration_system_user@example.com'
            maker_user = crud_internal_owner_contact.get_by_email_for_customer(db, customer_id, maker_user_email)
            if not maker_user:
                maker_id = user_id
            else:
                maker_id = maker_user.id
            
            instruction_type_code_enum = InstructionTypeCode.AMEND
            sub_instruction_code_enum = SubInstructionCode.AMENDMENT
            
            payload_data = {
                "lg_record_id": production_lg_record.id,
                "instruction_type": instruction_type,
                "template_id": source_data.get("template_id"),
                "status": source_data.get("status", "Instruction Issued"),
                "details": source_data.get("details"),
                "maker_user_id": maker_id,
            }
            instruction_create_payload = LGInstructionCreate(**payload_data)
        except ValidationError as e:
            logger.error(f"Pydantic validation failed for staged instruction {staged_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Data validation error for instruction: {e}")

        try:
            new_instruction = await crud_lg_instruction.create(
                db=db,
                obj_in=instruction_create_payload,
                entity_code=production_lg_record.beneficiary_corporate.code,
                lg_category_code=production_lg_record.lg_category.code,
                lg_sequence_number_str=str(production_lg_record.lg_sequence_number).zfill(4),
                instruction_type_code_enum=instruction_type_code_enum,
                sub_instruction_code_enum=sub_instruction_code_enum
            )
        except HTTPException as e:
            logger.error(f"Failed to create new LG instruction from staged data {staged_id}: {e.detail}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error creating new LG instruction from staged data {staged_id}: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred during instruction migration.")

        attachment_url = source_data.get("attachment_url")
        if attachment_url:
            await self._create_document_from_url(
                db=db,
                lg_record_id=production_lg_record.id,
                url=attachment_url,
                document_type=DOCUMENT_TYPE_INTERNAL_SUPPORTING,
                uploaded_by_user_id=user_id,
                original_instruction_serial=new_instruction.serial_number
            )
            
        db.refresh(new_instruction)
        return new_instruction
    
    def _normalize_for_comparison(self, val: Any) -> str:
        """Helper to cleanup strings for loose comparison."""
        if val is None:
            return ""
        if isinstance(val, (date, datetime)):
            return val.strftime("%Y-%m-%d")
        return str(val).strip().upper().replace(",", "").replace("_", " ")

    async def audit_staged_records(self, db: Session, customer_id: int) -> Dict[str, Any]:
        """
        Audits the FIRST and LAST record for each LG using AI.
        Returns a report of discrepancies.
        """
        from app.models import LGMigrationStaging, Currency, CustomerEntity, Bank
        from app.core.ai_integration import process_lg_document_with_ai, process_amendment_with_ai
        from sqlalchemy import func
        import os

        logger.info("=======================================================")
        logger.info(f"STARTING ENHANCED AI AUDIT for customer {customer_id}")
        logger.info("=======================================================")

        # 1. Print counts of ALL statuses to see where records went
        status_counts = db.query(
            LGMigrationStaging.record_status, func.count(LGMigrationStaging.id)
        ).filter(
            LGMigrationStaging.customer_id == customer_id
        ).group_by(LGMigrationStaging.record_status).all()

        logger.info("-> Current Record Statuses in DB:")
        for status_val, count in status_counts:
            logger.info(f"   - {status_val}: {count}")

        # 2. Fetch Eligible Records (include EXPIRED, DUPLICATE, ERROR so multi-row historical LGs are audited)
        records = db.query(LGMigrationStaging).filter(
            LGMigrationStaging.customer_id == customer_id,
            LGMigrationStaging.record_status.in_([
                MigrationRecordStatusEnum.READY_FOR_IMPORT,
                MigrationRecordStatusEnum.PENDING,
                MigrationRecordStatusEnum.NEEDS_REVIEW,
                MigrationRecordStatusEnum.ERROR,
                MigrationRecordStatusEnum.EXPIRED,
                MigrationRecordStatusEnum.DUPLICATE,
            ])
        ).order_by(LGMigrationStaging.id).all()

        if not records:
            logger.info("STOPPING: No eligible staged records found.")
            return {"status": "skipped", "message": "No staged records found.", "audit_summary": [], "records_checked": 0, "files_scanned": 0, "missing_files": []}

        # --- PRE-FETCH CACHES ---
        currency_map = {c.id: c.iso_code for c in db.query(Currency).all()}
        bank_map = {b.id: b.name for b in db.query(Bank).all()}

        # Entity Map (Safe Name Lookup)
        entities = db.query(CustomerEntity).filter(CustomerEntity.customer_id == customer_id).all()
        entity_map = {}
        for e in entities:
            e_name = getattr(e, 'name', getattr(e, 'entity_name', getattr(e, 'company_name', 'Unknown')))
            entity_map[e.id] = e_name

        from sqlalchemy.orm.attributes import flag_modified
        try:
            from app.api.v1.endpoints.migration import normalize_record_keys
        except Exception:
            normalize_record_keys = None

        grouped_records = {}
        for r in records:
            data = r.source_data_json or {}
            if normalize_record_keys and isinstance(data, dict):
                before_keys = set(data.keys())
                before_att = data.get("attachment_url")
                normalize_record_keys(data)
                if set(data.keys()) != before_keys or data.get("attachment_url") != before_att:
                    r.source_data_json = data
                    flag_modified(r, "source_data_json")
                    db.add(r)
            lg_num = data.get('lg_number')
            if not lg_num:
                continue
            if lg_num not in grouped_records:
                grouped_records[lg_num] = []
            grouped_records[lg_num].append(r)

        audit_report = []
        audited_passed = []
        skipped_records = []
        files_scanned = 0
        missing_files = []

        # Helpers
        def clean_date(val):
            if not val:
                return ""
            if isinstance(val, (date, datetime)):
                return val.strftime("%Y-%m-%d")
            s = str(val).strip()
            # Match YYYY-MM-DD or YYYY/MM/DD at start (ignoring any trailing T..., _..., space, time, or Z)
            m_iso = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
            if m_iso:
                return f"{m_iso.group(1)}-{int(m_iso.group(2)):02d}-{int(m_iso.group(3)):02d}"
            # Match DD-MM-YYYY or DD/MM/YYYY at start
            m_dmy = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})", s)
            if m_dmy:
                return f"{m_dmy.group(3)}-{int(m_dmy.group(2)):02d}-{int(m_dmy.group(1)):02d}"
            # Fallback: split on T, underscore, or space
            return re.split(r"[T_\s]", s)[0]

        def norm(val):
            return str(val).strip().upper().replace(",", "").replace(".", "").replace("  ", " ")

        def _extract_attachment_url(json_data: Dict[str, Any]) -> Optional[str]:
            for key in ('attachment_url', 'attachement', 'attachment', 'file_path', 'document', 'pdf', 'scan'):
                val = json_data.get(key)
                if val and str(val).strip() and str(val).strip().upper() not in ('NAN', 'NONE', 'N/A'):
                    return str(val).strip()
            return None

        for lg_num, group in grouped_records.items():
            group.sort(key=lambda x: x.id)
            first_rec = group[0]
            last_rec = group[-1]
            logger.info(f"[AUDIT] Checking staged LG group (first_record_id={first_rec.id}, count={len(group)})")
            discrepancies = []
            was_scanned = False
            skip_reasons = []

            # --- 1. AUDIT CREATION (First Record) ---
            json_data = first_rec.source_data_json or {}
            first_url = _extract_attachment_url(json_data)

            if first_url:
                clean_url = str(first_url).strip().strip("'").strip('"').replace("\u202a", "").replace("\u202c", "")
                fname = os.path.basename(clean_url.replace("\\", "/"))
                content, mime = await self._fetch_file_content(clean_url, customer_id=customer_id, db=db)

                if content:
                    files_scanned += 1
                    was_scanned = True
                    logger.info(f"   -> AI Scanning Original Document for staged record id={first_rec.id}...")
                    ai_data, _ = await process_lg_document_with_ai(content, mime, lg_number_hint=str(lg_num))

                    if ai_data:
                        src = first_rec.source_data_json

                        # A. LG Number
                        ai_lg_num = ai_data.get('lgNumber', '')
                        if norm(lg_num) != norm(ai_lg_num):
                            err = f"LG Number Mismatch: Excel='{lg_num}' vs AI='{ai_lg_num}'"
                            discrepancies.append(err)
                            logger.info(f"      [Audit Mismatch] record_id={first_rec.id} field=lg_number")

                        # B. Amount
                        try:
                            val_excel = float(src.get('lg_amount', 0))
                            val_ai = float(ai_data.get('lgAmount', 0))
                            if abs(val_excel - val_ai) > 1.0:
                                err = f"Amount Mismatch: Excel='{val_excel:,.2f}' vs AI='{val_ai:,.2f}'"
                                discrepancies.append(err)
                                logger.info(f"      [Audit Mismatch] record_id={first_rec.id} field=lg_amount")
                        except Exception:
                            pass

                        # C. Currency
                        excel_curr_id = src.get('lg_currency_id')
                        excel_curr_code = currency_map.get(excel_curr_id, "Unknown")
                        ai_curr = ai_data.get('currency', '')
                        if norm(excel_curr_code) != norm(ai_curr):
                            err = f"Currency Mismatch: Excel='{excel_curr_code}' vs AI='{ai_curr}'"
                            discrepancies.append(err)
                            logger.info(f"      [Audit Mismatch] record_id={first_rec.id} field=currency")

                        # D. Issuing Bank
                        excel_bank_id = src.get('issuing_bank_id')
                        excel_bank_name = bank_map.get(excel_bank_id, "Unknown")
                        ai_bank = ai_data.get('issuingBankName', '')
                        if excel_bank_name != "Unknown":
                            if norm(excel_bank_name) not in norm(ai_bank) and norm(ai_bank) not in norm(excel_bank_name):
                                err = f"Bank Mismatch: Excel='{excel_bank_name}' vs AI='{ai_bank}'"
                                discrepancies.append(err)
                                logger.info(f"      [Audit Mismatch] record_id={first_rec.id} field=issuing_bank")

                        # E. Dates (Issuance & Expiry)
                        excel_date = clean_date(src.get('issuance_date'))
                        ai_date = clean_date(ai_data.get('issuanceDate'))
                        if excel_date != ai_date:
                            err = f"Issuance Date Mismatch: Excel='{excel_date}' vs AI='{ai_date}'"
                            discrepancies.append(err)
                            logger.info(f"      [Audit Mismatch] record_id={first_rec.id} field=issuance_date")

                        excel_exp = clean_date(src.get('expiry_date'))
                        ai_exp = clean_date(ai_data.get('expiryDate'))
                        if excel_exp != ai_exp:
                            err = f"Expiry Date Mismatch: Excel='{excel_exp}' vs AI='{ai_exp}'"
                            discrepancies.append(err)
                            logger.info(f"      [Audit Mismatch] record_id={first_rec.id} field=expiry_date")
                    else:
                        skip_reasons.append(f"AI could not extract text from '{fname}'")
                        logger.warning(f"   ⚠️ AI returned NO data for staged record id={first_rec.id}.")
                else:
                    missing_files.append(f"LG {lg_num}: '{fname}'")
                    skip_reasons.append(f"File '{fname}' not uploaded/found on server")
                    logger.warning(f"   ❌ Attachment file not found for staged record id={first_rec.id}")
            else:
                skip_reasons.append("No attachment path in Excel")

            # --- 2. AUDIT LATEST STATUS (Last Record) ---
            if last_rec.id != first_rec.id:
                last_url = _extract_attachment_url(last_rec.source_data_json or {})
                if last_url:
                    clean_url_last = str(last_url).strip().strip("'").strip('"').replace("\u202a", "").replace("\u202c", "")
                    fname_last = os.path.basename(clean_url_last.replace("\\", "/"))

                    content, mime = await self._fetch_file_content(clean_url_last, customer_id=customer_id, db=db)

                    if content:
                        files_scanned += 1
                        was_scanned = True
                        logger.info(f"   -> AI Scanning Amendment for staged record id={last_rec.id}...")
                        context = {"lg_record_details": {"lgNumber": str(lg_num)}}
                        ai_amend, _ = await process_amendment_with_ai(content, mime, context)

                        if ai_amend and ai_amend.get('is_relevant_amendment'):
                            changes = ai_amend.get('amendedFields', {})
                            src = last_rec.source_data_json

                            if "expiryDate" in changes:
                                val_excel = clean_date(src.get('expiry_date'))
                                val_ai = clean_date(changes['expiryDate'])
                                if val_excel != val_ai:
                                    err = f"Latest Expiry Mismatch: Excel='{val_excel}' vs AI='{val_ai}'"
                                    discrepancies.append(err)
                                    logger.info(f"      [Audit Mismatch] record_id={last_rec.id} field=latest_expiry_date")

                            if "lgAmount" in changes:
                                try:
                                    val_excel = float(src.get('lg_amount', 0))
                                    val_ai = float(changes['lgAmount'])
                                    if abs(val_excel - val_ai) > 1.0:
                                        err = f"Latest Amount Mismatch: Excel='{val_excel}' vs AI='{val_ai}'"
                                        discrepancies.append(err)
                                        logger.info(f"      [Audit Mismatch] record_id={last_rec.id} field=latest_lg_amount")
                                except Exception:
                                    pass
                    else:
                        missing_files.append(f"LG {lg_num} (Amendment): '{fname_last}'")
                        skip_reasons.append(f"Amendment file '{fname_last}' not uploaded/found")
                        logger.warning(f"   ❌ Amendment attachment not found for staged record id={last_rec.id}")

            first_rec.validation_log = dict(first_rec.validation_log or {})
            ignored_issues = first_rec.validation_log.get('ignored_ai_issues') or []
            if isinstance(ignored_issues, list) and ("*" in ignored_issues or ignored_issues):
                if "*" in ignored_issues:
                    discrepancies = []
                else:
                    discrepancies = [d for d in discrepancies if d not in ignored_issues]

            if discrepancies:
                audit_report.append({"record_id": first_rec.id, "lg_number": lg_num, "issues": discrepancies})
                first_rec.validation_log['ai_warning'] = " | ".join(discrepancies)
                first_rec.validation_log['ai_audit_status'] = f"MISMATCH: {' | '.join(discrepancies)}"
                if first_rec.record_status == MigrationRecordStatusEnum.READY_FOR_IMPORT:
                    first_rec.record_status = MigrationRecordStatusEnum.NEEDS_REVIEW
            elif was_scanned and not skip_reasons:
                msg = "Reviewed & approved by user" if ignored_issues else "Verified against PDF scan — No discrepancies found."
                audited_passed.append({"record_id": first_rec.id, "lg_number": lg_num, "message": msg})
                first_rec.validation_log.pop('ai_warning', None)
                first_rec.validation_log['ai_audit_status'] = f"AUDITED_PASSED: {msg}"
                non_ai_errors = [
                    k for k in first_rec.validation_log.keys()
                    if k not in ('ai_warning', 'ai_audit_status', 'ignored_ai_issues')
                ]
                if not non_ai_errors and first_rec.record_status == MigrationRecordStatusEnum.NEEDS_REVIEW:
                    first_rec.record_status = MigrationRecordStatusEnum.READY_FOR_IMPORT
            else:
                reason_str = "; ".join(skip_reasons) if skip_reasons else "Skipped"
                skipped_records.append({"record_id": first_rec.id, "lg_number": lg_num, "reason": reason_str})
                first_rec.validation_log['ai_audit_status'] = f"SKIPPED: {reason_str}"

            flag_modified(first_rec, "validation_log")
            db.add(first_rec)

        db.commit()
        logger.info("=======================================================")
        logger.info(
            f"🕵️ AUDIT FINISHED. LGs checked: {len(grouped_records)}, Files scanned: {files_scanned}, "
            f"Passed: {len(audited_passed)}, Discrepancies: {len(audit_report)}, Skipped: {len(skipped_records)}"
        )
        logger.info("=======================================================")
        return {
            "audit_summary": audit_report,
            "audited_passed": audited_passed,
            "skipped_records": skipped_records,
            "records_checked": len(grouped_records),
            "files_scanned": files_scanned,
            "missing_files": missing_files,
        }

# Initialize the service instance
migration_service = MigrationService()