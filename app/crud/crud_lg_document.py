# app/crud/crud_lg_document.py
import json
import os
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type, Tuple
from fastapi import HTTPException, status, UploadFile
from sqlalchemy import func, desc, exists, and_
from sqlalchemy.orm import Session, selectinload
import decimal

from app.crud.crud import CRUDBase, log_action
import app.models as models
from app.models import LGDocument, Customer, SubscriptionPlan
from app.schemas.all_schemas import LGDocumentCreate
from app.core.ai_integration import _upload_to_gcs, GCS_BUCKET_NAME 

import logging
logger = logging.getLogger(__name__)

def _generate_serial_based_document_filename(
    original_instruction_serial: str,
    document_type: str, 
    lg_record_id: int, 
    original_file_extension: str 
) -> str:
    """
    Generates a document filename based on the instruction's serial number.
    """
    if not original_instruction_serial or len(original_instruction_serial) != 20: 
        logger.warning(f"[NamingHelper] Invalid serial format for naming. Serial: '{original_instruction_serial}', Expected Length: 20. Falling back to UUID naming.") 
        return f"{lg_record_id}_{document_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}.{original_file_extension}"

    base_serial = original_instruction_serial[:-1] 
    
    sub_code = ''
    if document_type == 'DELIVERY_PROOF':
        sub_code = 'D'
    elif document_type == 'BANK_REPLY':
        sub_code = 'R'

    new_filename_without_ext = f"{base_serial}{sub_code}"
    final_filename = f"{new_filename_without_ext}.{original_file_extension}" 
    logger.debug(f"[NamingHelper] Generated serial-based filename: '{final_filename}' from serial '{original_instruction_serial}', type '{document_type}'.") 
    return final_filename

def _slugify_doc_type(document_type: str) -> str:
    """
    Helper to convert raw document types into clean folder names.
    Examples: 
    'ORIGINAL_LG_DOCUMENT' -> 'original_lg'
    'Internal Supporting Document' -> 'internal_supporting'
    """
    if not document_type:
        return "misc"
    
    # specific mappings for system constants
    mappings = {
        models.DOCUMENT_TYPE_ORIGINAL_LG: "original_lg",
        "AMENDMENT_LETTER": "amendment",
        "DELIVERY_PROOF": "delivery_proof",
        "BANK_REPLY": "bank_reply",
        "INTERNAL_SUPPORTING": "internal_supporting",
        "AI_SCAN": "ai_scan" # Should generally be caught by ORIGINAL_LG mapping, but good fallback
    }
    
    if document_type in mappings:
        return mappings[document_type]
        
    # Fallback for loose strings: lowercase and replace spaces/special chars with underscore
    return "".join(x if x.isalnum() else "_" for x in document_type.lower()).strip("_")

class CRUDLGDocument(CRUDBase):
    def __init__(self, model: Type[LGDocument], crud_customer_configuration_instance: Any):
        super().__init__(model)
        self.crud_customer_configuration_instance = crud_customer_configuration_instance

    async def create_document(
        self,
        db: Session,
        obj_in: LGDocumentCreate, 
        file_content: bytes,
        lg_record_id: int,
        uploaded_by_user_id: int,
        original_instruction_serial: Optional[str] = None,
        lg_record_details: Optional[Dict[str, Any]] = None        
    ) -> LGDocument:
        logger.debug(f"[CRUDLGDocument.create_document] START. lg_record_id={lg_record_id}, doc_type={obj_in.document_type}, orig_filename={obj_in.file_name}") 

        # 1. Validate Customer and Subscription
        customer_id = db.query(models.LGRecord.customer_id).filter(models.LGRecord.id == lg_record_id).scalar()
        if not customer_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Associated LG Record not found for document storage.")

        customer_obj = db.query(Customer).options(selectinload(Customer.subscription_plan)).filter(Customer.id == customer_id).first()
        if not customer_obj or not customer_obj.subscription_plan or not customer_obj.subscription_plan.can_image_storage:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Customer's subscription plan does not support image/document storage."
            )

        file_extension = obj_in.file_name.split('.')[-1] if '.' in obj_in.file_name else 'bin'
        
        # 2. Generate Unique Filename (The leaf name)
        unique_filename = ""
        if obj_in.document_type == models.DOCUMENT_TYPE_ORIGINAL_LG and lg_record_details:
            # Format: [Beneficiary]-[Cat]-[LG_Num]-ORIGINAL.pdf
            beneficiary_code = lg_record_details.get('beneficiary_corporate_code', 'UNKNOWN_ENTITY')
            lg_category_code = lg_record_details.get('lg_category_code', 'UNKNOWN_CAT')
            lg_number = lg_record_details.get('lg_number', f'UNKNOWN_LG_{lg_record_id}')

            unique_filename = f"{beneficiary_code}-{lg_category_code}-{lg_number}-ORIGINAL.{file_extension}"
            unique_filename = "".join(x for x in unique_filename if x.isalnum() or x in ("-", "_", ".")).replace(" ", "_")

        elif obj_in.document_type in ['DELIVERY_PROOF', 'BANK_REPLY'] and original_instruction_serial:
            unique_filename = _generate_serial_based_document_filename(
                original_instruction_serial,
                obj_in.document_type,
                lg_record_id,
                file_extension
            )
        else:
            unique_filename = f"{lg_record_id}_{obj_in.document_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}.{file_extension}"

        if not unique_filename.endswith(f".{file_extension}"):
            unique_filename = f"{unique_filename}.{file_extension}"

        # 3. Construct Hierarchical Path (The Folder Structure)
        # Structure: customer_{id}/lg_{id}/{type_slug}/{filename}
        doc_type_slug = _slugify_doc_type(obj_in.document_type)
        blob_path = f"customer_{customer_id}/lg_{lg_record_id}/{doc_type_slug}/{unique_filename}"

        if not GCS_BUCKET_NAME:
            logger.error("GCS_BUCKET_NAME is not set in environment. Cannot upload document.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error: GCS bucket not set for document storage.")
            
        try:
            # 4. Upload to GCS using the new hierarchical blob path
            stored_gcs_uri = await _upload_to_gcs(GCS_BUCKET_NAME, blob_path, file_content, obj_in.mime_type)
            if not stored_gcs_uri:
                raise Exception("GCS upload returned no URI.")
            logger.info(f"Document uploaded to GCS: {stored_gcs_uri}")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to upload document {blob_path} to GCS: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to store document file in cloud storage: {e}")

        # 5. Save Metadata to DB
        document_data = obj_in.model_dump()
        document_data["file_path"] = stored_gcs_uri # This stores the full gs:// path, preserving access
        document_data["file_name"] = unique_filename # Keep the clean filename for display

        lg_instruction_id_from_obj_in = document_data.pop("lg_instruction_id", None)

        db_document = self.model(
            lg_record_id=lg_record_id,
            uploaded_by_user_id=uploaded_by_user_id,
            lg_instruction_id=lg_instruction_id_from_obj_in, 
            **document_data, 
        )
        db.add(db_document)
        db.flush()

        log_action(
            db,
            user_id=uploaded_by_user_id,
            action_type="DOCUMENT_UPLOADED",
            entity_type="LGDocument",
            entity_id=db_document.id,
            details={
                "lg_record_id": lg_record_id,
                "lg_instruction_id": obj_in.lg_instruction_id, 
                "document_type": obj_in.document_type,
                "file_name": unique_filename, 
                "stored_path": stored_gcs_uri 
            },
            customer_id=customer_id,
            lg_record_id=lg_record_id,
        )
        db.refresh(db_document)
        return db_document