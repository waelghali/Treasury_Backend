

# app/api/v1/endpoints/public.py
import os
import shutil
import uuid
from typing import Optional
from datetime import datetime
import pytz

from fastapi import APIRouter, Depends, HTTPException, status, Form, UploadFile, Request, BackgroundTasks, File
from fastapi.responses import FileResponse
from fastapi import Query
from sqlalchemy.orm import Session
from app.database import get_db
from app.schemas.all_schemas import (
    LegalArtifactVersionsOut, LegalArtifactOut,
    TrialRegistrationCreate, TrialRegistrationOut,
)
from app.crud.crud import crud_legal_artifact, crud_global_configuration, crud_trial_registration
from app.core.email_service import get_global_email_settings, send_email, EmailAttachment
from app.core.document_generator import generate_pdf_from_html
from app.constants import GlobalConfigKey, LegalArtifactType

import logging

router = APIRouter()
logger = logging.getLogger(__name__)

# --- Mock File Storage Utility ---
async def upload_file_to_storage(file: UploadFile, folder_name: str) -> str:
    """Mocks file upload to a local 'uploads' directory."""
    try:
        # Create the uploads directory if it doesn't exist
        upload_dir = os.path.join("uploads", folder_name)
        os.makedirs(upload_dir, exist_ok=True)
        
        # Generate a unique filename to prevent conflicts
        unique_filename = f"{uuid.uuid4()}_{file.filename}"
        file_path = os.path.join(upload_dir, unique_filename)
        
        # Write the file content to the new path
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        logger.info(f"File saved to local path: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Failed to save uploaded file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save the uploaded file."
        )


@router.get("/legal-versions", response_model=LegalArtifactVersionsOut)
def get_legal_versions(
    db: Session = Depends(get_db)
):
    """
    Retrieves the latest version of the Terms and Conditions and Privacy Policy.
    This is a public endpoint and does not require authentication.
    """
    try:
        latest_tc_artifact = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.TERMS_AND_CONDITIONS)
        latest_pp_artifact = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.PRIVACY_POLICY)

        if not latest_tc_artifact:
            tc_version_config = crud_global_configuration.get_by_key(db, GlobalConfigKey.TC_VERSION)
            tc_version = float(tc_version_config.value_default) if tc_version_config else 0.0
        else:
            tc_version = latest_tc_artifact.version

        if not latest_pp_artifact:
            pp_version_config = crud_global_configuration.get_by_key(db, GlobalConfigKey.PP_VERSION)
            pp_version = float(pp_version_config.value_default) if pp_version_config else 0.0
        else:
            pp_version = latest_pp_artifact.version
            
        logger.info(f"Retrieved legal artifact versions: TC={tc_version}, PP={pp_version}")
        return LegalArtifactVersionsOut(tc_version=tc_version, pp_version=pp_version)

    except Exception as e:
        logger.error(f"Failed to retrieve legal artifact versions: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching legal versions."
        )

@router.get("/legal-content/{artifact_type}", response_model=LegalArtifactOut)
def get_legal_content(
    artifact_type: str,
    db: Session = Depends(get_db)
):
    """
    Retrieves the latest content of a specific legal artifact.
    This is a public endpoint and does not require authentication.
    """
    if artifact_type not in [LegalArtifactType.TERMS_AND_CONDITIONS.value, LegalArtifactType.PRIVACY_POLICY.value]:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid legal artifact type.")

    try:
        artifact = crud_legal_artifact.get_by_artifact_type(db, artifact_type=artifact_type)
        if not artifact:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No content found for legal artifact type: {artifact_type}")
        return LegalArtifactOut.model_validate(artifact)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Failed to retrieve legal artifact content for {artifact_type}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching legal content."
        )

@router.post(
    "/register-free-trial/",
    response_model=TrialRegistrationOut,
    status_code=status.HTTP_201_CREATED,
)
async def register_free_trial(
    request: Request,
    db: Session = Depends(get_db),
    background_tasks: BackgroundTasks = None,
    organization_name: str = Form(...),
    organization_address: str = Form(...),
    contact_admin_name: str = Form(...),
    contact_phone: str = Form(...),
    admin_email: str = Form(...),
    entities_count: str = Form(...),
    commercial_register_document: UploadFile = File(...),
    accepted_terms: str = Form(...),
):
    """
    Handles a new free trial registration from a public user.
    """
    if accepted_terms.lower() != 'true':
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Terms and Conditions must be accepted.")
        
    if not commercial_register_document.filename.endswith(('.pdf', '.jpg', '.jpeg', '.png')):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Commercial register document must be a PDF, JPG, or PNG.")

    existing_registration = crud_trial_registration.get_by_email_and_status(db, admin_email, "pending")
    if existing_registration:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A pending registration with this email already exists.")

    file_path = await upload_file_to_storage(commercial_register_document, "commercial_registers")
    
    latest_tc = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.TERMS_AND_CONDITIONS)
    tc_version = latest_tc.version if latest_tc else 0.0

    # CORRECTED: Use the direct client host from the request object
    client_ip = request.client.host if request.client else None
    
    # Corrected section: Create an instance of the Pydantic model and pass the data
    registration_in = TrialRegistrationCreate(
        organization_name=organization_name,
        organization_address=organization_address,
        contact_admin_name=contact_admin_name,
        contact_phone=contact_phone,
        admin_email=admin_email,
        entities_count=entities_count,
        commercial_register_document_path=file_path,
        accepted_terms_version=tc_version,
        accepted_terms_ip=client_ip,
        accepted_terms_at=datetime.now(pytz.timezone('Africa/Cairo')),
    )

    db_registration = crud_trial_registration.create(db, obj_in=registration_in)

    email_settings = get_global_email_settings()
    subject = "LG Custody Free Trial Registration Confirmation"
    body = f"""
        <html><body>
            <p>Dear {contact_admin_name},</p>
            <p>Thank you for registering for a free trial of the LG Custody Platform.</p>
            <p>Your registration has been submitted and will be reviewed and activated within 2 working days. We will notify you via email as soon as your account is ready.</p>
            <p>Please find a copy of our Terms & Conditions and Free Trial Disclaimer attached for your records.</p>
            <p>Best regards,</p>
            <p>The LG Custody Team</p>
        </body></html>
    """

    tc_content = latest_tc.content if latest_tc else "Terms and Conditions not found."
    pdf_content = await generate_pdf_from_html(tc_content)
    attachment = EmailAttachment("Terms_and_Conditions.pdf", pdf_content, "application/pdf")

    background_tasks.add_task(send_email, db, [admin_email], subject, body, {}, email_settings, attachments=[attachment])
    
    return db_registration


@router.get("/documents/", tags=["Public"], response_class=FileResponse)
async def serve_document(
    file_path: str = Query(..., description="The path to the document to be served."),
    db: Session = Depends(get_db)
):
    """
    Serves a document from the local file system. This endpoint is public for now,
    but should be secured in a production environment.
    """
    # Security check: Ensure the path is within the allowed uploads directory.
    # This prevents directory traversal attacks.
    base_dir = os.path.abspath("uploads")
    full_path = os.path.abspath(file_path)

    if not full_path.startswith(base_dir):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access to the requested path is forbidden."
        )

    # Check if the file exists
    if not os.path.exists(full_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found."
        )

    return FileResponse(full_path)