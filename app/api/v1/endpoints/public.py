# app/api/v1/endpoints/public.py
import os
# import shutil  <-- REMOVED: No longer needed for local copy
import uuid
from html import escape as html_escape
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
from app.core.document_generator import generate_multipage_pdf_from_html, wrap_legal_artifact_for_pdf
from app.constants import GlobalConfigKey, LegalArtifactType
from app.core.honeypot import verify_honeypot

# *** FIXED: Use storage.Client() inside function (not at module level) ***
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

async def upload_file_to_gcs(file: UploadFile, folder_name: str) -> str:
    """Uploads file to GCS. Client is created per-call to avoid module-level crashes."""
    import asyncio
    from google.cloud import storage as gcs_storage

    await file.seek(0)
    file_content = await file.read()
    unique_filename = f"{uuid.uuid4()}_{file.filename}"
    blob_name = f"{folder_name}/{unique_filename}"
    
    bucket_name = os.environ.get("GCS_BUCKET_NAME", "")
    if not bucket_name:
        raise HTTPException(status_code=500, detail="GCS_BUCKET_NAME not configured.")

    try:
        client = gcs_storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        await asyncio.to_thread(blob.upload_from_string, file_content, content_type=file.content_type)
        gcs_uri = f"gs://{bucket_name}/{blob_name}"
        logger.info(f"File uploaded to GCS: {gcs_uri}")
        return gcs_uri
    except Exception as e:
        logger.error(f"GCS upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to upload to cloud storage: {e}")


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

_PERSONAL_OR_DISPOSABLE_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "yahoo.co.uk", "yahoo.fr", "ymail.com",
    "hotmail.com", "hotmail.co.uk", "outlook.com", "live.com", "msn.com",
    "icloud.com", "me.com", "mac.com", "aol.com", "protonmail.com", "proton.me",
    "yandex.com", "mail.com", "gmx.com", "mailinator.com", "guerrillamail.com",
    "tempmail.com", "10minutemail.com", "yopmail.com", "sharklasers.com", "trashmail.com",
}


def _validate_corporate_work_email(email_str: str) -> str:
    clean = (email_str or "").strip().lower()
    if "@" not in clean:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please provide a valid corporate work email address.",
        )
    domain = clean.rsplit("@", 1)[1].strip()
    if not domain or "." not in domain or domain in _PERSONAL_OR_DISPOSABLE_EMAIL_DOMAINS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please register using your corporate work email address (e.g. name@yourcompany.com) rather than a personal inbox.",
        )
    return clean


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
    requested_modules: str = Form("custody", description="Comma-separated list of requested modules"),
    commercial_register_document: Optional[UploadFile] = File(None),
    accepted_terms: str = Form(...),
    website_url: Optional[str] = Form(None),
):
    """
    Handles a new free trial registration from a public user.
    Commercial Register document is optional for Sandbox Trial evaluation (required only before live production go-live).
    """
    # Perimeter Anti-Bot Honeypot Defense (§2.8)
    await verify_honeypot(
        website_url,
        request=request,
        context="free_trial_registration",
        legitimate_values=[
            admin_email,
            contact_admin_name,
            organization_name,
            organization_address,
            contact_phone,
        ],
    )

    admin_email = _validate_corporate_work_email(admin_email)

    if accepted_terms.lower() != 'true':
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Terms and Conditions must be accepted.")

    has_cr_file = bool(
        commercial_register_document
        and getattr(commercial_register_document, "filename", None)
        and str(commercial_register_document.filename).strip()
    )
    if has_cr_file and not commercial_register_document.filename.lower().endswith(('.pdf', '.jpg', '.jpeg', '.png')):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Commercial register document must be a PDF, JPG, or PNG.")

    # Reject ONLY if (a) an active non-deleted customer/corporate user already exists, or
    # (b) a free trial request is currently pending approval. Deleted customers can re-apply freely!
    from sqlalchemy import func
    from app.models import TrialRegistration
    from app.models.models import Customer, User

    admin_domain = admin_email.rsplit("@", 1)[1].strip().lower()
    active_cust_conflict = (
        db.query(Customer)
        .filter(
            Customer.is_deleted == False,
            (
                (func.lower(Customer.contact_email) == admin_email)
                | (func.lower(Customer.name) == organization_name.strip().lower())
                | (Customer.domains.contains([admin_domain]))
            ),
        )
        .first()
    )
    active_corp_user_conflict = (
        db.query(User)
        .filter(
            User.is_deleted == False,
            User.customer_id.isnot(None),
            func.lower(User.email) == admin_email,
        )
        .first()
    )
    if active_cust_conflict or active_corp_user_conflict:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An active corporate account for this organization or email already exists. Please sign in or contact support.",
        )

    existing_regs = (
        db.query(TrialRegistration)
        .filter(
            func.lower(TrialRegistration.admin_email) == admin_email,
            TrialRegistration.is_deleted == False,
        )
        .all()
    )
    for reg_row in existing_regs:
        if reg_row.status == "pending":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A free trial request for this email is already pending approval.",
            )

    # Any remaining rows are either rejected or belonged to a deleted customer — clear them so re-registration succeeds cleanly
    for reg_row in existing_regs:
        db.delete(reg_row)
    if existing_regs:
        db.flush()

    file_path = None
    if has_cr_file:
        file_path = await upload_file_to_gcs(commercial_register_document, "commercial_registers")
    
    latest_tc = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.TERMS_AND_CONDITIONS)
    tc_version = latest_tc.version if latest_tc else 0.0

    # CORRECTED: Use the direct client host from the request object
    client_ip = request.client.host if request.client else None
    
    # Parse and validate requested modules
    valid_modules = {"custody", "issuance", "quotations", "reconciliation"}
    parsed_modules = [m.strip().lower() for m in requested_modules.split(",") if m.strip()]
    parsed_modules = [m for m in parsed_modules if m in valid_modules]
    if not parsed_modules:
        parsed_modules = ["custody"]  # Default fallback

    # Corrected section: Create an instance of the Pydantic model and pass the data
    registration_in = TrialRegistrationCreate(
        organization_name=organization_name,
        organization_address=organization_address,
        contact_admin_name=contact_admin_name,
        contact_phone=contact_phone,
        admin_email=admin_email,
        entities_count=entities_count,
        requested_modules=parsed_modules,
        # 'file_path' is now the GCS URI
        commercial_register_document_path=file_path, 
        accepted_terms_version=tc_version,
        accepted_terms_ip=client_ip,
        accepted_terms_at=datetime.now(pytz.timezone('Africa/Cairo')),
    )

    db_registration = crud_trial_registration.create(db, obj_in=registration_in)

    # Build clean module list for branded SaaS confirmation email + founder lead alert
    module_names = []
    if "reconciliation" in parsed_modules:
        module_names.append("Bank Auto-Reconciliation")
    if "custody" in parsed_modules:
        module_names.append("LG Custody & Lifecycle")
    if "issuance" in parsed_modules:
        module_names.append("LG Issuance & Risk Shield")
    if "quotations" in parsed_modules:
        module_names.append("FX & T-Bill Sealed Bidding")
    if not module_names:
        module_names = ["Grow Corporate Treasury Suite"]

    from app.core.routing import get_frontend_base_url
    from app.services.unified_email_builder import build_standard_email_html

    base_url = get_frontend_base_url(request=request)
    bullets_html = "".join(f"<li style='margin-bottom:4px;'>{html_escape(str(m))}</li>" for m in module_names)

    safe_org = html_escape(str(organization_name))
    safe_contact = html_escape(str(contact_admin_name))
    safe_phone = html_escape(str(contact_phone))
    safe_entities = html_escape(str(entities_count))

    attachments = None
    try:
        tc_content = latest_tc.content if latest_tc else "<p>Terms of Service not found.</p>"
        wrapped_tc_html = wrap_legal_artifact_for_pdf(
            tc_content,
            document_title="Terms of Service — Grow Corporate Treasury Platform",
            version=tc_version,
        )
        pdf_content = await generate_multipage_pdf_from_html(
            wrapped_tc_html,
            filename_hint="Grow_Terms_of_Service",
        )
        if isinstance(pdf_content, (bytes, bytearray)) and len(pdf_content) > 0:
            attachments = [EmailAttachment("Grow_Terms_of_Service.pdf", bytes(pdf_content), "application/pdf")]
    except Exception as pdf_err:
        logger.warning(f"Failed to generate Terms of Service PDF attachment for trial request: {pdf_err}")
        attachments = None

    tos_note_html = (
        f"A copy of our Terms of Service (v{tc_version}) is attached for your records. "
        if attachments
        else ""
    )

    email_settings = get_global_email_settings()
    subject = f"Grow Treasury — Guided Trial Sandbox Request Received ({organization_name})"
    body = build_standard_email_html(
        customer_name=safe_org,
        title="Guided Trial Sandbox Request Received",
        recipient_name=safe_contact,
        content_html=(
            f"<p>Thank you for requesting Guided Trial Sandbox access for <strong>{safe_org}</strong>.</p>"
            f"<div style='margin: 16px 0; padding: 12px 16px; background-color: #f8fafc; border-left: 3px solid #1e40af; border-radius: 6px;'>"
            f"<strong style='font-size:12px; color:#0f172a; text-transform:uppercase; letter-spacing:0.5px;'>Selected Modules:</strong>"
            f"<ul style='margin:8px 0 0 0; padding-left:18px; color:#334155; font-size:13px;'>{bullets_html}</ul>"
            f"</div>"
            f"<p><strong>What happens next:</strong> A treasury specialist will reach out briefly to align on your evaluation focus and send your private workspace activation link. No Commercial Register or bank setup is required to test inside the sandbox.</p>"
            f"<p style='color:#64748b; font-size:12.5px;'>{tos_note_html}In the meantime, you can explore live platform workflows anytime below.</p>"
        ),
        cta_text="Explore Interactive Demo",
        cta_url=f"{base_url}/demo",
        platform_name="Grow Treasury Platform",
        footer_subtitle="Guided Sandbox Onboarding",
    )

    support_cc = "support@growbusinessdevelopment.com"
    cc_emails = [support_cc] if admin_email.lower() != support_cc else []
    if background_tasks:
        background_tasks.add_task(
            send_email,
            db,
            [admin_email],
            subject,
            body,
            {},
            email_settings,
            cc_emails=cc_emails,
            attachments=attachments,
        )

    # Notify System Owner(s) immediately of the new corporate trial lead
    try:
        from app.constants import UserRole
        from app.models.models import User
        so_emails = [
            u_email for (u_email,) in db.query(User.email).filter(
                User.role == UserRole.SYSTEM_OWNER,
                User.is_deleted == False,
            ).all() if u_email
        ]
        if so_emails and background_tasks:
            so_subject = f"New Trial Lead — {organization_name} ({contact_admin_name})"
            so_body = build_standard_email_html(
                customer_name="Grow Platform Admin",
                title=f"New Guided Trial Request: {safe_org}",
                content_html=(
                    f"<p>A new corporate prospect submitted a Guided Trial Sandbox request:</p>"
                    f"<ul style='line-height:1.7; color:#1e293b;'>"
                    f"<li><strong>Company:</strong> {safe_org} ({safe_entities} entity structure)</li>"
                    f"<li><strong>Contact:</strong> {safe_contact}</li>"
                    f"<li><strong>Work Email:</strong> {html_escape(admin_email)}</li>"
                    f"<li><strong>Phone / WhatsApp:</strong> {safe_phone}</li>"
                    f"<li><strong>Commercial Register:</strong> {'Attached' if has_cr_file else 'Skipped (Sandbox Mode)'}</li>"
                    f"<li><strong>Modules:</strong> {', '.join(html_escape(str(m)) for m in module_names)}</li>"
                    f"</ul>"
                ),
                cta_text="Review in System Owner Console",
                cta_url=f"{base_url}/system-owner/customers",
                platform_name="Grow Lead Alerts",
                footer_subtitle="System Owner Notification",
            )
            background_tasks.add_task(send_email, db, so_emails, so_subject, so_body, {}, email_settings)
    except Exception as so_err:
        logger.warning(f"System Owner trial lead notification skipped: {so_err}")
    
    return db_registration


@router.get("/documents/", tags=["Public"], response_class=FileResponse)
def serve_document(
    request: Request,
    file_path: str = Query(..., description="The path to the document to be served."),
    token: Optional[str] = Query(None, description="Optional Bank Dealer invitation token"),
    auth_token: Optional[str] = Query(None, description="Optional Corporate User JWT"),
    db: Session = Depends(get_db)
):
    """
    Legacy local quotation document reader.
    Restricted strictly to authenticated Corporate Users or invited Bank Dealers,
    and confined exclusively to uploads/quotations/ (never inbox or AI chat logs).
    """
    from app.core.security import verify_access_token

    allowed_base = os.path.realpath(os.path.join("uploads", "quotations"))
    full_path = os.path.realpath(file_path)

    if not (full_path.startswith(allowed_base + os.sep)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access to the requested path is forbidden."
        )

    is_authorized = False
    auth_header = request.headers.get("Authorization") if request else None
    raw_jwt = auth_header.split(" ", 1)[1].strip() if (auth_header and auth_header.startswith("Bearer ")) else auth_token
    if raw_jwt:
        try:
            payload = verify_access_token(raw_jwt)
            if payload.get("user_id"):
                is_authorized = True
        except Exception:
            is_authorized = False

    if not is_authorized and token:
        from app.models.models_quotation import QuotationBankAssignment
        assignment = db.query(QuotationBankAssignment).filter(
            QuotationBankAssignment.token == token,
            QuotationBankAssignment.is_deleted == False
        ).first()
        if assignment and getattr(assignment, "is_document_visible", True) is not False:
            rfq = getattr(assignment, "rfq", None)
            fname = os.path.basename(full_path)
            rfq_paths = ((rfq.document_path or "") if rfq else "") + " " + " ".join(
                [(l.document_path or "") for l in (getattr(rfq, "legs", None) or [])]
            )
            if fname in rfq_paths:
                is_authorized = True

    if not is_authorized:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required to access document."
        )

    if not os.path.isfile(full_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found."
        )

    return FileResponse(full_path)


from pydantic import BaseModel
from typing import Optional

class InstructionVerifyConfirmIn(BaseModel):
    token: str
    code: str
    lg_tail: Optional[str] = ""
    bank_branch_code: Optional[str] = ""
    elapsed_ms: Optional[int] = None


def _valid_ip_or_none(value: Optional[str]) -> Optional[str]:
    import ipaddress
    try:
        return str(ipaddress.ip_address((value or "").strip()))
    except ValueError:
        return None


def _extract_client_ip(request: Request) -> str:
    """
    Extracts the originating client IP behind reverse proxies (Render / Cloudflare).

    Order matters for anti-spoofing:
    1. `CF-Connecting-IP` — set/overwritten by Cloudflare's edge (which fronts Render), so a
       visitor cannot fake it.
    2. `X-Forwarded-For` / `X-Real-IP` — fallback only; the left-most XFF entry can be supplied
       by the visitor, so it is NOT trusted when the Cloudflare header is available.
    3. Direct socket address.
    Anything that is not a syntactically valid IP is ignored (prevents junk/oversized keys).
    """
    cf_ip = _valid_ip_or_none(request.headers.get("cf-connecting-ip"))
    if cf_ip:
        return cf_ip
    xff = request.headers.get("x-forwarded-for")
    if xff:
        xff_ip = _valid_ip_or_none(xff.split(",")[0])
        if xff_ip:
            return xff_ip
    xrip = _valid_ip_or_none(request.headers.get("x-real-ip"))
    if xrip:
        return xrip
    return request.client.host if request.client else "unknown"


@router.get("/verify-instruction/inspect", tags=["Public"])
def inspect_instruction_qr_token(
    request: Request,
    t: str = Query(..., description="Cryptographic QR verification token"),
    db: Session = Depends(get_db),
):
    """
    Public endpoint: validates the QR token signature & secret issuance timestamp,
    and returns the masked instruction reference for the 6-digit Security Code gate.
    """
    from app.core.instruction_verification import inspect_verification_token
    client_ip = _extract_client_ip(request)
    return inspect_verification_token(db, t, client_ip=client_ip)


@router.post("/verify-instruction/confirm", tags=["Public"])
def confirm_instruction_authenticity(
    payload: InstructionVerifyConfirmIn,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Public endpoint: verifies the 6-digit Security Code printed on the physical letter
    against the cryptographic QR token and secret database issuance timestamp.
    """
    from app.core.instruction_verification import verify_instruction_with_code
    from app.crud.crud import log_action

    client_ip = _extract_client_ip(request)
    result = verify_instruction_with_code(
        db,
        payload.token,
        payload.code,
        client_ip=client_ip,
        honeypot_value=payload.bank_branch_code or "",
        elapsed_ms=payload.elapsed_ms,
        lg_tail=payload.lg_tail or "",
    )
    honeypot_tripped = bool(result.pop("_honeypot_tripped", False))
    tamper_alert = bool(result.get("tamper_alert", False))

    try:
        action_name = (
            "PUBLIC_INSTRUCTION_HONEYPOT_TRIGGERED"
            if honeypot_tripped
            else (
                "PUBLIC_INSTRUCTION_TAMPER_ALERT"
                if tamper_alert
                else ("PUBLIC_INSTRUCTION_VERIFIED" if result.get("verified") else "PUBLIC_INSTRUCTION_VERIFY_FAILED")
            )
        )
        log_action(
            db,
            user_id=None,
            action_type=action_name,
            entity_type="LGInstruction",
            entity_id=None,
            details={
                "token_prefix": (payload.token or "")[:24],
                "verified": bool(result.get("verified")),
                "instruction_serial": result.get("instruction_serial"),
                "client_ip": client_ip,
                "honeypot_tripped": honeypot_tripped,
                "tamper_alert": tamper_alert,
            },
            customer_id=None,
            lg_record_id=None,
        )
        db.commit()
    except Exception:
        db.rollback()

    return result
