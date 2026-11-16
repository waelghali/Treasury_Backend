# app/api/v1/endpoints/platform_activity_endpoints.py
"""
Platform Activity & Signatory Control Verification Endpoints.
Accessible to Corporate Admin and Checker (Reviewer / Signatory) roles.
"""

import base64
import io
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session, selectinload

from app.core.document_generator import generate_pdf_from_html
from app.core.security import TokenData, get_current_approver_context
from app.database import get_db
from app.models.models import CustomerEntity, LGInstruction, LGRecord, User
from app.services.platform_activity_service import platform_activity_service

logger = logging.getLogger(__name__)
router = APIRouter()


class VerifyListPayload(BaseModel):
    raw_text: str
    window_days: int = 45
    cohort_start_date: Optional[str] = None
    cohort_end_date: Optional[str] = None


@router.get("/filter-options", response_model=Dict[str, Any])
def get_activity_filter_options(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    customer_id = current_user.customer_id
    users = (
        db.query(User)
        .filter(User.customer_id == customer_id, User.is_deleted == False)
        .order_by(User.email.asc())
        .all()
    )
    entities = (
        db.query(CustomerEntity)
        .filter(CustomerEntity.customer_id == customer_id, CustomerEntity.is_deleted == False)
        .order_by(CustomerEntity.entity_name.asc())
        .all()
    )
    return {
        "users": [
            {
                "id": u.id,
                "email": u.email,
                "role": u.role.value if hasattr(u.role, "value") else str(u.role),
            }
            for u in users
        ],
        "entities": [
            {
                "id": e.id,
                "entity_name": e.entity_name,
                "code": e.code,
            }
            for e in entities
        ],
    }


@router.get("/feed", response_model=Dict[str, Any])
def get_platform_activity_feed_endpoint(
    module: str = Query("ALL", description="ALL, CUSTODY, ISSUANCE, QUOTATIONS, RECONCILIATION, SMART_INBOX, GOVERNANCE"),
    category: str = Query("ALL", description="ALL, RENEWAL, RELEASE, NEW_LG, AMENDMENT, LIQUIDATION, LOGISTICS, APPROVAL, RFQ_DEAL, RECON, INBOX, ADMIN"),
    user_id: Optional[int] = Query(None),
    actor_key: Optional[str] = Query(None),
    entity_id: Optional[int] = Query(None),
    bank: Optional[str] = Query(None),
    lifecycle: str = Query("ALL", description="ALL, NEEDS_SIGN_OR_DELIVERY, AWAITING_BANK_REPLY, PENDING_APPROVAL, FLAGGED, COMPLETED"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(250, ge=10, le=1000),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    return platform_activity_service.get_platform_activity_feed(
        db=db,
        customer_id=current_user.customer_id,
        module_filter=module,
        category_filter=category,
        user_id_filter=user_id,
        actor_key_filter=actor_key,
        entity_id_filter=entity_id,
        bank_filter=bank,
        lifecycle_filter=lifecycle,
        start_date=start_date,
        end_date=end_date,
        search=search,
        limit=limit,
    )


@router.get("/audit-log-facets", response_model=Dict[str, Any])
def get_audit_log_facets_endpoint(
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    return platform_activity_service.get_audit_log_facets(db=db, customer_id=current_user.customer_id)


@router.get("/controls", response_model=Dict[str, Any])
def get_control_verification_workspace_endpoint(
    window_days: int = Query(45, ge=1, le=365),
    cohort_start_date: Optional[str] = Query(None),
    cohort_end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    return platform_activity_service.get_control_verification_workspace(
        db=db,
        customer_id=current_user.customer_id,
        window_days=window_days,
        cohort_start_date=cohort_start_date,
        cohort_end_date=cohort_end_date,
    )


@router.post("/verify-list", response_model=Dict[str, Any])
def verify_handed_list_endpoint(
    payload: VerifyListPayload = Body(...),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    return platform_activity_service.verify_handed_list(
        db=db,
        customer_id=current_user.customer_id,
        raw_text=payload.raw_text,
        window_days=payload.window_days,
        cohort_start_date=payload.cohort_start_date,
        cohort_end_date=payload.cohort_end_date,
    )


@router.get("/instructions/{instruction_id}/preview-letter")
async def preview_custody_instruction_letter(
    instruction_id: int,
    raw_pdf: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: TokenData = Depends(get_current_approver_context),
):
    """
    Read-only Signatory / Corporate Admin preview of an official Custody LGInstruction letter.
    Does NOT mutate `is_printed` on the instruction.
    """
    from app.api.v1.endpoints.end_user import _render_instruction_letter_html

    db_instruction = (
        db.query(LGInstruction)
        .options(
            selectinload(LGInstruction.lg_record).selectinload(LGRecord.lg_currency),
            selectinload(LGInstruction.lg_record).selectinload(LGRecord.issuing_bank),
            selectinload(LGInstruction.lg_record).selectinload(LGRecord.beneficiary_corporate),
            selectinload(LGInstruction.lg_record).selectinload(LGRecord.internal_owner_contact),
            selectinload(LGInstruction.lg_record).selectinload(LGRecord.customer),
            selectinload(LGInstruction.lg_record).selectinload(LGRecord.communication_bank),
        )
        .filter(
            LGInstruction.id == instruction_id,
            LGInstruction.is_deleted == False,
            LGInstruction.lg_record.has(LGRecord.customer_id == current_user.customer_id),
        )
        .first()
    )
    if not db_instruction:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instruction letter not found.")

    rendered_html = _render_instruction_letter_html(db, db_instruction)
    pdf_bytes = await generate_pdf_from_html(rendered_html, f"signatory_preview_{db_instruction.serial_number}")
    if not pdf_bytes:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to render PDF preview.")

    if raw_pdf:
        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{db_instruction.serial_number}.pdf"'},
        )

    pdf_base64 = base64.b64encode(pdf_bytes).decode("utf-8")
    lg_num = db_instruction.lg_record.lg_number if db_instruction.lg_record else "N/A"
    html_page = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Signatory Preview — {db_instruction.serial_number}</title>
  <style>
    body {{ margin: 0; padding: 0; overflow: hidden; font-family: 'Segoe UI', sans-serif; background: #0f172a; }}
    .bar {{ height: 50px; background: #0f172a; color: #f8fafc; padding: 0 20px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid #334155; }}
    .badge {{ background: #1d4ed8; color: #fff; font-size: 11px; font-weight: 700; padding: 3px 8px; border-radius: 999px; margin-right: 10px; }}
    .btn {{ background: #334155; color: #fff; text-decoration: none; padding: 6px 12px; border-radius: 6px; font-size: 12px; font-weight: 600; }}
    iframe {{ width: 100vw; height: calc(100vh - 50px); border: none; background: #e2e8f0; }}
  </style>
</head>
<body>
  <div class="bar">
    <div><span class="badge">SIGNATORY VERIFICATION PREVIEW (READ-ONLY)</span> LG: <strong>{lg_num}</strong> &bull; Serial: <strong>{db_instruction.serial_number}</strong></div>
    <a class="btn" href="data:application/pdf;base64,{pdf_base64}" download="{db_instruction.serial_number}.pdf">Download PDF</a>
  </div>
  <iframe src="data:application/pdf;base64,{pdf_base64}"></iframe>
</body>
</html>"""
    return HTMLResponse(content=html_page)
