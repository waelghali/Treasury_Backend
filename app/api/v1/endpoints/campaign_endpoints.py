# app/api/v1/endpoints/campaign_endpoints.py
from typing import List, Optional, Any
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.core.security import get_current_user, get_current_system_owner
from app.models import User, Bank
from app.crud.crud_campaign import crud_campaign
import csv
import io
from fastapi.responses import StreamingResponse

from app.schemas.schemas_campaign import (
    CampaignCreate, CampaignUpdate, CampaignOut,
    CashbackClaimOut, CashbackClaimStatusUpdate,
    CampaignPerformanceStats, CustomerCashbackSummary,
    ManualVerifyRequest, ActiveBankPromotionOut
)

def _format_claim_out(c) -> CashbackClaimOut:
    lg_num = None
    bank_name = None
    if c.issued_lg_record:
        lg_num = c.issued_lg_record.bank_lg_number or c.issued_lg_record.lg_ref_number
        if c.issued_lg_record.bank:
            bank_name = c.issued_lg_record.bank.name
    elif c.lg_record:
        lg_num = c.lg_record.lg_number
        if c.lg_record.issuing_bank:
            bank_name = c.lg_record.issuing_bank.name

    verified_by_name = None
    if c.verified_by_user:
        verified_by_name = f"{c.verified_by_user.first_name or ''} {c.verified_by_user.last_name or ''}".strip() or c.verified_by_user.email

    return CashbackClaimOut(
        id=c.id,
        campaign_id=c.campaign_id,
        campaign_name=c.campaign.name if c.campaign else None,
        customer_id=c.customer_id,
        customer_name=c.customer.name if c.customer else None,
        issued_lg_record_id=c.issued_lg_record_id,
        lg_record_id=c.lg_record_id,
        lg_number=lg_num,
        bank_name=bank_name,
        lg_amount=c.lg_amount,
        cashback_amount=c.cashback_amount,
        currency_symbol=c.currency_symbol,
        status=c.status,
        created_at=c.created_at,
        verified_at=c.verified_at,
        verified_by_user_id=c.verified_by_user_id,
        verified_by_name=verified_by_name,
        verification_source=c.verification_source,
        paid_at=c.paid_at,
        notes=c.notes
    )

router = APIRouter()

# ==============================================================================
# SYSTEM OWNER ENDPOINTS
# ==============================================================================

@router.get("/system-owner/campaigns", response_model=List[CampaignOut])
def list_campaigns_for_system_owner(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    campaigns = crud_campaign.list_campaigns(db, skip=skip, limit=limit)
    res = []
    for c in campaigns:
        out = CampaignOut.from_orm(c)
        claims = c.claims or []
        out.total_claims_count = len([cl for cl in claims if not cl.is_deleted])
        out.total_cashback_accrued = sum((cl.cashback_amount for cl in claims if not cl.is_deleted and cl.status != "REJECTED"), Decimal("0.00"))
        res.append(out)
    return res


@router.get("/system-owner/campaigns/banks")
def list_banks_for_campaigns(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    """Retrieve all available partner banks for campaign configuration."""
    banks = db.query(Bank).order_by(Bank.name).all()
    return [
        {
            "id": b.id,
            "name": b.name,
            "short_name": getattr(b, "short_name", None),
            "swift_code": getattr(b, "swift_code", None)
        }
        for b in banks
    ]


@router.post("/system-owner/campaigns", response_model=CampaignOut, status_code=status.HTTP_201_CREATED)
def create_campaign(
    obj_in: CampaignCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    if obj_in.end_date < obj_in.start_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="End date cannot be earlier than start date."
        )
    return crud_campaign.create_campaign(db, obj_in=obj_in)


@router.get("/system-owner/campaigns/{campaign_id}", response_model=CampaignOut)
def get_campaign_detail(
    campaign_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    camp = crud_campaign.get_campaign(db, campaign_id)
    if not camp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign not found.")
    return camp


@router.put("/system-owner/campaigns/{campaign_id}", response_model=CampaignOut)
@router.patch("/system-owner/campaigns/{campaign_id}", response_model=CampaignOut)
def update_campaign(
    campaign_id: int,
    obj_in: CampaignUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    if obj_in.start_date and obj_in.end_date and obj_in.end_date < obj_in.start_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="End date cannot be earlier than start date."
        )
    camp = crud_campaign.update_campaign(db, campaign_id, obj_in)
    if not camp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign not found.")

    out = CampaignOut.from_orm(camp)
    claims = camp.claims or []
    out.total_claims_count = len([cl for cl in claims if not cl.is_deleted])
    out.total_cashback_accrued = sum((cl.cashback_amount for cl in claims if not cl.is_deleted and cl.status != "REJECTED"), Decimal("0.00"))
    return out


@router.get("/system-owner/cashback-claims", response_model=List[CashbackClaimOut])
def list_cashback_claims_for_system_owner(
    customer_id: Optional[int] = None,
    campaign_id: Optional[int] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    claims = crud_campaign.list_claims(
        db,
        customer_id=customer_id,
        campaign_id=campaign_id,
        status=status_filter,
        skip=skip,
        limit=limit
    )
    return [_format_claim_out(c) for c in claims]


@router.post("/system-owner/cashback-claims/{claim_id}/manual-verify", response_model=CashbackClaimOut)
def manual_verify_claim(
    claim_id: int,
    payload: ManualVerifyRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    claim = crud_campaign.manual_verify_claim(db, claim_id=claim_id, user_id=current_user.id, notes=payload.notes)
    if not claim:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Claim not found.")
    return _format_claim_out(claim)


@router.patch("/system-owner/cashback-claims/{claim_id}/status", response_model=CashbackClaimOut)
def update_claim_status(
    claim_id: int,
    obj_in: CashbackClaimStatusUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    if obj_in.status not in ["PAID", "REJECTED", "RECONCILED", "CALCULATED", "VERIFIED"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid status. Must be PAID, REJECTED, RECONCILED, VERIFIED, or CALCULATED."
        )
    claim = crud_campaign.update_claim_status(db, claim_id, obj_in)
    if not claim:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Claim not found.")
    return _format_claim_out(claim)


@router.get("/system-owner/campaigns-stats", response_model=CampaignPerformanceStats)
def get_campaign_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    return crud_campaign.get_system_stats(db)


@router.get("/system-owner/campaigns/export-partner-report")
def export_partner_report(
    bank_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_system_owner)
):
    claims = crud_campaign.list_claims(db, customer_id=customer_id, status=status_filter, limit=1000)
    if bank_id:
        claims = [c for c in claims if (c.issued_lg_record and c.issued_lg_record.bank_id == bank_id) or (c.lg_record and c.lg_record.issuing_bank_id == bank_id)]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Claim ID", "Campaign", "Customer", "Bank",
        "LG Reference / Bank LG #", "LG Amount (EGP)",
        "Cashback Amount (EGP)", "Status", "Verification Source",
        "Verified At", "Verified By", "Notes"
    ])
    for c in claims:
        fc = _format_claim_out(c)
        writer.writerow([
            fc.id,
            fc.campaign_name or "N/A",
            fc.customer_name or f"Customer #{fc.customer_id}",
            fc.bank_name or "All Banks",
            fc.lg_number or "N/A",
            f"{float(fc.lg_amount):.2f}",
            f"{float(fc.cashback_amount):.2f}",
            fc.status,
            fc.verification_source or "N/A",
            fc.verified_at.strftime("%Y-%m-%d %H:%M") if fc.verified_at else "N/A",
            fc.verified_by_name or "N/A",
            fc.notes or ""
        ])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=partner_cashback_audit_report.csv"}
    )


# ==============================================================================
# CUSTOMER / TENANT ENDPOINTS (Corporate Admin & End User)
# ==============================================================================

@router.get("/customer/campaigns/active-bank-promotions", response_model=List[ActiveBankPromotionOut])
def get_active_bank_promotions(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    promotions = crud_campaign.get_active_bank_promotions(db, customer_id=current_user.customer_id)
    return [ActiveBankPromotionOut(**p) for p in promotions]


@router.get("/customer/campaigns/cashback-summary", response_model=CustomerCashbackSummary)
def get_customer_cashback_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if not current_user.customer_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User is not associated with a customer organization.")

    summary = crud_campaign.get_customer_summary(db, customer_id=current_user.customer_id)
    raw_claims = summary["claims"]
    formatted_claims = [_format_claim_out(c) for c in raw_claims]

    return CustomerCashbackSummary(
        active_campaign=CampaignOut.from_orm(summary["active_campaign"]) if summary["active_campaign"] else None,
        claims_used=summary["claims_used"],
        claims_limit=summary["claims_limit"],
        total_earned=summary["total_earned"],
        total_paid=summary["total_paid"],
        claims=formatted_claims
    )


@router.get("/customer/campaigns/check-eligibility")
def check_lg_eligibility(
    bank_id: Optional[int] = None,
    amount: Optional[Decimal] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Live helper for the UI when creating or recording an LG:
    Returns whether the transaction is eligible for cashback and the estimated reward.
    """
    if not current_user.customer_id:
        return {"is_eligible": False}

    camp = crud_campaign.get_active_campaign_for_lg(
        db,
        bank_id=bank_id,
        customer_id=current_user.customer_id
    )
    if not camp:
        return {"is_eligible": False}

    estimated_cashback = Decimal("0.00")
    if amount and amount > 0:
        raw = amount * Decimal(str(camp.cashback_percentage))
        estimated_cashback = min(raw, Decimal(str(camp.max_cashback_per_lg)))

    summary = crud_campaign.get_customer_summary(db, customer_id=current_user.customer_id)
    remaining_quota = max(0, camp.max_lgs_per_customer - summary["claims_used"])

    return {
        "is_eligible": True,
        "campaign_id": camp.id,
        "campaign_name": camp.name,
        "bank_id": camp.bank_id,
        "cashback_percentage": float(camp.cashback_percentage),
        "cashback_rate_label": f"{float(camp.cashback_percentage * 100):.2f}%",
        "max_cashback_per_lg": float(camp.max_cashback_per_lg),
        "estimated_cashback": float(round(estimated_cashback, 2)),
        "remaining_quota": remaining_quota,
        "message": f"Eligible for {float(camp.cashback_percentage * 100):.2f}% Cashback (up to EGP {float(camp.max_cashback_per_lg):,.0f}) under '{camp.name}'"
    }
