# app/api/v1/endpoints/trial_sandbox.py
"""
Sales-Engineered Trial Sandbox API (`TRIAL_MODE=true` ONLY).

PRODUCTION ZERO-BLAST-RADIUS GUARANTEE:
- On Production (`TRIAL_MODE=false`), `GET /status` returns `{"trial_mode": false}` in <0.1ms
  with zero DB queries, and every action endpoint returns `404 Not Found`.
- On Trial Sandbox (`TRIAL_MODE=true`):
  1. Lazily ensures the tenant's pre-seeded facilities, entities, FX desks & personas exist.
  2. Powers 1-click solo `Maker <-> Checker <-> Corporate Admin` persona switching.
  3. Powers the Dual-Side Quotation Simulator:
     - Auto-submits realistic AES-256-GCM encrypted bids from Simulated Banks B & C anchored
       to live market / CBE rates.
     - Surfaces Bank A's real Zero-Password Dealer Desk URL + live 6-digit Trial OTP + 1-click
       Magic Dealer Link so the prospect can test the bank trader console & fat-finger rate
       guardrail firsthand.
     - Provides 1-click `[Fast-Forward to 00:00]` to unseal bids, cherry-pick winners, and
       generate the HMAC-SHA256 Winner-Only Deal Receipt & Best Execution Audit Certificate.
"""

import io
import logging
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.constants import UserRole
from app.core.otp_security import hash_otp_code
from app.core.routing import get_frontend_base_url
from app.core.security import TokenData, create_access_token, get_current_user
from app.core.trial_guard import (
    ensure_trial_activated_clock,
    get_trial_usage_snapshot,
    is_trial_customer,
)
from app.database import get_db
from app.services.trial_sandbox_seeder import ensure_trial_sandbox_seeded

logger = logging.getLogger("app.trial_sandbox")

router = APIRouter()

# Realistic fallback FX mid-rates when live feed is offline
_FALLBACK_PAIR_RATES: Dict[str, float] = {
    "USD/EGP": 50.8500,
    "EUR/EGP": 54.9200,
    "GBP/EGP": 64.3500,
    "SAR/EGP": 13.5600,
    "AED/EGP": 13.8450,
    "EUR/USD": 1.0800,
    "GBP/USD": 1.2655,
}


class SwitchPersonaIn(BaseModel):
    target_role: str  # "end_user" | "checker" | "corporate_admin"


def _require_trial_access(db: Session, current_user: TokenData) -> int:
    if not current_user.customer_id or not is_trial_customer(db, current_user.customer_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not available for non-trial accounts.")
    return int(current_user.customer_id)


def _resolve_reference_fx_rate(db: Session, buy_ccy: Optional[str], sell_ccy: Optional[str]) -> float:
    b = (buy_ccy or "USD").strip().upper()
    s = (sell_ccy or "EGP").strip().upper()
    pair = f"{b}/{s}"
    inv_pair = f"{s}/{b}"

    try:
        from app.services.live_market_service import live_market_service
        live = live_market_service.get_live_interbank_mid(db, b, s)
        if isinstance(live, dict):
            mid = live.get("live_mid") or live.get("cbe_official_mid")
            if mid and float(mid) > 0:
                return float(mid)
    except Exception:
        pass

    if pair in _FALLBACK_PAIR_RATES:
        return _FALLBACK_PAIR_RATES[pair]
    if inv_pair in _FALLBACK_PAIR_RATES and _FALLBACK_PAIR_RATES[inv_pair] > 0:
        return round(1.0 / _FALLBACK_PAIR_RATES[inv_pair], 6)
    return 50.8500


def _ensure_demo_otp_for_assignment(db: Session, assignment: Any) -> Dict[str, str]:
    """Creates or refreshes a predictable 6-digit demo OTP + magic token on Bank #1 for instant testing."""
    from app.models.models_quotation import QuotationAccessOTP, QuotationBank

    q_bank = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
    bank_name = q_bank.bank.name if (q_bank and q_bank.bank) else "Relationship Bank A"

    dealer_email = "fx.desk@fncb-sandbox-bank.com"
    if q_bank:
        if isinstance(q_bank.contacts, list) and q_bank.contacts:
            dealer_email = q_bank.contacts[0].get("email") or dealer_email
        elif q_bank.emails:
            dealer_email = q_bank.emails.split(",")[0].strip() or dealer_email

    # Deterministic per-assignment 6-digit code so UI can display it cleanly to the trial evaluator
    demo_code = str(100000 + (abs(hash(str(assignment.id))) % 899999))
    now_utc = datetime.now(timezone.utc)

    existing_otp = (
        db.query(QuotationAccessOTP)
        .filter(
            QuotationAccessOTP.assignment_id == assignment.id,
            QuotationAccessOTP.email == dealer_email,
            QuotationAccessOTP.is_used == False,
            QuotationAccessOTP.expires_at > now_utc,
        )
        .order_by(QuotationAccessOTP.id.desc())
        .first()
    )

    if not existing_otp:
        magic_tok = uuid.uuid4().hex
        existing_otp = QuotationAccessOTP(
            assignment_id=assignment.id,
            email=dealer_email,
            role="EXECUTION",
            otp_code=hash_otp_code(demo_code),
            magic_token=magic_tok,
            expires_at=now_utc + timedelta(hours=2),
            is_used=False,
            failed_attempts=0,
        )
        db.add(existing_otp)
        db.flush()

    return {
        "bank_name": bank_name,
        "dealer_email": dealer_email,
        "demo_otp": demo_code,
        "token": assignment.token,
        "magic_token": existing_otp.magic_token or "",
    }


def _populate_simulated_bank_quotes(
    db: Session,
    rfq: Any,
    include_first_bank_if_empty: bool = False,
) -> int:
    """
    Submits realistic AES-256-GCM encrypted bids for Simulated Banks (#2, #3, and optionally #1
    if the user fast-forwards without manually typing a quote on Bank #1) so multi-leg cherry-picking
    and All-In net ranking shine immediately.
    """
    from app.models.models_quotation import (
        QuotationBank,
        QuotationBankAssignment,
        QuotationOffer,
        QuotationTBillOffer,
    )
    from app.services.tenant_key_service import tenant_key_service

    assignments = (
        db.query(QuotationBankAssignment)
        .filter(QuotationBankAssignment.rfq_id == rfq.id)
        .order_by(QuotationBankAssignment.created_at.asc(), QuotationBankAssignment.id.asc())
        .all()
    )
    if not assignments:
        return 0

    dek = tenant_key_service.get_or_create_tenant_dek(db, rfq.customer_id)
    legs = list(getattr(rfq, "legs", None) or [])
    is_tbill = (rfq.type or "FX_SPOT").upper() == "TBILL"
    created_quotes = 0

    # Spread offsets (bps) designed so Bank #1 wins Leg 1 and Bank #2 wins Leg 2 (showcasing Cherry-Picking!)
    leg1_bps = [-12.0, +8.0, +24.0]
    leg2_bps = [+16.0, -14.0, +29.0]

    for b_idx, asgn in enumerate(assignments):
        if b_idx == 0 and not include_first_bank_if_empty:
            continue

        q_bank = db.query(QuotationBank).filter(QuotationBank.id == asgn.quotation_bank_id).first()
        dealer_email = f"sim.dealer.{b_idx + 1}@sandbox-bank.com"
        if q_bank and q_bank.emails:
            dealer_email = q_bank.emails.split(",")[0].strip() or dealer_email

        if is_tbill:
            existing_tb = (
                db.query(QuotationTBillOffer)
                .filter(QuotationTBillOffer.assignment_id == asgn.id)
                .count()
            )
            if existing_tb > 0:
                continue

            is_sell = (rfq.direction or "Buy").lower() == "sell"
            base_disc = float(rfq.eval_rate or 25.85)
            # Buy T-Bill -> higher discount rate is better; Sell -> lower is better
            delta = [0.22, 0.08, -0.15][b_idx % 3]
            disc_rate = round(base_disc - delta if is_sell else base_disc + delta, 4)
            settle_str = rfq.settlement_date_start or date.today().isoformat()
            mat_str = rfq.maturity_date_start or (date.today() + timedelta(days=182)).isoformat()
            face_amt = float(rfq.amount or 5000000.0)

            tb_offer = QuotationTBillOffer(
                assignment_id=asgn.id,
                leg_id=legs[0].id if legs else None,
                settlement_date=settle_str,
                maturity_date=mat_str,
                discount_rate=None,
                max_amount=None,
                notes="Simulated Sovereign T-Bill Treasury Desk Bid",
                submitted_by_email=dealer_email,
            )
            tenant_key_service.apply_encrypted_tbill_offer(tb_offer, disc_rate, face_amt, dek)
            db.add(tb_offer)
            created_quotes += 1
        else:
            target_legs = legs if legs else [None]
            for l_pos, leg_obj in enumerate(target_legs):
                leg_id = leg_obj.id if leg_obj else None
                q_filter = [QuotationOffer.assignment_id == asgn.id]
                if leg_id:
                    q_filter.append(QuotationOffer.leg_id == leg_id)
                if db.query(QuotationOffer).filter(*q_filter).count() > 0:
                    continue

                buy_c = (leg_obj.buy_currency if leg_obj else rfq.buy_currency) or "USD"
                sell_c = (leg_obj.sell_currency if leg_obj else rfq.sell_currency) or "EGP"
                direction = ((leg_obj.direction if leg_obj else rfq.direction) or "Buy").lower()

                mid = _resolve_reference_fx_rate(db, buy_c, sell_c)
                bps_table = leg1_bps if l_pos % 2 == 0 else leg2_bps
                bps = bps_table[b_idx % len(bps_table)]
                # Flip sign for Sell so winner ordering stays natural
                adj_factor = 1.0 + ((-bps if direction == "sell" else bps) / 10000.0)
                quoted_rate = round(mid * adj_factor, 4)

                fx_offer = QuotationOffer(
                    assignment_id=asgn.id,
                    leg_id=leg_id,
                    price=None,
                    notes="Simulated FX Dealing Room Sealed Quote",
                    submitted_by_email=dealer_email,
                )
                tenant_key_service.apply_encrypted_offer_price(fx_offer, quoted_rate, dek)
                db.add(fx_offer)
                created_quotes += 1

    db.flush()
    return created_quotes


def _build_rfq_simulator_summary(db: Session, customer_id: int, request: Request) -> Optional[Dict[str, Any]]:
    from app.models.models_quotation import (
        QuotationBankAssignment,
        QuotationOffer,
        QuotationRequest,
        QuotationTBillOffer,
    )

    latest_rfq = (
        db.query(QuotationRequest)
        .filter(
            QuotationRequest.customer_id == customer_id,
            QuotationRequest.is_deleted == False,
            QuotationRequest.status.in_(["PENDING_APPROVAL", "PENDING", "OPEN", "EVALUATING", "COMPLETED"]),
        )
        .order_by(QuotationRequest.created_at.desc())
        .first()
    )
    if not latest_rfq:
        return None

    assignments = (
        db.query(QuotationBankAssignment)
        .filter(QuotationBankAssignment.rfq_id == latest_rfq.id)
        .order_by(QuotationBankAssignment.created_at.asc(), QuotationBankAssignment.id.asc())
        .all()
    )
    if not assignments:
        return None

    # If the RFQ is OPEN and simulated banks haven't submitted quotes yet, auto-seed Banks #2 & #3!
    if latest_rfq.status == "OPEN":
        added = _populate_simulated_bank_quotes(db, latest_rfq, include_first_bank_if_empty=False)
        if added > 0:
            db.commit()

    first_asgn = assignments[0]
    otp_meta = _ensure_demo_otp_for_assignment(db, first_asgn)
    db.commit()

    asgn_ids = [a.id for a in assignments]
    fx_q = db.query(QuotationOffer.assignment_id).filter(QuotationOffer.assignment_id.in_(asgn_ids)).distinct().count()
    tb_q = (
        db.query(QuotationTBillOffer.assignment_id)
        .filter(QuotationTBillOffer.assignment_id.in_(asgn_ids))
        .distinct()
        .count()
    )
    quoted_banks = max(fx_q, tb_q)

    base_url = get_frontend_base_url(request=request)
    return {
        "rfq_id": latest_rfq.id,
        "ref_no": latest_rfq.ref_no,
        "type": latest_rfq.type,
        "status": latest_rfq.status,
        "acceptance_status": latest_rfq.acceptance_status,
        "total_banks": len(assignments),
        "quoted_banks": quoted_banks,
        "dealer_playground": {
            "bank_name": otp_meta["bank_name"],
            "dealer_email": otp_meta["dealer_email"],
            "demo_otp": otp_meta["demo_otp"],
            "otp_gate_url": f"{base_url}/public-quotation/{otp_meta['token']}",
            "magic_url": f"{base_url}/public-quotation/{otp_meta['token']}?magic_token={otp_meta['magic_token']}",
        },
    }


@router.get("/status")
def get_trial_sandbox_status(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Returns Trial Sandbox state, Launchpad progress, Persona Switcher roster, and active RFQ simulator.
    Returns `{"trial_mode": false}` immediately for non-trial commercial customers.
    """
    if not current_user.customer_id or not is_trial_customer(db, current_user.customer_id):
        return {"trial_mode": False}

    cust_id = int(current_user.customer_id)
    ensure_trial_sandbox_seeded(db, cust_id)
    ensure_trial_activated_clock(db, cust_id, user_id=current_user.user_id, trigger="workspace_open")
    db.commit()

    from app.models.models import Customer, CustomerEntity, InternalOwnerContact, LGCategory, User
    from app.models.models_issuance import IssuanceFacility

    cust_obj = db.query(Customer).filter(Customer.id == cust_id).first()
    company_domain = ""
    if cust_obj and cust_obj.contact_email and "@" in cust_obj.contact_email:
        company_domain = cust_obj.contact_email.rsplit("@", 1)[1].strip().lower()

    users = (
        db.query(User)
        .filter(User.customer_id == cust_id, User.is_deleted == False)
        .order_by(User.id.asc())
        .all()
    )

    role_labels = {
        "end_user": "End User",
        "corporate_admin": "Corporate Admin",
    }
    personas = []
    seen_roles = set()
    for role_key in ("end_user", "corporate_admin"):
        u = next((x for x in users if (x.role.value if hasattr(x.role, "value") else str(x.role)) == role_key), None)
        if u and role_key not in seen_roles:
            seen_roles.add(role_key)
            personas.append(
                {
                    "role": role_key,
                    "label": role_labels[role_key],
                    "email": u.email,
                    "user_id": u.id,
                }
            )

    primary_entity = (
        db.query(CustomerEntity)
        .filter(CustomerEntity.customer_id == cust_id, CustomerEntity.is_deleted == False)
        .order_by(CustomerEntity.id.asc())
        .first()
    )
    owner_row = (
        db.query(InternalOwnerContact)
        .filter(InternalOwnerContact.customer_id == cust_id, InternalOwnerContact.is_deleted == False)
        .order_by(InternalOwnerContact.id.asc())
        .first()
    )
    cat_row = (
        db.query(LGCategory)
        .filter(LGCategory.customer_id == cust_id, LGCategory.is_deleted == False)
        .order_by(LGCategory.id.asc())
        .first()
    )
    top_fac = (
        db.query(IssuanceFacility)
        .filter(IssuanceFacility.customer_id == cust_id, IssuanceFacility.is_deleted == False)
        .order_by(IssuanceFacility.id.asc())
        .first()
    )
    primary_bank_name = (
        f"{top_fac.bank.name} ({top_fac.bank.short_name})"
        if top_fac and top_fac.bank and top_fac.bank.short_name
        else (top_fac.bank.name if top_fac and top_fac.bank else "Emirates NBD Egypt (ENBD)")
    )

    snap = get_trial_usage_snapshot(db, cust_id)
    rfq_sim = _build_rfq_simulator_summary(db, cust_id, request)

    active_role_str = (
        current_user.role.value if hasattr(current_user.role, "value") else str(current_user.role or "")
    )
    return {
        "trial_mode": True,
        "customer_id": cust_id,
        "customer_name": current_user.customer_name,
        "active_role": active_role_str,
        "active_email": current_user.email,
        "personas": personas,
        "custody_defaults": {
            "beneficiary_entity_name": (
                primary_entity.entity_name if primary_entity else (current_user.customer_name or "Main Operating Co.")
            ),
            "internal_owner_email": owner_row.email if owner_row else "projects.owner@yourcompany.com",
            "internal_owner_manager": owner_row.manager_email if owner_row else (current_user.email or ""),
            "category_label": (
                f"{cat_row.name} ({cat_row.code})" if cat_row else "Major Infrastructure Contracts (IN)"
            ),
            "primary_bank_name": primary_bank_name,
        },
        "caps": snap["caps"],
        "primary_module": snap.get("primary_module", "custody"),
        "funnel_stage": snap.get("funnel_stage", "ACTIVATED"),
        "stage_label": snap.get("stage_label", "Activated"),
        "own_documents_uploaded": bool(snap.get("own_documents_uploaded", False)),
        "invited_colleagues_count": int(snap.get("invited_colleagues_count", 0)),
        "invited_colleagues": snap.get("invited_colleagues", []),
        "company_email_domain": company_domain,
        "days_remaining": snap.get("days_remaining"),
        "usage": snap["usage"],
        "steps_completed": snap["steps_completed"],
        "active_rfq_simulator": rfq_sim,
    }


def _generate_personalized_custody_lg_pdf(
    beneficiary_name: str,
    beneficiary_address: str,
    lg_number: str,
    bank_name: str,
    bank_address: str,
    bank_phone: str,
    bank_swift: str,
) -> bytes:
    """
    Generates a crisp, authentic single-page Performance Letter of Guarantee PDF
    addressed directly to the trial customer's exact Beneficiary Entity so AI Scan
    auto-selects Beneficiary Corporate, Issuing Bank (`Emirates NBD Egypt`), LG Type,
    Currency, Amount, Dates, URDG 758, and Mandatory Claim Statement in one shot.
    """
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    issue_dt = date.today() - timedelta(days=14)
    expiry_dt = date.today() + timedelta(days=351)

    # Subtle evaluation watermark in background (light enough never to hinder OCR)
    c.saveState()
    c.translate(width / 2, height / 2)
    c.rotate(34)
    c.setFillColor(colors.Color(0.90, 0.92, 0.95))
    c.setFont("Helvetica-Bold", 34)
    c.drawCentredString(0, 15, "GROW TRIAL SAMPLE LG")
    c.setFont("Helvetica-Bold", 18)
    c.drawCentredString(0, -18, "FOR AI SCAN EVALUATION ONLY — NON-NEGOTIABLE")
    c.restoreState()

    # Top safe-evaluation strip
    c.setFillColor(colors.HexColor("#0f172a"))
    c.rect(36, height - 52, width - 72, 22, fill=1, stroke=0)
    c.setFillColor(colors.HexColor("#6ee7b7"))
    c.setFont("Helvetica-Bold", 8.5)
    c.drawCentredString(
        width / 2,
        height - 44,
        "GROW GUIDED TRIAL SAMPLE DOCUMENT — PERSONALIZED FOR AI OCR & RISK EXTRACTION TESTING",
    )

    # Bank Letterhead Box
    y = height - 80
    c.setFillColor(colors.HexColor("#0b2d5c"))
    c.setFont("Helvetica-Bold", 17)
    c.drawString(42, y, bank_name.upper())
    c.setFillColor(colors.HexColor("#334155"))
    c.setFont("Helvetica", 9)
    c.drawString(42, y - 14, f"Corporate Trade Finance Department · {bank_address}")
    c.drawString(42, y - 26, f"Country: Arab Republic of Egypt   |   Tel: {bank_phone}   |   SWIFT BIC: {bank_swift}")

    c.setStrokeColor(colors.HexColor("#0b2d5c"))
    c.setLineWidth(1.5)
    c.line(42, y - 36, width - 42, y - 36)

    # Instrument Title
    y -= 62
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 13.5)
    c.drawCentredString(width / 2, y, "IRREVOCABLE PERFORMANCE LETTER OF GUARANTEE (FINAL GUARANTEE)")

    # Structured Metadata Table Box (Crystal-clear for both humans & Gemini Vision)
    y -= 18
    box_top = y
    box_height = 172
    c.setFillColor(colors.HexColor("#f8fafc"))
    c.setStrokeColor(colors.HexColor("#cbd5e1"))
    c.setLineWidth(0.8)
    c.roundRect(42, box_top - box_height, width - 84, box_height, 6, fill=1, stroke=1)

    rows = [
        ("Guarantee Number (LG No.):", lg_number),
        ("Issuing Bank:", f"{bank_name} (Egypt)"),
        ("Beneficiary Corporate (To):", beneficiary_name),
        ("Beneficiary Address:", beneficiary_address),
        ("Applicant / Issuer (Ordered By):", "Red Sea Electromechanical & Civil Works S.A.E."),
        ("Applicant Commercial Reg. (ID):", "CR-774210"),
        ("Guarantee Type:", "Performance Guarantee (Final Letter of Guarantee)"),
        ("Currency & Guarantee Amount:", "EGP 3,750,000.00 (Three Million Seven Hundred Fifty Thousand Egyptian Pounds)"),
        ("Issuance Date:", issue_dt.strftime("%Y-%m-%d")),
        ("Expiry Date:", f"{expiry_dt.strftime('%Y-%m-%d')} (Fixed Expiry — Non-Auto-Renewing)"),
        ("Governing Rules:", "ICC Uniform Rules for Demand Guarantees (URDG 758)"),
    ]

    row_y = box_top - 15
    for label, val in rows:
        c.setFillColor(colors.HexColor("#475569"))
        c.setFont("Helvetica-Bold", 8.8)
        c.drawString(52, row_y, label)
        c.setFillColor(colors.HexColor("#0f172a"))
        c.setFont("Helvetica-Bold" if "Beneficiary Corporate" in label or "Guarantee Number" in label else "Helvetica", 9.0)
        c.drawString(215, row_y, str(val)[:78])
        row_y -= 14.8

    # Body Text
    y = box_top - box_height - 26
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 10)
    c.drawString(42, y, f"Messrs. {beneficiary_name}")
    y -= 16
    c.setFont("Helvetica", 9.3)

    paragraphs = [
        (
            f"Dear Sirs, At the request and for the account of our client, Red Sea Electromechanical & Civil Works S.A.E. "
            f"(Commercial Registration No. CR-774210) (hereinafter called the 'Applicant'), we, {bank_name}, "
            f"{bank_address}, hereby issue in your favor ({beneficiary_name}) our Irrevocable Performance Letter of Guarantee "
            f"No. {lg_number} for a maximum aggregate amount of EGP 3,750,000.00 (Three Million Seven Hundred Fifty Thousand "
            f"Egyptian Pounds Only)."
        ),
        (
            "PURPOSE / UNDERLYING CONTRACT: This Guarantee constitutes the 10% Contractual Performance Security for the "
            "Supply, Installation, Testing, and Commissioning of the Industrial Power & Utilities Substation Package "
            "under Contract Ref. EPC-2026-904."
        ),
        (
            "PAYMENT UNDERTAKING & MANDATORY CLAIM STATEMENT: We hereby irrevocably and unconditionally undertake to pay "
            "you upon your first written demand any sum or sums not exceeding in aggregate EGP 3,750,000.00, without cavil "
            "or argument and without needing to prove or show grounds for your demand, PROVIDED ALWAYS that any demand for "
            "payment under this Guarantee must explicitly state and be accompanied by the Beneficiary's signed written "
            "statement declaring that the Applicant is in breach of its contractual obligations under Contract Ref. EPC-2026-904."
        ),
        (
            f"VALIDITY & EXPIRY: This Guarantee enters into force on its Issuance Date ({issue_dt.strftime('%Y-%m-%d')}) "
            f"and shall remain valid in full force and effect until {expiry_dt.strftime('%Y-%m-%d')} ('Expiry Date') at the "
            f"close of banking hours at our Cairo Trade Finance Counters. Any claim hereunder must be received by us in writing "
            f"on or before {expiry_dt.strftime('%Y-%m-%d')}, after which date this Guarantee shall automatically become null "
            f"and void whether returned to us or not. This Guarantee is NOT subject to automatic renewal."
        ),
        (
            "APPLICABLE RULES: Except as otherwise expressly stated herein, this Guarantee is subject to the Uniform Rules "
            "for Demand Guarantees (URDG), 2010 Revision, ICC Publication No. 758 (URDG 758)."
        ),
    ]

    from reportlab.lib.utils import simpleSplit

    for para in paragraphs:
        lines = simpleSplit(para, "Helvetica", 9.2, width - 84)
        for ln in lines:
            c.drawString(42, y, ln)
            y -= 12.8
        y -= 7

    # Authorized Signatories Footer
    y -= 10
    c.setStrokeColor(colors.HexColor("#cbd5e1"))
    c.line(42, y, 220, y)
    c.line(width - 220, y, width - 42, y)
    c.setFont("Helvetica-Bold", 8.8)
    c.drawString(42, y - 13, f"For & On Behalf of {bank_name}")
    c.drawString(width - 220, y - 13, "Authorized Trade Finance Signatory B")
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.HexColor("#64748b"))
    c.drawString(42, y - 24, "Authorized Signatory A — Trade Operations")
    c.drawString(width - 220, y - 24, f"Official Stamp · Ref: {lg_number}")

    # Bottom disclaimer
    c.setFillColor(colors.HexColor("#64748b"))
    c.setFont("Helvetica-Oblique", 7.8)
    c.drawCentredString(
        width / 2,
        26,
        "Grow Business Development — Simulated Evaluation Instrument generated dynamically for your Guided Trial Sandbox. Not a real bank obligation.",
    )

    c.showPage()
    c.save()
    return buf.getvalue()


@router.get("/sample-custody-lg.pdf")
def download_personalized_sample_custody_lg(
    current_user: TokenData = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Response:
    """
    Generates a personalized Emirates NBD Egypt Performance LG PDF addressed to
    the current trial tenant's exact Entity #1 name (`beneficiary_corporate_id` auto-matches!)
    with a unique non-colliding `lg_number` per download.
    """
    cust_id = _require_trial_access(db, current_user)
    ensure_trial_sandbox_seeded(db, cust_id)

    from app.models.models import Bank, Customer, CustomerEntity, LGRecord

    customer = db.query(Customer).filter(Customer.id == cust_id).first()
    entity = (
        db.query(CustomerEntity)
        .filter(CustomerEntity.customer_id == cust_id, CustomerEntity.is_deleted == False)
        .order_by(CustomerEntity.id.asc())
        .first()
    )
    beneficiary_name = (
        entity.entity_name
        if entity and (entity.entity_name or "").strip()
        else (customer.name if customer else "Trial Operating Company")
    )
    beneficiary_address = (
        entity.address
        if entity and (entity.address or "").strip()
        else (customer.address if customer and customer.address else "Fifth Settlement, New Cairo, Egypt")
    )

    enbd = (
        db.query(Bank)
        .filter(Bank.is_deleted == False, Bank.short_name == "ENBD")
        .first()
        or db.query(Bank)
        .filter(Bank.is_deleted == False, Bank.name.ilike("%Emirates NBD%"))
        .first()
    )
    bank_name = enbd.name if enbd else "Emirates NBD Egypt"
    bank_address = (
        enbd.address
        if enbd and (enbd.address or "").strip()
        else "Plot 85, South Teseen St., Fifth Settlement, New Cairo, Egypt"
    )
    bank_phone = enbd.phone_number if enbd and (enbd.phone_number or "").strip() else "16664"
    bank_swift = enbd.swift_code if enbd and (enbd.swift_code or "").strip() else "EBONEECA"

    # Pick a clean, unused LG number so repeated test scans never collide on `lg_records.lg_number`
    seq = 1
    while seq < 99:
        candidate_no = f"ENBD-LG-2026-{cust_id:03d}-{seq:02d}"
        exists = db.query(LGRecord.id).filter(LGRecord.lg_number == candidate_no).first()
        if not exists:
            break
        seq += 1
    lg_number = f"ENBD-LG-2026-{cust_id:03d}-{seq:02d}"

    pdf_bytes = _generate_personalized_custody_lg_pdf(
        beneficiary_name=beneficiary_name,
        beneficiary_address=beneficiary_address,
        lg_number=lg_number,
        bank_name=bank_name,
        bank_address=bank_address,
        bank_phone=bank_phone,
        bank_swift=bank_swift,
    )

    safe_slug = re.sub(r"[^A-Za-z0-9]+", "_", beneficiary_name).strip("_")[:28] or "Company"
    filename = f"Sample_Performance_LG_{safe_slug}_{seq:02d}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


@router.post("/switch-persona")
async def switch_trial_persona(
    payload: SwitchPersonaIn,
    current_user: TokenData = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    1-Click solo evaluator Persona Switcher (`Maker <-> Checker <-> Corporate Admin`).
    Strictly confined to Trial Sandbox accounts and strictly within `current_user.customer_id`.
    """
    cust_id = _require_trial_access(db, current_user)

    target_role_clean = (payload.target_role or "").strip().lower()
    role_map = {
        "end_user": UserRole.END_USER,
        "corporate_admin": UserRole.CORPORATE_ADMIN,
    }
    if target_role_clean not in role_map:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid target persona role.")

    ensure_trial_sandbox_seeded(db, cust_id)

    from app.auth_v2.services import AuthService
    from app.models.models import User

    target_user = (
        db.query(User)
        .filter(
            User.customer_id == cust_id,
            User.role == role_map[target_role_clean],
            User.is_deleted == False,
        )
        .order_by(User.id.asc())
        .first()
    )
    if not target_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target trial persona not found.")

    token_data = await AuthService().build_user_token_data(db, target_user)
    new_jwt = create_access_token(data=token_data)

    return {
        "access_token": new_jwt,
        "role": target_role_clean,
        "email": target_user.email,
        "user_id": target_user.id,
    }


@router.post("/rfq/{rfq_id}/simulate-bids")
def trigger_simulated_rfq_bids(
    rfq_id: str,
    request: Request,
    current_user: TokenData = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Populates encrypted quotes from Simulated Banks #2 & #3 and returns Bank #1's Dealer Playground link."""
    cust_id = _require_trial_access(db, current_user)
    from app.models.models_quotation import QuotationRequest

    rfq = (
        db.query(QuotationRequest)
        .filter(
            QuotationRequest.id == rfq_id,
            QuotationRequest.customer_id == cust_id,
            QuotationRequest.is_deleted == False,
        )
        .first()
    )
    if not rfq:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RFQ not found.")

    added = _populate_simulated_bank_quotes(db, rfq, include_first_bank_if_empty=False)
    db.commit()
    sim = _build_rfq_simulator_summary(db, cust_id, request)
    return {"status": "ok", "quotes_added": added, "simulator": sim}


@router.post("/rfq/{rfq_id}/fast-forward")
def fast_forward_trial_rfq_to_unseal(
    rfq_id: str,
    request: Request,
    current_user: TokenData = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Ends the active RFQ bidding window immediately (`00:00`), ensures all 3 banks have sealed
    quotes so multi-leg cherry-picking shines, and transitions the RFQ to `EVALUATING` with an
    8-minute acceptance window.
    """
    cust_id = _require_trial_access(db, current_user)
    from app.models.models_quotation import QuotationRequest

    rfq = (
        db.query(QuotationRequest)
        .filter(
            QuotationRequest.id == rfq_id,
            QuotationRequest.customer_id == current_user.customer_id,
            QuotationRequest.is_deleted == False,
        )
        .first()
    )
    if not rfq:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RFQ not found.")

    if rfq.status in ("COMPLETED", "CANCELLED", "REJECTED"):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"RFQ is already {rfq.status}.")

    now_utc = datetime.now(timezone.utc)
    # Ensure all 3 banks (including Bank #1 if the user didn't submit manually) have competitive quotes
    _populate_simulated_bank_quotes(db, rfq, include_first_bank_if_empty=True)

    if not rfq.window_start or rfq.window_start > now_utc:
        rfq.window_start = now_utc - timedelta(minutes=5)
    rfq.window_end = now_utc - timedelta(seconds=2)
    rfq.is_dispatched = True
    rfq.dispatched_at = rfq.dispatched_at or (now_utc - timedelta(minutes=5))
    rfq.status = "EVALUATING"
    rfq.acceptance_status = "PENDING"
    rfq.acceptance_deadline = now_utc + timedelta(minutes=8)

    for leg in getattr(rfq, "legs", None) or []:
        if leg.status in ("PENDING", "OPEN"):
            leg.status = "EVALUATING"

    if (getattr(rfq, "type", None) or "FX_SPOT").upper() == "FX_SPOT" and not rfq.market_benchmark_snapshot:
        try:
            from app.services.live_market_service import live_market_service
            live_market_service.snapshot_rfq_market_benchmarks(db, rfq)
        except Exception:
            pass

    db.commit()
    sim = _build_rfq_simulator_summary(db, int(current_user.customer_id), request)
    return {
        "status": "ok",
        "message": "Bidding window closed (00:00) and AES-256 quotes unsealed for All-In Net evaluation!",
        "rfq_id": rfq.id,
        "simulator": sim,
    }


class InviteColleagueIn(BaseModel):
    email: str
    name: Optional[str] = None
    role: Optional[str] = "end_user"


@router.post("/purge-uploaded-data")
def purge_trial_uploaded_test_data(
    current_user: TokenData = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Self-serve privacy control: deletes all user-uploaded test records & files in the current
    Free Trial workspace while keeping the pre-loaded starter sample records intact.
    """
    cust_id = _require_trial_access(db, current_user)
    from app.crud.crud import log_action
    from app.models.models import Customer, LGDocument, LGInstruction, LGRecord
    from app.models.models_issuance import (
        IssuanceExposureEntry,
        IssuanceRequest,
        IssuanceRequestDocument,
        IssuedLGRecord,
    )

    deleted_counts = {"lgs": 0, "issuance_requests": 0, "statements": 0, "rfqs": 0}

    # 1. User-created Custody LGs & Documents (preserve `migration_source == 'TRIAL_SAMPLE'`)
    user_lgs = (
        db.query(LGRecord)
        .filter(
            LGRecord.customer_id == cust_id,
            LGRecord.is_deleted == False,
            (LGRecord.migration_source.is_(None)) | (LGRecord.migration_source != "TRIAL_SAMPLE"),
        )
        .all()
    )
    for lg in user_lgs:
        db.query(LGDocument).filter(LGDocument.lg_record_id == lg.id).update(
            {"is_deleted": True}, synchronize_session=False
        )
        db.query(LGInstruction).filter(LGInstruction.lg_record_id == lg.id).update(
            {"is_deleted": True}, synchronize_session=False
        )
        # Free unique `lg_number` so they can re-scan the same real PDF again if desired
        lg.lg_number = f"{lg.lg_number}__PURGED_{uuid.uuid4().hex[:6]}"[:64]
        lg.is_deleted = True
        db.add(lg)
        deleted_counts["lgs"] += 1

    # 2. User-created Issuance Requests, Documents & Issued LGs (preserve `REQ-SMPL-` & `ILG-SMPL-`)
    user_reqs = (
        db.query(IssuanceRequest)
        .filter(
            IssuanceRequest.customer_id == cust_id,
            ~IssuanceRequest.serial_number.like("REQ-SMPL-%"),
        )
        .all()
    )
    for ireq in user_reqs:
        db.query(IssuanceRequestDocument).filter(
            IssuanceRequestDocument.request_id == ireq.id
        ).update({"is_deleted": True}, synchronize_session=False)
        db.delete(ireq)
        deleted_counts["issuance_requests"] += 1

    user_ilgs = (
        db.query(IssuedLGRecord)
        .filter(
            IssuedLGRecord.customer_id == cust_id,
            ~IssuedLGRecord.lg_ref_number.like("ILG-SMPL-%"),
        )
        .all()
    )
    for ilg in user_ilgs:
        db.query(IssuanceExposureEntry).filter(
            IssuanceExposureEntry.issued_lg_id == ilg.id
        ).update({"is_active": False}, synchronize_session=False)
        db.delete(ilg)

    # Reset starter REQ-SMPL if it was locked/issued during testing
    smpl_req = (
        db.query(IssuanceRequest)
        .filter(
            IssuanceRequest.customer_id == cust_id,
            IssuanceRequest.serial_number.like("REQ-SMPL-%"),
        )
        .first()
    )
    if smpl_req:
        smpl_req.status = "APPROVED_INTERNAL"
        smpl_req.locked_for_issuance = False
        smpl_req.lg_record_id = None
        smpl_req.metadata_json = {}
        db.add(smpl_req)

    # 3. Bank Statements, Reconciliation Matches, Transactions & ERP Records
    try:
        from app.models.models_reconciliation_v2 import (
            BankStatement,
            BankTransaction,
            InternalLedgerRecord,
            ReconciliationMatch,
        )

        stmt_ids = [
            row[0]
            for row in db.query(BankStatement.id)
            .filter(BankStatement.company_id == cust_id)
            .all()
        ]
        if stmt_ids:
            txn_ids = [
                row[0]
                for row in db.query(BankTransaction.id)
                .filter(BankTransaction.statement_id.in_(stmt_ids))
                .all()
            ]
            if txn_ids:
                db.query(ReconciliationMatch).filter(
                    ReconciliationMatch.bank_txn_id.in_(txn_ids)
                ).delete(synchronize_session=False)
                db.query(BankTransaction).filter(
                    BankTransaction.id.in_(txn_ids)
                ).update({"is_deleted": True}, synchronize_session=False)

        db.query(InternalLedgerRecord).filter(
            InternalLedgerRecord.company_id == cust_id
        ).delete(synchronize_session=False)

        stmts = (
            db.query(BankStatement)
            .filter(BankStatement.company_id == cust_id, BankStatement.is_deleted == False)
            .all()
        )
        for st in stmts:
            st.is_deleted = True
            db.add(st)
            deleted_counts["statements"] += 1
    except Exception:
        pass

    # 4. Quotation RFQs
    try:
        from app.models.models_quotation import QuotationRequest

        rfqs = (
            db.query(QuotationRequest)
            .filter(QuotationRequest.customer_id == cust_id, QuotationRequest.is_deleted == False)
            .all()
        )
        for r_obj in rfqs:
            r_obj.is_deleted = True
            db.add(r_obj)
            deleted_counts["rfqs"] += 1
    except Exception:
        pass

    cust = db.query(Customer).filter(Customer.id == cust_id).first()
    if cust:
        cust.active_lg_count = 2
        cust.active_issuance_lg_count = 1
        db.add(cust)

    log_action(
        db,
        user_id=current_user.user_id,
        action_type="TRIAL_UPLOADED_DATA_PURGED",
        entity_type="Customer",
        entity_id=cust_id,
        details=deleted_counts,
        customer_id=cust_id,
    )
    db.commit()

    total_removed = sum(deleted_counts.values())
    return {
        "status": "ok",
        "deleted": deleted_counts,
        "total_removed": total_removed,
        "message": (
            f"Deleted {total_removed} uploaded test item(s). Pre-loaded sample data remains ready for testing."
            if total_removed > 0
            else "No uploaded test files found — only pre-loaded sample records are present."
        ),
    }


@router.post("/invite-colleague")
async def invite_trial_colleague(
    payload: InviteColleagueIn,
    request: Request,
    current_user: TokenData = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Allows a Free Trial evaluator to invite up to 3 colleagues from their exact same
    corporate email domain (e.g. Manager / CFO) with a secure Zero-Touch Activation link.
    """
    cust_id = _require_trial_access(db, current_user)
    import secrets
    from app.constants import LegalArtifactType
    from app.core.hashing import get_password_hash
    from app.crud.crud import crud_legal_artifact, log_action
    from app.models.models import Customer, User
    from app.services.customer_onboarding_service import send_corporate_admin_activation_email

    clean_email = (payload.email or "").strip().lower()
    if "@" not in clean_email or "." not in clean_email.split("@")[-1]:
        raise HTTPException(status_code=400, detail="Please enter a valid work email address.")

    cust = db.query(Customer).filter(Customer.id == cust_id).first()
    if not cust:
        raise HTTPException(status_code=404, detail="Trial account not found.")

    base_email = (cust.contact_email or current_user.email or "").strip().lower()
    expected_domain = base_email.rsplit("@", 1)[1] if "@" in base_email else ""
    invite_domain = clean_email.rsplit("@", 1)[1]
    if not expected_domain or invite_domain != expected_domain:
        raise HTTPException(
            status_code=400,
            detail=f"For security, invited colleagues must share your company email domain (@{expected_domain}).",
        )

    existing_user = db.query(User).filter(User.email.ilike(clean_email), User.is_deleted == False).first()
    if existing_user:
        if existing_user.customer_id == cust_id:
            raise HTTPException(status_code=409, detail="This colleague already has access to your trial.")
        raise HTTPException(status_code=409, detail="This email is already registered on Grow.")

    real_users_count = (
        db.query(User)
        .filter(
            User.customer_id == cust_id,
            User.is_deleted == False,
            ~User.email.ilike("%@sandbox.growbusinessdevelopment.com"),
        )
        .count()
    )
    if real_users_count >= 4:
        raise HTTPException(status_code=400, detail="You can invite up to 3 colleagues during the Free Trial.")

    if cust.subscription_plan and (cust.subscription_plan.max_users or 0) < real_users_count + 3:
        cust.subscription_plan.max_users = real_users_count + 3
        db.add(cust.subscription_plan)

    latest_tc = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.TERMS_AND_CONDITIONS)
    latest_pp = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.PRIVACY_POLICY)
    legal_ver = max(
        float(latest_tc.version) if latest_tc and latest_tc.version else 1.1,
        float(latest_pp.version) if latest_pp and latest_pp.version else 1.1,
    )

    target_role = (
        UserRole.CORPORATE_ADMIN
        if (payload.role or "").strip().lower() == "corporate_admin"
        else UserRole.END_USER
    )
    new_colleague = User(
        email=clean_email,
        password_hash=get_password_hash(secrets.token_urlsafe(24) + "Aa1!"),
        role=target_role,
        customer_id=cust_id,
        must_change_password=False,
        has_all_entity_access=True,
        last_accepted_legal_version=legal_ver,
    )
    db.add(new_colleague)
    db.flush()

    cust.active_user_count = (
        db.query(User).filter(User.customer_id == cust_id, User.is_deleted == False).count()
    )
    db.add(cust)
    db.commit()
    db.refresh(new_colleague)

    await send_corporate_admin_activation_email(
        db=db,
        user=new_colleague,
        customer_name=cust.name,
        request=request,
        is_trial=True,
        recipient_name=(payload.name or "").strip() or None,
    )

    log_action(
        db,
        user_id=current_user.user_id,
        action_type="TRIAL_COLLEAGUE_INVITED",
        entity_type="User",
        entity_id=new_colleague.id,
        details={"invited_email": clean_email, "role": target_role.value},
        customer_id=cust_id,
    )

    return {
        "status": "ok",
        "email": clean_email,
        "message": f"Activation link sent to {clean_email}!",
    }


@router.get("/value-summary.pdf")
def download_trial_management_value_summary_pdf(
    current_user: TokenData = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Response:
    """
    Generates a clear, 1-page Management & CFO Business Case Summary PDF tailored to the
    trial company's name and evaluated modules so evaluators can justify adoption internally.
    """
    cust_id = _require_trial_access(db, current_user)
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.utils import simpleSplit
    from reportlab.pdfgen import canvas
    from app.models.models import Customer

    cust = db.query(Customer).filter(Customer.id == cust_id).first()
    company_name = cust.name if cust else (current_user.customer_name or "Corporate Treasury Team")
    snap = get_trial_usage_snapshot(db, cust_id)
    steps = snap.get("steps_completed") or {}
    team_total = int(snap.get("invited_colleagues_count") or 0) + 1

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    # Header Banner
    c.setFillColor(colors.HexColor("#0f172a"))
    c.rect(0, height - 92, width, 92, fill=1, stroke=0)
    c.setFillColor(colors.HexColor("#6ee7b7"))
    c.setFont("Helvetica-Bold", 9)
    c.drawString(42, height - 30, "GROW BUSINESS DEVELOPMENT · EXECUTIVE EVALUATION BRIEF")
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(42, height - 54, f"Treasury Modernization Summary — {company_name[:42]}")
    c.setFillColor(colors.HexColor("#cbd5e1"))
    c.setFont("Helvetica", 9.5)
    c.drawString(
        42,
        height - 73,
        f"Prepared on {date.today().strftime('%d %b %Y')}   |   Active Evaluators: {team_total}   |   Ready for Management & CFO Review",
    )

    y = height - 118

    # Section 1: Executive Summary
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 11.5)
    c.drawString(42, y, "1. Why Treasury Teams Move from Spreadsheets & Manual Emails to Grow")
    y -= 15
    c.setFillColor(colors.HexColor("#334155"))
    c.setFont("Helvetica", 9.3)
    intro_lines = simpleSplit(
        f"During {company_name}'s evaluation on Grow, workflows that normally require manual data entry, spreadsheet "
        "tracking, Word templates, and scattered bank emails were consolidated into one controlled workspace — where AI "
        "prepares the heavy lifting in seconds and authorized staff review and approve every action before execution.",
        "Helvetica",
        9.3,
        width - 84,
    )
    for ln in intro_lines:
        c.drawString(42, y, ln)
        y -= 13

    # Section 2: 4 Business Value Pillars
    y -= 10
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 11.5)
    c.drawString(42, y, "2. Operational Time Saved, Risk Reduction & Financial Impact")
    y -= 14

    pillars = [
        (
            "LG Custody & Zero Missed Renewals",
            "Tested" if steps.get("custody") else "Included",
            "AI reads incoming bank LG PDFs into structured fields for review (< 60 sec vs. 20 min manual typing). "
            "Automated 30-day reminders to project owners + manager escalation prevent costly surprise expiries or claims.",
        ),
        (
            "LG Issuance, Wording Protection & Official Bank PDFs",
            "Tested" if steps.get("issuance") else "Included",
            "Screens client LG drafts to flag risky clauses before signing, compares banks on commission & cash margin "
            "(e.g., 0% vs. 10% margin frees EGP 600K cash on an EGP 6M LG), and fills official bank PDF forms cleanly.",
        ),
        (
            "Self-Learning Bank & ERP Reconciliation",
            "Tested" if steps.get("reconciliation") else "Included",
            "Verifies balances with zero variance, matches InstaPay, SWIFT fees & sweeps, and learns your counterparties, "
            "fee tolerances, and GL codes from every confirmation so manual work shrinks each month.",
        ),
        (
            "Multi-Bank FX & T-Bill Rate Competition",
            "Tested" if steps.get("quotations") else "Included",
            "Requests simultaneous closed-timer quotes from relationship banks benchmarked against live interbank market "
            "feeds, historical spreads, and CBE reference rates for clear best-execution proof.",
        ),
    ]

    for title_txt, badge_txt, desc_txt in pillars:
        box_h = 54
        c.setFillColor(colors.HexColor("#f8fafc"))
        c.setStrokeColor(colors.HexColor("#cbd5e1"))
        c.roundRect(42, y - box_h, width - 84, box_h, 6, fill=1, stroke=1)

        c.setFillColor(colors.HexColor("#0f172a"))
        c.setFont("Helvetica-Bold", 9.8)
        c.drawString(52, y - 15, title_txt)

        is_tested = badge_txt == "Tested"
        c.setFillColor(colors.HexColor("#dcfce7" if is_tested else "#e2e8f0"))
        c.roundRect(width - 112, y - 19, 60, 14, 4, fill=1, stroke=0)
        c.setFillColor(colors.HexColor("#166534" if is_tested else "#475569"))
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(width - 82, y - 14.5, badge_txt.upper())

        c.setFillColor(colors.HexColor("#334155"))
        c.setFont("Helvetica", 8.6)
        d_lines = simpleSplit(desc_txt, "Helvetica", 8.6, width - 104)
        dy = y - 29
        for dl in d_lines[:2]:
            c.drawString(52, dy, dl)
            dy -= 11.5
        y -= box_h + 8

    # Section 3: Security, Governance & Implementation
    y -= 6
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 11.5)
    c.drawString(42, y, "3. Security, Internal Controls & Fast Implementation (No Heavy IT Project)")
    y -= 15

    gov_bullets = [
        "Human Control First: AI prepares data and highlights anomalies; your team reviews and managers approve.",
        "Enterprise Data Security: Per-company AES-256 encryption, strict tenant isolation, and zero public AI training.",
        "Audit-Ready Governance: Maker-Checker dual control, immutable audit logs, and QR-verified bank letters.",
        "48-Hour Go-Live: Cloud-ready with zero ERP integration required on Day 1 (CSV/Excel & PDF ready out of the box).",
    ]
    c.setFont("Helvetica", 9.0)
    c.setFillColor(colors.HexColor("#1e293b"))
    for b_line in gov_bullets:
        c.drawString(50, y, f"•  {b_line}")
        y -= 14

    # Footer Callout
    c.setFillColor(colors.HexColor("#eff6ff"))
    c.setStrokeColor(colors.HexColor("#bfdbfe"))
    c.roundRect(42, 38, width - 84, 42, 6, fill=1, stroke=1)
    c.setFillColor(colors.HexColor("#1e3a8a"))
    c.setFont("Helvetica-Bold", 9.2)
    c.drawString(54, 62, "Ready to review implementation, security, or subscription pricing for your team?")
    c.setFont("Helvetica", 8.8)
    c.drawString(54, 48, "Contact Grow Business Development: info@growbusinessdevelopment.com  |  WhatsApp: +20 120 055 7551")

    c.showPage()
    c.save()

    safe_slug = re.sub(r"[^A-Za-z0-9]+", "_", company_name).strip("_")[:28] or "Company"
    fname = f"Grow_Management_Value_Summary_{safe_slug}.pdf"
    return Response(
        content=buf.getvalue(),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Cache-Control": "no-store",
        },
    )
