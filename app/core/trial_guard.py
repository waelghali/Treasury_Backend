# app/core/trial_guard.py
"""
Centralized Trial Sandbox Guard, Watermarking, Email Shield & Quota Engine.

ZERO-BLAST-RADIUS INVARIANT FOR PAYING CUSTOMERS:
- Every commercial customer (on any non-trial paid `SubscriptionPlan` when `TRIAL_MODE=false`)
  resolves `is_trial_customer() == False` and `is_active_trial_context() == False`:
  * Zero UI changes (`GET /trial-sandbox/status` -> `{"trial_mode": false}`).
  * Zero document watermarks, zero outbound email filtering, zero sandbox quotas.
- Every Trial Sandbox customer (assigned a `"Guided Trial Sandbox"` / `"Free Trial"` plan via
  `approve_trial_registration`, OR running inside an isolated `TRIAL_MODE=true` instance)
  automatically receives:
  * 14-day evaluation duration & few-uses quotas,
  * Day-1 pre-seeded entities, facilities, FX desks & `Maker <-> Checker <-> CFO` switcher,
  * Diagonal `TRIAL SANDBOX — EVALUATION ONLY` watermarks on HTML/PDF bank documents,
  * Outbound bank email suppression, and
  * Strict quarantine from all 5 cross-customer intelligence/benchmark engines.
"""

import contextvars
import io
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

logger = logging.getLogger("app.trial_guard")

TRIAL_WATERMARK_HEADLINE = "TRIAL SANDBOX — EVALUATION ONLY"
TRIAL_WATERMARK_SUBLINE = "NOT VALID FOR OFFICIAL BANK SUBMISSION"

# Per-request async/thread-safe context populated at HTTP request start (`init_request_trial_state`)
# and mutated in-place inside `get_current_user` (which FastAPI runs in `anyio.to_thread.run_sync`,
# where `ContextVar.set()` alone would stay confined to the worker thread's copied context!).
_request_trial_state_var: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar(
    "grow_request_trial_state", default=None
)
_request_is_trial_var: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "grow_request_is_trial", default=False
)
_request_trial_customer_id_var: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar(
    "grow_request_trial_customer_id", default=None
)


def init_request_trial_state() -> Dict[str, Any]:
    """
    Creates a fresh mutable dict bound to the current asyncio request task BEFORE FastAPI
    executes sync dependencies in worker threads, so in-place mutations inside `get_current_user`
    propagate back to `async def` endpoints, `asyncio.to_thread` PDF workers, and forked workers
    with zero cross-request leakage.
    """
    state: Dict[str, Any] = {"is_trial": False, "customer_id": None}
    _request_trial_state_var.set(state)
    _request_is_trial_var.set(False)
    _request_trial_customer_id_var.set(None)
    return state


class TrialRequestStateMiddleware:
    """
    Zero-overhead pure ASGI middleware that initializes `_request_trial_state_var` on the
    main asyncio event loop for each HTTP request so sync `Depends(get_current_user)` mutations
    survive `anyio.to_thread.run_sync` context unwinding.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            init_request_trial_state()
        await self.app(scope, receive, send)


def is_trial_mode() -> bool:
    """Returns True when the whole server instance is dedicated to Trial Mode (`TRIAL_MODE=true`)."""
    return os.getenv("TRIAL_MODE", "false").strip().lower() in ("true", "1", "yes", "on")


def is_trial_plan_name(plan_name: Optional[str]) -> bool:
    """Returns True strictly for Trial / Sandbox subscription plan names."""
    if not plan_name:
        return False
    lower = str(plan_name).strip().lower()
    return "trial" in lower or "sandbox" in lower


def set_request_trial_context(is_trial: bool, customer_id: Optional[int] = None) -> None:
    """Sets per-request trial state so downstream PDF/HTML/email helpers know whether to watermark/filter."""
    clean_flag = bool(is_trial)
    clean_cid = int(customer_id) if customer_id else None
    state = _request_trial_state_var.get()
    if isinstance(state, dict):
        state["is_trial"] = clean_flag
        state["customer_id"] = clean_cid
    _request_is_trial_var.set(clean_flag)
    _request_trial_customer_id_var.set(clean_cid)


def get_active_trial_customer_id() -> Optional[int]:
    state = _request_trial_state_var.get()
    if isinstance(state, dict) and state.get("customer_id"):
        return int(state["customer_id"])
    return _request_trial_customer_id_var.get()


def is_active_trial_context() -> bool:
    """True when either `TRIAL_MODE=true` globally or the active request belongs to a Trial customer."""
    if is_trial_mode():
        return True
    state = _request_trial_state_var.get()
    if isinstance(state, dict) and state.get("is_trial"):
        return True
    return bool(_request_is_trial_var.get())


def get_trial_caps() -> Dict[str, int]:
    """Configurable 'few-uses' evaluation caps for Trial Sandbox tenants."""
    max_lgs = int(os.getenv("TRIAL_MAX_LGS", os.getenv("TRIAL_MAX_LG_RECORDS", "8")))
    return {
        "max_lgs": max_lgs,
        "max_lg_records": max_lgs,
        "max_statements": int(os.getenv("TRIAL_MAX_STATEMENTS", "3")),
        "max_rfqs": int(os.getenv("TRIAL_MAX_RFQS", "5")),
        "max_ai_calls": int(os.getenv("TRIAL_MAX_AI_CALLS", "15")),
        "duration_days": int(os.getenv("TRIAL_DURATION_DAYS", "14")),
    }


def is_trial_customer(db: Optional[Session], customer_id: Optional[int]) -> bool:
    """
    Returns True if:
    1. `TRIAL_MODE=true` globally, OR
    2. `customer_id`'s current `SubscriptionPlan` is a Trial / Sandbox plan, OR
    3. `customer_id` originated from `TrialRegistration` AND is still on a $0 plan
       (if upgraded to a paid commercial plan, returns False so they graduate cleanly).
    """
    if is_trial_mode():
        return True
    if not customer_id:
        return False
    if is_active_trial_context() and get_active_trial_customer_id() == int(customer_id):
        return True
    if not db:
        return False

    try:
        from app.models.models import Customer, SubscriptionPlan, TrialRegistration

        row = (
            db.query(SubscriptionPlan.name, SubscriptionPlan.monthly_price)
            .join(Customer, Customer.subscription_plan_id == SubscriptionPlan.id)
            .filter(Customer.id == int(customer_id))
            .first()
        )
        if row:
            plan_name, monthly_price = row[0], row[1]
            if is_trial_plan_name(plan_name):
                return True
            if float(monthly_price or 0) == 0.0:
                from_trial_reg = (
                    db.query(TrialRegistration.id)
                    .filter(TrialRegistration.customer_id == int(customer_id))
                    .first()
                )
                if from_trial_reg:
                    return True
    except Exception as exc:
        logger.debug(f"is_trial_customer lookup skipped: {exc}")
    return False


def ensure_trial_activated_clock(
    db: Session,
    customer_id: Optional[int],
    user_id: Optional[int] = None,
    trigger: str = "login",
) -> Optional[datetime]:
    """
    Starts the 14-day trial countdown strictly upon FIRST user activation / login
    rather than at System Owner approval time, so prospects never lose evaluation days
    before setting their password. Idempotent (runs once per trial customer).
    """
    if not customer_id or not is_trial_customer(db, customer_id):
        return None

    try:
        from app.crud.crud import log_action
        from app.models.models import AuditLog, Customer

        existing_act = (
            db.query(AuditLog)
            .filter(
                AuditLog.customer_id == int(customer_id),
                AuditLog.action_type == "TRIAL_SANDBOX_ACTIVATED",
            )
            .order_by(AuditLog.id.asc())
            .first()
        )
        if existing_act:
            return existing_act.timestamp

        cust = db.query(Customer).filter(Customer.id == int(customer_id)).first()
        if not cust:
            return None

        duration_days = get_trial_caps()["duration_days"]
        use_tz = bool(cust.end_date and cust.end_date.tzinfo)
        now_dt = datetime.now(timezone.utc) if use_tz else datetime.utcnow()

        # If not previously extended manually beyond 31 days, anchor fresh 14-day window from activation
        has_manual_ext = (
            db.query(AuditLog.id)
            .filter(
                AuditLog.customer_id == int(customer_id),
                AuditLog.action_type == "TRIAL_DURATION_EXTENDED",
            )
            .first()
            is not None
        )
        if not has_manual_ext:
            cust.start_date = now_dt
            cust.end_date = now_dt + timedelta(days=duration_days)
            db.add(cust)
            db.flush()

        log_action(
            db,
            user_id=user_id,
            action_type="TRIAL_SANDBOX_ACTIVATED",
            entity_type="Customer",
            entity_id=int(customer_id),
            details={"trigger": trigger, "duration_days": duration_days},
            customer_id=int(customer_id),
        )
        return now_dt
    except Exception as exc:
        logger.debug(f"ensure_trial_activated_clock skipped for customer {customer_id}: {exc}")
        return None


def _is_sample_filename(fname: Optional[str]) -> bool:
    if not fname:
        return True
    low = str(fname).strip().lower()
    sample_prefixes = (
        "enbd_sample_",
        "fncb_",
        "sample_",
        "apex_contract_",
        "lg_instruction_",
        "filled_",
    )
    return any(low.startswith(p) or f"_{p}" in low for p in sample_prefixes)


def _is_sample_lg_number(lg_no: Optional[str]) -> bool:
    if not lg_no:
        return True
    up = str(lg_no).strip().upper()
    return up.startswith(("ENBD-LG-2026-", "LG-SMPL-", "FNCB-", "SMPL-"))


def get_trial_usage_snapshot(db: Session, customer_id: int) -> Dict[str, Any]:
    """
    Computes live usage vs. caps, primary interest module, activation status,
    own-document uploads, invited colleagues, and 7-stage funnel milestone.
    """
    caps = get_trial_caps()
    custody_count = 0
    custody_engaged = False
    issued_count = 0
    issuance_engaged = False
    statement_count = 0
    rfq_count = 0
    own_documents_uploaded = False
    invited_colleagues: List[str] = []
    primary_module = "custody"
    is_activated = False
    activated_at_iso: Optional[str] = None
    end_date_iso: Optional[str] = None
    days_remaining: Optional[int] = caps["duration_days"]
    days_since_activation: int = 0

    try:
        from app.models.models import (
            AuditLog,
            Customer,
            LGDocument,
            LGInstruction,
            LGRecord,
            PasswordResetToken,
            TrialRegistration,
            User,
        )

        cust = db.query(Customer).filter(Customer.id == int(customer_id)).first()
        if cust and cust.end_date:
            end_dt = cust.end_date
            end_date_iso = end_dt.isoformat()
            now_dt = datetime.now(timezone.utc) if end_dt.tzinfo else datetime.now()
            delta_days = (end_dt - now_dt).days + (1 if (end_dt - now_dt).seconds > 0 else 0)
            days_remaining = max(0, delta_days)

        treg = (
            db.query(TrialRegistration)
            .filter(TrialRegistration.customer_id == int(customer_id))
            .order_by(TrialRegistration.id.desc())
            .first()
        )
        if treg and isinstance(treg.requested_modules, list) and treg.requested_modules:
            raw_first = str(treg.requested_modules[0] or "custody").strip().lower()
            if raw_first in ("quotation", "quotations"):
                primary_module = "quotations"
            elif raw_first in ("custody", "issuance", "reconciliation"):
                primary_module = raw_first

        act_log = (
            db.query(AuditLog)
            .filter(
                AuditLog.customer_id == int(customer_id),
                AuditLog.action_type == "TRIAL_SANDBOX_ACTIVATED",
            )
            .order_by(AuditLog.id.asc())
            .first()
        )
        if act_log and act_log.timestamp:
            is_activated = True
            activated_at_iso = act_log.timestamp.isoformat()
            ts = act_log.timestamp
            now_cmp = datetime.now(timezone.utc) if ts.tzinfo else datetime.utcnow()
            days_since_activation = max(0, (now_cmp - ts).days)
        else:
            used_tok = (
                db.query(PasswordResetToken)
                .join(User, PasswordResetToken.user_id == User.id)
                .filter(User.customer_id == int(customer_id), PasswordResetToken.is_used == True)
                .order_by(PasswordResetToken.id.asc())
                .first()
            )
            if used_tok:
                is_activated = True
                ts = used_tok.updated_at or used_tok.created_at
                if ts:
                    activated_at_iso = ts.isoformat()
                    now_cmp = datetime.now(timezone.utc) if ts.tzinfo else datetime.utcnow()
                    days_since_activation = max(0, (now_cmp - ts).days)

        real_users = (
            db.query(User)
            .filter(
                User.customer_id == int(customer_id),
                User.is_deleted == False,
                ~User.email.ilike("%@sandbox.growbusinessdevelopment.com"),
            )
            .order_by(User.id.asc())
            .all()
        )
        if len(real_users) > 1:
            invited_colleagues = [u.email for u in real_users[1:] if u.email]

        custody_count = (
            db.query(LGRecord)
            .filter(
                LGRecord.customer_id == customer_id,
                LGRecord.is_deleted == False,
                (LGRecord.migration_source.is_(None)) | (LGRecord.migration_source != "TRIAL_SAMPLE"),
            )
            .count()
        )
        inst_count = (
            db.query(LGInstruction.id)
            .join(LGRecord, LGInstruction.lg_record_id == LGRecord.id)
            .filter(LGRecord.customer_id == customer_id, LGInstruction.is_deleted == False)
            .count()
        )
        custody_engaged = custody_count > 0 or inst_count > 0

        user_lgs_meta = (
            db.query(LGRecord.lg_number)
            .filter(
                LGRecord.customer_id == customer_id,
                LGRecord.is_deleted == False,
                (LGRecord.migration_source.is_(None)) | (LGRecord.migration_source != "TRIAL_SAMPLE"),
            )
            .limit(30)
            .all()
        )
        if any(not _is_sample_lg_number(lg_no) for (lg_no,) in user_lgs_meta):
            own_documents_uploaded = True
    except Exception:
        pass

    try:
        from app.models.models_issuance import (
            IssuanceRequest,
            IssuanceRequestDocument,
            IssuedLGRecord,
        )

        req_count = (
            db.query(IssuanceRequest)
            .filter(
                IssuanceRequest.customer_id == customer_id,
                ~IssuanceRequest.serial_number.like("REQ-SMPL-%"),
            )
            .count()
        )
        ilg_count = (
            db.query(IssuedLGRecord)
            .filter(
                IssuedLGRecord.customer_id == customer_id,
                ~IssuedLGRecord.lg_ref_number.like("ILG-SMPL-%"),
            )
            .count()
        )
        sample_req_touched = (
            db.query(IssuanceRequest.id)
            .filter(
                IssuanceRequest.customer_id == customer_id,
                IssuanceRequest.serial_number.like("REQ-SMPL-%"),
                (IssuanceRequest.status != "APPROVED_INTERNAL")
                | (IssuanceRequest.locked_for_issuance == True),
            )
            .first()
            is not None
        )
        issued_count = max(req_count, ilg_count)
        issuance_engaged = issued_count > 0 or sample_req_touched

        if not own_documents_uploaded:
            ireq_docs = (
                db.query(IssuanceRequestDocument.file_name)
                .join(IssuanceRequest, IssuanceRequestDocument.request_id == IssuanceRequest.id)
                .filter(
                    IssuanceRequest.customer_id == customer_id,
                    IssuanceRequestDocument.is_deleted == False,
                )
                .limit(30)
                .all()
            )
            if any(not _is_sample_filename(fn) for (fn,) in ireq_docs):
                own_documents_uploaded = True
    except Exception:
        pass

    try:
        from app.models.models_reconciliation_v2 import BankStatement

        stmts = (
            db.query(BankStatement)
            .filter(BankStatement.company_id == customer_id, BankStatement.is_deleted == False)
            .limit(20)
            .all()
        )
        statement_count = len(stmts)
        if not own_documents_uploaded:
            for st in stmts:
                fn = getattr(st, "file_name", None) or getattr(st, "original_filename", None) or ""
                if fn and not _is_sample_filename(fn):
                    own_documents_uploaded = True
                    break
    except Exception:
        pass

    try:
        from app.models.models_quotation import QuotationRequest

        rfq_count = (
            db.query(QuotationRequest)
            .filter(QuotationRequest.customer_id == customer_id, QuotationRequest.is_deleted == False)
            .count()
        )
    except Exception:
        pass

    total_lgs = custody_count + issued_count
    steps_completed = {
        "custody": custody_engaged,
        "issuance": issuance_engaged,
        "reconciliation": statement_count > 0,
        "quotations": rfq_count > 0,
    }
    any_test_done = any(steps_completed.values())

    if len(invited_colleagues) > 0:
        funnel_stage = "MULTI_STAKEHOLDER"
        stage_label = f"Team Involved ({len(invited_colleagues) + 1} Users)"
    elif own_documents_uploaded:
        funnel_stage = "OWN_DOCS_UPLOADED"
        stage_label = "Uploaded Own Files"
    elif any_test_done:
        funnel_stage = "FIRST_TEST_DONE"
        stage_label = "Completed 1st Test"
    elif is_activated:
        funnel_stage = "ACTIVATED"
        stage_label = "Activated (Logged In)"
    else:
        funnel_stage = "WAITING_ACTIVATION"
        stage_label = "Waiting for 1st Login"

    return {
        "caps": caps,
        "primary_module": primary_module,
        "is_activated": is_activated,
        "activated_at": activated_at_iso,
        "end_date": end_date_iso,
        "days_remaining": days_remaining if is_activated else caps["duration_days"],
        "days_since_activation": days_since_activation,
        "own_documents_uploaded": own_documents_uploaded,
        "invited_colleagues_count": len(invited_colleagues),
        "invited_colleagues": invited_colleagues,
        "funnel_stage": funnel_stage,
        "stage_label": stage_label,
        "usage": {
            "custody_lgs": custody_count,
            "issuance_items": issued_count,
            "total_lgs": total_lgs,
            "lg_records": total_lgs,
            "statements": statement_count,
            "rfqs": rfq_count,
        },
        "steps_completed": steps_completed,
    }


def enforce_trial_quota(db: Session, customer_id: Optional[int], resource: str) -> None:
    """
    Enforces 'few-uses' trial sandbox caps with an encouraging commercial upgrade message.
    100% no-op for paying customers.
    """
    if not customer_id or not is_trial_customer(db, customer_id):
        return

    snap = get_trial_usage_snapshot(db, int(customer_id))
    caps = snap["caps"]
    usage = snap["usage"]

    exceeded = False
    label = ""
    limit_val = 0

    if resource == "lg" and usage["total_lgs"] >= caps["max_lgs"]:
        exceeded = True
        label = "Guarantee records"
        limit_val = caps["max_lgs"]
    elif resource == "statement" and usage["statements"] >= caps["max_statements"]:
        exceeded = True
        label = "Bank Statement uploads"
        limit_val = caps["max_statements"]
    elif resource == "rfq" and usage["rfqs"] >= caps["max_rfqs"]:
        exceeded = True
        label = "Multi-Bank Quotations"
        limit_val = caps["max_rfqs"]

    if exceeded:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Trial Sandbox limit reached ({limit_val} {label}). "
                "Ready to run unlimited live operations? Book your 15-minute production setup with Grow!"
            ),
        )


def inject_trial_html_watermark(html_content: str) -> str:
    """
    Injects a non-bypassable diagonal watermark + top evaluation banner into HTML documents
    prior to WeasyPrint PDF generation for Trial Sandbox sessions.
    Zero effect on paying customers.
    """
    if not is_active_trial_context() or not html_content:
        return html_content
    if "grow-trial-sandbox-watermark" in html_content:
        return html_content

    watermark_block = f"""
    <style id="grow-trial-sandbox-watermark-css">
      #grow-trial-sandbox-banner {{
        position: fixed;
        top: 2mm;
        left: 10mm;
        right: 10mm;
        text-align: center;
        font-family: Arial, Helvetica, sans-serif;
        font-size: 7.5pt;
        font-weight: 700;
        letter-spacing: 0.08em;
        color: #991b1b;
        background: rgba(254, 226, 226, 0.88);
        border: 1px solid rgba(220, 38, 38, 0.45);
        border-radius: 3px;
        padding: 2px 8px;
        z-index: 99998;
      }}
      #grow-trial-sandbox-watermark {{
        position: fixed;
        top: 46%;
        left: 50%;
        width: 170mm;
        transform: translate(-50%, -50%) rotate(-32deg);
        text-align: center;
        font-family: Arial, Helvetica, sans-serif;
        color: rgba(185, 28, 28, 0.14);
        border: 2.5px solid rgba(185, 28, 28, 0.18);
        border-radius: 8px;
        padding: 10px 14px;
        pointer-events: none;
        z-index: 99999;
      }}
      #grow-trial-sandbox-watermark .wm-main {{
        display: block;
        font-size: 21pt;
        font-weight: 900;
        letter-spacing: 0.08em;
        line-height: 1.15;
      }}
      #grow-trial-sandbox-watermark .wm-sub {{
        display: block;
        font-size: 10pt;
        font-weight: 800;
        letter-spacing: 0.12em;
        margin-top: 4px;
      }}
    </style>
    <div id="grow-trial-sandbox-banner">
      {TRIAL_WATERMARK_HEADLINE} &nbsp;&middot;&nbsp; {TRIAL_WATERMARK_SUBLINE}
    </div>
    <div id="grow-trial-sandbox-watermark">
      <span class="wm-main">{TRIAL_WATERMARK_HEADLINE}</span>
      <span class="wm-sub">{TRIAL_WATERMARK_SUBLINE}</span>
    </div>
    """

    if "</body>" in html_content:
        return html_content.replace("</body>", f"{watermark_block}</body>", 1)
    return html_content + watermark_block


def stamp_trial_pdf_bytes_watermark(
    pdf_bytes: Optional[bytes],
    force_trial: bool = False,
) -> Optional[bytes]:
    """
    Overlays a crisp semi-transparent diagonal TRIAL SANDBOX watermark + top evaluation banner
    onto every page of a PDF byte stream (all bank instruction letters, auto-renewal batches,
    issuance letters, and auto-filled official bank PDF forms) for Trial Sandbox sessions.
    Idempotent and 100% zero effect on paying commercial customers.
    """
    if not pdf_bytes or (not force_trial and not is_active_trial_context()):
        return pdf_bytes

    try:
        from pypdf import PdfReader, PdfWriter
        from reportlab.pdfgen import canvas

        reader = PdfReader(io.BytesIO(pdf_bytes))
        if reader.metadata and reader.metadata.get("/GrowTrialWatermarked") == "1":
            return pdf_bytes

        writer = PdfWriter()
        if reader.metadata:
            writer.add_metadata(dict(reader.metadata))
        writer.add_metadata({"/GrowTrialWatermarked": "1"})

        for page in reader.pages:
            pw = float(page.mediabox.width)
            ph = float(page.mediabox.height)

            wm_stream = io.BytesIO()
            c = canvas.Canvas(wm_stream, pagesize=(pw, ph))

            # Top evaluation banner at the physical top edge of the page
            c.saveState()
            if hasattr(c, "setFillAlpha"):
                c.setFillAlpha(0.92)
            c.setFillColorRGB(0.99, 0.90, 0.90)
            c.setStrokeColorRGB(0.84, 0.14, 0.14)
            c.setLineWidth(1)
            c.roundRect(28, ph - 26, pw - 56, 17, 3, fill=1, stroke=1)
            c.setFillColorRGB(0.60, 0.09, 0.09)
            c.setFont("Helvetica-Bold", 8.0)
            c.drawCentredString(
                pw / 2.0,
                ph - 20,
                f"{TRIAL_WATERMARK_HEADLINE}  ·  {TRIAL_WATERMARK_SUBLINE}",
            )
            c.restoreState()

            # Diagonal center stamp across the page body
            c.saveState()
            if hasattr(c, "setFillAlpha"):
                c.setFillAlpha(0.20)
                c.setStrokeAlpha(0.28)
            c.translate(pw / 2.0, ph / 2.0)
            c.rotate(34)
            c.setStrokeColorRGB(0.74, 0.10, 0.10)
            c.setLineWidth(2.2)
            c.roundRect(-220, -38, 440, 76, 8, fill=0, stroke=1)
            c.setFillColorRGB(0.74, 0.10, 0.10)
            c.setFont("Helvetica-Bold", 20.5)
            c.drawCentredString(0, 7, TRIAL_WATERMARK_HEADLINE)
            c.setFont("Helvetica-Bold", 10.5)
            c.drawCentredString(0, -16, TRIAL_WATERMARK_SUBLINE)
            c.restoreState()

            c.save()
            wm_stream.seek(0)
            wm_page = PdfReader(wm_stream).pages[0]
            page.merge_page(wm_page)
            writer.add_page(page)

        out = io.BytesIO()
        writer.write(out)
        return out.getvalue()
    except Exception as exc:
        logger.warning(f"Trial PDF watermark stamp fallback triggered: {exc}")
        return pdf_bytes


def filter_trial_outbound_emails(
    db: Optional[Session],
    to_emails: List[str],
    customer_id: Optional[int] = None,
    extra_allowed_domains: Optional[Any] = None,
) -> List[str]:
    """
    For Trial Sandbox sessions, blocks outbound emails to external real-world bank domains
    and simulated `.sandbox@` / `-sandbox-bank.com` addresses, permitting delivery strictly
    to the trial customer's own domain and Grow admin addresses.
    100% pass-through for paying customers.
    """
    if not to_emails:
        return to_emails

    eff_cust_id = customer_id or get_active_trial_customer_id()
    in_trial = is_active_trial_context() or (bool(eff_cust_id) and is_trial_customer(db, eff_cust_id))
    if not in_trial:
        return to_emails

    allowed_domains = {"growbusinessdevelopment.com"}
    if extra_allowed_domains:
        for d in extra_allowed_domains:
            if d:
                allowed_domains.add(str(d).strip().lower())

    def _is_simulated_addr_or_domain(addr: str, d: str) -> bool:
        return (
            ".sandbox@" in addr
            or addr.startswith("fx.desk.sandbox@")
            or d.endswith(".local")
            or d.endswith(".invalid")
            or d.endswith("-sandbox-bank.com")
            or d in ("example.com", "sandbox-bank.com", "sandbox.growbusinessdevelopment.com")
        )

    if db:
        try:
            from app.models.models import TrialRegistration, User

            q = db.query(User.email).filter(User.is_deleted == False)
            if eff_cust_id:
                q = q.filter(User.customer_id == int(eff_cust_id))
            for (u_email,) in q.limit(300).all():
                if u_email and "@" in u_email:
                    clean_u = u_email.strip().lower()
                    dom = clean_u.rsplit("@", 1)[1].strip()
                    if not _is_simulated_addr_or_domain(clean_u, dom):
                        allowed_domains.add(dom)

            if is_trial_mode() and not eff_cust_id:
                for (reg_email,) in db.query(TrialRegistration.admin_email).limit(300).all():
                    if reg_email and "@" in reg_email:
                        clean_r = reg_email.strip().lower()
                        dom = clean_r.rsplit("@", 1)[1].strip()
                        if not _is_simulated_addr_or_domain(clean_r, dom):
                            allowed_domains.add(dom)
        except Exception:
            pass

    safe_recipients: List[str] = []
    for raw in to_emails:
        clean = (raw or "").strip().lower()
        if "@" not in clean:
            continue
        dom = clean.rsplit("@", 1)[1].strip()
        if _is_simulated_addr_or_domain(clean, dom):
            logger.info(f"[TrialSandbox] Suppressed simulated recipient email: {clean}")
            continue
        if dom in allowed_domains:
            safe_recipients.append(raw)
        else:
            logger.info(f"[TrialSandbox] Blocked external domain email for trial session: {clean}")
    return safe_recipients
