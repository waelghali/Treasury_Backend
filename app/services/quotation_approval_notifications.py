# app/services/quotation_approval_notifications.py
import logging
from typing import Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import BackgroundTasks, Request

from app.models.models_quotation import QuotationRequest
from app.models.models import User, UserRole
from app.core.routing import get_frontend_base_url
from app.core.email_service import get_customer_email_settings, send_email
from app.services.unified_email_builder import build_transaction_email_html
from app.crud.crud_common_communication import get_common_communication_emails

logger = logging.getLogger(__name__)


def _format_rfq_key_values(rfq: QuotationRequest) -> dict:
    """Extracts clean key-value summary for quotation emails."""
    kv = {
        "Quotation Reference": rfq.ref_no,
        "Type": "FX Spot" if rfq.type == "FX_SPOT" else "Treasury Bill (T-Bill)",
        "Direction": rfq.direction or "N/A",
    }
    
    legs_list = getattr(rfq, "legs", []) or []
    if rfq.type == "FX_SPOT":
        if len(legs_list) > 1:
            leg_bases = list(set((l.quotation_base or "Execution").capitalize() for l in legs_list))
            is_mixed = len(leg_bases) > 1
            mixed_str = " • Mixed Execution & Indicative" if is_mixed else ""
            kv["Package Structure"] = f"Multi-Currency Package ({len(legs_list)} Pairs{mixed_str})"
            for idx, leg in enumerate(legs_list, 1):
                leg_dir = leg.direction or "Buy"
                leg_amt = f"{leg.amount:,.2f} {leg.buy_currency}" if leg.amount else "N/A"
                leg_pair = leg.currency_pair or f"{leg.buy_currency}/{leg.sell_currency}"
                leg_val = str(leg.value_date) if leg.value_date else "Standard"
                leg_base = leg.quotation_base or "Execution"
                kv[f"Leg #{idx}: {leg_pair}"] = f"{leg_dir} {leg_amt} | Value: {leg_val} | Base: {leg_base}"
        else:
            if rfq.amount:
                curr = rfq.buy_currency if rfq.direction == "Buy" else rfq.sell_currency
                kv["Amount"] = f"{rfq.amount:,.2f} {curr or ''}".strip()
            if rfq.buy_currency and rfq.sell_currency:
                kv["Currency Pair"] = f"{rfq.buy_currency} / {rfq.sell_currency}"
            if rfq.value_date:
                kv["Value Date"] = str(rfq.value_date)
            if rfq.quotation_base:
                kv["Quotation Base"] = rfq.quotation_base
    else:
        if rfq.amount:
            kv["Face Value"] = f"{rfq.amount:,.2f} EGP"
        if rfq.maturity_date_start:
            kv["Maturity"] = (
                f"{rfq.maturity_date_start} to {rfq.maturity_date_end}"
                if rfq.maturity_date_end and rfq.maturity_date_end != rfq.maturity_date_start
                else str(rfq.maturity_date_start)
            )

    entity_name = rfq.entity.entity_name if getattr(rfq, "entity", None) and rfq.entity.entity_name else None
    if entity_name:
        kv["Requesting Entity"] = entity_name

    return kv


def dispatch_rfq_revision_requested_email(
    db: Session,
    background_tasks: BackgroundTasks,
    rfq: QuotationRequest,
    admin_notes: str,
    admin_email: str,
    request: Optional[Request] = None
):
    """
    Step 2: Corporate Admin sends RFQ back for revision.
    Notifies the End User Maker that action is required to revise and resubmit.
    """
    try:
        maker = db.query(User).filter(User.id == rfq.created_by_user_id).first()
        if not maker or not maker.email or "@" not in maker.email:
            logger.info(f"No maker email found for RFQ {rfq.ref_no} revision email.")
            return

        base_url = get_frontend_base_url(request=request)
        email_settings, _ = get_customer_email_settings(db, rfq.customer_id)
        customer_display_name = rfq.customer.name if getattr(rfq, "customer", None) and rfq.customer.name else "Corporate Treasury"

        kv = {
            "Quotation Reference": rfq.ref_no,
            "Revision Required By": admin_email,
            "Corporate Admin Feedback": admin_notes,
        }
        # Append trade details
        kv.update(_format_rfq_key_values(rfq))

        subject = f"ACTION REQUIRED: Quotation Request {rfq.ref_no} Returned for Revision"
        title = "⚠️ Quotation Returned for Revision"
        summary_text = (
            f"Your quotation request ({rfq.ref_no}) has been returned for revision by Corporate Admin ({admin_email}). "
            f"Please review the feedback below, make the necessary parameter adjustments in the Quotation Builder, and resubmit for approval."
        )
        cta_text = "Revise & Resubmit Quotation"
        cta_url = f"{base_url}/end-user/quotations/active?revision_rfq_id={rfq.id}"

        maker_name = getattr(maker, "first_name", "") or maker.email.split("@")[0]
        body = build_transaction_email_html(
            customer_name=customer_display_name,
            title=title,
            transaction_ref=rfq.ref_no,
            transaction_type="Quotation Revision Request",
            key_value_dict=kv,
            summary_text=summary_text,
            cta_text=cta_text,
            cta_url=cta_url,
            recipient_name=maker_name,
            platform_name="Grow Treasury Platform"
        )

        background_tasks.add_task(
            send_email,
            db,
            [maker.email],
            subject,
            body,
            {},
            email_settings
        )
        logger.info(f"Queued RFQ revision email to maker {maker.email} for RFQ {rfq.ref_no}")
    except Exception as e:
        logger.error(f"Error queueing revision email for RFQ {getattr(rfq, 'ref_no', '')}: {e}", exc_info=True)


def dispatch_rfq_resubmitted_email(
    db: Session,
    background_tasks: BackgroundTasks,
    rfq: QuotationRequest,
    user_notes: Optional[str],
    submitter_email: str,
    request: Optional[Request] = None
):
    """
    Step 3: End User revises and resubmits the RFQ.
    Notifies Corporate Admins that the revised RFQ is awaiting their approval again.
    """
    try:
        admins = db.query(User).filter(
            User.customer_id == rfq.customer_id,
            User.role == UserRole.CORPORATE_ADMIN,
            User.is_deleted == False
        ).all()

        admin_emails = list(dict.fromkeys(
            a.email.strip() for a in admins if a.email and "@" in a.email
        ))
        cc_list = get_common_communication_emails(db, rfq.customer_id)
        admin_set = {e.lower() for e in admin_emails}
        cc_emails = [e for e in cc_list if e.lower() not in admin_set]

        to_recipients = admin_emails if admin_emails else cc_emails
        cc_recipients = cc_emails if admin_emails else []

        if not to_recipients:
            logger.info(f"No admin recipients found for RFQ {rfq.ref_no} resubmission email.")
            return

        base_url = get_frontend_base_url(request=request)
        email_settings, _ = get_customer_email_settings(db, rfq.customer_id)
        customer_display_name = rfq.customer.name if getattr(rfq, "customer", None) and rfq.customer.name else "Corporate Treasury"

        kv = {
            "Quotation Reference": rfq.ref_no,
            "Resubmitted By": submitter_email,
            "Status": "Awaiting Corporate Admin Approval",
        }
        if user_notes and user_notes.strip():
            kv["Maker Response Notes"] = user_notes.strip()

        kv.update(_format_rfq_key_values(rfq))

        legs_list = getattr(rfq, "legs", []) or []
        multi_label = f" ({len(legs_list)} Pairs)" if len(legs_list) > 1 else ""

        subject = f"ACTION REQUIRED: Revised Quotation {rfq.ref_no}{multi_label} Resubmitted for Approval"
        title = "🔔 Revised Quotation Awaiting Approval"
        summary_text = (
            f"Maker {submitter_email} has addressed the previous revision feedback and resubmitted Quotation Request ({rfq.ref_no}). "
            f"Please review the updated parameters and approve for release to counterparties."
        )
        cta_text = "Review & Approve Quotation"
        cta_url = f"{base_url}/corporate-admin/quotations/history?rfq_id={rfq.id}"

        body = build_transaction_email_html(
            customer_name=customer_display_name,
            title=title,
            transaction_ref=rfq.ref_no,
            transaction_type="Revised Quotation Request",
            key_value_dict=kv,
            summary_text=summary_text,
            cta_text=cta_text,
            cta_url=cta_url,
            recipient_name="Corporate Admin",
            platform_name="Grow Treasury Platform"
        )

        background_tasks.add_task(
            send_email,
            db,
            to_recipients,
            subject,
            body,
            {},
            email_settings,
            cc_emails=cc_recipients
        )
        logger.info(f"Queued RFQ resubmission email to admins {to_recipients} for RFQ {rfq.ref_no}")
    except Exception as e:
        logger.error(f"Error queueing resubmission email for RFQ {getattr(rfq, 'ref_no', '')}: {e}", exc_info=True)


def dispatch_rfq_approved_email(
    db: Session,
    background_tasks: BackgroundTasks,
    rfq: QuotationRequest,
    admin_email: str,
    scheduled_time: Optional[datetime] = None,
    request: Optional[Request] = None
):
    """
    Step 4: Corporate Admin approves the RFQ.
    Notifies the End User Maker that their RFQ has been approved (and released or scheduled).
    """
    try:
        maker = db.query(User).filter(User.id == rfq.created_by_user_id).first()
        if not maker or not maker.email or "@" not in maker.email:
            logger.info(f"No maker email found for RFQ {rfq.ref_no} approval confirmation email.")
            return

        base_url = get_frontend_base_url(request=request)
        email_settings, _ = get_customer_email_settings(db, rfq.customer_id)
        customer_display_name = rfq.customer.name if getattr(rfq, "customer", None) and rfq.customer.name else "Corporate Treasury"

        is_scheduled = scheduled_time is not None
        sched_str = scheduled_time.strftime("%A, %d %b %Y at %H:%M UTC") if is_scheduled else None

        kv = {
            "Quotation Reference": rfq.ref_no,
            "Approved By": admin_email,
            "Status": "Scheduled for Release" if is_scheduled else "Approved & Released to Banks",
        }
        if is_scheduled:
            kv["Scheduled Release Time"] = sched_str

        kv.update(_format_rfq_key_values(rfq))

        if is_scheduled:
            subject = f"CONFIRMATION: Quotation Request {rfq.ref_no} Approved & Scheduled"
            title = "📅 Quotation Approved & Scheduled"
            summary_text = (
                f"Your quotation request ({rfq.ref_no}) has been approved by Corporate Admin ({admin_email}). "
                f"It is scheduled for automatic broadcast to invited banks on {sched_str}."
            )
            cta_text = "View Quotation Details"
        else:
            subject = f"CONFIRMATION: Quotation Request {rfq.ref_no} Approved & Released"
            title = "✅ Quotation Approved & Released"
            summary_text = (
                f"Your quotation request ({rfq.ref_no}) has been approved by Corporate Admin ({admin_email}) "
                f"and broadcast live to participating bank counterparties."
            )
            cta_text = "Track Live Quotation"

        cta_url = f"{base_url}/end-user/quotations/history?rfq_id={rfq.id}"
        maker_name = getattr(maker, "first_name", "") or maker.email.split("@")[0]

        body = build_transaction_email_html(
            customer_name=customer_display_name,
            title=title,
            transaction_ref=rfq.ref_no,
            transaction_type="Quotation Approval Confirmation",
            key_value_dict=kv,
            summary_text=summary_text,
            cta_text=cta_text,
            cta_url=cta_url,
            recipient_name=maker_name,
            platform_name="Grow Treasury Platform"
        )

        background_tasks.add_task(
            send_email,
            db,
            [maker.email],
            subject,
            body,
            {},
            email_settings
        )
        logger.info(f"Queued RFQ approval email to maker {maker.email} for RFQ {rfq.ref_no}")
    except Exception as e:
        logger.error(f"Error queueing approval email for RFQ {getattr(rfq, 'ref_no', '')}: {e}", exc_info=True)
