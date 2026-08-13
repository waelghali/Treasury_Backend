# app/services/issuance_notifications.py
"""
Notification helpers for issuance approval workflow and maintenance notifications.
Called from endpoints as background tasks. Uses unified SaaS email templates.
"""
import json
import logging
import os
from typing import List, Optional
from sqlalchemy.orm import Session

from app.core.email_service import send_email, get_customer_email_settings
from app.models import User, Customer
from app.constants import GlobalConfigKey
from app.services.unified_email_builder import (
    build_transaction_email_html,
    build_alert_email_html,
    build_standard_email_html
)

logger = logging.getLogger(__name__)


def _get_customer_name(db: Session, customer_id: int) -> str:
    """Helper to fetch customer name for email header branding."""
    c = db.query(Customer).filter(Customer.id == customer_id).first()
    return c.name if c else "Grow Treasury"


def get_common_communication_emails(db: Session, customer_id: int) -> List[str]:
    """
    Shared utility: Fetches the COMMON_COMMUNICATION_LIST emails from customer
    configuration (or global fallback). Reusable by issuance and custody modules.
    """
    from app.crud.crud_config import crud_customer_configuration
    try:
        config = crud_customer_configuration.get_customer_config_or_global_fallback(
            db, customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
        )
        if config and config.get('effective_value'):
            parsed = json.loads(config['effective_value'])
            if isinstance(parsed, list):
                return [e for e in parsed if isinstance(e, str) and "@" in e]
    except json.JSONDecodeError:
        logger.warning(f"COMMON_COMMUNICATION_LIST for customer {customer_id} is not valid JSON. Skipping.")
    except Exception:
        logger.warning(f"Failed to fetch COMMON_COMMUNICATION_LIST for customer {customer_id}.", exc_info=True)
    return []


def _get_user_emails(db: Session, user_ids: List[int]) -> List[str]:
    """Resolve user IDs to email addresses."""
    if not user_ids:
        return []
    users = db.query(User.email).filter(User.id.in_(user_ids)).all()
    return [u.email for u in users if u.email]


def _get_user_email(db: Session, user_id: Optional[int]) -> Optional[str]:
    """Resolve single user ID to email."""
    if not user_id:
        return None
    user = db.query(User).filter(User.id == user_id).first()
    return user.email if user else None


def _base_url() -> str:
    return os.getenv("FRONTEND_URL", "http://localhost:3000")


async def notify_approvers_of_submission(
    db: Session,
    request_serial: str,
    request_id: int,
    amount: str,
    currency_code: str,
    beneficiary: str,
    submitter_email: str,
    approver_user_ids: List[int],
    customer_id: int
):
    """
    Notify pending approvers that a new request needs their attention.
    """
    approver_emails = _get_user_emails(db, approver_user_ids)
    if not approver_emails:
        logger.warning(f"No approver emails found for request {request_id}")
        return

    customer_name = _get_customer_name(db, customer_id)
    email_settings, _ = get_customer_email_settings(db, customer_id)
    link = f"{_base_url()}/issuance/requests"
    cc_emails = get_common_communication_emails(db, customer_id)

    subject = f"ACTION REQUIRED: LG Issuance Request {request_serial} Awaiting Your Approval"

    body = build_transaction_email_html(
        customer_name=customer_name,
        title="🔔 New Approval Request",
        transaction_ref=request_serial,
        transaction_type="LG Issuance Request",
        key_value_dict={
            "Reference": request_serial,
            "Amount": f"{currency_code} {amount}",
            "Beneficiary": beneficiary,
            "Submitted By": submitter_email or 'External Requestor'
        },
        summary_text="A new LG issuance request has been submitted and requires your review and approval.",
        cta_text="Review Request",
        cta_url=link,
        recipient_name="Approver"
    )

    await send_email(db, approver_emails, subject, body, {}, email_settings, cc_emails=cc_emails)
    logger.info(f"Approval notification sent to {approver_emails} (CC: {cc_emails}) for request {request_serial}")


async def notify_submitter_of_approval(
    db: Session,
    request_serial: str,
    request_id: int,
    amount: str,
    currency_code: str,
    beneficiary: str,
    submitter_user_id: Optional[int],
    requestor_email: Optional[str],
    new_status: str,
    customer_id: int
):
    """
    Notify the original submitter that their request was approved.
    """
    to_emails = []
    submitter_email = _get_user_email(db, submitter_user_id)
    if submitter_email:
        to_emails.append(submitter_email)
    if requestor_email and requestor_email not in to_emails:
        to_emails.append(requestor_email)

    if not to_emails:
        return

    customer_name = _get_customer_name(db, customer_id)
    email_settings, _ = get_customer_email_settings(db, customer_id)
    link = f"{_base_url()}/issuance/requests"
    cc_emails = get_common_communication_emails(db, customer_id)

    is_final = new_status == "APPROVED_INTERNAL"
    status_label = "Fully Approved ✅" if is_final else "Step Approved — Proceeding to Next Approver"

    subject = f"LG Request {request_serial} — {status_label}"
    summary = "Your request is now ready for issuance execution." if is_final else "The request is moving to the next approval step."

    body = build_transaction_email_html(
        customer_name=customer_name,
        title="✅ Request Approved",
        transaction_ref=request_serial,
        transaction_type="LG Issuance Status",
        key_value_dict={
            "Reference": request_serial,
            "Amount": f"{currency_code} {amount}",
            "Beneficiary": beneficiary,
            "Current Status": new_status
        },
        summary_text=f"Your LG issuance request has been <strong>{status_label.lower()}</strong>. {summary}",
        cta_text="View Request Status",
        cta_url=link,
        recipient_name="Requestor"
    )

    await send_email(db, to_emails, subject, body, {}, email_settings, cc_emails=cc_emails)
    logger.info(f"Approval notification sent to {to_emails} (CC: {cc_emails}) for request {request_serial}")


async def notify_submitter_of_rejection(
    db: Session,
    request_serial: str,
    request_id: int,
    amount: str,
    currency_code: str,
    beneficiary: str,
    submitter_user_id: Optional[int],
    requestor_email: Optional[str],
    rejector_email: str,
    customer_id: int
):
    """
    Notify the original submitter that their request was rejected.
    """
    to_emails = []
    submitter_email = _get_user_email(db, submitter_user_id)
    if submitter_email:
        to_emails.append(submitter_email)
    if requestor_email and requestor_email not in to_emails:
        to_emails.append(requestor_email)

    if not to_emails:
        return

    customer_name = _get_customer_name(db, customer_id)
    email_settings, _ = get_customer_email_settings(db, customer_id)
    link = f"{_base_url()}/issuance/requests"
    cc_emails = get_common_communication_emails(db, customer_id)

    subject = f"LG Request {request_serial} — Rejected ❌"

    table_details = f"""
    <table style="width: 100%; border-collapse: collapse; text-align: left;">
        <tr style="border-bottom: 1px solid #e2e8f0;"><td style="padding: 8px 12px; font-weight: 600; color: #475569;">Reference:</td><td style="padding: 8px 12px; font-weight: 700;">{request_serial}</td></tr>
        <tr style="border-bottom: 1px solid #e2e8f0;"><td style="padding: 8px 12px; font-weight: 600; color: #475569;">Amount:</td><td style="padding: 8px 12px; font-weight: 700;">{currency_code} {amount}</td></tr>
        <tr style="border-bottom: 1px solid #e2e8f0;"><td style="padding: 8px 12px; font-weight: 600; color: #475569;">Beneficiary:</td><td style="padding: 8px 12px;">{beneficiary}</td></tr>
        <tr><td style="padding: 8px 12px; font-weight: 600; color: #475569;">Rejected by:</td><td style="padding: 8px 12px; font-weight: 700; color: #dc2626;">{rejector_email}</td></tr>
    </table>
    """

    body = build_alert_email_html(
        customer_name=customer_name,
        title="❌ Request Rejected",
        alert_type="critical",
        message="Your LG issuance request has been rejected by an approver. Please review the details below and revise/resubmit if necessary.",
        details_table_html=table_details,
        cta_text="View Request Details",
        cta_url=link,
        recipient_name="Requestor"
    )

    await send_email(db, to_emails, subject, body, {}, email_settings, cc_emails=cc_emails)
    logger.info(f"Rejection notification sent to {to_emails} (CC: {cc_emails}) for request {request_serial}")


async def notify_next_approvers(
    db: Session,
    request_serial: str,
    request_id: int,
    amount: str,
    currency_code: str,
    beneficiary: str,
    approver_user_ids: List[int],
    customer_id: int
):
    """
    Notify the next set of approvers in a multi-step workflow.
    """
    await notify_approvers_of_submission(
        db, request_serial, request_id, amount, currency_code,
        beneficiary, "Previous Approver", approver_user_ids, customer_id
    )


# ──────────────────────────────────────────────────
# MAINTENANCE ACTION NOTIFICATIONS
# ──────────────────────────────────────────────────

ACTION_TYPE_LABELS = {
    "EXTEND": "Extension",
    "INCREASE_AMOUNT": "Amount Increase",
    "AMENDMENT": "Amendment",
    "CLOSE": "Closure",
    "ACTIVATE": "Activation",
    "LIQUIDATION": "Liquidation",
    "CHANGE_OWNERSHIP": "Ownership Transfer",
}


async def notify_maintenance_action_executed(
    db: Session,
    action_type: str,
    lg_ref_number: str,
    bank_name: str,
    beneficiary_name: str,
    amount_formatted: str,
    letter_serial: str,
    action_details_html: str,
    creator_user_id: Optional[int],
    customer_id: int,
):
    """
    Notify the action creator (and CC list) when a maintenance action instruction letter is issued.
    """
    to_emails = []
    creator_email = _get_user_email(db, creator_user_id)
    if creator_email:
        to_emails.append(creator_email)

    if not to_emails:
        logger.warning(f"No recipient email for maintenance notification (action creator {creator_user_id})")
        return

    customer_name = _get_customer_name(db, customer_id)
    email_settings, _ = get_customer_email_settings(db, customer_id)
    cc_emails = get_common_communication_emails(db, customer_id)
    link = f"{_base_url()}/issuance/issued-lgs"

    type_label = ACTION_TYPE_LABELS.get(action_type, action_type.replace("_", " ").title())
    subject = f"LG Maintenance: {type_label} Instruction Issued — {lg_ref_number}"

    body = build_transaction_email_html(
        customer_name=customer_name,
        title=f"📋 {type_label} Instruction Issued",
        transaction_ref=lg_ref_number,
        transaction_type="Maintenance Instruction",
        key_value_dict={
            "LG Reference": lg_ref_number,
            "Bank": bank_name,
            "Beneficiary": beneficiary_name,
            "Amount": amount_formatted,
            "Instruction #": letter_serial
        },
        summary_text=f"A maintenance instruction ({type_label}) has been issued for the LG below. Please print and deliver it to the bank.",
        cta_text="View in Action Center",
        cta_url=link,
        recipient_name="Treasury User"
    )

    await send_email(db, to_emails, subject, body, {}, email_settings, cc_emails=cc_emails)
    logger.info(f"Maintenance execution notification sent to {to_emails} (CC: {cc_emails}) for {lg_ref_number}")


async def notify_maintenance_bank_confirmed(
    db: Session,
    action_type: str,
    lg_ref_number: str,
    bank_name: str,
    beneficiary_name: str,
    amount_formatted: str,
    action_details_html: str,
    creator_user_id: Optional[int],
    customer_id: int,
):
    """
    Notify the action creator (and CC list) when the bank confirms the maintenance action.
    """
    to_emails = []
    creator_email = _get_user_email(db, creator_user_id)
    if creator_email:
        to_emails.append(creator_email)

    if not to_emails:
        logger.warning(f"No recipient email for bank confirmation notification (creator {creator_user_id})")
        return

    customer_name = _get_customer_name(db, customer_id)
    email_settings, _ = get_customer_email_settings(db, customer_id)
    cc_emails = get_common_communication_emails(db, customer_id)
    link = f"{_base_url()}/issuance/issued-lgs"

    type_label = ACTION_TYPE_LABELS.get(action_type, action_type.replace("_", " ").title())
    subject = f"LG Maintenance: {type_label} Confirmed by Bank — {lg_ref_number}"

    body = build_transaction_email_html(
        customer_name=customer_name,
        title=f"✅ {type_label} Confirmed by Bank",
        transaction_ref=lg_ref_number,
        transaction_type="Bank Confirmation",
        key_value_dict={
            "LG Reference": lg_ref_number,
            "Bank": bank_name,
            "Beneficiary": beneficiary_name,
            "Amount": amount_formatted
        },
        summary_text=f"The bank has confirmed the {type_label.lower()} for the LG below. The record has been updated.",
        cta_text="View LG Details",
        cta_url=link,
        recipient_name="Treasury User"
    )

    await send_email(db, to_emails, subject, body, {}, email_settings, cc_emails=cc_emails)
    logger.info(f"Bank confirmation notification sent to {to_emails} (CC: {cc_emails}) for {lg_ref_number}")


# ──────────────────────────────────────────────────
# RECONCILIATION NOTIFICATIONS
# ──────────────────────────────────────────────────

async def notify_reconciliation_summary(
    db: Session,
    session_id: int,
    bank_name: str,
    position_date: str,
    stats: dict,
    submitter_user_id: int,
    customer_id: int
):
    """
    Notify Corporate Admins (and CC list) with a summary of a completed reconciliation session.
    """
    from app.constants import UserRole
    admin_users = db.query(User).filter(
        User.customer_id == customer_id,
        User.role == UserRole.CORPORATE_ADMIN
    ).all()
    to_emails = [u.email for u in admin_users if u.email]

    if not to_emails:
        logger.warning(f"No Corporate Admins found to send reconciliation summary to for customer {customer_id}")
        return

    customer_name = _get_customer_name(db, customer_id)
    email_settings, _ = get_customer_email_settings(db, customer_id)
    cc_emails = get_common_communication_emails(db, customer_id)
    
    submitter_email = _get_user_email(db, submitter_user_id) or 'Unknown User'
    link = f"{_base_url()}/issuance/reconciliation"

    subject = f"Reconciliation Complete: {bank_name} ({position_date})"

    body = build_transaction_email_html(
        customer_name=customer_name,
        title="📊 LG Reconciliation Finalized",
        transaction_ref=position_date,
        transaction_type="Reconciliation Summary",
        key_value_dict={
            "Bank": bank_name,
            "Position Date": position_date,
            "Total Parsed": stats.get('total', 0),
            "Perfect Matches": f"<span style='color: #16a34a; font-weight: 700;'>{stats.get('matched', 0)}</span>",
            "Disputes/Mismatches": f"<span style='color: #d97706; font-weight: 700;'>{stats.get('mismatched', 0)}</span>",
            "Untracked (Bank Only)": f"<span style='color: #ef4444; font-weight: 700;'>{stats.get('bankOnly', 0)}</span>",
            "Missing (System Only)": f"<span style='color: #8b5cf6; font-weight: 700;'>{stats.get('systemOnly', 0)}</span>"
        },
        summary_text=f"A bank statement reconciliation session for {bank_name} was finalized by <strong>{submitter_email}</strong>.",
        cta_text="View Dashboard",
        cta_url=link,
        recipient_name="Corporate Admin"
    )

    await send_email(db, to_emails, subject, body, {}, email_settings, cc_emails=cc_emails)
    logger.info(f"Reconciliation summary email sent to Admins ({to_emails}) for Session {session_id}")
