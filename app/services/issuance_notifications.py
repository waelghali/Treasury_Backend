# app/services/issuance_notifications.py
"""
Notification helpers for issuance approval workflow.
Called from endpoints as background tasks.
"""
import json
import logging
import os
from typing import List, Optional
from sqlalchemy.orm import Session

from app.core.email_service import send_email, get_customer_email_settings
from app.models import User
from app.constants import GlobalConfigKey

logger = logging.getLogger(__name__)


def get_common_communication_emails(db: Session, customer_id: int) -> List[str]:
    """
    Shared utility: Fetches the COMMON_COMMUNICATION_LIST emails from customer
    configuration (or global fallback). These emails get CC'd on every
    operational notification (NOT on private comms like passwords/OTP).
    
    Reusable by both issuance and custody modules.
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
    Called after submit_for_approval.
    """
    approver_emails = _get_user_emails(db, approver_user_ids)
    if not approver_emails:
        logger.warning(f"No approver emails found for request {request_id}")
        return

    email_settings, _ = get_customer_email_settings(db, customer_id)
    link = f"{_base_url()}/issuance/requests"

    # CC the common communication list
    cc_emails = get_common_communication_emails(db, customer_id)

    subject = f"ACTION REQUIRED: LG Issuance Request {request_serial} Awaiting Your Approval"
    body = f"""
    <html>
    <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
        <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
            <h2 style="color: #1a56db; margin-top: 0;">🔔 New Approval Request</h2>
            <p>A new LG issuance request has been submitted and requires your approval.</p>
            
            <div style="background: #f8fafc; border-left: 4px solid #1a56db; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{request_serial}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency_code} {amount}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{beneficiary}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Submitted by:</td><td style="padding: 4px 0;">{submitter_email or 'External Requestor'}</td></tr>
                </table>
            </div>

            <div style="text-align: center; margin: 25px 0;">
                <a href="{link}" style="padding: 12px 30px; background: #1a56db; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">Review Request</a>
            </div>
            
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
            <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
        </div>
    </body>
    </html>
    """

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

    email_settings, _ = get_customer_email_settings(db, customer_id)
    link = f"{_base_url()}/issuance/requests"

    # CC the common communication list
    cc_emails = get_common_communication_emails(db, customer_id)

    is_final = new_status == "APPROVED_INTERNAL"
    status_label = "Fully Approved ✅" if is_final else "Step Approved — Proceeding to Next Approver"

    subject = f"LG Request {request_serial} — {status_label}"
    body = f"""
    <html>
    <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
        <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
            <h2 style="color: #16a34a; margin-top: 0;">✅ Request Approved</h2>
            <p>Your LG issuance request has been <strong>{status_label.lower()}</strong>.</p>
            
            <div style="background: #f0fdf4; border-left: 4px solid #16a34a; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{request_serial}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency_code} {amount}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{beneficiary}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Status:</td><td style="padding: 4px 0; font-weight: bold; color: #16a34a;">{new_status}</td></tr>
                </table>
            </div>

            {"<p>Your request is now ready for issuance execution.</p>" if is_final else "<p>The request is moving to the next approval step.</p>"}

            <div style="text-align: center; margin: 25px 0;">
                <a href="{link}" style="padding: 12px 30px; background: #16a34a; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">View Status</a>
            </div>
            
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
            <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
        </div>
    </body>
    </html>
    """

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

    email_settings, _ = get_customer_email_settings(db, customer_id)
    link = f"{_base_url()}/issuance/requests"

    # CC the common communication list
    cc_emails = get_common_communication_emails(db, customer_id)

    subject = f"LG Request {request_serial} — Rejected ❌"
    body = f"""
    <html>
    <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
        <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
            <h2 style="color: #dc2626; margin-top: 0;">❌ Request Rejected</h2>
            <p>Your LG issuance request has been rejected by an approver.</p>
            
            <div style="background: #fef2f2; border-left: 4px solid #dc2626; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 4px 0; color: #666;">Reference:</td><td style="padding: 4px 0; font-weight: bold;">{request_serial}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Amount:</td><td style="padding: 4px 0; font-weight: bold;">{currency_code} {amount}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Beneficiary:</td><td style="padding: 4px 0;">{beneficiary}</td></tr>
                    <tr><td style="padding: 4px 0; color: #666;">Rejected by:</td><td style="padding: 4px 0;">{rejector_email}</td></tr>
                </table>
            </div>

            <p>Please review the request details and contact the approver if clarification is needed. You may need to revise and resubmit.</p>

            <div style="text-align: center; margin: 25px 0;">
                <a href="{link}" style="padding: 12px 30px; background: #dc2626; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold; display: inline-block;">View Request</a>
            </div>
            
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
            <p style="font-size: 12px; color: #999;">This is an automated notification from your Treasury LG Issuance system.</p>
        </div>
    </body>
    </html>
    """

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
    Notify the next set of approvers in a multi-step workflow
    after the previous step was approved.
    """
    # Reuse the same template as initial submission notification
    await notify_approvers_of_submission(
        db, request_serial, request_id, amount, currency_code,
        beneficiary, "Previous Approver", approver_user_ids, customer_id
    )
