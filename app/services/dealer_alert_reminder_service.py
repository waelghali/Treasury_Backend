# app/services/dealer_alert_reminder_service.py
"""
Dealer Alert Standing Reminders (Notifications N7b).

Allows sending Grow-branded reminder emails to bank dealers who receive RFQs
inviting them to enable mobile lock-screen alerts.
Standing setup links do not expire with an RFQ deal.

NOTE ON SENDER RULES:
Per the agreed architecture in QUOTATION_SYSTEM_ROADMAP.md (Rule 4 exception):
This reminder is sent from the Grow platform relay address, because it is a Grow platform
service covering all customers. RFQ invitation/bidding/results emails strictly remain on
customer-specific SMTP servers with no platform fallback.
"""

import logging
import secrets
from datetime import datetime, timezone
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from app.core.email_service import get_global_email_settings, send_email
from app.core.routing import get_frontend_base_url
from app.models.models import Bank
from app.models.models_push import DealerAlertReminder, PushSubscription
from app.models.models_quotation import QuotationBankDealer

logger = logging.getLogger(__name__)


def get_or_create_dealer_reminder(
    db: Session, email: str, bank_id: Optional[int] = None
) -> DealerAlertReminder:
    """Gets or initializes a standing reminder and setup token for a bank dealer email."""
    norm_email = email.strip().lower()
    rec = db.query(DealerAlertReminder).filter(DealerAlertReminder.email == norm_email).first()
    if not rec:
        token = secrets.token_urlsafe(32)
        rec = DealerAlertReminder(
            email=norm_email,
            bank_id=bank_id,
            setup_token=token,
            reminders_sent=0,
            opted_out=False,
        )
        db.add(rec)
        db.commit()
        db.refresh(rec)
    elif bank_id is not None and rec.bank_id is None:
        rec.bank_id = bank_id
        db.commit()
    return rec


def build_dealer_reminder_email_html(dealer_email: str, setup_url: str, opt_out_url: str, bank_name: Optional[str] = None) -> Tuple[str, str]:
    """Builds clean, professional Grow-branded email HTML."""
    subject = "Grow Treasury · Instant Mobile Alerts for your RFQ Desk"
    greeting = f"Dear {bank_name} Treasury Team" if bank_name else "Dear Treasury Partner"

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f8fafc; color: #1e293b; margin: 0; padding: 24px; }}
    .container {{ max-width: 580px; margin: 0 auto; background: #ffffff; border-radius: 16px; border: 1px solid #e2e8f0; overflow: hidden; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); }}
    .header {{ background: linear-gradient(135deg, #064e3b 0%, #0f172a 100%); color: #ffffff; padding: 32px 28px; text-align: left; }}
    .header h1 {{ margin: 0; font-size: 20px; font-weight: 700; letter-spacing: -0.02em; }}
    .header p {{ margin: 6px 0 0 0; font-size: 13px; color: #a7f3d0; }}
    .content {{ padding: 28px; line-height: 1.6; font-size: 14px; color: #334155; }}
    .card {{ background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 12px; padding: 16px; margin: 20px 0; }}
    .card-title {{ font-weight: 700; color: #166534; font-size: 13px; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; }}
    .card-text {{ font-size: 13px; color: #14532d; margin: 0; }}
    .btn {{ display: inline-block; background-color: #059669; color: #ffffff !important; text-decoration: none; padding: 13px 26px; border-radius: 10px; font-weight: 700; font-size: 14px; margin-top: 14px; text-align: center; }}
    .btn:hover {{ background-color: #047857; }}
    .footer {{ padding: 20px 28px; background: #f8fafc; border-top: 1px solid #e2e8f0; font-size: 11px; color: #64748b; line-height: 1.5; }}
    .footer a {{ color: #64748b; text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Grow Treasury Platform</h1>
      <p>Institutional Market Alerting Infrastructure</p>
    </div>
    <div class="content">
      <p style="margin-top: 0;">{greeting},</p>
      <p>Corporate treasury clients frequently issue time-sensitive quotations (RFQs) with tight 30-to-120 second bidding windows.</p>
      <div class="card">
        <div class="card-title">📱 Never Miss a Quotation Window</div>
        <p class="card-text">You can now receive <strong>instant vibration and sound alerts directly on your phone lock screen</strong> whenever your desk is invited to quote by any corporate client.</p>
      </div>
      <ul style="padding-left: 20px; margin: 16px 0;">
        <li><strong>One-time setup:</strong> Enabling alerts on your phone once covers RFQs from <strong>all corporate clients</strong> who invite your desk.</li>
        <li><strong>Zero-store footprint:</strong> No app store installation required; powered by secure browser Web Push.</li>
        <li><strong>Instant 1-tap access:</strong> Tap the lock-screen alert to immediately open your secure bidding desk.</li>
      </ul>
      <div style="text-align: center; margin: 26px 0;">
        <a href="{setup_url}" class="btn" target="_blank">Enable Phone Alerts Now &rarr;</a>
      </div>
      <p style="font-size: 12px; color: #64748b; margin-bottom: 0;">This standing setup link is securely associated with your official bank contact: <strong>{dealer_email}</strong>.</p>
    </div>
    <div class="footer">
      Sent by Grow Treasury Management Platform on behalf of the institutional quotation desk network.<br>
      If you prefer not to receive mobile alert reminders, you can <a href="{opt_out_url}">opt out of future reminders</a>.
    </div>
  </div>
</body>
</html>"""
    return subject, html


async def send_dealer_reminder_email(
    db: Session,
    reminder: DealerAlertReminder,
    base_url: Optional[str] = None
) -> bool:
    """
    Sends the standing setup reminder email via the Grow platform email settings.
    Guaranteed: This is the ONLY dealer-facing email using platform relay.
    """
    if reminder.opted_out:
        logger.info(f"Dealer {reminder.email} has opted out of reminders; skipping.")
        return False

    # Check if dealer already has an active phone alert
    has_active_push = db.query(PushSubscription).filter(
        PushSubscription.dealer_email == reminder.email,
        PushSubscription.is_active == True,  # noqa: E712
        PushSubscription.is_deleted == False,  # noqa: E712
    ).first()
    if has_active_push:
        logger.info(f"Dealer {reminder.email} already has active push subscription; skipping reminder.")
        return False

    if not base_url:
        base_url = get_frontend_base_url()

    setup_url = f"{base_url}/dealer/alerts/setup/{reminder.setup_token}"
    opt_out_url = f"{base_url}/dealer/alerts/setup/{reminder.setup_token}?optout=true"

    bank_name = None
    if reminder.bank_id:
        bank = db.get(Bank, reminder.bank_id)
        if bank:
            if getattr(bank, "allow_dealer_mobile_alerts", True) is False:
                logger.info(f"Bank {bank.name} has disabled mobile alerts by institutional policy; skipping reminder for {reminder.email}.")
                return False
            bank_name = bank.name

    subject, html_body = build_dealer_reminder_email_html(
        dealer_email=reminder.email,
        setup_url=setup_url,
        opt_out_url=opt_out_url,
        bank_name=bank_name
    )

    platform_settings = get_global_email_settings(db)
    if not platform_settings or not platform_settings.mail_server:
        logger.warning("Cannot send dealer alert reminder: Platform global SMTP settings not configured.")
        return False

    try:
        success = await send_email(
            db=db,
            to_emails=[reminder.email],
            subject_template=subject,
            body_template=html_body,
            template_data={},
            email_settings=platform_settings,
            sender_name="Grow Treasury Platform Alerts",
            allow_platform_fallback=True,  # Agreed exception: Grow-branded standing invitation
        )
        if success:
            reminder.reminders_sent = (reminder.reminders_sent or 0) + 1
            reminder.last_reminded_at = datetime.now(timezone.utc)
            db.commit()
            logger.info(f"Dealer alert reminder sent successfully to {reminder.email}")
            return True
        return False
    except Exception as e:
        logger.exception(f"Failed to send dealer alert reminder to {reminder.email}: {e}")
        return False
