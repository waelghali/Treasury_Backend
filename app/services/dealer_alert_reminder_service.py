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
    """
    Builds an ultra-polite, quiet, and reassurance-focused Grow-branded email HTML.
    Free of aggressive marketing words, jargon, or pressure.
    """
    subject = "Enable Instant Phone Alerts for RFQs"
    greeting = f"Hello {bank_name} Treasury Team" if bank_name else "Hello"

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f8fafc; color: #1e293b; margin: 0; padding: 28px 16px; -webkit-font-smoothing: antialiased; }}
    .container {{ max-width: 520px; margin: 0 auto; background: #ffffff; border-radius: 18px; border: 1px solid #e2e8f0; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }}
    .header {{ background: #0f172a; color: #ffffff; padding: 26px 28px; text-align: left; }}
    .header h1 {{ margin: 0; font-size: 17px; font-weight: 600; letter-spacing: -0.01em; color: #ffffff; }}
    .header p {{ margin: 4px 0 0 0; font-size: 12px; color: #94a3b8; font-weight: 400; }}
    .content {{ padding: 28px; line-height: 1.65; font-size: 14px; color: #334155; }}
    .btn-container {{ text-align: center; margin: 26px 0; }}
    .btn {{ display: inline-block; background-color: #059669; color: #ffffff !important; text-decoration: none; padding: 12px 26px; border-radius: 10px; font-weight: 600; font-size: 13px; letter-spacing: 0.01em; }}
    .reassurance-box {{ background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 12px; padding: 14px 16px; margin-top: 22px; font-size: 12px; color: #475569; }}
    .reassurance-box p {{ margin: 0 0 6px 0; font-weight: 600; color: #0f172a; }}
    .reassurance-box ul {{ margin: 0; padding-left: 18px; }}
    .reassurance-box li {{ margin-bottom: 4px; }}
    .footer {{ padding: 20px 28px; background: #f8fafc; border-top: 1px solid #e2e8f0; font-size: 11px; color: #94a3b8; line-height: 1.5; }}
    .footer a {{ color: #64748b; text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Grow Treasury</h1>
      <p>Institutional Quotation Portal</p>
    </div>
    <div class="content">
      <p style="margin-top: 0;">{greeting},</p>
      <p>When clients submit a quotation request, submission windows can sometimes be short.</p>
      <p>If helpful for your desk, you can link your smartphone to receive a quiet sound or vibration alert whenever a new request arrives, without needing to keep the browser portal open all day.</p>
      
      <div class="btn-container">
        <a href="{setup_url}" class="btn" target="_blank">Link Phone for Alerts</a>
      </div>

      <div class="reassurance-box">
        <p>A few quick details:</p>
        <ul>
          <li><strong>Completely optional:</strong> You can always continue quoting through the web portal as usual.</li>
          <li><strong>Confidential:</strong> Trade amounts, rates, and corporate identities are never displayed on your phone lock screen.</li>
          <li><strong>Easy setup:</strong> Requires no app store download, and you can disconnect or turn alerts off at any time.</li>
        </ul>
      </div>
    </div>
    <div class="footer">
      Sent to {dealer_email} regarding your quotation desk participation on Grow.<br>
      To never receive this reminder again, you can <a href="{opt_out_url}">unsubscribe here</a>.
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
    Enforces a strict 45-day cooldown and ensures no active linked device exists.
    """
    if reminder.opted_out:
        logger.info(f"Dealer {reminder.email} has opted out of reminders; skipping.")
        return False

    # Check 45-day cooldown between reminders
    if reminder.last_reminded_at:
        now_utc = datetime.now(timezone.utc)
        last_at = reminder.last_reminded_at if reminder.last_reminded_at.tzinfo else reminder.last_reminded_at.replace(tzinfo=timezone.utc)
        days_since = (now_utc - last_at).days
        if days_since < 45:
            logger.info(f"Dealer {reminder.email} was reminded {days_since} days ago (< 45 days); skipping.")
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

    platform_settings = get_global_email_settings()
    if not platform_settings or not platform_settings.smtp_host:
        logger.warning("Cannot send dealer alert reminder: Platform global SMTP settings not configured.")
        return False

    try:
        success, _ = await send_email(
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


async def process_quiet_times_dealer_reminders(
    db: Session,
    base_url: Optional[str] = None,
    max_batch: int = 50,
) -> int:
    """
    Finds eligible bank dealers and sends the quiet-times reminder email.
    Eligibility criteria (all must be satisfied):
    1. Dealer is active (is_active == True) in QuotationBankDealer.
    2. Bank allows dealer mobile alerts (bank.allow_dealer_mobile_alerts != False).
    3. Dealer has not opted out (DealerAlertReminder.opted_out == False).
    4. Dealer has NO active linked device in PushSubscription (is_active=True, is_deleted=False).
    5. Cooldown: Dealer has never been reminded, or last reminder was >= 45 days ago.
    """
    import asyncio

    dealers = db.query(QuotationBankDealer).filter(
        QuotationBankDealer.is_active == True  # noqa: E712
    ).all()

    sent_count = 0
    now_utc = datetime.now(timezone.utc)

    for dealer in dealers:
        if sent_count >= max_batch:
            break

        email = (dealer.email or "").strip().lower()
        if not email or "@" not in email:
            continue

        # Check bank policy
        if dealer.bank_id:
            bank = db.get(Bank, dealer.bank_id)
            if bank and getattr(bank, "allow_dealer_mobile_alerts", True) is False:
                continue

        # Check if dealer already has an active push device
        has_active_push = db.query(PushSubscription).filter(
            PushSubscription.dealer_email == email,
            PushSubscription.is_active == True,  # noqa: E712
            PushSubscription.is_deleted == False,  # noqa: E712
        ).first()
        if has_active_push:
            continue

        reminder = get_or_create_dealer_reminder(db, email, bank_id=dealer.bank_id)
        if reminder.opted_out:
            continue

        if reminder.last_reminded_at:
            last_at = reminder.last_reminded_at if reminder.last_reminded_at.tzinfo else reminder.last_reminded_at.replace(tzinfo=timezone.utc)
            days_since = (now_utc - last_at).days
            if days_since < 45:
                continue

        success = await send_dealer_reminder_email(db, reminder, base_url=base_url)
        if success:
            sent_count += 1
            await asyncio.sleep(1)  # Respectful pacing between platform emails

    return sent_count
