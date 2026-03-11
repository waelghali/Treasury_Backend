# app/core/background_tasks.py

import logging
import json
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import exc, and_, or_, func
from app.database import SessionLocal
# Pydantic
from pydantic import EmailStr

from app.schemas.all_schemas import SystemNotificationCreate
from app.crud.crud import crud_system_notification
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Application Imports
import app.models as models
from app.core.email_service import (
    get_customer_email_settings, 
    send_email
)
from app.crud.crud import (
    crud_customer,
    crud_customer_configuration,
    crud_lg_instruction,
    crud_user,
    crud_template,
    crud_lg_record,
    log_action,
)
from app.constants import (
    GlobalConfigKey,
    UserRole,
    LgStatusEnum,
    SubscriptionStatus,
    SubscriptionNotificationType,
    ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT,
    AUDIT_ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT_SENT,
    AUDIT_ACTION_TYPE_PRINT_REMINDER_SENT,
    AUDIT_ACTION_TYPE_PRINT_ESCALATION_SENT,
    ACTION_TYPE_LG_RELEASE,
    ACTION_TYPE_LG_LIQUIDATE,
    ACTION_TYPE_LG_DECREASE_AMOUNT,
    ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
    # A1/A2/A3/A7 new constants
    AUDIT_ACTION_TYPE_ISSUANCE_LG_EXPIRY_REMINDER_FIRST,
    AUDIT_ACTION_TYPE_ISSUANCE_LG_EXPIRY_REMINDER_SECOND,
    AUDIT_ACTION_TYPE_ISSUANCE_LG_EXPIRED_NOTIFICATION,
    AUDIT_ACTION_TYPE_REFERENCE_EXPIRY_REMINDER,
    AUDIT_ACTION_TYPE_LG_REFERENCE_VALIDITY_FLAGGED,
    AUDIT_ACTION_TYPE_FACILITY_UTILIZATION_ALERT,
)

import pytz
EEST_TIMEZONE = pytz.timezone('Africa/Cairo')

# Configuration
logger = logging.getLogger(__name__)


# --- Helper Functions ---

def _get_common_cc_emails(db: Session, customer_id: int) -> List[str]:
    """Helper to parse the common communication list config."""
    config = crud_customer_configuration.get_customer_config_or_global_fallback(
        db, customer_id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
    )
    cc_emails = []
    if config and config.get('effective_value'):
        try:
            parsed = json.loads(config['effective_value'])
            if isinstance(parsed, list):
                cc_emails = [e for e in parsed if isinstance(e, str) and "@" in e]
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON in COMMON_COMMUNICATION_LIST for customer {customer_id}.")
    return list(set(cc_emails))

def _get_int_config(db: Session, customer_id: int, key: GlobalConfigKey, default: int = None) -> Optional[int]:
    """Helper to safely get an integer configuration value."""
    config = crud_customer_configuration.get_customer_config_or_global_fallback(db, customer_id, key)
    try:
        if config and config.get('effective_value') is not None:
            return int(config['effective_value'])
    except ValueError:
        logger.error(f"Invalid integer for config '{key.value}' for customer {customer_id}.")
    return default


# --- Background Tasks ---

async def run_daily_undelivered_instructions_report(db: Session):
    """
    Identifies undelivered LG instructions and emails Corporate Admins.
    """
    logger.info("Starting task: Undelivered LG Instructions Report.")
    
    customers = crud_customer.get_all(db)
    if not customers:
        logger.info("No active customers found.")
        return

    # Check template existence once
    template_name = "Undelivered LG Instructions Notification"
    notification_template = crud_template.get_by_name_and_action_type(
        db, name=template_name,
        action_type=ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT,
        customer_id=None, is_notification_template=True
    )

    if not notification_template:
        logger.error(f"Missing global template: {template_name}")
        log_action(db, None, "REPORT_GENERATION_FAILED", "System", None, 
                   {"reason": "Missing email template"}, None, None)
        db.commit()
        return

    for customer in customers:
        try:
            # 1. Fetch Configuration
            start_days = _get_int_config(db, customer.id, GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_REPORT_UNDELIVERED)
            stop_days = _get_int_config(db, customer.id, GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_STOP_REPORTING_UNDELIVERED)

            if start_days is None or stop_days is None:
                continue # Warning logged in helper
            
            if start_days >= stop_days:
                logger.warning(f"Customer {customer.id}: Report start days ({start_days}) >= stop days ({stop_days}). Skipping.")
                continue

            # 2. Get Data
            undelivered = crud_lg_instruction.get_undelivered_instructions_for_reporting(
                db, customer.id, start_days, stop_days
            )
            if not undelivered:
                continue

            # 3. Get Recipients
            admins = crud_user.get_users_by_role_for_customer(db, customer.id, UserRole.CORPORATE_ADMIN)
            to_emails = [a.email for a in admins if a.email]
            
            if not to_emails:
                logger.warning(f"No Corporate Admins for customer {customer.id}.")
                log_action(db, None, "REPORT_GENERATION_FAILED", "Customer", customer.id, 
                           {"reason": "No Admin emails"}, customer.id, None)
                continue

            cc_emails = _get_common_cc_emails(db, customer.id)

            # 4. Build HTML content
            rows = []
            for inst in undelivered:
                lg = inst.lg_record
                days_pending = (date.today() - inst.instruction_date.date()).days
                rows.append(f"""
                    <tr>
                        <td>{lg.lg_number}</td>
                        <td>{inst.instruction_type}</td>
                        <td>{inst.serial_number}</td>
                        <td>{inst.instruction_date.strftime('%Y-%m-%d')}</td>
                        <td>{days_pending} days</td>
                        <td>{lg.issuing_bank.name if lg.issuing_bank else 'N/A'}</td>
                        <td>{lg.lg_currency.iso_code if lg.lg_currency else 'N/A'} {float(lg.lg_amount):,.2f}</td>
                        <td>{lg.internal_owner_contact.email if lg.internal_owner_contact else 'N/A'}</td>
                    </tr>
                """)
            
            table_html = f"""
                <table border="1" cellpadding="5" cellspacing="0" style="width:100%; border-collapse: collapse;">
                    <thead>
                        <tr>
                            <th>LG Number</th><th>Type</th><th>Serial</th><th>Date</th>
                            <th>Days Pending</th><th>Bank</th><th>Amount</th><th>Owner</th>
                        </tr>
                    </thead>
                    <tbody>{"".join(rows)}</tbody>
                </table>
            """

            # 5. Prepare and Send Email
            email_settings, email_method = get_customer_email_settings(db, customer.id)
            
            template_data = {
                "customer_name": customer.name,
                "report_start_days": start_days,
                "report_stop_days": stop_days,
                "undelivered_instructions_count": len(undelivered),
                "undelivered_instructions_table": table_html,
                "current_date": date.today().strftime('%Y-%m-%d'),
                "platform_name": "Treasury Management Platform"
            }

            subject = notification_template.subject.replace("{{customer_name}}", customer.name) \
                             .replace("{{undelivered_instructions_count}}", str(len(undelivered)))
            
            body = notification_template.content
            for k, v in template_data.items():
                body = body.replace(f"{{{{{k}}}}}", str(v) if v is not None else "")

            sent = await send_email(
                db=db, to_emails=to_emails, cc_emails=cc_emails,
                subject_template=subject, body_template=body, template_data=template_data,
                email_settings=email_settings, sender_name=customer.name
            )

            # 6. Audit Log
            audit_type = AUDIT_ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT_SENT if sent else "REPORT_GENERATION_FAILED"
            log_action(
                db, None, audit_type, "Customer", customer.id,
                {
                    "count": len(undelivered),
                    "recipients": to_emails,
                    "method": email_method,
                    "reason": "Email sent" if sent else "Email failed"
                }, customer.id, None
            )

        except Exception as e:
            db.rollback()
            logger.error(f"Error processing undelivered report for customer {customer.id}: {e}", exc_info=True)
        finally:
            db.commit()

    logger.info("Finished task: Undelivered LG Instructions Report.")


async def proactively_correct_customer_configs(global_config_id: int):
    """
    Triggered by API: Re-validates customer configs against global changes.
    Creates its own DB session to ensure connection remains open.
    """
    logger.info(f"Starting config correction for GlobalConfig ID: {global_config_id}")
    
    # NEW: Create a fresh session specifically for this background task
    db = SessionLocal()
    
    try:
        corrections = crud_customer_configuration.revalidate_customer_configs_for_global_change(
            db, global_config_id
        )
        db.commit()

        if corrections:
            # Group by customer
            grouped = {}
            for c in corrections:
                grouped.setdefault(c['customer_id'], []).append(c)

            for cust_id, items in grouped.items():
                await _send_config_correction_notification(db, cust_id, items)
        
        logger.info(f"Config correction complete. Corrected {len(corrections)} entries.")

    except Exception as e:
        db.rollback()
        logger.error(f"Error in config correction: {e}", exc_info=True)
        # We try to log the failure, but if the DB is the cause, this might fail too
        try:
            log_action(db, None, "TASK_PROCESSING_FAILED", "GlobalConfiguration", global_config_id,
                   {"reason": str(e)})
            db.commit()
        except:
            pass
    finally:
        # CRITICAL: Always close the session when the background task is done
        db.close()

async def _send_config_correction_notification(db: Session, customer_id: int, corrections: List[Dict[str, Any]]):
    customer = db.query(models.Customer).filter(models.Customer.id == customer_id).first()
    if not customer: return

    admins = db.query(models.User).filter(
        models.User.customer_id == customer_id,
        models.User.role == UserRole.CORPORATE_ADMIN,
        models.User.is_deleted == False
    ).all()
    
    to_emails = [a.email for a in admins if a.email]
    if not to_emails: return

    rows = "".join([
        f"<tr><td>{c['global_config_key']}</td><td>{c['old_value']}</td><td>{c['new_value']}</td></tr>"
        for c in corrections
    ])

    html_body = f"""
    <html><body>
        <p>Dear Corporate Admin,</p>
        <p>Some configuration settings for {customer.name} have been automatically adjusted to comply with global system limits.</p>
        <table border="1" cellpadding="5" cellspacing="0" style="width:100%; border-collapse: collapse;">
            <thead><tr><th>Config Key</th><th>Old Value</th><th>New Value</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
        <p>No action is required.</p>
    </body></html>
    """

    email_settings, _ = get_customer_email_settings(db, customer.id)
    await send_email(
        db=db, to_emails=to_emails, 
        subject_template=f"System Notification: Configuration Update for {customer.name}",
        body_template=html_body, template_data={}, 
        email_settings=email_settings, sender_name=customer.name
    )


async def run_daily_print_reminders(db: Session):
    logger.info("Running daily print reminders and escalation task...")
    TYPES_TO_PRINT = [
        ACTION_TYPE_LG_RELEASE, ACTION_TYPE_LG_LIQUIDATE,
        ACTION_TYPE_LG_DECREASE_AMOUNT, ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE
    ]

    customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()
    if not customers: return

    for customer in customers:
        try:
            # 1. Check Configs
            d_remind = _get_int_config(db, customer.id, GlobalConfigKey.DAYS_FOR_FIRST_PRINT_REMINDER, 2)
            d_escalate = _get_int_config(db, customer.id, GlobalConfigKey.DAYS_FOR_PRINT_ESCALATION, 5)

            if not (0 < d_remind < d_escalate):
                logger.warning(f"Invalid print reminder config for Customer {customer.id} ({d_remind} vs {d_escalate}). Skipping.")
                continue

            # 2. Find Approved Requests
            requests = db.query(models.ApprovalRequest).filter(
                models.ApprovalRequest.customer_id == customer.id,
                models.ApprovalRequest.status == models.ApprovalRequestStatusEnum.APPROVED,
                models.ApprovalRequest.entity_type == "LGRecord",
                models.ApprovalRequest.action_type.in_(TYPES_TO_PRINT),
                models.ApprovalRequest.related_instruction_id.isnot(None)
            ).options(
                selectinload(models.ApprovalRequest.related_instruction).selectinload(models.LGInstruction.lg_record).selectinload(models.LGRecord.lg_currency),
                selectinload(models.ApprovalRequest.maker_user),
                selectinload(models.ApprovalRequest.checker_user),
            ).all()

            # 1. This is "Aware" (has Egypt Timezone)
            current_time = datetime.now(EEST_TIMEZONE)

            for req in requests:
                inst = req.related_instruction
                if not inst or inst.is_printed or not req.maker_user:
                    continue

                # 2. Get the date from the database
                created_at = inst.instruction_date
                
                # 3. If it's a simple 'date', turn it into a 'datetime' first
                if isinstance(created_at, date) and not isinstance(created_at, datetime):
                    created_at = datetime.combine(created_at, datetime.min.time())

                # 4. If it has no timezone (Naive), give it the Egypt Timezone (Aware)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=EEST_TIMEZONE)
                # If it already has a timezone, move it to Egypt Timezone to be sure
                else:
                    created_at = created_at.astimezone(EEST_TIMEZONE)

                # 5. NOW they match perfectly. Both are Datetimes, both are Egypt Time.
                days_old = (current_time - created_at).days
                
                req_details = req.request_details or {}
                status = req_details.get("print_notification_status", "NONE")

                # 3. Determine Action
                action_mode = None # "REMIND" or "ESCALATE"
                
                if days_old >= d_escalate and status in ["REMINDER_SENT", "NONE"]:
                    action_mode = "ESCALATE"
                elif days_old >= d_remind and status == "NONE":
                    action_mode = "REMIND"
                
                if not action_mode:
                    continue

                # 4. Prepare Notification
                is_escalation = (action_mode == "ESCALATE")
                template_key = "PRINT_ESCALATION" if is_escalation else "PRINT_REMINDER"
                audit_type = AUDIT_ACTION_TYPE_PRINT_ESCALATION_SENT if is_escalation else AUDIT_ACTION_TYPE_PRINT_REMINDER_SENT
                
                # Recipients
                to_emails = [req.maker_user.email]
                cc_emails = []
                if is_escalation and req.checker_user:
                    cc_emails.append(req.checker_user.email)
                
                cc_emails.extend(_get_common_cc_emails(db, customer.id))
                cc_emails = list(set(cc_emails))

                # Template
                template = crud_template.get_by_name_and_action_type(
                    db, name=template_key.replace('_', ' ').title(),
                    action_type=template_key, customer_id=None, is_notification_template=True
                )
                
                if not template:
                    logger.error(f"Missing template '{template_key}' for customer {customer.id}")
                    continue

                # Build Email
                email_data = {
                    "maker_email": req.maker_user.email,
                    "maker_name": req.maker_user.email.split('@')[0],
                    "checker_email": req.checker_user.email if req.checker_user else "N/A",
                    "approval_request_id": req.id,
                    "action_type": req.action_type.replace('_', ' ').title(),
                    "lg_number": inst.lg_record.lg_number if inst.lg_record else "N/A",
                    "instruction_serial_number": inst.serial_number,
                    "days_overdue": days_old,
                    "customer_name": customer.name,
                    "platform_name": "Treasury Management Platform",
                    "print_link": f"/api/v1/end-user/lg-records/instructions/{inst.id}/view-letter?print=true",
                }

                body = template.content
                subject = template.subject or f"Print Reminder for LG #{{lg_number}}"
                for k, v in email_data.items():
                    val = str(v) if v is not None else ""
                    body = body.replace(f"{{{{{k}}}}}", val)
                    subject = subject.replace(f"{{{{{k}}}}}", val)

                # Send
                email_settings, email_method = get_customer_email_settings(db, customer.id)
                sent = await send_email(
                    db=db, to_emails=to_emails, cc_emails=cc_emails,
                    subject_template=subject, body_template=body, template_data=email_data,
                    email_settings=email_settings, sender_name=customer.name
                )

                if sent:
                    req_details["print_notification_status"] = "ESCALATION_SENT" if is_escalation else "REMINDER_SENT"
                    req.request_details = req_details
                    db.add(req)
                    
                    log_action(db, None, audit_type, "ApprovalRequest", req.id, {
                        "recipient": to_emails,
                        "notification_type": template_key,
                        "days_overdue": days_old
                    }, customer.id, req.entity_id)
                    db.flush() # Flush to save status update immediately
                else:
                    logger.error(f"Failed to send {template_key} for Request {req.id}")

        except Exception as e:
            db.rollback()
            logger.error(f"Error in print reminders for customer {customer.id}: {e}", exc_info=True)
            log_action(db, None, "TASK_PROCESSING_FAILED", "Customer", customer.id, {"reason": str(e)})

    logger.info("Print reminders task completed.")


async def run_daily_renewal_reminders(db: Session):
    """
    Orchestrates the daily execution of renewal reminders.
    """
    logger.info("Starting renewal reminders.")
    
    # Feature 1: Reminders to Users/Admins
    try:
        await crud_lg_record.run_renewal_reminders_to_users_and_admins(db)
    except Exception as e:
        db.rollback()
        logger.error(f"Error in Feature 1 (Users/Admins) reminders: {e}", exc_info=True)

    # Feature 2: Reminders to Internal Owners
    try:
        await crud_lg_record.run_internal_owner_renewal_reminders(db)
    except Exception as e:
        db.rollback()
        logger.error(f"Error in Feature 2 (Internal Owners) reminders: {e}", exc_info=True)

    logger.info("Renewal reminders completed.")


async def run_daily_subscription_status_update(db: Session):
    """
    Checks and updates subscription status, sending notifications for expiration/grace.
    """
    logger.info("Starting subscription status update.")
    
    customers = crud_customer.get_all(db)
    today = datetime.now(EEST_TIMEZONE).date()

    for customer in customers:
        try:
            # Refresh data
            customer = crud_customer.get_with_relations(db, customer.id)
            if not customer or not customer.subscription_plan:
                continue

            days_left = (customer.end_date.date() - today).days

            # 1. Pre-Expiry Reminders
            if days_left == 30:
                await _send_sub_notification(db, customer, SubscriptionNotificationType.RENEWAL_REMINDER_30_DAYS,
                    f"Subscription Renewal Reminder: {customer.name}",
                    f"Your subscription expires in 30 days on {customer.end_date.date()}.")
            elif days_left == 7:
                await _send_sub_notification(db, customer, SubscriptionNotificationType.RENEWAL_REMINDER_7_DAYS,
                    f"Urgent: Subscription Expiring Soon for {customer.name}",
                    f"Your subscription expires in 7 days on {customer.end_date.date()}.")

            # 2. Status Updates
            new_status = customer.status
            
            if today > customer.end_date.date():
                grace_days = _get_int_config(db, customer.id, GlobalConfigKey.GRACE_PERIOD_DAYS, 30)
                
                if (today - customer.end_date.date()).days <= grace_days:
                    new_status = SubscriptionStatus.GRACE
                else:
                    new_status = SubscriptionStatus.EXPIRED
            else:
                new_status = SubscriptionStatus.ACTIVE

            if new_status != customer.status:
                prev_status = customer.status
                customer.status = new_status
                db.add(customer)
                db.flush()

                if new_status == SubscriptionStatus.GRACE:
                    await _send_sub_notification(db, customer, SubscriptionNotificationType.GRACE_PERIOD_START,
                        f"Subscription Expired: Grace Period Started",
                        f"Your subscription expired. You are in a read-only grace period.")
                elif new_status == SubscriptionStatus.EXPIRED:
                    await _send_sub_notification(db, customer, SubscriptionNotificationType.EXPIRED,
                        f"Subscription Fully Expired",
                        f"Account locked. Grace period has passed.")
                
                logger.info(f"Customer {customer.id} status changed: {prev_status} -> {new_status}")

            db.commit()

        except Exception as e:
            db.rollback()
            logger.error(f"Error updating subscription for customer {customer.id}: {e}", exc_info=True)
    
    logger.info("Subscription status update completed.")


async def _send_sub_notification(db: Session, customer: models.Customer, type_enum: SubscriptionNotificationType, subject: str, body_text: str):
    """Internal helper to send subscription emails."""
    admins = crud_user.get_users_by_role_for_customer(db, customer.id, UserRole.CORPORATE_ADMIN)
    to_emails = [a.email for a in admins if a.email]
    
    if not to_emails:
        return

    email_settings, method = get_customer_email_settings(db, customer.id)
    sent, error_reason = await send_email(
        db=db, to_emails=to_emails, subject_template=subject,
        body_template=body_text, template_data={},
        email_settings=email_settings, sender_name=customer.name
    )

    log_action(
        db, None, f"NOTIFICATION_{'SENT' if sent else 'FAILED'}_{type_enum.value}", 
        "Customer", customer.id,
        {"recipients": to_emails, "subject": subject, "reason": error_reason if not sent else None}, customer.id
    )


async def run_daily_lg_status_update(db: Session):
    """
    Updates LG records to 'EXPIRED' if past expiry date.
    A2: Also sends notification when an LG expires.
    """
    logger.info("Starting LG status update to EXPIRED.")
    
    today = datetime.now(EEST_TIMEZONE).date()
    
    # Batch query for expired LGs with eager-loaded relationships
    expired_lgs = db.query(models.LGRecord).filter(
        models.LGRecord.expiry_date < today,
        models.LGRecord.lg_status_id == LgStatusEnum.VALID.value,
        models.LGRecord.is_deleted == False
    ).options(
        selectinload(models.LGRecord.customer),
        selectinload(models.LGRecord.lg_currency),
    ).all()
    
    if not expired_lgs:
        logger.info("No expired LGs found.")
        return

    count = 0
    for lg in expired_lgs:
        try:
            lg.lg_status_id = LgStatusEnum.EXPIRED.value
            db.add(lg)
            
            log_action(
                db, None, "LG_STATUS_UPDATE_EXPIRED", "LGRecord", lg.id,
                {"old_status": "VALID", "new_status": "EXPIRED", "reason": "Expiry date passed"},
                lg.customer_id, lg.id
            )

            # A2: Send in-app notification for expired LG
            try:
                users = db.query(models.User).filter(
                    models.User.customer_id == lg.customer_id,
                    models.User.is_deleted == False,
                    models.User.role.in_([models.UserRole.END_USER, models.UserRole.CORPORATE_ADMIN])
                ).all()
                start_dt = datetime.now() - timedelta(days=1)
                end_dt = datetime.now() + timedelta(days=30)
                for user in users:
                    notif = SystemNotificationCreate(
                        content=f"LG {lg.lg_number} ({lg.lg_currency.iso_code if lg.lg_currency else ''} {float(lg.lg_amount):,.2f}) has expired.",
                        notification_type="LG_EXPIRED",
                        link=f"/lg-records/{lg.id}",
                        start_date=start_dt,
                        end_date=end_dt,
                        target_user_ids=[user.id],
                        target_customer_ids=[lg.customer_id],
                        display_frequency="once-per-login",
                    )
                    crud_system_notification.create(db, obj_in=notif, user_id=1)
                log_action(
                    db, None, AUDIT_ACTION_TYPE_ISSUANCE_LG_EXPIRED_NOTIFICATION,
                    "LGRecord", lg.id,
                    {"lg_number": lg.lg_number, "notified_users": len(users)},
                    lg.customer_id, lg.id
                )
            except Exception as notif_err:
                logger.error(f"Failed to send expiry notification for LG {lg.id}: {notif_err}")

            count += 1
        except Exception as e:
            logger.error(f"Failed to expire LG {lg.id}: {e}")
            # Don't break the loop, try next LG

    db.commit()
    logger.info(f"Updated {count} LG records to EXPIRED.")



async def run_hourly_cbe_news_sync(db: Session):
    """
    This task automatically grabs news from the CBE website
    and puts it into your system notifications.
    """
    logger.info("Starting CBE News Scraper...")
    
    url = "https://www.cbe.org.eg/ar/news-publications/news"
    base_url = "https://www.cbe.org.eg"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}

    try:
        # 1. Get the news from the website
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        news_items = soup.find_all("h3")

        for item in news_items[:3]:
            link_tag = item.find("a")
            if not link_tag:
                continue

            title = item.get_text(strip=True)
            link = urljoin(base_url, link_tag.get("href"))

            # 2. Check if we already added this news (to avoid duplicates)
            exists = db.query(models.SystemNotification).filter(
                models.SystemNotification.link == link,
                models.SystemNotification.is_deleted == False
            ).first()

            if not exists:
                # 3. Prepare the notification settings
                # Start is set to yesterday to ensure it shows up immediately
                start_dt = datetime.now() - timedelta(days=1) 
                # End is set to 1 week from now
                end_dt = datetime.now() + timedelta(days=7)

                new_notif = SystemNotificationCreate(
                    content=title,
                    link=link,
                    start_date=start_dt,
                    end_date=end_dt,
                    is_active=True,
                    notification_type="cbe",
                    animation_type="fade",
                    display_frequency="repeat-x-times",
                    max_display_count=2,
                    is_popup=False,
                    popup_action_label="Acknowledge",
                    target_customer_ids=[], # Empty means 'Show to All Customers'
                    target_user_ids=[],     # Empty means 'Show to All Users'
                    target_roles=[]         # Empty means 'Show to All Roles'
                )

                # 4. Save it to your database
                crud_system_notification.create(
                    db, 
                    obj_in=new_notif, 
                    user_id=1 # Using System Admin ID
                )
                
                # 3. Prepare the notification settings
                # Start is set to yesterday to ensure it shows up immediately
                start_dt = datetime.now() - timedelta(days=1) 
                # End is set to 1 week from now
                end_dt = datetime.now() + timedelta(days=7)

                new_notif = SystemNotificationCreate(
                    content=title,
                    link=link,
                    start_date=start_dt,
                    end_date=end_dt,
                    is_active=True,
                    notification_type="cbe",
                    animation_type="fade",
                    display_frequency="repeat-x-times",
                    max_display_count=1,
                    is_popup=True,
                    popup_action_label="Acknowledge",
                    image_url="gs://lg_custody_bucket/system_notifications/images/CBE_Logo.jpg",
                    target_customer_ids=[], # Empty means 'Show to All Customers'
                    target_user_ids=[],     # Empty means 'Show to All Users'
                    target_roles=[]         # Empty means 'Show to All Roles'
                )

                # 4. Save it to your database
                crud_system_notification.create(
                    db, 
                    obj_in=new_notif, 
                    user_id=1 # Using System Admin ID
                )
        
        db.commit()
        logger.info("CBE News Sync complete.")

    except Exception as e:
        db.rollback()
        logger.error(f"Failed to sync CBE news: {e}")


async def run_daily_exchange_rate_sync(db: Session):
    logger.info("Starting Daily CBE Exchange Rate Sync...")
    
    url = "https://www.cbe.org.eg/en/economic-research/statistics/cbe-exchange-rates"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Map CBE names to your Currency.iso_code
    mapping = {
        "US Dollar": "USD", "Euro": "EUR", "Pound Sterling": "GBP",
        "Swiss Franc": "CHF", "Japanese Yen 100": "JPY", "Saudi Riyal": "SAR",
        "Kuwaiti Dinar": "KWD", "UAE Dirham": "AED", "Chinese Yuan": "CNY",
        "Canadian Dollar": "CAD", "Danish Krone": "DKK", "Norwegian Krone": "NOK",
        "Swedish Krona": "SEK", "Australian Dollar": "AUD", "Bahraini Dinar": "BHD",
        "Omani Riyal": "OMR", "Qatari Riyal": "QAR", "Jordanian Dinar": "JOD"
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        table = soup.find("table")
        if not table:
            logger.error("CBE Exchange Rate table not found on page.")
            return

        rows = table.find_all("tr")
        today_date = datetime.now(EEST_TIMEZONE).date()
        sync_count = 0

        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 3:
                cbe_name = cols[0].get_text(strip=True)
                iso_code = mapping.get(cbe_name)

                if not iso_code:
                    continue # Skip if currency not in our mapping

                try:
                    buy = float(cols[1].get_text(strip=True))
                    sell = float(cols[2].get_text(strip=True))
                except ValueError:
                    continue # Skip if rates are not numbers (headers)

                # 1. Get the currency ID from your existing table
                currency = db.query(models.Currency).filter(models.Currency.iso_code == iso_code).first()
                if not currency:
                    logger.warning(f"Currency {iso_code} found on CBE but not in our database. Skipping.")
                    continue

                # 2. Prevent Duplication: Check if rate already exists for today
                existing_rate = db.query(models.CurrencyExchangeRate).filter(
                    models.CurrencyExchangeRate.currency_id == currency.id,
                    models.CurrencyExchangeRate.rate_date == today_date
                ).first()

                if not existing_rate:
                    new_rate = models.CurrencyExchangeRate(
                        currency_id=currency.id,
                        buy_rate=buy,
                        sell_rate=sell,
                        rate_date=today_date
                    )
                    db.add(new_rate)
                    sync_count += 1
        
        db.commit()
        logger.info(f"CBE Exchange Rate Sync complete. Added {sync_count} new rates.")

    except Exception as e:
        db.rollback()
        logger.error(f"Error syncing CBE exchange rates: {e}", exc_info=True)


# ==============================================================================
# A1: ISSUANCE LG EXPIRY REMINDERS
# ==============================================================================

async def run_daily_issuance_lg_expiry_reminders(db: Session):
    """
    A1: Sends 2-tier expiry reminders for Issued LGs (issuance module).
    Mirrors the custody reminder logic from Feature 1.
    Uses ISSUANCE_LG_EXPIRY_FIRST_REMINDER_DAYS / SECOND / INTERVAL configs.
    """
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest
    logger.info("--- START: Issuance LG Expiry Reminders ---")
    current_date_only = date.today()

    customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()
    if not customers:
        logger.info("No active customers found.")
        return

    for customer in customers:
        try:
            first_cfg = _get_int_config(db, customer.id, GlobalConfigKey.ISSUANCE_LG_EXPIRY_FIRST_REMINDER_DAYS, 30)
            second_cfg = _get_int_config(db, customer.id, GlobalConfigKey.ISSUANCE_LG_EXPIRY_SECOND_REMINDER_DAYS, 14)
            interval_cfg = _get_int_config(db, customer.id, GlobalConfigKey.ISSUANCE_LG_EXPIRY_REMINDER_INTERVAL, 7)

            logger.info(f"[Customer: {customer.name}] Issuance expiry config: 1st={first_cfg}d, 2nd={second_cfg}d, interval={interval_cfg}d")

            eligible_lgs = db.query(IssuedLGRecord).filter(
                IssuedLGRecord.customer_id == customer.id,
                IssuedLGRecord.status == "ACTIVE",
                IssuedLGRecord.expiry_date != None,
                IssuedLGRecord.expiry_date >= current_date_only
            ).options(
                selectinload(IssuedLGRecord.customer),
                selectinload(IssuedLGRecord.currency),
                selectinload(IssuedLGRecord.bank),
                selectinload(IssuedLGRecord.current_owner),
            ).all()

            logger.info(f"[Customer: {customer.name}] Found {len(eligible_lgs)} active issued LGs.")

            start_dt = datetime.now() - timedelta(days=1)
            end_dt = datetime.now() + timedelta(days=30)

            for lg in eligible_lgs:
                lg_expiry_val = lg.expiry_date.date() if hasattr(lg.expiry_date, 'date') and callable(lg.expiry_date.date) else lg.expiry_date
                days_left = (lg_expiry_val - current_date_only).days

                is_urgent = days_left <= second_cfg
                is_normal = days_left <= first_cfg

                if not is_normal:
                    continue

                # Determine which tier and audit type
                if is_urgent:
                    audit_type = AUDIT_ACTION_TYPE_ISSUANCE_LG_EXPIRY_REMINDER_SECOND
                    prefix = "URGENT: "
                else:
                    audit_type = AUDIT_ACTION_TYPE_ISSUANCE_LG_EXPIRY_REMINDER_FIRST
                    prefix = ""

                # Anti-spam check
                last_reminder = db.query(models.AuditLog).filter(
                    models.AuditLog.entity_id == lg.id,
                    models.AuditLog.entity_type == "IssuedLGRecord",
                    models.AuditLog.action_type == audit_type
                ).order_by(models.AuditLog.timestamp.desc()).first()

                should_send = False
                if not last_reminder:
                    should_send = True
                else:
                    last_date = last_reminder.timestamp.date() if hasattr(last_reminder.timestamp, 'date') else last_reminder.timestamp
                    if (current_date_only - last_date).days >= interval_cfg:
                        should_send = True

                if should_send:
                    logger.info(f" -> Issued LG {lg.lg_ref_number}: Sending {prefix or 'NORMAL '}expiry reminder. {days_left} days left.")
                    # Create in-app notification
                    users = db.query(models.User).filter(
                        models.User.customer_id == customer.id,
                        models.User.is_deleted == False,
                        models.User.role.in_([models.UserRole.END_USER, models.UserRole.CORPORATE_ADMIN])
                    ).all()
                    for user in users:
                        notif = SystemNotificationCreate(
                            content=f"{prefix}Issued LG {lg.lg_ref_number} ({lg.currency.iso_code if lg.currency else ''} {float(lg.current_amount):,.2f}) expires in {days_left} days.",
                            notification_type="ISSUANCE_LG_EXPIRY",
                            link=f"/issuance/issued-lgs/{lg.id}",
                            start_date=start_dt,
                            end_date=end_dt,
                            target_user_ids=[user.id],
                            target_customer_ids=[customer.id],
                            display_frequency="once-per-login",
                        )
                        crud_system_notification.create(db, obj_in=notif, user_id=1)
                    log_action(
                        db, None, audit_type, "IssuedLGRecord", lg.id,
                        {"lg_ref": lg.lg_ref_number, "days_left": days_left, "tier": "URGENT" if is_urgent else "NORMAL"},
                        customer.id
                    )

        except Exception as e:
            db.rollback()
            logger.error(f"Error in issuance expiry reminders for customer {customer.id}: {e}", exc_info=True)
        finally:
            db.commit()

    logger.info("--- FINISHED: Issuance LG Expiry Reminders ---")


# ==============================================================================
# A3: REFERENCE / CONTRACT EXPIRY CHECK
# ==============================================================================

async def run_daily_reference_expiry_check(db: Session):
    """
    A3: Checks if any Issued LG's expiry date exceeds its originating
    request's reference_end_date (contract validity). If so, flags it.
    Also sends reminder when a reference (contract) is about to expire.
    """
    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest
    logger.info("--- START: Reference Expiry Check ---")
    current_date_only = date.today()

    customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()

    for customer in customers:
        try:
            reminder_days = _get_int_config(db, customer.id, GlobalConfigKey.REFERENCE_EXPIRY_REMINDER_DAYS, 30)

            # Find all active issued LGs with a linked request that has a reference_end_date
            lgs_with_ref = db.query(IssuedLGRecord).join(
                IssuanceRequest, IssuedLGRecord.request_id == IssuanceRequest.id
            ).filter(
                IssuedLGRecord.customer_id == customer.id,
                IssuedLGRecord.status == "ACTIVE",
                IssuanceRequest.reference_end_date != None
            ).options(
                selectinload(IssuedLGRecord.customer),
                selectinload(IssuedLGRecord.currency),
            ).all()

            for lg in lgs_with_ref:
                # Get the originating request's reference_end_date
                request = db.query(IssuanceRequest).filter(IssuanceRequest.id == lg.request_id).first()
                if not request or not request.reference_end_date:
                    continue

                ref_end = request.reference_end_date
                lg_expiry = lg.expiry_date.date() if hasattr(lg.expiry_date, 'date') and callable(lg.expiry_date.date) else lg.expiry_date

                # Flag LGs that extend beyond reference validity
                if lg_expiry and lg_expiry > ref_end:
                    if lg.reference_validity_flag != "EXCEEDED":
                        lg.reference_validity_flag = "EXCEEDED"
                        db.add(lg)
                        log_action(
                            db, None, AUDIT_ACTION_TYPE_LG_REFERENCE_VALIDITY_FLAGGED,
                            "IssuedLGRecord", lg.id,
                            {"lg_ref": lg.lg_ref_number, "lg_expiry": str(lg_expiry),
                             "reference_end": str(ref_end), "reference_type": request.reference_type},
                            customer.id
                        )
                        logger.info(f" -> Flagged LG {lg.lg_ref_number}: expiry {lg_expiry} > reference end {ref_end}")
                else:
                    if lg.reference_validity_flag == "EXCEEDED":
                        lg.reference_validity_flag = "VALID"
                        db.add(lg)

                # Reference expiry reminder
                if ref_end >= current_date_only:
                    days_to_ref_expiry = (ref_end - current_date_only).days
                    if days_to_ref_expiry <= reminder_days:
                        # Anti-spam: check if already sent today
                        last_ref_reminder = db.query(models.AuditLog).filter(
                            models.AuditLog.entity_id == lg.id,
                            models.AuditLog.entity_type == "IssuedLGRecord",
                            models.AuditLog.action_type == AUDIT_ACTION_TYPE_REFERENCE_EXPIRY_REMINDER
                        ).order_by(models.AuditLog.timestamp.desc()).first()

                        send_ref_reminder = False
                        if not last_ref_reminder:
                            send_ref_reminder = True
                        else:
                            last_date = last_ref_reminder.timestamp.date() if hasattr(last_ref_reminder.timestamp, 'date') else last_ref_reminder.timestamp
                            if (current_date_only - last_date).days >= 7:
                                send_ref_reminder = True

                        if send_ref_reminder:
                            start_dt = datetime.now() - timedelta(days=1)
                            end_dt = datetime.now() + timedelta(days=14)
                            users = db.query(models.User).filter(
                                models.User.customer_id == customer.id,
                                models.User.is_deleted == False,
                                models.User.role.in_([models.UserRole.END_USER, models.UserRole.CORPORATE_ADMIN])
                            ).all()
                            for user in users:
                                notif = SystemNotificationCreate(
                                    content=f"{request.reference_type or 'Reference'} '{request.reference_number}' expires in {days_to_ref_expiry} days. LG {lg.lg_ref_number} is linked to it.",
                                    notification_type="REFERENCE_EXPIRY",
                                    link=f"/issuance/issued-lgs/{lg.id}",
                                    start_date=start_dt,
                                    end_date=end_dt,
                                    target_user_ids=[user.id],
                                    target_customer_ids=[customer.id],
                                    display_frequency="once-per-login",
                                )
                                crud_system_notification.create(db, obj_in=notif, user_id=1)
                            log_action(
                                db, None, AUDIT_ACTION_TYPE_REFERENCE_EXPIRY_REMINDER,
                                "IssuedLGRecord", lg.id,
                                {"lg_ref": lg.lg_ref_number, "ref_number": request.reference_number,
                                 "days_to_ref_expiry": days_to_ref_expiry},
                                customer.id
                            )

        except Exception as e:
            db.rollback()
            logger.error(f"Error in reference expiry check for customer {customer.id}: {e}", exc_info=True)
        finally:
            db.commit()

    logger.info("--- FINISHED: Reference Expiry Check ---")


# ==============================================================================
# A7: FACILITY UTILIZATION ALERTS
# ==============================================================================

async def run_daily_facility_utilization_alerts(db: Session):
    """
    A7: Sends notifications when facility utilization reaches 80%, 90%, or 100%.
    Anti-spam: only sends once per threshold crossing (tracked via AuditLog).
    """
    from app.models.models_issuance import IssuanceFacility, IssuanceFacilitySubLimit, IssuanceExposureEntry
    logger.info("--- START: Facility Utilization Alerts ---")

    customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()

    for customer in customers:
        try:
            # Get threshold configs (boolean toggles, all default to True)
            alert_80 = _get_int_config(db, customer.id, GlobalConfigKey.FACILITY_UTILIZATION_ALERT_THRESHOLD_80, 1)
            alert_90 = _get_int_config(db, customer.id, GlobalConfigKey.FACILITY_UTILIZATION_ALERT_THRESHOLD_90, 1)
            alert_100 = _get_int_config(db, customer.id, GlobalConfigKey.FACILITY_UTILIZATION_ALERT_THRESHOLD_100, 1)

            facilities = db.query(IssuanceFacility).filter(
                IssuanceFacility.customer_id == customer.id,
                IssuanceFacility.status == "ACTIVE",
                IssuanceFacility.is_deleted == False
            ).options(
                selectinload(IssuanceFacility.sub_limits),
                selectinload(IssuanceFacility.bank),
                selectinload(IssuanceFacility.currency),
            ).all()

            for facility in facilities:
                # Calculate total utilization across all sub-limits
                total_utilized = db.query(
                    func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0)
                ).filter(
                    IssuanceExposureEntry.facility_id == facility.id,
                    IssuanceExposureEntry.is_active == True
                ).scalar()

                total_limit = float(facility.total_limit_amount) if facility.total_limit_amount else 0
                if total_limit <= 0:
                    continue

                utilization_pct = (float(total_utilized) / total_limit) * 100

                # Check thresholds (highest first)
                thresholds = []
                if alert_100 and utilization_pct >= 100:
                    thresholds.append(100)
                elif alert_90 and utilization_pct >= 90:
                    thresholds.append(90)
                elif alert_80 and utilization_pct >= 80:
                    thresholds.append(80)

                for threshold in thresholds:
                    # Anti-spam: check if already alerted for this threshold
                    already_sent = db.query(models.AuditLog).filter(
                        models.AuditLog.entity_id == facility.id,
                        models.AuditLog.entity_type == "IssuanceFacility",
                        models.AuditLog.action_type == AUDIT_ACTION_TYPE_FACILITY_UTILIZATION_ALERT,
                        models.AuditLog.details["threshold"].astext == str(threshold)
                    ).first()

                    if already_sent:
                        continue

                    logger.info(f" -> Facility '{facility.facility_name}' at {utilization_pct:.1f}% — alerting at {threshold}%")

                    start_dt = datetime.now() - timedelta(days=1)
                    end_dt = datetime.now() + timedelta(days=7)
                    users = db.query(models.User).filter(
                        models.User.customer_id == customer.id,
                        models.User.is_deleted == False,
                        models.User.role.in_([models.UserRole.END_USER, models.UserRole.CORPORATE_ADMIN])
                    ).all()
                    severity = "CRITICAL" if threshold == 100 else "WARNING"
                    for user in users:
                        notif = SystemNotificationCreate(
                            content=f"{severity}: Facility '{facility.facility_name}' ({facility.bank.name if facility.bank else 'N/A'}) is at {utilization_pct:.1f}% utilization (threshold: {threshold}%).",
                            notification_type="FACILITY_UTILIZATION",
                            link=f"/facilities/{facility.id}",
                            start_date=start_dt,
                            end_date=end_dt,
                            target_user_ids=[user.id],
                            target_customer_ids=[customer.id],
                            display_frequency="once-per-login",
                        )
                        crud_system_notification.create(db, obj_in=notif, user_id=1)

                    log_action(
                        db, None, AUDIT_ACTION_TYPE_FACILITY_UTILIZATION_ALERT,
                        "IssuanceFacility", facility.id,
                        {"facility_name": facility.facility_name, "threshold": threshold,
                         "utilization_pct": round(utilization_pct, 2),
                         "utilized": float(total_utilized), "limit": total_limit},
                        customer.id
                    )

        except Exception as e:
            db.rollback()
            logger.error(f"Error in facility utilization alerts for customer {customer.id}: {e}", exc_info=True)
        finally:
            db.commit()

    logger.info("--- FINISHED: Facility Utilization Alerts ---")
