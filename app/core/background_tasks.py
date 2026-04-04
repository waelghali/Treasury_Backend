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
    AUDIT_ACTION_TYPE_FACILITY_EXPIRY_ALERT,
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
        except Exception:
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
                        display_frequency="once",
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

        # C4: Run FX breach check after rates are updated
        try:
            await _check_fx_breach_auto_suspend(db)
        except Exception as breach_err:
            logger.error(f"FX breach check failed (non-blocking): {breach_err}", exc_info=True)

    except Exception as e:
        db.rollback()
        logger.error(f"Error syncing CBE exchange rates: {e}", exc_info=True)


async def _check_fx_breach_auto_suspend(db: Session):
    """
    C4: FX Breach Auto-Suspend.
    For each active multi-currency facility with fx_breach_auto_suspend=True:
    1. Recalculate total exposure at CURRENT FX rates
    2. If recalculated exposure > facility total limit → suspend the facility
    3. Notify Corp Admin
    
    Runs immediately after exchange rate sync (not as a standalone task).
    """
    from app.models.models_issuance import (
        IssuanceFacility, IssuanceExposureEntry
    )
    from app.services.fx_service import fx_service
    from decimal import Decimal

    logger.info("--- START: FX Breach Auto-Suspend Check ---")

    facilities = db.query(IssuanceFacility).filter(
        IssuanceFacility.status == "ACTIVE",
        IssuanceFacility.multi_currency_allowed == True,
        IssuanceFacility.fx_breach_auto_suspend == True,
        IssuanceFacility.is_deleted == False,
    ).all()

    suspended_count = 0
    for facility in facilities:
        try:
            # Get all active exposure entries
            entries = db.query(IssuanceExposureEntry).filter(
                IssuanceExposureEntry.facility_id == facility.id,
                IssuanceExposureEntry.is_active == True
            ).all()

            if not entries:
                continue

            # Recalculate total exposure at current FX rates
            recalculated_total = Decimal("0")
            for entry in entries:
                if entry.original_currency_id == facility.currency_id:
                    # Same currency — no conversion needed
                    recalculated_total += entry.original_amount_delta or Decimal("0")
                else:
                    # Convert from original currency to facility currency at current rate
                    converted, _ = fx_service.convert(
                        db,
                        Decimal(str(entry.original_amount_delta or 0)),
                        entry.original_currency_id,
                        facility.currency_id,
                        allow_ai=False,
                    )
                    if converted is not None:
                        recalculated_total += converted
                    else:
                        # Can't convert — use the stored equivalent as fallback
                        recalculated_total += entry.facility_equivalent_delta or Decimal("0")

            # Add initial utilization from sub-limits
            for sl in facility.sub_limits:
                recalculated_total += Decimal(str(getattr(sl, 'initial_utilization', 0) or 0))

            # Check breach
            total_limit = Decimal(str(facility.total_limit_amount))
            if recalculated_total > total_limit:
                breach_pct = float((recalculated_total - total_limit) / total_limit * 100)
                logger.warning(
                    f"FX BREACH: Facility {facility.id} ({facility.facility_name}) "
                    f"limit={total_limit}, recalculated={recalculated_total}, "
                    f"breach={breach_pct:.1f}%"
                )

                facility.status = "FX_SUSPENDED"
                db.add(facility)

                # In-app notification to Corp Admin
                try:
                    admins = db.query(models.User).filter(
                        models.User.customer_id == facility.customer_id,
                        models.User.role == "corporate_admin",
                        models.User.is_deleted == False
                    ).all()

                    for admin in admins:
                        notif = SystemNotificationCreate(
                            content=(
                                f"⚠️ Facility '{facility.facility_name}' has been auto-suspended due to FX breach. "
                                f"Current exposure: {float(recalculated_total):,.2f} exceeds limit {float(total_limit):,.2f} "
                                f"(+{breach_pct:.1f}%). Please review and reactivate if appropriate."
                            ),
                            notification_type="FX_BREACH_SUSPEND",
                            link=f"/issuance/facilities/{facility.id}",
                            start_date=datetime.now() - timedelta(days=1),
                            end_date=datetime.now() + timedelta(days=30),
                            target_user_ids=[admin.id],
                            target_customer_ids=[facility.customer_id],
                            display_frequency="once",
                        )
                        crud_system_notification.create(db, obj_in=notif, user_id=1)
                except Exception as notif_err:
                    logger.error(f"Failed to send FX breach notification: {notif_err}")

                log_action(
                    db, None, AUDIT_ACTION_TYPE_FACILITY_UTILIZATION_ALERT,
                    "IssuanceFacility", facility.id,
                    {
                        "reason": "FX_BREACH_AUTO_SUSPEND",
                        "recalculated_total": str(recalculated_total),
                        "total_limit": str(total_limit),
                        "breach_pct": f"{breach_pct:.1f}%"
                    },
                    facility.customer_id
                )
                suspended_count += 1

        except Exception as e:
            logger.error(f"Error checking FX breach for facility {facility.id}: {e}", exc_info=True)

    if suspended_count:
        db.commit()
        logger.warning(f"FX Breach: Suspended {suspended_count} facilities.")
    logger.info("--- END: FX Breach Auto-Suspend Check ---")


# ==============================================================================
# A1: ISSUANCE LG EXPIRY REMINDERS
# ==============================================================================

async def run_daily_issuance_lg_expiry_reminders(db: Session):
    """
    A1: Sends expiry reminders for Issued LGs (issuance module).
    Simplified: single tier using FIRST_REMINDER_DAYS + repeat INTERVAL.
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
            reminder_days = _get_int_config(db, customer.id, GlobalConfigKey.ISSUANCE_LG_EXPIRY_FIRST_REMINDER_DAYS, 30)
            interval_cfg = _get_int_config(db, customer.id, GlobalConfigKey.ISSUANCE_LG_EXPIRY_REMINDER_INTERVAL, 7)

            logger.info(f"[Customer: {customer.name}] Issuance expiry config: start={reminder_days}d, interval={interval_cfg}d")

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

                if days_left > reminder_days:
                    continue

                audit_type = AUDIT_ACTION_TYPE_ISSUANCE_LG_EXPIRY_REMINDER_FIRST

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
                    logger.info(f" -> Issued LG {lg.lg_ref_number}: Sending expiry reminder. {days_left} days left.")
                    users = db.query(models.User).filter(
                        models.User.customer_id == customer.id,
                        models.User.is_deleted == False,
                        models.User.role.in_([models.UserRole.END_USER, models.UserRole.CORPORATE_ADMIN])
                    ).all()
                    for user in users:
                        notif = SystemNotificationCreate(
                            content=f"Issued LG {lg.lg_ref_number} ({lg.currency.iso_code if lg.currency else ''} {float(lg.current_amount):,.2f}) expires in {days_left} days.",
                            notification_type="ISSUANCE_LG_EXPIRY",
                            link=f"/issuance/issued-lgs/{lg.id}",
                            start_date=start_dt,
                            end_date=end_dt,
                            target_user_ids=[user.id],
                            target_customer_ids=[customer.id],
                            display_frequency="once",
                        )
                        crud_system_notification.create(db, obj_in=notif, user_id=1)
                    log_action(
                        db, None, audit_type, "IssuedLGRecord", lg.id,
                        {"lg_ref": lg.lg_ref_number, "days_left": days_left},
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
                                    display_frequency="once",
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

                # --- Facility-level threshold alerts ---
                def _check_and_alert(entity_id, entity_type, entity_name, util_pct, limit_val, utilized_val, link_path):
                    thresholds = []
                    if alert_100 and util_pct >= 100:
                        thresholds.append(100)
                    elif alert_90 and util_pct >= 90:
                        thresholds.append(90)
                    elif alert_80 and util_pct >= 80:
                        thresholds.append(80)

                    for threshold in thresholds:
                        already_sent = db.query(models.AuditLog).filter(
                            models.AuditLog.entity_id == entity_id,
                            models.AuditLog.entity_type == entity_type,
                            models.AuditLog.action_type == AUDIT_ACTION_TYPE_FACILITY_UTILIZATION_ALERT,
                            models.AuditLog.details["threshold"].astext == str(threshold)
                        ).first()

                        if already_sent:
                            continue

                        logger.info(f" -> {entity_type} '{entity_name}' at {util_pct:.1f}% — alerting at {threshold}%")

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
                                content=f"{severity}: {entity_type.replace('Issuance', '')} '{entity_name}' ({facility.bank.name if facility.bank else 'N/A'}) is at {util_pct:.1f}% utilization (threshold: {threshold}%).",
                                notification_type="FACILITY_UTILIZATION",
                                link=link_path,
                                start_date=start_dt,
                                end_date=end_dt,
                                target_user_ids=[user.id],
                                target_customer_ids=[customer.id],
                                display_frequency="once",
                            )
                            crud_system_notification.create(db, obj_in=notif, user_id=1)

                        log_action(
                            db, None, AUDIT_ACTION_TYPE_FACILITY_UTILIZATION_ALERT,
                            entity_type, entity_id,
                            {"name": entity_name, "threshold": threshold,
                             "utilization_pct": round(util_pct, 2),
                             "utilized": float(utilized_val), "limit": float(limit_val)},
                            customer.id
                        )

                # Check facility-level thresholds
                _check_and_alert(
                    facility.id, "IssuanceFacility", facility.facility_name,
                    utilization_pct, total_limit, total_utilized,
                    f"/facilities/{facility.id}"
                )

                # --- Sub-limit level threshold alerts ---
                for sub in facility.sub_limits:
                    sub_limit_val = float(sub.limit_amount) if sub.limit_amount else 0
                    if sub_limit_val <= 0:
                        continue
                    sub_utilized = db.query(
                        func.coalesce(func.sum(IssuanceExposureEntry.facility_equivalent_delta), 0)
                    ).filter(
                        IssuanceExposureEntry.sub_limit_id == sub.id,
                        IssuanceExposureEntry.is_active == True
                    ).scalar()
                    sub_util_pct = (float(sub_utilized) / sub_limit_val) * 100
                    _check_and_alert(
                        sub.id, "IssuanceFacilitySubLimit", sub.limit_name,
                        sub_util_pct, sub_limit_val, sub_utilized,
                        f"/facilities/{facility.id}"
                    )

                # --- Facility review date / expiry date approaching alerts ---
                today = date.today()
                alert_window = timedelta(days=7)

                dates_to_check = []
                if facility.review_date and facility.review_required_flag:
                    dates_to_check.append(("review_date", facility.review_date, "Review Date"))
                if facility.expiry_date:
                    dates_to_check.append(("expiry_date", facility.expiry_date, "Expiry Date"))

                for date_key, date_val, date_label in dates_to_check:
                    days_until = (date_val - today).days
                    if 0 <= days_until <= 7:
                        # Anti-spam: check if already alerted for this specific date
                        already_alerted = db.query(models.AuditLog).filter(
                            models.AuditLog.entity_id == facility.id,
                            models.AuditLog.entity_type == "IssuanceFacility",
                            models.AuditLog.action_type == AUDIT_ACTION_TYPE_FACILITY_EXPIRY_ALERT,
                            models.AuditLog.details["date_key"].astext == date_key,
                            models.AuditLog.details["target_date"].astext == str(date_val),
                        ).first()

                        if already_alerted:
                            continue

                        severity = "CRITICAL" if days_until <= 2 else "WARNING"
                        days_text = "today" if days_until == 0 else f"in {days_until} day{'s' if days_until != 1 else ''}"
                        bank_name = facility.bank.name if facility.bank else "N/A"
                        msg = (f"{severity}: Facility '{facility.facility_name}' ({bank_name}) "
                               f"{date_label} is {days_text} ({date_val.strftime('%d %b %Y')}).")

                        logger.info(f" -> Facility '{facility.facility_name}' {date_label} approaching: {days_until} days")

                        start_dt = datetime.now() - timedelta(days=1)
                        end_dt = datetime.now() + timedelta(days=max(days_until, 1) + 1)
                        users = db.query(models.User).filter(
                            models.User.customer_id == customer.id,
                            models.User.is_deleted == False,
                            models.User.role.in_([models.UserRole.END_USER, models.UserRole.CORPORATE_ADMIN])
                        ).all()

                        for user in users:
                            notif = SystemNotificationCreate(
                                content=msg,
                                notification_type="FACILITY_EXPIRY",
                                link=f"/facilities/{facility.id}",
                                start_date=start_dt,
                                end_date=end_dt,
                                target_user_ids=[user.id],
                                target_customer_ids=[customer.id],
                                display_frequency="once",
                            )
                            crud_system_notification.create(db, obj_in=notif, user_id=1)

                        log_action(
                            db, None, AUDIT_ACTION_TYPE_FACILITY_EXPIRY_ALERT,
                            "IssuanceFacility", facility.id,
                            {"name": facility.facility_name, "date_key": date_key,
                             "target_date": str(date_val), "days_until": days_until},
                            customer.id
                        )

        except Exception as e:
            db.rollback()
            logger.error(f"Error in facility utilization alerts for customer {customer.id}: {e}", exc_info=True)
        finally:
            db.commit()

    logger.info("--- FINISHED: Facility Utilization Alerts ---")


# ==============================================================================
# 12. DAILY SLA BREACH ALERTS (Issuance requests delivered to bank but unfulfilled)
# ==============================================================================

async def run_daily_sla_breach_alerts(db: Session):
    """
    Checks for IssuedLGRecords that have been delivered to the bank
    (status=DELIVERED_TO_BANK) but have no bank_reply_type yet.
    If elapsed days since delivery exceed the facility's sla_agreement_days,
    sends email + in-app notification to end users and corporate admins.
    """
    logger.info("--- STARTING: Daily SLA Breach Alerts ---")

    try:
        from app.models.models_issuance import (
            IssuedLGRecord, IssuanceRequest,
            IssuanceFacility, IssuanceFacilitySubLimit,
        )

        today = date.today()

        # Get all LGs delivered to bank but awaiting reply
        pending_lgs = db.query(IssuedLGRecord).filter(
            IssuedLGRecord.status == "DELIVERED_TO_BANK",
            IssuedLGRecord.bank_reply_type == None,
            IssuedLGRecord.delivery_date != None,
        ).all()

        logger.info(f"SLA check: {len(pending_lgs)} LGs pending bank reply")

        # Group by customer
        customer_breaches = {}
        for lg in pending_lgs:
            elapsed_days = (today - lg.delivery_date).days

            # Get facility SLA
            sla_days = 7  # Default fallback
            if lg.request_id:
                request = db.query(IssuanceRequest).get(lg.request_id)
                if request and request.selected_sub_limit_id:
                    sub_limit = db.query(IssuanceFacilitySubLimit).get(request.selected_sub_limit_id)
                    if sub_limit:
                        facility = db.query(IssuanceFacility).get(sub_limit.facility_id)
                        if facility and facility.sla_agreement_days:
                            sla_days = facility.sla_agreement_days

            if elapsed_days > sla_days:
                cust_id = lg.customer_id
                if cust_id not in customer_breaches:
                    customer_breaches[cust_id] = []
                customer_breaches[cust_id].append({
                    "lg": lg,
                    "sla_days": sla_days,
                    "elapsed_days": elapsed_days,
                    "ref": lg.lg_ref_number,
                })

        logger.info(f"SLA breaches detected for {len(customer_breaches)} customers")

        # Process each customer's breaches
        for cust_id, breaches in customer_breaches.items():
            try:
                customer = db.query(models.Customer).get(cust_id)
                if not customer:
                    continue

                email_settings, _ = get_customer_email_settings(db, cust_id)

                # Build recipient list (end users + corporate admins)
                recipients = db.query(models.User).filter(
                    models.User.customer_id == cust_id,
                    models.User.role.in_([UserRole.END_USER, UserRole.CORPORATE_ADMIN]),
                    models.User.is_deleted == False,
                ).all()

                recipient_emails = [u.email for u in recipients if u.email]
                recipient_ids = [u.id for u in recipients]

                if not recipient_emails:
                    continue

                # Build email body
                lg_rows = "".join([
                    f"<tr><td style='padding:6px;border:1px solid #e5e7eb;'>{b['ref']}</td>"
                    f"<td style='padding:6px;border:1px solid #e5e7eb;text-align:center;'>{b['sla_days']}</td>"
                    f"<td style='padding:6px;border:1px solid #e5e7eb;text-align:center;color:#dc2626;font-weight:bold;'>{b['elapsed_days']}</td></tr>"
                    for b in breaches
                ])

                subject = f"⚠️ SLA Breach Alert — {len(breaches)} LG(s) Awaiting Bank Response"
                body = f"""
                <html>
                <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
                    <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                        <h2 style="color: #dc2626; margin-top: 0;">⚠️ SLA Breach Alert</h2>
                        <p>The following LG issuance requests have exceeded the agreed SLA with the bank and are still pending a response:</p>
                        <table style="width:100%; border-collapse:collapse; margin:15px 0;">
                            <tr style="background:#f9fafb;">
                                <th style="padding:8px;border:1px solid #e5e7eb;text-align:left;">LG Reference</th>
                                <th style="padding:8px;border:1px solid #e5e7eb;text-align:center;">SLA (days)</th>
                                <th style="padding:8px;border:1px solid #e5e7eb;text-align:center;">Elapsed (days)</th>
                            </tr>
                            {lg_rows}
                        </table>
                        <p>Please follow up with the respective bank(s) to expedite the issuance.</p>
                        <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
                        <p style="font-size: 12px; color: #999;">Automated notification from Treasury LG Issuance system.</p>
                    </div>
                </body>
                </html>
                """

                await send_email(db, recipient_emails, subject, body, {}, email_settings)

                # In-App Notifications
                for user_id in recipient_ids:
                    for b in breaches:
                        start_dt = datetime.now()
                        end_dt = start_dt + timedelta(days=1)
                        notif = SystemNotificationCreate(
                            content=f"⚠️ SLA Breach: {b['ref']} — Bank has not responded for {b['elapsed_days']} days (SLA: {b['sla_days']} days). Please follow up.",
                            notification_type="SLA_BREACH",
                            link=f"/issuance/issued-lgs/{b['lg'].id}",
                            start_date=start_dt,
                            end_date=end_dt,
                            target_user_ids=[user_id],
                            target_customer_ids=[cust_id],
                            display_frequency="once",
                        )
                        crud_system_notification.create(db, obj_in=notif, user_id=1)

                # Log audit action
                log_action(
                    db, None, "ISSUANCE_SLA_BREACH_ALERT",
                    "IssuedLGRecord", None,
                    {"customer_id": cust_id, "breach_count": len(breaches),
                     "lg_refs": [b["ref"] for b in breaches]},
                    cust_id
                )
                db.commit()

                logger.info(f"SLA breach alert sent for customer {cust_id}: {len(breaches)} LG(s)")

            except Exception as e:
                db.rollback()
                logger.error(f"Error sending SLA breach alert for customer {cust_id}: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Fatal error in SLA breach alerts: {e}", exc_info=True)

    logger.info("--- FINISHED: Daily SLA Breach Alerts ---")


# ==============================================================================
# C6: RESERVATION TTL & AUTO-EXPIRY
# ==============================================================================

async def run_daily_reservation_ttl_check(db_param: Session = None):
    """
    C6: Checks facility reservations against TTL configuration.
    - Sends reminders at 50% and 80% of TTL
    - Auto-releases expired reservations (> TTL days old)
    - Notifies requestor + corp admin via email and in-app notification
    
    Default TTL: 14 days (configurable per customer via CustomerFormConfiguration.reservation_ttl_days)
    """
    from app.models.models_issuance import (
        IssuanceRequest, IssuanceExposureEntry, CustomerFormConfiguration
    )

    db = db_param or SessionLocal()
    logger.info("--- START: Reservation TTL Check ---")

    DEFAULT_TTL_DAYS = 14
    today = date.today()

    try:
        customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()

        for customer in customers:
            try:
                # Get customer TTL config
                config = db.query(CustomerFormConfiguration).filter(
                    CustomerFormConfiguration.customer_id == customer.id
                ).first()
                ttl_days = getattr(config, 'reservation_ttl_days', None) or DEFAULT_TTL_DAYS

                # Find all FACILITY_RESERVED requests for this customer
                reserved_requests = db.query(IssuanceRequest).filter(
                    IssuanceRequest.customer_id == customer.id,
                    IssuanceRequest.status == "FACILITY_RESERVED",
                ).all()

                for request in reserved_requests:
                    try:
                        # Find the reservation exposure entry
                        reservation = db.query(IssuanceExposureEntry).filter(
                            IssuanceExposureEntry.request_id == request.id,
                            IssuanceExposureEntry.entry_type == "RESERVATION",
                            IssuanceExposureEntry.is_active == True,
                        ).first()

                        if not reservation or not reservation.effective_date:
                            continue

                        days_since = (today - reservation.effective_date).days
                        ttl_50 = int(ttl_days * 0.5)
                        ttl_80 = int(ttl_days * 0.8)

                        # Track notification status on the request
                        notif_status = (request.metadata_json or {}).get("reservation_ttl_status", "NONE")

                        if days_since >= ttl_days:
                            # AUTO-RELEASE
                            reservation.is_active = False
                            db.add(reservation)

                            request.status = "APPROVED_INTERNAL"
                            request.selected_sub_limit_id = None
                            meta = request.metadata_json or {}
                            meta["reservation_ttl_status"] = "AUTO_RELEASED"
                            meta["auto_released_at"] = str(datetime.utcnow())
                            meta["auto_released_days"] = days_since
                            request.metadata_json = meta
                            db.add(request)

                            # Notify via in-app notification
                            await _send_reservation_notification(
                                db, customer, request,
                                f"⏰ Reservation auto-released for request #{request.id} "
                                f"({request.lg_ref_number or 'N/A'}) after {days_since} days. "
                                f"Please re-select a facility to proceed.",
                                "RESERVATION_AUTO_RELEASE"
                            )

                            log_action(
                                db, None, "RESERVATION_AUTO_RELEASED",
                                "IssuanceRequest", request.id,
                                {"days_since": days_since, "ttl_days": ttl_days},
                                customer.id
                            )

                            logger.info(f"Auto-released reservation for request {request.id} (age: {days_since}d, TTL: {ttl_days}d)")

                        elif days_since >= ttl_80 and notif_status in ["NONE", "REMINDER_50"]:
                            # 80% TTL REMINDER
                            remaining = ttl_days - days_since
                            await _send_reservation_notification(
                                db, customer, request,
                                f"⚠️ Reservation for request #{request.id} ({request.lg_ref_number or 'N/A'}) "
                                f"will auto-expire in {remaining} day(s). Please issue or cancel.",
                                "RESERVATION_TTL_WARNING"
                            )
                            meta = request.metadata_json or {}
                            meta["reservation_ttl_status"] = "REMINDER_80"
                            request.metadata_json = meta
                            db.add(request)

                        elif days_since >= ttl_50 and notif_status == "NONE":
                            # 50% TTL REMINDER
                            remaining = ttl_days - days_since
                            await _send_reservation_notification(
                                db, customer, request,
                                f"ℹ️ Reservation for request #{request.id} ({request.lg_ref_number or 'N/A'}) "
                                f"has been held for {days_since} days. {remaining} day(s) until auto-release.",
                                "RESERVATION_TTL_REMINDER"
                            )
                            meta = request.metadata_json or {}
                            meta["reservation_ttl_status"] = "REMINDER_50"
                            request.metadata_json = meta
                            db.add(request)

                    except Exception as req_err:
                        logger.error(f"Error processing reservation TTL for request {request.id}: {req_err}")

                db.commit()

            except Exception as cust_err:
                db.rollback()
                logger.error(f"Error in reservation TTL for customer {customer.id}: {cust_err}", exc_info=True)

    except Exception as e:
        logger.error(f"Fatal error in reservation TTL check: {e}", exc_info=True)
    finally:
        if not db_param:
            db.close()

    logger.info("--- FINISHED: Reservation TTL Check ---")


async def _send_reservation_notification(
    db: Session, customer, request, message: str, notif_type: str
):
    """
    Sends both an in-app notification and email to the requestor + corp admins
    about reservation TTL events.
    """
    # Collect recipients
    recipient_ids = []
    to_emails = []

    # Requestor
    if request.requestor_user_id:
        requestor = db.query(models.User).filter(models.User.id == request.requestor_user_id).first()
        if requestor and not requestor.is_deleted:
            recipient_ids.append(requestor.id)
            if requestor.email:
                to_emails.append(requestor.email)

    # Corp admins
    admins = db.query(models.User).filter(
        models.User.customer_id == customer.id,
        models.User.role == "corporate_admin",
        models.User.is_deleted == False
    ).all()
    for admin in admins:
        if admin.id not in recipient_ids:
            recipient_ids.append(admin.id)
        if admin.email and admin.email not in to_emails:
            to_emails.append(admin.email)

    # In-app notification
    for uid in recipient_ids:
        try:
            notif = SystemNotificationCreate(
                content=message,
                notification_type=notif_type,
                link=f"/issuance/requests/{request.id}",
                start_date=datetime.now() - timedelta(days=1),
                end_date=datetime.now() + timedelta(days=7),
                target_user_ids=[uid],
                target_customer_ids=[customer.id],
                display_frequency="once",
            )
            crud_system_notification.create(db, obj_in=notif, user_id=1)
        except Exception as e:
            logger.error(f"Failed to create reservation notification for user {uid}: {e}")

    # Email notification
    if to_emails:
        try:
            email_settings, _ = get_customer_email_settings(db, customer.id)
            cc_emails = _get_common_cc_emails(db, customer.id)
            await send_email(
                db=db,
                to_emails=to_emails,
                cc_emails=cc_emails,
                subject_template=f"Reservation Alert: {request.lg_ref_number or f'Request #{request.id}'}",
                body_template=f"""
                <html><body>
                    <p>Dear User,</p>
                    <p>{message}</p>
                    <p>Request Reference: {request.lg_ref_number or f'#{request.id}'}</p>
                    <p>Please log in to take action.</p>
                    <p>Best regards,<br>{customer.name} - Treasury Management Platform</p>
                </body></html>
                """,
                template_data={},
                email_settings=email_settings,
                sender_name=customer.name,
            )
        except Exception as e:
            logger.error(f"Failed to send reservation email: {e}")


# ==============================================================================
# F2: MAINTENANCE DELIVERY REMINDERS & ESCALATION
# ==============================================================================

async def run_daily_maintenance_delivery_reminders(db: Session):
    """
    F2: Checks for maintenance actions with instruction_status='Instruction Issued'
    that haven't been delivered to the bank yet.
    
    Reuses the same reminder/escalation config as custody print reminders:
      - DAYS_FOR_FIRST_PRINT_REMINDER (default 2): send first reminder
      - DAYS_FOR_PRINT_ESCALATION (default 5): escalate to Corp Admin
    
    Mirrors the pattern from run_daily_print_reminders() for consistency.
    """
    logger.info("Running daily maintenance delivery reminders...")

    from app.models.models_issuance import IssuanceMaintenanceAction, IssuedLGRecord

    customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()
    if not customers:
        return

    EEST = pytz.timezone('Africa/Cairo')
    current_time = datetime.now(EEST)

    for customer in customers:
        try:
            # 1. Get reminder/escalation config (reuse custody config keys)
            d_remind = _get_int_config(db, customer.id, GlobalConfigKey.DAYS_FOR_FIRST_PRINT_REMINDER, 2)
            d_escalate = _get_int_config(db, customer.id, GlobalConfigKey.DAYS_FOR_PRINT_ESCALATION, 5)

            if not (0 < d_remind < d_escalate):
                continue

            # 2. Find maintenance actions with issued letters not yet delivered
            pending_actions = db.query(IssuanceMaintenanceAction).filter(
                IssuanceMaintenanceAction.instruction_status == "Instruction Issued",
                IssuanceMaintenanceAction.status == "EXECUTED",
                IssuanceMaintenanceAction.is_deleted == False,
            ).join(
                IssuedLGRecord,
                IssuedLGRecord.id == IssuanceMaintenanceAction.issued_lg_id
            ).filter(
                IssuedLGRecord.customer_id == customer.id,
            ).all()

            for action in pending_actions:
                try:
                    # Calculate days since execution
                    created_at = action.updated_at or action.created_at
                    if not created_at:
                        continue
                    
                    if isinstance(created_at, date) and not isinstance(created_at, datetime):
                        created_at = datetime.combine(created_at, datetime.min.time())
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=EEST)
                    else:
                        created_at = created_at.astimezone(EEST)

                    days_old = (current_time - created_at).days

                    # Check notification status (tracked in action_data JSONB)
                    action_data = action.action_data or {}
                    notif_status = action_data.get("delivery_notification_status", "NONE")

                    # 3. Determine action mode
                    action_mode = None
                    if days_old >= d_escalate and notif_status in ["REMINDER_SENT", "NONE"]:
                        action_mode = "ESCALATE"
                    elif days_old >= d_remind and notif_status == "NONE":
                        action_mode = "REMIND"

                    if not action_mode:
                        continue

                    is_escalation = (action_mode == "ESCALATE")

                    # 4. Get LG info for the notification
                    lg = db.query(IssuedLGRecord).filter(
                        IssuedLGRecord.id == action.issued_lg_id
                    ).first()
                    if not lg:
                        continue

                    # 5. Build recipients
                    initiator = db.query(models.User).filter(
                        models.User.id == action.initiated_by_user_id
                    ).first()
                    to_emails = [initiator.email] if initiator and initiator.email else []

                    cc_emails = []
                    if is_escalation:
                        # Escalate: add corp admins
                        admins = db.query(models.User).filter(
                            models.User.customer_id == customer.id,
                            models.User.role == UserRole.CORPORATE_ADMIN,
                            models.User.is_deleted == False,
                        ).all()
                        cc_emails = [a.email for a in admins if a.email]

                    cc_emails.extend(_get_common_cc_emails(db, customer.id))
                    cc_emails = list(set(cc_emails))

                    if not to_emails and not cc_emails:
                        continue

                    # 6. Build email
                    action_label = action.action_type.replace("_", " ").title()
                    lg_ref = lg.lg_ref_number or lg.bank_lg_number or f"LG #{lg.id}"
                    serial = action.letter_serial_number or f"Action #{action.id}"

                    if is_escalation:
                        subject = f"⚠️ Escalation: Maintenance Letter Not Delivered — {lg_ref} ({action_label})"
                        body_intro = f"The following maintenance letter has not been delivered for <strong>{days_old} days</strong>. This requires immediate attention."
                    else:
                        subject = f"Reminder: Maintenance Letter Pending Delivery — {lg_ref} ({action_label})"
                        body_intro = f"A maintenance letter generated <strong>{days_old} days ago</strong> has not yet been marked as delivered to the bank."

                    body = f"""
                    <html>
                    <body style="font-family: 'Segoe UI', sans-serif; color: #333; background-color: #f5f5f5; padding: 20px;">
                        <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                            <h2 style="color: {'#dc2626' if is_escalation else '#f59e0b'}; margin-top: 0;">
                                {'⚠️ Delivery Escalation' if is_escalation else '📋 Delivery Reminder'}
                            </h2>
                            <p>{body_intro}</p>
                            <table style="width:100%; border-collapse:collapse; margin:15px 0;">
                                <tr><td style="padding:8px;border:1px solid #e5e7eb;font-weight:bold;">LG Reference</td>
                                    <td style="padding:8px;border:1px solid #e5e7eb;">{lg_ref}</td></tr>
                                <tr><td style="padding:8px;border:1px solid #e5e7eb;font-weight:bold;">Action Type</td>
                                    <td style="padding:8px;border:1px solid #e5e7eb;">{action_label}</td></tr>
                                <tr><td style="padding:8px;border:1px solid #e5e7eb;font-weight:bold;">Serial Number</td>
                                    <td style="padding:8px;border:1px solid #e5e7eb;">{serial}</td></tr>
                                <tr><td style="padding:8px;border:1px solid #e5e7eb;font-weight:bold;">Days Pending</td>
                                    <td style="padding:8px;border:1px solid #e5e7eb;color:#dc2626;font-weight:bold;">{days_old} days</td></tr>
                            </table>
                            <p>Please print and deliver the letter to the bank, then mark it as delivered in the system.</p>
                            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;" />
                            <p style="font-size: 12px; color: #999;">Automated notification from Treasury LG Issuance system.</p>
                        </div>
                    </body>
                    </html>
                    """

                    # 7. Send email
                    email_settings, _ = get_customer_email_settings(db, customer.id)
                    sent = await send_email(
                        db=db, to_emails=to_emails, cc_emails=cc_emails,
                        subject_template=subject, body_template=body, template_data={},
                        email_settings=email_settings, sender_name=customer.name
                    )

                    # 8. In-app notification
                    user_ids = [action.initiated_by_user_id]
                    if is_escalation:
                        user_ids.extend([a.id for a in admins])
                    user_ids = list(set(uid for uid in user_ids if uid))

                    for uid in user_ids:
                        try:
                            notif = SystemNotificationCreate(
                                content=f"{'⚠️ Escalation' if is_escalation else '📋 Reminder'}: "
                                        f"{action_label} letter for {lg_ref} not delivered ({days_old} days).",
                                notification_type="MAINTENANCE_DELIVERY_REMINDER",
                                start_date=datetime.now(),
                                end_date=datetime.now() + timedelta(days=1),
                                target_user_ids=[uid],
                                target_customer_ids=[customer.id],
                                display_frequency="once",
                            )
                            crud_system_notification.create(db, obj_in=notif, user_id=1)
                        except Exception as e:
                            logger.error(f"Failed to create maintenance reminder notification: {e}")

                    # 9. Update notification status
                    if sent:
                        action_data["delivery_notification_status"] = (
                            "ESCALATION_SENT" if is_escalation else "REMINDER_SENT"
                        )
                        action.action_data = dict(action_data)
                        db.add(action)

                        log_action(db, None,
                            f"MAINTENANCE_DELIVERY_{'ESCALATION' if is_escalation else 'REMINDER'}_SENT",
                            "IssuanceMaintenanceAction", action.id,
                            {"days_overdue": days_old, "recipients": to_emails + cc_emails},
                            customer.id
                        )
                        db.flush()

                except Exception as action_err:
                    logger.error(f"Error processing maintenance reminder for action {action.id}: {action_err}")

        except Exception as e:
            db.rollback()
            logger.error(f"Error in maintenance delivery reminders for customer {customer.id}: {e}", exc_info=True)

    logger.info("Maintenance delivery reminders task completed.")


# ──────────────────────────────────────────────────────────────────────────────
# G5: DELAYED RECONCILIATION REMINDERS
# ──────────────────────────────────────────────────────────────────────────────

async def run_daily_reconciliation_reminders(db: Session):
    """
    G5: Checks for banks that haven't been reconciled for too long.

    For each customer+bank with live LGs, checks the most recent COMPLETED
    ReconciliationSession. If none exists or last one exceeds the threshold,
    notifies Corp Admin.

    Reuses config key: DAYS_FOR_RECONCILIATION_REMINDER (default: 60).
    """
    logger.info("Running daily reconciliation reminders...")

    from app.models.models_issuance import ReconciliationSession, IssuedLGRecord

    EEST = pytz.timezone('Africa/Cairo')
    current_time = datetime.now(EEST)

    customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()
    if not customers:
        return

    for customer in customers:
        try:
            # 1. Get threshold from config
            d_threshold = _get_int_config(
                db, customer.id,
                GlobalConfigKey.DAYS_FOR_RECONCILIATION_REMINDER, 60
            )
            if d_threshold <= 0:
                continue

            # 2. Find distinct banks with live LGs for this customer
            live_statuses = [
                "ACTIVE", "LG_ISSUED", "DELIVERED_TO_BANK", "INTERNAL_PROCESSING",
            ]
            bank_ids = db.query(IssuedLGRecord.bank_id).filter(
                IssuedLGRecord.customer_id == customer.id,
                IssuedLGRecord.status.in_(live_statuses),
                IssuedLGRecord.bank_id.isnot(None),
            ).distinct().all()

            bank_ids = [b[0] for b in bank_ids]
            if not bank_ids:
                continue

            overdue_banks = []

            for bank_id in bank_ids:
                # 3. Find most recent COMPLETED reconciliation session for this bank
                last_session = db.query(ReconciliationSession).filter(
                    ReconciliationSession.customer_id == customer.id,
                    ReconciliationSession.bank_id == bank_id,
                    ReconciliationSession.status == "COMPLETED",
                ).order_by(ReconciliationSession.reviewed_at.desc()).first()

                if last_session and last_session.reviewed_at:
                    last_date = last_session.reviewed_at
                    if last_date.tzinfo is None:
                        last_date = last_date.replace(tzinfo=EEST)
                    else:
                        last_date = last_date.astimezone(EEST)
                    days_since = (current_time - last_date).days
                else:
                    days_since = 999  # Never reconciled

                if days_since >= d_threshold:
                    bank = db.query(models.Bank).filter(models.Bank.id == bank_id).first()
                    bank_name = bank.name if bank else f"Bank #{bank_id}"
                    overdue_banks.append({
                        "bank_id": bank_id,
                        "bank_name": bank_name,
                        "days_since": days_since,
                        "last_date": str(last_session.reviewed_at.date()) if (last_session and last_session.reviewed_at) else "Never",
                    })

            if not overdue_banks:
                continue

            # 4. Notify Corp Admins and End Users
            target_users = db.query(models.User).filter(
                models.User.customer_id == customer.id,
                models.User.role.in_(["corporate_admin", "end_user"]),
                models.User.is_deleted == False,
            ).all()

            if not target_users:
                continue

            bank_list = ", ".join(
                f"{b['bank_name']} ({b['days_since']}d)" for b in overdue_banks
            )
            message = (
                f"⚠️ Reconciliation Overdue: {len(overdue_banks)} bank(s) have not been "
                f"reconciled for over {d_threshold} days — {bank_list}"
            )

            target_ids = [u.id for u in target_users]
            to_emails = [u.email for u in target_users if u.email]

            try:
                notif = SystemNotificationCreate(
                    content=message,
                    notification_type="RECONCILIATION_OVERDUE",
                    start_date=datetime.now(),
                    end_date=datetime.now() + timedelta(days=7),
                    target_user_ids=target_ids,
                    target_customer_ids=[customer.id],
                    display_frequency="once",
                )
                crud_system_notification.create(db, obj_in=notif, user_id=1)
                db.flush()
                logger.info(
                    f"Reconciliation overdue notification sent for customer {customer.id}: "
                    f"{len(overdue_banks)} bank(s)"
                )
            except Exception as notif_err:
                logger.error(f"Failed to create reconciliation reminder notification: {notif_err}")

            if to_emails:
                try:
                    email_settings, _ = get_customer_email_settings(db, customer.id)
                    cc_emails = _get_common_cc_emails(db, customer.id)
                    await send_email(
                        db=db,
                        to_emails=to_emails,
                        cc_emails=cc_emails,
                        subject_template="Action Required: LG Position Reconciliation Overdue",
                        body_template=f"<html><body style='font-family: Arial, sans-serif; color: #333; padding: 20px;'><p>Dear Treasury Team,</p><p>{message}</p><p>Please log in to the Treasury Management Platform and upload the latest bank position reports to complete the reconciliation process.</p></body></html>",
                        template_data={},
                        email_settings=email_settings,
                        sender_name=customer.name,
                    )
                except Exception as email_err:
                    logger.error(f"Failed to send reconciliation reminder email: {email_err}")

        except Exception as e:
            db.rollback()
            logger.error(
                f"Error in reconciliation reminders for customer {customer.id}: {e}",
                exc_info=True
            )

    logger.info("Reconciliation reminders task completed.")


async def run_daily_issuance_maintenance_reminders(db: Session):
    """
    Daily background task for issuance maintenance print/delivery reminders.
    Mirrors the custody module's run_daily_print_reminders pattern.

    - Day 1+: Reminder email to the end user who created the action
    - Day 3+: Escalation email to corporate admins + CC common list
    """
    from app.models.models_issuance import IssuanceMaintenanceAction, IssuedLGRecord

    logger.info("Starting task: Issuance Maintenance Reminders.")

    customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()
    if not customers:
        return

    current_time = datetime.now(EEST_TIMEZONE)

    for customer in customers:
        try:
            # Configurable thresholds (defaults: remind after 1 day, escalate after 3)
            d_remind = 1
            d_escalate = 3
            try:
                cfg_r = crud_customer_configuration.get_customer_config_or_global_fallback(
                    db, customer.id, "ISSUANCE_MAINTENANCE_REMINDER_DAYS"
                )
                if cfg_r and cfg_r.get("effective_value"):
                    d_remind = int(cfg_r["effective_value"])
            except Exception:
                db.rollback()
                pass
            try:
                cfg_e = crud_customer_configuration.get_customer_config_or_global_fallback(
                    db, customer.id, "ISSUANCE_MAINTENANCE_ESCALATION_DAYS"
                )
                if cfg_e and cfg_e.get("effective_value"):
                    d_escalate = int(cfg_e["effective_value"])
            except Exception:
                db.rollback()
                pass

            if d_remind >= d_escalate:
                continue

            # Find actions that need reminders:
            # - instruction_status = 'Instruction Issued' (letter sent but not delivered/confirmed)
            # - Not printed
            # - Not already cancelled
            actions = db.query(IssuanceMaintenanceAction).join(
                IssuedLGRecord, IssuanceMaintenanceAction.issued_lg_id == IssuedLGRecord.id
            ).filter(
                IssuedLGRecord.customer_id == customer.id,
                IssuanceMaintenanceAction.status == "EXECUTED",
                IssuanceMaintenanceAction.instruction_status == "Instruction Issued",
                IssuanceMaintenanceAction.is_printed == False,
                IssuanceMaintenanceAction.is_deleted == False,
            ).all()

            if not actions:
                continue

            email_settings, _ = get_customer_email_settings(db, customer.id)
            cc_emails = _get_common_cc_emails(db, customer.id)

            for action in actions:
                try:
                    # Calculate age
                    created_at = action.created_at
                    if isinstance(created_at, date) and not isinstance(created_at, datetime):
                        created_at = datetime.combine(created_at, datetime.min.time())
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=EEST_TIMEZONE)
                    else:
                        created_at = created_at.astimezone(EEST_TIMEZONE)

                    days_old = (current_time - created_at).days

                    # Check notification state
                    data = dict(action.action_data or {})
                    notif_status = data.get("print_notification_status", "NONE")

                    action_mode = None
                    if days_old >= d_escalate and notif_status in ("REMINDER_SENT", "NONE"):
                        action_mode = "ESCALATE"
                    elif days_old >= d_remind and notif_status == "NONE":
                        action_mode = "REMIND"

                    if not action_mode:
                        continue

                    # Get LG ref for email
                    lg = db.query(IssuedLGRecord).filter(IssuedLGRecord.id == action.issued_lg_id).first()
                    ref = lg.lg_ref_number if lg else str(action.issued_lg_id)

                    is_escalation = (action_mode == "ESCALATE")

                    # Recipients
                    to_emails = []
                    if is_escalation:
                        # Escalation goes to corporate admins
                        admins = db.query(models.User).filter(
                            models.User.customer_id == customer.id,
                            models.User.role == UserRole.CORPORATE_ADMIN,
                            models.User.is_deleted == False
                        ).all()
                        to_emails = [a.email for a in admins if a.email]
                    elif action.initiated_by_user_id:
                        # Reminder goes to the action creator
                        user = db.query(models.User).filter(models.User.id == action.initiated_by_user_id).first()
                        if user and user.email:
                            to_emails = [user.email]

                    if not to_emails:
                        continue

                    # Build email
                    color = "#dc2626" if is_escalation else "#b45309"
                    icon = "🚨" if is_escalation else "⏰"
                    label = "ESCALATION" if is_escalation else "Reminder"

                    subject = f"{icon} {label}: LG {ref} — {action.action_type.replace('_', ' ')} letter not yet printed ({days_old} days)"
                    body = f"""
                    <html><body style="font-family: 'Segoe UI', sans-serif; color: #333; padding: 20px;">
                    <div style="max-width: 600px; margin: auto; background: #fff; border-radius: 12px; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);{' border: 2px solid #dc2626;' if is_escalation else ''}">
                        <h2 style="color: {color};">{icon} Maintenance Letter — Print {label}</h2>
                        <p>The <strong>{action.action_type.replace('_', ' ')}</strong> letter for LG <strong>{ref}</strong> was issued <strong>{days_old} days ago</strong> but has not been printed yet.</p>
                        {"<p style='color: #dc2626; font-weight: bold;'>This is an escalation notice. Please ensure the letter is printed and delivered to the bank promptly.</p>" if is_escalation else "<p>Please print and deliver the instruction letter to the bank at your earliest convenience.</p>"}
                        <div style="background: #f8fafc; border-left: 4px solid {color}; padding: 15px; border-radius: 8px; margin: 20px 0;">
                            <table style="width: 100%; border-collapse: collapse;">
                                <tr><td style="padding: 4px 0; color: #666;">LG Reference:</td><td style="padding: 4px 0; font-weight: bold;">{ref}</td></tr>
                                <tr><td style="padding: 4px 0; color: #666;">Action Type:</td><td style="padding: 4px 0;">{action.action_type.replace('_', ' ')}</td></tr>
                                <tr><td style="padding: 4px 0; color: #666;">Days Since Issue:</td><td style="padding: 4px 0; font-weight: bold; color: {color};">{days_old} days</td></tr>
                            </table>
                        </div>
                        <div style="text-align: center; margin: 25px 0;">
                            <a href="{os.getenv('FRONTEND_URL', 'http://localhost:3000')}/corporate-admin/issuance/issued-lgs" style="padding: 12px 30px; background: {color}; color: #fff; text-decoration: none; border-radius: 8px; font-weight: bold;">View in System</a>
                        </div>
                        <hr style="border: none; border-top: 1px solid #eee;" />
                        <p style="font-size: 12px; color: #999;">Automated notification from Treasury LG Issuance system.</p>
                    </div></body></html>
                    """

                    sent = await send_email(
                        db=db, to_emails=to_emails, cc_emails=cc_emails,
                        subject_template=subject, body_template=body, template_data={},
                        email_settings=email_settings, sender_name=customer.name
                    )

                    if sent:
                        data["print_notification_status"] = "ESCALATION_SENT" if is_escalation else "REMINDER_SENT"
                        action.action_data = data
                        db.add(action)

                        log_action(
                            db, None,
                            "ISSUANCE_MAINTENANCE_PRINT_ESCALATION_SENT" if is_escalation else "ISSUANCE_MAINTENANCE_PRINT_REMINDER_SENT",
                            "IssuanceMaintenanceAction", action.id,
                            {"recipient": to_emails, "days_overdue": days_old},
                            customer.id, action.issued_lg_id
                        )
                        db.flush()

                except Exception as action_err:
                    db.rollback()
                    logger.error(f"Error processing reminder for action {action.id}: {action_err}", exc_info=True)

        except Exception as e:
            db.rollback()
            logger.error(f"Error in issuance maintenance reminders for customer {customer.id}: {e}", exc_info=True)
        finally:
            db.commit()

    logger.info("Issuance Maintenance Reminders task completed.")


# ==============================================================================
# AUTO-REJECT EXPIRED CANCEL & EDIT REQUESTS
# ==============================================================================

async def run_daily_issuance_approval_timeout(db: Session):
    """
    Auto-rejects LGs stuck in CANCEL_REQUESTED and IssuanceRequests stuck
    in EDIT_REQUESTED beyond the configured APPROVAL_REQUEST_MAX_PENDING_DAYS.
    Mirrors the ApprovalRequest auto-reject pattern.
    """
    logger.info("Running daily issuance approval timeout check...")

    from app.models.models_issuance import IssuedLGRecord, IssuanceRequest

    # Get max pending days config
    from app.crud.crud import crud_global_configuration
    max_pending_config = crud_global_configuration.get_by_key(
        db, GlobalConfigKey.APPROVAL_REQUEST_MAX_PENDING_DAYS
    )
    if not max_pending_config or not max_pending_config.value_default:
        logger.info("APPROVAL_REQUEST_MAX_PENDING_DAYS not configured. Skipping issuance timeout.")
        return

    try:
        max_days = int(max_pending_config.value_default)
    except (ValueError, TypeError):
        logger.warning("Invalid APPROVAL_REQUEST_MAX_PENDING_DAYS value. Skipping.")
        return

    cutoff = datetime.utcnow() - timedelta(days=max_days)
    auto_rejected_count = 0

    # 1. Auto-reject CANCEL_REQUESTED LGs
    pending_cancels = db.query(IssuedLGRecord).filter(
        IssuedLGRecord.status == "CANCEL_REQUESTED",
    ).all()

    for lg in pending_cancels:
        try:
            meta = dict(lg.metadata_json or {})
            cancel_meta = meta.get("pending_cancellation", {})
            requested_at_str = cancel_meta.get("requested_at")
            if not requested_at_str:
                continue

            requested_at = datetime.fromisoformat(requested_at_str)
            if requested_at > cutoff:
                continue  # Not yet expired

            # Auto-reject: restore previous status
            previous_status = cancel_meta.get("previous_status", "INTERNAL_PROCESSING")
            lg.status = previous_status

            # Clean up metadata
            if "pending_cancellation" in meta:
                del meta["pending_cancellation"]
            lg.metadata_json = meta

            # Audit trail
            ctl = list(lg.custody_transfer_log or [])
            ctl.append({
                "action": "CANCEL_AUTO_REJECTED",
                "reason": f"Auto-rejected: exceeded {max_days} day approval window.",
                "restored_status": previous_status,
                "timestamp": datetime.utcnow().isoformat(),
            })
            lg.custody_transfer_log = ctl

            log_action(db, None, "LG_CANCEL_AUTO_REJECTED",
                       "IssuedLGRecord", lg.id,
                       {"max_days": max_days, "restored_status": previous_status},
                       lg.customer_id)

            # Notify requestor
            try:
                requestor_id = cancel_meta.get("requested_by_user_id")
                if requestor_id:
                    _now = datetime.utcnow()
                    notif = SystemNotificationCreate(
                        content=f"Your cancellation request for LG {lg.lg_ref_number} was automatically "
                                f"rejected — no admin response within {max_days} days.",
                        notification_type="LG_CANCEL_AUTO_REJECTED",
                        start_date=_now,
                        end_date=_now + timedelta(days=30),
                        target_user_ids=[requestor_id],
                        target_customer_ids=[lg.customer_id],
                    )
                    crud_system_notification.create(db, obj_in=notif, user_id=1)
            except Exception:
                pass

            auto_rejected_count += 1
            logger.info(f"Auto-rejected cancel request for LG {lg.id} ({lg.lg_ref_number})")

        except Exception as e:
            logger.error(f"Error auto-rejecting cancel for LG {lg.id}: {e}", exc_info=True)

    # 2. Auto-reject EDIT_REQUESTED IssuanceRequests
    pending_edits = db.query(IssuanceRequest).filter(
        IssuanceRequest.status == "EDIT_REQUESTED",
    ).all()

    for req in pending_edits:
        try:
            meta = dict(req.metadata_json or {})
            edit_meta = meta.get("pending_edit", {})
            requested_at_str = edit_meta.get("requested_at")
            if not requested_at_str:
                continue

            requested_at = datetime.fromisoformat(requested_at_str)
            if requested_at > cutoff:
                continue

            previous_status = edit_meta.get("previous_status", "APPROVED_INTERNAL")
            req.status = previous_status

            if "pending_edit" in meta:
                del meta["pending_edit"]
            req.metadata_json = meta

            audit = list(req.approval_chain_audit or [])
            audit.append({
                "action": "EDIT_AUTO_REJECTED",
                "reason": f"Auto-rejected: exceeded {max_days} day approval window.",
                "restored_status": previous_status,
                "timestamp": datetime.utcnow().isoformat(),
            })
            req.approval_chain_audit = audit

            log_action(db, None, "ISSUANCE_EDIT_AUTO_REJECTED",
                       "IssuanceRequest", req.id,
                       {"max_days": max_days, "restored_status": previous_status},
                       req.customer_id)

            auto_rejected_count += 1
            logger.info(f"Auto-rejected edit request for request {req.id} ({req.serial_number})")

        except Exception as e:
            logger.error(f"Error auto-rejecting edit for request {req.id}: {e}", exc_info=True)

    if auto_rejected_count > 0:
        db.commit()
        logger.info(f"Auto-rejected {auto_rejected_count} expired issuance approval(s).")
    else:
        logger.info("No expired issuance approvals found.")

async def run_daily_auto_reject_expired_requests(db: Session):
    """
    Auto-rejects core system approval requests (Maker-Checker) that have exceeded
    the APPROVAL_REQUEST_MAX_PENDING_DAYS configuration.
    """
    logger.info("Running daily auto-rejection of expired core approval requests...")
    try:
        from app.crud.crud_approval_request import crud_approval_request
        crud_approval_request.auto_reject_expired_requests(db)
    except Exception as e:
        logger.error(f"Error during auto-rejection of expired requests: {e}", exc_info=True)
