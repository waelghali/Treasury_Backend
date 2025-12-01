# app/core/background_tasks.py

import logging
import json
import pytz
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import exc, and_, or_

# Pydantic
from pydantic import EmailStr

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
)

# Configuration
EEST_TIMEZONE = pytz.timezone('Africa/Cairo')
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


async def proactively_correct_customer_configs(global_config_id: int, db: Session):
    """
    Triggered by API: Re-validates customer configs against global changes.
    """
    logger.info(f"Starting config correction for GlobalConfig ID: {global_config_id}")
    
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
        log_action(db, None, "TASK_PROCESSING_FAILED", "GlobalConfiguration", global_config_id,
                   {"reason": str(e)})
        db.commit()

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

            current_time = datetime.now(EEST_TIMEZONE)

            for req in requests:
                inst = req.related_instruction
                # Skip if already printed or invalid data
                if not inst or inst.is_printed or not req.maker_user:
                    continue

                # Calculate Age
                created_at = inst.instruction_date
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=EEST_TIMEZONE)
                
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
    
    customers = db.query(models.Customer).filter(models.Customer.is_deleted == False).all()
    for customer in customers:
        # Feature 1: Reminders to Users/Admins
        try:
            await crud_lg_record.run_renewal_reminders_to_users_and_admins(db)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"Error Feature 1 reminders (Cust {customer.id}): {e}", exc_info=True)

        # Feature 2: Reminders to Internal Owners
        try:
            await crud_lg_record.run_internal_owner_renewal_reminders(db)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"Error Feature 2 reminders (Cust {customer.id}): {e}", exc_info=True)

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
    sent = await send_email(
        db=db, to_emails=to_emails, subject_template=subject,
        body_template=body_text, template_data={},
        email_settings=email_settings, sender_name=customer.name
    )

    log_action(
        db, None, f"NOTIFICATION_{'SENT' if sent else 'FAILED'}_{type_enum.value}", 
        "Customer", customer.id,
        {"recipients": to_emails, "subject": subject}, customer.id
    )


async def run_daily_lg_status_update(db: Session):
    """
    Updates LG records to 'EXPIRED' if past expiry date.
    """
    logger.info("Starting LG status update to EXPIRED.")
    
    today = datetime.now(EEST_TIMEZONE).date()
    
    # Batch query for expired LGs
    expired_lgs = db.query(models.LGRecord).filter(
        models.LGRecord.expiry_date < today,
        models.LGRecord.lg_status_id == LgStatusEnum.VALID.value,
        models.LGRecord.is_deleted == False
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
            count += 1
        except Exception as e:
            logger.error(f"Failed to expire LG {lg.id}: {e}")
            # Don't break the loop, try next LG

    db.commit()
    logger.info(f"Updated {count} LG records to EXPIRED.")