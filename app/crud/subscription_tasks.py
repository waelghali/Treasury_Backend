# app/crud/subscription_tasks.py

import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, TYPE_CHECKING
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import exc

import app.models as models
from app.constants import (
    GlobalConfigKey,
    UserRole,
    SubscriptionStatus,
    SubscriptionNotificationType,
)
from app.core.email_service import get_customer_email_settings, send_email, EmailSettings, get_global_email_settings
# REMOVED the direct import from app.crud.crud. Instead, these will be passed as arguments.

import pytz

EEST_TIMEZONE = pytz.timezone('Africa/Cairo')
logger = logging.getLogger(__name__)

# To assist with type hinting without causing a circular import, we can use TYPE_CHECKING
if TYPE_CHECKING:
    from app.crud.crud import CRUDBase, log_action, CRUDCustomer, CRUDCustomerConfiguration

async def _send_subscription_notification(
    db: Session,
    log_action: "log_action",
    customer: models.Customer,
    email_type: SubscriptionNotificationType,
    subject: str,
    body: str,
    details: Dict[str, Any]
):
    """Helper function to send a subscription-related notification email."""
    try:
        # Fetch all Corporate Admins for the customer
        corporate_admins = db.query(models.User).filter(
            models.User.customer_id == customer.id,
            models.User.role == UserRole.CORPORATE_ADMIN,
            models.User.is_deleted == False
        ).all()
        to_emails = [admin.email for admin in corporate_admins]

        if not to_emails:
            logger.warning(f"No Corporate Admins found for customer {customer.id}. Cannot send '{email_type.value}' notification.")
            log_action(
                db,
                user_id=None,
                action_type="NOTIFICATION_FAILED",
                entity_type="Customer",
                entity_id=customer.id,
                details={
                    "reason": f"No recipients for {email_type.value}",
                    "notification_type": email_type.value
                },
                customer_id=customer.id
            )
            return

        email_settings, email_method_for_log = get_customer_email_settings(db, customer.id)
        
        # In a real-world scenario, you would have an email template for each type.
        # For now, we use a simple text body.
        # template = crud_template.get_by_name_and_action_type(db, name=email_type.value, action_type=email_type.value, is_notification_template=True)
        # body = template.content if template else body_placeholder

        email_sent = await send_email(
            db=db,
            to_emails=to_emails,
            subject_template=subject,
            body_template=body,
            template_data=details,
            email_settings=email_settings,
            sender_name=customer.name
        )

        if email_sent:
            log_action(
                db,
                user_id=None,
                action_type=f"NOTIFICATION_SENT_{email_type.value}",
                entity_type="Customer",
                entity_id=customer.id,
                details={
                    "notification_type": email_type.value,
                    "recipients": to_emails,
                    "subject": subject,
                    "email_method": email_method_for_log
                },
                customer_id=customer.id
            )
        else:
            log_action(
                db,
                user_id=None,
                action_type="NOTIFICATION_FAILED",
                entity_type="Customer",
                entity_id=customer.id,
                details={
                    "reason": "Email service failed",
                    "notification_type": email_type.value,
                    "recipients": to_emails
                },
                customer_id=customer.id
            )

    except Exception as e:
        db.rollback()
        logger.error(f"Error sending subscription notification for customer {customer.id}: {e}", exc_info=True)
        log_action(
            db,
            user_id=None,
            action_type="NOTIFICATION_FAILED",
            entity_type="Customer",
            entity_id=customer.id,
            details={"reason": str(e), "notification_type": email_type.value},
            customer_id=customer.id
        )


async def run_daily_subscription_status_update(
    db: Session,
    log_action: "log_action",
    crud_customer: "CRUDCustomer",
    crud_customer_configuration: "CRUDCustomerConfiguration"
):
    """
    Daily background task to check and update the subscription status of all customers.
    Sends automated email notifications based on the subscription lifecycle.
    """
    logger.info("Starting daily subscription status update task.")

    customers = crud_customer.get_all_with_relations(db)
    current_date = datetime.now(EEST_TIMEZONE).date()

    if not customers:
        logger.info("No customers found. Task finished.")
        return

    try:
        for customer in customers:
            if not customer.subscription_plan:
                logger.warning(f"Customer {customer.id} has no assigned subscription plan. Skipping.")
                continue

            # Check for renewal reminders first
            days_until_expiry = (customer.end_date.date() - current_date).days

            if days_until_expiry == 30:
                subject = f"Subscription Renewal Reminder: {customer.name}"
                body = f"""Your subscription for {customer.name} is set to expire in 30 days on {customer.end_date.date()}.
                Please renew to ensure uninterrupted service."""
                await _send_subscription_notification(
                    db, log_action, customer, SubscriptionNotificationType.RENEWAL_REMINDER_30_DAYS, subject, body, {}
                )
            elif days_until_expiry == 7:
                subject = f"Urgent: Subscription Expiring Soon for {customer.name}"
                body = f"""Your subscription for {customer.name} will expire in 7 days on {customer.end_date.date()}.
                Please renew immediately to avoid service interruption."""
                await _send_subscription_notification(
                    db, log_action, customer, SubscriptionNotificationType.RENEWAL_REMINDER_7_DAYS, subject, body, {}
                )

            # Update status based on current date
            old_status = customer.status
            new_status = old_status

            if current_date > customer.end_date.date():
                grace_period_days = crud_customer_configuration.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.GRACE_PERIOD_DAYS
                )['effective_value']
                
                try:
                    grace_period_days = int(grace_period_days)
                except (ValueError, TypeError):
                    logger.error(f"Invalid grace period days config for customer {customer.id}. Defaulting to 30.")
                    grace_period_days = 30

                if (current_date - customer.end_date.date()).days <= grace_period_days:
                    new_status = SubscriptionStatus.GRACE
                else:
                    new_status = SubscriptionStatus.EXPIRED
            else:
                new_status = SubscriptionStatus.ACTIVE

            if new_status != old_status:
                customer.status = new_status
                db.add(customer)
                db.flush()

                if new_status == SubscriptionStatus.GRACE:
                    subject = f"Subscription Expired: Grace Period Started for {customer.name}"
                    body = f"""Your subscription for {customer.name} has now expired. You are in a read-only grace period
                    of {grace_period_days} days. Please renew your subscription to restore full access."""
                    await _send_subscription_notification(
                        db, log_action, customer, SubscriptionNotificationType.GRACE_PERIOD_START, subject, body, {}
                    )
                elif new_status == SubscriptionStatus.EXPIRED:
                    subject = f"Subscription Fully Expired for {customer.name}"
                    body = f"""Your subscription for {customer.name} has ended, and the grace period has passed. Your account is now
                    locked. Please contact the system owner to renew your subscription and regain access."""
                    await _send_subscription_notification(
                        db, log_action, customer, SubscriptionNotificationType.EXPIRED, subject, body, {}
                    )

            db.commit()

    except exc.SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Database error during subscription status update: {e}", exc_info=True)
    except Exception as e:
        db.rollback()
        logger.critical(f"Critical error in subscription status update task: {e}", exc_info=True)
    finally:
        db.close()

    logger.info("Finished daily subscription status update task.")