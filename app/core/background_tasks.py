# app/core/background_tasks.py

import logging
from datetime import date, datetime, timedelta
import json
from typing import List, Dict, Any

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import exc
from sqlalchemy.sql import func

import app.models
from app.crud.crud import (
    crud_customer,
    crud_global_configuration,
    crud_customer_configuration,
    crud_lg_instruction,
    crud_user,
    crud_template,
    crud_approval_request,
    crud_lg_record,
    log_action,
    # NEW: Import crud_subscription_plan to check grace period days
    crud_subscription_plan
)
from app.core.email_service import get_customer_email_settings, send_email, EmailSettings, get_global_email_settings
from app.constants import (
    GlobalConfigKey,
    UserRole,
    ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT,
    AUDIT_ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT_SENT,
    AUDIT_ACTION_TYPE_PRINT_REMINDER_SENT,
    AUDIT_ACTION_TYPE_PRINT_ESCALATION_SENT,
    ACTION_TYPE_LG_RELEASE,
    ACTION_TYPE_LG_LIQUIDATE,
    ACTION_TYPE_LG_DECREASE_AMOUNT,
    ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
    ACTION_TYPE_LG_AMEND,
    AUDIT_ACTION_TYPE_UPDATE,
    # NEW: Import subscription related constants
    SubscriptionStatus,
    SubscriptionNotificationType,
)
from pydantic import EmailStr

import pytz
EEST_TIMEZONE = pytz.timezone('Africa/Cairo')
logger = logging.getLogger(__name__)

async def run_daily_undelivered_instructions_report(db: Session):
    """
    Background task to identify undelivered LG instructions for each customer
    and send a report to Corporate Admins via email.
    """
    logger.info("Starting daily undelivered LG instructions report background task.")
    
    customers = crud_customer.get_all(db)
    if not customers:
        logger.info("No active customers found. Skipping undelivered instructions report.")
        return

    UNDELIVERED_REPORT_TEMPLATE_NAME = "Undelivered LG Instructions Notification"
    notification_template = crud_template.get_by_name_and_action_type(
        db,
        name=UNDELIVERED_REPORT_TEMPLATE_NAME,
        action_type=ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT,
        customer_id=None,
        is_notification_template=True
    )

    if not notification_template:
        logger.error(f"Required global email template for '{ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT}' not found. Cannot send undelivered instructions reports.")
        log_action(
            db,
            user_id=None,
            action_type="REPORT_GENERATION_FAILED",
            entity_type="System",
            entity_id=None,
            details={"reason": "Missing undelivered instructions report email template."},
            customer_id=None,
            lg_record_id=None
        )
        db.commit()
        return

    for customer in customers:
        try:
            logger.debug(f"Processing customer {customer.id} ({customer.name}) for undelivered instructions report.")

            report_start_days_config = crud_customer_configuration.get_customer_config_or_global_fallback(
                db, customer.id, GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_REPORT_UNDELIVERED
            )
            report_stop_days_config = crud_customer_configuration.get_customer_config_or_global_fallback(
                db, customer.id, GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_STOP_REPORTING_UNDELIVERED
            )

            report_start_days = None
            report_stop_days = None

            try:
                if report_start_days_config and report_start_days_config.get('effective_value') is not None:
                    report_start_days = int(report_start_days_config['effective_value'])
                else:
                    logger.warning(f"Config '{GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_REPORT_UNDELIVERED.value}' not found or has no effective value for customer {customer.id}. Skipping report for this customer.")
                    continue
            except ValueError:
                logger.error(f"Invalid integer value for '{GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_REPORT_UNDELIVERED.value}' for customer {customer.id}. Skipping report for this customer.")
                continue

            try:
                if report_stop_days_config and report_stop_days_config.get('effective_value') is not None:
                    report_stop_days = int(report_stop_days_config['effective_value'])
                else:
                    logger.warning(f"Config '{GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_STOP_REPORTING_UNDELIVERED.value}' not found or has no effective value for customer {customer.id}. Skipping report for this customer.")
                    continue
            except ValueError:
                logger.error(f"Invalid integer value for '{GlobalConfigKey.NUMBER_OF_DAYS_SINCE_ISSUANCE_TO_STOP_REPORTING_UNDELIVERED.value}' for customer {customer.id}. Skipping report for this customer.")
                continue

            if report_start_days >= report_stop_days:
                logger.warning(f"Report start days ({report_start_days}) must be less than stop days ({report_stop_days}) for customer {customer.id}. Skipping report.")
                continue

            undelivered_instructions = crud_lg_instruction.get_undelivered_instructions_for_reporting(
                db, customer.id, report_start_days, report_stop_days
            )

            if not undelivered_instructions:
                logger.info(f"No undelivered instructions found for customer {customer.id} within the reporting window. Skipping email.")
                continue

            corporate_admins = crud_user.get_users_by_role_for_customer(db, customer.id, UserRole.CORPORATE_ADMIN)
            if not corporate_admins:
                logger.warning(f"No active Corporate Admin users found for customer {customer.id}. Cannot send undelivered instructions report.")
                log_action(
                    db, user_id=None, action_type="REPORT_GENERATION_FAILED", entity_type="Customer", entity_id=customer.id,
                    details={"reason": "No Corporate Admins found to send report to.", "report_type": "Undelivered Instructions"},
                    customer_id=customer.id, lg_record_id=None
                )
                continue

            to_emails = [admin.email for admin in corporate_admins if admin.email]
            if not to_emails:
                logger.warning(f"No valid email addresses found for Corporate Admins of customer {customer.id}. Cannot send undelivered instructions report.")
                log_action(
                    db, user_id=None, action_type="REPORT_GENERATION_FAILED", entity_type="Customer", entity_id=customer.id,
                    details={"reason": "No valid Corporate Admin emails found.", "report_type": "Undelivered Instructions"},
                    customer_id=customer.id, lg_record_id=None
                )
                continue

            cc_emails = []
            common_comm_list_config = crud_customer_configuration.get_customer_config_or_global_fallback(
                db, customer.id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
            )
            if common_comm_list_config and common_comm_list_config.get('effective_value'):
                try:
                    parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                    if isinstance(parsed_common_list, list) and all(
                        isinstance(e, str) and "@" in e for e in parsed_common_list
                    ):
                        cc_emails.extend(parsed_common_list)
                except json.JSONDecodeError:
                    logger.warning(
                        f"COMMON_COMMUNICATION_LIST for customer {customer.id} is not a valid JSON list of emails. Skipping."
                    )
            cc_emails = list(set(cc_emails))

            instructions_table_rows = []
            for inst in undelivered_instructions:
                lg_record = inst.lg_record
                days_since_issued = (date.today() - inst.instruction_date.date()).days
                instructions_table_rows.append(f"""
                    <tr>
                        <td>{lg_record.lg_number}</td>
                        <td>{inst.instruction_type}</td>
                        <td>{inst.serial_number}</td>
                        <td>{inst.instruction_date.strftime('%Y-%m-%d')}</td>
                        <td>{days_since_issued} days</td>
                        <td>{lg_record.issuing_bank.name if lg_record.issuing_bank else 'N/A'}</td>
                        <td>{lg_record.lg_currency.iso_code if lg_record.lg_currency else 'N/A'} {float(lg_record.lg_amount):,.2f}</td>
                        <td>{lg_record.internal_owner_contact.email if lg_record.internal_owner_contact else 'N/A'}</td>
                    </tr>
                """)
            instructions_table_html = f"""
                <table border="1" cellpadding="5" cellspacing="0" style="width:100%; border-collapse: collapse;">
                    <thead>
                        <tr>
                            <th>LG Number</th>
                            <th>Instruction Type</th>
                            <th>Instruction Serial</th>
                            <th>Instruction Date</th>
                            <th>Days Undelivered</th>
                            <th>Issuing Bank</th>
                            <th>LG Amount</th>
                            <th>Internal Owner</th>
                        </tr>
                    </thead>
                    <tbody>
                        {"".join(instructions_table_rows)}
                    </tbody>
                </table>
            """

            template_data = {
                "customer_name": customer.name,
                "report_start_days": report_start_days,
                "report_stop_days": report_stop_days,
                "undelivered_instructions_count": len(undelivered_instructions),
                "undelivered_instructions_table": instructions_table_html,
                "current_date": date.today().strftime('%Y-%m-%d'),
                "platform_name": "Treasury Management Platform"
            }

            email_settings, email_method_for_log = get_customer_email_settings(db, customer.id)

            subject_filled = notification_template.subject.replace("{{customer_name}}", customer.name) \
                             .replace("{{undelivered_instructions_count}}", str(len(undelivered_instructions)))
            body_filled = notification_template.content
            for key, value in template_data.items():
                str_value = str(value) if value is not None else ""
                body_filled = body_filled.replace(f"{{{{{key}}}}}", str_value)

            email_sent_successfully = await send_email(
                db=db,
                to_emails=to_emails,
                cc_emails=cc_emails,
                subject_template=subject_filled,
                body_template=body_filled,
                template_data=template_data,
                email_settings=email_settings,
                sender_name=customer.name
            )

            if email_sent_successfully:
                log_action(
                    db,
                    user_id=None,
                    action_type=AUDIT_ACTION_TYPE_LG_UNDELIVERED_INSTRUCTIONS_REPORT_SENT,
                    entity_type="Customer",
                    entity_id=customer.id,
                    details={
                        "report_type": "Undelivered Instructions",
                        "undelivered_count": len(undelivered_instructions),
                        "recipients": to_emails,
                        "cc_recipients": cc_emails,
                        "email_subject": subject_filled,
                        "email_method": email_method_for_log
                    },
                    customer_id=customer.id,
                    lg_record_id=None
                )
                logger.info(f"Undelivered instructions report sent successfully for customer {customer.name}.")
            else:
                log_action(
                    db,
                    user_id=None,
                    action_type="REPORT_GENERATION_FAILED",
                    entity_type="Customer",
                    entity_id=customer.id,
                    details={
                        "reason": "Email sending failed",
                        "report_type": "Undelivered Instructions",
                        "undelivered_count": len(undelivered_instructions),
                        "recipients": to_emails,
                        "cc_recipients": cc_emails,
                        "email_subject": subject_filled,
                        "email_method": email_method_for_log
                    },
                    customer_id=customer.id,
                    lg_record_id=None
                )
                logger.error(f"Failed to send undelivered instructions report for customer {customer.name}.")

        except Exception as e:
            db.rollback()
            logger.error(f"Error processing undelivered instructions report for customer {customer.id} ({customer.name}): {e}", exc_info=True)
            log_action(
                db,
                user_id=None,
                action_type="TASK_PROCESSING_FAILED",
                entity_type="Customer",
                entity_id=customer.id,
                details={"reason": f"Unhandled error: {e}", "report_type": "Undelivered Instructions"},
                customer_id=customer.id,
                lg_record_id=None
            )
        finally:
            # We must commit the session for this customer's processing, as the scheduler
            # provides a new session for each job run. If we don't commit, no changes will persist.
            # However, the scheduler's job_wrapper should be responsible for this. Let's assume it is not
            # for now, and handle the commit here per customer loop.
            db.commit()

    logger.info("Finished daily undelivered LG instructions report background task.")

async def proactively_correct_customer_configs(global_config_id: int, db: Session):
    """
    Background task to re-validate and correct CustomerConfiguration entries
    that override a specific GlobalConfiguration, typically after the global
    range has been narrowed.
    This task is triggered by an API endpoint, not a scheduler.
    """
    logger.info(f"Starting proactive configuration correction task for GlobalConfiguration ID: {global_config_id}.")
    
    try:
        corrected_configs = crud_customer_configuration.revalidate_customer_configs_for_global_change(
            db, global_config_id
        )
        
        db.commit()

        if corrected_configs:
            customers_to_notify = {}
            for correction in corrected_configs:
                cust_id = correction['customer_id']
                if cust_id not in customers_to_notify:
                    customers_to_notify[cust_id] = []
                customers_to_notify[cust_id].append(correction)

            for cust_id, corrections in customers_to_notify.items():
                await _send_config_correction_notification(db, cust_id, corrections)
        
        logger.info(f"Proactive configuration correction task for GlobalConfig ID {global_config_id} completed. Corrected {len(corrected_configs)} entries.")

    except Exception as e:
        db.rollback()
        logger.error(f"Unhandled error in proactive config correction task for GlobalConfig ID {global_config_id}: {e}", exc_info=True)
        log_action(
            db,
            user_id=None,
            action_type="TASK_PROCESSING_FAILED",
            entity_type="GlobalConfiguration",
            entity_id=global_config_id,
            details={"reason": f"Unhandled error in config correction task: {e}"}
        )
        db.commit()
    finally:
        pass

async def _send_config_correction_notification(db: Session, customer_id: int, corrections: List[Dict[str, Any]]):
    """
    Helper function to send email notification to Corporate Admins about auto-corrected configs.
    """
    customer = db.query(app.models.Customer).filter(app.models.Customer.id == customer_id).first()
    if not customer:
        logger.warning(f"Customer with ID {customer_id} not found for sending config correction notification.")
        return

    corporate_admins = db.query(app.models.User).filter(
        app.models.User.customer_id == customer_id,
        app.models.User.role == UserRole.CORPORATE_ADMIN,
        app.models.User.is_deleted == False
    ).all()
    
    to_emails = [admin.email for admin in corporate_admins if admin.email]
    if not to_emails:
        logger.warning(f"No corporate admins found for customer {customer.id}. Cannot send config correction notification.")
        return

    email_settings, email_method_for_log = get_customer_email_settings(db, customer.id)

    table_rows = ""
    for correction in corrections:
        table_rows += f"""
        <tr>
            <td>{correction['global_config_key']}</td>
            <td>{correction['old_value']}</td>
            <td>{correction['new_value']}</td>
        </tr>
        """
    table_html = f"""
    <table border="1" cellpadding="5" cellspacing="0" style="width:100%; border-collapse: collapse;">
        <thead>
            <tr>
                <th>Configuration Key</th>
                <th>Old Value</th>
                <th>New Corrected Value</th>
            </tr>
        </thead>
        <tbody>
            {table_rows}
        </tbody>
    </table>
    """

    email_subject = f"System Notification: Automatic Configuration Update for {customer.name}"
    email_body_html = f"""
    <html>
        <body>
            <p>Dear Corporate Admin,</p>
            <p>This is an automated notification to inform you that one or more of your customer-specific configuration settings have been automatically adjusted by the system.</p>
            <p>This adjustment was necessary because the system-wide global limits for these configurations were narrowed, making your previous settings invalid.</p>
            <p>The following configurations were corrected:</p>
            {table_html}
            <p>No further action is required from you at this time. The new values are now in effect.</p>
            <p>Thank you,</p>
            <p>The Treasury Management Platform Team</p>
        </body>
    </html>
    """

    await send_email(
        db=db,
        to_emails=to_emails,
        subject_template=email_subject,
        body_template=email_body_html,
        template_data={},
        email_settings=email_settings,
        sender_name=customer.name
    )

async def run_daily_print_reminders(db: Session):
    logger.info("Running daily print reminders and escalation task...")
    
    INSTRUCTION_TYPES_REQUIRING_PRINTING = [
        ACTION_TYPE_LG_RELEASE,
        ACTION_TYPE_LG_LIQUIDATE,
        ACTION_TYPE_LG_DECREASE_AMOUNT,
        ACTION_TYPE_LG_ACTIVATE_NON_OPERATIVE,
    ]

    customers = db.query(app.models.Customer).filter(app.models.Customer.is_deleted == False).all()
    if not customers:
        logger.info("No active customers found. Skipping print reminders and escalation.")
        return

    try:
        for customer in customers:
            try:
                logger.debug(f"Processing print reminders for customer: {customer.name} (ID: {customer.id})")

                days_for_first_reminder_config = crud_customer_configuration.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.DAYS_FOR_FIRST_PRINT_REMINDER
                )
                days_for_escalation_config = crud_customer_configuration.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.DAYS_FOR_PRINT_ESCALATION
                )

                days_for_first_reminder = int(days_for_first_reminder_config.get('effective_value', 2)) if days_for_first_reminder_config else 2
                days_for_escalation = int(days_for_escalation_config.get('effective_value', 5)) if days_for_escalation_config else 5

                if not (0 < days_for_first_reminder < days_for_escalation):
                    logger.warning(f"Invalid print reminder configuration for customer {customer.id}: First reminder ({days_for_first_reminder} days) is not less than escalation ({days_for_escalation} days). Skipping for this customer.")
                    continue
                
                current_date_aware = datetime.now(EEST_TIMEZONE)

                approved_requests_for_printing = db.query(app.models.ApprovalRequest).filter(
                    app.models.ApprovalRequest.customer_id == customer.id,
                    app.models.ApprovalRequest.status == app.models.ApprovalRequestStatusEnum.APPROVED,
                    app.models.ApprovalRequest.related_instruction_id.isnot(None),
                    app.models.ApprovalRequest.entity_type == "LGRecord",
                    app.models.ApprovalRequest.action_type.in_(INSTRUCTION_TYPES_REQUIRING_PRINTING)
                ).options(
                    selectinload(app.models.ApprovalRequest.related_instruction).selectinload(app.models.LGInstruction.lg_record),
                    selectinload(app.models.ApprovalRequest.related_instruction).selectinload(app.models.LGInstruction.lg_record).selectinload(app.models.LGRecord.lg_currency),
                    selectinload(app.models.ApprovalRequest.related_instruction).selectinload(app.models.LGInstruction.lg_record).selectinload(app.models.LGRecord.issuing_bank),
                    selectinload(app.models.ApprovalRequest.related_instruction).selectinload(app.models.LGInstruction.lg_record).selectinload(app.models.LGRecord.internal_owner_contact),
                    selectinload(app.models.ApprovalRequest.related_instruction).selectinload(app.models.LGInstruction.template),
                    selectinload(app.models.ApprovalRequest.maker_user),
                    selectinload(app.models.ApprovalRequest.checker_user),
                ).all()

                for req in approved_requests_for_printing:
                    instruction = req.related_instruction
                    maker_user = req.maker_user
                    checker_user = req.checker_user

                    if not instruction or instruction.is_printed:
                        continue

                    if not maker_user:
                        logger.warning(f"Maker user for approval request {req.id} (instruction {instruction.id}) not found. Cannot send print reminder.")
                        continue

                    instruction_creation_time = instruction.instruction_date
                    if instruction_creation_time.tzinfo is None:
                        instruction_creation_time = instruction_creation_time.replace(tzinfo=EEST_TIMEZONE)

                    days_since_instruction_creation = (current_date_aware - instruction_creation_time).days
                    
                    request_details = req.request_details if req.request_details else {}
                    print_notification_status = request_details.get("print_notification_status", "NONE")

                    send_reminder = False
                    send_escalation = False
                    audit_action_type = None
                    template_action_type = None

                    if (print_notification_status == "NONE" and
                        days_since_instruction_creation >= days_for_first_reminder):
                        send_reminder = True
                        audit_action_type = AUDIT_ACTION_TYPE_PRINT_REMINDER_SENT
                        template_action_type = "PRINT_REMINDER"

                    elif (print_notification_status == "REMINDER_SENT" and
                          days_since_instruction_creation >= days_for_escalation):
                        send_escalation = True
                        audit_action_type = AUDIT_ACTION_TYPE_PRINT_ESCALATION_SENT
                        template_action_type = "PRINT_ESCALATION"

                    if send_reminder or send_escalation:
                        to_emails = [maker_user.email]
                        cc_emails = []
                        
                        if checker_user and send_escalation:
                            cc_emails.append(checker_user.email)
                        
                        common_comm_list_config = crud_customer_configuration.get_customer_config_or_global_fallback(
                            db, customer.id, GlobalConfigKey.COMMON_COMMUNICATION_LIST
                        )
                        if common_comm_list_config and common_comm_list_config.get('effective_value'):
                            try:
                                parsed_common_list = json.loads(common_comm_list_config['effective_value'])
                                if isinstance(parsed_common_list, list) and all(isinstance(e, str) and "@" in e for e in parsed_common_list):
                                    cc_emails.extend(parsed_common_list)
                            except json.JSONDecodeError:
                                logger.warning(f"COMMON_COMMUNICATION_LIST for customer {customer.id} is not a valid JSON list of emails. Skipping for print notification.")
                        cc_emails = list(set(cc_emails))

                        notification_template = crud_template.get_by_name_and_action_type(
                            db,
                            name=template_action_type.replace('_', ' ').title(),
                            action_type=template_action_type,
                            customer_id=None,
                            is_notification_template=True
                        )

                        if not notification_template:
                            logger.error(f"Notification template '{template_action_type}' not found for customer {customer.id}. Cannot send print reminder/escalation for Approval Request {req.id}.")
                            log_action(
                                db,
                                user_id=None,
                                action_type="NOTIFICATION_FAILED",
                                entity_type="ApprovalRequest",
                                entity_id=req.id,
                                details={"reason": f"Template '{template_action_type}' missing for print notification", "recipient": to_emails},
                                customer_id=customer.id,
                                lg_record_id=req.entity_id,
                            )
                            continue

                        template_data = {
                            "maker_email": maker_user.email,
                            "maker_name": maker_user.email.split('@')[0],
                            "checker_email": checker_user.email if checker_user else "N/A",
                            "approval_request_id": req.id,
                            "action_type": req.action_type.replace('_', ' ').title(),
                            "lg_number": instruction.lg_record.lg_number if instruction.lg_record else "N/A",
                            "instruction_serial_number": instruction.serial_number,
                            "days_overdue": days_since_instruction_creation,
                            "customer_name": customer.name,
                            "platform_name": "Treasury Management Platform",
                            "current_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "print_link": f"/api/v1/end-user/lg-records/instructions/{instruction.id}/view-letter?print=true",
                            "action_center_link": "/action-center"
                        }

                        email_settings_to_use, email_method_for_log = get_customer_email_settings(db, customer.id)

                        email_subject = notification_template.subject if notification_template.subject else f"Print Reminder for LG #{{lg_number}}"
                        email_body_html = notification_template.content
                        for key, value in template_data.items():
                            str_value = str(value) if value is not None else ""
                            email_body_html = email_body_html.replace(f"{{{{{key}}}}}", str_value)
                            email_subject = email_subject.replace(f"{{{{{key}}}}}", str_value)

                        email_sent_successfully = await send_email(
                            db=db,
                            to_emails=to_emails,
                            cc_emails=cc_emails,
                            subject_template=email_subject,
                            body_template=email_body_html,
                            template_data=template_data,
                            email_settings=email_settings_to_use,
                            sender_name=customer.name
                        )

                        if email_sent_successfully:
                            log_action(
                                db,
                                user_id=None,
                                action_type=audit_action_type,
                                entity_type="ApprovalRequest",
                                entity_id=req.id,
                                details={
                                    "recipient": to_emails,
                                    "cc_recipients": cc_emails,
                                    "subject": email_subject,
                                    "method": email_method_for_log,
                                    "notification_type": template_action_type,
                                    "lg_record_id": req.entity_id,
                                    "instruction_id": instruction.id,
                                    "days_overdue": days_since_instruction_creation
                                },
                                customer_id=customer.id,
                                lg_record_id=req.entity_id,
                            )
                            req.request_details = req.request_details if req.request_details else {}
                            if send_reminder:
                                req.request_details["print_notification_status"] = "REMINDER_SENT"
                                logger.info(f"Print reminder sent for Approval Request ID: {req.id} (Instruction: {instruction.serial_number}).")
                            elif send_escalation:
                                req.request_details["print_notification_status"] = "ESCALATION_SENT"
                                logger.info(f"Print escalation sent for Approval Request ID: {req.id} (Instruction: {instruction.serial_number}).")
                            db.add(req)
                            db.flush()

                        else:
                            log_action(
                                db,
                                user_id=None,
                                action_type="NOTIFICATION_FAILED",
                                entity_type="ApprovalRequest",
                                entity_id=req.id,
                                details={"reason": f"Email service failed to send {template_action_type} notification", "recipient": to_emails, "subject": email_subject, "method": email_method_for_log},
                                customer_id=customer.id,
                                lg_record_id=req.entity_id,
                            )
                            logger.error(f"Failed to send {template_action_type} notification for Approval Request ID: {req.id}.")
            except Exception as e:
                db.rollback()
                logger.error(f"Error processing print reminders for customer {customer.id} ({customer.name}): {e}", exc_info=True)
                log_action(
                    db,
                    user_id=None,
                    action_type="TASK_PROCESSING_FAILED",
                    entity_type="Customer",
                    entity_id=customer.id,
                    details={"reason": f"Unhandled error in print reminders task: {e}", "task": "Print Reminders"},
                    customer_id=customer.id,
                    lg_record_id=None
                )

        logger.info("Daily print reminders and escalation task completed.")
    except Exception as e:
        db.rollback()
        logger.critical(f"CRITICAL ERROR during daily print reminders and escalation task: {e}", exc_info=True)
    finally:
        pass

async def run_daily_renewal_reminders(db: Session):
    """
    Orchestrates the daily execution of both renewal reminder features.
    """
    logger.info("Starting daily LG renewal reminders orchestration.")
    
    customers = db.query(app.models.Customer).filter(app.models.Customer.is_deleted == False).all()
    if not customers:
        logger.info("No active customers found. Skipping all renewal reminder tasks.")
        return

    for customer in customers:
        logger.info(f"Processing renewal reminders for customer: {customer.name} (ID: {customer.id})")
        try:
            await crud_lg_record.run_renewal_reminders_to_users_and_admins(db)
            db.commit()
            logger.info(f"Successfully ran 'Renewal Reminders to Users & Admins' for customer {customer.id}.")
        except Exception as e:
            db.rollback()
            logger.error(f"Error running 'Renewal Reminders to Users & Admins' for customer {customer.id}: {e}", exc_info=True)
            log_action(
                db, user_id=None, action_type="TASK_PROCESSING_FAILED", entity_type="Customer", entity_id=customer.id,
                details={"reason": f"Error in Feature 1 reminders: {e}", "task": "Renewal Reminders to Users/Admins"},
                customer_id=customer.id, lg_record_id=None
            )
            db.commit()

        try:
            await crud_lg_record.run_internal_owner_renewal_reminders(db)
            db.commit()
            logger.info(f"Successfully ran 'Internal Owner Renewal Reminders' for customer {customer.id}.")
        except Exception as e:
            db.rollback()
            logger.error(f"Error running 'Internal Owner Renewal Reminders' for customer {customer.id}: {e}", exc_info=True)
            log_action(
                db, user_id=None, action_type="TASK_PROCESSING_FAILED", entity_type="Customer", entity_id=customer.id,
                details={"reason": f"Error in Feature 2 reminders: {e}", "task": "Internal Owner Renewal Reminders"},
                customer_id=customer.id, lg_record_id=None
            )
            db.commit()

    logger.info("Daily LG renewal reminders orchestration completed.")

# NEW FUNCTION: Run Daily Subscription Status Update
async def run_daily_subscription_status_update(db: Session):
    """
    Daily background task to check and update the subscription status of all customers.
    Sends automated email notifications based on the subscription lifecycle.
    """
    logger.info("Starting daily subscription status update task.")

    customers = crud_customer.get_all(db)
    current_date = datetime.now(EEST_TIMEZONE).date()

    if not customers:
        logger.info("No customers found. Task finished.")
        return

    try:
        for customer in customers:
            # Re-fetch customer with plan details in case it changed
            customer = crud_customer.get_with_relations(db, customer.id)
            if not customer or not customer.subscription_plan:
                logger.warning(f"Customer {customer.id} has no assigned subscription plan. Skipping.")
                continue

            # Check for renewal reminders first
            days_until_expiry = (customer.end_date.date() - current_date).days

            if days_until_expiry == 30:
                subject = f"Subscription Renewal Reminder: {customer.name}"
                body = f"""Your subscription for {customer.name} is set to expire in 30 days on {customer.end_date.date()}.
                Please renew to ensure uninterrupted service."""
                await _send_subscription_notification(
                    db, customer, SubscriptionNotificationType.RENEWAL_REMINDER_30_DAYS, subject, body, {}
                )
            elif days_until_expiry == 7:
                subject = f"Urgent: Subscription Expiring Soon for {customer.name}"
                body = f"""Your subscription for {customer.name} will expire in 7 days on {customer.end_date.date()}.
                Please renew immediately to avoid service interruption."""
                await _send_subscription_notification(
                    db, customer, SubscriptionNotificationType.RENEWAL_REMINDER_7_DAYS, subject, body, {}
                )

            # Update status based on current date
            old_status = customer.status
            new_status = old_status

            if current_date > customer.end_date.date():
                grace_period_days_config = crud_customer_configuration.get_customer_config_or_global_fallback(
                    db, customer.id, GlobalConfigKey.GRACE_PERIOD_DAYS
                )
                grace_period_days = int(grace_period_days_config.get('effective_value', 30))

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
                    of {grace_period_days_config.get('effective_value', 30)} days. Please renew your subscription to restore full access."""
                    await _send_subscription_notification(
                        db, customer, SubscriptionNotificationType.GRACE_PERIOD_START, subject, body, {}
                    )
                elif new_status == SubscriptionStatus.EXPIRED:
                    subject = f"Subscription Fully Expired for {customer.name}"
                    body = f"""Your subscription for {customer.name} has ended, and the grace period has passed. Your account is now
                    locked. Please contact the system owner to renew your subscription and regain access."""
                    await _send_subscription_notification(
                        db, customer, SubscriptionNotificationType.EXPIRED, subject, body, {}
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

async def _send_subscription_notification(
    db: Session,
    customer: app.models.Customer,
    email_type: SubscriptionNotificationType,
    subject: str,
    body: str,
    details: Dict[str, Any]
):
    """Helper function to send a subscription-related notification email."""
    try:
        # Fetch all Corporate Admins for the customer
        corporate_admins = crud_user.get_users_by_role_for_customer(db, customer.id, app.models.UserRole.CORPORATE_ADMIN)
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
    finally:
        db.rollback()