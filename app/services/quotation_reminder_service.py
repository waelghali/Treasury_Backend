import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, List

from sqlalchemy.orm import Session
from apscheduler.triggers.date import DateTrigger

from app.constants import AUDIT_ACTION_TYPE_QUOTATION_WINDOW_15M_REMINDER_SENT
from app.models.models import AuditLog
from app.models.models_quotation import QuotationRequest, QuotationBank, QuotationBankAssignment

logger = logging.getLogger(__name__)


def get_scheduler() -> Optional[Any]:
    """Helper to lazily retrieve the running APScheduler from FastAPI app state."""
    try:
        from app.main import app as fastapi_app
        if hasattr(fastapi_app.state, "scheduler"):
            return fastapi_app.state.scheduler
    except Exception as e:
        logger.debug(f"Could not retrieve scheduler from app state: {e}")
    return None


def has_15m_reminder_been_sent(db: Session, rfq_id: str, customer_id: int) -> bool:
    """Checks if the 15-minute reminder has already been logged as sent for this RFQ."""
    try:
        logs = db.query(AuditLog).filter(
            AuditLog.customer_id == customer_id,
            AuditLog.action_type == AUDIT_ACTION_TYPE_QUOTATION_WINDOW_15M_REMINDER_SENT
        ).all()
        for log_entry in logs:
            if isinstance(log_entry.details, dict) and str(log_entry.details.get("rfq_id")) == str(rfq_id):
                return True
    except Exception as e:
        logger.warning(f"Error checking 15m reminder audit log for RFQ {rfq_id}: {e}")
    return False


def schedule_rfq_15m_reminder(
    rfq_id: str,
    window_start: datetime,
    release_time: Optional[datetime] = None,
    scheduler: Optional[Any] = None
) -> bool:
    """
    Schedules a dynamic event-driven one-off reminder at window_start - 15 minutes.
    
    Rule:
    - Only schedules if the lead time (window_start - release_time) is at least 60 minutes.
    - If window_start - 15m is already in the past, skips scheduling.
    - Registered with APScheduler DateTrigger.
    """
    if release_time is None:
        release_time = datetime.now(timezone.utc)

    # Ensure timezone-aware UTC datetimes
    if window_start.tzinfo is None:
        window_start = window_start.replace(tzinfo=timezone.utc)
    else:
        window_start = window_start.astimezone(timezone.utc)

    if release_time.tzinfo is None:
        release_time = release_time.replace(tzinfo=timezone.utc)
    else:
        release_time = release_time.astimezone(timezone.utc)

    lead_seconds = (window_start - release_time).total_seconds()
    if lead_seconds < 3600:
        logger.info(
            f"RFQ {rfq_id} lead time is {lead_seconds / 60:.1f} minutes (< 60 min threshold). "
            f"Skipping 15-minute prior reminder."
        )
        return False

    reminder_time = window_start - timedelta(minutes=15)
    now_utc = datetime.now(timezone.utc)

    if reminder_time <= now_utc:
        logger.info(
            f"RFQ {rfq_id} reminder time {reminder_time.isoformat()} has already passed. "
            f"Skipping 15-minute prior reminder."
        )
        return False

    target_scheduler = scheduler or get_scheduler()
    if not target_scheduler:
        logger.warning(f"No active scheduler available to register 15m reminder for RFQ {rfq_id}.")
        return False

    job_id = f"rfq_reminder_15m_{rfq_id}"
    try:
        target_scheduler.add_job(
            func=execute_rfq_15m_reminder,
            trigger=DateTrigger(run_date=reminder_time, timezone=timezone.utc),
            id=job_id,
            name=f"RFQ 15m Reminder ({rfq_id})",
            args=[rfq_id],
            replace_existing=True,
            misfire_grace_time=300
        )
        logger.info(
            f"Successfully scheduled 15-minute prior reminder for RFQ {rfq_id} at {reminder_time.isoformat()} "
            f"(Lead time: {lead_seconds / 60:.1f} min)."
        )
        return True
    except Exception as e:
        logger.error(f"Failed to register reminder job for RFQ {rfq_id}: {e}", exc_info=True)
        return False


def cancel_rfq_15m_reminder(rfq_id: str, scheduler: Optional[Any] = None) -> bool:
    """Removes the scheduled 15-minute reminder job if the RFQ is cancelled before window start."""
    target_scheduler = scheduler or get_scheduler()
    if not target_scheduler:
        return False

    job_id = f"rfq_reminder_15m_{rfq_id}"
    try:
        if target_scheduler.get_job(job_id):
            target_scheduler.remove_job(job_id)
            logger.info(f"Successfully cancelled 15m reminder job {job_id} for RFQ {rfq_id}.")
            return True
    except Exception as e:
        logger.warning(f"Error cancelling reminder job {job_id}: {e}")
    return False


async def execute_rfq_15m_reminder(rfq_id: str, db: Optional[Session] = None) -> bool:
    """
    Dispatches the 15-minute prior reminder emails to counterparties:
    - If assignment.approval_status == 'PENDING': Urgently notifies Approvers only with authorization link.
    - If assignment.approval_status in ('APPROVED', None): Notifies Execution contacts only with portal quote link.
    - Idempotency: Records QUOTATION_WINDOW_15M_REMINDER_SENT in AuditLog to avoid double dispatch.
    """
    is_local_session = False
    if db is None:
        from app.database import SessionLocal
        db = SessionLocal()
        is_local_session = True

    try:
        rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
        if not rfq:
            logger.warning(f"15m reminder abort: RFQ {rfq_id} not found in database.")
            return False

        # Only dispatch if RFQ is still active and waiting for window start
        if rfq.status not in ("PENDING", "OPEN"):
            logger.info(
                f"15m reminder skipped: RFQ {rfq.ref_no} ({rfq_id}) has terminal or inactive status '{rfq.status}'."
            )
            return False

        # Check idempotency guard
        if has_15m_reminder_been_sent(db, rfq_id, rfq.customer_id):
            logger.info(f"15m reminder skipped: Already sent for RFQ {rfq.ref_no} ({rfq_id}).")
            return False

        from app.core.email_service import get_customer_email_settings, send_email
        from app.core.routing import get_frontend_base_url
        from app.services.unified_email_builder import build_quotation_rfq_bank_email
        from app.crud.base import log_action

        email_settings, _ = get_customer_email_settings(db, rfq.customer_id)
        base_url = get_frontend_base_url()
        customer_branding = (
            (rfq.entity.entity_name if rfq.entity else None)
            or (rfq.customer.name if rfq.customer else "Treasury Customer")
        )

        total_emails_sent = 0
        assignments = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == rfq.id).all()

        for assignment in assignments:
            # Skip declined or expired assignments
            if assignment.approval_status in ("DECLINED", "EXPIRED"):
                continue

            bank_row = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
            if not bank_row:
                continue

            contacts = bank_row.contacts if isinstance(bank_row.contacts, list) and len(bank_row.contacts) > 0 else []
            if not contacts and bank_row.emails:
                contacts = [
                    {"email": e.strip(), "name": "", "role": "EXECUTION"}
                    for e in bank_row.emails.split(",") if e.strip()
                ]
            if not contacts:
                continue

            bank_display_name = bank_row.bank.name if bank_row.bank else "Bank Partner"

            # Route 1: Bank Approval is PENDING -> Send reminder strictly to APPROVER contacts
            if assignment.approval_status == "PENDING":
                approver_emails = list(dict.fromkeys(
                    c.get("email", "").strip() for c in contacts
                    if c.get("role") == "APPROVER" and c.get("email")
                ))
                if not approver_emails:
                    approver_emails = list(dict.fromkeys(c.get("email", "").strip() for c in contacts if c.get("email")))

                for app_email in approver_emails:
                    approver_link = f"{base_url}/public-quotation/{assignment.token}?email={app_email}"
                    subject, body = build_quotation_rfq_bank_email(
                        rfq=rfq,
                        assignment=assignment,
                        bank_name=bank_display_name,
                        customer_branding=customer_branding,
                        link=approver_link,
                        email_purpose="WINDOW_START_REMINDER_APPROVER"
                    )
                    success, _ = await send_email(
                        db=db,
                        to_emails=[app_email],
                        subject_template=subject,
                        body_template=body,
                        template_data={},
                        email_settings=email_settings
                    )
                    if success:
                        total_emails_sent += 1

            # Route 2: Bank is APPROVED or requires NO APPROVAL -> Send reminder strictly to EXECUTION contacts
            elif assignment.approval_status in ("APPROVED", None):
                execution_emails = list(dict.fromkeys(
                    c.get("email", "").strip() for c in contacts
                    if c.get("role") == "EXECUTION" and c.get("email")
                ))
                if not execution_emails:
                    # Fallback excluding approvers and view_only
                    execution_emails = list(dict.fromkeys(
                        c.get("email", "").strip() for c in contacts
                        if c.get("role") not in ("APPROVER", "VIEW_ONLY") and c.get("email")
                    ))
                    if not execution_emails:
                        execution_emails = list(dict.fromkeys(c.get("email", "").strip() for c in contacts if c.get("email")))

                if execution_emails:
                    exec_link = f"{base_url}/public-quotation/{assignment.token}"
                    subject, body = build_quotation_rfq_bank_email(
                        rfq=rfq,
                        assignment=assignment,
                        bank_name=bank_display_name,
                        customer_branding=customer_branding,
                        link=exec_link,
                        email_purpose="WINDOW_START_REMINDER_EXECUTION"
                    )
                    success, _ = await send_email(
                        db=db,
                        to_emails=execution_emails,
                        subject_template=subject,
                        body_template=body,
                        template_data={},
                        email_settings=email_settings
                    )
                    if success:
                        total_emails_sent += 1

        # Audit log idempotent record
        now_dt = datetime.now(timezone.utc)
        log_action(
            db=db,
            user_id=None,
            action_type=AUDIT_ACTION_TYPE_QUOTATION_WINDOW_15M_REMINDER_SENT,
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": str(rfq.id),
                "ref_no": rfq.ref_no,
                "emails_sent_count": total_emails_sent,
                "window_start": rfq.window_start.isoformat() if rfq.window_start else None,
                "dispatched_at": now_dt.isoformat()
            },
            customer_id=rfq.customer_id
        )
        db.commit()
        logger.info(
            f"Successfully executed 15-minute prior reminder for RFQ {rfq.ref_no} ({total_emails_sent} emails dispatched)."
        )
        return True
    except Exception as e:
        logger.error(f"Error executing 15m reminder for RFQ {rfq_id}: {e}", exc_info=True)
        return False
    finally:
        if is_local_session:
            db.close()


def run_execute_rfq_15m_reminder_sync(rfq_id: str, db: Optional[Session] = None) -> bool:
    """Convenience synchronous wrapper to execute reminder from sync code or tests."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, execute_rfq_15m_reminder(rfq_id, db)).result()
        else:
            return loop.run_until_complete(execute_rfq_15m_reminder(rfq_id, db))
    except Exception:
        return asyncio.run(execute_rfq_15m_reminder(rfq_id, db))


def sync_pending_rfq_reminders_on_startup(scheduler: Any, session_factory: Any):
    """
    Startup recovery hook. Queries active RFQs qualifying for the 15-minute reminder
    and restores their one-off DateTrigger timers on APScheduler.
    """
    if not scheduler:
        return
    db = session_factory()
    try:
        now_utc = datetime.now(timezone.utc)
        min_start = now_utc + timedelta(minutes=15)

        active_rfqs = db.query(QuotationRequest).filter(
            QuotationRequest.status == "PENDING",
            QuotationRequest.window_start > min_start
        ).all()

        scheduled_count = 0
        for rfq in active_rfqs:
            release_time = rfq.admin_reviewed_at or rfq.created_at
            if not release_time or not rfq.window_start:
                continue

            w_start = rfq.window_start if rfq.window_start.tzinfo else rfq.window_start.replace(tzinfo=timezone.utc)
            r_time = release_time if release_time.tzinfo else release_time.replace(tzinfo=timezone.utc)

            # Check lead time >= 60 min
            if (w_start - r_time).total_seconds() < 3600:
                continue

            # Check if reminder already sent
            if has_15m_reminder_been_sent(db, str(rfq.id), rfq.customer_id):
                continue

            scheduled = schedule_rfq_15m_reminder(
                rfq_id=str(rfq.id),
                window_start=w_start,
                release_time=r_time,
                scheduler=scheduler
            )
            if scheduled:
                scheduled_count += 1

        logger.info(
            f"Startup RFQ 15m reminder sync: checked {len(active_rfqs)} candidate RFQs, registered {scheduled_count} jobs."
        )
    except Exception as e:
        logger.error(f"Error during startup sync of RFQ reminders: {e}", exc_info=True)
    finally:
        db.close()
