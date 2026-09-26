import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Any

from sqlalchemy.orm import Session
from apscheduler.triggers.date import DateTrigger

from app.database import SessionLocal, get_db
from app.models.models import AuditLog
from app.models.models_quotation import (
    QuotationRequest, QuotationBank, QuotationBankAssignment, QuotationNotification
)
from app.core.email_service import get_customer_email_settings, send_email
from app.services.unified_email_builder import build_quotation_rfq_bank_email
from app.services.quotation_reminder_service import schedule_rfq_15m_reminder

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


async def execute_scheduled_rfq_release_async(rfq_id: str):
    """Async wrapper executed by APScheduler job trigger."""
    logger.info(f"APScheduler trigger fired for scheduled release of RFQ {rfq_id}")
    db = SessionLocal()
    try:
        await broadcast_rfq_to_banks(rfq_id=rfq_id, db=db)
    except Exception as e:
        logger.error(f"Error executing scheduled RFQ release for {rfq_id}: {e}", exc_info=True)
    finally:
        db.close()


async def broadcast_rfq_to_banks(rfq_id: str, db: Optional[Session] = None, base_url: Optional[str] = None) -> dict:
    """
    Executes bank email broadcast and status transition for an RFQ.
    Idempotent: will not re-broadcast if is_dispatched is already True.
    """
    owns_session = False
    if db is None:
        db = SessionLocal()
        owns_session = True

    try:
        rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
        if not rfq:
            logger.warning(f"broadcast_rfq_to_banks: RFQ {rfq_id} not found.")
            return {"status": "error", "message": "RFQ not found"}

        if rfq.is_dispatched:
            logger.info(f"broadcast_rfq_to_banks: RFQ {rfq_id} is already dispatched.")
            return {"status": "skipped", "message": "RFQ already dispatched"}

        now_utc = datetime.now(timezone.utc)

        # Transition status from APPROVED_SCHEDULED or PENDING_APPROVAL to PENDING (active / awaiting window)
        if rfq.status in ("APPROVED_SCHEDULED", "PENDING_APPROVAL"):
            rfq.status = "PENDING"
        for leg in (rfq.legs or []):
            if leg.status in ("APPROVED_SCHEDULED", "PENDING_APPROVAL"):
                leg.status = "PENDING"

        rfq.is_dispatched = True
        rfq.dispatched_at = now_utc
        rfq.scheduled_release_job_id = None
        db.commit()

        # Audit log for dispatch
        from app.crud.base import log_action
        log_action(
            db,
            user_id=rfq.created_by_user_id or 1,
            action_type="QUOTATION_RFQ_DISPATCHED",
            entity_type="QuotationRequest",
            entity_id=None,
            details={
                "rfq_id": str(rfq.id),
                "ref_no": rfq.ref_no,
                "dispatched_at": now_utc.isoformat(),
                "scheduled_release_at": rfq.scheduled_release_at.isoformat() if rfq.scheduled_release_at else None,
                "type": rfq.type,
                "entity_name": rfq.entity.entity_name if rfq.entity else None
            },
            customer_id=rfq.customer_id
        )
        db.commit()

        # Schedule dynamic 15-minute prior reminder relative to window_start
        try:
            schedule_rfq_15m_reminder(rfq_id=rfq.id, window_start=rfq.window_start, release_time=now_utc)
        except Exception as rem_err:
            logger.warning(f"Failed to schedule 15m reminder for RFQ {rfq.id}: {rem_err}")

        # Broadcast emails to assigned banks
        assignments = db.query(QuotationBankAssignment).filter(QuotationBankAssignment.rfq_id == rfq.id).all()
        email_settings, source = get_customer_email_settings(db, rfq.customer_id)

        if not base_url:
            from app.core.routing import get_frontend_base_url
            base_url = get_frontend_base_url()

        customer_branding = (rfq.entity.entity_name if rfq.entity else None) or (rfq.customer.name if rfq.customer else "Treasury Customer")

        emails_sent = 0
        for assignment in assignments:
            bank_row = db.query(QuotationBank).filter(QuotationBank.id == assignment.quotation_bank_id).first()
            if not bank_row:
                continue

            contacts = bank_row.contacts if isinstance(bank_row.contacts, list) and len(bank_row.contacts) > 0 else []
            if not contacts and bank_row.emails:
                contacts = [{"email": e.strip(), "name": "", "role": "EXECUTION"} for e in bank_row.emails.split(',') if e.strip()]
            if not contacts:
                continue

            bank_display_name = bank_row.bank.name if bank_row.bank else "Bank Partner"
            link = f"{base_url}/public-quotation/{assignment.token}"

            all_bank_emails = list(dict.fromkeys(
                c.get("email", "").strip() for c in contacts if c.get("email")
            ))
            approver_emails = list(dict.fromkeys(
                c.get("email", "").strip() for c in contacts 
                if c.get("role") == "APPROVER" and c.get("email")
            ))
            approver_set = {e.lower() for e in approver_emails}
            non_approver_emails = [e for e in all_bank_emails if e.lower() not in approver_set]

            is_indicative = (getattr(rfq, "quotation_base", "") or "").lower() == "indicative" or (getattr(assignment, "quotation_base", "") or "").lower() == "indicative"
            has_approver = len(approver_emails) > 0
            has_execution = any(c.get("role") == "EXECUTION" for c in contacts)

            # Bank approval flow check
            if assignment.approval_status == 'PENDING' and not is_indicative and has_approver and has_execution:
                for app_email in approver_emails:
                    approver_link = f"{base_url}/public-quotation/{assignment.token}?email={app_email}"
                    subject, body = build_quotation_rfq_bank_email(
                        rfq=rfq,
                        assignment=assignment,
                        bank_name=bank_display_name,
                        customer_branding=customer_branding,
                        link=approver_link,
                        email_purpose="BANK_APPROVAL_REQUIRED"
                    )
                    await send_email(db, [app_email], subject, body, {}, email_settings)
                    emails_sent += 1

                if non_approver_emails:
                    subject, body = build_quotation_rfq_bank_email(
                        rfq=rfq,
                        assignment=assignment,
                        bank_name=bank_display_name,
                        customer_branding=customer_branding,
                        link="",
                        email_purpose="BANK_HEADS_UP"
                    )
                    await send_email(db, non_approver_emails, subject, body, {}, email_settings)
                    emails_sent += len(non_approver_emails)
            else:
                if assignment.approval_status == 'PENDING':
                    assignment.approval_status = None
                    db.commit()

                if all_bank_emails:
                    subject, body = build_quotation_rfq_bank_email(
                        rfq=rfq,
                        assignment=assignment,
                        bank_name=bank_display_name,
                        customer_branding=customer_branding,
                        link=link,
                        email_purpose="INVITATION"
                    )
                    await send_email(db, all_bank_emails, subject, body, {}, email_settings)
                    emails_sent += len(all_bank_emails)

        # Notify End User Maker
        if rfq.created_by_user_id:
            db.add(QuotationNotification(
                user_id=rfq.created_by_user_id,
                type="RFQ_DISPATCHED",
                title=f"RFQ {rfq.ref_no} Released to Banks",
                message=f"Your {rfq.type} quotation request has been released and invitations dispatched to banks.",
                link=f"/end-user/quotations/history?rfq_id={rfq.id}",
                is_read=False
            ))
            db.commit()

        logger.info(f"broadcast_rfq_to_banks: Successfully dispatched RFQ {rfq_id} to banks ({emails_sent} recipients).")
        return {"status": "success", "emails_sent": emails_sent, "rfq_id": rfq_id}

    finally:
        if owns_session:
            db.close()


def broadcast_rfq_to_banks_sync(rfq_id: str, db: Optional[Session] = None, base_url: Optional[str] = None) -> dict:
    """Synchronous runner for testing and CLI scripts."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, broadcast_rfq_to_banks(rfq_id=rfq_id, db=db, base_url=base_url)).result()
    else:
        return asyncio.run(broadcast_rfq_to_banks(rfq_id=rfq_id, db=db, base_url=base_url))


def schedule_rfq_bank_release(
    rfq_id: str,
    release_at: datetime,
    scheduler: Optional[Any] = None
) -> Optional[str]:
    """
    Schedules an APScheduler one-shot job to dispatch bank emails at release_at.
    If release_at is already in the past or now, executes broadcast_rfq_to_banks immediately.
    """
    if release_at.tzinfo is None:
        release_at = release_at.replace(tzinfo=timezone.utc)
    else:
        release_at = release_at.astimezone(timezone.utc)

    now_utc = datetime.now(timezone.utc)

    # If scheduled time is now or in the past, dispatch immediately
    if release_at <= now_utc:
        logger.info(f"Scheduled release time for RFQ {rfq_id} is in the past/now. Dispatching immediately.")
        broadcast_rfq_to_banks_sync(rfq_id=rfq_id)
        return None

    target_scheduler = scheduler or get_scheduler()
    if not target_scheduler:
        logger.warning(f"No active scheduler available to register scheduled release for RFQ {rfq_id}. Dispatching immediately.")
        broadcast_rfq_to_banks_sync(rfq_id=rfq_id)
        return None

    job_id = f"rfq_scheduled_release_{rfq_id}"
    try:
        target_scheduler.add_job(
            func=execute_scheduled_rfq_release_async,
            trigger=DateTrigger(run_date=release_at, timezone=timezone.utc),
            id=job_id,
            name=f"RFQ Scheduled Bank Release ({rfq_id})",
            args=[rfq_id],
            replace_existing=True,
            misfire_grace_time=3600
        )
        logger.info(f"Registered APScheduler job '{job_id}' for RFQ {rfq_id} at {release_at.isoformat()}.")
        return job_id
    except Exception as e:
        logger.error(f"Failed to register scheduled release job for RFQ {rfq_id}: {e}", exc_info=True)
        return None


def cancel_scheduled_rfq_release(rfq_id: str, scheduler: Optional[Any] = None) -> bool:
    """Cancels an existing scheduled release job in APScheduler."""
    target_scheduler = scheduler or get_scheduler()
    if not target_scheduler:
        return False

    job_id = f"rfq_scheduled_release_{rfq_id}"
    try:
        job = target_scheduler.get_job(job_id)
        if job:
            target_scheduler.remove_job(job_id)
            logger.info(f"Removed scheduled release job '{job_id}' for RFQ {rfq_id}.")
            return True
    except Exception as e:
        logger.warning(f"Error removing scheduled release job '{job_id}': {e}")
    return False


def sync_pending_scheduled_releases_on_startup(scheduler: Any, session_factory: Any):
    """
    Scans for any RFQs in APPROVED_SCHEDULED state upon server startup
    and restores their one-off DateTrigger release timers on APScheduler.
    """
    logger.info("Syncing pending scheduled RFQ releases on startup...")
    db = session_factory()
    try:
        pending_rfqs = db.query(QuotationRequest).filter(
            QuotationRequest.status == "APPROVED_SCHEDULED",
            QuotationRequest.is_dispatched == False,
            QuotationRequest.is_deleted == False
        ).all()

        now_utc = datetime.now(timezone.utc)
        restored = 0
        dispatched_now = 0

        for rfq in pending_rfqs:
            if not rfq.scheduled_release_at:
                continue

            rel_time = rfq.scheduled_release_at
            if rel_time.tzinfo is None:
                rel_time = rel_time.replace(tzinfo=timezone.utc)

            if rel_time <= now_utc:
                logger.info(f"RFQ {rfq.id} scheduled release time {rel_time.isoformat()} already passed. Dispatching now.")
                broadcast_rfq_to_banks(rfq.id, db=db)
                dispatched_now += 1
            else:
                job_id = schedule_rfq_bank_release(rfq.id, rel_time, scheduler=scheduler)
                if job_id:
                    rfq.scheduled_release_job_id = job_id
                    db.commit()
                    restored += 1

        logger.info(f"Startup scheduled RFQ releases sync complete: {restored} scheduled, {dispatched_now} dispatched immediately.")
    except Exception as e:
        logger.error(f"Error during startup sync of scheduled RFQ releases: {e}", exc_info=True)
    finally:
        db.close()
