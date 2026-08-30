# c:\Grow\app\main.py
import sys
import os
import re
import logging
import pytz
from datetime import datetime, timedelta

# FastAPI imports
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

# SQLAlchemy imports
from sqlalchemy.exc import SQLAlchemyError

# APScheduler Imports
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Database imports
from app.database import get_db, Base, engine

# ==============================================================================
# Production-Ready Log Filter: masks sensitive metadata from all log output
# ==============================================================================
class SensitiveDataFilter(logging.Filter):
    """Regex-based filter that replaces sensitive values in log messages."""

    def __init__(self):
        super().__init__()
        self._patterns: list = []

        # 1. GCP Project ID (from env)
        for env_key in ("GCP_PROJECT_ID", "DOCUMENT_AI_PROJECT_ID"):
            val = os.getenv(env_key, "")
            if val:
                self._patterns.append((re.compile(re.escape(val)), "[PROJECT_ID]"))

        # 2. Document AI Processor ID (from env)
        proc_id = os.getenv("DOCUMENT_AI_PROCESSOR_ID", "")
        if proc_id:
            self._patterns.append((re.compile(re.escape(proc_id)), "[PROCESSOR_ID]"))

        # 3. GCS bucket name (from env)
        bucket = os.getenv("GCS_BUCKET_NAME", "")
        if bucket:
            self._patterns.append((re.compile(re.escape(bucket)), "[BUCKET]"))

        # 4. Windows local user paths: C:\Users\<username>\...
        self._patterns.append((
            re.compile(r"[A-Z]:\\Users\\[^\\]+\\[^\s\"']+"),
            "[LOCAL_PATH]",
        ))

        # 5. Linux/Mac home paths: /home/<user>/...
        self._patterns.append((
            re.compile(r"/home/[^/]+/\S+"),
            "[LOCAL_PATH]",
        ))

        # 6. Service-account JSON file references
        self._patterns.append((
            re.compile(r"[\w./-]+\.json", re.IGNORECASE),
            "[CREDENTIALS_FILE]",
        ))

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            for pattern, replacement in self._patterns:
                record.msg = pattern.sub(replacement, record.msg)
        # Also scrub formatted args if they were already interpolated
        if record.args:
            try:
                formatted = record.getMessage()
                for pattern, replacement in self._patterns:
                    formatted = pattern.sub(replacement, formatted)
                record.msg = formatted
                record.args = None
            except Exception:
                pass
        return True

# Configure logging
# Defaults to INFO for production, checks env var for DEBUG override
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))

# Attach the filter to the root logger so every module inherits it
_sensitive_filter = SensitiveDataFilter()
for handler in logging.root.handlers:
    handler.addFilter(_sensitive_filter)

logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Treasury Management Platform API",
    description="API for managing financial instruments, primarily Letters of Guarantee.",
    version="1.0.0",
)

def configure_app_instance(fastapi_app: FastAPI):
    # --- Middleware Configuration ---
    origins = [
        "https://www.growbusinessdevelopment.com",
        "https://growbusinessdevelopment.com",
        "https://staging.growbusinessdevelopment.com",
        "https://demo.growbusinessdevelopment.com",
        "https://treasury-frontend-nu.vercel.app",
        "http://localhost",
        "http://localhost:3000",
        "http://127.0.0.1",
        "http://127.0.0.1:3000",
    ]

    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_origin_regex=r"^https:\/\/([a-zA-Z0-9_-]+\.)*(onrender\.com|vercel\.app|growbusinessdevelopment\.com)$",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from app.core.security_headers import SecurityHeadersMiddleware
    fastapi_app.add_middleware(SecurityHeadersMiddleware)

    # --- Module Imports ---
    # Imports are placed here to ensure app structure is ready or to avoid circular deps.
    # If these fail, the app will naturally crash with ImportError.
    import app.core.security as app_security
    import app.core.ai_integration as app_ai_integration
    import app.core.document_generator as app_document_generator
    import app.core.hashing as app_hashing
    import app.core.email_service as app_email_service
    import app.core.background_tasks as app_background_tasks
    import app.crud.subscription_tasks as subscription_tasks
    
    from app.api.v1.endpoints import (
        system_owner, corporate_admin, end_user, migration, 
        public, public_issuance, reports, facility_endpoints,
        quotations_endpoints, public_quotations, reconciliation_endpoints,
        notification_endpoints, ai_query_assistant, user_feedback,
        inbox_endpoints, system_holidays_endpoints
    )

    from app.api.v1.endpoints import issuance as issuance_package
    from app.auth_v2.routers import router as auth_v2_router
    from app.crud.crud import crud_customer, crud_customer_configuration, log_action
    
    # --- Database Initialization ---
    try:
        # Import models to register them with Base.metadata
        import app.models.models
        import app.models.models_quotation
        import app.models.models_reconciliation_v2
        import app.models.models_notification
        import app.models.models_inbox
        
        if Base.metadata.tables:
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables verified/created.")

            try:
                from sqlalchemy.orm import Session as DBSession
                from app.models.models import GlobalConfiguration
                from app.constants import GlobalConfigKey
                with DBSession(engine) as seed_db:
                    existing = seed_db.query(GlobalConfiguration).filter(
                        GlobalConfiguration.key == GlobalConfigKey.QUOTATION_APPROVAL_REQUIRED
                    ).first()
                    if not existing:
                        new_cfg = GlobalConfiguration(
                            key=GlobalConfigKey.QUOTATION_APPROVAL_REQUIRED,
                            value_default="false",
                            unit="boolean",
                            description="Require Corporate Admin approval before releasing quotation requests to banks",
                            module_tags=["quotations"]
                        )
                        seed_db.add(new_cfg)
                        seed_db.commit()
                        logger.info("Seeded QUOTATION_APPROVAL_REQUIRED into global_configurations.")
            except Exception as seed_err:
                logger.warning(f"Global configuration seed check skipped: {seed_err}")

            # --- System Health Watchdog: Startup & Crash / Reboot Detection ---
            try:
                from sqlalchemy.orm import Session as DBSession
                from app.core.telemetry_service import record_startup_watchdog
                with DBSession(engine) as watchdog_db:
                    record_startup_watchdog(watchdog_db)
            except Exception as w_err:
                logger.warning(f"Startup watchdog check skipped: {w_err}")

            # --- Auto-Sync Letter Templates from Disk → DB ---
            try:
                from sqlalchemy.orm import Session as DBSession
                from app.models import Template
                import re as _re

                templates_folder = os.path.join(os.path.dirname(__file__), "templates")
                if os.path.isdir(templates_folder):
                    with DBSession(engine) as tpl_db:
                        updated_count = 0
                        created_count = 0
                        for entry in os.scandir(templates_folder):
                            if not (entry.is_file() and entry.name.endswith("_template.html")):
                                continue

                            filename = entry.name
                            customer_match = _re.match(r"CustomerID_(\d+)_(.*)_template\.html", filename)

                            if customer_match:
                                customer_id = int(customer_match.group(1))
                                action_type = customer_match.group(2)
                                is_global = False
                            else:
                                action_type = filename.replace("_template.html", "")
                                customer_id = None
                                is_global = True

                            with open(entry.path, "r", encoding="utf-8") as f:
                                new_html = f.read()

                            query = tpl_db.query(Template).filter(
                                Template.action_type == action_type,
                                Template.is_notification_template == False,
                                Template.is_global == is_global,
                                Template.is_deleted == False
                            )
                            if not is_global:
                                query = query.filter(Template.customer_id == customer_id)

                            tpl = query.first()
                            if tpl:
                                # Update existing template if content differs
                                if tpl.content != new_html:
                                    tpl.content = new_html
                                    updated_count += 1
                            else:
                                # Create new template record from disk file
                                human_name = action_type.replace("_", " ").title() + " Instruction"
                                owner_label = f" (Customer {customer_id})" if not is_global else ""
                                new_tpl = Template(
                                    name=human_name + owner_label,
                                    template_type="LETTER",
                                    action_type=action_type,
                                    content=new_html,
                                    language="EN",
                                    is_global=is_global,
                                    customer_id=customer_id,
                                    is_notification_template=False,
                                    is_default=True,
                                )
                                tpl_db.add(new_tpl)
                                created_count += 1
                                logger.info(f"Template auto-sync: created '{action_type}'{owner_label} from disk.")

                        tpl_db.commit()
                        parts = []
                        if updated_count:
                            parts.append(f"updated {updated_count}")
                        if created_count:
                            parts.append(f"created {created_count}")
                        if parts:
                            logger.info(f"Template auto-sync: {', '.join(parts)} template(s).")
                        else:
                            logger.info("Template auto-sync: all templates up-to-date.")
            except Exception as tpl_err:
                logger.warning(f"Template auto-sync skipped: {tpl_err}")
        else:
            logger.critical("FATAL: No SQLAlchemy models registered. Tables cannot be created.")
            sys.exit(1)
            
    except SQLAlchemyError as e:
        logger.critical(f"FATAL: Database error during table creation: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.critical(f"FATAL: Unexpected error during startup: {e}", exc_info=True)
        sys.exit(1)

    # --- Router Registration ---
    fastapi_app.include_router(system_owner.router, prefix="/api/v1/system-owner")
    fastapi_app.include_router(corporate_admin.router, prefix="/api/v1/corporate-admin")
    fastapi_app.include_router(end_user.router, prefix="/api/v1/end-user")
    fastapi_app.include_router(migration.router, prefix="/api/v1/corporate-admin") # Check if this prefix overlap is intentional
    fastapi_app.include_router(auth_v2_router, prefix="/api/v1")
    fastapi_app.include_router(auth_v2_router, prefix="/api/v2")
    fastapi_app.include_router(reports.router, prefix="/api/v1")
    fastapi_app.include_router(public.router, prefix="/api/v1/public")
    from app.core.security import require_issuance_module, require_custody_module, require_issuance_or_custody_module
    fastapi_app.include_router(issuance_package.router, prefix="/api/v1/issuance", tags=["Issuance Module"], dependencies=[Depends(require_issuance_or_custody_module)])
    fastapi_app.include_router(facility_endpoints.router, prefix="/api/v1/facilities", tags=["Facilities"], dependencies=[Depends(require_issuance_module)])
    fastapi_app.include_router(public_issuance.router, prefix="/api/v1/public-issuance", tags=["Public Issuance Portal"])
    fastapi_app.include_router(notification_endpoints.router, prefix="/api/v1/notifications", tags=["Notifications"])
    fastapi_app.include_router(ai_query_assistant.router, prefix="/api/v1/ai-query-assistant", tags=["AI Data Query Assistant (Experimental)"])
    fastapi_app.include_router(user_feedback.router, prefix="/api/v1/feedback", tags=["User Feedback"])

    
    from app.core.security import require_quotation_module, require_reconciliation_module

    fastapi_app.include_router(
        quotations_endpoints.router, 
        prefix="/api/v1/end-user/quotations", 
        tags=["Quotation Module"],
        dependencies=[Depends(require_quotation_module)]
    )
    fastapi_app.include_router(
        public_quotations.router, 
        prefix="/api/v1/public-quotation", 
        tags=["Public Quotation Webhooks"]
        # Intentionally leaving out the dependency here so bank callbacks still function in background for existing records
    )
    fastapi_app.include_router(
        reconciliation_endpoints.router, 
        prefix="/api/v1/reconciliation", 
        tags=["Reconciliation Engine"],
        dependencies=[Depends(require_reconciliation_module)]
    )
    fastapi_app.include_router(
        inbox_endpoints.router,
        prefix="/api/v1/inbox",
        tags=["Smart Inbox"]
    )
    fastapi_app.include_router(
        system_holidays_endpoints.router,
        prefix="/api/v1"
    )

    # --- Static Files Mounting for Supporting Uploads ---
    from fastapi.staticfiles import StaticFiles
    os.makedirs("uploads/quotations", exist_ok=True)
    os.makedirs("uploads/inbox", exist_ok=True)
    fastapi_app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

    # --- APScheduler Setup ---
    scheduler = AsyncIOScheduler()
    fastapi_app.state.scheduler = scheduler
    EGYPT_TIMEZONE = pytz.timezone('Africa/Cairo')

    async def job_wrapper(task_func, *args, **kwargs):
        """Wraps scheduled tasks to provide a database session."""
        logger.info(f"Scheduler triggering {task_func.__name__}.")
        
        # Use a fresh session for every job execution
        db_session = next(get_db())
        try:
            await task_func(db_session, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error in scheduled task {task_func.__name__}: {e}", exc_info=True)
        finally:
            db_session.close()

    @fastapi_app.on_event("startup")
    async def start_scheduler():
        """Define and start cron jobs."""
        from app.crud.crud_quotation import crud_quotation
        
        # Mapping of jobs to their configuration for cleaner setup
        jobs = [
            {
                "func": app_background_tasks.run_daily_undelivered_instructions_report,
                "id": "undelivered_report_daily_job",
                "name": "Daily Undelivered Instructions Report",
                "minute": 0,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_print_reminders,
                "id": "print_reminders_daily_job",
                "name": "Daily Print Reminders",
                "minute": 5,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_renewal_reminders,
                "id": "renewal_reminders_daily_job",
                "name": "Daily Renewal Reminders",
                "minute": 10,
                "args": []
            },
            {
                "func": subscription_tasks.run_daily_subscription_status_update,
                "id": "subscription_status_daily_job",
                "name": "Daily Subscription Status",
                "minute": 15,
                "args": [log_action, crud_customer, crud_customer_configuration]
            },
            {
                "func": app_background_tasks.run_daily_lg_status_update,
                "id": "lg_status_daily_job",
                "name": "Daily LG Status Update",
                "minute": 20,
                "args": []
            },
            {
                "func": app_background_tasks.run_hourly_cbe_news_sync,
                "id": "cbe_news_hourly_job",
                "name": "Hourly CBE News Sync",
                "minute": 0,         # Run at the start of the hour
                "trigger_type": "hourly",
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_exchange_rate_sync,
                "id": "exchange_rate_daily_job",
                "name": "Daily CBE Exchange Rate Sync",
                "hours": [15, 23], # Run at 3 PM and 11 PM
                "minute": 0,
                "args": []
            },
            {
                "func": crud_quotation.process_quotation_timeouts,
                "id": "quotation_timeouts_minute_job",
                "name": "Quotation Processing (5-hourly)",
                "hours": [0, 5, 10, 15, 20],
                "minute": 0,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_issuance_lg_expiry_reminders,
                "id": "issuance_lg_expiry_reminders_daily_job",
                "name": "Daily Issuance LG Expiry Reminders",
                "minute": 25,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_reference_expiry_check,
                "id": "reference_expiry_check_daily_job",
                "name": "Daily Reference Expiry Check",
                "minute": 30,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_facility_utilization_alerts,
                "id": "facility_utilization_alerts_daily_job",
                "name": "Daily Facility Utilization Alerts",
                "minute": 35,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_sla_breach_alerts,
                "id": "sla_breach_alerts_daily_job",
                "name": "Daily SLA Breach Alerts",
                "minute": 40,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_maintenance_delivery_reminders,
                "id": "maintenance_delivery_reminders_daily_job",
                "name": "Daily Maintenance Delivery Reminders",
                "minute": 45,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_reconciliation_reminders,
                "id": "reconciliation_reminders_daily_job",
                "name": "Daily Reconciliation Overdue Reminders",
                "minute": 50,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_issuance_maintenance_reminders,
                "id": "issuance_maintenance_reminders_daily_job",
                "name": "Daily Issuance Maintenance Reminders",
                "minute": 55,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_issuance_approval_timeout,
                "id": "issuance_approval_timeout_daily_job",
                "name": "Daily Issuance Approval Timeout",
                "minute": 56,
                "args": []
            },
            {
                "func": app_background_tasks.run_daily_auto_reject_expired_requests,
                "id": "auto_reject_expired_requests_daily_job",
                "name": "Daily Core Approval Requests Auto-Rejection",
                "minute": 58,
                "args": []
            },
            {
                "func": app_background_tasks.run_inbox_email_poll,
                "id": "inbox_poll_working_hours_job",
                "name": "Smart Inbox Email Polling (Working Hours)",
                "cron_kwargs": {
                    "day_of_week": "sun,mon,tue,wed,thu",
                    "hour": "8-17",
                    "minute": "*/5"
                },
                "args": []
            },
            {
                "func": app_background_tasks.run_inbox_email_poll,
                "id": "inbox_poll_off_hours_workdays_job",
                "name": "Smart Inbox Email Polling (Off-Hours Workdays)",
                "cron_kwargs": {
                    "day_of_week": "sun,mon,tue,wed,thu",
                    "hour": "0-7,18-23",
                    "minute": "0,30"
                },
                "args": []
            },
            {
                "func": app_background_tasks.run_inbox_email_poll,
                "id": "inbox_poll_weekends_job",
                "name": "Smart Inbox Email Polling (Weekends)",
                "cron_kwargs": {
                    "day_of_week": "fri,sat",
                    "hour": "*",
                    "minute": "0,30"
                },
                "args": []
            },
            {
                "func": app_background_tasks.run_inbox_scheduled_outbound,
                "id": "inbox_scheduled_outbound_daily_job",
                "name": "Smart Inbox Scheduled Outbound Requests",
                "hours": [8],
                "minute": 0,
                "args": []
            }
        ]

        for job in jobs:
            if "cron_kwargs" in job:
                trigger = CronTrigger(**job["cron_kwargs"], timezone=EGYPT_TIMEZONE)
                schedule_desc = f"with cron parameters {job['cron_kwargs']}"
            elif job.get("trigger_type") == "hourly":
                trigger = CronTrigger(minute=job["minute"], timezone=EGYPT_TIMEZONE)
                schedule_desc = f"every hour at minute {job['minute']}"
            elif job.get("trigger_type") == "minutely":
                trigger = CronTrigger(minute='*', timezone=EGYPT_TIMEZONE)
                schedule_desc = "every minute"
            elif job.get("trigger_type") == "custom_cron":
                trigger = CronTrigger(minute=job.get("cron_custom_minute", "0"), timezone=EGYPT_TIMEZONE)
                schedule_desc = f"at minutes [{job.get('cron_custom_minute')}] every hour"
            else:
                # NEW LOGIC: Support multiple hours
                # If 'hours' is a list, join them (e.g., "15,23"), else use default 2
                run_hours = job.get("hours", 2)
                if isinstance(run_hours, list):
                    run_hours_str = ",".join(map(str, run_hours))
                else:
                    run_hours_str = str(run_hours)
                
                trigger = CronTrigger(hour=run_hours_str, minute=job["minute"], timezone=EGYPT_TIMEZONE)
                schedule_desc = f"daily at hours [{run_hours_str}] at minute {job['minute']}"

            scheduler.add_job(
                func=job_wrapper,
                trigger=trigger,
                id=job["id"],
                name=job["name"],
                args=[job["func"]] + job["args"],
                misfire_grace_time=3600
            )
            logger.info(f"Scheduled '{job['name']}' {schedule_desc} EEST.")

        scheduler.start()
        logger.info("APScheduler started.")

    @fastapi_app.on_event("shutdown")
    async def shutdown_scheduler():
        try:
            from sqlalchemy.orm import Session as DBSession
            from app.core.telemetry_service import record_shutdown_watchdog
            with DBSession(engine) as shutdown_db:
                record_shutdown_watchdog(shutdown_db)
        except Exception as sd_err:
            logger.warning(f"Shutdown watchdog logging skipped: {sd_err}")
        scheduler.shutdown()
        logger.info("APScheduler shut down.")

    @fastapi_app.get("/")
    async def root():
        return {"message": "Treasury Management Platform API is running!"}

# Call the configuration
configure_app_instance(app)