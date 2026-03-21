# c:\Grow\app\main.py
import sys
import os
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

# Configure logging
# Defaults to INFO for production, checks env var for DEBUG override
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
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
        "https://www.growbusinessdevelopment.com/",
        "https://www.growbusinessdevelopment.com",
        "https://treasury-frontend-46hip9jex-waels-projects-e59ad1d5.vercel.app/",
        "https://treasury-frontend-46hip9jex-waels-projects-e59ad1d5.vercel.app",
        "https://treasury-frontend-nu.vercel.app/",
        "https://treasury-frontend-nu.vercel.app",
        "http://localhost",
        "http://localhost:3000",
        "http://127.0.0.1",
        "http://127.0.0.1:3000",
    ]

    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

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
        public, issuance_endpoints, public_issuance, reports, facility_endpoints,
        quotations_endpoints, public_quotations, reconciliation_endpoints,
        notification_endpoints
    )
    from app.auth_v2.routers import router as auth_v2_router
    from app.crud.crud import crud_customer, crud_customer_configuration, log_action
    
    # --- Database Initialization ---
    try:
        # Import models to register them with Base.metadata
        import app.models.models
        import app.models.models_quotation
        import app.models.models_reconciliation_v2
        import app.models.models_notification
        
        if Base.metadata.tables:
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables verified/created.")
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
    from app.core.security import require_issuance_module, require_custody_module
    fastapi_app.include_router(issuance_endpoints.router, prefix="/api/v1/issuance", tags=["Issuance Module"], dependencies=[Depends(require_issuance_module)])
    fastapi_app.include_router(facility_endpoints.router, prefix="/api/v1/facilities", tags=["Facilities"], dependencies=[Depends(require_issuance_module)])
    fastapi_app.include_router(public_issuance.router, prefix="/api/v1/public-issuance", tags=["Public Issuance Portal"])
    fastapi_app.include_router(notification_endpoints.router, prefix="/api/v1/notifications", tags=["Notifications"])
    
    # Feature Toggle Dependency logic
    from app.core.security import get_current_user
    def require_customer_one(current_user = Depends(get_current_user)):
        if getattr(current_user, "customer_id", None) != 1:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Module is currently disabled under feature flag."
            )

    fastapi_app.include_router(
        quotations_endpoints.router, 
        prefix="/api/v1/end-user/quotations", 
        tags=["Quotation Module"],
        dependencies=[Depends(require_customer_one)]
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
        dependencies=[Depends(require_customer_one)]
    )

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
            }
        ]

        for job in jobs:
            if job.get("trigger_type") == "hourly":
                trigger = CronTrigger(minute=job["minute"], timezone=EGYPT_TIMEZONE)
                schedule_desc = f"every hour at minute {job['minute']}"
            elif job.get("trigger_type") == "minutely":
                trigger = CronTrigger(minute='*', timezone=EGYPT_TIMEZONE)
                schedule_desc = "every minute"
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
                args=[job["func"]] + job["args"]
            )
            logger.info(f"Scheduled '{job['name']}' {schedule_desc} EEST.")

        scheduler.start()
        logger.info("APScheduler started.")

    @fastapi_app.on_event("shutdown")
    async def shutdown_scheduler():
        scheduler.shutdown()
        logger.info("APScheduler shut down.")

    @fastapi_app.get("/")
    async def root():
        return {"message": "Treasury Management Platform API is running!"}

# Call the configuration
configure_app_instance(app)