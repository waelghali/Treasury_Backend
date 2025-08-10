# c:\Grow\app\main.py
import sys
import os
import logging
from datetime import datetime, timedelta

# FastAPI imports
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

# SQLAlchemy imports
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError, ProgrammingError, SQLAlchemyError

# APScheduler Imports
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

# Database imports
from app.database import get_db, Base, engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app GLOBALLY AT THE VERY BEGINNING
app = FastAPI(
    title="Treasury Management Platform API",
    description="API for managing financial instruments, primarily Letters of Guarantee.",
    version="1.0.0",
)


# Define a function to configure the app (middleware, routers, event handlers)
def configure_app_instance(fastapi_app: FastAPI):
    # CORS Middleware for frontend communication
    origins = [
        "https://treasury-frontend.vercel.app",
        "treasury-frontend-8btnica55-waels-projects-e59ad1d5.vercel.app",
        "https://treasury-frontend-8btnica55-waels-projects-e59ad1d5.vercel.app",
        "https://treasury-frontend-3sky3divg-waels-projects-e59ad1d5.vercel.app/",
        "https://treasury-frontend-3sky3divg-waels-projects-e59ad1d5.vercel.app,"
        "https://*.vercel.app",
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

    # Attempt to load core modules using absolute imports directly from 'app' package
    try:
        import app.core.security as app_security
        import app.core.ai_integration as app_ai_integration
        import app.core.document_generator as app_document_generator
        import app.core.hashing as app_hashing
        import app.core.email_service as app_email_service
        import app.core.background_tasks as app_background_tasks

        logger.info("Core modules (security, ai_integration, document_generator, hashing, email_service, background_tasks) pre-loaded using absolute paths.")
    except Exception as e:
        logger.critical(f"FATAL ERROR: Could not pre-load core modules. Ensure paths and dependencies are correct. Error: {e}", exc_info=True)
        sys.exit(1)

    # Now import API routers using absolute paths from 'app' package
    from app.api.v1.endpoints import system_owner, corporate_admin, end_user
    from app.auth_v2.routers import router as auth_v2_router
    from app.api.v1.endpoints import reports
    # NEW: Import the subscription tasks module
    from app.crud import subscription_tasks
    from app.crud.crud import crud_customer, crud_customer_configuration, log_action

    # --- DATABASE TABLE CREATION & DIAGNOSTICS ---
    logger.info("Attempting to ensure database tables exist (create if not existing)...")
    try:
        logger.info(f"DIAG: Engine DSN: {engine.url.render_as_string(hide_password=True)}")

        import app.models

        if Base.metadata.tables:
            logger.info(f"DIAG: Number of models registered with Base.metadata: {len(Base.metadata.tables)}")
            logger.info(f"DIAG: Tables expected by models: {list(Base.metadata.tables.keys())}")
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables ensured (created if not existing).")
        else:
            logger.critical("FATAL ERROR: No SQLAlchemy models were registered with Base.metadata. "
                            "Database tables cannot be created. Please check app/models.py and its imports in app/database.py.")
            sys.exit(1)

    except OperationalError as e:
        logger.critical(f"FATAL ERROR: Database connection failed during table creation. "
                        f"Please check DATABASE_URL and ensure PostgreSQL is running and accessible. Error: {e}", exc_info=True)
        sys.exit(1)
    except ProgrammingError as e:
        logger.critical(f"FATAL ERROR: Database programming error during table creation. "
                        f"This could indicate schema definition issues or insufficient user permissions. Error: {e}", exc_info=True)
        sys.exit(1)
    except SQLAlchemyError as e:
        logger.critical(f"FATAL ERROR: An SQLAlchemy error occurred during table creation: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.critical(f"FATAL ERROR: An unexpected error occurred during database table creation: {e}", exc_info=True)
        sys.exit(1)
    # --- END DATABASE TABLE CREATION & DIAGNOSTICS ---


    # Include API routers
    fastapi_app.include_router(system_owner.router, prefix="/api/v1/system-owner")
    fastapi_app.include_router(corporate_admin.router, prefix="/api/v1/corporate-admin")
    fastapi_app.include_router(end_user.router, prefix="/api/v1/end-user")
    fastapi_app.include_router(auth_v2_router, prefix="/api/v2")
    fastapi_app.include_router(reports.router, prefix="/api/v1")

    # --- APScheduler Setup and Event Handlers ---
    scheduler = AsyncIOScheduler()
    fastapi_app.state.scheduler = scheduler

    EGYPT_TIMEZONE = pytz.timezone('Africa/Cairo')

    def get_db_session_for_scheduler():
        db_session = None
        try:
            db_session = next(get_db())
            yield db_session
        finally:
            if db_session:
                db_session.close()

    async def job_wrapper(task_func, *args, **kwargs):
        logger.info(f"Scheduler triggering {task_func.__name__}.")
        for db_session in get_db_session_for_scheduler():
            try:
                # Pass the db_session as the first argument, followed by other args
                await task_func(db_session, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in scheduled task {task_func.__name__}: {e}", exc_info=True)
            finally:
                pass

    @fastapi_app.on_event("startup")
    async def start_scheduler():
        """Start the APScheduler and schedule tasks to run daily."""
        if not hasattr(app_background_tasks, 'run_daily_undelivered_instructions_report'):
            logger.critical("Background tasks module not fully loaded, cannot schedule jobs.")
            sys.exit(1)

        scheduler.add_job(
            func=job_wrapper,
            trigger=CronTrigger(hour=2, minute=0, timezone=EGYPT_TIMEZONE),
            id='undelivered_report_daily_job',
            name='Daily Undelivered Instructions Report',
            args=[app_background_tasks.run_daily_undelivered_instructions_report]
        )
        logger.info("Scheduled 'Daily Undelivered Instructions Report' to run every day at 2:00 AM EEST.")

        scheduler.add_job(
            func=job_wrapper,
            trigger=CronTrigger(hour=2, minute=5, timezone=EGYPT_TIMEZONE),
            id='print_reminders_daily_job',
            name='Daily Print Reminders and Escalations',
            args=[app_background_tasks.run_daily_print_reminders]
        )
        logger.info("Scheduled 'Daily Print Reminders and Escalations' to run every day at 2:05 AM EEST.")

        scheduler.add_job(
            func=job_wrapper,
            trigger=CronTrigger(hour=2, minute=10, timezone=EGYPT_TIMEZONE),
            id='renewal_reminders_daily_job',
            name='Daily LG Renewal Reminders',
            args=[app_background_tasks.run_daily_renewal_reminders]
        )
        logger.info("Scheduled 'Daily LG Renewal Reminders' to run every day at 2:10 AM EEST.")

        # NEW: Schedule the subscription status update task
        scheduler.add_job(
            func=job_wrapper,
            trigger=CronTrigger(hour=2, minute=15, timezone=EGYPT_TIMEZONE),
            id='subscription_status_daily_job',
            name='Daily Subscription Status Update',
            # Pass the dependencies as arguments
            args=[subscription_tasks.run_daily_subscription_status_update, log_action, crud_customer, crud_customer_configuration]
        )
        logger.info("Scheduled 'Daily Subscription Status Update' to run every day at 2:15 AM EEST.")

        scheduler.start()
        logger.info("APScheduler started and daily background tasks have been scheduled.")

    @fastapi_app.on_event("shutdown")
    async def shutdown_scheduler():
        """Shut down the APScheduler gracefully when the FastAPI application shuts down."""
        scheduler.shutdown()
        logger.info("APScheduler shut down.")

    # --- END APScheduler Setup ---

    @fastapi_app.get("/")
    async def root():
        return {"message": "Treasury Management Platform API is running!"}

# Call the configuration function at the module level, passing the 'app' instance
configure_app_instance(app)
