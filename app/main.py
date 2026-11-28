# c:\Grow\app\main.py
from dotenv import load_dotenv
load_dotenv()

import sys
import os
import re
import asyncio
import logging
import pytz
from datetime import datetime, timedelta

# Apply Linux glibc memory allocator tuning early so >=64KB buffers use mmap
# and are returned to the OS immediately when freed.
if sys.platform.startswith("linux"):
    try:
        import ctypes as _ctypes
        _libc = _ctypes.CDLL("libc.so.6")
        _libc.mallopt(-1, 65536)  # M_TRIM_THRESHOLD = 64KB
        _libc.mallopt(-3, 65536)  # M_MMAP_THRESHOLD = 64KB
        _libc.mallopt(-8, 2)      # M_ARENA_MAX = 2
    except Exception:
        pass

# FastAPI imports
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware

# SQLAlchemy imports
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

# APScheduler Imports
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Database imports
from app.database import get_db, Base, engine, SessionLocal

# ==============================================================================
# Production-Ready Log Filter: masks sensitive metadata from all log output
# ==============================================================================
class SensitiveDataFilter(logging.Filter):
    """Regex-based filter that replaces sensitive values in log messages."""

    def __init__(self, preserve_args: bool = False):
        super().__init__()
        # preserve_args=True scrubs each arg in place instead of collapsing them into msg.
        # Required for uvicorn/gunicorn access loggers whose formatters unpack record.args.
        self._preserve_args = preserve_args
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

        # --- Secrets & credentials (run before the generic digit masking below) ---
        # 6a. JWTs (header.payload.signature)
        self._patterns.append((
            re.compile(r"eyJ[A-Za-z0-9_\-]{5,}\.[A-Za-z0-9_\-]{5,}\.[A-Za-z0-9_\-]+"),
            "[JWT]",
        ))

        # 6b. Authorization headers (Bearer, Basic, or raw token) & standalone Bearer tokens
        self._patterns.append((
            re.compile(r"(\bAuthorization\s*[:=]\s*(?:Bearer\s+|Basic\s+)?)[A-Za-z0-9\-._~+/]+=*", re.IGNORECASE),
            r"\1[REDACTED]",
        ))
        self._patterns.append((
            re.compile(r"(\bBearer\s+)[A-Za-z0-9\-._~+/]+=*", re.IGNORECASE),
            r"\1[REDACTED]",
        ))

        # 6c. Fernet-encrypted payloads (portal/invite tokens)
        self._patterns.append((
            re.compile(r"gAAAAA[A-Za-z0-9_\-=]{20,}"),
            "[ENCRYPTED]",
        ))

        # 6d. Credentials embedded in connection strings: scheme://user:password@host
        self._patterns.append((
            re.compile(r"(\b[a-zA-Z][\w+.\-]*://[^:/\s@]+:)\S+@"),
            r"\1[REDACTED]@",
        ))

        # 6e. Secret-bearing query params, e.g. ?token=abc&otp=123 (anchored to ?/& to avoid prose like "code=200")
        self._patterns.append((
            re.compile(r"([?&](?:token|token_hint|access_token|refresh_token|otp|code|password|secret|signature|sig|api_key|key)=)[^&\s\"']+", re.IGNORECASE),
            r"\1[REDACTED]",
        ))

        # 6f. Smart Inbox Sync Watcher drop tokens (gws_...)
        self._patterns.append((
            re.compile(r"\bgws_[A-Za-z0-9_\-]{16,}(?![A-Za-z0-9_\-])"),
            "gws_[REDACTED]",
        ))

        # 6f. Secret-bearing key/value pairs in dicts/JSON/kwargs
        self._patterns.append((
            re.compile(
                r"""(['"]?(?:password|passwd|new_password|secret|client_secret|api_key|smtp_password|access_token|refresh_token|session_token|otp|otp_code|mfa_code|raw_code)['"]?\s*[:=]\s*['"]?)([^'",\s}&]+)""",
                re.IGNORECASE,
            ),
            r"\1[REDACTED]",
        ))

        # 6g. Opaque tokens as URL path segments, e.g. /public/quotations/<token>/live-rank
        self._patterns.append((
            re.compile(r"(?<=/)[A-Za-z0-9_\-]{24,}(?=[/?#\s\"']|$)"),
            "[TOKEN]",
        ))

        # 7. LG numbers in key/value form, e.g. 'lg_number': '5600772000008294'
        self._patterns.append((
            re.compile(r"""((?:lg_number|lgNumber|lg_number_hint)['"]?\s*[:=]\s*['"]?)([A-Za-z0-9/_\-]+)"""),
            lambda m: m.group(1) + self._mask(m.group(2)),
        ))

        # 8. LG numbers following the word "LG", e.g. "LG 5600772000008294" / "LG No: ABC-12345".
        #    Token must contain a digit and be >= 5 chars, so "LG id=42" is left untouched.
        self._patterns.append((
            re.compile(r"""\b(LG\s+(?:number\s*|No\.?\s*|#\s*)?[:#]?\s*['"]?)(?=[A-Za-z0-9/\-]*\d)([A-Za-z0-9/\-]{5,})"""),
            lambda m: m.group(1) + self._mask(m.group(2)),
        ))

        # 9. Catch-all: any long digit run (10+ digits) - bank LG/account numbers
        self._patterns.append((
            re.compile(r"(?<![\w*])\d{10,}(?!\w)"),
            lambda m: self._mask(m.group(0)),
        ))

        # 10. Email addresses: keep first char + domain, e.g. j***@acme.com
        self._patterns.append((
            re.compile(r"\b([A-Za-z0-9])[A-Za-z0-9._%+\-]*@([A-Za-z0-9.\-]+\.[A-Za-z]{2,})\b"),
            r"\1***@\2",
        ))

        # 11. Audit comparison values (Excel='...' / AI='...') if ever logged
        self._patterns.append((
            re.compile(r"\b(Excel|AI)='[^']*'"),
            r"\1='[REDACTED]'",
        ))

        # 12. Migration staging paths
        self._patterns.append((
            re.compile(r"(migration_(?:staging|attachments)[/\\])[^\s\"']+"),
            r"\1[REDACTED_FILE]",
        ))

    @staticmethod
    def _mask(value: str) -> str:
        """Keep only the last 4 characters of an identifier."""
        return "****" + value[-4:] if len(value) > 6 else "****"

    def _scrub(self, text: str) -> str:
        for pattern, replacement in self._patterns:
            text = pattern.sub(replacement, text)
        return text

    def filter(self, record: logging.LogRecord) -> bool:
        if record.args:
            try:
                if self._preserve_args:
                    if isinstance(record.msg, str):
                        record.msg = self._scrub(record.msg)
                    if isinstance(record.args, tuple):
                        record.args = tuple(self._scrub(a) if isinstance(a, str) else a for a in record.args)
                    elif isinstance(record.args, dict):
                        record.args = {k: (self._scrub(v) if isinstance(v, str) else v) for k, v in record.args.items()}
                else:
                    record.msg = self._scrub(record.getMessage())
                    record.args = None
            except Exception:
                if isinstance(record.msg, str):
                    record.msg = self._scrub(record.msg)
        elif isinstance(record.msg, str):
            record.msg = self._scrub(record.msg)
        # Scrub tracebacks: pre-render exc_text so the handler's Formatter reuses the cleaned copy
        if record.exc_info:
            try:
                if not record.exc_text:
                    record.exc_text = logging.Formatter().formatException(record.exc_info)
                record.exc_text = self._scrub(record.exc_text)
            except Exception:
                pass
        if record.stack_info:
            record.stack_info = self._scrub(record.stack_info)
        return True

# Configure logging
# Defaults to INFO for production, checks env var for DEBUG override
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))

# Attach the filter to the root logger so every module inherits it
_sensitive_filter = SensitiveDataFilter()
for handler in logging.root.handlers:
    handler.addFilter(_sensitive_filter)

# Server loggers don't propagate to root, so attach directly (logger-level filters survive
# gunicorn/uvicorn swapping handlers). Access logs carry portal tokens in URLs.
_server_log_filter = SensitiveDataFilter(preserve_args=True)
for _server_logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "gunicorn.error", "gunicorn.access"):
    logging.getLogger(_server_logger_name).addFilter(_server_log_filter)

logger = logging.getLogger(__name__)

# Initialize FastAPI app
# In production (e.g. Render), hide public API docs from automated scanners unless explicitly enabled via ENABLE_DOCS=true
_is_prod = (os.getenv("ENVIRONMENT", "").lower() == "production") or (os.getenv("RENDER") is not None)
_enable_docs = os.getenv("ENABLE_DOCS", "").lower() in ("true", "1") or not _is_prod

app = FastAPI(
    title="Treasury Management Platform API",
    description="API for managing financial instruments, primarily Letters of Guarantee.",
    version="1.0.0",
    docs_url="/docs" if _enable_docs else None,
    redoc_url="/redoc" if _enable_docs else None,
    openapi_url="/openapi.json" if _enable_docs else None,
)

def configure_app_instance(fastapi_app: FastAPI):
    # --- Middleware Configuration ---
    origins = [
        "https://www.growbusinessdevelopment.com",
        "https://growbusinessdevelopment.com",
        "https://staging.growbusinessdevelopment.com",
        "https://demo.growbusinessdevelopment.com",

        "http://localhost",
        "http://localhost:3000",
        "http://127.0.0.1",
        "http://127.0.0.1:3000",
    ]
    for env_origin_key in ("FRONTEND_URL", "ADDITIONAL_CORS_ORIGINS"):
        raw_env_origins = os.getenv(env_origin_key, "")
        for extra_origin in raw_env_origins.split(","):
            cleaned = extra_origin.strip().rstrip("/")
            if cleaned and cleaned not in origins:
                origins.append(cleaned)

    cors_origin_regex = os.getenv(
        "CORS_ORIGIN_REGEX",
        r"^https:\/\/([a-zA-Z0-9_-]+\.)*growbusinessdevelopment\.com$",
    )

    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_origin_regex=cors_origin_regex,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from app.core.security_headers import SecurityHeadersMiddleware
    from app.core.trial_guard import TrialRequestStateMiddleware
    from app.core.cloudflare_guard import CloudflareGuardMiddleware
    fastapi_app.add_middleware(SecurityHeadersMiddleware)
    fastapi_app.add_middleware(CloudflareGuardMiddleware)
    fastapi_app.add_middleware(TrialRequestStateMiddleware)

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
        quotations_endpoints, public_quotations, bank_dealer_endpoints, reconciliation_endpoints,
        notification_endpoints, ai_query_assistant, user_feedback,
        inbox_endpoints, system_holidays_endpoints, campaign_endpoints,
        document_streaming, public_demo_chat
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
        import app.models.models_push
        import app.models.models_inbox
        import app.models.models_campaign
        import app.models.models_field_provenance
        
        # Dedicated short-lived engine for startup DDL/seeding. A lock_timeout guarantees that
        # a stale session holding locks (orphaned uvicorn worker, hung test, open pgAdmin txn)
        # makes startup log a warning instead of blocking forever on ALTER TABLE.
        from sqlalchemy import create_engine as _create_startup_engine
        from sqlalchemy.pool import NullPool as _StartupNullPool
        from app.database import sqlalchemy_json_serializer as _startup_json_default
        import json as _startup_json
        _startup_lock_timeout_ms = int(os.getenv("STARTUP_DB_LOCK_TIMEOUT_MS", "3000"))
        _startup_blockers_reported = {"done": False}
        startup_engine = _create_startup_engine(
            engine.url,
            poolclass=_StartupNullPool,
            connect_args={
                "options": f"-c lock_timeout={_startup_lock_timeout_ms}",
                "application_name": f"grow-startup-pid{os.getpid()}",
            },
            json_serializer=lambda obj: _startup_json.dumps(obj, default=_startup_json_default),
            hide_parameters=True,
        )

        def _log_startup_lock_blockers(stmt_preview: str, err: Exception) -> None:
            """On lock timeout, surface WHO is holding the lock so it can be killed."""
            orig = getattr(err, "orig", None)
            if getattr(orig, "pgcode", None) != "55P03":  # lock_not_available
                return
            logger.warning(
                f"Startup migration skipped (lock timeout after {_startup_lock_timeout_ms}ms): {stmt_preview}"
            )
            if _startup_blockers_reported["done"]:
                return
            _startup_blockers_reported["done"] = True
            try:
                with startup_engine.connect() as diag:
                    rows = diag.execute(text(
                        "SELECT pid, application_name, state, now() - xact_start AS xact_age, left(query, 100) AS query "
                        "FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() "
                        "AND state = 'idle in transaction' ORDER BY xact_start"
                    )).fetchall()
                for r in rows:
                    logger.warning(
                        f"  Possible blocker -> pg pid={r.pid} app='{r.application_name}' state={r.state} "
                        f"xact_age={r.xact_age} last_query={r.query!r}"
                    )
                if rows:
                    logger.warning(
                        "  Stop the stale process (orphaned uvicorn worker / hung test) or run "
                        "SELECT pg_terminate_backend(<pid>); then restart."
                    )
            except Exception:
                pass

        from sqlalchemy import text
        if Base.metadata.tables:
            Base.metadata.create_all(bind=startup_engine)
            logger.info("Database tables verified/created.")
            try:
                from sqlalchemy import text
                migrations = [
                    "ALTER TABLE banks ADD COLUMN IF NOT EXISTS portal_access_enabled BOOLEAN NOT NULL DEFAULT TRUE;",
                    "ALTER TABLE quotation_offers ADD COLUMN IF NOT EXISTS leg_id VARCHAR;",
                    "ALTER TABLE quotation_offers ADD COLUMN IF NOT EXISTS encrypted_price VARCHAR(255);",
                    "ALTER TABLE quotation_offers ADD COLUMN IF NOT EXISTS encrypted_spread VARCHAR(255);",
                    "ALTER TABLE quotation_offers ADD COLUMN IF NOT EXISTS submitted_by_email VARCHAR;",
                    "ALTER TABLE quotation_offers ALTER COLUMN price DROP NOT NULL;",
                    "ALTER TABLE quotation_tbill_offers ADD COLUMN IF NOT EXISTS leg_id VARCHAR;",
                    "ALTER TABLE quotation_tbill_offers ADD COLUMN IF NOT EXISTS encrypted_discount_rate VARCHAR(255);",
                    "ALTER TABLE quotation_tbill_offers ADD COLUMN IF NOT EXISTS encrypted_max_amount VARCHAR(255);",
                    "ALTER TABLE quotation_tbill_offers ADD COLUMN IF NOT EXISTS submitted_by_email VARCHAR;",
                    "ALTER TABLE quotation_tbill_offers ALTER COLUMN discount_rate DROP NOT NULL;",
                    "ALTER TABLE quotation_tbill_offers ALTER COLUMN max_amount DROP NOT NULL;",
                    "ALTER TABLE quotation_legs ADD COLUMN IF NOT EXISTS encrypted_winner_rate VARCHAR(255);",
                    "ALTER TABLE quotation_legs ADD COLUMN IF NOT EXISTS encrypted_saved_vs_avg VARCHAR(255);",
                    "ALTER TABLE quotation_legs ADD COLUMN IF NOT EXISTS market_benchmark_snapshot JSONB;",
                    "ALTER TABLE quotation_rfqs ADD COLUMN IF NOT EXISTS encrypted_winner_rate VARCHAR(255);",
                    "ALTER TABLE quotation_rfqs ADD COLUMN IF NOT EXISTS encrypted_saved_vs_avg VARCHAR(255);",
                    "ALTER TABLE quotation_rfqs ADD COLUMN IF NOT EXISTS market_benchmark_snapshot JSONB;",
                    "ALTER TABLE quotation_anonymous_benchmarks ALTER COLUMN cbe_benchmark_rate DROP NOT NULL;",
                    "ALTER TABLE banks ADD COLUMN IF NOT EXISTS allow_dealer_comments BOOLEAN NOT NULL DEFAULT TRUE;",
                    "ALTER TABLE banks ADD COLUMN IF NOT EXISTS allow_dealer_feedback BOOLEAN NOT NULL DEFAULT TRUE;",
                    "ALTER TABLE banks ADD COLUMN IF NOT EXISTS allow_dealer_mobile_alerts BOOLEAN NOT NULL DEFAULT TRUE;",
                    "ALTER TABLE banks ADD COLUMN IF NOT EXISTS allow_manual_recording BOOLEAN NOT NULL DEFAULT FALSE;",
                    "ALTER TABLE dealer_alert_reminders ADD COLUMN IF NOT EXISTS otp_code VARCHAR(255);",
                    "ALTER TABLE dealer_alert_reminders ADD COLUMN IF NOT EXISTS otp_expires_at TIMESTAMPTZ;",
                    "ALTER TABLE dealer_alert_reminders ADD COLUMN IF NOT EXISTS otp_failed_attempts INTEGER NOT NULL DEFAULT 0;",
                    "ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS reference_key VARCHAR(64);",
                    "ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS urgency VARCHAR(10) NOT NULL DEFAULT 'normal';",
                    "ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;",
                    "CREATE INDEX IF NOT EXISTS ix_user_notifications_reference_key ON user_notifications (reference_key);",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'AUTO_ACCEPT_SINGLE_QUOTE';",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, is_deleted, created_at, updated_at) VALUES ('AUTO_ACCEPT_SINGLE_QUOTE', NULL, NULL, 'false', 'boolean', 'When Auto-Accept is active, controls whether uncontested single quotes can be auto-executed (true) or halted for manual corporate sign-off (false). Invariant: cannot be true if Default Action is Auto-Reject.', '[\"quotation\", \"quotations\"]'::jsonb, false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'FACILITY_SCORE_WEIGHT_RARITY';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'FACILITY_SCORE_WEIGHT_URGENT_RARITY';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'WEEKEND_DAYS_SCHEME';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'DEFAULT_CALENDAR_COUNTRY';",
                    "ALTER TABLE quotation_bank_contact_invitations ADD COLUMN IF NOT EXISTS encrypted_email TEXT;",
                    "ALTER TABLE quotation_bank_contact_invitations ADD COLUMN IF NOT EXISTS email_blind_index VARCHAR(64);",
                    "ALTER TABLE quotation_bank_contact_invitations ADD COLUMN IF NOT EXISTS token_hash VARCHAR(64);",
                    "ALTER TABLE quotation_bank_contact_invitations ADD COLUMN IF NOT EXISTS otp_hash VARCHAR(128);",
                    "ALTER TABLE quotation_bank_contact_invitations ADD COLUMN IF NOT EXISTS otp_expires_at TIMESTAMPTZ;",
                    "ALTER TABLE quotation_bank_contact_invitations ADD COLUMN IF NOT EXISTS otp_last_sent_at TIMESTAMPTZ;",
                    "ALTER TABLE quotation_bank_contact_invitations ADD COLUMN IF NOT EXISTS failed_otp_attempts INTEGER NOT NULL DEFAULT 0;",
                    "ALTER TABLE quotation_bank_contact_invitations ALTER COLUMN email DROP NOT NULL;",
                    "ALTER TABLE quotation_bank_contact_invitations ALTER COLUMN token DROP NOT NULL;",
                    "ALTER TABLE email_classification_history ADD COLUMN IF NOT EXISTS original_classification VARCHAR;",
                    "ALTER TABLE email_classification_history ADD COLUMN IF NOT EXISTS attachment_column_signature VARCHAR;",
                    "ALTER TABLE email_classification_history ADD COLUMN IF NOT EXISTS confidence_score INTEGER;",
                    "ALTER TABLE email_classification_history ALTER COLUMN confidence DROP NOT NULL;",
                    "UPDATE quotation_rfqs SET status = 'COMPLETED', acceptance_status = 'NO_QUOTES', acceptance_deadline = NULL, acceptance_resolved_at = NULL, acceptance_resolved_by_user_id = NULL WHERE status = 'EVALUATING' AND window_end < NOW() AND (acceptance_deadline IS NULL OR acceptance_deadline < NOW()) AND NOT EXISTS (SELECT 1 FROM quotation_offers qo JOIN quotation_bank_assignments qba ON qo.assignment_id = qba.id WHERE qba.rfq_id = quotation_rfqs.id);",
                    "UPDATE quotation_rfqs SET status = 'COMPLETED' WHERE status = 'INCONCLUSIVE';",
                    "UPDATE quotation_legs SET status = 'COMPLETED' WHERE status = 'INCONCLUSIVE';",
                    "ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS balance_continuity_status VARCHAR(40) DEFAULT 'VALIDATED';",
                    "ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS balance_variance_amount NUMERIC(18,2) DEFAULT 0;",
                    "ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS continuity_warnings_json JSON;",
                    "ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS detected_mapping_json JSON;",
                    "ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS erp_book_closing_balance NUMERIC(18,2);",
                    "ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS brs_signed_by INTEGER;",
                    "ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS brs_signed_at TIMESTAMPTZ;",
                    "ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS brs_signoff_notes TEXT;",
                    "ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS brs_worm_hash VARCHAR(64);",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS tokenized_description TEXT;",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS encrypted_raw_description TEXT;",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS encrypted_counterparty_name TEXT;",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS encrypted_custom_fields TEXT;",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS custom_fields_json JSON;",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS amount_tier VARCHAR(20);",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS iban_bank_code VARCHAR(10);",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS auto_confirmed BOOLEAN DEFAULT FALSE;",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS companion_parent_id INTEGER;",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS classification_reasoning TEXT;",
                    "ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS top_suggestions_json JSON;",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS bank_id INTEGER;",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS rule_origin VARCHAR(30) DEFAULT 'MANUAL';",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS lifecycle_state VARCHAR(30) DEFAULT 'AUTO_CONFIRMED';",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS signature_hash VARCHAR(64);",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS assigned_category VARCHAR(100);",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS assigned_sub_category VARCHAR(100);",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS confirmations_count INTEGER DEFAULT 1;",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS overrides_count INTEGER DEFAULT 0;",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS empirical_precision NUMERIC(5,2) DEFAULT 100.00;",
                    "ALTER TABLE classification_rules ADD COLUMN IF NOT EXISTS calibrated_confidence INTEGER DEFAULT 85;",
                    "ALTER TABLE classification_rules ALTER COLUMN created_by DROP NOT NULL;",
                    "ALTER TABLE reconciliation_counterparties ADD COLUMN IF NOT EXISTS blind_token VARCHAR(64);",
                    "ALTER TABLE reconciliation_counterparties ADD COLUMN IF NOT EXISTS encrypted_name TEXT;",
                    "ALTER TABLE collaborative_patterns ADD COLUMN IF NOT EXISTS amount_tier VARCHAR(20) DEFAULT 'ANY';",
                    "ALTER TABLE collaborative_patterns ADD COLUMN IF NOT EXISTS semantic_flags_json JSON;",
                    "ALTER TABLE collaborative_patterns ADD COLUMN IF NOT EXISTS rejection_count INTEGER DEFAULT 0;",
                    "ALTER TABLE internal_ledger_records ADD COLUMN IF NOT EXISTS blind_entity_token VARCHAR(64);",
                    "ALTER TABLE internal_ledger_records ADD COLUMN IF NOT EXISTS encrypted_entity_name TEXT;",
                    "ALTER TABLE internal_ledger_records ADD COLUMN IF NOT EXISTS custom_fields_json JSON;",
                    "ALTER TABLE internal_ledger_records ADD COLUMN IF NOT EXISTS encrypted_custom_fields TEXT;",
                    "ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_lg_category_id INTEGER REFERENCES lg_categories(id) ON DELETE SET NULL;",
                    """CREATE TABLE IF NOT EXISTS scheduler_paused_jobs (
                        job_id VARCHAR(128) PRIMARY KEY,
                        paused_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        paused_by_user_id INTEGER NULL
                    );""",
                    "SELECT setval(pg_get_serial_sequence('permissions', 'id'), COALESCE((SELECT MAX(id) FROM permissions), 1), true);",
                    "SELECT setval(pg_get_serial_sequence('role_permissions', 'id'), COALESCE((SELECT MAX(id) FROM role_permissions), 1), true);",
                    "INSERT INTO permissions (name, description) SELECT 'system_owner:pause_scheduler_job', 'Allows pausing scheduled background jobs.' WHERE NOT EXISTS (SELECT 1 FROM permissions WHERE name = 'system_owner:pause_scheduler_job');",
                    "INSERT INTO permissions (name, description) SELECT 'legal_artifact:view', 'Allows viewing legal artifacts.' WHERE NOT EXISTS (SELECT 1 FROM permissions WHERE name = 'legal_artifact:view');",
                    "INSERT INTO role_permissions (role, permission_id) SELECT 'system_owner', p.id FROM permissions p WHERE p.name = 'system_owner:pause_scheduler_job' AND NOT EXISTS (SELECT 1 FROM role_permissions rp WHERE rp.role = 'system_owner' AND rp.permission_id = p.id);",
                    "INSERT INTO role_permissions (role, permission_id) SELECT 'system_owner', p.id FROM permissions p WHERE p.name = 'legal_artifact:view' AND NOT EXISTS (SELECT 1 FROM role_permissions rp WHERE rp.role = 'system_owner' AND rp.permission_id = p.id);",
                    "ALTER TABLE global_configurations ADD COLUMN IF NOT EXISTS config_scope VARCHAR(40) NOT NULL DEFAULT 'CUSTOMER_CONFIGURABLE';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'LOGIN_MAX_FAILED_ATTEMPTS';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'LOGIN_LOCKOUT_DURATION_MINUTES';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'OTP_MAX_FAILED_ATTEMPTS';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'OTP_LOCKOUT_DURATION_MINUTES';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'PUBLIC_ISSUANCE_SESSION_EXPIRY_MINUTES';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'INVITE_LINK_EXPIRY_HOURS';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'PASSWORD_MIN_LENGTH';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'PASSWORD_REQUIRE_UPPERCASE';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'PASSWORD_REQUIRE_LOWERCASE';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'PASSWORD_REQUIRE_DIGIT';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'PASSWORD_RESET_TOKEN_EXPIRY_MINUTES';",
                    "ALTER TYPE globalconfigkey ADD VALUE IF NOT EXISTS 'GRACE_PERIOD_DAYS';",
                    "UPDATE global_configurations SET config_scope = 'PLATFORM_GLOBAL' WHERE key::text IN ('TC_VERSION','PP_VERSION','WEEKEND_DAYS_SCHEME','DEFAULT_CALENDAR_COUNTRY','INBOX_POLL_INTERVAL_MINUTES','INBOX_GENERIC_DOMAIN_BLACKLIST','CBE_OVERNIGHT_LENDING_RATE','CBE_OVERNIGHT_DEPOSIT_RATE','CBE_MID_CORRIDOR_RATE','TEMP_DOC_TEST');",
                    "UPDATE global_configurations SET config_scope = 'SYSTEM_CUSTOMER_OVERRIDE' WHERE key::text IN ('PASSWORD_MIN_LENGTH','PASSWORD_REQUIRE_UPPERCASE','PASSWORD_REQUIRE_LOWERCASE','PASSWORD_REQUIRE_DIGIT','PASSWORD_RESET_TOKEN_EXPIRY_MINUTES','LOGIN_MAX_FAILED_ATTEMPTS','LOGIN_LOCKOUT_DURATION_MINUTES','OTP_MAX_FAILED_ATTEMPTS','OTP_LOCKOUT_DURATION_MINUTES','PUBLIC_ISSUANCE_SESSION_EXPIRY_MINUTES','INVITE_LINK_EXPIRY_HOURS','GRACE_PERIOD_DAYS');",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('PASSWORD_MIN_LENGTH', '8', '64', '8', 'characters', 'Minimum required password length for user accounts.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('PASSWORD_REQUIRE_UPPERCASE', NULL, NULL, 'true', 'boolean', 'Require at least one uppercase letter in passwords.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('PASSWORD_REQUIRE_LOWERCASE', NULL, NULL, 'true', 'boolean', 'Require at least one lowercase letter in passwords.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('PASSWORD_REQUIRE_DIGIT', NULL, NULL, 'true', 'boolean', 'Require at least one numeric digit in passwords.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('PASSWORD_RESET_TOKEN_EXPIRY_MINUTES', '5', '1440', '15', 'minutes', 'Expiration time in minutes for password reset email links.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('LOGIN_MAX_FAILED_ATTEMPTS', '3', '20', '5', 'attempts', 'Maximum consecutive failed login attempts before temporary account lockout.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('LOGIN_LOCKOUT_DURATION_MINUTES', '1', '1440', '15', 'minutes', 'Lockout duration in minutes after exceeding maximum failed login attempts.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('OTP_MAX_FAILED_ATTEMPTS', '3', '10', '5', 'attempts', 'Maximum failed OTP verification attempts before rate-limit lockout on public portals.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('OTP_LOCKOUT_DURATION_MINUTES', '1', '1440', '15', 'minutes', 'OTP lockout duration in minutes after exceeding failed verification attempts.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, config_scope, is_deleted, created_at, updated_at) VALUES ('GRACE_PERIOD_DAYS', '0', '90', '30', 'days', 'Subscription grace period in days after expiration before read-only/lockout.', NULL, 'SYSTEM_CUSTOMER_OVERRIDE', false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING;",
                    "UPDATE global_configurations SET unit = 'boolean', value_min = 'False', value_max = 'True', value_default = CASE WHEN LOWER(COALESCE(value_default, 'true')) IN ('false', '0', 'no', 'off') THEN 'false' ELSE 'true' END WHERE key::text = 'LG_COPY_VERIFICATION_REQUIRED' AND (unit IS DISTINCT FROM 'boolean' OR value_min IS DISTINCT FROM 'False' OR value_max IS DISTINCT FROM 'True');",
                    "UPDATE global_configurations SET unit = 'boolean', value_min = 'False', value_max = 'True', value_default = CASE WHEN LOWER(COALESCE(value_default, '1')) IN ('false', '0', 'no', 'off') THEN 'false' ELSE 'true' END, description = REPLACE(COALESCE(description, ''), ' (1=on, 0=off)', '') WHERE key::text IN ('FACILITY_UTILIZATION_ALERT_THRESHOLD_80', 'FACILITY_UTILIZATION_ALERT_THRESHOLD_90', 'FACILITY_UTILIZATION_ALERT_THRESHOLD_100') AND (unit IS DISTINCT FROM 'boolean' OR value_default IN ('0', '1') OR value_min IS DISTINCT FROM 'False');",
                    "UPDATE customer_configurations cc SET configured_value = CASE WHEN LOWER(TRIM(cc.configured_value)) IN ('false', '0', 'no', 'off') THEN 'false' ELSE 'true' END FROM global_configurations gc WHERE cc.global_config_id = gc.id AND gc.key::text IN ('LG_COPY_VERIFICATION_REQUIRED', 'FACILITY_UTILIZATION_ALERT_THRESHOLD_80', 'FACILITY_UTILIZATION_ALERT_THRESHOLD_90', 'FACILITY_UTILIZATION_ALERT_THRESHOLD_100') AND cc.configured_value IS NOT NULL AND cc.configured_value NOT IN ('true', 'false');",
                    "UPDATE global_configurations SET value_min = 'False', value_max = 'True' WHERE unit = 'boolean' AND key::text != 'QUOTATION_ACCEPTANCE_DEFAULT_ACTION' AND (value_min IS NULL OR value_max IS NULL OR value_min = 'false' OR value_max = 'true');",
                    "UPDATE global_configurations SET unit = 'days', value_min = COALESCE(value_min, '1'), value_max = COALESCE(value_max, '180') WHERE key::text IN ('ISSUANCE_LG_EXPIRY_FIRST_REMINDER_DAYS', 'REFERENCE_EXPIRY_REMINDER_DAYS') AND (unit IS NULL OR value_min IS NULL OR value_max IS NULL);",
                    "UPDATE global_configurations SET unit = 'days', value_min = COALESCE(value_min, '1'), value_max = COALESCE(value_max, '90') WHERE key::text = 'ISSUANCE_LG_EXPIRY_SECOND_REMINDER_DAYS' AND (unit IS NULL OR value_min IS NULL OR value_max IS NULL);",
                    "UPDATE global_configurations SET unit = 'days', value_min = COALESCE(value_min, '1'), value_max = COALESCE(value_max, '60') WHERE key::text = 'ISSUANCE_LG_EXPIRY_REMINDER_INTERVAL' AND (unit IS NULL OR value_min IS NULL OR value_max IS NULL);",
                    "UPDATE global_configurations SET unit = 'days' WHERE key::text = 'INBOX_DEFAULT_POSITION_REQUEST_DAY' AND unit = 'day';",
                    "UPDATE global_configurations SET unit = 'percentage' WHERE key::text IN ('CBE_MID_CORRIDOR_RATE', 'CBE_OVERNIGHT_DEPOSIT_RATE', 'CBE_OVERNIGHT_LENDING_RATE') AND (unit IS NULL OR unit = '');",
                    "UPDATE global_configurations SET unit = 'string' WHERE key::text = 'INBOX_GENERIC_DOMAIN_BLACKLIST' AND (unit IS NULL OR unit = '');",
                    "ALTER TABLE customer_bank_accounts ADD COLUMN IF NOT EXISTS currency_code VARCHAR(3) DEFAULT 'EGP';",
                    "ALTER TABLE customer_bank_accounts ADD COLUMN IF NOT EXISTS erp_main_gl_account VARCHAR(100);",
                    "ALTER TABLE customer_bank_accounts ADD COLUMN IF NOT EXISTS erp_clearing_gl_account VARCHAR(100);",
                    "ALTER TABLE customer_bank_accounts ADD COLUMN IF NOT EXISTS statement_pdf_password_encrypted TEXT;",
                    "ALTER TABLE customer_bank_accounts ADD COLUMN IF NOT EXISTS auto_ingest_statements BOOLEAN NOT NULL DEFAULT FALSE;",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS handoff_mode VARCHAR(40) DEFAULT 'ALL';",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS offset_account_mode VARCHAR(40) DEFAULT 'MAIN_BANK_GL';",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS clearing_suspense_gl_account VARCHAR(100) DEFAULT '1010999 - Bank Clearing Suspense';",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS cash_in_transit_gl_account VARCHAR(100) DEFAULT '1010800 - Cash in Transit';",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS suspense_gl_account VARCHAR(100) DEFAULT '1010950 - Unapplied Cash Suspense';",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS wht_tax_gl_account VARCHAR(100) DEFAULT '1090300 - Withholding Tax Receivable';",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS fx_variance_gl_account VARCHAR(100) DEFAULT '6060100 - FX Gain / Loss';",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS split_variance_as_fee_row BOOLEAN DEFAULT FALSE;",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS exclude_paired_reversals BOOLEAN DEFAULT TRUE;",
                    "ALTER TABLE reconciliation_export_profiles ADD COLUMN IF NOT EXISTS account_gl_mappings_json JSON;",
                    "ALTER TABLE inbox_items ADD COLUMN IF NOT EXISTS extracted_balances_json JSON;",
                    "ALTER TABLE inbox_items ADD COLUMN IF NOT EXISTS ingestion_channel VARCHAR(30) NOT NULL DEFAULT 'IMAP_EMAIL';",
                    "ALTER TABLE external_request_tokens ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;",
                    "ALTER TABLE external_request_tokens ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;",
                    "CREATE INDEX IF NOT EXISTS ix_external_request_tokens_is_deleted ON external_request_tokens (is_deleted);",
                    "ALTER TABLE lg_records DROP CONSTRAINT IF EXISTS uq_lg_record_number;",
                    "DROP INDEX IF EXISTS uq_lg_record_number;",
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_lg_record_cust_bank_num_active ON lg_records (customer_id, issuing_bank_id, lower(lg_number)) WHERE is_deleted = false;",
                ]
                with startup_engine.connect() as conn:
                    for stmt in migrations:
                        try:
                            conn.execute(text(stmt))
                            conn.commit()
                        except Exception as stmt_err:
                            conn.rollback()
                            logger.debug(f"Schema migration statement ignored ({stmt[:40]}...): {stmt_err}")
                            _log_startup_lock_blockers(stmt[:80], stmt_err)
                try:
                    from sqlalchemy.orm import Session as DBSession
                    with DBSession(startup_engine) as weight_seed_db:
                        has_rarity = weight_seed_db.execute(
                            text("SELECT 1 FROM global_configurations WHERE key::text = 'FACILITY_SCORE_WEIGHT_RARITY' LIMIT 1")
                        ).first()
                        if not has_rarity:
                            rows_norm = dict(weight_seed_db.execute(
                                text("SELECT key::text, value_default FROM global_configurations WHERE key::text IN ('FACILITY_SCORE_WEIGHT_COST','FACILITY_SCORE_WEIGHT_MARGIN','FACILITY_SCORE_WEIGHT_SLA','FACILITY_SCORE_WEIGHT_CAPACITY','FACILITY_SCORE_WEIGHT_CURRENCY_MATCH') AND is_deleted IS NOT TRUE")
                            ).fetchall())
                            if not rows_norm:
                                rarity_def = '15'
                            elif (
                                rows_norm.get('FACILITY_SCORE_WEIGHT_COST') == '30'
                                and rows_norm.get('FACILITY_SCORE_WEIGHT_MARGIN') == '15'
                                and rows_norm.get('FACILITY_SCORE_WEIGHT_SLA') == '15'
                                and rows_norm.get('FACILITY_SCORE_WEIGHT_CAPACITY') == '20'
                                and rows_norm.get('FACILITY_SCORE_WEIGHT_CURRENCY_MATCH') == '20'
                            ):
                                weight_seed_db.execute(text("UPDATE global_configurations SET value_default = '25', updated_at = NOW() WHERE key::text = 'FACILITY_SCORE_WEIGHT_COST'"))
                                weight_seed_db.execute(text("UPDATE global_configurations SET value_default = '15', updated_at = NOW() WHERE key::text IN ('FACILITY_SCORE_WEIGHT_CAPACITY', 'FACILITY_SCORE_WEIGHT_CURRENCY_MATCH')"))
                                rarity_def = '15'
                            else:
                                rarity_def = '0'
                            weight_seed_db.execute(
                                text("INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, is_deleted, created_at, updated_at) VALUES ('FACILITY_SCORE_WEIGHT_RARITY', '0', '100', :def_val, 'percentage', 'Weight for preserving rare/specialized credit lines (cross-border, open-ended, multi-currency, rare LG types) for requests that genuinely need them while prioritizing dedicated project or standard common lines first.', '[\"issuance\"]'::jsonb, false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING"),
                                {"def_val": rarity_def},
                            )
                            weight_seed_db.commit()

                        has_urg_rarity = weight_seed_db.execute(
                            text("SELECT 1 FROM global_configurations WHERE key::text = 'FACILITY_SCORE_WEIGHT_URGENT_RARITY' LIMIT 1")
                        ).first()
                        if not has_urg_rarity:
                            rows_urg = dict(weight_seed_db.execute(
                                text("SELECT key::text, value_default FROM global_configurations WHERE key::text IN ('FACILITY_SCORE_WEIGHT_URGENT_COST','FACILITY_SCORE_WEIGHT_URGENT_MARGIN','FACILITY_SCORE_WEIGHT_URGENT_SLA','FACILITY_SCORE_WEIGHT_URGENT_CAPACITY','FACILITY_SCORE_WEIGHT_URGENT_CURRENCY_MATCH') AND is_deleted IS NOT TRUE")
                            ).fetchall())
                            if not rows_urg:
                                urg_rarity_def = '10'
                            elif (
                                rows_urg.get('FACILITY_SCORE_WEIGHT_URGENT_COST') == '15'
                                and rows_urg.get('FACILITY_SCORE_WEIGHT_URGENT_MARGIN') == '10'
                                and rows_urg.get('FACILITY_SCORE_WEIGHT_URGENT_SLA') == '40'
                                and rows_urg.get('FACILITY_SCORE_WEIGHT_URGENT_CAPACITY') == '15'
                                and rows_urg.get('FACILITY_SCORE_WEIGHT_URGENT_CURRENCY_MATCH') == '20'
                            ):
                                weight_seed_db.execute(text("UPDATE global_configurations SET value_default = '35', updated_at = NOW() WHERE key::text = 'FACILITY_SCORE_WEIGHT_URGENT_SLA'"))
                                weight_seed_db.execute(text("UPDATE global_configurations SET value_default = '15', updated_at = NOW() WHERE key::text = 'FACILITY_SCORE_WEIGHT_URGENT_CURRENCY_MATCH'"))
                                urg_rarity_def = '10'
                            else:
                                urg_rarity_def = '0'
                            weight_seed_db.execute(
                                text("INSERT INTO global_configurations (key, value_min, value_max, value_default, unit, description, module_tags, is_deleted, created_at, updated_at) VALUES ('FACILITY_SCORE_WEIGHT_URGENT_RARITY', '0', '100', :def_val, 'percentage', 'Urgent issuance weight for preserving rare/specialized credit lines when common or project-dedicated lines can fulfill the urgent request.', '[\"issuance\"]'::jsonb, false, NOW(), NOW()) ON CONFLICT (key) DO NOTHING"),
                                {"def_val": urg_rarity_def},
                            )
                            weight_seed_db.commit()
                except Exception as rarity_seed_err:
                    logger.debug(f"Facility rarity weight seed skipped: {rarity_seed_err}")
                try:
                    from sqlalchemy.orm import Session as DBSession
                    from app.services.banking_standards_ontology import banking_standards_ontology
                    from app.core.legal_seed import ensure_legal_artifacts_seeded_in_place
                    with DBSession(startup_engine) as seed_db:
                        banking_standards_ontology.ensure_foundational_knowledge_seeded(seed_db)
                        ensure_legal_artifacts_seeded_in_place(seed_db)
                except Exception as seed_err:
                    logger.debug(f"Foundational banking standards / legal seed skipped: {seed_err}")
            except Exception as e:
                logger.warning(f"Startup schema migration check: {e}")

            # --- System Health Watchdog: Startup & Crash / Reboot Detection ---
            try:
                from sqlalchemy.orm import Session as DBSession
                from app.models import Template
                from app.core.instruction_verification import strip_legacy_template_footers
                import re as _re

                templates_folder = os.path.join(os.path.dirname(__file__), "templates")
                if os.path.isdir(templates_folder):
                    with DBSession(startup_engine) as tpl_db:
                        updated_count = 0
                        created_count = 0
                        sanitized_count = 0
                        custom_templates_by_action = {}

                        for entry in os.scandir(templates_folder):
                            if not (entry.is_file() and entry.name.endswith("_template.html")):
                                continue

                            filename = entry.name
                            customer_match = _re.match(r"CustomerID_(\d+)_(.*)_template\.html", filename)

                            with open(entry.path, "r", encoding="utf-8") as f:
                                new_html = strip_legacy_template_footers(f.read())

                            if customer_match:
                                action_type = customer_match.group(2)
                                custom_templates_by_action[action_type] = new_html
                                continue

                            action_type = filename.replace("_template.html", "")
                            tpl = tpl_db.query(Template).filter(
                                Template.action_type == action_type,
                                Template.is_notification_template == False,
                                Template.is_global == True,
                                Template.is_deleted == False
                            ).first()

                            if tpl:
                                if tpl.content != new_html:
                                    tpl.content = new_html
                                    updated_count += 1
                            else:
                                human_name = action_type.replace("_", " ").title() + " Instruction"
                                new_tpl = Template(
                                    name=human_name,
                                    template_type="LETTER",
                                    action_type=action_type,
                                    content=new_html,
                                    language="EN",
                                    is_global=True,
                                    customer_id=None,
                                    is_notification_template=False,
                                    is_default=True,
                                )
                                tpl_db.add(new_tpl)
                                created_count += 1
                                logger.info(f"Template auto-sync: created global '{action_type}' from disk.")

                        # Update existing customized customer templates in-place regardless of customer_id
                        # across local and production DBs, and strip duplicate footers from ALL letter templates
                        all_letter_tpls = tpl_db.query(Template).filter(
                            Template.is_notification_template == False,
                            Template.is_deleted == False
                        ).all()

                        for tpl in all_letter_tpls:
                            if not tpl.content:
                                continue
                            target_html = tpl.content
                            # If this is a customized customer template built from our unified corporate layout,
                            # keep it synced with the latest layout regardless of which customer_id it has in this DB
                            if (
                                not tpl.is_global
                                and tpl.action_type in custom_templates_by_action
                                and 'class="letter-title"' in target_html
                                and ('class="meta-strip"' in target_html or 'class="ref-bar"' in target_html)
                            ):
                                target_html = custom_templates_by_action[tpl.action_type]

                            # Strip any legacy footer HTML or @bottom-* CSS from every template in the DB
                            target_html = strip_legacy_template_footers(target_html)
                            if target_html != tpl.content:
                                tpl.content = target_html
                                sanitized_count += 1

                        tpl_db.commit()
                        parts = []
                        if updated_count:
                            parts.append(f"updated {updated_count} global")
                        if created_count:
                            parts.append(f"created {created_count} global")
                        if sanitized_count:
                            parts.append(f"sanitized/updated {sanitized_count} DB")
                        if parts:
                            logger.info(f"Template auto-sync: {', '.join(parts)} template(s).")
                        else:
                            logger.info("Template auto-sync: all templates up-to-date.")
            except Exception as tpl_err:
                logger.warning(f"Template auto-sync skipped: {tpl_err}")
            finally:
                # Dispose connections opened in the master process before Gunicorn forks workers
                startup_engine.dispose()
                engine.dispose()
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
    from app.api.v1.endpoints import platform_activity_endpoints
    fastapi_app.include_router(system_owner.router, prefix="/api/v1/system-owner")
    fastapi_app.include_router(
        platform_activity_endpoints.router,
        prefix="/api/v1/corporate-admin/activity",
        tags=["Platform Activity & Controls"],
    )
    fastapi_app.include_router(corporate_admin.router, prefix="/api/v1/corporate-admin")
    fastapi_app.include_router(end_user.router, prefix="/api/v1/end-user")
    fastapi_app.include_router(migration.router, prefix="/api/v1/corporate-admin") # Check if this prefix overlap is intentional
    fastapi_app.include_router(auth_v2_router, prefix="/api/v1")
    fastapi_app.include_router(auth_v2_router, prefix="/api/v2")
    fastapi_app.include_router(reports.router, prefix="/api/v1")
    fastapi_app.include_router(public.router, prefix="/api/v1/public")
    fastapi_app.include_router(public_demo_chat.router, prefix="/api/v1/public-demo", tags=["Public Demo AI Product Expert"])
    from app.api.v1.endpoints import trial_sandbox, field_provenance_endpoints
    fastapi_app.include_router(trial_sandbox.router, prefix="/api/v1/trial-sandbox", tags=["Trial Sandbox Experience"])
    fastapi_app.include_router(field_provenance_endpoints.router, prefix="/api/v1/field-provenance", tags=["LG Field Provenance"])
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
    # Direct quotation document gateway accessible to both Corporate Users & Invited Bank Dealers
    # (Strictly exposes /documents/ticket and /documents/download only; all corporate trading routes require require_quotation_module)
    fastapi_app.include_router(
        quotations_endpoints.document_gateway_router,
        prefix="/api/v1/quotations",
        tags=["Quotation Document Gateway"]
    )
    # Universal Document Security Gateway (Streaming Unsealing across Custody, Issuance, Facilities)
    fastapi_app.include_router(
        document_streaming.router,
        prefix="/api/v1",
        tags=["Universal Document Security Gateway"]
    )

    fastapi_app.include_router(
        public_quotations.router, 
        prefix="/api/v1/public-quotation", 
        tags=["Public Quotation Webhooks"]
        # Intentionally leaving out the dependency here so bank callbacks still function in background for existing records
    )
    fastapi_app.include_router(
        bank_dealer_endpoints.router,
        prefix="/api/v1/bank-dealer",
        tags=["Bank Dealer Permanent Portal"]
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
    fastapi_app.include_router(
        campaign_endpoints.router,
        prefix="/api/v1",
        tags=["Campaigns & Cashback"]
    )

    # --- Protected Legacy Uploads Reader (replaces public StaticFiles mount) ---
    os.makedirs(os.path.join("uploads", "quotations"), exist_ok=True)
    os.makedirs(os.path.join("private_storage", "inbox"), exist_ok=True)
    os.makedirs(os.path.join("private_storage", "ai_chat_logs"), exist_ok=True)
    os.makedirs(os.path.join("private_storage", "public_demo_chat_logs"), exist_ok=True)

    @fastapi_app.get("/uploads/quotations/{legacy_rel_path:path}", include_in_schema=False)
    def serve_legacy_quotation_upload(
        legacy_rel_path: str,
        request: Request,
        token: Optional[str] = None,
        auth_token: Optional[str] = None,
        db: Session = Depends(get_db),
    ):
        from app.api.v1.endpoints.public import serve_document
        target_rel = os.path.join("uploads", "quotations", legacy_rel_path)
        return serve_document(
            request=request,
            file_path=target_rel,
            token=token,
            auth_token=auth_token,
            db=db,
        )

    # --- APScheduler Setup ---
    # Quiet internal APScheduler executor tick spam ("Running job ...", "Job ... executed successfully")
    logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)

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
        except asyncio.CancelledError:
            # Expected during server shutdown / hot-reload — not a real error
            logger.info(f"Scheduled task {task_func.__name__} cancelled (server shutdown or reload).")
        except Exception as e:
            logger.error(f"Error in scheduled task {task_func.__name__}: {e}", exc_info=True)
        finally:
            db_session.close()
            from app.core.ai_integration import _reclaim_system_memory
            _reclaim_system_memory()

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
                "minute": 12,        # Offset slightly from :00 to prevent concurrent memory collision with email polling
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
                "func": app_background_tasks.run_daily_cbe_lending_rate_sync,
                "id": "cbe_lending_rate_daily_job",
                "name": "Daily CBE Policy Interest Rates Sync (9AM & 9PM)",
                "hours": [9, 21],
                "minute": 5,
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
            },
            {
                "func": app_background_tasks.run_daily_memory_maintenance,
                "id": "memory_maintenance_daily_job",
                "name": "Daily Off-Peak Memory Maintenance",
                "hours": [3],
                "minute": 15,
                "args": []
            },
            {
                "func": app_background_tasks.run_quiet_times_dealer_mobile_reminders,
                "id": "quiet_times_dealer_mobile_reminders_job",
                "name": "Quiet-Times Dealer Mobile Alert Reminders",
                "hours": [16],
                "minute": 30,
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

        try:
            from sqlalchemy import text as _text
            with SessionLocal() as pause_db:
                paused_rows = pause_db.execute(
                    _text("SELECT job_id FROM scheduler_paused_jobs")
                ).fetchall()
                restored_paused = 0
                for (paused_job_id,) in paused_rows:
                    if scheduler.get_job(paused_job_id):
                        scheduler.pause_job(paused_job_id)
                        restored_paused += 1
                if restored_paused:
                    logger.info(f"Restored paused state for {restored_paused} scheduled job(s).")
        except Exception as pause_err:
            logger.warning(f"Startup restore of paused scheduler jobs skipped: {pause_err}")

        # Web Push (Notifications Q3). Misconfigured VAPID keys raise here on
        # purpose (fail fast); missing keys just leave push disabled.
        from app.services.notifications.push_config import validate_on_startup
        from app.services.notifications.push_channel import init_push_dispatcher
        validate_on_startup()
        init_push_dispatcher(scheduler, SessionLocal)

        # Timed quotation alerts (Notifications Q4): closing soon / quotes ready / auto-resolved.
        from app.services.notifications.quotation_timers import init_quotation_timers
        init_quotation_timers(scheduler, SessionLocal)

        # Re-register dynamic 15-minute prior quotation reminders for active RFQs
        try:
            from app.services.quotation_reminder_service import sync_pending_rfq_reminders_on_startup
            sync_pending_rfq_reminders_on_startup(scheduler, SessionLocal)
        except Exception as sync_err:
            logger.warning(f"Startup sync of RFQ 15m reminders encountered error: {sync_err}")

        # Re-register scheduled RFQ bank releases
        try:
            from app.services.quotation_release_scheduler import sync_pending_scheduled_releases_on_startup
            sync_pending_scheduled_releases_on_startup(scheduler, SessionLocal)
        except Exception as rel_err:
            logger.warning(f"Startup sync of scheduled RFQ releases encountered error: {rel_err}")

        # Catch up any expired pending approvals missed while server was offline
        try:
            with SessionLocal() as timeout_db:
                await app_background_tasks.run_daily_auto_reject_expired_requests(timeout_db)
                await app_background_tasks.run_daily_issuance_approval_timeout(timeout_db)
        except Exception as timeout_err:
            logger.warning(f"Startup sweep of expired approval requests encountered error: {timeout_err}")

        from app.core.ai_integration import _reclaim_system_memory
        _reclaim_system_memory()
        try:
            import gc as _gc
            if hasattr(_gc, "freeze"):
                _gc.freeze()
        except Exception:
            pass

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
from app.core.ai_integration import _reclaim_system_memory as _boot_reclaim_memory
_boot_reclaim_memory()