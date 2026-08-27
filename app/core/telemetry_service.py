# app/core/telemetry_service.py
import os
import sys
import platform
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text as sql_text

logger = logging.getLogger(__name__)

# Track the exact moment this Python process booted up
PROCESS_BOOT_TIME = datetime.utcnow()
PROCESS_PID = os.getpid()

# In-memory session tracking for memory high-watermark
_LOCAL_PEAK_MEMORY_MB = 0.0


def get_container_memory_limit_mb() -> float:
    """
    Determines the container/process RAM ceiling.
    - If MAX_CONTAINER_MEMORY_MB is explicitly configured in .env, use it.
    - If running on Render (detected via RENDER env vars), default to 512.0 MB.
    - Otherwise, default to 512.0 MB as the standard baseline ceiling.
    """
    custom_limit = os.getenv("MAX_CONTAINER_MEMORY_MB")
    if custom_limit:
        try:
            return float(custom_limit)
        except ValueError:
            pass

    is_render = bool(os.getenv("RENDER") or os.getenv("RENDER_SERVICE_ID") or os.getenv("RENDER_INSTANCE_ID"))
    if is_render:
        return 512.0

    return 512.0


def get_process_memory_stats() -> Dict[str, Any]:
    """
    Cross-platform, zero-dependency process memory telemetry.
    Reads current resident set size (RSS) and peak high-watermark (HWM).
    Supports Linux (/proc/self/status on Render/Docker) and Windows (psapi).
    """
    global _LOCAL_PEAK_MEMORY_MB
    current_rss_mb = 0.0
    peak_rss_mb = 0.0
    engine_type = "Standard Process Memory"

    # 1. Linux / Render / Docker Container environment
    if os.path.exists("/proc/self/status"):
        try:
            with open("/proc/self/status", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        current_rss_mb = round(int(line.split()[1]) / 1024.0, 2)
                    elif line.startswith("VmHWM:"):
                        peak_rss_mb = round(int(line.split()[1]) / 1024.0, 2)
            engine_type = "Linux Kernel /proc/self/status"
        except Exception as e:
            logger.debug(f"Could not read /proc/self/status: {e}")

    # 2. Windows Local Development environment (ctypes psapi)
    if current_rss_mb == 0.0 and sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ('cb', wintypes.DWORD),
                    ('PageFaultCount', wintypes.DWORD),
                    ('PeakWorkingSetSize', ctypes.c_size_t),
                    ('WorkingSetSize', ctypes.c_size_t),
                    ('QuotaPeakPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
                    ('PagefileUsage', ctypes.c_size_t),
                    ('PeakPagefileUsage', ctypes.c_size_t),
                ]

            psapi = ctypes.WinDLL('psapi')
            GetProcessMemoryInfo = psapi.GetProcessMemoryInfo
            GetProcessMemoryInfo.argtypes = (wintypes.HANDLE, ctypes.POINTER(PROCESS_MEMORY_COUNTERS), wintypes.DWORD)
            GetProcessMemoryInfo.restype = wintypes.BOOL

            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
            handle = ctypes.windll.kernel32.GetCurrentProcess()

            if GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                current_rss_mb = round(counters.WorkingSetSize / (1024.0 * 1024.0), 2)
                peak_rss_mb = round(counters.PeakWorkingSetSize / (1024.0 * 1024.0), 2)
                engine_type = "Windows NT K32/psapi"
        except Exception as e:
            logger.debug(f"Could not read Windows memory info: {e}")

    # Update in-memory tracker
    if current_rss_mb > _LOCAL_PEAK_MEMORY_MB:
        _LOCAL_PEAK_MEMORY_MB = current_rss_mb
    if peak_rss_mb < _LOCAL_PEAK_MEMORY_MB:
        peak_rss_mb = _LOCAL_PEAK_MEMORY_MB

    limit_mb = get_container_memory_limit_mb()
    current_percent = round((current_rss_mb / limit_mb) * 100.0, 1) if limit_mb > 0 else 0.0
    peak_percent = round((peak_rss_mb / limit_mb) * 100.0, 1) if limit_mb > 0 else 0.0

    # Determine status & diagnostic message
    if peak_percent >= 88.0 or current_percent >= 88.0:
        status = "CRITICAL"
        status_message = f"Critical memory pressure: Peak reached {peak_rss_mb} MB ({peak_percent}% of {limit_mb:.0f} MB ceiling). OOM termination risk."
        badge = "bg-rose-50 text-rose-700 border-rose-200"
    elif peak_percent >= 70.0 or current_percent >= 70.0:
        status = "WARNING"
        status_message = f"Elevated memory load: Peak reached {peak_rss_mb} MB ({peak_percent}%). Nearing 512MB hosting headroom."
        badge = "bg-amber-50 text-amber-700 border-amber-200"
    else:
        status = "HEALTHY"
        status_message = f"Memory utilization is healthy ({current_rss_mb} MB / {limit_mb:.0f} MB, {current_percent}%)."
        badge = "bg-emerald-50 text-emerald-700 border-emerald-200"

    return {
        "current_mb": current_rss_mb,
        "peak_mb": peak_rss_mb,
        "limit_mb": limit_mb,
        "current_percent": current_percent,
        "peak_percent": peak_percent,
        "status": status,
        "status_message": status_message,
        "badge": badge,
        "engine": engine_type,
        "pid": PROCESS_PID
    }


def format_uptime(seconds: float) -> str:
    """Formats uptime seconds into readable string (e.g., '2d 4h', '3h 25m', '14m 20s')."""
    sec = int(seconds)
    days, sec = divmod(sec, 86400)
    hours, sec = divmod(sec, 3600)
    minutes, sec = divmod(sec, 60)

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0 or days > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or (days == 0 and hours == 0):
        parts.append(f"{minutes}m")
    if days == 0 and hours == 0 and minutes < 5:
        parts.append(f"{sec}s")

    return " ".join(parts) if parts else "< 1m"


def get_system_uptime_stats(db: Optional[Session] = None) -> Dict[str, Any]:
    """
    Calculates uptime and counts unexpected reboots/crashes from the audit trail.
    """
    now = datetime.utcnow()
    uptime_seconds = (now - PROCESS_BOOT_TIME).total_seconds()
    uptime_human = format_uptime(uptime_seconds)

    restarts_24h = 0
    recent_events: List[Dict[str, Any]] = []

    if db is not None:
        try:
            # Query audit logs for lifecycle and reboot events in last 7 days
            from app.models.models import AuditLog
            last_24h = now - timedelta(hours=24)
            last_7d = now - timedelta(days=7)

            lifecycle_logs = (
                db.query(AuditLog)
                .filter(
                    AuditLog.timestamp >= last_7d,
                    AuditLog.action_type.in_([
                        "SYSTEM_SERVER_STARTUP",
                        "SYSTEM_UNEXPECTED_REBOOT_DETECTED",
                        "SYSTEM_SERVER_SHUTDOWN",
                        "SYSTEM_MEMORY_SPIKE_WARNING"
                    ])
                )
                .order_by(AuditLog.timestamp.desc())
                .limit(15)
                .all()
            )

            # Count boots/crashes in last 24h
            for log in lifecycle_logs:
                if log.timestamp >= last_24h and log.action_type in ("SYSTEM_SERVER_STARTUP", "SYSTEM_UNEXPECTED_REBOOT_DETECTED"):
                    restarts_24h += 1

                evt_type = "INFO"
                if log.action_type == "SYSTEM_UNEXPECTED_REBOOT_DETECTED":
                    evt_type = "CRASH_REBOOT"
                elif log.action_type == "SYSTEM_MEMORY_SPIKE_WARNING":
                    evt_type = "MEMORY_WARNING"
                elif log.action_type == "SYSTEM_SERVER_SHUTDOWN":
                    evt_type = "SHUTDOWN"

                recent_events.append({
                    "id": log.id,
                    "timestamp": log.timestamp.isoformat(),
                    "action_type": log.action_type,
                    "event_type": evt_type,
                    "details": log.details if isinstance(log.details, dict) else {},
                    "formatted_time": log.timestamp.strftime("%b %d, %I:%M %p")
                })
        except Exception as e:
            logger.debug(f"Error querying lifecycle events: {e}")

    # Estimate uptime SLA (penalize 0.05% per unexpected restart in 24h)
    sla_percentage = max(95.0, round(99.98 - (restarts_24h * 0.05), 2))

    return {
        "boot_time_utc": PROCESS_BOOT_TIME.isoformat(),
        "boot_time_human": PROCESS_BOOT_TIME.strftime("%b %d, %Y at %I:%M %p UTC"),
        "uptime_seconds": round(uptime_seconds, 1),
        "uptime_human": uptime_human,
        "restarts_24h": restarts_24h,
        "uptime_sla": f"{sla_percentage}%",
        "recent_events": recent_events
    }


def record_startup_watchdog(db: Session) -> None:
    """
    Invoked once during server boot.
    Detects if the previous shutdown was clean or if an ungraceful termination (OOM/Crash) occurred.
    """
    try:
        from app.models.models import AuditLog

        # Find the most recent lifecycle log before this boot
        last_log = (
            db.query(AuditLog)
            .filter(AuditLog.action_type.in_(["SYSTEM_SERVER_STARTUP", "SYSTEM_SERVER_SHUTDOWN", "SYSTEM_UNEXPECTED_REBOOT_DETECTED"]))
            .order_by(AuditLog.timestamp.desc())
            .first()
        )

        mem_stats = get_process_memory_stats()
        is_render = bool(os.getenv("RENDER") or os.getenv("RENDER_SERVICE_ID"))
        env_name = "Render Cloud" if is_render else f"{platform.system()} Host"

        # If the last recorded event was a STARTUP without a corresponding SHUTDOWN,
        # it indicates the previous instance was abruptly terminated (e.g. OOM or SIGKILL).
        is_unexpected_restart = False
        if last_log and last_log.action_type == "SYSTEM_SERVER_STARTUP":
            is_unexpected_restart = True
            gap_seconds = (datetime.utcnow() - last_log.timestamp).total_seconds()
            
            # Only flag as ungraceful reboot if the previous boot was recent (within 72 hours)
            if gap_seconds < 259200:
                reboot_details = {
                    "reason": "Previous process terminated without clean shutdown (Potential OOM / Container Eviction)",
                    "previous_boot_utc": last_log.timestamp.isoformat(),
                    "environment": env_name,
                    "initial_rss_mb": mem_stats["current_mb"],
                    "limit_mb": mem_stats["limit_mb"],
                    "detected_at_utc": datetime.utcnow().isoformat()
                }
                reboot_log = AuditLog(
                    user_id=None,
                    customer_id=None,
                    action_type="SYSTEM_UNEXPECTED_REBOOT_DETECTED",
                    entity_type="HostProcess",
                    entity_id=PROCESS_PID,
                    details=reboot_details,
                    timestamp=datetime.utcnow()
                )
                db.add(reboot_log)
                db.commit()
                logger.warning("Watchdog detected previous ungraceful reboot/crash (OOM kill). Logged to audit trail.")

        # Log clean boot event
        startup_details = {
            "pid": PROCESS_PID,
            "boot_time_utc": PROCESS_BOOT_TIME.isoformat(),
            "environment": env_name,
            "python_version": platform.python_version(),
            "initial_rss_mb": mem_stats["current_mb"],
            "limit_mb": mem_stats["limit_mb"],
            "is_post_crash_recovery": is_unexpected_restart
        }
        startup_log = AuditLog(
            user_id=None,
            customer_id=None,
            action_type="SYSTEM_SERVER_STARTUP",
            entity_type="HostProcess",
            entity_id=PROCESS_PID,
            details=startup_details,
            timestamp=datetime.utcnow()
        )
        db.add(startup_log)
        db.commit()
        logger.info(f"Server startup watchdog logged [PID {PROCESS_PID}, Initial RAM {mem_stats['current_mb']}MB].")

    except Exception as e:
        logger.warning(f"Could not record startup watchdog log: {e}")
        try:
            db.rollback()
        except Exception:
            pass


def record_shutdown_watchdog(db: Session) -> None:
    """Invoked during graceful application shutdown."""
    try:
        from app.models.models import AuditLog
        mem_stats = get_process_memory_stats()
        uptime_seconds = (datetime.utcnow() - PROCESS_BOOT_TIME).total_seconds()

        shutdown_details = {
            "pid": PROCESS_PID,
            "uptime_seconds": round(uptime_seconds, 1),
            "uptime_human": format_uptime(uptime_seconds),
            "final_rss_mb": mem_stats["current_mb"],
            "peak_rss_mb": mem_stats["peak_mb"],
            "shutdown_time_utc": datetime.utcnow().isoformat()
        }
        shutdown_log = AuditLog(
            user_id=None,
            customer_id=None,
            action_type="SYSTEM_SERVER_SHUTDOWN",
            entity_type="HostProcess",
            entity_id=PROCESS_PID,
            details=shutdown_details,
            timestamp=datetime.utcnow()
        )
        db.add(shutdown_log)
        db.commit()
        logger.info("Graceful server shutdown watchdog logged.")
    except Exception as e:
        logger.warning(f"Could not record shutdown watchdog log: {e}")
