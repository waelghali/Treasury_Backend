# app/core/honeypot.py
"""
Zero-Friction Perimeter Anti-Bot Honeypot Defense (Roadmap §2.8).

Provides reusable, zero-overhead client/server bot trapping to detect and
silently sinkhole automated scrapers, credential-stuffing crawlers, and form spammers
with 0% impact on legitimate human users and zero accessibility disruption.
"""

import asyncio
import time
import logging
from typing import Optional, List
from fastapi import HTTPException, Request, status

logger = logging.getLogger("security.honeypot")

DEFAULT_TARPIT_SECONDS: float = 2.0


def _is_autofill_collision(trap_value: str, legitimate_values: Optional[List[Optional[str]]]) -> bool:
    """Check if the trapped value is an identical copy of a legitimate form field populated by autofill."""
    if not legitimate_values:
        return False
    norm_trap = str(trap_value).strip().lower()
    for legit in legitimate_values:
        if legit and str(legit).strip().lower() == norm_trap:
            return True
    return False


async def verify_honeypot(
    trap_value: Optional[str],
    request: Optional[Request] = None,
    context: str = "general",
    tarpit_seconds: float = DEFAULT_TARPIT_SECONDS,
    legitimate_values: Optional[List[Optional[str]]] = None
) -> None:
    """
    Asynchronous honeypot verification for FastAPI async endpoints.
    
    If an automated bot, crawler, or form spammer fills the trapped field:
    1. Checks whether trap matches a submitted legitimate field (browser autofill collision).
    2. If true bot: injects a non-blocking asynchronous tarpit sleep to exhaust bot concurrency.
    3. Logs security event with client IP and User-Agent.
    4. Raises generic 400 Bad Request to silently sinkhole the attacker.
    """
    if trap_value and str(trap_value).strip():
        if _is_autofill_collision(trap_value, legitimate_values):
            logger.info(
                f"Perimeter Honeypot Autofill Shield [{context}]: Trap value matched submitted form field. "
                f"Safely bypassed as browser autofill false positive."
            )
            return

        client_ip = request.client.host if (request and request.client) else "unknown"
        user_agent = request.headers.get("user-agent", "unknown") if request else "unknown"
        
        logger.warning(
            f"Perimeter Honeypot Triggered [{context}]: Automated bot detected from IP {client_ip}. "
            f"Trap content preview: '{str(trap_value)[:50]}'. User-Agent: '{user_agent}'. "
            f"Engaging {tarpit_seconds}s tarpit delay."
        )
        
        if tarpit_seconds > 0:
            await asyncio.sleep(tarpit_seconds)
            
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid request submission. Please verify your information."
        )


def verify_honeypot_sync(
    trap_value: Optional[str],
    request: Optional[Request] = None,
    context: str = "general",
    tarpit_seconds: float = DEFAULT_TARPIT_SECONDS,
    legitimate_values: Optional[List[Optional[str]]] = None
) -> None:
    """
    Synchronous honeypot verification for synchronous worker endpoints.
    """
    if trap_value and str(trap_value).strip():
        if _is_autofill_collision(trap_value, legitimate_values):
            logger.info(
                f"Perimeter Honeypot Autofill Shield [{context}]: Trap value matched submitted form field. "
                f"Safely bypassed as browser autofill false positive."
            )
            return

        client_ip = request.client.host if (request and request.client) else "unknown"
        user_agent = request.headers.get("user-agent", "unknown") if request else "unknown"
        
        logger.warning(
            f"Perimeter Honeypot Triggered [{context}]: Automated bot detected from IP {client_ip}. "
            f"Trap content preview: '{str(trap_value)[:50]}'. User-Agent: '{user_agent}'. "
            f"Engaging {tarpit_seconds}s tarpit delay."
        )
        
        if tarpit_seconds > 0:
            time.sleep(tarpit_seconds)
            
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid request submission. Please verify your information."
        )
