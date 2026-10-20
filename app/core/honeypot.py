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
from typing import Optional
from fastapi import HTTPException, Request, status

logger = logging.getLogger("security.honeypot")

DEFAULT_TARPIT_SECONDS: float = 2.0


async def verify_honeypot(
    trap_value: Optional[str],
    request: Optional[Request] = None,
    context: str = "general",
    tarpit_seconds: float = DEFAULT_TARPIT_SECONDS
) -> None:
    """
    Asynchronous honeypot verification for FastAPI async endpoints.
    
    If an automated bot, crawler, or form spammer fills the trapped field:
    1. Injects a non-blocking asynchronous tarpit sleep to exhaust bot concurrency.
    2. Logs security event with client IP and User-Agent.
    3. Raises generic 400 Bad Request to silently sinkhole the attacker.
    """
    if trap_value and str(trap_value).strip():
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
    tarpit_seconds: float = DEFAULT_TARPIT_SECONDS
) -> None:
    """
    Synchronous honeypot verification for synchronous worker endpoints.
    """
    if trap_value and str(trap_value).strip():
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
