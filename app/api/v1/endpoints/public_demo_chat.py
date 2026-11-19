# c:\Grow\app\api\v1\endpoints\public_demo_chat.py
"""
Public AI Product Expert Chat Endpoint (`/api/v1/public-demo/chat`).

- Unauthenticated, 100% air-gapped from customer database sessions.
- Unlimited questions for visitors/prospects, protected by honeypot anti-bot defense
  and server-side payload sanitization.
- Includes a System-Owner-only read endpoint (`/api/v1/public-demo/admin/logs`)
  for reviewing real anonymous prospect questions.
"""

import base64
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, Field

from app.core.security import TokenData, get_current_system_owner
from app.services.public_product_expert_service import (
    ask_public_product_expert,
    ask_public_product_expert_voice,
    detect_response_language,
    hash_client_ip,
    is_loopback_ip,
    is_production_public_telemetry_env,
    public_demo_chat_logger,
    public_voice_control,
    synthesize_public_expert_speech,
)

router = APIRouter()

_ALLOWED_TELEMETRY_EVENTS = {
    "page_view",
    "story_start",
    "story_complete",
    "module_expand",
    "tour_open",
    "foundation_open",
    "chatbot_open",
    "cta_click",
    "lang_switch",
}

_TELEMETRY_BURST_PER_MIN = 120
_telemetry_ip_buckets: Dict[str, Deque[float]] = {}
_telemetry_lock = threading.Lock()

_VOICE_CALLS_PER_HOUR_PER_IP = 40
_voice_ip_buckets: Dict[str, Deque[float]] = {}
_voice_rate_lock = threading.Lock()
_MAX_AUDIO_BYTES = 2 * 1024 * 1024  # 2 MB (~30s opus/mp4 clip)


def _allow_voice_call(ip_hash: str) -> bool:
    """Per-IP hourly shield on voice/TTS (40 calls/hr/IP) to protect the monthly voice quota from bots."""
    now = time.monotonic()
    cutoff = now - 3600.0
    with _voice_rate_lock:
        if len(_voice_ip_buckets) > 4000:
            stale = [k for k, dq in _voice_ip_buckets.items() if not dq or dq[-1] < cutoff]
            for k in stale:
                _voice_ip_buckets.pop(k, None)
        dq = _voice_ip_buckets.get(ip_hash)
        if dq is None:
            dq = deque()
            _voice_ip_buckets[ip_hash] = dq
        while dq and dq[0] < cutoff:
            dq.popleft()
        if len(dq) >= _VOICE_CALLS_PER_HOUR_PER_IP:
            return False
        dq.append(now)
        return True


def _allow_telemetry_write(ip_hash: str) -> bool:
    """Silent anti-flood shield for `/telemetry` (120 events/min/IP) so bots cannot bloat disk."""
    now = time.monotonic()
    cutoff = now - 60.0
    with _telemetry_lock:
        if len(_telemetry_ip_buckets) > 4000:
            stale = [k for k, dq in _telemetry_ip_buckets.items() if not dq or dq[-1] < cutoff]
            for k in stale:
                _telemetry_ip_buckets.pop(k, None)
        dq = _telemetry_ip_buckets.get(ip_hash)
        if dq is None:
            dq = deque()
            _telemetry_ip_buckets[ip_hash] = dq
        while dq and dq[0] < cutoff:
            dq.popleft()
        if len(dq) >= _TELEMETRY_BURST_PER_MIN:
            return False
        dq.append(now)
        return True


def _extract_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        first_ip = forwarded.split(",")[0].strip()
        if first_ip:
            return first_ip[:64]
    if request.client and request.client.host:
        return request.client.host[:64]
    return "unknown"


def _extract_country_header(request: Request) -> str:
    for hdr in ("cf-ipcountry", "x-appengine-country", "x-country-code", "x-geo-country"):
        val = (request.headers.get(hdr) or "").strip()
        if val and len(val) <= 8:
            return val.upper()
    return ""


def _is_localhost_origin(request: Request) -> bool:
    for hdr in ("origin", "referer"):
        raw = (request.headers.get(hdr) or "").strip()
        if not raw:
            continue
        try:
            host = (urlparse(raw).hostname or "").strip("[]").lower()
            if host in ("localhost", "127.0.0.1", "::1") or host.endswith(".local"):
                return True
        except Exception:
            continue
    return False


# ==============================================================================
# REQUEST / RESPONSE SCHEMAS (GENEROUS / NON-BLOCKING VALIDATION)
# ==============================================================================

class PublicChatTurn(BaseModel):
    role: Optional[str] = Field(default="user")
    content: Optional[str] = Field(default="")


class PublicChatRequest(BaseModel):
    question: str = Field(..., min_length=1)
    history: Optional[List[PublicChatTurn]] = Field(default_factory=list)
    preferred_lang: Optional[str] = Field(default="en")
    page_context: Optional[str] = Field(default="demo")
    session_id: Optional[str] = Field(default="anon")
    website_url: Optional[str] = Field(default="")  # Anti-bot honeypot


class PublicChatActionOut(BaseModel):
    type: str
    id: str
    code: Optional[str] = None
    module_id: Optional[str] = None
    step: int = 1
    label: str


class PublicChatResponse(BaseModel):
    answer: str
    spoken_summary: Optional[str] = ""
    heard_question: Optional[str] = ""
    lang: str
    topic: str
    actions: List[PublicChatActionOut] = Field(default_factory=list)
    follow_ups: List[str] = Field(default_factory=list)
    show_commercial_cta: bool = False


class PublicVoiceChatRequest(BaseModel):
    audio_base64: str = Field(..., min_length=16)
    mime_type: Optional[str] = Field(default="audio/webm")
    history: Optional[List[PublicChatTurn]] = Field(default_factory=list)
    preferred_lang: Optional[str] = Field(default="en")
    page_context: Optional[str] = Field(default="demo")
    session_id: Optional[str] = Field(default="anon")
    website_url: Optional[str] = Field(default="")


class PublicTtsRequest(BaseModel):
    text: str = Field(..., min_length=1)
    lang: Optional[str] = Field(default="en")
    is_auto_reply: Optional[bool] = Field(default=False)
    website_url: Optional[str] = Field(default="")


class AdminVoiceSettingsUpdate(BaseModel):
    voice_enabled: Optional[bool] = None
    monthly_cap: Optional[int] = None
    auto_disable_at_cap: Optional[bool] = None


# ==============================================================================
# ROUTES
# ==============================================================================

@router.post("/chat", response_model=PublicChatResponse)
async def public_product_expert_chat(
    payload: PublicChatRequest,
    request: Request,
) -> Dict[str, Any]:
    """
    Public AI Product Expert endpoint for `/` and `/demo`.
    Answers treasury & platform edge-case questions with 1-click showcase deep links.
    """
    question = (payload.question or "").strip()[:3000]
    if not question:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please enter a question.",
        )

    pref_lang = (payload.preferred_lang or "en")[:8]

    # Anti-bot honeypot check: silently return benign response without invoking AI
    if payload.website_url and payload.website_url.strip():
        lang = detect_response_language(question, pref_lang)
        return {
            "answer": (
                "يرجى استخدام نافذة المحادثة مباشرة لطرح سؤالك حول منصة Grow."
                if lang == "ar"
                else "Please ask your question directly in the Grow Product Expert chat."
            ),
            "lang": lang,
            "topic": "honeypot_blocked",
            "actions": [],
            "follow_ups": [],
            "show_commercial_cta": False,
        }

    client_ip = _extract_client_ip(request)

    sanitized_history = [
        {
            "role": "user" if (t.role or "").lower() == "user" else "assistant",
            "content": (t.content or "").strip()[:800],
        }
        for t in (payload.history or [])[-8:]
        if (t.content or "").strip()
    ]

    user_agent = request.headers.get("user-agent", "")[:160]

    return await ask_public_product_expert(
        question=question,
        history=sanitized_history,
        preferred_lang=pref_lang,
        page_context=(payload.page_context or "demo")[:120],
        session_id=(payload.session_id or "anon")[:64],
        client_ip=client_ip,
        user_agent=user_agent,
    )


class PublicTelemetryEventRequest(BaseModel):
    event_type: str = Field(default="page_view")
    session_id: Optional[str] = Field(default="anon")
    visitor_id: Optional[str] = Field(default="")
    page: Optional[str] = Field(default="")
    path: Optional[str] = Field(default="")
    referrer_source: Optional[str] = Field(default="Direct")
    utm_campaign: Optional[str] = Field(default="")
    device_type: Optional[str] = Field(default="desktop")
    lang: Optional[str] = Field(default="en")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)


@router.post("/telemetry")
def record_public_telemetry_event(
    payload: PublicTelemetryEventRequest,
    request: Request,
) -> Dict[str, bool]:
    """
    Lightweight, zero-cookie first-party event beacon for `/`, `/demo`, `/know-more`,
    and `/free-trial-register`. Runs in FastAPI threadpool (never blocking asyncio loop),
    never touches customer DB tables, and never stores raw IPs.
    """
    evt = (payload.event_type or "").strip().lower()
    if evt not in _ALLOWED_TELEMETRY_EVENTS:
        return {"ok": False}

    client_ip = _extract_client_ip(request)
    if is_production_public_telemetry_env() and (is_loopback_ip(client_ip) or _is_localhost_origin(request)):
        return {"ok": True}

    ip_hash = hash_client_ip(client_ip)
    if not _allow_telemetry_write(ip_hash):
        return {"ok": True}

    user_agent = request.headers.get("user-agent", "")[:200]
    country = _extract_country_header(request)

    public_demo_chat_logger.log_telemetry_event(
        event_type=evt,
        session_id=(payload.session_id or "anon")[:64],
        visitor_id=(payload.visitor_id or "")[:64],
        page=(payload.page or "")[:40],
        path=(payload.path or "")[:160],
        referrer_source=(payload.referrer_source or "Direct")[:80],
        utm_campaign=(payload.utm_campaign or "")[:80],
        device_type=(payload.device_type or "desktop")[:20],
        lang=(payload.lang or "en")[:8],
        country=country,
        metadata=payload.metadata if isinstance(payload.metadata, dict) else {},
        ip_hash=ip_hash,
        user_agent=user_agent,
    )
    return {"ok": True}


@router.get("/voice-status")
def get_public_voice_status() -> Dict[str, bool]:
    """
    Lightweight public check returning whether Voice AI (Mic & Listen) is enabled.
    If System Owner disables it, the frontend hides all voice UI completely.
    """
    return public_voice_control.get_public_status()


@router.post("/voice-chat", response_model=PublicChatResponse)
async def public_product_expert_voice_chat(
    payload: PublicVoiceChatRequest,
    request: Request,
) -> Dict[str, Any]:
    """
    Multimodal voice question endpoint: accepts recorded visitor speech (EN or AR),
    transcribes `heard_question`, answers from the 500-question audited reference,
    and prepares `spoken_summary`. Disabled instantly when `voice_enabled == False`.
    """
    if not public_voice_control.is_voice_enabled():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Voice assistant is currently disabled.",
        )

    if payload.website_url and payload.website_url.strip():
        return {
            "answer": "Please ask your question directly in the Grow Product Expert chat.",
            "spoken_summary": "",
            "heard_question": "",
            "lang": "en",
            "topic": "honeypot_blocked",
            "actions": [],
            "follow_ups": [],
            "show_commercial_cta": False,
        }

    client_ip = _extract_client_ip(request)
    ip_hash = hash_client_ip(client_ip)
    if not _allow_voice_call(ip_hash):
        pref = (payload.preferred_lang or "en")[:8]
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                "تم الوصول للحد المسموح للأسئلة الصوتية خلال الساعة. يمكنك المتابعة كتابةً بلا حدود!"
                if pref.startswith("ar")
                else "Hourly voice limit reached—you can continue typing unlimited questions anytime!"
            ),
        )

    raw_b64 = (payload.audio_base64 or "").strip()
    if "," in raw_b64 and raw_b64.startswith("data:"):
        raw_b64 = raw_b64.split(",", 1)[1]

    try:
        audio_bytes = base64.b64decode(raw_b64, validate=False)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid audio encoding.",
        )

    if len(audio_bytes) < 200 or len(audio_bytes) > _MAX_AUDIO_BYTES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Audio clip must be under 30 seconds.",
        )

    sanitized_history = [
        {
            "role": "user" if (t.role or "").lower() == "user" else "assistant",
            "content": (t.content or "").strip()[:800],
        }
        for t in (payload.history or [])[-8:]
        if (t.content or "").strip()
    ]
    user_agent = request.headers.get("user-agent", "")[:160]

    try:
        return await ask_public_product_expert_voice(
            audio_bytes=audio_bytes,
            audio_mime_type=(payload.mime_type or "audio/webm")[:64],
            history=sanitized_history,
            preferred_lang=(payload.preferred_lang or "en")[:8],
            page_context=(payload.page_context or "demo")[:120],
            session_id=(payload.session_id or "anon")[:64],
            client_ip=client_ip,
            user_agent=user_agent,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Voice recognition is temporarily busy. Please try again or type your question. ({type(exc).__name__})",
        )


@router.post("/tts")
async def public_product_expert_tts(
    payload: PublicTtsRequest,
    request: Request,
) -> Response:
    """
    Studio neural TTS endpoint (`gemini-2.5-flash-tts`) returning `audio/wav`.
    Disabled instantly when `voice_enabled == False`.
    """
    if not public_voice_control.is_voice_enabled():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Voice assistant is currently disabled.",
        )

    if payload.website_url and payload.website_url.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid request.")

    client_ip = _extract_client_ip(request)
    ip_hash = hash_client_ip(client_ip)
    if not _allow_voice_call(ip_hash):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Hourly voice playback limit reached.",
        )

    try:
        audio_bytes, media_type = await synthesize_public_expert_speech(
            text=(payload.text or "").strip()[:450],
            lang=(payload.lang or "en")[:8],
            is_auto_reply=bool(payload.is_auto_reply),
        )
        return Response(
            content=audio_bytes,
            media_type=media_type,
            headers={"Cache-Control": "private, max-age=3600"},
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Audio synthesis is temporarily unavailable.",
        )


@router.get("/admin/voice-settings")
def get_admin_voice_settings(
    current_owner: TokenData = Depends(get_current_system_owner),
) -> Dict[str, Any]:
    """System-Owner-only endpoint returning Voice AI switch state and monthly quota usage."""
    return public_voice_control.get_admin_status()


@router.patch("/admin/voice-settings")
def update_admin_voice_settings(
    payload: AdminVoiceSettingsUpdate,
    current_owner: TokenData = Depends(get_current_system_owner),
) -> Dict[str, Any]:
    """System-Owner-only 1-click toggle for `voice_enabled`, `monthly_cap`, and `auto_disable_at_cap`."""
    return public_voice_control.update_settings(
        voice_enabled=payload.voice_enabled,
        monthly_cap=payload.monthly_cap,
        auto_disable_at_cap=payload.auto_disable_at_cap,
    )


@router.get("/admin/logs")
def get_public_demo_chat_logs(
    days: int = Query(default=14, ge=1, le=90),
    limit: int = Query(default=100, ge=1, le=500),
    current_owner: TokenData = Depends(get_current_system_owner),
) -> Dict[str, Any]:
    """
    System-Owner-only endpoint to inspect anonymous visitor questions & AI responses
    collected from `/` and `/demo`.
    """
    return public_demo_chat_logger.read_recent_logs(days=days, limit=limit)


@router.get("/admin/analytics")
def get_public_website_and_demo_analytics(
    days: int = Query(default=14, ge=1, le=90),
    chat_limit: int = Query(default=300, ge=1, le=500),
    session_limit: int = Query(default=150, ge=1, le=300),
    current_owner: TokenData = Depends(get_current_system_owner),
) -> Dict[str, Any]:
    """
    System-Owner-only unified analytics endpoint returning Website Visits,
    Demo Story & Workflow Engagement, AI Chatbot Conversion & Q&A Inbox,
    and Visitor Session Journeys.
    """
    return public_demo_chat_logger.compute_public_analytics(
        days=days,
        chat_limit=chat_limit,
        session_limit=session_limit,
    )
