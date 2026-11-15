# c:\Grow\app\api\v1\endpoints\public_demo_chat.py
"""
Public AI Product Expert Chat Endpoint (`/api/v1/public-demo/chat`).

- Unauthenticated, 100% air-gapped from customer database sessions.
- Unlimited questions for visitors/prospects, protected by honeypot anti-bot defense
  and server-side payload sanitization.
- Includes a System-Owner-only read endpoint (`/api/v1/public-demo/admin/logs`)
  for reviewing real anonymous prospect questions.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from app.core.security import TokenData, get_current_system_owner
from app.services.public_product_expert_service import (
    ask_public_product_expert,
    detect_response_language,
    public_demo_chat_logger,
)

router = APIRouter()


def _extract_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        first_ip = forwarded.split(",")[0].strip()
        if first_ip:
            return first_ip[:64]
    if request.client and request.client.host:
        return request.client.host[:64]
    return "unknown"


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
    lang: str
    topic: str
    actions: List[PublicChatActionOut] = Field(default_factory=list)
    follow_ups: List[str] = Field(default_factory=list)
    show_commercial_cta: bool = False


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
