# app/api/v1/endpoints/ai_query_assistant.py
import time
import logging
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.database import get_db
from app.core.security import get_current_active_user, TokenData
from app.services.ai_query_service import ai_query_assistant_service, is_ai_query_assistant_enabled
from app.services.ai_chat_logger import ai_chat_logger

logger = logging.getLogger(__name__)

router = APIRouter()


class AIQueryRequest(BaseModel):
    question: Optional[str] = Field("", max_length=500, description="Natural language question about LG data.")
    card_id: Optional[str] = Field(None, description="Backend-resolved card ID for Level 0 query execution.")


class LGReferenceItem(BaseModel):
    lg_id: int
    lg_number: str
    expiry_date: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None


class SuggestedChip(BaseModel):
    label: str
    query: str


class AIQueryResponse(BaseModel):
    success: bool
    answer: Optional[str] = None
    references: List[LGReferenceItem] = []
    suggested_chips: List[SuggestedChip] = []
    visual_metadata: Optional[Dict[str, Any]] = None
    level: Optional[int] = Field(None, description="Architecture Level used (0, 1, 2, or 3)")
    source_awareness: Optional[str] = Field(None, description="Source tag: SYSTEM_DATA, GENERAL_AI_KNOWLEDGE, COMBINATION")
    intent: Optional[str] = None
    privacy_notice: Optional[str] = Field(
        "Conversations are saved for analytical evaluation and continuous system enhancements.",
        description="Data collection transparency notice"
    )
    error: Optional[str] = None
    code: Optional[str] = None


@router.post("/chat", response_model=AIQueryResponse)
async def query_ai_assistant(
    payload: AIQueryRequest,
    current_user: TokenData = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    4-Level Treasury AI Assistant API Endpoint.
    Routes queries safely using Level 0 (card_id), Level 1, Level 2, or Level 3.
    Automatically logs interactions locally and to Google Cloud Storage.
    """
    if not is_ai_query_assistant_enabled():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="AI Data Assistant feature is disabled under system configuration."
        )

    if current_user.customer_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User is not associated with an active customer entity."
        )

    start_time = time.perf_counter()
    result = ai_query_assistant_service.process_query(
        db=db,
        user_question=payload.question or "",
        customer_id=current_user.customer_id,
        user_id=current_user.user_id,
        card_id=payload.card_id,
        has_all_entity_access=current_user.has_all_entity_access,
        entity_ids=current_user.entity_ids
    )
    elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

    # Automatically log interaction (Local TXT/JSONL + Google Cloud Storage)
    ai_chat_logger.log_interaction(
        customer_id=current_user.customer_id,
        user_id=current_user.user_id,
        question=payload.question or "",
        answer=result.get("answer") or "",
        level=result.get("level", 1),
        intent=result.get("intent"),
        source_awareness=result.get("source_awareness"),
        card_id=payload.card_id,
        references=result.get("references", []),
        error=result.get("error") if not result.get("success") else None,
        execution_time_ms=elapsed_ms
    )

    if not result.get("success"):
        return AIQueryResponse(
            success=False,
            answer=None,
            error=result.get("error", "Unable to process query."),
            code=result.get("code")
        )

    return AIQueryResponse(
        success=True,
        answer=result.get("answer"),
        references=result.get("references", []),
        suggested_chips=result.get("suggested_chips", []),
        visual_metadata=result.get("visual_metadata"),
        level=result.get("level"),
        source_awareness=result.get("source_awareness"),
        intent=result.get("intent")
    )

