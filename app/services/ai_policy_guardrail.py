# app/services/ai_policy_guardrail.py
"""
Internal Policy & Guardrail Layer for 4-Level Treasury AI Architecture

Responsibilities:
1. Resolves frontend card_id requests to approved Level 0 handlers (completely bypassing AI).
2. Validates requested operations & intents against explicit whitelist.
3. Enforces Treasury & System Domain scope policy for Level 3 / Level 4 questions.
4. Enforces response length boundaries and formatting expectations.
5. Ensures zero arbitrary SQL execution and zero unapproved data mutation.
"""

import os
import logging
from typing import Dict, Any, Tuple, Optional, Set

logger = logging.getLogger(__name__)

# Max response character limit
MAX_RESPONSE_CHARS = int(os.getenv("AI_DATA_ASSISTANT_MAX_RESPONSE_CHARS", "3000"))

# Whitelisted Level 0 Card IDs (Backend Authority)
APPROVED_CARD_IDS: Dict[str, Dict[str, Any]] = {
    "expiring_30_days": {"intent": "find_expiring_lgs", "params": {"days": 30}},
    "expiring_60_days": {"intent": "find_expiring_lgs", "params": {"days": 60}},
    "portfolio_summary": {"intent": "get_lg_analytics_summary", "params": {}},
    "facility_summary": {"intent": "get_facility_analytics", "params": {}},
    "facility_analytics": {"intent": "get_facility_analytics", "params": {}},
    "pending_approvals_check": {"intent": "get_pending_approvals", "params": {}},
    "system_capabilities_guide": {"intent": "system_help", "params": {"query": "system capabilities"}},
    "my_recent_activity": {"intent": "get_audit_history", "params": {"scope": "my_actions"}},
    "organization_activity": {"intent": "get_audit_history", "params": {"scope": "all_organization"}},
    "my_profile_permissions": {"intent": "get_user_profile", "params": {}},
}

# Whitelisted Operations Catalogue
APPROVED_INTENTS: Set[str] = {
    "find_expiring_lgs",
    "get_pending_approvals",
    "get_lg_analytics_summary",
    "get_facility_analytics",
    "search_lgs",
    "get_lg_details",
    "get_audit_history",
    "get_user_profile",
    "system_help"
}

# General Treasury Domain Topics (Level 3 Scope Policy)
# Includes abbreviations: fwd, ndf, irs, fx, etc.
TREASURY_KEYWORDS: Set[str] = {
    "treasury", "cash", "cash pooling", "liquidity", "fx", "foreign exchange",
    "guarantee", "letter of guarantee", "lg", "facility", "credit line",
    "working capital", "hedging", "interest rate", "swap", "trade finance",
    "bank", "banking", "counterparty", "swift", "reconciliation", "cashflow",
    "debt", "financing", "deposit", "money market", "treasury bill", "t-bill",
    "fwd", "forward", "forward contract", "forward deal", "ndf", "irs",
    "derivative", "derivatives", "options", "spot", "tenor", "maturity",
    "lc", "letter of credit", "standby", "sblc", "bid bond", "performance bond",
    "advance payment", "retention", "customs", "collateral", "margin"
}

# System & Platform Help Keywords (Level 4 Scope)
SYSTEM_KEYWORDS: Set[str] = {
    "system", "grow", "module", "how do i", "how to", "where is", "how can i",
    "navigation", "menu", "sidebar", "record new lg", "extend", "release",
    "liquidate", "decrease", "amend", "non-operative", "maker-checker", "checker",
    "maker", "approval", "approval matrix", "facility", "facilities", "migration",
    "template", "reconciliation", "quotation", "rfq", "user management",
    "customer", "subscription", "plan", "role", "roles", "permission", "permissions",
    "action center", "inbox", "discrepancy", "upload", "ocr", "scan", "wizard",
    "status", "valid", "expired", "released", "liquidated", "guide", "help",
    "why can't i", "why is", "how does", "what is the action center", "what is grow",
    "audit", "audit log", "audit trail", "activity", "recent actions", "history",
    "what did i do", "who created", "who modified", "who approved", "profile", "my permissions",
    "what can i do", "my role", "who am i", "my capabilities"
}


class AIPolicyGuardrail:
    """
    Internal Policy Engine - The Application Decides.
    Sits between AI classification and backend/database execution.
    """
    APPROVED_INTENTS = APPROVED_INTENTS
    APPROVED_CARD_IDS = APPROVED_CARD_IDS

    def resolve_card_id(self, card_id: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Determines whether a frontend card_id maps to an approved Level 0 handler.
        The backend alone determines level=0 status.
        """
        if not card_id or card_id not in APPROVED_CARD_IDS:
            return False, None
        return True, APPROVED_CARD_IDS[card_id]

    def validate_intent(self, intent: str, params: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Validates AI requested operation against explicit whitelisted catalogue.
        """
        if intent not in APPROVED_INTENTS:
            return False, f"Operation '{intent}' is not permitted or whitelisted.", {}

        if intent == "get_lg_analytics_summary":
            c_val = str(params.get("currency", "")).strip().upper()
            return True, "Approved", {"currency": c_val} if c_val else {}

        if intent in {"get_pending_approvals", "get_facility_analytics", "get_user_profile"}:
            return True, "Approved", {}

        if intent == "get_audit_history":
            scope = str(params.get("scope", "all_organization")).strip().lower()
            limit = int(params.get("limit", 15)) if str(params.get("limit", "")).isdigit() else 15
            lg_num = str(params.get("lg_number", "") or "").strip()
            return True, "Approved", {"scope": scope, "limit": limit, "lg_number": lg_num}

        if intent == "find_expiring_lgs":
            days = params.get("days", 60)
            month = params.get("month")
            year = params.get("year")
            status = params.get("status", "")
            try:
                days = int(days)
                if days <= 0 or days > 365:
                    days = 60
            except (ValueError, TypeError):
                days = 60
            return True, "Approved", {"days": days, "month": month, "year": year, "status": status}

        if intent == "search_lgs":
            q_val = str(params.get("query", "")).strip()
            c_val = str(params.get("currency", "")).strip()
            s_val = str(params.get("status", "")).strip()
            return True, "Approved", {"query": q_val, "currency": c_val, "status": s_val}

        if intent == "get_lg_details":
            raw_lg = params.get("lg_number")
            lg_num = str(raw_lg).strip() if raw_lg and str(raw_lg).lower() not in {"none", "null"} else ""
            return True, "Approved", {"lg_number": lg_num}

        if intent == "system_help":
            q_val = str(params.get("query", "")).strip()
            return True, "Approved", {"query": q_val}

        return False, "Invalid operation parameters.", {}

    def validate_treasury_scope(self, user_question: str, ai_classified_topic: str) -> Tuple[bool, str]:
        """
        Validates Level 3 Treasury domain scope.
        AI classifies topic; Application Policy enforces allowed execution.
        """
        q_lower = user_question.lower()

        # Check AI classification
        is_ai_treasury = ai_classified_topic.lower() in {"treasury", "system"}

        # Check policy keyword list as secondary guardrail
        has_treasury_keyword = any(kw in q_lower for kw in TREASURY_KEYWORDS)
        has_system_keyword = any(kw in q_lower for kw in SYSTEM_KEYWORDS)

        if is_ai_treasury or has_treasury_keyword or has_system_keyword:
            return True, "Approved Treasury / System Question"

        return False, "This question is outside the approved Treasury and System domain scope."

    def enforce_response_limit(self, response_text: str, max_chars: Optional[int] = None) -> str:
        """
        Enforces response size boundaries on final answers.
        """
        limit = max_chars or MAX_RESPONSE_CHARS
        if len(response_text) > limit:
            return response_text[: limit - 3] + "..."
        return response_text


policy_guardrail = AIPolicyGuardrail()
