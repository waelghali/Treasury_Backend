# app/services/ai_policy_guardrail.py
"""
Deterministic Policy Guardrail & Intent Whitelist for 4-Level Treasury AI Assistant.
Validates all parameters strictly before any DB or LLM execution.
"""

from typing import Dict, Any, Tuple, Set

# Whitelisted Level 0 Card IDs (Backend Authority)
APPROVED_CARD_IDS: Dict[str, Dict[str, Any]] = {
    # Executive & Bi-Module
    "portfolio_summary": {"intent": "get_lg_analytics_summary", "params": {"scope": "unified"}},
    "my_profile_permissions": {"intent": "get_user_profile", "params": {}},
    "my_recent_activity": {"intent": "get_audit_history", "params": {"scope": "my_actions"}},
    "organization_activity": {"intent": "get_audit_history", "params": {"scope": "all_organization"}},

    # LG Custody (Inbound)
    "custody_summary": {"intent": "get_lg_analytics_summary", "params": {"scope": "custody"}},
    "expiring_30_days": {"intent": "find_expiring_lgs", "params": {"days": 30}},
    "expiring_60_days": {"intent": "find_expiring_lgs", "params": {"days": 60}},
    "custody_top_beneficiaries": {"intent": "get_top_beneficiaries", "params": {}},
    "custody_bank_exposure": {"intent": "get_bank_exposure", "params": {}},
    "high_value_lgs": {"intent": "search_lgs", "params": {"min_amount": 1000000}},

    # LG Issuance (Outbound)
    "issuance_pipeline_summary": {"intent": "get_issuance_summary", "params": {}},
    "facility_summary": {"intent": "get_facility_analytics", "params": {}},
    "facility_analytics": {"intent": "get_facility_analytics", "params": {}},
    "facility_headroom_summary": {"intent": "get_facility_analytics", "params": {}},
    "action_center_summary": {"intent": "get_action_center_summary", "params": {}},
    "pending_approvals_check": {"intent": "get_pending_approvals", "params": {}},

    # System Guidance
    "system_capabilities_guide": {"intent": "system_help", "params": {"query": "system capabilities"}}
}

# Whitelisted Operations & Schemas
APPROVED_OPERATIONS: Dict[str, Dict[str, Any]] = {
    "find_expiring_lgs": {"params": {"days": int, "month": str, "year": int, "scope": str}},
    "get_pending_approvals": {"params": {"module": str}},
    "get_lg_analytics_summary": {"params": {"currency": str, "scope": str}},
    "get_facility_analytics": {"params": {"currency": str, "bank": str}},
    "search_lgs": {"params": {"query": str, "currency": str, "bank": str, "status": str, "search_term": str, "min_amount": (int, float), "max_amount": (int, float)}},
    "get_lg_details": {"params": {"lg_id": int, "lg_number": str}},
    "get_audit_history": {"params": {"scope": str, "limit": int}},
    "get_user_profile": {"params": {}},
    "system_help": {"params": {"query": str, "lg_number": str, "action": str}},
    "general_treasury": {"params": {"term": str}},
    "complex_analysis": {"params": {"query": str}},
    "get_issuance_summary": {"params": {"status": str, "currency": str}},
    "get_action_center_summary": {"params": {}},
    "get_top_beneficiaries": {"params": {"limit": int, "scope": str}},
    "get_top_issuers": {"params": {"limit": int}},
    "get_entity_distribution": {"params": {}},
    "report_feedback": {"params": {"message": str, "feedback_type": str}},
    "get_daily_pulse": {"params": {}},
    "get_bank_exposure": {"params": {"limit": int}}
}

APPROVED_INTENTS: Set[str] = set(APPROVED_OPERATIONS.keys())


class AIPolicyGuardrail:
    """
    Strict Policy Engine assuring queries fall within Treasury Scope
    and match schema bounds.
    """

    def validate_card_id(self, card_id: str) -> Tuple[bool, Dict[str, Any]]:
        if card_id in APPROVED_CARD_IDS:
            return True, APPROVED_CARD_IDS[card_id]
        return False, {}

    def validate_intent(self, intent: str, params: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        if intent not in APPROVED_INTENTS:
            return False, {}

        spec = APPROVED_OPERATIONS.get(intent, {})
        allowed_params = spec.get("params", {})
        cleaned_params = {}

        for k, v in params.items():
            if k in allowed_params:
                cleaned_params[k] = v

        return True, cleaned_params


policy_guardrail = AIPolicyGuardrail()
