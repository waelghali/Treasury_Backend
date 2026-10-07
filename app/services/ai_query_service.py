from app.models.models_feedback import UserFeedback, FeedbackType, FeedbackSentiment, FeedbackStatus
# app/services/ai_query_service.py
"""
4-Level Enterprise Treasury AI Assistant Architecture
------------------------------------------------------
Deterministic, Offline-Autonomous, Policy-Guarded AI Query Service for Grow Platform.

Levels:
- Level 0 (Instant System Cache): Pre-computed DB metrics & cards with role-tailored links (<10ms).
- Level 1 (Deterministic Single-Query ORM): Natural language intent-to-ORM mapping for Custody,
          Issuance, Facilities, Expiries, Counterparties, and Action Center (<25ms).
- Level 2 (Complex Multi-Step Analytical Planning): Tokenized privacy-preserving multi-step reasoning.
- Level 3 (General Treasury Scope / Fallback): Offline Treasury Glossary & Knowledge.
- Level 4 (System Knowledge & Workflow Navigation): Role-aware guided workflows with 1-click deep links.
"""

import re
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_, and_, desc

from app.models import (
    LGRecord, LgStatus, Customer, User, Bank, CustomerEntity,
    InternalOwnerContact, Currency, LGInstruction
)
from app.models.models_issuance import IssuanceRequest, IssuanceFacility
from app.models.models_quotation import (
    QuotationRequest, QuotationLeg, QuotationBankAssignment,
    QuotationOffer, QuotationTBillOffer, QuotationBank
)
from app.models.models import AuditLog
from app.services.ai_policy_guardrail import policy_guardrail
from app.services.ai_privacy_tokenizer import privacy_tokenizer
from app.services.system_knowledge_base import get_system_knowledge

logger = logging.getLogger(__name__)

OFFLINE_TREASURY_GLOSSARY = {
    "cash pooling": "Cash pooling is a corporate treasury technique used to centralize and optimize cash balances across multiple bank accounts to minimize borrowing costs and maximize interest income.",
    "pooling": "Cash pooling is a corporate treasury technique used to centralize and optimize cash balances across multiple bank accounts to minimize borrowing costs and maximize interest income.",
    "forward": "In Corporate Treasury, a Forward Contract (FX Forward) is a customized hedging agreement to buy or sell an asset/currency at a specified price on a future settlement date, protecting against foreign exchange volatility.",
    "fwd": "In Corporate Treasury, a Forward Contract (FX Forward) is a customized hedging agreement to buy or sell an asset/currency at a specified price on a future settlement date, protecting against foreign exchange volatility.",
    "fx forward": "A Foreign Exchange Forward is an over-the-counter contract that locks in an exchange rate for a currency purchase or sale at a specified future date to hedge currency risk.",
    "ndf": "A Non-Deliverable Forward (NDF) is a cash-settled forward contract on a thinly traded or restricted currency, where the profit/loss difference between the agreed rate and spot rate is settled in a major convertible currency (like USD).",
    "irs": "An Interest Rate Swap (IRS) is a derivative contract where two parties exchange interest rate cash flows based on a specified principal amount, typically swapping fixed-rate for floating-rate (e.g., SOFR/EURIBOR).",
    "interest rate swap": "An Interest Rate Swap (IRS) is a derivative contract where two parties exchange interest rate cash flows based on a specified principal amount, typically swapping fixed-rate for floating-rate.",
    "sblc": "A Standby Letter of Credit (SBLC) is a legal guarantee issued by a bank on behalf of a client, serving as a secondary payment mechanism if the applicant fails to fulfill contractual obligations.",
    "standby letter of credit": "A Standby Letter of Credit (SBLC) is a legal guarantee issued by a bank on behalf of a client, serving as a secondary payment mechanism if the applicant fails to fulfill contractual obligations.",
    "letter of guarantee": "A Letter of Guarantee (LG) is a formal commitment by an issuing bank to pay a designated beneficiary if the applicant defaults on specific financial or performance contractual obligations.",
    "maker checker": "Maker-Checker (Dual Control) is a strict internal control mechanism requiring two independent users: one to initiate (Maker) and a distinct authorized manager to approve (Checker) treasury transactions.",
    "dual control": "Maker-Checker (Dual Control) is a strict internal control mechanism requiring two independent users: one to initiate (Maker) and a distinct authorized manager to approve (Checker) treasury transactions."
}


def is_ai_query_assistant_enabled() -> bool:
    import os
    env_val = os.getenv("AI_DATA_ASSISTANT_ENABLED")
    if env_val is not None:
        return str(env_val).lower() in ("true", "1", "yes", "t")
    try:
        from app.core.config_provider import get_system_config_value
        val = get_system_config_value("AI_DATA_ASSISTANT_ENABLED", "true")
        return str(val).lower() in ("true", "1", "yes", "t")
    except Exception:
        return True


class AIQueryAssistantService:
    """
    Unified Bi-Module Treasury AI Assistant Service (LG Custody + LG Issuance + Facilities).
    Fully Autonomous, 100% Offline-Capable, Sub-25ms ORM Execution.
    """

    def __init__(self):
        self._last_referenced_lg: Optional[Dict[str, Any]] = None
        self._current_query_params: Dict[str, Any] = {}

    def classify_and_interpret(self, user_question: str) -> Dict[str, Any]:
        q_raw = user_question.strip()
        q_lower = q_raw.lower()

        # 0. Security, Exploitation & Malicious Intent Guardrail
        security_threat_keywords = [
            "hack", "exploit", "bypass security", "bypass auth", "bypass guardrail", "bypass login",
            "sql injection", "sqli", "xss", "cross-site scripting", "ddos", "dos attack",
            "steal data", "leak password", "crack password", "unauthorized access",
            "privilege escalation", "backdoor", "vulnerability scanner", "penetrate",
            "jailbreak", "override guardrail", "ignore instructions", "how can i hack",
            "how to hack", "hack this system", "hack the platform", "hack database"
        ]
        if any(kw in q_lower for kw in security_threat_keywords):
            return {
                "suggested_level": 3,
                "topic": "security_policy",
                "intent": "security_rejected",
                "parameters": {"query": q_raw}
            }

        # Non-treasury rejection (guard against sports/weather/trivia, but permit treasury questions like "who won the rfq/deal")
        is_treasury_query = any(w in q_lower for w in ["rfq", "rfqs", "deal", "trade", "bank", "quote", "quotation", "lg", "guarantee", "facility", "treasury"])
        if not is_treasury_query and any(kw in q_lower for kw in ["capital of", "weather in", "recipe", "who won", "president of", "football", "tell me a joke"]):
            return {
                "suggested_level": 3,
                "topic": "non_treasury",
                "intent": "rejected_scope",
                "parameters": {}
            }

        # Capability gap (transaction execution)
        if any(kw in q_lower for kw in ["wire transfer", "execute transfer", "payout", "send money", "automatic wire"]):
            return {
                "suggested_level": 3,
                "topic": "capability_gap",
                "intent": "capability_gap",
                "parameters": {}
            }

        # Feedback & Problem Listener Engine
        feedback_triggers = [
            "i want to give feedback", "share feedback", "give feedback", "report a bug",
            "report an issue", "feature request", "suggest a feature", "i wish there was",
            "i wish we had", "i have a complaint", "problem with the system", "it would be great if",
            "can you add a feature", "why does it take so long", "found a bug", "found an issue",
            "bug:", "bug report", "there is an issue with", "problem with", "system is slow",
            "something is wrong", "fails when", "crash when", "crashes when"
        ]
        if any(trig in q_lower for trig in feedback_triggers):
            # Extract feedback type
            f_type = "FEATURE_REQUEST" if any(w in q_lower for w in ["feature", "wish", "suggest", "add a", "great if"]) else (
                "BUG_REPORT" if any(w in q_lower for w in ["bug", "error", "broken", "fails", "crash"]) else (
                    "USABILITY_PAIN_POINT" if any(w in q_lower for w in ["slow", "confusing", "hard to", "difficult", "take so long"]) else "GENERAL_FEEDBACK"
                )
            )
            return {
                "suggested_level": 1,
                "topic": "feedback",
                "intent": "report_feedback",
                "parameters": {"message": q_raw, "feedback_type": f_type}
            }

        # Daily Treasury Pulse / Morning Briefing
        if any(kw in q_lower for kw in ["daily pulse", "morning pulse", "treasury pulse", "morning briefing", "daily briefing", "briefing", "pulse"]):
            return {
                "suggested_level": 0,
                "topic": "treasury",
                "intent": "get_daily_pulse",
                "parameters": {}
            }

        # Check for multi-step compound analytical queries (Level 2)
        if any(conn in q_lower for conn in ["and also", "and which", "compare", "correlation", "cross-analyze", "breakdown and"]):
            return {
                "suggested_level": 2,
                "topic": "treasury",
                "intent": "search_lgs",
                "parameters": {"query": q_raw}
            }

        # Highest / Largest / Maximum Amount Guarantees
        if any(kw in q_lower for kw in [
            "highest lg", "largest lg", "biggest lg", "highest amount", "largest amount",
            "biggest amount", "highest value", "largest guarantee", "biggest guarantee",
            "top lg by amount", "highest amount in my portfolio", "maximum lg", "max lg amount",
            "highest exposure lg", "which is the highest lg"
        ]):
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "search_lgs",
                "parameters": {"sort_by": "amount_desc", "limit": 5}
            }

        # Who Did / When Was (Audit Trail & Actor Attribution)
        if any(kw in q_lower for kw in [
            "who did", "who created", "who approved", "who deleted", "who updated", "who recorded",
            "who modified", "who performed", "who logged in", "when was", "when did", "who issued"
        ]):
            return {
                "suggested_level": 1,
                "topic": "system",
                "intent": "get_audit_history",
                "parameters": {"scope": "all_organization", "limit": 10}
            }

        # 1. Pronoun / Context continuation
        if self._last_referenced_lg and any(kw in q_lower for kw in [
            "this lg", "this guarantee", "it", "about it", "who is the beneficiary",
            "which bank issued it", "what is its amount", "when does it expire",
            "show me details", "more details", "what is the currency"
        ]):
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "get_lg_details",
                "parameters": {
                    "lg_id": self._last_referenced_lg.get("lg_id"),
                    "lg_number": self._last_referenced_lg.get("lg_number")
                }
            }

        # Feedback & Problem Listener Engine (Check BEFORE guide_patterns so feedback questions are not hijacked by 'how to')
        feedback_triggers = [
            "feedback", "feature request", "suggest a feature", "i wish there was",
            "i wish we had", "report a bug", "report an issue", "found a bug", "found an issue",
            "bug:", "bug report", "problem with the system", "it would be great if",
            "can you add a feature", "why does it take so long", "there is an issue with", "problem with",
            "system is slow", "something is wrong", "fails when", "crash when", "crashes when",
            "how to share feedback", "how to give feedback", "share feedback", "giving feedback",
            "sharing feedback"
        ]
        if any(trig in q_lower for trig in feedback_triggers):
            if any(w in q_lower for w in ["bug", "error", "fail", "crash", "wrong", "broken", "issue"]):
                fb_type = "BUG_REPORT"
            elif any(w in q_lower for w in ["wish", "feature", "suggest", "add", "would be great"]):
                fb_type = "FEATURE_REQUEST"
            elif any(w in q_lower for w in ["slow", "difficult", "hard", "confusing", "pain", "strange", "annoying"]):
                fb_type = "USABILITY_PAIN_POINT"
            else:
                fb_type = "GENERAL_FEEDBACK"

            return {
                "suggested_level": 1,
                "topic": "system",
                "intent": "report_feedback",
                "parameters": {
                    "message": q_raw,
                    "feedback_type": fb_type
                }
            }

        # Quotation Data: Direct Deal or RFQ Reference Lookup
        deal_ref_match = re.search(r'\b((?:DEAL|RFQ|EXEC)-[A-Za-z0-9_-]+)\b', q_raw, re.IGNORECASE)
        if deal_ref_match:
            return {
                "suggested_level": 1,
                "topic": "quotation",
                "intent": "lookup_deal_reference",
                "parameters": {"ref": deal_ref_match.group(1).strip()}
            }
        
        extracted_deal_word = re.search(r'(?:deal\s*(?:ref|reference|#)?|rfq\s*(?:ref|reference|#)?|execution\s*(?:ref|reference)?)\s*[:#]?\s*([A-Za-z0-9_-]{3,})', q_raw, re.IGNORECASE)
        if extracted_deal_word and extracted_deal_word.group(1).lower() not in ["ref", "rate", "details", "summary", "stats", "history"]:
            return {
                "suggested_level": 1,
                "topic": "quotation",
                "intent": "lookup_deal_reference",
                "parameters": {"ref": extracted_deal_word.group(1).strip()}
            }

        # Quotation Data: Participating Banks in Last / Specific RFQ
        if any(w in q_lower for w in [
            "participating banks", "who participated", "invited banks", "banks in the last rfq",
            "banks in last rfq", "who quoted on the last rfq", "last rfq banks", "banks in our last rfq",
            "who was in our last rfq", "participating banks in our last rfq", "who quoted on our last rfq",
            "invited banks in last rfq", "which banks participated"
        ]):
            rfq_match = re.search(r'\b(RFQ-[A-Za-z0-9_-]+)\b', q_raw, re.IGNORECASE)
            return {
                "suggested_level": 1,
                "topic": "quotation",
                "intent": "get_last_rfq_participants",
                "parameters": {"rfq_ref": rfq_match.group(1) if rfq_match else None}
            }

        # Quotation Data: Top Winning Relationship Banks
        if any(w in q_lower for w in [
            "maximum number of rfqs", "most rfqs", "most deals", "who won the most",
            "which bank won the most", "top winning banks", "highest winning bank",
            "bank with most deals", "winning relationship banks", "bank rankings",
            "who won most rfqs", "who wins the most deals", "top quoting banks"
        ]):
            return {
                "suggested_level": 1,
                "topic": "quotation",
                "intent": "get_top_winning_banks",
                "parameters": {"limit": 5}
            }

        # Quotation Data: RFQ Portfolio / Execution History Summary
        if any(w in q_lower for w in [
            "how many rfqs did we execute", "how many deals executed", "quotation history summary",
            "rfq summary", "total quotation volume", "show quotation stats", "rfq statistics",
            "executed quotation volume", "how many rfqs", "executed deals summary"
        ]):
            return {
                "suggested_level": 1,
                "topic": "quotation",
                "intent": "get_quotation_summary",
                "parameters": {}
            }

        # 2. Level 4 System Guides & Navigation (using word boundaries to prevent 'show top' colliding with 'how to')
        is_greeting = q_lower in ["hi", "hello", "hey", "start", "help", "who are you", "what can you do", "restart", "reset"]
        guide_patterns = [
            r"\bhow\s+can\s+i\b", r"\bhow\s+do\s+i\b", r"\bhow\s+to\b",
            r"\bwhere\s+can\s+i\b", r"\bguide\s+me\b", r"\bsteps\s+to\b",
            r"\bwhere\s+do\s+i\s+find\b", r"\bnavigation\b",
            r"\bcan\s+i\b", r"\bcan\s+we\b", r"\bhow\s+do\s+we\b",
            r"\bhow\s+can\s+we\b", r"\bhow\s+to\s+change\b", r"\bhow\s+to\s+make\b",
            r"\bwhat\s+does\b", r"\bwhat\s+is\s+going\s+to\s+happen\b",
            r"\bwhat\s+will\s+happen\b", r"\bif\s+i\s+change\b", r"\bif\s+i\s+did\s+not\b",
            r"\bhow\s+does\b", r"\bhow\s+do\b", r"\bhow\s+is\b", r"\bhow\s+are\b",
            r"\bhow\s+works?\b", r"\bexplain\b", r"\btell\s+me\s+about\b",
            r"\bwhat\s+is\s+the\s+quotation\b", r"\bwhat\s+is\s+quotation\b",
            r"\bquotation\s+module\b", r"\bquotations\s+module\b",
            r"\bacceptance\s+window\b", r"\bacceptance\s+timeout\b", r"\bauto[-_]?accept\b", r"\bauto[-_]?reject\b",
            r"\btolerance\s+limit\b", r"\btolerance\s+percent\b", r"\bcbe\s+benchmark\b",
            r"\bdealer\s+roles?\b", r"\btrader\s+takeover\b", r"\bmulti[- ]leg\b",
            r"\badditional\s+costs?\b", r"\bfinal\s+adjusted\s+price\b",
            r"\bcustody\s+module\b", r"\bissuance\s+module\b", r"\breconciliation\s+module\b",
            r"\bmaker\s+checker\b", r"\bapproval\s+matrix\b"
        ]
        if is_greeting or any(re.search(pat, q_lower) for pat in guide_patterns):
            return {
                "suggested_level": 4,
                "topic": "system_navigation",
                "intent": "system_help",
                "parameters": {"query": q_raw}
            }

        # 3. Level 3 Treasury Glossary
        for term in OFFLINE_TREASURY_GLOSSARY:
            if re.search(rf'\b{re.escape(term)}\b', q_lower):
                return {
                    "suggested_level": 3,
                    "topic": "general_treasury",
                    "intent": "general_treasury",
                    "parameters": {"term": term}
                }

        # 4. Profile & Permissions
        if any(kw in q_lower for kw in ["my role", "my profile", "my permissions", "what can i do", "who am i", "my access"]):
            return {"suggested_level": 0, "topic": "system", "intent": "get_user_profile", "parameters": {}}

        # 5. Audit Log & Recent Activity
        if any(kw in q_lower for kw in ["audit log", "recent actions", "what did i do", "activity history", "audit trail", "recent activity"]):
            scope = "my_actions" if any(w in q_lower for w in ["i do", "my actions", "my activity"]) else "all_organization"
            return {"suggested_level": 0, "topic": "system", "intent": "get_audit_history", "parameters": {"scope": scope}}

        # 6. Action Center & To-Dos
        if any(kw in q_lower for kw in ["action center", "awaiting bank reply", "awaiting reply", "pending print", "undelivered", "instructions awaiting"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_action_center_summary", "parameters": {}}

        # 7. Bank Facilities & Headroom
        if any(kw in q_lower for kw in ["facility", "facilities", "headroom", "credit limit", "credit line", "facility utilization", "available limit"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_facility_analytics", "parameters": {}}

        # 8. LG Issuance Pipeline (Outbound Requests)
        if any(kw in q_lower for kw in [
            "issuance pipeline", "issuance requests", "outbound lg", "outbound guarantees",
            "issued lgs", "issued guarantees", "how many issued", "pending issuance", "rejected requests"
        ]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_issuance_summary", "parameters": {}}

        # 9a. Specific Inbound Issuers / Contractors / Applicants
        if any(kw in q_lower for kw in ["top issuers", "top contractors", "top applicants", "custody issuers", "issuing counterparties", "who gave us lgs"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_top_issuers", "parameters": {"limit": 5}}

        # 9b. Subsidiary / Internal Entity Distribution
        if any(kw in q_lower for kw in ["subsidiary", "subsidiaries", "entity distribution", "distributed among my companies", "distributed among our companies", "entity breakdown"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_entity_distribution", "parameters": {}}

        # 9c. Top Beneficiaries / Counterparties (Bi-Module Aware)
        if any(kw in q_lower for kw in [
            "top beneficiaries", "show top beneficiaries", "beneficiaries", "beneficiary concentration",
            "highest beneficiary", "major beneficiaries", "top counterparty", "who are our beneficiaries",
            "outbound beneficiaries", "issuance beneficiaries"
        ]):
            scope = "issuance" if any(w in q_lower for w in ["outbound", "issuance", "issued to"]) else "all"
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_top_beneficiaries", "parameters": {"limit": 5, "scope": scope}}

        # 10. Bank Exposure Distribution
        if any(kw in q_lower for kw in ["bank exposure", "bank concentration", "exposure by bank", "distribution by bank", "banks holding"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_bank_exposure", "parameters": {"limit": 5}}

        # 11. Pending Approvals
        if any(kw in q_lower for kw in ["pending approval", "approvals pending", "requests to approve", "awaiting my approval"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_pending_approvals", "parameters": {}}

        # 12. Direct LG Number Lookup
        lg_num_match = re.search(r'\b([A-Za-z0-9_]+/[A-Za-z0-9_/-]+|LG-[A-Za-z0-9_-]+|ACME/[A-Za-z0-9_/-]+)\b', q_raw)
        if lg_num_match:
            matched_num = lg_num_match.group(1)
            if not any(k == matched_num.lower() for k in ["how/why", "and/or", "n/a"]):
                return {
                    "suggested_level": 1,
                    "topic": "treasury",
                    "intent": "get_lg_details",
                    "parameters": {"lg_number": matched_num}
                }

        # 13. Expiry Horizons
        days_window_match = re.search(r'\b(?:within|in|next|before|under)?\s*(\d+)\s*days?\b', q_lower)
        if days_window_match:
            parsed_days = int(days_window_match.group(1))
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "find_expiring_lgs",
                "parameters": {"days": parsed_days}
            }

        months_window_match = re.search(r'\b(?:within|in|next|before|under)?\s*(\d+)\s*months?\b', q_lower)
        if months_window_match:
            parsed_months = int(months_window_match.group(1))
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "find_expiring_lgs",
                "parameters": {"days": parsed_months * 30}
            }

        for month_name in ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]:
            if month_name in q_lower:
                return {
                    "suggested_level": 1,
                    "topic": "treasury",
                    "intent": "find_expiring_lgs",
                    "parameters": {"month": month_name.capitalize(), "days": 90}
                }

        if any(kw in q_lower for kw in ["expir", "upcoming expiries", "due for renewal", "expiring soon"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "find_expiring_lgs", "parameters": {"days": 60}}

        # 14. Currency Exposure & Search
        for curr_code, curr_keywords in [
            ("USD", ["usd", "dollar", "dollars"]),
            ("EGP", ["egp", "egyptian pound", "egyptian pounds", "pounds"]),
            ("EUR", ["eur", "euro", "euros"]),
            ("SAR", ["sar", "riyal", "riyals"]),
            ("AED", ["aed", "dirham", "dirhams"]),
            ("GBP", ["gbp", "sterling"])
        ]:
            if any(k in q_lower for k in curr_keywords):
                if any(v in q_lower for v in ["how much", "exposure", "total", "value", "portfolio in", "how many"]):
                    return {
                        "suggested_level": 1,
                        "topic": "treasury",
                        "intent": "get_lg_analytics_summary",
                        "parameters": {"currency": curr_code}
                    }
                if any(v in q_lower for v in ["list", "show", "find", "lgs in", "lg's in", "in usd", "in egp", "in eur", "in sar", "in aed", "in gbp", "guarantee"]):
                    return {
                        "suggested_level": 1,
                        "topic": "treasury",
                        "intent": "search_lgs",
                        "parameters": {"currency": curr_code}
                    }

        # 15. Search & Status Filters
        comp_match = re.search(r'\b(?:for|company|beneficiary)\s+([A-Za-z0-9_-]+)\b', q_raw)
        extracted_comp = comp_match.group(1).strip() if comp_match else None

        bank_match = re.search(r'\b(?:with|at|bank)\s+([A-Za-z0-9_-]+)\b', q_raw)
        extracted_bank = bank_match.group(1).strip() if bank_match else None

        status_match = re.search(r'\b(?:status|state)\s+([A-Za-z0-9_-]+)\b', q_lower)
        extracted_status = status_match.group(1).strip() if status_match else None

        if not extracted_status:
            for st_word in ["suspended", "draft", "cancelled", "expired", "released", "liquidated"]:
                if st_word in q_lower:
                    extracted_status = st_word
                    break

        if not extracted_status and any(kw in q_lower for kw in ["active", "valid"]):
            extracted_status = "valid"

        if extracted_comp or extracted_bank or extracted_status:
            s_params = {}
            if extracted_status: s_params["status"] = extracted_status
            if extracted_comp: s_params["query"] = extracted_comp
            if extracted_bank: s_params["bank"] = extracted_bank
            return {"suggested_level": 1, "topic": "treasury", "intent": "search_lgs", "parameters": s_params}

        # 15.5 Position & Latest Guarantees Inquiries
        if any(kw in q_lower for kw in [
            "position", "my position", "our position", "latest position", "lg position", "guarantee position",
            "current position", "latest lg position", "overall position", "consolidated position", "financial position"
        ]):
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "get_lg_analytics_summary",
                "parameters": {"scope": "position_overview"}
            }

        if any(kw in q_lower for kw in [
            "latest lg", "latest lgs", "recent lg", "recent lgs", "newest lg", "newest lgs",
            "new lgs", "new guarantees", "latest guarantees", "recent guarantees", "newly added lgs"
        ]) and "position" not in q_lower:
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "search_lgs",
                "parameters": {"sort_by": "date_desc", "limit": 10}
            }

        # 16. Broad Portfolio Overview
        if any(kw in q_lower for kw in [
            "how many lg", "how many guarantees", "our lg", "our portfolio", "guarantees do i have",
            "guarantees do we have", "portfolio overview", "portfolio summary", "total lg", "exposure overview",
            "show my lg", "show our lg", "active guarantees", "all lgs", "all guarantees"
        ]):
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "get_lg_analytics_summary",
                "parameters": {"scope": "unified"}
            }

        # 17. Level 2 Fallback
        return {
            "suggested_level": 2,
            "topic": "treasury",
            "intent": "search_lgs",
            "parameters": {"query": q_raw}
        }

    def execute_orm_query(
        self,
        db: Session,
        customer_id: int,
        user_id: int,
        intent: str,
        params: Dict[str, Any],
        has_all_entity_access: bool = True,
        entity_ids: Optional[List[int]] = None
    ) -> Any:
        self._current_query_params = params

        custody_base = db.query(LGRecord).filter(
            LGRecord.customer_id == customer_id,
            LGRecord.is_deleted == False
        )
        issuance_base = db.query(IssuanceRequest).filter(
            IssuanceRequest.customer_id == customer_id,
            IssuanceRequest.is_deleted == False
        )
        facility_base = db.query(IssuanceFacility).filter(
            IssuanceFacility.customer_id == customer_id,
            IssuanceFacility.is_deleted == False
        )

        if not has_all_entity_access and entity_ids:
            custody_base = custody_base.filter(LGRecord.entity_id.in_(entity_ids))
            issuance_base = issuance_base.filter(IssuanceRequest.issuing_entity_id.in_(entity_ids))

        if intent == "get_user_profile":
            user = db.query(User).filter(User.id == user_id).first()
            customer = db.query(Customer).filter(Customer.id == customer_id).first()
            return {"user": user, "customer": customer}

        if intent == "get_audit_history":
            scope = params.get("scope", "my_actions")
            limit = params.get("limit", 15)
            search_term = params.get("search_term")
            q = db.query(AuditLog).filter(AuditLog.customer_id == customer_id)
            if scope == "my_actions":
                q = q.filter(AuditLog.user_id == user_id)
            if search_term:
                q = q.filter(
                    or_(
                        AuditLog.entity_type.ilike(f"%{search_term}%"),
                        AuditLog.action_type.ilike(f"%{search_term}%")
                    )
                )
            return q.options(joinedload(AuditLog.user)).order_by(desc(AuditLog.timestamp)).limit(limit).all()

        if intent == "find_expiring_lgs":
            days = params.get("days", 60)
            today = datetime.now(timezone.utc).date()
            target_date = today + timedelta(days=days)

            q = custody_base.join(LGRecord.lg_status).filter(
                func.upper(LgStatus.name) == "VALID",
                LGRecord.expiry_date >= today,
                LGRecord.expiry_date <= target_date
            ).order_by(LGRecord.expiry_date.asc())
            return q.options(joinedload(LGRecord.lg_currency), joinedload(LGRecord.issuing_bank)).all()

        if intent == "get_lg_analytics_summary":
            currency_filter = params.get("currency")
            scope = params.get("scope", "unified")

            valid_custody = custody_base.join(LGRecord.lg_status).filter(func.upper(LgStatus.name) == "VALID")
            if currency_filter:
                valid_custody = valid_custody.join(LGRecord.lg_currency).filter(
                    func.upper(Currency.iso_code) == currency_filter.upper()
                )
            custody_records = valid_custody.options(joinedload(LGRecord.lg_currency)).all()
            issuance_requests = issuance_base.all()
            facilities = facility_base.options(joinedload(IssuanceFacility.bank), joinedload(IssuanceFacility.currency)).all()

            return {
                "custody_records": custody_records,
                "issuance_requests": issuance_requests,
                "facilities": facilities,
                "currency_filter": currency_filter,
                "scope": scope
            }

        if intent == "get_issuance_summary":
            return issuance_base.order_by(desc(IssuanceRequest.created_at)).all()

        if intent == "get_facility_analytics":
            return facility_base.options(
                joinedload(IssuanceFacility.bank),
                joinedload(IssuanceFacility.currency)
            ).all()

        if intent == "get_last_rfq_participants":
            rfq_ref = params.get("rfq_ref")
            q = db.query(QuotationRequest).filter(
                QuotationRequest.customer_id == customer_id,
                QuotationRequest.is_deleted == False
            )
            if rfq_ref:
                rfq = q.filter(QuotationRequest.ref_no.ilike(f"%{rfq_ref}%")).first()
            else:
                rfq = q.order_by(desc(QuotationRequest.created_at)).first()

            if not rfq:
                return None

            assignments = db.query(QuotationBankAssignment).filter(
                QuotationBankAssignment.rfq_id == rfq.id
            ).all()

            legs = db.query(QuotationLeg).filter(
                QuotationLeg.rfq_id == rfq.id
            ).order_by(QuotationLeg.leg_index.asc()).all()

            bank_data = []
            for a in assignments:
                bank_name = a.quotation_bank.bank.name if a.quotation_bank and a.quotation_bank.bank else "Bank"
                offers = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == a.id).all()
                tbill_offers = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id == a.id).all()

                quoted = len(offers) > 0 or len(tbill_offers) > 0
                best_price = min([o.price for o in offers]) if offers else None
                best_discount = min([t.discount_rate for t in tbill_offers]) if tbill_offers else None
                is_winner = any(l.winner_bank_id == a.quotation_bank.bank_id for l in legs if l.winner_bank_id and a.quotation_bank)

                bank_data.append({
                    "bank_name": bank_name,
                    "quoted": quoted,
                    "quotes_count": len(offers) + len(tbill_offers),
                    "best_price": best_price,
                    "best_discount": best_discount,
                    "is_winner": is_winner,
                    "approval_status": a.approval_status
                })

            return {
                "rfq": rfq,
                "legs": legs,
                "banks": bank_data
            }

        if intent == "get_top_winning_banks":
            limit = params.get("limit", 5)
            legs = db.query(QuotationLeg).join(QuotationRequest).filter(
                QuotationRequest.customer_id == customer_id,
                QuotationRequest.is_deleted == False,
                QuotationLeg.winner_bank_name.isnot(None),
                QuotationLeg.status.in_(["COMPLETED", "ACCEPTED"])
            ).all()

            bank_stats = {}
            for l in legs:
                b_name = l.winner_bank_name or "Unknown Bank"
                if b_name not in bank_stats:
                    bank_stats[b_name] = {
                        "bank_name": b_name,
                        "deals_won": 0,
                        "currencies": set(),
                        "total_volume": 0.0,
                        "currency_volumes": {},
                        "latest_deal_ref": l.execution_reference or (l.rfq.ref_no if l.rfq else None),
                        "latest_date": l.updated_at or l.created_at
                    }
                s = bank_stats[b_name]
                s["deals_won"] += 1
                c_pair = l.currency_pair or (f"{l.buy_currency}/{l.sell_currency}" if l.buy_currency else "FX")
                s["currencies"].add(c_pair)
                amt = float(l.amount or 0.0)
                s["total_volume"] += amt
                curr = l.buy_currency or "USD"
                s["currency_volumes"][curr] = s["currency_volumes"].get(curr, 0.0) + amt

            sorted_banks = sorted(bank_stats.values(), key=lambda x: x["deals_won"], reverse=True)
            return {
                "total_completed_deals": len(legs),
                "ranked_banks": sorted_banks[:limit]
            }

        if intent == "lookup_deal_reference":
            ref = str(params.get("ref") or "").strip()
            leg = db.query(QuotationLeg).join(QuotationRequest).filter(
                QuotationRequest.customer_id == customer_id,
                QuotationRequest.is_deleted == False,
                or_(
                    QuotationLeg.execution_reference.ilike(f"%{ref}%"),
                    QuotationRequest.ref_no.ilike(f"%{ref}%")
                )
            ).order_by(desc(QuotationLeg.created_at)).first()

            rfq = None
            if not leg:
                rfq = db.query(QuotationRequest).filter(
                    QuotationRequest.customer_id == customer_id,
                    QuotationRequest.is_deleted == False,
                    QuotationRequest.ref_no.ilike(f"%{ref}%")
                ).first()
                if rfq and rfq.legs:
                    leg = rfq.legs[0]

            return {
                "ref_searched": ref,
                "leg": leg,
                "rfq": leg.rfq if leg else rfq
            }

        if intent == "get_quotation_summary":
            rfqs = db.query(QuotationRequest).filter(
                QuotationRequest.customer_id == customer_id,
                QuotationRequest.is_deleted == False
            ).order_by(desc(QuotationRequest.created_at)).all()

            legs = db.query(QuotationLeg).join(QuotationRequest).filter(
                QuotationRequest.customer_id == customer_id,
                QuotationRequest.is_deleted == False
            ).all()

            return {
                "rfqs": rfqs,
                "legs": legs
            }

        if intent == "get_daily_pulse":
            now_dt = datetime.utcnow()
            d14 = (now_dt + timedelta(days=14)).date()
            expiring_14 = custody_base.join(LGRecord.lg_status).filter(
                func.upper(LgStatus.name) == "VALID",
                LGRecord.expiry_date != None,
                func.date(LGRecord.expiry_date) <= d14,
                func.date(LGRecord.expiry_date) >= now_dt.date()
            ).options(joinedload(LGRecord.lg_currency), joinedload(LGRecord.issuing_bank)).all()

            instructions = db.query(LGInstruction).filter(LGInstruction.is_deleted == False).all()
            pending_issuance = issuance_base.filter(
                IssuanceRequest.status.in_(["PENDING_APPROVAL", "SUBMITTED", "PENDING"])
            ).all()

            facilities = facility_base.options(
                joinedload(IssuanceFacility.bank),
                joinedload(IssuanceFacility.currency)
            ).all()

            return {
                "expiring_14": expiring_14,
                "instructions": instructions,
                "pending_issuance": pending_issuance,
                "facilities": facilities
            }

        if intent == "report_feedback":
            user = db.query(User).filter(User.id == user_id).first()
            u_email = user.email if user else None
            msg_text = params.get("message", "")
            f_type = params.get("feedback_type", "GENERAL_FEEDBACK")
            sentiment = "NEGATIVE" if any(w in msg_text.lower() for w in ["slow", "bug", "error", "confusing", "hard", "problem", "fail", "bad", "crash", "broken", "strange"]) else (
                "POSITIVE" if any(w in msg_text.lower() for w in ["love", "great", "awesome", "good", "helpful", "like"]) else "NEUTRAL"
            )

            # Only save if it's actual feedback, not just asking how to give feedback
            is_intro = (
                msg_text.lower().strip() in ["i want to give feedback", "share feedback", "give feedback", "feedback", "how to give feedback", "how to share feedback"]
                or "don't know how to use" in msg_text.lower()
                or "how to use it" in msg_text.lower()
                or "how do i use it" in msg_text.lower()
                or "how does feedback work" in msg_text.lower()
                or "sharing feedback is very strange" in msg_text.lower()
            )

            fb_id = None
            if not is_intro and msg_text.strip():
                feedback_entry = UserFeedback(
                    customer_id=customer_id,
                    user_id=user_id,
                    user_email=u_email,
                    feedback_type=f_type,
                    sentiment=sentiment,
                    message=msg_text,
                    status="NEW"
                )
                db.add(feedback_entry)
                db.commit()
                db.refresh(feedback_entry)
                fb_id = feedback_entry.id

            return {
                "feedback_id": fb_id or "N/A",
                "feedback_type": f_type,
                "sentiment": sentiment,
                "message": msg_text,
                "is_intro": is_intro
            }

        if intent == "get_action_center_summary":
            instructions = db.query(LGInstruction).join(LGRecord).filter(
                LGRecord.customer_id == customer_id,
                LGInstruction.is_deleted == False
            ).all()
            pending_issuance = issuance_base.filter(
                IssuanceRequest.status.in_(["PENDING_APPROVAL", "SUBMITTED", "PENDING"])
            ).all()
            return {
                "instructions": instructions,
                "pending_issuance": pending_issuance
            }

        if intent == "get_top_beneficiaries":
            # Return both Inbound Custody entities + Outbound Issuance beneficiaries for unified intelligence
            custody_records = custody_base.join(LGRecord.lg_status).filter(
                func.upper(LgStatus.name) == "VALID"
            ).options(
                joinedload(LGRecord.beneficiary_corporate),
                joinedload(LGRecord.lg_currency)
            ).all()

            outbound_records = db.query(
                IssuanceRequest.beneficiary_name,
                Currency.iso_code,
                func.sum(IssuanceRequest.amount)
            ).join(Currency, IssuanceRequest.currency_id == Currency.id).filter(
                IssuanceRequest.customer_id == customer_id,
                IssuanceRequest.is_deleted == False,
                IssuanceRequest.beneficiary_name != None
            ).group_by(IssuanceRequest.beneficiary_name, Currency.iso_code).all()

            inbound_issuers = db.query(
                LGRecord.issuer_name,
                Currency.iso_code,
                func.sum(LGRecord.lg_amount)
            ).join(Currency, LGRecord.lg_currency_id == Currency.id).join(LGRecord.lg_status).filter(
                LGRecord.customer_id == customer_id,
                LGRecord.is_deleted == False,
                func.upper(LgStatus.name) == "VALID",
                LGRecord.issuer_name != None
            ).group_by(LGRecord.issuer_name, Currency.iso_code).all()

            return {
                "custody_records": custody_records,
                "outbound_records": outbound_records,
                "inbound_issuers": inbound_issuers
            }

        if intent == "get_top_issuers":
            return db.query(
                LGRecord.issuer_name,
                Currency.iso_code,
                func.sum(LGRecord.lg_amount)
            ).join(Currency, LGRecord.lg_currency_id == Currency.id).join(LGRecord.lg_status).filter(
                LGRecord.customer_id == customer_id,
                LGRecord.is_deleted == False,
                func.upper(LgStatus.name) == "VALID",
                LGRecord.issuer_name != None
            ).group_by(LGRecord.issuer_name, Currency.iso_code).all()

        if intent == "get_entity_distribution":
            return custody_base.join(LGRecord.lg_status).filter(
                func.upper(LgStatus.name) == "VALID"
            ).options(
                joinedload(LGRecord.beneficiary_corporate),
                joinedload(LGRecord.lg_currency)
            ).all()

        if intent == "get_bank_exposure":
            return custody_base.join(LGRecord.lg_status).filter(
                func.upper(LgStatus.name) == "VALID"
            ).options(
                joinedload(LGRecord.issuing_bank),
                joinedload(LGRecord.lg_currency)
            ).all()

        if intent == "search_lgs":
            status_filter = params.get("status")
            currency_filter = params.get("currency")
            bank_filter = params.get("bank")
            query_val = params.get("query") or params.get("search_term")
            min_amount = params.get("min_amount")
            sort_by = params.get("sort_by")
            limit = params.get("limit")

            q = custody_base
            if status_filter:
                st_upper = status_filter.upper()
                q = q.join(LGRecord.lg_status).filter(func.upper(LgStatus.name) == st_upper)
            if currency_filter:
                q = q.join(LGRecord.lg_currency).filter(func.upper(Currency.iso_code) == currency_filter.upper())
            if bank_filter:
                q = q.join(LGRecord.issuing_bank).filter(Bank.name.ilike(f"%{bank_filter}%"))
            if min_amount:
                q = q.filter(LGRecord.lg_amount >= float(min_amount))
            if query_val:
                q = q.outerjoin(LGRecord.beneficiary_corporate).filter(
                    or_(
                        CustomerEntity.entity_name.ilike(f"%{query_val}%"),
                        LGRecord.lg_number.ilike(f"%{query_val}%"),
                        LGRecord.description_purpose.ilike(f"%{query_val}%")
                    )
                )

            if sort_by == "amount_desc":
                q = q.filter(LGRecord.lg_amount != None).order_by(desc(LGRecord.lg_amount))
            elif sort_by == "date_desc":
                q = q.order_by(desc(LGRecord.issuance_date), desc(LGRecord.id))

            q = q.options(
                joinedload(LGRecord.lg_currency),
                joinedload(LGRecord.issuing_bank),
                joinedload(LGRecord.lg_status),
                joinedload(LGRecord.beneficiary_corporate)
            )
            if limit:
                return q.limit(limit).all()
            return q.all()

        if intent == "get_lg_details":
            lg_id = params.get("lg_id")
            lg_number = params.get("lg_number")
            q = custody_base
            if lg_id:
                q = q.filter(LGRecord.id == lg_id)
            elif lg_number:
                q = q.filter(LGRecord.lg_number.ilike(f"%{lg_number}%"))
            return q.options(
                joinedload(LGRecord.lg_currency),
                joinedload(LGRecord.issuing_bank),
                joinedload(LGRecord.lg_status),
                joinedload(LGRecord.beneficiary_corporate)
            ).first()

        if intent == "get_pending_approvals":
            return issuance_base.filter(
                IssuanceRequest.status.in_(["PENDING_APPROVAL", "SUBMITTED", "PENDING"])
            ).all()

        return []

    def format_application_response(
        self,
        db: Session,
        customer_id: int,
        user_id: int,
        intent: str,
        query_result: Any,
        user_question: str
    ) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, str]]]:
        user = db.query(User).filter(User.id == user_id).first()
        role_code = str(user.role.value if hasattr(user.role, "value") else (user.role.name if hasattr(user.role, "name") else str(user.role))).upper() if (user and user.role) else "END_USER"
        nav_base = "/corporate-admin" if role_code == "CORPORATE_ADMIN" else ("/system-owner" if role_code == "SYSTEM_OWNER" else "/end-user")

        references: List[Dict[str, Any]] = []
        suggested_chips: List[Dict[str, str]] = []

        if intent == "get_user_profile":
            u = query_result.get("user")
            c = query_result.get("customer")
            role_name = (u.role.value if hasattr(u.role, 'value') else (u.role.name if hasattr(u.role, 'name') else str(u.role))).replace("_", " ").title() if (u and u.role) else "End User"
            plan_name = c.subscription_plan.name if (c and hasattr(c, 'subscription_plan') and c.subscription_plan) else "MAX All plan"
            comp_name = c.name if (c and hasattr(c, 'name')) else "Acme Corporation"

            answer = (
                f"👤 **User Profile & Capabilities Overview**:\n\n"
                f"- **User**: `{u.email if u else 'N/A'}`\n"
                f"- **Assigned Role**: **{role_name}**\n"
                f"- **Organization**: **{comp_name}** (Plan: *{plan_name}*)\n"
                f"- **Active Modules**: LG Custody, LG Issuance, FX & T-Bill Quotations, Bank Reconciliation\n"
                f"- **Entity Scope**: All Organization Entities\n"
                f"- **Maker-Checker Dual Control**: Disabled\n\n"
                f"**What you can do with your {role_name} role**:\n"
                f"Full operational access to assigned corporate treasury and guarantee workflows."
            )
            suggested_chips = [
                {"label": "📊 Portfolio Summary", "query": "show portfolio summary"},
                {"label": "📋 My Recent Activity", "query": "what did I do recently"},
                {"label": "🧭 System Capabilities Guide", "query": "what are my permissions"}
            ]
            return answer, references, suggested_chips

        if intent == "get_audit_history":
            logs = query_result or []
            if not logs:
                return "No recent activity recorded for this criteria.", [], []
            lines = [f"📋 **Recent Activity & Audit Trail ({len(logs)} most recent actions)**:\n"]
            for lg in logs[:15]:
                dt_str = lg.timestamp.strftime("%Y-%m-%d %H:%M UTC") if lg.timestamp else "N/A"
                u_email = lg.user.email if (hasattr(lg, 'user') and lg.user) else 'System'
                lines.append(f"- `[{dt_str}]` **{lg.action_type}** on **{lg.entity_type}** (by: *{u_email}*)")
            suggested_chips = [
                {"label": "👤 View My Profile", "query": "show my profile"},
                {"label": "📊 Portfolio Overview", "query": "show portfolio overview"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "find_expiring_lgs":
            records = query_result or []
            if not records:
                return "No active guarantees found matching that expiry timeframe.", [], [
                    {"label": "📅 Expiring in 120 Days", "query": "lgs expiring within 120 days"},
                    {"label": "📊 All Active LGs", "query": "show active LGs"}
                ]

            lines = [f"Found **{len(records)} guarantee(s)** expiring in the specified timeframe:\n"]
            for r in records[:15]:
                curr_code = r.lg_currency.iso_code if r.lg_currency else "EGP"
                bank_name = r.issuing_bank.name if r.issuing_bank else "N/A"
                exp_str = r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else "N/A"
                amt_str = f"{float(r.lg_amount):,.2f}" if r.lg_amount else "0.00"
                lines.append(f"- **{r.lg_number}**: {amt_str} {curr_code} (Bank: *{bank_name}*, Expiry: *{exp_str}*)")
                references.append({
                    "lg_id": r.id,
                    "lg_number": r.lg_number,
                    "expiry_date": exp_str,
                    "amount": float(r.lg_amount) if r.lg_amount else 0.0,
                    "currency": curr_code
                })
            lines.append(f"\n👉 [View All Expiring Guarantees in Custody]({nav_base}/lg-records)")

            suggested_chips = [
                {"label": "📅 Expiring in 120 Days", "query": "lgs expiring within 120 days"},
                {"label": "🏦 Bank Exposure", "query": "show bank exposure"},
                {"label": "📊 Top Beneficiaries", "query": "show top beneficiaries"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_lg_analytics_summary":
            custody_records = query_result.get("custody_records", [])
            issuance_requests = query_result.get("issuance_requests", [])
            facilities = query_result.get("facilities", [])
            curr_filter = query_result.get("currency_filter")

            if curr_filter:
                total_amt = sum(float(r.lg_amount or 0.0) for r in custody_records)
                avg_amt = total_amt / len(custody_records) if custody_records else 0.0
                answer = (
                    f"**{curr_filter.upper()} Portfolio Exposure**:\n\n"
                    f"- **Total Amount**: **{total_amt:,.2f} {curr_filter.upper()}**\n"
                    f"- **Active Guarantees**: **{len(custody_records)} LG(s)**\n"
                    f"- **Average Value**: **{avg_amt:,.2f} {curr_filter.upper()}**\n"
                    f"- **Portfolio Share**: **{round(len(custody_records)/max(len(custody_records), 1)*100, 1)}%** of active portfolio\n\n"
                    f"👉 [View {curr_filter.upper()} Guarantees in Custody]({nav_base}/lg-records)"
                )
                for r in custody_records[:15]:
                    references.append({
                        "lg_id": r.id,
                        "lg_number": r.lg_number,
                        "expiry_date": r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else None,
                        "amount": float(r.lg_amount) if r.lg_amount else 0.0,
                        "currency": curr_filter.upper()
                    })
                suggested_chips = [
                    {"label": "📋 List all in USD", "query": "list of LGs in USD"},
                    {"label": "📊 List all in EGP", "query": "list of LGs in EGP"},
                    {"label": "💶 List all in EUR", "query": "list of LGs in EUR"}
                ]
                return answer, references, suggested_chips

            currency_totals: Dict[str, float] = {}
            for r in custody_records:
                c_code = r.lg_currency.iso_code if r.lg_currency else "EGP"
                currency_totals[c_code] = currency_totals.get(c_code, 0.0) + float(r.lg_amount or 0.0)

            curr_lines = [f"- **{c}**: {amt:,.2f}" for c, amt in currency_totals.items()]
            pending_issuance_cnt = sum(1 for req in issuance_requests if str(req.status).upper() in ["PENDING_APPROVAL", "SUBMITTED", "PENDING"])
            issued_cnt = sum(1 for req in issuance_requests if str(req.status).upper() in ["ISSUED", "COMPLETED"])

            is_position_query = self._current_query_params.get("scope") == "position_overview" or "position" in getattr(self, "_current_user_question", "").lower()
            header_title = "📊 **Consolidated Letter of Guarantee Position**:" if is_position_query else "📊 **Corporate Guarantee Portfolio Overview**:"

            answer = (
                f"{header_title}\n\n"
                f"🛡️ **LG Custody (Inbound Guarantees Held)**:\n"
                f"- **{len(custody_records)} Active Guarantees** in custody\n"
                f"- **Total Exposure by Currency**:\n" + "\n".join(curr_lines) + "\n"
                f"👉 [Open LG Custody Vault]({nav_base}/lg-records)\n\n"
                f"📤 **LG Issuance (Outbound Guarantees Issued)**:\n"
                f"- **{len(issuance_requests)} Total Outbound Requests** ({issued_cnt} Issued, {pending_issuance_cnt} Pending Approval)\n"
                f"- **{len(facilities)} Active Bank Facilities** connected\n"
                f"👉 [Open LG Issuance Dashboard]({nav_base}/issuance/requests)"
            )

            suggested_chips = [
                {"label": "🛡️ View Inbound Custody", "query": "show custody summary"},
                {"label": "📤 View Issuance Pipeline", "query": "show issuance pipeline"},
                {"label": "🏛️ Bank Facility Headroom", "query": "what is our facility headroom"},
                {"label": "📅 Expiring in 60 Days", "query": "lgs expiring within 60 days"}
            ]
            return answer, references, suggested_chips

        if intent == "get_issuance_summary":
            requests = query_result or []
            if not requests:
                return "No issuance requests found for your organization.", [], [
                    {"label": "✍️ Record New Issuance Request", "query": "how do i request an lg"},
                    {"label": "🛡️ View Custody LGs", "query": "show custody LGs"}
                ]

            status_counts: Dict[str, int] = {}
            for req in requests:
                st = str(req.status or "DRAFT").upper()
                status_counts[st] = status_counts.get(st, 0) + 1

            lines = [
                f"📤 **LG Issuance Pipeline ({len(requests)} Total Requests)**:\n",
                f"- **Draft**: {status_counts.get('DRAFT', 0)} requests",
                f"- **Pending Approval**: {status_counts.get('PENDING_APPROVAL', 0) + status_counts.get('PENDING', 0) + status_counts.get('SUBMITTED', 0)} requests",
                f"- **Approved / Ready for Issuance**: {status_counts.get('APPROVED', 0)} requests",
                f"- **Issued & Active**: {status_counts.get('ISSUED', 0) + status_counts.get('COMPLETED', 0)} guarantees",
                f"- **Rejected / Cancelled**: {status_counts.get('REJECTED', 0) + status_counts.get('CANCELLED', 0)} requests\n",
                f"👉 [Open Issuance Requests Manager]({nav_base}/issuance/requests)"
            ]
            suggested_chips = [
                {"label": "⏳ Pending Approvals", "query": "show pending approvals"},
                {"label": "🏛️ Bank Facilities Headroom", "query": "show facility headroom"},
                {"label": "⚡ Action Center Items", "query": "show action center summary"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_facility_analytics":
            facilities = query_result or []
            if not facilities:
                return "No active bank credit facilities found.", [], []

            lines = [f"🏛️ **Bank Credit Facilities & Available Headroom ({len(facilities)} Facilities)**:\n"]
            for f in facilities[:10]:
                b_name = f.bank.name if f.bank else "Bank"
                c_code = f.currency.iso_code if f.currency else "EGP"
                limit_val = float(f.total_limit_amount or 0.0)
                lines.append(f"- **{b_name}**: Limit: **{limit_val:,.2f} {c_code}** (Status: *{f.status or 'Active'}*)")

            lines.append(f"\n👉 [Manage Bank Facilities]({nav_base}/facilities)")
            suggested_chips = [
                {"label": "📤 Issuance Pipeline", "query": "show issuance pipeline"},
                {"label": "🏦 Bank Exposure", "query": "show bank exposure"},
                {"label": "📊 Unified Portfolio Overview", "query": "show portfolio overview"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_last_rfq_participants":
            data = query_result or {}
            rfq = data.get("rfq")
            if not rfq:
                return (
                    "No quotation RFQs have been created yet for your organization.\n\n"
                    f"👉 [Create New Quotation Request]({nav_base}/quotations)"
                ), [], [{"label": "✍️ Create New RFQ", "query": "how to create an rfq"}]

            banks = data.get("banks") or []
            legs = data.get("legs") or []

            inst_type = rfq.type or "FX_SPOT"
            dir_str = f"{rfq.direction} " if rfq.direction else ""
            pair_str = f"{rfq.buy_currency}/{rfq.sell_currency}" if rfq.buy_currency else ""
            amt_str = f"{float(rfq.amount or 0.0):,.2f} {rfq.buy_currency or ''}".strip()
            status_badge = str(rfq.status or "OPEN").upper()

            lines = [
                f"📋 **Participating Banks in Latest RFQ ({rfq.ref_no})**:\n",
                f"- **Instrument**: **{inst_type}** ({dir_str}{pair_str} — {amt_str})",
                f"- **Current Status**: **{status_badge}**",
                f"- **Quotation Window**: {rfq.window_start.strftime('%Y-%m-%d %H:%M') if rfq.window_start else 'N/A'} to {rfq.window_end.strftime('%H:%M UTC') if rfq.window_end else 'N/A'}\n",
                f"🏛️ **Invited Banking Partners ({len(banks)} Banks)**:"
            ]

            if not banks:
                lines.append("- *No banks have been assigned to this RFQ yet.*")
            else:
                for b in banks:
                    b_name = b["bank_name"]
                    if b["quoted"]:
                        rate_info = f"Quoted (Best Rate: **{b['best_price']}**)" if b["best_price"] is not None else (
                            f"Quoted (Discount: **{b['best_discount']}%**)" if b["best_discount"] is not None else "Quote Submitted"
                        )
                        winner_tag = " 🏆 **WINNER / AWARDED**" if b["is_winner"] else ""
                        lines.append(f"- ✓ **{b_name}**: {rate_info}{winner_tag}")
                    else:
                        lines.append(f"- ○ **{b_name}**: Invited (*Pending rate submission*)")

            lines.append(f"\n👉 [Open Quotation in Workspace]({nav_base}/quotations)")

            references.append({
                "rfq_id": str(rfq.id),
                "ref_no": rfq.ref_no,
                "amount": float(rfq.amount or 0.0),
                "status": rfq.status
            })

            suggested_chips = [
                {"label": "🏆 Who won the most RFQs?", "query": "who won the maximum number of rfqs"},
                {"label": "📊 Quotation Stats", "query": "how many rfqs did we execute"},
                {"label": "✍️ Create New RFQ", "query": "how to create an rfq"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_top_winning_banks":
            data = query_result or {}
            total_deals = data.get("total_completed_deals", 0)
            ranked = data.get("ranked_banks") or []

            if not ranked:
                return (
                    "Your organization has not yet awarded or executed any RFQ deals with relationship banks.\n\n"
                    f"👉 [Open Quotation Control Center]({nav_base}/quotations)"
                ), [], [{"label": "✍️ Create New RFQ", "query": "how to create an rfq"}]

            medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
            lines = [
                f"🏆 **Relationship Bank Quotation Rankings & Win Leaderboard**:\n",
                f"Across **{total_deals} Completed Deal(s)**, here are your top executing banks:\n"
            ]

            for idx, b in enumerate(ranked):
                m = medals[idx] if idx < len(medals) else "🔹"
                win_pct = round((b["deals_won"] / max(total_deals, 1)) * 100, 1)
                curr_vol_str = ", ".join([f"{amt:,.0f} {curr}" for curr, amt in b["currency_volumes"].items()])
                lines.append(
                    f"{m} **{b['bank_name']}**: **{b['deals_won']} Deals Won** ({win_pct}% win share)\n"
                    f"   - Volume: {curr_vol_str or 'N/A'} | Pairs: {', '.join(list(b['currencies'])[:3])}"
                )

            lines.append(f"\n👉 [View Historical Analytics in Quotation Center]({nav_base}/quotations)")

            suggested_chips = [
                {"label": "📋 Participating banks in last RFQ", "query": "who was the participating banks in the last rfq"},
                {"label": "📊 Total RFQ volume", "query": "how many rfqs did we execute"},
                {"label": "✍️ Create New RFQ", "query": "how to create an rfq"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "lookup_deal_reference":
            data = query_result or {}
            ref = data.get("ref_searched", "")
            leg = data.get("leg")
            rfq = data.get("rfq")

            if not leg and not rfq:
                return (
                    f"No executed deal or RFQ matching **'{ref}'** was found under your organization.\n\n"
                    f"👉 [Search All Deals in Quotation Control Center]({nav_base}/quotations)"
                ), [], [{"label": "📋 Last RFQ details", "query": "who was the participating banks in the last rfq"}]

            target_ref = leg.execution_reference if (leg and leg.execution_reference) else (rfq.ref_no if rfq else ref)
            winner_bank = leg.winner_bank_name if (leg and leg.winner_bank_name) else "Pending / Multiple"
            rate_val = f"**{leg.winner_rate}**" if (leg and leg.winner_rate is not None) else "Evaluating"
            pair_val = leg.currency_pair if (leg and leg.currency_pair) else (f"{rfq.buy_currency}/{rfq.sell_currency}" if rfq and rfq.buy_currency else "FX")
            amt_val = f"{float(leg.amount or rfq.amount or 0.0):,.2f} {leg.buy_currency or (rfq.buy_currency if rfq else '')}".strip()
            val_date = leg.value_date or (rfq.value_date if rfq else "Standard")

            lines = [
                f"🔖 **Trade Execution Details: {target_ref}**\n",
                f"- **Master RFQ**: **{rfq.ref_no if rfq else 'N/A'}**",
                f"- **Awarded Bank**: **{winner_bank}** 🏆",
                f"- **Execution Rate**: {rate_val} {pair_val}",
                f"- **Trade Volume**: **{amt_val}**",
                f"- **Settlement / Value Date**: **{val_date}**",
                f"- **Status**: **{leg.status if leg else rfq.status}**",
                f"- **Deal Receipt**: ✓ Dual-Branded Trade Confirmation (PDF) generated with SHA-256 integrity seal\n",
                f"👉 [Open Trade Confirmation in Quotation Center]({nav_base}/quotations)"
            ]

            if leg and leg.execution_reference:
                references.append({
                    "deal_ref": leg.execution_reference,
                    "winner_bank": leg.winner_bank_name,
                    "rate": leg.winner_rate,
                    "status": leg.status
                })

            suggested_chips = [
                {"label": "🏆 Top winning banks", "query": "who won the maximum number of rfqs"},
                {"label": "📋 Last RFQ participants", "query": "who was the participating banks in the last rfq"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_quotation_summary":
            data = query_result or {}
            rfqs = data.get("rfqs") or []
            legs = data.get("legs") or []

            if not rfqs:
                return (
                    "No quotation RFQs have been created yet for your organization.\n\n"
                    f"👉 [Create Your First RFQ]({nav_base}/quotations)"
                ), [], [{"label": "✍️ Create New RFQ", "query": "how to create an rfq"}]

            status_counts = {}
            for r in rfqs:
                st = str(r.status or "DRAFT").upper()
                status_counts[st] = status_counts.get(st, 0) + 1

            completed_legs = [l for l in legs if str(l.status).upper() in ["COMPLETED", "ACCEPTED"]]
            vol_by_curr = {}
            for l in completed_legs:
                c = l.buy_currency or "USD"
                vol_by_curr[c] = vol_by_curr.get(c, 0.0) + float(l.amount or 0.0)

            curr_lines = [f"- **{c}**: {amt:,.2f}" for c, amt in vol_by_curr.items()]

            lines = [
                f"📊 **Quotation & RFQ Portfolio Summary ({len(rfqs)} Total RFQs)**:\n",
                f"- **Active / In Bidding**: {status_counts.get('OPEN', 0)} round(s)",
                f"- **Evaluating / Acceptance Window**: {status_counts.get('EVALUATING', 0)} round(s)",
                f"- **Executed & Completed**: {status_counts.get('COMPLETED', 0) + status_counts.get('ACCEPTED', 0)} round(s) ({len(completed_legs)} executed trade legs)",
                f"- **Pending Internal Approval**: {status_counts.get('PENDING_APPROVAL', 0)} round(s)",
                f"- **Rejected / Cancelled**: {status_counts.get('REJECTED', 0) + status_counts.get('CANCELLED', 0)} round(s)\n"
            ]

            if curr_lines:
                lines.append("💰 **Total Executed Volume**:\n" + "\n".join(curr_lines) + "\n")

            lines.append(f"👉 [Open Quotation Control Center]({nav_base}/quotations)")

            suggested_chips = [
                {"label": "🏆 Top winning banks", "query": "who won the maximum number of rfqs"},
                {"label": "📋 Participating banks in last RFQ", "query": "who was the participating banks in the last rfq"},
                {"label": "✍️ Create New RFQ", "query": "how to create an rfq"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_daily_pulse":
            data = query_result or {}
            expiring_14 = data.get("expiring_14") or []
            instructions = data.get("instructions") or []
            pending_issuance = data.get("pending_issuance") or []
            facilities = data.get("facilities") or []

            undelivered = sum(1 for i in instructions if i.delivery_date is None and i.bank_reply_date is None)
            awaiting_reply = sum(1 for i in instructions if i.delivery_date is not None and i.bank_reply_date is None)

            has_urgent = (len(expiring_14) > 0 or awaiting_reply > 0 or undelivered > 0 or len(pending_issuance) > 0)

            lines = ["☀️ **Daily Treasury Pulse & Morning Briefing**:\n"]
            if has_urgent:
                if expiring_14:
                    lines.append(f"⚠️ **{len(expiring_14)} Guarantee(s) Expiring within 14 Days**")
                    for lg in expiring_14[:3]:
                        curr = lg.lg_currency.iso_code if lg.lg_currency else "EGP"
                        exp_str = lg.expiry_date.strftime("%Y-%m-%d") if lg.expiry_date else "N/A"
                        lines.append(f"  - `{lg.lg_number}`: **{lg.lg_amount:,.2f} {curr}** (Expires: *{exp_str}*)")
                    lines.append("")

                if awaiting_reply > 0 or undelivered > 0 or len(pending_issuance) > 0:
                    lines.append("⚡ **Action Items Requiring Attention**:")
                    if awaiting_reply > 0:
                        lines.append(f"  - **{awaiting_reply}** instruction(s) awaiting bank reply")
                    if undelivered > 0:
                        lines.append(f"  - **{undelivered}** physical letter(s) pending bank delivery")
                    if len(pending_issuance) > 0:
                        lines.append(f"  - **{len(pending_issuance)}** issuance request(s) awaiting approval")
                    lines.append("")

                if facilities:
                    lines.append(f"🏛️ **{len(facilities)} Active Bank Credit Facilities Available**")
            else:
                lines.append("🟢 **All Systems Operational & Healthy**:")
                lines.append("- ✅ **0** Guarantees expiring in the next 14 days")
                lines.append("- ✅ **All** bank instructions delivered and replies up to date")
                lines.append("- ✅ **0** Pending approvals blocking the issuance pipeline")
                if facilities:
                    lines.append(f"- 🏛️ **{len(facilities)}** Active bank facilities ready with ample headroom")

            lines.append(f"\n👉 [Open Action Center]({nav_base}/action-center) | [View Portfolio]({nav_base}/lg-records)")

            suggested_chips = [
                {"label": "📅 Expiring in 60 Days", "query": "lgs expiring within 60 days"},
                {"label": "⚡ Action Center", "query": "show action center"},
                {"label": "🏛️ Facility Headroom", "query": "show facility headroom"},
                {"label": "💬 Share Feedback", "query": "i want to give feedback"}
            ]
            return "\n".join(lines).strip(), references, suggested_chips

        if intent == "report_feedback":
            data = query_result or {}
            fb_id = data.get("feedback_id", "N/A")
            fb_type_label = data.get("feedback_type", "FEEDBACK").replace("_", " ").title()
            msg = (data.get("message") or "").strip()
            is_intro = data.get("is_intro", False)

            if is_intro or not msg:
                intro_lines = [
                    "💬 **How to Share Feedback & Request Features**:\n",
                    "Sharing feedback is as simple as chatting with me! Everything you share is automatically logged for your **System Owner and the Grow Engineering Team**.\n",
                    "**Examples of what you can type**:",
                    "- 💡 *Feature request: Add Excel export for all credit facilities*",
                    "- 🐞 *Found an issue: Bank form preview is misaligned on mobile*",
                    "- ⚡ *I find it difficult to record delivery proof for multiple LGs*\n",
                    "💡 *Transparency Notice: Feedback submitted here is securely shared with your System Owner and Grow Engineering to prioritize platform enhancements.*\n",
                    "What would you like to share today?"
                ]
                return "\n".join(intro_lines), references, [
                    {"label": "💡 Suggest a Feature", "query": "Feature request: "},
                    {"label": "🐞 Report an Issue", "query": "Found an issue with: "},
                    {"label": "⚡ System Usability", "query": "I find it difficult to: "}
                ]

            answer_lines = [
                f"✅ **Feedback Received & Logged [Ref: #FB-{fb_id}]**\n",
                f"- **Category**: **{fb_type_label}**",
                f"- **Logged Note**: *{msg}*",
                f"- **Status**: Forwarded to System Inbox for Review\n",
                "💡 *Transparency Notice: Your feedback has been recorded and forwarded directly to the **System Owner and the Grow Engineering Team** for review and prioritization.*\n",
                "Thank you for helping us continuously improve Grow Treasury!"
            ]
            answer = "\n".join(answer_lines)
            suggested_chips = [
                {"label": "☀️ Daily Treasury Pulse", "query": "daily pulse"},
                {"label": "📊 Portfolio Summary", "query": "show portfolio summary"},
                {"label": "💬 Share More Feedback", "query": "i want to give feedback"}
            ]
            return answer, references, suggested_chips

        if intent == "get_action_center_summary":
            instructions = query_result.get("instructions", [])
            pending_issuance = query_result.get("pending_issuance", [])

            undelivered = sum(1 for i in instructions if i.delivery_date is None and i.bank_reply_date is None)
            awaiting_reply = sum(1 for i in instructions if i.delivery_date is not None and i.bank_reply_date is None)

            lines = [
                f"⚡ **Operational Action Center Summary**:\n",
                f"- **Instructions Awaiting Bank Reply**: **{awaiting_reply}** item(s)",
                f"- **Undelivered Physical Instructions**: **{undelivered}** item(s)",
                f"- **Issuance Requests Awaiting Approval**: **{len(pending_issuance)}** item(s)\n",
                f"👉 [Open Action Center]({nav_base}/action-center)"
            ]
            suggested_chips = [
                {"label": "⏳ View Pending Approvals", "query": "show pending approvals"},
                {"label": "📤 View Issuance Pipeline", "query": "show issuance pipeline"},
                {"label": "📅 Expiring in 60 Days", "query": "lgs expiring within 60 days"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_top_beneficiaries":
            data = query_result if isinstance(query_result, dict) else {"custody_records": query_result or [], "outbound_records": [], "inbound_issuers": []}
            lines = ["🏢 **Beneficiary & Counterparty Intelligence (Bi-Module Analysis)**:\n"]

            # 1. Outbound Issuance (External Beneficiaries)
            outbound_raw = data.get("outbound_records") or []
            outbound_map: Dict[str, Dict[str, float]] = {}
            for row in outbound_raw:
                name, curr, amt = row[0], row[1], row[2]
                if not name:
                    continue
                if name not in outbound_map:
                    outbound_map[name] = {}
                outbound_map[name][curr] = outbound_map[name].get(curr, 0.0) + float(amt or 0.0)

            if outbound_map:
                sorted_out = sorted(outbound_map.items(), key=lambda x: sum(x[1].values()), reverse=True)[:5]
                lines.append("📤 **Top External Beneficiaries (LG Issuance)**:")
                for idx, (name, currs) in enumerate(sorted_out, 1):
                    curr_strs = [f"{amt:,.2f} {c}" for c, amt in currs.items()]
                    lines.append(f"{idx}. **{name}**: {', '.join(curr_strs)}")
                lines.append("")

            # 2. Inbound Custody (Internal Subsidiaries)
            custody_recs = data.get("custody_records") or []
            custody_map: Dict[str, Dict[str, float]] = {}
            for r in custody_recs:
                b_name = r.beneficiary_corporate.entity_name if (r.beneficiary_corporate and hasattr(r.beneficiary_corporate, "entity_name") and r.beneficiary_corporate.entity_name) else "Unassigned Entity"
                curr = r.lg_currency.iso_code if (r.lg_currency and r.lg_currency.iso_code) else "EGP"
                amt = float(r.lg_amount or 0.0)
                if b_name not in custody_map:
                    custody_map[b_name] = {}
                custody_map[b_name][curr] = custody_map[b_name].get(curr, 0.0) + amt

            if custody_map:
                sorted_cust = sorted(custody_map.items(), key=lambda x: sum(x[1].values()), reverse=True)[:5]
                lines.append("🛡️ **Internal Subsidiary Allocation (LG Custody)**:")
                for idx, (name, currs) in enumerate(sorted_cust, 1):
                    curr_strs = [f"{amt:,.2f} {c}" for c, amt in currs.items()]
                    lines.append(f"{idx}. **{name}**: {', '.join(curr_strs)}")
                lines.append("")

            # 3. Inbound Issuers (Contractors / Applicants)
            issuers_raw = data.get("inbound_issuers") or []
            issuers_map: Dict[str, Dict[str, float]] = {}
            for row in issuers_raw:
                name, curr, amt = row[0], row[1], row[2]
                if not name:
                    continue
                if name not in issuers_map:
                    issuers_map[name] = {}
                issuers_map[name][curr] = issuers_map[name].get(curr, 0.0) + float(amt or 0.0)

            if issuers_map:
                sorted_iss = sorted(issuers_map.items(), key=lambda x: sum(x[1].values()), reverse=True)[:3]
                lines.append("🤝 **Top Issuing Contractors (Applicants in Custody)**:")
                for idx, (name, currs) in enumerate(sorted_iss, 1):
                    curr_strs = [f"{amt:,.2f} {c}" for c, amt in currs.items()]
                    lines.append(f"{idx}. **{name}**: {', '.join(curr_strs)}")
                lines.append("")

            lines.append(f"👉 [View All Entities]({nav_base}/entities) | [View Issuance Pipeline]({nav_base}/issuance/requests)")

            suggested_chips = [
                {"label": "🏢 Outbound Beneficiaries", "query": "show outbound beneficiaries"},
                {"label": "🤝 Top Inbound Issuers", "query": "show top contractors"},
                {"label": "🏛️ Subsidiary Distribution", "query": "show subsidiary distribution"},
                {"label": "🏦 Bank Exposure", "query": "bank exposure"}
            ]
            return "\n".join(lines).strip(), references, suggested_chips

        if intent == "get_top_issuers":
            raw_issuers = query_result or []
            if not raw_issuers:
                return "No counterparty applicant/issuer data found in custody guarantees.", [], []

            issuers_map: Dict[str, Dict[str, float]] = {}
            for row in raw_issuers:
                name, curr, amt = row[0], row[1], row[2]
                if not name:
                    continue
                if name not in issuers_map:
                    issuers_map[name] = {}
                issuers_map[name][curr] = issuers_map[name].get(curr, 0.0) + float(amt or 0.0)

            sorted_iss = sorted(issuers_map.items(), key=lambda x: sum(x[1].values()), reverse=True)[:5]
            lines = ["🤝 **Top Issuing Contractors / Applicants (LG Custody)**:\n"]
            for idx, (name, currs) in enumerate(sorted_iss, 1):
                curr_strs = [f"{amt:,.2f} {c}" for c, amt in currs.items()]
                lines.append(f"{idx}. **{name}**: {', '.join(curr_strs)}")

            lines.append(f"\n👉 [Open Custody Vault]({nav_base}/lg-records)")
            suggested_chips = [
                {"label": "🏢 Outbound Beneficiaries", "query": "show outbound beneficiaries"},
                {"label": "🏛️ Subsidiary Distribution", "query": "show subsidiary distribution"},
                {"label": "🏦 Bank Exposure", "query": "bank exposure"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_entity_distribution":
            records = query_result or []
            if not records:
                return "No active guarantee records found across subsidiaries.", [], []

            entity_map: Dict[str, Dict[str, float]] = {}
            for r in records:
                b_name = r.beneficiary_corporate.entity_name if (r.beneficiary_corporate and hasattr(r.beneficiary_corporate, "entity_name") and r.beneficiary_corporate.entity_name) else "Unassigned Entity"
                curr = r.lg_currency.iso_code if (r.lg_currency and r.lg_currency.iso_code) else "EGP"
                amt = float(r.lg_amount or 0.0)
                if b_name not in entity_map:
                    entity_map[b_name] = {}
                entity_map[b_name][curr] = entity_map[b_name].get(curr, 0.0) + amt

            lines = ["🏛️ **Guarantee Allocation by Internal Subsidiary / Entity**:\n"]
            for idx, (name, currs) in enumerate(entity_map.items(), 1):
                curr_strs = [f"{amt:,.2f} {c}" for c, amt in currs.items()]
                lines.append(f"{idx}. **{name}**: {', '.join(curr_strs)}")

            lines.append(f"\n👉 [Manage Corporate Entities]({nav_base}/entities)")
            suggested_chips = [
                {"label": "🤝 Top Inbound Issuers", "query": "show top contractors"},
                {"label": "🏢 Outbound Beneficiaries", "query": "show outbound beneficiaries"},
                {"label": "📊 Unified Portfolio Overview", "query": "show portfolio summary"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_bank_exposure":
            records = query_result or []
            if not records:
                return "No active guarantee records found to compute bank exposure.", [], []

            bank_map: Dict[str, Dict[str, float]] = {}
            for r in records:
                b_name = r.issuing_bank.name if r.issuing_bank else "Unknown Bank"
                c_code = r.lg_currency.iso_code if r.lg_currency else "EGP"
                if b_name not in bank_map:
                    bank_map[b_name] = {}
                bank_map[b_name][c_code] = bank_map[b_name].get(c_code, 0.0) + float(r.lg_amount or 0.0)

            sorted_banks = sorted(bank_map.items(), key=lambda item: sum(item[1].values()), reverse=True)[:5]

            lines = ["🏦 **Top 5 Issuing Banks by Exposure Concentration**:\n"]
            for idx, (b_name, curr_dict) in enumerate(sorted_banks, 1):
                amt_str = ", ".join([f"{amt:,.2f} {c}" for c, amt in curr_dict.items()])
                lines.append(f"{idx}. **{b_name}**: {amt_str}")

            lines.append(f"\n👉 [View All Issuing Banks]({nav_base}/banks)")
            suggested_chips = [
                {"label": "🏢 Top Beneficiaries", "query": "show top beneficiaries"},
                {"label": "🏛️ Bank Facility Headroom", "query": "show facility headroom"},
                {"label": "📊 Unified Portfolio Overview", "query": "show portfolio overview"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "search_lgs":
            records = query_result or []
            if not records:
                return "No records matching your search criteria.", [], [
                    {"label": "📊 View All Active LGs", "query": "show active LGs"},
                    {"label": "📤 View Issuance Pipeline", "query": "show issuance pipeline"}
                ]

            if self._current_query_params.get("sort_by") == "amount_desc":
                lines = [f"🏆 **Highest Value Guarantees in Portfolio (Ranked by Amount)**:\n"]
                for idx, r in enumerate(records[:10], 1):
                    curr_code = r.lg_currency.iso_code if r.lg_currency else "EGP"
                    bank_name = r.issuing_bank.name if r.issuing_bank else "N/A"
                    st_name = r.lg_status.name if (r.lg_status and hasattr(r.lg_status, 'name')) else "Valid"
                    amt_str = f"{float(r.lg_amount):,.2f}" if r.lg_amount else "0.00"
                    lines.append(f"{idx}. **{r.lg_number}**: **{amt_str} {curr_code}** (Bank: *{bank_name}*, Status: *{st_name}*)")
                    references.append({
                        "lg_id": r.id,
                        "lg_number": r.lg_number,
                        "expiry_date": r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else None,
                        "amount": float(r.lg_amount) if r.lg_amount else 0.0,
                        "currency": curr_code
                    })
                lines.append(f"\n👉 [Open Custody Vault]({nav_base}/lg-records)")
                suggested_chips = [
                    {"label": "📊 Portfolio Summary", "query": "show portfolio summary"},
                    {"label": "🏢 Top Beneficiaries", "query": "show top beneficiaries"},
                    {"label": "🏦 Bank Exposure", "query": "bank exposure"}
                ]
                return "\n".join(lines), references, suggested_chips

            lines = [f"Found **{len(records)} record(s)** matching your query:\n"]
            for r in records[:15]:
                curr_code = r.lg_currency.iso_code if r.lg_currency else "EGP"
                st_name = r.lg_status.name if r.lg_status else "Valid"
                exp_str = r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else "N/A"
                amt_str = f"{float(r.lg_amount):,.2f}" if r.lg_amount else "0.00"
                lines.append(f"- **{r.lg_number}**: {amt_str} {curr_code} (Status: *{st_name}*, Expiry: *{exp_str}*)")
                references.append({
                    "lg_id": r.id,
                    "lg_number": r.lg_number,
                    "expiry_date": exp_str,
                    "amount": float(r.lg_amount) if r.lg_amount else 0.0,
                    "currency": curr_code
                })
            suggested_chips = [
                {"label": "📅 Expiring in 60 Days", "query": "lgs expiring within 60 days"},
                {"label": "🏦 Bank Exposure", "query": "show bank exposure"},
                {"label": "🏢 Top Beneficiaries", "query": "show top beneficiaries"}
            ]
            return "\n".join(lines), references, suggested_chips

        if intent == "get_lg_details":
            r = query_result
            if not r:
                return "Guarantee record not found.", [], []
            self._last_referenced_lg = {"lg_id": r.id, "lg_number": r.lg_number}

            curr_code = r.lg_currency.iso_code if r.lg_currency else "EGP"
            b_name = r.issuing_bank.name if r.issuing_bank else "N/A"
            ben_name = r.beneficiary_corporate.entity_name if r.beneficiary_corporate else (r.beneficiary or "N/A")
            exp_str = r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else "N/A"
            amt_str = f"{float(r.lg_amount):,.2f}" if r.lg_amount else "0.00"

            answer = (
                f"📄 **Guarantee Details: {r.lg_number}**\n\n"
                f"- **Amount**: **{amt_str} {curr_code}**\n"
                f"- **Status**: **{r.lg_status.name if r.lg_status else 'Valid'}**\n"
                f"- **Issuing Bank**: {b_name}\n"
                f"- **Beneficiary**: {ben_name}\n"
                f"- **Expiry Date**: {exp_str}\n"
                f"- **Purpose**: {r.description_purpose or 'General Commercial Guarantee'}\n\n"
                f"👉 [Open Guarantee Record in Custody]({nav_base}/lg-records)"
            )
            references.append({
                "lg_id": r.id,
                "lg_number": r.lg_number,
                "expiry_date": exp_str,
                "amount": float(r.lg_amount) if r.lg_amount else 0.0,
                "currency": curr_code
            })
            suggested_chips = [
                {"label": "📅 Check Expiry Date", "query": f"when does {r.lg_number} expire"},
                {"label": "🏢 Who is the beneficiary?", "query": f"who is the beneficiary of {r.lg_number}"},
                {"label": "📊 Portfolio Overview", "query": "show portfolio overview"}
            ]
            return answer, references, suggested_chips

        if intent == "get_pending_approvals":
            requests = query_result or []
            if not requests:
                return "You have **0 pending approvals** in your queue.", [], [
                    {"label": "📤 View Issuance Pipeline", "query": "show issuance pipeline"},
                    {"label": "🛡️ View Custody LGs", "query": "show custody LGs"}
                ]
            lines = [f"⏳ **Pending Approvals ({len(requests)} Request(s))**:\n"]
            for req in requests[:10]:
                lines.append(f"- **{req.serial_number or 'REQ'}**: {float(req.amount or 0.0):,.2f} (Beneficiary: *{req.beneficiary_name or 'N/A'}*, Status: *{req.status}*)")
            lines.append(f"\n👉 [Review Approvals in Action Center]({nav_base}/action-center)")
            suggested_chips = [
                {"label": "⚡ Action Center", "query": "show action center summary"},
                {"label": "📤 Issuance Pipeline", "query": "show issuance pipeline"}
            ]
            return "\n".join(lines), references, suggested_chips

        return str(query_result), references, []

    def handle_system_help(self, user_question: str, user_role: str, user_email: str) -> str:

        q_lower = user_question.lower()
        role_label = user_role.replace("_", " ").title()
        is_admin = user_role.upper() in ["CORPORATE_ADMIN", "SYSTEM_OWNER"]
        role_note = (
            f"\n\n✅ **Role Access**: As **{role_label}**, you have full administrative permissions to modify organization settings."
            if is_admin else
            f"\n\n🔒 **Permission Note**: You are currently logged in as **{role_label}**. Only a **Corporate Admin** can modify organization settings (`/corporate-admin/module-configs`). Please refer this change to your Corporate Admin."
        )
        role_upper = str(user_role).upper() if user_role else "END_USER"
        nav_base = "/corporate-admin" if role_upper in ["CORPORATE_ADMIN", "ADMIN"] else ("/system-owner" if role_upper == "SYSTEM_OWNER" else "/end-user")

        # 0. Greeting / Fast Intro
        if any(w in q_lower for w in ["hi", "hello", "hey", "who are you", "start", "capabilities"]):
            return (
                f"Hello! I am your **Grow Treasury & System AI Assistant**.\n\n"
                f"Logged in as: **{user_email}** (*{role_label}*)\n\n"
                f"I am fully aware of your profile, active subscription plan, and permissions across both **LG Custody (Inbound)** and **LG Issuance (Outbound)**.\n\n"
                f"You can ask me about:\n"
                f"- **Unified Portfolio**: *\"How many LGs do we have?\"*, *\"What is our USD exposure?\"*\n"
                f"- **LG Custody**: *\"Show LGs expiring in August\"*, *\"Top beneficiaries\"*, *\"Bank exposure\"*\n"
                f"- **LG Issuance**: *\"Show issuance pipeline\"*, *\"What is our facility headroom?\"*\n"
                f"- **Operational Action Center**: *\"Show instructions awaiting bank reply\"*\n"
                f"- **Step-by-Step Guides**: *\"How do I record a new LG?\"*, *\"How do I extend an LG?\"*\n"
                f"- **Treasury Concepts**: *\"What is cash pooling?\"*, *\"How do forward contracts work?\"*"
            )

                # 0.4a Quotation Acceptance Window & Timeout Policies Guidance
        if any(w in q_lower for w in [
            "acceptance window", "acceptance timeout", "auto_accept", "auto_reject",
            "auto accept", "auto reject", "acceptance policy"
        ]):
            return (
                f"**How the Corporate Acceptance Window & Timeout Policies Work**:\n\n"
                f"When a quotation bidding window closes, Corporate Treasury enters a dedicated **Acceptance Window** (e.g. 30 seconds to 2 minutes) to review firm quotes and execute the trade:\n\n"
                f"1. **Live Synchronized Countdowns**:\n"
                f"   - **Corporate Admin Cockpit** (`{nav_base}/quotations`): Shows a prominent countdown timer with instant **Accept Winner** and **Decline Deal** buttons.\n"
                f"   - **Bank Bidding Portal**: Displays a live `Selection in Progress [⏱ XXs remaining]` badge so bank dealers know exactly how long their firm quotes must be held.\n\n"
                f"2. **Timeout Policies (Configured per RFQ)**:\n"
                f"   - **AUTO_ACCEPT**: If the timer hits zero without manual action, the engine **automatically awards** the deal to the lowest/best valid rate, generates the deal slip, and emails trade execution tickets to the winning desk.\n"
                f"   - **AUTO_REJECT**: If the timer hits zero without confirmation, the RFQ transitions to `REJECTED`, all quotes are preserved for internal review, and a neutral closure notification is sent to quoting banks without executing any trade.\n\n"
                f"3. **Manual Decision Overrides**:\n"
                f"   - Corporate Admin can manually accept the winner or decline the deal at any second during the active window.\n\n"
                f"👉 [Review Quotation Requests]({nav_base}/quotations)"
            )

        # 0.4b Quotation Tolerance Limits & Central Bank Benchmarks
        if any(w in q_lower for w in [
            "tolerance limit", "tolerance percent", "cbe benchmark", "inconclusive", "max tolerance"
        ]):
            return (
                f"**How Tolerance Limits & Central Bank (CBE) Benchmarks Work**:\n\n"
                f"To protect corporate treasury from executing trades at off-market spreads, the system enforces **Tolerance Limit Controls**:\n\n"
                f"1. **Benchmark Reference**:\n"
                f"   - The RFQ compares incoming quotes against the Central Bank (CBE) benchmark rate or indicative reference pricing.\n\n"
                f"2. **Tolerance Threshold (`max_tolerance_percent`)**:\n"
                f"   - If the best submitted execution rate deviates from the benchmark by more than the allowed tolerance percentage (e.g. > 0.5%), the trade is automatically flagged as **INCONCLUSIVE**.\n\n"
                f"3. **Execution Safeguard**:\n"
                f"   - Inconclusive deals cannot be accidentally auto-accepted, protecting the company from adverse market shifts.\n\n"
                f"👉 [Open Quotation Control Center]({nav_base}/quotations)"
            )

        # 0.4c Bank Dealer Roles & Takeover Mechanism
        if any(w in q_lower for w in [
            "bank role", "bank dealer role", "approver role", "execution dealer", "trader takeover", "bank roles"
        ]):
            return (
                f"**Bank Counterparty Roles & Quoting Portal Architecture**:\n\n"
                f"Each invited banking institution operates with structured dealer roles configured under `{nav_base}/settings` (Bank Counterparties):\n\n"
                f"1. **APPROVER (Internal Bank Authorization)**:\n"
                f"   - An authorized bank signatory who must approve participation before the trading desk can bid. If declined, the bank's portal locks with `PARTICIPATION_DECLINED`.\n\n"
                f"2. **EXECUTION (Active Quoting Trader)**:\n"
                f"   - Desk trader authorized to enter firm buy/sell rates and submit quotes during the bidding window.\n\n"
                f"3. **VIEW_ONLY (Spectator / Risk Monitor)**:\n"
                f"   - Read-only observer who can monitor window timings and RFQ specifications without quoting capability.\n\n"
                f"4. **Trader Takeover Mechanism**:\n"
                f"   - If another execution dealer from the same bank opens the link, they can seamlessly assume active quoting with one click, gracefully transitioning the previous trader to spectator mode.\n\n"
                f"5. **Zero-Password Authentication**:\n"
                f"   - Counterparties authenticate securely via 6-Digit OTP or 1-Click Magic Link with full audit logging.\n\n"
                f"👉 [Manage Bank Counterparties]({nav_base}/settings)"
            )

        # 0.4d Multi-Leg Baskets & Additional Cost Matrix
        if any(w in q_lower for w in [
            "multi leg", "multi-leg", "currency basket", "partial award", "additional cost", "final adjusted price"
        ]):
            return (
                f"**Multi-Leg Baskets & Final Adjusted Price Matrix**:\n\n"
                f"Grow supports complex multi-currency RFQ packages and true all-in pricing economics:\n\n"
                f"1. **Multi-Leg Baskets**:\n"
                f"   - Bundle multiple currency pairs (e.g. EUR/EGP + USD/EGP) into a single RFQ.\n"
                f"   - Banks submit quotes across all legs simultaneously.\n"
                f"   - Corporate Treasury can award individual legs to different banks or award the entire basket.\n\n"
                f"2. **Additional Cost Matrix (Spreads & Bank Fees)**:\n"
                f"   - Configure bank-specific fees: Percentage (%), Minimum fee, Maximum cap, and Flat fee.\n"
                f"   - The system automatically calculates the **Final Adjusted Price** = Quoted Rate + Additional Costs.\n"
                f"   - Winners are determined by the best Final Adjusted Price to reflect the true net economic cost.\n\n"
                f"👉 [Open Quotation Control Center]({nav_base}/quotations)"
            )

        # Specific: How to Create an RFQ / Request a Quote
        if any(w in q_lower for w in [
            "how to create an rfq", "how do i create an rfq", "how can i create an rfq",
            "create an rfq", "new rfq", "request a quote", "how to request a quote",
            "how do i request a quote", "how to submit an rfq", "create rfq"
        ]):
            return (
                f"**Step-by-Step: Creating a Quotation Request (RFQ) as {role_label}**:\n\n"
                f"1. **Navigate to the Quotation Center**:\n"
                f"   - Go to **Sidebar ➔ Quotations ➔ Quotation Requests & History** (`{nav_base}/quotations`).\n\n"
                f"2. **Initiate the Request**:\n"
                f"   - Click the **`+ New Quotation Request`** button at the top of the dashboard.\n\n"
                f"3. **Select Instrument & Specify Terms**:\n"
                f"   - **FX Spot**: Choose Currency Pair (e.g. `USD/EGP`), Direction (`Buy` or `Sell`), Amount, and Settlement Date.\n"
                f"   - **FX Forward**: Set future value date, tenor, and indicative evaluation rate.\n"
                f"   - **Treasury Bills (T-Bills)**: Specify issue amount, tenor (91/182/273/364 days), and settlement window.\n"
                f"   - *Multi-Leg Option*: Click **Add Currency Leg** to bundle multiple pairs into a single RFQ basket.\n\n"
                f"4. **Configure Quoting Window & Counterparties**:\n"
                f"   - Set the bidding window start and end times (e.g., 30-minute bidding round).\n"
                f"   - Select which relationship banks to invite from your approved counterparty roster.\n\n"
                f"5. **Submit & Broadcast**:\n"
                f"   - Click **Submit RFQ**. If `QUOTATION_APPROVAL_REQUIRED` is active, it routes to Corporate Admin first; otherwise, invited bank dealers immediately receive secure quoting invitations.\n\n"
                f"👉 [Open Quotation Workspace]({nav_base}/quotations)"
            )

        # 0.5 FX & T-Bills Quotation Module Guidance
        if any(w in q_lower for w in [
            "quotation", "quotations", "rfq", "rfqs", "fx quote", "fx quotation",
            "t-bill quote", "tbill quote", "t-bill quotation", "treasury bill quote",
            "quotation module", "request quotation", "request quote"
        ]):
            if is_admin:
                return (
                    f"**How the FX & T-Bills Quotation Module Works (Corporate Admin Guide)**:\n\n"
                    f"The Quotation Module provides end-to-end competitive rate discovery, multi-bank digital RFQs (Request for Quote), and automated trade execution for **Foreign Exchange (Spot / Forward)** and **Treasury Bills (T-Bills)**:\n\n"
                    f"1. **Centralized Quotation Control Center** (`{nav_base}/quotations`):\n"
                    f"   - View all active, pending, awarded, and expired RFQ rounds across all internal entities.\n"
                    f"   - Monitor live bank dealer submissions in a real-time side-by-side comparison matrix with **Final Adjusted Prices** (accounting for bank spreads and additional fees).\n"
                    f"   - Compare competitive rates, bid/ask spreads, and T-Bill yield curves after 20% withholding tax.\n\n"
                    f"2. **Pre-Broadcast Governance & Approval** (`QUOTATION_APPROVAL_REQUIRED`):\n"
                    f"   - Located under **Sidebar ➔ Configuration ➔ Settings** (`{nav_base}/module-configs` Group 4).\n"
                    f"   - When enabled (`true`), RFQs submitted by End Users are routed to Corporate Admin for verification before being broadcast to bank dealers.\n\n"
                    f"3. **Acceptance Window & Execution Policies**:\n"
                    f"   - A synchronized decision window (e.g. 30s to 120s) activates immediately upon bidding closure.\n"
                    f"   - Supports **AUTO_ACCEPT** (auto-awards best quote upon timeout) or **AUTO_REJECT** (auto-declines trade on timeout while preserving quotes for audit).\n\n"
                    f"4. **Awarding Deals & Trade Settlement**:\n"
                    f"   - Select winning counterparties per currency pair or award multi-leg packages.\n"
                    f"   - Automatically generates an official **Dual-Branded Deal Confirmation Receipt (PDF)** with execution reference, digital signature stamp, and SHA-256 seal.\n"
                    f"   - Dispatches execution tickets to winners and neutral outcome notifications to unselected counterparties.\n\n"
                    f"5. **Cryptographic WORM Audit Trail**:\n"
                    f"   - Complete audit trail tracking portal access, IP addresses, quoting timestamps, and trade executions, all permanently sealed in the sequential SHA-256 WORM audit chain.\n\n"
                    f"👉 [Open Quotation Control Center]({nav_base}/quotations)"
                )
            else:
                return (
                    f"**How the FX & T-Bills Quotation Module Works (End User Guide)**:\n\n"
                    f"The Quotation Module allows you to broadcast competitive digital RFQs (Request for Quote) to your relationship banks for **Foreign Exchange (Spot / Forward)** and **Treasury Bills (T-Bills)**:\n\n"
                    f"1. **Initiate an RFQ** (`{nav_base}/quotations`):\n"
                    f"   - Navigate to **Sidebar ➔ Quotations ➔ Quotation Requests & History** (`{nav_base}/quotations`).\n"
                    f"   - Click **New Quotation Request (Create RFQ)**.\n"
                    f"   - Select Instrument Type: **FX Spot**, **FX Forward**, or **Treasury Bills (T-Bills)**.\n"
                    f"   - Specify Currency Pair, Buy/Sell Direction, Amount, and Settlement/Value Dates.\n"
                    f"   - Bundle multiple pairs into a **Multi-Leg Basket** if required.\n"
                    f"   - Select relationship banks and submit for approval or release.\n\n"
                    f"2. **Bank Dealer Quoting**:\n"
                    f"   - Invited bank dealers receive a secure, tokenized public portal link (`/public/quotations/:token`) to submit live executable rates without needing system passwords.\n\n"
                    f"3. **Real-Time Rate Tracking & Awarding**:\n"
                    f"   - Watch incoming bank bids in real-time as dealers submit live rates and spreads on the Results View.\n"
                    f"   - Review competing quotes and route the best offer for awarding or corporate acceptance.\n"
                    f"   - Once awarded, official **Deal Confirmation Receipts (PDF)** are generated immediately.\n\n"
                    f"👉 [Open Quotations Workspace]({nav_base}/quotations)"
                )

        # 0.6 Bank Reconciliation Module Guidance
        if any(w in q_lower for w in [
            "bank reconciliation", "reconciliation module", "reconcile bank",
            "reconciliation rules", "position reconciliation", "accounting export"
        ]):
            return (
                f"**How the Bank Reconciliation Module Works**:\n\n"
                f"Grow's Bank Reconciliation Engine automates the matching of bank statement transactions against internal treasury records:\n\n"
                f"1. **Bank Statement Ingestion** (`{nav_base}/reconciliation`):\n"
                f"   - Upload MT940, CAMT.053, or CSV electronic bank statements.\n\n"
                f"2. **Automated Matching & Rules Engine** (`{nav_base}/reconciliation/rules`):\n"
                f"   - Auto-matches transactions based on reference numbers, amounts, and value dates.\n"
                f"   - Applies custom reconciliation rules for recurring fees, interest, and commission items.\n\n"
                f"3. **Position Reconciliation** (`/corporate-admin/issuance/reconciliation`):\n"
                f"   - Reconciles active LG facility limits and outstanding guarantee liability balances directly against bank facility certificates.\n\n"
                f"4. **Accounting Export** (`{nav_base}/reconciliation/export`):\n"
                f"   - Generates ERP-ready journal entries (SAP, Oracle, Microsoft Dynamics) for matched transactions.\n\n"
                f"👉 [Open Bank Reconciliation Workspace]({nav_base}/reconciliation)"
            )

        # 0.7 LG Custody vs LG Issuance Module Workflows
        if any(w in q_lower for w in ["how does custody work", "how does lg custody work", "custody module", "explain custody"]):
            return (
                f"**How the LG Custody (Inbound) Module Works**:\n\n"
                f"LG Custody manages guarantees received by your company (where your company is the Beneficiary or holding incoming guarantees):\n\n"
                f"1. **Intake & OCR Ingestion** (`{nav_base}/lg-records/new`):\n"
                f"   - Upload digital scans or paper copies. AI OCR auto-extracts guarantee number, issuing bank, amount, currency, and expiry.\n\n"
                f"2. **Centralized Vault & Expiry Watch** (`{nav_base}/lg-records`):\n"
                f"   - Comprehensive search, filtering, and maturity tracking across all business entities.\n\n"
                f"3. **Operational Lifecycle Maintenance** (Actions Menu):\n"
                f"   - Execute **Extend**, **Release**, **Liquidate (Claim)**, **Decrease**, and **Amend** operations with automated bank instruction letters.\n\n"
                f"4. **Action Center & Reminders** (`{nav_base}/action-center`):\n"
                f"   - Tracks pending bank replies, undelivered letters, and automated bank reminder cadence.\n\n"
                f"👉 [Open LG Custody Records]({nav_base}/lg-records)"
            )

        if any(w in q_lower for w in ["how does issuance work", "how does lg issuance work", "issuance module", "explain issuance"]):
            return (
                f"**How the LG Issuance (Outbound) Module Works**:\n\n"
                f"LG Issuance manages guarantees issued on behalf of your company to external beneficiaries against your active credit facilities:\n\n"
                f"1. **Issuance Request Wizard** (`{nav_base}/issuance/requests/new`):\n"
                f"   - 3-step structured request wizard specifying applicant entity, LG type, amount, currency, and beneficiary.\n\n"
                f"2. **Smart Bank Facility Recommendation** (`/corporate-admin/issuance/facilities`):\n"
                f"   - Evaluates bank headroom, pricing, collateral margin, and SLA turnaround to recommend optimal bank facilities.\n\n"
                f"3. **Multi-Level Approval Matrix** (`/corporate-admin/approval-requests`):\n"
                f"   - Routes requests through designated corporate approvers (CFO, Treasury Manager) based on configured authorization thresholds.\n\n"
                f"4. **Bank Application Form Automation** (`/corporate-admin/issuance/issued-lgs`):\n"
                f"   - Automatically populates bank-specific application forms (fillable PDF or physical print overlay) for immediate bank dispatch.\n\n"
                f"👉 [Open LG Issuance Portal]({nav_base}/issuance/requests)"
            )

# 1. Maker-Checker Removal / Liquidation Dual Control (Custody Truth)
        if ("maker checker" in q_lower or "maker-checker" in q_lower or "dual control" in q_lower) and any(w in q_lower for w in [
            "remove", "disable", "liquidation", "liquidate", "turn off", "bypass", "skip", "without", "can i", "stop"
        ]):
            return (
                f"**Maker-Checker Policy for Liquidation & Custody Actions**:\n\n"
                f"In Grow, **Liquidation** is an Inbound Custody maintenance action (claiming an LG held from a contractor/counterparty).\n\n"
                f"Maker-Checker dual control is an **optional setting provisioned at the organization level** that applies across custody actions. **You cannot disable or bypass it for liquidation specifically by yourself**.\n\n"
                f"If Maker-Checker is active for your organization, the only way to stop or adjust dual control is through an **official request from your organization's authorized signatory to Grow Business Development (BD) / System Owner**.\n\n"
                f"👉 [View Organization Profile & Plan]({nav_base}/dashboard)"
            )

        # 2. What happens if I did not add the approval matrix for issued LGs?
        if any(w in q_lower for w in [
            "did not add the approval matrix", "no approval matrix", "without approval matrix",
            "approval matrix for issued", "if i don't add approval matrix", "if no approval matrix",
            "didn't add the approval matrix", "not add the approval matrix"
        ]):
            return (
                f"**What Happens If No Approval Matrix Is Configured for Issued LGs**:\n\n"
                f"If an organization submits an LG Issuance Request without an active **Approval Matrix** (`/corporate-admin/approval-requests`):\n\n"
                f"1. **No System Crash / Block**: The request is created successfully and is not rejected.\n"
                f"2. **Fallback to Single-Tier Review**: The system routes the request via the legacy single-tier path (assigned to the designated checker or defaulting to the Corporate Admin inbox).\n"
                f"3. **Unassigned Approval Inbox**: Any authorized Checker or Corporate Admin can open the request in the Approval Center and approve/reject it directly.\n"
                f"4. **Recommendation**: Configuring an Approval Matrix is recommended if your organization requires multi-level thresholds (e.g., Level 1 CFO approval above $100k).\n\n"
                f"👉 [Configure Approval Matrix]({nav_base}/approval-requests)"
            )

        # 3. What does "Generate Invite" button do?
        if any(w in q_lower for w in ["generate invite", "invite button", "invite link", "what does generate invite"]):
            return (
                f"**What the 'Generate Invite' Button Does**:\n\n"
                f"In **LG Issuance Requests** (`{nav_base}/issuance/requests`), the **Generate Invite** button creates a secure, time-limited **Public Requestor Portal Link** for external project teams, procurement officers, or department staff who do not have full Grow user accounts.\n\n"
                f"**How it works**:\n"
                f"1. You enter the external requestor's work email.\n"
                f"2. The system generates a tokenized single-use link.\n"
                f"3. The external requestor accesses the streamlined form, verifies via email OTP, and submits their LG request directly into your organization's approval inbox without needing a paid license or system password.\n\n"
                f"👉 [Open Issuance Requests Inbox]({nav_base}/issuance/requests)"
            )

        # 4. What happens if I change reminder days to 0? (Disambiguated & Platform Rules)
        if any(w in q_lower for w in ["reminder days to 0", "change reminder days to 0", "reminder days 0", "reminder to 0"]):
            return (
                f"**Setting Reminder Timers to 0 & Rule Cancellation Impact**:\n\n"
                f"Grow platform features several reminder and grace period timers under **Settings ➔ Group 1**. The effect of setting a value to `0` depends on the specific parameter:\n\n"
                f"1. **Bank Follow-Up Reminders (`REMINDER_TO_BANKS_DAYS_SINCE_ISSUANCE`)**:\n"
                f"   - **Minimum Allowed**: `0` days.\n"
                f"   - **Impact**: Removes the cooldown buffer completely. On the exact same day that an instruction is issued (Day 0), users can immediately generate and print formal bank reminder letters.\n\n"
                f"2. **Instruction Cancellation Window (`MAX_DAYS_FOR_LAST_INSTRUCTION_CANCELLATION`)**:\n"
                f"   - **Impact**: Setting this to `0` **cancels the rule entirely** — meaning instruction cancellation is completely disabled, and users cannot withdraw unexecuted instructions.\n\n"
                f"3. **Print Escalation Timers (`DAYS_FOR_FIRST_PRINT_REMINDER`, `DAYS_FOR_PRINT_ESCALATION`)**:\n"
                f"   - **Impact**: Setting to `0` triggers immediate escalation on the day of approval.\n\n"
                f"⚠️ *Platform Rule*: You cannot set any timer lower than the system-enforced minimum threshold.{role_note}\n\n"
                f"👉 [Review Reminder Timers in Settings]({nav_base}/module-configs)"
            )

        # 5. Sending Particular LG Data to Someone Specific (Accurate UI Grounding)
        if any(w in q_lower for w in ["send", "share", "email", "export", "forward", "assign"]) and any(w in q_lower for w in [
            "lg data", "particular lg", "specific lg", "to someone", "to a person", "to colleague", "to specific", "data to someone"
        ]):
            return (
                f"**Sharing Particular Letter of Guarantee (LG) Data**:\n\n"
                f"You have 2 direct methods in Grow to route or share specific LG data with colleagues:\n\n"
                f"1. **Reassign Internal Custodian / Contact** (Automated Alerts):\n"
                f"   - Open **Sidebar ➔ LG Custody ➔ All LG Records** (`{nav_base}/lg-records`).\n"
                f"   - Click **Actions (3 dots)** next to the guarantee and select **Change LG Owner**.\n"
                f"   - Assign the designated colleague so all operational reminders, maturity alerts, and task notifications are routed directly to them.\n\n"
                f"2. **Filtered CSV Export** (Direct File Sharing):\n"
                f"   - In the LG Vault table, filter for the specific LG(s) by Bank, Beneficiary, or Status, then click **Export CSV** to generate and share the spreadsheet.\n\n"
                f"👉 [Open LG Custody Vault]({nav_base}/lg-records)"
            )

        # 6. Manager Always Copied (CC'd) in Emails
        if any(w in q_lower for w in [
            "manager always copied", "copy manager", "manager in the emails", "cc manager",
            "cc email", "manager copied", "common communication list", "always copy my manager"
        ]):
            return (
                f"**Configuring Automated Manager CC / Notification Distribution**:\n\n"
                f"To have your manager automatically copied on system emails:\n\n"
                f"1. Navigate to **Sidebar ➔ Configuration ➔ Settings** (`{nav_base}/module-configs`).\n"
                f"2. Scroll to **Group 4: Operational Governance & Controls**.\n"
                f"3. Locate **Common Communication List (`COMMON_COMMUNICATION_LIST`)**.\n"
                f"4. Add your manager's email address (e.g. `manager@company.com`) to the list.\n"
                f"5. Click **Save Changes**.\n\n"
                f"⚠️ **Important Note**: Enabling this option means that **ALL system actions, bank reminder letters, escalation notices, SLA breach alerts, and maturity warnings generated across the entire organization** will be shared/copied to the entered email address.{role_note}\n\n"
                f"👉 [Configure Common Communication CC List]({nav_base}/module-configs)"
            )

        # 7. Changing Forced Renewal Number of Days
        if any(w in q_lower for w in [
            "forced renewal", "forced renew", "renewal number of days",
            "days before it for renewal", "change renewal days"
        ]):
            return (
                f"**Changing Forced Renewal Days Threshold**:\n\n"
                f"To change the number of days before expiry for forced renewal:\n\n"
                f"1. Navigate to **Sidebar ➔ Configuration ➔ Settings** (`{nav_base}/module-configs`).\n"
                f"2. In **Group 1: Operational Timers, Expiries & Bank Reminder Windows**, locate **`FORCED_RENEW_DAYS_BEFORE_EXPIRY`**.\n"
                f"3. Enter the desired number of days (e.g., `30`, `45`, or `60`).\n"
                f"4. Click **Save Changes** at the bottom of the page.{role_note}\n\n"
                f"👉 [Open Expiry Settings]({nav_base}/module-configs)"
            )

        # 8. Making Delivery Receipt Mandatory
        if any(w in q_lower for w in [
            "delivery receipt mandatory", "make delivery receipt", "doc_mandatory_record_delivery",
            "mandatory delivery receipt", "require delivery proof", "mandatory delivery proof", "mandatory receipt"
        ]):
            return (
                f"**Enforcing Mandatory Delivery Receipts (`DOC_MANDATORY_RECORD_DELIVERY`)**:\n\n"
                f"To require that End Users attach physical delivery proof before recording bank instruction dispatch:\n\n"
                f"1. Navigate to **Sidebar -> Configuration -> Settings** (`{nav_base}/module-configs`).\n"
                f"2. Open **Group 2: Document Compliance & Mandatory Evidence Policies**.\n"
                f"3. Locate **`DOC_MANDATORY_RECORD_DELIVERY`** and toggle it to **`true` (Enabled)**.\n"
                f"4. Click **Save Changes** at the bottom of the page.{role_note}\n\n"
                f"🛡️ *Compliance Rule*: Once enabled, any user recording bank delivery in the Action Center will be strictly blocked from submitting until a signed courier receipt or stamped bank receiving voucher is uploaded.\n\n"
                f"👉 [Open Document Compliance Settings]({nav_base}/module-configs)"
            )

        # 9. Generic Action Guides (Extend, Record, Issue)
        if "extend" in q_lower:
            return (
                f"To extend a Letter of Guarantee (LG) in Grow as **{role_label}**:\n\n"
                f"1. Navigate to **Sidebar -> LG Custody -> All LG Records**.\n"
                f"2. Locate the guarantee using the search bar or filter by bank/beneficiary.\n"
                f"3. Click **Actions (3 dots)** next to the guarantee and select **Request Extension**.\n"
                f"4. Enter the **New Expiry Date** and optional remarks/justification.\n"
                f"5. Click **Submit Extension Instruction** to generate the bank instruction letter.\n\n"
                f"👉 [Click here to open the LG Custody Records page]({nav_base}/lg-records)"
            )

        if any(w in q_lower for w in ["record", "new lg", "add lg"]):
            return (
                f"To record a new Letter of Guarantee (LG) in Grow as **{role_label}**:\n\n"
                f"1. **Navigate to the Record Page**:\n"
                f"   - Go to **Sidebar -> LG Custody -> Record New LG**.\n"
                f"2. **Choose Entry Method**:\n"
                f"   - **AI Document Scan**: Upload a scanned PDF or image. The AI will automatically extract key details.\n"
                f"   - **Manual Entry**: Type details directly into the structured form.\n"
                f"3. **Complete Mandatory Fields**:\n"
                f"   - Fill Beneficiary Entity, Issuing Bank, LG Type, Amount, Currency, and Expiry Date.\n"
                f"4. **Submit Record**:\n"
                f"   - Click **Save LG Record** to add it to active custody.\n\n"
                f"👉 [Click here to open the Record New LG page]({nav_base}/lg-records/new)"
            )

        if any(w in q_lower for w in ["issue", "issuance", "request lg"]):
            return (
                f"To initiate a new LG Issuance Request in Grow as **{role_label}**:\n\n"
                f"1. Navigate to **Sidebar -> LG Issuance -> New Request**.\n"
                f"2. Select the **Issuing Entity** and **LG Type** (Performance, Bid Bond, Advance Payment).\n"
                f"3. Specify the **Amount**, **Currency**, **Requested Bank**, and **Beneficiary Details**.\n"
                f"4. Attach supporting tender/contract documents.\n"
                f"5. Click **Submit for Approval** to enter the corporate approval matrix.\n\n"
                f"👉 [Click here to open New Issuance Request]({nav_base}/issuance/requests/new)"
            )

        # 10. Custom Data Storage Bucket Setup (STORAGE_BUCKET_NAME)
        if any(w in q_lower for w in [
            "data bucket", "storage bucket", "custom bucket", "own bucket", "bucket name",
            "storage_bucket_name", "gcs bucket", "cloud storage", "setup my own bucket", "set up my own data bucket"
        ]):
            from app.core.ai_integration import get_service_account_email
            sa_email = get_service_account_email()
            return (
                f"**Setting Up a Custom Dedicated Data Storage Bucket (`STORAGE_BUCKET_NAME`)**:\n\n"
                f"Grow allows enterprise organizations to store all scanned LG copies, amendment files, and internal supporting documents in their own dedicated Google Cloud Storage (GCS) bucket for full data residency, compliance, and enterprise data ownership:\n\n"
                f"1. **Create Your Cloud Storage Bucket**:\n"
                f"   - In your company's Google Cloud Console, create a private storage bucket (e.g. `acme-treasury-guarantee-docs`).\n\n"
                f"2. **Authorize Grow Access (IAM Permissions)**:\n"
                f"   - In your Cloud Console, go to your bucket's **Permissions (IAM)** tab.\n"
                f"   - Click **Grant Access (Add Principal)** and paste the Grow Service Account email for your environment:\n"
                f"     ```text\n"
                f"     {sa_email}\n"
                f"     ```\n"
                f"   - Assign the role **`Storage Object Admin`** (allows uploading, OCR scanning, and secure viewing of LG documents).\n\n"
                f"3. **Link the Bucket in Grow**:\n"
                f"   - Navigate to **Sidebar ➔ Configuration ➔ Settings** (`{nav_base}/module-configs`).\n"
                f"   - Locate **Group 5: Security, Authentication & Platform Policies**.\n"
                f"   - Enter your bucket name in **`STORAGE_BUCKET_NAME`**.\n"
                f"   - Click **Save Changes** at the bottom of the page.{role_note}\n\n"
                f"👉 [Open Platform Settings]({nav_base}/module-configs)"
            )

        # Dynamic AI synthesis using Grounding Knowledge Base
        try:
            from app.core.ai_integration import _get_genai_client, _safe_generate_content_sync, GEMINI_MODEL_NAME
            from app.services.system_knowledge_base import get_system_knowledge
            client = _get_genai_client()
            if client:
                kb_context = get_system_knowledge()
                prompt = (
                    f"You are the Grow Platform & Treasury Assistant. A user ({user_email}, role: {role_label}) asked:\n"
                    f"\"{user_question}\"\n\n"
                    f"Using the authoritative platform knowledge base below, give a clear, accurate, step-by-step guidance response formatted in Markdown.\n"
                    f"Always maintain enterprise treasury professional tone and reference the appropriate navigation paths if applicable.\n\n"
                    f"--- PLATFORM KNOWLEDGE BASE ---\n"
                    f"{kb_context}\n"
                    f"--- END KNOWLEDGE BASE ---"
                )
                res = _safe_generate_content_sync(
                    client=client,
                    model=GEMINI_MODEL_NAME,
                    contents=prompt
                )
                if res and res.text and len(res.text.strip()) > 30:
                    return res.text.strip() + f"\n\n👉 [Open Platform Dashboard]({nav_base}/dashboard)"
        except Exception as e:
            logger.warning(f"Dynamic system help GenAI call failed: {e}")

        # Smart Section Matching fallback (prevents full-manual text dumps)
        from app.services.system_knowledge_base import find_relevant_section
        relevant_section = find_relevant_section(user_question)
        
        if relevant_section:
            return (
                f"**Grow Platform Guidance**:\n\n"
                f"{relevant_section}\n\n"
                f"👉 [Go to Dashboard]({nav_base}/dashboard)"
            )

        return (
            f"**Grow Treasury & Platform Guidance**:\n\n"
            f"I can guide you through any workflow, setting, or operational rule in Grow as **{role_label}**.\n\n"
            f"**Common Topics You Can Ask About**:\n"
            f"- **LG Custody (Inbound)**: *\"How do I record a new LG?\"*, *\"How do I extend validity?\"*, *\"How do I share LG data?\"*\n"
            f"- **LG Issuance (Outbound)**: *\"How do I create a new request?\"*, *\"What does Generate Invite do?\"*, *\"How do approval matrices work?\"*\n"
            f"- **Organization Settings**: *\"How to change forced renewal days?\"*, *\"How to configure custom storage bucket?\"*, *\"How to CC manager in emails?\"*\n\n"
            f"👉 [Open Platform Dashboard]({nav_base}/dashboard)"
        )

    def process_query(
        self,
        db: Session,
        user_question: str = "",
        customer_id: int = 1,
        user_id: int = 1,
        card_id: Optional[str] = None,
        has_all_entity_access: bool = True,
        entity_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        try:
            if not is_ai_query_assistant_enabled():
                return {
                    "success": False,
                    "error": "AI Data Assistant feature is currently disabled under system configuration.",
                    "code": "FEATURE_DISABLED"
                }
            user = db.query(User).filter(User.id == user_id).first()
            user_role = str(user.role.value if hasattr(user.role, "value") else (user.role.name if hasattr(user.role, "name") else str(user.role))).upper() if (user and user.role) else "END_USER"
            user_email = user.email if user else "user@example.com"

            # LEVEL 0: Card ID Resolution
            if card_id:
                logger.info(f"Level 0 AI Assistant card_id request: user_id={user_id}, card_id={card_id}")
                is_valid_card, card_meta = policy_guardrail.validate_card_id(card_id)
                if not is_valid_card:
                    return {
                        "success": False,
                        "error": f"Card ID '{card_id}' is not permitted or unrecognized.",
                        "code": "CARD_ID_REJECTED"
                    }

                intent = card_meta["intent"]
                params = card_meta.get("params", {})

                if intent == "system_help":
                    answer = self.handle_system_help(params.get("query", ""), user_role, user_email)
                    return {
                        "success": True,
                        "answer": answer,
                        "references": [],
                        "suggested_chips": [
                            {"label": "📊 Portfolio Summary", "query": "show portfolio summary"},
                            {"label": "🛡️ View Inbound Custody", "query": "show custody summary"},
                            {"label": "📤 View Issuance Pipeline", "query": "show issuance pipeline"}
                        ],
                        "level": 4,
                        "source_awareness": "SYSTEM_KNOWLEDGE",
                        "intent": "system_help"
                    }

                query_result = self.execute_orm_query(
                    db, customer_id, user_id, intent, params, has_all_entity_access, entity_ids
                )
                answer, references, suggested_chips = self.format_application_response(
                    db, customer_id, user_id, intent, query_result, user_question
                )
                return {
                    "success": True,
                    "answer": answer,
                    "references": references,
                    "suggested_chips": suggested_chips,
                    "level": 0,
                    "source_awareness": "SYSTEM_DATA",
                    "intent": intent
                }

            # LEVEL 4 / 3 / 1 / 2: Natural Language Query Pipeline
            logger.info(f"AI Assistant NL query: user_id={user_id}, customer_id={customer_id}, q='{user_question[:60]}'")
            classification = self.classify_and_interpret(user_question)
            suggested_level = classification.get("suggested_level", 1)
            intent = classification.get("intent", "search_lgs")
            params = classification.get("parameters", {})

            is_valid_op, valid_params = policy_guardrail.validate_intent(intent, params)
            if intent == "security_rejected":
                return {
                    "success": True,
                    "answer": (
                        "🛡️ **Security Policy Enforcement**:\n\n"
                        "I am an enterprise Treasury AI Assistant and cannot provide assistance, instructions, code, or guidance regarding hacking, exploiting vulnerabilities, bypassing security controls, or unauthorized access.\n\n"
                        "All operations in Grow are strictly role-enforced, authenticated via JWT, and logged in the immutable **Audit Log** (`/corporate-admin/audit-logs` or `/system-owner/audit-logs`).\n\n"
                        "If you have legitimate security inquiries or noticed potential suspicious behavior, please notify your organization's Corporate Administrator or Grow Security."
                    ),
                    "references": [],
                    "suggested_chips": [
                        {"label": "🛡️ View Audit Logs", "query": "show recent audit logs"},
                        {"label": "📊 Portfolio Summary", "query": "show portfolio summary"}
                    ],
                    "level": 3,
                    "source_awareness": "SYSTEM_KNOWLEDGE",
                    "intent": "security_rejected"
                }

            if intent == "capability_gap":
                return {
                    "success": True,
                    "answer": "I don't currently have enough information or transactional capability to execute that action. If you need assistance navigating the system, please ask!",
                    "references": [],
                    "suggested_chips": [
                        {"label": "📊 Portfolio Summary", "query": "show portfolio summary"},
                        {"label": "🛡️ View Inbound Custody", "query": "show custody summary"}
                    ],
                    "level": 3,
                    "source_awareness": "SYSTEM_KNOWLEDGE",
                    "intent": "capability_gap"
                }

            if not is_valid_op or intent == "rejected_scope":
                return {
                    "success": True,
                    "answer": "I am specialized strictly in corporate treasury, trade finance, guarantees, liquidity, and Grow platform operations. Please ask a treasury or system-related question.",
                    "references": [],
                    "suggested_chips": [
                        {"label": "📊 Portfolio Summary", "query": "show portfolio summary"},
                        {"label": "🛡️ View Inbound Custody", "query": "show custody summary"},
                        {"label": "📤 View Issuance Pipeline", "query": "show issuance pipeline"}
                    ],
                    "level": 3,
                    "source_awareness": "GENERAL_AI_KNOWLEDGE",
                    "intent": "rejected_scope"
                }

            if suggested_level == 4 or intent == "system_help":
                answer = self.handle_system_help(user_question, user_role, user_email)
                return {
                    "success": True,
                    "answer": answer,
                    "references": [],
                    "suggested_chips": [
                        {"label": "📊 Portfolio Summary", "query": "show portfolio summary"},
                        {"label": "🛡️ View Inbound Custody", "query": "show custody summary"},
                        {"label": "📤 View Issuance Pipeline", "query": "show issuance pipeline"}
                    ],
                    "level": 4,
                    "source_awareness": "SYSTEM_KNOWLEDGE",
                    "intent": "system_help"
                }

            if suggested_level == 3 and intent == "general_treasury":
                term = params.get("term", "")
                glossary_def = OFFLINE_TREASURY_GLOSSARY.get(term)
                if not glossary_def:
                    glossary_def = f"In corporate treasury, **{term.upper()}** is an essential financial risk and liquidity management mechanism."
                return {
                    "success": True,
                    "answer": glossary_def,
                    "references": [],
                    "suggested_chips": [
                        {"label": "📊 Portfolio Summary", "query": "show portfolio summary"},
                        {"label": "🏛️ Bank Facilities Headroom", "query": "show facility headroom"}
                    ],
                    "level": 3,
                    "source_awareness": "GENERAL_AI_KNOWLEDGE",
                    "intent": "general_treasury"
                }

            if suggested_level in (0, 1):
                query_result = self.execute_orm_query(
                    db, customer_id, user_id, intent, valid_params, has_all_entity_access, entity_ids
                )
                answer, references, suggested_chips = self.format_application_response(
                    db, customer_id, user_id, intent, query_result, user_question
                )
                return {
                    "success": True,
                    "answer": answer,
                    "references": references,
                    "suggested_chips": suggested_chips,
                    "visual_metadata": {"type": "lg_search", "count": len(references)},
                    "level": 1,
                    "source_awareness": "SYSTEM_DATA",
                    "intent": intent
                }

            try:
                from app.core.ai_integration import _get_genai_client, _safe_generate_content_sync, GEMINI_MODEL_NAME
                client = _get_genai_client()
                if client:
                    tok_recs, tok_ben, tok_fac, token_map = privacy_tokenizer.tokenize_complex_payload([], {}, [])
                    genai_contents = (
                        "You are Grow Treasury Assistant, an institutional corporate treasury AI expert embedded in the Grow Treasury System. "
                        "You have full knowledge of Grow modules: Letter of Guarantee (Custody & Issuance), Bank Reconciliation, and "
                        "the FX & T-Bill Digital Quotation Module (multi-bank RFQs, bidding window timers, post-window corporate acceptance "
                        "windows with AUTO_ACCEPT/AUTO_REJECT policies, multi-currency basket packages with partial leg awards, bank dealer roles "
                        "[Approver, Execution Trader, View-Only], additional cost matrices [Min, %, Max, Flat], Central Bank tolerance limits, "
                        "and neutral outcome conclusion notifications). Answer professionally, accurately, and concisely. "
                        f"User question: {sanitized_q}"
                    )
                    response = _safe_generate_content_sync(
                        client=client,
                        model=GEMINI_MODEL_NAME,
                        contents=genai_contents
                    )
                    raw_text = response.text or ""
                    final_answer = privacy_tokenizer.detokenize_response(raw_text, token_map)
                    return {
                        "success": True,
                        "answer": final_answer,
                        "references": [],
                        "suggested_chips": [
                            {"label": "📊 Portfolio Summary", "query": "show portfolio summary"},
                            {"label": "🛡️ View Inbound Custody", "query": "show custody summary"}
                        ],
                        "level": 2,
                        "source_awareness": "COMBINATION",
                        "intent": intent
                    }
            except Exception as genai_err:
                logger.warning(f"GenAI call failed, falling back to Level 1 ORM: {genai_err}")

            query_result = self.execute_orm_query(
                db, customer_id, user_id, intent, valid_params, has_all_entity_access, entity_ids
            )
            answer, references, suggested_chips = self.format_application_response(
                db, customer_id, user_id, intent, query_result, user_question
            )
            return {
                "success": True,
                "answer": answer,
                "references": references,
                "suggested_chips": suggested_chips,
                "visual_metadata": {"type": "lg_search", "count": len(references)},
                "level": 1,
                "source_awareness": "SYSTEM_DATA",
                "intent": intent
            }

        except Exception as e:
            logger.error(f"Error executing AI query assistant: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"An error occurred while processing your request: {str(e)}",
                "code": "EXECUTION_ERROR"
            }


ai_query_assistant_service = AIQueryAssistantService()
