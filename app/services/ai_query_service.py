# app/services/ai_query_service.py
"""
AI Data Query Assistant - Production-Grade 4-Level Treasury AI Architecture with System & User Self-Awareness

Principle:
- AI provides understanding, classification, and reasoning.
- APPLICATION provides authorization, control, policy enforcement, level determination, and execution.
- DATABASE remains the sole source of truth.

Levels:
- LEVEL 0: System Only (Bypasses AI completely via backend-resolved card_id)
- LEVEL 1: Simple AI + System (Single intent LLM classification -> ORM query -> Application formatting)
- LEVEL 2: Complex AI + System (Multi-step plan -> ORM -> Question & Record Tokenization -> LLM synthesis -> Token validation -> Detokenize)
- LEVEL 3: General Treasury AI (AI classification -> Policy Guardrail enforcement -> LLM Treasury domain answer)
- LEVEL 4: System Knowledge & User Guidance (User-aware platform guide -> Grounded workflow, role permissions & audit history)
"""

import os
import json
import logging
import re
import calendar
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy.orm import Session, joinedload, selectinload
from sqlalchemy import or_, func

import app.models as models
from app.models.models_issuance import IssuanceFacility, IssuedLGRecord
from app.constants import ApprovalRequestStatusEnum, LgStatusEnum, UserRole
from app.services.ai_policy_guardrail import policy_guardrail, MAX_RESPONSE_CHARS
from app.services.ai_privacy_tokenizer import privacy_tokenizer
from app.services.system_knowledge_base import get_system_knowledge

logger = logging.getLogger(__name__)


def is_ai_query_assistant_enabled() -> bool:
    """Checks if the AI Data Assistant feature flag is enabled in environment."""
    return os.getenv("AI_DATA_ASSISTANT_ENABLED", "false").lower() == "true"


def normalize_currency_terms(term: str) -> List[str]:
    """Normalizes currency queries so EURO, Eur, EUR, Dollars, EGP, L.E. all match correctly."""
    if not term:
        return []
    t = term.upper().strip()
    terms = {t}

    if t in {"EUR", "EURO", "EUROS", "€"}:
        terms.update(["EUR", "EURO", "EUROS", "€"])
    elif t in {"USD", "DOLLAR", "DOLLARS", "$"}:
        terms.update(["USD", "US DOLLAR", "DOLLAR", "DOLLARS", "$"])
    elif t in {"EGP", "POUND", "POUNDS", "LE", "L.E.", "EGYPTIAN POUND", "EGYPTIAN", "EGYPTIAN POUNDS"}:
        terms.update(["EGP", "EGYPTIAN POUND", "EGYPTIAN POUNDS", "POUND", "POUNDS", "LE", "L.E."])

    return list(terms)


def _extract_record_fields(rec) -> tuple:
    """
    Extracts (bank_name, currency_code, amount) from either a custody LGRecord
    or an issuance IssuedLGRecord, providing a unified interface for formatting.
    """
    if isinstance(rec, IssuedLGRecord):
        b_name = rec.bank.short_name if (rec.bank and rec.bank.short_name) else (rec.bank.name if rec.bank else "Unknown Bank")
        c_code = rec.currency.iso_code if (rec.currency and rec.currency.iso_code) else "EGP"
        amt = float(rec.current_amount) if rec.current_amount is not None else 0.0
    else:
        b_name = rec.issuing_bank.short_name or rec.issuing_bank.name if rec.issuing_bank else getattr(rec, 'issuer_name', None) or "Unknown Bank"
        c_code = rec.lg_currency.iso_code if (rec.lg_currency and rec.lg_currency.iso_code) else (rec.lg_currency.name if rec.lg_currency else "EGP")
        amt = float(rec.lg_amount) if rec.lg_amount is not None else 0.0
    return b_name, c_code, amt


def _get_lg_number(rec) -> str:
    """Returns the LG number/reference from either record type."""
    if isinstance(rec, IssuedLGRecord):
        return rec.lg_ref_number or rec.internal_serial or f"ISS-{rec.id}"
    return rec.lg_number


def _get_beneficiary(rec) -> str:
    """Returns the beneficiary name from either record type."""
    if isinstance(rec, IssuedLGRecord):
        return rec.beneficiary_name or "Corporate Entity"
    return rec.beneficiary_corporate.entity_name if rec.beneficiary_corporate else "Corporate Entity"


def _format_audit_action_name(action_type: str) -> str:
    """Translates system audit constants into friendly human-readable descriptions."""
    if not action_type:
        return "Action Performed"
    action_map = {
        "LG_EXTENDED": "Extended LG Validity",
        "LG_RELEASED": "Released LG Obligation",
        "LG_LIQUIDATED_FULL": "Fully Liquidated LG Claim",
        "LG_LIQUIDATED_PARTIAL": "Partially Liquidated LG Claim",
        "LG_DECREASED_AMOUNT": "Decreased LG Amount",
        "LG_AMENDED": "Amended LG Terms",
        "LG_ACTIVATED": "Activated Non-Operative LG",
        "LG_INSTRUCTION_CANCELED": "Canceled Last LG Instruction",
        "LG_INSTRUCTION_DELIVERED": "Recorded Delivery Proof to Bank",
        "LG_BANK_REPLY_RECORDED": "Recorded Bank Reply",
        "LG_REMINDER_TO_BANKS": "Sent Formal Reminder to Bank",
        "LG_SINGLE_OWNER_CHANGED": "Changed LG Internal Owner",
        "LG_BULK_OWNER_CHANGED": "Bulk Updated LG Owners",
        "CREATE": "Created Record",
        "UPDATE": "Updated Record Details",
        "DELETE": "Deleted Record",
        "SOFT_DELETE": "Archived Record",
        "RESTORE": "Restored Record",
        "LOGIN_SUCCESS": "User Logged In",
        "LOGIN_FAILED": "Failed Login Attempt",
        "LOGOUT": "User Logged Out",
        "AI_SCAN_SUCCESS": "Scanned LG Document via AI OCR",
        "AI_SCAN_FAILED": "AI OCR Scan Failed",
        "APPROVAL_REQUEST_SUBMITTED": "Submitted Maker-Checker Approval Request",
        "APPROVAL_REQUEST_APPROVED": "Approved Request (Checker)",
        "APPROVAL_REQUEST_REJECTED": "Rejected Request (Checker)",
        "APPROVAL_REQUEST_WITHDRAWN": "Withdrew Approval Request (Maker)",
        "CUSTOMER_ONBOARD": "Onboarded Organization",
        "SUBSCRIPTION_RENEWED": "Renewed Subscription Plan",
        "PASSWORD_CHANGE_FIRST_LOGIN_SUCCESS": "Changed Initial Password",
        "PASSWORD_RESET_COMPLETED": "Completed Password Reset",
    }
    return action_map.get(action_type, action_type.replace("_", " ").title())


# Ultra-short command / greeting fast paths
FAST_PATH_GREETINGS = {
    "hello", "hi", "hey", "help", "start", "restart", "reset", "clear", "?", "menu", "who are you",
    "ازيك", "مرحبا", "اهلا", "السلام عليكم", "مساء الخير", "صباح الخير"
}


class AIQueryAssistantService:
    """
    Production 4-Level Treasury AI Assistant Service with System & User Self-Awareness.
    Enforces application control, tokenization, policy guardrails, and audit logging.
    """

    def __init__(self):
        self.enabled = is_ai_query_assistant_enabled()
        self._last_lg_context: Dict[Tuple[int, int], Dict[str, Any]] = {}
        self._current_query_params: Dict[str, Any] = {}

    def _get_genai_client(self):
        """Lazy-loads Gemini client via vertexai or api key fallback."""
        try:
            from app.core.ai_integration import _get_genai_client as get_client
            client = get_client()
            if client:
                return client
        except Exception as e:
            logger.warning(f"Failed to load Vertex GenAI client: {e}")

        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if api_key:
            try:
                from google import genai
                return genai.Client(api_key=api_key)
            except Exception as e:
                logger.error(f"Failed to initialize direct GenAI client: {e}")
        return None

    def _call_llm(self, prompt: str) -> Optional[str]:
        """Invokes Gemini LLM safely with exponential backoff on transient rate limits."""
        client = self._get_genai_client()
        if not client:
            logger.warning("GenAI client unavailable for AI Query Assistant.")
            return None

        from app.core.ai_integration import GEMINI_MODEL_NAME
        model_name = GEMINI_MODEL_NAME or "gemini-2.5-flash"

        for attempt in range(2):
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt
                )
                return response.text if response else None
            except Exception as e:
                err_str = str(e)
                if ("429" in err_str or "RESOURCE_EXHAUSTED" in err_str) and attempt == 0:
                    logger.warning("Gemini 429 Rate Limit hit. Retrying after 2 second delay...")
                    time.sleep(2)
                    continue
                logger.error(f"Gemini LLM invocation error in AI Query Assistant: {e}")
                return None
        return None

    def get_user_context(self, db: Session, user_id: int, customer_id: int) -> Dict[str, Any]:
        """
        Builds a comprehensive user profile and subscription capability context
        for grounding the AI assistant's responses to the specific logged-in user.
        """
        user = db.query(models.User).options(
            joinedload(models.User.customer).joinedload(models.Customer.subscription_plan),
            selectinload(models.User.entity_associations).joinedload(models.UserCustomerEntityAssociation.customer_entity)
        ).filter(models.User.id == user_id).first()

        if not user:
            return {
                "user_id": user_id,
                "email": "user@organization.com",
                "role": "end_user",
                "role_display": "End User",
                "customer_name": "Your Organization",
                "plan_name": "Enterprise",
                "has_custody_module": True,
                "has_issuance_module": True,
                "has_quotation_module": True,
                "has_reconciliation_module": True,
                "can_maker_checker": True,
                "entities": ["All Organization Entities"]
            }

        cust = user.customer
        plan = cust.subscription_plan if cust else None

        role_str = user.role.value if hasattr(user.role, 'value') else str(user.role)
        role_display_map = {
            "system_owner": "System Owner (Super Admin)",
            "corporate_admin": "Corporate Administrator",
            "checker": "Checker (Senior Approver)",
            "end_user": "End User (Treasury Specialist)",
            "viewer": "Viewer (Read-Only Stakeholder)"
        }
        role_display = role_display_map.get(role_str, role_str.replace("_", " ").title())

        entities = []
        if user.has_all_entity_access:
            entities = ["All Organization Entities"]
        elif user.entity_associations:
            entities = [assoc.customer_entity.entity_name for assoc in user.entity_associations if assoc.customer_entity and not assoc.is_deleted]
        if not entities:
            entities = ["Primary Corporate Entity"]

        return {
            "user_id": user.id,
            "email": user.email,
            "role": role_str,
            "role_display": role_display,
            "customer_name": cust.name if cust else "Your Organization",
            "plan_name": plan.name if plan else "Standard Plan",
            "has_custody_module": plan.has_custody_module if plan else True,
            "has_issuance_module": plan.has_issuance_module if plan else False,
            "has_quotation_module": plan.has_quotation_module if plan else False,
            "has_reconciliation_module": plan.has_reconciliation_module if plan else False,
            "can_maker_checker": plan.can_maker_checker if plan else False,
            "entities": entities
        }

    def classify_and_interpret(self, user_question: str, customer_id: Optional[int] = None, user_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Classifies user question into appropriate Level (1, 2, 3, or 4) and extracts intent/topic.
        """
        prompt = f"""
You are an AI router and intent parser for the Grow Corporate Treasury & Trade Finance System.
Analyze the user's natural language question and classify it into the correct level:

LEVEL 1 ("simple_query"): Simple question mapping to one approved database operation:
  - "get_lg_analytics_summary": For portfolio exposure, total exposure by currency, portfolio breakdown, financial overview, "how many active LGs", or counting total LGs.
  - "search_lgs": For searching specific existing customer LGs in the database by currency, status (expired, released, liquidated), bank, or reference number. (e.g., "Find our USD guarantees", "Show expired LGs").
  - "find_expiring_lgs": For customer guarantees expiring within N days, or expiring during a specific month/year, or "next month", "this month".
  - "get_facility_analytics": For bank credit facility limits and credit line queries.
  - "get_pending_approvals": For checking pending approval requests awaiting review.
  - "get_lg_details": For details of a specific LG by number/ID.
  - "get_audit_history": For checking recent activity, audit logs, transactions, who created/modified/extended an LG, or history of actions. (e.g., "What did I do recently?", "Show my recent transactions", "Show organization audit logs", "Who modified LG...").
  - "get_user_profile": For asking about the user's profile, role, active permissions, capabilities, or enabled subscription modules. (e.g., "What can I do?", "What is my role?", "What are my permissions?", "Who am I?", "What modules are active?").

LEVEL 2 ("complex_analysis"): Complex question requiring multi-step analysis, relative rankings, or combining multiple cross-entity criteria (e.g. "Which beneficiaries have highest exposure and guarantees expiring in 90 days?").

LEVEL 3 ("general_treasury"): General corporate treasury / trade finance / finance knowledge questions requiring NO customer database data.
  - Conceptual/Rule questions: "What is cash pooling?", "How do forward contracts (fwd) work?", "What is an IRS swap?", "Can an Advance Payment LG be issued in USD?", "What is liquidity management?", "Explain the difference between LG and LC".
  - Short financial terms/abbreviations: "fwd", "fx forward", "ndf", "irs", "sblc", "bid bond definition".

LEVEL 4 ("system_help"): Questions about the Grow Platform itself, how to perform workflows, step-by-step guidance, navigation, or troubleshooting.
  - How-To Workflows: "How do I extend an LG?", "How can I record a new LG?", "Where is the migration hub?", "How does maker-checker work?", "What is the Action Center?", "How do I request a new issuance?".
  - Platform Behavior/Troubleshooting: "Why can't I see the Issuance menu?", "What does Non-Operative status mean?", "How do I print bank instructions?".

LEVEL 3 REJECTED ("non_treasury"): Questions outside corporate treasury/finance and outside the Grow system (e.g. jokes, general trivia, weather, games, non-financial topics).

UNSUPPORTED ("unsupported"): Commands asking to execute actual bank wire transfers, execute payouts, delete database records, or query internal employee payroll (e.g. "Execute bank wire transfer", "Delete customer records", "Show employee payroll").

Important Extraction Guidelines:
- If user asks about what THEY did or what happened recently in the system ("what did i do", "my recent activity", "audit logs", "who changed LG..."), classify as "get_audit_history" with parameter scope ("my_actions" or "all_organization").
- If user asks about their own capabilities or permissions ("what can i do", "what is my role", "my permissions"), classify as "get_user_profile".
- If user asks how to do something in the system ("how can i record a new lg", "how to extend"), classify as LEVEL 4 "system_help".
- If user asks whether something is conceptually possible ("Can advance LG be issued in USD?"), classify as LEVEL 3 "general_treasury".

User Question: "{user_question}"

Return ONLY a valid JSON object with NO markdown code blocks:
{{
  "suggested_level": 1,
  "topic": "treasury",
  "intent": "<find_expiring_lgs|get_pending_approvals|get_lg_analytics_summary|get_facility_analytics|search_lgs|get_lg_details|get_audit_history|get_user_profile|complex_analysis|general_treasury|system_help|unsupported>",
  "parameters": {{
    "days": 60,
    "month": "",
    "year": "",
    "query": "",
    "currency": "",
    "lg_number": "",
    "status": "",
    "scope": "all_organization",
    "limit": 15
  }}
}}
"""
        q_lower = user_question.lower().strip()

        # Direct pre-check for specific LG number detail lookups (100% offline resilient)
        # Matches patterns like "LG-ALEX-0030-5", "ACME/AE/Wa/001", "LG-CITI-1111", etc.
        lg_num_match = re.search(r'\b(LG[-_][A-Za-z0-9_-]+|[A-Za-z0-9]{2,10}/[A-Za-z0-9/_-]{3,30})\b', user_question, re.IGNORECASE)
        if lg_num_match and any(kw in q_lower for kw in ["detail", "show", "view", "find", "get", "lookup", "info", "what is"]):
            lg_num_val = lg_num_match.group(1).strip()
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "get_lg_details",
                "parameters": {
                    "lg_number": lg_num_val
                }
            }
        # Direct pre-check for transactional execution / unsupported mutations
        if any(kw in q_lower for kw in ["execute an automatic bank wire", "execute bank wire", "wire transfer or payout", "execute wire", "transfer funds", "delete record", "modify database", "employee payroll"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "unsupported", "parameters": {}}

        # Direct pre-check for user profile & permissions
        if any(kw in q_lower for kw in ["what can i do", "what is my role", "what are my permissions", "who am i", "my role", "my permissions", "what modules do we have", "my capabilities"]):
            return {"suggested_level": 1, "topic": "system", "intent": "get_user_profile", "parameters": {}}

        # Direct pre-check for audit & activity history
        if any(kw in q_lower for kw in ["what did i do", "my recent activity", "my recent transactions", "my activity", "what did we do", "audit log", "audit logs", "recent actions", "recent transactions", "activity log", "who created", "who extended", "who modified"]):
            scope = "my_actions" if any(p in q_lower for p in ["i do", "my recent", "my activity", "i perform"]) else "all_organization"
            return {"suggested_level": 1, "topic": "system", "intent": "get_audit_history", "parameters": {"scope": scope, "limit": 15}}


        # Direct pre-check for conversational feedback / request for simplification
        if any(kw in q_lower for kw in ["too hard", "too complex", "explain simpler", "simplify", "don't understand", "dont understand", "can you clarify", "make it simple", "confusing"]):
            return {"suggested_level": 4, "topic": "simplification", "intent": "system_help", "parameters": {"mode": "simplify"}}

        # Direct pre-check for settings and module configs
        if any(kw in q_lower for kw in ["module-config", "module config", "form config", "form-config", "settings", "configuration"]):
            return {"suggested_level": 4, "topic": "system", "intent": "system_help", "parameters": {}}

        # Direct pre-check for short treasury abbreviations
        if q_lower in {"fwd", "fx fwd", "forward", "forwards", "ndf", "irs", "swap", "sblc", "lc", "lg vs lc", "cash pooling"}:
            return {"suggested_level": 3, "topic": "treasury", "intent": "general_treasury", "parameters": {}}

        response_text = self._call_llm(prompt)
        if not response_text:
            # Deterministic rule-based fallback if LLM is unavailable or rate-limited
            if any(kw in q_lower for kw in ["how do i", "how to", "where is", "grow system", "navigation", "menu"]):
                return {"suggested_level": 4, "topic": "system", "intent": "system_help", "parameters": {"query": user_question}}
            if any(kw in q_lower for kw in ["what did i do", "audit", "activity"]):
                return {"suggested_level": 1, "topic": "system", "intent": "get_audit_history", "parameters": {"scope": "all_organization", "limit": 15}}
            if any(kw in q_lower for kw in ["what can i do", "my role", "permissions"]):
                return {"suggested_level": 1, "topic": "system", "intent": "get_user_profile", "parameters": {}}
            if any(kw in q_lower for kw in ["what is", "how do", "explain", "definition", "can a", "can we", "is it possible", "fwd", "swap"]):
                return {"suggested_level": 3, "topic": "treasury", "intent": "general_treasury", "parameters": {}}
            if "expir" in q_lower:
                return {"suggested_level": 1, "topic": "treasury", "intent": "find_expiring_lgs", "parameters": {}}
            if any(st in q_lower for st in ["expired", "released", "liquidated"]):
                status_val = next((s for s in ["expired", "released", "liquidated"] if s in q_lower), "")
                return {"suggested_level": 1, "topic": "treasury", "intent": "search_lgs", "parameters": {"status": status_val}}
            if any(curr in q_lower for curr in ["usd", "eur", "euro", "egp", "pound", "dollar", "egyptian"]):
                return {"suggested_level": 1, "topic": "treasury", "intent": "search_lgs", "parameters": {}}
            return {"suggested_level": 1, "topic": "treasury", "intent": "search_lgs", "parameters": {}}

        cleaned = re.sub(r"^```json\s*", "", response_text.strip(), flags=re.MULTILINE)
        cleaned = re.sub(r"^```\s*", "", cleaned, flags=re.MULTILINE)

        try:
            parsed = json.loads(cleaned)
            params = parsed.get("parameters", {}) or {}

            # Pronoun Resolution ("this LG", "it", "this guarantee", "the beneficiary", "the amount")
            q_lower = user_question.lower()
            pronoun_triggers = [
                "this lg", "this guarantee", "the lg", "this one", " it ", "it expire", "it issued",
                "who is the beneficiary", "what is the amount", "what is the currency", "when does it expire",
                "who is responsible"
            ]
            is_pronoun_ref = any(p in q_lower for p in pronoun_triggers)
            current_lg_num = params.get("lg_number")
            if is_pronoun_ref and (not current_lg_num or str(current_lg_num).lower() in {"none", "null", ""}):
                if customer_id and user_id and (customer_id, user_id) in self._last_lg_context:
                    cached = self._last_lg_context[(customer_id, user_id)]
                    params["lg_number"] = cached.get("lg_number")
                    params["lg_id"] = cached.get("lg_id")
                    parsed["parameters"] = params
                    parsed["intent"] = "get_lg_details"
                    parsed["suggested_level"] = 1

            # Detect single currency questions (e.g. "how much in EGP?", "how much in USD?")
            if "egp" in q_lower:
                params["currency"] = "EGP"
            elif "usd" in q_lower or "dollar" in q_lower:
                params["currency"] = "USD"
            elif "eur" in q_lower or "euro" in q_lower:
                params["currency"] = "EUR"
            elif "sar" in q_lower or "riyal" in q_lower:
                params["currency"] = "SAR"
            elif "aed" in q_lower or "dirham" in q_lower:
                params["currency"] = "AED"
            elif "gbp" in q_lower or "pound" in q_lower:
                params["currency"] = "GBP"

            parsed["parameters"] = params
            return parsed
        except json.JSONDecodeError:
            return {"suggested_level": 1, "topic": "treasury", "intent": "search_lgs", "parameters": {}}

    def handle_system_help(self, user_question: str, user_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Level 4 Handler: Answers questions regarding Grow platform capabilities,
        navigation paths, role permissions, and feature workflows using grounded system knowledge
        personalized to the user's specific role and active subscription modules.
        Includes direct 1-click action links whenever user asks how to perform an action.
        """
        system_kb = get_system_knowledge()
        ctx = user_context or {}
        q_lower = user_question.lower().strip()

        raw_role = str(ctx.get("role", "end_user")).lower()
        role_disp = ctx.get("role_display", "End User")
        cust_name = ctx.get("customer_name", "Your Organization")
        plan_name = ctx.get("plan_name", "Enterprise")
        cust_mod = ctx.get("has_custody_module", True)
        iss_mod = ctx.get("has_issuance_module", False)
        quot_mod = ctx.get("has_quotation_module", False)
        rec_mod = ctx.get("has_reconciliation_module", False)
        mk_ck = ctx.get("can_maker_checker", False)

        if "admin" in raw_role or raw_role == "corporate_admin":
            role_prefix = "corporate-admin"
        elif "checker" in raw_role:
            role_prefix = "checker"
        elif "system_owner" in raw_role or "owner" in raw_role:
            role_prefix = "system-owner"
        else:
            role_prefix = "end-user"

        record_new_url = f"/{role_prefix}/lg-records/new"
        records_url = f"/{role_prefix}/lg-records"
        issuance_req_url = f"/{role_prefix}/issuance/request-new"
        approvals_url = f"/{role_prefix}/approval-requests" if role_prefix == "corporate-admin" else f"/{role_prefix}/pending-approvals"
        settings_url = f"/{role_prefix}/module-configs"
        facilities_url = f"/{role_prefix}/issuance/facilities"
        recon_url = f"/{role_prefix}/issuance/reconciliation"
        users_url = f"/{role_prefix}/users"
        reports_url = f"/{role_prefix}/reports"

        profile_lines = [
            f"Logged-in User Role: {role_disp} (Route Prefix: /{role_prefix})",
            f"Customer Organization: {cust_name}",
            f"Active Subscription Plan: {plan_name}",
            f"Enabled Modules: Custody={cust_mod}, Issuance={iss_mod}, Quotations={quot_mod}, Bank Reconciliation={rec_mod}",
            f"Maker-Checker Workflow Active: {mk_ck}",
            "Direct Page Links Available For This Role:",
            f"- Record New LG: [{record_new_url}]({record_new_url})",
            f"- All LG Records: [{records_url}]({records_url})",
            f"- Issuance Request Form: [{issuance_req_url}]({issuance_req_url})",
            f"- Pending Approvals: [{approvals_url}]({approvals_url})",
            f"- Module Settings & Configurations: [{settings_url}]({settings_url})",
            f"- Bank Facilities: [{facilities_url}]({facilities_url})",
            f"- Position Reconciliation: [{recon_url}]({recon_url})"
        ]
        profile_str = chr(10).join(profile_lines)

        prompt = f"""
You are the executive AI Treasury Assistant for the Grow Corporate Treasury Platform.
Answer the user's question with elegance, high signal-to-noise ratio, and crystal-clear structure.

USER CONTEXT:
================================================================================
{profile_str}
================================================================================

GROUNDING PLATFORM KNOWLEDGE:
================================================================================
{system_kb}
================================================================================

CRITICAL RESPONSE STYLE GUIDELINES:
1. DIFFERENTIATE INTENTS CAREFULLY:
   - If the user asks about SYSTEM CAPABILITIES (e.g. "What can this system do?", "What is Grow?", "What are the platform capabilities?"):
     Explain the core purpose of Grow as an enterprise Corporate Treasury & Trade Finance platform, then present the 5 Core Pillars/Modules clearly with concise value propositions:
       • 🛡️ **LG Custody (Inbound)**: Digital vault, AI OCR scanning, automated milestone tracking, and full lifecycle maintenance (Extensions, Releases, Liquidations, Decreases).
       • 📤 **LG Issuance (Outbound)**: End-to-end issuance requests, AI-powered bank facility scoring (0-100), automated bank application forms, and bank position reconciliation.
       • 💱 **FX & T-Bill Quotations**: Real-time multi-bank RFQ dealing room with competitive bidding and corporate governance approval gates.
       • 📑 **Bank Position Reconciliation**: Automated matching engine for active bank credit facilities, statements, and accounting GL exports.
       • ⚖️ **Governance & Customization**: Maker-Checker dual control, dynamic approval matrix, 49+ customizable tenant settings, and tamper-evident audit trails.

   - If the user asks about AI ASSISTANT CAPABILITIES (e.g. "What can you do?", "What can you help me with?", "How can you assist me?"):
     Focus strictly on what YOU as the AI Assistant can do to assist them:
       • 🔍 **Live Portfolio Lookups**: Query active LG records, find expiring guarantees, search by beneficiary or bank, and inspect recent audit logs.
       • 🧭 **Step-by-Step Guidance**: Give exact, role-tailored click paths for any task or workflow in the platform.
       • ⚙️ **Settings & Policy Advisor**: Explain any of the 49 system configurations, timing windows, and compliance policies.
       • 💡 **Treasury Domain Expert**: Clarify trade finance rules, cash management concepts, bank facilities, and FX hedging.

   - If the user asks a specific HOW-TO workflow or navigation question (e.g. "How do I record an LG?", "How can I extend an LG?", "Where are settings?"):
     Provide exact, numbered steps with bold navigation paths (e.g., "**Sidebar -> LG Custody -> Record New LG**").
     ALWAYS conclude with an actionable 1-click button link formatted as:
     `👉 [Click here to open the Record New LG page]({record_new_url})` (or the relevant page link for the queried workflow).

2. Keep responses concise, modern, beautifully formatted, and under 250 words.

User Question: "{user_question}"
"""
        ai_response = self._call_llm(prompt)
        if not ai_response:
            if "record" in q_lower or "new lg" in q_lower:
                ai_response = f"""To record a new Letter of Guarantee (LG) in Grow:

1. **Navigate to the Record Page**:
   - Go to **Sidebar -> LG Custody -> Record New LG**.

2. **Choose Entry Method**:
   - **AI Document Scan**: Upload a scanned PDF or image. The AI will automatically extract key details.
   - **Manual Entry**: Type details directly into the structured form.

3. **Complete Mandatory Fields**:
   - Fill Beneficiary Entity, Issuing Bank, LG Type, Amount, Currency, and Expiry Date.

4. **Submit Record**:
   - Click **Save LG Record** to add it to active custody.

👉 [Click here to open the Record New LG page]({record_new_url})"""
            elif "extend" in q_lower or "validity" in q_lower:
                ai_response = f"""To extend a Letter of Guarantee (LG) in Grow:

1. Navigate to **Sidebar -> LG Custody -> All LG Records**.
2. Locate and open the specific guarantee.
3. Click the **Actions** menu and select **Extend Validity**.
4. Enter the new expiry date and submit for review.

👉 [Click here to view All LG Records]({records_url})"""
            elif "setting" in q_lower or "config" in q_lower:
                ai_response = f"""To manage system configurations and module settings:

1. Navigate to **Sidebar -> Platform Settings -> Module Configurations**.
2. Configure timing windows, notification alerts, and operational rules.

👉 [Click here to open System Settings]({settings_url})"""
            elif "what can this system do" in q_lower or "what is grow" in q_lower:
                ai_response = """**Grow** is an enterprise Corporate Treasury & Trade Finance platform designed to centralize and automate guarantee lifecycles and treasury operations.

### 🏛️ Core System Modules:
- 🛡️ **LG Custody (Inbound)**: Digital vault, AI OCR scanning, expiry tracking, and lifecycle maintenance (Extensions, Releases, Liquidations, Decreases).
- 📤 **LG Issuance (Outbound)**: End-to-end issuance requests, AI-driven bank facility scoring, automated bank application forms, and bank position reconciliation.
- 💱 **FX & T-Bill Quotations**: Real-time multi-bank RFQ dealing room with competitive bidding and corporate governance controls.
- 📑 **Bank Position Reconciliation**: Automated matching engine for active bank credit facilities, statements, and accounting GL exports.
- ⚖️ **Governance & Customization**: Maker-Checker dual control, dynamic approval matrix, 49+ customizable tenant settings, and immutable audit trails."""
            elif "what can you do" in q_lower or "what can you help" in q_lower or "how can you help" in q_lower:
                ai_response = """I am your **Grow Treasury & System AI Assistant**. Here is how I can assist you:

- 🔍 **Live Data Queries**: Look up active LGs, search by beneficiary or bank, calculate expiring liabilities, and view recent audit logs.
- 🧭 **Role-Tailored Guidance**: Provide step-by-step navigation for any task or workflow based on your permissions.
- ⚙️ **Settings & Policy Advisor**: Explain any of the 49 platform configuration keys, timing windows, and compliance policies.
- 💡 **Treasury Knowledge**: Answer questions regarding trade finance, bank guarantees, cash management, and FX hedging."""
            else:
                ai_response = f"You are logged in as **{role_disp}** at **{cust_name}**."

        # Ensure action link is appended if LLM answered a how-to without one
        if any(kw in q_lower for kw in ["how can i record", "how do i record", "record a new lg", "new lg"]) and "[" not in ai_response:
            ai_response += f"\n\n👉 [Click here to open the Record New LG page]({record_new_url})"
        elif any(kw in q_lower for kw in ["how to extend", "extend an lg", "view my lgs", "all lg records"]) and "[" not in ai_response:
            ai_response += f"\n\n👉 [Click here to view All LG Records]({records_url})"
        elif any(kw in q_lower for kw in ["where are settings", "module config", "configure system"]) and "[" not in ai_response:
            ai_response += f"\n\n👉 [Click here to open System Settings]({settings_url})"

        return {
            "success": True,
            "level": 4,
            "source_awareness": "SYSTEM_KNOWLEDGE",
            "intent": "system_help",
            "answer": ai_response.strip(),
            "query_type": "KNOWLEDGE"
        }

    def execute_orm_query(
        self,
        db: Session,
        customer_id: int,
        user_id: int,
        intent: str,
        params: Dict[str, Any],
        has_all_entity_access: bool = True,
        entity_ids: List[int] = None
    ) -> Any:
        """
        Backend executes authorized ORM query. Mandatory tenant isolation enforced.
        """
        if intent == "get_pending_approvals":
            return db.query(models.ApprovalRequest).options(
                joinedload(models.ApprovalRequest.maker_user)
            ).filter(
                models.ApprovalRequest.customer_id == customer_id,
                models.ApprovalRequest.status == ApprovalRequestStatusEnum.PENDING
            ).order_by(models.ApprovalRequest.created_at.desc()).all()

        if intent == "get_facility_analytics":
            return db.query(IssuanceFacility).options(
                joinedload(IssuanceFacility.bank),
                joinedload(IssuanceFacility.currency)
            ).filter(
                IssuanceFacility.customer_id == customer_id,
                IssuanceFacility.status == "ACTIVE"
            ).all()

        if intent == "get_user_profile":
            return self.get_user_context(db, user_id, customer_id)

        if intent == "get_audit_history":
            scope = str(params.get("scope", "all_organization")).strip().lower()
            limit = int(params.get("limit", 15)) if str(params.get("limit", "")).isdigit() else 15
            lg_num = str(params.get("lg_number", "") or "").strip()

            query = db.query(models.AuditLog).options(
                selectinload(models.AuditLog.user),
                selectinload(models.AuditLog.lg_record)
            ).filter(
                models.AuditLog.customer_id == customer_id
            )

            # Filter by personal user actions if requested
            if scope == "my_actions":
                query = query.filter(models.AuditLog.user_id == user_id)

            # Filter by specific LG number if mentioned
            if lg_num:
                query = query.join(models.AuditLog.lg_record).filter(
                    models.LGRecord.lg_number.ilike(f"%{lg_num}%")
                )

            return query.order_by(models.AuditLog.timestamp.desc()).limit(limit).all()

        # ============================================================================
        # STATUS-AWARE BASE QUERY:
        # Default to Valid (active) LGs only. Override for explicit status queries.
        # ============================================================================
        s_str = str(params.get("status", "") or "").strip().upper()
        STATUS_MAP = {
            "EXPIRED": LgStatusEnum.EXPIRED.value,
            "RELEASED": LgStatusEnum.RELEASED.value,
            "LIQUIDATED": LgStatusEnum.LIQUIDATED.value,
            "VALID": LgStatusEnum.VALID.value,
            "ACTIVE": LgStatusEnum.VALID.value,
            "ALL": None,
        }

        base_query = db.query(models.LGRecord).options(
            joinedload(models.LGRecord.lg_currency),
            joinedload(models.LGRecord.lg_status),
            joinedload(models.LGRecord.issuing_bank),
            joinedload(models.LGRecord.beneficiary_corporate)
        ).filter(
            models.LGRecord.customer_id == customer_id,
            models.LGRecord.is_deleted == False
        )

        if not has_all_entity_access and entity_ids:
            base_query = base_query.filter(models.LGRecord.beneficiary_corporate_id.in_(entity_ids))

        if s_str and s_str in STATUS_MAP:
            if STATUS_MAP[s_str] is not None:
                base_query = base_query.filter(
                    models.LGRecord.lg_status_id == STATUS_MAP[s_str]
                )
        else:
            base_query = base_query.filter(
                models.LGRecord.lg_status_id == LgStatusEnum.VALID.value
            )

        if intent == "find_expiring_lgs":
            now = datetime.now(timezone.utc)
            month_param = params.get("month") or params.get("month_name") or ""
            month_val = str(month_param).lower().strip()

            MONTH_MAP = {
                "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
                "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
                "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9, "october": 10, "oct": 10,
                "november": 11, "nov": 11, "december": 12, "dec": 12
            }

            target_month = None
            target_year = now.year

            if month_val in ("next_month", "next month"):
                next_m = now.month + 1
                if next_m > 12:
                    next_m = 1
                    target_year = now.year + 1
                target_month = next_m
            elif month_val in ("this_month", "this month"):
                target_month = now.month
            elif month_val in ("last_month", "last month"):
                last_m = now.month - 1
                if last_m < 1:
                    last_m = 12
                    target_year = now.year - 1
                target_month = last_m
            elif month_val in MONTH_MAP:
                target_month = MONTH_MAP[month_val]
            elif month_val.isdigit() and 1 <= int(month_val) <= 12:
                target_month = int(month_val)
            else:
                q_str = str(params.get("query", "") or "").lower()
                for token in re.findall(r'\w+', q_str):
                    if token in MONTH_MAP:
                        target_month = MONTH_MAP[token]
                        break

            if target_month:
                year_param = params.get("year")
                if year_param:
                    try:
                        target_year = int(year_param)
                    except (ValueError, TypeError):
                        pass

                return base_query.filter(
                    func.extract('month', models.LGRecord.expiry_date) == target_month,
                    func.extract('year', models.LGRecord.expiry_date) == target_year
                ).order_by(models.LGRecord.expiry_date.asc()).all()

            days = params.get("days", 60)
            try:
                days = int(days)
            except (ValueError, TypeError):
                days = 60
            cutoff = now + timedelta(days=days)
            return base_query.filter(
                models.LGRecord.expiry_date >= now - timedelta(days=1),
                models.LGRecord.expiry_date <= cutoff
            ).order_by(models.LGRecord.expiry_date.asc()).all()

        elif intent == "search_lgs":
            q_raw = params.get("query")
            c_raw = params.get("currency")
            s_raw = params.get("status")

            q_str = str(q_raw).strip() if q_raw and str(q_raw).lower() not in {"none", "null"} else ""
            c_str = str(c_raw).strip() if c_raw and str(c_raw).lower() not in {"none", "null"} else ""
            s_str = str(s_raw).strip() if s_raw and str(s_raw).lower() not in {"none", "null"} else ""

            q = base_query

            if not c_str and q_str:
                for token in q_str.split():
                    clean_tok = re.sub(r'[^\w]', '', token).upper()
                    if clean_tok in {"EURO", "EUR", "EUROS", "USD", "DOLLAR", "DOLLARS", "EGP", "POUND", "POUNDS", "LE", "EGYPTIAN"}:
                        c_str = clean_tok
                        break

            if c_str:
                terms = normalize_currency_terms(c_str)
                curr_conds = []
                for term in terms:
                    pattern = f"%{term}%"
                    curr_conds.append(models.Currency.name.ilike(pattern))
                    curr_conds.append(models.Currency.iso_code.ilike(pattern))
                    curr_conds.append(models.Currency.symbol.ilike(pattern))
                q = q.join(models.LGRecord.lg_currency).filter(or_(*curr_conds))

            STOP_WORDS = {
                "ARE", "THERE", "ANY", "VALID", "ACTIVE", "LG", "LGS", "IN", "FOR",
                "SHOW", "ME", "MY", "FIND", "LIST", "GET", "WHAT", "IS", "THE", "AMOUNT", "OF",
                "GUARANTEE", "GUARANTEES", "LETTER", "LETTERS", "EURO", "EUR", "EUROS",
                "USD", "DOLLAR", "DOLLARS", "EGP", "POUND", "POUNDS", "LE", "EGYPTIAN",
                "DENOMINATED", "HOW", "MANY", "GIVE", "NUMBER", "TOTAL", "COUNT",
                "EXPIRED", "RELEASED", "LIQUIDATED", "ALL", "CURRENT", "CURRENTLY",
                "HAVE", "WE", "DO", "I", "OUR", "CAN", "YOU", "TELL", "PLEASE", "NONE", "NULL"
            }

            tokens = [t for t in re.findall(r'\w+', q_str.upper()) if t not in STOP_WORDS]
            cleaned_search = " ".join(tokens).strip()

            if cleaned_search:
                pattern = f"%{cleaned_search}%"
                q = q.filter(
                    or_(
                        models.LGRecord.lg_number.ilike(pattern),
                        models.LGRecord.description_purpose.ilike(pattern)
                    )
                )

            return q.order_by(models.LGRecord.expiry_date.asc()).all()

        elif intent == "get_lg_details":
            lg_num = params.get("lg_number")
            unrestricted_q = db.query(models.LGRecord).options(
                joinedload(models.LGRecord.lg_currency),
                joinedload(models.LGRecord.lg_status),
                joinedload(models.LGRecord.issuing_bank),
                joinedload(models.LGRecord.beneficiary_corporate)
            ).filter(
                models.LGRecord.customer_id == customer_id,
                models.LGRecord.is_deleted == False
            )
            if lg_num:
                return unrestricted_q.filter(models.LGRecord.lg_number.ilike(f"%{lg_num}%")).limit(5).all()
            return unrestricted_q.order_by(models.LGRecord.created_at.desc()).limit(5).all()

        elif intent == "get_lg_analytics_summary" or intent == "complex_analysis":
            return base_query.order_by(models.LGRecord.expiry_date.asc()).all()

        return []

    def format_application_response(
        self,
        intent: str,
        query_result: Any
    ) -> Tuple[str, List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Level 1 Single AI Call Optimization: Application formats standard deterministic answers.
        Avoids a second AI call for standard queries.
        """
        references = []
        visual_metadata = None

        if intent == "get_user_profile":
            ctx = query_result
            role_desc = {
                "system_owner": "Super-Admin access to global configs, customer management, subscription plans, and audit logs.",
                "corporate_admin": "Full organization management: user accounts, approval matrix, bank facilities, bank accounts, LG categories, migration hub, and organization reports.",
                "checker": "Maker-checker approver: reviews and approves/rejects submitted LG lifecycle actions and issuance requests in the Approval Center.",
                "end_user": "Operational management: recording new LGs, managing custody records, initiating lifecycle actions (extend, release, liquidate), and requesting new issuances.",
                "viewer": "Read-only access: view dashboards, LG records, and reports without operational modification privileges."
            }.get(ctx.get("role", "end_user"), "Operational access.")

            modules = []
            if ctx.get("has_custody_module"): modules.append("LG Custody")
            if ctx.get("has_issuance_module"): modules.append("LG Issuance")
            if ctx.get("has_quotation_module"): modules.append("FX & T-Bill Quotations")
            if ctx.get("has_reconciliation_module"): modules.append("Bank Reconciliation")

            ans = (
                f"👤 **User Profile & Capabilities Overview**:\n\n"
                f"- **User**: `{ctx.get('email')}`\n"
                f"- **Assigned Role**: **{ctx.get('role_display')}**\n"
                f"- **Organization**: **{ctx.get('customer_name')}** (Plan: *{ctx.get('plan_name')}*)\n"
                f"- **Active Modules**: {', '.join(modules) if modules else 'LG Custody'}\n"
                f"- **Entity Scope**: {', '.join(ctx.get('entities', []))}\n"
                f"- **Maker-Checker Dual Control**: {'Enabled' if ctx.get('can_maker_checker') else 'Disabled'}\n\n"
                f"**What you can do with your {ctx.get('role_display')} role**:\n"
                f"{role_desc}"
            )
            return ans, [], {"type": "user_profile", "role": ctx.get("role"), "modules": modules}

        if intent == "get_audit_history":
            logs: List[models.AuditLog] = query_result
            if not logs:
                return "No recent activity or audit logs were found for your organization.", [], None

            ans = f"📋 **Recent Activity & Audit Trail ({len(logs)} most recent actions)**:\n"
            for log in logs:
                ts_str = log.timestamp.strftime("%Y-%m-%d %H:%M UTC") if log.timestamp else "N/A"
                user_email = log.user.email if log.user else "System Automation"
                action_display = _format_audit_action_name(log.action_type)
                
                target_str = ""
                if log.lg_record:
                    target_str = f" on **{log.lg_record.lg_number}**"
                elif log.entity_type:
                    target_str = f" on **{log.entity_type}**"

                ans += f"\n- `[{ts_str}]` **{action_display}**{target_str} (by: *{user_email}*)"

                if log.lg_record and len(references) < 10:
                    exp_s = log.lg_record.expiry_date.strftime("%Y-%m-%d") if log.lg_record.expiry_date else "N/A"
                    c_code = log.lg_record.lg_currency.iso_code if log.lg_record.lg_currency else "EGP"
                    references.append({
                        "lg_id": log.lg_record.id,
                        "lg_number": log.lg_record.lg_number,
                        "expiry_date": exp_s,
                        "amount": float(log.lg_record.lg_amount or 0.0),
                        "currency": c_code
                    })

            return ans, references, {"type": "audit_history", "count": len(logs)}

        if intent == "get_pending_approvals":
            reqs = query_result
            if not reqs:
                return "You currently have no pending approval requests requiring your action.", [], None
            
            ans = f"You currently have {len(reqs)} pending approval request(s) awaiting review:\n"
            for idx, r in enumerate(reqs, 1):
                maker = r.maker_user.email if r.maker_user else "System"
                act = r.action_type or "APPROVAL_REQUEST"
                ans += f"\n{idx}. Action: **{act}** (Submitted by: {maker})"
            return ans, [], None

        if intent == "get_facility_analytics":
            facs: List[IssuanceFacility] = query_result
            if not facs:
                return "No active bank credit facilities were found for your organization.", [], None

            bank_limits = {}
            fac_list = []
            for f in facs:
                b_name = f.bank.short_name or f.bank.name if f.bank else "Bank"
                c_code = f.currency.iso_code if (f.currency and f.currency.iso_code) else "EGP"
                lim = float(f.total_limit_amount) if f.total_limit_amount else 0.0
                
                fac_list.append(f"- **{f.facility_name}** ({b_name}): Limit of {lim:,.2f} {c_code}")
                if b_name not in bank_limits:
                    bank_limits[b_name] = {}
                bank_limits[b_name][c_code] = bank_limits[b_name].get(c_code, 0.0) + lim

            ans = f"Found {len(facs)} active bank credit facility limit(s):\n\n" + "\n".join(fac_list)
            visual_metadata = {"type": "facility_analytics", "total_facilities": len(facs), "bank_limits": bank_limits}
            return ans, [], visual_metadata

        if intent == "get_lg_analytics_summary":
            records: List[models.LGRecord] = query_result
            if not records:
                return "No LG records found in your portfolio.", [], None

            currency_stats = {}
            currency_counts = {}
            bank_stats = {}

            for rec in records:
                b_name, c_code, amt = _extract_record_fields(rec)

                currency_stats[c_code] = currency_stats.get(c_code, 0.0) + amt
                currency_counts[c_code] = currency_counts.get(c_code, 0) + 1
                if b_name not in bank_stats:
                    bank_stats[b_name] = 0
                bank_stats[b_name] += 1

                if len(references) < 10:
                    exp_str = rec.expiry_date.strftime("%Y-%m-%d") if rec.expiry_date else "N/A"
                    references.append({
                        "lg_id": rec.id,
                        "lg_number": _get_lg_number(rec),
                        "expiry_date": exp_str,
                        "amount": amt,
                        "currency": c_code
                    })

            # Check if query was filtered by a specific single currency
            req_curr = None
            if hasattr(self, '_current_query_params') and self._current_query_params:
                c = self._current_query_params.get("currency")
                if c:
                    req_curr = str(c).upper().strip()

            if req_curr and req_curr in currency_stats:
                total_in_curr = currency_stats[req_curr]
                count_in_curr = currency_counts.get(req_curr, 0)
                avg_val = total_in_curr / max(count_in_curr, 1)
                share_pct = round((count_in_curr / max(len(records), 1)) * 100, 1)
                ans_lines = [
                    f"**{req_curr} Portfolio Exposure**:\n",
                    f"- **Total Amount**: **{total_in_curr:,.2f} {req_curr}**",
                    f"- **Active Guarantees**: **{count_in_curr} LG(s)**",
                    f"- **Average Value**: **{avg_val:,.2f} {req_curr}**",
                    f"- **Portfolio Share**: **{share_pct}%** of active portfolio"
                ]
                ans = "\n".join(ans_lines)
            else:
                curr_lines = [f"- **{c_code}**: {amt:,.2f} ({currency_counts.get(c_code, 0)} LGs)" for c_code, amt in currency_stats.items()]
                ans = f"Portfolio Overview ({len(records)} Active LGs):\n\n**Total Exposure by Currency:**\n" + "\n".join(curr_lines)

            visual_metadata = {
                "type": "portfolio_summary",
                "total_count": len(records),
                "currencies": currency_stats,
                "top_banks": dict(list(bank_stats.items())[:5])
            }
            return ans, references, visual_metadata

        if intent in {"find_expiring_lgs", "search_lgs", "get_lg_details"}:
            records: List[models.LGRecord] = query_result
            if not records:
                return "No records matching your search criteria were found.", [], None

            # Single Record Focus (Rich card)
            if len(records) == 1 or intent == "get_lg_details":
                rec = records[0]
                b_name, c_code, amt = _extract_record_fields(rec)
                exp_str = rec.expiry_date.strftime("%Y-%m-%d") if rec.expiry_date else "N/A"
                iss_str = rec.issue_date.strftime("%Y-%m-%d") if hasattr(rec, 'issue_date') and rec.issue_date else "N/A"
                lg_num = _get_lg_number(rec)
                bene = _get_beneficiary(rec)
                owner = rec.created_by_user.email if hasattr(rec, 'created_by_user') and rec.created_by_user else "Treasury Team"
                status_name = rec.lg_status.name if hasattr(rec, 'lg_status') and rec.lg_status else "Active"

                ans_lines = [
                    f"**Guarantee Details: {lg_num}**\n",
                    f"- **Amount**: **{amt:,.2f} {c_code}**",
                    f"- **Beneficiary**: **{bene}**",
                    f"- **Issuing Bank**: **{b_name}**",
                    f"- **Expiry Date**: **{exp_str}**",
                    f"- **Issue Date**: **{iss_str}**",
                    f"- **Status**: **{status_name}**",
                    f"- **Internal Owner**: **{owner}**"
                ]
                ans = "\n".join(ans_lines)

                references.append({
                    "lg_id": rec.id,
                    "lg_number": lg_num,
                    "expiry_date": exp_str,
                    "amount": amt,
                    "currency": c_code
                })
                return ans, references, {"type": "lg_detail_card", "lg_number": lg_num}

            ans = f"Found {len(records)} Letter(s) of Guarantee matching your query:\n"
            for rec in records[:15]:
                b_name, c_code, amt = _extract_record_fields(rec)
                exp_str = rec.expiry_date.strftime("%Y-%m-%d") if rec.expiry_date else "N/A"
                lg_num = _get_lg_number(rec)

                ans += f"\n- **{lg_num}**: {amt:,.2f} {c_code} ({b_name}, Exp: {exp_str})"

                if len(references) < 10:
                    references.append({
                        "lg_id": rec.id,
                        "lg_number": lg_num,
                        "expiry_date": exp_str,
                        "amount": amt,
                        "currency": c_code
                    })

            if len(records) > 15:
                ans += f"\n\n*(Showing top 15 of {len(records)} results. Use specific filters to narrow down.)*"

            return ans, references, None

        return "Operation completed.", [], None

    def process_query(
        self,
        db: Session,
        user_question: str = "",
        customer_id: int = 1,
        user_id: int = 1,
        card_id: Optional[str] = None,
        has_all_entity_access: bool = True,
        entity_ids: List[int] = None
    ) -> Dict[str, Any]:
        """
        Main 4-Level Orchestrator with User Profile & System Self-Awareness.
        """
        if not is_ai_query_assistant_enabled():
            return {
                "success": False,
                "error": "AI Data Assistant feature is currently disabled under system configuration.",
                "code": "FEATURE_DISABLED"
            }

        logger.info(f"4-Level AI Assistant request: user_id={user_id}, customer_id={customer_id}, card_id={card_id}")

        try:
            # Build User Profile Context
            user_context = self.get_user_context(db, user_id, customer_id)

            # ==================================================================
            # LEVEL 0: SYSTEM ONLY (Frontend card_id resolution - Bypasses AI)
            # ==================================================================
            if card_id:
                is_l0, l0_config = policy_guardrail.resolve_card_id(card_id)
                if is_l0:
                    intent = l0_config["intent"]
                    params = l0_config["params"]
                    if intent == "system_help":
                        return self.handle_system_help("What are the system capabilities and modules in Grow?", user_context)
                    
                    query_result = self.execute_orm_query(db, customer_id, user_id, intent, params, has_all_entity_access, entity_ids)
                    ans, refs, vis = self.format_application_response(intent, query_result)
                    
                    return {
                        "success": True,
                        "answer": policy_guardrail.enforce_response_limit(ans),
                        "references": refs,
                        "visual_metadata": vis,
                        "level": 0,
                        "source_awareness": "SYSTEM_DATA",
                        "intent": intent
                    }

            raw_q = (user_question or "").strip()
            if not raw_q:
                return {"success": False, "error": "Question text is required.", "code": "EMPTY_QUESTION"}

            # ==================================================================
            # FAST-PATH: INSTANT GREETINGS / SHORT COMMANDS (0ms latency, No LLM call)
            # ==================================================================
            cleaned_short = raw_q.lower().strip()
            if cleaned_short in FAST_PATH_GREETINGS or (len(cleaned_short) <= 2 and cleaned_short not in {"lg", "lc", "fx"}):
                greeting_text = (
                    f"Hello! I am your **Grow Treasury & System AI Assistant**.\n\n"
                    f"Logged in as: **{user_context.get('email')}** (*{user_context.get('role_display')}* at *{user_context.get('customer_name')}*)\n\n"
                    "I can help you with:\n"
                    "- **My Activity & Audit History**: *\"What did I do recently?\"*, *\"Show organization audit logs\"*\n"
                    "- **My Role & Permissions**: *\"What can I do?\"*, *\"What are my permissions?\"*\n"
                    "- **Step-by-Step System Guides**: *\"How can I record a new LG?\"*, *\"How do I extend an LG?\"*\n"
                    "- **Portfolio & Expiry Queries**: *\"How many active LGs do we have?\"*, *\"Show LGs expiring in August\"*\n"
                    "- **Facilities & Credit Lines**: *\"What is our available facility headroom?\"*\n"
                    "- **Multi-Step Risk Analysis**: *\"Analyze our top beneficiary exposures and upcoming 90-day expiries\"*\n"
                    "- **Treasury Knowledge**: *\"What is cash pooling?\"*, *\"How do forward contracts (fwd) work?\"*"
                )
                return {
                    "success": True,
                    "answer": greeting_text,
                    "references": [],
                    "visual_metadata": {"type": "user_profile", "role": user_context.get("role")},
                    "level": 4,
                    "source_awareness": "SYSTEM_KNOWLEDGE",
                    "intent": "system_help"
                }

            # ==================================================================
            # STEP 1: AI Classifies Question & Intent
            # ==================================================================
            classification = self.classify_and_interpret(raw_q, customer_id=customer_id, user_id=user_id)
            s_level = classification.get("suggested_level", 1)
            intent = classification.get("intent", "search_lgs")
            topic = classification.get("topic", "treasury")
            params = classification.get("parameters", {})

            # ==================================================================
            # LEVEL 4: SYSTEM KNOWLEDGE & PLATFORM GUIDANCE
            # ==================================================================
            if s_level == 4 or intent == "system_help":
                return self.handle_system_help(raw_q, user_context)

            # Validate Treasury Domain Scope for Level 3 / General Questions
            if s_level == 3 or topic == "non_treasury" or intent == "general_treasury":
                is_valid_scope, scope_msg = policy_guardrail.validate_treasury_scope(raw_q, topic)
                if not is_valid_scope:
                    return {
                        "success": True,
                        "answer": "I am specialized strictly in corporate treasury, trade finance, guarantees, liquidity, and Grow platform operations. Please ask a treasury or system-related question.",
                        "references": [],
                        "level": 3,
                        "source_awareness": "GENERAL_AI_KNOWLEDGE",
                        "intent": "rejected_scope"
                    }

                # Generate Treasury Domain answer (Concise, executive-level prompt)
                prompt = f"""
You are an executive Corporate Treasury expert. Answer the following treasury or trade finance question directly, concisely, and professionally.

Strict Guidelines:
1. Keep the answer structured, clear, and direct (use bullet points where appropriate).
2. Avoid unnecessary conversational filler.
3. Keep the total response crisp and concise (under 200 words).
4. If the user asks in Arabic, answer in professional Arabic.

Question: "{raw_q}"
"""
                ai_ans = self._call_llm(prompt) or "Cash pooling is a centralized liquidity management strategy used by corporate treasuries to optimize bank balances across group entities."
                
                return {
                    "success": True,
                    "answer": policy_guardrail.enforce_response_limit(ai_ans, max_chars=3000),
                    "references": [],
                    "level": 3,
                    "source_awareness": "GENERAL_AI_KNOWLEDGE",
                    "intent": "general_treasury"
                }

            # Handle unsupported capability gap explicitly
            if intent == "unsupported":
                return {
                    "success": True,
                    "answer": "I don't currently have enough information or transactional capability to execute that action. If you need assistance navigating the system, please ask!",
                    "references": [],
                    "level": 1,
                    "source_awareness": "SYSTEM_DATA",
                    "intent": "unsupported"
                }

            # ==================================================================
            # LEVEL 1: SIMPLE AI + SYSTEM (Single Intent Mapping)
            # ==================================================================
            if s_level == 1 and intent in policy_guardrail.APPROVED_INTENTS:
                is_valid_op, reason, valid_params = policy_guardrail.validate_intent(intent, params)
                if not is_valid_op:
                    return {
                        "success": False,
                        "error": "Operation is not whitelisted.",
                        "code": "UNAPPROVED_OPERATION"
                    }

                self._current_query_params = valid_params
                query_result = self.execute_orm_query(db, customer_id, user_id, intent, valid_params, has_all_entity_access, entity_ids)
                ans, refs, vis = self.format_application_response(intent, query_result)
                
                # Cache last referenced LG for pronoun follow-ups ("this LG", "it")
                if refs and len(refs) > 0:
                    self._last_lg_context[(customer_id, user_id)] = {
                        "lg_number": refs[0].get("lg_number"),
                        "lg_id": refs[0].get("lg_id")
                    }

                return {
                    "success": True,
                    "answer": policy_guardrail.enforce_response_limit(ans),
                    "references": refs,
                    "visual_metadata": vis,
                    "level": 1,
                    "source_awareness": "SYSTEM_DATA",
                    "intent": intent
                }

            # ==================================================================
            # LEVEL 2: COMPLEX AI + SYSTEM (Multi-Step Reasoning & Authoritative Aggregations)
            # ==================================================================
            # Step A: Authoritative System Date & Horizon Windows (Application Authority)
            now = datetime.now(timezone.utc)
            today_str = now.strftime("%Y-%m-%d")
            cutoff_30_dt = now + timedelta(days=30)
            cutoff_60_dt = now + timedelta(days=60)
            cutoff_90_dt = now + timedelta(days=90)
            cutoff_180_dt = now + timedelta(days=180)

            date_context = {
                "today": today_str,
                "window_30_days": f"{today_str} to {cutoff_30_dt.strftime('%Y-%m-%d')}",
                "window_60_days": f"{today_str} to {cutoff_60_dt.strftime('%Y-%m-%d')}",
                "window_90_days": f"{today_str} to {cutoff_90_dt.strftime('%Y-%m-%d')}",
                "window_180_days": f"{today_str} to {cutoff_180_dt.strftime('%Y-%m-%d')}"
            }

            # Step B: Retrieve active LG database records
            query_result = self.execute_orm_query(db, customer_id, user_id, "get_lg_analytics_summary", {}, has_all_entity_access, entity_ids)

            # Step C: Authoritative Financial Aggregations & Groupings (Computed in Python, NOT LLM)
            portfolio_currency_totals = {}
            beneficiary_summary = {}
            bank_exposure_summary = {}
            upcoming_expiries_30 = []
            upcoming_expiries_60 = []
            upcoming_expiries_90 = []
            raw_dataset = []
            references = []

            for rec in query_result:
                b_name, c_code, amt = _extract_record_fields(rec)
                exp_dt = rec.expiry_date
                exp_str = exp_dt.strftime("%Y-%m-%d") if exp_dt else "N/A"
                lg_num = _get_lg_number(rec)
                beneficiary = _get_beneficiary(rec)

                # 1. Total by Currency
                portfolio_currency_totals[c_code] = portfolio_currency_totals.get(c_code, 0.0) + amt

                # 2. Beneficiary Grouping
                if beneficiary not in beneficiary_summary:
                    beneficiary_summary[beneficiary] = {
                        "total_exposure_by_currency": {},
                        "active_lgs_count": 0,
                        "expiring_in_90_days": []
                    }
                beneficiary_summary[beneficiary]["total_exposure_by_currency"][c_code] = (
                    beneficiary_summary[beneficiary]["total_exposure_by_currency"].get(c_code, 0.0) + amt
                )
                beneficiary_summary[beneficiary]["active_lgs_count"] += 1

                # 3. Bank Grouping
                if b_name not in bank_exposure_summary:
                    bank_exposure_summary[b_name] = {"total_by_currency": {}, "active_lgs_count": 0}
                bank_exposure_summary[b_name]["total_by_currency"][c_code] = (
                    bank_exposure_summary[b_name]["total_by_currency"].get(c_code, 0.0) + amt
                )
                bank_exposure_summary[b_name]["active_lgs_count"] += 1

                # 4. Expiry Horizons
                rec_info = {
                    "lg_number": lg_num,
                    "beneficiary": beneficiary,
                    "issuing_bank": b_name,
                    "amount": amt,
                    "currency": c_code,
                    "expiry_date": exp_str
                }
                raw_dataset.append(rec_info)

                if exp_dt:
                    if now <= exp_dt <= cutoff_30_dt:
                        upcoming_expiries_30.append(rec_info)
                    if now <= exp_dt <= cutoff_60_dt:
                        upcoming_expiries_60.append(rec_info)
                    if now <= exp_dt <= cutoff_90_dt:
                        upcoming_expiries_90.append(rec_info)
                        beneficiary_summary[beneficiary]["expiring_in_90_days"].append({
                            "lg_number": lg_num,
                            "amount": amt,
                            "currency": c_code,
                            "expiry_date": exp_str
                        })

                if len(references) < 10:
                    references.append({
                        "lg_id": rec.id,
                        "lg_number": lg_num,
                        "expiry_date": exp_str,
                        "amount": amt,
                        "currency": c_code
                    })

            # Step D: Retrieve Active Bank Facilities (if any) & calculate headroom
            facilities_result = self.execute_orm_query(db, customer_id, user_id, "get_facility_analytics", {})
            facility_summary = []
            for f in facilities_result:
                fb_name = f.bank.short_name or f.bank.name if f.bank else "Bank"
                fc_code = f.currency.iso_code if (f.currency and f.currency.iso_code) else "EGP"
                flim = float(f.total_limit_amount) if f.total_limit_amount else 0.0
                f_utilized = bank_exposure_summary.get(fb_name, {}).get("total_by_currency", {}).get(fc_code, 0.0)
                f_avail = max(0.0, flim - f_utilized)
                facility_summary.append({
                    "facility_name": f.facility_name,
                    "bank": fb_name,
                    "currency": fc_code,
                    "total_limit": flim,
                    "utilized_exposure": f_utilized,
                    "available_headroom": f_avail
                })

            # Step E: Tokenize question and dataset for Privacy
            sanitized_q, question_tokens = privacy_tokenizer.sanitize_user_question(raw_q)
            tokenized_dataset, tokenized_beneficiaries, tokenized_facilities, payload_tokens = privacy_tokenizer.tokenize_complex_payload(
                records=raw_dataset,
                beneficiary_summary=beneficiary_summary,
                facility_summary=facility_summary
            )
            all_valid_tokens = {**question_tokens, **payload_tokens}

            # Step F: LLM Synthesis with Authoritative Date, Verified Aggregates, and Tokenized Context
            prompt = f"""
You are an executive Corporate Treasury Analyst and Trade Finance Advisor.
Answer the user's multi-step analysis question using ONLY the verified facts and authoritative calculations provided below.

Sanitized Question: "{sanitized_q}"

================================================================================
1. CURRENT SYSTEM DATE & HORIZON WINDOWS (AUTHORITATIVE - USE STRICTLY):
- Today's Date: {date_context['today']} (Strictly use this exact reference date)
- Next 30 Days: {date_context['window_30_days']} (Total LGs expiring: {len(upcoming_expiries_30)})
- Next 60 Days: {date_context['window_60_days']} (Total LGs expiring: {len(upcoming_expiries_60)})
- Next 90 Days: {date_context['window_90_days']} (Total LGs expiring: {len(upcoming_expiries_90)})

2. VERIFIED TOTAL PORTFOLIO EXPOSURE (AUTHORITATIVE - DO NOT RECALCULATE):
{json.dumps(portfolio_currency_totals, indent=2)}

3. VERIFIED BENEFICIARY EXPOSURES & 90-DAY UPCOMING EXPIRIES:
{json.dumps(tokenized_beneficiaries, indent=2)}

4. VERIFIED BANK CREDIT FACILITIES & AVAILABLE HEADROOM (AUTHORITATIVE - REPORT AS-IS):
{json.dumps(tokenized_facilities, indent=2)}

5. TOKENIZED RECORD SAMPLE ({len(tokenized_dataset)} records):
{json.dumps(tokenized_dataset[:30], indent=2)}
================================================================================

Strict Rules:
1. DATE ACCURACY: Today's date is strictly {date_context['today']}. NEVER assume, invent, or state any other date (e.g. 2024, May 2024, March 2026, etc.). All expiry horizons start strictly from {date_context['today']}.
2. AUTHORITATIVE TOTALS: All portfolio totals, beneficiary sums, and facility available_headroom figures are pre-calculated by the application. Use and report these exact verified figures. Do NOT perform independent manual arithmetic.
3. ENTITY TOKENS: Refer to entities using ONLY their supplied tokens (e.g. LG_TOKEN_001, BENEFICIARY_TOKEN_001, BANK_TOKEN_001).
4. TREASURY JUDGMENT: When asked for recommendations or risk assessment, provide crisp executive-grade treasury insights regarding counterparty concentration, currency exposure, refinancing/renewal risk, and actionable mitigation strategies.
"""
            raw_synthesis = self._call_llm(prompt) or "Analyzed portfolio exposure based on requested criteria."

            # Step G: Output Token Validation
            is_valid_output, val_msg = privacy_tokenizer.validate_ai_output_tokens(raw_synthesis, all_valid_tokens)
            if not is_valid_output:
                logger.error(f"Level 2 Output Validation Rejected: {val_msg}")
                ans, refs, vis = self.format_application_response("get_lg_analytics_summary", query_result)
                return {
                    "success": True,
                    "answer": policy_guardrail.enforce_response_limit(ans),
                    "references": refs,
                    "visual_metadata": vis,
                    "level": 2,
                    "source_awareness": "SYSTEM_DATA",
                    "intent": "complex_analysis"
                }

            # Step H: Detokenize
            detokenized_ans = privacy_tokenizer.detokenize_response(raw_synthesis, all_valid_tokens)

            # Step I: Date Hallucination Post-Validation Scan (Preventive Guardrail)
            current_year = str(now.year)
            if re.search(r'(?:assuming\s+today[\'’]s\s+date\s+is\s+(?:2024|2025|May\s+14)|today[\'’]s\s+date\s+is\s+(?:2024|2025))', detokenized_ans, re.IGNORECASE):
                logger.warning("Preventive Date Hallucination Guardrail Triggered. Falling back to structured response.")
                ans, refs, vis = self.format_application_response("get_lg_analytics_summary", query_result)
                detokenized_ans = f"**Portfolio & Expiry Analysis (As of {today_str}):**\n\n" + ans

            return {
                "success": True,
                "answer": policy_guardrail.enforce_response_limit(detokenized_ans, max_chars=4000),
                "references": references,
                "visual_metadata": None,
                "level": 2,
                "source_awareness": "COMBINATION",
                "intent": "complex_analysis"
            }

        except Exception as e:
            logger.error(f"Error in 4-Level AI Assistant: {e}", exc_info=True)
            return {
                "success": False,
                "error": "An error occurred while processing your request.",
                "code": "INTERNAL_ERROR"
            }


ai_query_assistant_service = AIQueryAssistantService()
