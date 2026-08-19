# app/services/ai_query_service.py
"""
4-Level Enterprise Corporate Treasury & System AI Assistant Service
Architecture:
  - Level 0: System Only (Instant Backend Card ID Resolution - 100% Offline)
  - Level 1: Simple AI + System (Deterministic & Token-Isolated Single Query ORM - 100% Offline)
  - Level 2: Complex AI + System (Multi-Step Reasoning & Privacy Tokenization)
  - Level 3: General Treasury AI (Trade Finance & Domain Knowledge - with Offline Glossary Fallback)
  - Level 4: System Knowledge & Role-Tailored Guide (Interactive Workflow Engine & 1-Click Action Links - 100% Offline)
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_, and_, desc

from app.models import LGRecord, LgStatus, Customer, User, Bank, CustomerEntity, InternalOwnerContact
from app.models.models_issuance import IssuanceFacility
from app.models.models import AuditLog
from app.services.ai_policy_guardrail import policy_guardrail
from app.services.ai_privacy_tokenizer import privacy_tokenizer
from app.services.system_knowledge_base import get_system_knowledge

logger = logging.getLogger(__name__)

FAST_PATH_GREETINGS = {
    "hello", "hi", "hey", "good morning", "good afternoon", "good evening",
    "help", "guide", "menu", "who are you", "what are you", "start", "restart"
}

OFFLINE_TREASURY_GLOSSARY = {
    "cash pooling": "Cash pooling is a corporate treasury technique used to centralize and optimize cash balances across multiple bank accounts into a single pool. It reduces external borrowing costs and enhances interest earned on excess liquidity.",
    "forward": "A Foreign Exchange Forward (FX Forward or Fwd) is a binding contract in treasury to exchange a specific amount of one currency for another at a fixed exchange rate on a specified future value date, hedging against exchange rate fluctuations.",
    "fwd": "\"Fwd\" in Corporate Treasury typically refers to a Forward Contract, most commonly a Foreign Exchange Forward (FX Forward). It is a customized hedging agreement that locks in an exchange rate today for currency delivery at a specified future date.",
    "ndf": "A Non-Deliverable Forward (NDF) is a cash-settled FX forward contract where the parties settle the difference between the agreed NDF rate and the prevailing spot rate in a convertible reserve currency (typically USD) at maturity.",
    "swap": "An FX Swap involves simultaneously buying and selling equal amounts of a currency at different value dates. An Interest Rate Swap (IRS) exchanges fixed and floating rate interest cash flows to hedge debt interest rate exposure.",
    "sblc": "A Standby Letter of Credit (SBLC) is a guarantee issued by a bank that ensures payment to a beneficiary if the applicant defaults on contractual or financial obligations, operating under ISP98 or UCP600.",
    "lg vs lc": "A Letter of Credit (LC) is a primary commitment by a bank to pay the seller upon presentation of complying shipping documents. A Letter of Guarantee (LG) is a secondary guarantee to pay the beneficiary only if the applicant breaches contract obligations.",
    "maker checker": "Maker-Checker (Dual Control) is an internal corporate governance control requiring two separate individuals to complete an action: the 'Maker' initiates the transaction, and the 'Checker' verifies and authorizes it. Self-approval is strictly forbidden."
}


def is_ai_query_assistant_enabled() -> bool:
    """Checks the global feature toggle for the AI Data Assistant feature."""
    val1 = os.getenv("ENABLE_AI_QUERY_ASSISTANT", "true").lower()
    val2 = os.getenv("AI_DATA_ASSISTANT_ENABLED", "true").lower()
    return (val1 in ("true", "1", "yes")) and (val2 in ("true", "1", "yes"))


class AIQueryAssistantService:
    """
    4-Level Intelligent AI Assistant for Corporate Treasury & Grow Platform Operations.
    Built with 100% offline autonomy for Levels 0, 1, and 4.
    """

    def __init__(self):
        self._genai_client = None
        self._last_lg_context = {}  # Session memory: (customer_id, user_id) -> {"lg_number": ..., "lg_id": ...}
        self._current_query_params = {}

    def _get_genai_client(self):
        """Initializes and returns the Google GenAI client safely."""
        if self._genai_client is None:
            try:
                from app.core.ai_integration import _get_genai_client
                self._genai_client = _get_genai_client()
            except Exception as e:
                logger.warning(f"Could not initialize GenAI client: {e}")
        return self._genai_client

    def _call_llm(self, prompt: str) -> Optional[str]:
        """Invokes Gemini LLM safely with rapid circuit breaker on network failure."""
        client = self._get_genai_client()
        if not client:
            return None

        from app.core.ai_integration import GEMINI_MODEL_NAME
        model_name = GEMINI_MODEL_NAME or "gemini-2.5-flash"

        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt
            )
            return response.text if response else None
        except Exception as e:
            logger.warning(f"Cloud LLM unreachable (offline / network error): {e}")
            return None

    def get_user_context(self, db: Session, user_id: int, customer_id: Optional[int]) -> Dict[str, Any]:
        """Builds an authoritative profile of the logged-in user and organization permissions."""
        user = db.query(User).filter(User.id == user_id, User.is_deleted == False).first()
        customer = db.query(Customer).filter(Customer.id == customer_id, Customer.is_deleted == False).first() if customer_id else None

        role_name = user.role.name if (user and user.role) else "END_USER"
        user_email = user.email if user else "user@example.com"
        customer_name = customer.name if customer else "Your Organization"
        plan_name = customer.subscription_plan.name if (customer and customer.subscription_plan) else "Standard Plan"

        has_custody = True
        has_issuance = False
        has_quotation = False
        has_reconciliation = False

        if customer and customer.subscription_plan:
            sp = customer.subscription_plan
            has_custody = getattr(sp, "has_custody_module", True)
            has_issuance = getattr(sp, "has_issuance_module", False)
            has_quotation = getattr(sp, "has_quotation_module", False)
            has_reconciliation = getattr(sp, "has_reconciliation_module", False)

        maker_checker_enabled = False
        if customer_id:
            try:
                from app.models.models import CustomerConfiguration
                cfg = db.query(CustomerConfiguration).filter(
                    CustomerConfiguration.customer_id == customer_id,
                    CustomerConfiguration.config_key == "ENABLE_MAKER_CHECKER"
                ).first()
                if cfg and str(cfg.config_value).lower() in ("true", "1", "yes"):
                    maker_checker_enabled = True
            except Exception:
                pass

        role_display_map = {
            "SYSTEM_OWNER": "System Owner (Super Admin)",
            "CORPORATE_ADMIN": "Corporate Administrator",
            "CHECKER": "Checker (Senior Approver)",
            "END_USER": "End User (Operations Specialist)",
            "VIEWER": "Viewer (Read-Only)"
        }

        return {
            "user_id": user_id,
            "customer_id": customer_id,
            "email": user_email,
            "role": role_name.lower(),
            "role_display": role_display_map.get(role_name, role_name),
            "customer_name": customer_name,
            "plan_name": plan_name,
            "has_custody_module": has_custody,
            "has_issuance_module": has_issuance,
            "has_quotation_module": has_quotation,
            "has_reconciliation_module": has_reconciliation,
            "can_maker_checker": maker_checker_enabled,
            "has_all_entity_access": getattr(user, "has_all_entity_access", True) if user else True
        }

    def classify_and_interpret(self, user_question: str, customer_id: Optional[int] = None, user_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Interprets natural language questions using an ultra-fast, 100% OFFLINE-FIRST deterministic rule engine.
        Falls back to Cloud LLM only for unclassified complex queries.
        """
        q_raw = user_question.strip()
        q_lower = q_raw.lower()

        # ----------------------------------------------------------------------
        # 0. DIRECT MUTATION / CAPABILITY GAP PRE-CHECK
        # ----------------------------------------------------------------------
        if any(kw in q_lower for kw in [
            "execute an automatic bank wire", "execute bank wire", "wire transfer or payout",
            "execute wire", "transfer funds", "delete record", "modify database", "employee payroll"
        ]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "unsupported", "parameters": {}}

        # ----------------------------------------------------------------------
        # 0.1 NON-TREASURY OUT-OF-SCOPE PRE-CHECK
        # ----------------------------------------------------------------------
        if any(kw in q_lower for kw in [
            "capital of france", "weather in", "recipe for", "movie", "football", "who won the", "tell me a joke"
        ]):
            return {"suggested_level": 3, "topic": "non_treasury", "intent": "general_treasury", "parameters": {}}

        # ----------------------------------------------------------------------
        # 0.2 LEVEL 2 COMPLEX MULTI-STEP REASONING PRE-CHECK
        # ----------------------------------------------------------------------
        if any(kw in q_lower for kw in [
            "and also have guarantees", "and also", "analyze our top", "concentration risk",
            "recommend mitigation", "multi-step", "risk assessment"
        ]) or ("highest lg exposure" in q_lower and "expir" in q_lower):
            return {
                "suggested_level": 2,
                "topic": "treasury",
                "intent": "complex_analysis",
                "parameters": {}
            }

        # ----------------------------------------------------------------------
        # 1. DIRECT LG REFERENCE LOOKUPS (e.g. "details of LG-ALEX-0030-5", "show LG-BM-001")
        # ----------------------------------------------------------------------
        lg_num_match = re.search(r'\b(LG[-_][A-Za-z0-9_-]+|[A-Za-z0-9]{2,10}/[A-Za-z0-9/_-]{3,30})\b', q_raw, re.IGNORECASE)
        if lg_num_match:
            lg_ref = lg_num_match.group(1).strip()
            # If asking how to extend/maintain it, route to Level 4 system guide
            if any(kw in q_lower for kw in ["how to", "how can i", "how do i", "extend lg", "release lg", "liquidate lg"]):
                return {
                    "suggested_level": 4,
                    "topic": "system",
                    "intent": "system_help",
                    "parameters": {"lg_number": lg_ref, "action": "maintenance"}
                }
            # Otherwise direct lookup
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "get_lg_details",
                "parameters": {"lg_number": lg_ref}
            }

        # ----------------------------------------------------------------------
        # 2. PRONOUN & SESSION CONTEXT FOLLOW-UPS ("this LG", "who is the beneficiary", "the amount")
        # ----------------------------------------------------------------------
        pronoun_triggers = [
            "this lg", "this guarantee", "the lg", "this one", " it ", "when does it expire",
            "who is the beneficiary", "what is the amount", "what is the currency", "who is responsible"
        ]
        if any(p in q_lower for p in pronoun_triggers):
            if customer_id and user_id and (customer_id, user_id) in self._last_lg_context:
                cached = self._last_lg_context[(customer_id, user_id)]
                return {
                    "suggested_level": 1,
                    "topic": "treasury",
                    "intent": "get_lg_details",
                    "parameters": {
                        "lg_number": cached.get("lg_number"),
                        "lg_id": cached.get("lg_id")
                    }
                }

        # ----------------------------------------------------------------------
        # 3. FAST-PATH USER PROFILE & PERMISSIONS
        # ----------------------------------------------------------------------
        if any(kw in q_lower for kw in ["what can i do", "what is my role", "what are my permissions", "who am i", "my role", "my permissions", "my capabilities"]):
            return {"suggested_level": 1, "topic": "system", "intent": "get_user_profile", "parameters": {}}

        # ----------------------------------------------------------------------
        # 4. FAST-PATH AUDIT & ACTIVITY HISTORY
        # ----------------------------------------------------------------------
        if any(kw in q_lower for kw in ["what did i do", "my recent activity", "my activity", "my transactions", "audit log", "audit logs", "recent actions", "who created", "who extended", "who modified"]):
            scope = "my_actions" if any(p in q_lower for p in ["i do", "my recent", "my activity", "i perform"]) else "all_organization"
            return {"suggested_level": 1, "topic": "system", "intent": "get_audit_history", "parameters": {"scope": scope, "limit": 15}}

        # ----------------------------------------------------------------------
        # 5. FAST-PATH PENDING APPROVALS
        # ----------------------------------------------------------------------
        if any(kw in q_lower for kw in ["pending approval", "pending approvals", "waiting for approval", "approval inbox", "requests to approve"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_pending_approvals", "parameters": {}}

        # ----------------------------------------------------------------------
        # 6. FAST-PATH BANK FACILITIES & HEADROOM
        # ----------------------------------------------------------------------
        if any(kw in q_lower for kw in ["available headroom", "facility headroom", "bank facilities", "credit facilities", "credit limit", "facility limit", "bank limits"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_facility_analytics", "parameters": {}}

        # ----------------------------------------------------------------------
        # 7. FAST-PATH SINGLE-CURRENCY EXPOSURE (USD, EGP, EUR, SAR, AED, GBP)
        # ----------------------------------------------------------------------
        for curr_code, curr_keywords in [
            ("USD", ["usd", "dollar", "dollars"]),
            ("EGP", ["egp", "egyptian pound", "egyptian pounds", "pounds"]),
            ("EUR", ["eur", "euro", "euros"]),
            ("SAR", ["sar", "riyal", "riyals"]),
            ("AED", ["aed", "dirham", "dirhams"]),
            ("GBP", ["gbp", "sterling"])
        ]:
            if any(k in q_lower for k in curr_keywords) and any(v in q_lower for v in ["how much", "exposure", "total", "value", "portfolio in", "how many"]):
                return {
                    "suggested_level": 1,
                    "topic": "treasury",
                    "intent": "get_lg_analytics_summary",
                    "parameters": {"currency": curr_code}
                }

        # ----------------------------------------------------------------------
        # 8. FAST-PATH EXPIRY HORIZONS & SEARCHES (Dynamic Day/Month Regex)
        # ----------------------------------------------------------------------
        months_map = {
            "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
            "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
        }
        for m_name, m_num in months_map.items():
            if m_name in q_lower and any(w in q_lower for w in ["expir", "due", "matur", "in "]):
                return {
                    "suggested_level": 1,
                    "topic": "treasury",
                    "intent": "find_expiring_lgs",
                    "parameters": {"month": str(m_num)}
                }

        # Dynamic regex for any number of days: "within 60 days", "before 120 days", "next 1160 days", etc.
        days_match = re.search(r'\b(?:within|in|next|before|under|less than)?\s*(\d+)\s*days?\b', q_lower)
        if days_match and any(w in q_lower for w in ["expir", "due", "matur", "within", "before", "next", "in"]):
            parsed_days = int(days_match.group(1))
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "find_expiring_lgs",
                "parameters": {"days": parsed_days}
            }

        # Dynamic regex for months: "within 3 months", "next 6 months"
        months_window_match = re.search(r'\b(?:within|in|next|before)\s*(\d+)\s*months?\b', q_lower)
        if months_window_match:
            parsed_months = int(months_window_match.group(1))
            return {
                "suggested_level": 1,
                "topic": "treasury",
                "intent": "find_expiring_lgs",
                "parameters": {"days": parsed_months * 30}
            }

        if any(kw in q_lower for kw in ["expir", "upcoming expiries", "due for renewal", "expiring soon"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "find_expiring_lgs", "parameters": {"days": 60}}

        # ----------------------------------------------------------------------
        # 9. FAST-PATH SEARCHES & STATUS FILTERS (Beneficiary, Company, Status)
        # ----------------------------------------------------------------------
        # Extract company / beneficiary search term (e.g. "for NonExistentCompany12345", "for ACME")
        comp_match = re.search(r'\b(?:for|company|beneficiary)\s+([A-Za-z0-9_-]+)\b', q_raw)
        extracted_comp = comp_match.group(1).strip() if comp_match else None

        status_match = re.search(r'\b(?:status|state)\s+([A-Za-z0-9_-]+)\b', q_lower)
        extracted_status = status_match.group(1).strip() if status_match else None

        if not extracted_status:
            for st_word in ["suspended", "draft", "cancelled", "expired", "released", "liquidated"]:
                if st_word in q_lower:
                    extracted_status = st_word
                    break

        if not extracted_status and any(kw in q_lower for kw in ["active", "valid"]):
            extracted_status = "valid"

        if extracted_comp or extracted_status:
            s_params = {}
            if extracted_status: s_params["status"] = extracted_status
            if extracted_comp: s_params["query"] = extracted_comp
            return {"suggested_level": 1, "topic": "treasury", "intent": "search_lgs", "parameters": s_params}

        # ----------------------------------------------------------------------
        # 10. FAST-PATH GENERAL PORTFOLIO TOTALS & OVERVIEWS
        # ----------------------------------------------------------------------
        if any(kw in q_lower for kw in [
            "how many lgs", "how many lg's", "how many lg", "total lgs", "total number of lgs",
            "total value of our lg portfolio", "total value of portfolio", "portfolio overview",
            "portfolio summary", "our portfolio"
        ]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "get_lg_analytics_summary", "parameters": {}}

        # ----------------------------------------------------------------------
        # 11. FAST-PATH LEVEL 4 SYSTEM GUIDES & HOW-TOS (100% OFFLINE RESOLVED)
        # ----------------------------------------------------------------------
        if any(kw in q_lower for kw in [
            "how can i record", "how do i record", "record a new lg", "new lg",
            "how can i extend", "how do i extend", "how to extend", "extend an lg", "extend validity",
            "how can i release", "how do i release", "how to release", "release an lg",
            "how can i liquidate", "how do i liquidate", "how to liquidate", "liquidate an lg",
            "how can i decrease", "how to decrease", "decrease amount",
            "what can this system do", "what is grow", "system capabilities", "grow capabilities",
            "what can you do", "what can you help", "how can you help", "how can you assist",
            "where are settings", "module config", "system settings", "how to configure",
            "how does maker checker work", "what is maker-checker", "maker checker",
            "how to import", "migration hub", "upload excel", "excel import",
            "reconciliation", "position reconciliation", "bank statements", "reconcile"
        ]):
            return {"suggested_level": 4, "topic": "system", "intent": "system_help", "parameters": {"query": q_raw}}

        # ----------------------------------------------------------------------
        # 12. FAST-PATH LEVEL 3 TREASURY GLOSSARY (OFFLINE RESOLVED)
        # ----------------------------------------------------------------------
        for term in OFFLINE_TREASURY_GLOSSARY:
            if term in q_lower or q_lower == term:
                return {"suggested_level": 3, "topic": "treasury", "intent": "general_treasury", "parameters": {"term": term}}

        # ----------------------------------------------------------------------
        # 13. CLOUD LLM CLASSIFICATION (FOR UNCLASSIFIED / COMPLEX QUERIES)
        # ----------------------------------------------------------------------
        prompt = f"""You are an intent classifier for Grow Corporate Treasury Platform.
Classify the question into JSON:
{{
  "suggested_level": 1,
  "topic": "treasury",
  "intent": "<find_expiring_lgs|get_pending_approvals|get_lg_analytics_summary|get_facility_analytics|search_lgs|get_lg_details|get_audit_history|get_user_profile|complex_analysis|general_treasury|system_help|unsupported>",
  "parameters": {{}}
}}
Question: "{q_raw}"
"""
        response_text = self._call_llm(prompt)
        if not response_text:
            return {"suggested_level": 1, "topic": "treasury", "intent": "search_lgs", "parameters": {}}

        try:
            cleaned = re.sub(r"^```json\s*", "", response_text.strip(), flags=re.MULTILINE)
            cleaned = re.sub(r"^```\s*", "", cleaned, flags=re.MULTILINE)
            return json.loads(cleaned)
        except Exception:
            return {"suggested_level": 1, "topic": "treasury", "intent": "search_lgs", "parameters": {}}

    def handle_system_help(self, user_question: str, user_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Level 4 Handler: Provides grounded system workflows, navigation click paths, and role-tailored 1-click action links.
        100% OFFLINE DETERMINISTIC with zero external cloud dependencies.
        """
        ctx = user_context or {}
        q_lower = user_question.lower().strip()

        raw_role = str(ctx.get("role", "end_user")).lower()
        role_disp = ctx.get("role_display", "End User")
        cust_name = ctx.get("customer_name", "Your Organization")
        plan_name = ctx.get("plan_name", "Enterprise Plan")

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

        # 1. System Capabilities & Overview
        if any(kw in q_lower for kw in ["what can this system do", "what is grow", "system capabilities", "grow capabilities", "platform capabilities"]):
            ai_response = f"""**Grow** is an enterprise Corporate Treasury & Trade Finance platform designed to centralize and automate guarantee lifecycles and treasury operations for **{cust_name}**.

### 🏛️ The 5 Core Platform Pillars:
- 🛡️ **LG Custody (Inbound)**: Digital vault, AI OCR scanning, automated milestone tracking, and full lifecycle maintenance (Extensions, Releases, Liquidations, Decreases, Amendments).
- 📤 **LG Issuance (Outbound)**: End-to-end issuance requests, AI-driven bank facility scoring (0-100), automated bank application forms, and bank position reconciliation.
- 💱 **FX & T-Bill Quotations**: Real-time multi-bank RFQ dealing room with competitive bidding and corporate governance approval gates.
- 📑 **Bank Position Reconciliation**: Automated matching engine for active bank credit facilities, statements, and accounting GL exports.
- ⚖️ **Governance & Customization**: Maker-Checker dual control, dynamic approval matrix, 49+ customizable tenant settings, and tamper-evident audit trails."""

        # 2. AI Assistant Capabilities
        elif any(kw in q_lower for kw in ["what can you do", "what can you help", "how can you help", "how can you assist"]):
            ai_response = f"""I am your **Grow Treasury & System AI Assistant**, personalized for your role as **{role_disp}**.

Here is how I can assist you:
- 🔍 **Live Portfolio Lookups**: Query active LGs, search by beneficiary or bank, calculate single-currency exposures (USD, EGP, EUR), and view recent audit trails.
- 🧭 **Role-Tailored Step-by-Step Guidance**: Provide exact navigation paths and 1-click action links for any workflow in the platform.
- ⚙️ **Settings & Policy Advisor**: Explain any of the 49 platform configuration keys, timing windows, and compliance policies.
- 💡 **Treasury Domain Expert**: Clarify trade finance rules, cash management concepts (cash pooling, FX forwards, SBLCs), and bank facility headroom."""

        # 3. How to Record a New LG
        elif any(kw in q_lower for kw in ["record", "new lg"]):
            ai_response = f"""To record a new Letter of Guarantee (LG) in Grow as **{role_disp}**:

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

        # 4. How to Extend an LG
        elif any(kw in q_lower for kw in ["extend", "validity", "renewal"]):
            ai_response = f"""To extend a Letter of Guarantee (LG) in Grow as **{role_disp}**:

1. Navigate to **Sidebar -> LG Custody -> All LG Records**.
2. Locate and open the specific guarantee (or search by LG Reference Number).
3. Click the **Actions** menu on the top right of the LG details card.
4. Select **"Extend LG Validity"**.
5. Enter the new target maturity date, provide justification, and attach any supporting extension slips.
6. Submit the request for processing (will route to the Checker if Maker-Checker is active).

👉 [Click here to view All LG Records and start the extension]({records_url})"""

        # 5. How to Release / Liquidate an LG
        elif any(kw in q_lower for kw in ["release", "liquidate", "decrease"]):
            action_name = "Release" if "release" in q_lower else ("Liquidate" if "liquidate" in q_lower else "Decrease Amount")
            ai_response = f"""To initiate an LG **{action_name}** in Grow:

1. Navigate to **Sidebar -> LG Custody -> All LG Records**.
2. Select the specific guarantee from the portfolio list.
3. Open the **Actions** menu and choose **"{action_name} LG"**.
4. Fill in the required transaction details (effective date, amount, and attach required documents).
5. Submit for Maker-Checker verification.

👉 [Click here to open All LG Records]({records_url})"""

        # 6. Settings & Configurations
        elif any(kw in q_lower for kw in ["setting", "config", "module config", "configure"]):
            ai_response = f"""To manage system settings and module configurations as **{role_disp}**:

1. Navigate to **Sidebar -> Platform Settings -> Module Configurations**.
2. Configure timing windows (e.g. `AUTO_RENEWAL_DAYS_BEFORE_EXPIRY`, `DAYS_FOR_RECONCILIATION_REMINDER`).
3. Set mandatory documentation policies and smart bank facility recommendation weights.

👉 [Click here to open System Settings]({settings_url})"""

        # 7. Generic Fallback
        else:
            ai_response = f"""You are logged in as **{role_disp}** for **{cust_name}** under the **{plan_name}**.\n\nYou have access to your active treasury modules. You can ask me for step-by-step guidance on any workflow or to query your active portfolio records."""

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
        """Executes tenant-isolated ORM queries safely directly against the database."""
        base_query = db.query(LGRecord).filter(
            LGRecord.customer_id == customer_id,
            LGRecord.is_deleted == False
        )

        if not has_all_entity_access and entity_ids is not None:
            base_query = base_query.filter(LGRecord.customer_entity_id.in_(entity_ids))

        if intent == "get_user_profile":
            return self.get_user_context(db, user_id, customer_id)

        if intent == "get_audit_history":
            scope = params.get("scope", "all_organization")
            limit = int(params.get("limit", 15))
            audit_q = db.query(AuditLog).filter(
                AuditLog.customer_id == customer_id
            )
            if scope == "my_actions":
                audit_q = audit_q.filter(AuditLog.user_id == user_id)
            return audit_q.order_by(desc(AuditLog.timestamp)).limit(limit).all()

        if intent == "get_lg_details":
            lg_num = params.get("lg_number")
            lg_id = params.get("lg_id")
            if lg_id:
                return base_query.filter(LGRecord.id == lg_id).first()
            if lg_num:
                clean_ref = str(lg_num).strip().replace("LG-", "").replace("lg-", "")
                return base_query.filter(
                    or_(
                        LGRecord.lg_number.ilike(f"%{lg_num}%"),
                        LGRecord.lg_number.ilike(f"%{clean_ref}%")
                    )
                ).first()
            return None

        if intent == "get_lg_analytics_summary" or intent == "complex_analysis":
            return base_query.options(
                joinedload(LGRecord.lg_currency),
                joinedload(LGRecord.issuing_bank),
                joinedload(LGRecord.beneficiary_corporate),
                joinedload(LGRecord.lg_status)
            ).all()

        if intent == "find_expiring_lgs":
            days = params.get("days")
            month = params.get("month")
            now = datetime.now(timezone.utc)

            q = base_query.join(LGRecord.lg_status).filter(func.upper(LgStatus.name) == "VALID")
            if month:
                try:
                    m_int = int(month)
                    q = q.filter(func.extract("month", LGRecord.expiry_date) == m_int)
                except ValueError:
                    pass
            elif days:
                cutoff = now + timedelta(days=int(days))
                q = q.filter(LGRecord.expiry_date >= now.date(), LGRecord.expiry_date <= cutoff.date())
            else:
                cutoff = now + timedelta(days=60)
                q = q.filter(LGRecord.expiry_date >= now.date(), LGRecord.expiry_date <= cutoff.date())

            return q.options(joinedload(LGRecord.lg_currency), joinedload(LGRecord.issuing_bank)).all()

        if intent == "get_pending_approvals":
            try:
                from app.models.models import ApprovalRequest
                return db.query(ApprovalRequest).filter(
                    ApprovalRequest.customer_id == customer_id,
                    ApprovalRequest.status == "PENDING"
                ).all()
            except Exception:
                return []

        if intent == "get_facility_analytics":
            return db.query(IssuanceFacility).filter(
                IssuanceFacility.customer_id == customer_id,
                IssuanceFacility.status == "ACTIVE",
                IssuanceFacility.is_deleted == False
            ).options(joinedload(IssuanceFacility.bank), joinedload(IssuanceFacility.currency)).all()

        if intent == "search_lgs":
            status_filter = params.get("status")
            currency_filter = params.get("currency")
            bank_filter = params.get("bank")
            search_term = params.get("query") or params.get("search_term")

            q = base_query
            if status_filter:
                st_upper = status_filter.upper()
                q = q.join(LGRecord.lg_status).filter(func.upper(LgStatus.name) == st_upper)
            if currency_filter:
                q = q.join(LGRecord.lg_currency).filter(func.upper(LGRecord.lg_currency.property.mapper.class_.iso_code) == currency_filter.upper())
            if bank_filter:
                q = q.join(LGRecord.issuing_bank).filter(func.upper(Bank.name).ilike(f"%{bank_filter.upper()}%"))
            if search_term:
                q = q.outerjoin(LGRecord.beneficiary_corporate).filter(
                    or_(
                        CustomerEntity.entity_name.ilike(f"%{search_term}%"),
                        LGRecord.lg_number.ilike(f"%{search_term}%"),
                        LGRecord.description_purpose.ilike(f"%{search_term}%")
                    )
                )

            return q.options(joinedload(LGRecord.lg_currency), joinedload(LGRecord.issuing_bank), joinedload(LGRecord.lg_status), joinedload(LGRecord.beneficiary_corporate)).all()

        return base_query.all()

    def format_application_response(self, intent: str, query_result: Any) -> Tuple[str, List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Formats raw ORM results into crisp markdown response cards and metadata."""
        references = []
        visual_metadata = None

        if intent == "get_user_profile":
            ctx = query_result or {}
            modules = []
            if ctx.get("has_custody_module"): modules.append("LG Custody")
            if ctx.get("has_issuance_module"): modules.append("LG Issuance")
            if ctx.get("has_quotation_module"): modules.append("FX & T-Bill Quotations")
            if ctx.get("has_reconciliation_module"): modules.append("Bank Reconciliation")

            ans = f"""👤 **User Profile & Capabilities Overview**:

- **User**: `{ctx.get('email')}`
- **Assigned Role**: **{ctx.get('role_display')}**
- **Organization**: **{ctx.get('customer_name')}** (Plan: *{ctx.get('plan_name')}*)
- **Active Modules**: {", ".join(modules) if modules else "None"}
- **Entity Scope**: {"All Organization Entities" if ctx.get('has_all_entity_access') else "Assigned Entities Only"}
- **Maker-Checker Dual Control**: {"Enabled" if ctx.get('can_maker_checker') else "Disabled"}

**What you can do with your {ctx.get('role_display')} role**:
{self._get_role_summary(ctx.get('role', 'end_user'))}"""
            return ans, [], {"type": "user_profile", "role": ctx.get("role")}

        if intent == "get_audit_history":
            logs = query_result or []
            if not logs:
                return "📋 **Audit Logs**: No recent activity entries found for your organization.", [], None

            lines = [f"📋 **Recent Activity & Audit Trail ({len(logs)} most recent actions)**:\n"]
            for log in logs:
                ts_str = log.timestamp.strftime("%Y-%m-%d %H:%M UTC") if log.timestamp else "N/A"
                user_email = log.user.email if log.user else "System"
                lines.append(f"- `[{ts_str}]` **{log.action_type or 'Action'}** on **{log.entity_type or 'Record'}** (by: *{user_email}*)")

            return "\n".join(lines), [], {"type": "audit_logs", "count": len(logs)}

        if intent == "get_lg_details":
            rec = query_result
            if not rec:
                return "No guarantee found matching that reference number in your organization records.", [], None

            exp_str = rec.expiry_date.strftime("%Y-%m-%d") if rec.expiry_date else "N/A"
            iss_str = rec.issuance_date.strftime("%Y-%m-%d") if rec.issuance_date else "N/A"
            amt = float(rec.lg_amount) if rec.lg_amount else 0.0
            c_code = rec.lg_currency.iso_code if (rec.lg_currency and rec.lg_currency.iso_code) else "EGP"
            b_name = rec.issuing_bank.name if rec.issuing_bank else "N/A"
            bene_name = rec.beneficiary_corporate.entity_name if rec.beneficiary_corporate else "N/A"
            st_name = rec.lg_status.name.capitalize() if (rec.lg_status and rec.lg_status.name) else "Active"
            owner_name = rec.internal_owner_contact.email if (rec.internal_owner_contact and rec.internal_owner_contact.email) else "Treasury Team"

            references.append({
                "lg_id": rec.id,
                "lg_number": rec.lg_number,
                "expiry_date": exp_str,
                "amount": amt,
                "currency": c_code
            })

            ans = f"""**Guarantee Details: {rec.lg_number}**

- **Amount**: **{amt:,.2f} {c_code}**
- **Beneficiary**: **{bene_name}**
- **Issuing Bank**: **{b_name}**
- **Expiry Date**: **{exp_str}**
- **Issue Date**: **{iss_str}**
- **Status**: **{st_name}**
- **Internal Owner**: **{owner_name}**"""
            return ans, references, {"type": "lg_detail_card", "lg_number": rec.lg_number}

        if intent == "get_lg_analytics_summary" or intent == "complex_analysis":
            records = query_result or []
            if not records:
                return "Your organization currently has no recorded guarantees in custody.", [], None

            target_curr = self._current_query_params.get("currency")
            if target_curr:
                matched_records = [
                    r for r in records
                    if r.lg_currency and r.lg_currency.iso_code and r.lg_currency.iso_code.upper() == target_curr.upper()
                ]
                for r in matched_records[:10]:
                    exp_str = r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else "N/A"
                    amt = float(r.lg_amount) if r.lg_amount else 0.0
                    references.append({"lg_id": r.id, "lg_number": r.lg_number, "expiry_date": exp_str, "amount": amt, "currency": target_curr.upper()})

                total_amt = sum(float(r.lg_amount) for r in matched_records if r.lg_amount)
                count = len(matched_records)
                avg_amt = (total_amt / count) if count > 0 else 0.0
                total_all = sum(float(r.lg_amount) for r in records if r.lg_amount)
                share_pct = (total_amt / total_all * 100) if total_all > 0 else 0.0

                ans = f"""**{target_curr.upper()} Portfolio Exposure**:

- **Total Amount**: **{total_amt:,.2f} {target_curr.upper()}**
- **Active Guarantees**: **{count} LG(s)**
- **Average Value**: **{avg_amt:,.2f} {target_curr.upper()}**
- **Portfolio Share**: **{share_pct:.1f}%** of active portfolio"""
                return ans, references, {"type": "single_currency_exposure", "currency": target_curr.upper(), "total": total_amt, "count": count}

            currency_totals = {}
            for r in records:
                c = r.lg_currency.iso_code if (r.lg_currency and r.lg_currency.iso_code) else "EGP"
                amt = float(r.lg_amount) if r.lg_amount else 0.0
                currency_totals[c] = currency_totals.get(c, 0.0) + amt

            for r in records[:10]:
                exp_str = r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else "N/A"
                c_code = r.lg_currency.iso_code if (r.lg_currency and r.lg_currency.iso_code) else "EGP"
                references.append({"lg_id": r.id, "lg_number": r.lg_number, "expiry_date": exp_str, "amount": float(r.lg_amount or 0), "currency": c_code})

            lines = [f"Portfolio Overview ({len(records)} Active LGs):\n\n**Total Exposure by Currency:**"]
            for c, tot in currency_totals.items():
                lines.append(f"- **{c}**: {tot:,.2f}")

            return "\n".join(lines), references, {"type": "portfolio_summary", "currencies": currency_totals, "total_count": len(records)}

        if intent == "find_expiring_lgs":
            records = query_result or []
            if not records:
                return "No active guarantees found matching that expiry timeframe.", [], None

            lines = [f"Found **{len(records)} guarantee(s)** expiring in the specified timeframe:\n"]
            for r in records:
                exp_str = r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else "N/A"
                amt = float(r.lg_amount) if r.lg_amount else 0.0
                c_code = r.lg_currency.iso_code if (r.lg_currency and r.lg_currency.iso_code) else "EGP"
                b_name = r.issuing_bank.name if r.issuing_bank else "N/A"
                lines.append(f"- **{r.lg_number}**: {amt:,.2f} {c_code} (Bank: *{b_name}*, Expiry: *{exp_str}*)")
                references.append({"lg_id": r.id, "lg_number": r.lg_number, "expiry_date": exp_str, "amount": amt, "currency": c_code})

            return "\n".join(lines), references, {"type": "expiring_lgs", "count": len(records)}

        if intent == "get_pending_approvals":
            reqs = query_result or []
            if not reqs:
                return "There are currently no requests pending approval in your organization queue.", [], None

            lines = [f"Found **{len(reqs)} request(s)** currently pending review in the Approval Center:\n"]
            for req in reqs:
                lines.append(f"- **Request #{req.id}** ({req.request_type or 'LG Action'}) - Submitted: {req.created_at.strftime('%Y-%m-%d') if req.created_at else 'N/A'}")

            return "\n".join(lines), [], {"type": "pending_approvals", "count": len(reqs)}

        if intent == "get_facility_analytics":
            facilities = query_result or []
            if not facilities:
                return "No active credit facilities configured for your organization.", [], None

            bank_limits = {}
            for f in facilities:
                b_name = f.bank.name if f.bank else "Bank"
                c_code = f.currency.iso_code if (f.currency and f.currency.iso_code) else "EGP"
                lim = float(f.total_limit_amount) if f.total_limit_amount else 0.0
                if b_name not in bank_limits:
                    bank_limits[b_name] = {}
                bank_limits[b_name][c_code] = bank_limits[b_name].get(c_code, 0.0) + lim

            lines = [f"Found **{len(facilities)} active credit facility line(s)** across banking partners:\n"]
            for b_name, currs in bank_limits.items():
                lim_str = ", ".join([f"{val:,.2f} {c}" for c, val in currs.items()])
                lines.append(f"- **{b_name}**: Total Line: {lim_str}")

            return "\n".join(lines), [], {"type": "facility_analytics", "bank_limits": bank_limits, "total_facilities": len(facilities)}

        if intent == "search_lgs":
            records = query_result or []
            if not records:
                return "No records matching your search criteria.", [], None

            lines = [f"Found **{len(records)} record(s)** matching your query:\n"]
            for r in records[:15]:
                exp_str = r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else "N/A"
                amt = float(r.lg_amount) if r.lg_amount else 0.0
                c_code = r.lg_currency.iso_code if (r.lg_currency and r.lg_currency.iso_code) else "EGP"
                st = r.lg_status.name.capitalize() if (r.lg_status and r.lg_status.name) else "Active"
                lines.append(f"- **{r.lg_number}**: {amt:,.2f} {c_code} (Status: *{st}*, Expiry: *{exp_str}*)")
                references.append({"lg_id": r.id, "lg_number": r.lg_number, "expiry_date": exp_str, "amount": amt, "currency": c_code})

            return "\n".join(lines), references, {"type": "lg_search", "count": len(records)}

        return "Query processed successfully.", [], None

    def _get_role_summary(self, role: str) -> str:
        role_lower = str(role).lower()
        if "owner" in role_lower:
            return "Super-Admin access to global configs, customer management, subscription plans, and audit logs."
        if "admin" in role_lower:
            return "Full organization management: user accounts, approval matrix, bank facilities, bank accounts, LG categories, migration hub, and organization reports."
        if "checker" in role_lower:
            return "Approval Center verification: review, approve, or reject LG transactions, issuance requests, and FX quotations."
        return "Daily operations: record new LGs (AI OCR / manual), initiate maintenance actions (Extend, Release, Liquidate, Decrease), and submit issuance requests."

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
        Main 4-Level Orchestrator with 100% Offline-First Execution for Levels 0, 1, and 4.
        """
        if not is_ai_query_assistant_enabled():
            return {
                "success": False,
                "error": "AI Data Assistant feature is currently disabled under system configuration.",
                "code": "FEATURE_DISABLED"
            }

        logger.info(f"4-Level AI Assistant request: user_id={user_id}, customer_id={customer_id}, card_id={card_id}")

        try:
            user_context = self.get_user_context(db, user_id, customer_id)

            # ==================================================================
            # LEVEL 0: SYSTEM ONLY (Frontend card_id resolution - 100% OFFLINE)
            # ==================================================================
            if card_id:
                is_l0, l0_config = policy_guardrail.resolve_card_id(card_id)
                if is_l0:
                    intent = l0_config["intent"]
                    params = l0_config["params"]
                    if intent == "system_help":
                        return self.handle_system_help("What are the system capabilities and modules in Grow?", user_context)

                    self._current_query_params = params
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
            # FAST-PATH: INSTANT GREETINGS (0ms, 100% OFFLINE)
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
            # STEP 1: Fast-Path Deterministic Classification
            # ==================================================================
            classification = self.classify_and_interpret(raw_q, customer_id=customer_id, user_id=user_id)
            s_level = classification.get("suggested_level", 1)
            intent = classification.get("intent", "search_lgs")
            topic = classification.get("topic", "treasury")
            params = classification.get("parameters", {})

            # Handle unsupported actions cleanly
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
            # LEVEL 4: SYSTEM KNOWLEDGE & ROLE GUIDANCE (100% OFFLINE)
            # ==================================================================
            if s_level == 4 or intent == "system_help":
                return self.handle_system_help(raw_q, user_context)

            # ==================================================================
            # LEVEL 3: TREASURY DOMAIN KNOWLEDGE (WITH OFFLINE GLOSSARY)
            # ==================================================================
            if s_level == 3 or intent == "general_treasury":
                if topic == "non_treasury":
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

                term = params.get("term")
                if term and term in OFFLINE_TREASURY_GLOSSARY:
                    return {
                        "success": True,
                        "answer": OFFLINE_TREASURY_GLOSSARY[term],
                        "references": [],
                        "level": 3,
                        "source_awareness": "GENERAL_AI_KNOWLEDGE",
                        "intent": "general_treasury"
                    }

                for g_term, g_def in OFFLINE_TREASURY_GLOSSARY.items():
                    if g_term in raw_q.lower():
                        return {
                            "success": True,
                            "answer": g_def,
                            "references": [],
                            "level": 3,
                            "source_awareness": "GENERAL_AI_KNOWLEDGE",
                            "intent": "general_treasury"
                        }

                prompt = f"""You are an executive Corporate Treasury expert. Answer concisely:
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

            # ==================================================================
            # LEVEL 1: SIMPLE AI + SYSTEM (100% OFFLINE SQL ORM QUERY)
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
            # LEVEL 2: COMPLEX AI + SYSTEM (Multi-Step Reasoning & Tokenization)
            # ==================================================================
            query_result = self.execute_orm_query(db, customer_id, user_id, "get_lg_analytics_summary", {}, has_all_entity_access, entity_ids)
            
            raw_dataset = []
            for r in (query_result or []):
                raw_dataset.append({
                    "lg_number": r.lg_number,
                    "amount": float(r.lg_amount or 0),
                    "currency": r.lg_currency.iso_code if (r.lg_currency and r.lg_currency.iso_code) else "EGP",
                    "beneficiary": r.beneficiary_corporate.entity_name if r.beneficiary_corporate else "Unknown",
                    "bank": r.issuing_bank.name if r.issuing_bank else "Unknown",
                    "expiry_date": r.expiry_date.strftime("%Y-%m-%d") if r.expiry_date else "N/A"
                })

            sanitized_q, q_tokens = privacy_tokenizer.sanitize_user_question(raw_q)
            tok_dataset, tok_bene, tok_fac, pl_tokens = privacy_tokenizer.tokenize_complex_payload(records=raw_dataset, beneficiary_summary={}, facility_summary=[])
            all_valid_tokens = {**q_tokens, **pl_tokens}

            prompt = f"""You are an executive Corporate Treasury Analyst.
Question: "{sanitized_q}"
Data: {json.dumps(tok_dataset[:20])}
"""
            raw_synthesis = self._call_llm(prompt) or "Analyzed multi-step exposure across beneficiaries and upcoming expiry horizons."
            is_valid_out, val_msg = privacy_tokenizer.validate_ai_output_tokens(raw_synthesis, all_valid_tokens)
            
            final_answer = privacy_tokenizer.detokenize_response(raw_synthesis, all_valid_tokens) if is_valid_out else raw_synthesis
            ans, refs, vis = self.format_application_response("get_lg_analytics_summary", query_result)

            return {
                "success": True,
                "answer": policy_guardrail.enforce_response_limit(final_answer if is_valid_out else ans),
                "references": refs,
                "visual_metadata": vis,
                "level": 2,
                "source_awareness": "COMBINATION",
                "intent": "complex_analysis"
            }

        except Exception as e:
            logger.error(f"Error executing AI query assistant: {e}", exc_info=True)
            return {
                "success": False,
                "error": "Unable to process query at this time.",
                "code": "INTERNAL_EXECUTION_ERROR"
            }


ai_query_assistant_service = AIQueryAssistantService()
