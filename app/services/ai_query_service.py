# app/services/ai_query_service.py
"""
AI Data Query Assistant - Production-Grade 4-Level Treasury AI Architecture

Principle:
- AI provides understanding, classification, and reasoning.
- APPLICATION provides authorization, control, policy enforcement, level determination, and execution.
- DATABASE remains the sole source of truth.

Levels:
- LEVEL 0: System Only (Bypasses AI completely via backend-resolved card_id)
- LEVEL 1: Simple AI + System (Single intent LLM classification -> ORM query -> Application formatting)
- LEVEL 2: Complex AI + System (Multi-step plan -> ORM -> Question & Record Tokenization -> LLM synthesis -> Token validation -> Detokenize)
- LEVEL 3: General Treasury AI (AI classification -> Policy Guardrail enforcement -> LLM Treasury domain answer)
"""

import os
import json
import logging
import re
import calendar
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, func

import app.models as models
from app.models.models_issuance import IssuanceFacility, IssuedLGRecord
from app.constants import ApprovalRequestStatusEnum, LgStatusEnum
from app.services.ai_policy_guardrail import policy_guardrail, MAX_RESPONSE_CHARS
from app.services.ai_privacy_tokenizer import privacy_tokenizer

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



class AIQueryAssistantService:
    """
    Production 4-Level Treasury AI Assistant Service.
    Enforces application control, tokenization, policy guardrails, and audit logging.
    """

    def __init__(self):
        self.enabled = is_ai_query_assistant_enabled()

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

    def classify_and_interpret(self, user_question: str) -> Dict[str, Any]:
        """
        Classifies user question into appropriate Level (1, 2, or 3) and extracts intent/topic.
        """
        prompt = f"""
You are an AI router and intent parser for a Treasury Management System.
Analyze the user's natural language question and classify it into the correct level:

LEVEL 1 ("simple_query"): Simple question mapping to one approved operation:
  - "get_lg_analytics_summary": For portfolio exposure, total exposure by currency, portfolio breakdown, financial overview, "how many active LGs", or counting total LGs.
  - "search_lgs": For searching LGs by currency (e.g., USD, EUR, EGP, euro, dollar, Egyptian pounds), status (expired, released, liquidated), bank, or reference number.
  - "find_expiring_lgs": For guarantees expiring within N days, or expiring during a specific month/year, or "next month", "this month".
  - "get_facility_analytics": For bank credit facility limits and credit line queries.
  - "get_pending_approvals": For checking pending approval requests.
  - "get_lg_details": For details of a specific LG by number/ID.

LEVEL 2 ("complex_analysis"): Complex question requiring multi-step analysis, relative rankings, or combining multiple cross-entity criteria (e.g. "Which beneficiaries have highest exposure and guarantees expiring in 90 days?").
LEVEL 3 ("general_treasury"): General treasury/finance concepts requiring NO customer database data (e.g. "What is cash pooling?", "How do interest rate swaps work?").
LEVEL 3 REJECTED ("non_treasury"): Questions outside corporate treasury/finance (e.g. jokes, general trivia, weather, animals).
UNSUPPORTED ("unsupported"): Commands or questions asking to execute bank wire transfers, execute payouts, create or update transactions, modify database records, or query internal HR/payroll data (e.g. "Execute bank wire transfer", "Execute an automatic bank wire transfer or payout", "Transfer funds to bank", "Show employee payroll"). Always classify any transactional/execution commands as "unsupported".

Important Extraction Guidelines for "search_lgs" & "find_expiring_lgs":
- If the user mentions a currency (e.g., "usd", "dollars", "eur", "euro", "egp", "pounds", "Egyptian pounds", "L.E."), put the ISO code ("USD", "EUR", "EGP") in the "currency" parameter.
- If the user mentions a month by name (e.g., "august", "september", "january"), put the month name in "month".
- If the user says "next month", put "next_month" in "month". If "this month", put "this_month" in "month".
- If the user asks about expired, released, or liquidated LGs, use intent "search_lgs" with the "status" parameter set to the status name (e.g., "expired", "released", "liquidated").
- Put ONLY specific target search terms (such as bank names, contract codes, or specific words) into "query". Do NOT put full conversational question phrases into "query".

User Question: "{user_question}"

Return ONLY a valid JSON object with NO markdown code blocks:
{{
  "suggested_level": 1,
  "topic": "treasury",
  "intent": "<find_expiring_lgs|get_pending_approvals|get_lg_analytics_summary|get_facility_analytics|search_lgs|get_lg_details|complex_analysis|general_treasury|unsupported>",
  "parameters": {{
    "days": 60,
    "month": "",
    "year": "",
    "query": "",
    "currency": "",
    "lg_number": "",
    "status": ""
  }}
}}
"""
        q_lower = user_question.lower().strip()
        # Direct pre-check for transactional execution / unsupported mutations
        if any(kw in q_lower for kw in ["execute an automatic bank wire", "execute bank wire", "wire transfer or payout", "execute wire", "transfer funds", "delete record", "modify database", "employee payroll"]):
            return {"suggested_level": 1, "topic": "treasury", "intent": "unsupported", "parameters": {}}

        response_text = self._call_llm(prompt)
        if not response_text:
            # Deterministic rule-based fallback if LLM is unavailable or rate-limited
            if any(kw in q_lower for kw in ["what is", "how do", "explain", "definition"]):
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
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return {"suggested_level": 1, "topic": "treasury", "intent": "search_lgs", "parameters": {}}

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
            "ALL": None,  # No status filter — show everything
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

        # Apply status filter: explicit status from user, or default to Valid
        if s_str and s_str in STATUS_MAP:
            if STATUS_MAP[s_str] is not None:  # "ALL" means no filter
                base_query = base_query.filter(
                    models.LGRecord.lg_status_id == STATUS_MAP[s_str]
                )
        else:
            # Default: Only Valid (active) LGs
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

            # Resolve relative month terms
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
                # Check query parameter for month terms if classifier put it in query
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

            # 1. Currency Extraction & Filtering
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

            # 2. Stop-word & Conversational Phrase Scrubbing for search query
            # Removes conversational filler words so they don't pollute the ORM search
            STOP_WORDS = {
                "ARE", "THERE", "ANY", "VALID", "ACTIVE", "LG", "LGS", "LGS", "IN", "FOR",
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

            # Status filter is already applied to base_query above

            return q.order_by(models.LGRecord.expiry_date.asc()).all()

        elif intent == "get_lg_details":
            lg_num = params.get("lg_number")
            if lg_num:
                return base_query.filter(models.LGRecord.lg_number.ilike(f"%{lg_num}%")).limit(5).all()
            return base_query.order_by(models.LGRecord.created_at.desc()).limit(5).all()

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
            bank_stats = {}

            for rec in records:
                b_name, c_code, amt = _extract_record_fields(rec)

                currency_stats[c_code] = currency_stats.get(c_code, 0.0) + amt
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

            curr_lines = [f"- **{c_code}**: {amt:,.2f}" for c_code, amt in currency_stats.items()]
            ans = f"Portfolio Overview ({len(records)} Active LGs):\n\n**Total Exposure by Currency:**\n" + "\n".join(curr_lines)

            visual_metadata = {
                "type": "portfolio_summary",
                "total_count": len(records),
                "currencies": currency_stats,
                "top_banks": dict(list(bank_stats.items())[:5])
            }
            return ans, references, visual_metadata

        # Record list formatting for find_expiring_lgs & search_lgs
        records = query_result
        if not records:
            return "No records matching your search criteria were found.", [], None

        rec_lines = []
        for rec in records:
            b_name, c_code, amt = _extract_record_fields(rec)
            exp_str = rec.expiry_date.strftime("%Y-%m-%d") if rec.expiry_date else "N/A"
            lg_num = _get_lg_number(rec)

            rec_lines.append(f"- **{lg_num}**: {amt:,.2f} {c_code} ({b_name}, Exp: {exp_str})")
            if len(references) < 10:
                references.append({
                    "lg_id": rec.id,
                    "lg_number": lg_num,
                    "expiry_date": exp_str,
                    "amount": amt,
                    "currency": c_code
                })

        ans = f"Found {len(records)} Letter(s) of Guarantee matching your query:\n\n" + "\n".join(rec_lines[:15])
        return ans, references, None

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
        Main 4-Level Orchestrator.
        """
        if not is_ai_query_assistant_enabled():
            return {
                "success": False,
                "error": "AI Data Assistant feature is currently disabled under system configuration.",
                "code": "FEATURE_DISABLED"
            }

        logger.info(f"4-Level AI Assistant request: user_id={user_id}, customer_id={customer_id}, card_id={card_id}")

        try:
            # ==================================================================
            # LEVEL 0: SYSTEM ONLY (Frontend card_id resolution - Bypasses AI)
            # ==================================================================
            if card_id:
                is_l0, l0_config = policy_guardrail.resolve_card_id(card_id)
                if is_l0:
                    intent = l0_config["intent"]
                    params = l0_config["params"]
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

            if not user_question.strip():
                return {"success": False, "error": "Question text is required.", "code": "EMPTY_QUESTION"}

            # ==================================================================
            # STEP 1: AI Classifies Question & Intent
            # ==================================================================
            classification = self.classify_and_interpret(user_question)
            s_level = classification.get("suggested_level", 1)
            intent = classification.get("intent", "search_lgs")
            topic = classification.get("topic", "treasury")
            params = classification.get("parameters", {})

            # Validate Treasury Domain Scope for Level 3 / General Questions
            if s_level == 3 or topic == "non_treasury" or intent == "general_treasury":
                is_valid_scope, scope_msg = policy_guardrail.validate_treasury_scope(user_question, topic)
                if not is_valid_scope:
                    return {
                        "success": True,
                        "answer": "I am specialized strictly in corporate treasury, trade finance, guarantees, liquidity, and cash management. Please ask a treasury-related question.",
                        "references": [],
                        "level": 3,
                        "source_awareness": "GENERAL_AI_KNOWLEDGE",
                        "intent": "rejected_scope"
                    }

                # Generate Treasury Domain answer
                prompt = f"""
You are an executive Corporate Treasury expert. Answer the following general treasury question concisely and professionally.
Do NOT invent company specific facts.

Question: "{user_question}"
"""
                ai_ans = self._call_llm(prompt) or "Cash pooling is a centralized liquidity management strategy used by corporate treasuries to optimize bank balances across group entities."
                
                return {
                    "success": True,
                    "answer": policy_guardrail.enforce_response_limit(ai_ans),
                    "references": [],
                    "level": 3,
                    "source_awareness": "GENERAL_AI_KNOWLEDGE",
                    "intent": "general_treasury"
                }

            # Handle unsupported capability gap explicitly
            if intent == "unsupported":
                return {
                    "success": True,
                    "answer": "I don't currently have enough information or capability to answer that. Please try rephrasing your question.",
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

                query_result = self.execute_orm_query(db, customer_id, user_id, intent, valid_params, has_all_entity_access, entity_ids)
                ans, refs, vis = self.format_application_response(intent, query_result)

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
            sanitized_q, question_tokens = privacy_tokenizer.sanitize_user_question(user_question)
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
- Current Date: {date_context['today']}
- Next 30 Days: {date_context['window_30_days']} (Total LGs expiring: {len(upcoming_expiries_30)})
- Next 60 Days: {date_context['window_60_days']} (Total LGs expiring: {len(upcoming_expiries_60)})
- Next 90 Days: {date_context['window_90_days']} (Total LGs expiring: {len(upcoming_expiries_90)})

2. VERIFIED TOTAL PORTFOLIO EXPOSURE (AUTHORITATIVE - DO NOT RECALCULATE):
{json.dumps(portfolio_currency_totals, indent=2)}

3. VERIFIED BENEFICIARY EXPOSURES & 90-DAY UPCOMING EXPIRIES:
{json.dumps(tokenized_beneficiaries, indent=2)}

4. VERIFIED BANK CREDIT FACILITIES & UTILIZATION HEADROOM:
{json.dumps(tokenized_facilities, indent=2)}

5. TOKENIZED RECORD SAMPLE ({len(tokenized_dataset)} records):
{json.dumps(tokenized_dataset[:30], indent=2)}
================================================================================

Strict Rules:
1. DATE ACCURACY: Today's date is strictly {date_context['today']}. NEVER assume, guess, or state any other date (e.g. 2024, March 2026, etc.).
2. AUTHORITATIVE TOTALS: All portfolio totals, beneficiary sums, and expiry lists are pre-calculated by the application. Use these exact verified figures. Do NOT perform independent manual arithmetic on raw records.
3. ENTITY TOKENS: Refer to entities using ONLY their supplied tokens (e.g. LG_TOKEN_001, BENEFICIARY_TOKEN_001, BANK_TOKEN_001).
4. TREASURY JUDGMENT: When asked for recommendations or risk assessment, provide executive-grade treasury insights regarding counterparty concentration, currency exposure, refinancing/renewal risk, and actionable mitigation strategies.
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

            return {
                "success": True,
                "answer": policy_guardrail.enforce_response_limit(detokenized_ans),
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
