# app/services/fx_service.py
"""
Centralized FX Resolution Engine — 3-Tier Currency Conversion Service.

Tier 1: CBE daily rates (CurrencyExchangeRate table) — direct or triangulated via EGP
Tier 2: AI fallback (Gemini) — for exotic pairs not in CBE data, cached for 24h
Tier 3: Fail-safe — returns None so callers can decide how to proceed

All AI calls are logged to ai_usage_logs for cost tracking and optimization.
"""

import logging
from datetime import datetime, timedelta, date as date_type
from decimal import Decimal, InvalidOperation
from typing import Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import func, desc

logger = logging.getLogger(__name__)


class FxService:
    """Three-tier FX resolution engine."""

    # Cache AI results for 24 hours to avoid repeated calls for the same pair
    AI_CACHE_TTL_HOURS = 24

    # ──────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ──────────────────────────────────────────────────────────────────────

    def get_rate(
        self,
        db: Session,
        from_currency_id: int,
        to_currency_id: int,
        *,
        customer_id: Optional[int] = None,
        user_id: Optional[int] = None,
        allow_ai: bool = True,
    ) -> Optional[Decimal]:
        """
        Get the FX rate to convert 1 unit of from_currency into to_currency.

        Returns:
            Decimal rate if found (multiply from_amount by this to get to_amount).
            None if no rate could be determined (caller must handle).
        """
        # Same currency → 1.0
        if from_currency_id == to_currency_id:
            return Decimal("1.0")

        from app.models.models import Currency
        from_currency = db.query(Currency).filter(Currency.id == from_currency_id).first()
        to_currency = db.query(Currency).filter(Currency.id == to_currency_id).first()

        if not from_currency or not to_currency:
            logger.warning(f"FX: Currency not found — from_id={from_currency_id}, to_id={to_currency_id}")
            return None

        from_code = from_currency.iso_code.upper()
        to_code = to_currency.iso_code.upper()

        # Same ISO code but different IDs (shouldn't happen, but guard)
        if from_code == to_code:
            return Decimal("1.0")

        # Tier 1: CBE rates
        rate = self._tier1_cbe(db, from_currency_id, to_currency_id, from_code, to_code)
        if rate is not None:
            logger.debug(f"FX Tier 1 (CBE): {from_code}/{to_code} = {rate}")
            return rate

        # Tier 2: AI fallback (if allowed)
        if allow_ai:
            rate = self._tier2_ai(db, from_code, to_code, customer_id, user_id)
            if rate is not None:
                logger.info(f"FX Tier 2 (AI): {from_code}/{to_code} = {rate}")
                return rate

        # Tier 3: Fail-safe — return None
        logger.warning(f"FX: No rate found for {from_code}/{to_code} — returning None")
        return None

    def convert(
        self,
        db: Session,
        amount: Decimal,
        from_currency_id: int,
        to_currency_id: int,
        **kwargs,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Convenience: convert an amount between currencies.

        Returns:
            (converted_amount, rate_used) or (None, None) if no rate found.
        """
        rate = self.get_rate(db, from_currency_id, to_currency_id, **kwargs)
        if rate is None:
            return None, None
        return amount * rate, rate

    # ──────────────────────────────────────────────────────────────────────
    # TIER 1: CBE DAILY RATES
    # ──────────────────────────────────────────────────────────────────────

    def _tier1_cbe(
        self,
        db: Session,
        from_currency_id: int,
        to_currency_id: int,
        from_code: str,
        to_code: str,
    ) -> Optional[Decimal]:
        """
        Try to get rate from CurrencyExchangeRate table.
        All CBE rates are stored vs EGP, so we may need to triangulate.
        """
        from app.models.models import CurrencyExchangeRate

        # Case 1: Converting TO EGP — direct lookup
        if to_code == "EGP":
            return self._get_latest_cbe_rate(db, from_currency_id)

        # Case 2: Converting FROM EGP — inverse
        if from_code == "EGP":
            rate_to_egp = self._get_latest_cbe_rate(db, to_currency_id)
            if rate_to_egp and rate_to_egp != Decimal("0"):
                return Decimal("1") / rate_to_egp
            return None

        # Case 3: Neither is EGP — triangulate via EGP
        # from_currency → EGP → to_currency
        from_to_egp = self._get_latest_cbe_rate(db, from_currency_id)
        to_to_egp = self._get_latest_cbe_rate(db, to_currency_id)

        if from_to_egp and to_to_egp and to_to_egp != Decimal("0"):
            # 1 FROM = from_to_egp EGP
            # 1 TO = to_to_egp EGP
            # Therefore: 1 FROM = (from_to_egp / to_to_egp) TO
            return from_to_egp / to_to_egp

        return None

    def _get_latest_cbe_rate(self, db: Session, currency_id: int) -> Optional[Decimal]:
        """Get the latest sell_rate for a currency vs EGP."""
        from app.models.models import CurrencyExchangeRate

        latest = db.query(CurrencyExchangeRate).filter(
            CurrencyExchangeRate.currency_id == currency_id
        ).order_by(desc(CurrencyExchangeRate.rate_date)).first()

        if latest and latest.sell_rate:
            return Decimal(str(latest.sell_rate))
        return None

    # ──────────────────────────────────────────────────────────────────────
    # TIER 2: AI FALLBACK (with 24h cache)
    # ──────────────────────────────────────────────────────────────────────

    def _tier2_ai(
        self,
        db: Session,
        from_code: str,
        to_code: str,
        customer_id: Optional[int],
        user_id: Optional[int],
    ) -> Optional[Decimal]:
        """
        Ask Gemini for the latest closing rate of a currency pair.
        Results are cached for 24h to avoid repeat API calls.
        """
        # Check cache first
        cached_rate = self._check_ai_cache(db, from_code, to_code)
        if cached_rate is not None:
            logger.debug(f"FX Tier 2 (AI cache hit): {from_code}/{to_code} = {cached_rate}")
            return cached_rate

        # Call AI
        try:
            rate, tokens_used = self._ask_ai_for_rate(from_code, to_code)
            if rate is not None:
                self._save_ai_cache(db, from_code, to_code, rate)
                self._log_ai_usage(db, customer_id, user_id, from_code, to_code, tokens_used)
                return rate
        except Exception as e:
            logger.error(f"FX Tier 2 (AI) failed for {from_code}/{to_code}: {e}")

        return None

    def _check_ai_cache(self, db: Session, from_code: str, to_code: str) -> Optional[Decimal]:
        """Check if we have a recent AI-cached rate for this pair."""
        from app.models.models_issuance import AiFxRateCache
        cutoff = datetime.utcnow() - timedelta(hours=self.AI_CACHE_TTL_HOURS)

        # Check both directions
        cached = db.query(AiFxRateCache).filter(
            AiFxRateCache.from_currency_code == from_code,
            AiFxRateCache.to_currency_code == to_code,
            AiFxRateCache.cached_at >= cutoff,
        ).order_by(desc(AiFxRateCache.cached_at)).first()

        if cached:
            return cached.rate

        # Check inverse direction
        cached_inv = db.query(AiFxRateCache).filter(
            AiFxRateCache.from_currency_code == to_code,
            AiFxRateCache.to_currency_code == from_code,
            AiFxRateCache.cached_at >= cutoff,
        ).order_by(desc(AiFxRateCache.cached_at)).first()

        if cached_inv and cached_inv.rate and cached_inv.rate != Decimal("0"):
            return Decimal("1") / cached_inv.rate

        return None

    def _save_ai_cache(self, db: Session, from_code: str, to_code: str, rate: Decimal):
        """Save an AI-fetched rate to the cache."""
        from app.models.models_issuance import AiFxRateCache
        entry = AiFxRateCache(
            from_currency_code=from_code,
            to_currency_code=to_code,
            rate=rate,
            cached_at=datetime.utcnow(),
        )
        db.add(entry)
        # Don't commit here — let the caller's transaction handle it

    def _ask_ai_for_rate(self, from_code: str, to_code: str) -> Tuple[Optional[Decimal], int]:
        """
        Call Gemini to get the latest closing rate.
        Returns (rate, tokens_used) or (None, 0).
        """
        import google.generativeai as genai
        import json, os

        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            logger.warning("FX AI: GEMINI_API_KEY not set — skipping AI tier")
            return None, 0

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.0-flash")

        prompt = (
            f"What is the latest market closing exchange rate for converting "
            f"1 {from_code} to {to_code}? "
            f"Respond with ONLY a JSON object: {{\"rate\": <number>}}. "
            f"The rate should be a decimal number representing how many {to_code} "
            f"you get for 1 {from_code}. No explanation, no text — just the JSON."
        )

        try:
            response = model.generate_content(prompt)
            raw_text = response.text.strip()

            # Clean markdown fences if present
            if raw_text.startswith("```"):
                raw_text = raw_text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

            data = json.loads(raw_text)
            rate_value = data.get("rate")

            tokens_used = 0
            if hasattr(response, "usage_metadata"):
                meta = response.usage_metadata
                tokens_used = getattr(meta, "total_token_count", 0)

            if rate_value is not None:
                return Decimal(str(rate_value)), tokens_used

        except Exception as e:
            logger.error(f"FX AI parse error for {from_code}/{to_code}: {e}")

        return None, 0

    def _log_ai_usage(
        self,
        db: Session,
        customer_id: Optional[int],
        user_id: Optional[int],
        from_code: str,
        to_code: str,
        tokens_used: int,
    ):
        """Log AI usage to ai_usage_logs for cost tracking."""
        if not customer_id or not user_id:
            return

        from app.models.models import AIUsageLog
        log = AIUsageLog(
            customer_id=customer_id,
            user_id=user_id,
            doc_name=f"FX_RATE_{from_code}_{to_code}",
            model_name="gemini-2.0-flash",
            prompt_tokens=0,
            completion_tokens=0,
            total_tokens=tokens_used,
            ocr_characters=0,
            total_pages=0,
        )
        db.add(log)


# Module-level singleton for convenience
fx_service = FxService()
