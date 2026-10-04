# app/services/live_market_service.py
"""
Live Market Benchmark & Dynamic Empirical Reference Service (Phase 6.5).

1. Live Interbank Mid Provider:
   - Fetches live interbank FX spot rates (USD/EGP, EUR/EGP, GBP/EGP, EUR/USD, etc.).
   - Multi-tier resolution: In-memory cache (60s TTL) -> Free Real-time API -> CBE local DB fallback.
   - Calculates CBE Fixing Drift / Gap (Live Mid vs Official CBE Rate).

2. Dynamic Empirical Reference Engine:
   - Evaluates historical completed RFQs filtered hierarchically:
     Level 1: Customer ID + Currency Pair + Volume Tier
     Level 2: Customer ID + Currency Pair (All Volumes)
     Level 3: Platform-Wide Anonymized Benchmark
   - Derives Historical Mean Spread over Mid and Suggested Reference Rate.
   - Graceful cold-start (N < 3 tenders) with explicit notification.
   - Mandatory Regulatory & Governance Disclaimer attached to all payloads.
"""

import logging
import json
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from app.models.models_quotation import (
    QuotationRequest, QuotationBankAssignment, QuotationOffer, QuotationAnonymousBenchmark
)
from app.models.models import CurrencyExchangeRate, Currency

logger = logging.getLogger(__name__)

# In-memory TTL Cache: { "USD_EGP": { "rate": 52.22, "timestamp": datetime } }
_LIVE_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 60

DISCLAIMER_TEXT = (
    "⚠️ Historical Empirical Model: Reference rate and spread are derived mathematically from "
    "historical platform executions for this currency pair. This benchmark is provided solely "
    "for indicative reference and does not constitute market advice or replace customer rate "
    "revision, verification, or internal compliance policies."
)


class LiveMarketService:
    """Institutional Real-Time Interbank Mid & Empirical Historical Spread Engine."""

    def categorize_volume_tier(self, amount: Optional[float], currency: str = "USD") -> str:
        """Categorizes volume into TIER_1 (<$250k), TIER_2 ($250k-$1M), or TIER_3 (>$1M)."""
        if not amount:
            return "TIER_1"
        amt = float(amount)
        # Approximate EGP conversion if currency is EGP
        if currency and currency.upper() == "EGP":
            amt = amt / 50.0
        if amt < 250000.0:
            return "TIER_1"
        elif amt <= 1000000.0:
            return "TIER_2"
        return "TIER_3"

    def get_cbe_official_rate(self, db: Session, from_code: str, to_code: str) -> Optional[Dict[str, Any]]:
        """Retrieves official CBE daily exchange rate and calculates mid from local database."""
        from_code = (from_code or "USD").upper().strip()
        to_code = (to_code or "EGP").upper().strip()

        # Target foreign currency vs EGP
        target_code = from_code if to_code == "EGP" else (to_code if from_code == "EGP" else None)
        if not target_code:
            return None

        curr = db.query(Currency).filter(func.upper(Currency.iso_code) == target_code).first()
        if not curr:
            return None

        rate_rec = db.query(CurrencyExchangeRate).filter(
            CurrencyExchangeRate.currency_id == curr.id
        ).order_by(desc(CurrencyExchangeRate.rate_date)).first()

        if not rate_rec:
            return None

        buy = float(rate_rec.buy_rate)
        sell = float(rate_rec.sell_rate)
        mid = round((buy + sell) / 2.0, 4)

        return {
            "cbe_buy": buy,
            "cbe_sell": sell,
            "cbe_mid": mid,
            "rate_date": str(rate_rec.rate_date),
            "currency": target_code
        }

    def fetch_live_rates(self) -> Dict[str, float]:
        """Fetches live USD-based spot rates from free open market API with memory cache."""
        now = datetime.now(timezone.utc)
        cache_entry = _LIVE_CACHE.get("USD_BASE")
        if cache_entry and (now - cache_entry["timestamp"]).total_seconds() < CACHE_TTL_SECONDS:
            return cache_entry["rates"]

        try:
            req = urllib.request.Request(
                "https://open.er-api.com/v6/latest/USD",
                headers={"User-Agent": "GrowTreasury/2.0 (Platform Benchmark Engine)"}
            )
            with urllib.request.urlopen(req, timeout=4) as response:
                if response.status == 200:
                    payload = json.loads(response.read().decode('utf-8'))
                    rates = payload.get("rates", {})
                    if rates and "EGP" in rates:
                        _LIVE_CACHE["USD_BASE"] = {
                            "rates": rates,
                            "timestamp": now
                        }
                        return rates
        except Exception as e:
            logger.warning(f"Live FX API fetch failed or timed out: {e}. Falling back gracefully.")

        # If cache exists (even expired), return it rather than failing
        if cache_entry:
            return cache_entry["rates"]
        return {}

    def get_live_interbank_mid(
        self,
        db: Session,
        from_code: str,
        to_code: str
    ) -> Dict[str, Any]:
        """
        Calculates live interbank mid rate and CBE gap (drift).
        Multi-tier: Live Market API -> CBE Local Database.
        """
        from_code = (from_code or "USD").upper().strip()
        to_code = (to_code or "EGP").upper().strip()
        pair_str = f"{from_code}/{to_code}"

        cbe_info = self.get_cbe_official_rate(db, from_code, to_code)
        cbe_mid = cbe_info["cbe_mid"] if cbe_info else None

        rates = self.fetch_live_rates()
        live_mid = None
        source = "CBE_OFFICIAL_FIXING"

        # Direct cross calculation from USD base rates
        if rates:
            if from_code == "USD" and to_code in rates:
                live_mid = round(float(rates[to_code]), 4)
                source = "INTERBANK_LIVE_FEED"
            elif to_code == "USD" and from_code in rates:
                live_mid = round(1.0 / float(rates[from_code]), 4)
                source = "INTERBANK_LIVE_FEED"
            elif from_code in rates and to_code in rates and float(rates[from_code]) > 0:
                # Triangulate: e.g. EUR/EGP = (USD/EGP) / (USD/EUR)
                live_mid = round(float(rates[to_code]) / float(rates[from_code]), 4)
                source = "INTERBANK_LIVE_FEED"

        # Fallback to CBE official fixing if live feed is unavailable or missing pair
        if live_mid is None:
            if cbe_mid is not None:
                live_mid = cbe_mid
                source = "CBE_OFFICIAL_FIXING"
            else:
                live_mid = 50.0000
                source = "SYSTEM_DEFAULT_FALLBACK"

        # Calculate Intraday CBE Fixing Gap (Drift)
        cbe_gap = None
        cbe_gap_bps = None
        cbe_gap_pips = None
        if cbe_mid and cbe_mid > 0 and live_mid:
            cbe_gap = round(live_mid - cbe_mid, 4)
            cbe_gap_bps = round(((live_mid - cbe_mid) / cbe_mid) * 10000.0, 2)
            cbe_gap_pips = round((live_mid - cbe_mid) * 10000.0, 1)

        # Phase 6.6: Time-Series Market Spot Rate Archive (builds proprietary dataset)
        if live_mid and db:
            try:
                from app.models.models_quotation import QuotationMarketRateHistory
                cutoff = datetime.now(timezone.utc) - timedelta(seconds=60)
                recent_entry = db.query(QuotationMarketRateHistory).filter(
                    QuotationMarketRateHistory.currency_pair == pair_str,
                    QuotationMarketRateHistory.created_at >= cutoff
                ).first()
                if not recent_entry:
                    hist_record = QuotationMarketRateHistory(
                        currency_pair=pair_str,
                        base_currency=from_code,
                        quote_currency=to_code,
                        rate=live_mid,
                        source=source,
                        cbe_official_mid=cbe_mid,
                        cbe_gap_bps=cbe_gap_bps
                    )
                    db.add(hist_record)
                    db.commit()
            except Exception as archive_err:
                logger.debug(f"Spot rate archive skip or error: {archive_err}")
                db.rollback()

        return {
            "currency_pair": pair_str,
            "live_mid": live_mid,
            "source": source,
            "is_live_feed": (source == "INTERBANK_LIVE_FEED"),
            "cbe_official_mid": cbe_mid,
            "cbe_fixing_date": cbe_info["rate_date"] if cbe_info else None,
            "cbe_gap": cbe_gap,
            "cbe_gap_bps": cbe_gap_bps,
            "cbe_gap_pips": cbe_gap_pips,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def get_empirical_reference(
        self,
        db: Session,
        customer_id: Optional[int],
        from_code: str,
        to_code: str,
        direction: str = "BUY",
        amount: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Derives Suggested Reference Rate using Hierarchical Historical Platform Executions.
        Level 1: Customer + Pair + Volume Tier
        Level 2: Customer + Pair (All Volumes)
        Level 3: Platform-Wide Market Benchmark
        """
        from_code = (from_code or "USD").upper().strip()
        to_code = (to_code or "EGP").upper().strip()
        direction_upper = (direction or "BUY").upper().strip()
        volume_tier = self.categorize_volume_tier(amount, currency=from_code)
        pair_str = f"{from_code}/{to_code}"

        # 1. Obtain Live Interbank Mid & CBE Drift
        live_data = self.get_live_interbank_mid(db, from_code, to_code)
        live_mid = live_data["live_mid"]

        # Helper to extract past winning spread over historical benchmark
        def _get_spreads_from_rfqs(query):
            completed_rfqs = query.all()
            spreads = []
            curr_rec = db.query(Currency).filter(func.upper(Currency.iso_code) == from_code).first()

            for r in completed_rfqs:
                # Find winning offer
                assignments = db.query(QuotationBankAssignment).filter(
                    QuotationBankAssignment.rfq_id == r.id
                ).all()
                if not assignments:
                    continue
                offers = db.query(QuotationOffer).filter(
                    QuotationOffer.assignment_id.in_([a.id for a in assignments]),
                    QuotationOffer.price.isnot(None)
                ).all()
                if not offers:
                    continue
                prices = [o.price for o in offers if o.price and o.price > 0]
                if not prices:
                    continue

                is_sell = (r.direction and r.direction.upper() == "SELL")
                winning_price = max(prices) if is_sell else min(prices)

                # Determine historical reference rate on trade date
                trade_dt = None
                if r.window_start:
                    trade_dt = r.window_start.date() if hasattr(r.window_start, "date") else r.window_start
                elif r.created_at:
                    trade_dt = r.created_at.date() if hasattr(r.created_at, "date") else r.created_at

                hist_mid = None
                if curr_rec and trade_dt:
                    cbe_hist = db.query(CurrencyExchangeRate).filter(
                        CurrencyExchangeRate.currency_id == curr_rec.id,
                        CurrencyExchangeRate.rate_date <= trade_dt
                    ).order_by(desc(CurrencyExchangeRate.rate_date)).first()
                    if cbe_hist and cbe_hist.buy_rate and cbe_hist.sell_rate:
                        hist_mid = (float(cbe_hist.buy_rate) + float(cbe_hist.sell_rate)) / 2.0

                if not hist_mid or hist_mid <= 0:
                    hist_mid = r.eval_rate or (sum(prices) / len(prices))

                if hist_mid and hist_mid > 0:
                    spread_bps = ((winning_price - hist_mid) / hist_mid) * 10000.0
                    # Sanitize: filter out abnormal mock/test anomalies (> 5% spread)
                    if -500.0 <= spread_bps <= 500.0:
                        spread_val = (spread_bps / 10000.0) * live_mid
                        spreads.append({
                            "spread_val": spread_val,
                            "spread_bps": spread_bps,
                            "winning_rate": winning_price
                        })
            return spreads

        # Level 1: Customer + Pair + Volume Tier
        spreads_lvl1 = []
        if customer_id:
            q_lvl1 = db.query(QuotationRequest).filter(
                QuotationRequest.customer_id == customer_id,
                QuotationRequest.status.in_(["COMPLETED", "ACCEPTED"]),
                func.upper(QuotationRequest.buy_currency) == from_code,
                func.upper(QuotationRequest.sell_currency) == to_code,
                func.upper(QuotationRequest.direction) == direction_upper
            )
            # Filter volume tier
            all_cust_rfqs = q_lvl1.all()
            matching_tier = [r for r in all_cust_rfqs if self.categorize_volume_tier(r.amount, from_code) == volume_tier]
            if len(matching_tier) >= 3:
                # Use matching tier directly
                class DummyQuery:
                    def all(self): return matching_tier
                spreads_lvl1 = _get_spreads_from_rfqs(DummyQuery())

        # Level 2: Customer + Pair (All Volumes)
        spreads_lvl2 = []
        if len(spreads_lvl1) < 3 and customer_id:
            q_lvl2 = db.query(QuotationRequest).filter(
                QuotationRequest.customer_id == customer_id,
                QuotationRequest.status.in_(["COMPLETED", "ACCEPTED"]),
                func.upper(QuotationRequest.buy_currency) == from_code,
                func.upper(QuotationRequest.sell_currency) == to_code,
                func.upper(QuotationRequest.direction) == direction_upper
            )
            spreads_lvl2 = _get_spreads_from_rfqs(q_lvl2)

        # Level 3: Platform-Wide Completed RFQs
        spreads_lvl3 = []
        if len(spreads_lvl1) < 3 and len(spreads_lvl2) < 3:
            q_lvl3 = db.query(QuotationRequest).filter(
                QuotationRequest.status.in_(["COMPLETED", "ACCEPTED"]),
                func.upper(QuotationRequest.buy_currency) == from_code,
                func.upper(QuotationRequest.sell_currency) == to_code,
                func.upper(QuotationRequest.direction) == direction_upper
            )
            spreads_lvl3 = _get_spreads_from_rfqs(q_lvl3)

        # Select highest fidelity spread sample with >= 3 items
        selected_spreads = []
        resolution_tier = "COLD_START"
        if len(spreads_lvl1) >= 3:
            selected_spreads = spreads_lvl1
            resolution_tier = "CUSTOMER_VOLUME_TIER"
        elif len(spreads_lvl2) >= 3:
            selected_spreads = spreads_lvl2
            resolution_tier = "CUSTOMER_AGGREGATE"
        elif len(spreads_lvl3) >= 3:
            selected_spreads = spreads_lvl3
            resolution_tier = "PLATFORM_MARKET"
        else:
            # Fall back to anonymous benchmark mart if available
            bms = db.query(QuotationAnonymousBenchmark).filter(
                QuotationAnonymousBenchmark.currency_pair == pair_str
            ).all()
            if len(bms) >= 3:
                selected_spreads = [{
                    "spread_val": (b.winning_spread_bps / 10000.0) * live_mid,
                    "spread_bps": b.winning_spread_bps,
                    "winning_rate": (b.cbe_benchmark_rate or live_mid) * (1 + b.winning_spread_bps / 10000.0)
                } for b in bms if b.winning_spread_bps is not None]
                resolution_tier = "PLATFORM_ANONYMOUS_MART"

        sample_size = len(selected_spreads)
        is_empirical_active = (sample_size >= 3)

        if is_empirical_active:
            mean_spread_val = sum(s["spread_val"] for s in selected_spreads) / sample_size
            mean_spread_bps = sum(s["spread_bps"] for s in selected_spreads) / sample_size
            mean_spread_pips = round(mean_spread_val * 10000.0, 1)

            suggested_rate = round(live_mid + mean_spread_val, 4)
            status_message = (
                f"Empirical reference active ({sample_size} historical tenders evaluated). "
                f"Historical mean spread: {mean_spread_pips:+.1f} pips ({mean_spread_bps:+.1f} bps)."
            )
        else:
            mean_spread_val = 0.0
            mean_spread_bps = 0.0
            mean_spread_pips = 0.0
            suggested_rate = live_mid
            status_message = (
                f"Building empirical baseline ({sample_size} of 3 required tenders recorded). "
                "Displaying raw Live Interbank Mid."
            )

        return {
            "currency_pair": pair_str,
            "direction": direction_upper,
            "volume_tier": volume_tier,
            "live_mid": live_mid,
            "source": live_data["source"],
            "is_live_feed": live_data["is_live_feed"],
            "cbe_official_mid": live_data["cbe_official_mid"],
            "cbe_fixing_date": live_data["cbe_fixing_date"],
            "cbe_gap": live_data["cbe_gap"],
            "cbe_gap_bps": live_data["cbe_gap_bps"],
            "cbe_gap_pips": live_data["cbe_gap_pips"],
            "suggested_reference_rate": suggested_rate,
            "historical_mean_spread": round(mean_spread_val, 4),
            "historical_mean_spread_bps": round(mean_spread_bps, 2),
            "historical_mean_spread_pips": mean_spread_pips,
            "sample_size": sample_size,
            "resolution_tier": resolution_tier,
            "is_empirical_active": is_empirical_active,
            "status_message": status_message,
            "governance_disclaimer": DISCLAIMER_TEXT,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def evaluate_quote_spread(
        self,
        quote_rate: float,
        benchmark: Dict[str, Any],
        direction: str = "BUY"
    ) -> Dict[str, Any]:
        """
        Compares an incoming firm quote or winning rate against Live Mid and Suggested Reference.
        Returns exact basis points, pips, and market tightness assessment.
        """
        if not quote_rate or not benchmark:
            return {}

        direction_upper = (direction or "BUY").upper().strip()
        live_mid = benchmark.get("live_mid") or quote_rate
        suggested_ref = benchmark.get("suggested_reference_rate") or live_mid

        # Spread vs Live Mid
        spread_vs_mid = round(quote_rate - live_mid, 4)
        spread_vs_mid_bps = round(((quote_rate - live_mid) / live_mid) * 10000.0, 2)
        spread_vs_mid_pips = round((quote_rate - live_mid) * 10000.0, 1)

        # Variance vs Historical Suggested Reference
        variance_vs_ref = round(quote_rate - suggested_ref, 4)
        variance_vs_ref_bps = round(((quote_rate - suggested_ref) / live_mid) * 10000.0, 2)
        variance_vs_ref_pips = round((quote_rate - suggested_ref) * 10000.0, 1)

        # Assessment logic: For BUY, lower rate is tighter/better. For SELL, higher rate is tighter/better.
        if direction_upper == "BUY":
            is_favorable = variance_vs_ref <= 0
        else:
            is_favorable = variance_vs_ref >= 0

        if abs(variance_vs_ref_bps) <= 5.0:
            assessment = "CONSISTENT_WITH_HISTORICAL_NORM"
            assessment_label = "Consistent with Historical Norm"
        elif is_favorable:
            assessment = "TIGHTER_THAN_HISTORICAL_NORM"
            assessment_label = f"Tighter than Historical Norm by {abs(variance_vs_ref_bps):.1f} bps"
        else:
            assessment = "WIDER_THAN_HISTORICAL_NORM"
            assessment_label = f"Wider than Historical Norm by {abs(variance_vs_ref_bps):.1f} bps"

        return {
            "quote_rate": quote_rate,
            "spread_vs_mid": spread_vs_mid,
            "spread_vs_mid_bps": spread_vs_mid_bps,
            "spread_vs_mid_pips": spread_vs_mid_pips,
            "variance_vs_ref": variance_vs_ref,
            "variance_vs_ref_bps": variance_vs_ref_bps,
            "variance_vs_ref_pips": variance_vs_ref_pips,
            "assessment": assessment,
            "assessment_label": assessment_label,
            "is_favorable": is_favorable
        }


# Singleton instance
live_market_service = LiveMarketService()
