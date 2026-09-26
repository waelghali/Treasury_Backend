# app/services/live_ranking_service.py
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import desc, asc
import logging

from app.models.models_quotation import (
    BankLiveRankingConfig, QuotationRequest, QuotationBankAssignment,
    QuotationOffer, QuotationTBillOffer
)

logger = logging.getLogger(__name__)

class LiveRankingService:
    """
    Evaluates whether Live Ranking is enabled for a given bank under specific customer/entity/service rules,
    and computes the bank's live numerical rank among competitors with zero disclosure of confidential data.
    """

    @staticmethod
    def evaluate_live_ranking_eligibility(
        db: Session,
        bank_id: int,
        customer_id: int,
        entity_id: Optional[int],
        trade_type: str
    ) -> bool:
        """
        Hierarchical evaluation of live ranking permissions:
        1. Specific Customer + Specific Entity (Highest specificity)
        2. Specific Customer + All Entities
        3. All Customers for this Bank & Service
        4. Default: False (disabled if no active matching rule)
        """
        if not bank_id:
            return False

        trade_type_filter = [trade_type.upper(), "BOTH"]

        # 1. Check Specific Customer + Specific Entity
        if entity_id:
            specific_entity_rule = db.query(BankLiveRankingConfig).filter(
                BankLiveRankingConfig.bank_id == bank_id,
                BankLiveRankingConfig.trade_type.in_(trade_type_filter),
                BankLiveRankingConfig.scope_type == "SPECIFIC_CUSTOMER",
                BankLiveRankingConfig.customer_id == customer_id,
                BankLiveRankingConfig.entity_scope_type == "SPECIFIC_ENTITY",
                BankLiveRankingConfig.entity_id == entity_id
            ).first()
            if specific_entity_rule:
                return specific_entity_rule.is_enabled

        # 2. Check Specific Customer + All Entities
        customer_rule = db.query(BankLiveRankingConfig).filter(
            BankLiveRankingConfig.bank_id == bank_id,
            BankLiveRankingConfig.trade_type.in_(trade_type_filter),
            BankLiveRankingConfig.scope_type == "SPECIFIC_CUSTOMER",
            BankLiveRankingConfig.customer_id == customer_id,
            BankLiveRankingConfig.entity_scope_type == "ALL_ENTITIES"
        ).first()
        if customer_rule:
            return customer_rule.is_enabled

        # 3. Check All Customers (Global Bank Rule)
        all_customers_rule = db.query(BankLiveRankingConfig).filter(
            BankLiveRankingConfig.bank_id == bank_id,
            BankLiveRankingConfig.trade_type.in_(trade_type_filter),
            BankLiveRankingConfig.scope_type == "ALL_CUSTOMERS"
        ).first()
        if all_customers_rule:
            return all_customers_rule.is_enabled

        return False

    @staticmethod
    def calculate_bank_live_rank(
        db: Session,
        rfq_id: str,
        assignment_id: str,
        leg_id: Optional[str] = None
    ) -> Optional[int]:
        """
        Calculates the 1-based numerical rank of the bank among submitted quotes for an RFQ or specific leg.
        Strict Privacy: Only returns the integer rank (1, 2, 3...). Never returns competitor names or prices.
        """
        rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
        if not rfq:
            return None

        # Fetch all assignments for this RFQ
        assignments = db.query(QuotationBankAssignment).filter(
            QuotationBankAssignment.rfq_id == rfq_id
        ).all()
        assignment_ids = [a.id for a in assignments]
        if assignment_id not in assignment_ids:
            return None

        # Resolve target leg
        from app.models.models_quotation import QuotationLeg
        target_leg = None
        if leg_id:
            target_leg = db.query(QuotationLeg).filter(QuotationLeg.id == leg_id, QuotationLeg.rfq_id == rfq_id).first()
        elif rfq.legs:
            target_leg = rfq.legs[0]

        if rfq.type == "FX_SPOT":
            assignment_map = {a.id: a for a in assignments}

            # Filter offers by target leg if available
            offers_query = db.query(QuotationOffer).filter(
                QuotationOffer.assignment_id.in_(assignment_ids)
            )
            if target_leg:
                offers_query = offers_query.filter(
                    (QuotationOffer.leg_id == target_leg.id) | (QuotationOffer.leg_id.is_(None))
                )
            offers = offers_query.order_by(QuotationOffer.submitted_at.desc()).all()

            latest_offers_by_assignment = {}
            for off in offers:
                if off.assignment_id not in latest_offers_by_assignment and off.price is not None:
                    latest_offers_by_assignment[off.assignment_id] = off

            if assignment_id not in latest_offers_by_assignment:
                return None  # Bank hasn't submitted a quote yet for this leg

            deal_amount = float((target_leg.amount if target_leg and target_leg.amount else rfq.amount) or 1.0)
            trade_direction = (target_leg.direction if target_leg and target_leg.direction else rfq.direction) or 'Buy'
            is_sell_dir = (trade_direction.lower() == 'sell')
            rfq_target_val_date = (target_leg.value_date if target_leg and target_leg.value_date else rfq.value_date)

            ranked_items = []
            for aid, off in latest_offers_by_assignment.items():
                a = assignment_map.get(aid)
                price = float(off.price)

                # Bank fee calculation using pair config if available
                raw_fee = 0.0
                if a:
                    cfg = a.get_config_for_leg(target_leg.id) if target_leg else a
                    base_deal_volume = deal_amount * price
                    raw_fee = (base_deal_volume * (float(cfg.cost_percent or 0) / 100.0)) + float(cfg.cost_flat or 0)
                    if cfg.cost_min and cfg.cost_min > 0:
                        raw_fee = max(raw_fee, float(cfg.cost_min))
                    if cfg.cost_max and cfg.cost_max > 0:
                        raw_fee = min(raw_fee, float(cfg.cost_max))

                fee_per_unit = raw_fee / deal_amount if deal_amount > 0 else 0.0
                final_all_in_price = round((price - fee_per_unit) if is_sell_dir else (price + fee_per_unit), 5)

                # TVM Normalization against Target Value Date
                offered_val_date = off.offered_value_date or (a.value_date if a else None) or rfq_target_val_date
                normalized_price = final_all_in_price

                if rfq_target_val_date and offered_val_date:
                    try:
                        target_dt = datetime.strptime(str(rfq_target_val_date).split('T')[0], "%Y-%m-%d").date()
                        offered_dt = datetime.strptime(str(offered_val_date).split('T')[0], "%Y-%m-%d").date()
                        delta_days = (offered_dt - target_dt).days
                        if delta_days != 0:
                            r_eval = (rfq.eval_rate or 20.25) / 100.0
                            normalized_price = round(final_all_in_price * (1.0 - (r_eval * (delta_days / 365.0))), 5)
                    except Exception as err:
                        logger.warning(f"Error computing TVM for live ranking: {err}")

                sub_ts = off.submitted_at.timestamp() if (off.submitted_at and hasattr(off.submitted_at, 'timestamp')) else float('inf')
                ranked_items.append({
                    "assignment_id": aid,
                    "normalized_price": normalized_price,
                    "submitted_ts": sub_ts
                })

            # Sort based on direction and tie-breaker
            if is_sell_dir:
                ranked_items.sort(key=lambda item: (-item["normalized_price"], item["submitted_ts"]))
            else:
                ranked_items.sort(key=lambda item: (item["normalized_price"], item["submitted_ts"]))

            # Find 1-based rank
            for idx, item in enumerate(ranked_items):
                if item["assignment_id"] == assignment_id:
                    return idx + 1

        elif rfq.type == "TBILL":
            # For T-Bills, get offers per assignment and compute average/effective discount rate
            tbill_offers = db.query(QuotationTBillOffer).filter(
                QuotationTBillOffer.assignment_id.in_(assignment_ids)
            ).all()

            rates_by_assignment: Dict[str, List[float]] = {}
            for off in tbill_offers:
                if off.discount_rate is not None:
                    rates_by_assignment.setdefault(off.assignment_id, []).append(float(off.discount_rate))

            if assignment_id not in rates_by_assignment:
                return None

            # For each bank, take the best (highest) discount rate offered
            best_rate_by_assignment = {
                aid: max(rates) for aid, rates in rates_by_assignment.items() if rates
            }

            # Higher discount rate / yield is better for corporate buyer (descending sort)
            is_sell = (rfq.direction or "").upper() == "SELL"
            sorted_items = sorted(
                best_rate_by_assignment.items(),
                key=lambda item: item[1],
                reverse=not is_sell  # Buy = highest rate is best (descending)
            )

            for idx, (aid, _) in enumerate(sorted_items):
                if aid == assignment_id:
                    return idx + 1

        return None

    @staticmethod
    def calculate_bank_live_ranks_by_leg(
        db: Session,
        rfq_id: str,
        assignment_id: str
    ) -> Dict[str, Optional[int]]:
        """Calculates live rank for every leg of an RFQ where the bank is invited."""
        rfq = db.query(QuotationRequest).filter(QuotationRequest.id == rfq_id).first()
        if not rfq:
            return {}
        results = {}
        for leg in rfq.legs:
            results[leg.id] = LiveRankingService.calculate_bank_live_rank(
                db=db, rfq_id=rfq_id, assignment_id=assignment_id, leg_id=leg.id
            )
        return results

live_ranking_service = LiveRankingService()
