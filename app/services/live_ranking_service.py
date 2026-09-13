# app/services/live_ranking_service.py
from typing import Optional, List, Dict, Any
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
        assignment_id: str
    ) -> Optional[int]:
        """
        Calculates the 1-based numerical rank of the bank among submitted quotes.
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

        if rfq.type == "FX_SPOT":
            # For FX, get latest offer per assignment
            offers = db.query(QuotationOffer).filter(
                QuotationOffer.assignment_id.in_(assignment_ids)
            ).order_by(QuotationOffer.submitted_at.desc()).all()

            # Deduplicate by assignment_id (keep latest submitted)
            latest_by_assignment: Dict[str, float] = {}
            for off in offers:
                if off.assignment_id not in latest_by_assignment and off.price is not None:
                    latest_by_assignment[off.assignment_id] = float(off.price)

            if assignment_id not in latest_by_assignment:
                return None  # Bank hasn't submitted a quote yet

            # Sort based on direction:
            # Corporate Buy (e.g. Buy USD): Lower price is best (ascending)
            # Corporate Sell (e.g. Sell USD): Higher price is best (descending)
            is_sell = (rfq.direction or "").upper() == "SELL"
            sorted_items = sorted(
                latest_by_assignment.items(),
                key=lambda item: item[1],
                reverse=is_sell
            )

            # Find 1-based rank
            for idx, (aid, _) in enumerate(sorted_items):
                if aid == assignment_id:
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

live_ranking_service = LiveRankingService()
