# app/services/sla_actualization_service.py
"""
SLA Actualization & Turnaround Intelligence Service
Manages real-world SLA performance actualization, rolling Exponential Moving Averages (EMA),
and hierarchical Bayesian credibility blending for facility recommendation scoring.
"""

import logging
from typing import Optional, Dict, Any, Tuple
from decimal import Decimal
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.models import Bank
from app.models.models_issuance import (
    IssuedLGRecord, IssuanceFacility, IssuanceFacilitySubLimit
)
from app.services.business_calendar_service import business_calendar_service

logger = logging.getLogger("app.sla_actualization")

# Exponential Moving Average smoothing factor (0.25 = gives strong weight to recent 4-10 transactions)
EMA_ALPHA = 0.25

# Minimum completed transactions before empirical data begins driving ranking
MIN_SAMPLE_SIZE_CREDIBILITY = 3

# Maximum empirical weight cap (leaves 15% anchor to contractual terms)
MAX_EMPIRICAL_WEIGHT = 0.85


class SLAActualizationService:
    """
    Core engine for computing moving actual turnaround times, SLA commitment rates,
    and effective SLA blending.
    """

    def calculate_credibility_weight(self, sample_size: int) -> float:
        """
        Bayesian credibility curve:
        W_N = min(0.85, N / (N + 3))
        N=0 -> 0%
        N=1 -> 25%
        N=3 -> 50%
        N=6 -> 66%
        N=10 -> 77%
        N>=17 -> 85% (capped)
        """
        if sample_size <= 0:
            return 0.0
        weight = float(sample_size) / float(sample_size + 3)
        return min(MAX_EMPIRICAL_WEIGHT, round(weight, 3))

    def record_issuance_turnaround(
        self,
        db: Session,
        issued_lg: IssuedLGRecord
    ) -> Optional[Dict[str, Any]]:
        """
        Records the actual business days turnaround when an LG issuance is completed / confirmed.
        Updates the Exponential Moving Average (EMA) and on-time compliance % for both
        the specific Facility and the issuing Bank.
        """
        if not issued_lg:
            return None

        # Determine start date (delivery to bank) and end date (bank reply / issue date)
        delivery_date = issued_lg.delivery_date
        reply_date = issued_lg.bank_reply_date or issued_lg.bank_lg_issue_date or issued_lg.issue_date

        if not delivery_date or not reply_date:
            logger.debug(f"LG #{issued_lg.lg_ref_number} missing delivery_date or reply_date. Skipping SLA actualization.")
            return None

        # Calculate exact net business days
        turnaround_days = business_calendar_service.calculate_business_turnaround_days(
            start_date=delivery_date,
            end_date=reply_date,
            db=db
        )

        # Retrieve linked facility
        facility = None
        if issued_lg.facility_sub_limit_id:
            sub = db.query(IssuanceFacilitySubLimit).filter(
                IssuanceFacilitySubLimit.id == issued_lg.facility_sub_limit_id
            ).first()
            if sub and sub.facility_id:
                facility = db.query(IssuanceFacility).filter(
                    IssuanceFacility.id == sub.facility_id
                ).first()

        # Retrieve linked bank
        bank_id = issued_lg.bank_id or (facility.bank_id if facility else None)
        bank = db.query(Bank).filter(Bank.id == bank_id).first() if bank_id else None

        agreed_sla = float(facility.sla_agreement_days) if (facility and facility.sla_agreement_days) else 7.0
        is_on_time = (turnaround_days <= agreed_sla)

        updated_metrics = {
            "lg_ref_number": issued_lg.lg_ref_number,
            "turnaround_business_days": turnaround_days,
            "agreed_sla_days": agreed_sla,
            "is_on_time": is_on_time
        }

        # 1. Update Facility Moving Metrics
        if facility:
            prev_fac_sla = float(facility.actual_avg_sla_days) if facility.actual_avg_sla_days is not None else None
            prev_fac_count = facility.total_completed_issuances or 0
            new_fac_count = prev_fac_count + 1
            facility.total_completed_issuances = new_fac_count

            if prev_fac_sla is None:
                new_fac_sla = turnaround_days
            else:
                # Exponential Moving Average: new = turnaround * alpha + prev * (1 - alpha)
                new_fac_sla = round((turnaround_days * EMA_ALPHA) + (prev_fac_sla * (1.0 - EMA_ALPHA)), 2)

            facility.actual_avg_sla_days = Decimal(str(new_fac_sla))

            # Update rolling commitment rate
            prev_comm = float(facility.sla_commitment_pct) if facility.sla_commitment_pct is not None else 100.0
            new_comm = round((100.0 if is_on_time else 0.0) * EMA_ALPHA + prev_comm * (1.0 - EMA_ALPHA), 1)
            facility.sla_commitment_pct = Decimal(str(new_comm))

            updated_metrics["facility_id"] = facility.id
            updated_metrics["facility_new_avg_sla"] = new_fac_sla
            updated_metrics["facility_commitment_pct"] = new_comm

        # 2. Update Bank Moving Metrics
        if bank:
            prev_bank_sla = float(bank.actual_avg_sla_days) if bank.actual_avg_sla_days is not None else None
            prev_bank_count = bank.total_completed_issuances or 0
            new_bank_count = prev_bank_count + 1
            bank.total_completed_issuances = new_bank_count

            if prev_bank_sla is None:
                new_bank_sla = turnaround_days
            else:
                new_bank_sla = round((turnaround_days * EMA_ALPHA) + (prev_bank_sla * (1.0 - EMA_ALPHA)), 2)

            bank.actual_avg_sla_days = Decimal(str(new_bank_sla))

            prev_bank_comm = float(bank.sla_commitment_pct) if bank.sla_commitment_pct is not None else 100.0
            new_bank_comm = round((100.0 if is_on_time else 0.0) * EMA_ALPHA + prev_bank_comm * (1.0 - EMA_ALPHA), 1)
            bank.sla_commitment_pct = Decimal(str(new_bank_comm))

            updated_metrics["bank_id"] = bank.id
            updated_metrics["bank_new_avg_sla"] = new_bank_sla
            updated_metrics["bank_commitment_pct"] = new_bank_comm

        try:
            db.commit()
            logger.info(
                f"SLA Actualized for LG {issued_lg.lg_ref_number}: "
                f"Turnaround={turnaround_days}d (Agreed={agreed_sla}d, OnTime={is_on_time})"
            )
        except Exception as e:
            db.rollback()
            logger.error(f"Error persisting SLA actualization metrics: {e}")

        return updated_metrics

    def compute_effective_sla(
        self,
        facility: Optional[IssuanceFacility],
        bank: Optional[Bank] = None
    ) -> Dict[str, Any]:
        """
        Computes the Effective SLA for facility recommendation ranking.
        Uses Hierarchical Bayesian Credibility Blending:
        - Primary: Facility actual EMA if facility sample size >= 3.
        - Fallback: Bank actual EMA if bank sample size >= 3.
        - Floor: Contractual agreed SLA.
        """
        agreed_sla = float(facility.sla_agreement_days) if (facility and facility.sla_agreement_days) else 7.0

        fac_count = (facility.total_completed_issuances or 0) if facility else 0
        fac_actual = float(facility.actual_avg_sla_days) if (facility and facility.actual_avg_sla_days is not None) else None
        fac_comm = float(facility.sla_commitment_pct) if (facility and facility.sla_commitment_pct is not None) else 100.0

        bank_count = (bank.total_completed_issuances or 0) if bank else 0
        bank_actual = float(bank.actual_avg_sla_days) if (bank and bank.actual_avg_sla_days is not None) else None
        bank_comm = float(bank.sla_commitment_pct) if (bank and bank.sla_commitment_pct is not None) else 100.0

        # Hierarchical Selection
        if fac_count >= MIN_SAMPLE_SIZE_CREDIBILITY and fac_actual is not None:
            actual_sla = fac_actual
            sample_size = fac_count
            commitment_pct = fac_comm
            source = "FACILITY"
        elif bank_count >= MIN_SAMPLE_SIZE_CREDIBILITY and bank_actual is not None:
            actual_sla = bank_actual
            sample_size = bank_count
            commitment_pct = bank_comm
            source = "BANK"
        elif fac_actual is not None:
            actual_sla = fac_actual
            sample_size = fac_count
            commitment_pct = fac_comm
            source = "FACILITY_EARLY"
        elif bank_actual is not None:
            actual_sla = bank_actual
            sample_size = bank_count
            commitment_pct = bank_comm
            source = "BANK_EARLY"
        else:
            actual_sla = agreed_sla
            sample_size = 0
            commitment_pct = 100.0
            source = "CONTRACTUAL"

        # Calculate credibility weight
        credibility_weight = self.calculate_credibility_weight(sample_size)

        # Blend effective SLA
        effective_sla = round(
            ((1.0 - credibility_weight) * agreed_sla) + (credibility_weight * actual_sla),
            2
        )

        drift_days = round(actual_sla - agreed_sla, 2)
        is_fast_track = effective_sla <= 2.5
        slippage_risk = (drift_days >= 1.5) or (commitment_pct < 65.0)

        return {
            "effective_sla_days": effective_sla,
            "agreed_sla_days": agreed_sla,
            "actual_avg_sla_days": actual_sla,
            "sla_commitment_pct": commitment_pct,
            "credibility_weight": credibility_weight,
            "sample_size": sample_size,
            "source": source,
            "drift_days": drift_days,
            "is_fast_track": is_fast_track,
            "slippage_risk": slippage_risk
        }


sla_actualization_service = SLAActualizationService()
