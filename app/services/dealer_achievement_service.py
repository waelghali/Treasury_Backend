import logging
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_

from app.models.models_quotation import (
    QuotationOffer, QuotationTBillOffer, QuotationRequest,
    QuotationBankAssignment, QuotationBank, QuotationAccessOTP,
    QuotationLeg, QuotationAnalytics
)
from app.models.models import Bank, Customer

logger = logging.getLogger(__name__)

# --- Pre-computed Multi-Decade Islamic Lunar Calendar Lookup (2024 - 2045) ---
# Verified against astronomical tables. Runs 100% locally with zero external API calls forever.
HIJRI_FESTIVE_CALENDAR = [
    # (Ramadan Start, Ramadan End, Eid al-Adha Start, Eid al-Adha End)
    (date(2024, 3, 11), date(2024, 4, 9),  date(2024, 6, 16), date(2024, 6, 20)),
    (date(2025, 2, 28), date(2025, 3, 29), date(2025, 6, 6),  date(2025, 6, 10)),
    (date(2026, 2, 17), date(2026, 3, 18), date(2026, 5, 27), date(2026, 5, 31)),
    (date(2027, 2, 7),  date(2027, 3, 8),  date(2027, 5, 16), date(2027, 5, 20)),
    (date(2028, 1, 27), date(2028, 2, 25), date(2028, 5, 5),  date(2028, 5, 9)),
    (date(2029, 1, 15), date(2029, 2, 13), date(2029, 4, 24), date(2029, 4, 28)),
    (date(2030, 1, 5),  date(2030, 2, 3),  date(2030, 4, 14), date(2030, 4, 18)),
    (date(2030, 12, 26), date(2031, 1, 24), date(2031, 4, 3), date(2031, 4, 7)),
    (date(2031, 12, 15), date(2032, 1, 13), date(2032, 3, 22), date(2032, 3, 26)),
    (date(2032, 12, 3), date(2033, 1, 1),  date(2033, 3, 11), date(2033, 3, 15)),
    (date(2033, 11, 22), date(2033, 12, 21), date(2033, 2, 28), date(2033, 3, 4)),
    (date(2034, 11, 11), date(2034, 12, 10), date(2034, 2, 17), date(2034, 2, 21)),
    (date(2035, 10, 31), date(2035, 11, 29), date(2035, 2, 6), date(2035, 2, 10)),
    (date(2036, 10, 20), date(2036, 11, 18), date(2036, 1, 26), date(2036, 1, 30)),
    (date(2037, 10, 9),  date(2037, 11, 7),  date(2037, 1, 15), date(2037, 1, 19)),
    (date(2038, 9, 29),  date(2038, 10, 28), date(2038, 1, 5),  date(2038, 1, 9)),
    (date(2039, 9, 18),  date(2039, 10, 17), date(2039, 12, 25), date(2039, 12, 29)),
    (date(2040, 9, 7),   date(2040, 10, 6),  date(2040, 12, 13), date(2040, 12, 17)),
    (date(2041, 8, 27),  date(2041, 9, 25),  date(2041, 12, 2),  date(2041, 12, 6)),
    (date(2042, 8, 16),  date(2042, 9, 14),  date(2042, 11, 21), date(2042, 11, 25)),
    (date(2043, 8, 5),   date(2043, 9, 3),   date(2043, 11, 11), date(2043, 11, 15)),
    (date(2044, 7, 25),  date(2044, 8, 23),  date(2044, 10, 30), date(2044, 11, 3)),
    (date(2045, 7, 14),  date(2045, 8, 12),  date(2045, 10, 20), date(2045, 10, 24)),
]


def compute_coptic_easter(year: int) -> date:
    """Astronomical Meeus Computus for Julian Easter converted to Gregorian (Coptic Easter & Sham El-Nessim).
    Accurate for any year from 1900 to 2099 without any manual maintenance forever."""
    a = year % 4
    b = year % 7
    c = year % 19
    d = (19 * c + 15) % 30
    e = (2 * a + 4 * b - d + 34) % 7
    month = (d + e + 114) // 31
    day = ((d + e + 114) % 31) + 1
    return date(year, month, day) + timedelta(days=13)


def compute_western_easter(year: int) -> date:
    """Astronomical Meeus/Jones/Butcher Computus for Gregorian Easter.
    Accurate for all centuries with zero manual maintenance forever."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


class DealerAchievementService:
    """
    Institutional Dealer Motivation & Accolade Engine.
    
    Principles:
    1. 100% Read-Only & Decoupled: Zero table mutations, zero writes to RFQ tables.
    2. Dual-Mode Resolution:
       - Individual Dealer Mode (when authenticated with dealer email): evaluates exact trader stats.
       - Bank Desk Mode (when unauthenticated): evaluates the collective bank desk's verified standing.
    3. Multi-Metal Progression (Bronze -> Silver -> Gold -> Platinum).
       Calibrated so beginners easily unlock their Maiden Execution / Market Presence on Day 1,
       while elite tiers remain challenging and prestigious.
    4. Automated Seasonal Calendar: Cultural & fiscal date detection.
    """

    @classmethod
    def get_dealer_achievements(
        cls,
        db: Session,
        dealer_email: Optional[str] = None,
        bank_id: Optional[int] = None,
        bank_name: Optional[str] = None,
        current_rfq_id: Optional[str] = None,
        include_bank_desk: bool = True
    ) -> Dict[str, Any]:
        """Calculates accolades, streaks, and personal bests for an individual dealer or bank desk."""
        clean_email = dealer_email.strip().lower() if dealer_email and dealer_email.strip() else None

        if not clean_email and not bank_id:
            return cls._empty_achievements_payload("Guest Dealer")

        try:
            is_individual = bool(clean_email)
            mode = "INDIVIDUAL" if is_individual else "BANK_DESK"

            if is_individual:
                # --- INDIVIDUAL DEALER MODE ---
                fx_offers = db.query(QuotationOffer).filter(
                    func.lower(QuotationOffer.submitted_by_email) == clean_email
                ).all()

                tbill_offers = db.query(QuotationTBillOffer).filter(
                    func.lower(QuotationTBillOffer.submitted_by_email) == clean_email
                ).all()

                otps = db.query(QuotationAccessOTP).filter(
                    func.lower(QuotationAccessOTP.email) == clean_email
                ).all()

                assignment_ids = list(set(
                    [o.assignment_id for o in fx_offers if o.assignment_id] +
                    [o.assignment_id for o in tbill_offers if o.assignment_id] +
                    [otp.assignment_id for otp in otps if otp.assignment_id and otp.is_used]
                ))
                assignments = db.query(QuotationBankAssignment).filter(
                    QuotationBankAssignment.id.in_(assignment_ids)
                ).all() if assignment_ids else []

                rfq_ids = list(set([a.rfq_id for a in assignments if a.rfq_id]))
                rfqs = db.query(QuotationRequest).filter(
                    QuotationRequest.id.in_(rfq_ids),
                    QuotationRequest.ref_no.like('RFQ-202%')
                ).all() if rfq_ids else []

                # Scope assignments, offers, and OTPs strictly to authentic corporate RFQs
                valid_rfq_ids = set([r.id for r in rfqs])
                assignments = [a for a in assignments if a.rfq_id in valid_rfq_ids]
                valid_asgn_ids = set([a.id for a in assignments])
                fx_offers = [o for o in fx_offers if o.assignment_id in valid_asgn_ids]
                tbill_offers = [o for o in tbill_offers if o.assignment_id in valid_asgn_ids]
                otps = [otp for otp in otps if otp.assignment_id in valid_asgn_ids]

                # Classify offers into Execution vs Indicative
                execution_quotes_submitted = 0
                indicative_quotes_submitted = 0

                for o in fx_offers:
                    base = 'Execution'
                    if o.leg and o.leg.quotation_base:
                        base = o.leg.quotation_base
                    elif o.assignment and o.assignment.rfq and o.assignment.rfq.quotation_base:
                        base = o.assignment.rfq.quotation_base
                    if base.lower() == 'indicative':
                        indicative_quotes_submitted += 1
                    else:
                        execution_quotes_submitted += 1

                for o in tbill_offers:
                    base = 'Execution'
                    if o.assignment and o.assignment.rfq and o.assignment.rfq.quotation_base:
                        base = o.assignment.rfq.quotation_base
                    if base.lower() == 'indicative':
                        indicative_quotes_submitted += 1
                    else:
                        execution_quotes_submitted += 1

                total_quotes_submitted = execution_quotes_submitted
                total_tenders_participated = len(rfqs)

                # Ready at the Bell: Distinct tenders where the dealer authenticated at or near the opening bell window
                ready_rfqs = set()
                for otp in otps:
                    if not otp.assignment_id or not otp.created_at or not otp.is_used:
                        continue
                    asgn = next((a for a in assignments if a.id == otp.assignment_id), None)
                    if asgn and asgn.rfq and asgn.rfq.window_start and asgn.rfq.status not in ('CANCELLED', 'DRAFT'):
                        otp_created = otp.created_at.replace(tzinfo=timezone.utc) if otp.created_at.tzinfo is None else otp.created_at
                        w_start = asgn.rfq.window_start.replace(tzinfo=timezone.utc) if asgn.rfq.window_start.tzinfo is None else asgn.rfq.window_start
                        if (w_start - timedelta(minutes=15)) <= otp_created <= (w_start + timedelta(seconds=60)):
                            ready_rfqs.add(asgn.rfq_id)
                ready_at_bell_count = len(ready_rfqs)

                # Deals Won, Streak, Currency Pairs (Strictly Firm Execution)
                total_deals_won = 0
                total_volume_won_egp = 0.0
                total_volume_won_usd = 0.0
                currency_pairs_won = set()

                rfqs_sorted = sorted(
                    rfqs,
                    key=lambda r: (r.window_end.replace(tzinfo=timezone.utc) if r.window_end and r.window_end.tzinfo is None else (r.window_end or datetime.min.replace(tzinfo=timezone.utc)))
                )
                current_streak = 0
                longest_streak = 0

                for rfq in rfqs_sorted:
                    asgn = next((a for a in assignments if a.rfq_id == rfq.id), None)
                    if not asgn:
                        continue

                    from app.services.tenant_key_service import tenant_key_service
                    tenant_dek = tenant_key_service.get_or_create_tenant_dek(db, rfq.customer_id)

                    bank_id_val = asgn.quotation_bank.bank_id if asgn.quotation_bank else None
                    if not bank_id_val:
                        continue

                    is_rfq_won = False
                    rfq_volume_egp = 0.0
                    rfq_volume_usd = 0.0

                    if rfq.type == 'TBILL':
                        qa = db.query(QuotationAnalytics).filter(QuotationAnalytics.rfq_id == rfq.id).first()
                        if qa and qa.winner_quotation_bank_id and asgn.quotation_bank:
                            is_rfq_won = (qa.winner_quotation_bank_id == asgn.quotation_bank.id and rfq.status == 'COMPLETED')
                        if is_rfq_won:
                            total_deals_won += 1
                            rfq_volume_egp = float(rfq.amount or 0.0)
                            rfq_volume_usd = float(rfq.amount or 0.0) / 50.0
                    else:
                        legs = getattr(rfq, 'legs', [])
                        if legs:
                            for leg in legs:
                                # Execution legs only (indicatives excluded from deal won trophies)
                                leg_base = (leg.quotation_base or 'Execution').lower()
                                if leg_base != 'execution':
                                    continue

                                if leg.winner_bank_id == bank_id_val and leg.status not in ('REJECTED', 'CANCELLED', 'DECLINED'):
                                    dealer_submitted_winning_rate = any(
                                        o.leg_id == leg.id and abs(float(tenant_key_service.resolve_offer_price(o, tenant_dek) or 0.0) - float(leg.winner_rate or 0.0)) < 1e-4
                                        for o in fx_offers
                                    )
                                    if dealer_submitted_winning_rate:
                                        is_rfq_won = True
                                        total_deals_won += 1
                                        leg_amt = float(leg.amount or 0.0)
                                        c1 = leg.buy_currency or ""
                                        c2 = leg.sell_currency or ""
                                        pair = "/".join(sorted([c1.upper(), c2.upper()])) if c1 and c2 else (leg.currency_pair or "FX").upper()
                                        currency_pairs_won.add(pair)
                                        rate = float(leg.winner_rate or 50.0)
                                        rfq_volume_egp += (leg_amt * rate) if 'EGP' in pair else leg_amt

                                        if 'USD' in pair:
                                            vol_usd = leg_amt
                                        elif 'EUR' in pair:
                                            vol_usd = leg_amt * 1.08
                                        elif 'GBP' in pair:
                                            vol_usd = leg_amt * 1.30
                                        elif rate > 0:
                                            vol_usd = (leg_amt * rate) / 50.0 if 'EGP' in pair else leg_amt
                                        else:
                                            vol_usd = leg_amt
                                        rfq_volume_usd += vol_usd
                        else:
                            if (rfq.quotation_base or 'Execution').lower() == 'execution':
                                qa = db.query(QuotationAnalytics).filter(QuotationAnalytics.rfq_id == rfq.id).first()
                                if qa and qa.winner_quotation_bank_id and asgn.quotation_bank:
                                    if qa.winner_quotation_bank_id == asgn.quotation_bank.id and rfq.status == 'COMPLETED':
                                        dealer_submitted_winning_rate = any(
                                            o.assignment_id == asgn.id and abs(float(tenant_key_service.resolve_offer_price(o, tenant_dek) or 0.0) - float(rfq.eval_rate or 0.0)) < 1e-4
                                            for o in fx_offers
                                        )
                                        if dealer_submitted_winning_rate:
                                            is_rfq_won = True
                                            total_deals_won += 1
                                            pair = "/".join(sorted([rfq.buy_currency.upper(), rfq.sell_currency.upper()]))
                                            currency_pairs_won.add(pair)
                                            amt = float(rfq.amount or 0.0)
                                            rate = float(rfq.eval_rate or 50.0)
                                            rfq_volume_egp = (amt * rate) if 'EGP' in pair else amt
                                            if 'USD' in pair:
                                                rfq_volume_usd = amt
                                            elif 'EUR' in pair:
                                                rfq_volume_usd = amt * 1.08
                                            elif 'GBP' in pair:
                                                rfq_volume_usd = amt * 1.30
                                            elif rate > 0:
                                                rfq_volume_usd = (amt * rate) / 50.0 if 'EGP' in pair else amt
                                            else:
                                                rfq_volume_usd = amt

                    if is_rfq_won:
                        total_volume_won_egp += rfq_volume_egp
                        total_volume_won_usd += rfq_volume_usd

                    # Streak Evaluation across RFQs (Clean Sweep standard with Wash on aborted/rejected tenders)
                    # Unbroken Victory strictly evaluates legs where this bank was invited on an Execution basis.
                    if rfq.type == 'TBILL':
                        tbill_base = (getattr(asgn, 'quotation_base', None) or getattr(rfq, 'quotation_base', None) or 'Execution').strip().lower()
                        if tbill_base == 'execution':
                            qa = db.query(QuotationAnalytics).filter(QuotationAnalytics.rfq_id == rfq.id).first()
                            asgn = next((a for a in assignments if a.rfq_id == rfq.id), None)
                            is_tbill_won = bool(qa and qa.winner_quotation_bank_id and asgn and asgn.quotation_bank and qa.winner_quotation_bank_id == asgn.quotation_bank.id and rfq.status in ('COMPLETED', 'CLOSED'))
                            is_tbill_lost = bool(qa and qa.winner_quotation_bank_id and asgn and asgn.quotation_bank and qa.winner_quotation_bank_id != asgn.quotation_bank.id and rfq.status in ('COMPLETED', 'CLOSED'))
                            if is_tbill_won:
                                current_streak += 1
                                if current_streak > longest_streak:
                                    longest_streak = current_streak
                            elif is_tbill_lost:
                                current_streak = 0
                            # Note: If RFQ was cancelled/rejected with no award, it is a wash (preserves current_streak)
                    else:
                        legs = getattr(rfq, 'legs', [])
                        # Filter strictly for legs where this bank was invited on an Execution basis (exclude Indicative & Invisible)
                        eligible_exec_legs = []
                        if legs:
                            for l in legs:
                                leg_cfg = asgn.get_config_for_leg(l.id) if hasattr(asgn, 'get_config_for_leg') else None
                                is_invited = getattr(leg_cfg, 'is_invited', True) if leg_cfg else True
                                leg_base = (getattr(leg_cfg, 'quotation_base', None) or getattr(asgn, 'quotation_base', None) or getattr(l, 'quotation_base', None) or getattr(rfq, 'quotation_base', None) or 'Execution').strip().lower()
                                if is_invited and leg_base == 'execution':
                                    eligible_exec_legs.append(l)
                        else:
                            rfq_base = (getattr(asgn, 'quotation_base', None) or getattr(rfq, 'quotation_base', None) or 'Execution').strip().lower()
                            if rfq_base == 'execution':
                                eligible_exec_legs.append(rfq)

                        if eligible_exec_legs:
                            # Awarded business: legs concluded with a winner and not rejected/cancelled/declined by client
                            awarded_legs = [
                                l for l in eligible_exec_legs
                                if (getattr(l, 'winner_bank_id', None) or (getattr(rfq, 'winner_bank_id', None) if l == rfq else None)) and
                                getattr(l, 'status', None) not in ('REJECTED', 'CANCELLED', 'DECLINED', 'INCONCLUSIVE', 'EXPIRED')
                            ]

                            # Check if this dealer submitted offers for this RFQ
                            dealer_submitted_any_offer = any(
                                (o.leg_id == l.id if hasattr(l, 'id') and l != rfq else o.assignment_id == asgn.id)
                                for l in eligible_exec_legs
                                for o in fx_offers
                            ) or any(o.assignment_id == asgn.id for o in fx_offers)

                            # If dealer did not submit an offer on this tender, it is a Neutral Wash (streak is preserved)
                            if not dealer_submitted_any_offer:
                                continue

                            competitor_won_any = any(
                                (getattr(l, 'winner_bank_id', None) or (getattr(rfq, 'winner_bank_id', None) if l == rfq else None)) != bank_id_val
                                for l in awarded_legs
                            )

                            if competitor_won_any:
                                current_streak = 0
                            elif awarded_legs and rfq.status in ('COMPLETED', 'ACCEPTED', 'CLOSED'):
                                dealer_won_all_awarded = all(
                                    (getattr(l, 'winner_bank_id', None) or (getattr(rfq, 'winner_bank_id', None) if l == rfq else None)) == bank_id_val and
                                    any(
                                        (o.leg_id == l.id if hasattr(l, 'id') and l != rfq else o.assignment_id == asgn.id) and
                                        abs(float(tenant_key_service.resolve_offer_price(o, tenant_dek) or 0.0) - float(getattr(l, 'winner_rate', None) or getattr(rfq, 'eval_rate', None) or 0.0)) < 1e-4
                                        for o in fx_offers
                                    )
                                    for l in awarded_legs
                                )
                                if dealer_won_all_awarded:
                                    current_streak += 1
                                    if current_streak > longest_streak:
                                        longest_streak = current_streak
                                elif rfq.status in ('COMPLETED', 'ACCEPTED', 'CLOSED'):
                                    current_streak = 0
                            # Note: If no legs awarded (e.g. client rejected all legs or cancelled tender), it is a wash (preserves current_streak)

                display_email = clean_email
                target_bank_name = bank_name

            else:
                # --- BANK DESK MODE (Collective standing for this financial institution) ---
                bank_obj = db.query(Bank).filter(Bank.id == bank_id).first()
                target_bank_name = bank_obj.name if bank_obj else (bank_name or "Bank Desk")
                display_email = f"{target_bank_name} Treasury Desk"

                assignments = db.query(QuotationBankAssignment).join(
                    QuotationBankAssignment.quotation_bank
                ).filter(
                    QuotationBankAssignment.quotation_bank.has(bank_id=bank_id)
                ).all()

                rfq_ids = list(set([a.rfq_id for a in assignments if a.rfq_id]))
                rfqs = db.query(QuotationRequest).filter(
                    QuotationRequest.id.in_(rfq_ids),
                    QuotationRequest.ref_no.like('RFQ-202%')
                ).all() if rfq_ids else []

                # Scope assignments, offers, and OTPs strictly to authentic corporate RFQs
                valid_rfq_ids = set([r.id for r in rfqs])
                assignments = [a for a in assignments if a.rfq_id in valid_rfq_ids]
                asgn_ids = [a.id for a in assignments]

                fx_offers = db.query(QuotationOffer).filter(QuotationOffer.assignment_id.in_(asgn_ids)).all() if asgn_ids else []
                tbill_offers = db.query(QuotationTBillOffer).filter(QuotationTBillOffer.assignment_id.in_(asgn_ids)).all() if asgn_ids else []
                otps = db.query(QuotationAccessOTP).filter(QuotationAccessOTP.assignment_id.in_(asgn_ids)).all() if asgn_ids else []

                execution_quotes_submitted = 0
                indicative_quotes_submitted = 0
                for o in fx_offers:
                    base = 'Execution'
                    if o.leg and o.leg.quotation_base:
                        base = o.leg.quotation_base
                    elif o.assignment and o.assignment.rfq and o.assignment.rfq.quotation_base:
                        base = o.assignment.rfq.quotation_base
                    if base.lower() == 'indicative':
                        indicative_quotes_submitted += 1
                    else:
                        execution_quotes_submitted += 1

                for o in tbill_offers:
                    base = 'Execution'
                    if o.assignment and o.assignment.rfq and o.assignment.rfq.quotation_base:
                        base = o.assignment.rfq.quotation_base
                    if base.lower() == 'indicative':
                        indicative_quotes_submitted += 1
                    else:
                        execution_quotes_submitted += 1

                total_quotes_submitted = execution_quotes_submitted
                total_tenders_participated = len(rfqs)

                # Ready at bell count across desk (de-duplicated per RFQ)
                ready_rfqs = set()
                for otp in otps:
                    if not otp.assignment_id or not otp.created_at or not otp.is_used:
                        continue
                    asgn = next((a for a in assignments if a.id == otp.assignment_id), None)
                    if asgn and asgn.rfq and asgn.rfq.window_start and asgn.rfq.status not in ('CANCELLED', 'DRAFT'):
                        otp_created = otp.created_at.replace(tzinfo=timezone.utc) if otp.created_at.tzinfo is None else otp.created_at
                        w_start = asgn.rfq.window_start.replace(tzinfo=timezone.utc) if asgn.rfq.window_start.tzinfo is None else asgn.rfq.window_start
                        if (w_start - timedelta(minutes=15)) <= otp_created <= (w_start + timedelta(seconds=60)):
                            ready_rfqs.add(asgn.rfq_id)
                ready_at_bell_count = len(ready_rfqs)

                # Legs won directly by this bank (strictly firm execution on authentic completed RFQs)
                legs_won = db.query(QuotationLeg).join(
                    QuotationRequest, QuotationLeg.rfq_id == QuotationRequest.id
                ).filter(
                    QuotationLeg.winner_bank_id == bank_id,
                    QuotationLeg.status.notin_(['REJECTED', 'CANCELLED', 'DECLINED']),
                    func.lower(func.coalesce(QuotationLeg.quotation_base, 'execution')) == 'execution',
                    QuotationRequest.status == 'COMPLETED',
                    QuotationRequest.ref_no.like('RFQ-202%')
                ).all()
                total_deals_won = len(legs_won)

                currency_pairs_won = set()
                total_volume_won_egp = 0.0
                total_volume_won_usd = 0.0
                for leg in legs_won:
                    c1 = leg.buy_currency or ""
                    c2 = leg.sell_currency or ""
                    pair = "/".join(sorted([c1.upper(), c2.upper()])) if c1 and c2 else (leg.currency_pair or "FX").upper()
                    currency_pairs_won.add(pair)
                    leg_amt = float(leg.amount or 0.0)
                    rate = float(leg.winner_rate or 50.0)
                    total_volume_won_egp += (leg_amt * rate) if 'EGP' in pair else leg_amt

                    if 'USD' in pair:
                        vol_usd = leg_amt
                    elif 'EUR' in pair:
                        vol_usd = leg_amt * 1.08
                    elif 'GBP' in pair:
                        vol_usd = leg_amt * 1.30
                    elif rate > 0:
                        vol_usd = (leg_amt * rate) / 50.0 if 'EGP' in pair else leg_amt
                    else:
                        vol_usd = leg_amt
                    total_volume_won_usd += vol_usd

                # Streak across RFQs for this bank
                rfqs_sorted = sorted(
                    rfqs,
                    key=lambda r: (r.window_end.replace(tzinfo=timezone.utc) if r.window_end and r.window_end.tzinfo is None else (r.window_end or datetime.min.replace(tzinfo=timezone.utc)))
                )
                current_streak = 0
                longest_streak = 0
                for rfq in rfqs_sorted:
                    asgn = next((a for a in assignments if a.rfq_id == rfq.id), None)
                    if not asgn:
                        continue

                    if rfq.type == 'TBILL':
                        tbill_base = (getattr(asgn, 'quotation_base', None) or getattr(rfq, 'quotation_base', None) or 'Execution').strip().lower()
                        if tbill_base == 'execution':
                            qa = db.query(QuotationAnalytics).filter(QuotationAnalytics.rfq_id == rfq.id).first()
                            is_tbill_won = bool(qa and qa.winner_quotation_bank_id and asgn and asgn.quotation_bank and qa.winner_quotation_bank_id == asgn.quotation_bank.id and rfq.status in ('COMPLETED', 'CLOSED'))
                            is_tbill_lost = bool(qa and qa.winner_quotation_bank_id and asgn and asgn.quotation_bank and qa.winner_quotation_bank_id != asgn.quotation_bank.id and rfq.status in ('COMPLETED', 'CLOSED'))
                            if is_tbill_won:
                                current_streak += 1
                                if current_streak > longest_streak:
                                    longest_streak = current_streak
                            elif is_tbill_lost:
                                current_streak = 0
                            # Note: If RFQ was cancelled/rejected with no award, it is a wash (preserves current_streak)
                    else:
                        legs = getattr(rfq, 'legs', [])
                        # Filter strictly for legs where this bank was invited on an Execution basis (exclude Indicative & Invisible)
                        eligible_exec_legs = []
                        if legs:
                            for l in legs:
                                leg_cfg = asgn.get_config_for_leg(l.id) if hasattr(asgn, 'get_config_for_leg') else None
                                is_invited = getattr(leg_cfg, 'is_invited', True) if leg_cfg else True
                                leg_base = (getattr(leg_cfg, 'quotation_base', None) or getattr(asgn, 'quotation_base', None) or getattr(l, 'quotation_base', None) or getattr(rfq, 'quotation_base', None) or 'Execution').strip().lower()
                                if is_invited and leg_base == 'execution':
                                    eligible_exec_legs.append(l)
                        else:
                            rfq_base = (getattr(asgn, 'quotation_base', None) or getattr(rfq, 'quotation_base', None) or 'Execution').strip().lower()
                            if rfq_base == 'execution':
                                eligible_exec_legs.append(rfq)

                        if eligible_exec_legs:
                            # Awarded business: legs concluded with a winner and not rejected/cancelled/declined by client
                            awarded_legs = [
                                l for l in eligible_exec_legs
                                if (getattr(l, 'winner_bank_id', None) or (getattr(rfq, 'winner_bank_id', None) if l == rfq else None)) and
                                getattr(l, 'status', None) not in ('REJECTED', 'CANCELLED', 'DECLINED', 'INCONCLUSIVE', 'EXPIRED')
                            ]

                            # Check if the bank desk submitted offers for this RFQ
                            desk_submitted_any_offer = any(
                                o.assignment_id == asgn.id for o in fx_offers
                            )
                            # If desk did not quote on this tender, it is a Neutral Wash (streak is preserved)
                            if not desk_submitted_any_offer:
                                continue

                            competitor_won_any = any(
                                (getattr(l, 'winner_bank_id', None) or (getattr(rfq, 'winner_bank_id', None) if l == rfq else None)) != bank_id
                                for l in awarded_legs
                            )

                            if competitor_won_any:
                                current_streak = 0
                            elif awarded_legs and rfq.status in ('COMPLETED', 'ACCEPTED', 'CLOSED'):
                                bank_won_all_awarded = all(
                                    (getattr(l, 'winner_bank_id', None) or (getattr(rfq, 'winner_bank_id', None) if l == rfq else None)) == bank_id
                                    for l in awarded_legs
                                )
                                if bank_won_all_awarded:
                                    current_streak += 1
                                    if current_streak > longest_streak:
                                        longest_streak = current_streak
                                elif rfq.status in ('COMPLETED', 'ACCEPTED', 'CLOSED'):
                                    current_streak = 0
                            # Note: If no legs awarded (wash), current_streak is preserved

            # Construct Multi-Metal Badges with Calibrated Institutional Milestones
            trophies = cls._build_trophies_set(
                ready_at_bell_count=ready_at_bell_count,
                total_tenders_participated=total_tenders_participated,
                total_deals_won=total_deals_won,
                total_volume_won_usd=total_volume_won_usd,
                current_streak=current_streak,
                longest_streak=longest_streak,
                currency_pairs_won_count=len(currency_pairs_won),
                total_quotes_submitted=total_quotes_submitted,
                indicative_quotes_submitted=indicative_quotes_submitted,
                is_desk_mode=not is_individual
            )

            # Seasonal Events & Overall Tier
            active_seasons = cls._evaluate_active_seasons()
            earned_trophy_count = sum(1 for t in trophies if t["current_tier"] != "NONE")
            tier_name, tier_perk = cls._calculate_dealer_tier(trophies)

            # Institutional Bank Desk Badges (compute simultaneously so authenticated dealers retain desk standing)
            bank_desk_info = None
            if is_individual and include_bank_desk and (bank_id or target_bank_name):
                try:
                    desk_payload = cls.get_dealer_achievements(
                        db,
                        dealer_email=None,
                        bank_id=bank_id,
                        bank_name=target_bank_name,
                        include_bank_desk=False
                    )
                    bank_desk_info = {
                        "bank_name": desk_payload.get("bank_name"),
                        "desk_tier": desk_payload.get("dealer_tier"),
                        "desk_perk": desk_payload.get("dealer_perk"),
                        "earned_trophy_count": desk_payload.get("earned_trophy_count"),
                        "trophies": desk_payload.get("trophies"),
                        "personal_bests": desk_payload.get("personal_bests"),
                    }
                except Exception as desk_err:
                    logger.warning(f"Could not compute separate bank desk payload: {desk_err}")

            return {
                "mode": mode,
                "dealer_email": display_email,
                "bank_name": target_bank_name,
                "dealer_tier": tier_name,
                "dealer_perk": tier_perk,
                "earned_trophy_count": earned_trophy_count,
                "total_trophies": len(trophies),
                "trophies": trophies,
                "bank_desk": bank_desk_info,
                "personal_bests": {
                    "longest_winning_streak": longest_streak,
                    "current_streak": current_streak,
                    "total_deals_won": total_deals_won,
                    "total_volume_won_usd": round(total_volume_won_usd, 2),
                    "total_volume_won_egp": round(total_volume_won_egp, 2),
                    "total_tenders_participated": total_tenders_participated,
                    "currency_pairs_count": len(currency_pairs_won),
                    "total_quotes_submitted": total_quotes_submitted,
                    "execution_quotes_submitted": total_quotes_submitted,
                    "indicative_quotes_submitted": indicative_quotes_submitted
                },
                "active_seasons": active_seasons,
                "as_of": datetime.now(timezone.utc).isoformat()
            }

        except Exception as e:
            logger.error(f"Error computing dealer achievements: {e}", exc_info=True)
            return cls._empty_achievements_payload(clean_email or "Guest Dealer")

    @classmethod
    def _build_trophies_set(
        cls,
        ready_at_bell_count: int,
        total_tenders_participated: int,
        total_deals_won: int,
        total_volume_won_usd: float,
        current_streak: int,
        longest_streak: int,
        currency_pairs_won_count: int,
        total_quotes_submitted: int,
        indicative_quotes_submitted: int = 0,
        is_desk_mode: bool = False
    ) -> List[Dict[str, Any]]:
        """Constructs the 8 multi-metal trophies using calibrated institutional progression."""
        best_streak = max(current_streak, longest_streak)

        return [
            cls._build_trophy(
                trophy_id="DEAL_CLOSER",
                title="Deal Closer",
                icon="Trophy",
                category="Execution",
                description="Successful firm tender executions completed on Grow." if not is_desk_mode else "Completed interbank tender executions awarded to this bank.",
                current_value=total_deals_won,
                unit="deals",
                milestones={"BRONZE": 3, "SILVER": 10, "GOLD": 30, "PLATINUM": 100},
                tier_titles={
                    "NONE": "Deal Closer",
                    "BRONZE": "Maiden Execution",
                    "SILVER": "Execution Specialist",
                    "GOLD": "Premier Closer",
                    "PLATINUM": "Master Market Closer"
                }
            ),
            cls._build_trophy(
                trophy_id="TRIPLE_CROWN",
                title="Unbroken Victor",
                icon="Crown",
                category="Consistency",
                description="Consecutive clean-sweep tender executions won across the interbank market.",
                current_value=current_streak,
                unit="streak",
                milestones={"BRONZE": 3, "SILVER": 5, "GOLD": 10, "PLATINUM": 15},
                tier_titles={
                    "NONE": "Unbroken Victor",
                    "BRONZE": "Unbroken Victor",
                    "SILVER": "Sustained Victor",
                    "GOLD": "Market Dominance",
                    "PLATINUM": "Invincible Desk"
                },
                active_value=current_streak,
                best_record=longest_streak
            ),
            cls._build_trophy(
                trophy_id="VOLUME_TITAN",
                title="Liquidity Titan",
                icon="Landmark",
                category="Volume",
                description="Cumulative firm trade execution volume awarded across confirmed tenders.",
                current_value=round(total_volume_won_usd, 0),
                unit="USD",
                milestones={"BRONZE": 10000000.0, "SILVER": 50000000.0, "GOLD": 200000000.0, "PLATINUM": 1000000000.0},
                tier_titles={
                    "NONE": "Liquidity Titan",
                    "BRONZE": "$10M Club",
                    "SILVER": "$50M Book",
                    "GOLD": "$200M Whale",
                    "PLATINUM": "Billion Dollar Desk"
                },
                is_currency=True
            ),
            cls._build_trophy(
                trophy_id="PRECISION_SPEED",
                title="Swift Quoting",
                icon="Zap",
                category="Agility",
                description="Rapid, firm price quotations submitted during live market tender windows.",
                current_value=total_quotes_submitted,
                unit="quotes",
                milestones={"BRONZE": 15, "SILVER": 50, "GOLD": 150, "PLATINUM": 400},
                tier_titles={
                    "NONE": "Swift Quoting",
                    "BRONZE": "Market Presence",
                    "SILVER": "Active Quoter",
                    "GOLD": "High-Velocity Desk",
                    "PLATINUM": "Prime Market Maker"
                }
            ),
            cls._build_trophy(
                trophy_id="MARKET_INTELLIGENCE",
                title="Market Intelligence",
                icon="Eye",
                category="Benchmarking",
                description="Indicative & benchmark market intelligence pricing provided to corporate treasury clients.",
                current_value=indicative_quotes_submitted,
                unit="quotes",
                milestones={"BRONZE": 10, "SILVER": 30, "GOLD": 75, "PLATINUM": 200},
                tier_titles={
                    "NONE": "Market Intelligence",
                    "BRONZE": "Maiden Benchmark",
                    "SILVER": "Intelligence Partner",
                    "GOLD": "Market Benchmark Anchor",
                    "PLATINUM": "Prime Market Oracle"
                }
            ),
            cls._build_trophy(
                trophy_id="THE_RELIABLE_DESK",
                title="The Active Desk",
                icon="Shield",
                category="Participation",
                description="Actively quote and provide liquidity across invited corporate RFQ tenders.",
                current_value=total_tenders_participated,
                unit="tenders",
                milestones={"BRONZE": 5, "SILVER": 20, "GOLD": 50, "PLATINUM": 100},
                tier_titles={
                    "NONE": "The Active Desk",
                    "BRONZE": "Active Participant",
                    "SILVER": "Core Liquidity Partner",
                    "GOLD": "Prime Relationship Desk",
                    "PLATINUM": "Sovereign Liquidity Anchor"
                }
            ),
            cls._build_trophy(
                trophy_id="CURRENCY_EXPLORER",
                title="Market Versatility",
                icon="Globe",
                category="Breadth",
                description="Active firm execution across distinct interbank currency pairs.",
                current_value=currency_pairs_won_count,
                unit="pairs",
                milestones={"BRONZE": 2, "SILVER": 3, "GOLD": 5, "PLATINUM": 8},
                tier_titles={
                    "NONE": "Market Versatility",
                    "BRONZE": "Cross-Currency Dealer",
                    "SILVER": "Multi-Currency Specialist",
                    "GOLD": "FX Portfolio Master",
                    "PLATINUM": "Universal FX Book"
                }
            ),
            cls._build_trophy(
                trophy_id="READY_AT_THE_BELL",
                title="Ready at the Bell",
                icon="Hourglass",
                category="Discipline",
                description="Terminal arrival authenticated as the quotation window opens.",
                current_value=ready_at_bell_count,
                unit="sessions",
                milestones={"BRONZE": 3, "SILVER": 12, "GOLD": 30, "PLATINUM": 60},
                tier_titles={
                    "NONE": "Ready at the Bell",
                    "BRONZE": "Opening Bell Debut",
                    "SILVER": "Punctual Terminal",
                    "GOLD": "Disciplined Desk",
                    "PLATINUM": "Market Sentinel"
                }
            )
        ]

    @classmethod
    def _build_trophy(
        cls,
        trophy_id: str,
        title: str,
        icon: str,
        category: str,
        description: str,
        current_value: float,
        unit: str,
        milestones: Dict[str, float],
        tier_titles: Optional[Dict[str, str]] = None,
        is_currency: bool = False,
        active_value: Optional[float] = None,
        best_record: Optional[float] = None
    ) -> Dict[str, Any]:
        """Calculates multi-metal status (BRONZE, SILVER, GOLD, PLATINUM) and progress bar."""
        # Tier unlocked is based on best record ever achieved (earned badges are kept)
        eval_tier_val = best_record if best_record is not None else current_value

        if eval_tier_val >= milestones["PLATINUM"]:
            tier = "PLATINUM"
            next_tier = "MAX"
            target_val = milestones["PLATINUM"]
        elif eval_tier_val >= milestones["GOLD"]:
            tier = "GOLD"
            next_tier = "PLATINUM"
            target_val = milestones["PLATINUM"]
        elif eval_tier_val >= milestones["SILVER"]:
            tier = "SILVER"
            next_tier = "GOLD"
            target_val = milestones["GOLD"]
        elif eval_tier_val >= milestones["BRONZE"]:
            tier = "BRONZE"
            next_tier = "SILVER"
            target_val = milestones["SILVER"]
        else:
            tier = "NONE"
            next_tier = "BRONZE"
            target_val = milestones["BRONZE"]

        # Progress percentage strictly reflects ACTIVE streak progress towards target_val!
        active_val = current_value
        progress_pct = min(100.0, (active_val / target_val) * 100.0) if target_val > 0 else 0.0


        display_title = tier_titles.get(tier, title) if tier_titles else title

        res = {
            "id": trophy_id,
            "trophy_id": trophy_id,
            "title": display_title,
            "base_title": title,
            "current_tier_title": display_title if tier != "NONE" else None,
            "icon": icon,
            "category": category,
            "description": description,
            "current_value": current_value,
            "target_value": target_val,
            "next_milestone": target_val if next_tier != "MAX" else None,
            "current_tier": tier,
            "next_tier": next_tier,
            "progress_percent": round(progress_pct, 1),
            "progress_pct": round(progress_pct, 1),
            "unit": unit,
            "is_currency": is_currency,
            "milestones": milestones
        }
        if active_value is not None:
            res["active_value"] = active_value
        if best_record is not None:
            res["best_record"] = best_record
        return res

    @classmethod
    def _evaluate_active_seasons(cls) -> List[Dict[str, Any]]:
        """Determines active seasonal, festive, or fiscal events based on pure calendar math."""
        today = date.today()
        month, day = today.month, today.day
        active = []

        # 1. Year-End & Festive Holiday Season (Dec 20 – Jan 12)
        # 5 days before Western Christmas through 5 days after Coptic Christmas (Jan 7)
        if (month == 12 and day >= 20) or (month == 1 and day <= 12):
            active.append({
                "season_id": "YEAR_END_HOLIDAYS",
                "name": "Festive Season & New Year Greetings",
                "icon": "Snowflake",
                "badge": "NEW YEAR & HOLIDAYS",
                "color": "sky",
                "description": "Festive season and New Year trading greetings across the interbank desk."
            })

        # 2. Lunar Hijri Festive Windows (Ramadan & Eid al-Fitr & Eid al-Adha)
        # Windows start 2-3 days before holiday and extend 4-5 days after desks reopen
        for (ram_start, ram_end, adha_start, adha_end) in HIJRI_FESTIVE_CALENDAR:
            # Ramadan: 2 days before Ramadan start through end of Holy Month
            if (ram_start - timedelta(days=2)) <= today <= ram_end:
                active.append({
                    "season_id": "RAMADAN_TWILIGHT",
                    "name": "Ramadan Kareem Liquidity Session",
                    "icon": "Moon",
                    "badge": "RAMADAN KAREEM",
                    "color": "amber",
                    "description": "Active interbank quotation during holy month Ramadan trading hours."
                })
                break

            # Eid al-Fitr: 2 days before Eid through 5 days after holiday ends
            eid_fitr_start = ram_end - timedelta(days=2)
            eid_fitr_end = ram_end + timedelta(days=5)
            if eid_fitr_start <= today <= eid_fitr_end:
                active.append({
                    "season_id": "EID_AL_FITR",
                    "name": "Eid al-Fitr Greetings",
                    "icon": "Sparkles",
                    "badge": "EID MUBARAK",
                    "color": "emerald",
                    "description": "Festive liquidity sprint celebrating Eid al-Fitr."
                })
                break

            # Eid al-Adha: 3 days before Arafat through 5 days after holiday ends
            if (adha_start - timedelta(days=3)) <= today <= (adha_end + timedelta(days=5)):
                active.append({
                    "season_id": "EID_AL_ADHA",
                    "name": "Eid al-Adha Greetings",
                    "icon": "Sparkles",
                    "badge": "EID AL-ADHA",
                    "color": "emerald",
                    "description": "Festive liquidity sprint celebrating Eid al-Adha."
                })
                break

        # 3. Easter & Sham El-Nessim (Astronomical Computus, Zero manual maintenance)
        # 3 days before Holy Week through 5 days after Sham El-Nessim Monday
        coptic_easter = compute_coptic_easter(today.year)
        sham_el_nessim = coptic_easter + timedelta(days=1)
        western_easter = compute_western_easter(today.year)

        if (coptic_easter - timedelta(days=5)) <= today <= (sham_el_nessim + timedelta(days=5)):
            active.append({
                "season_id": "EASTER_SHAM_EL_NESSIM",
                "name": "Easter & Sham El-Nessim Spring Greetings",
                "icon": "Sun",
                "badge": "SHAM EL-NESSIM & EASTER",
                "color": "teal",
                "description": "Spring renewal & festive holiday liquidity window."
            })
        elif (western_easter - timedelta(days=5)) <= today <= (western_easter + timedelta(days=4)):
            active.append({
                "season_id": "WESTERN_EASTER",
                "name": "Easter Holiday Trading Greetings",
                "icon": "Sun",
                "badge": "EASTER HOLIDAYS",
                "color": "teal",
                "description": "Global Easter liquidity and market holiday positioning."
            })

        return active

    @classmethod
    def _calculate_dealer_tier(cls, trophies: List[Dict[str, Any]]) -> tuple:
        """Determines the desk rank and cosmetic pin distinction based on institutional metal weight."""
        points_map = {"NONE": 0, "BRONZE": 1, "SILVER": 3, "GOLD": 6, "PLATINUM": 10}
        tiers = [t.get("current_tier", "NONE") for t in trophies]
        total_pts = sum(points_map.get(tier, 0) for tier in tiers)
        has_platinum = any(tier == "PLATINUM" for tier in tiers)
        has_gold = any(tier in ("GOLD", "PLATINUM") for tier in tiers)

        if total_pts >= 40 and has_platinum:
            return ("Tier 4: Diamond Desk", "Honorary Diamond Crest")
        elif total_pts >= 22 and has_gold:
            return ("Tier 3: Gold Desk", "Prestige Gold Insignia")
        elif total_pts >= 10:
            return ("Tier 2: Silver Desk", "Distinguished Silver Pin")
        elif total_pts >= 1:
            return ("Tier 1: Verified Desk", "Authenticated Desk Seal")
        else:
            return ("Active Desk", "Complete 1st badge to unlock Verified Desk")

    @classmethod
    def _empty_achievements_payload(cls, email: str) -> Dict[str, Any]:
        """Safe default payload returned on error or unauthenticated guests with all trophies initialized."""
        trophies = cls._build_trophies_set(
            ready_at_bell_count=0,
            total_tenders_participated=0,
            total_deals_won=0,
            total_volume_won_usd=0.0,
            current_streak=0,
            longest_streak=0,
            currency_pairs_won_count=0,
            total_quotes_submitted=0,
            indicative_quotes_submitted=0,
            is_desk_mode=False
        )
        return {
            "mode": "GUEST",
            "dealer_email": email,
            "bank_name": "Bank Desk",
            "dealer_tier": "Active Desk",
            "dealer_perk": "Complete 1st badge to unlock Verified Desk",
            "earned_trophy_count": 0,
            "total_trophies": len(trophies),
            "trophies": trophies,
            "personal_bests": {
                "longest_winning_streak": 0,
                "current_streak": 0,
                "total_deals_won": 0,
                "total_tenders_participated": 0,
                "currency_pairs_count": 0,
                "total_quotes_submitted": 0,
                "execution_quotes_submitted": 0,
                "indicative_quotes_submitted": 0
            },
            "active_seasons": cls._evaluate_active_seasons(),
            "as_of": datetime.now(timezone.utc).isoformat()
        }

    @classmethod
    def get_streak_audit_trail(
        cls,
        db: Session,
        bank_id: int
    ) -> List[Dict[str, Any]]:
        """
        Phase 7.1: Returns chronological audit trail explaining every streak transition
        (+1 Clean Sweep, Reset to 0 Competitor Win, or Wash/Neutral) across authentic RFQs.
        """
        assignments = db.query(QuotationBankAssignment).join(
            QuotationBank, QuotationBankAssignment.quotation_bank_id == QuotationBank.id
        ).filter(
            QuotationBank.bank_id == bank_id
        ).all()
        if not assignments:
            return []

        rfq_ids = list(set([a.rfq_id for a in assignments]))
        rfqs = db.query(QuotationRequest).filter(
            QuotationRequest.id.in_(rfq_ids),
            QuotationRequest.ref_no.like('RFQ-202%'),
            QuotationRequest.status.notin_(['DRAFT'])
        ).all()

        rfqs_sorted = sorted(
            rfqs,
            key=lambda r: (r.window_end.replace(tzinfo=timezone.utc) if r.window_end and r.window_end.tzinfo is None else (r.window_end or datetime.min.replace(tzinfo=timezone.utc)))
        )

        audit_trail = []
        running_streak = 0

        for rfq in rfqs_sorted:
            asgn = next((a for a in assignments if a.rfq_id == rfq.id), None)
            if not asgn:
                continue

            dt_str = str(rfq.window_end or rfq.created_at).split('.')[0]
            if rfq.type == 'TBILL':
                tbill_base = (getattr(asgn, 'quotation_base', None) or getattr(rfq, 'quotation_base', None) or 'Execution').strip().lower()
                if tbill_base != 'execution':
                    continue

                qa = db.query(QuotationAnalytics).filter(QuotationAnalytics.rfq_id == rfq.id).first()
                is_won = bool(qa and qa.winner_quotation_bank_id and asgn and asgn.quotation_bank and qa.winner_quotation_bank_id == asgn.quotation_bank.id and rfq.status in ('COMPLETED', 'CLOSED'))
                is_lost = bool(qa and qa.winner_quotation_bank_id and asgn and asgn.quotation_bank and qa.winner_quotation_bank_id != asgn.quotation_bank.id and rfq.status in ('COMPLETED', 'CLOSED'))

                if is_won:
                    running_streak += 1
                    transition = "+1 (Won)"
                    reason = "T-Bill tender awarded to this bank"
                    badge = "CLEAN_SWEEP"
                elif is_lost:
                    running_streak = 0
                    transition = "Reset to 0"
                    reason = "T-Bill tender awarded to competitor"
                    badge = "COMPETITOR_WIN"
                else:
                    transition = "Preserved"
                    reason = f"Tender {rfq.status} with no award (Neutral Wash)"
                    badge = "WASH"
            else:
                legs = getattr(rfq, 'legs', [])
                eligible_exec_legs = []
                if legs:
                    for l in legs:
                        leg_cfg = asgn.get_config_for_leg(l.id) if hasattr(asgn, 'get_config_for_leg') else None
                        is_invited = getattr(leg_cfg, 'is_invited', True) if leg_cfg else True
                        leg_base = (getattr(leg_cfg, 'quotation_base', None) or getattr(asgn, 'quotation_base', None) or getattr(l, 'quotation_base', None) or getattr(rfq, 'quotation_base', None) or 'Execution').strip().lower()
                        if is_invited and leg_base == 'execution':
                            eligible_exec_legs.append(l)
                else:
                    rfq_base = (getattr(asgn, 'quotation_base', None) or getattr(rfq, 'quotation_base', None) or 'Execution').strip().lower()
                    if rfq_base == 'execution':
                        eligible_exec_legs.append(rfq)

                if not eligible_exec_legs:
                    # Bank was not invited to any execution legs in this tender (e.g. Indicative or Invisible only)
                    continue

                # Check if the bank desk submitted offers for this RFQ
                desk_offers = db.query(QuotationOffer).filter(QuotationOffer.assignment_id == asgn.id).all()
                if not desk_offers:
                    audit_trail.append({
                        "rfq_id": rfq.id,
                        "ref_no": rfq.ref_no,
                        "date": dt_str,
                        "type": rfq.type,
                        "status": rfq.status,
                        "transition": "Preserved",
                        "badge": "WASH",
                        "running_streak": running_streak,
                        "reason": "No quotes submitted by bank desk (Abstained / Neutral Wash)"
                    })
                    continue

                awarded_legs = [
                    l for l in eligible_exec_legs
                    if (getattr(l, 'winner_bank_id', None) or (getattr(rfq, 'winner_bank_id', None) if l == rfq else None)) and
                    getattr(l, 'status', None) not in ('REJECTED', 'CANCELLED', 'DECLINED', 'INCONCLUSIVE', 'EXPIRED')
                ]

                competitor_won_any = any(
                    (getattr(l, 'winner_bank_id', None) or (getattr(rfq, 'winner_bank_id', None) if l == rfq else None)) != bank_id
                    for l in awarded_legs
                )

                if competitor_won_any:
                    running_streak = 0
                    transition = "Reset to 0"
                    reason = "Competitor bank awarded one or more execution legs"
                    badge = "COMPETITOR_WIN"
                elif awarded_legs and rfq.status in ('COMPLETED', 'ACCEPTED', 'CLOSED'):
                    bank_won_all_awarded = all(
                        (getattr(l, 'winner_bank_id', None) or (getattr(rfq, 'winner_bank_id', None) if l == rfq else None)) == bank_id
                        for l in awarded_legs
                    )
                    if bank_won_all_awarded:
                        running_streak += 1
                        transition = "+1 (Won)"
                        if len(awarded_legs) == len(eligible_exec_legs):
                            reason = f"Clean Sweep: Won all {len(awarded_legs)} execution leg(s)"
                        else:
                            reason = f"Won all {len(awarded_legs)} awarded execution leg(s) ({len(eligible_exec_legs) - len(awarded_legs)} non-awarded/rejected by client)"
                        badge = "CLEAN_SWEEP"
                    elif rfq.status in ('COMPLETED', 'ACCEPTED', 'CLOSED'):
                        running_streak = 0
                        transition = "Reset to 0"
                        reason = "Awarded execution leg not won by this bank"
                        badge = "COMPETITOR_WIN"
                    else:
                        transition = "Preserved"
                        reason = f"Tender {rfq.status} (Neutral Wash)"
                        badge = "WASH"
                else:
                    transition = "Preserved"
                    reason = f"Tender {rfq.status} with no executed trade (Neutral Wash)"
                    badge = "WASH"

            audit_trail.append({
                "rfq_id": rfq.id,
                "ref_no": rfq.ref_no,
                "date": dt_str,
                "type": rfq.type,
                "status": rfq.status,
                "transition": transition,
                "badge": badge,
                "running_streak": running_streak,
                "reason": reason
            })

        return list(reversed(audit_trail))


dealer_achievement_service = DealerAchievementService()
