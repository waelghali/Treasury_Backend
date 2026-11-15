import re
from typing import Optional, Dict, Any, List
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_, desc
from datetime import datetime

from app.models.models_reconciliation_v2 import CollaborativePattern, CollaborativeTenantVote, Counterparty
from app.services.reconciliation_confidentiality_service import (
    reconciliation_confidentiality,
    classify_amount_tier,
)
import logging

logger = logging.getLogger("app.collaborative_learning")

# Consensus Promotion Thresholds
K_THRESHOLD_SAME_BANK = 2     # 2 distinct companies with the same bank
K_THRESHOLD_UNIVERSAL = 3     # 3 distinct companies across different banks
MIN_AGREEMENT_PERCENT = 80.0  # At least 80% consensus among voting companies

# Common financial noise words to strip during signature sanitization
STOP_WORDS = {
    "TRANSFER", "TRF", "PAYMENT", "PAYMT", "PMT", "DEBIT", "CREDIT", "DR", "CR",
    "REF", "REFERENCE", "NO", "NUM", "NUMBER", "DATE", "VAL", "VALUE",
    "FROM", "TO", "FOR", "VIA", "THRU", "BY", "AT", "ON", "AND", "THE",
    "EGP", "USD", "EUR", "GBP", "SAR", "AED", "KWD", "QAR",
    "تحويل", "سداد", "دفع", "رقم", "تاريخ", "من", "إلى", "عن", "عبر"
}


class CollaborativeLearningService:
    """
    Dynamic, privacy-preserving collaborative intelligence engine.
    Extracts zero-PII structural signatures from user confirmations across companies,
    qualifies patterns by amount scale tier and semantic flags, tallies distinct-tenant
    consensus, and autonomously promotes high-confidence patterns to benefit all users.
    """

    def sanitize_signature(
        self,
        raw_description: str,
        description_line2: Optional[str] = None,
        known_entities: Optional[List[str]] = None,
        counterparty_name: Optional[str] = None,
    ) -> Optional[str]:
        """
        The Zero-Knowledge Privacy Sanitizer Shield:
        Uses Counterparty-Aware PII Scrubbing to strip all customer/vendor names,
        legal suffixes, preposition-following beneficiaries, IBANs, account numbers,
        dates, reference codes, and amounts.
        Returns only verified structural banking vocabulary tokens.
        """
        if not raw_description:
            return None

        extracted = reconciliation_confidentiality.extract_privacy_safe_rule_signature(
            raw_description=raw_description,
            description_line2=description_line2,
            known_entities=known_entities,
            counterparty_name=counterparty_name,
        )
        # Prefer strict banking structural signature for cross-tenant sharing;
        # fall back to tenant_signature only if all tokens are non-entity banking words
        sig = extracted.get("collaborative_signature") or extracted.get("tenant_signature")
        if not sig or len(sig) < 4:
            return None
        return sig

    def submit_confirmation(
        self,
        db: Session,
        company_id: int,
        bank_id: Optional[int],
        raw_description: str,
        direction: str,  # "CREDIT" or "DEBIT"
        category: str,
        gl_account: Optional[str] = None,
        amount: Any = None,
        counterparty_name: Optional[str] = None,
        previous_category: Optional[str] = None,
    ) -> Optional[CollaborativePattern]:
        """
        Federated Learning Ingestion:
        Called whenever any company confirms or classifies a transaction.
        Processes the raw narrative through the Counterparty-Aware Privacy Sanitizer,
        qualifies by amount_tier, and records the consensus vote.
        """
        # Load tenant's known counterparty names so none of them can ever leak into collaborative signatures
        known_entities = []
        try:
            cps = db.query(Counterparty.name).filter(
                Counterparty.company_id == company_id,
                Counterparty.is_deleted == False
            ).limit(500).all()
            known_entities = [c[0] for c in cps if c and c[0]]
        except Exception:
            pass

        signature = self.sanitize_signature(
            raw_description=raw_description,
            known_entities=known_entities,
            counterparty_name=counterparty_name,
        )
        if not signature or not category:
            return None

        clean_direction = direction.upper() if direction in ["CREDIT", "DEBIT"] else "DEBIT"
        clean_category = category.strip()
        tier = classify_amount_tier(amount) if amount is not None else "ANY"

        semantic_flags = reconciliation_confidentiality.detect_semantic_flags(
            raw_description=raw_description,
            direction=clean_direction,
            amount=amount,
        )

        # Find existing pattern matching (signature, direction, bank_id, amount_tier)
        pattern = db.query(CollaborativePattern).filter(
            CollaborativePattern.signature_token == signature,
            CollaborativePattern.direction == clean_direction,
            CollaborativePattern.bank_id == bank_id,
            or_(
                CollaborativePattern.amount_tier == tier,
                CollaborativePattern.amount_tier == "ANY",
                CollaborativePattern.amount_tier == None,
            )
        ).first()

        if not pattern:
            pattern = CollaborativePattern(
                bank_id=bank_id,
                signature_token=signature,
                direction=clean_direction,
                amount_tier=tier,
                semantic_flags_json={
                    "is_reversal": semantic_flags.get("is_reversal", False),
                    "is_negative_collection": semantic_flags.get("is_negative_collection", False),
                },
                suggested_category=clean_category,
                suggested_gl_account=gl_account,
                distinct_tenants_count=1,
                total_confirmations=1,
                rejection_count=0,
                agreement_score=Decimal("100.00"),
                confidence_score=62,
                is_promoted=False
            )
            db.add(pattern)
            db.flush()

            vote = CollaborativeTenantVote(
                pattern_id=pattern.id,
                company_id=company_id,
                category_voted=clean_category
            )
            db.add(vote)
        else:
            pattern.total_confirmations = (pattern.total_confirmations or 0) + 1
            pattern.last_confirmed_at = func.now()
            if not pattern.amount_tier or pattern.amount_tier == "ANY":
                pattern.amount_tier = tier

            # If user overrode a previous category predicted by this pattern, record rejection penalty
            if previous_category and previous_category != clean_category and pattern.suggested_category == previous_category:
                pattern.rejection_count = (pattern.rejection_count or 0) + 1

            existing_vote = db.query(CollaborativeTenantVote).filter(
                CollaborativeTenantVote.pattern_id == pattern.id,
                CollaborativeTenantVote.company_id == company_id
            ).first()

            if not existing_vote:
                new_vote = CollaborativeTenantVote(
                    pattern_id=pattern.id,
                    company_id=company_id,
                    category_voted=clean_category
                )
                db.add(new_vote)
                db.flush()

                distinct_count = db.query(func.count(func.distinct(CollaborativeTenantVote.company_id))).filter(
                    CollaborativeTenantVote.pattern_id == pattern.id
                ).scalar() or 1
                pattern.distinct_tenants_count = distinct_count
            else:
                if existing_vote.category_voted != clean_category:
                    pattern.rejection_count = (pattern.rejection_count or 0) + 1
                existing_vote.category_voted = clean_category
                existing_vote.last_voted_at = func.now()

            total_votes = db.query(CollaborativeTenantVote).filter(
                CollaborativeTenantVote.pattern_id == pattern.id
            ).all()

            if total_votes:
                tally = {}
                for v in total_votes:
                    tally[v.category_voted] = tally.get(v.category_voted, 0) + 1
                
                top_category, top_votes = max(tally.items(), key=lambda x: x[1])
                rejections = int(pattern.rejection_count or 0)
                effective_denominator = max(1, len(total_votes) + rejections)
                agreement = (top_votes / effective_denominator) * 100
                pattern.agreement_score = Decimal(str(round(agreement, 2)))
                pattern.suggested_category = top_category

                raw_confidence = 62 + (pattern.distinct_tenants_count - 1) * 12
                scaled_confidence = int(min(97, raw_confidence * (agreement / 100)))
                pattern.confidence_score = max(40, scaled_confidence)

                k_threshold = K_THRESHOLD_SAME_BANK if pattern.bank_id else K_THRESHOLD_UNIVERSAL
                if pattern.distinct_tenants_count >= k_threshold and agreement >= MIN_AGREEMENT_PERCENT:
                    pattern.is_promoted = True
                elif agreement < MIN_AGREEMENT_PERCENT:
                    pattern.is_promoted = False

        db.commit()
        db.refresh(pattern)
        return pattern

    def match_collaborative(
        self,
        db: Session,
        bank_id: Optional[int],
        raw_description: str,
        direction: str,  # "CREDIT" or "DEBIT"
        description_line2: Optional[str] = None,
        amount: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Evaluates a transaction narrative against promoted collaborative patterns,
        enforcing direction and amount-scale compatibility (e.g. preventing a MICRO
        fee signature from misclassifying a LARGE vendor payment).
        """
        combined = f"{raw_description or ''} {description_line2 or ''}".upper()
        clean_dir = direction.upper() if direction in ["CREDIT", "DEBIT"] else "DEBIT"
        tx_tier = classify_amount_tier(amount) if amount is not None else "ANY"

        patterns = db.query(CollaborativePattern).filter(
            CollaborativePattern.is_promoted == True,
            or_(
                CollaborativePattern.direction == clean_dir,
                CollaborativePattern.direction == "EITHER"
            ),
            or_(
                CollaborativePattern.bank_id == bank_id,
                CollaborativePattern.bank_id == None
            )
        ).order_by(
            CollaborativePattern.bank_id.desc().nullslast(),
            CollaborativePattern.confidence_score.desc()
        ).all()

        if not patterns:
            return None

        for p in patterns:
            # Amount-scale guardrail: BANK_CHARGES pattern learned on MICRO/SMALL cannot fire on LARGE/MACRO
            p_tier = getattr(p, "amount_tier", "ANY") or "ANY"
            if p.suggested_category == "BANK_CHARGES" and tx_tier in ("LARGE", "MACRO"):
                continue
            if p_tier not in ("ANY", tx_tier):
                # Allow adjacent tiers (e.g., MICRO <-> SMALL or MEDIUM <-> LARGE)
                tier_order = ["MICRO", "SMALL", "MEDIUM", "LARGE", "MACRO"]
                if p_tier in tier_order and tx_tier in tier_order:
                    if abs(tier_order.index(p_tier) - tier_order.index(tx_tier)) > 1:
                        continue

            sig_tokens = p.signature_token.split()
            is_match = (p.signature_token in combined) or (len(sig_tokens) > 1 and all(tok in combined for tok in sig_tokens))
            if is_match:
                return {
                    "category": p.suggested_category,
                    "gl_account": p.suggested_gl_account,
                    "confidence": p.confidence_score,
                    "signature": p.signature_token,
                    "distinct_tenants": p.distinct_tenants_count,
                    "is_bank_specific": p.bank_id is not None,
                    "amount_tier": p_tier,
                }

        return None

    def get_platform_stats(self, db: Session) -> Dict[str, Any]:
        """
        Returns anonymized aggregate platform intelligence metrics.
        Zero customer details or transaction data exposed.
        """
        total_patterns = db.query(CollaborativePattern).count()
        promoted_patterns = db.query(CollaborativePattern).filter(CollaborativePattern.is_promoted == True).count()
        bank_specific_count = db.query(CollaborativePattern).filter(
            CollaborativePattern.is_promoted == True,
            CollaborativePattern.bank_id != None
        ).count()
        universal_count = promoted_patterns - bank_specific_count

        avg_confidence = db.query(func.avg(CollaborativePattern.confidence_score)).filter(
            CollaborativePattern.is_promoted == True
        ).scalar() or 0

        recent_promoted = db.query(CollaborativePattern).filter(
            CollaborativePattern.is_promoted == True
        ).order_by(CollaborativePattern.last_confirmed_at.desc()).limit(10).all()

        samples = [{
            "signature": p.signature_token,
            "category": p.suggested_category,
            "confidence": p.confidence_score,
            "distinct_tenants": p.distinct_tenants_count,
            "amount_tier": getattr(p, "amount_tier", "ANY") or "ANY",
            "scope": "Bank Specific" if p.bank_id else "Universal"
        } for p in recent_promoted]

        return {
            "total_patterns": total_patterns,
            "promoted_patterns": promoted_patterns,
            "bank_specific_count": bank_specific_count,
            "universal_count": universal_count,
            "average_confidence": round(float(avg_confidence), 1),
            "recent_promoted": samples
        }

    def decay_stale_patterns(
        self,
        db: Session,
        inactive_days: int = 180,
        decay_factor: float = 0.85,
        min_confidence: int = 50
    ) -> Dict[str, Any]:
        """
        Dynamically decays confidence of patterns that haven't been re-confirmed.
        """
        from datetime import datetime, timedelta, timezone
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=inactive_days)

        stale_patterns = db.query(CollaborativePattern).filter(
            CollaborativePattern.last_confirmed_at < cutoff_date
        ).all()

        decayed_count = 0
        demoted_count = 0

        for p in stale_patterns:
            old_conf = p.confidence_score or 50
            new_conf = max(30, int(old_conf * decay_factor))
            if new_conf != old_conf:
                p.confidence_score = new_conf
                decayed_count += 1

            if p.is_promoted and (new_conf < min_confidence or (p.agreement_score or 0) < MIN_AGREEMENT_PERCENT):
                p.is_promoted = False
                demoted_count += 1

        if decayed_count > 0 or demoted_count > 0:
            db.commit()
            
        return {
            "evaluated_stale_patterns": len(stale_patterns),
            "decayed_count": decayed_count,
            "demoted_count": demoted_count,
            "status": f"Decayed {decayed_count} patterns, demoted {demoted_count} stale signatures."
        }


collaborative_service = CollaborativeLearningService()
