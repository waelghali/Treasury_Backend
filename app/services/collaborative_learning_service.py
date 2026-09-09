import re
from typing import Optional, Dict, Any, List
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_, desc
from datetime import datetime

from app.models.models_reconciliation_v2 import CollaborativePattern, CollaborativeTenantVote
import logging

logger = logging.getLogger("app.collaborative_learning")

# Consensus Promotion Thresholds
K_THRESHOLD_SAME_BANK = 2     # 2 distinct companies with the same bank
K_THRESHOLD_UNIVERSAL = 3     # 3 distinct companies across different banks
MIN_AGREEMENT_PERCENT = 75.0  # At least 75% consensus among voting companies

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
    Extracts anonymized structural signatures from user confirmations across companies,
    tallies distinct-tenant consensus, and autonomously promotes high-confidence
    patterns to benefit all platform users.
    """

    def sanitize_signature(self, raw_description: str, description_line2: Optional[str] = None) -> Optional[str]:
        """
        The Privacy Sanitizer Shield:
        Strips all PII, IBANs, account numbers, dates, reference codes, amounts, and noise.
        Extracts only the stable canonical semantic anchor / n-gram.
        """
        if not raw_description:
            return None

        combined = f"{raw_description} {description_line2 or ''}".upper()

        # 1. Strip IBANs (International Bank Account Numbers)
        combined = re.sub(r'\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b', ' ', combined)

        # 2. Strip Account / Card number sequences (10+ digits or 4-digit groups)
        combined = re.sub(r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b', ' ', combined)
        combined = re.sub(r'\b\d{8,30}\b', ' ', combined)

        # 3. Strip Dates (various formats: YYYY-MM-DD, DD/MM/YYYY, etc.)
        combined = re.sub(r'\b\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}\b', ' ', combined)

        # 4. Strip Currencies and Amounts (e.g. EGP 15,000.00, $500, 1500.50 USD)
        combined = re.sub(r'[$€£¥]', ' ', combined)
        combined = re.sub(r'\b(EGP|USD|EUR|GBP|SAR|AED|KWD|QAR)\s*[\d,.]+', ' ', combined)
        combined = re.sub(r'[\d,.]+\s*(EGP|USD|EUR|GBP|SAR|AED|KWD|QAR)\b', ' ', combined)
        combined = re.sub(r'\b\d+([.,]\d+)?\b', ' ', combined)

        # 5. Strip Reference / Batch / Invoice alphanumeric codes
        combined = re.sub(r'\b(INV|INVOICE|REF|BATCH|TRF|TXN|TRX|TRACE|SEQ|CHQ|CHECK)[#:\-\s]*[A-Z0-9]+\b', ' ', combined)

        # 6. Normalize punctuation and spaces
        combined = re.sub(r'[^a-zA-Z\u0600-\u06FF\s]', ' ', combined)
        words = [w.strip() for w in combined.split() if len(w.strip()) >= 3]

        # 7. Remove solitary generic stop words while keeping composite phrases
        meaningful_words = [w for w in words if w not in STOP_WORDS]
        if not meaningful_words:
            return None

        # Build candidate signature (up to 4 key tokens)
        signature = " ".join(meaningful_words[:4]).strip()
        
        # Minimum length check to avoid noise
        if len(signature) < 4:
            return None

        return signature

    def submit_confirmation(
        self,
        db: Session,
        company_id: int,
        bank_id: Optional[int],
        raw_description: str,
        direction: str,  # "CREDIT" or "DEBIT"
        category: str,
        gl_account: Optional[str] = None
    ) -> Optional[CollaborativePattern]:
        """
        Federated Learning Ingestion:
        Called whenever any company confirms or classifies a transaction.
        Processes the raw narrative through the Privacy Sanitizer and records the consensus vote.
        """
        signature = self.sanitize_signature(raw_description)
        if not signature or not category:
            return None

        clean_direction = direction.upper() if direction in ["CREDIT", "DEBIT"] else "DEBIT"
        clean_category = category.strip()

        # Find existing pattern (first bank-scoped, then universal)
        pattern = db.query(CollaborativePattern).filter(
            CollaborativePattern.signature_token == signature,
            CollaborativePattern.direction == clean_direction,
            CollaborativePattern.bank_id == bank_id
        ).first()

        if not pattern:
            # Create candidate pattern
            pattern = CollaborativePattern(
                bank_id=bank_id,
                signature_token=signature,
                direction=clean_direction,
                suggested_category=clean_category,
                suggested_gl_account=gl_account,
                distinct_tenants_count=1,
                total_confirmations=1,
                agreement_score=Decimal("100.00"),
                confidence_score=60,
                is_promoted=False
            )
            db.add(pattern)
            db.flush()

            # Record initial vote
            vote = CollaborativeTenantVote(
                pattern_id=pattern.id,
                company_id=company_id,
                category_voted=clean_category
            )
            db.add(vote)
        else:
            # Existing pattern: update confirmation count
            pattern.total_confirmations = (pattern.total_confirmations or 0) + 1
            pattern.last_confirmed_at = func.now()

            # Check if this company has voted before
            existing_vote = db.query(CollaborativeTenantVote).filter(
                CollaborativeTenantVote.pattern_id == pattern.id,
                CollaborativeTenantVote.company_id == company_id
            ).first()

            if not existing_vote:
                # New distinct tenant voting!
                new_vote = CollaborativeTenantVote(
                    pattern_id=pattern.id,
                    company_id=company_id,
                    category_voted=clean_category
                )
                db.add(new_vote)
                db.flush()

                # Re-calculate distinct tenant count
                distinct_count = db.query(func.count(func.distinct(CollaborativeTenantVote.company_id))).filter(
                    CollaborativeTenantVote.pattern_id == pattern.id
                ).scalar() or 1
                pattern.distinct_tenants_count = distinct_count
            else:
                existing_vote.category_voted = clean_category
                existing_vote.last_voted_at = func.now()

            # Re-calculate consensus agreement ratio
            total_votes = db.query(CollaborativeTenantVote).filter(
                CollaborativeTenantVote.pattern_id == pattern.id
            ).all()

            if total_votes:
                # Tally votes per category
                tally = {}
                for v in total_votes:
                    tally[v.category_voted] = tally.get(v.category_voted, 0) + 1
                
                # Winning category
                top_category, top_votes = max(tally.items(), key=lambda x: x[1])
                agreement = (top_votes / len(total_votes)) * 100
                pattern.agreement_score = Decimal(str(round(agreement, 2)))
                pattern.suggested_category = top_category

                # Update confidence score based on distinct companies and agreement
                # Base 60 + 12 per distinct tenant, scaled by agreement
                raw_confidence = 60 + (pattern.distinct_tenants_count - 1) * 12
                scaled_confidence = int(min(98, raw_confidence * (agreement / 100)))
                pattern.confidence_score = max(50, scaled_confidence)

                # Promotion Evaluation
                k_threshold = K_THRESHOLD_SAME_BANK if pattern.bank_id else K_THRESHOLD_UNIVERSAL
                if pattern.distinct_tenants_count >= k_threshold and agreement >= MIN_AGREEMENT_PERCENT:
                    pattern.is_promoted = True

        db.commit()
        db.refresh(pattern)
        return pattern

    def match_collaborative(
        self,
        db: Session,
        bank_id: Optional[int],
        raw_description: str,
        direction: str,  # "CREDIT" or "DEBIT"
        description_line2: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Evaluates a transaction narrative against promoted collaborative patterns.
        Returns prediction if high-confidence collaborative consensus exists.
        """
        combined = f"{raw_description or ''} {description_line2 or ''}".upper()
        clean_dir = direction.upper() if direction in ["CREDIT", "DEBIT"] else "DEBIT"

        # Fetch active promoted patterns matching this bank OR universal (bank_id is None)
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
            # Specific bank matches take precedence over universal matches
            CollaborativePattern.bank_id.desc().nullslast(),
            CollaborativePattern.confidence_score.desc()
        ).all()

        if not patterns:
            return None

        for p in patterns:
            sig_tokens = p.signature_token.split()
            is_match = (p.signature_token in combined) or (len(sig_tokens) > 1 and all(tok in combined for tok in sig_tokens))
            if is_match:
                return {
                    "category": p.suggested_category,
                    "gl_account": p.suggested_gl_account,
                    "confidence": p.confidence_score,
                    "signature": p.signature_token,
                    "distinct_tenants": p.distinct_tenants_count,
                    "is_bank_specific": p.bank_id is not None
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

        # Sample of recently promoted patterns (purely signature and category)
        recent_promoted = db.query(CollaborativePattern).filter(
            CollaborativePattern.is_promoted == True
        ).order_by(CollaborativePattern.last_confirmed_at.desc()).limit(10).all()

        samples = [{
            "signature": p.signature_token,
            "category": p.suggested_category,
            "confidence": p.confidence_score,
            "distinct_tenants": p.distinct_tenants_count,
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
        Ensures that if banks change their narration syntax or obsolete formats fade out,
        the knowledge base self-prunes and demotes outdated patterns without manual intervention.
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

            # Demote if confidence falls below threshold or if agreement is insufficient
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
