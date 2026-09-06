# app/crud/crud_campaign.py
from datetime import date, datetime
from typing import Optional, List, Dict, Any, Tuple
from decimal import Decimal
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, and_, or_, desc

from app.models.models_campaign import Campaign, CashbackClaim
from app.models.models import Bank, Customer, LGRecord, User
from app.models.models_issuance import IssuedLGRecord
from app.schemas.schemas_campaign import CampaignCreate, CampaignUpdate, CashbackClaimStatusUpdate

class CRUDCampaign:
    def get_campaign(self, db: Session, campaign_id: int) -> Optional[Campaign]:
        return db.query(Campaign).filter(Campaign.id == campaign_id, Campaign.is_deleted == False).first()

    def list_campaigns(self, db: Session, skip: int = 0, limit: int = 100) -> List[Campaign]:
        return db.query(Campaign).options(joinedload(Campaign.bank)).filter(
            Campaign.is_deleted == False
        ).order_by(desc(Campaign.created_at)).offset(skip).limit(limit).all()

    def create_campaign(self, db: Session, obj_in: CampaignCreate) -> Campaign:
        db_obj = Campaign(
            name=obj_in.name,
            description=obj_in.description,
            bank_id=obj_in.bank_id,
            cashback_percentage=obj_in.cashback_percentage,
            max_cashback_per_lg=obj_in.max_cashback_per_lg,
            max_lgs_per_customer=obj_in.max_lgs_per_customer,
            start_date=obj_in.start_date,
            end_date=obj_in.end_date,
            is_active=obj_in.is_active
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def update_campaign(self, db: Session, campaign_id: int, obj_in: CampaignUpdate) -> Optional[Campaign]:
        db_obj = self.get_campaign(db, campaign_id)
        if not db_obj:
            return None
        update_data = obj_in.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_obj, field, value)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def get_active_campaign_for_lg(self, db: Session, bank_id: Optional[int], customer_id: int) -> Optional[Campaign]:
        """
        Determines if an LG qualifies for a campaign:
        1. Checks bank-specific campaigns first, then all-bank campaigns.
        2. Validates current date window and is_active flag.
        3. Enforces customer's max_lgs_per_customer quota.
        """
        today = date.today()
        # Look for active campaigns matching bank or all-banks (bank_id is None)
        query = db.query(Campaign).filter(
            Campaign.is_active == True,
            Campaign.is_deleted == False,
            Campaign.start_date <= today,
            Campaign.end_date >= today,
            or_(Campaign.bank_id == bank_id, Campaign.bank_id.is_(None))
        )
        # Prioritize specific bank campaign over general campaign
        campaigns = query.order_by(Campaign.bank_id.desc().nullslast()).all()
        for camp in campaigns:
            used_claims = db.query(CashbackClaim).filter(
                CashbackClaim.campaign_id == camp.id,
                CashbackClaim.customer_id == customer_id,
                CashbackClaim.is_deleted == False,
                CashbackClaim.status != "REJECTED"
            ).count()
            if used_claims < camp.max_lgs_per_customer:
                return camp
        return None

    def get_active_bank_promotions(self, db: Session, customer_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Returns active campaigns with bank information for display in the Issuance Wizard.
        """
        today = date.today()
        active_campaigns = db.query(Campaign).options(joinedload(Campaign.bank)).filter(
            Campaign.is_active == True,
            Campaign.is_deleted == False,
            Campaign.start_date <= today,
            Campaign.end_date >= today
        ).all()

        promotions = []
        for camp in active_campaigns:
            claims_used = 0
            if customer_id:
                claims_used = db.query(CashbackClaim).filter(
                    CashbackClaim.campaign_id == camp.id,
                    CashbackClaim.customer_id == customer_id,
                    CashbackClaim.is_deleted == False,
                    CashbackClaim.status != "REJECTED"
                ).count()
            promotions.append({
                "bank_id": camp.bank_id,
                "bank_name": camp.bank.name if camp.bank else "All Banks",
                "campaign_name": camp.name,
                "cashback_rate_pct": float(camp.cashback_percentage * 100),
                "max_per_lg": float(camp.max_cashback_per_lg),
                "max_lgs_per_customer": camp.max_lgs_per_customer,
                "claims_used": claims_used
            })
        return promotions

    def record_claim_for_issued_lg(self, db: Session, issued_lg: IssuedLGRecord) -> Optional[CashbackClaim]:
        """
        Evaluates and saves a cashback claim when an IssuedLGRecord is issued or verified.
        """
        if not issued_lg:
            return None

        # Check if a claim already exists for this issued LG
        existing = db.query(CashbackClaim).filter(
            CashbackClaim.issued_lg_record_id == issued_lg.id,
            CashbackClaim.is_deleted == False
        ).first()
        if existing:
            # Only promote claim to SCAN_VERIFIED if backed by a verified document scan
            if issued_lg.verification_status in ("MATCHED", "ACCEPTED") and existing.status == "CALCULATED":
                if getattr(issued_lg, 'verification_source', None) == "SCAN_VERIFIED":
                    existing.status = "SCAN_VERIFIED"
                    existing.verified_at = issued_lg.verified_at or datetime.utcnow()
                    existing.verification_source = "SCAN_VERIFIED"
                    existing.notes = (existing.notes or "") + f" | Scan copy verified: {issued_lg.verification_status}"
                else:
                    existing.verification_source = getattr(issued_lg, 'verification_source', None) or "MANUAL_ENTRY"
                    existing.notes = (existing.notes or "") + f" | Confirmed via manual entry ({issued_lg.verification_status}), pending bank scan copy verification"
                db.flush()
            return existing

        bank_id = getattr(issued_lg, 'bank_id', None)
        customer_id = getattr(issued_lg, 'customer_id', None)
        campaign = self.get_active_campaign_for_lg(db, bank_id=bank_id, customer_id=customer_id)
        if not campaign:
            return None

        amount = Decimal(str(issued_lg.current_amount or 0))
        rate = Decimal(str(campaign.cashback_percentage))
        cap = Decimal(str(campaign.max_cashback_per_lg))
        calculated = min(amount * rate, cap)

        is_scan_verified = (
            issued_lg.verification_status in ("MATCHED", "ACCEPTED")
            and getattr(issued_lg, "verification_source", None) == "SCAN_VERIFIED"
        )
        status = "SCAN_VERIFIED" if is_scan_verified else "CALCULATED"
        source = "SCAN_VERIFIED" if is_scan_verified else (getattr(issued_lg, "verification_source", None) or "MANUAL_ENTRY")

        claim = CashbackClaim(
            campaign_id=campaign.id,
            customer_id=customer_id,
            issued_lg_record_id=issued_lg.id,
            lg_amount=amount,
            cashback_amount=round(calculated, 2),
            currency_symbol="EGP",
            status=status,
            verified_at=issued_lg.verified_at if is_scan_verified else None,
            verification_source=source,
            notes=f"Auto-calculated for issued LG {issued_lg.bank_lg_number or issued_lg.lg_ref_number} under campaign '{campaign.name}'" + (
                "" if is_scan_verified else " (Pending scan verification)"
            )
        )
        db.add(claim)
        db.flush()
        return claim

    def manual_verify_claim(self, db: Session, claim_id: int, user_id: int, notes: Optional[str] = None) -> Optional[CashbackClaim]:
        """
        Allows System Owner to manually verify a claim with audit notes.
        """
        claim = db.query(CashbackClaim).filter(CashbackClaim.id == claim_id, CashbackClaim.is_deleted == False).first()
        if not claim:
            return None
        claim.status = "VERIFIED"
        claim.verified_at = datetime.utcnow()
        claim.verified_by_user_id = user_id
        claim.verification_source = "MANUAL_AUDIT"
        audit_note = f"Manually verified by auditor ID #{user_id} on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}."
        if notes:
            audit_note += f" Reason: {notes}"
        claim.notes = f"{claim.notes} | {audit_note}" if claim.notes else audit_note
        db.commit()
        db.refresh(claim)
        return claim

    def reconcile_claims_for_lgs(self, db: Session, lg_ids: List[int]) -> int:
        """
        Upgrades claims from CALCULATED / SCAN_VERIFIED to RECONCILED when bank reconciliation verifies the LGs.
        """
        if not lg_ids:
            return 0
        claims = db.query(CashbackClaim).filter(
            or_(
                CashbackClaim.issued_lg_record_id.in_(lg_ids),
                CashbackClaim.lg_record_id.in_(lg_ids)
            ),
            CashbackClaim.status.in_(["CALCULATED", "SCAN_VERIFIED"]),
            CashbackClaim.is_deleted == False
        ).all()
        now = datetime.utcnow()
        count = 0
        for c in claims:
            c.status = "RECONCILED"
            c.verified_at = now
            c.verification_source = "AUTO_RECONCILIATION"
            c.notes = (c.notes or "") + " | Verified via Bank Position Reconciliation."
            count += 1
        if count > 0:
            db.flush()
        return count

    def list_claims(
        self,
        db: Session,
        customer_id: Optional[int] = None,
        campaign_id: Optional[int] = None,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 200
    ) -> List[CashbackClaim]:
        query = db.query(CashbackClaim).options(
            joinedload(CashbackClaim.campaign),
            joinedload(CashbackClaim.customer),
            joinedload(CashbackClaim.issued_lg_record).joinedload(IssuedLGRecord.bank),
            joinedload(CashbackClaim.lg_record).joinedload(LGRecord.issuing_bank),
            joinedload(CashbackClaim.verified_by_user)
        ).filter(CashbackClaim.is_deleted == False)

        if customer_id:
            query = query.filter(CashbackClaim.customer_id == customer_id)
        if campaign_id:
            query = query.filter(CashbackClaim.campaign_id == campaign_id)
        if status:
            query = query.filter(CashbackClaim.status == status)

        return query.order_by(desc(CashbackClaim.created_at)).offset(skip).limit(limit).all()

    def update_claim_status(self, db: Session, claim_id: int, obj_in: CashbackClaimStatusUpdate) -> Optional[CashbackClaim]:
        claim = db.query(CashbackClaim).filter(CashbackClaim.id == claim_id, CashbackClaim.is_deleted == False).first()
        if not claim:
            return None
        claim.status = obj_in.status
        if obj_in.status == "PAID":
            claim.paid_at = datetime.utcnow()
        if obj_in.notes:
            claim.notes = (claim.notes or "") + f" | {obj_in.notes}"
        db.commit()
        db.refresh(claim)
        return claim

    def get_system_stats(self, db: Session) -> Dict[str, Any]:
        total_active_campaigns = db.query(Campaign).filter(
            Campaign.is_active == True,
            Campaign.is_deleted == False,
            Campaign.end_date >= date.today()
        ).count()

        claims = db.query(CashbackClaim).filter(CashbackClaim.is_deleted == False).all()
        total_claims = len(claims)
        total_accrued = sum(c.cashback_amount for c in claims if c.status != "REJECTED")
        total_paid = sum(c.cashback_amount for c in claims if c.status == "PAID")

        return {
            "total_active_campaigns": total_active_campaigns,
            "total_claims": total_claims,
            "total_accrued_cashback": total_accrued,
            "total_paid_cashback": total_paid
        }

    def get_customer_summary(self, db: Session, customer_id: int) -> Dict[str, Any]:
        # Find any active campaign available to this customer
        today = date.today()
        active_camp = db.query(Campaign).options(joinedload(Campaign.bank)).filter(
            Campaign.is_active == True,
            Campaign.is_deleted == False,
            Campaign.start_date <= today,
            Campaign.end_date >= today
        ).first()

        claims = self.list_claims(db, customer_id=customer_id, limit=200)
        valid_claims = [c for c in claims if c.status != "REJECTED"]
        total_earned = sum(c.cashback_amount for c in valid_claims)
        total_paid = sum(c.cashback_amount for c in valid_claims if c.status == "PAID")

        claims_limit = active_camp.max_lgs_per_customer if active_camp else 10

        return {
            "active_campaign": active_camp,
            "claims_used": len(valid_claims),
            "claims_limit": claims_limit,
            "total_earned": total_earned,
            "total_paid": total_paid,
            "claims": claims
        }

crud_campaign = CRUDCampaign()
