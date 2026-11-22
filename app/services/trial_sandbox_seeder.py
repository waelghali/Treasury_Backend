# app/services/trial_sandbox_seeder.py
"""
Idempotent 'Zero-Blank-Screen' Trial Sandbox Seeder (`TRIAL_MODE=true` ONLY).

Pre-configures structural prerequisites for a newly provisioned Trial Sandbox customer
so evaluators experience Grow's full enterprise depth in 60 seconds without empty-table
setup friction—while keeping the platform completely open to upload their own real files:
1. Linked `Maker` (End User) + `Checker` (Approver) companion personas for 1-click solo switching.
2. 2 Legal Entities (Main Operating Co. + Subsidiary) demonstrating holding-group governance.
3. 2 Departments + 1 Internal Project Owner with escalation manager bound.
4. 3 Pre-configured Bank Credit Facilities (`0%` vs `10%` Cash Margin, historical SLA speed,
   and scarce FX line) so `0-100` Smart Facility Scoring works immediately on Day 1.
5. 3 Active Relationship Bank FX & T-Bill Dealer Desks (`handshake_confirmed_at` set) ready
   for immediate multi-bank sealed bidding.
"""

import logging
import secrets
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from app.constants import LegalArtifactType, UserRole
from app.core.hashing import get_password_hash
from app.core.trial_guard import get_trial_caps, is_trial_customer, is_trial_plan_name

logger = logging.getLogger("app.trial_sandbox_seeder")


def get_or_create_trial_sandbox_plan(
    db: Session,
    wants_custody: bool = True,
    wants_issuance: bool = True,
    wants_quotations: bool = True,
    wants_reconciliation: bool = True,
) -> Any:
    """
    Returns a dedicated $0 'Guided Trial Sandbox' SubscriptionPlan matching the requested
    modules so trial approvals NEVER bind to 12-month commercial plans like 'MAX All plan'.
    """
    from app.models.models import SubscriptionPlan

    if wants_custody and wants_issuance and wants_quotations and wants_reconciliation:
        plan_name = "Guided Trial Sandbox — Full Suite"
    else:
        tags = []
        if wants_custody:
            tags.append("Custody")
        if wants_issuance:
            tags.append("Issuance")
        if wants_quotations:
            tags.append("Quotations")
        if wants_reconciliation:
            tags.append("Recon")
        plan_name = f"Guided Trial Sandbox — {' + '.join(tags) or 'Custody'}"

    caps = get_trial_caps()
    existing = db.query(SubscriptionPlan).filter(SubscriptionPlan.name == plan_name).first()
    if existing:
        existing.is_deleted = False
        existing.monthly_price = 0.0
        existing.annual_price = 0.0
        existing.max_users = max(int(existing.max_users or 0), 5)
        existing.max_checker_users = max(int(existing.max_checker_users or 0), 2)
        existing.max_records = caps["max_lgs"]
        existing.max_issuance_records = caps["max_lgs"]
        existing.can_maker_checker = True
        existing.can_multi_entity = True
        existing.can_ai_integration = True
        existing.can_image_storage = True
        existing.has_custody_module = bool(wants_custody)
        existing.has_issuance_module = bool(wants_issuance)
        existing.has_quotation_module = bool(wants_quotations)
        existing.has_reconciliation_module = bool(wants_reconciliation)
        db.flush()
        return existing

    new_plan = SubscriptionPlan(
        name=plan_name,
        description=f"Isolated {caps['duration_days']}-day Guided Trial Sandbox plan with evaluation quotas.",
        duration_months=1,
        monthly_price=0.0,
        annual_price=0.0,
        max_users=5,
        max_checker_users=2,
        max_records=caps["max_lgs"],
        max_issuance_records=caps["max_lgs"],
        can_maker_checker=True,
        can_multi_entity=True,
        can_ai_integration=True,
        can_image_storage=True,
        can_email_inbox=False,
        has_custody_module=bool(wants_custody),
        has_issuance_module=bool(wants_issuance),
        has_quotation_module=bool(wants_quotations),
        has_reconciliation_module=bool(wants_reconciliation),
        grace_period_days=3,
    )
    db.add(new_plan)
    db.flush()
    return new_plan


def _ensure_reference_currencies_and_lg_types(db: Session) -> Dict[str, Any]:
    from app.models.models import Currency, LgType

    egp = db.query(Currency).filter(Currency.iso_code == "EGP", Currency.is_deleted == False).first()
    if not egp:
        egp = Currency(name="Egyptian Pound", iso_code="EGP", symbol="EGP")
        db.add(egp)
        db.flush()

    usd = db.query(Currency).filter(Currency.iso_code == "USD", Currency.is_deleted == False).first()
    if not usd:
        usd = Currency(name="US Dollar", iso_code="USD", symbol="$")
        db.add(usd)
        db.flush()

    eur = db.query(Currency).filter(Currency.iso_code == "EUR", Currency.is_deleted == False).first()
    if not eur:
        eur = Currency(name="Euro", iso_code="EUR", symbol="€")
        db.add(eur)
        db.flush()

    lg_types = db.query(LgType).filter(LgType.is_deleted == False).all()
    if not lg_types:
        for name, desc in [
            ("Performance Guarantee", "Final performance guarantee"),
            ("Bid Bond", "Initial tender / bid guarantee"),
            ("Advance Payment LG", "Advance payment guarantee"),
            ("Financial Guarantee", "Financial / payment guarantee"),
        ]:
            lt = LgType(name=name, description=desc)
            db.add(lt)
            lg_types.append(lt)
        db.flush()

    perf_lt = next((t for t in lg_types if "performance" in (t.name or "").lower()), lg_types[0])
    adv_lt = next((t for t in lg_types if "advance" in (t.name or "").lower()), lg_types[0])
    bid_lt = next((t for t in lg_types if "bid" in (t.name or "").lower()), lg_types[0])

    return {
        "egp": egp,
        "usd": usd,
        "eur": eur,
        "lg_type_ids": [lt.id for lt in lg_types],
        "perf_type_id": perf_lt.id,
        "adv_type_id": adv_lt.id,
        "bid_type_id": bid_lt.id,
    }


def _ensure_reference_banks(db: Session) -> List[Any]:
    """
    Prioritizes flagship real banks that have analyzed Fillable Bank PDF templates:
      #1: Emirates NBD Egypt (`ENBD` — Flagship bilingual fillable PDF form, 0% margin #1 winner)
      #2: Banque Misr (`BM` — 10% cash margin & slower SLA contrast)
      #3: Crédit Agricole Egypt (`CAE` — Scarce cross-border & FX line)
    Falls back gracefully on clean/empty test databases.
    """
    from app.models.models import Bank

    all_active = (
        db.query(Bank)
        .filter(Bank.is_deleted == False)
        .order_by(Bank.id.asc())
        .all()
    )
    local_banks = [b for b in all_active if (b.name or "").strip().lower() != "foreign bank"]

    form_bank_ids = set()
    try:
        from app.models.models_issuance import BankFormTemplate

        form_bank_ids = {
            int(r[0])
            for r in db.query(BankFormTemplate.bank_id)
            .filter(BankFormTemplate.is_deleted == False, BankFormTemplate.is_active == True)
            .all()
            if r and r[0] is not None
        }
    except Exception:
        pass

    def _pick(predicate):
        return next((b for b in local_banks if predicate(b)), None)

    ordered: List[Any] = []
    enbd = _pick(
        lambda b: (b.short_name or "").strip().upper() == "ENBD"
        or "emirates nbd" in (b.name or "").lower()
    )
    first_form_bank = (
        enbd
        if (enbd and enbd.id in form_bank_ids)
        else _pick(lambda b: b.id in form_bank_ids)
    )
    bm = _pick(
        lambda b: (b.short_name or "").strip().upper() == "BM"
        or "banque misr" in (b.name or "").lower()
    )
    cae = _pick(
        lambda b: (b.short_name or "").strip().upper() == "CAE"
        or "agricole" in (b.name or "").lower()
    )

    for candidate in (first_form_bank, enbd, bm, cae):
        if candidate and all(x.id != candidate.id for x in ordered):
            ordered.append(candidate)

    # Ensure ENBD has crisp address & hotline for Custody AI Scan & Issuance letters
    if enbd:
        changed_enbd = False
        if not (enbd.address or "").strip():
            enbd.address = "Plot 85, South Teseen St., Fifth Settlement, New Cairo, Egypt"
            changed_enbd = True
        if not (enbd.phone_number or "").strip():
            enbd.phone_number = "16664"
            changed_enbd = True
        if changed_enbd:
            db.add(enbd)
            db.flush()

    for b in local_banks:
        if len(ordered) >= 3:
            break
        if all(x.id != b.id for x in ordered):
            ordered.append(b)

    if len(ordered) >= 3:
        return ordered[:3]

    seed_specs = [
        ("Emirates NBD Egypt", "ENBD", "EBONEECA", "emiratesnbd.com"),
        ("Banque Misr", "BM", "BMISEGCX", "banquemisr.com"),
        ("Crédit Agricole Egypt", "CAE", "AGRIEGCX", "ca-egypt.com"),
    ]
    for name, short_n, swift, dom in seed_specs:
        if len(ordered) >= 3:
            break
        existing = db.query(Bank).filter(Bank.name == name, Bank.is_deleted == False).first()
        if not existing:
            existing = Bank(
                name=name,
                short_name=short_n,
                swift_code=swift,
                email_domain=dom,
                address="Plot 85, South Teseen St., Fifth Settlement, New Cairo, Egypt",
                phone_number="16664",
                portal_access_enabled=True,
                allow_dealer_comments=True,
            )
            db.add(existing)
            db.flush()
        if all(b.id != existing.id for b in ordered):
            ordered.append(existing)

    return ordered[:3]


def ensure_trial_sandbox_seeded(db: Session, customer_id: int) -> Dict[str, Any]:
    """
    Idempotently seeds trial personas, entities, facilities, and simulated FX dealer desks.
    Strictly executes ONLY for Trial Sandbox customers (`is_trial_customer == True`).
    """
    if not customer_id or not is_trial_customer(db, customer_id):
        return {"seeded": False, "reason": "not_trial_customer"}

    from app.crud.crud import crud_legal_artifact
    from app.models.models import (
        Customer,
        CustomerEntity,
        Department,
        InternalOwnerContact,
        LGCategory,
        User,
    )
    from app.models.models_issuance import (
        CustomerBankAccount,
        IssuanceFacility,
        IssuanceFacilitySubLimit,
    )
    from app.models.models_quotation import QuotationBank

    customer = db.query(Customer).filter(Customer.id == int(customer_id)).first()
    if not customer:
        return {"seeded": False, "reason": "customer_not_found"}

    # Ensure latest legal version so companion personas never hit legal modal friction when switching
    latest_tc = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.TERMS_AND_CONDITIONS)
    latest_pp = crud_legal_artifact.get_by_artifact_type(db, artifact_type=LegalArtifactType.PRIVACY_POLICY)
    legal_ver = max(
        float(latest_tc.version) if latest_tc and latest_tc.version else 1.0,
        float(latest_pp.version) if latest_pp and latest_pp.version else 1.0,
    )

    # Ensure trial plan permits Maker-Checker & AI features during sandbox evaluation (never mutate paid commercial plans)
    if customer.subscription_plan and is_trial_plan_name(customer.subscription_plan.name):
        plan = customer.subscription_plan
        if (plan.max_users or 0) < 5:
            plan.max_users = 5
        if (plan.max_checker_users or 0) < 2:
            plan.max_checker_users = 2
        plan.can_maker_checker = True
        plan.can_ai_integration = True
        plan.can_image_storage = True
        plan.can_multi_entity = True

    # 1. Entities (Ensure at least 2 entities to showcase multi-entity holding visibility)
    entities = (
        db.query(CustomerEntity)
        .filter(CustomerEntity.customer_id == customer.id, CustomerEntity.is_deleted == False)
        .order_by(CustomerEntity.id.asc())
        .all()
    )
    admin_email = customer.contact_email or f"admin.{customer.id}@sandbox.growbusinessdevelopment.com"
    email_domain = admin_email.split("@")[1] if "@" in admin_email else "sandbox.growbusinessdevelopment.com"

    if len(entities) == 0:
        from app.crud.crud_customer import generate_unique_entity_code
        e1 = CustomerEntity(
            customer_id=customer.id,
            entity_name=customer.name[:100],
            code=generate_unique_entity_code(db, customer.name or "HQ"),
            address=customer.address or "Fifth Settlement, New Cairo, Egypt",
            contact_person="Treasury Desk",
            contact_email=admin_email,
            is_active=True,
        )
        db.add(e1)
        db.flush()
        entities.append(e1)
    elif (entities[0].entity_name or "").strip().lower() in ("main entity", "default entity"):
        entities[0].entity_name = customer.name[:100]
        if not (entities[0].address or "").strip():
            entities[0].address = customer.address or "Fifth Settlement, New Cairo, Egypt"
        db.add(entities[0])
        db.flush()

    if len(entities) == 1:
        from app.crud.crud_customer import generate_unique_entity_code
        sub_code = generate_unique_entity_code(db, f"{(customer.name or 'SB')[:2]}S")
        e2 = CustomerEntity(
            customer_id=customer.id,
            entity_name=f"{customer.name} — Engineering & Projects Subsidiary"[:100],
            code=sub_code,
            address=customer.address or "New Cairo Operations",
            contact_person="Projects Finance",
            contact_email=admin_email,
            is_active=True,
        )
        db.add(e2)
        db.flush()
        entities.append(e2)

    # 2. Default Registrant to `End User` (Operational Hands-On Profile) + Companion `Corporate Admin` Profile
    existing_users = (
        db.query(User)
        .filter(User.customer_id == customer.id, User.is_deleted == False)
        .order_by(User.id.asc())
        .all()
    )
    primary_real_user = next(
        (u for u in existing_users if "@sandbox.growbusinessdevelopment.com" not in (u.email or "").lower()),
        None,
    )
    if primary_real_user:
        changed_primary = False
        if primary_real_user.role != UserRole.END_USER:
            primary_real_user.role = UserRole.END_USER
            changed_primary = True
        if primary_real_user.must_change_password:
            primary_real_user.must_change_password = False
            changed_primary = True
        if (primary_real_user.last_accepted_legal_version or 0.0) < legal_ver:
            primary_real_user.last_accepted_legal_version = legal_ver
            changed_primary = True
        if changed_primary:
            db.add(primary_real_user)

    # Clean up any obsolete sandbox helper users (e.g. old maker/checker helpers) except corpadmin.sandbox
    companion_admin = None
    for u in existing_users:
        if primary_real_user and u.id == primary_real_user.id:
            continue
        if u.role == UserRole.CORPORATE_ADMIN and companion_admin is None:
            companion_admin = u
        elif "@sandbox.growbusinessdevelopment.com" in (u.email or "").lower():
            u.is_deleted = True
            db.add(u)

    if companion_admin is None:
        companion_admin = User(
            email=f"corpadmin.sandbox.{customer.id}@sandbox.growbusinessdevelopment.com",
            password_hash=get_password_hash(secrets.token_urlsafe(20)),
            role=UserRole.CORPORATE_ADMIN,
            customer_id=customer.id,
            must_change_password=False,
            has_all_entity_access=True,
            last_accepted_legal_version=legal_ver,
        )
        db.add(companion_admin)

    db.flush()
    active_count = db.query(User).filter(User.customer_id == customer.id, User.is_deleted == False).count()
    customer.active_user_count = active_count
    customer.active_checker_count = 0
    db.add(customer)
    db.flush()

    # 3. Departments, LG Categories & Internal Owner Contact (for Custody Escalations)
    if db.query(Department).filter(Department.customer_id == customer.id, Department.is_deleted == False).count() == 0:
        db.add(Department(name="Projects & EPC", customer_id=customer.id))
        db.add(Department(name="Procurement & Supply Chain", customer_id=customer.id))

    if db.query(LGCategory).filter(LGCategory.customer_id == customer.id, LGCategory.is_deleted == False).count() == 0:
        db.add(
            LGCategory(
                name="Major Infrastructure Contracts",
                code="IN",
                customer_id=customer.id,
                is_default=True,
                has_all_entity_access=True,
            )
        )

    if (
        db.query(InternalOwnerContact)
        .filter(InternalOwnerContact.customer_id == customer.id, InternalOwnerContact.is_deleted == False)
        .count()
        == 0
    ):
        from app.core.trial_guard import is_personal_email_domain
        owner_contact_email = (
            admin_email
            if is_personal_email_domain(email_domain)
            else f"projects.owner@{email_domain}"
        )
        db.add(
            InternalOwnerContact(
                customer_id=customer.id,
                email=owner_contact_email,
                phone_number=customer.contact_phone or "+201000000000",
                internal_id="PM-01",
                manager_email=admin_email,
            )
        )

    refs = _ensure_reference_currencies_and_lg_types(db)
    banks = _ensure_reference_banks(db)
    today = date.today()

    # 4. Pre-Seed 3 Credit Facilities — `Emirates NBD Egypt (ENBD)` Ranked #1 Winning Facility!
    facility_specs = []
    if len(banks) >= 3:
        facility_specs = [
            {
                "bank": banks[0],
                "name": f"{banks[0].short_name or banks[0].name} — Flagship Corporate Umbrella Facility",
                "ref": f"FAC-UMB-{customer.id}01",
                "currency": refs["egp"],
                "limit": Decimal("100000000.00"),
                "init_util": Decimal("10000000.00"),
                "comm": Decimal("0.006500"),
                "margin": Decimal("0.00"),
                "sla_agreed": 1,
                "sla_actual": Decimal("1.00"),
                "sla_pct": Decimal("99.0"),
                "completed": 14,
                "multi_ccy": False,
                "cross_border": False,
                "sub_name": "0% Margin Bid, Advance & Performance Umbrella Line",
                "branch": "Fifth Settlement Corporate Branch, New Cairo",
            },
            {
                "bank": banks[1],
                "name": f"{banks[1].short_name or banks[1].name} — Standard Trade Line",
                "ref": f"FAC-TRD-{customer.id}02",
                "currency": refs["egp"],
                "limit": Decimal("50000000.00"),
                "init_util": Decimal("15000000.00"),
                "comm": Decimal("0.013500"),
                "margin": Decimal("10.00"),
                "sla_agreed": 3,
                "sla_actual": Decimal("3.60"),
                "sla_pct": Decimal("76.0"),
                "completed": 6,
                "multi_ccy": False,
                "cross_border": False,
                "sub_name": "Cash-Collaterized Domestic LG Line (10% Margin)",
                "branch": "Cairo Main Corporate Branch",
            },
            {
                "bank": banks[2],
                "name": f"{banks[2].short_name or banks[2].name} — Scarce Cross-Border & FX Line",
                "ref": f"FAC-FX-{customer.id}03",
                "currency": refs["egp"],
                "limit": Decimal("60000000.00"),
                "init_util": Decimal("8000000.00"),
                "comm": Decimal("0.010500"),
                "margin": Decimal("0.00"),
                "sla_agreed": 2,
                "sla_actual": Decimal("1.90"),
                "sla_pct": Decimal("91.0"),
                "completed": 5,
                "multi_ccy": True,
                "cross_border": True,
                "sub_name": "Multi-Currency & International Beneficiary Line",
                "branch": "Multinational Trade Finance Desk, New Cairo",
            },
        ]

    existing_facs = (
        db.query(IssuanceFacility)
        .filter(IssuanceFacility.customer_id == customer.id, IssuanceFacility.is_deleted == False)
        .order_by(IssuanceFacility.id.asc())
        .all()
    )
    if len(existing_facs) == 0 and facility_specs:
        for idx, spec in enumerate(facility_specs, start=1):
            acct = CustomerBankAccount(
                customer_id=customer.id,
                bank_id=spec["bank"].id,
                entity_id=entities[0].id,
                account_name=entities[0].entity_name,
                account_number=f"100290{customer.id:03d}{idx:02d}",
                customer_number=f"CIF-{customer.id}0{idx}",
                branch_name=spec["branch"],
                iban=f"EG380019000500000100290{customer.id:03d}{idx:02d}",
                is_default=True,
            )
            db.add(acct)
            db.flush()

            fac = IssuanceFacility(
                facility_name=spec["name"],
                reference_number=spec["ref"],
                bank_id=spec["bank"].id,
                customer_id=customer.id,
                facility_type="LG",
                currency_id=spec["currency"].id,
                total_limit_amount=spec["limit"],
                tenor_months=24,
                multi_currency_allowed=spec["multi_ccy"],
                status="ACTIVE",
                start_date=today - timedelta(days=90),
                expiry_date=today + timedelta(days=365),
                margin_reduces_exposure=True,
                facility_default_margin_pct=spec["margin"],
                sla_agreement_days=spec["sla_agreed"],
                actual_avg_sla_days=spec["sla_actual"],
                sla_commitment_pct=spec["sla_pct"],
                total_completed_issuances=spec["completed"],
                allow_cross_border=spec["cross_border"],
                allow_third_party_issuance=True,
                bank_account_id=acct.id,
            )
            fac.entities = list(entities)
            db.add(fac)
            db.flush()

            sub = IssuanceFacilitySubLimit(
                facility_id=fac.id,
                lg_type_ids=refs["lg_type_ids"],
                limit_name=spec["sub_name"],
                limit_amount=spec["limit"],
                initial_utilization=spec["init_util"],
                max_tenor_days=730,
                allows_confirmation=spec["cross_border"],
                default_commission_rate=spec["comm"],
                default_cash_margin_pct=spec["margin"],
                default_min_commission=Decimal("500.00"),
                default_flat_fee=Decimal("250.00"),
            )
            db.add(sub)
    elif len(existing_facs) == 3 and facility_specs and existing_facs[0].bank_id != facility_specs[0]["bank"].id:
        # Seamlessly align earlier-seeded sandbox facilities so ENBD is Facility #1 (Winning Facility + Fillable Form)
        for idx, (fac_obj, spec) in enumerate(zip(existing_facs, facility_specs), start=1):
            fac_obj.bank_id = spec["bank"].id
            fac_obj.facility_name = spec["name"]
            fac_obj.total_limit_amount = spec["limit"]
            fac_obj.facility_default_margin_pct = spec["margin"]
            fac_obj.sla_agreement_days = spec["sla_agreed"]
            fac_obj.actual_avg_sla_days = spec["sla_actual"]
            fac_obj.sla_commitment_pct = spec["sla_pct"]
            fac_obj.total_completed_issuances = spec["completed"]
            fac_obj.multi_currency_allowed = spec["multi_ccy"]
            fac_obj.allow_cross_border = spec["cross_border"]
            db.add(fac_obj)

            if fac_obj.bank_account_id:
                acct_obj = db.query(CustomerBankAccount).filter(CustomerBankAccount.id == fac_obj.bank_account_id).first()
                if acct_obj:
                    acct_obj.bank_id = spec["bank"].id
                    acct_obj.account_name = entities[0].entity_name
                    acct_obj.branch_name = spec["branch"]
                    db.add(acct_obj)

            subs = db.query(IssuanceFacilitySubLimit).filter(IssuanceFacilitySubLimit.facility_id == fac_obj.id).all()
            for s_obj in subs:
                s_obj.limit_name = spec["sub_name"]
                s_obj.limit_amount = spec["limit"]
                s_obj.initial_utilization = spec["init_util"]
                s_obj.default_commission_rate = spec["comm"]
                s_obj.default_cash_margin_pct = spec["margin"]
                s_obj.allows_confirmation = spec["cross_border"]
                s_obj.lg_type_ids = refs["lg_type_ids"]
                db.add(s_obj)
        db.flush()

    # 5. Pre-Seed 3 Active Relationship Bank FX & T-Bill Desks
    existing_qbanks = (
        db.query(QuotationBank)
        .filter(QuotationBank.customer_id == customer.id, QuotationBank.is_deleted == False)
        .order_by(QuotationBank.id.asc())
        .all()
    )
    if len(existing_qbanks) == 0 and len(banks) >= 3:
        now_utc = datetime.now(timezone.utc)
        for idx, b in enumerate(banks, start=1):
            slug = (b.short_name or f"bank{idx}").lower().replace(" ", "")
            bank_dom = (getattr(b, "email_domain", None) or "").strip().lower()
            dealer_email = (
                f"fx.desk.sandbox@{bank_dom}"
                if bank_dom and "." in bank_dom
                else f"fx.desk.sandbox@{slug}-sandbox-bank.com"
            )
            qb = QuotationBank(
                customer_id=customer.id,
                bank_id=b.id,
                trade_type="BOTH",
                entity_scope="ALL_ENTITIES",
                emails=dealer_email,
                contacts=[
                    {
                        "email": dealer_email,
                        "name": f"{b.short_name or b.name} Treasury Dealing Room",
                        "role": "EXECUTION",
                    }
                ],
                authorized_contact_email=dealer_email,
                authorized_contact_name=f"{b.short_name or b.name} Head of FX Desk",
                handshake_confirmed_at=now_utc,
            )
            db.add(qb)
    elif len(existing_qbanks) == 3 and len(banks) >= 3 and existing_qbanks[0].bank_id != banks[0].id:
        for idx, (qb_obj, b) in enumerate(zip(existing_qbanks, banks[:3]), start=1):
            slug = (b.short_name or f"bank{idx}").lower().replace(" ", "")
            bank_dom = (getattr(b, "email_domain", None) or "").strip().lower()
            dealer_email = (
                f"fx.desk.sandbox@{bank_dom}"
                if bank_dom and "." in bank_dom
                else f"fx.desk.sandbox@{slug}-sandbox-bank.com"
            )
            qb_obj.bank_id = b.id
            qb_obj.emails = dealer_email
            qb_obj.contacts = [
                {
                    "email": dealer_email,
                    "name": f"{b.short_name or b.name} Treasury Dealing Room",
                    "role": "EXECUTION",
                }
            ]
            qb_obj.authorized_contact_email = dealer_email
            qb_obj.authorized_contact_name = f"{b.short_name or b.name} Head of FX Desk"
            db.add(qb_obj)

    # 6. Pre-Seed Realistic Starter Transactions so Dashboards, Action Center & Scoring Inbox Are Immediately Alive!
    try:
        from app.models.models import IssuingMethod, LGRecord, LgStatus, Rule
        from app.models.models_issuance import IssuanceRequest, IssuedLGRecord

        now_utc = datetime.now(timezone.utc)

        valid_status = db.query(LgStatus).filter(LgStatus.name == "Valid").first() or db.query(LgStatus).first()
        if not valid_status:
            valid_status = LgStatus(name="Valid", description="Active valid guarantee")
            db.add(valid_status)
            db.flush()

        manual_method = db.query(IssuingMethod).first()
        if not manual_method:
            manual_method = IssuingMethod(name="Manual Delivery", description="Physical bank letter")
            db.add(manual_method)
            db.flush()

        urdg_rule = (
            db.query(Rule).filter(Rule.name.ilike("%758%")).first()
            or db.query(Rule).first()
        )
        if not urdg_rule:
            urdg_rule = Rule(name="URDG 758", description="ICC Uniform Rules for Demand Guarantees")
            db.add(urdg_rule)
            db.flush()

        owner_row = (
            db.query(InternalOwnerContact)
            .filter(InternalOwnerContact.customer_id == customer.id, InternalOwnerContact.is_deleted == False)
            .first()
        )
        cat_row = (
            db.query(LGCategory)
            .filter(LGCategory.customer_id == customer.id, LGCategory.is_deleted == False)
            .first()
        )

        perf_type_id = refs["perf_type_id"]
        adv_type_id = refs["adv_type_id"]

        # 6a. Seed 2 Inbound Custody LGs (1 Expiring in 18 Days -> Action Center, 1 Active USD LG)
        existing_custody_rows = (
            db.query(LGRecord)
            .filter(LGRecord.customer_id == customer.id, LGRecord.is_deleted == False)
            .order_by(LGRecord.id.asc())
            .all()
        )
        if len(existing_custody_rows) == 0 and banks and owner_row and cat_row:
            lg1 = LGRecord(
                customer_id=customer.id,
                beneficiary_corporate_id=entities[0].id,
                lg_sequence_number=1,
                issuer_name="Al-Nour Electromechanical Contractors S.A.E.",
                issuer_id="CR-449102",
                lg_number=f"LG-SMPL-{customer.id}-8840",
                lg_amount=Decimal("4500000.00"),
                lg_currency_id=refs["egp"].id,
                lg_payable_currency_id=refs["egp"].id,
                issuance_date=now_utc - timedelta(days=345),
                expiry_date=now_utc + timedelta(days=18),
                lg_period_months=12,
                auto_renewal=True,
                lg_type_id=perf_type_id,
                lg_status_id=valid_status.id,
                description_purpose="10% Final Performance Guarantee — New Cairo Substation Package (Sample Record)",
                issuing_bank_id=banks[0].id,
                issuing_bank_address=banks[0].address or "Plot 85, South Teseen St., Fifth Settlement, New Cairo",
                issuing_bank_phone=banks[0].phone_number or "16664",
                issuing_method_id=manual_method.id,
                applicable_rule_id=urdg_rule.id,
                internal_owner_contact_id=owner_row.id,
                lg_category_id=cat_row.id,
                internal_contract_project_id="EPC-NC-2026-04",
                notes="[TRIAL SAMPLE] Expiring soon — open this record to test Extend, Reduce, Release, or Liquidate letters!",
                migration_source="TRIAL_SAMPLE",
            )
            lg2 = LGRecord(
                customer_id=customer.id,
                beneficiary_corporate_id=entities[-1].id,
                lg_sequence_number=2,
                issuer_name="Global Turbine & Power Supplies Ltd.",
                issuer_id="REG-99812",
                lg_number=f"LG-SMPL-{customer.id}-8845",
                lg_amount=Decimal("125000.00"),
                lg_currency_id=refs["usd"].id,
                lg_payable_currency_id=refs["usd"].id,
                issuance_date=now_utc - timedelta(days=45),
                expiry_date=now_utc + timedelta(days=165),
                lg_period_months=7,
                auto_renewal=True,
                lg_type_id=adv_type_id,
                lg_status_id=valid_status.id,
                description_purpose="Advance Payment Security — Gas Compression Equipment Supply (Sample Record)",
                issuing_bank_id=banks[min(1, len(banks) - 1)].id,
                issuing_bank_address=banks[min(1, len(banks) - 1)].address or "Cairo Main Branch",
                issuing_bank_phone=banks[min(1, len(banks) - 1)].phone_number or "19888",
                issuing_method_id=manual_method.id,
                applicable_rule_id=urdg_rule.id,
                internal_owner_contact_id=owner_row.id,
                lg_category_id=cat_row.id,
                internal_contract_project_id="PO-GT-2026-19",
                notes="[TRIAL SAMPLE] Active foreign-currency guarantee demonstrating multi-entity & multi-currency visibility.",
                migration_source="TRIAL_SAMPLE",
            )
            db.add(lg1)
            db.add(lg2)
            customer.active_lg_count = 2
            db.flush()

            # Seed 1 Day-1 Overdue Delivered Instruction on Sample LG #2 strictly at initial sandbox creation
            # (NEVER retroactively injected into an already-active trial workspace!)
            from app.models.models import LGInstruction, Template

            ext_template = (
                db.query(Template)
                .filter(
                    Template.action_type == "LG_EXTENSION",
                    Template.is_notification_template == False,
                    Template.is_deleted == False,
                )
                .order_by(Template.is_global.desc(), Template.is_default.desc(), Template.id.desc())
                .first()
                or db.query(Template)
                .filter(
                    Template.is_notification_template == False,
                    Template.is_deleted == False,
                )
                .order_by(Template.id.asc())
                .first()
            )
            maker_actor = primary_real_user or companion_admin
            if ext_template and maker_actor:
                new_exp_dt = (today + timedelta(days=365)).isoformat()
                old_exp_dt = (today + timedelta(days=165)).isoformat()
                starter_inst = LGInstruction(
                    lg_record_id=lg2.id,
                    instruction_type="LG_EXTENSION",
                    serial_number=f"TRIAL-EXT-{customer.id}-01",
                    global_seq_per_lg=1,
                    type_seq_per_lg=1,
                    template_id=ext_template.id,
                    status="Instruction Delivered",
                    instruction_date=now_utc - timedelta(days=10),
                    delivery_date=now_utc - timedelta(days=8),
                    bank_reply_date=None,
                    details={
                        "old_expiry_date": old_exp_dt,
                        "new_expiry_date": new_exp_dt,
                        "requested_expiry_date": new_exp_dt,
                        "extension_months": 6,
                        "trial_sample": True,
                        "notes": "Delivered 8 days ago — ready in Action Center to test Send Bank Reminder & Record Bank Reply!",
                    },
                    maker_user_id=maker_actor.id,
                    is_printed=True,
                    sent_to_bank=True,
                )
                db.add(starter_inst)
                db.flush()
        elif existing_custody_rows and banks and existing_custody_rows[0].migration_source == "TRIAL_SAMPLE":
            changed_lg0 = False
            if existing_custody_rows[0].issuing_bank_id != banks[0].id:
                existing_custody_rows[0].issuing_bank_id = banks[0].id
                existing_custody_rows[0].issuing_bank_address = banks[0].address or "Plot 85, South Teseen St., Fifth Settlement, New Cairo"
                existing_custody_rows[0].issuing_bank_phone = banks[0].phone_number or "16664"
                existing_custody_rows[0].lg_type_id = perf_type_id
                changed_lg0 = True
            if not existing_custody_rows[0].auto_renewal:
                existing_custody_rows[0].auto_renewal = True
                changed_lg0 = True
            if changed_lg0:
                db.add(existing_custody_rows[0])

        # 6b. Resolve first sub-limit & ensure initial_utilization is populated
        all_facs = (
            db.query(IssuanceFacility)
            .filter(IssuanceFacility.customer_id == customer.id, IssuanceFacility.is_deleted == False)
            .order_by(IssuanceFacility.id.asc())
            .all()
        )
        util_defaults = [Decimal("10000000.00"), Decimal("15000000.00"), Decimal("8000000.00")]
        first_sub_limit = None
        for f_i, fac_obj in enumerate(all_facs):
            subs = (
                db.query(IssuanceFacilitySubLimit)
                .filter(IssuanceFacilitySubLimit.facility_id == fac_obj.id)
                .all()
            )
            for s_obj in subs:
                if first_sub_limit is None:
                    first_sub_limit = s_obj
                if not s_obj.initial_utilization or Decimal(str(s_obj.initial_utilization)) == Decimal("0"):
                    s_obj.initial_utilization = util_defaults[f_i % len(util_defaults)]
                    db.add(s_obj)

        # 6c. Seed 1 Approved Issuance Request (Ready for ENBD #1 Scoring + 1-Click ENBD Fillable PDF!) + 1 Active Issued LG
        sample_req = (
            db.query(IssuanceRequest)
            .filter(
                IssuanceRequest.customer_id == customer.id,
                IssuanceRequest.serial_number == f"REQ-SMPL-{customer.id:03d}-01",
            )
            .first()
        )
        existing_ireq = db.query(IssuanceRequest).filter(IssuanceRequest.customer_id == customer.id).count()
        if existing_ireq == 0:
            sample_req = IssuanceRequest(
                customer_id=customer.id,
                serial_number=f"REQ-SMPL-{customer.id:03d}-01",
                status="APPROVED_INTERNAL",
                transaction_type="NEW_ISSUANCE",
                issuing_entity_id=entities[0].id,
                requestor_name="Tarek El-Masry",
                requestor_email=f"projects.lead@{email_domain}",
                department="Projects & EPC",
                job_title="Contracts Manager",
                phone_number=customer.contact_phone or "+201001234567",
                reference_type="Contract",
                reference_number="CNT-NUCA-2026-77",
                reference_amount=Decimal("60000000.00"),
                reference_currency_id=refs["egp"].id,
                lg_type_id=perf_type_id,
                lg_purpose="10% Performance Bond — New Administrative Capital Utilities Package (Sample Request)",
                amount=Decimal("6000000.00"),
                currency_id=refs["egp"].id,
                payable_currency_id=refs["egp"].id,
                requested_issue_date=today,
                requested_expiry_date=today + timedelta(days=365),
                expiry_type="FIXED_DATE",
                operational_status="Operative",
                lg_language="EN",
                beneficiary_name="New Urban Communities Authority (NUCA)",
                beneficiary_address="Sheikh Zayed, Giza, Egypt",
                beneficiary_country="EG",
                applicable_rules="URDG_758",
                custom_field_1_value="10%",
                custom_field_2_value="0%",
                locked_for_issuance=False,
                comments="[TRIAL_SAMPLE] Approved request ready for execution — click 'Issue LG', select the #1 Recommended Bank Facility (0% Margin), and choose 'Fill Bank Form' to auto-fill the bank's official PDF!",
            )
            db.add(sample_req)
            db.flush()
        elif sample_req and sample_req.status == "APPROVED_INTERNAL":
            # Ensure ENBD Fillable PDF compatibility & release any stale false mutex
            meta = dict(sample_req.metadata_json or {})
            if sample_req.locked_for_issuance and not meta.get("locked_bank_id"):
                sample_req.locked_for_issuance = False
            sample_req.lg_type_id = perf_type_id
            sample_req.lg_language = "EN"
            sample_req.applicable_rules = "URDG_758"
            if not sample_req.custom_field_1_value:
                sample_req.custom_field_1_value = "10%"
            if not sample_req.custom_field_2_value:
                sample_req.custom_field_2_value = "0%"
            db.add(sample_req)

        existing_ilg_rows = (
            db.query(IssuedLGRecord)
            .filter(IssuedLGRecord.customer_id == customer.id)
            .order_by(IssuedLGRecord.id.asc())
            .all()
        )
        if len(existing_ilg_rows) == 0 and banks and first_sub_limit:
            sample_ilg = IssuedLGRecord(
                lg_ref_number=f"ILG-SMPL-{customer.id:03d}-01",
                internal_serial=f"2026-HQ01-0001S{customer.id}",
                customer_id=customer.id,
                facility_sub_limit_id=first_sub_limit.id,
                bank_id=banks[0].id,
                beneficiary_name="Egyptian Electricity Transmission Co. (EETC)",
                current_amount=Decimal("8500000.00"),
                currency_id=refs["egp"].id,
                issue_date=today - timedelta(days=60),
                expiry_date=today + timedelta(days=305),
                expiry_type="FIXED_DATE",
                status="ACTIVE",
                issuance_method="MANUAL_PDF",
                delivery_date=today - timedelta(days=61),
                delivery_method="HAND_DELIVERY",
                bank_reply_type="LG_ISSUED",
                bank_reply_date=today - timedelta(days=60),
                bank_lg_number=f"ENBD-LG-9920{customer.id}",
                bank_lg_issue_date=today - timedelta(days=60),
                bank_lg_expiry_date=today + timedelta(days=305),
                bank_lg_amount=Decimal("8500000.00"),
            )
            db.add(sample_ilg)
            customer.active_issuance_lg_count = 1
            db.flush()
        elif existing_ilg_rows and banks and existing_ilg_rows[0].lg_ref_number == f"ILG-SMPL-{customer.id:03d}-01":
            if existing_ilg_rows[0].bank_id != banks[0].id:
                existing_ilg_rows[0].bank_id = banks[0].id
                db.add(existing_ilg_rows[0])
    except Exception as starter_exc:
        logger.warning(f"[TrialSandboxSeeder] Starter records seed warning for customer_id={customer.id}: {starter_exc}", exc_info=True)

    db.commit()
    logger.info(f"[TrialSandboxSeeder] Ensured sandbox baseline for customer_id={customer.id}")
    return {"seeded": True, "customer_id": customer.id}
