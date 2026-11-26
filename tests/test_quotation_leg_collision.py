"""
Unit & Integration Tests for Quotation Pair Uniqueness & Counterparty Leg Collision Logic.
Verifies:
1. _normalize_leg_trade_key canonicalizes Buy/Sell + pair equivalence
   (Buy USD/EGP == Sell EGP/USD) while keeping Buy USD/EGP != Sell USD/EGP.
2. get_bank_leg_signature ignores direction so a single counterparty bank cannot
   receive both Buy and Sell on the same currency pair, value date, and base.
3. crud_quotation.create_request:
   - Permits same currency pair with different Value Dates.
   - Permits same currency pair & Value Date with different Quotation Bases (Execution vs Indicative).
   - Rejects economically identical legs (Buy USD/EGP + Sell EGP/USD) on same date & base.
   - Permits Buy USD/EGP + Sell USD/EGP on same date & base when routed to different banks
     (via Invisible/excluded per-leg selection).
   - Rejects Buy USD/EGP + Sell USD/EGP on same date & base when both active for the same bank.
   - Rejects cross-entity bank invited to Execution + Indicative legs on same pair & date
     (because cross-entity is always forced to Indicative).
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.main  # ensure all models are registered
from app.crud.crud_quotation import (
    _normalize_leg_trade_key,
    crud_quotation,
    get_bank_leg_signature,
)
from app.models.models import (
    Bank,
    Customer,
    CustomerConfiguration,
    CustomerEntity,
    GlobalConfiguration,
    SubscriptionPlan,
    User,
)
from app.models.models_quotation import (
    QuotationBank,
    QuotationBankAssignment,
    QuotationBankContactInvitation,
    QuotationBankEntity,
    QuotationBankLegConfig,
    QuotationLeg,
    QuotationRequest,
)
from app.schemas.schemas_quotation import QuotationLegCreate, QuotationRequestCreate


@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(type_, compiler, **kw):
    return "JSON"


def _make_sqlite_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    tables = [
        SubscriptionPlan.__table__,
        Customer.__table__,
        CustomerEntity.__table__,
        User.__table__,
        GlobalConfiguration.__table__,
        CustomerConfiguration.__table__,
        Bank.__table__,
        QuotationBank.__table__,
        QuotationBankEntity.__table__,
        QuotationBankContactInvitation.__table__,
        QuotationRequest.__table__,
        QuotationLeg.__table__,
        QuotationBankAssignment.__table__,
        QuotationBankLegConfig.__table__,
    ]
    for tbl in tables:
        tbl.create(bind=engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    return Session()


def _seed_banks(db):
    plan = SubscriptionPlan(
        id=1, name="Enterprise", duration_months=12, monthly_price=100.0, annual_price=1000.0
    )
    cust = Customer(
        id=1,
        name="Acme Corp",
        contact_email="treasury@acme.com",
        subscription_plan_id=1,
        end_date=datetime.now(timezone.utc) + timedelta(days=365),
    )
    ent1 = CustomerEntity(id=10, customer_id=1, entity_name="Acme Egypt", code="EGY", is_active=True, is_deleted=False)
    ent2 = CustomerEntity(id=20, customer_id=1, entity_name="Acme UAE", code="UAE", is_active=True, is_deleted=False)
    user = User(id=1, customer_id=1, email="maker@acme.com", password_hash="x", role="end_user")
    bank_a = Bank(id=101, name="Bank Alpha", short_name="ALPHA", allow_manual_recording=True)
    bank_b = Bank(id=102, name="Bank Beta", short_name="BETA", allow_manual_recording=True)
    bank_cross = Bank(id=103, name="Cross Entity Bank", short_name="CROSS", allow_manual_recording=True)

    qb_a = QuotationBank(
        id=201, customer_id=1, bank_id=101, trade_type="BOTH",
        entity_scope="ALL_ENTITIES", emails="desk@alpha.com", contacts=[],
    )
    qb_b = QuotationBank(
        id=202, customer_id=1, bank_id=102, trade_type="BOTH",
        entity_scope="ALL_ENTITIES", emails="desk@beta.com", contacts=[],
    )
    qb_cross = QuotationBank(
        id=203, customer_id=1, bank_id=103, trade_type="BOTH",
        entity_scope="SPECIFIC_ENTITIES", emails="desk@cross.com", contacts=[],
    )
    assoc_cross = QuotationBankEntity(quotation_bank_id=203, entity_id=20)

    db.add_all([plan, cust, ent1, ent2, user, bank_a, bank_b, bank_cross, qb_a, qb_b, qb_cross, assoc_cross])
    db.commit()


def _base_rfq_kwargs():
    now = datetime.now(timezone.utc)
    return {
        "type": "FX_SPOT",
        "direction": "Buy",
        "entity_id": 10,
        "windowStart": now + timedelta(hours=1),
        "windowEnd": now + timedelta(hours=2),
        "legalDisclaimerAccepted": True,
    }


def test_normalize_trade_key_and_bank_signature():
    # Economically identical legs share trade key
    assert _normalize_leg_trade_key("USD", "EGP", "Buy") == _normalize_leg_trade_key("EGP", "USD", "Sell")
    # Opposite trades on same pair have distinct trade keys
    assert _normalize_leg_trade_key("USD", "EGP", "Buy") != _normalize_leg_trade_key("USD", "EGP", "Sell")

    # Bank-level signature omits direction: Buy USD/EGP and Sell USD/EGP collide on same date & base
    sig_buy = get_bank_leg_signature("USD", "EGP", "Buy", "2026-12-01", "Execution")
    sig_sell = get_bank_leg_signature("USD", "EGP", "Sell", "2026-12-01", "Execution")
    assert sig_buy == sig_sell

    # Different value date -> no collision
    sig_other_date = get_bank_leg_signature("USD", "EGP", "Buy", "2026-12-02", "Execution")
    assert sig_buy != sig_other_date

    # Different quotation base -> no collision
    sig_indic = get_bank_leg_signature("USD", "EGP", "Buy", "2026-12-01", "Indicative")
    assert sig_buy != sig_indic


def test_create_request_allows_different_value_dates_and_bases():
    db = _make_sqlite_session()
    try:
        _seed_banks(db)
        d1 = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")
        d2 = (datetime.now(timezone.utc) + timedelta(days=5)).strftime("%Y-%m-%d")

        payload = QuotationRequestCreate(
            **_base_rfq_kwargs(),
            selectedBanks=json.dumps([{"id": 101, "quotationBase": "Execution"}]),
            pairs=[
                QuotationLegCreate(
                    direction="Buy", buyCurrency="USD", sellCurrency="EGP", amount=100000,
                    valueDate=d1, quotationBase="Execution",
                    selectedBanks=[{"id": 101, "quotationBase": "Execution", "valueDate": d1, "isInvited": True}],
                ),
                QuotationLegCreate(
                    direction="Buy", buyCurrency="USD", sellCurrency="EGP", amount=150000,
                    valueDate=d2, quotationBase="Execution",
                    selectedBanks=[{"id": 101, "quotationBase": "Execution", "valueDate": d2, "isInvited": True}],
                ),
                QuotationLegCreate(
                    direction="Buy", buyCurrency="USD", sellCurrency="EGP", amount=200000,
                    valueDate=d1, quotationBase="Indicative",
                    selectedBanks=[{"id": 101, "quotationBase": "Indicative", "valueDate": d1, "isInvited": True}],
                ),
            ],
        )

        rfq, _ = crud_quotation.create_request(db, customer_id=1, user_id=1, requires_approval=False, obj_in=payload)
        assert rfq.id is not None
        assert len(rfq.legs) == 3
    finally:
        db.close()


def test_create_request_rejects_economically_identical_inverse_pair():
    db = _make_sqlite_session()
    try:
        _seed_banks(db)
        d1 = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")

        payload = QuotationRequestCreate(
            **_base_rfq_kwargs(),
            selectedBanks=json.dumps([{"id": 101, "quotationBase": "Execution"}]),
            pairs=[
                QuotationLegCreate(
                    direction="Buy", buyCurrency="USD", sellCurrency="EGP", amount=100000,
                    valueDate=d1, quotationBase="Execution",
                    selectedBanks=[{"id": 101, "quotationBase": "Execution", "valueDate": d1}],
                ),
                QuotationLegCreate(
                    direction="Sell", buyCurrency="EGP", sellCurrency="USD", amount=4800000,
                    valueDate=d1, quotationBase="Execution",
                    selectedBanks=[{"id": 101, "quotationBase": "Execution", "valueDate": d1}],
                ),
            ],
        )

        try:
            crud_quotation.create_request(db, customer_id=1, user_id=1, requires_approval=False, obj_in=payload)
            assert False, "Expected HTTPException for economically identical inverse pair"
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Duplicate currency pair detected" in exc.detail
    finally:
        db.close()


def test_create_request_buy_and_sell_same_day_split_across_different_banks():
    db = _make_sqlite_session()
    try:
        _seed_banks(db)
        d1 = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")

        # 1. Allowed when Buy goes to Bank Alpha and Sell goes to Bank Beta
        split_payload = QuotationRequestCreate(
            **_base_rfq_kwargs(),
            selectedBanks=json.dumps([
                {"id": 101, "quotationBase": "Execution"},
                {"id": 102, "quotationBase": "Execution"},
            ]),
            pairs=[
                QuotationLegCreate(
                    direction="Buy", buyCurrency="USD", sellCurrency="EGP", amount=100000,
                    valueDate=d1, quotationBase="Execution",
                    selectedBanks=[
                        {"id": 101, "quotationBase": "Execution", "valueDate": d1, "isInvited": True},
                        {"id": 102, "quotationBase": "Invisible", "valueDate": d1, "isInvited": False},
                    ],
                ),
                QuotationLegCreate(
                    direction="Sell", buyCurrency="USD", sellCurrency="EGP", amount=100000,
                    valueDate=d1, quotationBase="Execution",
                    selectedBanks=[
                        {"id": 101, "quotationBase": "Invisible", "valueDate": d1, "isInvited": False},
                        {"id": 102, "quotationBase": "Execution", "valueDate": d1, "isInvited": True},
                    ],
                ),
            ],
        )
        rfq, _ = crud_quotation.create_request(db, customer_id=1, user_id=1, requires_approval=False, obj_in=split_payload)
        assert len(rfq.legs) == 2

        # 2. Rejected when BOTH Buy and Sell are active for the SAME bank on the same date & base
        same_bank_payload = QuotationRequestCreate(
            **_base_rfq_kwargs(),
            selectedBanks=json.dumps([{"id": 101, "quotationBase": "Execution"}]),
            pairs=[
                QuotationLegCreate(
                    direction="Buy", buyCurrency="USD", sellCurrency="EGP", amount=100000,
                    valueDate=d1, quotationBase="Execution",
                    selectedBanks=[{"id": 101, "quotationBase": "Execution", "valueDate": d1, "isInvited": True}],
                ),
                QuotationLegCreate(
                    direction="Sell", buyCurrency="USD", sellCurrency="EGP", amount=100000,
                    valueDate=d1, quotationBase="Execution",
                    selectedBanks=[{"id": 101, "quotationBase": "Execution", "valueDate": d1, "isInvited": True}],
                ),
            ],
        )
        try:
            crud_quotation.create_request(db, customer_id=1, user_id=1, requires_approval=False, obj_in=same_bank_payload)
            assert False, "Expected HTTPException when both Buy and Sell active on same bank"
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Counterparty conflict" in exc.detail
    finally:
        db.close()


def test_cross_entity_bank_always_indicative_collision():
    db = _make_sqlite_session()
    try:
        _seed_banks(db)
        d1 = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")

        payload = QuotationRequestCreate(
            **_base_rfq_kwargs(),
            selectedBanks=json.dumps([{"id": 103, "quotationBase": "Indicative"}]),
            pairs=[
                QuotationLegCreate(
                    direction="Buy", buyCurrency="USD", sellCurrency="EGP", amount=100000,
                    valueDate=d1, quotationBase="Execution",
                    selectedBanks=[{"id": 103, "quotationBase": "Execution", "valueDate": d1, "isInvited": True}],
                ),
                QuotationLegCreate(
                    direction="Buy", buyCurrency="USD", sellCurrency="EGP", amount=100000,
                    valueDate=d1, quotationBase="Indicative",
                    selectedBanks=[{"id": 103, "quotationBase": "Indicative", "valueDate": d1, "isInvited": True}],
                ),
            ],
        )

        with patch(
            "app.crud.crud_config.crud_customer_configuration.get_customer_config_or_global_fallback",
            return_value={"effective_value": "true"},
        ):
            try:
                crud_quotation.create_request(db, customer_id=1, user_id=1, requires_approval=False, obj_in=payload)
                assert False, "Expected HTTPException for cross-entity bank Execution+Indicative collision"
            except HTTPException as exc:
                assert exc.status_code == 400
                assert "Counterparty conflict" in exc.detail
    finally:
        db.close()


if __name__ == "__main__":
    test_normalize_trade_key_and_bank_signature()
    print("[PASS] test_normalize_trade_key_and_bank_signature")
    test_create_request_allows_different_value_dates_and_bases()
    print("[PASS] test_create_request_allows_different_value_dates_and_bases")
    test_create_request_rejects_economically_identical_inverse_pair()
    print("[PASS] test_create_request_rejects_economically_identical_inverse_pair")
    test_create_request_buy_and_sell_same_day_split_across_different_banks()
    print("[PASS] test_create_request_buy_and_sell_same_day_split_across_different_banks")
    test_cross_entity_bank_always_indicative_collision()
    print("[PASS] test_cross_entity_bank_always_indicative_collision")
    print("\nALL QUOTATION LEG COLLISION TESTS PASSED")
