"""
Phase 4 Verification Suite: Closed-Loop Cross-Module Treasury Integration
Verifies:
1. Isolated SQLite Resilience (when LG/Facility/Quotation tables do not exist in minimal unit-test DBs,
   and Shadow LG detection + Idle Cash T-Bill RFQ Opportunity generation).
2. Live PostgreSQL Cross-Module End-to-End Audit:
   - Facility SubLimit (`1.50%` p.a. commission, `500` flat fee, `10%` cash margin cap) + `IssuedLGRecord` (`1,000,000` EGP):
     a) Bank debits `22,000` EGP commission vs contractual `15,500` EGP -> flags `COMMISSION_OVERCHARGE_CLAIM` (`+6,500` claimable)!
     b) Bank debits `250,000` EGP (`25%`) cash margin vs agreed `10%` (`100,000`) -> flags `EXCESS_CASH_MARGIN_HELD` (`+150,000` claimable)!
     c) Bank credits `100,000` EGP margin release -> flags `CASH_MARGIN_RELEASED`!
     d) Bank debits `12,000` EGP on unknown `LG-SHADOW-9941` -> flags `SHADOW_LG_UNREGISTERED`!
   - Awarded FX Spot RFQ (`Buy 100,000 USD` vs `EGP` at `winner_rate = 49.50` -> expected `4,950,000` EGP):
     a) Deal 1 settled at `4,965,000` EGP -> flags `SETTLED_RATE_OR_FEE_DISCREPANCY` (`+15,000` EGP slippage claim)!
     b) Deal 2 overdue past `value_date` with no bank line -> flags `OVERDUE_UNSETTLED_DEAL`!
   - Idle Cash 30d Excess -> generates pre-filled `prefill_type=TBILL` URL & 91-day yield calculation!
   - Live HTTP Multi-Page Landscape Claim Pack PDF (WeasyPrint) + 3-Sheet Excel Workbook + Multi-Tenant Isolation!
"""

import io
import os
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.models.models_reconciliation_v2 import (
    Base,
    BankAccountProfile,
    BankStatement,
    BankTransaction,
    ClassificationRule,
    ClassificationTaxonomy,
    CollaborativePattern,
    CollaborativeTenantVote,
    Counterparty,
    InternalLedgerRecord,
    MatchingToleranceRule,
    MultiReference,
    ReconciliationExportProfile,
    ReconciliationMatch,
    StatementInputMappingProfile,
)
from app.services.reconciliation_treasury_bridge_service import (
    reconciliation_treasury_bridge_service,
)


def _make_sqlite_db():
    eng = create_engine("sqlite:///:memory:", echo=False)
    SessionCls = sessionmaker(bind=eng, autoflush=False, autocommit=False)
    tables = [
        ClassificationTaxonomy.__table__,
        Counterparty.__table__,
        ClassificationRule.__table__,
        BankStatement.__table__,
        BankTransaction.__table__,
        MultiReference.__table__,
        InternalLedgerRecord.__table__,
        ReconciliationMatch.__table__,
        CollaborativePattern.__table__,
        CollaborativeTenantVote.__table__,
        BankAccountProfile.__table__,
        MatchingToleranceRule.__table__,
        StatementInputMappingProfile.__table__,
        ReconciliationExportProfile.__table__,
    ]
    Base.metadata.create_all(bind=eng, tables=tables)
    return SessionCls()


def test_sqlite_shadow_lg_and_idle_cash_rfq_resilience():
    db = _make_sqlite_db()
    try:
        stmt = BankStatement(
            company_id=55,
            bank_id=1,
            account_number="ACC-P4-SQLITE",
            file_name="Aug2026.csv",
            opening_balance=Decimal("1000000.00"),
            closing_balance=Decimal("985000.00"),
            statement_start_date=datetime(2026, 8, 1),
            statement_end_date=datetime(2026, 8, 31),
            status="VALIDATED",
        )
        db.add(stmt)
        db.flush()

        db.add(
            BankTransaction(
                statement_id=stmt.id,
                account_number="ACC-P4-SQLITE",
                booking_date=datetime(2026, 8, 14),
                value_date=datetime(2026, 8, 14),
                debit_amount=Decimal("15000.00"),
                credit_amount=Decimal("0.00"),
                currency="EGP",
                running_balance=Decimal("985000.00"),
                raw_description="QUARTERLY GUARANTEE COMMISSION REF LG-SHADOW-7701",
            )
        )
        db.commit()

        rep = reconciliation_treasury_bridge_service.build_treasury_bridge_report(
            db=db,
            customer_id=55,
            currency="EGP",
            minimum_cash_buffer=200000.0,
            benchmark_annual_rate_pct=20.0,
        )
        assert rep["summary_kpis"]["shadow_unregistered_lgs_count"] == 1
        assert rep["shadow_lgs"][0]["suspected_lg_ref"] == "LG-SHADOW-7701"

        # Excess cash = 985,000 - 200,000 = 785,000 -> rounded 10k tranche = 780,000!
        assert len(rep["idle_cash_opportunities"]) == 1
        opp = rep["idle_cash_opportunities"][0]
        assert Decimal(str(opp["recommended_rfq_tranche"])) == Decimal("780000")
        assert "prefill_type=TBILL&prefill_amount=780000" in opp["prefill_url_query"]
        assert opp["estimated_91d_interest_gain"] > 38000.0
        print("[PASS] test_sqlite_shadow_lg_and_idle_cash_rfq_resilience passed")
    finally:
        db.close()


def test_phase4_postgres_lg_tariff_and_rfq_settlement_e2e():
    from _test_harness import (
        IsolatedTenantFixture,
        SessionLocal,
        app,
        extract_pdf_info,
        make_auth_headers,
    )
    from app.models.models_issuance import (
        IssuanceFacility,
        IssuanceFacilitySubLimit,
        IssuedLGRecord,
    )
    from app.models.models_quotation import QuotationLeg, QuotationRequest

    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="P4_BRIDGE")
    client = TestClient(app)
    created_rfq_ids = []
    created_lg_id = None
    created_fac_id = None
    try:
        admin_h = make_auth_headers(fx.admin_user, extra_claims={"has_reconciliation_module": True})
        beta_h = make_auth_headers(fx.beta_admin, extra_claims={"has_reconciliation_module": True})
        uid = uuid.uuid4().hex[:6].upper()

        # 1. Seed IssuanceFacility + SubLimit (1.5% annual commission, 500 EGP flat fee, 10% cash margin)
        fac = IssuanceFacility(
            facility_name=f"CIB Master LG Line {uid}",
            reference_number=f"FAC-{uid}",
            bank_id=fx.bank_cib.id,
            customer_id=fx.cust_alpha.id,
            currency_id=fx.ccy_egp.id,
            total_limit_amount=Decimal("20000000.00"),
            status="ACTIVE",
            facility_default_margin_pct=Decimal("10.00"),
        )
        db.add(fac)
        db.flush()
        created_fac_id = fac.id

        sub = IssuanceFacilitySubLimit(
            facility_id=fac.id,
            lg_type_ids=[],
            limit_name="Performance Bonds",
            limit_amount=Decimal("10000000.00"),
            default_commission_rate=Decimal("0.015000"),  # 1.50% p.a.
            default_cash_margin_pct=Decimal("10.00"),     # 10.00% cash margin cap
            default_min_commission=Decimal("500.00"),
            default_flat_fee=Decimal("500.00"),
        )
        db.add(sub)
        db.flush()

        # Seed 1-Year (365d) 1,000,000 EGP IssuedLGRecord:
        # Contractual Commission = 1,000,000 * 1.5% * (365/365) + 500 = 15,500.00 EGP!
        # Contractual Cash Margin = 1,000,000 * 10% = 100,000.00 EGP!
        lg_ref = f"LG-CIB-{uid}"
        ilg = IssuedLGRecord(
            lg_ref_number=lg_ref,
            bank_lg_number=lg_ref,
            customer_id=fx.cust_alpha.id,
            facility_sub_limit_id=sub.id,
            bank_id=fx.bank_cib.id,
            beneficiary_name="Ministry of Transport",
            current_amount=Decimal("1000000.00"),
            currency_id=fx.ccy_egp.id,
            issue_date=date(2026, 8, 1),
            expiry_date=date(2027, 8, 1),
            status="ACTIVE",
        )
        db.add(ilg)
        db.flush()
        created_lg_id = ilg.id

        # 2. Seed Two Awarded FX Spot RFQs at CIB:
        #    - RFQ 1 (`RFQ-SLIP-{uid}`): Buy 100,000 USD vs EGP at 49.50 on 2026-08-10 (Expected = 4,950,000 EGP)
        #    - RFQ 2 (`RFQ-MISS-{uid}`): Buy 50,000 USD vs EGP at 49.40 on 2026-08-15 (Unsettled despite Aug 31 statement!)
        now_utc = datetime.now(timezone.utc)
        rfq1_id = str(uuid.uuid4())
        rfq2_id = str(uuid.uuid4())
        created_rfq_ids.extend([rfq1_id, rfq2_id])

        rfq1_ref = f"RFQ-SLIP-{uid}"
        rfq2_ref = f"RFQ-MISS-{uid}"

        rfq1 = QuotationRequest(
            id=rfq1_id,
            ref_no=rfq1_ref,
            customer_id=fx.cust_alpha.id,
            created_by_user_id=fx.admin_user.id,
            type="FX_SPOT",
            direction="Buy",
            value_date="2026-08-10",
            amount=100000.0,
            buy_currency="USD",
            sell_currency="EGP",
            window_start=now_utc,
            window_end=now_utc,
            quotation_base="Execution",
            status="COMPLETED",
            acceptance_timeout_seconds=120,
            acceptance_timeout_action="AUTO_REJECT",
            acceptance_status="ACCEPTED",
            winner_bank_id=fx.bank_cib.id,
            winner_bank_name="CIB",
        )
        rfq1._db_winner_rate = 49.50

        leg1 = QuotationLeg(
            id=str(uuid.uuid4()),
            rfq_id=rfq1_id,
            leg_index=1,
            type="FX_SPOT",
            direction="Buy",
            buy_currency="USD",
            sell_currency="EGP",
            amount=100000.0,
            value_date="2026-08-10",
            quotation_base="Execution",
            status="ACCEPTED",
            winner_bank_id=fx.bank_cib.id,
            winner_bank_name="CIB",
            execution_reference=f"EXEC-{uid}-1",
        )
        leg1._db_winner_rate = 49.50

        rfq2 = QuotationRequest(
            id=rfq2_id,
            ref_no=rfq2_ref,
            customer_id=fx.cust_alpha.id,
            created_by_user_id=fx.admin_user.id,
            type="FX_SPOT",
            direction="Buy",
            value_date="2026-08-15",
            amount=50000.0,
            buy_currency="USD",
            sell_currency="EGP",
            window_start=now_utc,
            window_end=now_utc,
            quotation_base="Execution",
            status="COMPLETED",
            acceptance_timeout_seconds=120,
            acceptance_timeout_action="AUTO_REJECT",
            acceptance_status="ACCEPTED",
            winner_bank_id=fx.bank_cib.id,
            winner_bank_name="CIB",
        )
        rfq2._db_winner_rate = 49.40
        db.add_all([rfq1, leg1, rfq2])
        db.flush()

        # 3. Seed CIB Bank Statement (2026-08-01 .. 2026-08-31) with all 5 Closed-Loop Audit Scenarios!
        stmt = BankStatement(
            company_id=fx.cust_alpha.id,
            bank_id=fx.bank_cib.id,
            account_number="CIB-OP-881",
            file_name="CIB_Aug2026_Bridge.csv",
            opening_balance=Decimal("8000000.00"),
            closing_balance=Decimal("2751000.00"),
            statement_start_date=datetime(2026, 8, 1),
            statement_end_date=datetime(2026, 8, 31),
            status="VALIDATED",
        )
        db.add(stmt)
        db.flush()

        db.add_all([
            # A) LG Commission Overcharge: Debited 22,000 vs 15,500 contractual -> +6,500.00 claimable!
            BankTransaction(
                statement_id=stmt.id,
                account_number="CIB-OP-881",
                booking_date=datetime(2026, 8, 3),
                value_date=datetime(2026, 8, 3),
                debit_amount=Decimal("22000.00"),
                credit_amount=Decimal("0.00"),
                currency="EGP",
                running_balance=Decimal("7978000.00"),
                raw_description=f"LG ISSUANCE COMMISSION REF {lg_ref}",
            ),
            # B) Excess Cash Margin Hold: Debited 250,000 (25%) vs 100,000 (10%) -> +150,000.00 excess held!
            BankTransaction(
                statement_id=stmt.id,
                account_number="CIB-OP-881",
                booking_date=datetime(2026, 8, 3),
                value_date=datetime(2026, 8, 3),
                debit_amount=Decimal("250000.00"),
                credit_amount=Decimal("0.00"),
                currency="EGP",
                running_balance=Decimal("7728000.00"),
                raw_description=f"LG CASH MARGIN HOLD 25PCT REF {lg_ref}",
            ),
            # C) Cash Margin Partial Release (+100,000)
            BankTransaction(
                statement_id=stmt.id,
                account_number="CIB-OP-881",
                booking_date=datetime(2026, 8, 25),
                value_date=datetime(2026, 8, 25),
                debit_amount=Decimal("0.00"),
                credit_amount=Decimal("100000.00"),
                currency="EGP",
                running_balance=Decimal("7828000.00"),
                raw_description=f"RELEASE REFUND LG CASH MARGIN REF {lg_ref}",
            ),
            # D) Unregistered Shadow LG Debit (-12,000) with formula-injection attempt in description
            BankTransaction(
                statement_id=stmt.id,
                account_number="CIB-OP-881",
                booking_date=datetime(2026, 8, 19),
                value_date=datetime(2026, 8, 19),
                debit_amount=Decimal("12000.00"),
                credit_amount=Decimal("0.00"),
                currency="EGP",
                running_balance=Decimal("7816000.00"),
                raw_description="=SUM(1+1) BANK GUARANTEE COMMISSION LG-SHADOW-9941",
            ),
            # E) FX Deal 1 Settlement with Rate Slippage: Debited 4,965,000 vs 4,950,000 agreed -> +15,000.00 claimable!
            BankTransaction(
                statement_id=stmt.id,
                account_number="CIB-OP-881",
                booking_date=datetime(2026, 8, 10),
                value_date=datetime(2026, 8, 10),
                debit_amount=Decimal("4965000.00"),
                credit_amount=Decimal("0.00"),
                currency="EGP",
                running_balance=Decimal("2851000.00"),
                raw_description=f"FX SPOT SETTLEMENT EXEC-{uid}-1 USD BUY",
            ),
        ])
        db.commit()

        # 4. Call GET /api/v1/reconciliation/treasury-bridge
        r = client.get("/api/v1/reconciliation/treasury-bridge?currency=EGP", headers=admin_h)
        assert r.status_code == 200, r.text
        data = r.json()
        kpis = data["summary_kpis"]

        # Verify exact claimable recovery: 6,500 (LG Comm) + 150,000 (LG Excess Margin) + 15,000 (FX Slippage) = 171,500.00 EGP!
        assert Decimal(str(kpis["total_claimable_bank_recovery"])) == Decimal("171500.00"), kpis
        assert kpis["lg_tariff_or_margin_breaches"] == 2
        assert kpis["shadow_unregistered_lgs_count"] == 1
        assert kpis["rfq_settlement_exceptions"] == 2  # 1 slippage + 1 overdue unsettled!

        lg_statuses = {row["audit_status"] for row in data["lg_facility_audit"]}
        assert "COMMISSION_OVERCHARGE_CLAIM" in lg_statuses
        assert "EXCESS_CASH_MARGIN_HELD" in lg_statuses
        assert "CASH_MARGIN_RELEASED" in lg_statuses

        rfq_statuses = {row["ref_no"]: row["settlement_status"] for row in data["quotation_settlement_audit"]}
        assert rfq_statuses[rfq1_ref] == "SETTLED_RATE_OR_FEE_DISCREPANCY"
        assert rfq_statuses[rfq2_ref] == "OVERDUE_UNSETTLED_DEAL"

        # Cross-tenant isolation check (Beta sees 0 claimables & 0 shadow LGs)
        r_beta = client.get("/api/v1/reconciliation/treasury-bridge?currency=EGP", headers=beta_h)
        assert r_beta.status_code == 200
        assert r_beta.json()["summary_kpis"]["total_claimable_bank_recovery"] == 0.0
        assert r_beta.json()["summary_kpis"]["shadow_unregistered_lgs_count"] == 0

        # 5. Export Bank Claim Pack PDF via WeasyPrint & verify rendered text
        r_pdf = client.get("/api/v1/reconciliation/treasury-bridge/export?format=PDF&currency=EGP", headers=admin_h)
        assert r_pdf.status_code == 200, r_pdf.text
        assert r_pdf.content.startswith(b"%PDF")
        pdf_info = extract_pdf_info(r_pdf.content)
        assert "171,500.00" in pdf_info["text"]
        assert "LG-SHADOW-9941" in pdf_info["text"]

        # 6. Export 3-Sheet Audit Excel Workbook & verify formula-injection neutralization
        r_xlsx = client.get("/api/v1/reconciliation/treasury-bridge/export?format=XLSX&currency=EGP", headers=admin_h)
        assert r_xlsx.status_code == 200
        df_sh = pd.read_excel(io.BytesIO(r_xlsx.content), sheet_name="2_Shadow_Unregistered_LGs")
        raw_cell = str(df_sh.iloc[0]["raw_description"])
        assert raw_cell.startswith("'=SUM"), f"Formula injection must be neutralized, got: {raw_cell}"
        print("[PASS] test_phase4_postgres_lg_tariff_and_rfq_settlement_e2e passed")
    finally:
        for rid in created_rfq_ids:
            try:
                db.execute(text("DELETE FROM quotation_legs WHERE rfq_id = :rid"), {"rid": rid})
                db.execute(text("DELETE FROM quotation_rfqs WHERE id = :rid"), {"rid": rid})
                db.commit()
            except Exception:
                db.rollback()
        if created_lg_id:
            try:
                db.execute(text("DELETE FROM issued_lg_records WHERE id = :lid"), {"lid": created_lg_id})
                db.commit()
            except Exception:
                db.rollback()
        if created_fac_id:
            try:
                db.execute(text("DELETE FROM issuance_facility_sub_limits WHERE facility_id = :fid"), {"fid": created_fac_id})
                db.execute(text("DELETE FROM facilities WHERE id = :fid"), {"fid": created_fac_id})
                db.commit()
            except Exception:
                db.rollback()
        fx.cleanup()
        db.close()


if __name__ == "__main__":
    test_sqlite_shadow_lg_and_idle_cash_rfq_resilience()
    test_phase4_postgres_lg_tariff_and_rfq_settlement_e2e()
    print("ALL_PHASE4_CLOSED_LOOP_TREASURY_BRIDGE_TESTS_PASSED")
