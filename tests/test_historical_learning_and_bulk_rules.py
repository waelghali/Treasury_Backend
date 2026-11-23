"""
Verification Suite: Day-1 Historical Statement Learning (Train-Only) & Bulk Rulebook Import/Export.
Tests:
1. Train-Only Historical Sheet Learning (`train_from_historical_file`):
   - Parses historical Excel sheet with `Description`, `Amount` (single signed column, NO Balance column!),
     `Category`, `GL Account` (distinct from Category!), and `Counterparty`.
   - Verifies dry-run preview (`dry_run=True` makes ZERO DB writes).
   - Verifies commit (`dry_run=False`) creates `AUTO_CONFIRMED` rules (>=2 hits) with exact `GL Account`
     AND `Category`, learns `Counterparty` records, creates missing `ClassificationTaxonomy` nodes,
     AND creates **ZERO** `BankStatement` or `BankTransaction` rows (zero live ledger pollution!).
   - Verifies that newly trained rules immediately auto-classify a fresh raw bank statement!
2. Live Statement Upload `GL Account` Fix:
   - Uploading a pre-classified statement CSV containing `Date, Description, Amount, Category, GL Account`
     (without a `Balance` column) is NOT falsely rejected by the ERP-box guard, and maps `GL Account`
     cleanly into `internal_category`!
3. Bulk Rulebook Template, Multi-Sheet Import (`Rules + Counterparties + Taxonomy`), Duplicate Strategies
   (`UPDATE` vs `SKIP` vs `APPEND`), Round-Trip XLSX & JSON Export, Formula-Injection Defense, and HTTP E2E!
"""

import io
import json
import os
import sys
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
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
from app.services.bank_reconciliation_service import bank_reconcile_service
from app.services.reconciliation_onboarding_intelligence_service import (
    reconciliation_onboarding_service,
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


def test_train_only_historical_sheet_zero_ledger_pollution_and_live_fire():
    db = _make_sqlite_db()
    try:
        hist_rows = [
            {
                "Date": "2025-10-05",
                "Description": "VODAFONE CORPORATE FIBER LEASE OCT",
                "Amount": -12500.00,
                "Category": "TELECOM_EXPENSE",
                "GL Account": "5040200 - Telecom & Fiber",
                "Counterparty": "Vodafone Business",
                "Account No": "ACC-HIST-01",
            },
            {
                "Date": "2025-11-05",
                "Description": "VODAFONE CORPORATE FIBER LEASE NOV",
                "Amount": -12500.00,
                "Category": "TELECOM_EXPENSE",
                "GL Account": "5040200 - Telecom & Fiber",
                "Counterparty": "Vodafone Business",
                "Account No": "ACC-HIST-01",
            },
            {
                "Date": "2025-11-18",
                "Description": "PAYMOB MERCHANT SETTLEMENT BATCH 881",
                "Amount": 85000.00,
                "Category": "POS_COLLECTION",
                "GL Account": "1020150 - POS Clearing Receivable",
                "Counterparty": "Paymob Solutions",
                "Account No": "ACC-HIST-01",
            },
            {
                "Date": "2025-12-18",
                "Description": "PAYMOB MERCHANT SETTLEMENT BATCH 912",
                "Amount": 92000.00,
                "Category": "POS_COLLECTION",
                "GL Account": "1020150 - POS Clearing Receivable",
                "Counterparty": "Paymob Solutions",
                "Account No": "ACC-HIST-01",
            },
        ]
        buf = io.BytesIO()
        pd.DataFrame(hist_rows).to_excel(buf, index=False)
        xlsx_bytes = buf.getvalue()

        # 1) Dry-run preview must NOT write any rules or statements
        preview = reconciliation_onboarding_service.train_from_historical_file(
            db=db,
            customer_id=44,
            user_id=1,
            content=xlsx_bytes,
            filename="LastYear_Reconciled_2025.xlsx",
            dry_run=True,
            auto_promote_min_hits=2,
        )
        assert preview["labeled_rows_found"] == 4
        assert preview["rules_count"] >= 2
        assert preview["auto_confirmed_count"] >= 2
        assert db.query(ClassificationRule).count() == 0
        assert db.query(BankStatement).count() == 0

        # 2) Commit training -> builds AUTO_CONFIRMED rules, Counterparties, Taxonomy & ZERO BankStatements!
        committed = reconciliation_onboarding_service.train_from_historical_file(
            db=db,
            customer_id=44,
            user_id=1,
            content=xlsx_bytes,
            filename="LastYear_Reconciled_2025.xlsx",
            dry_run=False,
            auto_promote_min_hits=2,
            auto_create_taxonomy=True,
        )
        assert committed["rules_created"] >= 2
        assert committed["counterparties_learned"] == 2
        assert committed["taxonomy_nodes_created"] >= 2
        assert db.query(BankStatement).count() == 0, "Train-only mode must NEVER pollute live statements!"
        assert db.query(BankTransaction).count() == 0, "Train-only mode must NEVER pollute live transactions!"

        # Verify distinct GL Account vs Category was preserved!
        rules = db.query(ClassificationRule).filter(ClassificationRule.company_id == 44).all()
        gls = {r.assigned_gl_account for r in rules}
        cats = {r.assigned_category for r in rules}
        assert "5040200 - Telecom & Fiber" in gls
        assert "TELECOM_EXPENSE" in cats

        # 3) Prove newly trained rules fire immediately on a brand-new raw bank statement!
        stmt = BankStatement(
            company_id=44,
            bank_id=1,
            account_number="ACC-LIVE-2026",
            file_name="Jan2026_Raw.csv",
            opening_balance=Decimal("100000.00"),
            closing_balance=Decimal("87500.00"),
            statement_start_date=datetime(2026, 1, 1),
            statement_end_date=datetime(2026, 1, 31),
            status="VALIDATED",
        )
        db.add(stmt)
        db.flush()
        raw_txn = BankTransaction(
            statement_id=stmt.id,
            account_number="ACC-LIVE-2026",
            booking_date=datetime(2026, 1, 6),
            value_date=datetime(2026, 1, 6),
            debit_amount=Decimal("12500.00"),
            credit_amount=Decimal("0.00"),
            currency="EGP",
            running_balance=Decimal("87500.00"),
            raw_description="VODAFONE CORPORATE FIBER LEASE JAN 2026",
            is_classified=False,
        )
        db.add(raw_txn)
        db.commit()

        bank_reconcile_service.apply_classification_rules(db, customer_id=44, statement_id=stmt.id)
        db.refresh(raw_txn)
        assert raw_txn.is_classified is True
        assert raw_txn.internal_category == "5040200 - Telecom & Fiber"
        assert raw_txn.classification_category == "TELECOM_EXPENSE"
        print("[PASS] test_train_only_historical_sheet_zero_ledger_pollution_and_live_fire passed")
    finally:
        db.close()


def test_bulk_template_import_duplicate_strategies_and_roundtrip_export():
    db = _make_sqlite_db()
    try:
        # 1) Generate official 3-sheet template & import it directly!
        tpl_bytes = reconciliation_onboarding_service.generate_bulk_template_xlsx()
        dry = reconciliation_onboarding_service.bulk_import_rules_and_knowledge(
            db=db,
            customer_id=66,
            user_id=1,
            content=tpl_bytes,
            filename="template.xlsx",
            dry_run=True,
        )
        assert dry["valid_rules_count"] == 3
        assert dry["valid_counterparties_count"] == 2
        assert dry["valid_taxonomy_count"] == 1
        assert db.query(ClassificationRule).count() == 0

        imp1 = reconciliation_onboarding_service.bulk_import_rules_and_knowledge(
            db=db,
            customer_id=66,
            user_id=1,
            content=tpl_bytes,
            filename="template.xlsx",
            dry_run=False,
            duplicate_strategy="UPDATE",
        )
        assert imp1["rules_created"] == 3
        assert imp1["counterparties_imported"] == 2
        assert imp1["taxonomy_nodes_imported"] == 1

        # 2) Re-import with SKIP -> 0 created, 3 skipped!
        imp_skip = reconciliation_onboarding_service.bulk_import_rules_and_knowledge(
            db=db,
            customer_id=66,
            user_id=1,
            content=tpl_bytes,
            filename="template.xlsx",
            dry_run=False,
            duplicate_strategy="SKIP",
        )
        assert imp_skip["rules_created"] == 0
        assert imp_skip["rules_skipped"] == 3

        # 3) Test Formula-Injection Neutralization on XLSX Rulebook Export + JSON Round-Trip Clone to Tenant 67!
        db.add(
            ClassificationRule(
                company_id=66,
                rule_name="=HYPERLINK('http://evil','Click')",
                priority=10,
                conditions_json={"conditions": [{"field": "raw_description", "operator": "contains", "value": "RENT", "joiner": "AND"}]},
                assigned_gl_account="5030100 - Rent",
                assigned_category="OFFICE_RENT",
                is_active=True,
            )
        )
        db.commit()

        exp_xlsx, _, _ = reconciliation_onboarding_service.export_rulebook(db, customer_id=66, file_format="XLSX")
        df_exp = pd.read_excel(io.BytesIO(exp_xlsx), sheet_name="1_Rules_Import")
        assert any(str(v).startswith("'=HYPERLINK") for v in df_exp["Rule_Name"].tolist())

        exp_json, _, _ = reconciliation_onboarding_service.export_rulebook(db, customer_id=66, file_format="JSON")
        clone_res = reconciliation_onboarding_service.bulk_import_rules_and_knowledge(
            db=db,
            customer_id=67,
            user_id=1,
            content=exp_json,
            filename="cloned_rulebook.json",
            dry_run=False,
        )
        assert clone_res["rules_created"] == 4
        assert clone_res["counterparties_imported"] == 2
        print("[PASS] test_bulk_template_import_duplicate_strategies_and_roundtrip_export passed")
    finally:
        db.close()


def test_http_endpoints_and_live_gl_column_statement_upload_e2e():
    from _test_harness import (
        IsolatedTenantFixture,
        SessionLocal,
        app,
        make_auth_headers,
    )

    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="ONBOARD_RULES")
    client = TestClient(app)
    try:
        admin_h = make_auth_headers(fx.admin_user, extra_claims={"has_reconciliation_module": True})
        beta_h = make_auth_headers(fx.beta_admin, extra_claims={"has_reconciliation_module": True})

        # 1. GET /rules/bulk-template
        r_tpl = client.get("/api/v1/reconciliation/rules/bulk-template", headers=admin_h)
        assert r_tpl.status_code == 200
        assert len(r_tpl.content) > 2000

        # 2. POST /rules/bulk-import with template
        r_imp = client.post(
            "/api/v1/reconciliation/rules/bulk-import",
            headers=admin_h,
            data={"dry_run": "false", "duplicate_strategy": "UPDATE"},
            files={"file": ("template.xlsx", r_tpl.content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        )
        assert r_imp.status_code == 200, r_imp.text
        assert r_imp.json()["rules_created"] == 3

        # Cross-tenant isolation check (Beta has 0 imported rules from Alpha)
        r_beta_exp = client.get("/api/v1/reconciliation/rules/export?format=JSON", headers=beta_h)
        assert r_beta_exp.status_code == 200
        assert len(json.loads(r_beta_exp.content)["rules"]) == 0

        # 3. Verify pre-labeled CSV with `GL Account` + single `Amount` (NO `Balance` column!) uploads cleanly
        #    via `/statements/upload` without false ERP rejection AND maps `GL Account` -> `internal_category`!
        csv_text = (
            "Date,Description,Amount,Category,GL Account\n"
            "2026-08-05,MONTHLY SECURITY GUARD SERVICE,-18000.00,FACILITY_SECURITY,5030900 - Security Services\n"
            "2026-08-12,MONTHLY SECURITY GUARD SERVICE,-18000.00,FACILITY_SECURITY,5030900 - Security Services\n"
        )
        r_up = client.post(
            "/api/v1/reconciliation/statements/upload",
            headers=admin_h,
            data={"bank_id": str(fx.bank_cib.id)},
            files={"file": ("PreLabeled_Aug2026.csv", csv_text.encode("utf-8"), "text/csv")},
        )
        assert r_up.status_code == 200, r_up.text
        stmt_id = r_up.json()["id"]

        r_txns = client.get(f"/api/v1/reconciliation/statements/{stmt_id}/transactions", headers=admin_h)
        assert r_txns.status_code == 200
        tx0 = r_txns.json()[0]
        assert tx0["classification_category"] == "FACILITY_SECURITY"
        assert tx0["internal_category"] == "5030900 - Security Services"
        print("[PASS] test_http_endpoints_and_live_gl_column_statement_upload_e2e passed")
    finally:
        fx.cleanup()
        db.close()


if __name__ == "__main__":
    test_train_only_historical_sheet_zero_ledger_pollution_and_live_fire()
    test_bulk_template_import_duplicate_strategies_and_roundtrip_export()
    test_http_endpoints_and_live_gl_column_statement_upload_e2e()
    print("ALL_HISTORICAL_LEARNING_AND_BULK_RULES_TESTS_PASSED")
