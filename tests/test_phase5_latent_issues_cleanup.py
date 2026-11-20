# tests/test_phase5_latent_issues_cleanup.py
"""
Phase 5 Pre-Existing Latent Issues Cleanup Verification Suite:
1. FastAPI route uniqueness (0 duplicate method+path) & migration route order (/staged/delete-all before /staged/{record_id})
2. Full SQLAlchemy Base.metadata vs live PostgreSQL schema parity (incl. external_request_tokens.is_deleted & deleted_at)
3. Live DELETE /api/v1/corporate-admin/migration/staged/delete-all
4. Live GET /api/v1/corporate-admin/banks
5. POST /api/v1/public-issuance/issued-lgs/{lg_id}/maintenance ownership (403 unlinked & wrong requestor, 200 owner)
6. POST /corporate-admin/email-settings/{id}/restore (CustomerEmailSetting imported) & POST /system-owner/customers/{id}/restore (multi-entity single-plan limit without client_host UnboundLocalError)
7. issuance/base._apply_admin_change FORM_CONFIG_UPDATE verification_policy two-way sync to CustomerConfiguration
"""

import json
import os
import sys
import urllib.parse
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import inspect as sa_inspect, text

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)

from _test_harness import (  # noqa: E402
    IsolatedTenantFixture,
    SessionLocal,
    app,
    install_email_capture,
    install_in_memory_gcs,
    make_auth_headers,
    purge_test_customers_by_ids,
)
from app.database import Base, engine  # noqa: E402
from app.constants import GlobalConfigKey, SubscriptionStatus  # noqa: E402
from app.core.encryption import encrypt_data  # noqa: E402
from app.crud.crud_config import crud_customer_configuration  # noqa: E402
from app.models.models import (  # noqa: E402
    Customer,
    CustomerEmailSetting,
    CustomerEntity,
    ExternalRequestToken,
    LGMigrationStaging,
    SubscriptionPlan,
)
from app.models.models_issuance import (  # noqa: E402
    AdminChangeRequest,
    CustomerFormConfiguration,
    IssuanceMaintenanceAction,
    IssuanceRequest,
    IssuedLGRecord,
)
from app.schemas.migration_schemas import MigrationRecordStatusEnum, MigrationTypeEnum  # noqa: E402
from app.api.v1.endpoints.issuance.base import _apply_admin_change  # noqa: E402


def run_all_tests():
    install_in_memory_gcs()
    install_email_capture()
    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="P5_CLEAN")
    client = TestClient(app)

    temp_cust_ids = []
    temp_plan_ids = []

    try:
        today = date.today()
        admin_h = make_auth_headers(fx.admin_user)
        so_h = make_auth_headers(fx.sys_owner)

        # ------------------------------------------------------------------
        # 1. Zero Duplicate Routes & Migration Route Ordering
        # ------------------------------------------------------------------
        print("[1/7] Checking route uniqueness & /staged/delete-all precedence...")
        seen = defaultdict(list)
        route_order = []
        for idx, route in enumerate(app.routes):
            methods = getattr(route, "methods", None)
            path = getattr(route, "path", None)
            if methods and path:
                for m in sorted(methods):
                    if m in ("HEAD", "OPTIONS"):
                        continue
                    ep_name = getattr(route.endpoint, "__name__", str(route.endpoint))
                    seen[(m, path)].append(ep_name)
                    route_order.append((idx, m, path))

        dups = {k: v for k, v in seen.items() if len(v) > 1}
        assert not dups, f"Duplicate FastAPI routes detected: {dups}"

        idx_del_all = next(
            (i for i, m, p in route_order if m == "DELETE" and p == "/api/v1/corporate-admin/migration/staged/delete-all"),
            None,
        )
        idx_del_one = next(
            (i for i, m, p in route_order if m == "DELETE" and p == "/api/v1/corporate-admin/migration/staged/{record_id}"),
            None,
        )
        assert idx_del_all is not None and idx_del_one is not None
        assert idx_del_all < idx_del_one, f"Expected delete-all ({idx_del_all}) before {{record_id}} ({idx_del_one})"

        # ------------------------------------------------------------------
        # 2. Full ORM vs PostgreSQL Schema Parity & ExternalRequestToken
        # ------------------------------------------------------------------
        print("[2/7] Verifying ORM-vs-PostgreSQL schema parity...")
        insp = sa_inspect(engine)
        db_tables = set(insp.get_table_names())
        missing_tables = []
        missing_cols = []
        for tname, table in sorted(Base.metadata.tables.items()):
            if tname not in db_tables:
                missing_tables.append(tname)
                continue
            db_cols = {c["name"] for c in insp.get_columns(tname)}
            for col in table.columns:
                if col.name not in db_cols:
                    missing_cols.append((tname, col.name))

        assert not missing_tables, f"Missing DB tables: {missing_tables}"
        assert not missing_cols, f"Missing DB columns: {missing_cols}"

        ext_tok = ExternalRequestToken(
            token=f"EXT-P5-{fx.uid}",
            recipient_email=f"ext.{fx.uid.lower()}@globex.com",
            customer_id=fx.cust_alpha.id,
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
            is_used=False,
            sender_user_id=fx.maker_user.id,
        )
        db.add(ext_tok)
        db.commit()
        found_tok = (
            db.query(ExternalRequestToken)
            .filter(
                ExternalRequestToken.token == f"EXT-P5-{fx.uid}",
                ExternalRequestToken.is_deleted == False,
            )
            .first()
        )
        assert found_tok is not None and found_tok.deleted_at is None
        db.commit()

        # ------------------------------------------------------------------
        # 3. Migration DELETE /staged/delete-all
        # ------------------------------------------------------------------
        print("[3/7] Testing DELETE /api/v1/corporate-admin/migration/staged/delete-all...")
        stg = LGMigrationStaging(
            file_name=f"p5_staged_{fx.uid}.xlsx",
            record_status=MigrationRecordStatusEnum.PENDING,
            customer_id=fx.cust_alpha.id,
            migration_type=MigrationTypeEnum.RECORD,
            source_data_json={"lg_number": f"STG-{fx.uid}"},
        )
        db.add(stg)
        db.commit()

        r_del_all = client.delete("/api/v1/corporate-admin/migration/staged/delete-all", headers=admin_h)
        assert r_del_all.status_code == 200, r_del_all.text
        assert r_del_all.json()["deleted_count"] >= 1
        remaining = db.query(LGMigrationStaging).filter(LGMigrationStaging.customer_id == fx.cust_alpha.id).count()
        assert remaining == 0
        db.commit()

        # ------------------------------------------------------------------
        # 4. GET /api/v1/corporate-admin/banks
        # ------------------------------------------------------------------
        print("[4/7] Testing GET /api/v1/corporate-admin/banks...")
        r_banks = client.get("/api/v1/corporate-admin/banks?skip=0&limit=50", headers=admin_h)
        assert r_banks.status_code == 200, r_banks.text
        banks_json = r_banks.json()
        assert isinstance(banks_json, list) and len(banks_json) >= 1

        # ------------------------------------------------------------------
        # 5. Public Portal Maintenance Ownership & Execution
        # ------------------------------------------------------------------
        print("[5/7] Testing POST /public-issuance/issued-lgs/{lg_id}/maintenance...")
        unlinked_lg = IssuedLGRecord(
            lg_ref_number=f"ISS-UNLINKED-{fx.uid}",
            internal_serial=f"2026-P5U-{fx.uid}",
            bank_lg_number=f"CIB-P5U-{fx.uid}",
            customer_id=fx.cust_alpha.id,
            bank_id=fx.bank_cib.id,
            issuing_entity_id=fx.entity_alpha_hq.id,
            beneficiary_name="Unlinked Beneficiary",
            current_amount=Decimal("100000.00"),
            currency_id=fx.ccy_egp.id,
            issue_date=today - timedelta(days=10),
            expiry_date=today + timedelta(days=90),
            status="ACTIVE",
            issued_by_user_id=fx.maker_user.id,
            request_id=None,
        )
        req_owner = IssuanceRequest(
            customer_id=fx.cust_alpha.id,
            serial_number=f"REQ-P5-{fx.uid}",
            status="ISSUED",
            issuing_entity_id=fx.entity_alpha_hq.id,
            requestor_name="Portal Requestor",
            requestor_email=f"portal.owner.{fx.uid.lower()}@globex.com",
            beneficiary_name="Portal Beneficiary",
            amount=Decimal("150000.00"),
            currency_id=fx.ccy_egp.id,
            lg_type_id=fx.lg_type_perf.id,
        )
        db.add_all([unlinked_lg, req_owner])
        db.flush()

        linked_lg = IssuedLGRecord(
            lg_ref_number=f"ISS-LINKED-{fx.uid}",
            internal_serial=f"2026-P5L-{fx.uid}",
            bank_lg_number=f"CIB-P5L-{fx.uid}",
            customer_id=fx.cust_alpha.id,
            bank_id=fx.bank_cib.id,
            issuing_entity_id=fx.entity_alpha_hq.id,
            beneficiary_name="Portal Beneficiary",
            current_amount=Decimal("150000.00"),
            currency_id=fx.ccy_egp.id,
            issue_date=today - timedelta(days=10),
            expiry_date=today + timedelta(days=90),
            status="ACTIVE",
            issued_by_user_id=fx.maker_user.id,
            request_id=req_owner.id,
        )
        db.add(linked_lg)
        db.commit()

        exp_iso = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        owner_tok = urllib.parse.quote(
            encrypt_data(f"{fx.cust_alpha.id}|PublicDiscovery|portal.owner.{fx.uid.lower()}@globex.com|{exp_iso}")
        )
        other_tok = urllib.parse.quote(
            encrypt_data(f"{fx.cust_alpha.id}|PublicDiscovery|other.user.{fx.uid.lower()}@globex.com|{exp_iso}")
        )

        ext_payload = {
            "action_type": "EXTEND",
            "action_data": {"new_expiry_date": (today + timedelta(days=180)).isoformat()},
            "notes": "Phase 5 portal extension",
        }
        assert client.post(
            f"/api/v1/public-issuance/issued-lgs/{unlinked_lg.id}/maintenance?token={owner_tok}",
            json=ext_payload,
        ).status_code == 403
        assert client.post(
            f"/api/v1/public-issuance/issued-lgs/{linked_lg.id}/maintenance?token={other_tok}",
            json=ext_payload,
        ).status_code == 403

        r_portal_ok = client.post(
            f"/api/v1/public-issuance/issued-lgs/{linked_lg.id}/maintenance?token={owner_tok}",
            json=ext_payload,
        )
        assert r_portal_ok.status_code == 200, r_portal_ok.text
        portal_json = r_portal_ok.json()
        assert portal_json["action_type"] == "EXTEND" and portal_json.get("id") and portal_json.get("created_at")

        # ------------------------------------------------------------------
        # 6. Email Settings Restore & System Owner Customer Restore
        # ------------------------------------------------------------------
        print("[6/7] Testing CustomerEmailSetting restore & single-entity Customer restore...")
        em_setting = CustomerEmailSetting(
            customer_id=fx.cust_alpha.id,
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_username="mailer",
            smtp_password_encrypted="enc-dummy",
            sender_email=f"noreply.{fx.uid.lower()}@globex.com",
            is_active=True,
            is_deleted=True,
            deleted_at=datetime.now(timezone.utc),
        )
        db.add(em_setting)
        db.commit()

        r_em = client.post(
            f"/api/v1/corporate-admin/email-settings/{em_setting.id}/restore",
            headers=admin_h,
        )
        assert r_em.status_code == 200, r_em.text
        db.expire(em_setting)
        assert db.get(CustomerEmailSetting, em_setting.id).is_deleted is False
        db.commit()

        single_plan = SubscriptionPlan(
            name=f"Single Entity Plan P5 {fx.uid}",
            duration_months=12,
            monthly_price=100.0,
            annual_price=1000.0,
            max_users=5,
            max_checker_users=1,
            max_records=10,
            max_issuance_records=10,
            can_multi_entity=False,
        )
        db.add(single_plan)
        db.flush()
        temp_plan_ids.append(single_plan.id)

        temp_cust = Customer(
            name=f"P5 Restore Single Corp {fx.uid}",
            address="Cairo",
            contact_email=f"restore.{fx.uid.lower()}@globex.com",
            subscription_plan_id=single_plan.id,
            start_date=datetime.now(timezone.utc) - timedelta(days=5),
            end_date=datetime.now(timezone.utc) + timedelta(days=360),
            status=SubscriptionStatus.ACTIVE,
            is_deleted=True,
            deleted_at=datetime.now(timezone.utc),
        )
        db.add(temp_cust)
        db.flush()
        temp_cust_ids.append(temp_cust.id)

        ent1 = CustomerEntity(
            customer_id=temp_cust.id,
            entity_name="Primary Entity",
            code=f"P{fx.uid[:3]}",
            is_active=True,
            is_deleted=True,
            deleted_at=datetime.now(timezone.utc),
        )
        ent2 = CustomerEntity(
            customer_id=temp_cust.id,
            entity_name="Secondary Entity",
            code=f"S{fx.uid[:3]}",
            is_active=True,
            is_deleted=True,
            deleted_at=datetime.now(timezone.utc),
        )
        db.add_all([ent1, ent2])
        db.commit()

        r_cust_res = client.post(
            f"/api/v1/system-owner/customers/{temp_cust.id}/restore",
            headers=so_h,
        )
        assert r_cust_res.status_code == 200, r_cust_res.text
        db.expire_all()
        restored_ents = (
            db.query(CustomerEntity)
            .filter(CustomerEntity.customer_id == temp_cust.id, CustomerEntity.is_deleted == False)
            .all()
        )
        assert len(restored_ents) == 2
        assert sum(1 for e in restored_ents if e.is_active) == 1
        assert sum(1 for e in restored_ents if not e.is_active) == 1
        db.commit()

        # ------------------------------------------------------------------
        # 7. _apply_admin_change FORM_CONFIG_UPDATE Two-Way Sync
        # ------------------------------------------------------------------
        print("[7/7] Testing _apply_admin_change FORM_CONFIG_UPDATE verification_policy two-way sync...")
        new_policy = {
            "enforcement_mode": "STRICT",
            "expiry_date_tolerance_days": 1,
            "beneficiary_match_pct": 95,
            "issuer_match_pct": 95,
            "verify_issuing_bank": True,
            "verify_issuer_name": True,
            "block_issuance_without_scan": True,
        }
        change_req = AdminChangeRequest(
            customer_id=fx.cust_alpha.id,
            requested_by_user_id=fx.admin_user.id,
            approved_by_user_id=fx.admin2_user.id,
            change_type="FORM_CONFIG_UPDATE",
            change_payload={"new_value": {"verification_policy": new_policy}},
            status="APPROVED",
        )
        db.add(change_req)
        db.flush()
        _apply_admin_change(db, change_req)
        db.commit()

        form_cfg = (
            db.query(CustomerFormConfiguration)
            .filter(CustomerFormConfiguration.customer_id == fx.cust_alpha.id)
            .first()
        )
        assert form_cfg and form_cfg.verification_policy == new_policy

        cfg_out = crud_customer_configuration.get_customer_config_or_global_fallback(
            db, fx.cust_alpha.id, GlobalConfigKey.ISSUED_LG_VERIFICATION_POLICY
        )
        assert cfg_out and cfg_out.get("effective_value"), f"Expected synced config, got: {cfg_out}"
        assert json.loads(cfg_out["effective_value"]) == new_policy

        # ------------------------------------------------------------------
        # 8. Action Center Unified Pending Bank Reply & Multipart Delivery
        # ------------------------------------------------------------------
        print("[8/8] Testing multipart issuance/maintenance delivery & unified-pending-bank-reply...")
        maker_h = make_auth_headers(fx.maker_user)
        lg_deliv = IssuedLGRecord(
            lg_ref_number=f"ISS-DELIV-{fx.uid}",
            internal_serial=f"2026-P5D-{fx.uid}",
            bank_lg_number=f"CIB-P5D-{fx.uid}",
            customer_id=fx.cust_alpha.id,
            bank_id=fx.bank_cib.id,
            issuing_entity_id=fx.entity_alpha_hq.id,
            beneficiary_name="Delivery Test Beneficiary",
            current_amount=Decimal("200000.00"),
            currency_id=fx.ccy_egp.id,
            issue_date=today - timedelta(days=5),
            expiry_date=today + timedelta(days=120),
            status="INTERNAL_PROCESSING",
            issued_by_user_id=fx.maker_user.id,
        )
        db.add(lg_deliv)
        db.flush()

        maint_deliv = IssuanceMaintenanceAction(
            issued_lg_id=lg_deliv.id,
            action_type="EXTEND",
            action_data={"new_expiry_date": (today + timedelta(days=180)).isoformat()},
            status="EXECUTED",
            instruction_status="Instruction Issued",
            letter_serial_number=f"MNT-DEL-{fx.uid}",
            initiated_by_user_id=fx.maker_user.id,
            is_deleted=False,
        )
        db.add(maint_deliv)
        db.commit()

        dummy_pdf = ("proof.pdf", b"%PDF-1.4\n%%EOF\n", "application/pdf")
        r_lg_del = client.post(
            f"/api/v1/issuance/lg-records/{lg_deliv.id}/record-delivery",
            headers=maker_h,
            data={"delivery_date": today.isoformat(), "delivery_method": "HAND_DELIVERY"},
            files={"delivery_document_file": dummy_pdf},
        )
        assert r_lg_del.status_code == 200, r_lg_del.text
        assert r_lg_del.json()["status"] == "DELIVERED_TO_BANK"
        assert r_lg_del.json()["delivery_date"] == today.isoformat()

        r_mnt_del = client.post(
            f"/api/v1/issuance/maintenance/{maint_deliv.id}/delivery",
            headers=maker_h,
            data={"delivery_date": today.isoformat()},
            files={"delivery_document_file": dummy_pdf},
        )
        assert r_mnt_del.status_code == 200, r_mnt_del.text
        assert r_mnt_del.json()["instruction_status"] == "Instruction Delivered"
        assert r_mnt_del.json()["delivery_date"] is not None

        r_unified = client.get("/api/v1/issuance/action-center/unified-pending-bank-reply", headers=maker_h)
        assert r_unified.status_code == 200, r_unified.text
        rows_unified = r_unified.json()
        lg_row = next((x for x in rows_unified if x.get("source") == "issuance" and x.get("id") == lg_deliv.id), None)
        mnt_row = next((x for x in rows_unified if x.get("source") == "maintenance" and x.get("id") == maint_deliv.id), None)
        assert lg_row is not None and lg_row.get("created_at"), f"Missing initial LG row: {rows_unified}"
        assert mnt_row is not None and mnt_row.get("issued_lg_id") == lg_deliv.id and mnt_row.get("created_at"), f"Missing maint row fields: {rows_unified}"

        print("\nALL PHASE 5 LATENT ISSUES CLEANUP CHECKS PASSED!")

    finally:
        try:
            db.rollback()
            db.execute(text("DELETE FROM external_request_tokens WHERE customer_id = :cid"), {"cid": fx.cust_alpha.id})
            db.commit()
        except Exception:
            db.rollback()

        if temp_cust_ids or temp_plan_ids:
            purge_test_customers_by_ids(db, cust_ids=temp_cust_ids, plan_ids=temp_plan_ids)

        fx.cleanup()
        db.close()


if __name__ == "__main__":
    run_all_tests()
