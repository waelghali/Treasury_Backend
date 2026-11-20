# tests/test_lg_category_default_and_migration.py
"""
Verification suite for LG Custody Default Category & Post-Recording Category Migration:
1. Corporate Admin & System Owner Default Category management (auto-swap without 409,
   customer default > universal default precedence, customer choosing a universal category
   as default, entity-all-access guard, clear default, delete guard, Migration Hub fallback,
   and accurate lg_count).
2. Post-Recording Category Change / Migration (POST /api/v1/end-user/lg-records/change-category):
   - No approval workflow (direct update)
   - Works across ALL LG statuses (Valid, Expired, Released, Liquidated)
   - Scopes: single, multiple, all (by source_category_id & entire accessible portfolio)
   - Bidirectional extra field migration: no-field <-> mandatory/optional extra-field,
     extra-field A -> extra-field B (uniform value, per-LG map, carry_over_old_extra_field)
   - Atomic rollback on partial mandatory-field or entity-compatibility failures
   - Custody serial preserved; prior instruction serials preserved; subsequent instruction serial uses new category code
   - Restricted user entity boundary respected in 'all' scope
"""

import os
import sys
import json
from datetime import date, timedelta

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from _test_harness import (
    app,
    SessionLocal,
    IsolatedTenantFixture,
    install_in_memory_gcs,
    install_email_capture,
    make_auth_headers,
)
from app.constants import LgStatusEnum
from app.crud.crud import crud_lg_category
from app.models.models import LGRecord, LGCategory, LGInstruction, AuditLog

client = TestClient(app)


def run_lg_category_default_and_migration_suite():
    print("=" * 88)
    print("LG CUSTODY — DEFAULT CATEGORY & POST-RECORDING CATEGORY MIGRATION SUITE")
    print("=" * 88)

    install_in_memory_gcs()
    install_email_capture()
    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="CAT_MIG")

    admin_h = make_auth_headers(fx.admin_user)
    maker_h = make_auth_headers(fx.maker_user)
    restricted_h = make_auth_headers(fx.restricted_maker)
    beta_admin_h = make_auth_headers(fx.beta_admin)

    created_univ_cat_id = None

    def _base_lg_payload(lg_num, category_id, entity_id=None, extra=None):
        today = date.today()
        payload = {
            "beneficiary_corporate_id": entity_id or fx.entity_alpha_hq.id,
            "issuer_name": "Test Contractor SAE",
            "issuer_id": "CR-112233",
            "lg_number": lg_num,
            "lg_amount": "500000.00",
            "lg_currency_id": fx.ccy_egp.id,
            "lg_payable_currency_id": fx.ccy_egp.id,
            "issuance_date": (today - timedelta(days=30)).isoformat(),
            "expiry_date": (today + timedelta(days=180)).isoformat(),
            "auto_renewal": True,
            "lg_type_id": fx.lg_type_perf.id,
            "description_purpose": "Category migration test guarantee",
            "issuing_bank_id": fx.bank_cib.id,
            "issuing_bank_address": "CIB Tower",
            "issuing_bank_phone": "+20219666",
            "issuing_method_id": fx.issuing_method.id,
            "applicable_rule_id": fx.rule_urdg.id,
            "lg_category_id": category_id,
            "internal_owner_email": f"cat.owner.{fx.uid.lower()}@globex.com",
            "internal_owner_phone": "+20100112233",
            "manager_email": f"cat.mgr.{fx.uid.lower()}@globex.com",
        }
        if extra is not None:
            payload["additional_field_values"] = extra
        return payload

    def _create_lg(lg_num, category_id, entity_id=None, extra=None):
        res = client.post(
            "/api/v1/end-user/lg-records/",
            headers=maker_h,
            data={"lg_record_in": json.dumps(_base_lg_payload(lg_num, category_id, entity_id, extra))},
        )
        assert res.status_code == 201, f"Failed to create LG {lg_num}: {res.text}"
        return res.json()

    try:
        # ==============================================================================
        # PART 1: DEFAULT CATEGORY MANAGEMENT & PRECEDENCE
        # ==============================================================================
        print("\n[PART 1] Default Category Auto-Swap, Entity Guard, Universal Selection & Precedence")

        # 1.1 Create first default category
        res_c1 = client.post(
            "/api/v1/corporate-admin/lg-categories/",
            headers=admin_h,
            json={
                "name": f"Default One {fx.uid}",
                "code": "D1",
                "is_default": True,
                "has_all_entity_access": True,
            },
        )
        assert res_c1.status_code == 201, f"Create default cat 1 failed: {res_c1.text}"
        c1 = res_c1.json()
        assert c1["is_default"] is True

        # 1.2 Create second default category -> must auto-unset c1 instead of 409 Conflict!
        res_c2 = client.post(
            "/api/v1/corporate-admin/lg-categories/",
            headers=admin_h,
            json={
                "name": f"Default Two {fx.uid}",
                "code": "D2",
                "is_default": True,
                "has_all_entity_access": True,
            },
        )
        assert res_c2.status_code == 201, f"Create default cat 2 should auto-swap, got: {res_c2.text}"
        c2 = res_c2.json()
        assert c2["is_default"] is True

        res_list1 = client.get("/api/v1/corporate-admin/lg-categories/", headers=admin_h)
        assert res_list1.status_code == 200
        cats_map1 = {c["id"]: c for c in res_list1.json()}
        assert cats_map1[c1["id"]]["is_default"] is False, "Previous default c1 must be unset!"
        assert cats_map1[c2["id"]]["is_default"] is True, "New default c2 must be active!"
        print("  [PASS] Creating second is_default=True auto-unsets prior default (no 409 Conflict)")

        # 1.3 Entity-restricted category cannot be default
        res_rest_cat = client.post(
            "/api/v1/corporate-admin/lg-categories/",
            headers=admin_h,
            json={
                "name": f"HQ Only {fx.uid}",
                "code": "HQ",
                "is_default": False,
                "has_all_entity_access": False,
                "entity_ids": [fx.entity_alpha_hq.id],
            },
        )
        assert res_rest_cat.status_code == 201, res_rest_cat.text
        c_hq = res_rest_cat.json()

        res_bad_default_create = client.post(
            "/api/v1/corporate-admin/lg-categories/",
            headers=admin_h,
            json={
                "name": f"Bad Default {fx.uid}",
                "code": "BD",
                "is_default": True,
                "has_all_entity_access": False,
                "entity_ids": [fx.entity_alpha_hq.id],
            },
        )
        assert res_bad_default_create.status_code in (400, 422)

        res_bad_set_default = client.post(
            f"/api/v1/corporate-admin/lg-categories/{c_hq['id']}/set-default",
            headers=admin_h,
        )
        assert res_bad_set_default.status_code == 400
        print("  [PASS] Entity-restricted categories blocked from becoming default")

        # 1.4 Switch default back to c1 via /set-default
        res_swap = client.post(
            f"/api/v1/corporate-admin/lg-categories/{c1['id']}/set-default",
            headers=admin_h,
        )
        assert res_swap.status_code == 200 and res_swap.json()["is_default"] is True

        # Cannot delete active default category
        res_del_def = client.delete(f"/api/v1/corporate-admin/lg-categories/{c1['id']}", headers=admin_h)
        assert res_del_def.status_code == 400

        # 1.5 Precedence: Customer Default > Universal Default, and Universal selection by Customer
        univ_cat = LGCategory(
            customer_id=None,
            name=f"Univ Test {fx.uid}",
            code="UT",
            is_default=False,
            has_all_entity_access=True,
        )
        db.add(univ_cat)
        db.commit()
        db.refresh(univ_cat)
        created_univ_cat_id = univ_cat.id

        # Customer default c1 wins in end-user list and Migration Hub helper
        res_eu_cats = client.get("/api/v1/end-user/lg-categories/", headers=maker_h)
        assert res_eu_cats.status_code == 200, res_eu_cats.text
        eu_cats_map = {c["id"]: c for c in res_eu_cats.json()}
        assert eu_cats_map[c1["id"]]["is_default"] is True
        assert eu_cats_map[c2["id"]]["is_default"] is False
        assert eu_cats_map[univ_cat.id]["is_default"] is False

        db.expire_all()
        eff = crud_lg_category.get_effective_default_category(db, fx.cust_alpha.id)
        assert eff is not None and eff.id == c1["id"], f"Expected c1={c1['id']}, got {eff.id if eff else None}"

        # Corporate Admin picks Universal category as customer default
        res_set_univ = client.post(
            f"/api/v1/corporate-admin/lg-categories/{univ_cat.id}/set-default",
            headers=admin_h,
        )
        assert res_set_univ.status_code == 200 and res_set_univ.json()["is_default"] is True

        res_eu_cats2 = client.get("/api/v1/end-user/lg-categories/", headers=maker_h)
        eu_cats_map2 = {c["id"]: c for c in res_eu_cats2.json()}
        assert eu_cats_map2[univ_cat.id]["is_default"] is True
        assert eu_cats_map2[c1["id"]]["is_default"] is False

        # Tenant Beta must NOT see Tenant Alpha's universal-category default selection
        res_beta_cats = client.get("/api/v1/corporate-admin/lg-categories/", headers=beta_admin_h)
        beta_map = {c["id"]: c for c in res_beta_cats.json()}
        assert beta_map[univ_cat.id]["is_default"] is False

        # Delete old unused c2 now that it is no longer default
        res_del_c2 = client.delete(f"/api/v1/corporate-admin/lg-categories/{c2['id']}", headers=admin_h)
        assert res_del_c2.status_code == 200

        # Clear default on Tenant Alpha
        res_clear_def = client.post("/api/v1/corporate-admin/lg-categories/clear-default", headers=admin_h)
        assert res_clear_def.status_code == 200
        # Restore c1 as default
        client.post(f"/api/v1/corporate-admin/lg-categories/{c1['id']}/set-default", headers=admin_h)
        print("  [PASS] Customer Default > Universal Default, per-tenant universal default, clear & delete guards")

        # Create extra-field category B for A <-> B migration tests
        res_cb = client.post(
            "/api/v1/corporate-admin/lg-categories/",
            headers=admin_h,
            json={
                "name": f"Cost Center Group {fx.uid}",
                "code": "CB",
                "extra_field_name": "cost_center",
                "is_mandatory": True,
                "has_all_entity_access": True,
            },
        )
        assert res_cb.status_code == 201, res_cb.text
        cat_extra_b = res_cb.json()

        # ==============================================================================
        # PART 2: POST-RECORDING CATEGORY CHANGE / MIGRATION (ALL STATUSES & SCOPES)
        # ==============================================================================
        print("\n[PART 2] Post-Recording Category Migration (All Statuses, Scopes & Extra Fields)")

        lg_valid = _create_lg(f"LG-CAT-VAL-{fx.uid}", fx.cat_standard.id)
        lg_expired = _create_lg(f"LG-CAT-EXP-{fx.uid}", fx.cat_standard.id)
        lg_released = _create_lg(f"LG-CAT-REL-{fx.uid}", fx.cat_mandatory_extra.id, extra={"project_wbs_code": "WBS-REL-100"})
        lg_liquidated = _create_lg(f"LG-CAT-LIQ-{fx.uid}", fx.cat_standard.id)
        lg_sub_ent = _create_lg(f"LG-CAT-SUB-{fx.uid}", fx.cat_standard.id, entity_id=fx.entity_alpha_sub.id)

        # Put lg_expired, lg_released, lg_liquidated in respective statuses directly to test all statuses
        db.query(LGRecord).filter(LGRecord.id == lg_expired["id"]).update({"lg_status_id": LgStatusEnum.EXPIRED.value})
        db.query(LGRecord).filter(LGRecord.id == lg_released["id"]).update({"lg_status_id": LgStatusEnum.RELEASED.value})
        db.query(LGRecord).filter(LGRecord.id == lg_liquidated["id"]).update({"lg_status_id": LgStatusEnum.LIQUIDATED.value})
        db.commit()

        # 2.1 Verify lg_count on /end-user/lg-categories/ (even for user with lg_record:view_own)
        res_counts = client.get("/api/v1/end-user/lg-categories/", headers=maker_h)
        assert res_counts.status_code == 200
        counts_map = {c["id"]: c["lg_count"] for c in res_counts.json()}
        assert counts_map[fx.cat_standard.id] == 4
        assert counts_map[fx.cat_mandatory_extra.id] == 1

        # 2.2 Single scope: No-field -> Mandatory extra-field (missing value blocked -> HTTP 400)
        res_missing = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "single",
                "lg_record_id": lg_valid["id"],
                "new_lg_category_id": fx.cat_mandatory_extra.id,
            },
        )
        assert res_missing.status_code == 400 and "project_wbs_code" in res_missing.json()["detail"]

        # Capture custody_serial and issue an instruction BEFORE changing category
        orig_custody_serial = lg_valid["custody_serial"]
        res_ext1 = client.post(
            f"/api/v1/end-user/lg-records/{lg_valid['id']}/extend",
            headers=maker_h,
            json={"new_expiry_date": (date.today() + timedelta(days=270)).isoformat()},
        )
        assert res_ext1.status_code == 200, res_ext1.text
        inst1_id = res_ext1.json()["latest_instruction_id"]
        db.expire_all()
        inst1 = db.query(LGInstruction).filter(LGInstruction.id == inst1_id).first()
        inst1_serial_before = inst1.serial_number
        assert fx.cat_standard.code in inst1_serial_before

        # Single scope: No-field -> Mandatory extra-field WITH value (no approval workflow!)
        res_s1 = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "single",
                "lg_record_id": lg_valid["id"],
                "new_lg_category_id": fx.cat_mandatory_extra.id,
                "extra_field_value": "WBS-NEW-001",
                "reason": "Reclassifying to strategic project",
            },
        )
        assert res_s1.status_code == 200, res_s1.text
        s1_data = res_s1.json()
        assert "approval_request_id" not in s1_data
        assert s1_data["lg_record"]["lg_category"]["id"] == fx.cat_mandatory_extra.id
        assert s1_data["lg_record"]["additional_field_values"] == {"project_wbs_code": "WBS-NEW-001"}
        assert s1_data["lg_record"]["custody_serial"] == orig_custody_serial

        # Verify previous instruction serial stayed intact, while new instruction uses new category code
        res_s1b = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "single",
                "lg_record_id": lg_valid["id"],
                "new_lg_category_id": cat_extra_b["id"],
                "carry_over_old_extra_field": True,
                "reason": "Moving extra field A -> extra field B via carry-over",
            },
        )
        assert res_s1b.status_code == 200, res_s1b.text
        assert res_s1b.json()["lg_record"]["additional_field_values"] == {"cost_center": "WBS-NEW-001"}

        res_ext2 = client.post(
            f"/api/v1/end-user/lg-records/{lg_valid['id']}/extend",
            headers=maker_h,
            json={"new_expiry_date": (date.today() + timedelta(days=360)).isoformat()},
        )
        assert res_ext2.status_code == 200, res_ext2.text
        inst2_id = res_ext2.json()["latest_instruction_id"]
        db.expire_all()
        inst1_after = db.query(LGInstruction).filter(LGInstruction.id == inst1_id).first()
        inst2 = db.query(LGInstruction).filter(LGInstruction.id == inst2_id).first()
        assert inst1_after.serial_number == inst1_serial_before, "Existing instruction serial must stay immutable!"
        assert "CB" in inst2.serial_number, f"New instruction serial should embed 'CB', got {inst2.serial_number}"
        print("  [PASS] Single migration (no-field -> mandatory -> extra A -> extra B) & serial immutability")

        # 2.3 Single scope on Released LG: Extra-field -> No-field (clears active JSON, preserves AuditLog)
        res_s_rel = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "single",
                "lg_record_id": lg_released["id"],
                "new_lg_category_id": fx.cat_standard.id,
                "reason": "Cleanup released LG category",
            },
        )
        assert res_s_rel.status_code == 200, res_s_rel.text
        assert res_s_rel.json()["lg_record"]["lg_category"]["id"] == fx.cat_standard.id
        assert res_s_rel.json()["lg_record"]["additional_field_values"] is None

        res_hist = client.get(f"/api/v1/end-user/lg-records/{lg_released['id']}/lifecycle-history", headers=maker_h)
        assert res_hist.status_code == 200
        cat_events = [e for e in res_hist.json() if e["action_type"] == "LG_SINGLE_CATEGORY_CHANGED"]
        assert len(cat_events) == 1
        assert cat_events[0]["details"]["old_additional_field_values"] == {"project_wbs_code": "WBS-REL-100"}
        assert cat_events[0]["details"]["new_additional_field_values"] is None

        # Also check Expired and Liquidated statuses work cleanly
        res_s_exp = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "single",
                "lg_record_id": lg_expired["id"],
                "new_lg_category_id": c1["id"],
            },
        )
        assert res_s_exp.status_code == 200, res_s_exp.text

        res_s_liq = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "single",
                "lg_record_id": lg_liquidated["id"],
                "new_lg_category_id": c1["id"],
            },
        )
        assert res_s_liq.status_code == 200, res_s_liq.text
        print("  [PASS] Category change allowed on Expired, Released, and Liquidated LGs + AuditLog preserved")

        # 2.4 Entity compatibility guard
        res_ent_bad = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "single",
                "lg_record_id": lg_sub_ent["id"],
                "new_lg_category_id": c_hq["id"],
            },
        )
        assert res_ent_bad.status_code == 400 and "restricted" in res_ent_bad.json()["detail"].lower()

        # 2.5 Multiple scope: atomic rollback if one LG lacks mandatory extra field
        res_mult_fail = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "multiple",
                "lg_record_ids": [lg_expired["id"], lg_liquidated["id"]],
                "new_lg_category_id": fx.cat_mandatory_extra.id,
                "per_lg_extra_field_values": {str(lg_expired["id"]): "WBS-ONLY-ONE"},
            },
        )
        assert res_mult_fail.status_code == 400
        db.expire_all()
        assert db.query(LGRecord).get(lg_expired["id"]).lg_category_id == c1["id"], "Atomic rollback failed!"

        # Multiple scope with per-LG map succeeds
        res_mult_ok = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "multiple",
                "lg_record_ids": [lg_expired["id"], lg_liquidated["id"]],
                "new_lg_category_id": fx.cat_mandatory_extra.id,
                "per_lg_extra_field_values": {
                    str(lg_expired["id"]): "WBS-EXP-01",
                    str(lg_liquidated["id"]): "WBS-LIQ-02",
                },
                "reason": "Batch reclassification",
            },
        )
        assert res_mult_ok.status_code == 200 and res_mult_ok.json()["affected_lgs_count"] == 2
        db.expire_all()
        assert db.query(LGRecord).get(lg_expired["id"]).additional_field_values == {"project_wbs_code": "WBS-EXP-01"}
        assert db.query(LGRecord).get(lg_liquidated["id"]).additional_field_values == {"project_wbs_code": "WBS-LIQ-02"}
        print("  [PASS] Multiple scope atomic validation + per-LG extra field values")

        # 2.6 All scope by source_category_id -> moves all in cat_mandatory_extra to c1 (clears extra fields)
        res_all_src = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "all",
                "source_category_id": fx.cat_mandatory_extra.id,
                "new_lg_category_id": c1["id"],
                "reason": "Consolidate strategic projects into Default One",
            },
        )
        assert res_all_src.status_code == 200 and res_all_src.json()["affected_lgs_count"] == 2
        db.expire_all()
        assert db.query(LGRecord).get(lg_expired["id"]).lg_category_id == c1["id"]
        assert db.query(LGRecord).get(lg_expired["id"]).additional_field_values is None

        # 2.7 All scope by restricted user (only entity_alpha_hq) -> must NOT touch entity_alpha_sub!
        res_all_rest = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=restricted_h,
            json={
                "change_scope": "all",
                "new_lg_category_id": fx.cat_standard.id,
                "reason": "HQ user standardizing accessible LGs",
            },
        )
        assert res_all_rest.status_code == 200, res_all_rest.text
        db.expire_all()
        assert db.query(LGRecord).get(lg_valid["id"]).lg_category_id == fx.cat_standard.id
        assert db.query(LGRecord).get(lg_expired["id"]).lg_category_id == fx.cat_standard.id
        assert db.query(LGRecord).get(lg_sub_ent["id"]).lg_category_id == fx.cat_standard.id
        # Also verify lg_sub_ent was NOT in affected_lg_ids of restricted user's call
        assert lg_sub_ent["id"] not in res_all_rest.json()["affected_lg_ids"]
        print("  [PASS] All scope (by source category & restricted user entity isolation)")

        # ==============================================================================
        # PART 3: IMMEDIATE IN-PLACE CATEGORY EDIT PROPAGATION
        # ==============================================================================
        print("\n[PART 3] Immediate In-Place Category Edit Propagation")

        # Migrate lg_valid (HQ) and lg_sub_ent (Sub) into cat_extra_b ('cost_center')
        res_setup_p3 = client.post(
            "/api/v1/end-user/lg-records/change-category",
            headers=maker_h,
            json={
                "change_scope": "multiple",
                "lg_record_ids": [lg_valid["id"], lg_sub_ent["id"]],
                "new_lg_category_id": cat_extra_b["id"],
                "per_lg_extra_field_values": {
                    str(lg_valid["id"]): "CC-100",
                    str(lg_sub_ent["id"]): "CC-200",
                },
                "reason": "Assign Cost Center Group before in-place edit",
            },
        )
        assert res_setup_p3.status_code == 200 and res_setup_p3.json()["affected_lgs_count"] == 2

        # 3.1 Rename name, code, and extra_field_name in-place -> immediate effect on linked LGs
        renamed_cat_name = f"Updated Cost Group {fx.uid}"
        res_edit_rename = client.put(
            f"/api/v1/corporate-admin/lg-categories/{cat_extra_b['id']}",
            headers=admin_h,
            json={
                "name": renamed_cat_name,
                "code": "UC",
                "extra_field_name": "department_code",
                "is_mandatory": True,
                "has_all_entity_access": True,
            },
        )
        assert res_edit_rename.status_code == 200, res_edit_rename.text

        res_get_val = client.get(f"/api/v1/end-user/lg-records/{lg_valid['id']}", headers=maker_h)
        res_get_sub = client.get(f"/api/v1/end-user/lg-records/{lg_sub_ent['id']}", headers=maker_h)
        assert res_get_val.status_code == 200 and res_get_sub.status_code == 200
        v_json = res_get_val.json()
        s_json = res_get_sub.json()
        assert v_json["lg_category"]["name"] == renamed_cat_name
        assert v_json["lg_category"]["code"] == "UC"
        assert v_json["lg_category"]["extra_field_name"] == "department_code"
        assert v_json["additional_field_values"] == {"department_code": "CC-100"}
        assert s_json["additional_field_values"] == {"department_code": "CC-200"}
        print("  [PASS] Category name, code, and extra_field_name rename propagated immediately to linked LGs")

        # 3.2 Restricting entities when an excluded entity still has LGs must return 400 Bad Request
        res_bad_restrict = client.put(
            f"/api/v1/corporate-admin/lg-categories/{cat_extra_b['id']}",
            headers=admin_h,
            json={
                "has_all_entity_access": False,
                "entity_ids": [fx.entity_alpha_hq.id],
            },
        )
        assert res_bad_restrict.status_code == 400, f"Expected 400, got {res_bad_restrict.status_code}: {res_bad_restrict.text}"
        print("  [PASS] Restricting category entities blocked when LGs from excluded entities exist")

        # 3.3 Clearing extra_field_name immediately clears additional_field_values and logs AuditLog
        res_edit_clear = client.put(
            f"/api/v1/corporate-admin/lg-categories/{cat_extra_b['id']}",
            headers=admin_h,
            json={
                "extra_field_name": "",
                "is_mandatory": False,
            },
        )
        assert res_edit_clear.status_code == 200, res_edit_clear.text

        res_get_val_clr = client.get(f"/api/v1/end-user/lg-records/{lg_valid['id']}", headers=maker_h)
        res_get_sub_clr = client.get(f"/api/v1/end-user/lg-records/{lg_sub_ent['id']}", headers=maker_h)
        assert res_get_val_clr.status_code == 200 and res_get_sub_clr.status_code == 200
        assert res_get_val_clr.json()["lg_category"]["extra_field_name"] is None
        assert res_get_val_clr.json()["additional_field_values"] is None
        assert res_get_sub_clr.json()["additional_field_values"] is None

        db.expire_all()
        cleared_audit = (
            db.query(AuditLog)
            .filter(
                AuditLog.lg_record_id == lg_valid["id"],
                AuditLog.action_type == "LG_CATEGORY_EXTRA_FIELD_CLEARED",
            )
            .order_by(AuditLog.id.desc())
            .first()
        )
        assert cleared_audit is not None
        assert cleared_audit.details["old_additional_field_values"] == {"department_code": "CC-100"}
        print("  [PASS] Clearing extra_field_name cleared active JSON on linked LGs & archived prior value in AuditLog")

        print("\nALL DEFAULT CATEGORY & POST-RECORDING MIGRATION CHECKS PASSED!")
    finally:
        if created_univ_cat_id is not None:
            try:
                db.execute(
                    text_sql := __import__("sqlalchemy").text(
                        "UPDATE customers SET default_lg_category_id = NULL WHERE default_lg_category_id = :cid"
                    ),
                    {"cid": created_univ_cat_id},
                )
                db.execute(
                    __import__("sqlalchemy").text(
                        "DELETE FROM audit_logs WHERE entity_type = 'LGCategory' AND entity_id = :cid"
                    ),
                    {"cid": created_univ_cat_id},
                )
                db.execute(
                    __import__("sqlalchemy").text("DELETE FROM lg_categories WHERE id = :cid"),
                    {"cid": created_univ_cat_id},
                )
                db.commit()
            except Exception as e:
                db.rollback()
                print(f"[Cleanup warning] Failed to clean universal category {created_univ_cat_id}: {e}")
        fx.cleanup()
        db.close()


if __name__ == "__main__":
    run_lg_category_default_and_migration_suite()
