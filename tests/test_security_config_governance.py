# c:\Grow\tests\test_security_config_governance.py
"""
Verification suite for:
1. 3-tier ConfigScope ownership classification (PLATFORM_GLOBAL, SYSTEM_CUSTOMER_OVERRIDE, CUSTOMER_CONFIGURABLE).
2. Corporate Admin & End User isolation (security, OTP, legal-version, and grace-period configs blocked/hidden).
3. System Owner per-customer security overrides + Forward-Only (Option A) enforcement.
4. Emergency Force Password Change (Option C: per-customer & global) + password-reuse guard + live token refresh.
"""

import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy import text

from _test_harness import (
    app,
    SessionLocal,
    IsolatedTenantFixture,
    install_email_capture,
    install_in_memory_gcs,
    make_auth_headers,
)
from app.constants import (
    ConfigScope,
    GlobalConfigKey,
    UserRole,
    get_default_config_scope,
)
from app.core.hashing import get_password_hash
from app.core.security import SECRET_KEY, ALGORITHM
from app.crud.crud import crud_role_permission
from app.models.models import User


def _hdrs(db, u: User):
    r_str = u.role.value if hasattr(u.role, "value") else str(u.role)
    perms = [p.name for p in crud_role_permission.get_permissions_for_role(db, r_str)]
    return make_auth_headers(u, extra_claims={"permissions": perms, "can_email_inbox": True})


def run_security_config_governance_tests():
    print("=" * 90)
    print("GROW TREASURY — SECURITY CONFIG GOVERNANCE & EMERGENCY ROTATION TEST SUITE")
    print("=" * 90)

    install_in_memory_gcs()
    install_email_capture()

    db = SessionLocal()
    fx = IsolatedTenantFixture(db, tag="SEC_GOV")
    client = TestClient(app)

    pre_existing_false_user_ids = []
    extra_user_ids = []

    try:
        so_h = _hdrs(db, fx.sys_owner)
        admin_alpha_h = _hdrs(db, fx.admin_user)
        maker_alpha_h = _hdrs(db, fx.maker_user)
        beta_maker_h = _hdrs(db, fx.beta_maker)

        # Create dedicated 9-char password users for Tenant Alpha and Tenant Beta
        sfx = fx.uid.lower()
        legacy_pw = "NineCh#99"  # 9 chars: satisfies global 8-char rule, fails 12-char override
        alpha_legacy = User(
            email=f"legacy.alpha.{sfx}@globex.com",
            password_hash=get_password_hash(legacy_pw),
            role=UserRole.END_USER,
            customer_id=fx.cust_alpha.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        beta_legacy = User(
            email=f"legacy.beta.{sfx}@globex.com",
            password_hash=get_password_hash(legacy_pw),
            role=UserRole.END_USER,
            customer_id=fx.cust_beta.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        db.add_all([alpha_legacy, beta_legacy])
        db.commit()
        db.refresh(alpha_legacy)
        db.refresh(beta_legacy)
        extra_user_ids.extend([alpha_legacy.id, beta_legacy.id])

        # ------------------------------------------------------------------
        # CHECK 1: ConfigScope mapping & System Owner global list
        # ------------------------------------------------------------------
        print("\n[CHECK 1] 3-Tier ConfigScope Classification")
        assert get_default_config_scope(GlobalConfigKey.PASSWORD_MIN_LENGTH) == ConfigScope.SYSTEM_CUSTOMER_OVERRIDE
        assert get_default_config_scope(GlobalConfigKey.LOGIN_MAX_FAILED_ATTEMPTS) == ConfigScope.SYSTEM_CUSTOMER_OVERRIDE
        assert get_default_config_scope(GlobalConfigKey.OTP_MAX_FAILED_ATTEMPTS) == ConfigScope.SYSTEM_CUSTOMER_OVERRIDE
        assert get_default_config_scope(GlobalConfigKey.GRACE_PERIOD_DAYS) == ConfigScope.SYSTEM_CUSTOMER_OVERRIDE
        assert get_default_config_scope(GlobalConfigKey.TC_VERSION) == ConfigScope.PLATFORM_GLOBAL
        assert get_default_config_scope(GlobalConfigKey.CBE_MID_CORRIDOR_RATE) == ConfigScope.PLATFORM_GLOBAL
        assert get_default_config_scope(GlobalConfigKey.STORAGE_BUCKET_NAME) == ConfigScope.CUSTOMER_CONFIGURABLE
        assert get_default_config_scope(GlobalConfigKey.AUTO_RENEWAL_DAYS_BEFORE_EXPIRY) == ConfigScope.CUSTOMER_CONFIGURABLE

        r_global = client.get("/api/v1/system-owner/global-configurations/", headers=so_h)
        assert r_global.status_code == 200, f"Global config list failed: {r_global.text}"
        global_by_key = {row["key"]: row for row in r_global.json()}
        assert global_by_key["PASSWORD_MIN_LENGTH"]["config_scope"] == "SYSTEM_CUSTOMER_OVERRIDE"
        assert global_by_key["GRACE_PERIOD_DAYS"]["config_scope"] == "SYSTEM_CUSTOMER_OVERRIDE"
        assert global_by_key["TC_VERSION"]["config_scope"] == "PLATFORM_GLOBAL"
        print("  [PASS] ConfigScope mapping and GlobalConfiguration API output verified")

        # ------------------------------------------------------------------
        # CHECK 2: Corporate Admin & End User Scope Isolation
        # ------------------------------------------------------------------
        print("\n[CHECK 2] Corporate Admin & End User Scope Isolation")
        r_ca_list = client.get("/api/v1/corporate-admin/customer-configurations/", headers=admin_alpha_h)
        assert r_ca_list.status_code == 200, f"CA config list failed: {r_ca_list.text}"
        ca_keys = {row["global_config_key"] for row in r_ca_list.json()}

        forbidden_ca_keys = {
            "PASSWORD_MIN_LENGTH",
            "PASSWORD_REQUIRE_UPPERCASE",
            "PASSWORD_REQUIRE_LOWERCASE",
            "PASSWORD_REQUIRE_DIGIT",
            "PASSWORD_RESET_TOKEN_EXPIRY_MINUTES",
            "LOGIN_MAX_FAILED_ATTEMPTS",
            "LOGIN_LOCKOUT_DURATION_MINUTES",
            "OTP_MAX_FAILED_ATTEMPTS",
            "OTP_LOCKOUT_DURATION_MINUTES",
            "PUBLIC_ISSUANCE_SESSION_EXPIRY_MINUTES",
            "INVITE_LINK_EXPIRY_HOURS",
            "GRACE_PERIOD_DAYS",
            "TC_VERSION",
            "PP_VERSION",
        }
        leaked = ca_keys.intersection(forbidden_ca_keys)
        assert not leaked, f"SECURITY BUG: CA endpoint leaked restricted keys: {leaked}"
        if "STORAGE_BUCKET_NAME" in global_by_key:
            assert "STORAGE_BUCKET_NAME" in ca_keys, "STORAGE_BUCKET_NAME should remain CUSTOMER_CONFIGURABLE"

        for blocked_key in ("PASSWORD_MIN_LENGTH", "GRACE_PERIOD_DAYS", "TC_VERSION"):
            r_put = client.put(
                f"/api/v1/corporate-admin/customer-configurations/{blocked_key}",
                headers=admin_alpha_h,
                json={"configured_value": "12" if blocked_key != "TC_VERSION" else "9.9"},
            )
            assert r_put.status_code == 403, f"Expected 403 for CA PUT {blocked_key}, got {r_put.status_code}: {r_put.text}"

        r_eu_get = client.get(
            "/api/v1/end-user/customer-configurations/PASSWORD_MIN_LENGTH",
            headers=maker_alpha_h,
        )
        assert r_eu_get.status_code == 403, f"Expected 403 for EU GET PASSWORD_MIN_LENGTH, got {r_eu_get.status_code}"
        print("  [PASS] Corporate Admin and End User blocked from security/platform keys")

        # ------------------------------------------------------------------
        # CHECK 3: System Owner Per-Customer Override & Forward-Only Option A
        # ------------------------------------------------------------------
        print("\n[CHECK 3] System Owner Per-Customer Overrides + Forward-Only Option A")
        r_ca_so = client.get(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}/security-configurations",
            headers=admin_alpha_h,
        )
        assert r_ca_so.status_code == 403, "Corporate Admin must not access SO security override endpoint"

        r_so_sec = client.get(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}/security-configurations",
            headers=so_h,
        )
        assert r_so_sec.status_code == 200
        sec_keys = {row["global_config_key"] for row in r_so_sec.json()}
        assert "PASSWORD_MIN_LENGTH" in sec_keys
        assert "AUTO_RENEWAL_DAYS_BEFORE_EXPIRY" not in sec_keys

        # Reject PLATFORM_GLOBAL override per customer
        r_pg_override = client.put(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}/security-configurations/TC_VERSION",
            headers=so_h,
            json={"configured_value": "2.0"},
        )
        assert r_pg_override.status_code == 400

        # Reject out-of-bounds value (below global min=8)
        r_oob = client.put(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}/security-configurations/PASSWORD_MIN_LENGTH",
            headers=so_h,
            json={"configured_value": "4"},
        )
        assert r_oob.status_code == 400

        # Set Tenant Alpha PASSWORD_MIN_LENGTH = 12 (Global remains 8)
        r_set12 = client.put(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}/security-configurations/PASSWORD_MIN_LENGTH",
            headers=so_h,
            json={"configured_value": "12"},
        )
        assert r_set12.status_code == 200, f"Failed to set Alpha override: {r_set12.text}"
        assert r_set12.json()["effective_value"] == "12"
        assert r_set12.json()["configured_value"] == "12"

        # Forward-Only Option A: existing 9-char password user in Alpha still logs in normally
        dev_alpha = f"dev-alpha-{sfx}"
        r_login_alpha = client.post(
            "/api/v2/login",
            json={"email": alpha_legacy.email, "password": legacy_pw, "device_id": dev_alpha, "remember_me": True},
        )
        assert r_login_alpha.status_code == 200, f"Forward-Only login failed: {r_login_alpha.text}"
        alpha_tok = r_login_alpha.json()["access_token"]
        decoded_alpha = jwt.decode(alpha_tok, SECRET_KEY, algorithms=[ALGORITHM])
        assert decoded_alpha.get("must_change_password") is False

        # Dynamic /password-policy respects customer override vs global default
        r_pol_a = client.get("/api/v2/password-policy", headers={"Authorization": f"Bearer {alpha_tok}"})
        assert r_pol_a.status_code == 200 and r_pol_a.json()["min_length"] == 12

        r_pol_b = client.get("/api/v2/password-policy", headers=beta_maker_h)
        assert r_pol_b.status_code == 200 and r_pol_b.json()["min_length"] == 8

        # Changing password in Alpha rejects 10 chars, accepts 13 chars; Beta accepts 10 chars
        r_alpha_10 = client.post(
            "/api/v2/change-password",
            headers={"Authorization": f"Bearer {alpha_tok}"},
            json={"current_password": legacy_pw, "new_password": "TenChar#10", "confirm_new_password": "TenChar#10"},
        )
        assert r_alpha_10.status_code == 400 and "12" in r_alpha_10.text

        alpha_pw_13 = "TwelvePlus#13"
        r_alpha_13 = client.post(
            "/api/v2/change-password",
            headers={"Authorization": f"Bearer {alpha_tok}"},
            json={"current_password": legacy_pw, "new_password": alpha_pw_13, "confirm_new_password": alpha_pw_13},
        )
        assert r_alpha_13.status_code == 200, f"13-char change failed: {r_alpha_13.text}"

        dev_beta = f"dev-beta-{sfx}"
        r_login_beta = client.post(
            "/api/v2/login",
            json={"email": beta_legacy.email, "password": legacy_pw, "device_id": dev_beta, "remember_me": True},
        )
        assert r_login_beta.status_code == 200
        beta_tok = r_login_beta.json()["access_token"]
        r_beta_10 = client.post(
            "/api/v2/change-password",
            headers={"Authorization": f"Bearer {beta_tok}"},
            json={"current_password": legacy_pw, "new_password": "TenChar#10", "confirm_new_password": "TenChar#10"},
        )
        assert r_beta_10.status_code == 200, f"Beta 10-char change should pass global 8: {r_beta_10.text}"

        # Reset override -> back to global 8, then re-override -> revives soft-deleted row without UniqueViolation
        r_del = client.delete(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}/security-configurations/PASSWORD_MIN_LENGTH",
            headers=so_h,
        )
        assert r_del.status_code == 200
        assert r_del.json()["configured_value"] is None
        assert r_del.json()["effective_value"] == "8"

        r_reoverride = client.put(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}/security-configurations/PASSWORD_MIN_LENGTH",
            headers=so_h,
            json={"configured_value": "14"},
        )
        assert r_reoverride.status_code == 200 and r_reoverride.json()["effective_value"] == "14"

        client.delete(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}/security-configurations/PASSWORD_MIN_LENGTH",
            headers=so_h,
        )

        # Per-customer LOGIN_MAX_FAILED_ATTEMPTS = 3
        r_set_lock = client.put(
            f"/api/v1/system-owner/customers/{fx.cust_alpha.id}/security-configurations/LOGIN_MAX_FAILED_ATTEMPTS",
            headers=so_h,
            json={"configured_value": "3"},
        )
        assert r_set_lock.status_code == 200

        for i in range(1, 3):
            r_fail_a = client.post(
                "/api/v2/login",
                json={"email": alpha_legacy.email, "password": "WrongPassword!1", "device_id": dev_alpha, "remember_me": False},
            )
            assert r_fail_a.status_code == 401, f"Alpha attempt {i} expected 401, got {r_fail_a.status_code}"

        r_fail_a3 = client.post(
            "/api/v2/login",
            json={"email": alpha_legacy.email, "password": "WrongPassword!1", "device_id": dev_alpha, "remember_me": False},
        )
        assert r_fail_a3.status_code == 403 and "locked" in r_fail_a3.text.lower()

        for i in range(1, 4):
            r_fail_b = client.post(
                "/api/v2/login",
                json={"email": beta_legacy.email, "password": "WrongPassword!1", "device_id": dev_beta, "remember_me": False},
            )
            assert r_fail_b.status_code == 401, f"Beta attempt {i} should remain 401 under global max=5"
        print("  [PASS] Per-customer password & lockout overrides + Forward-Only verified")

        # ------------------------------------------------------------------
        # CHECK 4: Emergency Force Password Change (Option C) + Reuse Guard
        # ------------------------------------------------------------------
        print("\n[CHECK 4] Emergency Force Password Change (Option C) & Password Reuse Guard")
        r_force_alpha = client.post(
            "/api/v1/system-owner/security/force-password-change",
            headers=so_h,
            json={"customer_id": fx.cust_alpha.id, "reason": "Security audit rotation"},
        )
        assert r_force_alpha.status_code == 200, f"Emergency force failed: {r_force_alpha.text}"
        assert r_force_alpha.json()["scope"] == "customer"
        assert r_force_alpha.json()["affected_user_count"] >= 1

        # Active Alpha session immediately blocked with 403 must-change-password
        r_blocked = client.get(
            "/api/v1/end-user/customer-configurations/AUTO_RENEWAL_DAYS_BEFORE_EXPIRY",
            headers=maker_alpha_h,
        )
        assert r_blocked.status_code == 403 and "must change your password" in r_blocked.text.lower()

        # Beta and System Owner are unaffected
        db.expire_all()
        db_beta_maker = db.query(User).filter(User.id == fx.beta_maker.id).first()
        db_so = db.query(User).filter(User.id == fx.sys_owner.id).first()
        assert db_beta_maker.must_change_password is False
        assert db_so.must_change_password is False

        # /refresh-token succeeds for flagged user and encodes must_change_password=True
        r_refresh = client.post("/api/v2/refresh-token", headers=maker_alpha_h)
        assert r_refresh.status_code == 200, f"Refresh token failed for forced user: {r_refresh.text}"
        refreshed_tok = r_refresh.json()["access_token"]
        decoded_refreshed = jwt.decode(refreshed_tok, SECRET_KEY, algorithms=[ALGORITHM])
        assert decoded_refreshed.get("must_change_password") is True

        # Reusing exact same current password is rejected
        r_reuse = client.post(
            "/api/v2/change-password",
            headers={"Authorization": f"Bearer {refreshed_tok}"},
            json={
                "current_password": "StrongPass!2026",
                "new_password": "StrongPass!2026",
                "confirm_new_password": "StrongPass!2026",
            },
        )
        assert r_reuse.status_code == 400 and "different" in r_reuse.text.lower()

        # Setting a new compliant password clears flag & restores access
        r_rotate = client.post(
            "/api/v2/change-password",
            headers={"Authorization": f"Bearer {refreshed_tok}"},
            json={
                "current_password": "StrongPass!2026",
                "new_password": "RotatedVault#2028",
                "confirm_new_password": "RotatedVault#2028",
            },
        )
        assert r_rotate.status_code == 200
        rotated_tok = r_rotate.json()["access_token"]
        decoded_rotated = jwt.decode(rotated_tok, SECRET_KEY, algorithms=[ALGORITHM])
        assert decoded_rotated.get("must_change_password") is False

        r_restored = client.get(
            "/api/v1/end-user/customer-configurations/AUTO_RENEWAL_DAYS_BEFORE_EXPIRY",
            headers=_hdrs(db, fx.maker_user),
        )
        assert r_restored.status_code == 200

        # Global Emergency Force Password Change (snapshot & restore any pre-existing DB users)
        pre_existing_false_user_ids = [
            row[0]
            for row in db.query(User.id).filter(
                User.is_deleted == False,
                User.must_change_password == False,
                User.customer_id.isnot(None),
                User.customer_id.notin_([fx.cust_alpha.id, fx.cust_beta.id]),
            ).all()
        ]
        r_force_all = client.post(
            "/api/v1/system-owner/security/force-password-change",
            headers=so_h,
            json={"customer_id": None, "reason": "Platform-wide emergency test"},
        )
        assert r_force_all.status_code == 200
        assert r_force_all.json()["scope"] == "all_customers"

        db.expire_all()
        assert db.query(User).filter(User.id == fx.maker_user.id).first().must_change_password is True
        assert db.query(User).filter(User.id == fx.beta_maker.id).first().must_change_password is True
        assert db.query(User).filter(User.id == fx.sys_owner.id).first().must_change_password is False
        print("  [PASS] Emergency Force Password Change (per-customer & global) + reuse guard verified")

        print("\nALL SECURITY CONFIG GOVERNANCE CHECKS PASSED SUCCESSFULLY!")
    finally:
        if pre_existing_false_user_ids:
            try:
                db.execute(
                    text("UPDATE users SET must_change_password = false WHERE id = ANY(:uids)"),
                    {"uids": pre_existing_false_user_ids},
                )
                db.commit()
            except Exception:
                db.rollback()
        try:
            db.execute(
                text(
                    "DELETE FROM audit_logs WHERE action_type = 'EMERGENCY_FORCE_PASSWORD_CHANGE' "
                    "AND (customer_id IS NULL OR customer_id = ANY(:cids))"
                ),
                {"cids": [fx.cust_alpha.id, fx.cust_beta.id]},
            )
            db.commit()
        except Exception:
            db.rollback()
        if extra_user_ids:
            try:
                db.execute(text("DELETE FROM user_devices WHERE user_id = ANY(:uids)"), {"uids": extra_user_ids})
                db.execute(text("DELETE FROM audit_logs WHERE user_id = ANY(:uids)"), {"uids": extra_user_ids})
                db.execute(text("DELETE FROM users WHERE id = ANY(:uids)"), {"uids": extra_user_ids})
                db.commit()
            except Exception:
                db.rollback()
        fx.cleanup()
        db.close()


if __name__ == "__main__":
    run_security_config_governance_tests()
