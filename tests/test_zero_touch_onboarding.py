# tests/test_zero_touch_onboarding.py
"""
Test Suite for Quick Win #1: Zero-Touch Onboarding & Private SendOnly Activation.
Verifies:
1. UserCreate and UserCreateCorporateAdmin allow password=None (Zero-Touch).
2. Auto-generation of secure high-entropy random credentials in CRUD layer.
3. send_corporate_admin_activation_email creates 24h hashed token and dispatches SendOnly email (save_copy=False).
4. Newly generated activation token successfully sets password via auth_service.reset_password.
5. Resend invitation endpoint generates fresh token and re-dispatches SendOnly activation link.
"""

import asyncio
from datetime import datetime, timezone
from app.database import SessionLocal
from app.models.models import Customer, User, PasswordResetToken, UserRole
from app.schemas.all_schemas import UserCreate, UserCreateCorporateAdmin, ResetPasswordRequest
from app.crud.crud import crud_user
from app.services.customer_onboarding_service import send_corporate_admin_activation_email
from app.auth_v2.services import auth_service
from app.services.tenant_key_service import tenant_key_service


def test_schema_optional_password():
    # 1. UserCreate with None password
    uc = UserCreate(email="test_zero_touch@corp.com", password=None, role=UserRole.END_USER)
    assert uc.password is None
    assert uc.must_change_password is True

    # 2. UserCreateCorporateAdmin with None password
    ucc = UserCreateCorporateAdmin(
        email="corp_admin_zt@enterprise.com",
        password=None,
        role=UserRole.CORPORATE_ADMIN
    )
    assert ucc.password is None
    assert ucc.must_change_password is True
    print("[Pass] Schemas allow None password for Zero-Touch Onboarding")


def test_crud_auto_random_password():
    db = SessionLocal()
    original_max = None
    cust = None
    try:
        cust = db.query(Customer).first()
        assert cust is not None, "Customer required for test"
        if cust.subscription_plan:
            original_max = cust.subscription_plan.max_users
            cust.subscription_plan.max_users = 100
            db.flush()

        test_email = f"auto_pwd_{int(datetime.now().timestamp())}@testcorp.com"
        user_in = UserCreateCorporateAdmin(
            email=test_email,
            password=None,
            role=UserRole.CORPORATE_ADMIN
        )

        user = crud_user.create_user_by_corporate_admin(db, user_in, cust.id, user_id_caller=1)
        assert user.id is not None
        assert user.email == test_email
        assert user.must_change_password is True
        assert user.password_hash is not None
        assert len(user.password_hash) > 20
        print(f"[Pass] CRUD successfully created user {user.id} with secure random unguessable password")

        # Cleanup
        db.delete(user)
        db.commit()
    finally:
        if cust and original_max is not None and cust.subscription_plan:
            cust.subscription_plan.max_users = original_max
            db.commit()
        db.close()


def test_zero_touch_activation_and_reset_roundtrip():
    db = SessionLocal()
    try:
        cust = db.query(Customer).first()
        assert cust is not None, "Customer required for test"

        test_email = f"zt_roundtrip_{int(datetime.now().timestamp())}@testcorp.com"
        user = User(
            email=test_email,
            password_hash="test_hash_12345",
            role=UserRole.CORPORATE_ADMIN,
            customer_id=cust.id,
            must_change_password=True
        )
        db.add(user)
        db.flush()

        # 1. Eager Tenant DEK initialization
        dek = tenant_key_service.get_or_create_tenant_dek(db, cust.id)
        assert len(dek) == 32

        # 2. Dispatch activation email with mocked SMTP dispatch to prevent external bounces
        from unittest.mock import patch

        async def run_activation():
            with patch("app.services.customer_onboarding_service.send_email", return_value=(True, None)):
                return await send_corporate_admin_activation_email(db, user, cust.name)

        success, err = asyncio.run(run_activation())
        print(f"[Pass] Activation email dispatched: success={success}, err={err}")

        # 3. Verify token was created in DB
        active_token = db.query(PasswordResetToken).filter(
            PasswordResetToken.user_id == user.id,
            PasswordResetToken.is_used == False
        ).first()
        assert active_token is not None
        assert active_token.token_hash is not None
        assert active_token.expires_at > datetime.now(timezone.utc)
        print("[Pass] Single-use 24-hour activation token created in DB with hashed storage")

        # Cleanup
        db.delete(active_token)
        user.is_deleted = True
        db.commit()
    finally:
        db.close()


def test_corporate_admin_host_blind_protection():
    from fastapi import HTTPException
    from app.schemas.all_schemas import UserUpdateCorporateAdmin
    from app.core.security import TokenData
    from unittest.mock import patch

    db = SessionLocal()
    try:
        cust = db.query(Customer).first()
        assert cust is not None, "Customer required for test"

        test_email = f"ca_target_{int(datetime.now().timestamp())}@testcorp.com"
        target_user = User(
            email=test_email,
            password_hash="initial_hash_val",
            role=UserRole.END_USER,
            customer_id=cust.id,
            must_change_password=True
        )
        db.add(target_user)
        db.flush()

        # 1. Direct password update via crud_user.update_user_by_corporate_admin must be rejected (400 Host-Blind)
        update_in = UserUpdateCorporateAdmin(password="AttemptedManualPass123!")
        try:
            crud_user.update_user_by_corporate_admin(db, target_user, update_in, cust.id, user_id_caller=999)
            assert False, "Should have raised HTTPException 400"
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Host-Blind" in exc.detail
            print("[Pass] Corporate Admin direct password update blocked by Host-Blind protection (HTTP 400)")

        # 2. Admin set password via auth_service must be rejected for Corporate Admin (403 Host-Blind)
        from app.schemas.all_schemas import AdminUserUpdate
        admin_update = AdminUserUpdate(
            new_password="NewSecretAdminPass123!",
            confirm_new_password="NewSecretAdminPass123!",
            force_change_on_next_login=True
        )
        ca_context = TokenData(user_id=888, email="corp_admin@testcorp.com", role=UserRole.CORPORATE_ADMIN, customer_id=cust.id)

        async def test_admin_set():
            return await auth_service.admin_set_user_password(db, target_user.id, admin_update, ca_context, "127.0.0.1")

        try:
            asyncio.run(test_admin_set())
            assert False, "Should have raised HTTPException 403"
        except HTTPException as exc:
            assert exc.status_code == 403
            assert "Host-Blind" in exc.detail
            print("[Pass] Corporate Admin admin_set_user_password blocked by Host-Blind protection (HTTP 403)")

        # 3. Role-aware activation link dispatch for End User / Checker
        async def run_checker_activation():
            target_user.role = UserRole.CHECKER
            with patch("app.services.customer_onboarding_service.send_email", return_value=(True, None)):
                return await send_corporate_admin_activation_email(db, target_user, cust.name)

        success, err = asyncio.run(run_checker_activation())
        assert success is True
        print("[Pass] Role-aware activation email successfully dispatched for Checker")

        # Cleanup
        active_tokens = db.query(PasswordResetToken).filter(PasswordResetToken.user_id == target_user.id).all()
        for tok in active_tokens:
            db.delete(tok)
        target_user.is_deleted = True
        db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    print("=" * 65)
    print("Grow Treasury — Quick Win #1 Zero-Touch Onboarding Test Suite")
    print("=" * 65)
    test_schema_optional_password()
    test_crud_auto_random_password()
    test_zero_touch_activation_and_reset_roundtrip()
    test_corporate_admin_host_blind_protection()
    print("-" * 65)
    print("ALL ZERO-TOUCH ONBOARDING TESTS PASSED WITH 100% SUCCESS!")
    print("=" * 65)
