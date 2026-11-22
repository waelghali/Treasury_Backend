# tests/test_lg_field_provenance_sidecar.py
"""
End-to-End & Integration Suite for Additive LG Field Provenance Sidecar:
1. Exact provenance capture (AI_SCAN vs USER_EDITED_AI vs USER_MANUAL vs SYSTEM_DEFAULT)
2. Post-recording amendment & category/auto-renewal overlay from AuditLog
3. Smart read-only DB correlation for old existing records:
   - Unscanned manual legacy LGs -> marks LG Core & Bank/Rule fields as USER_MANUAL
   - AI-scanned legacy LGs (matched via AI_SCAN_SUCCESS / AIUsageLog) -> leaves OCR fields unmarked unless later amended
   - Migrated ('LEGACY') & Trial Sample ('TRIAL_SAMPLE') LGs -> leaves initial fields unmarked
4. Strict multi-tenant & subsidiary entity isolation (404 across tenants / restricted subsidiaries)
"""

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from _test_harness import (
    app,
    SessionLocal,
    IsolatedTenantFixture,
    make_auth_headers,
)
from app.constants import (
    AUDIT_ACTION_TYPE_LG_AMENDED,
    AUDIT_ACTION_TYPE_LG_SINGLE_CATEGORY_CHANGED,
    DOCUMENT_TYPE_ORIGINAL_LG,
    LgStatusEnum,
    LgTypeEnum,
)
from app.models.models import (
    AIUsageLog,
    AuditLog,
    InternalOwnerContact,
    IssuingMethod,
    LGCategory,
    LGDocument,
    LGRecord,
)


def _seed_lg_row(
    db,
    fx: IsolatedTenantFixture,
    owner: InternalOwnerContact,
    cat: LGCategory,
    method: IssuingMethod,
    *,
    lg_number: str,
    entity=None,
    migration_source=None,
    auto_renewal=True,
    other_conditions=None,
) -> LGRecord:
    now_utc = datetime.now(timezone.utc)
    target_entity = entity or fx.entity_alpha_hq
    lg = LGRecord(
        customer_id=fx.cust_alpha.id,
        beneficiary_corporate_id=target_entity.id,
        lg_sequence_number=int(uuid.uuid4().int % 9000) + 100,
        issuer_name="Orascom Construction",
        lg_number=lg_number,
        lg_amount=250000,
        lg_currency_id=fx.ccy_egp.id,
        lg_payable_currency_id=fx.ccy_egp.id,
        issuance_date=now_utc - timedelta(days=30),
        expiry_date=now_utc + timedelta(days=335),
        lg_period_months=12,
        auto_renewal=auto_renewal,
        lg_type_id=fx.lg_type_perf.id if fx.lg_type_perf else LgTypeEnum.PERFORMANCE_GUARANTEE.value,
        lg_status_id=LgStatusEnum.VALID.value,
        description_purpose="Performance guarantee for EPC contract",
        issuing_bank_id=fx.bank_cib.id,
        issuing_bank_address=fx.bank_cib.address or "CIB HQ",
        issuing_bank_phone=fx.bank_cib.phone_number or "19666",
        issuing_bank_fax="",
        issuing_method_id=method.id,
        applicable_rule_id=fx.rule_urdg.id,
        other_conditions=other_conditions,
        internal_owner_contact_id=owner.id,
        lg_category_id=cat.id,
        migration_source=migration_source,
    )
    db.add(lg)
    db.commit()
    db.refresh(lg)
    return lg


def run_suite():
    print("=" * 72)
    print("RUNNING LG FIELD PROVENANCE SIDECAR SUITE")
    print("=" * 72)

    db = SessionLocal()
    fx = None
    try:
        fx = IsolatedTenantFixture(db, tag="PROV")
        client = TestClient(app)

        headers_maker = make_auth_headers(fx.maker_user)
        headers_restricted = make_auth_headers(fx.restricted_maker)
        headers_admin = make_auth_headers(fx.admin_user)
        headers_beta = make_auth_headers(fx.beta_maker)

        owner = InternalOwnerContact(
            customer_id=fx.cust_alpha.id,
            email=f"owner.{fx.uid.lower()}@globex.com",
            phone_number="+201009998877",
            manager_email=f"mgr.{fx.uid.lower()}@globex.com",
        )
        cat_default = LGCategory(
            customer_id=fx.cust_alpha.id,
            name=f"Default Cat {fx.uid}",
            code="DF",
            is_default=True,
            is_mandatory=False,
            has_all_entity_access=True,
        )
        db.add_all([owner, cat_default])
        db.flush()

        manual_method = (
            db.query(IssuingMethod).filter(IssuingMethod.name == "Manual Delivery").first()
            or fx.issuing_method
        )
        db.commit()

        # 1. New AI-Scanned LG with 1 user-edited field & 1 manual field
        lg_new = _seed_lg_row(
            db,
            fx,
            owner,
            cat_default,
            manual_method,
            lg_number=f"LG-PROV-NEW-{uuid.uuid4().hex[:6]}",
        )
        post_resp = client.post(
            f"/api/v1/field-provenance/lg-records/{lg_new.id}",
            headers=headers_maker,
            json={
                "record_origin": "AI_SCAN",
                "field_sources": {
                    "lg_number": "AI_SCAN",
                    "issuer_name": "AI_SCAN",
                    "lg_amount": "USER_EDITED_AI",
                    "lg_currency_id": "AI_SCAN",
                    "issuance_date": "AI_SCAN",
                    "expiry_date": "AI_SCAN",
                    "description_purpose": "USER_MANUAL",
                    "issuing_method_id": "SYSTEM_DEFAULT",
                    "lg_category_id": "SYSTEM_DEFAULT",
                    "internal_owner_email": "USER_MANUAL",  # Untracked internal field -> filtered out
                },
            },
        )
        assert post_resp.status_code == 200, post_resp.text
        body = post_resp.json()
        assert body["tracking_mode"] == "EXACT"
        assert body["record_origin"] == "AI_SCAN"
        assert body["field_sources"]["lg_number"] == "AI_SCAN"
        assert body["field_sources"]["lg_amount"] == "USER_EDITED_AI"
        assert body["field_sources"]["description_purpose"] == "USER_MANUAL"
        assert "internal_owner_email" not in body["field_sources"]
        print("[PASS] 1. Exact provenance saved & sanitized for newly created LG")

        # 2. Corporate Admin read + AuditLog Amendment & Category overlay
        db.add(
            AuditLog(
                user_id=fx.maker_user.id,
                customer_id=fx.cust_alpha.id,
                lg_record_id=lg_new.id,
                entity_type="LGRecord",
                entity_id=lg_new.id,
                action_type=AUDIT_ACTION_TYPE_LG_AMENDED,
                details={
                    "lg_number": lg_new.lg_number,
                    "amended_fields": {"expiry_date": {"old": "2027-01-01", "new": "2027-06-01"}},
                },
            )
        )
        db.add(
            AuditLog(
                user_id=fx.maker_user.id,
                customer_id=fx.cust_alpha.id,
                lg_record_id=lg_new.id,
                entity_type="LGRecord",
                entity_id=lg_new.id,
                action_type=AUDIT_ACTION_TYPE_LG_SINGLE_CATEGORY_CHANGED,
                details={"lg_number": lg_new.lg_number},
            )
        )
        db.commit()

        get_resp = client.get(
            f"/api/v1/field-provenance/lg-records/{lg_new.id}",
            headers=headers_admin,
        )
        assert get_resp.status_code == 200, get_resp.text
        gbody = get_resp.json()
        assert gbody["field_sources"]["expiry_date"] == "USER_AMENDED"
        assert gbody["field_sources"]["lg_category_id"] == "USER_AMENDED"
        assert gbody["field_sources"]["lg_number"] == "AI_SCAN"
        print("[PASS] 2. Post-creation AuditLog amendment & category change overlaid accurately")

        # 3. Cross-tenant & Subsidiary Entity isolation check
        cross_resp = client.get(
            f"/api/v1/field-provenance/lg-records/{lg_new.id}",
            headers=headers_beta,
        )
        assert cross_resp.status_code == 404

        lg_sub = _seed_lg_row(
            db,
            fx,
            owner,
            cat_default,
            manual_method,
            lg_number=f"LG-SUB-{uuid.uuid4().hex[:6]}",
            entity=fx.entity_alpha_sub,
        )
        sub_resp = client.get(
            f"/api/v1/field-provenance/lg-records/{lg_sub.id}",
            headers=headers_restricted,
        )
        assert sub_resp.status_code == 404
        print("[PASS] 3. Cross-tenant & restricted-subsidiary provenance access strictly rejected (404)")

        # 4. Old Existing Record WITHOUT AI Scan -> MANUAL_LEGACY correlation
        lg_old_manual = _seed_lg_row(
            db,
            fx,
            owner,
            cat_default,
            manual_method,
            lg_number=f"LG-OLD-MAN-{uuid.uuid4().hex[:6]}",
            auto_renewal=False,
            other_conditions="Payable at sight",
        )
        old_man_resp = client.get(
            f"/api/v1/field-provenance/lg-records/{lg_old_manual.id}",
            headers=headers_maker,
        )
        assert old_man_resp.status_code == 200, old_man_resp.text
        om = old_man_resp.json()
        assert om["tracking_mode"] == "LEGACY_CORRELATED"
        assert om["record_origin"] == "MANUAL_LEGACY"
        assert om["field_sources"]["lg_number"] == "USER_MANUAL"
        assert om["field_sources"]["lg_amount"] == "USER_MANUAL"
        assert om["field_sources"]["other_conditions"] == "USER_MANUAL"
        assert om["field_sources"]["auto_renewal"] == "USER_EDITED_DEFAULT"
        if manual_method.name == "Manual Delivery":
            assert "issuing_method_id" not in om["field_sources"]
        assert "lg_category_id" not in om["field_sources"]  # Default category kept clean
        print("[PASS] 4. Old unscanned LG correlated as MANUAL_LEGACY with manual & non-default marks")

        # 5. Old Existing Record WITH AI Scan -> AI_SCAN_LEGACY (unmarked unless amended)
        lg_old_ai = _seed_lg_row(
            db,
            fx,
            owner,
            cat_default,
            manual_method,
            lg_number=f"LG-OLD-AI-{uuid.uuid4().hex[:6]}",
        )
        doc_fname = f"scan_{uuid.uuid4().hex[:6]}.pdf"
        db.add(
            LGDocument(
                lg_record_id=lg_old_ai.id,
                document_type=DOCUMENT_TYPE_ORIGINAL_LG,
                file_name=doc_fname,
                file_path=f"gs://test/{doc_fname}",
                mime_type="application/pdf",
                uploaded_by_user_id=fx.maker_user.id,
                status="FINAL",
            )
        )
        db.add(
            AIUsageLog(
                customer_id=fx.cust_alpha.id,
                user_id=fx.maker_user.id,
                doc_name=doc_fname,
                model_name="gemini-3.8-flash",
                total_tokens=420,
            )
        )
        db.commit()

        old_ai_resp = client.get(
            f"/api/v1/field-provenance/lg-records/{lg_old_ai.id}",
            headers=headers_maker,
        )
        assert old_ai_resp.status_code == 200, old_ai_resp.text
        oa = old_ai_resp.json()
        assert oa["tracking_mode"] == "LEGACY_CORRELATED"
        assert oa["record_origin"] == "AI_SCAN_LEGACY"
        assert oa["field_sources"] == {}
        print("[PASS] 5. Old AI-scanned LG correlated as AI_SCAN_LEGACY (initial OCR fields kept clean)")

        # 6. Migrated ('LEGACY') record -> LEGACY_MIGRATED (clean unless amended)
        lg_migrated = _seed_lg_row(
            db,
            fx,
            owner,
            cat_default,
            manual_method,
            lg_number=f"LG-MIG-{uuid.uuid4().hex[:6]}",
            migration_source="LEGACY",
        )
        mig_resp = client.get(
            f"/api/v1/field-provenance/lg-records/{lg_migrated.id}",
            headers=headers_maker,
        )
        assert mig_resp.status_code == 200, mig_resp.text
        mg = mig_resp.json()
        assert mg["tracking_mode"] == "LEGACY_MIGRATED"
        assert mg["field_sources"] == {}
        print("[PASS] 6. Migrated LEGACY record kept clean unless later amended")

        print("\nALL 6 LG FIELD PROVENANCE SIDECAR TESTS PASSED!")
    finally:
        if fx is not None:
            fx.cleanup()
        db.close()


if __name__ == "__main__":
    run_suite()
