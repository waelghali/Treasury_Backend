# tests/_test_harness.py
"""
Shared End-to-End Test Harness for Grow Treasury Platform Suites.
Provides:
- UTF-8 console output safety on Windows
- InMemoryGCSClient (runs real 64 KB STREAM V2 AES-256-GCM encryption/decryption in memory with 0ms latency)
- EmailCaptureHarness (captures all outbound emails and returns EmailSendResult(True, None))
- JWT auth header builders for END_USER, CHECKER, CORPORATE_ADMIN, and SYSTEM_OWNER
- Real WeasyPrint PDF text/page extractor (via PyMuPDF / fitz) & QR / Security PIN extractor
- Idempotent isolated multi-tenant Custody & Treasury fixture seeding + cascading cleanup
"""

import io
import os
import re
import sys
import uuid
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

# Ensure console supports UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

# Self-heal any orphaned 'idle in transaction' or blocked DDL sessions before app.main runs ALTER TABLE
try:
    from sqlalchemy import create_engine as _ce, text as _txt
    _db_url = os.getenv("DATABASE_URL")
    if _db_url:
        _tmp_eng = _ce(_db_url)
        with _tmp_eng.connect() as _c:
            _stale = _c.execute(
                _txt(
                    "SELECT pid FROM pg_stat_activity "
                    "WHERE datname = current_database() AND pid != pg_backend_pid() "
                    "AND ((state = 'idle in transaction' AND now() - state_change > interval '10 seconds') "
                    "     OR (query LIKE 'ALTER TABLE%' AND now() - query_start > interval '10 seconds'))"
                )
            ).fetchall()
            for (_pid,) in _stale:
                _c.execute(_txt("SELECT pg_terminate_backend(:p)"), {"p": _pid})
            _c.commit()
        _tmp_eng.dispose()
except Exception:
    pass

import app.main  # Triggers DB schema verification & template sync
from app.main import app
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal, engine
from app.constants import UserRole, SubscriptionStatus
from app.core.security import create_access_token
from app.core.hashing import get_password_hash
from app.core.email_service import EmailSendResult
from app.models.models import (
    Customer,
    CustomerEntity,
    SubscriptionPlan,
    User,
    UserCustomerEntityAssociation,
    Bank,
    Currency,
    LgType,
    LgStatus,
    LgOperationalStatus,
    IssuingMethod,
    Rule,
    LGCategory,
    InternalOwnerContact,
    LGRecord,
    LGInstruction,
    LGDocument,
    ApprovalRequest,
    ApprovalRequestStep,
    AuditLog,
    LGMigrationStaging,
    LGChangeLog,
    MigrationBatch,
    CustomerConfiguration,
    CustomerEmailSetting,
    Department,
    ApprovalGroup,
)
from app.models.models_issuance import AdminChangeRequest


# ======================================================================================
# 1. IN-MEMORY GCS HARNESS (Runs 100% Real AES-256-GCM STREAM V2 Crypto on Memory Blobs)
# ======================================================================================
class InMemoryGCSBlob:
    def __init__(self, bucket_name: str, name: str, store: Dict[str, "InMemoryGCSBlob"]):
        self.bucket_name = bucket_name
        self.name = name
        self._store = store
        self.data: bytes = b""
        self.content_type: str = "application/octet-stream"
        self.size: int = 0
        self.updated: Optional[datetime] = None
        self.md5_hash: str = ""

    @property
    def _key(self) -> str:
        return f"{self.bucket_name}/{self.name}"

    def exists(self) -> bool:
        return self._key in self._store

    def open(self, mode: str = "rb", content_type: Optional[str] = None):
        if content_type:
            self.content_type = content_type
        if "w" in mode:
            blob_ref = self

            class _MemWriter(io.RawIOBase):
                def __init__(self):
                    self._buf = io.BytesIO()

                def writable(self) -> bool:
                    return True

                def write(self, b) -> int:
                    return self._buf.write(b)

                def close(self):
                    if not self.closed:
                        raw = self._buf.getvalue()
                        blob_ref.data = raw
                        blob_ref.size = len(raw)
                        blob_ref.updated = datetime.now(timezone.utc)
                        blob_ref._store[blob_ref._key] = blob_ref
                    super().close()

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    self.close()

            return _MemWriter()
        else:
            existing = self._store.get(self._key)
            raw = existing.data if existing else self.data
            return io.BytesIO(raw)

    def upload_from_string(self, data: Any, content_type: Optional[str] = None):
        raw = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        self.data = raw
        self.size = len(raw)
        if content_type:
            self.content_type = content_type
        self.updated = datetime.now(timezone.utc)
        self._store[self._key] = self

    def upload_from_file(self, file_obj: Any, content_type: Optional[str] = None):
        self.upload_from_string(file_obj.read(), content_type=content_type)

    def download_as_bytes(self) -> bytes:
        existing = self._store.get(self._key)
        if not existing:
            raise FileNotFoundError(f"InMemoryGCSBlob not found: {self._key}")
        return existing.data

    def delete(self):
        self._store.pop(self._key, None)

    def generate_signed_url(self, version: str = "v4", expiration: Any = None, method: str = "GET", **kwargs) -> str:
        quoted = urllib.parse.quote(self.name, safe="/")
        return f"https://storage.googleapis.com/{self.bucket_name}/{quoted}?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Signature=mock_sig"


class InMemoryGCSBucket:
    def __init__(self, name: str, store: Dict[str, InMemoryGCSBlob]):
        self.name = name
        self._store = store

    def blob(self, blob_name: str) -> InMemoryGCSBlob:
        key = f"{self.name}/{blob_name}"
        if key in self._store:
            return self._store[key]
        return InMemoryGCSBlob(self.name, blob_name, self._store)

    def list_blobs(self, prefix: str = "") -> List[InMemoryGCSBlob]:
        full_prefix = f"{self.name}/{prefix}"
        return [b for k, b in self._store.items() if k.startswith(full_prefix)]


class InMemoryGCSClient:
    def __init__(self):
        self.blobs: Dict[str, InMemoryGCSBlob] = {}

    def bucket(self, name: str) -> InMemoryGCSBucket:
        return InMemoryGCSBucket(name, self.blobs)

    def get_raw_bytes_by_uri(self, gcs_uri: str) -> bytes:
        assert gcs_uri.startswith("gs://"), f"Expected gs:// URI, got {gcs_uri}"
        key = gcs_uri[5:]
        if key not in self.blobs:
            raise KeyError(f"GCS URI {gcs_uri} not found in InMemoryGCSClient. Keys: {list(self.blobs.keys())[:10]}")
        return self.blobs[key].data


def install_in_memory_gcs() -> InMemoryGCSClient:
    """Patches GCS client across storage_service and ai_integration with an in-memory vault."""
    mem_client = InMemoryGCSClient()
    import app.core.storage_service as _ss
    import app.core.ai_integration as _ai
    import app.crud.crud_approval_request as _car

    _ss._get_gcs_client = lambda: mem_client
    _ai._get_gcs_client = lambda: mem_client
    _ai._get_genai_client = lambda: None  # Prevent live paid Gemini calls during offline tests
    if not _ss.GCS_BUCKET_NAME:
        _ss.GCS_BUCKET_NAME = "lg_custody_bucket"
    if not _ai.GCS_BUCKET_NAME:
        _ai.GCS_BUCKET_NAME = "lg_custody_bucket"

    def _mock_delete_file_from_gcs(gcs_uri: str) -> bool:
        if gcs_uri and gcs_uri.startswith("gs://"):
            mem_client.blobs.pop(gcs_uri[5:], None)
        return True

    _ai.delete_file_from_gcs = _mock_delete_file_from_gcs
    _car.delete_file_from_gcs = _mock_delete_file_from_gcs
    return mem_client


# ======================================================================================
# 2. EMAIL CAPTURE HARNESS
# ======================================================================================
class EmailCaptureHarness:
    def __init__(self):
        self.captured: List[Dict[str, Any]] = []

    def clear(self):
        self.captured.clear()

    def find_by_recipient(self, email: str) -> List[Dict[str, Any]]:
        target = (email or "").strip().lower()
        return [
            e for e in self.captured
            if any(target == str(addr).strip().lower() for addr in (e.get("to_emails") or []) + (e.get("cc_emails") or []))
        ]

    async def mock_send_email(
        self,
        db: Any = None,
        to_emails: Any = None,
        subject_template: str = "",
        body_template: str = "",
        template_data: Optional[Dict[str, Any]] = None,
        email_settings: Any = None,
        sender_name: Optional[str] = None,
        cc_emails: Any = None,
        bcc_emails: Any = None,
        attachments: Any = None,
        attachment_paths: Any = None,
        save_copy: bool = True,
        allow_platform_fallback: bool = True,
        **kwargs,
    ) -> EmailSendResult:
        to_list = list(to_emails) if isinstance(to_emails, (list, tuple, set)) else ([to_emails] if to_emails else [])
        cc_list = list(cc_emails) if isinstance(cc_emails, (list, tuple, set)) else ([cc_emails] if cc_emails else [])
        self.captured.append({
            "to_emails": to_list,
            "cc_emails": cc_list,
            "subject": subject_template,
            "body": body_template,
            "sender_name": sender_name,
            "attachments_count": len(attachments or []) + len(attachment_paths or []),
            "allow_platform_fallback": allow_platform_fallback,
        })
        return EmailSendResult(True, None)


def install_email_capture() -> EmailCaptureHarness:
    harness = EmailCaptureHarness()
    targets = [
        "app.core.email_service",
        "app.crud.crud_lg_record",
        "app.crud.crud_lg_instruction",
        "app.crud.crud_approval_request",
        "app.crud.crud_lg_owner",
        "app.api.v1.endpoints.end_user",
        "app.api.v1.endpoints.corporate_admin",
        "app.services.customer_onboarding_service",
        "app.auth_v2.services",
        "app.core.background_tasks",
    ]
    for mod_name in targets:
        mod = sys.modules.get(mod_name)
        if mod is None:
            try:
                __import__(mod_name)
                mod = sys.modules.get(mod_name)
            except Exception:
                mod = None
        if mod is not None and hasattr(mod, "send_email"):
            setattr(mod, "send_email", harness.mock_send_email)
    return harness


# ======================================================================================
# 3. AUTH & PDF / QR EXTRACTION HELPERS
# ======================================================================================
def make_auth_headers(user: User, is_mfa_verified: bool = True, extra_claims: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    role_str = user.role.value if hasattr(user.role, "value") else str(user.role)
    payload = {
        "sub": user.email,
        "user_id": user.id,
        "role": role_str,
        "customer_id": user.customer_id,
        "has_custody_module": True,
        "has_issuance_module": True,
        "has_quotation_module": True,
        "has_reconciliation_module": True,
        "has_all_entity_access": bool(getattr(user, "has_all_entity_access", True)),
        "is_mfa_verified": is_mfa_verified,
    }
    if extra_claims:
        payload.update(extra_claims)
    tok = create_access_token(data=payload, expires_delta=timedelta(hours=2))
    return {"Authorization": f"Bearer {tok}"}


def extract_pdf_info(pdf_bytes: bytes) -> Dict[str, Any]:
    """Parses real rendered PDF bytes via PyMuPDF (fitz) and returns page count + extracted text."""
    import fitz
    assert pdf_bytes and pdf_bytes.startswith(b"%PDF"), "Invalid PDF bytes (missing %PDF magic header)"
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        pages_text = [doc[i].get_text() for i in range(doc.page_count)]
        return {
            "page_count": doc.page_count,
            "pages_text": pages_text,
            "text": "\n---PAGE---\n".join(pages_text),
        }
    finally:
        doc.close()


def extract_qr_token_and_pin_from_html(html_content: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts the v2 cryptographic QR token (`t=I2.<id>.<sig>`) or Pin from rendered HTML.
    Note: In HTML, the QR SVG encodes the URL, while the 4-digit PIN is printed in a monospace span
    inside `#grow-mandatory-security-footer`.
    """
    pin_match = re.search(
        r"Security Code:\s*<span[^>]*>\s*([0-9\-]{4,7})\s*</span>",
        html_content,
        flags=re.IGNORECASE | re.DOTALL,
    )
    pin = pin_match.group(1).strip() if pin_match else None
    return pin


# ======================================================================================
# 4. ISOLATED MULTI-TENANT CUSTODY & PLATFORM TEST FIXTURE
# ======================================================================================
class IsolatedTenantFixture:
    """
    Creates two completely isolated test tenants (Tenant Alpha & Tenant Beta) with:
    - Full-featured SubscriptionPlan (and ability to toggle flags/quotas in tests)
    - 2 Corporate Entities on Tenant Alpha (ALPH, SUBB) + 1 Entity on Tenant Beta (BETA)
    - Maker (all entities), Restricted Maker (ALPH only), Checker, Admin 1, Admin 2, System Owner
    - Customer-specific LGCategory with mandatory custom field + standard LGCategory
    - Local Banks (CIB, HSBC, QNB), Foreign Bank ("Foreign Bank"), Currencies (EGP, USD, EUR)
    - Guaranteed cascading cleanup of every record created under these test tenants.
    """

    def __init__(self, db: Session, tag: str = "SUITE"):
        self.db = db
        self.db.expire_on_commit = False
        self.uid = uuid.uuid4().hex[:6].upper()
        self.tag = f"{tag}_{self.uid}"
        self._build()

    def _build(self):
        now_utc = datetime.now(timezone.utc)
        pw_hash = get_password_hash("StrongPass!2026")

        # 1. Subscription Plan
        self.plan = SubscriptionPlan(
            name=f"Enterprise Suite Plan {self.tag}",
            description="Automated E2E verification plan",
            duration_months=12,
            monthly_price=1000.0,
            annual_price=10000.0,
            max_users=20,
            max_checker_users=3,
            max_records=50,
            max_issuance_records=50,
            can_maker_checker=True,
            can_multi_entity=True,
            can_ai_integration=True,
            can_image_storage=True,
            can_email_inbox=True,
            has_custody_module=True,
            has_issuance_module=True,
            has_quotation_module=True,
            has_reconciliation_module=True,
            grace_period_days=30,
        )
        self.db.add(self.plan)
        self.db.flush()

        # 2. Customers (Tenant Alpha & Tenant Beta)
        self.cust_alpha = Customer(
            name=f"Alpha Custody Corp {self.tag}",
            address="100 Nile Corniche, Cairo, Egypt",
            contact_email=f"treasury.alpha.{self.uid.lower()}@globex.com",
            contact_phone="+201000000001",
            subscription_plan_id=self.plan.id,
            start_date=now_utc - timedelta(days=10),
            end_date=now_utc + timedelta(days=355),
            status=SubscriptionStatus.ACTIVE,
            domains=[f"alphacorp-{self.uid.lower()}.com"],
        )
        self.cust_beta = Customer(
            name=f"Beta Rival Corp {self.tag}",
            address="200 Smart Village, Giza, Egypt",
            contact_email=f"treasury.beta.{self.uid.lower()}@globex.com",
            contact_phone="+201000000002",
            subscription_plan_id=self.plan.id,
            start_date=now_utc - timedelta(days=10),
            end_date=now_utc + timedelta(days=355),
            status=SubscriptionStatus.ACTIVE,
            domains=[f"betacorp-{self.uid.lower()}.com"],
        )
        self.db.add_all([self.cust_alpha, self.cust_beta])
        self.db.flush()

        # 3. Entities (4-char codes strictly alphanumeric uppercase)
        c1 = f"A{self.uid[:3]}"
        c2 = f"S{self.uid[:3]}"
        c3 = f"B{self.uid[:3]}"
        self.entity_alpha_hq = CustomerEntity(
            customer_id=self.cust_alpha.id,
            entity_name=f"Alpha HQ SAE {self.uid}",
            code=c1,
            address="Tower 1, New Cairo",
            contact_email=f"hq.alpha.{self.uid.lower()}@globex.com",
            is_active=True,
        )
        self.entity_alpha_sub = CustomerEntity(
            customer_id=self.cust_alpha.id,
            entity_name=f"Alpha Subsidiary Power {self.uid}",
            code=c2,
            address="Zone 4, Ain Sokhna",
            contact_email=f"sub.alpha.{self.uid.lower()}@globex.com",
            is_active=True,
        )
        self.entity_beta_hq = CustomerEntity(
            customer_id=self.cust_beta.id,
            entity_name=f"Beta HQ {self.uid}",
            code=c3,
            address="Giza Plaza",
            contact_email=f"hq.beta.{self.uid.lower()}@globex.com",
            is_active=True,
        )
        self.db.add_all([self.entity_alpha_hq, self.entity_alpha_sub, self.entity_beta_hq])
        self.db.flush()

        # 4. Users
        u_sfx = self.uid.lower()
        self.maker_user = User(
            email=f"maker.{u_sfx}@globex.com",
            password_hash=pw_hash,
            role=UserRole.END_USER,
            customer_id=self.cust_alpha.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        self.restricted_maker = User(
            email=f"restricted.{u_sfx}@globex.com",
            password_hash=pw_hash,
            role=UserRole.END_USER,
            customer_id=self.cust_alpha.id,
            must_change_password=False,
            has_all_entity_access=False,
        )
        self.checker_user = User(
            email=f"checker.{u_sfx}@globex.com",
            password_hash=pw_hash,
            role=UserRole.CHECKER,
            customer_id=self.cust_alpha.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        self.admin_user = User(
            email=f"admin1.{u_sfx}@globex.com",
            password_hash=pw_hash,
            role=UserRole.CORPORATE_ADMIN,
            customer_id=self.cust_alpha.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        self.admin2_user = User(
            email=f"admin2.{u_sfx}@globex.com",
            password_hash=pw_hash,
            role=UserRole.CORPORATE_ADMIN,
            customer_id=self.cust_alpha.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        self.beta_maker = User(
            email=f"beta.maker.{u_sfx}@globex.com",
            password_hash=pw_hash,
            role=UserRole.END_USER,
            customer_id=self.cust_beta.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        self.beta_admin = User(
            email=f"beta.admin.{u_sfx}@globex.com",
            password_hash=pw_hash,
            role=UserRole.CORPORATE_ADMIN,
            customer_id=self.cust_beta.id,
            must_change_password=False,
            has_all_entity_access=True,
        )
        self.sys_owner = self.db.query(User).filter(
            User.role == UserRole.SYSTEM_OWNER,
            User.is_deleted == False,
        ).first()
        self._created_sys_owner = False
        if not self.sys_owner:
            self.sys_owner = User(
                email=f"sysowner.{u_sfx}@globex.com",
                password_hash=pw_hash,
                role=UserRole.SYSTEM_OWNER,
                customer_id=None,
                must_change_password=False,
                has_all_entity_access=True,
            )
            self.db.add(self.sys_owner)
            self._created_sys_owner = True

        self.db.add_all([
            self.maker_user,
            self.restricted_maker,
            self.checker_user,
            self.admin_user,
            self.admin2_user,
            self.beta_maker,
            self.beta_admin,
        ])
        self.db.flush()

        # Bind restricted_maker strictly to entity_alpha_hq ONLY (NOT entity_alpha_sub)
        assoc = UserCustomerEntityAssociation(
            user_id=self.restricted_maker.id,
            customer_entity_id=self.entity_alpha_hq.id,
            is_deleted=False,
        )
        self.db.add(assoc)
        self.db.flush()

        # 5. Categories for Tenant Alpha & Beta
        self.cat_standard = LGCategory(
            customer_id=self.cust_alpha.id,
            name=f"Standard Suppliers {self.uid}",
            code="PR",
            is_mandatory=False,
            has_all_entity_access=True,
            communication_list=[f"contracts.{u_sfx}@globex.com"],
        )
        self.cat_mandatory_extra = LGCategory(
            customer_id=self.cust_alpha.id,
            name=f"Strategic Projects {self.uid}",
            code="SP",
            extra_field_name="project_wbs_code",
            is_mandatory=True,
            has_all_entity_access=True,
        )
        self.cat_beta = LGCategory(
            customer_id=self.cust_beta.id,
            name=f"Beta Default {self.uid}",
            code="BT",
            is_mandatory=False,
            has_all_entity_access=True,
        )
        self.db.add_all([self.cat_standard, self.cat_mandatory_extra, self.cat_beta])
        self.db.flush()

        # 6. Master Lookups (Currencies, LG Types, Statuses, Rules, Issuing Methods, Banks)
        self.ccy_egp = self._ensure_currency("EGP", "Egyptian Pound", "EGP")
        self.ccy_usd = self._ensure_currency("USD", "US Dollar", "$")
        self.ccy_eur = self._ensure_currency("EUR", "Euro", "EUR")

        self.lg_type_perf = self.db.query(LgType).filter(LgType.id == 1).first() or self.db.query(LgType).first()
        self.lg_type_apg = self.db.query(LgType).filter(LgType.id == 3).first()
        if not self.lg_type_apg:
            self.lg_type_apg = self.db.query(LgType).filter(LgType.name.ilike("%Advance%")).first()

        self.op_operative = self.db.query(LgOperationalStatus).filter(LgOperationalStatus.id == 1).first()
        self.op_non_operative = self.db.query(LgOperationalStatus).filter(LgOperationalStatus.id == 2).first()

        self.rule_urdg = self.db.query(Rule).filter(Rule.name != "Other").first()
        self.rule_other = self.db.query(Rule).filter(Rule.name == "Other").first()
        self.issuing_method = self.db.query(IssuingMethod).first()

        self.bank_cib = self._ensure_bank("Commercial International Bank", "CIB", "cibeg.com", "Smart Village, Giza")
        self.bank_hsbc = self._ensure_bank("HSBC Bank Egypt", "HSBC", "hsbc.com", "306 Corniche El Nil, Maadi")
        self.bank_qnb = self._ensure_bank("QNB Al Ahli", "QNB", "qnb.com.eg", "Dar El Salam, Cairo")
        self.bank_foreign = self.db.query(Bank).filter(Bank.name == "Foreign Bank", Bank.is_deleted == False).first()
        if not self.bank_foreign:
            self.bank_foreign = Bank(
                name="Foreign Bank",
                short_name="FRGN",
                address="International Counterparty",
                phone_number="N/A",
            )
            self.db.add(self.bank_foreign)
            self.db.flush()

        self.db.commit()

    def _ensure_currency(self, iso: str, name: str, symbol: str) -> Currency:
        c = self.db.query(Currency).filter(Currency.iso_code == iso).first()
        if not c:
            c = Currency(iso_code=iso, name=name, symbol=symbol)
            self.db.add(c)
            self.db.flush()
        return c

    def _ensure_bank(self, name: str, short_name: str, domain: str, address: str) -> Bank:
        b = self.db.query(Bank).filter(
            (Bank.short_name == short_name) | (Bank.name.ilike(f"%{name}%")),
            Bank.is_deleted == False,
        ).first()
        if not b:
            b = Bank(
                name=f"{name} ({self.uid})",
                short_name=short_name,
                email_domain=domain,
                address=address,
                phone_number="+20219666",
            )
            self.db.add(b)
            self.db.flush()
        else:
            if not b.address:
                b.address = address
                self.db.flush()
        return b

    def cleanup(self):
        """Cascading cleanup of all test rows created for cust_alpha and cust_beta."""
        try:
            purge_test_customers_by_ids(
                self.db,
                cust_ids=[self.cust_alpha.id, self.cust_beta.id],
                plan_ids=[self.plan.id],
                extra_sys_owner_id=self.sys_owner.id if (self._created_sys_owner and self.sys_owner) else None,
            )
        except Exception as e:
            self.db.rollback()
            print(f"[WARN] Cleanup warning in IsolatedTenantFixture: {e}")
        finally:
            try:
                engine.dispose()
            except Exception:
                pass


def purge_test_customers_by_ids(
    db: Session,
    cust_ids: List[int],
    plan_ids: Optional[List[int]] = None,
    extra_sys_owner_id: Optional[int] = None,
) -> None:
    """Deletes all dependent child rows in topological FK order for the given test customer IDs."""
    if not cust_ids and not plan_ids:
        return
    db.rollback()
    stmts = [
        # Notifications & AI / Email history
        "DELETE FROM notification_deliveries WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM user_notifications WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids)) OR actor_user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM ai_usage_logs WHERE customer_id = ANY(:cids) OR user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM email_classification_history WHERE customer_id = ANY(:cids) OR confirmed_by_user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM system_notification_view_logs WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids)) OR notification_id IN (SELECT id FROM system_notifications WHERE created_by_user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids)))",
        "DELETE FROM system_notifications WHERE created_by_user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        # Campaigns & Cashback
        "DELETE FROM cashback_claims WHERE customer_id = ANY(:cids) OR verified_by_user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM campaign_entries WHERE customer_id = ANY(:cids)",
        # LG Position Reconciliation
        "DELETE FROM reconciliation_results WHERE session_id IN (SELECT id FROM reconciliation_sessions WHERE customer_id = ANY(:cids))",
        "DELETE FROM reconciliation_bank_rows WHERE session_id IN (SELECT id FROM reconciliation_sessions WHERE customer_id = ANY(:cids))",
        "DELETE FROM reconciliation_sessions WHERE customer_id = ANY(:cids)",
        "DELETE FROM bank_column_mappings WHERE customer_id = ANY(:cids)",
        # Issuance, Facilities & Maintenance
        "DELETE FROM issuance_maintenance_actions WHERE issued_lg_id IN (SELECT id FROM issued_lg_records WHERE customer_id = ANY(:cids))",
        "DELETE FROM issuance_exposure_entries WHERE facility_id IN (SELECT id FROM facilities WHERE customer_id = ANY(:cids)) OR lg_record_id IN (SELECT id FROM issued_lg_records WHERE customer_id = ANY(:cids)) OR request_id IN (SELECT id FROM issuance_requests WHERE customer_id = ANY(:cids))",
        "UPDATE issuance_requests SET lg_record_id = NULL, selected_sub_limit_id = NULL WHERE customer_id = ANY(:cids)",
        "UPDATE issued_lg_records SET sub_limit_id = NULL, facility_id = NULL, request_id = NULL WHERE customer_id = ANY(:cids)",
        "DELETE FROM issued_lg_records WHERE customer_id = ANY(:cids)",
        "DELETE FROM issuance_request_snapshots WHERE request_id IN (SELECT id FROM issuance_requests WHERE customer_id = ANY(:cids))",
        "DELETE FROM issuance_request_versions WHERE request_id IN (SELECT id FROM issuance_requests WHERE customer_id = ANY(:cids))",
        "DELETE FROM issuance_request_documents WHERE request_id IN (SELECT id FROM issuance_requests WHERE customer_id = ANY(:cids))",
        "DELETE FROM issuance_requests WHERE customer_id = ANY(:cids)",
        "DELETE FROM issuance_facility_audit_logs WHERE facility_id IN (SELECT id FROM facilities WHERE customer_id = ANY(:cids))",
        "DELETE FROM issuance_facility_sub_limits WHERE facility_id IN (SELECT id FROM facilities WHERE customer_id = ANY(:cids))",
        "DELETE FROM facility_entities WHERE facility_id IN (SELECT id FROM facilities WHERE customer_id = ANY(:cids)) OR entity_id IN (SELECT id FROM customer_entities WHERE customer_id = ANY(:cids))",
        "DELETE FROM facilities WHERE customer_id = ANY(:cids)",
        "DELETE FROM issuance_workflow_policies WHERE customer_id = ANY(:cids)",
        "DELETE FROM customer_form_configurations WHERE customer_id = ANY(:cids)",
        "DELETE FROM form_field_user_values WHERE customer_id = ANY(:cids)",
        "DELETE FROM bank_live_ranking_configs WHERE customer_id = ANY(:cids)",
        "DELETE FROM bank_form_issue_reports WHERE customer_id = ANY(:cids)",
        "DELETE FROM lg_applicant_records WHERE customer_id = ANY(:cids)",
        # Smart Inbox & Bank Statement Reconciliation v2
        "DELETE FROM cash_balance_snapshots WHERE customer_id = ANY(:cids)",
        "DELETE FROM inbox_sync_tokens WHERE customer_id = ANY(:cids)",
        "DELETE FROM inbox_attachments WHERE inbox_item_id IN (SELECT id FROM inbox_items WHERE customer_id = ANY(:cids))",
        "DELETE FROM inbox_items WHERE customer_id = ANY(:cids)",
        "DELETE FROM inbox_outbound_requests WHERE customer_id = ANY(:cids)",
        "DELETE FROM inbox_schedule_configs WHERE customer_id = ANY(:cids)",
        "DELETE FROM reconciliation_matches WHERE bank_txn_id IN (SELECT id FROM bank_transactions WHERE statement_id IN (SELECT id FROM bank_statements WHERE company_id = ANY(:cids)))",
        "UPDATE internal_ledger_records SET matched_bank_txn_id = NULL WHERE company_id = ANY(:cids)",
        "DELETE FROM internal_ledger_records WHERE company_id = ANY(:cids)",
        "UPDATE bank_transactions SET linked_txn_id = NULL WHERE statement_id IN (SELECT id FROM bank_statements WHERE company_id = ANY(:cids))",
        "DELETE FROM bank_transactions WHERE statement_id IN (SELECT id FROM bank_statements WHERE company_id = ANY(:cids))",
        "DELETE FROM bank_statements WHERE company_id = ANY(:cids)",
        "DELETE FROM classification_rules WHERE company_id = ANY(:cids)",
        "DELETE FROM classification_taxonomy WHERE company_id = ANY(:cids)",
        "DELETE FROM reconciliation_counterparties WHERE company_id = ANY(:cids)",
        "DELETE FROM reconciliation_matching_rules WHERE company_id = ANY(:cids)",
        "DELETE FROM matching_tolerance_rules WHERE company_id = ANY(:cids)",
        "DELETE FROM reconciliation_export_profiles WHERE company_id = ANY(:cids)",
        "DELETE FROM reconciliation_input_mappings WHERE company_id = ANY(:cids)",
        "DELETE FROM reconciliation_account_profiles WHERE company_id = ANY(:cids)",
        "DELETE FROM bank_account_profiles WHERE company_id = ANY(:cids)",
        "DELETE FROM collaborative_tenant_votes WHERE company_id = ANY(:cids)",
        # Custody, Migration, Approvals & Audit Logs
        "DELETE FROM lg_change_log WHERE lg_id IN (SELECT id FROM lg_records WHERE customer_id = ANY(:cids))",
        "DELETE FROM lg_change_logs WHERE lg_id IN (SELECT id FROM lg_records WHERE customer_id = ANY(:cids))",
        "DELETE FROM lg_migration_staging WHERE customer_id = ANY(:cids) OR production_lg_id IN (SELECT id FROM lg_records WHERE customer_id = ANY(:cids))",
        "DELETE FROM migration_staging_records WHERE customer_id = ANY(:cids) OR created_by_user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM migration_batches WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM approval_request_steps WHERE approval_request_id IN (SELECT id FROM approval_requests WHERE customer_id = ANY(:cids)) OR approver_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "UPDATE approval_requests SET related_instruction_id = NULL WHERE customer_id = ANY(:cids)",
        "UPDATE lg_instructions SET approval_request_id = NULL WHERE lg_record_id IN (SELECT id FROM lg_records WHERE customer_id = ANY(:cids))",
        "UPDATE lg_documents SET lg_instruction_id = NULL WHERE lg_record_id IN (SELECT id FROM lg_records WHERE customer_id = ANY(:cids))",
        "DELETE FROM lg_documents WHERE lg_record_id IN (SELECT id FROM lg_records WHERE customer_id = ANY(:cids)) OR uploaded_by_user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM lg_instructions WHERE lg_record_id IN (SELECT id FROM lg_records WHERE customer_id = ANY(:cids))",
        "DELETE FROM templates WHERE customer_id = ANY(:cids)",
        "DELETE FROM approval_requests WHERE customer_id = ANY(:cids)",
        "DELETE FROM audit_logs WHERE customer_id = ANY(:cids) OR user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids)) OR lg_record_id IN (SELECT id FROM lg_records WHERE customer_id = ANY(:cids))",
        "DELETE FROM lg_records WHERE customer_id = ANY(:cids)",
        "DELETE FROM internal_owner_contacts WHERE customer_id = ANY(:cids)",
        "DELETE FROM lg_category_customer_entity_association WHERE lg_category_id IN (SELECT id FROM lg_categories WHERE customer_id = ANY(:cids)) OR customer_entity_id IN (SELECT id FROM customer_entities WHERE customer_id = ANY(:cids))",
        "UPDATE customers SET default_lg_category_id = NULL WHERE id = ANY(:cids)",
        "DELETE FROM lg_categories WHERE customer_id = ANY(:cids)",
        "DELETE FROM admin_change_requests WHERE customer_id = ANY(:cids)",
        "DELETE FROM customer_configurations WHERE customer_id = ANY(:cids)",
        "DELETE FROM customer_email_settings WHERE customer_id = ANY(:cids)",
        "DELETE FROM customer_bank_accounts WHERE customer_id = ANY(:cids)",
        "DELETE FROM approval_matrix_levels WHERE matrix_id IN (SELECT id FROM approval_matrices WHERE customer_id = ANY(:cids)) OR approver_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM approval_matrices WHERE customer_id = ANY(:cids)",
        "DELETE FROM user_approval_group_association WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM approval_groups WHERE customer_id = ANY(:cids)",
        "DELETE FROM corporate_projects WHERE customer_id = ANY(:cids)",
        "UPDATE departments SET manager_id = NULL WHERE customer_id = ANY(:cids)",
        "DELETE FROM departments WHERE customer_id = ANY(:cids)",
        # Users, Entities, Tenant Keys & Customers
        "DELETE FROM user_customer_entity_association WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids)) OR customer_entity_id IN (SELECT id FROM customer_entities WHERE customer_id = ANY(:cids))",
        "DELETE FROM user_devices WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM legal_acceptances WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM user_legal_acceptance WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM user_legal_acceptances WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM user_document_acceptance WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM user_mobile_pair_tokens WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM user_webauthn_credentials WHERE customer_id = ANY(:cids) OR user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM push_subscriptions WHERE customer_id = ANY(:cids) OR user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM user_feedbacks WHERE customer_id = ANY(:cids) OR user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM password_reset_tokens WHERE user_id IN (SELECT id FROM users WHERE customer_id = ANY(:cids))",
        "DELETE FROM quotation_tenant_keys WHERE customer_id = ANY(:cids)",
        "DELETE FROM users WHERE customer_id = ANY(:cids)",
        "DELETE FROM customer_entities WHERE customer_id = ANY(:cids)",
        "DELETE FROM customers WHERE id = ANY(:cids)",
    ]
    params = {"cids": cust_ids}
    for sql_str in stmts:
        try:
            db.execute(text(sql_str), params)
            db.commit()
        except Exception:
            db.rollback()

    if plan_ids:
        try:
            db.execute(text("DELETE FROM subscription_plans WHERE id = ANY(:pids)"), {"pids": list(plan_ids)})
            db.commit()
        except Exception:
            db.rollback()

    if extra_sys_owner_id:
        try:
            db.execute(text("DELETE FROM users WHERE id = :uid"), {"uid": extra_sys_owner_id})
            db.commit()
        except Exception:
            db.rollback()
