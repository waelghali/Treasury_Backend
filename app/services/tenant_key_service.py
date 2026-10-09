# app/services/tenant_key_service.py
"""
Grow Treasury — Tenant Key Store & Lifecycle Management Service (Phase 8.2).

Responsibilities:
1. Automated On-Demand Provisioning:
   - Retrieves or cryptographically generates a 256-bit Tenant Data Encryption Key (DEK).
   - Wraps (seals) DEK using the Master KEK bound to customer_id via Associated Authenticated Data.
   - Persists wrapped envelope in `quotation_tenant_keys`.
2. High-Throughput Memory Cache:
   - Thread-safe TTL cache prevents repeated Master KEK unwrapping on hot queries.
3. Dual-Read Safe Decryption Resolvers:
   - Decrypts financial quote rows seamlessly with legacy fallback for historical deals.
"""

import time
import uuid
import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.models.models_quotation import QuotationTenantKey, QuotationOffer, QuotationTBillOffer
from app.core.security_crypto import (
    get_key_provider,
    generate_tenant_dek,
    encrypt_field,
    decrypt_field,
    is_encrypted,
    CryptoError,
    TamperDetectedError,
    DecryptionError
)

logger = logging.getLogger(__name__)

# In-memory DEK Cache: { customer_id: {"dek": bytes, "cached_at": float} }
_DEK_CACHE: Dict[int, Dict[str, Any]] = {}
DEK_CACHE_TTL_SECONDS = 600  # 10 minutes cache TTL


class TenantKeyService:
    """Manages per-customer envelope encryption keys and dual-read financial resolution."""

    def get_or_create_tenant_dek(self, db: Session, customer_id: int) -> bytes:
        """
        Retrieves the 32-byte Tenant DEK for a customer, unsealing it from the database.
        If no key exists for this customer, generates a new one, wraps it via Master KEK,
        commits it, and returns the raw DEK.
        """
        now = time.time()
        cached = _DEK_CACHE.get(customer_id)
        if cached and (now - cached["cached_at"]) < DEK_CACHE_TTL_SECONDS:
            return cached["dek"]

        record = db.query(QuotationTenantKey).filter(
            QuotationTenantKey.customer_id == customer_id,
            QuotationTenantKey.status == "ACTIVE"
        ).first()

        provider = get_key_provider()
        tenant_context = str(customer_id)

        if record:
            dek = provider.unwrap_dek(record.wrapped_dek, tenant_context)
        else:
            # Generate new Tenant DEK and seal with Master KEK
            dek = generate_tenant_dek()
            wrapped_dek = provider.wrap_dek(dek, tenant_context)

            record = QuotationTenantKey(
                customer_id=customer_id,
                key_id=uuid.uuid4().hex,
                wrapped_dek=wrapped_dek,
                key_version=1,
                status="ACTIVE"
            )
            db.add(record)
            db.commit()
            db.refresh(record)
            logger.info(f"Initialized new Tenant DEK envelope for Customer ID {customer_id} (Key ID: {record.key_id})")

        _DEK_CACHE[customer_id] = {
            "dek": dek,
            "cached_at": now
        }
        return dek

    def clear_cache(self, customer_id: Optional[int] = None) -> None:
        """Clears in-memory DEK cache (e.g. after key rotation or during testing)."""
        if customer_id is not None:
            _DEK_CACHE.pop(customer_id, None)
        else:
            _DEK_CACHE.clear()

    # --- Dual-Read Resolvers for FX Spot Offers ---

    def resolve_offer_price(self, offer: QuotationOffer, dek: Optional[bytes] = None) -> float:
        """
        Resolves the final quote price.
        If encrypted_price is present and dek is provided, decrypts it.
        If dek is not provided or decryption is unneeded, returns legacy plaintext price.
        """
        if getattr(offer, "encrypted_price", None) and dek:
            try:
                decrypted = decrypt_field(offer.encrypted_price, dek, field_context="price")
                if decrypted is not None:
                    return float(decrypted)
            except Exception as e:
                logger.warning(f"Failed to decrypt offer {offer.id} price: {e}. Falling back to legacy column.")
        
        return float(offer.price) if offer.price is not None else 0.0

    def apply_encrypted_offer_price(self, offer: QuotationOffer, raw_price: float, dek: bytes) -> None:
        """Encrypts price into encrypted_price and ensures legacy plaintext column is NULL for pure ciphertext storage."""
        offer.encrypted_price = encrypt_field(float(raw_price), dek, field_context="price")
        offer.price = None

    # --- Dual-Read Resolvers for T-Bill Offers ---

    def resolve_tbill_discount_rate(self, tbill_offer: QuotationTBillOffer, dek: Optional[bytes] = None) -> float:
        """Resolves T-Bill discount rate using ciphertext when available, falling back to legacy column."""
        if getattr(tbill_offer, "encrypted_discount_rate", None) and dek:
            try:
                decrypted = decrypt_field(tbill_offer.encrypted_discount_rate, dek, field_context="discount_rate")
                if decrypted is not None:
                    return float(decrypted)
            except Exception as e:
                logger.warning(f"Failed to decrypt tbill discount rate for offer {tbill_offer.id}: {e}")

        return float(tbill_offer.discount_rate) if tbill_offer.discount_rate is not None else 0.0

    def apply_encrypted_tbill_offer(self, tbill_offer: QuotationTBillOffer, discount_rate: float, max_amount: float, dek: bytes) -> None:
        """Encrypts T-Bill terms and ensures legacy plaintext columns are NULL for pure ciphertext storage."""
        tbill_offer.encrypted_discount_rate = encrypt_field(float(discount_rate), dek, field_context="discount_rate")
        tbill_offer.encrypted_max_amount = encrypt_field(float(max_amount), dek, field_context="max_amount")
        tbill_offer.discount_rate = None
        tbill_offer.max_amount = None

    # --- Dual-Read Resolvers & Enveloping for Winning Deal Terms ---

    def apply_encrypted_leg_winner(self, leg: Any, win_rate: Optional[float], saved_vs_avg: Optional[float], dek: bytes) -> None:
        """Encrypts winner rate and saved_vs_avg on QuotationLeg, ensuring plaintext columns are strictly NULL."""
        if win_rate is not None:
            leg.encrypted_winner_rate = encrypt_field(float(win_rate), dek, field_context="winner_rate")
        else:
            leg.encrypted_winner_rate = None

        if saved_vs_avg is not None:
            leg.encrypted_saved_vs_avg = encrypt_field(float(saved_vs_avg), dek, field_context="saved_vs_avg")
        else:
            leg.encrypted_saved_vs_avg = None

        if hasattr(leg, "_db_winner_rate"):
            leg._db_winner_rate = None
        if hasattr(leg, "_db_saved_vs_avg"):
            leg._db_saved_vs_avg = None
        if hasattr(leg, "_resolved_winner_rate"):
            leg._resolved_winner_rate = float(win_rate) if win_rate is not None else None
        if hasattr(leg, "_resolved_saved_vs_avg"):
            leg._resolved_saved_vs_avg = float(saved_vs_avg) if saved_vs_avg is not None else None

    def apply_encrypted_rfq_winner(self, rfq: Any, win_rate: Optional[float], saved_vs_avg: Optional[float], dek: bytes) -> None:
        """Encrypts root winner rate and savings on QuotationRequest, ensuring plaintext columns are strictly NULL."""
        if win_rate is not None:
            rfq.encrypted_winner_rate = encrypt_field(float(win_rate), dek, field_context="winner_rate")
        else:
            rfq.encrypted_winner_rate = None

        if saved_vs_avg is not None:
            rfq.encrypted_saved_vs_avg = encrypt_field(float(saved_vs_avg), dek, field_context="saved_vs_avg")
        else:
            rfq.encrypted_saved_vs_avg = None

        if hasattr(rfq, "_db_winner_rate"):
            rfq._db_winner_rate = None
        if hasattr(rfq, "_db_saved_vs_avg"):
            rfq._db_saved_vs_avg = None
        if hasattr(rfq, "_resolved_winner_rate"):
            rfq._resolved_winner_rate = float(win_rate) if win_rate is not None else None
        if hasattr(rfq, "_resolved_saved_vs_avg"):
            rfq._resolved_saved_vs_avg = float(saved_vs_avg) if saved_vs_avg is not None else None

    def re_envelope_tenant_dek(self, db: Session, customer_id: int) -> dict:
        """
        Unseals the current DEK and re-wraps it with the Master KEK.
        Called during credentials rotation or security upgrades.
        Guarantees zero data loss: the underlying DEK remains identical so existing ciphertext remains valid.
        """
        from sqlalchemy.sql import func
        record = db.query(QuotationTenantKey).filter(
            QuotationTenantKey.customer_id == customer_id,
            QuotationTenantKey.status == "ACTIVE"
        ).first()

        if not record:
            raise ValueError(f"No active tenant key found for customer {customer_id}")

        provider = get_key_provider()
        tenant_context = str(customer_id)

        # 1. Unseal existing DEK
        current_dek = provider.unwrap_dek(record.wrapped_dek, tenant_context)

        # 2. Re-seal DEK under Master KEK
        new_wrapped = provider.wrap_dek(current_dek, tenant_context)

        record.wrapped_dek = new_wrapped
        record.key_version += 1
        record.rotated_at = func.now()
        db.commit()

        # Invalidate cache
        self.clear_cache(customer_id)
        logger.info(f"Re-enveloped Tenant DEK for Customer {customer_id} (Version: {record.key_version})")

        return {
            "customer_id": customer_id,
            "key_id": record.key_id,
            "key_version": record.key_version,
            "status": "RE_ENVELOPED_HEALTHY"
        }

    def verify_tenant_keys_health(self, db: Session) -> dict:
        """
        Disaster Recovery & Integrity Diagnostic Tool:
        Audits all tenant key records in the database, verifying Master KEK unwrap ability,
        AES-GCM MAC tag integrity, and roundtrip canary encryption.
        """
        records = db.query(QuotationTenantKey).filter(
            QuotationTenantKey.status == "ACTIVE"
        ).all()

        provider = get_key_provider()
        total_keys = len(records)
        healthy = 0
        corrupted = 0
        details = []

        start_t = time.perf_counter()

        for rec in records:
            tenant_context = str(rec.customer_id)
            try:
                # 1. Test unwrap
                dek = provider.unwrap_dek(rec.wrapped_dek, tenant_context)
                if len(dek) != 32:
                    raise ValueError("DEK length is invalid")

                # 2. Test synthetic canary roundtrip
                canary_test = 49.9999
                enc = encrypt_field(canary_test, dek, field_context="canary")
                dec = decrypt_field(enc, dek, field_context="canary")
                if abs(dec - canary_test) > 1e-5:
                    raise ValueError("Canary roundtrip assertion failed")

                healthy += 1
                details.append({
                    "customer_id": rec.customer_id,
                    "key_id": rec.key_id,
                    "key_version": rec.key_version,
                    "status": "HEALTHY"
                })
            except Exception as e:
                corrupted += 1
                details.append({
                    "customer_id": rec.customer_id,
                    "key_id": rec.key_id,
                    "key_version": rec.key_version,
                    "status": "CORRUPTED",
                    "error": str(e)
                })

        duration = time.perf_counter() - start_t
        avg_ms = (duration / max(1, total_keys)) * 1000.0

        return {
            "status": "HEALTHY" if corrupted == 0 else "WARNING",
            "total_keys_audited": total_keys,
            "healthy_keys": healthy,
            "corrupted_keys": corrupted,
            "audit_duration_seconds": round(duration, 4),
            "avg_unseal_latency_ms": round(avg_ms, 4),
            "keys_audit": details
        }


tenant_key_service = TenantKeyService()

