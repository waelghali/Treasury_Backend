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
        """Encrypts price into encrypted_price and populates dual-write legacy column."""
        offer.encrypted_price = encrypt_field(float(raw_price), dek, field_context="price")
        offer.price = float(raw_price)

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
        """Encrypts T-Bill terms and populates dual-write legacy columns."""
        tbill_offer.encrypted_discount_rate = encrypt_field(float(discount_rate), dek, field_context="discount_rate")
        tbill_offer.encrypted_max_amount = encrypt_field(float(max_amount), dek, field_context="max_amount")
        tbill_offer.discount_rate = float(discount_rate)
        tbill_offer.max_amount = float(max_amount)


tenant_key_service = TenantKeyService()
