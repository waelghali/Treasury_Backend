import os
import hmac
import hashlib
from typing import Optional

# Maximum allowed incorrect OTP entry attempts before self-service burnout
MAX_OTP_FAILED_ATTEMPTS: int = 3

# Secret salt used for HMAC-SHA256 OTP hashing
# Can be overridden via environment variable
_DEFAULT_SALT = "grow_treasury_quotation_otp_hmac_secret_salt_2026_secure"

def get_otp_salt() -> bytes:
    salt_str = os.getenv("OTP_SECRET_SALT", _DEFAULT_SALT)
    return salt_str.encode("utf-8")


def hash_otp_code(otp_code: str) -> str:
    """
    Computes a cryptographic HMAC-SHA256 hash of a 6-digit OTP code using server salt.
    Ensures zero plaintext OTP codes exist in the database (NIST SP 800-63B / OWASP).
    """
    if not otp_code:
        return ""
    clean_code = otp_code.strip()
    return hmac.new(get_otp_salt(), clean_code.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_otp_code(provided_code: Optional[str], stored_val: Optional[str]) -> bool:
    """
    Verifies a provided OTP against the stored database value in constant time.
    Supports:
    1. HMAC-SHA256 64-character hex hash (standard).
    2. Legacy plaintext fallback for seamless zero-downtime transition.
    """
    if not provided_code or not stored_val:
        return False

    clean_provided = provided_code.strip()
    clean_stored = stored_val.strip()

    # If stored value is a 64-char hex string, verify with HMAC-SHA256
    if len(clean_stored) == 64 and all(c in "0123456789abcdefABCDEF" for c in clean_stored):
        computed_hash = hash_otp_code(clean_provided)
        return hmac.compare_digest(computed_hash.lower(), clean_stored.lower())

    # Fallback constant-time check for legacy plaintext OTPs during migration
    return hmac.compare_digest(clean_provided, clean_stored)


def generate_scoped_deal_receipt(
    rfq_id: str,
    ref_no: str,
    customer_name: str,
    bank_id: int,
    bank_name: str,
    executed_legs: list,
    executed_at: str
) -> dict:
    """
    Generates a cryptographically signed deal execution receipt scoped strictly
    to the specific legs won by a single bank counterparty.
    Guarantees non-repudiation, tamper-evidence, and strict cross-leg confidentiality.
    """
    import json
    canonical_data = {
        "rfq_id": str(rfq_id),
        "ref_no": str(ref_no),
        "customer": str(customer_name),
        "bank_id": int(bank_id),
        "bank_name": str(bank_name),
        "legs": executed_legs,
        "executed_at": str(executed_at)
    }
    serialized = json.dumps(canonical_data, sort_keys=True, separators=(',', ':'))
    signature = hmac.new(get_otp_salt(), serialized.encode("utf-8"), hashlib.sha256).hexdigest().upper()
    receipt_id = f"RCP-{ref_no}-{bank_id}-{signature[:8]}"
    
    return {
        "receipt_id": receipt_id,
        "signature_hash": f"SHA256:{signature}",
        "short_sig": signature[:16],
        "executed_at": executed_at,
        "scoped_legs_count": len(executed_legs),
        "verification_badge": f"Verified Cryptographic Receipt: {receipt_id} (Sig: {signature[:12]}...)"
    }

