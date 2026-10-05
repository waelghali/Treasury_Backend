# app/core/security_crypto.py
"""
Grow Treasury — Zero-Knowledge Cryptographic Architecture (Phase 8.1)
Institutional Envelope Encryption Engine (AES-256-GCM + AAD).

Features:
1. Abstracted Key Management Provider:
   - Stage 1: $0.00 LocalServerKeyProvider (AES-256-GCM Envelope via HKDF-derived Master KEK).
   - Stage 2: CloudKmsKeyProvider (Drop-in ready for AWS KMS / Google Cloud KMS HSM).
2. Authenticated Envelope Encryption (AES-256-GCM):
   - Cryptographically random 256-bit Tenant Data Encryption Keys (DEKs).
   - 96-bit unique nonces per encryption.
   - Associated Authenticated Data (AAD) binding prevents cross-tenant or cross-field replay attacks.
3. Type-Preserving Field Encryption & Dual-Read Compatibility:
   - Serializes and restores native types (float, int, str, dict, list).
   - Safe `is_encrypted()` checks enable dual-read transition without breaking existing rows.
"""

import os
import json
import base64
import secrets
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple, Dict, Union

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.exceptions import InvalidTag

logger = logging.getLogger(__name__)

# --- Cryptographic Exceptions ---
class CryptoError(Exception):
    """Base exception for cryptographic operations."""
    pass

class DecryptionError(CryptoError):
    """Raised when ciphertext cannot be decrypted (invalid key, corrupt data, or bad nonce)."""
    pass

class TamperDetectedError(DecryptionError):
    """Raised when ciphertext or authentication tag fails MAC verification (tampering detected)."""
    pass

class InvalidKeyError(CryptoError):
    """Raised when a provided DEK or KEK does not satisfy cryptographic length or format requirements."""
    pass


# --- Key Provider Abstraction (Stage 1: $0.00 Server Isolation -> Stage 2: Cloud KMS) ---
class KeyProvider(ABC):
    """Abstract Key Management Provider for wrapping and unwrapping Tenant DEKs."""

    @abstractmethod
    def wrap_dek(self, dek: bytes, tenant_id: str) -> str:
        """Wraps (encrypts) a tenant DEK using the Master KEK and returns a base64 string."""
        pass

    @abstractmethod
    def unwrap_dek(self, wrapped_dek_b64: str, tenant_id: str) -> bytes:
        """Unwraps (decrypts) a tenant DEK using the Master KEK."""
        pass


class LocalServerKeyProvider(KeyProvider):
    """
    Stage 1: $0.00 Self-Contained Server Isolation Key Provider.
    Derives a 256-bit Master Key Encryption Key (KEK) using HKDF-SHA256 from
    MASTER_ENCRYPTION_KEY or application SECRET_KEY, sealing tenant DEKs via AES-256-GCM
    with tenant-specific Associated Authenticated Data (AAD).
    """

    def __init__(self, master_secret: Optional[str] = None):
        secret = master_secret or os.getenv("MASTER_ENCRYPTION_KEY") or os.getenv("SECRET_KEY", "default-grow-dev-secret-change-in-prod")
        if not secret:
            raise InvalidKeyError("Cannot initialize LocalServerKeyProvider: No secret provided.")
        
        # Derive strict 256-bit (32 bytes) KEK via HKDF-SHA256
        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b"GrowTreasury-MasterKEK-Salt-v1",
            info=b"GrowTreasury-MasterKEK-Context",
        )
        self._master_kek = hkdf.derive(secret.encode("utf-8"))
        self._aesgcm = AESGCM(self._master_kek)

    def wrap_dek(self, dek: bytes, tenant_id: str) -> str:
        """Wraps tenant DEK with Master KEK and binds it to tenant_id via AAD."""
        if len(dek) != 32:
            raise InvalidKeyError("Tenant DEK must be exactly 32 bytes (256-bit AES).")
        nonce = secrets.token_bytes(12)
        aad = f"tenant:{tenant_id}".encode("utf-8")
        ciphertext = self._aesgcm.encrypt(nonce, dek, aad)
        # Store as base64 payload: nonce + ciphertext
        payload = nonce + ciphertext
        return base64.b64encode(payload).decode("ascii")

    def unwrap_dek(self, wrapped_dek_b64: str, tenant_id: str) -> bytes:
        """Unwraps tenant DEK with Master KEK, verifying tenant_id binding."""
        try:
            payload = base64.b64decode(wrapped_dek_b64.encode("ascii"))
            if len(payload) < 28: # 12 bytes nonce + 16 bytes tag min
                raise DecryptionError("Malformed wrapped DEK payload length.")
            nonce = payload[:12]
            ciphertext = payload[12:]
            aad = f"tenant:{tenant_id}".encode("utf-8")
            return self._aesgcm.decrypt(nonce, ciphertext, aad)
        except InvalidTag:
            raise TamperDetectedError("Master KEK MAC validation failed: Tenant DEK envelope has been tampered with or tenant_id mismatch.")
        except Exception as e:
            if isinstance(e, CryptoError):
                raise
            raise DecryptionError(f"Failed to unwrap tenant DEK: {e}")


class CloudKmsKeyProvider(KeyProvider):
    """
    Stage 2: Plug-and-Play Cloud KMS Provider (AWS KMS / Google Cloud KMS).
    Reserved interface for institutional deployments requiring FIPS 140-2 Level 2/3 hardware isolation.
    """

    def __init__(self, key_uri: str):
        self.key_uri = key_uri

    def wrap_dek(self, dek: bytes, tenant_id: str) -> str:
        # Implemented when connecting Cloud KMS client (e.g. google-cloud-kms / boto3)
        raise NotImplementedError("Cloud KMS Key Provider is configured in production enterprise deployment.")

    def unwrap_dek(self, wrapped_dek_b64: str, tenant_id: str) -> bytes:
        raise NotImplementedError("Cloud KMS Key Provider is configured in production enterprise deployment.")


# --- Global Key Provider Initialization ---
_KEY_PROVIDER: KeyProvider = LocalServerKeyProvider()

def get_key_provider() -> KeyProvider:
    """Returns the currently active KeyProvider instance."""
    return _KEY_PROVIDER

def set_key_provider(provider: KeyProvider) -> None:
    """Configures a custom KeyProvider (e.g. for testing or Cloud KMS upgrade)."""
    global _KEY_PROVIDER
    _KEY_PROVIDER = provider


# --- Core Cryptographic Operations (Tenant DEK Level) ---
CIPHER_PREFIX = "enc:v1:"

def generate_tenant_dek() -> bytes:
    """Generates a cryptographically random 256-bit (32 bytes) Tenant Data Encryption Key."""
    return secrets.token_bytes(32)

def is_encrypted(val: Any) -> bool:
    """Checks whether a value is already formatted as a Phase 8 ciphertext string."""
    return isinstance(val, str) and val.startswith(CIPHER_PREFIX)

def encrypt_field(value: Any, dek: bytes, field_context: str = "") -> Optional[str]:
    """
    Encrypts a single field (float, int, string, or boolean) using the Tenant DEK.
    Binds field_context (e.g. 'price' or 'spread') as AAD to prevent cross-field swapping.
    Returns format: 'enc:v1:{nonce_b64}:{ciphertext_b64}'
    """
    if value is None:
        return None
    
    if len(dek) != 32:
        raise InvalidKeyError("Tenant DEK must be exactly 32 bytes.")

    # Encode value with type indicator prefix:
    # 'f:' = float, 'i:' = int, 's:' = str, 'b:' = bool, 'j:' = json dict/list
    if isinstance(value, float):
        encoded = f"f:{value}".encode("utf-8")
    elif isinstance(value, bool):
        encoded = f"b:{1 if value else 0}".encode("utf-8")
    elif isinstance(value, int):
        encoded = f"i:{value}".encode("utf-8")
    elif isinstance(value, (dict, list)):
        encoded = f"j:{json.dumps(value, separators=(',', ':'))}".encode("utf-8")
    else:
        encoded = f"s:{str(value)}".encode("utf-8")

    nonce = secrets.token_bytes(12)
    aad = field_context.encode("utf-8") if field_context else b""

    aesgcm = AESGCM(dek)
    ciphertext = aesgcm.encrypt(nonce, encoded, aad)

    nonce_b64 = base64.b64encode(nonce).decode("ascii")
    ct_b64 = base64.b64encode(ciphertext).decode("ascii")
    return f"{CIPHER_PREFIX}{nonce_b64}:{ct_b64}"

def decrypt_field(cipher_str: Optional[str], dek: bytes, field_context: str = "") -> Any:
    """
    Decrypts a Phase 8 ciphertext string using the Tenant DEK and restores the native type.
    Dual-Read Safe: If cipher_str is not encrypted, returns cipher_str as-is.
    """
    if cipher_str is None:
        return None

    # Dual-Read Fallback: If not encrypted with Phase 8 format, return raw value
    if not is_encrypted(cipher_str):
        return cipher_str

    if len(dek) != 32:
        raise InvalidKeyError("Tenant DEK must be exactly 32 bytes.")

    payload = cipher_str[len(CIPHER_PREFIX):]
    parts = payload.split(":")
    if len(parts) != 2:
        raise DecryptionError("Malformed Phase 8 ciphertext structure.")

    try:
        nonce = base64.b64decode(parts[0].encode("ascii"))
        ciphertext = base64.b64decode(parts[1].encode("ascii"))
        aad = field_context.encode("utf-8") if field_context else b""

        aesgcm = AESGCM(dek)
        decrypted_bytes = aesgcm.decrypt(nonce, ciphertext, aad)
        decrypted_str = decrypted_bytes.decode("utf-8")

        # Parse type prefix
        type_prefix = decrypted_str[:2]
        raw_val = decrypted_str[2:]

        if type_prefix == "f:":
            return float(raw_val)
        elif type_prefix == "i:":
            return int(raw_val)
        elif type_prefix == "b:":
            return bool(int(raw_val))
        elif type_prefix == "j:":
            return json.loads(raw_val)
        elif type_prefix == "s:":
            return raw_val
        else:
            # Fallback if no prefix
            return decrypted_str
    except InvalidTag:
        raise TamperDetectedError(f"Ciphertext MAC verification failed for context '{field_context}'. Potential data tampering or incorrect tenant key.")
    except Exception as e:
        if isinstance(e, CryptoError):
            raise
        raise DecryptionError(f"Field decryption failed: {e}")

def encrypt_json(data: Dict[str, Any], dek: bytes, context: str = "") -> Optional[str]:
    """Helper to encrypt an entire JSON dictionary structure."""
    if data is None:
        return None
    return encrypt_field(data, dek, field_context=context)

def decrypt_json(cipher_str: Optional[str], dek: bytes, context: str = "") -> Optional[Dict[str, Any]]:
    """Helper to decrypt an entire JSON dictionary structure."""
    if cipher_str is None:
        return None
    res = decrypt_field(cipher_str, dek, field_context=context)
    if isinstance(res, dict):
        return res
    if isinstance(res, str):
        try:
            return json.loads(res)
        except Exception:
            return None
    return None
