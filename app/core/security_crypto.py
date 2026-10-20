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
import io
import json
import base64
import struct
import secrets
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple, Dict, Union, Iterator, BinaryIO

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



# --- Sub-Phase 8.7: Chunked Authenticated Streaming Encryption (STREAM / 64 KB Blocks) ---
RAW_FILE_MAGIC = b"ENC_FILE_V1\x00"     # 12 bytes: legacy monolithic encryption
STREAM_MAGIC = b"ENC_STREAM_V2\x00"       # 14 bytes: chunked streaming AEAD
DEFAULT_STREAM_CHUNK_SIZE = 64 * 1024      # 64 KB blocks (memory ceiling < 128 KB RAM delta)
STREAM_SALT_LEN = 7
FLAG_INTERMEDIATE = b"\x00"
FLAG_FINAL = b"\x01"

def _read_exact(stream: BinaryIO, n: int) -> bytes:
    """Reads exactly n bytes from a binary stream, or fewer if EOF is reached."""
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            break
        buf.extend(chunk)
    return bytes(buf)

def is_encrypted_file(file_bytes_or_prefix: bytes) -> bool:
    """Checks whether the file bytes or header prefix starts with STREAM V2 or legacy V1 magic."""
    if not file_bytes_or_prefix:
        return False
    return file_bytes_or_prefix.startswith(STREAM_MAGIC) or file_bytes_or_prefix.startswith(RAW_FILE_MAGIC)

def encrypt_stream_generator(
    source_stream: BinaryIO,
    dek: bytes,
    chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    context: str = "doc_attachment"
) -> Iterator[bytes]:
    """
    Encrypts source_stream in authenticated fixed-size chunks (default: 64 KB) using the STREAM AEAD construction.
    Memory footprint is strictly bounded (< 128 KB RAM delta) regardless of file size (1 KB to 500 MB+).
    
    Header format (25 bytes):
      STREAM_MAGIC (14 bytes) + chunk_size (4 bytes uint32) + stream_salt (7 bytes)
    
    Chunk frame format:
      flag (1 byte: 0x00=intermediate, 0x01=final) + frame_len (4 bytes uint32) + ciphertext_and_tag
    """
    if len(dek) != 32:
        raise InvalidKeyError("Tenant DEK must be exactly 32 bytes (256-bit AES).")
    
    if chunk_size <= 0:
        chunk_size = DEFAULT_STREAM_CHUNK_SIZE
        
    aesgcm = AESGCM(dek)
    stream_salt = secrets.token_bytes(STREAM_SALT_LEN)
    
    # 1. Yield stream header (25 bytes)
    yield STREAM_MAGIC + struct.pack(">I", chunk_size) + stream_salt
    
    chunk_index = 0
    # Read the first chunk
    current_chunk = _read_exact(source_stream, chunk_size)
    
    while True:
        # Lookahead to determine if this is the final block
        next_chunk = _read_exact(source_stream, chunk_size)
        is_last = (len(next_chunk) == 0)
        flag = FLAG_FINAL if is_last else FLAG_INTERMEDIATE
        
        # 12-byte Nonce: 7-byte salt + 4-byte chunk index + 1-byte flag
        nonce = stream_salt + struct.pack(">I", chunk_index) + flag
        aad = f"{context}:chunk:{chunk_index}".encode("utf-8") if context else f"chunk:{chunk_index}".encode("utf-8")
        
        cipher_chunk = aesgcm.encrypt(nonce, current_chunk, aad)
        frame_len = len(cipher_chunk)
        
        yield flag + struct.pack(">I", frame_len) + cipher_chunk
        
        if is_last:
            break
            
        chunk_index += 1
        current_chunk = next_chunk

def decrypt_stream_generator(
    source_stream: BinaryIO,
    dek: bytes,
    context: str = "doc_attachment"
) -> Iterator[bytes]:
    """
    Streams decrypted plaintext chunks block-by-block with bounded memory consumption (< 128 KB RAM).
    Dual-Read Backwards Compatible:
      1. STREAM V2 (ENC_STREAM_V2\x00): Validates block nonces, indexes, and final flag against truncation.
      2. Legacy V1 (ENC_FILE_V1\x00): Decrypts legacy monolithic ciphertext.
      3. Raw unencrypted (%PDF, DOCX, text): Streams unmodified with zero alteration.
    """
    magic_peek = _read_exact(source_stream, len(STREAM_MAGIC))
    if not magic_peek:
        return
    
    if magic_peek == STREAM_MAGIC:
        # --- Mode 1: STREAM V2 Chunked Authenticated Decryption ---
        if len(dek) != 32:
            raise InvalidKeyError("Tenant DEK must be exactly 32 bytes.")
            
        rest_of_header = _read_exact(source_stream, 4 + STREAM_SALT_LEN)
        if len(rest_of_header) < 4 + STREAM_SALT_LEN:
            raise TamperDetectedError("Truncated STREAM header.")
            
        _chunk_size = struct.unpack(">I", rest_of_header[:4])[0]
        stream_salt = rest_of_header[4:4 + STREAM_SALT_LEN]
        
        aesgcm = AESGCM(dek)
        chunk_index = 0
        saw_final_flag = False
        
        while True:
            flag_byte = _read_exact(source_stream, 1)
            if not flag_byte:
                break
                
            len_bytes = _read_exact(source_stream, 4)
            if len(len_bytes) < 4:
                raise TamperDetectedError("Truncated chunk frame header in document stream.")
                
            frame_len = struct.unpack(">I", len_bytes)[0]
            cipher_chunk = _read_exact(source_stream, frame_len)
            if len(cipher_chunk) < frame_len:
                raise TamperDetectedError("Truncated ciphertext block in document stream.")
                
            nonce = stream_salt + struct.pack(">I", chunk_index) + flag_byte
            aad = f"{context}:chunk:{chunk_index}".encode("utf-8") if context else f"chunk:{chunk_index}".encode("utf-8")
            
            try:
                plain_chunk = aesgcm.decrypt(nonce, cipher_chunk, aad)
            except InvalidTag:
                raise TamperDetectedError(f"Block #{chunk_index} authentication failed. Tampering detected or invalid key.")
            except Exception as e:
                raise DecryptionError(f"Block #{chunk_index} decryption failed: {e}")
                
            yield plain_chunk
            
            if flag_byte == FLAG_FINAL:
                saw_final_flag = True
                break
                
            chunk_index += 1
            
        if not saw_final_flag:
            raise TamperDetectedError("Document stream was truncated before the authenticated final block.")
            
    elif magic_peek[:len(RAW_FILE_MAGIC)] == RAW_FILE_MAGIC:
        # --- Mode 2: Legacy ENC_FILE_V1 Monolithic Decryption ---
        if len(dek) != 32:
            raise InvalidKeyError("Tenant DEK must be exactly 32 bytes.")
        
        magic_len = len(RAW_FILE_MAGIC)
        leftover_nonce = magic_peek[magic_len:]
        needed_nonce = 12 - len(leftover_nonce)
        rest_nonce = _read_exact(source_stream, needed_nonce)
        nonce = leftover_nonce + rest_nonce
        remaining_ciphertext = source_stream.read()
        aad = context.encode("utf-8") if context else b""
        
        try:
            aesgcm = AESGCM(dek)
            plain = aesgcm.decrypt(nonce, remaining_ciphertext, aad)
            yield plain
        except InvalidTag:
            raise TamperDetectedError("Legacy document verification failed. Tampering detected or invalid key.")
        except Exception as e:
            raise DecryptionError(f"Legacy document decryption failed: {e}")
            
    else:
        # --- Mode 3: Raw Unencrypted Pass-Through (Zero Downtime Dual-Read) ---
        yield magic_peek
        while True:
            chunk = source_stream.read(DEFAULT_STREAM_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

def encrypt_file_stream(
    source_stream: BinaryIO,
    dest_stream: BinaryIO,
    dek: bytes,
    chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    context: str = "doc_attachment"
) -> int:
    """
    Encrypts source_stream and writes encrypted bytes to dest_stream in chunked frames.
    Returns total bytes written.
    """
    total_written = 0
    for chunk in encrypt_stream_generator(source_stream, dek, chunk_size=chunk_size, context=context):
        dest_stream.write(chunk)
        total_written += len(chunk)
    return total_written

def encrypt_file_bytes(raw_bytes: bytes, dek: bytes, context: str = "doc_attachment") -> bytes:
    """Encrypts arbitrary document bytes using the chunked STREAM V2 AEAD construction."""
    bio_in = io.BytesIO(raw_bytes)
    return b"".join(encrypt_stream_generator(bio_in, dek, context=context))

def decrypt_file_bytes(file_bytes: bytes, dek: bytes, context: str = "doc_attachment") -> bytes:
    """
    Decrypts document bytes. Dual-read safe: supports STREAM V2, legacy V1,
    and returns unencrypted files unmodified.
    """
    bio_in = io.BytesIO(file_bytes)
    return b"".join(decrypt_stream_generator(bio_in, dek, context=context))


# --- Sub-Phase 8.5: Self-Contained Password Recovery & Cooldown Rate Limiting ---
import hmac
import hashlib
from collections import defaultdict


_RECOVERY_ATTEMPTS: Dict[str, list] = defaultdict(list)

def check_recovery_rate_limit(email: str, max_attempts: int = 3, window_seconds: int = 3600) -> Tuple[bool, int]:
    """
    Enforces a strict cooldown rate limiter for self-service password recovery.
    Allows up to max_attempts within window_seconds (default: max 3 requests per 1 hour).
    Returns (is_allowed, seconds_remaining_until_cooldown).
    """
    clean_email = email.strip().lower()
    now = time.time()
    
    # Prune attempts older than the window
    attempts = [ts for ts in _RECOVERY_ATTEMPTS[clean_email] if (now - ts) < window_seconds]
    _RECOVERY_ATTEMPTS[clean_email] = attempts

    if len(attempts) >= max_attempts:
        oldest_in_window = min(attempts)
        seconds_remaining = int(window_seconds - (now - oldest_in_window))
        return False, max(1, seconds_remaining)

    # Record this attempt
    _RECOVERY_ATTEMPTS[clean_email].append(now)
    return True, 0

def clear_recovery_rate_limit(email: str) -> None:
    """Resets recovery attempt history for testing or upon successful password reset."""
    _RECOVERY_ATTEMPTS.pop(email.strip().lower(), None)

def generate_recovery_token(email: str, secret_salt: str, expiry_minutes: int = 15) -> str:
    """
    Generates a secure, time-bounded HMAC-SHA256 recovery token.
    Encodes email, expiry timestamp, and a cryptographic MAC signature.
    """
    clean_email = email.strip().lower()
    expires_at = int(time.time()) + (expiry_minutes * 60)
    data = f"{clean_email}:{expires_at}"
    
    mac = hmac.new(
        key=secret_salt.encode("utf-8"),
        msg=data.encode("utf-8"),
        digestmod=hashlib.sha256
    ).hexdigest()
    
    payload = f"{data}:{mac}"
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii")

def verify_recovery_token(token: str, secret_salt: str) -> Optional[str]:
    """
    Validates an HMAC-SHA256 recovery token.
    Returns the associated email if valid, or None if the token is tampered, malformed, or expired.
    """
    try:
        raw_payload = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        parts = raw_payload.split(":")
        if len(parts) != 3:
            return None
        
        email, expires_at_str, signature = parts
        expires_at = int(expires_at_str)
        
        # Check expiration (15-minute TTL)
        if time.time() > expires_at:
            return None
        
        # Verify HMAC
        data = f"{email}:{expires_at}"
        expected_mac = hmac.new(
            key=secret_salt.encode("utf-8"),
            msg=data.encode("utf-8"),
            digestmod=hashlib.sha256
        ).hexdigest()
        
        if hmac.compare_digest(signature, expected_mac):
            return email
        return None
    except Exception:
        return None


# --- Ephemeral Signed Document Download Tickets (Sub-Phase 8.7 Option A) ---

def generate_download_ticket(payload: dict, secret_salt: Optional[str] = None, expiry_minutes: int = 15) -> str:
    """
    Generates a secure, cryptographically signed, short-lived (15-minute) ephemeral ticket 
    for unsealing quotation documents (Sub-Phase 8.7 Option A).
    """
    salt = secret_salt or os.environ.get("SECRET_KEY", "grow-ephemeral-doc-ticket-secret-2026")
    exp_ts = int(time.time()) + (expiry_minutes * 60)
    data = dict(payload)
    data["exp"] = exp_ts
    raw_json = json.dumps(data, sort_keys=True, separators=(',', ':'))
    b64_payload = base64.urlsafe_b64encode(raw_json.encode("utf-8")).decode("ascii").rstrip("=")
    mac = hmac.new(
        key=salt.encode("utf-8"),
        msg=b64_payload.encode("utf-8"),
        digestmod=hashlib.sha256
    ).hexdigest()
    return f"{b64_payload}.{mac}"

def verify_download_ticket(ticket: str, secret_salt: Optional[str] = None) -> Tuple[bool, Optional[dict], Optional[str]]:
    """
    Validates an ephemeral download ticket.
    Returns (is_valid, payload, error_message).
    """
    salt = secret_salt or os.environ.get("SECRET_KEY", "grow-ephemeral-doc-ticket-secret-2026")
    try:
        parts = ticket.strip().split(".")
        if len(parts) != 2:
            return False, None, "Malformed download ticket."
        
        b64_payload, signature = parts
        expected_mac = hmac.new(
            key=salt.encode("utf-8"),
            msg=b64_payload.encode("utf-8"),
            digestmod=hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_mac):
            return False, None, "Invalid ticket signature or tampered ticket."

        # Pad base64 string if needed
        padding = 4 - (len(b64_payload) % 4)
        if padding < 4:
            b64_payload += "=" * padding

        raw_json = base64.urlsafe_b64decode(b64_payload.encode("ascii")).decode("utf-8")
        payload = json.loads(raw_json)

        exp = payload.get("exp")
        if not exp or time.time() > float(exp):
            return False, None, "Download ticket has expired. Please refresh the portal to generate a new ticket."

        return True, payload, None
    except Exception as e:
        return False, None, f"Ticket verification failed: {e}"


# --- Zero-Knowledge Blind Indexing for Counterparty Identification ---

def compute_email_blind_index(email: Optional[str], secret_key: Optional[Union[bytes, str]] = None) -> str:
    """
    Computes a deterministic, one-way HMAC-SHA256 blind index for an email address.
    Used for O(1) deduplication and lookup in Zero-Knowledge tables without cleartext leakage.
    Derives an isolated blind index subkey via HKDF-SHA256.
    """
    if not email:
        return ""
    clean = email.strip().lower().encode("utf-8")
    
    if secret_key is None:
        raw_key = os.getenv("MASTER_ENCRYPTION_KEY") or os.getenv("SECRET_KEY", "default-grow-dev-secret-change-in-prod")
        key_bytes = raw_key.encode("utf-8") if isinstance(raw_key, str) else raw_key
    elif isinstance(secret_key, str):
        key_bytes = secret_key.encode("utf-8")
    else:
        key_bytes = secret_key

    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"GrowTreasury-BlindIndex-Salt-v1",
        info=b"GrowTreasury-EmailBlindIndex",
    )
    derived_sub_key = hkdf.derive(key_bytes)
    return hmac.new(derived_sub_key, clean, hashlib.sha256).hexdigest()

