# app/core/webauthn_service.py
"""
WebAuthn / Passkey Biometric Service (Face ID, Touch ID, Android Fingerprint)
for Permanent Users (End Users, Corporate Admins, Checkers, System Owners).

Uses standard W3C WebAuthn Level 2/3 assertions verified via Python's `cryptography`
library and stateless JWT-signed challenges for multi-worker compatibility.
"""

import base64
import hashlib
import json
import logging
import secrets
import struct
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from fastapi import HTTPException, Request, status
from jose import JWTError, jwt

from app.core.security import ALGORITHM, SECRET_KEY

logger = logging.getLogger(__name__)

WEBAUTHN_CHALLENGE_EXPIRE_MINUTES = 10


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(val: str) -> bytes:
    if not val:
        return b""
    cleaned = val.strip().replace("+", "-").replace("/", "_")
    pad = "=" * ((4 - len(cleaned) % 4) % 4)
    return base64.urlsafe_b64decode(cleaned + pad)


def resolve_rp_id(request: Request, explicit_rp_id: Optional[str] = None) -> str:
    """
    Determines the WebAuthn Relying Party ID (effective domain) from the request
    Origin/Host or explicit_rp_id sent by the browser.
    """
    if explicit_rp_id:
        clean = explicit_rp_id.strip().lower().split(":")[0]
        if clean:
            return clean

    origin = request.headers.get("origin") or request.headers.get("referer")
    if origin:
        try:
            parsed = urlparse(origin)
            if parsed.hostname:
                return parsed.hostname.lower()
        except Exception:
            pass

    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or "localhost"
    return host.split(",")[0].strip().split(":")[0].lower()


# ── Minimal RFC 8949 CBOR Decoder (for attestationObject & COSE_Key fallback) ──

def _cbor_decode_item(data: bytes, offset: int = 0) -> Tuple[Any, int]:
    if offset >= len(data):
        raise ValueError("Unexpected end of CBOR data")
    initial = data[offset]
    major = initial >> 5
    addl = initial & 0x1F
    offset += 1

    if addl < 24:
        val = addl
    elif addl == 24:
        val = data[offset]
        offset += 1
    elif addl == 25:
        val = struct.unpack(">H", data[offset:offset + 2])[0]
        offset += 2
    elif addl == 26:
        val = struct.unpack(">I", data[offset:offset + 4])[0]
        offset += 4
    elif addl == 27:
        val = struct.unpack(">Q", data[offset:offset + 8])[0]
        offset += 8
    else:
        raise ValueError(f"Unsupported CBOR additional info: {addl}")

    if major == 0:  # Unsigned int
        return val, offset
    if major == 1:  # Negative int
        return -1 - val, offset
    if major == 2:  # Byte string
        return data[offset:offset + val], offset + val
    if major == 3:  # Text string
        return data[offset:offset + val].decode("utf-8", errors="replace"), offset + val
    if major == 4:  # Array
        arr = []
        for _ in range(val):
            item, offset = _cbor_decode_item(data, offset)
            arr.append(item)
        return arr, offset
    if major == 5:  # Map
        mp = {}
        for _ in range(val):
            k, offset = _cbor_decode_item(data, offset)
            v, offset = _cbor_decode_item(data, offset)
            mp[k] = v
        return mp, offset
    if major == 6:  # Semantic tag
        return _cbor_decode_item(data, offset)
    if major == 7:  # Simple / bool / null
        if addl == 20:
            return False, offset
        if addl == 21:
            return True, offset
        if addl in (22, 23):
            return None, offset
        return val, offset
    raise ValueError(f"Unsupported CBOR major type: {major}")


def _cose_key_to_spki_der(cose_key: Dict[Any, Any]) -> Tuple[bytes, int]:
    """Converts a decoded COSE_Key map (EC2 P-256 or RSA) into SPKI DER bytes."""
    kty = cose_key.get(1)
    alg = int(cose_key.get(3, -7))

    if kty == 2:  # EC2 (P-256 / ES256)
        x_bytes = cose_key.get(-2)
        y_bytes = cose_key.get(-3)
        if not isinstance(x_bytes, (bytes, bytearray)) or not isinstance(y_bytes, (bytes, bytearray)):
            raise ValueError("Invalid EC2 COSE key coordinates")
        x_int = int.from_bytes(x_bytes, "big")
        y_int = int.from_bytes(y_bytes, "big")
        pub_numbers = ec.EllipticCurvePublicNumbers(x_int, y_int, ec.SECP256R1())
        pub_key = pub_numbers.public_key()
        spki = pub_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        return spki, alg

    if kty == 3:  # RSA (RS256)
        n_bytes = cose_key.get(-1)
        e_bytes = cose_key.get(-2)
        if not isinstance(n_bytes, (bytes, bytearray)) or not isinstance(e_bytes, (bytes, bytearray)):
            raise ValueError("Invalid RSA COSE key parameters")
        n_int = int.from_bytes(n_bytes, "big")
        e_int = int.from_bytes(e_bytes, "big")
        pub_numbers = rsa.RSAPublicNumbers(e_int, n_int)
        pub_key = pub_numbers.public_key()
        spki = pub_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        return spki, alg

    raise ValueError(f"Unsupported COSE key type: {kty}")


# ── Challenge Creation & Validation ─────────────────────────────────────────

def build_registration_options(
    user_id: int,
    user_email: str,
    user_display_name: str,
    rp_id: str,
    existing_credential_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Creates WebAuthn PublicKeyCredentialCreationOptions payload and a signed
    stateless challenge_token so multi-worker servers can verify without sticky sessions.
    """
    raw_challenge = secrets.token_bytes(32)
    challenge_b64 = b64url_encode(raw_challenge)
    user_handle_b64 = b64url_encode(f"grow-user-{user_id}".encode("utf-8"))

    now = datetime.now(timezone.utc)
    challenge_token = jwt.encode(
        {
            "purpose": "webauthn_register",
            "challenge": challenge_b64,
            "user_id": user_id,
            "rp_id": rp_id,
            "exp": now + timedelta(minutes=WEBAUTHN_CHALLENGE_EXPIRE_MINUTES),
        },
        SECRET_KEY,
        algorithm=ALGORITHM,
    )

    exclude_credentials = [
        {"type": "public-key", "id": cid}
        for cid in (existing_credential_ids or [])
        if cid
    ]

    return {
        "challenge_token": challenge_token,
        "public_key": {
            "challenge": challenge_b64,
            "rp": {
                "name": "Grow Treasury Platform",
                "id": rp_id,
            },
            "user": {
                "id": user_handle_b64,
                "name": user_email,
                "displayName": user_display_name or user_email,
            },
            "pubKeyCredParams": [
                {"type": "public-key", "alg": -7},    # ES256 (Face ID / Touch ID / Android TEE)
                {"type": "public-key", "alg": -257},  # RS256 (Windows Hello / fallback)
            ],
            "authenticatorSelection": {
                "authenticatorAttachment": "platform",
                "userVerification": "required",
                "residentKey": "preferred",
            },
            "timeout": 60000,
            "attestation": "none",
            "excludeCredentials": exclude_credentials,
        },
    }


def build_authentication_options(
    rp_id: str,
    allowed_credential_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Creates WebAuthn PublicKeyCredentialRequestOptions payload for biometric login.
    """
    raw_challenge = secrets.token_bytes(32)
    challenge_b64 = b64url_encode(raw_challenge)

    now = datetime.now(timezone.utc)
    challenge_token = jwt.encode(
        {
            "purpose": "webauthn_login",
            "challenge": challenge_b64,
            "rp_id": rp_id,
            "exp": now + timedelta(minutes=WEBAUTHN_CHALLENGE_EXPIRE_MINUTES),
        },
        SECRET_KEY,
        algorithm=ALGORITHM,
    )

    allow_credentials = [
        {"type": "public-key", "id": cid}
        for cid in (allowed_credential_ids or [])
        if cid
    ]

    return {
        "challenge_token": challenge_token,
        "public_key": {
            "challenge": challenge_b64,
            "rpId": rp_id,
            "allowCredentials": allow_credentials,
            "userVerification": "required",
            "timeout": 60000,
        },
    }


# ── Verification Logic ──────────────────────────────────────────────────────

def verify_registration_response(
    challenge_token: str,
    credential_id: str,
    client_data_json_b64: str,
    attestation_object_b64: Optional[str] = None,
    authenticator_data_b64: Optional[str] = None,
    public_key_spki_b64: Optional[str] = None,
    public_key_alg: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Verifies a WebAuthn registration response and returns the verified
    user_id, credential_id, public_key_spki (base64url), algorithm, and sign_count.
    """
    try:
        token_payload = jwt.decode(challenge_token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Biometric setup session expired. Please try again.",
        )

    if token_payload.get("purpose") != "webauthn_register":
        raise HTTPException(status_code=400, detail="Invalid biometric challenge token.")

    expected_challenge = token_payload.get("challenge")
    expected_rp_id = token_payload.get("rp_id")
    user_id = token_payload.get("user_id")

    # 1. Verify clientDataJSON
    try:
        client_data_bytes = b64url_decode(client_data_json_b64)
        client_data = json.loads(client_data_bytes.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed WebAuthn clientDataJSON.")

    if client_data.get("type") != "webauthn.create":
        raise HTTPException(status_code=400, detail="Invalid WebAuthn ceremony type.")

    if client_data.get("challenge") != expected_challenge:
        raise HTTPException(status_code=400, detail="Biometric challenge mismatch.")

    # 2. Extract authenticatorData
    auth_data_bytes: Optional[bytes] = None
    if authenticator_data_b64:
        auth_data_bytes = b64url_decode(authenticator_data_b64)
    elif attestation_object_b64:
        try:
            att_obj, _ = _cbor_decode_item(b64url_decode(attestation_object_b64))
            if isinstance(att_obj, dict) and isinstance(att_obj.get("authData"), (bytes, bytearray)):
                auth_data_bytes = bytes(att_obj["authData"])
        except Exception as e:
            logger.warning(f"Could not decode attestationObject CBOR: {e}")

    if not auth_data_bytes or len(auth_data_bytes) < 37:
        raise HTTPException(status_code=400, detail="Missing or invalid WebAuthn authenticatorData.")

    # 3. Verify RP ID hash & User Present flag
    expected_rp_hash = hashlib.sha256(expected_rp_id.encode("utf-8")).digest()
    if auth_data_bytes[:32] != expected_rp_hash:
        raise HTTPException(status_code=400, detail="RP ID domain mismatch during biometric enrollment.")

    flags = auth_data_bytes[32]
    if (flags & 0x01) == 0:
        raise HTTPException(status_code=400, detail="User presence was not confirmed by device authenticator.")

    sign_count = struct.unpack(">I", auth_data_bytes[33:37])[0]

    # 4. Extract & validate Public Key (SPKI DER)
    spki_bytes: Optional[bytes] = None
    alg: int = int(public_key_alg) if public_key_alg is not None else -7

    if public_key_spki_b64:
        try:
            candidate_spki = b64url_decode(public_key_spki_b64)
            serialization.load_der_public_key(candidate_spki)
            spki_bytes = candidate_spki
        except Exception as e:
            logger.warning(f"Direct SPKI public key validation failed, trying COSE fallback: {e}")

    if spki_bytes is None:
        # Parse attested credential data from authData
        if len(auth_data_bytes) < 55:
            raise HTTPException(status_code=400, detail="Authenticator data does not contain public key.")
        cred_id_len = struct.unpack(">H", auth_data_bytes[53:55])[0]
        cose_offset = 55 + cred_id_len
        if len(auth_data_bytes) <= cose_offset:
            raise HTTPException(status_code=400, detail="Truncated attested credential data.")
        try:
            cose_map, _ = _cbor_decode_item(auth_data_bytes, cose_offset)
            spki_bytes, alg = _cose_key_to_spki_der(cose_map)
        except Exception as e:
            logger.error(f"Failed to parse COSE public key: {e}")
            raise HTTPException(status_code=400, detail="Unsupported biometric public key format.")

    return {
        "user_id": int(user_id),
        "credential_id": credential_id.strip(),
        "public_key_spki": b64url_encode(spki_bytes),
        "algorithm": alg,
        "sign_count": sign_count,
    }


def verify_authentication_assertion(
    challenge_token: str,
    client_data_json_b64: str,
    authenticator_data_b64: str,
    signature_b64: str,
    public_key_spki_b64: str,
    stored_sign_count: int = 0,
) -> int:
    """
    Verifies a WebAuthn assertion (Face ID / Fingerprint sign-in) against the stored SPKI public key.
    Returns the updated sign_count if valid, or raises HTTPException on failure.
    """
    try:
        token_payload = jwt.decode(challenge_token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Biometric login challenge expired. Please tap Sign In with Face ID / Fingerprint again.",
        )

    if token_payload.get("purpose") != "webauthn_login":
        raise HTTPException(status_code=400, detail="Invalid biometric login token.")

    expected_challenge = token_payload.get("challenge")
    expected_rp_id = token_payload.get("rp_id")

    # 1. Verify clientDataJSON
    try:
        client_data_bytes = b64url_decode(client_data_json_b64)
        client_data = json.loads(client_data_bytes.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed WebAuthn clientDataJSON.")

    if client_data.get("type") != "webauthn.get":
        raise HTTPException(status_code=400, detail="Invalid WebAuthn assertion type.")

    if client_data.get("challenge") != expected_challenge:
        raise HTTPException(status_code=401, detail="Biometric login challenge mismatch.")

    # 2. Verify authenticatorData
    try:
        auth_data_bytes = b64url_decode(authenticator_data_b64)
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed authenticatorData.")

    if len(auth_data_bytes) < 37:
        raise HTTPException(status_code=400, detail="Invalid authenticatorData length.")

    expected_rp_hash = hashlib.sha256(expected_rp_id.encode("utf-8")).digest()
    if auth_data_bytes[:32] != expected_rp_hash:
        raise HTTPException(status_code=401, detail="Biometric credential domain mismatch.")

    flags = auth_data_bytes[32]
    # Bit 0 = UP (User Present), Bit 2 = UV (User Verified via biometric/device lock)
    if (flags & 0x01) == 0:
        raise HTTPException(status_code=401, detail="User presence not verified.")
    if (flags & 0x04) == 0:
        raise HTTPException(status_code=401, detail="Biometric user verification required.")

    new_sign_count = struct.unpack(">I", auth_data_bytes[33:37])[0]
    if stored_sign_count > 0 and new_sign_count > 0 and new_sign_count <= stored_sign_count:
        raise HTTPException(status_code=401, detail="Biometric authenticator counter anomaly detected.")

    # 3. Verify cryptographic signature over (authenticatorData || SHA256(clientDataJSON))
    client_data_hash = hashlib.sha256(client_data_bytes).digest()
    signed_data = auth_data_bytes + client_data_hash

    try:
        sig_bytes = b64url_decode(signature_b64)
        pub_key_der = b64url_decode(public_key_spki_b64)
        pub_key = serialization.load_der_public_key(pub_key_der)

        if isinstance(pub_key, ec.EllipticCurvePublicKey):
            pub_key.verify(sig_bytes, signed_data, ec.ECDSA(hashes.SHA256()))
        elif isinstance(pub_key, rsa.RSAPublicKey):
            pub_key.verify(sig_bytes, signed_data, padding.PKCS1v15(), hashes.SHA256())
        else:
            raise HTTPException(status_code=400, detail="Unsupported public key algorithm.")
    except InvalidSignature:
        raise HTTPException(status_code=401, detail="Biometric signature verification failed.")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"WebAuthn signature verification error: {e}")
        raise HTTPException(status_code=401, detail="Could not verify biometric signature.")

    return max(stored_sign_count, new_sign_count)

