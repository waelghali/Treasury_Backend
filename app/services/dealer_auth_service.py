# app/services/dealer_auth_service.py
"""
Dealer Authentication & 2-Factor Enrolment Service
Secures the Permanent Bank Dealer Trading Desk using RFC 6238 TOTP (Microsoft / Google Authenticator)
and cryptographic proof-of-possession ceremonies.
"""

import os
import io
import base64
import logging
import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

import pyotp
import qrcode
from jose import jwt, JWTError

from app.core.hashing import verify_password, get_password_hash
from app.core.security import SECRET_KEY, ALGORITHM
from app.core.otp_security import hash_otp_code, verify_otp_code

logger = logging.getLogger(__name__)

# Institutional Trading Desk session duration (12 hours active market day)
DEALER_TOKEN_EXPIRE_HOURS = 12

# Verification Code TTL (15 minutes)
EMAIL_OTP_TTL_MINUTES = 15

# Maximum failed login attempts before temporary account lock
MAX_FAILED_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION_MINUTES = 15


def generate_totp_secret() -> str:
    """Generates a cryptographically random 32-character base32 secret for RFC 6238 TOTP."""
    return pyotp.random_base32()


def generate_totp_uri(secret: str, email: str, bank_name: str) -> str:
    """Generates the standardized RFC 6238 otpauth URI for authenticator applications."""
    totp = pyotp.TOTP(secret)
    issuer = f"Grow Treasury ({bank_name})"
    return totp.provisioning_uri(name=email, issuer_name=issuer)


def generate_qr_code_base64(uri: str) -> str:
    """Renders the otpauth URI as a base64-encoded PNG image data URL."""
    qr = qrcode.make(uri)
    buf = io.BytesIO()
    qr.save(buf, format="PNG")
    b64_str = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{b64_str}"


def verify_totp_code(secret: str, code: str) -> bool:
    """
    Verifies a 6-digit rolling TOTP code against the dealer's secret.
    Allows a 1-step window (±30 seconds) to accommodate clock drift between smartphone and server.
    """
    if not secret or not code:
        return False
    clean_code = str(code).strip().replace(" ", "").replace("-", "")
    if len(clean_code) != 6 or not clean_code.isdigit():
        return False
    try:
        totp = pyotp.TOTP(secret)
        return bool(totp.verify(clean_code, valid_window=1))
    except Exception as e:
        logger.warning(f"Error verifying TOTP code: {e}")
        return False


def create_dealer_access_token(
    dealer_id: int,
    email: str,
    bank_id: int,
    bank_name: str,
    full_name: str,
    role: str = "EXECUTION",
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Creates a signed JWT access token specifically scoped for Bank Dealer portal sessions.
    """
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(hours=DEALER_TOKEN_EXPIRE_HOURS)

    payload: Dict[str, Any] = {
        "sub": str(dealer_id),
        "dealer_id": dealer_id,
        "email": email.lower().strip(),
        "bank_id": bank_id,
        "bank_name": bank_name,
        "full_name": full_name,
        "role": role,
        "type": "bank_dealer",
        "iat": now,
        "exp": expire
    }

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_dealer_access_token(token: str) -> Dict[str, Any]:
    """
    Decodes and validates a bank dealer JWT token.
    Raises ValueError on invalid signature or expiration.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "bank_dealer":
            raise ValueError("Token is not an authorized bank dealer session token.")
        return payload
    except JWTError as e:
        raise ValueError(f"Invalid dealer authentication token: {str(e)}")


def create_enrollment_handover_token(dealer_id: int, email: str, bank_id: int) -> str:
    """
    Issues a short-lived cryptographic handover token (Gate 1 -> Gate 2)
    proving the corporate email OTP was verified.
    """
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=15)
    payload = {
        "sub": str(dealer_id),
        "email": email.lower().strip(),
        "bank_id": bank_id,
        "type": "dealer_enrollment_handover",
        "exp": expire
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_enrollment_handover_token(token: str) -> Dict[str, Any]:
    """
    Validates the Gate 1 -> Gate 2 enrollment handover token.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "dealer_enrollment_handover":
            raise ValueError("Invalid handover token type.")
        return payload
    except JWTError as e:
        raise ValueError(f"Invalid or expired enrollment token: {str(e)}")
