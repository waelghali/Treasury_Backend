# app/services/notifications/push_config.py
"""
VAPID configuration for Web Push. Keys come from the environment ONLY:

    VAPID_PUBLIC_KEY   base64url, uncompressed P-256 point (65 bytes)
    VAPID_PRIVATE_KEY  base64url, raw P-256 private key (32 bytes)
    VAPID_SUBJECT      mailto:ops@yourdomain  (or https://...)

Behaviour:
  * All three missing  -> push channel disabled (logged at WARNING at startup);
                          the rest of the platform keeps working.
  * Partly set / malformed -> RuntimeError at startup (fail fast; a broken key
                          would otherwise silently drop every alert).
"""

import base64
import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VapidConfig:
    public_key: str
    private_key: str
    subject: str


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


from dotenv import load_dotenv

load_dotenv()

_cached_vapid_config: Optional[VapidConfig] = None


def get_vapid_config() -> Optional[VapidConfig]:
    global _cached_vapid_config
    if _cached_vapid_config is not None:
        return _cached_vapid_config

    load_dotenv()
    public_key = (os.getenv("VAPID_PUBLIC_KEY") or "").strip()
    private_key = (os.getenv("VAPID_PRIVATE_KEY") or "").strip()
    subject = (os.getenv("VAPID_SUBJECT") or "").strip()

    if not (public_key or private_key or subject):
        return None
    if not (public_key and private_key and subject):
        raise RuntimeError("Web Push: VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY and VAPID_SUBJECT must all be set (or none).")
    if not (subject.startswith("mailto:") or subject.startswith("https://")):
        raise RuntimeError("Web Push: VAPID_SUBJECT must start with 'mailto:' or 'https://'.")

    try:
        raw_public = _b64url_decode(public_key)
        raw_private = _b64url_decode(private_key)
    except Exception as exc:
        raise RuntimeError(f"Web Push: VAPID keys are not valid base64url: {exc}") from exc
    if len(raw_public) != 65 or raw_public[0] != 0x04:
        raise RuntimeError("Web Push: VAPID_PUBLIC_KEY must be an uncompressed P-256 point (65 bytes).")
    if len(raw_private) != 32:
        raise RuntimeError("Web Push: VAPID_PRIVATE_KEY must be a raw 32-byte P-256 key.")

    # The private key must match the public key, otherwise browsers reject every push.
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization
    derived = ec.derive_private_key(int.from_bytes(raw_private, "big"), ec.SECP256R1()).public_key()
    derived_raw = derived.public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
    if derived_raw != raw_public:
        raise RuntimeError("Web Push: VAPID_PRIVATE_KEY does not match VAPID_PUBLIC_KEY.")

    _cached_vapid_config = VapidConfig(public_key=public_key, private_key=private_key, subject=subject)
    return _cached_vapid_config


def push_enabled() -> bool:
    return get_vapid_config() is not None


def validate_on_startup() -> bool:
    """Called once at boot. Raises on misconfiguration; returns whether push is on."""
    config = get_vapid_config()
    if config is None:
        logger.warning("Web Push is DISABLED: VAPID_* environment variables are not set.")
        return False
    logger.info("Web Push is enabled (VAPID subject %s).", config.subject)
    return True
