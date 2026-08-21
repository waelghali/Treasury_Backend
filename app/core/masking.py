# app/core/masking.py
"""
Financial & Sensitive Data Masking Utility
Provides non-destructive masking for display, audit logs, and analytics export.
"""
from typing import Optional


def mask_iban(iban: Optional[str]) -> str:
    """
    Masks middle characters of an IBAN or bank account number.
    Example: EG12345678901234567890 -> EG12 **** **** 7890
    """
    if not iban:
        return ""
    clean = iban.strip().replace(" ", "")
    if len(clean) <= 8:
        return f"{clean[:2]}***{clean[-2:]}"
    prefix = clean[:4]
    suffix = clean[-4:]
    return f"{prefix} **** **** {suffix}"


def mask_email(email: Optional[str]) -> str:
    """
    Masks user email address for privacy.
    Example: wael@growbd.com -> w***l@growbd.com
    """
    if not email or "@" not in email:
        return email or ""
    name_part, domain = email.split("@", 1)
    if len(name_part) <= 2:
        masked_name = name_part[0] + "*"
    else:
        masked_name = name_part[0] + ("*" * (len(name_part) - 2)) + name_part[-1]
    return f"{masked_name}@{domain}"


def mask_national_id(nid: Optional[str]) -> str:
    """
    Masks National ID or Tax Identification Number.
    Example: 29001011234567 -> 2900******4567
    """
    if not nid:
        return ""
    clean = str(nid).strip()
    if len(clean) <= 6:
        return "***"
    return f"{clean[:4]}{'*' * (len(clean) - 8)}{clean[-4:]}"


def mask_token(token: Optional[str]) -> str:
    """
    Masks security token or JWT in log messages.
    Example: eyJhbGciOi...12345 -> eyJh...2345
    """
    if not token or len(token) < 10:
        return "***"
    return f"{token[:4]}...{token[-4:]}"
