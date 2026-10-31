# app/services/notifications/lg_custody_masking.py
"""
Confidentiality masking utilities for LG Custody notifications.

Strict Institutional Standard:
- Serial numbers are masked to their suffix (e.g. '...8412').
- Counterparty / Issuer names are masked keeping the initial token + initials (e.g. 'Hassan A*** C***').
- Financial amounts, credit limits, and currencies are strictly omitted from push notifications.
"""

from typing import Optional


def mask_lg_number(lg_number: Optional[str]) -> str:
    """
    Masks the LG number to display only the last 4 characters.
    Example: 'LG-2024-8412' -> '...8412'
    """
    if not lg_number:
        return "—"
    clean = str(lg_number).strip()
    if not clean:
        return "—"
    last4 = clean[-4:] if len(clean) >= 4 else clean
    return f"...{last4}"


def mask_issuer_name(issuer_name: Optional[str]) -> str:
    """
    Masks issuer/counterparty name while preserving institutional readability.
    Example:
        'Hassan Ahmed Contracting' -> 'Hassan A*** C***'
        'شركة حسن أحمد للمقاولات' -> 'شركة ح*** أ*** ل***'
        'Siemens' -> 'Sie***'
    """
    if not issuer_name:
        return "—"
    words = str(issuer_name).strip().split()
    if not words:
        return "—"

    if len(words) == 1:
        w = words[0]
        return w[:3] + "***" if len(w) > 3 else w[0] + "***"

    first_word = words[0]
    masked_rest = [w[0] + "***" if len(w) > 1 else w for w in words[1:]]
    return f"{first_word} {' '.join(masked_rest)}"
