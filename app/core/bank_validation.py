import os
import re
from typing import Set, List, Optional, Tuple
from app.models.models import Customer, Bank

# List of public, personal, and disposable email domains prohibited for bank counterparties
DISALLOWED_PUBLIC_EMAIL_DOMAINS: Set[str] = {
    # Major global free webmail providers
    "gmail.com", "googlemail.com",
    "yahoo.com", "ymail.com", "rocketmail.com", "yahoo.co.uk", "yahoo.fr",
    "hotmail.com", "outlook.com", "live.com", "msn.com",
    "icloud.com", "me.com", "mac.com",
    "proton.me", "protonmail.com",
    "mail.com", "email.com",
    "zoho.com", "zohomail.com",
    "yandex.com", "yandex.ru",
    "gmx.com", "gmx.net",
    "aol.com", "aim.com",
    # Temporary / disposable burner email services
    "mailinator.com", "tempmail.com", "10minutemail.com", "guerrillamail.com",
    "throwawaymail.com", "sharklasers.com", "dispostable.com", "trashmail.com"
}

# Whitelist patterns for authorized testing accounts (e.g. for QA, foreign counterparty simulation)
# Always includes waelghali79+*@gmail.com, plus any extra comma-separated patterns from environment
DEFAULT_TEST_PATTERNS = [
    r"^waelghali79(\+.*)?@gmail\.com$",
]

def get_authorized_test_patterns() -> List[str]:
    patterns = list(DEFAULT_TEST_PATTERNS)
    extra = os.getenv("AUTHORIZED_TEST_BANK_EMAIL_PATTERNS", "")
    if extra:
        for p in extra.split(","):
            p_clean = p.strip()
            if p_clean and p_clean not in patterns:
                patterns.append(p_clean)
    return patterns


def validate_bank_contact_email(
    email: str,
    bank: Optional[Bank] = None,
    customer: Optional[Customer] = None,
    current_user_email: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Validates a bank contact email address against counterparty integrity controls:
    1. Proper email format.
    2. Authorized testing whitelist bypass (e.g. waelghali79+*@gmail.com).
    3. Bank Official Domain Match: If the bank has an official registered email_domain (e.g. cibeg.com, nbe.com.eg),
       the contact email domain must strictly match that domain or its subdomains.
    4. Negative List: Blocked public/free/disposable email providers.
    5. Anti-Collusion: Blocked customer's own corporate domain.
    
    Returns: (is_valid: bool, error_message: Optional[str])
    """
    if not email or "@" not in email:
        return False, "Invalid email address format."

    clean_email = email.strip().lower()
    parts = clean_email.split("@")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return False, "Invalid email address format."

    domain = parts[1].strip()

    # 1. Check authorized test counterparty whitelist
    for pattern in get_authorized_test_patterns():
        if re.match(pattern, clean_email, re.IGNORECASE):
            return True, None

    # 2. Bank Official Domain Validation (if Bank has email_domain configured)
    if bank and getattr(bank, "email_domain", None):
        expected_domain = bank.email_domain.strip().lower().lstrip("@")
        if expected_domain:
            is_domain_match = (domain == expected_domain) or domain.endswith(f".{expected_domain}")
            if not is_domain_match:
                bank_label = getattr(bank, "name", None) or f"Bank #{getattr(bank, 'id', '')}"
                return False, (
                    f"Contact email domain '@{domain}' does not match the official registered domain '@{expected_domain}' for {bank_label}."
                )

    # 3. Block public/free/disposable email providers
    if domain in DISALLOWED_PUBLIC_EMAIL_DOMAINS:
        return False, (
            f"Registration with public or personal email providers (@{domain}) is prohibited for bank counterparties. "
            "Please use the representative's official corporate banking email address."
        )

    # 4. Block customer's own internal domain (Anti-collusion / self-quoting)
    customer_domains: Set[str] = set()
    if customer:
        if getattr(customer, "domains", None) and isinstance(customer.domains, list):
            for d in customer.domains:
                if d and isinstance(d, str):
                    customer_domains.add(d.strip().lower().lstrip("@"))
        if getattr(customer, "contact_email", None) and "@" in customer.contact_email:
            c_domain = customer.contact_email.split("@")[1].strip().lower()
            if c_domain not in DISALLOWED_PUBLIC_EMAIL_DOMAINS:
                customer_domains.add(c_domain)

    if current_user_email and "@" in current_user_email:
        u_domain = current_user_email.split("@")[1].strip().lower()
        if u_domain not in DISALLOWED_PUBLIC_EMAIL_DOMAINS:
            customer_domains.add(u_domain)

    if domain in customer_domains:
        return False, (
            f"Bank representative email cannot use your organization's internal domain (@{domain}) "
            "to prevent conflict of interest and unauthorized self-quoting."
        )

    return True, None
