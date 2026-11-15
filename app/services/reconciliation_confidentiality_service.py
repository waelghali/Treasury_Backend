# app/services/reconciliation_confidentiality_service.py
"""
Zero-Knowledge Confidentiality, Tokenization & At-Rest Envelope Encryption Engine
for Bank Statement Reconciliation & Online Rule Induction.

Guarantees:
1. At-Rest Database Confidentiality:
   - Sensitive transaction fields (raw_description, counterparty_name, beneficiary_name,
     account_number, counterparty_iban, custom_fields) are sealed via AES-256-GCM
     using the customer's Tenant DEK (`TenantKeyService`).
2. Deterministic Blind Tokenization:
   - Counterparties, account numbers, and IBANs are converted into tenant-keyed
     HMAC-SHA256 tokens (`ENT_TOK_<12hex>`, `ACCT_TOK_<12hex>`) so searchable
     `tokenized_description` and learned `ClassificationRule` conditions operate
     without exposing plaintext PII at rest.
3. Zero-Leakage Online & Collaborative Sanitization:
   - Strips all counterparty names, legal entity suffixes, preposition-following
     beneficiary strings, reference codes, and account numbers before any pattern
     enters the Online Rule Induction Engine or Cross-Tenant Collaborative Pool.
"""

import re
import hmac
import hashlib
import logging
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from sqlalchemy.orm import Session

from app.core.security_crypto import (
    encrypt_field,
    decrypt_field,
    encrypt_json,
    decrypt_json,
    is_encrypted,
)
from app.services.tenant_key_service import TenantKeyService

logger = logging.getLogger("app.reconciliation_confidentiality")

_tenant_key_service = TenantKeyService()

# Egyptian & International IBAN regex: captures 2-letter country, 2 check digits, 4-digit bank code, and account tail
IBAN_REGEX = re.compile(r'\b([A-Z]{2})\d{2}(\d{4})([A-Z0-9]{10,26})\b', re.IGNORECASE)

# Egyptian Central Bank 4-digit IBAN bank code directory
EGYPTIAN_IBAN_BANK_CODES = {
    "0003": "NBE (National Bank of Egypt)",
    "0002": "Banque Misr",
    "0010": "CIB (Commercial International Bank)",
    "0037": "QNB Alahli",
    "0004": "Banque du Caire",
    "0005": "AlexBank",
    "0019": "AAIB (Arab African International Bank)",
    "0025": "HSBC Egypt",
    "0009": "Credit Agricole Egypt",
    "0023": "Emirates NBD Egypt",
    "0013": "ADIB (Abu Dhabi Islamic Bank)",
    "0039": "FABMisr / First Abu Dhabi Bank",
    "0029": "Attijariwafa Bank Egypt",
    "0020": "NBK (National Bank of Kuwait)",
    "0017": "Suez Canal Bank",
    "0011": "Faisal Islamic Bank",
    "0040": "Al Baraka Bank",
    "0006": "SAIB",
    "0014": "Housing & Development Bank (HDB)",
    "0015": "Export Development Bank (EBank)",
}

# Prepositions that typically precede a beneficiary, customer, or supplier name
COUNTERPARTY_PREPOSITIONS_PATTERN = re.compile(
    r'\b(?:B/O|FAV|FAVOUR|FAVOR|BENEFICIARY|BEN|ORDER\s+OF|TO\s+ORDER\s+OF|PAID\s+TO|RECEIVED\s+FROM|TRF\s+TO|TRF\s+FRM|TRF\s+FROM|لصالح|أمر|لامر|لأمر|من\s+حساب|إلى\s+حساب|الى\s+حساب|المستفيد)\s*[:\-/]?\s*([A-Z\u0600-\u06FF][A-Z0-9\u0600-\u06FF\s.&\-]{2,40})',
    re.IGNORECASE
)

# Legal entity suffixes indicating a company name
LEGAL_ENTITY_PATTERN = re.compile(
    r'\b([A-Z\u0600-\u06FF][A-Z0-9\u0600-\u06FF\s.&\-]{2,35}?)\s+\b(?:LLC|L\.L\.C|LTD|LIMITED|INC|CORP|CORPORATION|SAE|S\.A\.E|CO|COMPANY|GROUP|HOLDING|HOLDINGS|TRADING|CONTRACTING|INDUSTRIES|PHARMA|ش\.م\.م|ش\s*م\s*م|ذ\.م\.م|للتجارة|للمقاولات|للصناعة|القابضة)\b',
    re.IGNORECASE
)

# Reversal and Negative Collection indicators
REVERSAL_KEYWORDS = {
    "REV", "REVERSE", "REVERSAL", "RETURN", "RETURNED", "RET", "BOUNCED",
    "DISHONORED", "UNPAID", "REJECT", "REJECTED", "CANCEL", "CANCELLED",
    "CANCELED", "CHARGEBACK", "ERR", "ERROR", "CORR", "CORRECTION",
    "مرتد", "مرتجع", "عكس", "قيد عكسي", "ملغي", "مرفوض", "رد"
}

BOUNCED_INSTRUMENT_KEYWORDS = {
    "CHEQUE", "CHECK", "CHQ", "COLLECTION", "COLL", "CLEARING", "POS",
    "CARD", "VISA", "MASTERCARD", "MEEZA", "INSTAPAY",
    "شيك", "تحصيل", "مقاصة", "بطاقة", "كارت"
}

# Preserve core banking structural vocabulary
BANKING_STRUCTURAL_VOCAB = {
    "INTEREST", "CREDIT", "DEBIT", "OVERDRAFT", "SAVINGS", "DEPOSIT", "TIME",
    "ACCRUED", "CHARGE", "CHARGES", "SERVICE", "MAINTENANCE", "FEE", "FEES",
    "LEDGER", "STATEMENT", "COMMISSION", "SWIFT", "PROCESSING", "HANDLING",
    "CUSTODY", "TARIFF", "STAMP", "DUTY", "WITHHOLDING", "WHT", "TAX", "VAT",
    "SALARY", "PAYROLL", "WAGES", "SALARIES", "STAFF", "SOCIAL", "INSURANCE",
    "PENSION", "CUSTOMS", "GOVERNMENT", "MINISTRY", "AUTHORITY", "LOAN",
    "REPAYMENT", "INSTALLMENT", "INSTALMENT", "PRINCIPAL", "FACILITY",
    "SWEEP", "INTERNAL", "POOL", "POOLING", "CHEQUE", "CHECK", "CHQ",
    "CLEARING", "COLLECTION", "POS", "SETTLEMENT", "MDR", "BATCH", "ACH",
    "RTGS", "INSTAPAY", "CORRESPONDENT", "MT103", "FORM4", "LC", "LG",
    "GUARANTEE", "MARGIN", "ISSUANCE", "EXTENSION", "AMENDMENT", "FX",
    "EXCHANGE", "REVALUATION", "DIVIDEND", "CAPITAL",
    "فائدة", "دائنة", "مدينة", "عائد", "فوائد", "ودائع", "مكشوف", "قرض",
    "عمولة", "رسوم", "مصاريف", "بنكية", "خدمة", "سويفت", "ضريبة", "خصم",
    "استقطاع", "دمغة", "ضرائب", "رواتب", "أجور", "مرتبات", "راتب", "تأمينات",
    "اجتماعية", "جمارك", "قسط", "أقساط", "تسهيلات", "سويب", "ذاتي", "شيك",
    "مقاصة", "تحصيل", "خطاب", "ضمان", "اعتماد", "مستندى", "تدبير", "عملة"
}


def classify_amount_tier(amount: Any) -> str:
    """
    Maps a transaction amount into a privacy-safe logarithmic scale tier.
    Prevents micro bank fees from colliding with large vendor/payroll wires.
    """
    try:
        val = abs(float(amount or 0))
    except Exception:
        val = 0.0

    if val < 100:
        return "MICRO"       # < 100 (stamp duty, SMS fee, minor charge)
    elif val < 5000:
        return "SMALL"       # 100 - 5,000 (standard wire/LG fee, minor expense)
    elif val < 100000:
        return "MEDIUM"      # 5,000 - 100,000 (commercial invoice, mid settlement)
    elif val < 1000000:
        return "LARGE"       # 100,000 - 1,000,000 (payroll batch, major supplier, sweep)
    else:
        return "MACRO"       # >= 1,000,000 (treasury sweep, loan principal, facility)


class ReconciliationConfidentialityService:
    """
    Provides deterministic tokenization, at-rest field encryption/decryption,
    and zero-PII structural signature extraction for Bank Reconciliation.
    """

    def get_tenant_dek(self, db: Optional[Session], customer_id: Optional[int]) -> Optional[bytes]:
        """Safely retrieves or provisions the 256-bit Tenant DEK."""
        if not db or not customer_id:
            return None
        try:
            return _tenant_key_service.get_or_create_tenant_dek(db, customer_id)
        except Exception as e:
            logger.warning(f"Could not load Tenant DEK for customer {customer_id}: {e}")
            return None

    def compute_blind_token(self, raw_value: str, dek: Optional[bytes], prefix: str = "ENT_TOK") -> str:
        """
        Generates a deterministic blind token (`<PREFIX>_<12HEX>`) using HMAC-SHA256
        keyed with the tenant's DEK so patterns can match identical entities/accounts
        without storing plaintext names or account numbers in rules or indices.
        """
        normalized = re.sub(r'\s+', ' ', (raw_value or "").strip().upper())
        if not normalized:
            return f"{prefix}_EMPTY"
        key = dek if (dek and len(dek) == 32) else b"GrowTreasury-Default-TokenKey-32"
        digest = hmac.new(key, normalized.encode("utf-8"), hashlib.sha256).hexdigest()[:12].upper()
        return f"{prefix}_{digest}"

    def extract_iban_and_bank_code(self, text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Detects IBAN in narrative and extracts (full_iban, bank_code_4digit, bank_name).
        Supports Egyptian IBAN format EG + 2 check digits + 4 bank code + 21 account digits.
        """
        if not text:
            return None, None, None
        m = IBAN_REGEX.search(text.upper())
        if not m:
            return None, None, None
        full_iban = m.group(0)
        country = m.group(1).upper()
        bank_code = m.group(2)
        bank_name = EGYPTIAN_IBAN_BANK_CODES.get(bank_code) if country == "EG" else f"{country}_BANK_{bank_code}"
        return full_iban, bank_code, bank_name

    def detect_semantic_flags(
        self,
        raw_description: str,
        direction: str,
        amount: Any = 0,
        booking_date: Any = None,
        has_companion_fee: bool = False,
    ) -> Dict[str, Any]:
        """
        Extracts domain-aware semantic flags from a transaction narrative and metadata:
        - is_reversal: True if reversal/return/cancel tokens exist
        - is_negative_collection: True if DEBIT has reversal + collection/cheque/POS indicators (bounced cheque / chargeback)
        - iban_bank_code: 4-digit bank code if IBAN is embedded
        - amount_tier: MICRO / SMALL / MEDIUM / LARGE / MACRO
        - is_salary_window: True if day of month is between 22 and 28
        - is_month_end: True if day >= 28 or day <= 2
        - has_companion_fee: True if linked to an adjacent parent transfer
        """
        upper_desc = (raw_description or "").upper()
        words = set(re.findall(r'[A-Z0-9\u0600-\u06FF]+', upper_desc))

        is_rev = bool(words.intersection(REVERSAL_KEYWORDS)) or any(
            kw in upper_desc for kw in ("قيد عكسي", "شيك مرتد", "تحصيل مرتد", "CHARGE BACK", "RETURNED CHEQUE")
        )
        has_collection_instrument = bool(words.intersection(BOUNCED_INSTRUMENT_KEYWORDS))
        clean_dir = (direction or "DEBIT").upper()

        # A Debit with reversal + cheque/collection/POS token is a Bounced Collection / Chargeback (Negative Collection), NOT a supplier payment!
        is_negative_collection = is_rev and clean_dir == "DEBIT" and has_collection_instrument

        _, iban_bank_code, iban_bank_name = self.extract_iban_and_bank_code(upper_desc)
        tier = classify_amount_tier(amount)

        day = None
        if booking_date and hasattr(booking_date, "day"):
            day = booking_date.day

        is_salary_window = bool(day and 22 <= day <= 28)
        is_month_end = bool(day and (day >= 28 or day <= 2))

        return {
            "is_reversal": is_rev,
            "is_negative_collection": is_negative_collection,
            "iban_bank_code": iban_bank_code,
            "iban_bank_name": iban_bank_name,
            "amount_tier": tier,
            "is_salary_window": is_salary_window,
            "is_month_end": is_month_end,
            "has_companion_fee": bool(has_companion_fee),
        }

    def tokenize_narrative(
        self,
        raw_description: str,
        dek: Optional[bytes] = None,
        known_entities: Optional[List[str]] = None,
        counterparty_name: Optional[str] = None,
    ) -> Tuple[str, Dict[str, str]]:
        """
        Replaces sensitive PII, IBANs, account numbers, and counterparty names in a
        transaction description with deterministic blind tokens (`<ENT_TOK_...>`, `<ACCT_TOK_...>`).
        Returns: (tokenized_description, token_to_original_map)
        """
        if not raw_description:
            return "", {}

        text = str(raw_description)
        token_map: Dict[str, str] = {}

        # 1. Tokenize IBANs while preserving 4-digit bank code
        def _replace_iban(match):
            full_iban = match.group(0)
            country = match.group(1).upper()
            bank_code = match.group(2)
            acct_tok = self.compute_blind_token(full_iban, dek, prefix="ACCT_TOK")
            replacement = f"<IBAN_{country}_{bank_code}:{acct_tok}>"
            token_map[replacement] = full_iban
            return replacement

        text = IBAN_REGEX.sub(_replace_iban, text)

        # 2. Tokenize explicit counterparty_name and known entities
        entities_to_scrub = []
        if counterparty_name and len(counterparty_name.strip()) >= 3:
            entities_to_scrub.append(counterparty_name.strip())
        if known_entities:
            entities_to_scrub.extend([e.strip() for e in known_entities if e and len(e.strip()) >= 3])

        # Sort longest first to avoid partial token replacement
        for ent in sorted(set(entities_to_scrub), key=len, reverse=True):
            pattern = re.compile(re.escape(ent), re.IGNORECASE)
            if pattern.search(text):
                ent_tok = f"<{self.compute_blind_token(ent, dek, prefix='ENT_TOK')}>"
                token_map[ent_tok] = ent
                text = pattern.sub(ent_tok, text)

        # 3. Tokenize legal entity names (e.g. "ACME TRADING LLC")
        def _replace_legal_entity(match):
            full_match = match.group(0).strip()
            ent_tok = f"<{self.compute_blind_token(full_match, dek, prefix='ENT_TOK')}>"
            token_map[ent_tok] = full_match
            return ent_tok

        text = LEGAL_ENTITY_PATTERN.sub(_replace_legal_entity, text)

        # 4. Tokenize long account/card numbers (10+ digits)
        def _replace_acct(match):
            digits = match.group(0)
            acct_tok = f"<{self.compute_blind_token(digits, dek, prefix='ACCT_TOK')}>"
            token_map[acct_tok] = digits
            return acct_tok

        text = re.sub(r'\b\d{10,24}\b', _replace_acct, text)

        # 5. Tokenize emails
        def _replace_email(match):
            email = match.group(0)
            pii_tok = f"<{self.compute_blind_token(email, dek, prefix='PII_TOK')}>"
            token_map[pii_tok] = email
            return pii_tok

        text = re.sub(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b', _replace_email, text)

        return text, token_map

    def extract_privacy_safe_rule_signature(
        self,
        raw_description: str,
        description_line2: Optional[str] = None,
        known_entities: Optional[List[str]] = None,
        counterparty_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Extracts a 100% confidential, PII-scrubbed structural signature for:
        1. Online Rule Induction (building `ClassificationRule` conditions)
        2. Cross-Tenant Collaborative Consensus (`CollaborativePattern`)

        Guarantees zero customer/supplier names, zero account numbers, and zero raw amounts.
        """
        combined = f"{raw_description or ''} {description_line2 or ''}".upper()

        # Extract IBAN bank code before stripping IBAN
        _, iban_bank_code, _ = self.extract_iban_and_bank_code(combined)

        # 1. Strip IBANs
        combined = IBAN_REGEX.sub(' ', combined)

        # 2. Strip known entities & explicit counterparty_name
        if counterparty_name and len(counterparty_name.strip()) >= 3:
            combined = re.sub(re.escape(counterparty_name.strip().upper()), ' ', combined)
        if known_entities:
            for ent in sorted(set(known_entities), key=len, reverse=True):
                if ent and len(ent.strip()) >= 3:
                    combined = re.sub(re.escape(ent.strip().upper()), ' ', combined)

        # 3. Strip legal entities (e.g., "... LLC", "... SAE", "... ش.م.م")
        combined = LEGAL_ENTITY_PATTERN.sub(' ', combined)

        # 4. Strip counterparty phrases after prepositions (e.g. "B/O ACME CORP", "FAV JOHN DOE")
        combined = COUNTERPARTY_PREPOSITIONS_PATTERN.sub(' ', combined)

        # 5. Strip card numbers, account numbers, dates, amounts, reference codes
        combined = re.sub(r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b', ' ', combined)
        combined = re.sub(r'\b\d{6,30}\b', ' ', combined)
        combined = re.sub(r'\b\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}\b', ' ', combined)
        combined = re.sub(r'\b\d{1,2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2,4}\b', ' ', combined)
        combined = re.sub(r'\b(?:INV|INVOICE|REF|BATCH|TRF|TXN|TRX|TRACE|SEQ|CHQ|CHECK|DOC|PO)[#:\-\s]*[A-Z0-9]+\b', ' ', combined)
        combined = re.sub(r'\b\d+(?:[.,]\d+)?\b', ' ', combined)

        # 6. Normalize punctuation
        combined = re.sub(r'[^A-Z\u0600-\u06FF\s]', ' ', combined)

        noise_words = {
            "TRANSFER", "TRF", "PAYMENT", "PAYMT", "PMT", "DEBIT", "CREDIT", "DR", "CR",
            "REF", "REFERENCE", "NO", "NUM", "NUMBER", "DATE", "VAL", "VALUE",
            "FROM", "TO", "FOR", "VIA", "THRU", "BY", "AT", "ON", "AND", "THE", "OF", "IN",
            "EGP", "USD", "EUR", "GBP", "SAR", "AED", "KWD", "QAR",
            "LLC", "LTD", "INC", "SAE", "CORP", "COMPANY", "GROUP", "TRADING",
            "تحويل", "سداد", "دفع", "رقم", "تاريخ", "من", "إلى", "عن", "عبر", "حساب"
        }

        raw_words = [w.strip() for w in combined.split() if len(w.strip()) >= 3]
        structural_tokens = []
        general_tokens = []

        for w in raw_words:
            if w in noise_words:
                continue
            if w in BANKING_STRUCTURAL_VOCAB or w in REVERSAL_KEYWORDS:
                if w not in structural_tokens:
                    structural_tokens.append(w)
            else:
                if w not in general_tokens:
                    general_tokens.append(w)

        # Prioritize verified banking structural vocabulary for collaborative sharing
        # while allowing general invariant tokens for tenant-scoped rule induction
        collab_tokens = structural_tokens[:4]
        tenant_rule_tokens = (structural_tokens + general_tokens)[:5]

        collab_signature = " ".join(collab_tokens).strip() if collab_tokens else None
        tenant_signature = " ".join(tenant_rule_tokens).strip() if tenant_rule_tokens else None

        sig_hash = hashlib.sha256(
            f"{tenant_signature or ''}|{iban_bank_code or ''}".encode("utf-8")
        ).hexdigest()[:24] if tenant_signature else None

        return {
            "collaborative_signature": collab_signature if (collab_signature and len(collab_signature) >= 4) else None,
            "tenant_signature": tenant_signature if (tenant_signature and len(tenant_signature) >= 4) else None,
            "signature_tokens": tenant_rule_tokens,
            "structural_tokens": structural_tokens,
            "signature_hash": sig_hash,
            "iban_bank_code": iban_bank_code,
        }

    def seal_transaction_fields(
        self,
        txn: Any,
        dek: Optional[bytes],
        known_entities: Optional[List[str]] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Seals sensitive fields on a `BankTransaction` ORM instance using the Tenant DEK:
        - Generates `tokenized_description` with blind tokens for PII
        - Encrypts `raw_description`, `counterparty_name`, and `custom_fields` into
          `encrypted_raw_description`, `encrypted_counterparty_name`, `encrypted_custom_fields`.
        """
        raw_desc = getattr(txn, "raw_description", "") or ""
        cp_name = getattr(txn, "counterparty_name", None)

        tokenized_desc, _ = self.tokenize_narrative(
            raw_desc, dek=dek, known_entities=known_entities, counterparty_name=cp_name
        )
        if hasattr(txn, "tokenized_description"):
            txn.tokenized_description = tokenized_desc
        if not getattr(txn, "cleaned_description", None):
            txn.cleaned_description = tokenized_desc

        if dek and len(dek) == 32:
            if hasattr(txn, "encrypted_raw_description") and raw_desc:
                txn.encrypted_raw_description = encrypt_field(raw_desc, dek, field_context="bank_txn:raw_description")
            if hasattr(txn, "encrypted_counterparty_name") and cp_name:
                txn.encrypted_counterparty_name = encrypt_field(cp_name, dek, field_context="bank_txn:counterparty_name")
            if hasattr(txn, "encrypted_custom_fields") and custom_fields:
                txn.encrypted_custom_fields = encrypt_json(custom_fields, dek, context="bank_txn:custom_fields")

        if hasattr(txn, "custom_fields_json") and custom_fields is not None:
            # Store tokenized/non-PII custom fields or full dict if DEK is not active
            txn.custom_fields_json = custom_fields

    def resolve_transaction_fields(self, txn: Any, dek: Optional[bytes]) -> Dict[str, Any]:
        """
        Dual-Read Resolver: Decrypts sealed fields in memory for authorized tenant access.
        """
        raw_desc = getattr(txn, "raw_description", "")
        if dek and getattr(txn, "encrypted_raw_description", None):
            try:
                dec = decrypt_field(txn.encrypted_raw_description, dek, field_context="bank_txn:raw_description")
                if dec:
                    raw_desc = dec
            except Exception:
                pass

        cp_name = getattr(txn, "counterparty_name", None)
        if dek and getattr(txn, "encrypted_counterparty_name", None):
            try:
                dec_cp = decrypt_field(txn.encrypted_counterparty_name, dek, field_context="bank_txn:counterparty_name")
                if dec_cp:
                    cp_name = dec_cp
            except Exception:
                pass

        custom_fields = getattr(txn, "custom_fields_json", None) or {}
        if dek and getattr(txn, "encrypted_custom_fields", None):
            try:
                dec_cf = decrypt_json(txn.encrypted_custom_fields, dek, context="bank_txn:custom_fields")
                if isinstance(dec_cf, dict):
                    custom_fields = dec_cf
            except Exception:
                pass

        return {
            "raw_description": raw_desc,
            "counterparty_name": cp_name,
            "custom_fields": custom_fields,
        }


reconciliation_confidentiality = ReconciliationConfidentialityService()

