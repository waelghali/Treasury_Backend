# app/services/pdf_statement_parser.py
"""
PDF Statement Unlocking, Transaction Table Extraction & Bilingual Inline Cash Balance Parser.
Designed for Egyptian corporate banking workflows:
- Unlocks password-protected e-statements using stored static PDF passwords or CIF/account identifiers.
- Extracts transaction tables from grid-based and text-aligned bank PDFs into pandas DataFrames.
- Parses bilingual (English & Arabic) cash balances from email bodies and PDF balance summaries.
"""

import io
import re
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import fitz  # PyMuPDF
import pandas as pd
from pypdf import PdfReader, PdfWriter
from sqlalchemy.orm import Session

from app.core.encryption import decrypt_data
from app.models.models_issuance import CustomerBankAccount

logger = logging.getLogger("app.pdf_statement_parser")

ARABIC_INDIC_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩٫٬", "0123456789.,")

CURRENCY_ALIASES: Dict[str, str] = {
    "EGP": "EGP",
    "LE": "EGP",
    "L.E": "EGP",
    "L.E.": "EGP",
    "ج.م": "EGP",
    "جم": "EGP",
    "جنيه": "EGP",
    "جنيها": "EGP",
    "جنيه مصري": "EGP",
    "USD": "USD",
    "US$": "USD",
    "$": "USD",
    "دولار": "USD",
    "دولار أمريكي": "USD",
    "دولار امريكي": "USD",
    "EUR": "EUR",
    "€": "EUR",
    "يورو": "EUR",
    "GBP": "GBP",
    "£": "GBP",
    "استرليني": "GBP",
    "إسترليني": "GBP",
    "جنيه استرليني": "GBP",
    "SAR": "SAR",
    "ر.س": "SAR",
    "ريال": "SAR",
    "ريال سعودي": "SAR",
    "AED": "AED",
    "د.إ": "AED",
    "درهم": "AED",
    "درهم إماراتي": "AED",
}

CURRENCY_REGEX = (
    r"(?:EGP|USD|EUR|GBP|SAR|AED|US\$|L\.E\.?|LE|ج\.م|جنيه(?:\s+مصري)?|دولار(?:\s+[اأ]مريكي)?|"
    r"يورو|ريال(?:\s+سعودي)?|درهم(?:\s+[إا]ماراتي)?|[إا]سترليني|[\$€£])"
)

AVAILABLE_BALANCE_LABELS = (
    r"available\s+bal(?:ance)?|avail\.?\s*bal(?:ance)?|cleared\s+bal(?:ance)?|"
    r"الرصيد\s+المتاح|رصيد\s+متاح"
)

LEDGER_BALANCE_LABELS = (
    r"ledger\s+bal(?:ance)?|book\s+bal(?:ance)?|current\s+bal(?:ance)?|"
    r"closing\s+bal(?:ance)?|ending\s+bal(?:ance)?|account\s+bal(?:ance)?|"
    r"net\s+bal(?:ance)?|total\s+bal(?:ance)?|"
    r"الرصيد\s+الدفتري|رصيد\s+دفتري|الرصيد\s+الحالي|رصيد\s+حالي|"
    r"رصيد\s+الإقفال|رصيد\s+الاقفال|رصيد\s+[آا]خر\s+المدة|رصيد\s+الحساب|صافي\s+الرصيد"
)

OPENING_BALANCE_LABELS = (
    r"opening\s+bal(?:ance)?|previous\s+bal(?:ance)?|balance\s+b/?f|brought\s+forward|"
    r"الرصيد\s+الافتتاحي|رصيد\s+[أا]ول\s+المدة|رصيد\s+سابق"
)

GENERIC_BALANCE_LABELS = r"(?:available|ledger|closing|current|book|ending)?\s*balance|الرصيد|رصيد"

ACCOUNT_PATTERN = re.compile(
    r"(?:EG\d{25,27}|"
    r"(?:account(?:\s*(?:no\.?|number|#))?|acc(?:\s*no\.?)?|a/c(?:\s*no\.?)?|acct\.?|iban|"
    r"رقم\s*الحساب|حساب(?:\s*رقم)?)\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-\*]{3,33}))",
    re.IGNORECASE,
)

DATE_PATTERN = re.compile(
    r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})"
)


def normalize_arabic_digits(text: str) -> str:
    """Converts Arabic-Indic digits and separators into ASCII equivalents."""
    if not text:
        return ""
    return text.translate(ARABIC_INDIC_MAP)


def normalize_currency_token(raw_ccy: Optional[str], fallback: str = "EGP") -> str:
    """Maps English or Arabic currency tokens to 3-letter ISO currency codes."""
    if not raw_ccy:
        return fallback
    cleaned = raw_ccy.strip()
    upper = cleaned.upper()
    if upper in CURRENCY_ALIASES:
        return CURRENCY_ALIASES[upper]
    if cleaned in CURRENCY_ALIASES:
        return CURRENCY_ALIASES[cleaned]
    for key, iso in CURRENCY_ALIASES.items():
        if key.lower() == cleaned.lower():
            return iso
    if len(upper) == 3 and upper.isalpha():
        return upper
    return fallback


def _parse_numeric_amount(raw_num: str, context_after: str = "") -> Optional[float]:
    """Parses formatted balance strings including DR/CR or parentheses into signed floats."""
    if not raw_num:
        return None
    s = normalize_arabic_digits(raw_num).strip()
    is_negative = False
    if s.startswith("(") and s.endswith(")"):
        is_negative = True
        s = s[1:-1].strip()
    if s.startswith("-"):
        is_negative = True
        s = s[1:].strip()
    elif s.startswith("+"):
        s = s[1:].strip()

    # Remove spaces and thousand commas
    s = s.replace(" ", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            # European 1.234,56
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts[-1]) == 2 and len(parts) <= 2:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")

    try:
        val = float(Decimal(s))
    except (InvalidOperation, ValueError):
        return None

    after_low = (context_after or "").strip().lower()
    if re.match(r"^(?:dr|debit|مدين)\b", after_low):
        is_negative = True

    return -abs(val) if is_negative else val


def get_candidate_pdf_passwords(
    db: Optional[Session],
    customer_id: Optional[int],
    bank_id: Optional[int] = None,
    explicit_password: Optional[str] = None,
) -> List[str]:
    """
    Returns ordered, deduplicated PDF password candidates:
    1. Explicit user-supplied password (if any)
    2. Encrypted static statement PDF passwords stored on CustomerBankAccount
       (accounts for the target bank first, then other customer accounts)
    3. Corporate identifiers commonly used by Egyptian banks (CIF / customer_number,
       full account_number, last 6 digits, last 4 digits).
    """
    candidates: List[str] = []

    def _add(val: Optional[str]) -> None:
        if val is None:
            return
        s = str(val).strip()
        if s and s not in candidates:
            candidates.append(s)

    if explicit_password:
        _add(explicit_password)

    if db is None or customer_id is None:
        return candidates

    accounts = (
        db.query(CustomerBankAccount)
        .filter(
            CustomerBankAccount.customer_id == customer_id,
            CustomerBankAccount.is_deleted == False,
            CustomerBankAccount.is_active == True,
        )
        .all()
    )

    # Prioritize matching bank_id and default accounts first
    ordered_accounts = sorted(
        accounts,
        key=lambda a: (
            0 if (bank_id is not None and a.bank_id == bank_id) else 1,
            0 if a.is_default else 1,
            a.id,
        ),
    )

    # Pass 1: Explicit static statement PDF passwords
    for acct in ordered_accounts:
        if acct.statement_pdf_password_encrypted:
            try:
                _add(decrypt_data(acct.statement_pdf_password_encrypted))
            except Exception as e:
                logger.warning(f"Could not decrypt stored PDF password on bank account {acct.id}: {e}")

    # Pass 2: Common Egyptian bank automatic PDF passwords (CIF / Account digits)
    for acct in ordered_accounts:
        if acct.customer_number:
            _add(acct.customer_number)
        if acct.account_number:
            clean_acct = re.sub(r"[^A-Za-z0-9]", "", acct.account_number)
            _add(acct.account_number)
            _add(clean_acct)
            if len(clean_acct) >= 6:
                _add(clean_acct[-6:])
            if len(clean_acct) >= 4:
                _add(clean_acct[-4:])

    return candidates


def unlock_pdf_bytes(
    pdf_bytes: bytes,
    candidate_passwords: Optional[List[str]] = None,
) -> Tuple[bytes, bool, bool]:
    """
    Checks if a PDF is encrypted and attempts to unlock it with candidate passwords.
    Returns (resulting_pdf_bytes, was_encrypted, is_unlocked).
    """
    passwords = candidate_passwords or []

    # Try PyMuPDF first
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            if not doc.needs_pass:
                return pdf_bytes, False, True

            for pwd in passwords:
                if doc.authenticate(pwd) > 0:
                    unlocked = doc.tobytes()
                    return unlocked, True, True
        finally:
            doc.close()
    except Exception as fitz_err:
        logger.debug(f"PyMuPDF unlock check fell back to pypdf: {fitz_err}")

    # Fallback to pypdf (handles certain AES-256 / RC4 variants cleanly)
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        if not reader.is_encrypted:
            return pdf_bytes, False, True

        for pwd in passwords:
            try:
                res = reader.decrypt(pwd)
                if res:
                    writer = PdfWriter()
                    for page in reader.pages:
                        writer.add_page(page)
                    out_buf = io.BytesIO()
                    writer.write(out_buf)
                    return out_buf.getvalue(), True, True
            except Exception:
                continue
        return pdf_bytes, True, False
    except Exception as pypdf_err:
        logger.warning(f"PDF unlock inspection failed: {pypdf_err}")
        return pdf_bytes, False, False


def _extract_account_from_segment(segment: str) -> Optional[str]:
    """Extracts the most specific account/IBAN token from a line or block."""
    iban_m = re.search(r"\b(EG\d{25,27})\b", segment, re.IGNORECASE)
    if iban_m:
        return iban_m.group(1).upper()
    m = ACCOUNT_PATTERN.search(segment)
    if m:
        grp = m.group(1) if m.lastindex else m.group(0)
        if grp and any(ch.isdigit() for ch in grp):
            return grp.strip()
    return None


def extract_cash_balances_from_text(
    text: str,
    default_currency: str = "EGP",
    default_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Extracts bilingual (English & Arabic) account cash balances from free-form email text
    or PDF balance summary headers.
    Returns list of dicts:
      [{"account_number": Optional[str], "currency": str,
        "available_balance": float, "ledger_balance": Optional[float], "as_of_date": Optional[str]}]
    """
    if not text:
        return []

    norm_text = normalize_arabic_digits(text)
    lines = [ln.strip() for ln in re.split(r"[\r\n]+", norm_text) if ln.strip()]
    if not lines:
        return []

    global_date_match = DATE_PATTERN.search(norm_text)
    global_date = global_date_match.group(1) if global_date_match else default_date

    # Number token pattern (requires digit, allows commas and decimals, optional parens/sign)
    num_pat = r"(?P<num>\(?[+-]?\d{1,3}(?:[,\s]\d{3})*(?:\.\d{1,4})?\)?|\(?[+-]?\d+(?:\.\d{1,4})?\)?)"

    # Patterns matching labeled balances on a segment
    avail_re = re.compile(
        rf"(?:{AVAILABLE_BALANCE_LABELS})\s*(?:\([^\)]*\))?\s*[:=\-]?\s*(?:(?P<ccy_pre>{CURRENCY_REGEX})\s*)?{num_pat}(?:\s*(?P<ccy_post>{CURRENCY_REGEX}))?(?:\s*(?P<dc>DR|CR|debit|credit|مدين|دائن))?",
        re.IGNORECASE,
    )
    ledger_re = re.compile(
        rf"(?:{LEDGER_BALANCE_LABELS})\s*(?:\([^\)]*\))?\s*[:=\-]?\s*(?:(?P<ccy_pre>{CURRENCY_REGEX})\s*)?{num_pat}(?:\s*(?P<ccy_post>{CURRENCY_REGEX}))?(?:\s*(?P<dc>DR|CR|debit|credit|مدين|دائن))?",
        re.IGNORECASE,
    )
    # Also handle currency-prefixed / post-labeled lines like "USD Available Balance: USD 85,200.50"
    generic_ccy_bal_re = re.compile(
        rf"(?:^|\b)(?:(?P<ccy_lead>{CURRENCY_REGEX})\s+)?(?:{GENERIC_BALANCE_LABELS})\s*[:=\-]?\s*(?:(?P<ccy_pre>{CURRENCY_REGEX})\s*)?{num_pat}(?:\s*(?P<ccy_post>{CURRENCY_REGEX}))?(?:\s*(?P<dc>DR|CR|debit|credit|مدين|دائن))?",
        re.IGNORECASE,
    )

    # Track per-(account, currency) aggregated entries while preserving order
    entries_map: Dict[Tuple[Optional[str], str], Dict[str, Any]] = {}
    current_account: Optional[str] = None
    current_date: Optional[str] = global_date

    def _upsert(
        acct: Optional[str],
        ccy: str,
        avail: Optional[float] = None,
        ledger: Optional[float] = None,
        as_of: Optional[str] = None,
    ) -> None:
        key = (acct, ccy)
        if key not in entries_map:
            entries_map[key] = {
                "account_number": acct,
                "currency": ccy,
                "available_balance": avail if avail is not None else ledger,
                "ledger_balance": ledger,
                "as_of_date": as_of or current_date,
            }
        else:
            rec = entries_map[key]
            if avail is not None:
                rec["available_balance"] = avail
            if ledger is not None:
                rec["ledger_balance"] = ledger
                if rec["available_balance"] is None:
                    rec["available_balance"] = ledger
            if as_of:
                rec["as_of_date"] = as_of

    for idx, line in enumerate(lines):
        # Update contextual account number / date if present on this line
        acct_hit = _extract_account_from_segment(line)
        if acct_hit:
            current_account = acct_hit

        date_hit = DATE_PATTERN.search(line)
        if date_hit:
            current_date = date_hit.group(1)

        # Look for leading currency hint on the line (e.g. "USD Available Balance: ...")
        leading_ccy_m = re.search(rf"(?:^|\s)({CURRENCY_REGEX})(?:\s|$)", line, re.IGNORECASE)
        line_ccy_hint = normalize_currency_token(leading_ccy_m.group(1), default_currency) if leading_ccy_m else default_currency

        matched_this_line = False

        for m in avail_re.finditer(line):
            gd = m.groupdict()
            amt = _parse_numeric_amount(gd.get("num") or "", gd.get("dc") or "")
            if amt is not None:
                ccy = normalize_currency_token(gd.get("ccy_post") or gd.get("ccy_pre"), line_ccy_hint)
                _upsert(current_account, ccy, avail=amt)
                matched_this_line = True

        for m in ledger_re.finditer(line):
            gd = m.groupdict()
            amt = _parse_numeric_amount(gd.get("num") or "", gd.get("dc") or "")
            if amt is not None:
                ccy = normalize_currency_token(gd.get("ccy_post") or gd.get("ccy_pre"), line_ccy_hint)
                _upsert(current_account, ccy, ledger=amt)
                matched_this_line = True

        if not matched_this_line:
            # Check generic balance keyword (skip opening balance lines)
            if not re.search(OPENING_BALANCE_LABELS, line, re.IGNORECASE):
                for m in generic_ccy_bal_re.finditer(line):
                    gd = m.groupdict()
                    amt = _parse_numeric_amount(gd.get("num") or "", gd.get("dc") or "")
                    if amt is not None:
                        ccy = normalize_currency_token(
                            gd.get("ccy_post") or gd.get("ccy_pre") or gd.get("ccy_lead"),
                            line_ccy_hint,
                        )
                        _upsert(current_account, ccy, avail=amt)
                        matched_this_line = True

    results = [
        entry for entry in entries_map.values()
        if entry.get("available_balance") is not None
    ]
    return results


def _extract_opening_closing_meta_from_text(text: str) -> Dict[str, Any]:
    """Extracts opening/closing balance, account number, and currency from PDF text headers."""
    meta: Dict[str, Any] = {
        "opening_balance": None,
        "closing_balance": None,
        "account_number": None,
        "currency": None,
    }
    if not text:
        return meta

    norm_text = normalize_arabic_digits(text)
    acct = _extract_account_from_segment(norm_text)
    if acct:
        meta["account_number"] = acct

    num_pat = r"(?P<num>\(?[+-]?\d{1,3}(?:,\d{3})*(?:\.\d{1,4})?\)?|\(?[+-]?\d+(?:\.\d{1,4})?\)?)"
    open_re = re.compile(
        rf"(?:{OPENING_BALANCE_LABELS})\s*(?:\([^\)]*\))?\s*[:=\-]?\s*(?:(?P<ccy_pre>{CURRENCY_REGEX})\s*)?{num_pat}(?:\s*(?P<ccy_post>{CURRENCY_REGEX}))?(?:\s*(?P<dc>DR|CR|مدين|دائن))?",
        re.IGNORECASE,
    )
    close_re = re.compile(
        rf"(?:{LEDGER_BALANCE_LABELS})\s*(?:\([^\)]*\))?\s*[:=\-]?\s*(?:(?P<ccy_pre>{CURRENCY_REGEX})\s*)?{num_pat}(?:\s*(?P<ccy_post>{CURRENCY_REGEX}))?(?:\s*(?P<dc>DR|CR|debit|credit|مدين|دائن))?",
        re.IGNORECASE,
    )

    m_open = open_re.search(norm_text)
    if m_open:
        gd = m_open.groupdict()
        val = _parse_numeric_amount(gd.get("num") or "", gd.get("dc") or "")
        if val is not None:
            meta["opening_balance"] = Decimal(str(val))
        ccy_tok = gd.get("ccy_post") or gd.get("ccy_pre")
        if ccy_tok:
            meta["currency"] = normalize_currency_token(ccy_tok)

    m_close = close_re.search(norm_text)
    if m_close:
        gd = m_close.groupdict()
        val = _parse_numeric_amount(gd.get("num") or "", gd.get("dc") or "")
        if val is not None:
            meta["closing_balance"] = Decimal(str(val))
        ccy_tok = gd.get("ccy_post") or gd.get("ccy_pre")
        if ccy_tok and not meta["currency"]:
            meta["currency"] = normalize_currency_token(ccy_tok)

    return meta


def _fallback_extract_lines_table(doc: fitz.Document) -> List[List[str]]:
    """
    Fallback parser for borderless text-aligned bank PDFs (where find_tables() finds nothing).
    Detects transaction lines containing a date token and at least one numeric amount,
    splitting columns on multi-space boundaries.
    """
    tx_rows: List[List[str]] = []
    header_row: Optional[List[str]] = None

    date_line_re = re.compile(
        r"(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}|\d{1,2}[-\s][A-Za-z]{3}[-\s]\d{2,4})"
    )
    amt_re = re.compile(r"\(?[+-]?\d{1,3}(?:,\d{3})*\.\d{2}\)?")

    for page in doc:
        page_text = normalize_arabic_digits(page.get_text("text") or "")
        for raw_line in page_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            parts = [p.strip() for p in re.split(r"\s{2,}|\t|\|", line) if p.strip()]
            low_line = line.lower()

            if header_row is None and (
                ("date" in low_line or "تاريخ" in low_line)
                and any(k in low_line for k in ("debit", "credit", "amount", "balance", "مدين", "دائن", "رصيد", "مبلغ"))
                and len(parts) >= 3
            ):
                header_row = parts
                continue

            if date_line_re.search(line) and amt_re.search(line):
                if len(parts) >= 3:
                    tx_rows.append(parts)
                else:
                    # Single-space separated line fallback: [date] [desc...] [amounts...]
                    d_m = date_line_re.search(line)
                    amts = amt_re.findall(line)
                    if d_m and amts:
                        date_str = d_m.group(1)
                        # Remove date and trailing amounts to isolate description
                        rest = line.replace(date_str, "", 1)
                        for a in amts:
                            idx = rest.rfind(a)
                            if idx != -1:
                                rest = rest[:idx] + rest[idx + len(a):]
                        desc_clean = re.sub(r"\s+", " ", rest).strip() or "PDF Statement Transaction"
                        if len(amts) >= 3:
                            tx_rows.append([date_str, desc_clean, amts[0], amts[1], amts[2]])
                        elif len(amts) == 2:
                            tx_rows.append([date_str, desc_clean, amts[0], amts[1]])
                        else:
                            tx_rows.append([date_str, desc_clean, amts[0]])

    if not tx_rows:
        return []

    max_cols = max(len(r) for r in tx_rows)
    if header_row and len(header_row) == max_cols:
        return [header_row] + tx_rows

    # Synthesize clear headers when falling back to positional row splitting
    if max_cols == 5:
        synth_hdr = ["Date", "Description", "Debit", "Credit", "Balance"]
    elif max_cols == 4:
        synth_hdr = ["Date", "Description", "Amount", "Balance"]
    else:
        synth_hdr = ["Date", "Description", "Amount"] + [f"Col_{i}" for i in range(4, max_cols + 1)]

    padded = [r + [""] * (max_cols - len(r)) for r in tx_rows]
    return [synth_hdr[:max_cols]] + padded


def extract_statement_from_pdf(
    pdf_bytes: bytes,
) -> Tuple[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]]:
    """
    Extracts transaction table DataFrame, statement metadata, and summary balances from
    an UNLOCKED PDF byte stream.
    Returns (df, pdf_meta, summary_balances).
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        full_text_parts: List[str] = []
        table_rows: List[List[str]] = []
        first_header_sig: Optional[Tuple[str, ...]] = None

        for page in doc:
            full_text_parts.append(page.get_text("text") or "")
            try:
                tabs = page.find_tables()
                if tabs and getattr(tabs, "tables", None):
                    for tab in tabs.tables:
                        extracted = tab.extract() or []
                        for r_idx, row in enumerate(extracted):
                            cleaned_row = [
                                normalize_arabic_digits(str(c or "")).replace("\n", " ").strip()
                                for c in row
                            ]
                            if not any(cleaned_row):
                                continue
                            sig = tuple(c.lower() for c in cleaned_row)
                            # Skip repeated table headers on subsequent pages
                            if r_idx == 0 and first_header_sig is not None and sig == first_header_sig:
                                continue
                            if first_header_sig is None and any(
                                any(k in cell for k in ("date", "تاريخ", "debit", "credit", "amount", "balance"))
                                for cell in sig
                            ):
                                first_header_sig = sig
                            table_rows.append(cleaned_row)
            except Exception as tab_err:
                logger.debug(f"PyMuPDF find_tables failed on page: {tab_err}")

        full_text = "\n".join(full_text_parts)
        pdf_meta = _extract_opening_closing_meta_from_text(full_text)
        summary_balances = extract_cash_balances_from_text(
            full_text,
            default_currency=pdf_meta.get("currency") or "EGP",
        )

        # Fallback to text-line table extraction if find_tables() found no usable multi-row table
        if len(table_rows) < 2:
            fallback_rows = _fallback_extract_lines_table(doc)
            if len(fallback_rows) >= 2:
                table_rows = fallback_rows

        if not table_rows:
            return pd.DataFrame(), pdf_meta, summary_balances

        max_cols = max(len(r) for r in table_rows)
        normalized_rows = [r + [""] * (max_cols - len(r)) for r in table_rows]
        df = pd.DataFrame(normalized_rows)
        return df, pdf_meta, summary_balances
    finally:
        doc.close()
