from app.core.ai_integration import _LazyModule, _reclaim_system_memory
pd = _LazyModule("pandas")
import io
import re
import csv
import math
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, func
from fastapi import HTTPException

from app.models.models_reconciliation_v2 import (
    BankStatement, BankTransaction, ReconciliationMatch, InternalLedgerRecord,
    ClassificationRule, Counterparty, BankAccountProfile, MatchingToleranceRule,
    StatementInputMappingProfile, ReconciliationExportProfile
)
from app.models import LGRecord
from app.crud.crud_reconciliation_v2 import crud_bank_statement, crud_bank_transaction
from app.services.reconciliation_confidentiality_service import (
    reconciliation_confidentiality,
    classify_amount_tier,
)
from app.services.banking_standards_ontology import banking_standards_ontology
from app.core.security_crypto import encrypt_field

import logging

logger = logging.getLogger("app.bank_reconciliation")


class BankReconciliationService:
    """
    Enterprise Bank Statement Reconciliation, Online Rule Induction,
    Schema-Agnostic Ingestion, Multi-Pass Optimal Matching, and Customizable Export Engine.
    """

    # ═══════════════════════════════════════════════════════════════════
    #  BUILT-IN CLASSIFIERS — Universal patterns that work for ALL customers
    # ═══════════════════════════════════════════════════════════════════
    BUILTIN_CLASSIFIERS = [
        {
            "name": "REVERSED_COLLECTION_BOUNCED",
            "category": "REVERSED_COLLECTION",
            "sub_category": "BOUNCED_CHEQUE_OR_CHARGEBACK",
            "keywords": [
                "returned cheque", "bounced cheque", "dishonored cheque", "unpaid cheque",
                "returned check", "bounced chq", "chq returned", "cheque returned",
                "pos chargeback", "card chargeback", "collection reversal",
                "شيك مرتد", "شيك مرفوض", "تحصيل مرتد", "عكس تحصيل", "مرتد مقاصة"
            ],
            "direction": "DEBIT",
            "confidence": 92
        },
        {
            "name": "BANK_INTEREST_EARNED",
            "category": "BANK_INTEREST",
            "sub_category": "INTEREST_EARNED",
            "keywords": [
                "interest credit", "interest earned", "int credit", "int earned",
                "deposit interest", "savings interest", "accrued interest",
                "فائدة دائنة", "عائد", "فوائد الودائع", "فائدة على الحساب"
            ],
            "direction": "CREDIT",  # Only match credits
            "confidence": 90
        },
        {
            "name": "BANK_INTEREST_CHARGED",
            "category": "BANK_INTEREST",
            "sub_category": "INTEREST_CHARGED",
            "keywords": [
                "interest debit", "interest charge", "int debit", "int charge",
                "overdraft interest", "loan interest", "debit interest",
                "فائدة مدينة", "فائدة على المكشوف", "فائدة القرض"
            ],
            "direction": "DEBIT",
            "confidence": 90
        },
        {
            "name": "BANK_CHARGES",
            "category": "BANK_CHARGES",
            "sub_category": None,
            "keywords": [
                "bank charge", "service charge", "account fee", "maintenance fee",
                "ledger fee", "statement fee", "commission", "bank fee",
                "swift charge", "transfer fee", "processing fee", "handling fee",
                "custody fee", "account charge", "admin fee", "tariff", "postage",
                "عمولة", "رسوم", "مصاريف بنكية", "رسوم الحساب", "رسوم الخدمة",
                "مصاريف", "رسوم تحويل", "عمولة تحصيل", "مصاريف ادارية"
            ],
            "direction": "DEBIT",
            "confidence": 85
        },
        {
            "name": "TAX_DEDUCTION",
            "category": "TAX_DEDUCTION",
            "sub_category": None,
            "keywords": [
                "withholding tax", "wht", "tax deduction", "vat", "stamp duty",
                "tax on interest", "tax deducted", "proportional stamp",
                "ضريبة", "ضريبة خصم", "ضريبة استقطاع", "ضريبة القيمة المضافة",
                "دمغة", "ضرائب", "دمغة نسبية"
            ],
            "direction": "DEBIT",
            "confidence": 85
        },
        {
            "name": "SALARY_PAYROLL",
            "category": "SALARY_PAYROLL",
            "sub_category": None,
            "keywords": [
                "salary", "payroll", "wages", "salaries", "staff pay",
                "employee pay", "net salary", "gross salary",
                "رواتب", "أجور", "مرتبات", "راتب"
            ],
            "direction": "DEBIT",
            "confidence": 85
        },
        {
            "name": "GOVERNMENT_PAYMENT",
            "category": "GOVERNMENT_PAYMENT",
            "sub_category": None,
            "keywords": [
                "social insurance", "pension", "customs", "excise",
                "government", "ministry", "authority", "cps", "e-finance",
                "تأمينات اجتماعية", "جمارك", "حكومة", "هيئة", "مصلحة", "مدفوعات حكومية"
            ],
            "direction": None,
            "confidence": 70
        },
        {
            "name": "LOAN_REPAYMENT",
            "category": "LOAN_REPAYMENT",
            "sub_category": None,
            "keywords": [
                "loan repayment", "installment", "instalment", "emi",
                "loan payment", "principal repayment", "facility repayment",
                "قسط", "سداد قرض", "أقساط", "تسهيلات"
            ],
            "direction": "DEBIT",
            "confidence": 80
        },
        {
            "name": "INTER_BANK_SWEEP",
            "category": "INTER_BANK_SWEEP",
            "sub_category": "LIQUIDITY_SWEEP",
            "keywords": [
                "sweep from", "sweep to", "sweep", "internal transfer", "own account",
                "inter-account", "intercompany transfer", "cash pool", "pooling",
                "تحويل بين الحسابات", "سويب", "تحويل ذاتي"
            ],
            "direction": None,
            "confidence": 90
        },
        {
            "name": "CHEQUE_TRANSACTION",
            "category": "CHEQUE",
            "sub_category": None,
            "keywords": [
                "cheque", "check", "chq", "chq no",
                "شيك", "صك"
            ],
            "direction": None,
            "confidence": 70
        },
        {
            "name": "BANK_INTEREST_GENERIC",
            "category": "BANK_INTEREST",
            "sub_category": None,
            "keywords": [
                "interest", "فائدة", "فوائد"
            ],
            "direction": None,  # Either direction fallback
            "confidence": 75
        },
    ]

    # Sweep/transfer keywords used in cross-bank detection
    SWEEP_KEYWORDS = [
        "sweep", "transfer", "own account", "internal transfer",
        "a/c transfer", "fund transfer", "between accounts",
        "تحويل", "تحويل داخلي", "بين الحسابات", "تحويل ذاتي"
    ]

    MONTH_MAP = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
        'يناير': 1, 'فبراير': 2, 'مارس': 3, 'ابريل': 4, 'مايو': 5, 'يونيو': 6,
        'يوليو': 7, 'اغسطس': 8, 'سبتمبر': 9, 'اكتوبر': 10, 'نوفمبر': 11, 'ديسمبر': 12
    }

    _ARABIC_DIGIT_TRANS = str.maketrans(
        "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹٫٬",
        "01234567890123456789.,",
    )

    def _clean_decimal(self, val: Any) -> Decimal:
        if pd.isna(val) or val == "" or val is None:
            return Decimal("0.00")
        if isinstance(val, (int, float, Decimal)) and not isinstance(val, bool):
            try:
                return Decimal(str(val)).quantize(Decimal("1.00"))
            except (InvalidOperation, ValueError):
                return Decimal("0.00")
        try:
            s = str(val).translate(self._ARABIC_DIGIT_TRANS).strip()
            if not s or s.lower() in ("nan", "none", "null"):
                return Decimal("0.00")
            s_up = s.upper()
            # Handle accounting parentheses (1,250.00), leading/trailing minus (15,000.00-), or DR/مدين suffix
            is_neg = bool(
                (s.startswith("(") and s.endswith(")"))
                or s.startswith("-")
                or s.startswith("−")
                or s.endswith("-")
                or s.endswith("−")
                or re.search(r"(?:\bDR|\bDEBIT|مدين)\s*$", s_up)
            )
            num_part = re.sub(r"[^\d.,]", "", s)
            if not num_part or not re.search(r"\d", num_part):
                return Decimal("0.00")

            # Resolve US/UK (1,250,000.50) vs European (1.250.000,50) decimal/thousand separators
            if "." in num_part and "," in num_part:
                if num_part.rfind(",") > num_part.rfind("."):
                    # European format: 1.250.000,50
                    num_part = num_part.replace(".", "").replace(",", ".")
                else:
                    # Standard US/UK format: 1,250,000.50
                    num_part = num_part.replace(",", "")
            elif "," in num_part:
                # Single comma with 1-2 trailing digits -> decimal comma (e.g. 0,00 or 15400,25)
                if num_part.count(",") == 1 and len(num_part.rsplit(",", 1)[1]) in (1, 2):
                    num_part = num_part.replace(",", ".")
                else:
                    num_part = num_part.replace(",", "")
            elif num_part.count(".") > 1:
                # Multiple dots with no comma -> dot thousands (e.g. 1.250.000)
                num_part = num_part.replace(".", "")

            if not num_part or num_part == ".":
                return Decimal("0.00")
            dec = Decimal(num_part).quantize(Decimal("1.00"))
            if is_neg and dec > 0:
                dec = -dec
            return dec
        except (InvalidOperation, ValueError):
            return Decimal("0.00")

    def _sanitize_text(self, val: Any) -> str:
        """Strips illegal XML/control characters and normalizes Unicode/Arabic spacing."""
        if pd.isna(val) or val is None:
            return ""
        s = str(val)
        if s.lower() == "nan":
            return ""
        # Remove control chars that break Excel/JSON
        s = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', ' ', s)
        return re.sub(r'\s+', ' ', s).strip()

    def _resolve_transaction_date(self, date_val: Any, narrative: str = "", metadata: Dict[str, Any] = None) -> Optional[datetime]:
        """
        Contextual Multi-Signal Date Resolution Engine:
        Prevents date format inversion (e.g. 02/05/2026 vs 05/02/2026) using:
        1. Narrative month tokens (e.g., '05FEB2026', '05-FEB-26', 'FEB', 'فبراير')
        2. Statement period metadata (e.g. start_date/end_date month boundaries)
        3. Standard dayfirst=True for Egyptian & Middle Eastern banking formats
        """
        if pd.isna(date_val) or date_val is None:
            return None

        if isinstance(date_val, datetime):
            return date_val

        narrative_upper = (narrative or "").upper()

        # Signal 1: Month tokens in narrative (e.g. 05FEB2026, 05-FEB-26)
        month_token_match = re.search(r'\b(\d{1,2})[-/]?([A-Z]{3})[-/]?(\d{2,4})?\b', narrative_upper)
        if month_token_match:
            day_str = month_token_match.group(1)
            month_str = month_token_match.group(2)
            year_str = month_token_match.group(3)
            if month_str in self.MONTH_MAP:
                m = self.MONTH_MAP[month_str]
                d = int(day_str)
                y = int(year_str) if year_str else 2026
                if len(str(y)) == 2:
                    y += 2000
                try:
                    return datetime(y, m, d)
                except Exception:
                    pass

        # Signal 2: Smart ISO vs Day-First parse (prevents YYYY-MM-DD from being inverted as YYYY-DD-MM)
        val_str = str(date_val).strip()
        is_iso_year_first = bool(re.match(r'^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}', val_str))
        dt_cand = pd.to_datetime(date_val, dayfirst=not is_iso_year_first, errors='coerce')
        if pd.isna(dt_cand):
            return None
        
        dt = dt_cand.to_pydatetime() if hasattr(dt_cand, 'to_pydatetime') else dt_cand

        # Signal 3: Statement period header cross-check (detect and correct inverted MM/DD vs DD/MM)
        if metadata:
            meta_start = metadata.get("start_date")
            if meta_start and isinstance(meta_start, (datetime, pd.Timestamp)):
                target_month = meta_start.month
                target_year = meta_start.year
                if dt.month != target_month and dt.day == target_month and dt.month <= 31:
                    try:
                        return datetime(target_year, target_month, dt.month)
                    except Exception:
                        pass

        return dt

    def _effective_date_distance_days(
        self,
        dt1_booking: Optional[datetime],
        dt1_value: Optional[datetime],
        dt2_booking: Optional[datetime],
        dt2_value: Optional[datetime] = None,
    ) -> int:
        """
        Posting Date (`booking_date`) & Value Date (`value_date`) Clearing Lag Resolver:
        Computes the minimum calendar-day distance across both Posting Date and Value Date.
        Example: A transfer leaves Bank A on Posting Date Feb 5 with Value Date Feb 7,
        and arrives in Bank B (or ERP ledger) on Feb 7 -> effective distance is 0 days.
        """
        d1_candidates = [d.replace(tzinfo=None) for d in (dt1_booking, dt1_value) if d is not None]
        d2_candidates = [d.replace(tzinfo=None) for d in (dt2_booking, dt2_value) if d is not None]
        if not d1_candidates or not d2_candidates:
            return 0
        return min(abs((a.date() - b.date()).days) for a in d1_candidates for b in d2_candidates)

    # ═══════════════════════════════════════════════════════════════════
    #  SCHEMA-AGNOSTIC INPUT ENGINE (35% Header Similarity + 65% Value Distribution)
    # ═══════════════════════════════════════════════════════════════════

    def _compute_header_signature_hash(self, columns: List[Any]) -> str:
        norm = "|".join(sorted(str(c).strip().lower() for c in columns if not str(c).startswith("Unnamed")))
        return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:32]

    def _detect_column_mapping(self, df: Any, db: Optional[Session] = None, company_id: Optional[int] = None, bank_id: Optional[int] = None) -> Dict[str, str]:
        """
        Schema-Agnostic Column Discovery Engine:
        Combines learned bank/customer input profiles, header semantics (35% weight),
        and actual cell value distribution profiling (65% weight) so any column order
        or naming convention is accurately mapped.
        """
        mapping: Dict[str, str] = {}

        # 0. Check if we have a previously learned mapping profile for this exact header signature
        header_hash = self._compute_header_signature_hash(list(df.columns))
        if db and header_hash:
            try:
                learned_profile = db.query(StatementInputMappingProfile).filter(
                    StatementInputMappingProfile.header_signature_hash == header_hash,
                    StatementInputMappingProfile.source_type == "BANK_STATEMENT",
                    or_(
                        StatementInputMappingProfile.company_id == company_id,
                        StatementInputMappingProfile.company_id == None
                    ),
                    or_(
                        StatementInputMappingProfile.bank_id == bank_id,
                        StatementInputMappingProfile.bank_id == None
                    )
                ).order_by(StatementInputMappingProfile.usage_count.desc()).first()
                if learned_profile and isinstance(learned_profile.column_mapping_json, dict):
                    valid_learned = {
                        k: v for k, v in learned_profile.column_mapping_json.items()
                        if v in df.columns
                    }
                    if "date" in valid_learned and ("amount" in valid_learned or "debitamount" in valid_learned or "creditamount" in valid_learned):
                        learned_profile.usage_count = (learned_profile.usage_count or 1) + 1
                        learned_profile.last_used_at = datetime.utcnow()
                        return valid_learned
            except Exception as e:
                logger.debug(f"Input mapping profile lookup skipped: {e}")

        # Keywords for matching (value_date placed before date so 'Value Date' is never swallowed by generic 'date')
        keywords = {
            "value_date": [
                'value date', 'valuedate', 'val date', 'effective date', 'settlement date', 'maturity date',
                'تاريخ الاستحقاق', 'تاريخ القيمة', 'تاريخ الحق', 'تاريخ الفعالية'
            ],
            "date": [
                'posting date', 'post date', 'postdate', 'booking date', 'booking', 'transaction date',
                'trx date', 'entry date', 'date', 'تاريخ القيد', 'تاريخ الحركة', 'تاريخ العملية', 'تاريخ', 'يوم'
            ],
            "amount": ['amount', 'value', 'txn amount', 'transaction amount', 'مبلغ', 'القيمة', 'المبلغ'],
            "debitamount": ['debit', 'withdraw', 'withdrawal', 'dr amount', 'paid out', 'مدين', 'سحب', 'مسحوبات', 'منه'],
            "creditamount": ['credit', 'deposit', 'cr amount', 'paid in', 'دائن', 'إيداع', 'ايداع', 'له'],
            "dc_flag": ['d/c', 'dr/cr', 'debit/credit', 'type d/c', 'indicator', 'نوع الحركة مدين/دائن'],
            "desc": ['desc', 'narrative', 'details', 'particulars', 'remarks', 'explanation', 'البيان', 'وصف', 'التفاصيل', 'desiption', 'transaction details', 'بيان الحركة'],
            "desc2": ['desiption2', 'description2', 'additional details', 'narrative 2', 'البيان 2', 'تفاصيل اضافية'],
            "balance": ['balance', 'running balance', 'closing balance', 'ledger balance', 'رصيد الحساب', 'الرصيد', 'رصيد'],
            "company": ['company', 'entity', 'الشركة', 'الجهة'],
            "account_no": ['accountno', 'account number', 'account #', 'acct no', 'iban', 'رقم الحساب'],
            "back_office_ref": ['backofficereference', 'bo ref', 'reference', 'ref no', 'trn ref', 'trx id', 'doc no', 'المرجع', 'رقم المرجع'],
            "cheque_no": ['cheque', 'check no', 'chq no', 'instrument no', 'رقم الشيك'],
            "currency": ['currency', 'ccy', 'curr', 'العملة', 'عملة'],
            "category": ['category', 'class', 'فئة', 'تصنيف'],
            "sub_category": ['subcategory', 'subclass', 'فئة فرعية'],
            "net": ['net', 'صافي'],
            "positive": ['positive', 'موجب'],
            "source": ['source', 'مصدر', 'نظام'],
            "rate_egp": ['egp rate', 'سعر الجنيه'],
            "rate_eur_usd": ['eur/usd rate', 'سعر اليورو/دولار'],
            "beneficiary": ['beneficiary', 'counterparty', 'payee', 'payer', 'المستفيد', 'الطرف الثاني'],
            "purpose": ['purpose', 'الغرض'],
            "amount_currency": ['amount in currency', 'القيمة بالعملة'],
            "amount_egp": ['amount on egp', 'amount in egp', 'القيمة بالجنيه'],
            "transfer_type": ['transfer type', 'نوع التحويل']
        }

        # 1. Try Keyword matching on column names (exact / substring priority)
        for c in df.columns:
            c_str = str(c).strip()
            if "Unnamed" in c_str or isinstance(c, (int, float)):
                continue
            
            c_low = c_str.lower().replace(".", "").replace("_", " ").strip()
            c_compact = c_low.replace(" ", "")
            # Priority check so "running balance" maps to "balance" and not "amount"
            if any(b_kw.replace(" ", "") in c_compact for b_kw in keywords["balance"]) and "balance" not in mapping:
                mapping["balance"] = c
                continue

            for key, kw_list in keywords.items():
                if key in mapping:
                    continue
                if any(kw.replace(" ", "") in c_compact for kw in kw_list):
                    mapping[key] = c
                    break

        # 2. Value-Distribution Profiling (65% weight) when critical columns are missing or ambiguous
        if 'date' not in mapping or ('amount' not in mapping and 'creditamount' not in mapping and 'debitamount' not in mapping):
            data_start_idx = 0
            for i in range(min(50, len(df))):
                row_str = " ".join(str(v) for v in df.iloc[i].values).lower()
                if re.search(r'(\d{1,2}[A-Z]{3}\d{2,4})|(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4})', row_str, re.IGNORECASE):
                    data_start_idx = i
                    break
            
            sample_data = df.iloc[data_start_idx:data_start_idx + 80]
            candidate_date_cols = []
            candidate_amt_cols = []
            candidate_desc_cols = []

            for col in df.columns:
                values = sample_data[col].dropna().astype(str)
                values = values[values.str.strip() != ""]
                if values.empty:
                    continue
                
                # Date distribution check
                date_hits = 0
                for v in values:
                    if re.search(r'(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4})|(\d{1,2}[A-Z]{3}\d{2,4})', v, re.IGNORECASE):
                        try:
                            pd.to_datetime(v, errors='raise')
                            date_hits += 1
                        except Exception:
                            pass
                if date_hits >= 1:
                    candidate_date_cols.append((col, date_hits))
                
                # Numeric / Money distribution check
                money_vals = []
                zero_count = 0
                neg_count = 0
                for v in values:
                    clean_v = re.sub(r'[^\d.-]', '', v)
                    if clean_v and clean_v not in ("-", ".", "-."):
                        try:
                            f_val = float(clean_v.replace(',', ''))
                            if f_val == 0:
                                zero_count += 1
                            elif 0.01 <= abs(f_val) <= 10000000000:
                                money_vals.append(f_val)
                                if f_val < 0:
                                    neg_count += 1
                        except Exception:
                            pass
                
                if len(money_vals) >= 1:
                    has_decimals = any(abs(v % 1) > 0.0001 for v in money_vals)
                    variance = pd.Series(money_vals).std() if len(money_vals) > 1 else 0
                    score = len(money_vals) + (10 if has_decimals else 0) + (5 if variance > 10 else 0)
                    candidate_amt_cols.append((col, score, neg_count, zero_count, money_vals))
                
                # Description text entropy check
                text_values = values[~values.str.contains(r'^[ \d,.$€£()-]*$')]
                if not text_values.empty:
                    co_occur_texts = []
                    co_occur = 0
                    for _, row in sample_data.iterrows():
                        if not pd.isna(row[col]) and str(row[col]).strip() != "":
                            row_vals = " ".join(str(v) for v in row.values).lower()
                            if re.search(r'(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4})|(\d{1,2}[A-Z]{3}\d{2,4})', row_vals, re.IGNORECASE):
                                co_occur += 1
                                co_occur_texts.append(str(row[col]))
                    
                    if co_occur >= 1 and co_occur_texts:
                        avg_len = sum(len(t) for t in co_occur_texts) / len(co_occur_texts)
                        space_count = sum(1 for t in co_occur_texts if " " in t.strip())
                        space_ratio = space_count / len(co_occur_texts)
                        unique_ratio = len(set(co_occur_texts)) / len(co_occur_texts)
                        score = (co_occur * 20) + (space_ratio * 50) + avg_len + (unique_ratio * 10)
                        candidate_desc_cols.append((col, score))

            if candidate_date_cols:
                # Sort date columns by left-to-right appearance when hit counts are comparable
                max_hits = max(x[1] for x in candidate_date_cols)
                strong_date_cols = [
                    x[0] for x in candidate_date_cols
                    if x[1] >= max(1, int(max_hits * 0.6))
                ]
                strong_date_cols.sort(key=lambda col_name: list(df.columns).index(col_name))
                if 'date' not in mapping and strong_date_cols:
                    mapping['date'] = strong_date_cols[0]
                if 'value_date' not in mapping and len(strong_date_cols) >= 2:
                    second_date_col = next((c for c in strong_date_cols if c != mapping.get('date')), None)
                    if second_date_col is not None:
                        mapping['value_date'] = second_date_col
            
            if 'amount' not in mapping and 'creditamount' not in mapping and candidate_amt_cols:
                filtered_amt = [c for c in candidate_amt_cols if c[0] != mapping.get('date')]
                filtered_amt.sort(key=lambda x: x[1], reverse=True)
                best_amt_candidates = [x[0] for x in filtered_amt if x[1] >= 1]
                best_amt_candidates.sort(key=lambda col_name: list(df.columns).index(col_name))
                
                if len(best_amt_candidates) >= 3:
                    mapping['debitamount'] = best_amt_candidates[0]
                    mapping['creditamount'] = best_amt_candidates[1]
                    mapping['balance'] = best_amt_candidates[2]
                elif len(best_amt_candidates) == 2:
                    mapping['amount'] = best_amt_candidates[0]
                    mapping['balance'] = best_amt_candidates[1]
                elif best_amt_candidates:
                    mapping['amount'] = best_amt_candidates[0]

            mapped_cols = [mapping.get(k) for k in mapping if mapping.get(k) is not None]
            if 'desc' not in mapping:
                filtered_desc = [c for c in candidate_desc_cols if c[0] not in mapped_cols]
                if filtered_desc:
                    filtered_desc.sort(key=lambda x: x[1], reverse=True)
                    mapping['desc'] = filtered_desc[0][0]
                elif candidate_desc_cols:
                    mapping['desc'] = max(candidate_desc_cols, key=lambda x: x[1])[0]
                else:
                    text_cols = []
                    for col in sample_data.columns:
                        if col not in mapped_cols:
                            texts = sample_data[col].astype(str)
                            texts = texts[~texts.str.contains(r'^[ \d,.-]*$|^nan$', na=False, case=False)]
                            if not texts.empty:
                                text_cols.append((col, len(texts)))
                    if text_cols:
                        text_cols.sort(key=lambda x: x[1], reverse=True)
                        mapping['desc'] = text_cols[0][0]

        # 3. Persist learned input mapping profile online if DB is available
        if db and header_hash and mapping.get("date"):
            try:
                existing_prof = db.query(StatementInputMappingProfile).filter(
                    StatementInputMappingProfile.header_signature_hash == header_hash,
                    StatementInputMappingProfile.company_id == company_id,
                    StatementInputMappingProfile.bank_id == bank_id
                ).first()
                serializable_map = {k: str(v) for k, v in mapping.items() if v is not None}
                if not existing_prof:
                    new_prof = StatementInputMappingProfile(
                        company_id=company_id,
                        bank_id=bank_id,
                        source_type="BANK_STATEMENT",
                        header_signature_hash=header_hash,
                        column_mapping_json=serializable_map,
                        usage_count=1
                    )
                    db.add(new_prof)
                    db.flush()
            except Exception as e:
                logger.debug(f"Could not save learned input mapping profile: {e}")

        return mapping

    def _detect_metadata(self, df: Any) -> Dict[str, Any]:
        """
        Scans top and bottom rows for balance keywords (English & Arabic).
        """
        metadata = {
            "opening_balance": None, 
            "closing_balance": None, 
            "start_date": None, 
            "end_date": None, 
            "account_number": None,
            "company_name": None
        }
        
        sample_head = df.head(50).astype(str)
        sample_tail = df.tail(50).astype(str)
        
        def _looks_like_date_or_period_cell(raw_val: Any) -> bool:
            s = str(raw_val or "").strip()
            if not s or s.lower() in ("nan", "none", "nat"):
                return True
            low = s.lower()
            # Reject date ranges & calendar dates (e.g. "01/10/2026 - 31/10/2026" -> would otherwise concatenate digits!)
            if re.search(r"\b\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}\b", s):
                return True
            if any(m in low for m in ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "يناير", "فبراير", "مارس", "أبريل", "ابريل", "مايو", "يونيو", "يوليو", "أغسطس", "اغسطس", "سبتمبر", "أكتوبر", "اكتوبر", "نوفمبر", "ديسمبر")):
                return True
            # Reject long unformatted account/IBAN/reference digit strings (>12 digits with no decimal separator)
            digits_only = re.sub(r"\D", "", s)
            if len(digits_only) > 12 and "." not in s and "," not in s:
                return True
            return False

        for _, sample in [("head", sample_head), ("tail", sample_tail)]:
            for _, row in sample.iterrows():
                row_str = " ".join(row.values).lower()
                
                if any(kw in row_str for kw in ["opening", "balance b/f", "رصيد سابق", "رصيد اول", "carried forward"]):
                    for val in row.values:
                        if _looks_like_date_or_period_cell(val):
                            continue
                        dec = self._clean_decimal(val)
                        if dec != 0 and metadata["opening_balance"] is None: 
                            metadata["opening_balance"] = dec
                
                if any(kw in row_str for kw in ["closing", "balance c/f", "رصيد حالي", "رصيد اخر", "current balance", "ending balance"]):
                    for val in row.values:
                        if _looks_like_date_or_period_cell(val):
                            continue
                        dec = self._clean_decimal(val)
                        if dec != 0 and metadata["closing_balance"] is None: 
                            metadata["closing_balance"] = dec

                if any(kw in row_str for kw in ["date", "تاريخ", "period", "فترة"]):
                    for val in row.values:
                        try:
                            dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
                            if not pd.isna(dt):
                                if metadata["start_date"] is None:
                                    metadata["start_date"] = dt
                                else:
                                    metadata["end_date"] = dt
                        except Exception:
                            pass

                if any(kw in row_str for kw in ["account no", "account #", "رقم الحساب", "account number", "iban"]):
                    for val in row.values:
                        val_str = str(val).strip()
                        if val_str.isdigit() and len(val_str) > 5:
                            metadata["account_number"] = val_str
                        elif "EG" in val_str.upper() and len(val_str) > 15:
                            metadata["account_number"] = val_str.upper()

                if any(kw in row_str for kw in ["company", "customer", "الشركة", "العميل", "account name"]):
                    for val in row.values:
                        val_str = str(val).strip()
                        if len(val_str) > 5 and not any(kw in val_str.lower() for kw in ["balance", "date", "opening", "closing", "رصيد", "account", "currency"]):
                            metadata["company_name"] = val_str

        return metadata

    def _link_companion_fees_in_batch(self, transactions: List[Dict[str, Any]]) -> None:
        """
        Relational Context Analyzer:
        Scans parsed statement lines for small fee debits (`MICRO`/`SMALL`) that occur
        on the same booking date within +/- 3 rows of a larger parent transfer (or share a reference).
        Marks `_has_companion_parent = True` and `_companion_parent_idx` so the classifier
        and rule engine know this line is an attached wire/SWIFT fee.
        """
        n = len(transactions)
        for i, txn in enumerate(transactions):
            debit = txn.get("debit_amount") or Decimal("0.00")
            if debit <= 0 or debit > Decimal("2500.00"):
                continue
            b_date = txn.get("booking_date")
            ref_i = txn.get("back_office_ref")
            # Search neighbors within +/- 3 rows on the same date
            for j in range(max(0, i - 3), min(n, i + 4)):
                if i == j:
                    continue
                other = transactions[j]
                other_debit = other.get("debit_amount") or Decimal("0.00")
                other_credit = other.get("credit_amount") or Decimal("0.00")
                other_amt = max(other_debit, other_credit)
                if other_amt >= debit * Decimal("10") and other_amt >= Decimal("5000.00"):
                    same_date = (b_date and other.get("booking_date") and b_date.date() == other["booking_date"].date())
                    same_ref = (ref_i and other.get("back_office_ref") and ref_i == other.get("back_office_ref"))
                    if same_date or same_ref:
                        txn["_has_companion_parent"] = True
                        txn["_companion_parent_idx"] = j
                        break

    def parse_statement_content(
        self,
        content: bytes,
        file_type: str,
        db: Optional[Session] = None,
        company_id: Optional[int] = None,
        bank_id: Optional[int] = None,
        pdf_password: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Universal Schema-Agnostic Statement Parser:
        - Reads any column order, split Debit/Credit or single Amount + D/C flag
        - Preserves all unmapped customer/bank columns in `custom_fields_json`
        - Audits running balance continuity without crashing on bank statement quirks
        - Links companion wire fees to adjacent parent transfers
        """
        ft = file_type.lower().strip().lstrip(".")
        pdf_extra_meta: Dict[str, Any] = {}
        # 0. Check for Native SWIFT MT940 (.mt940, .sta, .940) or ISO 20022 CAMT.053 (.xml)
        if ft in ("mt940", "sta", "940") or (ft in ("txt", "dat") and banking_standards_ontology.is_mt940_content(content)):
            txns, meta = banking_standards_ontology.parse_mt940_statement(content)
            for t in txns:
                iban, b_code, b_name = reconciliation_confidentiality.extract_iban_and_bank_code(t.get("raw_description") or "")
                t["counterparty_iban"] = iban
                t["iban_bank_code"] = b_code
                t["counterparty_bank"] = b_name
                t["amount_tier"] = classify_amount_tier(max(t.get("debit_amount") or 0, t.get("credit_amount") or 0))
            self._link_companion_fees_in_batch(txns)
            return txns, meta
        elif ft == "xml" or banking_standards_ontology.is_camt053_content(content):
            try:
                txns, meta = banking_standards_ontology.parse_camt053_statement(content)
            except ValueError as xml_err:
                raise HTTPException(status_code=400, detail=str(xml_err)) from xml_err
            for t in txns:
                iban, b_code, b_name = reconciliation_confidentiality.extract_iban_and_bank_code(t.get("raw_description") or "")
                t["counterparty_iban"] = iban
                t["iban_bank_code"] = b_code
                t["counterparty_bank"] = b_name
                t["amount_tier"] = classify_amount_tier(max(t.get("debit_amount") or 0, t.get("credit_amount") or 0))
            self._link_companion_fees_in_batch(txns)
            return txns, meta
        elif ft in ('csv', 'txt'):
            def _load_csv_bytes(raw_bytes: bytes, enc: str = "utf-8"):
                text_probe = raw_bytes[:4096].decode(enc, errors="ignore")
                first_line = next((ln for ln in text_probe.splitlines() if ln.strip()), "")
                delim = ";" if (first_line.count(";") > first_line.count(",")) else ("\t" if (first_line.count("\t") > first_line.count(",")) else ",")
                try:
                    return pd.read_csv(io.BytesIO(raw_bytes), encoding=enc, sep=delim)
                except Exception:
                    text_data = raw_bytes.decode(enc, errors="replace")
                    rows_list = list(csv.reader(io.StringIO(text_data), delimiter=delim))
                    if not rows_list:
                        return pd.DataFrame()
                    max_cols = max(len(r) for r in rows_list)
                    padded = [r + [None] * (max_cols - len(r)) for r in rows_list]
                    return pd.DataFrame(padded)

            try:
                df = _load_csv_bytes(content, "utf-8")
            except UnicodeDecodeError:
                df = _load_csv_bytes(content, "cp1256")
        elif ft in ('xls', 'xlsx', 'xlsb'):
            try:
                xl = pd.ExcelFile(io.BytesIO(content))
            except Exception as xl_err:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid or corrupted Excel file. Please upload a valid .xlsx, .xls, or .csv bank statement.",
                ) from xl_err

            # Score worksheets by banking header cues rather than raw row count so lookup/instruction tabs never hijack ingestion
            best_sheet_df = None
            best_sheet_score = (-1, -1)
            stmt_date_kws = ('date', 'booking', 'posting', 'trx', 'تاريخ', 'يوم')
            stmt_money_kws = ('debit', 'credit', 'withdraw', 'deposit', 'balance', 'running', 'amount', 'مدين', 'دائن', 'رصيد', 'مبلغ')
            for s_name in xl.sheet_names:
                cand_df = xl.parse(s_name, header=None).dropna(how='all')
                if cand_df.empty:
                    continue
                s_score = 0
                s_low = str(s_name).lower()
                if any(w in s_low for w in ('statement', 'stmt', 'account', 'transactions', 'كشف', 'حركات')):
                    s_score += 4
                if any(w in s_low for w in ('instruction', 'lookup', 'readme', 'codes', 'glossary', 'تعليمات', 'دليل')):
                    s_score -= 5
                for r_i in range(min(25, len(cand_df))):
                    r_txt = " ".join(str(v).lower() for v in cand_df.iloc[r_i].values if pd.notna(v))
                    has_d = any(k in r_txt for k in stmt_date_kws)
                    m_hits = sum(1 for k in stmt_money_kws if k in r_txt)
                    if has_d and m_hits >= 1:
                        s_score = max(s_score, 10 + m_hits * 3)
                cand_rank = (s_score, len(cand_df))
                if best_sheet_df is None or cand_rank > best_sheet_score:
                    best_sheet_df = cand_df
                    best_sheet_score = cand_rank
            df = best_sheet_df if best_sheet_df is not None else pd.DataFrame()
        elif ft == 'pdf':
            from app.services.pdf_statement_parser import (
                get_candidate_pdf_passwords,
                unlock_pdf_bytes,
                extract_statement_from_pdf,
            )
            passwords = get_candidate_pdf_passwords(
                db=db,
                customer_id=company_id,
                bank_id=bank_id,
                explicit_password=pdf_password,
            )
            unlocked_bytes, was_encrypted, is_unlocked = unlock_pdf_bytes(content, passwords)
            if was_encrypted and not is_unlocked:
                raise HTTPException(
                    status_code=400,
                    detail="PDF_PASSWORD_REQUIRED: This PDF statement is password-protected. Provide the PDF password or save it on the Bank Account.",
                )
            if not is_unlocked:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid or unreadable PDF statement file.",
                )
            try:
                df, pdf_extra_meta, _ = extract_statement_from_pdf(unlocked_bytes)
            except ValueError as pdf_err:
                raise HTTPException(status_code=400, detail=str(pdf_err)) from pdf_err
            if df is None or df.empty:
                raise HTTPException(
                    status_code=400,
                    detail="No transaction table found in PDF statement.",
                )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_type}")

        logger.info(f"Ingesting file: {ft.upper()}, Shape: {df.shape}")

        df = df.dropna(how='all').reset_index(drop=True)
        metadata_pre = self._detect_metadata(df)

        balance_keywords = ["opening balance", "closing balance", "balance b/f", "balance c/f", "رصيد سابق", "رصيد حالي", "رصيد اول", "رصيد اخر"]
        header_keywords = {
            "date": ['date', 'booking', 'trx', 'post', 'تاريخ', 'يوم'],
            "amt": ['amount', 'debit', 'credit', 'withdraw', 'deposit', 'value', 'balance', 'مبلغ', 'القيمة', 'مدين', 'دائن', 'رصيد']
        }

        # Check for wrong-document-type / wrong-upload-box spreadsheets before header promotion
        probe_header_cells: List[str] = [
            str(c).lower().strip() for c in df.columns if pd.notna(c) and not str(c).startswith("Unnamed")
        ]
        for p_idx in range(min(10, len(df))):
            probe_header_cells.extend(
                str(v).lower().strip() for v in df.iloc[p_idx].values if pd.notna(v) and str(v).strip()
            )
        has_cash_ledger_cols = (
            any(any(k in h for k in ('running balance', 'closing balance', 'opening balance', 'balance', 'رصيد')) for h in probe_header_cells)
            or (
                any(any(k in h for k in ('debit', 'withdrawal', 'مدين')) for h in probe_header_cells)
                and any(any(k in h for k in ('credit', 'deposit', 'دائن')) for h in probe_header_cells)
            )
        )
        hr_kw_hits = sum(
            1
            for kw in ('employee id', 'emp id', 'hire date', 'monthly salary', 'basic salary', 'net salary', 'job title', 'payroll', 'رقم الموظف', 'تاريخ التعيين', 'الراتب')
            if any(kw in h for h in probe_header_cells)
        )
        if hr_kw_hits >= 2 and not has_cash_ledger_cols:
            raise HTTPException(
                status_code=400,
                detail="Wrong document type: This file appears to be an HR / Payroll spreadsheet rather than a Bank Statement. Please upload a valid Bank Cash Statement.",
            )
        if (
            any(any(k in h for k in ('lg number', 'lg_number', 'lg no', 'guarantee no', 'guarantee number', 'رقم الخطاب', 'خطاب ضمان')) for h in probe_header_cells)
            and any(any(k in h for k in ('expiry', 'beneficiary', 'تاريخ الانتهاء', 'المستفيد')) for h in probe_header_cells)
            and not has_cash_ledger_cols
        ):
            raise HTTPException(
                status_code=400,
                detail="Wrong upload box: This file appears to be a Bank LG Position sheet rather than a Cash Bank Statement. Please upload it in LG Position Reconciliation instead.",
            )
        top_col_low = [str(c).lower().strip() for c in df.columns if pd.notna(c) and not str(c).startswith("Unnamed")]
        if not top_col_low and len(df) > 0:
            top_col_low = [str(v).lower().strip() for v in df.iloc[0].values if pd.notna(v)]
        has_erp_col_or_marker = (
            any(any(k in h for k in ('invoice date', 'bill date', 'record type', 'invoice no', 'invoice #', 'bill #', 'voucher no', 'gl account', 'رقم الفاتورة')) for h in top_col_low)
            or any(any(k in cell for k in ('ar_invoice', 'ap_bill', 'sales invoice', 'purchase invoice', 'vendor bill', 'open invoice')) for cell in probe_header_cells)
        )
        if has_erp_col_or_marker and not has_cash_ledger_cols:
            raise HTTPException(
                status_code=400,
                detail="Wrong upload box: This file looks like an ERP Open Invoices / Payables ledger rather than a Bank Statement. Please upload it under 'ERP Open Invoices / Payables' instead.",
            )

        # Check if df.columns already represents the real table header
        existing_col_strs = [str(c).lower().strip() for c in df.columns if pd.notna(c) and not str(c).startswith("Unnamed")]
        cols_have_date = any(any(kw in c for kw in header_keywords["date"]) for c in existing_col_strs)
        cols_have_amt = any(any(kw in c for kw in header_keywords["amt"]) for c in existing_col_strs)

        if not (cols_have_date and cols_have_amt):
            for i in range(min(50, len(df))):
                row_vals = [str(v).lower().strip() for v in df.iloc[i].values if pd.notna(v)]
                has_date_kw = any(any(kw in v for kw in header_keywords["date"]) for v in row_vals)
                has_amt_kw = any(any(kw in v for kw in header_keywords["amt"]) for v in row_vals)
                if has_date_kw and has_amt_kw:
                    df.columns = [self._sanitize_text(c) or f"Col_{idx}" for idx, c in enumerate(df.iloc[i])]
                    df = df.iloc[i + 1:].reset_index(drop=True)
                    break
        
        metadata = self._detect_metadata(df)
        for k, v in metadata_pre.items():
            if metadata.get(k) is None and v is not None:
                metadata[k] = v
        for k in ("opening_balance", "closing_balance", "account_number", "currency"):
            if metadata.get(k) is None and pdf_extra_meta.get(k) is not None:
                metadata[k] = pdf_extra_meta[k]

        mapping = self._detect_column_mapping(df, db=db, company_id=company_id, bank_id=bank_id) or {}
        metadata["detected_mapping"] = {k: str(v) for k, v in mapping.items() if v is not None}
        metadata["continuity_warnings"] = []
        metadata["balance_variance_amount"] = Decimal("0.00")
        metadata["balance_continuity_status"] = "VALIDATED"

        if not mapping.get('date') or (not mapping.get('amount') and not mapping.get('creditamount') and not mapping.get('debitamount')):
            is_unnamed_table = all(
                isinstance(c, (int, float))
                or str(c).startswith(("Unnamed", "Col_"))
                or bool(re.match(r'^[\d\s,.-]+$', str(c)))
                for c in df.columns
                if pd.notna(c) and str(c).strip() != ""
            )
            if not is_unnamed_table:
                raise HTTPException(
                    status_code=400,
                    detail="Unrecognized Bank Statement format: Could not detect required Date and Amount (or Debit/Credit) columns.",
                )
            logger.warning("Headerless table detected. Using value-based smart fallback...")
            non_empty_cols = [c for c in df.columns if df[c].dropna().count() > 0]
            if len(non_empty_cols) >= 2:
                detected_date = False
                for c in non_empty_cols:
                    sample = df[c].dropna().astype(str).head(20)
                    if any(re.search(r'(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4})|(\d{1,2}[A-Z]{3}\d{2,4})', v, re.IGNORECASE) for v in sample):
                        mapping['date'] = c
                        detected_date = True
                        break
                if not detected_date:
                    raise HTTPException(
                        status_code=400,
                        detail="Unrecognized file format: Could not detect transaction dates in the uploaded file.",
                    )
                remaining = [c for c in non_empty_cols if c != mapping.get('date')]
                if remaining:
                    mapping['amount'] = remaining[0]
                    if len(remaining) > 1:
                        mapping['desc'] = remaining[1]

        # Identify extra unmapped columns to preserve in `custom_fields_json`
        mapped_col_names = set(mapping.values())
        unmapped_cols = [
            c for c in df.columns
            if c not in mapped_col_names and not str(c).startswith("Unnamed") and not isinstance(c, (int, float))
        ]

        summary_exact_labels = {
            "total", "totals", "grand total", "sub total", "subtotal", "summary",
            "page total", "statement total", "إجمالي", "اجمالي", "الإجمالي",
            "الاجمالي", "مجموع", "المجموع", "ملخص",
        }
        summary_phrases = (
            "total movements", "total transactions", "total debit", "total credit",
            "إجمالي الحركات", "اجمالي الحركات", "مجموع الحركات",
            "إجمالي المدين", "اجمالي المدين", "إجمالي الدائن", "اجمالي الدائن",
        )

        transactions = []
        
        for idx, row in df.iterrows():
            try:
                date_val = row.get(mapping.get("date"))
                amount_val = row.get(mapping.get("amount"))
                debit_val = row.get(mapping.get("debitamount"))
                credit_val = row.get(mapping.get("creditamount"))
                dc_flag_val = row.get(mapping.get("dc_flag")) if mapping.get("dc_flag") else None
                
                desc_val = row.get(mapping.get("desc"))
                desc = self._sanitize_text(desc_val)

                def _is_blank(v: Any) -> bool:
                    return v is None or pd.isna(v) or str(v).strip() in ("", "nan", "None")

                is_continuation = (
                    _is_blank(date_val)
                    and (_is_blank(amount_val) or self._clean_decimal(amount_val) == Decimal("0.00"))
                    and (_is_blank(debit_val) or self._clean_decimal(debit_val) == Decimal("0.00"))
                    and (_is_blank(credit_val) or self._clean_decimal(credit_val) == Decimal("0.00"))
                )
                if is_continuation:
                    if transactions and desc and not any(kw in desc.lower() for kw in balance_keywords):
                        desc_cont_norm = re.sub(r"[\s:._\-]+", " ", desc.lower()).strip()
                        if desc_cont_norm not in summary_exact_labels and not any(p in desc_cont_norm for p in summary_phrases):
                            transactions[-1]["raw_description"] = f"{transactions[-1]['raw_description']} {desc}".strip()
                    continue

                if _is_blank(date_val):
                    continue

                date_str_low = str(date_val).strip().lower()
                if any(tok in date_str_low for tok in ("total", "subtotal", "إجمالي", "اجمالي", "مجموع", "ملخص")):
                    continue
                
                desc_low = desc.lower()
                if any(kw in desc_low for kw in balance_keywords):
                    continue
                desc_norm = re.sub(r"[\s:._\-]+", " ", desc_low).strip()
                if desc_norm in summary_exact_labels or any(p in desc_norm for p in summary_phrases):
                    continue
                    
                booking_date = self._resolve_transaction_date(date_val, narrative=desc, metadata=metadata)
                if not booking_date or pd.isna(booking_date):
                    continue
                
                # Amounts & D/C indicator resolution
                amount = self._clean_decimal(amount_val)
                if dc_flag_val is not None and not pd.isna(dc_flag_val):
                    dc_str = str(dc_flag_val).strip().upper()
                    if dc_str in ("D", "DR", "DEBIT", "مدين", "سحب") and amount > 0:
                        amount = -amount
                    elif dc_str in ("C", "CR", "CREDIT", "دائن", "إيداع", "ايداع") and amount < 0:
                        amount = abs(amount)

                debit = abs(self._clean_decimal(debit_val)) if mapping.get("debitamount") else (abs(amount) if amount < 0 else Decimal("0.00"))
                credit = abs(self._clean_decimal(credit_val)) if mapping.get("creditamount") else (amount if amount > 0 else Decimal("0.00"))

                # Skip summary footer rows that aggregate both non-zero Debit and Credit totals on one line
                if debit > 0 and credit > 0 and (
                    not desc_norm or any(w in desc_norm for w in ("total", "sum", "إجمالي", "اجمالي", "مجموع", "ملخص"))
                ):
                    continue
                
                if mapping.get("debitamount") or mapping.get("creditamount"):
                    amount = credit - debit

                if amount == 0 and debit == 0 and credit == 0 and not desc:
                    continue

                excel_balance_val = row.get(mapping.get("balance")) if mapping.get("balance") else None
                reported_balance = None
                if excel_balance_val is not None and not pd.isna(excel_balance_val) and str(excel_balance_val).strip() != "":
                    reported_balance = self._clean_decimal(excel_balance_val)

                # Preserve any extra customer/bank columns in custom_fields_json
                custom_fields: Dict[str, Any] = {}
                for u_col in unmapped_cols:
                    u_val = row.get(u_col)
                    if pd.notna(u_val) and str(u_val).strip() not in ("", "nan", "None"):
                        custom_fields[str(u_col).strip()] = self._sanitize_text(u_val)

                rate_egp = self._clean_decimal(row.get(mapping.get("rate_egp")))
                rate_eur_usd = self._clean_decimal(row.get(mapping.get("rate_eur_usd")))
                
                txn_amount_currency = self._clean_decimal(row.get(mapping.get("amount_currency")))
                if txn_amount_currency == 0:
                    txn_amount_currency = amount
                
                default_currency = (metadata.get("currency") or "EGP").upper()
                raw_ccy_val = row.get(mapping.get("currency")) if mapping.get("currency") else None
                row_currency = (
                    self._sanitize_text(raw_ccy_val).upper()[:3]
                    if raw_ccy_val is not None and not pd.isna(raw_ccy_val) and str(raw_ccy_val).strip()
                    else default_currency
                ) or default_currency

                txn_amount_egp = self._clean_decimal(row.get(mapping.get("amount_egp")))
                if txn_amount_egp == 0 and rate_egp > 0:
                    txn_amount_egp = amount * rate_egp
                elif txn_amount_egp == 0:
                    txn_amount_egp = amount if row_currency == "EGP" else Decimal("0.00")

                txn_net = self._clean_decimal(row.get(mapping.get("net")))
                if txn_net == 0:
                    txn_net = amount

                txn_positive = row.get(mapping.get("positive"))
                if pd.isna(txn_positive) or txn_positive == "":
                    is_positive = amount > 0
                else:
                    is_positive = str(txn_positive).lower() in ["yes", "true", "1", "positive"]

                desc2_clean = self._sanitize_text(row.get(mapping.get("desc2"))) if mapping.get("desc2") else None
                full_narrative = f"{desc} {desc2_clean or ''}".strip()
                extracted_iban, iban_bank_code, iban_bank_name = reconciliation_confidentiality.extract_iban_and_bank_code(full_narrative)
                amt_tier = classify_amount_tier(max(debit, credit))

                val_date_raw = row.get(mapping.get("value_date")) if mapping.get("value_date") else None
                resolved_value_date = self._resolve_transaction_date(val_date_raw, narrative=desc, metadata=metadata) if val_date_raw is not None and not pd.isna(val_date_raw) else None
                effective_value_date = resolved_value_date or booking_date
                clearing_float_days = (effective_value_date.date() - booking_date.date()).days
                if clearing_float_days != 0:
                    custom_fields["clearing_float_days"] = str(clearing_float_days)

                # Decode embedded SWIFT MT940 / ISO 20022 / :86: Structured Tags & Payment Rails
                row_direction = "CREDIT" if credit > 0 else "DEBIT"
                std_decoded = banking_standards_ontology.decode_narrative_standards(
                    desc, desc2_clean, direction=row_direction, amount=max(debit, credit)
                )
                if std_decoded.get("payment_rail"):
                    custom_fields["payment_rail"] = std_decoded["payment_rail"]
                if std_decoded.get("mt940_code"):
                    custom_fields["swift_mt940_code"] = std_decoded["mt940_code"]
                if std_decoded.get("iso20022_code"):
                    custom_fields["iso20022_code"] = std_decoded["iso20022_code"]
                if std_decoded.get("remittance_info"):
                    custom_fields["remittance_info"] = std_decoded["remittance_info"]
                if std_decoded.get("embedded_fee_amount") is not None:
                    custom_fields["embedded_fee_amount"] = str(std_decoded["embedded_fee_amount"])

                pre_category = self._sanitize_text(row.get(mapping.get("category"))) if mapping.get("category") else None
                pre_sub_category = self._sanitize_text(row.get(mapping.get("sub_category"))) if mapping.get("sub_category") else None
                mapped_beneficiary = self._sanitize_text(row.get(mapping.get("beneficiary"))) if mapping.get("beneficiary") else None
                effective_counterparty = mapped_beneficiary or std_decoded.get("extracted_counterparty")
                mapped_bo_ref = self._sanitize_text(row.get(mapping.get("back_office_ref"))) if mapping.get("back_office_ref") else None

                txn = {
                    "booking_date": booking_date,
                    "value_date": effective_value_date,
                    "debit_amount": debit,
                    "credit_amount": credit,
                    "raw_description": desc,
                    "description_line2": desc2_clean,
                    "currency": row_currency,
                    "running_balance": reported_balance,
                    "_reported_balance": reported_balance,
                    "_signed_amount": amount,
                    "amount_tier": amt_tier,
                    "counterparty_iban": extracted_iban,
                    "counterparty_bank": iban_bank_name,
                    "iban_bank_code": iban_bank_code,
                    "counterparty_name": effective_counterparty,
                    "e2e_id": std_decoded.get("extracted_e2e_ref"),
                    "custom_fields_json": custom_fields if custom_fields else None,
                    "is_duplicate": False,
                    
                    # Expanded fields with auto-filling defaults
                    "company_name": self._sanitize_text(row.get(mapping.get("company"))) if mapping.get("company") else metadata.get("company_name"),
                    "account_number": self._sanitize_text(row.get(mapping.get("account_no"))) if mapping.get("account_no") else metadata.get("account_number"),
                    "back_office_ref": mapped_bo_ref,
                    "cheque_number": self._sanitize_text(row.get(mapping.get("cheque_no"))) if mapping.get("cheque_no") else None,
                    "category": pre_category,
                    "sub_category": pre_sub_category,
                    "net_amount": txn_net,
                    "is_positive": is_positive,
                    "source_system": self._sanitize_text(row.get(mapping.get("source"))) if mapping.get("source") else "BANK_UPLOAD",
                    "exchange_rate_egp": rate_egp,
                    "exchange_rate_eur_usd": rate_eur_usd,
                    "beneficiary_name": effective_counterparty,
                    "purpose_of_payment": self._sanitize_text(row.get(mapping.get("purpose"))) if mapping.get("purpose") else None,
                    "amount_in_currency": txn_amount_currency,
                    "amount_in_egp": txn_amount_egp,
                    "transfer_type": self._sanitize_text(row.get(mapping.get("transfer_type"))) if mapping.get("transfer_type") else None,
                }
                if pre_category:
                    txn["classification_category"] = pre_category
                    txn["suggested_category"] = pre_category
                    txn["internal_category"] = pre_category
                    txn["classification_source"] = "HISTORICAL_IMPORT"
                    txn["classification_confidence"] = 100
                    txn["auto_confirmed"] = True
                    txn["manual_override"] = True
                    txn["is_classified"] = True
                    txn["classification_reasoning"] = f"Imported with historical classification '{pre_category}'"
                transactions.append(txn)
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Row {idx} mapping error: {e}")
                continue

        # Normalize reverse-chronological exports (newest-first) to chronological order BEFORE running balance audit
        if transactions:
            is_reverse_order = len(transactions) >= 2 and transactions[0]["booking_date"] > transactions[-1]["booking_date"]
            if is_reverse_order:
                transactions = [
                    pair[1]
                    for pair in sorted(enumerate(transactions), key=lambda pair: (pair[1]["booking_date"], -pair[0]))
                ]

            running_balance_acc = (
                self._clean_decimal(metadata.get("opening_balance"))
                if metadata.get("opening_balance") is not None
                else None
            )
            strong_anchor = False
            seen_row_checksums = set()

            for txn in transactions:
                amt = txn.get("_signed_amount") or Decimal("0.00")
                rep_bal = txn.get("_reported_balance")
                b_dt = txn["booking_date"]

                if running_balance_acc is not None:
                    expected_balance = running_balance_acc + amt
                    if rep_bal is not None:
                        variance = abs(expected_balance - rep_bal)
                        if variance > Decimal("0.02") and strong_anchor:
                            warn_msg = (
                                f"Balance continuity gap on {b_dt.strftime('%Y-%m-%d')}: "
                                f"Expected {expected_balance} ({running_balance_acc} + {amt}), "
                                f"Reported {rep_bal} (Delta: {rep_bal - expected_balance})."
                            )
                            metadata["continuity_warnings"].append(warn_msg)
                            metadata["balance_variance_amount"] += (rep_bal - expected_balance)
                            metadata["balance_continuity_status"] = "CONTINUITY_WARNING"
                        running_balance_acc = rep_bal
                        strong_anchor = True
                    else:
                        running_balance_acc = expected_balance
                else:
                    if rep_bal is not None:
                        running_balance_acc = rep_bal
                        strong_anchor = True
                    else:
                        running_balance_acc = amt

                txn["running_balance"] = running_balance_acc
                row_sig = f"{b_dt.strftime('%Y-%m-%d')}|{txn['debit_amount']}|{txn['credit_amount']}|{running_balance_acc}|{(txn.get('raw_description') or '')[:40]}"
                txn["is_duplicate"] = row_sig in seen_row_checksums
                seen_row_checksums.add(row_sig)

        # Relational Context Pass: Link companion wire fees to adjacent parent transfers
        self._link_companion_fees_in_batch(transactions)

        # Initial Built-in Classification Pass with Accounting Sanity Guardrails (skip rows already labeled by historical import)
        for txn in transactions:
            if txn.get("is_classified"):
                continue
            builtin_result = self._run_builtin_classifiers(txn)
            if builtin_result:
                txn["internal_category"] = builtin_result["category"]
                txn["classification_category"] = builtin_result["category"]
                txn["sub_category"] = builtin_result.get("sub_category") or txn.get("sub_category")
                txn["classification_source"] = "BUILTIN"
                txn["classification_confidence"] = builtin_result["confidence"]
                txn["auto_confirmed"] = builtin_result["confidence"] >= 85
                txn["classification_reasoning"] = builtin_result.get("reasoning")
                txn["is_classified"] = True
                if builtin_result["category"] == "REVERSED_COLLECTION":
                    txn["is_reversal"] = True

        if transactions:
            sorted_txns = sorted(transactions, key=lambda x: x["booking_date"])
            metadata["start_date"] = sorted_txns[0]["booking_date"]
            metadata["end_date"] = sorted_txns[-1]["booking_date"]
            earliest_txn = transactions[0]
            latest_txn = transactions[-1]
            # Only back-calculate opening_balance when an explicit reported balance column exists on the earliest row
            if metadata.get("opening_balance") is None and earliest_txn.get("_reported_balance") is not None:
                e_credit = Decimal(str(earliest_txn.get("credit_amount") or 0))
                e_debit = Decimal(str(earliest_txn.get("debit_amount") or 0))
                metadata["opening_balance"] = earliest_txn["_reported_balance"] - (e_credit - e_debit)
            if metadata.get("closing_balance") is None and latest_txn.get("running_balance") is not None:
                metadata["closing_balance"] = latest_txn["running_balance"]

            for txn in transactions:
                txn.pop("_reported_balance", None)
                txn.pop("_signed_amount", None)

        del df
        _reclaim_system_memory()
        return transactions, metadata

    def process_ingestion(self, db: Session, content: bytes, file_type: str, company_id: int, user_id: int, overrides: Dict[str, Any]) -> BankStatement:
        """
        Coordinates schema-agnostic parsing, at-rest AES-256-GCM sealing + PII tokenization,
        online rule evaluation, and relationship detection.
        """
        bank_id = overrides.get("bank_id", 1)
        txns_data, detected_meta = self.parse_statement_content(
            content,
            file_type,
            db=db,
            company_id=company_id,
            bank_id=bank_id,
            pdf_password=overrides.get("pdf_password"),
        )
        logger.info(f"Parsed {len(txns_data)} transactions.")

        def to_dt(val):
            if not val:
                return None
            if isinstance(val, datetime):
                return val
            try:
                return pd.to_datetime(val, dayfirst=True).to_pydatetime()
            except Exception:
                return None

        start_date = to_dt(overrides.get("start_date")) or detected_meta.get("start_date")
        end_date = to_dt(overrides.get("end_date")) or detected_meta.get("end_date")
        
        opening_balance = self._clean_decimal(overrides.get("opening_balance")) if overrides.get("opening_balance") else (detected_meta.get("opening_balance") or Decimal("0.00"))
        closing_balance = self._clean_decimal(overrides.get("closing_balance")) if overrides.get("closing_balance") else (detected_meta.get("closing_balance") or Decimal("0.00"))

        account_number = overrides.get("account_number") or detected_meta.get("account_number")
        currency_id = overrides.get("currency_id")

        if not txns_data:
            raise HTTPException(
                status_code=400,
                detail="No valid bank transactions were found in the uploaded file. Please check the file contents and format.",
            )

        if not start_date or not end_date:
            raise HTTPException(
                status_code=400,
                detail=f"Could not detect valid statement dates across {len(txns_data)} rows.",
            )

        # Guard against accidental duplicate uploads of the exact same statement period & balances
        dup_candidates = db.query(BankStatement).filter(
            BankStatement.company_id == company_id,
            BankStatement.bank_id == bank_id,
            BankStatement.is_deleted == False,
            BankStatement.statement_start_date == start_date,
            BankStatement.statement_end_date == end_date,
            BankStatement.opening_balance == opening_balance,
            BankStatement.closing_balance == closing_balance,
        ).all()
        for dup_stmt in dup_candidates:
            existing_txn_cnt = db.query(BankTransaction).filter(
                BankTransaction.statement_id == dup_stmt.id,
                BankTransaction.is_deleted == False,
            ).count()
            if existing_txn_cnt == len(txns_data):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Duplicate statement rejected: This bank statement "
                        f"({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}, "
                        f"{existing_txn_cnt} transactions) has already been uploaded (Statement #{dup_stmt.id})."
                    ),
                )

        continuity_status = detected_meta.get("balance_continuity_status", "VALIDATED")
        db_stmt = BankStatement(
            bank_id=bank_id,
            company_id=company_id,
            file_name=overrides.get("file_name", "unknown"),
            opening_balance=opening_balance,
            closing_balance=closing_balance,
            statement_start_date=start_date,
            statement_end_date=end_date,
            account_number=account_number,
            currency_id=currency_id,
            created_by=user_id,
            status=continuity_status,
            balance_continuity_status=continuity_status,
            balance_variance_amount=detected_meta.get("balance_variance_amount", Decimal("0.00")),
            continuity_warnings_json=detected_meta.get("continuity_warnings") or None,
            detected_mapping_json=detected_meta.get("detected_mapping") or None,
        )
        db.add(db_stmt)
        db.flush()

        # Load Tenant DEK & known entities for at-rest encryption and deterministic PII tokenization
        dek = reconciliation_confidentiality.get_tenant_dek(db, company_id)
        known_entities = []
        try:
            cps = db.query(Counterparty.name).filter(
                or_(Counterparty.company_id == company_id, Counterparty.company_id == None),
                Counterparty.is_deleted == False
            ).limit(500).all()
            known_entities = [c[0] for c in cps if c and c[0]]
        except Exception:
            pass

        created_txns: List[BankTransaction] = []
        companion_links: List[Tuple[int, int]] = []

        for idx, t_data in enumerate(txns_data):
            parent_idx = t_data.pop("_companion_parent_idx", None)
            t_data.pop("_has_companion_parent", None)
            custom_fields = t_data.get("custom_fields_json")

            db_txn = BankTransaction(
                statement_id=db_stmt.id,
                **t_data
            )
            if not db_txn.account_number:
                db_txn.account_number = account_number

            # Seal sensitive fields with Tenant DEK & generate tokenized_description
            reconciliation_confidentiality.seal_transaction_fields(
                db_txn, dek=dek, known_entities=known_entities, custom_fields=custom_fields
            )

            db.add(db_txn)
            created_txns.append(db_txn)
            if parent_idx is not None and 0 <= parent_idx < len(txns_data):
                companion_links.append((idx, parent_idx))

        db.flush()

        # Resolve companion_parent_id foreign keys
        for child_idx, parent_idx in companion_links:
            if child_idx < len(created_txns) and parent_idx < len(created_txns):
                created_txns[child_idx].companion_parent_id = created_txns[parent_idx].id

        db.commit()
        db.refresh(db_stmt)

        # Unsupervised Learning on Upload: auto-discover counterparties, bank fee scales, clearing float, and account purity
        try:
            unsup_res = self._unsupervised_learn_from_statement_upload(
                db=db,
                customer_id=company_id,
                statement=db_stmt,
                created_txns=created_txns,
                user_id=user_id,
            )
            logger.info(f"Unsupervised statement learning on #{db_stmt.id}: {unsup_res}")
        except Exception as unsup_err:
            logger.debug(f"Unsupervised learning on statement #{db_stmt.id} skipped: {unsup_err}")

        # If the uploaded statement contained pre-classified historical rows, automatically mine rules & profiles from them
        has_historical_labels = any(getattr(t, "classification_source", None) == "HISTORICAL_IMPORT" for t in created_txns)
        if has_historical_labels:
            try:
                hist_learn = self.learn_from_history(db, customer_id=company_id, statement_id=db_stmt.id, user_id=user_id)
                logger.info(f"Historical learning from uploaded statement #{db_stmt.id}: {hist_learn}")
            except Exception as hl_err:
                logger.warning(f"Historical learning on statement #{db_stmt.id} skipped: {hl_err}")

        # Run comprehensive online rule evaluation, concept matching, collaborative consensus, and relationship pairing
        try:
            result = self.apply_classification_rules(db, customer_id=company_id, statement_id=db_stmt.id)
            logger.info(f"Auto-classification result: {result}")
        except Exception as e:
            logger.warning(f"Auto-classification failed: {e}")

        return db_stmt

    # ═══════════════════════════════════════════════════════════════════
    #  ONLINE LEARNING THAT BUILDS & SELF-HEALS RULES
    # ═══════════════════════════════════════════════════════════════════

    def _update_account_profile_online(
        self,
        db: Session,
        customer_id: int,
        bank_id: Optional[int],
        account_number: Optional[str],
        category: str,
        dek: Optional[bytes] = None,
    ) -> Optional[BankAccountProfile]:
        """
        Online Account Purity Tracker:
        Tracks category frequency per bank account (using blind `account_token`).
        If an account is >=85% dedicated to a single category (e.g. Payroll or POS Collections),
        future transactions in that account receive an account-purity confidence boost.
        """
        if not account_number or not category or category in ("BANK_CHARGES", "TAX_DEDUCTION"):
            # Ignore incidental bank fees when computing primary operating purpose of an account
            return None

        acct_tok = reconciliation_confidentiality.compute_blind_token(account_number, dek, prefix="ACCT_TOK")
        profile = db.query(BankAccountProfile).filter(
            BankAccountProfile.company_id == customer_id,
            BankAccountProfile.account_token == acct_tok
        ).first()

        if not profile:
            enc_acct = encrypt_field(account_number, dek, field_context="account_profile:account_number") if dek else None
            counts = {category: 1}
            profile = BankAccountProfile(
                company_id=customer_id,
                bank_id=bank_id,
                account_token=acct_tok,
                encrypted_account_number=enc_acct,
                total_txns_observed=1,
                category_counts_json=counts,
                dominant_category=category,
                purity_ratio=Decimal("100.00"),
            )
            db.add(profile)
        else:
            counts = dict(profile.category_counts_json or {})
            counts[category] = counts.get(category, 0) + 1
            total = sum(counts.values())
            dom_cat, dom_count = max(counts.items(), key=lambda x: x[1])
            purity = round((dom_count / max(1, total)) * 100.0, 2)

            profile.category_counts_json = counts
            profile.total_txns_observed = total
            profile.dominant_category = dom_cat
            profile.purity_ratio = Decimal(str(purity))
            profile.last_observed_at = func.now()

        db.flush()
        return profile

    def _induce_or_evolve_rule_online(
        self,
        db: Session,
        txn: BankTransaction,
        customer_id: int,
        user_id: Optional[int],
        chosen_category: str,
        chosen_gl: Optional[str],
        chosen_sub_category: Optional[str] = None,
        previous_rule_id: Optional[int] = None,
        previous_category: Optional[str] = None,
        known_entities: Optional[List[str]] = None,
    ) -> Optional[ClassificationRule]:
        """
        Core Online Rule Induction & Self-Healing Engine ("Build Rules Based on Learning"):
        1. If the user overrode an existing rule (`previous_rule_id`), immediately self-heals
           that rule by recording the override, recalculating precision, and adding exclusion
           tokens or amount caps so it never repeats the mistake.
        2. Extracts the transaction's PII-scrubbed structural signature, direction, and amount tier,
           and creates or consolidates a transparent `ClassificationRule` in the DB.
        3. Promotes incubating `SUGGESTED` rules to `AUTO_CONFIRMED` once empirical precision is proven.
        """
        direction = "CREDIT" if (txn.credit_amount or 0) > 0 else "DEBIT"
        amt = float(max(txn.credit_amount or 0, txn.debit_amount or 0))
        amt_tier = txn.amount_tier or classify_amount_tier(amt)
        bank_id = txn.statement.bank_id if txn.statement else None

        sig_data = reconciliation_confidentiality.extract_privacy_safe_rule_signature(
            raw_description=txn.raw_description,
            description_line2=txn.description_line2,
            known_entities=known_entities,
            counterparty_name=txn.counterparty_name,
        )
        sig_tokens = sig_data.get("signature_tokens") or []
        sig_hash = sig_data.get("signature_hash")

        # ── Step 1: Online Mistake Adaptation (Self-Healing on Override) ──
        if previous_rule_id and previous_category and previous_category != chosen_category:
            misfired_rule = db.query(ClassificationRule).filter(
                ClassificationRule.id == previous_rule_id,
                ClassificationRule.company_id == customer_id
            ).first()
            if misfired_rule:
                misfired_rule.overrides_count = (misfired_rule.overrides_count or 0) + 1
                conf_cnt = max(1, misfired_rule.confirmations_count or 1)
                ovr_cnt = misfired_rule.overrides_count
                precision = round((conf_cnt / (conf_cnt + ovr_cnt)) * 100.0, 2)
                misfired_rule.empirical_precision = Decimal(str(precision))

                # Self-heal conditions_json: add distinguishing exclusion tokens or tighten amount cap
                cond_group = dict(misfired_rule.conditions_json or {})
                existing_conds = list(cond_group.get("conditions", []))
                rule_tokens = {
                    str(c.get("value", "")).upper()
                    for c in existing_conds
                    if c.get("operator") == "contains"
                }
                distinguishing_tokens = [t for t in sig_tokens if t not in rule_tokens]
                if distinguishing_tokens:
                    exclude_list = list(cond_group.get("exclude_tokens", []))
                    if distinguishing_tokens[0] not in exclude_list:
                        exclude_list.append(distinguishing_tokens[0])
                    cond_group["exclude_tokens"] = exclude_list

                # If a BANK_CHARGES rule misfired on a large payment, add an automatic amount cap
                if misfired_rule.assigned_category == "BANK_CHARGES" and amt > 5000:
                    cond_group["amount_max"] = min(float(cond_group.get("amount_max") or 5000.0), max(1000.0, amt * 0.5))

                misfired_rule.conditions_json = cond_group
                misfired_rule.rule_origin = "SYSTEM_ADAPTED"
                misfired_rule.calibrated_confidence = max(50, int(precision * 0.9))
                if precision < 80.0:
                    misfired_rule.lifecycle_state = "SUGGESTED"
                db.flush()

        # ── Step 2: Induce or Reinforce Rule for Chosen Category ──
        if not sig_tokens or not sig_hash:
            return None

        dir_field = "credit_amount" if direction == "CREDIT" else "debit_amount"
        existing_learned = db.query(ClassificationRule).filter(
            ClassificationRule.company_id == customer_id,
            ClassificationRule.signature_hash == sig_hash,
            ClassificationRule.is_deleted == False
        ).first()

        target_gl = chosen_gl or chosen_category
        if existing_learned:
            if existing_learned.assigned_category == chosen_category or existing_learned.assigned_gl_account == target_gl:
                existing_learned.confirmations_count = (existing_learned.confirmations_count or 1) + 1
                conf_cnt = existing_learned.confirmations_count
                ovr_cnt = existing_learned.overrides_count or 0
                precision = round((conf_cnt / (conf_cnt + ovr_cnt)) * 100.0, 2)
                existing_learned.empirical_precision = Decimal(str(precision))

                # Expand allowed amount tiers in conditions_json
                cond_group = dict(existing_learned.conditions_json or {})
                tiers = list(cond_group.get("allowed_tiers", [amt_tier]))
                if amt_tier not in tiers:
                    tiers.append(amt_tier)
                cond_group["allowed_tiers"] = tiers
                existing_learned.conditions_json = cond_group

                # Boost confidence and promote from SUGGESTED -> AUTO_CONFIRMED once proven (>=2 confirmations & >=90% precision)
                new_conf = min(98, 78 + (conf_cnt * 5))
                existing_learned.calibrated_confidence = int(new_conf * (precision / 100.0))
                if conf_cnt >= 2 and precision >= 90.0:
                    existing_learned.lifecycle_state = "AUTO_CONFIRMED"
                db.flush()
                return existing_learned
            else:
                # Existing learned rule had same signature but different category -> record override
                existing_learned.overrides_count = (existing_learned.overrides_count or 0) + 1

        # Build human-readable, UI-compatible composite conditions_json
        anchor_tokens = sig_tokens[:3]
        ui_conditions = [
            {"field": "raw_description", "operator": "contains", "value": tok, "joiner": "AND"}
            for tok in anchor_tokens
        ]
        ui_conditions.append({"field": dir_field, "operator": "gt", "value": 0, "joiner": "AND"})

        composite_conditions = {
            "conditions": ui_conditions,
            "direction": direction,
            "allowed_tiers": [amt_tier],
            "exclude_tokens": [],
            "iban_bank_code": sig_data.get("iban_bank_code"),
        }
        if chosen_category == "BANK_CHARGES":
            composite_conditions["amount_max"] = 10000.0

        auto_rule_name = f"Learned: {' '.join(anchor_tokens)} -> {chosen_category}"
        new_rule = ClassificationRule(
            company_id=customer_id,
            bank_id=bank_id,
            rule_name=auto_rule_name[:120],
            rule_type="LEARNED_COMPOSITE",
            rule_origin="LEARNED_ONLINE",
            lifecycle_state="SUGGESTED",  # Incubates as SUGGESTED, auto-promotes on next confirmation
            signature_hash=sig_hash,
            priority=50,
            stop_after_match=True,
            conditions_json=composite_conditions,
            assigned_gl_account=target_gl,
            assigned_category=chosen_category,
            assigned_sub_category=chosen_sub_category,
            usage_count=1,
            confirmations_count=1,
            overrides_count=0,
            empirical_precision=Decimal("100.00"),
            calibrated_confidence=82,
            is_active=True,
            created_by=user_id,
        )
        db.add(new_rule)
        db.flush()
        return new_rule

    def learn_from_history(
        self,
        db: Session,
        customer_id: int,
        statement_id: Optional[int] = None,
        user_id: Optional[int] = None,
        limit: int = 2000,
    ) -> Dict[str, Any]:
        """
        Bulk Historical Learning Engine:
        Scans historical classified statement lines (from uploaded historical sheets or previously
        classified transactions in PostgreSQL) and automatically builds/promotes:
        1. `ClassificationRule`s (online-induced composite rules with PII-scrubbed signatures & amount tiers)
        2. `BankAccountProfile`s (account purity ratios for dedicated payroll/collection accounts)
        3. `Counterparty` entries (when beneficiary/counterparty names are present)
        4. Anonymized `CollaborativePattern`s (zero-PII structural consensus)
        """
        from app.crud.crud_reconciliation_v2 import crud_counterparty
        from app.services.collaborative_learning_service import collaborative_service

        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_deleted == False,
            or_(
                BankTransaction.is_classified == True,
                BankTransaction.category != None,
                BankTransaction.classification_category != None
            )
        )
        if statement_id:
            query = query.filter(BankTransaction.statement_id == statement_id)

        historical_txns = query.order_by(BankTransaction.booking_date.asc()).limit(limit).all()
        if not historical_txns:
            return {
                "transactions_scanned": 0,
                "rules_induced_or_reinforced": 0,
                "account_profiles_updated": 0,
                "counterparties_learned": 0,
                "status": "No classified historical transactions found to learn from."
            }

        dek = reconciliation_confidentiality.get_tenant_dek(db, customer_id)
        known_entities = [
            c[0] for c in db.query(Counterparty.name).filter(
                or_(Counterparty.company_id == customer_id, Counterparty.company_id == None),
                Counterparty.is_deleted == False
            ).limit(500).all() if c and c[0]
        ]

        rules_touched = set()
        profiles_touched = set()
        cps_learned = 0
        scanned = 0

        for txn in historical_txns:
            cat = txn.classification_category or txn.category
            if not cat:
                continue
            # Do not induce redundant rules from built-in keyword matches unless confirmed/imported
            if txn.classification_source == "BUILTIN" and not txn.manual_override:
                continue

            scanned += 1
            resolved = reconciliation_confidentiality.resolve_transaction_fields(txn, dek)
            if resolved.get("raw_description"):
                txn.raw_description = resolved["raw_description"]
            if resolved.get("counterparty_name"):
                txn.counterparty_name = resolved["counterparty_name"]

            gl = txn.internal_category or cat
            sub_cat = txn.sub_category
            bank_id = txn.statement.bank_id if txn.statement else None

            # 1. Learn Counterparty if beneficiary/counterparty is present
            cp_name = txn.counterparty_name or txn.beneficiary_name
            if cp_name and len(cp_name.strip()) >= 3:
                ent_type = "CUSTOMER" if (txn.credit_amount or 0) > 0 else "SUPPLIER"
                crud_counterparty.learn_or_increment(
                    db=db,
                    name=cp_name.strip(),
                    customer_id=customer_id,
                    entity_type=ent_type,
                    default_category=cat,
                    default_gl=gl,
                    user_id=user_id
                )
                if cp_name.strip() not in known_entities:
                    known_entities.append(cp_name.strip())
                cps_learned += 1

            # 2. Update BankAccountProfile online purity
            prof = self._update_account_profile_online(
                db=db,
                customer_id=customer_id,
                bank_id=bank_id,
                account_number=txn.account_number,
                category=cat,
                dek=dek,
            )
            if prof:
                profiles_touched.add(prof.account_token)

            # 3. Induce or reinforce ClassificationRule
            rule = self._induce_or_evolve_rule_online(
                db=db,
                txn=txn,
                customer_id=customer_id,
                user_id=user_id,
                chosen_category=cat,
                chosen_gl=gl,
                chosen_sub_category=sub_cat,
                known_entities=known_entities,
            )
            if rule:
                rules_touched.add(rule.id)

            # 4. Feed privacy-safe signature to Collaborative Consensus Engine
            try:
                direction = "CREDIT" if (txn.credit_amount or 0) > 0 else "DEBIT"
                amt = max(txn.credit_amount or Decimal("0.00"), txn.debit_amount or Decimal("0.00"))
                collaborative_service.submit_confirmation(
                    db=db,
                    company_id=customer_id,
                    bank_id=bank_id,
                    raw_description=txn.raw_description,
                    direction=direction,
                    category=cat,
                    gl_account=gl,
                    amount=amt,
                    counterparty_name=cp_name,
                )
            except Exception:
                pass

        db.commit()
        return {
            "transactions_scanned": scanned,
            "rules_induced_or_reinforced": len(rules_touched),
            "account_profiles_updated": len(profiles_touched),
            "counterparties_learned": cps_learned,
            "status": f"Learned from {scanned} historical transactions: {len(rules_touched)} rules built/reinforced, {len(profiles_touched)} account profiles updated."
        }

    def _unsupervised_learn_from_statement_upload(
        self,
        db: Session,
        customer_id: int,
        statement: BankStatement,
        created_txns: List[BankTransaction],
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Unsupervised Learning on Every Statement Upload (Learns Before User Clicks):
        1. Ensures Foundational Corporate Rules & Collaborative Patterns are seeded.
        2. Auto-discovers recurring counterparties from structured `:86:` tags (`/BENM/`, `/ORDP/`),
           beneficiary columns, or ERP entity cross-checks, and registers them in `Counterparty`.
        3. Auto-profiles the bank's typical companion wire fee scale & Posting-to-Value Date
           clearing float (`clearing_float_days`) into `MatchingToleranceRule`.
        4. Updates `BankAccountProfile` purity statistics for high-confidence (>=90%) classifications.
        """
        from app.crud.crud_reconciliation_v2 import crud_counterparty

        banking_standards_ontology.ensure_foundational_knowledge_seeded(db, customer_id=customer_id)
        dek = reconciliation_confidentiality.get_tenant_dek(db, customer_id)

        # Load known ERP entity names (if any) to cross-verify extracted counterparties
        erp_entities = {
            (r[0] or "").strip().upper(): (r[0] or "").strip()
            for r in db.query(InternalLedgerRecord.entity_name).filter(
                InternalLedgerRecord.company_id == customer_id,
                InternalLedgerRecord.is_deleted == False,
                InternalLedgerRecord.entity_name != None,
            ).limit(1000).all()
            if r and r[0]
        }

        entity_counts: Dict[str, Dict[str, Any]] = {}
        max_companion_fee = Decimal("0.00")
        max_float_days = 0
        profiles_updated = 0

        for txn in created_txns:
            resolved = reconciliation_confidentiality.resolve_transaction_fields(txn, dek)
            cp_raw = (resolved.get("counterparty_name") or txn.counterparty_name or txn.beneficiary_name or "").strip()
            debit = Decimal(str(txn.debit_amount or "0.00"))
            credit = Decimal(str(txn.credit_amount or "0.00"))
            is_credit = credit > 0

            # 1. Track extracted counterparty candidates
            if len(cp_raw) >= 3:
                cp_key = cp_raw.upper()
                if cp_key not in entity_counts:
                    entity_counts[cp_key] = {"display_name": cp_raw, "credits": 0, "debits": 0}
                if is_credit:
                    entity_counts[cp_key]["credits"] += 1
                else:
                    entity_counts[cp_key]["debits"] += 1

            # 2. Observe companion wire fees & embedded /CHGS/ fees + clearing float
            cf = resolved.get("custom_fields") or txn.custom_fields_json or {}
            if txn.companion_parent_id and Decimal("0.00") < debit <= Decimal("2500.00"):
                max_companion_fee = max(max_companion_fee, debit)
            if cf.get("embedded_fee_amount"):
                try:
                    emb_fee = Decimal(str(cf["embedded_fee_amount"]))
                    if Decimal("0.00") < emb_fee <= Decimal("2500.00"):
                        max_companion_fee = max(max_companion_fee, emb_fee)
                except Exception:
                    pass
            if cf.get("clearing_float_days"):
                try:
                    max_float_days = max(max_float_days, abs(int(cf["clearing_float_days"])))
                except Exception:
                    pass

            # 3. Auto-update BankAccountProfile for high-confidence (>=90%) standard classifications
            if (txn.classification_confidence or 0) >= 90 and txn.classification_category:
                prof = self._update_account_profile_online(
                    db=db,
                    customer_id=customer_id,
                    bank_id=statement.bank_id,
                    account_number=txn.account_number or statement.account_number,
                    category=txn.classification_category,
                    dek=dek,
                )
                if prof:
                    profiles_updated += 1

        # Register counterparties that either recur >=2 times in consistent direction OR match ERP entities
        cps_learned = 0
        for cp_key, stats in entity_counts.items():
            total_occ = stats["credits"] + stats["debits"]
            matches_erp = cp_key in erp_entities or any(cp_key in ek or ek in cp_key for ek in erp_entities if len(ek) >= 4)
            if total_occ >= 2 or matches_erp:
                is_cust = stats["credits"] >= stats["debits"]
                ent_type = "CUSTOMER" if is_cust else "SUPPLIER"
                def_cat = "COLLECTION_FROM_CUSTOMER" if is_cust else "PAYMENT_TO_SUPPLIER"
                def_gl = "ACCOUNTS_RECEIVABLE" if is_cust else "ACCOUNTS_PAYABLE"
                crud_counterparty.learn_or_increment(
                    db=db,
                    name=stats["display_name"],
                    customer_id=customer_id,
                    entity_type=ent_type,
                    default_category=def_cat,
                    default_gl=def_gl,
                    user_id=user_id,
                )
                cps_learned += 1

        # Auto-update Bank Tolerance Rule if companion fees or clearing float were observed
        tolerance_updated = False
        if max_companion_fee > Decimal("0.00") or max_float_days > 0:
            self._learn_matching_tolerance_online(
                db=db,
                customer_id=customer_id,
                bank_id=statement.bank_id,
                direction="EITHER",
                record_type="AUTO_PROFILED",
                variance_amount=max_companion_fee if max_companion_fee > Decimal("0.00") else Decimal("50.00"),
                gross_amount=Decimal("10000.00"),
                date_lag_days=max(5, max_float_days),
                disposition="BANK_FEE_WRITEOFF",
                user_id=user_id,
            )
            tolerance_updated = True

        db.commit()
        return {
            "counterparties_auto_discovered": cps_learned,
            "account_profiles_updated": profiles_updated,
            "tolerance_profile_updated": tolerance_updated,
            "max_companion_fee_observed": float(max_companion_fee),
            "max_clearing_float_days": max_float_days,
        }

    # ═══════════════════════════════════════════════════════════════════
    #  RULE EVALUATION & 7-FACTOR CONFIDENCE CALIBRATION
    # ═══════════════════════════════════════════════════════════════════

    def _evaluate_condition(self, txn: BankTransaction, condition: Dict[str, Any]) -> bool:
        field = condition.get("field")
        op = condition.get("operator")
        val = condition.get("value")
        
        if field == "concept":
            concept_name = str(val).upper()
            debit = float(txn.debit_amount or 0)
            credit = float(txn.credit_amount or 0)
            if concept_name in ["COLLECTION", "COLLECTION_FROM_CUSTOMER"]:
                return credit > 0 and bool(txn.counterparty_name)
            elif concept_name in ["PAYMENT", "PAYMENT_TO_SUPPLIER", "SUPPLIER_PAYMENT"]:
                return debit > 0 and bool(txn.counterparty_name)
            elif concept_name in ["SWEEP", "TRANSFER", "INTER_BANK_SWEEP"]:
                return (txn.internal_category in ["INTER_BANK_SWEEP", "INTERNAL_TRANSFER"]) or bool(txn.linked_txn_id)
            return False

        if field in ["counterparty_name", "counterparty"]:
            txn_cp = (txn.counterparty_name or "").upper()
            if op in ["is_not_empty", "exists"]:
                return bool(txn.counterparty_name)
            elif op == "contains":
                return str(val).upper() in txn_cp
            elif op == "equals":
                return txn_cp == str(val).upper()
            return False

        # Support querying dynamic custom_fields.<key>
        if field and str(field).startswith("custom_fields."):
            cf_key = str(field).split(".", 1)[1]
            cf_dict = getattr(txn, "custom_fields_json", None) or {}
            txn_val = cf_dict.get(cf_key)
        else:
            txn_val = getattr(txn, field, None)
            if field == "raw_description" and not txn_val:
                txn_val = getattr(txn, "tokenized_description", None)

        if txn_val is None:
            return False
        
        if isinstance(txn_val, str):
            txn_val = txn_val.upper()
            val = str(val).upper()
        elif isinstance(txn_val, (Decimal, float, int)):
            try:
                txn_val = float(txn_val)
                val = float(val)
            except Exception:
                return False
            
        if op == "contains":
            return val in txn_val
        elif op == "not_contains":
            return val not in txn_val
        elif op == "equals":
            return txn_val == val
        elif op == "starts_with":
            return str(txn_val).startswith(str(val))
        elif op == "gt":
            return txn_val > val
        elif op == "lt":
            return txn_val < val
        return False

    def _evaluate_group(self, txn: BankTransaction, group: Dict[str, Any]) -> bool:
        """
        Evaluates a group of conditions against a transaction, including online self-healed
        constraints (`exclude_tokens`, `amount_max`, `amount_min`, `allowed_tiers`).
        """
        if not group or not isinstance(group, dict):
            return False

        # Check online self-healing guardrails first
        desc_upper = f"{getattr(txn, 'raw_description', '') or ''} {getattr(txn, 'description_line2', '') or ''}".upper()
        exclude_tokens = group.get("exclude_tokens") or []
        for ex_tok in exclude_tokens:
            if ex_tok and str(ex_tok).upper() in desc_upper:
                return False

        amt = float(max(getattr(txn, "debit_amount", 0) or 0, getattr(txn, "credit_amount", 0) or 0))
        if group.get("amount_max") is not None and amt > float(group["amount_max"]):
            return False
        if group.get("amount_min") is not None and amt < float(group["amount_min"]):
            return False

        if "concept" in group:
            return self._evaluate_condition(txn, {"field": "concept", "operator": "equals", "value": group["concept"]})

        conditions = group.get("conditions", [])
        if not conditions:
            return False
            
        cond0 = conditions[0]
        result = self._evaluate_group(txn, cond0) if "conditions" in cond0 else self._evaluate_condition(txn, cond0)
        
        for i in range(1, len(conditions)):
            cond = conditions[i]
            joiner = str(cond.get("joiner", "AND")).upper()
            current_val = self._evaluate_group(txn, cond) if "conditions" in cond else self._evaluate_condition(txn, cond)
            
            if joiner == "OR":
                result = result or current_val
            else:
                result = result and current_val
                
        return result

    def _calibrate_confidence_and_explain(
        self,
        txn: BankTransaction,
        candidates: List[Dict[str, Any]],
        account_profile: Optional[BankAccountProfile] = None,
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        7-Factor Reality-Calibrated Confidence Scorer:
        1. Base source / empirical rule precision
        2. Top-1 vs Top-2 ambiguity margin dampener
        3. BankAccountProfile purity alignment (+7 boost if aligned with >=85% dedicated account, -12 if contradictory)
        4. Amount-scale sanity guardrail (prevents large vendor payments from auto-confirming as BANK_CHARGES)
        5. Companion fee / IBAN verification bonus (+6)
        6. Sparse narrative penalty (-8 if description < 6 chars)
        7. Rule lifecycle governance (AUTO_CONFIRMED vs SUGGESTED)
        """
        if not candidates:
            return None, []

        amt = float(max(txn.debit_amount or 0, txn.credit_amount or 0))
        desc_clean = (txn.raw_description or "").strip()

        calibrated_list = []
        for cand in candidates:
            score = float(cand.get("confidence", 75))
            reasons = [cand.get("reason", f"Matched {cand.get('source', 'Rule')}")]
            cat = cand.get("category", "")

            # Factor 1: Amount-scale sanity guardrail
            if cat == "BANK_CHARGES" and amt > 15000 and not txn.companion_parent_id:
                score -= 25
                reasons.append("Large amount dampened for Bank Charges (>15,000)")
            elif cat == "BANK_CHARGES" and txn.companion_parent_id:
                score += 6
                reasons.append("Linked to adjacent parent transfer")

            # Factor 2: Single-legged sweep caution
            if cat in ("INTER_BANK_SWEEP", "INTERNAL_TRANSFER") and not txn.linked_txn_id and not txn.iban_bank_code:
                score = min(score, 78.0)
                reasons.append("Unpaired single-leg transfer (awaiting counterpart leg)")

            # Factor 3: Account purity alignment
            if account_profile and (account_profile.total_txns_observed or 0) >= 5:
                purity = float(account_profile.purity_ratio or 0)
                dom_cat = account_profile.dominant_category
                if purity >= 85.0 and dom_cat:
                    if cat == dom_cat:
                        score += 7
                        reasons.append(f"Aligned with dedicated account profile ({purity:.0f}% {dom_cat})")
                    elif cat not in ("BANK_CHARGES", "TAX_DEDUCTION", "BANK_INTEREST"):
                        score -= 10
                        reasons.append(f"Differs from dominant account profile ({dom_cat})")

            # Factor 4: Sparse text penalty
            if len(desc_clean) < 6:
                score -= 8
                reasons.append("Sparse narrative text")

            cand_copy = dict(cand)
            cand_copy["raw_score"] = score
            cand_copy["reasons"] = reasons
            calibrated_list.append(cand_copy)

        # Sort candidates by calibrated score descending
        calibrated_list.sort(key=lambda x: x["raw_score"], reverse=True)

        # Factor 5: Ambiguity margin between Top-1 and Top-2 of DIFFERENT categories
        top1 = calibrated_list[0]
        distinct_cats = []
        for c in calibrated_list:
            if c["category"] not in [d["category"] for d in distinct_cats]:
                distinct_cats.append(c)

        if len(distinct_cats) >= 2:
            margin = distinct_cats[0]["raw_score"] - distinct_cats[1]["raw_score"]
            if margin < 8:
                top1["raw_score"] -= 8
                top1["reasons"].append(f"Competing alternative '{distinct_cats[1]['category']}' within {margin:.0f} pts")

        final_conf = int(max(35, min(99, round(top1["raw_score"]))))
        top1["confidence"] = final_conf
        top1["reasoning_text"] = " | ".join(top1["reasons"])

        top_3_summary = [
            {
                "category": c["category"],
                "gl_account": c.get("gl_account"),
                "confidence": int(max(30, min(99, round(c["raw_score"])))),
                "source": c.get("source"),
            }
            for c in distinct_cats[:3]
        ]
        return top1, top_3_summary

    def apply_classification_rules(self, db: Session, customer_id: int, statement_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Multi-Layer Cognitive & Online-Rule Classification Pipeline:
        Evaluates Custom & Online-Learned Rules, Built-in Classifiers, Counterparty Concepts,
        and Collaborative Consensus Patterns, calibrating confidence and generating full audit explainability.
        """
        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.manual_override == False
        )
        if statement_id:
            query = query.filter(BankTransaction.statement_id == statement_id)
            
        transactions = query.all()
        dek = reconciliation_confidentiality.get_tenant_dek(db, customer_id)

        # Ensure Day-1 Foundational Rules & Collaborative Patterns are seeded in DB
        banking_standards_ontology.ensure_foundational_knowledge_seeded(db, customer_id=customer_id)

        # Preload active rules (both manual, foundational, and online-learned)
        rules = db.query(ClassificationRule).filter(
            ClassificationRule.company_id == customer_id,
            ClassificationRule.is_active == True,
            ClassificationRule.is_deleted == False
        ).order_by(ClassificationRule.priority.asc(), ClassificationRule.confirmations_count.desc()).all()

        # Preload counterparties
        from app.crud.crud_reconciliation_v2 import crud_counterparty
        counterparties = crud_counterparty.find_all_active(db, customer_id)
        sorted_cps = sorted(counterparties, key=lambda c: len(c.name), reverse=True)

        # Preload account profiles
        acct_profiles = {
            p.account_token: p
            for p in db.query(BankAccountProfile).filter(BankAccountProfile.company_id == customer_id).all()
        }

        from app.services.collaborative_learning_service import collaborative_service

        builtin_count = 0
        rule_count = 0
        concept_count = 0
        collab_count = 0

        for txn in transactions:
            # Resolve encrypted narrative in memory if needed
            resolved = reconciliation_confidentiality.resolve_transaction_fields(txn, dek)
            raw_desc = resolved.get("raw_description") or txn.raw_description or ""

            candidates: List[Dict[str, Any]] = []

            # 1. Evaluate Custom & Online-Learned Rules
            for rule in rules:
                if rule.bank_id and txn.statement and rule.bank_id != txn.statement.bank_id:
                    continue
                if self._evaluate_group(txn, rule.conditions_json):
                    base_conf = rule.calibrated_confidence or (95 if rule.rule_origin == "MANUAL" else 84)
                    cat = rule.assigned_category or rule.assigned_gl_account
                    candidates.append({
                        "category": cat,
                        "sub_category": rule.assigned_sub_category,
                        "gl_account": rule.assigned_gl_account,
                        "source": "LEARNED_RULE" if rule.rule_origin == "LEARNED_ONLINE" else "RULE",
                        "confidence": base_conf,
                        "rule_id": rule.id,
                        "lifecycle_state": rule.lifecycle_state or "AUTO_CONFIRMED",
                        "reason": f"Matched Rule '{rule.rule_name or rule.id}' ({rule.confirmations_count or 1} confirmations, {float(rule.empirical_precision or 100):.0f}% precision)",
                    })
                    rule.usage_count = (rule.usage_count or 0) + 1
                    rule.last_triggered_date = datetime.now()
                    if rule.stop_after_match and rule.lifecycle_state == "AUTO_CONFIRMED":
                        break

            # 2. Evaluate Built-in Domain Classifiers
            builtin_res = self._run_builtin_classifiers({
                "raw_description": raw_desc,
                "description_line2": txn.description_line2,
                "debit_amount": txn.debit_amount,
                "credit_amount": txn.credit_amount,
            })
            if builtin_res:
                candidates.append({
                    "category": builtin_res["category"],
                    "sub_category": builtin_res.get("sub_category"),
                    "gl_account": builtin_res["category"],
                    "source": "BUILTIN",
                    "confidence": builtin_res["confidence"],
                    "lifecycle_state": "AUTO_CONFIRMED",
                    "reason": builtin_res.get("reasoning") or f"Matched Built-in Classifier '{builtin_res['classifier_name']}'",
                })

            # 3. Evaluate Counterparty & Concept Rules
            desc_upper = f"{raw_desc} {txn.description_line2 or ''}".upper()
            for cp in sorted_cps:
                cp_match = (len(cp.name) >= 3 and cp.name.upper() in desc_upper)
                if not cp_match and cp.aliases and isinstance(cp.aliases, list):
                    cp_match = any(len(str(a)) >= 3 and str(a).upper() in desc_upper for a in cp.aliases)
                if cp_match:
                    txn.counterparty_name = cp.name
                    debit = txn.debit_amount or Decimal("0.00")
                    credit = txn.credit_amount or Decimal("0.00")
                    # Check if this is a reversed collection on Debit!
                    sem_flags = reconciliation_confidentiality.detect_semantic_flags(
                        raw_desc, "CREDIT" if credit > 0 else "DEBIT", max(debit, credit), txn.booking_date
                    )
                    if debit > 0 and sem_flags.get("is_negative_collection"):
                        cat = "REVERSED_COLLECTION"
                        gl = cp.default_gl_account or "ACCOUNTS_RECEIVABLE"
                        reason_str = f"Reversed/Bounced collection detected for counterparty '{cp.name}'"
                    elif credit > 0 and cp.entity_type in ["CUSTOMER", "OTHER"]:
                        cat = cp.default_category or "COLLECTION_FROM_CUSTOMER"
                        gl = cp.default_gl_account or "ACCOUNTS_RECEIVABLE"
                        reason_str = f"Credit inflow from registered {cp.entity_type} '{cp.name}'"
                    elif debit > 0 and cp.entity_type in ["SUPPLIER", "OTHER"]:
                        cat = cp.default_category or "PAYMENT_TO_SUPPLIER"
                        gl = cp.default_gl_account or "ACCOUNTS_PAYABLE"
                        reason_str = f"Debit outflow to registered {cp.entity_type} '{cp.name}'"
                    elif cp.entity_type == "BANK":
                        cat = cp.default_category or "BANK_TRANSFER"
                        gl = cp.default_gl_account or "INTERNAL_TRANSFER"
                        reason_str = f"Counterparty is registered Bank '{cp.name}'"
                    else:
                        cat = cp.default_category or "GOVERNMENT_PAYMENT"
                        gl = cp.default_gl_account or "TAX_EXPENSE"
                        reason_str = f"Counterparty is registered entity '{cp.name}'"

                    candidates.append({
                        "category": cat,
                        "gl_account": gl,
                        "source": "CONCEPT",
                        "confidence": 88 if cp.is_verified else 84,
                        "lifecycle_state": "AUTO_CONFIRMED" if cp.is_verified else "SUGGESTED",
                        "reason": reason_str,
                    })
                    break

            # 4. Evaluate Privacy-Safe Collaborative Consensus Patterns
            direction = "CREDIT" if (txn.credit_amount or 0) > 0 else "DEBIT"
            bank_id = txn.statement.bank_id if txn.statement else None
            amt_val = max(txn.credit_amount or Decimal("0.00"), txn.debit_amount or Decimal("0.00"))
            collab_pred = collaborative_service.match_collaborative(
                db=db,
                bank_id=bank_id,
                raw_description=raw_desc,
                direction=direction,
                description_line2=txn.description_line2,
                amount=amt_val,
            )
            if collab_pred:
                candidates.append({
                    "category": collab_pred["category"],
                    "gl_account": collab_pred.get("gl_account") or collab_pred["category"],
                    "source": "COLLABORATIVE",
                    "confidence": collab_pred["confidence"],
                    "lifecycle_state": "AUTO_CONFIRMED" if collab_pred["confidence"] >= 85 else "SUGGESTED",
                    "reason": f"Collaborative consensus '{collab_pred['signature']}' across {collab_pred['distinct_tenants']} companies",
                })

            if candidates:
                acct_tok = reconciliation_confidentiality.compute_blind_token(txn.account_number or "", dek, prefix="ACCT_TOK")
                acct_prof = acct_profiles.get(acct_tok)
                winner, top_3 = self._calibrate_confidence_and_explain(txn, candidates, account_profile=acct_prof)
                if winner:
                    is_auto = (winner.get("lifecycle_state") == "AUTO_CONFIRMED") and (winner["confidence"] >= 80)
                    txn.classification_category = winner["category"]
                    txn.suggested_category = winner["category"]
                    txn.internal_category = winner.get("gl_account") or winner["category"]
                    if winner.get("sub_category"):
                        txn.sub_category = winner["sub_category"]
                    txn.classification_source = winner["source"]
                    txn.classification_confidence = winner["confidence"]
                    txn.classification_reasoning = winner["reasoning_text"]
                    txn.top_suggestions_json = top_3
                    txn.applied_rule_id = winner.get("rule_id")
                    txn.auto_confirmed = is_auto
                    txn.is_classified = True

                    src = winner["source"]
                    if src in ("RULE", "LEARNED_RULE"):
                        rule_count += 1
                    elif src == "BUILTIN":
                        builtin_count += 1
                    elif src == "CONCEPT":
                        concept_count += 1
                    elif src == "COLLABORATIVE":
                        collab_count += 1

        db.commit()

        # Layer 5: Detect sweeps, internal transfers, and reversals across accounts
        try:
            rel_res = self._detect_logical_relationships_sync(db, customer_id=customer_id)
        except Exception as e:
            logger.warning(f"Relationship detection skipped during classification: {e}")
            rel_res = {}

        total_classified = builtin_count + rule_count + concept_count + collab_count
        return {
            "classified_count": total_classified,
            "builtin_matched": builtin_count,
            "rules_matched": rule_count,
            "concept_matched": concept_count,
            "collaborative_matched": collab_count,
            "relationships_detected": rel_res.get("affected_count", 0),
            "status": f"Successfully classified {total_classified} transactions ({builtin_count} built-in, {rule_count} rules, {concept_count} concepts, {collab_count} collaborative)."
        }

    # ═══════════════════════════════════════════════════════════════════
    #  4-PASS MATHEMATICAL & LEARNED-TOLERANCE MATCHING ENGINE
    # ═══════════════════════════════════════════════════════════════════

    def _learn_matching_tolerance_online(
        self,
        db: Session,
        customer_id: int,
        bank_id: Optional[int],
        direction: str,
        record_type: Optional[str],
        variance_amount: Decimal,
        gross_amount: Decimal,
        date_lag_days: int,
        disposition: str = "BANK_FEE_WRITEOFF",
        user_id: Optional[int] = None,
    ) -> Optional[MatchingToleranceRule]:
        """
        Online Tolerance Rule Induction:
        When an accountant confirms a match with a fee/FX variance or date lag,
        learns or refines a `MatchingToleranceRule` for future automatic tolerance passes.
        """
        if variance_amount <= Decimal("0.00") or gross_amount <= Decimal("0.00"):
            return None
        pct_var = float(variance_amount / gross_amount)
        if pct_var > 0.10:  # Don't learn extreme >10% variances as automatic fee tolerances
            return None

        rule = db.query(MatchingToleranceRule).filter(
            MatchingToleranceRule.company_id == customer_id,
            MatchingToleranceRule.bank_id == bank_id,
            MatchingToleranceRule.direction == direction,
            MatchingToleranceRule.is_deleted == False
        ).first()

        if not rule:
            rule = MatchingToleranceRule(
                company_id=customer_id,
                bank_id=bank_id,
                rule_name=f"Learned {direction} Fee/Lag Tolerance",
                pass_type="FEE_TOLERANCE",
                direction=direction,
                record_type=record_type,
                max_date_window_days=max(7, abs(date_lag_days) + 2),
                max_fixed_fee_variance=max(Decimal("50.00"), (variance_amount * Decimal("1.15")).quantize(Decimal("1.00"))),
                max_percentage_variance=Decimal(str(round(max(0.015, pct_var * 1.15), 4))),
                default_variance_disposition=disposition or "BANK_FEE_WRITEOFF",
                variance_gl_account="BANK_CHARGES",
                confirmations_count=1,
                lifecycle_state="AUTO_CONFIRMED",
                is_active=True,
                created_by=user_id,
            )
            db.add(rule)
        else:
            rule.confirmations_count = (rule.confirmations_count or 1) + 1
            if variance_amount > (rule.max_fixed_fee_variance or Decimal("0.00")):
                rule.max_fixed_fee_variance = (variance_amount * Decimal("1.10")).quantize(Decimal("1.00"))
            if abs(date_lag_days) > (rule.max_date_window_days or 7):
                rule.max_date_window_days = min(30, abs(date_lag_days) + 2)

        db.flush()
        return rule

    def run_matching_engine(self, db: Session, customer_id: int, user_id: int, statement_id: Optional[int] = None) -> Dict[str, Any]:
        """
        4-Pass Enterprise Mathematical & Learned-Tolerance Matching Engine:
        - Pass 1: Global Optimal 1:1 Assignment (Hungarian Algorithm / Minimum Cost Assignment)
                  for Exact Reference, Entity Name, and Tier 1 Penny-Rounding matches.
        - Pass 2: Learned Fee / WHT / FX Tolerance Pass (matches where Bank = ERP - Fee within learned tolerance).
        - Pass 3: Automated Many-to-1 (Batch ERP Receipts -> 1 Bank Deposit) & 1-to-Many (Split Bank Lines -> 1 ERP Record).
        - Pass 4: Treasury LG Reference Matching & Two-Legged Inter-Bank Sweep/Reversal Detection.
        """
        rel_result = {}
        try:
            rel_result = self._detect_logical_relationships_sync(db, customer_id)
        except Exception as e:
            logger.warning(f"Relationship detection during auto-match failed: {e}")

        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_reconciled == False,
            BankTransaction.is_deleted == False
        )
        if statement_id:
            query = query.filter(BankTransaction.statement_id == statement_id)
            
        transactions = query.all()
        open_erp_records = db.query(InternalLedgerRecord).filter(
            InternalLedgerRecord.company_id == customer_id,
            InternalLedgerRecord.status == "OPEN",
            InternalLedgerRecord.is_deleted == False
        ).all()

        # Load learned tolerance rules for Pass 2
        tolerance_rules = db.query(MatchingToleranceRule).filter(
            MatchingToleranceRule.company_id == customer_id,
            MatchingToleranceRule.is_active == True,
            MatchingToleranceRule.is_deleted == False
        ).all()

        matched_count = 0
        pass1_count = 0
        pass2_tolerance_count = 0
        pass3_batch_count = 0
        used_bank_ids = set()
        used_erp_ids = set()

        # ── PASS 1: Global Optimal 1:1 Assignment (Exact & Reference & Penny Rounding) ──
        if transactions and open_erp_records:
            BIG_M = 1e9
            n_bank = len(transactions)
            n_erp = len(open_erp_records)
            cost_matrix = [[BIG_M for _ in range(n_erp)] for _ in range(n_bank)]
            match_meta: Dict[Tuple[int, int], str] = {}

            for i, txn in enumerate(transactions):
                if txn.is_reconciled:
                    continue
                desc_upper = f"{txn.raw_description or ''} {txn.e2e_id or ''} {txn.back_office_ref or ''}".upper()
                cp_upper = (txn.counterparty_name or "").strip().upper()
                txn_credit = Decimal(str(txn.credit_amount or 0))
                txn_debit = Decimal(str(txn.debit_amount or 0))
                txn_is_credit = txn_credit > Decimal("0.00")
                txn_abs = float(txn_credit if txn_is_credit else txn_debit)
                if txn_abs <= 0:
                    continue

                txn_ccy = (txn.currency or "EGP").strip().upper()

                for j, erp in enumerate(open_erp_records):
                    erp_ccy = (erp.currency or "EGP").strip().upper()
                    if txn_ccy != erp_ccy:
                        continue
                    erp_amt = Decimal(str(erp.amount or 0))
                    erp_is_credit = erp_amt > Decimal("0.00")
                    if txn_is_credit != erp_is_credit:
                        continue

                    erp_abs = float(abs(erp_amt))
                    amt_diff = abs(txn_abs - erp_abs)
                    if amt_diff > 1.00:  # Pass 1 only considers exact or penny rounding <= 1.00
                        continue

                    ref_clean = (erp.reference_number or "").strip().upper()
                    ent_clean = (erp.entity_name or "").strip().upper()

                    has_ref = len(ref_clean) >= 4 and ref_clean in desc_upper
                    has_ent = (len(ent_clean) >= 3 and ent_clean in desc_upper) or (cp_upper and ent_clean in cp_upper)

                    days_diff = self._effective_date_distance_days(
                        txn.booking_date, getattr(txn, "value_date", None), erp.record_date
                    )

                    if not has_ref and not has_ent and days_diff > 10:
                        continue

                    # Compute assignment cost (lower is better: Reference < Entity+Amount < Exact Amount+Close Date)
                    if has_ref and amt_diff <= 0.05:
                        cost = 1.0 + days_diff * 0.1
                        logic = "REFERENCE"
                    elif has_ent and amt_diff <= 0.05:
                        cost = 10.0 + days_diff * 0.5
                        logic = "EXACT"
                    elif (has_ref or has_ent) and amt_diff <= 1.00:
                        cost = 25.0 + amt_diff * 5.0 + days_diff * 0.5
                        logic = "TOLERANCE"
                    elif amt_diff < 0.01 and days_diff <= 5:
                        cost = 50.0 + days_diff * 2.0
                        logic = "EXACT"
                    else:
                        continue

                    cost_matrix[i][j] = cost
                    match_meta[(i, j)] = logic

            # Solve optimal global assignment via scipy Hungarian algorithm (or greedy sorted cost fallback)
            assigned_pairs: List[Tuple[int, int]] = []
            try:
                import numpy as np
                from scipy.optimize import linear_sum_assignment
                mat = np.array(cost_matrix, dtype=float)
                row_ind, col_ind = linear_sum_assignment(mat)
                for r, c in zip(row_ind, col_ind):
                    if mat[r, c] < BIG_M * 0.1:
                        assigned_pairs.append((int(r), int(c)))
            except Exception:
                # Fallback: sort all valid (i, j) edges by cost ascending
                valid_edges = [
                    (cost_matrix[i][j], i, j)
                    for (i, j) in match_meta.keys()
                    if cost_matrix[i][j] < BIG_M * 0.1
                ]
                valid_edges.sort(key=lambda x: x[0])
                for _, r, c in valid_edges:
                    if transactions[r].id not in used_bank_ids and open_erp_records[c].id not in used_erp_ids:
                        assigned_pairs.append((r, c))

            for r, c in assigned_pairs:
                txn = transactions[r]
                erp = open_erp_records[c]
                if txn.id in used_bank_ids or erp.id in used_erp_ids:
                    continue
                logic = match_meta.get((r, c), "EXACT")
                txn_abs = max(Decimal(str(txn.credit_amount or 0)), Decimal(str(txn.debit_amount or 0)))
                var_amt = abs(txn_abs - abs(Decimal(str(erp.amount or 0))))

                new_match = ReconciliationMatch(
                    bank_txn_id=txn.id,
                    source_type=erp.record_type,
                    source_record_id=erp.id,
                    match_type="1:1",
                    match_logic=logic,
                    variance_amount=var_amt,
                    variance_disposition="ROUNDING" if var_amt > 0 else "NONE",
                    created_by=user_id
                )
                db.add(new_match)
                txn.is_reconciled = True
                if var_amt > 0:
                    txn.variance_amount = var_amt
                erp.status = "RECONCILED"
                erp.matched_bank_txn_id = txn.id
                used_bank_ids.add(txn.id)
                used_erp_ids.add(erp.id)
                matched_count += 1
                pass1_count += 1

        # ── PASS 2: Learned Fee / WHT / FX Tolerance Pass ──
        for txn in transactions:
            if txn.is_reconciled or txn.id in used_bank_ids:
                continue
            desc_upper = f"{txn.raw_description or ''} {txn.e2e_id or ''} {txn.back_office_ref or ''}".upper()
            cp_upper = (txn.counterparty_name or "").strip().upper()
            txn_credit = Decimal(str(txn.credit_amount or 0))
            txn_debit = Decimal(str(txn.debit_amount or 0))
            txn_is_credit = txn_credit > Decimal("0.00")
            txn_abs = txn_credit if txn_is_credit else txn_debit
            direction = "CREDIT" if txn_is_credit else "DEBIT"
            bank_id = txn.statement.bank_id if txn.statement else None

            # Determine applicable tolerance bounds:
            # - Default unlearned bank-fee tolerance is strictly capped at <= 500 AND <= 2.5% (so SWIFT/wire fees like 150 on 1.25M match, while large partial payments never auto-writeoff).
            # - Explicit learned MatchingToleranceRule rows honor their configured max_fixed / max_pct bounds.
            has_learned_rule = False
            max_fixed = Decimal("500.00")
            max_pct = Decimal("0.0250")
            max_days = 7
            for tr in tolerance_rules:
                if (tr.bank_id in (None, bank_id)) and (tr.direction in ("EITHER", direction)):
                    has_learned_rule = True
                    max_fixed = max(max_fixed, Decimal(str(tr.max_fixed_fee_variance or 100)))
                    max_pct = max(max_pct, Decimal(str(tr.max_percentage_variance or 0.025)))
                    max_days = max(max_days, int(tr.max_date_window_days or 7))

            txn_ccy_p2 = (txn.currency or "EGP").strip().upper()
            for erp in open_erp_records:
                if erp.id in used_erp_ids:
                    continue
                if (erp.currency or "EGP").strip().upper() != txn_ccy_p2:
                    continue
                erp_amt = Decimal(str(erp.amount or 0))
                if (erp_amt > 0) != txn_is_credit:
                    continue
                erp_abs = abs(erp_amt)
                diff = abs(erp_abs - txn_abs)
                if diff <= Decimal("0.00"):
                    continue

                ref_clean = (erp.reference_number or "").strip().upper()
                ent_clean = (erp.entity_name or "").strip().upper()
                has_ref = len(ref_clean) >= 4 and ref_clean in desc_upper
                has_ent = (len(ent_clean) >= 3 and ent_clean in desc_upper) or (cp_upper and ent_clean in cp_upper)
                if not (has_ref or has_ent):
                    continue

                days_diff = self._effective_date_distance_days(
                    txn.booking_date, getattr(txn, "value_date", None), erp.record_date
                )
                if days_diff > max_days:
                    continue

                if has_learned_rule:
                    within_tolerance = (diff <= max_fixed) or (erp_abs > 0 and (diff / erp_abs) <= max_pct)
                else:
                    within_tolerance = (diff <= max_fixed) and (erp_abs > 0 and (diff / erp_abs) <= max_pct)
                if within_tolerance:
                    new_match = ReconciliationMatch(
                        bank_txn_id=txn.id,
                        source_type=erp.record_type,
                        source_record_id=erp.id,
                        match_type="1:1",
                        match_logic="TOLERANCE",
                        variance_amount=diff,
                        variance_disposition="BANK_FEE_WRITEOFF",
                        created_by=user_id
                    )
                    db.add(new_match)
                    txn.is_reconciled = True
                    txn.variance_amount = diff
                    erp.status = "RECONCILED"
                    erp.matched_bank_txn_id = txn.id
                    used_bank_ids.add(txn.id)
                    used_erp_ids.add(erp.id)
                    matched_count += 1
                    pass2_tolerance_count += 1
                    break

        # ── PASS 3: Automated Many-to-1 (Batch ERP Receipts -> 1 Lump-Sum Bank Line) & 1-to-Many ──
        rem_bank = [t for t in transactions if not t.is_reconciled and t.id not in used_bank_ids]
        rem_erp = [e for e in open_erp_records if e.id not in used_erp_ids]

        # 3A: Many ERP records -> 1 Bank Transaction (e.g. 2-6 customer invoices paid in 1 wire/deposit)
        for txn in rem_bank:
            if txn.id in used_bank_ids:
                continue
            txn_credit = Decimal(str(txn.credit_amount or 0))
            txn_debit = Decimal(str(txn.debit_amount or 0))
            txn_is_credit = txn_credit > Decimal("0.00")
            txn_abs = txn_credit if txn_is_credit else txn_debit
            b_date = txn.booking_date.replace(tzinfo=None) if txn.booking_date else None

            # Filter candidate ERP records with same currency, same sign, and within 7 days
            txn_ccy_p3 = (txn.currency or "EGP").strip().upper()
            cand_erps = []
            for e in rem_erp:
                if e.id in used_erp_ids:
                    continue
                if (e.currency or "EGP").strip().upper() != txn_ccy_p3:
                    continue
                e_amt = Decimal(str(e.amount or 0))
                if (e_amt > 0) != txn_is_credit or abs(e_amt) >= txn_abs:
                    continue
                if self._effective_date_distance_days(txn.booking_date, getattr(txn, "value_date", None), e.record_date) > 7:
                    continue
                cand_erps.append(e)

            if len(cand_erps) >= 2:
                # Check contiguous windows or same-entity subsets (up to size 6)
                matched_subset = None
                for start_k in range(len(cand_erps)):
                    running_sum = Decimal("0.00")
                    subset = []
                    for end_k in range(start_k, min(len(cand_erps), start_k + 6)):
                        subset.append(cand_erps[end_k])
                        running_sum += abs(Decimal(str(cand_erps[end_k].amount or 0)))
                        if len(subset) >= 2 and abs(running_sum - txn_abs) <= Decimal("1.00"):
                            matched_subset = subset
                            break
                    if matched_subset:
                        break

                if matched_subset:
                    txn.is_reconciled = True
                    used_bank_ids.add(txn.id)
                    for e_rec in matched_subset:
                        m = ReconciliationMatch(
                            bank_txn_id=txn.id,
                            source_type=e_rec.record_type,
                            source_record_id=e_rec.id,
                            match_type="1:M",
                            match_logic="BATCH_SUM",
                            variance_amount=Decimal("0.00"),
                            variance_disposition="NONE",
                            created_by=user_id
                        )
                        db.add(m)
                        e_rec.status = "RECONCILED"
                        e_rec.matched_bank_txn_id = txn.id
                        used_erp_ids.add(e_rec.id)
                    matched_count += 1
                    pass3_batch_count += 1

        # ── PASS 4: Treasury LG Number Match ──
        lg_records = db.query(LGRecord).filter(LGRecord.customer_id == customer_id).all()
        lg_map = {lg.lg_number.strip().upper(): lg for lg in lg_records if lg.lg_number}

        for txn in transactions:
            if txn.is_reconciled or txn.id in used_bank_ids:
                continue
            desc_upper = (txn.raw_description or "").upper()
            e2e_upper = (txn.e2e_id or "").upper()
            match_lg = None
            for lg_num, lg_obj in lg_map.items():
                if len(lg_num) > 4 and (lg_num in desc_upper or lg_num in e2e_upper):
                    match_lg = lg_obj
                    break
            if match_lg:
                new_match = ReconciliationMatch(
                    bank_txn_id=txn.id,
                    source_type="Treasury (LG)",
                    source_record_id=match_lg.id,
                    match_type="1:1",
                    match_logic="REFERENCE",
                    created_by=user_id
                )
                db.add(new_match)
                txn.is_reconciled = True
                used_bank_ids.add(txn.id)
                matched_count += 1

        db.commit()
        _reclaim_system_memory()
        return {
            "matched_count": matched_count,
            "pass1_optimal_1to1": pass1_count,
            "pass2_tolerance_matched": pass2_tolerance_count,
            "pass3_batch_grouped": pass3_batch_count,
            "relationships": rel_result,
            "status": f"Successfully auto-matched {matched_count} transactions (Pass 1 Exact/Ref: {pass1_count}, Pass 2 Tolerance: {pass2_tolerance_count}, Pass 3 Batch: {pass3_batch_count})."
        }

    def _match_counterparty_and_concept(self, db: Session, customer_id: int, statement_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Concept-based Intelligent Matching:
        Matches counterparties against transaction descriptions and applies direction-aware
        and reversal-aware concept rules.
        """
        from app.crud.crud_reconciliation_v2 import crud_counterparty
        counterparties = crud_counterparty.find_all_active(db, customer_id)
        if not counterparties:
            return {"matched_count": 0, "status": "No counterparties found in registry"}

        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_classified == False
        )
        if statement_id:
            query = query.filter(BankTransaction.statement_id == statement_id)
        
        unclassified_txns = query.all()
        matched_count = 0
        sorted_cps = sorted(counterparties, key=lambda c: len(c.name), reverse=True)

        for txn in unclassified_txns:
            desc_upper = f"{txn.raw_description or ''} {txn.description_line2 or ''}".upper()
            matched_cp = None

            for cp in sorted_cps:
                cp_name_upper = cp.name.upper()
                if len(cp_name_upper) >= 3 and cp_name_upper in desc_upper:
                    matched_cp = cp
                    break
                if cp.aliases and isinstance(cp.aliases, list):
                    for alias in cp.aliases:
                        alias_upper = str(alias).upper()
                        if len(alias_upper) >= 3 and alias_upper in desc_upper:
                            matched_cp = cp
                            break
                    if matched_cp:
                        break

            if matched_cp:
                txn.counterparty_name = matched_cp.name
                debit = txn.debit_amount or Decimal("0.00")
                credit = txn.credit_amount or Decimal("0.00")

                if credit > 0 and matched_cp.entity_type in ["CUSTOMER", "OTHER"]:
                    category = matched_cp.default_category or "COLLECTION_FROM_CUSTOMER"
                    gl = matched_cp.default_gl_account or "ACCOUNTS_RECEIVABLE"
                    txn.internal_category = gl
                    txn.classification_category = category
                    txn.suggested_category = category
                    txn.classification_source = "CONCEPT"
                    txn.classification_confidence = 85
                    txn.is_classified = True
                    matched_count += 1
                elif debit > 0 and matched_cp.entity_type in ["SUPPLIER", "OTHER"]:
                    category = matched_cp.default_category or "PAYMENT_TO_SUPPLIER"
                    gl = matched_cp.default_gl_account or "ACCOUNTS_PAYABLE"
                    txn.internal_category = gl
                    txn.classification_category = category
                    txn.suggested_category = category
                    txn.classification_source = "CONCEPT"
                    txn.classification_confidence = 85
                    txn.is_classified = True
                    matched_count += 1
                elif matched_cp.entity_type == "BANK":
                    category = matched_cp.default_category or "BANK_TRANSFER"
                    txn.internal_category = matched_cp.default_gl_account or "INTERNAL_TRANSFER"
                    txn.classification_category = category
                    txn.suggested_category = category
                    txn.classification_source = "CONCEPT"
                    txn.classification_confidence = 80
                    txn.is_classified = True
                    matched_count += 1
                elif matched_cp.entity_type == "GOVERNMENT":
                    category = matched_cp.default_category or "GOVERNMENT_PAYMENT"
                    txn.internal_category = matched_cp.default_gl_account or "TAX_EXPENSE"
                    txn.classification_category = category
                    txn.suggested_category = category
                    txn.classification_source = "CONCEPT"
                    txn.classification_confidence = 85
                    txn.is_classified = True
                    matched_count += 1

        db.commit()
        _reclaim_system_memory()
        return {"matched_count": matched_count, "status": f"Concept engine classified {matched_count} transactions."}

    def _match_collaborative_patterns(self, db: Session, customer_id: int, statement_id: Optional[int] = None) -> int:
        from app.services.collaborative_learning_service import collaborative_service
        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_classified == False
        )
        if statement_id:
            query = query.filter(BankTransaction.statement_id == statement_id)

        unclassified = query.all()
        matched = 0
        for txn in unclassified:
            direction = "CREDIT" if (txn.credit_amount or 0) > 0 else "DEBIT"
            bank_id = txn.statement.bank_id if txn.statement else None
            amt = max(txn.credit_amount or Decimal("0.00"), txn.debit_amount or Decimal("0.00"))
            prediction = collaborative_service.match_collaborative(
                db=db,
                bank_id=bank_id,
                raw_description=txn.raw_description,
                direction=direction,
                description_line2=txn.description_line2,
                amount=amt,
            )
            if prediction:
                txn.classification_category = prediction["category"]
                txn.suggested_category = prediction["category"]
                txn.internal_category = prediction.get("gl_account") or prediction["category"]
                txn.classification_source = "COLLABORATIVE"
                txn.classification_confidence = prediction["confidence"]
                txn.is_classified = True
                matched += 1

        if matched > 0:
            db.commit()
        return matched

    def confirm_and_learn(
        self, db: Session, transaction_id: int, customer_id: int, user_id: int,
        counterparty_name: Optional[str] = None,
        entity_type: Optional[str] = "CUSTOMER",
        category: Optional[str] = None,
        gl_account: Optional[str] = None,
        sub_category: Optional[str] = None,
    ) -> BankTransaction:
        """
        Online Feedback & Rule-Building Loop:
        1. Updates the individual transaction and seals sensitive fields with Tenant DEK.
        2. Learns/reinforces the private counterparty in the tenant registry (with blind token).
        3. Updates `BankAccountProfile` online purity statistics.
        4. Induces or self-heals a `ClassificationRule` online based on the user's confirmation/override!
        5. Submits a zero-PII structural signature to the Collaborative Consensus Engine.
        """
        from app.crud.crud_reconciliation_v2 import crud_counterparty
        
        txn = db.query(BankTransaction).join(BankStatement).filter(
            BankTransaction.id == transaction_id,
            BankStatement.company_id == customer_id
        ).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transaction not found")

        dek = reconciliation_confidentiality.get_tenant_dek(db, customer_id)
        resolved = reconciliation_confidentiality.resolve_transaction_fields(txn, dek)
        if resolved.get("raw_description"):
            txn.raw_description = resolved["raw_description"]

        previous_category = txn.classification_category
        previous_rule_id = txn.applied_rule_id

        chosen_category = category or txn.classification_category or "CLASSIFIED"
        chosen_gl = gl_account or txn.internal_category or chosen_category

        if category:
            txn.classification_category = category
            txn.suggested_category = category
        if sub_category:
            txn.sub_category = sub_category
        if gl_account:
            txn.internal_category = gl_account
        if counterparty_name:
            txn.counterparty_name = counterparty_name.strip()
            crud_counterparty.learn_or_increment(
                db=db,
                name=counterparty_name,
                customer_id=customer_id,
                entity_type=entity_type or ("CUSTOMER" if (txn.credit_amount or 0) > 0 else "SUPPLIER"),
                default_category=chosen_category,
                default_gl=chosen_gl,
                user_id=user_id
            )

        txn.is_classified = True
        txn.auto_confirmed = True
        txn.manual_override = True
        txn.classification_source = "MANUAL"
        txn.classification_confidence = 100

        # Load known entities for privacy scrubbing
        known_entities = []
        try:
            cps = db.query(Counterparty.name).filter(
                or_(Counterparty.company_id == customer_id, Counterparty.company_id == None),
                Counterparty.is_deleted == False
            ).limit(500).all()
            known_entities = [c[0] for c in cps if c and c[0]]
        except Exception:
            pass

        # Seal updated counterparty/narrative at rest
        reconciliation_confidentiality.seal_transaction_fields(
            txn, dek=dek, known_entities=known_entities, custom_fields=txn.custom_fields_json
        )

        # Online Account Profile Purity Update
        bank_id = txn.statement.bank_id if txn.statement else None
        try:
            self._update_account_profile_online(
                db=db,
                customer_id=customer_id,
                bank_id=bank_id,
                account_number=txn.account_number,
                category=chosen_category,
                dek=dek,
            )
        except Exception as prof_err:
            logger.debug(f"Account profile update skipped: {prof_err}")

        # Online Rule Induction & Self-Healing
        try:
            induced_rule = self._induce_or_evolve_rule_online(
                db=db,
                txn=txn,
                customer_id=customer_id,
                user_id=user_id,
                chosen_category=chosen_category,
                chosen_gl=chosen_gl,
                chosen_sub_category=sub_category or txn.sub_category,
                previous_rule_id=previous_rule_id,
                previous_category=previous_category,
                known_entities=known_entities,
            )
            if induced_rule:
                txn.applied_rule_id = induced_rule.id
                txn.classification_reasoning = f"Confirmed by user; online rule '{induced_rule.rule_name}' updated ({induced_rule.lifecycle_state})."
        except Exception as rule_err:
            logger.warning(f"Online rule induction skipped: {rule_err}")

        # Submit zero-PII signature to Collaborative Consensus Engine
        try:
            from app.services.collaborative_learning_service import collaborative_service
            direction = "CREDIT" if (txn.credit_amount or 0) > 0 else "DEBIT"
            amt = max(txn.credit_amount or Decimal("0.00"), txn.debit_amount or Decimal("0.00"))
            collaborative_service.submit_confirmation(
                db=db,
                company_id=customer_id,
                bank_id=bank_id,
                raw_description=txn.raw_description,
                direction=direction,
                category=chosen_category,
                gl_account=chosen_gl,
                amount=amt,
                counterparty_name=txn.counterparty_name,
                previous_category=previous_category,
            )
        except Exception as col_err:
            logger.warning(f"Collaborative learning submission skipped: {col_err}")

        db.commit()
        db.refresh(txn)
        return txn

    def auto_discover_counterparties_from_history(self, db: Session, customer_id: int, limit: int = 500) -> List[Dict[str, Any]]:
        txns = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_deleted == False
        ).order_by(BankTransaction.id.desc()).limit(limit).all()

        STOP_WORDS = {
            "TRANSFER", "PAYMENT", "BANK", "COMMISSION", "BRANCH", "SWIFT", "DEBIT", 
            "CREDIT", "CHARGE", "FEES", "ONLINE", "ATM", "POS", "REF", "TRF", "CHQ", 
            "CHECK", "TO", "FROM", "FOR", "AND", "THE", "INVOICE", "INV", "SALARY", 
            "TAX", "WITHHOLDING", "EGP", "USD", "EUR", "GBP", "SAR", "AED",
            "تحويل", "سداد", "بنك", "عمولة", "فرع", "شيك", "حساب", "فاتورة", "دفع", "مبلغ", "مصاريف"
        }

        token_directions = {}
        for t in txns:
            raw = (t.raw_description or "").upper()
            words = [w for w in re.split(r'[^a-zA-Z\u0600-\u06FF]+', raw) if len(w) >= 3 and not w.isdigit()]
            is_credit = (t.credit_amount or 0) > 0

            for word in words:
                if word in STOP_WORDS:
                    continue
                if word not in token_directions:
                    token_directions[word] = {"credits": 0, "debits": 0, "samples": []}
                if is_credit:
                    token_directions[word]["credits"] += 1
                else:
                    token_directions[word]["debits"] += 1
                if len(token_directions[word]["samples"]) < 2 and raw not in token_directions[word]["samples"]:
                    token_directions[word]["samples"].append(raw)

        suggestions = []
        for word, stats in token_directions.items():
            total = stats["credits"] + stats["debits"]
            if total >= 2:
                predominant_type = "CUSTOMER" if stats["credits"] >= stats["debits"] else "SUPPLIER"
                suggestions.append({
                    "suggested_name": word,
                    "entity_type": predominant_type,
                    "frequency": total,
                    "credit_count": stats["credits"],
                    "debit_count": stats["debits"],
                    "default_category": "COLLECTION_FROM_CUSTOMER" if predominant_type == "CUSTOMER" else "PAYMENT_TO_SUPPLIER",
                    "default_gl_account": "ACCOUNTS_RECEIVABLE" if predominant_type == "CUSTOMER" else "ACCOUNTS_PAYABLE",
                    "sample_descriptions": stats["samples"]
                })

        return sorted(suggestions, key=lambda s: s["frequency"], reverse=True)[:50]

    def _run_builtin_classifiers(self, txn_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Runs universal built-in classifiers and international/regional banking standards
        (SWIFT MT940 codes, ISO 20022 codes, and MENA Treasury rails) against a parsed transaction dict.
        Includes Accounting Sanity Guardrails so huge commercial wires with the word
        "fee" or "commission" are not blindly classified as small BANK_CHARGES.
        """
        desc = (txn_data.get("raw_description") or "").lower()
        desc2 = (txn_data.get("description_line2") or "").lower()
        combined_desc = f"{desc} {desc2}"
        
        debit = Decimal(str(txn_data.get("debit_amount") or "0.00"))
        credit = Decimal(str(txn_data.get("credit_amount") or "0.00"))
        direction = "CREDIT" if credit > 0 else "DEBIT"
        amt = max(debit, credit)

        # 0. Check SWIFT MT940, ISO 20022 (CAMT.053), and Corporate Treasury Standards Ontology first
        std_decoded = banking_standards_ontology.decode_narrative_standards(
            txn_data.get("raw_description"),
            txn_data.get("description_line2"),
            direction=direction,
            amount=amt,
        )
        if std_decoded.get("classification"):
            cls_hit = std_decoded["classification"]
            if cls_hit["category"] == "BANK_CHARGES" and debit > Decimal("100000.00") and not txn_data.get("_has_companion_parent"):
                pass
            else:
                return {
                    "category": cls_hit["category"],
                    "sub_category": cls_hit.get("sub_category"),
                    "confidence": cls_hit["confidence"],
                    "classifier_name": cls_hit["source"],
                    "reasoning": cls_hit["reason"],
                }
        
        for classifier in self.BUILTIN_CLASSIFIERS:
            if classifier["direction"] == "CREDIT" and credit <= 0:
                continue
            if classifier["direction"] == "DEBIT" and debit <= 0:
                continue
            
            matched_kw = None
            for kw in classifier["keywords"]:
                if kw in combined_desc:
                    matched_kw = kw
                    break
            
            if matched_kw:
                # Accounting Sanity Guardrail: Do not classify debits > 100,000 as BANK_CHARGES unless explicitly companion-linked
                if classifier["category"] == "BANK_CHARGES" and debit > Decimal("100000.00") and not txn_data.get("_has_companion_parent"):
                    continue
                return {
                    "category": classifier["category"],
                    "sub_category": classifier.get("sub_category"),
                    "confidence": classifier["confidence"],
                    "classifier_name": classifier["name"],
                    "reasoning": f"Matched built-in banking pattern '{matched_kw}' ({classifier['name']})"
                }
        
        return None

    def _detect_logical_relationships_sync(self, db: Session, customer_id: int) -> Dict[str, Any]:
        """
        Detects reversals, intra-bank transfers, and CROSS-BANK zero-sum sweeps
        across all statements for the customer, including IBAN bank-code verification.
        """
        txns = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_reconciled == False,
            BankTransaction.linked_txn_id == None,
            BankTransaction.is_deleted == False
        ).all()
        
        if not txns:
            return {"affected_count": 0, "status": "No candidate transactions found"}

        counts = {"reversals": 0, "transfers": 0, "sweeps": 0}
        processed_ids = set()

        rev_keywords = [
            "REVERSE", "REVERSAL", "ADJ", "ADJUSTMENT", "ERR", "ERROR", "OFFSET", "CORR", "CORRECTION",
            "RETURNED", "BOUNCED", "UNPAID", "DISHONORED", "REJECTED", "CANCELLED",
            "مرتد", "عكس", "ارتداد", "مرفوض", "بدون رصيد"
        ]
        sweep_keywords_upper = [kw.upper() for kw in self.SWEEP_KEYWORDS] + ["SWEEP", "سويب", "تحويل ذاتي", "CASH POOL"]

        def extract_sweep_tokens(desc: Optional[str]) -> set:
            if not desc:
                return set()
            tokens = set(re.findall(r'\b[A-Z0-9_]{8,}\b', desc.upper()))
            return {t for t in tokens if any(c.isdigit() for c in t) and not t.isdigit()}

        # ── Pass 1: Direct Sweep Reference / Token Matching (Strictly Cross-Account / Cross-Statement) ──
        for i, t1 in enumerate(txns):
            if t1.id in processed_ids:
                continue

            t1_desc_upper = (t1.raw_description or "").upper()
            if any(rk in t1_desc_upper for rk in rev_keywords):
                continue
            has_sweep_kw_1 = any(kw in t1_desc_upper for kw in sweep_keywords_upper)
            if not has_sweep_kw_1:
                continue

            t1_is_credit = (t1.credit_amount or 0) > 0
            t1_tokens = extract_sweep_tokens(t1.raw_description)
            if t1.e2e_id:
                t1_tokens.add(str(t1.e2e_id).upper())
            if t1.back_office_ref:
                t1_tokens.add(str(t1.back_office_ref).upper())

            if not t1_tokens:
                continue

            for j in range(i + 1, len(txns)):
                t2 = txns[j]
                if t2.id in processed_ids:
                    continue

                # Never classify two legs inside the exact same statement/account as an inter-bank sweep
                same_stmt_or_acct = (t1.statement_id == t2.statement_id) or bool(
                    t1.account_number and t2.account_number and t1.account_number == t2.account_number
                )
                if same_stmt_or_acct:
                    continue

                t2_is_credit = (t2.credit_amount or 0) > 0
                if t1_is_credit == t2_is_credit:
                    continue

                t2_desc_upper = (t2.raw_description or "").upper()
                if any(rk in t2_desc_upper for rk in rev_keywords):
                    continue
                has_sweep_kw_2 = any(kw in t2_desc_upper for kw in sweep_keywords_upper)
                if not has_sweep_kw_2:
                    continue

                t2_tokens = extract_sweep_tokens(t2.raw_description)
                if t2.e2e_id:
                    t2_tokens.add(str(t2.e2e_id).upper())
                if t2.back_office_ref:
                    t2_tokens.add(str(t2.back_office_ref).upper())

                common_tokens = t1_tokens.intersection(t2_tokens)
                if common_tokens:
                    t1.linked_txn_id = t2.id
                    t2.linked_txn_id = t1.id

                    t1_val = (t1.credit_amount or 0) if t1_is_credit else (t1.debit_amount or 0)
                    t2_val = (t2.credit_amount or 0) if t2_is_credit else (t2.debit_amount or 0)
                    fee_variance = abs(t1_val - t2_val)
                    if fee_variance > 0:
                        t1.variance_amount = fee_variance
                        t2.variance_amount = fee_variance

                    t1.internal_category = "INTER_BANK_SWEEP"
                    t2.internal_category = "INTER_BANK_SWEEP"
                    t1.classification_category = "INTER_BANK_SWEEP"
                    t2.classification_category = "INTER_BANK_SWEEP"
                    t1.classification_source = "BUILTIN"
                    t2.classification_source = "BUILTIN"
                    t1.classification_confidence = 96
                    t2.classification_confidence = 96
                    t1.auto_confirmed = True
                    t2.auto_confirmed = True
                    t1.is_classified = True
                    t2.is_classified = True

                    processed_ids.add(t1.id)
                    processed_ids.add(t2.id)
                    counts["sweeps"] += 2
                    break

        # ── Pass 2: Two-Legged Zero-Sum Clearing (-1 to +5 days window) & Reversals ──
        for i, t1 in enumerate(txns):
            if t1.id in processed_ids:
                continue

            t1_is_credit = (t1.credit_amount or 0) > 0
            t1_val = (t1.credit_amount or 0) if t1_is_credit else (t1.debit_amount or 0)
            if t1_val == 0:
                continue

            for j in range(i + 1, len(txns)):
                t2 = txns[j]
                if t2.id in processed_ids:
                    continue

                t2_is_credit = (t2.credit_amount or 0) > 0
                if t1_is_credit == t2_is_credit:
                    continue

                t2_val = (t2.credit_amount or 0) if t2_is_credit else (t2.debit_amount or 0)
                if t2_val == 0:
                    continue

                amt_diff = abs(t1_val - t2_val)
                is_same_account = (t1.statement_id == t2.statement_id) or bool(
                    t1.account_number and t2.account_number and t1.account_number == t2.account_number
                )
                date_diff = self._effective_date_distance_days(
                    t1.booking_date, getattr(t1, "value_date", None),
                    t2.booking_date, getattr(t2, "value_date", None),
                )

                # Reversals: Same Statement / Account, <= 7 days, equal amount
                if is_same_account and date_diff <= 7 and amt_diff == 0:
                    desc1 = (t1.raw_description or "").upper()
                    desc2 = (t2.raw_description or "").upper()

                    has_rev_keyword = any(kw in desc1 or kw in desc2 for kw in rev_keywords)
                    names_match = bool(desc1.split()[:3] and desc1.split()[:3] == desc2.split()[:3])
                    refs_match = bool(t1.e2e_id and t2.e2e_id and t1.e2e_id == t2.e2e_id)
                    shared_num_refs = bool(
                        set(re.findall(r"\b\d{5,}\b", desc1)).intersection(
                            set(re.findall(r"\b\d{5,}\b", desc2))
                        )
                    )

                    if has_rev_keyword or names_match or refs_match or shared_num_refs:
                        t1.linked_txn_id = t2.id
                        t2.linked_txn_id = t1.id
                        t1.is_reversal = True
                        t2.is_reversal = True
                        t1.is_reconciled = True
                        t2.is_reconciled = True
                        t1.classification_source = "BUILTIN"
                        t2.classification_source = "BUILTIN"
                        t1.classification_confidence = 94
                        t2.classification_confidence = 94
                        t1.auto_confirmed = True
                        t2.auto_confirmed = True

                        processed_ids.add(t1.id)
                        processed_ids.add(t2.id)
                        counts["reversals"] += 2
                        break

                # Sweeps / Transfers: Different Accounts, <= 5 days clearing lag, within fee tolerance
                is_within_fee_tolerance = (amt_diff == 0) or (amt_diff <= 150) or (max(t1_val, t2_val) > 0 and (amt_diff / max(t1_val, t2_val)) <= 0.03)

                if not is_same_account and date_diff <= 5 and is_within_fee_tolerance:
                    desc1_upper = (t1.raw_description or "").upper()
                    desc2_upper = (t2.raw_description or "").upper()
                    if any(rk in desc1_upper or rk in desc2_upper for rk in rev_keywords):
                        continue

                    refs_match = t1.e2e_id and t2.e2e_id and t1.e2e_id == t2.e2e_id
                    names_match = desc1_upper.split()[:3] == desc2_upper.split()[:3]
                    has_sweep_keyword = any(kw in desc1_upper or kw in desc2_upper for kw in sweep_keywords_upper)
                    has_iban_link = bool(t1.iban_bank_code or t2.iban_bank_code)

                    is_cross_bank = False
                    try:
                        is_cross_bank = t1.statement.bank_id != t2.statement.bank_id
                    except Exception:
                        pass

                    if refs_match or names_match or has_sweep_keyword or has_iban_link:
                        t1.linked_txn_id = t2.id
                        t2.linked_txn_id = t1.id

                        if amt_diff > 0:
                            t1.variance_amount = amt_diff
                            t2.variance_amount = amt_diff

                        category = "INTER_BANK_SWEEP" if (is_cross_bank or has_sweep_keyword) else "INTERNAL_TRANSFER"
                        if category == "INTER_BANK_SWEEP":
                            counts["sweeps"] += 2
                        else:
                            counts["transfers"] += 2

                        conf = 92 if (refs_match or has_iban_link) else 85
                        for leg in (t1, t2):
                            leg.internal_category = category
                            leg.classification_category = category
                            leg.classification_source = "BUILTIN"
                            leg.classification_confidence = conf
                            leg.auto_confirmed = True
                            leg.is_classified = True
                            leg.classification_reasoning = f"Paired two-legged zero-sum {category} (Delta: {amt_diff})"

                        processed_ids.add(t1.id)
                        processed_ids.add(t2.id)
                        break

        db.commit()
        total_affected = counts["sweeps"] + counts["reversals"] + counts["transfers"]
        return {
            "affected_count": total_affected,
            "reversals_count": counts["reversals"],
            "transfers_count": counts["transfers"],
            "sweeps_count": counts["sweeps"],
            "status": f"Found {counts['sweeps'] // 2} sweep pairs ({counts['sweeps']} transactions), {counts['reversals'] // 2} reversal pairs."
        }

    async def detect_logical_relationships(self, db: Session, customer_id: int) -> Dict[str, Any]:
        return self._detect_logical_relationships_sync(db, customer_id)

    # ═══════════════════════════════════════════════════════════════════
    #  SCHEMA-AGNOSTIC ERP INGESTION (Preserves Custom Columns & Blind Tokens)
    # ═══════════════════════════════════════════════════════════════════

    def process_erp_file_ingestion(self, db: Session, file_content: bytes, file_type: str, company_id: int, user_id: int) -> Dict[str, Any]:
        ft_erp = (file_type or "").lower().strip().lstrip(".")
        if ft_erp not in ("csv", "txt", "xlsx", "xls"):
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported ERP file format '.{ft_erp}'. Please upload a .csv or .xlsx ERP ledger/invoices file.",
            )
        if not file_content or file_content.lstrip().startswith(b"%PDF"):
            raise HTTPException(
                status_code=400,
                detail="Invalid ERP file: PDF documents cannot be uploaded as ERP Open Invoices / Payables. Please upload a .csv or .xlsx file.",
            )
        try:
            if ft_erp in ("csv", "txt"):
                def _read_erp_csv(raw_b: bytes, enc: str):
                    head_s = raw_b[:4096].decode(enc, errors="ignore")
                    first_ln = next((ln for ln in head_s.splitlines() if ln.strip()), "")
                    sep = ","
                    if first_ln:
                        c_semi = first_ln.count(";")
                        c_comma = first_ln.count(",")
                        c_tab = first_ln.count("\t")
                        if c_semi > c_comma and c_semi >= c_tab:
                            sep = ";"
                        elif c_tab > c_comma and c_tab > c_semi:
                            sep = "\t"
                    return pd.read_csv(io.BytesIO(raw_b), encoding=enc, sep=sep)

                try:
                    df = _read_erp_csv(file_content, "utf-8")
                except UnicodeDecodeError:
                    df = _read_erp_csv(file_content, "cp1256")
            else:
                df = pd.read_excel(io.BytesIO(file_content))
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to read ERP file: {str(e)}")

        if df.empty:
            raise HTTPException(status_code=400, detail="Uploaded ERP file is empty.")

        raw_erp_headers = [str(c).strip().lower() for c in df.columns if pd.notna(c) and not str(c).startswith("Unnamed")]
        erp_hr_hits = sum(
            1
            for kw in ('employee id', 'emp id', 'hire date', 'monthly salary', 'basic salary', 'net salary', 'job title', 'payroll', 'department', 'رقم الموظف', 'تاريخ التعيين', 'الراتب')
            if any(kw in h for h in raw_erp_headers)
        )
        if erp_hr_hits >= 2:
            raise HTTPException(
                status_code=400,
                detail="Wrong document type: This file appears to be an HR / Employee spreadsheet rather than ERP Open Invoices / Payables.",
            )
        if (
            any(any(k in h for k in ('running balance', 'closing balance', 'opening balance', 'رصيد')) for h in raw_erp_headers)
            and any(any(k in h for k in ('booking', 'value date', 'narration', 'description', 'بيان')) for h in raw_erp_headers)
            and not any(any(k in h for k in ('invoice', 'bill', 'voucher', 'vendor', 'customer', 'gl', 'فاتورة')) for h in raw_erp_headers)
        ):
            raise HTTPException(
                status_code=400,
                detail="Wrong upload box: This file appears to be a Bank Cash Statement rather than ERP Open Invoices / Payables. Please upload it under 'Bank Statements' instead.",
            )
        if (
            any(any(k in h for k in ('lg number', 'lg_number', 'lg no', 'guarantee no', 'guarantee number', 'رقم الخطاب')) for h in raw_erp_headers)
            and any(any(k in h for k in ('expiry', 'beneficiary', 'تاريخ الانتهاء', 'المستفيد')) for h in raw_erp_headers)
        ):
            raise HTTPException(
                status_code=400,
                detail="Wrong upload box: This file appears to be a Bank LG Position list rather than ERP Open Invoices / Payables.",
            )

        col_map = {}
        for c in df.columns:
            c_clean = str(c).strip().lower()
            if not col_map.get("date") and any(k in c_clean for k in ['date', 'posting date', 'invoice date', 'bill date', 'doc date', 'تاريخ']):
                col_map["date"] = c
            elif not col_map.get("ref") and any(k in c_clean for k in ['reference', 'ref', 'invoice', 'bill', 'doc #', 'document', 'voucher', 'مرجع', 'فاتورة', 'مستند']):
                col_map["ref"] = c
            elif not col_map.get("entity") and any(k in c_clean for k in ['customer', 'vendor', 'supplier', 'party', 'entity', 'name', 'beneficiary', 'عميل', 'مورد', 'الاسم', 'الطرف']):
                col_map["entity"] = c
            elif not col_map.get("amount") and any(k in c_clean for k in ['amount', 'total', 'net', 'value', 'مبلغ', 'قيمة', 'إجمالي', 'صافي']):
                col_map["amount"] = c
            elif not col_map.get("debit") and any(k in c_clean for k in ['debit', 'dr', 'مدين']):
                col_map["debit"] = c
            elif not col_map.get("credit") and any(k in c_clean for k in ['credit', 'cr', 'دائن']):
                col_map["credit"] = c
            elif not col_map.get("type") and any(k in c_clean for k in ['type', 'record type', 'doc type', 'category', 'نوع']):
                col_map["type"] = c
            elif not col_map.get("gl") and any(k in c_clean for k in ['gl', 'account', 'ledger', 'حساب']):
                col_map["gl"] = c
            elif not col_map.get("currency") and any(k in c_clean for k in ['currency', 'ccy', 'عملة']):
                col_map["currency"] = c

        # Value-distribution fallback ONLY for headerless tables or when an explicit ERP column (ref/gl/type) was already matched
        is_unnamed_erp = all(
            isinstance(c, (int, float)) or str(c).startswith("Unnamed")
            for c in df.columns
            if pd.notna(c) and str(c).strip() != ""
        )
        has_erp_anchor = bool(col_map.get("ref") or col_map.get("gl") or col_map.get("type"))
        if (is_unnamed_erp or has_erp_anchor) and (
            not col_map.get("ref") or (not col_map.get("amount") and not (col_map.get("debit") and col_map.get("credit")))
        ):
            for c in df.columns:
                if c in col_map.values():
                    continue
                sample_vals = df[c].dropna().astype(str).head(25)
                if sample_vals.empty:
                    continue
                num_hits = sum(1 for v in sample_vals if re.match(r'^[-+]?[\d,.]+(\.\d+)?$', v.strip()))
                if num_hits >= len(sample_vals) * 0.7 and not col_map.get("amount"):
                    col_map["amount"] = c
                elif not col_map.get("ref"):
                    col_map["ref"] = c

        if not col_map.get("ref") or (not col_map.get("amount") and not (col_map.get("debit") and col_map.get("credit"))):
            raise HTTPException(
                status_code=400,
                detail=f"Could not detect required ERP Reference/Invoice and Amount columns. Detected: {list(col_map.keys())}"
            )

        mapped_erp_cols = set(col_map.values())
        unmapped_erp_cols = [c for c in df.columns if c not in mapped_erp_cols and not str(c).startswith("Unnamed")]
        dek = reconciliation_confidentiality.get_tenant_dek(db, company_id)

        imported_count = 0
        errors = []

        for idx, row in df.iterrows():
            ref_val = row.get(col_map.get("ref"))
            if pd.isna(ref_val) or not str(ref_val).strip():
                continue

            ref_str = self._sanitize_text(ref_val)

            if col_map.get("amount"):
                amt = self._clean_decimal(row.get(col_map.get("amount")))
            else:
                deb = self._clean_decimal(row.get(col_map.get("debit")))
                cred = self._clean_decimal(row.get(col_map.get("credit")))
                amt = cred - deb

            if amt == Decimal("0.00"):
                continue

            date_val = row.get(col_map.get("date")) if col_map.get("date") else None
            rec_date = self._resolve_transaction_date(date_val) if date_val is not None else datetime.utcnow()
            if not rec_date:
                rec_date = datetime.utcnow()

            entity_val = row.get(col_map.get("entity")) if col_map.get("entity") else "General Entity"
            entity_name = self._sanitize_text(entity_val) or "General Entity"
            blind_ent = reconciliation_confidentiality.compute_blind_token(entity_name, dek, prefix="ENT_TOK")

            raw_type = str(row.get(col_map.get("type"), "")).upper() if col_map.get("type") else ""
            if "AR" in raw_type or "INVOICE" in raw_type or "RECEIVABLE" in raw_type:
                record_type = "AR_INVOICE"
            elif "AP" in raw_type or "BILL" in raw_type or "PAYABLE" in raw_type:
                record_type = "AP_BILL"
            elif "PAYROLL" in raw_type or "SALARY" in raw_type:
                record_type = "PAYROLL_BATCH"
            elif "LG" in raw_type or "GUARANTEE" in raw_type:
                record_type = "LG_COMMISSION"
            else:
                record_type = "AR_INVOICE" if amt > 0 else "AP_BILL"

            gl_acct = self._sanitize_text(row.get(col_map.get("gl"))) if col_map.get("gl") and pd.notna(row.get(col_map.get("gl"))) else None
            ccy = self._sanitize_text(row.get(col_map.get("currency"), "EGP")).upper()[:3] if col_map.get("currency") else "EGP"

            custom_fields = {}
            for u_col in unmapped_erp_cols:
                u_val = row.get(u_col)
                if pd.notna(u_val) and str(u_val).strip() not in ("", "nan"):
                    custom_fields[str(u_col).strip()] = self._sanitize_text(u_val)

            existing = db.query(InternalLedgerRecord).filter(
                InternalLedgerRecord.company_id == company_id,
                InternalLedgerRecord.reference_number == ref_str,
                InternalLedgerRecord.currency == (ccy or "EGP"),
                InternalLedgerRecord.is_deleted == False
            ).first()

            if existing:
                existing.amount = amt
                existing.record_date = rec_date
                existing.entity_name = entity_name
                existing.blind_entity_token = blind_ent
                existing.record_type = record_type
                existing.currency = ccy or "EGP"
                if custom_fields:
                    existing.custom_fields_json = custom_fields
                if gl_acct:
                    existing.gl_account = gl_acct
                existing.updated_by = user_id
            else:
                new_rec = InternalLedgerRecord(
                    company_id=company_id,
                    record_type=record_type,
                    reference_number=ref_str,
                    entity_name=entity_name,
                    blind_entity_token=blind_ent,
                    record_date=rec_date,
                    amount=amt,
                    currency=ccy or "EGP",
                    gl_account=gl_acct,
                    custom_fields_json=custom_fields if custom_fields else None,
                    status="OPEN",
                    created_by=user_id
                )
                db.add(new_rec)

            imported_count += 1

        if imported_count == 0:
            raise HTTPException(
                status_code=400,
                detail="No valid ERP invoice or payable rows with non-zero amounts were found in the uploaded file.",
            )

        db.commit()
        _reclaim_system_memory()
        return {
            "imported_count": imported_count,
            "total_rows_processed": len(df),
            "errors": errors,
            "status": f"Successfully ingested {imported_count} ERP records into ledger."
        }

    def ingest_erp_records_bulk(self, db: Session, records_data: List[Dict[str, Any]], company_id: int, user_id: int) -> Dict[str, Any]:
        imported_count = 0
        dek = reconciliation_confidentiality.get_tenant_dek(db, company_id)
        for item in records_data:
            ref_str = str(item.get("reference_number", "")).strip()
            if not ref_str:
                continue

            amt = Decimal(str(item.get("amount", 0)))
            rec_type = item.get("record_type", "AR_INVOICE" if amt > 0 else "AP_BILL")
            entity_name = item.get("entity_name", "General Entity")
            blind_ent = reconciliation_confidentiality.compute_blind_token(entity_name, dek, prefix="ENT_TOK")
            raw_date = item.get("record_date")
            rec_date = pd.to_datetime(raw_date).to_pydatetime() if raw_date else datetime.utcnow()
            bulk_ccy = str(item.get("currency") or "EGP").strip().upper()[:3] or "EGP"

            existing = db.query(InternalLedgerRecord).filter(
                InternalLedgerRecord.company_id == company_id,
                InternalLedgerRecord.reference_number == ref_str,
                InternalLedgerRecord.currency == bulk_ccy,
                InternalLedgerRecord.is_deleted == False
            ).first()

            if existing:
                existing.amount = amt
                existing.record_date = rec_date
                existing.entity_name = entity_name
                existing.blind_entity_token = blind_ent
                existing.record_type = rec_type
                existing.currency = bulk_ccy
                if item.get("gl_account"):
                    existing.gl_account = item.get("gl_account")
                existing.updated_by = user_id
            else:
                new_rec = InternalLedgerRecord(
                    company_id=company_id,
                    record_type=rec_type,
                    reference_number=ref_str,
                    entity_name=entity_name,
                    blind_entity_token=blind_ent,
                    record_date=rec_date,
                    amount=amt,
                    currency=item.get("currency", "EGP"),
                    gl_account=item.get("gl_account"),
                    status="OPEN",
                    created_by=user_id
                )
                db.add(new_rec)
            imported_count += 1

        if imported_count == 0:
            raise HTTPException(
                status_code=400,
                detail="No valid ERP invoice or payable rows with non-zero amounts were found in the uploaded file.",
            )

        db.commit()
        _reclaim_system_memory()
        return {
            "imported_count": imported_count,
            "total_rows_processed": len(records_data),
            "errors": [],
            "status": f"Successfully ingested {imported_count} ERP records."
        }

    # ═══════════════════════════════════════════════════════════════════
    #  USER-CUSTOMIZABLE EXPORT ENGINE (Any Format / Column Order / Journal Legs)
    # ═══════════════════════════════════════════════════════════════════

    DEFAULT_EXPORT_COLUMNS = [
        {"source_field": "booking_date", "custom_header": "Booking Date"},
        {"source_field": "value_date", "custom_header": "Value Date"},
        {"source_field": "account_number", "custom_header": "Account Number"},
        {"source_field": "raw_description", "custom_header": "Description"},
        {"source_field": "counterparty_name", "custom_header": "Counterparty"},
        {"source_field": "debit_amount", "custom_header": "Debit"},
        {"source_field": "credit_amount", "custom_header": "Credit"},
        {"source_field": "currency", "custom_header": "Currency"},
        {"source_field": "running_balance", "custom_header": "Balance"},
        {"source_field": "classification_category", "custom_header": "Category"},
        {"source_field": "internal_category", "custom_header": "GL Account"},
        {"source_field": "classification_confidence", "custom_header": "Confidence %"},
        {"source_field": "is_reconciled", "custom_header": "Reconciled"},
        {"source_field": "matched_erp_reference", "custom_header": "Matched ERP Ref"},
        {"source_field": "variance_amount", "custom_header": "Fee / FX Variance"},
    ]

    def _resolve_variance_gl_account(
        self,
        disposition: Optional[str],
        wht_gl: str = "1090300 - Withholding Tax Receivable",
        fx_gl: str = "6060100 - FX Gain / Loss",
        fee_gl: str = "5040100 - Bank Charges",
    ) -> Tuple[str, str]:
        disp = (disposition or "BANK_FEE_WRITEOFF").strip().upper()
        if disp in ("WHT", "WHT_DEDUCTION", "WITHHOLDING_TAX", "TAX_DEDUCTION"):
            return wht_gl, "TAX_DEDUCTION"
        if disp in ("FX", "FX_DIFFERENCE", "FX_VARIANCE"):
            return fx_gl, "FX_DIFFERENCE"
        if disp == "ROUNDING":
            return "6060900 - Rounding Differences", "ROUNDING"
        return fee_gl, "BANK_CHARGES"

    def _load_account_gl_lookup(
        self,
        db: Session,
        customer_id: int,
        profile_mappings: Optional[Dict[str, Any]] = None,
        override_mappings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, str]]:
        """
        Builds a normalized lookup table mapping account numbers (and bank_<id> fallbacks)
        to {"main_gl": ..., "clearing_gl": ...}.
        Priority: Request override_mappings > Profile account_gl_mappings_json > CustomerBankAccount DB rows.
        """
        lookup: Dict[str, Dict[str, str]] = {}

        # 1. Load CustomerBankAccount rows if table is available (wrapped safely for isolated SQLite unit tests)
        try:
            from app.models.models_issuance import CustomerBankAccount
            cbas = db.query(CustomerBankAccount).filter(
                CustomerBankAccount.customer_id == customer_id,
                CustomerBankAccount.is_deleted == False,
            ).all()
            for a in cbas:
                entry = {}
                if getattr(a, "erp_main_gl_account", None):
                    entry["main_gl"] = a.erp_main_gl_account.strip()
                if getattr(a, "erp_clearing_gl_account", None):
                    entry["clearing_gl"] = a.erp_clearing_gl_account.strip()
                if entry:
                    if a.account_number:
                        lookup[a.account_number.strip().upper()] = dict(entry)
                    if a.iban:
                        lookup[a.iban.strip().upper()] = dict(entry)
                    if a.bank_id and a.is_default:
                        lookup.setdefault(f"BANK_{a.bank_id}", dict(entry))
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass

        # 2. Merge saved profile mappings + ad-hoc request mappings
        for src_map in (profile_mappings, override_mappings):
            if isinstance(src_map, dict):
                for raw_k, raw_v in src_map.items():
                    if not raw_k or not isinstance(raw_v, dict):
                        continue
                    norm_k = str(raw_k).strip().upper()
                    cur = lookup.setdefault(norm_k, {})
                    if raw_v.get("main_gl") and str(raw_v["main_gl"]).strip():
                        cur["main_gl"] = str(raw_v["main_gl"]).strip()
                    if raw_v.get("clearing_gl") and str(raw_v["clearing_gl"]).strip():
                        cur["clearing_gl"] = str(raw_v["clearing_gl"]).strip()

        return lookup

    def _resolve_effective_bank_gl(
        self,
        t: BankTransaction,
        account_gl_lookup: Dict[str, Dict[str, str]],
        offset_account_mode: str,
        default_main_gl: str,
        default_clearing_gl: str,
    ) -> str:
        acct_key = (t.account_number or (t.statement.account_number if t.statement else "") or "").strip().upper()
        bank_key = f"BANK_{t.statement.bank_id}" if t.statement and t.statement.bank_id else ""
        mapped = account_gl_lookup.get(acct_key) or account_gl_lookup.get(bank_key) or {}
        if offset_account_mode == "BANK_CLEARING_SUSPENSE":
            return mapped.get("clearing_gl") or default_clearing_gl
        return mapped.get("main_gl") or default_main_gl

    def build_custom_export_rows(
        self,
        db: Session,
        customer_id: int,
        req: Any,
    ) -> Tuple[List[str], List[Dict[str, Any]], List[BankTransaction], str]:
        """
        Builds customizable export rows according to either a saved `ReconciliationExportProfile`
        or ad-hoc user parameters (columns, custom headers, column order, date format, amount mode,
        handoff_mode, offset_account_mode, per-account GL resolution, and double-entry GL journal expansion).
        Returns: (ordered_headers, rows_as_dicts, exported_txn_objects, file_format)
        """
        profile = None
        if getattr(req, "profile_id", None):
            profile = db.query(ReconciliationExportProfile).filter(
                ReconciliationExportProfile.id == req.profile_id,
                ReconciliationExportProfile.company_id == customer_id,
                ReconciliationExportProfile.is_deleted == False
            ).first()

        columns_config = (
            getattr(req, "columns_config", None)
            or (profile.columns_config_json if profile else None)
            or self.DEFAULT_EXPORT_COLUMNS
        )
        file_format = (
            getattr(req, "file_format", None)
            or (profile.file_format if profile else "CSV")
        ).upper()
        date_fmt = (
            getattr(req, "date_format", None)
            or (profile.date_format if profile else "%Y-%m-%d")
        )
        amount_mode = (
            getattr(req, "amount_mode", None)
            or (profile.amount_mode if profile else "SPLIT_DEBIT_CREDIT")
        )
        expand_legs = (
            getattr(req, "expand_journal_legs", None)
            if getattr(req, "expand_journal_legs", None) is not None
            else (profile.expand_journal_legs if profile else False)
        )
        bank_gl = (
            getattr(req, "bank_gl_account", None)
            or (profile.bank_gl_account if profile else None)
            or "1010010 - Cash at Bank"
        )
        handoff_mode = (
            getattr(req, "handoff_mode", None)
            or (getattr(profile, "handoff_mode", None) if profile else None)
            or "ALL"
        ).strip().upper()
        offset_account_mode = (
            getattr(req, "offset_account_mode", None)
            or (getattr(profile, "offset_account_mode", None) if profile else None)
            or "MAIN_BANK_GL"
        ).strip().upper()
        clearing_suspense_gl = (
            getattr(req, "clearing_suspense_gl_account", None)
            or (getattr(profile, "clearing_suspense_gl_account", None) if profile else None)
            or "1010999 - Bank Clearing Suspense"
        )
        transit_gl = (
            getattr(req, "cash_in_transit_gl_account", None)
            or (getattr(profile, "cash_in_transit_gl_account", None) if profile else None)
            or "1010800 - Cash in Transit"
        )
        suspense_gl = (
            getattr(req, "suspense_gl_account", None)
            or (getattr(profile, "suspense_gl_account", None) if profile else None)
            or "1010950 - Unapplied Cash Suspense"
        )
        wht_gl = (
            getattr(req, "wht_tax_gl_account", None)
            or (getattr(profile, "wht_tax_gl_account", None) if profile else None)
            or "1090300 - Withholding Tax Receivable"
        )
        fx_gl = (
            getattr(req, "fx_variance_gl_account", None)
            or (getattr(profile, "fx_variance_gl_account", None) if profile else None)
            or "6060100 - FX Gain / Loss"
        )
        split_variance_row = (
            getattr(req, "split_variance_as_fee_row", None)
            if getattr(req, "split_variance_as_fee_row", None) is not None
            else (bool(getattr(profile, "split_variance_as_fee_row", False)) if profile else False)
        )
        if getattr(req, "exclude_paired_reversals", None) is not None:
            exclude_reversals = bool(req.exclude_paired_reversals)
        elif profile is not None and getattr(profile, "exclude_paired_reversals", None) is not None and handoff_mode != "ALL":
            exclude_reversals = bool(profile.exclude_paired_reversals)
        else:
            exclude_reversals = handoff_mode in ("ENRICHED_STATEMENT", "MISSING_GL_JOURNAL")

        account_gl_lookup = self._load_account_gl_lookup(
            db=db,
            customer_id=customer_id,
            profile_mappings=getattr(profile, "account_gl_mappings_json", None) if profile else None,
            override_mappings=getattr(req, "account_gl_mappings", None),
        )

        # Query transactions
        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_deleted == False
        )
        if getattr(req, "statement_id", None):
            query = query.filter(BankTransaction.statement_id == req.statement_id)
        if getattr(req, "bank_id", None):
            query = query.filter(BankStatement.bank_id == req.bank_id)
        if getattr(req, "only_reconciled", None) is not None:
            query = query.filter(BankTransaction.is_reconciled == req.only_reconciled)
        if getattr(req, "only_classified", None) is not None:
            query = query.filter(BankTransaction.is_classified == req.only_classified)
        if getattr(req, "exclude_already_exported", False):
            query = query.filter(or_(BankTransaction.is_exported == False, BankTransaction.is_exported == None))

        all_txns = query.order_by(BankTransaction.booking_date.asc(), BankTransaction.id.asc()).limit(5000).all()
        all_txns = crud_bank_transaction._resolve_confidential_fields(db, all_txns, customer_id=customer_id)

        # Preload ERP match references & linked sweep/reversal counterparts
        txn_ids = [t.id for t in all_txns]
        erp_ref_by_txn: Dict[int, str] = {}
        erp_gl_by_txn: Dict[int, str] = {}
        erp_matches_by_txn: Dict[int, List[Tuple[ReconciliationMatch, InternalLedgerRecord]]] = {}
        bank_count_per_erp: Dict[int, int] = {}
        primary_bank_for_erp: Dict[int, int] = {}

        if txn_ids:
            matches = db.query(ReconciliationMatch).filter(
                ReconciliationMatch.bank_txn_id.in_(txn_ids),
                ReconciliationMatch.is_deleted == False,
            ).all()
            erp_ids = [
                m.source_record_id
                for m in matches
                if m.source_record_id and (m.source_type or "").upper() != "TREASURY (LG)"
            ]
            erp_map: Dict[int, InternalLedgerRecord] = {}
            if erp_ids:
                erp_objs = db.query(InternalLedgerRecord).filter(
                    InternalLedgerRecord.id.in_(erp_ids),
                    InternalLedgerRecord.company_id == customer_id,
                    InternalLedgerRecord.is_deleted == False,
                ).all()
                erp_map = {e.id: e for e in erp_objs}

            for m in matches:
                if (m.source_type or "").upper() == "TREASURY (LG)":
                    continue
                erp_obj = erp_map.get(m.source_record_id)
                if erp_obj:
                    erp_matches_by_txn.setdefault(m.bank_txn_id, []).append((m, erp_obj))
                    bank_count_per_erp[erp_obj.id] = bank_count_per_erp.get(erp_obj.id, 0) + 1
                    primary_bank_for_erp.setdefault(erp_obj.id, m.bank_txn_id)
                    prev = erp_ref_by_txn.get(m.bank_txn_id)
                    erp_ref_by_txn[m.bank_txn_id] = f"{prev}, {erp_obj.reference_number}" if prev else erp_obj.reference_number
                    if erp_obj.gl_account:
                        erp_gl_by_txn[m.bank_txn_id] = erp_obj.gl_account

        linked_ids = [t.linked_txn_id for t in all_txns if t.linked_txn_id]
        linked_txn_map: Dict[int, BankTransaction] = {}
        if linked_ids:
            linked_objs = db.query(BankTransaction).filter(BankTransaction.id.in_(linked_ids)).all()
            linked_txn_map = {lt.id: lt for lt in linked_objs}

        # Filter transactions according to handoff_mode & reversal suppression
        filtered_txns: List[BankTransaction] = []
        for t in all_txns:
            if exclude_reversals and t.is_reversal and t.linked_txn_id:
                continue
            has_erp_match = bool(erp_matches_by_txn.get(t.id))
            if handoff_mode == "SUBLEDGER_CLEARING" and not has_erp_match:
                continue
            if handoff_mode == "MISSING_GL_JOURNAL" and has_erp_match:
                continue
            filtered_txns.append(t)

        ordered_headers = [c.get("custom_header") or c.get("source_field") for c in columns_config]
        output_rows: List[Dict[str, Any]] = []

        def _is_sweep_txn(t: BankTransaction) -> bool:
            cats = {
                (t.internal_category or "").strip().upper(),
                (t.classification_category or "").strip().upper(),
                (t.category or "").strip().upper(),
            }
            return bool(cats.intersection({"INTER_BANK_SWEEP", "INTERNAL_TRANSFER"})) or (
                bool(t.linked_txn_id) and not t.is_reversal
            )

        def _get_primary_erp_meta(t: BankTransaction) -> Tuple[Optional[ReconciliationMatch], Optional[InternalLedgerRecord], Decimal, str, str]:
            pairs = erp_matches_by_txn.get(t.id) or []
            if not pairs:
                disp = "BANK_FEE_WRITEOFF"
                v_gl, _ = self._resolve_variance_gl_account(disp, wht_gl=wht_gl, fx_gl=fx_gl)
                return None, None, Decimal("0.00"), disp, v_gl
            first_m, first_e = pairs[0]
            gross_sum = sum(abs(Decimal(str(e.amount or 0))) for _, e in pairs)
            disp = (first_m.variance_disposition or "NONE").upper()
            if disp == "NONE" and (Decimal(str(t.variance_amount or 0)) > 0 or Decimal(str(first_m.variance_amount or 0)) > 0):
                desc_up = (t.raw_description or "").upper()
                if any(w in desc_up for w in ("WHT", "WITHHOLDING", "خصم", "ضريبة خصم")):
                    disp = "WHT_DEDUCTION"
                else:
                    disp = "BANK_FEE_WRITEOFF"
            v_gl, _ = self._resolve_variance_gl_account(disp, wht_gl=wht_gl, fx_gl=fx_gl)
            return first_m, first_e, gross_sum, disp, v_gl

        def _extract_val(
            t: BankTransaction,
            field_key: str,
            override_vals: Dict[str, Any] = None,
            erp_ctx: Optional[InternalLedgerRecord] = None,
            match_ctx: Optional[ReconciliationMatch] = None,
            eff_bank_gl: str = bank_gl,
        ) -> Any:
            if override_vals and field_key in override_vals:
                ov = override_vals[field_key]
                if isinstance(ov, datetime):
                    try:
                        return ov.strftime(date_fmt)
                    except Exception:
                        return ov.strftime("%Y-%m-%d")
                if isinstance(ov, Decimal):
                    return float(ov)
                return ov

            first_m, first_e, gross_erp_sum, disp, v_gl = _get_primary_erp_meta(t)
            active_erp = erp_ctx or first_e
            active_match = match_ctx or first_m

            cr_val = Decimal(str((override_vals or {}).get("credit_amount", t.credit_amount or 0)))
            dr_val = Decimal(str((override_vals or {}).get("debit_amount", t.debit_amount or 0)))

            if field_key == "matched_erp_reference":
                if erp_ctx is not None:
                    return erp_ctx.reference_number or ""
                return erp_ref_by_txn.get(t.id, "")
            if field_key == "payment_or_erp_ref":
                if erp_ctx is not None and erp_ctx.reference_number:
                    return erp_ctx.reference_number
                return erp_ref_by_txn.get(t.id) or t.e2e_id or t.back_office_ref or t.cheque_number or ""
            if field_key == "matched_erp_entity":
                if active_erp:
                    return active_erp.entity_name or ""
                pairs = erp_matches_by_txn.get(t.id) or []
                return ", ".join(dict.fromkeys(e.entity_name for _, e in pairs if e.entity_name))
            if field_key == "matched_erp_type":
                return active_erp.record_type if active_erp and active_erp.record_type else ""
            if field_key == "matched_erp_gross_amount":
                if erp_ctx is not None:
                    return float(abs(Decimal(str(erp_ctx.amount or 0))))
                return float(gross_erp_sum) if gross_erp_sum > 0 else ""
            if field_key == "allocated_erp_amount":
                if erp_ctx is not None:
                    if bank_count_per_erp.get(erp_ctx.id, 1) > 1:
                        return float(max(cr_val, dr_val))
                    return float(abs(Decimal(str(erp_ctx.amount or 0))))
                return float(gross_erp_sum) if gross_erp_sum > 0 else float(max(cr_val, dr_val))
            if field_key == "variance_disposition":
                if active_match and active_match.variance_disposition and active_match.variance_disposition != "NONE":
                    return active_match.variance_disposition
                return disp if Decimal(str(t.variance_amount or 0)) > 0 else ""
            if field_key == "variance_gl_account":
                var_dec = Decimal(str((override_vals or {}).get("variance_amount", t.variance_amount or 0)))
                return v_gl if var_dec > 0 else ""
            if field_key == "effective_bank_gl":
                return eff_bank_gl
            if field_key == "handoff_mode":
                return handoff_mode
            if field_key == "dc_indicator":
                return "CR" if cr_val > 0 else "DR"
            if field_key == "signed_amount":
                return float(cr_val - dr_val)
            if field_key == "absolute_amount":
                return float(max(cr_val, dr_val))
            if field_key == "applied_rule_name":
                return t.applied_rule_name or ""
            if field_key.startswith("custom_fields."):
                cf_k = field_key.split(".", 1)[1]
                return (t.custom_fields_json or {}).get(cf_k, "")
            if field_key.startswith("erp_custom_fields."):
                ecf_k = field_key.split(".", 1)[1]
                if active_erp and isinstance(active_erp.custom_fields_json, dict):
                    return active_erp.custom_fields_json.get(ecf_k, "")
                return ""

            val = getattr(t, field_key, None)
            if field_key == "account_number" and not val and t.statement:
                val = t.statement.account_number
            if isinstance(val, datetime):
                try:
                    return val.strftime(date_fmt)
                except Exception:
                    return val.strftime("%Y-%m-%d")
            if isinstance(val, Decimal):
                return float(val)
            if val is None:
                return ""
            return val

        def _emit_row(
            t: BankTransaction,
            eff_bank_gl: str,
            override_vals: Optional[Dict[str, Any]] = None,
            erp_ctx: Optional[InternalLedgerRecord] = None,
            match_ctx: Optional[ReconciliationMatch] = None,
        ) -> None:
            row_dict: Dict[str, Any] = {}
            for col_spec in columns_config:
                src = col_spec.get("source_field", "")
                hdr = col_spec.get("custom_header") or src
                val = _extract_val(
                    t,
                    src,
                    override_vals=override_vals,
                    erp_ctx=erp_ctx,
                    match_ctx=match_ctx,
                    eff_bank_gl=eff_bank_gl,
                )
                if val == "" and col_spec.get("default_value"):
                    val = col_spec["default_value"]
                row_dict[hdr] = val
            output_rows.append(row_dict)

        bank_leg_prefix = (
            "[CLEARING OFFSET LEG]" if offset_account_mode == "BANK_CLEARING_SUSPENSE" else "[BANK LEG]"
        )

        for t in filtered_txns:
            eff_bank_gl = self._resolve_effective_bank_gl(
                t=t,
                account_gl_lookup=account_gl_lookup,
                offset_account_mode=offset_account_mode,
                default_main_gl=bank_gl,
                default_clearing_gl=clearing_suspense_gl,
            )
            pairs = erp_matches_by_txn.get(t.id) or []
            first_m, first_e, gross_erp_sum, disp, var_gl = _get_primary_erp_meta(t)
            _, var_cat = self._resolve_variance_gl_account(disp, wht_gl=wht_gl, fx_gl=fx_gl)
            is_cr = (t.credit_amount or 0) > 0
            bank_amt = Decimal(str(t.credit_amount if is_cr else t.debit_amount))
            var_amt = Decimal(str(t.variance_amount or (first_m.variance_amount if first_m and first_m.variance_amount else 0) or 0))
            is_sweep = _is_sweep_txn(t)

            # Determine resolved offset GL for non-ERP or general lines
            if handoff_mode != "ALL" and is_sweep and not pairs:
                resolved_offset_gl = transit_gl
            else:
                raw_offset = erp_gl_by_txn.get(t.id) or t.internal_category or t.classification_category
                if not raw_offset or (handoff_mode != "ALL" and raw_offset in ("INTER_BANK_SWEEP", "INTERNAL_TRANSFER")):
                    resolved_offset_gl = transit_gl if is_sweep else (suspense_gl if handoff_mode != "ALL" else "UNMAPPED_GL")
                else:
                    resolved_offset_gl = raw_offset

            # ── MODE 1: ENRICHED_STATEMENT ("Grow First" Clean Statement for ERP Import) ──
            if handoff_mode == "ENRICHED_STATEMENT" and not expand_legs:
                enriched_cp = (first_e.entity_name if first_e and first_e.entity_name else t.counterparty_name) or ""
                enriched_ref = erp_ref_by_txn.get(t.id) or t.e2e_id or t.back_office_ref or ""

                # If pre-splitting batch matches (1:M) or fee/WHT variances into clean ERP-ready statement lines:
                if split_variance_row and pairs and (len(pairs) > 1 or var_amt > 0):
                    signed_bank = bank_amt if is_cr else -bank_amt
                    run_bal_end = Decimal(str(t.running_balance or 0))
                    cursor_bal = run_bal_end - signed_bank

                    # Determine whether ERP items are single/1:M (full invoice amounts) vs M:1 partial wire
                    is_m_to_1 = len(pairs) == 1 and bank_count_per_erp.get(pairs[0][1].id, 1) > 1
                    sub_lines: List[Tuple[Decimal, bool, str, Optional[InternalLedgerRecord], Optional[ReconciliationMatch], Dict[str, Any]]] = []

                    if not is_m_to_1:
                        for m_obj, e_obj in pairs:
                            e_abs = abs(Decimal(str(e_obj.amount or 0)))
                            e_gl = e_obj.gl_account or resolved_offset_gl
                            desc_line = f"{e_obj.reference_number} | {t.raw_description}" if e_obj.reference_number and e_obj.reference_number.upper() not in (t.raw_description or "").upper() else t.raw_description
                            sub_lines.append((
                                e_abs,
                                is_cr,
                                desc_line,
                                e_obj,
                                m_obj,
                                {
                                    "counterparty_name": e_obj.entity_name or enriched_cp,
                                    "internal_category": e_gl,
                                    "variance_amount": 0.0,
                                },
                            ))
                        # Compute exact signed residual so sum(sub_lines) == signed_bank to the penny!
                        gross_signed = sum((amt if cr_flag else -amt) for amt, cr_flag, *_ in sub_lines)
                        residual_signed = signed_bank - gross_signed
                        if abs(residual_signed) >= Decimal("0.01"):
                            fee_is_cr = residual_signed > 0
                            fee_abs = abs(residual_signed)
                            sub_lines.append((
                                fee_abs,
                                fee_is_cr,
                                f"[{var_cat} ON {enriched_ref}] {t.raw_description}",
                                None,
                                first_m,
                                {
                                    "matched_erp_reference": "",
                                    "payment_or_erp_ref": t.e2e_id or t.back_office_ref or "",
                                    "counterparty_name": enriched_cp,
                                    "classification_category": var_cat,
                                    "internal_category": var_gl,
                                    "is_reconciled": False,
                                    "variance_amount": float(fee_abs),
                                },
                            ))
                    else:
                        # M:1 partial wire: keep bank wire amount intact
                        sub_lines.append((
                            bank_amt,
                            is_cr,
                            f"{enriched_ref} | {t.raw_description}" if enriched_ref else t.raw_description,
                            first_e,
                            first_m,
                            {"counterparty_name": enriched_cp, "internal_category": resolved_offset_gl},
                        ))

                    for idx_s, (s_amt, s_is_cr, s_desc, s_erp, s_match, extra_ov) in enumerate(sub_lines):
                        cursor_bal += (s_amt if s_is_cr else -s_amt)
                        if idx_s == len(sub_lines) - 1:
                            cursor_bal = run_bal_end
                        ov = {
                            "debit_amount": 0.0 if s_is_cr else float(s_amt),
                            "credit_amount": float(s_amt) if s_is_cr else 0.0,
                            "running_balance": float(cursor_bal),
                            "raw_description": s_desc,
                            **extra_ov,
                        }
                        _emit_row(t, eff_bank_gl=eff_bank_gl, override_vals=ov, erp_ctx=s_erp, match_ctx=s_match)
                    continue

                # Standard 1-row enriched statement output
                enriched_desc = t.raw_description
                if first_e and first_e.reference_number and first_e.reference_number.upper() not in (t.raw_description or "").upper():
                    enriched_desc = f"{first_e.reference_number} | {t.raw_description}"
                _emit_row(
                    t,
                    eff_bank_gl=eff_bank_gl,
                    override_vals={
                        "counterparty_name": enriched_cp or t.counterparty_name or "",
                        "internal_category": resolved_offset_gl,
                        "raw_description": enriched_desc,
                    },
                    erp_ctx=first_e,
                    match_ctx=first_m,
                )
                continue

            # ── MODE 2: SUBLEDGER_CLEARING (Flat 1-Row-Per-Invoice Cash Application Batch) ──
            if handoff_mode == "SUBLEDGER_CLEARING" and not expand_legs:
                if len(pairs) > 1:
                    # 1:M Batch Match: emit 1 row per ERP record so SAP/Odoo/Oracle clears every invoice individually
                    gross_total = sum(abs(Decimal(str(e_obj.amount or 0))) for _, e_obj in pairs)
                    total_diff = gross_total - bank_amt  # >0 if fee/WHT deducted on AR, <0 if wire fee added on AP
                    for p_idx, (m_obj, e_obj) in enumerate(pairs):
                        e_abs = abs(Decimal(str(e_obj.amount or 0)))
                        row_var = abs(total_diff) if p_idx == 0 and abs(total_diff) >= Decimal("0.01") else Decimal("0.00")
                        if p_idx == 0 and abs(total_diff) >= Decimal("0.01"):
                            cash_for_row = e_abs - total_diff
                        else:
                            cash_for_row = e_abs
                        cash_for_row = max(Decimal("0.00"), cash_for_row)
                        _emit_row(
                            t,
                            eff_bank_gl=eff_bank_gl,
                            override_vals={
                                "debit_amount": 0.0 if is_cr else float(cash_for_row),
                                "credit_amount": float(cash_for_row) if is_cr else 0.0,
                                "counterparty_name": e_obj.entity_name or t.counterparty_name or "",
                                "internal_category": e_obj.gl_account or resolved_offset_gl,
                                "variance_amount": float(row_var),
                            },
                            erp_ctx=e_obj,
                            match_ctx=m_obj,
                        )
                else:
                    m_obj, e_obj = pairs[0]
                    is_secondary_m1 = (
                        bank_count_per_erp.get(e_obj.id, 1) > 1
                        and primary_bank_for_erp.get(e_obj.id) != t.id
                    )
                    eff_var = Decimal("0.00") if is_secondary_m1 else var_amt
                    _emit_row(
                        t,
                        eff_bank_gl=eff_bank_gl,
                        override_vals={
                            "counterparty_name": e_obj.entity_name or t.counterparty_name or "",
                            "internal_category": e_obj.gl_account or resolved_offset_gl,
                            "variance_amount": float(eff_var),
                        },
                        erp_ctx=e_obj,
                        match_ctx=m_obj,
                    )
                continue

            # ── FLAT OUTPUT FOR "ALL" OR "MISSING_GL_JOURNAL" (when expand_journal_legs == False) ──
            if not expand_legs:
                ov_flat: Dict[str, Any] = {}
                if handoff_mode != "ALL":
                    ov_flat["internal_category"] = resolved_offset_gl
                _emit_row(t, eff_bank_gl=eff_bank_gl, override_vals=ov_flat or None, erp_ctx=first_e, match_ctx=first_m)
                continue

            # ══════════════════════════════════════════════════════════════════
            #  DOUBLE-ENTRY GL JOURNAL EXPANSION (expand_journal_legs == True)
            # ══════════════════════════════════════════════════════════════════

            # Special Case A: Paired Inter-Bank Sweep in Smart Modes -> Route via Cash-in-Transit without double-counting fee!
            if handoff_mode != "ALL" and is_sweep and not pairs:
                cp_txn = linked_txn_map.get(t.linked_txn_id) if t.linked_txn_id else None
                if cp_txn is not None:
                    cp_is_cr = (cp_txn.credit_amount or 0) > 0
                    cp_amt = Decimal(str(cp_txn.credit_amount if cp_is_cr else cp_txn.debit_amount))
                    outflow_amt = bank_amt if not is_cr else cp_amt
                    inflow_amt = bank_amt if is_cr else cp_amt
                    sweep_fee = outflow_amt - inflow_amt
                    if sweep_fee > Decimal("0.00"):
                        if not is_cr:
                            # Outflow bank leg: Cr Outflow Bank (outflow_amt) = Dr Transit (inflow_amt) + Dr Bank Charges (sweep_fee)
                            sweep_legs = [
                                (
                                    {
                                        "internal_category": eff_bank_gl,
                                        "debit_amount": 0.0,
                                        "credit_amount": float(bank_amt),
                                        "variance_amount": float(sweep_fee),
                                        "raw_description": f"{bank_leg_prefix} {t.raw_description}",
                                    },
                                    None,
                                    None,
                                ),
                                (
                                    {
                                        "internal_category": transit_gl,
                                        "debit_amount": float(inflow_amt),
                                        "credit_amount": 0.0,
                                        "variance_amount": 0.0,
                                        "raw_description": f"[TRANSIT LEG] {t.raw_description}",
                                    },
                                    None,
                                    None,
                                ),
                                (
                                    {
                                        "internal_category": "5040100 - Bank Charges",
                                        "classification_category": "BANK_CHARGES",
                                        "debit_amount": float(sweep_fee),
                                        "credit_amount": 0.0,
                                        "variance_amount": float(sweep_fee),
                                        "raw_description": f"[VARIANCE FEE LEG] {t.raw_description}",
                                    },
                                    None,
                                    None,
                                ),
                            ]
                        else:
                            # Inflow bank leg: Dr Inflow Bank (inflow_amt) = Cr Transit (inflow_amt) (fee booked on outflow side)
                            sweep_legs = [
                                (
                                    {
                                        "internal_category": eff_bank_gl,
                                        "debit_amount": float(bank_amt),
                                        "credit_amount": 0.0,
                                        "variance_amount": 0.0,
                                        "raw_description": f"{bank_leg_prefix} {t.raw_description}",
                                    },
                                    None,
                                    None,
                                ),
                                (
                                    {
                                        "internal_category": transit_gl,
                                        "debit_amount": 0.0,
                                        "credit_amount": float(bank_amt),
                                        "variance_amount": 0.0,
                                        "raw_description": f"[TRANSIT LEG] {t.raw_description}",
                                    },
                                    None,
                                    None,
                                ),
                            ]
                        for leg_ov, l_erp, l_m in sweep_legs:
                            _emit_row(t, eff_bank_gl=eff_bank_gl, override_vals=leg_ov, erp_ctx=l_erp, match_ctx=l_m)
                        continue

                # Equal-amount or single-legged sweep: clean 2-leg entry against Cash-in-Transit
                for leg_ov in (
                    {
                        "internal_category": eff_bank_gl,
                        "debit_amount": float(bank_amt) if is_cr else 0.0,
                        "credit_amount": 0.0 if is_cr else float(bank_amt),
                        "variance_amount": 0.0,
                        "raw_description": f"{bank_leg_prefix} {t.raw_description}",
                    },
                    {
                        "internal_category": transit_gl,
                        "debit_amount": 0.0 if is_cr else float(bank_amt),
                        "credit_amount": float(bank_amt) if is_cr else 0.0,
                        "variance_amount": 0.0,
                        "raw_description": f"[TRANSIT LEG] {t.raw_description}",
                    },
                ):
                    _emit_row(t, eff_bank_gl=eff_bank_gl, override_vals=leg_ov)
                continue

            # Special Case B: SUBLEDGER_CLEARING with 1:M Batch ERP Matches -> Expand 1 Subledger Leg per Invoice!
            if handoff_mode == "SUBLEDGER_CLEARING" and len(pairs) > 1:
                _emit_row(
                    t,
                    eff_bank_gl=eff_bank_gl,
                    override_vals={
                        "internal_category": eff_bank_gl,
                        "debit_amount": float(bank_amt) if is_cr else 0.0,
                        "credit_amount": 0.0 if is_cr else float(bank_amt),
                        "matched_erp_reference": erp_ref_by_txn.get(t.id, ""),
                        "raw_description": f"{bank_leg_prefix} {t.raw_description}",
                    },
                )
                signed_bank = bank_amt if is_cr else -bank_amt
                signed_erp_total = Decimal("0.00")
                for m_obj, e_obj in pairs:
                    e_abs = abs(Decimal(str(e_obj.amount or 0)))
                    signed_erp_total += (e_abs if is_cr else -e_abs)
                    _emit_row(
                        t,
                        eff_bank_gl=eff_bank_gl,
                        override_vals={
                            "internal_category": e_obj.gl_account or resolved_offset_gl,
                            "counterparty_name": e_obj.entity_name or t.counterparty_name or "",
                            "debit_amount": 0.0 if is_cr else float(e_abs),
                            "credit_amount": float(e_abs) if is_cr else 0.0,
                            "variance_amount": 0.0,
                            "raw_description": f"[SUBLEDGER LEG] {e_obj.reference_number} — {t.raw_description}",
                        },
                        erp_ctx=e_obj,
                        match_ctx=m_obj,
                    )
                # Balance any batch fee/WHT/FX variance so Total DR == Total CR!
                # Bank leg signed = +bank_amt (DR) if is_cr else -bank_amt (CR)
                # Subledger legs signed = -erp_total (CR) if is_cr else +erp_total (DR)
                # Net DR - CR so far = signed_bank - signed_erp_total. Need variance leg = -(signed_bank - signed_erp_total) = signed_erp_total - signed_bank!
                needed_dr_minus_cr = signed_erp_total - signed_bank
                if abs(needed_dr_minus_cr) >= Decimal("0.01"):
                    v_abs = abs(needed_dr_minus_cr)
                    v_is_dr = needed_dr_minus_cr > 0
                    _emit_row(
                        t,
                        eff_bank_gl=eff_bank_gl,
                        override_vals={
                            "internal_category": var_gl if handoff_mode != "ALL" else "BANK_CHARGES",
                            "classification_category": var_cat,
                            "debit_amount": float(v_abs) if v_is_dr else 0.0,
                            "credit_amount": 0.0 if v_is_dr else float(v_abs),
                            "variance_amount": float(v_abs),
                            "matched_erp_reference": "",
                            "raw_description": f"[VARIANCE FEE LEG] {t.raw_description}",
                        },
                        match_ctx=first_m,
                    )
                continue

            # Standard 1:1 / Bank-Only Double-Entry GL Journal Expansion (100% balanced across AR & AP variances!)
            # Determine exact offset amount:
            # - If matched to a single ERP item (not M:1 partial wire), offset clears full `gross_erp_sum`!
            # - Otherwise, on Credit (`is_cr=True`), fee was deducted (`offset = bank_amt + var_amt`),
            #   and on Debit (`is_cr=False`), wire fee was added (`offset = bank_amt - var_amt` if bank_amt > var_amt else bank_amt).
            is_m1_partial = len(pairs) == 1 and bank_count_per_erp.get(pairs[0][1].id, 1) > 1
            if pairs and not is_m1_partial and gross_erp_sum > Decimal("0.00"):
                offset_amt = gross_erp_sum
            elif var_amt > Decimal("0.00"):
                offset_amt = (bank_amt + var_amt) if is_cr else (bank_amt - var_amt if bank_amt > var_amt else bank_amt)
            else:
                offset_amt = bank_amt

            offset_prefix = "[SUBLEDGER LEG]" if (handoff_mode == "SUBLEDGER_CLEARING" and pairs) else "[OFFSET LEG]"
            legs: List[Dict[str, Any]] = [
                {
                    "internal_category": eff_bank_gl,
                    "debit_amount": float(bank_amt) if is_cr else 0.0,
                    "credit_amount": 0.0 if is_cr else float(bank_amt),
                    "raw_description": f"{bank_leg_prefix} {t.raw_description}",
                },
                {
                    "internal_category": resolved_offset_gl,
                    "debit_amount": 0.0 if is_cr else float(offset_amt),
                    "credit_amount": float(offset_amt) if is_cr else 0.0,
                    "raw_description": f"{offset_prefix} {t.raw_description}",
                },
            ]

            # Compute exact balancing variance leg:
            # Current DR - CR = (bank_amt - offset_amt) if is_cr else (offset_amt - bank_amt)
            dr_minus_cr = (bank_amt - offset_amt) if is_cr else (offset_amt - bank_amt)
            if abs(dr_minus_cr) >= Decimal("0.01"):
                v_abs = abs(dr_minus_cr)
                # Need opposite sign of dr_minus_cr to bring DR - CR to 0.00!
                v_is_dr = dr_minus_cr < Decimal("0.00")
                target_var_gl = var_gl if handoff_mode != "ALL" else "BANK_CHARGES"
                legs.append({
                    "internal_category": target_var_gl,
                    "classification_category": var_cat,
                    "debit_amount": float(v_abs) if v_is_dr else 0.0,
                    "credit_amount": 0.0 if v_is_dr else float(v_abs),
                    "variance_amount": float(v_abs),
                    "raw_description": f"[VARIANCE FEE LEG] {t.raw_description}",
                })

            for leg_override in legs:
                _emit_row(t, eff_bank_gl=eff_bank_gl, override_vals=leg_override, erp_ctx=first_e, match_ctx=first_m)

        return ordered_headers, output_rows, filtered_txns, file_format


bank_reconcile_service = BankReconciliationService()
