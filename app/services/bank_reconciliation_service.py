import pandas as pd
import io
import re
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal, InvalidOperation
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.models.models_reconciliation_v2 import BankStatement, BankTransaction, ReconciliationMatch, InternalLedgerRecord
from app.models import LGRecord
from app.crud.crud_reconciliation_v2 import crud_bank_statement, crud_bank_transaction

import logging

logger = logging.getLogger("app.bank_reconciliation")

class BankReconciliationService:
    """
    Core engine for bank statement ingestion, validation, and matching.
    Includes smart heuristic detection for various bank formats.
    Built-in classifiers automatically detect interest, charges, sweeps, salary, etc.
    """

    # ═══════════════════════════════════════════════════════════════════
    #  BUILT-IN CLASSIFIERS — Universal patterns that work for ALL customers
    # ═══════════════════════════════════════════════════════════════════
    BUILTIN_CLASSIFIERS = [
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
                "custody fee", "account charge",
                "عمولة", "رسوم", "مصاريف بنكية", "رسوم الحساب", "رسوم الخدمة",
                "مصاريف", "رسوم تحويل", "عمولة تحصيل"
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
                "tax on interest", "tax deducted",
                "ضريبة", "ضريبة خصم", "ضريبة استقطاع", "ضريبة القيمة المضافة",
                "دمغة", "ضرائب"
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
                "government", "ministry", "authority",
                "تأمينات اجتماعية", "جمارك", "حكومة", "هيئة", "مصلحة"
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

    def _clean_decimal(self, val: Any) -> Decimal:
        if pd.isna(val) or val == "":
            return Decimal("0.00")
        try:
            # Remove currency symbols and commas
            clean_val = re.sub(r'[^\d.-]', '', str(val))
            return Decimal(clean_val).quantize(Decimal("1.00"))
        except (InvalidOperation, ValueError):
            return Decimal("0.00")

    MONTH_MAP = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
        'يناير': 1, 'فبراير': 2, 'مارس': 3, 'ابريل': 4, 'مايو': 5, 'يونيو': 6,
        'يوليو': 7, 'اغسطس': 8, 'سبتمبر': 9, 'اكتوبر': 10, 'نوفمبر': 11, 'ديسمبر': 12
    }

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

        narrative_upper = (narrative or "").upper()

        # Signal 1: Month tokens in narrative (e.g. 05FEB2026, 05-FEB-26, 05FEB)
        month_token_match = re.search(r'\b(\d{1,2})[-/]?([A-Z]{3})[-/]?(\d{2,4})?\b', narrative_upper)
        if month_token_match:
            day_str = month_token_match.group(1)
            month_str = month_token_match.group(2)
            year_str = month_token_match.group(3)
            if month_str in self.MONTH_MAP:
                m = self.MONTH_MAP[month_str]
                d = int(day_str)
                y = int(year_str) if year_str else 2026
                if len(str(y)) == 2: y += 2000
                try:
                    return datetime(y, m, d)
                except Exception:
                    pass

        # Signal 2: Pandas dayfirst parse
        dt_cand = pd.to_datetime(date_val, dayfirst=True, errors='coerce')
        if pd.isna(dt_cand):
            return None
        
        dt = dt_cand.to_pydatetime() if hasattr(dt_cand, 'to_pydatetime') else dt_cand

        # Signal 3: Statement period header cross-check (detect and correct inverted MM/DD vs DD/MM)
        if metadata:
            meta_start = metadata.get("start_date")
            if meta_start and isinstance(meta_start, (datetime, pd.Timestamp)):
                target_month = meta_start.month
                target_year = meta_start.year
                # If parsed month != target_month, but parsed day == target_month and parsed month <= 31:
                # Then day and month were swapped by the Excel reader!
                if dt.month != target_month and dt.day == target_month and dt.month <= 31:
                    try:
                        return datetime(target_year, target_month, dt.month)
                    except Exception:
                        pass

        return dt

    def _detect_column_mapping(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Heuristically identifies columns for Date, Amount, and Description.
        Handles sparse Excel files where headers might be missing or 'Unnamed'.
        """
        mapping = {}
        
        # Keywords for matching
        keywords = {
            "date": ['date', 'booking', 'transaction date', 'تاريخ', 'يوم', 'postdate'],
            "value_date": ['value date', 'valuedate', 'تاريخ الاستحقاق'],
            "amount": ['amount', 'value', 'مبلغ', 'القيمة', 'رصيد'],
            "debitamount": ['debit', 'withdraw', 'مدين', 'سحب'],
            "creditamount": ['credit', 'deposit', 'دائن', 'إيداع'],
            "desc": ['desc', 'narrative', 'details', 'particulars', 'البيان', 'وصف', 'التفاصيل', 'desiption', 'transaction details'],
            "desc2": ['desiption2', 'description2', 'البيان 2'],
            "balance": ['balance', 'رصيد الحساب', 'الرصيد'],
            "company": ['company', 'entity', 'الشركة', 'الجهة'],
            "account_no": ['accountno', 'account number', 'رقم الحساب'],
            "back_office_ref": ['backofficereference', 'bo ref', 'المرجع'],
            "category": ['category', 'class', 'فئة', 'تصنيف'],
            "sub_category": ['subcategory', 'subclass', 'فئة فرعية'],
            "net": ['net', 'صافي'],
            "positive": ['positive', 'موجب'],
            "source": ['source', 'مصدر', 'نظام'],
            "rate_egp": ['egp rate', 'سعر الجنيه'],
            "rate_eur_usd": ['eur/usd rate', 'سعر اليورو/دولار'],
            "beneficiary": ['beneficiary', 'المستفيد'],
            "purpose": ['purpose', 'الغرض'],
            "amount_currency": ['amount in currency', 'القيمة بالعملة'],
            "amount_egp": ['amount on egp', 'amount in egp', 'القيمة بالجنيه'],
            "transfer_type": ['transfer type', 'نوع التحويل']
        }

        # 1. Try Keyword matching on column names
        for c in df.columns:
            c_str = str(c)
            if "Unnamed" in c_str or isinstance(c, (int, float)): continue 
            
            c_low = c_str.lower().replace(".", "").replace(" ", "")
            for key, kw_list in keywords.items():
                if key in mapping: continue
                if any(kw.replace(" ", "") in c_low for kw in kw_list):
                    mapping[key] = c

        # 2. Content-based detection for critical columns if keywords failed
        if 'date' not in mapping or ('amount' not in mapping and 'creditamount' not in mapping):
            # Focus on rows that look like data (avoid top metadata)
            data_start_idx = 0
            for i in range(min(50, len(df))):
                row_str = " ".join(str(v) for v in df.iloc[i].values).lower()
                # Wider date patterns: 28FEB23, 28/02/2023, 2023-01-01
                if re.search(r'(\d{1,2}[A-Z]{3}\d{2,4})|(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4})', row_str, re.IGNORECASE):
                    data_start_idx = i
                    break
            
            sample_data = df.iloc[data_start_idx:data_start_idx+60]
            candidate_date_cols = []
            candidate_amt_cols = []
            candidate_desc_cols = []

            for col in df.columns:
                values = sample_data[col].dropna().astype(str)
                values = values[values.str.strip() != ""]
                if values.empty: continue
                
                # Date detection
                date_hits = 0
                for v in values:
                    if re.search(r'(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4})|(\d{1,2}[A-Z]{3}\d{2,4})', v, re.IGNORECASE):
                        try:
                            pd.to_datetime(v, errors='raise')
                            date_hits += 1
                        except Exception: pass
                if date_hits >= 1: candidate_date_cols.append((col, date_hits))
                
                # Numeric detection (Amount/Balance)
                money_vals = []
                for v in values:
                    # Relaxed: Allow symbols like EGP, USD, etc.
                    clean_v = re.sub(r'[^\d.-]', '', v)
                    if clean_v:
                        try:
                            f_val = float(clean_v.replace(',', ''))
                            if 0.01 <= abs(f_val) <= 1000000000:
                                money_vals.append(f_val)
                        except Exception: pass
                
                if len(money_vals) >= 1: # Lowered to catch sparse columns
                    has_decimals = any(abs(v % 1) > 0.0001 for v in money_vals)
                    variance = pd.Series(money_vals).std() if len(money_vals) > 1 else 0
                    score = len(money_vals) + (10 if has_decimals else 0)
                    if variance > 10: score += 5
                    candidate_amt_cols.append((col, score))
                
                # Description detection
                text_values = values[~values.str.contains(r'^[ \d,.-]*$')]
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
                        # Only analyze the text from rows that ACTUALLY look like transactions
                        avg_len = sum(len(t) for t in co_occur_texts) / len(co_occur_texts)
                        space_count = sum(1 for t in co_occur_texts if " " in t.strip())
                        space_ratio = space_count / len(co_occur_texts)
                        unique_ratio = len(set(co_occur_texts)) / len(co_occur_texts)

                        # Favor columns with spaces heavily to avoid picking metadata (like IBANs)
                        score = (co_occur * 20) + (space_ratio * 50) + avg_len + (unique_ratio * 10)
                        candidate_desc_cols.append((col, score))

            if 'date' not in mapping and candidate_date_cols:
                mapping['date'] = max(candidate_date_cols, key=lambda x: x[1])[0]
            
            if 'amount' not in mapping and 'creditamount' not in mapping and candidate_amt_cols:
                filtered_amt = [c for c in candidate_amt_cols if c[0] != mapping.get('date')]
                filtered_amt.sort(key=lambda x: x[1], reverse=True)
                # Take all that could be amounts
                best_amt_candidates = [x[0] for x in filtered_amt if x[1] >= 1]
                best_amt_candidates.sort() 
                
                if len(best_amt_candidates) >= 3:
                    # In this format: Debit is usually first, then Credit, then Balance
                    mapping['debitamount'] = best_amt_candidates[0]
                    mapping['creditamount'] = best_amt_candidates[1]
                    mapping['balance'] = best_amt_candidates[2]
                elif len(best_amt_candidates) == 2:
                    mapping['amount'] = best_amt_candidates[0]
                    mapping['balance'] = best_amt_candidates[1]
                elif best_amt_candidates:
                    mapping['amount'] = best_amt_candidates[0]

            # Check if mapping['desc'] is missing or invalid
            mapped_cols = [mapping.get(k) for k in mapping if mapping.get(k) is not None]
            
            if 'desc' not in mapping:
                filtered_desc = [c for c in candidate_desc_cols if c[0] not in mapped_cols]
                if filtered_desc:
                    filtered_desc.sort(key=lambda x: x[1], reverse=True)
                    mapping['desc'] = filtered_desc[0][0]
                elif candidate_desc_cols:
                    # If all were picked, maybe description is one of the amount candidates wrongly picked
                    mapping['desc'] = max(candidate_desc_cols, key=lambda x: x[1])[0]
                else:
                    # Ultimate fallback: pick the column with the most varied text
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

            logger.info(f"Final Detection Mapping: {mapping}")
            return mapping

        # Keyword-based detection was sufficient; return what was found
        return mapping

    def _detect_metadata(self, df: pd.DataFrame) -> Dict[str, Any]:
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
        
        # Scan Head (first 50 rows)
        sample_head = df.head(50).astype(str)
        # Scan Tail (last 50 rows)
        sample_tail = df.tail(50).astype(str)
        
        for name, sample in [("head", sample_head), ("tail", sample_tail)]:
            for _, row in sample.iterrows():
                row_str = " ".join(row.values).lower()
                
                # Balance detection
                if any(kw in row_str for kw in ["opening", "balance b/f", "رصيد سابق", "carried forward"]):
                    for val in row.values:
                        dec = self._clean_decimal(val)
                        if dec != 0 and metadata["opening_balance"] is None: 
                            metadata["opening_balance"] = dec
                
                if any(kw in row_str for kw in ["closing", "balance c/f", "رصيد حالي", "current balance"]):
                    for val in row.values:
                        dec = self._clean_decimal(val)
                        if dec != 0 and metadata["closing_balance"] is None: 
                            metadata["closing_balance"] = dec

                # DATE DETECTION IN METADATA
                if any(kw in row_str for kw in ["date", "تاريخ", "period"]):
                    for val in row.values:
                        try:
                            dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
                            if not pd.isna(dt):
                                if metadata["start_date"] is None: metadata["start_date"] = dt
                                else: metadata["end_date"] = dt
                        except Exception: pass

                # Account number detection
                if any(kw in row_str for kw in ["account no", "account #", "رقم الحساب", "account number"]):
                    for val in row.values:
                        val_str = str(val).strip()
                        # Account numbers are often long digits
                        if val_str.isdigit() and len(val_str) > 5:
                            metadata["account_number"] = val_str
                        elif "EG" in val_str and len(val_str) > 15: # IBAN
                            metadata["account_number"] = val_str

                # Company name detection
                if any(kw in row_str for kw in ["company", "customer", "الشركة", "العميل", "account name"]):
                    for val in row.values:
                        val_str = str(val).strip()
                        if len(val_str) > 5 and not any(kw in val_str.lower() for kw in ["balance", "date", "opening", "closing", "رصيد", "account", "currency"]):
                            metadata["company_name"] = val_str

        return metadata

    def parse_statement_content(self, content: bytes, file_type: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Parses file and returns (transactions, detected_metadata).
        """
        if file_type.lower() == 'csv':
            df = pd.read_csv(io.BytesIO(content))
        elif file_type.lower() in ['xls', 'xlsx']:
            xl = pd.ExcelFile(io.BytesIO(content))
            df = xl.parse(xl.sheet_names[0], header=None)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")

        # LOGGING
        logger.info(f"Ingesting file: {file_type.upper()}, Shape: {df.shape}")

        df = df.dropna(how='all').reset_index(drop=True)
        
        # If we read without headers, let's try to find the data row
        header_row = 0
        balance_keywords = ["opening balance", "closing balance", "balance b/f", "balance c/f", "رصيد سابق", "رصيد حالي"]
        
        # Scan for date/amount headers if present
        header_keywords = {
            "date": ['date', 'booking', 'تاريخ', 'يوم'],
            "amt": ['amount', 'debit', 'credit', 'value', 'مبلغ', 'القيمة']
        }
        
        for i in range(min(50, len(df))):
            row_vals = [str(v).lower() for v in df.iloc[i].values]
            row_str = " ".join(row_vals)
            
            has_date_kw = any(any(kw in v for kw in header_keywords["date"]) for v in row_vals)
            has_amt_kw = any(any(kw in v for kw in header_keywords["amt"]) for v in row_vals)
            
            if has_date_kw and has_amt_kw:
                df.columns = df.iloc[i]
                header_row = i + 1
                df = df.iloc[header_row:].reset_index(drop=True)
                break
        
        metadata = self._detect_metadata(df)
        mapping = self._detect_column_mapping(df)
        
        logger.info(f"Detected Mapping: {mapping}")
        logger.info(f"Detected Metadata: {metadata}")
        
        # Fallback if detection still failed (mapping may be None when no column heuristic matched)
        if mapping is None:
            mapping = {}
        if not mapping.get('date') or (not mapping.get('amount') and not mapping.get('creditamount')):
             logger.warning("Heuristic detection failed or incomplete. Using smart fallback...")
             # Find non-empty columns
             non_empty_cols = [c for c in df.columns if df[c].dropna().count() > 0]
             if len(non_empty_cols) >= 2:
                 # Try to find date-like column in non-empty ones
                 detected_date = False
                 for c in non_empty_cols:
                     sample = df[c].dropna().astype(str).head(20)
                     if any(re.search(r'(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4})|(\d{1,2}[A-Z]{3}\d{2,4})', v, re.IGNORECASE) for v in sample):
                         mapping['date'] = c
                         detected_date = True
                         break
                 
                 if not detected_date:
                     mapping['date'] = non_empty_cols[0]
                 
                 remaining = [c for c in non_empty_cols if c != mapping.get('date')]
                 if remaining:
                     mapping['amount'] = remaining[0]
                     if len(remaining) > 1:
                         mapping['desc'] = remaining[1]
             
        transactions = []
        # Support for calculating running balance if sparse in Excel
        running_balance_acc = self._clean_decimal(metadata.get("opening_balance")) if metadata.get("opening_balance") else None
        strong_anchor = False
        
        for idx, row in df.iterrows():
            try:
                date_val = row.get(mapping.get("date"))
                amount_val = row.get(mapping.get("amount"))
                debit_val = row.get(mapping.get("debitamount"))
                credit_val = row.get(mapping.get("creditamount"))
                
                desc_val = row.get(mapping.get("desc"))
                # CRITICAL: pd.notna to avoid "nan" string
                desc = str(desc_val).strip() if pd.notna(desc_val) else ""
                
                # Debug first few rows
                if idx < 5 or len(transactions) < 5:
                    logger.info(f"Row {idx} debug: Date={date_val}, Amt={amount_val}, Desc='{desc}'")

                # Check if this is a continuation row (no date, no amounts, but has description)
                is_continuation = pd.isna(date_val) and pd.isna(amount_val) and pd.isna(debit_val) and pd.isna(credit_val)
                
                if is_continuation:
                    if transactions and desc and desc != "nan":
                        # Append to last transaction
                        transactions[-1]["raw_description"] += " " + desc
                    continue

                if pd.isna(date_val):
                    continue
                
                desc_low = desc.lower()
                if any(kw in desc_low for kw in balance_keywords):
                    continue
                    
                booking_date = self._resolve_transaction_date(date_val, narrative=desc, metadata=metadata)
                if not booking_date or pd.isna(booking_date): 
                    # If date fails but it was expected, maybe it's just a spacer or random text
                    continue
                
                # Amounts
                amount = self._clean_decimal(amount_val)
                debit = self._clean_decimal(debit_val) if mapping.get("debitamount") else (abs(amount) if amount < 0 else Decimal("0.00"))
                credit = self._clean_decimal(credit_val) if mapping.get("creditamount") else (amount if amount > 0 else Decimal("0.00"))
                
                # If both debit/credit were detected but mapping['amount'] was also there
                # Recalculate based on columns for consistency
                if mapping.get("debitamount") or mapping.get("creditamount"):
                    amount = credit - debit

                # If all zero, might not be a transaction row
                if amount == 0 and debit == 0 and credit == 0 and not desc:
                    continue

                # Running Balance Logic: Use Excel value if present, else calculate
                excel_balance_val = row.get(mapping.get("balance"))
                reported_balance = None
                if not pd.isna(excel_balance_val) and str(excel_balance_val).strip() != "":
                    reported_balance = self._clean_decimal(excel_balance_val)
                
                # Verify Balance Integrity
                if running_balance_acc is not None:
                    expected_balance = running_balance_acc + amount
                    if reported_balance is not None:
                        variance = abs(expected_balance - reported_balance)
                        if variance > Decimal("0.01"):
                            if not strong_anchor:
                                # Header opening balance was probably a summary, ignore and anchor here
                                pass
                            else:
                                raise HTTPException(
                                    status_code=400, 
                                    detail=f"Balance Integrity Error on {booking_date.strftime('%Y-%m-%d')}: "
                                           f"Previous Balance {running_balance_acc} + Transaction {amount} "
                                           f"does not equal Reported Balance {reported_balance}."
                                )
                        running_balance_acc = reported_balance
                        strong_anchor = True
                    else:
                        running_balance_acc = expected_balance
                else:
                    running_balance_acc = reported_balance if reported_balance is not None else Decimal("0.00")
                    if reported_balance is not None:
                        strong_anchor = True
                
                running_balance = running_balance_acc

                # AUTO-FILLING LOGIC
                rate_egp = self._clean_decimal(row.get(mapping.get("rate_egp")))
                rate_eur_usd = self._clean_decimal(row.get(mapping.get("rate_eur_usd")))
                
                txn_amount_currency = self._clean_decimal(row.get(mapping.get("amount_currency")))
                if txn_amount_currency == 0:
                    txn_amount_currency = amount
                
                txn_amount_egp = self._clean_decimal(row.get(mapping.get("amount_egp")))
                if txn_amount_egp == 0 and rate_egp > 0:
                    txn_amount_egp = amount * rate_egp
                elif txn_amount_egp == 0:
                    txn_amount_egp = amount if str(row.get(mapping.get("currency"), "EGP")).upper() == "EGP" else Decimal("0.00")

                txn_net = self._clean_decimal(row.get(mapping.get("net")))
                if txn_net == 0:
                    txn_net = amount

                txn_positive = row.get(mapping.get("positive"))
                if pd.isna(txn_positive) or txn_positive == "":
                    is_positive = amount > 0
                else:
                    is_positive = str(txn_positive).lower() in ["yes", "true", "1", "positive"]

                txn = {
                    "booking_date": booking_date,
                    "value_date": self._resolve_transaction_date(row.get(mapping.get("value_date"), date_val), narrative=desc, metadata=metadata) or booking_date,
                    "debit_amount": debit,
                    "credit_amount": credit,
                    "raw_description": desc,
                    "description_line2": str(row.get(mapping.get("desc2"), "")) if mapping.get("desc2") else None,
                    "currency": str(row.get(mapping.get("currency"), "EGP")).upper(),
                    "running_balance": running_balance,
                    
                    # Expanded fields with auto-filling defaults
                    "company_name": str(row.get(mapping.get("company"), "")) if mapping.get("company") else metadata.get("company_name"),
                    "account_number": str(row.get(mapping.get("account_no"), "")) if mapping.get("account_no") else metadata.get("account_number"),
                    "back_office_ref": str(row.get(mapping.get("back_office_ref"), "")) if mapping.get("back_office_ref") else None,
                    "category": str(row.get(mapping.get("category"), "")) if mapping.get("category") else None,
                    "sub_category": str(row.get(mapping.get("sub_category"), "")) if mapping.get("sub_category") else None,
                    "net_amount": txn_net,
                    "is_positive": is_positive,
                    "source_system": str(row.get(mapping.get("source"), "")) if mapping.get("source") else "BANK_UPLOAD",
                    "exchange_rate_egp": rate_egp,
                    "exchange_rate_eur_usd": rate_eur_usd,
                    "beneficiary_name": str(row.get(mapping.get("beneficiary"), "")) if mapping.get("beneficiary") else None,
                    "purpose_of_payment": str(row.get(mapping.get("purpose"), "")) if mapping.get("purpose") else None,
                    "amount_in_currency": txn_amount_currency,
                    "amount_in_egp": txn_amount_egp,
                    "transfer_type": str(row.get(mapping.get("transfer_type"), "")) if mapping.get("transfer_type") else None,
                }
                
                # ── Run built-in classifiers on each transaction ──
                builtin_result = self._run_builtin_classifiers(txn)
                if builtin_result:
                    txn["internal_category"] = builtin_result["category"]
                    txn["classification_category"] = builtin_result["category"]
                    txn["sub_category"] = builtin_result.get("sub_category") or txn.get("sub_category")
                    txn["classification_source"] = "BUILTIN"
                    txn["classification_confidence"] = builtin_result["confidence"]
                    txn["is_classified"] = True
                
                transactions.append(txn)
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Row {idx} mapping error: {e}")
                continue
                
        if transactions:
            sorted_txns = sorted(transactions, key=lambda x: x["booking_date"])
            # Always override metadata dates with the true transaction bounds to avoid print-date conflicts
            metadata["start_date"] = sorted_txns[0]["booking_date"]
            metadata["end_date"] = sorted_txns[-1]["booking_date"]

        return transactions, metadata


    def process_ingestion(self, db: Session, content: bytes, file_type: str, company_id: int, user_id: int, overrides: Dict[str, Any]) -> BankStatement:
        """
        Coordinates parsing, metadata merging, and DB persistence.
        Includes duplicate detection logic.
        """
        # 1. Parse & Detect
        txns_data, detected_meta = self.parse_statement_content(content, file_type)
        logger.info(f"Parsed {len(txns_data)} transactions.")
        if txns_data:
            logger.info(f"First transaction: {txns_data[0]}")
        
        # 2. Merge overrides & Convert to datetime
        def to_dt(val):
            if not val: return None
            if isinstance(val, datetime): return val
            try: return pd.to_datetime(val, dayfirst=True)
            except Exception: return None

        start_date = to_dt(overrides.get("start_date")) or detected_meta.get("start_date")
        end_date = to_dt(overrides.get("end_date")) or detected_meta.get("end_date")
        
        opening_balance = self._clean_decimal(overrides.get("opening_balance")) if overrides.get("opening_balance") else (detected_meta.get("opening_balance") or Decimal("0.00"))
        closing_balance = self._clean_decimal(overrides.get("closing_balance")) if overrides.get("closing_balance") else (detected_meta.get("closing_balance") or Decimal("0.00"))

        account_number = overrides.get("account_number") or detected_meta.get("account_number")
        currency_id = overrides.get("currency_id")

        if not start_date or not end_date:
            msg = "Could not detect statement dates."
            if not txns_data: msg += " No transactions were found in the file."
            else: msg += f" Found {len(txns_data)} transactions but dates were invalid."
            raise HTTPException(status_code=400, detail=msg)

        # 3. Duplicate Detection
        existing = db.query(BankStatement).filter(
            BankStatement.company_id == company_id,
            BankStatement.account_number == account_number,
            BankStatement.statement_start_date == start_date,
            BankStatement.statement_end_date == end_date,
            BankStatement.is_deleted == False # Ignore soft-deleted
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=400, 
                detail=f"Statement already uploaded: Account {account_number}, Period {start_date.date()} to {end_date.date()}. See list ID {existing.id}."
            )

        # 4. Create Statement
        db_stmt = BankStatement(
            bank_id=overrides.get("bank_id", 1),
            company_id=company_id,
            file_name=overrides.get("file_name", "unknown"),
            opening_balance=opening_balance,
            closing_balance=closing_balance,
            statement_start_date=start_date,
            statement_end_date=end_date,
            account_number=account_number,
            currency_id=currency_id,
            created_by=user_id,
            status="VALIDATED"
        )
        db.add(db_stmt)
        db.flush()
        
        # 4. Create Transactions
        for t_data in txns_data:
            db_txn = BankTransaction(
                statement_id=db_stmt.id,
                **t_data
            )
            # Ensure consistency with statement if txn missing account_number
            if not db_txn.account_number:
                db_txn.account_number = account_number
            
            db.add(db_txn)
            
        db.commit()
        db.refresh(db_stmt)
        
        # 5. Automatically run classification rules (for transactions NOT already classified by builtins)
        try:
            result = self.apply_classification_rules(db, customer_id=company_id, statement_id=db_stmt.id)
            logger.info(f"Auto-classification result: {result}")
        except Exception as e:
            logger.warning(f"Auto-classification failed: {e}")
        
        # 5b. Run counterparty concept matching (Credit + Customer -> Collection, Debit + Supplier -> Payment)
        try:
            concept_result = self._match_counterparty_and_concept(db, customer_id=company_id, statement_id=db_stmt.id)
            logger.info(f"Concept classification result: {concept_result}")
        except Exception as e:
            logger.warning(f"Concept classification failed: {e}")
        
        # 5c. Run collaborative consensus pattern matching
        try:
            collab_result = self._match_collaborative_patterns(db, customer_id=company_id, statement_id=db_stmt.id)
            logger.info(f"Collaborative consensus classification result: {collab_result}")
        except Exception as e:
            logger.warning(f"Collaborative consensus classification failed: {e}")
        
        # 6. Automatically detect reversals and inter-account/inter-bank transfers
        try:
            rel_result = self._detect_logical_relationships_sync(db, company_id)
            logger.info(f"Auto relationship detection: {rel_result}")
        except Exception as e:
            logger.warning(f"Auto relationship detection failed: {e}")
            
        return db_stmt

    def run_matching_engine(self, db: Session, customer_id: int, user_id: int, statement_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Runs comprehensive multi-criteria rules to match unmatched transactions with:
        1. Internal ERP Ledger Records (AR Invoices, AP Bills, Payroll Batches, LG Fees)
        2. LG Records (Treasury)
        3. Automated sweeps / transfers and reversals
        """
        # 1. First run logical relationship detection (Sweeps and reversals)
        rel_result = {}
        try:
            rel_result = self._detect_logical_relationships_sync(db, customer_id)
        except Exception as e:
            logger.warning(f"Relationship detection during auto-match failed: {e}")

        # 2. Fetch unmatched transactions
        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_reconciled == False
        )
        if statement_id:
            query = query.filter(BankTransaction.statement_id == statement_id)
            
        transactions = query.all()

        # 3. Fetch open ERP records
        open_erp_records = db.query(InternalLedgerRecord).filter(
            InternalLedgerRecord.company_id == customer_id,
            InternalLedgerRecord.status == "OPEN",
            InternalLedgerRecord.is_deleted == False
        ).all()

        # 4. Fetch active LG records
        lg_records = db.query(LGRecord).filter(LGRecord.customer_id == customer_id).all()
        lg_map = {lg.lg_number.strip().upper(): lg for lg in lg_records if lg.lg_number}

        matched_count = 0
        used_erp_ids = set()

        for txn in transactions:
            if txn.is_reconciled:
                continue

            desc_upper = (txn.raw_description or "").upper()
            e2e_upper = (txn.e2e_id or "").upper()
            back_office_upper = (txn.back_office_ref or "").upper()
            combined_text = f"{desc_upper} {e2e_upper} {back_office_upper}"

            txn_credit = Decimal(str(txn.credit_amount or 0))
            txn_debit = Decimal(str(txn.debit_amount or 0))
            txn_is_credit = txn_credit > Decimal("0.00")
            txn_amount = txn_credit if txn_is_credit else -txn_debit

            match_erp = None
            match_logic = "REFERENCE"

            # Pass 1: Exact Reference Match on ERP records (e.g. INV-2026-..., BILL-2026-...)
            for erp_rec in open_erp_records:
                if erp_rec.id in used_erp_ids:
                    continue

                ref_clean = erp_rec.reference_number.strip().upper()
                if len(ref_clean) >= 4 and ref_clean in combined_text:
                    # Verify direction and amount
                    erp_amt = Decimal(str(erp_rec.amount))
                    direction_ok = (txn_is_credit and erp_amt > 0) or (not txn_is_credit and erp_amt < 0)
                    amt_diff = abs(abs(txn_amount) - abs(erp_amt))
                    
                    if direction_ok and amt_diff < Decimal("0.05"):
                        match_erp = erp_rec
                        match_logic = "REFERENCE"
                        break

            # Pass 2: Counterparty Entity Name & Exact Amount Match
            if not match_erp:
                for erp_rec in open_erp_records:
                    if erp_rec.id in used_erp_ids:
                        continue

                    entity_upper = erp_rec.entity_name.strip().upper()
                    cp_name = (txn.counterparty_name or "").strip().upper()
                    entity_matches = (len(entity_upper) >= 3 and entity_upper in desc_upper) or (cp_name and entity_upper in cp_name)
                    
                    if entity_matches:
                        erp_amt = Decimal(str(erp_rec.amount))
                        direction_ok = (txn_is_credit and erp_amt > 0) or (not txn_is_credit and erp_amt < 0)
                        amt_diff = abs(abs(txn_amount) - abs(erp_amt))

                        if direction_ok and amt_diff < Decimal("0.05"):
                            match_erp = erp_rec
                            match_logic = "EXACT"
                            break

            if match_erp:
                new_match = ReconciliationMatch(
                    bank_txn_id=txn.id,
                    source_type=match_erp.record_type,
                    source_record_id=match_erp.id,
                    match_type="1:1",
                    match_logic=match_logic,
                    created_by=user_id
                )
                db.add(new_match)
                txn.is_reconciled = True
                match_erp.status = "RECONCILED"
                match_erp.matched_bank_txn_id = txn.id
                used_erp_ids.add(match_erp.id)
                matched_count += 1
                continue

            # Pass 3: Treasury LG Number Match
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
                matched_count += 1

        db.commit()
        return {
            "matched_count": matched_count,
            "relationships": rel_result,
            "status": f"Successfully auto-matched {matched_count} transactions against ERP ledger."
        }


    def _evaluate_condition(self, txn: BankTransaction, condition: Dict[str, Any]) -> bool:
        field = condition.get("field")
        op = condition.get("operator")
        val = condition.get("value")
        
        # Concept shortcut condition
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

        # Get actual value from transaction
        txn_val = getattr(txn, field, None)
        if txn_val is None: return False
        
        # Normalize for comparison
        if isinstance(txn_val, str):
            txn_val = txn_val.upper()
            val = str(val).upper()
        elif isinstance(txn_val, (Decimal, float, int)):
            try:
                txn_val = float(txn_val)
                val = float(val)
            except Exception: return False
            
        if op == "contains":
            return val in txn_val
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
        Evaluates a group of conditions against a transaction.
        Supports sequential AND/OR logic at the line level, as well as concept shortcuts.
        Group structure: { "conditions": [ {field, op, val, joiner}, ... ] }
        Or concept format: { "concept": "COLLECTION_FROM_CUSTOMER" }
        """
        if not group or not isinstance(group, dict):
            return False

        if "concept" in group:
            return self._evaluate_condition(txn, {"field": "concept", "operator": "equals", "value": group["concept"]})

        conditions = group.get("conditions", [])
        if not conditions:
            return False
            
        # Initialize with first condition
        cond0 = conditions[0]
        result = self._evaluate_group(txn, cond0) if "conditions" in cond0 else self._evaluate_condition(txn, cond0)
        
        # Iterate through remaining conditions applying joiners sequentially
        for i in range(1, len(conditions)):
            cond = conditions[i]
            joiner = str(cond.get("joiner", "AND")).upper()
            current_val = self._evaluate_group(txn, cond) if "conditions" in cond else self._evaluate_condition(txn, cond)
            
            if joiner == "OR":
                result = result or current_val
            else: # Default AND
                result = result and current_val
                
        return result

    def apply_classification_rules(self, db: Session, customer_id: int, statement_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Applies active classification rules to unmatched transactions.
        If statement_id is provided, only processes that statement. Otherwise processes all for customer.
        """
        from app.models.models_reconciliation_v2 import ClassificationRule
        
        # 1. Fetch unclassified transactions
        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_classified == False
        )
        if statement_id:
            query = query.filter(BankTransaction.statement_id == statement_id)
            
        transactions = query.all()
        
        builtin_count = 0
        rule_count = 0

        # Layer 1: Run Built-in Classifiers on unclassified transactions
        for txn in transactions:
            if not txn.is_classified:
                builtin_res = self._run_builtin_classifiers({
                    "raw_description": txn.raw_description,
                    "description_line2": txn.description_line2,
                    "debit_amount": txn.debit_amount,
                    "credit_amount": txn.credit_amount
                })
                if builtin_res:
                    txn.internal_category = builtin_res["category"]
                    txn.classification_category = builtin_res["category"]
                    txn.sub_category = builtin_res.get("sub_category") or txn.sub_category
                    txn.classification_source = "BUILTIN"
                    txn.classification_confidence = builtin_res["confidence"]
                    txn.is_classified = True
                    builtin_count += 1

        if builtin_count > 0:
            db.commit()

        # Layer 2: Fetch active custom rules for customer, sorted by priority ascending
        rules = db.query(ClassificationRule).filter(
            ClassificationRule.company_id == customer_id,
            ClassificationRule.is_active == True,
            ClassificationRule.is_deleted == False
        ).order_by(ClassificationRule.priority.asc()).all()
        
        if rules:
            for txn in transactions:
                if not txn.is_classified:
                    for rule in rules:
                        res = self._evaluate_group(txn, rule.conditions_json)
                        if res:
                            txn.internal_category = rule.assigned_gl_account
                            txn.classification_category = rule.assigned_gl_account
                            txn.applied_rule_id = rule.id
                            txn.classification_source = "RULE"
                            txn.classification_confidence = 100
                            txn.is_classified = True
                            rule.usage_count = (rule.usage_count or 0) + 1
                            rule.last_triggered_date = datetime.now()
                            rule_count += 1
                            if rule.stop_after_match:
                                break
            if rule_count > 0:
                db.commit()
        
        # Layer 3: Concept-based counterparty matching for remaining unclassified transactions
        concept_res = self._match_counterparty_and_concept(db, customer_id=customer_id, statement_id=statement_id)
        
        # Layer 4: Dynamic collaborative consensus intelligence for remaining unclassified transactions
        collab_count = self._match_collaborative_patterns(db, customer_id=customer_id, statement_id=statement_id)
        
        # Layer 5: Detect sweeps, internal transfers, and reversals
        try:
            rel_res = self._detect_logical_relationships_sync(db, customer_id=customer_id)
        except Exception as e:
            logger.warning(f"Relationship detection skipped during classification: {e}")
            rel_res = {}

        total_classified = builtin_count + rule_count + concept_res.get("matched_count", 0) + collab_count
        
        return {
            "classified_count": total_classified,
            "builtin_matched": builtin_count,
            "rules_matched": rule_count,
            "concept_matched": concept_res.get("matched_count", 0),
            "collaborative_matched": collab_count,
            "relationships_detected": rel_res.get("affected_count", 0),
            "status": f"Successfully classified {total_classified} transactions ({builtin_count} built-in, {rule_count} custom rules, {concept_res.get('matched_count', 0)} concepts, {collab_count} collaborative)."
        }

    def _match_counterparty_and_concept(self, db: Session, customer_id: int, statement_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Concept-based Intelligent Matching:
        1. Loads all known counterparties visible to this customer (customer-specific + global system counterparties).
        2. Matches counterparties against transaction descriptions (name and aliases, case-insensitive).
        3. Applies concept rules:
           - CREDIT + Known Customer -> "COLLECTION_FROM_CUSTOMER" (GL: AR or default_gl)
           - DEBIT + Known Supplier -> "PAYMENT_TO_SUPPLIER" (GL: AP or default_gl)
           - Transfer keywords / Bank counterparty -> "INTER_BANK_SWEEP" or "BANK_TRANSFER"
        4. Sets counterparty_name, classification_source="CONCEPT", classification_confidence=85, is_classified=True.
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

        # Sort counterparties by name length descending so longer/more specific names match first
        sorted_cps = sorted(counterparties, key=lambda c: len(c.name), reverse=True)

        for txn in unclassified_txns:
            desc_upper = f"{txn.raw_description or ''} {txn.description_line2 or ''}".upper()
            matched_cp = None

            for cp in sorted_cps:
                # Check main name
                cp_name_upper = cp.name.upper()
                if len(cp_name_upper) >= 3 and cp_name_upper in desc_upper:
                    matched_cp = cp
                    break
                
                # Check aliases
                if cp.aliases and isinstance(cp.aliases, list):
                    for alias in cp.aliases:
                        alias_upper = str(alias).upper()
                        if len(alias_upper) >= 3 and alias_upper in desc_upper:
                            matched_cp = cp
                            break
                    if matched_cp:
                        break

            if matched_cp:
                # Set counterparty
                txn.counterparty_name = matched_cp.name
                debit = txn.debit_amount or Decimal("0.00")
                credit = txn.credit_amount or Decimal("0.00")

                # Concept Rule 1: Collection from customer (Credit + Customer)
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
                
                # Concept Rule 2: Payment to supplier (Debit + Supplier)
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
                
                # Concept Rule 3: Bank / Treasury entity
                elif matched_cp.entity_type == "BANK":
                    category = matched_cp.default_category or "BANK_TRANSFER"
                    txn.internal_category = matched_cp.default_gl_account or "INTERNAL_TRANSFER"
                    txn.classification_category = category
                    txn.suggested_category = category
                    txn.classification_source = "CONCEPT"
                    txn.classification_confidence = 80
                    txn.is_classified = True
                    matched_count += 1
                
                # Concept Rule 4: Government entity (Taxes/Customs/Social insurance)
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
        return {"matched_count": matched_count, "status": f"Concept engine classified {matched_count} transactions."}

    def _match_collaborative_patterns(self, db: Session, customer_id: int, statement_id: Optional[int] = None) -> int:
        """
        Dynamically matches unclassified transactions against platform-wide promoted
        collaborative consensus patterns (both bank-specific and universal cross-bank).
        Zero customer PII, 100% anonymized signatures.
        """
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
            prediction = collaborative_service.match_collaborative(
                db=db,
                bank_id=bank_id,
                raw_description=txn.raw_description,
                direction=direction,
                description_line2=txn.description_line2
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
        gl_account: Optional[str] = None
    ) -> BankTransaction:
        """
        Feedback & Learning Loop:
        1. Updates the individual transaction.
        2. Learns/reinforces the private counterparty in the tenant registry.
        3. Submits an anonymized structural signature to the platform-wide Collaborative Consensus Engine.
        """
        from app.crud.crud_reconciliation_v2 import crud_counterparty
        
        txn = db.query(BankTransaction).join(BankStatement).filter(
            BankTransaction.id == transaction_id,
            BankStatement.company_id == customer_id
        ).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transaction not found")

        chosen_category = category or txn.classification_category or "CLASSIFIED"
        chosen_gl = gl_account or txn.internal_category

        if category:
            txn.classification_category = category
            txn.suggested_category = category
        if gl_account:
            txn.internal_category = gl_account
        if counterparty_name:
            txn.counterparty_name = counterparty_name.strip()
            # Learn or increment in tenant registry
            crud_counterparty.learn_or_increment(
                db=db,
                name=counterparty_name,
                customer_id=customer_id,
                entity_type=entity_type or "CUSTOMER",
                default_category=chosen_category,
                default_gl=chosen_gl,
                user_id=user_id
            )

        txn.is_classified = True
        txn.classification_source = "MANUAL"
        txn.classification_confidence = 100

        # Submit to federated collaborative consensus learning via Privacy Sanitizer
        try:
            from app.services.collaborative_learning_service import collaborative_service
            direction = "CREDIT" if (txn.credit_amount or 0) > 0 else "DEBIT"
            bank_id = txn.statement.bank_id if txn.statement else None
            collaborative_service.submit_confirmation(
                db=db,
                company_id=customer_id,
                bank_id=bank_id,
                raw_description=txn.raw_description,
                direction=direction,
                category=chosen_category,
                gl_account=chosen_gl
            )
        except Exception as col_err:
            logger.warning(f"Collaborative learning submission skipped: {col_err}")

        db.commit()
        db.refresh(txn)
        return txn

    def auto_discover_counterparties_from_history(self, db: Session, customer_id: int, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Analyzes historical transactions for recurring counterparty candidates.
        Returns suggestions with frequency, observed direction (CREDIT -> CUSTOMER, DEBIT -> SUPPLIER),
        and example descriptions.
        """
        txns = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id
        ).order_by(BankTransaction.id.desc()).limit(limit).all()

        STOP_WORDS = {
            "TRANSFER", "PAYMENT", "BANK", "COMMISSION", "BRANCH", "SWIFT", "DEBIT", 
            "CREDIT", "CHARGE", "FEES", "ONLINE", "ATM", "POS", "REF", "TRF", "CHQ", 
            "CHECK", "TO", "FROM", "FOR", "AND", "THE", "INVOICE", "INV", "SALARY", 
            "TAX", "WITHHOLDING", "EGP", "USD", "EUR", "GBP", "SAR", "AED",
            "تحويل", "سداد", "بنك", "عمولة", "فرع", "شيك", "حساب", "فاتورة", "دفع", "مبلغ", "مصاريف"
        }

        token_directions = {}  # token -> {"credits": int, "debits": int, "samples": list}
        
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
        Runs universal built-in classifiers against a parsed transaction dict.
        These are banking-universal patterns (interest, charges, tax, salary, etc.)
        that work for ALL customers without any configuration.
        
        Returns {"category", "sub_category", "confidence", "classifier_name"} or None.
        First match wins (classifiers are ordered from most specific to most generic).
        """
        desc = (txn_data.get("raw_description") or "").lower()
        desc2 = (txn_data.get("description_line2") or "").lower()
        combined_desc = f"{desc} {desc2}"
        
        debit = txn_data.get("debit_amount", Decimal("0.00"))
        credit = txn_data.get("credit_amount", Decimal("0.00"))
        
        for classifier in self.BUILTIN_CLASSIFIERS:
            # Check direction constraint
            if classifier["direction"] == "CREDIT" and credit <= 0:
                continue
            if classifier["direction"] == "DEBIT" and debit <= 0:
                continue
            
            # Check keyword match
            matched = False
            for kw in classifier["keywords"]:
                if kw in combined_desc:
                    matched = True
                    break
            
            if matched:
                return {
                    "category": classifier["category"],
                    "sub_category": classifier.get("sub_category"),
                    "confidence": classifier["confidence"],
                    "classifier_name": classifier["name"]
                }
        
        return None

    def _detect_logical_relationships_sync(self, db: Session, customer_id: int) -> Dict[str, Any]:
        """
        Synchronous version of detect_logical_relationships for use during ingestion.
        Detects reversals, intra-bank transfers, and CROSS-BANK sweeps.
        Works across ALL statements for the customer to catch inter-bank movements.
        """
        txns = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_reconciled == False,
            BankTransaction.linked_txn_id == None
        ).all()
        
        if not txns:
            return {"affected_count": 0, "status": "No candidate transactions found"}

        counts = {"reversals": 0, "transfers": 0, "sweeps": 0}
        processed_ids = set()

        rev_keywords = ["REVERSE", "REVERSAL", "ADJ", "ADJUSTMENT", "ERR", "ERROR", "OFFSET", "CORR", "CORRECTION"]
        sweep_keywords_upper = [kw.upper() for kw in self.SWEEP_KEYWORDS] + ["SWEEP", "سويب", "تحويل ذاتي", "CASH POOL"]

        def extract_sweep_tokens(desc: Optional[str]) -> set:
            if not desc:
                return set()
            tokens = set(re.findall(r'\b[A-Z0-9_]{8,}\b', desc.upper()))
            # Keep tokens containing at least one digit and not pure numbers
            return {t for t in tokens if any(c.isdigit() for c in t) and not t.isdigit()}

        # ── Pass 1: Direct Sweep Reference / Token Matching (Exact Ref Match Across Legs) ──
        for i, t1 in enumerate(txns):
            if t1.id in processed_ids:
                continue

            t1_desc_upper = (t1.raw_description or "").upper()
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

                t2_is_credit = (t2.credit_amount or 0) > 0
                # Must be opposite direction: one Credit, one Debit
                if t1_is_credit == t2_is_credit:
                    continue

                t2_desc_upper = (t2.raw_description or "").upper()
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
                    # Match found! Link both transactions as paired sweeps
                    t1.linked_txn_id = t2.id
                    t2.linked_txn_id = t1.id

                    # Record variance due to bank charges, commission, or FX conversion
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
                    t1.classification_confidence = 95
                    t2.classification_confidence = 95
                    t1.is_classified = True
                    t2.is_classified = True

                    processed_ids.add(t1.id)
                    processed_ids.add(t2.id)
                    counts["sweeps"] += 2
                    break

        # ── Pass 2: Opposite Sign Clearing (Exact Amount or Minor Bank Charge Variance) ──
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
                # Must be opposite sign: one Credit, one Debit
                if t1_is_credit == t2_is_credit:
                    continue

                t2_val = (t2.credit_amount or 0) if t2_is_credit else (t2.debit_amount or 0)
                if t2_val == 0:
                    continue

                amt_diff = abs(t1_val - t2_val)
                is_same_account = t1.account_number == t2.account_number
                date_diff = abs((t1.booking_date - t2.booking_date).days)

                # Reversals: Same Account, Close Dates (<= 7 days), strict equal amount
                if is_same_account and date_diff <= 7 and amt_diff == 0:
                    desc1 = (t1.raw_description or "").upper()
                    desc2 = (t2.raw_description or "").upper()

                    has_rev_keyword = any(kw in desc1 or kw in desc2 for kw in rev_keywords)
                    names_match = desc1.split()[:3] == desc2.split()[:3]
                    refs_match = t1.e2e_id and t2.e2e_id and t1.e2e_id == t2.e2e_id

                    if has_rev_keyword or names_match or refs_match:
                        t1.linked_txn_id = t2.id
                        t2.linked_txn_id = t1.id
                        t1.is_reversal = True
                        t2.is_reversal = True
                        t1.is_reconciled = True
                        t2.is_reconciled = True
                        t1.classification_source = "BUILTIN"
                        t2.classification_source = "BUILTIN"
                        t1.classification_confidence = 90
                        t2.classification_confidence = 90

                        processed_ids.add(t1.id)
                        processed_ids.add(t2.id)
                        counts["reversals"] += 2
                        break

                # Sweeps / Transfers: Different Accounts, Close Dates (<= 2 days)
                # Supports bank charge fixed fee variance (e.g. diff <= 150 or <= 5% fee tolerance)
                is_within_fee_tolerance = (amt_diff == 0) or (amt_diff <= 150) or (max(t1_val, t2_val) > 0 and (amt_diff / max(t1_val, t2_val)) <= 0.05)

                if not is_same_account and date_diff <= 2 and is_within_fee_tolerance:
                    desc1_upper = (t1.raw_description or "").upper()
                    desc2_upper = (t2.raw_description or "").upper()

                    refs_match = t1.e2e_id and t2.e2e_id and t1.e2e_id == t2.e2e_id
                    names_match = desc1_upper.split()[:3] == desc2_upper.split()[:3]
                    has_sweep_keyword = any(
                        kw in desc1_upper or kw in desc2_upper
                        for kw in sweep_keywords_upper
                    )

                    is_cross_bank = False
                    try:
                        is_cross_bank = t1.statement.bank_id != t2.statement.bank_id
                    except Exception:
                        pass

                    if refs_match or names_match or has_sweep_keyword:
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

                        t1.internal_category = category
                        t2.internal_category = category
                        t1.classification_category = category
                        t2.classification_category = category
                        t1.classification_source = "BUILTIN"
                        t2.classification_source = "BUILTIN"
                        t1.classification_confidence = 85 if (refs_match or names_match) else 75
                        t2.classification_confidence = 85 if (refs_match or names_match) else 75
                        t1.is_classified = True
                        t2.is_classified = True

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
        """
        Async wrapper for the relationship detection engine (used by the API endpoint).
        """
        return self._detect_logical_relationships_sync(db, customer_id)

    def process_erp_file_ingestion(self, db: Session, file_content: bytes, file_type: str, company_id: int, user_id: int) -> Dict[str, Any]:
        """
        Inward ERP Processing Engine:
        Reads ERP ledger exports (.xlsx, .xls, .csv), detects columns, validates,
        and saves into internal_ledger_records for dual-pane matching.
        """
        try:
            if file_type.lower() in ['csv', 'txt']:
                df = pd.read_csv(io.BytesIO(file_content))
            else:
                df = pd.read_excel(io.BytesIO(file_content))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to read ERP file: {str(e)}")

        if df.empty:
            return {"imported_count": 0, "total_rows_processed": 0, "errors": ["File is empty"], "status": "Empty file"}

        # Heuristic Column Detection
        col_map = {}
        for c in df.columns:
            c_clean = str(c).strip().lower()
            if not col_map.get("date") and any(k in c_clean for k in ['date', 'posting date', 'invoice date', 'bill date', 'تاريخ']):
                col_map["date"] = c
            elif not col_map.get("ref") and any(k in c_clean for k in ['reference', 'ref', 'invoice', 'bill', 'doc #', 'document', 'مرجع', 'فاتورة']):
                col_map["ref"] = c
            elif not col_map.get("entity") and any(k in c_clean for k in ['customer', 'vendor', 'supplier', 'party', 'entity', 'name', 'عميل', 'مورد', 'الاسم']):
                col_map["entity"] = c
            elif not col_map.get("amount") and any(k in c_clean for k in ['amount', 'total', 'net', 'value', 'مبلغ', 'قيمة', 'إجمالي']):
                col_map["amount"] = c
            elif not col_map.get("debit") and any(k in c_clean for k in ['debit', 'مدين']):
                col_map["debit"] = c
            elif not col_map.get("credit") and any(k in c_clean for k in ['credit', 'دائن']):
                col_map["credit"] = c
            elif not col_map.get("type") and any(k in c_clean for k in ['type', 'record type', 'doc type', 'category', 'نوع']):
                col_map["type"] = c
            elif not col_map.get("gl") and any(k in c_clean for k in ['gl', 'account', 'حساب']):
                col_map["gl"] = c

        if not col_map.get("ref") or (not col_map.get("amount") and not (col_map.get("debit") and col_map.get("credit"))):
            raise HTTPException(
                status_code=400,
                detail=f"Could not reliably detect Reference and Amount columns. Detected: {list(col_map.keys())}"
            )

        imported_count = 0
        errors = []

        for idx, row in df.iterrows():
            ref_val = row.get(col_map.get("ref"))
            if pd.isna(ref_val) or not str(ref_val).strip():
                continue

            ref_str = str(ref_val).strip()

            # Amount
            if col_map.get("amount"):
                amt = self._clean_decimal(row.get(col_map.get("amount")))
            else:
                deb = self._clean_decimal(row.get(col_map.get("debit")))
                cred = self._clean_decimal(row.get(col_map.get("credit")))
                amt = cred - deb

            if amt == Decimal("0.00"):
                continue

            # Date
            date_val = row.get(col_map.get("date")) if col_map.get("date") else None
            rec_date = self._resolve_transaction_date(date_val) if date_val is not None else datetime.utcnow()
            if not rec_date:
                rec_date = datetime.utcnow()

            # Entity
            entity_val = row.get(col_map.get("entity")) if col_map.get("entity") else "General Entity"
            entity_name = str(entity_val).strip() if pd.notna(entity_val) else "General Entity"

            # Type
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

            gl_acct = str(row.get(col_map.get("gl"), "")) if col_map.get("gl") and pd.notna(row.get(col_map.get("gl"))) else None

            # Upsert
            existing = db.query(InternalLedgerRecord).filter(
                InternalLedgerRecord.company_id == company_id,
                InternalLedgerRecord.reference_number == ref_str,
                InternalLedgerRecord.is_deleted == False
            ).first()

            if existing:
                existing.amount = amt
                existing.record_date = rec_date
                existing.entity_name = entity_name
                existing.record_type = record_type
                if gl_acct:
                    existing.gl_account = gl_acct
                existing.updated_by = user_id
            else:
                new_rec = InternalLedgerRecord(
                    company_id=company_id,
                    record_type=record_type,
                    reference_number=ref_str,
                    entity_name=entity_name,
                    record_date=rec_date,
                    amount=amt,
                    currency="EGP",
                    gl_account=gl_acct,
                    status="OPEN",
                    created_by=user_id
                )
                db.add(new_rec)

            imported_count += 1

        db.commit()
        return {
            "imported_count": imported_count,
            "total_rows_processed": len(df),
            "errors": errors,
            "status": f"Successfully ingested {imported_count} ERP records into ledger."
        }

    def ingest_erp_records_bulk(self, db: Session, records_data: List[Dict[str, Any]], company_id: int, user_id: int) -> Dict[str, Any]:
        """
        Accepts structured bulk JSON records from REST API / webhooks.
        """
        imported_count = 0
        for item in records_data:
            ref_str = str(item.get("reference_number", "")).strip()
            if not ref_str:
                continue

            amt = Decimal(str(item.get("amount", 0)))
            rec_type = item.get("record_type", "AR_INVOICE" if amt > 0 else "AP_BILL")
            entity_name = item.get("entity_name", "General Entity")
            raw_date = item.get("record_date")
            rec_date = pd.to_datetime(raw_date).to_pydatetime() if raw_date else datetime.utcnow()

            existing = db.query(InternalLedgerRecord).filter(
                InternalLedgerRecord.company_id == company_id,
                InternalLedgerRecord.reference_number == ref_str,
                InternalLedgerRecord.is_deleted == False
            ).first()

            if existing:
                existing.amount = amt
                existing.record_date = rec_date
                existing.entity_name = entity_name
                existing.record_type = rec_type
                if item.get("gl_account"):
                    existing.gl_account = item.get("gl_account")
                existing.updated_by = user_id
            else:
                new_rec = InternalLedgerRecord(
                    company_id=company_id,
                    record_type=rec_type,
                    reference_number=ref_str,
                    entity_name=entity_name,
                    record_date=rec_date,
                    amount=amt,
                    currency=item.get("currency", "EGP"),
                    gl_account=item.get("gl_account"),
                    status="OPEN",
                    created_by=user_id
                )
                db.add(new_rec)
            imported_count += 1

        db.commit()
        return {
            "imported_count": imported_count,
            "total_rows_processed": len(records_data),
            "errors": [],
            "status": f"Successfully ingested {imported_count} ERP records."
        }

bank_reconcile_service = BankReconciliationService()

