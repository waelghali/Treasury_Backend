import pandas as pd
import io
import re
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal, InvalidOperation
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.models.models_reconciliation_v2 import BankStatement, BankTransaction, ReconciliationMatch
from app.models import LGRecord
from app.crud.crud_reconciliation_v2 import crud_bank_statement, crud_bank_transaction

import logging

logger = logging.getLogger("app.bank_reconciliation")

class BankReconciliationService:
    """
    Core engine for bank statement ingestion, validation, and matching.
    Includes smart heuristic detection for various bank formats.
    """

    def _clean_decimal(self, val: Any) -> Decimal:
        if pd.isna(val) or val == "":
            return Decimal("0.00")
        try:
            # Remove currency symbols and commas
            clean_val = re.sub(r'[^\d.-]', '', str(val))
            return Decimal(clean_val).quantize(Decimal("1.00"))
        except (InvalidOperation, ValueError):
            return Decimal("0.00")

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
        
        # Fallback if detection still failed
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
                    
                booking_date = pd.to_datetime(date_val, dayfirst=True, errors='coerce')
                if pd.isna(booking_date): 
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
                    "value_date": pd.to_datetime(row.get(mapping.get("value_date"), date_val), dayfirst=True, errors='coerce') or booking_date,
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
        
        # 5. Automatically run classification rules
        try:
            self.apply_classification_rules(db, db_stmt.id, company_id)
        except Exception as e:
            # Classification failure should not block ingestion completion
            logger.warning(f"Auto-classification failed: {e}")
            
        return db_stmt

    def run_matching_engine(self, db: Session, customer_id: int, user_id: int, statement_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Runs deterministic rules to match unmatched transactions with Internal Records (LG).
        If statement_id is provided, only processes that statement. Otherwise processes all for customer.
        """
        # 1. Fetch unmatched transactions
        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_reconciled == False
        )
        if statement_id:
            query = query.filter(BankTransaction.statement_id == statement_id)
            
        transactions = query.all()
        
        if not transactions:
            return {"matched_count": 0, "status": "No unmatched transactions found"}

        # 2. Fetch active LG records for this customer
        # We'll match against lg_number
        lg_records = db.query(LGRecord).filter(LGRecord.customer_id == customer_id).all()
        lg_map = {lg.lg_number.strip().upper(): lg for lg in lg_records if lg.lg_number}
        
        matched_count = 0
        
        # Rule 1: Reference Match (LG Number)
        for txn in transactions:
            desc_upper = txn.raw_description.upper()
            e2e_upper = (txn.e2e_id or "").upper()
            
            match_found = None
            
            # Efficient check: if any known LG number is a substring of the description
            for lg_num, lg_obj in lg_map.items():
                if len(lg_num) > 4 and (lg_num in desc_upper or lg_num in e2e_upper): 
                    # Only match if LG number is reasonably long to avoid false positives with small codes
                    match_found = lg_obj
                    break
            
            if match_found:
                # Create match record
                new_match = ReconciliationMatch(
                    bank_txn_id=txn.id,
                    source_type="Treasury (LG)",
                    source_record_id=match_found.id,
                    match_type="1:1",
                    match_logic="REFERENCE",
                    created_by=user_id
                )
                db.add(new_match)
                txn.is_reconciled = True
                matched_count += 1
                
        db.commit()
        return {"matched_count": matched_count, "status": f"Successfully matched {matched_count} transactions."}

    def _evaluate_condition(self, txn: BankTransaction, condition: Dict[str, Any]) -> bool:
        field = condition.get("field")
        op = condition.get("operator")
        val = condition.get("value")
        
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
        Supports sequential AND/OR logic at the line level.
        Group structure: { "conditions": [ {field, op, val, joiner}, ... ] }
        """
        conditions = group.get("conditions", [])
        if not conditions:
            return False
            
        # Initialize with first condition
        cond0 = conditions[0]
        result = self._evaluate_group(txn, cond0) if "conditions" in cond0 else self._evaluate_condition(txn, cond0)
        
        # Iterate through remaining conditions applying joiners sequentially
        for i in range(1, len(conditions)):
            cond = conditions[i]
            # Joiner tells us how to combine THIS condition with the previous RESULT
            joiner = str(cond.get("joiner", "AND")).upper()
            
            # Evaluate current condition/group
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
        
        # 1. Fetch active rules for customer, sorted by priority ascending
        # Smallest number = Highest Priority (runs first)
        rules = db.query(ClassificationRule).filter(
            ClassificationRule.company_id == customer_id,
            ClassificationRule.is_active == True,
            ClassificationRule.is_deleted == False
        ).order_by(ClassificationRule.priority.asc()).all()
        
        if not rules:
            return {"classified_count": 0, "status": "No active rules found"}

        # 2. Fetch unclassified transactions
        query = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_classified == False
        )
        if statement_id:
            query = query.filter(BankTransaction.statement_id == statement_id)
            
        transactions = query.all()
        
        classified_count = 0
        for txn in transactions:
            for rule in rules:
                res = self._evaluate_group(txn, rule.conditions_json)
                if res:
                    txn.internal_category = rule.assigned_gl_account
                    txn.classification_category = rule.assigned_gl_account # Ensure UI visibility
                    txn.applied_rule_id = rule.id # Track which rule was used
                    txn.is_classified = True
                    rule.usage_count = (rule.usage_count or 0) + 1
                    rule.last_triggered_date = datetime.now()
                    classified_count += 1
                    if rule.stop_after_match:
                        break
        
        db.commit()
        return {"classified_count": classified_count, "status": f"Successfully classified {classified_count} transactions."}

    def detect_reversals(self, db: Session, statement_id: int):
        pass

    async def detect_logical_relationships(self, db: Session, customer_id: int) -> Dict[str, Any]:
        """
        Detects reversals and inter-account transfers.
        """
        # 1. Fetch all unreconciled, unlinked transactions for this customer
        # We need to look across all statements
        txns = db.query(BankTransaction).join(BankStatement).filter(
            BankStatement.company_id == customer_id,
            BankTransaction.is_reconciled == False,
            BankTransaction.linked_txn_id == None
        ).all()
        
        if not txns:
            return {"affected_count": 0, "status": "No candidate transactions found"}

        counts = {"reversals": 0, "transfers": 0}
        processed_ids = set()

        # Reversal keywords for fuzzy matching
        rev_keywords = ["REVERSE", "REVERSAL", "ADJ", "ADJUSTMENT", "ERR", "ERROR", "OFFSET", "CORR", "CORRECTION"]

        for i, t1 in enumerate(txns):
            if t1.id in processed_ids: continue
            
            t1_amt = (t1.credit_amount or 0) - (t1.debit_amount or 0)
            if t1_amt == 0: continue

            for j in range(i + 1, len(txns)):
                t2 = txns[j]
                if t2.id in processed_ids: continue
                
                t2_amt = (t2.credit_amount or 0) - (t2.debit_amount or 0)
                
                # Condition A: Opposite Sign & Same Magnitude
                if t1_amt != -t2_amt: continue

                # Logic 1: Reversal Detection (Same Account, Fuzzy Desc, Close Dates)
                is_same_account = t1.account_number == t2.account_number
                date_diff = abs((t1.booking_date - t2.booking_date).days)
                
                if is_same_account and date_diff <= 7:
                    desc1 = t1.raw_description.upper()
                    desc2 = t2.raw_description.upper()
                    
                    has_rev_keyword = any(kw in desc1 or kw in desc2 for kw in rev_keywords)
                    names_match = desc1.split()[:3] == desc2.split()[:3] # Fuzzy name start match
                    
                    if has_rev_keyword or names_match or t1.e2e_id == t2.e2e_id:
                        # Link them
                        t1.linked_txn_id = t2.id
                        t2.linked_txn_id = t1.id
                        t1.is_reversal = True
                        t2.is_reversal = True
                        t1.is_reconciled = True
                        t2.is_reconciled = True
                        
                        processed_ids.add(t1.id)
                        processed_ids.add(t2.id)
                        counts["reversals"] += 2
                        break

                # Logic 2: Internal Transfer Detection (Different Account, Same Date, Same Magnitude)
                if not is_same_account and date_diff <= 2:
                    # Often transfers have very similar descriptions or refs
                    if t1.e2e_id == t2.e2e_id or t1.raw_description.split()[:3] == t2.raw_description.split()[:3]:
                        t1.linked_txn_id = t2.id
                        t2.linked_txn_id = t1.id
                        t1.internal_category = "INTERNAL_TRANSFER"
                        t2.internal_category = "INTERNAL_TRANSFER"
                        # We don't mark as reconciled automatically for transfers yet, 
                        # just link them for visibility, OR we can if user prefers.
                        # Let's link them and categorize for now.
                        
                        processed_ids.add(t1.id)
                        processed_ids.add(t2.id)
                        counts["transfers"] += 2
                        break

        db.commit()
        return {
            "reversals_count": counts["reversals"],
            "transfers_count": counts["transfers"],
            "status": f"Found {counts['reversals'] // 2} reversals and {counts['transfers'] // 2} transfers."
        }

bank_reconcile_service = BankReconciliationService()
