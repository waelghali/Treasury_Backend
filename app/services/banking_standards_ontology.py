"""
Permanent Banking Standards Ontology & Living Knowledge Seeder
==============================================================
Provides Day-1 institutional banking intelligence that continuously evolves online:
1. SWIFT MT940 Transaction Type Identification Codes (`Nxxx` / `Fxxx`) & `:86:` Structured Subfields
   (`/BENM/`, `/ORDP/`, `/EREF/`, `/PREF/`, `/REMI/`, `/CHGS/`, `/OCMT/`).
2. ISO 20022 (`CAMT.053` / `CAMT.054`) Bank Transaction Codes (`Domn-Fmly-SubFmly`) & XML Parser.
3. Regional MENA & Egyptian Clearing Rails (ACH, RTGS, IPN/InstaPay, E-Finance/CPS, POS Settlements,
   Treasury Letters of Guarantee / Documentary Credits).
4. Living Foundational Knowledge Seeder: Pre-populates `CollaborativePattern` and tenant
   `ClassificationRule` tables with foundational banking rules that evolve and self-heal online.
"""

import re
import hashlib
import logging
from defusedxml import ElementTree as SafeET
from defusedxml.common import DefusedXmlException
from xml.etree.ElementTree import ParseError as XMLParseError
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy.orm import Session

from app.models.models_reconciliation_v2 import (
    ClassificationRule,
    CollaborativePattern,
)

logger = logging.getLogger("app.banking_standards")


class BankingStandardsOntology:
    """
    Decodes international (SWIFT MT940, ISO 20022 CAMT.053) and regional (MENA/Egyptian)
    banking standards embedded in narratives or native bank files, and seeds living
    foundational rules into PostgreSQL.
    """

    # ═══════════════════════════════════════════════════════════════════
    # 1. SWIFT MT940 TRANSACTION TYPE IDENTIFICATION CODES (:61: & :86:)
    # ═══════════════════════════════════════════════════════════════════
    MT940_CODE_ONTOLOGY: Dict[str, Dict[str, Any]] = {
        "NCHG": {
            "category": "BANK_CHARGES",
            "sub_category": "SWIFT_OR_ACCOUNT_CHARGE",
            "gl_account": "BANK_CHARGES",
            "direction": "DEBIT",
            "confidence": 94,
            "max_amount_guardrail": Decimal("50000.00"),
            "label": "SWIFT MT940 Charge/Fee (NCHG)",
        },
        "FCHG": {
            "category": "BANK_CHARGES",
            "sub_category": "FINANCIAL_INSTITUTION_CHARGE",
            "gl_account": "BANK_CHARGES",
            "direction": "DEBIT",
            "confidence": 94,
            "max_amount_guardrail": Decimal("50000.00"),
            "label": "SWIFT MT940 Financial Charge (FCHG)",
        },
        "NCOM": {
            "category": "BANK_CHARGES",
            "sub_category": "BANK_COMMISSION",
            "gl_account": "BANK_CHARGES",
            "direction": "DEBIT",
            "confidence": 93,
            "max_amount_guardrail": Decimal("50000.00"),
            "label": "SWIFT MT940 Commission (NCOM)",
        },
        "NRTI": {
            "category": "REVERSED_COLLECTION",
            "sub_category": "BOUNCED_CHEQUE_OR_RETURNED_ITEM",
            "gl_account": "ACCOUNTS_RECEIVABLE",
            "direction": "DEBIT",
            "confidence": 96,
            "is_reversal": True,
            "label": "SWIFT MT940 Returned Item / Bounced Collection (NRTI)",
        },
        "NINT": {
            "category": "BANK_INTEREST",
            "sub_category_credit": "INTEREST_EARNED",
            "sub_category_debit": "INTEREST_CHARGED",
            "gl_account": "BANK_INTEREST",
            "direction": None,
            "confidence": 95,
            "label": "SWIFT MT940 Interest (NINT)",
        },
        "FINT": {
            "category": "BANK_INTEREST",
            "sub_category_credit": "INTEREST_EARNED",
            "sub_category_debit": "INTEREST_CHARGED",
            "gl_account": "BANK_INTEREST",
            "direction": None,
            "confidence": 95,
            "label": "SWIFT MT940 Financial Interest (FINT)",
        },
        "NTAX": {
            "category": "TAX_DEDUCTION",
            "sub_category": "WITHHOLDING_OR_STAMP_TAX",
            "gl_account": "TAX_EXPENSE",
            "direction": "DEBIT",
            "confidence": 94,
            "label": "SWIFT MT940 Tax / Stamp Duty (NTAX)",
        },
        "NSWP": {
            "category": "INTER_BANK_SWEEP",
            "sub_category": "LIQUIDITY_SWEEP",
            "gl_account": "INTERNAL_TRANSFER",
            "direction": None,
            "confidence": 94,
            "label": "SWIFT MT940 Cash Pool / Sweep (NSWP)",
        },
        "NLDP": {
            "category": "LOAN_REPAYMENT",
            "sub_category_credit": "TIME_DEPOSIT_MATURITY_OR_DRAWDOWN",
            "sub_category_debit": "LOAN_OR_DEPOSIT_PLACEMENT",
            "gl_account": "LOANS_AND_DEPOSITS",
            "direction": None,
            "confidence": 91,
            "label": "SWIFT MT940 Loan / Deposit (NLDP)",
        },
        "NCHK": {
            "category": "CHEQUE",
            "sub_category": "CHEQUE_CLEARING",
            "gl_account": "CHEQUES_UNDER_COLLECTION",
            "direction": None,
            "confidence": 88,
            "label": "SWIFT MT940 Cheque Clearing (NCHK)",
        },
        "NSAL": {
            "category": "SALARY_PAYROLL",
            "sub_category": "PAYROLL_DISBURSEMENT",
            "gl_account": "SALARY_PAYROLL",
            "direction": "DEBIT",
            "confidence": 95,
            "label": "SWIFT MT940 Payroll / Salary (NSAL)",
        },
        "NFEX": {
            "category": "FX_CONVERSION",
            "sub_category": "FOREIGN_EXCHANGE_SETTLEMENT",
            "gl_account": "FX_CLEARING",
            "direction": None,
            "confidence": 92,
            "label": "SWIFT MT940 Foreign Exchange (NFEX)",
        },
        "NDCR": {
            "category": "TRADE_FINANCE_LC",
            "sub_category": "DOCUMENTARY_CREDIT",
            "gl_account": "TRADE_FINANCE_LC",
            "direction": None,
            "confidence": 92,
            "label": "SWIFT MT940 Documentary Credit / LC (NDCR)",
        },
    }

    # ═══════════════════════════════════════════════════════════════════
    # 2. ISO 20022 (CAMT.053) DOMAIN-FAMILY-SUBFAMILY CODE ONTOLOGY
    # ═══════════════════════════════════════════════════════════════════
    ISO20022_CODE_ONTOLOGY: Dict[str, Dict[str, Any]] = {
        "CAMT-CAPL": {
            "category": "INTER_BANK_SWEEP",
            "sub_category": "CASH_POOLING_SWEEP",
            "gl_account": "INTERNAL_TRANSFER",
            "direction": None,
            "confidence": 96,
            "label": "ISO 20022 Cash Pooling / Sweep (CAMT-CAPL)",
        },
        "ACMT-ACOP": {
            "category": "BANK_CHARGES",
            "sub_category": "ACCOUNT_MAINTENANCE_FEE",
            "gl_account": "BANK_CHARGES",
            "direction": "DEBIT",
            "confidence": 95,
            "label": "ISO 20022 Account Operation Charge (ACMT-ACOP)",
        },
        "ACMT-MDOP": {
            "category": "BANK_CHARGES",
            "sub_category": "ACCOUNT_SERVICE_FEE",
            "gl_account": "BANK_CHARGES",
            "direction": "DEBIT",
            "confidence": 95,
            "label": "ISO 20022 Account Service Fee (ACMT-MDOP)",
        },
        "PMNT-MCOP": {
            "category": "POS_MERCHANT_SETTLEMENT",
            "sub_category": "CARD_POS_BATCH_COLLECTION",
            "gl_account": "ACCOUNTS_RECEIVABLE_POS",
            "direction": "CREDIT",
            "confidence": 94,
            "label": "ISO 20022 Merchant Card / POS Settlement (PMNT-MCOP)",
        },
        "LDAS-FTDP": {
            "category": "TIME_DEPOSIT",
            "sub_category": "FIXED_TERM_DEPOSIT",
            "gl_account": "TIME_DEPOSITS",
            "direction": None,
            "confidence": 93,
            "label": "ISO 20022 Fixed Term Deposit (LDAS-FTDP)",
        },
        "LDAS-CSLN": {
            "category": "LOAN_REPAYMENT",
            "sub_category": "CORPORATE_LOAN_FACILITY",
            "gl_account": "LOAN_REPAYMENT",
            "direction": None,
            "confidence": 93,
            "label": "ISO 20022 Corporate Loan (LDAS-CSLN)",
        },
        "TRAD-GUAR": {
            "category": "LETTER_OF_GUARANTEE",
            "sub_category": "LG_TREASURY_OPERATION",
            "gl_account": "LETTER_OF_GUARANTEE",
            "direction": None,
            "confidence": 94,
            "label": "ISO 20022 Letter of Guarantee (TRAD-GUAR)",
        },
        "TRAD-DCCT": {
            "category": "TRADE_FINANCE_LC",
            "sub_category": "DOCUMENTARY_CREDIT_LC",
            "gl_account": "TRADE_FINANCE_LC",
            "direction": None,
            "confidence": 94,
            "label": "ISO 20022 Documentary Credit (TRAD-DCCT)",
        },
    }

    # ═══════════════════════════════════════════════════════════════════
    # 3. REGIONAL MENA / EGYPTIAN RAILS & CORPORATE TREASURY PATTERNS
    # ═══════════════════════════════════════════════════════════════════
    CORPORATE_TREASURY_PATTERNS: List[Dict[str, Any]] = [
        # Letters of Guarantee (LG) — Cash Margin vs. Commission
        {
            "name": "LG_CASH_MARGIN_HOLD_OR_RELEASE",
            "category": "LETTER_OF_GUARANTEE",
            "sub_category": "LG_CASH_MARGIN",
            "gl_account": "LG_CASH_MARGIN",
            "keywords": [
                "lg margin", "lg cash margin", "guarantee margin", "cash cover lg", "margin block",
                "margin release", "lg cover", "غطاء خطاب ضمان", "تعلية غطاء", "افراج غطاء", "تأمين نقدي خطاب ضمان"
            ],
            "direction": None,
            "confidence": 94,
            "amount_tier": None,
        },
        {
            "name": "LG_ISSUANCE_OR_EXTENSION_COMMISSION",
            "category": "BANK_CHARGES",
            "sub_category": "LG_COMMISSION_FEE",
            "gl_account": "BANK_CHARGES_LG",
            "keywords": [
                "lg comm", "lg issuance comm", "lg extension fee", "guarantee commission",
                "عمولة اصدار خطاب ضمان", "عمولة مد خطاب ضمان", "عمولة خطاب ضمان", "مصاريف خطاب ضمان"
            ],
            "direction": "DEBIT",
            "confidence": 93,
            "amount_tier": None,
        },
        # POS / Card Acquiring Batch Settlements
        {
            "name": "POS_MERCHANT_BATCH_SETTLEMENT",
            "category": "POS_MERCHANT_SETTLEMENT",
            "sub_category": "CARD_ACQUIRING_SETTLEMENT",
            "gl_account": "ACCOUNTS_RECEIVABLE_POS",
            "keywords": [
                "pos settlement", "merchant settlement", "card settlement", "visa settlement",
                "mastercard settlement", "fawry settlement", "paymob settlement", "geidea",
                "حصيلة نقاط البيع", "تسوية نقاط البيع", "تسوية مبيعات الكروت", "تسوية فيزا"
            ],
            "direction": "CREDIT",
            "confidence": 93,
            "amount_tier": None,
        },
        # E-Finance / CPS Government Customs, Tax & Social Insurance
        {
            "name": "EFINANCE_GOVERNMENT_TAX_CUSTOMS",
            "category": "GOVERNMENT_PAYMENT",
            "sub_category": "EFINANCE_CPS_SETTLEMENT",
            "gl_account": "GOVERNMENT_AND_TAX_PAYABLE",
            "keywords": [
                "e-finance", "efinance", "cps payment", "cps tax", "cps customs",
                "national authority for social insurance", "nasi", "egyptian tax authority",
                "اي فاينانس", "إي فاينانس", "مصلحة الضرائب", "مصلحة الجمارك", "الهيئة القومية للتأمين الاجتماعي"
            ],
            "direction": "DEBIT",
            "confidence": 94,
            "amount_tier": None,
        },
        # Time Deposit Placement & Maturity
        {
            "name": "TIME_DEPOSIT_OPERATION",
            "category": "TIME_DEPOSIT",
            "sub_category": "TIME_DEPOSIT_SWEEP_OR_MATURITY",
            "gl_account": "TIME_DEPOSITS",
            "keywords": [
                "time deposit", "term deposit", "td placement", "td maturity", "td liquidation",
                "certificate of deposit", "ربط وديعة", "كسر وديعة", "استحقاق وديعة", "وديعة لأجل"
            ],
            "direction": None,
            "confidence": 92,
            "amount_tier": None,
        },
        # Foreign Exchange (FX) Spot / Deal Settlement
        {
            "name": "FX_CURRENCY_EXCHANGE",
            "category": "FX_CONVERSION",
            "sub_category": "FX_SPOT_CONVERSION",
            "gl_account": "FX_CLEARING",
            "keywords": [
                "fx deal", "fx spot", "currency exchange", "fx conversion", "buy usd", "sell usd",
                "buy eur", "sell eur", "تغيير عملة", "تدبير عملة", "بيع عملة", "شراء عملة"
            ],
            "direction": None,
            "confidence": 91,
            "amount_tier": None,
        },
    ]

    # ═══════════════════════════════════════════════════════════════════
    #  NARRATIVE STANDARDS DECODER (:86: Tags, MT940 Codes, ISO 20022)
    # ═══════════════════════════════════════════════════════════════════

    def decode_narrative_standards(
        self,
        raw_description: Optional[str],
        description_line2: Optional[str] = None,
        direction: str = "DEBIT",
        amount: Decimal = Decimal("0.00"),
    ) -> Dict[str, Any]:
        """
        Inspects a transaction narrative for embedded SWIFT MT940 codes, `:86:` structured
        subfields (`/BENM/`, `/ORDP/`, `/EREF/`, `/REMI/`, `/CHGS/`, `/OCMT/`), ISO 20022
        domain-family codes, and MENA/Egyptian payment rail markers.
        """
        combined = f"{raw_description or ''} {description_line2 or ''}".strip()
        combined_upper = combined.upper()
        combined_lower = combined.lower()

        extracted: Dict[str, Any] = {
            "mt940_code": None,
            "iso20022_code": None,
            "payment_rail": None,
            "extracted_counterparty": None,
            "extracted_e2e_ref": None,
            "remittance_info": None,
            "embedded_fee_amount": None,
            "embedded_fee_currency": None,
            "original_currency_amount": None,
            "original_currency": None,
            "classification": None,
        }
        if not combined:
            return extracted

        # ── 1. Extract Structured :86: / Corporate Wire Subfield Tags ──
        # Beneficiary (/BENM/... or B/O: or FAV: or للمستفيد)
        benm_match = re.search(
            r'(?:/BENM/|BENM/|FAV(?:\.|OR)?\s*:?|BENEFICIARY\s*:?|للمستفيد\s*:?)\s*([A-Z0-9\u0600-\u06FF .,&\-]{3,45}?)(?=\s*(?:/[A-Z]{4}/|REF|TRN|EREF|CHGS|$))',
            combined,
            re.IGNORECASE,
        )
        if benm_match:
            cand_cp = benm_match.group(1).strip(" /:-,.")
            if len(cand_cp) >= 3 and not cand_cp.isdigit():
                extracted["extracted_counterparty"] = cand_cp

        # Ordering Party (/ORDP/... or ORDERING PARTY: or من حساب)
        if not extracted["extracted_counterparty"]:
            ordp_match = re.search(
                r'(?:/ORDP/|ORDP/|ORDERING PARTY\s*:?|BY ORDER OF\s*:?|من حساب\s*:?|محول من\s*:?)\s*([A-Z0-9\u0600-\u06FF .,&\-]{3,45}?)(?=\s*(?:/[A-Z]{4}/|REF|TRN|EREF|REMI|$))',
                combined,
                re.IGNORECASE,
            )
            if ordp_match:
                cand_cp = ordp_match.group(1).strip(" /:-,.")
                if len(cand_cp) >= 3 and not cand_cp.isdigit():
                    extracted["extracted_counterparty"] = cand_cp

        # End-to-End / Payment Reference (/EREF/, /PREF/, /TRRF/, E2E:)
        eref_match = re.search(r'(?:/EREF/|/PREF/|/TRRF/|E2E\s*:?)\s*([A-Z0-9\-_]{5,35})', combined_upper)
        if eref_match and eref_match.group(1) not in ("NOTPROVIDED", "NONE"):
            extracted["extracted_e2e_ref"] = eref_match.group(1)

        # Remittance Info (/REMI/...)
        remi_match = re.search(r'(?:/REMI/|REMI/)\s*([^/]{3,60})', combined_upper)
        if remi_match:
            extracted["remittance_info"] = remi_match.group(1).strip()

        # Embedded Wire Charges (/CHGS/EGP75.00 or /CHGS/USD25,00)
        chgs_match = re.search(r'/CHGS/([A-Z]{3})\s*(\d+(?:[.,]\d{1,2})?)', combined_upper)
        if chgs_match:
            extracted["embedded_fee_currency"] = chgs_match.group(1)
            try:
                extracted["embedded_fee_amount"] = Decimal(chgs_match.group(2).replace(",", "."))
            except Exception:
                pass

        # Original Currency Amount (/OCMT/USD10000.00)
        ocmt_match = re.search(r'/OCMT/([A-Z]{3})\s*(\d+(?:[.,]\d{1,2})?)', combined_upper)
        if ocmt_match:
            extracted["original_currency"] = ocmt_match.group(1)
            try:
                extracted["original_currency_amount"] = Decimal(ocmt_match.group(2).replace(",", "."))
            except Exception:
                pass

        # ── 2. Detect Payment Rail Markers (ACH, RTGS, IPN/InstaPay, SWIFT, E-Finance, POS) ──
        if any(k in combined_upper for k in ("INSTAPAY", "IPN", "انستاباي", "إنستاباي")):
            extracted["payment_rail"] = "INSTAPAY_IPN"
        elif any(k in combined_upper for k in ("E-FINANCE", "EFINANCE", "CPS", "اي فاينانس", "إي فاينانس")):
            extracted["payment_rail"] = "EFINANCE_CPS"
        elif any(k in combined_upper for k in ("EG-ACH", "ACH ", " ACH", "غرفة المقاصة")):
            extracted["payment_rail"] = "ACH"
        elif "RTGS" in combined_upper:
            extracted["payment_rail"] = "RTGS"
        elif any(k in combined_upper for k in ("SWIFT", "MT103", "MT202")):
            extracted["payment_rail"] = "SWIFT"

        # ── 3. Detect ISO 20022 Domain-Family Codes (e.g. CAMT-CAPL, PMNT-MCOP) ──
        for iso_prefix, spec in self.ISO20022_CODE_ONTOLOGY.items():
            if iso_prefix in combined_upper:
                extracted["iso20022_code"] = iso_prefix
                if spec["direction"] is None or spec["direction"] == direction:
                    extracted["classification"] = {
                        "category": spec["category"],
                        "sub_category": spec.get("sub_category"),
                        "gl_account": spec["gl_account"],
                        "confidence": spec["confidence"],
                        "source": "ISO20022_STANDARD",
                        "reason": f"Matched {spec['label']}",
                    }
                    return extracted

        # ── 4. Detect SWIFT MT940 Transaction Type Identification Codes (Nxxx / Fxxx) ──
        for mt_code, spec in self.MT940_CODE_ONTOLOGY.items():
            if re.search(rf'\b{mt_code}\b', combined_upper):
                extracted["mt940_code"] = mt_code
                if spec["direction"] and spec["direction"] != direction:
                    continue
                max_guard = spec.get("max_amount_guardrail")
                if max_guard and abs(amount) > max_guard:
                    continue
                sub_cat = spec.get("sub_category")
                if not sub_cat:
                    sub_cat = spec.get("sub_category_credit") if direction == "CREDIT" else spec.get("sub_category_debit")
                extracted["classification"] = {
                    "category": spec["category"],
                    "sub_category": sub_cat,
                    "gl_account": spec["gl_account"],
                    "confidence": spec["confidence"],
                    "is_reversal": spec.get("is_reversal", False),
                    "source": "MT940_STANDARD",
                    "reason": f"Matched {spec['label']}",
                }
                return extracted

        # ── 5. Detect Corporate Treasury & MENA Rail Patterns ──
        for pat in self.CORPORATE_TREASURY_PATTERNS:
            if pat["direction"] and pat["direction"] != direction:
                continue
            for kw in pat["keywords"]:
                if kw in combined_lower:
                    extracted["classification"] = {
                        "category": pat["category"],
                        "sub_category": pat.get("sub_category"),
                        "gl_account": pat["gl_account"],
                        "confidence": pat["confidence"],
                        "source": "BANKING_ONTOLOGY",
                        "reason": f"Matched corporate treasury pattern '{kw}' ({pat['name']})",
                    }
                    return extracted

        return extracted

    # ═══════════════════════════════════════════════════════════════════
    #  NATIVE SWIFT MT940 & ISO 20022 (CAMT.053) STATEMENT PARSERS
    # ═══════════════════════════════════════════════════════════════════

    def is_mt940_content(self, content: bytes) -> bool:
        sample = content[:4096].decode("utf-8", errors="ignore")
        return bool(re.search(r':20:|(?::60[FM]:)|(?::61:)', sample))

    def is_camt053_content(self, content: bytes) -> bool:
        if not content:
            return False
        sample_bytes = content[:4096]
        if sample_bytes.startswith((b"\xff\xfe", b"\xfe\xff")):
            even_len = len(sample_bytes) - (len(sample_bytes) % 2)
            sample = sample_bytes[:even_len].decode("utf-16", errors="ignore")
        else:
            sample = sample_bytes.decode("utf-8", errors="ignore")
        if any(
            marker in sample
            for marker in (
                "urn:iso:std:iso:20022:tech:xsd:camt.053",
                "urn:iso:std:iso:20022:tech:xsd:camt.052",
                "urn:iso:std:iso:20022:tech:xsd:camt.054",
            )
        ):
            return True
        return bool(
            re.search(
                r"<(?:[A-Za-z0-9_.-]+:)?BkToCstmr(?:Stmt|AcctRpt|DbtCdtNtfctn)\b",
                sample,
            )
        )

    is_iso20022_camt_content = is_camt053_content

    def parse_mt940_statement(self, content: bytes) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Native SWIFT MT940 Customer Statement Message Parser:
        Extracts :25: Account, :60F:/:60M: Opening Balance, :61: Statement Lines
        (Value Date, Entry/Posting Date, D/C/Reversal Indicator, Amount, Nxxx Type Code,
        Customer Reference, Bank Reference), :86: Structured Narrative, and :62F:/:62M: Closing Balance.
        """
        text = content.decode("utf-8", errors="replace")
        metadata: Dict[str, Any] = {
            "opening_balance": Decimal("0.00"),
            "closing_balance": Decimal("0.00"),
            "start_date": None,
            "end_date": None,
            "account_number": None,
            "company_name": None,
            "currency": "EGP",
            "detected_mapping": {"format": "SWIFT_MT940"},
            "continuity_warnings": [],
            "balance_variance_amount": Decimal("0.00"),
            "balance_continuity_status": "VALIDATED",
        }

        # :25: Account Identification
        acct_m = re.search(r':25:([^\r\n]+)', text)
        if acct_m:
            metadata["account_number"] = acct_m.group(1).strip()

        # :60F: or :60M: Opening Balance -> e.g. C260801EGP100000,00
        open_m = re.search(r':60[FM]:([CD])(\d{6})([A-Z]{3})([\d,\.]+)', text)
        if open_m:
            dc, yymmdd, ccy, amt_str = open_m.groups()
            metadata["currency"] = ccy
            amt = Decimal(amt_str.replace(",", "."))
            metadata["opening_balance"] = amt if dc == "C" else -amt
            try:
                metadata["start_date"] = datetime.strptime(yymmdd, "%y%m%d")
            except Exception:
                pass

        # :62F: or :62M: Closing Balance
        close_m = re.search(r':62[FM]:([CD])(\d{6})([A-Z]{3})([\d,\.]+)', text)
        if close_m:
            dc, yymmdd, ccy, amt_str = close_m.groups()
            amt = Decimal(amt_str.replace(",", "."))
            metadata["closing_balance"] = amt if dc == "C" else -amt
            try:
                metadata["end_date"] = datetime.strptime(yymmdd, "%y%m%d")
            except Exception:
                pass

        # Parse :61: and optional following :86: blocks
        # :61: YYMMDD [MMDD] (C|D|RC|RD)[A-Z]? Amount (Nxxx|Fxxx|Sxxx) CustomerRef [//BankRef]
        line_pattern = re.compile(
            r':61:(\d{6})(\d{4})?(R?[CD])[A-Z]?([\d,\.]+)([NFS][A-Z0-9]{3})([^\r\n]*)(?:\r?\n:86:((?:(?!:61:|:62[FM]:|:64:|:65:|-}).)*))?',
            re.DOTALL,
        )

        transactions: List[Dict[str, Any]] = []
        running_bal = metadata["opening_balance"] or Decimal("0.00")

        for match in line_pattern.finditer(text):
            val_yymmdd, entry_mmdd, dc_ind, amt_str, swift_code, ref_tail, narrative_block = match.groups()
            try:
                val_dt = datetime.strptime(val_yymmdd, "%y%m%d")
            except Exception:
                val_dt = metadata.get("start_date") or datetime.utcnow()

            if entry_mmdd:
                try:
                    entry_month = int(entry_mmdd[:2])
                    entry_day = int(entry_mmdd[2:])
                    entry_year = val_dt.year
                    # Handle year boundary (e.g. Value Date Jan 2, Posting Date Dec 31)
                    if val_dt.month == 1 and entry_month == 12:
                        entry_year -= 1
                    elif val_dt.month == 12 and entry_month == 1:
                        entry_year += 1
                    booking_dt = datetime(entry_year, entry_month, entry_day)
                except Exception:
                    booking_dt = val_dt
            else:
                booking_dt = val_dt

            amt = Decimal(amt_str.replace(",", ".")).quantize(Decimal("1.00"))
            is_reversal = dc_ind.startswith("R")
            # In MT940: C = Credit, D = Debit, RC = Reversal of Credit (i.e. Debit movement), RD = Reversal of Debit (Credit movement)
            if dc_ind in ("C", "RD"):
                credit = amt
                debit = Decimal("0.00")
                signed = amt
                direction = "CREDIT"
            else:
                debit = amt
                credit = Decimal("0.00")
                signed = -amt
                direction = "DEBIT"

            running_bal += signed

            # Extract Customer Ref ( E2E ) and Bank Ref ( // )
            ref_tail_clean = (ref_tail or "").strip()
            parts = ref_tail_clean.split("//", 1)
            cust_ref = parts[0].strip() if parts[0].strip() not in ("NONREF", "") else None
            bank_ref = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None

            raw_desc = re.sub(r'\s+', ' ', (narrative_block or "").strip())
            if swift_code and swift_code not in raw_desc:
                raw_desc = f"[{swift_code}] {raw_desc}".strip()

            std_info = self.decode_narrative_standards(raw_desc, direction=direction, amount=amt)
            float_days = (val_dt.date() - booking_dt.date()).days
            custom_fields: Dict[str, Any] = {"swift_mt940_code": swift_code}
            if float_days != 0:
                custom_fields["clearing_float_days"] = str(float_days)
            if std_info.get("payment_rail"):
                custom_fields["payment_rail"] = std_info["payment_rail"]
            if std_info.get("remittance_info"):
                custom_fields["remittance_info"] = std_info["remittance_info"]
            if std_info.get("embedded_fee_amount") is not None:
                custom_fields["embedded_fee_amount"] = str(std_info["embedded_fee_amount"])

            cls_info = std_info.get("classification")
            txn: Dict[str, Any] = {
                "booking_date": booking_dt,
                "value_date": val_dt,
                "debit_amount": debit,
                "credit_amount": credit,
                "raw_description": raw_desc or f"MT940 {swift_code} {cust_ref or ''}".strip(),
                "description_line2": None,
                "currency": metadata["currency"],
                "running_balance": running_bal,
                "account_number": metadata["account_number"],
                "e2e_id": cust_ref or std_info.get("extracted_e2e_ref"),
                "back_office_ref": bank_ref,
                "counterparty_name": std_info.get("extracted_counterparty"),
                "beneficiary_name": std_info.get("extracted_counterparty"),
                "net_amount": signed,
                "is_positive": signed > 0,
                "is_reversal": is_reversal or bool(cls_info and cls_info.get("is_reversal")),
                "source_system": "SWIFT_MT940",
                "custom_fields_json": custom_fields,
            }
            if cls_info:
                txn["internal_category"] = cls_info["gl_account"]
                txn["classification_category"] = cls_info["category"]
                txn["sub_category"] = cls_info.get("sub_category")
                txn["classification_source"] = cls_info["source"]
                txn["classification_confidence"] = cls_info["confidence"]
                txn["auto_confirmed"] = cls_info["confidence"] >= 85
                txn["classification_reasoning"] = cls_info["reason"]
                txn["is_classified"] = True

            transactions.append(txn)

        if transactions:
            if not metadata["start_date"]:
                metadata["start_date"] = min(t["booking_date"] for t in transactions)
            if not metadata["end_date"]:
                metadata["end_date"] = max(t["booking_date"] for t in transactions)

        return transactions, metadata

    def parse_camt053_statement(self, content: bytes) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Native ISO 20022 (CAMT.053 / CAMT.052 / CAMT.054) XML Bank-to-Customer Parser:
        Uses defusedxml to block entity-expansion/DTD attacks and extracts Account IBAN,
        Opening/Closing Balances, and Entry (`<Ntry>`) Posting Date, Value Date, Reversal
        Indicator, ISO `<BkTxCd>`, Counterparties, and Remittance Info.
        """
        try:
            root = SafeET.fromstring(content, forbid_dtd=True)
        except (DefusedXmlException, XMLParseError) as exc:
            raise ValueError(f"Invalid or unsafe XML bank statement file: {exc}") from exc

        def _parse_decimal(raw: Optional[str], field_label: str) -> Optional[Decimal]:
            cleaned = (raw or "").strip()
            if not cleaned:
                return None
            try:
                return Decimal(cleaned)
            except InvalidOperation as exc:
                raise ValueError(f"Invalid decimal value for {field_label}: {cleaned!r}") from exc

        def _parse_iso_dt(raw: Optional[str]) -> Optional[datetime]:
            cleaned = (raw or "").strip()
            if not cleaned:
                return None
            try:
                return datetime.fromisoformat(cleaned[:10])
            except ValueError:
                return None

        # Strip XML namespaces for universal camt.052 / camt.053 / camt.054 compatibility
        for elem in root.iter():
            if "}" in elem.tag:
                elem.tag = elem.tag.split("}", 1)[1]

        containers = root.findall(".//Stmt")
        fmt_code, label_prefix = "ISO20022_CAMT053", "CAMT.053"
        if not containers:
            containers = root.findall(".//Rpt")
            fmt_code, label_prefix = "ISO20022_CAMT052", "CAMT.052"
        if not containers:
            containers = root.findall(".//Ntfctn")
            fmt_code, label_prefix = "ISO20022_CAMT054", "CAMT.054"

        metadata: Dict[str, Any] = {
            "opening_balance": Decimal("0.00"),
            "closing_balance": Decimal("0.00"),
            "start_date": None,
            "end_date": None,
            "account_number": None,
            "company_name": None,
            "currency": "EGP",
            "detected_mapping": {"format": fmt_code},
            "continuity_warnings": [],
            "balance_variance_amount": Decimal("0.00"),
            "balance_continuity_status": "VALIDATED",
        }

        if not containers:
            return [], metadata

        primary_acct: Optional[str] = None
        seen_accounts: List[str] = []
        has_currency = False
        has_opening_bal = False
        has_closing_bal = False
        cre_dt_fallback: Optional[datetime] = None
        primary_running_bal = Decimal("0.00")
        transactions: List[Dict[str, Any]] = []

        grp_cre_dt = _parse_iso_dt(root.findtext(".//GrpHdr/CreDtTm"))

        for container in containers:
            container_acct_raw = (
                container.findtext("./Acct/Id/IBAN")
                or container.findtext("./Acct/Id/Othr/Id")
                or container.findtext(".//Acct/Id/IBAN")
                or container.findtext(".//Acct/Id/Othr/Id")
            )
            container_acct = (container_acct_raw or "").strip() or None
            if container_acct:
                if primary_acct is None:
                    primary_acct = container_acct
                    metadata["account_number"] = container_acct
                if container_acct not in seen_accounts:
                    seen_accounts.append(container_acct)

            container_ccy_raw = container.findtext("./Acct/Ccy") or container.findtext(".//Acct/Ccy")
            container_ccy = (container_ccy_raw or "").strip() or None
            if container_ccy and not has_currency:
                metadata["currency"] = container_ccy
                has_currency = True

            container_cre_dt = _parse_iso_dt(container.findtext("./CreDtTm") or container.findtext(".//CreDtTm")) or grp_cre_dt
            if cre_dt_fallback is None and container_cre_dt is not None:
                cre_dt_fallback = container_cre_dt

            is_primary_container = (not primary_acct) or (not container_acct) or (container_acct == primary_acct)

            container_has_open = False
            container_open_bal = Decimal("0.00")

            for bal in container.findall(".//Bal"):
                code = (bal.findtext(".//Tp/CdOrPrtry/Cd") or "").strip().upper()
                amt_text = bal.findtext(".//Amt")
                cdt_dbt = (bal.findtext(".//CdtDbtInd") or "").strip().upper()
                dt_text = bal.findtext(".//Dt/Dt") or bal.findtext(".//Dt/DtTm")
                val = _parse_decimal(amt_text, f"Bal/{code or 'UNKNOWN'}/Amt")
                if code and val is not None:
                    if cdt_dbt == "DBIT":
                        val = -val
                    bal_dt = _parse_iso_dt(dt_text)
                    if code in ("OPBD", "PRCD", "ITOP") and not container_has_open:
                        container_has_open = True
                        container_open_bal = val
                        if is_primary_container and not has_opening_bal:
                            has_opening_bal = True
                            metadata["opening_balance"] = val
                            if bal_dt is not None:
                                metadata["start_date"] = bal_dt
                    elif code in ("CLBD", "ITBD"):
                        if is_primary_container:
                            has_closing_bal = True
                            metadata["closing_balance"] = val
                            if bal_dt is not None:
                                metadata["end_date"] = bal_dt

            container_running_bal = (
                container_open_bal
                if container_has_open
                else (primary_running_bal if is_primary_container else Decimal("0.00"))
            )

            for ntry in container.findall(".//Ntry"):
                amt_el = ntry.find("./Amt")
                if amt_el is None or not (amt_el.text or "").strip():
                    continue
                parsed_amt = _parse_decimal(amt_el.text, "Ntry/Amt")
                if parsed_amt is None:
                    continue
                amt = parsed_amt.quantize(Decimal("1.00"))
                ccy = (amt_el.attrib.get("Ccy") or container_ccy or metadata["currency"]).strip()
                is_credit = (ntry.findtext("./CdtDbtInd") or "").strip().upper() == "CRDT"
                is_reversal = (ntry.findtext("./RvslInd") or "").strip().lower() == "true"

                book_text = ntry.findtext("./BookgDt/Dt") or ntry.findtext("./BookgDt/DtTm")
                val_text = ntry.findtext("./ValDt/Dt") or ntry.findtext("./ValDt/DtTm")
                booking_dt = _parse_iso_dt(book_text) or container_cre_dt or cre_dt_fallback or datetime.utcnow()
                value_dt = _parse_iso_dt(val_text) or booking_dt

                domn = (ntry.findtext(".//BkTxCd/Domn/Cd") or "").strip()
                fmly = (ntry.findtext(".//BkTxCd/Domn/Fmly/Cd") or "").strip()
                subfmly = (ntry.findtext(".//BkTxCd/Domn/Fmly/SubFmlyCd") or "").strip()
                iso_code = f"{domn}-{fmly}" if (domn and fmly) else ""

                e2e_id = (ntry.findtext(".//NtryDtls/TxDtls/Refs/EndToEndId") or "").strip() or None
                acct_svcr_ref = (
                    ntry.findtext("./AcctSvcrRef")
                    or ntry.findtext(".//NtryDtls/TxDtls/Refs/AcctSvcrRef")
                    or ""
                ).strip() or None
                ustrd = (
                    ntry.findtext(".//NtryDtls/TxDtls/RmtInf/Ustrd")
                    or ntry.findtext("./AddtlNtryInf")
                    or ""
                ).strip()

                if is_credit:
                    cp_name = (
                        ntry.findtext(".//NtryDtls/TxDtls/RltdPties/Dbtr/Nm")
                        or ntry.findtext(".//NtryDtls/TxDtls/RltdPties/Dbtr/Pty/Nm")
                        or ""
                    ).strip() or None
                    credit, debit, signed, direction = amt, Decimal("0.00"), amt, "CREDIT"
                else:
                    cp_name = (
                        ntry.findtext(".//NtryDtls/TxDtls/RltdPties/Cdtr/Nm")
                        or ntry.findtext(".//NtryDtls/TxDtls/RltdPties/Cdtr/Pty/Nm")
                        or ""
                    ).strip() or None
                    debit, credit, signed, direction = amt, Decimal("0.00"), -amt, "DEBIT"

                container_running_bal += signed
                raw_desc = f"[{iso_code}-{subfmly}] {ustrd} {cp_name or ''}".strip(" -[]")
                std_info = self.decode_narrative_standards(raw_desc, direction=direction, amount=amt)
                cls_info = std_info.get("classification")

                float_days = (value_dt.date() - booking_dt.date()).days
                custom_fields: Dict[str, Any] = {}
                if iso_code:
                    custom_fields["iso20022_code"] = f"{iso_code}-{subfmly}".strip("-")
                if float_days != 0:
                    custom_fields["clearing_float_days"] = str(float_days)

                txn: Dict[str, Any] = {
                    "booking_date": booking_dt,
                    "value_date": value_dt,
                    "debit_amount": debit,
                    "credit_amount": credit,
                    "raw_description": raw_desc or f"{label_prefix} {iso_code}".strip(),
                    "currency": ccy,
                    "running_balance": container_running_bal,
                    "account_number": container_acct or metadata["account_number"],
                    "e2e_id": e2e_id if e2e_id not in ("NOTPROVIDED", None) else std_info.get("extracted_e2e_ref"),
                    "back_office_ref": acct_svcr_ref,
                    "counterparty_name": cp_name or std_info.get("extracted_counterparty"),
                    "beneficiary_name": cp_name or std_info.get("extracted_counterparty"),
                    "net_amount": signed,
                    "is_positive": signed > 0,
                    "is_reversal": is_reversal,
                    "source_system": fmt_code,
                    "custom_fields_json": custom_fields or None,
                }
                if cls_info:
                    txn["internal_category"] = cls_info["gl_account"]
                    txn["classification_category"] = cls_info["category"]
                    txn["sub_category"] = cls_info.get("sub_category")
                    txn["classification_source"] = cls_info["source"]
                    txn["classification_confidence"] = cls_info["confidence"]
                    txn["auto_confirmed"] = cls_info["confidence"] >= 85
                    txn["classification_reasoning"] = cls_info["reason"]
                    txn["is_classified"] = True

                transactions.append(txn)

            if is_primary_container:
                primary_running_bal = container_running_bal

        if len(seen_accounts) > 1:
            metadata["continuity_warnings"].append(
                f"Multi-account CAMT file detected ({len(seen_accounts)} accounts); statement summary metadata reflects primary account {primary_acct}."
            )
            metadata["detected_mapping"]["multi_account_count"] = len(seen_accounts)

        primary_txns = [
            t for t in transactions
            if (not primary_acct) or (t.get("account_number") == primary_acct)
        ] or transactions

        if primary_txns:
            if not metadata["start_date"]:
                metadata["start_date"] = min(t["booking_date"] for t in primary_txns)
            if not metadata["end_date"]:
                metadata["end_date"] = max(t["booking_date"] for t in primary_txns)
            if not has_closing_bal:
                metadata["closing_balance"] = primary_running_bal
        elif not metadata["start_date"] and cre_dt_fallback is not None:
            metadata["start_date"] = cre_dt_fallback

        return transactions, metadata

    # ═══════════════════════════════════════════════════════════════════
    #  LIVING FOUNDATIONAL KNOWLEDGE SEEDER (Day-1 Intelligence in DB)
    # ═══════════════════════════════════════════════════════════════════

    FOUNDATIONAL_COLLABORATIVE_SEEDS: List[Dict[str, Any]] = [
        # Bank Charges & SWIFT Fees
        {"signature": "CHARGES COMMISSION SWIFT", "direction": "DEBIT", "amount_tier": "MICRO", "category": "BANK_CHARGES", "gl": "BANK_CHARGES"},
        {"signature": "CHARGES COMMISSION TRANSFER", "direction": "DEBIT", "amount_tier": "SMALL", "category": "BANK_CHARGES", "gl": "BANK_CHARGES"},
        {"signature": "ACCOUNT MAINTENANCE SERVICE", "direction": "DEBIT", "amount_tier": "MICRO", "category": "BANK_CHARGES", "gl": "BANK_CHARGES"},
        {"signature": "رسوم تحويل عمولة", "direction": "DEBIT", "amount_tier": "MICRO", "category": "BANK_CHARGES", "gl": "BANK_CHARGES"},
        {"signature": "ادارية بنكية مصاريف", "direction": "DEBIT", "amount_tier": "MICRO", "category": "BANK_CHARGES", "gl": "BANK_CHARGES"},
        # Taxes & Stamp Duty
        {"signature": "DUTY PROPORTIONAL STAMP", "direction": "DEBIT", "amount_tier": "SMALL", "category": "TAX_DEDUCTION", "gl": "TAX_EXPENSE"},
        {"signature": "DEDUCTION TAX WITHHOLDING", "direction": "DEBIT", "amount_tier": "MEDIUM", "category": "TAX_DEDUCTION", "gl": "TAX_EXPENSE"},
        {"signature": "دمغة ضريبة نسبية", "direction": "DEBIT", "amount_tier": "SMALL", "category": "TAX_DEDUCTION", "gl": "TAX_EXPENSE"},
        # Government & E-Finance
        {"signature": "CPS EFINANCE PAYMENT", "direction": "DEBIT", "amount_tier": "LARGE", "category": "GOVERNMENT_PAYMENT", "gl": "GOVERNMENT_AND_TAX_PAYABLE"},
        {"signature": "AUTHORITY INSURANCE SOCIAL", "direction": "DEBIT", "amount_tier": "MEDIUM", "category": "GOVERNMENT_PAYMENT", "gl": "GOVERNMENT_AND_TAX_PAYABLE"},
        {"signature": "اجتماعية تامينات هيئة", "direction": "DEBIT", "amount_tier": "MEDIUM", "category": "GOVERNMENT_PAYMENT", "gl": "GOVERNMENT_AND_TAX_PAYABLE"},
        # Payroll
        {"signature": "BATCH DISBURSEMENT PAYROLL", "direction": "DEBIT", "amount_tier": "LARGE", "category": "SALARY_PAYROLL", "gl": "SALARY_PAYROLL"},
        {"signature": "اجور رواتب مرتبات", "direction": "DEBIT", "amount_tier": "LARGE", "category": "SALARY_PAYROLL", "gl": "SALARY_PAYROLL"},
        # POS & Merchant Collections
        {"signature": "MERCHANT POS SETTLEMENT", "direction": "CREDIT", "amount_tier": "MEDIUM", "category": "POS_MERCHANT_SETTLEMENT", "gl": "ACCOUNTS_RECEIVABLE_POS"},
        {"signature": "البيع تسوية نقاط", "direction": "CREDIT", "amount_tier": "MEDIUM", "category": "POS_MERCHANT_SETTLEMENT", "gl": "ACCOUNTS_RECEIVABLE_POS"},
        # Treasury LG Operations
        {"signature": "CASH GUARANTEE MARGIN", "direction": "DEBIT", "amount_tier": "LARGE", "category": "LETTER_OF_GUARANTEE", "gl": "LG_CASH_MARGIN"},
        {"signature": "خطاب ضمان غطاء", "direction": "DEBIT", "amount_tier": "LARGE", "category": "LETTER_OF_GUARANTEE", "gl": "LG_CASH_MARGIN"},
        {"signature": "COMMISSION GUARANTEE ISSUANCE", "direction": "DEBIT", "amount_tier": "SMALL", "category": "BANK_CHARGES", "gl": "BANK_CHARGES_LG"},
        # Bounced / Returned Collections
        {"signature": "BOUNCED CHEQUE RETURNED", "direction": "DEBIT", "amount_tier": "MEDIUM", "category": "REVERSED_COLLECTION", "gl": "ACCOUNTS_RECEIVABLE"},
        {"signature": "شيك مرتد مرفوض", "direction": "DEBIT", "amount_tier": "MEDIUM", "category": "REVERSED_COLLECTION", "gl": "ACCOUNTS_RECEIVABLE"},
        # Sweeps & Cash Pooling
        {"signature": "ACCOUNT OWN SWEEP TRANSFER", "direction": "DEBIT", "amount_tier": "LARGE", "category": "INTER_BANK_SWEEP", "gl": "INTERNAL_TRANSFER"},
        {"signature": "ACCOUNT OWN SWEEP TRANSFER", "direction": "CREDIT", "amount_tier": "LARGE", "category": "INTER_BANK_SWEEP", "gl": "INTERNAL_TRANSFER"},
    ]

    FOUNDATIONAL_TENANT_RULES: List[Dict[str, Any]] = [
        {
            "rule_name": "Standard: SWIFT MT940 Returned/Bounced Item (NRTI)",
            "tokens": ["NRTI"],
            "direction": "DEBIT",
            "category": "REVERSED_COLLECTION",
            "sub_category": "BOUNCED_CHEQUE_OR_RETURNED_ITEM",
            "gl_account": "ACCOUNTS_RECEIVABLE",
            "priority": 10,
            "confidence": 96,
        },
        {
            "rule_name": "Standard: LG Cash Margin Block/Release (غطاء خطاب ضمان)",
            "tokens_or": ["LG MARGIN", "CASH MARGIN", "غطاء خطاب ضمان", "تعلية غطاء"],
            "direction": "EITHER",
            "category": "LETTER_OF_GUARANTEE",
            "sub_category": "LG_CASH_MARGIN",
            "gl_account": "LG_CASH_MARGIN",
            "priority": 15,
            "confidence": 94,
        },
        {
            "rule_name": "Standard: LG Issuance/Extension Commission (عمولة خطاب ضمان)",
            "tokens_or": ["LG COMM", "GUARANTEE COMMISSION", "عمولة خطاب ضمان", "مصاريف خطاب ضمان"],
            "direction": "DEBIT",
            "category": "BANK_CHARGES",
            "sub_category": "LG_COMMISSION_FEE",
            "gl_account": "BANK_CHARGES_LG",
            "amount_max": 50000.0,
            "priority": 16,
            "confidence": 93,
        },
        {
            "rule_name": "Standard: POS & Card Merchant Batch Settlement (حصيلة نقاط البيع)",
            "tokens_or": ["POS SETTLEMENT", "MERCHANT SETTLEMENT", "PMNT-MCOP", "حصيلة نقاط البيع", "تسوية نقاط البيع"],
            "direction": "CREDIT",
            "category": "POS_MERCHANT_SETTLEMENT",
            "sub_category": "CARD_ACQUIRING_SETTLEMENT",
            "gl_account": "ACCOUNTS_RECEIVABLE_POS",
            "priority": 18,
            "confidence": 94,
        },
        {
            "rule_name": "Standard: E-Finance / CPS Government Settlement (إي فاينانس)",
            "tokens_or": ["E-FINANCE", "EFINANCE", "CPS PAYMENT", "اي فاينانس", "إي فاينانس"],
            "direction": "DEBIT",
            "category": "GOVERNMENT_PAYMENT",
            "sub_category": "EFINANCE_CPS_SETTLEMENT",
            "gl_account": "GOVERNMENT_AND_TAX_PAYABLE",
            "priority": 20,
            "confidence": 94,
        },
        {
            "rule_name": "Standard: Time Deposit Placement / Maturity (وديعة لأجل)",
            "tokens_or": ["TIME DEPOSIT", "TERM DEPOSIT", "LDAS-FTDP", "ربط وديعة", "استحقاق وديعة"],
            "direction": "EITHER",
            "category": "TIME_DEPOSIT",
            "sub_category": "FIXED_TERM_DEPOSIT",
            "gl_account": "TIME_DEPOSITS",
            "priority": 22,
            "confidence": 92,
        },
        {
            "rule_name": "Standard: FX Spot / Currency Conversion (تدبير/تغيير عملة)",
            "tokens_or": ["FX DEAL", "FX SPOT", "NFEX", "تغيير عملة", "تدبير عملة"],
            "direction": "EITHER",
            "category": "FX_CONVERSION",
            "sub_category": "FX_SPOT_CONVERSION",
            "gl_account": "FX_CLEARING",
            "priority": 24,
            "confidence": 91,
        },
    ]

    def ensure_foundational_knowledge_seeded(
        self, db: Session, customer_id: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Idempotently seeds global collaborative patterns and tenant foundational rules
        into PostgreSQL so the module is smart from Day 1 and evolves online over time.
        """
        collab_added = 0
        rules_added = 0

        try:
            for item in self.FOUNDATIONAL_COLLABORATIVE_SEEDS:
                exists = db.query(CollaborativePattern).filter(
                    CollaborativePattern.signature_token == item["signature"],
                    CollaborativePattern.direction == item["direction"],
                    CollaborativePattern.amount_tier == item["amount_tier"],
                ).first()
                if not exists:
                    db.add(
                        CollaborativePattern(
                            bank_id=None,
                            signature_token=item["signature"],
                            direction=item["direction"],
                            amount_tier=item["amount_tier"],
                            suggested_category=item["category"],
                            suggested_gl_account=item["gl"],
                            distinct_tenants_count=5,
                            total_confirmations=15,
                            rejection_count=0,
                            agreement_score=Decimal("100.00"),
                            confidence_score=92,
                            is_promoted=True,
                        )
                    )
                    collab_added += 1

            if customer_id:
                for r_spec in self.FOUNDATIONAL_TENANT_RULES:
                    sig_hash = hashlib.sha256(
                        f"FOUNDATIONAL|{r_spec['rule_name']}|{r_spec['direction']}".encode("utf-8")
                    ).hexdigest()[:40]
                    existing_rule = db.query(ClassificationRule).filter(
                        ClassificationRule.company_id == customer_id,
                        ClassificationRule.signature_hash == sig_hash,
                        ClassificationRule.is_deleted == False,
                    ).first()
                    if not existing_rule:
                        conds: List[Dict[str, Any]] = []
                        if r_spec.get("tokens"):
                            for tok in r_spec["tokens"]:
                                conds.append({"field": "raw_description", "operator": "contains", "value": tok, "joiner": "AND"})
                        elif r_spec.get("tokens_or"):
                            or_sub = [
                                {"field": "raw_description", "operator": "contains", "value": tok, "joiner": "OR"}
                                for tok in r_spec["tokens_or"]
                            ]
                            conds.append({"conditions": or_sub, "joiner": "AND"})

                        if r_spec["direction"] == "CREDIT":
                            conds.append({"field": "credit_amount", "operator": "gt", "value": 0, "joiner": "AND"})
                        elif r_spec["direction"] == "DEBIT":
                            conds.append({"field": "debit_amount", "operator": "gt", "value": 0, "joiner": "AND"})

                        comp_json: Dict[str, Any] = {
                            "conditions": conds,
                            "direction": r_spec["direction"],
                            "exclude_tokens": [],
                        }
                        if r_spec.get("amount_max"):
                            comp_json["amount_max"] = r_spec["amount_max"]

                        db.add(
                            ClassificationRule(
                                company_id=customer_id,
                                bank_id=None,
                                rule_name=r_spec["rule_name"],
                                rule_type="FOUNDATIONAL_STANDARD",
                                rule_origin="FOUNDATIONAL_ONTOLOGY",
                                lifecycle_state="AUTO_CONFIRMED",
                                signature_hash=sig_hash,
                                priority=r_spec["priority"],
                                stop_after_match=True,
                                conditions_json=comp_json,
                                assigned_gl_account=r_spec["gl_account"],
                                assigned_category=r_spec["category"],
                                assigned_sub_category=r_spec.get("sub_category"),
                                usage_count=0,
                                confirmations_count=5,
                                overrides_count=0,
                                empirical_precision=Decimal("100.00"),
                                calibrated_confidence=r_spec["confidence"],
                                is_active=True,
                            )
                        )
                        rules_added += 1

            if collab_added > 0 or rules_added > 0:
                db.commit()
        except Exception as e:
            db.rollback()
            logger.debug(f"Foundational knowledge seeding skipped: {e}")

        return {"collaborative_patterns_seeded": collab_added, "foundational_rules_seeded": rules_added}


banking_standards_ontology = BankingStandardsOntology()
