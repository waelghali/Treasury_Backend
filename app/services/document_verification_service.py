# app/services/document_verification_service.py
import re
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)

import importlib.util

FITZ_AVAILABLE = bool(importlib.util.find_spec("fitz"))
if not FITZ_AVAILABLE:
    logger.warning("PyMuPDF (fitz) is not installed. PDF text extraction will fall back to OCR.")

# Default calendar convention tolerance window in days (e.g. 90-day vs 3-month calendar quarter / weekend bump)
DEFAULT_DATE_TOLERANCE_DAYS = 3


def _normalize_date(date_val) -> Optional[str]:
    """Normalize date or string to YYYY-MM-DD."""
    if not date_val:
        return None
    if isinstance(date_val, (datetime, date)):
        return date_val.strftime("%Y-%m-%d")
    s = str(date_val).replace("/", "-").strip()
    match = re.search(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', s)
    if match:
        y, m, d = match.groups()
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    match_dmy = re.search(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})', s)
    if match_dmy:
        d, m, y = match_dmy.groups()
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return s[:10] if len(s) >= 10 else None


def _extract_lg_numbers_from_text(text: str) -> List[str]:
    """Extract potential Letter of Guarantee reference numbers from document text."""
    if not text:
        return []
    patterns = [
        r'\bLG-[A-Za-z0-9_-]+\b',
        r'(?:Letter of Guarantee|LG|Guarantee|خطاب ضمان|خطاب الضمان)[\s\S]{0,25}?(?:[:#]|No\.?|Ref\.?|رقم)[\s:]*([A-Za-z0-9_\-\/]{4,30})',
    ]
    results = set()
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            val = m.group(1) if m.groups() else m.group(0)
            clean = val.strip().strip(':').strip('#').strip()
            if len(clean) >= 4 and clean.lower() not in ('letter', 'guarantee', 'number', 'ref'):
                results.add(clean.upper())
    return list(results)


def _extract_instruction_serials_from_text(text: str) -> List[str]:
    """Extract corporate instruction serial numbers or advice references from text."""
    if not text:
        return []
    patterns = [
        r'\b[A-Z0-9]{2,10}-\d{4}-[A-Z]{3}-\d{2}-[A-Z]-\d{4}\b',
        r'\bINST-[A-Za-z0-9_-]+\b',
        r'(?:Instruction|Reference|Your Ref|Our Ref|إشارة إلى خطابكم|خطابكم رقم|مرجع)[\s\S]{0,20}?(?:[:#]|No\.?|Ref\.?|رقم)[\s:]*([A-Za-z0-9_\-\/]{4,35})',
    ]
    found = []
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            val = m.group(1) if m.groups() else m.group(0)
            clean = val.strip().strip(':').strip('#').strip()
            if len(clean) >= 4 and clean.upper() not in found:
                found.append(clean.upper())
    return found


def _extract_bank_names_from_text(text: str) -> List[str]:
    """Detect Egyptian and regional commercial banks mentioned in document text."""
    if not text:
        return []
    text_lower = text.lower()
    bank_map = {
        "HSBC Bank": ["hsbc", "اتش اس بي سي", "بنك اتش اس بي سي"],
        "Faisal Islamic Bank": ["faisal islamic bank", "بنك فيصل", "فيصل الإسلامي", "faisal"],
        "Commercial International Bank (CIB)": ["commercial international bank", "البنك التجاري الدولي", "cib"],
        "National Bank of Egypt (NBE)": ["national bank of egypt", "البنك الأهلي المصري", "البنك الاهلي", "nbe"],
        "Banque Misr": ["banque misr", "بنك مصر", "misr"],
        "QNB": ["qnb", "قطر الوطني"],
        "Alexbank": ["alexbank", "bank of alexandria", "بنك الاسكندرية", "بنك الإسكندرية"],
        "ADIB": ["adib", "abu dhabi islamic bank", "أبوظبي الإسلامي"],
        "AAIB": ["aaib", "arab african international bank", "العربي الافريقي"],
        "Banque du Caire": ["banque du caire", "بنك القاهرة"],
        "SAIB": ["saib", "بنك الشركة المصرفية"],
        "EGBank": ["egbank", "البنك المصري الخليجي"],
        "FAB": ["fab", "first abu dhabi", "بنك أبوظبي الأول"],
        "ABK": ["abk", "al ahli bank of kuwait", "البنك الأهلي الكويتي"],
    }
    found = []
    for b_name, aliases in bank_map.items():
        if any(a in text_lower for a in aliases):
            found.append(b_name)
    return found


def _check_bank_name_match(
    detected: str,
    expected: str,
    short_name: Optional[str] = None,
    alternate_names: Optional[List[str]] = None,
) -> bool:
    """Fuzzy check if detected bank name matches expected bank name or any advising/communication bank."""
    if not detected or not expected:
        return True

    candidates = [expected]
    if short_name:
        candidates.append(short_name)
    if alternate_names:
        candidates.extend([n for n in alternate_names if n])

    det_lower = detected.lower()

    aliases = {
        "cib": ["commercial international bank", "البنك التجاري الدولي", "cib"],
        "nbe": ["national bank of egypt", "البنك الاهلي المصري", "البنك الأهلي المصري", "nbe", "الأهلي"],
        "banque misr": ["banque misr", "بنك مصر", "misr"],
        "qnb": ["qnb alahli", "qnb", "قطر الوطني", "بنك قطر الوطني"],
        "alexbank": ["bank of alexandria", "بنك الاسكندرية", "بنك الإسكندرية", "alexbank", "alex bank"],
        "adib": ["abu dhabi islamic bank", "مصرف أبوظبي الإسلامي", "adib"],
        "faisal": ["faisal islamic bank", "بنك فيصل الإسلامي", "بنك فيصل", "فيصل", "fais"],
        "hsbc": ["hsbc", "ايه بي سي", "بنك اتش اس بي سي", "اتش اس بي سي"],
        "aaib": ["arab african international bank", "البنك العربي الافريقي الدولي", "aaib"],
    }

    for cand in candidates:
        cand_lower = cand.lower().strip()
        if not cand_lower:
            continue
        if cand_lower in det_lower or det_lower in cand_lower:
            return True
        for key, alias_list in aliases.items():
            if any(a in cand_lower for a in alias_list) and any(a in det_lower for a in alias_list):
                return True

    return False


def _normalize_instruction_purpose(inst_type: Optional[str]) -> Tuple[str, str]:
    """
    Normalize an instruction/action type or AI-detected reply purpose into
    (canonical_purpose, human_readable_label).
    """
    if not inst_type:
        return ("UNKNOWN", "Unknown")
    upper = str(inst_type).strip().upper()
    if any(k in upper for k in ("EXTEND", "EXTENSION", "RENEW")):
        return ("EXTENSION", "Extension")
    if any(k in upper for k in ("DECREASE", "REDUC", "PARTIAL_RELEASE", "PARTIAL RELEASE")):
        return ("DECREASE_AMOUNT", "Decrease Amount")
    if any(k in upper for k in ("INCREASE",)):
        return ("INCREASE_AMOUNT", "Increase Amount")
    if any(k in upper for k in ("CANCEL_LIQUIDATION",)):
        return ("CANCEL_LIQUIDATION", "Cancel Liquidation")
    if any(k in upper for k in ("LIQUIDAT", "CLAIM")):
        return ("LIQUIDATION", "Liquidation")
    if any(k in upper for k in ("RELEASE", "DISCHARGE", "CANCEL")):
        return ("RELEASE", "Release")
    if any(k in upper for k in ("ACTIVAT", "OPERATIVE")):
        return ("ACTIVATION", "Activation")
    if any(k in upper for k in ("AMEND",)):
        return ("AMENDMENT", "Amendment")
    return (upper, inst_type.replace("LG_", "").replace("_", " ").title())


class DocumentVerificationService:
    """
    AI Document Verification Service for LG Lifecycle Events:
    1. Delivery Evidence: Bank incoming receiving stamp & date validation + LG matching.
    2. Bank Reply: Bank branch seal, terms matching, Red Security Stamp detection + LG matching.
    3. Bank Expiry Date Tolerance Alignment (Calendar vs. 90-Day Convention):
       Classifies small variations (<= 3 days) as DATE_CONVENTION_VARIANCE with 1-click user alignment.
    4. Multi-Instruction Bank Reply Disambiguation:
       Cross-validates uploaded bank replies against all pending instructions for an LG and provides
       1-click redirection if the user selected the wrong instruction card.
    """

    DATE_TOLERANCE_DAYS: int = DEFAULT_DATE_TOLERANCE_DAYS

    @staticmethod
    def extract_text(file_bytes: bytes, mime_type: str, file_name: str = "document") -> str:
        """Extract text from PDF using PyMuPDF if possible, else empty for OCR fallback."""
        if not file_bytes:
            return ""

        if mime_type == "application/pdf" and FITZ_AVAILABLE:
            doc = None
            try:
                import fitz
                doc = fitz.open(stream=file_bytes, filetype="pdf")
                pages_text = [page.get_text() for page in doc]
                extracted = "\n".join(pages_text).strip()
                if len(extracted) > 40:
                    logger.info(f"Fast PDF extraction succeeded: {len(extracted)} chars from {file_name}")
                    return extracted
            except Exception as e:
                logger.warning(f"Fast PDF extraction failed: {e}")
            finally:
                if doc is not None:
                    try:
                        doc.close()
                    except Exception:
                        pass
                del doc
                import gc
                gc.collect()

        # OCR fallback using Google Vision if available
        try:
            from app.core.ai_integration import (
                perform_ocr_with_google_vision,
                _upload_to_gcs,
                _cleanup_gcs_files,
                GCS_BUCKET_NAME,
                _convert_pdf_to_images_and_upload_to_gcs,
            )
            import uuid
            import asyncio

            if not GCS_BUCKET_NAME:
                return ""

            unique_id = f"verify_{uuid.uuid4().hex[:8]}"

            def _run_sync(coro):
                loop = asyncio.new_event_loop()
                try:
                    return loop.run_until_complete(coro)
                finally:
                    loop.close()

            if mime_type.startswith("image/"):
                ext = mime_type.split("/")[-1]
                blob_name = f"lg_scans_temp/{unique_id}/img.{ext}"
                gcs_uri = _run_sync(_upload_to_gcs(GCS_BUCKET_NAME, blob_name, file_bytes, mime_type))
                if gcs_uri:
                    text = _run_sync(perform_ocr_with_google_vision(gcs_uri, unique_id)) or ""
                    _run_sync(_cleanup_gcs_files(GCS_BUCKET_NAME, f"lg_scans_temp/{unique_id}/"))
                    return text
            elif mime_type == "application/pdf":
                uris = _run_sync(_convert_pdf_to_images_and_upload_to_gcs(file_bytes, GCS_BUCKET_NAME, unique_id))
                texts = []
                for uri in uris:
                    page_text = _run_sync(perform_ocr_with_google_vision(uri, unique_id))
                    if page_text:
                        texts.append(page_text)
                _run_sync(_cleanup_gcs_files(GCS_BUCKET_NAME, f"lg_scans_temp/{unique_id}/"))
                return "\n".join(texts)
        except Exception as e:
            logger.warning(f"OCR fallback failed: {e}")
        finally:
            import gc
            gc.collect()

        return ""

    @staticmethod
    def _check_duplicate_scan(
        file_sha256: Optional[str],
        existing_doc_hashes: Optional[List[Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:
        """Check if the uploaded file's SHA-256 fingerprint matches an already uploaded document on this LG."""
        if not file_sha256 or not existing_doc_hashes:
            return None
        for entry in existing_doc_hashes:
            if entry and entry.get("file_sha256") == file_sha256:
                doc_type_label = entry.get("document_type_label") or entry.get("document_type") or "Document"
                inst_serial = entry.get("instruction_serial")
                inst_seq = entry.get("instruction_seq")
                inst_type_label = entry.get("instruction_type_label")
                if not inst_type_label and entry.get("instruction_type"):
                    _, inst_type_label = _normalize_instruction_purpose(entry.get("instruction_type"))
                if inst_type_label and inst_serial:
                    loc_str = f" for the {inst_type_label} instruction ({inst_serial})"
                elif inst_serial:
                    loc_str = f" for instruction {inst_serial}"
                else:
                    loc_str = ""
                msg = (
                    f"Duplicate Scan Detected: This exact file was already uploaded as "
                    f"'{doc_type_label}'{loc_str}."
                )
                return {
                    "file_sha256": file_sha256,
                    "previous_document_type": entry.get("document_type"),
                    "previous_document_type_label": doc_type_label,
                    "previous_instruction_id": entry.get("instruction_id"),
                    "previous_instruction_serial": inst_serial,
                    "previous_instruction_seq": inst_seq,
                    "previous_instruction_type_label": inst_type_label,
                    "prompt_message": msg,
                }
        return None

    @classmethod
    def verify_delivery_evidence(
        cls,
        raw_text: str,
        expected_bank_name: str,
        expected_bank_short_name: Optional[str],
        instruction_issue_date: date,
        instruction_serial: str,
        lg_number: str,
        user_delivery_date: Optional[Any] = None,
        file_sha256: Optional[str] = None,
        existing_doc_hashes: Optional[List[Dict[str, Any]]] = None,
        alternate_bank_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Verifies Delivery Evidence document:
        1. Duplicate Scan Check (SHA-256): Flags if the exact scan was already uploaded on this LG.
        2. LG Number Matching: Ensures the document actually relates to this specific LG.
        3. Counterparty Bank Matching: Verifies the counterparty matches the instructed issuing/advising bank.
        4. Document Relevance: Ensures user didn't attach an unrelated document (e.g. original LG or invoice).
        5. Bank Incoming Stamp Presence: Corporate instruction letter returned with teller receiving stamp.
        6. Chronological Integrity & Delivery Stamp Date Alignment:
           - Receipt Date >= Instruction Date.
           - User-entered delivery_date == Bank receiving stamp date (with 1-click alignment if different).
        """
        norm_issue_date_str = _normalize_date(instruction_issue_date)
        norm_user_delivery_str = _normalize_date(user_delivery_date)

        extracted_data = cls._call_gemini_delivery_prompt(
            raw_text, expected_bank_name, norm_issue_date_str, instruction_serial, lg_number
        )

        if not extracted_data:
            extracted_data = cls._heuristic_delivery_extraction(raw_text, norm_issue_date_str)

        matches = []
        mismatches = []
        delivery_date_variance: Optional[Dict[str, Any]] = None

        # --- 0. Duplicate Scan Guard (SHA-256) ---
        duplicate_scan_info = cls._check_duplicate_scan(file_sha256, existing_doc_hashes)
        if duplicate_scan_info:
            mismatches.append(duplicate_scan_info["prompt_message"])

        is_bank_stamp = extracted_data.get("is_bank_stamp_detected", False)
        is_courier = extracted_data.get("is_courier_receipt", False)
        detected_bank = extracted_data.get("detected_bank_name")
        detected_date_str = _normalize_date(
            extracted_data.get("detected_stamp_date") or extracted_data.get("courier_delivery_date")
        )

        # --- 1. LG Number & Reference Matching Check ---
        expected_lg_clean = lg_number.strip().upper() if lg_number else ""
        detected_lgs = extracted_data.get("detected_lg_numbers") or []
        heuristic_lgs = _extract_lg_numbers_from_text(raw_text)
        all_detected_lgs = list(dict.fromkeys([l.upper() for l in detected_lgs + heuristic_lgs if l]))

        mismatched_lgs = [l for l in all_detected_lgs if expected_lg_clean and l != expected_lg_clean]
        exact_lg_match = any(l == expected_lg_clean for l in all_detected_lgs) if expected_lg_clean else False

        if mismatched_lgs and not exact_lg_match:
            mismatches.append(
                f"LG Number Mismatch: Document references '{mismatched_lgs[0]}', but this instruction is for '{lg_number}'."
            )
        elif exact_lg_match:
            matches.append(f"LG Number matched: references '{lg_number}'.")

        # --- 2. Document Relevance & Type Check ---
        doc_type = extracted_data.get("detected_document_type")
        if doc_type == "ORIGINAL_LG_LETTER":
            mismatches.append(
                "Document Type Mismatch: Attached document appears to be an Original LG Issuance Letter, not the stamped delivery copy or courier slip for this instruction."
            )
        elif doc_type == "UNRELATED_DOCUMENT":
            mismatches.append(
                "Document Irrelevance: Attached document does not appear to be related to banking delivery evidence."
            )

        # --- 3. Counterparty Bank Check ---
        if not detected_bank:
            heuristic_banks = _extract_bank_names_from_text(raw_text)
            if heuristic_banks:
                detected_bank = heuristic_banks[0]

        if detected_bank and expected_bank_name:
            if _check_bank_name_match(
                detected_bank, expected_bank_name, expected_bank_short_name, alternate_names=alternate_bank_names
            ):
                matches.append(f"Bank identity verified: {detected_bank}.")
            else:
                mismatches.append(
                    f"Bank Mismatch: Document belongs to '{detected_bank}', but expected issuing bank is '{expected_bank_name}'."
                )

        # --- 4. Incoming Bank Stamp Presence ---
        if not is_bank_stamp and not is_courier:
            mismatches.append("No official bank receiving stamp (خاتم وارد / استلام الفرع) or courier delivery slip detected on the scan.")
        elif is_bank_stamp:
            branch_info = f", {extracted_data['detected_branch_name']}" if extracted_data.get("detected_branch_name") else ""
            matches.append(f"Bank receiving stamp detected ({detected_bank or 'Bank Stamp'}{branch_info}).")
        elif is_courier:
            matches.append(f"Courier delivery slip detected ({extracted_data.get('courier_name') or 'Registered Mail / علم وصول'}).")

        # --- 5. Chronology Check: Receipt Date >= Instruction Date, <= Today & User Delivery Date vs Stamp Date ---
        is_stamp_date_invalid = False
        stamp_invalid_reason = None
        today_dt = date.today()
        today_str = today_dt.isoformat()
        if detected_date_str:
            try:
                rec_dt = datetime.strptime(detected_date_str, "%Y-%m-%d").date()
                if rec_dt > today_dt:
                    is_stamp_date_invalid = True
                    stamp_invalid_reason = f"Cannot align to {detected_date_str}: Stamp date is in the future (today is {today_str})."
                    mismatches.append(
                        f"Chronology conflict: Stamp receipt date ({detected_date_str}) is in the future (after today {today_str})."
                    )
                elif norm_issue_date_str:
                    iss_dt = datetime.strptime(norm_issue_date_str, "%Y-%m-%d").date()
                    # Allow 1-day grace when instruction and delivery are tested on the same day
                    if rec_dt < (iss_dt - timedelta(days=1)):
                        is_stamp_date_invalid = True
                        stamp_invalid_reason = f"Cannot align to {detected_date_str}: Stamp date is earlier than instruction issuance date ({norm_issue_date_str})."
                        mismatches.append(
                            f"Chronology conflict: Stamp receipt date ({detected_date_str}) is earlier than instruction issuance date ({norm_issue_date_str})."
                        )
                    else:
                        matches.append(f"Receipt date verified: {detected_date_str} (on/after issuance date {norm_issue_date_str}).")
            except Exception:
                pass
        elif is_bank_stamp and not detected_date_str:
            mismatches.append("Bank stamp detected, but the stamped date is faint or illegible. Please inspect.")

        if detected_date_str and norm_user_delivery_str:
            if detected_date_str == norm_user_delivery_str:
                matches.append(f"Entered delivery date matches bank receiving stamp date ({detected_date_str}).")
            else:
                stamp_mismatch_msg = (
                    f"Delivery date mismatch: You entered {norm_user_delivery_str}, "
                    f"but the bank receiving stamp date on the scan is {detected_date_str}."
                )
                mismatches.append(stamp_mismatch_msg)
                delivery_date_variance = {
                    "classification": "DELIVERY_STAMP_DATE_MISMATCH",
                    "user_delivery_date": norm_user_delivery_str,
                    "detected_stamp_date": detected_date_str,
                    "prompt_message": stamp_mismatch_msg,
                    "can_align": not is_stamp_date_invalid,
                    "invalid_reason": stamp_invalid_reason,
                }

        status = "verified" if len(mismatches) == 0 else "mismatch"

        import gc
        gc.collect()

        return {
            "status": status,
            "matches": matches,
            "mismatches": mismatches,
            "detected_stamp_date": detected_date_str,
            "delivery_date_variance": delivery_date_variance,
            "duplicate_scan_detected": duplicate_scan_info,
            "file_sha256": file_sha256,
            "extracted_data": extracted_data,
            "summary": extracted_data.get("confidence_summary") or ("Evidence matches expected delivery terms." if status == "verified" else "Discrepancies require user confirmation.")
        }

    @classmethod
    def verify_bank_reply(
        cls,
        raw_text: str,
        expected_bank_name: str,
        expected_bank_short_name: Optional[str],
        action_type: str,
        expected_new_expiry: Optional[Any],
        expected_new_amount: Optional[float],
        lg_number: str,
        target_instruction: Optional[Dict[str, Any]] = None,
        pending_instructions: Optional[List[Dict[str, Any]]] = None,
        date_tolerance_days: Optional[int] = None,
        user_reply_date: Optional[Any] = None,
        file_sha256: Optional[str] = None,
        existing_doc_hashes: Optional[List[Dict[str, Any]]] = None,
        alternate_bank_names: Optional[List[str]] = None,
        _pre_extracted_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Verifies Bank Reply document:
        0. Duplicate Scan Guard (SHA-256): Flags if the exact file was already uploaded on this LG.
        1. LG Number Matching: Ensures the reply refers to the correct guarantee.
        2. Counterparty Bank Check: Matches the issuing or advising/communication bank.
        3. Multi-Instruction Disambiguation (Item 2): Checks if the reply actually belongs to a sibling pending instruction.
        4. Bank Seal: Checks for official branch seal/stamp.
        5. Red Security Stamp: For Original LG or Amount Increase, checks for the mandatory Red Value Stamp.
        6. Expiry Terms & Calendar Convention Tolerance (Item 1):
           Validates updated expiry date against requested date, classifying <= 3 calendar days difference
           as DATE_CONVENTION_VARIANCE rather than a hard defect.
        7. Amount Terms Verification & Bank-Modified Terms Escalation:
           When the bank alters the expiry date (> 3 days) or amount compared to the corporate instruction,
           populates `bank_terms_modification` so the End User can submit a Maker-Checker Admin Approval Request.
        8. Bank Reply Letter Date Capture & Chronology Check:
           Extracts `bank_reply_letter_date` (`cited_effective_date`); never requires `user_reply_date` (received date)
           to equal the bank letter date, but flags a chronology conflict if `user_reply_date < bank_reply_letter_date`.
        """
        tolerance_days = cls.DATE_TOLERANCE_DAYS if date_tolerance_days is None else int(date_tolerance_days)
        norm_expiry_str = _normalize_date(expected_new_expiry)
        norm_user_reply_str = _normalize_date(user_reply_date)

        extracted_data = _pre_extracted_data or cls._call_gemini_bank_reply_prompt(
            raw_text, expected_bank_name, action_type, norm_expiry_str, expected_new_amount, lg_number,
            pending_instructions=pending_instructions
        )

        if not extracted_data:
            extracted_data = cls._heuristic_bank_reply_extraction(
                raw_text, action_type, norm_expiry_str, expected_new_amount
            )

        matches: List[str] = []
        mismatches: List[str] = []
        date_convention_variance: Optional[Dict[str, Any]] = None
        instruction_disambiguation: Optional[Dict[str, Any]] = None
        bank_terms_modification: Optional[Dict[str, Any]] = None

        # --- 0. Duplicate Scan Guard (SHA-256) ---
        duplicate_scan_info = cls._check_duplicate_scan(file_sha256, existing_doc_hashes)
        if duplicate_scan_info:
            mismatches.append(duplicate_scan_info["prompt_message"])

        is_seal = extracted_data.get("is_bank_seal_detected", False)
        detected_bank = extracted_data.get("detected_bank_name")
        is_red_stamp = extracted_data.get("is_red_security_stamp_detected", False)
        amended_expiry = _normalize_date(extracted_data.get("amended_expiry_date"))
        amended_amount = extracted_data.get("amended_amount")
        bank_reply_letter_date = _normalize_date(extracted_data.get("cited_effective_date"))

        # --- 1. LG Number Matching Check ---
        expected_lg_clean = lg_number.strip().upper() if lg_number else ""
        detected_lgs = extracted_data.get("detected_lg_numbers") or []
        heuristic_lgs = _extract_lg_numbers_from_text(raw_text)
        all_detected_lgs = list(dict.fromkeys([l.upper() for l in detected_lgs + heuristic_lgs if l]))

        mismatched_lgs = [l for l in all_detected_lgs if expected_lg_clean and l != expected_lg_clean]
        exact_lg_match = any(l == expected_lg_clean for l in all_detected_lgs) if expected_lg_clean else False

        if mismatched_lgs and not exact_lg_match:
            mismatches.append(
                f"LG Number Mismatch: Bank reply references '{mismatched_lgs[0]}', but this record is for '{lg_number}'."
            )
        elif exact_lg_match:
            matches.append(f"LG Number matched: references '{lg_number}'.")

        # --- 2. Bank Identity Check ---
        if not detected_bank:
            heuristic_banks = _extract_bank_names_from_text(raw_text)
            if heuristic_banks:
                detected_bank = heuristic_banks[0]

        if detected_bank and expected_bank_name:
            if _check_bank_name_match(
                detected_bank, expected_bank_name, expected_bank_short_name, alternate_names=alternate_bank_names
            ):
                matches.append(f"Bank identity verified: {detected_bank}.")
            else:
                mismatches.append(f"Bank Mismatch: Document issued by '{detected_bank}', but expected '{expected_bank_name}'.")

        # --- 2b. Document Type & Relevance Check ---
        detected_doc_type = extracted_data.get("detected_document_type")
        if detected_doc_type == "ORIGINAL_LG_LETTER" and action_type in (
            "LG_EXTENSION", "EXTEND", "EXTENSION", "LG_DECREASE_AMOUNT", "LG_LIQUIDATION", "LG_LIQUIDATE", "LG_RELEASE", "CANCEL_LIQUIDATION"
        ):
            mismatches.append(
                "Document Type Mismatch: Attached document appears to be an Original LG Issuance Letter, not a bank reply advice for this instruction."
            )

        # --- 2c. Multi-Instruction Bank Reply Disambiguation (Item 2) ---
        if target_instruction and pending_instructions and len(pending_instructions) > 1:
            instruction_disambiguation = cls._disambiguate_multi_instruction(
                raw_text=raw_text,
                extracted_data=extracted_data,
                target_instruction=target_instruction,
                pending_instructions=pending_instructions,
                tolerance_days=tolerance_days,
            )
            if instruction_disambiguation:
                mismatches.insert(0, instruction_disambiguation["prompt_message"])
                # Pre-compute AI verification for the matched sibling instruction in 0ms (reusing extracted_data)
                matched_inst_obj = next(
                    (
                        p for p in (pending_instructions or [])
                        if p.get("id") == instruction_disambiguation.get("matched_instruction_id")
                    ),
                    None,
                )
                if matched_inst_obj and _pre_extracted_data is None:
                    m_details = matched_inst_obj.get("details") or {}
                    m_exp_expiry = m_details.get("new_expiry_date") or m_details.get("requested_expiry_date")
                    m_exp_amount = (
                        m_details.get("new_amount")
                        or m_details.get("new_lg_amount")
                        or m_details.get("requested_amount")
                    )
                    if m_exp_amount is not None:
                        try:
                            m_exp_amount = float(m_exp_amount)
                        except Exception:
                            m_exp_amount = None
                    matched_verif = cls.verify_bank_reply(
                        raw_text=raw_text,
                        expected_bank_name=expected_bank_name,
                        expected_bank_short_name=expected_bank_short_name,
                        action_type=matched_inst_obj.get("instruction_type") or "",
                        expected_new_expiry=m_exp_expiry,
                        expected_new_amount=m_exp_amount,
                        lg_number=lg_number,
                        date_tolerance_days=tolerance_days,
                        target_instruction=matched_inst_obj,
                        pending_instructions=None,
                        user_reply_date=user_reply_date,
                        file_sha256=file_sha256,
                        existing_doc_hashes=existing_doc_hashes,
                        alternate_bank_names=alternate_bank_names,
                        _pre_extracted_data=extracted_data,
                    )
                    matched_verif["matches"] = [
                        f"Switched target instruction to {instruction_disambiguation.get('matched_type_label')} ({instruction_disambiguation.get('matched_serial')}).",
                        *(matched_verif.get("matches") or []),
                    ]
                    instruction_disambiguation["matched_verification"] = matched_verif

        # --- 3. Bank Seal Check ---
        if is_seal:
            matches.append(f"Official bank seal detected ({detected_bank or 'Issuing Bank'}).")
        else:
            mismatches.append("Official bank seal / branch stamp was not detected on the document scan.")

        # --- 4. Red Security Stamp for Original LG or Amount Increase ---
        is_amount_sensitive = action_type in ("LG_INCREASE_AMOUNT", "INCREASE_AMOUNT", "NEW_LG", "LG_CREATED", "ISSUANCE")
        if is_amount_sensitive:
            if is_red_stamp:
                matches.append("Bank Red Security Stamp (الختم الأحمر بالمبلغ / الختم البارز) detected and verified.")
            else:
                mismatches.append("Warning: Bank Red Security Stamp (الختم الأحمر بالمبلغ) not detected for this amount action.")

        # --- 5. Expiry Terms Check & Calendar Convention Tolerance (Item 1) ---
        target_purpose, _ = _normalize_instruction_purpose(action_type)
        modified_expiry_for_approval: Optional[str] = None
        modified_amount_for_approval: Optional[float] = None

        if target_purpose == "EXTENSION" and norm_expiry_str and not instruction_disambiguation:
            if amended_expiry:
                if amended_expiry == norm_expiry_str:
                    matches.append(f"Extension expiry date matches requested: {norm_expiry_str}.")
                else:
                    try:
                        bank_dt = datetime.strptime(amended_expiry, "%Y-%m-%d").date()
                        req_dt = datetime.strptime(norm_expiry_str, "%Y-%m-%d").date()
                        diff_days = (bank_dt - req_dt).days
                    except Exception:
                        diff_days = None

                    if diff_days is not None and 1 <= abs(diff_days) <= tolerance_days:
                        sign_str = f"+{diff_days}" if diff_days > 0 else f"{diff_days}"
                        day_word = "day" if abs(diff_days) == 1 else "days"
                        variance_msg = (
                            f"Bank confirmed expiry date as {amended_expiry} ({sign_str} {day_word} from requested date "
                            f"{norm_expiry_str} due to bank calendar convention). Do you accept aligning your record to the bank's confirmed date?"
                        )
                        date_convention_variance = {
                            "classification": "DATE_CONVENTION_VARIANCE",
                            "requested_expiry_date": norm_expiry_str,
                            "bank_confirmed_expiry_date": amended_expiry,
                            "variance_days": diff_days,
                            "tolerance_days": tolerance_days,
                            "favorable_to_beneficiary": diff_days > 0,
                            "prompt_message": variance_msg,
                        }
                    elif diff_days is not None and diff_days > tolerance_days:
                        variance_msg = (
                            f"Bank extended the LG to a further expiry date ({amended_expiry}, +{diff_days} days beyond requested {norm_expiry_str}). "
                            f"Because this provides additional guarantee coverage in the Beneficiary's favor at no extra cost, accepting the extended date is recommended."
                        )
                        date_convention_variance = {
                            "classification": "FAVORABLE_EXTENDED_EXPIRY",
                            "requested_expiry_date": norm_expiry_str,
                            "bank_confirmed_expiry_date": amended_expiry,
                            "variance_days": diff_days,
                            "tolerance_days": tolerance_days,
                            "favorable_to_beneficiary": True,
                            "prompt_message": variance_msg,
                        }
                        mismatches.append(
                            f"Favorable Extended Expiry (+{diff_days} days): bank advice grants coverage until {amended_expiry} (requested {norm_expiry_str})."
                        )
                        modified_expiry_for_approval = amended_expiry
                    else:
                        mismatches.append(f"Expiry date mismatch: bank advice states {amended_expiry}, expected {norm_expiry_str}.")
                        modified_expiry_for_approval = amended_expiry
            else:
                mismatches.append(f"New expiry date ({norm_expiry_str}) not clearly detected in the bank advice text.")

        # --- 6. Amount Terms Check (for Increases or Decreases) ---
        if expected_new_amount and amended_amount is not None and not instruction_disambiguation:
            if abs(float(amended_amount) - float(expected_new_amount)) < 1.0:
                matches.append(f"Amended amount verified: {expected_new_amount:,.2f}.")
            else:
                mismatches.append(f"Amount mismatch: bank advice specifies {amended_amount:,.2f}, expected {expected_new_amount:,.2f}.")
                modified_amount_for_approval = float(amended_amount)

        # --- 6b. Bank-Modified Terms Escalation (Requires Corporate Admin Maker-Checker Approval) ---
        if (modified_expiry_for_approval or modified_amount_for_approval is not None) and not instruction_disambiguation:
            amendment_details: Dict[str, Any] = {}
            summary_parts: List[str] = []
            is_favorable_ext = bool(
                date_convention_variance
                and date_convention_variance.get("favorable_to_beneficiary") is True
                and modified_amount_for_approval is None
            )
            favorable_days = date_convention_variance.get("variance_days") if date_convention_variance else None
            if modified_expiry_for_approval:
                amendment_details["expiry_date"] = modified_expiry_for_approval
                summary_parts.append(f"Expiry Date = {modified_expiry_for_approval} (requested {norm_expiry_str})")
            if modified_amount_for_approval is not None:
                amendment_details["lg_amount"] = modified_amount_for_approval
                summary_parts.append(
                    f"Amount = {modified_amount_for_approval:,.2f} (requested {float(expected_new_amount):,.2f})"
                )
            bank_terms_modification = {
                "classification": "FAVORABLE_EXTENDED_EXPIRY" if is_favorable_ext else "BANK_MODIFIED_TERMS_APPROVAL_REQUIRED",
                "is_favorable_extension": is_favorable_ext,
                "variance_days": favorable_days,
                "amendment_details": amendment_details,
                "requested_expiry_date": norm_expiry_str,
                "bank_confirmed_expiry_date": modified_expiry_for_approval,
                "requested_amount": float(expected_new_amount) if expected_new_amount is not None else None,
                "bank_confirmed_amount": modified_amount_for_approval,
                "prompt_message": (
                    f"Bank granted a further extension to {modified_expiry_for_approval} (+{favorable_days} days extra coverage beyond {norm_expiry_str}). "
                    f"Since this is in the Beneficiary's favor at no fee impact, we recommend accepting the extended date (Corporate Admin will be notified automatically)."
                    if is_favorable_ext
                    else f"Bank modified terms beyond standard tolerance ({'; '.join(summary_parts)}). "
                         f"Updating the LG's core terms requires Corporate Admin approval."
                ),
            }

        # --- 7. Bank Reply Letter Date Capture & Chronology Check ---
        if bank_reply_letter_date:
            today_dt = date.today()
            today_str = today_dt.isoformat()
            try:
                ltr_dt = datetime.strptime(bank_reply_letter_date, "%Y-%m-%d").date()
                if ltr_dt > today_dt:
                    mismatches.append(
                        f"Chronology conflict: Bank reply letter date ({bank_reply_letter_date}) is in the future (after today {today_str})."
                    )
                elif norm_user_reply_str:
                    usr_dt = datetime.strptime(norm_user_reply_str, "%Y-%m-%d").date()
                    if usr_dt < ltr_dt:
                        mismatches.append(
                            f"Chronology conflict: Recorded bank reply receipt date ({norm_user_reply_str}) is earlier than the bank letter issuance date ({bank_reply_letter_date})."
                        )
                    else:
                        matches.append(
                            f"Bank reply letter date extracted: {bank_reply_letter_date} (received on {norm_user_reply_str})."
                        )
                else:
                    matches.append(f"Bank reply letter date extracted: {bank_reply_letter_date}.")
            except Exception:
                matches.append(f"Bank reply letter date extracted: {bank_reply_letter_date}.")

        # Determine overall verification status
        if len(mismatches) > 0:
            status = "mismatch"
        elif date_convention_variance is not None:
            status = "date_variance"
        else:
            status = "verified"

        import gc
        gc.collect()

        if instruction_disambiguation:
            summary_text = instruction_disambiguation["prompt_message"]
        elif date_convention_variance and status == "date_variance":
            summary_text = date_convention_variance["prompt_message"]
        else:
            summary_text = extracted_data.get("summary") or (
                "Bank reply instrument verified successfully."
                if status == "verified"
                else "Discrepancies found requiring treasury confirmation."
            )

        return {
            "status": status,
            "matches": matches,
            "mismatches": mismatches,
            "date_convention_variance": date_convention_variance,
            "instruction_disambiguation": instruction_disambiguation,
            "bank_terms_modification": bank_terms_modification,
            "bank_reply_letter_date": bank_reply_letter_date,
            "duplicate_scan_detected": duplicate_scan_info,
            "file_sha256": file_sha256,
            "extracted_data": extracted_data,
            "summary": summary_text,
        }

    @classmethod
    def _disambiguate_multi_instruction(
        cls,
        raw_text: str,
        extracted_data: Dict[str, Any],
        target_instruction: Dict[str, Any],
        pending_instructions: List[Dict[str, Any]],
        tolerance_days: int = DEFAULT_DATE_TOLERANCE_DAYS,
    ) -> Optional[Dict[str, Any]]:
        """
        Item 2: Cross-Instruction Validation.
        Evaluates whether the uploaded bank reply belongs to a sibling pending instruction
        rather than the target instruction selected by the user.
        """
        target_id = target_instruction.get("id")
        siblings = [inst for inst in pending_instructions if inst.get("id") != target_id]
        if not siblings:
            return None

        target_purpose, target_type_label = _normalize_instruction_purpose(target_instruction.get("instruction_type"))
        target_serial = (target_instruction.get("serial_number") or "").strip().upper()
        target_seq = (
            target_instruction.get("global_seq_per_lg")
            or target_instruction.get("seq")
            or target_instruction.get("id")
        )

        # Gather extracted cues
        detected_serials = [
            s.strip().upper()
            for s in (extracted_data.get("detected_instruction_serials") or []) + _extract_instruction_serials_from_text(raw_text)
            if s
        ]
        raw_upper = (raw_text or "").upper()
        detected_purpose_raw = extracted_data.get("detected_reply_purpose")
        if not detected_purpose_raw and extracted_data.get("is_discharge_confirmed"):
            detected_purpose_raw = "RELEASE"
        detected_purpose, _ = _normalize_instruction_purpose(detected_purpose_raw) if detected_purpose_raw else ("UNKNOWN", "Unknown")

        amended_expiry_str = _normalize_date(extracted_data.get("amended_expiry_date"))
        cited_prior_expiry_str = _normalize_date(extracted_data.get("cited_prior_expiry_date"))
        amended_amount = extracted_data.get("amended_amount")

        # 1. Check if the document explicitly cites the target instruction serial
        if target_serial and (target_serial in detected_serials or target_serial in raw_upper):
            return None

        # 2. Check if the document explicitly cites a sibling instruction's serial number
        for sib in siblings:
            sib_serial = (sib.get("serial_number") or "").strip().upper()
            if sib_serial and (sib_serial in detected_serials or sib_serial in raw_upper):
                return cls._build_disambiguation_payload(
                    target_instruction, target_seq, target_type_label, sib, extracted_data, detected_purpose
                )

        # 3. Check if detected document purpose/type conflicts with target instruction but matches a sibling
        if detected_purpose != "UNKNOWN" and detected_purpose != target_purpose:
            matching_siblings = [
                sib for sib in siblings
                if _normalize_instruction_purpose(sib.get("instruction_type"))[0] == detected_purpose
            ]
            if matching_siblings:
                best_sib = cls._pick_best_sibling_by_terms(
                    matching_siblings, amended_expiry_str, cited_prior_expiry_str, amended_amount, tolerance_days
                )
                return cls._build_disambiguation_payload(
                    target_instruction, target_seq, target_type_label, best_sib, extracted_data, detected_purpose
                )

        # 4. Same-purpose disambiguation (e.g. two pending extensions or two partial releases)
        same_purpose_siblings = [
            sib for sib in siblings
            if _normalize_instruction_purpose(sib.get("instruction_type"))[0] == target_purpose
        ]
        if same_purpose_siblings:
            target_details = target_instruction.get("details") or {}
            target_exp_expiry = _normalize_date(
                target_details.get("new_expiry_date") or target_details.get("requested_expiry_date")
            )
            target_exp_amount = (
                target_details.get("new_amount")
                or target_details.get("new_lg_amount")
                or target_details.get("requested_amount")
            )

            # Check if target fails expiry match while a sibling matches within tolerance
            if target_purpose == "EXTENSION" and amended_expiry_str:
                target_matches_expiry = cls._dates_within_tolerance(amended_expiry_str, target_exp_expiry, tolerance_days)
                if not target_matches_expiry:
                    for sib in same_purpose_siblings:
                        sib_details = sib.get("details") or {}
                        sib_exp_expiry = _normalize_date(
                            sib_details.get("new_expiry_date") or sib_details.get("requested_expiry_date")
                        )
                        sib_old_expiry = _normalize_date(sib_details.get("old_expiry_date"))
                        if cls._dates_within_tolerance(amended_expiry_str, sib_exp_expiry, tolerance_days) or (
                            cited_prior_expiry_str and sib_old_expiry and cited_prior_expiry_str == sib_old_expiry
                        ):
                            return cls._build_disambiguation_payload(
                                target_instruction, target_seq, target_type_label, sib, extracted_data, detected_purpose
                            )

            # Check if target fails amount match while a sibling matches amount
            if target_purpose in ("DECREASE_AMOUNT", "INCREASE_AMOUNT") and amended_amount is not None:
                target_matches_amt = False
                if target_exp_amount is not None:
                    try:
                        target_matches_amt = abs(float(amended_amount) - float(target_exp_amount)) < 1.0
                    except Exception:
                        pass
                if not target_matches_amt:
                    for sib in same_purpose_siblings:
                        sib_details = sib.get("details") or {}
                        sib_exp_amt = (
                            sib_details.get("new_amount")
                            or sib_details.get("new_lg_amount")
                            or sib_details.get("requested_amount")
                        )
                        if sib_exp_amt is not None:
                            try:
                                if abs(float(amended_amount) - float(sib_exp_amt)) < 1.0:
                                    return cls._build_disambiguation_payload(
                                        target_instruction, target_seq, target_type_label, sib, extracted_data, detected_purpose
                                    )
                            except Exception:
                                pass

        return None

    @staticmethod
    def _dates_within_tolerance(date_a_str: Optional[str], date_b_str: Optional[str], tolerance_days: int) -> bool:
        if not date_a_str or not date_b_str:
            return False
        try:
            dt_a = datetime.strptime(date_a_str, "%Y-%m-%d").date()
            dt_b = datetime.strptime(date_b_str, "%Y-%m-%d").date()
            return abs((dt_a - dt_b).days) <= tolerance_days
        except Exception:
            return date_a_str == date_b_str

    @classmethod
    def _pick_best_sibling_by_terms(
        cls,
        candidates: List[Dict[str, Any]],
        amended_expiry_str: Optional[str],
        cited_prior_expiry_str: Optional[str],
        amended_amount: Optional[float],
        tolerance_days: int,
    ) -> Dict[str, Any]:
        if len(candidates) == 1:
            return candidates[0]
        for sib in candidates:
            details = sib.get("details") or {}
            sib_exp = _normalize_date(details.get("new_expiry_date") or details.get("requested_expiry_date"))
            sib_old = _normalize_date(details.get("old_expiry_date"))
            sib_amt = (
                details.get("new_amount")
                or details.get("new_lg_amount")
                or details.get("requested_amount")
            )
            if amended_expiry_str and sib_exp and cls._dates_within_tolerance(amended_expiry_str, sib_exp, tolerance_days):
                return sib
            if cited_prior_expiry_str and sib_old and cited_prior_expiry_str == sib_old:
                return sib
            if amended_amount is not None and sib_amt is not None:
                try:
                    if abs(float(amended_amount) - float(sib_amt)) < 1.0:
                        return sib
                except Exception:
                    pass
        return candidates[0]

    @staticmethod
    def _build_disambiguation_payload(
        target_instruction: Dict[str, Any],
        target_seq: Any,
        target_type_label: str,
        matched_inst: Dict[str, Any],
        extracted_data: Dict[str, Any],
        detected_purpose: str,
    ) -> Dict[str, Any]:
        matched_seq = (
            matched_inst.get("global_seq_per_lg")
            or matched_inst.get("seq")
            or matched_inst.get("id")
        )
        _, matched_type_label = _normalize_instruction_purpose(matched_inst.get("instruction_type"))
        target_serial_str = target_instruction.get("serial_number") or f"#{target_seq}"
        matched_serial_str = matched_inst.get("serial_number") or f"#{matched_seq}"

        def _describe_terms(inst_dict: Dict[str, Any], type_lbl: str) -> str:
            d = inst_dict.get("details") or {}
            exp = _normalize_date(d.get("new_expiry_date") or d.get("requested_expiry_date"))
            amt = d.get("new_amount") or d.get("new_lg_amount") or d.get("requested_amount")
            if exp and "Extension" in type_lbl:
                return f"{type_lbl} to {exp}"
            if amt is not None and ("Decrease" in type_lbl or "Increase" in type_lbl):
                try:
                    return f"{type_lbl} to {float(amt):,.2f}"
                except Exception:
                    pass
            return type_lbl

        matched_desc = _describe_terms(matched_inst, matched_type_label)
        target_desc = _describe_terms(target_instruction, target_type_label)

        prompt_message = (
            f"Wrong Instruction Selected: This bank reply belongs to your {matched_desc} instruction "
            f"({matched_serial_str}), not the currently selected {target_desc} instruction ({target_serial_str})."
        )
        return {
            "classification": "WRONG_INSTRUCTION_TARGET",
            "target_instruction_id": target_instruction.get("id"),
            "target_seq": target_seq,
            "target_serial": target_instruction.get("serial_number"),
            "target_type": target_instruction.get("instruction_type"),
            "target_type_label": target_type_label,
            "matched_instruction_id": matched_inst.get("id"),
            "matched_seq": matched_seq,
            "matched_serial": matched_inst.get("serial_number"),
            "matched_type": matched_inst.get("instruction_type"),
            "matched_type_label": matched_type_label,
            "bank_advice_reference": extracted_data.get("bank_advice_reference"),
            "detected_reply_purpose": detected_purpose,
            "cited_effective_date": _normalize_date(extracted_data.get("cited_effective_date")),
            "cited_prior_expiry_date": _normalize_date(extracted_data.get("cited_prior_expiry_date")),
            "prompt_message": prompt_message,
            "action_label": f"Switch to {matched_desc} ({matched_serial_str})",
        }

    # ─────────────────────────────────────────────────────────────
    # Gemini Prompts
    # ─────────────────────────────────────────────────────────────

    @classmethod
    def _call_gemini_delivery_prompt(
        cls, raw_text: str, expected_bank: str, issue_date: Optional[str], serial: str, lg_num: str
    ) -> Optional[Dict[str, Any]]:
        if not raw_text or len(raw_text.strip()) < 15:
            return None

        try:
            from app.core.ai_integration import _get_genai_client, _safe_generate_content_sync, GEMINI_MODEL_NAME, genai_types
            client = _get_genai_client()
            if not client:
                return None

            prompt = f"""
You are an expert trade finance auditor verifying scanned DELIVERY EVIDENCE for a Letter of Guarantee (LG) corporate instruction.

EXPECTED PARAMETERS:
- Expected LG Number: "{lg_num}"
- Expected Instruction Serial: "{serial}"
- Expected Counterparty Bank: "{expected_bank}"
- Instruction Issue Date: "{issue_date}"

DOCUMENT TEXT (Extracted from user's scan):
{raw_text[:12000]}

AUDIT TASKS:
1. LG Number Matching: Does this scanned document reference "{lg_num}"? Extract any and all LG / Guarantee numbers mentioned.
2. Instruction Reference: Does it mention instruction serial "{serial}" or refer to this corporate letter?
3. Bank Identification: What bank issued or is addressed in this document? Extract the bank name found in header/watermark/body/stamp.
4. Document Type: What kind of document is this? Categorize as:
   - "CORPORATE_INSTRUCTION_LETTER_STAMPED" (The corporate letter stamped with teller incoming receipt)
   - "ORIGINAL_LG_LETTER" (An actual guarantee issuance letter from a bank)
   - "BANK_AMENDMENT_ADVICE" (Bank advice of extension/amendment)
   - "COURIER_WAYBILL" (Courier delivery slip e.g. Aramex / DHL / Bosta / Egypt Post)
   - "REGISTERED_MAIL_RECEIPT" (مسجل بعلم الوصول)
   - "UNRELATED_DOCUMENT" (Invoice, commercial contract, or unrelated document)
5. Bank Receiving Stamp Detection:
   - CRITICAL RULE: Do NOT treat printed template text, blank signature lines, or form placeholders (such as "Signature and Stamp", "Signature:", "ختم وتوقيع", "Authorised Signatory", or blank underlines "____") as a stamp!
   - A bank receiving stamp (خاتم وارد / استلام الفرع) is ONLY detected if there is evidence of an actual applied teller stamp with a date, branch name, or teller receiving note. If there is only printed boilerplate text like "Signature and Stamp" without an applied teller stamp, set is_bank_stamp_detected = false.

Return a valid JSON object matching this schema:
{{
  "detected_lg_numbers": ["string"],
  "detected_instruction_serials": ["string"],
  "detected_bank_name": "string or null",
  "detected_document_type": "string",
  "is_bank_stamp_detected": boolean,
  "detected_branch_name": "string or null",
  "detected_stamp_date": "YYYY-MM-DD or null",
  "detected_stamp_time": "string or null",
  "is_courier_receipt": boolean,
  "courier_name": "string or null",
  "courier_delivery_date": "YYYY-MM-DD or null",
  "confidence_summary": "string (1-sentence summary of findings)"
}}
"""
            config = genai_types.GenerateContentConfig(
                response_mime_type="application/json"
            )
            response = _safe_generate_content_sync(client, "gemini-2.5-flash", prompt, config=config)
            return json.loads(response.text)
        except Exception as e:
            logger.warning(f"Gemini delivery evidence extraction failed: {e}")
            return None

    @classmethod
    def _call_gemini_bank_reply_prompt(
        cls,
        raw_text: str,
        expected_bank: str,
        action_type: str,
        exp_expiry: Optional[str],
        exp_amount: Optional[float],
        lg_num: str,
        pending_instructions: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not raw_text or len(raw_text.strip()) < 15:
            return None

        try:
            from app.core.ai_integration import _get_genai_client, _safe_generate_content_sync, GEMINI_MODEL_NAME, genai_types
            client = _get_genai_client()
            if not client:
                return None

            pending_context_str = ""
            if pending_instructions:
                summary_lines = []
                for inst in pending_instructions:
                    seq = inst.get("global_seq_per_lg") or inst.get("seq") or inst.get("id")
                    i_type = inst.get("instruction_type")
                    i_serial = inst.get("serial_number")
                    i_details = inst.get("details") or {}
                    exp_dt = i_details.get("new_expiry_date") or i_details.get("requested_expiry_date") or ""
                    amt = (
                        i_details.get("new_amount")
                        or i_details.get("new_lg_amount")
                        or i_details.get("requested_amount")
                        or ""
                    )
                    summary_lines.append(
                        f"  * Instruction #{seq}: serial={i_serial}, type={i_type}, target_expiry={exp_dt}, target_amount={amt}"
                    )
                pending_context_str = "\nOPEN INSTRUCTIONS ON THIS GUARANTEE:\n" + "\n".join(summary_lines) + "\n"

            prompt = f"""
You are an expert trade finance auditor verifying a scanned BANK REPLY for a Letter of Guarantee (LG).

EXPECTED PARAMETERS:
- Expected LG Number: "{lg_num}"
- Expected Issuing Bank: "{expected_bank}"
- Target Instruction Action Type: "{action_type}"
- Expected New Expiry Date: "{exp_expiry}"
- Expected New Amount: "{exp_amount}"
{pending_context_str}
DOCUMENT TEXT:
{raw_text[:12000]}

AUDIT TASKS:
1. LG Number Matching: Does this reply reference "{lg_num}"? Extract all LG numbers found.
2. Instruction & Bank Advice References:
   - Extract any corporate instruction reference / serial numbers mentioned (`detected_instruction_serials`).
   - Extract the bank's own advice reference number (`bank_advice_reference`), if any.
3. Bank Identity: Is this reply issued by "{expected_bank}"?
4. Document Classification (`detected_document_type`):
   - "BANK_AMENDMENT_ADVICE" (Formal bank letter confirming extension, release, reduction, amendment, or status)
   - "ORIGINAL_LG_LETTER" (Original guarantee issuance letter starting with "We hereby guarantee" / "نتعهد بأن ندفع")
   - "CORPORATE_INSTRUCTION_LETTER" (A corporate letter addressed to the bank)
   - "UNRELATED_DOCUMENT"
5. Specific Bank Reply Purpose (`detected_reply_purpose`):
   Classify the exact operational purpose of this bank advice into one of:
   - "EXTENSION" (Extension / renewal of validity date / مد سريان / تجديد خطاب الضمان)
   - "RELEASE" (Release / discharge / cancellation of the guarantee / إلغاء / شطب / رد أصل خطاب الضمان / إخلاء طرف)
   - "DECREASE_AMOUNT" (Partial release / reduction of guarantee amount / تخفيض قيمة خطاب الضمان)
   - "INCREASE_AMOUNT" (Increase of guarantee amount / زيادة قيمة خطاب الضمان)
   - "LIQUIDATION" (Liquidation / payment of claim / مصادرة / تسييل خطاب الضمان)
   - "ACTIVATION" (Activation of non-operative guarantee / تفعيل خطاب الضمان)
   - "AMENDMENT" (Other general text amendment)
   - "UNKNOWN"
6. Dates & Amounts Extracted:
   - `amended_expiry_date`: New/confirmed expiry date stated by the bank (YYYY-MM-DD).
   - `cited_prior_expiry_date`: Previous expiry date cited in the advice before extension (YYYY-MM-DD), if mentioned.
   - `cited_effective_date`: Effective date or advice issuance date (YYYY-MM-DD), if mentioned.
   - `amended_amount`: New/confirmed guarantee amount if mentioned.
7. Bank Seal & Red Security Stamp:
   - CRITICAL RULE: Do NOT treat printed template text, blank signature lines, or labels (such as "Signature and Stamp", "Authorised Signatory", "ختم وتوقيع", or blank underlines "____") as an applied seal or stamp!
   - An official bank branch seal is ONLY detected if there is an actual applied branch seal or teller stamp imprint with identifying branch text or receipt date.
   - For Original LG or Amount Increase: Bank Red Security / Value Stamp (الختم الأحمر بالمبلغ / الختم البارز 3D) present?

Return a valid JSON object matching this schema:
{{
  "detected_lg_numbers": ["string"],
  "detected_instruction_serials": ["string"],
  "bank_advice_reference": "string or null",
  "detected_bank_name": "string or null",
  "detected_document_type": "string",
  "detected_reply_purpose": "string",
  "is_bank_seal_detected": boolean,
  "is_red_security_stamp_detected": boolean,
  "red_stamp_amount": number or null,
  "amended_expiry_date": "YYYY-MM-DD or null",
  "cited_prior_expiry_date": "YYYY-MM-DD or null",
  "cited_effective_date": "YYYY-MM-DD or null",
  "amended_amount": number or null,
  "is_discharge_confirmed": boolean,
  "summary": "string (1-sentence summary)"
}}
"""
            config = genai_types.GenerateContentConfig(
                response_mime_type="application/json"
            )
            response = _safe_generate_content_sync(client, "gemini-2.5-flash", prompt, config=config)
            return json.loads(response.text)
        except Exception as e:
            logger.warning(f"Gemini bank reply extraction failed: {e}")
            return None

    # ─────────────────────────────────────────────────────────────
    # Heuristic Fallback Analysis
    # ─────────────────────────────────────────────────────────────

    @classmethod
    def _heuristic_delivery_extraction(cls, text: str, issue_date_str: Optional[str]) -> Dict[str, Any]:
        text_lower = (text or "").lower()

        applied_stamp_keywords = [
            "خاتم وارد", "استلام الفرع", "وارد بتاريخ", "تم الاستلام بتاريخ", "ختم استلام",
            "استلمنا في", "incoming stamp", "received by branch", "teller stamp", "receipt stamp"
        ]
        has_stamp = any(kw in text_lower for kw in applied_stamp_keywords)

        courier_keywords = ["aramex", "dhl", "bosta", "fedex", "egypt post", "البريد المصري", "علم وصول", "مسجل بعلم الوصول", "بوليصة شحن"]
        is_courier = any(kw in text_lower for kw in courier_keywords)

        detected_banks = _extract_bank_names_from_text(text)
        detected_bank = detected_banks[0] if detected_banks else None

        detected_lgs = _extract_lg_numbers_from_text(text)
        detected_serials = _extract_instruction_serials_from_text(text)

        dates_found = re.findall(r'(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4})', text or "")
        detected_date = _normalize_date(dates_found[0]) if dates_found else None

        is_orig_lg = any(kw in text_lower for kw in ["we hereby guarantee", "نتعهد بأن ندفع", "letter of guarantee no", "خطاب ضمان رقم"])

        return {
            "is_bank_stamp_detected": has_stamp,
            "detected_bank_name": detected_bank,
            "detected_branch_name": None,
            "detected_stamp_date": detected_date if has_stamp else None,
            "detected_stamp_time": None,
            "detected_lg_numbers": detected_lgs,
            "detected_instruction_serials": detected_serials,
            "detected_document_type": "ORIGINAL_LG_LETTER" if is_orig_lg else ("COURIER_WAYBILL" if is_courier else ("CORPORATE_INSTRUCTION_LETTER_STAMPED" if has_stamp else "UNVERIFIED_SCAN")),
            "is_courier_receipt": is_courier,
            "courier_name": "Courier / Registered Mail" if is_courier else None,
            "courier_delivery_date": detected_date if is_courier else None,
            "confidence_summary": "Heuristic analysis evaluated delivery receipt indicators."
        }

    @classmethod
    def _heuristic_bank_reply_extraction(
        cls, text: str, action_type: str, exp_expiry: Optional[str], exp_amount: Optional[float]
    ) -> Dict[str, Any]:
        text_lower = (text or "").lower()

        applied_seal_keywords = ["خاتم الفرع", "ختم الفرع", "مختوم بختم الفرع", "official branch seal", "certified seal", "bank seal"]
        is_seal = any(w in text_lower for w in applied_seal_keywords)
        is_red_stamp = any(w in text_lower for w in ["ختم أحمر", "الختم الأحمر", "الختم البارز", "ختم القيمة", "red security stamp"])

        detected_banks = _extract_bank_names_from_text(text)
        detected_bank = detected_banks[0] if detected_banks else None

        detected_lgs = _extract_lg_numbers_from_text(text)
        detected_serials = _extract_instruction_serials_from_text(text)

        is_orig_lg = any(kw in text_lower for kw in ["we hereby guarantee", "نتعهد بأن ندفع", "letter of guarantee no", "خطاب ضمان رقم"])

        dates_found = re.findall(r'(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4})', text or "")
        norm_dates = [_normalize_date(d) for d in dates_found if _normalize_date(d)]

        detected_expiry = None
        cited_prior_expiry = None
        if exp_expiry and exp_expiry in norm_dates:
            detected_expiry = exp_expiry
            other_dates = [d for d in norm_dates if d != exp_expiry]
            if other_dates:
                cited_prior_expiry = other_dates[0]
        elif norm_dates:
            detected_expiry = norm_dates[-1]
            if len(norm_dates) >= 2:
                cited_prior_expiry = norm_dates[-2]

        amounts_found = re.findall(r'[\d,]+\.\d{2}', text or "")
        detected_amount = None
        if amounts_found:
            try:
                detected_amount = float(amounts_found[0].replace(",", ""))
            except Exception:
                pass

        is_discharge = any(
            w in text_lower
            for w in ["لاغياً", "شطب", "إلغاء", "إخلاء طرف", "discharged", "cancelled", "released", "release notice", "discharge notice", "رد أصل"]
        )
        is_extension = any(
            w in text_lower
            for w in ["extend", "extended", "extension", "validity", "new expiry", "مد سريان", "تجديد", "مد أجل", "تمديد"]
        )
        is_decrease = any(
            w in text_lower
            for w in ["decrease", "reduced", "reduction", "partial release", "تخفيض"]
        )
        is_increase = any(
            w in text_lower
            for w in ["increase", "increased", "زيادة"]
        )
        is_liquidation = any(
            w in text_lower
            for w in ["liquidat", "claim payment", "مصادرة", "تسييل"]
        )

        if is_discharge and not is_decrease:
            detected_purpose = "RELEASE"
        elif is_decrease:
            detected_purpose = "DECREASE_AMOUNT"
        elif is_increase:
            detected_purpose = "INCREASE_AMOUNT"
        elif is_liquidation:
            detected_purpose = "LIQUIDATION"
        elif is_extension:
            detected_purpose = "EXTENSION"
        else:
            detected_purpose = "UNKNOWN"

        return {
            "document_type": "ORIGINAL_LG_LETTER" if is_orig_lg else "AMENDMENT_ADVICE",
            "detected_document_type": "ORIGINAL_LG_LETTER" if is_orig_lg else "BANK_AMENDMENT_ADVICE",
            "detected_reply_purpose": detected_purpose,
            "detected_bank_name": detected_bank,
            "is_bank_seal_detected": is_seal,
            "detected_lg_numbers": detected_lgs,
            "detected_instruction_serials": detected_serials,
            "bank_advice_reference": None,
            "is_red_security_stamp_detected": is_red_stamp,
            "red_stamp_amount": detected_amount if is_red_stamp else None,
            "amended_expiry_date": detected_expiry,
            "cited_prior_expiry_date": cited_prior_expiry,
            "cited_effective_date": norm_dates[0] if norm_dates else None,
            "amended_amount": detected_amount,
            "is_discharge_confirmed": is_discharge,
            "summary": "Heuristic analysis evaluated bank advice contents."
        }


document_verification_service = DocumentVerificationService()
