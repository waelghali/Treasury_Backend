"""
Advisory Bilingual (Arabic & English) Guarantee Wording Risk & Protection Analyzer.

Supports two opposite domain perspectives (100% Additive & Non-Blocking):
1. perspective="APPLICANT" (LG Issuance — Outbound):
   - Protects our corporate customer as Applicant against open-ended/evergreen lockups,
     advance-payment guarantees missing non-operative clauses, uncapped liability, etc.
2. perspective="BENEFICIARY" (LG Custody — Inbound from Vendors/Contractors):
   - Protects our corporate customer as Beneficiary holding an incoming guarantee, flagging
     non-operative clauses, court-judgment/surety hurdles, vendor-consent gates, revocability,
     unilateral vendor reductions, onerous claim documents, and unconfirmed foreign banks.

Never blocks submission, approval, reservation, issuance, or custody workflows.
"""

import io
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Maximum file size for AI / OCR special-wording analysis (10 MB)
WORDING_DOC_MAX_SIZE_BYTES = 10 * 1024 * 1024


def load_and_unseal_stored_document_bytes(
    db: Any,
    file_path: str,
    customer_id: int,
    encryption_context: str = "lg_issuance_doc",
) -> bytes:
    """
    Loads raw bytes from GCS ('gs://...') or local disk and transparently unseals
    STREAM V2 / V1 tenant encryption (or passes legacy plaintext bytes untouched).
    """
    import os

    if not file_path:
        raise FileNotFoundError("Document file path is empty.")

    raw_bytes: Optional[bytes] = None
    if file_path.startswith("gs://"):
        try:
            from app.core.storage_service import stream_decrypted_gcs_blob
            from app.services.tenant_key_service import tenant_key_service

            tenant_dek = tenant_key_service.get_or_create_tenant_dek(db, customer_id)
            gen, _, _ = stream_decrypted_gcs_blob(
                file_path,
                dek=tenant_dek,
                context=encryption_context,
            )
            return b"".join(gen)
        except Exception:
            from app.core.ai_integration import _get_gcs_client

            path_parts = file_path[5:].split("/", 1)
            bucket_name, blob_name = path_parts[0], path_parts[1]
            client = _get_gcs_client()
            if not client:
                raise RuntimeError("GCS storage client is not available.")
            raw_bytes = client.bucket(bucket_name).blob(blob_name).download_as_bytes()
    elif os.path.exists(file_path):
        with open(file_path, "rb") as f:
            raw_bytes = f.read()
    else:
        raise FileNotFoundError(f"Document file not found: {file_path}")

    if raw_bytes:
        try:
            from app.core.security_crypto import decrypt_stream_generator
            from app.services.tenant_key_service import tenant_key_service

            tenant_dek = tenant_key_service.get_or_create_tenant_dek(db, customer_id)
            return b"".join(
                decrypt_stream_generator(io.BytesIO(raw_bytes), tenant_dek, context=encryption_context)
            )
        except Exception:
            return raw_bytes
    return b""

_ARABIC_CHAR_RE = re.compile(r"[\u0600-\u06FF]")
_LATIN_CHAR_RE = re.compile(r"[A-Za-z]")
_ARABIC_DIACRITICS_RE = re.compile(r"[\u064B-\u065F\u0670\u0640]")


def _normalize_arabic(text: str) -> str:
    """Normalizes Arabic variants & strips tatweel/diacritics for reliable legal phrase matching."""
    if not text:
        return ""
    s = _ARABIC_DIACRITICS_RE.sub("", text)
    s = re.sub(r"[أإآٱ]", "ا", s)
    s = s.replace("ة", "ه").replace("ى", "ي").replace("ؤ", "و").replace("ئ", "ي")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def detect_wording_language(text: str) -> str:
    """Detects 'AR', 'EN', or 'BILINGUAL' from text content."""
    if not text:
        return "EN"
    ar_count = len(_ARABIC_CHAR_RE.findall(text))
    en_count = len(_LATIN_CHAR_RE.findall(text))
    if ar_count >= 15 and en_count >= 25:
        return "BILINGUAL"
    if ar_count > en_count:
        return "AR"
    return "EN"


def extract_text_from_document_bytes(file_bytes: bytes, filename: str = "document.pdf") -> str:
    """Best-effort local text extraction from PDF, DOCX, or UTF-8 text files."""
    if not file_bytes:
        return ""
    lower_name = (filename or "").lower()

    # 1. Plain text
    if lower_name.endswith((".txt", ".md", ".html", ".htm")):
        return file_bytes.decode("utf-8", errors="ignore")

    # 2. Word (.docx)
    if lower_name.endswith(".docx"):
        try:
            import docx  # type: ignore
            doc = docx.Document(io.BytesIO(file_bytes))
            return "\n".join(p.text for p in doc.paragraphs if p.text)
        except Exception:
            pass

    # 3. PDF (try pypdf / PyPDF2 / fitz)
    if lower_name.endswith(".pdf") or file_bytes[:4] == b"%PDF":
        for reader_mod in ("pypdf", "PyPDF2"):
            try:
                mod = __import__(reader_mod)
                reader = mod.PdfReader(io.BytesIO(file_bytes))
                pages = []
                for page in reader.pages[:25]:
                    pages.append(page.extract_text() or "")
                extracted = "\n".join(pages).strip()
                if extracted:
                    return extracted
            except Exception:
                continue
        try:
            import fitz  # type: ignore
            pdf_doc = fitz.open(stream=file_bytes, filetype="pdf")
            pages = [pdf_doc[i].get_text("text") for i in range(min(len(pdf_doc), 25))]
            pdf_doc.close()
            extracted = "\n".join(pages).strip()
            if extracted:
                return extracted
        except Exception:
            pass

    # Fallback UTF-8 decode if printable text
    if file_bytes[:4] != b"%PDF":
        try:
            decoded = file_bytes.decode("utf-8")
            if len(decoded.strip()) >= 10:
                return decoded
        except Exception:
            pass
    return ""


def _extract_snippet(raw_text: str, match_span: Tuple[int, int], window: int = 85) -> str:
    start = max(0, match_span[0] - window)
    end = min(len(raw_text), match_span[1] + window)
    snippet = re.sub(r"\s+", " ", raw_text[start:end]).strip()
    if start > 0:
        snippet = "…" + snippet
    if end < len(raw_text):
        snippet = snippet + "…"
    return snippet


def _find_bilingual_match(
    raw_text: str,
    norm_ar_text: str,
    en_patterns: List[str],
    ar_phrases: List[str],
    ar_regex_patterns: Optional[List[str]] = None,
) -> Optional[str]:
    """Searches English regexes on raw_text and normalized Arabic phrases/regexes on norm_ar_text."""
    for pat in en_patterns:
        m = re.search(pat, raw_text, flags=re.IGNORECASE)
        if m:
            return _extract_snippet(raw_text, m.span())

    if ar_regex_patterns:
        for ar_pat in ar_regex_patterns:
            m_ar = re.search(ar_pat, norm_ar_text)
            if m_ar:
                ratio = m_ar.start() / max(1, len(norm_ar_text))
                approx_pos = int(ratio * len(raw_text))
                span_len = max(25, (m_ar.end() - m_ar.start()) + 20)
                return _extract_snippet(raw_text, (approx_pos, min(len(raw_text), approx_pos + span_len)))

    for phrase in ar_phrases:
        norm_phrase = _normalize_arabic(phrase)
        idx = norm_ar_text.find(norm_phrase)
        if idx != -1:
            # Approximate snippet around proportional position in raw_text
            ratio = idx / max(1, len(norm_ar_text))
            approx_pos = int(ratio * len(raw_text))
            return _extract_snippet(raw_text, (approx_pos, min(len(raw_text), approx_pos + len(phrase) + 20)))
    return None


def _looks_like_guarantee_wording(raw_text: str, norm_ar_text: str) -> bool:
    """
    Heuristic sanity gate: returns True only if the text contains recognizable Letter of Guarantee,
    Bank Guarantee, Standby LC, or contractual guarantee/surety terminology in English or Arabic.
    Prevents unrelated documents (e.g. invoices, menus, CVs, bank statements) from being stamped 'GREEN — Safe'.
    """
    if not raw_text or len(raw_text.strip()) < 25:
        return False
    en_cues = (
        r"\bletter\s+of\s+guarantee\b",
        r"\bbank\s+guarantee\b",
        r"\bperformance\s+(?:guarantee|bond|security)\b",
        r"\badvance\s+payment\s+(?:guarantee|bond|security)\b",
        r"\bbid\s+bond\b",
        r"\btender\s+(?:guarantee|bond|security)\b",
        r"\bretention\s+(?:money\s+)?(?:guarantee|bond)\b",
        r"\bstandby\s+letter\s+of\s+credit\b",
        r"\birrevocabl[ey]\s+(?:and\s+unconditionall?[ey]\s+)?undertake\b",
        r"\bhereby\s+(?:guarantee|undertake)\b",
        r"\bfirst\s+written\s+demand\b",
        r"\burdg\s*758\b",
        r"\bisp\s*98\b",
        r"\bbeneficiary\b.*\b(?:guarantee|undertake|demand|expiry)\b",
    )
    if any(re.search(p, raw_text, flags=re.IGNORECASE | re.DOTALL) for p in en_cues):
        return True
    ar_cues = (
        "خطاب ضمان",
        "خطاب الضمان",
        "ضمان بنكي",
        "ضمان حسن التنفيذ",
        "ضمان الدفعه المقدمه",
        "تامين ابتدائي",
        "تامين نهائي",
        "نتعهد تعهدا غير قابل للالغاء",
        "بمجرد اول طلب كتابي",
        "يلتزم البنك بدفع",
        "ساري المفعول حتي",
    )
    return any(_normalize_arabic(c) in norm_ar_text for c in ar_cues)


def run_deterministic_wording_scan(
    wording_text: str,
    request_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Deterministic bilingual (Arabic & English) legal/treasury clause scanner.
    Provides fast, consistent detection and acts as a safety net alongside Gemini AI.
    """
    ctx = request_context or {}
    raw = (wording_text or "").strip()
    norm_ar = _normalize_arabic(raw)
    lang = detect_wording_language(raw)

    findings: List[Dict[str, Any]] = []
    positive_protections: List[str] = []

    if not raw:
        return {
            "detected_language": lang,
            "findings": [],
            "positive_protections": [],
            "risk_score": 0,
            "risk_level": "GREEN",
        }

    # --- Positive Protective Clauses ---
    has_urdg = bool(
        re.search(r"\bURDG\s*758\b|\bICC\s+Publication\s+No\.?\s*758\b", raw, re.IGNORECASE)
        or _find_bilingual_match(raw, norm_ar, [], ["نشرة غرفة التجارة الدولية رقم 758", "القواعد الموحدة لخطابات الضمان 758", "URDG 758"])
    )
    if has_urdg:
        positive_protections.append("Subject to ICC URDG 758 standard rules (protects against arbitrary non-compliant demands).")

    has_null_upon_expiry = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"null\s+and\s+void\s+(?:automatically\s+)?(?:upon|after)\s+expiry\s+whether\s+returned\s+or\s+not",
            r"expire\s+automatically\s+without\s+the\s+need\s+for\s+return",
            r"whether\s+or\s+not\s+this\s+guarantee\s+is\s+returned",
        ],
        [
            "سواء اعيد الينا الاصل ام لم يعد",
            "يصبح لاغيا بانتهاء مدته سواء رد الينا",
            "ينتهي تلقائيا دون حاجه لاسترداد اصل الخطاب",
        ],
    )
    if has_null_upon_expiry:
        positive_protections.append("Automatic expiry clause regardless of physical return of the original paper guarantee.")

    has_non_operative_advance = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"(?:shall\s+)?(?:not\s+be(?:come)?|only\s+be(?:come)?|be(?:come)?\s+only)\s+(?:operative|effective|valid)\s+(?:until|upon|after|when)\b[^.;]{0,140}(?:advance|credited|account)",
            r"\bnon[-\s]?operative\s+(?:until|unless)\b[^.;]{0,140}(?:advance|credited|account)",
            r"(?:operative|effective)\s+only\s+(?:upon|after|when)\b[^.;]{0,140}(?:advance|credited|account)",
            r"enter\s+into\s+force\s+only\s+(?:upon|after)\b[^.;]{0,140}(?:advance|credited|account)",
        ],
        [
            "لا يصبح ساريا الا بعد",
            "لا يسري مفعوله الا بعد ورود الدفعه المقدمه",
            "لا يعد نافذا الا بعد اضافه قيمه الدفعه المقدمه",
            "غير ساري المفعول الا بعد استلام الدفعه المقدمه",
            "معلق علي ورود الدفعه المقدمه",
        ],
        ar_regex_patterns=[
            r"لا\s+(?:يصبح|يسري|يعد|يكون|يدخل)(?:\s+\S+){0,4}\s+(?:ساريا|نافذا|ساري\s+المفعول|نافذ\s+المفعول|مفعوله|حيز\s+النفاذ)\s+الا\s+بعد",
        ],
    )
    if has_non_operative_advance:
        positive_protections.append("Contains a Non-Operative activation protection tying liability to actual receipt of the Advance Payment.")

    has_breach_statement = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"stating\s+in\s+what\s+respect\s+the\s+(?:applicant|principal|contractor)\s+is\s+in\s+breach",
            r"accompanied\s+by\s+a\s+written\s+statement\s+of\s+breach",
            r"specifying\s+the\s+nature\s+of\s+the\s+default",
        ],
        [
            "بيان وجه الاخلال",
            "موضحا به اوجه اخلال العميل",
            "مرفقا به اقرار كتابي يوضح الاخلال بالتزاماته التعاقديه",
        ],
    )
    if has_breach_statement:
        positive_protections.append("Requires Beneficiary to state in writing the specific contractual breach when claiming.")

    # --- TRAP 1: Open-Ended / Indefinite / Auto-Renewal Without Consent (RED) ---
    evergreen_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\bopen[-\s]?ended\b",
            r"\bwithout\s+(?:an?\s+)?expiry\s+date\b",
            r"\bvalid\s+indefinitely\b",
            r"\bvalid\s+until\s+(?:the\s+original\s+is\s+)?returned\b",
            r"\bremain\s+in\s+full\s+force\s+until\s+cancelled\s+by\s+(?:the\s+)?beneficiary\b",
            r"\bautomatically\s+(?:renewed|extended)\b(?![^.;]{0,70}subject\s+to\s+bank)",
            r"\bevergreen\b",
        ],
        [
            "مفتوح المده",
            "غير محدد المده",
            "دون تاريخ انتهاء",
            "بدون تاريخ استحقاق",
            "يمدد تلقائيا",
            "يجدد تلقائيا",
            "يتجدد تلقائيا",
            "ساريا لحين رده",
            "ساري المفعول حتي اعادته",
            "لحين استرداد الاصل",
            "يظل ساريا لحين الغائه من المستفيد",
        ],
    )
    if evergreen_excerpt:
        findings.append({
            "clause_type": "OPEN_ENDED_OR_EVERGREEN",
            "severity": "RED",
            "title": "Open-Ended or Automatic Evergreen Renewal Trap",
            "plain_explanation": "The wording ties release to returning the original paper or renews automatically without your consent, locking up your credit facility and commissions indefinitely.",
            "quoted_excerpt": evergreen_excerpt,
            "suggested_protective_rewording": "Specify a fixed calendar expiry date and add: 'This guarantee shall automatically become null and void after [DD-MMM-YYYY] whether or not the original instrument is returned to the Bank.' (يصبح هذا الخطاب لاغياً تلقائياً بانتهاء يوم [...] سواء أعيد أصل الخطاب إلى البنك أم لم يُعد).",
        })

    # --- TRAP 2: Unconditional Advance Payment Guarantee Missing Non-Operative Clause (RED) ---
    lg_type_str = str(ctx.get("lg_type") or "").lower()
    purpose_str = str(ctx.get("lg_purpose") or "").lower()
    text_mentions_advance = bool(
        _find_bilingual_match(
            raw,
            norm_ar,
            [r"\badvance\s+payment\s+guarantee\b", r"\badvance\s+payment\b", r"\bdown\s+payment\b"],
            ["دفعه مقدمه", "الدفعه المقدمه", "ضمان الدفعه المقدمه"],
        )
    )
    text_explicitly_other_lg_type = bool(
        not text_mentions_advance
        and _find_bilingual_match(
            raw,
            norm_ar,
            [
                r"\bperformance\s+(?:letter\s+of\s+)?guarantee\b",
                r"\bperformance\s+bond\b",
                r"\bfinal\s+(?:letter\s+of\s+)?guarantee\b",
                r"\bbid\s+bond\b",
                r"\btender\s+(?:letter\s+of\s+)?guarantee\b",
                r"\bretention\s+(?:money\s+)?guarantee\b",
            ],
            [
                "خطاب ضمان نهائي",
                "ضمان حسن التنفيذ",
                "خطاب ضمان ابتدائي",
                "تامين ابتدائي",
                "تامين نهائي",
            ],
        )
    )
    mentions_advance = bool(
        text_mentions_advance
        or (
            not text_explicitly_other_lg_type
            and (
                "advance" in lg_type_str
                or "دفعة مقدمة" in lg_type_str
                or "دفعه مقدمه" in _normalize_arabic(lg_type_str)
                or "advance payment" in purpose_str
            )
        )
    )
    if mentions_advance and not has_non_operative_advance:
        findings.append({
            "clause_type": "UNCONDITIONAL_ADVANCE_PAYMENT",
            "severity": "RED",
            "title": "Advance Payment Guarantee Missing 'Non-Operative' Protection",
            "plain_explanation": "This wording covers an Advance Payment but is immediately operative upon issuance — meaning the beneficiary could legally demand payment before transferring the advance funds to your bank account.",
            "quoted_excerpt": "Advance Payment wording detected without a conditional receipt-of-funds clause.",
            "suggested_protective_rewording": "Add a Non-Operative clause: 'This Guarantee shall only become operative upon receipt of the full advance payment into Applicant's Account No. [...] with our Bank, free of any deductions.' (لا يصبح خطاب الضمان هذا سارياً أو نافذ المفعول إلا بعد ورود كامل قيمة الدفعة المقدمة وإضافتها لحساب العميل لدينا).",
        })

    # --- TRAP 3: Pure First-Demand Waiver of All Proof / Contestation (AMBER / RED) ---
    waiver_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"without\s+(?:any\s+)?(?:proof|justification|stating\s+reasons?)",
            r"notwithstanding\s+any\s+(?:contestation|objection|dispute)",
            r"waiving\s+all\s+rights\s+of\s+objection",
            r"upon\s+your\s+first\s+written\s+demand\s+without\s+cavil\s+or\s+argument",
        ],
        [
            "دون ابداء اسباب",
            "بدون ابداء الاسباب",
            "دون الالتفات الي اي معارضه",
            "بمجرد اول طلب دون النظر لاي معارضه",
            "دون التوقف علي موافقه العميل",
            "دون حاجه الي انذار او اجراء قضائي",
        ],
    )
    if waiver_excerpt and not has_breach_statement:
        is_cb = bool(ctx.get("is_cross_border"))
        sev = "RED" if (is_cb or not has_urdg) and "without" in waiver_excerpt.lower() and is_cb else "AMBER"
        # Escalate to RED if cross-border unconditional without URDG 758
        if is_cb and not has_urdg:
            sev = "RED"
        findings.append({
            "clause_type": "PURE_FIRST_DEMAND_WAIVER",
            "severity": sev,
            "title": "Unconditional First-Demand Payment Without Breach Statement",
            "plain_explanation": "The beneficiary can liquidate the guarantee on a bare demand letter without even stating what contractual obligation was breached (common in local government tenders, but high-risk in private or cross-border contracts).",
            "quoted_excerpt": waiver_excerpt,
            "suggested_protective_rewording": "Require a URDG 758 Art. 15(a) statement: 'Any demand must be accompanied by the Beneficiary's signed statement specifying in what respect the Applicant is in breach of its obligations under the underlying contract.' (يشترط إرفاق بيان كتابي موقع يوضح وجه إخلال العميل بالتزاماته التعاقدية).",
        })

    # --- TRAP 4: "Extend-or-Pay" Immediate Liquidation Coercion (AMBER / RED) ---
    extend_or_pay_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\bextend\s+or\s+pay\b",
            r"\bpay\s+or\s+extend\b",
            r"payable\s+automatically\s+unless\s+extended\s+at\s+least\s+\d+\s+days",
            r"forfeited\s+if\s+not\s+renewed\s+\d+\s+days\s+prior",
        ],
        [
            "التمديد او الدفع",
            "ادفعوا او مددوا",
            "اما التمديد او المصادره",
            "يصادر تلقائيا في حاله عدم التجديد قبل",
            "ملزمين بالدفع فورا ما لم يتم مد الصلاحيه",
        ],
    )
    if extend_or_pay_excerpt:
        findings.append({
            "clause_type": "EXTEND_OR_PAY_COERCION",
            "severity": "AMBER",
            "title": "'Extend-or-Pay' Automatic Forfeiture Pressure Clause",
            "plain_explanation": "Allows the beneficiary to force renewal or trigger automatic cash payout days/weeks before expiry even when there is no contractual default.",
            "quoted_excerpt": extend_or_pay_excerpt,
            "suggested_protective_rewording": "Remove automatic forfeiture on non-renewal, or subject any Extend-or-Pay demand to URDG 758 Article 26 (28-day suspension period to negotiate extension).",
        })

    # --- TRAP 5: Free Assignment / Transferability to Third Parties (AMBER) ---
    transfer_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\bfreely\s+(?:transferable|assignable)\b",
            r"\btransferable\s+without\s+(?:prior\s+)?consent\b",
            r"\bmay\s+assign\s+this\s+guarantee\s+to\s+any\s+third\s+party\b",
            r"\bin\s+favor\s+of\s+.*?\s+and/or\s+its\s+assigns\b",
        ],
        [
            "قابل للتحويل للغير",
            "يجوز التنازل عن خطاب الضمان",
            "قابل للتنازل دون موافقه",
            "تحويله لاي طرف ثالث دون الرجوع",
        ],
    )
    if transfer_excerpt:
        findings.append({
            "clause_type": "FREE_ASSIGNMENT_OR_TRANSFER",
            "severity": "AMBER",
            "title": "Unrestricted Transferability to Unknown Third Parties",
            "plain_explanation": "Lets the beneficiary transfer drawing rights to another party (such as a lender or assignee) whom your company has no direct contract or set-off rights with.",
            "quoted_excerpt": transfer_excerpt,
            "suggested_protective_rewording": "Add explicit non-transferability: 'This Guarantee is personal to the named Beneficiary and is not transferable or assignable without the prior written consent of the Bank and the Applicant.' (هذا الضمان شخصي للمستفيد المسمى وغير قابل للتحويل أو التنازل للغير).",
        })

    # --- TRAP 6: Uncapped / Compounding Liability Beyond Face Value (RED) ---
    uncapped_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\bplus\s+(?:accrued\s+)?interest\b",
            r"\bplus\s+all\s+legal\s+costs\s+and\s+penalties\b",
            r"\bin\s+addition\s+to\s+any\s+damages\s+or\s+interest\b",
            r"\bsubject\s+to\s+automatic\s+upward\s+adjustment\b",
        ],
        [
            "بالاضافه الي الفوائد والمصاريف",
            "بخلاف التعويضات والغرامات",
            "مضافا اليه الفوائد القانونيه",
            "دون التقيد بالحد الاقصي لمبلغ الضمان",
        ],
    )
    if uncapped_excerpt:
        findings.append({
            "clause_type": "UNCAPPED_OR_COMPOUNDING_LIABILITY",
            "severity": "RED",
            "title": "Uncapped Charges / Interest Beyond Guarantee Face Amount",
            "plain_explanation": "Exposes your company to open-ended interest, legal expenses, or upward adjustments above the approved guarantee amount, which most banks will either reject or require 100%+ extra cash margin for.",
            "quoted_excerpt": uncapped_excerpt,
            "suggested_protective_rewording": "Cap total liability strictly: 'Our total aggregate liability under this Guarantee shall under no circumstances exceed the maximum sum of [Currency & Amount].' (لا تتجاوز مسؤوليتنا الإجمالية بموجب هذا الخطاب بأي حال من الأحوال مبلغ [...] فقط لا غير).",
        })

    # --- TRAP 7: Onerous Foreign Law / Exclusive Foreign Courts on Domestic Entity (AMBER) ---
    issuing_country = str(ctx.get("issuance_country") or "Egypt").strip().lower()
    foreign_law_m = re.search(
        r"(?:governed\s+by\s+and\s+construed\s+in\s+accordance\s+with\s+the\s+laws\s+of|exclusive\s+jurisdiction\s+of\s+the\s+courts\s+of)\s+([A-Za-z\s]{3,30})",
        raw,
        flags=re.IGNORECASE,
    )
    if foreign_law_m:
        jurisdiction_named = foreign_law_m.group(1).strip()
        if issuing_country and issuing_country not in jurisdiction_named.lower():
            findings.append({
                "clause_type": "FOREIGN_LAW_OR_JURISDICTION",
                "severity": "AMBER",
                "title": f"Foreign Governing Law / Court Venue ({jurisdiction_named})",
                "plain_explanation": f"Subjects the guarantee to foreign law/courts ({jurisdiction_named}), making urgent local injunctive relief against abusive calls harder and costlier.",
                "quoted_excerpt": _extract_snippet(raw, foreign_law_m.span()),
                "suggested_protective_rewording": "Align governing law with the issuing bank's jurisdiction and subject the guarantee to ICC URDG 758.",
            })

    # --- TRAP 8: Missing Pro-Rata Reduction Schedule when Auto-Reducing / Advance Payment ---
    has_reduction_clause = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\bautomatically\s+reduced\b",
            r"\breduced\s+pro[-\s]?rata\b",
            r"\breduction\s+schedule\b",
            r"\bupon\s+presentation\s+of\s+(?:shipping\s+documents|interim\s+payment\s+certificates|invoices)\b",
        ],
        [
            "يخفض تلقائيا",
            "تخفض قيمه الخطاب تلقائيا",
            "بنسبه التوريد",
            "نسبيا مع المستخلصات",
        ],
    )
    if (ctx.get("is_auto_reducing") or mentions_advance) and not has_reduction_clause:
        findings.append({
            "clause_type": "MISSING_REDUCTION_SCHEDULE",
            "severity": "AMBER",
            "title": "No Automatic Value Reduction Schedule Found",
            "plain_explanation": "Full guarantee exposure stays locked until final expiry even as deliveries or interim invoices amortize the underlying obligation.",
            "quoted_excerpt": "No pro-rata or milestone reduction clause found in wording.",
            "suggested_protective_rewording": "Insert an objective reduction trigger: 'The Guarantee amount shall automatically reduce pro-rata upon presentation to the Bank of copies of signed delivery receipts / interim payment certificates.' (تُخفّض قيمة خطاب الضمان تلقائياً بنسبة المستخلصات أو محاضر الاستلام الجزئية).",
        })

    # Compute Risk Score & Level
    score = 0
    has_red = False
    has_amber = False
    for f in findings:
        if f["severity"] == "RED":
            has_red = True
            score += 42
        elif f["severity"] == "AMBER":
            has_amber = True
            score += 22
    if positive_protections:
        score = max(5 if (has_red or has_amber) else 0, score - min(15, len(positive_protections) * 5))
    score = min(100, max(0, score))

    if has_red or score >= 65:
        risk_level = "RED"
    elif has_amber or score >= 20:
        risk_level = "AMBER"
    else:
        risk_level = "GREEN"

    return {
        "detected_language": lang,
        "findings": findings,
        "positive_protections": positive_protections,
        "risk_score": score,
        "risk_level": risk_level,
    }


def run_deterministic_beneficiary_scan(
    wording_text: str,
    request_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Deterministic bilingual (Arabic & English) Beneficiary Protection Check for LG Custody.
    Evaluates an incoming guarantee from the exact OPPOSITE perspective of LG Issuance:
    our corporate customer is the Beneficiary holding a vendor/contractor LG and needs
    clean, unconditional first-demand enforceability without claim-blocking hurdles.
    """
    ctx = request_context or {}
    raw = (wording_text or "").strip()
    norm_ar = _normalize_arabic(raw)
    lang = detect_wording_language(raw)

    findings: List[Dict[str, Any]] = []
    positive_protections: List[str] = []

    if not raw and not ctx.get("foreign_bank_name") and not ctx.get("operational_status"):
        return {
            "detected_language": lang,
            "findings": [],
            "positive_protections": [],
            "risk_score": 0,
            "risk_level": "GREEN",
        }

    # --- Positive Beneficiary Strengths (GREEN signals) ---
    has_urdg = bool(
        re.search(r"\bURDG\s*758\b|\bICC\s+Publication\s+No\.?\s*758\b", raw, re.IGNORECASE)
        or _find_bilingual_match(
            raw,
            norm_ar,
            [],
            ["نشرة غرفة التجارة الدولية رقم 758", "القواعد الموحدة لخطابات الضمان 758", "URDG 758"],
        )
    )
    if has_urdg:
        positive_protections.append(
            "Governed by ICC URDG 758 independent guarantee rules (insulates your claim from underlying contract disputes under Art. 5)."
        )

    has_first_demand_strength = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\birrevocably\s+and\s+unconditionally\s+undertake\s+to\s+pay\b",
            r"\bupon\s+your\s+first\s+written\s+demand\b",
            r"\bwithout\s+(?:any\s+)?(?:proof|justification|cavil|argument)\b",
            r"\bnotwithstanding\s+any\s+(?:contestation|objection|dispute)\s+by\s+(?:the\s+)?(?:applicant|principal|contractor|supplier)?",
        ],
        [
            "بمجرد اول طلب كتابي",
            "دون الالتفات الي اي معارضه",
            "دون النظر لاي معارضه",
            "دون التوقف علي موافقه العميل",
            "غير قابل للالغاء وغير مشروط",
            "تعهدا باتا لا رجوع فيه",
            "دون حاجه الي انذار او اجراء قضائي",
            "دون حاجه الي حكم قضائي",
        ],
    )
    if has_first_demand_strength:
        positive_protections.append(
            "Unconditional first-demand undertaking — bank commits to pay on your written demand without requiring vendor consent or litigation."
        )

    has_free_of_deductions = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\bfree\s+and\s+clear\s+of\s+(?:any\s+and\s+all\s+)?(?:deductions|withholdings|set[-\s]?off|taxes)\b",
            r"\bwithout\s+any\s+(?:set[-\s]?off|counterclaim|deduction|withholding)\b",
        ],
        [
            "خاليه من اي خصم او مقاصه",
            "دون اي خصم او استقطاع",
            "كاملا دون اي مقاصه",
        ],
    )
    if has_free_of_deductions:
        positive_protections.append(
            "Payable in full free of deductions, set-off, counterclaim, or withholding."
        )

    has_beneficiary_evergreen = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\bautomatically\s+(?:renewed|extended)\b",
            r"\bremain\s+in\s+full\s+force\s+until\s+(?:cancelled|released)\s+by\s+(?:the\s+)?beneficiary\b",
            r"\bvalid\s+until\s+(?:the\s+original\s+is\s+)?returned\b",
        ],
        [
            "يمدد تلقائيا",
            "يجدد تلقائيا",
            "يتجدد تلقائيا",
            "يظل ساريا لحين الغائه من المستفيد",
            "ساريا لحين رده",
        ],
    )
    if has_beneficiary_evergreen or ctx.get("auto_renewal") is True:
        if has_beneficiary_evergreen:
            positive_protections.append(
                "Continuous / automatic renewal protection in your favor until formally released."
            )

    # --- BENEFICIARY TRAP 1: Non-Operative or Suspended Validity (RED) ---
    non_op_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\bnon[-\s]?operative\b",
            r"(?:shall\s+)?(?:not\s+be(?:come)?|only\s+be(?:come)?|be(?:come)?\s+only)\s+(?:operative|effective|valid)\s+(?:until|upon|after|when)\b",
            r"\b(?:operative|effective)\s+only\s+(?:upon|after|when)\b",
            r"\benter\s+into\s+force\s+only\s+(?:upon|after|when)\b",
            r"\bsubject\s+to\s+prior\s+activation\b",
        ],
        [
            "غير نافذ المفعول",
            "غير ساري المفعول",
            "لا يصبح ساريا الا بعد",
            "لا يسري مفعوله الا بعد",
            "لا يعد نافذا الا بعد",
            "معلق علي شرط",
            "لا يدخل حيز النفاذ الا بعد",
        ],
        ar_regex_patterns=[
            r"لا\s+(?:يصبح|يسري|يعد|يكون|يدخل)(?:\s+\S+){0,4}\s+(?:ساريا|نافذا|ساري\s+المفعول|نافذ\s+المفعول|مفعوله|حيز\s+النفاذ)\s+الا\s+بعد",
        ],
    )
    op_status_str = str(ctx.get("operational_status") or "").strip().lower()
    if non_op_excerpt or ("non-operative" in op_status_str or "non operative" in op_status_str):
        findings.append({
            "clause_type": "NON_OPERATIVE_OR_SUSPENDED_VALIDITY",
            "severity": "RED",
            "title": "Non-Operative or Suspended Guarantee Trap",
            "plain_explanation": "This incoming guarantee is NOT immediately enforceable — the issuing bank can refuse your claim unless a separate activation condition is proven first.",
            "quoted_excerpt": non_op_excerpt or f"Recorded Operational Status: {ctx.get('operational_status')}",
            "suggested_protective_rewording": "Require an immediately operative guarantee from issuance date, or obtain written Bank Confirmation of Operative Status before releasing payments or site access. (يشترط أن يكون خطاب الضمان سارياً ونافذ المفعول فوراً من تاريخ إصداره دون تعليق على أي شرط).",
        })

    # --- BENEFICIARY TRAP 2: Conditional / Court-Judgment / Suretyship Wording (RED) ---
    # Guard carefully against standard protective waivers ("without court judgment" / "دون حاجة إلى حكم قضائي")
    conditional_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"(?:upon|against|subject\s+to|accompanied\s+by|supported\s+by|only\s+after|require[sd]?|present(?:ation\s+of|ing))\s+(?:a\s+|an\s+|the\s+)?(?:certified\s+copy\s+of\s+(?:a\s+)?)?(?:final\s+(?:and\s+binding\s+)?)?(?:court\s+(?:judgment|order|ruling)|judicial\s+(?:decision|order|ruling)|arbitral\s+award)",
            r"(?:payable\s+only\s+if|only\s+payable\s+when)\s+[^.;]{0,70}(?:court|arbitra|proven|established)",
            r"(?:upon|subject\s+to|after|require[sd]?)\s+(?:your\s+)?proving\s+(?:actual\s+)?(?:loss|damages|default|breach)",
            r"(?:subject\s+to|upon|against)\s+proof\s+of\s+(?:actual\s+)?(?:loss|damages|breach|default)",
            r"\b(?:accessory\s+suretyship|subsidiary\s+surety|surety\s+bond\s+subject\s+to\s+proof)\b",
        ],
        [],
        ar_regex_patterns=[
            r"(?:بموجب|بناء\s+علي|بشرط(?:\s+تقديم|\s+صدور|\s+استصدار)?|بعد\s+(?:صدور|استصدار|تقديم)|مرفقا\s+به|لا\s+(?:يصرف|يدفع|يسدد)\s+الا\s+(?:بموجب|بناء\s+علي|بعد))\s+(?:صوره\s+رسميه\s+من\s+)?(?:حكم\s+(?:قضائي|محكمه|تحكيم|تحكيمي)|قرار\s+تحكيم|امر\s+قضائي)",
            r"(?:بشرط|يتطلب|يلزم|مرفقا\s+به\s+ما\s+يفيد|بعد\s+ثبوت|بعد)\s+اثبات\s+(?:الضرر|الخساره|الاخلال|التقصير)",
            r"\bكفاله\s+(?:تبعيه|عاديه\s+غير\s+مستقله)\b",
        ],
    )
    if conditional_excerpt:
        findings.append({
            "clause_type": "CONDITIONAL_OR_SURETYSHIP_WORDING",
            "severity": "RED",
            "title": "Conditional / Court-Judgment Payment Hurdle (Not First-Demand)",
            "plain_explanation": "Instead of paying on your written demand, the bank requires a court ruling, arbitral award, or proof of actual loss — freezing cash recovery for months or years during litigation.",
            "quoted_excerpt": conditional_excerpt,
            "suggested_protective_rewording": "Demand an unconditional independent bank guarantee: 'We irrevocably and unconditionally undertake to pay you upon your first written demand, without requiring proof of loss, judicial order, or arbitral award.' (نتعهد تعهداً باتاً لا رجوع فيه ولا قيد ولا شرط بأن ندفع لكم بمجرد أول طلب كتابي منكم دون حاجة لإثبات الضرر أو استصدار حكم قضائي أو تحكيمي).",
        })

    # --- BENEFICIARY TRAP 3: Vendor / Applicant Consent or Objection Gate (RED) ---
    applicant_gate_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"(?:subject\s+to|upon|with|require[sd]?|accompanied\s+by|after\s+obtaining)\s+(?:the\s+)?(?:prior\s+)?(?:written\s+)?(?:approval|consent|countersignature|confirmation|agreement)\s+of\s+the\s+(?:applicant|principal|contractor|supplier|account\s+party)",
            r"\bjointly\s+signed\s+by\s+(?:you\s+and\s+)?(?:the\s+)?(?:applicant|principal|contractor|supplier)\b",
            r"\bsubject\s+to\s+(?:prior\s+)?(?:verification|confirmation|clearance)\s+with\s+the\s+(?:applicant|principal|contractor|supplier)\b",
            r"\bunless\s+(?:contested|objected\s+to|disputed)\s+by\s+the\s+(?:applicant|principal|contractor|supplier)\b",
            r"\bprovided\s+(?:that\s+)?(?:the\s+)?(?:applicant|principal|contractor|supplier)\s+does\s+not\s+object\b",
        ],
        [],
        ar_regex_patterns=[
            r"(?:بشرط|يتطلب|يلزم|بعد\s+الحصول\s+علي|بموجب|مرفقا\s+به|يتوقف\s+(?:الصرف|الدفع)\s+علي)\s+موافقه\s+(?:العميل|المورد|المقاول|الامر)",
            r"(?:بشرط\s+عدم|ما\s+لم\s+يرد|شريطه\s+عدم)\s+(?:اعتراض|معارضه)\s+(?:من\s+)?(?:العميل|المورد|المقاول)",
            r"(?:بعد\s+الرجوع\s+(?:الي\s+(?:العميل|المورد|المقاول)|للعميل|للمورد|للمقاول)|بعد\s+استطلاع\s+راي\s+(?:العميل|المورد|المقاول))",
            r"توقيع\s+مشترك\s+من\s+(?:العميل|المورد|المقاول)",
        ],
    )
    if applicant_gate_excerpt:
        findings.append({
            "clause_type": "APPLICANT_OBJECTION_OR_CONSENT_GATE",
            "severity": "RED",
            "title": "Vendor / Applicant Consent or Objection Gate",
            "plain_explanation": "Gives the vendor/applicant power to block or delay bank payout simply by withholding signature or raising an objection.",
            "quoted_excerpt": applicant_gate_excerpt,
            "suggested_protective_rewording": "Delete any Applicant verification/consent requirement and insert: '...notwithstanding any objection, dispute, or contestation by the Applicant or any third party.' (ودون الالتفات إلى أي معارضة أو منازعة من العميل أو من أي طرف ثالث).",
        })

    # --- BENEFICIARY TRAP 4: Revocable Guarantee or Unilateral Bank Cancellation (RED) ---
    revocable_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"(?<!ir)(?<!non-)(?<!non\s)\brevocable\b(?!\s+only\s+with\s+(?:the\s+)?beneficiary)",
            r"\bbank\s+(?:reserves\s+the\s+right|is\s+entitled|may\s+at\s+its\s+sole\s+discretion|shall\s+have\s+the\s+right)\s+to\s+(?:cancel|terminate|revoke|withdraw)\s+this\s+guarantee\b",
            r"\bmay\s+be\s+(?:cancelled|revoked|terminated)\s+at\s+any\s+time\s+by\s+the\s+(?:issuing\s+)?bank\b",
        ],
        [],
        ar_regex_patterns=[
            r"(?<!غير\s)(?<!عدم\s)\bقابل[ها]?\s+للالغاء\b",
            r"(?:يحق\s+للبنك|يجوز\s+للبنك|للبنك\s+الحق\s+في)\s+(?:الغاء|انهاء|الرجوع\s+في)\s+(?:هذا\s+)?(?:الخطاب|الضمان)",
        ],
    )
    if revocable_excerpt:
        findings.append({
            "clause_type": "REVOCABLE_OR_BANK_CANCELLATION_CLAUSE",
            "severity": "RED",
            "title": "Revocable Guarantee or Unilateral Bank Cancellation Right",
            "plain_explanation": "Allows the guarantee to be revoked or cancelled unilaterally before expiry, leaving your contract unprotected when default risk rises.",
            "quoted_excerpt": revocable_excerpt,
            "suggested_protective_rewording": "Ensure the guarantee explicitly states it is 'Irrevocable' (غير قابل للإلغاء) and cannot be cancelled or amended before expiry without the Beneficiary's written release.",
        })

    # --- BENEFICIARY TRAP 5: Unilateral Reduction by Vendor Without Beneficiary Sign-Off (RED) ---
    unilateral_red_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\breduced\s+(?:automatically\s+)?upon\s+presentation\s+by\s+the\s+(?:applicant|principal|supplier|contractor)\b(?![^.;]{0,90}signed\s+by\s+(?:the\s+)?beneficiary)",
            r"\breduced\s+against\s+copies\s+of\s+(?:commercial\s+)?invoices\s+(?:submitted\s+by\s+the\s+(?:applicant|supplier)|without\s+(?:the\s+)?beneficiary)",
            r"\bupon\s+the\s+(?:applicant|supplier|contractor)['’]?s\s+written\s+declaration\s+of\s+(?:delivery|performance|shipment)\b",
        ],
        [
            "يخفض بمجرد تقديم العميل",
            "تخفض قيمه الخطاب بناء علي فواتير مقدمه من العميل",
            "يخفض دون توقيع المستفيد",
            "دون حاجه لموافقه المستفيد علي التخفيض",
            "بموجب مستندات يقدمها المورد منفردا",
        ],
    )
    if unilateral_red_excerpt:
        findings.append({
            "clause_type": "UNILATERAL_APPLICANT_REDUCTION_CLAUSE",
            "severity": "RED",
            "title": "Unilateral Value Reduction by Vendor Without Your Sign-Off",
            "plain_explanation": "Allows the vendor to reduce your guarantee balance using self-submitted invoices or declarations without your company's signed acceptance certificate.",
            "quoted_excerpt": unilateral_red_excerpt,
            "suggested_protective_rewording": "Require Beneficiary sign-off: 'Any reduction in the Guarantee amount shall only take effect upon presentation of an acceptance certificate or release duly signed by authorized representatives of the Beneficiary.' (لا يجوز تخفيض قيمة خطاب الضمان إلا بموجب شهادة استلام أو موافقة كتابية موقعة من المستفيد).",
        })

    # --- BENEFICIARY TRAP 6: Onerous Third-Party Documents or Original Paper Surrender for Claims (AMBER) ---
    onerous_docs_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"(?:accompanied\s+by|supported\s+by|upon\s+presentation\s+of)\s+[^.;]{0,80}(?:independent\s+(?:engineer|consultant|expert|surveyor)|third[-\s]?party\s+certificate|notarized|consularized|legalized)",
            r"(?:claim|demand)\s+must\s+be\s+accompanied\s+by\s+the\s+original\s+(?:of\s+this\s+)?(?:letter\s+of\s+)?guarantee",
        ],
        [
            "شهاده من استشاري مستقل",
            "بشهاده من خبير مستقل",
            "مصدق عليها من الشهر العقاري",
            "بشرط تقديم اصل خطاب الضمان عند المطالبه",
            "مرفقا بالمطالبه اصل هذا الخطاب",
        ],
    )
    if onerous_docs_excerpt:
        findings.append({
            "clause_type": "ONEROUS_THIRD_PARTY_CLAIM_DOCUMENTS",
            "severity": "AMBER",
            "title": "Onerous Third-Party Certificates or Original-Paper Claim Hurdle",
            "plain_explanation": "Requires external consultant certificates, notarization, or surrendering the physical original paper LG to claim — risking fatal deadline misses or loss of security on partial claims.",
            "quoted_excerpt": onerous_docs_excerpt,
            "suggested_protective_rewording": "Allow claims strictly against the Beneficiary's signed written demand (or authenticated SWIFT via your relationship bank) without third-party certificates or surrendering the original paper for partial drawings.",
        })

    # --- BENEFICIARY TRAP 7: Early Claim Cut-Off Before Expiry or Foreign Counter Presentation (AMBER) ---
    cutoff_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"(?:(?:claim|demand)s?\s+)?must\s+be\s+(?:received|presented|delivered)\s+(?:by\s+us\s+|at\s+our\s+counters\s+)?at\s+least\s+\d+\s+(?:calendar\s+|business\s+|working\s+)?days\s+(?:prior\s+to|before)\s+(?:the\s+)?expiry",
            r"\bpresented\s+exclusively\s+in\s+person\s+at\s+our\s+counters\s+in\b",
        ],
        [
            "يجب تقديم المطالبه قبل انتهاء السريان بمده",
            "ترد الينا المطالبه قبل تاريخ الانتهاء ب",
            "تسلم باليد فقط في مقر البنك بالخارج",
        ],
    )
    if cutoff_excerpt:
        findings.append({
            "clause_type": "EARLY_CLAIM_CUTOFF_OR_COUNTER_HURDLE",
            "severity": "AMBER",
            "title": "Early Claim Cut-Off Before Stated Expiry Date",
            "plain_explanation": "Bars claims during the final days before expiry or restricts delivery channels, creating a hidden uncovered gap before contract completion.",
            "quoted_excerpt": cutoff_excerpt,
            "suggested_protective_rewording": "Ensure claims may be presented on or at any time up to and including the stated expiry date (plus a 15-day claim mailing grace period if feasible), including via authenticated SWIFT.",
        })

    # --- BENEFICIARY TRAP 8: Unconfirmed Foreign Issuing Bank or Foreign Court Venue (AMBER) ---
    beneficiary_country = str(ctx.get("beneficiary_country") or "Egypt").strip().lower()
    foreign_law_m = re.search(
        r"(?:governed\s+by\s+and\s+construed\s+in\s+accordance\s+with\s+the\s+laws\s+of|exclusive\s+jurisdiction\s+of\s+the\s+courts\s+of)\s+([A-Za-z\s]{3,30})",
        raw,
        flags=re.IGNORECASE,
    )
    unconfirmed_advice_excerpt = _find_bilingual_match(
        raw,
        norm_ar,
        [
            r"\badvised\s+without\s+any\s+(?:engagement|responsibility|confirmation)\s+on\s+our\s+part\b",
            r"\bwithout\s+adding\s+our\s+confirmation\b",
        ],
        [
            "دون اي التزام او مسؤوليه من جانبنا",
            "دون تعزيز من جانبنا",
            "تبليغ فقط دون ادني مسؤوليه",
        ],
    )
    foreign_bank_name = str(ctx.get("foreign_bank_name") or "").strip()
    advising_status_str = str(ctx.get("advising_status") or "").strip().lower()
    has_unconfirmed_foreign_ctx = bool(foreign_bank_name and "confirmed" not in advising_status_str)
    has_foreign_law = bool(
        foreign_law_m and beneficiary_country and beneficiary_country not in foreign_law_m.group(1).strip().lower()
    )

    if unconfirmed_advice_excerpt or has_unconfirmed_foreign_ctx or has_foreign_law:
        if unconfirmed_advice_excerpt:
            excerpt = unconfirmed_advice_excerpt
        elif foreign_law_m and has_foreign_law:
            excerpt = _extract_snippet(raw, foreign_law_m.span())
        else:
            excerpt = f"Foreign Issuing Bank: {foreign_bank_name} (Status: {ctx.get('advising_status') or 'Unconfirmed'})"
        findings.append({
            "clause_type": "UNCONFIRMED_FOREIGN_BANK_OR_LAW",
            "severity": "AMBER",
            "title": "Unconfirmed Foreign Bank or Foreign Court Enforcement Hurdle",
            "plain_explanation": "Collecting on an unconfirmed foreign bank guarantee or litigating under foreign law exposes your company to cross-border payment delays and foreign court costs.",
            "quoted_excerpt": excerpt,
            "suggested_protective_rewording": "Request local Confirmation by an approved domestic relationship bank (تعزيز محلي من بنك معتمد) or re-issuance via a local bank counter-guarantee governed by local law / ICC URDG 758.",
        })

    # Compute Risk Score & Level
    score = 0
    has_red = False
    has_amber = False
    for f in findings:
        if f["severity"] == "RED":
            has_red = True
            score += 42
        elif f["severity"] == "AMBER":
            has_amber = True
            score += 22
    if positive_protections:
        score = max(5 if (has_red or has_amber) else 0, score - min(15, len(positive_protections) * 5))
    score = min(100, max(0, score))

    if has_red or score >= 65:
        risk_level = "RED"
    elif has_amber or score >= 20:
        risk_level = "AMBER"
    else:
        risk_level = "GREEN"

    return {
        "detected_language": lang,
        "findings": findings,
        "positive_protections": positive_protections,
        "risk_score": score,
        "risk_level": risk_level,
    }


async def analyze_special_wording_traps(
    wording_text: Optional[str] = None,
    file_bytes: Optional[bytes] = None,
    filename: str = "special_wording.pdf",
    request_context: Optional[Dict[str, Any]] = None,
    db: Any = None,
    customer_id: Optional[int] = None,
    user_id: Optional[int] = None,
    perspective: str = "APPLICANT",
) -> Dict[str, Any]:
    """
    Hybrid Bilingual (Arabic & English) Guarantee Wording Analyzer.
    - perspective="APPLICANT" (default, LG Issuance): Special-Wording Trap Detector for our outbound LGs.
    - perspective="BENEFICIARY" (LG Custody): Opposite Beneficiary Protection Check for incoming vendor LGs.
    Never blocks approval, issuance, or custody status changes.
    """
    eff_perspective = "BENEFICIARY" if str(perspective or "").strip().upper() == "BENEFICIARY" else "APPLICANT"

    if file_bytes and len(file_bytes) > WORDING_DOC_MAX_SIZE_BYTES:
        return {
            "status": "TOO_LARGE",
            "perspective": eff_perspective,
            "message": f"Document is too large for AI wording scan ({len(file_bytes) / (1024 * 1024):.1f} MB). Maximum is {WORDING_DOC_MAX_SIZE_BYTES // (1024 * 1024)} MB.",
        }

    extracted_doc_text = extract_text_from_document_bytes(file_bytes, filename) if file_bytes else ""
    combined_text = "\n\n".join(part for part in [(wording_text or "").strip(), extracted_doc_text.strip()] if part).strip()

    if not combined_text and not file_bytes:
        return {
            "status": "ERROR",
            "perspective": eff_perspective,
            "message": "No guarantee wording text or attached document was provided to scan.",
        }

    if eff_perspective == "BENEFICIARY":
        det_result = run_deterministic_beneficiary_scan(combined_text, request_context=request_context)
    else:
        det_result = run_deterministic_wording_scan(combined_text, request_context=request_context)

    findings_by_type: Dict[str, Dict[str, Any]] = {
        f["clause_type"]: f for f in det_result["findings"]
    }
    positive_protections: List[str] = list(det_result["positive_protections"])
    detected_lang = det_result["detected_language"]
    ai_summary: Optional[str] = None
    ai_is_guarantee_doc: Optional[bool] = None
    is_det_guarantee_doc = _looks_like_guarantee_wording(combined_text, _normalize_arabic(combined_text))
    analysis_engine = "DETERMINISTIC_BILINGUAL_RULES"

    # Attempt Gemini deep legal & multimodal analysis if configured
    try:
        from app.core.ai_integration import (
            GEMINI_MODEL_NAME,
            _get_genai_client,
            _safe_generate_content_async,
            genai_types,
            log_ai_usage_sync,
        )
        client = _get_genai_client()
    except Exception:
        client = None

    if client:
        ctx = request_context or {}
        if eff_perspective == "BENEFICIARY":
            ctx_summary = json.dumps(
                {
                    "our_role": "BENEFICIARY (holding incoming guarantee from vendor/contractor)",
                    "vendor_issuer_name": ctx.get("issuer_name"),
                    "beneficiary_entity": ctx.get("beneficiary_name"),
                    "lg_type": ctx.get("lg_type"),
                    "currency": ctx.get("currency"),
                    "amount": ctx.get("lg_value") or ctx.get("amount"),
                    "expiry_date": ctx.get("lg_expiry_date"),
                    "operational_status": ctx.get("operational_status"),
                    "advising_status": ctx.get("advising_status"),
                    "foreign_bank_name": ctx.get("foreign_bank_name"),
                    "foreign_bank_country": ctx.get("foreign_bank_country"),
                    "auto_renewal": ctx.get("auto_renewal"),
                    "applicable_rules": ctx.get("applicable_rules"),
                },
                ensure_ascii=False,
            )
            prompt = f"""You are a Senior Corporate Treasury Trade-Finance Legal Advisor specializing in Letters of Guarantee (خطابات الضمان) under ICC URDG 758, ISP98, and Egyptian / MENA banking practice.

CRITICAL PERSPECTIVE: Our corporate customer is the **BENEFICIARY** holding an incoming Letter of Guarantee provided by a supplier/contractor (LG Custody).
Therefore, unconditional first-demand payment ("upon your first written demand without proof or objection" / "بمجرد أول طلب كتابي دون الالتفات لأي معارضة") and automatic renewal in the Beneficiary's favor are **POSITIVE PROTECTIONS** (NOT traps!).

Context: {ctx_summary}

Identify any legal, documentary, or operational hurdles that could BLOCK, DELAY, or WEAKEN our corporate customer's (the Beneficiary's) right to claim/liquidate cash upon vendor default:
1. NON_OPERATIVE_OR_SUSPENDED_VALIDITY: Guarantee is non-operative, suspended, or conditional upon an unconfirmed event (غير نافذ المفعول / لا يصبح سارياً إلا بعد).
2. CONDITIONAL_OR_SURETYSHIP_WORDING: Requires a court judgment, arbitral award, proof of actual damages, or accessory suretyship instead of independent first-demand payment (حكم قضائي / حكم تحكيم / إثبات الضرر / كفالة تبعية). Note: waiving court judgments ("without court judgment" / "دون حكم قضائي") is a POSITIVE strength, NOT a trap!
3. APPLICANT_OBJECTION_OR_CONSENT_GATE: Requires vendor/applicant consent, joint signature, verification with the applicant, or absence of vendor objection (بشرط موافقة العميل / بشرط عدم اعتراض العميل / بعد الرجوع للعميل).
4. REVOCABLE_OR_BANK_CANCELLATION_CLAUSE: Guarantee is revocable or permits unilateral bank cancellation before expiry (قابل للإلغاء / يحق للبنك إلغاء الخطاب).
5. UNILATERAL_APPLICANT_REDUCTION_CLAUSE: Value reduces automatically against copies/declarations submitted unilaterally by the vendor without Beneficiary sign-off.
6. ONEROUS_THIRD_PARTY_CLAIM_DOCUMENTS: Claims require independent engineer/consultant certificates, notarized documents, or surrendering the physical original paper LG even for partial claims.
7. EARLY_CLAIM_CUTOFF_OR_COUNTER_HURDLE: Claims must arrive X days BEFORE expiry or exclusively in person at foreign counters.
8. UNCONFIRMED_FOREIGN_BANK_OR_LAW: Issued/advised without local bank confirmation ("without engagement on our part" / "دون التزام أو مسؤولية") or governed by foreign courts.
9. OTHER_BENEFICIARY_HURDLE: Any other hidden restriction weakening the Beneficiary's claim rights (e.g., ultra-short validity vs contract obligation, restrictive currency/set-off carve-outs).

Return ONLY valid JSON with this exact schema:
{{
  "is_guarantee_document": true | false,
  "detected_language": "AR" | "EN" | "BILINGUAL",
  "executive_summary": "<2 concise plain-English sentences summarizing how strongly this incoming guarantee protects the Beneficiary's right to claim (or explaining why is_guarantee_document is false if it is an invoice/receipt/unrelated file)>",
  "findings": [
    {{
      "clause_type": "<one of the codes above>",
      "severity": "RED" | "AMBER",
      "title": "<short plain-English title of the claim hurdle>",
      "plain_explanation": "<1-2 plain-English sentences explaining how this weakens or delays collecting on a claim>",
      "quoted_excerpt": "<exact short quote from the Arabic or English text>",
      "suggested_protective_rewording": "<exact amendment to request from the vendor / issuing bank in matching language (or bilingual)>"
    }}
  ],
  "positive_protections": ["<short plain-English bullet of any strong Beneficiary protection confirmed in the text>"]
}}
IMPORTANT: Set `"is_guarantee_document": false` if the document/text is NOT a Letter of Guarantee, Bank Guarantee, Standby LC, or contractual guarantee clause (for example, if it is a sales/catering invoice, bank cash statement, CV, receipt, or unrelated text).
"""
        else:
            ctx_summary = json.dumps(
                {
                    "lg_type": ctx.get("lg_type"),
                    "currency": ctx.get("currency"),
                    "amount": ctx.get("lg_value") or ctx.get("amount"),
                    "requested_expiry_date": ctx.get("lg_expiry_date"),
                    "is_cross_border": ctx.get("is_cross_border"),
                    "issuance_country": ctx.get("issuance_country", "Egypt"),
                    "is_auto_reducing": ctx.get("is_auto_reducing"),
                    "applicable_rules": ctx.get("applicable_rules"),
                },
                ensure_ascii=False,
            )

            prompt = f"""You are a Senior Corporate Treasury Trade-Finance Legal Advisor specializing in Letters of Guarantee (خطابات الضمان) under ICC URDG 758, ISP98, and Egyptian / MENA banking practice.

Analyze the provided Guarantee Special Wording (Arabic, English, or Bilingual) against the transaction context:
Context: {ctx_summary}

Identify any legal or financial traps for the Applicant (corporate customer), focusing on:
1. OPEN_ENDED_OR_EVERGREEN: Open-ended validity, automatic evergreen renewal without applicant consent, or release tied strictly to returning the physical paper original (مفتوح المدة / يجدد تلقائياً / ساري لحين رد الأصل).
2. UNCONDITIONAL_ADVANCE_PAYMENT: Advance payment guarantee missing a Non-Operative clause tying activation to actual receipt of advance funds into the Applicant's account (عدم وجود شرط سريان الخطاب بعد ورود الدفعة المقدمة).
3. PURE_FIRST_DEMAND_WAIVER: Pure unconditional first-demand payment waiving any written statement of contractual breach.
4. EXTEND_OR_PAY_COERCION: Automatic liquidation if renewal is not delivered X days before expiry ("Extend or Pay" / التمديد أو الدفع).
5. FREE_ASSIGNMENT_OR_TRANSFER: Allowing beneficiary to transfer/assign the guarantee without applicant consent.
6. UNCAPPED_OR_COMPOUNDING_LIABILITY: Liability for interest, penalties, or legal costs above the guarantee face amount.
7. FOREIGN_LAW_OR_JURISDICTION: Onerous foreign governing law or exclusive foreign court jurisdiction.
8. MISSING_REDUCTION_SCHEDULE: Missing automatic pro-rata reduction clause when deliveries/milestones apply.
9. OTHER_ONEROUS_CLAUSE: Any other unusual trap (e.g., ultra-short cure window, payable in a different scarce currency, beneficiary-controlled expiry event).

Return ONLY valid JSON with this exact schema:
{{
  "is_guarantee_document": true | false,
  "detected_language": "AR" | "EN" | "BILINGUAL",
  "executive_summary": "<2 concise plain-English sentences summarizing overall wording safety for an executive approver (or explaining why is_guarantee_document is false if the file is an invoice, receipt, bank statement, or unrelated document)>",
  "findings": [
    {{
      "clause_type": "<one of the codes above>",
      "severity": "RED" | "AMBER",
      "title": "<short plain-English title>",
      "plain_explanation": "<1-2 plain-English sentences explaining the real business/financial risk>",
      "quoted_excerpt": "<exact short quote from the Arabic or English text>",
      "suggested_protective_rewording": "<short practical protective clause in matching language (or bilingual)>"
    }}
  ],
  "positive_protections": ["<short plain-English bullet of any protective clause already present>"]
}}
IMPORTANT: Set `"is_guarantee_document": false` if the document/text is NOT a Letter of Guarantee, Bank Guarantee draft, Standby LC, or contractual guarantee clause (for example, if it is a sales/catering invoice, bank cash statement, CV, receipt, or unrelated placeholder text).
"""
        contents: List[Any] = [prompt]
        if combined_text:
            contents.append(f"Guarantee Wording Text:\n{combined_text[:30000]}")
        if file_bytes:
            lower_fn = (filename or "").lower()
            mime = "application/pdf"
            if lower_fn.endswith(".png"):
                mime = "image/png"
            elif lower_fn.endswith((".jpg", ".jpeg")):
                mime = "image/jpeg"
            if mime in ("application/pdf", "image/png", "image/jpeg"):
                contents.append(genai_types.Part.from_bytes(data=file_bytes, mime_type=mime))

        try:
            response = await _safe_generate_content_async(
                client=client,
                model=GEMINI_MODEL_NAME,
                contents=contents,
                config=genai_types.GenerateContentConfig(
                    response_mime_type="application/json",
                ),
            )
            usage_meta = getattr(response, "usage_metadata", None)
            if usage_meta and db is not None and customer_id is not None and user_id is not None:
                try:
                    log_ai_usage_sync(
                        db,
                        customer_id,
                        user_id,
                        filename,
                        prompt_tokens=getattr(usage_meta, "prompt_token_count", 0),
                        completion_tokens=getattr(usage_meta, "candidates_token_count", 0),
                        call_type=(
                            "beneficiary_protection_wording_check"
                            if eff_perspective == "BENEFICIARY"
                            else "special_wording_trap_analysis"
                        ),
                    )
                except Exception as log_err:
                    logger.debug(f"Could not log special wording AI usage: {log_err}")

            ai_json = json.loads(response.text or "{}")
            if isinstance(ai_json, dict):
                analysis_engine = "HYBRID_AI_AND_RULES"
                if "is_guarantee_document" in ai_json and isinstance(ai_json["is_guarantee_document"], bool):
                    ai_is_guarantee_doc = ai_json["is_guarantee_document"]
                if ai_json.get("detected_language") in ("AR", "EN", "BILINGUAL"):
                    detected_lang = ai_json["detected_language"]
                if ai_json.get("executive_summary"):
                    ai_summary = str(ai_json["executive_summary"]).strip()

                default_other_code = (
                    "OTHER_BENEFICIARY_HURDLE" if eff_perspective == "BENEFICIARY" else "OTHER_ONEROUS_CLAUSE"
                )
                for ai_f in ai_json.get("findings") or []:
                    if not isinstance(ai_f, dict) or not ai_f.get("title"):
                        continue
                    ctype = str(ai_f.get("clause_type") or default_other_code).upper()
                    sev = str(ai_f.get("severity") or "AMBER").upper()
                    if sev not in ("RED", "AMBER"):
                        sev = "AMBER"
                    cleaned_finding = {
                        "clause_type": ctype,
                        "severity": sev,
                        "title": str(ai_f.get("title", "")).strip(),
                        "plain_explanation": str(ai_f.get("plain_explanation", "")).strip(),
                        "quoted_excerpt": str(ai_f.get("quoted_excerpt", "")).strip(),
                        "suggested_protective_rewording": str(ai_f.get("suggested_protective_rewording", "")).strip(),
                    }
                    if ctype in findings_by_type:
                        # Preserve highest severity & enrich explanation if deterministic caught it too
                        existing = findings_by_type[ctype]
                        if sev == "RED" and existing["severity"] != "RED":
                            existing["severity"] = "RED"
                        if cleaned_finding["quoted_excerpt"] and len(cleaned_finding["quoted_excerpt"]) > 8:
                            existing["quoted_excerpt"] = cleaned_finding["quoted_excerpt"]
                    else:
                        findings_by_type[ctype] = cleaned_finding

                for pos in ai_json.get("positive_protections") or []:
                    pos_str = str(pos).strip()
                    if pos_str and pos_str not in positive_protections:
                        positive_protections.append(pos_str)
        except Exception as ai_err:
            logger.warning(f"Gemini wording analysis ({eff_perspective}) fell back to deterministic bilingual scanner: {ai_err}")

    # Finalize sorted findings (RED first, then AMBER)
    final_findings = sorted(
        findings_by_type.values(),
        key=lambda x: (0 if x.get("severity") == "RED" else 1, x.get("clause_type", "")),
    )

    red_count = sum(1 for f in final_findings if f.get("severity") == "RED")
    amber_count = sum(1 for f in final_findings if f.get("severity") == "AMBER")

    risk_score = min(100, red_count * 42 + amber_count * 22)
    if positive_protections:
        risk_score = max(8 if (red_count or amber_count) else 0, risk_score - min(15, len(positive_protections) * 5))

    is_non_lg_doc = bool(
        ai_is_guarantee_doc is False
        or (
            ai_is_guarantee_doc is None
            and not is_det_guarantee_doc
            and red_count == 0
            and amber_count == 0
            and not positive_protections
        )
    )

    if is_non_lg_doc:
        risk_level = "INVALID"
        risk_score = 0
        final_findings = []
        positive_protections = []
        risk_label = "Invalid Document — Not a Letter of Guarantee"
        if not ai_summary:
            ai_summary = (
                "The submitted file or text does not appear to be a Letter of Guarantee or guarantee clause "
                "(for example, it may be an invoice, receipt, bank statement, or placeholder text). "
                "Please attach a draft Letter of Guarantee to scan for wording traps."
            )
    elif eff_perspective == "BENEFICIARY":
        if red_count > 0 or risk_score >= 65:
            risk_level = "RED"
            risk_label = "Weak Protection — Claim-Blocking Hurdles Detected"
        elif amber_count > 0 or risk_score >= 20:
            risk_level = "AMBER"
            risk_label = "Moderate Protection — Claim Conditions Need Review"
        else:
            risk_level = "GREEN"
            risk_label = "Strong Beneficiary Protection — Clean Demand Guarantee"

        if not ai_summary:
            if risk_level == "RED":
                ai_summary = (
                    f"Detected {red_count} critical claim-blocking hurdle(s) and {amber_count} caution condition(s) in this incoming guarantee. "
                    "Consider requesting an amendment from the vendor / issuing bank so your right to claim cannot be delayed or contested."
                )
            elif risk_level == "AMBER":
                ai_summary = (
                    f"No outright claim-blocking traps found, but {amber_count} documentary/operational condition(s) warrant review so claiming remains frictionless."
                )
            else:
                ai_summary = (
                    "Strong Beneficiary protection confirmed. No claim-blocking hurdles, vendor-consent gates, or conditional litigation triggers were detected."
                )
    else:
        if red_count > 0 or risk_score >= 65:
            risk_level = "RED"
            risk_label = "High Risk — Critical Wording Trap Detected"
        elif amber_count > 0 or risk_score >= 20:
            risk_level = "AMBER"
            risk_label = "Moderate Caution — Negotiable Clauses Found"
        else:
            risk_level = "GREEN"
            risk_label = "Low Risk — Standard Protective Wording"

        if not ai_summary:
            if risk_level == "RED":
                ai_summary = (
                    f"Detected {red_count} critical trap(s) and {amber_count} caution clause(s). "
                    "Review the highlighted terms and consider requesting protective rewording before bank issuance."
                )
            elif risk_level == "AMBER":
                ai_summary = (
                    f"No critical traps found, but {amber_count} caution clause(s) warrant review before approval."
                )
            else:
                ai_summary = (
                    "No high-risk guarantee traps were detected in the scanned wording. Standard protective terms appear intact."
                )

    return {
        "status": "OK",
        "advisory_only": True,
        "perspective": eff_perspective,
        "risk_level": risk_level,
        "risk_label": risk_label,
        "risk_score": risk_score,
        "detected_language": detected_lang,
        "red_count": red_count,
        "amber_count": amber_count,
        "executive_summary": ai_summary,
        "findings": final_findings,
        "positive_protections": positive_protections[:8],
        "analysis_engine": analysis_engine,
        "analyzed_source": filename if file_bytes else "Guarantee Wording Text",
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }
