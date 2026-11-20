# app/core/instruction_verification.py
"""
Platform-Enforced Instruction Verification & Mandatory QR Security Footer.

Security Model:
1. QR Token ('t'): Signed with HMAC-SHA256 using SECRET_KEY + (record_kind, record_id, secret_issuance_timestamp).
   The exact issuance timestamp (`created_at` / `instruction_date` epoch seconds) is known ONLY to the database
   and is never printed on the physical letter, making it impossible for an external party to forge a QR token.
2. 6-Digit Security Code ('XXX-XXX'): Deterministically derived via HMAC-SHA256 from
   (record_kind, record_id, serial_number, lg_number, secret_issuance_timestamp) and printed in the mandatory
   platform footer alongside the QR code.
3. Non-Bypassable Footer Injection: `document_generator.py` strips any template-level footer and injects the
   official Grow Business Development verification footer + QR code into every generated instruction PDF.
"""
import os
import re
import hmac
import hashlib
import base64
import logging
import threading
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

from app.core.routing import get_frontend_base_url

logger = logging.getLogger(__name__)

_RAW_SECRET = os.getenv("SECRET_KEY")
if not _RAW_SECRET:
    logger.warning(
        "SECRET_KEY is not set: instruction QR tokens/security codes are signed with a public fallback "
        "key and are FORGEABLE. Set SECRET_KEY in the environment."
    )
_SECRET_KEY = (_RAW_SECRET or "grow-bd-fallback-secret").encode("utf-8")

# Thread-safe bounded cache for newly flushed instructions (before outer transaction commits)
_RECENT_INSTRUCTIONS_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_CACHE_LOCK = threading.Lock()
_MAX_CACHE_SIZE = 500

# PDF filename hints for documents that have NO verification record by design, so they always get a
# sample QR footer. No warning is logged for these (it would fire on every print).
# NOTE: "lg_issuance_letter_" and "cancellation_notice_" ARE sent to banks but currently cannot be
# verified — supporting them needs a new record kind in this module.
_NON_LETTER_HINT_PREFIXES = (
    "Grow_Terms_of_Service",
    "application_",
    "req_",
    "document",
    "lg_issuance_letter_",
    "cancellation_notice_",
)

# Shown when someone scans a sample QR code (Template Studio preview, or a document type that has
# no verifiable instruction record). Must never say "verified", but also must not call a genuine
# issuance letter / cancellation notice "fake".
_PREVIEW_REJECTION_REASON = (
    "SAMPLE QR CODE – CANNOT BE VERIFIED. This QR code is not linked to an issued instruction record, "
    "so it cannot confirm that this letter is genuine. Do not rely on this QR code; contact the issuing "
    "company directly to confirm the letter."
)
_PREVIEW_STATUS_LABEL = "SAMPLE QR CODE – CANNOT BE VERIFIED"


def _to_epoch_seconds(dt: Any) -> int:
    """Converts a datetime object to integer epoch seconds (UTC)."""
    if dt is None:
        return 0
    if isinstance(dt, (int, float)):
        return int(dt)
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    return 0


def register_instruction_for_verification(
    record_id: int,
    serial_number: str,
    lg_number: str,
    created_at: Any,
    record_kind: str = "I",
) -> None:
    """
    Registers a newly flushed LGInstruction ('I') or IssuanceMaintenanceAction ('M') in memory
    so `document_generator.py` can resolve its exact ID and secret issuance timestamp immediately,
    even before the outer database transaction commits.
    """
    if not record_id:
        return
    secret_ts = _to_epoch_seconds(created_at) or int(datetime.now(timezone.utc).timestamp())
    clean_serial = (serial_number or "").strip()
    clean_kind = (record_kind or "I").strip().upper()
    meta = {
        "kind": clean_kind,
        "id": int(record_id),
        "serial_number": clean_serial,
        "lg_number": (lg_number or "").strip(),
        "secret_ts": secret_ts,
    }
    _cache_put(f"{clean_kind}:{int(record_id)}", meta)
    if clean_serial:
        _cache_put(clean_serial, meta)


def attach_verification_marker(
    html_content: str,
    serial_number: Optional[str] = None,
    record_kind: Optional[str] = None,
    record_id: Optional[int] = None,
) -> str:
    """
    Embeds an invisible verification metadata comment in rendered letter HTML so merged/bulk
    WeasyPrint pipelines can always resolve the exact database instruction record even when
    a customer uses a global or custom template without a visible serial line.
    """
    if not html_content or (not serial_number and not record_id):
        return html_content
    if "<!-- GROW_VERIFY_META:" in html_content:
        return html_content
    clean_kind = (record_kind or "").strip().upper()
    clean_id = int(record_id) if record_id else 0
    clean_serial = (serial_number or "").replace("-->", "").strip()
    marker = f"<!-- GROW_VERIFY_META:kind={clean_kind}|id={clean_id}|serial={clean_serial} -->"
    if re.search(r"</body>", html_content, flags=re.IGNORECASE):
        return re.sub(r"</body>", f"{marker}</body>", html_content, count=1, flags=re.IGNORECASE)
    return f"{html_content}\n{marker}"


def _extract_embedded_verify_marker(html_content: str) -> Optional[Dict[str, Any]]:
    if not html_content:
        return None
    m = re.search(
        r"<!--\s*GROW_VERIFY_META:kind=([A-Za-z]*)\|id=(\d+)\|serial=([^>]*?)\s*-->",
        html_content,
    )
    if not m:
        return None
    return {
        "kind": m.group(1).strip().upper(),
        "id": int(m.group(2)),
        "serial": m.group(3).strip(),
    }


def _cache_put(key: str, meta: Dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _RECENT_INSTRUCTIONS_CACHE[key] = meta
        _RECENT_INSTRUCTIONS_CACHE.move_to_end(key)
        while len(_RECENT_INSTRUCTIONS_CACHE) > _MAX_CACHE_SIZE:
            _RECENT_INSTRUCTIONS_CACHE.popitem(last=False)


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    with _CACHE_LOCK:
        return _RECENT_INSTRUCTIONS_CACHE.get(key)


def _compute_hmac_sig(payload: str, length: int = 16) -> str:
    digest = hmac.new(_SECRET_KEY, payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")[:length]


def _generate_v1_token_and_6digit_code(
    record_kind: str,
    record_id: int,
    serial_number: str,
    lg_number: str,
    secret_ts: int,
) -> Tuple[str, str]:
    """
    Legacy v1 generator (6-digit footer code 'XXX-XXX' with token kind 'I', 'M', 'P').
    Preserved so all previously printed v1 instructions continue to verify seamlessly.
    """
    base_kind = record_kind.rstrip("2")
    sig = _compute_hmac_sig(f"QR_TOKEN:{base_kind}.{record_id}:{secret_ts}:{serial_number}", length=18)
    token = f"{base_kind}.{record_id}.{sig}"

    gate_payload = f"GATE_PIN:{base_kind}:{record_id}:{(serial_number or '').strip().upper()}:{(lg_number or '').strip().upper()}:{secret_ts}"
    gate_digest = hmac.new(_SECRET_KEY, gate_payload.encode("utf-8"), hashlib.sha256).digest()
    code_int = (int.from_bytes(gate_digest[:4], "big") % 900000) + 100000
    code_str = f"{str(code_int)[:3]}-{str(code_int)[3:]}"
    return token, code_str


def _extract_lg_last2_candidates(lg_number: str) -> Tuple[str, str]:
    """
    Returns (alphanumeric_last2, raw_nonspace_last2) in uppercase for matching the last 2 characters
    of the LG Number printed in the body of the instruction.
    """
    raw = re.sub(r"\s+", "", (lg_number or "").strip()).upper()
    alnum = re.sub(r"[^A-Z0-9]", "", raw)
    c1 = alnum[-2:] if len(alnum) >= 2 else alnum.zfill(2)
    c2 = raw[-2:] if len(raw) >= 2 else raw.zfill(2)
    return c1, c2


def _matches_lg_last2(lg_number: str, entered_lg_tail: str) -> bool:
    """Checks if the user-entered 2 characters match the last 2 characters of the LG Number."""
    entered_raw = re.sub(r"\s+", "", (entered_lg_tail or "").strip()).upper()
    entered_alnum = re.sub(r"[^A-Z0-9]", "", entered_raw)
    if not entered_raw:
        return False
    c1, c2 = _extract_lg_last2_candidates(lg_number)
    return entered_alnum == c1 or entered_raw == c2


def generate_verification_token_and_code(
    record_kind: str,
    record_id: int,
    serial_number: str,
    lg_number: str,
    secret_ts: int,
) -> Tuple[str, str]:
    """
    Generates a v2 split-verification QR token ('I2.<id>.<sig>') and 4-digit Footer Security Code ('XXXX').
    During verification, the user must provide BOTH:
      1) The last 2 characters of the LG Number (from the upper body of the letter), AND
      2) The 4-digit Security Code (from the bottom footer).
    Note: secret_ts is mixed into the cryptographic HMAC signature and 4-digit PIN
    calculation, but is NEVER exposed in the token string or portal response.
    """
    base_kind = record_kind.rstrip("2")
    v2_kind = f"{base_kind}2"
    sig = _compute_hmac_sig(f"QR_TOKEN:{v2_kind}.{record_id}:{secret_ts}:{serial_number}", length=18)
    token = f"{v2_kind}.{record_id}.{sig}"

    # 4-digit footer security code (1000..9999) derived from secret_ts + instruction identity + LG number
    gate_payload = f"GATE_PIN_V2:{base_kind}:{record_id}:{(serial_number or '').strip().upper()}:{(lg_number or '').strip().upper()}:{secret_ts}"
    gate_digest = hmac.new(_SECRET_KEY, gate_payload.encode("utf-8"), hashlib.sha256).digest()
    code_int = (int.from_bytes(gate_digest[:4], "big") % 9000) + 1000  # Always 4 digits: 1000..9999
    return token, str(code_int)


def generate_qr_svg_data_uri(url: str) -> str:
    """
    Generates a crisp vector SVG QR code as a base64 data URI using ReportLab (no network calls).
    """
    try:
        from reportlab.graphics.barcode import qr
        from reportlab.graphics.shapes import Drawing
        from reportlab.graphics import renderSVG

        qr_widget = qr.QrCodeWidget(url, barLevel="M", barBorder=1)
        bounds = qr_widget.getBounds()
        w = bounds[2] - bounds[0]
        h = bounds[3] - bounds[1]
        size = 90.0
        drawing = Drawing(size, size, transform=[size / w, 0, 0, size / h, 0, 0])
        drawing.add(qr_widget)
        svg_content = renderSVG.drawToString(drawing)
        svg_bytes = svg_content.encode("utf-8") if isinstance(svg_content, str) else svg_content
        b64 = base64.b64encode(svg_bytes).decode("ascii")
        return f"data:image/svg+xml;base64,{b64}"
    except Exception as e:
        logger.warning(f"Failed to generate QR SVG: {e}")
        return ""


def _extract_serial_from_html_or_hint(html_content: str, filename_hint: str = "") -> Optional[str]:
    """
    Extracts the instruction serial number from the rendered HTML or filename_hint.
    """
    # 1. Check common HTML patterns: Ref: <strong>...</strong>, Instruction Ref: <strong>...</strong>, or Serial: ...
    patterns = [
        r"(?:Instruction\s+Ref|Original\s+Ref|Internal\s+Ref|Ref|Serial)\s*:\s*<strong[^>]*>\s*([A-Za-z0-9_\-/]+)\s*</strong>",
        r"(?:Instruction\s+Ref|Original\s+Ref|Internal\s+Ref|Ref|Serial)\s*:\s*([A-Za-z0-9_\-/]{6,40})",
    ]
    for pat in patterns:
        m = re.search(pat, html_content, flags=re.IGNORECASE)
        if m:
            candidate = m.group(1).strip()
            if candidate and "{{" not in candidate and candidate.upper() != "N/A":
                return candidate

    # 2. Check filename_hint (e.g. lg_instruction_ORGPF0042EXT0001001O or maint_...)
    if filename_hint:
        for prefix in ("lg_instruction_", "lg_bank_reminder_", "maint_"):
            if filename_hint.startswith(prefix):
                cand = filename_hint[len(prefix):].strip()
                if cand and cand != "None":
                    return cand

    return None


def _resolve_instruction_meta(html_content: str, filename_hint: str = "") -> Dict[str, Any]:
    """
    Resolves (kind, id, serial_number, lg_number, secret_ts) from the embedded marker, in-memory cache,
    or database. Falls back to a deterministic preview token if rendering a studio preview.
    """
    embedded = _extract_embedded_verify_marker(html_content)
    serial_from_marker: Optional[str] = None
    if embedded:
        emb_kind = embedded["kind"]
        emb_id = embedded["id"]
        emb_serial = embedded["serial"]

        if emb_kind and emb_id > 0:
            cached_by_id = _cache_get(f"{emb_kind}:{emb_id}")
            if cached_by_id:
                return cached_by_id

        if emb_serial:
            cached_by_serial = _cache_get(emb_serial)
            if cached_by_serial and (emb_id <= 0 or cached_by_serial.get("id") == emb_id):
                return cached_by_serial

        if emb_kind in ("I", "M") and emb_id > 0:
            try:
                from app.database import SessionLocal
                from sqlalchemy.orm import selectinload

                db = SessionLocal()
                try:
                    if emb_kind == "I":
                        from app.models.models import LGInstruction
                        inst = (
                            db.query(LGInstruction)
                            .options(selectinload(LGInstruction.lg_record))
                            .filter(LGInstruction.id == emb_id, LGInstruction.is_deleted == False)
                            .first()
                        )
                        if inst and inst.lg_record and not inst.lg_record.is_deleted:
                            ts = _to_epoch_seconds(getattr(inst, "created_at", None) or getattr(inst, "instruction_date", None))
                            lg_num = inst.lg_record.lg_number or ""
                            meta = {
                                "kind": "I",
                                "id": int(inst.id),
                                "serial_number": inst.serial_number or "",
                                "lg_number": lg_num or "",
                                "secret_ts": ts,
                            }
                            _cache_put(f"I:{inst.id}", meta)
                            if inst.serial_number:
                                _cache_put(inst.serial_number.strip(), meta)
                            return meta
                    elif emb_kind == "M":
                        from app.models.models_issuance import IssuanceMaintenanceAction
                        maint = (
                            db.query(IssuanceMaintenanceAction)
                            .options(selectinload(IssuanceMaintenanceAction.issued_lg))
                            .filter(IssuanceMaintenanceAction.id == emb_id, IssuanceMaintenanceAction.is_deleted == False)
                            .first()
                        )
                        if maint and maint.issued_lg and not getattr(maint.issued_lg, "is_deleted", False):
                            ts = _to_epoch_seconds(getattr(maint, "created_at", None))
                            lg_num = maint.issued_lg.bank_lg_number or maint.issued_lg.lg_ref_number or ""
                            meta = {
                                "kind": "M",
                                "id": int(maint.id),
                                "serial_number": maint.letter_serial_number or "",
                                "lg_number": lg_num or "",
                                "secret_ts": ts,
                            }
                            _cache_put(f"M:{maint.id}", meta)
                            if maint.letter_serial_number:
                                _cache_put(maint.letter_serial_number.strip(), meta)
                            return meta
                finally:
                    db.close()
            except Exception as e:
                logger.debug(f"Could not query DB via embedded verification marker ({embedded}): {e}")

        if emb_serial:
            serial_from_marker = emb_serial

    serial = serial_from_marker or _extract_serial_from_html_or_hint(html_content, filename_hint)
    cached = _cache_get(serial) if serial else None
    if cached:
        return cached

    if serial:
        try:
            from app.database import SessionLocal
            from app.models.models import LGInstruction
            from sqlalchemy.orm import selectinload

            db = SessionLocal()
            try:
                inst = (
                    db.query(LGInstruction)
                    .options(selectinload(LGInstruction.lg_record))
                    .filter(LGInstruction.serial_number == serial, LGInstruction.is_deleted == False)
                    .order_by(LGInstruction.id.desc())
                    .first()
                )
                if inst and inst.lg_record and not inst.lg_record.is_deleted:
                    ts = _to_epoch_seconds(getattr(inst, "created_at", None) or getattr(inst, "instruction_date", None))
                    lg_num = inst.lg_record.lg_number or ""
                    meta = {
                        "kind": "I",
                        "id": int(inst.id),
                        "serial_number": inst.serial_number,
                        "lg_number": lg_num or "",
                        "secret_ts": ts,
                    }
                    _cache_put(serial, meta)
                    return meta

                # Also check IssuanceMaintenanceAction for Issuance module letters
                try:
                    from app.models.models_issuance import IssuanceMaintenanceAction
                    maint = (
                        db.query(IssuanceMaintenanceAction)
                        .options(selectinload(IssuanceMaintenanceAction.issued_lg))
                        .filter(
                            IssuanceMaintenanceAction.letter_serial_number == serial,
                            IssuanceMaintenanceAction.is_deleted == False,
                        )
                        .order_by(IssuanceMaintenanceAction.id.desc())
                        .first()
                    )
                    if maint and maint.issued_lg and not getattr(maint.issued_lg, "is_deleted", False):
                        ts = _to_epoch_seconds(getattr(maint, "created_at", None))
                        lg_num = maint.issued_lg.bank_lg_number or maint.issued_lg.lg_ref_number or ""
                        meta = {
                            "kind": "M",
                            "id": int(maint.id),
                            "serial_number": maint.letter_serial_number,
                            "lg_number": lg_num or "",
                            "secret_ts": ts,
                        }
                        _cache_put(serial, meta)
                        return meta
                except Exception:
                    pass
            finally:
                db.close()
        except Exception as e:
            logger.debug(f"Could not query DB for instruction verification meta ({serial}): {e}")

    # Fallback for Template Studio live previews or sample renders.
    # Warn when this happens during real PDF generation (a filename_hint is passed) or when a
    # serial WAS found but no matching record exists — that means a real letter is about to be
    # printed with a SAMPLE QR code that will fail verification at the bank.
    # Template Studio in-browser previews pass no hint and no real serial, so they stay quiet.
    hint = (filename_hint or "").strip()
    is_known_non_letter = hint.startswith(_NON_LETTER_HINT_PREFIXES)
    if (serial or hint) and not is_known_non_letter:
        logger.warning(
            "Instruction verification: could not resolve a real instruction record "
            f"(extracted serial={serial!r}, filename_hint={hint!r}). The document will carry a SAMPLE "
            "QR code that will NOT verify. Check that the template prints the instruction serial as "
            "'Ref: <strong>SERIAL</strong>' and that the record was saved/registered before rendering."
        )

    return {
        "kind": "P",
        "id": 0,
        "serial_number": serial or "SAMPLE-REF-0001",
        "lg_number": "LG-2026-4120",
        "secret_ts": 1762600000,
    }


def strip_legacy_template_footers(html_content: str) -> str:
    """
    Removes any legacy template-level footer HTML, platform sign-off taglines, or CSS page-counter rules
    (<div class="footer">, <div class="footer-note">, <footer>, "For and on behalf of ... Platform",
    and @bottom-* rules) so that the platform-enforced security footer is the sole footer on every instruction.
    """
    if not html_content:
        return html_content

    cleaned = html_content
    # 1. Remove <div class="footer">...</div> and <div class="footer-note">...</div>
    cleaned = re.sub(
        r'\s*<div\s+class=["\']footer(?:-note)?["\'][^>]*>.*?</div>',
        "",
        cleaned,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # 2. Remove <footer>...</footer> tags if any
    cleaned = re.sub(
        r'\s*<footer\b[^>]*>.*?</footer>',
        "",
        cleaned,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # 3. Remove legacy "<em ...>For and on behalf of ... Platform</em>" line in closing blocks
    cleaned = re.sub(
        r'\s*<em\b[^>]*>\s*For and on behalf of[^<]*Platform\s*</em>',
        "",
        cleaned,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # 4. Expand cramped signature spacing (.signature { margin-top: 0.8em; }) in standard/custom templates
    cleaned = re.sub(
        r'(\.signature\s*\{\s*)margin-top:\s*0\.8em;',
        r'\1margin-top: 4.5em;',
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r'(\.closing\s*\{\s*margin-top:\s*2em;\s*text-align:\s*left;)(\s*\})',
        r'\1\n            margin-bottom: 2.2em;\2',
        cleaned,
        flags=re.IGNORECASE,
    )
    # 5. Expand cramped signature spacing in Bank Companion Letter (25px/40px -> 56px)
    if "Bank Instruction Letter & Annex" in cleaned:
        cleaned = re.sub(
            r'margin-top:\s*(?:25|40)px;\s*border-top:\s*1px\s+solid\s+#94a3b8;',
            'margin-top: 56px; border-top: 1px solid #94a3b8;',
            cleaned,
            flags=re.IGNORECASE,
        )
    # 6. Expand cramped "<br/><p>Sincerely,<br/>{{customer_name}}</p>" in Issuance maintenance letters
    cleaned = re.sub(
        r'<br\s*/?>\s*<p>\s*Sincerely,\s*<br\s*/?>\s*\{\{customer_name\}\}\s*</p>',
        '<div style="margin-top: 24px; margin-bottom: 32px;"><p style="margin-bottom: 52px;">Sincerely,</p><p style="font-weight: bold; margin: 0;">{{customer_name}}</p><p style="margin: 2px 0 0 0; font-size: 9pt; color: #475569;">Authorized Signatory</p></div>',
        cleaned,
        flags=re.IGNORECASE,
    )
    # 7. Remove @bottom-center / @bottom-left / @bottom-right CSS margin boxes (e.g., Page X of Y)
    cleaned = re.sub(
        r'\s*@bottom-(?:center|left|right)\s*\{[^}]*\}',
        "",
        cleaned,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return cleaned


def inject_mandatory_verification_footer(html_content: str, filename_hint: str = "") -> str:
    """
    Enforces the platform-wide Grow Business Development security footer with QR code and
    4-digit Security Code (v2) on any letter HTML. Strips any template-level `.footer` or
    `.footer-note` so templates cannot duplicate or bypass it.
    """
    if not html_content or "</body>" not in html_content.lower():
        return html_content

    # Avoid double-injection if already present
    if 'id="grow-mandatory-security-footer"' in html_content:
        return html_content

    # 1. Strip any existing template-level footers or @bottom-* page counters
    cleaned_html = strip_legacy_template_footers(html_content)

    # 2. Resolve instruction metadata & generate cryptographic QR token + 6-digit code
    meta = _resolve_instruction_meta(cleaned_html, filename_hint)
    token, code_formatted = generate_verification_token_and_code(
        record_kind=meta["kind"],
        record_id=meta["id"],
        serial_number=meta["serial_number"],
        lg_number=meta["lg_number"],
        secret_ts=meta["secret_ts"],
    )

    frontend_base = get_frontend_base_url().rstrip("/")
    verify_url = f"{frontend_base}/public/verify-instruction?t={token}"
    qr_data_uri = generate_qr_svg_data_uri(verify_url)

    qr_img_html = (
        f'<img src="{qr_data_uri}" alt="Verify QR" style="width: 52px; height: 52px; display: block; border: 1px solid #cbd5e1; padding: 2px; background: #ffffff;" />'
        if qr_data_uri
        else ""
    )

    footer_html = f"""
    <div id="grow-mandatory-security-footer" style="margin-top: 16px; border-top: 1.5px solid #cbd5e1; padding-top: 7px; page-break-inside: avoid; font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif;">
        <table style="width: 100%; border-collapse: collapse; border: none; margin: 0; padding: 0;">
            <tr>
                <td style="vertical-align: middle; text-align: left; border: none; padding: 0; font-size: 7.5pt; line-height: 1.45; color: #475569;">
                    <div style="color: #0f172a; font-weight: 700; font-size: 8pt;">
                        Official Corporate Banking Instruction &bull; Generated via <span style="color: #0f172a;">Grow Business Development</span> &mdash; Treasury Management Platform
                    </div>
                    <div style="margin-top: 2px;">
                        <strong>Authenticity Verification:</strong> Scan the QR code and enter the <strong>last 2 characters of the LG Number</strong> + Security Code:
                        <span style="display: inline-block; font-family: 'Courier New', Courier, monospace; font-weight: 800; font-size: 8.5pt; color: #0f172a; background-color: #f1f5f9; border: 1px solid #94a3b8; padding: 0px 5px; letter-spacing: 0.8px;">{code_formatted}</span>
                    </div>
                </td>
                <td style="width: 60px; vertical-align: middle; text-align: right; border: none; padding: 0 0 0 8px;">
                    {qr_img_html}
                </td>
            </tr>
        </table>
    </div>
"""

    # Insert right before </body>
    idx = cleaned_html.lower().rfind("</body>")
    if idx != -1:
        return cleaned_html[:idx] + footer_html + "\n" + cleaned_html[idx:]
    return cleaned_html + footer_html


def _mask_text(value: str, visible_start: int = 2, visible_end: int = 2) -> str:
    """Masks a string while keeping key identifying start/end characters for visual assurance."""
    if not value:
        return "N/A"
    s = value.strip()
    if len(s) <= visible_start + visible_end + 1:
        return s[0] + "***" if len(s) > 1 else "***"
    words = s.split()
    if len(words) > 1:
        masked_words = []
        for w in words:
            if len(w) <= 3:
                masked_words.append(w)
            else:
                masked_words.append(w[:2] + "*" * min(4, len(w) - 3) + w[-1:])
        return " ".join(masked_words)
    return s[:visible_start] + "*" * min(6, len(s) - visible_start - visible_end) + s[-visible_end:]


def _mask_lg_number(lg_number: str) -> str:
    if not lg_number:
        return "N/A"
    s = lg_number.strip()
    if len(s) <= 4:
        return "**" + s[-2:]
    return "*" * min(6, len(s) - 4) + s[-4:]


# ==============================================================================
# PROGRESSIVE COOLDOWN ("BOREDOM" FRICTION) PER CLIENT IP + TOKEN
# ==============================================================================
# 2 free typo attempts (attempts 1 & 2 -> 0s cooldown)
# Starting on 3rd failed attempt: 1m, 3m, 5m, 10m, 15m, 30m (max cap, never permanent)
_FREE_TYPO_ATTEMPTS = 2
_COOLDOWN_SCHEDULE_SECONDS = [60, 180, 300, 600, 900, 1800]
_INACTIVITY_RESET_SECONDS = 3600  # Reset failure counter after 1 hour of no failed tries
# Per-letter limit (shared by ALL visitors, regardless of IP) — defeats IP rotation/spoofing.
# Slightly more generous than the per-IP limit so a genuine bank is rarely affected.
import tempfile
try:
    import fcntl as _fcntl
except ImportError:
    _fcntl = None

from contextlib import contextmanager

_LETTER_FREE_ATTEMPTS = 5
_COOLDOWN_MEMORY_STORE: Dict[str, Dict[str, float]] = {}
_COOLDOWN_LOCK = threading.Lock()


def _at_fork_prepare() -> None:
    _CACHE_LOCK.acquire()
    _COOLDOWN_LOCK.acquire()


def _at_fork_parent() -> None:
    try:
        _COOLDOWN_LOCK.release()
    finally:
        _CACHE_LOCK.release()


def _at_fork_child() -> None:
    global _CACHE_LOCK, _COOLDOWN_LOCK
    _CACHE_LOCK = threading.Lock()
    _COOLDOWN_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_at_fork_prepare,
        after_in_parent=_at_fork_parent,
        after_in_child=_at_fork_child,
    )


def _canonical_token_key(token: str) -> str:
    """Normalizes a token so v1, v2, 4-part legacy, whitespace, and forged signatures for one letter share one bucket."""
    clean = (token or "").strip()
    if clean and clean.isascii() and clean.count(".") in (2, 3):
        parts = clean.split(".")
        base_kind = parts[0].rstrip("2")
        if base_kind in ("I", "M") and parts[1].isdigit():
            return f"{base_kind}:{int(parts[1])}"
    return f"raw:{hashlib.sha256(clean.encode('utf-8')).hexdigest()[:24]}"


def _letter_key(token: str) -> str:
    return f"letter:{_canonical_token_key(token)}"


def _get_cooldown_file_path() -> str:
    return os.path.join(tempfile.gettempdir(), "grow_verify_ip_cooldown.json")


@contextmanager
def _cooldown_file_lock():
    """Cross-process file lock on POSIX (no-op on Windows where thread lock already serializes)."""
    if _fcntl is None:
        yield
        return
    lock_path = f"{_get_cooldown_file_path()}.lock"
    lock_fh = None
    try:
        lock_fh = open(lock_path, "a+", encoding="utf-8")
        _fcntl.flock(lock_fh.fileno(), _fcntl.LOCK_EX)
    except Exception:
        if lock_fh is not None:
            try:
                lock_fh.close()
            except Exception:
                pass
            lock_fh = None
    try:
        yield
    finally:
        if lock_fh is not None:
            try:
                _fcntl.flock(lock_fh.fileno(), _fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                lock_fh.close()
            except Exception:
                pass


def _load_cooldown_store() -> Dict[str, Dict[str, float]]:
    import json
    now = datetime.now(timezone.utc).timestamp()
    store = dict(_COOLDOWN_MEMORY_STORE)
    path = _get_cooldown_file_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                disk_data = json.load(f)
            if isinstance(disk_data, dict):
                for k, v in disk_data.items():
                    if isinstance(v, dict):
                        mem_v = store.get(k)
                        if not mem_v or float(v.get("last_fail", 0)) >= float(mem_v.get("last_fail", 0)):
                            store[k] = v
        except Exception:
            pass
    # Prune expired entries older than _INACTIVITY_RESET_SECONDS
    cleaned = {
        k: v
        for k, v in store.items()
        if now - float(v.get("last_fail", 0)) < _INACTIVITY_RESET_SECONDS
    }
    _COOLDOWN_MEMORY_STORE.clear()
    _COOLDOWN_MEMORY_STORE.update(cleaned)
    return cleaned


def _save_cooldown_store(store: Dict[str, Dict[str, float]]) -> None:
    import json
    _COOLDOWN_MEMORY_STORE.clear()
    _COOLDOWN_MEMORY_STORE.update(store)
    path = _get_cooldown_file_path()
    target_dir = os.path.dirname(path) or tempfile.gettempdir()
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target_dir,
            prefix=".grow_verify_cd_",
            suffix=".tmp",
            delete=False,
        ) as tf:
            tmp_path = tf.name
            json.dump(store, tf)
            tf.flush()
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


def _format_duration_human(seconds: int) -> str:
    if seconds >= 60:
        mins = (seconds + 59) // 60
        return f"{mins} minute{'s' if mins != 1 else ''}"
    return f"{seconds} second{'s' if seconds != 1 else ''}"


def _check_cooldown_status(client_ip: str, token: str) -> int:
    """
    Returns remaining cooldown seconds (> 0) if this (client_ip, token) or client_ip is currently in cooldown,
    or 0 if allowed to proceed.
    """
    ip = (client_ip or "unknown").strip()
    canon = _canonical_token_key(token)
    tok_key = f"ip_tok:{ip}:{canon}"
    ip_key = f"ip_all:{ip}"
    l_key = _letter_key(token)
    now = datetime.now(timezone.utc).timestamp()

    with _COOLDOWN_LOCK:
        with _cooldown_file_lock():
            store = _load_cooldown_store()
            rem_tok = int(max(0.0, float(store.get(tok_key, {}).get("locked_until", 0.0)) - now))
            rem_ip = int(max(0.0, float(store.get(ip_key, {}).get("locked_until", 0.0)) - now))
            rem_letter = int(max(0.0, float(store.get(l_key, {}).get("locked_until", 0.0)) - now))
            return max(rem_tok, rem_ip, rem_letter)


def _record_failed_attempt(client_ip: str, token: str) -> Tuple[int, int, int]:
    """
    Records a failed verification attempt for (client_ip, token) and returns:
    (fail_count, cooldown_seconds_applied, free_attempts_remaining).
    """
    ip = (client_ip or "unknown").strip()
    canon = _canonical_token_key(token)
    tok_key = f"ip_tok:{ip}:{canon}"
    ip_key = f"ip_all:{ip}"
    letter_key = _letter_key(token)
    now = datetime.now(timezone.utc).timestamp()

    with _COOLDOWN_LOCK:
        with _cooldown_file_lock():
            store = _load_cooldown_store()

            # 1. Update per-(IP + Canonical Token) counter
            entry = dict(store.get(tok_key, {"fails": 0, "last_fail": 0.0, "locked_until": 0.0}))
            fails = int(entry.get("fails", 0)) + 1
            cooldown_sec = 0
            if fails > _FREE_TYPO_ATTEMPTS:
                idx = min(fails - _FREE_TYPO_ATTEMPTS - 1, len(_COOLDOWN_SCHEDULE_SECONDS) - 1)
                cooldown_sec = _COOLDOWN_SCHEDULE_SECONDS[idx]

            entry["fails"] = fails
            entry["last_fail"] = now
            entry["locked_until"] = (now + cooldown_sec) if cooldown_sec > 0 else 0.0
            store[tok_key] = entry

            # 2. Update broader per-IP counter across all tokens (allows 5 free tries across tokens before cooldown)
            ip_entry = dict(store.get(ip_key, {"fails": 0, "last_fail": 0.0, "locked_until": 0.0}))
            ip_fails = int(ip_entry.get("fails", 0)) + 1
            ip_cooldown_sec = 0
            if ip_fails > 5:
                ip_idx = min(ip_fails - 6, len(_COOLDOWN_SCHEDULE_SECONDS) - 1)
                ip_cooldown_sec = _COOLDOWN_SCHEDULE_SECONDS[ip_idx]
            ip_entry["fails"] = ip_fails
            ip_entry["last_fail"] = now
            ip_entry["locked_until"] = max(
                float(ip_entry.get("locked_until", 0.0)),
                (now + ip_cooldown_sec) if ip_cooldown_sec > 0 else 0.0,
            )
            store[ip_key] = ip_entry

            # 3. Update per-LETTER counter (independent of IP)
            letter_entry = dict(store.get(letter_key, {"fails": 0, "last_fail": 0.0, "locked_until": 0.0}))
            letter_fails = int(letter_entry.get("fails", 0)) + 1
            letter_cooldown_sec = 0
            if letter_fails > _LETTER_FREE_ATTEMPTS:
                l_idx = min(letter_fails - _LETTER_FREE_ATTEMPTS - 1, len(_COOLDOWN_SCHEDULE_SECONDS) - 1)
                letter_cooldown_sec = _COOLDOWN_SCHEDULE_SECONDS[l_idx]
            letter_entry["fails"] = letter_fails
            letter_entry["last_fail"] = now
            letter_entry["locked_until"] = (now + letter_cooldown_sec) if letter_cooldown_sec > 0 else 0.0
            store[letter_key] = letter_entry

            _save_cooldown_store(store)
            effective_cooldown = max(cooldown_sec, ip_cooldown_sec, letter_cooldown_sec)
            free_remaining = max(0, min(_FREE_TYPO_ATTEMPTS - fails, _LETTER_FREE_ATTEMPTS - letter_fails))
            return fails, effective_cooldown, free_remaining


def _reset_cooldown_on_success(client_ip: str, token: str) -> None:
    """Clears the per-(IP + Token) and per-letter failure counters upon a valid verification."""
    ip = (client_ip or "unknown").strip()
    tok_key = f"ip_tok:{ip}:{_canonical_token_key(token)}"
    l_key = _letter_key(token)
    with _COOLDOWN_LOCK:
        with _cooldown_file_lock():
            store = _load_cooldown_store()
            changed = False
            for key in (tok_key, l_key):
                if key in store:
                    del store[key]
                    changed = True
            if changed:
                _save_cooldown_store(store)


def inspect_verification_token(db: Any, token: str, client_ip: str = "unknown") -> Dict[str, Any]:
    """
    Validates the QR token signature and returns minimal gate prompt info (without revealing LG details
    until the Security Code is entered). Supports both v1 (6-digit footer code) and v2 (2 LG chars + 4-digit footer code).
    """
    token = (token or "").strip()
    rem_cooldown = _check_cooldown_status(client_ip, token)

    # ASCII-only guard: hmac.compare_digest raises TypeError on non-ASCII str, and str.isdigit()
    # accepts Unicode digits (e.g. '²') that int() rejects -> both would surface as HTTP 500.
    if not token or not token.isascii() or token.count(".") not in (2, 3):
        return {"valid_token": False, "reason": "Malformed verification link."}

    parts = token.split(".")
    if len(parts) == 4:
        kind, id_str, _legacy_ts_str, sig = parts
        sig_len = 16
    else:
        kind, id_str, sig = parts
        sig_len = 18

    is_v2 = kind.endswith("2")
    base_kind = kind.rstrip("2")
    version = 2 if is_v2 else 1

    if base_kind == "P":
        # Sample/preview QR codes (Template Studio) must NEVER be presented as a valid instruction,
        # otherwise a fraudster could copy a sample QR + code onto a fake letter.
        return {
            "valid_token": False,
            "is_preview": True,
            "version": version,
            "cooldown_seconds": rem_cooldown,
            "reason": _PREVIEW_REJECTION_REASON,
        }

    if base_kind not in ("I", "M") or not id_str.isdigit():
        return {"valid_token": False, "reason": "Invalid verification token parameters."}

    record_id = int(id_str)

    if base_kind == "I":
        from app.models.models import LGInstruction
        from sqlalchemy.orm import selectinload
        inst = (
            db.query(LGInstruction)
            .options(selectinload(LGInstruction.lg_record))
            .filter(LGInstruction.id == record_id, LGInstruction.is_deleted == False)
            .first()
        )
        if not inst or not inst.lg_record or inst.lg_record.is_deleted:
            return {"valid_token": False, "reason": "Instruction record not found."}
        db_ts = _to_epoch_seconds(getattr(inst, "created_at", None) or getattr(inst, "instruction_date", None))
        expected_sig = _compute_hmac_sig(
            f"QR_TOKEN:{kind}.{record_id}.{db_ts}:{inst.serial_number}"
            if sig_len == 16
            else f"QR_TOKEN:{kind}.{record_id}:{db_ts}:{inst.serial_number}",
            length=sig_len,
        )
        if not hmac.compare_digest(sig, expected_sig):
            return {"valid_token": False, "reason": "Cryptographic signature mismatch."}
        serial = inst.serial_number or ""
        masked_serial = serial[:6] + "••••" + serial[-5:] if len(serial) > 12 else serial
        return {
            "valid_token": True,
            "is_preview": False,
            "version": version,
            "masked_serial": masked_serial,
            "cooldown_seconds": rem_cooldown,
            "prompt": (
                "Enter the last 2 characters of the LG Number (from the letter body) and the 4-digit Security Code (from the footer)."
                if version == 2
                else "Enter the 6-digit Security Code printed in the footer of the instruction letter."
            ),
        }

    # base_kind == "M"
    from app.models.models_issuance import IssuanceMaintenanceAction
    from sqlalchemy.orm import selectinload
    maint = (
        db.query(IssuanceMaintenanceAction)
        .options(selectinload(IssuanceMaintenanceAction.issued_lg))
        .filter(IssuanceMaintenanceAction.id == record_id, IssuanceMaintenanceAction.is_deleted == False)
        .first()
    )
    if not maint or not maint.issued_lg or getattr(maint.issued_lg, "is_deleted", False):
        return {"valid_token": False, "reason": "Maintenance instruction record not found."}
    db_ts = _to_epoch_seconds(getattr(maint, "created_at", None))
    expected_sig = _compute_hmac_sig(
        f"QR_TOKEN:{kind}.{record_id}.{db_ts}:{maint.letter_serial_number}"
        if sig_len == 16
        else f"QR_TOKEN:{kind}.{record_id}:{db_ts}:{maint.letter_serial_number}",
        length=sig_len,
    )
    if not hmac.compare_digest(sig, expected_sig):
        return {"valid_token": False, "reason": "Cryptographic signature mismatch."}
    serial = maint.letter_serial_number or ""
    masked_serial = serial[:6] + "••••" + serial[-4:] if len(serial) > 10 else serial
    return {
        "valid_token": True,
        "is_preview": False,
        "version": version,
        "masked_serial": masked_serial,
        "cooldown_seconds": rem_cooldown,
        "prompt": (
            "Enter the last 2 characters of the LG Number (from the letter body) and the 4-digit Security Code (from the footer)."
            if version == 2
            else "Enter the 6-digit Security Code printed in the footer of the instruction letter."
        ),
    }


def _build_failed_code_response(client_ip: str, token: str, custom_reason: Optional[str] = None, tamper_alert: bool = False) -> Dict[str, Any]:
    """Records a failed verification attempt and returns a structured response with progressive cooldown."""
    fails, cooldown_sec, free_rem = _record_failed_attempt(client_ip, token)
    if cooldown_sec > 0:
        base_msg = (
            custom_reason
            if custom_reason
            else f"Invalid Security Code (Attempt #{fails})."
        )
        return {
            "verified": False,
            "tamper_alert": tamper_alert,
            "cooldown_seconds": cooldown_sec,
            "failed_attempts": fails,
            "reason": (
                f"{base_msg} To protect instruction security, "
                f"verification from your connection is paused for {_format_duration_human(cooldown_sec)}."
            ),
        }
    hint = f" ({free_rem} attempt{'s' if free_rem != 1 else ''} remaining before temporary cooldown)." if free_rem > 0 else "."
    default_msg = f"Verification Failed: The Security Code does not match this instruction letter{hint}"
    return {
        "verified": False,
        "tamper_alert": tamper_alert,
        "cooldown_seconds": 0,
        "failed_attempts": fails,
        "reason": f"{custom_reason}{hint}" if custom_reason else default_msg,
    }


def _build_tamper_lg_mismatch_response(client_ip: str, token: str, entered_lg_tail: str) -> Dict[str, Any]:
    """
    Triggered when the 4-digit footer code matches the QR token, but the 2 LG characters entered
    from the letter body do NOT match the LG attached to that QR token (indicating a copied footer).
    """
    tail_disp = (entered_lg_tail or "").strip().upper() or "??"
    msg = (
        f"Security Warning — Guarantee Number Mismatch: The 4-digit footer code belongs to this QR seal, "
        f"but this QR seal was NOT issued for a Guarantee ending in '{tail_disp}'. "
        f"Please check that the bottom footer was not copied from a different instruction"
    )
    return _build_failed_code_response(client_ip, token, custom_reason=msg, tamper_alert=True)


def _trigger_honeypot_lockout(client_ip: str, token: str) -> Dict[str, Any]:
    """
    Silently trips the maximum 30-minute cooldown (1800s) for automated bots that fill the
    decoy honeypot field or submit faster than humanly possible (< 800ms).
    Returns a standard cooldown response so the bot does not know a honeypot was triggered.
    """
    ip = (client_ip or "unknown").strip()
    tok_key = f"ip_tok:{ip}:{_canonical_token_key(token)}"
    ip_key = f"ip_all:{ip}"
    now = datetime.now(timezone.utc).timestamp()
    max_cooldown = _COOLDOWN_SCHEDULE_SECONDS[-1]  # 1800s (30 minutes)

    with _COOLDOWN_LOCK:
        with _cooldown_file_lock():
            store = _load_cooldown_store()
            store[tok_key] = {
                "fails": _FREE_TYPO_ATTEMPTS + len(_COOLDOWN_SCHEDULE_SECONDS),
                "last_fail": now,
                "locked_until": now + max_cooldown,
            }
            store[ip_key] = {
                "fails": 12,
                "last_fail": now,
                "locked_until": now + max_cooldown,
            }
            _save_cooldown_store(store)

    return {
        "verified": False,
        "cooldown_seconds": max_cooldown,
        "_honeypot_tripped": True,
        "reason": (
            f"Too many failed attempts from your connection. Please wait "
            f"{_format_duration_human(max_cooldown)} ({max_cooldown}s) before trying again."
        ),
    }


def verify_instruction_with_code(
    db: Any,
    token: str,
    entered_code: str,
    client_ip: str = "unknown",
    honeypot_value: str = "",
    elapsed_ms: Optional[int] = None,
    lg_tail: str = "",
) -> Dict[str, Any]:
    """
    Verifies both the cryptographic QR token (bound to the secret database issuance timestamp)
    AND the physical Security Code.
    - For v1 tokens ('I.', 'M.', 'P.'): verifies the 6-digit footer code (100% backward compatible).
    - For v2 tokens ('I2.', 'M2.', 'P2.'): verifies the 4-digit footer code AND the last 2 characters
      of the LG Number from the letter body, triggering a Tamper Warning if the footer was copied onto a different LG.
    """
    token = (token or "").strip()
    rem_cooldown = _check_cooldown_status(client_ip, token)
    if rem_cooldown > 0:
        return {
            "verified": False,
            "cooldown_seconds": rem_cooldown,
            "reason": (
                f"Too many failed attempts from your connection. Please wait "
                f"{_format_duration_human(rem_cooldown)} ({rem_cooldown}s) before trying again."
            ),
        }

    # Check Honeypot field & Sub-second Bot Speed Trap (< 800ms from page render)
    if (honeypot_value and str(honeypot_value).strip()) or (
        elapsed_ms is not None and 0 <= int(elapsed_ms) < 800
    ):
        return _trigger_honeypot_lockout(client_ip, token)

    if not token or not token.isascii() or token.count(".") not in (2, 3):
        return {"verified": False, "reason": "Invalid verification token."}

    parts = token.split(".")
    if len(parts) == 4:
        kind, id_str, _legacy_ts_str, sig = parts
        sig_len = 16
    else:
        kind, id_str, sig = parts
        sig_len = 18

    is_v2 = kind.endswith("2")
    base_kind = kind.rstrip("2")

    if base_kind == "P":
        # Sample/preview QR codes are never "verified", whatever code is entered.
        return {
            "verified": False,
            "is_preview": True,
            "status_category": "SAMPLE",
            "status_label": _PREVIEW_STATUS_LABEL,
            "reason": _PREVIEW_REJECTION_REASON,
        }

    # Normalize inputs for v1 vs v2
    entered_lg_tail = (lg_tail or "").strip()
    clean_code = re.sub(r"[^0-9]", "", entered_code or "")

    if is_v2:
        # Allow combined 6-char entry (2 LG chars + 4 digits) if lg_tail wasn't passed separately
        if not entered_lg_tail and len(re.sub(r"[\s\-]", "", entered_code or "")) == 6:
            compact = re.sub(r"[\s\-]", "", entered_code or "")
            entered_lg_tail = compact[:2]
            clean_code = re.sub(r"[^0-9]", "", compact[2:])
        tail_len = len(re.sub(r"\s+", "", entered_lg_tail))
        if tail_len < 2:
            return {"verified": False, "reason": "Please enter the last 2 characters of the LG Number from the top of the letter."}
        if tail_len > 2:
            # Without this, a correct code + over-long tail would be reported as a false TAMPER alert.
            return {"verified": False, "reason": "Please enter only the last 2 characters of the LG Number."}
        if len(clean_code) != 4:
            return {"verified": False, "reason": "Please enter the 4-digit Security Code printed in the letter footer."}
    else:
        if len(clean_code) != 6:
            return {"verified": False, "reason": "Please enter the complete 6-digit numeric Security Code."}

    if base_kind not in ("I", "M") or not id_str.isdigit():
        return {"verified": False, "reason": "Invalid verification token."}

    record_id = int(id_str)

    if base_kind == "I":
        from app.models.models import LGInstruction, LGRecord
        from sqlalchemy.orm import selectinload

        inst = (
            db.query(LGInstruction)
            .options(
                selectinload(LGInstruction.lg_record).selectinload(LGRecord.lg_currency),
                selectinload(LGInstruction.lg_record).selectinload(LGRecord.issuing_bank),
                selectinload(LGInstruction.lg_record).selectinload(LGRecord.beneficiary_corporate),
                selectinload(LGInstruction.lg_record).selectinload(LGRecord.customer),
            )
            .filter(LGInstruction.id == record_id, LGInstruction.is_deleted == False)
            .first()
        )
        if not inst or not inst.lg_record or inst.lg_record.is_deleted:
            return {"verified": False, "reason": "Instruction record not found."}

        lg = inst.lg_record
        db_ts = _to_epoch_seconds(getattr(inst, "created_at", None) or getattr(inst, "instruction_date", None))
        expected_sig = _compute_hmac_sig(
            f"QR_TOKEN:{kind}.{record_id}.{db_ts}:{inst.serial_number}"
            if sig_len == 16
            else f"QR_TOKEN:{kind}.{record_id}:{db_ts}:{inst.serial_number}",
            length=sig_len,
        )
        if not hmac.compare_digest(sig, expected_sig):
            return {"verified": False, "reason": "Cryptographic signature mismatch."}

        if is_v2:
            _, expected_code = generate_verification_token_and_code(
                "I", inst.id, inst.serial_number or "", lg.lg_number or "", db_ts
            )
            if not hmac.compare_digest(clean_code, expected_code):
                return _build_failed_code_response(client_ip, token)
            if not _matches_lg_last2(lg.lg_number or "", entered_lg_tail):
                return _build_tamper_lg_mismatch_response(client_ip, token, entered_lg_tail)
        else:
            _, expected_code = _generate_v1_token_and_6digit_code(
                "I", inst.id, inst.serial_number or "", lg.lg_number or "", db_ts
            )
            if not hmac.compare_digest(clean_code, expected_code.replace("-", "")):
                return _build_failed_code_response(client_ip, token)

        _reset_cooldown_on_success(client_ip, token)

        details = inst.details if isinstance(inst.details, dict) else {}
        raw_status = (inst.status or "Instruction Issued").strip()
        is_canceled = "cancel" in raw_status.lower()
        status_category = "CANCELED" if is_canceled else "VALID"

        action_map = {
            "LG_EXTENSION": "LG Extension Instruction",
            "EXTENSION": "LG Extension Instruction",
            "LG_DECREASE_AMOUNT": "LG Amount Reduction Instruction",
            "DECREASE_AMOUNT": "LG Amount Reduction Instruction",
            "LG_RELEASE": "LG Release & Discharge Instruction",
            "RELEASE": "LG Release & Discharge Instruction",
            "LG_LIQUIDATE": "LG Liquidation Demand",
            "LIQUIDATION": "LG Liquidation Demand",
            "LG_ACTIVATE_NON_OPERATIVE": "LG Activation Instruction",
            "LG_REMINDER_TO_BANKS": "Follow-Up Bank Reminder",
        }
        action_label = action_map.get(inst.instruction_type, (inst.instruction_type or "Instruction").replace("_", " ").title())

        currency_code = lg.lg_currency.iso_code if lg.lg_currency else "EGP"
        amount_str = f"{currency_code} {float(lg.lg_amount):,.2f}" if lg.lg_amount is not None else "N/A"

        param_label = "Authorized Instruction"
        param_value = action_label
        if inst.instruction_type in ("LG_EXTENSION", "EXTENSION"):
            param_label = "Extended To (New Expiry Date)"
            param_value = str(details.get("new_expiry_date") or (lg.expiry_date.strftime("%d-%b-%Y") if lg.expiry_date else "N/A"))
            # Format YYYY-MM-DD to DD-MMM-YYYY if needed
            if re.match(r"^\d{4}-\d{2}-\d{2}$", param_value):
                try:
                    param_value = datetime.strptime(param_value, "%Y-%m-%d").strftime("%d-%b-%Y")
                except Exception:
                    pass
        elif inst.instruction_type in ("LG_DECREASE_AMOUNT", "DECREASE_AMOUNT"):
            param_label = "Amended LG Amount After Reduction"
            param_value = str(details.get("new_lg_amount_formatted") or amount_str)
        elif inst.instruction_type in ("LG_LIQUIDATE", "LIQUIDATION"):
            liq_type = details.get("liquidation_type", "Full")
            liq_amt = details.get("liquidation_amount_formatted") or amount_str
            param_label = f"Liquidation Demand ({liq_type})"
            param_value = str(liq_amt)
        elif inst.instruction_type in ("LG_RELEASE", "RELEASE"):
            param_label = "Release Scope"
            param_value = f"Full Release & Discharge ({amount_str})"
        elif inst.instruction_type == "LG_ACTIVATE_NON_OPERATIVE":
            param_label = "Advance Payment Executed"
            param_value = str(details.get("payment_amount_formatted") or amount_str)

        issue_dt = getattr(inst, "instruction_date", None) or getattr(inst, "created_at", None)
        issue_date_str = issue_dt.strftime("%d-%b-%Y") if isinstance(issue_dt, datetime) else "N/A"

        cust_name = lg.customer.name if lg.customer else ""
        entity_name = lg.beneficiary_corporate.entity_name if lg.beneficiary_corporate else cust_name
        bank_name = lg.issuing_bank.name if lg.issuing_bank else (lg.foreign_bank_name or "Issuing Bank")

        delivery_info = "Issued — Pending Bank Delivery"
        if is_canceled:
            delivery_info = "CANCELED / REVOKED — Do Not Execute"
        elif inst.bank_reply_date:
            delivery_info = f"Bank Reply Recorded ({inst.bank_reply_date.strftime('%d-%b-%Y')})"
        elif inst.delivery_date:
            delivery_info = f"Delivered to Bank ({inst.delivery_date.strftime('%d-%b-%Y')})"

        from app.core.trial_guard import is_trial_customer
        return {
            "verified": True,
            "is_trial_document": is_trial_customer(db, getattr(lg, "customer_id", None)),
            "status_category": status_category,
            "status_label": raw_status,
            "instruction_serial": inst.serial_number,
            "action_type": action_label,
            "issue_date": issue_date_str,
            "customer_masked": _mask_text(cust_name),
            "beneficiary_masked": _mask_text(entity_name),
            "bank_name": bank_name,
            "lg_number_masked": _mask_lg_number(lg.lg_number or ""),
            "lg_amount_formatted": amount_str,
            "authorized_parameter_label": param_label,
            "authorized_parameter_value": param_value,
            "delivery_status": delivery_info,
        }

    # kind == "M" (IssuanceMaintenanceAction)
    from app.models.models_issuance import IssuanceMaintenanceAction, IssuedLGRecord
    from sqlalchemy.orm import selectinload

    maint = (
        db.query(IssuanceMaintenanceAction)
        .options(
            selectinload(IssuanceMaintenanceAction.issued_lg).selectinload(IssuedLGRecord.currency),
            selectinload(IssuanceMaintenanceAction.issued_lg).selectinload(IssuedLGRecord.bank),
            selectinload(IssuanceMaintenanceAction.issued_lg).selectinload(IssuedLGRecord.customer),
        )
        .filter(
            IssuanceMaintenanceAction.id == record_id,
            IssuanceMaintenanceAction.is_deleted == False,
        )
        .first()
    )
    if not maint or not maint.issued_lg or getattr(maint.issued_lg, "is_deleted", False):
        return {"verified": False, "reason": "Maintenance instruction record not found."}

    ilg = maint.issued_lg
    lg_num = ilg.bank_lg_number or ilg.lg_ref_number or ""
    db_ts = _to_epoch_seconds(getattr(maint, "created_at", None))
    expected_sig = _compute_hmac_sig(
        f"QR_TOKEN:{kind}.{record_id}.{db_ts}:{maint.letter_serial_number}"
        if sig_len == 16
        else f"QR_TOKEN:{kind}.{record_id}:{db_ts}:{maint.letter_serial_number}",
        length=sig_len,
    )
    if not hmac.compare_digest(sig, expected_sig):
        return {"verified": False, "reason": "Cryptographic signature mismatch."}

    if is_v2:
        _, expected_code = generate_verification_token_and_code(
            "M", maint.id, maint.letter_serial_number or "", lg_num, db_ts
        )
        if not hmac.compare_digest(clean_code, expected_code):
            return _build_failed_code_response(client_ip, token)
        if not _matches_lg_last2(lg_num, entered_lg_tail):
            return _build_tamper_lg_mismatch_response(client_ip, token, entered_lg_tail)
    else:
        _, expected_code = _generate_v1_token_and_6digit_code(
            "M", maint.id, maint.letter_serial_number or "", lg_num, db_ts
        )
        if not hmac.compare_digest(clean_code, expected_code.replace("-", "")):
            return _build_failed_code_response(client_ip, token)

    _reset_cooldown_on_success(client_ip, token)

    curr_code = ilg.currency.iso_code if ilg.currency else "EGP"
    amt_str = f"{curr_code} {float(ilg.current_amount):,.2f}" if ilg.current_amount is not None else "N/A"
    issue_dt = getattr(maint, "created_at", None)
    issue_date_str = issue_dt.strftime("%d-%b-%Y") if isinstance(issue_dt, datetime) else "N/A"

    from app.core.trial_guard import is_trial_customer
    is_canceled_maint = (maint.status or "").upper() in ("CANCELLED", "CANCELED", "REJECTED")
    return {
        "verified": True,
        "is_trial_document": is_trial_customer(db, getattr(ilg, "customer_id", None)),
        "status_category": "CANCELED" if is_canceled_maint else "VALID",
        "status_label": "CANCELED / REVOKED — Do Not Execute" if is_canceled_maint else (maint.status or "Executed"),
        "instruction_serial": maint.letter_serial_number,
        "action_type": f"LG {(maint.action_type or '').replace('_', ' ').title()}",
        "issue_date": issue_date_str,
        "customer_masked": _mask_text(ilg.customer.name if ilg.customer else ""),
        "beneficiary_masked": _mask_text(ilg.beneficiary_name or ""),
        "bank_name": ilg.bank.name if ilg.bank else "Issuing Bank",
        "lg_number_masked": _mask_lg_number(lg_num),
        "lg_amount_formatted": amt_str,
        "authorized_parameter_label": "Maintenance Action",
        "authorized_parameter_value": (maint.action_type or "").replace("_", " ").title(),
        "delivery_status": maint.instruction_status or "Instruction Issued",
    }

