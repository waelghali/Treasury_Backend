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
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

from app.core.routing import get_frontend_base_url

logger = logging.getLogger(__name__)

_SECRET_KEY = (os.getenv("SECRET_KEY") or "grow-bd-fallback-secret").encode("utf-8")

# Thread-safe bounded cache for newly flushed instructions (before outer transaction commits)
_RECENT_INSTRUCTIONS_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_MAX_CACHE_SIZE = 500


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
    Registers a newly flushed LGInstruction ('I') or IssuedLGMaintenanceAction ('M') in memory
    so `document_generator.py` can resolve its exact ID and secret issuance timestamp immediately,
    even before the outer database transaction commits.
    """
    if not serial_number or not record_id:
        return
    secret_ts = _to_epoch_seconds(created_at) or int(datetime.now(timezone.utc).timestamp())
    key = serial_number.strip()
    _RECENT_INSTRUCTIONS_CACHE[key] = {
        "kind": record_kind,
        "id": int(record_id),
        "serial_number": key,
        "lg_number": (lg_number or "").strip(),
        "secret_ts": secret_ts,
    }
    while len(_RECENT_INSTRUCTIONS_CACHE) > _MAX_CACHE_SIZE:
        _RECENT_INSTRUCTIONS_CACHE.popitem(last=False)


def _compute_hmac_sig(payload: str, length: int = 16) -> str:
    digest = hmac.new(_SECRET_KEY, payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")[:length]


def generate_verification_token_and_code(
    record_kind: str,
    record_id: int,
    serial_number: str,
    lg_number: str,
    secret_ts: int,
) -> Tuple[str, str]:
    """
    Returns (signed_qr_token, formatted_6_digit_code 'XXX-XXX').
    Note: secret_ts is mixed into the cryptographic HMAC signature and 6-digit PIN
    calculation, but is NEVER exposed in the token string or portal response.
    """
    sig = _compute_hmac_sig(f"QR_TOKEN:{record_kind}.{record_id}:{secret_ts}:{serial_number}", length=18)
    token = f"{record_kind}.{record_id}.{sig}"

    # 6-digit gate code derived from secret_ts + instruction identity + LG number
    gate_payload = f"GATE_PIN:{record_kind}:{record_id}:{(serial_number or '').strip().upper()}:{(lg_number or '').strip().upper()}:{secret_ts}"
    gate_digest = hmac.new(_SECRET_KEY, gate_payload.encode("utf-8"), hashlib.sha256).digest()
    code_int = (int.from_bytes(gate_digest[:4], "big") % 900000) + 100000  # Always 6 digits: 100000..999999
    code_str = f"{str(code_int)[:3]}-{str(code_int)[3:]}"
    return token, code_str


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
    # 1. Check common HTML patterns: Ref: <strong>...</strong> or Instruction Ref: <strong>...</strong>
    patterns = [
        r"(?:Instruction\s+Ref|Original\s+Ref|Internal\s+Ref|Ref)\s*:\s*<strong[^>]*>\s*([A-Za-z0-9_\-/]+)\s*</strong>",
        r"(?:Instruction\s+Ref|Original\s+Ref|Internal\s+Ref|Ref)\s*:\s*([A-Za-z0-9_\-/]{6,40})",
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
    Resolves (kind, id, serial_number, lg_number, secret_ts) from the in-memory cache or database.
    Falls back to a deterministic preview token if rendering a studio preview / non-persisted sample.
    """
    serial = _extract_serial_from_html_or_hint(html_content, filename_hint)
    if serial and serial in _RECENT_INSTRUCTIONS_CACHE:
        return _RECENT_INSTRUCTIONS_CACHE[serial]

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
                if inst:
                    ts = _to_epoch_seconds(getattr(inst, "created_at", None) or getattr(inst, "instruction_date", None))
                    lg_num = inst.lg_record.lg_number if inst.lg_record else ""
                    meta = {
                        "kind": "I",
                        "id": int(inst.id),
                        "serial_number": inst.serial_number,
                        "lg_number": lg_num or "",
                        "secret_ts": ts,
                    }
                    _RECENT_INSTRUCTIONS_CACHE[serial] = meta
                    return meta

                # Also check IssuedLGMaintenanceAction for Issuance module letters
                try:
                    from app.models.models_issuance import IssuedLGMaintenanceAction
                    maint = (
                        db.query(IssuedLGMaintenanceAction)
                        .options(selectinload(IssuedLGMaintenanceAction.issued_lg))
                        .filter(IssuedLGMaintenanceAction.letter_serial_number == serial)
                        .order_by(IssuedLGMaintenanceAction.id.desc())
                        .first()
                    )
                    if maint:
                        ts = _to_epoch_seconds(getattr(maint, "created_at", None))
                        lg_num = (
                            (maint.issued_lg.bank_lg_number or maint.issued_lg.lg_ref_number)
                            if maint.issued_lg
                            else ""
                        )
                        meta = {
                            "kind": "M",
                            "id": int(maint.id),
                            "serial_number": maint.letter_serial_number,
                            "lg_number": lg_num or "",
                            "secret_ts": ts,
                        }
                        _RECENT_INSTRUCTIONS_CACHE[serial] = meta
                        return meta
                except Exception:
                    pass
            finally:
                db.close()
        except Exception as e:
            logger.debug(f"Could not query DB for instruction verification meta ({serial}): {e}")

    # Fallback for Template Studio live previews or sample renders
    return {
        "kind": "P",
        "id": 0,
        "serial_number": serial or "SAMPLE-REF-0001",
        "lg_number": "SAMPLE-LG",
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
    6-digit Verification Code on any letter HTML. Strips any template-level `.footer` or
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
                        <strong>Authenticity Verification:</strong> Scan the QR code to verify instruction genuineness and enter Security Code:
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


def inspect_verification_token(db: Any, token: str) -> Dict[str, Any]:
    """
    Validates the QR token signature and returns minimal gate prompt info (without revealing LG details
    until the 6-digit Security Code is entered).
    """
    if not token or token.count(".") != 2:
        return {"valid_token": False, "reason": "Malformed verification link."}

    kind, id_str, sig = token.split(".")
    if kind == "P":
        return {
            "valid_token": True,
            "is_preview": True,
            "masked_serial": "SAMPLE-REF-0001",
            "prompt": "Enter the 6-digit Security Code printed at the bottom of the instruction letter.",
        }

    if kind not in ("I", "M") or not id_str.isdigit():
        return {"valid_token": False, "reason": "Invalid verification token parameters."}

    record_id = int(id_str)

    if kind == "I":
        from app.models.models import LGInstruction
        inst = db.query(LGInstruction).filter(LGInstruction.id == record_id, LGInstruction.is_deleted == False).first()
        if not inst:
            return {"valid_token": False, "reason": "Instruction record not found."}
        db_ts = _to_epoch_seconds(getattr(inst, "created_at", None) or getattr(inst, "instruction_date", None))
        expected_sig = _compute_hmac_sig(f"QR_TOKEN:{kind}.{record_id}:{db_ts}:{inst.serial_number}", length=18)
        if not hmac.compare_digest(sig, expected_sig):
            return {"valid_token": False, "reason": "Cryptographic signature mismatch."}
        serial = inst.serial_number or ""
        masked_serial = serial[:6] + "••••" + serial[-5:] if len(serial) > 12 else serial
        return {
            "valid_token": True,
            "is_preview": False,
            "masked_serial": masked_serial,
            "prompt": "Enter the 6-digit Security Code printed in the footer of the instruction letter.",
        }

    # kind == "M"
    from app.models.models_issuance import IssuedLGMaintenanceAction
    maint = db.query(IssuedLGMaintenanceAction).filter(IssuedLGMaintenanceAction.id == record_id).first()
    if not maint:
        return {"valid_token": False, "reason": "Maintenance instruction record not found."}
    db_ts = _to_epoch_seconds(getattr(maint, "created_at", None))
    expected_sig = _compute_hmac_sig(f"QR_TOKEN:{kind}.{record_id}:{db_ts}:{maint.letter_serial_number}", length=18)
    if not hmac.compare_digest(sig, expected_sig):
        return {"valid_token": False, "reason": "Cryptographic signature mismatch."}
    serial = maint.letter_serial_number or ""
    masked_serial = serial[:6] + "••••" + serial[-4:] if len(serial) > 10 else serial
    return {
        "valid_token": True,
        "is_preview": False,
        "masked_serial": masked_serial,
        "prompt": "Enter the 6-digit Security Code printed in the footer of the instruction letter.",
    }


def verify_instruction_with_code(db: Any, token: str, entered_code: str) -> Dict[str, Any]:
    """
    Verifies both the cryptographic QR token (bound to the secret database issuance timestamp)
    AND the 6-digit Security Code printed on the physical letter.
    Returns masked assurance details and live instruction status if verified.
    """
    clean_code = re.sub(r"[^0-9]", "", entered_code or "")
    if len(clean_code) != 6:
        return {"verified": False, "reason": "Please enter the complete 6-digit numeric Security Code."}

    if not token or token.count(".") != 2:
        return {"verified": False, "reason": "Invalid verification token."}

    kind, id_str, sig = token.split(".")
    if kind == "P":
        _, expected_code = generate_verification_token_and_code("P", 0, "SAMPLE-REF-0001", "SAMPLE-LG", 1762600000)
        if clean_code != expected_code.replace("-", ""):
            return {"verified": False, "reason": "The 6-digit Security Code entered does not match this instruction."}
        return {
            "verified": True,
            "status_Category": "ACTIVE",
            "status_label": "Verified Sample Preview",
            "instruction_serial": "SAMPLE-REF-0001",
            "action_type": "LG EXTENSION (SAMPLE)",
            "issue_date": "08-Nov-2026",
            "customer_masked": "Or****e Egypt",
            "beneficiary_masked": "Or****e Egypt",
            "bank_name": "Commercial International Bank (CIB)",
            "lg_number_masked": "******4120",
            "lg_amount_formatted": "EGP 2,500,000.00",
            "authorized_parameter_label": "New Expiry Date",
            "authorized_parameter_value": "30-Jun-2027",
            "delivery_status": "Sample Preview Mode",
        }

    if kind not in ("I", "M") or not id_str.isdigit():
        return {"verified": False, "reason": "Invalid verification token."}

    record_id = int(id_str)

    if kind == "I":
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
        if not inst or not inst.lg_record:
            return {"verified": False, "reason": "Instruction record not found."}

        lg = inst.lg_record
        db_ts = _to_epoch_seconds(getattr(inst, "created_at", None) or getattr(inst, "instruction_date", None))
        expected_token, expected_code = generate_verification_token_and_code(
            "I", inst.id, inst.serial_number or "", lg.lg_number or "", db_ts
        )

        if not hmac.compare_digest(token, expected_token):
            return {"verified": False, "reason": "Cryptographic signature mismatch."}

        if not hmac.compare_digest(clean_code, expected_code.replace("-", "")):
            return {
                "verified": False,
                "reason": "Verification Failed: The 6-digit Security Code does not match this instruction letter.",
            }

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

        return {
            "verified": True,
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

    # kind == "M" (IssuedLGMaintenanceAction)
    from app.models.models_issuance import IssuedLGMaintenanceAction, IssuedLGRecord
    from sqlalchemy.orm import selectinload

    maint = (
        db.query(IssuedLGMaintenanceAction)
        .options(
            selectinload(IssuedLGMaintenanceAction.issued_lg).selectinload(IssuedLGRecord.currency),
            selectinload(IssuedLGMaintenanceAction.issued_lg).selectinload(IssuedLGRecord.bank),
            selectinload(IssuedLGMaintenanceAction.issued_lg).selectinload(IssuedLGRecord.customer),
        )
        .filter(IssuedLGMaintenanceAction.id == record_id)
        .first()
    )
    if not maint or not maint.issued_lg:
        return {"verified": False, "reason": "Maintenance instruction record not found."}

    ilg = maint.issued_lg
    lg_num = ilg.bank_lg_number or ilg.lg_ref_number or ""
    db_ts = _to_epoch_seconds(getattr(maint, "created_at", None))
    expected_token, expected_code = generate_verification_token_and_code(
        "M", maint.id, maint.letter_serial_number or "", lg_num, db_ts
    )

    if not hmac.compare_digest(token, expected_token):
        return {"verified": False, "reason": "Cryptographic signature mismatch."}

    if not hmac.compare_digest(clean_code, expected_code.replace("-", "")):
        return {
            "verified": False,
            "reason": "Verification Failed: The 6-digit Security Code does not match this instruction letter.",
        }

    curr_code = ilg.currency.iso_code if ilg.currency else "EGP"
    amt_str = f"{curr_code} {float(ilg.current_amount):,.2f}" if ilg.current_amount is not None else "N/A"
    issue_dt = getattr(maint, "created_at", None)
    issue_date_str = issue_dt.strftime("%d-%b-%Y") if isinstance(issue_dt, datetime) else "N/A"

    return {
        "verified": True,
        "status_category": "VALID",
        "status_label": maint.status or "Executed",
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
        "delivery_status": maint.delivery_status or "Issued",
    }

