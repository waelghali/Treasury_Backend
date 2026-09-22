"""
Unified Email Builder Service
Provides high-aesthetic, corporate SaaS HTML templates for all outgoing platform emails.
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime


def build_standard_email_html(
    customer_name: str,
    title: str,
    content_html: str,
    recipient_name: Optional[str] = None,
    cta_text: Optional[str] = None,
    cta_url: Optional[str] = None,
    platform_name: str = "Grow Treasury Platform"
) -> str:
    """
    Generates a standard corporate SaaS HTML email with dark navy branding.
    """
    greeting = f"Dear <strong>{recipient_name}</strong>," if recipient_name else "Hello,"
    cta_html = ""
    if cta_text and cta_url:
        cta_html = f"""
        <div style="text-align: center; margin: 32px 0 16px 0;">
            <a href="{cta_url}" style="background-color: #1e40af; color: #ffffff; padding: 14px 32px; text-decoration: none; border-radius: 8px; font-weight: 700; font-size: 14px; display: inline-block; box-shadow: 0 4px 6px -1px rgba(30, 64, 175, 0.2);">
                {cta_text} &rarr;
            </a>
        </div>
        """

    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M EEST")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f1f5f9; font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #1e293b; -webkit-font-smoothing: antialiased;">
    <div style="max-width: 650px; margin: 30px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05), 0 4px 6px -2px rgba(0, 0, 0, 0.025); border: 1px solid #e2e8f0;">
        
        <!-- HEADER -->
        <div style="background-color: #0f172a; padding: 26px 32px; color: #ffffff;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td>
                        <span style="font-size: 11px; font-weight: 800; letter-spacing: 1.5px; text-transform: uppercase; color: #38bdf8; display: block; margin-bottom: 4px;">{platform_name.upper()}</span>
                        <h1 style="margin: 0; font-size: 19px; font-weight: 700; color: #ffffff;">{title}</h1>
                    </td>
                    <td style="text-align: right; vertical-align: middle;">
                        <span style="background-color: rgba(56, 189, 248, 0.15); color: #38bdf8; padding: 6px 12px; border-radius: 20px; font-size: 11px; font-weight: 700; border: 1px solid rgba(56, 189, 248, 0.3);">
                            {customer_name}
                        </span>
                    </td>
                </tr>
            </table>
        </div>

        <!-- BODY CONTENT -->
        <div style="padding: 32px;">
            <p style="margin-top: 0; margin-bottom: 20px; font-size: 15px; line-height: 1.6; color: #334155;">
                {greeting}
            </p>
            
            <div style="font-size: 14px; line-height: 1.6; color: #334155;">
                {content_html}
            </div>

            {cta_html}
        </div>

        <!-- FOOTER -->
        <div style="background-color: #f8fafc; border-top: 1px solid #e2e8f0; padding: 20px 32px; text-align: center;">
            <p style="margin: 0 0 4px 0; font-size: 12px; color: #64748b; font-weight: 500;">
                {platform_name} &bull; Automated System Notification
            </p>
            <p style="margin: 0; font-size: 11px; color: #94a3b8;">
                Generated on {current_time_str} for {customer_name}. Confidential.
            </p>
        </div>

    </div>
</body>
</html>
"""


def build_alert_email_html(
    customer_name: str,
    title: str,
    alert_type: str,  # "critical", "warning", or "info"
    message: str,
    details_table_html: Optional[str] = None,
    cta_text: Optional[str] = None,
    cta_url: Optional[str] = None,
    recipient_name: Optional[str] = None,
    platform_name: str = "Grow Treasury Platform"
) -> str:
    """
    Generates a color-coded alert email for warnings, SLA breaches, and threshold notices.
    """
    alert_type_lower = (alert_type or "info").lower()
    
    if alert_type_lower in ["critical", "red", "error", "breach"]:
        badge_text = "🔴 CRITICAL ALERT"
        banner_bg = "#fef2f2"
        banner_border = "#fecaca"
        banner_color = "#991b1b"
        title_color = "#dc2626"
    elif alert_type_lower in ["warning", "amber", "yellow"]:
        badge_text = "🟡 WARNING ALERT"
        banner_bg = "#fffbeb"
        banner_border = "#fef3c7"
        banner_color = "#92400e"
        title_color = "#d97706"
    else:
        badge_text = "🔵 INFORMATION"
        banner_bg = "#f0f9ff"
        banner_border = "#bae6fd"
        banner_color = "#075985"
        title_color = "#0284c7"

    table_block = ""
    if details_table_html:
        table_block = f"""
        <div style="overflow-x: auto; border: 1px solid #e2e8f0; border-radius: 8px; margin: 20px 0;">
            {details_table_html}
        </div>
        """

    content_html = f"""
    <div style="background-color: {banner_bg}; border: 1px solid {banner_border}; border-radius: 8px; padding: 18px 20px; margin-bottom: 24px;">
        <span style="font-size: 11px; font-weight: 800; color: {banner_color}; letter-spacing: 1px; text-transform: uppercase; display: block; margin-bottom: 6px;">
            {badge_text}
        </span>
        <h3 style="margin: 0 0 8px 0; font-size: 16px; font-weight: 700; color: {title_color};">
            {title}
        </h3>
        <p style="margin: 0; font-size: 14px; line-height: 1.5; color: #334155;">
            {message}
        </p>
    </div>

    {table_block}
    """

    return build_standard_email_html(
        customer_name=customer_name,
        title=title,
        content_html=content_html,
        recipient_name=recipient_name,
        cta_text=cta_text,
        cta_url=cta_url,
        platform_name=platform_name
    )


def build_transaction_email_html(
    customer_name: str,
    title: str,
    transaction_ref: str,
    transaction_type: str,
    key_value_dict: Dict[str, Any],
    summary_text: Optional[str] = None,
    cta_text: Optional[str] = None,
    cta_url: Optional[str] = None,
    recipient_name: Optional[str] = None,
    platform_name: str = "Grow Treasury Platform"
) -> str:
    """
    Generates a structured transaction/approval/quotation detail email.
    """
    rows = []
    for idx, (key, value) in enumerate(key_value_dict.items()):
        bg_color = "#ffffff" if idx % 2 == 0 else "#f8fafc"
        rows.append(f"""
        <tr style="background-color: {bg_color}; border-bottom: 1px solid #e2e8f0;">
            <td style="padding: 10px 14px; font-size: 13px; font-weight: 600; color: #475569; width: 35%;">{key}</td>
            <td style="padding: 10px 14px; font-size: 13px; font-weight: 600; color: #0f172a;">{value}</td>
        </tr>
        """)

    rows_html = "".join(rows)

    summary_block = f"""<p style="margin-top: 0; margin-bottom: 18px; font-size: 14px; line-height: 1.6; color: #334155;">{summary_text}</p>""" if summary_text else ""

    content_html = f"""
    {summary_block}
    
    <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 14px 18px; margin-bottom: 20px;">
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="font-size: 12px; font-weight: 700; color: #64748b; text-transform: uppercase;">
                    {transaction_type}
                </td>
                <td style="text-align: right;">
                    <span style="background-color: #e0f2fe; color: #0369a1; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: 700;">
                        Ref: {transaction_ref}
                    </span>
                </td>
            </tr>
        </table>
    </div>

    <div style="overflow-x: auto; border: 1px solid #e2e8f0; border-radius: 8px; margin-bottom: 24px;">
        <table style="width: 100%; border-collapse: collapse; text-align: left;">
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """

    return build_standard_email_html(
        customer_name=customer_name,
        title=title,
        content_html=content_html,
        recipient_name=recipient_name,
        cta_text=cta_text,
        cta_url=cta_url,
        platform_name=platform_name
    )


def build_security_email_html(
    title: str,
    user_email: str,
    message: str,
    otp_code: Optional[str] = None,
    action_url: Optional[str] = None,
    action_text: Optional[str] = None,
    platform_name: str = "Grow Treasury Security"
) -> str:
    """
    Generates a secure email for authentication, password resets, and account security notifications.
    """
    code_block = ""
    if otp_code:
        code_block = f"""
        <div style="text-align: center; margin: 28px 0;">
            <span style="font-size: 11px; font-weight: 700; color: #64748b; text-transform: uppercase; letter-spacing: 1px; display: block; margin-bottom: 8px;">Your Verification Code</span>
            <div style="font-size: 30px; font-weight: 800; letter-spacing: 6px; color: #0284c7; background-color: #f8fafc; border: 2px dashed #0284c7; padding: 16px 28px; border-radius: 10px; display: inline-block;">
                {otp_code}
            </div>
        </div>
        """

    cta_block = ""
    if action_url and action_text:
        cta_block = f"""
        <div style="text-align: center; margin: 28px 0;">
            <a href="{action_url}" style="background-color: #0284c7; color: #ffffff; padding: 14px 32px; text-decoration: none; border-radius: 8px; font-weight: 700; font-size: 14px; display: inline-block; box-shadow: 0 4px 6px -1px rgba(2, 132, 199, 0.2);">
                {action_text} &rarr;
            </a>
        </div>
        """

    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M EEST")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f1f5f9; font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #1e293b; -webkit-font-smoothing: antialiased;">
    <div style="max-width: 580px; margin: 30px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05), 0 4px 6px -2px rgba(0, 0, 0, 0.025); border: 1px solid #e2e8f0;">
        
        <!-- HEADER -->
        <div style="background-color: #0f172a; padding: 24px 30px; color: #ffffff; text-align: center;">
            <span style="font-size: 11px; font-weight: 800; letter-spacing: 1.5px; text-transform: uppercase; color: #38bdf8; display: block; margin-bottom: 4px;">{platform_name.upper()}</span>
            <h1 style="margin: 0; font-size: 20px; font-weight: 700; color: #ffffff;">🛡️ {title}</h1>
        </div>

        <!-- CONTENT -->
        <div style="padding: 30px;">
            <p style="margin-top: 0; margin-bottom: 16px; font-size: 14px; color: #334155; line-height: 1.6;">
                Hello <strong>{user_email}</strong>,
            </p>
            <p style="font-size: 14px; color: #334155; line-height: 1.6; margin-bottom: 20px;">
                {message}
            </p>

            {code_block}
            {cta_block}

            <div style="background-color: #f8fafc; border-left: 4px solid #0284c7; padding: 12px 16px; border-radius: 4px; margin-top: 24px;">
                <p style="margin: 0; font-size: 12px; color: #475569; line-height: 1.5;">
                    🔒 <strong>Security Advisory:</strong> If you did not initiate this request, please disregard this email or contact your Treasury Administrator immediately.
                </p>
            </div>
        </div>

        <!-- FOOTER -->
        <div style="background-color: #f8fafc; border-top: 1px solid #e2e8f0; padding: 18px 30px; text-align: center;">
            <p style="margin: 0; font-size: 11px; color: #94a3b8;">
                {platform_name} &bull; Generated on {current_time_str}
            </p>
        </div>

    </div>
</body>
</html>
"""


def build_quotation_rfq_bank_email(
    rfq: Any,
    assignment: Any = None,
    bank_name: str = "Bank Partner",
    customer_branding: str = "Corporate Treasury",
    link: str = "#",
    email_purpose: str = "INVITATION",
    platform_name: str = "Grow Treasury Platform"
) -> Tuple[str, str]:
    """
    Generates a standardized, high-aesthetic HTML quotation email for bank desks.
    Fully details:
    - Exact date and time of submission deadline (Cairo local time & UTC)
    - Formatted amount with thousands separator (e.g. USD 17,092,604.00)
    - Currency pair (e.g. USD / EGP) and client trade flow (Buy / Sell)
    - Exact target value date and whether alternative value date is permitted
    - Quotation base (Indicative vs Firm Execution)
    - Direct secure CTA button and direct clickable fallback link
    """
    # 0. Format Requesting Legal Entity
    rfq_entity = getattr(rfq, "entity", None)
    entity_name = (rfq_entity.entity_name if rfq_entity and getattr(rfq_entity, "entity_name", None) else None) or customer_branding
    entity_cr = getattr(rfq_entity, "cr_number", None) if rfq_entity else None
    entity_tax = getattr(rfq_entity, "tax_id", None) if rfq_entity else None

    entity_meta_parts = []
    if entity_cr:
        entity_meta_parts.append(f"CR: {entity_cr}")
    if entity_tax:
        entity_meta_parts.append(f"Tax ID: {entity_tax}")
    entity_meta_html = f'<div style="font-size: 11px; color: #64748b; font-family: monospace; font-weight: normal; margin-top: 3px;">{" &bull; ".join(entity_meta_parts)}</div>' if entity_meta_parts else ""

    # 1. Format Submission Deadline
    window_end = getattr(rfq, "window_end", None)
    deadline_cairo_str = "N/A"
    deadline_utc_str = ""
    if window_end:
        try:
            if isinstance(window_end, str):
                window_end = parser.parse(window_end)
            if window_end.tzinfo is None:
                window_end = window_end.replace(tzinfo=timezone.utc)
            cairo_tz = ZoneInfo("Africa/Cairo")
            cairo_dt = window_end.astimezone(cairo_tz)
            deadline_cairo_str = cairo_dt.strftime("%A, %d %b %Y at %H:%M:%S %Z")
            deadline_utc_str = window_end.astimezone(timezone.utc).strftime("%H:%M:%S UTC")
        except Exception:
            deadline_cairo_str = str(window_end)

    # 2. Format Target Value Date
    raw_val_date = (assignment.value_date if assignment and getattr(assignment, "value_date", None) else getattr(rfq, "value_date", None))
    value_date_display = "N/A"
    if raw_val_date:
        clean_vd = str(raw_val_date).strip().split("T")[0]
        try:
            d_obj = datetime.strptime(clean_vd, "%Y-%m-%d")
            value_date_display = f"{d_obj.strftime('%A, %d %b %Y')} ({clean_vd})"
        except Exception:
            value_date_display = clean_vd

    # 3. Alternative Value Date Permission
    is_alt_allowed = False
    if assignment and getattr(assignment, "allow_alternative_value_date", None) is not None:
        is_alt_allowed = bool(assignment.allow_alternative_value_date)
    else:
        is_alt_allowed = bool(getattr(rfq, "allow_alternative_value_date", False))

    if is_alt_allowed:
        alt_date_badge = """<span style="background-color: #ecfdf5; color: #047857; border: 1px solid #a7f3d0; padding: 4px 10px; border-radius: 6px; font-weight: 700; font-size: 12px; display: inline-block;">&#10004; Alternative Date Permitted</span>"""
        alt_date_desc = "Your desk is permitted to submit an alternative settlement date if necessary."
    else:
        alt_date_badge = """<span style="background-color: #f1f5f9; color: #475569; border: 1px solid #cbd5e1; padding: 4px 10px; border-radius: 6px; font-weight: 700; font-size: 12px; display: inline-block;">Fixed Date Only</span>"""
        alt_date_desc = "Quotations must settle strictly on the specified value date (Alternative dates not permitted)."

    # 4. Format Amount
    raw_amount = getattr(rfq, "amount", None)
    amount_str = "N/A"
    if raw_amount is not None:
        try:
            amount_str = f"{float(raw_amount):,.2f}"
        except Exception:
            amount_str = str(raw_amount)

    # 5. Currency Pair & Direction
    rfq_type = getattr(rfq, "type", "FX_SPOT")
    buy_curr = getattr(rfq, "buy_currency", None) or ""
    sell_curr = getattr(rfq, "sell_currency", None) or ""
    direction = getattr(rfq, "direction", None) or "Buy"

    if rfq_type == "FX_SPOT":
        pair_str = f"{buy_curr}/{sell_curr}" if buy_curr and sell_curr else (buy_curr or sell_curr or "FX")
        amount_display = f"<strong>{amount_str}</strong> {buy_curr}" if buy_curr else f"<strong>{amount_str}</strong>"
        if direction.upper() == "BUY":
            direction_display = f"Client <strong>BUYING {buy_curr}</strong> / <strong>SELLING {sell_curr}</strong>"
        elif direction.upper() == "SELL":
            direction_display = f"Client <strong>SELLING {sell_curr or buy_curr}</strong> / <strong>BUYING {buy_curr or sell_curr}</strong>"
        else:
            direction_display = direction
    else:
        # T-Bill
        pair_str = "TBILL"
        amount_display = f"<strong>{amount_str}</strong> {buy_curr or sell_curr or 'EGP'}"
        direction_display = f"Client <strong>{direction.upper()}</strong> Treasury Bills"

    # 6. Quotation Base
    q_base = (assignment.quotation_base if assignment and getattr(assignment, "quotation_base", None) else getattr(rfq, "quotation_base", None)) or "Indicative"
    if q_base.lower() == "indicative":
        base_badge = """<span style="background-color: #f0f9ff; color: #0284c7; border: 1px solid #bae6fd; padding: 4px 10px; border-radius: 6px; font-weight: 700; font-size: 12px;">Indicative Pricing</span>"""
        base_note = "Non-binding indicative quotation for price discovery and evaluation."
    else:
        base_badge = """<span style="background-color: #fef3c7; color: #b45309; border: 1px solid #fde68a; padding: 4px 10px; border-radius: 6px; font-weight: 700; font-size: 12px;">Firm Execution</span>"""
        base_note = "Binding execution quotation subject to prompt corporate allocation upon submission."

    # 7. Subject Line & Email Purpose
    ref_no = getattr(rfq, "ref_no", "RFQ")
    if email_purpose == "RE_TENDER":
        subject = f"ACTION REQUIRED: Re-Tender RFQ Request from {customer_branding} - {pair_str} - {ref_no}"
        banner_title = "Re-Tender Request for Quotation"
        salutation = f"Dear <strong>{bank_name} FX &amp; Treasury Desk</strong>,"
        intro_text = f"You have received a <strong>re-tendered</strong> Request for Quotation (RFQ) on behalf of <strong>{customer_branding}</strong>."
        instruction_text = "Please review the required trade specifications below and access the live portal to enter your quotation."
    elif email_purpose == "REMINDER":
        subject = f"REMINDER: RFQ Submission Pending - {customer_branding} - {pair_str} - {ref_no}"
        banner_title = "Quotation Submission Reminder"
        salutation = f"Dear <strong>{bank_name} FX &amp; Treasury Desk</strong>,"
        intro_text = f"This is a reminder that Request for Quotation (RFQ) <strong>{ref_no}</strong> for <strong>{customer_branding}</strong> is pending submission."
        instruction_text = "Please review the required trade specifications below and access the live portal to enter your quotation."
    elif email_purpose == "BANK_APPROVAL_REQUIRED":
        subject = f"APPROVAL REQUIRED: RFQ {ref_no} ({customer_branding}) - {pair_str}"
        banner_title = "Bank Approval Required &bull; RFQ Authorization"
        salutation = f"Dear <strong>{bank_name} Authorized Approver</strong>,"
        intro_text = f"Your bank has been invited to participate in a new <strong>Firm Execution</strong> Request for Quotation (RFQ) on behalf of <strong>{customer_branding}</strong>."
        instruction_text = "Please review the required trade specifications below and authorize your bank's participation. Once authorized, your execution desk will receive access to submit quotes."
    elif email_purpose == "BANK_HEADS_UP":
        subject = f"HEADS UP: New RFQ Pending Bank Approval ({customer_branding}) - {ref_no}"
        banner_title = "RFQ Pending Bank Approval"
        salutation = f"Dear <strong>{bank_name} FX &amp; Treasury Desk</strong>,"
        intro_text = f"A new Request for Quotation (RFQ) on behalf of <strong>{customer_branding}</strong> has been received by your bank and is currently <strong>pending authorization from your bank's designated approver</strong>."
        instruction_text = "Please review the required trade specifications below. You will receive a direct access link to submit your quotation as soon as your bank's approver authorizes participation."
    elif email_purpose == "APPROVED_BY_BANK":
        subject = f"ACTION REQUIRED: RFQ {ref_no} Authorized - Submit Your Quote"
        banner_title = "RFQ Authorized for Desk Submission"
        salutation = f"Dear <strong>{bank_name} FX &amp; Treasury Desk</strong>,"
        intro_text = f"Your bank's authorized approver has <strong>approved participation</strong> for RFQ <strong>{ref_no}</strong> on behalf of <strong>{customer_branding}</strong>."
        instruction_text = "Please review the required trade specifications below and access the live portal to enter your quotation."
    elif email_purpose == "WINDOW_START_REMINDER_APPROVER":
        subject = f"URGENT: RFQ {ref_no} Starts in 15 Minutes - Bank Approval Required ({customer_branding})"
        banner_title = "Urgent: RFQ Window Opens in 15 Minutes &bull; Approval Required"
        salutation = f"Dear <strong>{bank_name} Authorized Approver</strong>,"
        intro_text = f"This is an urgent reminder that Request for Quotation (RFQ) <strong>{ref_no}</strong> on behalf of <strong>{customer_branding}</strong> will open for live quotation in <strong>15 minutes</strong>, but your bank's authorization is still <strong>pending</strong>."
        instruction_text = "Please authorize your bank's participation immediately so your execution desk can submit quotes as soon as the window opens."
    elif email_purpose == "WINDOW_START_REMINDER_EXECUTION":
        subject = f"REMINDER: RFQ {ref_no} Opens in 15 Minutes - Prepare Your Quotation ({customer_branding})"
        banner_title = "Quotation Window Opens in 15 Minutes"
        salutation = f"Dear <strong>{bank_name} FX &amp; Treasury Desk</strong>,"
        intro_text = f"This is a reminder that the live quotation window for RFQ <strong>{ref_no}</strong> on behalf of <strong>{customer_branding}</strong> will open in <strong>15 minutes</strong>."
        instruction_text = "Please access the quotation portal below to review terms and be ready to submit your quote when the window opens."
    else:
        subject = f"ACTION REQUIRED: New RFQ Request from {customer_branding} - {pair_str} - {ref_no}"
        banner_title = "New Request for Quotation"
        salutation = f"Dear <strong>{bank_name} FX &amp; Treasury Desk</strong>,"
        intro_text = f"You have received a new Request for Quotation (RFQ) on behalf of <strong>{customer_branding}</strong>."
        instruction_text = "Please review the required trade specifications below and access the live portal to enter your quotation."

    # 8. Action Box / Call To Action
    if email_purpose == "BANK_HEADS_UP":
        action_box_html = f"""
            <!-- PENDING APPROVAL INFORMATION BOX -->
            <div style="background-color: #fffbeb; border: 1px solid #fde68a; border-left: 5px solid #f59e0b; border-radius: 8px; padding: 16px 20px; margin: 28px 0 20px 0; text-align: center;">
                <span style="font-size: 11px; font-weight: 800; color: #b45309; letter-spacing: 1px; text-transform: uppercase; display: block; margin-bottom: 4px;">
                    &#9203; ACTION STATUS &bull; PENDING AUTHORIZATION
                </span>
                <div style="font-size: 15px; font-weight: 800; color: #92400e; line-height: 1.4;">
                    Awaiting Bank Approver Authorization
                </div>
                <p style="margin: 6px 0 0 0; font-size: 12px; color: #78350f; line-height: 1.4;">
                    Your bank's designated approver has been notified with a secure authorization link. Once approved, your execution desk will automatically receive an invitation email with the live quotation portal link.
                </p>
            </div>
        """
    elif email_purpose in ("BANK_APPROVAL_REQUIRED", "WINDOW_START_REMINDER_APPROVER"):
        action_box_html = f"""
            <!-- PRIMARY CALL TO ACTION BUTTON (APPROVER) -->
            <div style="text-align: center; margin: 36px 0 20px 0;">
                <a href="{link}" style="background-color: #2563eb; color: #ffffff; padding: 15px 36px; text-decoration: none; border-radius: 8px; font-weight: 800; font-size: 15px; display: inline-block; box-shadow: 0 4px 6px -1px rgba(37, 99, 235, 0.3); letter-spacing: 0.5px;">
                    &#9889; Review &amp; Authorize RFQ Participation &rarr;
                </a>
            </div>

            <!-- DIRECT LINK FALLBACK -->
            <p style="text-align: center; margin: 0 0 24px 0; font-size: 12px; color: #64748b; line-height: 1.5;">
                Or copy and paste this authorization link into your browser:<br/>
                <a href="{link}" style="color: #0284c7; word-break: break-all; font-size: 12px; text-decoration: underline;">{link}</a>
            </p>
        """
    else:
        action_box_html = f"""
            <!-- PRIMARY CALL TO ACTION BUTTON -->
            <div style="text-align: center; margin: 36px 0 20px 0;">
                <a href="{link}" style="background-color: #0f172a; color: #ffffff; padding: 15px 36px; text-decoration: none; border-radius: 8px; font-weight: 800; font-size: 15px; display: inline-block; box-shadow: 0 4px 6px -1px rgba(15, 23, 42, 0.25); letter-spacing: 0.5px;">
                    Access Quotation Portal &amp; Submit Quote &rarr;
                </a>
            </div>

            <!-- DIRECT LINK FALLBACK -->
            <p style="text-align: center; margin: 0 0 24px 0; font-size: 12px; color: #64748b; line-height: 1.5;">
                Or copy and paste this link into your browser:<br/>
                <a href="{link}" style="color: #0284c7; word-break: break-all; font-size: 12px; text-decoration: underline;">{link}</a>
            </p>
        """

    # 9. Extra T-Bill Rows if applicable
    tbill_rows = ""
    if rfq_type == "TBILL":
        s_start = getattr(rfq, "settlement_date_start", "") or ""
        s_end = getattr(rfq, "settlement_date_end", "") or ""
        m_start = getattr(rfq, "maturity_date_start", "") or ""
        m_end = getattr(rfq, "maturity_date_end", "") or ""
        
        settle_range = f"{s_start} to {s_end}" if s_start and s_end and s_start != s_end else (s_start or "N/A")
        mat_range = f"{m_start} to {m_end}" if m_start and m_end and m_start != m_end else (m_start or "N/A")

        tbill_rows = f"""
                        <tr style="background-color: #ffffff; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px 16px; font-size: 13px; font-weight: 600; color: #64748b; width: 35%;">Settlement Window</td>
                            <td style="padding: 12px 16px; font-size: 14px; font-weight: 600; color: #0f172a;">{settle_range}</td>
                        </tr>
                        <tr style="background-color: #f8fafc; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px 16px; font-size: 13px; font-weight: 600; color: #64748b; width: 35%;">Maturity Window</td>
                            <td style="padding: 12px 16px; font-size: 14px; font-weight: 600; color: #0f172a;">{mat_range}</td>
                        </tr>
        """

    # 10. HTML Template
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M EEST")

    html_body = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{subject}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f1f5f9; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #1e293b; -webkit-font-smoothing: antialiased;">
    <div style="max-width: 650px; margin: 30px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05), 0 4px 6px -2px rgba(0, 0, 0, 0.025); border: 1px solid #e2e8f0;">
        
        <!-- HEADER -->
        <div style="background-color: #0f172a; padding: 26px 32px; color: #ffffff;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td>
                        <span style="font-size: 11px; font-weight: 800; letter-spacing: 1.5px; text-transform: uppercase; color: #38bdf8; display: block; margin-bottom: 4px;">{platform_name.upper()} &bull; FX &amp; MONEY MARKET</span>
                        <h1 style="margin: 0; font-size: 20px; font-weight: 700; color: #ffffff;">{banner_title}</h1>
                    </td>
                    <td style="text-align: right; vertical-align: middle;">
                        <span style="background-color: rgba(56, 189, 248, 0.15); color: #38bdf8; padding: 6px 14px; border-radius: 20px; font-size: 12px; font-weight: 700; border: 1px solid rgba(56, 189, 248, 0.3); display: inline-block;">
                            {customer_branding}
                        </span>
                    </td>
                </tr>
            </table>
        </div>

        <!-- BODY -->
        <div style="padding: 32px;">
            <p style="margin-top: 0; margin-bottom: 16px; font-size: 15px; color: #334155; line-height: 1.6;">
                {salutation}
            </p>
            <p style="margin-top: 0; margin-bottom: 24px; font-size: 14px; color: #334155; line-height: 1.6;">
                {intro_text} {instruction_text}
            </p>

            <!-- SUBMISSION DEADLINE ALERT BOX -->
            <div style="background-color: #fef2f2; border: 1px solid #fecaca; border-left: 5px solid #dc2626; border-radius: 8px; padding: 16px 20px; margin-bottom: 26px;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="vertical-align: middle; width: 32px; font-size: 22px; color: #dc2626;">
                            &#9200;
                        </td>
                        <td style="vertical-align: top;">
                            <span style="font-size: 11px; font-weight: 800; color: #991b1b; letter-spacing: 1px; text-transform: uppercase; display: block; margin-bottom: 4px;">
                                SUBMISSION DEADLINE &bull; MANDATORY CUTOFF
                            </span>
                            <div style="font-size: 16px; font-weight: 800; color: #991b1b; line-height: 1.4;">
                                {deadline_cairo_str}
                            </div>
                            {f'<div style="font-size: 12px; font-weight: 600; color: #b91c1c; margin-top: 2px;">({deadline_utc_str})</div>' if deadline_utc_str else ''}
                            <p style="margin: 6px 0 0 0; font-size: 12px; color: #7f1d1d; line-height: 1.4;">
                                &#9888; Quotes submitted after this cutoff cannot be accepted. The portal will automatically lock upon window closure.
                            </p>
                        </td>
                    </tr>
                </table>
            </div>

            <!-- CORE TRADE SPECIFICATIONS TABLE -->
            <div style="border: 1px solid #e2e8f0; border-radius: 10px; overflow: hidden; margin-bottom: 28px;">
                <div style="background-color: #f8fafc; padding: 12px 18px; border-bottom: 1px solid #e2e8f0;">
                    <span style="font-size: 12px; font-weight: 800; color: #475569; letter-spacing: 0.5px; text-transform: uppercase;">
                        Trade Specifications &bull; {ref_no}
                    </span>
                </div>
                <table style="width: 100%; border-collapse: collapse; text-align: left;">
                    <tbody>
                        <tr style="background-color: #ffffff; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px 16px; font-size: 13px; font-weight: 600; color: #64748b; width: 35%;">RFQ Reference</td>
                            <td style="padding: 12px 16px; font-size: 14px; font-weight: 700; color: #0f172a; font-family: monospace;">{ref_no}</td>
                        </tr>
                        <tr style="background-color: #f8fafc; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px 16px; font-size: 13px; font-weight: 600; color: #64748b;">Requesting Legal Entity</td>
                            <td style="padding: 12px 16px; font-size: 14px; font-weight: 700; color: #0f172a;">
                                {entity_name}
                                {entity_meta_html}
                            </td>
                        </tr>
                        <tr style="background-color: #ffffff; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px 16px; font-size: 13px; font-weight: 600; color: #64748b;">Product &amp; Quotation Base</td>
                            <td style="padding: 12px 16px; font-size: 14px; font-weight: 600; color: #0f172a;">
                                <span style="margin-right: 8px;"><strong>{rfq_type}</strong></span>
                                {base_badge}
                                <div style="font-size: 12px; color: #64748b; margin-top: 4px; font-weight: normal;">{base_note}</div>
                            </td>
                        </tr>
                        <tr style="background-color: #ffffff; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px 16px; font-size: 13px; font-weight: 600; color: #64748b;">Currency Pair &amp; Flow</td>
                            <td style="padding: 12px 16px; font-size: 14px; color: #0f172a;">
                                <span style="background-color: #e0f2fe; color: #0369a1; padding: 3px 8px; border-radius: 4px; font-weight: 800; font-size: 13px; letter-spacing: 0.5px; margin-right: 8px;">
                                    {pair_str}
                                </span>
                                <span style="font-size: 13px; color: #334155;">{direction_display}</span>
                            </td>
                        </tr>
                        <tr style="background-color: #f8fafc; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px 16px; font-size: 13px; font-weight: 600; color: #64748b;">Quotation Amount</td>
                            <td style="padding: 12px 16px; font-size: 17px; font-weight: 800; color: #0f172a; letter-spacing: 0.5px;">
                                {amount_display}
                            </td>
                        </tr>
                        <tr style="background-color: #ffffff; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px 16px; font-size: 13px; font-weight: 600; color: #64748b;">Target Value Date</td>
                            <td style="padding: 12px 16px; font-size: 14px; font-weight: 700; color: #0f172a;">
                                {value_date_display}
                            </td>
                        </tr>
                        <tr style="background-color: #f8fafc; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 12px 16px; font-size: 13px; font-weight: 600; color: #64748b;">Alternative Value Date</td>
                            <td style="padding: 12px 16px; font-size: 13px; color: #0f172a;">
                                {alt_date_badge}
                                <div style="font-size: 12px; color: #64748b; margin-top: 4px;">{alt_date_desc}</div>
                            </td>
                        </tr>
                        {tbill_rows}
                    </tbody>
                </table>
            </div>

            {action_box_html}

            <!-- INSTITUTIONAL RESERVATION CLAUSE -->
            <div style="background-color: #f8fafc; border-left: 3px solid #94a3b8; padding: 10px 14px; margin-top: 14px; font-size: 11px; color: #64748b; line-height: 1.4;">
                <strong>Reservation of Rights:</strong> The corporate treasury desk reserves the right to amend, postpone, or withdraw this quotation request prior to the scheduled quotation window opening. Counterparties will be promptly notified of any schedule alterations.
            </div>

            <!-- SECURITY & AUTHENTICATION CALLOUT -->
            <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 14px 18px; margin-top: 14px;">
                <p style="margin: 0; font-size: 12px; color: #475569; line-height: 1.5;">
                    <strong>Institution-Specific Access:</strong> This secure access token is uniquely generated for <strong>{bank_name}</strong>. Access is protected by 2FA OTP verification delivered directly to registered traders. Please do not forward this message.
                </p>
            </div>
        </div>

        <!-- FOOTER -->
        <div style="background-color: #f8fafc; border-top: 1px solid #e2e8f0; padding: 20px 32px; text-align: center;">
            <p style="margin: 0 0 4px 0; font-size: 12px; color: #64748b; font-weight: 600;">
                {platform_name} &bull; Institutional Financial Trading
            </p>
            <p style="margin: 0; font-size: 11px; color: #94a3b8;">
                Generated on {current_time_str} for {customer_branding}. Strictly confidential and intended solely for the designated recipient.
            </p>
            <p style="margin: 8px auto 0 auto; font-size: 10px; color: #94a3b8; line-height: 1.4; max-width: 540px;">
                <strong>Legal Notice:</strong> {platform_name} operates solely as an independent communications and workflow technology platform (&ldquo;AS IS&rdquo;). Neither {platform_name} nor its affiliates are party to this transaction or assume credit, market, or settlement liability. All commercial commitments and settlement obligations exist exclusively between {entity_name} and {bank_name}.
            </p>
        </div>

    </div>
</body>
</html>
"""
    return (subject, html_body)


def build_quotation_withdrawn_bank_email(
    rfq: Any,
    bank_name: str = "Bank Partner",
    customer_branding: str = "Corporate Treasury",
    platform_name: str = "Grow Treasury Platform"
) -> Tuple[str, str]:
    """
    Generates an official, dignified withdrawal notification email to counterparties when an RFQ is cancelled.
    Notice: Crucially conceals internal corporate cancellation reasons from external banks.
    """
    from datetime import datetime
    ref_no = getattr(rfq, "ref_no", "RFQ")
    rfq_type = getattr(rfq, "type", "FX_SPOT")
    buy_curr = getattr(rfq, "buy_currency", None) or ""
    sell_curr = getattr(rfq, "sell_currency", None) or ""
    pair_str = f"{buy_curr}/{sell_curr}" if buy_curr and sell_curr else (buy_curr or sell_curr or rfq_type)

    raw_amount = getattr(rfq, "amount", None)
    amount_str = f"{float(raw_amount):,.2f}" if raw_amount is not None else "N/A"

    subject = f"NOTICE OF WITHDRAWAL: RFQ {ref_no} Cancelled - {customer_branding}"
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M EEST")

    html_body = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{subject}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f1f5f9; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #1e293b; -webkit-font-smoothing: antialiased;">
    <div style="max-width: 650px; margin: 30px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05); border: 1px solid #e2e8f0;">
        
        <!-- HEADER -->
        <div style="background-color: #334155; padding: 26px 32px; color: #ffffff;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td>
                        <span style="font-size: 11px; font-weight: 800; letter-spacing: 1.5px; text-transform: uppercase; color: #94a3b8; display: block; margin-bottom: 4px;">{platform_name.upper()} &bull; OFFICIAL NOTICE</span>
                        <h1 style="margin: 0; font-size: 20px; font-weight: 700; color: #ffffff;">Quotation Request Withdrawn</h1>
                    </td>
                    <td style="text-align: right; vertical-align: middle;">
                        <span style="background-color: rgba(255, 255, 255, 0.15); color: #ffffff; padding: 6px 14px; border-radius: 20px; font-size: 12px; font-weight: 700; display: inline-block;">
                            {customer_branding}
                        </span>
                    </td>
                </tr>
            </table>
        </div>

        <!-- BODY -->
        <div style="padding: 32px;">
            <p style="margin-top: 0; margin-bottom: 16px; font-size: 15px; color: #334155; line-height: 1.6;">
                Dear <strong>{bank_name} FX &amp; Treasury Desk</strong>,
            </p>
            <p style="margin-top: 0; margin-bottom: 24px; font-size: 14px; color: #334155; line-height: 1.6;">
                Please be advised that Request for Quotation (RFQ) <strong>{ref_no}</strong> previously released on behalf of <strong>{customer_branding}</strong> has been officially <strong>withdrawn and cancelled</strong> prior to execution.
            </p>

            <!-- WITHDRAWAL NOTICE BOX -->
            <div style="background-color: #f8fafc; border: 1px solid #cbd5e1; border-left: 5px solid #64748b; border-radius: 8px; padding: 18px 20px; margin-bottom: 26px;">
                <p style="margin: 0 0 6px 0; font-size: 14px; font-weight: 700; color: #1e293b;">
                    No Action or Quotation is Required
                </p>
                <p style="margin: 0; font-size: 13px; color: #475569; line-height: 1.5;">
                    This quotation request was officially withdrawn by the corporate treasury desk. Counterparty portal submission links for this RFQ have been deactivated. We apologize for any inconvenience.
                </p>
            </div>

            <!-- RFQ REFERENCE SUMMARY -->
            <div style="border: 1px solid #e2e8f0; border-radius: 10px; overflow: hidden; margin-bottom: 24px;">
                <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 13px;">
                    <tbody>
                        <tr style="background-color: #ffffff; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 10px 16px; font-weight: 600; color: #64748b; width: 35%;">RFQ Reference</td>
                            <td style="padding: 10px 16px; font-weight: 700; color: #0f172a; font-family: monospace;">{ref_no}</td>
                        </tr>
                        <tr style="background-color: #f8fafc; border-bottom: 1px solid #e2e8f0;">
                            <td style="padding: 10px 16px; font-weight: 600; color: #64748b;">Instrument / Currency</td>
                            <td style="padding: 10px 16px; font-weight: 600; color: #0f172a;">{rfq_type} &bull; {pair_str}</td>
                        </tr>
                        <tr style="background-color: #ffffff;">
                            <td style="padding: 10px 16px; font-weight: 600; color: #64748b;">Quotation Amount</td>
                            <td style="padding: 10px 16px; font-weight: 700; color: #0f172a;">{amount_str} {buy_curr or ''}</td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <p style="margin: 0; font-size: 13px; color: #64748b; line-height: 1.5;">
                Should the client reschedule or issue a new request for quotation, your desk will receive a separate formal invitation.
            </p>
        </div>

        <!-- FOOTER -->
        <div style="background-color: #f8fafc; border-top: 1px solid #e2e8f0; padding: 20px 32px; text-align: center;">
            <p style="margin: 0 0 4px 0; font-size: 12px; color: #64748b; font-weight: 600;">
                {platform_name} &bull; Institutional Financial Trading
            </p>
            <p style="margin: 0; font-size: 11px; color: #94a3b8;">
                Notice dispatched on {current_time_str} on behalf of {customer_branding}.
            </p>
        </div>

    </div>
</body>
</html>
"""
    return (subject, html_body)

