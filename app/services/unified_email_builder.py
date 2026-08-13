"""
Unified Email Builder Service
Provides high-aesthetic, corporate SaaS HTML templates for all outgoing platform emails.
"""

from typing import List, Dict, Any, Optional
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
