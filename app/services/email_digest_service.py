"""
Email Digest Generator Service
Provides modern, corporate-grade SaaS HTML templates for consolidated background emails.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime


def build_lg_renewal_digest_html(
    customer_name: str,
    recipient_name: Optional[str] = None,
    digest_title: str = "DAILY EXPIRY & RENEWAL DIGEST",
    items: List[Dict[str, Any]] = None,
    portal_url: str = "https://app.growtreasury.com/lg/history"
) -> str:
    """
    Generates a responsive HTML digest email for LG Renewal & Expiry Reminders.
    
    Each item in `items` is expected to have:
    - lg_number: str
    - lg_type: str
    - issuing_bank: str
    - currency: str
    - amount: float
    - amount_formatted: str
    - expiry_date_str: str
    - days_until_expiry: int
    - urgency_level: str ("urgent" or "normal")
    - auto_renewal: bool
    """
    items = items or []
    total_count = len(items)

    # Calculate financial exposure per currency
    currency_totals: Dict[str, float] = {}
    urgent_count = 0
    normal_count = 0

    for item in items:
        curr = item.get("currency", "EGP")
        amt = float(item.get("amount", 0.0))
        currency_totals[curr] = currency_totals.get(curr, 0.0) + amt

        if item.get("urgency_level") == "urgent" or item.get("days_until_expiry", 999) <= 30:
            urgent_count += 1
        else:
            normal_count += 1

    # Formatted exposure summary string
    exposure_parts = [f"{curr} {amt:,.2f}" for curr, amt in currency_totals.items()]
    exposure_summary = " | ".join(exposure_parts) if exposure_parts else "N/A"

    # Greeting
    greeting_name = recipient_name if recipient_name else "Treasury Team"

    # Build Table Rows
    table_rows = []
    for idx, item in enumerate(items):
        bg_color = "#ffffff" if idx % 2 == 0 else "#f8fafc"
        days_left = item.get("days_until_expiry", 0)
        is_urgent = item.get("urgency_level") == "urgent"


        if is_urgent:
            badge_html = f"""<span style="display: inline-block; padding: 4px 10px; border-radius: 12px; background-color: #fef2f2; color: #dc2626; font-size: 11px; font-weight: 700; border: 1px solid #fecaca;">🔴 Urgent ({days_left}d)</span>"""
        else:
            badge_html = f"""<span style="display: inline-block; padding: 4px 10px; border-radius: 12px; background-color: #fffbeb; color: #d97706; font-size: 11px; font-weight: 700; border: 1px solid #fef3c7;">🟡 Notice ({days_left}d)</span>"""

        auto_renew_badge = "Auto-Renew" if item.get("auto_renewal") else "Manual"

        row = f"""
        <tr style="background-color: {bg_color}; border-bottom: 1px solid #e2e8f0;">
            <td style="padding: 12px 14px; font-size: 13px; font-weight: 600; color: #0f172a;">{item.get('lg_number', 'N/A')}</td>
            <td style="padding: 12px 14px; font-size: 13px; color: #334155;">{item.get('issuing_bank', 'N/A')}</td>
            <td style="padding: 12px 14px; font-size: 13px; color: #334155;">{item.get('lg_type', 'N/A')} <span style="font-size: 11px; color: #64748b;">({auto_renew_badge})</span></td>
            <td style="padding: 12px 14px; font-size: 13px; font-weight: 600; color: #0f172a; text-align: right;">{item.get('amount_formatted', 'N/A')}</td>
            <td style="padding: 12px 14px; font-size: 13px; color: #334155; text-align: center;">{item.get('expiry_date_str', 'N/A')}</td>
            <td style="padding: 12px 14px; text-align: center;">{badge_html}</td>
        </tr>
        """
        table_rows.append(row)

    rows_html = "".join(table_rows)
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M EEST")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{digest_title}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f1f5f9; font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #1e293b; -webkit-font-smoothing: antialiased;">
    <div style="max-width: 680px; margin: 30px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05), 0 4px 6px -2px rgba(0, 0, 0, 0.025); border: 1px solid #e2e8f0;">
        
        <!-- HEADER -->
        <div style="background-color: #0f172a; padding: 28px 32px; color: #ffffff;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td>
                        <span style="font-size: 11px; font-weight: 800; tracking: 1.5px; letter-spacing: 1.5px; text-transform: uppercase; color: #38bdf8; display: block; margin-bottom: 4px;">GROW TREASURY PLATFORM</span>
                        <h1 style="margin: 0; font-size: 20px; font-weight: 700; color: #ffffff;">{digest_title}</h1>
                    </td>
                    <td style="text-align: right; vertical-align: middle;">
                        <span style="background-color: rgba(56, 189, 248, 0.15); color: #38bdf8; padding: 6px 12px; border-radius: 20px; font-size: 11px; font-weight: 700; border: 1px solid rgba(56, 189, 248, 0.3);">
                            {customer_name}
                        </span>
                    </td>
                </tr>
            </table>
        </div>

        <!-- CONTENT CONTAINER -->
        <div style="padding: 32px;">
            <p style="margin-top: 0; margin-bottom: 20px; font-size: 15px; line-height: 1.6; color: #334155;">
                Dear <strong>{greeting_name}</strong>,<br>
                Below is your consolidated summary of Letters of Guarantee (LGs) approaching expiry that require review or action.
            </p>

            <!-- KPI SUMMARY CARDS -->
            <table style="width: 100%; border-collapse: separate; border-spacing: 12px; margin: -12px -12px 24px -12px;">
                <tr>
                    <td style="width: 33%; background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; text-align: center;">
                        <span style="font-size: 11px; font-weight: 700; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; display: block; margin-bottom: 6px;">Expiring Instruments</span>
                        <span style="font-size: 22px; font-weight: 800; color: #0f172a;">{total_count} LG(s)</span>
                    </td>
                    <td style="width: 34%; background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; text-align: center;">
                        <span style="font-size: 11px; font-weight: 700; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; display: block; margin-bottom: 6px;">Total Exposure</span>
                        <span style="font-size: 15px; font-weight: 700; color: #0284c7; word-break: break-word;">{exposure_summary}</span>
                    </td>
                    <td style="width: 33%; background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; text-align: center;">
                        <span style="font-size: 11px; font-weight: 700; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; display: block; margin-bottom: 6px;">Urgency Breakdown</span>
                        <span style="font-size: 13px; font-weight: 700; color: #0f172a;">
                            <span style="color: #dc2626;">{urgent_count} Critical</span> / <span style="color: #d97706;">{normal_count} Notice</span>
                        </span>
                    </td>
                </tr>
            </table>

            <!-- DETAILED TABLE -->
            <div style="overflow-x: auto; border: 1px solid #e2e8f0; border-radius: 8px; margin-bottom: 28px;">
                <table style="width: 100%; border-collapse: collapse; text-align: left;">
                    <thead>
                        <tr style="background-color: #f8fafc; border-bottom: 2px solid #e2e8f0;">
                            <th style="padding: 12px 14px; font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase; letter-spacing: 0.5px;">LG Ref</th>
                            <th style="padding: 12px 14px; font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase; letter-spacing: 0.5px;">Bank</th>
                            <th style="padding: 12px 14px; font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase; letter-spacing: 0.5px;">Type</th>
                            <th style="padding: 12px 14px; font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase; letter-spacing: 0.5px; text-align: right;">Amount</th>
                            <th style="padding: 12px 14px; font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase; letter-spacing: 0.5px; text-align: center;">Expiry Date</th>
                            <th style="padding: 12px 14px; font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase; letter-spacing: 0.5px; text-align: center;">Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>

            <!-- CALL TO ACTION -->
            <div style="text-align: center; margin: 32px 0 16px 0;">
                <a href="{portal_url}" style="background-color: #1e40af; color: #ffffff; padding: 14px 32px; text-decoration: none; border-radius: 8px; font-weight: 700; font-size: 14px; display: inline-block; box-shadow: 0 4px 6px -1px rgba(30, 64, 175, 0.2);">
                    Review Renewal Requests in Treasury Portal &rarr;
                </a>
            </div>
        </div>

        <!-- FOOTER -->
        <div style="background-color: #f8fafc; border-top: 1px solid #e2e8f0; padding: 20px 32px; text-align: center;">
            <p style="margin: 0 0 6px 0; font-size: 12px; color: #64748b; font-weight: 500;">
                Grow Treasury Management Platform &bull; Automated Daily Notification Service
            </p>
            <p style="margin: 0; font-size: 11px; color: #94a3b8;">
                Generated on {current_time_str} for {customer_name}. Confidential.
            </p>
        </div>

    </div>
</body>
</html>
"""
    return html
