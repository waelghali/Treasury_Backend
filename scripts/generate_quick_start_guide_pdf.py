"""
Generates the illustrated Full-Suite Quick Start Guide PDF at:
    app/templates/quick_start_guide.pdf

Covers:
  Page 1 — Welcome, Roles & Day-1 Setup (Commercial & Guided Free Trial)
  Page 2 — Module 1: LG Custody & Lifecycle Governance
  Page 3 — Module 2: Outbound LG Issuance & Multi-Bank Facility Scoring
  Page 4 — Module 3: Competitive FX & Sovereign T-Bill Quotations
  Page 5 — Module 4: Cash & Bank Statement Auto-Reconciliation
  Page 6 — AI Treasury Copilot, Institutional Security & Support

Usage:
    venv\\Scripts\\python.exe scripts\\generate_quick_start_guide_pdf.py
"""

import base64
import io
import os
import sys
from pathlib import Path
from PIL import Image
from weasyprint import HTML

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

OUT_PDF = ROOT_DIR / "app" / "templates" / "quick_start_guide.pdf"
SHOTS_DIR = ROOT_DIR / "frontend" / "public" / "demo" / "shots"

GROW_LEAF_PATH = (
    "m 35.940217,38.500167 c -0.0102,-1.94922 -0.12121,-4.321 -0.24676,-5.27062 "
    "-0.53779,-4.06762 -0.55018,-3.90205 0.42854,-5.72498 1.42388,-2.6521 3.9536,-5.33517 "
    "6.57854,-6.97734 1.02194,-0.63932 0.45123,-0.73376 -0.80684,-0.13351 "
    "-2.28239,1.089 -5.1891,3.7869 -6.41699,5.95601 -0.33125,0.58518 -0.63363,1.06396 "
    "-0.67196,1.06396 -0.0383,0 -0.21906,-0.42937 -0.40164,-0.95416 "
    "-0.66852,-1.92156 -1.89713,-3.98345 -3.10208,-5.20601 -1.1931,-1.21054 "
    "-4.18646,-3.11616 -4.4663,-2.84332 -0.0777,0.0757 0.0759,0.26954 0.34133,0.43069 "
    "0.84423,0.51259 3.47557,2.99199 4.06585,3.83108 0.67504,0.95956 1.95503,4.56758 "
    "2.39143,6.74092 0.47178,2.34953 0.5769,5.56945 0.29933,9.16883 -0.28246,3.66278 "
    "-0.30658,3.58963 1.15743,3.50983 l 0.86861,-0.0473 z m -8.18508,-15.61123 "
    "c -1.95988,-0.20231 -3.05786,-1.05757 -5.15718,-4.01712 -1.26619,-1.78504 "
    "-2.02842,-2.47758 -3.41693,-3.10453 -1.08938,-0.49188 -1.12026,-0.52394 "
    "-0.66242,-0.68784 1.19655,-0.42834 4.34471,-0.93301 5.82019,-0.93301 2.17519,0 "
    "3.34261,0.35476 4.5607,1.38593 1.1149,0.94382 1.76073,1.89432 2.24296,3.30108 "
    "0.42958,1.25318 0.56452,1.12142 0.1617,-0.1579 -0.44703,-1.41969 -0.95823,-2.28173 "
    "-1.93787,-3.26785 -1.27225,-1.28065 -2.27163,-1.66214 -4.6262,-1.76593 "
    "-2.26402,-0.0998 -3.82854,0.11508 -6.58795,0.90482 -1.49881,0.42897 -1.97851,0.64126 "
    "-1.97851,0.87559 0,0.23662 0.2321,0.33522 0.98734,0.41944 1.99692,0.22269 "
    "3.56151,1.26336 5.05906,3.36499 2.07789,2.91607 3.42889,3.86462 5.4386,3.8185 "
    "l 1.15815,-0.0266 z m 15.57234,4.97727 c 1.83993,-0.77319 3.42321,-2.43328 "
    "5.71815,-5.99558 1.70556,-2.64745 2.7356,-3.84961 4.16575,-4.86185 1.383,-0.97886 "
    "2.93567,-1.57201 4.55918,-1.74169 1.126,-0.11769 1.15816,-0.136 1.15816,-0.65953 "
    "0,-0.51167 -0.0865,-0.56892 -1.73723,-1.14953 -4.87344,-1.71417 -7.66012,-2.28607 "
    "-11.09895,-2.27781 -2.64044,0.006 -4.0282,0.27845 -5.65937,1.10969 "
    "-1.19065,0.60675 -3.00117,2.42843 -3.87019,3.89406 -0.83792,1.41315 "
    "-1.79843,4.08086 -1.94933,5.41404 l -0.11315,0.9996 0.37262,-1.27222 "
    "c 1.0436,-3.56304 2.71139,-6.1418 4.93333,-7.62796 1.89712,-1.26891 2.6465,-1.44834 "
    "5.99655,-1.43583 2.45565,0.009 3.21784,0.0847 5.01866,0.49711 2.10333,0.48174 "
    "5.11517,1.35572 5.11517,1.48433 0,0.0367 -0.69548,0.40743 -1.54552,0.82382 "
    "-2.55861,1.25335 -3.7578,2.57959 -6.75457,7.47022 -1.51064,2.46532 -3.29949,4.2937 "
    "-4.84725,4.95439 -0.59091,0.25224 -1.66518,0.50618 -2.48771,0.58806 "
    "-1.41628,0.14098 -1.42885,0.1466 -0.57908,0.259 1.25416,0.16589 2.46295,0.008 "
    "3.60478,-0.47232 z"
)


def encode_screenshot_b64(rel_path: str, max_width: int = 1280, quality: int = 86) -> str:
    full_path = SHOTS_DIR / rel_path
    if not full_path.exists():
        raise FileNotFoundError(f"Missing screenshot: {full_path}")
    with Image.open(full_path) as im:
        rgb = im.convert("RGB")
        if rgb.width > max_width:
            new_h = int(rgb.height * (max_width / rgb.width))
            rgb = rgb.resize((max_width, new_h), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        rgb.save(buf, format="JPEG", quality=quality, optimize=True)
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        return f"data:image/jpeg;base64,{b64}"


def _step_cards_html(steps: list, accent_hex: str) -> str:
    cells = []
    for idx, (title, desc) in enumerate(steps, start=1):
        cells.append(
            f"""
            <td class="step-cell">
              <div class="step-card" style="border-top: 2.5px solid {accent_hex};">
                <div class="step-num" style="color:{accent_hex};">STEP {idx}</div>
                <div class="step-title">{title}</div>
                <div class="step-desc">{desc}</div>
              </div>
            </td>
            """
        )
    return f'<table class="steps-table" role="presentation"><tr>{"".join(cells)}</tr></table>'


def _module_page_html(
    module_badge: str,
    accent_hex: str,
    badge_bg: str,
    title: str,
    subtitle: str,
    steps: list,
    shot1_uri: str,
    shot1_label: str,
    shot1_caption: str,
    shot2_uri: str,
    shot2_label: str,
    shot2_caption: str,
) -> str:
    return f"""
<div class="page-break"></div>
<div class="module-header" style="border-left: 4px solid {accent_hex};">
  <span class="module-pill" style="background:{badge_bg}; color:{accent_hex};">{module_badge}</span>
  <h2 class="module-title">{title}</h2>
  <p class="module-sub">{subtitle}</p>
</div>

{_step_cards_html(steps, accent_hex)}

<div class="shot-card">
  <div class="shot-frame">
    <img src="{shot1_uri}" alt="{shot1_label}" />
  </div>
  <div class="shot-caption">
    <span class="shot-tag" style="background:{badge_bg}; color:{accent_hex};">{shot1_label}</span>
    <span>{shot1_caption}</span>
  </div>
</div>

<div class="shot-card">
  <div class="shot-frame">
    <img src="{shot2_uri}" alt="{shot2_label}" />
  </div>
  <div class="shot-caption">
    <span class="shot-tag" style="background:{badge_bg}; color:{accent_hex};">{shot2_label}</span>
    <span>{shot2_caption}</span>
  </div>
</div>
""".strip()


def build_guide_html() -> str:
    shots = {
        "custody_cockpit": encode_screenshot_b64("custody_lifecycle/step1_cockpit_and_protection_check.png"),
        "custody_action": encode_screenshot_b64("custody_action_center/step1_action_center_radar.png"),
        "issuance_score": encode_screenshot_b64("issuance_2_2/step_03_smart_facility_scoring.png"),
        "issuance_issue": encode_screenshot_b64("issuance_2_3/step_02_autofill_confirm_issue.png"),
        "fx_floor": encode_screenshot_b64("quotations_3_1/step_05_blind_live_floor.png"),
        "fx_receipt": encode_screenshot_b64("quotations_3_3/step_05_scoped_hmac_deal_receipt.png"),
        "recon_lines": encode_screenshot_b64("recon_4_1/step_03_statement_lines_workspace.png"),
        "recon_export": encode_screenshot_b64("recon_4_4/step_04_live_balanced_journal_preview.png"),
    }

    page2 = _module_page_html(
        module_badge="MODULE 1 · LG CUSTODY & LIFECYCLE",
        accent_hex="#1d4ed8",
        badge_bg="#eff6ff",
        title="LG Custody, AI Extraction & Action Center",
        subtitle="Centralize received guarantees, audit protective wording in English or Arabic, and track bank replies end-to-end.",
        steps=[
            ("AI OCR & Wording Shield", "Upload an LG PDF/scan to extract fields in seconds and flag claim-blocking wording hurdles."),
            ("Maker-Checker Control", "Extensions run directly or under rule limits; critical releases, decreases & liquidations route for approval."),
            ("QR-Secured Bank Letters", "Print formal instruction letters carrying cryptographic QR codes and 4-digit security verification pins."),
            ("Closed-Loop Action Center", "Track Pending Prints, Due Renewals, Delivery Proofs & Overdue Bank Replies in one live radar."),
        ],
        shot1_uri=shots["custody_cockpit"],
        shot1_label="LG Cockpit & Risk Check",
        shot1_caption="Single-screen LG record cockpit with live Beneficiary Protection Check, lifecycle timeline, and dual-control action bar.",
        shot2_uri=shots["custody_action"],
        shot2_label="Action Center Radar",
        shot2_caption="Unified Action Center surfacing Approved Requests Pending Print, Approaching Expiry/Renewal, Delivery Confirmation, and Bank Replies.",
    )

    page3 = _module_page_html(
        module_badge="MODULE 2 · OUTBOUND LG ISSUANCE",
        accent_hex="#047857",
        badge_bg="#ecfdf5",
        title="Outbound LG Issuance & Multi-Bank Facility Scoring",
        subtitle="Route project LG requests through internal approvals, compare relationship bank credit lines, and generate bank forms.",
        steps=[
            ("Dept Request & AI Verify", "Business units submit requests via the Issuance Portal; AI checks against the underlying contract/PO."),
            ("Credit Approval Matrix", "Multi-tier internal approvals validate project scope, amount limits, and signatory rules before issuance."),
            ("Smart Facility Scoring", "Ranks available bank credit lines by commission rate, cash margin, SLA speed, and rare-line preservation."),
            ("Auto-Fill & Position Recon", "Reserves headroom, auto-fills official bank PDF applications, and reconciles bank LG position statements."),
        ],
        shot1_uri=shots["issuance_score"],
        shot1_label="Multi-Bank Facility Scoring",
        shot1_caption="Side-by-side bank credit facility scoring showing available headroom, commission cost, cash margin, and smart recommendations.",
        shot2_uri=shots["issuance_issue"],
        shot2_label="1-Click Bank Issuance",
        shot2_caption="Guided issuance wizard generating filled official bank application PDFs and companion legal wording annexes.",
    )

    page4 = _module_page_html(
        module_badge="MODULE 3 · FX & SOVEREIGN T-BILLS",
        accent_hex="#6d28d9",
        badge_bg="#f5f3ff",
        title="Competitive FX & Sovereign T-Bill Sealed Bidding",
        subtitle="Run blind multi-bank tenders directly from your corporate email with zero-password bank dealer verification and audit receipts.",
        steps=[
            ("Multi-Pair / T-Bill Builder", "Build single or multi-leg FX Spot and Sovereign T-Bill RFQs in Execution or Indicative mode with CBE benchmarks."),
            ("Direct Mail & Dealer OTP", "Dispatches through your corporate SMTP/Exchange; bank dealers authenticate via single-use link + 6-digit OTP."),
            ("Blind Live Bidding Floor", "Real-time 1.5s telemetry tracks active desks while sealing competing quotes until the bidding window closes."),
            ("Award & HMAC Deal Receipt", "Compare net standings or cherry-pick legs, enforce countdown/delegation rules, and issue SHA-256 Deal Receipts."),
        ],
        shot1_uri=shots["fx_floor"],
        shot1_label="Blind Live Bidding Floor",
        shot1_caption="Real-time RFQ cockpit showing multi-currency legs, CBE reference evaluation benchmarks, governance trail, and sealed desk status.",
        shot2_uri=shots["fx_receipt"],
        shot2_label="Deal Execution & Receipt",
        shot2_caption="Bank dealer view post-award displaying won leg scopes and a verifiable HMAC-SHA256 cryptographic Deal Execution Receipt.",
    )

    page5 = _module_page_html(
        module_badge="MODULE 4 · BANK AUTO-RECONCILIATION",
        accent_hex="#0f766e",
        badge_bg="#f0fdfa",
        title="Cash & Bank Statement Auto-Reconciliation",
        subtitle="Ingest multi-bank statements, auto-pair inter-bank sweeps and ERP open items, teach self-learning rules, and export GL journals.",
        steps=[
            ("Ingest & Continuity Guard", "Upload Excel, CSV, MT940, or ISO 20022 statements with automatic column mapping and balance-continuity checks."),
            ("4-Pass ERP Auto-Matching", "Pairs 1:1 records, date-lag/fee tolerances, multi-invoice batches, inter-bank sweeps, and returned-wire reversals."),
            ("Self-Learning & Encrypted Vault", "1-click rule induction learns from overrides while protecting counterparties in per-tenant AES-256 vaults."),
            ("Double-Entry ERP Export", "Build custom SAP/Oracle/ERP column profiles, preview balanced debit/credit GL rows, and lock XLSX exports."),
        ],
        shot1_uri=shots["recon_lines"],
        shot1_label="Statement Workspace",
        shot1_caption="Unified transaction ledger highlighting auto-matched ERP records, linked inter-bank sweeps, reversal netting, and learned rules.",
        shot2_uri=shots["recon_export"],
        shot2_label="Balanced GL Journal Export",
        shot2_caption="Customizable accounting & ERP export builder previewing expanded double-entry debit/credit rows prior to one-click XLSX lock.",
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Grow Corporate Treasury Platform — Full-Suite Quick Start Guide</title>
<style>
  @page {{
    size: A4;
    margin: 13mm 14mm 14mm 14mm;
    @bottom-left {{
      content: "Grow Business Development  ·  Grow Corporate Treasury Platform  ·  Full-Suite Quick Start Guide";
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
      font-size: 7.5pt;
      color: #64748b;
    }}
    @bottom-right {{
      content: "Page " counter(page) " of " counter(pages);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
      font-size: 7.5pt;
      font-weight: 600;
      color: #475569;
    }}
  }}
  * {{
    box-sizing: border-box;
  }}
  html, body {{
    margin: 0;
    padding: 0;
    background: #ffffff;
    color: #1e293b;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
    font-size: 9pt;
    line-height: 1.42;
  }}
  .page-break {{
    page-break-before: always;
    break-before: page;
  }}

  /* Hero Masthead (Page 1) */
  .hero-banner {{
    background: #0f172a;
    color: #ffffff;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 13px;
    border-bottom: 4px solid #44aa00;
  }}
  .hero-top {{
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 8px;
  }}
  .hero-top td {{
    padding: 0;
    vertical-align: middle;
  }}
  .hero-brand-name {{
    font-size: 16pt;
    font-weight: 800;
    color: #ffffff;
    letter-spacing: -0.02em;
    line-height: 1.05;
  }}
  .hero-brand-sub {{
    font-size: 7pt;
    font-weight: 700;
    color: #60a5fa;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    margin-top: 2px;
  }}
  .hero-badge {{
    display: inline-block;
    background: rgba(68, 170, 0, 0.18);
    color: #86efac;
    border: 1px solid rgba(134, 239, 172, 0.45);
    border-radius: 999px;
    padding: 3px 10px;
    font-size: 7.5pt;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }}
  .hero-title {{
    margin: 4px 0 4px 0;
    font-size: 16.5pt;
    font-weight: 800;
    color: #ffffff;
    line-height: 1.2;
  }}
  .hero-desc {{
    margin: 0;
    font-size: 8.8pt;
    color: #cbd5e1;
    line-height: 1.4;
  }}

  /* Section Titles on Page 1 & 6 */
  .sec-heading {{
    margin: 11px 0 6px 0;
    padding-left: 8px;
    border-left: 3px solid #1e40af;
    font-size: 10.5pt;
    font-weight: 700;
    color: #0f172a;
  }}

  /* Role Grid */
  .grid-table {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 6px;
    margin: -6px 0 4px 0;
  }}
  .grid-table td {{
    vertical-align: top;
    width: 50%;
  }}
  .info-card {{
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 7px;
    padding: 8px 10px;
  }}
  .info-card-title {{
    font-size: 8.8pt;
    font-weight: 700;
    color: #0f172a;
    margin-bottom: 2px;
  }}
  .role-chip {{
    display: inline-block;
    font-size: 6.8pt;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 1px 6px;
    border-radius: 999px;
    margin-left: 4px;
    vertical-align: middle;
  }}
  .info-card p {{
    margin: 0;
    font-size: 8pt;
    color: #475569;
    line-height: 1.38;
  }}

  /* Day-1 Checklist */
  .setup-box {{
    border: 1px solid #cbd5e1;
    border-radius: 8px;
    padding: 9px 12px;
    background: #ffffff;
    margin-bottom: 8px;
  }}
  .setup-row {{
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 6px;
  }}
  .setup-row:last-child {{
    margin-bottom: 0;
  }}
  .setup-num {{
    width: 22px;
    height: 22px;
    border-radius: 999px;
    background: #1e40af;
    color: #ffffff;
    font-size: 8.5pt;
    font-weight: 700;
    text-align: center;
    line-height: 22px;
  }}
  .setup-text {{
    padding-left: 9px;
    font-size: 8.3pt;
    color: #334155;
    line-height: 1.38;
  }}
  .setup-text strong {{
    color: #0f172a;
  }}

  /* Trial vs Commercial Note */
  .dual-mode-note {{
    background: #eff6ff;
    border: 1px solid #bfdbfe;
    border-left: 3.5px solid #1d4ed8;
    border-radius: 6px;
    padding: 8px 11px;
    font-size: 8.1pt;
    color: #1e3a8a;
    line-height: 1.4;
    margin-bottom: 8px;
  }}

  /* Suite Map Strip */
  .suite-table {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 5px;
    margin: 2px 0 0 0;
  }}
  .suite-cell {{
    width: 25%;
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 6px 7px;
    vertical-align: top;
  }}
  .suite-mod {{
    font-size: 7pt;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 2px;
  }}
  .suite-name {{
    font-size: 8.2pt;
    font-weight: 700;
    color: #0f172a;
    margin-bottom: 2px;
  }}
  .suite-page {{
    font-size: 7.4pt;
    color: #64748b;
  }}

  /* Module Pages (2-5) */
  .module-header {{
    padding: 2px 0 2px 11px;
    margin-bottom: 8px;
  }}
  .module-pill {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 7.2pt;
    font-weight: 800;
    letter-spacing: 0.06em;
    margin-bottom: 3px;
  }}
  .module-title {{
    margin: 2px 0 2px 0;
    font-size: 13.5pt;
    font-weight: 800;
    color: #0f172a;
    line-height: 1.2;
  }}
  .module-sub {{
    margin: 0;
    font-size: 8.4pt;
    color: #475569;
  }}

  .steps-table {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 5px;
    margin: 0 -5px 7px -5px;
  }}
  .step-cell {{
    width: 25%;
    vertical-align: top;
  }}
  .step-card {{
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 6px 7px;
    min-height: 62px;
  }}
  .step-num {{
    font-size: 6.8pt;
    font-weight: 800;
    letter-spacing: 0.08em;
    margin-bottom: 2px;
  }}
  .step-title {{
    font-size: 8pt;
    font-weight: 700;
    color: #0f172a;
    margin-bottom: 2px;
    line-height: 1.2;
  }}
  .step-desc {{
    font-size: 7.3pt;
    color: #475569;
    line-height: 1.3;
  }}

  .shot-card {{
    border: 1px solid #cbd5e1;
    border-radius: 8px;
    background: #ffffff;
    overflow: hidden;
    margin-bottom: 9px;
  }}
  .shot-frame {{
    background: #f1f5f9;
    text-align: center;
    padding: 4px;
    border-bottom: 1px solid #e2e8f0;
  }}
  .shot-frame img {{
    display: block;
    margin: 0 auto;
    height: 78mm;
    width: auto;
    max-width: 100%;
    border-radius: 4px;
    border: 1px solid #cbd5e1;
  }}
  .shot-caption {{
    padding: 5px 10px;
    font-size: 7.8pt;
    color: #334155;
    line-height: 1.35;
    background: #f8fafc;
  }}
  .shot-tag {{
    display: inline-block;
    padding: 1.5px 7px;
    border-radius: 999px;
    font-size: 7pt;
    font-weight: 700;
    margin-right: 6px;
  }}

  /* Page 6 Security & Support */
  .security-card {{
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    background: #f8fafc;
    padding: 10px 12px;
    margin-bottom: 9px;
  }}
  .security-card h3 {{
    margin: 0 0 4px 0;
    font-size: 9.5pt;
    color: #0f172a;
  }}
  .security-card ul {{
    margin: 3px 0 0 0;
    padding-left: 16px;
  }}
  .security-card li {{
    margin-bottom: 4px;
    font-size: 8.3pt;
    color: #334155;
    line-height: 1.4;
  }}
  .support-banner {{
    background: #0f172a;
    color: #ffffff;
    border-radius: 9px;
    padding: 14px 18px;
    margin-top: 10px;
    border-left: 5px solid #44aa00;
  }}
  .support-banner h3 {{
    margin: 0 0 4px 0;
    font-size: 11pt;
    color: #ffffff;
  }}
  .support-banner p {{
    margin: 0 0 6px 0;
    font-size: 8.4pt;
    color: #cbd5e1;
  }}
  .support-pill {{
    display: inline-block;
    background: rgba(255, 255, 255, 0.10);
    border: 1px solid rgba(255, 255, 255, 0.22);
    border-radius: 999px;
    padding: 4px 11px;
    font-size: 8.2pt;
    font-weight: 600;
    color: #ffffff;
    margin-right: 8px;
  }}
</style>
</head>
<body>

<!-- PAGE 1: WELCOME, ROLES & DAY-1 SETUP -->
<div class="hero-banner">
  <table class="hero-top" role="presentation">
    <tr>
      <td style="width:44px;">
        <svg viewBox="15.4 10.4 44.3 32.5" width="36" height="27" xmlns="http://www.w3.org/2000/svg">
          <path d="{GROW_LEAF_PATH}" fill="#44aa00" />
        </svg>
      </td>
      <td>
        <div class="hero-brand-name">Grow</div>
        <div class="hero-brand-sub">Business Development</div>
      </td>
      <td style="text-align:right;">
        <span class="hero-badge">Full-Suite Onboarding Guide</span>
      </td>
    </tr>
  </table>
  <h1 class="hero-title">Grow Corporate Treasury Platform — Quick Start Guide</h1>
  <p class="hero-desc">
    Your complete orientation across all four treasury modules: <strong>LG Custody</strong>,
    <strong>Outbound LG Issuance</strong>, <strong>FX &amp; T-Bill Quotations</strong>, and
    <strong>Bank Auto-Reconciliation</strong>.
  </p>
</div>

<div class="sec-heading">1. Platform Roles &amp; Dual-Control Governance</div>
<table class="grid-table" role="presentation">
  <tr>
    <td>
      <div class="info-card">
        <div class="info-card-title">Corporate Admin <span class="role-chip" style="background:#eef2ff;color:#3730a3;">Governance</span></div>
        <p>Configures legal entities, users, SMTP/Exchange mail relay, bank facilities, GL export profiles, and approves critical workflow actions.</p>
      </div>
    </td>
    <td>
      <div class="info-card">
        <div class="info-card-title">End User (Maker) <span class="role-chip" style="background:#eff6ff;color:#1d4ed8;">Daily Ops</span></div>
        <p>Scans &amp; manages LGs, prepares bank issuance packages, builds FX/T-Bill RFQs, uploads bank statements, and initiates lifecycle actions.</p>
      </div>
    </td>
  </tr>
  <tr>
    <td>
      <div class="info-card">
        <div class="info-card-title">Checker (Approver) <span class="role-chip" style="background:#ecfdf5;color:#047857;">Sign-Off</span></div>
        <p>Reviews and approves or returns high-risk actions (releases, liquidations, facility changes, and RFQ release gates) with full diff history.</p>
      </div>
    </td>
    <td>
      <div class="info-card">
        <div class="info-card-title">Requestor, Viewer &amp; Bank Dealer <span class="role-chip" style="background:#f5f3ff;color:#6d28d9;">Scoped</span></div>
        <p>Departments submit LG requests via the Issuance Portal; Viewers hold read-only audit access; Bank Dealers quote via OTP-gated links.</p>
      </div>
    </td>
  </tr>
</table>

<div class="sec-heading">2. Three-Step Day-1 Workspace Setup</div>
<div class="setup-box">
  <table class="setup-row" role="presentation">
    <tr>
      <td style="width:24px; vertical-align:top;"><div class="setup-num">1</div></td>
      <td class="setup-text">
        <strong>Activate Your Confidential Password:</strong> Open the single-use activation link in your onboarding email. For institutional security (<em>Host-Blind Architecture</em>), Grow staff and Corporate Admins never see user passwords.
      </td>
    </tr>
  </table>
  <table class="setup-row" role="presentation">
    <tr>
      <td style="width:24px; vertical-align:top;"><div class="setup-num">2</div></td>
      <td class="setup-text">
        <strong>Set Up Your Team (Manage Users):</strong> Commercial Corporate Admins should click <strong>Manage Users &rarr; Add New User</strong> to invite at least one <strong>End User (Maker)</strong> and one <strong>Checker</strong> so Maker-Checker separation operates smoothly.
      </td>
    </tr>
  </table>
  <table class="setup-row" role="presentation">
    <tr>
      <td style="width:24px; vertical-align:top;"><div class="setup-num">3</div></td>
      <td class="setup-text">
        <strong>Connect Corporate Email (Module Settings &rarr; Manage Email Settings):</strong> Configure your corporate SMTP or Microsoft Exchange EWS server so LG reminders and live FX/T-Bill RFQs dispatch directly from your organization's domain.
      </td>
    </tr>
  </table>
</div>

<div class="dual-mode-note">
  <strong>Evaluating in a Guided Free Trial?</strong> Your 14-day clock begins only upon first login. Your workspace arrives pre-seeded with sample LGs, 3 credit facilities, FX/T-Bill desks, and sample statements — plus a <strong>1-click Role Switcher (Maker &harr; Checker &harr; Corporate Admin)</strong> in the header and up to 3 colleague invites. External bank emails are suppressed and trial PDFs carry evaluation watermarks.
</div>

<div class="sec-heading">3. Full-Suite Module Map</div>
<table class="suite-table" role="presentation">
  <tr>
    <td class="suite-cell" style="border-top:2.5px solid #1d4ed8;">
      <div class="suite-mod" style="color:#1d4ed8;">Module 1</div>
      <div class="suite-name">LG Custody</div>
      <div class="suite-page">AI OCR, Wording Shield, QR Bank Letters &amp; Action Center &rarr; <strong>Page 2</strong></div>
    </td>
    <td class="suite-cell" style="border-top:2.5px solid #047857;">
      <div class="suite-mod" style="color:#047857;">Module 2</div>
      <div class="suite-name">LG Issuance</div>
      <div class="suite-page">Dept Requests, Facility Scoring &amp; Auto-Fill Forms &rarr; <strong>Page 3</strong></div>
    </td>
    <td class="suite-cell" style="border-top:2.5px solid #6d28d9;">
      <div class="suite-mod" style="color:#6d28d9;">Module 3</div>
      <div class="suite-name">FX &amp; T-Bills</div>
      <div class="suite-page">Blind Multi-Bank Bidding, OTP Portal &amp; Deal Receipts &rarr; <strong>Page 4</strong></div>
    </td>
    <td class="suite-cell" style="border-top:2.5px solid #0f766e;">
      <div class="suite-mod" style="color:#0f766e;">Module 4</div>
      <div class="suite-name">Reconciliation</div>
      <div class="suite-page">Sweeps, 4-Pass ERP Match, AI Rules &amp; GL Export &rarr; <strong>Page 5</strong></div>
    </td>
  </tr>
</table>

{page2}
{page3}
{page4}
{page5}

<!-- PAGE 6: AI COPILOT, INSTITUTIONAL SECURITY & SUPPORT -->
<div class="page-break"></div>
<div class="module-header" style="border-left: 4px solid #0f172a;">
  <span class="module-pill" style="background:#f1f5f9; color:#0f172a;">PLATFORM INTELLIGENCE &amp; SECURITY</span>
  <h2 class="module-title">AI Treasury Copilot, Security Architecture &amp; Support</h2>
  <p class="module-sub">Built-in guidance, tenant-isolated cryptography, and human-in-the-loop accountability.</p>
</div>

<div class="security-card">
  <h3>In-App AI Treasury Assistant (L4 System Guide &amp; Portfolio Copilot)</h3>
  <ul>
    <li><strong>System Guide Mode:</strong> Click the <strong>AI Assistant</strong> button in the bottom-right corner anytime for instant step-by-step navigation help tailored to your current role and active module.</li>
    <li><strong>Portfolio Queries &amp; Risk Explanations:</strong> Ask questions about expiring guarantees, facility utilization headroom, bank quotation spreads, or unmatched statement lines — grounded directly in your tenant's live records.</li>
    <li><strong>Advisory Decision Support:</strong> AI OCR extractions, wording risk alerts, facility scores, and statement matches are preparation aids; your authorized treasury and accounting team retains final sign-off prior to bank execution or GL posting.</li>
  </ul>
</div>

<div class="security-card">
  <h3>Institutional Security &amp; Zero-Knowledge Confidentiality</h3>
  <ul>
    <li><strong>Per-Tenant AES-256 Key Envelopes:</strong> Sensitive corporate documents, counterparty registries, and FX/T-Bill winning rates are encrypted using isolated tenant data-encryption keys (DEKs).</li>
    <li><strong>Zero Public AI Training:</strong> Uploaded guarantees, contracts, and bank statements are never sold or used to train public third-party AI models.</li>
    <li><strong>Cryptographic Audit Chain &amp; QR Verification:</strong> Every approval, RFQ award, and instruction carries a sequential SHA-256 audit log entry; printed bank letters include a tamper-evident QR code + 4-digit security code gate.</li>
    <li><strong>Bank Counterparty Authentication:</strong> External dealers access live tenders strictly through single-use tokenized links protected by 6-digit OTP verification (no shared passwords).</li>
  </ul>
</div>

<div class="security-card">
  <h3>Quick Operational Checklist for Best Results</h3>
  <ul>
    <li><strong>LG Custody &amp; Issuance:</strong> Run the <em>Beneficiary Protection Check</em> on incoming vendor LGs before acceptance, and always log delivery proofs in the <em>Action Center</em> so bank SLA escalation timers stay accurate.</li>
    <li><strong>FX &amp; T-Bill Quotations:</strong> Verify your <em>Bank Dealer Roster</em> and corporate SMTP settings before launching an Execution tender; assign a colleague delegation backup if an acceptance countdown timer is active.</li>
    <li><strong>Bank Reconciliation:</strong> Check statement continuity alerts immediately after upload, confirm 1-click self-learned rules after reviewing the sample lines, and preview balanced debit/credit rows before locking an ERP/GL export.</li>
  </ul>
</div>

<div class="support-banner">
  <h3>Need a Walkthrough or Production Setup Session?</h3>
  <p>Our treasury specialists are ready to help with entity structuring, corporate SMTP/Exchange integration, bank form mapping, or custom ERP journal templates.</p>
  <span class="support-pill">Email: support@growbusinessdevelopment.com</span>
  <span class="support-pill">Web: www.growbusinessdevelopment.com</span>
</div>

</body>
</html>
"""


def main() -> None:
    html_str = build_guide_html()
    OUT_PDF.parent.mkdir(parents=True, exist_ok=True)
    pdf_bytes = HTML(string=html_str).write_pdf()
    OUT_PDF.write_bytes(pdf_bytes)
    print(f"Generated {OUT_PDF} ({len(pdf_bytes):,} bytes)")


if __name__ == "__main__":
    main()
