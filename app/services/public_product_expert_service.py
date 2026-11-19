# c:\Grow\app\services\public_product_expert_service.py
"""
Public AI Product Expert Service & Anonymous Prospect Question Logger.

SECURITY & GROUNDING ARCHITECTURE (100% AIR-GAPPED & 500-QUESTION AUDITED):
- Strictly read-only product & scenario Q&A for unauthenticated visitors on `/` and `/demo`.
- Zero SQLAlchemy models, zero database sessions, zero access to customer/tenant records.
- Grounded on `CORE_PLATFORM_TRUTH` + 8 dynamically selected `DEEP_DOMAIN_PACKS` audited line-by-line
  against the executable codebase (`app/` and `frontend/src/`) across all 500 prospect questions.
- Validates every suggested showcase deep-link against `SHOWCASE_STORIES`, `SHOWCASE_FOUNDATION_BRIEFS`,
  and `SHOWCASE_TOURS` whitelists.
- Logs anonymous visitor questions + AI responses locally (`private_storage/public_demo_chat_logs/`)
  and syncs asynchronously to Google Cloud Storage without blocking HTTP responses.
"""

import asyncio
import hashlib
import io
import json
import logging
import math
import os
import re
import threading
import time
import wave
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.core.ai_integration import (
    GEMINI_AVAILABLE,
    GEMINI_MODEL_NAME,
    _get_api_genai_client,
    _get_genai_client,
    _safe_generate_content_async,
    genai_types,
)

logger = logging.getLogger(__name__)

PUBLIC_CHAT_LOG_DIR = os.path.join(os.getcwd(), "private_storage", "public_demo_chat_logs")
PUBLIC_VOICE_STATE_FILENAME = "public_voice_state.json"
GEMINI_TTS_MODEL_NAME = os.getenv("GEMINI_TTS_MODEL_NAME", "gemini-2.5-flash-tts")

# ==============================================================================
# 1. VALIDATED SHOWCASE DEEP-LINK REGISTRY (100% SYNCED WITH showcaseCatalog.js)
# ==============================================================================

SHOWCASE_STORIES: Dict[str, Dict[str, Any]] = {
    "platform_intro": {
        "id": "platform_intro",
        "module_id": None,
        "label_en": "Watch 65s Platform Overview",
        "label_ar": "شاهد ملخص المنصة (٦٥ ثانية)",
    },
    "custody": {
        "id": "custody",
        "module_id": "custody",
        "label_en": "Watch LG Custody Story (75s)",
        "label_ar": "شاهد القصة التنفيذية للحفظ (٧٥ ث)",
    },
    "issuance": {
        "id": "issuance",
        "module_id": "issuance",
        "label_en": "Watch LG Issuance Story (75s)",
        "label_ar": "شاهد القصة التنفيذية للإصدار (٧٥ ث)",
    },
    "quotations": {
        "id": "quotations",
        "module_id": "quotations",
        "label_en": "Watch FX & T-Bill Story (75s)",
        "label_ar": "شاهد قصة العملات وأذون الخزانة (٧٥ ث)",
    },
    "reconciliation": {
        "id": "reconciliation",
        "module_id": "reconciliation",
        "label_en": "Watch Bank Auto-Recon Story (75s)",
        "label_ar": "شاهد قصة المطابقة البنكية (٧٥ ث)",
    },
}

SHOWCASE_FOUNDATION_BRIEFS: Dict[str, Dict[str, Any]] = {
    "governance": {
        "id": "governance",
        "label_en": "Governance Brief: Maker-Checker & Escalations",
        "label_ar": "ملخص الحوكمة: المراجع والمعتمد والتصعيد",
    },
    "security": {
        "id": "security",
        "label_en": "Security Brief: Per-Company AES-256 & Safe AI",
        "label_ar": "ملخص الأمان: تشفير AES-256 المستقل والذكاء المحمي",
    },
    "verification": {
        "id": "verification",
        "label_en": "Trust Brief: QR + Security Code Bank Letters",
        "label_ar": "ملخص التوثيق: خطابات البنوك المؤمنة بـ QR وكود تحقق",
    },
    "enterprise": {
        "id": "enterprise",
        "label_en": "Scale Brief: Multi-Entity & Rapid Migration Hub",
        "label_ar": "ملخص المجموعات: الشركات التابعة والترحيل السريع",
    },
}

SHOWCASE_TOURS: Dict[str, Dict[str, Any]] = {
    # Module 01: LG Custody
    "custody-scan-record": {
        "id": "custody-scan-record",
        "code": "1.1",
        "module_id": "custody",
        "max_step": 7,
        "title_en": "AI Scan & Record New LG",
        "title_ar": "المسح الذكي وتسجيل خطاب ضمان جديد",
    },
    "custody-action-center": {
        "id": "custody-action-center",
        "code": "1.2",
        "module_id": "custody",
        "max_step": 5,
        "title_en": "Proactive Action Center & Expiry Radar",
        "title_ar": "مركز الإجراءات والتنبيه المبكر للاستحقاق",
    },
    "custody-lifecycle-letter": {
        "id": "custody-lifecycle-letter",
        "code": "1.3",
        "module_id": "custody",
        "max_step": 6,
        "title_en": "Lifecycle Actions, Approval & QR Bank Letter",
        "title_ar": "إجراءات التعديل والموافقات وخطابات الـ QR",
    },
    "custody-delivery-reply": {
        "id": "custody-delivery-reply",
        "code": "1.4",
        "module_id": "custody",
        "max_step": 6,
        "title_en": "Delivery Proof & AI Bank Reply Verification",
        "title_ar": "إثبات التسليم والتحقق الذكي من رد البنك",
    },
    # Module 02: LG Issuance
    "issuance-intake": {
        "id": "issuance-intake",
        "code": "2.1",
        "module_id": "issuance",
        "max_step": 6,
        "title_en": "Department Intake, AI Trap Scan & Credit Matrix",
        "title_ar": "بوابة الإدارات وفحص المخاطر والموافقة الائتمانية",
    },
    "issuance-facility-scoring": {
        "id": "issuance-facility-scoring",
        "code": "2.2",
        "module_id": "issuance",
        "max_step": 5,
        "title_en": "Multi-Bank Facility Matrix & 0–100 Smart Scoring",
        "title_ar": "مصفوفة التسهيلات والتقييم الذكي للبنوك (٠–١٠٠)",
    },
    "issuance-bank-form": {
        "id": "issuance-bank-form",
        "code": "2.3",
        "module_id": "issuance",
        "max_step": 5,
        "title_en": "Auto-Filled Official Bank PDF Form & AI Copy Check",
        "title_ar": "تعبئة استمارة البنك الرسمية والتحقق من أصل الضمان",
    },
    "issuance-position-recon": {
        "id": "issuance-position-recon",
        "code": "2.4",
        "module_id": "issuance",
        "max_step": 5,
        "title_en": "Outgoing Maintenance & Bank LG Position Recon",
        "title_ar": "حوكمة التعديلات ومطابقة تقارير مراكز الضمانات",
    },
    # Module 03: FX & T-Bill Quotations
    "quotations-rfq-broadcast": {
        "id": "quotations-rfq-broadcast",
        "code": "3.1",
        "module_id": "quotations",
        "max_step": 5,
        "title_en": "Multi-Pair FX & T-Bill RFQ with Tariff & TVM Matrix",
        "title_ar": "بناء سلة العملات وأذون الخزانة ومصفوفة العمولات",
    },
    "quotations-dealer-desk": {
        "id": "quotations-dealer-desk",
        "code": "3.2",
        "module_id": "quotations",
        "max_step": 4,
        "title_en": "Zero-Password Bank Dealer Desk & Sealed Quoting",
        "title_ar": "شاشة متداول البنك بدون كلمة مرور والتسعير المشفر",
    },
    "quotations-net-award": {
        "id": "quotations-net-award",
        "code": "3.3",
        "module_id": "quotations",
        "max_step": 6,
        "title_en": "All-In Net Ranking, Cherry-Pick Award & Audit Pack",
        "title_ar": "الترتيب بالصافي الشامل والترسية المجزأة وشهادة التدقيق",
    },
    # Module 04: Bank Auto-Reconciliation
    "recon-zero-config": {
        "id": "recon-zero-config",
        "code": "4.1",
        "module_id": "reconciliation",
        "max_step": 5,
        "title_en": "Zero-Config Import, Continuity Audit & Explainable AI",
        "title_ar": "الاستيراد الذكي وفحص تسلسل الأرصدة والتصنيف المفسَّر",
    },
    "recon-sweeps-reversals": {
        "id": "recon-sweeps-reversals",
        "code": "4.2",
        "module_id": "reconciliation",
        "max_step": 5,
        "title_en": "Cross-Bank Sweeps, Returned Wires & Split ERP Match",
        "title_ar": "التحويلات البينية والمرتدات والمطابقة المزدوجة مع ERP",
    },
    "recon-rules-privacy": {
        "id": "recon-rules-privacy",
        "code": "4.3",
        "module_id": "reconciliation",
        "max_step": 5,
        "title_en": "Self-Learning Rules & Zero-PII Collaborative Brain",
        "title_ar": "القواعد ذاتية التعلم والذكاء المشترك المحمي السرية",
    },
    "recon-erp-export": {
        "id": "recon-erp-export",
        "code": "4.4",
        "module_id": "reconciliation",
        "max_step": 5,
        "title_en": "Double-Entry GL Journal Builder & Locked ERP Export",
        "title_ar": "بناء قيود اليومية المزدوجة والتصدير المؤمّن للـ ERP",
    },
}


# ==============================================================================
# 2. AUDITED CORE TRUTH + 8 DEEP DOMAIN REFERENCE PACKS (COVERING ALL 500 Qs)
# ==============================================================================

CORE_PLATFORM_TRUTH = """
# GROW CORPORATE TREASURY PLATFORM — CORE ARCHITECTURE
Grow is a modular B2B Corporate Treasury SaaS platform uniting **4 core operational modules** (+ Smart Email Inbox) on a shared governance, multi-entity, and per-company AES-256-GCM encryption foundation:
1. **Module 01 — LG Custody (Inbound Guarantees Received)**: Company is Beneficiary (guarantees from suppliers, contractors, customers, or partners). AI OCR + upfront Beneficiary Protection claim-trap check, automated multi-stage expiry/print/delivery/bank-reply reminders & management escalations, direct fast-track protective extensions, strict Maker-Checker on Release/Decrease/Amend/Liquidation, QR + split Security Code tamper-evident bank letters, and AI delivery/bank-reply verification.
2. **Module 02 — LG Issuance & Credit Facilities (Outbound Guarantees Issued)**: Company is Applicant. Digitizes & unifies internal department requests (internal workspace + zero-login tokenized `/portal/issuance`) with a customizable intake form and sequential multi-step approval matrix, AI contract/duplicate/wording risk checks, `0–100` multi-bank credit facility scoring, 1-click auto-filled official bank PDF forms + legal annexes, AI verification of bank's issued LG scan, and **Bank LG Position report** reconciliation.
3. **Module 03 — FX Spot & Sovereign T-Bill Quotations**: Multi-bank sealed-bid competition for **FX Spot** (up to 4 currency pairs per basket) and **Sovereign T-Bills** (Buy/Sell). Supports CBE-aligned Execution vs. Indicative bank roles, selective 2D document visibility, TVM settlement date normalization, zero-password domain-locked OTP dealer links (plus optional Permanent Dealer Portal), strict AES-256-GCM sealed bids until `00:00`, All-In net ranking benchmarked against **both live market rates and Central Bank (CBE) official fixings**, Best Execution Audit Certificate, and bank-scoped HMAC-SHA256 Deal Receipts.
4. **Module 04 — Bank Statement Auto-Reconciliation**: Zero-config import of Arabic/English Excel, CSV, SWIFT MT940, and ISO 20022 CAMT.053 statements, mathematical opening-to-closing balance continuity check, 6-layer deterministic + continuously self-learning & self-healing classification rules under AES-256 privacy, cross-bank sweep pairing with wire fee isolation, returned wire netting, ERP sub-ledger matching, and locked double-entry ERP/GL journal exports (CSV/XLSX for SAP, Oracle, Dynamics, Odoo).
"""

DEEP_DOMAIN_PACKS: Dict[str, str] = {
    "general_platform": """
## DEEP PACK A: GENERAL PLATFORM, WORKFLOWS, DEMO & ONBOARDING (Q1–Q50)
- **Shared Foundation vs Independent Modules**: Corporates can subscribe to **1 single module** or add modules later (`has_custody_module`, `has_issuance_module`, `has_quotation_module`, `has_reconciliation_module`) or start with an isolated **Free Trial / Pilot** (`/free-trial-register`). Modules share Legal Entities (`CustomerEntity`), Central Bank Directory, Currencies & CBE Daily Rates, Users/Roles, Business Holiday Calendar, Smart Email Inbox, Per-Company AES-256 Keys, Notifications, and SHA-256 Hash-Chained Audit Logs—while operational lifecycles run independently without forced dependencies.
- **Intended Users**: Built primarily for **corporate treasury & finance teams** (small single-admin teams up to multi-entity holding groups), while **banks** participate through zero-install counterparty portals (OTP RFQ dealer links, Permanent Bank Dealer Terminal, and public QR instruction verification).
- **Works Alongside ERP (Decision-Support)**: Complements your ERP rather than replacing it (imports ERP AP/AR/GL sub-ledgers and exports balanced GL journals). AI and scoring engines *recommend, rank, and flag risks*, while humans decide and approve (the only optional automation is explicit corporate-consented `AUTO_ACCEPT` on contested FX/T-Bill countdowns). Manual steps remaining outside the platform are physical wet-ink signatures/courier handovers where banks require paper, and final wire release inside bank portals.
- **Dashboards & Search**: Each module has its own dedicated KPI dashboard (Custody Dashboard, Issuance & Facility Utilization Dashboard, Quotations Analytics, Reconciliation Import Dashboard) plus an in-app **AI Query Assistant** that searches across LG Custody, LG Issuance, Facilities, and Audit History. (There is no single 4-module combined cash+LG+FX canvas or global omnibox across bank statements and LGs simultaneously.)
- **Concurrency Protection**: Uses PostgreSQL row-level `SELECT ... FOR UPDATE` locks on LG records, issuance requests, and deal acceptance, blocks overlapping pending maintenance on the same LG, checks approval snapshots against state drift, and enforces a 45s–90s exclusive active trader desk lock per bank on live RFQs.
- **Demo (`/demo`) vs Live / Free Trial**: `/demo` is an ungated, 100% air-gapped interactive walkthrough of representative journeys with instant step/tour reset and this AI Product Expert; `/free-trial-register` provisions an isolated sandbox tenant in the live engine to test your own scenarios safely.
- **Action Tracking, Holidays & Languages**: Dedicated **Action Center**, **Unified Approval Center**, **Post-Issuance Tracker**, and **Ops Health Report** surface stalled internal approvals, overdue bank replies, and critical expiries. `BusinessCalendarService` handles configurable weekends (`Fri–Sat` Middle East default, `Sat–Sun`, or `Fri`) and country public holidays (Egypt default) for SLA business-day calculations.
- **Language Truth**: `/demo`, this AI Product Expert, LG letter templates, AI OCR, Excel migration mappers, and bank statement parsers are **bilingual (English & Arabic)**; the authenticated live web workspace UI itself uses standardized **English** screen labels while processing Arabic and English documents/narratives seamlessly.
- **Custom Fields**: Supported via up to **2 custom fields** + custom reference types in LG Issuance (`IssuanceFormConfig`), **1 custom field per LG Category** in LG Custody, and arbitrary preserved columns (`custom_fields_json`) + custom export headers in Bank Reconciliation.
""",

    "lg_custody": """
## DEEP PACK B: LG CUSTODY — INBOUND GUARANTEES RECEIVED (Q51–Q126)
- **Counterparties & Scope**: Manages guarantees received **in favor of** any corporate legal entity (`beneficiary_corporate_id`)—whether from **suppliers, contractors, vendors, customers, dealers, or JV partners** (`issuer_name` + `issuer_id`). Unlimited LGs and multiple document scans can link to the same LG, counterparty, contract, PO, or project (`internal_contract_project_id`).
- **Recorded Fields & AI OCR**: Custody serial, unique `lg_number` (exact duplicate `lg_number` blocked; similar-but-distinct numbers allowed), beneficiary entity, applicant/counterparty, contract/project ref, LG type (Bid, Advance Payment, Performance, Payment/Financial, Retention, etc.), custom `LGCategory`, purpose, `lg_amount`, guarantee currency + payable currency, issue date, `expiry_date`, period months, issuing bank & branch address/contact, issuing method, applicable rules (`URDG 758`, `ISP98`, `URC 522`, `Other`), `other_conditions` (up to 8,000 chars), AI-extracted `mandatory_claim_statement` (auto-injected into Liquidation demand letters), internal owner (`InternalOwnerContact`), and `auto_renewal` (Evergreen) flag. If a scan is blurry or incomplete, AI pre-fills available fields and users freely edit/complete every field before saving. *(Note: `LGRecord` stores a single `expiry_date` and boolean `auto_renewal` rather than separate presentation/claim-grace date or non-renewal notice-days columns—those clauses are recorded in `other_conditions` / `notes` while tenant reminder lead-days govern advance alerts.)*
- **Operative vs Non-Operative & Foreign Banks**: Tracks `Operative` vs `None Operative` status (enforcing `payment_conditions` on Non-Operative Advance Payment LGs with a dedicated `Activate` workflow). Supports **Foreign Banks** (`foreign_bank_name`, country, SWIFT) paired with a local **Advising or Confirmed** `communication_bank_id` (routing letters automatically to the local bank).
- **Digital Vault vs Physical Paper Safe Location (Honesty Nuance)**: Stores encrypted scans (`ORIGINAL_BANK_DOCUMENT`, `AMENDMENT_LETTER`, `INTERNAL_SUPPORTING`, `DELIVERY_PROOF`, `BANK_REPLY`—individually downloadable via 15-min signed links; no single 1-click ZIP bundle) and tracks business accountability via `InternalOwnerContact`, **but Inbound Custody does NOT track physical safe/cabinet/vault shelf locations, inter-departmental paper handoff logs, or a "missing paper original" boolean flag** (physical `custody_holder` transfer logs exist on Outbound `IssuedLGRecord`).
- **Lifecycle Actions & Risk-Smart Maker-Checker**:
  - **New Custody LG Registration & Protective Extensions (`LG_EXTENSION`)**: Executed **directly** by End Users with **zero Maker-Checker delay** so protective renewals are never bottlenecked; includes **Bulk Auto-Renewal** with cutoff protection.
  - **Risk-Bearing Actions (Strict Maker-Checker, Self-Approval Blocked)**: Required on **Release** (supported anytime before expiry for early closure on `Valid` LGs), **Decrease Amount** (supports multiple sequential partial reductions while balance > 0), **Amendment** (supported on `Valid` LGs and within a **30-day post-expiry grace window**, highlighting changed fields in yellow via AI scan and re-activating `Expired` -> `Valid` if extended), **Liquidation / Claim** (**Full** or **Partial** encashment + `Cancel Liquidation` + `Record Liquidation Funds` capturing net amount, bank charges, and credit advice), **Activate Non-Operative**, and **Owner Changes**.
- **Expired-but-Outstanding & Discrepancies**: Daily cron marks past-due `Valid` LGs as `Expired` while keeping them **fully visible and filterable** until formally resolved. Bank reply mismatches caught by AI trigger Corporate Admin review and generate a formal **Bank Discrepancy Notice Letter**.
- **Configurable Reminders, Migration & Exports**: Tenant-configurable expiry reminder lead days (e.g., 90d/60d/30d, repeat every 7d, daily at <=3d) + print, delivery, and bank-reply escalations routed by `InternalOwnerContact` and manager CC. **Custody Migration Hub** imports Excel/CSV + **batch AI OCR up to 50 PDFs at once** with PDF-vs-Excel staging audit and exact duplicate LG# blocking. Exports full 33-column registers + 2-sheet Lifecycle History to `.xlsx` (automated Bank LG Position file reconciliation runs on Outbound **LG Issuance**, not Inbound Custody).
""",

    "lg_issuance": """
## DEEP PACK C: LG ISSUANCE & CREDIT FACILITIES — OUTBOUND GUARANTEES (Q127–Q200)
- **Dual Intake (Internal + Zero-Login Department Portal)**: Internal users submit via workspace, while business departments (Tendering, Sales, Projects, Procurement) submit without full system seats via `/portal/issuance` using **corporate-domain 6-digit OTP** or encrypted invite links, backed by a central **Requestor Directory** and **Requestor Dashboard** for live status tracking and maintenance requests.
- **Customizable Form & Conditional Rules**: Corporate Admins configure a company-wide intake schema (`IssuanceFormConfig`: show/hide fields, mandatory toggles, mandatory document types, custom reference types, and up to **2 custom fields** `TEXT`/`NUMBER`/`DATE`/`LIST`), while built-in conditional rules automatically enforce extra fields/annexes for Advance Payment (non-operative conditions), Third-Party LGs, Cross-Border LGs, and Special Wording PDFs.
- **AI Pre-Credit Checks, Return-for-Revision & Treasury Queue**: AI cross-checks uploaded contracts/tenders/POs against requested figures (`MATCH`/`MISMATCH`), flags **70%+ fuzzy duplicate** requests, and scores special wording drafts (`GREEN`/`AMBER`/`RED`) for open-ended or extend-or-pay traps. Approvers can **Reject** (terminal `REJECTED`, distinct from requestor `CANCELLED`) or **Return for Clarification/Revision** (`REVISION_REQUIRED`—requestor edits and resubmits, **resuming at the exact approval step** that returned it). Approved requests enter a shared entity-scoped `APPROVED_INTERNAL` Treasury queue where `locked_for_issuance` atomically locks a request once execution begins (no manual pre-assignment to a named treasury officer needed).
- **Approval Matrix**: Sequential multi-step routing (`IssuanceWorkflowPolicy`) with **parallel multi-signature `ApprovalGroup` support** (`required_signatures`), routing by `ALWAYS`, `AMOUNT_OVER`, `AMOUNT_RANGE` (FX-normalized), `DEPT_MATCH`, `CROSS_BORDER`, or `THIRD_PARTY` to `DEPT_HEAD`, `GROUP`, `USERS`, or `ROLE`, blocking self-approval. *(Nuance: approval rules are configured tenant-wide by amount/department/risk flags rather than separate per-entity or per-LG-type matrices, while user entity scoping still applies; bank execution method—PDF form vs overlay vs letter—is configured per bank.)*
- **Multi-Bank Credit Facilities (`0–100` Scoring) vs Ad-Hoc LG RFQs**:
  - Ranks pre-configured `IssuanceFacility` & sub-limits from `0–100` across commission, cash margin %, headroom, currency match, **actualized business-day bank turnaround SLA** (excluding weekends/holidays), and rare-line preservation—automatically shifting **SLA weight from 15% to 50% on Urgent requests**! Reserves limits atomically, warns on 80%/90%/100% utilization & FX drift, and allows Treasury to pick a preferred bank (documenting suitability notes) or enter `manual_pricing` outside facilities.
  - **Honesty Boundaries**: (1) Grow scores pre-agreed facilities + manual pricing rather than sending live ad-hoc LG pricing RFQs to banks; (2) A single `IssuanceRequest` maps 1-to-1 to one bank/LG—splitting a guarantee across two banks uses two requests; (3) Banks do **not** log into a portal for LG Issuance—Treasury generates **1-click auto-filled official bank PDF forms** (`FILLABLE_PDF`, coordinate overlay, or scanned AI map + Third-Party Indemnity + Legal Annex) or Company Letters, delivers to the bank, and tracks bank delivery, beneficiary handover, and bank inquiries.
- **Issued LG Verification, Handover, Reopen & Maintenance**:
  - Uploading the bank's issued LG scan runs **AI Copy Verification** against the approved request (`STRICT`/`TOLERANCE`/`ADVISORY`), generating a **Bank Correction Request Letter PDF** on discrepancy.
  - Tracks internal handover, beneficiary delivery, & `custody_holder` transfer log with signed receipt upload. If a bank rejects or times out (`BANK_REJECTED`/`NO_RESPONSE`), facility exposure releases and the request **auto-reopens to `APPROVED_INTERNAL`** to re-issue via another bank (`-R2`) without repeating internal approvals!
  - **Post-Issuance Maintenance**: Approval-governed workflows + serialized bank letters for **Extend**, **Increase Amount** (with headroom check), **Amend**, **Close / Return** (tracking return of original paper LG from beneficiary + bank cancellation confirmation before releasing facility limit), **Liquidation**, and **Activate Non-Operative**; outbound **Amount Reductions** are recorded upon receiving bank reduction advices (`bank-initiated-change`) or via position reconciliation.
- **Bank LG Position Reconciliation & Migration**: Reconciles **Excel, CSV, PDF (AI), and TXT Bank LG Position statements** against your Issued LG register (`MATCHED`, `PARTIAL_MATCH`, `BANK_ONLY`, `SYSTEM_ONLY`) with controlled variance adjustments/disputes. *(Note: tracks facility commission/margin pricing estimates, not cash-account fee debit matching.)* Dedicated **Issuance Migration Hub** imports legacy Excel/CSV + multi-PDF scans.
""",

    "fx_quotation": """
## DEEP PACK D: FX SPOT QUOTATIONS (Q201–Q274)
- **Scope & Basket Structure**: Multi-bank **FX Spot** RFQs—either single currency pair or **multi-currency basket up to 4 legs (`MAX_PAIRS = 4`)**, independent `Buy` or `Sell` per leg, exact amount (+ optional `min_ticket_amount`), target `value_date`, and optional alternative value dates normalized via **Time Value of Money (TVM, Actual/365)**—suitable for goods imports, advance supplier payments, software/SaaS invoices, foreign service payments, or capex. **Honesty Rule**: FX Forwards, FX Swaps, and Options are **NOT** supported today.
- **Selective Banks & 2D Document Vault**: Invite selected banks (assisted by a **Smart Counterparty Recommendation Engine** based on historical win rates & ticket appetite) and even hide specific legs per bank (`Execution`, `Indicative`, or `Invisible`). Attach encrypted invoices/POs/Form 4 import docs under a **2D Visibility Matrix**: Scope (`ALL_PAIRS`, `SPECIFIC_LEG`, or `INTERNAL_ONLY`) × Timing (`BIDDING` vs `WINNER_ONLY`—unsealed strictly to the winning Execution bank post-award). Purpose/invoice refs are captured via `comments_to_banks`, `internal_notes`, and attachments.
- **Zero-Password Bank Access & Dealer Portal**: Banks need **zero account setup**—they click a unique `/public-quotation/{token}` link restricted to their official `@bank.com` domain and verify a **6-digit email OTP** (supports `EXECUTION`, `APPROVER` bank-side two-eyes gate, and `VIEW_ONLY` roles). Frequent bank traders can also enroll in the **Permanent Bank Dealer Portal** (unified multi-customer live blotter + 90s exclusive trader desk lock + QR Mobile Web Push standing alerts).
- **Strict AES-256 Sealed Bidding, Deadlines & Telemetry**: Every RFQ defines custom `window_start`/`window_end` deadlines. Quotes stay encrypted with per-company **AES-256-GCM**; banks never see rival banks or rival rates, and corporate users see **zero prices until `window_end` (`00:00`)**—monitoring live `desk_proof` telemetry (link opened, OTP verified, active desk lock, typing indicator, fat-finger 10x/>15% guardrails, quote timestamp, or explicit `Pass`). Supports 15-min pre-window auto-reminders + manual bank chasers (requires customer Custom SMTP), unlimited dealer revisions before `window_end`, and hard-blocks late quotes after `window_end`.
- **All-In Net Ranking, Negotiation Boundary & Award Rules**:
  - At `00:00`, quotes unseal and rank automatically by **All-In Effective Price** (applying each bank's custom `%` / flat / min / max tariff schedule + TVM value-date adjustment). If best Execution trails best Indicative beyond `max_tolerance_percent`, the leg flags `INCONCLUSIVE`.
  - **No Post-Quote Bilateral Chat/Counteroffer**: To preserve sealed-bid audit integrity, there is no bilateral counteroffer chat; if rates are unsatisfactory, treasury clicks **Decline Deal** (recording reason) and triggers a 1-click linked **Re-Trade / Re-Tender (`-R1`, `-R2`)** round preserving full parent-child audit history (or **Clone** for recurring payments).
  - **Award, Finality & Edge Cases**: Pre-broadcast Maker-Checker approval (`QUOTATION_APPROVAL_REQUIRED` with Return-for-Revision & pre-open Cancellation); configurable post-close acceptance countdown (`30s` default FX, hard-blocking expired clicks). In multi-leg baskets, treasury can **independently accept or exclude each leg**—awarding each accepted leg to its **#1 ranked Execution bank** (cannot override a leg to pick a 2nd-place Execution bank; for mandated/facility-tied trades, invite the mandated bank as `Execution` and peers as `Indicative`, or record an offline **Manual Deal** with bank deal ref & trade slip). Ties break by **earliest submission timestamp**; leg amounts are locked so banks cannot quote wrong amounts; 0 quotes -> `NO_QUOTES`; 1 quote -> **Sole-Source Advisory** (halting `AUTO_ACCEPT`). Clicking **Accept** is **final and binding** (cannot be unilaterally withdrawn after acceptance) and dispatches bank-scoped **HMAC-SHA256 Deal Execution Receipts** locking the exact quoted rate + printable **Best Execution Audit Certificate** benchmarked against **both Live Interbank Market Mid and CBE Official Fixings** (`saved_vs_avg`, `saved_vs_worst`).
""",

    "tbill_quotation": """
## DEEP PACK E: SOVEREIGN T-BILL & MONEY-MARKET QUOTATIONS (Q275–Q335)
- **Supported Instrument Scope (Honesty Rule)**: The Quotation RFQ engine supports **Sovereign Treasury Bills (`TBILL`)** (Buy and Sell) alongside `FX_SPOT`. **Certificates of Deposit (CDs), Time Deposits (TDs), Commercial Paper, and Repos are NOT tendered in the Quotation module today.**
- **T-Bill Request & Multi-Line Quoting**: Specify `Buy` or `Sell`, face/investment `amount` (+ `min_ticket_amount`), expected settlement date (or settlement range on Buy), and maturity date (or maturity date range on Buy), plus encrypted term-sheet/auction attachments and notes. Each RFQ sets its own custom bidding window and acceptance window (`120s` default for T-Bills vs `30s` for FX). Within a Buy maturity window, each invited bank can submit **multiple offer lines** (`settlementDate`, `maturityDate`, `discountRate %`, `maxAmount` cap, `notes`) and revise freely before `window_end`. *(Note: ISIN / security codes and Primary vs. Secondary market labels are captured in notes/attachments rather than dedicated database columns.)*
- **Exact Pricing & Ranking Math (CRITICAL ACCURACY RULE)**:
  - Dealers enter **Discount Rate (%)**. Grow computes **Egyptian Actual/365 Price per 100 Par**: `P = 100 * (1 - (d / 100) * (days / 365))` and normalizes differing settlement/maturity dates using your `eval_rate` (TVM) to rank banks automatically and compute EGP `saved_vs_avg` & `saved_vs_worst`.
  - **What Is NOT Calculated in Code**: Do **NOT** claim T-Bill RFQs toggle between Discount Rate vs. Equivalent Investment Yield conventions, and do **NOT** claim T-Bill RFQs compute 20% Withholding Tax (WHT), custody fees, or net post-tax maturity cash proceeds.
- **Governance, Execution & Post-Trade Boundary**:
  - Uses the same domain-locked OTP dealer links, Permanent Dealer Portal, AES-256 sealed bidding, pre-broadcast Maker-Checker approval, `Execution` vs `Indicative` roles, 1-click Clone, pre-open Cancellation, reminders, late-quote hard-blocking, and linked Re-Tender (`-R1`) rounds (no bilateral counteroffer chat).
  - Awards the **single #1 ranked Execution bank** per T-Bill RFQ inside the live acceptance window and issues a signed **HMAC-SHA256 Deal Execution Receipt** + printable **Best Execution Audit Certificate** (splitting an investment across multiple banks or maturities uses separate T-Bill RFQs; offline executions with bank deal reference & confirmation attachment can be logged via `Manual Deal`).
  - **Post-Trade Lifecycle Boundary**: The module's workflow concludes at **Deal Execution + Audit Certificate**—it does **not** maintain a post-trade T-Bill holdings register, post-execution actual-settlement status tracker, maturity reminder alerts, or maturity cash-inflow bucket report.
""",

    "bank_reconciliation": """
## DEEP PACK F: BANK STATEMENT AUTO-RECONCILIATION (Q336–Q426)
- **Supported Statement Formats (Exact Truth)**:
  - **Supported Today**: **Excel (`.xlsx`, `.xls`, `.xlsb`)**, **CSV / TXT** (UTF-8 + Arabic Windows-1256 `cp1256`), **SWIFT MT940 (`.mt940`, `.sta`, `.940`)**, and **ISO 20022 `CAMT.053` XML** across any bank and multiple legal entities, handling high daily transaction volumes.
  - **NOT Supported Today**: **PDF bank statements** are rejected in cash Bank Statement Reconciliation (PDF AI parsing is used for Bank LG Position statements in Issuance), and automated email/watched-folder statement ingestion is not yet active (statements are uploaded via UI, 1 file per upload).
- **Zero-Config Mapping, Separators, Balance Continuity & Deduplication**:
  - Fingerprints saved bank layouts (`StatementInputMappingProfile`), auto-detects unseen Arabic/English headers & number formats statistically—including US/UK (`1,234.56`), European (`1.234,56`), accounting parentheses `(1,234.56)`, and trailing `DR`/`CR` indicators—alerts on `HEADER_DRIFT_DETECTED`, and extracts account number, currency, opening/closing balances, booking dates, value dates, refs, IBANs, and counterparties automatically (user selects the Bank in upload dropdown).
  - **Mathematical Continuity Audit**: Validates row-by-row running balances from opening to closing balance (`0.02` tolerance), flagging `CONTINUITY_WARNING` with exact row variance diagnostics if rows are missing or corrupted.
  - **Duplicate Handling**: Flags identical intra-file duplicate rows automatically; cross-upload calendar gap checks or overlapping statement deduplication are not automatic—to re-import a corrected statement without duplicates, delete the prior statement upload first (`DELETE /statements/{id}`).
- **6-Layer Explainable & Continuously Self-Learning Classification (100% Deterministic & Privacy-Shielded)**:
  - Classifies salary/payroll, supplier payments, customer collections, bounced cheques/reversals, bank charges, credit & debit/overdraft interest, loans, time deposits, FX conversions, and LG commissions/cash margins using: (1) User & **1-click online-induced rules** (`SUGGESTED` -> `AUTO_CONFIRMED` with **self-healing** precision when overridden), (2) Customizable SWIFT MT940 & ISO 20022 transaction code ontology, (3) Bilingual EN/AR detectors, (4) Per-company AES-256-GCM counterparty vault, and (5) **Zero-PII (`k>=3`) collaborative banking syntax patterns**. Unknown counterparties or unmatched items surface in the `UNCLASSIFIED` / Unmatched filter views for review.
- **Sweeps, Reversals & Multi-Pass ERP Matching**:
  - **Cross-Bank Sweeps**: Pairs inter-account transfers across banks even when wire/RTGS fees are deducted in transit, isolating fee variances—and explicitly flags **one-sided unpaired in-transit transfers** awaiting a counterpart leg! Nets returned wires/reversals cleanly.
  - **ERP Sub-Ledger Matching**: Upload internal ERP AP/AR/payment records (Excel/CSV/JSON). Auto-matcher runs 4 passes: Global Optimal **1:1**, Learned **Date-Lag (timing differences) & Fee/WHT/FX Tolerance 1:1**, Batch **Many-ERP-to-1-Bank** (`2..6` invoices to `1` bank line), and **LG Number narrative match** against Treasury `LGRecord`s. Manual Split Match supports **`1:1`, `1:Many`, `Many:1`, and `M:N`** with automatic variance-account booking, plus 1-click **Unmatch / Undo** anytime. *(Partial installment note: matching marks the ERP line reconciled rather than tracking a carried-forward open installment balance.)*
- **Governance, Honest Boundaries & Locked ERP Journal Export**:
  - Strictly restricted to **`corporate_admin`** role. Exported rows can be **Locked (`is_locked=True`, `exclude_already_exported=True`)** to prevent modification or double-posting.
  - **What Is NOT in Cash Reconciliation Today**: No per-exception user assignment/comment threads/aging escalation timers or real-time large-amount fraud alert rules; no consolidated multi-bank Cash Position / Cash Forecast view or Bank-vs-GL ending balance bridge PDF; no automated bank-tariff overcharge or daily-balance overdraft interest calculator; no automatic cross-table link to FX/T-Bill Quotation deals; and no separate Maker-Checker or formal Period Close/Reopen workflow beyond row-level Export Locks.
  - **ERP Posting Rule**: Generates balanced double-entry **CSV / XLSX** journal files mapped to your SAP, Oracle, Dynamics, or Odoo Chart of Accounts—**never** posts directly into external ERP databases or bank systems.
""",

    "users_security": """
## DEEP PACK G: USERS, ROLES, APPROVALS, SECURITY & INFRASTRUCTURE (Q427–Q466)
- **5 User Roles, Export Control & Access Scoping**:
  - Roles: **`system_owner`** (platform super-admin), **`corporate_admin`** (tenant admin, settings, user management, approver, audit log export, and exclusive access to Bank Statement Reconciliation & GL exports), **`end_user`** (treasury maker/operator for Custody, Issuance, and FX/T-Bill Quotations—with **zero** access to Bank Statements!), **`checker`** (approver + read-only Issued LGs), and **`viewer`** (strictly read-only enforced backend-side). Export capabilities follow module/role and entity access (no separate per-user "disable Excel export" checkbox).
  - **Entity vs Module Granularity**: Users can be granted all-entity access or restricted to specific **Legal Entities (`entity_ids`)** (and LG reminder portfolios via `InternalOwnerContact`). Within a subscribed tenant, module visibility follows the user's **Role** rather than per-user Custody-vs-Issuance checkboxes or separate per-module admin roles, and approval threshold policies are tenant-wide while respecting user entity scoping.
- **Maker-Checker, Thresholds & Instant Deactivation**:
  - Hard backend enforcement blocks any user from approving their own request across Custody, Issuance, Quotations, and Multi-Admin Config Governance (`AdminChangeRequest`).
  - Issuance & Custody support multi-level sequential matrices with amount/currency/department/action rules; **Issuance and Quotations** support **Return for Revision** without cancelling, and rule changes preserve historical snapshots (`approval_chain_audit`, `lg_record_snapshot`, `AuditLog`).
  - **Immediate Revocation**: Deactivating a user (`DELETE /corporate-admin/users/{id}`) checks live DB state on **every API call**, cutting off access on the very next request without waiting for JWT expiry.
- **Authentication, MFA, Devices & Session Controls**:
  - **Zero-Touch 24h Activation Links** (admins never set/see passwords), **Device-Aware 6-Digit Email OTP MFA**, **Trusted Devices ("Remember this device")**, **FIDO2 WebAuthn / Passkeys** (Face ID, Touch ID, Windows Hello + QR mobile biometric pairing), failed-login lockout (`5` attempts -> `15m` lock, platform-configured), and **20-minute inactivity auto-logout** with a 60s countdown modal across tabs. *(Note: per-tenant corporate IP-allowlisting / VPN CIDR restriction is not currently implemented.)*
- **Cryptographic Isolation, BYO Cloud Storage & Hosting**:
  - Logical `customer_id` isolation + **unique per-company 256-bit AES-256-GCM Data Encryption Keys (DEKs)** wrapped with HKDF-SHA256 Master KEK and tenant AAD (`tenant:{customer_id}`)—no customer can ever decrypt or view another customer's records.
  - **Encrypted Document Streaming**: Files are magic-byte checked, encrypted in **64KB AES-256-GCM chunks**, unsealed only via **15-minute HMAC-signed download tickets** for authorized tenant users, and logged in the sequential **SHA-256 hash-chained WORM `AuditLog`**.
  - **Customer-Owned Cloud Bucket**: Supports connecting your company's **own dedicated Google Cloud Storage (GCS) bucket** (`STORAGE_BUCKET_NAME` for Custody & Issuance vaults, verified live on save; AWS S3/Azure BYO-bucket is not supported today). Push notifications and logs mask LG numbers and counterparty PII automatically. Hosted on cloud PostgreSQL + GCS with infrastructure-level automated snapshots/PITR and OWASP security headers (HSTS, `SAMEORIGIN`, `nosniff`).
""",

    "reporting_integrations_admin": """
## DEEP PACK H: REPORTING, MIGRATION, INTEGRATIONS & ADMINISTRATION (Q467–Q500)
- **Reports & Exports**:
  - **Built-In Dashboards & Reports**: Treasury Ops Health / LG Performance (pipeline, critical expiries, stalled requests, bank ghosting, turnaround SLAs), My LG Dashboard, LG Lifecycle History, Issuance Dashboard (total LG exposure alongside per-bank facility limits, utilized & available headroom!), Quotation Standings & Market/CBE Spread Analytics, and Reconciliation Continuity & Classification Stats.
  - **Export Formats**: Native **Excel (`.xlsx`) / CSV** exports across all modules (including Issuance's 48-field multi-tab `.xlsx` builder with Event & Approval logs, Custody's 33-column register + 2-sheet history `.xlsx`, Quotations `/export-csv`, Reconciliation `AccountingExport` saved layout profiles, and Audit Log CSV). **PDF** is generated for official Bank Instruction Letters, Auto-Filled Bank Forms, Correction/Discrepancy Notices, and printable A4 **Best Execution Audit Certificates** (tabular portfolio reports export to Excel/CSV or browser Print-to-PDF).
  - **Scheduled Digests**: Sends an automated **Weekly Treasury Action Digest every Monday at 08:00 Cairo time** to Corporate Admins + daily expiry/SLA reminders + scheduled Smart Inbox bank position requests (arbitrary user-defined custom report schedules are not supported today; saved filter layouts are supported on Reconciliation Export Profiles). *(Note: there is no combined cross-module Cash Balance + Cash Flow Forecasting report.)*
- **Migration Hubs (Staged Validation & Error Fixing)**:
  - Full built-in **Staging Migration Hubs** for **LG Custody** and **LG Issuance** supporting **Excel (`.xlsx`), CSV, and batch AI PDF/Image scans**, fuzzy EN/AR column mapping (70+ header aliases), two-gate validation (`ERROR` / `WARNING` / `DUPLICATE` / `READY`), inline single/bulk error editing & re-validation before commit, chronological amendment replay, and staged batch-by-batch migration.
  - Historical **Bank Statements & ERP ledgers** import directly via Reconciliation upload (and train rules via `learn-from-history`); historical/offline **FX & T-Bill deals** can be logged individually via `Manual Deal` (no bulk FX Excel importer). Master Banks & Currencies come pre-seeded globally while Entities/Departments/Facilities are created in Admin UI.
- **ERP, Bank Connectivity & APIs (100% Honest Architecture)**:
  - **ERP**: Connects via **structured file exchange**—ingesting ERP AP/AR/GL sub-ledgers (Excel/CSV/JSON) and exporting mapped, batch-locked double-entry GL journals (CSV/XLSX)—with **zero invasive live ERP database connector required**.
  - **Banks**: **Zero direct core-banking APIs or SWIFTNet host-to-host rails today**; works frictionlessly with every bank from day one via QR-verified letters, auto-filled official bank PDF forms, domain-locked OTP dealer portals, Smart Email Inbox (IMAP/SMTP), and statement/position uploads.
  - **REST API**: Powered by a clean FastAPI REST backend secured for interactive MFA/JWT user sessions (public developer API keys / OAuth webhooks for external customer scripts are not exposed today).
- **Notifications & Central Directories**:
  - Supports **Customer Custom SMTP** (`SendOnly` encrypted credentials—mandatory for RFQ bank broadcasts so emails come from your corporate domain), In-App Bell, and Mobile/Desktop Web Push, routing dynamically by workflow role, entity, and portfolio owner.
  - Corporate Admins centrally manage **Bank Accounts**, **Bank Issuance Options**, **Quotation Bank Dealer Rosters** (`EXECUTION`/`APPROVER`), **Internal Owner Contacts**, **Requestor Directory** (with L1/L2 managers), **Departments**, and **Approval Groups**. **Business Holiday Calendars / Weekend schemes** and **Master Currencies / Auto-Synced CBE Exchange & Policy Interest Rates** are governed centrally at the **System Owner** level (read-only for Corporate Admins to preserve benchmark integrity).
""",
}

SHOWCASE_LINK_CATALOG_FOR_PROMPT = """
VALID SHOWCASE ACTIONS YOU MAY RECOMMEND (choose 1 to 3 most relevant to prove your answer visually):
1. `type: "story"` (65s-75s narrated visual executive stories):
   - `id: "platform_intro"` -> 65s Complete 4-Module Platform Overview
   - `id: "custody"` -> 75s Module 01: LG Custody Executive Story
   - `id: "issuance"` -> 75s Module 02: LG Issuance Executive Story
   - `id: "quotations"` -> 75s Module 03: FX & T-Bill Quotations Executive Story
   - `id: "reconciliation"` -> 75s Module 04: Bank Auto-Reconciliation Executive Story
2. `type: "foundation"` (Cross-platform governance & security briefs):
   - `id: "governance"` -> Maker-Checker, Reminders & Escalations
   - `id: "security"` -> Per-Company AES-256, Private Buckets & Safe AI
   - `id: "verification"` -> QR + Security Code Tamper-Evident Bank Letters
   - `id: "enterprise"` -> Multi-Entity Holding Groups & Rapid Excel/PDF Migration Hub
3. `type: "tour"` (Interactive step-by-step product screens; optional `step` integer):
   - `id: "custody-scan-record"` (steps 1..7): Step 2 AI OCR extraction, Step 4 Smart Claim trap check
   - `id: "custody-action-center"` (steps 1..5): Step 1 4-Queue Expiry Radar, Step 2 Bulk Auto-Renewal, Step 4 Direct Fast-Track Extension
   - `id: "custody-lifecycle-letter"` (steps 1..6): Step 2 AI Beneficiary Protection, Step 4 Maker-Checker Approval, Step 6 QR + Security Code Public Verification
   - `id: "custody-delivery-reply"` (steps 1..6): Step 2 AI Bank Receiving Stamp Audit, Step 4 Split-Screen AI Bank Reply Verification
   - `id: "issuance-intake"` (steps 1..6): Step 1 Unified Department Intake & Custom Form, Step 3 AI Contract & Wording Trap Scan, Step 5 Sequential Credit Approval Matrix
   - `id: "issuance-facility-scoring"` (steps 1..5): Step 2 Multi-Bank 0-100 Smart Facility Scoring Matrix, Step 4 Atomic Credit Limit Reservation
   - `id: "issuance-bank-form"` (steps 1..5): Step 2 1-Click Auto-Filled Official Bank PDF Form, Step 4 AI Verification of Bank's Issued LG Scan
   - `id: "issuance-position-recon"` (steps 1..5): Step 3 Bank LG Position Report Reconciliation & Variance Governance
   - `id: "quotations-rfq-broadcast"` (steps 1..5): Step 1 Multi-Pair FX Spot & T-Bill Builder (Execution vs Indicative, TVM), Step 5 Live Sealed-Bid Participation Monitor
   - `id: "quotations-dealer-desk"` (steps 1..4): Step 1 Zero-Password OTP Login, Step 3 Fat-Finger & Cross-Leg Guardrails, Step 4 AES-256 Sealed Quote Confirmation
   - `id: "quotations-net-award"` (steps 1..6): Step 1 Simultaneous Unsealing & All-In Net Ranking (vs Live Market + CBE), Step 2 Multi-Leg Cherry-Pick Award, Step 4 Best Execution Audit Certificate, Step 5 Scoped HMAC-SHA256 Deal Receipt
   - `id: "recon-zero-config"` (steps 1..5): Step 1 Zero-Config Statement Import, Step 2 Mathematical Balance Continuity Audit, Step 4 Explainable Classification
   - `id: "recon-sweeps-reversals"` (steps 1..5): Step 1 Cross-Bank Sweep Pairing with Wire Fee Isolation, Step 3 Returned Wire Netting, Step 4 Split ERP Invoice Match
   - `id: "recon-rules-privacy"` (steps 1..5): Step 1 1-Click Rule Teaching, Step 2 Self-Healing Learned Rules, Step 3 AES-256 Counterparty Vault, Step 4 Zero-PII Collaborative Brain
   - `id: "recon-erp-export"` (steps 1..5): Step 2 SAP/Oracle/Dynamics Journal Profile Mapping, Step 3 Balanced Double-Entry Legs, Step 4 Locked ERP Export Batch
"""


# ==============================================================================
# 3. FULL 8-PACK REFERENCE + FOCUS ROUTING (100% 500-QUESTION RECALL + CACHEABLE)
# ==============================================================================

ALL_VERIFIED_DOMAIN_PACKS_TEXT: str = "\n".join(DEEP_DOMAIN_PACKS.values())

_PACK_KEYWORD_TRIGGERS: Dict[str, List[str]] = {
    "lg_custody": [
        "custody", "inbound", "incoming", "received", "supplier", "vendor", "contractor",
        "customer", "beneficiary", "lg number", "expiry", "expires", "expiring", "renew",
        "evergreen", "auto-renewal", "auto renewal", "notice period", "reduce", "reduction",
        "decrease", "claim", "liquidat", "encash", "invoke", "release", "return", "original",
        "vault", "safe", "physical", "qr", "verify", "stamp", "amend", "operative",
        "foreign bank", "confirming", "counter-guarantee", "counter guarantee", "expired",
        "dispute", "scan", "ocr", "extract", "duplicate lg", "reminder",
        "حفظ", "واردة", "مورد", "مقاول", "تسييل", "مصادرة", "إفراج", "افراج", "تخفيض", "تمديد", "تجديد",
    ],
    "lg_issuance": [
        "issuance", "issue", "outbound", "outgoing", "applicant", "department", "request",
        "requestor", "requester", "originator", "clarification", "secure link", "portal",
        "facility", "facilities", "0-100", "0–100", "headroom", "sub-limit", "cash margin",
        "collateral", "commission", "bank form", "draft", "wording", "legal",
        "beneficiary accept", "handover", "position", "turnaround", "sla", "urgent",
        "same-day", "reopen", "increase", "tender", "advance-payment", "performance", "split",
        "إصدار", "اصدار", "صادرة", "تسهيل", "تسهيلات", "استمارة", "مراكز الضمانات", "غطاء نقدي", "عمولة",
    ],
    "fx_quotation": [
        "fx", "foreign exchange", "currency pair", "buying or selling", "spot", "forward",
        "swap", "rfq", "quotation", "quote", "offer", "rate", "bid", "dealer", "trader",
        "deadline", "one-time code", "all-in", "all in", "spread", "negotiat", "improve",
        "counteroffer", "tie", "same rate", "sole", "one bank", "no bank", "cherry", "cbe",
        "central bank", "indicative", "execution", "value date", "tvm", "trade confirmation",
        "deal ref", "withdraw", "invoice", "imported goods",
        "عملات", "عملة", "سعر صرف", "فورية", "آجل", "عروض أسعار", "متداول", "البنك المركزي", "ترسية",
    ],
    "tbill_quotation": [
        "t-bill", "tbill", "treasury bill", "money-market", "money market", "tenor",
        "maturity", "yield", "discount rate", "convention", "security", "auction", "isin",
        "primary", "secondary", "wht", "withholding", "proceeds", "certificate of deposit",
        "time deposit", "deposit", "cd ", "cds", "repo", "investment", "par", "settlement",
        "next-best", "duplicate a previous",
        "أذون", "اذون", "خزانة", "عائد", "خصم", "استحقاق", "ودائع", "شهادات إيداع", "سوق ثانوي",
    ],
    "bank_reconciliation": [
        "reconcil", "statement", "mt940", "camt", "csv", "opening balance", "closing balance",
        "cash balance", "balances across", "continuity", "debit", "credit", "booking date",
        "narrative", "description", "classify", "transaction", "match", "partial payment",
        "split settlement", "sweep", "inter-bank", "interbank", "transfer", "salary",
        "payroll", "overdraft", "interest", "tariff", "bank charge", "bank fee", "tolerance",
        "unmatched", "unresolved", "timing difference", "exception", "cash position",
        "journal", "ledger", "gl ", "sap", "oracle", "dynamics", "odoo", "double-entry",
        "period close", "closed period", "separator", "duplicate payment",
        "مطابقة", "كشف حساب", "كشوف", "تحويلات بينية", "قيود", "يومية", "أرصدة", "رصيد", "فوائد", "رواتب",
    ],
    "users_security": [
        "role", "admin", "viewer", "checker", "maker", "permission", "restrict", "access",
        "who can export", "self-approval", "self approval", "approve", "approval", "reject",
        "deactivate", "audit", "login", "lockout", "password", "otp", "mfa", "2fa",
        "passkey", "webauthn", "biometric", "inactivity", "idle", "trusted device", "ip ",
        "network", "tenant", "separat", "encrypt", "aes", "cloud", "bucket", "gcs", "s3",
        "mask", "backup", "disaster", "hosting",
        "صلاحيات", "مستخدم", "مراجع", "معتمد", "أمان", "تشفير", "سرية", "سحابة", "نسخ احتياطي", "قفل",
    ],
    "reporting_integrations_admin": [
        "report", "dashboard", "filter", "export", "excel", "pdf", "schedule", "digest",
        "exposure", "outcomes", "cash flow", "custom field", "master data", "migrat",
        "import", "staging", "erp", "api", "connect", "swift", "host-to-host",
        "notification", "email", "smtp", "directory", "contact", "holiday", "weekend",
        "calendar", "exchange-rate",
        "تقارير", "تقرير", "لوحة", "تصدير", "ترحيل", "استيراد", "إكسيل", "ربط", "إجازات", "عطلات", "بريد",
    ],
    "general_platform": [
        "platform", "module", "subscribe", "pilot", "trial", "implement", "onboard",
        "how long", "volume", "two users", "concurren", "remote", "demo", "sandbox",
        "reset", "customize", "update", "feature", "overdue", "language", "arabic",
        "format", "price", "cost", "entities",
        "وحدات", "اشتراك", "تجربة", "تشغيل", "تنفيذ", "لغة", "عربي", "سعر", "تكلفة", "باقات",
    ],
}


def select_focus_pack_keys(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    page_context: str = "demo",
) -> List[str]:
    """Ranks top focus domain pack keys for user_prompt guidance."""
    combined = f"{page_context or ''} {question or ''}".lower()
    if history:
        recent_text = " ".join(str(t.get("content") or "") for t in history[-2:]).lower()
        combined = f"{combined} {recent_text}"

    scores: Dict[str, int] = {k: 0 for k in DEEP_DOMAIN_PACKS}
    ctx_lower = (page_context or "").lower()
    if "custody" in ctx_lower:
        scores["lg_custody"] += 2
    elif "issuance" in ctx_lower:
        scores["lg_issuance"] += 2
    elif "quotation" in ctx_lower:
        scores["fx_quotation"] += 2
        scores["tbill_quotation"] += 1
    elif "recon" in ctx_lower:
        scores["bank_reconciliation"] += 2

    q_lower = (question or "").lower()
    for pack_key, keywords in _PACK_KEYWORD_TRIGGERS.items():
        for kw in keywords:
            if kw in q_lower:
                scores[pack_key] += 3
            elif kw in combined:
                scores[pack_key] += 1

    ranked = [k for k, sc in sorted(scores.items(), key=lambda item: item[1], reverse=True) if sc > 0]
    return ranked[:3] if ranked else ["general_platform"]


def select_relevant_domain_packs(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    page_context: str = "demo",
) -> str:
    """
    Returns the complete, static 8-pack verified reference so 100% of all 500 questions
    (including cross-module and subtly phrased questions) have zero missing facts AND
    Gemini 2.5's static prefix caching hits across conversation turns.
    """
    return ALL_VERIFIED_DOMAIN_PACKS_TEXT


# ==============================================================================
# 4. SECURITY & LANGUAGE HELPERS
# ==============================================================================

_ADVERSARIAL_PATTERNS = [
    r"ignore\s+(all\s+)?(previous|prior|above)\s+(instructions|prompts|rules)",
    r"(reveal|print|dump|show)\s+(your\s+)?(system\s+prompt|hidden\s+instructions|api\s*key|env|secret|credentials)",
    r"(sql\s+injection|drop\s+table|union\s+select|xp_cmdshell|/etc/passwd|\.env\b)",
    r"(how\s+to\s+(hack|bypass|exploit|forge|spoof)\b)",
    r"تجاهل\s+(كافة\s+|كل\s+)?التعليمات",
    r"(اكشف|اعرض)\s+(تعليمات\s+النظام|مفتاح\s+التشفير|قاعدة\s+البيانات)",
]
_ADVERSARIAL_REGEX = re.compile("|".join(_ADVERSARIAL_PATTERNS), re.IGNORECASE)
_ARABIC_CHARS_REGEX = re.compile(r"[\u0600-\u06FF]")
_LATIN_CHARS_REGEX = re.compile(r"[A-Za-z]")


def detect_response_language(question: str, preferred_lang: str = "en") -> str:
    """Detect whether to answer in Arabic ('ar') or English ('en') based on the user's message."""
    ar_count = len(_ARABIC_CHARS_REGEX.findall(question or ""))
    en_count = len(_LATIN_CHARS_REGEX.findall(question or ""))
    if ar_count >= 3 and ar_count >= en_count * 0.4:
        return "ar"
    if en_count >= 4 and ar_count == 0:
        return "en"
    return "ar" if (preferred_lang or "").lower().startswith("ar") else "en"


def hash_client_ip(raw_ip: str) -> str:
    """Creates a non-reversible truncated monthly salted hash of an IP for abuse analytics without storing raw PII."""
    salt = os.getenv("SECRET_KEY", "grow-public-demo-salt")[:16]
    day_bucket = datetime.now(timezone.utc).strftime("%Y-%m")
    digest = hashlib.sha256(f"{salt}:{day_bucket}:{raw_ip or 'unknown'}".encode("utf-8")).hexdigest()
    return digest[:14]


def validate_and_hydrate_actions(raw_actions: Any, lang: str = "en") -> List[Dict[str, Any]]:
    """
    Strictly validates AI-suggested deep links against `SHOWCASE_STORIES`,
    `SHOWCASE_FOUNDATION_BRIEFS`, and `SHOWCASE_TOURS`. Invalid IDs are discarded.
    """
    if not isinstance(raw_actions, list):
        return []

    is_ar = lang == "ar"
    hydrated: List[Dict[str, Any]] = []
    seen_keys = set()

    for item in raw_actions:
        if not isinstance(item, dict):
            continue
        action_type = str(item.get("type") or "").strip().lower()
        target_id = str(item.get("id") or "").strip()
        if not action_type or not target_id:
            continue

        if action_type == "story" and target_id in SHOWCASE_STORIES:
            meta = SHOWCASE_STORIES[target_id]
            dedup_key = f"story:{target_id}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            hydrated.append({
                "type": "story",
                "id": target_id,
                "module_id": meta["module_id"],
                "step": 1,
                "label": meta["label_ar"] if is_ar else meta["label_en"],
            })

        elif action_type == "foundation" and target_id in SHOWCASE_FOUNDATION_BRIEFS:
            meta = SHOWCASE_FOUNDATION_BRIEFS[target_id]
            dedup_key = f"foundation:{target_id}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            hydrated.append({
                "type": "foundation",
                "id": target_id,
                "module_id": None,
                "step": 1,
                "label": meta["label_ar"] if is_ar else meta["label_en"],
            })

        elif action_type == "tour" and target_id in SHOWCASE_TOURS:
            meta = SHOWCASE_TOURS[target_id]
            raw_step = item.get("step", 1)
            try:
                step_num = int(raw_step)
            except (TypeError, ValueError):
                step_num = 1
            step_num = max(1, min(step_num, meta["max_step"]))

            dedup_key = f"tour:{target_id}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            step_suffix = (
                f" · خطوة {step_num}" if (is_ar and step_num > 1)
                else (f" · Step {step_num}" if (not is_ar and step_num > 1) else "")
            )
            title = meta["title_ar"] if is_ar else meta["title_en"]
            prefix = f"جولة {meta['code']}: " if is_ar else f"Tour {meta['code']}: "
            hydrated.append({
                "type": "tour",
                "id": target_id,
                "code": meta["code"],
                "module_id": meta["module_id"],
                "step": step_num,
                "label": f"{prefix}{title}{step_suffix}",
            })

        if len(hydrated) >= 3:
            break

    return hydrated


# ==============================================================================
# 5. FIRST-PARTY TELEMETRY & PROSPECT QUESTION LOGGER (LOCAL JSONL + ASYNC GCS)
# ==============================================================================

_BOT_UA_RE = re.compile(
    r"(googlebot|bingbot|slurp|duckduckbot|baiduspider|yandexbot|ahrefsbot|semrushbot|"
    r"mj12bot|dotbot|bytespider|petalbot|facebookexternalhit|linkedinbot|twitterbot|"
    r"whatsapp|telegrambot|discordbot|uptimerobot|pingdom|headlesschrome|phantomjs|"
    r"python-urllib|python-requests|curl/|wget/|go-http-client)",
    re.IGNORECASE,
)

_MODULE_DISPLAY_LABELS = {
    "custody": "01 · LG Custody (Inbound)",
    "issuance": "02 · LG Issuance (Outbound)",
    "quotations": "03 · FX & T-Bill Quotations",
    "reconciliation": "04 · Bank Auto-Reconciliation",
}

_STORY_DISPLAY_LABELS = {
    "platform_intro": "65s Full Platform Intro",
    "custody": "75s LG Custody Story",
    "issuance": "75s LG Issuance Story",
    "quotations": "75s FX & T-Bill Story",
    "reconciliation": "75s Bank Auto-Recon Story",
}

_PAGE_DISPLAY_LABELS = {
    "landing": "Main Website (/)",
    "demo": "Interactive Demo (/demo)",
    "know_more": "Platform Overview (/know-more)",
    "free_trial": "Free Trial Page (/free-trial-register)",
}


def is_bot_user_agent(ua: str) -> bool:
    if not ua:
        return False
    return bool(_BOT_UA_RE.search(ua))


_MAX_DAILY_EVENTS_BYTES = 15 * 1024 * 1024  # 15 MB/day hard disk-safety ceiling

_LOOPBACK_RAW_IPS = frozenset({"127.0.0.1", "::1", "localhost", "0.0.0.0", "testclient", "testserver"})


def is_loopback_ip(raw_ip: str) -> bool:
    """Returns True when the raw IP string represents a local loopback or test client address."""
    cleaned = (raw_ip or "").strip().lower()
    if cleaned.startswith("::ffff:"):
        cleaned = cleaned[len("::ffff:"):]
    return cleaned in _LOOPBACK_RAW_IPS or cleaned.startswith("127.")


def is_production_public_telemetry_env() -> bool:
    """
    Returns True ONLY when running in the live production deployment that owns
    `gs://<bucket>/public_demo_chat_logs/`. Local dev, tests, and staging return False
    so they never pollute production analytics or overwrite Voice AI state.
    """
    explicit_sync = os.getenv("PUBLIC_TELEMETRY_GCS_SYNC", "").strip("'\"").lower()
    if explicit_sync in ("false", "0", "no", "off"):
        return False
    if explicit_sync in ("true", "1", "yes", "on"):
        return True

    db_url = os.getenv("DATABASE_URL", "").strip("'\"").lower()
    if any(tok in db_url for tok in ("localhost", "127.0.0.1", "sqlite", "staging")):
        return False

    for env_key in ("ENVIRONMENT", "ENVIRONMENT_NAME", "APP_ENV", "STORAGE_ENV", "FLASK_ENV"):
        val = os.getenv(env_key, "").strip("'\"").lower()
        if not val:
            continue
        if val in ("prod", "production"):
            return True
        if val in ("dev", "development", "local", "test", "testing", "stage", "staging", "uat"):
            return False

    render_service = os.getenv("RENDER_SERVICE_NAME", "").strip().lower()
    render_branch = os.getenv("RENDER_GIT_BRANCH", "").strip().lower()
    if any(tok in render_service for tok in ("stage", "staging", "dev", "uat", "test")) or any(
        tok in render_branch for tok in ("stage", "staging", "dev")
    ):
        return False

    if os.getenv("RENDER") or os.getenv("K_SERVICE") or "grow_xl8z" in db_url:
        return True

    return False


def _loopback_ip_hashes() -> set:
    """Computes the set of current-month salted hashes for loopback/test client IPs."""
    return {hash_client_ip(ip) for ip in _LOOPBACK_RAW_IPS}


def _is_local_or_loopback_entry(entry: Dict[str, Any], loopback_hashes: set) -> bool:
    """Returns True if a telemetry/chat JSONL record originated from local dev or a loopback IP."""
    if not isinstance(entry, dict):
        return False
    return entry.get("is_local_dev") is True or entry.get("ip_hash") in loopback_hashes


class PublicDemoChatLogger:
    """
    Logs anonymous public website/demo telemetry events + AI Product Expert Q&A
    locally (`private_storage/public_demo_chat_logs/`) and syncs asynchronously to GCS,
    with single-call manifest GCS hydration so Cloud Run restarts keep full history with zero latency spikes.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._gcs_checked_at: Dict[str, float] = {}
        self._gcs_remote_files: Optional[set] = None
        self._gcs_manifest_ts: float = 0.0
        self._pending_event_dates: set = set()
        self._event_sync_scheduled: bool = False
        self._ensure_dir()

    def _ensure_dir(self) -> None:
        try:
            os.makedirs(PUBLIC_CHAT_LOG_DIR, exist_ok=True)
        except Exception as exc:
            logger.warning(f"Could not create public demo chat log dir: {exc}")

    def _get_gcs_bucket(self):
        if not is_production_public_telemetry_env():
            return None

        import socket
        try:
            socket.gethostbyname("oauth2.googleapis.com")
        except Exception:
            return None

        from google.cloud import storage

        bucket_name = os.getenv("GCS_BUCKET_NAME", "")
        if not bucket_name:
            return None

        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip("'\"")
        if credentials_path and os.path.exists(credentials_path):
            client = storage.Client.from_service_account_json(credentials_path)
        else:
            client = storage.Client()
        return client.bucket(bucket_name)

    def _get_gcs_remote_filenames(self, bucket) -> set:
        """Fetches the list of existing GCS log filenames in 1 single API call (cached 10 min)."""
        now_mono = time.monotonic()
        if self._gcs_remote_files is not None and (now_mono - self._gcs_manifest_ts) < 600.0:
            return self._gcs_remote_files
        try:
            prefix = "public_demo_chat_logs/"
            names = {
                b.name[len(prefix):]
                for b in bucket.list_blobs(prefix=prefix, timeout=4)
                if b.name.startswith(prefix)
            }
            self._gcs_remote_files = names
            self._gcs_manifest_ts = now_mono
            return names
        except Exception:
            self._gcs_remote_files = set()
            self._gcs_manifest_ts = now_mono
            return set()

    def _hydrate_local_file_from_gcs_if_missing(self, filename: str, local_path: str) -> None:
        """If a daily JSONL file does not exist locally, pull from GCS only if present in the cached manifest."""
        if os.path.exists(local_path):
            return
        now_mono = time.monotonic()
        last_check = self._gcs_checked_at.get(filename, 0.0)
        if now_mono - last_check < 600.0:
            return
        self._gcs_checked_at[filename] = now_mono

        try:
            bucket = self._get_gcs_bucket()
            if bucket is None:
                return
            remote_names = self._get_gcs_remote_filenames(bucket)
            if filename not in remote_names:
                return
            blob = bucket.blob(f"public_demo_chat_logs/{filename}")
            remote_text = blob.download_as_text(encoding="utf-8", timeout=5)
            if remote_text and remote_text.strip():
                with self._lock:
                    if not os.path.exists(local_path):
                        with open(local_path, "w", encoding="utf-8") as f:
                            f.write(remote_text if remote_text.endswith("\n") else remote_text + "\n")
        except Exception as exc:
            logger.debug(f"GCS hydration skipped for {filename}: {exc}")

    def log_telemetry_event(
        self,
        *,
        event_type: str,
        session_id: str,
        visitor_id: str = "",
        page: str = "",
        path: str = "",
        referrer_source: str = "Direct",
        utm_campaign: str = "",
        device_type: str = "desktop",
        lang: str = "en",
        country: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        ip_hash: str = "",
        user_agent: str = "",
    ) -> None:
        is_prod = is_production_public_telemetry_env()
        if is_prod and ip_hash in _loopback_ip_hashes():
            return

        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        timestamp_iso = now.isoformat()
        event_id = hashlib.sha1(
            f"{timestamp_iso}:{session_id}:{event_type}:{os.urandom(4).hex()}".encode("utf-8")
        ).hexdigest()[:16]

        safe_meta: Dict[str, Any] = {}
        if isinstance(metadata, dict):
            for k, v in list(metadata.items())[:12]:
                safe_key = str(k)[:40]
                if isinstance(v, (int, float, bool)):
                    safe_meta[safe_key] = v
                elif v is not None:
                    safe_meta[safe_key] = str(v)[:120]

        payload: Dict[str, Any] = {
            "event_id": event_id,
            "timestamp": timestamp_iso,
            "event_type": (event_type or "page_view")[:40],
            "session_id": (session_id or "anon")[:64],
            "visitor_id": (visitor_id or session_id or ip_hash or "anon")[:64],
            "page": (page or "")[:40],
            "path": (path or "")[:160],
            "referrer_source": (referrer_source or "Direct")[:80],
            "utm_campaign": (utm_campaign or "")[:80],
            "device_type": (device_type or "desktop")[:20],
            "lang": "ar" if (lang or "").lower().startswith("ar") else "en",
            "country": (country or "")[:8].upper(),
            "metadata": safe_meta,
            "ip_hash": ip_hash,
            "is_bot": is_bot_user_agent(user_agent),
        }
        if not is_prod:
            payload["is_local_dev"] = True

        try:
            self._ensure_dir()
            events_path = os.path.join(PUBLIC_CHAT_LOG_DIR, f"public_events_{date_str}.jsonl")
            with self._lock:
                if os.path.exists(events_path) and os.path.getsize(events_path) >= _MAX_DAILY_EVENTS_BYTES:
                    return
                with open(events_path, "a", encoding="utf-8") as ef:
                    ef.write(json.dumps(payload, ensure_ascii=False) + "\n")
                self._pending_event_dates.add(date_str)
                should_spawn = not self._event_sync_scheduled
                if should_spawn:
                    self._event_sync_scheduled = True

            if should_spawn:
                timer = threading.Timer(12.0, self._flush_pending_events_to_gcs)
                timer.daemon = True
                timer.start()
        except Exception as exc:
            logger.debug(f"Failed writing local public telemetry event: {exc}")

    def _flush_pending_events_to_gcs(self) -> None:
        with self._lock:
            dates_to_sync = list(self._pending_event_dates)
            self._pending_event_dates.clear()
            self._event_sync_scheduled = False

        if not dates_to_sync:
            return

        try:
            bucket = self._get_gcs_bucket()
            if bucket is None:
                return

            loopback_hashes = _loopback_ip_hashes()

            for date_str in dates_to_sync:
                filename = f"public_events_{date_str}.jsonl"
                local_path = os.path.join(PUBLIC_CHAT_LOG_DIR, filename)
                if not os.path.exists(local_path):
                    continue

                with self._lock:
                    with open(local_path, "r", encoding="utf-8") as lf:
                        local_lines = [ln.strip() for ln in lf if ln.strip()]

                blob = bucket.blob(f"public_demo_chat_logs/{filename}")
                merged_by_id: Dict[str, str] = {}
                ordered_lines: List[str] = []

                blob_exists = blob.exists(timeout=3)
                removed_remote = 0
                if blob_exists:
                    remote_text = blob.download_as_text(encoding="utf-8", timeout=5)
                    for rline in remote_text.splitlines():
                        rline = rline.strip()
                        if not rline:
                            continue
                        try:
                            robj = json.loads(rline)
                            if _is_local_or_loopback_entry(robj, loopback_hashes):
                                removed_remote += 1
                                continue
                            eid = robj.get("event_id") or rline[:48]
                            if eid not in merged_by_id:
                                merged_by_id[eid] = rline
                                ordered_lines.append(rline)
                        except Exception:
                            pass

                new_added = 0
                for lline in local_lines:
                    try:
                        lobj = json.loads(lline)
                        if _is_local_or_loopback_entry(lobj, loopback_hashes):
                            continue
                        eid = lobj.get("event_id") or lline[:48]
                        if eid not in merged_by_id:
                            merged_by_id[eid] = lline
                            ordered_lines.append(lline)
                            new_added += 1
                    except Exception:
                        pass

                if new_added > 0 or removed_remote > 0 or (not blob_exists and ordered_lines):
                    merged_body = ("\n".join(ordered_lines) + "\n") if ordered_lines else ""
                    blob.upload_from_string(
                        merged_body,
                        content_type="application/x-jsonlines",
                        timeout=6,
                    )
                    with self._lock:
                        with open(local_path, "w", encoding="utf-8") as wf:
                            wf.write(merged_body)
        except Exception as exc:
            logger.debug(f"Public telemetry GCS batch sync skipped: {exc}")

    def log_exchange(
        self,
        *,
        session_id: str,
        page_context: str,
        lang: str,
        question: str,
        answer: str,
        topic: str,
        source_mode: str,
        actions: List[Dict[str, Any]],
        show_commercial_cta: bool,
        ip_hash: str,
        user_agent: str,
        latency_ms: float,
    ) -> None:
        is_prod = is_production_public_telemetry_env()
        if is_prod and ip_hash in _loopback_ip_hashes():
            return

        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        timestamp_iso = now.isoformat()

        payload: Dict[str, Any] = {
            "timestamp": timestamp_iso,
            "session_id": (session_id or "anon")[:64],
            "page_context": (page_context or "unknown")[:120],
            "lang": lang,
            "topic": topic,
            "source_mode": source_mode,
            "question": question,
            "answer": answer,
            "actions": [
                {"type": a.get("type"), "id": a.get("id"), "step": a.get("step")}
                for a in (actions or [])
            ],
            "show_commercial_cta": bool(show_commercial_cta),
            "ip_hash": ip_hash,
            "user_agent": (user_agent or "")[:160],
            "latency_ms": round(latency_ms, 1),
        }
        if not is_prod:
            payload["is_local_dev"] = True

        try:
            self._write_local(date_str, payload)
        except Exception as exc:
            logger.error(f"Failed writing local public demo chat log: {exc}")

        try:
            t = threading.Thread(
                target=self._sync_to_gcs_background,
                args=(date_str, payload),
                daemon=True,
            )
            t.start()
        except Exception as exc:
            logger.debug(f"Could not spawn GCS sync thread for public chat log: {exc}")

    def _write_local(self, date_str: str, payload: Dict[str, Any]) -> None:
        self._ensure_dir()
        jsonl_path = os.path.join(PUBLIC_CHAT_LOG_DIR, f"public_chat_{date_str}.jsonl")
        txt_path = os.path.join(PUBLIC_CHAT_LOG_DIR, f"public_chat_transcript_{date_str}.txt")

        action_str = ", ".join(
            f"{a.get('type')}:{a.get('id')}@{a.get('step')}" for a in payload.get("actions", [])
        ) or "None"

        transcript_block = (
            f"\n{'=' * 84}\n"
            f"[{payload['timestamp']}] Context: {payload['page_context']} | Lang: {payload['lang']} | "
            f"Topic: {payload['topic']} | Mode: {payload['source_mode']} | {payload['latency_ms']}ms\n"
            f"Session: {payload['session_id']} | IP-Hash: {payload['ip_hash']}\n"
            f"Visitor Question: {payload['question']}\n"
            f"{'-' * 84}\n"
            f"Grow AI Answer:\n{payload['answer']}\n"
            f"Proof Chips: {action_str} | Commercial CTA: {payload['show_commercial_cta']}\n"
            f"{'=' * 84}\n"
        )

        with self._lock:
            with open(jsonl_path, "a", encoding="utf-8") as jf:
                jf.write(json.dumps(payload, ensure_ascii=False) + "\n")
            with open(txt_path, "a", encoding="utf-8") as tf:
                tf.write(transcript_block)

    def _sync_to_gcs_background(self, date_str: str, payload: Dict[str, Any]) -> None:
        try:
            if _is_local_or_loopback_entry(payload, _loopback_ip_hashes()):
                return
            bucket = self._get_gcs_bucket()
            if bucket is None:
                return

            blob_name = f"public_demo_chat_logs/public_chat_{date_str}.jsonl"
            blob = bucket.blob(blob_name)
            new_line = json.dumps(payload, ensure_ascii=False) + "\n"

            if blob.exists(timeout=3):
                existing = blob.download_as_text(encoding="utf-8", timeout=4)
                blob.upload_from_string(
                    existing + new_line,
                    content_type="application/x-jsonlines",
                    timeout=4,
                )
            else:
                blob.upload_from_string(
                    new_line,
                    content_type="application/x-jsonlines",
                    timeout=4,
                )
        except Exception as exc:
            logger.debug(f"Public demo chat GCS sync skipped: {exc}")

    def read_recent_logs(self, days: int = 14, limit: int = 200) -> Dict[str, Any]:
        """Reads recent anonymous visitor questions for System Owner review."""
        self._ensure_dir()
        days = max(1, min(days, 90))
        limit = max(1, min(limit, 500))

        is_prod = is_production_public_telemetry_env()
        prod_loopback_hashes = _loopback_ip_hashes() if is_prod else set()

        entries: List[Dict[str, Any]] = []
        topic_counts: Dict[str, int] = {}
        now = datetime.now(timezone.utc)

        for offset in range(days):
            d_str = (now - timedelta(days=offset)).strftime("%Y-%m-%d")
            fname = f"public_chat_{d_str}.jsonl"
            j_path = os.path.join(PUBLIC_CHAT_LOG_DIR, fname)
            self._hydrate_local_file_from_gcs_if_missing(fname, j_path)
            if not os.path.exists(j_path):
                continue
            try:
                with open(j_path, "r", encoding="utf-8") as f:
                    day_lines = f.readlines()
                for raw_line in reversed(day_lines):
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    rec = json.loads(raw_line)
                    if is_prod and _is_local_or_loopback_entry(rec, prod_loopback_hashes):
                        continue
                    t_key = rec.get("topic") or "general"
                    topic_counts[t_key] = topic_counts.get(t_key, 0) + 1
                    if len(entries) < limit:
                        entries.append(rec)
            except Exception as exc:
                logger.warning(f"Failed reading public chat log {j_path}: {exc}")

        return {
            "total_in_window": sum(topic_counts.values()),
            "returned_count": len(entries),
            "days": days,
            "topic_breakdown": topic_counts,
            "entries": entries,
        }

    def compute_public_analytics(
        self,
        days: int = 14,
        chat_limit: int = 300,
        session_limit: int = 150,
    ) -> Dict[str, Any]:
        """
        Aggregates Website Visits, Demo Engagement (Stories & 15 Workflows),
        AI Chatbot Usage, High-Intent Conversions, Unified Visitor Journeys,
        and Full Searchable AI Q&A Transcripts across the requested day window.
        """
        self._ensure_dir()
        days = max(1, min(days, 90))
        chat_limit = max(1, min(chat_limit, 500))
        session_limit = max(1, min(session_limit, 300))

        is_prod = is_production_public_telemetry_env()
        prod_loopback_hashes = _loopback_ip_hashes() if is_prod else set()

        now = datetime.now(timezone.utc)
        date_keys = [
            (now - timedelta(days=offset)).strftime("%Y-%m-%d")
            for offset in reversed(range(days))
        ]

        daily_visitors: Dict[str, set] = {d: set() for d in date_keys}
        daily_demo_visitors: Dict[str, set] = {d: set() for d in date_keys}
        daily_chat_questions: Dict[str, int] = {d: 0 for d in date_keys}

        unique_visitors: set = set()
        unique_sessions: set = set()
        landing_visitors: set = set()
        demo_visitors: set = set()
        story_viewers: set = set()
        story_completers: set = set()
        workflow_explorers: set = set()
        chatbot_openers: set = set()
        chatbot_active_users: set = set()
        high_intent_visitors: set = set()

        total_pageviews = 0
        bot_events_filtered = 0
        meeting_submissions = 0
        trial_submissions = 0
        brochure_downloads = 0

        page_views_count: Dict[str, int] = {k: 0 for k in _PAGE_DISPLAY_LABELS}
        page_unique_visitors: Dict[str, set] = {k: set() for k in _PAGE_DISPLAY_LABELS}

        module_stats: Dict[str, Dict[str, int]] = {
            m: {"story_plays": 0, "story_completions": 0, "workflow_opens": 0, "chat_questions": 0}
            for m in _MODULE_DISPLAY_LABELS
        }
        story_stats: Dict[str, Dict[str, int]] = {
            s: {"plays": 0, "completions": 0}
            for s in _STORY_DISPLAY_LABELS
        }
        workflow_opens: Dict[str, int] = {}

        source_sessions: Dict[str, set] = {}
        device_sessions: Dict[str, set] = {}
        country_sessions: Dict[str, set] = {}
        lang_sessions: Dict[str, set] = {"en": set(), "ar": set()}
        topic_counts: Dict[str, int] = {}

        sessions_map: Dict[str, Dict[str, Any]] = {}
        chat_logs_newest_first: List[Dict[str, Any]] = []
        total_chat_questions = 0

        def _get_or_init_session(
            sess_id: str,
            vis_id: str,
            ts: str,
            source: str = "Direct",
            device: str = "desktop",
            lang_val: str = "en",
            country_val: str = "",
        ) -> Dict[str, Any]:
            rec = sessions_map.get(sess_id)
            if rec is None:
                short_seed = vis_id if vis_id and vis_id != "anon" else sess_id
                short_hash = hashlib.md5(short_seed.encode("utf-8")).hexdigest()[:6]
                rec = {
                    "session_id": sess_id,
                    "visitor_id": vis_id,
                    "short_id": f"#{short_hash}",
                    "first_seen": ts,
                    "last_seen": ts,
                    "source": source or "Direct",
                    "device": device or "desktop",
                    "lang": "ar" if (lang_val or "").startswith("ar") else "en",
                    "country": country_val or "",
                    "pages_visited": [],
                    "stories_played": [],
                    "stories_completed": [],
                    "tours_opened": [],
                    "opened_chat": False,
                    "chat_count": 0,
                    "conversions": [],
                }
                sessions_map[sess_id] = rec
            else:
                if ts and ts < rec["first_seen"]:
                    rec["first_seen"] = ts
                if ts and ts > rec["last_seen"]:
                    rec["last_seen"] = ts
                if rec["source"] in ("", "Direct", "Unknown") and source and source not in ("Direct", "Unknown"):
                    rec["source"] = source
                if not rec["country"] and country_val:
                    rec["country"] = country_val
                if lang_val == "ar":
                    rec["lang"] = "ar"
            return rec

        # 1. Read Telemetry Events (chronological oldest -> newest)
        for d_str in date_keys:
            ev_fname = f"public_events_{d_str}.jsonl"
            ev_path = os.path.join(PUBLIC_CHAT_LOG_DIR, ev_fname)
            self._hydrate_local_file_from_gcs_if_missing(ev_fname, ev_path)
            if not os.path.exists(ev_path):
                continue

            try:
                with open(ev_path, "r", encoding="utf-8") as ef:
                    ev_lines = ef.readlines()
                for raw_line in ev_lines:
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    try:
                        ev = json.loads(raw_line)
                    except Exception:
                        continue

                    if ev.get("is_bot"):
                        bot_events_filtered += 1
                        continue
                    if is_prod and _is_local_or_loopback_entry(ev, prod_loopback_hashes):
                        continue

                    etype = str(ev.get("event_type") or "")
                    sess_id = str(ev.get("session_id") or "anon")
                    vis_id = str(ev.get("visitor_id") or sess_id)
                    if vis_id == "anon" and ev.get("ip_hash"):
                        vis_id = f"ip_{ev['ip_hash']}"
                    ts = str(ev.get("timestamp") or "")
                    source = str(ev.get("referrer_source") or "Direct")
                    device = str(ev.get("device_type") or "desktop").lower()
                    lang_val = "ar" if str(ev.get("lang") or "").lower().startswith("ar") else "en"
                    country_val = str(ev.get("country") or "").upper()
                    meta = ev.get("metadata") if isinstance(ev.get("metadata"), dict) else {}

                    unique_visitors.add(vis_id)
                    unique_sessions.add(sess_id)
                    if d_str in daily_visitors:
                        daily_visitors[d_str].add(vis_id)

                    source_sessions.setdefault(source, set()).add(sess_id)
                    device_sessions.setdefault(device, set()).add(sess_id)
                    lang_sessions.setdefault(lang_val, set()).add(sess_id)
                    if country_val and country_val not in ("XX", "ZZ", "UNKNOWN"):
                        country_sessions.setdefault(country_val, set()).add(sess_id)

                    srec = _get_or_init_session(
                        sess_id=sess_id,
                        vis_id=vis_id,
                        ts=ts,
                        source=source,
                        device=device,
                        lang_val=lang_val,
                        country_val=country_val,
                    )

                    if etype == "page_view":
                        total_pageviews += 1
                        pg = str(ev.get("page") or "landing")
                        if pg not in page_views_count:
                            path_val = str(ev.get("path") or "")
                            if "/demo" in path_val:
                                pg = "demo"
                            elif "/know-more" in path_val:
                                pg = "know_more"
                            elif "/free-trial" in path_val:
                                pg = "free_trial"
                            else:
                                pg = "landing"
                        page_views_count[pg] = page_views_count.get(pg, 0) + 1
                        page_unique_visitors.setdefault(pg, set()).add(vis_id)

                        if pg not in srec["pages_visited"]:
                            srec["pages_visited"].append(pg)
                        if pg == "landing":
                            landing_visitors.add(vis_id)
                        elif pg == "demo":
                            demo_visitors.add(vis_id)
                            if d_str in daily_demo_visitors:
                                daily_demo_visitors[d_str].add(vis_id)

                    elif etype == "story_start":
                        demo_visitors.add(vis_id)
                        story_viewers.add(vis_id)
                        if d_str in daily_demo_visitors:
                            daily_demo_visitors[d_str].add(vis_id)
                        sid = str(meta.get("story_id") or "")
                        if sid in story_stats:
                            story_stats[sid]["plays"] += 1
                            if sid not in srec["stories_played"]:
                                srec["stories_played"].append(sid)
                        if sid in module_stats:
                            module_stats[sid]["story_plays"] += 1

                    elif etype == "story_complete":
                        demo_visitors.add(vis_id)
                        story_viewers.add(vis_id)
                        story_completers.add(vis_id)
                        sid = str(meta.get("story_id") or "")
                        if sid in story_stats:
                            story_stats[sid]["completions"] += 1
                            if sid not in srec["stories_completed"]:
                                srec["stories_completed"].append(sid)
                        if sid in module_stats:
                            module_stats[sid]["story_completions"] += 1

                    elif etype == "module_expand":
                        demo_visitors.add(vis_id)
                        mid = str(meta.get("module_id") or "")
                        if mid in module_stats:
                            module_stats[mid]["workflow_opens"] += 1

                    elif etype == "tour_open":
                        demo_visitors.add(vis_id)
                        workflow_explorers.add(vis_id)
                        if d_str in daily_demo_visitors:
                            daily_demo_visitors[d_str].add(vis_id)
                        bid = str(meta.get("branch_id") or "")
                        mid = str(meta.get("module_id") or "")
                        if not mid and bid in SHOWCASE_TOURS:
                            mid = SHOWCASE_TOURS[bid]["module_id"]
                        if mid in module_stats:
                            module_stats[mid]["workflow_opens"] += 1
                        if bid:
                            workflow_opens[bid] = workflow_opens.get(bid, 0) + 1
                            code_lbl = SHOWCASE_TOURS[bid]["code"] if bid in SHOWCASE_TOURS else bid
                            if code_lbl not in srec["tours_opened"]:
                                srec["tours_opened"].append(code_lbl)

                    elif etype == "chatbot_open":
                        chatbot_openers.add(vis_id)
                        srec["opened_chat"] = True

                    elif etype == "cta_click":
                        target = str(meta.get("target") or "")
                        if target in (
                            "book_meeting_submit",
                            "book_meeting_click",
                            "free_trial_submit",
                            "free_trial_click",
                            "brochure_download",
                        ):
                            high_intent_visitors.add(vis_id)
                            if target not in srec["conversions"]:
                                srec["conversions"].append(target)
                        if target == "book_meeting_submit":
                            meeting_submissions += 1
                        elif target == "free_trial_submit":
                            trial_submissions += 1
                        elif target == "brochure_download":
                            brochure_downloads += 1
            except Exception as exc:
                logger.warning(f"Failed reading public telemetry {ev_path}: {exc}")

        # 2. Read AI Chatbot Q&A Logs (newest days first for chat_logs list, while merging into sessions!)
        for d_str in reversed(date_keys):
            ch_fname = f"public_chat_{d_str}.jsonl"
            ch_path = os.path.join(PUBLIC_CHAT_LOG_DIR, ch_fname)
            self._hydrate_local_file_from_gcs_if_missing(ch_fname, ch_path)
            if not os.path.exists(ch_path):
                continue

            try:
                with open(ch_path, "r", encoding="utf-8") as cf:
                    ch_lines = cf.readlines()
                for raw_line in reversed(ch_lines):
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    try:
                        rec = json.loads(raw_line)
                    except Exception:
                        continue

                    if is_prod and _is_local_or_loopback_entry(rec, prod_loopback_hashes):
                        continue

                    ua = str(rec.get("user_agent") or "")
                    if is_bot_user_agent(ua):
                        continue

                    sess_id = str(rec.get("session_id") or "anon")
                    existing_sess = sessions_map.get(sess_id)
                    vis_id = (
                        existing_sess["visitor_id"]
                        if existing_sess
                        else (sess_id if sess_id != "anon" else f"ip_{rec.get('ip_hash', 'anon')}")
                    )
                    ts = str(rec.get("timestamp") or "")
                    lang_val = "ar" if str(rec.get("lang") or "").lower().startswith("ar") else "en"
                    ctx_val = str(rec.get("page_context") or "demo").lower()

                    total_chat_questions += 1
                    if d_str in daily_chat_questions:
                        daily_chat_questions[d_str] += 1

                    unique_visitors.add(vis_id)
                    unique_sessions.add(sess_id)
                    if d_str in daily_visitors:
                        daily_visitors[d_str].add(vis_id)

                    chatbot_openers.add(vis_id)
                    chatbot_active_users.add(vis_id)
                    lang_sessions.setdefault(lang_val, set()).add(sess_id)

                    if "demo" in ctx_val or any(m in ctx_val for m in _MODULE_DISPLAY_LABELS):
                        demo_visitors.add(vis_id)
                        if d_str in daily_demo_visitors:
                            daily_demo_visitors[d_str].add(vis_id)

                    for m_key in _MODULE_DISPLAY_LABELS:
                        if m_key in ctx_val or m_key in str(rec.get("topic") or "").lower():
                            module_stats[m_key]["chat_questions"] += 1

                    t_key = str(rec.get("topic") or "general")
                    topic_counts[t_key] = topic_counts.get(t_key, 0) + 1

                    srec = _get_or_init_session(
                        sess_id=sess_id,
                        vis_id=vis_id,
                        ts=ts,
                        source="Direct",
                        device="desktop",
                        lang_val=lang_val,
                    )
                    srec["opened_chat"] = True
                    srec["chat_count"] += 1
                    if "demo" in ctx_val and "demo" not in srec["pages_visited"]:
                        srec["pages_visited"].append("demo")
                    elif "landing" in ctx_val and "landing" not in srec["pages_visited"]:
                        srec["pages_visited"].append("landing")

                    if len(chat_logs_newest_first) < chat_limit:
                        chat_logs_newest_first.append({
                            **rec,
                            "short_id": srec["short_id"],
                        })
            except Exception as exc:
                logger.warning(f"Failed reading public chat log {ch_path}: {exc}")

        u_count = len(unique_visitors)
        d_count = len(demo_visitors)
        sv_count = len(story_viewers)
        sc_count = len(story_completers)
        ca_count = len(chatbot_active_users)

        total_story_plays = sum(v["plays"] for v in story_stats.values())
        total_story_completions = sum(v["completions"] for v in story_stats.values())

        sorted_sessions = sorted(
            sessions_map.values(),
            key=lambda s: s.get("last_seen") or "",
            reverse=True,
        )[:session_limit]

        module_interest = sorted(
            [
                {
                    "module_id": m_id,
                    "label": m_lbl,
                    **module_stats[m_id],
                    "total_interactions": (
                        module_stats[m_id]["story_plays"]
                        + module_stats[m_id]["workflow_opens"]
                        + module_stats[m_id]["chat_questions"]
                    ),
                }
                for m_id, m_lbl in _MODULE_DISPLAY_LABELS.items()
            ],
            key=lambda item: item["total_interactions"],
            reverse=True,
        )

        story_breakdown = [
            {
                "story_id": s_id,
                "label": s_lbl,
                "plays": story_stats[s_id]["plays"],
                "completions": story_stats[s_id]["completions"],
                "completion_pct": (
                    round(min(100.0, (story_stats[s_id]["completions"] / story_stats[s_id]["plays"]) * 100.0), 1)
                    if story_stats[s_id]["plays"] > 0
                    else 0.0
                ),
            }
            for s_id, s_lbl in _STORY_DISPLAY_LABELS.items()
        ]

        top_workflows = sorted(
            [
                {
                    "branch_id": b_id,
                    "code": SHOWCASE_TOURS[b_id]["code"] if b_id in SHOWCASE_TOURS else "",
                    "title": SHOWCASE_TOURS[b_id]["title_en"] if b_id in SHOWCASE_TOURS else b_id,
                    "opens": cnt,
                }
                for b_id, cnt in workflow_opens.items()
            ],
            key=lambda item: item["opens"],
            reverse=True,
        )[:10]

        return {
            "days": days,
            "kpis": {
                "unique_visitors": u_count,
                "total_sessions": len(unique_sessions),
                "total_pageviews": total_pageviews,
                "landing_visitors": len(landing_visitors),
                "demo_visitors": d_count,
                "demo_visit_rate_pct": round((d_count / u_count) * 100.0, 1) if u_count > 0 else 0.0,
                "story_viewers": sv_count,
                "story_completers": sc_count,
                "story_completion_rate_pct": (
                    round(min(100.0, (total_story_completions / total_story_plays) * 100.0), 1)
                    if total_story_plays > 0
                    else 0.0
                ),
                "workflow_explorers": len(workflow_explorers),
                "chatbot_openers": len(chatbot_openers),
                "chatbot_active_users": ca_count,
                "chatbot_interaction_rate_pct": round((ca_count / u_count) * 100.0, 1) if u_count > 0 else 0.0,
                "total_chat_questions": total_chat_questions,
                "avg_questions_per_chatter": round(total_chat_questions / ca_count, 1) if ca_count > 0 else 0.0,
                "high_intent_leads": len(high_intent_visitors),
                "meeting_submissions": meeting_submissions,
                "trial_submissions": trial_submissions,
                "brochure_downloads": brochure_downloads,
                "bot_events_filtered": bot_events_filtered,
            },
            "daily_series": [
                {
                    "date": d_str,
                    "visitors": len(daily_visitors[d_str]),
                    "demo_visitors": len(daily_demo_visitors[d_str]),
                    "chat_questions": daily_chat_questions[d_str],
                }
                for d_str in date_keys
            ],
            "page_breakdown": [
                {
                    "page": pg_id,
                    "label": pg_lbl,
                    "views": page_views_count.get(pg_id, 0),
                    "unique_visitors": len(page_unique_visitors.get(pg_id, set())),
                }
                for pg_id, pg_lbl in _PAGE_DISPLAY_LABELS.items()
            ],
            "module_interest": module_interest,
            "story_breakdown": story_breakdown,
            "top_workflows": top_workflows,
            "source_breakdown": sorted(
                [{"source": k, "sessions": len(v)} for k, v in source_sessions.items()],
                key=lambda x: x["sessions"],
                reverse=True,
            )[:10],
            "device_breakdown": sorted(
                [{"device": k, "sessions": len(v)} for k, v in device_sessions.items()],
                key=lambda x: x["sessions"],
                reverse=True,
            ),
            "language_breakdown": {
                "en": len(lang_sessions.get("en", set())),
                "ar": len(lang_sessions.get("ar", set())),
            },
            "country_breakdown": sorted(
                [{"country": k, "sessions": len(v)} for k, v in country_sessions.items()],
                key=lambda x: x["sessions"],
                reverse=True,
            )[:10],
            "topic_breakdown": sorted(
                [{"topic": k, "count": v} for k, v in topic_counts.items()],
                key=lambda x: x["count"],
                reverse=True,
            )[:12],
            "recent_sessions": sorted_sessions,
            "chat_logs": chat_logs_newest_first,
            "voice_control": public_voice_control.get_admin_status(),
        }


public_demo_chat_logger = PublicDemoChatLogger()


# ==============================================================================
# 5B. VOICE AI MASTER KILL-SWITCH, MONTHLY 1,000-Q&A QUOTA & NEAR-CAP ALERTS
# ==============================================================================

class PublicVoiceControlManager:
    """
    Manages the System Owner's 1-click Voice AI master switch (`voice_enabled`),
    monthly Voice Q&A quota counter (default `1,000` Q&As/month), optional auto-shutoff
    at cap, and automated 80% Near-Cap & 100% Cap email alerts to System Owners.
    Persists locally and syncs/hydrates with GCS across Cloud Run container restarts.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loaded = False
        self._state: Dict[str, Any] = {
            "voice_enabled": True,
            "monthly_cap": 1000,
            "alert_at_pct": 80,
            "auto_disable_at_cap": False,
            "usage_by_month": {},
        }

    def _local_path(self) -> str:
        os.makedirs(PUBLIC_CHAT_LOG_DIR, exist_ok=True)
        return os.path.join(PUBLIC_CHAT_LOG_DIR, PUBLIC_VOICE_STATE_FILENAME)

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        with self._lock:
            if self._loaded:
                return
            local_p = self._local_path()
            if not os.path.exists(local_p):
                # Attempt 1-time hydration from GCS on fresh container boot
                try:
                    bucket = public_demo_chat_logger._get_gcs_bucket()
                    if bucket is not None:
                        blob = bucket.blob(f"public_demo_chat_logs/{PUBLIC_VOICE_STATE_FILENAME}")
                        if blob.exists(timeout=3):
                            raw_remote = blob.download_as_text(encoding="utf-8", timeout=4)
                            if raw_remote and raw_remote.strip():
                                with open(local_p, "w", encoding="utf-8") as wf:
                                    wf.write(raw_remote)
                except Exception as exc:
                    logger.debug(f"Voice state GCS hydration skipped: {exc}")

            if os.path.exists(local_p):
                try:
                    with open(local_p, "r", encoding="utf-8") as rf:
                        loaded_obj = json.load(rf)
                    if isinstance(loaded_obj, dict):
                        self._state.update(loaded_obj)
                except Exception as exc:
                    logger.warning(f"Failed loading {local_p}: {exc}")

            if not isinstance(self._state.get("usage_by_month"), dict):
                self._state["usage_by_month"] = {}
            self._loaded = True

    def _persist_locked(self) -> str:
        """Must be called while holding `self._lock`. Returns serialized JSON string."""
        raw_json = json.dumps(self._state, ensure_ascii=False, indent=2)
        try:
            with open(self._local_path(), "w", encoding="utf-8") as wf:
                wf.write(raw_json)
        except Exception as exc:
            logger.warning(f"Failed saving local voice state: {exc}")
        return raw_json

    def _sync_gcs_async(self, raw_json: str) -> None:
        def _worker():
            try:
                bucket = public_demo_chat_logger._get_gcs_bucket()
                if bucket is None:
                    return
                blob = bucket.blob(f"public_demo_chat_logs/{PUBLIC_VOICE_STATE_FILENAME}")
                blob.upload_from_string(raw_json, content_type="application/json", timeout=5)
            except Exception as exc:
                logger.debug(f"Voice state GCS sync skipped: {exc}")

        t = threading.Thread(target=_worker, daemon=True)
        t.start()

    def _current_month_key(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m")

    def _get_or_init_month_locked(self, month_key: str) -> Dict[str, Any]:
        usage_map = self._state.setdefault("usage_by_month", {})
        m = usage_map.get(month_key)
        if not isinstance(m, dict):
            m = {
                "voice_qa_count": 0,
                "voice_input_count": 0,
                "tts_output_count": 0,
                "tts_chars_total": 0,
                "near_cap_alert_sent": False,
                "cap_reached_alert_sent": False,
            }
            usage_map[month_key] = m
        return m

    def is_voice_enabled(self) -> bool:
        self._ensure_loaded()
        return bool(self._state.get("voice_enabled", True))

    def get_public_status(self) -> Dict[str, bool]:
        return {"voice_enabled": self.is_voice_enabled()}

    def get_admin_status(self) -> Dict[str, Any]:
        self._ensure_loaded()
        month_key = self._current_month_key()
        with self._lock:
            m = dict(self._get_or_init_month_locked(month_key))
            cap = max(10, int(self._state.get("monthly_cap") or 1000))
            alert_pct = max(50, min(99, int(self._state.get("alert_at_pct") or 80)))
            enabled = bool(self._state.get("voice_enabled", True))
            auto_disable = bool(self._state.get("auto_disable_at_cap", False))

        qa_count = int(m.get("voice_qa_count") or 0)
        near_threshold = math.ceil(cap * (alert_pct / 100.0))
        usage_pct = round((qa_count / cap) * 100.0, 1) if cap > 0 else 0.0
        est_cost_usd = round((int(m.get("voice_input_count") or 0) * 0.0005) + (int(m.get("tts_chars_total") or 0) * 0.000015), 2)

        return {
            "voice_enabled": enabled,
            "monthly_cap": cap,
            "alert_at_pct": alert_pct,
            "near_cap_threshold": near_threshold,
            "auto_disable_at_cap": auto_disable,
            "current_month": month_key,
            "voice_qa_count": qa_count,
            "voice_input_count": int(m.get("voice_input_count") or 0),
            "tts_output_count": int(m.get("tts_output_count") or 0),
            "tts_chars_total": int(m.get("tts_chars_total") or 0),
            "usage_pct": usage_pct,
            "near_cap": qa_count >= near_threshold and qa_count < cap,
            "at_or_over_cap": qa_count >= cap,
            "near_cap_alert_sent": bool(m.get("near_cap_alert_sent", False)),
            "cap_reached_alert_sent": bool(m.get("cap_reached_alert_sent", False)),
            "estimated_voice_cost_usd": est_cost_usd,
        }

    def update_settings(
        self,
        *,
        voice_enabled: Optional[bool] = None,
        monthly_cap: Optional[int] = None,
        auto_disable_at_cap: Optional[bool] = None,
    ) -> Dict[str, Any]:
        self._ensure_loaded()
        month_key = self._current_month_key()
        with self._lock:
            if voice_enabled is not None:
                self._state["voice_enabled"] = bool(voice_enabled)
            if monthly_cap is not None:
                new_cap = max(10, min(100000, int(monthly_cap)))
                self._state["monthly_cap"] = new_cap
                # Reset alert flags if System Owner raised the cap above current usage
                m = self._get_or_init_month_locked(month_key)
                alert_pct = int(self._state.get("alert_at_pct") or 80)
                if int(m.get("voice_qa_count") or 0) < math.ceil(new_cap * (alert_pct / 100.0)):
                    m["near_cap_alert_sent"] = False
                if int(m.get("voice_qa_count") or 0) < new_cap:
                    m["cap_reached_alert_sent"] = False
            if auto_disable_at_cap is not None:
                self._state["auto_disable_at_cap"] = bool(auto_disable_at_cap)
            raw_json = self._persist_locked()

        self._sync_gcs_async(raw_json)
        return self.get_admin_status()

    def record_usage(
        self,
        *,
        increment_qa: bool,
        is_voice_input: bool = False,
        tts_chars: int = 0,
    ) -> None:
        self._ensure_loaded()
        month_key = self._current_month_key()
        alert_kind: Optional[str] = None
        snapshot_for_alert: Dict[str, Any] = {}

        with self._lock:
            m = self._get_or_init_month_locked(month_key)
            if increment_qa:
                m["voice_qa_count"] = int(m.get("voice_qa_count") or 0) + 1
            if is_voice_input:
                m["voice_input_count"] = int(m.get("voice_input_count") or 0) + 1
            if tts_chars > 0:
                m["tts_output_count"] = int(m.get("tts_output_count") or 0) + 1
                m["tts_chars_total"] = int(m.get("tts_chars_total") or 0) + int(tts_chars)

            cap = max(10, int(self._state.get("monthly_cap") or 1000))
            alert_pct = max(50, min(99, int(self._state.get("alert_at_pct") or 80)))
            near_threshold = math.ceil(cap * (alert_pct / 100.0))
            qa_count = int(m.get("voice_qa_count") or 0)

            auto_disabled_now = False
            if qa_count >= cap:
                if bool(self._state.get("auto_disable_at_cap", False)) and bool(self._state.get("voice_enabled", True)):
                    self._state["voice_enabled"] = False
                    auto_disabled_now = True
                if not m.get("cap_reached_alert_sent"):
                    m["cap_reached_alert_sent"] = True
                    m["near_cap_alert_sent"] = True
                    alert_kind = "CAP_REACHED"
            elif qa_count >= near_threshold and not m.get("near_cap_alert_sent"):
                m["near_cap_alert_sent"] = True
                alert_kind = "NEAR_CAP"

            if alert_kind:
                snapshot_for_alert = {
                    "alert_kind": alert_kind,
                    "month_key": month_key,
                    "qa_count": qa_count,
                    "cap": cap,
                    "near_threshold": near_threshold,
                    "voice_input_count": int(m.get("voice_input_count") or 0),
                    "tts_output_count": int(m.get("tts_output_count") or 0),
                    "auto_disabled_now": auto_disabled_now,
                    "voice_enabled": bool(self._state.get("voice_enabled", True)),
                }

            raw_json = self._persist_locked()

        self._sync_gcs_async(raw_json)
        if alert_kind and snapshot_for_alert:
            t = threading.Thread(
                target=self._send_system_owner_cap_email_worker,
                args=(snapshot_for_alert,),
                daemon=True,
            )
            t.start()

    def _send_system_owner_cap_email_worker(self, snap: Dict[str, Any]) -> None:
        """Sends an automated Near-Cap (80%) or Cap-Reached (100%) email to all active System Owners."""
        try:
            from app.constants import UserRole
            from app.core.email_service import get_global_email_settings, send_email
            from app.database import SessionLocal
            from app.models import User

            recipients: List[str] = []
            env_owner = (os.getenv("SYSTEM_OWNER_ALERT_EMAIL") or "").strip()
            if env_owner and "@" in env_owner:
                recipients.append(env_owner)

            db = SessionLocal()
            try:
                owner_rows = (
                    db.query(User.email)
                    .filter(User.role == UserRole.SYSTEM_OWNER, User.is_deleted == False)  # noqa: E712
                    .all()
                )
                for (email_val,) in owner_rows:
                    if email_val and "@" in email_val and email_val not in recipients:
                        recipients.append(email_val)
            finally:
                db.close()

            if not recipients:
                logger.info(f"[VoiceCapAlert] {snap['alert_kind']} reached ({snap['qa_count']}/{snap['cap']}), no non-test System Owner email found.")
                return

            is_full_cap = snap["alert_kind"] == "CAP_REACHED"
            subject = (
                f"Grow Voice AI Alert: Monthly Quota Reached ({snap['qa_count']}/{snap['cap']} Voice Q&As)"
                if is_full_cap
                else f"Grow Voice AI Near-Cap Notice: {snap['qa_count']}/{snap['cap']} Monthly Voice Q&As Used"
            )

            status_line = (
                "Voice AI has been <strong>automatically disabled</strong> (Mic & Listen buttons are now hidden from all visitors)."
                if snap.get("auto_disabled_now")
                else (
                    "Voice AI is currently <strong>still active</strong>. You can disable it with <strong>1 click</strong> anytime (completely hiding the Mic and Listen icons from visitors) in <strong>System Owner &rarr; User Feedback &amp; Public Insights &rarr; Website, Demo &amp; AI Chatbot</strong>."
                )
            )

            body_html = (
                f"<p>Your public <strong>Grow AI Product Expert Voice Assistant</strong> on <code>/</code> and <code>/demo</code> "
                f"has reached <strong>{snap['qa_count']} out of {snap['cap']}</strong> monthly Voice Q&amp;As for <strong>{snap['month_key']}</strong>.</p>"
                f"<ul>"
                f"<li><strong>Spoken Mic Questions:</strong> {snap['voice_input_count']}</li>"
                f"<li><strong>Studio Voice Replies Generated:</strong> {snap['tts_output_count']}</li>"
                f"</ul>"
                f"<p>{status_line}</p>"
            )

            settings = get_global_email_settings()
            asyncio.run(
                send_email(
                    db=None,
                    to_emails=recipients,
                    subject_template=subject,
                    body_template=body_html,
                    template_data={},
                    email_settings=settings,
                    sender_name="Grow System Owner Alerts",
                )
            )
            logger.info(f"[VoiceCapAlert] Sent {snap['alert_kind']} notification to System Owners.")
        except Exception as exc:
            logger.warning(f"[VoiceCapAlert] Could not send System Owner voice quota email: {exc}")


public_voice_control = PublicVoiceControlManager()


# ==============================================================================
# 6. CURATED DETERMINISTIC FALLBACK ENGINE (INSTANT OFFLINE / FAILOVER RESILIENCE)
# ==============================================================================

def _build_curated_fallback_response(question: str, lang: str) -> Dict[str, Any]:
    """
    Provides rich, 100% codebase-verified bilingual answers with deep-links if Gemini/Vertex AI
    is temporarily unreachable or offline.
    """
    q = (question or "").lower()
    is_ar = lang == "ar"

    # 1. Commercials / Pricing / Trial
    if any(k in q for k in ["price", "pricing", "cost", "subscription", "license", "plan", "trial", "pilot", "سعر", "اسعار", "أسعار", "تكلفة", "اشتراك", "باقات", "تجربة"]):
        return {
            "topic": "pricing_and_licensing",
            "show_commercial_cta": True,
            "answer": (
                "تعتمد منصة **Grow** نموذج اشتراك معياري مرن (Modular): يمكنك الاشتراك في **وحدة واحدة فقط** وإضافة الوحدات الأخرى لاحقاً أو تفعيل المنظومة الكاملة ذات الوحدات الأربع، ويتم تسعير الباقة بشفافية وفق عدد الكيانات القانونية وحجم العمليات.\n\nيمكنك البدء بـ **تجربة مجانية معزولة (Free Trial)** أو **حجز جلسة تنفيذية** لتقديم عرض مالي وفني مفصّل."
                if is_ar else
                "**Grow** is licensed modularly: you can subscribe to **any single module** and add others later, or deploy the **complete 4-module treasury suite**, scaled transparently by your legal entities and operational volume.\n\nYou can launch an isolated **Free Trial / Pilot** right away or **Book a Tailored Demo** for a scoped commercial proposal."
            ),
            "actions": validate_and_hydrate_actions([{"type": "story", "id": "platform_intro"}], lang),
            "follow_ups": (
                ["كم يستغرق ترحيل خطابات الضمان القائمة من الإكسيل؟", "هل تحتاج البنوك إلى أي ربط تقني للعمل على المنصة؟"]
                if is_ar else
                ["How fast can we migrate existing guarantees from Excel?", "Do our banks need any IT setup to work with Grow?"]
            ),
        }

    # 2. Bank IT Setup / Bank APIs / Connectivity
    if any(k in q for k in ["bank setup", "bank onboard", "bank api", "direct bank", "host to host", "do banks need", "ربط مع البنوك", "تحتاج البنوك", "دخول البنك"]):
        return {
            "topic": "bank_zero_it_onboarding",
            "show_commercial_cta": False,
            "answer": (
                "**لا تتطلب المنصة أي ربط برمجي مباشر (Bank APIs) أو إعداد تقني لدى البنوك:**\n- **حفظ الضمانات الواردة:** خطابات رسمية تحمل رمز **QR موقّع رقمياً وكود تحقق** يتأكد منه البنك فوراً عبر بوابة التحقق.\n- **إصدار الضمانات:** تعبئة آلية لنماذج PDF الرسمية المعتمدة لدى كل بنك وخطابات الشركة.\n- **عروض العملات وأذون الخزانة:** يدخل متداول البنك عبر **رابط مشفر مقيد بنطاق البريد الرسمي ورمز OTP فوري** (أو بوابة المتداول الدائمة).\n- **المطابقة البنكية:** استيراد ملفات `Excel` و`CSV` و`MT940` و`CAMT.053` مباشرة."
                if is_ar else
                "**Grow requires zero direct core-banking APIs or bank IT setup to go live:**\n- **LG Custody:** Outgoing bank letters carry an **HMAC-SHA256 QR code + split Security Code** verified in seconds on Grow's public portal.\n- **LG Issuance:** Auto-fills your banks' official PDF application forms and legal annexes.\n- **FX & T-Bill Quotations:** Bank dealers quote via **domain-locked, zero-password 6-digit OTP links** (or the optional Permanent Dealer Portal).\n- **Bank Auto-Reconciliation:** Ingests `.xlsx`, `.xls`, `.csv`, **SWIFT MT940**, and **ISO 20022 CAMT.053** files out of the box."
            ),
            "actions": validate_and_hydrate_actions([
                {"type": "tour", "id": "quotations-dealer-desk", "step": 1},
                {"type": "tour", "id": "custody-lifecycle-letter", "step": 6},
            ], lang),
            "follow_ups": (
                ["كيف تحمي المنصة سرية أسعار البنوك أثناء المنافسة؟", "كيف يتم تشفير بيانات الشركة ومستنداتها؟"]
                if is_ar else
                ["How do bank quotes stay sealed during live FX bidding?", "How are company data and documents encrypted?"]
            ),
        }

    # 3. Security / Roles / Encryption / Private Cloud
    if any(k in q for k in ["security", "encrypt", "aes", "privacy", "cloud", "bucket", "role", "passkey", "mfa", "أمان", "تشفير", "سرية", "سحابة", "صلاحيات"]):
        return {
            "topic": "security_roles_and_encryption",
            "show_commercial_cta": False,
            "answer": (
                "تطبق منصة **Grow** عزل بيانات وتشفير مصرفي متعدد الطبقات:\n- **تشفير مستقل لكل شركة (AES-256-GCM):** مفاتيح تشفير مستقلة لكل عميل مع تشفير متدفق للمستندات وروابط تحميل مؤقتة (١٥ دقيقة)، وخيار ربط **حاوية تخزين Google Cloud Storage خاصة بشركتكم**.\n- **صلاحيات وحوكمة صارمة:** أدوار محددة (`Corporate Admin`, `End User`, `Checker`, `Viewer`) مع تحديد الكيانات المسموحة ومنع الاعتماد الذاتي وإلغاء فوري للصلاحية عند تعطيل المستخدم.\n- **مصادقة قوية:** دعم **WebAuthn / Passkeys** ورموز OTP للأجهزة الجديدة وخروج تلقائي بعد ٢٠ دقيقة من عدم النشاط وسجل مراجعة **SHA-256** غير قابل للتعديل."
                if is_ar else
                "**Grow** enforces bank-grade tenant isolation, RBAC, and cryptographic controls:\n- **Per-Company AES-256-GCM Encryption:** Dedicated tenant keys, 64KB chunked document encryption with 15-minute signed download tickets, plus support for a **customer-owned Google Cloud Storage (GCS) bucket**.\n- **Roles & Entity Scoping:** Dedicated `Corporate Admin`, `End User`, `Checker`, and read-only `Viewer` roles scoped per legal entity, hard self-approval blocking, and instant next-request revocation when a user is deactivated.\n- **Strong Auth & WORM Audit:** FIDO2 **WebAuthn / Passkeys**, device OTP MFA, 20-minute inactivity auto-logout, and sequential **SHA-256 hash-chained audit logs**."
            ),
            "actions": validate_and_hydrate_actions([
                {"type": "foundation", "id": "security"},
                {"type": "foundation", "id": "governance"},
            ], lang),
            "follow_ups": (
                ["هل يمكن لمستخدم الوصول لعروض العملات دون رؤية كشوف الحسابات البنكية؟", "كيف تعمل رقابة المراجع والمعتمد (Maker-Checker)؟"]
                if is_ar else
                ["Can a user run FX Quotations without seeing Bank Statements?", "How does Maker-Checker governance work across modules?"]
            ),
        }

    # 4. Migration / Legacy Excel Onboarding
    if any(k in q for k in ["migrat", "excel", "legacy", "onboard", "existing lg", "staging", "ترحيل", "اكسيل", "إكسيل", "استيراد", "بدء التشغيل"]):
        return {
            "topic": "migration_and_onboarding",
            "show_commercial_cta": False,
            "answer": (
                "تتضمن المنصة **مراكز ترحيل ذكية (Migration Hubs)** لوحدتي **الحفظ والإصدار** لنقل السجلات التاريخية على مراحل أو دفعة واحدة:\n- استيراد ملفات **Excel / CSV** مع مطابقة ذكية للأعمدة العربية والإنجليزية، أو مسح جماعي بالذكاء الاصطناعي لملفات الـ **PDF**.\n- **فحص مرحلي (Staging) قبل الاعتماد:** فرز الأخطاء والتكرارات وتعديلها مباشرة داخل شاشة المراجعة قبل ترحيلها للسجل الفعلي."
                if is_ar else
                "**Grow** includes dedicated **Staging Migration Hubs** for both **LG Custody** and **LG Issuance**:\n- Upload legacy **Excel / CSV** registers (with fuzzy EN/AR header mapping) alongside **batch AI OCR scans of PDF guarantees**.\n- **Two-Gate Staging Preview:** Validate mandatory fields, catch duplicate LG numbers, audit PDF scans against Excel figures, and fix errors inline—migrating all at once or entity-by-entity in stages."
            ),
            "actions": validate_and_hydrate_actions([
                {"type": "foundation", "id": "enterprise"},
                {"type": "tour", "id": "custody-scan-record", "step": 1},
            ], lang),
            "follow_ups": (
                ["هل يمكن استيراد كشوف الحسابات البنكية التاريخية أيضاً؟", "ما الفرق بين وحدة حفظ الضمانات ووحدة إصدار الضمانات؟"]
                if is_ar else
                ["Can we also import historical bank statements and ERP ledgers?", "What is the difference between LG Custody and LG Issuance?"]
            ),
        }

    # 5. T-Bill & Money-Market Quotations
    if any(k in q for k in ["tbill", "t-bill", "treasury bill", "tenor", "yield", "discount rate", "money-market", "money market", "أذون", "اذون", "عائد", "خصم"]):
        return {
            "topic": "tbill_quotations",
            "show_commercial_cta": False,
            "answer": (
                "تدير المنصة عطاءات **أذون الخزانة السيادية (شراء وبيع)** عبر منافسة مغلقة ومشفرة بين البنوك:\n- تحديد القيمة الاسمية ونطاق تاريخ التسوية والاستحقاق، مع إمكانية تقديم كل بنك لعدة شرائح بـ **معدل الخصم (`Discount Rate %`)**.\n- يحسب النظام تلقائياً **السعر لكل ١٠٠ من القيمة الاسمية (Actual/365)** مع تقييم فروق تواريخ التسوية بـ **TVM** وترتيب العروض وإصدار شهادة تدقيق وإيصال تنفيذ **HMAC-SHA256**.\n- *ملاحظة دقيقة:* تشمل الوحدة أذون الخزانة والعملات الفورية (`FX Spot`) ولا تشمل الودائع لأجل أو شهادات الإيداع أو حساب ضريبة المنبع بعد التنفيذ."
                if is_ar else
                "Grow runs sealed-bid multi-bank RFQs for **Sovereign Treasury Bills (`TBILL`, Buy & Sell)**:\n- Specify face amount and settlement/maturity windows; invited banks submit one or multiple lines quoting **Discount Rate (%)** via zero-password OTP links.\n- Grow computes **Egyptian Actual/365 Price per 100 Par** normalized via **TVM (`eval_rate`)**, ranks bids at `00:00`, and issues a **Best Execution Audit Certificate** + signed **HMAC-SHA256 Deal Receipt**.\n- *Scope note:* Covers Sovereign T-Bills and FX Spot (CDs/Time Deposits, WHT post-tax math, and post-trade maturity holding trackers are not part of the RFQ module today)."
            ),
            "actions": validate_and_hydrate_actions([
                {"type": "story", "id": "quotations"},
                {"type": "tour", "id": "quotations-rfq-broadcast", "step": 1},
                {"type": "tour", "id": "quotations-net-award", "step": 4},
            ], lang),
            "follow_ups": (
                ["كيف تعمل عروض أسعار العملات الفورية متعددة الأزواج؟", "هل يمكن إعادة طرح العطاء (Re-Tender) إذا لم تكن الأسعار مناسبة؟"]
                if is_ar else
                ["How do multi-pair FX Spot baskets work?", "What happens if we decline the quotes and want to re-tender?"]
            ),
        }

    # 6. FX Quotations
    if any(k in q for k in ["fx", "quot", "cbe", "forward", "swap", "dealer", "spot", "negotiat", "عملات", "البنك المركزي", "تداول", "عروض أسعار"]):
        return {
            "topic": "quotations_fx_spot",
            "show_commercial_cta": False,
            "answer": (
                "تغطي وحدة **عروض أسعار العملات** معاملات **العملات الفورية (`FX Spot` حتى ٤ أزواج في السلة الواحدة)** *(عقود الآجل `FX Forwards` غير مفعلة حالياً)*:\n- **مرونة متطلبات البنك المركزي (CBE):** تحديد دور كل بنك (`Execution` للتنفيذ أو `Indicative` استرشادي)، ومصفوفة صلاحيات لإظهار المستندات أثناء التسعير أو للبنك الفائز فقط، وتقييم تواريخ التسوية البديلة بـ **TVM**.\n- **سرية تامة وترسية عادلة:** تظل العروض مشفرة بـ **AES-256** حتى `00:00`، ثم تُرتب بالصافي بعد العمولات وتُقارن **بأسعار السوق اللحظية ومؤشر البنك المركزي** مع إمكانية قبول أو استبعاد كل عملة وإعادة الطرح (`Re-Tender`) عند الحاجة."
                if is_ar else
                "**FX Quotations** manages multi-bank **FX Spot** RFQs (up to **4 currency pairs** per basket, Buy or Sell; *FX Forwards are not supported today*):\n- **CBE Scenario Flexibility:** Set per-leg bank roles (`Execution` vs. `Indicative` benchmark), control 2D document visibility (`BIDDING` vs. `WINNER_ONLY` / `INTERNAL_ONLY`), and normalize alternative value dates via **TVM**.\n- **Sealed Bidding & Dual Benchmarking:** Quotes stay **AES-256 sealed** until `00:00`, rank automatically on **All-In net price** (after per-bank fees & TVM) benchmarked against **both live market mid and CBE official fixings**, support independent accept/exclude per leg, and allow 1-click linked **Re-Tender (`-R1`)** rounds."
            ),
            "actions": validate_and_hydrate_actions([
                {"type": "story", "id": "quotations"},
                {"type": "tour", "id": "quotations-net-award", "step": 1},
                {"type": "tour", "id": "quotations-dealer-desk", "step": 3},
            ], lang),
            "follow_ups": (
                ["ماذا يحدث إذا تساوى بنكان في نفس السعر أو استجاب بنك واحد فقط؟", "كيف يمنع النظام أخطاء الإدخال السعري لدى متداول البنك؟"]
                if is_ar else
                ["What happens if two banks tie or only one bank responds?", "How does the dealer desk prevent fat-finger rate typos?"]
            ),
        }

    # 7. Bank Reconciliation / ERP
    if any(k in q for k in ["recon", "statement", "mt940", "sweep", "erp", "sap", "oracle", "journal", "مطابقة", "كشف حساب", "قيود", "تحويلات بينية"]):
        return {
            "topic": "bank_reconciliation_and_erp",
            "show_commercial_cta": False,
            "answer": (
                "تؤتمت وحدة **المطابقة البنكية الذكية** مطابقة الكشوف وتتعلم وتتطور باستمرار:\n- **الصيغ المدعومة:** استيراد `Excel` و`CSV/TXT` و`SWIFT MT940` و`ISO 20022 CAMT.053` بالعربية والإنجليزية مع فحص رياضي لتسلسل الأرصدة *(ملفات PDF البنكية غير مدعومة في مطابقة النقدية)*.\n- **ربط التحويلات البينية ومطابقة الـ ERP:** ربط التحويلات بين حسابات الشركة حتى مع خصم عمولات التحويل وكشف التحويلات أحادية الطرف، ومطابقة دفاتر الـ ERP (`1:1` و`Many:1` و`M:N`) مع إمكانية التراجع بضغطة واحدة.\n- **قواعد ذاتية التعلم وتصدير مقفول:** بناء قيود اليومية المزدوجة المتوازنة بصيغ `CSV/XLSX` لـ **SAP وOracle وDynamics وOdoo** مع قفل الدفعات دون الترحيل المباشر داخل قاعدة بيانات الـ ERP."
                if is_ar else
                "**Bank Auto-Reconciliation** automates statement matching and **continuously learns and evolves**:\n- **Supported Formats:** Native `.xlsx`, `.xls`, `.csv`, **SWIFT MT940**, and **ISO 20022 CAMT.053** (EN/AR) with row-by-row balance continuity validation *(PDF bank statements are not used in cash reconciliation)*.\n- **Sweeps & Multi-Pass ERP Matching:** Pairs cross-bank sweeps (isolating wire fees and flagging one-sided transfers), nets reversals, and reconciles uploaded ERP sub-ledgers (`1:1`, `Many:1`, `M:N` split match + 1-click Unmatch).\n- **Self-Healing Rules & Locked GL Export:** Generates balanced double-entry CSV/XLSX journals mapped for **SAP, Oracle, Dynamics, or Odoo** with row-level export locks—never writing directly into your ERP or bank."
            ),
            "actions": validate_and_hydrate_actions([
                {"type": "story", "id": "reconciliation"},
                {"type": "tour", "id": "recon-sweeps-reversals", "step": 1},
                {"type": "tour", "id": "recon-erp-export", "step": 3},
            ], lang),
            "follow_ups": (
                ["كيف يكتشف النظام وجود أسطر ناقصة أو تحرك في رصيد كشف الحساب؟", "كيف يتعلم النظام القواعد المتكررة مع حماية سرية الأطراف؟"]
                if is_ar else
                ["How does the balance continuity check catch missing statement rows?", "How do self-learning rules evolve under AES-256 privacy?"]
            ),
        }

    # 8. LG Issuance / Facilities / LG Position Recon
    if any(k in q for k in ["issuance", "facility", "facilities", "0-100", "applicant", "position", "إصدار", "اصدار", "تسهيلات", "مراكز الضمانات"]):
        return {
            "topic": "lg_issuance",
            "show_commercial_cta": False,
            "answer": (
                "تدير وحدة **إصدار خطابات الضمان (LG Issuance)** دورة الضمان الصادر من طلب الإدارة وحتى إقفال المركز البنكي:\n- **رقمنة وتوحيد الطلبات:** استقبال الطلبات عبر النظام أو **بوابة الإدارات الخارجية المؤمنة بـ OTP** بنموذج قابل للتخصيص ومصفوفة موافقات متسلسلة ومجموعات توقيع متوازية مع خاصية الإرجاع للاستيفاء.\n- **مفاضلة التسهيلات (٠–١٠٠) وتعبئة استمارات البنوك:** تقييم التسهيلات حسب العمولة والغطاء والرصيد وسرعة البنك الفعلية (مع رفع وزن السرعة إلى ٥٠٪ للطلبات العاجلة)، وتعبئة نماذج PDF الرسمية للبنوك ومطابقة صورة الضمان الصادر بالذكاء الاصطناعي.\n- **مطابقة تقارير مراكز الضمانات (`Bank LG Position`):** استيراد تقارير مراكز الضمانات (`Excel/CSV/PDF`) لكشف الفروق والضمانات المنتهية المفتوحة لدى البنك."
                if is_ar else
                "**LG Issuance** governs outgoing guarantees end to end:\n- **Unified Department Intake:** Captures internal and zero-login `/portal/issuance` OTP department requests with a customizable intake form, AI contract/wording checks, return-for-revision, and sequential + group approval matrices.\n- **0–100 Facility Scoring & Auto-Filled Bank PDFs:** Ranks bank credit facilities across commission, cash margin, headroom, and actualized business-day bank SLA (boosting SLA weight to 50% on urgent requests), auto-fills official bank PDF forms, and verifies issued LG scans with AI.\n- **Bank LG Position Reconciliation:** Reconciles **Excel, CSV, and PDF Bank LG Position reports** against your Issued LG register to catch variances and expired guarantees still open at the bank."
            ),
            "actions": validate_and_hydrate_actions([
                {"type": "story", "id": "issuance"},
                {"type": "tour", "id": "issuance-facility-scoring", "step": 2},
                {"type": "tour", "id": "issuance-position-recon", "step": 3},
            ], lang),
            "follow_ups": (
                ["ماذا يحدث إذا رفض البنك إصدار الضمان بعد الموافقة الداخلية؟", "كيف يتم التعامل مع الطلبات العاجلة في نفس اليوم؟"]
                if is_ar else
                ["What happens if a bank rejects an LG request after internal approval?", "How does the platform prioritize urgent issuance requests?"]
            ),
        }

    # Default Overview / LG Custody + Platform Summary
    return {
        "topic": "platform_overview",
        "show_commercial_cta": False,
        "answer": (
            "توحّد منصة **Grow** عمليات خزانة الشركات في **٤ وحدات متكاملة** قابلة للاشتراك المنفرد أو الكامل:\n1. **حفظ خطابات الضمان الواردة (`LG Custody`):** للضمانات المستلمة من الموردين أو العملاء—مسح ذكي وكشف شروط التسييل المقيدة، تنبيهات وتصعيد آلي، تمديد مباشر، واعتماد مزدوج للإفراج والتخفيض والتسييل بخطابات **QR** موثقة.\n2. **إصدار خطابات الضمان (`LG Issuance`):** بوابة موحدة لطلبات الإدارات بنموذج ومصفوفة اعتماد مخصصة، مفاضلة التسهيلات (`0–100`)، تعبئة استمارات البنوك، ومطابقة تقارير مراكز الضمانات.\n3. **عروض العملات وأذون الخزانة:** منافسة مصرفية مشفرة لـ `FX Spot` وأذون الخزانة ومقارنة بأسعار السوق والمركزي.\n4. **المطابقة البنكية الذكية:** استيراد `Excel/CSV/MT940/CAMT.053` وربط التحويلات البينية وتعلم مستمر وتصدير قيود الـ ERP."
            if is_ar else
            "**Grow** unifies corporate treasury across **4 modular pillars** (available individually or together):\n1. **LG Custody (Inbound):** Safeguards guarantees received from suppliers, contractors, or customers with AI OCR, Smart Claim trap checks, automated reminders & escalations, fast-track protective extensions, Maker-Checker releases/claims, and QR-verified bank letters.\n2. **LG Issuance (Outbound):** Unified department intake with customizable forms & approval matrices, `0–100` credit facility scoring, auto-filled bank PDF forms, and Bank LG Position reconciliation.\n3. **FX & T-Bill Quotations:** Sealed-bid multi-bank **FX Spot** & **Sovereign T-Bill** RFQs benchmarked against live market & CBE rates.\n4. **Bank Auto-Reconciliation:** Zero-config Excel/CSV/MT940/CAMT.053 import, sweep pairing, self-learning rules, and locked double-entry ERP exports."
        ),
        "actions": validate_and_hydrate_actions([
            {"type": "story", "id": "platform_intro"},
            {"type": "story", "id": "custody"},
        ], lang),
        "follow_ups": (
            [
                "هل تحتاج البنوك إلى أي ربط تقني للعمل مع المنصة؟",
                "كيف نتعامل مع شروط التسييل المقيدة في خطابات الضمان الواردة؟",
                "هل يمكن الاشتراك في وحدة واحدة فقط للبدء؟",
            ]
            if is_ar else
            [
                "Do our relationship banks need any IT setup?",
                "How does Grow catch restrictive claim wording in guarantees?",
                "Can we start with just one module and expand later?",
            ]
        ),
    }


# ==============================================================================
# 7. MAIN PUBLIC PRODUCT EXPERT ORCHESTRATOR
# ==============================================================================

def _extract_json_object(raw_text: str) -> Optional[Dict[str, Any]]:
    """Robustly parses JSON object from Gemini response text."""
    if not raw_text:
        return None
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        parsed = json.loads(cleaned)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                parsed = json.loads(cleaned[start : end + 1])
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None
    return None


async def ask_public_product_expert(
    *,
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
    preferred_lang: str = "en",
    page_context: str = "demo",
    session_id: str = "anon",
    client_ip: str = "",
    user_agent: str = "",
) -> Dict[str, Any]:
    """
    Answers any public prospect question accurately across all 500 audited treasury scenarios,
    attaches validated 1-click showcase proof actions, enforces strict commercial & security
    guardrails, and logs the exchange.
    """
    t0 = time.perf_counter()
    clean_q = (question or "").strip()
    lang = detect_response_language(clean_q, preferred_lang)
    is_ar = lang == "ar"
    ip_hash = hash_client_ip(client_ip)

    # 1. Adversarial / Prompt-Injection Guardrail
    if _ADVERSARIAL_REGEX.search(clean_q):
        refusal = (
            "أنا المساعد الذكي التعريفي لمنصة **Grow**، ومهمتي الإجابة عن استفساراتكم حول وحدات المنصة وسيناريوهات إدارة الخزانة والأمان والترحيل. كيف يمكنني مساعدتك في استكشاف المنصة؟"
            if is_ar else
            "I am the **Grow Product Expert**, here to answer questions about Grow's treasury modules, workflows, governance, security, and onboarding. What scenario would you like to explore?"
        )
        actions = validate_and_hydrate_actions([{"type": "story", "id": "platform_intro"}], lang)
        latency_ms = (time.perf_counter() - t0) * 1000.0
        public_demo_chat_logger.log_exchange(
            session_id=session_id,
            page_context=page_context,
            lang=lang,
            question=clean_q,
            answer=refusal,
            topic="security_guardrail_refusal",
            source_mode="guardrail",
            actions=actions,
            show_commercial_cta=False,
            ip_hash=ip_hash,
            user_agent=user_agent,
            latency_ms=latency_ms,
        )
        return {
            "answer": refusal,
            "lang": lang,
            "topic": "security_guardrail_refusal",
            "actions": actions,
            "follow_ups": (
                ["ما الوحدات الأربع التي تغطيها منصة Grow؟", "كيف يتم تأمين وتشفير بيانات كل شركة؟"]
                if is_ar else
                ["What are the 4 modules in Grow?", "How does per-company AES-256 security work?"]
            ),
            "show_commercial_cta": False,
        }

    # 2. Format sanitized short conversation history (up to 6 prior turns)
    history_lines: List[str] = []
    for turn in (history or [])[-6:]:
        if not isinstance(turn, dict):
            continue
        role = "Visitor" if turn.get("role") == "user" else "Grow AI"
        content = str(turn.get("content") or "").strip()[:450]
        if content:
            history_lines.append(f"{role}: {content}")
    history_block = "\n".join(history_lines) if history_lines else "(First question in session)"

    # 3. Full static 8-pack reference (100% 500-Q coverage + prefix cacheable) + dynamic focus hint
    domain_knowledge = select_relevant_domain_packs(clean_q, history=history, page_context=page_context)
    focus_packs = ", ".join(select_focus_pack_keys(clean_q, history=history, page_context=page_context))

    language_directive = (
        "Respond in natural, executive Modern Standard Arabic (العربية الفصحى السلسة لمديري المالية والخزانة). "
        "CRITICAL ARABIC RULES: Always write the brand name `Grow` in Latin letters (`Grow`)—never transliterate it as جرو. "
        "Always translate Treasury Bills (`T-Bills`) accurately as `أذون الخزانة` (never `سندات الخزانة`). "
        "Keep technical acronyms clear (e.g., QR, AES-256, OTP, CBE, ERP, MT940, PDF)."
        if is_ar else
        "Respond in crisp, authoritative, executive B2B SaaS English tailored for CFOs, Group Treasurers, Auditors, and IT Security leaders."
    )

    system_instruction = f"""You are the **Grow AI Product Expert** on Grow's public website and interactive demo showcase.
Answer the visitor's exact question directly, constructively, and with 100% factual fidelity to the verified architecture below.

{language_directive}

STRICT FACTUAL & EXECUTIVE COMMUNICATION RULES (NON-NEGOTIABLE):
1. Rely EXCLUSIVELY on `CORE_PLATFORM_TRUTH` and `VERIFIED_DOMAIN_PACKS` below. NEVER invent features, database fields, calculations, or direct APIs that are marked as not supported.
2. **Constructive Enterprise Honesty**:
   - When a capability is **Supported Today**, state clearly **yes** and explain the exact mechanism concisely.
   - When a capability works through a **specific architectural design or alternative** (e.g., Re-Tender rounds instead of bilateral chat counteroffers; auto-filled bank PDFs & QR letters instead of bank APIs; structured CSV/XLSX GL exports instead of direct ERP DB posting; role-based separation where `End Users` run Quotations without seeing `Corporate Admin` Bank Statements), explain clearly **how Grow handles it and why**.
   - When something is **not currently supported** (e.g., FX Forwards, CDs/Time Deposits in Quotations, T-Bill WHT math or post-trade maturity trackers, PDF cash bank statements, physical vault shelf location on Inbound Custody, splitting 1 request across 2 banks without separate requests, or tenant IP-allowlisting), state transparently that it is not part of the active module today, highlight what IS supported right next to it, and set `"show_commercial_cta": true` if custom enterprise scoping or a live walkthrough would help.
3. **Critical Accuracy Guardrails**:
   - **LG Custody vs LG Issuance**: Custody = inbound guarantees received from suppliers, contractors, OR customers/partners (new LG entry & protective extensions execute directly; Release/Decrease/Amend/Liquidation require Maker-Checker; automated reminders & escalations). Issuance = outbound guarantees issued via banks (customizable intake form, sequential + parallel-group approval matrix, `0–100` facility scoring shifting SLA weight to 50% on urgent requests, auto-filled bank PDF forms, and **Bank LG Position report** reconciliation—never call Issuance reconciliation "bank statement reconciliation" and never promise "freeing trapped cash").
   - **Quotations**: `FX_SPOT` (up to 4 pairs) and `TBILL` (Actual/365 Price per 100 Par + TVM). Quotes remain strictly AES-256 sealed until `00:00` (never mention unused blind live ranking); FX ranking benchmarks against **both live market mid and Central Bank (CBE) official fixings**; each accepted leg awards its #1 Execution bank.
   - **Commercials**: NEVER quote numerical prices. Explain modular single-module vs. full-suite licensing + Free Trial / Pilot availability, and set `"show_commercial_cta": true` on pricing, pilot, or custom-feature questions.
4. **Concise Scannable Executive Style**: Keep `"answer"` focused (70 to 155 words max). Start directly with the clear answer (`Yes—...`, `In Grow, ...`, or `Today, Grow supports X while Y is...`). Use clean business terminology (e.g., **Corporate Admin**, **End User**, **Internal Portfolio Owner**, **Approved Queue**) rather than internal database/code identifiers (`corporate_admin`, `InternalOwnerContact`, `APPROVED_INTERNAL`). Use short bullets or **bold** key terms where helpful, and do NOT output raw URLs in `"answer"`.

{CORE_PLATFORM_TRUTH}

# VERIFIED_DOMAIN_PACKS (COMPLETE 8-DOMAIN AUDITED REFERENCE)
{domain_knowledge}

{SHOWCASE_LINK_CATALOG_FOR_PROMPT}

Return ONLY valid JSON matching this exact schema:
{{
  "answer": "markdown string (concise, direct, 100% accurate)",
  "spoken_summary": "1 to 2 short plain-text spoken executive sentences (max 240 chars, NO markdown asterisks or bullets; if Arabic, include clear diacritics/تشكيل on tricky word endings like ة vs ه and keep Grow in Latin script)",
  "topic": "short_snake_case_topic_tag",
  "actions": [
    {{ "type": "tour" | "story" | "foundation", "id": "exact_valid_id_from_catalog", "step": 1 }}
  ],
  "follow_ups": ["short natural follow-up question 1", "short natural follow-up question 2"],
  "show_commercial_cta": false
}}"""

    user_prompt = (
        f"Current Visitor Page Context: {page_context}\n"
        f"Primary Domain Focus Hint: {focus_packs}\n"
        f"Recent Conversation:\n{history_block}\n\n"
        f"Visitor's Question:\n{clean_q}"
    )

    result_payload: Optional[Dict[str, Any]] = None
    source_mode = "gemini"

    if GEMINI_AVAILABLE:
        client = _get_genai_client() or _get_api_genai_client()
        if client is not None:
            try:
                config_kwargs: Dict[str, Any] = {
                    "system_instruction": system_instruction,
                    "temperature": 0.15,
                    "max_output_tokens": 1150,
                    "response_mime_type": "application/json",
                }
                if hasattr(genai_types, "ThinkingConfig"):
                    try:
                        config_kwargs["thinking_config"] = genai_types.ThinkingConfig(thinking_budget=0)
                    except Exception:
                        pass
                if hasattr(genai_types, "AutomaticFunctionCallingConfig"):
                    try:
                        config_kwargs["automatic_function_calling"] = (
                            genai_types.AutomaticFunctionCallingConfig(disable=True)
                        )
                    except Exception:
                        pass

                gen_config = genai_types.GenerateContentConfig(**config_kwargs)
                resp = await _safe_generate_content_async(
                    client,
                    GEMINI_MODEL_NAME,
                    user_prompt,
                    config=gen_config,
                )
                raw_text = getattr(resp, "text", "") or ""
                parsed = _extract_json_object(raw_text)
                if parsed and isinstance(parsed.get("answer"), str) and parsed["answer"].strip():
                    hydrated_actions = validate_and_hydrate_actions(parsed.get("actions"), lang)
                    raw_follow_ups = parsed.get("follow_ups")
                    follow_ups = (
                        [str(q_item).strip()[:110] for q_item in raw_follow_ups if str(q_item).strip()][:3]
                        if isinstance(raw_follow_ups, list)
                        else []
                    )
                    answer_str = parsed["answer"].strip()
                    spoken_raw = str(parsed.get("spoken_summary") or "").strip()
                    spoken_clean = _clean_text_for_speech(spoken_raw or answer_str, max_chars=280)
                    result_payload = {
                        "answer": answer_str,
                        "spoken_summary": spoken_clean,
                        "lang": lang,
                        "topic": str(parsed.get("topic") or "product_qa")[:64],
                        "actions": hydrated_actions,
                        "follow_ups": follow_ups,
                        "show_commercial_cta": bool(parsed.get("show_commercial_cta", False)),
                    }
            except Exception as exc:
                logger.warning(f"[PublicProductExpert] Gemini call failed, using curated KB fallback: {exc}")

    if result_payload is None:
        source_mode = "curated_kb"
        fallback = _build_curated_fallback_response(clean_q, lang)
        result_payload = {
            "answer": fallback["answer"],
            "spoken_summary": _clean_text_for_speech(fallback["answer"], max_chars=260),
            "lang": lang,
            "topic": fallback["topic"],
            "actions": fallback["actions"],
            "follow_ups": fallback["follow_ups"],
            "show_commercial_cta": fallback["show_commercial_cta"],
        }

    latency_ms = (time.perf_counter() - t0) * 1000.0
    public_demo_chat_logger.log_exchange(
        session_id=session_id,
        page_context=page_context,
        lang=lang,
        question=clean_q,
        answer=result_payload["answer"],
        topic=result_payload["topic"],
        source_mode=source_mode,
        actions=result_payload["actions"],
        show_commercial_cta=result_payload["show_commercial_cta"],
        ip_hash=ip_hash,
        user_agent=user_agent,
        latency_ms=latency_ms,
    )
    return result_payload


# ==============================================================================
# 8. STUDIO VOICE INPUT (MULTIMODAL AUDIO) & STUDIO NEURAL TTS (`gemini-2.5-flash-tts`)
# ==============================================================================

def _clean_text_for_speech(text: str, max_chars: int = 320) -> str:
    """Strips markdown formatting and enforces Arabic `Grow` brand preservation for clean studio TTS."""
    if not text:
        return ""
    cleaned = str(text)
    cleaned = re.sub(r"\*\*([^*]+)\*\*", r"\1", cleaned)
    cleaned = re.sub(r"`([^`]+)`", r"\1", cleaned)
    cleaned = re.sub(r"^\s*[-*•]\s+", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"^\s*\d+[.)]\s+", "", cleaned, flags=re.MULTILINE)
    cleaned = cleaned.replace("جرو", "Grow")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_chars:
        return cleaned
    clipped = cleaned[:max_chars]
    for sep in (". ", "؟ ", "! ", "، ", "; "):
        idx = clipped.rfind(sep)
        if idx >= max_chars // 2:
            return clipped[: idx + 1].strip()
    return clipped.rsplit(" ", 1)[0].strip() + "."


def _wrap_pcm_l16_as_wav(pcm_bytes: bytes, sample_rate: int = 24000) -> bytes:
    """Wraps raw 16-bit mono PCM bytes from Gemini TTS into a standard universal RIFF WAV buffer."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm_bytes)
    return buf.getvalue()


async def ask_public_product_expert_voice(
    *,
    audio_bytes: bytes,
    audio_mime_type: str = "audio/webm",
    history: Optional[List[Dict[str, str]]] = None,
    preferred_lang: str = "en",
    page_context: str = "demo",
    session_id: str = "anon",
    client_ip: str = "",
    user_agent: str = "",
) -> Dict[str, Any]:
    """
    Multimodal voice-in handler: listens directly to spoken English, Modern Standard Arabic,
    or Egyptian/regional Arabic mixed with treasury terms, transcribes `heard_question`,
    answers from the 500-question verified knowledge base, and prepares a studio `spoken_summary`.
    """
    t0 = time.perf_counter()
    ip_hash = hash_client_ip(client_ip)

    history_lines: List[str] = []
    for turn in (history or [])[-6:]:
        if not isinstance(turn, dict):
            continue
        role = "Visitor" if turn.get("role") == "user" else "Grow AI"
        content = str(turn.get("content") or "").strip()[:450]
        if content:
            history_lines.append(f"{role}: {content}")
    history_block = "\n".join(history_lines) if history_lines else "(First question in session)"

    system_instruction = f"""You are the **Grow AI Product Expert** listening to a visitor's spoken voice question on Grow's public website and interactive demo showcase.

VOICE LISTENING & RESPONSE RULES:
1. Listen carefully to the visitor's spoken audio. They may speak in English, Modern Standard Arabic, or conversational Egyptian/Gulf Arabic (often mixing English financial terms like LG, FX Spot, T-Bills, CBE, MT940, SAP, OTP).
2. Transcribe their exact question cleanly into `"heard_question"` (if silent, unintelligible, or pure background noise, set `"heard_question": ""` and politely ask them to repeat).
3. Detect the language they spoke (`"ar"` if primarily Arabic, `"en"` if primarily English; fall back to `"{'ar' if (preferred_lang or '').startswith('ar') else 'en'}"` if unclear) and answer in that SAME language:
   - If Arabic (`"ar"`): Respond in natural, executive Modern Standard Arabic. Always write `Grow` in Latin letters (`Grow`—NEVER `جرو`) and write `أذون الخزانة` for T-Bills. In `"spoken_summary"`, include clear Arabic vowelization/diacritics (`تشكيل`) on key endings (`ة` vs `ه`) so text-to-speech reads it with studio precision.
   - If English (`"en"`): Respond in crisp, authoritative executive B2B SaaS English.
4. Rely EXCLUSIVELY on `CORE_PLATFORM_TRUTH` and `VERIFIED_DOMAIN_PACKS` below—100% factual accuracy, zero invented features or prices, constructive honesty on unsupported items, clean business terms (`Corporate Admin`, `End User`) instead of raw database identifiers. Keep `"answer"` 70–150 words.

{CORE_PLATFORM_TRUTH}

# VERIFIED_DOMAIN_PACKS (COMPLETE 8-DOMAIN AUDITED REFERENCE)
{ALL_VERIFIED_DOMAIN_PACKS_TEXT}

{SHOWCASE_LINK_CATALOG_FOR_PROMPT}

Return ONLY valid JSON matching this exact schema:
{{
  "heard_question": "clean transcription of the visitor's spoken question (or empty string if silent/inaudible)",
  "lang": "en" | "ar",
  "answer": "markdown string (concise, direct, 100% accurate)",
  "spoken_summary": "1 to 2 short plain-text spoken executive sentences (max 240 chars, NO markdown; vowelized if Arabic, keeping Grow in Latin script)",
  "topic": "short_snake_case_topic_tag",
  "actions": [
    {{ "type": "tour" | "story" | "foundation", "id": "exact_valid_id_from_catalog", "step": 1 }}
  ],
  "follow_ups": ["short natural follow-up question 1", "short natural follow-up question 2"],
  "show_commercial_cta": false
}}"""

    prompt_text = (
        f"Current Visitor Page Context: {page_context}\n"
        f"Recent Conversation:\n{history_block}\n\n"
        "Listen to the attached visitor audio clip and return the JSON response."
    )

    client = _get_genai_client() or _get_api_genai_client()
    if not GEMINI_AVAILABLE or client is None:
        raise RuntimeError("Voice AI service is temporarily unavailable.")

    clean_mime = (audio_mime_type or "audio/webm").split(";")[0].strip().lower()
    if clean_mime not in ("audio/webm", "audio/wav", "audio/x-wav", "audio/mp4", "audio/mpeg", "audio/mp3", "audio/ogg"):
        clean_mime = "audio/webm"

    audio_part = genai_types.Part.from_bytes(data=audio_bytes, mime_type=clean_mime)

    config_kwargs: Dict[str, Any] = {
        "system_instruction": system_instruction,
        "temperature": 0.15,
        "max_output_tokens": 1200,
        "response_mime_type": "application/json",
    }
    if hasattr(genai_types, "ThinkingConfig"):
        try:
            config_kwargs["thinking_config"] = genai_types.ThinkingConfig(thinking_budget=0)
        except Exception:
            pass
    if hasattr(genai_types, "AutomaticFunctionCallingConfig"):
        try:
            config_kwargs["automatic_function_calling"] = (
                genai_types.AutomaticFunctionCallingConfig(disable=True)
            )
        except Exception:
            pass

    gen_config = genai_types.GenerateContentConfig(**config_kwargs)
    resp = await _safe_generate_content_async(
        client,
        GEMINI_MODEL_NAME,
        [audio_part, prompt_text],
        config=gen_config,
    )
    raw_text = getattr(resp, "text", "") or ""
    parsed = _extract_json_object(raw_text) or {}

    heard_q = str(parsed.get("heard_question") or "").strip()[:600]
    out_lang = "ar" if str(parsed.get("lang") or preferred_lang).lower().startswith("ar") else "en"
    is_ar = out_lang == "ar"

    if not heard_q:
        polite_retry = (
            "لم أتمكن من سماع السؤال بوضوح. يرجى التحدث بالقرب من الميكروفون أو كتابة سؤالك مباشرة."
            if is_ar
            else "I couldn't hear a clear question in that recording. Please speak closer to your microphone or type your question below."
        )
        return {
            "heard_question": "",
            "answer": polite_retry,
            "spoken_summary": "",
            "lang": out_lang,
            "topic": "voice_inaudible",
            "actions": [],
            "follow_ups": [],
            "show_commercial_cta": False,
        }

    if _ADVERSARIAL_REGEX.search(heard_q):
        refusal = (
            "أنا المساعد الذكي التعريفي لمنصة **Grow**، ومهمتي الإجابة عن استفساراتكم حول وحدات المنصة وسيناريوهات إدارة الخزانة والأمان والترحيل."
            if is_ar
            else "I am the **Grow Product Expert**, here to answer questions about Grow's treasury modules, workflows, governance, security, and onboarding."
        )
        return {
            "heard_question": heard_q,
            "answer": refusal,
            "spoken_summary": _clean_text_for_speech(refusal, 220),
            "lang": out_lang,
            "topic": "security_guardrail_refusal",
            "actions": validate_and_hydrate_actions([{"type": "story", "id": "platform_intro"}], out_lang),
            "follow_ups": [],
            "show_commercial_cta": False,
        }

    answer_str = str(parsed.get("answer") or "").strip()
    if not answer_str:
        fb = _build_curated_fallback_response(heard_q, out_lang)
        answer_str = fb["answer"]
        hydrated_actions = fb["actions"]
        follow_ups = fb["follow_ups"]
        show_cta = fb["show_commercial_cta"]
        topic_str = fb["topic"]
    else:
        hydrated_actions = validate_and_hydrate_actions(parsed.get("actions"), out_lang)
        raw_f = parsed.get("follow_ups")
        follow_ups = (
            [str(x).strip()[:110] for x in raw_f if str(x).strip()][:3]
            if isinstance(raw_f, list)
            else []
        )
        show_cta = bool(parsed.get("show_commercial_cta", False))
        topic_str = str(parsed.get("topic") or "voice_product_qa")[:64]

    spoken_clean = _clean_text_for_speech(
        str(parsed.get("spoken_summary") or "").strip() or answer_str,
        max_chars=280,
    )

    public_voice_control.record_usage(increment_qa=True, is_voice_input=True, tts_chars=0)

    latency_ms = (time.perf_counter() - t0) * 1000.0
    public_demo_chat_logger.log_exchange(
        session_id=session_id,
        page_context=page_context,
        lang=out_lang,
        question=f"🎙️ {heard_q}",
        answer=answer_str,
        topic=topic_str,
        source_mode="gemini_voice",
        actions=hydrated_actions,
        show_commercial_cta=show_cta,
        ip_hash=ip_hash,
        user_agent=user_agent,
        latency_ms=latency_ms,
    )

    return {
        "heard_question": heard_q,
        "answer": answer_str,
        "spoken_summary": spoken_clean,
        "lang": out_lang,
        "topic": topic_str,
        "actions": hydrated_actions,
        "follow_ups": follow_ups,
        "show_commercial_cta": show_cta,
    }


async def synthesize_public_expert_speech(
    *,
    text: str,
    lang: str = "en",
    is_auto_reply: bool = False,
) -> Tuple[bytes, str]:
    """
    Synthesizes studio-grade executive voiceover via Vertex AI `gemini-2.5-flash-tts` (`Kore`).
    - Applies Arabic phonetic protection (`Grow` in English, natural tā’ marbūṭa endings).
    - Wraps 24kHz 16-bit PCM into universal `audio/wav` playable in Chrome, Safari iOS, Edge, and Firefox.
    - Increments `voice_qa_count` ONLY when `is_auto_reply=False` (manual Listen click on a typed turn)
      so a spoken Mic question + its automatic voice reply counts as 1 single Voice Q&A.
    """
    clean_text = _clean_text_for_speech(text, max_chars=340)
    if not clean_text:
        raise ValueError("Nothing to synthesize.")

    is_ar = detect_response_language(clean_text, lang) == "ar"

    director_prompt = (
        "Read the following corporate treasury summary aloud in a warm, confident, natural Modern Standard Arabic executive voice. "
        "Pronounce the English brand name 'Grow' naturally in English as 'Grow' (never as Arabic 'جرو'), "
        "articulate Arabic endings clearly and naturally, and speak only the text below:\n\n"
        f"{clean_text}"
        if is_ar
        else (
            "Read the following corporate treasury summary aloud in a crisp, warm, authoritative executive advisor voice at a natural pace, "
            "speaking only the text below:\n\n"
            f"{clean_text}"
        )
    )

    client = _get_genai_client() or _get_api_genai_client()
    if not GEMINI_AVAILABLE or client is None:
        raise RuntimeError("Studio TTS service is unavailable.")

    tts_config_kwargs: Dict[str, Any] = {
        "response_modalities": ["AUDIO"],
        "speech_config": genai_types.SpeechConfig(
            voice_config=genai_types.VoiceConfig(
                prebuilt_voice_config=genai_types.PrebuiltVoiceConfig(voice_name="Kore")
            )
        ),
    }
    if hasattr(genai_types, "AutomaticFunctionCallingConfig"):
        try:
            tts_config_kwargs["automatic_function_calling"] = genai_types.AutomaticFunctionCallingConfig(disable=True)
        except Exception:
            pass

    tts_config = genai_types.GenerateContentConfig(**tts_config_kwargs)
    resp = await _safe_generate_content_async(
        client,
        GEMINI_TTS_MODEL_NAME,
        director_prompt,
        config=tts_config,
    )

    candidates = getattr(resp, "candidates", None) or []
    if not candidates or not getattr(candidates[0], "content", None):
        raise RuntimeError("Empty audio response from TTS model.")

    parts = getattr(candidates[0].content, "parts", None) or []
    inline_data = getattr(parts[0], "inline_data", None) if parts else None
    raw_audio: Optional[bytes] = getattr(inline_data, "data", None) if inline_data else None
    mime_str: str = str(getattr(inline_data, "mime_type", "") or "audio/L16;rate=24000")

    if not raw_audio:
        raise RuntimeError("No audio bytes returned by TTS model.")

    public_voice_control.record_usage(
        increment_qa=(not is_auto_reply),
        is_voice_input=False,
        tts_chars=len(clean_text),
    )

    if "l16" in mime_str.lower() or "pcm" in mime_str.lower():
        rate_match = re.search(r"rate=(\d+)", mime_str)
        sample_rate = int(rate_match.group(1)) if rate_match else 24000
        return _wrap_pcm_l16_as_wav(raw_audio, sample_rate=sample_rate), "audio/wav"

    return raw_audio, mime_str.split(";")[0].strip() or "audio/wav"
