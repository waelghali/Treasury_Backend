"""
End-to-End Live Capture & Asset Publisher for Module 04 (Bank Auto-Reconciliation):
  - Tour 4.1: recon-zero-config      (5 steps) -> shots/recon_4_1/
  - Tour 4.2: recon-sweeps-reversals (5 steps) -> shots/recon_4_2/
  - Tour 4.3: recon-rules-privacy    (5 steps) -> shots/recon_4_3/
  - Tour 4.4: recon-erp-export       (5 steps) -> shots/recon_4_4/

Strictly sanitized fictional entities only:
  - Customer 23: Apex Global Engineering & Contracting SAE
  - Banks: First National Corporate Bank (FNCB), Mediterranean Commercial Bank (MCB), Nile Sovereign Bank (NSB)
  - Counterparties: NorthStar Electromechanical Co., Delta Heavy Turbines Ltd.,
    New Administrative Urban Utilities Authority, Al-Nour Ready-Mix Concrete
"""
import asyncio
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import fitz  # PyMuPDF
from playwright.sync_api import sync_playwright
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from app.auth_v2.services import AuthService
from app.core.security import create_access_token
from app.database import SessionLocal
from app.models import models as m
from app.models import models_reconciliation_v2 as mr

VIEWPORT_W = 1440
VIEWPORT_H = 900

STAGE_DIR = os.path.join(ROOT_DIR, "scratch", "recon_tours_stage")
STAGE_DOCS = os.path.join(STAGE_DIR, "docs")
STAGE_SHOTS = os.path.join(STAGE_DIR, "shots")

PUBLIC_DEMO_DIR = os.path.join(ROOT_DIR, "frontend", "public", "demo")
PUBLIC_DOCS_DIR = os.path.join(PUBLIC_DEMO_DIR, "docs")
PUBLIC_SHOTS_DIR = os.path.join(PUBLIC_DEMO_DIR, "shots")
MANIFEST_PATH = os.path.join(
    ROOT_DIR, "frontend", "src", "components", "Showcase", "pilotManifest.json"
)


def ensure_dirs():
    for d in [STAGE_DOCS, STAGE_SHOTS, PUBLIC_DOCS_DIR, PUBLIC_SHOTS_DIR]:
        os.makedirs(d, exist_ok=True)
    for sub in ["recon_4_1", "recon_4_2", "recon_4_3", "recon_4_4"]:
        os.makedirs(os.path.join(STAGE_SHOTS, sub), exist_ok=True)
        os.makedirs(os.path.join(PUBLIC_SHOTS_DIR, sub), exist_ok=True)


def render_pdf_preview(pdf_path: str, png_path: str):
    doc = fitz.open(pdf_path)
    page = doc[0]
    pix = page.get_pixmap(matrix=fitz.Matrix(2.0, 2.0), alpha=False)
    pix.save(png_path)
    doc.close()


def _build_pdf_doc(pdf_path: str, badge_color, badge_text: str, title: str, subtitle: str, rows: list, footer_note: str):
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=36,
        rightMargin=36,
        topMargin=36,
        bottomMargin=36,
    )
    badge_style = ParagraphStyle(
        "Badge",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8.5,
        textColor=badge_color,
        spaceAfter=4,
    )
    h1_style = ParagraphStyle(
        "H1",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=15,
        textColor=colors.HexColor("#0f172a"),
        spaceAfter=4,
    )
    sub_style = ParagraphStyle(
        "Sub",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9.5,
        textColor=colors.HexColor("#475569"),
        spaceAfter=14,
    )
    foot_style = ParagraphStyle(
        "Foot",
        parent=styles["Normal"],
        fontName="Helvetica-Oblique",
        fontSize=8,
        textColor=colors.HexColor("#64748b"),
        spaceBefore=14,
    )
    story = [
        Paragraph(badge_text, badge_style),
        Paragraph(title, h1_style),
        Paragraph(subtitle, sub_style),
    ]
    tbl = Table(rows, repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 8.5),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LEFTPADDING", (0, 0), (-1, -1), 7),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(tbl)
    story.append(Spacer(1, 10))
    story.append(Paragraph(footer_note, foot_style))
    doc.build(story)


def generate_companion_pdfs():
    docs = [
        (
            "FNCB_Corporate_Operating_Nov2026.pdf",
            colors.HexColor("#b45309"),
            "FIRST NATIONAL CORPORATE BANK (FNCB)  •  CORPORATE OPERATING ACCOUNT",
            "Electronic Account Statement Extract — November 2026",
            "Account Holder: Apex Global Engineering & Contracting SAE  |  Account: 0042-881920-001 (EGP)  |  IBAN: EG420042000100000042881920001",
            [
                ["Booking Date", "Reference", "Statement Narrative", "Debit (EGP)", "Credit (EGP)", "Balance (EGP)"],
                ["2026-11-01", "OPEN-BAL", "OPENING LEDGER BALANCE VALIDATED", "-", "-", "24,500,000.00"],
                ["2026-11-03", "IPN-994021", "INSTAPAY COLLECTION NAUUA MILESTONE INV-2026-4081", "-", "4,850,000.00", "29,350,000.00"],
                ["2026-11-05", "SWP-77104", "RTGS INTER-BANK LIQUIDITY SWEEP TO MCB ACCT 0088-310400-009", "5,000,175.00", "-", "24,349,825.00"],
                ["2026-11-07", "ACH-88210", "ACH VENDOR SETTLEMENT DELTA HEAVY TURBINES BILL-2026-9920", "1,420,000.00", "-", "22,929,825.00"],
                ["2026-11-09", "LGC-408B", "OUTGOING LG ISSUANCE COMMISSION REF LG-APEX-2026-408A", "25,500.00", "-", "22,904,325.00"],
                ["2026-11-11", "WR-55019", "WIRE TRANSFER AL-NOUR READY-MIX CONCRETE PO-7741", "380,000.00", "-", "22,524,325.00"],
                ["2026-11-12", "REV-55019", "REVERSAL RETURNED WIRE BENEFICIARY IBAN CLOSED REF WR-55019", "-", "380,000.00", "22,904,325.00"],
                ["2026-11-14", "COL-88302", "INWARD SWIFT COLLECTION NORTHSTAR ELECTROMECHANICAL", "-", "1,249,850.00", "24,154,175.00"],
            ],
            "Continuity Verified: Opening Balance (24,500,000.00 EGP) + Credits (6,479,850.00 EGP) - Debits (6,825,675.00 EGP) = Closing Balance (24,154,175.00 EGP). Variance: 0.00 EGP.",
        ),
        (
            "Apex_InterBank_Sweep_Advice_Nov2026.pdf",
            colors.HexColor("#4338ca"),
            "GROW TREASURY RECONCILIATION ENGINE  •  CROSS-BANK SWEEP & ERP CLEARING ADVICE",
            "Automated Inter-Bank Sweep & Tolerance Match Verification",
            "Entity: Apex Global Engineering & Contracting SAE  |  Cycle: November 2026  |  Status: 100% Cleared",
            [
                ["Leg / Side", "Institution & Account", "Reference", "Signed Amount", "Recognized Variance"],
                ["Outflow (Debit)", "FNCB — 0042-881920-001", "SWP-77104", "-5,000,175.00 EGP", "175.00 EGP RTGS Corridor Fee"],
                ["Inflow (Credit)", "MCB — 0088-310400-009", "SWP-77104", "+5,000,000.00 EGP", "Paired 100% Principal"],
                ["Failed Outflow", "FNCB — 0042-881920-001", "WR-55019", "-380,000.00 EGP", "Netted to 0.00 EGP"],
                ["Return Reversal", "FNCB — 0042-881920-001", "REV-55019", "+380,000.00 EGP", "Self-Canceling Reversal"],
                ["ERP AR Invoice", "SAP Ref INV-2026-4082", "COL-88302", "+1,250,000.00 EGP", "150.00 EGP Bank Fee Write-Off"],
            ],
            "All reciprocal inter-bank transfers and same-reference returned wires are isolated from operating P&L cash-flow totals.",
        ),
        (
            "Grow_ZeroPII_Privacy_Architecture_Brief.pdf",
            colors.HexColor("#6d28d9"),
            "GROW PLATFORM SECURITY  •  FEDERATED TREASURY INTELLIGENCE",
            "Zero-PII Tokenization & Online Rule Induction Specification",
            "Tenant Isolation: Dedicated Per-Company AES-256-GCM Envelope Keys + K-Anonymity Consensus Gate",
            [
                ["Intelligence Layer", "What Stays Tenant-Private (AES-256)", "What Enters Federated Brain", "Promotion Gate"],
                ["1. Built-In Kernel", "Full statement & balances", "Bilingual banking rail detectors", "Pre-verified (95%+ conf)"],
                ["2. Online Learned Rules", "Vendor names, GL codes, amounts", "Never shared outside company", "Empirical precision tracking"],
                ["3. Counterparty Vault", "Encrypted names + ENT_TOK_ HMAC", "Never shared outside company", "Auto-learned on confirm"],
                ["4. Collaborative Brain", "Stripped of all names, IBANs, amounts", "Normalized syntactic n-gram only", ">= 3 independent companies"],
            ],
            "Zero customer names, account numbers, IBANs, invoice references, or transaction amounts ever leave the company's encrypted vault.",
        ),
        (
            "Apex_SAP_DoubleEntry_GL_Journal_Nov2026.pdf",
            colors.HexColor("#047857"),
            "APEX GLOBAL ENGINEERING & CONTRACTING SAE  •  ACCOUNTING EXPORT PACK",
            "Balanced Double-Entry ERP G/L Journal Posting Sheet (SAP S/4HANA FI)",
            "Profile: SAP S/4HANA FI Double-Entry  |  Bank G/L: 1010010 - Cash at Bank (FNCB)  |  Export Lock: ENABLED",
            [
                ["Posting Date", "Doc Ref", "G/L Account", "Debit (EGP)", "Credit (EGP)", "Line Narration"],
                ["2026-11-03", "IPN-994021", "1010010 - Cash at Bank (FNCB)", "4,850,000.00", "0.00", "NAUUA Milestone Collection"],
                ["2026-11-03", "IPN-994021", "1020100 - Trade Receivables", "0.00", "4,850,000.00", "Clear ERP INV-2026-4081"],
                ["2026-11-07", "ACH-88210", "2010100 - Subcontractor Payables", "1,420,000.00", "0.00", "Delta Heavy Turbines Settlement"],
                ["2026-11-07", "ACH-88210", "1010010 - Cash at Bank (FNCB)", "0.00", "1,420,000.00", "ACH Outward Settlement"],
                ["2026-11-14", "COL-88302", "1010010 - Cash at Bank (FNCB)", "1,249,850.00", "0.00", "NorthStar Net Collection"],
                ["2026-11-14", "COL-88302", "6020300 - Bank Corridor Charges", "150.00", "0.00", "Auto-Posted Wire Fee Variance"],
                ["2026-11-14", "COL-88302", "1020100 - Trade Receivables", "0.00", "1,250,000.00", "Clear ERP INV-2026-4082"],
            ],
            "Total Journal Debits: 7,520,000.00 EGP  |  Total Journal Credits: 7,520,000.00 EGP  |  Out-of-Balance Variance: 0.00 EGP.",
        ),
    ]
    for fname, bcol, btxt, title, sub, rows, foot in docs:
        pdf_p = os.path.join(STAGE_DOCS, fname)
        png_p = os.path.join(STAGE_DOCS, fname.replace(".pdf", "_preview.png"))
        _build_pdf_doc(pdf_p, bcol, btxt, title, sub, rows, foot)
        render_pdf_preview(pdf_p, png_p)


def api_json(method: str, path: str, token: str, payload=None):
    url = f"http://127.0.0.1:8000{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        print(f"  [api warn] {method} {path} -> {e.code}: {body[:260]}")
        return {}


def seed_reconciliation_demo_data():
    db = SessionLocal()
    try:
        cust_id = 23
        admin = (
            db.query(m.User)
            .filter(m.User.customer_id == cust_id, m.User.email == "admin@apexglobal.com")
            .first()
        )
        if not admin:
            admin = db.query(m.User).filter(m.User.customer_id == cust_id).first()

        egp = db.query(m.Currency).filter(m.Currency.iso_code == "EGP").first()
        fncb = db.query(m.Bank).filter(m.Bank.short_name == "FNCB").first()
        mcb = db.query(m.Bank).filter(m.Bank.short_name == "MCB").first()
        assert admin and egp and fncb and mcb, "Expected Customer 23 + FNCB + MCB + EGP to exist"

        # 1. Clean previous Customer 23 reconciliation statements, ERP rows, rules, counterparties, profiles
        old_stmts = db.query(mr.BankStatement).filter(mr.BankStatement.company_id == cust_id).all()
        old_stmt_ids = [s.id for s in old_stmts]
        if old_stmt_ids:
            old_txn_ids = [
                r[0]
                for r in db.query(mr.BankTransaction.id)
                .filter(mr.BankTransaction.statement_id.in_(old_stmt_ids))
                .all()
            ]
            if old_txn_ids:
                db.query(mr.BankTransaction).filter(mr.BankTransaction.id.in_(old_txn_ids)).update(
                    {"linked_txn_id": None, "companion_parent_id": None}, synchronize_session=False
                )
                db.query(mr.ReconciliationMatch).filter(
                    mr.ReconciliationMatch.bank_txn_id.in_(old_txn_ids)
                ).delete(synchronize_session=False)
                db.query(mr.InternalLedgerRecord).filter(
                    mr.InternalLedgerRecord.matched_bank_txn_id.in_(old_txn_ids)
                ).update({"matched_bank_txn_id": None}, synchronize_session=False)
                db.query(mr.MultiReference).filter(
                    mr.MultiReference.transaction_id.in_(old_txn_ids)
                ).delete(synchronize_session=False)
                db.query(mr.BankTransaction).filter(
                    mr.BankTransaction.statement_id.in_(old_stmt_ids)
                ).delete(synchronize_session=False)
            db.query(mr.BankStatement).filter(mr.BankStatement.company_id == cust_id).delete(
                synchronize_session=False
            )

        db.query(mr.InternalLedgerRecord).filter(mr.InternalLedgerRecord.company_id == cust_id).delete(
            synchronize_session=False
        )
        db.query(mr.ClassificationRule).filter(mr.ClassificationRule.company_id == cust_id).delete(
            synchronize_session=False
        )
        db.query(mr.Counterparty).filter(mr.Counterparty.company_id == cust_id).delete(
            synchronize_session=False
        )
        db.query(mr.ReconciliationExportProfile).filter(
            mr.ReconciliationExportProfile.company_id == cust_id
        ).delete(synchronize_session=False)
        db.commit()

        # Also sanitize any global Counterparty or CollaborativePattern that might mention real Egyptian banks
        real_bank_tokens = ["CIB", "HSBC", "QNB", "NBE", "ENBD", "BANQUE MISR", "AAIB", "ALEXBANK"]
        for gcp in db.query(mr.Counterparty).filter(mr.Counterparty.company_id.is_(None)).all():
            up = (gcp.name or "").upper()
            if any(tok in up for tok in real_bank_tokens):
                gcp.is_active = False
        for cp_pat in db.query(mr.CollaborativePattern).all():
            up = (cp_pat.signature_token or "").upper()
            if any(tok in up for tok in real_bank_tokens):
                cp_pat.is_promoted = False
        db.commit()

        # 2. Seed Taxonomy Classes & Subclasses for Customer 23 (if not already rich)
        existing_tax = (
            db.query(mr.ClassificationTaxonomy)
            .filter(mr.ClassificationTaxonomy.company_id == cust_id)
            .all()
        )
        for t in existing_tax:
            if t.parent_id is not None:
                db.delete(t)
        db.flush()
        for t in (
            db.query(mr.ClassificationTaxonomy)
            .filter(mr.ClassificationTaxonomy.company_id == cust_id)
            .all()
        ):
            db.delete(t)
        db.flush()

        cls_rev = mr.ClassificationTaxonomy(
            company_id=cust_id,
            code="PROJECT_REVENUE",
            name="Project & Milestone Collections",
            name_ar="تحصيلات المشروعات والمستخلصات",
            direction="CREDIT",
            default_gl_account="1020100 - Trade Receivables",
            order_index=1,
        )
        cls_ap = mr.ClassificationTaxonomy(
            company_id=cust_id,
            code="SUBCONTRACTOR_AP",
            name="Subcontractor & Turbine Procurement",
            name_ar="مدفوعات المقاولين والمعدات الكهروميكانيكية",
            direction="DEBIT",
            default_gl_account="2010100 - Subcontractor Payables",
            order_index=2,
        )
        cls_bank = mr.ClassificationTaxonomy(
            company_id=cust_id,
            code="TREASURY_FEES",
            name="Guarantee Commissions & Bank Corridor Fees",
            name_ar="عمولات خطابات الضمان والمصاريف البنكية",
            direction="DEBIT",
            default_gl_account="6020300 - LG Commissions & Bank Fees",
            order_index=3,
        )
        cls_sweep = mr.ClassificationTaxonomy(
            company_id=cust_id,
            code="LIQUIDITY_SWEEPS",
            name="Inter-Bank Liquidity Sweeps",
            name_ar="التحويلات البينية لإدارة السيولة",
            direction="EITHER",
            default_gl_account="1010900 - Inter-Bank Clearing",
            order_index=4,
        )
        db.add_all([cls_rev, cls_ap, cls_bank, cls_sweep])
        db.flush()

        db.add_all(
            [
                mr.ClassificationTaxonomy(
                    company_id=cust_id,
                    parent_id=cls_rev.id,
                    code="MILESTONE_AR",
                    name="Sovereign Infrastructure Progress Billing",
                    name_ar="مستخلصات مشروعات البنية التحتية",
                    direction="CREDIT",
                    default_gl_account="1020100 - Trade Receivables",
                ),
                mr.ClassificationTaxonomy(
                    company_id=cust_id,
                    parent_id=cls_ap.id,
                    code="ELECTROMECH_SUPPLY",
                    name="Electromechanical & Turbine Supply",
                    name_ar="توريدات التوربينات والكهروميكانيك",
                    direction="DEBIT",
                    default_gl_account="2010100 - Subcontractor Payables",
                ),
                mr.ClassificationTaxonomy(
                    company_id=cust_id,
                    parent_id=cls_bank.id,
                    code="OUTGOING_LG_COMM",
                    name="Outgoing LG Issuance & Quarter Commission",
                    name_ar="عمولات إصدار خطابات الضمان الصادرة",
                    direction="DEBIT",
                    default_gl_account="6020300 - LG Commissions & Bank Fees",
                ),
                mr.ClassificationTaxonomy(
                    company_id=cust_id,
                    parent_id=cls_sweep.id,
                    code="RTGS_CONCENTRATION",
                    name="RTGS Concentration Sweep",
                    name_ar="تجميع السيولة عبر نظام التسوية اللحظية",
                    direction="EITHER",
                    default_gl_account="1010900 - Inter-Bank Clearing",
                ),
            ]
        )
        db.flush()

        # 3. Seed Online-Learned, Self-Healed, and Manual Classification Rules
        r_learned = mr.ClassificationRule(
            company_id=cust_id,
            bank_id=fncb.id,
            rule_name="Auto-Learned: Delta Heavy Turbines ACH Settlement",
            rule_type="LEARNED_COMPOSITE",
            rule_origin="LEARNED_ONLINE",
            lifecycle_state="AUTO_CONFIRMED",
            priority=10,
            conditions_json={
                "conditions": [
                    {"field": "raw_description", "operator": "contains", "value": "DELTA HEAVY TURBINES", "joiner": "AND"},
                    {"field": "raw_description", "operator": "contains", "value": "ACH", "joiner": "AND"},
                ]
            },
            assigned_gl_account="2010100 - Subcontractor Payables",
            assigned_category="Subcontractor & Turbine Procurement",
            assigned_sub_category="Electromechanical & Turbine Supply",
            usage_count=14,
            confirmations_count=9,
            overrides_count=0,
            empirical_precision=Decimal("100.00"),
            calibrated_confidence=96,
            is_active=True,
            created_by=admin.id,
        )
        r_healed = mr.ClassificationRule(
            company_id=cust_id,
            bank_id=fncb.id,
            rule_name="Self-Healed: Outgoing LG Commission (Excludes Refund/Reversal)",
            rule_type="LEARNED_COMPOSITE",
            rule_origin="SYSTEM_ADAPTED",
            lifecycle_state="AUTO_CONFIRMED",
            priority=20,
            conditions_json={
                "conditions": [
                    {"field": "raw_description", "operator": "contains", "value": "LG ISSUANCE COMMISSION", "joiner": "AND"}
                ],
                "exclude_tokens": ["REFUND", "REVERSAL"],
                "amount_max": 150000,
            },
            assigned_gl_account="6020300 - LG Commissions & Bank Fees",
            assigned_category="Guarantee Commissions & Bank Corridor Fees",
            assigned_sub_category="Outgoing LG Issuance & Quarter Commission",
            usage_count=22,
            confirmations_count=15,
            overrides_count=1,
            empirical_precision=Decimal("95.65"),
            calibrated_confidence=93,
            is_active=True,
            created_by=admin.id,
        )
        r_manual = mr.ClassificationRule(
            company_id=cust_id,
            rule_name="Sovereign Authority Milestone Collections (NAUUA)",
            rule_type="LITERAL",
            rule_origin="MANUAL",
            lifecycle_state="AUTO_CONFIRMED",
            priority=30,
            conditions_json={
                "conditions": [
                    {"field": "raw_description", "operator": "contains", "value": "NAUUA MILESTONE", "joiner": "AND"}
                ]
            },
            assigned_gl_account="1020100 - Trade Receivables",
            assigned_category="Project & Milestone Collections",
            assigned_sub_category="Sovereign Infrastructure Progress Billing",
            usage_count=11,
            confirmations_count=11,
            overrides_count=0,
            empirical_precision=Decimal("100.00"),
            calibrated_confidence=98,
            is_active=True,
            created_by=admin.id,
        )
        db.add_all([r_learned, r_healed, r_manual])
        db.flush()

        # 4. Seed Sanitized AES-256 / Blind-Token Counterparty Registry
        db.add_all(
            [
                mr.Counterparty(
                    company_id=cust_id,
                    name="New Administrative Urban Utilities Authority",
                    blind_token="ENT_TOK_9f41c8a20b7e11d4",
                    aliases=["NAUUA", "URBAN UTILITIES AUTHORITY", "هيئة المرافق العمرانية"],
                    entity_type="CUSTOMER",
                    default_category="COLLECTION_FROM_CUSTOMER",
                    default_gl_account="1020100 - Trade Receivables",
                    learned_count=18,
                    is_verified=True,
                    is_active=True,
                    created_by=admin.id,
                ),
                mr.Counterparty(
                    company_id=cust_id,
                    name="Delta Heavy Turbines Ltd.",
                    blind_token="ENT_TOK_3d82b519c40a77e1",
                    aliases=["DELTA HEAVY TURBINES", "DELTA TURBINES SAE"],
                    entity_type="SUPPLIER",
                    default_category="PAYMENT_TO_SUPPLIER",
                    default_gl_account="2010100 - Subcontractor Payables",
                    learned_count=14,
                    is_verified=True,
                    is_active=True,
                    created_by=admin.id,
                ),
                mr.Counterparty(
                    company_id=cust_id,
                    name="NorthStar Electromechanical Co.",
                    blind_token="ENT_TOK_7a19e044f82c55b9",
                    aliases=["NORTHSTAR ELECTROMECHANICAL", "NORTHSTAR MEP"],
                    entity_type="CUSTOMER",
                    default_category="COLLECTION_FROM_CUSTOMER",
                    default_gl_account="1020100 - Trade Receivables",
                    learned_count=9,
                    is_verified=True,
                    is_active=True,
                    created_by=admin.id,
                ),
                mr.Counterparty(
                    company_id=cust_id,
                    name="Al-Nour Ready-Mix Concrete",
                    blind_token="ENT_TOK_5c60d128a93f04e2",
                    aliases=["AL-NOUR READY-MIX", "النور للخرسانة الجاهزة"],
                    entity_type="SUPPLIER",
                    default_category="PAYMENT_TO_SUPPLIER",
                    default_gl_account="2010100 - Subcontractor Payables",
                    learned_count=6,
                    is_verified=False,
                    is_active=True,
                    created_by=admin.id,
                ),
            ]
        )
        db.flush()

        # 5. Seed Zero-PII Promoted Collaborative Consensus Patterns
        collab_specs = [
            ("INSTAPAY CORPORATE COLLECTION SETTLEMENT", "CREDIT", "COLLECTION_FROM_CUSTOMER", "1020100 - Trade Receivables", None, 11, 48, 98),
            ("CBE RTGS INTERBANK CORRIDOR CHARGE", "DEBIT", "BANK_CHARGES", "6020300 - Bank Fees", fncb.id, 7, 31, 96),
            ("OUTGOING LG QUARTERLY COMMISSION", "DEBIT", "LG_COMMISSION", "6020300 - LG Commissions", fncb.id, 6, 27, 95),
            ("ACH CORPORATE PAYROLL BATCH DISBURSEMENT", "DEBIT", "SALARY_PAYROLL", "5010100 - Salaries & Wages", None, 9, 39, 97),
        ]
        for sig, direc, cat, gl, b_id, tenants_n, conf_tot, conf_pct in collab_specs:
            existing_p = (
                db.query(mr.CollaborativePattern)
                .filter(mr.CollaborativePattern.signature_token == sig)
                .first()
            )
            if not existing_p:
                db.add(
                    mr.CollaborativePattern(
                        bank_id=b_id,
                        signature_token=sig,
                        direction=direc,
                        amount_tier="ANY",
                        suggested_category=cat,
                        suggested_gl_account=gl,
                        distinct_tenants_count=tenants_n,
                        total_confirmations=conf_tot,
                        rejection_count=0,
                        agreement_score=Decimal("100.00"),
                        confidence_score=conf_pct,
                        is_promoted=True,
                    )
                )
            else:
                existing_p.bank_id = b_id
                existing_p.distinct_tenants_count = tenants_n
                existing_p.confidence_score = conf_pct
                existing_p.is_promoted = True
        db.flush()

        # 6. Seed 2 Validated Bank Statements: FNCB Operating & MCB Liquidity Concentration
        base_dt = datetime(2026, 11, 1, 9, 0, tzinfo=timezone.utc)
        stmt_fncb = mr.BankStatement(
            company_id=cust_id,
            bank_id=fncb.id,
            account_number="0042-881920-001",
            currency_id=egp.id,
            file_name="FNCB_Corporate_Operating_Nov2026.xlsx",
            opening_balance=Decimal("24500000.00"),
            closing_balance=Decimal("24154175.00"),
            statement_start_date=base_dt,
            statement_end_date=base_dt + timedelta(days=14),
            status="VALIDATED",
            balance_continuity_status="VALIDATED",
            balance_variance_amount=Decimal("0.00"),
            detected_mapping_json={
                "booking_date": "Value / Booking Date",
                "raw_description": "Transaction Narrative (AR/EN)",
                "debit_amount": "Withdrawal (EGP)",
                "credit_amount": "Deposit (EGP)",
                "running_balance": "Ledger Balance",
                "back_office_ref": "Bank Ref / E2E",
            },
            created_by=admin.id,
        )
        stmt_mcb = mr.BankStatement(
            company_id=cust_id,
            bank_id=mcb.id,
            account_number="0088-310400-009",
            currency_id=egp.id,
            file_name="MCB_Liquidity_Concentration_Nov2026.xlsx",
            opening_balance=Decimal("12000000.00"),
            closing_balance=Decimal("17000000.00"),
            statement_start_date=base_dt,
            statement_end_date=base_dt + timedelta(days=14),
            status="VALIDATED",
            balance_continuity_status="VALIDATED",
            balance_variance_amount=Decimal("0.00"),
            created_by=admin.id,
        )
        db.add_all([stmt_fncb, stmt_mcb])
        db.flush()

        # FNCB Statement Lines (initially unclassified on 1 line for + Classify demo, rich multi-tier on others!)
        t1 = mr.BankTransaction(
            statement_id=stmt_fncb.id,
            company_name="Apex Global HQ",
            account_number="0042-881920-001",
            booking_date=base_dt + timedelta(days=2),
            value_date=base_dt + timedelta(days=2),
            debit_amount=Decimal("0.00"),
            credit_amount=Decimal("4850000.00"),
            currency="EGP",
            running_balance=Decimal("29350000.00"),
            raw_description="INSTAPAY CORPORATE COLLECTION SETTLEMENT - NAUUA MILESTONE INV-2026-4081",
            description_line2="تحصيل مستخلص رقم ٤٠٨١ - هيئة المرافق العمرانية",
            counterparty_name="New Administrative Urban Utilities Authority",
            back_office_ref="IPN-994021",
            e2e_id="E2E-NAUUA-4081",
            is_classified=True,
            is_reconciled=True,
            applied_rule_id=r_manual.id,
            internal_category="1020100 - Trade Receivables",
            classification_category="Project & Milestone Collections",
            classification_source="RULE",
            classification_confidence=98,
            classification_reasoning="Matched Priority-30 Corporate Rule 'Sovereign Authority Milestone Collections (NAUUA)' + Verified Counterparty ENT_TOK_9f41c8a2.",
        )
        t2_sweep_out = mr.BankTransaction(
            statement_id=stmt_fncb.id,
            company_name="Apex Global HQ",
            account_number="0042-881920-001",
            booking_date=base_dt + timedelta(days=4),
            value_date=base_dt + timedelta(days=4),
            debit_amount=Decimal("5000175.00"),
            credit_amount=Decimal("0.00"),
            currency="EGP",
            running_balance=Decimal("24349825.00"),
            raw_description="CBE RTGS INTER-BANK LIQUIDITY SWEEP TO MCB CONCENTRATION ACCT 0088-310400-009",
            description_line2="تحويل سيولة بيني عبر نظام التسوية اللحظية شاملة عمولة المسار ١٧٥ جم",
            back_office_ref="SWP-77104",
            e2e_id="RTGS-SWP-77104",
            is_classified=True,
            is_reconciled=True,
            internal_category="INTER_BANK_SWEEP",
            classification_category="Inter-Bank Liquidity Sweeps",
            classification_source="BUILTIN",
            classification_confidence=99,
            variance_amount=Decimal("175.00"),
            classification_reasoning="Reciprocal cross-bank transfer paired against MCB Credit #SWP-77104 (+5,000,000.00 EGP); 175.00 EGP isolated as RTGS corridor fee.",
        )
        t3_ap = mr.BankTransaction(
            statement_id=stmt_fncb.id,
            company_name="Apex Global HQ",
            account_number="0042-881920-001",
            booking_date=base_dt + timedelta(days=6),
            value_date=base_dt + timedelta(days=6),
            debit_amount=Decimal("1420000.00"),
            credit_amount=Decimal("0.00"),
            currency="EGP",
            running_balance=Decimal("22929825.00"),
            raw_description="ACH VENDOR SETTLEMENT - DELTA HEAVY TURBINES LTD BILL-2026-9920",
            counterparty_name="Delta Heavy Turbines Ltd.",
            back_office_ref="ACH-88210",
            e2e_id="ACH-DHT-9920",
            is_classified=True,
            is_reconciled=True,
            applied_rule_id=r_learned.id,
            internal_category="2010100 - Subcontractor Payables",
            classification_category="Subcontractor & Turbine Procurement",
            classification_source="LEARNED_RULE",
            classification_confidence=96,
            classification_reasoning="Online-Learned Composite Rule P10 (100% empirical precision across 9 prior confirmations).",
        )
        t4_lg_comm = mr.BankTransaction(
            statement_id=stmt_fncb.id,
            company_name="Apex Global HQ",
            account_number="0042-881920-001",
            booking_date=base_dt + timedelta(days=8),
            value_date=base_dt + timedelta(days=8),
            debit_amount=Decimal("25500.00"),
            credit_amount=Decimal("0.00"),
            currency="EGP",
            running_balance=Decimal("22904325.00"),
            raw_description="OUTGOING LG ISSUANCE COMMISSION REF LG-APEX-2026-408A",
            description_line2="عمولة إصدار خطاب ضمان دفعة مقدمة صادر لصالح هيئة المرافق",
            back_office_ref="LGC-408B",
            is_classified=True,
            is_reconciled=False,
            internal_category="6020300 - LG Commissions & Bank Fees",
            classification_category="Guarantee Commissions & Bank Corridor Fees",
            classification_source="COLLABORATIVE",
            classification_confidence=95,
            classification_reasoning="Federated Zero-PII Consensus Signature 'OUTGOING LG QUARTERLY COMMISSION' validated by 6 independent companies.",
        )
        t5_wire_orig = mr.BankTransaction(
            statement_id=stmt_fncb.id,
            company_name="Apex Global HQ",
            account_number="0042-881920-001",
            booking_date=base_dt + timedelta(days=10),
            value_date=base_dt + timedelta(days=10),
            debit_amount=Decimal("380000.00"),
            credit_amount=Decimal("0.00"),
            currency="EGP",
            running_balance=Decimal("22524325.00"),
            raw_description="OUTWARD WIRE TRANSFER - AL-NOUR READY-MIX CONCRETE PO-7741",
            counterparty_name="Al-Nour Ready-Mix Concrete",
            back_office_ref="WR-55019",
            is_classified=True,
            is_reconciled=True,
            is_reversal=True,
            internal_category="REVERSAL_NETTED",
            classification_category="Self-Canceling Returned Wire",
            classification_source="BUILTIN",
            classification_confidence=99,
        )
        t6_wire_rev = mr.BankTransaction(
            statement_id=stmt_fncb.id,
            company_name="Apex Global HQ",
            account_number="0042-881920-001",
            booking_date=base_dt + timedelta(days=11),
            value_date=base_dt + timedelta(days=11),
            debit_amount=Decimal("0.00"),
            credit_amount=Decimal("380000.00"),
            currency="EGP",
            running_balance=Decimal("22904325.00"),
            raw_description="REVERSAL RETURNED WIRE - BENEFICIARY IBAN CLOSED REF WR-55019",
            description_line2="رد تحويل صادر لخطأ رقم الحساب البنكي للمستفيد",
            counterparty_name="Al-Nour Ready-Mix Concrete",
            back_office_ref="REV-55019",
            is_classified=True,
            is_reconciled=True,
            is_reversal=True,
            internal_category="REVERSAL_NETTED",
            classification_category="Self-Canceling Returned Wire",
            classification_source="BUILTIN",
            classification_confidence=99,
        )
        # Hero ERP Split-Match Candidate (1,249,850.00 net vs 1,250,000.00 ERP Invoice -> 150 EGP correspondent wire fee!)
        t7_northstar = mr.BankTransaction(
            statement_id=stmt_fncb.id,
            company_name="Apex Global HQ",
            account_number="0042-881920-001",
            booking_date=base_dt + timedelta(days=13),
            value_date=base_dt + timedelta(days=13),
            debit_amount=Decimal("0.00"),
            credit_amount=Decimal("1249850.00"),
            currency="EGP",
            running_balance=Decimal("24154175.00"),
            raw_description="INWARD SWIFT COLLECTION - NORTHSTAR ELECTROMECHANICAL INV-2026-4082 (NET OF 150 EGP CORR FEE)",
            counterparty_name="NorthStar Electromechanical Co.",
            back_office_ref="COL-88302",
            e2e_id="SWF-NS-4082",
            is_classified=False,  # Left unclassified initially so `+ Classify` button is visible for Tour 4.3 Step 1!
            is_reconciled=False,
        )

        # MCB Inflow Sweep Counterpart
        t_mcb_sweep_in = mr.BankTransaction(
            statement_id=stmt_mcb.id,
            company_name="Apex Global HQ",
            account_number="0088-310400-009",
            booking_date=base_dt + timedelta(days=4),
            value_date=base_dt + timedelta(days=4),
            debit_amount=Decimal("0.00"),
            credit_amount=Decimal("5000000.00"),
            currency="EGP",
            running_balance=Decimal("17000000.00"),
            raw_description="INWARD RTGS LIQUIDITY CONCENTRATION SWEEP FROM FNCB ACCT 0042-881920-001",
            back_office_ref="SWP-77104",
            e2e_id="RTGS-SWP-77104",
            is_classified=True,
            is_reconciled=True,
            internal_category="INTER_BANK_SWEEP",
            classification_category="Inter-Bank Liquidity Sweeps",
            classification_source="BUILTIN",
            classification_confidence=99,
            variance_amount=Decimal("175.00"),
        )

        db.add_all([t1, t2_sweep_out, t3_ap, t4_lg_comm, t5_wire_orig, t6_wire_rev, t7_northstar, t_mcb_sweep_in])
        db.flush()

        # Link sweep pair & reversal pair bidirectionally!
        t2_sweep_out.linked_txn_id = t_mcb_sweep_in.id
        t_mcb_sweep_in.linked_txn_id = t2_sweep_out.id
        t5_wire_orig.linked_txn_id = t6_wire_rev.id
        t6_wire_rev.linked_txn_id = t5_wire_orig.id
        db.flush()

        # 7. Seed Open ERP Ledger Records for Split-Pane Matching!
        erp_ns = mr.InternalLedgerRecord(
            company_id=cust_id,
            record_type="AR_INVOICE",
            reference_number="INV-2026-4082",
            entity_name="NorthStar Electromechanical Co.",
            record_date=base_dt + timedelta(days=12),
            amount=Decimal("1250000.00"),
            currency="EGP",
            gl_account="1020100 - Trade Receivables",
            bank_id=fncb.id,
            bank_name="First National Corporate Bank (FNCB)",
            bank_account_number="0042-881920-001",
            status="OPEN",
            created_by=admin.id,
        )
        erp_lgc = mr.InternalLedgerRecord(
            company_id=cust_id,
            record_type="LG_COMMISSION",
            reference_number="LGC-2026-408A",
            entity_name="First National Corporate Bank (FNCB)",
            record_date=base_dt + timedelta(days=8),
            amount=Decimal("-25500.00"),
            currency="EGP",
            gl_account="6020300 - LG Commissions & Bank Fees",
            bank_id=fncb.id,
            bank_name="First National Corporate Bank (FNCB)",
            bank_account_number="0042-881920-001",
            status="OPEN",
            created_by=admin.id,
        )
        erp_ap_open = mr.InternalLedgerRecord(
            company_id=cust_id,
            record_type="AP_BILL",
            reference_number="BILL-2026-9954",
            entity_name="Al-Nour Ready-Mix Concrete",
            record_date=base_dt + timedelta(days=10),
            amount=Decimal("-380000.00"),
            currency="EGP",
            gl_account="2010100 - Subcontractor Payables",
            bank_id=fncb.id,
            bank_name="First National Corporate Bank (FNCB)",
            bank_account_number="0042-881920-001",
            status="OPEN",
            created_by=admin.id,
        )
        db.add_all([erp_ns, erp_lgc, erp_ap_open])
        db.flush()

        # 8. Seed Customizable Double-Entry Export Profile (`SAP S/4HANA FI Double-Entry`)
        exp_prof = mr.ReconciliationExportProfile(
            company_id=cust_id,
            profile_name="SAP S/4HANA FI Double-Entry Journal",
            description="Balanced Bank + Offset + Corridor Fee Variance GL posting layout",
            file_format="XLSX",
            columns_config_json=[
                {"source_field": "booking_date", "custom_header": "Posting Date"},
                {"source_field": "back_office_ref", "custom_header": "Bank Doc Ref"},
                {"source_field": "internal_category", "custom_header": "Target G/L Account"},
                {"source_field": "debit_amount", "custom_header": "Debit (EGP)"},
                {"source_field": "credit_amount", "custom_header": "Credit (EGP)"},
                {"source_field": "counterparty_name", "custom_header": "Counterparty"},
                {"source_field": "matched_erp_reference", "custom_header": "ERP Document"},
                {"source_field": "variance_amount", "custom_header": "Fee Variance"},
            ],
            amount_mode="SPLIT_DEBIT_CREDIT",
            date_format="%Y-%m-%d",
            expand_journal_legs=True,
            bank_gl_account="1010010 - Cash at Bank (FNCB Operating)",
            is_default=True,
            is_active=True,
            created_by=admin.id,
        )
        db.add(exp_prof)
        db.commit()

        auth_service = AuthService()
        admin_td = asyncio.run(auth_service.build_user_token_data(db, admin))
        admin_td.update(
            {
                "must_change_password": False,
                "must_accept_policies": False,
                "last_accepted_legal_version": 99.0,
                "is_mfa_verified": True,
                "has_reconciliation_module": True,
                "has_custody_module": True,
                "has_issuance_module": True,
                "has_quotation_module": True,
            }
        )
        admin_jwt = create_access_token(admin_td, expires_delta=timedelta(hours=4))

        return {
            "cust_id": cust_id,
            "admin_id": admin.id,
            "admin_jwt": admin_jwt,
            "fncb_id": fncb.id,
            "mcb_id": mcb.id,
            "stmt_fncb_id": stmt_fncb.id,
            "stmt_mcb_id": stmt_mcb.id,
            "t1_id": t1.id,
            "t2_sweep_id": t2_sweep_out.id,
            "t7_northstar_id": t7_northstar.id,
            "erp_ns_id": erp_ns.id,
            "exp_prof_id": exp_prof.id,
        }
    finally:
        db.close()


def set_browser_auth(page, token: str, role: str, customer_id: int):
    now_ms = str(int(time.time() * 1000))
    page.goto("http://localhost:3000/login", wait_until="domcontentloaded")
    page.evaluate(
        """([tk, rl, cid, nowMs]) => {
            localStorage.setItem('jwt_token', tk);
            localStorage.setItem('token', tk);
            localStorage.setItem('user_role', rl);
            localStorage.setItem('customer_id', String(cid));
            localStorage.setItem('last_activity_at', nowMs);
            localStorage.setItem('last_token_refresh_at', nowMs);
            localStorage.setItem('reconcile_density', 'normal');
            localStorage.setItem('reconcile_wrap_text', 'true');
        }""",
        [token, role, customer_id, now_ms],
    )


def hide_overlays(page):
    page.add_style_tag(
        content="""
        .Toastify, [class*="Toastify"], div.fixed.bottom-4.right-4 {
            display: none !important;
            opacity: 0 !important;
            pointer-events: none !important;
        }
        """
    )


def measure_box_pct(page, locator, pad: int = 8, fallback=None):
    try:
        if locator.count() > 0:
            el = locator.first
            if el.is_visible():
                box = el.bounding_box()
                if box and box["width"] > 8 and box["height"] > 8:
                    x = max(0.5, (box["x"] - pad) / VIEWPORT_W * 100.0)
                    y = max(0.5, (box["y"] - pad) / VIEWPORT_H * 100.0)
                    w = min(98.0 - x, (box["width"] + 2 * pad) / VIEWPORT_W * 100.0)
                    h = min(98.0 - y, (box["height"] + 2 * pad) / VIEWPORT_H * 100.0)
                    return {
                        "xPct": round(x, 2),
                        "yPct": round(y, 2),
                        "wPct": round(w, 2),
                        "hPct": round(h, 2),
                    }
    except Exception:
        pass
    return fallback or {"xPct": 25.0, "yPct": 22.0, "wPct": 50.0, "hPct": 26.0}


def capture_all_reconciliation_tours(ctx):
    tours_manifest = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        context = browser.new_context(
            viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
            device_scale_factor=2,
        )
        page = context.new_page()
        set_browser_auth(page, ctx["admin_jwt"], "corporate_admin", ctx["cust_id"])

        # ======================================================================
        # TOUR 4.1: Zero-Config Statement Ingestion & Continuity Audit (5 steps)
        # ======================================================================
        print("=== Capturing Tour 4.1: recon-zero-config ===")
        steps_4_1 = []
        page.goto("http://localhost:3000/corporate-admin/reconciliation", wait_until="domcontentloaded")
        try:
            page.locator("text=First National Corporate Bank").first.wait_for(state="visible", timeout=15000)
        except Exception:
            time.sleep(4.5)
        time.sleep(1.0)
        hide_overlays(page)

        # Ensure FNCB is selected in the Select Bank dropdown so zero real bank names show!
        bank_sel = page.locator("select").first
        if bank_sel.count():
            try:
                bank_sel.select_option(value=str(ctx["fncb_id"]))
                time.sleep(0.5)
            except Exception:
                pass

        shot = os.path.join(STAGE_SHOTS, "recon_4_1", "step_01_smart_import_hub.png")
        page.screenshot(path=shot)
        hs_41_1 = measure_box_pct(
            page,
            page.locator("form").first,
            pad=10,
            fallback={"xPct": 19.0, "yPct": 24.0, "wPct": 25.0, "hPct": 50.0},
        )
        steps_4_1.append(
            {
                "step": 1,
                "key": "smart_import_hub",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation",
                "image": "/demo/shots/recon_4_1/step_01_smart_import_hub.png",
                "hotspot": hs_41_1,
            }
        )

        # Step 2: Highlight Recent Validated Statement Imports & Continuity Check
        shot = os.path.join(STAGE_SHOTS, "recon_4_1", "step_02_continuity_validated_statements.png")
        page.screenshot(path=shot)
        hs_41_2 = measure_box_pct(
            page,
            page.locator("div:has(> div > h3:has-text('Recent Statement Imports'))").first,
            pad=8,
            fallback={"xPct": 45.0, "yPct": 22.0, "wPct": 51.0, "hPct": 54.0},
        )
        steps_4_1.append(
            {
                "step": 2,
                "key": "continuity_validated_statements",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation#validated",
                "image": "/demo/shots/recon_4_1/step_02_continuity_validated_statements.png",
                "hotspot": hs_41_2,
            }
        )

        # Step 3: Open FNCB Statement Workspace & focus on Run Rules & AI
        page.goto(
            f"http://localhost:3000/corporate-admin/reconciliation/workspace/{ctx['stmt_fncb_id']}",
            wait_until="domcontentloaded",
        )
        time.sleep(2.6)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "recon_4_1", "step_03_statement_lines_workspace.png")
        page.screenshot(path=shot)
        hs_41_3 = measure_box_pct(
            page,
            page.locator("button:has-text('Run Rules & AI')").first,
            pad=8,
            fallback={"xPct": 84.0, "yPct": 11.0, "wPct": 12.0, "hPct": 5.5},
        )
        steps_4_1.append(
            {
                "step": 3,
                "key": "statement_lines_workspace",
                "url": f"app.growbusinessdevelopment.com/corporate-admin/reconciliation/workspace/{ctx['stmt_fncb_id']}",
                "image": "/demo/shots/recon_4_1/step_03_statement_lines_workspace.png",
                "hotspot": hs_41_3,
            }
        )

        # Step 4: Focus on Multi-Tier Explainable Classification Pills across the table
        shot = os.path.join(STAGE_SHOTS, "recon_4_1", "step_04_multitier_ai_badges.png")
        page.screenshot(path=shot)
        hs_41_4 = measure_box_pct(
            page,
            page.locator("tbody").first,
            pad=6,
            fallback={"xPct": 19.0, "yPct": 25.0, "wPct": 78.0, "hPct": 52.0},
        )
        steps_4_1.append(
            {
                "step": 4,
                "key": "multitier_ai_badges",
                "url": f"app.growbusinessdevelopment.com/corporate-admin/reconciliation/workspace/{ctx['stmt_fncb_id']}#classified",
                "image": "/demo/shots/recon_4_1/step_04_multitier_ai_badges.png",
                "hotspot": hs_41_4,
            }
        )

        # Step 5: Open Transaction Deep-Dive Audit Drawer on Row 1 (NAUUA Milestone Collection)
        info_btn = page.locator("tr:has-text('NAUUA MILESTONE') button[title*='audit drawer']").first
        if info_btn.count():
            info_btn.click()
            time.sleep(1.2)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "recon_4_1", "step_05_deep_dive_audit_drawer.png")
        page.screenshot(path=shot)
        hs_41_5 = measure_box_pct(
            page,
            page.locator("div.w-\\[520px\\]").first,
            pad=4,
            fallback={"xPct": 63.5, "yPct": 2.0, "wPct": 36.0, "hPct": 95.0},
        )
        steps_4_1.append(
            {
                "step": 5,
                "key": "deep_dive_audit_drawer",
                "url": f"app.growbusinessdevelopment.com/corporate-admin/reconciliation/workspace/{ctx['stmt_fncb_id']}#audit-drawer",
                "image": "/demo/shots/recon_4_1/step_05_deep_dive_audit_drawer.png",
                "hotspot": hs_41_5,
            }
        )

        tours_manifest["recon-zero-config"] = {
            "branchId": "recon-zero-config",
            "moduleId": "reconciliation",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "FNCB Account Statement Extract",
                "titleAr": "كشف حساب بنك FNCB التشغيلي",
                "pdfUrl": "/demo/docs/FNCB_Corporate_Operating_Nov2026.pdf",
                "previewPngUrl": "/demo/docs/FNCB_Corporate_Operating_Nov2026_preview.png",
            },
            "steps": steps_4_1,
        }

        # ======================================================================
        # TOUR 4.2: Cross-Bank Sweep Pairing, Reversals & Split ERP Match (5 steps)
        # ======================================================================
        print("=== Capturing Tour 4.2: recon-sweeps-reversals ===")
        steps_4_2 = []
        page.goto("http://localhost:3000/corporate-admin/reconciliation/workspace", wait_until="domcontentloaded")
        time.sleep(2.6)
        hide_overlays(page)

        shot = os.path.join(STAGE_SHOTS, "recon_4_2", "step_01_unified_ledger_sweeps.png")
        page.screenshot(path=shot)
        hs_42_1 = measure_box_pct(
            page,
            page.locator("tr:has-text('INTER-BANK LIQUIDITY SWEEP')").first,
            pad=6,
            fallback={"xPct": 19.0, "yPct": 32.0, "wPct": 78.0, "hPct": 12.0},
        )
        steps_4_2.append(
            {
                "step": 1,
                "key": "unified_ledger_sweeps",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/workspace#sweeps",
                "image": "/demo/shots/recon_4_2/step_01_unified_ledger_sweeps.png",
                "hotspot": hs_42_1,
            }
        )

        # Step 2: Click Sweep badge -> Side-by-Side Sweep Pair Verification Modal
        sweep_btn = page.locator("button:has-text('Sweep ↔')").first
        if sweep_btn.count():
            sweep_btn.click()
            time.sleep(1.2)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "recon_4_2", "step_02_sweep_pair_verification_modal.png")
        page.screenshot(path=shot)
        hs_42_2 = measure_box_pct(
            page,
            page.locator("div:has(> div > button:has-text('Close Verification'))").last,
            pad=6,
            fallback={"xPct": 18.0, "yPct": 12.0, "wPct": 64.0, "hPct": 76.0},
        )
        steps_4_2.append(
            {
                "step": 2,
                "key": "sweep_pair_verification_modal",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/workspace#verify-sweep",
                "image": "/demo/shots/recon_4_2/step_02_sweep_pair_verification_modal.png",
                "hotspot": hs_42_2,
            }
        )

        close_ver = page.locator("button:has-text('Close Verification')").first
        if close_ver.count():
            close_ver.click()
            time.sleep(0.8)

        # Step 3: Highlight Self-Canceling Reversal & Returned Wire Pair
        shot = os.path.join(STAGE_SHOTS, "recon_4_2", "step_03_returned_wire_reversal_netting.png")
        page.screenshot(path=shot)
        hs_42_3 = measure_box_pct(
            page,
            page.locator("tr:has-text('REVERSAL RETURNED WIRE')").first,
            pad=8,
            fallback={"xPct": 19.0, "yPct": 52.0, "wPct": 78.0, "hPct": 14.0},
        )
        steps_4_2.append(
            {
                "step": 3,
                "key": "returned_wire_reversal_netting",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/workspace#reversals",
                "image": "/demo/shots/recon_4_2/step_03_returned_wire_reversal_netting.png",
                "hotspot": hs_42_3,
            }
        )

        # Step 4: Switch to Split Match Dual-Pane Mode & select Bank Line + ERP Invoice (1,249,850 vs 1,250,000)
        split_btn = page.locator("button:has-text('Split Match')").first
        if split_btn.count():
            split_btn.click()
            time.sleep(1.4)
        ns_bank_row = page.locator("tr:has-text('NORTHSTAR ELECTROMECHANICAL')").first
        if ns_bank_row.count():
            ns_bank_row.locator("input[type='checkbox']").first.click()
            time.sleep(0.5)
        ns_erp_card = page.locator("tr:has-text('INV-2026-4082'), div:has-text('INV-2026-4082')").last
        if ns_erp_card.count():
            ns_erp_card.click()
            time.sleep(0.6)
        # Hide bottom floating dock temporarily on Split Match so both panes look super clean
        page.add_style_tag(content="div.fixed.bottom-6 { display: none !important; }")
        shot = os.path.join(STAGE_SHOTS, "recon_4_2", "step_04_split_pane_erp_match.png")
        page.screenshot(path=shot)
        hs_42_4 = measure_box_pct(
            page,
            page.locator("button:has-text('Confirm Match')").first,
            pad=8,
            fallback={"xPct": 83.0, "yPct": 11.0, "wPct": 14.0, "hPct": 5.5},
        )
        steps_4_2.append(
            {
                "step": 4,
                "key": "split_pane_erp_match",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/workspace#split-match",
                "image": "/demo/shots/recon_4_2/step_04_split_pane_erp_match.png",
                "hotspot": hs_42_4,
            }
        )

        # Step 5: Confirm Match -> ERP Invoice reconciled with 150 EGP Fee Tolerance Write-Off!
        conf_match_btn = page.locator("button:has-text('Confirm Match')").first
        if conf_match_btn.count() and conf_match_btn.is_enabled():
            conf_match_btn.click()
            time.sleep(1.8)
        else:
            # Ensure deterministic DB match if ERP pane used card click differently
            db2 = SessionLocal()
            try:
                bt = db2.query(mr.BankTransaction).filter(mr.BankTransaction.id == ctx["t7_northstar_id"]).first()
                er = db2.query(mr.InternalLedgerRecord).filter(mr.InternalLedgerRecord.id == ctx["erp_ns_id"]).first()
                if bt and er and not bt.is_reconciled:
                    bt.is_reconciled = True
                    bt.variance_amount = Decimal("150.00")
                    er.status = "RECONCILED"
                    er.matched_bank_txn_id = bt.id
                    db2.add(
                        mr.ReconciliationMatch(
                            bank_txn_id=bt.id,
                            source_type="AR",
                            source_record_id=er.id,
                            match_type="1:1",
                            match_logic="TOLERANCE",
                            variance_amount=Decimal("150.00"),
                            variance_disposition="BANK_FEE_WRITEOFF",
                            created_by=ctx["admin_id"],
                        )
                    )
                    db2.commit()
            finally:
                db2.close()
            page.reload(wait_until="domcontentloaded")
            time.sleep(2.2)
            hide_overlays(page)
            if page.locator("button:has-text('Split Match')").count():
                page.locator("button:has-text('Split Match')").first.click()
                time.sleep(1.0)

        shot = os.path.join(STAGE_SHOTS, "recon_4_2", "step_05_tolerance_match_confirmed.png")
        page.screenshot(path=shot)
        hs_42_5 = measure_box_pct(
            page,
            page.locator("tr:has-text('NORTHSTAR ELECTROMECHANICAL')").first,
            pad=6,
            fallback={"xPct": 19.0, "yPct": 62.0, "wPct": 42.0, "hPct": 11.0},
        )
        steps_4_2.append(
            {
                "step": 5,
                "key": "tolerance_match_confirmed",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/workspace#matched",
                "image": "/demo/shots/recon_4_2/step_05_tolerance_match_confirmed.png",
                "hotspot": hs_42_5,
            }
        )

        tours_manifest["recon-sweeps-reversals"] = {
            "branchId": "recon-sweeps-reversals",
            "moduleId": "reconciliation",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "Cross-Bank Sweep & Clearing Advice",
                "titleAr": "إشعار التحويل البيني ومطابقة ERP",
                "pdfUrl": "/demo/docs/Apex_InterBank_Sweep_Advice_Nov2026.pdf",
                "previewPngUrl": "/demo/docs/Apex_InterBank_Sweep_Advice_Nov2026_preview.png",
            },
            "steps": steps_4_2,
        }

        # ======================================================================
        # TOUR 4.3: Online Self-Learning Rules & Zero-PII Collaborative Brain (5 steps)
        # ======================================================================
        print("=== Capturing Tour 4.3: recon-rules-privacy ===")
        steps_4_3 = []
        page.goto(
            f"http://localhost:3000/corporate-admin/reconciliation/workspace/{ctx['stmt_fncb_id']}",
            wait_until="domcontentloaded",
        )
        time.sleep(2.4)
        hide_overlays(page)

        # Step 1: Click `+ Classify` on NorthStar row -> Classify Bank Transaction modal with Remember for Counterparty!
        cls_btn = page.locator("button:has-text('Classify')").first
        if cls_btn.count():
            cls_btn.click()
            time.sleep(1.0)
            rem_cb = page.locator("label:has-text('Remember this classification for Counterparty') input[type='checkbox']").first
            if rem_cb.count() and not rem_cb.is_checked():
                rem_cb.click()
                time.sleep(0.4)
        shot = os.path.join(STAGE_SHOTS, "recon_4_3", "step_01_inline_classify_remember_counterparty.png")
        page.screenshot(path=shot)
        hs_43_1 = measure_box_pct(
            page,
            page.locator("div.max-w-lg").last,
            pad=6,
            fallback={"xPct": 32.0, "yPct": 16.0, "wPct": 36.0, "hPct": 68.0},
        )
        steps_4_3.append(
            {
                "step": 1,
                "key": "inline_classify_remember_counterparty",
                "url": f"app.growbusinessdevelopment.com/corporate-admin/reconciliation/workspace/{ctx['stmt_fncb_id']}#classify",
                "image": "/demo/shots/recon_4_3/step_01_inline_classify_remember_counterparty.png",
                "hotspot": hs_43_1,
            }
        )

        # Mark t7 classified in DB so Export tour has 100% (8 of 8) classified!
        db3 = SessionLocal()
        try:
            bt7 = db3.query(mr.BankTransaction).filter(mr.BankTransaction.id == ctx["t7_northstar_id"]).first()
            if bt7:
                bt7.is_classified = True
                bt7.internal_category = "1020100 - Trade Receivables"
                bt7.classification_category = "Project & Milestone Collections"
                bt7.classification_source = "MANUAL"
                bt7.classification_confidence = 100
                db3.commit()
        finally:
            db3.close()

        # Step 2: Navigate to `/corporate-admin/reconciliation/rules` -> Tab 1: Custom Rules (Online Learned + Self-Healed)
        page.goto("http://localhost:3000/corporate-admin/reconciliation/rules", wait_until="domcontentloaded")
        time.sleep(2.4)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "recon_4_3", "step_02_online_learned_self_healed_rules.png")
        page.screenshot(path=shot)
        hs_43_2 = measure_box_pct(
            page,
            page.locator("table").first,
            pad=8,
            fallback={"xPct": 19.0, "yPct": 28.0, "wPct": 78.0, "hPct": 44.0},
        )
        steps_4_3.append(
            {
                "step": 2,
                "key": "online_learned_self_healed_rules",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/rules#custom-rules",
                "image": "/demo/shots/recon_4_3/step_02_online_learned_self_healed_rules.png",
                "hotspot": hs_43_2,
            }
        )

        # Step 3: Tab 2: Counterparty Registry (AES-256-GCM + Aliases + Default GL)
        cp_tab = page.locator("button:has-text('Counterparty Registry')").first
        if cp_tab.count():
            cp_tab.click()
            time.sleep(1.0)
        shot = os.path.join(STAGE_SHOTS, "recon_4_3", "step_03_encrypted_counterparty_registry.png")
        page.screenshot(path=shot)
        hs_43_3 = measure_box_pct(
            page,
            page.locator("table").first,
            pad=8,
            fallback={"xPct": 19.0, "yPct": 28.0, "wPct": 78.0, "hPct": 46.0},
        )
        steps_4_3.append(
            {
                "step": 3,
                "key": "encrypted_counterparty_registry",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/rules#counterparties",
                "image": "/demo/shots/recon_4_3/step_03_encrypted_counterparty_registry.png",
                "hotspot": hs_43_3,
            }
        )

        # Step 4: Tab 3: Zero-PII Collaborative Consensus Brain
        col_tab = page.locator("button:has-text('Collaborative Brain')").first
        if col_tab.count():
            col_tab.click()
            time.sleep(1.2)
        shot = os.path.join(STAGE_SHOTS, "recon_4_3", "step_04_zero_pii_collaborative_brain.png")
        page.screenshot(path=shot)
        hs_43_4 = measure_box_pct(
            page,
            page.locator("div:has(> h3:has-text('Privacy-Preserving Federated Intelligence')), div.bg-gradient-to-r").first,
            pad=8,
            fallback={"xPct": 19.0, "yPct": 18.0, "wPct": 78.0, "hPct": 22.0},
        )
        steps_4_3.append(
            {
                "step": 4,
                "key": "zero_pii_collaborative_brain",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/rules#collaborative-brain",
                "image": "/demo/shots/recon_4_3/step_04_zero_pii_collaborative_brain.png",
                "hotspot": hs_43_4,
            }
        )

        # Step 5: Tab 4: Bilingual Hierarchical Taxonomy (Classes & Subclasses)
        tax_tab = page.locator("button:has-text('Taxonomy')").first
        if tax_tab.count():
            tax_tab.click()
            time.sleep(1.0)
        shot = os.path.join(STAGE_SHOTS, "recon_4_3", "step_05_bilingual_gl_taxonomy.png")
        page.screenshot(path=shot)
        hs_43_5 = measure_box_pct(
            page,
            page.locator("div:has(> div > div > div > h3:has-text('Project & Milestone Collections'))").first,
            pad=8,
            fallback={"xPct": 19.0, "yPct": 28.0, "wPct": 78.0, "hPct": 52.0},
        )
        steps_4_3.append(
            {
                "step": 5,
                "key": "bilingual_gl_taxonomy",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/rules#taxonomy",
                "image": "/demo/shots/recon_4_3/step_05_bilingual_gl_taxonomy.png",
                "hotspot": hs_43_5,
            }
        )

        tours_manifest["recon-rules-privacy"] = {
            "branchId": "recon-rules-privacy",
            "moduleId": "reconciliation",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "Zero-PII Federated AI Architecture Brief",
                "titleAr": "مذكرة حماية الخصوصية والتعلم المشترك",
                "pdfUrl": "/demo/docs/Grow_ZeroPII_Privacy_Architecture_Brief.pdf",
                "previewPngUrl": "/demo/docs/Grow_ZeroPII_Privacy_Architecture_Brief_preview.png",
            },
            "steps": steps_4_3,
        }

        # ======================================================================
        # TOUR 4.4: Customizable Double-Entry GL Journal & Locked ERP Export (5 steps)
        # ======================================================================
        print("=== Capturing Tour 4.4: recon-erp-export ===")
        steps_4_4 = []
        page.goto("http://localhost:3000/corporate-admin/reconciliation/export", wait_until="domcontentloaded")
        time.sleep(2.6)
        hide_overlays(page)

        # Step 1: 4 Readiness KPI Cards & Accounting Export Header
        shot = os.path.join(STAGE_SHOTS, "recon_4_4", "step_01_export_readiness_kpis.png")
        page.screenshot(path=shot)
        hs_44_1 = measure_box_pct(
            page,
            page.locator("div.grid.md\\:grid-cols-4").first,
            pad=8,
            fallback={"xPct": 19.0, "yPct": 18.0, "wPct": 78.0, "hPct": 16.0},
        )
        steps_4_4.append(
            {
                "step": 1,
                "key": "export_readiness_kpis",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/export",
                "image": "/demo/shots/recon_4_4/step_01_export_readiness_kpis.png",
                "hotspot": hs_44_1,
            }
        )

        # Step 2: Select saved `SAP S/4HANA FI Double-Entry Journal` profile -> custom column builder
        prof_sel = page.locator("select").first
        if prof_sel.count():
            try:
                prof_sel.select_option(value=str(ctx["exp_prof_id"]))
                time.sleep(1.5)
            except Exception:
                pass
        shot = os.path.join(STAGE_SHOTS, "recon_4_4", "step_02_sap_profile_column_builder.png")
        page.screenshot(path=shot)
        hs_44_2 = measure_box_pct(
            page,
            page.locator("div:has(> label:has-text('Saved Export Template'))").first,
            pad=10,
            fallback={"xPct": 20.0, "yPct": 36.0, "wPct": 62.0, "hPct": 13.0},
        )
        steps_4_4.append(
            {
                "step": 2,
                "key": "sap_profile_column_builder",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/export#layout-builder",
                "image": "/demo/shots/recon_4_4/step_02_sap_profile_column_builder.png",
                "hotspot": hs_44_2,
            }
        )

        # Step 3: Highlight `Expand Double-Entry GL Journal Legs (Bank + Offset + Fee Variance)` toggle
        shot = os.path.join(STAGE_SHOTS, "recon_4_4", "step_03_double_entry_gl_expansion.png")
        page.screenshot(path=shot)
        hs_44_3 = measure_box_pct(
            page,
            page.locator("label:has-text('Expand Double-Entry GL Journal Legs')").first,
            pad=10,
            fallback={"xPct": 20.0, "yPct": 49.0, "wPct": 46.0, "hPct": 7.0},
        )
        steps_4_4.append(
            {
                "step": 3,
                "key": "double_entry_gl_expansion",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/export#double-entry",
                "image": "/demo/shots/recon_4_4/step_03_double_entry_gl_expansion.png",
                "hotspot": hs_44_3,
            }
        )

        # Step 4: Hide Layout Builder temporarily so the Live Export Layout Preview table fills the viewport crisply!
        hide_builder_btn = page.locator("button:has-text('Hide Layout Builder')").first
        if hide_builder_btn.count():
            hide_builder_btn.click()
            time.sleep(0.8)
        shot = os.path.join(STAGE_SHOTS, "recon_4_4", "step_04_live_balanced_journal_preview.png")
        page.screenshot(path=shot)
        hs_44_4 = measure_box_pct(
            page,
            page.locator("div:has(> div > div > h3:has-text('Live Export Layout Preview'))").first,
            pad=6,
            fallback={"xPct": 19.0, "yPct": 34.0, "wPct": 78.0, "hPct": 54.0},
        )
        steps_4_4.append(
            {
                "step": 4,
                "key": "live_balanced_journal_preview",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/export#preview",
                "image": "/demo/shots/recon_4_4/step_04_live_balanced_journal_preview.png",
                "hotspot": hs_44_4,
            }
        )

        # Step 5: Re-open builder, enable `Lock & Mark as Exported on Download`, trigger export lock via API & focus Download XLSX!
        show_builder_btn = page.locator("button:has-text('Customize Columns & Order')").first
        if show_builder_btn.count():
            show_builder_btn.click()
            time.sleep(0.6)
        lock_cb = page.locator("label:has-text('Lock & Mark as Exported on Download') input[type='checkbox']").first
        if lock_cb.count() and not lock_cb.is_checked():
            lock_cb.click()
            time.sleep(0.5)
        shot = os.path.join(STAGE_SHOTS, "recon_4_4", "step_05_lock_and_download_xlsx.png")
        page.screenshot(path=shot)
        hs_44_5 = measure_box_pct(
            page,
            page.locator("button:has-text('Download XLSX'), button:has-text('Download CSV')").first,
            pad=8,
            fallback={"xPct": 84.0, "yPct": 10.0, "wPct": 13.0, "hPct": 6.0},
        )
        steps_4_4.append(
            {
                "step": 5,
                "key": "lock_and_download_xlsx",
                "url": "app.growbusinessdevelopment.com/corporate-admin/reconciliation/export#locked-export",
                "image": "/demo/shots/recon_4_4/step_05_lock_and_download_xlsx.png",
                "hotspot": hs_44_5,
            }
        )

        tours_manifest["recon-erp-export"] = {
            "branchId": "recon-erp-export",
            "moduleId": "reconciliation",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "SAP Double-Entry GL Journal Pack",
                "titleAr": "حزمة قيود اليومية المزدوجة لنظام ERP",
                "pdfUrl": "/demo/docs/Apex_SAP_DoubleEntry_GL_Journal_Nov2026.pdf",
                "previewPngUrl": "/demo/docs/Apex_SAP_DoubleEntry_GL_Journal_Nov2026_preview.png",
            },
            "steps": steps_4_4,
        }

        browser.close()
    return tours_manifest


def publish_assets_and_manifest(tours_manifest: dict):
    for fn in os.listdir(STAGE_DOCS):
        shutil.copy2(os.path.join(STAGE_DOCS, fn), os.path.join(PUBLIC_DOCS_DIR, fn))
    for sub in ["recon_4_1", "recon_4_2", "recon_4_3", "recon_4_4"]:
        src_sub = os.path.join(STAGE_SHOTS, sub)
        dst_sub = os.path.join(PUBLIC_SHOTS_DIR, sub)
        for fn in os.listdir(src_sub):
            shutil.copy2(os.path.join(src_sub, fn), os.path.join(dst_sub, fn))

    manifest = {}
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    if "tours" not in manifest:
        manifest["tours"] = {}
    for k, v in tours_manifest.items():
        manifest["tours"][k] = v
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"[DONE] Published 4 Reconciliation tours ({sum(len(v['steps']) for v in tours_manifest.values())} steps) into {MANIFEST_PATH}")


if __name__ == "__main__":
    ensure_dirs()
    generate_companion_pdfs()
    ctx = seed_reconciliation_demo_data()
    tours = capture_all_reconciliation_tours(ctx)
    publish_assets_and_manifest(tours)
