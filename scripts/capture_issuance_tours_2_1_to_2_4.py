# c:\Grow\scripts\capture_issuance_tours_2_1_to_2_4.py
"""
End-to-end live capture script for Grow LG Issuance Interactive Tours (Module 02):
  - 2.1: issuance-intake           ("Department Request, AI Trap Scan & Approval" - 6 steps)
  - 2.2: issuance-facility-scoring ("Multi-Bank Facility Matrix & 0-100 Smart Scoring" - 5 steps)
  - 2.3: issuance-bank-form        ("Auto-Filled Official Bank PDF Form & AI Copy Check" - 5 steps)
  - 2.4: issuance-position-recon   ("Post-Issuance Governance & Bank Position Reconciliation" - 5 steps)

Strict Domain Separation:
  Module 02 = Outgoing Corporate Letters of Guarantee where Apex Global Engineering SAE
  is the APPLICANT utilizing its own bank credit facilities, paying commissions, and
  pledging cash margins (100% distinct from Module 01 LG Custody incoming vendor guarantees).

Strictly fictional banks & corporates:
  - First National Corporate Bank (FNCB)
  - Mediterranean Commercial Bank (MCB)
  - Nile Sovereign Bank (NSB)
"""
import asyncio
import io
import json
import os
import shutil
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT_DIR)

import fitz  # PyMuPDF
import openpyxl
from playwright.sync_api import sync_playwright
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from app.auth_v2.services import AuthService
from app.core.encryption import encrypt_data
from app.core.pdf_form_filler import build_request_data_dict, generate_scanned_fill_pdf
from app.core.security import create_access_token
from app.database import SessionLocal
from app.models import models
from app.models import models_issuance as mi

VIEWPORT_W = 1440
VIEWPORT_H = 900

STAGE_DIR = os.path.join(ROOT_DIR, "scratch", "issuance_tours_stage")
STAGE_DOCS = os.path.join(STAGE_DIR, "docs")
STAGE_SHOTS = os.path.join(STAGE_DIR, "shots")

PUBLIC_DEMO_DIR = os.path.join(ROOT_DIR, "frontend", "public", "demo")
PUBLIC_DOCS_DIR = os.path.join(PUBLIC_DEMO_DIR, "docs")
PUBLIC_SHOTS_DIR = os.path.join(PUBLIC_DEMO_DIR, "shots")
MANIFEST_PATH = os.path.join(
    ROOT_DIR, "frontend", "src", "components", "Showcase", "pilotManifest.json"
)


def render_pdf_first_page_png(pdf_path: str, png_path: str, scale: float = 2.2) -> None:
    doc = fitz.open(pdf_path)
    try:
        page = doc[0]
        pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        pix.save(png_path)
    finally:
        doc.close()


def ensure_fictional_banks(db) -> dict:
    """Ensures FNCB, MCB, and NSB exist in `banks` and returns their ORM objects."""
    specs = [
        {
            "key": "FNCB",
            "name": "First National Corporate Bank",
            "short_name": "FNCB",
            "swift_code": "FNCBEGCXXXX",
        },
        {
            "key": "MCB",
            "name": "Mediterranean Commercial Bank",
            "short_name": "MCB",
            "swift_code": "MCBKEGCXXXX",
        },
        {
            "key": "NSB",
            "name": "Nile Sovereign Bank",
            "short_name": "NSB",
            "swift_code": "NSBKEGCXXXX",
        },
    ]
    out = {}
    for sp in specs:
        bank = (
            db.query(models.Bank)
            .filter(models.Bank.short_name == sp["short_name"])
            .first()
        )
        if not bank:
            bank = (
                db.query(models.Bank)
                .filter(models.Bank.name == sp["name"])
                .first()
            )
        if not bank:
            bank = models.Bank(
                name=sp["name"],
                short_name=sp["short_name"],
                swift_code=sp["swift_code"],
                is_deleted=False,
            )
            db.add(bank)
            db.flush()
        else:
            bank.name = sp["name"]
            bank.short_name = sp["short_name"]
            bank.swift_code = sp["swift_code"]
            bank.is_deleted = False
            db.flush()
        out[sp["key"]] = bank
    db.commit()
    return out


def generate_fncb_blank_bank_form_pdf(pdf_path: str) -> None:
    """Creates a clean, authentic official FNCB LG Issuance Application blank template PDF."""
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    pw, ph = A4  # 595.27 x 841.89 pt
    c = rl_canvas.Canvas(pdf_path, pagesize=A4)

    # Header banner
    c.setFillColor(colors.HexColor("#1e293b"))
    c.rect(14 * mm, ph - 34 * mm, pw - 28 * mm, 22 * mm, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 13)
    c.drawString(20 * mm, ph - 21 * mm, "FIRST NATIONAL CORPORATE BANK (FNCB)")
    c.setFont("Helvetica", 9)
    c.drawString(
        20 * mm,
        ph - 27.5 * mm,
        "Corporate Trade Finance Division  |  Official Letter of Guarantee Issuance Application (Form TF-LG-01)",
    )

    def draw_box_field(label: str, x_pct: float, y_pct: float, w_pct: float, h_pt: float = 22):
        x = (x_pct / 100.0) * pw
        y_top = (1.0 - y_pct / 100.0) * ph
        w = (w_pct / 100.0) * pw
        c.setFillColor(colors.HexColor("#475569"))
        c.setFont("Helvetica-Bold", 7.5)
        c.drawString(x, y_top + 4, label.upper())
        c.setStrokeColor(colors.HexColor("#cbd5e1"))
        c.setFillColor(colors.HexColor("#f8fafc"))
        c.roundRect(x, y_top - h_pt + 2, w, h_pt, 3, fill=1, stroke=1)

    # Section 1: Applicant & Account Details
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 9.5)
    c.drawString(15 * mm, ph - 43 * mm, "1. APPLICANT CORPORATE & ACCOUNT DETAILS")
    c.setStrokeColor(colors.HexColor("#e2e8f0"))
    c.line(15 * mm, ph - 45 * mm, pw - 15 * mm, ph - 45 * mm)

    draw_box_field("Applicant Legal Entity Name", 7.5, 19.5, 55.0)
    draw_box_field("Application Date", 65.0, 19.5, 27.5)
    draw_box_field("Corporate Account / IBAN", 7.5, 25.5, 42.0)
    draw_box_field("FNCB Branch", 52.0, 25.5, 22.0)
    draw_box_field("Customer CIF #", 76.0, 25.5, 16.5)

    # Section 2: Beneficiary & Contract Reference
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 9.5)
    c.drawString(15 * mm, ph - 82 * mm, "2. BENEFICIARY & UNDERLYING CONTRACT MANDATE")
    c.line(15 * mm, ph - 84 * mm, pw - 15 * mm, ph - 84 * mm)

    draw_box_field("Beneficiary Official Name", 7.5, 33.5, 85.0)
    draw_box_field("Beneficiary Address & Country", 7.5, 39.5, 55.0)
    draw_box_field("Contract / Tender Reference", 65.0, 39.5, 27.5)

    # Section 3: Guarantee Financial & Validity Terms
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 9.5)
    c.drawString(15 * mm, ph - 124 * mm, "3. GUARANTEE INSTRUMENT & FACILITY TERMS")
    c.line(15 * mm, ph - 126 * mm, pw - 15 * mm, ph - 126 * mm)

    draw_box_field("Guarantee Type", 7.5, 48.0, 35.0)
    draw_box_field("Currency & Principal Amount", 45.0, 48.0, 24.0)
    draw_box_field("Requested Expiry Date", 71.5, 48.0, 21.0)
    draw_box_field("Amount in Words", 7.5, 54.5, 85.0)
    draw_box_field("Underlying Purpose & Project Scope", 7.5, 61.0, 85.0)
    draw_box_field("Applicable Rules & Companion Annex Reference", 7.5, 67.5, 85.0)

    # Checkboxes row
    c.setFillColor(colors.HexColor("#334155"))
    c.setFont("Helvetica-Bold", 8)
    cb_y_pct = 74.0
    cb_y = (1.0 - cb_y_pct / 100.0) * ph
    for bx_pct, lbl in [
        (8.0, "Domestic LG"),
        (28.0, "Cross-Border SWIFT MT760"),
        (56.0, "Special Wording Annex Attached"),
    ]:
        bx = (bx_pct / 100.0) * pw
        c.setStrokeColor(colors.HexColor("#64748b"))
        c.rect(bx, cb_y - 9, 10, 10, fill=0, stroke=1)
        c.drawString(bx + 15, cb_y - 7, lbl)

    # Authorized Signatories Box
    c.setStrokeColor(colors.HexColor("#cbd5e1"))
    c.setFillColor(colors.HexColor("#f8fafc"))
    c.roundRect(15 * mm, 24 * mm, pw - 30 * mm, 36 * mm, 4, fill=1, stroke=1)
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 8.5)
    c.drawString(
        20 * mm,
        53 * mm,
        "APPLICANT INDEMNITY & AUTHORIZED CORPORATE SIGNATURES",
    )
    c.setFont("Helvetica", 7.5)
    c.setFillColor(colors.HexColor("#475569"))
    c.drawString(
        20 * mm,
        46 * mm,
        "We hereby instruct First National Corporate Bank to issue the above Letter of Guarantee under our approved",
    )
    c.drawString(
        20 * mm,
        41 * mm,
        "Credit Facility Agreement and authorize debiting applicable commissions and cash margins from our account.",
    )
    c.drawString(20 * mm, 29 * mm, "Authorized Signatory A: ______________________")
    c.drawString(110 * mm, 29 * mm, "Authorized Signatory B & Corporate Seal: ______________________")

    c.setFont("Helvetica", 7)
    c.setFillColor(colors.HexColor("#94a3b8"))
    c.drawString(
        15 * mm,
        14 * mm,
        "FNCB Form TF-LG-01 (Rev. 2026) · Auto-Mapped via Grow Bank Form Overlay Engine",
    )
    c.save()


def build_companion_pdf(pdf_path: str, badge: str, title: str, subtitle: str, rows: list, note: str) -> None:
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=16 * mm,
        rightMargin=16 * mm,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
    )
    styles = getSampleStyleSheet()
    story = []

    hdr_style = ParagraphStyle(
        "Hdr",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=9,
        textColor=colors.HexColor("#6d28d9"),
        spaceAfter=3,
    )
    title_style = ParagraphStyle(
        "TitleS",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=15,
        leading=19,
        textColor=colors.HexColor("#0f172a"),
        spaceAfter=4,
    )
    sub_style = ParagraphStyle(
        "SubS",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=13,
        textColor=colors.HexColor("#475569"),
        spaceAfter=10,
    )
    body_style = ParagraphStyle(
        "BodyS",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=13.5,
        textColor=colors.HexColor("#1e293b"),
    )

    story.append(Paragraph(badge.upper(), hdr_style))
    story.append(Paragraph(title, title_style))
    story.append(Paragraph(subtitle, sub_style))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#cbd5e1")))
    story.append(Spacer(1, 10))

    tbl_data = []
    for k, v in rows:
        tbl_data.append([
            Paragraph(f"<b>{k}</b>", body_style),
            Paragraph(str(v), body_style),
        ])

    tbl = Table(tbl_data, colWidths=[62 * mm, 114 * mm])
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f8fafc")),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#0f172a")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    story.append(tbl)
    story.append(Spacer(1, 12))

    note_box = Table([[Paragraph(note, body_style)]], colWidths=[176 * mm])
    note_box.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f5f3ff")),
                ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#c4b5fd")),
                ("TOPPADDING", (0, 0), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    story.append(note_box)
    doc.build(story)


def generate_issued_lg_scan_pdf(pdf_path: str, lg_number: str, amount_str: str, beneficiary: str, expiry_str: str) -> None:
    build_companion_pdf(
        pdf_path,
        badge="FIRST NATIONAL CORPORATE BANK (FNCB) · TRADE FINANCE",
        title=f"OUTGOING LETTER OF GUARANTEE NO. {lg_number}",
        subtitle="Issued on behalf of Applicant: Apex Global Engineering & Contracting SAE",
        rows=[
            ("Issuing Bank", "First National Corporate Bank (FNCB) — Corporate Trade Finance Center"),
            ("Applicant (Obligor)", "Apex Global Engineering SAE (HQ) · Credit Facility #FNCB-FAC-2026-01"),
            ("Beneficiary", beneficiary),
            ("Guarantee Type", "Advance Payment Guarantee (URDG 758)"),
            ("Principal Amount", amount_str),
            ("Issue Date", date.today().strftime("%Y-%m-%d")),
            ("Expiry Date", expiry_str),
            ("Underlying Contract", "Contract #NAUUA-2026-408 · New Capital Main Pumping Station"),
            ("Operational Clause", "Operative strictly upon receipt of advance payment into Applicant IBAN at FNCB"),
        ],
        note=(
            "<b>Bank Undertaking:</b> First National Corporate Bank hereby irrevocably undertakes to pay the "
            "Beneficiary up to the Principal Amount above upon receipt of a compliant written demand prior to the Expiry Date."
        ),
    )


def seed_issuance_demo_data() -> dict:
    db = SessionLocal()
    try:
        banks = ensure_fictional_banks(db)
        fncb, mcb, nsb = banks["FNCB"], banks["MCB"], banks["NSB"]

        cust_id = 23
        entity_hq = (
            db.query(models.CustomerEntity)
            .filter(models.CustomerEntity.customer_id == cust_id)
            .order_by(models.CustomerEntity.id.asc())
            .first()
        )
        egp = db.query(models.Currency).filter(models.Currency.iso_code == "EGP").first()
        usd = db.query(models.Currency).filter(models.Currency.iso_code == "USD").first()
        apg_type = (
            db.query(models.LgType)
            .filter(models.LgType.name.ilike("%Advance%"))
            .first()
            or db.query(models.LgType).first()
        )
        perf_type = (
            db.query(models.LgType)
            .filter(models.LgType.name.ilike("%Performance%"))
            .first()
            or apg_type
        )

        maker = (
            db.query(models.User)
            .filter(models.User.customer_id == cust_id, models.User.email == "treasury@apexglobal.com")
            .first()
        )
        admin = (
            db.query(models.User)
            .filter(models.User.customer_id == cust_id, models.User.email == "admin@apexglobal.com")
            .first()
        )

        # 1. Sanitize any legacy ENBD facility / issued LG rows for Customer 23!
        for old_fac in db.query(mi.IssuanceFacility).filter(mi.IssuanceFacility.customer_id == cust_id).all():
            if "ENBD" in (old_fac.facility_name or "") or old_fac.bank_id not in (fncb.id, mcb.id, nsb.id):
                old_fac.bank_id = fncb.id
                old_fac.facility_name = "FNCB Multi-Option Corporate Umbrella Facility"
                old_fac.reference_number = "FNCB-MOF-2026-01"
                old_fac.total_limit_amount = Decimal("150000000.00")
                old_fac.status = "ACTIVE"
                old_fac.multi_currency_allowed = True
                old_fac.allow_cross_border = False
                old_fac.fx_breach_auto_suspend = True
                old_fac.sla_agreement_days = 2
                old_fac.facility_default_margin_pct = Decimal("5.00")
                old_fac.expiry_date = date.today() + timedelta(days=340)

        for old_lg in db.query(mi.IssuedLGRecord).filter(mi.IssuedLGRecord.customer_id == cust_id).all():
            if old_lg.bank_id not in (fncb.id, mcb.id, nsb.id):
                old_lg.bank_id = fncb.id
            if old_lg.bank_lg_number and "ENBD" in old_lg.bank_lg_number:
                old_lg.bank_lg_number = old_lg.bank_lg_number.replace("ENBD", "FNCB")
        import re as _re
        for r_ex in db.query(mi.IssuanceRequest).filter(mi.IssuanceRequest.customer_id == cust_id).all():
            if r_ex.phone_number:
                r_ex.phone_number = _re.sub(r"[^0-9+]", "", r_ex.phone_number) or "+201004428810"
        db.commit()

        # 2. Ensure Corporate Project exists
        proj = (
            db.query(mi.CorporateProject)
            .filter(
                mi.CorporateProject.customer_id == cust_id,
                mi.CorporateProject.reference_number == "NAUUA-2026-408",
            )
            .first()
        )
        if not proj:
            proj = mi.CorporateProject(
                customer_id=cust_id,
                name="New Capital Main Water Pumping Station — Package B",
                project_type="CONTRACT",
                reference_number="NAUUA-2026-408",
                status="ACTIVE",
            )
            db.add(proj)
            db.flush()

        # 3. Ensure 3 clean multi-bank Facilities (FNCB = best score, MCB = #2, NSB = #3 rare cross-border line)
        fac_fncb = (
            db.query(mi.IssuanceFacility)
            .filter(
                mi.IssuanceFacility.customer_id == cust_id,
                mi.IssuanceFacility.bank_id == fncb.id,
            )
            .first()
        )
        if not fac_fncb:
            fac_fncb = mi.IssuanceFacility(
                customer_id=cust_id,
                bank_id=fncb.id,
                facility_name="FNCB Multi-Option Corporate Umbrella Facility",
                reference_number="FNCB-MOF-2026-01",
                currency_id=egp.id,
                total_limit_amount=Decimal("150000000.00"),
                status="ACTIVE",
                multi_currency_allowed=False,
                allow_cross_border=False,
                fx_breach_auto_suspend=True,
                margin_reduces_exposure=True,
                facility_default_margin_pct=Decimal("0.00"),
                sla_agreement_days=1,
                expiry_date=date.today() + timedelta(days=340),
            )
            db.add(fac_fncb)
            db.flush()
        else:
            fac_fncb.facility_name = "FNCB Multi-Option Corporate Umbrella Facility"
            fac_fncb.currency_id = egp.id
            fac_fncb.total_limit_amount = Decimal("600000000.00")
            fac_fncb.sla_agreement_days = 1
            fac_fncb.actual_avg_sla_days = Decimal("1.2")
            fac_fncb.total_completed_issuances = 8
            fac_fncb.facility_default_margin_pct = Decimal("0.00")
            fac_fncb.allow_cross_border = False
            fac_fncb.multi_currency_allowed = False
            fac_fncb.status = "ACTIVE"
            fac_fncb.is_deleted = False

        all_fncb_sls = (
            db.query(mi.IssuanceFacilitySubLimit)
            .filter(mi.IssuanceFacilitySubLimit.facility_id == fac_fncb.id)
            .order_by(mi.IssuanceFacilitySubLimit.id.desc())
            .all()
        )
        sl_fncb = all_fncb_sls[0] if all_fncb_sls else None
        for extra_sl in all_fncb_sls[1:]:
            extra_sl.max_amount_per_lg = Decimal("1.00")
        if not sl_fncb:
            sl_fncb = mi.IssuanceFacilitySubLimit(
                facility_id=fac_fncb.id,
                limit_name="Domestic Contracting & Infrastructure LGs",
                limit_amount=Decimal("100000000.00"),
                initial_utilization=Decimal("18500000.00"),
                default_commission_rate=Decimal("0.85"),
                default_cash_margin_pct=Decimal("0.00"),
                default_min_commission=Decimal("500.00"),
                default_flat_fee=Decimal("250.00"),
                max_tenor_days=730,
                max_amount_per_lg=None,
                allowed_countries=None,
                lg_type_ids=[],
            )
            db.add(sl_fncb)
            db.flush()
        else:
            sl_fncb.limit_name = "Domestic Contracting & Infrastructure LGs"
            sl_fncb.limit_amount = Decimal("100000000.00")
            sl_fncb.initial_utilization = Decimal("18500000.00")
            sl_fncb.default_commission_rate = Decimal("0.85")
            sl_fncb.default_cash_margin_pct = Decimal("0.00")
            sl_fncb.max_tenor_days = 730
            sl_fncb.max_amount_per_lg = None
            sl_fncb.allowed_countries = None
            sl_fncb.dedicated_project_ids = None
            sl_fncb.lg_type_ids = []

        fac_mcb = (
            db.query(mi.IssuanceFacility)
            .filter(
                mi.IssuanceFacility.customer_id == cust_id,
                mi.IssuanceFacility.bank_id == mcb.id,
            )
            .first()
        )
        if not fac_mcb:
            fac_mcb = mi.IssuanceFacility(
                customer_id=cust_id,
                bank_id=mcb.id,
                facility_name="MCB Trade & Project Guarantees Line",
                reference_number="MCB-TPG-2026-04",
                currency_id=egp.id,
                total_limit_amount=Decimal("90000000.00"),
                status="ACTIVE",
                multi_currency_allowed=True,
                allow_cross_border=False,
                fx_breach_auto_suspend=True,
                margin_reduces_exposure=True,
                facility_default_margin_pct=Decimal("10.00"),
                sla_agreement_days=3,
                expiry_date=date.today() + timedelta(days=290),
            )
            db.add(fac_mcb)
            db.flush()

        sl_mcb = (
            db.query(mi.IssuanceFacilitySubLimit)
            .filter(mi.IssuanceFacilitySubLimit.facility_id == fac_mcb.id)
            .first()
        )
        if not sl_mcb:
            sl_mcb = mi.IssuanceFacilitySubLimit(
                facility_id=fac_mcb.id,
                limit_name="General Performance & Advance LGs",
                limit_amount=Decimal("75000000.00"),
                initial_utilization=Decimal("24000000.00"),
                default_commission_rate=Decimal("1.35"),
                default_cash_margin_pct=Decimal("10.00"),
                default_min_commission=Decimal("750.00"),
                default_flat_fee=Decimal("350.00"),
                max_tenor_days=730,
                lg_type_ids=[],
            )
            db.add(sl_mcb)
            db.flush()
        else:
            sl_mcb.max_tenor_days = 730

        fac_nsb = (
            db.query(mi.IssuanceFacility)
            .filter(
                mi.IssuanceFacility.customer_id == cust_id,
                mi.IssuanceFacility.bank_id == nsb.id,
            )
            .first()
        )
        if not fac_nsb:
            fac_nsb = mi.IssuanceFacility(
                customer_id=cust_id,
                bank_id=nsb.id,
                facility_name="NSB Cross-Border & Multi-Currency SWIFT Line",
                reference_number="NSB-XBG-2026-09",
                currency_id=egp.id,
                total_limit_amount=Decimal("60000000.00"),
                status="ACTIVE",
                multi_currency_allowed=True,
                allow_cross_border=True,
                allow_third_party_issuance=True,
                fx_breach_auto_suspend=True,
                margin_reduces_exposure=False,
                facility_default_margin_pct=Decimal("15.00"),
                sla_agreement_days=4,
                expiry_date=date.today() + timedelta(days=260),
            )
            db.add(fac_nsb)
            db.flush()

        sl_nsb = (
            db.query(mi.IssuanceFacilitySubLimit)
            .filter(mi.IssuanceFacilitySubLimit.facility_id == fac_nsb.id)
            .first()
        )
        if not sl_nsb:
            sl_nsb = mi.IssuanceFacilitySubLimit(
                facility_id=fac_nsb.id,
                limit_name="International SWIFT MT760 & Umbrella Line",
                limit_amount=Decimal("50000000.00"),
                initial_utilization=Decimal("22000000.00"),
                default_commission_rate=Decimal("1.75"),
                default_cash_margin_pct=Decimal("15.00"),
                default_min_commission=Decimal("1000.00"),
                default_flat_fee=Decimal("500.00"),
                max_tenor_days=730,
                lg_type_ids=[],
            )
            db.add(sl_nsb)
            db.flush()
        else:
            sl_nsb.max_tenor_days = 730

        # 4. Ensure CustomerBankAccount for FNCB so bank form auto-fills account/IBAN/CIF!
        cba = (
            db.query(mi.CustomerBankAccount)
            .filter(
                mi.CustomerBankAccount.customer_id == cust_id,
                mi.CustomerBankAccount.bank_id == fncb.id,
            )
            .first()
        )
        if not cba:
            cba = mi.CustomerBankAccount(
                customer_id=cust_id,
                bank_id=fncb.id,
                entity_id=entity_hq.id,
                account_name="Apex Global Engineering SAE — Corporate Operating",
                account_number="0042-881920-001",
                iban="EG420042000100000042881920001",
                branch_name="Cairo Corporate Tower Branch",
                customer_number="CIF-994021",
                is_default=True,
                is_active=True,
                is_deleted=False,
            )
            db.add(cba)
            db.flush()

        # 5. Ensure FNCB Official BankFormTemplate (SCANNED_FILL) exists locally!
        blank_form_rel = os.path.join("uploads", "bank_forms", str(fncb.id), "v1_FNCB_Official_LG_Form.pdf")
        blank_form_abs = os.path.join(ROOT_DIR, blank_form_rel)
        generate_fncb_blank_bank_form_pdf(blank_form_abs)

        field_mapping = [
            {"pdf_field_name": "applicant_name", "label": "Applicant Legal Entity Name", "mapped_to": "entity_name", "page": 0, "x_pct": 8.5, "y_pct": 20.6, "width_pct": 52, "font_size": 9.5, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "app_date", "label": "Application Date", "mapped_to": "current_date", "page": 0, "x_pct": 66.0, "y_pct": 20.6, "width_pct": 24, "font_size": 9.5, "field_type": "date", "date_format": "YYYY-MM-DD", "language": "shared"},
            {"pdf_field_name": "iban", "label": "Corporate Account / IBAN", "mapped_to": "iban", "page": 0, "x_pct": 8.5, "y_pct": 26.6, "width_pct": 40, "font_size": 8.5, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "bank_branch", "label": "FNCB Branch", "mapped_to": "bank_branch", "page": 0, "x_pct": 53.0, "y_pct": 26.6, "width_pct": 20, "font_size": 8.5, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "cif", "label": "Customer CIF #", "mapped_to": "customer_cif_number", "page": 0, "x_pct": 77.0, "y_pct": 26.6, "width_pct": 15, "font_size": 9, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "beneficiary_name", "label": "Beneficiary Official Name", "mapped_to": "beneficiary_name", "page": 0, "x_pct": 8.5, "y_pct": 34.6, "width_pct": 82, "font_size": 9.5, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "beneficiary_address", "label": "Beneficiary Address", "mapped_to": "beneficiary_address", "page": 0, "x_pct": 8.5, "y_pct": 40.6, "width_pct": 52, "font_size": 8.5, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "reference_number", "label": "Contract Reference", "mapped_to": "reference_number", "page": 0, "x_pct": 66.0, "y_pct": 40.6, "width_pct": 25, "font_size": 9, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "lg_type", "label": "Guarantee Type", "mapped_to": "lg_type", "page": 0, "x_pct": 8.5, "y_pct": 49.1, "width_pct": 33, "font_size": 9, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "amount_with_currency", "label": "Currency & Amount", "mapped_to": "amount_with_currency", "page": 0, "x_pct": 46.0, "y_pct": 49.1, "width_pct": 22, "font_size": 9.5, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "expiry_date", "label": "Requested Expiry Date", "mapped_to": "expiry_date", "page": 0, "x_pct": 72.5, "y_pct": 49.1, "width_pct": 19, "font_size": 9.5, "field_type": "date", "date_format": "YYYY-MM-DD", "language": "shared"},
            {"pdf_field_name": "amount_in_words", "label": "Amount in Words", "mapped_to": "amount_in_words", "page": 0, "x_pct": 8.5, "y_pct": 55.6, "width_pct": 82, "font_size": 8.5, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "lg_purpose", "label": "Underlying Purpose", "mapped_to": "lg_purpose", "page": 0, "x_pct": 8.5, "y_pct": 62.1, "width_pct": 82, "font_size": 8.5, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "additional_conditions", "label": "Applicable Rules & Annex", "mapped_to": "additional_conditions", "page": 0, "x_pct": 8.5, "y_pct": 68.6, "width_pct": 82, "font_size": 8.5, "field_type": "text", "language": "shared"},
            {"pdf_field_name": "cb_domestic", "label": "Domestic LG", "mapped_to": "is_local_lg", "page": 0, "x_pct": 8.0, "y_pct": 74.0, "width_pct": 2, "font_size": 9, "field_type": "checkbox", "language": "shared"},
            {"pdf_field_name": "cb_special", "label": "Special Wording Annex", "mapped_to": "lg_format_is_special", "page": 0, "x_pct": 56.0, "y_pct": 74.0, "width_pct": 2, "font_size": 9, "field_type": "checkbox", "language": "shared"},
        ]

        bft = (
            db.query(mi.BankFormTemplate)
            .filter(
                mi.BankFormTemplate.bank_id == fncb.id,
                mi.BankFormTemplate.is_deleted == False,
            )
            .first()
        )
        if not bft:
            bft = mi.BankFormTemplate(
                bank_id=fncb.id,
                name="FNCB Official LG Application Form (TF-LG-01)",
                version=1,
                form_type="SCANNED_FILL",
                form_role="PRIMARY_ISSUER",
                file_path=blank_form_rel,
                original_filename="FNCB_Official_LG_Form_TF_LG_01.pdf",
                lg_type_ids=None,
                field_mapping=field_mapping,
                ai_analysis_status="COMPLETED",
                form_language="BILINGUAL",
                allows_period_expiry=False,
                is_active=True,
                priority=10,
                is_deleted=False,
                uploaded_by=admin.id,
            )
            db.add(bft)
            db.flush()
        else:
            bft.name = "FNCB Official LG Application Form (TF-LG-01)"
            bft.form_type = "SCANNED_FILL"
            bft.file_path = blank_form_rel
            bft.field_mapping = field_mapping
            bft.ai_analysis_status = "COMPLETED"
            bft.is_active = True

        # 6. Ensure Workflow Policy exists for Credit Approval Matrix
        pol = (
            db.query(mi.IssuanceWorkflowPolicy)
            .filter(
                mi.IssuanceWorkflowPolicy.customer_id == cust_id,
                mi.IssuanceWorkflowPolicy.is_active == True,
            )
            .first()
        )
        if not pol:
            pol = mi.IssuanceWorkflowPolicy(
                customer_id=cust_id,
                step_sequence=1,
                condition_type="ALWAYS",
                approver_type="ROLE",
                approver_values=["corporate_admin"],
                required_signatures=1,
                is_active=True,
            )
            db.add(pol)
            db.flush()

        # 7. Create a prior Issued LG to trigger the 92% Weighted Duplicate / Similarity Check!
        exp_target = date.today() + timedelta(days=270)
        dup_lg = (
            db.query(mi.IssuedLGRecord)
            .filter(
                mi.IssuedLGRecord.customer_id == cust_id,
                mi.IssuedLGRecord.lg_ref_number == "LG-APEX-2026-408A",
            )
            .first()
        )
        if not dup_lg:
            dup_lg = mi.IssuedLGRecord(
                customer_id=cust_id,
                lg_ref_number="LG-APEX-2026-408A",
                internal_serial="ISS-2026-0408",
                bank_lg_number="FNCB-APG-2026-8812",
                beneficiary_name="New Administrative Urban Utilities Authority",
                current_amount=Decimal("12000000.00"),
                currency_id=egp.id,
                lg_type_id=apg_type.id,
                issuing_entity_id=entity_hq.id,
                bank_id=fncb.id,
                facility_sub_limit_id=sl_fncb.id,
                issue_date=date.today() - timedelta(days=18),
                expiry_date=exp_target - timedelta(days=6),
                reference_type="CONTRACT",
                reference_number="NAUUA-2026-408",
                department="Projects & Infrastructure",
                recipient_name="Eng. Tarek El-Masry",
                recipient_email="projects@apex-global.com",
                status="ACTIVE",
                verification_status="VERIFIED",
                issuance_method="BANK_FORM",
                issued_by_user_id=maker.id,
            )
            db.add(dup_lg)
            db.flush()

        # Also create an expired-but-still-ACTIVE Issued LG so Position Reconciliation catches SYSTEM_ONLY!
        stale_lg = (
            db.query(mi.IssuedLGRecord)
            .filter(
                mi.IssuedLGRecord.customer_id == cust_id,
                mi.IssuedLGRecord.lg_ref_number == "LG-APEX-2025-319",
            )
            .first()
        )
        if not stale_lg:
            stale_lg = mi.IssuedLGRecord(
                customer_id=cust_id,
                lg_ref_number="LG-APEX-2025-319",
                internal_serial="ISS-2025-0319",
                bank_lg_number="FNCB-BB-2025-4410",
                beneficiary_name="East Port Said Industrial Zone Authority",
                current_amount=Decimal("3500000.00"),
                currency_id=egp.id,
                lg_type_id=perf_type.id,
                issuing_entity_id=entity_hq.id,
                bank_id=fncb.id,
                facility_sub_limit_id=sl_fncb.id,
                issue_date=date.today() - timedelta(days=395),
                expiry_date=date.today() - timedelta(days=30),
                reference_type="TENDER",
                reference_number="EPS-TND-2025-19",
                department="Tenders & Proposals",
                recipient_name="Eng. Tarek El-Masry",
                recipient_email="projects@apex-global.com",
                status="ACTIVE",  # Still ACTIVE in system, locking EGP 3.5M of credit line!
                verification_status="VERIFIED",
                issuance_method="BANK_FORM",
                issued_by_user_id=maker.id,
            )
            db.add(stale_lg)
            db.flush()

        # 8. Create our Hero IssuanceRequest for Tours 2.1, 2.2, 2.3!
        # Delete any previous demo run of REQ-2026-APEX-408B to keep state crisp
        old_demo_req = (
            db.query(mi.IssuanceRequest)
            .filter(
                mi.IssuanceRequest.customer_id == cust_id,
                mi.IssuanceRequest.serial_number == "REQ-2026-APEX-408B",
            )
            .first()
        )
        if old_demo_req:
            old_demo_req.lg_record_id = None
            db.flush()
            db.query(mi.IssuanceExposureEntry).filter(
                mi.IssuanceExposureEntry.request_id == old_demo_req.id
            ).delete()
            db.query(mi.IssuedLGRecord).filter(
                mi.IssuedLGRecord.lg_ref_number == "LG-REQ-2026-APEX-408B"
            ).delete()
            db.delete(old_demo_req)
            db.commit()

        wording_trap_analysis = {
            "status": "OK",
            "perspective": "APPLICANT",
            "detected_language": "EN / AR",
            "risk_level": "RED",
            "risk_score": 78,
            "risk_label": "2 Applicant Exposure Traps Detected in Draft Beneficiary Wording",
            "executive_summary": (
                "Applicant-side legal scan flagged two high-exposure clauses in the beneficiary's draft Advance Payment "
                "wording: missing Non-Operative condition prior to cash receipt and an Extend-or-Pay demand trigger."
            ),
            "findings": [
                {
                    "severity": "RED",
                    "title": "Unconditional Advance Payment Without Non-Operative Trigger",
                    "plain_explanation": (
                        "Guarantee enters into force immediately upon issuance before the EGP 12,000,000 advance payment "
                        "is credited to Apex Global Engineering's account at FNCB."
                    ),
                    "quoted_excerpt": "This guarantee shall be immediately operative from the date of issue regardless of advance disbursement.",
                    "suggested_protective_rewording": (
                        "This Advance Payment Guarantee shall remain NON-OPERATIVE and shall enter into force strictly upon receipt "
                        "of the full advance payment into Applicant Account IBAN EG420042000100000042881920001 at FNCB."
                    ),
                },
                {
                    "severity": "AMBER",
                    "title": "Extend-or-Pay Coercive Renewal Clause",
                    "plain_explanation": (
                        "Allows beneficiary to treat non-extension 15 days prior to expiry as an automatic liquidation call "
                        "without alleging contractual default."
                    ),
                    "quoted_excerpt": "Should the Bank not extend validity 15 days prior to expiry, this request serves as an immediate claim to pay.",
                    "suggested_protective_rewording": (
                        "Subject to URDG 758 Article 20; any demand for payment must state in what respect the Applicant is in breach "
                        "of its obligations under Contract #NAUUA-2026-408."
                    ),
                },
            ],
            "positive_protections": [
                "Capped maximum liability at EGP 12,000,000.00 (no uncapped interest/cost escalation)",
                "Fixed calendar expiry date aligned with Contract Milestone Schedule",
            ],
        }

        hero_req = mi.IssuanceRequest(
            customer_id=cust_id,
            issuing_entity_id=entity_hq.id,
            requestor_user_id=maker.id,
            serial_number="REQ-2026-APEX-408B",
            status="PENDING_APPROVAL",
            transaction_type="NEW_ISSUANCE",
            requestor_name="Eng. Tarek El-Masry",
            requestor_email="projects@apex-global.com",
            department="Projects & Infrastructure",
            job_title="Project Controls Director",
            phone_number="+201004428810",
            employee_id="APEX-1104",
            manager_email="treasury@apexglobal.com",
            reference_type="CONTRACT",
            reference_number="NAUUA-2026-408",
            reference_amount=Decimal("120000000.00"),
            reference_currency_id=egp.id,
            project_id=proj.id,
            lg_type_id=apg_type.id,
            lg_purpose="10% Advance Payment Guarantee for New Capital Main Water Pumping Station — Package B",
            amount=Decimal("12000000.00"),
            currency_id=egp.id,
            payable_currency_id=egp.id,
            requested_issue_date=date.today(),
            requested_expiry_date=exp_target,
            operational_status="NON_OPERATIVE",
            lg_language="EN",
            beneficiary_name="New Administrative Urban Utilities Authority",
            beneficiary_id_number="GOV-NAUUA-881",
            beneficiary_country="Egypt",
            beneficiary_address="Government District, Plot 14, New Administrative Capital, Cairo",
            beneficiary_contact_person="Eng. Mahmoud El-Shafei",
            beneficiary_email="tenders@nauua-gov.example",
            requires_special_wording=True,
            is_cross_border=False,
            is_third_party=False,
            applicable_rules="URDG_758",
            other_conditions="Include URDG 758 Non-Operative advance receipt clause credited to FNCB Corporate IBAN.",
            current_approval_step=1,
            pending_approver_users=[admin.id],
            signatures_collected=0,
            approval_chain_audit=[
                {
                    "action": "SUBMITTED",
                    "user_id": maker.id,
                    "user_email": "projects@apex-global.com",
                    "step": 1,
                    "note": "Submitted via Department Issuance Portal with Contract Award PDF & Special Wording",
                    "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=25)).isoformat(),
                }
            ],
            metadata_json={"special_wording_analysis": wording_trap_analysis},
        )
        db.add(hero_req)
        db.flush()

        # Attach Contract Award PDF + AI Verification result to hero_req
        contract_pdf_rel = os.path.join("uploads", "issuance_docs", f"Apex_Contract_Award_NAUUA_408_{hero_req.id}.pdf")
        contract_pdf_abs = os.path.join(ROOT_DIR, contract_pdf_rel)
        build_companion_pdf(
            contract_pdf_abs,
            badge="NEW ADMINISTRATIVE URBAN UTILITIES AUTHORITY (NAUUA) · CONTRACT AWARD",
            title="NOTICE OF CONTRACT AWARD — PACKAGE B (#NAUUA-2026-408)",
            subtitle="Awarded Contractor (Applicant): Apex Global Engineering & Contracting SAE",
            rows=[
                ("Employer / Beneficiary", "New Administrative Urban Utilities Authority (NAUUA)"),
                ("Awarded Contractor", "Apex Global Engineering SAE (HQ)"),
                ("Contract Reference", "NAUUA-2026-408 — Main Water Pumping Station Package B"),
                ("Total Contract Value", "EGP 120,000,000.00 (One Hundred Twenty Million Egyptian Pounds)"),
                ("Required Outgoing LG", "10% Advance Payment Guarantee — EGP 12,000,000.00"),
                ("Required Validity", f"Valid through {exp_target.strftime('%Y-%m-%d')} (270 Days)"),
                ("Operative Condition", "Non-Operative until receipt of EGP 12,000,000 advance into Contractor Bank IBAN"),
                ("Governing Rules", "ICC Uniform Rules for Demand Guarantees (URDG 758)"),
            ],
            note=(
                "<b>Treasury Intake Notice:</b> Contractor must instruct its relationship bank to issue the "
                "Advance Payment Guarantee in favor of NAUUA within 10 business days of award signature."
            ),
        )

        doc_ai_result = {
            "status": "OK",
            "mismatches": 0,
            "total_fields_compared": 8,
            "summary": "All 8 core contract award parameters (Beneficiary, EGP 12,000,000.00 10% Advance LG, Contract #NAUUA-2026-408, Expiry, and Non-Operative clause) match the submitted request.",
            "comparison": [
                {"field": "beneficiary_name", "label": "Beneficiary Name", "request_value": "New Administrative Urban Utilities Authority", "document_value": "New Administrative Urban Utilities Authority", "status": "MATCH", "match": True},
                {"field": "amount", "label": "Guarantee Amount (10%)", "request_value": "EGP 12,000,000.00", "document_value": "EGP 12,000,000.00 (10% of EGP 120M)", "status": "MATCH", "match": True},
                {"field": "reference_number", "label": "Contract Reference", "request_value": "NAUUA-2026-408", "document_value": "NAUUA-2026-408", "status": "MATCH", "match": True},
                {"field": "operational_status", "label": "Operative Trigger", "request_value": "NON_OPERATIVE", "document_value": "Non-Operative until advance receipt", "status": "MATCH", "match": True},
                {"field": "requested_expiry_date", "label": "Contract LG Expiry", "request_value": exp_target.strftime("%Y-%m-%d"), "document_value": exp_target.strftime("%Y-%m-%d"), "status": "MATCH", "match": True},
            ],
        }

        req_doc = mi.IssuanceRequestDocument(
            request_id=hero_req.id,
            document_type="CONTRACT",
            file_name="NAUUA_Contract_Award_PackageB_408.pdf",
            file_path=contract_pdf_rel,
            uploaded_by=maker.id,
            ai_verification_result=doc_ai_result,
        )
        db.add(req_doc)
        db.commit()

        # Generate companion sample PDFs for all 4 tours
        os.makedirs(STAGE_DOCS, exist_ok=True)
        doc_2_1 = os.path.join(STAGE_DOCS, "Apex_Contract_Award_NAUUA_2026.pdf")
        shutil.copyfile(contract_pdf_abs, doc_2_1)
        render_pdf_first_page_png(doc_2_1, doc_2_1.replace(".pdf", "_preview.png"))

        doc_2_2 = os.path.join(STAGE_DOCS, "FNCB_Credit_Facility_Agreement_2026.pdf")
        build_companion_pdf(
            doc_2_2,
            badge="FIRST NATIONAL CORPORATE BANK (FNCB) · CORPORATE CREDIT",
            title="MULTI-OPTION GUARANTEE FACILITY AGREEMENT (#FNCB-MOF-2026-01)",
            subtitle="Borrower / Applicant: Apex Global Engineering & Contracting SAE",
            rows=[
                ("Facility Bank", "First National Corporate Bank (FNCB)"),
                ("Approved Total Limit", "EGP 150,000,000.00 (Domestic Infrastructure Sub-Limit: EGP 100,000,000.00)"),
                ("Commission Tariff", "0.85% p.a. (Minimum EGP 500 + EGP 250 Swift/Admin Flat Fee)"),
                ("Cash Margin Requirement", "0.00% Cash Margin on Domestic Sovereign & Utilities Contracts"),
                ("Contractual Bank SLA", "1 Business Day Issuance Turnaround"),
                ("FX & Exposure Governance", "Margin Reduces Exposure Enabled · FX Breach Auto-Suspend Active"),
                ("Facility Expiry Date", (date.today() + timedelta(days=340)).strftime("%Y-%m-%d")),
            ],
            note=(
                "<b>AI Facility Verification:</b> Grow cross-checks this signed credit facility agreement against "
                "configured system sub-limits, commission rates, cash margins, and SLA commitments."
            ),
        )
        render_pdf_first_page_png(doc_2_2, doc_2_2.replace(".pdf", "_preview.png"))

        # Generate REAL auto-filled FNCB Bank Application PDF via pdf_form_filler.py!
        with open(blank_form_abs, "rb") as bf:
            blank_bytes = bf.read()
        db.refresh(hero_req)
        req_dict = build_request_data_dict(hero_req, db, bank_id=fncb.id, form_role="PRIMARY_ISSUER", form_template=bft)
        filled_pdf_bytes = generate_scanned_fill_pdf(
            template_pdf_bytes=blank_bytes,
            field_mapping=field_mapping,
            request_data=req_dict,
            lg_language="EN",
        )
        doc_2_3 = os.path.join(STAGE_DOCS, "FNCB_AutoFilled_LG_Application_2026.pdf")
        with open(doc_2_3, "wb") as out_f:
            out_f.write(filled_pdf_bytes)
        render_pdf_first_page_png(doc_2_3, doc_2_3.replace(".pdf", "_preview.png"))

        # Generate FNCB LG Position Statement Excel (for real upload/matching!) + Companion PDF for 2.4
        active_fncb_lgs = (
            db.query(mi.IssuedLGRecord)
            .filter(
                mi.IssuedLGRecord.customer_id == cust_id,
                mi.IssuedLGRecord.bank_id == fncb.id,
                mi.IssuedLGRecord.status == "ACTIVE",
            )
            .all()
        )
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "FNCB LG Position"
        ws.append(["LG Number", "Beneficiary Name", "Currency", "LG Amount", "Issue Date", "Expiry Date"])
        recon_rows_for_pdf = []
        for idx, r_lg in enumerate(active_fncb_lgs):
            # Skip stale_lg (`LG-APEX-2025-319`) so it triggers SYSTEM_ONLY (expired & dropped by bank, reclaiming EGP 3.5M!)
            if r_lg.lg_ref_number == "LG-APEX-2025-319":
                continue
            b_num = r_lg.bank_lg_number or r_lg.lg_ref_number
            amt = float(r_lg.current_amount or 0)
            exp_d = r_lg.expiry_date.strftime("%Y-%m-%d") if r_lg.expiry_date else "2027-06-30"
            iss_d = r_lg.issue_date.strftime("%Y-%m-%d") if r_lg.issue_date else "2026-01-15"
            # Introduce one crisp AMOUNT reduction variance on the first row (partial reduction at bank)
            if idx == 0 and r_lg.lg_ref_number != "LG-APEX-2026-408A":
                amt = max(100000.0, amt - 250000.0)
            ws.append([b_num, r_lg.beneficiary_name, "EGP", amt, iss_d, exp_d])
            if len(recon_rows_for_pdf) < 5:
                recon_rows_for_pdf.append((b_num, f"{r_lg.beneficiary_name} — EGP {amt:,.2f} (Exp: {exp_d})"))

        # Add 1 BANK_ONLY row (active at bank, missing in system!)
        ws.append([
            "FNCB-PG-2026-9941",
            "Red Sea Desalination & Grid Consortium",
            "EGP",
            1850000.00,
            date.today().strftime("%Y-%m-%d"),
            (date.today() + timedelta(days=190)).strftime("%Y-%m-%d"),
        ])
        recon_rows_for_pdf.append((
            "FNCB-PG-2026-9941 (Bank Only)",
            "Red Sea Desalination & Grid Consortium — EGP 1,850,000.00 (Unrecorded at Corporate)",
        ))
        recon_rows_for_pdf.append((
            "FNCB-BB-2025-4410 (Dropped)",
            "East Port Said Authority — EGP 3,500,000.00 (Expired & Released by Bank -> Reclaim Limit!)",
        ))

        pos_xlsx_path = os.path.join(STAGE_DIR, "FNCB_Monthly_LG_Position_Nov2026.xlsx")
        wb.save(pos_xlsx_path)

        doc_2_4 = os.path.join(STAGE_DOCS, "FNCB_LG_Position_Statement_Nov2026.pdf")
        build_companion_pdf(
            doc_2_4,
            badge="FIRST NATIONAL CORPORATE BANK (FNCB) · POSITION STATEMENT",
            title="MONTHLY OUTGOING LG POSITION REPORT — NOVEMBER 2026",
            subtitle="Corporate Obligor: Apex Global Engineering & Contracting SAE (Facility #FNCB-MOF-2026-01)",
            rows=recon_rows_for_pdf,
            note=(
                "<b>Reconciliation Audit Highlight:</b> Comparing this bank statement against Grow immediately "
                "identifies <b>FNCB-BB-2025-4410 (EGP 3.5M)</b> dropped by the bank (reclaiming trapped credit headroom), "
                "<b>FNCB-PG-2026-9941</b> active at the bank only, and any amount/expiry variances."
            ),
        )
        render_pdf_first_page_png(doc_2_4, doc_2_4.replace(".pdf", "_preview.png"))

        # Generate Issued LG Copy PDF for Step 2.3.4 AI Copy Verification
        issued_scan_pdf = os.path.join(STAGE_DIR, "FNCB_Issued_APG_408B_Scan.pdf")
        generate_issued_lg_scan_pdf(
            issued_scan_pdf,
            lg_number="FNCB-APG-2026-9104",
            amount_str="EGP 12,000,000.00",
            beneficiary="New Administrative Urban Utilities Authority",
            expiry_str=exp_target.strftime("%Y-%m-%d"),
        )

        # Build JWT tokens for Maker & Admin + Department Portal Token
        cust_obj = db.query(models.Customer).filter(models.Customer.id == cust_id).first()
        if cust_obj:
            for attr in ("has_custody_module", "has_issuance_module", "has_quotation_module", "has_reconciliation_module"):
                if hasattr(cust_obj, attr):
                    setattr(cust_obj, attr, True)
        maker.must_change_password = False
        admin.must_change_password = False
        db.commit()

        auth_service = AuthService()
        maker_td = asyncio.run(auth_service.build_user_token_data(db, maker))
        maker_td["must_change_password"] = False
        maker_td["must_accept_policies"] = False
        maker_td["is_mfa_verified"] = True
        maker_td["has_issuance_module"] = True
        maker_td["has_custody_module"] = True
        maker_jwt = create_access_token(maker_td, expires_delta=timedelta(hours=4))

        admin_td = asyncio.run(auth_service.build_user_token_data(db, admin))
        admin_td["must_change_password"] = False
        admin_td["must_accept_policies"] = False
        admin_td["is_mfa_verified"] = True
        admin_td["has_issuance_module"] = True
        admin_td["has_custody_module"] = True
        admin_jwt = create_access_token(admin_td, expires_delta=timedelta(hours=4))

        portal_exp = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        portal_token = encrypt_data(f"{cust_id}|PublicDiscovery|projects@apex-global.com|{portal_exp}")

        return {
            "cust_id": cust_id,
            "fncb_id": fncb.id,
            "fac_fncb_id": fac_fncb.id,
            "sl_fncb_id": sl_fncb.id,
            "hero_req_id": hero_req.id,
            "hero_serial": hero_req.serial_number,
            "dup_lg_id": dup_lg.id,
            "exp_target": exp_target.strftime("%Y-%m-%d"),
            "maker_jwt": maker_jwt,
            "admin_jwt": admin_jwt,
            "portal_token": portal_token,
            "pos_xlsx_path": pos_xlsx_path,
            "issued_scan_pdf": issued_scan_pdf,
            "contract_pdf_abs": contract_pdf_abs,
        }
    finally:
        db.close()


def set_browser_auth(page, token: str, role: str, customer_id: int = 23):
    now_ms = int(time.time() * 1000)
    page.goto("http://localhost:3000/login", wait_until="domcontentloaded")
    page.evaluate(
        """({token, role, customerId, now}) => {
            localStorage.clear();
            sessionStorage.clear();
            localStorage.setItem('jwt_token', token);
            localStorage.setItem('user_role', role);
            localStorage.setItem('customer_id', String(customerId));
            localStorage.setItem('must_change_password', 'false');
            localStorage.setItem('last_activity_at', String(now));
            localStorage.setItem('last_token_refresh_at', String(now));
            localStorage.setItem('i18nextLng', 'en');
            localStorage.setItem('has_custody_module', 'true');
            localStorage.setItem('has_issuance_module', 'true');
        }""",
        {"token": token, "role": role, "customerId": customer_id, "now": now_ms},
    )


def api_json(method: str, path: str, token: str, payload: dict = None):
    import urllib.request
    import urllib.error
    url = f"http://127.0.0.1:8000{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else b""
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data if method != "GET" else None, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="ignore")
        print(f"  [api warn] {method} {path} -> {e.code}: {err_body[:300]}")
        return {}


def api_multipart_data(path: str, token: str, data_dict: dict):
    import urllib.request
    import urllib.error
    boundary = "----GrowDemoBoundary98421"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="data"\r\n\r\n'
        f"{json.dumps(data_dict)}\r\n"
        f"--{boundary}--\r\n"
    ).encode("utf-8")
    req = urllib.request.Request(
        f"http://127.0.0.1:8000{path}",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        print(f"  [api warn] POST {path} -> {e.code}: {e.read().decode('utf-8', errors='ignore')[:300]}")
        return {}


def hide_toasts(page):
    try:
        page.add_style_tag(
            content="""
            .Toastify, .Toastify__toast-container,
            div.fixed.bottom-4.right-4 { display: none !important; opacity: 0 !important; pointer-events: none !important; }
            """
        )
    except Exception:
        pass


def measure_box_pct(page, locator, pad: int = 6, fallback: dict = None) -> dict:
    try:
        cnt = locator.count()
        for i in range(cnt):
            el = locator.nth(i)
            if el.is_visible():
                box = el.bounding_box()
                if box and box["width"] > 8 and box["height"] > 8:
                    x = max(0.0, box["x"] - pad)
                    y = max(0.0, box["y"] - pad)
                    w = min(VIEWPORT_W - x, box["width"] + pad * 2)
                    h = min(VIEWPORT_H - y, box["height"] + pad * 2)
                    return {
                        "xPct": round(x / VIEWPORT_W * 100.0, 2),
                        "yPct": round(y / VIEWPORT_H * 100.0, 2),
                        "wPct": round(w / VIEWPORT_W * 100.0, 2),
                        "hPct": round(h / VIEWPORT_H * 100.0, 2),
                    }
    except Exception as exc:
        print(f"  [warn] measure_box_pct fallback: {exc}")
    return fallback or {"xPct": 25.0, "yPct": 22.0, "wPct": 50.0, "hPct": 16.0}


def capture_all_issuance_tours(ctx_data: dict) -> dict:
    tours_manifest = {}
    for d in ["issuance_2_1", "issuance_2_2", "issuance_2_3", "issuance_2_4"]:
        os.makedirs(os.path.join(STAGE_SHOTS, d), exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        context = browser.new_context(
            viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
            device_scale_factor=2,
        )
        page = context.new_page()

        # ======================================================================
        # TOUR 2.1: Department Request, AI Trap Scan & Credit Approval (6 steps)
        # ======================================================================
        print("=== Capturing Tour 2.1: issuance-intake ===")
        steps_2_1 = []

        # Step 1: Tokenized Department Requestor Dashboard (`/public-issuance/dashboard?token=...`)
        portal_tok = ctx_data["portal_token"]
        page.goto(
            f"http://localhost:3000/public-issuance/dashboard?token={portal_tok}",
            wait_until="domcontentloaded",
        )
        time.sleep(1.5)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_1", "step_01_department_portal.png")
        page.screenshot(path=shot)
        hs1 = measure_box_pct(
            page,
            page.locator("button:has-text('New Request')"),
            pad=8,
            fallback={"xPct": 76.0, "yPct": 10.0, "wPct": 16.0, "hPct": 7.0},
        )
        steps_2_1.append({
            "step": 1,
            "key": "department_portal",
            "url": "app.growbusinessdevelopment.com/public-issuance/dashboard",
            "image": "/demo/shots/issuance_2_1/step_01_department_portal.png",
            "hotspot": hs1,
        })

        # Step 2: 4-Step Outgoing Guarantee Request Wizard (`/public-issuance/form?token=...`)
        page.goto(
            f"http://localhost:3000/public-issuance/form?token={portal_tok}",
            wait_until="domcontentloaded",
        )
        time.sleep(1.3)
        hide_toasts(page)
        # Fill clean department fields on Step 1 of wizard so screenshot looks rich
        try:
            inputs = page.locator("input[type='text'], input[type='email'], input[type='tel']")
            if inputs.count() >= 2:
                name_in = page.locator("input[placeholder*='Name'], input[name='requestor_name']").first
                if name_in.count() and not name_in.input_value():
                    name_in.fill("Eng. Tarek El-Masry")
        except Exception:
            pass
        shot = os.path.join(STAGE_SHOTS, "issuance_2_1", "step_02_request_wizard.png")
        page.screenshot(path=shot)
        hs2 = measure_box_pct(
            page,
            page.locator("button:has-text('Next')"),
            pad=8,
            fallback={"xPct": 72.0, "yPct": 80.0, "wPct": 14.0, "hPct": 7.0},
        )
        steps_2_1.append({
            "step": 2,
            "key": "request_wizard",
            "url": "app.growbusinessdevelopment.com/public-issuance/form",
            "image": "/demo/shots/issuance_2_1/step_02_request_wizard.png",
            "hotspot": hs2,
        })

        # Step 3: Treasury Requests Inbox -> Open Hero Request -> Click AI Verified Contract Badge
        set_browser_auth(page, ctx_data["maker_jwt"], "end_user")
        page.goto(
            f"http://localhost:3000/end-user/issuance/requests?request_id={ctx_data['hero_req_id']}",
            wait_until="domcontentloaded",
        )
        time.sleep(2.2)
        hide_toasts(page)
        if not page.locator(f"h2:has-text('{ctx_data['hero_serial']}')").is_visible():
            row_loc = page.locator(f"tr:has-text('{ctx_data['hero_serial']}')").first
            if row_loc.count():
                row_loc.locator("button").first.click()
                time.sleep(1.5)
        hide_toasts(page)

        # Scroll modal left pane to Attached Documents & click "Verified" pill to show AI Contract Verification!
        verified_btn = page.locator("button:has-text('Verified')").first
        if verified_btn.count():
            verified_btn.scroll_into_view_if_needed()
            time.sleep(0.4)
            verified_btn.click()
            time.sleep(0.8)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_1", "step_03_ai_contract_verify.png")
        page.screenshot(path=shot)
        hs3 = measure_box_pct(
            page,
            page.locator("div[class*='z-[70]'] > div, button:has-text('Verified')"),
            pad=6,
            fallback={"xPct": 31.0, "yPct": 9.0, "wPct": 38.0, "hPct": 82.0},
        )
        steps_2_1.append({
            "step": 3,
            "key": "ai_contract_verify",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/requests#ai-doc-verify",
            "image": "/demo/shots/issuance_2_1/step_03_ai_contract_verify.png",
            "hotspot": hs3,
        })
        # Close docAnalysis popup (z-[70] overlay)
        close_doc_x = page.locator("div[class*='z-[70]'] button").first
        if close_doc_x.count() and close_doc_x.is_visible():
            close_doc_x.click()
            time.sleep(0.5)
        elif page.locator("div[class*='z-[70]']").count():
            page.mouse.click(40, 40)
            time.sleep(0.5)

        # Step 4: Weighted Duplicate / Similarity Check on Right Pane (`Duplicate Check · 1 match`)
        dup_sec = page.locator("button:has-text('Duplicate Check')").first
        if dup_sec.count():
            dup_sec.scroll_into_view_if_needed()
            time.sleep(0.5)
            if not page.locator("text=LG-APEX-2026-408A").is_visible():
                dup_sec.click()
                time.sleep(0.6)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_1", "step_04_duplicate_guardrail.png")
        page.screenshot(path=shot)
        hs4 = measure_box_pct(
            page,
            page.locator("div:has-text('LG-APEX-2026-408A')").last,
            pad=8,
            fallback={"xPct": 63.0, "yPct": 48.0, "wPct": 29.0, "hPct": 28.0},
        )
        steps_2_1.append({
            "step": 4,
            "key": "duplicate_guardrail",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/requests#duplicate-check",
            "image": "/demo/shots/issuance_2_1/step_04_duplicate_guardrail.png",
            "hotspot": hs4,
        })

        # Step 5: Bilingual Applicant Special-Wording Trap Scanner (`🔴 High Risk · Risk Score: 78/100`)
        trap_card = page.locator("h4:has-text('Trap Detector')").first
        if trap_card.count():
            trap_card.scroll_into_view_if_needed()
            time.sleep(0.6)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_1", "step_05_applicant_trap_scan.png")
        page.screenshot(path=shot)
        hs5 = measure_box_pct(
            page,
            page.locator("div:has(h4:has-text('Trap Detector'))").last,
            pad=6,
            fallback={"xPct": 9.0, "yPct": 24.0, "wPct": 53.0, "hPct": 58.0},
        )
        steps_2_1.append({
            "step": 5,
            "key": "applicant_trap_scan",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/requests#wording-trap-scan",
            "image": "/demo/shots/issuance_2_1/step_05_applicant_trap_scan.png",
            "hotspot": hs5,
        })

        # Step 6: Corporate Admin Credit Approval Matrix (`/corporate-admin/approval-requests` -> Issuance Requests)
        set_browser_auth(page, ctx_data["admin_jwt"], "corporate_admin")
        page.goto("http://localhost:3000/corporate-admin/approval-requests", wait_until="domcontentloaded")
        time.sleep(1.6)
        hide_toasts(page)
        iss_tab = page.locator("button:has-text('Issuance Requests')").first
        if iss_tab.count():
            iss_tab.click()
            time.sleep(1.0)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_1", "step_06_credit_approval_matrix.png")
        page.screenshot(path=shot)
        hs6 = measure_box_pct(
            page,
            page.locator("button:has-text('Approve')").first,
            pad=8,
            fallback={"xPct": 74.0, "yPct": 34.0, "wPct": 16.0, "hPct": 8.0},
        )
        steps_2_1.append({
            "step": 6,
            "key": "credit_approval_matrix",
            "url": "app.growbusinessdevelopment.com/corporate-admin/approval-requests",
            "image": "/demo/shots/issuance_2_1/step_06_credit_approval_matrix.png",
            "hotspot": hs6,
        })

        # Approve hero_req in DB via real API/service so Tour 2.2 sees APPROVED_INTERNAL!
        api_json(
            "POST",
            f"/api/v1/issuance/requests/{ctx_data['hero_req_id']}/approve",
            ctx_data["admin_jwt"],
            {"notes": "Approved under FNCB Infrastructure Umbrella Facility"},
        )
        time.sleep(0.5)

        tours_manifest["issuance-intake"] = {
            "branchId": "issuance-intake",
            "moduleId": "issuance",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "Contract Award & Outgoing LG Mandate",
                "titleAr": "خطاب الترسية وطلب إصدار الضمان الصادر",
                "pdfUrl": "/demo/docs/Apex_Contract_Award_NAUUA_2026.pdf",
                "previewPngUrl": "/demo/docs/Apex_Contract_Award_NAUUA_2026_preview.png",
            },
            "steps": steps_2_1,
        }

        # ======================================================================
        # TOUR 2.2: Multi-Bank Facility Matrix & 0-100 Smart Scoring (5 steps)
        # ======================================================================
        print("=== Capturing Tour 2.2: issuance-facility-scoring ===")
        steps_2_2 = []

        # Step 1: Corporate Admin Multi-Bank Credit Facilities (`/corporate-admin/issuance/facilities`)
        page.goto("http://localhost:3000/corporate-admin/issuance/facilities", wait_until="domcontentloaded")
        time.sleep(1.8)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_2", "step_01_multi_bank_facilities.png")
        page.screenshot(path=shot)
        hs_22_1 = measure_box_pct(
            page,
            page.locator("div:has-text('FNCB Multi-Option Corporate Umbrella Facility')").last,
            pad=8,
            fallback={"xPct": 19.0, "yPct": 44.0, "wPct": 25.0, "hPct": 44.0},
        )
        steps_2_2.append({
            "step": 1,
            "key": "multi_bank_facilities",
            "url": "app.growbusinessdevelopment.com/corporate-admin/issuance/facilities",
            "image": "/demo/shots/issuance_2_2/step_01_multi_bank_facilities.png",
            "hotspot": hs_22_1,
        })

        # Step 2: AI Facility Agreement Verification Modal on FacilitiesPage
        # Intercept `/facilities/{id}/analyze-agreement` to guarantee deterministic fast response matching real schema!
        def mock_agreement_ai(route):
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({
                    "status": "OK",
                    "mismatches": 0,
                    "comparison": [
                        {"label": "Total Facility Limit", "current_value": "EGP 150,000,000", "agreement_value": "EGP 150,000,000", "match": True},
                        {"label": "Domestic Sub-Limit", "current_value": "EGP 100,000,000", "agreement_value": "EGP 100,000,000", "match": True},
                        {"label": "Commission Rate", "current_value": "0.85% p.a.", "agreement_value": "0.85% p.a. (Min EGP 500)", "match": True},
                        {"label": "Cash Margin %", "current_value": "0.00%", "agreement_value": "0% on Sovereign / Utilities Contracts", "match": True},
                        {"label": "Contractual SLA", "current_value": "1 Business Day", "agreement_value": "24 Hours / 1 Business Day", "match": True},
                    ],
                    "special_terms": [
                        "Cash margin reduces net credit line exposure 1:1",
                        "Automatic FX revaluation protection enabled on foreign-currency utilizations",
                    ],
                }),
            )

        page.route("**/api/v1/facilities/*/analyze-agreement", mock_agreement_ai)
        file_inputs = page.locator("input[type='file']")
        if file_inputs.count():
            file_inputs.first.set_input_files(os.path.join(STAGE_DOCS, "FNCB_Credit_Facility_Agreement_2026.pdf"))
            time.sleep(1.2)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_2", "step_02_ai_agreement_verify.png")
        page.screenshot(path=shot)
        hs_22_2 = measure_box_pct(
            page,
            page.locator("div:has(h3:has-text('Agreement Verification'))").last,
            pad=6,
            fallback={"xPct": 32.0, "yPct": 12.0, "wPct": 36.0, "hPct": 74.0},
        )
        steps_2_2.append({
            "step": 2,
            "key": "ai_agreement_verify",
            "url": "app.growbusinessdevelopment.com/corporate-admin/issuance/facilities#ai-agreement-audit",
            "image": "/demo/shots/issuance_2_2/step_02_ai_agreement_verify.png",
            "hotspot": hs_22_2,
        })
        page.unroute("**/api/v1/facilities/*/analyze-agreement")

        # Step 3: End User Opens Approved Request -> Right-Side 0-100 Smart Facility Ranking Cards!
        set_browser_auth(page, ctx_data["maker_jwt"], "end_user")
        page.goto(
            f"http://localhost:3000/end-user/issuance/requests?request_id={ctx_data['hero_req_id']}",
            wait_until="domcontentloaded",
        )
        time.sleep(2.2)
        hide_toasts(page)
        if not page.locator(f"h2:has-text('{ctx_data['hero_serial']}')").is_visible():
            row_loc = page.locator(f"tr:has-text('{ctx_data['hero_serial']}')").first
            if row_loc.count():
                row_loc.locator("button").first.click()
                time.sleep(1.5)
        hide_toasts(page)
        # Scroll right column to Smart Facility Recommendation cards
        rec_hdr = page.locator("text=First National Corporate Bank").first
        if rec_hdr.count():
            rec_hdr.scroll_into_view_if_needed()
            time.sleep(0.5)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_2", "step_03_smart_facility_scoring.png")
        page.screenshot(path=shot)
        hs_22_3 = measure_box_pct(
            page,
            page.locator("div:has-text('First National Corporate Bank'):has-text('/100')").last,
            pad=6,
            fallback={"xPct": 63.0, "yPct": 30.0, "wPct": 30.0, "hPct": 48.0},
        )
        steps_2_2.append({
            "step": 3,
            "key": "smart_facility_scoring",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/requests#facility-scoring",
            "image": "/demo/shots/issuance_2_2/step_03_smart_facility_scoring.png",
            "hotspot": hs_22_3,
        })

        # Step 4: Click "Issue LG" / Open IssuanceWizardModal Step 1 (What-If Pipeline Headroom & Reserve)
        issue_btn = page.locator("button:has-text('Issue LG'), button:has-text('Issue to Bank'), button:has-text('Execute Issuance')").first
        if issue_btn.count():
            issue_btn.scroll_into_view_if_needed()
            issue_btn.click(force=True)
            time.sleep(1.5)
        hide_toasts(page)
        # Click FNCB facility card in Wizard Step 1
        wiz_fncb = page.locator("button:has-text('First National Corporate Bank')").first
        if wiz_fncb.count():
            wiz_fncb.click()
            time.sleep(0.6)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_2", "step_04_pipeline_headroom_reserve.png")
        page.screenshot(path=shot)
        hs_22_4 = measure_box_pct(
            page,
            page.locator("button:has-text('Reserve'), button:has-text('First National Corporate Bank')").first,
            pad=8,
            fallback={"xPct": 27.0, "yPct": 24.0, "wPct": 46.0, "hPct": 32.0},
        )
        steps_2_2.append({
            "step": 4,
            "key": "pipeline_headroom_reserve",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/requests#wizard-select-bank",
            "image": "/demo/shots/issuance_2_2/step_04_pipeline_headroom_reserve.png",
            "hotspot": hs_22_4,
        })

        # Reserve the facility via real API so Step 5 shows FACILITY_RESERVED lock!
        api_json(
            "POST",
            f"/api/v1/issuance/requests/{ctx_data['hero_req_id']}/reserve?sub_limit_id={ctx_data['sl_fncb_id']}",
            ctx_data["maker_jwt"],
        )
        page.goto(
            f"http://localhost:3000/end-user/issuance/requests?request_id={ctx_data['hero_req_id']}",
            wait_until="domcontentloaded",
        )
        time.sleep(2.0)
        hide_toasts(page)
        if not page.locator(f"h2:has-text('{ctx_data['hero_serial']}')").is_visible():
            row_loc = page.locator(f"tr:has-text('{ctx_data['hero_serial']}')").first
            if row_loc.count():
                row_loc.locator("button").first.click()
                time.sleep(1.5)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_2", "step_05_facility_reserved_lock.png")
        page.screenshot(path=shot)
        hs_22_5 = measure_box_pct(
            page,
            page.locator("button:has-text('Proceed to Issue'), button:has-text('Issue'), span:has-text('FACILITY RESERVED')").first,
            pad=8,
            fallback={"xPct": 63.0, "yPct": 52.0, "wPct": 30.0, "hPct": 24.0},
        )
        steps_2_2.append({
            "step": 5,
            "key": "facility_reserved_lock",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/requests#facility-reserved",
            "image": "/demo/shots/issuance_2_2/step_05_facility_reserved_lock.png",
            "hotspot": hs_22_5,
        })

        tours_manifest["issuance-facility-scoring"] = {
            "branchId": "issuance-facility-scoring",
            "moduleId": "issuance",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "FNCB Credit Facility Agreement",
                "titleAr": "عقد التسهيلات الائتمانية مع بنك FNCB",
                "pdfUrl": "/demo/docs/FNCB_Credit_Facility_Agreement_2026.pdf",
                "previewPngUrl": "/demo/docs/FNCB_Credit_Facility_Agreement_2026_preview.png",
            },
            "steps": steps_2_2,
        }

        # ======================================================================
        # TOUR 2.3: Auto-Filled Official Bank PDF Form & AI Issued-Copy Check (5 steps)
        # ======================================================================
        print("=== Capturing Tour 2.3: issuance-bank-form ===")
        steps_2_3 = []

        # Open IssuanceWizardModal from FACILITY_RESERVED request -> lands directly on Step 2 (Choose Method)!
        proc_btn = page.locator(
            "button:has-text('Proceed to Issue'), button:has-text('Issue LG'), button:has-text('Complete Issuance'), button:has-text('Issue to Bank')"
        ).first
        if proc_btn.count():
            proc_btn.scroll_into_view_if_needed()
            proc_btn.click(force=True)
            time.sleep(2.4)
        hide_toasts(page)
        # Select BANK_FORM card
        bf_card = page.locator("button:has-text('Bank Form'), button:has-text('Official Bank')").first
        if bf_card.count():
            bf_card.click()
            time.sleep(0.8)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_3", "step_01_choose_bank_form_method.png")
        page.screenshot(path=shot)
        hs_23_1 = measure_box_pct(
            page,
            page.locator("button:has-text('Next'), button:has-text('Bank Form')").first,
            pad=8,
            fallback={"xPct": 28.0, "yPct": 28.0, "wPct": 44.0, "hPct": 28.0},
        )
        steps_2_3.append({
            "step": 1,
            "key": "choose_bank_form_method",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/requests#choose-method",
            "image": "/demo/shots/issuance_2_3/step_01_choose_bank_form_method.png",
            "hotspot": hs_23_1,
        })

        # Step 2: Advance to Wizard Step 3 (Confirm & Issue — Auto-Filled FNCB Form TF-LG-01 + Companion Annex)
        next_wiz = page.locator("button:has-text('Next')").last
        if next_wiz.count() and next_wiz.is_visible():
            next_wiz.click()
            try:
                page.locator("text=Processing Issuance").wait_for(state="hidden", timeout=10000)
            except Exception:
                pass
            time.sleep(2.4)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_3", "step_02_autofill_confirm_issue.png")
        page.screenshot(path=shot)
        hs_23_2 = measure_box_pct(
            page,
            page.locator("button:has-text('Confirm & Issue'), button:has-text('Fill & Issue'), button:has-text('Issue')").last,
            pad=8,
            fallback={"xPct": 58.0, "yPct": 82.0, "wPct": 16.0, "hPct": 7.0},
        )
        steps_2_3.append({
            "step": 2,
            "key": "autofill_confirm_issue",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/requests#confirm-bank-form",
            "image": "/demo/shots/issuance_2_3/step_02_autofill_confirm_issue.png",
            "hotspot": hs_23_2,
        })

        # Execute real `/issue` via API so IssuedLGRecord is created cleanly!
        issued_res = api_json(
            "POST",
            f"/api/v1/issuance/requests/{ctx_data['hero_req_id']}/issue",
            ctx_data["maker_jwt"],
            {
                "sub_limit_id": ctx_data["sl_fncb_id"],
                "bank_id": ctx_data["fncb_id"],
                "issued_ref_number": "LG-REQ-2026-APEX-408B",
                "expiry_date": ctx_data["exp_target"],
                "issuance_method": "BANK_FORM",
            },
        )
        new_lg_id = issued_res.get("id")
        print(f"  [ok] Issued LG created: id={new_lg_id}, ref={issued_res.get('lg_ref_number')}")

        # Step 3: Navigate to `/end-user/issuance/issued-lgs`, open new LG's PostIssuanceTracker -> Step 2 Delivery!
        page.goto("http://localhost:3000/end-user/issuance/issued-lgs", wait_until="domcontentloaded")
        time.sleep(1.8)
        hide_toasts(page)
        lg_row = page.locator("tr:has-text('LG-REQ-2026-APEX-408B'), div:has-text('LG-REQ-2026-APEX-408B')").first
        if lg_row.count():
            lg_row.click()
            time.sleep(1.6)
        tl_tab = page.locator("button:has-text('TIMELINE'), button:has-text('Timeline')").first
        if tl_tab.count() and tl_tab.is_visible():
            tl_tab.click()
            time.sleep(1.0)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_3", "step_03_post_issuance_delivery.png")
        page.screenshot(path=shot)
        hs_23_3 = measure_box_pct(
            page,
            page.locator("button:has-text('Record Delivery'), button:has-text('Confirm Delivery')").first,
            pad=8,
            fallback={"xPct": 55.0, "yPct": 58.0, "wPct": 22.0, "hPct": 8.0},
        )
        steps_2_3.append({
            "step": 3,
            "key": "post_issuance_delivery",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/issued-lgs#delivery-tracking",
            "image": "/demo/shots/issuance_2_3/step_03_post_issuance_delivery.png",
            "hotspot": hs_23_3,
        })

        # Record delivery via API so Step 4 opens BANK_REPLY -> "LG Issued" -> AI Copy Verification!
        if new_lg_id:
            api_multipart_data(
                f"/api/v1/issuance/lg-records/{new_lg_id}/record-delivery",
                ctx_data["maker_jwt"],
                {
                    "delivery_method": "HAND_DELIVERY",
                    "delivery_date": date.today().isoformat(),
                    "delivery_notes": "Signed FNCB Form TF-LG-01 delivered to Trade Finance Desk",
                },
            )

        # Reload & open LG detail -> select "LG Issued" -> upload scan to show AI Copy Verification!
        def mock_extract_lg_copy(route):
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({
                    "extracted": {
                        "lg_number": "FNCB-APG-2026-9104",
                        "amount": 12000000.0,
                        "currency": "EGP",
                        "issue_date": date.today().isoformat(),
                        "expiry_date": ctx_data["exp_target"],
                        "beneficiary_name": "New Administrative Urban Utilities Authority",
                        "issuing_bank_name": "First National Corporate Bank",
                        "issuer_name": "Apex Global Engineering SAE (HQ)",
                    },
                    "comparison": {
                        "overall_match": True,
                        "has_discrepancy": False,
                        "fields": [
                            {"field": "amount", "label": "Guarantee Amount", "requested": "12,000,000.00", "extracted": "12,000,000.00", "match": True},
                            {"field": "currency", "label": "Currency", "requested": "EGP", "extracted": "EGP", "match": True},
                            {"field": "beneficiary_name", "label": "Beneficiary", "requested": "New Administrative Urban Utilities Authority", "extracted": "New Administrative Urban Utilities Authority", "match": True},
                            {"field": "expiry_date", "label": "Expiry Date", "requested": ctx_data["exp_target"], "extracted": ctx_data["exp_target"], "match": True},
                            {"field": "issuing_bank", "label": "Issuing Bank", "requested": "First National Corporate Bank", "extracted": "First National Corporate Bank", "match": True},
                        ],
                    },
                }),
            )

        page.route("**/api/v1/issuance/lg-records/*/extract-lg-copy", mock_extract_lg_copy)
        page.goto("http://localhost:3000/end-user/issuance/issued-lgs", wait_until="domcontentloaded")
        time.sleep(1.6)
        hide_toasts(page)
        lg_row = page.locator("tr:has-text('LG-REQ-2026-APEX-408B')").first
        if lg_row.count():
            lg_row.click()
            time.sleep(1.5)
        tl_tab2 = page.locator("button:has-text('TIMELINE'), button:has-text('Timeline')").first
        if tl_tab2.count() and tl_tab2.is_visible():
            tl_tab2.click()
            time.sleep(1.0)
        lg_issued_card = page.locator("button:has-text('LG Issued'), div:has-text('Upload scanned LG')").first
        if lg_issued_card.count():
            lg_issued_card.click()
            time.sleep(0.6)
        f_in = page.locator("input[type='file']").first
        if f_in.count():
            f_in.set_input_files(ctx_data["issued_scan_pdf"])
            time.sleep(1.2)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_3", "step_04_ai_issued_copy_verify.png")
        page.screenshot(path=shot)
        hs_23_4 = measure_box_pct(
            page,
            page.locator("button:has-text('Confirm & Verify'), button:has-text('Save & Verify'), div:has-text('FNCB-APG-2026-9104')").first,
            pad=8,
            fallback={"xPct": 48.0, "yPct": 35.0, "wPct": 44.0, "hPct": 46.0},
        )
        steps_2_3.append({
            "step": 4,
            "key": "ai_issued_copy_verify",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/issued-lgs#ai-copy-verification",
            "image": "/demo/shots/issuance_2_3/step_04_ai_issued_copy_verify.png",
            "hotspot": hs_23_4,
        })
        page.unroute("**/api/v1/issuance/lg-records/*/extract-lg-copy")

        # Complete bank reply + verification in DB so LG becomes ACTIVE + VERIFIED!
        if new_lg_id:
            db_v = SessionLocal()
            try:
                lg_obj = db_v.query(mi.IssuedLGRecord).filter(mi.IssuedLGRecord.id == new_lg_id).first()
                if lg_obj:
                    lg_obj.status = "ACTIVE"
                    lg_obj.verification_status = "VERIFIED"
                    lg_obj.bank_reply_type = "LG_ISSUED"
                    lg_obj.bank_lg_number = "FNCB-APG-2026-9104"
                    lg_obj.issue_date = date.today()
                    db_v.commit()
            finally:
                db_v.close()

        # Step 5: Active Outgoing Issued LGs Register (`/end-user/issuance/issued-lgs`)
        page.goto("http://localhost:3000/end-user/issuance/issued-lgs", wait_until="domcontentloaded")
        time.sleep(1.8)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_3", "step_05_active_issued_portfolio.png")
        page.screenshot(path=shot)
        hs_23_5 = measure_box_pct(
            page,
            page.locator("tr:has-text('FNCB-APG-2026-9104'), tr:has-text('LG-REQ-2026-APEX-408B')").first,
            pad=6,
            fallback={"xPct": 19.0, "yPct": 28.0, "wPct": 77.0, "hPct": 10.0},
        )
        steps_2_3.append({
            "step": 5,
            "key": "active_issued_portfolio",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/issued-lgs",
            "image": "/demo/shots/issuance_2_3/step_05_active_issued_portfolio.png",
            "hotspot": hs_23_5,
        })

        tours_manifest["issuance-bank-form"] = {
            "branchId": "issuance-bank-form",
            "moduleId": "issuance",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "Auto-Filled FNCB Official Form",
                "titleAr": "استمارة بنك FNCB المعبأة تلقائياً",
                "pdfUrl": "/demo/docs/FNCB_AutoFilled_LG_Application_2026.pdf",
                "previewPngUrl": "/demo/docs/FNCB_AutoFilled_LG_Application_2026_preview.png",
            },
            "steps": steps_2_3,
        }

        # ======================================================================
        # TOUR 2.4: Post-Issuance Governance & Bank Position Reconciliation (5 steps)
        # ======================================================================
        print("=== Capturing Tour 2.4: issuance-position-recon ===")
        steps_2_4 = []

        # Step 1: Open Active Issued LG Detail -> Maintenance Actions (Close/Return Direct vs Extend/Increase Matrix)
        lg_row = page.locator("tr:has-text('FNCB-APG-2026-9104'), tr:has-text('LG-APEX-2026-408A')").first
        if lg_row.count():
            lg_row.click()
            time.sleep(1.5)
        maint_tab = page.locator("button:has-text('Maintenance'), button:has-text('Actions')").first
        if maint_tab.count() and maint_tab.is_visible():
            maint_tab.click()
            time.sleep(0.8)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_4", "step_01_outgoing_maintenance_governance.png")
        page.screenshot(path=shot)
        hs_24_1 = measure_box_pct(
            page,
            page.locator("button:has-text('Extend'), button:has-text('Close'), button:has-text('Increase')").first,
            pad=8,
            fallback={"xPct": 48.0, "yPct": 22.0, "wPct": 44.0, "hPct": 20.0},
        )
        steps_2_4.append({
            "step": 1,
            "key": "outgoing_maintenance_governance",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/issued-lgs#maintenance",
            "image": "/demo/shots/issuance_2_4/step_01_outgoing_maintenance_governance.png",
            "hotspot": hs_24_1,
        })

        # Step 2: Navigate to `/end-user/issuance/reconciliation` -> Upload Tab (FNCB selected)
        page.goto("http://localhost:3000/end-user/issuance/reconciliation", wait_until="domcontentloaded")
        time.sleep(1.5)
        hide_toasts(page)
        up_tab = page.locator("button:has-text('Upload')").first
        if up_tab.count():
            up_tab.click()
            time.sleep(0.7)
        # Select First National Corporate Bank in dropdown
        sel = page.locator("select").first
        if sel.count():
            sel.select_option(value=str(ctx_data["fncb_id"]))
        date_in = page.locator("input[type='date']").first
        if date_in.count():
            date_in.fill(date.today().isoformat())
        f_up = page.locator("input[type='file']").first
        if f_up.count():
            f_up.set_input_files(ctx_data["pos_xlsx_path"])
        notes_in = page.locator("textarea, input[placeholder*='note' i]").first
        if notes_in.count():
            notes_in.fill("FNCB Monthly Outgoing LG Position Statement — November 2026")
        time.sleep(0.6)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_4", "step_02_upload_position_statement.png")
        page.screenshot(path=shot)
        hs_24_2 = measure_box_pct(
            page,
            page.locator("button:has-text('Upload & Parse')").first,
            pad=8,
            fallback={"xPct": 22.0, "yPct": 74.0, "wPct": 74.0, "hPct": 6.0},
        )
        steps_2_4.append({
            "step": 2,
            "key": "upload_position_statement",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/reconciliation#upload",
            "image": "/demo/shots/issuance_2_4/step_02_upload_position_statement.png",
            "hotspot": hs_24_2,
        })

        # Click Upload & Parse & Run Matching!
        submit_up = page.locator("button:has-text('Upload & Parse')").first
        if submit_up.count():
            submit_up.click()
            time.sleep(2.8)
        run_match_btn = page.locator("button:has-text('Run Matching'), button:has-text('Match')").first
        if run_match_btn.count() and run_match_btn.is_visible():
            run_match_btn.click()
            time.sleep(2.2)
        hide_toasts(page)

        # Step 3: 3-Way Position Matching Dashboard (Matched vs System Only vs Bank Only vs Mismatched)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_4", "step_03_position_variance_matrix.png")
        page.screenshot(path=shot)
        hs_24_3 = measure_box_pct(
            page,
            page.locator("div:has-text('System Only'), div:has-text('Bank Only')").first,
            pad=8,
            fallback={"xPct": 19.0, "yPct": 20.0, "wPct": 76.0, "hPct": 18.0},
        )
        steps_2_4.append({
            "step": 3,
            "key": "position_variance_matrix",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/reconciliation#session-results",
            "image": "/demo/shots/issuance_2_4/step_03_position_variance_matrix.png",
            "hotspot": hs_24_3,
        })

        # Step 4: Expand / Scroll to Variance Rows showing Anti-Bypass Controls (Bank Only + System Only Expired LG)
        var_row = page.locator("div:has-text('FNCB-PG-2026-9941'), tr:has-text('FNCB-PG-2026-9941'), div:has-text('Bank Only')").last
        if var_row.count():
            var_row.scroll_into_view_if_needed()
            time.sleep(0.5)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_4", "step_04_anti_bypass_resolution.png")
        page.screenshot(path=shot)
        hs_24_4 = measure_box_pct(
            page,
            page.locator("button:has-text('Dispute'), button:has-text('Accept & Adjust')").first,
            pad=8,
            fallback={"xPct": 22.0, "yPct": 48.0, "wPct": 72.0, "hPct": 28.0},
        )
        steps_2_4.append({
            "step": 4,
            "key": "anti_bypass_resolution",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/reconciliation#variance-governance",
            "image": "/demo/shots/issuance_2_4/step_04_anti_bypass_resolution.png",
            "hotspot": hs_24_4,
        })

        # Resolve all session results in DB so session is marked COMPLETED for Step 5!
        db_rc = SessionLocal()
        try:
            from app.models.models_issuance import ReconciliationSession, ReconciliationResult as ReconResModel
            sess = (
                db_rc.query(ReconciliationSession)
                .filter(ReconciliationSession.customer_id == ctx_data["cust_id"])
                .order_by(ReconciliationSession.id.desc())
                .first()
            )
            if sess:
                for rr in db_rc.query(ReconResModel).filter(ReconResModel.session_id == sess.id).all():
                    if not rr.user_resolution:
                        rr.user_resolution = "ADJUSTED" if rr.mismatch_type == "SYSTEM_ONLY" else "DISPUTED"
                sess.status = "COMPLETED"
                db_rc.commit()
        except Exception as rc_err:
            print(f"  [db recon note] {rc_err}")
        finally:
            db_rc.close()
        page.goto("http://localhost:3000/end-user/issuance/reconciliation", wait_until="domcontentloaded")
        time.sleep(1.5)
        hide_toasts(page)
        shot = os.path.join(STAGE_SHOTS, "issuance_2_4", "step_05_completed_recon_sessions.png")
        page.screenshot(path=shot)
        hs_24_5 = measure_box_pct(
            page,
            page.locator("div:has-text('COMPLETED'), tr:has-text('COMPLETED'), div:has-text('First National Corporate Bank')").first,
            pad=8,
            fallback={"xPct": 19.0, "yPct": 28.0, "wPct": 76.0, "hPct": 20.0},
        )
        steps_2_4.append({
            "step": 5,
            "key": "completed_recon_sessions",
            "url": "app.growbusinessdevelopment.com/end-user/issuance/reconciliation",
            "image": "/demo/shots/issuance_2_4/step_05_completed_recon_sessions.png",
            "hotspot": hs_24_5,
        })

        tours_manifest["issuance-position-recon"] = {
            "branchId": "issuance-position-recon",
            "moduleId": "issuance",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "FNCB LG Position Statement",
                "titleAr": "كشف مراكز خطابات الضمان ببنك FNCB",
                "pdfUrl": "/demo/docs/FNCB_LG_Position_Statement_Nov2026.pdf",
                "previewPngUrl": "/demo/docs/FNCB_LG_Position_Statement_Nov2026_preview.png",
            },
            "steps": steps_2_4,
        }

        browser.close()
    return tours_manifest


def publish_assets(tours_manifest: dict) -> None:
    os.makedirs(PUBLIC_DOCS_DIR, exist_ok=True)
    os.makedirs(PUBLIC_SHOTS_DIR, exist_ok=True)

    for fname in os.listdir(STAGE_DOCS):
        shutil.copy2(os.path.join(STAGE_DOCS, fname), os.path.join(PUBLIC_DOCS_DIR, fname))

    for dname in os.listdir(STAGE_SHOTS):
        src_d = os.path.join(STAGE_SHOTS, dname)
        dst_d = os.path.join(PUBLIC_SHOTS_DIR, dname)
        if os.path.isdir(src_d):
            os.makedirs(dst_d, exist_ok=True)
            for fname in os.listdir(src_d):
                shutil.copy2(os.path.join(src_d, fname), os.path.join(dst_d, fname))

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    manifest.setdefault("branches", {})
    for bid, bdata in tours_manifest.items():
        manifest["branches"][bid] = bdata

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"[DONE] Published {len(tours_manifest)} Issuance tours into {MANIFEST_PATH}")


if __name__ == "__main__":
    os.makedirs(STAGE_DIR, exist_ok=True)
    ctx = seed_issuance_demo_data()
    tours = capture_all_issuance_tours(ctx)
    publish_assets(tours)
