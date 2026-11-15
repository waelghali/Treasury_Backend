# c:\Grow\scripts\capture_custody_tours_1_2_to_1_4.py
"""
End-to-end live capture script for Grow LG Custody Interactive Tours:
  - 1.2: custody-action-center   ("Proactive Action Center & Expiry Radar" - 5 steps)
  - 1.3: custody-lifecycle-letter ("Lifecycle Actions, Approval & QR Bank Letter" - 6 steps)
  - 1.4: custody-delivery-reply   ("Delivery Proof & AI Bank Reply Verification" - 6 steps)

100% factual, driven against the live local backend (http://localhost:8000) and
frontend (http://localhost:3000) at 1440x900 @ 2x retina, using strictly fictional
corporate entities (Apex Global Engineering SAE & First National Corporate Bank - FNCB).
"""
import base64
import json
import os
import shutil
import sys
import time
import urllib.request
from datetime import date, datetime, timedelta, timezone

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT_DIR)

import fitz  # PyMuPDF
from playwright.sync_api import sync_playwright
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from app.auth_v2.services import AuthService
from app.core.instruction_verification import generate_verification_token_and_code, _to_epoch_seconds
from app.core.security import create_access_token
from app.database import SessionLocal
from app.models import models
from app.models.models_issuance import IssuedLGRecord
import asyncio

VIEWPORT_W = 1440
VIEWPORT_H = 900

STAGE_DIR = os.path.join(ROOT_DIR, "scratch", "custody_tours_stage")
STAGE_DOCS = os.path.join(STAGE_DIR, "docs")
STAGE_SHOTS = os.path.join(STAGE_DIR, "shots")

PUBLIC_DEMO_DIR = os.path.join(ROOT_DIR, "frontend", "public", "demo")
PUBLIC_DOCS_DIR = os.path.join(PUBLIC_DEMO_DIR, "docs")
PUBLIC_SHOTS_DIR = os.path.join(PUBLIC_DEMO_DIR, "shots")
MANIFEST_PATH = os.path.join(
    ROOT_DIR, "frontend", "src", "components", "Showcase", "pilotManifest.json"
)

DEMO_BANK_NAME = "First National Corporate Bank"
DEMO_BANK_SHORT = "FNCB"


def render_pdf_first_page_png(pdf_path: str, png_path: str, scale: float = 2.2) -> None:
    doc = fitz.open(pdf_path)
    try:
        page = doc[0]
        pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        pix.save(png_path)
    finally:
        doc.close()


def pdf_bytes_to_data_uri(pdf_bytes: bytes, scale: float = 2.4) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        pix = doc[0].get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        png_bytes = pix.tobytes("png")
        b64 = base64.b64encode(png_bytes).decode("ascii")
        return f"data:image/png;base64,{b64}"
    finally:
        doc.close()


def generate_milestone_certificate_pdf(pdf_path: str) -> None:
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=18 * mm,
        bottomMargin=18 * mm,
        title="Phase-1 Milestone Completion Certificate",
    )
    styles = getSampleStyleSheet()
    h_style = ParagraphStyle(
        "CertTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=15,
        leading=20,
        textColor=colors.HexColor("#0f172a"),
    )
    sub_style = ParagraphStyle(
        "CertSub",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=13,
        textColor=colors.HexColor("#475569"),
    )
    body_style = ParagraphStyle(
        "CertBody",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10.5,
        leading=15,
        textColor=colors.HexColor("#1e293b"),
    )

    story = [
        Paragraph("APEX GLOBAL ENGINEERING SAE — PROJECT MANAGEMENT OFFICE", h_style),
        Paragraph(
            "Interim Partial Take-Over & Milestone Completion Certificate (Ref: PMO-CERT-2026-114)",
            sub_style,
        ),
        Spacer(1, 4 * mm),
        HRFlowable(width="100%", thickness=1.5, color=colors.HexColor("#1e3a8a")),
        Spacer(1, 5 * mm),
        Paragraph(
            "<b>Contract Reference:</b> CNT-2026-NS-09 (220kV East Grid Substation — Electromechanical Package)<br/>"
            "<b>Contractor / Applicant:</b> NorthStar Electromechanical Co. SAE<br/>"
            "<b>Associated Performance LG:</b> LG-FNCB-2026-8840 (First National Corporate Bank)<br/>"
            "<b>Original Guarantee Value:</b> EGP 4,500,000.00",
            body_style,
        ),
        Spacer(1, 5 * mm),
        Paragraph(
            "This is to certify that <b>NorthStar Electromechanical Co. SAE</b> has satisfactorily completed, "
            "tested, and handed over <b>Phase-1 Primary Switchgear & Transformer Bay Works</b> in accordance "
            "with Contract CNT-2026-NS-09.",
            body_style,
        ),
        Spacer(1, 3 * mm),
        Paragraph(
            "Pursuant to Sub-Clause 14.9 of the Contract Conditions, Treasury is hereby authorized to instruct "
            "First National Corporate Bank (FNCB) to <b>reduce Performance Guarantee No. LG-FNCB-2026-8840 by "
            "EGP 1,500,000.00</b>, leaving an active remaining guarantee balance of <b>EGP 3,000,000.00</b> "
            "through final provisional acceptance.",
            body_style,
        ),
        Spacer(1, 10 * mm),
        Paragraph(
            "<b>Approved By:</b> Technical Project Director & Contracts Lead — Apex Global Engineering SAE",
            sub_style,
        ),
    ]
    doc.build(story)


def generate_stamped_delivery_copy_pdf(
    pdf_path: str,
    lg_number: str,
    instruction_serial: str,
    issue_date_iso: str,
    stamp_date_iso: str,
    requested_expiry_iso: str,
) -> None:
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title=f"Stamped Delivery Copy - {instruction_serial}",
    )
    styles = getSampleStyleSheet()
    title_s = ParagraphStyle(
        "DelTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=13.5,
        leading=18,
        textColor=colors.HexColor("#0f172a"),
    )
    body_s = ParagraphStyle(
        "DelBody",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        leading=14.5,
        textColor=colors.HexColor("#1e293b"),
    )
    stamp_s = ParagraphStyle(
        "StampBox",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=10.5,
        leading=15,
        textColor=colors.HexColor("#0f5132"),
    )

    stamp_table = Table(
        [
            [
                Paragraph(
                    f"OFFICIAL BANK INCOMING RECEIVING STAMP (خاتم وارد الفرع)<br/>"
                    f"RECEIVED — FIRST NATIONAL CORPORATE BANK (FNCB)<br/>"
                    f"Branch: Corporate Trade Finance Center — New Cairo Branch<br/>"
                    f"Stamp Date: {stamp_date_iso} &nbsp;&nbsp;|&nbsp;&nbsp; Time: 11:24 AM &nbsp;&nbsp;|&nbsp;&nbsp; Teller ID: TF-09",
                    stamp_s,
                )
            ]
        ],
        colWidths=[170 * mm],
    )
    stamp_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#ecfdf5")),
                ("BOX", (0, 0), (-1, -1), 1.8, colors.HexColor("#059669")),
                ("TOPPADDING", (0, 0), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
            ]
        )
    )

    story = [
        Paragraph("APEX GLOBAL ENGINEERING SAE (HQ) — CORPORATE TREASURY", title_s),
        Spacer(1, 2 * mm),
        HRFlowable(width="100%", thickness=1.2, color=colors.HexColor("#334155")),
        Spacer(1, 4 * mm),
        Paragraph(
            f"<b>Date:</b> {issue_date_iso}<br/>"
            f"<b>Instruction Ref:</b> {instruction_serial}<br/>"
            f"<b>To:</b> First National Corporate Bank (FNCB) — Trade Finance & Guarantees Dept.<br/>"
            f"<b>Subject:</b> Extension Instruction for Letter of Guarantee No. <b>{lg_number}</b>",
            body_s,
        ),
        Spacer(1, 4 * mm),
        Paragraph(
            f"Dear Sirs,<br/><br/>"
            f"With reference to Advance Payment Letter of Guarantee No. <b>{lg_number}</b> issued by "
            f"<b>First National Corporate Bank (FNCB)</b> in favor of <b>Apex Global Engineering SAE (HQ)</b> "
            f"for the amount of <b>EGP 2,850,000.00</b> on behalf of <b>Delta Metro Civil Works SAE</b>, "
            f"kindly extend the validity of the above-referenced guarantee up to <b>{requested_expiry_iso}</b> "
            f"(or pay the full guarantee value upon first demand).",
            body_s,
        ),
        Spacer(1, 8 * mm),
        stamp_table,
    ]
    doc.build(story)


def generate_bank_extension_advice_pdf(
    pdf_path: str,
    lg_number: str,
    instruction_serial: str,
    letter_date_iso: str,
    prior_expiry_iso: str,
    confirmed_expiry_iso: str,
) -> None:
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title=f"FNCB Extension Advice - {lg_number}",
    )
    styles = getSampleStyleSheet()
    bank_h = ParagraphStyle(
        "BankH",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=15,
        leading=19,
        textColor=colors.HexColor("#1e3a8a"),
    )
    bank_sub = ParagraphStyle(
        "BankSub",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=12.5,
        textColor=colors.HexColor("#475569"),
    )
    adv_title = ParagraphStyle(
        "AdvTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=16,
        alignment=1,
        textColor=colors.HexColor("#0f172a"),
    )
    body_s = ParagraphStyle(
        "AdvBody",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        leading=14.5,
        textColor=colors.HexColor("#1e293b"),
    )
    seal_s = ParagraphStyle(
        "SealS",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=9.5,
        leading=13.5,
        textColor=colors.HexColor("#1e40af"),
    )

    summary_tbl = Table(
        [
            ["Guarantee Number:", lg_number, "Advice Reference:", "FNCB/TF/EXT/2026/9914"],
            ["Your Instruction Ref:", instruction_serial, "Advice Issue Date:", letter_date_iso],
            ["Guarantee Amount:", "EGP 2,850,000.00 (Unchanged)", "Previous Expiry:", prior_expiry_iso],
            ["Amended New Expiry Date:", confirmed_expiry_iso, "Status:", "Valid & Operative"],
        ],
        colWidths=[42 * mm, 48 * mm, 36 * mm, 44 * mm],
    )
    summary_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
                ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#cbd5e1")),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
                ("FONTNAME", (1, 3), (1, 3), "Helvetica-Bold"),
                ("TEXTCOLOR", (1, 3), (1, 3), colors.HexColor("#1d4ed8")),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )

    seal_tbl = Table(
        [
            [
                Paragraph(
                    "OFFICIAL BANK BRANCH SEAL — FIRST NATIONAL CORPORATE BANK (FNCB)<br/>"
                    "Central Trade Finance & Letters of Guarantee Operations — Authorized Signatures (#A-441 / #B-109)<br/>"
                    f"Issued & Sealed on: {letter_date_iso} | SWIFT: FNCBEGCXXXX",
                    seal_s,
                )
            ]
        ],
        colWidths=[170 * mm],
    )
    seal_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eff6ff")),
                ("BOX", (0, 0), (-1, -1), 1.5, colors.HexColor("#2563eb")),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )

    story = [
        Paragraph("FIRST NATIONAL CORPORATE BANK (FNCB)", bank_h),
        Paragraph(
            "Trade Finance & Letters of Guarantee Center • Financial District, New Cairo, Egypt • SWIFT: FNCBEGCXXXX",
            bank_sub,
        ),
        Spacer(1, 3 * mm),
        HRFlowable(width="100%", thickness=1.6, color=colors.HexColor("#1e3a8a")),
        Spacer(1, 4 * mm),
        Paragraph("OFFICIAL AMENDMENT / EXTENSION ADVICE — LETTER OF GUARANTEE", adv_title),
        Spacer(1, 4 * mm),
        Paragraph(
            f"<b>To Beneficiary:</b> Apex Global Engineering SAE (HQ)<br/>"
            f"<b>Applicant:</b> Delta Metro Civil Works SAE<br/>"
            f"<b>Date:</b> {letter_date_iso}",
            body_s,
        ),
        Spacer(1, 4 * mm),
        summary_tbl,
        Spacer(1, 5 * mm),
        Paragraph(
            f"We hereby confirm that, pursuant to your corporate extension instruction Ref. <b>{instruction_serial}</b>, "
            f"the validity of our Advance Payment Guarantee No. <b>{lg_number}</b> for <b>EGP 2,850,000.00</b> has been "
            f"officially extended up to and including <b>{confirmed_expiry_iso}</b> (adjusted to next banking business day "
            f"per standard calendar convention). All other terms and conditions of our original Letter of Guarantee remain "
            f"unchanged and in full force and effect.",
            body_s,
        ),
        Spacer(1, 8 * mm),
        seal_tbl,
    ]
    doc.build(story)


def seed_custody_demo_environment():
    """
    Prepares clean, deterministic fictional records for Customer 23 (Apex Global Engineering)
    and First National Corporate Bank (FNCB) for Tours 1.2, 1.3, and 1.4.
    """
    db = SessionLocal()
    try:
        customer = db.query(models.Customer).filter(models.Customer.id == 23).first()
        if not customer:
            raise RuntimeError("Demo customer id=23 (Apex Global Engineering) not found. Run capture_demo_pilot.py first.")

        entity = (
            db.query(models.CustomerEntity)
            .filter(models.CustomerEntity.customer_id == 23, models.CustomerEntity.is_deleted == False)
            .first()
        )
        maker = (
            db.query(models.User)
            .filter(models.User.customer_id == 23, models.User.email == "treasury@apexglobal.com")
            .first()
        )
        checker = (
            db.query(models.User)
            .filter(models.User.customer_id == 23, models.User.email == "admin@apexglobal.com")
            .first()
        )
        if not maker or not checker:
            raise RuntimeError("Demo maker/checker users not found for customer 23.")

        bank = db.query(models.Bank).filter(models.Bank.name == DEMO_BANK_NAME).first()
        if not bank:
            raise RuntimeError(f"Demo bank '{DEMO_BANK_NAME}' not found.")

        owner = (
            db.query(models.InternalOwnerContact)
            .filter(
                models.InternalOwnerContact.customer_id == 23,
                models.InternalOwnerContact.email == "project.owner@apexglobal.com",
                models.InternalOwnerContact.is_deleted == False,
            )
            .first()
        )
        cat = (
            db.query(models.LGCategory)
            .filter(models.LGCategory.customer_id == 23, models.LGCategory.is_deleted == False)
            .first()
        )
        egp = db.query(models.Currency).filter(models.Currency.iso_code == "EGP").first()
        perf_type = db.query(models.LgType).filter(models.LgType.name.ilike("%Performance%")).first()
        apg_type = db.query(models.LgType).filter(models.LgType.name.ilike("%Advance%")).first() or perf_type
        ret_type = db.query(models.LgType).filter(models.LgType.name.ilike("%Retention%")).first() or perf_type
        rule_urdg = db.query(models.Rule).filter(models.Rule.name.ilike("%URDG%")).first() or db.query(models.Rule).first()
        method_deliv = db.query(models.IssuingMethod).first()

        # Find an instruction template for seeded instructions
        ext_template = (
            db.query(models.Template)
            .filter(models.Template.action_type.ilike("%EXTEND%"), models.Template.is_deleted == False)
            .first()
            or db.query(models.Template).filter(models.Template.is_deleted == False).first()
        )

        # Sanitize any legacy IssuedLGRecord references for customer 23 so zero real bank acronyms appear
        for iss_rec in db.query(IssuedLGRecord).filter(IssuedLGRecord.customer_id == 23).all():
            if iss_rec.lg_ref_number and "ENBD" in iss_rec.lg_ref_number:
                iss_rec.lg_ref_number = iss_rec.lg_ref_number.replace("ENBD", "FNCB")
            if iss_rec.bank_lg_number and "ENBD" in iss_rec.bank_lg_number:
                iss_rec.bank_lg_number = iss_rec.bank_lg_number.replace("ENBD", "FNCB")
            if iss_rec.beneficiary_name and "ENBD" in iss_rec.beneficiary_name:
                iss_rec.beneficiary_name = iss_rec.beneficiary_name.replace("ENBD", "FNCB")
        db.flush()

        # Clean up any prior runs of 8841, 8842, 8843, 8844 AND reset open instructions/approvals on 8840
        target_nums = [
            "LG-FNCB-2026-8841",
            "LG-FNCB-2026-8842",
            "LG-FNCB-2026-8843",
            "LG-FNCB-2026-8844",
        ]
        old_lgs = (
            db.query(models.LGRecord)
            .filter(models.LGRecord.customer_id == 23, models.LGRecord.lg_number.in_(target_nums))
            .all()
        )
        for r in old_lgs:
            db.query(models.ApprovalRequest).filter(models.ApprovalRequest.entity_id == r.id).update({"related_instruction_id": None}, synchronize_session=False)
            db.query(models.LGInstruction).filter(models.LGInstruction.lg_record_id == r.id).update({"approval_request_id": None}, synchronize_session=False)
            db.query(models.AuditLog).filter(models.AuditLog.lg_record_id == r.id).delete(synchronize_session=False)
            db.query(models.LGDocument).filter(models.LGDocument.lg_record_id == r.id).delete(synchronize_session=False)
            db.query(models.LGInstruction).filter(models.LGInstruction.lg_record_id == r.id).delete(synchronize_session=False)
            db.query(models.ApprovalRequest).filter(models.ApprovalRequest.entity_id == r.id).delete(synchronize_session=False)
            db.delete(r)
        db.flush()

        # Reset LG-FNCB-2026-8840 so Tour 1.3 always starts cleanly at EGP 4,500,000.00 Valid
        lg_8840 = (
            db.query(models.LGRecord)
            .filter(models.LGRecord.customer_id == 23, models.LGRecord.lg_number == "LG-FNCB-2026-8840")
            .first()
        )
        if not lg_8840:
            lg_8840 = models.LGRecord(
                customer_id=23,
                beneficiary_corporate_id=entity.id,
                issuer_name="NorthStar Electromechanical Co. SAE",
                lg_number="LG-FNCB-2026-8840",
                lg_amount=4500000.00,
                lg_currency_id=egp.id,
                lg_payable_currency_id=egp.id,
                issuance_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
                expiry_date=datetime(2027, 11, 1, tzinfo=timezone.utc),
                auto_renewal=True,
                lg_type_id=perf_type.id,
                lg_operational_status_id=1,
                payment_conditions="Unconditional payment upon first written demand without proof of default.",
                description_purpose="Performance Guarantee — 220kV Substation Electromechanical Package (CNT-2026-NS-09)",
                issuing_bank_id=bank.id,
                issuing_bank_address="Corporate Trade Finance Center, New Cairo",
                issuing_bank_phone="+20 2 2480 9000",
                issuing_method_id=method_deliv.id,
                applicable_rule_id=rule_urdg.id,
                internal_owner_contact_id=owner.id,
                lg_category_id=cat.id,
                lg_status_id=1,
                lg_period_months=12,
                mandatory_claim_statement=(
                    "Any demand hereunder must explicitly state that NorthStar Electromechanical Co. SAE "
                    "has failed to fulfill its contractual obligations under Contract No. CNT-2026-NS-09."
                ),
            )
            db.add(lg_8840)
            db.flush()
        else:
            db.query(models.ApprovalRequest).filter(models.ApprovalRequest.entity_id == lg_8840.id).update({"related_instruction_id": None}, synchronize_session=False)
            db.query(models.LGInstruction).filter(models.LGInstruction.lg_record_id == lg_8840.id).update({"approval_request_id": None}, synchronize_session=False)
            db.query(models.LGDocument).filter(
                models.LGDocument.lg_record_id == lg_8840.id,
                models.LGDocument.document_type != "ORIGINAL_LG_DOCUMENT",
            ).delete(synchronize_session=False)
            db.query(models.LGInstruction).filter(models.LGInstruction.lg_record_id == lg_8840.id).delete(synchronize_session=False)
            db.query(models.ApprovalRequest).filter(models.ApprovalRequest.entity_id == lg_8840.id).delete(synchronize_session=False)
            lg_8840.lg_amount = 4500000.00
            lg_8840.lg_status_id = 1
            lg_8840.payment_conditions = (
                "We irrevocably and unconditionally undertake to pay Apex Global Engineering SAE (HQ) "
                "upon your first written demand up to EGP 4,500,000.00 notwithstanding any objection by the Applicant."
            )
            lg_8840.mandatory_claim_statement = (
                "Any demand hereunder must explicitly state that NorthStar Electromechanical Co. SAE "
                "has failed to fulfill its contractual obligations under Contract No. CNT-2026-NS-09."
            )
            db.add(lg_8840)
            db.flush()

        # Sanitize any legacy non-fictional bank codes in customer 23 IssuedLGs / MaintenanceActions (e.g. ENBD -> FNCB)
        for model_name, attr_name in (("IssuedLG", "lg_ref_number"), ("IssuanceMaintenanceAction", "lg_ref_number")):
            mdl = getattr(models, model_name, None)
            col = getattr(mdl, attr_name, None) if mdl is not None else None
            if mdl is not None and col is not None:
                for row in db.query(mdl).filter(col.ilike("%ENBD%")).all():
                    setattr(row, attr_name, getattr(row, attr_name).replace("ENBD", "FNCB").replace("enbd", "FNCB"))
                    db.add(row)

        # Ensure LG-FNCB-2026-8840 has its ORIGINAL_LG_DOCUMENT row pointing to FNCB_Performance_LG_8840.pdf
        orig_pdf_path = os.path.join(PUBLIC_DOCS_DIR, "FNCB_Performance_LG_8840.pdf")
        existing_orig_doc = (
            db.query(models.LGDocument)
            .filter(
                models.LGDocument.lg_record_id == lg_8840.id,
                models.LGDocument.document_type == "ORIGINAL_LG_DOCUMENT",
                models.LGDocument.is_deleted == False,
            )
            .first()
        )
        if not existing_orig_doc and os.path.exists(orig_pdf_path):
            db.add(
                models.LGDocument(
                    lg_record_id=lg_8840.id,
                    document_type="ORIGINAL_LG_DOCUMENT",
                    file_name="FNCB_Performance_LG_8840.pdf",
                    file_path=orig_pdf_path,
                    mime_type="application/pdf",
                    uploaded_by_user_id=maker.id,
                )
            )
            db.flush()

        now_utc = datetime.now(timezone.utc)
        today_dt = date.today()
        from sqlalchemy import func
        max_seq = (
            db.query(func.max(models.LGRecord.lg_sequence_number))
            .filter(models.LGRecord.beneficiary_corporate_id == entity.id)
            .scalar()
            or 1
        )

        # 1) LG-FNCB-2026-8841: Expiring in 12 days (Urgent / Forced Renewal + used in 1.2 & 1.4!)
        exp_8841 = datetime.combine(today_dt + timedelta(days=12), datetime.min.time(), tzinfo=timezone.utc)
        lg_8841 = models.LGRecord(
            customer_id=23,
            beneficiary_corporate_id=entity.id,
            lg_sequence_number=max_seq + 1,
            issuer_name="Delta Metro Civil Works SAE",
            lg_number="LG-FNCB-2026-8841",
            lg_amount=2850000.00,
            lg_currency_id=egp.id,
            lg_payable_currency_id=egp.id,
            issuance_date=now_utc - timedelta(days=168),
            expiry_date=exp_8841,
            auto_renewal=True,
            lg_type_id=apg_type.id,
            lg_operational_status_id=1,
            description_purpose="Advance Payment Guarantee — West Depot Civil Infrastructure (CNT-2026-DM-14)",
            issuing_bank_id=bank.id,
            issuing_bank_address="Corporate Trade Finance Center, New Cairo",
            issuing_bank_phone="+20 2 2480 9000",
            issuing_method_id=method_deliv.id,
            applicable_rule_id=rule_urdg.id,
            internal_owner_contact_id=owner.id,
            lg_category_id=cat.id,
            lg_status_id=1,
            lg_period_months=6,
        )
        db.add(lg_8841)

        # 2) LG-FNCB-2026-8842: Expiring in 23 days (Proactive Auto-Renewal candidate in 1.2)
        exp_8842 = datetime.combine(today_dt + timedelta(days=23), datetime.min.time(), tzinfo=timezone.utc)
        lg_8842 = models.LGRecord(
            customer_id=23,
            beneficiary_corporate_id=entity.id,
            lg_sequence_number=max_seq + 2,
            issuer_name="MedGrid Power Systems SAE",
            lg_number="LG-FNCB-2026-8842",
            lg_amount=6200000.00,
            lg_currency_id=egp.id,
            lg_payable_currency_id=egp.id,
            issuance_date=now_utc - timedelta(days=340),
            expiry_date=exp_8842,
            auto_renewal=True,
            lg_type_id=perf_type.id,
            lg_operational_status_id=1,
            description_purpose="Performance Guarantee — Coastal HV Transformer Supply (CNT-2026-MG-22)",
            issuing_bank_id=bank.id,
            issuing_bank_address="Corporate Trade Finance Center, New Cairo",
            issuing_bank_phone="+20 2 2480 9000",
            issuing_method_id=method_deliv.id,
            applicable_rule_id=rule_urdg.id,
            internal_owner_contact_id=owner.id,
            lg_category_id=cat.id,
            lg_status_id=1,
            lg_period_months=6,
        )
        db.add(lg_8842)

        # 3) LG-FNCB-2026-8843: Has an Overdue Bank Reply (delivered 14 days ago, awaiting bank reply)
        lg_8843 = models.LGRecord(
            customer_id=23,
            beneficiary_corporate_id=entity.id,
            lg_sequence_number=max_seq + 3,
            issuer_name="Nile Valley Steel Structures SAE",
            lg_number="LG-FNCB-2026-8843",
            lg_amount=1750000.00,
            lg_currency_id=egp.id,
            lg_payable_currency_id=egp.id,
            issuance_date=now_utc - timedelta(days=200),
            expiry_date=now_utc + timedelta(days=165),
            auto_renewal=True,
            lg_type_id=ret_type.id,
            lg_operational_status_id=1,
            description_purpose="Retention Guarantee — Industrial Steel Gantry Package (CNT-2026-NV-05)",
            issuing_bank_id=bank.id,
            issuing_bank_address="Corporate Trade Finance Center, New Cairo",
            issuing_bank_phone="+20 2 2480 9000",
            issuing_method_id=method_deliv.id,
            applicable_rule_id=rule_urdg.id,
            internal_owner_contact_id=owner.id,
            lg_category_id=cat.id,
            lg_status_id=1,
            lg_period_months=6,
        )
        db.add(lg_8843)

        # 4) LG-FNCB-2026-8844: Has 1 Pending Print instruction + 1 Undelivered instruction
        lg_8844 = models.LGRecord(
            customer_id=23,
            beneficiary_corporate_id=entity.id,
            lg_sequence_number=max_seq + 4,
            issuer_name="Cairo Industrial Turbines SAE",
            lg_number="LG-FNCB-2026-8844",
            lg_amount=950000.00,
            lg_currency_id=egp.id,
            lg_payable_currency_id=egp.id,
            issuance_date=now_utc - timedelta(days=120),
            expiry_date=now_utc + timedelta(days=190),
            auto_renewal=False,
            lg_type_id=perf_type.id,
            lg_operational_status_id=1,
            description_purpose="Performance Guarantee — Auxiliary Cooling Units (CNT-2026-CT-19)",
            issuing_bank_id=bank.id,
            issuing_bank_address="Corporate Trade Finance Center, New Cairo",
            issuing_bank_phone="+20 2 2480 9000",
            issuing_method_id=method_deliv.id,
            applicable_rule_id=rule_urdg.id,
            internal_owner_contact_id=owner.id,
            lg_category_id=cat.id,
            lg_status_id=1,
            lg_period_months=6,
        )
        db.add(lg_8844)
        db.flush()

        if ext_template:
            # Overdue Bank Reply instruction on 8843 (delivered 21 days ago -> eligible for Remind & Bulk Reminders!)
            inst_overdue_reply = models.LGInstruction(
                lg_record_id=lg_8843.id,
                instruction_type="LG_EXTENSION",
                serial_number="APX-2026-EXT-8843-01",
                global_seq_per_lg=1,
                type_seq_per_lg=1,
                template_id=ext_template.id,
                status="Instruction Delivered",
                instruction_date=now_utc - timedelta(days=25),
                delivery_date=now_utc - timedelta(days=21),
                bank_reply_date=None,
                is_printed=True,
                sent_to_bank=True,
                maker_user_id=maker.id,
                details={
                    "old_expiry_date": (today_dt + timedelta(days=15)).isoformat(),
                    "new_expiry_date": (today_dt + timedelta(days=165)).isoformat(),
                },
            )
            db.add(inst_overdue_reply)

            # Undelivered instruction on 8843 (issued 6 days ago, printed, awaiting physical bank delivery)
            inst_undelivered = models.LGInstruction(
                lg_record_id=lg_8843.id,
                instruction_type="LG_AMEND",
                serial_number="APX-2026-AMD-8843-02",
                global_seq_per_lg=2,
                type_seq_per_lg=1,
                template_id=ext_template.id,
                status="Instruction Issued",
                instruction_date=now_utc - timedelta(days=6),
                delivery_date=None,
                bank_reply_date=None,
                is_printed=True,
                sent_to_bank=False,
                maker_user_id=maker.id,
                details={"reason": "Update project contract annex reference"},
            )
            db.add(inst_undelivered)

            # Approved Request Pending Print on 8844 (lights up Pending Prints KPI card!)
            inst_pending_print = models.LGInstruction(
                lg_record_id=lg_8844.id,
                instruction_type="LG_DECREASE_AMOUNT",
                serial_number="APX-2026-DEC-8844-01",
                global_seq_per_lg=1,
                type_seq_per_lg=1,
                template_id=ext_template.id,
                status="Instruction Issued",
                instruction_date=now_utc - timedelta(days=1),
                delivery_date=None,
                bank_reply_date=None,
                is_printed=False,
                sent_to_bank=False,
                maker_user_id=maker.id,
                checker_user_id=checker.id,
                details={
                    "original_lg_amount": 1200000.0,
                    "decrease_amount": 250000.0,
                    "new_lg_amount": 950000.0,
                },
            )
            db.add(inst_pending_print)
            db.flush()

            appr_pending_print = models.ApprovalRequest(
                customer_id=23,
                entity_type="LGRecord",
                entity_id=lg_8844.id,
                action_type="LG_DECREASE_AMOUNT",
                status="APPROVED",
                maker_user_id=maker.id,
                checker_user_id=checker.id,
                related_instruction_id=inst_pending_print.id,
                request_details={"decrease_amount": 250000.0, "reason": "Interim delivery milestone"},
                lg_record_snapshot={"lg_number": "LG-FNCB-2026-8844", "lg_amount": 1200000.0},
            )
            db.add(appr_pending_print)

        maker.must_change_password = False
        checker.must_change_password = False
        db.add(maker)
        db.add(checker)
        db.commit()

        auth_svc = AuthService()
        maker_payload = asyncio.run(auth_svc.build_user_token_data(db, maker))
        maker_payload["must_change_password"] = False
        maker_payload["must_accept_policies"] = False
        maker_payload["is_mfa_verified"] = True
        maker_token = create_access_token(maker_payload, expires_delta=timedelta(hours=4))

        checker_payload = asyncio.run(auth_svc.build_user_token_data(db, checker))
        checker_payload["must_change_password"] = False
        checker_payload["must_accept_policies"] = False
        checker_payload["is_mfa_verified"] = True
        checker_token = create_access_token(checker_payload, expires_delta=timedelta(hours=4))

        return {
            "maker_token": maker_token,
            "checker_token": checker_token,
            "lg_8840_id": lg_8840.id,
            "lg_8841_id": lg_8841.id,
            "lg_8842_id": lg_8842.id,
        }
    finally:
        db.close()


def set_browser_auth(page, token: str, role: str) -> None:
    now_ms = int(time.time() * 1000)
    page.evaluate(
        """({token, role, now}) => {
            localStorage.setItem('jwt_token', token);
            localStorage.setItem('user_role', role);
            localStorage.setItem('customer_id', '23');
            localStorage.setItem('must_change_password', 'false');
            localStorage.setItem('last_activity_at', String(now));
            localStorage.setItem('last_token_refresh_at', String(now));
            localStorage.setItem('i18nextLng', 'en');
        }""",
        {"token": token, "role": role, "now": now_ms},
    )


def hide_toasts(page) -> None:
    page.add_style_tag(
        content=".Toastify, .Toastify__toast-container { display: none !important; opacity: 0 !important; }"
    )
    min_btn = page.locator("button[aria-label='Minimize button']")
    if min_btn.count() > 0 and min_btn.first.is_visible():
        try:
            min_btn.first.click(timeout=1000)
        except Exception:
            pass


def measure_box_pct(locator) -> dict:
    box = locator.bounding_box()
    if not box:
        return {"xPct": 50.0, "yPct": 50.0, "wPct": 12.0, "hPct": 5.0}
    return {
        "xPct": round((box["x"] / VIEWPORT_W) * 100, 2),
        "yPct": round((box["y"] / VIEWPORT_H) * 100, 2),
        "wPct": round((box["width"] / VIEWPORT_W) * 100, 2),
        "hPct": round((box["height"] / VIEWPORT_H) * 100, 2),
    }


def fetch_instruction_pdf_bytes(inst_id: int, token: str) -> bytes:
    url = f"http://localhost:8000/api/v1/end-user/lg-records/instructions/{inst_id}/view-letter?raw_pdf=true"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def main():
    if os.path.exists(STAGE_DIR):
        shutil.rmtree(STAGE_DIR)
    os.makedirs(STAGE_DOCS, exist_ok=True)
    for sub in ("custody_action_center", "custody_lifecycle", "custody_delivery_reply"):
        os.makedirs(os.path.join(STAGE_SHOTS, sub), exist_ok=True)

    print("[1/5] Seeding deterministic LG Custody environment...")
    env = seed_custody_demo_environment()
    maker_token = env["maker_token"]
    checker_token = env["checker_token"]
    lg_8840_id = env["lg_8840_id"]
    lg_8841_id = env["lg_8841_id"]

    milestone_pdf = os.path.join(STAGE_DOCS, "Phase1_Milestone_Completion_Cert.pdf")
    generate_milestone_certificate_pdf(milestone_pdf)

    steps_1_2 = []
    steps_1_3 = []
    steps_1_4 = []

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        context = browser.new_context(
            viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
            device_scale_factor=2,
        )
        # Prevent window.open popups from stealing focus during automated clicks
        context.add_init_script("window.open = function() { return null; };")

        page = context.new_page()
        page.on("dialog", lambda d: d.accept())
        page.goto("http://localhost:3000/login", wait_until="domcontentloaded")
        set_browser_auth(page, maker_token, "end_user")

        # ═══════════════════════════════════════════════════════════════════════
        # TOUR 1.2: Proactive Action Center & Expiry Radar (5 Steps)
        # ═══════════════════════════════════════════════════════════════════════
        print("[2/5] Capturing Tour 1.2: Proactive Action Center & Expiry Radar...")
        page.goto("http://localhost:3000/end-user/action-center", wait_until="networkidle")
        hide_toasts(page)
        page.wait_for_selector("text=LG-FNCB-2026-8841", timeout=20000)
        time.sleep(0.8)

        # Step 1.2.1: Action Center Overview + Expiry Radar
        auto_renew_btn = page.locator("button:has-text('Auto Renewal')").first
        auto_renew_btn.wait_for(state="visible", timeout=10000)
        shot_12_1 = os.path.join(STAGE_SHOTS, "custody_action_center", "step1_action_center_radar.png")
        page.screenshot(path=shot_12_1)
        steps_1_2.append(
            {
                "step": 1,
                "url": "app.growbusinessdevelopment.com/end-user/action-center",
                "image": "/demo/shots/custody_action_center/step1_action_center_radar.png",
                "hotspot": measure_box_pct(auto_renew_btn),
            }
        )

        # Step 1.2.2: Bulk Auto-Renewal Engine Modal
        auto_renew_btn.click()
        confirm_bulk_btn = page.locator("button:has-text('Confirm & Extend')").first
        confirm_bulk_btn.wait_for(state="visible", timeout=15000)
        time.sleep(0.6)
        shot_12_2 = os.path.join(STAGE_SHOTS, "custody_action_center", "step2_bulk_auto_renewal.png")
        page.screenshot(path=shot_12_2)
        steps_1_2.append(
            {
                "step": 2,
                "url": "app.growbusinessdevelopment.com/end-user/action-center#auto-renewal",
                "image": "/demo/shots/custody_action_center/step2_bulk_auto_renewal.png",
                "hotspot": measure_box_pct(confirm_bulk_btn),
            }
        )
        # Close Bulk Auto-Renewal modal so we demonstrate direct fast-track single extension on 8841
        page.locator("button:has-text('Close')").last.click()
        time.sleep(0.4)

        # Step 1.2.3: Direct Protective Extension (No Maker-Checker Delay!)
        row_8841 = page.locator("tr", has_text="LG-FNCB-2026-8841").first
        row_8841.locator("button:has-text('Renew')").click()
        extend_submit_btn = page.locator("button[type='submit']:has-text('Extend LG')").first
        extend_submit_btn.wait_for(state="visible", timeout=10000)
        notes_area = page.locator("textarea[name='notes']")
        if notes_area.count() > 0:
            notes_area.fill("Protective 6-month extension pending West Depot civil infrastructure completion.")
        time.sleep(0.4)
        shot_12_3 = os.path.join(STAGE_SHOTS, "custody_action_center", "step3_direct_extension_modal.png")
        page.screenshot(path=shot_12_3)
        steps_1_2.append(
            {
                "step": 3,
                "url": "app.growbusinessdevelopment.com/end-user/action-center#direct-extend",
                "image": "/demo/shots/custody_action_center/step3_direct_extension_modal.png",
                "hotspot": measure_box_pct(extend_submit_btn),
            }
        )
        # Execute real extension & wait for modal to close!
        extend_submit_btn.click()
        try:
            page.wait_for_selector("text=Extend Letter of Guarantee:", state="hidden", timeout=25000)
        except Exception:
            time.sleep(2.5)
        time.sleep(0.8)

        # Fetch newly created extension instruction on 8841 & save real QR Extension Letter PDF
        db = SessionLocal()
        try:
            inst_8841 = (
                db.query(models.LGInstruction)
                .filter(models.LGInstruction.lg_record_id == lg_8841_id, models.LGInstruction.is_deleted == False)
                .order_by(models.LGInstruction.id.desc())
                .first()
            )
            if not inst_8841:
                raise RuntimeError("Expected extension instruction on LG-FNCB-2026-8841 was not created!")
            inst_8841_id = inst_8841.id
            inst_8841_serial = inst_8841.serial_number
            inst_8841_details = dict(inst_8841.details or {})
            # Backdate instruction_date by 2 days so Tour 1.4 can demonstrate yesterday's bank stamp date alignment cleanly!
            two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
            inst_8841.instruction_date = two_days_ago
            inst_8841.is_printed = True
            db.add(inst_8841)
            db.commit()
        finally:
            db.close()

        ext_pdf_bytes = fetch_instruction_pdf_bytes(inst_8841_id, maker_token)
        ext_pdf_stage = os.path.join(STAGE_DOCS, "FNCB_Extension_Instruction_8841.pdf")
        ext_png_stage = os.path.join(STAGE_DOCS, "FNCB_Extension_Instruction_8841_preview.png")
        with open(ext_pdf_stage, "wb") as f:
            f.write(ext_pdf_bytes)
        render_pdf_first_page_png(ext_pdf_stage, ext_png_stage)

        # Step 1.2.4: Reload Action Center & scroll to Undelivered + Awaiting Bank Reply Radar
        page.goto("http://localhost:3000/end-user/action-center", wait_until="networkidle")
        hide_toasts(page)
        page.wait_for_selector("text=Awaiting Bank Reply", timeout=15000)
        bulk_rem_btn = page.locator("button:has-text('Bulk Reminders')").first
        bulk_rem_btn.scroll_into_view_if_needed()
        page.evaluate("window.scrollBy(0, -110)")
        time.sleep(0.5)
        shot_12_4 = os.path.join(STAGE_SHOTS, "custody_action_center", "step4_delivery_and_reply_radar.png")
        page.screenshot(path=shot_12_4)
        steps_1_2.append(
            {
                "step": 4,
                "url": "app.growbusinessdevelopment.com/end-user/action-center#bank-replies",
                "image": "/demo/shots/custody_action_center/step4_delivery_and_reply_radar.png",
                "hotspot": measure_box_pct(bulk_rem_btn),
            }
        )

        # Step 1.2.5: 1-Click Bulk Bank Chaser / Reminders Modal
        bulk_rem_btn.click()
        gen_rem_btn = page.locator("button:has-text('Generate & Print Reminders')").first
        gen_rem_btn.wait_for(state="visible", timeout=10000)
        time.sleep(0.4)
        shot_12_5 = os.path.join(STAGE_SHOTS, "custody_action_center", "step5_bulk_bank_reminders.png")
        page.screenshot(path=shot_12_5)
        steps_1_2.append(
            {
                "step": 5,
                "url": "app.growbusinessdevelopment.com/end-user/action-center#bulk-reminders",
                "image": "/demo/shots/custody_action_center/step5_bulk_bank_reminders.png",
                "hotspot": measure_box_pct(gen_rem_btn),
            }
        )
        page.locator("button:has-text('Cancel')").last.click()
        time.sleep(0.3)

        # ═══════════════════════════════════════════════════════════════════════
        # TOUR 1.3: Lifecycle Actions, Approval & QR Bank Letter (6 Steps)
        # ═══════════════════════════════════════════════════════════════════════
        print("[3/5] Capturing Tour 1.3: Lifecycle Actions, Approval & QR Bank Letter...")
        page.goto(f"http://localhost:3000/end-user/lg-records/{lg_8840_id}", wait_until="networkidle")
        hide_toasts(page)
        page.wait_for_selector("text=LG-FNCB-2026-8840", timeout=15000)

        # Open Documents tab & run live AI Beneficiary Protection Check!
        page.locator("nav button:has-text('Documents')").click()
        time.sleep(0.5)
        doc_protect_btn = page.locator("button:has-text('Beneficiary Protection Check')").first
        if doc_protect_btn.count() > 0 and doc_protect_btn.is_visible():
            doc_protect_btn.click()
            try:
                page.wait_for_selector("text=Beneficiary Protection Check (Incoming LG)", timeout=110000)
            except Exception as e:
                print(f"  [Info] Doc protection check slow, reloading for fast text check: {e}")
                page.reload(wait_until="networkidle")
                hide_toasts(page)
                page.locator("nav button:has-text('Documents')").click()
                time.sleep(0.4)
        paste_wording_btn = page.locator("button:has-text('Paste Vendor LG Text')").first
        if page.locator("text=Beneficiary Protection Check (Incoming LG)").count() == 0 and paste_wording_btn.count() > 0:
            paste_wording_btn.click()
            time.sleep(0.3)
            page.locator("textarea[placeholder*='Paste draft or issued vendor LG wording']").fill(
                "We irrevocably and unconditionally undertake to pay Apex Global Engineering SAE (HQ) "
                "upon your first written demand up to EGP 4,500,000.00 notwithstanding any objection "
                "by NorthStar Electromechanical Co. SAE, without requiring proof of default or court order. "
                "Any demand must state that the Applicant failed to fulfill its obligations under Contract CNT-2026-NS-09."
            )
            run_paste_btn = page.locator("button:has-text('Run Beneficiary Protection Check')").first
            if run_paste_btn.is_enabled():
                run_paste_btn.click()
                try:
                    page.wait_for_selector("text=Beneficiary Protection Check (Incoming LG)", timeout=90000)
                except Exception as e:
                    print(f"  [Info] AI wording check note: {e}")
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.5)

        # Step 1.3.1: LG Cockpit Lifecycle Bar + AI Beneficiary Protection Check
        dec_btn = page.locator("button[title='Decrease LG Amount']").first
        dec_btn.wait_for(state="visible", timeout=10000)
        shot_13_1 = os.path.join(STAGE_SHOTS, "custody_lifecycle", "step1_cockpit_and_protection_check.png")
        page.screenshot(path=shot_13_1)
        steps_1_3.append(
            {
                "step": 1,
                "url": "app.growbusinessdevelopment.com/end-user/lg-records/details#protection-check",
                "image": "/demo/shots/custody_lifecycle/step1_cockpit_and_protection_check.png",
                "hotspot": measure_box_pct(dec_btn),
            }
        )

        # Step 1.3.2: Decrease Amount Modal (Risk-Bearing Governance + Mandatory Supporting Evidence)
        dec_btn.click()
        page.wait_for_selector("input[name='decreaseAmount'], input#decreaseAmount", timeout=10000)
        amt_inp = page.locator("input[name='decreaseAmount'], input#decreaseAmount").first
        amt_inp.fill("1500000")
        reason_inp = page.locator("textarea[name='reason'], textarea#reason").first
        reason_inp.fill(
            "Phase-1 Switchgear & Transformer Bay Take-Over Certificate (Ref: PMO-CERT-2026-114) signed by Project Consultant."
        )
        notes_inp = page.locator("textarea[name='notes'], textarea#notes")
        if notes_inp.count() > 0:
            notes_inp.first.fill("Reduce guarantee balance from EGP 4,500,000.00 to EGP 3,000,000.00 per Clause 14.9.")
        file_inp = page.locator("input[type='file']").last
        file_inp.set_input_files(milestone_pdf)
        time.sleep(0.5)

        dec_submit_btn = page.locator("form button[type='submit']").last
        shot_13_2 = os.path.join(STAGE_SHOTS, "custody_lifecycle", "step2_decrease_amount_modal.png")
        page.screenshot(path=shot_13_2)
        steps_1_3.append(
            {
                "step": 2,
                "url": "app.growbusinessdevelopment.com/end-user/lg-records/details#decrease-amount",
                "image": "/demo/shots/custody_lifecycle/step2_decrease_amount_modal.png",
                "hotspot": measure_box_pct(dec_submit_btn),
            }
        )
        # Execute real Maker-Checker submission & wait for DB ApprovalRequest!
        dec_submit_btn.click()
        for _ in range(40):
            db = SessionLocal()
            try:
                appr_8840 = (
                    db.query(models.ApprovalRequest)
                    .filter(models.ApprovalRequest.entity_id == lg_8840_id, models.ApprovalRequest.status == "PENDING")
                    .first()
                )
                if appr_8840:
                    break
            finally:
                db.close()
            time.sleep(0.5)
        time.sleep(0.5)

        # Step 1.3.3: Switch to Corporate Admin (`admin@apexglobal.com`) in an isolated browser context -> Pending Approvals Queue
        checker_context = browser.new_context(
            viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
            device_scale_factor=2,
        )
        now_ms_checker = int(time.time() * 1000)
        checker_context.add_init_script(
            f"""
            window.open = function() {{ return null; }};
            localStorage.setItem('jwt_token', {json.dumps(checker_token)});
            localStorage.setItem('user_role', 'corporate_admin');
            localStorage.setItem('customer_id', '23');
            localStorage.setItem('must_change_password', 'false');
            localStorage.setItem('last_activity_at', '{now_ms_checker}');
            localStorage.setItem('last_token_refresh_at', '{now_ms_checker}');
            localStorage.setItem('i18nextLng', 'en');
            """
        )
        checker_page = checker_context.new_page()
        checker_page.on("dialog", lambda d: d.accept())
        checker_page.goto("http://localhost:3000/corporate-admin/approval-requests", wait_until="networkidle")
        hide_toasts(checker_page)
        checker_page.wait_for_selector("text=LG-FNCB-2026-8840", timeout=20000)
        time.sleep(0.6)

        pending_row = checker_page.locator("tr", has_text="LG-FNCB-2026-8840").first
        inspect_eye_btn = pending_row.locator("button").first
        shot_13_3 = os.path.join(STAGE_SHOTS, "custody_lifecycle", "step3_admin_pending_approvals.png")
        checker_page.screenshot(path=shot_13_3)
        steps_1_3.append(
            {
                "step": 3,
                "url": "app.growbusinessdevelopment.com/corporate-admin/approval-requests",
                "image": "/demo/shots/custody_lifecycle/step3_admin_pending_approvals.png",
                "hotspot": measure_box_pct(inspect_eye_btn),
            }
        )

        # Step 1.3.4: Side-by-Side Snapshot Diff & Corporate Admin Approval
        inspect_eye_btn.click()
        approve_btn = checker_page.locator("button:has-text('Approve')").last
        approve_btn.wait_for(state="visible", timeout=10000)
        # Wait for snapshot comparison grid to finish loading (`(Changed)` or `LG Number:`)
        try:
            checker_page.wait_for_selector("text=LG Number:", timeout=8000)
        except Exception:
            pass
        time.sleep(0.5)
        shot_13_4 = os.path.join(STAGE_SHOTS, "custody_lifecycle", "step4_approval_snapshot_diff.png")
        checker_page.screenshot(path=shot_13_4)
        steps_1_3.append(
            {
                "step": 4,
                "url": "app.growbusinessdevelopment.com/corporate-admin/approval-requests#review",
                "image": "/demo/shots/custody_lifecycle/step4_approval_snapshot_diff.png",
                "hotspot": measure_box_pct(approve_btn),
            }
        )
        # Click Approve to execute reduction & generate QR instruction! (window.confirm auto-accepted)
        approve_btn.click()
        try:
            checker_page.wait_for_selector("text=Review Approval Request", state="hidden", timeout=25000)
        except Exception:
            time.sleep(3.0)
        time.sleep(0.8)
        checker_page.close()
        checker_context.close()

        # Poll DB up to 15s for the newly generated Decrease Amount instruction on 8840 & compute its exact QR token + 4-digit PIN
        inst_8840_id = None
        inst_8840_serial = None
        qr_token = None
        footer_pin = None
        for _ in range(30):
            db = SessionLocal()
            try:
                inst_8840 = (
                    db.query(models.LGInstruction)
                    .filter(models.LGInstruction.lg_record_id == lg_8840_id, models.LGInstruction.is_deleted == False)
                    .order_by(models.LGInstruction.id.desc())
                    .first()
                )
                if inst_8840:
                    inst_8840_id = inst_8840.id
                    inst_8840_serial = inst_8840.serial_number
                    secret_ts = _to_epoch_seconds(
                        getattr(inst_8840, "created_at", None) or getattr(inst_8840, "instruction_date", None)
                    )
                    qr_token, footer_pin = generate_verification_token_and_code(
                        record_kind="I",
                        record_id=inst_8840_id,
                        serial_number=inst_8840_serial,
                        lg_number="LG-FNCB-2026-8840",
                        secret_ts=secret_ts,
                    )
                    break
            finally:
                db.close()
            time.sleep(0.5)

        if not inst_8840_id:
            raise RuntimeError("Decrease instruction on LG-FNCB-2026-8840 was not generated after approval!")

        # Fetch raw PDF of the QR-sealed Decrease Instruction Letter
        dec_pdf_bytes = fetch_instruction_pdf_bytes(inst_8840_id, maker_token)
        dec_pdf_stage = os.path.join(STAGE_DOCS, "FNCB_Decrease_Instruction_8840.pdf")
        dec_png_stage = os.path.join(STAGE_DOCS, "FNCB_Decrease_Instruction_8840_preview.png")
        with open(dec_pdf_stage, "wb") as f:
            f.write(dec_pdf_bytes)
        render_pdf_first_page_png(dec_pdf_stage, dec_png_stage)
        dec_letter_data_uri = pdf_bytes_to_data_uri(dec_pdf_bytes, scale=2.5)

        # Step 1.3.5: Official QR + Split Security Code Bank Instruction Letter Viewer
        letter_view_url = f"http://localhost:8000/api/v1/end-user/lg-records/instructions/{inst_8840_id}/view-letter?token={maker_token}"
        page.goto(letter_view_url, wait_until="domcontentloaded")
        time.sleep(0.5)
        # Render the exact generated PDF page crisply inside the viewer frame (headless Chrome doesn't run PDF plugin inside blob iframe)
        page.evaluate(
            """(dataUri) => {
                const frame = document.getElementById('pdfFrame');
                const wrapper = document.createElement('div');
                wrapper.style.cssText = 'width:100vw;height:calc(100vh - 54px);background:#334155;display:flex;justify-content:center;align-items:center;padding:14px;box-sizing:border-box;overflow:hidden;';
                const pageBox = document.createElement('div');
                pageBox.id = 'renderedLetterPage';
                pageBox.style.cssText = 'height:100%;aspect-ratio:210/297;background:#fff;border-radius:6px;box-shadow:0 20px 50px rgba(0,0,0,0.45);position:relative;overflow:hidden;';
                const img = document.createElement('img');
                img.src = dataUri;
                img.style.cssText = 'width:100%;height:100%;object-fit:contain;display:block;';
                const qrHotspot = document.createElement('div');
                qrHotspot.id = 'qrSecurityFooterTarget';
                qrHotspot.style.cssText = 'position:absolute;left:5%;right:5%;bottom:2.5%;height:11%;pointer-events:none;';
                pageBox.appendChild(img);
                pageBox.appendChild(qrHotspot);
                wrapper.appendChild(pageBox);
                if (frame) frame.replaceWith(wrapper);
            }""",
            dec_letter_data_uri,
        )
        time.sleep(0.4)
        qr_footer_loc = page.locator("#qrSecurityFooterTarget")
        shot_13_5 = os.path.join(STAGE_SHOTS, "custody_lifecycle", "step5_qr_bank_instruction_letter.png")
        page.screenshot(path=shot_13_5)
        steps_1_3.append(
            {
                "step": 5,
                "url": "app.growbusinessdevelopment.com/end-user/lg-records/instructions/view-letter",
                "image": "/demo/shots/custody_lifecycle/step5_qr_bank_instruction_letter.png",
                "hotspot": measure_box_pct(qr_footer_loc),
            }
        )

        # Step 1.3.6: Public Bank Authenticity Verification Portal (`/public/verify-instruction?t=...`)
        pub_page = context.new_page()
        pub_page.goto(f"http://localhost:3000/public/verify-instruction?t={qr_token}", wait_until="networkidle")
        pub_page.wait_for_selector("text=Verify Physical Document", timeout=15000)
        time.sleep(1.6)
        pub_page.locator("input[placeholder='e.g. 45']").fill("40")
        pub_page.locator("input[placeholder='0000']").fill(footer_pin)
        time.sleep(0.3)
        pub_page.locator("button[type='submit']").click()
        pub_page.wait_for_selector("text=Verified Genuine Corporate Instruction", timeout=15000)
        time.sleep(0.5)
        verified_banner = pub_page.locator("text=Verified Genuine Corporate Instruction").first
        shot_13_6 = os.path.join(STAGE_SHOTS, "custody_lifecycle", "step6_public_qr_verified_portal.png")
        pub_page.screenshot(path=shot_13_6)
        steps_1_3.append(
            {
                "step": 6,
                "url": "app.growbusinessdevelopment.com/public/verify-instruction",
                "image": "/demo/shots/custody_lifecycle/step6_public_qr_verified_portal.png",
                "hotspot": measure_box_pct(verified_banner),
            }
        )
        pub_page.close()

        # ═══════════════════════════════════════════════════════════════════════
        # TOUR 1.4: Delivery Proof & AI Bank Reply Verification (6 Steps)
        # ═══════════════════════════════════════════════════════════════════════
        print("[4/5] Capturing Tour 1.4: Delivery Proof & AI Bank Reply Verification...")
        set_browser_auth(page, maker_token, "end_user")
        page.goto(f"http://localhost:3000/end-user/lg-records/{lg_8841_id}", wait_until="networkidle")
        hide_toasts(page)
        page.wait_for_selector("text=In-Flight Instruction", timeout=15000)
        time.sleep(0.5)

        # Step 1.4.1: In-Flight Instruction Banner (`Awaiting Bank Delivery`)
        rec_deliv_btn = page.locator("button:has-text('Record Delivery')").first
        shot_14_1 = os.path.join(STAGE_SHOTS, "custody_delivery_reply", "step1_inflight_awaiting_delivery.png")
        page.screenshot(path=shot_14_1)
        steps_1_4.append(
            {
                "step": 1,
                "url": "app.growbusinessdevelopment.com/end-user/lg-records/details#in-flight",
                "image": "/demo/shots/custody_delivery_reply/step1_inflight_awaiting_delivery.png",
                "hotspot": measure_box_pct(rec_deliv_btn),
            }
        )

        # Prepare Stamped Delivery Copy PDF (stamped YESTERDAY while user enters TODAY -> triggers 1-click Bank Stamp Date Alignment!)
        today_iso = date.today().isoformat()
        yesterday_iso = (date.today() - timedelta(days=1)).isoformat()
        two_days_ago_iso = (date.today() - timedelta(days=2)).isoformat()
        req_exp_str = str(inst_8841_details.get("new_expiry_date") or (date.today() + timedelta(days=192)).isoformat())[:10]
        old_exp_str = str(inst_8841_details.get("old_expiry_date") or (date.today() + timedelta(days=12)).isoformat())[:10]
        req_exp_dt = datetime.strptime(req_exp_str, "%Y-%m-%d").date()
        bank_confirmed_exp_iso = (req_exp_dt + timedelta(days=2)).isoformat()

        stamped_deliv_pdf = os.path.join(STAGE_DOCS, "FNCB_Stamped_Delivery_Copy_8841.pdf")
        generate_stamped_delivery_copy_pdf(
            stamped_deliv_pdf,
            lg_number="LG-FNCB-2026-8841",
            instruction_serial=inst_8841_serial,
            issue_date_iso=two_days_ago_iso,
            stamp_date_iso=yesterday_iso,
            requested_expiry_iso=req_exp_str,
        )

        # Step 1.4.2: Upload Stamped Copy -> Live AI Delivery Verification + 1-Click Stamp Date Alignment
        rec_deliv_btn.click()
        deliv_date_inp = page.locator("input[name='delivery_date'], input[type='date']").first
        deliv_date_inp.wait_for(state="visible", timeout=10000)
        deliv_date_inp.fill(today_iso)
        page.locator("input[type='file']").last.set_input_files(stamped_deliv_pdf)
        time.sleep(0.3)
        page.locator("form button[type='submit']:has-text('Record Delivery')").click()

        align_stamp_btn = page.locator(f"button:has-text('Align Date to {yesterday_iso}')").first
        align_stamp_btn.wait_for(state="visible", timeout=35000)
        time.sleep(0.5)
        shot_14_2 = os.path.join(STAGE_SHOTS, "custody_delivery_reply", "step2_ai_delivery_stamp_verification.png")
        page.screenshot(path=shot_14_2)
        steps_1_4.append(
            {
                "step": 2,
                "url": "app.growbusinessdevelopment.com/end-user/lg-records/details#delivery-ai",
                "image": "/demo/shots/custody_delivery_reply/step2_ai_delivery_stamp_verification.png",
                "hotspot": measure_box_pct(align_stamp_btn),
            }
        )
        # Click 1-click Align Date to Bank Stamp -> records delivery cleanly!
        align_stamp_btn.click()
        # If modal transitions to "Confirm & Record Delivery", click it to complete
        time.sleep(1.2)
        confirm_after_align = page.locator("button:has-text('Confirm & Record Delivery')")
        if confirm_after_align.count() > 0 and confirm_after_align.first.is_visible():
            confirm_after_align.first.click()
        page.wait_for_selector("button:has-text('Record Bank Reply')", timeout=20000)
        time.sleep(0.6)

        # Step 1.4.3: Updated In-Flight Banner (`Delivered — Awaiting Bank Reply`)
        rec_reply_btn = page.locator("button:has-text('Record Bank Reply')").first
        shot_14_3 = os.path.join(STAGE_SHOTS, "custody_delivery_reply", "step3_delivered_awaiting_bank_reply.png")
        page.screenshot(path=shot_14_3)
        steps_1_4.append(
            {
                "step": 3,
                "url": "app.growbusinessdevelopment.com/end-user/lg-records/details#awaiting-reply",
                "image": "/demo/shots/custody_delivery_reply/step3_delivered_awaiting_bank_reply.png",
                "hotspot": measure_box_pct(rec_reply_btn),
            }
        )

        # Generate Official FNCB Extension Advice PDF (+2 days calendar convention variance!)
        bank_advice_pdf = os.path.join(STAGE_DOCS, "FNCB_Official_Bank_Advice_8841.pdf")
        bank_advice_png = os.path.join(STAGE_DOCS, "FNCB_Official_Bank_Advice_8841_preview.png")
        generate_bank_extension_advice_pdf(
            bank_advice_pdf,
            lg_number="LG-FNCB-2026-8841",
            instruction_serial=inst_8841_serial,
            letter_date_iso=today_iso,
            prior_expiry_iso=old_exp_str,
            confirmed_expiry_iso=bank_confirmed_exp_iso,
        )
        render_pdf_first_page_png(bank_advice_pdf, bank_advice_png)
        with open(bank_advice_pdf, "rb") as f:
            advice_data_uri = pdf_bytes_to_data_uri(f.read(), scale=2.3)

        # Step 1.4.4: Upload Bank Reply Advice -> Live AI Cross-Check + Calendar Convention Variance (+2d)
        rec_reply_btn.click()
        reply_date_inp = page.locator("input[name='bank_reply_date'], input[type='date']").first
        reply_date_inp.wait_for(state="visible", timeout=10000)
        reply_date_inp.fill(today_iso)
        reply_notes = page.locator("textarea[name='reply_details']")
        if reply_notes.count() > 0:
            reply_notes.first.fill("Official FNCB extension confirmation advice Ref. FNCB/TF/EXT/2026/9914 received.")
        page.locator("input[type='file']").last.set_input_files(bank_advice_pdf)
        time.sleep(0.3)
        page.locator("form button[type='submit']:has-text('Record Bank Reply')").click()

        inspect_scan_btn = page.locator("button:has-text('Inspect Scan')").first
        inspect_scan_btn.wait_for(state="visible", timeout=35000)
        time.sleep(0.5)
        shot_14_4 = os.path.join(STAGE_SHOTS, "custody_delivery_reply", "step4_ai_bank_reply_verification.png")
        page.screenshot(path=shot_14_4)
        steps_1_4.append(
            {
                "step": 4,
                "url": "app.growbusinessdevelopment.com/end-user/lg-records/details#bank-reply-ai",
                "image": "/demo/shots/custody_delivery_reply/step4_ai_bank_reply_verification.png",
                "hotspot": measure_box_pct(inspect_scan_btn),
            }
        )

        # Step 1.4.5: Split-Screen Document & AI Verification Inspector (`DocumentInspectorModal`)
        inspect_scan_btn.click()
        accept_align_inspector_btn = page.locator(f"button:has-text('Accept & Align to {bank_confirmed_exp_iso}')").last
        accept_align_inspector_btn.wait_for(state="visible", timeout=10000)
        # Replace blob PDF iframe in headless Chrome with exact rendered PNG of the uploaded FNCB Bank Advice PDF
        page.evaluate(
            """(dataUri) => {
                const iframes = document.querySelectorAll('iframe');
                iframes.forEach((fr) => {
                    const img = document.createElement('img');
                    img.src = dataUri;
                    img.style.cssText = 'width:100%;height:100%;object-fit:contain;background:#fff;display:block;';
                    fr.replaceWith(img);
                });
            }""",
            advice_data_uri,
        )
        time.sleep(0.5)
        shot_14_5 = os.path.join(STAGE_SHOTS, "custody_delivery_reply", "step5_split_document_ai_inspector.png")
        page.screenshot(path=shot_14_5)
        steps_1_4.append(
            {
                "step": 5,
                "url": "app.growbusinessdevelopment.com/end-user/lg-records/details#ai-inspector",
                "image": "/demo/shots/custody_delivery_reply/step5_split_document_ai_inspector.png",
                "hotspot": measure_box_pct(accept_align_inspector_btn),
            }
        )

        # Click Accept & Align to Bank's Confirmed Date -> then click Confirm to save & close both modals!
        accept_align_inspector_btn.click()
        time.sleep(0.5)
        inspector_confirm_btn = page.locator("button:has-text('Confirm')").last
        if inspector_confirm_btn.count() > 0 and inspector_confirm_btn.is_visible():
            inspector_confirm_btn.click()
            time.sleep(0.6)
        reply_confirm_btn = page.locator("button:has-text('Confirm & Record')").last
        if reply_confirm_btn.count() > 0 and reply_confirm_btn.is_visible():
            reply_confirm_btn.click()
        try:
            page.wait_for_selector("text=Record Bank Reply for Instruction:", state="hidden", timeout=25000)
        except Exception:
            time.sleep(2.5)
        time.sleep(0.8)

        # Step 1.4.6: Closed-Loop Lifecycle History Audit Trail (`Confirmed by Bank`)
        page.locator("nav button:has-text('Lifecycle History')").click()
        time.sleep(0.8)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.3)
        back_list_btn = page.locator("button:has-text('Back to LG Records')").first
        shot_14_6 = os.path.join(STAGE_SHOTS, "custody_delivery_reply", "step6_closed_loop_lifecycle_history.png")
        page.screenshot(path=shot_14_6)
        steps_1_4.append(
            {
                "step": 6,
                "url": "app.growbusinessdevelopment.com/end-user/lg-records/details#lifecycle-history",
                "image": "/demo/shots/custody_delivery_reply/step6_closed_loop_lifecycle_history.png",
                "hotspot": measure_box_pct(back_list_btn),
            }
        )

        browser.close()

    # ═══════════════════════════════════════════════════════════════════════
    # Copy Staged Captures & PDFs into frontend/public/demo & Update Manifest
    # ═══════════════════════════════════════════════════════════════════════
    print("[5/5] Publishing retina screenshots, sample PDFs & updating pilotManifest.json...")
    os.makedirs(PUBLIC_DOCS_DIR, exist_ok=True)
    for fname in (
        "FNCB_Extension_Instruction_8841.pdf",
        "FNCB_Extension_Instruction_8841_preview.png",
        "FNCB_Decrease_Instruction_8840.pdf",
        "FNCB_Decrease_Instruction_8840_preview.png",
        "FNCB_Official_Bank_Advice_8841.pdf",
        "FNCB_Official_Bank_Advice_8841_preview.png",
    ):
        shutil.copy2(os.path.join(STAGE_DOCS, fname), os.path.join(PUBLIC_DOCS_DIR, fname))

    for sub in ("custody_action_center", "custody_lifecycle", "custody_delivery_reply"):
        dst_sub = os.path.join(PUBLIC_SHOTS_DIR, sub)
        os.makedirs(dst_sub, exist_ok=True)
        src_sub = os.path.join(STAGE_SHOTS, sub)
        for f in os.listdir(src_sub):
            shutil.copy2(os.path.join(src_sub, f), os.path.join(dst_sub, f))

    existing_manifest = {}
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            existing_manifest = json.load(f)

    scan_steps = existing_manifest.get("capturedSteps", [])
    scan_urls = {
        1: "app.growbusinessdevelopment.com/end-user/lg-records/new",
        2: "app.growbusinessdevelopment.com/end-user/lg-records/new",
        3: "app.growbusinessdevelopment.com/end-user/lg-records/new#main-lg-data",
        4: "app.growbusinessdevelopment.com/end-user/lg-records/new#bank-data",
        5: "app.growbusinessdevelopment.com/end-user/lg-records/new#internal-data",
        6: "app.growbusinessdevelopment.com/end-user/lg-records",
        7: "app.growbusinessdevelopment.com/end-user/lg-records/details",
    }
    for s in scan_steps:
        s.setdefault("url", scan_urls.get(s.get("step"), "app.growbusinessdevelopment.com/end-user/lg-records"))

    updated_manifest = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H},
        "document": existing_manifest.get("document", {}),
        "capturedSteps": scan_steps,
        "branches": {
            "custody-scan-record": {
                "audioDir": "custody_scan",
                "document": {
                    "tabLabelEn": "Bank LG PDF",
                    "tabLabelAr": "أصل الضمان",
                    "badgeEn": "Sample Bank Guarantee Input",
                    "badgeAr": "نموذج خطاب الضمان للتجربة",
                    "titleEn": "FNCB Performance LG · EGP 4,500,000",
                    "titleAr": "خطاب ضمان حسن تنفيذ FNCB · ٤.٥ مليون ج.م",
                    "pdfUrl": "/demo/docs/FNCB_Performance_LG_8840.pdf",
                    "previewImageUrl": "/demo/docs/FNCB_Performance_LG_8840_preview.png",
                },
                "capturedSteps": scan_steps,
            },
            "custody-action-center": {
                "audioDir": "custody_action_center",
                "document": {
                    "tabLabelEn": "Extension PDF",
                    "tabLabelAr": "خطاب التمديد",
                    "badgeEn": "Generated Protective Extension Letter",
                    "badgeAr": "نموذج خطاب التمديد البنكي الموثق",
                    "titleEn": "FNCB Extension Instruction · QR Sealed",
                    "titleAr": "خطاب تمديد ضمان FNCB · موثق برمز QR",
                    "pdfUrl": "/demo/docs/FNCB_Extension_Instruction_8841.pdf",
                    "previewImageUrl": "/demo/docs/FNCB_Extension_Instruction_8841_preview.png",
                },
                "capturedSteps": steps_1_2,
            },
            "custody-lifecycle-letter": {
                "audioDir": "custody_lifecycle",
                "document": {
                    "tabLabelEn": "QR Letter PDF",
                    "tabLabelAr": "خطاب البنك الموثق",
                    "badgeEn": "QR + Split Security Code Bank Letter",
                    "badgeAr": "خطاب تعليمات بنكية بختم QR وكود الأمان",
                    "titleEn": "FNCB Amount Reduction Letter · EGP 3.0M",
                    "titleAr": "خطاب تخفيض ضمان FNCB إلى ٣.٠ مليون ج.م",
                    "pdfUrl": "/demo/docs/FNCB_Decrease_Instruction_8840.pdf",
                    "previewImageUrl": "/demo/docs/FNCB_Decrease_Instruction_8840_preview.png",
                },
                "capturedSteps": steps_1_3,
            },
            "custody-delivery-reply": {
                "audioDir": "custody_delivery_reply",
                "document": {
                    "tabLabelEn": "Bank Reply PDF",
                    "tabLabelAr": "رد البنك الرسمي",
                    "badgeEn": "Sample Bank Amendment Advice",
                    "badgeAr": "نموذج إشعار البنك الرسمي بالتمديد",
                    "titleEn": "FNCB Official Extension Advice · LG-8841",
                    "titleAr": "إشعار البنك الرسمي بتمديد الضمان · LG-8841",
                    "pdfUrl": "/demo/docs/FNCB_Official_Bank_Advice_8841.pdf",
                    "previewImageUrl": "/demo/docs/FNCB_Official_Bank_Advice_8841_preview.png",
                },
                "capturedSteps": steps_1_4,
            },
        },
    }

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(updated_manifest, f, indent=2, ensure_ascii=False)

    print("ALL_CUSTODY_TOURS_CAPTURED_SUCCESSFULLY")
    print(f"  1.2 steps: {len(steps_1_2)} | 1.3 steps: {len(steps_1_3)} | 1.4 steps: {len(steps_1_4)}")


if __name__ == "__main__":
    main()
