"""
Automated Real-System Test & Screen Capture for Customer Demo — Pilot #1
Branch 1.1: LG Custody -> Scan & Record a New LG (AI OCR to Custody Cockpit)

1. Generates a clean, customer-ready CIB Performance LG PDF (bilingual header, zero test watermarks)
   + high-DPI PNG page preview for the side-by-side "Inspect Original Document" drawer.
2. Prepares idempotent local seed data for Apex Global Engineering & Contracting SAE.
3. Drives real Google Chrome (1440x900 @ 2x retina) against live localhost:3000 + localhost:8000,
   executing the real Gemini AI OCR scan and real DB creation, while measuring exact DOM
   bounding boxes for interactive click hotspots.
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import fitz  # PyMuPDF
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.auth_v2.services import AuthService
from app.core.security import create_access_token
from app.models import User

import shutil

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PUBLIC_DEMO_DIR = os.path.join(BASE_DIR, "frontend", "public", "demo")
DOCS_DIR = os.path.join(PUBLIC_DEMO_DIR, "docs")
FINAL_SHOTS_DIR = os.path.join(PUBLIC_DEMO_DIR, "shots", "custody_scan")
# Write screenshots outside frontend/ during capture so webpack-dev-server doesn't live-reload mid-run!
SHOTS_DIR = os.path.join(BASE_DIR, "scratch", "demo_shots_staging")
MANIFEST_OUT = os.path.join(BASE_DIR, "frontend", "src", "components", "Showcase", "pilotManifest.json")

os.makedirs(DOCS_DIR, exist_ok=True)
os.makedirs(FINAL_SHOTS_DIR, exist_ok=True)
os.makedirs(SHOTS_DIR, exist_ok=True)
os.makedirs(os.path.dirname(MANIFEST_OUT), exist_ok=True)

VIEWPORT_W = 1440
VIEWPORT_H = 900
DEMO_LG_NUMBER = "LG-FNCB-2026-8840"


def generate_clean_demo_lg_pdf() -> tuple[str, str]:
    """Generates a realistic, 100% fictional corporate bank Performance Guarantee PDF and PNG preview."""
    pdf_path = os.path.join(SHOTS_DIR, "FNCB_Performance_LG_8840.pdf")
    png_path = os.path.join(SHOTS_DIR, "FNCB_Performance_LG_8840_preview.png")

    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=38,
        rightMargin=38,
        topMargin=34,
        bottomMargin=34,
    )
    styles = getSampleStyleSheet()

    bank_title = ParagraphStyle(
        "BankTitle",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#0f2942"),
        alignment=TA_CENTER,
        spaceAfter=2,
    )
    bank_sub = ParagraphStyle(
        "BankSub",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor("#475569"),
        alignment=TA_CENTER,
        spaceAfter=8,
    )
    doc_heading = ParagraphStyle(
        "DocHeading",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=14,
        textColor=colors.HexColor("#1d4ed8"),
        alignment=TA_CENTER,
        spaceBefore=4,
        spaceAfter=8,
    )
    sec_h = ParagraphStyle(
        "SecH",
        parent=styles["Heading3"],
        fontName="Helvetica-Bold",
        fontSize=9.5,
        leading=13,
        textColor=colors.HexColor("#0f172a"),
        spaceBefore=8,
        spaceAfter=4,
    )
    body = ParagraphStyle(
        "Body",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8.8,
        leading=13,
        textColor=colors.HexColor("#1e293b"),
        alignment=TA_JUSTIFY,
        spaceAfter=6,
    )
    cell = ParagraphStyle(
        "Cell",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8.2,
        leading=11,
        textColor=colors.HexColor("#1e293b"),
    )
    cell_b = ParagraphStyle(
        "CellB",
        parent=cell,
        fontName="Helvetica-Bold",
    )

    story = []
    story.append(Paragraph("FIRST NATIONAL CORPORATE BANK S.A.E.", bank_title))
    story.append(
        Paragraph(
            "CORPORATE TRADE FINANCE & LETTERS OF GUARANTEE CENTER — FINANCIAL DISTRICT, CAIRO<br/>"
            "Hotline: 19000  |  SWIFT: FNCBEGCXXXX",
            bank_sub,
        )
    )
    story.append(HRFlowable(width="100%", thickness=1.5, color=colors.HexColor("#1d4ed8"), spaceAfter=6))
    story.append(Paragraph("IRREVOCABLE LETTER OF GUARANTEE — PERFORMANCE GUARANTEE", doc_heading))

    meta_rows = [
        [
            Paragraph("<b>Guarantee No (LG Ref):</b>", cell),
            Paragraph(f"<b>{DEMO_LG_NUMBER}</b>", cell_b),
            Paragraph("<b>Issuance Date:</b>", cell),
            Paragraph("2026-11-01", cell),
        ],
        [
            Paragraph("<b>Beneficiary Corporate:</b>", cell),
            Paragraph("<b>Apex Global Engineering SAE (HQ)</b>", cell_b),
            Paragraph("<b>Expiry Date:</b>", cell),
            Paragraph("<b>2027-11-01</b>", cell_b),
        ],
        [
            Paragraph("<b>Applicant / Ordered By:</b>", cell),
            Paragraph("NorthStar Electromechanical Co. SAE (ID: CR-441092)", cell),
            Paragraph("<b>Guarantee Type:</b>", cell),
            Paragraph("Performance Guarantee", cell),
        ],
        [
            Paragraph("<b>Currency & Amount:</b>", cell),
            Paragraph("<b>EGP 4,500,000.00</b>", cell_b),
            Paragraph("<b>Applicable Rules:</b>", cell),
            Paragraph("URDG 758", cell),
        ],
        [
            Paragraph("<b>Underlying Contract:</b>", cell),
            Paragraph("Contract Ref #APEX-SUB-2026-14 — 220kV Substation Switchgear Supply & Commissioning", cell),
            Paragraph("<b>Issuing Branch:</b>", cell),
            Paragraph("Corporate Center (#0104)", cell),
        ],
    ]
    t = Table(meta_rows, colWidths=[112, 168, 98, 141])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
                ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#94a3b8")),
                ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 10))

    story.append(Paragraph("To: M/s Apex Global Engineering SAE (HQ), Plot B12, Business Park, Cairo, Egypt", cell_b))
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            f"Dear Sirs,<br/><br/>"
            f"At the request and for the account of our client <b>NorthStar Electromechanical Co. SAE</b>, we, "
            f"<b>First National Corporate Bank S.A.E.</b>, Corporate Trade Finance Center, hereby issue our "
            f"irrevocable and unconditional Performance Letter of Guarantee No. <b>{DEMO_LG_NUMBER}</b> in your favor "
            f"for a maximum aggregate sum not exceeding <b>EGP 4,500,000.00 (Four Million Five Hundred Thousand Egyptian Pounds Only)</b>, "
            f"representing 5% final performance security under <b>Contract Ref #APEX-SUB-2026-14</b> for the supply, installation, "
            f"and commissioning of 220kV GIS Substation Switchgear.",
            body,
        )
    )
    story.append(Paragraph("UNDERTAKING & SPECIAL CLAIM CONDITIONS", sec_h))
    story.append(
        Paragraph(
            "We undertake to pay you any sum or sums not exceeding in total the above-mentioned amount of EGP 4,500,000.00 "
            "upon receipt at our counters of your first written demand, without cavil or objection from the Applicant, "
            "strictly subject to the following claim wording and presentation requirements:<br/>"
            "• <b>Mandatory Claim Statement:</b> Any written demand for payment must explicitly state: "
            "<i>\"We hereby certify that NorthStar Electromechanical Co. SAE has failed to fulfill its contractual obligations "
            "under Contract Ref #APEX-SUB-2026-14 and the amount claimed is rightfully due.\"</i><br/>"
            "• <b>Original Presentation:</b> Any liquidation or claim request must be accompanied by this original Letter of Guarantee "
            "delivered to First National Corporate Bank Trade Finance Counter prior to expiry.",
            body,
        )
    )
    story.append(Paragraph("VALIDITY & EXPIRY CLAUSE", sec_h))
    story.append(
        Paragraph(
            "This Letter of Guarantee enters into force on <b>01-Nov-2026</b> and shall remain valid in full force and effect "
            "until <b>01-Nov-2027</b> (Expiry Date: 2027-11-01). Any claim hereunder must reach First National Corporate Bank (FNCB) "
            "Corporate Trade Finance Center on or before <b>01-Nov-2027</b>, after which date this guarantee shall automatically "
            "become null and void and of no legal effect whatsoever whether the original instrument is returned to us or not. "
            "This guarantee is subject to the Uniform Rules for Demand Guarantees (ICC Publication No. <b>URDG 758</b>).",
            body,
        )
    )
    story.append(Spacer(1, 12))

    stamp_rows = [
        [
            Paragraph(
                "<b>OFFICIAL BANK SEAL — FNCB TRADE FINANCE</b><br/>"
                "Corporate Trade Finance Center (Branch #0104)<br/>"
                "Authorized Signatory — Class A<br/>"
                "Authorized Signatory — Class B",
                cell,
            ),
            Paragraph(
                "<font color='#b91c1c'><b>SECURITY VALUE STAMP — VERIFIED ORIGINAL</b></font><br/>"
                "<font color='#b91c1c'><b>GOOD FOR UP TO EGP 4,500,000.00</b></font><br/>"
                f"Issued: 01-Nov-2026  |  Ref: {DEMO_LG_NUMBER}<br/>"
                "Verified Trade Instrument — Non-Transferable",
                cell,
            ),
        ]
    ]
    st = Table(stamp_rows, colWidths=[258, 261])
    st.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, 0), colors.HexColor("#eff6ff")),
                ("BACKGROUND", (1, 0), (1, 0), colors.HexColor("#fef2f2")),
                ("BOX", (0, 0), (0, 0), 0.8, colors.HexColor("#3b82f6")),
                ("BOX", (1, 0), (1, 0), 0.8, colors.HexColor("#ef4444")),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    story.append(st)

    doc.build(story)

    # Render crisp 2x PNG preview of Page 1 for instant side-by-side drawer viewing
    pdf_doc = fitz.open(pdf_path)
    pix = pdf_doc[0].get_pixmap(matrix=fitz.Matrix(2.0, 2.0), alpha=False)
    pix.save(png_path)
    pdf_doc.close()

    print(f"[OK] Generated clean demo LG PDF: {pdf_path}")
    print(f"[OK] Generated PNG preview: {png_path}")
    return pdf_path, png_path


async def prepare_db_and_mint_token() -> tuple[str, dict]:
    """Ensures idempotent demo seed data for Apex Global (customer_id=23) and returns a valid JWT."""
    engine = create_engine(os.environ["DATABASE_URL"])
    SessionLocal = sessionmaker(bind=engine)

    with engine.begin() as conn:
        # 1. Retire any previous demo runs cleanly and free up lg_sequence_number=1
        suffix = int(time.time())
        conn.execute(
            text(
                """
                UPDATE lg_records
                SET lg_number = CASE
                        WHEN lg_number IN (:lg_no, 'LG-CIB-2026-8840') THEN lg_number || '-OLD-' || :suf
                        ELSE lg_number
                    END,
                    lg_sequence_number = 90000 + id,
                    is_deleted = TRUE,
                    deleted_at = COALESCE(deleted_at, NOW())
                WHERE lg_number IN (:lg_no, 'LG-CIB-2026-8840')
                   OR lg_number LIKE 'LG-CIB-2026-8840-OLD-%'
                   OR lg_number LIKE 'LG-FNCB-2026-8840-OLD-%'
                   OR (beneficiary_corporate_id = 27 AND is_deleted = TRUE AND lg_sequence_number < 90000)
                """
            ),
            {"suf": str(suffix), "lg_no": DEMO_LG_NUMBER},
        )

        # 1b. Ensure 100% fictional demo bank 'First National Corporate Bank' (FNCB) exists in banks
        bank_id = conn.execute(
            text("SELECT id FROM banks WHERE swift_code = 'FNCBEGCXXXX' OR name = 'First National Corporate Bank'")
        ).scalar()
        if not bank_id:
            conn.execute(
                text(
                    """
                    INSERT INTO banks
                        (name, short_name, swift_code, address, phone_number, fax, email_domain,
                         total_completed_issuances, portal_access_enabled, allow_dealer_comments,
                         allow_dealer_feedback, allow_dealer_mobile_alerts, allow_manual_recording,
                         created_at, updated_at, is_deleted)
                    VALUES
                        ('First National Corporate Bank', 'FNCB', 'FNCBEGCXXXX',
                         '10 Financial District Blvd., Corporate Tower, Cairo', '19000', '+20 2 2790 1000',
                         'fncb-demo.example', 0, TRUE, TRUE, TRUE, TRUE, FALSE, NOW(), NOW(), FALSE)
                    """
                )
            )

        # 2. Ensure treasury@apexglobal.com has accepted latest legal versions and no forced password reset
        conn.execute(
            text(
                """
                UPDATE users
                SET must_change_password = FALSE,
                    last_accepted_legal_version = 99.0,
                    has_all_entity_access = TRUE
                WHERE email = 'treasury@apexglobal.com'
                """
            )
        )

        # 3. Retire personal-name owner & ensure role-based Internal Owner Contact exists for Apex Global
        conn.execute(
            text(
                """
                UPDATE internal_owner_contacts
                SET is_deleted = TRUE, deleted_at = COALESCE(deleted_at, NOW())
                WHERE customer_id = 23 AND email = 'karim.mostafa@apexglobal.com'
                """
            )
        )
        owner_id = conn.execute(
            text(
                """
                SELECT id FROM internal_owner_contacts
                WHERE customer_id = 23 AND email = 'project.owner@apexglobal.com' AND is_deleted = FALSE
                """
            )
        ).scalar()
        if not owner_id:
            conn.execute(
                text(
                    """
                    INSERT INTO internal_owner_contacts
                        (customer_id, email, phone_number, internal_id, manager_email, created_at, updated_at, is_deleted)
                    VALUES
                        (23, 'project.owner@apexglobal.com', '+20 100 000 1042', 'EMP-1042', 'contracts.lead@apexglobal.com', NOW(), NOW(), FALSE)
                    """
                )
            )

        # 4. Ensure clean EP LG Category exists for Apex Global (customer_id=23)
        cat_id = conn.execute(
            text(
                """
                SELECT id FROM lg_categories
                WHERE customer_id = 23 AND code = 'EP' AND is_deleted = FALSE
                """
            )
        ).scalar()
        if not cat_id:
            conn.execute(
                text(
                    """
                    INSERT INTO lg_categories
                        (customer_id, name, code, extra_field_name, is_mandatory, is_default, has_all_entity_access, created_at, updated_at, is_deleted)
                    VALUES
                        (23, 'EPC & Power Projects', 'EP', 'Contract Code', TRUE, FALSE, TRUE, NOW(), NOW(), FALSE)
                    """
                )
            )

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == "treasury@apexglobal.com", User.is_deleted == False).first()
        if not user:
            raise RuntimeError("User treasury@apexglobal.com not found in local DB")
        auth_svc = AuthService()
        token_payload = await auth_svc.build_user_token_data(db, user)
        token_payload["must_accept_policies"] = False
        token_payload["is_mfa_verified"] = True
        jwt_token = create_access_token(token_payload, expires_delta=timedelta(hours=2))
        return jwt_token, token_payload
    finally:
        db.close()


async def scroll_into_stage(page, locator, top_offset: int = 80):
    """Scrolls the inner overflow-y-auto layout container so `locator` sits cleanly at `top_offset` px from top."""
    handle = await locator.element_handle()
    if not handle:
        return
    await page.evaluate(
        """({ el, offset }) => {
            let parent = el.parentElement;
            while (parent && parent !== document.body) {
                const style = window.getComputedStyle(parent);
                if ((style.overflowY === 'auto' || style.overflowY === 'scroll') && parent.scrollHeight > parent.clientHeight) {
                    const parentRect = parent.getBoundingClientRect();
                    const elRect = el.getBoundingClientRect();
                    parent.scrollTop += (elRect.top - parentRect.top) - offset;
                    return;
                }
                parent = parent.parentElement;
            }
            const rect = el.getBoundingClientRect();
            window.scrollBy(0, rect.top - offset);
        }""",
        {"el": handle, "offset": top_offset},
    )


async def measure_box_pct(locator) -> dict:
    """Measures DOM element bounding box as viewport percentages for responsive hotspot positioning."""
    box = await locator.bounding_box()
    if not box:
        return {"xPct": 50.0, "yPct": 50.0, "wPct": 14.0, "hPct": 6.0}
    return {
        "xPct": round((box["x"] / VIEWPORT_W) * 100, 2),
        "yPct": round((box["y"] / VIEWPORT_H) * 100, 2),
        "wPct": round((box["width"] / VIEWPORT_W) * 100, 2),
        "hPct": round((box["height"] / VIEWPORT_H) * 100, 2),
    }


async def run_browser_capture(pdf_path: str, jwt_token: str):
    from playwright.async_api import async_playwright

    steps_captured = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(channel="chrome", headless=True)
        context = await browser.new_context(
            viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
            device_scale_factor=2,
        )
        page = await context.new_page()

        # Seed JWT token cleanly before navigating to protected route
        await page.goto("http://localhost:3000/login", wait_until="domcontentloaded")
        now_ms = int(time.time() * 1000)
        await page.evaluate(
            """({ token, now }) => {
                localStorage.setItem('jwt_token', token);
                localStorage.setItem('last_activity_at', String(now));
                localStorage.setItem('last_token_refresh_at', String(now));
                localStorage.setItem('i18nextLng', 'en');
            }""",
            {"token": jwt_token, "now": now_ms},
        )

        # Navigate to Record New LG
        print("[1/7] Navigating to /end-user/lg-records/new ...")
        await page.goto("http://localhost:3000/end-user/lg-records/new", wait_until="networkidle")
        upload_btn = page.locator("button:has-text('Upload & Scan')").first
        await upload_btn.wait_for(state="visible", timeout=20000)
        await page.wait_for_timeout(800)

        # Minimize floating AI Assistant badge to clean sparkle icon so 'POC' label doesn't distract
        min_ai_btn = page.locator("button[aria-label='Minimize button']")
        if await min_ai_btn.count() > 0:
            await min_ai_btn.first.click()
            await page.wait_for_timeout(300)

        # Hide toast containers if any stray toast exists
        await page.evaluate("document.querySelectorAll('.Toastify').forEach(e => e.style.display = 'none')")
        box1 = await measure_box_pct(upload_btn)
        shot1_file = "step1_upload_zone.png"
        await page.screenshot(path=os.path.join(SHOTS_DIR, shot1_file))
        steps_captured.append({"step": 1, "image": f"/demo/shots/custody_scan/{shot1_file}", "hotspot": box1})
        print(f"  -> Captured {shot1_file} | Hotspot: {box1}")

        # Step 2: Attach PDF file to #ai_scan_file
        print("[2/7] Attaching real CIB Performance LG PDF ...")
        await page.locator("#ai_scan_file").set_input_files(pdf_path)
        scan_now_btn = page.locator("button:has-text('Scan Now')").first
        await scan_now_btn.wait_for(state="visible", timeout=10000)
        await page.wait_for_timeout(500)

        box2 = await measure_box_pct(scan_now_btn)
        shot2_file = "step2_file_ready.png"
        await page.screenshot(path=os.path.join(SHOTS_DIR, shot2_file))
        steps_captured.append({"step": 2, "image": f"/demo/shots/custody_scan/{shot2_file}", "hotspot": box2})
        print(f"  -> Captured {shot2_file} | Hotspot: {box2}")

        # Step 3: Trigger Real AI OCR Scan and wait for completion!
        print(f"[3/7] Clicking 'Scan Now' on {page.url} (calling real Gemini AI OCR + entity/bank matching) ...")
        print("  Buttons currently visible:", await page.locator("button").all_inner_texts())
        await page.evaluate("() => { const b = Array.from(document.querySelectorAll('button')).find(el => el.textContent.includes('Scan Now')); if (b) b.click(); }")
        await page.locator("text=AI scan complete").first.wait_for(state="visible", timeout=90000)
        await page.wait_for_timeout(800)
        await page.evaluate("document.querySelectorAll('.Toastify').forEach(e => e.style.display = 'none')")

        # Ensure Beneficiary Corporate & LG Type & Currency are cleanly selected if fuzzy score needed nudge
        ben_val = await page.locator("#beneficiary_corporate_id").input_value()
        if not ben_val:
            await page.locator("#beneficiary_corporate_id").select_option(index=1)

        # Clear any hidden conditional fields (e.g. lg_operational_status_id when LG Type != 'Advance Payment LG')
        lg_type_sel = page.locator("#lg_type_id")
        current_lg_type_val = await lg_type_sel.input_value()
        type_opts = await lg_type_sel.locator("option").all_inner_texts()
        adv_idx = next((i for i, t in enumerate(type_opts) if "Advance Payment" in t), None)
        if adv_idx is not None:
            await lg_type_sel.select_option(index=adv_idx)
            await page.wait_for_timeout(150)
            op_sel = page.locator("#lg_operational_status_id")
            if await op_sel.count() > 0:
                op_val = await op_sel.input_value()
                if op_val:
                    print(f"  [Info] Clearing hidden lg_operational_status_id={op_val} for Performance Guarantee ...")
                    await op_sel.select_option(value="")
            if current_lg_type_val:
                await lg_type_sel.select_option(value=current_lg_type_val)
            else:
                perf_idx = next((i for i, t in enumerate(type_opts) if "Performance" in t), 1)
                await lg_type_sel.select_option(index=perf_idx)
            await page.wait_for_timeout(150)

        # Also ensure applicable_rules_text is empty when Applicable Rule != 'Other'
        rule_sel = page.locator("#applicable_rule_id")
        current_rule_val = await rule_sel.input_value()
        rule_opts = await rule_sel.locator("option").all_inner_texts()
        other_rule_idx = next((i for i, t in enumerate(rule_opts) if t.strip() == "Other"), None)
        if other_rule_idx is not None and current_rule_val:
            await rule_sel.select_option(index=other_rule_idx)
            await page.wait_for_timeout(100)
            rules_txt = page.locator("#applicable_rules_text")
            if await rules_txt.count() > 0 and await rules_txt.input_value():
                await rules_txt.fill("")
            await rule_sel.select_option(value=current_rule_val)
            await page.wait_for_timeout(100)

        # Scroll inner container so extracted Main LG Data + Bank Data header all sit cleanly in frame
        bank_acc_btn = page.locator("button:has-text('Bank Data')").first
        await scroll_into_stage(page, bank_acc_btn, top_offset=785)
        await page.wait_for_timeout(400)
        box3 = await measure_box_pct(bank_acc_btn)
        shot3_file = "step3_ai_extracted_main.png"
        await page.screenshot(path=os.path.join(SHOTS_DIR, shot3_file))
        steps_captured.append({"step": 3, "image": f"/demo/shots/custody_scan/{shot3_file}", "hotspot": box3})
        print(f"  -> Captured {shot3_file} | Hotspot: {box3}")

        # Step 4: Bank Data is already auto-opened by AI Scan! Scroll it cleanly to top of stage
        print("[4/7] Framing auto-populated 'Bank Data' & Smart Claim Intelligence ...")
        await scroll_into_stage(page, bank_acc_btn, top_offset=20)
        await page.wait_for_timeout(450)

        internal_acc_btn = page.locator("button:has-text('Internal Data')").first
        ib_check = await measure_box_pct(internal_acc_btn)
        if ib_check["yPct"] > 90:
            await scroll_into_stage(page, internal_acc_btn, top_offset=770)
            await page.wait_for_timeout(300)
        box4 = await measure_box_pct(internal_acc_btn)
        shot4_file = "step4_bank_smart_claim.png"
        await page.screenshot(path=os.path.join(SHOTS_DIR, shot4_file))
        steps_captured.append({"step": 4, "image": f"/demo/shots/custody_scan/{shot4_file}", "hotspot": box4})
        print(f"  -> Captured {shot4_file} | Hotspot: {box4}")

        # Step 5: Open Internal Data Accordion & Assign Governance Owner + Category
        print("[5/7] Opening 'Internal Data' & assigning Internal Owner + EPC Category ...")
        await internal_acc_btn.click()
        await page.wait_for_timeout(400)

        owner_input = page.locator("#internal_owner_email")
        await owner_input.fill("project.owner@apexglobal.com")
        await owner_input.dispatch_event("blur")
        await page.wait_for_timeout(400)

        # Select EPC & Power Projects category
        cat_select = page.locator("#lg_category_id")
        options_text = await cat_select.locator("option").all_inner_texts()
        epc_idx = next((i for i, t in enumerate(options_text) if "EPC" in t), 1)
        await cat_select.select_option(index=epc_idx)
        await page.wait_for_timeout(300)

        extra_input = page.locator("#additional_field_value")
        if await extra_input.count() > 0:
            await extra_input.fill("APEX-SUB-2026-14")

        proj_input = page.locator("#internal_contract_project_id")
        if await proj_input.count() > 0:
            val = await proj_input.input_value()
            if not val:
                await proj_input.fill("APEX-SUB-2026-14")

        save_btn = page.locator("button:has-text('Save LG Record')").first
        await scroll_into_stage(page, save_btn, top_offset=770)
        await page.wait_for_timeout(400)

        box5 = await measure_box_pct(save_btn)
        shot5_file = "step5_internal_governance.png"
        await page.screenshot(path=os.path.join(SHOTS_DIR, shot5_file))
        steps_captured.append({"step": 5, "image": f"/demo/shots/custody_scan/{shot5_file}", "hotspot": box5})
        print(f"  -> Captured {shot5_file} | Hotspot: {box5}")

        # Step 6: Click Save LG Record -> Navigate to Manage LG Records & locate newly created LG
        print("[6/7] Saving LG Record to real database & opening Custody Register ...")
        await save_btn.click()
        try:
            await page.wait_for_url("**/end-user/dashboard**", timeout=20000)
        except Exception:
            alert_el = page.locator("[role='alert']")
            alert_txt = await alert_el.all_inner_texts() if await alert_el.count() > 0 else []
            print(f"  [ERROR] Form did not redirect! Current URL: {page.url} | Alert: {alert_txt}")
            await page.screenshot(path=os.path.join(SHOTS_DIR, "debug_save_error.png"))
            raise
        await page.wait_for_timeout(600)

        # Dismiss any toast before clean screenshot of Manage LG Records
        await page.goto("http://localhost:3000/end-user/lg-records", wait_until="networkidle")
        lg_row = page.locator("table tbody tr", has_text=DEMO_LG_NUMBER).first
        await lg_row.wait_for(state="visible", timeout=20000)
        await page.evaluate("document.querySelectorAll('.Toastify').forEach(e => e.style.display = 'none')")
        await page.wait_for_timeout(500)

        # Find the View Details button inside the visible desktop table row
        view_btn = lg_row.locator("button[title='View Details']").first
        if await view_btn.count() == 0:
            view_btn = lg_row.locator("td", has_text=DEMO_LG_NUMBER).first

        box6 = await measure_box_pct(view_btn)
        shot6_file = "step6_custody_register.png"
        await page.screenshot(path=os.path.join(SHOTS_DIR, shot6_file))
        steps_captured.append({"step": 6, "image": f"/demo/shots/custody_scan/{shot6_file}", "hotspot": box6})
        print(f"  -> Captured {shot6_file} | Hotspot: {box6}")

        # Step 7: Click View Details -> Capture Live LG Custody Cockpit
        print("[7/7] Opening LG Details Cockpit ...")
        await view_btn.click()
        await page.wait_for_url("**/end-user/lg-records/**", timeout=15000)
        await page.wait_for_load_state("networkidle")
        await page.evaluate("window.scrollTo(0, 0); document.querySelectorAll('.Toastify').forEach(e => e.style.display = 'none')")
        await page.wait_for_timeout(700)

        # Highlight Extend / Actions area on Details screen as final visual anchor
        extend_btn = page.locator("button:has-text('Extend')").first
        box7 = await measure_box_pct(extend_btn) if await extend_btn.count() > 0 else {"xPct": 78.0, "yPct": 18.0, "wPct": 12.0, "hPct": 5.0}
        shot7_file = "step7_lg_cockpit.png"
        await page.screenshot(path=os.path.join(SHOTS_DIR, shot7_file))
        steps_captured.append({"step": 7, "image": f"/demo/shots/custody_scan/{shot7_file}", "hotspot": box7})
        print(f"  -> Captured {shot7_file} | Hotspot: {box7}")

        await browser.close()

    # Copy staged assets into frontend/public/demo/ AFTER browser closes so webpack never reloads mid-run
    for old_doc in ("CIB_Performance_LG_8840.pdf", "CIB_Performance_LG_8840_preview.png"):
        old_p = os.path.join(DOCS_DIR, old_doc)
        if os.path.exists(old_p):
            os.remove(old_p)

    for doc_name in ("FNCB_Performance_LG_8840.pdf", "FNCB_Performance_LG_8840_preview.png"):
        src = os.path.join(SHOTS_DIR, doc_name)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(DOCS_DIR, doc_name))

    for fname in os.listdir(SHOTS_DIR):
        if fname.startswith("step") and fname.endswith(".png"):
            shutil.copy2(os.path.join(SHOTS_DIR, fname), os.path.join(FINAL_SHOTS_DIR, fname))

    with open(MANIFEST_OUT, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H},
                "document": {
                    "pdfUrl": "/demo/docs/FNCB_Performance_LG_8840.pdf",
                    "previewImageUrl": "/demo/docs/FNCB_Performance_LG_8840_preview.png",
                    "lgNumber": DEMO_LG_NUMBER,
                    "bank": "First National Corporate Bank (FNCB)",
                    "amount": "EGP 4,500,000.00",
                    "applicant": "NorthStar Electromechanical Co. SAE",
                    "beneficiary": "Apex Global Engineering SAE (HQ)",
                    "expiryDate": "01-Nov-2027",
                },
                "capturedSteps": steps_captured,
            },
            f,
            indent=2,
        )
    print(f"[SUCCESS] Wrote manifest to {MANIFEST_OUT}")


async def main():
    pdf_path, _ = generate_clean_demo_lg_pdf()
    jwt_token, _ = await prepare_db_and_mint_token()
    await run_browser_capture(pdf_path, jwt_token)


if __name__ == "__main__":
    asyncio.run(main())
