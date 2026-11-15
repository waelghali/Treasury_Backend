# c:\Grow\scripts\capture_quotations_tours_3_1_to_3_3.py
"""
End-to-end live capture script for Grow Module 03 — FX & T-Bill Quotations:
  - 3.1: quotations-rfq-broadcast ("Multi-Pair FX & T-Bill RFQ with Tariff & TVM Matrix" - 5 steps)
  - 3.2: quotations-dealer-desk   ("Zero-Password Bank Dealer Desk & Blind Live Rank" - 5 steps)
  - 3.3: quotations-net-award     ("All-In Net Ranking, Cherry-Pick Award & Audit Pack" - 6 steps)

100% factual, driven against live backend (http://127.0.0.1:8000) and frontend (http://localhost:3000)
at 1440x900 @ 2x retina, using strictly fictional corporate & bank counterparties:
  - Corporate: Apex Global Engineering & Contracting SAE (Customer 23)
  - Banks: First National Corporate Bank (FNCB), Mediterranean Commercial Bank (MCB), Nile Sovereign Bank (NSB)
"""
import asyncio
import json
import os
import secrets
import shutil
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import date, datetime, timedelta, timezone

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT_DIR)

import fitz  # PyMuPDF
from playwright.sync_api import sync_playwright
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import HRFlowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from app.auth_v2.services import AuthService
from app.core.security import create_access_token
from app.core.security_crypto import encrypt_field
from app.database import SessionLocal
from app.models import models
from app.models import models_quotation as mq
from app.services.tenant_key_service import tenant_key_service

VIEWPORT_W = 1440
VIEWPORT_H = 900

STAGE_DIR = os.path.join(ROOT_DIR, "scratch", "quotations_tours_stage")
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
        pix = doc[0].get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        pix.save(png_path)
    finally:
        doc.close()


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
        textColor=colors.HexColor("#047857"),
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

    tbl_data = [[Paragraph(f"<b>{k}</b>", body_style), Paragraph(str(v), body_style)] for k, v in rows]
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
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#ecfdf5")),
                ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#6ee7b7")),
                ("TOPPADDING", (0, 0), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    story.append(note_box)
    doc.build(story)


def api_json(method: str, path: str, token: str = None, payload: dict = None):
    url = f"http://127.0.0.1:8000{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else b""
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data if method != "GET" else None, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        print(f"  [api warn] {method} {path} -> {e.code}: {e.read().decode('utf-8', errors='ignore')[:300]}")
        return {}


def ensure_fictional_banks(db):
    specs = [
        ("First National Corporate Bank", "FNCB", "FNCBEGCX"),
        ("Mediterranean Commercial Bank", "MCB", "MCBKEGCX"),
        ("Nile Sovereign Bank", "NSB", "NSBKEGCX"),
    ]
    out = {}
    for name, short, swift in specs:
        b = db.query(models.Bank).filter(models.Bank.short_name == short).first()
        if not b:
            b = models.Bank(name=name, short_name=short, swift_code=swift, is_deleted=False)
            db.add(b)
            db.flush()
        out[short] = b
    return out


def seed_quotations_demo_data() -> dict:
    db = SessionLocal()
    try:
        cust_id = 23
        banks = ensure_fictional_banks(db)
        fncb, mcb, nsb = banks["FNCB"], banks["MCB"], banks["NSB"]

        entity_hq = (
            db.query(models.CustomerEntity)
            .filter(models.CustomerEntity.customer_id == cust_id)
            .order_by(models.CustomerEntity.id.asc())
            .first()
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

        # Clean any non-fictional QuotationBank rows for Customer 23 so ONLY FNCB, MCB, NSB appear!
        fictional_bank_ids = {fncb.id, mcb.id, nsb.id}
        for old_qb in db.query(mq.QuotationBank).filter(mq.QuotationBank.customer_id == cust_id).all():
            if old_qb.bank_id not in fictional_bank_ids:
                db.delete(old_qb)
        db.flush()

        # Clean any previous demo RFQs for Customer 23 so History & Results are 100% sanitized
        for old_rfq in db.query(mq.QuotationRequest).filter(mq.QuotationRequest.customer_id == cust_id).all():
            db.delete(old_rfq)
        db.commit()

        # Ensure QuotationBank records for FNCB, MCB, NSB
        qb_specs = [
            (fncb, "fx.desk@fncb-bank.com", "Karim Mansour — Head of FX Trading"),
            (mcb, "treasury.sales@mcb-bank.com", "Nadia El-Gamal — Senior FX Dealer"),
            (nsb, "dealing.room@nsb-bank.com", "Hossam Farouk — Corporate FX Desk"),
        ]
        qbanks = {}
        for bank_obj, email_addr, dealer_name in qb_specs:
            qb = (
                db.query(mq.QuotationBank)
                .filter(mq.QuotationBank.customer_id == cust_id, mq.QuotationBank.bank_id == bank_obj.id)
                .first()
            )
            contacts_payload = [{"email": email_addr, "name": dealer_name, "role": "EXECUTION"}]
            if not qb:
                qb = mq.QuotationBank(
                    customer_id=cust_id,
                    bank_id=bank_obj.id,
                    trade_type="BOTH",
                    entity_scope="ALL_ENTITIES",
                    emails=email_addr,
                    contacts=contacts_payload,
                    authorized_contact_email=email_addr,
                    authorized_contact_name=dealer_name,
                    handshake_confirmed_at=datetime.now(timezone.utc) - timedelta(days=20),
                )
                db.add(qb)
                db.flush()
            else:
                qb.trade_type = "BOTH"
                qb.entity_scope = "ALL_ENTITIES"
                qb.emails = email_addr
                qb.contacts = contacts_payload

            # Enable Live Ranking HUD for each bank!
            lrc = (
                db.query(mq.BankLiveRankingConfig)
                .filter(mq.BankLiveRankingConfig.bank_id == bank_obj.id)
                .first()
            )
            if not lrc:
                lrc = mq.BankLiveRankingConfig(
                    bank_id=bank_obj.id,
                    trade_type="BOTH",
                    scope_type="ALL_CUSTOMERS",
                    entity_scope_type="ALL_ENTITIES",
                    is_enabled=True,
                )
                db.add(lrc)
            else:
                lrc.is_enabled = True
                lrc.trade_type = "BOTH"
            qbanks[bank_obj.short_name] = qb

        # Seed >= 4 k-anonymous cohort benchmarks + CBE MarketRateHistory so MarketSpreadTicker & 4-Pillar Audit shine!
        now_utc = datetime.now(timezone.utc)
        for pair, base_c, quote_c, spot_r, cbe_r in [
            ("USD/EGP", "USD", "EGP", 51.26, 51.22),
            ("EUR/EGP", "EUR", "EGP", 55.50, 55.45),
        ]:
            db.add(
                mq.QuotationMarketRateHistory(
                    currency_pair=pair,
                    base_currency=base_c,
                    quote_currency=quote_c,
                    rate=spot_r,
                    source="CBE_INTERBANK_FIXING",
                    cbe_official_mid=cbe_r,
                    cbe_gap_bps=7.8,
                )
            )
            for c_idx in range(4):
                db.add(
                    mq.QuotationAnonymousBenchmark(
                        tenant_cohort_hash=f"anon_cohort_{pair.replace('/', '_')}_{c_idx}",
                        currency_pair=pair,
                        trade_type="FX_SPOT",
                        deal_tier="TIER_2",
                        cbe_benchmark_rate=cbe_r,
                        winning_spread_bps=6.5 + c_idx * 1.2,
                        avg_spread_bps=14.0 + c_idx * 1.5,
                        num_participating_banks=3,
                        num_quotes_submitted=3,
                        response_duration_seconds=42 + c_idx * 8,
                        window_time_slot="WED_11_13",
                    )
                )
        db.flush()

        dek = tenant_key_service.get_or_create_tenant_dek(db, cust_id)
        val_date_str = (date.today() + timedelta(days=2)).isoformat()

        # Helper to build a 2-leg Multi-Pair FX RFQ (`Buy USD 500,000` + `Buy EUR 250,000` vs EGP)
        def create_multi_leg_rfq(
            ref_no: str,
            status: str,
            win_start: datetime,
            win_end: datetime,
            quotes_by_bank: dict,
            acceptance_status: str = None,
            admin_reviewed_at: datetime = None,
        ):
            rfq_id = str(uuid.uuid4())
            rfq = mq.QuotationRequest(
                id=rfq_id,
                ref_no=ref_no,
                customer_id=cust_id,
                created_by_user_id=maker.id,
                entity_id=entity_hq.id,
                type="FX_SPOT",
                direction="Buy",
                value_date=val_date_str,
                amount=500000.0,
                buy_currency="USD",
                sell_currency="EGP",
                eval_rate=19.75,
                window_start=win_start,
                window_end=win_end,
                quotation_base="Execution",
                allow_alternative_value_date=True,
                status=status,
                token_validity_hours=24,
                internal_notes="Turbine & Switchgear Import Settlement — Contract #NAUUA-2026-408",
                comments_to_banks="Firm execution across USD/EGP and EUR/EGP legs; alternative value date (+1d) permitted with TVM normalization.",
                is_dispatched=(status != "PENDING_APPROVAL"),
                dispatched_at=admin_reviewed_at,
                admin_reviewed_at=admin_reviewed_at,
                acceptance_timeout_seconds=600,
                acceptance_timeout_action="AUTO_REJECT",
                acceptance_deadline=(win_end + timedelta(minutes=15)) if status in ("EVALUATING", "COMPLETED") else None,
                acceptance_status=acceptance_status,
            )
            db.add(rfq)
            db.flush()

            leg_usd = mq.QuotationLeg(
                id=str(uuid.uuid4()),
                rfq_id=rfq_id,
                leg_index=1,
                type="FX_SPOT",
                direction="Buy",
                buy_currency="USD",
                sell_currency="EGP",
                amount=500000.0,
                value_date=val_date_str,
                allow_alternative_value_date=True,
                quotation_base="Execution",
                eval_rate=19.75,
                status="OPEN" if status == "OPEN" else ("EVALUATING" if status == "EVALUATING" else "PENDING"),
                entity_id=entity_hq.id,
            )
            leg_eur = mq.QuotationLeg(
                id=str(uuid.uuid4()),
                rfq_id=rfq_id,
                leg_index=2,
                type="FX_SPOT",
                direction="Buy",
                buy_currency="EUR",
                sell_currency="EGP",
                amount=250000.0,
                value_date=val_date_str,
                allow_alternative_value_date=True,
                quotation_base="Execution",
                eval_rate=19.75,
                status="OPEN" if status == "OPEN" else ("EVALUATING" if status == "EVALUATING" else "PENDING"),
                entity_id=entity_hq.id,
            )
            db.add_all([leg_usd, leg_eur])
            db.flush()

            # Tariff setup per bank (demonstrates All-In Net Adjusted Cost!)
            tariffs = {
                "FNCB": {"cost_percent": 0.05, "cost_min": 150.0, "cost_max": 2500.0, "cost_flat": 100.0},
                "MCB":  {"cost_percent": 0.08, "cost_min": 200.0, "cost_max": 3000.0, "cost_flat": 150.0},
                "NSB":  {"cost_percent": 0.12, "cost_min": 250.0, "cost_max": 3500.0, "cost_flat": 200.0},
            }

            tokens = {}
            magic_tokens = {}
            for short_code, qb in qbanks.items():
                tf = tariffs[short_code]
                tok = f"qtk_{short_code.lower()}_{secrets.token_urlsafe(16)}"
                asgn = mq.QuotationBankAssignment(
                    id=str(uuid.uuid4()),
                    rfq_id=rfq_id,
                    quotation_bank_id=qb.id,
                    token=tok,
                    cost_min=tf["cost_min"],
                    cost_percent=tf["cost_percent"],
                    cost_max=tf["cost_max"],
                    cost_flat=tf["cost_flat"],
                    quotation_base="Execution",
                    is_document_visible=True,
                    value_date=date.fromisoformat(val_date_str),
                    allow_alternative_value_date=True,
                )
                db.add(asgn)
                db.flush()

                for lg in (leg_usd, leg_eur):
                    db.add(
                        mq.QuotationBankLegConfig(
                            id=str(uuid.uuid4()),
                            assignment_id=asgn.id,
                            leg_id=lg.id,
                            is_invited=True,
                            is_passed=False,
                            cost_min=tf["cost_min"],
                            cost_percent=tf["cost_percent"],
                            cost_max=tf["cost_max"],
                            cost_flat=tf["cost_flat"],
                            quotation_base="Execution",
                            is_document_visible=True,
                            value_date=date.fromisoformat(val_date_str),
                            allow_alternative_value_date=True,
                        )
                    )

                m_tok = f"mgk_{short_code.lower()}_{secrets.token_urlsafe(18)}"
                db.add(
                    mq.QuotationAccessOTP(
                        assignment_id=asgn.id,
                        email=qb.emails,
                        role="EXECUTION",
                        otp_code="482910",
                        magic_token=m_tok,
                        expires_at=now_utc + timedelta(hours=6),
                        is_used=False,
                    )
                )
                tokens[short_code] = tok
                magic_tokens[short_code] = m_tok

                if short_code in quotes_by_bank:
                    q_usd, q_eur = quotes_by_bank[short_code]
                    if q_usd is not None:
                        db.add(
                            mq.QuotationOffer(
                                assignment_id=asgn.id,
                                leg_id=leg_usd.id,
                                price=None,
                                encrypted_price=encrypt_field(float(q_usd), dek, field_context="price"),
                                offered_value_date=date.fromisoformat(val_date_str),
                                notes="Firm interbank spot liquidity — immediate settlement ready",
                                submitted_by_email=qb.emails,
                                submitted_at=win_start + timedelta(seconds=35),
                            )
                        )
                    if q_eur is not None:
                        db.add(
                            mq.QuotationOffer(
                                assignment_id=asgn.id,
                                leg_id=leg_eur.id,
                                price=None,
                                encrypted_price=encrypt_field(float(q_eur), dek, field_context="price"),
                                offered_value_date=date.fromisoformat(val_date_str),
                                notes="Firm EUR/EGP corporate desk rate",
                                submitted_by_email=qb.emails,
                                submitted_at=win_start + timedelta(seconds=42),
                            )
                        )
            db.flush()
            return {
                "rfq_id": rfq_id,
                "ref_no": ref_no,
                "leg_usd_id": leg_usd.id,
                "leg_eur_id": leg_eur.id,
                "tokens": tokens,
                "magic_tokens": magic_tokens,
            }

        # RFQ A: PENDING_APPROVAL (for Tour 3.1 Step 4 Pre-Broadcast Admin Approval!)
        rfq_pending = create_multi_leg_rfq(
            ref_no="RFQ-FX-2026-0901",
            status="PENDING_APPROVAL",
            win_start=now_utc + timedelta(minutes=5),
            win_end=now_utc + timedelta(minutes=25),
            quotes_by_bank={},
        )

        # RFQ B: LIVE OPEN Bidding Window (for Tour 3.1 Step 5 Blind Floor + Tour 3.2 Dealer Desk!)
        # Pre-seed MCB & NSB quotes so when FNCB quotes 51.34 on USD/EGP it ranks #2 of 3, then sharpens to 51.24 -> #1!
        rfq_live = create_multi_leg_rfq(
            ref_no="RFQ-FX-2026-0902",
            status="OPEN",
            win_start=now_utc - timedelta(minutes=2),
            win_end=now_utc + timedelta(minutes=18),
            quotes_by_bank={
                "MCB": (51.2800, 55.4800),
                "NSB": (51.3900, 55.6600),
            },
            admin_reviewed_at=now_utc - timedelta(minutes=3),
        )

        # RFQ C: CLOSED WINDOW Awaiting Cherry-Pick Award (for Tour 3.3!)
        # FNCB wins Leg #1 USD/EGP @ 51.2400; MCB wins Leg #2 EUR/EGP @ 55.4800!
        rfq_closed = create_multi_leg_rfq(
            ref_no="RFQ-FX-2026-0903",
            status="EVALUATING",
            win_start=now_utc - timedelta(minutes=16),
            win_end=now_utc - timedelta(minutes=1),
            quotes_by_bank={
                "FNCB": (51.2400, 55.5900),
                "MCB":  (51.2900, 55.4800),
                "NSB":  (51.3800, 55.6700),
            },
            acceptance_status="PENDING",
            admin_reviewed_at=now_utc - timedelta(minutes=18),
        )

        # Also seed 1 completed Sovereign T-Bill RFQ in History so Step 3.3.6 History Dashboard looks executive!
        tb_id = str(uuid.uuid4())
        tb_rfq = mq.QuotationRequest(
            id=tb_id,
            ref_no="RFQ-TB-2026-0894",
            customer_id=cust_id,
            created_by_user_id=maker.id,
            entity_id=entity_hq.id,
            type="TBILL",
            direction="Buy",
            amount=25000000.0,
            settlement_date_start=(date.today() - timedelta(days=5)).isoformat(),
            settlement_date_end=(date.today() - timedelta(days=5)).isoformat(),
            maturity_date_start=(date.today() + timedelta(days=86)).isoformat(),
            maturity_date_end=(date.today() + timedelta(days=177)).isoformat(),
            eval_rate=26.50,
            window_start=now_utc - timedelta(days=5, minutes=30),
            window_end=now_utc - timedelta(days=5, minutes=10),
            quotation_base="Execution",
            status="COMPLETED",
            acceptance_timeout_seconds=120,
            acceptance_timeout_action="AUTO_REJECT",
            acceptance_status="ACCEPTED",
            acceptance_resolved_at=now_utc - timedelta(days=5, minutes=5),
            acceptance_resolved_by_user_id=maker.id,
            admin_reviewed_at=now_utc - timedelta(days=5, minutes=32),
            is_dispatched=True,
            winner_bank_id=fncb.id,
            winner_bank_name=fncb.name,
            encrypted_winner_rate=encrypt_field("27.45", dek, field_context="winner_rate"),
            encrypted_saved_vs_avg=encrypt_field("48250.00", dek, field_context="saved_vs_avg"),
        )
        db.add(tb_rfq)
        db.commit()

        # Generate Companion Sample PDFs for Tours 3.1, 3.2, 3.3
        os.makedirs(STAGE_DOCS, exist_ok=True)
        doc_3_1 = os.path.join(STAGE_DOCS, "Apex_FX_TBill_RFQ_Mandate_2026.pdf")
        build_companion_pdf(
            doc_3_1,
            badge="APEX GLOBAL ENGINEERING SAE · GROUP TREASURY MANDATE",
            title="MULTI-CURRENCY FX SPOT & SOVEREIGN T-BILL RFQ SHEET",
            subtitle="Pre-Broadcast Governance & All-In Bank Tariff Schedule (#RFQ-FX-2026-0902)",
            rows=[
                ("Requesting Entity", "Apex Global Engineering SAE (HQ)"),
                ("Leg #1 Specification", "BUY USD 500,000.00 against EGP · Firm Execution"),
                ("Leg #2 Specification", "BUY EUR 250,000.00 against EGP · Firm Execution"),
                ("Target Settlement", f"{val_date_str} (Alternative Value Date Permitted with 19.75% TVM Normalization)"),
                ("Invited Bank Panel", "First National Corporate Bank (FNCB), Mediterranean Commercial Bank (MCB), Nile Sovereign Bank (NSB)"),
                ("Tariff Overlay Formula", "All-In Net Rate = Quoted Spot + Commission % (Clamped by Min/Max) + Flat SWIFT/Transfer Fee"),
                ("Pre-Broadcast Gate", "Corporate Admin Release Authorization & Mandatory Liability Acknowledgment"),
            ],
            note=(
                "<b>Blind Bidding Integrity:</b> Counterparty pricing and bank identities remain cryptographically "
                "sealed in AES-256-GCM storage until the live bidding countdown reaches 00:00."
            ),
        )
        render_pdf_first_page_png(doc_3_1, doc_3_1.replace(".pdf", "_preview.png"))

        doc_3_2 = os.path.join(STAGE_DOCS, "FNCB_Dealer_RFQ_Invitation_Notice_2026.pdf")
        build_companion_pdf(
            doc_3_2,
            badge="GROW INSTITUTIONAL TRADING DESK · COUNTERPARTY DISPATCH",
            title="ZERO-PASSWORD BANK DEALER RFQ INVITATION (#RFQ-FX-2026-0902)",
            subtitle="Dispatched to Authorized Execution Desk: First National Corporate Bank (fx.desk@fncb-bank.com)",
            rows=[
                ("Corporate Client", "Apex Global Engineering & Contracting SAE"),
                ("Assigned Counterparty", "First National Corporate Bank (FNCB) — Corporate FX Desk"),
                ("Authentication Gate", "Domain-Locked Email + 6-Digit One-Time Passcode (Zero Static Passwords)"),
                ("Desk Concurrency", "45-Second Heartbeat Single-Trader Active Desk Lock + Spectator Mode"),
                ("Live Competitive Signal", "Blind Live Rank HUD Enabled (Displays Rank # Only — Zero Competitor Price Leakage)"),
                ("Input Safeguards", "Real-Time CBE Fixing Triangulation, 10x Decimal Shift & Cross-Leg Swap Guardrails"),
            ],
            note=(
                "<b>Frictionless Institutional Participation:</b> Bank FX and Money Market dealers quote directly "
                "from their browser in seconds without IT onboarding or static password management."
            ),
        )
        render_pdf_first_page_png(doc_3_2, doc_3_2.replace(".pdf", "_preview.png"))

        doc_3_3 = os.path.join(STAGE_DOCS, "Apex_Best_Execution_Audit_Certificate_2026.pdf")
        build_companion_pdf(
            doc_3_3,
            badge="CERTIFIED BEST EXECUTION AUDIT PACK · REGULATORY & EXTERNAL AUDIT",
            title="MULTI-LEG CHERRY-PICK AWARD & RATE TRIANGULATION (#RFQ-FX-2026-0903)",
            subtitle="Executed by Corporate Treasury · Cryptographic HMAC-SHA256 Counterparty Receipts Issued",
            rows=[
                ("Leg #1 Award (USD/EGP)", "First National Corporate Bank (FNCB) · USD 500,000 @ 51.2400 (Rank #1 All-In Net Cost)"),
                ("Leg #2 Award (EUR/EGP)", "Mediterranean Commercial Bank (MCB) · EUR 250,000 @ 55.4800 (Rank #1 All-In Net Cost)"),
                ("CBE Official Fixing", "USD/EGP Mid: 51.2200 · EUR/EGP Mid: 55.4500 (Frozen at Execution Timestamp)"),
                ("All-In Savings vs Avg", "EGP 43,750.00 Combined Net Treasury Savings Across Awarded Currency Pairs"),
                ("Counterparty Isolation", "Bank-Scoped HMAC-SHA256 Execution Receipts (Each Winner Sees Strictly Its Won Leg)"),
                ("Governance Chain", "Maker: treasury@apexglobal.com · Released by: admin@apexglobal.com"),
            ],
            note=(
                "<b>External Audit Proof:</b> Every competitive quote, timestamp, tariff adjustment, CBE fixing benchmark, "
                "and per-leg award decision is permanently sealed for internal and statutory auditors."
            ),
        )
        render_pdf_first_page_png(doc_3_3, doc_3_3.replace(".pdf", "_preview.png"))

        auth_service = AuthService()
        maker_td = asyncio.run(auth_service.build_user_token_data(db, maker))
        maker_td.update({"must_change_password": False, "must_accept_policies": False, "last_accepted_legal_version": 99.0, "is_mfa_verified": True, "has_quotation_module": True})
        maker_jwt = create_access_token(maker_td, expires_delta=timedelta(hours=4))

        admin_td = asyncio.run(auth_service.build_user_token_data(db, admin))
        admin_td.update({"must_change_password": False, "must_accept_policies": False, "last_accepted_legal_version": 99.0, "is_mfa_verified": True, "has_quotation_module": True})
        admin_jwt = create_access_token(admin_td, expires_delta=timedelta(hours=4))

        return {
            "cust_id": cust_id,
            "fncb_id": fncb.id,
            "mcb_id": mcb.id,
            "nsb_id": nsb.id,
            "val_date_str": val_date_str,
            "maker_jwt": maker_jwt,
            "admin_jwt": admin_jwt,
            "rfq_pending": rfq_pending,
            "rfq_live": rfq_live,
            "rfq_closed": rfq_closed,
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
            localStorage.setItem('has_quotation_module', 'true');
        }""",
        {"token": token, "role": role, "customerId": customer_id, "now": now_ms},
    )


def hide_overlays(page):
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
    return fallback or {"xPct": 24.0, "yPct": 22.0, "wPct": 52.0, "hPct": 20.0}


def capture_all_quotations_tours(ctx: dict) -> dict:
    tours_manifest = {}
    for d in ["quotations_3_1", "quotations_3_2", "quotations_3_3"]:
        os.makedirs(os.path.join(STAGE_SHOTS, d), exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        context = browser.new_context(
            viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
            device_scale_factor=2,
        )
        page = context.new_page()

        # ======================================================================
        # TOUR 3.1: Multi-Pair FX & T-Bill RFQ with Tariff & TVM Matrix (5 steps)
        # ======================================================================
        print("=== Capturing Tour 3.1: quotations-rfq-broadcast ===")
        steps_3_1 = []

        set_browser_auth(page, ctx["maker_jwt"], "end_user")
        page.goto("http://localhost:3000/end-user/quotations/active", wait_until="domcontentloaded")
        try:
            page.locator("text=First National Corporate Bank").first.wait_for(state="visible", timeout=15000)
        except Exception:
            time.sleep(4.5)
        time.sleep(0.8)
        hide_overlays(page)

        # Dismiss top mobile alerts promo banner if visible so the workspace header is super clean
        dismiss_alert = page.locator("button:has-text('Dismiss')").first
        if dismiss_alert.count() and dismiss_alert.is_visible():
            try:
                dismiss_alert.click(force=True)
                time.sleep(0.3)
            except Exception:
                pass

        # Fill Pair #1 Amount = 500000 and add Pair #2 (EUR/EGP 250000)
        amt_inputs = page.locator("input[placeholder='0.00'], input[placeholder*='Amount' i], input[type='number']")
        if amt_inputs.count():
            try:
                amt_inputs.first.fill("500000")
            except Exception:
                pass
        add_pair_btn = page.locator("button:has-text('Add Pair'), button:has-text('Add Currency Pair')").first
        if add_pair_btn.count() and add_pair_btn.is_visible():
            add_pair_btn.click(force=True)
            time.sleep(0.7)
            selects = page.locator("select")
            for idx_s in range(selects.count()):
                s_el = selects.nth(idx_s)
                try:
                    txt = s_el.inner_text()
                    if "EUR" in txt and s_el.input_value() == "USD":
                        s_el.select_option("EUR")
                        break
                except Exception:
                    pass
            time.sleep(0.3)
            amt_eur = page.locator("input[placeholder='0.00']").first
            if amt_eur.count() and amt_eur.is_visible():
                try:
                    amt_eur.fill("250000")
                except Exception:
                    pass
        time.sleep(0.6)
        hide_overlays(page)

        # Step 1: Multi-Pair FX Spot RFQ Builder + Live Market Spread Benchmark
        shot = os.path.join(STAGE_SHOTS, "quotations_3_1", "step_01_multi_pair_fx_builder.png")
        page.screenshot(path=shot)
        hs_31_1 = measure_box_pct(
            page,
            page.locator("button:has-text('Add Pair'), button:has-text('Add Currency Pair'), div:has-text('USD/EGP')").first,
            pad=8,
            fallback={"xPct": 21.0, "yPct": 24.0, "wPct": 74.0, "hPct": 34.0},
        )
        steps_3_1.append({
            "step": 1,
            "key": "multi_pair_fx_builder",
            "url": "app.growbusinessdevelopment.com/end-user/quotations/active",
            "image": "/demo/shots/quotations_3_1/step_01_multi_pair_fx_builder.png",
            "hotspot": hs_31_1,
        })

        # Step 2: Select All 3 Fictional Banks + Enable Alternative Value Date (TVM) & Tariff Fee Matrix
        sel_all_btn = page.locator("button:has-text('Select All')").first
        if sel_all_btn.count() and sel_all_btn.is_visible():
            sel_all_btn.scroll_into_view_if_needed()
            sel_all_btn.click(force=True)
            time.sleep(0.8)
        else:
            for bname in ["First National Corporate Bank", "Mediterranean Commercial Bank", "Nile Sovereign Bank"]:
                b_el = page.locator(f"div:has-text('{bname}')").last
                if b_el.count():
                    try:
                        b_el.click(force=True)
                    except Exception:
                        pass
        # Scroll to selected banks / tariff panel
        fncb_card = page.locator("text=First National Corporate Bank").first
        if fncb_card.count():
            fncb_card.scroll_into_view_if_needed()
            time.sleep(0.6)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_1", "step_02_tariff_and_tvm_matrix.png")
        page.screenshot(path=shot)
        hs_31_2 = measure_box_pct(
            page,
            page.locator("div:has-text('First National Corporate Bank'):has-text('Execution')").last,
            pad=8,
            fallback={"xPct": 21.0, "yPct": 30.0, "wPct": 74.0, "hPct": 42.0},
        )
        steps_3_1.append({
            "step": 2,
            "key": "tariff_and_tvm_matrix",
            "url": "app.growbusinessdevelopment.com/end-user/quotations/active#bank-tariffs-tvm",
            "image": "/demo/shots/quotations_3_1/step_02_tariff_and_tvm_matrix.png",
            "hotspot": hs_31_2,
        })

        # Step 3: Scroll top & switch Trade Type to Sovereign Treasury Bills (T-Bills) Mode!
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.4)
        tbill_btn = page.locator("button:has-text('T-Bill'), button:has-text('Treasury Bills')").first
        if tbill_btn.count() and tbill_btn.is_visible():
            tbill_btn.click(force=True)
            time.sleep(0.9)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_1", "step_03_tbill_auction_mode.png")
        page.screenshot(path=shot)
        hs_31_3 = measure_box_pct(
            page,
            page.locator("button:has-text('T-Bill'), button:has-text('Treasury Bills')").first,
            pad=8,
            fallback={"xPct": 22.0, "yPct": 16.0, "wPct": 32.0, "hPct": 8.0},
        )
        steps_3_1.append({
            "step": 3,
            "key": "tbill_auction_mode",
            "url": "app.growbusinessdevelopment.com/end-user/quotations/active#tbill-mode",
            "image": "/demo/shots/quotations_3_1/step_03_tbill_auction_mode.png",
            "hotspot": hs_31_3,
        })

        # Step 4: Corporate Admin Pre-Broadcast Approval & Mandatory Liability Acknowledgment (`rfq_pending`)
        set_browser_auth(page, ctx["admin_jwt"], "corporate_admin")
        page.goto(
            f"http://localhost:3000/corporate-admin/quotations/results/{ctx['rfq_pending']['rfq_id']}",
            wait_until="domcontentloaded",
        )
        try:
            page.locator("text=Retrieving quotation standings").wait_for(state="hidden", timeout=15000)
        except Exception:
            pass
        time.sleep(1.5)
        hide_overlays(page)
        open_app_btn = page.locator("button:has-text('Approve & Release RFQ')").first
        if open_app_btn.count() and open_app_btn.is_visible():
            open_app_btn.click(force=True)
            time.sleep(0.7)
        legal_cb = page.locator("#inlineAdminLegalConfirmed")
        if legal_cb.count() and not legal_cb.is_checked():
            legal_cb.check(force=True)
            time.sleep(0.4)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_1", "step_04_pre_broadcast_approval.png")
        page.screenshot(path=shot)
        hs_31_4 = measure_box_pct(
            page,
            page.locator("button:has-text('Confirm & Release to Banks'), button:has-text('Approve & Release')").first,
            pad=8,
            fallback={"xPct": 65.0, "yPct": 68.0, "wPct": 26.0, "hPct": 8.0},
        )
        steps_3_1.append({
            "step": 4,
            "key": "pre_broadcast_approval",
            "url": "app.growbusinessdevelopment.com/corporate-admin/quotations/results#release-approval",
            "image": "/demo/shots/quotations_3_1/step_04_pre_broadcast_approval.png",
            "hotspot": hs_31_4,
        })

        # Step 5: Live Blind Trading Floor Telemetry (`rfq_live` OPEN window!)
        set_browser_auth(page, ctx["maker_jwt"], "end_user")
        page.goto(
            f"http://localhost:3000/end-user/quotations/results/{ctx['rfq_live']['rfq_id']}",
            wait_until="domcontentloaded",
        )
        try:
            page.locator("text=Retrieving quotation standings").wait_for(state="hidden", timeout=15000)
        except Exception:
            pass
        time.sleep(1.5)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_1", "step_05_blind_live_floor.png")
        page.screenshot(path=shot)
        hs_31_5 = measure_box_pct(
            page,
            page.locator("div:has-text('Live Trading Floor Telemetry'):has-text('Blind Bidding Protected')").last,
            pad=6,
            fallback={"xPct": 20.0, "yPct": 22.0, "wPct": 75.0, "hPct": 14.0},
        )
        steps_3_1.append({
            "step": 5,
            "key": "blind_live_floor",
            "url": "app.growbusinessdevelopment.com/end-user/quotations/results#live-telemetry",
            "image": "/demo/shots/quotations_3_1/step_05_blind_live_floor.png",
            "hotspot": hs_31_5,
        })

        tours_manifest["quotations-rfq-broadcast"] = {
            "branchId": "quotations-rfq-broadcast",
            "moduleId": "quotations",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "Multi-Currency FX & T-Bill Mandate",
                "titleAr": "مذكرة طرح عروض العملات وأذون الخزانة",
                "pdfUrl": "/demo/docs/Apex_FX_TBill_RFQ_Mandate_2026.pdf",
                "previewPngUrl": "/demo/docs/Apex_FX_TBill_RFQ_Mandate_2026_preview.png",
            },
            "steps": steps_3_1,
        }

        # ======================================================================
        # TOUR 3.2: Zero-Password Bank Dealer Desk & Blind Live Rank (5 steps)
        # ======================================================================
        print("=== Capturing Tour 3.2: quotations-dealer-desk ===")
        steps_3_2 = []

        dealer_ctx = browser.new_context(
            viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
            device_scale_factor=2,
        )
        dpage = dealer_ctx.new_page()
        fncb_tok = ctx["rfq_live"]["tokens"]["FNCB"]
        fncb_mgk = ctx["rfq_live"]["magic_tokens"]["FNCB"]

        # Step 1: Domain-Locked Zero-Password Email + OTP Gate (`/public-quotation/:token`)
        dpage.goto(f"http://localhost:3000/public-quotation/{fncb_tok}", wait_until="domcontentloaded")
        time.sleep(1.8)
        hide_overlays(dpage)
        email_in = dpage.locator("input[type='email'], input[placeholder*='email' i]").first
        if email_in.count() and email_in.is_visible():
            email_in.fill("fx.desk@fncb-bank.com")
            time.sleep(0.3)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_2", "step_01_dealer_zero_password_gate.png")
        dpage.screenshot(path=shot)
        hs_32_1 = measure_box_pct(
            dpage,
            dpage.locator("button:has-text('Send'), button:has-text('Request'), button:has-text('Verify'), form").first,
            pad=8,
            fallback={"xPct": 32.0, "yPct": 36.0, "wPct": 36.0, "hPct": 36.0},
        )
        steps_3_2.append({
            "step": 1,
            "key": "dealer_zero_password_gate",
            "url": "app.growbusinessdevelopment.com/public-quotation/fncb-desk#otp-gate",
            "image": "/demo/shots/quotations_3_2/step_01_dealer_zero_password_gate.png",
            "hotspot": hs_32_1,
        })

        # Step 2: Authenticated Multi-Leg Dealer Quoting Console (via Magic Token auto-verify!)
        dpage.goto(
            f"http://localhost:3000/public-quotation/{fncb_tok}?magic_token={fncb_mgk}&email=fx.desk@fncb-bank.com",
            wait_until="domcontentloaded",
        )
        time.sleep(2.4)
        hide_overlays(dpage)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_2", "step_02_authenticated_dealer_console.png")
        dpage.screenshot(path=shot)
        hs_32_2 = measure_box_pct(
            dpage,
            dpage.locator("input[type='number'], input[inputmode='decimal']").first,
            pad=10,
            fallback={"xPct": 16.0, "yPct": 36.0, "wPct": 68.0, "hPct": 38.0},
        )
        steps_3_2.append({
            "step": 2,
            "key": "authenticated_dealer_console",
            "url": "app.growbusinessdevelopment.com/public-quotation/fncb-desk#live-console",
            "image": "/demo/shots/quotations_3_2/step_02_authenticated_dealer_console.png",
            "hotspot": hs_32_2,
        })

        # Step 3: Trigger Fat-Finger / 10x Displaced Decimal Guardrail (`512.50` on USD/EGP + `55.62` on EUR/EGP)
        rate_inputs = dpage.locator("input[type='number'], input[inputmode='decimal'], input[placeholder*='0.0000'], input[placeholder*='rate' i]")
        if rate_inputs.count() >= 2:
            rate_inputs.nth(0).fill("512.50")
            rate_inputs.nth(1).fill("55.6200")
            time.sleep(0.4)
            sub_btn = dpage.locator("button[type='submit'], button:has-text('Submit')").first
            if sub_btn.count():
                sub_btn.scroll_into_view_if_needed()
                sub_btn.click(force=True)
                time.sleep(1.0)
        hide_overlays(dpage)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_2", "step_03_fat_finger_guardrail.png")
        dpage.screenshot(path=shot)
        hs_32_3 = measure_box_pct(
            dpage,
            dpage.locator("div:has-text('Displaced Decimal'), button:has-text('51.2500')").first,
            pad=8,
            fallback={"xPct": 28.0, "yPct": 24.0, "wPct": 44.0, "hPct": 48.0},
        )
        steps_3_2.append({
            "step": 3,
            "key": "fat_finger_guardrail",
            "url": "app.growbusinessdevelopment.com/public-quotation/fncb-desk#rate-guardrail",
            "image": "/demo/shots/quotations_3_2/step_03_fat_finger_guardrail.png",
            "hotspot": hs_32_3,
        })

        # Dismiss Fat-Finger modal (click Cancel / Edit or Escape) and submit initial quote (51.3400 -> Rank #2 of 3!)
        cancel_ff = dpage.locator("button:has-text('Edit'), button:has-text('Cancel'), button:has-text('Apply Suggested')").first
        if cancel_ff.count() and cancel_ff.is_visible():
            cancel_ff.click(force=True)
            time.sleep(0.5)
        if rate_inputs.count() >= 2:
            rate_inputs.nth(0).fill("51.3400")
            rate_inputs.nth(1).fill("55.6200")
            time.sleep(0.3)
            sub_btn = dpage.locator("button[type='submit'], button:has-text('Submit')").first
            if sub_btn.count():
                sub_btn.click(force=True)
                time.sleep(2.0)

        # Step 4: Blind Live Competitive Rank HUD (`#2 of 3` on USD/EGP!)
        dpage.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.5)
        hide_overlays(dpage)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_2", "step_04_blind_live_rank_hud.png")
        dpage.screenshot(path=shot)
        hs_32_4 = measure_box_pct(
            dpage,
            dpage.locator("span:has-text('#2'), div:has-text('Rank #2'), div:has-text('#2 of 3')").first,
            pad=10,
            fallback={"xPct": 58.0, "yPct": 34.0, "wPct": 26.0, "hPct": 14.0},
        )
        steps_3_2.append({
            "step": 4,
            "key": "blind_live_rank_hud",
            "url": "app.growbusinessdevelopment.com/public-quotation/fncb-desk#blind-rank-2",
            "image": "/demo/shots/quotations_3_2/step_04_blind_live_rank_hud.png",
            "hotspot": hs_32_4,
        })

        # Step 5: Dealer Sharpens USD/EGP Quote to `51.2400` -> Captures `#1` Leading Quote!
        rate_inputs = dpage.locator("input[type='number'], input[inputmode='decimal']")
        if rate_inputs.count() >= 2:
            rate_inputs.nth(0).fill("51.2400")
            rate_inputs.nth(1).fill("55.5900")
            time.sleep(0.3)
            upd_btn = dpage.locator("button[type='submit'], button:has-text('Update'), button:has-text('Submit')").first
            if upd_btn.count():
                upd_btn.scroll_into_view_if_needed()
                upd_btn.click(force=True)
                time.sleep(2.0)
        dpage.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.5)
        hide_overlays(dpage)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_2", "step_05_sharpen_to_rank_one.png")
        dpage.screenshot(path=shot)
        hs_32_5 = measure_box_pct(
            dpage,
            dpage.locator("span:has-text('#1'), div:has-text('Rank #1'), div:has-text('Leading')").first,
            pad=10,
            fallback={"xPct": 58.0, "yPct": 34.0, "wPct": 26.0, "hPct": 14.0},
        )
        steps_3_2.append({
            "step": 5,
            "key": "sharpen_to_rank_one",
            "url": "app.growbusinessdevelopment.com/public-quotation/fncb-desk#leading-quote",
            "image": "/demo/shots/quotations_3_2/step_05_sharpen_to_rank_one.png",
            "hotspot": hs_32_5,
        })
        dealer_ctx.close()

        tours_manifest["quotations-dealer-desk"] = {
            "branchId": "quotations-dealer-desk",
            "moduleId": "quotations",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "Zero-Password Dealer RFQ Invitation",
                "titleAr": "دعوة غرفة التداول البنكية بدون كلمة مرور",
                "pdfUrl": "/demo/docs/FNCB_Dealer_RFQ_Invitation_Notice_2026.pdf",
                "previewPngUrl": "/demo/docs/FNCB_Dealer_RFQ_Invitation_Notice_2026_preview.png",
            },
            "steps": steps_3_2,
        }

        # ======================================================================
        # TOUR 3.3: All-In Net Ranking, Cherry-Pick Award & Audit Pack (6 steps)
        # ======================================================================
        print("=== Capturing Tour 3.3: quotations-net-award ===")
        steps_3_3 = []

        closed_id = ctx["rfq_closed"]["rfq_id"]
        set_browser_auth(page, ctx["maker_jwt"], "end_user")
        page.goto(f"http://localhost:3000/end-user/quotations/results/{closed_id}", wait_until="domcontentloaded")
        try:
            page.locator("text=Retrieving quotation standings").wait_for(state="hidden", timeout=15000)
        except Exception:
            pass
        time.sleep(2.0)
        hide_overlays(page)

        # Note: if global Deal Acceptance Modal auto-opened, capture Step 2 modal or minimize first for Step 1!
        minimize_modal = page.locator("button[title*='Minimize' i], div.fixed button:has(svg.lucide-minimize-2)").first
        if minimize_modal.count() and minimize_modal.is_visible():
            try:
                minimize_modal.click(force=True)
                time.sleep(0.6)
            except Exception:
                pass

        # Step 1: Provisional Best Execution Standings (Window Closed, All-In Net Cost Unsealed!)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_3", "step_01_provisional_net_standings.png")
        page.screenshot(path=shot)
        hs_33_1 = measure_box_pct(
            page,
            page.locator("div:has-text('Provisional Best Execution Standings'), #acceptanceDecisionPanel").first,
            pad=6,
            fallback={"xPct": 20.0, "yPct": 16.0, "wPct": 75.0, "hPct": 28.0},
        )
        steps_3_3.append({
            "step": 1,
            "key": "provisional_net_standings",
            "url": "app.growbusinessdevelopment.com/end-user/quotations/results#net-standings",
            "image": "/demo/shots/quotations_3_3/step_01_provisional_net_standings.png",
            "hotspot": hs_33_1,
        })

        # Step 2: Multi-Leg Cherry-Picking Decision Panel (`USD/EGP -> FNCB`, `EUR/EGP -> MCB`)
        page.goto(f"http://localhost:3000/end-user/quotations/results/{closed_id}", wait_until="domcontentloaded")
        try:
            page.locator("text=Retrieving quotation standings").wait_for(state="hidden", timeout=15000)
        except Exception:
            pass
        time.sleep(2.0)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_3", "step_02_multi_leg_cherry_pick.png")
        page.screenshot(path=shot)
        hs_33_2 = measure_box_pct(
            page,
            page.locator("button:has-text('Accept & Execute Trade'), button:has-text('Accept All Legs & Execute')").first,
            pad=8,
            fallback={"xPct": 74.0, "yPct": 74.0, "wPct": 19.0, "hPct": 7.0},
        )
        steps_3_3.append({
            "step": 2,
            "key": "multi_leg_cherry_pick",
            "url": "app.growbusinessdevelopment.com/end-user/quotations/results#cherry-pick-award",
            "image": "/demo/shots/quotations_3_3/step_02_multi_leg_cherry_pick.png",
            "hotspot": hs_33_2,
        })

        # Execute Real Multi-Leg Award via Backend API (`USD -> FNCB`, `EUR -> MCB`)!
        api_json(
            "POST",
            f"/api/v1/end-user/quotations/{closed_id}/accept-deal",
            ctx["maker_jwt"],
            {
                "accepted_leg_ids": [ctx["rfq_closed"]["leg_usd_id"], ctx["rfq_closed"]["leg_eur_id"]],
                "declined_leg_ids": [],
            },
        )
        page.goto(f"http://localhost:3000/end-user/quotations/results/{closed_id}", wait_until="domcontentloaded")
        try:
            page.locator("text=Retrieving quotation standings").wait_for(state="hidden", timeout=15000)
        except Exception:
            pass
        time.sleep(2.0)
        hide_overlays(page)

        # Step 3: Multi-Leg Bidding Ladder + 4-Pillar Market Reference & CBE Fixing Triangulation
        ref_chk = page.locator("div:has-text('Awarded to First National Corporate Bank')").first
        if ref_chk.count():
            ref_chk.scroll_into_view_if_needed()
            time.sleep(0.6)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_3", "step_03_rate_reference_audit.png")
        page.screenshot(path=shot)
        hs_33_3 = measure_box_pct(
            page,
            page.locator("div:has-text('Reference Check:'), div:has-text('Awarded to First National Corporate Bank')").first,
            pad=8,
            fallback={"xPct": 22.0, "yPct": 24.0, "wPct": 72.0, "hPct": 22.0},
        )
        steps_3_3.append({
            "step": 3,
            "key": "rate_reference_audit",
            "url": "app.growbusinessdevelopment.com/end-user/quotations/results#cbe-rate-triangulation",
            "image": "/demo/shots/quotations_3_3/step_03_rate_reference_audit.png",
            "hotspot": hs_33_3,
        })

        # Step 4: Open Printable A4 `Best Execution Audit Pack` Certificate Modal!
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.5)
        audit_btn = page.locator("button:has-text('Best Execution Audit Pack')").first
        if audit_btn.count():
            audit_btn.click(force=True)
            time.sleep(1.2)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_3", "step_04_best_execution_audit_pack.png")
        page.screenshot(path=shot)
        hs_33_4 = measure_box_pct(
            page,
            page.locator("#audit-certificate-card"),
            pad=4,
            fallback={"xPct": 18.0, "yPct": 6.0, "wPct": 64.0, "hPct": 88.0},
        )
        steps_3_3.append({
            "step": 4,
            "key": "best_execution_audit_pack",
            "url": "app.growbusinessdevelopment.com/end-user/quotations/results#audit-certificate",
            "image": "/demo/shots/quotations_3_3/step_04_best_execution_audit_pack.png",
            "hotspot": hs_33_4,
        })

        # Step 5: Bank-Scoped HMAC-SHA256 Cryptographic Deal Execution Receipt on FNCB's Dealer Link!
        receipt_ctx = browser.new_context(
            viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
            device_scale_factor=2,
        )
        rpage = receipt_ctx.new_page()
        c_tok = ctx["rfq_closed"]["tokens"]["FNCB"]
        c_mgk = ctx["rfq_closed"]["magic_tokens"]["FNCB"]
        rpage.goto(
            f"http://localhost:3000/public-quotation/{c_tok}?magic_token={c_mgk}&email=fx.desk@fncb-bank.com",
            wait_until="domcontentloaded",
        )
        time.sleep(2.4)
        hide_overlays(rpage)
        rcpt_el = rpage.locator("div:has-text('Cryptographic Deal Execution Receipt')").last
        if rcpt_el.count():
            rcpt_el.scroll_into_view_if_needed()
            time.sleep(0.5)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_3", "step_05_scoped_hmac_deal_receipt.png")
        rpage.screenshot(path=shot)
        hs_33_5 = measure_box_pct(
            rpage,
            rpage.locator("div:has-text('Cryptographic Deal Execution Receipt'):has-text('HMAC-SHA256')").last,
            pad=8,
            fallback={"xPct": 24.0, "yPct": 38.0, "wPct": 52.0, "hPct": 34.0},
        )
        steps_3_3.append({
            "step": 5,
            "key": "scoped_hmac_deal_receipt",
            "url": "app.growbusinessdevelopment.com/public-quotation/fncb-desk#hmac-execution-receipt",
            "image": "/demo/shots/quotations_3_3/step_05_scoped_hmac_deal_receipt.png",
            "hotspot": hs_33_5,
        })
        receipt_ctx.close()

        # Step 6: Corporate Quotation History & Treasury Value Analytics (`/end-user/quotations/history`)
        page.goto("http://localhost:3000/end-user/quotations/history", wait_until="domcontentloaded")
        time.sleep(2.2)
        hide_overlays(page)
        shot = os.path.join(STAGE_SHOTS, "quotations_3_3", "step_06_quotation_history_analytics.png")
        page.screenshot(path=shot)
        hs_33_6 = measure_box_pct(
            page,
            page.locator("tr:has-text('RFQ-FX-2026-0903'), div:has-text('RFQ-FX-2026-0903')").first,
            pad=8,
            fallback={"xPct": 20.0, "yPct": 32.0, "wPct": 75.0, "hPct": 16.0},
        )
        steps_3_3.append({
            "step": 6,
            "key": "quotation_history_analytics",
            "url": "app.growbusinessdevelopment.com/end-user/quotations/history",
            "image": "/demo/shots/quotations_3_3/step_06_quotation_history_analytics.png",
            "hotspot": hs_33_6,
        })

        tours_manifest["quotations-net-award"] = {
            "branchId": "quotations-net-award",
            "moduleId": "quotations",
            "GeneratedAt": datetime.now(timezone.utc).isoformat(),
            "viewport": {"width": VIEWPORT_W, "height": VIEWPORT_H, "deviceScaleFactor": 2},
            "sampleDocument": {
                "titleEn": "Best Execution Audit Certificate",
                "titleAr": "شهادة التدقيق لأفضل تنفيذ سعري",
                "pdfUrl": "/demo/docs/Apex_Best_Execution_Audit_Certificate_2026.pdf",
                "previewPngUrl": "/demo/docs/Apex_Best_Execution_Audit_Certificate_2026_preview.png",
            },
            "steps": steps_3_3,
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
    print(f"[DONE] Published {len(tours_manifest)} Quotations tours into {MANIFEST_PATH}")


if __name__ == "__main__":
    os.makedirs(STAGE_DIR, exist_ok=True)
    ctx_q = seed_quotations_demo_data()
    tours_q = capture_all_quotations_tours(ctx_q)
    publish_assets(tours_q)
