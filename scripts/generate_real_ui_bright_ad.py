"""
GROW BD TREASURY — REAL SYSTEM INTEGRATED BRIGHT EDITORIAL COMMERCIAL AD
Blends real system UI components, modals, and wizards from the codebase into the
approved Bright Editorial story and continuous data transformation narrative.
"""

import os
import sys
import cv2
import subprocess
import numpy as np
from PIL import Image, ImageDraw, ImageFont
import arabic_reshaper
import imageio_ffmpeg
from mutagen.mp3 import MP3

try:
    sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUDIO_DIR = os.path.join(BASE_DIR, "audio_assets")
VOICEOVER_DIR = os.path.join(AUDIO_DIR, "voiceover")
BGM_PATH = os.path.join(AUDIO_DIR, "bgm", "corporate_ambient_pad.wav")
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "interactive-experience", "assets", "screenshots")
SAMPLES_DIR = os.path.join(AUDIO_DIR, "style_samples")
OUTPUT_VIDEO_PATH = os.path.join(BASE_DIR, "LG_Issuance_Commercial_Ad_Arabic.mp4")
TEMP_VIDEO_SILENT = os.path.join(AUDIO_DIR, "temp_real_ui_silent.mp4")
TEMP_AUDIO_MIXED = os.path.join(AUDIO_DIR, "temp_real_ui_audio.wav")

os.makedirs(SAMPLES_DIR, exist_ok=True)

WIDTH = 1280
HEIGHT = 720
FPS = 30

# Colors
BG_BASE = (248, 250, 252)         # Slate 50 Warm Off-White
CARD_BG = (255, 255, 255)         # Pure White
CARD_BORDER = (226, 232, 240)     # Slate 200
NAVY_PRIMARY = (15, 23, 42)       # Slate 900
NAVY_MUTED = (100, 116, 139)      # Slate 500
TURQUOISE = (0, 191, 165)         # Grow Turquoise (#00bfa5)
TURQUOISE_BG = (230, 255, 250)    # Soft Mint
AMBER_WARN = (245, 158, 11)       # Amber 500
AMBER_BG = (254, 243, 199)        # Amber 100
BORDER_SOFT = (241, 245, 249)

def ar(text):
    if not text:
        return ""
    return arabic_reshaper.reshape(text)

def get_font(size, bold=False):
    font_names = ["arialbd.ttf" if bold else "arial.ttf", "segoeuib.ttf" if bold else "segoeui.ttf", "calibrib.ttf" if bold else "calibri.ttf"]
    for fn in font_names:
        try:
            return ImageFont.truetype(fn, size)
        except:
            continue
    return ImageFont.load_default()

FONT_BRAND = get_font(20, bold=True)
FONT_HERO = get_font(30, bold=True)
FONT_TITLE = get_font(20, bold=True)
FONT_CARD_H = get_font(15, bold=True)
FONT_BODY = get_font(13, bold=False)
FONT_BODY_B = get_font(13, bold=True)
FONT_NUM_BIG = get_font(24, bold=True)
FONT_SUBTITLE = get_font(14, bold=False)
FONT_TAG = get_font(11, bold=True)

def draw_vector_check(draw, x, y, size=10, color=TURQUOISE, width=2):
    pts = [(x, y + size * 0.5), (x + size * 0.4, y + size), (x + size, y)]
    draw.line([pts[0], pts[1]], fill=color, width=width)
    draw.line([pts[1], pts[2]], fill=color, width=width)

# Real screenshot paths
SHOT_DETAILS = os.path.join(SCREENSHOTS_DIR, "Issued_LG_Details.jpg")
SHOT_FACILITY = os.path.join(SCREENSHOTS_DIR, "Facilities.jpg")
SHOT_REQUEST = os.path.join(SCREENSHOTS_DIR, "Request_form.jpg")
SHOT_APPROVAL = os.path.join(SCREENSHOTS_DIR, "Approval Center_D.jpg")
SHOT_RECON = os.path.join(SCREENSHOTS_DIR, "Reconciliation.jpg")
SHOT_ACTION = os.path.join(SCREENSHOTS_DIR, "Action_Center.jpg")

SCENES_DATA = [
    {
        "id": 0,
        "title": "نقطة انطلاق المعاملة",
        "voiceover_file": "00 ElevenLabs_2026-08-22T14_38_26_Hanafi_pvc_sp100_s50_sb100_se43_b_m2.mp3",
        "subtitle": "كل تفصيلة محسوبة... كل حركة متسجلة... ومفيش مفاجآت في ضماناتك. Grow BD مش مجرد سيستم بيسجل... ده مساعد شاطر جنبك فاهم موقفك وبيساعدك تشتغل أذكى."
    },
    {
        "id": 1,
        "title": "التسهيلات والحدود",
        "voiceover_file": "01 ElevenLabs_2026-08-22T14_43_29_Hanafi_pvc_sp100_s50_sb100_se43_b_m2.mp3",
        "subtitle": "قبل ما تبدأ طلب إصدار... السيستم بيحسب استخدامك للـ limits والرصيد المتاح في كل بنك، ويوريك موقفك عشان تاخد قرارك وإنت شايف الصورة كاملة."
    },
    {
        "id": 2,
        "title": "مراجعة الطلب وكشف التداخل",
        "voiceover_file": "02 ElevenLabs_2026-08-22T15_08_54_Hanafi_pvc_sp100_s50_sb100_se43_b_m2.mp3",
        "subtitle": "لما يوصل لك طلب الإصدار... السيستم بيحسب مدة التغطية والعمولة المتوقعة، ويقارن تفاصيله بالطلبات الموجودة ليكشف أي تداخل وينبهك لأي حاجة محتاجة مراجعة."
    },
    {
        "id": 3,
        "title": "التحول لاستمارة البنك",
        "voiceover_file": "03 ElevenLabs_2026-08-22T14_54_23_Hanafi_pvc_sp100_s50_sb100_se43_b_m2.mp3",
        "subtitle": "وبعد ما الموافقات الداخلية تكتمل... السيستم بيجهزلك استمارة البنك بالصيغة المعتمدة، ويخليك شايف كل خطوة لحد ما الضمان يتسلم فعلياً للجهة المطلوبة."
    },
    {
        "id": 4,
        "title": "مطابقة السجلات وقفل الحقول",
        "voiceover_file": "04 ElevenLabs_2026-08-22T15_33_20_Hanafi_pvc_sp100_s50_sb100_se43_b_m2.mp3",
        "subtitle": "وبعد الإصدار... المتابعة بتكمل. السيستم بيطابق موقف وسجلات خطابات الضمان مع سجلات البنك، ويتابع التعديلات والتجديدات والاستردادات ويظهرلك أي اختلاف محتاج مراجعة."
    },
    {
        "id": 5,
        "title": "الختام — المسار المستمر",
        "voiceover_file": "05 ElevenLabs_2026-08-22T14_59_00_Hanafi_pvc_sp100_s50_sb100_se43_b_m2.mp3",
        "subtitle": "من أول طلب الإصدار... لحد آخر تعديل أو استرداد، Grow BD يفضل معاك في كل خطوة: يتابع... يحسب... ينبهك... ويساعدك تاخد القرار في وقته."
    }
]

def draw_header(draw):
    draw.rectangle([0, 0, WIDTH, 54], fill=BG_BASE)
    draw.line([(40, 54), (WIDTH - 40, 54)], fill=CARD_BORDER, width=1)
    
    draw.rounded_rectangle([40, 14, 66, 40], radius=6, fill=TURQUOISE)
    draw.text((48, 17), "G", fill=(255, 255, 255), font=FONT_TAG)
    draw.text((76, 17), "Grow BD Treasury", fill=NAVY_PRIMARY, font=FONT_BRAND)
    
    # Real breadcrumb from the system
    draw.text((WIDTH - 280, 20), ar("إدارة الضمانات البنكية — LG Module"), fill=NAVY_MUTED, font=FONT_BODY)

def draw_bottom_dock(draw, text):
    sub_w = 1040
    sub_h = 44
    x1 = (WIDTH - sub_w) // 2
    y1 = HEIGHT - 60
    
    draw.rounded_rectangle([x1, y1, x1 + sub_w, y1 + sub_h], radius=22, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.ellipse([x1 + 18, y1 + 17, x1 + 28, y1 + 27], fill=TURQUOISE)
    
    reshaped = ar(f'"{text}"')
    draw.text((x1 + 45, y1 + 13), reshaped, fill=NAVY_PRIMARY, font=FONT_SUBTITLE)

# -------------------------------------------------------------
# SCENE RENDERERS (Real UI Components + Animate the Meaning)
# -------------------------------------------------------------

def render_scene_0(progress):
    # Scene 0: Real Issued LG Details Modal Component + Expanding Field Chips
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Left Card: Real Issued LG Details View (from actual screenshot)
    lx = 60
    ly = 90
    lw = 620
    lh = 490
    draw.rounded_rectangle([lx, ly, lx + lw, ly + lh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # Modal Bar Header
    draw.rounded_rectangle([lx + 20, ly + 18, lx + 120, ly + 42], radius=6, fill=TURQUOISE_BG)
    draw.text((lx + 30, ly + 22), "ACTIVE LG", fill=TURQUOISE, font=FONT_TAG)
    draw.text((lx + 135, ly + 22), "LG-AE01-2026-0078", fill=NAVY_PRIMARY, font=FONT_CARD_H)
    
    # Real Modal UI Crop (from Issued_LG_Details.jpg)
    if os.path.exists(SHOT_DETAILS):
        raw = Image.open(SHOT_DETAILS).convert("RGB")
        # Crop the modal content area
        sw, sh = raw.size
        crop_ui = raw.crop((int(sw * 0.38), int(sh * 0.12), int(sw * 0.90), int(sh * 0.72))).resize((lw - 40, 400), Image.Resampling.LANCZOS)
        img.paste(crop_ui, (lx + 20, ly + 65))
        
    # Right Column: The Transaction Meaning Card
    rx = 710
    ry = 90
    rw = 510
    rh = 490
    draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((rx + 30, ry + 25), ar("بيانات المعاملة الموثقة"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((rx + 30, ry + 55), "Performance Guarantee Details", fill=NAVY_MUTED, font=FONT_BODY)
    draw.line([(rx + 30, ry + 80), (rx + rw - 30, ry + 80)], fill=BORDER_SOFT, width=1)
    
    # Value highlight
    draw.text((rx + 30, ry + 95), ar("قيمة الضمان"), fill=NAVY_MUTED, font=FONT_BODY)
    draw.text((rx + 30, ry + 120), "EGP 10,000,000.00", fill=NAVY_PRIMARY, font=FONT_NUM_BIG)
    
    chips = [
        ("المستفيد الرسمي", "الهيئة القومية للمشروعات"),
        ("البنك المصدر", "Banque Misr — الفرع الرئيسي"),
        ("تاريخ الاستحقاق", "16 Mar 2027 (ساري 12 شهر)"),
        ("تغطية الهامش", "100% Cash Margin Blocked")
    ]
    
    cy = ry + 180
    for i, (k, v) in enumerate(chips):
        py = cy + i * 65
        draw.rounded_rectangle([rx + 30, py, rx + rw - 30, py + 55], radius=8, fill=BORDER_SOFT, outline=CARD_BORDER, width=1)
        draw.text((rx + 45, py + 8), ar(k), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((rx + 45, py + 28), ar(v), fill=NAVY_PRIMARY, font=FONT_BODY_B)
        draw_vector_check(draw, rx + rw - 60, py + 20, size=11, color=TURQUOISE, width=2)
        
    draw_bottom_dock(draw, SCENES_DATA[0]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_1(progress):
    # Scene 1: Real Bank Facilities Page Components (Utilization Meter + Bank Lines)
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Left Card: Real Facilities Page UI (from Facilities.jpg)
    lx = 60
    ly = 90
    lw = 620
    lh = 490
    draw.rounded_rectangle([lx, ly, lx + lw, ly + lh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((lx + 30, ly + 20), ar("سجل التسهيلات البنكية الحقيقية"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((lx + 30, ly + 50), "Corporate Bank Facilities & Headroom", fill=NAVY_MUTED, font=FONT_BODY)
    
    if os.path.exists(SHOT_FACILITY):
        raw = Image.open(SHOT_FACILITY).convert("RGB")
        sw, sh = raw.size
        # Crop the facility card & utilization meter from real UI
        crop_ui = raw.crop((int(sw * 0.02), int(sh * 0.18), int(sw * 0.50), int(sh * 0.85))).resize((lw - 40, 395), Image.Resampling.LANCZOS)
        img.paste(crop_ui, (lx + 20, ly + 75))
        
    # Right Column: Meaning & Capacity Breakdown
    rx = 710
    ry = 90
    rw = 510
    rh = 490
    draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((rx + 30, ry + 25), ar("متابعة الحدود وسقف كل بنك"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((rx + 30, ry + 55), "Live Headroom & Limits Calculation", fill=NAVY_MUTED, font=FONT_BODY)
    draw.line([(rx + 30, ry + 80), (rx + rw - 30, ry + 80)], fill=BORDER_SOFT, width=1)
    
    banks = [
        ("National Bank of Egypt (NBE)", "Limit: EGP 150M", "Available: EGP 45M", 0.70),
        ("Commercial International Bank (CIB)", "Limit: EGP 150M", "Available: EGP 60M", 0.60),
        ("QNB Alahli", "Limit: EGP 100M", "Available: EGP 35M", 0.65),
        ("Banque Misr", "Limit: EGP 100M", "Available: EGP 20M", 0.80)
    ]
    
    row_y = ry + 95
    for i, (bname, blimit, bavail, bpct) in enumerate(banks):
        py = row_y + i * 90
        draw.rounded_rectangle([rx + 30, py, rx + rw - 30, py + 75], radius=10, fill=BORDER_SOFT, outline=CARD_BORDER, width=1)
        draw.text((rx + 45, py + 12), bname, fill=NAVY_PRIMARY, font=FONT_CARD_H)
        draw.text((rx + 45, py + 36), blimit, fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((rx + rw - 185, py + 12), bavail, fill=TURQUOISE, font=FONT_BODY_B)
        
        # Real-style progress meter
        draw.rounded_rectangle([rx + rw - 185, py + 42, rx + rw - 45, py + 48], radius=3, fill=(226, 232, 240))
        draw.rounded_rectangle([rx + rw - 185, py + 42, rx + rw - 185 + int(140 * bpct), py + 48], radius=3, fill=TURQUOISE)
        
    draw_bottom_dock(draw, SCENES_DATA[1]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_2(progress):
    # Scene 2: Real LG Request Form UI + 87% Match Analysis
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Left Card: Real Request Wizard Modal (from Request_form.jpg)
    lx = 60
    ly = 90
    lw = 620
    lh = 490
    draw.rounded_rectangle([lx, ly, lx + lw, ly + lh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((lx + 30, ly + 20), ar("نموذج إدخال طلب الضمان الذكي"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((lx + 30, ly + 50), "3-Step Structured Issuance Wizard", fill=NAVY_MUTED, font=FONT_BODY)
    
    if os.path.exists(SHOT_REQUEST):
        raw = Image.open(SHOT_REQUEST).convert("RGB")
        sw, sh = raw.size
        crop_ui = raw.crop((int(sw * 0.15), int(sh * 0.10), int(sw * 0.85), int(sh * 0.88))).resize((lw - 40, 395), Image.Resampling.LANCZOS)
        img.paste(crop_ui, (lx + 20, ly + 75))
        
    # Right Column: Match Analysis & Pre-Validation Checks
    rx = 710
    ry = 90
    rw = 510
    rh = 490
    draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # 87% Match Badge
    draw.rounded_rectangle([rx + 30, ry + 25, rx + 170, ry + 62], radius=8, fill=TURQUOISE_BG, outline=TURQUOISE, width=1)
    draw.text((rx + 45, ry + 32), "87% MATCH", fill=TURQUOISE, font=FONT_TITLE)
    draw.text((rx + 185, ry + 35), ar("فحص التطابق مع سجلات المشروع"), fill=NAVY_PRIMARY, font=FONT_CARD_H)
    draw.line([(rx + 30, ry + 80), (rx + rw - 30, ry + 80)], fill=BORDER_SOFT, width=1)
    
    checks = [
        ("المستفيد (Beneficiary)", "تطابق 100% مع العقود السابقة للمشروع", True),
        ("بيانات العقد (Contract)", "مسجل ومعتمد مسبقاً في قاعدة البيانات", True),
        ("فترة التغطية (Dates)", "متوافقة مع الجدول الزمني للمشروع", True),
        ("تداخل القيمة (Amount)", "تنبيه: تداخل جزئي مع دفعة مقدمة سابقة للمراجعة", False)
    ]
    
    cy = ry + 95
    for i, (item_h, item_d, is_ok) in enumerate(checks):
        py = cy + i * 90
        bg_col = BORDER_SOFT if is_ok else AMBER_BG
        border_col = CARD_BORDER if is_ok else AMBER_WARN
        icon_col = TURQUOISE if is_ok else AMBER_WARN
        
        draw.rounded_rectangle([rx + 30, py, rx + rw - 30, py + 76], radius=10, fill=bg_col, outline=border_col, width=1)
        draw.ellipse([rx + 45, py + 18, rx + 75, py + 48], fill=CARD_BG, outline=icon_col, width=2)
        if is_ok:
            draw_vector_check(draw, rx + 53, py + 26, size=12, color=TURQUOISE, width=2)
        else:
            draw.text((rx + 57, py + 22), "!", fill=AMBER_WARN, font=FONT_CARD_H)
            
        draw.text((rx + 90, py + 14), ar(item_h), fill=NAVY_PRIMARY, font=FONT_CARD_H)
        draw.text((rx + 90, py + 40), ar(item_d), fill=NAVY_MUTED if is_ok else AMBER_WARN, font=FONT_BODY)
        
    draw_bottom_dock(draw, SCENES_DATA[2]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_3(progress):
    # Scene 3: Real Approval Center + Auto-Filled Bank Form
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Left Card: Real Approval Inbox / Activity Log (from Approval Center_D.jpg)
    lx = 60
    ly = 90
    lw = 620
    lh = 490
    draw.rounded_rectangle([lx, ly, lx + lw, ly + lh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((lx + 30, ly + 20), ar("مسار الموافقات وتتبع البنك الحقيقي"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((lx + 30, ly + 50), "Multi-Tier Signing & Post-Issuance Tracking", fill=NAVY_MUTED, font=FONT_BODY)
    
    if os.path.exists(SHOT_APPROVAL):
        raw = Image.open(SHOT_APPROVAL).convert("RGB")
        sw, sh = raw.size
        crop_ui = raw.crop((int(sw * 0.40), int(sh * 0.12), int(sw * 0.90), int(sh * 0.85))).resize((lw - 40, 395), Image.Resampling.LANCZOS)
        img.paste(crop_ui, (lx + 20, ly + 75))
        
    # Right Column: The Auto-Filled Bank Application Form
    rx = 710
    ry = 90
    rw = 510
    rh = 490
    draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    draw.rounded_rectangle([rx + 30, ry + 22, rx + 145, ry + 50], radius=6, fill=TURQUOISE_BG, outline=TURQUOISE, width=1)
    draw.text((rx + 42, ry + 26), "APPROVED", fill=TURQUOISE, font=FONT_BODY_B)
    draw_vector_check(draw, rx + 120, ry + 30, size=10, color=TURQUOISE, width=2)
    draw.text((rx + 160, ry + 25), ar("استمارة البنك الرسمية — Auto-Fill"), fill=NAVY_PRIMARY, font=FONT_CARD_H)
    draw.line([(rx + 30, ry + 65), (rx + rw - 30, ry + 65)], fill=BORDER_SOFT, width=1)
    
    form_fields = [
        ("جهة الإصدار", "Banque Misr — الفرع الرئيسي"),
        ("اسم طالب الإصدار", "Grow BD Engineering SAE"),
        ("المستفيد الرسمي", "National Projects Authority"),
        ("مبلغ ونوع الضمان", "Performance LG — EGP 10,000,000"),
        ("تاريخ الاستحقاق", "16 March 2027 — صيغة موحدة معتمدة")
    ]
    
    sy = ry + 75
    for i, (lbl, val) in enumerate(form_fields):
        py = sy + i * 58
        draw.rounded_rectangle([rx + 30, py, rx + rw - 30, py + 48], radius=6, fill=BORDER_SOFT, outline=CARD_BORDER, width=1)
        draw.text((rx + 42, py + 8), ar(lbl), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((rx + 42, py + 26), ar(val), fill=NAVY_PRIMARY, font=FONT_BODY_B)
        
    # Delivery Tracker
    ty = ry + 380
    draw.rounded_rectangle([rx + 30, ty, rx + rw - 30, ty + 85], radius=10, fill=TURQUOISE_BG, outline=TURQUOISE, width=1)
    draw.text((rx + 45, ty + 12), ar("مسار التسليم الفعلي:"), fill=NAVY_PRIMARY, font=FONT_CARD_H)
    draw.text((rx + 45, ty + 38), ar("1. موافقة الإدارة ✓  ←  2. تجهيز الاستمارة ✓  ←  3. تسليم الجهة ⏳"), fill=NAVY_PRIMARY, font=FONT_BODY_B)
    
    draw_bottom_dock(draw, SCENES_DATA[3]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_4(progress):
    # Scene 4: Real Reconciliation Page + Field-Locking Matrix
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Left Card: Real Reconciliation Page UI (from Reconciliation.jpg)
    lx = 60
    ly = 90
    lw = 620
    lh = 490
    draw.rounded_rectangle([lx, ly, lx + lw, ly + lh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((lx + 30, ly + 20), ar("شاشة مطابقة موقف البنك الفعلي"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((lx + 30, ly + 50), "LG Position Reconciliation & Discrepancy Matching", fill=NAVY_MUTED, font=FONT_BODY)
    
    if os.path.exists(SHOT_RECON):
        raw = Image.open(SHOT_RECON).convert("RGB")
        sw, sh = raw.size
        crop_ui = raw.crop((int(sw * 0.46), int(sh * 0.12), int(sw * 0.98), int(sh * 0.85))).resize((lw - 40, 395), Image.Resampling.LANCZOS)
        img.paste(crop_ui, (lx + 20, ly + 75))
        
    # Right Column: Matching Rows Locked on Same Horizontal Height
    rx = 710
    ry = 90
    rw = 510
    rh = 490
    draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((rx + 30, ry + 25), ar("مطابقة موقف الخزينة وموقف البنك"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((rx + 30, ry + 55), "Internal Treasury vs Bank Official Records", fill=NAVY_MUTED, font=FONT_BODY)
    draw.line([(rx + 30, ry + 80), (rx + rw - 30, ry + 80)], fill=BORDER_SOFT, width=1)
    
    matches = [
        ("رقم مرجع الضمان", "Ref: LG-NBE-99410", True),
        ("قيمة الضمان", "EGP 10,000,000 = EGP 10,000,000", True),
        ("تاريخ الانتهاء", "16 Mar 2027 = 16 Mar 2027", True),
        ("طلب تعديل معلق", "تعديل قيمة +1M (قيد المعالجة بالبنك)", False)
    ]
    
    row_y = ry + 95
    for i, (lbl, val, is_matched) in enumerate(matches):
        py = row_y + i * 90
        bg_col = BORDER_SOFT if is_matched else AMBER_BG
        tag_col = TURQUOISE if is_matched else AMBER_WARN
        tag_txt = "MATCHED" if is_matched else "UNDER REVIEW"
        
        draw.rounded_rectangle([rx + 30, py, rx + rw - 30, py + 76], radius=10, fill=bg_col, outline=CARD_BORDER, width=1)
        draw.text((rx + 45, py + 12), ar(lbl), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((rx + 45, py + 38), ar(val), fill=NAVY_PRIMARY, font=FONT_CARD_H)
        
        draw.rounded_rectangle([rx + rw - 165, py + 22, rx + rw - 45, py + 52], radius=5, fill=CARD_BG, outline=tag_col, width=1)
        draw.text((rx + rw - 155, py + 28), tag_txt, fill=tag_col, font=FONT_TAG)
        if is_matched:
            draw_vector_check(draw, rx + rw - 65, py + 32, size=9, color=TURQUOISE, width=2)
            
    draw_bottom_dock(draw, SCENES_DATA[4]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_5(progress):
    # Scene 5: Grand Finale — The Continuous Lifecycle Path
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Grand Center Container
    cw = 860
    ch = 460
    cx = (WIDTH - cw) // 2
    cy = 95
    
    draw.rounded_rectangle([cx, cy, cx + cw, cy + ch], radius=20, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # Logo Box
    draw.rounded_rectangle([WIDTH // 2 - 32, cy + 30, WIDTH // 2 + 32, cy + 94], radius=12, fill=TURQUOISE)
    draw.text((WIDTH // 2 - 14, cy + 40), "G", fill=(255, 255, 255), font=FONT_HERO)
    
    draw.text((WIDTH // 2 - 140, cy + 110), "Grow BD Treasury", fill=NAVY_PRIMARY, font=FONT_HERO)
    draw.text((WIDTH // 2 - 190, cy + 160), ar("منصة إدارة ومطابقة خطابات الضمان المؤسسية"), fill=NAVY_MUTED, font=FONT_TITLE)
    
    # The Continuous Journey Track
    track_y = cy + 220
    draw.line([(cx + 60, track_y + 30), (cx + cw - 60, track_y + 30)], fill=TURQUOISE, width=3)
    
    pillars = [
        ("يتابع", "متابعة فورية"),
        ("يحسب", "حساب التسهيل"),
        ("ينبهك", "كشف التداخل"),
        ("يساعدك تقرر", "مطابقة الموقف")
    ]
    
    pw = (cw - 120) // 4
    for i, (p_title, p_desc) in enumerate(pillars):
        px = cx + 60 + i * pw
        draw.ellipse([px + 35, track_y + 18, px + 59, track_y + 42], fill=CARD_BG, outline=TURQUOISE, width=3)
        draw.text((px + 10, track_y + 60), ar(p_title), fill=NAVY_PRIMARY, font=FONT_TITLE)
        draw.text((px + 10, track_y + 92), ar(p_desc), fill=NAVY_MUTED, font=FONT_BODY)
        
    # CTA Pill Button
    draw.rounded_rectangle([WIDTH // 2 - 130, cy + 380, WIDTH // 2 + 130, cy + 426], radius=23, fill=TURQUOISE)
    draw.text((WIDTH // 2 - 80, cy + 392), ar("طلب جلسة استعراضية"), fill=(255, 255, 255), font=FONT_CARD_H)
    
    draw_bottom_dock(draw, SCENES_DATA[5]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

RENDERERS = [
    render_scene_0,
    render_scene_1,
    render_scene_2,
    render_scene_3,
    render_scene_4,
    render_scene_5
]

def build_audio_track(scenes):
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    print("Muxing audio tracks with FFmpeg...")
    
    concat_txt_path = os.path.join(AUDIO_DIR, "voiceover_concat_real.txt")
    temp_silence = os.path.join(AUDIO_DIR, "silence_04.wav")
    
    cmd_silence = [ffmpeg_exe, "-y", "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo", "-t", "0.4", temp_silence]
    subprocess.run(cmd_silence, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    with open(concat_txt_path, "w", encoding="utf-8") as f:
        silence_norm = temp_silence.replace("\\", "/")
        for scene in scenes:
            vo_path = os.path.join(VOICEOVER_DIR, scene["voiceover_file"]).replace("\\", "/")
            f.write(f"file '{vo_path}'\n")
            f.write(f"file '{silence_norm}'\n")
            
    temp_vo_combined = os.path.join(AUDIO_DIR, "temp_vo_real_combined.wav")
    cmd_concat = [ffmpeg_exe, "-y", "-f", "concat", "-safe", "0", "-i", concat_txt_path, "-c:a", "pcm_s16le", temp_vo_combined]
    subprocess.run(cmd_concat, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    cmd_mix = [
        ffmpeg_exe, "-y",
        "-i", temp_vo_combined,
        "-i", BGM_PATH,
        "-filter_complex", "[1:a]volume=0.14[bgm];[0:a][bgm]amix=inputs=2:duration=first[aout]",
        "-map", "[aout]",
        "-c:a", "pcm_s16le",
        TEMP_AUDIO_MIXED
    ]
    subprocess.run(cmd_mix, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def generate_video():
    print("Generating Real-UI Integrated Commercial Video...")
    
    for scene in SCENES_DATA:
        vo_path = os.path.join(VOICEOVER_DIR, scene["voiceover_file"])
        audio_dur = MP3(vo_path).info.length
        scene["duration_sec"] = audio_dur + 0.4
        print(f"Scene {scene['id']}: {scene['duration_sec']:.2f}s")
        
    total_sec = sum(s["duration_sec"] for s in SCENES_DATA)
    total_frames = int(total_sec * FPS)
    print(f"Total Video Length: {total_sec:.2f}s ({total_frames} frames)")
    
    # Save 6 Sample Stills for user review
    for idx, scene in enumerate(SCENES_DATA):
        frame = RENDERERS[idx](0.8)
        out_file = os.path.join(SAMPLES_DIR, f"real_ui_scene_{scene['id']}.png")
        cv2.imwrite(out_file, frame)
        print(f"Saved still: {out_file}")
        
    build_audio_track(SCENES_DATA)
    
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(TEMP_VIDEO_SILENT, fourcc, FPS, (WIDTH, HEIGHT))
    
    current_frame = 0
    for idx, scene in enumerate(SCENES_DATA):
        scene_frames = int(scene["duration_sec"] * FPS)
        for f in range(scene_frames):
            prog = f / float(scene_frames)
            frame = RENDERERS[idx](prog)
            out.write(frame)
            current_frame += 1
            if current_frame % 100 == 0:
                print(f"Rendering: {current_frame}/{total_frames} frames ({(current_frame*100)//total_frames}%)")
                
    out.release()
    print("Silent video rendered.")
    
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    cmd_mux = [
        ffmpeg_exe, "-y",
        "-i", TEMP_VIDEO_SILENT,
        "-i", TEMP_AUDIO_MIXED,
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        OUTPUT_VIDEO_PATH
    ]
    subprocess.run(cmd_mux, check=True)
    print(f"Final Real-UI Commercial Video Delivered: {OUTPUT_VIDEO_PATH}")
    file_size_mb = os.path.getsize(OUTPUT_VIDEO_PATH) / (1024 * 1024)
    print(f"File Size: {file_size_mb:.2f} MB (Optimized for WhatsApp!)")

if __name__ == "__main__":
    generate_video()
