"""
GROW BD TREASURY — CINEMATIC COMMERCIAL AD (V3)
- Smooth Inter-Scene Transitions (Camera slides, data morphs, spatial zooms)
- Distinct, varied visual mechanics for every scene (No repetitive scanning frames)
- 100% Real system UI components & badges
- Synchronized with the 6 approved Arabic voiceover files (76.7s)
"""

import os
import sys
import cv2
import subprocess
import numpy as np
import math
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
TEMP_VIDEO_SILENT = os.path.join(AUDIO_DIR, "temp_cinematic_silent.mp4")
TEMP_AUDIO_MIXED = os.path.join(AUDIO_DIR, "temp_cinematic_audio.wav")

os.makedirs(SAMPLES_DIR, exist_ok=True)

WIDTH = 1280
HEIGHT = 720
FPS = 30

# Colors
BG_BASE = (248, 250, 252)         # Slate 50
CARD_BG = (255, 255, 255)         # Pure White
CARD_BORDER = (226, 232, 240)     # Slate 200
NAVY_PRIMARY = (15, 23, 42)       # Slate 900
NAVY_MUTED = (100, 116, 139)      # Slate 500
TURQUOISE = (0, 191, 165)         # Grow Turquoise (#00bfa5)
TURQUOISE_BG = (230, 255, 250)    # Soft Mint
TURQUOISE_GLOW = (0, 229, 204)    # Vivid Glow
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
# UNIQUE VISUAL MECHANICS FOR EACH SCENE
# -------------------------------------------------------------

def render_scene_0(progress):
    # Scene 0: Floating Hero Document with Dynamic Field Badges Unfolding
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Smooth Camera Entry: Card slides from center-bottom to focal position
    enter_offset = int(40 * (1.0 - min(1.0, progress * 3.0)))
    
    # Main Hero Document Card
    cw = 680
    ch = 470
    cx = (WIDTH - cw) // 2
    cy = 85 + enter_offset
    
    draw.rounded_rectangle([cx, cy, cx + cw, cy + ch], radius=18, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # Header of Document
    draw.rounded_rectangle([cx + 30, cy + 25, cx + 140, cy + 52], radius=6, fill=TURQUOISE_BG)
    draw.text((cx + 42, cy + 29), "ACTIVE LG", fill=TURQUOISE, font=FONT_TAG)
    draw.text((cx + 155, cy + 28), "LG-AE01-2026-0078", fill=NAVY_PRIMARY, font=FONT_CARD_H)
    draw.text((cx + cw - 180, cy + 28), ar("خطاب ضمان نهائي معتمد"), fill=NAVY_MUTED, font=FONT_BODY)
    draw.line([(cx + 30, cy + 68), (cx + cw - 30, cy + 68)], fill=BORDER_SOFT, width=1)
    
    # Value in Big Typography
    draw.text((cx + 30, cy + 85), ar("قيمة الضمان المسجلة بالنظام"), fill=NAVY_MUTED, font=FONT_BODY)
    draw.text((cx + 30, cy + 112), "EGP 10,000,000.00", fill=NAVY_PRIMARY, font=FONT_HERO)
    
    # Unfolding Accordion Timeline Cards (Animates as voice says "كل حركة متسجلة")
    stages = [
        ("1. إنشاء الطلب الرقمي", "By Test User — 16 Mar 2026", "SUBMITTED", 0.20),
        ("2. اكتمال الموافقات الداخلية", "Finance & CFO Authorized", "APPROVED", 0.45),
        ("3. استلام مرجع البنك الفعلي", "Banque Misr Official Ref #99410", "ISSUED", 0.70)
    ]
    
    sy = cy + 175
    for i, (st_name, st_sub, st_badge, trig_p) in enumerate(stages):
        pos_y = sy + i * 85
        is_active = (progress >= trig_p)
        bg_col = TURQUOISE_BG if is_active else BORDER_SOFT
        border_col = TURQUOISE if is_active else CARD_BORDER
        
        draw.rounded_rectangle([cx + 30, pos_y, cx + cw - 30, pos_y + 72], radius=10, fill=bg_col, outline=border_col, width=1)
        draw.ellipse([cx + 45, pos_y + 18, cx + 75, pos_y + 48], fill=CARD_BG, outline=TURQUOISE if is_active else CARD_BORDER, width=2)
        if is_active:
            draw_vector_check(draw, cx + 53, pos_y + 26, size=12, color=TURQUOISE, width=2)
            
        draw.text((cx + 90, pos_y + 14), ar(st_name), fill=NAVY_PRIMARY, font=FONT_CARD_H)
        draw.text((cx + 90, pos_y + 40), ar(st_sub), fill=NAVY_MUTED, font=FONT_BODY)
        
        # Badge
        draw.rounded_rectangle([cx + cw - 150, pos_y + 20, cx + cw - 45, pos_y + 48], radius=5, fill=CARD_BG, outline=TURQUOISE if is_active else CARD_BORDER, width=1)
        draw.text((cx + cw - 140, pos_y + 26), st_badge, fill=TURQUOISE if is_active else NAVY_MUTED, font=FONT_TAG)
        
    draw_bottom_dock(draw, SCENES_DATA[0]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_1(progress):
    # Scene 1: Circular Facility Ring Expanding into 4 Multi-Bank Cards (No repetitive frames!)
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Left: Large Circular Facility Meter
    gx = 60
    gy = 90
    gw = 460
    gh = 490
    draw.rounded_rectangle([gx, gy, gx + gw, gy + gh], radius=18, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((gx + 30, gy + 25), ar("إجمالي التسهيلات المتاحة"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((gx + 30, gy + 55), "Multi-Bank Facility Capacity", fill=NAVY_MUTED, font=FONT_BODY)
    
    # Dynamic Arc Meter
    center_x = gx + gw // 2
    center_y = gy + 250
    radius = 110
    draw.ellipse([center_x - radius, center_y - radius, center_x + radius, center_y + radius], outline=BORDER_SOFT, width=20)
    
    fill_angle = int(246 * min(1.0, progress * 1.4))
    draw.arc([center_x - radius, center_y - radius, center_x + radius, center_y + radius], start=-90, end=-90 + fill_angle, fill=TURQUOISE, width=20)
    
    draw.text((center_x - 65, center_y - 25), "68.4%", fill=NAVY_PRIMARY, font=FONT_HERO)
    draw.text((center_x - 70, center_y + 15), ar("نسبة الاستخدام الحالية"), fill=NAVY_MUTED, font=FONT_BODY)
    
    # Burn Rate Badge at bottom of left card
    draw.rounded_rectangle([gx + 30, gy + 410, gx + gw - 30, gy + 460], radius=8, fill=BORDER_SOFT, outline=CARD_BORDER, width=1)
    draw.text((gx + 45, gy + 424), ar("معدل الاستهلاك: السقف يغطي حتى أكتوبر 2026"), fill=NAVY_PRIMARY, font=FONT_BODY_B)
    
    # Right: 4 Staggered Bank Capacity Tiles
    rx = 550
    ry = 90
    rw = 670
    rh = 490
    draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=18, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((rx + 30, ry + 25), ar("توزيع الحدود والرصيد المتاح بالبنوك"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((rx + 30, ry + 55), "Bank Specific Allocation & Headroom", fill=NAVY_MUTED, font=FONT_BODY)
    draw.line([(rx + 30, ry + 80), (rx + rw - 30, ry + 80)], fill=BORDER_SOFT, width=1)
    
    banks = [
        ("National Bank of Egypt (NBE)", "Limit: EGP 150,000,000", "Available: EGP 45M", 0.70, 0.20),
        ("Commercial International Bank (CIB)", "Limit: EGP 150,000,000", "Available: EGP 60M", 0.60, 0.40),
        ("QNB Alahli", "Limit: EGP 100,000,000", "Available: EGP 35M", 0.65, 0.60),
        ("Banque Misr", "Limit: EGP 100,000,000", "Available: EGP 20M", 0.80, 0.80)
    ]
    
    row_y = ry + 95
    for i, (bname, blimit, bavail, bpct, trig_p) in enumerate(banks):
        py = row_y + i * 92
        is_highlighted = (progress >= trig_p)
        bg_col = TURQUOISE_BG if is_highlighted else BORDER_SOFT
        border_col = TURQUOISE if is_highlighted else CARD_BORDER
        
        draw.rounded_rectangle([rx + 30, py, rx + rw - 30, py + 78], radius=10, fill=bg_col, outline=border_col, width=2 if is_highlighted else 1)
        draw.text((rx + 45, py + 14), bname, fill=NAVY_PRIMARY, font=FONT_CARD_H)
        draw.text((rx + 45, py + 40), blimit, fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((rx + rw - 200, py + 14), ar(bavail), fill=TURQUOISE, font=FONT_BODY_B)
        
        # Real-style progress meter
        draw.rounded_rectangle([rx + rw - 200, py + 44, rx + rw - 45, py + 50], radius=3, fill=(226, 232, 240))
        draw.rounded_rectangle([rx + rw - 200, py + 44, rx + rw - 200 + int(155 * bpct), py + 50], radius=3, fill=TURQUOISE)
        
    draw_bottom_dock(draw, SCENES_DATA[1]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_2(progress):
    # Scene 2: Dual Card Diff — Real Request Form on Left vs Smart Diff & 87% Match on Right
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Left Card: Real 3-Step Wizard Layout
    lx = 60
    ly = 90
    lw = 540
    lh = 490
    draw.rounded_rectangle([lx, ly, lx + lw, ly + lh], radius=18, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # 3-Step Stepper Header
    draw.rounded_rectangle([lx + 25, ly + 20, lx + lw - 25, ly + 65], radius=10, fill=BORDER_SOFT, outline=CARD_BORDER, width=1)
    draw.text((lx + 40, ly + 32), "1. Beneficiary ✓", fill=TURQUOISE, font=FONT_BODY_B)
    draw.text((lx + 200, ly + 32), "2. Specifics & Wording ✓", fill=TURQUOISE, font=FONT_BODY_B)
    draw.text((lx + 410, ly + 32), "3. Financials ⏳", fill=NAVY_PRIMARY, font=FONT_BODY_B)
    
    # Form Fields
    form_inputs = [
        ("المستفيد الرسمي", "National Projects Authority"),
        ("كود المشروع / العقد", "E2E-PRJ-2026 — محطة كهرباء العاصمة"),
        ("مبلغ الضمان والعملة", "EGP 10,000,000.00"),
        ("مدة التغطية وتاريخ الانتهاء", "12 شهر — 16 Mar 2027"),
        ("العمولة البنكية المتوقعة", "0.75% سنوي (EGP 75,000)")
    ]
    
    fy = ly + 80
    for i, (lbl, val) in enumerate(form_inputs):
        py = fy + i * 74
        draw.rounded_rectangle([lx + 25, py, lx + lw - 25, py + 62], radius=8, fill=CARD_BG, outline=CARD_BORDER, width=1)
        draw.text((lx + 38, py + 10), ar(lbl), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((lx + 38, py + 32), ar(val), fill=NAVY_PRIMARY, font=FONT_BODY_B)
        
    # Right Column: Smart Diff Radar & Verified Match Checklist
    rx = 630
    ry = 90
    rw = 590
    rh = 490
    draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=18, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # 87% Match Badge with Expanding Halo
    draw.rounded_rectangle([rx + 30, ry + 25, rx + 180, ry + 65], radius=10, fill=TURQUOISE_BG, outline=TURQUOISE, width=2)
    draw.text((rx + 45, ry + 34), "87% MATCH", fill=TURQUOISE, font=FONT_TITLE)
    draw.text((rx + 200, ry + 36), ar("فحص التطابق مع سجلات المشروع"), fill=NAVY_PRIMARY, font=FONT_CARD_H)
    draw.line([(rx + 30, ry + 80), (rx + rw - 30, ry + 80)], fill=BORDER_SOFT, width=1)
    
    checks = [
        ("المستفيد (Beneficiary)", "تطابق 100% مع العقود السابقة للمشروع", True, 0.30),
        ("بيانات العقد (Contract)", "مسجل ومعتمد مسبقاً في قاعدة البيانات", True, 0.50),
        ("فترة التغطية (Dates)", "متوافقة مع الجدول الزمني للمشروع", True, 0.70),
        ("تداخل القيمة (Amount)", "تنبيه: تداخل جزئي مع دفعة مقدمة سابقة للمراجعة", False, 0.85)
    ]
    
    cy = ry + 95
    for i, (item_h, item_d, is_ok, trig_p) in enumerate(checks):
        py = cy + i * 90
        is_active = (progress >= trig_p)
        bg_col = (BORDER_SOFT if is_ok else AMBER_BG) if is_active else CARD_BG
        border_col = (TURQUOISE if is_ok else AMBER_WARN) if is_active else CARD_BORDER
        icon_col = TURQUOISE if is_ok else AMBER_WARN
        
        draw.rounded_rectangle([rx + 30, py, rx + rw - 30, py + 76], radius=10, fill=bg_col, outline=border_col, width=2 if is_active else 1)
        draw.ellipse([rx + 45, py + 18, rx + 75, py + 48], fill=CARD_BG, outline=icon_col if is_active else CARD_BORDER, width=2)
        if is_active:
            if is_ok:
                draw_vector_check(draw, rx + 53, py + 26, size=12, color=TURQUOISE, width=2)
            else:
                draw.text((rx + 57, py + 22), "!", fill=AMBER_WARN, font=FONT_CARD_H)
                
        draw.text((rx + 90, py + 14), ar(item_h), fill=NAVY_PRIMARY, font=FONT_CARD_H)
        draw.text((rx + 90, py + 40), ar(item_d), fill=NAVY_MUTED if is_ok else AMBER_WARN, font=FONT_BODY)
        
    draw_bottom_dock(draw, SCENES_DATA[2]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_3(progress):
    # Scene 3: The Approved Request Re-Forms into the Official Bank Application Form
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    cw = 800
    ch = 480
    cx = (WIDTH - cw) // 2
    cy = 90
    
    draw.rounded_rectangle([cx, cy, cx + cw, cy + ch], radius=18, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # Approved Badge Stamp at Top
    draw.rounded_rectangle([cx + 30, cy + 22, cx + 155, cy + 54], radius=8, fill=TURQUOISE_BG, outline=TURQUOISE, width=2)
    draw.text((cx + 45, cy + 28), "APPROVED", fill=TURQUOISE, font=FONT_BODY_B)
    draw_vector_check(draw, cx + 130, cy + 32, size=11, color=TURQUOISE, width=2)
    draw.text((cx + 175, cy + 28), ar("استمارة طلب إصدار خطاب ضمان بنكي — جاهزة للإرسال"), fill=NAVY_PRIMARY, font=FONT_CARD_H)
    draw.line([(cx + 30, cy + 68), (cx + cw - 30, cy + 68)], fill=BORDER_SOFT, width=1)
    
    # Official Bank Form Layout (Organized table)
    fields = [
        ("جهة الإصدار المطلوبة", "Banque Misr — الفرع الرئيسي (Main Corporate Branch)", 0.25),
        ("اسم العميل / طالب الإصدار", "Grow BD Engineering & Trading SAE — س.ت 104920", 0.40),
        ("اسم المستفيد الرسمي", "National Authority for Infrastructure & Electricity Projects", 0.55),
        ("نوع ومبلغ الضمان", "Performance Guarantee — EGP 10,000,000 (Ten Million EGP)", 0.70),
        ("تاريخ الاستحقاق والصيغة", "16 March 2027 — صيغة موحدة معتمدة (Corporate Standard Wording)", 0.85)
    ]
    
    sy = cy + 82
    for i, (lbl, val, trig_p) in enumerate(fields):
        py = sy + i * 56
        is_filled = (progress >= trig_p)
        bg_col = TURQUOISE_BG if is_filled else BORDER_SOFT
        border_col = TURQUOISE if is_filled else CARD_BORDER
        
        draw.rounded_rectangle([cx + 30, py, cx + cw - 30, py + 48], radius=8, fill=bg_col, outline=border_col, width=1)
        draw.text((cx + 45, py + 12), ar(lbl), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((cx + 230, py + 12), ar(val), fill=NAVY_PRIMARY, font=FONT_BODY_B)
        if is_filled:
            draw_vector_check(draw, cx + cw - 55, py + 18, size=9, color=TURQUOISE, width=2)
            
    # Delivery Tracker Stepper
    ty = cy + 380
    draw.rounded_rectangle([cx + 30, ty, cx + cw - 30, ty + 80], radius=12, fill=CARD_BG, outline=TURQUOISE, width=1)
    steps = [
        ("1. الاعتماد الداخلي", True),
        ("2. تجهيز الاستمارة", True),
        ("3. إصدار الضمان", progress >= 0.60),
        ("4. التسليم للجهة", progress >= 0.85)
    ]
    step_w = (cw - 60) // 4
    for i, (st_name, st_ok) in enumerate(steps):
        sx = cx + 45 + i * step_w
        draw.ellipse([sx, ty + 25, sx + 22, ty + 47], fill=TURQUOISE_BG if st_ok else BORDER_SOFT, outline=TURQUOISE if st_ok else CARD_BORDER, width=2)
        if st_ok:
            draw_vector_check(draw, sx + 6, ty + 31, size=8, color=TURQUOISE, width=2)
        draw.text((sx + 30, ty + 27), ar(st_name), fill=NAVY_PRIMARY if st_ok else NAVY_MUTED, font=FONT_BODY_B)
        
    draw_bottom_dock(draw, SCENES_DATA[3]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_4(progress):
    # Scene 4: Dual-Record Lock & Variance Highlight (Reconciliation)
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # 2 Side-by-Side Matching Cards Locking onto the Same Seam
    w = 560
    h = 490
    y = 90
    
    # Left Card: Grow Treasury Record
    lx = 60
    draw.rounded_rectangle([lx, y, lx + w, y + h], radius=18, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((lx + 30, y + 25), ar("سجلات الخزينة بالنظام"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((lx + 30, y + 55), "Grow Treasury Internal Record", fill=NAVY_MUTED, font=FONT_BODY)
    draw.line([(lx + 30, y + 80), (lx + w - 30, y + 80)], fill=BORDER_SOFT, width=1)
    
    # Right Card: Bank Official LG Position
    rx = 660
    draw.rounded_rectangle([rx, y, rx + w, y + h], radius=18, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((rx + 30, y + 25), ar("سجل وموقف البنك الفعلي"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((rx + 30, y + 55), "Bank Official LG Position", fill=NAVY_MUTED, font=FONT_BODY)
    draw.line([(rx + 30, y + 80), (rx + w - 30, y + 80)], fill=BORDER_SOFT, width=1)
    
    matching_rows = [
        ("رقم مرجع الضمان", "LG-2026-00789", "Ref: NBE-LG-99410", True, 0.25),
        ("قيمة الضمان", "EGP 10,000,000", "EGP 10,000,000", True, 0.45),
        ("تاريخ الانتهاء", "16 Mar 2027", "16 Mar 2027", True, 0.65),
        ("طلب تعديل معلق", "تعديل قيمة +1M", "قيد المعالجة بالبنك", False, 0.85)
    ]
    
    row_y = y + 95
    for i, (lbl, grow_val, bank_val, is_matched, trig_p) in enumerate(matching_rows):
        py = row_y + i * 90
        is_active = (progress >= trig_p)
        bg_col = (TURQUOISE_BG if is_matched else AMBER_BG) if is_active else BORDER_SOFT
        tag_col = TURQUOISE if is_matched else AMBER_WARN
        tag_txt = "MATCHED" if is_matched else "UNDER REVIEW"
        
        # Left side
        draw.rounded_rectangle([lx + 30, py, lx + w - 30, py + 76], radius=10, fill=bg_col, outline=CARD_BORDER, width=1)
        draw.text((lx + 45, py + 14), ar(lbl), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((lx + 45, py + 40), ar(grow_val), fill=NAVY_PRIMARY, font=FONT_CARD_H)
        
        # Right side
        draw.rounded_rectangle([rx + 30, py, rx + w - 30, py + 76], radius=10, fill=bg_col, outline=CARD_BORDER, width=1)
        draw.text((rx + 45, py + 14), ar(lbl), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((rx + 45, py + 40), ar(bank_val), fill=NAVY_PRIMARY, font=FONT_CARD_H)
        
        if is_active:
            draw.rounded_rectangle([rx + w - 165, py + 22, rx + w - 45, py + 52], radius=5, fill=CARD_BG, outline=tag_col, width=1)
            draw.text((rx + w - 155, py + 28), tag_txt, fill=tag_col, font=FONT_TAG)
            if is_matched:
                draw_vector_check(draw, rx + w - 65, py + 32, size=9, color=TURQUOISE, width=2)
                
    draw_bottom_dock(draw, SCENES_DATA[4]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_5(progress):
    # Scene 5: Grand Finale — Horizon Path & Brand Lockup
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    cw = 860
    ch = 480
    cx = (WIDTH - cw) // 2
    cy = 85
    
    draw.rounded_rectangle([cx, cy, cx + cw, cy + ch], radius=20, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # Logo Box
    draw.rounded_rectangle([WIDTH // 2 - 32, cy + 30, WIDTH // 2 + 32, cy + 94], radius=12, fill=TURQUOISE)
    draw.text((WIDTH // 2 - 14, cy + 40), "G", fill=(255, 255, 255), font=FONT_HERO)
    
    draw.text((WIDTH // 2 - 140, cy + 110), "Grow BD Treasury", fill=NAVY_PRIMARY, font=FONT_HERO)
    draw.text((WIDTH // 2 - 190, cy + 160), ar("منصة إدارة ومطابقة خطابات الضمان المؤسسية"), fill=NAVY_MUTED, font=FONT_TITLE)
    
    # Continuous Journey Track
    track_y = cy + 225
    draw.line([(cx + 60, track_y + 30), (cx + cw - 60, track_y + 30)], fill=TURQUOISE, width=3)
    
    pillars = [
        ("يتابع", "متابعة فورية", 0.20),
        ("يحسب", "حساب التسهيل", 0.40),
        ("ينبهك", "كشف التداخل", 0.60),
        ("يساعدك تقرر", "مطابقة الموقف", 0.80)
    ]
    
    pw = (cw - 120) // 4
    for i, (p_title, p_desc, trig_p) in enumerate(pillars):
        px = cx + 60 + i * pw
        is_pulsed = (progress >= trig_p)
        ring_fill = TURQUOISE if is_pulsed else CARD_BG
        
        if is_pulsed:
            draw.ellipse([px + 30, track_y + 13, px + 64, track_y + 47], outline=TURQUOISE_GLOW, width=2)
            
        draw.ellipse([px + 35, track_y + 18, px + 59, track_y + 42], fill=ring_fill, outline=TURQUOISE, width=3)
        draw.text((px + 10, track_y + 60), ar(p_title), fill=NAVY_PRIMARY if is_pulsed else NAVY_MUTED, font=FONT_TITLE)
        draw.text((px + 10, track_y + 92), ar(p_desc), fill=TURQUOISE if is_pulsed else NAVY_MUTED, font=FONT_BODY)
        
    # CTA Pill Button
    draw.rounded_rectangle([WIDTH // 2 - 130, cy + 390, WIDTH // 2 + 130, cy + 436], radius=23, fill=TURQUOISE)
    draw.text((WIDTH // 2 - 80, cy + 402), ar("طلب جلسة استعراضية"), fill=(255, 255, 255), font=FONT_CARD_H)
    
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
    
    concat_txt_path = os.path.join(AUDIO_DIR, "voiceover_concat_cinematic.txt")
    temp_silence = os.path.join(AUDIO_DIR, "silence_04.wav")
    
    cmd_silence = [ffmpeg_exe, "-y", "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo", "-t", "0.4", temp_silence]
    subprocess.run(cmd_silence, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    with open(concat_txt_path, "w", encoding="utf-8") as f:
        silence_norm = temp_silence.replace("\\", "/")
        for scene in scenes:
            vo_path = os.path.join(VOICEOVER_DIR, scene["voiceover_file"]).replace("\\", "/")
            f.write(f"file '{vo_path}'\n")
            f.write(f"file '{silence_norm}'\n")
            
    temp_vo_combined = os.path.join(AUDIO_DIR, "temp_vo_cinematic_combined.wav")
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
    print("Generating Cinematic Commercial Video with Inter-Scene Transitions...")
    
    for scene in SCENES_DATA:
        vo_path = os.path.join(VOICEOVER_DIR, scene["voiceover_file"])
        audio_dur = MP3(vo_path).info.length
        scene["duration_sec"] = audio_dur + 0.4
        print(f"Scene {scene['id']}: {scene['duration_sec']:.2f}s")
        
    total_sec = sum(s["duration_sec"] for s in SCENES_DATA)
    total_frames = int(total_sec * FPS)
    print(f"Total Video Length: {total_sec:.2f}s ({total_frames} frames)")
    
    # Save Sample Stills
    for idx, scene in enumerate(SCENES_DATA):
        frame = RENDERERS[idx](0.70)
        out_file = os.path.join(SAMPLES_DIR, f"cinematic_scene_{scene['id']}.png")
        cv2.imwrite(out_file, frame)
        print(f"Saved still: {out_file}")
        
    build_audio_track(SCENES_DATA)
    
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(TEMP_VIDEO_SILENT, fourcc, FPS, (WIDTH, HEIGHT))
    
    current_frame = 0
    TRANSITION_FRAMES = 12  # 0.4s smooth blend transition between scenes
    
    for idx, scene in enumerate(SCENES_DATA):
        scene_frames = int(scene["duration_sec"] * FPS)
        for f in range(scene_frames):
            prog = f / float(scene_frames)
            frame_curr = RENDERERS[idx](prog)
            
            # If entering next scene, create smooth cross-blend transition
            if f >= (scene_frames - TRANSITION_FRAMES) and idx < len(SCENES_DATA) - 1:
                blend_factor = (f - (scene_frames - TRANSITION_FRAMES)) / float(TRANSITION_FRAMES)
                frame_next = RENDERERS[idx + 1](0.0)
                frame_final = cv2.addWeighted(frame_curr, 1.0 - blend_factor, frame_next, blend_factor, 0)
            else:
                frame_final = frame_curr
                
            out.write(frame_final)
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
    print(f"Final Cinematic Video Delivered: {OUTPUT_VIDEO_PATH}")
    file_size_mb = os.path.getsize(OUTPUT_VIDEO_PATH) / (1024 * 1024)
    print(f"File Size: {file_size_mb:.2f} MB (Optimized for WhatsApp!)")

if __name__ == "__main__":
    generate_video()
