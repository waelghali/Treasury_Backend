"""
GROW BD TREASURY — HIGH-END COMMERCIAL ADVERTISEMENT VIDEO GENERATOR
Theme: Bright Premium Editorial ("Animate the Meaning")
Synchronized 100% with the 6 approved Arabic voiceover files (74.3 seconds).
"""

import os
import sys
import cv2
import subprocess
import numpy as np
import math
from PIL import Image, ImageDraw, ImageFont
import arabic_reshaper
from bidi.algorithm import get_display
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
SAMPLES_DIR = os.path.join(AUDIO_DIR, "style_samples")
OUTPUT_VIDEO_PATH = os.path.join(BASE_DIR, "LG_Issuance_Commercial_Ad_Arabic.mp4")
TEMP_VIDEO_SILENT = os.path.join(AUDIO_DIR, "temp_bright_silent.mp4")
TEMP_AUDIO_MIXED = os.path.join(AUDIO_DIR, "temp_bright_audio.wav")

os.makedirs(SAMPLES_DIR, exist_ok=True)

WIDTH = 1280
HEIGHT = 720
FPS = 30

# Bright Premium Editorial Palette
BG_BASE = (248, 250, 252)         # Warm Off-White / Slate 50
CARD_BG = (255, 255, 255)         # Pure White Card
CARD_BORDER = (226, 232, 240)     # Hairline Slate 200
NAVY_PRIMARY = (15, 23, 42)       # Deep Slate 900
NAVY_SECONDARY = (51, 65, 85)     # Slate 700
NAVY_MUTED = (100, 116, 139)      # Slate 500
TURQUOISE = (0, 191, 165)         # Grow Turquoise (#00bfa5)
TURQUOISE_BG = (230, 255, 250)    # Soft Mint Tint
AMBER_WARN = (245, 158, 11)       # Subtle Amber (#f59e0b)
AMBER_BG = (254, 243, 199)        # Soft Amber Tint
BORDER_SOFT = (241, 245, 249)

# Helper for Arabic text shaping
def ar(text):
    if not text:
        return ""
    return arabic_reshaper.reshape(text)

# Fonts
def get_font(size, bold=False):
    font_names = ["arialbd.ttf" if bold else "arial.ttf", "segoeuib.ttf" if bold else "segoeui.ttf", "calibrib.ttf" if bold else "calibri.ttf"]
    for fn in font_names:
        try:
            return ImageFont.truetype(fn, size)
        except:
            continue
    return ImageFont.load_default()

FONT_BRAND = get_font(20, bold=True)
FONT_HERO = get_font(32, bold=True)
FONT_TITLE = get_font(22, bold=True)
FONT_CARD_H = get_font(16, bold=True)
FONT_BODY = get_font(14, bold=False)
FONT_BODY_B = get_font(14, bold=True)
FONT_NUM_BIG = get_font(26, bold=True)
FONT_SUBTITLE = get_font(15, bold=False)
FONT_TAG = get_font(12, bold=True)

def draw_vector_check(draw, x, y, size=10, color=TURQUOISE, width=2):
    # Clean vector checkmark
    pts = [(x, y + size * 0.5), (x + size * 0.4, y + size), (x + size, y)]
    draw.line([pts[0], pts[1]], fill=color, width=width)
    draw.line([pts[1], pts[2]], fill=color, width=width)

# 6 Storyboard Scenes (Mapped to 6 voiceovers)
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
    # Top Brand Header (Bright Editorial)
    draw.rectangle([0, 0, WIDTH, 54], fill=BG_BASE)
    draw.line([(40, 54), (WIDTH - 40, 54)], fill=CARD_BORDER, width=1)
    
    # Logo mark
    draw.rounded_rectangle([40, 14, 66, 40], radius=6, fill=TURQUOISE)
    draw.text((48, 17), "G", fill=(255, 255, 255), font=FONT_TAG)
    draw.text((76, 17), "Grow BD Treasury", fill=NAVY_PRIMARY, font=FONT_BRAND)
    
    # Clean section pill
    draw.text((WIDTH - 240, 20), ar("إدارة الضمانات البنكية"), fill=NAVY_MUTED, font=FONT_BODY)

def draw_bottom_dock(draw, text):
    sub_w = 1040
    sub_h = 46
    x1 = (WIDTH - sub_w) // 2
    y1 = HEIGHT - 64
    
    # Soft white pill with clean drop shadow
    draw.rounded_rectangle([x1, y1, x1 + sub_w, y1 + sub_h], radius=23, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.ellipse([x1 + 18, y1 + 18, x1 + 28, y1 + 28], fill=TURQUOISE)
    
    reshaped = ar(f'"{text}"')
    draw.text((x1 + 45, y1 + 14), reshaped, fill=NAVY_PRIMARY, font=FONT_SUBTITLE)

# -------------------------------------------------------------
# SCENE RENDERERS (Animate the Meaning)
# -------------------------------------------------------------

def render_scene_0(progress):
    # Scene 0: Single LG Document as Visual Protagonist
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Document Card in Center
    cw = 520
    ch = 320
    cx = (WIDTH - cw) // 2
    cy = 150
    
    # Base Card
    draw.rounded_rectangle([cx, cy, cx + cw, cy + ch], radius=14, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # Card Header
    draw.rounded_rectangle([cx + 20, cy + 20, cx + 120, cy + 44], radius=6, fill=TURQUOISE_BG)
    draw.text((cx + 32, cy + 24), "LG-2026-00789", fill=TURQUOISE, font=FONT_TAG)
    draw.text((cx + 140, cy + 24), ar("خطاب ضمان نهائي — Performance LG"), fill=NAVY_PRIMARY, font=FONT_CARD_H)
    draw.line([(cx + 20, cy + 54), (cx + cw - 20, cy + 54)], fill=BORDER_SOFT, width=1)
    
    # Main Value
    draw.text((cx + 20, cy + 70), ar("قيمة الضمان"), fill=NAVY_MUTED, font=FONT_BODY)
    draw.text((cx + 20, cy + 95), "EGP 10,000,000", fill=NAVY_PRIMARY, font=FONT_NUM_BIG)
    
    # Expanding Data Chips (Animates with progress)
    chips = [
        ("المستفيد", "الهيئة القومية للمشروعات (National Projects)"),
        ("البنك المصدر", "البنك الأهلي المصري (NBE)"),
        ("تاريخ الانتهاء", "16 Mar 2027 (ساري 12 شهر)"),
        ("تغطية الهامش", "100% Cash Cover Blocked")
    ]
    
    chip_y = cy + 145
    for i, (k, v) in enumerate(chips):
        show_prog = min(1.0, max(0.0, (progress - i * 0.15) / 0.25))
        if show_prog > 0.1:
            cy_pos = chip_y + i * 38
            draw.rounded_rectangle([cx + 20, cy_pos, cx + cw - 20, cy_pos + 32], radius=6, fill=BORDER_SOFT, outline=CARD_BORDER, width=1)
            draw.text((cx + 32, cy_pos + 7), ar(k), fill=NAVY_MUTED, font=FONT_BODY)
            draw.text((cx + 140, cy_pos + 7), ar(v), fill=NAVY_PRIMARY, font=FONT_BODY_B)
            
    # Subtitle
    draw_bottom_dock(draw, SCENES_DATA[0]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_1(progress):
    # Scene 1: Facility Limits (Circular Gauge & Bank Allocation)
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Left: Circular Gauge Card
    gx = 80
    gy = 110
    gw = 480
    gh = 480
    draw.rounded_rectangle([gx, gy, gx + gw, gy + gh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((gx + 30, gy + 30), ar("إجمالي التسهيلات المتاحة"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((gx + 30, gy + 65), "EGP 500,000,000 Total Limit", fill=NAVY_MUTED, font=FONT_BODY)
    
    # Circular Gauge Arc
    center_x = gx + gw // 2
    center_y = gy + 260
    radius = 120
    draw.ellipse([center_x - radius, center_y - radius, center_x + radius, center_y + radius], outline=BORDER_SOFT, width=18)
    
    # Animated utilized arc
    fill_angle = int(240 * min(1.0, progress * 1.3))
    # Simulated arc via pie slice outline
    draw.arc([center_x - radius, center_y - radius, center_x + radius, center_y + radius], start=-90, end=-90 + fill_angle, fill=TURQUOISE, width=18)
    
    draw.text((center_x - 65, center_y - 25), "68%", fill=NAVY_PRIMARY, font=FONT_HERO)
    draw.text((center_x - 70, center_y + 15), ar("نسبة الاستخدام الحالية"), fill=NAVY_MUTED, font=FONT_BODY)
    
    # Right: 4 Bank Capacity Rows
    bx = 600
    by = 110
    bw = 600
    bh = 480
    draw.rounded_rectangle([bx, by, bx + bw, by + bh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((bx + 30, by + 30), ar("توزيع الحدود والرصيد المتاح بالبنوك"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((bx + 30, by + 65), ar("رؤية فورية لسقف كل بنك قبل إصدار أي طلب"), fill=NAVY_MUTED, font=FONT_BODY)
    
    banks = [
        ("Bank A (NBE)", "EGP 150M", "Available: EGP 45M", 0.70),
        ("Bank B (CIB)", "EGP 150M", "Available: EGP 60M", 0.60),
        ("Bank C (QNB)", "EGP 100M", "Available: EGP 35M", 0.65),
        ("Bank D (Banque Misr)", "EGP 100M", "Available: EGP 20M", 0.80),
    ]
    
    row_y = by + 110
    for i, (bname, bcap, bavail, bpct) in enumerate(banks):
        ry = row_y + i * 85
        draw.rounded_rectangle([bx + 30, ry, bx + bw - 30, ry + 72], radius=10, fill=BORDER_SOFT, outline=CARD_BORDER, width=1)
        draw.text((bx + 45, ry + 12), bname, fill=NAVY_PRIMARY, font=FONT_CARD_H)
        draw.text((bx + 45, ry + 38), ar(bcap), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((bx + bw - 210, ry + 12), ar(bavail), fill=TURQUOISE, font=FONT_BODY_B)
        
        # Progress line
        draw.rounded_rectangle([bx + bw - 210, ry + 42, bx + bw - 50, ry + 48], radius=3, fill=(226, 232, 240))
        draw.rounded_rectangle([bx + bw - 210, ry + 42, bx + bw - 210 + int(160 * bpct), ry + 48], radius=3, fill=TURQUOISE)
        
    draw_bottom_dock(draw, SCENES_DATA[1]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_2(progress):
    # Scene 2: Smart Intake & Flags (87% Match & Relationship Breakdown)
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Left: New Incoming Request Card
    rx = 80
    ry = 110
    rw = 480
    rh = 480
    draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.rounded_rectangle([rx + 30, ry + 25, rx + 130, ry + 48], radius=6, fill=TURQUOISE_BG)
    draw.text((rx + 42, ry + 28), "NEW REQUEST", fill=TURQUOISE, font=FONT_TAG)
    draw.text((rx + 30, ry + 60), ar("طلب إصدار خطاب ضمان جديد"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((rx + 30, ry + 95), ar("عقد توريد محطة كهرباء العاصمة"), fill=NAVY_MUTED, font=FONT_BODY)
    
    fields = [
        ("قيمة الطلب", "EGP 10,000,000"),
        ("المستفيد", "National Projects Authority"),
        ("فترة التغطية", "12 شهر (إلى مارس 2027)"),
        ("العمولة المقدرة", "0.75% سنوي (EGP 75,000)")
    ]
    fy = ry + 140
    for i, (k, v) in enumerate(fields):
        pos_y = fy + i * 75
        draw.rounded_rectangle([rx + 30, pos_y, rx + rw - 30, pos_y + 60], radius=8, fill=BORDER_SOFT, outline=CARD_BORDER, width=1)
        draw.text((rx + 45, pos_y + 8), ar(k), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((rx + 45, pos_y + 30), ar(v), fill=NAVY_PRIMARY, font=FONT_BODY_B)
        
    # Right: Match & Scan Card
    mx = 600
    my = 110
    mw = 600
    mh = 480
    draw.rounded_rectangle([mx, my, mx + mw, my + mh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # 87% Match Pill
    draw.rounded_rectangle([mx + 30, my + 30, mx + 160, my + 68], radius=8, fill=TURQUOISE_BG, outline=TURQUOISE, width=1)
    draw.text((mx + 45, my + 38), "87% MATCH", fill=TURQUOISE, font=FONT_TITLE)
    draw.text((mx + 180, my + 40), ar("فحص التطابق مع سجلات المشروع القائمة"), fill=NAVY_PRIMARY, font=FONT_CARD_H)
    
    # Breakdown Items
    checks = [
        ("المستفيد (Beneficiary)", "تطابق 100% مع العقود السابقة للمشروع", True),
        ("بيانات العقد (Contract)", "مسجل ومعتمد مسبقاً في قاعدة البيانات", True),
        ("فترة التغطية (Dates)", "متوافقة مع الجدول الزمني للمشروع", True),
        ("تداخل القيمة (Amount)", "تنبيه: تداخل جزئي مع دفعة مقدمة سابقة للمراجعة", False)
    ]
    
    cy = my + 95
    for i, (item_h, item_d, is_ok) in enumerate(checks):
        pos_y = cy + i * 85
        bg_col = BORDER_SOFT if is_ok else AMBER_BG
        border_col = CARD_BORDER if is_ok else AMBER_WARN
        icon_col = TURQUOISE if is_ok else AMBER_WARN
        icon_text = "✓" if is_ok else "!"
        
        draw.rounded_rectangle([mx + 30, pos_y, mx + mw - 30, pos_y + 72], radius=10, fill=bg_col, outline=border_col, width=1)
        draw.ellipse([mx + 45, pos_y + 16, mx + 75, pos_y + 46], fill=CARD_BG, outline=icon_col, width=2)
        if is_ok:
            draw_vector_check(draw, mx + 53, pos_y + 24, size=12, color=TURQUOISE, width=2)
        else:
            draw.text((mx + 57, pos_y + 20), "!", fill=AMBER_WARN, font=FONT_CARD_H)
        
        draw.text((mx + 90, pos_y + 12), ar(item_h), fill=NAVY_PRIMARY, font=FONT_CARD_H)
        draw.text((mx + 90, pos_y + 38), ar(item_d), fill=NAVY_MUTED if is_ok else AMBER_WARN, font=FONT_BODY)
        
    draw_bottom_dock(draw, SCENES_DATA[2]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_3(progress):
    # Scene 3: Approvals to Official Bank Form Transformation
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Center Card: The Official Bank Application Form
    fw = 760
    fh = 470
    fx = (WIDTH - fw) // 2
    fy = 110
    
    draw.rounded_rectangle([fx, fy, fx + fw, fy + fh], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # Approved Stamp Header
    draw.rounded_rectangle([fx + 30, fy + 24, fx + 160, fy + 52], radius=6, fill=TURQUOISE_BG, outline=TURQUOISE, width=1)
    draw.text((fx + 45, fy + 28), "APPROVED", fill=TURQUOISE, font=FONT_BODY_B)
    draw_vector_check(draw, fx + 132, fy + 32, size=11, color=TURQUOISE, width=2)
    draw.text((fx + 180, fy + 26), ar("استمارة طلب إصدار خطاب ضمان بنكي — جاهزة للإرسال"), fill=NAVY_PRIMARY, font=FONT_CARD_H)
    draw.line([(fx + 30, fy + 65), (fx + fw - 30, fy + 65)], fill=BORDER_SOFT, width=1)
    
    # Official Bank Form Fields (Organized layout)
    form_sections = [
        ("جهة الإصدار المطلوبة", "البنك الأهلي المصري (NBE) — الفرع الرئيسي"),
        ("اسم العميل / طالب الإصدار", "Grow BD Engineering & Trading SAE"),
        ("اسم المستفيد الرسمي", "National Authority for Infrastructure Projects"),
        ("نوع ومبلغ الضمان", "Performance Guarantee — EGP 10,000,000"),
        ("تاريخ الاستحقاق والصيغة", "16 March 2027 — صيغة موحدة معتمدة (Standard Wording)")
    ]
    
    sy = fy + 80
    for i, (lbl, val) in enumerate(form_sections):
        pos_y = sy + i * 54
        draw.rounded_rectangle([fx + 30, pos_y, fx + fw - 30, pos_y + 44], radius=6, fill=BORDER_SOFT, outline=CARD_BORDER, width=1)
        draw.text((fx + 45, pos_y + 11), ar(lbl), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((fx + 220, pos_y + 11), ar(val), fill=NAVY_PRIMARY, font=FONT_BODY_B)
        
    # Delivery Tracker Timeline
    ty = fy + 375
    draw.rounded_rectangle([fx + 30, ty, fx + fw - 30, ty + 70], radius=10, fill=TURQUOISE_BG, outline=TURQUOISE, width=1)
    steps = ["1. الاعتماد الداخلي", "2. تجهيز استمارة البنك", "3. إصدار الضمان", "4. التسليم للجهة"]
    step_w = (fw - 60) // 4
    for i, st in enumerate(steps):
        sx = fx + 40 + i * step_w
        draw.text((sx, ty + 25), ar(st), fill=NAVY_PRIMARY, font=FONT_BODY_B)
        
    draw_bottom_dock(draw, SCENES_DATA[3]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_4(progress):
    # Scene 4: Reconciliation Field Locking (Grow Records vs Bank Position)
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # 2 Side-by-Side Matching Cards
    w = 540
    h = 470
    y = 110
    
    # Left Card: Grow Treasury Record
    lx = 80
    draw.rounded_rectangle([lx, y, lx + w, y + h], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((lx + 30, y + 25), ar("سجلات الخزينة بالنظام"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((lx + 30, y + 55), "Grow Treasury Internal Record", fill=NAVY_MUTED, font=FONT_BODY)
    draw.line([(lx + 30, y + 80), (lx + w - 30, y + 80)], fill=BORDER_SOFT, width=1)
    
    # Right Card: Bank LG Position
    rx = 660
    draw.rounded_rectangle([rx, y, rx + w, y + h], radius=16, fill=CARD_BG, outline=CARD_BORDER, width=1)
    draw.text((rx + 30, y + 25), ar("سجل وموقف البنك الفعلي"), fill=NAVY_PRIMARY, font=FONT_TITLE)
    draw.text((rx + 30, y + 55), "Bank Official LG Position", fill=NAVY_MUTED, font=FONT_BODY)
    draw.line([(rx + 30, y + 80), (rx + w - 30, y + 80)], fill=BORDER_SOFT, width=1)
    
    # Locked Matching Rows across both sides
    matching_rows = [
        ("رقم مرجع الضمان", "LG-2026-00789", "Ref: NBE-LG-99410", True),
        ("قيمة الضمان", "EGP 10,000,000", "EGP 10,000,000", True),
        ("تاريخ السريان", "16 Mar 2027", "16 Mar 2027", True),
        ("طلب تعديل معلق", "تعديل قيمة +EGP 1M", "قيد المعالجة بالبنك", False)
    ]
    
    row_y = y + 105
    for i, (lbl, grow_val, bank_val, is_matched) in enumerate(matching_rows):
        pos_y = row_y + i * 85
        bg_col = BORDER_SOFT if is_matched else AMBER_BG
        tag_col = TURQUOISE if is_matched else AMBER_WARN
        tag_txt = "MATCHED" if is_matched else "UNDER REVIEW"
        
        # Left side row
        draw.rounded_rectangle([lx + 30, pos_y, lx + w - 30, pos_y + 70], radius=8, fill=bg_col, outline=CARD_BORDER, width=1)
        draw.text((lx + 45, pos_y + 12), ar(lbl), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((lx + 45, pos_y + 36), ar(grow_val), fill=NAVY_PRIMARY, font=FONT_CARD_H)
        
        # Right side row (Locked to exact horizontal height)
        draw.rounded_rectangle([rx + 30, pos_y, rx + w - 30, pos_y + 70], radius=8, fill=bg_col, outline=CARD_BORDER, width=1)
        draw.text((rx + 45, pos_y + 12), ar(lbl), fill=NAVY_MUTED, font=FONT_BODY)
        draw.text((rx + 45, pos_y + 36), ar(bank_val), fill=NAVY_PRIMARY, font=FONT_CARD_H)
        draw.rounded_rectangle([rx + w - 170, pos_y + 20, rx + w - 45, pos_y + 50], radius=5, fill=CARD_BG, outline=tag_col, width=1)
        draw.text((rx + w - 158, pos_y + 26), tag_txt, fill=tag_col, font=FONT_TAG)
        if is_matched:
            draw_vector_check(draw, rx + w - 68, pos_y + 30, size=9, color=TURQUOISE, width=2)
        
    draw_bottom_dock(draw, SCENES_DATA[4]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_5(progress):
    # Scene 5: Grand Finale — The Continuous Path
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_BASE)
    draw = ImageDraw.Draw(img)
    draw_header(draw)
    
    # Grand Center Container
    cw = 860
    ch = 460
    cx = (WIDTH - cw) // 2
    cy = 110
    
    draw.rounded_rectangle([cx, cy, cx + cw, cy + ch], radius=20, fill=CARD_BG, outline=CARD_BORDER, width=1)
    
    # Brand Lockup
    draw.rounded_rectangle([WIDTH // 2 - 30, cy + 40, WIDTH // 2 + 30, cy + 100], radius=12, fill=TURQUOISE)
    draw.text((WIDTH // 2 - 12, cy + 50), "G", fill=(255, 255, 255), font=FONT_HERO)
    
    draw.text((WIDTH // 2 - 140, cy + 120), "Grow BD Treasury", fill=NAVY_PRIMARY, font=FONT_HERO)
    draw.text((WIDTH // 2 - 190, cy + 175), ar("منصة إدارة ومطابقة خطابات الضمان المؤسسية"), fill=NAVY_MUTED, font=FONT_TITLE)
    
    # 4 Core Values on a single continuous track
    track_y = cy + 240
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
        draw.text((px + 10, track_y + 95), ar(p_desc), fill=NAVY_MUTED, font=FONT_BODY)
        
    # Bottom CTA Pill
    draw.rounded_rectangle([WIDTH // 2 - 130, cy + 390, WIDTH // 2 + 130, cy + 434], radius=22, fill=TURQUOISE)
    draw.text((WIDTH // 2 - 80, cy + 400), ar("طلب جلسة استعراضية"), fill=(255, 255, 255), font=FONT_CARD_H)
    
    draw_bottom_dock(draw, SCENES_DATA[5]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

# Map scene functions
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
    print("Muxing 6 Arabic ElevenLabs audio tracks with FFmpeg...")
    
    concat_txt_path = os.path.join(AUDIO_DIR, "voiceover_concat_bright.txt")
    temp_silence = os.path.join(AUDIO_DIR, "silence_04.wav")
    
    cmd_silence = [ffmpeg_exe, "-y", "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo", "-t", "0.4", temp_silence]
    subprocess.run(cmd_silence, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    with open(concat_txt_path, "w", encoding="utf-8") as f:
        silence_norm = temp_silence.replace("\\", "/")
        for scene in scenes:
            vo_path = os.path.join(VOICEOVER_DIR, scene["voiceover_file"]).replace("\\", "/")
            f.write(f"file '{vo_path}'\n")
            f.write(f"file '{silence_norm}'\n")
            
    temp_vo_combined = os.path.join(AUDIO_DIR, "temp_vo_bright_combined.wav")
    cmd_concat = [ffmpeg_exe, "-y", "-f", "concat", "-safe", "0", "-i", concat_txt_path, "-c:a", "pcm_s16le", temp_vo_combined]
    subprocess.run(cmd_concat, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Mix with ambient music
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
    print("Mixed audio ready:", TEMP_AUDIO_MIXED)

def generate_video():
    print("Generating High-End Bright Editorial Arabic Ad Video...")
    
    for scene in SCENES_DATA:
        vo_path = os.path.join(VOICEOVER_DIR, scene["voiceover_file"])
        audio_dur = MP3(vo_path).info.length
        scene["duration_sec"] = audio_dur + 0.4
        print(f"Scene {scene['id']} ({scene['title']}): {scene['duration_sec']:.2f}s")
        
    total_sec = sum(s["duration_sec"] for s in SCENES_DATA)
    total_frames = int(total_sec * FPS)
    print(f"Total Video Length: {total_sec:.2f}s ({total_frames} frames)")
    
    # Save 6 Stills for user preview
    for idx, scene in enumerate(SCENES_DATA):
        frame = RENDERERS[idx](0.8)
        out_file = os.path.join(SAMPLES_DIR, f"bright_scene_{scene['id']}.png")
        cv2.imwrite(out_file, frame)
        print(f"Saved still: {out_file}")
        
    # Audio Build
    build_audio_track(SCENES_DATA)
    
    # Video Render
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
    
    # Final H.264 + AAC Mux
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
    print(f"Final Commercial Video Delivered: {OUTPUT_VIDEO_PATH}")
    file_size_mb = os.path.getsize(OUTPUT_VIDEO_PATH) / (1024 * 1024)
    print(f"File Size: {file_size_mb:.2f} MB (Optimized for WhatsApp!)")

if __name__ == "__main__":
    generate_video()
