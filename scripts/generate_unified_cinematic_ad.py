"""
GROW BD TREASURY — UNIFIED CINEMATIC ADVERTISEMENT (REFERENCE-MATCHED)
Matches 100% of the visual grammar, 3D document physicality, rotating HUD radar orbits,
warm cream background, circuit leader lines, and slow deliberate cinematic pacing
seen in 'Make_sure_this_can_merge_with.mp4' and 'Very_slow_paced_I_also_want_i.mp4'.
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
SAMPLES_DIR = os.path.join(AUDIO_DIR, "style_samples")
OUTPUT_VIDEO_PATH = os.path.join(BASE_DIR, "LG_Issuance_Commercial_Ad_Arabic.mp4")
TEMP_VIDEO_SILENT = os.path.join(AUDIO_DIR, "temp_unified_silent.mp4")
TEMP_AUDIO_MIXED = os.path.join(AUDIO_DIR, "temp_unified_audio.wav")

os.makedirs(SAMPLES_DIR, exist_ok=True)

WIDTH = 1280
HEIGHT = 720
FPS = 30

# Exact Reference Palette (Warm Cream & Ambient Light)
BG_LIGHT = (246, 246, 244)         # Warm Cream / Off-White
BG_DARK = (235, 235, 230)          # Subtle radial gradient tone
CIRCUIT_LINE = (210, 215, 222)     # Hairline Trace
CIRCUIT_GLOW = (255, 255, 255)     # Glowing Traces
DOC_PAPER = (255, 255, 255)        # Pure textured paper
DOC_BORDER = (180, 185, 195)       # Classical ornate border
CARD_BORDER = (215, 220, 228)      # Card border line
NAVY_PRIMARY = (18, 24, 38)        # Executive Slate
NAVY_MUTED = (110, 120, 135)       # Caption gray
TURQUOISE = (0, 175, 150)          # Grow Turquoise (#00af96)
TURQUOISE_GLOW = (0, 225, 195)     # Soft Glow
AMBER_WARN = (220, 140, 10)        # Amber alert

def ar(text):
    if not text:
        return ""
    return arabic_reshaper.reshape(text)

def get_font(size, bold=False):
    font_names = ["arialbd.ttf" if bold else "arial.ttf", "timesbd.ttf" if bold else "times.ttf", "segoeuib.ttf" if bold else "segoeui.ttf"]
    for fn in font_names:
        try:
            return ImageFont.truetype(fn, size)
        except:
            continue
    return ImageFont.load_default()

FONT_DOC_TITLE = get_font(18, bold=True)
FONT_DOC_H = get_font(13, bold=True)
FONT_DOC_P = get_font(11, bold=False)
FONT_CALLOUT_H = get_font(15, bold=True)
FONT_CALLOUT_VAL = get_font(17, bold=True)
FONT_SUBTITLE = get_font(14, bold=False)
FONT_TAG = get_font(11, bold=True)
FONT_BRAND = get_font(20, bold=True)

def draw_vector_check(draw, x, y, size=10, color=TURQUOISE, width=2):
    pts = [(x, y + size * 0.5), (x + size * 0.4, y + size), (x + size, y)]
    draw.line([pts[0], pts[1]], fill=color, width=width)
    draw.line([pts[1], pts[2]], fill=color, width=width)

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

# -------------------------------------------------------------
# UNIFIED BACKGROUND & 3D HUD ORBIT ENGINE
# -------------------------------------------------------------

def draw_unified_background(draw, global_time=0.0):
    # Base warm gradient
    draw.rectangle([0, 0, WIDTH, HEIGHT], fill=BG_LIGHT)
    
    # Top subtle header
    draw.rectangle([0, 0, WIDTH, 50], fill=(255, 255, 255, 180))
    draw.line([(40, 50), (WIDTH - 40, 50)], fill=CIRCUIT_LINE, width=1)
    
    draw.rounded_rectangle([40, 12, 66, 38], radius=6, fill=TURQUOISE)
    draw.text((48, 15), "G", fill=(255, 255, 255), font=FONT_TAG)
    draw.text((76, 15), "Grow BD Treasury", fill=NAVY_PRIMARY, font=FONT_BRAND)
    draw.text((WIDTH - 280, 18), ar("إدارة الضمانات البنكية — LG Module"), fill=NAVY_MUTED, font=FONT_DOC_P)
    
    # Concentric HUD Radar Orbits (Slowly rotating as in reference video)
    cx, cy = WIDTH // 2, HEIGHT // 2 + 10
    rot_deg = (global_time * 12.0) % 360.0
    
    for r in [160, 240, 320, 420]:
        draw.ellipse([cx - r, cy - r, cx + r, cy + r], outline=(225, 228, 235), width=1)
        
    # Orbit nodes and dashed tick marks
    for angle_deg in [0, 45, 90, 135, 180, 225, 270, 315]:
        rad = math.radians(angle_deg + rot_deg)
        nx = int(cx + 320 * math.cos(rad))
        ny = int(cy + 320 * math.sin(rad))
        draw.ellipse([nx - 3, ny - 3, nx + 3, ny + 3], fill=(210, 215, 225))
        
    # Circuit connector traces
    traces = [
        ((80, 160), (280, 160), (360, 240)),
        ((WIDTH - 80, 160), (WIDTH - 280, 160), (WIDTH - 360, 240)),
        ((80, 520), (280, 520), (360, 440)),
        ((WIDTH - 80, 520), (WIDTH - 280, 520), (WIDTH - 360, 440)),
    ]
    for pts in traces:
        for i in range(len(pts) - 1):
            draw.line([pts[i], pts[i+1]], fill=CIRCUIT_LINE, width=1)
        draw.ellipse([pts[0][0] - 3, pts[0][1] - 3, pts[0][0] + 3, pts[0][1] + 3], fill=CIRCUIT_LINE)

def draw_physical_lg_document(draw, x, y, w, h, doc_data=None):
    # Realistic 3D Paper Layer with Cast Shadow
    shadow_offset = 8
    draw.rounded_rectangle([x + shadow_offset, y + shadow_offset, x + w + shadow_offset, y + h + shadow_offset], radius=8, fill=(215, 218, 225))
    draw.rounded_rectangle([x, y, x + w, y + h], radius=8, fill=DOC_PAPER, outline=(200, 205, 215), width=1)
    
    # Ornate classical border
    inset = 12
    draw.rounded_rectangle([x + inset, y + inset, x + w - inset, y + h - inset], radius=4, outline=DOC_BORDER, width=1)
    
    # Document Header & Official Seal
    draw.text((x + w // 2 - 110, y + 25), "LETTER OF GUARANTEE", fill=NAVY_PRIMARY, font=FONT_DOC_TITLE)
    draw.text((x + w // 2 - 40, y + 48), "(LG / خطاب ضمان)", fill=NAVY_MUTED, font=FONT_DOC_P)
    
    # Document fields
    fields = [
        ("Reference / المرجع", doc_data.get("ref", "LG-AE01-2026-0078")),
        ("Issuing Bank / البنك", doc_data.get("bank", "Banque Misr — Main Branch")),
        ("Beneficiary / المستفيد", doc_data.get("beneficiary", "National Projects Authority")),
        ("Amount / القيمة", doc_data.get("amount", "EGP 10,000,000.00")),
        ("Expiry Date / الانتهاء", doc_data.get("expiry", "16 March 2027")),
        ("Purpose / الغرض", doc_data.get("purpose", "Performance Security (حسن تنفيذ)"))
    ]
    
    fy = y + 78
    for lbl, val in fields:
        draw.text((x + 28, fy), lbl, fill=NAVY_MUTED, font=FONT_DOC_P)
        draw.text((x + 28, fy + 15), val, fill=NAVY_PRIMARY, font=FONT_DOC_H)
        fy += 44
        
    # Signature line and official stamp watermark
    draw.line([(x + w - 180, y + h - 50), (x + w - 30, y + h - 50)], fill=DOC_BORDER, width=1)
    draw.text((x + w - 170, y + h - 42), "Authorized Signature / التوقيع المعتمد", fill=NAVY_MUTED, font=get_font(9))
    draw.ellipse([x + 30, y + h - 65, x + 80, y + h - 15], outline=(200, 220, 215), width=2)
    draw.text((x + 40, y + h - 46), "SEAL", fill=(180, 205, 200), font=get_font(10, bold=True))

def draw_leader_callout(draw, start_pt, end_pt, title, value, is_left=True, color=TURQUOISE, active=True):
    # Draws the iconic leader line (━━━●) with text block seen in reference video
    sx, sy = start_pt
    ex, ey = end_pt
    
    # Anchor point on document
    draw.ellipse([sx - 4, sy - 4, sx + 4, sy + 4], fill=color if active else CIRCUIT_LINE)
    
    # Angled connector path
    mid_x = (sx + ex) // 2
    draw.line([(sx, sy), (mid_x, ey)], fill=color if active else CIRCUIT_LINE, width=2 if active else 1)
    draw.line([(mid_x, ey), (ex, ey)], fill=color if active else CIRCUIT_LINE, width=2 if active else 1)
    draw.ellipse([ex - 3, ey - 3, ex + 3, ey + 3], fill=color if active else CIRCUIT_LINE)
    
    # Text block
    tx = ex - 160 if is_left else ex + 10
    draw.text((tx, ey - 22), ar(title), fill=NAVY_MUTED, font=FONT_CALLOUT_H)
    draw.text((tx, ey - 2), ar(value), fill=NAVY_PRIMARY, font=FONT_CALLOUT_VAL)

def draw_bottom_dock(draw, text):
    sub_w = 1040
    sub_h = 44
    x1 = (WIDTH - sub_w) // 2
    y1 = HEIGHT - 58
    
    draw.rounded_rectangle([x1, y1, x1 + sub_w, y1 + sub_h], radius=22, fill=(255, 255, 255), outline=CARD_BORDER, width=1)
    draw.ellipse([x1 + 18, y1 + 17, x1 + 28, y1 + 27], fill=TURQUOISE)
    
    reshaped = ar(f'"{text}"')
    draw.text((x1 + 45, y1 + 13), reshaped, fill=NAVY_PRIMARY, font=FONT_SUBTITLE)

# -------------------------------------------------------------
# 6 UNIFIED SCENE RENDERERS (Exact Visual Harmony)
# -------------------------------------------------------------

def render_scene_0(progress):
    # Scene 0: The Central Physical LG Document + 4 Connected Leader Callouts
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_LIGHT)
    draw = ImageDraw.Draw(img)
    draw_unified_background(draw, global_time=progress * 3.0)
    
    # Central Physical Document
    dw, dh = 460, 420
    dx = (WIDTH - dw) // 2
    dy = 80
    
    doc_data = {
        "ref": "LG-AE01-2026-0078",
        "bank": "Banque Misr — Main Corporate",
        "beneficiary": "National Projects Authority",
        "amount": "EGP 10,000,000.00",
        "expiry": "16 March 2027",
        "purpose": "Performance Guarantee (حسن تنفيذ)"
    }
    draw_physical_lg_document(draw, dx, dy, dw, dh, doc_data)
    
    # 4 Symmetrical Leader Callouts (Matching the reference video)
    # Left Top: Financial Amount
    draw_leader_callout(
        draw,
        start_pt=(dx + 28, dy + 225),
        end_pt=(dx - 120, dy + 140),
        title="قيمة الضمان المسجلة",
        value="EGP 10,000,000.00",
        is_left=True,
        color=TURQUOISE,
        active=progress >= 0.20
    )
    
    # Left Bottom: Issuing Bank
    draw_leader_callout(
        draw,
        start_pt=(dx + 28, dy + 135),
        end_pt=(dx - 120, dy + 320),
        title="البنك المصدر",
        value="Banque Misr",
        is_left=True,
        color=TURQUOISE,
        active=progress >= 0.40
    )
    
    # Right Top: Expiry Date
    draw_leader_callout(
        draw,
        start_pt=(dx + dw - 28, dy + 270),
        end_pt=(dx + dw + 120, dy + 140),
        title="تاريخ الاستحقاق",
        value="16 March 2027",
        is_left=False,
        color=TURQUOISE,
        active=progress >= 0.60
    )
    
    # Right Bottom: Beneficiary Authority
    draw_leader_callout(
        draw,
        start_pt=(dx + dw - 28, dy + 180),
        end_pt=(dx + dw + 120, dy + 320),
        title="المستفيد الرسمي",
        value="National Authority",
        is_left=False,
        color=TURQUOISE,
        active=progress >= 0.80
    )
    
    draw_bottom_dock(draw, SCENES_DATA[0]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_1(progress):
    # Scene 1: Central Document branches directly into the Multi-Bank Facility Network
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_LIGHT)
    draw = ImageDraw.Draw(img)
    draw_unified_background(draw, global_time=progress * 3.0 + 3.0)
    
    # Left: Circular Facility Hub Card
    fx, fy, fw, fh = 70, 80, 480, 420
    draw.rounded_rectangle([fx + 6, fy + 6, fx + fw + 6, fy + fh + 6], radius=12, fill=(215, 218, 225))
    draw.rounded_rectangle([fx, fy, fx + fw, fy + fh], radius=12, fill=DOC_PAPER, outline=DOC_BORDER, width=1)
    
    draw.text((fx + 30, fy + 25), ar("إجمالي التسهيلات المتاحة"), fill=NAVY_PRIMARY, font=FONT_CALLOUT_VAL)
    draw.text((fx + 30, fy + 52), "Multi-Bank Facility Limit: EGP 500M", fill=NAVY_MUTED, font=FONT_DOC_P)
    
    # Central Gauge Ring
    cx, cy = fx + fw // 2, fy + 220
    r = 100
    draw.ellipse([cx - r, cy - r, cx + r, cy + r], outline=(225, 228, 235), width=16)
    
    fill_ang = int(246 * min(1.0, progress * 1.3))
    draw.arc([cx - r, cy - r, cx + r, cy + r], start=-90, end=-90 + fill_ang, fill=TURQUOISE, width=16)
    draw.text((cx - 50, cy - 20), "68.4%", fill=NAVY_PRIMARY, font=FONT_BRAND)
    draw.text((cx - 60, cy + 12), ar("الاستهلاك الحالي"), fill=NAVY_MUTED, font=FONT_DOC_P)
    
    draw.rounded_rectangle([fx + 30, fy + 355, fx + fw - 30, fy + 395], radius=6, fill=BG_LIGHT, outline=CIRCUIT_LINE, width=1)
    draw.text((fx + 45, fy + 368), ar("معدل الاستهلاك: السقف يغطي حتى أكتوبر 2026"), fill=NAVY_PRIMARY, font=FONT_TAG)
    
    # Connecting circuit lines from hub to bank cards
    for i in range(4):
        by = fy + 25 + i * 95 + 35
        draw.line([(fx + fw, cy), (fx + fw + 40, by), (590, by)], fill=TURQUOISE if progress >= (0.2 + i * 0.2) else CIRCUIT_LINE, width=2)
        
    # Right: 4 Bank Capacity Cards (Exact match styling)
    banks = [
        ("National Bank of Egypt (NBE)", "Limit: EGP 150M", "Available: EGP 45M", 0.70, 0.20),
        ("Commercial International Bank (CIB)", "Limit: EGP 150M", "Available: EGP 60M", 0.60, 0.40),
        ("QNB Alahli", "Limit: EGP 100M", "Available: EGP 35M", 0.65, 0.60),
        ("Banque Misr", "Limit: EGP 100M", "Available: EGP 20M", 0.80, 0.80)
    ]
    
    bx, bw, bh = 590, 620, 78
    for i, (bname, blimit, bavail, bpct, trig_p) in enumerate(banks):
        by = fy + 20 + i * 95
        is_act = (progress >= trig_p)
        draw.rounded_rectangle([bx + 4, by + 4, bx + bw + 4, by + bh + 4], radius=8, fill=(220, 225, 230))
        draw.rounded_rectangle([bx, by, bx + bw, by + bh], radius=8, fill=DOC_PAPER, outline=TURQUOISE if is_act else DOC_BORDER, width=2 if is_act else 1)
        
        draw.text((bx + 25, by + 16), bname, fill=NAVY_PRIMARY, font=FONT_CALLOUT_H)
        draw.text((bx + 25, by + 42), blimit, fill=NAVY_MUTED, font=FONT_DOC_P)
        draw.text((bx + bw - 180, by + 16), ar(bavail), fill=TURQUOISE, font=FONT_CALLOUT_VAL)
        
        # Headroom bar
        draw.rounded_rectangle([bx + bw - 180, by + 45, bx + bw - 30, by + 51], radius=3, fill=(225, 228, 235))
        draw.rounded_rectangle([bx + bw - 180, by + 45, bx + bw - 180 + int(150 * bpct), by + 51], radius=3, fill=TURQUOISE)
        
    draw_bottom_dock(draw, SCENES_DATA[1]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_2(progress):
    # Scene 2: Request Document vs Historical Records with 87% Match Radar
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_LIGHT)
    draw = ImageDraw.Draw(img)
    draw_unified_background(draw, global_time=progress * 3.0 + 6.0)
    
    # Left: New Request Document Card
    rx, ry, rw, rh = 70, 80, 520, 420
    draw.rounded_rectangle([rx + 6, ry + 6, rx + rw + 6, ry + rh + 6], radius=12, fill=(215, 218, 225))
    draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=12, fill=DOC_PAPER, outline=DOC_BORDER, width=1)
    
    draw.rounded_rectangle([rx + 25, ry + 20, rx + 140, ry + 46], radius=6, fill=(230, 255, 250))
    draw.text((rx + 35, ry + 25), "NEW REQUEST", fill=TURQUOISE, font=FONT_TAG)
    draw.text((rx + 155, ry + 25), ar("طلب إصدار خطاب ضمان"), fill=NAVY_PRIMARY, font=FONT_CALLOUT_H)
    draw.line([(rx + 25, ry + 60), (rx + rw - 25, ry + 60)], fill=CIRCUIT_LINE, width=1)
    
    req_fields = [
        ("المستفيد الرسمي", "National Projects Authority"),
        ("كود المشروع / العقد", "E2E-PRJ-2026 — محطة كهرباء العاصمة"),
        ("مبلغ ونوع الضمان", "Performance LG — EGP 10,000,000.00"),
        ("فترة التغطية والانتهاء", "12 شهر — 16 March 2027"),
        ("العمولة المقدرة", "0.75% سنوي (EGP 75,000)")
    ]
    
    fy = ry + 75
    for lbl, val in req_fields:
        draw.text((rx + 28, fy), ar(lbl), fill=NAVY_MUTED, font=FONT_DOC_P)
        draw.text((rx + 28, fy + 16), ar(val), fill=NAVY_PRIMARY, font=FONT_DOC_H)
        fy += 56
        
    # Connecting scan lines to right
    for i in range(4):
        cy_pos = ry + 100 + i * 78 + 30
        draw.line([(rx + rw, cy_pos), (630, cy_pos)], fill=TURQUOISE if progress >= (0.3 + i * 0.2) else CIRCUIT_LINE, width=2)
        
    # Right: 87% Match Radar & Attribute Checklist Card
    mx, my, mw, mh = 630, 80, 580, 420
    draw.rounded_rectangle([mx + 6, my + 6, mx + mw + 6, my + mh + 6], radius=12, fill=(215, 218, 225))
    draw.rounded_rectangle([mx, my, mx + mw, my + mh], radius=12, fill=DOC_PAPER, outline=TURQUOISE if progress >= 0.40 else DOC_BORDER, width=2 if progress >= 0.40 else 1)
    
    # 87% Match Badge Header
    draw.rounded_rectangle([mx + 25, my + 20, mx + 160, my + 54], radius=8, fill=(230, 255, 250), outline=TURQUOISE, width=2)
    draw.text((mx + 35, my + 26), "87% MATCH", fill=TURQUOISE, font=FONT_CALLOUT_VAL)
    draw.text((mx + 180, my + 28), ar("فحص التطابق مع سجلات المشروع"), fill=NAVY_PRIMARY, font=FONT_CALLOUT_H)
    draw.line([(mx + 25, my + 68), (mx + mw - 25, my + 68)], fill=CIRCUIT_LINE, width=1)
    
    checks = [
        ("المستفيد (Beneficiary)", "تطابق 100% مع العقود السابقة للمشروع", True, 0.30),
        ("بيانات العقد (Contract)", "مسجل ومعتمد مسبقاً في قاعدة البيانات", True, 0.50),
        ("فترة التغطية (Dates)", "متوافقة مع الجدول الزمني للمشروع", True, 0.70),
        ("تداخل القيمة (Amount)", "تنبيه: تداخل جزئي مع دفعة مقدمة سابقة للمراجعة", False, 0.85)
    ]
    
    cy_box = my + 82
    for item_h, item_d, is_ok, trig_p in checks:
        is_act = (progress >= trig_p)
        bg = (230, 255, 250) if (is_act and is_ok) else ((254, 243, 199) if (is_act and not is_ok) else BG_LIGHT)
        bdr = TURQUOISE if (is_act and is_ok) else (AMBER_WARN if (is_act and not is_ok) else CIRCUIT_LINE)
        
        draw.rounded_rectangle([mx + 25, cy_box, mx + mw - 25, cy_box + 68], radius=8, fill=bg, outline=bdr, width=1)
        draw.ellipse([mx + 38, cy_box + 16, mx + 64, cy_box + 42], fill=DOC_PAPER, outline=TURQUOISE if is_ok else AMBER_WARN, width=2)
        if is_act:
            if is_ok:
                draw_vector_check(draw, mx + 45, cy_box + 22, size=10, color=TURQUOISE, width=2)
            else:
                draw.text((mx + 48, cy_box + 18), "!", fill=AMBER_WARN, font=FONT_CALLOUT_H)
                
        draw.text((mx + 78, cy_box + 12), ar(item_h), fill=NAVY_PRIMARY, font=FONT_CALLOUT_H)
        draw.text((mx + 78, cy_box + 36), ar(item_d), fill=NAVY_MUTED if is_ok else AMBER_WARN, font=FONT_DOC_P)
        cy_box += 78
        
    draw_bottom_dock(draw, SCENES_DATA[2]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_3(progress):
    # Scene 3: The Approved Request transforms into the Official Bank Application Form
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_LIGHT)
    draw = ImageDraw.Draw(img)
    draw_unified_background(draw, global_time=progress * 3.0 + 9.0)
    
    # Official Bank Form Document (Central Physical Sheet)
    fx, fy, fw, fh = (WIDTH - 820) // 2, 80, 820, 420
    draw.rounded_rectangle([fx + 6, fy + 6, fx + fw + 6, fy + fh + 6], radius=12, fill=(215, 218, 225))
    draw.rounded_rectangle([fx, fy, fx + fw, fy + fh], radius=12, fill=DOC_PAPER, outline=DOC_BORDER, width=1)
    
    # Approved Stamp Badge
    draw.rounded_rectangle([fx + 30, fy + 20, fx + 155, fy + 52], radius=6, fill=(230, 255, 250), outline=TURQUOISE, width=2)
    draw.text((fx + 42, fy + 26), "APPROVED", fill=TURQUOISE, font=FONT_CALLOUT_H)
    draw_vector_check(draw, fx + 130, fy + 30, size=10, color=TURQUOISE, width=2)
    
    draw.text((fx + 175, fy + 26), ar("استمارة طلب إصدار خطاب ضمان بنكي — جاهزة للإرسال"), fill=NAVY_PRIMARY, font=FONT_CALLOUT_H)
    draw.line([(fx + 30, fy + 64), (fx + fw - 30, fy + 64)], fill=CIRCUIT_LINE, width=1)
    
    form_lines = [
        ("جهة الإصدار المطلوبة", "Banque Misr — الفرع الرئيسي (Main Corporate Branch)", 0.25),
        ("اسم العميل / طالب الإصدار", "Grow BD Engineering & Trading SAE — س.ت 104920", 0.40),
        ("اسم المستفيد الرسمي", "National Authority for Infrastructure & Electricity Projects", 0.55),
        ("نوع ومبلغ الضمان", "Performance Guarantee — EGP 10,000,000 (Ten Million EGP)", 0.70),
        ("تاريخ الاستحقاق والصيغة", "16 March 2027 — صيغة موحدة معتمدة (Corporate Standard Wording)", 0.85)
    ]
    
    sy = fy + 76
    for lbl, val, trig_p in form_lines:
        is_pop = (progress >= trig_p)
        bg = (230, 255, 250) if is_pop else BG_LIGHT
        draw.rounded_rectangle([fx + 30, sy, fx + fw - 30, sy + 44], radius=6, fill=bg, outline=TURQUOISE if is_pop else CIRCUIT_LINE, width=1)
        draw.text((fx + 45, sy + 10), ar(lbl), fill=NAVY_MUTED, font=FONT_DOC_P)
        draw.text((fx + 220, sy + 10), ar(val), fill=NAVY_PRIMARY, font=FONT_CALLOUT_H)
        if is_pop:
            draw_vector_check(draw, fx + fw - 50, sy + 16, size=8, color=TURQUOISE, width=2)
        sy += 52
        
    # Delivery Tracker at bottom of sheet
    ty = fy + 345
    draw.rounded_rectangle([fx + 30, ty, fx + fw - 30, ty + 60], radius=8, fill=DOC_PAPER, outline=TURQUOISE, width=1)
    steps = ["1. الاعتماد الداخلي ✓", "2. تجهيز الاستمارة ✓", "3. إصدار الضمان ⏳", "4. التسليم للجهة ⚪"]
    step_w = (fw - 60) // 4
    for i, st in enumerate(steps):
        sx = fx + 40 + i * step_w
        draw.text((sx, ty + 20), ar(st), fill=NAVY_PRIMARY if i < 2 else NAVY_MUTED, font=FONT_DOC_H)
        
    draw_bottom_dock(draw, SCENES_DATA[3]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_4(progress):
    # Scene 4: Dual Physical Records Lock (Treasury vs Bank Position)
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_LIGHT)
    draw = ImageDraw.Draw(img)
    draw_unified_background(draw, global_time=progress * 3.0 + 12.0)
    
    # 2 Side-by-Side Symmetrical Document Cards
    w, h, y = 550, 420, 80
    
    # Left Card: Grow Treasury
    lx = 70
    draw.rounded_rectangle([lx + 6, y + 6, lx + w + 6, y + h + 6], radius=12, fill=(215, 218, 225))
    draw.rounded_rectangle([lx, y, lx + w, y + h], radius=12, fill=DOC_PAPER, outline=DOC_BORDER, width=1)
    draw.text((lx + 30, y + 22), ar("سجلات الخزينة بالنظام"), fill=NAVY_PRIMARY, font=FONT_CALLOUT_VAL)
    draw.text((lx + 30, y + 48), "Grow Treasury Internal Position", fill=NAVY_MUTED, font=FONT_DOC_P)
    draw.line([(lx + 30, y + 68), (lx + w - 30, y + 68)], fill=CIRCUIT_LINE, width=1)
    
    # Right Card: Bank LG Position
    rx = 660
    draw.rounded_rectangle([rx + 6, y + 6, rx + w + 6, y + h + 6], radius=12, fill=(215, 218, 225))
    draw.rounded_rectangle([rx, y, rx + w, y + h], radius=12, fill=DOC_PAPER, outline=DOC_BORDER, width=1)
    draw.text((rx + 30, y + 22), ar("سجل وموقف البنك الفعلي"), fill=NAVY_PRIMARY, font=FONT_CALLOUT_VAL)
    draw.text((rx + 30, y + 48), "Bank Official LG Position", fill=NAVY_MUTED, font=FONT_DOC_P)
    draw.line([(rx + 30, y + 68), (rx + w - 30, y + 68)], fill=CIRCUIT_LINE, width=1)
    
    rows = [
        ("رقم مرجع الضمان", "LG-2026-00789", "Ref: NBE-LG-99410", True, 0.25),
        ("قيمة الضمان", "EGP 10,000,000", "EGP 10,000,000", True, 0.45),
        ("تاريخ الاستحقاق", "16 March 2027", "16 March 2027", True, 0.65),
        ("طلب تعديل معلق", "تعديل قيمة +1M", "قيد المعالجة بالبنك", False, 0.85)
    ]
    
    sy = y + 82
    for lbl, lval, rval, is_match, trig_p in rows:
        is_act = (progress >= trig_p)
        bg = (230, 255, 250) if (is_act and is_match) else ((254, 243, 199) if (is_act and not is_match) else BG_LIGHT)
        bdr = TURQUOISE if (is_act and is_match) else (AMBER_WARN if (is_act and not is_match) else CIRCUIT_LINE)
        
        # Left row
        draw.rounded_rectangle([lx + 25, sy, lx + w - 25, sy + 68], radius=8, fill=bg, outline=bdr, width=1)
        draw.text((lx + 38, sy + 12), ar(lbl), fill=NAVY_MUTED, font=FONT_DOC_P)
        draw.text((lx + 38, sy + 34), ar(lval), fill=NAVY_PRIMARY, font=FONT_CALLOUT_H)
        
        # Connecting line across the center seam
        draw.line([(lx + w - 25, sy + 34), (rx + 25, sy + 34)], fill=TURQUOISE if (is_act and is_match) else CIRCUIT_LINE, width=2)
        
        # Right row
        draw.rounded_rectangle([rx + 25, sy, rx + w - 25, sy + 68], radius=8, fill=bg, outline=bdr, width=1)
        draw.text((rx + 38, sy + 12), ar(lbl), fill=NAVY_MUTED, font=FONT_DOC_P)
        draw.text((rx + 38, sy + 34), ar(rval), fill=NAVY_PRIMARY, font=FONT_CALLOUT_H)
        
        if is_act:
            draw.rounded_rectangle([rx + w - 160, sy + 20, rx + w - 40, sy + 48], radius=4, fill=DOC_PAPER, outline=TURQUOISE if is_match else AMBER_WARN, width=1)
            draw.text((rx + w - 150, sy + 25), "MATCHED" if is_match else "REVIEW ⚠", fill=TURQUOISE if is_match else AMBER_WARN, font=FONT_TAG)
            
        sy += 78
        
    draw_bottom_dock(draw, SCENES_DATA[4]["subtitle"])
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

def render_scene_5(progress):
    # Scene 5: Horizon Pull-Back & Grand Network Hub Lockup
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_LIGHT)
    draw = ImageDraw.Draw(img)
    draw_unified_background(draw, global_time=progress * 3.0 + 15.0)
    
    cw, ch = 860, 420
    cx = (WIDTH - cw) // 2
    cy = 80
    
    draw.rounded_rectangle([cx + 8, cy + 8, cx + cw + 8, cy + ch + 8], radius=16, fill=(215, 218, 225))
    draw.rounded_rectangle([cx, cy, cx + cw, cy + ch], radius=16, fill=DOC_PAPER, outline=DOC_BORDER, width=1)
    
    # Official Logo Mark
    draw.rounded_rectangle([WIDTH // 2 - 30, cy + 30, WIDTH // 2 + 30, cy + 90], radius=10, fill=TURQUOISE)
    draw.text((WIDTH // 2 - 12, cy + 38), "G", fill=(255, 255, 255), font=FONT_BRAND)
    
    draw.text((WIDTH // 2 - 135, cy + 105), "Grow BD Treasury", fill=NAVY_PRIMARY, font=FONT_BRAND)
    draw.text((WIDTH // 2 - 180, cy + 145), ar("منصة إدارة ومطابقة خطابات الضمان المؤسسية"), fill=NAVY_MUTED, font=FONT_CALLOUT_VAL)
    
    # Connected Continuous Track
    track_y = cy + 210
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
        is_pul = (progress >= trig_p)
        draw.ellipse([px + 35, track_y + 18, px + 59, track_y + 42], fill=TURQUOISE if is_pul else DOC_PAPER, outline=TURQUOISE, width=3)
        draw.text((px + 10, track_y + 55), ar(p_title), fill=NAVY_PRIMARY if is_pul else NAVY_MUTED, font=FONT_CALLOUT_VAL)
        draw.text((px + 10, track_y + 85), ar(p_desc), fill=TURQUOISE if is_pul else NAVY_MUTED, font=FONT_DOC_P)
        
    # CTA Button
    draw.rounded_rectangle([WIDTH // 2 - 130, cy + 350, WIDTH // 2 + 130, cy + 395], radius=22, fill=TURQUOISE)
    draw.text((WIDTH // 2 - 80, cy + 362), ar("طلب جلسة استعراضية"), fill=(255, 255, 255), font=FONT_CALLOUT_H)
    
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
    
    concat_txt_path = os.path.join(AUDIO_DIR, "voiceover_concat_unified.txt")
    temp_silence = os.path.join(AUDIO_DIR, "silence_04.wav")
    
    cmd_silence = [ffmpeg_exe, "-y", "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo", "-t", "0.4", temp_silence]
    subprocess.run(cmd_silence, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    with open(concat_txt_path, "w", encoding="utf-8") as f:
        silence_norm = temp_silence.replace("\\", "/")
        for scene in scenes:
            vo_path = os.path.join(VOICEOVER_DIR, scene["voiceover_file"]).replace("\\", "/")
            f.write(f"file '{vo_path}'\n")
            f.write(f"file '{silence_norm}'\n")
            
    temp_vo_combined = os.path.join(AUDIO_DIR, "temp_vo_unified_combined.wav")
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
    print("Generating Reference-Matched Unified Cinematic Ad...")
    
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
        out_file = os.path.join(SAMPLES_DIR, f"unified_scene_{scene['id']}.png")
        cv2.imwrite(out_file, frame)
        print(f"Saved still: {out_file}")
        
    build_audio_track(SCENES_DATA)
    
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(TEMP_VIDEO_SILENT, fourcc, FPS, (WIDTH, HEIGHT))
    
    current_frame = 0
    TRANSITION_FRAMES = 12
    
    for idx, scene in enumerate(SCENES_DATA):
        scene_frames = int(scene["duration_sec"] * FPS)
        for f in range(scene_frames):
            prog = f / float(scene_frames)
            frame_curr = RENDERERS[idx](prog)
            
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
    print(f"Final Unified Video Delivered: {OUTPUT_VIDEO_PATH}")
    file_size_mb = os.path.getsize(OUTPUT_VIDEO_PATH) / (1024 * 1024)
    print(f"File Size: {file_size_mb:.2f} MB (Optimized for WhatsApp!)")

if __name__ == "__main__":
    generate_video()
