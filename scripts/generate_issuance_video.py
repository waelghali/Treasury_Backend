"""
GROW BD — LG ISSUANCE PRODUCT ADVERTISEMENT VIDEO GENERATOR (V2 PREMIUM)
- Sleek executive dark theme (Zero red pills / Zero cheap buttons)
- Minimalist luxury 7-stage header ribbon
- Full audio mixing: Voiceover + Background Music track (BGM)
"""

import os
import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont

WIDTH = 1280
HEIGHT = 720
FPS = 30

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "interactive-experience", "assets", "screenshots")
OUTPUT_VIDEO_PATH = os.path.join(BASE_DIR, "LG_Issuance_Demo_WhatsApp.mp4")
AUDIO_DIR = os.path.join(BASE_DIR, "audio_assets")
VOICEOVER_DIR = os.path.join(AUDIO_DIR, "voiceover")
BGM_DIR = os.path.join(AUDIO_DIR, "bgm")

def get_font(size, bold=False):
    font_names = ["arialbd.ttf" if bold else "arial.ttf", "segoeuib.ttf" if bold else "segoeui.ttf", "calibrib.ttf" if bold else "calibri.ttf"]
    for fn in font_names:
        try:
            return ImageFont.truetype(fn, size)
        except:
            continue
    return ImageFont.load_default()

FONT_TITLE = get_font(26, bold=True)
FONT_HEADLINE = get_font(34, bold=True)
FONT_SUBTITLE = get_font(19, bold=False)
FONT_CALLOUT_H = get_font(17, bold=True)
FONT_CALLOUT_P = get_font(14, bold=False)
FONT_TAG = get_font(13, bold=True)
FONT_NUM = get_font(12, bold=True)

STAGES_CONFIG = [
    {"num": "01", "name": "Facility"},
    {"num": "02", "name": "Request"},
    {"num": "03", "name": "Communication"},
    {"num": "04", "name": "Approval"},
    {"num": "05", "name": "Issuance"},
    {"num": "06", "name": "Maintenance"},
    {"num": "07", "name": "Reconciliation"},
]

SCENES = [
    # Scene 0: Hook & The Reality (Sleek Executive Dark Aesthetic)
    {
        "type": "hero_opening",
        "duration_sec": 6.5,
        "kicker": "GROW BD TREASURY PLATFORM",
        "headline": "An LG doesn't start with a bank.\nIt starts with a business requirement.",
        "frictions": [
            "Scattered Emails & Manual Follow-ups",
            "Disconnected Spreadsheets & Static Registers",
            "Unverified Facility Headroom & Limit Risks"
        ],
        "subtitle": "An LG doesn't start with a bank. It starts with a business requirement. Managing that requirement shouldn't mean chasing emails, spreadsheets and approvals.",
        "active_stage": 0
    },
    # Stage 1: Facility
    {
        "type": "product",
        "duration_sec": 7.0,
        "image": os.path.join(SCREENSHOTS_DIR, "Facilities.jpg"),
        "stage_tag": "STAGE 1 — FACILITY",
        "headline": "Know your capacity.",
        "subtitle": "Start with the facility. Know your available capacity before the request moves forward.",
        "spotlight": (120, 140, 1040, 330),
        "callouts": [
            {"x": 160, "y": 190, "w": 420, "h": 85, "title": "Multi-Bank Facility Lines", "desc": "Instant headroom check across issuing partner banks."},
            {"x": 680, "y": 370, "w": 440, "h": 85, "title": "Sub-limit & Margin Control", "desc": "Enforce dedicated caps for Bid, Performance & Advance LGs."}
        ],
        "active_stage": 1
    },
    # Stage 2: Request
    {
        "type": "product",
        "duration_sec": 7.0,
        "image": os.path.join(SCREENSHOTS_DIR, "Request_form.jpg"),
        "stage_tag": "STAGE 2 — ISSUANCE REQUEST",
        "headline": "Structure the request.",
        "subtitle": "Turn the business requirement into a structured issuance request.",
        "spotlight": (180, 140, 920, 360),
        "callouts": [
            {"x": 220, "y": 190, "w": 420, "h": 85, "title": "Structured Digital Intake", "desc": "Capture contract clauses, specs, and expiry terms with zero ambiguity."},
            {"x": 640, "y": 400, "w": 420, "h": 85, "title": "Pre-Validated Parameters", "desc": "Automatic checks against available facility headroom and approved wording."}
        ],
        "active_stage": 2
    },
    # Stage 3: Communication
    {
        "type": "product",
        "duration_sec": 7.0,
        "image": os.path.join(SCREENSHOTS_DIR, "Issuance_Follow_Up.jpg"),
        "stage_tag": "STAGE 3 — COMMUNICATION",
        "headline": "Manage the bank interaction.",
        "subtitle": "Manage the bank interaction and issuance options within the workflow.",
        "spotlight": (140, 140, 1000, 340),
        "callouts": [
            {"x": 180, "y": 190, "w": 430, "h": 85, "title": "RFQ & Quotation Comparison", "desc": "Compare competitive bank fee quotes, margin terms, and turnaround times."},
            {"x": 670, "y": 390, "w": 430, "h": 85, "title": "Bank Form Auto-Fill", "desc": "Auto-populate issuing bank application templates without manual data re-entry."}
        ],
        "active_stage": 3
    },
    # Stage 4: Approval
    {
        "type": "product",
        "duration_sec": 7.0,
        "image": os.path.join(SCREENSHOTS_DIR, "Approval Center_D.jpg"),
        "stage_tag": "STAGE 4 — APPROVAL",
        "headline": "Control before issuance.",
        "subtitle": "Apply the right controls before the LG reaches the bank.",
        "spotlight": (140, 140, 1000, 340),
        "callouts": [
            {"x": 180, "y": 190, "w": 430, "h": 85, "title": "Threshold Authorization Matrix", "desc": "Automated multi-tier approval routing governed by financial signing limits."},
            {"x": 670, "y": 390, "w": 430, "h": 85, "title": "Comprehensive Audit Trail", "desc": "Every approval, delegation, and exception check is permanently logged."}
        ],
        "active_stage": 4
    },
    # Stage 5: Issuance
    {
        "type": "product",
        "duration_sec": 7.0,
        "image": os.path.join(SCREENSHOTS_DIR, "Issued_LG_Details.jpg"),
        "stage_tag": "STAGE 5 — ISSUANCE",
        "headline": "Move from request to reality.",
        "subtitle": "Move from approved request to actual issuance with full visibility.",
        "spotlight": (140, 140, 1000, 340),
        "callouts": [
            {"x": 180, "y": 190, "w": 440, "h": 85, "title": "Bank LG Reference & Active Record", "desc": "Formal financial instrument registered with verified bank reference."},
            {"x": 660, "y": 390, "w": 440, "h": 85, "title": "Cash Margin & Maturity Tracking", "desc": "Automatic collateral accounting and exact timeline tracking to expiry."}
        ],
        "active_stage": 5
    },
    # Stage 6: Maintenance
    {
        "type": "product",
        "duration_sec": 7.0,
        "image": os.path.join(SCREENSHOTS_DIR, "Action_Center.jpg"),
        "stage_tag": "STAGE 6 — MAINTENANCE",
        "headline": "Control the LG throughout its lifecycle.",
        "subtitle": "And once issued, keep control throughout the LG lifecycle.",
        "spotlight": (140, 140, 1000, 350),
        "callouts": [
            {"x": 180, "y": 190, "w": 440, "h": 85, "title": "Active Lifecycle Actions", "desc": "Execute extensions, amount amendments, and releases with governed workflows."},
            {"x": 660, "y": 390, "w": 440, "h": 85, "title": "Proactive Expiry Management", "desc": "Automated 30, 60, and 90-day notifications prevent critical default risks."}
        ],
        "active_stage": 6
    },
    # Stage 7: Reconciliation
    {
        "type": "product",
        "duration_sec": 7.5,
        "image": os.path.join(SCREENSHOTS_DIR, "Reconciliation.jpg"),
        "stage_tag": "STAGE 7 — RECONCILIATION",
        "headline": "Keep the system position aligned with the bank position.",
        "subtitle": "Finally, reconcile the bank LG position with the system position and identify any gaps.",
        "spotlight": (140, 140, 1000, 350),
        "callouts": [
            {"x": 180, "y": 190, "w": 440, "h": 85, "title": "Bank LG Position vs System Position", "desc": "Automated matching flags amount variances, expiry mismatches, and unrecorded items."},
            {"x": 660, "y": 390, "w": 440, "h": 85, "title": "Resolution Workflow", "desc": "Structured exception resolution ensures internal books mirror exact bank positions."}
        ],
        "active_stage": 7
    },
    # Closing Finale
    {
        "type": "hero_closing",
        "duration_sec": 6.5,
        "kicker": "THE COMPLETE TREASURY WORKFLOW",
        "headline": "From request to issuance.\nFrom issuance to lifecycle.\nMaintain control over every LG.",
        "subtitle": "From request to issuance. From issuance to lifecycle. Maintain control over every LG.",
        "active_stage": 8
    }
]

def draw_sleek_ribbon(draw, active_stage):
    # Minimalist sleek frosted glass ribbon
    draw.rectangle([0, 0, WIDTH, 48], fill=(10, 16, 24, 240))
    draw.line([(0, 48), (WIDTH, 48)], fill=(255, 255, 255, 20), width=1)
    
    # Brand Tag
    draw.ellipse([26, 21, 34, 29], fill=(0, 209, 178))
    draw.text((42, 16), "LG ISSUANCE", fill=(0, 209, 178), font=FONT_TAG)
    
    # 7-Stage Sleek Track
    start_x = 220
    track_w = 1000
    step_w = track_w // 7
    
    for i, stg in enumerate(STAGES_CONFIG):
        stg_num = i + 1
        x = start_x + i * step_w
        is_active = (stg_num == active_stage)
        is_passed = (stg_num < active_stage)
        
        # Step indicator
        num_c = (0, 209, 178) if (is_active or is_passed) else (100, 116, 139)
        text_c = (255, 255, 255) if is_active else ((203, 213, 225) if is_passed else (148, 163, 184))
        
        if is_active:
            draw.rounded_rectangle([x - 6, 10, x + step_w - 14, 38], radius=6, fill=(0, 209, 178, 30), outline=(0, 209, 178, 180), width=1)
            
        draw.text((x, 16), stg["num"], fill=num_c, font=FONT_NUM)
        draw.text((x + 22, 16), stg["name"], fill=text_c, font=FONT_TAG)
        
        if i < len(STAGES_CONFIG) - 1:
            draw.text((x + step_w - 22, 16), "›", fill=(71, 85, 105), font=FONT_TAG)

def draw_subtitles(draw, text):
    if not text:
        return
    sub_w = 1080
    sub_h = 44
    x1 = (WIDTH - sub_w) // 2
    y1 = HEIGHT - 64
    draw.rounded_rectangle([x1, y1, x1 + sub_w, y1 + sub_h], radius=22, fill=(8, 12, 18, 235), outline=(255, 255, 255, 30), width=1)
    
    bbox = draw.textbbox((0, 0), text, font=FONT_SUBTITLE)
    text_w = bbox[2] - bbox[0]
    tx = x1 + (sub_w - text_w) // 2
    draw.text((tx, y1 + 10), text, fill=(248, 250, 252), font=FONT_SUBTITLE)

def render_frame(scene, progress):
    img = Image.new("RGBA", (WIDTH, HEIGHT), (8, 12, 16, 255))
    draw = ImageDraw.Draw(img)
    
    if scene["type"] == "hero_opening":
        # Luxury Dark Backdrop
        draw.rectangle([0, 0, WIDTH, HEIGHT], fill=(8, 14, 22))
        draw_sleek_ribbon(draw, 0)
        
        # Elegant Kicker
        draw.text((WIDTH//2 - 150, 130), scene["kicker"], fill=(56, 189, 248), font=FONT_TAG)
        
        # Headline
        lines = scene["headline"].split("\n")
        draw.text((WIDTH//2 - 380, 175), lines[0], fill=(255, 255, 255), font=FONT_HEADLINE)
        draw.text((WIDTH//2 - 380, 225), lines[1], fill=(0, 209, 178), font=FONT_HEADLINE)
        
        # Sleek Glass Cards (No bright red pills)
        card_w = 780
        card_h = 46
        start_y = 310
        for i, friction in enumerate(scene["frictions"]):
            cy = start_y + i * 56
            draw.rounded_rectangle([WIDTH//2 - card_w//2, cy, WIDTH//2 + card_w//2, cy + card_h], radius=8, fill=(15, 23, 34, 220), outline=(255, 255, 255, 25), width=1)
            draw.ellipse([WIDTH//2 - card_w//2 + 18, cy + 19, WIDTH//2 - card_w//2 + 26, cy + 27], fill=(56, 189, 248))
            draw.text((WIDTH//2 - card_w//2 + 38, cy + 12), friction, fill=(203, 213, 225), font=FONT_SUBTITLE)
            
        # Controlled Solution Banner
        sol_text = "From Request to Issuance — Controlled in One Workflow"
        draw.rounded_rectangle([WIDTH//2 - 280, 500, WIDTH//2 + 280, 548], radius=24, fill=(0, 209, 178, 30), outline=(0, 209, 178, 200), width=1)
        draw.text((WIDTH//2 - 240, 513), sol_text, fill=(255, 255, 255), font=FONT_SUBTITLE)
        
        draw_subtitles(draw, scene["subtitle"])
        
    elif scene["type"] == "hero_closing":
        draw.rectangle([0, 0, WIDTH, HEIGHT], fill=(8, 14, 22))
        draw_sleek_ribbon(draw, 8)
        
        draw.text((WIDTH//2 - 170, 110), scene["kicker"], fill=(56, 189, 248), font=FONT_TAG)
        
        lines = scene["headline"].split("\n")
        draw.text((WIDTH//2 - 260, 150), lines[0], fill=(255, 255, 255), font=FONT_HEADLINE)
        draw.text((WIDTH//2 - 260, 195), lines[1], fill=(255, 255, 255), font=FONT_HEADLINE)
        draw.text((WIDTH//2 - 320, 245), lines[2], fill=(0, 209, 178), font=FONT_HEADLINE)
        
        # 7-stage connected flow
        card_w = 142
        card_h = 68
        start_x = 100
        y = 340
        for i, stg in enumerate(STAGES_CONFIG):
            cx = start_x + i * (card_w + 14)
            draw.rounded_rectangle([cx, y, cx + card_w, y + card_h], radius=8, fill=(16, 26, 38, 230), outline=(0, 209, 178, 100), width=1)
            draw.text((cx + 12, y + 10), stg["num"], fill=(0, 209, 178), font=FONT_NUM)
            draw.text((cx + 12, y + 32), stg["name"], fill=(255, 255, 255), font=FONT_TAG)
            
        # CTAs
        draw.rounded_rectangle([WIDTH//2 - 220, 470, WIDTH//2 - 20, 520], radius=8, fill=(0, 209, 178, 255))
        draw.text((WIDTH//2 - 195, 485), "Request Sandbox", fill=(8, 12, 16), font=FONT_SUBTITLE)
        
        draw.rounded_rectangle([WIDTH//2 + 20, 470, WIDTH//2 + 220, 520], radius=8, fill=(255, 255, 255, 15), outline=(255, 255, 255, 40), width=1)
        draw.text((WIDTH//2 + 45, 485), "Schedule Session", fill=(255, 255, 255), font=FONT_SUBTITLE)
        
        draw_subtitles(draw, scene["subtitle"])
        
    elif scene["type"] == "product":
        if os.path.exists(scene["image"]):
            screen_img = Image.open(scene["image"]).convert("RGBA")
            sw, sh = screen_img.size
            crop_box = (
                int(sw * (0.04 * progress)),
                int(sh * (0.04 * progress)),
                int(sw * (1.0 - 0.04 * progress)),
                int(sh * (1.0 - 0.04 * progress))
            )
            cropped = screen_img.crop(crop_box).resize((WIDTH, HEIGHT - 48), Image.Resampling.LANCZOS)
            img.paste(cropped, (0, 48))
            
            # Subtle dim mask
            dim_layer = Image.new("RGBA", (WIDTH, HEIGHT), (8, 12, 16, 140))
            img = Image.alpha_composite(img, dim_layer)
            draw = ImageDraw.Draw(img)
            
            # Laser spotlight frame
            sp = scene["spotlight"]
            draw.rounded_rectangle([sp[0], sp[1], sp[0] + sp[2], sp[1] + sp[3]], radius=8, outline=(0, 209, 178, 220), width=2)
            
        draw_sleek_ribbon(draw, scene["active_stage"])
        
        # Stage Header Overlay
        draw.text((36, 68), scene["stage_tag"], fill=(0, 209, 178), font=FONT_TAG)
        draw.text((36, 88), scene["headline"], fill=(255, 255, 255), font=FONT_HEADLINE)
        
        # Animated Callout Cards
        if progress > 0.12:
            for callout in scene["callouts"]:
                cx, cy, cw, ch = callout["x"], callout["y"], callout["w"], callout["h"]
                draw.rounded_rectangle([cx, cy, cx + cw, cy + ch], radius=8, fill=(12, 20, 30, 245), outline=(0, 209, 178, 130), width=1)
                
                # Indicator dot
                draw.ellipse([cx + 14, cy + 18, cx + 22, cy + 26], fill=(0, 209, 178))
                draw.text((cx + 32, cy + 12), callout["title"], fill=(255, 255, 255), font=FONT_CALLOUT_H)
                draw.text((cx + 14, cy + 44), callout["desc"], fill=(203, 213, 225), font=FONT_CALLOUT_P)
                
        draw_subtitles(draw, scene["subtitle"])

    return cv2.cvtColor(np.array(img.convert("RGB")), cv2.COLOR_RGB2BGR)

def generate_video():
    print(f"Generating Premium LG Issuance Demo Video: {OUTPUT_VIDEO_PATH}")
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(OUTPUT_VIDEO_PATH, fourcc, FPS, (WIDTH, HEIGHT))
    
    total_frames = 0
    for scene in SCENES:
        frames_count = int(scene["duration_sec"] * FPS)
        total_frames += frames_count
        
    current_frame = 0
    for scene in SCENES:
        scene_frames = int(scene["duration_sec"] * FPS)
        for f in range(scene_frames):
            prog = f / float(scene_frames)
            frame = render_frame(scene, prog)
            out.write(frame)
            current_frame += 1
                
    out.release()
    print("Video frame generation completed!")

if __name__ == "__main__":
    generate_video()
