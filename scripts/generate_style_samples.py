"""
Generate 3 distinct sample style still images for user review:
Style 1: Clean Studio Hero (Exact mirror of the web presentation - bright UI, subtle glow)
Style 2: Split Canvas / Editorial (Sidebar on left, 100% clean unobstructed UI on right)
Style 3: Pure Full-Screen Cinematic (Natural vibrant UI with bottom caption bar only, zero overlay)
"""

import os
import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "interactive-experience", "assets", "screenshots")
SAMPLES_DIR = os.path.join(BASE_DIR, "audio_assets", "style_samples")
os.makedirs(SAMPLES_DIR, exist_ok=True)

facility_img_path = os.path.join(SCREENSHOTS_DIR, "Facilities.jpg")
WIDTH = 1280
HEIGHT = 720

def get_font(size, bold=False):
    font_names = ["arialbd.ttf" if bold else "arial.ttf", "segoeuib.ttf" if bold else "segoeui.ttf"]
    for fn in font_names:
        try:
            return ImageFont.truetype(fn, size)
        except:
            continue
    return ImageFont.load_default()

FONT_H1 = get_font(28, bold=True)
FONT_H2 = get_font(20, bold=True)
FONT_BODY = get_font(15, bold=False)
FONT_SUB = get_font(18, bold=False)
FONT_TAG = get_font(13, bold=True)

# ----------------------------------------------------
# STYLE 1: Clean Studio Hero (Mirrors Interactive Web Demo)
# ----------------------------------------------------
def render_style_1():
    img = Image.new("RGBA", (WIDTH, HEIGHT), (12, 18, 26, 255))
    draw = ImageDraw.Draw(img)
    
    # 1. Top Sleek Ribbon
    draw.rectangle([0, 0, WIDTH, 48], fill=(16, 24, 34, 255))
    draw.line([(0, 48), (WIDTH, 48)], fill=(255, 255, 255, 25), width=1)
    draw.text((24, 15), "LG ISSUANCE", fill=(0, 209, 178), font=FONT_TAG)
    draw.text((140, 15), "01 Facility  ›  02 Request  ›  03 Communication  ›  04 Approval  ›  05 Issuance  ›  06 Maintenance  ›  07 Reconciliation", fill=(148, 163, 184), font=FONT_TAG)
    
    # 2. Real screenshot (100% natural brightness, framed in sleek bezel)
    if os.path.exists(facility_img_path):
        screen = Image.open(facility_img_path).convert("RGBA")
        # Resize to fit frame with padding
        sw, sh = screen.size
        # Crop to upper facility metrics
        cropped = screen.crop((0, int(sh * 0.12), sw, int(sh * 0.85))).resize((1200, 560), Image.Resampling.LANCZOS)
        
        # Paste with drop shadow
        img.paste(cropped, (40, 68))
        draw.rounded_rectangle([40, 68, 1240, 628], radius=8, outline=(0, 209, 178, 180), width=2)
        
    # 3. Stage title overlay (Top-Left, unobtrusive)
    draw.rounded_rectangle([60, 88, 380, 150], radius=8, fill=(10, 16, 24, 220), outline=(0, 209, 178, 100), width=1)
    draw.text((76, 96), "STAGE 1 — FACILITY", fill=(0, 209, 178), font=FONT_TAG)
    draw.text((76, 116), "Know your capacity.", fill=(255, 255, 255), font=FONT_H2)
    
    # 4. Bottom Subtitle Ribbon (Floating, clean)
    draw.rounded_rectangle([140, 648, 1140, 698], radius=25, fill=(10, 16, 24, 240), outline=(255, 255, 255, 30), width=1)
    draw.text((180, 662), "Start with the facility. Know your available capacity before the request moves forward.", fill=(241, 245, 249), font=FONT_SUB)
    
    out_path = os.path.join(SAMPLES_DIR, "style_1_clean_studio.png")
    img.save(out_path)
    print(f"Saved Style 1 to {out_path}")

# ----------------------------------------------------
# STYLE 2: Split Canvas (Sidebar on Left, 100% Clean UI on Right)
# ----------------------------------------------------
def render_style_2():
    img = Image.new("RGBA", (WIDTH, HEIGHT), (10, 16, 24, 255))
    draw = ImageDraw.Draw(img)
    
    # Left Sidebar (360px wide)
    draw.rectangle([0, 0, 360, HEIGHT], fill=(14, 22, 32, 255))
    draw.line([(360, 0), (360, HEIGHT)], fill=(0, 209, 178, 60), width=2)
    
    # Sidebar Branding & Step
    draw.text((32, 36), "GROW BD TREASURY", fill=(56, 189, 248), font=FONT_TAG)
    draw.text((32, 70), "STAGE 01", fill=(0, 209, 178), font=FONT_H1)
    draw.text((32, 110), "Facility Lines", fill=(255, 255, 255), font=FONT_H1)
    
    draw.text((32, 160), "Know your capacity.", fill=(203, 213, 225), font=FONT_H2)
    draw.line([(32, 196), (320, 196)], fill=(255, 255, 255, 20), width=1)
    
    # Feature Bullet Points in Sidebar (Zero overlap with image)
    bullets = [
        ("Multi-Bank Lines", "Real-time headroom check across partner banks."),
        ("Sub-limit Control", "Enforce caps for Bid, Performance & Advance LGs."),
        ("Margin Tracking", "Monitor cash cover & blocked collaterals.")
    ]
    
    by = 220
    for title, desc in bullets:
        draw.ellipse([32, by + 6, 40, by + 14], fill=(0, 209, 178))
        draw.text((50, by), title, fill=(255, 255, 255), font=FONT_H2)
        draw.text((50, by + 28), desc, fill=(148, 163, 184), font=FONT_BODY)
        by += 80
        
    # Subtitle in Sidebar bottom
    draw.rounded_rectangle([24, HEIGHT - 130, 336, HEIGHT - 30], radius=8, fill=(20, 31, 44, 255), outline=(0, 209, 178, 80), width=1)
    draw.text((36, HEIGHT - 118), "VOICEOVER", fill=(0, 209, 178), font=FONT_TAG)
    draw.text((36, HEIGHT - 96), "Start with the facility. Know your\navailable capacity before the request.", fill=(226, 232, 240), font=FONT_BODY)
    
    # Right Side: 100% Pristine Unobstructed Screenshot (880px wide)
    if os.path.exists(facility_img_path):
        screen = Image.open(facility_img_path).convert("RGBA")
        sw, sh = screen.size
        cropped = screen.crop((0, int(sh * 0.1), sw, int(sh * 0.9))).resize((880, 660), Image.Resampling.LANCZOS)
        img.paste(cropped, (380, 30))
        draw.rounded_rectangle([380, 30, 1260, 690], radius=8, outline=(255, 255, 255, 30), width=1)
        
    out_path = os.path.join(SAMPLES_DIR, "style_2_split_canvas.png")
    img.save(out_path)
    print(f"Saved Style 2 to {out_path}")

# ----------------------------------------------------
# STYLE 3: Pure Full-Screen UI (Vibrant Screenshot, Minimalist Top/Bottom Bars)
# ----------------------------------------------------
def render_style_3():
    img = Image.new("RGBA", (WIDTH, HEIGHT), (8, 12, 16, 255))
    draw = ImageDraw.Draw(img)
    
    # Full screen real image in natural colors
    if os.path.exists(facility_img_path):
        screen = Image.open(facility_img_path).convert("RGBA")
        resized = screen.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS)
        img.paste(resized, (0, 0))
        
    # Top Subtle Gradient Bar
    draw.rectangle([0, 0, WIDTH, 60], fill=(10, 16, 24, 220))
    draw.line([(0, 60), (WIDTH, 60)], fill=(0, 209, 178, 100), width=1)
    
    draw.text((30, 20), "01 FACILITY", fill=(0, 209, 178), font=FONT_TAG)
    draw.text((130, 16), "Know your capacity before issuing.", fill=(255, 255, 255), font=FONT_H2)
    
    # Bottom Subtitle Bar only
    draw.rectangle([0, HEIGHT - 60, WIDTH, HEIGHT], fill=(10, 16, 24, 230))
    draw.line([(0, HEIGHT - 60), (WIDTH, HEIGHT - 60)], fill=(255, 255, 255, 20), width=1)
    draw.text((WIDTH//2 - 380, HEIGHT - 42), "Start with the facility. Know your available capacity before the request moves forward.", fill=(255, 255, 255), font=FONT_SUB)
    
    out_path = os.path.join(SAMPLES_DIR, "style_3_pure_fullscreen.png")
    img.save(out_path)
    print(f"Saved Style 3 to {out_path}")

if __name__ == "__main__":
    render_style_1()
    render_style_2()
    render_style_3()
