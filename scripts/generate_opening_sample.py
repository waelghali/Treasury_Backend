"""
Generate sample still for the redesigned Opening Screen
"""

import os
from PIL import Image, ImageDraw, ImageFont

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES_DIR = os.path.join(BASE_DIR, "audio_assets", "style_samples")
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

FONT_H1 = get_font(34, bold=True)
FONT_SUB = get_font(19, bold=False)
FONT_TAG = get_font(13, bold=True)

def render_opening():
    img = Image.new("RGBA", (WIDTH, HEIGHT), (8, 14, 22, 255))
    draw = ImageDraw.Draw(img)
    
    # Sleek Ribbon
    draw.rectangle([0, 0, WIDTH, 48], fill=(10, 16, 24, 240))
    draw.line([(0, 48), (WIDTH, 48)], fill=(255, 255, 255, 20), width=1)
    draw.ellipse([26, 21, 34, 29], fill=(0, 209, 178))
    draw.text((42, 16), "LG ISSUANCE", fill=(0, 209, 178), font=FONT_TAG)
    draw.text((220, 16), "01 Facility  ›  02 Request  ›  03 Communication  ›  04 Approval  ›  05 Issuance  ›  06 Maintenance  ›  07 Reconciliation", fill=(148, 163, 184), font=FONT_TAG)
    
    # Elegant Kicker
    draw.text((WIDTH//2 - 150, 130), "GROW BD TREASURY PLATFORM", fill=(56, 189, 248), font=FONT_TAG)
    
    # Headline
    draw.text((WIDTH//2 - 380, 175), "An LG doesn't start with a bank.", fill=(255, 255, 255), font=FONT_H1)
    draw.text((WIDTH//2 - 380, 225), "It starts with a business requirement.", fill=(0, 209, 178), font=FONT_H1)
    
    # Sleek Minimalist Friction Cards (Zero Red Pills)
    frictions = [
        "Scattered Emails & Manual Follow-ups",
        "Disconnected Spreadsheets & Static Registers",
        "Unverified Facility Headroom & Limit Risks"
    ]
    card_w = 780
    card_h = 46
    start_y = 310
    for i, friction in enumerate(frictions):
        cy = start_y + i * 56
        draw.rounded_rectangle([WIDTH//2 - card_w//2, cy, WIDTH//2 + card_w//2, cy + card_h], radius=8, fill=(15, 23, 34, 220), outline=(255, 255, 255, 25), width=1)
        draw.ellipse([WIDTH//2 - card_w//2 + 18, cy + 19, WIDTH//2 - card_w//2 + 26, cy + 27], fill=(56, 189, 248))
        draw.text((WIDTH//2 - card_w//2 + 38, cy + 12), friction, fill=(203, 213, 225), font=FONT_SUB)
        
    # Controlled Solution Banner
    sol_text = "From Request to Issuance — Controlled in One Workflow"
    draw.rounded_rectangle([WIDTH//2 - 280, 500, WIDTH//2 + 280, 548], radius=24, fill=(0, 209, 178, 255), outline=(0, 209, 178, 255), width=1)
    draw.text((WIDTH//2 - 240, 513), sol_text, fill=(8, 14, 22), font=FONT_SUB)
    
    # Subtitle
    sub_w = 1080
    sub_h = 44
    x1 = (WIDTH - sub_w) // 2
    y1 = HEIGHT - 64
    draw.rounded_rectangle([x1, y1, x1 + sub_w, y1 + sub_h], radius=22, fill=(8, 12, 18, 235), outline=(255, 255, 255, 30), width=1)
    draw.text((x1 + 40, y1 + 10), "An LG doesn't start with a bank. It starts with a business requirement. Managing that requirement shouldn't mean chasing emails.", fill=(248, 250, 252), font=FONT_SUB)
    
    out_path = os.path.join(SAMPLES_DIR, "opening_redesigned_sample.png")
    img.save(out_path)
    print(f"Saved Opening Sample to {out_path}")

if __name__ == "__main__":
    render_opening()
