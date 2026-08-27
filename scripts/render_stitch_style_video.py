"""
GROW BD — STITCH DARK SAAS STYLE COMPLETE VIDEO GENERATOR
- Renders the entire 7-stage LG Issuance demo matching the user-approved Stitch Dark design.
- Alternating Left/Right layouts for visual rhythm.
- Real UI screenshots in 100% natural brightness inside clean browser mockups.
- 3 Bottom feature cards per stage with circular icon badges.
- Floating bottom voiceover pill dock.
- Full Audio Muxing: ElevenLabs Voiceover files + Corporate Ambient BGM mixed with FFmpeg.
"""

import os
import cv2
import subprocess
import numpy as np
from PIL import Image, ImageDraw, ImageFont
import imageio_ffmpeg
from mutagen.mp3 import MP3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "interactive-experience", "assets", "screenshots")
SAMPLES_DIR = os.path.join(BASE_DIR, "audio_assets", "style_samples")
OUTPUT_VIDEO_PATH = os.path.join(BASE_DIR, "LG_Issuance_Demo_WhatsApp.mp4")
VOICEOVER_DIR = os.path.join(BASE_DIR, "audio_assets", "voiceover")
BGM_PATH = os.path.join(BASE_DIR, "audio_assets", "bgm", "corporate_ambient_pad.wav")
TEMP_VIDEO_SILENT = os.path.join(BASE_DIR, "audio_assets", "temp_silent_video.mp4")
TEMP_AUDIO_MIXED = os.path.join(BASE_DIR, "audio_assets", "temp_mixed_audio.wav")

os.makedirs(SAMPLES_DIR, exist_ok=True)

WIDTH = 1280
HEIGHT = 720
FPS = 30

# Colors matching the Stitch dark design
BG_COLOR = (9, 13, 21)           # Deep SaaS Navy
CARD_BG = (14, 22, 34)           # Dark Slate Card
CARD_BORDER = (28, 42, 60)       # Subtle border
CYAN = (0, 229, 204)             # Vivid Cyan/Teal
SKY = (56, 189, 248)             # Sky Blue
TEXT_WHITE = (255, 255, 255)
TEXT_MUTED = (148, 163, 184)
TEXT_DIM = (100, 116, 139)

def get_font(size, bold=False):
    font_names = ["arialbd.ttf" if bold else "arial.ttf", "segoeuib.ttf" if bold else "segoeui.ttf"]
    for fn in font_names:
        try:
            return ImageFont.truetype(fn, size)
        except:
            continue
    return ImageFont.load_default()

FONT_LOGO = get_font(18, bold=True)
FONT_STAGE_TAG = get_font(11, bold=True)
FONT_TAG = get_font(13, bold=True)
FONT_HERO_TITLE = get_font(30, bold=True)
FONT_HERO_DESC = get_font(14, bold=False)
FONT_BTN = get_font(13, bold=True)
FONT_CARD_TITLE = get_font(14, bold=True)
FONT_CARD_DESC = get_font(11, bold=False)
FONT_SUBTITLE = get_font(14, bold=False)

STAGES_DATA = [
    # Scene 0: Opening
    {
        "type": "opening",
        "stage_num": "00",
        "stage_name": "OVERVIEW",
        "title": "Letters of Guarantee",
        "subtitle_text": "An LG doesn't start with a bank. It starts with a business requirement. Managing that requirement shouldn't mean chasing emails, spreadsheets and approvals. It should be one controlled process.",
        "voiceover_file": "00_opening.mp3",
        "hero_desc": "From business requirement to bank issuance. Control every step of the LG lifecycle in one governed enterprise workflow.",
        "btn_label": "Start Journey",
        "cards": [
            {"title": "Intelligent Intake", "desc": "Structured capture of contract terms, clauses, and expiry dates."},
            {"title": "Bank Coordination", "desc": "Integrated RFQ comparison and bank application auto-filling."},
            {"title": "Governance & Rec", "desc": "Threshold-based sign-offs and automated bank position matching."}
        ]
    },
    # Stage 1: Facility Lines (Left Info, Right Mockup)
    {
        "type": "split_left",
        "stage_num": "01",
        "stage_name": "FACILITY LINES",
        "title": "Facility Lines",
        "hero_desc": "Know your capacity before issuing. Aggregate and monitor your credit lines across multiple banking partners in real-time.",
        "btn_label": "Explore Capacity",
        "image": os.path.join(SCREENSHOTS_DIR, "Facilities.jpg"),
        "voiceover_file": "01_facility.mp3",
        "subtitle_text": "Start with the facility. Know your available capacity before the request moves forward.",
        "cards": [
            {"title": "Multi-Bank Lines", "desc": "Consolidate credit facilities from distinct financial institutions into a single view."},
            {"title": "Sub-limit Control", "desc": "Track specific sub-limits for LC/LG issuance, bid bonds, and advance payments."},
            {"title": "Margin Tracking", "desc": "Monitor facility headroom and margin requirements dynamically as limits fluctuate."}
        ]
    },
    # Stage 2: Issuance Request (Right Info, Left Mockup - Mirrored Variation!)
    {
        "type": "split_right",
        "stage_num": "02",
        "stage_name": "ISSUANCE REQUEST",
        "title": "Digital Intake",
        "hero_desc": "Structure the request. Turn operational contracts into formal treasury requests with zero wording errors.",
        "btn_label": "Create Request",
        "image": os.path.join(SCREENSHOTS_DIR, "Request_form.jpg"),
        "voiceover_file": "02_request.mp3",
        "subtitle_text": "Turn the business requirement into a structured issuance request.",
        "cards": [
            {"title": "Standardized Forms", "desc": "Capture beneficiary details, project codes, amount, and expiry dates."},
            {"title": "Wording Library", "desc": "Pre-approved corporate wording templates for Bid and Performance guarantees."},
            {"title": "Headroom Check", "desc": "Automatic pre-validation against available banking line limits."}
        ]
    },
    # Stage 3: Communication (Left Info, Right Mockup)
    {
        "type": "split_left",
        "stage_num": "03",
        "stage_name": "BANK COMMUNICATION",
        "title": "Bank Interaction",
        "hero_desc": "Manage the bank interaction. Connect directly with partner banks to compare quotes and auto-fill applications.",
        "btn_label": "Compare Quotes",
        "image": os.path.join(SCREENSHOTS_DIR, "Issuance_Follow_Up.jpg"),
        "voiceover_file": "03_communication.mp3",
        "subtitle_text": "Manage the bank interaction and issuance options within the workflow.",
        "cards": [
            {"title": "RFQ & Rate Quotes", "desc": "Receive competitive bank commission rates and fee structures in one place."},
            {"title": "Bank Form Auto-Fill", "desc": "Direct mapping to official bank application forms without duplicate data entry."},
            {"title": "Status Tracking", "desc": "Track issuance status from submission to active processing."}
        ]
    },
    # Stage 4: Approval (Right Info, Left Mockup - Mirrored Variation!)
    {
        "type": "split_right",
        "stage_num": "04",
        "stage_name": "INTERNAL APPROVAL",
        "title": "Approval Center",
        "hero_desc": "Control before issuance. Apply multi-tier signing limits and governance checks before transmission to the bank.",
        "btn_label": "Review Approvals",
        "image": os.path.join(SCREENSHOTS_DIR, "Approval Center_D.jpg"),
        "voiceover_file": "04_approval.mp3",
        "subtitle_text": "Apply the right controls before the LG reaches the bank.",
        "cards": [
            {"title": "Threshold Routing", "desc": "Rule-based approval matrices based on amount thresholds and entity policies."},
            {"title": "Discrepancy Checks", "desc": "Automated verification flags unexpected variances before authorization."},
            {"title": "Full Audit Trail", "desc": "Complete immutable timestamped history for compliance and governance."}
        ]
    },
    # Stage 5: Issuance (Left Info, Right Mockup)
    {
        "type": "split_left",
        "stage_num": "05",
        "stage_name": "BANK ISSUANCE",
        "title": "Issued LG Details",
        "hero_desc": "Move from request to reality. Capture verified bank reference numbers and track active financial commitments.",
        "btn_label": "View Issued LG",
        "image": os.path.join(SCREENSHOTS_DIR, "Issued_LG_Details.jpg"),
        "voiceover_file": "05_issuance.mp3",
        "subtitle_text": "Move from approved request to actual issuance with full visibility.",
        "cards": [
            {"title": "Verified Bank Ref", "desc": "Formal bank guarantee reference registered with issuing branch and date."},
            {"title": "Cash Margin Lock", "desc": "Automatic visibility into collateral blocked and release conditions."},
            {"title": "Document Vault", "desc": "Secure digital attachment of bank-issued guarantee copies."}
        ]
    },
    # Stage 6: Maintenance (Right Info, Left Mockup - Mirrored Variation!)
    {
        "type": "split_right",
        "stage_num": "06",
        "stage_name": "LIFECYCLE MAINTENANCE",
        "title": "Action Center",
        "hero_desc": "Control the LG throughout its lifecycle. Execute extensions, reductions, and governed releases proactively.",
        "btn_label": "Manage Lifecycle",
        "image": os.path.join(SCREENSHOTS_DIR, "Action_Center.jpg"),
        "voiceover_file": "06_maintenance.mp3",
        "subtitle_text": "And once issued, keep control throughout the LG lifecycle.",
        "cards": [
            {"title": "Lifecycle Actions", "desc": "One-click workflows for LG extensions, value amendments, and transfers."},
            {"title": "Expiry Defense", "desc": "Proactive 30, 60, and 90-day alerts prevent costly penalty charges."},
            {"title": "Release & Return", "desc": "Controlled release verification and return of bank liability."}
        ]
    },
    # Stage 7: Reconciliation (Left Info, Right Mockup)
    {
        "type": "split_left",
        "stage_num": "07",
        "stage_name": "RECONCILIATION",
        "title": "Bank vs System Position",
        "hero_desc": "Keep the system position aligned with the bank position. Reconcile records to detect variances and close gaps.",
        "btn_label": "Reconcile Positions",
        "image": os.path.join(SCREENSHOTS_DIR, "Reconciliation.jpg"),
        "voiceover_file": "07_reconciliation.mp3",
        "subtitle_text": "Finally, reconcile the bank LG position with the system position and identify any gaps.",
        "cards": [
            {"title": "Automated Matching", "desc": "Instantly compares Bank LG Position against System LG Position."},
            {"title": "Variance Detection", "desc": "Identifies amount differences, expiry shifts, and unrecorded items."},
            {"title": "Resolution Engine", "desc": "Structured exception handling to adjust and verify internal treasury books."}
        ]
    },
    # Scene 8: Closing Finale
    {
        "type": "closing",
        "stage_num": "08",
        "stage_name": "TREASURY PLATFORM",
        "title": "Maintain Control Over Every LG",
        "hero_desc": "From request to issuance. From issuance to lifecycle. The complete connected Treasury workflow.",
        "btn_label": "Get Sandbox Access",
        "voiceover_file": "08_closing.mp3",
        "subtitle_text": "From request to issuance. From issuance to lifecycle. Maintain control over every LG.",
        "cards": [
            {"title": "Full Visibility", "desc": "Single source of truth across all banking relationships."},
            {"title": "Zero Spreadsheets", "desc": "Eliminate manual data tracking and fragmented emails."},
            {"title": "Audit-Ready", "desc": "Continuous compliance, approval history, and bank reconciliation."}
        ]
    }
]

def get_audio_duration(vo_file):
    fpath = os.path.join(VOICEOVER_DIR, vo_file)
    if os.path.exists(fpath):
        try:
            return MP3(fpath).info.length
        except:
            return 5.0
    return 5.0

def draw_header(draw):
    draw.rounded_rectangle([36, 18, 56, 38], radius=4, fill=(0, 229, 204))
    draw.text((42, 20), "G", fill=(8, 14, 22), font=FONT_TAG)
    draw.text((64, 20), "Grow Treasury", fill=TEXT_WHITE, font=FONT_LOGO)
    draw.text((WIDTH - 240, 22), "Overview  Facility Lines  Reports", fill=TEXT_DIM, font=FONT_CARD_DESC)

def draw_subtitle_dock(draw, text):
    sub_w = 980
    sub_h = 42
    x1 = (WIDTH - sub_w) // 2
    y1 = HEIGHT - 58
    draw.rounded_rectangle([x1, y1, x1 + sub_w, y1 + sub_h], radius=21, fill=(12, 18, 28, 245), outline=(0, 229, 204, 130), width=1)
    
    draw.ellipse([x1 + 20, y1 + 16, x1 + 30, y1 + 26], fill=CYAN)
    
    bbox = draw.textbbox((0, 0), text, font=FONT_SUBTITLE)
    text_w = bbox[2] - bbox[0]
    tx = max(x1 + 45, x1 + (sub_w - text_w) // 2)
    draw.text((tx, y1 + 12), f'"{text}"', fill=(226, 232, 240), font=FONT_SUBTITLE)

def draw_bottom_cards(draw, cards, y=490):
    card_w = 380
    card_h = 95
    gap = 25
    start_x = 36
    
    for i, c in enumerate(cards):
        cx = start_x + i * (card_w + gap)
        draw.rounded_rectangle([cx, y, cx + card_w, y + card_h], radius=8, fill=CARD_BG, outline=CARD_BORDER, width=1)
        
        # Circle icon badge
        draw.ellipse([cx + 14, y + 14, cx + 42, y + 42], fill=(0, 229, 204, 25), outline=(0, 229, 204, 90), width=1)
        draw.text((cx + 24, y + 20), "*", fill=CYAN, font=FONT_TAG)
        
        draw.text((cx + 52, y + 16), c["title"], fill=TEXT_WHITE, font=FONT_CARD_TITLE)
        
        words = c["desc"].split(" ")
        line1 = " ".join(words[:6])
        line2 = " ".join(words[6:])
        draw.text((cx + 14, y + 50), line1, fill=TEXT_MUTED, font=FONT_CARD_DESC)
        if line2:
            draw.text((cx + 14, y + 68), line2, fill=TEXT_MUTED, font=FONT_CARD_DESC)

def render_scene_frame(scene, progress=0.0):
    img = Image.new("RGBA", (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)
    
    draw_header(draw)
    
    stype = scene["type"]
    
    if stype == "split_left" or stype == "split_right":
        is_left = (stype == "split_left")
        
        tx = 36 if is_left else 874
        mx = 436 if is_left else 36
        mw = 808
        mh = 390
        
        # Stage Badge (Crisp Cyan on Dark)
        draw.rounded_rectangle([tx, 72, tx + 88, 96], radius=12, fill=(14, 26, 38), outline=(0, 229, 204), width=1)
        draw.text((tx + 14, 78), f"STAGE {scene['stage_num']}", fill=CYAN, font=FONT_STAGE_TAG)
        
        # Hero Title
        draw.text((tx, 110), scene["title"], fill=CYAN, font=FONT_HERO_TITLE)
        
        # Description
        desc_words = scene["hero_desc"].split(" ")
        line1 = " ".join(desc_words[:5])
        line2 = " ".join(desc_words[5:11])
        line3 = " ".join(desc_words[11:])
        
        draw.text((tx, 160), line1, fill=TEXT_WHITE, font=FONT_HERO_DESC)
        draw.text((tx, 182), line2, fill=TEXT_MUTED, font=FONT_HERO_DESC)
        draw.text((tx, 204), line3, fill=TEXT_MUTED, font=FONT_HERO_DESC)
        
        # Button
        draw.rounded_rectangle([tx, 248, tx + 160, 286], radius=5, fill=CYAN)
        draw.text((tx + 18, 258), scene["btn_label"], fill=(8, 14, 22), font=FONT_BTN)
        
        # Mockup Frame
        draw.rounded_rectangle([mx, 72, mx + mw, 72 + mh], radius=8, fill=(13, 19, 29), outline=(0, 229, 204, 90), width=1)
        draw.ellipse([mx + 12, 82, mx + 20, 90], fill=(239, 68, 68))
        draw.ellipse([mx + 26, 82, mx + 34, 90], fill=(245, 158, 11))
        draw.ellipse([mx + 40, 82, mx + 48, 90], fill=(16, 185, 129))
        draw.text((mx + 58, 78), "Grow BD Treasury Engine", fill=TEXT_DIM, font=FONT_CARD_DESC)
        
        # Real Screenshot with subtle progressive zoom
        if os.path.exists(scene["image"]):
            screen = Image.open(scene["image"]).convert("RGBA")
            sw, sh = screen.size
            zoom_offset = 0.03 * progress
            cropped = screen.crop((
                int(sw * zoom_offset),
                int(sh * (0.08 + zoom_offset)),
                int(sw * (1.0 - zoom_offset)),
                int(sh * (0.92 - zoom_offset))
            )).resize((mw - 4, mh - 30), Image.Resampling.LANCZOS)
            img.paste(cropped, (mx + 2, 98))
            
        draw_bottom_cards(draw, scene["cards"], y=485)
        draw_subtitle_dock(draw, scene["subtitle_text"])
        
    elif stype == "opening" or stype == "closing":
        draw.rounded_rectangle([WIDTH//2 - 50, 72, WIDTH//2 + 50, 96], radius=12, fill=(14, 26, 38), outline=(0, 229, 204), width=1)
        draw.text((WIDTH//2 - 36, 78), scene["stage_name"], fill=CYAN, font=FONT_STAGE_TAG)
        
        draw.text((WIDTH//2 - 200, 115), scene["title"], fill=CYAN, font=FONT_HERO_TITLE)
        draw.text((WIDTH//2 - 310, 168), scene["hero_desc"], fill=TEXT_WHITE, font=FONT_HERO_DESC)
        
        draw.rounded_rectangle([WIDTH//2 - 90, 215, WIDTH//2 + 90, 255], radius=5, fill=CYAN)
        draw.text((WIDTH//2 - 60, 226), scene["btn_label"], fill=(8, 14, 22), font=FONT_BTN)
        
        draw_bottom_cards(draw, scene["cards"], y=330)
        draw_subtitle_dock(draw, scene["subtitle_text"])

    return cv2.cvtColor(np.array(img.convert("RGB")), cv2.COLOR_RGB2BGR)

def build_audio_track(scenes):
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    print("Building audio track with FFmpeg...")
    
    # 1. Prepare concat list of voiceovers with precise padding
    concat_txt_path = os.path.join(BASE_DIR, "audio_assets", "voiceover_concat.txt")
    temp_silence = os.path.join(BASE_DIR, "audio_assets", "silence_05.wav")
    
    # Generate 0.5s silence
    cmd_silence = [ffmpeg_exe, "-y", "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo", "-t", "0.5", temp_silence]
    subprocess.run(cmd_silence, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    with open(concat_txt_path, "w", encoding="utf-8") as f:
        silence_norm = temp_silence.replace("\\", "/")
        for scene in scenes:
            vo_path = os.path.join(VOICEOVER_DIR, scene["voiceover_file"]).replace("\\", "/")
            f.write(f"file '{vo_path}'\n")
            f.write(f"file '{silence_norm}'\n")
            
    # Concat all voiceovers into one WAV
    temp_vo_combined = os.path.join(BASE_DIR, "audio_assets", "temp_vo_combined.wav")
    cmd_concat = [ffmpeg_exe, "-y", "-f", "concat", "-safe", "0", "-i", concat_txt_path, "-c:a", "pcm_s16le", temp_vo_combined]
    subprocess.run(cmd_concat, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Overlay Background Music (ducked by -16dB)
    cmd_mix = [
        ffmpeg_exe, "-y",
        "-i", temp_vo_combined,
        "-i", BGM_PATH,
        "-filter_complex", "[1:a]volume=0.18[bgm];[0:a][bgm]amix=inputs=2:duration=first[aout]",
        "-map", "[aout]",
        "-c:a", "pcm_s16le",
        TEMP_AUDIO_MIXED
    ]
    subprocess.run(cmd_mix, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("Mixed audio track ready:", TEMP_AUDIO_MIXED)

def generate_video():
    print(f"Starting Video Generation...")
    
    # Calculate scene durations from voiceover files
    for scene in STAGES_DATA:
        vo_dur = get_audio_duration(scene["voiceover_file"])
        # Give duration = voiceover + 0.5s breathing pause
        scene["duration_sec"] = vo_dur + 0.5
        print(f"Scene {scene['stage_num']} ({scene['title']}): {scene['duration_sec']:.2f}s")
        
    total_sec = sum(s["duration_sec"] for s in STAGES_DATA)
    total_frames = int(total_sec * FPS)
    print(f"Total Video Length: {total_sec:.2f}s ({total_frames} frames)")
    
    # Build mixed audio
    build_audio_track(STAGES_DATA)
    
    # Render video stream
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(TEMP_VIDEO_SILENT, fourcc, FPS, (WIDTH, HEIGHT))
    
    current_frame = 0
    for scene in STAGES_DATA:
        scene_frames = int(scene["duration_sec"] * FPS)
        for f in range(scene_frames):
            prog = f / float(scene_frames)
            frame = render_scene_frame(scene, prog)
            out.write(frame)
            current_frame += 1
            if current_frame % 90 == 0:
                print(f"Rendering: {current_frame}/{total_frames} frames ({(current_frame*100)//total_frames}%)")
                
    out.release()
    print("Silent video rendered successfully.")
    
    # Final Mux with FFmpeg (H.264 + AAC MP4)
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
    print(f"Final Premium Video Generated: {OUTPUT_VIDEO_PATH}")
    file_size_mb = os.path.getsize(OUTPUT_VIDEO_PATH) / (1024 * 1024)
    print(f"Final MP4 Size: {file_size_mb:.2f} MB (Optimized for WhatsApp!)")

if __name__ == "__main__":
    generate_video()
