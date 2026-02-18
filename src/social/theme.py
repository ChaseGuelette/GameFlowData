"""Theme constants and drawing helpers for social media cards.

Colors match the dashboard's Tailwind dark theme. Font weights map to
Montserrat static files in assets/fonts/.
"""

from __future__ import annotations

import math
import os
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FONTS_DIR = Path(__file__).resolve().parents[2] / "assets" / "fonts"

# ---------------------------------------------------------------------------
# Color Palette (RGB tuples matching dashboard Tailwind classes)
# ---------------------------------------------------------------------------
BG_PRIMARY = (2, 6, 23)  # slate-950  #020617
BG_CARD = (30, 41, 59)  # slate-800  #1E293B
BG_ROW_ALT = (51, 65, 85)  # slate-700  #334155

TEXT_PRIMARY = (248, 250, 252)  # slate-50   #F8FAFC
TEXT_SECONDARY = (148, 163, 184)  # slate-400  #94A3B8
TEXT_MUTED = (100, 116, 139)  # slate-500  #64748B

# Edge tier colours
GREEN = (34, 197, 94)  # #22C55E  high edge (>=7%)
YELLOW = (234, 179, 8)  # #EAB308  medium edge (>=5%)
SLATE = (100, 116, 139)  # #64748B  low edge (<5%)

# Stat badge colours
BLUE = (59, 130, 246)  # #3B82F6  PTS
TEAL = (20, 184, 166)  # #14B8A6  REB
PURPLE = (168, 85, 247)  # #A855F7  AST

STAT_COLORS = {"pts": BLUE, "reb": TEAL, "ast": PURPLE}

# Result colours
WON_GREEN = (34, 197, 94)  # #22C55E
LOST_RED = (239, 68, 68)  # #EF4444
PUSH_GRAY = (148, 163, 184)  # #94A3B8

# Branding
AMBER = (217, 119, 6)  # #D97706
GOLD = (255, 215, 0)  # #FFD700

# Star colour
STAR_ACTIVE = (250, 204, 21)  # yellow-400
STAR_INACTIVE = (71, 85, 105)  # slate-600

# ---------------------------------------------------------------------------
# Font helpers
# ---------------------------------------------------------------------------
FONT_WEIGHTS = {
    "bold": "Montserrat-Bold.ttf",
    "semibold": "Montserrat-SemiBold.ttf",
    "medium": "Montserrat-Medium.ttf",
}


@lru_cache(maxsize=32)
def get_font(weight: str = "medium", size: int = 24) -> ImageFont.FreeTypeFont:
    """Return a cached Pillow FreeTypeFont for the given weight and size."""
    filename = FONT_WEIGHTS.get(weight, FONT_WEIGHTS["medium"])
    font_path = FONTS_DIR / filename
    if not font_path.exists():
        # Fallback to default if font file is missing
        return ImageFont.load_default()
    return ImageFont.truetype(str(font_path), size)


# ---------------------------------------------------------------------------
# Edge helpers (mirrors dashboard utils.ts + PropCard.tsx)
# ---------------------------------------------------------------------------

def get_edge_tier(edge: float) -> str:
    """Return 'high', 'medium', or 'low' based on absolute edge value."""
    abs_edge = abs(edge) if edge and math.isfinite(edge) else 0
    if abs_edge >= 0.07:
        return "high"
    if abs_edge >= 0.05:
        return "medium"
    return "low"


def get_edge_color(edge: float) -> tuple[int, int, int]:
    """Return RGB tuple for the edge tier."""
    tier = get_edge_tier(edge)
    return {"high": GREEN, "medium": YELLOW, "low": SLATE}[tier]


def get_confidence_label(edge: float) -> str:
    """Return a human-readable confidence label (no exact %)."""
    tier = get_edge_tier(edge)
    return {"high": "Strong Edge", "medium": "High Confidence", "low": "Lean"}[tier]


def get_star_count(edge: float) -> int:
    """1-5 star rating matching PropCard.tsx:31 formula."""
    safe = abs(edge) if edge and math.isfinite(edge) else 0
    return min(5, max(1, math.ceil(safe * 50)))


def get_best_side(prediction: dict) -> tuple[str, float]:
    """Return (direction, edge) for the stronger side."""
    over = prediction.get("over_edge") or 0
    under = prediction.get("under_edge") or 0
    if over >= under:
        return "OVER", over
    return "UNDER", under


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------

def draw_rounded_rect(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int, int, int],
    radius: int,
    fill: tuple[int, int, int] | None = None,
    outline: tuple[int, int, int] | None = None,
    width: int = 1,
) -> None:
    """Draw a rounded rectangle."""
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def draw_circle_image(
    base: Image.Image,
    img: Image.Image,
    center: tuple[int, int],
    diameter: int,
    ring_color: tuple[int, int, int] | None = None,
    ring_width: int = 4,
) -> None:
    """Paste a circular-cropped image onto *base* at *center*."""
    # Resize source to fill diameter
    resized = img.resize((diameter, diameter), Image.LANCZOS)

    # Create circular mask
    mask = Image.new("L", (diameter, diameter), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, diameter, diameter), fill=255)

    # Draw optional glow ring first
    if ring_color:
        ring_d = diameter + ring_width * 2
        ring_x = center[0] - ring_d // 2
        ring_y = center[1] - ring_d // 2
        ring_draw = ImageDraw.Draw(base)
        ring_draw.ellipse(
            (ring_x, ring_y, ring_x + ring_d, ring_y + ring_d),
            fill=ring_color,
        )

    # Paste circular image
    top_left = (center[0] - diameter // 2, center[1] - diameter // 2)
    base.paste(resized, top_left, mask)


def draw_text_centered(
    draw: ImageDraw.ImageDraw,
    text: str,
    y: int,
    font: ImageFont.FreeTypeFont,
    fill: tuple[int, int, int],
    image_width: int,
) -> None:
    """Draw text horizontally centred at the given y coordinate."""
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    x = (image_width - tw) // 2
    draw.text((x, y), text, font=font, fill=fill)


def draw_star_rating(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    count: int,
    font: ImageFont.FreeTypeFont | None = None,
) -> int:
    """Draw 1-5 star rating. Returns total width drawn."""
    if font is None:
        font = get_font("bold", 20)
    star_char = "\u2605"  # ★
    spacing = 2
    total_w = 0
    for i in range(5):
        color = STAR_ACTIVE if i < count else STAR_INACTIVE
        draw.text((x + total_w, y), star_char, font=font, fill=color)
        bbox = draw.textbbox((0, 0), star_char, font=font)
        total_w += (bbox[2] - bbox[0]) + spacing
    return total_w


def draw_stat_badge(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    stat: str,
    font: ImageFont.FreeTypeFont | None = None,
) -> int:
    """Draw a coloured rounded badge for the stat type. Returns badge width."""
    if font is None:
        font = get_font("bold", 16)
    label = stat.upper()
    color = STAT_COLORS.get(stat.lower(), BLUE)
    bbox = draw.textbbox((0, 0), label, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    pad_x, pad_y = 12, 6
    badge_w = tw + pad_x * 2
    badge_h = th + pad_y * 2
    draw_rounded_rect(draw, (x, y, x + badge_w, y + badge_h), radius=6, fill=color)
    draw.text((x + pad_x, y + pad_y), label, font=font, fill=TEXT_PRIMARY)
    return badge_w


def create_placeholder_headshot(size: int = 200) -> Image.Image:
    """Generate a silhouette placeholder matching dashboard's SVG."""
    img = Image.new("RGBA", (size, size), (51, 65, 85, 255))
    draw = ImageDraw.Draw(img)
    cx, cy_head = size // 2, int(size * 0.35)
    r_head = int(size * 0.20)
    draw.ellipse(
        (cx - r_head, cy_head - r_head, cx + r_head, cy_head + r_head),
        fill=(100, 116, 139, 255),
    )
    ry_body = int(size * 0.25)
    rx_body = int(size * 0.35)
    cy_body = int(size * 0.85)
    draw.ellipse(
        (cx - rx_body, cy_body - ry_body, cx + rx_body, cy_body + ry_body),
        fill=(100, 116, 139, 255),
    )
    return img
