"""Image renderers for social media pick cards.

Three renderer classes:
  - PickCardRenderer   — individual player feature card
  - SlateCardRenderer  — daily top picks on one image
  - ResultsCardRenderer — yesterday's outcomes recap
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import requests
from PIL import Image, ImageDraw

from src.social.theme import (
    AMBER,
    BG_CARD,
    BG_PRIMARY,
    BG_ROW_ALT,
    LOST_RED,
    PUSH_GRAY,
    TEXT_MUTED,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
    WON_GREEN,
    create_placeholder_headshot,
    draw_circle_image,
    draw_rounded_rect,
    draw_star_rating,
    draw_stat_badge,
    draw_text_centered,
    get_best_side,
    get_confidence_label,
    get_edge_color,
    get_font,
    get_star_count,
)

logger = logging.getLogger(__name__)

# Headshot URL pattern (matches dashboard/src/lib/utils.ts)
HEADSHOT_URL = "https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
HEADSHOT_CACHE_DIR = Path(__file__).resolve().parents[2] / "data" / "headshots"

# Image dimensions
SQUARE = (1080, 1080)
STORY = (1080, 1920)


# ---------------------------------------------------------------------------
# Headshot cache
# ---------------------------------------------------------------------------

class HeadshotCache:
    """Downloads and caches NBA player headshots to disk."""

    def __init__(self, cache_dir: Path | None = None):
        self.cache_dir = cache_dir or HEADSHOT_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, player_id: int) -> Image.Image:
        """Return headshot Image (from cache or network). Falls back to placeholder."""
        cached = self.cache_dir / f"{player_id}.png"
        if cached.exists():
            try:
                img = Image.open(cached).convert("RGBA")
                # Check if it's a placeholder (very small file = placeholder marker)
                if img.size[0] < 10:
                    return create_placeholder_headshot()
                return img
            except Exception:
                pass

        # Download from NBA CDN
        url = HEADSHOT_URL.format(player_id=player_id)
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            with open(cached, "wb") as f:
                f.write(resp.content)
            return Image.open(cached).convert("RGBA")
        except Exception as e:
            logger.debug(f"Headshot download failed for {player_id}: {e}")
            # Save a tiny placeholder marker to prevent retries
            placeholder = create_placeholder_headshot()
            marker = Image.new("RGBA", (1, 1), (0, 0, 0, 0))
            marker.save(cached)
            return placeholder


# ---------------------------------------------------------------------------
# Individual pick card
# ---------------------------------------------------------------------------

class PickCardRenderer:
    """Renders a single-player feature card."""

    def __init__(self, headshot_cache: HeadshotCache | None = None):
        self.headshots = headshot_cache or HeadshotCache()

    def render(
        self,
        pick: dict,
        prediction_date: date,
        fmt: str = "square",
    ) -> Image.Image:
        """Render an individual pick card.

        Args:
            pick: dict from data_provider.get_top_picks_sync
            prediction_date: date for the header
            fmt: 'square' (1080x1080) or 'story' (1080x1920)
        """
        w, h = STORY if fmt == "story" else SQUARE
        img = Image.new("RGB", (w, h), BG_PRIMARY)
        draw = ImageDraw.Draw(img)

        direction, edge = get_best_side(pick)
        stars = get_star_count(edge)
        tier_color = get_edge_color(edge)
        confidence = get_confidence_label(edge)
        stat = pick.get("stat", "pts")
        line = pick.get("line", 0)
        player_name = pick.get("player_name", "Unknown")
        player_id = pick.get("player_id", 0)
        opp = pick.get("feat_opp_abbrev", "???")
        q50 = pick.get("pred_q50")
        q10 = pick.get("pred_q10")
        q90 = pick.get("pred_q90")
        game_time = pick.get("game_time")

        # Layout vertical positions scale with height
        y_cursor = 30

        # Watermark / brand bar
        wm_font = get_font("semibold", 18)
        draw_text_centered(draw, "GAMEFLOWDATA", y_cursor, wm_font, AMBER, w)
        y_cursor += 40

        # Date
        date_font = get_font("medium", 16)
        date_str = prediction_date.strftime("%B %d, %Y").upper()
        draw_text_centered(draw, date_str, y_cursor, date_font, TEXT_MUTED, w)
        y_cursor += 50

        # Headshot with glow ring
        headshot_size = 240 if fmt == "story" else 200
        headshot_img = self.headshots.get(player_id)
        draw_circle_image(
            img,
            headshot_img,
            center=(w // 2, y_cursor + headshot_size // 2),
            diameter=headshot_size,
            ring_color=tier_color,
            ring_width=6,
        )
        # Re-create draw after pasting
        draw = ImageDraw.Draw(img)
        y_cursor += headshot_size + 30

        # Player name
        name_font = get_font("bold", 36)
        draw_text_centered(draw, player_name, y_cursor, name_font, TEXT_PRIMARY, w)
        y_cursor += 50

        # Matchup + game time
        matchup_parts = [f"vs {opp}"]
        if game_time:
            try:
                if isinstance(game_time, str):
                    gt = datetime.fromisoformat(game_time.replace("Z", "+00:00"))
                else:
                    gt = game_time
                matchup_parts.append(gt.strftime("%I:%M %p ET"))
            except (ValueError, AttributeError):
                pass
        matchup_font = get_font("medium", 20)
        draw_text_centered(
            draw, "  |  ".join(matchup_parts), y_cursor, matchup_font, TEXT_SECONDARY, w
        )
        y_cursor += 45

        # Stat badge (centred)
        badge_font = get_font("bold", 20)
        badge_label = stat.upper()
        bbox = draw.textbbox((0, 0), badge_label, font=badge_font)
        badge_w = (bbox[2] - bbox[0]) + 24
        badge_x = (w - badge_w) // 2
        draw_stat_badge(draw, badge_x, y_cursor, stat, badge_font)
        y_cursor += 50

        # Direction + line
        dir_font = get_font("bold", 48)
        dir_text = f"{direction} {line}"
        draw_text_centered(draw, dir_text, y_cursor, dir_font, TEXT_PRIMARY, w)
        y_cursor += 70

        # Star rating + confidence label (centred)
        star_font = get_font("bold", 28)
        star_char = "\u2605"
        star_bbox = draw.textbbox((0, 0), star_char, font=star_font)
        single_star_w = (star_bbox[2] - star_bbox[0]) + 2
        total_star_w = single_star_w * 5

        conf_font = get_font("semibold", 20)
        conf_bbox = draw.textbbox((0, 0), confidence, font=conf_font)
        conf_w = conf_bbox[2] - conf_bbox[0]

        total_w = total_star_w + 16 + conf_w
        sx = (w - total_w) // 2
        draw_star_rating(draw, sx, y_cursor, stars, star_font)
        draw.text(
            (sx + total_star_w + 16, y_cursor + 4),
            confidence,
            font=conf_font,
            fill=tier_color,
        )
        y_cursor += 50

        # Projection range
        if q50 is not None:
            proj_font = get_font("medium", 20)
            proj_text = f"Projection: {q50:.1f}"
            if q10 is not None and q90 is not None:
                proj_text += f"  ({q10:.1f} - {q90:.1f})"
            draw_text_centered(draw, proj_text, y_cursor, proj_font, TEXT_SECONDARY, w)
            y_cursor += 40

        # Branding bar at bottom
        bar_h = 50
        bar_y = h - bar_h
        draw_rounded_rect(draw, (0, bar_y, w, h), radius=0, fill=BG_CARD)
        brand_font = get_font("semibold", 16)
        draw_text_centered(
            draw,
            "gameflowdata.com  |  Free picks daily",
            bar_y + 15,
            brand_font,
            AMBER,
            w,
        )

        return img


# ---------------------------------------------------------------------------
# Slate card (multiple picks)
# ---------------------------------------------------------------------------

class SlateCardRenderer:
    """Renders a multi-pick daily slate card."""

    def __init__(self, headshot_cache: HeadshotCache | None = None):
        self.headshots = headshot_cache or HeadshotCache()

    def render(
        self,
        picks: list[dict],
        prediction_date: date,
        fmt: str = "square",
    ) -> Image.Image:
        """Render a slate card with multiple picks."""
        w, h = STORY if fmt == "story" else SQUARE
        max_rows = 3 if fmt == "story" else 5
        picks = picks[:max_rows]

        img = Image.new("RGB", (w, h), BG_PRIMARY)
        draw = ImageDraw.Draw(img)

        y_cursor = 30

        # Header
        title_font = get_font("bold", 36)
        draw_text_centered(draw, "TODAY'S TOP PICKS", y_cursor, title_font, TEXT_PRIMARY, w)
        y_cursor += 50
        date_font = get_font("medium", 20)
        draw_text_centered(
            draw,
            prediction_date.strftime("%B %d, %Y").upper(),
            y_cursor,
            date_font,
            TEXT_SECONDARY,
            w,
        )
        y_cursor += 50

        # Divider
        draw.line([(40, y_cursor), (w - 40, y_cursor)], fill=BG_ROW_ALT, width=2)
        y_cursor += 20

        # Rows
        row_h = (h - y_cursor - 80) // max(len(picks), 1)
        row_h = min(row_h, 160)
        headshot_d = 70

        for i, pick in enumerate(picks):
            row_y = y_cursor + i * row_h
            # Alternating row background
            if i % 2 == 1:
                draw_rounded_rect(
                    draw,
                    (20, row_y, w - 20, row_y + row_h - 4),
                    radius=12,
                    fill=BG_CARD,
                )

            direction, edge = get_best_side(pick)
            stars = get_star_count(edge)
            tier_color = get_edge_color(edge)
            stat = pick.get("stat", "pts")
            line = pick.get("line", 0)
            player_name = pick.get("player_name", "Unknown")
            player_id = pick.get("player_id", 0)
            opp = pick.get("feat_opp_abbrev", "???")

            ry = row_y + row_h // 2  # vertical centre of row

            # Headshot
            headshot_img = self.headshots.get(player_id)
            draw_circle_image(
                img,
                headshot_img,
                center=(70, ry),
                diameter=headshot_d,
                ring_color=tier_color,
                ring_width=3,
            )
            draw = ImageDraw.Draw(img)

            # Player name + matchup
            name_font = get_font("bold", 22)
            matchup_font = get_font("medium", 16)
            draw.text((120, ry - 24), player_name, font=name_font, fill=TEXT_PRIMARY)
            draw.text((120, ry + 4), f"vs {opp}", font=matchup_font, fill=TEXT_SECONDARY)

            # Stat badge
            badge_font = get_font("bold", 14)
            badge_x = 500
            draw_stat_badge(draw, badge_x, ry - 14, stat, badge_font)

            # Direction + line
            dir_font = get_font("bold", 22)
            dir_text = f"{direction} {line}"
            draw.text((600, ry - 14), dir_text, font=dir_font, fill=TEXT_PRIMARY)

            # Stars + tier
            star_font = get_font("bold", 18)
            conf_label = get_confidence_label(edge)
            conf_font = get_font("medium", 14)

            star_x = 800
            draw_star_rating(draw, star_x, ry - 14, stars, star_font)
            draw.text((star_x, ry + 10), conf_label, font=conf_font, fill=tier_color)

        # Footer
        footer_y = h - 60
        draw.line([(40, footer_y - 10), (w - 40, footer_y - 10)], fill=BG_ROW_ALT, width=2)
        footer_font = get_font("semibold", 18)
        draw_text_centered(
            draw,
            "gameflowdata.com  |  Free picks daily",
            footer_y + 5,
            footer_font,
            AMBER,
            w,
        )

        return img


# ---------------------------------------------------------------------------
# Results card
# ---------------------------------------------------------------------------

class ResultsCardRenderer:
    """Renders a results recap card for the previous day's bets."""

    def render(
        self,
        bets: list[dict],
        summary: dict | None,
        game_date: date,
        performance: dict | None = None,
        fmt: str = "square",
    ) -> Image.Image:
        """Render a results card.

        Args:
            bets: list from data_provider.get_resolved_bets
            summary: dict from data_provider.get_daily_summary (or None)
            game_date: the date being recapped
            performance: dict from data_provider.get_performance_stats_sync (or None)
            fmt: 'square' or 'story'
        """
        w, h = STORY if fmt == "story" else SQUARE
        img = Image.new("RGB", (w, h), BG_PRIMARY)
        draw = ImageDraw.Draw(img)

        y_cursor = 30

        # Header
        title_font = get_font("bold", 36)
        draw_text_centered(draw, "YESTERDAY'S RESULTS", y_cursor, title_font, TEXT_PRIMARY, w)
        y_cursor += 50
        date_font = get_font("medium", 20)
        draw_text_centered(
            draw,
            game_date.strftime("%B %d, %Y").upper(),
            y_cursor,
            date_font,
            TEXT_SECONDARY,
            w,
        )
        y_cursor += 40

        # Summary bar
        if summary:
            bar_y = y_cursor
            draw_rounded_rect(
                draw, (30, bar_y, w - 30, bar_y + 60), radius=12, fill=BG_CARD
            )
            bar_font = get_font("semibold", 20)
            won = summary.get("bets_won", 0)
            lost = summary.get("bets_lost", 0)
            push = summary.get("bets_push", 0)
            pnl = summary.get("total_pnl", 0.0)
            total = won + lost + push
            wr = won / total * 100 if total > 0 else 0

            summary_text = (
                f"{won}W - {lost}L - {push}P"
                f"  |  Win Rate: {wr:.1f}%"
                f"  |  P&L: ${pnl:+.2f}"
            )
            draw_text_centered(draw, summary_text, bar_y + 18, bar_font, TEXT_PRIMARY, w)
            y_cursor = bar_y + 80
        else:
            y_cursor += 20

        # Divider
        draw.line([(40, y_cursor), (w - 40, y_cursor)], fill=BG_ROW_ALT, width=2)
        y_cursor += 15

        # Result rows
        max_rows = min(len(bets), 8)
        if max_rows == 0:
            no_data_font = get_font("medium", 24)
            draw_text_centered(
                draw, "No resolved bets for this date", y_cursor + 40, no_data_font, TEXT_MUTED, w
            )
        else:
            avail_h = h - y_cursor - 100
            row_h = min(avail_h // max_rows, 90)

            for i, bet in enumerate(bets[:max_rows]):
                row_y = y_cursor + i * row_h
                # Alternating background
                if i % 2 == 1:
                    draw_rounded_rect(
                        draw,
                        (20, row_y, w - 20, row_y + row_h - 4),
                        radius=10,
                        fill=BG_CARD,
                    )

                ry = row_y + row_h // 2
                status = bet.get("status", "")
                player_name = bet.get("player_name", "Unknown")
                stat = bet.get("stat_type", "")
                direction = bet.get("bet_direction", "").upper()
                line = bet.get("line", 0)
                actual = bet.get("actual_value")
                pnl = bet.get("pnl", 0)

                # Status icon colour
                if status == "won":
                    status_color = WON_GREEN
                    icon = "\u2713"  # ✓
                elif status == "lost":
                    status_color = LOST_RED
                    icon = "\u2717"  # ✗
                else:
                    status_color = PUSH_GRAY
                    icon = "—"

                # Player + stat
                name_font = get_font("bold", 20)
                stat_font = get_font("medium", 16)
                draw.text((50, ry - 18), player_name, font=name_font, fill=TEXT_PRIMARY)
                draw.text((50, ry + 6), stat.upper(), font=stat_font, fill=TEXT_SECONDARY)

                # Direction + line → actual
                dir_font = get_font("semibold", 20)
                dir_text = f"{direction} {line}"
                if actual is not None:
                    dir_text += f"  →  {actual:.0f}"
                draw.text((480, ry - 10), dir_text, font=dir_font, fill=TEXT_PRIMARY)

                # Status icon
                icon_font = get_font("bold", 32)
                draw.text((920, ry - 16), icon, font=icon_font, fill=status_color)

                # P&L
                pnl_font = get_font("semibold", 16)
                pnl_text = f"${pnl:+.2f}" if pnl else ""
                pnl_color = WON_GREEN if pnl and pnl > 0 else LOST_RED if pnl and pnl < 0 else TEXT_MUTED
                draw.text((960, ry - 8), pnl_text, font=pnl_font, fill=pnl_color)

        # Footer with season stats
        footer_y = h - 60
        draw.line([(40, footer_y - 10), (w - 40, footer_y - 10)], fill=BG_ROW_ALT, width=2)
        footer_font = get_font("semibold", 18)

        if performance:
            wr = performance.get("win_rate", 0)
            roi = performance.get("roi", 0)
            footer_text = (
                f"Season: {wr:.1%} Win Rate  |  {roi:+.1%} ROI"
                f"  |  gameflowdata.com"
            )
        else:
            footer_text = "gameflowdata.com  |  Free picks daily"

        draw_text_centered(draw, footer_text, footer_y + 5, footer_font, AMBER, w)

        return img
