# Social Media Pick Image Generator

## Overview

CLI tool (`src/social/`) that auto-generates professional, dark-themed social media images from GameFlowData's daily predictions. Designed for organic marketing on Instagram, TikTok, and Discord to build community before monetizing with paid subscriptions.

## Motivation

Manual creation of daily pick graphics is unsustainable. This tool reads directly from the database and generates branded images matching the dashboard's visual identity, eliminating manual effort.

## Card Types

### Slate Card (Daily Picks)
- **Purpose:** Main daily post showing top 3-5 picks
- **Content:** Header with date, rows with headshot + player name + matchup + stat badge + direction/line + star rating + confidence tier
- **Formats:** 1080x1080 (IG feed) or 1080x1920 (IG story/TikTok)
- **Footer:** "gameflowdata.com | Free picks daily"

### Individual Pick Card
- **Purpose:** Single player feature for stories/reels
- **Content:** Watermark, headshot with colored glow ring, player name, matchup + game time, stat badge, "OVER 25.5", star rating + confidence label, projection range
- **Formats:** 1080x1080 or 1080x1920

### Results Card
- **Purpose:** Yesterday's outcomes with proof of results
- **Content:** Summary bar (W-L-P, win rate, P&L), result rows with green check/red X, season stats footer
- **Formats:** 1080x1080 or 1080x1920

## Module Architecture

### `theme.py` — Constants & Drawing Helpers

**Color palette** matches dashboard Tailwind classes:
- Backgrounds: slate-950 `#020617`, slate-800 `#1E293B`, slate-700 `#334155`
- Text: slate-50, slate-400, slate-500
- Edge tiers: green (high >=7%), yellow (medium >=5%), slate (low)
- Stat badges: blue (PTS), teal (REB), purple (AST)
- Results: green (won), red (lost), gray (push)

**Key functions:**
- `get_edge_tier(edge)` — "high"/"medium"/"low" (mirrors `utils.ts:getEdgeTier`)
- `get_star_count(edge)` — 1-5 stars (mirrors `PropCard.tsx:31`)
- `get_confidence_label(edge)` — "Strong Edge"/"High Confidence"/"Lean"
- `get_best_side(prediction)` — Returns (direction, edge) for stronger side
- Drawing helpers: `draw_rounded_rect()`, `draw_circle_image()`, `draw_text_centered()`, `draw_star_rating()`, `draw_stat_badge()`

### `data_provider.py` — Sync DB Queries

Synchronous wrappers using `src/db/client.get_engine()`. Intentionally does NOT reuse the async Discord services.

| Function | Query | Returns |
|----------|-------|---------|
| `get_top_picks_sync(date, n, min_edge)` | `daily_predictions` ordered by best edge | list[dict] |
| `get_resolved_bets(date)` | `paper_bets` where resolved | list[dict] |
| `get_daily_summary(date)` | `paper_trading_daily_log` for date | dict or None |
| `get_performance_stats_sync(days)` | Aggregate from `paper_bets` | dict (win_rate, roi, total_bets) |

### `card_renderer.py` — Image Generation

**HeadshotCache:**
- Cache dir: `data/headshots/`
- URL: `https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png`
- Downloads on first access, saves placeholder marker on failure (prevents retries)
- Circular crop applied at render time

**Renderers:** `PickCardRenderer`, `SlateCardRenderer`, `ResultsCardRenderer` — each has a `render()` method returning a Pillow `Image`.

### `generate_images.py` — CLI Entry Point

Lazy-imports DB modules so `--dry-run` works without a database connection.

## CLI Usage

```bash
# Daily slate (main use case)
python src/social/generate_images.py --date 2026-02-18 --type picks

# Results recap
python src/social/generate_images.py --date 2026-02-17 --type results

# Both picks + yesterday's results
python src/social/generate_images.py --date 2026-02-18 --type both

# Story format for IG stories / TikTok
python src/social/generate_images.py --date 2026-02-18 --type picks --format story

# Also generate individual pick cards
python src/social/generate_images.py --date 2026-02-18 --type picks --individual

# Dry run
python src/social/generate_images.py --date 2026-02-18 --type picks --dry-run
```

### CLI Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--date` | YYYY-MM-DD | required | Primary date for picks |
| `--type` | picks/results/both | picks | What to generate |
| `--format` | square/story | square | Image dimensions |
| `--top` | int | 5 | Number of top picks |
| `--min-edge` | float | 0.05 | Minimum edge threshold |
| `--output-dir` | path | output/social/ | Where to save images |
| `--individual` | flag | false | Also generate per-player cards |
| `--dry-run` | flag | false | Preview without DB/images |

### Date Logic for `--type both`
- Picks: from `--date` (today)
- Results: from `--date - 1` (yesterday)

## Output

Images saved to `output/social/` with naming convention:
- `slate_2026-02-18_square.png`
- `pick_2026-02-18_russell_westbrook_pts_square.png`
- `results_2026-02-17_square.png`

## Design Decisions

1. **No exact percentages on images** — Only tier labels to keep premium data behind dashboard/Discord
2. **Sync queries** — Avoids asyncio complexity for a CLI tool
3. **Lazy imports** — `--dry-run` works without DB connection
4. **Headshot caching** — Prevents repeated downloads; placeholder markers prevent retry storms
5. **Montserrat font** — Clean, modern sans-serif matching dashboard aesthetic (OFL license)

## Dependencies

- `Pillow>=10.0.0` — Image generation
- `requests` — Headshot downloads (already in requirements)
- `sqlalchemy` — DB queries (already in requirements)

## Tests

33 tests in `tests/test_card_renderer.py`:
- Theme utility tests (edge tiers, star formula, confidence labels, best side logic)
- Placeholder headshot generation
- All three renderers (square and story dimensions, edge cases)
- HeadshotCache with mocked HTTP (failure handling, disk caching)
