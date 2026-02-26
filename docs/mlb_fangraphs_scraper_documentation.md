# MLB FanGraphs Scraper Documentation

## Overview

`src/scrapers/mlb/mlb_fangraphs_scraper.py` fetches season-level advanced stats from FanGraphs via `pybaseball` and upserts into `mlb_player_season_advanced`.

## Data Source

**pybaseball** functions:
- `batting_stats(season, qual=50)` — returns all qualified batters with FanGraphs advanced stats
- `pitching_stats(season, qual=10)` — returns all qualified pitchers

## Player ID Resolution

FanGraphs uses its own player IDs. Resolution strategy:

1. **Direct name match** — lowercase match against `mlb_players.player_name`
2. **Name variation handling** — tries with/without Jr., Sr., II, III, IV suffixes
3. **Crosswalk fallback** — `pybaseball.playerid_reverse_lookup([fg_id], key_type="fangraphs")` maps FanGraphs ID → MLBAM ID
4. **Auto-insert** — if a player is resolved but not in `mlb_players`, inserts a stub record

Match rates improve significantly after running the boxscore backfill (which populates `mlb_players` with all active players).

## Stats Collected

### Batting

| Column | FanGraphs Source | Description |
|--------|-----------------|-------------|
| `wrc_plus` | `wRC+` | Weighted Runs Created Plus (park/league adjusted) |
| `woba` | `wOBA` | Weighted On-Base Average |
| `iso` | `ISO` | Isolated Power (SLG - AVG) |
| `war` | `WAR` | Wins Above Replacement |
| `babip` | `BABIP` | Batting Average on Balls In Play |
| `bb_pct` | `BB%` | Walk rate |
| `k_pct` | `K%` | Strikeout rate |
| `hard_pct` | `Hard%` | Hard contact rate |
| `avg/obp/slg/ops` | Standard | Slash line stats |
| `pa` | `PA` | Plate appearances (sample size) |

### Pitching

| Column | FanGraphs Source | Description |
|--------|-----------------|-------------|
| `fip` | `FIP` | Fielding Independent Pitching |
| `xfip` | `xFIP` | Expected FIP (normalizes HR/FB) |
| `xera` | `xERA` | Expected ERA |
| `siera` | `SIERA` | Skill-Interactive ERA |
| `era` | `ERA` | Earned Run Average |
| `war` | `WAR` | Wins Above Replacement |
| `lob_pct` | `LOB%` | Left On Base percentage |
| `gb_pct` | `GB%` | Ground ball rate |
| `k_per_9` | `K/9` | Strikeouts per 9 innings |
| `bb_per_9` | `BB/9` | Walks per 9 innings |
| `hr_per_9` | `HR/9` | Home runs per 9 innings |
| `ip` | `IP` | Innings pitched (sample size) |

## Usage

```bash
# Single season
python -m src.scrapers.mlb.mlb_fangraphs_scraper --season 2025

# All seasons (2022-2025)
python -m src.scrapers.mlb.mlb_fangraphs_scraper --all-seasons

# Batting or pitching only
python -m src.scrapers.mlb.mlb_fangraphs_scraper --season 2024 --batting-only
python -m src.scrapers.mlb.mlb_fangraphs_scraper --season 2024 --pitching-only
```

## Database Table

### `mlb_player_season_advanced`
- PK: `(player_id, season, player_type)` where `player_type` is `'batter'` or `'pitcher'`
- FK: `player_id → mlb_players(player_id)`
- Includes `fangraphs_id` for cross-reference
- `ON CONFLICT DO UPDATE` upserts — safe to re-run for updated stats

## Dependencies

- `pybaseball>=2.2.7`
- `numpy` (for NaN handling)
- `sqlalchemy` (via `src.db.client`)

## Backfill Order

Run the boxscore backfill FIRST to populate `mlb_players`, then FanGraphs:

```bash
python -m src.scrapers.mlb.mlb_backfill --seasons 2022 2023 2024 2025
python -m src.scrapers.mlb.mlb_fangraphs_scraper --all-seasons
```
