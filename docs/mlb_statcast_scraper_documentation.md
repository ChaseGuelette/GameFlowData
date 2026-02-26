# MLB Statcast Scraper Documentation

## Overview

`src/scrapers/mlb/mlb_statcast_scraper.py` fetches daily Statcast pitch-level data from Baseball Savant via `pybaseball`, aggregates it per (player, game_date), and upserts into two tables:

- `mlb_player_game_statcast_batting` — per-game batting aggregates
- `mlb_player_game_statcast_pitching` — per-game pitching aggregates

## Data Source

**pybaseball** (`pybaseball.statcast()`) wraps the Baseball Savant API. Returns pitch-level data with ~118 columns per pitch row, including launch_speed, launch_angle, estimated_ba_using_speedangle, barrel flag, zone, pitch_type, release_speed, release_spin_rate, and more.

A typical MLB game day returns ~4,000-5,000 pitch rows across all games.

## Aggregation Logic

### Batting Metrics

| Metric | Calculation |
|--------|-------------|
| `avg_exit_velocity` | Mean of `launch_speed` on batted balls (`type == 'X'`) |
| `max_exit_velocity` | Max of `launch_speed` on batted balls |
| `barrel_pct` | `barrel` flag sum / batted ball count |
| `hard_hit_pct` | Count of `launch_speed >= 95` / batted ball count |
| `sweet_spot_pct` | Count of `launch_angle` in [8, 32] / batted ball count |
| `xba` | Mean of `estimated_ba_using_speedangle` on batted balls |
| `xslg` | Mean of `estimated_slg_using_speedangle` on batted balls |
| `xwoba` | Mean of `estimated_woba_using_speedangle` on batted balls |
| `woba` | Mean of `woba_value` on batted balls |
| `gb/fb/ld/popup_pct` | From `bb_type` column: `ground_ball`, `fly_ball`, `line_drive`, `popup` |
| `pull/center/oppo_pct` | From `hc_x`: <100=pull, 100-150=center, >150=oppo |
| `zone_pct` | Pitches in zones 1-9 / total pitches seen |
| `chase_pct` | Swings on pitches outside zone / pitches outside zone |
| `whiff_pct` | Swinging strikes / total swings |

### Pitching Metrics

All batting contact metrics computed "against" (from pitcher perspective), plus:

| Metric | Calculation |
|--------|-------------|
| `avg/max_fastball_velo` | `release_speed` on fastball types (FF, SI, FC, FA) |
| `avg_fastball_spin` | `release_spin_rate` on fastball types |
| `avg_breaking_spin` | `release_spin_rate` on breaking types (SL, CU, KC, SV, etc.) |
| `fastball/breaking/offspeed_pct` | Pitch count by type / total pitches |
| `csw_pct` | (Called strikes + whiffs) / total pitches |
| `xera` | Approximate from xwOBA: `((xwoba - 0.310) / 1.157) * 9 + 3.10` |

### Pitch Type Classification

| Category | Pitch Types |
|----------|-------------|
| Fastball | FF (4-seam), SI (sinker), FC (cutter), FA (fastball) |
| Breaking | SL (slider), CU (curve), KC (knuckle curve), SV (sweeper), CS, KN, SC, ST, EP |
| Offspeed | CH (changeup), FS (splitter), FO (forkball) |

## Usage

```bash
# Single day
python -m src.scrapers.mlb.mlb_statcast_scraper --date 2025-06-15

# Yesterday's games
python -m src.scrapers.mlb.mlb_statcast_scraper --yesterday

# Date range backfill (1 req/sec)
python -m src.scrapers.mlb.mlb_statcast_scraper --backfill --start-date 2025-04-01 --end-date 2025-04-07
```

## Bulk Backfill

For full season backfills, use the dedicated orchestrator with progress resume:

```bash
python -m src.scrapers.mlb.mlb_statcast_backfill --seasons 2024 2025
python -m src.scrapers.mlb.mlb_statcast_backfill --start-date 2025-04-01 --end-date 2025-06-30
python -m src.scrapers.mlb.mlb_statcast_backfill --seasons 2024 --no-resume
```

Progress saved to `mlb_statcast_backfill_progress.json`. Resume-safe — interrupt and restart at any time.

## Database Tables

### `mlb_player_game_statcast_batting`
- PK: `(player_id, game_date)`
- FK: `player_id → mlb_players(player_id)`
- Indexes: `game_date`, `player_id`

### `mlb_player_game_statcast_pitching`
- PK: `(player_id, game_date)`
- FK: `player_id → mlb_players(player_id)`
- Indexes: `game_date`, `player_id`

## Dependencies

- `pybaseball>=2.2.7`
- `pandas`, `numpy` (for aggregation)
- `sqlalchemy` (via `src.db.client`)

## Rate Limiting

pybaseball recommends ~1 request per second for Statcast queries. The backfill orchestrator enforces this with `time.sleep(1)` between day fetches.
