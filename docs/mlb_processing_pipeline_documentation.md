# MLB Processing Pipeline Documentation

## Overview

The MLB processing pipeline (Phase 2) transforms raw scraped data into model-ready features. It consists of two main components:

1. **MLB Linker** — Connects props data to game/player/team entities
2. **MLB Rolling Averages** — Computes shift(1) rolling averages for batting and pitching

All modules live in `src/processing/mlb/` and share configuration via `mlb_config.py`.

---

## Module Reference

### `mlb_config.py` — Shared Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `BATTING_WINDOWS` | `{"l5": 5, "l10": 10, "l20": 20, "szn": None}` | Rolling windows for batting averages |
| `PITCHING_WINDOWS` | `{"l3": 3, "l5": 5, "szn": None}` | Rolling windows for pitching averages |
| `BATTING_STATS` | 12 stats | pa, ab, r, h, doubles, triples, hr, rbi, bb, so, sb, tb |
| `BATTING_STD_STATS` | 7 stats | h, hr, tb, rbi, r, so, sb (L5 std devs) |
| `PITCHING_STATS` | 8 stats | ip, h_allowed, r_allowed, er, bb, so, hr_allowed, pitches_thrown |
| `PITCHING_STD_STATS` | 2 stats | so, er (L3 std devs) |
| `BATCH_SIZE` | 100 | Rows per DB insert batch |
| `LINKER_BATCH_SIZE` | 50,000 | Rows per linker processing batch |
| `FUZZY_THRESHOLD` | 0.70 | Minimum SequenceMatcher ratio for auto-match |
| `MLB_TEAM_ALIASES` | 66 entries | All 30 teams with Odds API names, variants, and abbreviation pass-through |

---

### `mlb_linker.py` — Props Entity Linking

Links `mlb_raw_player_props` rows by populating `game_id`, `player_id`, and `team_id`.

**Pattern:** Mirrors `src/processing/nba_linker_local.py` with MLB-specific adaptations.

#### Key Functions

| Function | Description |
|----------|-------------|
| `normalize_player(name)` | Strip accents, remove Jr/III/II/IV suffixes, punctuation |
| `normalize_team(name)` | Apply `MLB_TEAM_ALIASES` dict |
| `find_closest_game_date(candidates, target_date, max_days=1)` | ±1 day window |
| `build_game_lookup(engine)` | From `mlb_game_schedule` + `mlb_teams`, key: `(home_abbrev, away_abbrev)` |
| `build_player_lookup(engine)` | From `mlb_players`, key: `normalized_name → player_id` |
| `build_team_id_lookup(engine)` | UNION batting+pitching, key: `(player_id, game_id) → team_id` |
| `match_batch()` | Vectorized `.map()` for exact match, fuzzy only on unique misses, cached |
| `apply_updates()` | Temp table UPDATE with `AND r.game_id IS NULL` guard |

#### MLB vs NBA Differences

| Aspect | NBA | MLB |
|--------|-----|-----|
| `game_id` type | Zero-padded string (10 chars) | Integer |
| Date window | ±90 days (futures) | ±1 day (reliable timestamps) |
| Team ID source | NBA API lookup | Boxscore cross-reference (batting + pitching UNION) |
| Future games | NBA API call needed | `mlb_game_schedule` already has them |

#### Usage

```bash
python -m src.processing.mlb.mlb_linker incremental                    # Daily
python -m src.processing.mlb.mlb_linker backfill                       # One-time (all unlinked)
python -m src.processing.mlb.mlb_linker incremental --batch-size 100000  # Custom batch size
```

#### Resilience

- **Retry logic:** 20 attempts with escalating waits (10s to 60s)
- **Connection recovery:** `engine.dispose()` on errors creates fresh pool
- **Idempotent:** Queries `WHERE player_id IS NULL`, so restarts continue from last point
- **Laptop sleep:** Survives sleep/wake cycles via retry loop

---

### `mlb_populate_averages.py` — Full Backfill

TRUNCATE + reload both average tables.

#### Batting Flow

1. Fetch all rows from `mlb_player_game_stats_batting` (WHERE `did_not_play = false`)
2. Group by `(player_id, season)`, sort by `game_date`
3. `shift(1)` each stat, then rolling mean for each window in `BATTING_WINDOWS`
4. Std devs at L5 via `rolling(5, min_periods=2).std()`
5. Rate stats (BA, OBP, SLG, OPS) from rolling **sums** of numerator/denominator
6. `rest_days` from date diff (clip 0-14, default 4), `games_last_7d` via calendar loop
7. TRUNCATE `mlb_player_average_batting`, batch insert 100 rows at a time

#### Pitching Flow

1. Fetch from `mlb_player_game_stats_pitching` (WHERE `did_not_play = false`)
2. Same shift(1) + rolling pattern with `PITCHING_WINDOWS`
3. Derived rates (ERA, WHIP, K/9, BB/9) from rolling sums
4. `days_rest`, `pitch_count_last_start = pitches_thrown.shift(1)`
5. `starts_l3/l5/szn` = rolling sum of `is_starter` (shifted)
6. TRUNCATE `mlb_player_average_pitching`, batch insert

#### Critical Design Decision: Rate Stats from Rolling Sums

```
CORRECT: batting_avg_l10 = sum(h over L10) / sum(ab over L10)
WRONG:   batting_avg_l10 = mean(h/ab per game over L10)
```

The "mean of ratios" approach is mathematically incorrect — it gives equal weight to a 1-for-1 game and a 2-for-5 game. Rolling sums preserve the correct denominators.

#### Usage

```bash
python -m src.processing.mlb.mlb_populate_averages --table all              # Both tables
python -m src.processing.mlb.mlb_populate_averages --table batting           # Batting only
python -m src.processing.mlb.mlb_populate_averages --table pitching          # Pitching only
python -m src.processing.mlb.mlb_populate_averages --table all --season 2024 # Single season
```

---

### `mlb_populate_averages_incremental.py` — Daily Incremental

Processes only players active on the target date.

#### Flow

1. Find season for target date
2. Get batter/pitcher IDs with games on that date
3. Fetch all season-to-date games for those players (needed for correct SZN expanding average)
4. Per-player rolling calculation, filter to target-date rows only
5. UPSERT via `ON CONFLICT (player_id, game_id) DO UPDATE`

#### Usage

```bash
python -m src.processing.mlb.mlb_populate_averages_incremental                 # Today
python -m src.processing.mlb.mlb_populate_averages_incremental --date 2024-09-15  # Specific date
```

---

## Database Tables

### `mlb_player_average_batting`

- **PK:** `(player_id, game_id)`
- **Index:** `(player_id, game_date DESC)`
- **Columns:** 71 total
  - Identity: player_id, game_id, game_date, season, team_id
  - Window counts: game_number, games_l5, games_l10, games_l20, games_szn
  - 12 batting stats × 4 windows = 48 `avg_*` columns
  - 7 std devs at L5: `std_{h,hr,tb,rbi,r,so,sb}_l5`
  - 4 rate stats at L10: `avg_batting_avg_l10`, `avg_obp_l10`, `avg_slg_l10`, `avg_ops_l10`
  - Context: `rest_days`, `games_last_7d`

### `mlb_player_average_pitching`

- **PK:** `(player_id, game_id)`
- **Index:** `(player_id, game_date DESC)`
- **Columns:** 41 total
  - Identity: player_id, game_id, game_date, season, team_id
  - 8 pitching stats × 3 windows = 24 `avg_*` columns
  - 4 derived rate stats at L5: ERA, WHIP, K/9, BB/9
  - 2 std devs: `std_so_l3`, `std_er_l3`
  - Context: game_number, days_rest, pitch_count_last_start, starts_l3/l5/szn

---

## Pipeline Order

```
1. Scrapers (Phase 1)     → Raw data in DB
2. MLB Linker             → Props linked to game_id/player_id/team_id
3. MLB Rolling Averages   → Pre-game feature tables populated
4. Feature Store (Phase 3) → Model-ready feature vectors
5. Training (Phase 3)     → XGBoost quantile regression models
```
