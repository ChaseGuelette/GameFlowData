# Plan: Capture Real Starter Data + Starter Probability Feature

## Context

The model predicts unders for players who start, but starters SMASH overs. Root cause: `games_started_l5` uses a `min >= 20` proxy instead of actual starter data, and this feature is only in the minutes model — rate models (PTS/REB/AST) have no starter signal at all. The CDN boxscore JSON already has a `starter` field ("1"/"0") that we're downloading but not extracting.

## Research Findings

### What Exists Now
- `games_started_l5` feature uses `min >= 20` heuristic (`STARTER_MINUTES_THRESHOLD = 20`)
- Only used in `MINUTES_FEATURES`, NOT in any `RATE_FEATURES_*` lists
- CDN boxscore JSON (`cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json`) has a `starter` field ("1"/"0") per player — scraper downloads this but doesn't extract it
- `nba_unified_scraper.py` uses BoxScoreTraditionalV3 which doesn't have START_POSITION; V2 has it but hits stats.nba.com (blocked on Railway)
- `player_game_stats` table has NO `started` column
- `player_average_game_stats` has `games_started_l5` (smallint)

### Available NBA Data Sources for Starter Info

| Source | Starter Field | Historical Depth | Railway Safe |
|--------|--------------|------------------|-------------|
| **CDN Boxscore** (already fetched!) | `starter: "1"/"0"` | Current + recent seasons | Yes |
| **BoxScoreTraditionalV2** | `START_POSITION: "F"/"G"/"C"/""` | 1996+ | No (stats.nba.com) |
| BoxScoreTraditionalV3 (currently used) | **None** | N/A | No |

### Pre-Game Lineup Sources
- **No free reliable API** for pre-game lineups
- Teams submit lineups ~30 min before tipoff
- RotoWire/RotoGrinders have projections but require scraping
- SportsDataIO is gold standard but costs money

### The 20-Min Proxy Problem
The `min >= 20` heuristic misses:
- Bench players who play 20+ min in blowouts (false positive)
- Starters who play <20 min due to injury/foul trouble (false negative)
- ~5-10% disagreement rate with actual starter data

## Approach: Heuristic, Not ML

NBA starting lineups are >90% predictable from recent history + injuries. A simple `games_started_l5 / 5.0` probability estimate is sufficient. The model learns the interaction between starter probability and other features (injury context, minutes trends). No separate starter prediction model needed.

**Why not ML:**
1. Signal dominated by (a) recent starter history and (b) injury status — two features capture nearly all variance
2. Cold-start is minimal — traded players slot into same role, rookies ramp up gradually
3. Self-correction is built-in — L5 window updates daily as actual data flows in
4. Heuristic outputs a calibrated probability: 4/5 starts = 0.80

---

## Phase 1: Capture Actual Starter Data

### Step 1: Database Migration
Add `started` boolean column to `player_game_stats` via Supabase `apply_migration`:
```sql
ALTER TABLE public.player_game_stats ADD COLUMN started boolean DEFAULT NULL;

COMMENT ON COLUMN public.player_game_stats.started IS
    'Whether the player started the game. Source: CDN boxscore starter field. NULL = unknown (pre-backfill).';
```

Nullable boolean. `NULL` = unknown (historical data before backfill). `true`/`false` = authoritative.

### Step 2: CDN Scraper (1-line change)
**File:** `src/scrapers/nba_cdn_scraper.py` (line 278, in `transform_player_stats()`)

Add to the `row` dict after `"did_not_play": is_dnp,`:
```python
"started": player.get("starter") == "1",
```

The CDN JSON returns `"1"` (string) for starters, `"0"` for bench. `player.get("starter")` returns `None` if missing → `None == "1"` is `False` (safe default). No changes needed to `insert_player_stats()` — it uses `df.to_sql()` which handles new columns automatically.

### Step 3: Backfill Script
**File:** NEW `src/processing/backfill_starter_data.py`

**Strategy A: CDN Backfill (preferred, Railway-safe)**
- Query all distinct `game_id` values in `player_game_stats` where `started IS NULL`
- Fetch CDN boxscore JSON per game (`cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json`)
- Extract `starter` field for each player
- Batch UPDATE: `SET started = :started WHERE player_id = :pid AND game_id = :gid`
- Rate-limit 0.5s/game (~1200 games current season = ~10 min)
- CLI flags: `--season`, `--batch-size`, `--dry-run`

**Strategy B: BoxScoreTraditionalV2 (local only, for older seasons)**
- Uses `nba_api.stats.endpoints.boxscoretraditionalv2` with `START_POSITION` column
- `START_POSITION` non-empty = starter, empty string = bench
- Hits stats.nba.com → **MUST run locally, NEVER on Railway**
- Only needed if CDN doesn't have older seasons

**Fallback: minutes proxy for very old data:**
```sql
UPDATE player_game_stats SET started = (min >= 20) WHERE started IS NULL;
```

### Step 4: Update Rolling Average Computation
**Files:** `src/processing/populate_average_stats.py` (line 309) and `populate_average_stats_incremental.py` (line 292)

Replace minutes proxy with actual `started` column, falling back to proxy when NULL:
```python
# B4: Games started L5 (use actual started column, fall back to minutes proxy)
if "started" in player_df.columns and player_df["started"].notna().any():
    is_starter = player_df["started"].fillna(
        player_df["min"] >= STARTER_MINUTES_THRESHOLD
    ).astype(float)
else:
    is_starter = (player_df["min"] >= STARTER_MINUTES_THRESHOLD).astype(float)
shifted_starter = is_starter.shift(1).where(min_mask)
player_df["games_started_l5"] = shifted_starter.rolling(window=5, min_periods=1).sum()
```

Also add `started` to the SELECT queries that fetch `player_game_stats` data:
- `populate_average_stats_incremental.py` line ~166-177 (`fetch_player_basic_season_games()`)
- `populate_average_stats.py` equivalent query

---

## Phase 2: Add `player_starter_prob` Feature

### Step 5: Feature Store
**File:** `src/models/feature_store.py`

**Add `"player_starter_prob"` to feature lists:**
```python
# MINUTES_FEATURES (line 39, after player_games_started_l5):
"player_starter_prob",

# RATE_FEATURES_PTS (line ~81, after injury context block):
"player_starter_prob",

# RATE_FEATURES_REB (line ~110):
"player_starter_prob",

# RATE_FEATURES_AST (line ~139):
"player_starter_prob",

# RATE_FEATURES_THREES is archived, skip
```

**Why add to rate models:** The core hypothesis is that starters produce more stats. Rate models currently have no direct starter signal — they rely on `player_avg_min_l5` as a blended proxy. `player_starter_prob` gives rate models a clean signal: "this player is expected to start tonight."

**Add computed column to all SQL query paths** (search for all references to `games_started_l5` in feature_store.py):

```sql
LEAST(COALESCE(p_avg.games_started_l5, 0) / 5.0, 1.0) as player_starter_prob,
```

There are 4 query paths:
1. `get_training_dataset()` / `get_features_for_date()` batch SQL (~line 377)
2. `get_features_for_date_range()` batch SQL (~line 695)
3. Additional batch query variant (~line 1135)
4. Single-player `_get_player_rolling_stats()` Python path (~line 1364)

For the Python path (#4):
```python
stats["player_starter_prob"] = min((stats["player_games_started_l5"] or 0) / 5.0, 1.0)
```

---

## Phase 3: Testing

### Step 6: Unit Tests

**`tests/test_populate_average_stats.py`** — add tests:
1. `test_games_started_uses_actual_started_column` — player with `started=True/False` data, verify `games_started_l5` uses it instead of minutes proxy
2. `test_games_started_falls_back_to_minutes_proxy` — player with `started=NULL`, verify fallback to `min >= 20`
3. `test_games_started_mixed_data` — mix of real and NULL `started` values, verify `fillna()` works correctly

**CDN scraper test:**
- Test `transform_player_stats()` returns `started` field correctly
- Test defensive handling when `starter` key missing from JSON

---

## Execution Sequence

1. Apply migration (Supabase `apply_migration`)
2. Modify CDN scraper (1-line change)
3. Create + run backfill script (~10 min for current season)
4. Update `populate_average_stats*.py` (both files)
5. Update `feature_store.py` (4 feature lists + all SQL paths)
6. Add unit tests
7. Run `ruff check --fix` + `pytest`
8. Run full average stats recalculation: `python -m src.processing.populate_average_stats --season 2025-26 --table player`
9. Spot-check via SQL that `games_started_l5` values changed for players with mixed starter/bench games
10. **Later (separate session):** Retrain model to pick up `player_starter_prob` feature

---

## Verification

### SQL Spot-Check After Backfill
```sql
-- Compare actual starter data vs minutes proxy accuracy
SELECT
    started,
    CASE WHEN min >= 20 THEN true ELSE false END as proxy_started,
    COUNT(*) as cnt
FROM player_game_stats
WHERE started IS NOT NULL AND game_date >= '2025-10-01'
GROUP BY 1, 2
ORDER BY 1, 2;
```
Expected: ~5-10% disagreement rate (bench players with 20+ min, starters with <20 min).

### Validate Feature Store
```sql
-- Check player_starter_prob is being computed
SELECT player_id, game_date, games_started_l5,
       LEAST(COALESCE(games_started_l5, 0) / 5.0, 1.0) as starter_prob
FROM player_average_game_stats
WHERE game_date = '2026-03-17'
ORDER BY games_started_l5 DESC NULLS LAST
LIMIT 20;
```

---

## Data Leakage Analysis

Safe from data leakage:
1. `started` column is post-game data, only accessed via `shift(1)` in rolling averages
2. `games_started_l5` reflects games N-5 through N-1 (never current game)
3. `player_starter_prob` derived purely from `games_started_l5` (pre-game feature)
4. Injury features come from `report_date <= game_date` (pre-game reports)

---

## Important Notes

- **Model retrain required** — existing production model won't use `player_starter_prob` until retrained. The feature will exist in the data but the model's saved feature selection won't include it. Retrain is a separate step.
- **Backward compatible** — NULL `started` values fall back to the existing 20-min proxy via `fillna()`.
- **Railway safe** — CDN scraper change works on Railway. Backfill can use CDN (Railway-safe) or V2 API (local only).
- **No new dependencies** — uses existing scraper patterns, DB patterns, and feature store patterns.

## Files Changed Summary

| File | Type | Change |
|------|------|--------|
| Supabase migration | NEW | `started` boolean column on `player_game_stats` |
| `src/scrapers/nba_cdn_scraper.py` | MODIFY | Extract `starter` field (1 line in `transform_player_stats()`) |
| `src/processing/backfill_starter_data.py` | NEW | CDN-based historical backfill script |
| `src/processing/populate_average_stats.py` | MODIFY | Use actual `started` with fallback to proxy |
| `src/processing/populate_average_stats_incremental.py` | MODIFY | Use actual `started` with fallback + add `started` to SELECT |
| `src/models/feature_store.py` | MODIFY | `player_starter_prob` in 4 feature lists + 4 SQL query paths |
| `tests/test_populate_average_stats.py` | MODIFY | 3 new starter-related tests |
