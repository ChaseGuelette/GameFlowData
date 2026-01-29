# B2 + B3 + B4: Rest, Trend, and Minutes Stability Features

## Overview

Three feature groups from ACTIONITEMS.md Track B. All target new signal sources the market may price imperfectly.

| Group | New Features | Target Lists | Storage |
|---|---|---|---|
| **B2** Rest/B2B | `rest_days`, `is_back_to_back`, `games_in_last_7_days` | `MINUTES_FEATURES` | Pre-computed in `player_average_game_stats` |
| **B3** L3 + Trend + Std | `player_avg_{stat}_l3`, `player_{stat}_l3_l15_ratio`, `player_std_{stat}_l5` (×4 stats + min L3) | `RATE_FEATURES_*` + `MINUTES_FEATURES` | 10 new columns in `player_average_game_stats` |
| **B4** Minutes Stability | `player_min_std_l5` (shared w/ B3), `player_min_floor_l5`, `player_games_started_l5` | `MINUTES_FEATURES` | 2 new columns in `player_average_game_stats` |

**Total new features:** 20 (3 B2 + 13 B3 + 4 B4, with `player_min_std_l5` shared)
**Total new DB columns:** 14

---

## 1. Database Migration (via `execute_sql`)

Add 14 columns to `player_average_game_stats`:

```sql
-- B3: L3 averages
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS avg_min_l3 numeric(5,2);
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS avg_pts_l3 numeric(5,2);
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS avg_reb_l3 numeric(5,2);
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS avg_ast_l3 numeric(5,2);
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS avg_fg3m_l3 numeric(5,2);

-- B3/B4: Rolling L5 standard deviation
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS std_min_l5 numeric(5,2);
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS std_pts_l5 numeric(5,2);
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS std_reb_l5 numeric(5,2);
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS std_ast_l5 numeric(5,2);
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS std_fg3m_l5 numeric(5,2);

-- B4: Minutes stability
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS min_floor_l5 numeric(5,2);
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS games_started_l5 smallint;

-- B2: Schedule density
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS rest_days smallint;
ALTER TABLE player_average_game_stats ADD COLUMN IF NOT EXISTS games_last_7d smallint;
```

No new indexes needed — existing `(player_id, game_date DESC)` index covers the LATERAL JOIN lookups.

---

## 2. Backfill Script: `src/processing/populate_average_stats.py`

### New constants
```python
B3_B4_STATS = ["min", "pts", "reb", "ast", "fg3m"]
STARTER_MINUTES_THRESHOLD = 20  # No start_position col in DB; proxy via minutes
```

### New function: `calculate_b3_b4_features(df)`

Called after `calculate_player_basic_averages()`. Computes:

1. **L3 rolling averages** for each B3_B4_STAT — `shift(1)` + `rolling(3, min_periods=1).mean()`
2. **L5 rolling std** for each B3_B4_STAT — `shift(1)` + `rolling(5, min_periods=2).std()`
3. **L5 rolling min** for minutes — `shift(1)` + `rolling(5, min_periods=1).min()` → `min_floor_l5`
4. **L5 games started** — `(min >= 20)` flag, `shift(1)`, `rolling(5, min_periods=1).sum()` → `games_started_l5`
5. **Rest days** — `groupby.shift(1)` on `game_date`, compute days diff, clip to 7
6. **Games in last 7 days** — calendar-based count of prior games within 7 days

Uses same `rolling_with_groupby()` helper and `shift(1)` no-leakage pattern as existing code.

### New helper: `_count_games_in_window(df, group_cols, days=7)`

For each game, counts how many prior games (same player+season) occurred within `days` calendar days. Loop-based per group since it's calendar-window not game-window.

### Modify `insert_player_basic_averages()`

Append new column names to the insert column list:
- `avg_{stat}_l3` for each B3_B4_STAT
- `std_{stat}_l5` for each B3_B4_STAT
- `min_floor_l5`, `games_started_l5`
- `rest_days`, `games_last_7d`

Add rounding for `std_*` and `min_floor_*`.

### Modify `main()` flow

```python
if args.table in ["player", "all"]:
    df = fetch_player_game_stats(engine, season_filter, from_year)
    df = calculate_player_basic_averages(df)
    df = calculate_b3_b4_features(df)          # NEW
    insert_player_basic_averages(engine, df)
```

---

## 3. Feature Store: `src/models/feature_store.py`

### 3a. Feature list updates

**MINUTES_FEATURES** — append:
```python
# B2
"rest_days",
"is_back_to_back",
"games_in_last_7_days",
# B3
"player_avg_min_l3",
# B4
"player_min_std_l5",
"player_min_floor_l5",
"player_games_started_l5",
```

**RATE_FEATURES_PTS** — append:
```python
"player_avg_pts_l3",
"player_pts_l3_l15_ratio",
"player_std_pts_l5",
```

**RATE_FEATURES_REB** — append:
```python
"player_avg_reb_l3",
"player_reb_l3_l15_ratio",
"player_std_reb_l5",
```

**RATE_FEATURES_AST** — append:
```python
"player_avg_ast_l3",
"player_ast_l3_l15_ratio",
"player_std_ast_l5",
```

**RATE_FEATURES_THREES** — append:
```python
"player_avg_fg3m_l3",
"player_fg3m_l3_l15_ratio",
"player_std_fg3m_l5",
```

### 3b. SQL changes — all 3 bulk query paths

Apply to `get_training_dataset()`, `get_features_for_date()`, `get_features_for_date_range()`.

**Extend `p_avg` LATERAL JOIN SELECT** to also fetch:
```sql
avg_min_l3, avg_pts_l3, avg_reb_l3, avg_ast_l3, avg_fg3m_l3,
std_min_l5, std_pts_l5, std_reb_l5, std_ast_l5, std_fg3m_l5,
min_floor_l5, games_started_l5,
rest_days as stored_rest_days, games_last_7d
```

**Add new SELECT columns** (with COALESCE + aliases):
```sql
-- B3: L3 averages
COALESCE(p_avg.avg_min_l3, 0) as player_avg_min_l3,
COALESCE(p_avg.avg_pts_l3, 0) as player_avg_pts_l3,
COALESCE(p_avg.avg_reb_l3, 0) as player_avg_reb_l3,
COALESCE(p_avg.avg_ast_l3, 0) as player_avg_ast_l3,
COALESCE(p_avg.avg_fg3m_l3, 0) as player_avg_fg3m_l3,

-- B3: Momentum ratios (L3/L15 for PTS, L3/L5 for others)
CASE WHEN COALESCE(p_avg.avg_pts_l15, 0) > 0
     THEN COALESCE(p_avg.avg_pts_l3, 0) / p_avg.avg_pts_l15
     ELSE 1.0 END as player_pts_l3_l15_ratio,
CASE WHEN COALESCE(p_avg.avg_reb_l5, 0) > 0
     THEN COALESCE(p_avg.avg_reb_l3, 0) / p_avg.avg_reb_l5
     ELSE 1.0 END as player_reb_l3_l15_ratio,
CASE WHEN COALESCE(p_avg.avg_ast_l5, 0) > 0
     THEN COALESCE(p_avg.avg_ast_l3, 0) / p_avg.avg_ast_l5
     ELSE 1.0 END as player_ast_l3_l15_ratio,
CASE WHEN COALESCE(p_avg.avg_fg3m_l5, 0) > 0
     THEN COALESCE(p_avg.avg_fg3m_l3, 0) / p_avg.avg_fg3m_l5
     ELSE 1.0 END as player_fg3m_l3_l15_ratio,

-- B3/B4: Standard deviations
COALESCE(p_avg.std_min_l5, 0) as player_min_std_l5,
COALESCE(p_avg.std_pts_l5, 0) as player_std_pts_l5,
COALESCE(p_avg.std_reb_l5, 0) as player_std_reb_l5,
COALESCE(p_avg.std_ast_l5, 0) as player_std_ast_l5,
COALESCE(p_avg.std_fg3m_l5, 0) as player_std_fg3m_l5,

-- B4: Minutes stability
COALESCE(p_avg.min_floor_l5, 0) as player_min_floor_l5,
COALESCE(p_avg.games_started_l5, 0) as player_games_started_l5,

-- B2: Rest/schedule
LEAST(COALESCE(p_avg.stored_rest_days, 3), 7) as rest_days,
CASE WHEN COALESCE(p_avg.stored_rest_days, 3) = 1 THEN 1 ELSE 0 END as is_back_to_back,
COALESCE(p_avg.games_last_7d, 2) as games_in_last_7_days,
```

**Note on ratio denominators:** PTS uses L15 (we have `avg_pts_l15`). REB/AST/THREES use L5 (we don't store their L15 in the LATERAL). The feature name says "l3_l15_ratio" but for REB/AST/THREES it's actually L3/L5. This is fine — the signal is "recent form vs. recent baseline" either way.

### 3c. Training path cleanup

In `get_training_dataset()`: remove the Python-based `_get_travel_and_rest_features()` merge block (lines ~896-937). Rest features now come from the SQL query. Keep hardcoded zeros for `opp_rest_days`, `opp_travel_dist`, `opp_is_back_to_back`, `travel_dist` (not in feature lists).

### 3d. Inference paths cleanup

In `get_features_for_date()` and `get_features_for_date_range()`: remove `rest_days`, `travel_dist`, `is_back_to_back` from the hardcoded-zeros block (now from SQL). Keep `opp_*` zeros.

### 3e. Single-player path: `_get_player_rolling_stats()`

Extend the SQL query to fetch all new columns from `player_average_game_stats`. Update the return dict mapping and None-fallback dict. Compute momentum ratios in Python (safe division, default 1.0). Add `rest_days`, `is_back_to_back`, `games_in_last_7_days` to the return dict.

---

## 4. Tests

### `tests/test_feature_store.py`
- Assert all new features present in their respective lists

### `tests/test_populate_average_stats.py`
- Test `calculate_b3_b4_features()`: L3 avg no-leakage, std correctness, min floor, games_started, rest_days, games_last_7d

---

## 5. Update `database/schema.sql`

Add the 14 new columns to the `player_average_game_stats` table definition.

---

## 6. Files Changed Summary

| File | Changes |
|------|---------|
| `src/models/feature_store.py` | Feature lists (5), SQL queries (3 bulk + 1 helper), rest cleanup |
| `src/processing/populate_average_stats.py` | New `calculate_b3_b4_features()`, `_count_games_in_window()`, insert columns |
| `tests/test_feature_store.py` | New assertions |
| `tests/test_populate_average_stats.py` | New tests for B3/B4 computation |
| `database/schema.sql` | 14 new columns |

---

## 7. Post-Implementation: Backfill Execution Plan

After code changes pass tests:

1. **Add DB columns** via Supabase `execute_sql` (the ALTER TABLE block from Section 1)
2. **Run backfill**: `python -m src.processing.populate_average_stats --table player --from-year 2021`
   - This truncates and reloads `player_average_game_stats` with all columns (existing + new)
   - Takes a few minutes for ~150k+ rows
3. **Verify**: `SELECT player_id, game_date, avg_pts_l3, std_pts_l5, min_floor_l5, rest_days FROM player_average_game_stats WHERE avg_pts_l3 IS NOT NULL LIMIT 10;`
4. **Retrain models** (new features require retraining)

---

## 8. Verification

1. `pytest` — all tests pass
2. `ruff check` — clean lint
3. Each feature list has its expected new features
4. SQL consistency across all 4 query paths
5. Backfill produces non-null values
