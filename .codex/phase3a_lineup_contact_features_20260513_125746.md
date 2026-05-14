# Implementation Spec: MLB Pitcher K Phase 3A Lineup Contact Features

## Goal
Add high-signal projected opposing lineup contact/K profile features for `pitcher_strikeouts`, excluding umpire features for now because current umpire data coverage is too sparse.

## Context / Baselines
- Current clean Phase 2 artifact: `src/models/mlb/artifacts/mlb_run_20260513_111207`.
- Phase 2 under-only baselines to beat later:
  - Raw under edge=0.05: 131 bets, +21.72% ROI, Sharpe 2.58, +$9,596.
  - BL under tau=0.90 z_max=0.25 mw=0.8 edge=0.02: 110 bets, +34.68% ROI, Sharpe 4.00, +$7,873.
- Data refresh already ran before this spec:
  - `mlb_stats_scraper --season 2026 --boxscores-only` found no missing boxscores.
  - `mlb_statcast_scraper --backfill --start-date 2026-04-13 --end-date 2026-05-10` completed.
  - `mlb_populate_averages --table batting --season 2026` completed.
  - `mlb_populate_statcast_averages --table batting --season 2026` completed.
  - affected tables synced to local.
- Local coverage after sync:
  - `mlb_game_lineups`: 7,878 rows, 2026-04-15..2026-05-13.
  - `mlb_player_average_batting`: max 2026-05-12.
  - `mlb_player_average_statcast_batting`: max 2026-05-12.
  - lineup coverage has K/contact profiles for ~6507/6511 non-pitcher lineup rows.

## Files to edit
- `src/processing/mlb/mlb_matchup_features.py`
- `src/models/mlb/mlb_feature_store.py`
- Add a focused test file if practical, e.g. `tests/test_mlb_lineup_contact_features.py`

## Non-goals
- Do not add umpire features now.
- Do not add pitcher-specific umpire interactions.
- Do not add hand-crafted pitch-mix interaction products.
- Do not add battery/catcher-history features.
- Do not alter training calibration logic, probability logic, empirical CDF behavior, BL/tau backtest logic, or model gates.
- Do not add DB writes/migrations.

## Required new/updated features
Keep existing compatibility features and add the following numeric columns for pitcher K:

Existing feature to repair/improve:
- `projected_lineup_k_pct`
  - Compute from `mlb_player_average_batting` using time-safe latest rows strictly before the game date.
  - Preferred rate per batter: `avg_so_szn / avg_pa_szn` when `avg_pa_szn > 0`.
  - Fallback rate per batter: `avg_so_l20 / avg_pa_l20`, then `avg_so_l10 / avg_pa_l10`, then `0.22`.
  - Use lineup slot weights, not simple unweighted average.

New contact profile features:
- `projected_lineup_whiff_pct`
- `projected_lineup_chase_pct`
- `projected_lineup_contact_rate`

Use `mlb_player_average_statcast_batting` with time-safe latest rows strictly before the game date.
For each batter, prefer season average, then l10, then l5, then neutral defaults.
Recommended defaults:
- K%: `0.22`
- whiff: `0.22`
- chase: `0.28`
- contact: `1.0 - whiff`

New handedness profile features:
- `projected_lineup_same_hand_k_pct`
- `projected_lineup_opposite_hand_k_pct`
- `projected_lineup_hand_k_delta`

Use pitcher throws and batter `mlb_players.bats`:
- same-hand means non-switch batter `bats == pitcher_throws`.
- opposite bucket includes opposite-side and switch hitters.
- If bucket empty, fall back to overall lineup K%.
- delta = same_hand_k_pct - opposite_hand_k_pct.

Top/middle/bottom concentration features:
- `projected_lineup_top3_k_pct`
- `projected_lineup_mid3_k_pct`
- `projected_lineup_bot3_k_pct`
- `projected_lineup_k_concentration`

Use lineup positions:
- top: 1-3
- mid: 4-6
- bot: 7-9
- If a group is empty, fallback to overall lineup K%.
- concentration = max(top3, mid3, bot3) - min(top3, mid3, bot3).

Compatibility feature:
- Keep `pct_opp_lineup_same_hand` behavior, but it can be computed from the same query/data. Do not remove/rename it.

## Slot weighting
Use modest projected-PA-style weights by lineup position. Suggested weights:
- 1: 1.12
- 2: 1.09
- 3: 1.06
- 4: 1.03
- 5: 1.00
- 6: 0.97
- 7: 0.94
- 8: 0.91
- 9: 0.88
Normalize by sum of present weights.

## Bulk path requirements
In `compute_lineup_features_bulk(engine, season)`:
- Return columns:
  - `player_id`, `game_id`, existing columns, and all new feature columns.
- Use time-safe latest player average rows before each game date, not global latest rows after the game.
- Avoid Python row-by-row DB queries; use one SQL query and pandas post-processing if needed.
- Handle missing `mlb_game_lineups` by returning an empty DataFrame with all expected columns.
- Fill missing games with neutral defaults, as current code does.

## Single-game inference requirements
In `get_lineup_k_features(...)`:
- Return existing and new feature keys.
- Use the same defaults and formulas as bulk.
- Query only rows for that game/team and latest average rows before `game_date`.
- Work if only partial lineups are present; require >=3 batters like current code.

## Feature store requirements
In `src/models/mlb/mlb_feature_store.py`:
- Add new feature names to `PITCHER_K_FEATURES` under lineup/contact section.
- Add neutral defaults in `_add_derived_features` or equivalent default handling so training/backtest/inference never miss the columns.
- Update `enrich_with_matchup_features` merge/default behavior so suffixed computed lineup values override neutral defaults if base defaults already exist. This repo has had suffix/default merge pitfalls before.
- Ensure `get_player_game_features` includes values returned by `get_lineup_k_features`.
- Ensure `get_features_for_date`/training path can produce aligned columns.

## Tests / validation
Use strict TDD if adding tests.
At minimum run:

1. Syntax/compile:
`venv/Scripts/python.exe -m py_compile src/processing/mlb/mlb_matchup_features.py src/models/mlb/mlb_feature_store.py`

2. Focused tests if added:
`venv/Scripts/python.exe -m pytest tests/test_mlb_lineup_contact_features.py -q`

3. Runtime smoke using local DB:
Create/run a short one-off Python command or script that:
- imports `MLBFeatureStore`
- uses `get_engine(local=True)`
- calls `get_features_for_date('2026-05-10')`
- prints non-null counts and `nunique` for all new lineup features
- fails/alerts if all new features are missing/all default/all single-valued.

Expected after current data refresh:
- new lineup K/contact columns should have meaningful non-null counts on 2026-05-10.
- `projected_lineup_k_pct` should have far more than 2 unique values over the backtest window.

## Review criteria
- Diff touches only listed files plus optional focused test.
- No DB writes or migrations.
- No umpire features added.
- No broad refactors.
- Existing `projected_lineup_k_pct` and `pct_opp_lineup_same_hand` remain available.
- New columns are numeric and feature selection can pick them automatically.
