# Handoff 013 — MLB Backtest Timeout Fix + at_bats Leakage Code Fix

> Part of [[Handoffs]]

**Date**: April 01, 2026 at 12:16 PM

## Summary

Fixed two critical bugs preventing the MLB backtest sweep from producing results: (1) Supavisor pooler silently stripping statement timeouts, causing all per-date feature queries to fail, and (2) the `at_bats` data leakage in the feature store SQL — renamed to `actual_at_bats` and excluded from NegBin feature candidates. Successfully ran first `batter_runs_scored` backtest sweep (+49.5% ROI best config, though likely inflated by leakage in the existing model).

## What Was Done

### Supavisor Timeout Fix (5 locations)
- **`src/processing/mlb/mlb_batter_matchup_features.py`** — Added `SET statement_timeout` to `compute_opposing_starter_bulk()` (300s) and `compute_platoon_splits_bulk()` (900s)
- **`src/models/mlb/mlb_batter_feature_store.py`** — Added `SET statement_timeout` to `get_features_for_date()` (300s) and `_load_single_season_training()` (300s)
- **`src/backtesting/mlb/run_mlb_sweep.py`** — Added `SET statement_timeout` to `_fetch_lines_for_date()` (300s), improved error logging with exception type

### at_bats Data Leakage Fix (3 files)
- **`src/models/mlb/mlb_batter_feature_store.py`** — Renamed `at_bats` → `actual_at_bats` in both SQL queries (training line 242, inference line 546). Column still available for binomial model fitting/filtering but won't be picked up as a NegBin feature candidate
- **`src/models/mlb/mlb_batter_train_pipeline.py`** — Updated binomial trainer references (`at_bats` → `actual_at_bats`), added `"actual_at_bats"` to NegBin trainer exclusion set (was previously missing — root cause of the leakage)
- **`src/models/mlb/mlb_daily_runner.py`** — Removed the `feat["at_bats"]` hack (line 497), replaced with `projected_ab` fallback

### Backtest Results
- **`batter_runs_scored`**: 6,732 predictions across 28 dates (Sep 2025). Best ROI: +49.5% (tau=0.9, z_max=0.25, mw=0.8, edge=0.10). Best Sharpe: 9.86 (tau=0.5, z_max=0.25, edge=0.15)

## Decisions Made

1. **Rename over drop**: Renamed `at_bats` → `actual_at_bats` rather than dropping it from the SQL query entirely, because the binomial model (hits) still needs actual at-bats for `n` (number of trials) during training and for filtering zero-AB rows
2. **Explicit SET timeout**: Used per-connection `SET statement_timeout` rather than trying to fix the engine-level `connect_args.options`, since Supavisor strips those parameters in transaction mode
3. **15-min timeout for platoon splits**: Heavy LATERAL join query that scans opponent pitching across 20 prior games per batter. 5 min wasn't enough — set to 15 min (runs once per backtest)

## Blockers and Open Questions

- **Retrain required**: The code fix is in place but `batter_total_bases` and `batter_runs_scored` models still have `at_bats` baked into their trained weights. Must retrain to eliminate leakage
- **Backtest results likely inflated**: Current sweep used models trained WITH `at_bats`. The backtest also had actual ABs available (via the feature store queries). True out-of-sample performance will only be known after retraining
- **Other batter stat sweeps not yet run**: Only `batter_runs_scored` was swept. Still need hits, total_bases, rbis, home_runs

## Recommended Next Steps

1. **Retrain `batter_total_bases` and `batter_runs_scored`** (Step 1.3) — Code fix is applied, just run:
   ```bash
   python src/models/mlb/mlb_batter_train_pipeline.py --stat total_bases --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100
   python src/models/mlb/mlb_batter_train_pipeline.py --stat runs --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100
   ```
2. **Re-run batter sweeps** (Step 1.6) — After retrain, sweep all batter stats to get clean backtest numbers
3. **Stripe integration** (Phase 3) — Next major product milestone
4. **Fund Kalshi account** — Enable live trading (Phase 7.9 code complete)

## Files to Read on Resume

- [[Execution-Plan]] — Steps 1.3 and 1.6 in progress
- [[MLB-Model]] — Updated with Session 13 bug fixes and backtest results
- `src/models/mlb/mlb_batter_feature_store.py` — at_bats → actual_at_bats rename
- `src/models/mlb/mlb_batter_train_pipeline.py` — NegBin exclusion set fix
- `src/backtesting/mlb/run_mlb_sweep.py` — Timeout fixes, ready for re-sweep

#mlb #backtest #timeout #data-leakage #session-13
