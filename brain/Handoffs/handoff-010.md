# Handoff 010

> Part of [[Handoffs]]

**Date**: March 31, 2026 at 5:17 PM
**Session**: 10

## Summary

Fixed critical MLB inference bugs causing broken dashboard predictions (all batters showing "under 0.5 bases"). Root caused to three issues: model naming mismatch for `batter_runs_scored`, hardcoded `stat="hits"` in the daily runner (missing stat-specific prop lines), and `at_bats` feature leakage in two models. Also fixed MLB RLS policies blocking all dashboard data. All 688 tests pass, linter clean.

## What Was Done

- **Fixed MLB RLS policies** — All 4 MLB tables (`mlb_daily_predictions`, `mlb_daily_prediction_samples`, `mlb_paper_bets`, `mlb_paper_trading_daily_log`) had `is_subscribed(auth.uid())` which blocked all users since subscription expired March 20. Applied Supabase migration replacing with `USING (true)` to match NBA behavior. Extended subscription to 2026-12-31 as belt-and-suspenders.
- **Fixed `mlb_model_suite.py` naming** — `STAT_TO_NEGBIN_MODEL_NAME` mapped `batter_runs_scored` → `batter_runs_scored` but artifacts are named `batter_runs_*`. Fixed to `batter_runs`. Also fixed `STAT_TO_NEGBIN_SHORT` from `runs_scored` → `runs`.
- **Fixed `mlb_daily_runner.py` per-stat prop lines** — Added `_bulk_fetch_batter_prop_lines()` method that queries all batter prop lines in one DB call. Per-stat loop now injects correct `prop_line_{market_key}` instead of every stat getting only `prop_line_batter_hits`.
- **Fixed `mlb_daily_runner.py` at_bats proxy** — Models expecting `at_bats` now get `projected_ab` (= `max(avg_ab_l5, 1.0)`) as a pre-game estimate instead of defaulting to 0.
- **Verified backtest sweep** — `BATTER_STAT_FS_MAP` in `run_mlb_sweep.py` already had correct `"batter_runs_scored": "runs"` mapping.
- **Ran full test suite** — 688 tests pass, 0 failures. Ruff linter clean.

## Decisions Made

- **RLS open-access for MLB**: Matching NBA's `USING (true)` policy since Stripe isn't built yet. When Phase 3 (Stripe) ships, can re-add subscription gating on both NBA and MLB together.
- **`at_bats` → `projected_ab` proxy**: Temporary fix using L5 average AB as pre-game estimate. Models trained with actual game-day `at_bats` will be noisier with this proxy, especially `batter_total_bases` where it's 1 of only 6 features. Proper fix is retraining without `at_bats`.

## Blockers and Open Questions

- **`batter_total_bases` needs retraining** (HIGH priority) — `at_bats` is 1 of only 6 features. The proxy mapping is a bandaid. Should replace `at_bats` with `batter_avg_ab_l5` in the training feature set.
- **`batter_runs_scored` needs retraining** (MEDIUM priority) — Same `at_bats` leakage but less critical (28 other features). Still should retrain.
- **MLB has no automated line re-scraping + inference rerun** — Unlike NBA, MLB inference runs once. Not wired into Railway for periodic re-runs as lines move.
- **No batter backtests run yet** — Sweep commands provided for total_bases, rbis, runs_scored, home_runs. User was starting to run them when bugs were discovered.

## Recommended Next Steps

1. **Retrain `batter_total_bases` without `at_bats`** (HIGH) — Replace `at_bats` with `batter_avg_ab_l5` in feature selection/training. This is the most impactful fix since it's 1 of 6 features.
   - File: `src/models/mlb/mlb_batter_train_pipeline.py`
   - File: `src/models/mlb/mlb_batter_feature_store.py` (training data query)
   - Complexity: Medium

2. **Retrain `batter_runs_scored` without `at_bats`** (MEDIUM) — Same leakage fix.
   - Complexity: Small (same process as total_bases)

3. **Run batter backtest sweeps** — Commands ready from this session:
   ```
   python src/backtesting/mlb/run_mlb_sweep.py --start 2025-07-01 --end 2025-09-28 --stats batter_total_bases --tau none 0.5 0.9 1.5 --edge 0.02 0.05 0.08 0.10 --kelly 0.10 0.125 0.15 0.20 --z-max 0.5 0.75 1.0 --max-weight 0.50 0.65 0.80
   ```
   (Similar for rbis, runs_scored, home_runs)
   - Complexity: Small (just run commands)

4. **Wire MLB line re-scraping + inference rerun** — Match NBA's periodic pattern.
   - Complexity: Medium

## Files to Read on Resume

- [[MLB-Model]] — Updated model status with training/retraining needs
- [[Execution-Plan]] — Updated Phase 1 progress
- `src/models/mlb/mlb_daily_runner.py` — Fixed inference pipeline (lines 458-494)
- `src/models/mlb/mlb_model_suite.py` — Fixed naming mappings (lines 44-57)
- `src/models/mlb/mlb_batter_feature_store.py` — Feature store with `at_bats` in training query (line 525)

#handoff #mlb #bugfix
