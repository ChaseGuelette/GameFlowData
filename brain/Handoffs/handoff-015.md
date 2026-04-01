# Handoff 015

> Part of [[Handoffs]]

**Date**: April 01, 2026 at 3:08 PM

## Summary

Major database optimization and developer infrastructure session. Dropped 47 GB of unused indexes from Supabase (116 GB → 69 GB), fixed MLB pipeline bugs (schedule scraper + averages script), fixed history page P&L display, implemented MLB backtest matchup cache, analyzed batter_rbis sweep, fixed RLS performance warnings, and built a local Postgres sync system for offline training/backtesting.

## What Was Done

### Database Optimization (47 GB freed)
- Dropped 7 unused indexes on `raw_player_props_combined` — **45 GB saved** (biggest: `idx_combined_dedupe` at 34 GB with only 106 scans)
- Dropped 25+ unused indexes across other tables — **2 GB saved**
- Dropped duplicate indexes on `player_position_history` (3→1) and `team_allowed_by_position` (2→1)
- Fixed 9 RLS policies on `user_subscriptions`, `user_profiles`, `user_bets` — changed `auth.uid()` to `(select auth.uid())` for per-row evaluation optimization
- Created index `idx_mlb_pitching_game_team_starter` on `mlb_player_game_stats_pitching (game_id, team_id, is_starter)` for LATERAL join performance
- Created index `idx_mlb_avg_statcast_pitching_player_date` on `mlb_player_average_statcast_pitching (player_id, game_date DESC)`

### MLB Backtest Matchup Cache
- Modified `src/backtesting/mlb/run_mlb_sweep.py` — precomputes `compute_opposing_starter_bulk()` and `compute_platoon_splits_bulk()` once per season instead of per-date (was 86+ calls → now 1)
- Modified `src/models/mlb/mlb_batter_feature_store.py` — added `matchup_cache` param to `enrich_with_matchup_features()` and `get_features_for_date()`
- Bumped platoon splits query timeout from 5 min to 15 min in `src/processing/mlb/mlb_batter_matchup_features.py`

### MLB Pipeline Bug Fixes
- Fixed `src/scrapers/mlb/mlb_stats_scraper.py` — removed `if game_pk in existing: continue` that prevented game status updates from "Scheduled" to "Final"
- Fixed `src/processing/mlb/mlb_populate_averages_incremental.py` — added missing `--type` CLI argument that the daily job was passing

### Dashboard Fixes
- Fixed `dashboard/src/lib/hooks/useHistoryData.ts` — removed `.limit(PAGE_SIZE)` that truncated history P&L calculations
- Fixed `dashboard/src/app/(protected)/history/page.tsx` — added "ALL" lifetime date preset

### Local Postgres for Training/Backtesting
- Created `scripts/sync_local_db.py` — syncs 28 tables (12 MLB + 16 NBA) from Supabase to local Postgres using psycopg2 COPY. Supports incremental sync and `--full` refresh.
- Modified `src/db/client.py` — added `get_engine(local=True)` that connects to `LOCAL_DATABASE_URL`
- Added `--local` flag to 5 scripts: `run_mlb_sweep.py`, `run_backtest.py`, `run_sweep.py`, `train_pipeline.py`, `mlb_batter_train_pipeline.py`

### Backtest Analysis
- Analyzed batter_rbis sweep output. User selected config #465: tau=0.9, z_max=0.25, mw=0.8, edge=0.08 (37 bets, 62.2% win, +$11,838, 7.7% DD)

## Decisions Made

1. **Prioritize absolute profit over ROI% for sweep selection** — User explicitly prefers configs with higher total profit at reasonable volume, not highest ROI% or Sharpe with few bets.
2. **Local Postgres for batch jobs** — Heavy training/backtesting should run against a local DB to avoid hammering Supabase with long-running queries. No statement timeouts, no resource warnings.
3. **Keep `raw_player_props_combined` data but drop bloated indexes** — The data itself is needed, but 54 GB of indexes (2x the data!) was wasteful. Kept only 4 actively-used indexes.

## Blockers and Open Questions

- All code changes are **local and uncommitted** — need to push to Git and Railway for pipeline fixes to take effect
- Local Postgres setup requires user to install Postgres (or Docker) — one-time setup not yet done
- `raw_player_props_archive` (17 GB, 68M rows) — still sitting in DB, unclear if it's needed
- `raw_game_lines_staging` (1.9 GB back to 2020) — old staging data could be purged

## Recommended Next Steps

1. **Commit and push changes** — MLB pipeline fixes need to reach Railway for the daily job to work
2. **Install local Postgres** and run `python scripts/sync_local_db.py --full --sport mlb` for first sync
3. **Retry batter_runs_scored sweep** with `--local` flag — the platoon query should work now
4. **Run sweeps for remaining batter stats** (hits, total_bases, home_runs) using the absolute-profit evaluation framework
5. **Continue Step 1.3** — retrain `batter_total_bases` and `batter_runs_scored` with the at_bats leakage fix already applied

## Files to Read on Resume

- `scripts/sync_local_db.py` — The new local DB sync script
- `src/db/client.py` — Updated with `get_engine(local=True)` support
- `src/backtesting/mlb/run_mlb_sweep.py` — Matchup cache optimization + `--local` flag
- `src/scrapers/mlb/mlb_stats_scraper.py` — Schedule update bug fix
- [[Execution-Plan]] — Phase 1 and Phase 5 status updates
