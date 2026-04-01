# Handoff 016 — MLB Pipeline Debugging: Supavisor timeout, bet resolution, Discord alerts

> Part of [[Handoffs]]

**Date**: April 01, 2026 at 3:24 PM

## Summary

Investigated and fixed multiple interrelated MLB pipeline failures on Railway. The daily stats job was crashing on Step 4 (batting rolling averages) due to Supavisor stripping connection-level timeouts. Paper bets weren't resolving because no 2026 game stats existed in the database. Added missing MLB Discord P&L notifications and disabled paper bet placement until leaky models are retrained.

## What Was Done

### Pipeline Fixes (all committed & pushed to Railway)
- **Supavisor timeout fix** (`src/processing/mlb/mlb_populate_averages_incremental.py`): Added explicit `SET statement_timeout = '120000'` to `fetch_batter_season_games()` and `fetch_pitcher_season_games()`. Supavisor transaction-mode pooler strips `-c` startup params, so the 8s role-level timeout was killing the queries.
- **Bet resolution column fix** (`src/paper_trading/mlb_paper_trader.py`): Changed `pitcher_outs` mapping from `"outs"` to `"outs_recorded"` — actual column name in `mlb_player_game_stats_pitching`.
- **MLB Discord P&L** (`src/orchestration/mlb_daily_stats_job.py`): Added `_send_mlb_pnl_summary()` function that queries `mlb_paper_trading_daily_log` and calls `send_pnl_summary_sync(sport="mlb")` after bet resolution.
- **Disabled paper bets** (`src/orchestration/scheduler.py`): Added `--skip-bets` flag to MLB inference job. Also split Kalshi refresh into separate NBA/MLB jobs.
- **Improved error logging** (`src/orchestration/mlb_daily_stats_job.py`): Capture stdout on `CalledProcessError`, fixed stderr truncation direction (head → tail).

### Data Backfill (done locally)
- Ran `mlb_stats_scraper.py --season 2026` — updated 2431 games, finalized 77 to "Final" status, scraped boxscores.
- Ran bet resolution manually — 946 of 952 pending bets resolved (467W / 404L / 75C). 287 from April 1 filtered by `exclude_today=True`.

### Brain Updates
- Updated [[Scheduling]] with full MLB job schedule and Kalshi NBA/MLB split
- Updated [[Known-Issues]] with resolved bugs and active `--skip-bets` note
- Updated [[MLB-Model]] with Session 15 bugs fixed
- Updated [[Discord-Bot]] with MLB P&L alert details
- Updated [[Execution-Plan]] steps 1.3 and 1.7

## Decisions Made

1. **Disabled MLB paper bet placement** — `batter_total_bases` and `batter_runs_scored` models have at_bats leakage. No point placing bets with compromised models. Re-enable after retraining + backtesting.
2. **120-second statement timeout** for averages queries — generous enough for season-to-date aggregation, well under Railway's process limits.
3. **Split Kalshi refresh into NBA/MLB** — separate job functions and cron entries for cleaner scheduling and independent failure handling.

## Blockers and Open Questions

- **2 models need retraining**: `batter_total_bases` and `batter_runs_scored` (at_bats leakage). Training commands ready, just need to execute locally (~30-60 min each with tuning).
- **287 pending bets from April 1**: Will auto-resolve at tomorrow's 10 AM ET daily stats job — first test of the deployed fixes.
- **Railway deployment verification needed**: Pushed fixes but won't know if the Supavisor timeout fix works until the 10 AM ET job runs tomorrow.

## Recommended Next Steps

1. **Verify Railway daily stats succeeds tomorrow 10 AM ET** — check Discord for success/failure alert and MLB P&L summary. This confirms the Supavisor timeout fix works.
2. **Retrain leaky models** (locally):
   ```bash
   python src/models/mlb/mlb_batter_train_pipeline.py --stat total_bases --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100
   python src/models/mlb/mlb_batter_train_pipeline.py --stat runs --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100
   ```
3. **Run backtest sweeps** on retrained models to confirm ROI without leakage inflation.
4. **Re-enable `--skip-bets`** in scheduler.py once models are validated.
5. **Commit remaining 13 modified files** in working tree (backtesting, Kalshi scrapers, brain docs, db/client changes from previous sessions).

## Files to Read on Resume

- [[MLB-Model]] — Full model status, bugs fixed, training commands
- [[Scheduling]] — Railway job schedule (MLB + NBA + Kalshi)
- [[Known-Issues]] — Active bugs including disabled paper bets
- [[Execution-Plan]] — Phase 1 progress (steps 1.3, 1.6 still in_progress)
- [[handoff-015]] — Previous session context (DB optimization, local Postgres sync)
