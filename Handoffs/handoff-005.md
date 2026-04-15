> Part of [[Handoffs]]

**Date**: April 15, 2026 at 2:00 PM

## Summary

Session 33 trained the `batter_hrr` (H+R+RBI combined) model to strong calibration, built the full MLB lineup and active roster scraper pipeline, and did the groundwork to unblock the BL sweep. The main pending item is running the `batter_hits_runs_rbis` historical odds backfill (commands ready), then the linker, then the sweep.

---

## What Was Done

### batter_hrr Model Trained
- **Artifacts**: `src/models/mlb/artifacts/mlb_run_batter_hrr_20260415_122937/`
- 87,336 training rows (2023 + 2024), 28 features selected from 75 via NLL-based selection
- Best trial 21, val NLL = 1.7210 | bias ratio = 0.9950 (0.5% underprediction — excellent)
- Zero fraction gap = −0.013 (small). ECE gate passes comfortably
- Top features: `batter_woba_szn`, `lineup_position`, `line_total`, `batter_avg_pa_l5`, `opp_pitcher_avg_k_per_9_l5`
- Model suite fix: added `batter_hrr` to `STAT_TO_NEGBIN_MODEL_NAME` + `STAT_TO_NEGBIN_SHORT` in `src/models/mlb/mlb_model_suite.py`

### MLB Lineup + Roster Scrapers Built
- **New**: `src/scrapers/mlb/mlb_lineup_scraper.py` — fetches confirmed batting lineups from MLB Stats API boxscore endpoint pre-game. Per-game: checks `battingOrder`, stores positions 1–9, marks pitchers
- **New**: `src/scrapers/mlb/mlb_roster_scraper.py` — fetches 26-man active roster for all 30 teams via `/teams/{teamId}/roster?rosterType=active`
- **New**: `src/orchestration/mlb_lineup_scraper_job.py` — thin job wrapper, logs to `mlb_lineup_scraper.log`
- **New**: `src/orchestration/mlb_roster_scraper_job.py` — thin job wrapper, logs to `mlb_roster_scraper.log`
- **Modified**: `src/orchestration/scheduler.py` — added roster job at 9:30 AM ET, lineup jobs at 12:45 PM ET + 6:10 PM ET
- **Modified**: `src/models/mlb/mlb_daily_runner.py` — added `_filter_batters_by_lineup()`: per-game filter, falls back to all active batters for games with no lineup confirmed yet
- **Migration 025 applied**: `mlb_game_lineups` + `mlb_active_roster` tables with public read RLS

### BL Sweep Unblocked
- Confirmed `batter_total_bases` already has **6.47M rows** — no backfill needed
- `batter_hits_runs_rbis` has zero rows — full historical backfill needed (2023-09-30)
- **Modified**: `src/backtesting/mlb/run_mlb_sweep.py`
  - Added `"batter_hrr": "hrr"` to `BATTER_STAT_FS_MAP`
  - Added `STAT_TO_MARKET_KEY = {"batter_hrr": "batter_hits_runs_rbis"}`
  - Fixed `_fetch_lines_for_date()` to translate stat name → DB market key before querying, then remap back after fetch

### Research / Analysis
- Confirmed NBA injury features DO survive model feature selection: `team_out_usg_sum`, `opp_out_min_sum`, `team_out_count`, `opp_out_count`, etc. all in `selected_features.json`
- Confirmed lineup/roster data = inference-time filters only (not model features) — same pattern as `_filter_injured_players()` in NBA runner

---

## Decisions Made

- **batter_hrr is Kalshi-only** — no sportsbook equivalent. BL sweep will use `batter_hits_runs_rbis` sportsbook lines as a proxy market (same underlying stat, close enough for BL parameter optimization)
- **Lineup/roster = inference-time filters, not model features** — binary same-day context can't be learned historically. The filter pattern (`_filter_batters_by_lineup`) is the right approach, identical to NBA injury filtering
- **`prop_line_batter_hrr` at inference should stay 0** — training was done with it always = 0 (no sportsbook line exists). Passing a real Kalshi line would be out-of-distribution

---

## Blockers and Open Questions

- **Kalshi KXMLBHRR ticker is a placeholder** — needs verification against a live Kalshi HRR market before batter_hrr goes live. Check `src/scrapers/kalshi/kalshi_utils.py` KALSHI_PROP_SERIES["mlb"]
- **`batter_hits_runs_rbis` backfill not yet run** — sweep will return no bets until this is done

---

## Recommended Next Steps

1. **(Highest priority) Run odds backfill** — start with dry run to check credit cost, then full run:
   ```bash
   # Dry run first
   python -m src.scrapers.mlb.mlb_player_props_scraper \
       --markets batter_hits_runs_rbis \
       --start-date 2023-04-01 --end-date 2025-09-30 --dry-run

   # Actual backfill (resumable)
   python -m src.scrapers.mlb.mlb_player_props_scraper \
       --markets batter_hits_runs_rbis \
       --start-date 2023-04-01 --end-date 2025-09-30
   ```

2. **Run the linker** after backfill completes:
   ```bash
   python src/processing/mlb/mlb_linker.py backfill
   ```

3. **Run BL sweep**:
   ```bash
   python src/backtesting/mlb/run_mlb_sweep.py --local \
       --start 2025-07-01 --end 2025-09-28 \
       --stats batter_hrr \
       --tau none 0.03 0.05 0.10 0.25 0.5 0.75 0.9 \
       --edge 0.02 0.05 0.08 0.10 0.12 0.15 \
       --kelly 0.125 \
       --z-max 0.25 0.5 0.75 1.0 \
       --max-weight 0.50 0.65 0.80
   ```

4. **Promote if gates pass** (ROI > 0%, Z-score > 1.5):
   - Add entry to `STAT_BL_CONFIGS` in `src/models/mlb/mlb_stat_config.py`
   - Copy artifacts to `src/models/mlb/artifacts/production/`
   - Run combined backtest to verify all 4 stats work together

5. **Verify Kalshi HRR ticker** — confirm `KXMLBHRR` in `src/scrapers/kalshi/kalshi_utils.py` matches live Kalshi market before enabling live trading for batter_hrr

6. **NBA calibration check** — next check due Apr 13 (already past). Run `/check-calibration` to see current state

---

## Files to Read on Resume

- [[handoff-005]] — this file
- `src/backtesting/mlb/run_mlb_sweep.py` — sweep with batter_hrr fixes applied
- `src/models/mlb/mlb_stat_config.py` — where to add BL config after sweep
- `plans/batter_hrr_model.md` — full plan doc with go/no-go gates
- `src/scrapers/kalshi/kalshi_utils.py` — verify KXMLBHRR ticker before going live
