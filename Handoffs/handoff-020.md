# Handoff 020

> Part of [[Handoffs]]

**Date**: April 21, 2026 at 10:56 PM

## Summary

Deep investigation into MLB paper trader performance. Discovered batter_rbis is a -$13k anchor dragging down an otherwise +$14k portfolio. Implemented line tiebreaker for Kalshi dedup, added 3% bet cap to MLB paper trader, fixed combined-mode backtest crash, and enabled extended market scraping (HRR lines) on Railway.

## What Was Done

### Code Changes
- **`src/paper_trading/kalshi_live_trader.py`** — Added `_LINE_TIEBREAK_THRESHOLD = 0.03` and 3-branch sportsbook-proximity tiebreaker to dedup logic (replaces simple highest-edge pick)
- **`src/paper_trading/kalshi_paper_trader.py`** — Same tiebreaker logic applied to paper trader dedup
- **`src/paper_trading/mlb_paper_trader.py`** — Added `DEFAULT_MAX_BET_PCT = 0.03` (3% cap per bet, configurable via `MLB_PAPER_TRADING_MAX_BET_PCT` env var). Changed `max_bet_pct` field default from `None` to 3%
- **`src/backtesting/mlb/run_mlb_sweep.py`** — Fixed combined-mode crash: (1) line 933 — `BlackLittermanBlender` constructor now skips when `bl_cfg is None`, (2) line 1345 — logging handles None BL gracefully for stats like batter_rbis
- **`src/orchestration/scheduler.py`** — Added `--extended` flag to both `run_mlb_lines_props_only()` and `run_mlb_lines_full()` so `batter_hits_runs_rbis` (HRR) prop lines are scraped for 2026

### Investigation Findings
- **MLB paper trading P&L by stat** (all-time resolved bets):
  - batter_hits: 130 bets, 63.1% win, +$7,622
  - batter_total_bases: 1,035 bets, 53.5% win, +$3,650 (HISTORICAL — no longer placed)
  - batter_runs_scored: 82 bets, 63.4% win, +$1,743 (HISTORICAL)
  - pitcher_strikeouts: 77 bets, 51.9% win, +$1,149
  - batter_rbis: 233 bets, 58.4% win, **-$13,176**
  - Non-RBI total: **+$14,321**
- **batter_rbis backtest on Apr 2026 data**: Only 48/208 configs profitable (23%). Deployed config (tau=None, edge=0.12) lost -4.8% ROI. Best viable config (tau=0.9, z=0.75, edge=0.12) only 24 bets. Model is fundamentally broken on 2026 April data.
- **batter_total_bases / batter_runs_scored**: Historical bets from earlier deployment. Current `mlb_paper_trader.py` has explicit stat filter blocking new bets. Model files still in production dir (wasting inference compute).
- **batter_hrr backtest zeros**: NOT because sportsbook lines don't exist (2.6M rows from 17 bookmakers, 2023-2025). The data stops at Sept 2025 because the scheduler wasn't passing `--extended` flag. Fixed now.
- **HRR backfill needed**: April 1-21 2026 has zero HRR lines. Backfill command provided.

## Decisions Made

1. **Kill batter_rbis from production** — 58% win rate still loses money due to terrible odds on 0.5-line unders. No config is reliably profitable on 2026 data.
2. **3% bet cap** — Easy win to prevent $800+ stakes on a $10k bankroll. Kelly was uncapped before.
3. **Extended markets on scheduler** — HRR lines now flow going forward. Needed for HRR backtesting + eventual Kalshi HRR trading on sportsbook lines.
4. **Kalshi line tiebreaker** — When two lines for same player+stat have edges within 3%, prefer the one closer to sportsbook consensus. Sportsbook-aligned lines have more reliable calibration.

## Blockers and Open Questions

1. **batter_rbis**: Should be disabled immediately. Either remove from `MLB_STATS` or set edge threshold impossibly high. The model's NegBin probability estimates don't match reality on 0.5 lines.
2. **HRR backfill**: Need to run `for d in 2026-04-{01..21}; do python -m src.scrapers.mlb.mlb_daily_player_props_scraper --date $d --markets batter_hits_runs_rbis; done` to get April data, then run linker, then sweep.
3. **batter_total_bases model files**: Still in `src/models/mlb/artifacts/production/` — model suite loads them every inference run (wasted compute). Should be removed.
4. **Edge refresh job**: `mlb_edge_refresh_job.py` uses `MLB_STATS.get(stat, {})` with permissive 8% default — any stat in `mlb_daily_predictions` but not in `MLB_STATS` gets processed. Potential leak.

## Recommended Next Steps

1. **Disable batter_rbis** — Remove from `MLB_STATS` in `mlb_stat_config.py` and from paper trader stat filter. Deploy to Railway. (Small — 5 min)
2. **Backfill HRR lines for Apr 2026** — Run the backfill command, then linker, then sweep to evaluate HRR on 2026 data. (Medium — 30 min)
3. **Clean up production artifacts** — Remove `batter_total_bases_*` and `batter_runs_scored_*` model files from production dir. (Small — 2 min)
4. **Investigate Matz 40% edge** — The Kalshi edge for Matz 4+ K was 40.9%, brushing the 40% sanity cap. May want to lower `KALSHI_LIVE_MAX_EDGE` or investigate why the model is so confident on tail lines.
5. **Late-season MLB config sweeps** — Current configs are from early-season (Apr-Jun 2025) backtests. Need Jul-Sep sweeps before mid-season transition.

## Files to Read on Resume

- [[handoff-020]] (this file)
- `src/models/mlb/mlb_stat_config.py` — current production stat configs
- `src/paper_trading/mlb_paper_trader.py` — bet cap + stat filter
- `src/orchestration/scheduler.py` — extended flag on props scraper
- `backtest_results/mlb_sweep_20260421_211628/sweep_results.json` — April 2026 RBI sweep (both directions, all configs unprofitable)
