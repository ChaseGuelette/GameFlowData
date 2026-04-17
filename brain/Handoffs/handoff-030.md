# Handoff 030

> Part of [[Handoffs]]

**Date**: April 16, 2026 at 02:05 PM

## Summary

Focused on performance engineering and playoff model deployment. Applied a 55x backtesting speedup (precompute + vectorized BL) to the MLB sweep, analyzed the 2025 NBA playoff sweep results (confirming 2025 is fully out-of-sample), and deployed the playoff model to Railway with the winning config. The system is now live and ready for the NBA playoffs starting Apr 19.

## What Was Done

- **Backtesting speedup documented**: Created `memory/performance_optimizations.md` tracking the 55x speedup (NBA sweep: 11.2s → 0.2s/config). Root causes: `iterrows()` in hot loops, per-config data rebuilds, per-resolve lookup dict rebuilds.
- **MLB sweep fast path added** (`src/backtesting/mlb/run_mlb_sweep.py`): Applied same precompute + vectorized BL pattern as NBA. Added `precompute_mlb_base_probs()`, `run_single_config_fast_mlb()`, Phase 0b in `main()`. Vectorized best-line selection with `groupby().idxmin()` replacing `_select_sharpest_line` iterrows.
- **In-sample contamination clarified**: Model trained on 42019-42023, calibrated on 42024 → **2025 is fully out-of-sample**, 2024 has mild calibration-season overlap. Prior session had it backwards — corrected.
- **Playoff model deployed** (`src/models/daily_runner.py`): Added `NBA_PLAYOFF_MODE` env var detection. Winning config from 2025 out-of-sample backtest: `tau=0.9, z_max=0.25, mw=0.8, edge=0.12` (63.6% hit, +19.3% ROI, 272 bets, Sharpe 2.33). Added `DEFAULT_BL_MAX_WEIGHT` constant (was missing from BLConfig call).
- **Railway env var set**: `NBA_PLAYOFF_MODE=true` active — switches both model dir (`production_playoffs/`) and BL config simultaneously.
- **Brain NBA-Model.md updated**: Added Playoffs section documenting run ID, training seasons, out-of-sample validation results, BL config, and deployment status.

## Decisions Made

- **Winning playoff config**: `tau=0.9, z_max=0.25, mw=0.8, edge=0.12` — chosen because it was the best config on the fully out-of-sample 2025 backtest. The 2024 sweep showed 500+ bets/season at 53% hit but that's contaminated (calibration season). The 2025 sweep's 272 bets at 63.6% is the real signal.
- **Single env var for dual switch**: `NBA_PLAYOFF_MODE=true` flips both model dir AND BL config atomically. Simpler than separate vars.
- **CDN discovery unchanged**: Playoff game IDs (`004xxxx`) already handled in `daily_runner.py` — no changes needed to game discovery logic.

## Blockers and Open Questions

- **NBA calibration check overdue**: Was due Apr 13 per memory. Last check was Apr 10 (+10.9% ROI, MONITOR). Need to run `/check-calibration` to assess model health before playoffs start Apr 19.
- **batter_hrr sweep** (Step 1.9): Still pending. Commands are ready; just need to run the sweep locally.
- **Track Record page — Excel import**: Noted in handoff-029 but not worked on this session.

## Recommended Next Steps

1. **Run `/check-calibration`** — calibration check is overdue (due Apr 13, now Apr 16). With playoffs starting Apr 19 this is urgent. Confirm model health before playoff traffic begins.
2. **Monitor playoff inference** — Apr 19 is first playoff game. Check Railway logs to confirm `NBA_PLAYOFF_MODE=true` is active and the correct model dir is loading.
3. **batter_hrr sweep** — Run the MLB batter_hrr BL parameter sweep locally. Commands are ready per handoff-029.
4. **Kalshi go-live** — Was in prep stage (handoff-037 in Session 37). Fund account and flip `KALSHI_LIVE_TRADING_ENABLED=true` when ready.

## Files to Read on Resume

- [[NBA-Model]] — updated this session with playoff model details
- `src/models/daily_runner.py` — playoff mode switch (lines 18-30 approx for BL config)
- `memory/performance_optimizations.md` — full sweep speedup documentation
- [[handoff-029]] — prior session context (Track Record page, History edits)
