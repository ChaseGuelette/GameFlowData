> Part of [[Handoffs]]

**Date**: April 12, 2026 at 1:17 PM

## Summary

This session implemented pitcher K UNDER-only mode for the MLB paper trader and backtest sweep, then ran comprehensive early-season (Apr–May 2025) and late-season (Jul–Sep 2025) parameter sweeps to find the optimal BL config. The data clearly confirmed UNDER-only is the right call year-round, and the edge threshold was bumped from 0.05 to 0.08 based on backtest evidence.

## What Was Done

- **`src/models/mlb/mlb_stat_config.py`** — Added `"allowed_directions": ["under"]` to `pitcher_strikeouts`; bumped `edge_threshold` from `0.05` to `0.08`
- **`src/paper_trading/mlb_paper_trader.py`** — Added direction filter in `select_bets()` using `allowed_dirs` from `MLB_STATS`; OVER bets silently skipped for any stat with a direction restriction
- **`src/backtesting/mlb/run_mlb_sweep.py`** — Added `--direction {over,under,both}` CLI flag; added `allowed_bets` parameter to both `run_single_config()` and `run_combined_config()`; combined mode auto-reads `allowed_directions` from `MLB_STATS` config
- **`memory/MEMORY.md`** — Added MLB Pitcher K Config section with seasonal transition plan (early vs. late season configs)
- Ran full early-season sweep (510 configs): best = `tau=0.9, z_max=0.25, mw=0.8, edge=0.08` → +22.6% ROI, 125 bets, Sharpe 2.62
- Ran full late-season sweep (510 configs): best stable config = `tau=0.9, z_max=0.5, mw=0.8, edge=0.08` → +16.6% ROI, 86 bets, Sharpe 2.1

## Decisions Made

- **UNDER-only is permanent** — OVER bled -15.5% ROI in live paper trading (25 bets), UNDER crushed at +48.4%. Not a fluke; backtest confirms it across both early and late season. The model structurally over-predicts K totals.
- **Use current-period optimal config, not year-round compromise** — z_max=0.25 dominates early season (market lines are maximally wrong on K in April/May). Rather than averaging to a less-optimal year-round config, use the best config now and update in late May/June.
- **edge=0.08 over edge=0.12** — Higher edge levels show better ROI% but thin sample (55–68 bets/2 months = 1/day, too variance-prone). 125 bets at +22.6% is better supported and generates similar absolute profit.
- **Current STAT_BL_CONFIGS unchanged** — `tau=0.9, z_max=0.25, mw=0.80` was already the validated config; the UNDER-only filter + edge bump were the actual fixes.

## Blockers and Open Questions

- **Seasonal config transition**: Need to swap `z_max=0.25 → 0.5` around late May / early June when early-season K bias fades. Set a calendar reminder or this will be missed.
- **No live validation yet**: The paper trader will start generating UNDER-only bets from today. First real check should be ~2 weeks of paper trading to confirm the filter is working as expected in production.
- **REB UNDER still a concern**: From the NBA Apr 10 check, REB UNDER had -15.1% ROI (28 bets). That's an NBA issue, not MLB, but it's unresolved.

## Recommended Next Steps

1. **Monitor MLB paper trader** — confirm pitcher K bets are UNDER-only and edge ≥ 0.08 in Discord alerts (~2-day check)
2. **Set reminder for late May** — swap pitcher K config to `tau=0.9, z_max=0.5, mw=0.8, edge=0.08` in `STAT_BL_CONFIGS` + `edge_threshold`
3. **NBA REB UNDER investigation** — -15.1% ROI over 28 bets is getting material; worth a targeted backtest
4. **NBA calibration check due Apr 13** — model is 18 days old, 3-week trigger hits tomorrow
5. **Apply Supabase migration 023** (from Session 27) if not yet done — MLB Stats Vault views + RLS

## Files to Read on Resume

- `src/models/mlb/mlb_stat_config.py` — source of truth for all MLB stat configs including direction restrictions
- `src/paper_trading/mlb_paper_trader.py` — direction filter implementation (around line 268)
- `src/backtesting/mlb/run_mlb_sweep.py` — `--direction` flag + `allowed_bets` wiring
- `memory/MEMORY.md` — MLB Pitcher K Config section for seasonal transition plan
