> Part of [[Handoffs]]

**Date**: April 25, 2026 at 5:00 PM

## Summary

Short session focused on Kalshi live trader hardening and pipeline timing. Added cap-aware exposure enforcement to `execute_trades()` so sweep-resized contracts can't exceed the daily exposure cap, shifted MLB and NBA early inference windows ~1 hour earlier, and fixed a production bug where `reprice_stale_orders` was failing every 5 minutes due to a nonexistent column in its SELECT query.

## What Was Done

- **Cap-aware resize in `execute_trades()`** (`src/paper_trading/kalshi_live_trader.py`)
  - Queries daily exposure cap + today's existing exposure at start of `execute_trades()`
  - Tracks `running_exposure` and clamps contracts in both swept and non-swept paths
  - Increments `running_exposure += total_cost` after each filled order
  - Prevents sweep-resize from pushing daily spend past `bankroll × KALSHI_DAILY_EXPOSURE_PCT`

- **Scheduler early window shift** (`src/orchestration/scheduler.py`)
  - MLB early window: stats→9:00, retry→9:20, weather→9:25, props→9:30, lineups→9:35, inference→9:50
  - NBA early window: props→10:00, inference→10:15 (was 11:00/11:15 AM)
  - Noon and 4 PM full windows unchanged

- **Fixed `reprice_stale_orders` production failure** (`src/paper_trading/kalshi_live_trader.py`)
  - Removed `expected_fee` from SELECT (column doesn't exist in `kalshi_live_orders`, actual col is `fee_paid`)
  - Job was failing every 5 minutes in production; now resolved

## Decisions Made

- **Accept incomplete lineups for early MLB inference**: Moving lineup scrape to 9:35 AM means fewer confirmed lineups vs 10:50 AM, but the model defaults `lineup_position=0` already so the quality tradeoff is acceptable. Gained ~70 min of earlier market exposure.
- **Direct edit vs GLM for `reprice_stale_orders` fix**: Single-line change, directly edited rather than GLM handoff (below GLM threshold of 20 lines / 2+ files).
- **Cap clamp breaks on exhaustion**: When cap is fully exhausted, the non-swept path breaks the trade loop entirely (skips remaining trades), while the swept path just skips the individual trade and continues — intentional asymmetry since order within a batch matters less for sweeps.

## Blockers and Open Questions

- **`reprice_stale_orders` was crashing with resting orders** (`fc09e92c`, `9fa172c1`, `ddc6afd8`) — these 3 orders may be stuck as `pending` in `kalshi_live_orders`. Worth checking after deploy whether they get repriced or remain stale.
- **9:00 AM job contention**: MLB daily stats now fires alongside NBA stats, MLB roster scraper, and Polymarket scrape — 4+ subprocesses at once. Monitor Railway logs the first morning after deploy for timeout/resource pressure.
- **Late-season sweep backtest TODO**: MLB BL configs were tuned on early-season data; need Jul-Sep backtests before mid-season switch.

## Recommended Next Steps

1. **Deploy and monitor**: Push to Railway, watch 9:00–10:15 AM window the next morning to confirm early inference fires cleanly and the cap clamp logs appear on sweeps
2. **Check the 3 stale orders**: Query `SELECT * FROM kalshi_live_orders WHERE kalshi_order_id IN ('fc09e92c-29de-4a1d-b842-4271b93045c0', '9fa172c1-ff9b-488e-8314-fc346283d0f0', 'ddc6afd8-e744-4ea6-ae34-2d61db295567')` — determine if they resolved or need manual cleanup
3. **Lineup position pipeline** (90% done from previous session) — completing this obsoletes the star-hitter filter and should improve batter_hits model quality at early inference times
4. **Late-season MLB backtests**: Run Jul-Sep sweeps for `batter_hits` and `pitcher_strikeouts` to validate configs hold through end of season

## Files to Read on Resume

- [[Kalshi-Live-Trading-Startup]] — live trader architecture and current state
- [[handoff-049]] — previous session for full Kalshi context
- [[Scheduling]] — updated schedule times
- [[Operations]] — daily runbook and invariants
