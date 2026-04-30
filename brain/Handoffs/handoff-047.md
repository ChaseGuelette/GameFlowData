> Part of [[Handoffs]]

**Date**: April 25, 2026 at 10:00 AM

## Summary

Session focused on three Kalshi live trader improvements: implementing contract resizing after orderbook sweep acceptance (pre-approved plan), fixing two null-data columns on the bot tracker dashboard (fill price and game start time), and correcting an F821 linting error introduced by module-level constants referencing helper functions before they were defined. A deeper architectural discussion revealed that `execute_approved_trades()` does NOT recalculate contract counts after human approval, meaning rejected bets leave cap headroom that approved bets can't absorb — identified as a real bug, not yet implemented.

## What Was Done

- **`src/paper_trading/kalshi_live_trader.py`** — 4 distinct changes:
  1. **Sweep resize** (execute_trades ~line 810): After sweep check accepts a new price, recalculates Kelly contracts using fresh price + current balance. Updates `contracts`, `expected_cost`, `expected_fee` in trade dict. Clips to balance, skips if 0 contracts. Logs old → new count.
  2. **fill_price fallback** (execute_trades ~line 889): When Kalshi API returns null for `yes_price`/`avg_price` on a filled order, falls back to `trade["yes_price"]` (snapshot price). Prevents null fill_price for "filled" status orders.
  3. **`_lookup_game_start_times` rewrite** (~line 718): Replaced broken query (referenced nonexistent `nba_game_schedule` table, had cartesian JOIN) with query against `kalshi_markets.close_time`. Kalshi sports markets close at game start, so `close_time` IS the game start time. Uses `DISTINCT ON (ticker)` with most-recent snapshot.
  4. **F821 lint fix** (~line 45): Moved `KALSHI_SWEEP_MAX_CENTS` and `KALSHI_SWEEP_EDGE_RETENTION` constants to after `_env_int`/`_env_float` helper definitions. They were defined before the helpers they call.

- **DB backfill** (Supabase direct): Updated all 47 `kalshi_live_orders` rows to set `game_start_time` from `kalshi_markets.close_time` joined by ticker. All 47 now have game_start_time populated.

## Decisions Made

- **`kalshi_markets.close_time` as game start time**: Kalshi closes sports prop markets exactly at game start. This is the correct source — no need to join to separate schedule tables. Simpler and sport-agnostic.
- **fill_price fallback = snapshot price**: For immediately-filled orders where the API doesn't return a fill price, snapshot price is the best available proxy. `reconcile_fills()` will overwrite with the actual price when the game resolves.
- **Cap-aware resizing NOT implemented yet**: Confirmed the bug — `select_trades()` clips lower-priority bets' contracts to fit cap, and rejected bets' cap headroom is never redistributed to approved bets in `execute_approved_trades()`. Fix described but deferred to next session.

## Blockers and Open Questions

- **Cap-aware resizing in `execute_approved_trades()`**: When some queued bets are rejected by the human reviewer, the remaining approved bets may have clipped contract counts (because they were sized assuming other bets would consume cap). Fix: before executing approved trades, re-run the Kelly sizing pass with current balance + remaining daily cap, sorted by edge desc. Not implemented.
- **No `sportsbook_consensus_line` on `kalshi_trade_queue`**: Queue table does not have this column (mentioned in memory but worth verifying the trade dict that flows into `execute_approved_trades` has it).

## Recommended Next Steps

1. **Implement cap-aware resizing in `execute_approved_trades()`**: In the function, before calling `execute_trades()`, query today's existing exposure from DB, compute `effective_cap`, sort approved trades by edge desc, re-run Kelly sizing with remaining cap. This mirrors the `select_trades()` sizing logic applied to the approved subset.
2. **Deploy to Railway**: All 4 Python changes need to be deployed. Current changes are local only.
3. **Verify bot tracker dashboard**: After deployment, confirm fill price and game start time show correctly for new live orders.
4. **Late season MLB backtest sweep**: Still outstanding — need Jul–Sep backtests to validate configs before mid-season.

## Files to Read on Resume

- [[Kalshi-Live-Trading-Startup]] — full live trading architecture and flow
- [[handoff-046]] — previous session for context
- `src/paper_trading/kalshi_live_trader.py` — all changes this session are here
