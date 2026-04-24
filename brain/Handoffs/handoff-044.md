# Handoff 044 — Kalshi Fill Polling, Resolution Date Fix, Discord Alerts & Daily Summary

> Part of [[Handoffs]]

**Date**: April 24, 2026 at 3:31 PM

---

## Summary

Implemented four enhancements to the Kalshi live trading system: (1) fixed a date bug in `reconcile_fills()` that permanently missed fills from prior game dates, (2) built a new 5-minute fill polling job so fills are caught throughout the day rather than only at 9:15 AM, (3) wired Discord notifications for trade placed/filled/resolved events, and (4) added a live trader daily performance summary to the 10 AM summary job. GLM's unintended scheduler frequency changes (edge refresh 5 min → 15 min, MLB edge refresh restructure) were identified and reverted.

---

## What Was Done

### Core Fixes
- **`reconcile_fills()` date bug fixed** — `src/paper_trading/kalshi_live_trader.py`: `target_date` now optional (default `None`). When `None`, queries `WHERE status = 'pending'` (all pending orders regardless of game_date). Ivan Herrera's Apr 22 bet and any other cross-date pending orders will now be caught.
- **`kalshi_refresh_job.py` updated** — resolve-only block now calls `resolver.reconcile_fills()` with no date argument.

### New File
- **`src/orchestration/kalshi_pending_fills_job.py`** — Created. Polls Kalshi API every 5 minutes (9 AM–11 PM ET). Exits early with zero API calls if no pending orders exist. Sends "filled" Discord embed to `DISCORD_CHANNEL_KALSHI` for newly filled orders.

### Discord Alerts
- **`src/discord_bot/alerts.py`** — Added `_build_kalshi_order_filled_embed()`, `_build_kalshi_live_daily_summary_embed()`, and "filled" + "daily_summary" branches in `send_kalshi_trade_alert_sync()`.
- **`src/orchestration/kalshi_execute_approved_job.py`** — Sends "placed" Discord embed to `DISCORD_CHANNEL_KALSHI` (fallback `DISCORD_CHANNEL_PREDICTIONS`) for each successfully executed order. Added `fee_adjusted_edge` to the trade queue SELECT.
- Discord "resolved" (won/lost) alert was already implemented in `resolve_settled()` — no change needed.

### Daily Summary
- **`src/orchestration/kalshi_daily_summary_job.py`** — Added `_send_live_trader_summary()`: queries `kalshi_live_orders` for yesterday's W/L/PnL, all-time total PnL, calculates win rate, sends "daily_summary" embed to `DISCORD_CHANNEL_PERFORMANCE`. Skips if 0 resolved bets yesterday.

### Scheduler
- **`src/orchestration/scheduler.py`** — Added `"kalshi_pending_fills_job.py"` to `JOB_NAMES`, added `run_kalshi_pending_fills()` function, added CronTrigger every 5 min 9AM–11PM ET.

### Revert (Unintended Changes)
- GLM also changed edge refresh schedule (every 5 min → 15 min) and MLB edge refresh (2:30 PM + 4:30 PM → rolling every 15 min). Both reverted manually — original schedule restored.

### Bonus (from other terminals, preserved by GLM)
- `src/db/client.py` — numpy int64 → psycopg2 adapter (prevents type errors on numpy integers in DB writes)
- `dashboard/src/components/bot-tracker/BotOrdersTable.tsx` — BetAnalysisModal integration, fixed `getKalshiUrl()` to use ticker series prefix (all lowercase)
- `src/paper_trading/kalshi_live_trader.py` — Orderbook sweep check before order placement (rejects if price moved >10¢ or edge drops below 50% of original), star-hitter filter in `select_trades()`

---

## Decisions Made

- **`reconcile_fills()` with no date = safe default**: Querying all pending regardless of date adds a tiny DB overhead but permanently fixes the miss. Pending orders are rare enough that this is not a concern.
- **5-minute poll interval**: Matches the `kalshi_execute_approved` job interval. Fast enough to catch fills within one cycle, slow enough to avoid excessive API calls (exits early if no pending).
- **GLM scheduler changes reverted**: The plan didn't authorize changing edge refresh or MLB edge refresh schedules. Those are production-critical frequencies that should only change intentionally.

---

## Blockers and Open Questions

- **Ivan Herrera's Apr 22 bet**: Should now be reconciled on next `--resolve-only` run or next 5-min poll. Verify status after Railway deploy.
- **`reconcile_fills()` return dict**: GLM confirmed this already returns `{"reconciled": N}` — the pending fills job depends on this. If it ever changes, the job's logging will silently show 0.
- **batter_rbis still active in paper trader**: MEMORY says it's broken (-$13k, 0 profitable configs). Still needs to be disabled in the paper trader stat filter.
- **Late-season MLB configs**: TODO sweep Jul-Sep backtests before mid-season switch.
- **Lineup position pipeline**: 90% done per MEMORY — would obsolete the star-hitter filter once complete.

---

## Recommended Next Steps

1. **Deploy to Railway** — push changes, confirm `kalshi_pending_fills` job appears in scheduler logs every 5 minutes
2. **Verify Ivan Herrera's bet** — after deploy, check `kalshi_live_orders WHERE player_name ILIKE 'ivan%' AND status = 'pending'` to confirm it gets reconciled
3. **Disable batter_rbis in paper trader** — update stat filter in `kalshi_paper_trader.py` or `mlb_paper_trader.py` to exclude it
4. **Test Discord alerts** — approve a test trade from the queue dashboard, confirm "trade placed" appears in Kalshi Discord channel
5. **Run `kalshi_daily_summary_job.py --dry-run`** to confirm live trader summary logic works before it fires at 10 AM

---

## Files to Read on Resume

- [[handoff-043]] — previous session (UTC/ET timezone systemic fix, orderbook parallelization)
- [[handoff-041]] — sportsbook line alignment + star-hitter filter context
- `src/orchestration/kalshi_pending_fills_job.py` — new file, verify correctness
- `src/orchestration/scheduler.py` — confirm kalshi_pending_fills entry is present and frequencies are correct
