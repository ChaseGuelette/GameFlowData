# Handoff 016 — Kalshi Live Trading Post-Mortem & 8-Fix Overhaul

> Part of [[Handoffs]]

**Date**: April 20, 2026 at 1:27 PM

## Summary

First day of Kalshi live trading (April 19) was a disaster: 21 NBA bets totaling $233 in 16 seconds using a broken model with 17-46% edge values. This session implemented an 8-fix overhaul addressing resolution decoupling, safety gates, exposure caps, and a human-in-the-loop trade approval flow. Railway env vars deployed. Partial resolution ran but revealed garbage PnL from null fill_price bug.

## What Was Done

### Backend Fixes (Python)
- **Fix 1**: `KalshiLiveTrader(resolve_only=True)` — resolution decoupled from `KALSHI_LIVE_TRADING_ENABLED`. Resolution now runs unconditionally (Step 4.5a) before the live trading gate (Step 4.5b). Added `--resolve-only` CLI flag.
- **Fix 2**: Morning resolution job added to `scheduler.py` at 9:15 AM ET daily. Runs `--resolve-only` for both NBA and MLB.
- **Fix 3**: Per-sport trading gate — `select_trades()` checks `{SPORT}_TRADING_ENABLED` env var. `NBA_TRADING_ENABLED=false` blocks all NBA Kalshi bets.
- **Fix 4**: Edge sanity cap — `KALSHI_LIVE_MAX_EDGE=0.40` rejects edges above 40% as model garbage.
- **Fix 5 backend**: Trade approval flow — `propose_trades()` writes to `kalshi_trade_queue` table instead of executing immediately. `execute_approved_trades()` executes approved queue items. Discord notification on trade proposal.
- **Fix 6**: Shared cross-sport exposure cap — exposure query now checks ALL sports (was per-sport). MLB fires at :00, NBA at :02 for priority.

### Frontend Fixes (Dashboard)
- **Fix 5 frontend**: `TradeApprovalPanel` component with checkboxes, approve/reject buttons, live countdown timers. `useTradeQueue` hook (15s polling). Two API routes: `GET /api/kalshi/queue`, `POST /api/kalshi/approve`.
- **Fix 7**: `game_start_time` column on `kalshi_live_orders`, type updated, `BotOrdersTable` now shows game start time for live orders.

### Database
- Migration applied: `kalshi_trade_queue` table + `game_start_time` column on `kalshi_live_orders`

### Railway Env Vars Set
- `KALSHI_MAX_DAILY_EXPOSURE=200`
- `KALSHI_MIN_DAILY_EXPOSURE=200`
- `KALSHI_LIVE_MAX_EDGE=0.40`
- `NBA_TRADING_ENABLED=false`

### Files Modified
- `src/paper_trading/kalshi_live_trader.py` — resolve_only init, sport gate, max edge, propose_trades(), execute_approved_trades(), game_start_time lookup
- `src/orchestration/kalshi_refresh_job.py` — decoupled resolution, --resolve-only flag, propose flow, Discord approval alert
- `src/orchestration/scheduler.py` — 9:15 AM morning resolution job
- `dashboard/src/types/bot-tracker.ts` — KalshiTradeQueueItem type, game_start_time on KalshiLiveOrder
- `dashboard/src/components/bot-tracker/BotOrdersTable.tsx` — game_start_time display for live orders
- `dashboard/src/app/(protected)/bot-tracker/page.tsx` — TradeApprovalPanel integration

### Files Created
- `dashboard/src/app/api/kalshi/queue/route.ts`
- `dashboard/src/app/api/kalshi/approve/route.ts`
- `dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx`
- `dashboard/src/lib/hooks/useTradeQueue.ts`

## Decisions Made

1. **Trade approval flow over auto-execution** — All live trades now go through a human-in-the-loop approval step. Trades expire after 30 minutes. This is the most important structural change.
2. **Shared exposure cap ($200)** — MLB gets priority (fires at :00), NBA gets remainder (fires at :02). Single $200 cap across both sports, no per-sport independence.
3. **NBA trading disabled** — `NBA_TRADING_ENABLED=false` blocks NBA Kalshi bets until model is fixed/validated.
4. **Resolution always runs** — Even when `KALSHI_LIVE_TRADING_ENABLED=false`, resolution and fill reconciliation run every cycle.

## Blockers and Open Questions

1. **21 April 19 bets have garbage PnL** — 13 resolved but all have `fill_price=null`, causing wrong PnL calculation (losses show as -$451 on $233 wagered, won bets show negative PnL). Need a one-time fix script to fetch actual fill prices from Kalshi API and recalculate.
2. **8 bets still "pending"** — These were never reconciled from `pending` → `filled`. The `reconcile_fills()` may have failed on these (API issue?). They have fill_price values already set from order placement.
3. **Discord `approval_needed` alert type** — Added to `_send_trade_approval_alert()` in refresh job but the corresponding embed builder in `alerts.py` may not exist yet. Need to add it.
4. **Game start time lookup** — `_lookup_game_start_times()` is best-effort and may not match well (schedule table join needs ticker→team extraction). Low priority cosmetic.
5. **Changes not yet committed** — All changes are local only. Need to commit and deploy.

## Recommended Next Steps

1. **Write one-off fix script** for the 21 April 19 bets: fetch fills from Kalshi API, update fill_price, recalculate PnL, move 8 pending → filled → resolved.
2. **Add `approval_needed` embed builder** to `src/discord_bot/alerts.py` so Discord notifications work.
3. **Commit and deploy** all changes to Railway.
4. **Validate approval flow** end-to-end: trigger a trade proposal, see it on dashboard, approve, verify execution.
5. **Decide NBA model path**: retrain, disable permanently, or investigate what caused 17-46% edges.

## Files to Read on Resume

- [[Kalshi-Live-Trading-Startup]] — the operational playbook (needs updating post-incident)
- `src/paper_trading/kalshi_live_trader.py` — all the backend fixes
- `src/orchestration/kalshi_refresh_job.py` — resolution decoupling + approval flow
- `dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx` — approval UI
- `src/discord_bot/alerts.py` — needs `approval_needed` embed builder added
