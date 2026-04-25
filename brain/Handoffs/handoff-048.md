> Part of [[Handoffs]]

**Date**: April 25, 2026 at 03:14 PM

## Summary
Implemented the full Stale Fill Cancellation Queue — a human-approval system that detects pending Kalshi orders whose games have started, queues them for review, and executes approved cancellations via the Kalshi API. Also analyzed the live trader's bet sizing and identified the Kelly fraction as the primary lever for increasing bet sizes.

## What Was Done

### Stale Fill Cancellation Queue (full implementation)
- **DB**: Applied migration `add_kalshi_cancel_queue` — new table `kalshi_cancel_queue` with status flow: pending_review → approved/rejected → cancelled/failed
- **Created** `src/orchestration/kalshi_stale_fills_job.py` — detects pending orders with game_start_time <= now(), enqueues to cancel queue, fires Discord alert (circuit_breaker type). Fixed column bug: `expected_cost` doesn't exist on kalshi_live_orders, correct column is `total_cost`.
- **Created** `src/orchestration/kalshi_execute_cancellations_job.py` — polls approved records, calls KalshiClient.cancel_order(). Deliberately NOT gated on KALSHI_LIVE_TRADING_ENABLED — cancellations always run regardless of trading state.
- **Modified** `src/orchestration/scheduler.py` — added two run_* functions + job registrations: stale fills every 5 min, execute cancellations every 2 min, both 9AM-11PM ET
- **Created** `dashboard/src/app/api/kalshi/cancel-queue/route.ts` (GET — pending_review orders)
- **Created** `dashboard/src/app/api/kalshi/cancel-approve/route.ts` (POST — approve/reject/approve_all)
- **Created** `dashboard/src/components/bot-tracker/StaleOrdersPanel.tsx` — UI panel with "Cancel Order"/"Keep" buttons, polls every 30s, hidden when no orders
- **Modified** `dashboard/src/app/(protected)/bot-tracker/page.tsx` — StaleOrdersPanel added adjacent to TradeApprovalPanel
- TypeScript type-check: clean. Python job e2e tested locally.

### GLM Unspecified Change (pending decision)
GLM modified `src/paper_trading/kalshi_live_trader.py` — added contract resizing in the sweep buffer. When price moves during sweep, now recalculates Kelly contracts at the new price (not just updates price+edge). User has not decided to keep or revert.

### Kalshi Bet Sizing Analysis
- 20 bets at ~$4 each is mathematically correct: 1/8 Kelly (0.125) on 15-18% edges with ~$200 bankroll
- Three levers: (1) `KALSHI_LIVE_KELLY_FRACTION` 0.125→0.25 doubles all bets, (2) raise `KALSHI_LIVE_MIN_EDGE` for fewer/larger bets, (3) deposit more
- No change made yet

## Decisions Made

- **Cancellations always run regardless of KALSHI_LIVE_TRADING_ENABLED** — the trading gate is for placing new bets, not executing human-approved cancellations. Decoupled intentionally.
- **kalshi_cancel_queue uses `expected_cost` column** (copy of the value at detection time from `total_cost` in kalshi_live_orders) — the naming difference is intentional: the cancel queue stores a snapshot.
- **StaleOrdersPanel hidden when no stale orders** (returns null) — avoids cluttering the UI when everything is fine.

## Blockers and Open Questions

1. **GLM sweep resize change** — `src/paper_trading/kalshi_live_trader.py` was modified without being in the spec. Decision pending: keep (better contract sizing on price-moved sweeps) or revert.
2. **Kelly fraction** — should `KALSHI_LIVE_KELLY_FRACTION` be bumped from 0.125 to 0.25? User acknowledged the analysis but no decision made.
3. **Railway deployment** — not yet pushed. Two new scheduler jobs need to be verified in Railway logs after deploy.
4. **Dashboard API routes** — not yet tested against live dashboard (only TypeScript type-check done). Needs manual verification after deploy.

## Recommended Next Steps

1. **Decide on kalshi_live_trader.py sweep change** — run `git diff src/paper_trading/kalshi_live_trader.py` to review, then keep or `git checkout src/paper_trading/kalshi_live_trader.py`
2. **Decide on Kelly fraction** — if bumping, set `KALSHI_LIVE_KELLY_FRACTION=0.25` on Railway
3. **Deploy to Railway** — push main, check scheduler logs for `kalshi_stale_fills` and `kalshi_execute_cancellations` job registrations
4. **Verify dashboard** — test cancel-queue GET and cancel-approve POST on live Vercel dashboard
5. **Monitor first live stale order** — next time a bet goes unfilled past game start, verify Discord alert fires and StaleOrdersPanel shows it

## Files to Read on Resume
- [[Kalshi-Live-Trading-Startup]] — operations runbook for the Kalshi trading system
- [[Execution-Plan]] — current build priorities
- [[handoff-047]] — previous session for broader context
