> Part of [[Handoffs]]

**Date**: April 25, 2026

## Summary

Shipped Kalshi failed trade visibility and retry: failed orders now surface in the bot-tracker dashboard with one-click retry, Discord fires a red alert embed when any order fails to fill, and the queue API was extended to return both pending and recent failed trades in a single request.

## What Was Done

- **Discord failure alert** (`src/discord_bot/alerts.py`):
  - Added `_build_kalshi_trade_failure_embed()` — red (0xFF0000) embed with player/stat/line/side/contracts/cost/edge/ticker/error fields
  - Added `send_kalshi_trade_failure_alert()` (async) and `send_kalshi_trade_failure_alert_sync()` (sync wrapper with asyncio.run_coroutine_threadsafe pattern)

- **Failure detection in executor** (`src/orchestration/kalshi_execute_approved_job.py`):
  - Added `executed_tickers: set` — populated during the success-alert loop by collecting `r["ticker"]` on each successful fill
  - After success loop: iterates all `approved_trades` (built from `rows` with column indices 0-8) and fires `send_kalshi_trade_failure_alert_sync()` for any ticker not in `executed_tickers`
  - Column index mapping: `id[0], ticker[1], player_name[2], stat_type[3], line[4], side[5], contracts[6], expected_cost[7], fee_adjusted_edge[8]`

- **Queue API extended** (`dashboard/src/app/api/kalshi/queue/route.ts`):
  - Replaced single query with two separate Supabase queries merged in JS (cleaner than raw SQL UNION with PostgREST)
  - Query 1: `status='pending_approval' AND expires_at > now()`
  - Query 2: `status='failed' AND proposed_at > now()-24h`
  - Both returned as `{ trades: [...pending, ...failed] }` with `status` field on every row

- **Retry endpoint** (`dashboard/src/app/api/kalshi/approve/route.ts`):
  - Added `'retry'` to the action union type
  - Validates trade exists with `status='failed'`, updates to `status='approved'` with fresh 30-min `expires_at`
  - Returns `{ success: true, action: 'retry', count, trade_ids }`

- **Hook updated** (`dashboard/src/lib/hooks/useTradeQueue.ts`):
  - `approveAction()` function type extended to include `'retry'`
  - Added `retry` mutation (same invalidation keys as approve/reject)
  - Exported from `useTradeApproval()`

- **TradeApprovalPanel** (`dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx`):
  - Added `useQueryClient` import + `const queryClient = useQueryClient()` instantiation
  - Split `trades` into `pendingTrades` (status=pending_approval) and `failedTrades` (status=failed)
  - All existing approval logic (sports filter, select all, approve/reject buttons) now uses `pendingTrades` only
  - Added **"Failed Orders (last 24h)"** section below pending panel — red border, red background on rows
  - Each failed row shows: player, stat, line, side, cost, edge, ticker; Retry button POSTs `action:'retry'`
  - `isPending` check includes `retry.isPending`; error display includes `retry.error`
  - Return wrapped in `<>` fragment to accommodate both panels side by side

- **Build**: `npm run build` — clean, 0 TypeScript errors, all 33 routes present

## Decisions Made

- **Two Supabase queries vs raw SQL UNION**: PostgREST client doesn't support UNION syntax cleanly. Two separate queries merged in JS is simpler and equally performant for small result sets.
- **Retry re-enters approved pipeline unchanged**: The executor's sweep buffer re-fetches live orderbook price and resizes contracts at execution time. Stale `yes_price`/`contracts` in queue are just starting points — live price overrides. No special re-evaluation logic needed.
- **Fragment wrapper for dual panels**: `<>...</>` wraps pending + failed sections; both can render independently when one is empty.
- **Accidental revert of stale_fills_job.py changes**: Changes from another terminal session to `kalshi_stale_fills_job.py` were accidentally reverted via `git checkout`. Those changes are permanently lost — need to re-check that file against other terminal sessions.

## Blockers and Open Questions

- **stale_fills_job.py revert**: Changes from another Claude terminal were accidentally discarded. Need to review what those changes were (likely `expected_cost` → `total_cost` column rename in the SELECT). Check git log from other terminals or Railway logs for symptoms.
- **Retry UX on persistent failures**: If a trade fails because of price movement > 10¢ or edge collapse, retry will fail again. No user-facing explanation for why retry might fail again — consider adding a tooltip or note in the UI.
- **`queryClient.invalidateQueries` in retry button**: Retry button calls `queryClient.invalidateQueries` in `onSuccess` callback — this is redundant (the hook's `onSuccess` already invalidates). Harmless but could be cleaned up.

## Recommended Next Steps

1. **Test failed trade flow**: Manually set a `kalshi_trade_queue` row to `status='failed'` in Supabase → verify it shows in Failed Orders section on `/bot-tracker`. Click Retry → verify row flips to `approved` and executor picks it up within 2 min.
2. **Verify Discord failure alert**: Mock `create_order` to return `None` locally (or trigger a real failure) → confirm red embed fires with correct fields.
3. **Investigate stale_fills_job.py**: Check git history or Railway logs to understand what changes were lost in the accidental revert.
4. **Continue lineup position pipeline** — still 90% done. Completing it will obsolete the `KALSHI_STAR_HITS_YES_PRICE=72` star-hitter filter.
5. **Monitor NBA trade queue** — NBA trading is enabled, watch that approval panel shows NBA trades correctly and they execute cleanly.

## Files to Read on Resume

- [[Bot-Tracker]] — dashboard approval panel + new failed orders section
- [[Kalshi-Integration-Design]] — full Kalshi architecture and approval queue design
- [[Kalshi-Live-Trading-Startup]] — env vars, circuit breakers, scaling plan
- [[Operations]] — daily runbook
