# Handoff 021 — Bot Tracker Page + Admin Access Control

> Part of [[Handoffs]]

**Date**: April 03, 2026 at 11:48 AM

## Summary

Built the admin-only `/bot-tracker` dashboard page showing all Kalshi bot trading activity (live + paper trades, P&L, circuit breaker status). Also implemented full admin access control infrastructure: `admin_users` table, `is_admin()` SECURITY DEFINER function, RLS policies on all 5 Kalshi trading tables, middleware route gating, and conditional navbar link.

## What Was Done

### Database (2 migrations applied via Supabase)
- Created `admin_users` table with RLS (no authenticated SELECT — only accessible via SECURITY DEFINER)
- Created `is_admin()` function mirroring `is_subscribed()` pattern
- Inserted owner as admin (`3e84de04-...`)
- Replaced public-read RLS policies on `kalshi_paper_bets` and `kalshi_paper_trading_daily_log` with admin-only SELECT
- Enabled RLS + added admin-only SELECT policies on `kalshi_live_orders`, `kalshi_live_trading_daily_log`, `kalshi_live_trading_config`
- Created `get_kalshi_bot_summary()` RPC (aggregates config + live stats + paper stats, admin check, 15s timeout)

### Frontend (8 files created, 2 modified)
- `dashboard/src/types/bot-tracker.ts` — Types for all Kalshi entities + stat labels
- `dashboard/src/lib/hooks/useAdmin.ts` — Admin check hook (cached 30min)
- `dashboard/src/lib/hooks/useBotTracker.ts` — Summary, orders, daily logs hooks with date range + 1min auto-refresh
- `dashboard/src/components/bot-tracker/CircuitBreakerCard.tsx` — Green/red status card
- `dashboard/src/components/bot-tracker/BotSummaryCards.tsx` — KPI row (P&L, win rate, trades, balance)
- `dashboard/src/components/bot-tracker/BotOrdersTable.tsx` — Sortable/filterable orders table
- `dashboard/src/components/bot-tracker/DailyPnlTable.tsx` — Daily log with cumulative P&L + ROI
- `dashboard/src/app/(protected)/bot-tracker/page.tsx` — Main page with Live/Paper tabs + date range presets
- `dashboard/src/lib/supabase/middleware.ts` — Added `ADMIN_ROUTES` array + admin gate after auth
- `dashboard/src/components/layout/Navbar.tsx` — Conditional "Bot" link for admin users (desktop + mobile)

### Build
- Dashboard builds clean with no errors

## Decisions Made
- **`admin_users` table** over hardcoded emails — clean, extensible, follows `is_subscribed()` pattern
- **Separate `/bot-tracker` page** rather than tab on `/prediction-markets` — different audiences (market opportunities vs bot execution)
- **Replaced public-read policies** on paper tables with admin-only — previously anyone could read paper bets
- **P&L values stored in cents** (Kalshi standard) — `formatDollars()` and `formatCents()` helpers divide by 100 for display
- **1-minute auto-refresh** on all bot tracker data via React Query `refetchInterval`
- **30-minute cache** for admin check to minimize RPC calls

## Blockers and Open Questions
- None — implementation complete and building clean
- Python backend unaffected (uses `postgres` role which bypasses RLS)

## Recommended Next Steps
1. **Deploy to Vercel** — push changes to trigger deployment, verify admin gate works in production
2. **Test end-to-end**: admin user sees "Bot" link + page data; non-admin gets redirected, no data via RLS
3. **Phase 3 (Stripe)** — next logical priority for monetization
4. **MLB model sweeps** — total_bases and runs_scored backtest sweeps still pending (Step 1.3/1.6)

## Files to Read on Resume
- [[Bot-Tracker]] — Product spec for the new page
- [[Auth-And-RLS]] — Updated with admin infrastructure
- [[Execution-Plan]] — Step 7.11 completed
- [[Handoffs]] — This handoff

#handoff #admin #bot-tracker #kalshi #session-20
