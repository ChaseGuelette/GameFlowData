# Bot Tracker

> Part of [[Product]]

Admin-only dashboard page showing all Kalshi bot trading activity. Built in Session 20.

## Route
`/bot-tracker` — protected + admin-gated via middleware `is_admin()` RPC check.

## Features
- **Circuit Breaker Card**: Live status (ACTIVE/HALTED), halt reason, loss streak count
- **Summary KPIs**: Total P&L, Win Rate, Trades (with pending count), Balance
- **Tab Toggle**: Switch between Live Orders and Paper Bets
- **Date Range Presets**: Today / 7d / 30d / All
- **Orders Table**: Sortable (player, stat, edge, P&L, status), filterable by stat type + status. **Session 27**: Added "Value" column (live: `total_cost`, paper: `contracts × entryPrice / 100`) and Kalshi market link icon (external link to kalshi.com game page, URL built from ticker). **Session 34**: Added "Placed" (bet placement timestamp) and "Game Start" (market close_time) columns; colSpan updated 13→15; `formatTime()` helper added to `BotOrdersTable.tsx`. **Session 49**: Fixed Kalshi market URLs — `getKalshiUrl()` now derives series from ticker prefix (`ticker.split('-')[0].toLowerCase()`) and uses full ticker as the market path (was hardcoding `kxnbagame`/`kxmlbgame` for all bets). `TradeApprovalPanel.tsx` link now lowercase. Added BetAnalysisModal — bar-chart icon on each row opens a modal showing bet metadata (edge, model prob, Kalshi implied, status/P&L) + player's last 5 game history with chart and stats table.
- **Daily P&L Log**: Date, trades, won, lost, daily P&L, cumulative P&L, ROI

## Data Sources
- `kalshi_live_orders` — live trading orders
- `kalshi_paper_bets` — paper trading bets
- `kalshi_live_trading_daily_log` / `kalshi_paper_trading_daily_log` — daily aggregates
- `kalshi_live_trading_config` — circuit breaker state
- `get_kalshi_bot_summary()` RPC — aggregated summary in one round-trip

## Access Control
- `admin_users` table + `is_admin()` SECURITY DEFINER function
- Middleware redirects non-admin users to `/dashboard`
- Navbar "Bot" link hidden for non-admin users
- All 5 Kalshi trading tables have admin-only RLS SELECT policies

## Auto-Refresh
All data hooks refresh every 60 seconds via React Query `refetchInterval`.

## Key Files
- `dashboard/src/app/(protected)/bot-tracker/page.tsx`
- `dashboard/src/components/bot-tracker/` (5 components: BotOrdersTable, BetAnalysisModal, TradeApprovalPanel, BotSummaryCards, CircuitBreakerCard, DailyPnlTable, PriceBucketTable)
- `dashboard/src/lib/hooks/useAdmin.ts`
- `dashboard/src/lib/hooks/useBotTracker.ts`
- `dashboard/src/types/bot-tracker.ts`

#admin #kalshi #bot-tracker #dashboard
