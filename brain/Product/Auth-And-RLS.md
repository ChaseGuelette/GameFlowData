# Auth & RLS

> Part of [[Product]]

## Authentication
- Supabase email/password auth via `@supabase/ssr`
- Middleware redirects unauthenticated users from protected routes
- `/api/slate` POST handler checks Supabase auth (returns 401 if unauthenticated)

## Row Level Security

### Subscription-Gated Tables
These tables require active subscription via `is_subscribed()` SECURITY DEFINER function:
- `daily_predictions`
- `paper_bets`
- `paper_trading_daily_log`
- `daily_prediction_samples`

### Public Read Tables
- `player_game_stats`
- `raw_player_props_combined`

### Admin-Gated Tables (Session 20)
These tables require admin role via `is_admin()` SECURITY DEFINER function:
- `kalshi_live_orders`
- `kalshi_live_trading_daily_log`
- `kalshi_live_trading_config`
- `kalshi_paper_bets`
- `kalshi_paper_trading_daily_log`

### User-Scoped Tables
- `user_subscriptions` — users can view own subscription only
- `user_bets` — users can CRUD own bets only

### `is_subscribed(uuid)` Function
Checks `status IN ('active', 'trialing')` AND `current_period_end > now()`. Returns boolean.

### `is_admin()` Function (Session 20)
Checks `admin_users` table for `auth.uid()`. SECURITY DEFINER. Used by middleware route gating + RLS policies on Kalshi trading tables. `admin_users` table has RLS with no authenticated SELECT policy (only accessible via the function).

### Role Architecture
- **`postgres` role**: Python backend — bypasses ALL RLS
- **`authenticated` role**: Dashboard users — governed by RLS, 8s `statement_timeout`
- For slow RPCs: `ALTER FUNCTION SET statement_timeout = '30s'`

### RLS Lockdown (Session 79)
Enabled RLS on all 20 previously unprotected tables via migration 018. 3 authenticated SELECT policies added for dashboard-facing tables. All others default-deny.

## Free Beta Model
Currently no paywall. Public `/picks` shows 3 real picks. Stripe infra preserved dormant.

#auth #rls #security
