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

### User-Scoped Tables
- `user_subscriptions` — users can view own subscription only
- `user_bets` — users can CRUD own bets only

### `is_subscribed(uuid)` Function
Checks `status IN ('active', 'trialing')` AND `current_period_end > now()`. Returns boolean.

### Role Architecture
- **`postgres` role**: Python backend — bypasses ALL RLS
- **`authenticated` role**: Dashboard users — governed by RLS, 8s `statement_timeout`
- For slow RPCs: `ALTER FUNCTION SET statement_timeout = '30s'`

### RLS Lockdown (Session 79)
Enabled RLS on all 20 previously unprotected tables via migration 018. 3 authenticated SELECT policies added for dashboard-facing tables. All others default-deny.

## Free Beta Model
Currently no paywall. Public `/picks` shows 3 real picks. Stripe infra preserved dormant.

#auth #rls #security
