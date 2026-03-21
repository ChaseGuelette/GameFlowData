# Scaling & Performance Guide

Last updated: 2026-03-21

## Current Architecture

- **Frontend**: Next.js 16 on Vercel (serverless)
- **Database**: Supabase (Postgres + pgbouncer connection pooling)
- **Auth**: Supabase Auth (session cookies, middleware check on every route)
- **Backend**: Python on Railway (APScheduler, postgres role bypasses RLS)
- **External data**: NBA CDN (cached 30s-1hr), RapidAPI injuries

## Current Capacity Estimate

**~500 concurrent users comfortably** with no changes.

The app is lightweight: minimal bundle (~400-500KB gzipped), no WebSocket connections, 8 total npm dependencies. Vercel auto-scales serverless functions and Supabase provides built-in connection pooling.

---

## Known Bottlenecks

### Tier 1: Will slow down at 500+ users

#### No Pagination on Historical Data
- **Where**: Performance page (`paper_bets`, `paper_trading_daily_log`), History page (`paper_bets`, `user_bets`)
- **Problem**: Fetches ALL resolved bets with no limit. As data grows daily, these queries get slower and transfer more data.
- **Fix**: Add cursor-based pagination (by `game_date`). Show last 30 days by default, load more on scroll.
- **Effort**: Medium

#### Stats Page Loads Entire Tables
- **Where**: `stats/page.tsx` — loads `player_stats_latest`, `team_stats_latest`, `defense_by_position_latest` in full
- **Problem**: All filtering (position, team, search) happens client-side after downloading everything.
- **Fix**: Add server-side filtering parameters, or implement virtual scrolling for large result sets.
- **Effort**: Low-Medium

#### N+1 Query Pattern on Performance/History
- **Where**: Both pages fetch `paper_bets`, then separately query `daily_predictions` by IDs to get `is_recommended`
- **Problem**: Two sequential queries instead of a join. The second query can have hundreds of IDs.
- **Fix**: Use a Supabase join (`paper_bets!inner(daily_predictions(is_recommended))`) or create an RPC that returns enriched data.
- **Effort**: Medium

### Tier 2: Will matter at 2,000+ users

#### Middleware Auth Latency
- **Where**: `middleware.ts` calls `supabase.auth.getUser()` on every navigation
- **Problem**: Adds ~150-300ms per page transition (network roundtrip to Supabase Auth)
- **Fix**: Use `supabase.auth.getSession()` for middleware checks (reads cookie locally, no network call). Reserve `getUser()` for server components that need verified identity.
- **Note**: `getSession()` trusts the JWT without server verification — acceptable for route gating since actual data is protected by RLS.
- **Effort**: Low

#### No Client-Side Data Caching
- **Where**: All protected pages re-fetch data from Supabase on every navigation
- **Problem**: Switching between Dashboard → Performance → Dashboard re-fetches everything
- **Fix**: Add React Query (TanStack Query) with `staleTime` of 2-5 minutes for predictions, 10+ minutes for historical data.
- **Effort**: Medium

#### Missing Database Indexes
- **Where**: `raw_player_props_combined` is queried with `.in('bookmaker', ...).in('game_id', ...)` on every filter change
- **Problem**: Without composite indexes, these are full table scans on a table that grows daily
- **Fix**: Add indexes:
  ```sql
  CREATE INDEX idx_rpc_bookmaker_game ON raw_player_props_combined (bookmaker, game_id);
  CREATE INDEX idx_paper_bets_status_date ON paper_bets (status, game_date);
  CREATE INDEX idx_user_bets_status_date ON user_bets (status, game_date);
  ```
- **Effort**: Low

### Tier 3: Would need for 5,000+ users

#### OG Image Generation (`/api/slate`)
- **Where**: CPU-bound image rendering in a Vercel serverless function
- **Problem**: Each request loads a font file + renders a 1080px PNG. Under heavy load, functions queue up and can timeout (60s default).
- **Fix**: Cache generated images (hash picks → CDN URL), or pre-render during off-peak hours.
- **Effort**: Medium-High

#### Supabase Connection Pool Limits
- **Where**: Every Supabase query consumes a connection from the pool
- **Problem**: Free/Pro tier pools have limited slots. Pages making 3-4 sequential queries hold connections longer.
- **Fix**: Upgrade Supabase tier, consolidate sequential queries into RPCs, use connection-efficient patterns.
- **Effort**: Low (tier upgrade) to Medium (query consolidation)

---

## Scaling Roadmap

### Phase 1: Easy Wins (half a day)
- [ ] Add database indexes for common query patterns
- [ ] Switch middleware from `getUser()` to `getSession()`
- [ ] Add `.limit(500)` to history/performance queries as a safety net

### Phase 2: Pagination & Caching (1-2 days)
- [ ] Add date-range pagination to Performance page (default: last 30 days)
- [ ] Add date-range pagination to History page (already has date range, just add limit + offset)
- [ ] Add React Query for client-side caching across page navigations
- [ ] Consolidate the paper_bets + daily_predictions N+1 into a single RPC

### Phase 3: Scale to Thousands (as needed)
- [ ] Implement image caching for slate generation
- [ ] Add server-side filtering to Stats page
- [ ] Upgrade Supabase tier for more connections
- [ ] Consider edge-cached static pages for public routes (landing, pricing, picks)
- [ ] Add Vercel function configuration (memory, timeout) for heavy routes

### Phase 4: Enterprise (probably never needed)
- [ ] Redis caching layer for predictions
- [ ] Vercel Enterprise for function concurrency controls
- [ ] Database read replicas
- [ ] CDN-cached API responses for predictions (5-min staleness acceptable)

---

## What's Already Good

- **Bundle size**: Minimal — 8 dependencies, no heavy libraries
- **No WebSockets**: Polling for scoreboard (30s) avoids persistent connection overhead
- **External API caching**: NBA CDN cached at 30s (scoreboard) and 1hr (schedule)
- **Image optimization**: Using `next/image` with NBA CDN whitelist
- **RLS**: All tables have Row Level Security — no over-permissioned queries
- **Auth on API routes**: `/api/slate` requires authentication
- **Security headers**: X-Frame-Options, X-Content-Type-Options, Referrer-Policy, etc.

## Monitoring Checklist

When user count grows, watch these metrics:

1. **Vercel function duration** — if `/api/slate` exceeds 10s average, implement caching
2. **Supabase connection count** — if approaching pool limit, consolidate queries
3. **Page load time** — if Performance/History pages exceed 3s, add pagination
4. **Supabase API latency** — if `getUser()` calls in middleware exceed 200ms average, switch to `getSession()`
5. **Database query time** — check Supabase dashboard for slow queries, add indexes as needed
