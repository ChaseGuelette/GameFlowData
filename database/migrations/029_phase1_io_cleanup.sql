-- Migration 029: Phase 1 disk-IO cleanup
-- Addresses Supabase disk-IO budget depletion warnings.
-- Drops unused indexes (cuts write amplification), consolidates duplicate
-- permissive RLS policies, and fixes auth.uid() per-row re-evaluation on
-- user_sportsbooks.
--
-- Reversible: indexes can be recreated; policies can be recreated from this
-- migration's CREATE statements.

-- ============================================================================
-- 1. Drop unused indexes (zero scans reported by pg_stat_user_indexes)
-- ============================================================================
-- Each of these has idx_scan = 0 per Supabase performance advisor.
-- Keeping them costs IO on every INSERT/UPDATE to the parent table.

DROP INDEX IF EXISTS public.idx_polymarket_player_stat;
DROP INDEX IF EXISTS public.idx_user_subs_stripe_customer;
DROP INDEX IF EXISTS public.idx_user_subs_stripe_sub;
DROP INDEX IF EXISTS public.idx_mlb_pitcher_inning_stats_player_game;
DROP INDEX IF EXISTS public.idx_mlb_active_roster_date;
DROP INDEX IF EXISTS public.idx_mlb_active_roster_team;

-- ============================================================================
-- 2. Consolidate duplicate permissive RLS policies
-- ============================================================================
-- Each duplicate forces Postgres to evaluate both policies per row per SELECT.
-- We keep the broader policy and drop the redundant one.

-- user_bets_daily_log: _all covers SELECT (FOR ALL), so _select is redundant
DROP POLICY IF EXISTS user_bets_daily_log_select ON public.user_bets_daily_log;

-- mlb_* tables: "Allow public read" already grants SELECT to public, so the
-- narrower authenticated-only policies added in migration 023 are redundant.
DROP POLICY IF EXISTS "auth_read_mlb_avg_batting"  ON public.mlb_player_average_batting;
DROP POLICY IF EXISTS "auth_read_mlb_avg_pitching" ON public.mlb_player_average_pitching;
DROP POLICY IF EXISTS "auth_read_mlb_players"      ON public.mlb_players;
DROP POLICY IF EXISTS "auth_read_mlb_teams"        ON public.mlb_teams;

-- ============================================================================
-- 3. Fix user_sportsbooks RLS initplan
-- ============================================================================
-- Wrapping auth.uid() in a subquery lets Postgres evaluate it once per query
-- instead of once per row. See lint 0003_auth_rls_initplan.

DROP POLICY IF EXISTS "Users manage own sportsbooks" ON public.user_sportsbooks;

CREATE POLICY "Users manage own sportsbooks"
  ON public.user_sportsbooks
  FOR ALL
  USING ((SELECT auth.uid()) = user_id)
  WITH CHECK ((SELECT auth.uid()) = user_id);
