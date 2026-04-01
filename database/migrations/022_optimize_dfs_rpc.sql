-- Migration 022: Optimize get_dfs_lines and get_sportsbook_lines RPCs
--
-- Changes:
-- 1. Remove LPAD from PARTITION BY clauses — game_ids are now normalized
--    to 10-digit format in scrapers, and old data is archived.
-- 2. Switch from LANGUAGE plpgsql to LANGUAGE sql STABLE for better
--    query planning (allows inlining).
-- 3. Add statement_timeout override (30s) to handle edge cases.
-- 4. Keep LPAD in SELECT output for backward compatibility (no-op on
--    already-normalized data).

-- 1. Optimized get_dfs_lines
CREATE OR REPLACE FUNCTION public.get_dfs_lines(target_date date)
 RETURNS TABLE(
   player_id bigint,
   player_name text,
   game_id text,
   bookmaker text,
   market_key text,
   line numeric,
   over_odds integer,
   under_odds integer,
   snapshot_time timestamp with time zone,
   game_time timestamp with time zone
 )
 LANGUAGE sql
 STABLE
 SECURITY DEFINER
 SET statement_timeout = '30s'
AS $$
  WITH dfs_raw AS (
    SELECT
      r.player_id,
      r.game_id,
      r.bookmaker,
      r.market_key,
      r.line,
      r.odds_american,
      r.outcome_label,
      r.snapshot_time,
      r.commence_time,
      ROW_NUMBER() OVER (
        PARTITION BY r.player_id, r.game_id, r.bookmaker, r.market_key, r.outcome_label
        ORDER BY r.snapshot_time DESC
      ) AS rn
    FROM raw_player_props_combined r
    WHERE r.bookmaker IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
      AND r.player_id IS NOT NULL
      AND r.commence_time >= (target_date::timestamp AT TIME ZONE 'America/New_York')
      AND r.commence_time < ((target_date + 1)::timestamp AT TIME ZONE 'America/New_York')
      AND r.snapshot_time > now() - interval '48 hours'
  )
  SELECT
    d.player_id,
    COALESCE(p.player_name, 'Player ' || d.player_id::text) AS player_name,
    LPAD(d.game_id, 10, '0') AS game_id,
    d.bookmaker,
    d.market_key,
    d.line,
    MAX(CASE WHEN d.outcome_label = 'Over' THEN d.odds_american END)::integer AS over_odds,
    MAX(CASE WHEN d.outcome_label = 'Under' THEN d.odds_american END)::integer AS under_odds,
    MAX(d.snapshot_time) AS snapshot_time,
    MAX(d.commence_time) AS game_time
  FROM dfs_raw d
  LEFT JOIN players p ON p.player_id = d.player_id
  WHERE d.rn = 1
  GROUP BY d.player_id, p.player_name, d.game_id, d.bookmaker, d.market_key, d.line;
$$;

-- 2. Optimized get_sportsbook_lines
CREATE OR REPLACE FUNCTION public.get_sportsbook_lines(target_date date)
 RETURNS TABLE(
   player_id bigint,
   game_id text,
   bookmaker text,
   market_key text,
   line numeric,
   over_odds integer,
   under_odds integer,
   snapshot_time timestamp with time zone
 )
 LANGUAGE sql
 STABLE
 SECURITY DEFINER
 SET statement_timeout = '30s'
AS $$
  WITH sb_raw AS (
    SELECT
      r.player_id,
      r.game_id,
      r.bookmaker,
      r.market_key,
      r.line,
      r.odds_american,
      r.outcome_label,
      r.snapshot_time,
      ROW_NUMBER() OVER (
        PARTITION BY r.player_id, r.game_id, r.bookmaker, r.market_key, r.outcome_label
        ORDER BY r.snapshot_time DESC
      ) AS rn
    FROM raw_player_props_combined r
    WHERE r.bookmaker NOT IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
      AND r.player_id IS NOT NULL
      AND r.commence_time >= (target_date::timestamp AT TIME ZONE 'America/New_York')
      AND r.commence_time < ((target_date + 1)::timestamp AT TIME ZONE 'America/New_York')
      AND r.snapshot_time > now() - interval '48 hours'
  )
  SELECT
    d.player_id,
    LPAD(d.game_id, 10, '0') AS game_id,
    d.bookmaker,
    d.market_key,
    d.line,
    MAX(CASE WHEN d.outcome_label = 'Over' THEN d.odds_american END)::integer AS over_odds,
    MAX(CASE WHEN d.outcome_label = 'Under' THEN d.odds_american END)::integer AS under_odds,
    MAX(d.snapshot_time) AS snapshot_time
  FROM sb_raw d
  WHERE d.rn = 1
  GROUP BY d.player_id, d.game_id, d.bookmaker, d.market_key, d.line;
$$;
