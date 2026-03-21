-- Migration 019: Fix DFS & sportsbook RPC performance
--
-- Problem: get_dfs_lines and get_sportsbook_lines use
--   (r.commence_time AT TIME ZONE 'America/New_York')::date = target_date
-- which applies a timezone conversion + date cast to EVERY row, preventing
-- idx_props_commence_time from being used. On the 25M+ row
-- raw_player_props_combined table this causes a full table scan that exceeds
-- the Supabase statement timeout.
--
-- Fix: Convert target_date ET boundaries to UTC timestamps instead:
--   r.commence_time >= (target_date::timestamp AT TIME ZONE 'America/New_York')
--   r.commence_time <  ((target_date + 1)::timestamp AT TIME ZONE 'America/New_York')
-- This is both correct (respects ET calendar day, handles DST) AND fast
-- (the index on commence_time is usable).
--
-- Also adds a snapshot_time cutoff (48 hours) to further limit the scan,
-- matching the pattern already used in get_sportsbook_lines_by_games.

-- 1. Fix get_dfs_lines
CREATE OR REPLACE FUNCTION public.get_dfs_lines(target_date date)
 RETURNS TABLE(player_id bigint, player_name text, game_id text, bookmaker text, market_key text, line numeric, over_odds integer, under_odds integer, snapshot_time timestamp with time zone, game_time timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH dfs_raw AS (
    SELECT
      r.player_id,
      LPAD(r.game_id, 10, '0') AS game_id,
      r.bookmaker,
      r.market_key,
      r.line,
      r.odds_american,
      r.outcome_label,
      r.snapshot_time,
      r.commence_time,
      ROW_NUMBER() OVER (
        PARTITION BY r.player_id, LPAD(r.game_id, 10, '0'), r.bookmaker, r.market_key, r.outcome_label
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
    d.game_id,
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
END; $function$;

-- 2. Fix get_sportsbook_lines
CREATE OR REPLACE FUNCTION public.get_sportsbook_lines(target_date date)
 RETURNS TABLE(player_id bigint, game_id text, bookmaker text, market_key text, line numeric, over_odds integer, under_odds integer, snapshot_time timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH sb_raw AS (
    SELECT
      r.player_id,
      LPAD(r.game_id, 10, '0') AS game_id,
      r.bookmaker,
      r.market_key,
      r.line,
      r.odds_american,
      r.outcome_label,
      r.snapshot_time,
      ROW_NUMBER() OVER (
        PARTITION BY r.player_id, LPAD(r.game_id, 10, '0'), r.bookmaker, r.market_key, r.outcome_label
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
    d.game_id,
    d.bookmaker,
    d.market_key,
    d.line,
    MAX(CASE WHEN d.outcome_label = 'Over' THEN d.odds_american END)::integer AS over_odds,
    MAX(CASE WHEN d.outcome_label = 'Under' THEN d.odds_american END)::integer AS under_odds,
    MAX(d.snapshot_time) AS snapshot_time
  FROM sb_raw d
  WHERE d.rn = 1
  GROUP BY d.player_id, d.game_id, d.bookmaker, d.market_key, d.line;
END; $function$;
