-- Fix get_dfs_lines to scope by ET calendar day instead of UTC day
--
-- Root cause: predictions use prediction_date in ET (e.g., a 7 PM ET game on March 2
-- has prediction_date = March 2), but get_dfs_lines was scoping by UTC day
-- (commence_time >= target_date::timestamptz), so that same game (March 3 00:10 UTC)
-- fell outside the March 2 UTC window. Result: DFS page showed no edges because
-- the game_ids from DFS lines didn't match the game_ids from predictions.
--
-- Fix: use AT TIME ZONE 'America/New_York' to match the ET-based prediction_date.

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
        PARTITION BY r.player_id, r.game_id, r.bookmaker, r.market_key, r.outcome_label
        ORDER BY r.snapshot_time DESC
      ) AS rn
    FROM raw_player_props_combined r
    WHERE r.bookmaker IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
      AND r.player_id IS NOT NULL
      AND (r.commence_time AT TIME ZONE 'America/New_York')::date = target_date
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

-- Also fix get_sportsbook_lines to use ET day scoping for consistency
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
        PARTITION BY r.player_id, r.game_id, r.bookmaker, r.market_key, r.outcome_label
        ORDER BY r.snapshot_time DESC
      ) AS rn
    FROM raw_player_props_combined r
    WHERE r.bookmaker NOT IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
      AND r.player_id IS NOT NULL
      AND (r.commence_time AT TIME ZONE 'America/New_York')::date = target_date
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
