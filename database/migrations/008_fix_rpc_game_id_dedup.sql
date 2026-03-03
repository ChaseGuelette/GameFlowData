-- Migration 008: Fix game_id deduplication in DFS and sportsbook RPCs
--
-- Bug: ROW_NUMBER partitioned on r.game_id (raw, e.g. '22400001') but the
-- output used LPAD(r.game_id, 10, '0') (padded, e.g. '0022400001').
-- If the same game exists with both padded and unpadded game_ids in
-- raw_player_props_combined, both rows get rn=1 and appear as duplicates.
--
-- Fix: LPAD inside the PARTITION BY so both variants collapse to one partition.

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

-- 3. Fix get_sportsbook_lines_by_games
DROP FUNCTION IF EXISTS get_sportsbook_lines_by_games(text[]);

CREATE FUNCTION get_sportsbook_lines_by_games(p_game_ids text[])
RETURNS TABLE (
  player_id   bigint,
  game_id     text,
  bookmaker   text,
  market_key  text,
  line        numeric,
  over_odds   integer,
  under_odds  integer,
  snapshot_time timestamptz
)
LANGUAGE sql SECURITY DEFINER STABLE AS $$
  WITH latest AS (
    SELECT
      rp.player_id,
      LPAD(rp.game_id, 10, '0') AS game_id,
      rp.bookmaker, rp.market_key,
      rp.outcome_label, rp.line, rp.odds_american, rp.snapshot_time,
      ROW_NUMBER() OVER (
        PARTITION BY rp.player_id, LPAD(rp.game_id, 10, '0'), rp.bookmaker, rp.market_key, rp.outcome_label
        ORDER BY rp.snapshot_time DESC
      ) AS rn
    FROM raw_player_props_combined rp
    WHERE LPAD(rp.game_id, 10, '0') = ANY(p_game_ids)
      AND rp.market_key IN ('player_points', 'player_rebounds', 'player_assists')
      AND rp.bookmaker NOT IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
      AND rp.player_id IS NOT NULL
      AND rp.snapshot_time > now() - interval '24 hours'
  )
  SELECT
    l.player_id, l.game_id, l.bookmaker, l.market_key, l.line,
    MAX(CASE WHEN l.outcome_label = 'Over'  THEN l.odds_american END)::integer AS over_odds,
    MAX(CASE WHEN l.outcome_label = 'Under' THEN l.odds_american END)::integer AS under_odds,
    MAX(l.snapshot_time) AS snapshot_time
  FROM latest l WHERE l.rn = 1
  GROUP BY l.player_id, l.game_id, l.bookmaker, l.market_key, l.line
  HAVING MAX(CASE WHEN l.outcome_label = 'Over'  THEN l.odds_american END) IS NOT NULL
     AND MAX(CASE WHEN l.outcome_label = 'Under' THEN l.odds_american END) IS NOT NULL;
$$;

GRANT EXECUTE ON FUNCTION get_sportsbook_lines_by_games(text[]) TO anon, authenticated, service_role;
