-- Cut dashboard NBA quote RPCs over to the short-retention inference table.
-- Legacy raw tables are intentionally left untouched.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE FUNCTION public.get_game_commence_times(p_game_ids text[])
RETURNS TABLE(game_id text, commence_time timestamptz)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO public, pg_temp
SET statement_timeout TO '30s'
AS $function$
  SELECT requested.game_id, quote.commence_time
  FROM unnest(p_game_ids) AS requested(game_id)
  CROSS JOIN LATERAL (
    SELECT p.commence_time
    FROM public.nba_player_props_inference AS p
    WHERE p.game_id = requested.game_id
      AND p.snapshot_time > now() - interval '48 hours'
    ORDER BY p.source_snapshot_time DESC, p.staging_id DESC
    LIMIT 1
  ) AS quote;
$function$;

CREATE OR REPLACE FUNCTION public.get_sportsbook_lines(target_date date)
RETURNS TABLE(
  player_id bigint,
  game_id text,
  bookmaker text,
  market_key text,
  line numeric,
  over_odds integer,
  under_odds integer,
  snapshot_time timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO public, pg_temp
SET statement_timeout TO '30s'
AS $function$
  WITH latest AS (
    SELECT
      p.player_id,
      p.game_id,
      p.bookmaker,
      p.market_key,
      p.line,
      p.odds_american,
      p.outcome_label,
      p.snapshot_time,
      row_number() OVER (
        PARTITION BY p.player_id, p.game_id, p.bookmaker, p.market_key, p.outcome_label
        ORDER BY p.snapshot_time DESC, p.staging_id DESC
      ) AS rn
    FROM public.nba_player_props_inference AS p
    WHERE p.bookmaker NOT IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
      AND p.player_id IS NOT NULL
      AND p.game_id IS NOT NULL
      AND p.commence_time >= (target_date::timestamp AT TIME ZONE 'America/New_York')
      AND p.commence_time < ((target_date + 1)::timestamp AT TIME ZONE 'America/New_York')
      AND p.snapshot_time > now() - interval '48 hours'
  )
  SELECT
    l.player_id,
    l.game_id,
    l.bookmaker,
    l.market_key,
    l.line,
    max(CASE WHEN l.outcome_label = 'Over' THEN l.odds_american END)::integer AS over_odds,
    max(CASE WHEN l.outcome_label = 'Under' THEN l.odds_american END)::integer AS under_odds,
    max(l.snapshot_time) AS snapshot_time
  FROM latest AS l
  WHERE l.rn = 1
  GROUP BY l.player_id, l.game_id, l.bookmaker, l.market_key, l.line;
$function$;

CREATE OR REPLACE FUNCTION public.get_sportsbook_lines_by_games(p_game_ids text[])
RETURNS TABLE(
  player_id bigint,
  game_id text,
  bookmaker text,
  market_key text,
  line numeric,
  over_odds integer,
  under_odds integer,
  snapshot_time timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO public, pg_temp
SET statement_timeout TO '30s'
AS $function$
  WITH latest AS (
    SELECT
      p.player_id,
      p.game_id,
      p.bookmaker,
      p.market_key,
      p.line,
      p.odds_american,
      p.outcome_label,
      p.snapshot_time,
      row_number() OVER (
        PARTITION BY p.player_id, p.game_id, p.bookmaker, p.market_key, p.outcome_label
        ORDER BY p.snapshot_time DESC, p.staging_id DESC
      ) AS rn
    FROM public.nba_player_props_inference AS p
    WHERE p.game_id = ANY(p_game_ids)
      AND p.bookmaker NOT IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
      AND p.player_id IS NOT NULL
      AND p.snapshot_time > now() - interval '24 hours'
  )
  SELECT
    l.player_id,
    l.game_id,
    l.bookmaker,
    l.market_key,
    l.line,
    max(CASE WHEN l.outcome_label = 'Over' THEN l.odds_american END)::integer AS over_odds,
    max(CASE WHEN l.outcome_label = 'Under' THEN l.odds_american END)::integer AS under_odds,
    max(l.snapshot_time) AS snapshot_time
  FROM latest AS l
  WHERE l.rn = 1
  GROUP BY l.player_id, l.game_id, l.bookmaker, l.market_key, l.line
  HAVING max(CASE WHEN l.outcome_label = 'Over' THEN l.odds_american END) IS NOT NULL
     AND max(CASE WHEN l.outcome_label = 'Under' THEN l.odds_american END) IS NOT NULL;
$function$;

REVOKE ALL ON FUNCTION public.get_game_commence_times(text[]) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION public.get_sportsbook_lines(date) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION public.get_sportsbook_lines_by_games(text[]) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.get_game_commence_times(text[]) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_sportsbook_lines(date) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_sportsbook_lines_by_games(text[]) TO authenticated, service_role;

REVOKE ALL ON FUNCTION public.get_dfs_lines(date) FROM PUBLIC, anon, authenticated, service_role;
DROP FUNCTION public.get_dfs_lines(date);

COMMIT;
