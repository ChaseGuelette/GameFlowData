-- Function: get_games_for_date
-- Returns distinct games from staging table for a target date (ET convention).
-- Used as fallback on the dashboard when predictions haven't been generated yet.
-- Uses ET date boundaries to match prediction_date convention (NBA games tip off
-- 7-10pm ET, which is the next UTC date).

CREATE OR REPLACE FUNCTION get_games_for_date(target_date date)
RETURNS TABLE (home_team text, away_team text, commence_time timestamptz)
LANGUAGE plpgsql SECURITY DEFINER AS $func$
BEGIN
  RETURN QUERY
  SELECT DISTINCT ON (g.home_team, g.away_team)
    g.home_team, g.away_team, g.commence_time
  FROM raw_game_lines_staging g
  WHERE g.commence_time >= (target_date::timestamp AT TIME ZONE 'America/New_York')
    AND g.commence_time < ((target_date + 1)::timestamp AT TIME ZONE 'America/New_York')
  ORDER BY g.home_team, g.away_team, g.commence_time DESC;
END; $func$;

-- Required index (must exist for performance on 3M+ row table):
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_commence_time
--   ON raw_game_lines_staging (commence_time);
