-- Function: get_sportsbook_lines
-- Returns NON-DFS bookmaker lines for games on target_date.
-- Used by the DFS Edge Finder page for Market Edge and Combined Edge modes.
-- Scopes by commence_time::date so it works before inference runs.

CREATE OR REPLACE FUNCTION get_sportsbook_lines(target_date date)
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
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN QUERY
  WITH latest AS (
    SELECT
      rp.player_id, rp.game_id, rp.bookmaker, rp.market_key,
      rp.outcome_label, rp.line, rp.odds_american, rp.snapshot_time,
      ROW_NUMBER() OVER (
        PARTITION BY rp.player_id, rp.game_id, rp.bookmaker, rp.market_key, rp.outcome_label
        ORDER BY rp.snapshot_time DESC
      ) AS rn
    FROM raw_player_props_combined rp
    WHERE rp.market_key IN ('player_points', 'player_rebounds', 'player_assists')
      AND rp.bookmaker NOT IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
      AND rp.player_id IS NOT NULL
      AND rp.commence_time::date = target_date
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
END; $$;
