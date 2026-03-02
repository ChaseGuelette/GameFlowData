-- Migration 005: Fast sportsbook lines RPC
-- The original get_sportsbook_lines(date) times out on Supabase because scanning
-- raw_player_props_combined by commence_time is too slow (no efficient index path).
--
-- New approach: dashboard first loads DFS lines (fast), then passes known game_ids
-- to get_sportsbook_lines_by_games(text[]). This uses idx_props_sportsbook_lookup
-- (game_id first column) and is sub-second.

CREATE OR REPLACE FUNCTION get_sportsbook_lines_by_games(p_game_ids text[])
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
    WHERE rp.game_id = ANY(p_game_ids)
      AND rp.market_key IN ('player_points', 'player_rebounds', 'player_assists')
      AND rp.bookmaker NOT IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
      AND rp.player_id IS NOT NULL
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

GRANT EXECUTE ON FUNCTION get_sportsbook_lines_by_games(text[]) TO anon, authenticated, service_role;
