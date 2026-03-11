-- Migration 005: Fast sportsbook lines RPC
-- The original get_sportsbook_lines(date) times out on Supabase because scanning
-- raw_player_props_combined by commence_time is too slow (millions of accumulated
-- snapshot rows, no efficient index path for sportsbooks).
--
-- New approach: dashboard first loads DFS lines (fast), extracts unique game_ids,
-- then passes them to get_sportsbook_lines_by_games(text[]) in batches of 3.
-- This uses idx_props_sportsbook_lookup (game_id first column) and the 24h
-- snapshot_time cutoff to keep queries sub-second.

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
