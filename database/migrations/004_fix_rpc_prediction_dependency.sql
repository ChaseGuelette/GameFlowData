-- Migration 004: Remove daily_predictions dependency from DFS RPCs
-- Both get_dfs_lines and get_sportsbook_lines previously scoped games via
-- daily_predictions, meaning they returned nothing before inference ran.
-- Now they scope by commence_time range directly, so DFS/market edge data
-- is available as soon as lines are scraped.
--
-- Also adds idx_props_commence_time index for performance — the old
-- commence_time::date cast caused full table scans and statement timeouts.

-- 0. Index for commence_time range queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_props_commence_time
  ON raw_player_props_combined (commence_time);

-- 1. get_dfs_lines: DROP + recreate (return type changes: adds player_name, game_time)
DROP FUNCTION IF EXISTS get_dfs_lines(date);

CREATE OR REPLACE FUNCTION get_dfs_lines(target_date date)
RETURNS TABLE (
  player_id     bigint,
  player_name   text,
  game_id       text,
  bookmaker     text,
  market_key    text,
  line          numeric,
  over_odds     integer,
  under_odds    integer,
  snapshot_time timestamptz,
  game_time     timestamptz
)
LANGUAGE plpgsql SECURITY DEFINER AS $$
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
      AND r.commence_time >= target_date::timestamptz
      AND r.commence_time < (target_date + 1)::timestamptz
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
END; $$;


-- 2. get_sportsbook_lines: same fix — range instead of ::date cast
DROP FUNCTION IF EXISTS get_sportsbook_lines(date);

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
      AND rp.commence_time >= target_date::timestamptz
      AND rp.commence_time < (target_date + 1)::timestamptz
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
