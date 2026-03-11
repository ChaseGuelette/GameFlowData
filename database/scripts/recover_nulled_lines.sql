-- Recovery script: restore nulled-out lines on daily_predictions
-- ================================================================
-- The edge_refresh_job was nulling out line/odds columns on old predictions
-- when fresh lines weren't available (snapshot_time > now() - 24h filter).
-- This script restores them from raw_player_props_combined.
--
-- Run once against Supabase to fix historical data.
-- Safe to run multiple times (only updates NULL lines).

-- Step 1: Check how many predictions have NULL lines
SELECT prediction_date, COUNT(*) as null_line_count
FROM daily_predictions
WHERE line IS NULL
GROUP BY prediction_date
ORDER BY prediction_date DESC;

-- Step 2: Restore lines from raw_player_props_combined
WITH grouped_props AS (
    SELECT
        player_id,
        LPAD(game_id, 10, '0') as game_id,
        bookmaker,
        market_key,
        line,
        MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
        MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
    FROM raw_player_props_combined
    WHERE player_id IS NOT NULL
      AND line IS NOT NULL
      AND market_key IN ('player_points', 'player_rebounds', 'player_assists')
    GROUP BY player_id, LPAD(game_id, 10, '0'), bookmaker, market_key, line
    HAVING MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) IS NOT NULL
       AND MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) IS NOT NULL
),
-- Pick sharpest book (lowest vig) per player/game/market
best_props AS (
    SELECT DISTINCT ON (player_id, game_id, market_key)
        player_id,
        game_id,
        bookmaker,
        market_key,
        line,
        over_odds,
        under_odds
    FROM grouped_props
    ORDER BY player_id, game_id, market_key,
        -- vig = sum of implied probs; lower = sharper
        (CASE WHEN over_odds > 0 THEN 100.0/(over_odds+100) ELSE ABS(over_odds)/(ABS(over_odds)+100.0) END
       + CASE WHEN under_odds > 0 THEN 100.0/(under_odds+100) ELSE ABS(under_odds)/(ABS(under_odds)+100.0) END)
        ASC
),
mapped_props AS (
    SELECT
        player_id,
        game_id,
        CASE market_key
            WHEN 'player_points' THEN 'pts'
            WHEN 'player_rebounds' THEN 'reb'
            WHEN 'player_assists' THEN 'ast'
        END as stat,
        line,
        over_odds,
        under_odds,
        bookmaker
    FROM best_props
)
UPDATE daily_predictions dp
SET
    line = mp.line,
    over_odds = mp.over_odds,
    under_odds = mp.under_odds,
    bookmaker = mp.bookmaker
FROM mapped_props mp
WHERE dp.player_id = mp.player_id
  AND dp.game_id = mp.game_id
  AND dp.stat = mp.stat
  AND dp.line IS NULL;

-- Step 3: Verify the fix
SELECT prediction_date, COUNT(*) as null_line_count
FROM daily_predictions
WHERE line IS NULL
GROUP BY prediction_date
ORDER BY prediction_date DESC;
