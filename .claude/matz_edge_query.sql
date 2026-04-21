-- Query Kalshi edge data for Steven Matz pitcher strikeouts
SELECT
    player_name,
    stat_type,
    line,
    side,
    yes_price,
    model_prob,
    kalshi_implied,
    raw_edge,
    fee_adjusted_edge,
    sportsbook_consensus_line,
    line_vs_sportsbook,
    bl_model_prob,
    bl_edge,
    game_date
FROM kalshi_edge_log
WHERE player_name ILIKE '%matz%'
  AND stat_type = 'pitcher_strikeouts'
ORDER BY game_date DESC, line ASC
LIMIT 20;
