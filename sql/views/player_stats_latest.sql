-- View: player_stats_latest
-- Joins latest player basic + advanced stats for current season (22025)
-- Used by the Data Vault Players tab

CREATE OR REPLACE VIEW player_stats_latest AS
SELECT
    g.player_id,
    p.player_name,
    COALESCE(
        ph.position_group,
        CASE p.position_group
            WHEN 'Guard' THEN 'G'
            WHEN 'Forward' THEN 'W'
            WHEN 'Big' THEN 'B'
            ELSE p.position_group
        END
    ) AS position_group,
    g.team_id,
    g.game_date,
    g.games_l5, g.games_l15, g.games_szn,
    -- Basic box score averages
    g.avg_min_l5, g.avg_min_l15, g.avg_min_szn,
    g.avg_pts_l5, g.avg_pts_l15, g.avg_pts_szn,
    g.avg_reb_l5, g.avg_reb_l15, g.avg_reb_szn,
    g.avg_ast_l5, g.avg_ast_l15, g.avg_ast_szn,
    g.avg_stl_l5, g.avg_stl_l15, g.avg_stl_szn,
    g.avg_blk_l5, g.avg_blk_l15, g.avg_blk_szn,
    g.avg_tov_l5, g.avg_tov_l15, g.avg_tov_szn,
    g.avg_pf_l5, g.avg_pf_l15, g.avg_pf_szn,
    g.avg_plus_minus_l5, g.avg_plus_minus_l15, g.avg_plus_minus_szn,
    -- Shooting
    g.avg_fgm_l5, g.avg_fgm_l15, g.avg_fgm_szn,
    g.avg_fga_l5, g.avg_fga_l15, g.avg_fga_szn,
    g.avg_fg_pct_l5, g.avg_fg_pct_l15, g.avg_fg_pct_szn,
    g.avg_fg3m_l5, g.avg_fg3m_l15, g.avg_fg3m_szn,
    g.avg_fg3a_l5, g.avg_fg3a_l15, g.avg_fg3a_szn,
    g.avg_fg3_pct_l5, g.avg_fg3_pct_l15, g.avg_fg3_pct_szn,
    g.avg_ftm_l5, g.avg_ftm_l15, g.avg_ftm_szn,
    g.avg_fta_l5, g.avg_fta_l15, g.avg_fta_szn,
    g.avg_ft_pct_l5, g.avg_ft_pct_l15, g.avg_ft_pct_szn,
    g.avg_oreb_l5, g.avg_oreb_l15, g.avg_oreb_szn,
    g.avg_dreb_l5, g.avg_dreb_l15, g.avg_dreb_szn,
    -- L3 averages
    g.avg_min_l3, g.avg_pts_l3, g.avg_reb_l3, g.avg_ast_l3, g.avg_fg3m_l3,
    -- Consistency
    g.std_pts_l5, g.std_reb_l5, g.std_ast_l5, g.std_fg3m_l5, g.std_min_l5,
    g.min_floor_l5, g.games_started_l5,
    g.rest_days, g.games_last_7d,
    -- Advanced stats (from separate table)
    a.avg_off_rtg_l5, a.avg_off_rtg_l15, a.avg_off_rtg_szn,
    a.avg_def_rtg_l5, a.avg_def_rtg_l15, a.avg_def_rtg_szn,
    a.avg_net_rtg_l5, a.avg_net_rtg_l15, a.avg_net_rtg_szn,
    a.avg_ts_pct_l5, a.avg_ts_pct_l15, a.avg_ts_pct_szn,
    a.avg_efg_pct_l5, a.avg_efg_pct_l15, a.avg_efg_pct_szn,
    a.avg_usg_pct_l5, a.avg_usg_pct_l15, a.avg_usg_pct_szn,
    a.avg_ast_pct_l5, a.avg_ast_pct_l15, a.avg_ast_pct_szn,
    a.avg_ast_tov_l5, a.avg_ast_tov_l15, a.avg_ast_tov_szn,
    a.avg_tov_ratio_l5, a.avg_tov_ratio_l15, a.avg_tov_ratio_szn,
    a.avg_reb_pct_l5, a.avg_reb_pct_l15, a.avg_reb_pct_szn,
    a.avg_oreb_pct_l5, a.avg_oreb_pct_l15, a.avg_oreb_pct_szn,
    a.avg_dreb_pct_l5, a.avg_dreb_pct_l15, a.avg_dreb_pct_szn,
    a.avg_pace_l5, a.avg_pace_l15, a.avg_pace_szn,
    a.avg_pie_l5, a.avg_pie_l15, a.avg_pie_szn
FROM (
    SELECT DISTINCT ON (player_id)
        *
    FROM player_average_game_stats
    WHERE season_id = '22025'
    ORDER BY player_id, game_date DESC
) g
JOIN players p ON p.player_id = g.player_id
LEFT JOIN (
    SELECT DISTINCT ON (player_id)
        player_id, position_group
    FROM player_position_history
    WHERE season_id = '22025'
    ORDER BY player_id, snapshot_date DESC
) ph ON ph.player_id = g.player_id
LEFT JOIN (
    SELECT DISTINCT ON (player_id)
        *
    FROM player_average_advanced_stats
    WHERE season_id = '22025'
    ORDER BY player_id, game_date DESC
) a ON a.player_id = g.player_id;
