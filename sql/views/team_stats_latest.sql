-- View: team_stats_latest
-- Latest team rolling averages for current season (22025)
-- Used by the Data Vault Teams tab

CREATE OR REPLACE VIEW team_stats_latest AS
SELECT
    g.team_id, g.game_id, g.season_id, g.game_date, g.game_number,
    g.games_l5, g.games_l15, g.games_szn,
    -- Scoring
    g.avg_pts_l5, g.avg_pts_l15, g.avg_pts_szn,
    g.avg_fgm_l5, g.avg_fgm_l15, g.avg_fgm_szn,
    g.avg_fga_l5, g.avg_fga_l15, g.avg_fga_szn,
    g.avg_fg_pct_l5, g.avg_fg_pct_l15, g.avg_fg_pct_szn,
    g.avg_fg3m_l5, g.avg_fg3m_l15, g.avg_fg3m_szn,
    g.avg_fg3a_l5, g.avg_fg3a_l15, g.avg_fg3a_szn,
    g.avg_fg3_pct_l5, g.avg_fg3_pct_l15, g.avg_fg3_pct_szn,
    g.avg_ftm_l5, g.avg_ftm_l15, g.avg_ftm_szn,
    g.avg_fta_l5, g.avg_fta_l15, g.avg_fta_szn,
    g.avg_ft_pct_l5, g.avg_ft_pct_l15, g.avg_ft_pct_szn,
    -- Rebounds
    g.avg_oreb_l5, g.avg_oreb_l15, g.avg_oreb_szn,
    g.avg_dreb_l5, g.avg_dreb_l15, g.avg_dreb_szn,
    g.avg_reb_l5, g.avg_reb_l15, g.avg_reb_szn,
    -- Playmaking / Misc
    g.avg_ast_l5, g.avg_ast_l15, g.avg_ast_szn,
    g.avg_stl_l5, g.avg_stl_l15, g.avg_stl_szn,
    g.avg_blk_l5, g.avg_blk_l15, g.avg_blk_szn,
    g.avg_tov_l5, g.avg_tov_l15, g.avg_tov_szn,
    g.avg_pf_l5, g.avg_pf_l15, g.avg_pf_szn,
    g.avg_plus_minus_l5, g.avg_plus_minus_l15, g.avg_plus_minus_szn,
    -- Advanced
    g.avg_off_rtg_l5, g.avg_off_rtg_l15, g.avg_off_rtg_szn,
    g.avg_def_rtg_l5, g.avg_def_rtg_l15, g.avg_def_rtg_szn,
    g.avg_net_rtg_l5, g.avg_net_rtg_l15, g.avg_net_rtg_szn,
    g.avg_ts_pct_l5, g.avg_ts_pct_l15, g.avg_ts_pct_szn,
    g.avg_efg_pct_l5, g.avg_efg_pct_l15, g.avg_efg_pct_szn,
    g.avg_pace_l5, g.avg_pace_l15, g.avg_pace_szn,
    g.avg_poss_l5, g.avg_poss_l15, g.avg_poss_szn,
    g.avg_ast_ratio_l5, g.avg_ast_ratio_l15, g.avg_ast_ratio_szn,
    g.avg_tov_ratio_l5, g.avg_tov_ratio_l15, g.avg_tov_ratio_szn,
    g.avg_reb_pct_l5, g.avg_reb_pct_l15, g.avg_reb_pct_szn,
    g.avg_oreb_pct_l5, g.avg_oreb_pct_l15, g.avg_oreb_pct_szn,
    g.avg_dreb_pct_l5, g.avg_dreb_pct_l15, g.avg_dreb_pct_szn,
    g.avg_pie_l5, g.avg_pie_l15, g.avg_pie_szn,
    t.team_name
FROM (
    SELECT DISTINCT ON (team_id)
        *
    FROM team_average_game_stats
    WHERE season_id = '22025'
    ORDER BY team_id, game_date DESC
) g
JOIN teams t ON t.team_id = g.team_id;
