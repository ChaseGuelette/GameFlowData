-- View: defense_by_position_latest
-- Latest opponent-allowed stats by position group for current season
-- Used by the Data Vault Defense tab
-- Note: game_date filter uses '2025-10-01' to cover the 2025-26 season

CREATE OR REPLACE VIEW defense_by_position_latest AS
SELECT
    d.team_id, d.game_id, d.game_date, d.position_group,
    d.games_l5, d.games_l15, d.games_szn,
    d.poss_faced_l5, d.poss_faced_l15, d.poss_faced_szn,
    -- Totals (rolling averages per game)
    d.pts_allowed_l5, d.pts_allowed_l15, d.pts_allowed_szn,
    d.reb_allowed_l5, d.reb_allowed_l15, d.reb_allowed_szn,
    d.ast_allowed_l5, d.ast_allowed_l15, d.ast_allowed_szn,
    d.stl_allowed_l5, d.stl_allowed_l15, d.stl_allowed_szn,
    d.blk_allowed_l5, d.blk_allowed_l15, d.blk_allowed_szn,
    d.threes_allowed_l5, d.threes_allowed_l15, d.threes_allowed_szn,
    d.tov_forced_l5, d.tov_forced_l15, d.tov_forced_szn,
    d.fta_allowed_l5, d.fta_allowed_l15, d.fta_allowed_szn,
    d.oreb_allowed_l5, d.oreb_allowed_l15, d.oreb_allowed_szn,
    d.pf_allowed_l5, d.pf_allowed_l15, d.pf_allowed_szn,
    -- Per-100 possession rates
    d.off_rtg_allowed_l5, d.off_rtg_allowed_l15, d.off_rtg_allowed_szn,
    d.reb_per100_allowed_l5, d.reb_per100_allowed_l15, d.reb_per100_allowed_szn,
    d.ast_per100_allowed_l5, d.ast_per100_allowed_l15, d.ast_per100_allowed_szn,
    d.stl_per100_allowed_l5, d.stl_per100_allowed_l15, d.stl_per100_allowed_szn,
    d.blk_per100_allowed_l5, d.blk_per100_allowed_l15, d.blk_per100_allowed_szn,
    d.threes_per100_allowed_l5, d.threes_per100_allowed_l15, d.threes_per100_allowed_szn,
    d.tov_per100_forced_l5, d.tov_per100_forced_l15, d.tov_per100_forced_szn,
    d.fta_per100_allowed_l5, d.fta_per100_allowed_l15, d.fta_per100_allowed_szn,
    d.oreb_per100_allowed_l5, d.oreb_per100_allowed_l15, d.oreb_per100_allowed_szn,
    d.pf_per100_allowed_l5, d.pf_per100_allowed_l15, d.pf_per100_allowed_szn,
    d.created_at,
    t.team_name
FROM (
    SELECT DISTINCT ON (team_id, position_group)
        *
    FROM team_allowed_by_position
    WHERE game_date >= '2025-10-01'::date
      AND position_group IN ('G', 'W', 'B')
    ORDER BY team_id, position_group, game_date DESC
) d
JOIN teams t ON t.team_id = d.team_id;
