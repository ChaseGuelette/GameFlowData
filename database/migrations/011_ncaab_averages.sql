-- Migration 011: NCAAB Team Rolling Averages Table
-- Pre-computed shift(1) rolling team stats for feature store consumption.

CREATE TABLE IF NOT EXISTS ncaab_team_rolling_averages (
    avg_id          BIGSERIAL PRIMARY KEY,
    team_id         INTEGER NOT NULL REFERENCES ncaab_teams(team_id),
    game_id         BIGINT NOT NULL REFERENCES ncaab_game_schedule(game_id),
    game_date       DATE NOT NULL,
    season          INTEGER NOT NULL,
    game_number     SMALLINT,
    -- Window counts
    games_l5        SMALLINT,
    games_l10       SMALLINT,
    games_l20       SMALLINT,
    games_szn       SMALLINT,
    -- Box score rolling averages (L5/L10/L20/SZN)
    avg_pts_l5      NUMERIC, avg_pts_l10     NUMERIC, avg_pts_l20     NUMERIC, avg_pts_szn     NUMERIC,
    avg_poss_l5     NUMERIC, avg_poss_l10    NUMERIC, avg_poss_l20    NUMERIC, avg_poss_szn    NUMERIC,
    avg_fga_l5      NUMERIC, avg_fga_l10     NUMERIC, avg_fga_l20     NUMERIC, avg_fga_szn     NUMERIC,
    avg_fta_l5      NUMERIC, avg_fta_l10     NUMERIC, avg_fta_l20     NUMERIC, avg_fta_szn     NUMERIC,
    avg_oreb_l5     NUMERIC, avg_oreb_l10    NUMERIC, avg_oreb_szn    NUMERIC,
    avg_dreb_l5     NUMERIC, avg_dreb_l10    NUMERIC, avg_dreb_szn    NUMERIC,
    avg_ast_l5      NUMERIC, avg_ast_l10     NUMERIC, avg_ast_szn     NUMERIC,
    avg_tov_l5      NUMERIC, avg_tov_l10     NUMERIC, avg_tov_szn     NUMERIC,
    avg_stl_l5      NUMERIC, avg_stl_szn     NUMERIC,
    avg_blk_l5      NUMERIC, avg_blk_szn     NUMERIC,
    avg_fg3m_l5     NUMERIC, avg_fg3m_l10    NUMERIC, avg_fg3m_szn    NUMERIC,
    avg_fg3a_l5     NUMERIC, avg_fg3a_l10    NUMERIC, avg_fg3a_szn    NUMERIC,
    -- Four Factors rolling averages
    avg_efg_pct_l5      NUMERIC, avg_efg_pct_l10     NUMERIC, avg_efg_pct_szn     NUMERIC,
    avg_tov_pct_l5      NUMERIC, avg_tov_pct_l10     NUMERIC, avg_tov_pct_szn     NUMERIC,
    avg_orb_pct_l5      NUMERIC, avg_orb_pct_l10     NUMERIC, avg_orb_pct_szn     NUMERIC,
    avg_ft_rate_l5      NUMERIC, avg_ft_rate_l10     NUMERIC, avg_ft_rate_szn     NUMERIC,
    -- Defensive Four Factors
    avg_opp_efg_pct_l5  NUMERIC, avg_opp_efg_pct_szn NUMERIC,
    avg_opp_tov_pct_l5  NUMERIC, avg_opp_tov_pct_szn NUMERIC,
    avg_opp_orb_pct_l5  NUMERIC, avg_opp_orb_pct_szn NUMERIC,
    avg_opp_ft_rate_l5  NUMERIC, avg_opp_ft_rate_szn NUMERIC,
    -- Standard deviations
    std_pts_l5      NUMERIC,
    std_poss_l5     NUMERIC,
    -- Rest/schedule context
    rest_days       SMALLINT,
    games_last_7d   SMALLINT,

    UNIQUE(team_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_ncaab_avgroll_team_date
    ON ncaab_team_rolling_averages(team_id, game_date DESC);

ALTER TABLE ncaab_team_rolling_averages ENABLE ROW LEVEL SECURITY;
