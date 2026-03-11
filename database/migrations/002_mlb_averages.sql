-- Migration 002: MLB Rolling Average Tables
-- Creates mlb_player_average_batting and mlb_player_average_pitching
-- for model consumption (shift(1) pre-game rolling averages).

-- ============================================================================
-- BATTING AVERAGES
-- ============================================================================

CREATE TABLE IF NOT EXISTS mlb_player_average_batting (
    -- Identity
    player_id       INTEGER NOT NULL,
    game_id         INTEGER NOT NULL,
    game_date       DATE NOT NULL,
    season          INTEGER NOT NULL,
    team_id         INTEGER,

    -- Window counts
    game_number     INTEGER,
    games_l5        SMALLINT,
    games_l10       SMALLINT,
    games_l20       SMALLINT,
    games_szn       SMALLINT,

    -- 12 batting stats x 4 windows (L5/L10/L20/SZN) = 48 columns
    avg_pa_l5       NUMERIC, avg_pa_l10      NUMERIC, avg_pa_l20      NUMERIC, avg_pa_szn      NUMERIC,
    avg_ab_l5       NUMERIC, avg_ab_l10      NUMERIC, avg_ab_l20      NUMERIC, avg_ab_szn      NUMERIC,
    avg_r_l5        NUMERIC, avg_r_l10       NUMERIC, avg_r_l20       NUMERIC, avg_r_szn       NUMERIC,
    avg_h_l5        NUMERIC, avg_h_l10       NUMERIC, avg_h_l20       NUMERIC, avg_h_szn       NUMERIC,
    avg_doubles_l5  NUMERIC, avg_doubles_l10 NUMERIC, avg_doubles_l20 NUMERIC, avg_doubles_szn NUMERIC,
    avg_triples_l5  NUMERIC, avg_triples_l10 NUMERIC, avg_triples_l20 NUMERIC, avg_triples_szn NUMERIC,
    avg_hr_l5       NUMERIC, avg_hr_l10      NUMERIC, avg_hr_l20      NUMERIC, avg_hr_szn      NUMERIC,
    avg_rbi_l5      NUMERIC, avg_rbi_l10     NUMERIC, avg_rbi_l20     NUMERIC, avg_rbi_szn     NUMERIC,
    avg_bb_l5       NUMERIC, avg_bb_l10      NUMERIC, avg_bb_l20      NUMERIC, avg_bb_szn      NUMERIC,
    avg_so_l5       NUMERIC, avg_so_l10      NUMERIC, avg_so_l20      NUMERIC, avg_so_szn      NUMERIC,
    avg_sb_l5       NUMERIC, avg_sb_l10      NUMERIC, avg_sb_l20      NUMERIC, avg_sb_szn      NUMERIC,
    avg_tb_l5       NUMERIC, avg_tb_l10      NUMERIC, avg_tb_l20      NUMERIC, avg_tb_szn      NUMERIC,

    -- 7 std devs at L5
    std_h_l5        NUMERIC,
    std_hr_l5       NUMERIC,
    std_tb_l5       NUMERIC,
    std_rbi_l5      NUMERIC,
    std_r_l5        NUMERIC,
    std_so_l5       NUMERIC,
    std_sb_l5       NUMERIC,

    -- 4 rate stats at L10 (computed from rolling sums, not avg of per-game rates)
    avg_batting_avg_l10 NUMERIC,
    avg_obp_l10         NUMERIC,
    avg_slg_l10         NUMERIC,
    avg_ops_l10         NUMERIC,

    -- Context
    rest_days       SMALLINT,
    games_last_7d   SMALLINT,

    PRIMARY KEY (player_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_mlb_avg_batting_player_date
    ON mlb_player_average_batting (player_id, game_date DESC);

ALTER TABLE mlb_player_average_batting ENABLE ROW LEVEL SECURITY;


-- ============================================================================
-- PITCHING AVERAGES
-- ============================================================================

CREATE TABLE IF NOT EXISTS mlb_player_average_pitching (
    -- Identity
    player_id       INTEGER NOT NULL,
    game_id         INTEGER NOT NULL,
    game_date       DATE NOT NULL,
    season          INTEGER NOT NULL,
    team_id         INTEGER,

    -- 8 pitching stats x 3 windows (L3/L5/SZN) = 24 columns
    avg_ip_l3              NUMERIC, avg_ip_l5              NUMERIC, avg_ip_szn              NUMERIC,
    avg_h_allowed_l3       NUMERIC, avg_h_allowed_l5       NUMERIC, avg_h_allowed_szn       NUMERIC,
    avg_r_allowed_l3       NUMERIC, avg_r_allowed_l5       NUMERIC, avg_r_allowed_szn       NUMERIC,
    avg_er_l3              NUMERIC, avg_er_l5              NUMERIC, avg_er_szn              NUMERIC,
    avg_bb_l3              NUMERIC, avg_bb_l5              NUMERIC, avg_bb_szn              NUMERIC,
    avg_so_l3              NUMERIC, avg_so_l5              NUMERIC, avg_so_szn              NUMERIC,
    avg_hr_allowed_l3      NUMERIC, avg_hr_allowed_l5      NUMERIC, avg_hr_allowed_szn      NUMERIC,
    avg_pitches_thrown_l3   NUMERIC, avg_pitches_thrown_l5   NUMERIC, avg_pitches_thrown_szn   NUMERIC,

    -- 4 derived rate stats at L5 (from rolling sums)
    avg_era_l5         NUMERIC,
    avg_whip_l5        NUMERIC,
    avg_k_per_9_l5     NUMERIC,
    avg_bb_per_9_l5    NUMERIC,

    -- 2 std devs
    std_so_l3          NUMERIC,
    std_er_l3          NUMERIC,

    -- Context
    game_number            INTEGER,
    days_rest              SMALLINT,
    pitch_count_last_start INTEGER,
    starts_l3              SMALLINT,
    starts_l5              SMALLINT,
    starts_szn             SMALLINT,

    PRIMARY KEY (player_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_mlb_avg_pitching_player_date
    ON mlb_player_average_pitching (player_id, game_date DESC);

ALTER TABLE mlb_player_average_pitching ENABLE ROW LEVEL SECURITY;
