-- Migration 003: MLB Statcast Rolling Average Tables
-- Creates mlb_player_average_statcast_batting and mlb_player_average_statcast_pitching
-- for model consumption (shift(1) pre-game rolling averages of Statcast metrics).

-- ============================================================================
-- STATCAST BATTING AVERAGES
-- ============================================================================

CREATE TABLE IF NOT EXISTS mlb_player_average_statcast_batting (
    -- Identity
    player_id       INTEGER NOT NULL,
    game_date       DATE NOT NULL,
    season          INTEGER NOT NULL,

    -- Window counts
    game_number     INTEGER,

    -- Contact quality (L5/L10/L20/SZN)
    avg_avg_exit_velocity_l5    NUMERIC, avg_avg_exit_velocity_l10   NUMERIC, avg_avg_exit_velocity_l20   NUMERIC, avg_avg_exit_velocity_szn   NUMERIC,
    avg_max_exit_velocity_l5    NUMERIC, avg_max_exit_velocity_l10   NUMERIC, avg_max_exit_velocity_l20   NUMERIC, avg_max_exit_velocity_szn   NUMERIC,
    avg_avg_launch_angle_l5     NUMERIC, avg_avg_launch_angle_l10    NUMERIC, avg_avg_launch_angle_l20    NUMERIC, avg_avg_launch_angle_szn    NUMERIC,
    avg_barrel_pct_l5           NUMERIC, avg_barrel_pct_l10          NUMERIC, avg_barrel_pct_l20          NUMERIC, avg_barrel_pct_szn          NUMERIC,
    avg_hard_hit_pct_l5         NUMERIC, avg_hard_hit_pct_l10        NUMERIC, avg_hard_hit_pct_l20        NUMERIC, avg_hard_hit_pct_szn        NUMERIC,
    avg_sweet_spot_pct_l5       NUMERIC, avg_sweet_spot_pct_l10      NUMERIC, avg_sweet_spot_pct_l20      NUMERIC, avg_sweet_spot_pct_szn      NUMERIC,

    -- Expected stats (L5/L10/L20/SZN)
    avg_xba_l5      NUMERIC, avg_xba_l10     NUMERIC, avg_xba_l20     NUMERIC, avg_xba_szn     NUMERIC,
    avg_xslg_l5     NUMERIC, avg_xslg_l10    NUMERIC, avg_xslg_l20    NUMERIC, avg_xslg_szn    NUMERIC,
    avg_xwoba_l5    NUMERIC, avg_xwoba_l10   NUMERIC, avg_xwoba_l20   NUMERIC, avg_xwoba_szn   NUMERIC,
    avg_woba_l5     NUMERIC, avg_woba_l10    NUMERIC, avg_woba_l20    NUMERIC, avg_woba_szn    NUMERIC,

    -- Batted ball distribution (L10/SZN only — less volatile)
    avg_gb_pct_l10      NUMERIC, avg_gb_pct_szn      NUMERIC,
    avg_fb_pct_l10      NUMERIC, avg_fb_pct_szn      NUMERIC,
    avg_ld_pct_l10      NUMERIC, avg_ld_pct_szn      NUMERIC,
    avg_popup_pct_l10   NUMERIC, avg_popup_pct_szn   NUMERIC,

    -- Spray direction (L10/SZN only)
    avg_pull_pct_l10    NUMERIC, avg_pull_pct_szn    NUMERIC,
    avg_center_pct_l10  NUMERIC, avg_center_pct_szn  NUMERIC,
    avg_oppo_pct_l10    NUMERIC, avg_oppo_pct_szn    NUMERIC,

    -- Plate discipline (L5/L10/SZN)
    avg_zone_pct_l5     NUMERIC, avg_zone_pct_l10    NUMERIC, avg_zone_pct_szn    NUMERIC,
    avg_chase_pct_l5    NUMERIC, avg_chase_pct_l10   NUMERIC, avg_chase_pct_szn   NUMERIC,
    avg_whiff_pct_l5    NUMERIC, avg_whiff_pct_l10   NUMERIC, avg_whiff_pct_szn   NUMERIC,

    -- Volume (L5/L10/SZN)
    avg_batted_balls_l5     NUMERIC, avg_batted_balls_l10    NUMERIC, avg_batted_balls_szn    NUMERIC,
    avg_total_pitches_seen_l5 NUMERIC, avg_total_pitches_seen_l10 NUMERIC, avg_total_pitches_seen_szn NUMERIC,

    -- Std devs at L5 (key volatile stats)
    std_avg_exit_velocity_l5    NUMERIC,
    std_barrel_pct_l5           NUMERIC,
    std_xwoba_l5                NUMERIC,
    std_hard_hit_pct_l5         NUMERIC,

    PRIMARY KEY (player_id, game_date)
);

CREATE INDEX IF NOT EXISTS idx_mlb_avg_statcast_bat_date ON mlb_player_average_statcast_batting (game_date);
CREATE INDEX IF NOT EXISTS idx_mlb_avg_statcast_bat_season ON mlb_player_average_statcast_batting (season);

ALTER TABLE mlb_player_average_statcast_batting ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON mlb_player_average_statcast_batting FOR SELECT USING (true);

-- ============================================================================
-- STATCAST PITCHING AVERAGES
-- ============================================================================

CREATE TABLE IF NOT EXISTS mlb_player_average_statcast_pitching (
    -- Identity
    player_id       INTEGER NOT NULL,
    game_date       DATE NOT NULL,
    season          INTEGER NOT NULL,

    -- Window counts
    game_number     INTEGER,

    -- Contact quality against (L3/L5/SZN)
    avg_avg_exit_velocity_against_l3  NUMERIC, avg_avg_exit_velocity_against_l5  NUMERIC, avg_avg_exit_velocity_against_szn NUMERIC,
    avg_max_exit_velocity_against_l3  NUMERIC, avg_max_exit_velocity_against_l5  NUMERIC, avg_max_exit_velocity_against_szn NUMERIC,
    avg_barrel_pct_against_l3         NUMERIC, avg_barrel_pct_against_l5         NUMERIC, avg_barrel_pct_against_szn        NUMERIC,
    avg_hard_hit_pct_against_l3       NUMERIC, avg_hard_hit_pct_against_l5       NUMERIC, avg_hard_hit_pct_against_szn      NUMERIC,

    -- Expected stats against (L3/L5/SZN)
    avg_xba_against_l3    NUMERIC, avg_xba_against_l5    NUMERIC, avg_xba_against_szn   NUMERIC,
    avg_xslg_against_l3   NUMERIC, avg_xslg_against_l5   NUMERIC, avg_xslg_against_szn  NUMERIC,
    avg_xwoba_against_l3  NUMERIC, avg_xwoba_against_l5  NUMERIC, avg_xwoba_against_szn NUMERIC,
    avg_xera_l3           NUMERIC, avg_xera_l5           NUMERIC, avg_xera_szn          NUMERIC,

    -- Velocity and spin (L3/L5/SZN)
    avg_avg_fastball_velo_l3    NUMERIC, avg_avg_fastball_velo_l5    NUMERIC, avg_avg_fastball_velo_szn   NUMERIC,
    avg_max_fastball_velo_l3    NUMERIC, avg_max_fastball_velo_l5    NUMERIC, avg_max_fastball_velo_szn   NUMERIC,
    avg_avg_fastball_spin_l3    NUMERIC, avg_avg_fastball_spin_l5    NUMERIC, avg_avg_fastball_spin_szn   NUMERIC,
    avg_avg_breaking_spin_l3    NUMERIC, avg_avg_breaking_spin_l5    NUMERIC, avg_avg_breaking_spin_szn   NUMERIC,

    -- Pitch mix (L5/SZN only — less volatile)
    avg_fastball_pct_l5     NUMERIC, avg_fastball_pct_szn    NUMERIC,
    avg_breaking_pct_l5     NUMERIC, avg_breaking_pct_szn    NUMERIC,
    avg_offspeed_pct_l5     NUMERIC, avg_offspeed_pct_szn    NUMERIC,

    -- Batted ball distribution (L5/SZN)
    avg_gb_pct_l5       NUMERIC, avg_gb_pct_szn      NUMERIC,
    avg_fb_pct_l5       NUMERIC, avg_fb_pct_szn      NUMERIC,
    avg_ld_pct_l5       NUMERIC, avg_ld_pct_szn      NUMERIC,
    avg_popup_pct_l5    NUMERIC, avg_popup_pct_szn   NUMERIC,

    -- Plate discipline (L3/L5/SZN)
    avg_zone_pct_l3     NUMERIC, avg_zone_pct_l5     NUMERIC, avg_zone_pct_szn    NUMERIC,
    avg_chase_pct_l3    NUMERIC, avg_chase_pct_l5    NUMERIC, avg_chase_pct_szn   NUMERIC,
    avg_whiff_pct_l3    NUMERIC, avg_whiff_pct_l5    NUMERIC, avg_whiff_pct_szn   NUMERIC,
    avg_csw_pct_l3      NUMERIC, avg_csw_pct_l5      NUMERIC, avg_csw_pct_szn     NUMERIC,

    -- Volume (L3/L5/SZN)
    avg_total_pitches_l3    NUMERIC, avg_total_pitches_l5    NUMERIC, avg_total_pitches_szn   NUMERIC,

    -- Std devs at L3
    std_avg_exit_velocity_against_l3    NUMERIC,
    std_xera_l3                         NUMERIC,
    std_whiff_pct_l3                    NUMERIC,
    std_avg_fastball_velo_l3            NUMERIC,

    PRIMARY KEY (player_id, game_date)
);

CREATE INDEX IF NOT EXISTS idx_mlb_avg_statcast_pitch_date ON mlb_player_average_statcast_pitching (game_date);
CREATE INDEX IF NOT EXISTS idx_mlb_avg_statcast_pitch_season ON mlb_player_average_statcast_pitching (season);

ALTER TABLE mlb_player_average_statcast_pitching ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON mlb_player_average_statcast_pitching FOR SELECT USING (true);
