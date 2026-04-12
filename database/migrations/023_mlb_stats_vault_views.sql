-- Migration 023: MLB Stats Vault Views
-- Creates mlb_batters_latest and mlb_pitchers_latest views for the Data Vault dashboard.
-- Also adds public-read RLS policies on the underlying tables.

-- ============================================================
-- 1. RLS policies on underlying tables (dashboard reads these)
-- ============================================================

CREATE POLICY "auth_read_mlb_avg_batting" ON mlb_player_average_batting
  FOR SELECT TO authenticated USING (true);

CREATE POLICY "auth_read_mlb_avg_pitching" ON mlb_player_average_pitching
  FOR SELECT TO authenticated USING (true);

CREATE POLICY "auth_read_mlb_players" ON mlb_players
  FOR SELECT TO authenticated USING (true);

CREATE POLICY "auth_read_mlb_teams" ON mlb_teams
  FOR SELECT TO authenticated USING (true);

-- ============================================================
-- 2. Batter rolling-average view (latest row per player)
-- ============================================================

CREATE OR REPLACE VIEW mlb_batters_latest AS
SELECT DISTINCT ON (b.player_id)
    b.player_id,
    p.player_name,
    t.team_abbreviation AS team_abbrev,
    p.bats,
    b.game_date,
    -- Window game counts
    b.games_l5, b.games_l10, b.games_l20, b.games_szn,
    -- Hits
    b.avg_h_l5, b.avg_h_l10, b.avg_h_l20, b.avg_h_szn,
    -- RBI
    b.avg_rbi_l5, b.avg_rbi_l10, b.avg_rbi_l20, b.avg_rbi_szn,
    -- Runs
    b.avg_r_l5, b.avg_r_l10, b.avg_r_l20, b.avg_r_szn,
    -- Home runs
    b.avg_hr_l5, b.avg_hr_l10, b.avg_hr_l20, b.avg_hr_szn,
    -- Stolen bases
    b.avg_sb_l5, b.avg_sb_l10, b.avg_sb_l20, b.avg_sb_szn,
    -- Walks
    b.avg_bb_l5, b.avg_bb_l10, b.avg_bb_l20, b.avg_bb_szn,
    -- Strikeouts
    b.avg_so_l5, b.avg_so_l10, b.avg_so_l20, b.avg_so_szn,
    -- At bats
    b.avg_ab_l5, b.avg_ab_l10, b.avg_ab_l20, b.avg_ab_szn,
    -- Total bases
    b.avg_tb_l5, b.avg_tb_l10, b.avg_tb_l20, b.avg_tb_szn,
    -- Doubles / triples / PA
    b.avg_doubles_l5, b.avg_doubles_l10, b.avg_doubles_l20, b.avg_doubles_szn,
    b.avg_triples_l5, b.avg_triples_l10, b.avg_triples_l20, b.avg_triples_szn,
    b.avg_pa_l5, b.avg_pa_l10, b.avg_pa_l20, b.avg_pa_szn,
    -- Rate stats (windowless — computed from L10 rolling sums)
    b.avg_batting_avg_l10,
    b.avg_obp_l10,
    b.avg_slg_l10,
    b.avg_ops_l10,
    -- Std devs (L5)
    b.std_h_l5,
    b.std_hr_l5,
    b.std_rbi_l5,
    b.std_r_l5,
    b.std_so_l5,
    b.std_sb_l5,
    b.std_tb_l5,
    -- Context
    b.rest_days,
    b.games_last_7d
FROM mlb_player_average_batting b
JOIN mlb_players p ON p.player_id = b.player_id
LEFT JOIN mlb_teams t ON t.team_id = b.team_id
ORDER BY b.player_id, b.game_date DESC;

-- ============================================================
-- 3. Pitcher rolling-average view (latest row per player)
-- ============================================================

CREATE OR REPLACE VIEW mlb_pitchers_latest AS
SELECT DISTINCT ON (pit.player_id)
    pit.player_id,
    p.player_name,
    t.team_abbreviation AS team_abbrev,
    p.throws,
    pit.game_date,
    -- IP
    pit.avg_ip_l3, pit.avg_ip_l5, pit.avg_ip_szn,
    -- Strikeouts
    pit.avg_so_l3, pit.avg_so_l5, pit.avg_so_szn,
    -- Hits allowed
    pit.avg_h_allowed_l3, pit.avg_h_allowed_l5, pit.avg_h_allowed_szn,
    -- Runs allowed
    pit.avg_r_allowed_l3, pit.avg_r_allowed_l5, pit.avg_r_allowed_szn,
    -- Earned runs
    pit.avg_er_l3, pit.avg_er_l5, pit.avg_er_szn,
    -- Walks
    pit.avg_bb_l3, pit.avg_bb_l5, pit.avg_bb_szn,
    -- HR allowed
    pit.avg_hr_allowed_l3, pit.avg_hr_allowed_l5, pit.avg_hr_allowed_szn,
    -- Pitches thrown
    pit.avg_pitches_thrown_l3, pit.avg_pitches_thrown_l5, pit.avg_pitches_thrown_szn,
    -- Rate stats (windowless — computed from L5 rolling sums)
    pit.avg_era_l5,
    pit.avg_whip_l5,
    pit.avg_k_per_9_l5,
    pit.avg_bb_per_9_l5,
    -- Std devs (L3)
    pit.std_so_l3,
    pit.std_er_l3,
    -- Context
    pit.days_rest,
    pit.pitch_count_last_start,
    pit.starts_l3, pit.starts_l5, pit.starts_szn,
    pit.game_number
FROM mlb_player_average_pitching pit
JOIN mlb_players p ON p.player_id = pit.player_id
LEFT JOIN mlb_teams t ON t.team_id = pit.team_id
ORDER BY pit.player_id, pit.game_date DESC;
