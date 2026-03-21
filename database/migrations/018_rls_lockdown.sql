-- Migration 018: Enable RLS on all unprotected tables
-- Blocks all client (anon/authenticated) access by default.
-- Python backend uses postgres role which bypasses RLS.

-- ============================================================
-- 1. Enable RLS on all 18 unprotected tables
-- ============================================================

-- DFS paper trading tables
ALTER TABLE dfs_paper_daily_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE dfs_paper_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE dfs_paper_legs ENABLE ROW LEVEL SECURITY;

-- Internal tables
ALTER TABLE job_executions ENABLE ROW LEVEL SECURITY;

-- Injury data
ALTER TABLE rapidapi_injuries ENABLE ROW LEVEL SECURITY;

-- MLB tables
ALTER TABLE mlb_daily_predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_daily_prediction_samples ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_game_schedule ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_paper_bets ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_paper_trading_daily_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_park_factors ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_player_game_statcast_batting ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_player_game_statcast_pitching ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_player_game_stats_batting ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_player_game_stats_pitching ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_player_season_advanced ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_players ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_raw_game_lines ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_raw_player_props ENABLE ROW LEVEL SECURITY;
ALTER TABLE mlb_teams ENABLE ROW LEVEL SECURITY;

-- ============================================================
-- 2. Add SELECT policies for tables the dashboard reads
-- ============================================================

-- DFS performance page needs these two tables
CREATE POLICY "auth_read_dfs_daily_log" ON dfs_paper_daily_log
  FOR SELECT TO authenticated USING (true);

CREATE POLICY "auth_read_dfs_entries" ON dfs_paper_entries
  FOR SELECT TO authenticated USING (true);

-- Dashboard injury filter needs this table
CREATE POLICY "auth_read_injuries" ON rapidapi_injuries
  FOR SELECT TO authenticated USING (true);
