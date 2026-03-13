-- Migration 016: Add probable pitcher columns to mlb_game_schedule
-- These columns are populated by the stats scraper using hydrate=probablePitcher
-- and consumed by the daily runner and backtest harness for pitcher identification.

ALTER TABLE mlb_game_schedule
    ADD COLUMN IF NOT EXISTS probable_pitcher_home_id INTEGER,
    ADD COLUMN IF NOT EXISTS probable_pitcher_away_id INTEGER;

-- Index for daily runner queries that filter by probable pitchers
CREATE INDEX IF NOT EXISTS idx_mlb_schedule_pitchers
    ON mlb_game_schedule (game_date, probable_pitcher_home_id, probable_pitcher_away_id)
    WHERE status != 'Cancelled';
