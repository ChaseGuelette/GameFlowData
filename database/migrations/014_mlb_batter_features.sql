-- Migration 014: MLB Batter Feature Indexes
-- Adds indexes for opposing starter identification and platoon lookups,
-- and ensures bats/throws columns exist on mlb_players.

-- Ensure bats/throws columns exist (idempotent)
ALTER TABLE mlb_players ADD COLUMN IF NOT EXISTS bats VARCHAR(1);
ALTER TABLE mlb_players ADD COLUMN IF NOT EXISTS throws VARCHAR(1);

-- Index for opposing starter identification per game
CREATE INDEX IF NOT EXISTS idx_mlb_pitching_starter_game
    ON mlb_player_game_stats_pitching (game_id, team_id, is_starter)
    WHERE is_starter = TRUE AND did_not_play = FALSE;

-- Index for platoon lookups (batter games by date)
CREATE INDEX IF NOT EXISTS idx_mlb_batting_player_date
    ON mlb_player_game_stats_batting (player_id, game_date DESC);
