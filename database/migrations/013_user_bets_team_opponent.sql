-- Add team/opponent columns to user_bets for matchup display in history
ALTER TABLE user_bets ADD COLUMN IF NOT EXISTS team_abbrev text;
ALTER TABLE user_bets ADD COLUMN IF NOT EXISTS opponent_abbrev text;
