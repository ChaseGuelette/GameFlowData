-- Migration 027: Add is_paper_trade column to user_bets
-- Allows users to paper-trade any pick without real money;
-- bets auto-resolve against actual game results.

ALTER TABLE user_bets ADD COLUMN IF NOT EXISTS is_paper_trade boolean NOT NULL DEFAULT false;

-- Update unique constraint to allow both real + paper bets on same player/stat/direction
ALTER TABLE user_bets DROP CONSTRAINT IF EXISTS user_bets_unique_bet;
ALTER TABLE user_bets ADD CONSTRAINT user_bets_unique_bet
  UNIQUE (user_id, game_date, player_name, stat_type, bet_direction, is_paper_trade);
