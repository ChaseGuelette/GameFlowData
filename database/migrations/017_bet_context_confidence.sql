-- Migration 017: Add bet context snapshot and user confidence to user_bets
-- bet_context stores the full analysis snapshot (quantiles, features, insights, kelly, etc.)
-- user_confidence stores 1-5 star rating from user at bet time

ALTER TABLE user_bets ADD COLUMN IF NOT EXISTS bet_context jsonb;
ALTER TABLE user_bets ADD COLUMN IF NOT EXISTS user_confidence smallint;
ALTER TABLE user_bets ADD CONSTRAINT chk_user_confidence
  CHECK (user_confidence IS NULL OR (user_confidence >= 1 AND user_confidence <= 5));
