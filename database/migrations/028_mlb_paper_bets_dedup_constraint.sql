-- Migration 028: Fix mlb_paper_bets unique constraint to prevent opposing-direction bets
--
-- Problem: the old constraint (game_date, player_id, stat_type, bet_direction) allowed
-- one "over" AND one "under" bet for the same player+stat on the same day. This happens
-- when different bookmakers produce opposing is_recommended=True rows in mlb_daily_predictions,
-- causing the paper trader to log a flat net position (betting both sides) across inference runs.
--
-- Fix: drop bet_direction from the constraint so only ONE bet per (player, stat, day) is kept.
-- Subsequent inference runs update the existing row (refreshing direction + odds) rather than
-- inserting a new opposing row.

-- Step 1: Clean up existing opposing-direction pairs — keep only the highest-|edge| bet
-- per (game_date, player_id, stat_type). Delete the lower-edge duplicate.
DELETE FROM mlb_paper_bets a
USING mlb_paper_bets b
WHERE a.game_date = b.game_date
  AND a.player_id = b.player_id
  AND a.stat_type = b.stat_type
  AND a.bet_direction != b.bet_direction
  AND abs(a.edge) < abs(b.edge);

-- Step 2: If any exact ties remain (same edge, different direction), keep lower id
DELETE FROM mlb_paper_bets a
USING mlb_paper_bets b
WHERE a.game_date = b.game_date
  AND a.player_id = b.player_id
  AND a.stat_type = b.stat_type
  AND a.id > b.id;

-- Step 3: Drop old constraint (includes bet_direction)
ALTER TABLE mlb_paper_bets
  DROP CONSTRAINT IF EXISTS mlb_paper_bets_game_date_player_id_stat_type_bet_direction_key;

-- Also try common alternative constraint names
ALTER TABLE mlb_paper_bets
  DROP CONSTRAINT IF EXISTS mlb_paper_bets_unique;

ALTER TABLE mlb_paper_bets
  DROP CONSTRAINT IF EXISTS uq_mlb_paper_bets_player_stat_day;

-- Step 4: Add new constraint — one bet per (player, stat, day)
ALTER TABLE mlb_paper_bets
  ADD CONSTRAINT mlb_paper_bets_unique_player_stat_day
  UNIQUE (game_date, player_id, stat_type);
