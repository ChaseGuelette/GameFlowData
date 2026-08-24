-- Add pitcher short-outing risk rolling feature.
-- Populated by src/processing/mlb/mlb_populate_averages*.py using prior-game IP only.

ALTER TABLE mlb_player_average_pitching
ADD COLUMN IF NOT EXISTS min_ip_l5 FLOAT;
