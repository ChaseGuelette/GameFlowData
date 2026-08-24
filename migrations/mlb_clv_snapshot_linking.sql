-- Migration 031: Link dense MLB CLV snapshots to GameFlow ids
-- Purpose: make public.mlb_player_props_clv_snapshots directly usable by CLV
-- diagnostics that match on GameFlow game_id/player_id.

ALTER TABLE public.mlb_player_props_clv_snapshots
  ADD COLUMN IF NOT EXISTS game_id INTEGER,
  ADD COLUMN IF NOT EXISTS linked_player_name TEXT,
  ADD COLUMN IF NOT EXISTS game_link_method TEXT,
  ADD COLUMN IF NOT EXISTS player_link_method TEXT,
  ADD COLUMN IF NOT EXISTS linked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_mlb_clv_snap_game_player_market_time
  ON public.mlb_player_props_clv_snapshots (game_id, player_id, market_key, snapshot_time);

CREATE INDEX IF NOT EXISTS idx_mlb_clv_snap_unlinked_game
  ON public.mlb_player_props_clv_snapshots (commence_time, home_team, away_team)
  WHERE game_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_mlb_clv_snap_unlinked_player
  ON public.mlb_player_props_clv_snapshots (game_id, api_player_name)
  WHERE player_id IS NULL;

COMMENT ON COLUMN public.mlb_player_props_clv_snapshots.game_id IS
  'GameFlow MLB game_id linked from mlb_game_schedule for CLV matching.';

COMMENT ON COLUMN public.mlb_player_props_clv_snapshots.player_id IS
  'GameFlow MLB player_id linked from game participation/stats/name tables for CLV matching.';
