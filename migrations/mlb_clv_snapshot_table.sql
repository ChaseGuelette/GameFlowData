-- Migration 030: MLB CLV-focused player prop snapshots
-- Purpose: store dense, targeted Odds API snapshots for CLV/timing validation
-- without increasing the already-large mlb_raw_player_props table.
--
-- This table is designed for research/backtest validation and future CLV
-- scraper output. It intentionally preserves request metadata so downstream
-- diagnostics can distinguish close snapshots, fixed decision snapshots, and
-- selected-quote/timing horizons.

CREATE TABLE IF NOT EXISTS public.mlb_player_props_clv_snapshots (
  id BIGSERIAL PRIMARY KEY,

  -- Odds API / game identity
  api_game_id TEXT NOT NULL,
  odds_api_event_id TEXT,
  player_id INTEGER,
  api_player_name TEXT,

  -- Quote identity
  bookmaker TEXT NOT NULL,
  bookmaker_name TEXT,
  market_key TEXT NOT NULL,
  outcome_label TEXT NOT NULL,
  line NUMERIC,
  odds_american INTEGER,

  -- Game metadata
  commence_time TIMESTAMPTZ,
  home_team TEXT,
  away_team TEXT,

  -- Snapshot / update timestamps
  snapshot_time TIMESTAMPTZ NOT NULL,
  requested_snapshot_time TIMESTAMPTZ NOT NULL,
  market_last_update TIMESTAMPTZ,
  bookmaker_last_update TIMESTAMPTZ,

  -- Why this row was requested
  scrape_reason TEXT NOT NULL,
  target_offset_minutes INTEGER,

  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT mlb_clv_snapshot_reason_check CHECK (
    scrape_reason IN (
      'close_t_minus_60',
      'close_t_minus_30',
      'close_t_minus_15',
      'close_t_minus_5',
      'selected_quote',
      'selected_plus_15',
      'selected_plus_30',
      'selected_plus_60',
      'fixed_decision_0930',
      'fixed_decision_1030',
      'fixed_decision_1130',
      'fixed_decision_1230',
      'fixed_decision_1330',
      'fixed_decision_1530',
      'fixed_decision_1730',
      'manual'
    )
  )
);

-- Idempotency: avoid exact duplicate quote rows for repeated scrape attempts.
-- Multiple lines/prices at the same snapshot remain representable because line
-- and odds_american are part of the key.
CREATE UNIQUE INDEX IF NOT EXISTS uq_mlb_clv_snapshot_quote
  ON public.mlb_player_props_clv_snapshots (
    api_game_id,
    bookmaker,
    market_key,
    COALESCE(api_player_name, ''),
    outcome_label,
    COALESCE(line, -999999),
    COALESCE(odds_american, -999999),
    snapshot_time
  );

-- Narrow query support for CLV matching and coverage audits. The table starts
-- empty, so regular CREATE INDEX is safe here; do not copy this pattern to huge
-- existing raw prop tables without CONCURRENTLY planning.
CREATE INDEX IF NOT EXISTS idx_mlb_clv_snap_market_game_time
  ON public.mlb_player_props_clv_snapshots (market_key, api_game_id, snapshot_time);

CREATE INDEX IF NOT EXISTS idx_mlb_clv_snap_commence
  ON public.mlb_player_props_clv_snapshots (commence_time);

CREATE INDEX IF NOT EXISTS idx_mlb_clv_snap_book_market_player_time
  ON public.mlb_player_props_clv_snapshots (bookmaker, market_key, api_player_name, snapshot_time);

CREATE INDEX IF NOT EXISTS idx_mlb_clv_snap_reason_time
  ON public.mlb_player_props_clv_snapshots (scrape_reason, requested_snapshot_time);

COMMENT ON TABLE public.mlb_player_props_clv_snapshots IS
  'Dense targeted MLB player-prop snapshots for CLV/timing validation; kept separate from mlb_raw_player_props to avoid growing production raw odds table.';
