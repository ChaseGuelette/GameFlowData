-- User DFS Entries & Legs
-- Run in Supabase SQL Editor (NOT via apply_migration)
-- Creates tables for tracking user DFS parlay entries

-- ─── Entry-level table (one row per parlay slip) ────────────────────────────
CREATE TABLE IF NOT EXISTS user_dfs_entries (
    id             bigserial PRIMARY KEY,
    user_id        uuid NOT NULL REFERENCES auth.users(id),
    entry_date     date NOT NULL,
    slip_type      text NOT NULL,
    platform       text NOT NULL,
    num_legs       smallint NOT NULL,
    stake          numeric(10,2) NOT NULL,
    combined_prob  numeric(8,6) NOT NULL,
    kelly_fraction_used numeric(6,4) NOT NULL,
    payout_multiplier   numeric(6,2) NOT NULL,
    edge_mode      text NOT NULL,
    status         text NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','won','lost','push','cancelled')),
    legs_won       smallint NOT NULL DEFAULT 0,
    legs_lost      smallint NOT NULL DEFAULT 0,
    legs_push      smallint NOT NULL DEFAULT 0,
    legs_cancelled smallint NOT NULL DEFAULT 0,
    pnl            numeric(10,2) NOT NULL DEFAULT 0,
    placed_at      timestamptz NOT NULL DEFAULT now(),
    resolved_at    timestamptz,

    UNIQUE (user_id, entry_date, slip_type, placed_at)
);

CREATE INDEX IF NOT EXISTS idx_user_dfs_entries_user_date
    ON user_dfs_entries (user_id, entry_date);

CREATE INDEX IF NOT EXISTS idx_user_dfs_entries_status
    ON user_dfs_entries (status);

-- ─── Leg-level table (one row per leg in a parlay) ──────────────────────────
CREATE TABLE IF NOT EXISTS user_dfs_legs (
    id             bigserial PRIMARY KEY,
    entry_id       bigint NOT NULL REFERENCES user_dfs_entries(id) ON DELETE CASCADE,
    player_id      integer NOT NULL,
    player_name    text NOT NULL,
    game_id        text NOT NULL,
    stat_type      text NOT NULL,
    line           numeric(6,1) NOT NULL,
    direction      text NOT NULL CHECK (direction IN ('over','under')),
    dfs_bookmaker  text NOT NULL,
    model_prob     numeric(8,6),
    market_prob    numeric(8,6),
    edge           numeric(8,6) NOT NULL DEFAULT 0,
    status         text NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','won','lost','push','cancelled')),
    actual_value   numeric(8,2),

    UNIQUE (entry_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_user_dfs_legs_entry
    ON user_dfs_legs (entry_id);

-- ─── RLS Policies ───────────────────────────────────────────────────────────
ALTER TABLE user_dfs_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_dfs_legs ENABLE ROW LEVEL SECURITY;

-- Entries: users can read their own
CREATE POLICY "Users can view own DFS entries"
    ON user_dfs_entries FOR SELECT
    USING (auth.uid() = user_id);

-- Entries: users can insert their own
CREATE POLICY "Users can insert own DFS entries"
    ON user_dfs_entries FOR INSERT
    WITH CHECK (auth.uid() = user_id);

-- Entries: users can delete only pending entries
CREATE POLICY "Users can delete pending DFS entries"
    ON user_dfs_entries FOR DELETE
    USING (auth.uid() = user_id AND status = 'pending');

-- Legs: users can read via entry ownership
CREATE POLICY "Users can view legs of own entries"
    ON user_dfs_legs FOR SELECT
    USING (EXISTS (
        SELECT 1 FROM user_dfs_entries
        WHERE id = user_dfs_legs.entry_id AND user_id = auth.uid()
    ));

-- Legs: users can insert via entry ownership
CREATE POLICY "Users can insert legs for own entries"
    ON user_dfs_legs FOR INSERT
    WITH CHECK (EXISTS (
        SELECT 1 FROM user_dfs_entries
        WHERE id = user_dfs_legs.entry_id AND user_id = auth.uid()
    ));
