-- DFS Paper Trading Tables
-- Run in Supabase SQL editor

-- 1. DFS Paper Entries (one row per slip)
CREATE TABLE IF NOT EXISTS dfs_paper_entries (
    id              bigserial PRIMARY KEY,
    entry_date      date NOT NULL,
    slip_type       text NOT NULL,          -- ud_3_standard, ud_5_standard, pp_5_flex, pp_6_flex
    platform        text NOT NULL,          -- underdog, prizepicks
    num_legs        integer NOT NULL,
    stake           numeric(10,2) NOT NULL DEFAULT 10.00,
    status          text NOT NULL DEFAULT 'pending',  -- pending/won/lost/partial/cancelled
    legs_won        integer NOT NULL DEFAULT 0,
    legs_lost       integer NOT NULL DEFAULT 0,
    legs_push       integer NOT NULL DEFAULT 0,
    legs_cancelled  integer NOT NULL DEFAULT 0,
    payout_multiplier numeric(10,4) DEFAULT 0,
    pnl             numeric(10,2) DEFAULT 0,
    avg_edge        numeric(8,5),
    min_edge        numeric(8,5),
    created_at      timestamptz NOT NULL DEFAULT now(),
    resolved_at     timestamptz,
    UNIQUE (entry_date, slip_type)
);

-- 2. DFS Paper Legs (individual picks within entries)
CREATE TABLE IF NOT EXISTS dfs_paper_legs (
    id              bigserial PRIMARY KEY,
    entry_id        bigint NOT NULL REFERENCES dfs_paper_entries(id) ON DELETE CASCADE,
    player_id       bigint NOT NULL,
    player_name     text NOT NULL,
    game_id         text,
    stat_type       text NOT NULL,          -- pts, reb, ast
    line            numeric(6,2) NOT NULL,
    direction       text NOT NULL,          -- over, under
    dfs_bookmaker   text NOT NULL,          -- prizepicks, underdog
    market_prob     numeric(8,5),
    market_books    integer DEFAULT 0,
    edge            numeric(8,5),
    status          text NOT NULL DEFAULT 'pending',  -- pending/won/lost/push/cancelled
    actual_value    numeric(8,2),
    UNIQUE (entry_id, player_id)
);

-- 3. DFS Paper Daily Log (daily aggregate tracking)
CREATE TABLE IF NOT EXISTS dfs_paper_daily_log (
    id              bigserial PRIMARY KEY,
    entry_date      date NOT NULL UNIQUE,
    entries_placed  integer NOT NULL DEFAULT 0,
    entries_won     integer NOT NULL DEFAULT 0,
    entries_lost    integer NOT NULL DEFAULT 0,
    entries_partial integer NOT NULL DEFAULT 0,
    total_staked    numeric(10,2) NOT NULL DEFAULT 0,
    total_pnl       numeric(10,2) NOT NULL DEFAULT 0,
    roi_pct         numeric(8,4) DEFAULT 0,
    cumulative_pnl  numeric(10,2) NOT NULL DEFAULT 0,
    bankroll_after  numeric(10,2) NOT NULL DEFAULT 100.00,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Indices for common queries
CREATE INDEX IF NOT EXISTS idx_dfs_entries_date_status ON dfs_paper_entries(entry_date, status);
CREATE INDEX IF NOT EXISTS idx_dfs_legs_entry_id ON dfs_paper_legs(entry_id);
CREATE INDEX IF NOT EXISTS idx_dfs_daily_log_date ON dfs_paper_daily_log(entry_date);
