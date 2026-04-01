-- Kalshi Live Trading Tables
-- Run in Supabase SQL Editor (NOT via apply_migration)
-- No RLS needed — accessed by Python backend via postgres role

-- Table 1: Live order log
CREATE TABLE IF NOT EXISTS kalshi_live_orders (
    id bigserial PRIMARY KEY,
    game_date date NOT NULL,
    ticker text NOT NULL,
    sport text NOT NULL DEFAULT 'nba',
    player_id integer,
    player_name text,
    stat_type text NOT NULL,
    line numeric(6,1) NOT NULL,
    side text NOT NULL CHECK (side IN ('yes', 'no')),
    order_type text NOT NULL DEFAULT 'market',
    contracts integer NOT NULL,
    -- Kalshi API fields
    kalshi_order_id text,
    fill_price integer,              -- actual fill price in cents
    fill_count integer,              -- actual contracts filled
    total_cost numeric(10,2),        -- total $ spent
    fee_paid numeric(10,4),          -- actual fee charged
    -- Model fields
    model_prob numeric(8,6),
    kalshi_implied numeric(8,6),
    edge numeric(8,6),
    fee_adjusted_edge numeric(8,6),
    -- Resolution
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'filled', 'won', 'lost', 'cancelled')),
    actual_value numeric(8,2),
    pnl numeric(10,2) DEFAULT 0,
    placed_at timestamptz DEFAULT now(),
    filled_at timestamptz,
    resolved_at timestamptz,
    UNIQUE (game_date, ticker, side, placed_at)
);

CREATE INDEX idx_kalshi_live_orders_date ON kalshi_live_orders (game_date);
CREATE INDEX idx_kalshi_live_orders_status ON kalshi_live_orders (status);

-- Table 2: Daily trading summary
CREATE TABLE IF NOT EXISTS kalshi_live_trading_daily_log (
    game_date date PRIMARY KEY,
    total_trades integer DEFAULT 0,
    trades_won integer DEFAULT 0,
    trades_lost integer DEFAULT 0,
    trades_cancelled integer DEFAULT 0,
    trades_pending integer DEFAULT 0,
    total_cost numeric(10,2) DEFAULT 0,
    total_pnl numeric(10,2) DEFAULT 0,
    roi_pct numeric(8,2) DEFAULT 0,
    cumulative_pnl numeric(10,2) DEFAULT 0,
    balance_after numeric(10,2) DEFAULT 0,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

-- Table 3: Singleton config / circuit breaker state
CREATE TABLE IF NOT EXISTS kalshi_live_trading_config (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    starting_bankroll numeric(10,2) NOT NULL DEFAULT 100,
    is_halted boolean NOT NULL DEFAULT false,
    halt_reason text,
    halted_at timestamptz,
    daily_loss_reset_date date,
    streak_count integer DEFAULT 0,
    last_updated timestamptz DEFAULT now()
);

-- Insert default config row
INSERT INTO kalshi_live_trading_config (id, starting_bankroll)
VALUES (1, 100)
ON CONFLICT (id) DO NOTHING;
