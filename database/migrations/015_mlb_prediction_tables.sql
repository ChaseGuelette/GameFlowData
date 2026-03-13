-- MLB Prediction & Paper Trading Tables
-- Mirrors the NBA prediction/paper-trading schema for MLB.

-- 1. MLB Daily Predictions
CREATE TABLE IF NOT EXISTS mlb_daily_predictions (
    id BIGSERIAL,
    prediction_date DATE NOT NULL,
    player_id INTEGER NOT NULL,
    player_name TEXT,
    game_id INTEGER NOT NULL,
    team_id INTEGER,
    opponent_id INTEGER,
    stat TEXT NOT NULL,
    model_type TEXT,          -- 'quantile', 'negbin', 'binary'
    pred_mean NUMERIC,
    pred_std NUMERIC,
    pred_median NUMERIC,
    pred_q10 NUMERIC,
    pred_q25 NUMERIC,
    pred_q50 NUMERIC,
    pred_q75 NUMERIC,
    pred_q90 NUMERIC,
    pred_prob NUMERIC,        -- for binary stats (e.g. P(HR >= 1))
    line NUMERIC,
    over_odds NUMERIC,
    under_odds NUMERIC,
    over_prob NUMERIC,
    under_prob NUMERIC,
    implied_over NUMERIC,
    implied_under NUMERIC,
    over_edge NUMERIC,
    under_edge NUMERIC,
    game_time TIMESTAMPTZ,
    bookmaker TEXT,
    -- Feature columns for dashboard insights
    feat_days_rest INTEGER,
    feat_lineup_position INTEGER,
    feat_park_factor NUMERIC,
    feat_player_avg_stat_l5 NUMERIC,
    feat_player_avg_stat_szn NUMERIC,
    feat_opp_abbrev TEXT,
    -- Black-Litterman blended values
    bl_over_prob NUMERIC,
    bl_under_prob NUMERIC,
    bl_over_edge NUMERIC,
    bl_under_edge NUMERIC,
    bl_confidence NUMERIC,
    is_recommended BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (prediction_date, player_id, game_id, stat)
);

CREATE INDEX IF NOT EXISTS idx_mlb_daily_predictions_date
    ON mlb_daily_predictions (prediction_date);
CREATE INDEX IF NOT EXISTS idx_mlb_daily_predictions_player
    ON mlb_daily_predictions (player_id, prediction_date);

-- 2. MLB Daily Prediction Samples (gzip MC samples as BYTEA)
CREATE TABLE IF NOT EXISTS mlb_daily_prediction_samples (
    id BIGSERIAL,
    prediction_date DATE NOT NULL,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    stat TEXT NOT NULL,
    n_samples INTEGER NOT NULL,
    samples_gz BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (prediction_date, player_id, game_id, stat)
);

-- 3. MLB Paper Bets
CREATE TABLE IF NOT EXISTS mlb_paper_bets (
    id BIGSERIAL PRIMARY KEY,
    prediction_id BIGINT,
    game_date DATE NOT NULL,
    player_id INTEGER NOT NULL,
    player_name TEXT,
    stat_type TEXT NOT NULL,
    line NUMERIC,
    bet_direction TEXT NOT NULL,     -- 'over' or 'under'
    odds_at_bet NUMERIC,
    implied_prob NUMERIC,
    model_prob NUMERIC,
    edge NUMERIC,
    stake NUMERIC,
    kelly_fraction NUMERIC,
    status TEXT DEFAULT 'pending',   -- pending, won, lost, push, cancelled
    actual_value NUMERIC,
    pnl NUMERIC,
    placed_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    UNIQUE (game_date, player_id, stat_type, bet_direction)
);

CREATE INDEX IF NOT EXISTS idx_mlb_paper_bets_date
    ON mlb_paper_bets (game_date);
CREATE INDEX IF NOT EXISTS idx_mlb_paper_bets_status
    ON mlb_paper_bets (status);

-- 4. MLB Paper Trading Daily Log
CREATE TABLE IF NOT EXISTS mlb_paper_trading_daily_log (
    id BIGSERIAL PRIMARY KEY,
    game_date DATE NOT NULL UNIQUE,
    total_bets INTEGER DEFAULT 0,
    bets_won INTEGER DEFAULT 0,
    bets_lost INTEGER DEFAULT 0,
    bets_push INTEGER DEFAULT 0,
    bets_pending INTEGER DEFAULT 0,
    total_staked NUMERIC DEFAULT 0,
    total_pnl NUMERIC DEFAULT 0,
    roi_pct NUMERIC DEFAULT 0,
    cumulative_pnl NUMERIC DEFAULT 0,
    bankroll_after NUMERIC DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
