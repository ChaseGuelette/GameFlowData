-- 012_user_bet_tracking.sql
-- User bet tracking: profiles (preferences sync) + bets (cross-device taken bets)

-- ============================================================
-- user_profiles: Cross-device preferences (bankroll, kelly, state)
-- ============================================================
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id        uuid PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    user_state     text,
    bankroll       numeric(12,2) DEFAULT 1000,
    kelly_fraction numeric(5,4) DEFAULT 0.25,
    use_custom_kelly boolean DEFAULT false,
    created_at     timestamptz DEFAULT now(),
    updated_at     timestamptz DEFAULT now()
);

-- ============================================================
-- user_bets: Tracks bets users have "taken" via PropCard checkmark
-- ============================================================
CREATE TABLE IF NOT EXISTS user_bets (
    id             bigserial PRIMARY KEY,
    user_id        uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    prediction_id  bigint NOT NULL,
    game_date      date NOT NULL,
    player_id      bigint NOT NULL,
    player_name    text NOT NULL,
    stat_type      text NOT NULL,            -- 'pts', 'reb', 'ast'
    line           numeric(6,2) NOT NULL,
    bet_direction  text NOT NULL,            -- 'over', 'under'
    odds_at_bet    numeric(8,2),
    book_at_bet    text,
    model_prob     numeric(5,4),
    edge           numeric(5,4),
    stake          numeric(10,2),
    status         text NOT NULL DEFAULT 'pending',
    actual_value   numeric(8,2),
    pnl            numeric(10,2),
    placed_at      timestamptz DEFAULT now(),
    resolved_at    timestamptz,
    UNIQUE (user_id, game_date, player_id, stat_type)
);

CREATE INDEX IF NOT EXISTS idx_user_bets_user_date ON user_bets (user_id, game_date DESC);
CREATE INDEX IF NOT EXISTS idx_user_bets_pending ON user_bets (status, game_date) WHERE status = 'pending';

-- ============================================================
-- RLS Policies: Users can only access their own rows
-- ============================================================

-- user_profiles RLS
ALTER TABLE user_profiles ENABLE ROW LEVEL SECURITY;

CREATE POLICY user_profiles_select ON user_profiles
    FOR SELECT USING (auth.uid() = user_id);

CREATE POLICY user_profiles_insert ON user_profiles
    FOR INSERT WITH CHECK (auth.uid() = user_id);

CREATE POLICY user_profiles_update ON user_profiles
    FOR UPDATE USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY user_profiles_delete ON user_profiles
    FOR DELETE USING (auth.uid() = user_id);

-- user_bets RLS
ALTER TABLE user_bets ENABLE ROW LEVEL SECURITY;

CREATE POLICY user_bets_select ON user_bets
    FOR SELECT USING (auth.uid() = user_id);

CREATE POLICY user_bets_insert ON user_bets
    FOR INSERT WITH CHECK (auth.uid() = user_id);

CREATE POLICY user_bets_update ON user_bets
    FOR UPDATE USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY user_bets_delete ON user_bets
    FOR DELETE USING (auth.uid() = user_id);

-- ============================================================
-- updated_at trigger for user_profiles
-- ============================================================
CREATE OR REPLACE FUNCTION update_user_profiles_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_user_profiles_updated_at
    BEFORE UPDATE ON user_profiles
    FOR EACH ROW
    EXECUTE FUNCTION update_user_profiles_updated_at();
