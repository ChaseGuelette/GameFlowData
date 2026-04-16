-- Migration 026: Track Record
-- Adds manual/CSV bet import support and daily log aggregation for user bets.

-- ============================================================
-- 1a. Alter user_bets: nullable prediction_id and player_id,
--     add source column, fix unique constraint
-- ============================================================

ALTER TABLE user_bets ALTER COLUMN prediction_id DROP NOT NULL;
ALTER TABLE user_bets ALTER COLUMN player_id DROP NOT NULL;

ALTER TABLE user_bets ADD COLUMN IF NOT EXISTS source text DEFAULT 'prop_card';

-- Drop the old unique constraint (keyed on player_id, no bet_direction)
ALTER TABLE user_bets DROP CONSTRAINT IF EXISTS user_bets_user_id_game_date_player_id_stat_type_key;

-- New constraint: uses player_name (works for CSV imports without player_id)
-- and includes bet_direction (allows over + under on same player/stat from different books)
ALTER TABLE user_bets ADD CONSTRAINT user_bets_unique_bet
  UNIQUE (user_id, game_date, player_name, stat_type, bet_direction);

-- ============================================================
-- 1b. user_bets_daily_log — pre-computed daily aggregations
-- ============================================================

CREATE TABLE IF NOT EXISTS user_bets_daily_log (
    id             serial PRIMARY KEY,
    user_id        uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    game_date      date NOT NULL,
    total_bets     integer DEFAULT 0,
    bets_won       integer DEFAULT 0,
    bets_lost      integer DEFAULT 0,
    bets_push      integer DEFAULT 0,
    total_staked   numeric(12,2) DEFAULT 0,
    total_pnl      numeric(12,2) DEFAULT 0,
    roi_pct        numeric(8,4),
    cumulative_pnl numeric(12,2) DEFAULT 0,
    bankroll_after numeric(12,2) DEFAULT 0,
    created_at     timestamptz DEFAULT now(),
    updated_at     timestamptz DEFAULT now(),
    UNIQUE (user_id, game_date)
);

ALTER TABLE user_bets_daily_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY user_bets_daily_log_select ON user_bets_daily_log
    FOR SELECT USING ((select auth.uid()) = user_id);

CREATE POLICY user_bets_daily_log_all ON user_bets_daily_log
    FOR ALL USING ((select auth.uid()) = user_id);

-- ============================================================
-- 1c. RPC: rebuild_user_daily_log(target_user_id uuid)
-- Recalculates entire daily log from user_bets for a given user.
-- Chains cumulative_pnl and bankroll_after from user's initial_bankroll.
-- ============================================================

CREATE OR REPLACE FUNCTION rebuild_user_daily_log(target_user_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '30s'
AS $$
DECLARE
    v_initial_bankroll numeric(12,2);
    v_cumulative       numeric(12,2) := 0;
    v_bankroll         numeric(12,2);
    rec                RECORD;
BEGIN
    -- Get the user's initial bankroll (default 1000 if not set)
    SELECT COALESCE(initial_bankroll, 1000)
    INTO v_initial_bankroll
    FROM user_profiles
    WHERE user_id = target_user_id;

    IF NOT FOUND THEN
        v_initial_bankroll := 1000;
    END IF;

    v_bankroll := v_initial_bankroll;

    -- Remove existing log rows for this user
    DELETE FROM user_bets_daily_log WHERE user_id = target_user_id;

    -- Aggregate per day and insert ordered
    FOR rec IN
        SELECT
            game_date,
            COUNT(*) FILTER (WHERE status IN ('won','lost','push'))              AS total_bets,
            COUNT(*) FILTER (WHERE status = 'won')                               AS bets_won,
            COUNT(*) FILTER (WHERE status = 'lost')                              AS bets_lost,
            COUNT(*) FILTER (WHERE status = 'push')                              AS bets_push,
            COALESCE(SUM(stake) FILTER (WHERE status IN ('won','lost','push')), 0) AS total_staked,
            COALESCE(SUM(pnl)   FILTER (WHERE status IN ('won','lost','push')), 0) AS total_pnl
        FROM user_bets
        WHERE user_id = target_user_id
          AND status IN ('won', 'lost', 'push')
        GROUP BY game_date
        ORDER BY game_date ASC
    LOOP
        v_cumulative := v_cumulative + rec.total_pnl;
        v_bankroll   := v_initial_bankroll + v_cumulative;

        INSERT INTO user_bets_daily_log (
            user_id, game_date,
            total_bets, bets_won, bets_lost, bets_push,
            total_staked, total_pnl,
            roi_pct, cumulative_pnl, bankroll_after,
            updated_at
        ) VALUES (
            target_user_id, rec.game_date,
            rec.total_bets, rec.bets_won, rec.bets_lost, rec.bets_push,
            rec.total_staked, rec.total_pnl,
            CASE WHEN rec.total_staked > 0 THEN (rec.total_pnl / rec.total_staked) * 100 ELSE 0 END,
            v_cumulative, v_bankroll,
            now()
        );
    END LOOP;
END;
$$;

-- Allow authenticated users to call this for their own data
GRANT EXECUTE ON FUNCTION rebuild_user_daily_log(uuid) TO authenticated;

-- ============================================================
-- 1d. RPC: get_track_record_summary(target_user_id uuid)
-- Returns monthly aggregated data. If target_user_id = auth.uid(),
-- returns their own full data. Designed to support sharing in future.
-- ============================================================

CREATE OR REPLACE FUNCTION get_track_record_summary(target_user_id uuid)
RETURNS TABLE (
    month           text,
    total_bets      bigint,
    wins            bigint,
    losses          bigint,
    pushes          bigint,
    total_pnl       numeric,
    total_staked    numeric,
    roi_pct         numeric,
    cumulative_pnl  numeric
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET statement_timeout = '30s'
AS $$
    SELECT
        to_char(game_date, 'YYYY-MM')      AS month,
        SUM(total_bets)::bigint            AS total_bets,
        SUM(bets_won)::bigint              AS wins,
        SUM(bets_lost)::bigint             AS losses,
        SUM(bets_push)::bigint             AS pushes,
        SUM(total_pnl)                     AS total_pnl,
        SUM(total_staked)                  AS total_staked,
        CASE WHEN SUM(total_staked) > 0
             THEN (SUM(total_pnl) / SUM(total_staked)) * 100
             ELSE 0
        END                                AS roi_pct,
        MAX(cumulative_pnl)                AS cumulative_pnl
    FROM user_bets_daily_log
    WHERE user_id = target_user_id
      AND (select auth.uid()) = target_user_id
    GROUP BY to_char(game_date, 'YYYY-MM')
    ORDER BY month ASC;
$$;

GRANT EXECUTE ON FUNCTION get_track_record_summary(uuid) TO authenticated;
