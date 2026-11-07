-- Additive reconciliation state for authoritative NBA injury game starts.
-- Apply explicitly; application code never applies this migration.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '15s';

CREATE TABLE IF NOT EXISTS public.nba_injury_game_timing (
    game_id text PRIMARY KEY,
    game_date date NOT NULL,
    game_start_utc timestamp with time zone NULL,
    basis text NOT NULL,
    status text NOT NULL,
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT ck_nba_injury_game_timing_game_id
        CHECK (game_id ~ '^(002|004|005)[0-9]{7}$'),
    CONSTRAINT ck_nba_injury_game_timing_status
        CHECK (status IN ('resolved', 'unknown'))
);

ALTER TABLE public.nba_injury_game_timing ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.nba_injury_game_timing FROM PUBLIC;
-- Supabase has these client roles; the standalone local mirror need not.
DO $$
DECLARE client_role text;
BEGIN
    FOREACH client_role IN ARRAY ARRAY['anon', 'authenticated'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = client_role) THEN
            EXECUTE format('REVOKE ALL ON TABLE public.nba_injury_game_timing FROM %I', client_role);
        END IF;
    END LOOP;
END $$;

COMMENT ON TABLE public.nba_injury_game_timing IS
    'Backend-only, one-row-per-game reconciliation of NBA game starts used for injury cutoffs.';

COMMIT;
