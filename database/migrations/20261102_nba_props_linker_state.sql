-- Prepared artifact only. Apply explicitly, then provision the single production row.
SET lock_timeout = '2s';
SET statement_timeout = '20s';

CREATE TABLE IF NOT EXISTS public.nba_props_linker_state (
    state_name text PRIMARY KEY,
    replay_floor bigint NOT NULL,
    cursor bigint NOT NULL,
    high_water bigint,
    first_unresolved_id bigint,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT nba_props_linker_state_single_name CHECK (state_name = 'production'),
    CONSTRAINT nba_props_linker_state_nonnegative CHECK (
        replay_floor >= 0 AND cursor >= replay_floor
    ),
    CONSTRAINT nba_props_linker_state_high_water CHECK (
        high_water IS NULL OR high_water >= cursor
    ),
    CONSTRAINT nba_props_linker_state_unresolved_cycle CHECK (
        first_unresolved_id IS NULL
        OR (
            high_water IS NOT NULL
            AND first_unresolved_id > replay_floor
            AND first_unresolved_id <= high_water
        )
    )
);

ALTER TABLE public.nba_props_linker_state ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.nba_props_linker_state FROM anon, authenticated;

COMMENT ON TABLE public.nba_props_linker_state IS
    'Backend-only durable checkpoint for bounded NBA props identity linking. Row initialization is owner-controlled.';
COMMENT ON COLUMN public.nba_props_linker_state.replay_floor IS
    'Exclusive owner-provisioned lower replay boundary; automation never decreases it.';
COMMENT ON COLUMN public.nba_props_linker_state.cursor IS
    'Exclusive last-scanned staging_id for the active or next cycle.';
COMMENT ON COLUMN public.nba_props_linker_state.high_water IS
    'Inclusive frozen source high-water for the active cycle; NULL between cycles.';
COMMENT ON COLUMN public.nba_props_linker_state.first_unresolved_id IS
    'Minimum unresolved staging_id observed in the active cycle, retained across bounded calls.';

-- Deliberately no INSERT/UPSERT and no authenticated/anon policy.
-- Owner initialization example (choose replay_floor deliberately; do not run blindly):
-- INSERT INTO public.nba_props_linker_state
--     (state_name, replay_floor, cursor, high_water, first_unresolved_id)
-- VALUES ('production', <exclusive_replay_floor>, <exclusive_replay_floor>, NULL, NULL);
