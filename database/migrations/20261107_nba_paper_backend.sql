-- PREPARED ONLY: apply separately after review/approval, before deploying readers.
-- Grounded in paper-backend-schema.md production preflight, 2026-11-07.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '15s';

ALTER TABLE public.daily_predictions
    ADD COLUMN IF NOT EXISTS quote_snapshot jsonb,
    ADD COLUMN IF NOT EXISTS model_snapshot jsonb;
ALTER TABLE public.paper_bets
    ADD COLUMN IF NOT EXISTS game_id text,
    ADD COLUMN IF NOT EXISTS evaluation_run_id text,
    ADD COLUMN IF NOT EXISTS entry_snapshot jsonb;

-- Legacy rows remain NULL, unchanged and outside this first-entry constraint.
-- Retain existing uniqueness as a conservative additional protection.
CREATE UNIQUE INDEX IF NOT EXISTS paper_bets_evaluation_first_entry
    ON public.paper_bets (evaluation_run_id, player_id, game_id, stat_type)
    WHERE evaluation_run_id IS NOT NULL;

ALTER TABLE public.paper_bets ADD CONSTRAINT paper_bets_evaluation_evidence
    CHECK (evaluation_run_id IS NULL OR (
        game_id IS NOT NULL AND game_id ~ '^[0-9]{10}$'
        AND entry_snapshot IS NOT NULL AND jsonb_typeof(entry_snapshot) = 'object'
        AND stake = 10 AND kelly_fraction = 0
    ));
COMMIT;
