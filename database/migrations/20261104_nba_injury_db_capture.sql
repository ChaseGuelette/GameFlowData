-- Additive only: keep the existing injury table, IDs and unique index.
-- Apply explicitly before deploying the DB-first capture writer.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '15s';
ALTER TABLE public.rapidapi_injuries
    ADD COLUMN IF NOT EXISTS game_id text,
    ADD COLUMN IF NOT EXISTS source_observations jsonb;
COMMIT;

-- Readback for the operator; no data backfill is performed here.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'rapidapi_injuries'
  AND column_name IN ('game_id', 'source_observations')
ORDER BY column_name;
