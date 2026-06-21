-- Move RapidAPI injury schema cleanup out of the recurring scraper path.
--
-- Context: src/scrapers/rapidapi_injury_backfill.py previously ran these
-- schema mutations on every scheduled scrape. Keep production data-refresh jobs
-- data-only; apply this migration explicitly after read-only schema preflight.
--
-- Production preflight:
--   SELECT column_name, is_nullable
--   FROM information_schema.columns
--   WHERE table_schema = 'public'
--     AND table_name = 'rapidapi_injuries'
--     AND column_name = 'team';
--
--   SELECT indexname, indexdef
--   FROM pg_indexes
--   WHERE schemaname = 'public'
--     AND tablename = 'rapidapi_injuries'
--     AND indexname = 'idx_rapidapi_inj_unique';
--
-- If the table is large or locks cannot be tolerated, use an explicitly
-- approved CREATE UNIQUE INDEX CONCURRENTLY flow outside a transaction instead.

SET lock_timeout = '5s';
SET statement_timeout = '60s';

ALTER TABLE public.rapidapi_injuries
    ALTER COLUMN team DROP NOT NULL;

DROP INDEX IF EXISTS public.idx_rapidapi_inj_unique;

CREATE UNIQUE INDEX IF NOT EXISTS idx_rapidapi_inj_unique
    ON public.rapidapi_injuries(report_date, COALESCE(team, ''), player, status, COALESCE(reason, ''));
