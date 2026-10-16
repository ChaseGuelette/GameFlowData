-- Prepared for review only. Do not apply until NBA Layer 2 production approval.
-- The schedule is mutable current state; no historical as-of replay is implied.
BEGIN;

CREATE TABLE IF NOT EXISTS public.nba_game_schedule (
  game_id text NOT NULL,
  season_id text NOT NULL,
  season text NOT NULL,
  game_type text NOT NULL,
  game_date date NOT NULL,
  commence_time_utc timestamp with time zone NULL,
  home_team_id bigint NULL,
  away_team_id bigint NULL,
  game_status smallint NULL,
  status_text text NULL,
  postponed_status text NULL,
  venue_name text NULL,
  source text NOT NULL,
  source_updated_at timestamp with time zone NULL,
  observed_at timestamp with time zone NOT NULL,
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT pk_nba_game_schedule PRIMARY KEY (game_id),
  CONSTRAINT ck_nba_game_schedule_game_type CHECK (game_type IN ('002', '004', '005')),
  CONSTRAINT ck_nba_game_schedule_distinct_teams CHECK (
    home_team_id IS NULL OR away_team_id IS NULL OR home_team_id <> away_team_id
  ),
  CONSTRAINT fk_nba_game_schedule_home_team FOREIGN KEY (home_team_id)
    REFERENCES public.teams (team_id),
  CONSTRAINT fk_nba_game_schedule_away_team FOREIGN KEY (away_team_id)
    REFERENCES public.teams (team_id)
);

CREATE INDEX IF NOT EXISTS idx_nba_game_schedule_date
  ON public.nba_game_schedule (game_date, game_id);

ALTER TABLE public.nba_game_schedule ENABLE ROW LEVEL SECURITY;
GRANT SELECT ON public.nba_game_schedule TO authenticated;

DO $policy$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public' AND tablename = 'nba_game_schedule'
      AND policyname = 'nba_game_schedule_authenticated_read'
  ) THEN
    CREATE POLICY nba_game_schedule_authenticated_read
      ON public.nba_game_schedule FOR SELECT TO authenticated USING (true);
  END IF;
END
$policy$;

COMMIT;
