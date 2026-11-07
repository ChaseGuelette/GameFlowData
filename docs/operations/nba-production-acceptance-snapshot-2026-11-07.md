# NBA production acceptance snapshot — 2026-11-07

Canonical authority: remote `DATABASE_URL` / Supabase production database.
Database mode: read-only transactions; `SET LOCAL statement_timeout = '15s'` verified per successful query.
SQL executions used: **5** of 6 maximum. Optional authenticated-role/RLS check was not run.

## Acceptance verdict

- Sufficient to ATTEMPT no-bet inference: **YES**
- Reason: All 7 exact schedule game IDs are represented by non-null linked game/player quote rows within the last 48 hours; this supports attempting no-bet inference only.
- This receipt does **not** claim betting readiness.

## Compact results

### Q1 — Nov 7 Eastern schedule and guard

Outcome: **OK**

Result:

```json
{"fixtures": [{"away_team_id": 1610612743, "commence_time_utc": "2026-11-07T22:00:00+00:00", "game_date": "2026-11-07", "game_id": "0022600189", "game_status": 1, "home_team_id": 1610612754, "observed_at": "2026-11-07T20:05:01.048916+00:00", "source": "nba_cdn", "source_updated_at": "2026-11-07T19:59:24.5924+00:00", "status_text": "5:00 pm ET", "updated_at": "2026-11-07T20:05:01.927727+00:00"}, {"away_team_id": 1610612751, "commence_time_utc": "2026-11-08T00:30:00+00:00", "game_date": "2026-11-07", "game_id": "0022600190", "game_status": 1, "home_team_id": 1610612737, "observed_at": "2026-11-07T20:05:01.048916+00:00", "source": "nba_cdn", "source_updated_at": "2026-11-07T19:59:24.5924+00:00", "status_text": "7:30 pm ET", "updated_at": "2026-11-07T20:05:01.927727+00:00"}, {"away_team_id": 1610612752, "commence_time_utc": "2026-11-08T01:00:00+00:00", "game_date": "2026-11-07", "game_id": "0022600191", "game_status": 1, "home_team_id": 1610612739, "observed_at": "2026-11-07T20:05:01.048916+00:00", "source": "nba_cdn", "source_updated_at": "2026-11-07T19:59:24.5924+00:00", "status_text": "8:00 pm ET", "updated_at": "2026-11-07T20:05:01.927727+00:00"}, {"away_team_id": 1610612764, "commence_time_utc": "2026-11-08T01:00:00+00:00", "game_date": "2026-11-07", "game_id": "0022600192", "game_status": 1, "home_team_id": 1610612760, "observed_at": "2026-11-07T20:05:01.048916+00:00", "source": "nba_cdn", "source_updated_at": "2026-11-07T19:59:24.5924+00:00", "status_text": "8:00 pm ET", "updated_at": "2026-11-07T20:05:01.927727+00:00"}, {"away_team_id": 1610612753, "commence_time_utc": "2026-11-08T01:30:00+00:00", "game_date": "2026-11-07", "game_id": "0022600193", "game_status": 1, "home_team_id": 1610612744, "observed_at": "2026-11-07T20:05:01.048916+00:00", "source": "nba_cdn", "source_updated_at": "2026-11-07T19:59:24.5924+00:00", "status_text": "8:30 pm ET", "updated_at": "2026-11-07T20:05:01.927727+00:00"}, {"away_team_id": 1610612745, "commence_time_utc": "2026-11-08T02:00:00+00:00", "game_date": "2026-11-07", "game_id": "0022600194", "game_status": 1, "home_team_id": 1610612756, "observed_at": "2026-11-07T20:05:01.048916+00:00", "source": "nba_cdn", "source_updated_at": "2026-11-07T19:59:24.5924+00:00", "status_text": "9:00 pm ET", "updated_at": "2026-11-07T20:05:01.927727+00:00"}, {"away_team_id": 1610612759, "commence_time_utc": "2026-11-08T03:00:00+00:00", "game_date": "2026-11-07", "game_id": "0022600195", "game_status": 1, "home_team_id": 1610612757, "observed_at": "2026-11-07T20:05:01.048916+00:00", "source": "nba_cdn", "source_updated_at": "2026-11-07T19:59:24.5924+00:00", "status_text": "10:00 pm ET", "updated_at": "2026-11-07T20:05:01.927727+00:00"}], "schedule_count": 7, "statement_timeout": "15s", "transaction_read_only": "on"}
```

Parameters:

```json
["2026-11-07"]
```

Exact SQL:

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
SELECT
  current_setting('transaction_read_only') AS transaction_read_only,
  current_setting('statement_timeout') AS statement_timeout,
  COUNT(*)::int AS schedule_count,
  COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'game_id', game_id,
        'game_date', game_date,
        'commence_time_utc', commence_time_utc,
        'home_team_id', home_team_id,
        'away_team_id', away_team_id,
        'game_status', game_status,
        'status_text', status_text,
        'source', source,
        'source_updated_at', source_updated_at,
        'observed_at', observed_at,
        'updated_at', updated_at
      ) ORDER BY commence_time_utc, game_id
    ), '[]'::jsonb
  ) AS fixtures
FROM public.nba_game_schedule
WHERE game_date = %s;
```

### Q2 — Current linked inference quotes for exact schedule IDs

Outcome: **OK**

Result:

```json
{"by_game": [{"game_id": "0022600189", "markets": 12, "players": 14, "rows": 18600}, {"game_id": "0022600190", "markets": 12, "players": 13, "rows": 14974}, {"game_id": "0022600191", "markets": 12, "players": 14, "rows": 5568}, {"game_id": "0022600192", "markets": 12, "players": 14, "rows": 9142}, {"game_id": "0022600193", "markets": 12, "players": 13, "rows": 10106}, {"game_id": "0022600194", "markets": 12, "players": 15, "rows": 6676}, {"game_id": "0022600195", "markets": 12, "players": 12, "rows": 5848}], "distinct_games": 7, "distinct_markets": 12, "distinct_players": 95, "paired_group_count": 25779, "paired_quote_row_count": 51558, "represented_game_ids": ["0022600189", "0022600190", "0022600191", "0022600192", "0022600193", "0022600194", "0022600195"], "rows": 70914, "statement_timeout": "15s", "transaction_read_only": "on"}
```

Parameters:

```json
[["0022600189", "0022600190", "0022600191", "0022600192", "0022600193", "0022600194", "0022600195"]]
```

Exact SQL:

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
WITH scoped AS MATERIALIZED (
  SELECT staging_id, game_id, player_id, market_key, line, bookmaker,
         outcome_label, source_snapshot_time, observed_at
  FROM public.nba_player_props_inference
  WHERE game_id = ANY(%s::text[])
    AND game_id IS NOT NULL
    AND player_id IS NOT NULL
    AND observed_at >= now() - interval '48 hours'
), grouped AS (
  SELECT game_id, player_id, market_key, line, bookmaker, source_snapshot_time,
         COUNT(*)::int AS group_rows,
         bool_or(lower(outcome_label) = 'over') AS has_over,
         bool_or(lower(outcome_label) = 'under') AS has_under
  FROM scoped
  GROUP BY game_id, player_id, market_key, line, bookmaker, source_snapshot_time
), paired AS (
  SELECT * FROM grouped WHERE has_over AND has_under
)
SELECT
  current_setting('transaction_read_only') AS transaction_read_only,
  current_setting('statement_timeout') AS statement_timeout,
  (SELECT COUNT(*)::int FROM scoped) AS rows,
  (SELECT COUNT(DISTINCT player_id)::int FROM scoped) AS distinct_players,
  (SELECT COUNT(DISTINCT game_id)::int FROM scoped) AS distinct_games,
  (SELECT COUNT(DISTINCT market_key)::int FROM scoped) AS distinct_markets,
  (SELECT COUNT(*)::int FROM paired) AS paired_group_count,
  (SELECT COALESCE(SUM(group_rows), 0)::int FROM paired) AS paired_quote_row_count,
  (SELECT COALESCE(jsonb_agg(game_id ORDER BY game_id), '[]'::jsonb)
     FROM (SELECT DISTINCT game_id FROM scoped) d) AS represented_game_ids,
  (SELECT COALESCE(jsonb_agg(jsonb_build_object('game_id', game_id, 'rows', rows,
                                                'players', players, 'markets', markets)
                              ORDER BY game_id), '[]'::jsonb)
     FROM (SELECT game_id, COUNT(*)::int rows, COUNT(DISTINCT player_id)::int players,
                  COUNT(DISTINCT market_key)::int markets
           FROM scoped GROUP BY game_id) g) AS by_game;
```

### Q3 — Latest 20,000 inference rows by primary key

Outcome: **OK**

Result:

```json
{"bounded_rows": 20000, "groups": [{"eastern_fixture_date": "2026-11-07", "identity_status": "linked_game_and_player", "rows": 20000}], "max_staging_id": 837169, "min_staging_id": 817090, "statement_timeout": "15s", "transaction_read_only": "on"}
```

Exact SQL:

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
WITH bounded AS MATERIALIZED (
  SELECT staging_id, commence_time, game_id, player_id
  FROM public.nba_player_props_inference
  ORDER BY staging_id DESC
  LIMIT 20000
), grouped AS (
  SELECT (commence_time AT TIME ZONE 'America/New_York')::date AS eastern_fixture_date,
         CASE
           WHEN game_id IS NOT NULL AND player_id IS NOT NULL THEN 'linked_game_and_player'
           WHEN game_id IS NULL AND player_id IS NULL THEN 'missing_game_and_player'
           WHEN game_id IS NULL THEN 'missing_game_only'
           ELSE 'missing_player_only'
         END AS identity_status,
         COUNT(*)::int AS rows
  FROM bounded
  GROUP BY 1, 2
)
SELECT
  current_setting('transaction_read_only') AS transaction_read_only,
  current_setting('statement_timeout') AS statement_timeout,
  (SELECT COUNT(*)::int FROM bounded) AS bounded_rows,
  (SELECT MIN(staging_id) FROM bounded) AS min_staging_id,
  (SELECT MAX(staging_id) FROM bounded) AS max_staging_id,
  COALESCE(jsonb_agg(jsonb_build_object('eastern_fixture_date', eastern_fixture_date,
                                        'identity_status', identity_status,
                                        'rows', rows)
                     ORDER BY eastern_fixture_date DESC NULLS LAST, identity_status), '[]'::jsonb) AS groups
FROM grouped;
```

### Q4 — Nov 7 daily predictions and MC samples

Outcome: **OK**

Result:

```json
{"prediction_games": 0, "prediction_markets": 0, "prediction_players": 0, "prediction_rows": 0, "sample_games": 0, "sample_markets": 0, "sample_players": 0, "sample_rows": 0, "statement_timeout": "15s", "transaction_read_only": "on"}
```

Parameters:

```json
["2026-11-07", "2026-11-07", "2026-11-07", "2026-11-07", "2026-11-07", "2026-11-07", "2026-11-07", "2026-11-07"]
```

Exact SQL:

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
SELECT
  current_setting('transaction_read_only') AS transaction_read_only,
  current_setting('statement_timeout') AS statement_timeout,
  (SELECT COUNT(*)::int FROM public.daily_predictions WHERE prediction_date = %s) AS prediction_rows,
  (SELECT COUNT(DISTINCT game_id)::int FROM public.daily_predictions WHERE prediction_date = %s) AS prediction_games,
  (SELECT COUNT(DISTINCT player_id)::int FROM public.daily_predictions WHERE prediction_date = %s) AS prediction_players,
  (SELECT COUNT(DISTINCT stat)::int FROM public.daily_predictions WHERE prediction_date = %s) AS prediction_markets,
  (SELECT COUNT(*)::int FROM public.daily_prediction_samples WHERE prediction_date = %s) AS sample_rows,
  (SELECT COUNT(DISTINCT game_id)::int FROM public.daily_prediction_samples WHERE prediction_date = %s) AS sample_games,
  (SELECT COUNT(DISTINCT player_id)::int FROM public.daily_prediction_samples WHERE prediction_date = %s) AS sample_players,
  (SELECT COUNT(DISTINCT stat)::int FROM public.daily_prediction_samples WHERE prediction_date = %s) AS sample_markets;
```

### Q5 — Visible active writer jobs

Outcome: **OK**

Result:

```json
{"has_pg_monitor": true, "inspected_as": "postgres", "statement_timeout": "15s", "transaction_read_only": "on", "visible_active_writer_count": 0, "writers": []}
```

Exact SQL:

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
WITH active_writers AS (
  SELECT pid, application_name, usename, state, wait_event_type, wait_event,
         now() - query_start AS query_age,
         left(regexp_replace(query, E'[\n\r\t ]+', ' ', 'g'), 300) AS query_snippet
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid()
    AND state = 'active'
    AND query ~* '\m(insert|update|delete|merge|copy|truncate|alter|create|drop)\M'
    AND query ~* '(nba_game_schedule|nba_player_props_inference|daily_predictions|daily_prediction_samples|mc_samples)'
  ORDER BY query_start
  LIMIT 50
)
SELECT
  current_setting('transaction_read_only') AS transaction_read_only,
  current_setting('statement_timeout') AS statement_timeout,
  current_user AS inspected_as,
  pg_has_role(current_user, 'pg_monitor', 'member') AS has_pg_monitor,
  COUNT(*)::int AS visible_active_writer_count,
  COALESCE(jsonb_agg(jsonb_build_object('pid', pid, 'application_name', application_name,
                                        'usename', usename, 'state', state,
                                        'wait_event_type', wait_event_type, 'wait_event', wait_event,
                                        'query_age', query_age, 'query_snippet', query_snippet)
                     ORDER BY query_age DESC), '[]'::jsonb) AS writers
FROM active_writers;
```

## Interpretation notes

- Q2 used only `public.nba_player_props_inference` with exact schedule `game_id` values, non-null linked identities, and `observed_at >= now() - interval '48 hours'`, matching the existing partial linked index access shape.
- Paired Over/Under groups use the exact supported key `(game_id, player_id, market_key, line, bookmaker, source_snapshot_time)` and require both normalized outcome labels.
- Q3 is explicitly capped at the latest 20,000 rows by primary key `staging_id`; its grouped result is not a whole-table aggregate.
- Q5 reports only active writers visible through the current role's `pg_stat_activity` permissions; zero visible writers is not proof that no external job exists.
