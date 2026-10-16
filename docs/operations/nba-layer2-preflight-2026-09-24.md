# NBA Layer 2 preflight — 2026-09-24 (for review)

This is preparation for approval, not authorization to apply the migration, persist a full schedule, deploy, or publish customer picks. Layer 1 is already committed separately.

## Source canary (read-only, local Windows environment)

- Endpoint: `https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json` using existing `nba_cdn_scraper.fetch_schedule()` and its browser-like CDN headers. This local GET succeeded; reproducible report: `docs/operations/nba-layer2-canary-2026-09-24.json` (`python src/scrapers/nba_schedule.py --canary --output docs/operations/nba-layer2-canary-2026-09-24.json`). No DB engine is created by this command.
- 1,274 games in feed: 67 `001` preseason, 1,206 `002` regular season, 1 `006` Cup Championship placeholder. At this observation time there were no `004`/`005` rows. Six competitive games had an unassigned home or away team, and none lacked `gameDateTimeUTC`. The `0062600001` placeholder had home/away team IDs `0`; it must be excluded from schedule persistence and all prediction discovery.
- Canonical schedule date is each `gameDates[].gameDate`; start time is `games[].gameDateTimeUTC` (UTC). `gameDateTimeEst` is misleadingly suffixed `Z` despite denoting Eastern time. Live timing comparison also showed `meta.time` is Eastern wall-clock time mislabeled `Z`: source `13:44:49Z` when New York time was `13:50` and actual UTC was `17:50`. The parser corrects `meta.time` with `America/New_York` before UTC age checks; the report retains the raw value. Team IDs `0`/missing mean TBD rather than a valid team FK.
- Railway CLI authentication is restored and a public SSH key is registered. The read-only in-container source check is still **unverified**: the SSH key required a local passphrase, the prompt was not completed, and the hung attempt was canceled. Railway MCP `list_services` separately returned Unauthorized; do not substitute the successful local CDN check for Railway reachability.

## Policy and planned schema

- `006`: blanket exclusion (not persisted, not prediction-eligible). `001` and `003` are also excluded; `002`/`004`/`005` remain eligible.
- Remote SELECT-only preflight found `public.nba_game_schedule` absent (information_schema count `0`). `public.teams.team_id` is `bigint`, and all 30 NBA team IDs in the CDN mapping are present (missing IDs `0`), so the proposed nullable team FKs have matching targets. `mlb_game_schedule` is not a type template because its team IDs are `integer`; its RLS permits authenticated SELECT. Recheck team coverage immediately before a bounded ingest.
- Proposed new migration: `database/migrations/20260924_nba_game_schedule.sql`. Canonical snapshot updated in `database/schema.sql`. Text `game_id` preserves leading zeroes; bigint nullable home/away FKs allow TBD teams. Current-state `game_date`, nullable UTC commence, season/ID, game type, numeric/text status including `postponed_status`, source/time/observation metadata. Authenticated SELECT under RLS; `postgres` backend handles writes. Only the new small table is indexed.
- These SQL files were written for review but **not executed**. The new migration directory is an addition: this repo had only a schema snapshot, not an existing numbered migration convention. The SQL has only static test coverage; it has not been parsed by a Postgres server or applied in staging/production.

## Prepared Layer 2 code (not deployed)

- `src/scrapers/nba_schedule.py`: strict competitive schedule parser and read-only `--canary` (optional JSON output), explicit `--ingest` with one-transaction idempotent upsert; rejects malformed, empty and stale feeds and prevents older source snapshots from replacing newer rows. `006` never persists.
- `src/models/daily_runner.py`: use fresh persisted schedule for slate game ID, teams and UTC time, else direct CDN. Today's rows must be recent (90 minutes), future rows within 48 hours; no NBA stats API, sportsbook-time enrichment or past boxscore table is an inference slate fallback. Incomplete/TBD games remain non-publishable.
- `src/processing/nba_linker_local.py`: daily incremental props linkage uses the schedule DB or shared CDN parser for upcoming games rather than `stats.nba.com`; all `006`, cancelled and postponed games are excluded. Legacy historical boxscore matching remains for completed games.
- `src/orchestration/nba_schedule_job.py` and `src/orchestration/scheduler.py`: schedule refresh is opt-in only with `NBA_SCHEDULE_INGEST_ENABLED=true`, ahead of the three scheduled NBA inference windows (9:45, 12:05, 16:05 ET). The existing `daily_stats_job.py --cdn-only` path is untouched. Do not enable the flag before schema approval/application and a bounded ingest canary.
- Current-state rows cannot reconstruct a prior decision's exact schedule without history. Do not use this table alone for historical point-in-time replay or customer pick certification.

## Approval gates remaining

1. Unlock the local SSH key in its native prompt on a fresh SSH attempt and demonstrate the read-only canary from the Railway container. Inspect differences from the local observation, including postponed/rescheduled and TBD fixtures, and fail closed if source is stale.
2. Review code/tests and the migration, including `006` exclusion, date/UTC semantics, nullable team FKs, idempotent upserts and stale-feed behavior. Confirm the schema against the remote database immediately before application.
3. Separately approve applying the migration, bounded schedule backfill/upsert, and deployment/consumer activation. Run post-application counts, RLS/reader checks, and opener canary before enabling customer output. Historical roster/eligibility and quote readiness remain separate Layer 3+ work.

## Validation

- `python -m pytest -q tests/test_nba_schedule_schema.py` — passed (1 test).
- `python -m ruff check tests/test_nba_schedule_schema.py` — passed.
- `git diff --check -- database/schema.sql` — passed.
- Combined scoped run across schedule, scheduler, linker, daily inference, source-boundary, season, feature-store, and CLI suites — 136 passed (one existing pytest-asyncio deprecation warning).
- Scoped Ruff, `git diff --check`, disabled-job CLI, and real local CDN linker smoke — passed; the live CDN linker saw 16 upcoming games for October 20–22 with zero `006` IDs.
