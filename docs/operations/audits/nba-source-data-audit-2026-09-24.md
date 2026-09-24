# NBA Source-Data Readiness Audit — 2026-09-24

**Lane:** A1 source data  
**Audit completed:** 2026-09-24T11:59:25-04:00  
**Mode:** Read-only; no database mutations, backfills, DDL, deploys, or source edits  
**Production truth:** remote PostgreSQL via `DATABASE_URL`, read-only transaction  
**Training-mirror truth:** local PostgreSQL via `LOCAL_DATABASE_URL_AGENT`/`LOCAL_DATABASE_URL`, read-only transaction

## Verdict

**NOT READY for a dependable 2026–27 NBA daily slate.** Historical basic game/box-score data, dimensions, and rolling tables are structurally strong and remote/local are nearly identical, but the retained CDN path and scheduled-game inference path still hard-code the completed **2025–26** season (`22025`). If left unchanged, 2026–27 rows and scheduled inference context will be mislabeled. This is a **P0 season-rollover blocker**.

The remote database is otherwise usable as a historical source: core keys have no duplicates, basic player/team rows have complete dimension linkage, and current retained dates correctly stop at the 2025–26 Finals on 2026-06-13. The next-season restart still has material P1 gaps: there is no retained NBA schedule, roster, or lineup table; advanced-stat coverage is incomplete and workstation-only; `game_id_map` is stale/incomplete; injury linkage has unresolved players; and job failure evidence is too truncated to diagnose 66 historical failures.

No 2026–27 game-performance rows are expected yet because the season has not started. Their absence is not itself a defect; readiness depends on fixing rollover logic and proving a bounded preseason/first-game canary.

## Priority findings

### P0 — blocks the first dependable 2026–27 slate

1. **Season identity is hard-coded to 2025–26 in the production-safe CDN path.**
   - `src/scrapers/nba_cdn_scraper.py:52-53` defines `DEFAULT_SEASON_ID = "22025"` and `DEFAULT_SEASON = "2025-26"`.
   - Team rows use those constants at `src/scrapers/nba_cdn_scraper.py:187-217`.
   - Player rows use `DEFAULT_SEASON_ID` at `src/scrapers/nba_cdn_scraper.py:253-285`.
   - Railway runs this path through `nba_unified_scraper.py --cdn-only` (`src/orchestration/daily_stats_job.py:390-395`).
   - Consequence: the first finalized 2026–27 game would be written under `22025`/`2025-26`, contaminating season-scoped averages, training cohorts, and consumer context.

2. **Scheduled-game inference also hard-codes `season_id = "22025"`.**
   - `src/models/features/nba/inference_loader.py:67-82` returns `22025` whenever scheduled context is supplied.
   - The current test explicitly expects that stale value (`tests/test_nba_inference_loader.py:102-105`), so the regression is encoded in the suite.
   - Consequence: even before a final box score exists, 2026–27 inference records can carry the wrong season identity.

### P1 — repair before declaring source/automation ready

1. **No retained NBA schedule, roster, or lineup base table exists remotely or locally.**
   - `information_schema` returned no non-MLB table containing `schedule`, `roster`, or `lineup`.
   - The CDN schedule is fetched transiently and only final games are persisted to `team_game_stats`/`player_game_stats` (`src/scrapers/nba_cdn_scraper.py:77-130`, `391-517`).
   - This leaves no durable future-slate schedule or roster truth for preseason canaries, traded/newly rostered players, or independent frontend validation. Current scheduled context must be supplied by downstream event/props paths rather than a retained NBA source table.

2. **Advanced box-score history is incomplete and is not populated by Railway.**
   - For 2025-10-01 through 2026-06-30: 38,701 basic player-game rows; 34,488 linked advanced rows; **4,213** basic rows lack advanced rows, including **230 active-player rows**.
   - `player_average_advanced_stats` contains **230 rows without a matching advanced source row**.
   - Railway intentionally runs CDN-only, and the CDN scraper explicitly does not populate advanced stats (`src/scrapers/nba_cdn_scraper.py:8-15`; `src/scrapers/nba_unified_scraper.py:972-1018`). The retained advanced catch-up is workstation-only.
   - The `created_at`-bounded orphan diagnostic found 85,609 advanced rows without matching basic rows among 535,862 advanced rows created during the audit window. Because `created_at` is scrape time rather than game date, this is a historical-integrity warning, not a season-window count; it needs a keyed game-ID audit before use in a training cohort.

3. **`game_id_map` is stale and incomplete relative to game stats.**
   - Remote/local: 1,891 rows; max `game_date_utc` **2026-01-10**, while basic game stats continue through **2026-06-13**.
   - **657** rows have null home and away team IDs; **50** mapped internal game IDs have no matching `team_game_stats` game in the audited window.
   - Core box-score consumers can join directly by NBA `game_id`, but any linker or consumer relying on `game_id_map` cannot be considered current without a bounded usage/repair decision.

4. **Production job evidence shows recovery, but historical failures are not diagnosable from stored evidence.**
   - Remote `job_executions`, 2026-06-26 through 2026-09-24: **58 successes** and **66 failures** for `daily_stats_job.py`.
   - Latest success: **2026-09-24T13:00:00.041556+00:00**, ending **2026-09-24T13:11:26.928224+00:00**.
   - Latest failure: **2026-08-24T13:30:00.256029+00:00**. The latest 20 runs, 2026-09-05 through 2026-09-24, were successful.
   - All 66 stored failure messages are 500-character tail excerpts showing only the final generic `DAILY STATS JOB FAILED` summary, not the failed critical step. Scheduler storage takes only `stderr[-500:]` (`src/orchestration/scheduler.py:462-472`).
   - A green offseason run proves orchestration executes; it does not prove a new-game insert, average update, or downstream linkage.

5. **Injury data is historically broad but not fully linked.**
   - Remote/local: 43,090 rows from **2024-10-21** through **2026-06-12**; 22,329 rows in the 2025–26 window.
   - **170 rows** have null `player_id`; team IDs are fully populated; linked IDs have zero dangling dimension links.
   - Injury consumers use exact `report_date = game_date` predicates and default to zero context when no matching row exists (`tests/test_nba_injury_context.py:92-138`). Same-day scraping must therefore be proven for 2026–27; historical availability alone is insufficient.

6. **Small but real game-context inconsistencies remain.**
   - All 2,641 audited games have exactly two `team_game_stats` rows and two distinct teams.
   - **5 team-game rows** have a non-reciprocal opponent relationship.
   - `player_game_stats` has **823 null `started` values** in 67,113 rows. The rolling pipeline has a minutes-based starter fallback, so this is repairable rather than fatal (`src/processing/populate_average_stats_incremental.py:290-299`).

7. **Local mirror is nearly synchronized, but position history is one day behind.**
   - All audited core counts match remote exactly except `player_position_history`.
   - Remote: 115,739 rows through **2026-09-24**. Local: 115,158 rows through **2026-09-23**. Difference: **581 rows / one snapshot day**.
   - This affects local training/backtest truth, not remote production truth.

### P2 — quality debt / expected cold-start states

1. `players` has 2,015 rows with no duplicate IDs, but **8 blank names** and **70 missing primary-position/position-group values**. The point-in-time `player_position_history.position_group` field is complete in the audited window, so current feature consumers are not blocked.
2. `player_position_history.primary_position` is blank on **78,340** remote rows, while `position_group` is complete. Consumers use `position_group`; the raw primary-position field is quality debt.
3. Injury data contains **2 logical duplicates** under the audit key used here, despite unique IDs.
4. First-game/cold-start rows produce expected null/default-like rolling values:
   - `player_average_game_stats`: 2,640 null season minutes/points averages; 1,423 rows with `games_szn = 0`.
   - `player_average_advanced_stats`: 1,502 null season offensive/defensive ratings; 1,417 rows with `games_szn = 0`.
   - `team_average_game_stats`: 84 null season points/offensive-rating rows; 84 rows with `games_szn = 0`.
   - These tables have no declared SQL defaults. The nulls align with shift(1) first-game semantics, but the 2026–27 cold-start behavior must be exercised by the derived-feature lane.
5. Local intentionally has no `job_executions` table; production automation evidence is remote-only.

## Readiness by source

| Source/layer | Remote evidence as of 2026-09-24 | Status | Reason |
|---|---:|---|---|
| Teams | 30 rows; no duplicate/null IDs or names | **Ready** | Complete NBA team dimension |
| Players | 2,015 rows; 8 blank names; 70 missing dimension positions | **Stale but repairable** | IDs link cleanly; quality gaps remain |
| Schedule/future games | No NBA schedule table | **Unavailable / P1** | CDN schedule is transient; only final games persist |
| Team game stats | 5,282 rows / 2,641 games, 2024-10-22 to 2026-06-13; zero duplicate keys | **Historically ready; rollover blocked** | Five reciprocal-opponent mismatches; CDN season hard-code is P0 |
| Player game stats / box scores | 67,113 rows / 2,641 games; zero duplicate keys or dangling links | **Historically ready; rollover blocked** | 823 null starter flags; same P0 season hard-code |
| Player advanced game stats | 34,488 current-season linked rows; 230 active basic rows missing advanced | **Incomplete historical coverage** | Workstation-only source; not Railway-populated |
| Player basic rolling averages | 58,718 rows through 2026-06-13; zero duplicate/dangling keys | **Ready historically** | Nulls/defaults largely first-game semantics |
| Player advanced rolling averages | 57,763 rows; 230 rows lack source advanced row | **Incomplete historical coverage** | Mirrors advanced-source gap |
| Team rolling averages | 5,186 rows; zero duplicate/dangling keys | **Ready historically** | Expected first-game nulls |
| Injuries/availability | 43,090 rows through 2026-06-12; 170 unlinked players | **Stale but repairable** | Offseason end date is plausible; same-day 2026–27 refresh unproven |
| Rosters | No NBA roster table | **Unavailable / P1** | Player/team truth inferred from games/props/position snapshots |
| Lineups | No NBA lineup table | **Intentionally unavailable** | No retained NBA lineup source/consumer contract found |
| Position snapshots | Remote current through 2026-09-24; complete position groups | **Ready remote / local stale** | Local trails by 581 rows and one day |
| Game ID mapping | 1,891 rows, max 2026-01-10; 657 null team pairs; 50 missing game links | **Stale/incomplete / P1** | Not aligned with complete 2025–26 stats |
| Daily ingestion automation | Latest run succeeded 2026-09-24; latest 20 succeeded | **Code path active; first-game proof missing** | 66 older failures have unusable error tails |

## Schema and consumer compatibility

### Compatible

- Core primary keys are explicit and duplicate-free:
  - `team_game_stats(team_id, game_id)`
  - `player_game_stats(player_id, game_id)`
  - `player_game_advanced_stats(game_id, player_id)`
  - rolling tables keyed by entity plus `game_id`
  - `player_position_history(player_id, snapshot_date)`
- Basic game rows link cleanly to `teams`, `players`, and the corresponding team-game row.
- Rolling consumers use strict pre-game predicates (`game_date < :as_of_date`) in `src/models/features/nba/player_sources.py:14-45`, `team_sources.py`, and `context_sources.py:9-30`.
- Rolling generation uses `shift(1)` and the eight-minute source rule (`src/processing/populate_average_stats_incremental.py:244-318`).
- Focused source tests passed: **90 passed, 1 deprecation warning**.

### Mismatched or fragile

- 2026–27 season identity is not derived from the game date or CDN schedule metadata; it is hard-coded in both source ingestion and scheduled inference.
- Scheduled inference requires externally supplied team/opponent/home context because no future schedule table exists (`src/models/feature_store.py:75-99`).
- Injury queries require exact report-date equality. Missing same-day collection silently becomes zero/default injury context.
- The daily job reports one wrapper status for seven source/processing steps. Failed DB evidence stores only the final stderr tail, preventing step-level diagnosis.
- `game_id_map` is materially behind the core game tables and should not be assumed compatible with current-season consumers.

## Automation evidence

The scheduler registers:

- 9:00 AM ET daily stats and 9:30 AM retry (`src/orchestration/scheduler.py:730-744`).
- 10:00 AM props and 10:15 AM inference (`scheduler.py:754-770`).
- Optional 12:01 PM and 4:01 PM full lines/injury runs plus 12:15/4:15 inference (`scheduler.py:772-833`).
- Props every five minutes and edge refresh at a two-minute offset (`scheduler.py:796-810`).

The daily stats source chain is:

1. `nba_unified_scraper.py --cdn-only`
2. incremental linker
3. recent team-ID backfill
4. player-position update
5. league-position averages
6. rolling averages for yesterday
7. opponent-allowed update

Steps 1, 2, 6, and 7 are critical (`src/orchestration/daily_stats_job.py:387-444`). The chain is production-safe with respect to the `stats.nba.com` restriction, but it currently lacks a season-rollover guard and an output assertion proving that a game day produced the expected source/derived rows.

## Safe, bounded repair scope

These are code/data-validation repairs, not actions performed by this audit:

1. Replace hard-coded `22025`/`2025-26` values with a tested season resolver derived from game date/schedule metadata; add 2026–27 boundary tests for CDN team rows, CDN player rows, and scheduled inference.
2. Add a read-only preseason canary that fetches the CDN schedule and reports upcoming 2026–27 dates/game IDs without inserting rows.
3. Decide whether a durable NBA schedule/roster table is required. If not, document the event/props source as the canonical future-slate/roster contract and add explicit new/traded-player validation.
4. Run a bounded keyed audit of the 230 active rows missing advanced stats and the 85,609 `created_at`-window advanced/basic orphans before any approved workstation catch-up.
5. Audit `game_id_map` consumers, then repair only the required date/game-ID window after approval.
6. Improve job evidence to persist failed step name and a useful bounded error excerpt; add output metrics such as games inserted, player rows inserted, and rolling rows upserted.
7. Resolve the 170 unlinked injury rows and five reciprocal opponent anomalies with exact keyed reports before changing data.
8. Sync the single missing local position-snapshot day only after remote remains the verified source of truth.

## Approval boundaries

**No approval needed for a future implementation proposal/code-only change:** season resolver, tests, dry-run/canary, richer job metrics, and consumer documentation.

**Requires Chase approval before execution:** any remote backfill/update, advanced-stats catch-up, injury relink write, `game_id_map` repair, local mirror sync, schema addition for schedules/rosters, or deployment.

## Explicit unknowns

- No 2026–27 final-game row exists yet, so insertion behavior cannot be proven against real new-season box scores.
- This lane did not call live NBA/RapidAPI endpoints; source-provider availability and credential validity were not tested.
- Railway deployment identity/logs were not independently fetched. Remote `job_executions` proves recorded runs, not the exact deployed commit.
- Stored failure excerpts do not identify which critical daily-stats step caused the 66 failures.
- The 85,609 advanced/basic orphan count is bounded by advanced-row `created_at`, not game date, and must not be interpreted as 2025–26 missing-game coverage.
- Whether `game_id_map` is still on a core consumer path requires a targeted linker/consumer lane decision.

## Evidence commands and exact SQL

### Repository and test commands

```text
git status --short --branch
git log -1 --format='%H %cI %s'
python -m pytest tests/test_nba_unified_scraper.py tests/test_populate_average_stats.py tests/test_espn_injury_scraper.py tests/test_injury_database.py tests/test_injury_scraper_job.py tests/test_rapidapi_injury_backfill.py tests/test_nba_source_boundaries.py tests/test_nba_injury_context.py -q -o addopts=
date --iso-8601=seconds
```

Test result: `90 passed, 1 warning in 6.79s`.

### SQL safety controls

```sql
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '30000ms';
SET LOCAL lock_timeout = '3000ms';
SELECT current_database(), current_user, current_setting('transaction_read_only');
```

Both sessions returned `transaction_read_only = on`; remote user/database were `postgres`/`postgres`, local database was `gameflow_local`.

### Table-presence query

```sql
SELECT t.table_name
FROM information_schema.tables AS t
WHERE t.table_schema = 'public'
  AND t.table_type = 'BASE TABLE'
  AND t.table_name NOT ILIKE 'mlb_%'
  AND (
    t.table_name IN (
      'teams','players','team_game_stats','player_game_stats',
      'player_game_advanced_stats','player_average_game_stats',
      'player_average_advanced_stats','team_average_game_stats',
      'rapidapi_injuries','player_position_history','game_id_map','job_executions'
    )
    OR t.table_name ILIKE '%schedule%'
    OR t.table_name ILIKE '%roster%'
    OR t.table_name ILIKE '%lineup%'
  )
ORDER BY t.table_name;
```

### Core bounded summary pattern

The exact per-table SELECTs used qualified aliases and the half-open window below; each table was queried separately to keep statements bounded and to avoid cross-table aggregate timeouts.

```sql
WITH w AS (
  SELECT pgs.season_id, pgs.player_id, pgs.game_id, pgs.team_id,
         pgs.game_date, pgs.did_not_play, pgs.started,
         pgs.min AS minutes, pgs.pts AS points
  FROM public.player_game_stats AS pgs
  WHERE pgs.game_date >= DATE '2024-10-01'
    AND pgs.game_date < DATE '2026-09-25'
), latest AS (
  SELECT w.season_id
  FROM w
  WHERE w.season_id IS NOT NULL
  ORDER BY w.game_date DESC, w.season_id DESC
  LIMIT 1
)
SELECT count(*) AS exact_window_rows,
       count(DISTINCT w.game_id) AS exact_window_games,
       min(w.game_date) AS min_game_date,
       max(w.game_date) AS max_game_date,
       (SELECT latest.season_id FROM latest) AS latest_season_id,
       count(*) FILTER (WHERE w.season_id = (SELECT latest.season_id FROM latest)) AS latest_season_rows,
       count(*) - count(DISTINCT ROW(w.player_id, w.game_id)) AS duplicate_player_game_rows,
       count(*) FILTER (WHERE w.season_id IS NULL) AS null_season_id,
       count(*) FILTER (WHERE w.game_id IS NULL) AS null_game_id,
       count(*) FILTER (WHERE w.player_id IS NULL) AS null_player_id,
       count(*) FILTER (WHERE w.team_id IS NULL) AS null_team_id,
       count(*) FILTER (WHERE w.did_not_play IS NULL) AS null_did_not_play,
       count(*) FILTER (WHERE w.started IS NULL) AS null_started,
       count(*) FILTER (WHERE COALESCE(w.did_not_play, false) = false AND w.minutes IS NULL) AS active_rows_null_minutes,
       count(*) FILTER (WHERE COALESCE(w.did_not_play, false) = false AND w.points IS NULL) AS active_rows_null_points
FROM w;
```

### Advanced-coverage query

```sql
WITH b AS (
  SELECT pgs.player_id, pgs.game_id, pgs.did_not_play
  FROM public.player_game_stats AS pgs
  WHERE pgs.game_date >= DATE '2025-10-01'
    AND pgs.game_date < DATE '2026-07-01'
)
SELECT count(*) AS exact_basic_rows,
       count(*) FILTER (WHERE a.game_id IS NULL) AS basic_rows_missing_advanced,
       count(*) FILTER (
         WHERE a.game_id IS NULL AND COALESCE(b.did_not_play, false) = false
       ) AS active_basic_rows_missing_advanced
FROM b
LEFT JOIN public.player_game_advanced_stats AS a
  ON a.player_id = b.player_id AND a.game_id = b.game_id;
```

### Job evidence query

```sql
SELECT je.job_name, je.status, count(*) AS exact_runs,
       min(je.started_at) AS first_started_at,
       max(je.started_at) AS last_started_at,
       count(*) FILTER (WHERE je.ended_at IS NULL) AS null_ended_at,
       count(*) FILTER (
         WHERE je.error_message IS NOT NULL AND btrim(je.error_message) <> ''
       ) AS runs_with_error_message
FROM public.job_executions AS je
WHERE je.started_at >= TIMESTAMPTZ '2026-06-26 00:00:00+00'
  AND je.started_at < TIMESTAMPTZ '2026-09-25 00:00:00+00'
  AND je.job_name = 'daily_stats_job.py'
GROUP BY je.job_name, je.status
ORDER BY je.job_name, je.status;
```

### Complete SQL evidence transcript

The isolated SQL runner wrote the complete verbatim query/result transcript, including remote/local versions and all null/linkage queries, to:

- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-source-data-sql-evidence-2026-09-24.md`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-source-data-sql-supplement-2026-09-24.md`

The supplement supersedes failed ambiguous queries in the first transcript.

## Working-tree preservation

The repository was already dirty before the audit. No existing modified/untracked file was edited. This report is the only repository artifact created by this lane. No commit was made.
