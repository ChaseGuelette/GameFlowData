# NBA reconciliation and scraper incident — 2026-11-02 UTC

## Status

The owner approved proceeding with reconciliation and requested an owner-launch linker command. Railway CLI authentication is restored. The verified successful deployment is `2aeb972e-7f47-4033-a087-f6d02dda5bab`, commit `79a744316c54161b207240b711a7a0a850b4ce26`; the ingestion/linker files inspected here had no diff from the earlier `d478458` deployment. After owner steering to reuse the existing linker, the separate reconciliation/linker draft was stopped and preserved unintegrated. A narrow repair is running against `src/processing/nba_linker_local.py` in the isolated checkout; it is not accepted, merged or deployed. The additive local props smoke passed separate independent readback. The owner ran the CDN catch-up, whose canonical output now passes bounded verification. No remote write, deployment, archive transfer, pruning or index change was performed by the agent.

### Authenticated follow-up

- Retrieved bounded historical Railway logs for the actual failed deployment. Job-execution 287847 failed with `psycopg2.OperationalError: SSL connection has been closed unexpectedly` during the missing-game-ID SQL pass. The generic `e3q8` tail no longer needs to be treated as the complete available exception. The reason the remote connection closed is still not established; the unbounded query and repeated hard timeouts remain concrete repair targets.
- Railway transport timestamps lagged the job's logged end by approximately nine seconds. Widening the upper log bound recovered the initially empty linker window; an empty narrowly bounded log response was not treated as proof of no failure.
- Daily Stats' emitted final tail additionally shows `UndefinedTable: relation "user_dfs_entries" does not exist` in optional DFS resolution. That is separate from the earlier critical scraper failure; it does not explain away stale source stats.
- A network/DB-blocked local startup probe with Railway-injected current production variables found no configured `PROXY_URL` and reached the intentionally stopped DB boundary. The reproduced malformed-proxy bug is not supported as the cause under the current production configuration. This was not an in-container connectivity/dependency canary.
- The read-only SSH Python-version canary selected the existing registered key but timed out after 40 seconds; in-container log-file/startup verification remains unverified. No SSH key was registered or modified.
- Sanitized bounded logs and startup-probe metadata are saved under `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-reconcile-incident/`.

The checked-out main was `bd1b87a` when this investigation started. Do not assume Railway runs that commit.

## Established evidence

### Discord excerpt

The supplied excerpt contains 13 failed NBA lines alerts, including 12 exact 45-minute durations, and four Daily Stats alerts. Three Daily Stats alerts are synthetic zero-duration stale-input warnings, distinct from the genuine approximately 50-second pipeline failure. Player-prop fetching explicitly completed in 12 lines alerts; fetching and downstream linking must not be conflated.

Source: `C:/Users/Chase/AppData/Local/hermes/pastes/paste_1_202356.txt`.

### Canonical job history

SELECT-only audit window: 2026-10-29 00:00 UTC through DB time 2026-11-02 01:26:31 UTC.

- `lines_job.py`: 70 hard timeouts at 2700.099–2700.113 seconds. One separate failure, job-execution ID 287847, ran 1018.201 seconds and ended at `Linking Props (Incremental)`.
- The separate failure retained `ORDER BY staging_id` and `days=30`, but not the leading database exception. SQLAlchemy `e3q8` alone does not identify the root cause.
- Last recorded successful lines run: 2026-10-23 15:15 UTC. No successful lines run occurred in the inspected window.
- `daily_stats_job.py`: eight real failures at 50.479–51.973 seconds with `steps_completed=1/7`. All stored errors are exactly 500 characters and retain final calibration/wrapper messages instead of the initiating traceback.
- Last recorded successful Daily Stats run: 2026-10-16 13:00 UTC, 7/7 steps.
- `inference_job.py`: 12/12 process successes in the inspected window. This does not prove fresh features, predictions, recommendations or publication readiness.
- No overlapping non-skipped lines runs were found. Cross-job overlap with daily stats/inference exists but is not proven causal.
- Snapshot of the audited DB: `default_transaction_read_only=off` (server default writable), audit transaction read-only, ten sessions, three active, zero idle-in-transaction, zero COPY. No current global read-only/pool-exhaustion incident was established; the Railway application's own connection configuration and transient historical pressure remain unverified.

### Current linker defects/risks verified from source

`src/processing/nba_linker_local.py`:

1. Initial/final missing-player counts scan a rolling 30-day predicate on the giant combined props table. Current indexes do not include a timestamp index.
2. Main player-ID work is keyset-paged, but the subsequent missing-game-ID pass loads the entire matching 30-day set into one DataFrame without a row limit or pagination (around lines 1390–1408).
3. If the initial missing-player count is zero, the function returns before reaching the missing-game-ID pass (around lines 1222–1224). Already player-linked rows can therefore remain game-unlinked.
4. Team assignment falls back to the first recognized home/away team rather than establishing the player's actual team. A reconciliation tool must not silently repeat that inference.
5. Current active incremental schedule discovery is persisted schedule then Railway-safe CDN fallback; the legacy `stats.nba.com` method is not the active incremental route. Do not repeat an old-source diagnosis without deployed-code evidence.

`src/orchestration/lines_job.py` does not impose a child subprocess timeout. Its current error boundary emits stdout head then stderr tail. The 45-minute cap is external to that wrapper; the historical job rows establish the timeout class, not which exact query was active in every timeout.

### Daily Stats failure boundaries and controlled reproduction

`src/orchestration/daily_stats_job.py` invokes `nba_unified_scraper.py --cdn-only` first, with three total attempts and 15/30-second backoffs. Critical failure breaks downstream seven-step processing, but resolution/calibration tasks still run afterward, explaining why the stored final tail can obscure the first failure. The per-attempt scraper timeout is 600 seconds, so a roughly 50-second wrapper failure is consistent with rapid child failures plus retries—not a 600-second timeout.

`src/scrapers/nba_unified_scraper.py` has no `--dry-run`. Argparse `--help` exits before main network/DB work, but only after imports and dotenv loading. Import/configuration and uncaught CDN schedule-fetch failures remain candidate early-error classes until the real Railway stderr is available.

A concrete startup defect was reproduced locally through the real entrypoint: a truthy `PROXY_URL` without `@` raises `IndexError` at the proxy-display expression around line 956. The controlled probe used `PYTHON_DOTENV_DISABLED=1`, empty database URLs and a dummy unauthenticated proxy URI. It exited 1 before the database banner. No DB connection or CDN call was reached. This proves the local defect, **not** that Railway has that proxy value or that it caused the production failures.

Unified CDN mode also reports failed-game counts without turning that count into a nonzero process exit; a green wrapper can therefore be insufficient output evidence. Do not fix this by placing advanced stats scraping on Railway.

## Reconciliation requirements from completed post-sync checks

See [post-sync checks 1–2](nba-post-sync-checks-2026-11-02.md).

- The props transfer passed distributed source/local parity: 24,000 rows, 12 windows, all 19 columns equal.
- Local injuries trail remote by report date; remote game lines have additional PKs.
- Historical quote, game-map and team-linkage gaps exist even when local matches the current source.
- Core source/derived stats max dates agree at 2026-06-13, so a local refresh alone cannot repair the observed source watermark.
- The separate remote archive is excluded locally and has no indexes. Current-table misses are not proof that historical quotes require rescraping.
- `sync_local_db.py`'s commence-time incremental cursor can miss later inserts for earlier games when future fixtures set the max date. Full refresh commits truncation before export. Neither is an acceptable blind reconciliation shortcut.

### Completed reconciliation preflights and local smoke

- Live schema/key preflight confirms props/game-lines PK `staging_id`, injuries PK `id`, and `game_id_map` PK `external_game_id`; `nba_game_schedule` is absent on both databases.
- Same-ID source/local comparison of 600 rows found zero differences in link IDs, API event IDs or commence times. The sampled tail's missing game/team IDs already existed on the canonical source. Different newest-row samples must not be treated as copy divergence.
- The existing safe props sync copied `(279217733, 279218733]` as one local-only 1,000-row batch. The complete smoke journal is `logs/sync/nba_reconcile_props_smoke_20261102.json`; the runner's fresh-connection verification matched all 19 fields, with zero missing/extra/mismatched rows. A separate verification runner is checking this exact target again. The remaining catch-up has not been run.
- The real schedule `--canary` passed locally: 1,206 competitive games, one excluded 006 game, six TBD/unassigned games and zero missing UTC starts. This is local-source validation, not a Railway egress proof or evidence that the database schedule is populated.
- Existing missing-schedule migration: `database/migrations/20260924_nba_game_schedule.sql`. Apply only after explicit DB/DDL approval, then run the existing `nba_schedule.py --ingest` path. No migration was applied here.

### Verified owner-run source catch-up

The owner ran the standalone bounded CDN catch-up. Independent canonical post-run checks found 90 games, 2,440 player rows and 180 team rows for October 20–November 1, with identical game sets and exactly two team rows per game. Basic player/team source dates now reach 2026-11-01. This verifies resulting state, not attribution of every row to that single invocation. No CDN rerun is currently indicated for that window.

For reference, the executed command surface is:

` .\venv\Scripts\python.exe -X utf8 src\scrapers\nba_cdn_scraper.py --date-range 2026-10-20 2026-11-01 `

It writes canonical `DATABASE_URL`, not the local mirror, and does not call `stats.nba.com`. The agent did not launch the catch-up. Source watermarks and game membership were verified separately from process success.

Rolling-average catch-up accepts one `--date` at a time and needs the missing source dates processed, not merely yesterday. The post-CDN check still found basic player, team and advanced player averages at 2026-06-13. Opponent-allowed catch-up exposes runtime-relative `--days-back`/`--lookback-buffer`, not explicit start/end dates; its advanced-stats inner join must be satisfied first. Advanced-stat scraping remains local-only. Do not represent a CDN basic-stats catch-up as restoring advanced history or every derived feature.

Additional scraper-scope hazard: `nba_unified_scraper.py::scrape_advanced_stats()` calls `get_games_missing_advanced_stats()`, which selects every game with null team offensive rating ordered by game ID. Its missing-game selector does not accept/filter the CLI season, so passing `--season 2026-27` does not bound the advanced backlog, and `--advanced-limit` can choose oldest missing games rather than the requested new-season window. Do not recommend a blind advanced rerun; verify a targeted existing helper or repair its date/game-ID scope first.

## Superseded reconciliation-tool draft — do not integrate

The owner redirected this work to reuse the existing linker and restore remote automation. The separate tool/runbook draft below is preserved only in the isolated worktree and is not an approved replacement pipeline.

New files only:

- `scripts/reconcile_nba_data.py`
- a focused reconciliation runbook under `docs/operations/`

The tool should default to read-only inventory and a machine-readable repair manifest for the previously audited NBA tables. Reuse `scripts/sync_nba_props_by_id.py` for props-ID catch-up; do not duplicate its journal/pagination framework or silently change it. Validate existing schemas/keys, actual source/target identity, query/index safety and independent verification boundaries.

Separate plan categories:

1. **Bounded local catch-up:** new props IDs, game-line append/update reconciliation and injury refresh. Use explicit execution selection, staging/upsert, per-table checkpoints, row/batch/time caps, overlap guards and readback. Do not truncate targets or rely on future commence maxima.
2. **Source pipeline repair:** record failed production jobs, missing source/derived watermarks and exact required repairs. Production code changes, writes, source catch-up and deployment require separate approval after traceback/deployment verification.
3. **Linkage repair:** explicitly scope missing game/player/team identity and ambiguous matches; never assign the home team simply because its name resolves. Do not invoke the existing unbounded linker as a catch-all fix.
4. **Historical archive recovery:** prepare source/schema/size/deduplication/restore-verification requirements. No automatic 171M-row transfer, unindexed repeated scans, index creation or destructive deduplication.

The optional local execution path must not report “everything reconciled” when source-side, archive or policy blockers remain. Production changes, archive bulk recovery, pruning, index changes, training and deployment stay disabled pending separate decisions.

## Required next evidence / owner gates

1. Railway authentication, deployment metadata and the bounded linker-error log check are complete. The initial critical Daily Stats scraper traceback and actual in-container egress remain unresolved; its final optional DFS error is not the initiating failure.
2. The later owner message authorizes proceeding with the reconciliation scope and asks for the linker command. Implement and independently validate the bounded entrypoint before handing off a real write command; do not recommend the old unbounded incremental command merely with a smaller `--limit`.
3. Turn verified root causes into separately approved source changes, rerun the original failing paths in a safe bounded mode, and verify actual output/freshness—not only process exit.

### Current existing-linker acceptance and automation priority

- Applied the repaired existing `src/processing/nba_linker_local.py` to the main checkout as an uncommitted local edit; no push/deploy. Only explicit `--limit` uses the bounded path; no-limit production callers remain legacy/unbounded and have not been fixed by deployment.
- Independent review approved the corrected independent identity passes. Main checkout validation: six existing tests, 45 disposable guarded checks, Ruff, compile and whitespace checks passed. Differential Pyright found zero introduced errors (66 baseline errors versus 63 current legacy errors).
- The corrected-core real CLI preview completed in enforced read-only mode over 1,000 rows, with one CDN request and zero proposed fills. All rows already contained game/player/team IDs and referred to November 3 future fixtures. The 1,000 conflict/unverified flags are not proof those IDs are wrong; the checked future game IDs lack completed-game stats, and historical identity acceptance remains unresolved. No remote rows were updated.
- Current production verification does not establish that the old SSL/network failure or Daily Stats first-stage failure is repaired. Restore the scheduled remote chain and verify its actual outputs. Rolling averages, source scraping and sync should not become routine owner-run manual procedures; inspect their existing schedules and dependencies first. The scheduled local-mirror sync wiring/reachability has not yet been verified.
- Current preview evidence: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-reconcile-incident/existing-linker-live-preview.current.json`. It binds the tested working-copy core revision; the later main-checkout scalar/type cleanups were verified separately with the full controlled checks and Pyright comparison.

No root-cause or repair completion is claimed from truncated alert tails.

## Detailed DB evidence

- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-reconcile-incident/job-executions-summary.md`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-reconcile-incident/job-executions-metadata.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-reconcile-incident/job-executions-evidence.json`
- Exact 20 SELECT statements, schema, activity and overlap calculations are retained in the associated scratch audit artifacts; scratch files can expire.
