# NBA Injury DB-First Correction — November 4, 2026

## Current status

**Code implemented and offline-verified. Schema changes, DB import/linking and Railway deployment have NOT been executed.**

Normal flow is now:

`RapidAPI → existing rapidapi_injuries → existing linker fills nullable IDs`

No staging table or linking table is introduced. The existing `job_executions.metrics` field records atomic per-request completion, including empty/missing reports, for DB-backed resume. The production path does not require workstation paths, capture manifests, local output directories or filesystem checkpoints.

## Changed code

- `src/scrapers/rapidapi_injury_backfill.py`: retains the existing Railway scraper entrypoint; writes directly to DB, uses payload game date, fails on real API errors, and has optional saved-capture import and DB-derived season dates.
- `src/scrapers/nba_injury_db.py`: shared bulk upsert/observation retention and transactional checkpoint functions. Same injury table and existing unique index.
- `scripts/run_nba_injury_recovery.py`: its normal CLI now delegates to that DB path. Preview is default; `--execute` enables writes. Old file-only launch flags are rejected rather than silently reused. Legacy file helpers remain internal for historical diagnostics, not the production route.
- `src/processing/link_injury_data.py`: reuses existing matching; fills NULL player/team IDs without overwriting known IDs; adds bounded game-ID matching by exact game date/team. It no longer automatically guesses missing teams from future/past nearest games or runs DDL in normal/dry-run modes. Normal runs no longer require writing an unmatched CSV; that report is explicit/optional.
- `scripts/run_nba_injury_pilot.py` and its capture helper remain diagnostic evidence utilities. They are not the production ingestion command.

The existing Railway chain in `src/orchestration/lines_job.py:283–293` already calls the DB scraper followed by the linker. `railway.toml` runs the existing scheduler. No new service, scheduler registration or Railway volume is required by this code path. Linux deployment itself remains unverified until the code is published and a Railway run is observed.

## Narrow additive schema change

Migration: `database/migrations/20261104_nba_injury_db_capture.sql`.

Add two nullable columns to `rapidapi_injuries`:
- `game_id text`: filled by the existing linker’s new date/team step.
- `source_observations jsonb`: retains original records, request date, capture identity and receipt time on the same injury rows.

No existing column, row, primary key or unique index is removed. The existing unique key continues grouping date/team/player/status/reason; distinct captured observations are retained inside that row instead of discarded. Identical response content for a request date reuses a capture identity. New `report_date` values are the payload game dates expected by existing game joins; source request date stays in the observation metadata.

Preflight confirmed the table/unique key and `job_executions.metrics`; the two new columns were absent. The migration/explicit bootstrap has bounded locks and performs column readback. Apply it before deploying the new writer. Its nullable additions remain compatible with the currently deployed old writer.

## Verification performed

- Twelve existing offline tests passed; scoped Ruff and compilation passed.
- Real completed-download import preview passed: **567 request dates, 61,725 observations, 410 invalid statuses retained as raw evidence, three dated missing-report responses**. Zero API calls and zero database access.
- Controlled writer/linker boundary probe passed: payload game dates preserved; original request/capture metadata retained; DB rows and checkpoint share a transaction; checkpoint failure propagates/rolls back; missing report is not treated as healthy; dry-run game matching contains no UPDATE; existing player/team IDs are preserved.
- Those controlled checks do not claim real SQL writes or a live match rate. The real schema/import/link/deploy verification is still pending owner execution.
- Focused read-only code review: PASS, no concrete blocker in the writer/checkpoint/linker path. The coordinator subsequently verified the optional diagnostic-CSV change, content-based capture identity and schema-only dispatch through scoped checks. This is code verification, not evidence of a live DB import or Railway deployment.

## Owner execution sequence — not run by the agent

From `C:\Users\Chase\Projects\GameFlowData` in PowerShell. These write to canonical `DATABASE_URL`; `--import-captures` does not call RapidAPI.

1. Apply/read back the additive schema (explicit DB operation):

` .\venv\Scripts\python.exe scripts/run_nba_injury_recovery.py --ensure-schema --schema-only --execute `

Alternatively apply the reviewed SQL migration directly with your normal DB migration tooling.

2. Import the already downloaded history without rescraping:

` .\venv\Scripts\python.exe scripts/run_nba_injury_recovery.py --import-captures C:\Users\Chase\GameFlowArtifacts\nba-injury-recovery-20261104 --max-dates 600 --execute `

3. Link the table’s missing player/team IDs and eligible game IDs:

` .\venv\Scripts\python.exe src/processing/link_injury_data.py --game-batch-size 1000 --max-game-batches 100 `

Actual table counts/selected capture checkpoints should be read back after these writes. Unresolved identities and malformed statuses remain visible, not guessed away.

## Railway operation after publishing the code

Use the existing injury scraper/linker chain, with `DATABASE_URL` and `RAPIDAPI_KEY` from Railway environment settings. Do not copy the workstation capture folder or local cohort files to Railway for normal operation.

For an explicitly owner-launched historical API backfill from Railway, the portable command is:

` python scripts/run_nba_injury_recovery.py --seasons 22023 22024 22025 --max-dates 600 --execute `

That derives dates from historical game records in the DB, with no props or minimum-minutes filter. Previously completed DB ingestions are skipped; current-day API jobs refresh rather than treating existing rows as final. This historical API command is not needed for the already downloaded data once its import succeeds.

Do not deploy before the migration is applied. No commit, push, Railway deployment, migration or ingestion was performed in this coding pass. Source publication/edition acceptance for T-60 training remains a separate downstream question; DB ingestion does not invent historical availability.
