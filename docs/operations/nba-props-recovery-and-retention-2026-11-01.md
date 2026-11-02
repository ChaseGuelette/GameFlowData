# NBA raw-props recovery and retention — November 1, 2026

## Conclusion

The standalone sync tool is implemented and has passed a bounded real transfer, resume, completed no-op, idempotent replay and overlap-lock validation. The local `public.raw_player_props_combined` table now holds 2,000 validated smoke rows; the full recovery has not been launched. Remote access remained read-only. No pruning, permanent DDL, archive sync or scheduler change was performed.

## Verified state

| Target | Evidence | Result |
|---|---|---|
| Local combined table before transfer validation | `EXISTS`, exact `COUNT(*)`, catalogs | Existed with exactly **0 rows**; 40,960 total relation bytes at that initial inspection |
| Remote combined table | PostgreSQL catalogs, not an exact count | Estimated 105,473,888 rows; 68,276,404,224 total bytes including indexes |
| Remote archive table | PostgreSQL catalogs, not an exact count | Exists; estimated 171,185,488 rows; 46,195,671,040 total bytes; 20 columns; no indexes |
| Active sync/export work | Bounded activity probes | No active local sync/COPY or stale remote sync/export session surfaced at probe time |

The combined table has a `staging_id` primary key plus game/player indexes. No index on `commence_time`, `snapshot_time`, `market_last_update` or `inserted_at` was found. The tested one-day date query plans a sequential scan and sort; LIMIT does not make that query cheap.

The archive is separate historical storage. Copying only the combined table must not be described as restoring all retained NBA quote history. Historical coverage and consumers' archive access remain unverified.

## Prepared query and measured result

This reference query is now implemented by `scripts/sync_nba_props_by_id.py` as a bounded COPY/stage/upsert workflow. Use psycopg2 parameters; never substitute identifier or credential text into it.

```sql
SELECT staging_id, api_game_id, api_player_name, bookmaker, market_key,
       outcome_label, line, odds_american, commence_time, home_team, away_team,
       inserted_at, market_last_update, bookmaker_last_update, bookmaker_name,
       snapshot_time, game_id, player_id, team_id
FROM public.raw_player_props_combined
WHERE staging_id > %(last_staging_id)s
  AND staging_id <= %(upper_staging_id)s
ORDER BY staging_id
LIMIT %(batch_size)s;
```

Actual remote smoke parameters: `last_staging_id=174552600`, `upper_staging_id=279202070`, `batch_size=1000`.

- Returned exactly 1,000 rows and the expected 19 columns in **385.66 ms**.
- IDs were strictly increasing, from 174552601 through 174553600, within the requested bounds.
- EXPLAIN selected `Limit -> Index Scan` on `raw_player_props_combined_pkey`.
- Transaction was verified read-only, with 8-second statement and 1-second lock timeouts, then rolled back and closed.
- No fetched quote rows were persisted. One small successful page is not an end-to-end bulk throughput or restore benchmark.

For a future transfer, capture a fresh upper primary key at the start. Advance the cursor only after durable page output or local commit. A high watermark bounds new IDs but does **not** provide one consistent snapshot of mutable rows across independently resumed transactions, nor cover late commits below the cursor. Linkage corrections to old rows require explicit reconciliation.

## Recovery choices

### Resumable keyset transfer — recommended operational path

The separate raw-props-only tool leaves the broad NBA sync unchanged:

- metadata/preflight by default; explicit execution gate;
- one remote read-only connection and bounded primary-key pages;
- explicit columns, source IDs and all original timestamps preserved;
- durable checkpoints and per-page counts/integrity metadata;
- no broad COUNT, OFFSET pagination, permanent DDL, destination/source TRUNCATE or CASCADE (only its own temporary staging table is cleared);
- local writes only with separate explicit import approval, staged/upserted and verified;
- actual row/page counts, incomplete state and failure reason recorded;
- fixed high watermark and documented consistency/reconciliation limits.

Bounded validation is recorded below. Chase launches the large transfer.

### Standalone targeted sync tool

`scripts/sync_nba_props_by_id.py` implements the combined-table-only path without changing the broad sync. It requires existing, schema-compatible remote and local `public.raw_player_props_combined` tables and explicit `DATABASE_URL` plus `LOCAL_DATABASE_URL` values. It never creates the database/table, truncates either table, writes remotely, resets sequences, prunes rows, or includes `raw_player_props_archive`.

The default command is a strictly read-only preflight on both databases. It validates connection identity, requires the local server to resolve to loopback, rejects a source/target endpoint collision, compares all 19 ordered columns/types/nullability/identity metadata and the exact `staging_id` primary key, and reads only catalog estimates plus indexed first/last IDs. It does not create a checkpoint or directory:

```powershell
.\venv\Scripts\python.exe scripts\sync_nba_props_by_id.py --preflight
```

Checkpoint status is offline: it does not load credentials, connect to either database, or create files. Relative state paths are resolved from the repository root, independent of the launch directory:

```powershell
.\venv\Scripts\python.exe scripts\sync_nba_props_by_id.py --status --state-file logs/sync/nba_props_by_id.json
```

Execution requires the explicit gate below. The default is one 10,000-row page; `--max-batches 0` is the owner-requested full run through the upper `staging_id` captured when the journal is created. A session advisory lock prevents two instances of this tool from overlapping, and the tool refuses cheaply identifiable active legacy local work on the same table.

```powershell
.\venv\Scripts\python.exe scripts\sync_nba_props_by_id.py --execute --batch-size 10000 --max-batches 0 --state-file logs/sync/nba_props_by_id.json
```

Each page is exported by bounded keyset `COPY`, checked for the exact header and strictly increasing IDs, copied into a connection-local temporary table, and upserted by `staging_id`. All non-key fields are updated only when different. Full-row stage-to-local equality is verified before the local commit. The checkpoint is atomically replaced only after that commit; a crash between commit and checkpoint is safe to replay because the upsert is idempotent. `committed_source_rows` counts processed source-page rows, while each `changed_rows` value counts inserted or changed destination rows and may be zero on replay. No whole-table local count is claimed.

A completed journal is a no-op for its same frozen upper ID. To sync later source IDs, use a new explicitly named state file and, when selecting a subset, explicit `--start-after-id`/`--end-id` bounds. Existing malformed journals fail closed, and supplied bounds must match an existing journal's original range. `--max-batches` may change between resumes. Timeouts are configurable but capped (`--statement-timeout-seconds` and `--lock-timeout-seconds` at 120 seconds; `--connect-timeout-seconds` at 120 seconds).

This is resumable keyed mirroring, not one consistent remote snapshot. The captured upper ID does not cover late commits below the cursor, later updates to already passed rows, remote deletes, or archive-only history. Reconciliation, archive coverage, pruning, index/sequence maintenance, training, and deployment remain separate operations.

### Executed validation

Validated through the actual CLI and database connections on November 1, 2026:

- Read-only preflight passed and created no checkpoint; remote/local ordered schema, types, nullability and PK matched.
- First capped run inserted 1,000 rows and paused at ID 174553600.
- A second invocation resumed automatically, inserted another 1,000 rows and completed the declared smoke range at ID 174554600.
- Rerunning the completed journal performed no transfer.
- Replaying the same 2,000 source rows using a separate journal reported zero inserted/changed rows on both pages; the local table remained exactly 2,000 rows with 2,000 distinct IDs.
- Full comparison found all 19 values equal across all 2,000 source/local rows: zero mismatched rows/cells or duplicate IDs.
- An independently held advisory lock caused a competing execute invocation to fail before creating its checkpoint or changing data.
- No stale tool sessions remained on either database.
- Parent checks: compile, scoped Ruff, 12 offline CLI safety checks and three independently rerun rollback/checkpoint failure-injection probes passed. The 11 existing sync regression tests passed; existing pytest-asyncio deprecation warnings remain. No repository unit tests were added or edited.
- Independent SELECT-only readback passed: local total exactly 2,000; remote/local smoke intervals each had 2,000 distinct IDs and all 19 fields equal; the completed smoke checkpoint matched actual rows. The default full-run checkpoint was confirmed absent. Bounded validation does not certify full-table coverage or historical training readiness.

The smoke checkpoint is `logs/sync/nba_props_by_id_smoke_20261101.json`. **Do not use it for the full run:** its upper ID is intentionally capped. Use the full-run command above with the separate `logs/sync/nba_props_by_id.json` journal. That first full invocation captures a fresh upper ID and safely replays the existing smoke rows before continuing. Later interruptions resume by rerunning that same full command. A completed full journal does not silently extend to subsequently appended source rows.

Source IDs are preserved and the local sequence is deliberately not advanced: this destination is a downstream modeling mirror, not an independently inserting writer. No sequence values were changed during validation.

### Table-only native dump — valid alternative

A native `pg_dump` can provide a consistent one-time table snapshot and compressed artifact without the sync script's preliminary exact COUNT. It still transfers the full selected table; it does not automatically solve long-running COPY/network issues or offer page-level resumability. Parallel dump workers do not split one large table into independently resumable chunks. Verify supported connection mode and server/client compatibility before launch; no passwords in command arguments or output.

PostgreSQL 18.1 `pg_dump` and `pg_restore` are installed under `C:/Program Files/PostgreSQL/18/bin/`, but are not on PATH. No dump or restore has been attempted. A combined-table-only dump does not include the separate archive's history.

## Recurring pruning: review and activation gate

An existing implementation is `src/orchestration/archive_old_props_job.py`. It moves old snapshots into `raw_player_props_archive` and deletes the inserted IDs in the same transaction. Defaults are seven-day snapshot retention, 50,000 rows per batch and 500,000 rows per run. Its docstring claims a daily schedule, but the inspected scheduler only showed a job-name mapping, not an invocation. Actual deployment scheduling remains unverified.

Do **not** enable the legacy defaults unchanged:

1. Define hot retention, completed-game eligibility, retention clock, NULL handling, historical research requirements and archive-aware consumer behavior.
2. Verify archive schema/key/deduplication compatibility. The legacy `SELECT r.*, now()` insertion relies on positional schema compatibility.
3. Moving rows to an archive in the same Supabase database is hot-table management, not a reduction of total retained database history. Decide whether the long-term archive remains there or moves to verified external storage.
4. Require a dry-run default, explicit execute gate, overlap prevention, statement/lock timeouts, capped rows/batches/runtime, and truthful backlog/failure metrics.
5. Independently verify exact counts for each approved destructive candidate set. Do not launch a giant historical DELETE or archive catch-up.
6. Preserve verified, restorable historical quote data before any permanent purge. A successful file write is not restore verification.
7. Any new raw-props index needs separate approval and a concurrent build path. Ordinary vacuum reuses deleted space; do not promise immediate file shrink or schedule VACUUM FULL on the live giant table.

The targeted recovery tool and its bounded local validation were owner-approved and implemented. Retention-policy selection, existing archiver hardening/scheduler wiring, index creation and production activation remain separate decisions.

## Evidence

Scratch artifacts can expire; durable key results and query text are preserved above.

- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_local_state_evidence.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_export_preflight.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_keyset_query.sql`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_keyset_smoke.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_sync_live_validation_20261101.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_smoke_independent_readback_20261101.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_props_sync_offline/results.json`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/probe_nba_props_sync_review.py`

Related code: `scripts/sync_local_db.py` (pre-export committed truncation and generic failure accounting), `src/orchestration/archive_old_props_job.py` (existing retention mechanism).

Native dump/snapshot/vacuum behavior was checked against official PostgreSQL 18 documentation; Supabase connection guidance was checked against its official backup/restore and Postgres connection documentation. Only the explicitly capped sync transfer was exercised; no full dump or full-table transfer was launched.
