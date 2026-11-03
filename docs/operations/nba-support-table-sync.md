# Bounded NBA support-table sync

Status: implemented and independently reviewed in the isolated `nba-support-sync` worktree; real read-only preflight passed. No transfer has been run. Source integration is a separate Git operation, not data acceptance. This mode is opt-in and changes no schedulers, BAT wrappers, model code, raw-odds pipelines, or legacy sync strategies.

## Why replay instead of incrementing MAX(date)?

Equal maximum dates do not establish completeness: a game can be missing within an already-present day. Historical and same-day averages can also be corrected. Advanced statistics have no game-date column, and `created_at` cannot detect corrections to existing rows. Explicit replay selects the entire requested interval from the canonical remote source, regardless of the local watermark, then inserts missing primary keys and updates existing keys. Local-only rows are retained; deletion reconciliation is deferred.

## Operator contract

Use `scripts/sync_local_db.py --nba-support` with an explicit duplicate-free `--tables` list and concrete `--start-date YYYY-MM-DD --end-date YYYY-MM-DD`. Both endpoints must already exist. `DATABASE_URL` (canonical, non-loopback source) and `LOCAL_DATABASE_URL` (distinct loopback target) are required through the normal project `.env` or process environment. Environment loading follows argument/status validation; import, help and status do not load credential files. This mode does not use a default local credential, create databases, or repair schemas. Neither URLs nor driver exception text are logged.

The default is a genuine database-read-only preview; `--dry-run` is an optional synonym. Only `--execute` enables local writes. `--execute --dry-run` is rejected, as is `--execute` outside this mode. Full refresh, unknown-table overrides, ID-window/bound/replay options, MLB and raw-odds selections are rejected before connections or journal writes.

The inclusive window must cover 1–31 calendar days; timestamps, implicit dates and reversed bounds are rejected. Internally the upper bound is the following midnight (UTC session timezone). Even reference-only requests require explicit dates. `--max-table-rows` defaults to 50,000 and accepts 1–100,000. Each source count wraps a selection limited to budget + 1; an unknown count or overflow fails the whole preflight. Every selected table must pass before any target mutation.

## Selection scopes

| Tables | Source selection |
| --- | --- |
| `player_game_stats`, `player_average_game_stats`, `player_average_advanced_stats`, `team_game_stats`, `team_average_game_stats`, `team_allowed_by_position` | `game_date` in the explicit interval |
| `player_position_history` | `snapshot_date` in the interval |
| `rapidapi_injuries` | `report_date` in the interval |
| `player_game_advanced_stats` | `EXISTS` matching **both** `player_id` and `game_id` in canonical `public.player_game_stats` within its `game_date` interval |
| `players`, `teams`, `game_id_map`, `league_priors_history` | Entire source reference key set, subject to the same per-table budget; the date interval does **not** filter these tables |

These are the only 13 permitted tables. No archive, raw-odds or MLB tables are queried by this mode. Scope and row counts are reported for each table, including the full-reference exception.

## Examples (operator execution, not run during implementation)

Run from the intended checkout with your Python environment and configured DB variables. These are single-line PowerShell commands. Use separate journal paths for preview and execution:

```powershell
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe scripts/sync_local_db.py --nba-support --tables player_game_stats player_game_advanced_stats player_average_game_stats player_average_advanced_stats --start-date 2026-11-01 --end-date 2026-11-01 --state-file logs/sync/nba_support_preview_2026-11-01.json
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe scripts/sync_local_db.py --nba-support --execute --tables player_game_stats player_game_advanced_stats player_average_game_stats player_average_advanced_stats --start-date 2026-11-01 --end-date 2026-11-01 --state-file logs/sync/nba_support_execute_2026-11-01.json
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe scripts/sync_local_db.py --nba-support --status --state-file logs/sync/nba_support_execute_2026-11-01.json
```

Review the preview before explicitly running execute. A later execute performs its own fresh preflight; preview is not a frozen transfer artifact. Reduce the date window or choose fewer tables if a row budget/timeout fails; reference selections cannot be reduced by narrowing dates. Do not bypass schema/routing failures with legacy full refresh.

## Safety and execution mechanics

- Both URL hosts and actual connected server address/port/database identities must establish non-loopback source, loopback destination and distinct endpoints. This is a routing guard, not a server allowlist; the operator must export the correct canonical source URL. Proxy/container routes whose actual target address is not loopback fail closed.
- Sessions start read-only, with 10-second connection, 30-second statement, 3-second lock and 60-second idle-in-transaction timeouts. Source remains read-only in one repeatable-read snapshot for selection counts and exports. Target preflight performs only SELECTs, including in execute mode. Preview creates no staging tables, COPY imports, advisory/row locks, schema changes or database writes. Ordinary SELECT relation locks are PostgreSQL's normal behavior.
- Preflight compares ordered logical column names/types/nullability and identity metadata, and identical nonempty ordered primary keys. Missing tables, generated columns, invalid/unready/deferrable primary keys and incompatible contracts are refused. No fallback initialization or full refresh exists in this mode.
- Execute ends the target read-only preflight, then uses one write transaction per selected table. Explicit columns are copied to temporary staging; the actual primary key (including composite keys) drives UPSERT. `OVERRIDING SYSTEM VALUE` preserves source ALWAYS identity values. Identity sequences are not reset/advanced: this is a canonical-key mirror, not a sequence-reconciliation tool.
- Selected/exported/imported/staged counts must agree. A primary-key join with JSONB whole-row logical value comparison must match every staged row before commit. Staging is preserved across commit for a second explicit target readback before table success is reported. Staging is dropped afterward; sessions close on success/error. A precommit mismatch rolls the table back. A postcommit failure cannot undo its already-committed transaction.

## Journals and recovery

Default dedicated journal: `logs/sync/nba_support_sync.json`. The generic default `logs/sync/sync_state.json` is refused. **Any existing destination journal is preserved and refused**, even a completed preview; archive it yourself or choose a fresh path. The initial journal is exclusively reserved to prevent another invocation replacing it. Preview writes only this diagnostic file, not the databases. `--nba-support --status` reads the selected journal without credentials or connections.

The receipt records operation, interval, selection scope, budget, counts, phase and per-table outcome. `rows_copied` measures staging import, not durable writes. `rows_committed`/`total_rows_committed` count selected rows with confirmed commits, not newly inserted rows or net changes. `rows_verified`/`total_rows_verified` count rows confirmed by postcommit readback. A failed precommit checkpoint leaves the commit `not_attempted` and the data transaction rolls back. A durable `prepared`/`pending` receipt left by process loss is inconclusive: a commit may have followed it, so inspect before replay. A commit call without a confirmed reply is `commit_unknown` with null committed count. A confirmed commit without completed readback is `committed_unverified`; cleanup or receipt errors after successful readback preserve verification as `committed_verified_error`. Failed runs exit nonzero and retain earlier confirmed table commits; they never issue an all-complete receipt. Not-yet-run tables may remain `ready`.

This is a diagnostic replay receipt, **not keyset resume**. After investigating a failure, rerun the same explicit bounds with a new journal path: canonical rows are replayed idempotently, including old/same-date corrections. Rows absent from the source are not deleted locally. Do not infer completeness from either a watermark or a copied-row count alone.

## Offline verification boundary

Validation uses the main project venv, the unchanged 11-test `tests/test_sync_local_db_dense_clv.py` suite, scoped Ruff, `py_compile`, CLI help, and one scratch guard probe with network and dotenv loading blocked and DB sessions stubbed. The probe exercises invalid requests, exact advanced selection, SELECT-only preview, composite-PK/identity SQL, pre/post-commit readback shape, journal preservation and connection-free status. Controlled stubs validate control flow/SQL shape, not PostgreSQL runtime behavior or real transfer correctness. Actual schema compatibility, driver COPY counts, performance and real data correctness still require a separately authorized bounded live preflight/execute.

## Accepted code and live preview

The existing 11 tests, Ruff, compilation, help and whitespace checks passed. Independent specification/quality review approved the implementation and the final failure-accounting corrections. Final reviewed Python SHA-256: `89c5a2623ac24b62f2560f6dea9100618f1c0ff9a0b286b765ba31163da33b4c`.

The real final-byte preview exited zero in 2.376 seconds for all 13 tables, with a 50,000-row/table budget and inclusive October 3–November 2, 2026 bounds. Schema/PK/routing checks passed; selected rows totalled 24,087; copied/committed rows were zero. Final receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-support-preview-20261103T020956Z-final/nba_support_preview_receipt.json`.

Source counts reveal a separate coverage gate: 2,493 player basic rows and 2,491 matching advanced rows versus only 104 rows in each player-averages table; 184 team basic rows versus eight team averages and zero opponent-allowed rows. A targeted canonical output-key/eligibility audit and owner-launched derived catch-up must precede any training-readiness claim. Do not treat current MAX(date), a passing preview, or this code approval as complete feature coverage. Actual write/COPY runtime remains unverified until Chase launches the bounded transfer. Runbook commands assume the reviewed code has been integrated into the intended checkout with its normal credentials.
