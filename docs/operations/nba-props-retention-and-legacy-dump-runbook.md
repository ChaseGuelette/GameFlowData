# NBA props retention and legacy dump runbook

This cutover keeps `public.nba_player_props_inference` as the recent-quote source, retains permanent `t60_v1` training lines in `public.nba_player_prop_feature_lines`, and leaves both legacy raw tables intact until a separately reviewed destructive migration.

## November 12 scraper follow-up — read before the original cutover steps

The scraper/consumer source is published through `ccb292e1`, and Railway deployment `0bc6d98d-1f50-45ec-8993-c5fc8551ce5d` is successful with native scheduler startup verified. Live captures use eight supported markets, no DFS, half-hour slots and change-only quote history; no new activation environment variable is required. The first native capture/unchanged-capture reduction remains unverified; the successful 11:42 ET edge refresh is consumer evidence, not scraper evidence.

- **Keep the completed backfill.** Handoff 155 records the approved keyset backfill as complete. Historical/backfilled rows keep their original clocks and capture semantics; change-only mode affects new live ingestion only. It does not retroactively deduplicate/delete those rows or reduce the existing retained population. Do not rerun the backfill to enable the new scraper.
- **RPC cutover is now receipt-aware.** `20261112_nba_change_only_capture.sql` is applied/read back in production. The three non-DFS RPCs now read current quote/pair views backed by inference rows and real capture confirmations. **Do not blindly apply the older full `20261112_nba_props_inference_rpc_cutover.sql` afterward:** it would replace those functions with history-based readers and lose withdrawal/confirmation behavior. Preserve existing signatures and authenticated/service-role grants.
- **DFS retirement is a confirmed remaining dependency.** Guarded read-only production metadata confirms `get_dfs_lines(date)` still directly reads `raw_player_props_combined`; it does not read the archive. The newer migration intentionally does not drop it. Account for that dependency and obtain approval for a narrow retirement step before removing combined data; do not rerun the three superseded RPC replacements just to retire DFS. The three current-view RPCs no longer directly reference either legacy raw table. Receipt: `logs/receipts/sql_metadata/20261112_nba_rpc_capture_metadata_readback.json`. No function/table drop is authorized by this note.
- **Keep the existing 21-day pruner and materialization gates.** New `nba_props_live_captures` stores compact observation receipts, not another recent-odds table. Its quote IDs have no blocking FKs: ordinary quote deletion does not automatically protect or clean receipt references. Permanent T-60 lines retain separate capture evidence; confirm evidence preservation before calling end-to-end retention acceptance complete. The 3 AM prune registration is deployed, but a successful scheduled prune has not been verified in this scraper lane.
- **Receipt cleanup is not yet implemented.** Coordinate bounded receipt retention with the pruning owner; do not add a competing quote pruner or delete the latest empty receipt while its old quotes could reappear through fallback. Source rollback must keep producer/consumers coherent rather than merely reverting to legacy snapshot writes.
- **Dump/drop gates remain unchanged.** Chase chose to preserve only `raw_player_props_combined`, using `--skip-archive`; `raw_player_props_archive` is intentionally not being dumped. Validate the selected combined-table dump/list/hash/manifest and independently recheck dependencies/counts before separately approved drops. Successful scraper deployment does not prove either dump or deletion completed.

This checkpoint supersedes the three-function replacement instructions in section 1 below. Keep that section as the original migration history, not the next command to run.

## Safety gates

- Apply database changes only through the normal owner-approved migration process.
- The backfill, dump, and any future table drop are owner-launched operations.
- Never add or run a non-concurrent index build on `raw_player_props_combined`.
- This runbook does not authorize dropping, truncating, or modifying either legacy raw table.
- The reviewed keyset envelope is operationally bounded, but it is not proof that no legacy row arrived out of order. The complete local dumps preserve the source before any future drop.

## 1. Original RPC cutover migration — superseded by the scraper follow-up above

Migration: `database/migrations/20261112_nba_props_inference_rpc_cutover.sql`

The migration replaces `get_game_commence_times(text[])`, `get_sportsbook_lines(date)`, and `get_sportsbook_lines_by_games(text[])`, restricts execution to `authenticated` and `service_role`, and removes `get_dfs_lines(date)`. It does not drop either raw table.

After application, inspect function definitions and grants. Confirm all three replacement bodies read `nba_player_props_inference`, have the same return contracts, and use the 30-second function timeout.

## 2. Backfill the reviewed recent window

Default planning is DB-free and does not load `.env`:

```powershell
python scripts/backfill_nba_props_inference_recent.py
```

Reviewed receipt window and ID envelope:

- IDs: `staging_id < 290239015 AND staging_id >= 268000000`, descending keyset order.
- ET dates: October 23, 2026 through November 12, 2026 inclusive.
- UTC bounds: `[2026-10-23T04:00:00Z, 2026-11-13T05:00:00Z)`; the unequal offsets account for the November 1 DST transition.

Owner-launched one-batch smoke:

```powershell
python scripts/backfill_nba_props_inference_recent.py --execute --start-id 290239015 --floor-id 268000000 --max-batches 1
```

Verify the printed scanned, in-window, rejected-clock, inserted-after-dedupe, timeout, and next-cursor metrics. Inspect the checkpoint under the user's `GameFlowArchives` directory. Continue only after checking target rows and the checkpoint:

```powershell
python scripts/backfill_nba_props_inference_recent.py --execute --start-id 290239015 --floor-id 268000000 --batch-size 20000 --max-batches 1000 --max-runtime-seconds 1800
```

Re-running the same command resumes from the committed checkpoint and preserves cumulative totals. Use explicit `--window-start` and `--window-end` timestamps with offsets for any different window; do not derive a fixed UTC offset for an ET range that crosses DST. Keep the 20,000-row batch only if the one-batch smoke is healthy; otherwise retain the 5,000-row default.

## 3. Verify serving and compact-line coverage

Before relying on retention:

1. Verify dashboard game-time fallback and sportsbook-line paths for authenticated users.
2. Verify the three replacement RPCs return the expected columns and paired prices.
3. Verify daily `t60_v1` coverage for points, rebounds, assists, and threes in `nba_player_prop_feature_lines`.
4. Investigate unresolved or missing compact-feature rows. The prune job intentionally retains those core-market inference rows.

## 4. Deploy and observe 21-day pruning

The retained scheduler filename is `src/orchestration/archive_old_props_job.py`, but its behavior is now bounded pruning of `nba_player_props_inference`. It runs at 3:00 AM ET, uses `(commence_time, staging_id)` ordering, skips locked rows, and deletes:

- non-core markets after 21 days;
- core points/rebounds/assists/threes only when a matching `t60_v1` compact feature row exists.

Observe initial runs for deleted core/non-core counts, runtime, lock timeouts, and overlap skips. Do not add a second retention schedule.

## 5. Prepare and create local legacy dumps

The default path is outside the repository: `~/GameFlowArchives/nba-props-2026-11-12`.

DB-free tool/path plan:

```powershell
python scripts/dump_legacy_nba_props.py
```

Remote read-only preflight (loads project environment, checks tool versions, remote sizes, free space, and output nonexistence without creating the output directory):

```powershell
python scripts/dump_legacy_nba_props.py --preflight
```

Owner-launched dumps:

```powershell
python scripts/dump_legacy_nba_props.py --execute
```

To preserve only `raw_player_props_combined` and deliberately skip the disposable archive table, include `--skip-archive` in both preflight and execution:

```powershell
python scripts/dump_legacy_nba_props.py --preflight --skip-archive
python scripts/dump_legacy_nba_props.py --execute --skip-archive
```

Use `--force` only after independently deciding that existing dump, restore-list, and manifest files may be replaced. The helper creates separate custom-format dumps for:

- `public.raw_player_props_combined`
- `public.raw_player_props_archive`

It passes credentials to child processes through PostgreSQL environment variables, never prints the URL, includes an owned `staging_id` sequence when one exists, runs and validates `pg_restore --list`, hashes each dump with SHA-256, and writes a credential-free JSON manifest. It refuses transaction-pooler port 6543 because `pg_dump` needs a direct/session connection. It performs no remote mutation.

## 6. Independently verify dump receipts

Before considering a destructive migration:

1. Confirm every selected dump and its restore-list file, plus `manifest.json`, exist outside the repository. For Chase's approved `--skip-archive` path, this means the combined-table dump/list only; an archive dump is not required.
2. Recalculate SHA-256 and compare with the manifest.
3. Run `pg_restore --list` independently on each selected dump and inspect the named table objects.
4. Recheck local free space and preserve an additional copy if required.
5. Compare remote table identity, counts, and minimum/maximum `staging_id` with a restored local copy where feasible. Treat estimated size metadata as supporting evidence, not row-count proof.
6. Record the exact verification date, tool versions, and operator.

## 7. Future destructive drop is a separate change

Only after the dumps and restored contents are independently verified may a new migration be prepared to drop the legacy tables. Before that migration is approved, independently verify candidate row counts and all table/function/view dependencies again. No drop is included or authorized by this cutover.
