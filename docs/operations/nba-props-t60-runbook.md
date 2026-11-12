# NBA compact T-60 prop lines runbook

November 12 change-only ingestion code is delivered locally: [NBA quote-volume scope](nba-inference-quote-volume-reduction-2026-11-12.md). Its separate `20261112_nba_change_only_capture.sql` migration is now applied to canonical production and independently read back, following Chase's explicit approval. The no-environment-variable follow-up is pushed as `ccb292e1`; backend runtime/live capture remains separately verified. Preserve separately owned retention changes; this delivery does not authorize cleanup or deployment.

### Change-only live confirmation evidence (local code; activation gated)

The live producer stores changed numeric lines/prices as immutable inference rows and records actual successful reobservations in `nba_props_live_captures`. For live captures, the T-60 loader expands eligible pre-cutoff receipts and the selector ranks by that genuine confirmation clock/provider metadata while retaining original quote clocks and source row ID. New nullable compact evidence fields are `capture_id`, `capture_received_at`, `capture_market_last_update`, and `capture_bookmaker_last_update`; historical rows keep them null. The `t60_v1` numeric-selection/eligibility contract remains unchanged and expanded-response parity was checked; do not use a post-cutoff last-seen update to create earlier availability. No historical compact rows were rewritten.

Apply/read back the new migration before deploying the corrected backend: change-only capture is the normal `lines_job.py --live` path, with no new environment variable. The historical path remains unchanged. Preserve authoritative receipts and coordinate any producer/consumer rollback with the retention owner. Current materializer safety caps and existing backlog recovery are unchanged.

## Status and scope

Implemented and integrated into the main checkout; remote migration committed and read back on November 5, 2026. The historical backfill has not been launched. These changes do **not** activate a production cutover: Railway/Vercel remain on deployed code until the owner publishes/deploys this slice. The original `src/scrapers/player_prop_scraper.py` and existing raw/archive tables are unchanged. Compact lines are numeric features, not executable paired odds history.

Migration artifact: `database/migrations/20261105_nba_compact_prop_lines.sql`

## Frozen consumer schema contract

`public.nba_player_prop_feature_lines` exposes these required columns exactly:

- `game_id text`
- `player_id bigint`
- `market_key text`
- `line numeric`
- `bookmaker text`
- `game_date date`
- `season_id text`
- `selection_policy_version text` — constrained to literal `t60_v1`
- `decision_time timestamptz`
- `commence_time timestamptz`
- `source_snapshot_time timestamptz`

Additional identity/clocks/provenance are `team_id`, `api_game_id`, `api_player_name`, `source_kind`, `requested_snapshot_time`, `market_last_update`, `bookmaker_last_update`, `observed_at`, `fetched_at`, `task_id`, and `source_staging_id`.

Canonical grain is `(game_id, player_id, market_key, selection_policy_version)`. No FK targets the current-season schedule. Consumer readers must pin `selection_policy_version = 't60_v1'` and enforce their existing date/as-of boundary.

`public.nba_player_props_inference` mirrors the 19 legacy raw column names with its own identity PK and adds the fixed fields:

- `requested_snapshot_time`
- `source_snapshot_time`
- `source_kind`
- `observed_at`

Its `snapshot_time` is the actual provider/observation snapshot and equals `source_snapshot_time`; it is not the requested historical date. `observed_at` is live receipt availability for live rows and historical wrapper availability for historical rows. Fetching an old response today does not make today its source availability.

`public.nba_prop_capture_tasks` records resumable historical event/T-60 tasks. `success` and `empty` are terminal/resumable; `error` and `unresolved` retry. Per-event compressed responses and discovery responses live under the caller-selected `--state-dir`, outside the DB and repository.

## Fixed `t60_v1` selection policy

Initial feature markets are points, rebounds, assists, and threes. Allowed books are DraftKings and Pinnacle.

1. Require linked canonical game/player identity, a real commence time, and a positive numeric line.
2. Decision time is exactly `commence_time - 60 minutes`.
3. Require actual source availability, local availability, market update, and bookmaker update (when present) to be no later than decision time and pregame.
4. Reject contradictory numeric lines for the same game/player/book/market/source snapshot.
5. A paired Over/Under quote is not required for the numeric feature. The inference table still retains raw sides/prices for live quote consumers.
6. Rank by newest valid `source_snapshot_time`, then newest `market_last_update`, then deterministic DraftKings before Pinnacle, then source staging ID.
7. Historical requests retain both requested T-60 and actual wrapper snapshot. Live coarse samples retain their actual age; neither path claims an exact unavailable snapshot.

## Migration

Applied through the isolated SQL runner with 2-second lock and 30-second statement timeouts. An earlier version that modified the active legacy checkpoint hit a lock timeout and rolled back completely. The applied version is wholly additive and uses `nba_props_inference_linker_state`, leaving the existing production checkpoint and constraint unchanged.

Verified post-commit from fresh connections:

- all four new tables exist (two data tables, capture-task journal and independent linker checkpoint);
- the compact required columns and `t60_v1` constraint exist;
- inference uses its own identity sequence;
- RLS is enabled;
- `authenticated` has read-only access to the inference quote table and no write access;
- compact feature lines, capture tasks and inference checkpoint remain backend (`postgres`) only;
- the new `nba_inference` checkpoint starts at cursor/replay floor 0;
- the migration contains no legacy raw/archive DDL/DML and preserves the production checkpoint.

Rollback-only constraint checks accepted a valid T-60 feature and Yes/No inference rows, and rejected missing players, late observations and duplicate compact grains. No synthetic rows persisted. Applied migration SHA-256: `354beb611257af8551794d42e394a4e92d7f00b2dde5fafa7587cf0def6dc6f5`.

Independent verification exercised the integrated Python functions against remote Postgres: compact feature read and isolated checkpoint read passed; actual `upsert_feature_lines` wrote 1 controlled row, returned 0 on identical retry, and rejected an equal-rank changed line. The transaction was rolled back and a fresh read confirmed zero test rows. This verifies schema/consumer plumbing, not real historical provider coverage. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_compact_remote_independent_verification.json`.

## Historical backfill (owner-launched)

### November 6 validation: stored data valid, coverage partial

#### Alias repair MVP — prepared, not executed

Root cause: the 13 needed mappings already existed in ignored `data/linker_data/player_mappings.csv`, while both new loaders looked for nonexistent root `linker_data/player_mappings.csv`. The other legacy CSV has conflicting Lonnie Walker IDs. `src/processing/nba_player_aliases.py` now provides only the independently verified aliases to both historical and daily contexts. Each override requires the expected canonical ID/name; conflicting manual aliases remain ambiguous rather than overwritten.

Canonical `players` records and exact game/team appearances verified these mappings:

| Provider alias | Canonical name | Player ID | Evidence game |
|---|---|---:|---|
| B.J. Boston Jr | Brandon Boston | 1630527 | 0022400186 |
| Cameron Thomas | Cam Thomas | 1630560 | 0022300084 |
| Carlton Carrington | Bub Carrington | 1642267 | 0022400155 |
| Gregory Jackson | GG Jackson | 1641713 | 0022300807 |
| Herb Jones | Herbert Jones | 1630529 | 0022300925 |
| James Huff | Jay Huff | 1630643 | 0022400293 |
| Lonnie Walker | Lonnie Walker IV | 1629022 | 0022300930 |
| Marcus Morris | Marcus Morris Sr. | 202694 | 0022301014 |
| Moe Wagner | Moritz Wagner | 1629021 | 0022300947 |
| Mohamed Bamba | Mo Bamba | 1628964 | 0022300939 |
| Nicolas Claxton | Nic Claxton | 1629651 | 0022300067 |
| Ron Holland | Ronald Holland II | 1641842 | 0022400278 |
| Vincent Williams Jr | Vince Williams Jr. | 1631246 | 0022400819 |

`scripts/repair_nba_prop_aliases.py` defaults to a real read-only preview; `--execute` replays only unresolved player-alias tasks through the existing atomic importer. Missing caches fail closed and provider HTTP is prohibited. Existing task API-cost metadata survives zero-request replay. The script sets/checks transaction-local 15-second statement and 2-second lock timeouts: the pooler ignored connection-level timeout options during the first preview attempt.

Verified full preview: **649 tasks, 647 ready, 2 blocked, 29,855 feature rows ready**. Provider HTTP calls and API credits were zero; the unresolved queue remained 649 before/after. The two blocked events contain contradictory same-time Pinnacle quotes, not unresolved aliases: `aef0c26b3670edaf7fb230056fb4adc7` (game `0022500399`, player `1642273`, points) and `6579d9162df09172d12f48eecc5220fb` (game `0022600165`, player `1631255`, rebounds). Do not pick an arbitrary contradictory line to force completeness. The eight original non-alias failures and 97 errors are outside this script's queue.

Owner-run repair, before local sync:

```text
.\venv\Scripts\python.exe scripts\repair_nba_prop_aliases.py --start-date 2023-10-24 --end-date 2026-11-04 --state-dir C:/Users/Chase/.gameflow/nba-props-t60 --report C:/Users/Chase/.gameflow/nba-props-t60/alias_repair_execute.json --execute
```

Expected from the verified snapshot: 647 successful event imports and two unresolved quote-conflict events; exit code **2** reports those remaining conflicts, not rollback of successful events. Check actual output/receipt before syncing; reruns select only remaining alias failures. No repair writes, local sync, commit, push or deployment were launched by the agent. Existing relevant tests: 22 passed; Ruff passed; Pyright had zero errors (one benign event-callback unused-function warning after the guard fix).

Preview receipts in Hermes scratch: `nba_alias_repair_preview_20261106.json` and `.verification.json`; canonical identity evidence: `nba_t60_alias_final_receipt.json` and `nba_t60_alias_exact_query_receipt.json`.

#### Baseline before alias repair

Read-only remote validation and local cached-discovery reconciliation matched the exact expected task-ID set. In the October 24, 2023–November 4, 2026 window, the compact table contains **148,426 rows, 3,370 canonical games and 579 players**. Of 4,144 known discovered events: 3,370 succeeded (81.32%), 20 were empty, 657 remained unresolved and 97 errored. The old pilot error is superseded by its successful replacement and is excluded from those counts. One discovery snapshot (`2025-03-20T21:00:00Z`) is absent from the cache, so the known-event denominator is not proof of exhaustive provider discovery.

Stored-row checks found zero duplicate canonical grains, missing required IDs, invalid lines, wrong policy/book/market, T-60 alignment violations, post-cutoff clock violations or Eastern game-date mismatches. Representative indexed game/date/team mappings matched across the four seasons; this is not an all-row independent identity certification or model certification.

| Season | Compact games | Known discovered events |
|---|---:|---:|
| 22023 | 1,176 | 1,355 |
| 22024 | 1,094 | 1,334 |
| 22025 | 987 | 1,334 |
| 22026 through November 4 | 113 | 121 |

Journal classification of unresolved events: 649 player-alias cases, 5 quote conflicts, 3 game-identity cases. All 657 have cached event payloads. Errors: 56 HTTP 404, 4 HTTP 502, 37 commence-time disagreements. Prioritize verified alias reconciliation and cached reprocessing rather than another full rescrape. No repairs or retry calls were made during validation.

The local target was absent at preflight. To copy the **validated but incomplete subset** now, run from the repository root:

```text
.\venv\Scripts\python.exe scripts\sync_local_db.py --tables nba_player_prop_feature_lines --full --start-date 2023-10-24 --end-date 2026-11-04 --state-file C:/Users/Chase/.gameflow/sync/nba_player_prop_feature_lines_2023-10-24_2026-11-04.json
```

Verified against the current sync implementation: explicit dates select window-mode staging/UPSERT, not TRUNCATE, despite the `--full` flag. It creates the missing local table and preserves its composite PK, columns and checks; the remote `task_id` FK is intentionally omitted locally, so the capture journal is not required. Only the compact table is selected. Expected source selection is 148,426 rows at this validation snapshot; recheck the journal after completion. The agent did not launch the sync. Missing games still require repair before full-cohort training acceptance.

Receipts: `logs/sync/nba_player_prop_feature_lines_preflight_20261106.json`; exact remote SQL/results in Hermes scratch `nba_t60_remote_validation_101ed7149858.sql` and `.json`; cached-discovery reconciliation in `nba_t60_completion_cache_receipt.json`.

Pilot correction: historical discovery snapshots may retain already-started games and replace their scheduled tipoff with an actual start timestamp. Discovery now ignores any event whose wrapper snapshot is at/after commence, so post-start listings cannot overwrite the pregame plan. The October 22 Boston/New York pilot was reproduced from the owner's cache: pregame commence `23:40:00Z` was formerly overwritten by post-start `23:35:56Z`; the corrected requested T-60 is `22:40:00Z`. Cache replay verified the fix without API calls. Reuse the same state directory and pilot command; no cache deletion is needed. Rejected paid event payloads are now retained for diagnosis but must pass the full validator on every read; a cached rejection is never accepted as training evidence.

Offline plan (no dotenv, DB, API, or state write):

```text
python scripts/backfill_nba_prop_lines_t60.py --start-date 2024-10-22 --end-date 2024-10-22
```

Bounded first real invocation after migration and parent review:

```text
.\venv\Scripts\python.exe scripts\backfill_nba_prop_lines_t60.py --start-date 2024-10-22 --end-date 2024-10-22 --max-events 1 --max-requests 10 --max-credits 50 --state-dir C:/Users/Chase/.gameflow/nba-props-t60 --execute
```

The script discovers events at multiple ET times (including morning and a following-day boundary query), filters by exact ET commence date, deduplicates provider event IDs, requests each event at provider T-60, verifies response event/teams/commence, reuses exact linker context, and writes compact rows plus task success in one transaction. Cached compressed responses permit mapping retries without another paid event request. A request/credit/event cap, unresolved identity, discovery failure, or event error returns a partial/nonzero result rather than a false success.

Do not point `--state-dir` inside the repository. Do not launch a broad range until the one-event receipt and credit accounting are reviewed.

Full owner command from the GameFlowData repository root (after reviewing the one-event pilot; the migration is already applied):

```text
.\venv\Scripts\python.exe scripts\backfill_nba_prop_lines_t60.py --start-date 2023-10-24 --end-date 2026-11-04 --max-events 10000 --max-requests 50000 --max-credits 500000 --state-dir C:/Users/Chase/.gameflow/nba-props-t60 --execute
```

The default state directory is `~/.gameflow/nba-props-t60`; the commands above share it so pilot responses are reused. Only strictly validated 2xx discovery/event payloads are cached. A validated discovery `data: []` is a confirmed empty snapshot, including off-season snapshots, but malformed/missing data and HTTP errors (including 422) are nonterminal failures. Resume terminal state is authoritative in `nba_prop_capture_tasks`; the local journal is diagnostic only. Credit capacity is reserved before every request and retry (1 discovery credit, at most 40 per event for four markets and up to ten books under this policy).

## Daily materialization

Offline default plan (previous/current ET day, max 50 events):

```text
python scripts/materialize_nba_prop_lines_t60.py
```

Daily bounded execution after the parent-owned inference writer and linker source are deployed:

```text
python scripts/materialize_nba_prop_lines_t60.py --max-events 50 --execute
```

Explicit date replay:

```text
python scripts/materialize_nba_prop_lines_t60.py --start-date 2026-11-04 --end-date 2026-11-05 --max-events 50 --execute
```

Only linked rows with actual availability at/before T-60 are selected. Post-cutoff observations cannot replace T-60 features. Processing is event-atomic: a contradictory or capped event rolls back with a nonzero partial result while other events may commit. UTC commence bounds are sargable, event discovery reads at most `max-events + 1`, and each event reads at most `--max-rows-per-event + 1` to detect partial work. An event-cap-only partial run prints an opaque `--after-event` cursor: repeat the same explicit date bounds with that cursor to continue, rather than selecting the earliest events again. Normal daily invocations start a fresh cycle.

The inference linker blocks materialization on unfinished windows or unresolved required game/player identities. A missing postgame player/team relation does not block verified pregame IDs. Its checkpoint is stored in the separate backend-only `nba_props_inference_linker_state` table.

## Safe inference retention

No automatic retention duration is activated by this lane. Dry-run is DB-free and required cutoff intent is explicit:

```text
python scripts/prune_nba_props_inference.py --retention-days 14 --scan-days 90 --max-batch-rows 5000 --max-batches 1
```

Execution example after owner chooses retention:

```text
python scripts/prune_nba_props_inference.py --before-date 2026-10-20 --scan-days 90 --max-batch-rows 5000 --max-batches 1 --execute
```

The DELETE targets only `nba_player_props_inference` inside the finite `--scan-days` UTC commence window. Old unsupported feature markets are deletable directly. For the four supported markets, only rows with a matching compact `t60_v1` feature grain are deleted; genuine unlinked/unmaterialized gaps are retained and reported. Legacy tables are never deleted.

## Consumer boundary

Compact rows are numeric historical features, not a full quote tape. Training/date/historical consumers may use them under the parent-owned as-of/eligibility changes. Live execution and OU pricing continue to require coherent inference quotes. Strict replay/backtests that require multiple historical executable quotes remain unsupported by this one-line compact table and must not be silently repointed.

The compact feature reader does not expose a T-60 row to an earlier decision. Before the cutoff/materialization, its existing missing-line zero fallback applies. Deployment is not certification of predictions made earlier than T-60; review the intended inference schedule before promoting a newly trained artifact. Historical rescraping and the forward quote-table migration do not change model mathematics or authorize promotion.

## Verification and delivery

- Integrated scoped changes into the main checkout without overwriting concurrent injury work. The original bulk scraper is unchanged. No commit, push or deploy was performed.
- Destination focused suite: 105 passed; two existing tests are not green: the line-source test explicitly requires the retired raw-table query, and the CDN discovery fixture is stale at the current date. The final compatible rerun deselected those two; no tests were changed or added.
- Scoped Ruff, TypeScript `--noEmit` and diff checks passed. Frontend lint had zero errors and five warnings on unchanged hook code.
- Controlled public-path probes passed for game-level eligibility, live/historical selector parity, exact identity rejection, post-cutoff rejection, pre-request credit limits, HTTP 422 error handling, independent checkpoint routing, required-identity completion gates, future games without postgame team stats, materializer continuation and capture resume after feature refresh.
- API-free full-range backfill, daily materializer and retention CLI plans executed successfully. No paid provider request or historical population job was run by the agent.
- The pilot's `--max-events 1` can deliberately produce exit 2 when another event remains. Full runs also return 2 for exhausted budgets or unresolved source/linkage work; inspect the printed reason rather than treating every partial as complete.
- `--max-credits` is a spending ceiling for that invocation, not a forecast. Repeating a command resumes committed captures and reuses valid cached responses; do not automatically loop failed/unresolved tasks indefinitely.

Suggested commit subject: `feat(nba): split T60 training props from short-retention inference quotes`
