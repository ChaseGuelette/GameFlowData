# NBA local training-data sync readiness

## Scope and ownership

Chase confirmed the bounded advanced catch-up completed. This lane owns local mirror/data readiness; slice 8 is being worked on elsewhere and is not modified here. No sync, derived job, migration, scheduling, training, or deployment was executed during this audit.

## Verified evidence

SELECT-only canonical/local preflight, November 2, 2026 EST:

- Canonical player/team basic stats and player basic/advanced and team averages reach November 1. Local averages stop at June 13.
- Three canonical November 1 games have advanced rows with positive possessions for all 65 played players sampled. The two games present locally have zero advanced matches for 39 played players. The third canonical game, `0022600156`, is absent locally despite a matching latest basic-stat date.
- Injuries reach November 2 remotely versus November 1 locally.
- All 13 inspected support tables exist on both endpoints; logical ordered columns and primary keys are compatible. No touching local foreign keys were found for the inspected tables.
- Endpoint freshness for opponent-allowed, position history, league priors and game map remains unverified. Advanced completeness is sampled, not whole-season certification.
- Exact SQL/results: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_sync_preflight_20261103.json`. Scratch may expire; decisive findings are preserved above.

## Existing code and actual blocker

`scripts/run_nba_local_sync.bat` executes only the props follower. `scripts/sync_local_db.py` mixes support tables and both large raw odds tables in its NBA registry. Several support strategies remain full-refresh with `TRUNCATE ... CASCADE`; ordinary incremental date logic can skip same-date gaps and historical corrections. Generic dry-run can initialize local schema. Do not use broad `--sport nba` or generic full refresh as a shortcut.

A matching max date is insufficient: the absent sampled game demonstrates an actual same-date gap. Reuse current sync infrastructure but add an explicitly selected, non-destructive support-table path with primary-key UPSERT and bounded replay rather than appending only beyond the local watermark.

## Approved implementation (committed and integrated; owner data jobs remain)

Chase approved this sync implementation and confirmed slice 8 continues in a separate tree. Writer scope is limited to `scripts/sync_local_db.py` and a dedicated sync runbook in `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-support-sync`, based on `a07cd1e2`. No transfer, scheduler, model or deployment action is authorized by that approval. The prior automation repairs are now committed in main at that base; do not overwrite them with older worktree snapshots.

The implemented `--nba-support` mode defaults to database-read-only preview and requires explicit `--execute` for UPSERT. It enforces explicit <=31-day windows, per-table row budgets, schema/key/endpoint checks and protected diagnostic journals. The unchanged 11-test sync suite, scoped Ruff, compilation, help, whitespace checks and independent review passed. The actual main-checkout read-only preview using normal credentials passed all 13 tables, selecting 24,087 rows and committing zero. Source is integrated locally; no intentional transfer job or push was performed.

Reviewed source SHA-256 before integration: `89c5a2623ac24b62f2560f6dea9100618f1c0ff9a0b286b765ba31163da33b4c`. Preview receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-support-preview-20261103T020956Z-final/nba_support_preview_receipt.json`. Source committed as `af093150` and integrated into local main as `bf740ad6` with exact reviewed source bytes; no push. Operator runbook is now `docs/operations/nba-support-table-sync.md`.

The destination test run exposed a dotenv integration regression: an imported `main()` reloaded DATABASE_URL after an existing test deliberately removed it, potentially entering legacy initialization/sync. The run timed out after nine tests; no pytest process remained afterward. A SELECT-only session audit found no recent lingering sync/COPY/init sessions on either endpoint. Cumulative counters cannot attribute when writes happened, so zero prior writes cannot be proven. No speculative restore was attempted. Env loading is corrected to the CLI entrypoint only and committed as `cf9325d9`. All 11 destination tests pass with DB connections forbidden, independent narrow acceptance passed, and the actual main CLI read-only preview passed. Final source hash: `0559f9a937c5388d02d21c00979e418c06debd7f2d7f36551d4f01b6c3f8f6b4`. Main preview: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_support_preview_acceptance_main_20261102_14222_3146.json`. Session audit: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_timeout_session_audit.json`.

### Live preview reveals source coverage gaps

The inclusive October 3–November 2 preview selected 24,087 rows across 13 tables and committed zero. It selected 2,493 player basic rows, 2,491 matching advanced rows and 184 team basic rows, but only 104 player-basic averages, 104 player-advanced averages, eight team averages and zero opponent-allowed rows. These are exact selected row counts, not yet a classified expected-output deficit; player eligibility and output-key policy still need checking.

This supersedes any implication that matching current endpoints established canonical derived completeness. Support-sync plumbing is not the sole remaining data-readiness task. Next, inspect bounded canonical output-key coverage and prepare the existing derived catch-up commands for Chase to launch; copying alone cannot supply source rows that are absent. Do not repeat the successful advanced scrape blindly. Initial live receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-support-preview-20261103T020543Z-33/nba_support_preview_receipt.json`.

Further SELECT-only coverage classified the current source range as October 20–November 1, season `22026`. Player basic/advanced averages and team averages exist only on November 1; October 20–31 has no output rows. All 176 earlier team/game keys are candidate gaps; all 184 team/game keys lack opponent-allowed output. The exact player-output anti-join timed out and was not broadly retried. The actual all-positive-minutes/team-field advanced source guard subsequently returned **zero missing pairs** through November 1. Historical position coverage remains blocked: **195 of 2,070** positive-minute player/game keys lack any position-history row with `snapshot_date < game_date`. Latest snapshot on/before October 19 is October 16; presence alone does not establish non-null groups for remaining rows. Do not backdate current snapshots. Evidence: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_derived_gap_results_full.json` and `nba_canonical_preflight_20261103.json` in the same scratch directory.

1. Harden the existing sync with an opt-in support-table UPSERT mode: no database/schema bootstrap or TRUNCATE, read-only preflight, exact schema/PK checks, explicit table/window selection, overlap for corrected rows and dedicated journals. Fail closed on unsupported keys or unsafe bounds. Preserve existing unrelated modes.
2. Date-backed sources/averages use explicit bounded replay. Advanced stats select by verified game IDs/source-season relationship, not `created_at` alone; updates to old rows must propagate. Small references use explicit keyed UPSERT with deletion semantics deferred, not silently inferred.
3. Keep props on their existing follower and game lines on the reviewed ID-window path. Do not run them through generic timestamp sync.
4. Verify remaining source-table coverage and proposed volume/disk scope before handing Chase exact preview/execution commands. Chase launches all transfers.
5. After copying, compare exact game/player keys and relevant values across the approved window, then run a bounded read-only feature-consumer check. Historical acceptance and training approval remain separate, with slice 8 owned elsewhere.

No renewed advanced scrape or unbounded canonical rebuild is justified by these checks alone. Current canonical endpoints and three sampled games passed, but the full bounded preview exposed sparse derived output. A targeted output-key audit and owner-launched catch-up are the next data gates.

## Owner commands and order

Run in PowerShell from `C:\Users\Chase\Projects\GameFlowData`. Derived scripts write canonical `DATABASE_URL`; support sync then copies remote to `LOCAL_DATABASE_URL`. No owner jobs below were run by this lane.

### 1. Average catch-up can run now

The full builder updates basic and advanced player/team averages. Advanced source coverage passed, so no separate basic-only pass is necessary. Replay all 13 source dates, including November 1, and stop on error:

    $d = [datetime]'2026-10-20'; while ($d -le [datetime]'2026-11-01') { $target = $d.ToString('yyyy-MM-dd'); .\venv\Scripts\python.exe -X utf8 src\processing\populate_average_stats_incremental.py --date $target; if ($LASTEXITCODE -ne 0) { throw "Average catch-up failed on $target; stop here." }; $d = $d.AddDays(1) }

Verify output keys/values afterward, not MAX(date) alone. Do not run the full Daily Stats wrapper: its later durable-linker stage remains separately gated.

### 2. Historical positions before opponent derivation

Extract the missing player/date keys and locate trustworthy retained historical position evidence. Restore only genuinely evidenced snapshots, or use the separately approved historical-policy lane's missing-source/cohort handling. Do not invent historical availability or backdate today's position snapshot. Recheck eligible non-null position groups and advanced completeness; current position/league snapshot jobs are not historical repairs.

Only after the position gate is cleared, run:

    .\venv\Scripts\python.exe -X utf8 src\processing\backfill_opponent_allowed_incremental.py --start-date 2026-10-20 --end-date 2026-11-01

This reads relevant full-season history but writes target dates only. Running it before position coverage is settled can produce incomplete aggregates.

### 3. Canonical support tables to local Postgres

After canonical repair, define the allowlist once in the same PowerShell session:

    $tables = @('players','teams','game_id_map','league_priors_history','rapidapi_injuries','player_game_stats','player_game_advanced_stats','player_average_game_stats','player_average_advanced_stats','player_position_history','team_game_stats','team_average_game_stats','team_allowed_by_position')

Preview (DB-read-only; diagnostic journal only):

    .\venv\Scripts\python.exe -X utf8 scripts\sync_local_db.py --nba-support --tables $tables --start-date 2026-10-03 --end-date 2026-11-02 --max-table-rows 50000 --state-file logs/sync/nba_support_owner_preview.json

After reviewing a successful preview, launch the actual remote-to-local transfer:

    .\venv\Scripts\python.exe -X utf8 scripts\sync_local_db.py --nba-support --execute --tables $tables --start-date 2026-10-03 --end-date 2026-11-02 --max-table-rows 50000 --state-file logs/sync/nba_support_owner_execute.json

Existing journals are refused; use fresh descriptive filenames for reruns. The October 3 lower bound includes pre-opening position snapshots; reference tables copy full bounded key sets. This 31-day replay is not certification of all historical training inputs. You may mirror interim stats/averages after step 1, but it remains incomplete for training until position/opponent coverage is resolved and replayed. Props stay on the dedicated ID follower; game lines stay on the bounded ID-window path. Do not use generic `--sport nba`, `--full`, or broad raw-odds transfer for this repair. Compare source/local keys/values and exercise a bounded real training-feature consumer after copying.

## Related context

### Consumer inventory checked during implementation

`src/models/features/nba/training_loader.py:301-369` directly consumes player/team game stats, player position history, player basic/advanced averages, team averages and opponent-allowed rows. `src/models/features/nba/injury_context.py:217-258` additionally consumes injuries. These eight non-quote physical sources are covered by the selected support-table allowlist. The other five support tables serve reference/upstream needs rather than direct reads in this inspected loader.

The training loader also reads both raw odds tables (`training_loader.py:371-398`), which remain on their separate transfer paths. Support sync therefore does not certify quote coverage or historical clock semantics. This inventory is not acceptance of slice 8, a model-policy change, or evidence that every training consumer has passed a real-data check.

- [Automation repair](nba-automation-repair-2026-11-02.md)
- [Pipeline boundaries](../understanding/nba-production-data-pipeline.md)
- [Prior post-sync findings](nba-post-sync-checks-2026-11-02.md)
