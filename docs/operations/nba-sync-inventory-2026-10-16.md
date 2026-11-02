# NBA-only Supabase-to-local sync inventory — October 16, 2026

## Kick off immediately

- You: the NBA-only full refresh is already running; let it finish and preserve the summary. Do not launch a second sync or the superseded `--sport all` command.
- This chat: remote inventory, input mapping and local schema/CASCADE preflight are reviewed. No registry or source-code edits were needed. Review the sync summary after it finishes, then continue S2 data checks against the refreshed mirror with canonical remote spot checks.
- Other chat: S1 code is delivered in its isolated worktree; preserve it pending [coordinator acceptance](nba-s1-coordinator-review-2026-10-16.md). No MLB copying is needed.

### Recorded launch command — already running; do not launch again

```powershell
Set-Location 'C:\Users\Chase\Projects\GameFlowData'; .\venv\Scripts\python.exe scripts\sync_local_db.py --sport nba --full --state-file logs/sync/nba_full_20261016.json
```

After completion, inspect the persisted summary:

```powershell
.\venv\Scripts\python.exe scripts\sync_local_db.py --status --state-file logs/sync/nba_full_20261016.json
```

No sync was launched by the coordinator. It was subsequently owner-launched: the journal records NBA/full mode starting at 21:51:23 UTC (17:51:23 EDT), status `running`; the coordinator observed active sync processes and no failed tables recorded at review time. If a table fails, share the summary before restarting a broad full refresh. Do not run real-data S1 validation against tables being rewritten.

Status: remote inventory, dependency mapping and local schema preflight complete; owner-run NBA full refresh in progress. All 15 registered NBA tables exist; no registry additions or blocking recorded-field schema mismatches were found. Transfer completion and post-sync data readiness are not yet verified.

## Active investigations

| Lane | Agent | Scope | State |
| --- | --- | --- | --- |
| Remote inventory | `sa-0-e156eccb` | Remote-only SELECT metadata, registry presence/columns/PK/FK and estimated sizes | Report read; metadata accepted |
| Historical input mapping | `sa-1-75b39aa1` | Three exact train/range/injury source files | Report read; all 11 visible physical inputs registered |
| Context input mapping | `sa-2-b357fe01` | Bounded context/priors/position physical source mapping | Report read; no extra required training tables |
| Local schema compatibility | `sa-0-2e70c0fa` | Local metadata only compared to recorded remote columns/keys; not a source-data audit | Report read; no blocking recorded-field drift or extra local CASCADE targets |

Delegation batch: `deleg_45338bb3`.

## Authority correction

Chase confirmed that the local database has not been synced in months. Prior local counts and local missing-table observations describe a stale mirror, not current Supabase. S2's newer local diagnostic probe was stopped before execution: zero local and zero remote S2 data queries. Code-source findings remain separate evidence.

Do not infer remote absence, fresh data, or model readiness from the stale local mirror. Remote schema presence also does not prove data freshness or historical point-in-time validity.

## What the existing sync code actually does

Source: `scripts/sync_local_db.py`, inspected before preparing a replacement command.

- `NBA_TABLES` is an explicit registry, not discovery of every Supabase table.
- Without `--tables`, `--sport nba` selects only NBA registry entries. With `--tables`, explicit names override the sport selection, so every passed name must be checked for NBA scope.
- Full mode truncates/reloads local targets with `CASCADE`; remote is the export source. The main context has not executed a sync.
- Some NBA reference/source tables use full mode even without `--full`.
- Incremental date-max copying can miss changes to older rows; a fresh max date is not proof of historical parity.
- `--dry-run` is not a strictly read-only metadata probe: local database/schema initialization happens before its per-table branch.
- Existing local tables are accepted by `ensure_table_schema` without altering their columns. A data refresh is not a schema migration; table-name coverage alone cannot certify that a months-old mirror will load current exports.

## Required classification before launch

1. Required NBA source/reference/feature inputs: must be accounted for in the registry and present remotely, or explicitly blocked.
2. Optional/current-slate inputs: list separately; do not silently add a nonexistent roster or unapplied schedule table.
3. Derived outputs such as predictions, samples, paper bets and user data: not automatically required for historical training; exclude unless a named consumer needs them.
4. MLB tables: excluded from this sync plan.
5. Large raw NBA odds: identify size, required history and column/schema compatibility before recommending an unbounded copy. No broad row-count scans or indexing are authorized by the metadata audit.

## Reviewed remote findings

Six SELECT-only metadata queries used the configured `DATABASE_URL`, verified a non-local Supabase domain and a read-only transaction, then rolled back/closed. One NullPool connection; connect/statement/lock/idle timeouts 8s/10s/1s/30s. No row scans, sync, writes, table creation or MLB data reads.

All 15 NBA registry tables exist remotely, with primary keys and valid configured incremental date columns:

| Group | Registered tables |
| --- | --- |
| Reference/history | `players`, `teams`, `game_id_map`, `league_priors_history`, `rapidapi_injuries`, `player_position_history` |
| Player stats/features | `player_game_stats`, `player_game_advanced_stats`, `player_average_game_stats`, `player_average_advanced_stats` |
| Team stats/features | `team_game_stats`, `team_average_game_stats`, `team_allowed_by_position` |
| Historical odds inputs | `raw_player_props_combined`, `raw_game_lines_staging` |

The inspected train/range/injury consumers use eleven physical inputs, all registered. Context snapshots are runtime joins, not a missing physical snapshot table. The coordinator checked single-date batch query dependencies too; no extra physical input surfaced there.

Remote `nba_game_schedule` and `nba_roster_snapshots` were absent as public base tables. Do not register nonexistent tables to satisfy a planned current-slate design; their migration/ingestion remains a separate publication task. Prediction/sample output tables, live/archive alternatives and other candidate tables are not automatically historical training inputs. No MLB tables belong in this sync scope.

### Size and capacity, not a duration promise

- Remote `raw_player_props_combined`: planner estimate about 108 million rows; total relation size 63.59 GiB including indexes.
- Remote `raw_game_lines_staging`: about 3.1 million estimated rows; total relation size 1.32 GiB including indexes.
- The workstation C: volume reported about 825.1 GiB free during coordinator review. This is a point-in-time measurement, not guaranteed final headroom.
- Relation bytes are not CSV transfer bytes or a guaranteed local footprint. The large NBA props table is why an NBA-only full refresh can still be a long job.

### Launch preflight result

The NBA-only registry plan was parsed from source and verified to select 15 tables with zero `mlb_` names. The existing `--sport nba` selection is sufficient; no registry/source edits are needed. A full refresh is the owner-requested catch-up mode for this stale mirror, but is not launched here. No MLB-specific large-full-refresh override is needed for this NBA selection.

The local follow-up executed four metadata-only SELECTs in a verified read-only transaction: all 15 local tables exist and match recorded remote ordered names, types/UDTs, nullability, identity attributes and primary keys. No local touching FKs or outside-table CASCADE children were found, versus 17 recorded remote FK edges. This is expected to reduce local loading dependencies, not proof of production constraint parity.

No old column-order/type copy blocker was detected. Remote character lengths and numeric/datetime precision/scale were not recorded, so full parity for those attributes is not certified. No local data was read or changed by this schema check. Actual transfer success and source-data freshness remain separate checks.

### Evidence paths

Temporary detailed artifacts are under `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-sync-inventory-20261016-173908/`: `remote-inventory.json`, `remote-inventory.md`, `training-inputs.md`, `context-inputs.md`, `local-metadata.json` and `local-metadata.md`. The coordinator read and reviewed the reports. Reviewed facts above are the durable summary; metadata is not a data-freshness audit.

## Read next

- [Repair spec and S1 handoff](nba-historical-repair-spec-2026-10-16.md)
- [Historical research report](nba-historical-readiness-2026-10-16.md)
- [NBA rebuild wiki](../understanding/nba-rebuild.md)

This report is a sync-scope/readiness page, not permission to change production data or launch a long job.
