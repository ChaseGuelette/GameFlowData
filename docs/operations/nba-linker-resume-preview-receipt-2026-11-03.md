# NBA linker production resume preview receipt — 2026-11-03

- Captured: `2026-11-03T12:49:23-05:00`
- Target: canonical production `DATABASE_URL`, executed from the native repository venv.
- Scope: SELECT-only checkpoint read plus one 500-row resume dry-run batch. No database/checkpoint writes.
- Subprocess timeout: 90 seconds.

## Production checkpoint state

Exact query:

```sql
SELECT state_name, replay_floor, cursor, high_water, first_unresolved_id, updated_at
FROM public.nba_props_linker_state
WHERE state_name = :state_name
LIMIT 1;
```

Result (`state_name = 'production'`):

```json
{"cursor": 264597335, "first_unresolved_id": null, "high_water": 281173287, "replay_floor": 264587335, "state_name": "production", "updated_at": "2026-11-03 17:45:20.566567+00:00"}
```

## Resume preview

Command:

```text
venv/Scripts/python.exe src/processing/nba_linker_local.py incremental --resume --batch-size 500 --max-batches 1 --dry-run
```

Captured `LINKER_SUMMARY`:

```text
LINKER_SUMMARY {"batches": 1, "conflict_count": 500, "cursor": 264597835, "dry_run": true, "fill_candidates": {"game_id": 0, "player_id": 0, "team_id": 0}, "first_unresolved_id": null, "high_water": 281173287, "identity_ready": true, "mode": "resume_preview", "reference_available": true, "reference_unverified_count": 500, "rows_scanned": 500, "rows_updated": 0, "status": "incomplete"}
```

## Diagnosis

- The production query and linker preview executed successfully; this is not an execution failure.
- The preview inspected exactly 500 rows and found 500 conflicts plus 500 reference-unverified rows.
- It proposed no identity fills and performed zero updates.
- `status = incomplete` is expected after the single bounded batch; the CLI returned exit code 2 for that incomplete dry-run status.
- The durable production checkpoint was not advanced.
