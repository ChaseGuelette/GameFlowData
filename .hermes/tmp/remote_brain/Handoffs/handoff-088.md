---
title: Handoff 088 — Railway Lines Scraper intermittent linker failures
type: handoff
domain: handoffs
status: completed
owner: Chase
effective_date: 2026-05-25
tags: [handoff, railway, lines-scraper, nba, known-issue]
---

# Handoff 088 — Railway Lines Scraper intermittent linker failures

> Part of [[Handoffs]]

**Date**: 2026-05-25

## Summary

Investigated weeks of intermittent Railway `Lines Scraper` failures. The job is not fully down: production `job_executions` shows many successful runs interleaved with failures, and the current remote DB had zero `raw_player_props_combined` rows with `player_id IS NULL` in the last 30 days by the time of investigation. The failures consistently occur in `src/processing/nba_linker_local.py incremental` during the `Linking Props (Incremental)` step, but the stored error text is truncated before the actual traceback.

A small diagnostic patch was made to `src/orchestration/lines_job.py` so future failures preserve useful stderr tail context in Railway/Discord alerts. Keep this as an active work item until the next failure reveals the actual DB/Python exception or the linker is redesigned.

## What Was Done

- Queried recent remote `job_executions` rows through the GameFlow SQL-runner pattern, not Supabase MCP.
- Confirmed last-7-day pattern for `lines_job.py`: frequent successes plus intermittent failures; e.g. 2026-05-25 had 14 failures and 79 successes.
- Confirmed recent failure examples report 10k-20k unlinked rows before failing at `Linking Props (Incremental)`.
- Confirmed current remote `raw_player_props_combined` last-30-day unlinked count was 0 after later successful runs.
- Checked `raw_player_props_combined` schema/index support:
  - table is about 156M live tuples, about 43 GB table / 60 GB total;
  - primary key/unique index exists on `staging_id`;
  - indexes exist on `game_id`, `(game_id, market_key)`, and `(player_id, game_id)`;
  - no index includes `commence_time`.
- Railway MCP/CLI auth was unavailable in this session (`Unauthorized. Please run railway login again.`), so raw Railway logs could not be fetched.
- Patched `src/orchestration/lines_job.py` locally so `run_command()` logs child stdout head first and stderr tail last, making scheduler/Discord's `stderr[-500:]` more likely to capture the real traceback.
- Also fixed local Windows command splitting in `lines_job.py`: `shlex.split(command, posix=(os.name != "nt"))` prevents `C:\Users\...\python.exe` from being mangled into `C:Users...`.

## Decisions Made

- Do not add an index or run DDL on `raw_player_props_combined` during this investigation. Any index on this huge table must be plan-first and concurrent/dashboard-safe.
- Treat this as an intermittent linker/database contention or timeout issue until the next full stderr tail proves the exact exception.
- Preserve the logging patch as the immediate low-risk next step before deeper linker redesign.

## Blockers and Open Questions

- **Active work item:** Deploy the `lines_job.py` logging/Windows-splitting patch and monitor the next Railway `Lines Scraper` failure for the real traceback.
- Railway auth is currently expired for the agent environment; raw Railway deploy logs require `railway login` or another authenticated path.
- The stored `job_executions.error_message` is capped to 500 chars, and old alerts captured progress text rather than the underlying exception.
- Root cause remains unproven until a future failure captures the actual DB/Python error.
- Potential durable fixes to evaluate after traceback capture:
  - reduce linker batch size/window;
  - change linker to target only rows from the latest scrape/snapshot rather than all last-30-day unlinked rows;
  - add a carefully approved concurrent index supporting the linker predicate;
  - split/link by newly inserted `staging_id` ranges or snapshot metadata.

## Validation Captured

Commands run locally after the patch:

```powershell
.\venv\Scripts\python.exe -m py_compile src\orchestration\lines_job.py
```

Result: passed.

```powershell
.\venv\Scripts\python.exe src\orchestration\lines_job.py --live --props-only --dry-run
```

Result: passed; dry-run preserved the expected live props scraper and incremental linker commands.

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_nba_linker_local.py -q
```

Result: 6 passed, 1 warning.

A broader `tests/test_pipeline_resilience.py` run had unrelated failures because `check_dependency()` falls back to live `job_executions` rows and sees real recent successes.

## Files to Read on Resume

- `src/orchestration/lines_job.py`
- `src/processing/nba_linker_local.py`
- `src/orchestration/scheduler.py`
- `src/db/client.py`
- [[Known-Issues]]

## Recommended Next Steps

1. Review and deploy the `src/orchestration/lines_job.py` logging patch.
2. Re-auth Railway CLI/MCP if raw deployment logs are needed.
3. Wait for the next failure or manually run a bounded props-only job only if needed.
4. Use the newly captured stderr tail to identify the exact exception before changing linker logic.
5. If the exception confirms query timeout/contention, design a safe linker follow-up that avoids broad last-30-day scans on `raw_player_props_combined`.
