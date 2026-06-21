# NBA Lines Cron Failure Investigation and Future Fixes

> **For Hermes:** Use `subagent-driven-development` or the GameFlow implementation-worker lane to implement this plan task-by-task. Do not run schema changes, scheduler behavior changes, or production deploys without Chase approval.

**Goal:** Document the investigated failing “cron” alerts, the real root causes/risks found, and a future implementation plan to make the NBA lines/injury jobs quieter, safer, and easier to debug.

**Architecture:** The failing alerts are GameFlow Railway/APScheduler jobs, not Hermes cron jobs. Fixes should be split between scheduler orchestration (`src/orchestration/scheduler.py`), NBA lines orchestration (`src/orchestration/lines_job.py`), and RapidAPI injury schema/bootstrap cleanup (`src/scrapers/rapidapi_injury_backfill.py`). Runtime scraper paths should not perform recurring DDL, overlapping scheduled jobs should be prevented, and timeout/failure persistence should retain useful evidence.

**Tech Stack:** Python, APScheduler/CronTrigger, Railway deploy logs, Postgres/Supabase via SQLAlchemy, GameFlow `job_executions`, pytest/py_compile.

---

## Investigation Summary

### What the alert actually is

This is not a Hermes cron job.

Evidence gathered:

- Hermes `cronjob list` returned `count: 0` and no jobs.
- The alert maps to GameFlow scheduler/Railway execution:
  - scheduler: `src/orchestration/scheduler.py`
  - script: `src/orchestration/lines_job.py`
  - alert display name: `Lines Scraper (NBA deferred)`
- The “NBA deferred” wording is intentionally added by `DEFERRED_FAILURE_JOBS` in `src/orchestration/scheduler.py`:
  - `lines_job.py`: `NBA deferred — intermittent NBA lines linker failure is parked`

### Current production state when investigated

Railway latest deploy:

- deployment: `a594e777-94ba-4f5a-852e-c6d300266fb8`
- status: `SUCCESS`
- deployed: `2026-06-21 19:39:19 UTC`
- commit: `afb16a7de396e8d9542284dd5058037e0d9e0a4c`

After that deploy, `lines_job.py` was observed succeeding repeatedly:

- 19:45 UTC success
- 19:50 UTC success
- 19:55 UTC success
- 20:00 UTC success
- 20:05 UTC success
- 20:10 UTC success
- 20:15 UTC success
- 20:20 UTC success
- 20:25 UTC success
- 20:30 UTC success
- 20:35 UTC success

A full/parallel run at 20:00 UTC also succeeded:

- `Scraping Injuries (RapidAPI)`: completed in 14.6s
- `Linking Injury Player IDs`: completed in 42.6s
- `Linking Props (Incremental)`: completed in 85.2s
- total: 88.2s

### Recent failed `job_executions` evidence

Read-only query against `job_executions` showed recent non-success `lines_job.py` rows:

| started_at UTC | status | duration | persisted error summary |
| --- | --- | ---: | --- |
| 2026-06-21 16:00 | timeout | 2700s | only deferred label persisted |
| 2026-06-21 13:55 | failed | 2.6s | `EMAXCONNSESSION max clients reached in session mode - max clients are limited to pool_size: 15` |
| 2026-06-20 20:00 | timeout | 2700s | only deferred label persisted |
| 2026-06-20 16:00 | timeout | 2700s | only deferred label persisted |

The specific `EMAXCONNSESSION` failure appears related to the DB connection-pressure incident and likely improved after commit `afb16a7`, but the latent scheduling/schema/diagnostic issues below still remain.

---

## Issues Found

## 1. Scheduler overlap: full NBA lines jobs collide with every-5-min props-only jobs

### Evidence

In `src/orchestration/scheduler.py`:

- `run_lines_props_only_silent()` schedules `lines_job.py --live --props-only` every 5 minutes:
  - `CronTrigger(hour='9-23', minute='*/5', timezone=ET)`
- Full NBA lines jobs are also scheduled at exact `minute=0`:
  - noon ET: `run_lines_full` with `lines_job.py --live`
  - 4PM ET: `run_lines_full_parallel` with `lines_job.py --live --parallel`

Observed DB evidence at `2026-06-21 20:00 UTC` showed two `lines_job.py` rows starting at the same time:

- a props-only run
- a full/parallel run

### Why this matters

Overlapping `lines_job.py` processes can concurrently run:

- live player props scrape
- incremental NBA props linker
- full injury scrape/linker path

This increases DB session pressure and creates avoidable timeout/failure risk. It is especially risky when Supabase session-pool pressure is already present.

### Proposed fixes

Implement both a schedule-level and process-level guard:

1. Offset full NBA lines jobs away from props-only minutes.
   - Example: noon full job at 12:01 ET, 4PM full job at 16:01 ET.
   - Or keep full jobs on `:00` and configure props-only to skip known full-job minutes.
2. Add a per-script lock around `lines_job.py` execution in `scheduler.py`.
   - The lock should prevent two scheduler threads from running the same script concurrently.
   - If a second `lines_job.py` starts while a prior one is active, record a clear skipped/blocked event rather than launching another process.

### Non-goals

- Do not redesign the NBA linker algorithm in this slice.
- Do not change MLB/Kalshi job schedules.
- Do not suppress failures globally.

---

## 2. RapidAPI injury scraper runs schema DDL in the recurring production path

### Evidence

`src/scrapers/rapidapi_injury_backfill.py` currently has runtime DDL in `ensure_table()`:

- `CREATE TABLE IF NOT EXISTS public.rapidapi_injuries ...`
- `CREATE INDEX IF NOT EXISTS ...`
- `ALTER TABLE public.rapidapi_injuries ALTER COLUMN team DROP NOT NULL;`
- `DROP INDEX IF EXISTS idx_rapidapi_inj_unique;`
- `CREATE UNIQUE INDEX IF NOT EXISTS idx_rapidapi_inj_unique ...`

The pasted failure text referenced exactly this area:

- `ALTER COLUMN team DROP NOT NULL`
- `idx_rapidapi_inj_unique`

### Why this matters

Recurring production scraper runs should not repeatedly perform schema migration work. This can:

- take locks during scheduled jobs
- drop/rebuild indexes repeatedly
- fail or block under concurrent DB load
- convert a data-refresh job into a schema-mutation job
- make root-cause triage harder because a scrape failure can actually be a DDL failure

### Proposed fixes

1. Move the `team DROP NOT NULL` and unique-index rebuild into a canonical migration file under `database/migrations/`.
2. Make the migration idempotent and bounded.
3. Change `ensure_table()` so the production hot path does not drop/recreate indexes.
4. Consider adding an explicit CLI flag for bootstrap/dev schema creation if this script still needs standalone local setup behavior.

Suggested shape:

- New migration: `database/migrations/0xx_rapidapi_injuries_nullable_team_unique_index.sql`
- Keep DDL idempotent:
  - `ALTER TABLE ... ALTER COLUMN team DROP NOT NULL;`
  - `DROP INDEX IF EXISTS idx_rapidapi_inj_unique;`
  - recreate the functional unique index once
- Scraper runtime:
  - production mode assumes schema exists
  - optional bootstrap mode can run table creation only for local/dev

### Approval needed

This includes DDL on production Postgres. Before applying remotely:

1. Confirm current table/index state with read-only schema queries.
2. Estimate lock risk.
3. Ask Chase for explicit migration approval.
4. Use bounded timeouts (`lock_timeout`, `statement_timeout`).

---

## 3. Timeout failures lose useful diagnostics in `job_executions`

### Evidence

In `src/orchestration/scheduler.py`, `run_job()` handles `subprocess.TimeoutExpired` by building:

- `partial_stdout`
- `partial_stderr`

and sends these to `_send_job_alert(...)`.

However, the persistent `record_job_execution(...)` call later uses the outer `stdout` and `stderr` variables. Those remain empty in the timeout branch unless explicitly assigned.

Observed DB evidence confirms this: timeout rows persisted only the deferred label, not the timeout detail:

- `[NBA deferred — intermittent NBA lines linker failure is parked]`

### Why this matters

Future incidents become difficult to investigate because the database record lacks:

- timeout duration text
- stderr tail
- partial stdout/stderr from the killed subprocess
- last completed step before timeout

### Proposed fix

In `except subprocess.TimeoutExpired as e:` inside `run_job()`:

1. Decode partial stdout/stderr as currently done.
2. Assign them back to the outer variables before the final persistence block:
   - `stdout = partial_stdout`
   - `stderr = partial_stderr`
3. Ensure `partial_stderr` includes `Job timed out after {timeout_mins} minutes` even if no stderr was captured.
4. Consider adding a timeout-specific metric, e.g. `metrics["timeout_minutes"] = timeout_mins`, if metrics parsing supports it cleanly.

### Proposed tests

Add/extend a scheduler unit test that simulates `subprocess.TimeoutExpired` and verifies:

- `record_job_execution` is called with `status='timeout'`
- `error_message` includes `Job timed out after 45 minutes`
- deferred decoration still applies to `lines_job.py`
- the persisted message is not just the deferred label

---

## 4. The 13:55 failure was DB session-pool pressure, not a sport-specific scraper bug

### Evidence

`job_executions` failure at `2026-06-21 13:55 UTC` included:

- `EMAXCONNSESSION max clients reached in session mode - max clients are limited to pool_size: 15`
- failure step: `Linking Props (Incremental)`
- script: `lines_job.py`

### Why this matters

This should be classified as global DB connection pressure unless proven otherwise. Debugging only the injury scraper or only the NBA linker may miss the real issue.

### Proposed fix

No immediate code change is required if the latest DB-pressure fix remains green, but add the following runbook behavior:

1. When `EMAXCONNSESSION` appears, first inspect DB session pressure and overlapping scheduled jobs.
2. Do not assume the current step owns the root cause.
3. Check for simultaneous scheduled GameFlow jobs, especially at exact `:00` minutes.
4. Keep the scheduler overlap fix above as the primary prevention for `lines_job.py` concurrency.

Optional future code hardening:

- Make subprocess job wrappers log start/end plus a lightweight concurrency marker.
- Add a scheduler lock skip reason so skipped overlaps are visible without opening more DB sessions.

---

## 5. NBA deferred/off-season jobs still create avoidable operational noise

### Evidence

- `rapidapi_injuries` latest observed `report_date`: `2026-06-12`
- NBA lines are explicitly tagged deferred in `DEFERRED_FAILURE_JOBS`.
- The scheduler still runs:
  - NBA props-only every 5 minutes from 9AM-11PM ET
  - full NBA lines/injury runs at noon and 4PM ET

### Why this matters

Even if currently green, this spends DB/API capacity and creates alert/noise risk while the NBA lane is parked or out of season.

### Proposed fixes

Add an explicit NBA job gate rather than relying on failure decoration:

- Environment variable examples:
  - `NBA_LINES_ENABLED=true|false`
  - `NBA_FULL_LINES_ENABLED=true|false`
  - `NBA_INJURIES_ENABLED=true|false`
- Default should be chosen intentionally with Chase approval.
- If disabled, the scheduler should not register the relevant jobs, or the wrapper should record a clear skipped status.

Recommended minimal gate:

- Keep NBA props-only enabled only if still needed for dashboard/live lines.
- Gate full NBA lines/injury runs separately so full injury/linker work can be paused without disabling all NBA props polling.

### Non-goals

- Do not alter NBA prediction logic.
- Do not change betting/staking behavior.
- Do not hide true failures behind success statuses.

---

## Implementation Plan

### Task 1: Add focused scheduler tests for timeout persistence

**Objective:** Lock in expected timeout diagnostics before changing `scheduler.py`.

**Files:**

- Modify or create: `tests/test_pipeline_resilience.py` or a new focused scheduler test file such as `tests/test_scheduler_job_execution.py`
- Modify: `src/orchestration/scheduler.py`

**Steps:**

1. Locate existing tests for scheduler/job status tracking.
2. Add a test that monkeypatches `subprocess.run` to raise `subprocess.TimeoutExpired` with partial stdout/stderr.
3. Monkeypatch `record_job_execution` and `_send_job_alert` so no real DB or Discord calls occur.
4. Call `run_job('lines_job.py', timeout=2700)`.
5. Assert the recorded status is `timeout`.
6. Assert persisted error text includes:
   - `Job timed out after 45 minutes`
   - `NBA deferred`
   - a tail of partial stderr if provided
7. Run the test and verify it fails before implementation.

**Validation command:**

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_pipeline_resilience.py -q
```

If a new focused file is used:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_scheduler_job_execution.py -q
```

---

### Task 2: Persist timeout stdout/stderr in `scheduler.py`

**Objective:** Ensure timeout rows in `job_executions` contain actionable failure evidence.

**Files:**

- Modify: `src/orchestration/scheduler.py:407-456`
- Test: same test file from Task 1

**Steps:**

1. In the `except subprocess.TimeoutExpired as e:` branch, after decoding partial output, assign:
   - `stdout = partial_stdout`
   - `stderr = partial_stderr`
2. Ensure `partial_stderr` includes timeout text even when no subprocess stderr is available.
3. Run the focused timeout test and verify it passes.
4. Run scheduler py_compile.

**Validation commands:**

```powershell
.\venv\Scripts\python.exe -m py_compile src/orchestration/scheduler.py
.\venv\Scripts\python.exe -m pytest tests/test_pipeline_resilience.py -q
```

---

### Task 3: Add a per-script scheduler concurrency guard for `lines_job.py`

**Objective:** Prevent overlapping NBA lines subprocesses from running concurrently.

**Files:**

- Modify: `src/orchestration/scheduler.py`
- Test: scheduler test file

**Design:**

Add an in-process lock registry in `scheduler.py`, for example:

- module-level dict: `JOB_LOCKS: dict[str, threading.Lock]`
- helper: `_get_job_lock(script_name)`
- optional allowlist: only lock `lines_job.py` first to minimize blast radius

Behavior:

- Before launching subprocess, try non-blocking acquire for lockable scripts.
- If acquire fails:
  - log a clear skip message
  - optionally record `job_executions` with `status='skipped'` and error/reason `Skipped because another lines_job.py run is still active`
  - do not send failure alert unless Chase wants overlap skips alerted
- Always release the lock in `finally` after run completion.

**Open decision for Chase:**

Should overlap skips be persisted as `skipped`, or only logged? Recommendation: persist as `skipped` so schedule behavior is auditable, but do not Discord-alert by default.

**Validation:**

- Unit test two concurrent/simulated calls where second acquire fails.
- Assert second call does not invoke `subprocess.run`.
- Assert first call still releases lock.

---

### Task 4: Offset or skip overlapping NBA full-line schedules

**Objective:** Reduce the chance of intentional schedule collisions even before/alongside the lock.

**Files:**

- Modify: `src/orchestration/scheduler.py:853-895`

**Option A: offset full jobs**

- Noon full job: `CronTrigger(hour=12, minute=1, timezone=ET)`
- 4PM full job: `CronTrigger(hour=16, minute=1, timezone=ET)`

**Option B: skip props-only at full-job minutes**

- Keep full jobs at `minute=0`.
- Replace `props_every_5` cron with a trigger/list that excludes `12:00` and `16:00`, or add a wrapper that returns early for those exact minutes.

**Recommendation:** Option A plus lock. It is smaller and easier to reason about.

**Validation:**

- Run py_compile.
- If scheduler has tests around registered jobs, update expected trigger names/minutes.
- Start scheduler in a dry/local mode only if safe; otherwise inspect `scheduler.get_jobs()` in a no-start test harness.

---

### Task 5: Move RapidAPI injury DDL into a migration

**Objective:** Remove schema mutation from the recurring production scrape path.

**Files:**

- Create: `database/migrations/0xx_rapidapi_injuries_nullable_team_unique_index.sql`
- Modify: `src/scrapers/rapidapi_injury_backfill.py:60-102`

**Preflight required before remote execution:**

Run read-only schema checks through the GameFlow SQL runner pattern:

```sql
SELECT column_name, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'rapidapi_injuries'
  AND column_name = 'team';
```

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'rapidapi_injuries'
  AND indexname = 'idx_rapidapi_inj_unique';
```

**Migration content shape:**

Use bounded timeouts when applying remotely:

```sql
SET lock_timeout = '5s';
SET statement_timeout = '60s';

ALTER TABLE public.rapidapi_injuries
  ALTER COLUMN team DROP NOT NULL;

DROP INDEX IF EXISTS public.idx_rapidapi_inj_unique;

CREATE UNIQUE INDEX IF NOT EXISTS idx_rapidapi_inj_unique
  ON public.rapidapi_injuries(report_date, COALESCE(team, ''), player, status, COALESCE(reason, ''));
```

**Important:** If `rapidapi_injuries` is large enough for index creation to be risky, ask Chase whether to use `CREATE UNIQUE INDEX CONCURRENTLY`. If using concurrent index creation, do not wrap that migration in a transaction.

**Approval gate:** Ask Chase before applying this migration to production.

---

### Task 6: Stop recurring scraper runs from dropping/recreating indexes

**Objective:** Keep `rapidapi_injury_backfill.py` data-refresh-only during scheduled production execution.

**Files:**

- Modify: `src/scrapers/rapidapi_injury_backfill.py:60-102`
- Test: new or existing scraper test

**Steps:**

1. Remove `DROP INDEX` and `ALTER TABLE` from runtime `ensure_table()`.
2. Keep only safe bootstrap DDL if needed, or make schema bootstrap opt-in with a CLI flag.
3. Ensure `main()` does not run DDL by default in production if schema is expected to exist.
4. Add/adjust tests to assert that runtime `ensure_table()` does not execute `DROP INDEX`.

**Validation commands:**

```powershell
.\venv\Scripts\python.exe -m py_compile src/scrapers/rapidapi_injury_backfill.py
.\venv\Scripts\python.exe src\scrapers\rapidapi_injury_backfill.py --dry-run --start 2026-06-21 --end 2026-06-21
```

Do not run a real injury scrape/backfill against production without approval.

---

### Task 7: Add NBA deferred/off-season gates if Chase wants less operational noise

**Objective:** Allow NBA full lines/injury jobs to be intentionally disabled without producing failure alerts.

**Files:**

- Modify: `src/orchestration/scheduler.py`
- Possibly modify: Railway environment variables

**Possible env flags:**

- `NBA_PROPS_ONLY_ENABLED`
- `NBA_FULL_LINES_ENABLED`
- `NBA_INJURIES_ENABLED`

**Recommended initial behavior:**

- Gate full NBA lines/injury jobs separately from props-only polling.
- If disabled, do not register full NBA jobs at scheduler startup and log:
  - `NBA full lines disabled by NBA_FULL_LINES_ENABLED=false`
- Avoid turning actual failed runs into success.

**Approval gate:** Ask Chase before changing production job registration defaults.

---

## Verification Checklist for Future Implementation

Before claiming the future fix is done:

- [ ] `python -m py_compile src/orchestration/scheduler.py`
- [ ] `python -m py_compile src/orchestration/lines_job.py` if touched
- [ ] `python -m py_compile src/scrapers/rapidapi_injury_backfill.py` if touched
- [ ] Focused scheduler tests pass
- [ ] RapidAPI scraper dry-run does not require DB mutation or API calls
- [ ] `git diff --check` passes
- [ ] Railway deploy logs show no overlapping `lines_job.py` launches at full-job times
- [ ] `job_executions` timeout rows include actual timeout details, not only deferred labels
- [ ] No production DDL applied without explicit Chase approval

---

## Suggested Commit Message

For the future implementation slice:

```text
fix: harden NBA lines scheduler failure handling
```

If splitting commits:

```text
test: cover scheduler timeout persistence
fix: persist scheduler timeout diagnostics
fix: prevent overlapping NBA lines jobs
db: migrate rapidapi injury unique index schema
fix: remove injury scraper runtime index rebuild
```

---

## Notes / Open Questions

1. Should overlap skips be persisted to `job_executions` as `skipped`, or only logged?
2. Should full NBA lines/injury jobs be disabled while NBA is deferred/off-season?
3. Should the RapidAPI injury unique index be created concurrently in production?
4. Should success alerts stay silent for skipped overlap events?
5. Should the deferred label remain once the scheduler and injury DDL fixes are complete, or should it be removed after NBA lines are no longer considered parked?
