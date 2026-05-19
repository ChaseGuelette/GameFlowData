# Scheduler / Job Registry Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to change production schedules, Railway start commands, job frequencies, credentials, or job side effects.

**Goal:** Rebuild `src/orchestration/scheduler.py` into a declarative job registry with explicit job specs, dependency/retry policies, execution telemetry, and schedule registration while preserving every current production schedule and wrapper behavior.

**Architecture:** Keep `src/orchestration/scheduler.py` as the Railway entry point and compatibility facade. Extract job execution, environment validation, metrics parsing, alerting, and APScheduler registration into focused modules. Represent schedules as data (`JobSpec`) before changing any job times or functions.

**Tech Stack:** Python, APScheduler, subprocess, SQLAlchemy, Discord alerts, Railway always-on worker, pytest.

---

## Relevant prior lessons/invariants

Retrieved before writing this plan:

- `operations/hard-facts`
- `operations/critical-invariants`
- `infrastructure/railway-setup`

Applied facts/invariants:

1. Production backend is a single Railway always-on Python worker using APScheduler and `src/orchestration/scheduler.py`.
2. Railway start command is `/app/venv/bin/python src/orchestration/scheduler.py`; do not break this entry point.
3. Railway `daily_stats_job` must use CDN-only mode and must not call `stats.nba.com` from Railway.
4. Advanced stats scraping is local-only; do not add jobs that move it to Railway.
5. All subprocess calls use `sys.executable` for venv compatibility.
6. Kalshi and arbitrage jobs run under the same worker, so schedule/timeout/alert changes are production-impacting.

---

## Executive diagnosis

`scheduler.py` is a production-critical god module:

- `src/orchestration/scheduler.py`
  - 1,177 total lines
  - 844 non-comment LOC
  - 43 top-level functions
  - `main(...)`: 459 lines
  - 45 explicit `scheduler.add_job(...)` registrations

It currently owns:

- environment validation
- subprocess command construction
- job execution and timeout handling
- stdout/stderr capture and log surfacing
- metrics parsing from job output
- Discord job alerts
- DB job execution recording
- dependency checks using in-memory `JOB_STATUS`
- dozens of job wrapper functions
- APScheduler construction and lifecycle
- all cron schedules and schedule comments
- signal handling and graceful shutdown
- `--run-test` entry behavior

This makes schedule changes risky because schedule data, runtime mechanics, alert policy, and wrapper behavior all live in one file.

---

## Current scheduled job inventory

Mechanical extraction from `scheduler.add_job(...)` found 45 registrations:

### NBA / user paper / maintenance

- `daily_stats` → `run_daily_stats`, 9:00 ET
- `daily_stats_retry` → `run_daily_stats_retry`, 9:30 ET
- `user_paper_bet_resolution` → `run_user_paper_bet_resolution`, 9:30 ET
- `lines_props_10am` → `run_lines_props_only`, 10:00 ET
- `inference_1015am` → `run_inference`, 10:15 ET
- `lines_noon_full` → `run_lines_full`, 12:00 ET
- `inference_noon` → `run_inference`, 12:15 ET
- `props_every_5` → `run_lines_props_only_silent`, 9:00-23:00 every 5 min
- `edge_refresh_every_5` → `run_edge_refresh_silent`, 9:00-23:00 offset every 5 min
- `lines_4pm_full` → `run_lines_full_parallel`, 16:00 ET
- `inference_4pm` → lambda `run_inference(skip_bets=True)`, 16:15 ET
- `archive_old_props` → `run_archive_old_props`, 3:00 ET

### MLB

- `mlb_roster_scraper`, `mlb_daily_stats`, `mlb_daily_stats_retry`
- `mlb_weather_forecast`, `mlb_lines_props_930am`, `mlb_lineup_scraper_935am`, `mlb_umpire_scraper_936am`, `mlb_inference_950am`
- noon/afternoon/evening lines, lineup, inference, and edge-refresh windows
- `mlb_pregame_30min_props`, every 10 min 10:00-23:00 ET

### Kalshi / prediction markets

- `kalshi_live_resolution`, 9:15 ET
- `kalshi_daily_summary`, 10:00 ET
- `kalshi_refresh_mlb`, every 10 min on :00, MLB first
- `kalshi_refresh_nba`, every 10 min on :02, after MLB
- `kalshi_nonsports_refresh`, every 10 min
- `kalshi_execute_approved`, every 2 min
- `kalshi_reprice_stale`, every 2 min
- `kalshi_pending_fills`, every 5 min
- `kalshi_stale_fills`, every 5 min
- `kalshi_execute_cancellations`, every 2 min

### Arbitrage / non-sports

- `arb_scan_mlb`, every 10 min 12:05-23:05 ET
- `nonsports_scrape`, 9:00 and 17:00 ET, 2-hour timeout
- `arb_scan_all_categories`, every 30 min 9:00-23:00 ET

This schedule inventory must become a golden characterization test before moving schedules into a registry.

---

## Current ownership problems

### 1. Schedule data is embedded in imperative code

`main(...)` contains all `scheduler.add_job(...)` calls, comments, IDs, names, triggers, and wrapper references.

Why this is wrong:

- Diff review of schedule changes is noisy.
- Duplicate job wrapper functions hide which script/args actually run.
- A typo in a lambda or wrapper changes production behavior without registry validation.

Target owner:

- `src/orchestration/job_registry.py`

Core type:

- `JobSpec(id, name, function_key, trigger, domain, schedule_comment, enabled_env=None, timeout=None, silent_on_success=False, dependencies=None)`

---

### 2. Job execution mechanics are mixed with schedule registration

Current `run_job(...)` owns:

- script path construction
- `sys.executable` subprocess invocation
- working directory selection
- timeout handling
- stdout/stderr capture
- success/failure/timeout status
- alert sending
- in-memory status updates
- metrics parsing
- persistent DB record writes

Target owners:

- `src/orchestration/job_runner.py`
- `src/orchestration/job_result.py`
- `src/orchestration/job_metrics.py`

Required behavior:

- use `sys.executable`
- `cwd` remains project root
- alert suppression via `silent_on_success` preserved
- timeout handling still alerts with partial output
- DB recording remains non-fatal

---

### 3. Job wrappers are tiny but numerous and hard to audit

Examples:

- `run_daily_stats()` → `run_job("daily_stats_job.py")`
- `run_lines_full()` → `run_job("lines_job.py", extra_args="--live")`
- `run_mlb_daily_stats()` → `run_job("mlb_daily_stats_job.py")`
- `run_kalshi_refresh_mlb()` → `run_job("kalshi_refresh_job.py", extra_args="--sport mlb")`
- `run_arb_scan_all_categories()` → `run_job("arb_scan_job.py", extra_args="--all-categories --skip-scrape")`

Target owner:

- `src/orchestration/job_definitions.py`

Approach:

- Replace ad-hoc wrappers with `ScriptJob(script_name, extra_args, timeout, silent_on_success)` specs.
- Keep wrapper functions as compatibility delegates until registry is stable.

---

### 4. Retry/dependency behavior is inconsistent and in-memory only

Current helpers:

- `JOB_STATUS`
- `check_dependency(...)`
- `run_daily_stats_retry(...)`
- `run_mlb_daily_stats_retry(...)`

Why this is wrong:

- Retry logic is embedded in wrapper functions and comments.
- In-memory status is reset on deploy/restart, while persistent job log exists separately.
- Retry messages reference old schedule wording in places and are hard to test.

Target owner:

- `src/orchestration/job_policies.py`

Initial extraction:

- Preserve in-memory behavior.
- Characterize existing retry wrappers before considering persistent dependency checks.

---

### 5. Metrics parsing and job execution logs are hidden in scheduler

Current functions:

- `_parse_metrics_from_output(...)`
- `record_job_execution(...)`

Target owners:

- `src/orchestration/job_metrics.py`
- `src/orchestration/job_execution_log.py`

Tests:

- parse metrics from representative job outputs.
- DB recording failure is non-fatal.
- error message truncation behavior preserved.

---

### 6. Alerting is scheduler-local

Current functions:

- `_send_job_alert(...)`
- alert calls inside `run_job(...)` and retry wrappers

Target owner:

- `src/orchestration/job_alerts.py`

Safety:

- Preserve alert-on-failure behavior.
- Preserve `silent_on_success` behavior.
- Preserve timeout alert content.

---

### 7. Environment validation is global and not job-aware

Current `_validate_environment()` logs required/optional variables but does not tie env vars to job specs.

Target owners:

- `src/orchestration/environment.py`
- job-specific env requirements in `JobSpec`

First step:

- Extract current global env check unchanged.
- Later add job-specific optional metadata without changing startup behavior.

---

## Target design by responsibility

### A. `orchestration/job_specs.py`

Dataclasses/enums:

- `JobSpec`
- `ScriptJob`
- `FunctionJob`
- `JobDomain`
- `CronSpec`

### B. `orchestration/job_registry.py`

Single source of truth for all 45 job specs and schedule metadata.

### C. `orchestration/schedule_builder.py`

Converts `JobSpec` data into APScheduler jobs.

### D. `orchestration/job_runner.py`

Runs scripts, captures output, handles timeout/status, returns `JobResult`.

### E. `orchestration/job_metrics.py`

Parses stdout/stderr into metrics.

### F. `orchestration/job_execution_log.py`

Records executions to DB.

### G. `orchestration/job_alerts.py`

Formats/sends job alerts.

### H. `orchestration/job_policies.py`

Retry/dependency/silent-success policies.

### I. `scheduler.py` compatibility entry point

Final `scheduler.py` role:

- parse CLI
- validate environment
- build scheduler from registry
- support `--run-test`
- signal handling
- call `scheduler.start()`

---

## Refactor phases

### Phase 0: Characterization and schedule inventory tests

Objective: Make the current production schedule explicit before extraction.

Files:

- Create: `tests/test_scheduler_inventory.py`

Tests:

- AST or builder-based inventory contains exactly current job IDs.
- critical schedule IDs exist with current names.
- Railway entry point remains importable.
- `--run-test` still calls `run_job("test_job.py")` and exits.
- `daily_stats_job.py` wrapper keeps CDN-only behavior if currently provided through job script/args; do not introduce stats.nba.com call.

Validation:

`venv/Scripts/python.exe -m pytest tests/test_scheduler_inventory.py -q`

---

### Phase 1: Extract pure job result and metrics parsing

Objective: Move non-schedule pure-ish logic first.

Files:

- Create: `src/orchestration/job_result.py`
- Create: `src/orchestration/job_metrics.py`
- Create: `tests/test_scheduler_job_metrics.py`
- Modify: `src/orchestration/scheduler.py` wrapper imports

Tests:

- representative stdout/stderr metrics parse unchanged.
- empty output returns current empty/default metrics.
- parser handles known NBA/MLB/Kalshi output snippets.

---

### Phase 2: Extract alert and execution log adapters

Objective: Move side-effect boundaries without changing `run_job` behavior.

Files:

- Create: `src/orchestration/job_alerts.py`
- Create: `src/orchestration/job_execution_log.py`
- Create: `tests/test_scheduler_job_alerts.py`
- Create: `tests/test_scheduler_job_execution_log.py`

Tests:

- alert is skipped on success only when `silent_on_success=True`.
- failure always alerts.
- timeout alert includes timeout marker.
- DB recording exceptions do not fail job execution.

---

### Phase 3: Extract job runner

Objective: Give subprocess execution one owner.

Files:

- Create: `src/orchestration/job_runner.py`
- Create: `tests/test_scheduler_job_runner.py`
- Modify: `scheduler.run_job` to delegate.

Tests:

- command uses `sys.executable`.
- working directory is project root.
- `extra_args` are parsed with `shlex.split`.
- success/failure/timeout statuses match current behavior.
- `JOB_STATUS` compatibility remains updated.

---

### Phase 4: Extract job specs and registry without rewiring scheduler

Objective: Introduce data representation in parallel with current imperative schedule.

Files:

- Create: `src/orchestration/job_specs.py`
- Create: `src/orchestration/job_registry.py`
- Create: `tests/test_scheduler_job_registry.py`

Tests:

- registry contains all 45 current job IDs.
- each job has domain, name, trigger, callable/script spec.
- duplicate IDs fail validation.
- every wrapper/function key resolves.
- critical offsets preserved: Kalshi MLB before NBA, edge refresh after props, arb scan offset after Kalshi refresh.

No production scheduler rewiring yet.

---

### Phase 5: Extract schedule builder and compare old vs new registry

Objective: Prove registry can reproduce current APScheduler registration.

Files:

- Create: `src/orchestration/schedule_builder.py`
- Create: `tests/test_scheduler_schedule_builder.py`

Tests:

- building from registry creates same count and IDs.
- trigger string representations match current inventory for critical jobs.
- lambda job `inference_4pm` is represented as explicit script/function spec with `skip_bets=True` metadata.

---

### Phase 6: Switch `scheduler.main` to registry builder

Objective: Replace imperative `add_job` block with registry-driven registration.

Files:

- Modify: `src/orchestration/scheduler.py`

Tests:

- inventory tests still pass.
- `scheduler.main` dry construction path can be tested without starting infinite loop.
- log scheduled jobs still works.

Safety:

- One PR only after old/new schedule parity is proven.

---

### Phase 7: Replace wrappers with generated delegates where safe

Objective: Remove wrapper duplication without breaking imports.

Approach:

- Keep public wrapper names as delegates for one migration cycle.
- Add removal guard only after no external callsites import wrappers directly.

Tests:

- wrapper functions call the expected `ScriptJob` spec.
- callsite scan confirms only scheduler uses wrappers before removal.

---

### Phase 8: Add anti-regrowth guards and docs

Guards:

- no raw `scheduler.add_job(...)` calls in `scheduler.py` outside `schedule_builder`.
- no subprocess execution in `scheduler.py` outside delegated `run_job` wrapper.
- registry validation enforces unique IDs.
- all job specs include domain and timeout.

Recommended endpoint:

- `scheduler.py` under 250 non-comment LOC.

---

## Files likely touched

Existing:

- `src/orchestration/scheduler.py`

New:

- `src/orchestration/job_specs.py`
- `src/orchestration/job_registry.py`
- `src/orchestration/schedule_builder.py`
- `src/orchestration/job_runner.py`
- `src/orchestration/job_result.py`
- `src/orchestration/job_metrics.py`
- `src/orchestration/job_execution_log.py`
- `src/orchestration/job_alerts.py`
- `src/orchestration/job_policies.py`
- `src/orchestration/environment.py`

Tests:

- `tests/test_scheduler_inventory.py`
- `tests/test_scheduler_job_metrics.py`
- `tests/test_scheduler_job_alerts.py`
- `tests/test_scheduler_job_execution_log.py`
- `tests/test_scheduler_job_runner.py`
- `tests/test_scheduler_job_registry.py`
- `tests/test_scheduler_schedule_builder.py`

---

## Validation commands

Inventory baseline:

`venv/Scripts/python.exe -m pytest tests/test_scheduler_inventory.py -q`

Runner/metrics extraction:

`venv/Scripts/python.exe -m pytest tests/test_scheduler_job_metrics.py tests/test_scheduler_job_alerts.py tests/test_scheduler_job_execution_log.py tests/test_scheduler_job_runner.py -q`

Registry parity:

`venv/Scripts/python.exe -m pytest tests/test_scheduler_job_registry.py tests/test_scheduler_schedule_builder.py tests/test_scheduler_inventory.py -q`

Lane-wide:

`venv/Scripts/python.exe -m pytest tests -k "scheduler or job_registry or job_runner" -q`

Compile:

`venv/Scripts/python.exe -m py_compile src/orchestration/scheduler.py src/orchestration/job_specs.py src/orchestration/job_registry.py src/orchestration/schedule_builder.py src/orchestration/job_runner.py src/orchestration/job_result.py src/orchestration/job_metrics.py src/orchestration/job_execution_log.py src/orchestration/job_alerts.py src/orchestration/job_policies.py src/orchestration/environment.py`

Diff hygiene:

`git diff --check -- src/orchestration/scheduler.py src/orchestration/job_*.py src/orchestration/schedule_builder.py src/orchestration/environment.py tests .hermes/plans/god-class-migrations/07-scheduler-job-registry-migration.md`

Manual deployment safety before merge/deploy:

- Review generated schedule table against current schedule inventory.
- Confirm Railway start command still points to `src/orchestration/scheduler.py`.
- Confirm daily stats job remains CDN-only.

---

## Risk controls / non-goals

Non-goals:

- Do not change job times/frequencies.
- Do not add/remove jobs.
- Do not change Railway start command.
- Do not move advanced stats scraping to Railway.
- Do not change job script behavior.
- Do not change alert channels or credentials.
- Do not introduce persistent dependency policy until in-memory behavior is characterized.
- Do not mix scheduler registry migration with paper-trading/arbitrage code rewrites.

Hard rules:

- Registry parity test must pass before `main` is rewired.
- `sys.executable` subprocess behavior must remain.
- Failed jobs still alert.
- `silent_on_success` jobs still alert on failure.
- DB execution logging remains non-fatal.

---

## Expansion checkpoints learned from Kalshi

Trigger a new named sub-slice if you discover:

1. A wrapper encodes non-obvious args or timeout behavior.
2. A schedule comment encodes an ordering invariant not represented in data.
3. A job depends on prior in-memory status.
4. A script expects cwd/project-root assumptions.
5. A Railway env var is actually required for a subset of jobs.
6. A job should be disabled gracefully when an optional credential is missing.
7. A lambda job hides arguments that need typed representation.
8. A callsite imports scheduler wrappers directly.
9. A behavior-changing schedule fix appears; split it from extraction.
10. A schedule parity guard is needed before deleting old imperative registrations.

Progress log entries must distinguish: registry spec added, builder parity proven, scheduler switched, wrapper retained, wrapper removed, schedule behavior unchanged, behavior-changing issue deferred.

---

## First implementation PR recommendation

Start with characterization and pure extraction:

1. Add `tests/test_scheduler_inventory.py` with the current 45 job IDs and critical names.
2. Extract `job_result.py` and `job_metrics.py`.
3. Keep `scheduler.py` running exactly as before.
4. Do not move the `add_job` block yet.
5. Run inventory and metrics tests.

This avoids changing production schedule behavior while making the future registry measurable.

---

## Progress log

### 2026-05-19 initial migration documentation

Created from bounded code/brain deep dive.

Evidence inspected:

- AST/function inventory for `src/orchestration/scheduler.py`.
- Targeted reads of `run_job(...)`, `_validate_environment(...)`, and `main(...)` schedule registration sections.
- Mechanical extraction of 45 `scheduler.add_job(...)` registrations.
- GBrain hard facts, critical invariants, and Railway setup.

Current status:

- Documentation only.
- No production code changed.
- No scheduler/jobs started.

---

## Done when

- `scheduler.py` is a small Railway-compatible entry point.
- All jobs live in a declarative, validated registry.
- Job runner, metrics, alerts, execution log, policies, and environment checks have focused owners.
- Current 45-job schedule is reproduced exactly by tests.
- Production job behavior remains unchanged unless a separate approved schedule-change PR is made.
