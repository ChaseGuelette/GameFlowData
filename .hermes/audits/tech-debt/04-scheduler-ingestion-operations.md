# Lane E Audit — Scheduler, Ingestion, and Runtime Operations

Audit date: 2026-07-18  
Mode: read-only static audit  
Scope: `src/orchestration/`, scheduler-called scraper/processing entrypoints, scheduler tests, current scheduler understanding docs, `railway.toml`, God-Class Lane 07, and selected remote GBrain handoffs.

## Executive summary

The production boundary remains one Railway worker running `src/orchestration/scheduler.py` (`railway.toml:7-10`). The current scheduler has **48 `scheduler.add_job(...)` call sites**, not the 45 recorded by the 2026-05-19 Lane 07 plan. With current default gates, 45 jobs are registered: all 48 call sites minus three default-off arbitrage registrations. `NBA_FULL_LINES_ENABLED=false` removes another two; enabling both arbitrage gates restores all 48.

Highest-risk findings:

1. `mlb_lines_job.py` discards child-step failures and normally exits zero, so the scheduler can persist and alert a false success after an MLB scrape or linker failure.
2. Four fixed MLB lines jobs collide exactly with the recurring close-snapshot job, while only NBA `lines_job.py` has a cross-schedule lock.
3. Timeout ownership stops at each direct child process. Nested scraper/processing descendants are not explicitly terminated as a process group and may outlive a timed-out wrapper or be duplicated by a retry.
4. The Railway CDN-only invariant is implemented correctly in current code, but no scheduler-level characterization test prevents removal of `--cdn-only`.

No production calls, scrapes, jobs, tests that execute jobs, DB queries, deployment actions, or plan/register edits were performed.

## Authority and reconciliation

### Hard invariants enforced

- `AGENTS.md:20-21`: advanced stats scraping must never run on Railway; Railway `daily_stats_job` must use `--cdn-only` and make no `stats.nba.com` calls.
- `daily_stats_job.main` hard-codes `src/scrapers/nba_unified_scraper.py --cdn-only` at `src/orchestration/daily_stats_job.py:391-395`.
- `nba_unified_scraper` describes `--cdn-only` as skipping `stats.nba.com` at `src/scrapers/nba_unified_scraper.py:931-932` and enters the CDN-only branch at `src/scrapers/nba_unified_scraper.py:972-978`.
- Result: the current Railway NBA daily-stats path complies. No recommendation in this audit moves advanced-stat scraping to Railway.

### God-Class Lane 07 reconciliation

Lane 07 is **documentation only / not implemented**.

Evidence:

- `.hermes/plans/god-class-migrations/07-scheduler-job-registry-migration.md:614-631` records only the 2026-05-19 documentation pass and says no production code changed.
- Its `Done when` conditions remain unmet at `.hermes/plans/god-class-migrations/07-scheduler-job-registry-migration.md:635-641`.
- None of the proposed registry/runner/policy modules appears in the current `src/orchestration/` inventory.
- The plan says 45 explicit registrations (`:63-65`, `:415`) and a 1,177-line scheduler (`:34-41`); current code is 1,333 lines with 48 registration call sites.
- The plan says dependencies are in-memory only (`:186-199`), but current `check_dependency` now falls back to `job_executions` (`src/orchestration/scheduler.py:178-221`). Retry wrappers still use only memory.
- The plan does not include later controls now present in code: `NBA_FULL_LINES_ENABLED`, default-off arbitrage gates, the `lines_job.py` lock, 12:01/16:01 offsets, MLB dense CLV, or the 48-site inventory.

Plan interaction for this audit: findings that align with Lane 07 should update its characterization baseline before implementation. Behavior fixes—especially false success, overlap, process-tree termination, and CDN-only enforcement—need RED tests and should not be hidden inside a mechanical registry extraction.

### Relevant handoffs

Read-only remote canonical handoff evidence was available:

- `/home/chase/GameFlowBrain/Handoffs/handoff-106.md:45-55` confirms scheduler complexity/stale schedule docs, candidate duplicate 10 AM NBA props trigger, and current scheduler plus `docs/understanding/railway-scheduler.md` as schedule truth.
- `/home/chase/GameFlowBrain/Handoffs/handoff-105.md:23-34` records deployed NBA lines hardening: one-script lock, persisted overlap skips, timeout diagnostics, 12:01/16:01 offsets, and `NBA_FULL_LINES_ENABLED` gating.
- `/home/chase/GameFlowBrain/Handoffs/handoff-104.md:43-51` records the earlier overlap and session-pool-pressure incident that motivated those mitigations.
- `/home/chase/GameFlowBrain/Handoffs/handoff-097.md:19-34` records that Railway scheduler writes require the writable Supabase session pooler on port 5432. This audit did not inspect credentials or live variables.

## Mechanical runtime inventory

### Process and ownership boundary

- Railway starts `/app/venv/bin/python src/orchestration/scheduler.py` (`railway.toml:7-10`).
- `BlockingScheduler(timezone="America/New_York")` is created without explicit executors or job defaults (`src/orchestration/scheduler.py:850`).
- `run_job` builds `[sys.executable, src/orchestration/<script>, ...]`, uses project-root cwd, captures output, and applies a default 2,700-second timeout (`src/orchestration/scheduler.py:371-421`).
- Success/failure/timeout is held in `JOB_STATUS` and persisted to `job_executions`; persistence and Discord failures are non-fatal (`src/orchestration/scheduler.py:137-175`, `:335-368`, `:484-502`).
- Signal shutdown uses `scheduler.shutdown(wait=False)` (`src/orchestration/scheduler.py:1317-1324`).

### Registration counts and gates

| Registration state | Count | Evidence |
|---|---:|---|
| Static `add_job` call sites | 48 | Mechanical AST inventory of `scheduler.main`; source region `src/orchestration/scheduler.py:857-1308` |
| Default registration (`NBA_FULL_LINES_ENABLED=true`, arb gates false) | 45 | NBA gate `:901-952`; arb gates `:1269-1308` |
| NBA full lines disabled, arb gates false | 43 | Two NBA full registrations omitted |
| All optional registrations enabled | 48 | Two NBA full plus three arb registrations |

### Current registered-job map

All times are ET. “Conditional” means the scheduler registration itself is env-gated; job-level gates are listed separately.

| Domain | Scheduler IDs and windows | Wrapper / script | Gate or notable policy |
|---|---|---|---|
| Maintenance | `mlb_dense_clv_snapshots` 02:20; `archive_old_props` 03:00; `user_paper_bet_resolution` 09:30 | dense CLV, archive, resolver scripts | Dense CLV wrapper scheduled always; script gate defaults off |
| NBA stats | `daily_stats` 09:00; `daily_stats_retry` 09:30 | `daily_stats_job.py` | CDN-only inside entrypoint; retry memory-only |
| NBA lines | `lines_props_10am` 10:00; `props_every_5` every 5m 09:00-23:59; `lines_noon_full` 12:01; `lines_4pm_full` 16:01 | `lines_job.py` | script lock; full jobs conditional on `NBA_FULL_LINES_ENABLED` |
| NBA prediction | `inference_1015am` 10:15; `inference_noon` 12:15; `edge_refresh_every_5` +2m offset; `inference_4pm` 16:15 | inference/edge scripts | stale dependency warns but does not block; 16:15 uses `--skip-bets`; edge uses `--skip-paper` |
| MLB stats/roster | roster and daily stats 09:00; daily retry 09:20; roster retry 10:05 | MLB stats/roster scripts | retries memory-only |
| MLB context | weather 09:25; lineup 09:35/12:05/12:45/18:10; umpire 09:36 | MLB API wrappers | no scheduler script lock |
| MLB lines | props 09:30/13:00/18:00; full 12:00/17:00; close snapshots every 10m 10:00-23:50 | `mlb_lines_job.py` | no scheduler script lock; close mode writes dense CLV target |
| MLB prediction | inference 09:50/12:15/13:30/18:30; edge 14:30/16:30 | MLB inference/edge scripts | stale dependency warns but does not block |
| Kalshi refresh/summary | resolution 09:15; summary 10:00; MLB refresh :00/10m; NBA refresh :02/10m; non-sports refresh every 10m | Kalshi scripts | credentials/live behavior mostly job-level gated |
| Kalshi live lifecycle | approved executor odd minutes/2m; reprice 02-58/4m; fills and stale-fill detector every 5m; cancellations 00-58/4m | Kalshi lifecycle scripts | `KALSHI_LIVE_TRADING_ENABLED` enforced in job scripts, not scheduler registration |
| Arbitrage | MLB scan 12:05-23:55/10m; non-sports scrape 09:00 and 17:00; all-category scan 09:00-23:30/30m | `arb_scan_job.py` | registration default-off; scrape/alert/paper controls are separate flags |

Exact registration evidence is `src/orchestration/scheduler.py:857-1308`. Wrapper-to-script/argument evidence is `src/orchestration/scheduler.py:546-823`.

### Directly called ingestion/processing entrypoints

| Orchestrator | Direct child entrypoints | Failure/retry/timeout behavior |
|---|---|---|
| `daily_stats_job.py` | `src/scrapers/nba_unified_scraper.py --cdn-only`; NBA linker; incremental team-ID, rolling-average and opponent-allowed processors; position/league-average scrapers | Critical/noncritical steps; per-step 5-20m timeout; up to two retries with exponential backoff (`:275-362`, `:387-444`) |
| `lines_job.py` | game lines, live/historical player props, NBA linker, RapidAPI injuries, injury linker (`:199-236`) | No child timeout or retry; continues remaining steps and exits nonzero if any group failed (`:67-142`, `:238-257`) |
| `mlb_daily_stats_job.py` | MLB boxscores, Statcast, MLB linker, batting/pitching averages, bullpen workload, advanced-history derivation (`:288-370`) | Per-step 10-15m timeout; up to two retries; freshness validation hard-fails; bet resolution remains non-fatal |
| `mlb_lines_job.py` | MLB game lines, player props, incremental linker, dense CLV linkers (`:145-157`, `:192-262`) | No child timeout/retry; child return values are not propagated; process normally exits zero |
| `mlb_dense_clv_job.py` | bounded snapshot scraper and linker (`:163-200`) | Scheduler outer timeout 2h; inner `subprocess.run(timeout=None)`; raises on nonzero child exit |

Other scheduler-owned entrypoints are inference, edge refresh, archive, resolution, Kalshi lifecycle, and arb wrappers. They were mechanically inventoried for env/subprocess/exit patterns but not deeply audited beyond scheduler-facing ownership because this lane is ingestion/runtime focused.

### Existing controls matrix

| Concern | Current control | Gap |
|---|---|---|
| Railway CDN-only | Hard-coded `--cdn-only` plus scraper branch | No characterization test at scheduler/job boundary |
| Dependencies | In-memory status plus DB fallback for inference checks | Retry wrappers bypass DB fallback; gates warn and continue |
| Retry | Daily NBA/MLB and roster scheduled retries; per-step retry in daily stats jobs | Not declarative; restart-sensitive; many ingestion jobs have none |
| Timeout | Scheduler default 45m; dense CLV and non-sports scrape 2h; daily-stats child timeouts | No process-group ownership; NBA/MLB lines children have no inner timeout |
| Overlap | In-process lock for `lines_job.py`; fixed NBA full jobs offset to :01 | No MLB lines lock; no cross-process/distributed lease; skip path does not alert |
| APScheduler overlap/misfire | Library defaults only | No explicit `max_instances`, coalescing, misfire grace, event listener, or persisted missed-run event |
| Idempotency | Downstream scrapers/processors commonly use incremental/upsert semantics; dense CLV is bounded/resume-aware | No central contract or registry metadata; not proven for every retry/overlap path |
| Partial failure | Critical/noncritical classification in daily jobs; scheduler records terminal status | Some noncritical outcomes are only logs; MLB lines reports false success; settlement runs after critical failure |
| Metrics/alerts | Discord completion/failure, output-tail logs, `job_executions`, parser for selected scripts | Execution identity is script-only; many MLB/Kalshi jobs have no parsed metrics; missed APScheduler runs are not persisted |
| Env gates | Startup env status; NBA/arb registration gates; dense CLV/live trading job-level gates | Startup does not fail; requirements are not job-specific; disabled job can be recorded as successful no-op |

## Findings

### E-01 — MLB lines child failures are reported as scheduler success

Severity: Critical  
Confidence: High

Evidence:

- `src/orchestration/mlb_lines_job.py:65-101` converts child nonzero exits/exceptions into `False`.
- Callers ignore those booleans at `:218-222`, `:230-236`, `:243-247`, and `:262`.
- `main` logs completion but has no failure accumulator or nonzero exit at `:264-269`.
- Scheduler wrappers invoke this script repeatedly, often `silent_on_success=True` (`src/orchestration/scheduler.py:644-660`).
- Scheduler defines success only as child return code zero (`src/orchestration/scheduler.py:427-444`).
- No focused `mlb_lines_job` failure-propagation test surfaced in the scheduler-related test scan.

Concrete runtime failure mode:

A props scraper, game-lines scraper, incremental linker, or dense CLV linker can exit nonzero. `mlb_lines_job.py` logs the error but exits zero; `run_job` then records `status='success'`. Silent schedules emit no Discord failure, and full schedules can emit a success alert. Downstream inference then consumes stale or partially linked lines without a trustworthy orchestration failure signal.

Existing mitigation:

Child stderr is captured and logged inside `mlb_lines_job`; scheduler logs up to the last 50 combined output lines on apparent success. These are forensic clues, not correct status propagation.

Plan interaction:

Lane 07 Phase 3 intends to centralize runner status, but this is entrypoint behavior and must be characterized/fixed before registry parity treats current false success as a contract.

Safe first evidence step:

Add a pure unit test that mocks one `run_command`/group result as `False`, calls `main` with no real I/O, and asserts a nonzero `SystemExit`. Do not run a scrape.

Done condition:

Every MLB lines branch aggregates game-lines/props/linker outcomes; any required failed step causes nonzero process exit; focused tests cover props-only, parallel, sequential, dense-linker, and success paths; scheduler records/alerts failure.

### E-02 — Exact MLB lines schedule collisions have no cross-schedule lock

Severity: High  
Confidence: High

Evidence:

- Recurring close snapshots run `mlb_lines_job.py` every ten minutes on `:00` through `:50`, 10:00-23:59 (`src/orchestration/scheduler.py:1150-1157`).
- Fixed jobs run the same script at 12:00 (`:1062-1068`), 13:00 (`:1086-1092`), 17:00 (`:1118-1124`), and 18:00 (`:1126-1132`).
- All route to `mlb_lines_job.py` (`src/orchestration/scheduler.py:644-660`).
- `LOCKABLE_JOB_SCRIPTS` contains only `lines_job.py` (`src/orchestration/scheduler.py:116-121`); `_get_job_lock` returns no lock for MLB (`:327-332`).
- APScheduler jobs have different IDs, so per-job `max_instances` does not serialize them as one script-level resource.

Concrete runtime failure mode:

At 12:00, 13:00, 17:00, and 18:00 ET, two distinct APScheduler jobs can concurrently launch MLB Odds API scrapes and linkers against overlapping games/tables. This can double API consumption, increase DB/session pressure, race incremental linkers, and produce inconsistent close-snapshot bounds. E-01 can then hide one branch's failure.

Existing mitigation:

The close-snapshot path targets `mlb_player_props_clv_snapshots`, while normal lines target production tables; downstream writes are intended to be incremental/upsert-like. That reduces duplicate-row risk but does not serialize API and linker work.

Plan interaction:

Lane 07 identifies ordering/overlap metadata as an expansion checkpoint (`.hermes/plans/god-class-migrations/07-scheduler-job-registry-migration.md:581-596`) but its 45-job baseline predates this collision set. Registry specs need a shared resource/lock key, not only unique scheduler IDs.

Safe first evidence step:

Add a static schedule characterization test that expands critical cron minutes and asserts no two enabled jobs sharing an unsafe script/resource key collide. Separately inspect historical `job_executions`/Railway logs only after approval; no scrape is needed.

Done condition:

The four collisions are intentionally offset or share a tested lock/lease; skips or deferrals are observable; no concurrent `mlb_lines_job.py` children can start for the same worker/resource; schedule docs match the chosen behavior.

### E-03 — Nested subprocess timeout does not own the full process tree

Severity: High  
Confidence: Medium-High

Evidence:

- Scheduler times out only its direct orchestration child via `subprocess.run(..., timeout=timeout)` (`src/orchestration/scheduler.py:371-421`, `:446-482`).
- `lines_job.run_command` starts scraper/linker grandchildren with no timeout (`src/orchestration/lines_job.py:67-91`).
- `mlb_lines_job.run_command` does the same (`src/orchestration/mlb_lines_job.py:65-83`).
- `mlb_dense_clv_job.run_command` explicitly uses `timeout=None` (`src/orchestration/mlb_dense_clv_job.py:91-108`) beneath a 2h scheduler timeout (`src/orchestration/scheduler.py:663-665`).
- Daily-stats wrappers also create grandchildren with their own retry loops (`src/orchestration/daily_stats_job.py:275-362`; `src/orchestration/mlb_daily_stats_job.py:205-269`).
- No process-group/session creation or group termination is present in these subprocess calls.

Concrete runtime failure mode:

When the scheduler's direct wrapper reaches 45m/2h, Python terminates that direct child, but an active scraper/linker grandchild is not explicitly owned as a process group. It may continue API/DB work until Railway/container cleanup, while the scheduler records timeout and a scheduled retry or later trigger starts a second copy. Long daily-stats retry budgets can also approach/exceed the scheduler's outer deadline.

Existing mitigation:

The scheduler records timeout details and alerts; NBA/MLB daily stats have per-step timeouts; Railway restarts the service on process failure. None proves descendant termination.

Plan interaction:

Lane 07 Phase 3 extracts `job_runner`, but process-group semantics and nested timeout budgets are absent from its current tests. Add them as a named behavior-hardening slice, separate from extraction.

Safe first evidence step:

Use a local test fixture that launches a harmless sleeping child/grandchild, forces a short timeout, and checks that no descendant remains. Do not use production scripts or DB/API access.

Done condition:

A single owner launches each process tree, timeout terminates the full tree cross-platform (Railway Linux behavior required; local Windows behavior documented), nested timeout budgets cannot exceed the outer deadline unexpectedly, and tests prove no surviving descendant or duplicate retry.

### E-04 — Critical Railway CDN-only invariant lacks an executable scheduler guard

Severity: High  
Confidence: High

Evidence:

- Invariant: `AGENTS.md:20-21`.
- Current compliant command: `src/orchestration/daily_stats_job.py:391-395`.
- Current compliant scraper branch: `src/scrapers/nba_unified_scraper.py:931-932`, `:972-978`.
- Lane 07 Phase 0 explicitly requested a test preserving CDN-only behavior (`.hermes/plans/god-class-migrations/07-scheduler-job-registry-migration.md:322-340`).
- Existing `tests/test_pipeline_resilience.py` covers retry/timeout/lock behavior but not command inventory or `--cdn-only`; the targeted `nba_unified_scraper` test scan surfaced helper/data tests, not this orchestration invariant.

Concrete runtime failure mode:

A future wrapper/registry refactor drops `--cdn-only`. Railway then enters `nba_unified_scraper`'s normal path and can call `stats.nba.com`, violating the datacenter-IP invariant and causing daily stats failure or pressure to add unsafe advanced scraping to Railway.

Existing mitigation:

The argument and comments are hard-coded in current production code, and the scraper has a dedicated branch. The invariant is also documented in `AGENTS.md` and current scheduler understanding docs.

Plan interaction:

This is the missing first Lane 07 characterization guard and should precede registry extraction.

Safe first evidence step:

Add a static/isolated test that captures `daily_stats_job.main`'s first command and asserts `--cdn-only`, then assert the CDN-only scraper branch never invokes team/traditional/advanced `stats.nba.com` paths through mocks.

Done condition:

CI fails if the Railway daily-stats command loses `--cdn-only` or the scraper's CDN-only branch can reach `stats.nba.com`; registry metadata labels advanced-stat jobs local-only; no Railway schedule registers them.

### E-05 — Retry wrappers forget successful runs across scheduler restarts

Severity: Medium  
Confidence: High

Evidence:

- `check_dependency` has persistent `job_executions` fallback (`src/orchestration/scheduler.py:178-221`).
- NBA retry checks only `JOB_STATUS` (`src/orchestration/scheduler.py:550-564`).
- MLB daily-stats and roster retries also check only `JOB_STATUS` (`:634-641`, `:683-690`).
- Existing retry tests explicitly expect an empty `JOB_STATUS` to rerun (`tests/test_pipeline_resilience.py:307-350`).

Concrete runtime failure mode:

If Railway redeploys/restarts after a successful 09:00 job but before its retry window, memory is empty. The retry runs the full ingestion/processing/settlement path again despite a recent persistent success, consuming APIs/DB capacity and increasing exposure to non-idempotent side effects.

Existing mitigation:

Most ingestion paths are incremental or upsert-oriented, resolution code attempts to process pending records, and successful execution is available in `job_executions`. These reduce damage but do not avoid duplicate work.

Plan interaction:

Lane 07 Phase 5/policy extraction should preserve intentional retry semantics only after characterizing persistent-success behavior; its statement that all dependencies are memory-only is stale.

Safe first evidence step:

Add a unit test with empty `JOB_STATUS` and a mocked recent successful persistent execution, then decide whether retry policy should skip. No DB query is required in the test.

Done condition:

All retry wrappers use one tested dependency/retry policy with a date/window-aware persistent fallback; restart scenarios neither suppress a needed retry nor duplicate a successful run; retry decisions are logged and persisted.

### E-06 — Critical pipeline failure does not prevent settlement side effects

Severity: Medium  
Confidence: Medium

Evidence:

- NBA critical-step failure sets `success=False` and breaks (`src/orchestration/daily_stats_job.py:436-444`), but paper/user/DFS resolution and calibration still run (`:446-463`).
- MLB critical-step/freshness failure sets `success=False` (`src/orchestration/mlb_daily_stats_job.py:314-370`), but MLB bet resolution still runs (`:372-374`).
- Resolution exceptions are intentionally non-fatal (`src/orchestration/daily_stats_job.py:86-131`, `:187-272`; `src/orchestration/mlb_daily_stats_job.py:163-202`).

Concrete runtime failure mode:

After source stats, linking, rolling averages, or freshness validation fails, settlement code still executes against whatever data is available. The likely mitigation is that unresolved/missing-stat records are skipped, but that behavior was not proven in this bounded audit. A stale or partial source state could produce a no-op, delayed settlement, or incorrect settlement depending on resolver guards.

Existing mitigation:

Resolvers operate on pending records, report skipped dates, catch exceptions, and the top-level job still exits nonzero when the critical pipeline failed.

Plan interaction:

This is an entrypoint partial-failure policy, not only a registry concern. Lane 07 job specs should record critical outputs/dependencies but must not silently change settlement semantics during extraction.

Safe first evidence step:

Add characterization tests that force each critical phase to fail and mock the resolvers, documenting whether settlement must be skipped or is intentionally safe to run. Then inspect resolver preconditions in a separate bounded paper-trading audit if needed.

Done condition:

A documented and tested policy decides settlement after each critical failure class; unsafe states skip settlement with an actionable alert; safe continuation proves required source-date/finality guards.

### E-07 — Execution history collapses distinct schedules into one script identity

Severity: Medium  
Confidence: High

Evidence:

- `record_job_execution` stores `job_name` but receives only `script_name` (`src/orchestration/scheduler.py:137-172`, `:491-502`).
- `run_job` logs arguments at launch but does not persist them (`:371-382`).
- Distinct schedules share scripts: NBA lines (`:567-579`, `:617-619`), MLB lines (`:644-660`), NBA inference (`:582-609`), Kalshi NBA/MLB/resolve (`:724-737`), and arb modes (`:790-818`).

Concrete runtime failure mode:

A `job_executions` row named `mlb_lines_job.py` cannot identify whether it was full lines, props-only, or close snapshot; `kalshi_refresh_job.py` cannot identify NBA, MLB, or resolve-only. Operators cannot reliably attribute a failure/timeout/skip to its trigger, args, schedule ID, or env-gated mode without correlating ephemeral Railway logs.

Existing mitigation:

Start logs include `extra_args`; scheduler startup logs job names/triggers (`src/orchestration/scheduler.py:1310-1313`); Discord display names provide script-level labels.

Plan interaction:

Lane 07's `JobSpec`/execution telemetry target directly addresses this, but should persist stable scheduler ID, mode/args, trigger time, attempt, and gate outcome while retaining script name.

Safe first evidence step:

Define a pure execution-context/result contract and tests for two modes of the same script. Do not change the DB schema until a separate DB-safe plan/preflight is approved; structured logs can be the first evidence path.

Done condition:

Every run, skip, timeout, retry, and disabled no-op is attributable to scheduler ID plus script/mode/attempt in logs and persistent telemetry; existing history consumers remain compatible or are migrated explicitly.

### E-08 — APScheduler missed/max-instance events are not first-class telemetry

Severity: Medium  
Confidence: Medium-High

Evidence:

- Scheduler construction supplies only timezone (`src/orchestration/scheduler.py:850`).
- No explicit `job_defaults`, `max_instances`, `coalesce`, `misfire_grace_time`, executor sizing, or scheduler event listener exists in `scheduler.py`.
- High-frequency jobs run every 2-5 minutes (`src/orchestration/scheduler.py:923-937`, `:1217-1262`).
- Only a subprocess that reaches `run_job` can write `job_executions` (`:371-502`).

Concrete runtime failure mode:

If a job instance is still running, the thread pool is saturated, the process pauses, or a trigger is missed, APScheduler can skip/coalesce according to library defaults before `run_job` starts. The event appears only in scheduler logs; there is no `job_executions` row, Discord alert, or structured metric explaining the missing ingestion window.

Existing mitigation:

Railway logs include APScheduler messages, and critical jobs have later retries or repeated windows. NBA lines has its own explicit skip persistence after `run_job` starts.

Plan interaction:

Lane 07 schedule builder and telemetry extraction should make these defaults explicit and add event telemetry without changing schedules accidentally.

Safe first evidence step:

In an isolated unit test, construct the scheduler with a harmless blocked function and short interval, capture max-instance/misfire events, and define expected structured handling. Do not start the production scheduler.

Done condition:

Concurrency, coalescing, and misfire policy are explicit per job class; missed/max-instance events are observable and attributable; critical missed windows alert or invoke a tested recovery policy.

### E-09 — Startup environment validation is advisory and not job-aware

Severity: Medium  
Confidence: High

Evidence:

- `_validate_environment` labels `DATABASE_URL`, `ODDS_API_KEY`, and `RAPIDAPI_KEY` required (`src/orchestration/scheduler.py:505-520`).
- Missing required variables only produce warnings; scheduler startup continues (`:522-543`, `:841-850`).
- Registration/job gates are distributed across scheduler and child scripts: NBA full lines (`:901-952`), arbitrage (`:1269-1308`), dense CLV (`src/orchestration/mlb_dense_clv_job.py:124-129`), and Kalshi job-level checks.
- `NBA_FULL_LINES_ENABLED=false` does not disable props-only Odds API jobs (`docs/understanding/railway-scheduler.md:83-94`).

Concrete runtime failure mode:

A missing core variable can produce repeated high-frequency failures after a seemingly healthy scheduler startup. Conversely, `RAPIDAPI_KEY` is described globally required even when full injury jobs are disabled, creating noisy/misleading startup status. Job-level disabled paths can also exit zero and look like successful work rather than explicit disabled/no-op telemetry.

Existing mitigation:

Startup warnings name missing variables; child jobs often fail loudly or skip gracefully; Discord and `job_executions` surface launched-job outcomes.

Plan interaction:

Lane 07 already proposes job-specific env metadata (`.hermes/plans/god-class-migrations/07-scheduler-job-registry-migration.md:251-263`). Preserve startup compatibility first, then distinguish fatal core requirements, optional integrations, and disabled jobs.

Safe first evidence step:

Create a static requirements matrix from current wrappers/child guards and unit-test validation output for representative gate combinations. Do not inspect or print live Railway variables.

Done condition:

Each job declares required/optional variables and gate owner; startup reports enabled-but-unrunnable jobs distinctly; selected truly core requirements fail readiness intentionally; disabled jobs are recorded as disabled/no-op rather than success.

### E-10 — Schedule documentation and Lane 07 baseline are stale

Severity: Medium  
Confidence: High

Evidence:

- `docs/railway_deployment.md:7-18` says seven APScheduler definitions and old 11:00/noon/16:00 schedules; `:70-94` incorrectly describes Railway cron entries and UTC/DST behavior despite `railway.toml` only starting one worker.
- `docs/daily_pipeline_automation.md:7-30`, `:85-123`, and `:194-200` retain older 11:00/10:00 MLB/11:00-23:00 schedule prose.
- Current schedule truth is ET `CronTrigger` data in `src/orchestration/scheduler.py:850-1308`.
- The newer explainer explicitly identifies old docs as stale (`docs/understanding/railway-scheduler.md:5-10`, `:175-186`) but will itself drift without mechanical generation/validation.
- Lane 07 remains at 45 registrations and an older scheduler shape.

Concrete runtime failure mode:

Operators debug the wrong window, assume Railway cron/DST semantics that do not exist, believe a gate disables more jobs than it does, or build registry parity against 45 instead of 48 call sites/current gated counts.

Existing mitigation:

`docs/understanding/railway-scheduler.md` is a much closer current map and handoff 106 designates it plus source as current truth. `docs/understanding/tech-debt-register.md:137-179` already tracks stale docs and the 10:00 duplicate.

Plan interaction:

Refresh the Lane 07 characterization inventory before any implementation. Prefer one mechanically testable registry/schedule table rather than maintaining several independent prose schedules.

Safe first evidence step:

Generate a read-only schedule snapshot in a unit test or AST helper and compare IDs/counts/critical triggers to a checked characterization fixture. Mark old schedule sections historical or delegate them to the generated/current source.

Done condition:

One declared schedule source produces/tests all IDs, gates, triggers, wrapper args, timeout, and silence policy; Lane 07 counts/current controls match code; stale docs no longer claim Railway cron or old times.

## Rejected suspicions / false positives

1. **Rejected: Railway daily stats currently calls `stats.nba.com`.** The scheduled path hard-codes `--cdn-only`, and the scraper's branch explicitly skips `stats.nba.com` (`daily_stats_job.py:391-395`; `nba_unified_scraper.py:931-932`, `:972-978`). The missing regression guard remains E-04.
2. **Rejected: Railway uses `railway.toml` cron entries.** `railway.toml:7-10` only starts the always-on scheduler. APScheduler owns cron timing and ET/DST behavior.
3. **Rejected: NBA full-line jobs still collide at noon and 16:00.** They were moved to 12:01 and 16:01 (`scheduler.py:903-913`, `:941-952`), and `lines_job.py` has an in-process lock. The separate 10:00 duplicate remains known debt.
4. **Rejected: `NBA_FULL_LINES_ENABLED=false` pauses all NBA ingestion.** It removes only the two full-line registrations; props-only, inference, edge refresh, daily stats, and Kalshi NBA remain registered (`scheduler.py:901-952`; `docs/understanding/railway-scheduler.md:83-94`).
5. **Rejected: dependency checks are entirely memory-only.** `check_dependency` now falls back to persistent execution history (`scheduler.py:178-221`). Only retry wrappers remain memory-only.
6. **Rejected: all scheduler jobs can overlap freely.** `lines_job.py` has a tested nonblocking lock and persists a skipped status (`scheduler.py:116-121`, `:327-412`; `tests/test_pipeline_resilience.py:256-290`). The control is narrow and in-process.
7. **Not promoted: duplicate 10:00 NBA props trigger as a correctness failure.** It is real (`scheduler.py:883-889`, `:923-930`) but both paths use equivalent args, and the lock prevents concurrent children. Current evidence supports skip-noise/ambiguity, already tracked as TD-006, not demonstrated data corruption.
8. **Not promoted: global conformal/calibration behavior.** This audit did not assess model calibration. The Railway/no-advanced-stats and CDN-only invariants were the only model-adjacent constraints enforced here.

## Coverage gaps

- No live Railway variables, logs, deployments, process lists, or runtime job history were queried. Env-gate state and real collision frequency remain unverified.
- No DB queries were run. Idempotency/uniqueness claims for each target table, persistent retry history, connection pressure, and settlement correctness remain unverified.
- No scraper, processing entrypoint, scheduler loop, or production-like job was executed.
- Scheduler-related tests were read, not run. The user limited validation to the report and `git diff --check`.
- Deep internals of inference, Kalshi lifecycle, arb matching/trading, archive SQL, and paper/user/DFS resolvers belong to other audit lanes; only their scheduler-facing contracts were inventoried here.
- Direct entrypoints were mechanically scanned, but only the five ingestion orchestrators in the direct-entrypoint table received line-by-line failure/timeout review.
- APScheduler version-specific default semantics were not inferred from memory or a live package probe; E-02 relies only on distinct job IDs and missing script lock, while E-08 asks that defaults be made explicit and tested.
- Remote GBrain review was limited to handoffs 097, 104, 105, and 106 selected by scheduler/ingestion keywords. No broad Brain scan was performed.
- No workstation Task Scheduler/cron state was inspected. Older docs say local tasks were disabled, but current host-level truth is outside this bounded repo audit.

## Safe sequencing

1. RED characterization for E-01 (MLB failure propagation) and E-04 (CDN-only invariant).
2. Static schedule/resource collision characterization for E-02 and refresh the current 48-site Lane 07 baseline.
3. Harmless local process-tree timeout test for E-03.
4. Characterize restart-aware retry and critical-failure settlement semantics (E-05/E-06).
5. Only then update Lane 07 design and begin pure registry/result extraction. Keep schedule changes, DB telemetry schema changes, and behavior hardening as separately approved slices.

## Validation record

- Re-read the full `.hermes/audits/tech-debt/04-scheduler-ingestion-operations.md` report; required finding fields, false positives, coverage gaps, Lane 07 reconciliation, and CDN-only enforcement are present.
- `git diff --check -- .hermes/audits/tech-debt/04-scheduler-ingestion-operations.md` passed with no output.
- Scoped `git status --short -- .hermes/audits/tech-debt/04-scheduler-ingestion-operations.md` showed only this new report (`??`); no source, test, plan, register, or configuration file was edited.
