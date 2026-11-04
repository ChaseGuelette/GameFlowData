
# Layer 7 — Daily orchestration and data quality

- Revision: 2026-11-03 / v1. Status: operational design; all scheduled NCAAB jobs remain disabled until separately approved.
- Goal: maintain current inputs and reproducible paper predictions without overlapping writers, hidden stale features, or accidental long jobs.
- Architecture: thin wrappers in the retained scheduler directory call domain job services; DB-fenced leases and persisted readiness evidence protect the full path.

[Previous: evaluation](06-evaluation-and-forward-tracking.md) | [Shared contracts](../architecture-overview.md#shared-contracts) | [Summary](../README.md)

## 1. Retained scheduler integration

Inspected `src/orchestration/scheduler.py` uses `BlockingScheduler(timezone='America/New_York')`. Its `run_job(script_name, extra_args='', silent_on_success=False, timeout=2700)` resolves scripts beneath `src/orchestration/` and invokes a subprocess with argument-array construction.

`JOB_NAMES` supplies display names; zero-argument wrapper functions and `CronTrigger`/stable job IDs register work. `job_executions` is the existing shell-level log. Existing locking is process-local and applies only to an explicit script allowlist; it is not multi-process protection. Existing dependency success records do not certify NCAAB table/feature readiness.

Do not alter NBA/MLB scheduling, source policies, alerts, model loads or dependency graphs as part of NCAAB integration. Add only default-off registrations and named wrappers after this layer's acceptance gate.

## 2. Proposed files and integration changes

| File | Surface | Ownership |
|---|---|---|
| `src/ncaab/operations/jobs.py` | `NcaabJobService`, `JobRequest`, `JobResult` | Coordinate layer services and reconcile outputs |
| `src/ncaab/operations/readiness.py` | `ReadinessService`, `ReadinessReport`, named check registry | Source/identity/feature/quote/artifact/output checks |
| `src/ncaab/operations/leases.py` | `LeaseManager`, `LeaseKeeper`, `LeaseToken` | Cross-process claim, heartbeat, fencing and release |
| `src/orchestration/ncaab_stats_job.py` | Import-safe `main()` | Approved bounded source refresh and derived-input maintenance |
| `src/orchestration/ncaab_odds_job.py` | Import-safe `main()` | Approved current quote capture; no automatic historical backfill |
| `src/orchestration/ncaab_inference_job.py` | Import-safe `main()` | Due-game prediction/decision and paper recording |
| `src/orchestration/ncaab_settlement_job.py` | Import-safe `main()` | Pending-paper reconciliation and closing evidence |
| `src/orchestration/scheduler.py` | Scoped future modification | Display names, four wrapper functions, explicit registrations/process locks |
| `configs/ncaab/paper.yaml` | Operational config | Reviewed data roots, books, rulesets, resource ceilings and release IDs |

Separate wrapper filenames preserve distinct existing scheduler status/log identities. One generic wrapper filename with four unrelated tasks would collide in the retained script-keyed status model. Domain logic remains in `src/ncaab`, not copied across wrappers.

```python
class NcaabJobService:
    def refresh_stats(self, request: JobRequest) -> JobResult: ...
    def capture_odds(self, request: JobRequest) -> JobResult: ...
    def predict_due_games(self, request: JobRequest) -> JobResult: ...
    def settle_paper(self, request: JobRequest) -> JobResult: ...

class ReadinessService:
    def evaluate(self, game_ids: Sequence[int], context: AsOfContext,
                 *, capabilities: ModelCapabilities) -> ReadinessReport: ...

class LeaseManager:
    def acquire(self, scope: str, *, ttl_seconds: int) -> LeaseToken: ...
    def renew(self, token: LeaseToken) -> LeaseToken: ...
    def release(self, token: LeaseToken) -> None: ...
```

## 3. Enablement and starting cadence

These are proposed defaults, not enabled schedules:

| Job | Starting schedule | Bounded purpose |
|---|---|---|
| Stats | 07:00 America/New_York daily | Previous three ET dates plus unfinished/retryable games; new finals first, then corrections |
| Odds | Every five minutes inside the active event window | Current snapshots from earliest eligible start minus 90 minutes through last start minus five minutes |
| Inference | Every minute when due games exist | Intended T-60 decisions; maximum dispatch lateness 90 seconds |
| Settlement | Every 30 minutes when unresolved paper records exist | Final results, corrections, and available closing observations |

Use timezone-aware triggers and test DST transitions. No fixed UTC time masquerades as a permanent ET schedule. The active-window computation must handle date boundaries, late games and reschedules.

Registration gate: `NCAAB_SCHEDULED_JOBS_ENABLED=false` by default. Component gates: `NCAAB_STATS_ENABLED`, `NCAAB_ODDS_ENABLED`, `NCAAB_PAPER_ENABLED`, all false by default. Manual research CLI execution is separately explicit and does not require enabling recurring production jobs.

For each registration, set stable ID, `max_instances=1`, explicit coalescing and misfire policy. Inference misfires beyond the decision window are skipped, not replayed as bets; source refresh can coalesce into one bounded catch-up. Do not infer adequate process protection from APScheduler's per-instance setting alone.

## 4. Job flows

### Stats refresh

Acquire stats lease -> resolve date/task plan -> discover schedules -> fetch/persist new or corrected final games -> verify expected record coverage -> invalidate/update relevant feature caches and optional forward states -> write readiness/run receipt -> release lease.

The job does not retrain model weights. State filtering after newly observed games is separate from fitting global state parameters. A completed source job does not mark derived features current unless the consumer check succeeds.

### Quote capture

Acquire odds lease -> check approved quota budget and eligible event window -> perform current sport-wide request -> persist coherent source snapshots/outcomes -> link known games -> record link/coverage/freshness -> release.

No automatic paid historical backfill repairs a missing live capture. A missed current observation stays missing historically; later prices cannot be backdated. Out-of-window/no-eligible-game work is a truthful no-op only after schedule state is verified.

### Due-game inference

Acquire inference lease -> identify unrecorded intended slots -> resolve immutable releases -> establish actual decision cutoff -> evaluate model-specific readiness -> call shared prediction/decision engine -> atomically persist decisions and idempotent paper selections -> write result counts -> release.

Do not predict stale games merely because yesterday's job succeeded. Missing inputs block affected games/variants; unrelated valid games may continue with a degraded run result. A missing aware reference market need not block independent prediction, but it prevents a fair paired comparison for that opportunity.

### Settlement

Acquire settlement lease -> select bounded unresolved/correction candidates -> obtain eligible final-result revisions -> apply recorded book rules -> append settlements -> compute/match closing evidence -> write counters -> release.

Settlement never opens new wagers or rewrites a finalized historical report. Manual-review cases remain visible with reason and age.

## 5. Leases, transactions and overlapping jobs

Use `ncaab_job_leases`, not a long transaction held during HTTP. Claim by atomic insert/conditional update of an expired scope, returning an owner token and incremented fence. Renew only when token/fence still match. Release only the caller's lease.

Initial TTL is 15 minutes with a one-minute heartbeat; bounded job runtime may exceed one TTL only while renewal succeeds. `LeaseKeeper` uses short independent DB operations, signals loss to the worker, and does not hold a connection/transaction while waiting on the network. Every persistence batch rechecks the owner/fence; a paused old worker cannot resume writing after a new owner acquired the scope.

Scopes distinguish canonical stats, canonical odds, inference experiment and settlement. Manual and scheduled invocations share scope semantics. A second active writer is `skipped_active_lease`, not a second backfill. Whole-pipeline locks are not required when different jobs touch independent append-only data and readers pin snapshots.

Persist task checkpoints and data atomically. A timeout or canceled transaction is rolled back/closed before another query. Use the retained engine's pool safeguards; do not open one permanent connection per scheduled component. Fenced leases do not authorize destructive work.

## 6. Readiness contract

Persist named checks in `ncaab_readiness_checks` with context, policy/version and evidence. Separate gates:

| Gate | Required evidence | Failure behavior |
|---|---|---|
| Source | Expected completed games present with required team fields and valid revisions | Block affected histories; do not use MAX(date) alone |
| Identity/cohort | Verified mappings/orientation and season-specific eligibility | Block unresolved games |
| Temporal | Supported clocks/mode and no future observations | Block invalid inputs; label reconstruction honestly |
| Features | Exact schema/order, allowed nulls, source/state hashes and consumer success | Block incompatible/undeclared defaults |
| Artifact | Digest, capabilities, fit/selection bounds and paper release | Block invalid or unapproved artifacts |
| Quotes | Allowlisted coherent pairs, freshness and pregame eligibility | Block decisions; preserve prediction coverage separately |
| Outputs | Expected predictions/decisions committed and counters reconciled | Degraded/failed job, not successful empty output |
| Settlement | Valid result/ruleset and no unresolved contradictions | Pending/manual-review, never invented outcomes |

Cold starts, DNP, no scheduled games, missing raw rows and a provider outage are distinct states. Optional roster/state capabilities require their own checks rather than blocking the baseline on unused tables.

A paper candidate trained on reconstructed history may be evaluated prospectively with strict observed inputs after explicit approval. Source-grade labels are not a substitute for model evidence, and code readiness is not profitability certification.

## 7. Result/status and observability

`JobResult` contains job/run/lease IDs, config/release/source hashes, start/finish, status, expected/observed counts, blocked-game reasons, request/credit usage, retry/deferred tasks and output refs. Emit a sanitized structured summary to stdout and persist it in `ncaab_job_runs`.

- `completed`: all requested bounded work reconciled, including a verified no-work case.
- `degraded`: partial usable output with explicit blocked/deferred work.
- `failed`: unrecoverable or shared-contract failure.
- `skipped_active_lease` / `disabled`: no execution, no freshness advancement.

Execution exit codes: 0 for completed/explicit disabled/skipped; nonzero for degraded/failed so the retained wrapper cannot turn partial work into green success. The NCAAB readiness layer uses domain receipts, not a generic successful shell exit. Dry run produces a plan only and no persistent job success.

Reuse the retained scheduler's alert channel for a concise run summary; do not build another dashboard/notification framework. Alert on authentication/schema failures, missed inference slots, persistent missing games, quota exhaustion, invalid artifacts, lost leases and aged unsettled paper records. Aggregate repeated game-level warnings to avoid alert floods. Never log DB URLs, API keys or raw exception request URLs.

## 8. Resource bounds and correction recovery

Every job has explicit maximum tasks/games, HTTP calls/bytes/credits, DB batch size, deadline and worker count. Initial planning defaults: HTTP concurrency two, one request/second per upstream host, three total attempts, 10-second connect/30-second read timeouts; DB write batches no larger than 500 records. These are conservative controls, not published provider guarantees. Actual task/credit/day ceilings must be supplied before enablement.

New finals are prioritized over optional correction refreshes. Reaching a limit leaves resumable deferred tasks and a degraded receipt; do not delete the backlog or claim freshness. Historical backfills and full training/sweeps are never auto-started as a repair.

Recovery patterns:

- Provider outage: retain last verified data, block affected predictions, retry within budget; no silent stale-data substitution.
- Schema change: quarantine payload, pin prior parser, investigate; no dynamic numeric-column guessing.
- Identity ambiguity: retain source bytes/quotes, resolve mapping, reprocess locally rather than refetch paid history.
- Late correction: append source/result revision, invalidate affected feature hashes/settlements, preserve frozen prior reports.
- Artifact corruption: reject load and retire/disable that release; do not fall back to arbitrary legacy files.
- Lease loss: stop writes and leave resumable checkpoints.

## 9. Deployment and acceptance ladder

The retained `nixpacks.toml` install phase runs `/app/venv/bin/python -m pip install -r requirements.txt`; both it and `railway.toml` start `src/orchestration/scheduler.py`. The future deployment edit is limited to selecting `requirements-ncaab.txt` in that install phase when NCAAB is approved. Preserve the existing Python 3.11/shared-library setup and scheduler start command. Do not create a second always-on service or silently add neural dependencies to the common worker. These files were inspected, not changed.

1. Offline contract/unit tests and import-safe help.
2. Controlled end-to-end fixture from ingestion through features, prediction and settlement, with no network or live DB.
3. Isolated test-DB migration/repository and lease race tests.
4. Explicit bounded read-only source canary from the actual deployment host, including current scoreboard and a completed game's team/player boxes.
5. Verify durable payload/artifact storage, dependency install path, backend DB writability, release/config identities, and clock/timezone behavior.
6. Owner-approved bounded writing smoke, then daily source maintenance while paper inference remains disabled.
7. Verify source and derived readiness/output counters over recurring runs, then explicitly approve a forward-paper release and enable its jobs.

No single local HTTP response, fixture suite or successful deployment skips this ladder. Dependency installation must include the selected NCAAB overlay where needed; verify the actual retained deployment build command before changing it. Optional neural dependencies must not break the baseline serving process.

The local mirror has a separate explicit freshness/transfer process. A canonical remote refresh does not prove local parity, and a local historical dataset does not prove canonical source freshness. Do not trigger broad NBA/MLB or all-sports syncs from these jobs.

## 10. CLI, tests and implementation slices

Planned commands only:

```powershell
.\venv\Scripts\python.exe -m src.ncaab.cli health --config configs/ncaab/paper.yaml --read-only
.\venv\Scripts\python.exe -m src.ncaab.cli run-job --job stats --config configs/ncaab/paper.yaml --dry-run
.\venv\Scripts\python.exe -m src.ncaab.cli run-job --job inference --config configs/ncaab/paper.yaml --dry-run
```

`--read-only` health cannot repair data or activate a release. All writing jobs require explicit execution authority; enabling recurring jobs authorizes only their frozen bounded maintenance scope, not arbitrary remediation.

Implement readiness checks and structured receipts first, then leased job services, thin wrappers, isolated integration tests, and finally default-off scheduler registration. Do not enable jobs as part of merging the code.

Proposed tests: `tests/ncaab/test_readiness.py`, `test_job_leases.py`, `test_job_services.py`, `test_scheduler_integration.py`, `test_operational_recovery.py`. Existing `tests/test_pipeline_resilience.py` and `tests/test_db_client.py` are protected regression targets for the eventual integration change.

Acceptance includes overlapping processes/fencing, lost heartbeat, timeout rollback, bounded partial work, zero-game versus outage handling, stale-source/fresh-job mismatch, disabled registrations making no calls, DST/reschedules, missed inference slots, immutable release resolution, no repeated paper bets, correct exit status and no effects on NBA/MLB registrations.

## References

Existing inspected integration: `src/orchestration/scheduler.py:343` (`run_job`), its `JOB_NAMES`/`LOCKABLE_JOB_SCRIPTS` registration pattern, and `src/db/client.py`. Supporting scheduler semantics: [APScheduler 3.x user guide](https://apscheduler.readthedocs.io/en/3.x/userguide.html). Exact code changes remain future scoped work.
