# Tech Debt Register

This register is for evidence-backed debt and migration candidates across GameFlowData, Hermes Agent usage, GBrain/GameFlowBrain, and production operations.

It is not a dumping ground for vague unease. A debt item should help Chase decide what to confirm, shelf, fix, or explicitly accept.

## Status taxonomy

| Status | Meaning |
|---|---|
| candidate | Some evidence exists, but scope/priority is not confirmed. |
| confirmed | Chase or strong evidence confirms this is real debt worth tracking. |
| planned | A migration/remediation plan exists. |
| in-progress | Work has started. |
| resolved | Fix/mitigation landed and validation passed. |
| accepted | Known debt intentionally left in place for now. |
| rejected | Investigation showed this is not a real/current problem. |

## Evidence rules

Every entry needs at least one of:

- exact code path/function;
- failing or missing test evidence;
- production log/incident/handoff;
- GBrain canonical page/lesson;
- repeated Chase confusion;
- explicit Chase observation;
- measured performance/reliability issue.

If the evidence is only “this feels messy,” keep it out of the register until a bounded audit creates evidence.

## Entry template

```markdown
### TD-000: Title

Status: candidate
Area: repo / Hermes / GBrain / pipeline / model / infra / dashboard / DB
Evidence:
- Path/log/page/user observation.
Why it matters:
- What risk or confusion this creates.
Current workaround:
- How we avoid damage today.
Risk if ignored:
- Concrete failure mode.
Safe first step:
- Small investigation or no-risk refactor.
Validation:
- How we prove the issue is fixed or accepted.
Owner decision needed:
- Confirm / shelf / ignore / approve plan.
```

## Candidate items

### TD-001: GBrain direct-read/source-routing fragility

Status: candidate
Area: GBrain / Hermes MCP
Evidence:
- `gameflow resume` can retrieve source-scoped query results for known pages while direct `get_page` / `list_pages` fails or returns empty in some sessions.
- This behavior is documented in GameFlow resume skill caveats and recent resume output.
Why it matters:
- Agents may incorrectly report that GBrain is down or that pages are missing.
- Resume reliability and Chase trust suffer when retrieval paths disagree.
Current workaround:
- Use `mcp_gbrain_query(..., source_id='__all__')` fallback and label the route.
Risk if ignored:
- Stale/incorrect resumes, repeated debugging, and accidental local markdown fallback against the wrong brain.
Safe first step:
- Create a quiet-window smoke test that compares direct page reads, list_pages, query, raw MCP, and remote CLI for a small known-slug set.
Validation:
- Fresh Hermes session can retrieve latest handoff, `execution-plan`, `operations/critical-invariants`, and `operations/hard-facts` through the intended direct route or a documented explicit source route.
Owner decision needed:
- Confirm whether to prioritize a GBrain source-routing fix versus keep fallback discipline.

### TD-002: GBrain stale-pages metadata hygiene ambiguity

Status: candidate
Area: GBrain / GameFlowBrain maintenance
Evidence:
- GBrain health can show high `stale_pages` while `embed_coverage=1`, `missing_embeddings=0`, and `orphan_pages=0`.
- Resume skill says this may be timeline metadata hygiene rather than failed sync/embedding.
Why it matters:
- Health warnings become noisy if Chase and agents cannot tell real staleness from metadata artifacts.
Current workaround:
- Do not call it stale embeddings unless embed checks prove it.
Risk if ignored:
- Agents may run unnecessary repairs or dismiss real future health problems because warnings are noisy.
Safe first step:
- Run the documented stale-pages diagnostic during a maintenance window and classify exact stale pages by cause.
Validation:
- Health interpretation docs explain what stale_pages means in current GBrain and what action, if any, is required.
Owner decision needed:
- Confirm whether this is worth a cleanup pass now or should remain a known warning.

### TD-003: Structural migration lanes remain partially complete

Status: candidate
Area: repo architecture / god-class migrations
Evidence:
- `.hermes/plans/god-class-migrations/README.md` tracks multiple migration lanes; some are complete, core-complete, medium priority, or documentation-only.
Why it matters:
- The repo can keep accumulating large orchestrators/components if responsibility boundaries are not finished.
Current workaround:
- Use lane-specific plans and progress logs before touching a migration lane.
Risk if ignored:
- New work attaches to old god classes and makes future migrations harder.
Safe first step:
- Run a lane status review that distinguishes documentation complete, partial implementation, complete, and accepted-deferred.
Validation:
- Updated migration index with current lane statuses and next approved slice.
Owner decision needed:
- Choose whether the next structural lane is worth prioritizing over model/ops work.

### TD-004: MLB stat-suite tooling fragmentation

Status: candidate
Area: MLB modeling / tooling
Evidence:
- Recent MLB stat-suite rebuild plans and commits added generic ablation runner support, shared feature controls, and artifact helpers after batter_hits and pitcher_strikeouts diverged.
Why it matters:
- Divergent one-off scripts make model experiments harder to compare and easier to run incorrectly.
Current workaround:
- Use the stat-suite rebuild plan and dry-run commands before any long training.
Risk if ignored:
- New stat lanes clone old wrappers or bypass validation gates.
Safe first step:
- Review current stat-suite plan status and run only dry-run/help/static tests for the intended next slice.
Validation:
- Pitcher and batter lanes share documented runner/artifact contracts without changing model math unexpectedly.
Owner decision needed:
- Confirm whether to continue the shared suite migration before more model experiments.

### TD-005: Production scheduler/job complexity and stale docs

Status: confirmed
Area: Railway / orchestration
Evidence:
- Recent handoffs around NBA lines hardening, Supabase connection pressure, overlap locks, and env gates show scheduler behavior has many interacting flags/jobs.
- `src/orchestration/scheduler.py` currently defines one always-on APScheduler process with many NBA, MLB, Kalshi, maintenance, and env-gated arbitrage jobs.
- `docs/railway_deployment.md` still says the worker has "7 APScheduler job definitions" and lists older 11 AM / noon / 4 PM-era schedules, which no longer matches the current scheduler code.
- `docs/daily_pipeline_automation.md` still contains older Railway schedule prose in places, while its resilience section remains useful for `JOB_STATUS` / `job_executions` concepts.
- `tests/test_pipeline_resilience.py` covers several scheduler safety behaviors, proving this is important enough to test: dependency checks, retry behavior, timeout persistence, deferred NBA lines failure tagging, and `lines_job.py` overlap skipping.
Why it matters:
- Chase needs to know which jobs are running, which are gated, and what operational alerts mean.
- Stale schedule docs can cause agents or Chase to debug the wrong runtime window or misunderstand what `NBA_FULL_LINES_ENABLED=false` actually pauses.
Current workaround:
- Use `docs/understanding/railway-scheduler.md` plus current `src/orchestration/scheduler.py` as the human/code pair for schedule understanding.
Risk if ignored:
- Off-season gates, props-only refreshes, and full-line jobs get confused; agents may pause or debug the wrong layer.
Safe first step:
- Decide whether to refresh or deprecate the older schedule sections in `docs/railway_deployment.md` and `docs/daily_pipeline_automation.md` so they point at the newer scheduler explainer instead of drifting independently.
Validation:
- Chase can answer: which NBA/MLB/Kalshi/arb jobs run, on what schedule, what env flags gate them, what alerts indicate, and where to check real runtime history.
Owner decision needed:
- Confirm whether to treat old schedule docs as archived historical guides or update them to delegate schedule truth to `docs/understanding/railway-scheduler.md`.

### TD-006: Duplicate 10 AM NBA props-only scheduler trigger

Status: candidate
Area: Railway / orchestration
Evidence:
- `src/orchestration/scheduler.py` adds `lines_props_10am` at 10:00 AM ET and also adds `props_every_5` for every 5 minutes from 9 AM through 11 PM, which includes 10:00 AM.
- `LOCKABLE_JOB_SCRIPTS = {"lines_job.py"}` means the second simultaneous NBA lines launch should be skipped and persisted as `status='skipped'`, not run concurrently.
Why it matters:
- The lock likely prevents overlap damage, but the duplicate trigger can create noisy skipped job history and confuse schedule explanations.
Current workaround:
- The in-process `lines_job.py` lock prevents concurrent subprocess execution.
Risk if ignored:
- Agents or Chase may misread expected 10 AM skips as a failure, or future schedule edits may accidentally rely on duplicate behavior.
Safe first step:
- Check Railway logs / `job_executions` around 10:00 AM ET for repeated skipped `lines_job.py` rows, then decide whether to remove the explicit `lines_props_10am` trigger or offset it.
Validation:
- After the chosen cleanup, 10:00 AM ET has exactly one intended NBA props-only launch path and no routine lock-skip row.
Owner decision needed:
- Confirm whether to clean this up now or accept the harmless skip noise until a scheduler refactor pass.

## Review cadence

Manual for now:
- review after major incidents;
- review after large migration/doc sessions;
- review weekly only if Chase asks.

Suggested review output:

```text
Tech debt review
- Confirm now:
- Shelf:
- Reject/ignore:
- Needs evidence:
- Recommended first safe action:
```
