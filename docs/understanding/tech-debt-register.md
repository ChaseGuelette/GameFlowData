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

### TD-005: Production scheduler/job complexity is hard to explain

Status: candidate
Area: Railway / orchestration
Evidence:
- Recent handoffs around NBA lines hardening, Supabase connection pressure, overlap locks, and env gates show scheduler behavior has many interacting flags/jobs.
Why it matters:
- Chase needs to know which jobs are running, which are gated, and what operational alerts mean.
Current workaround:
- Use handoff-105 behavior notes and `daily_pipeline_automation.md` for schedule context.
Risk if ignored:
- Off-season gates, props-only refreshes, and full-line jobs get confused; agents may pause or debug the wrong layer.
Safe first step:
- Create/update a scheduler explainer table from current `src/orchestration/scheduler.py`, with env gates and job names.
Validation:
- Chase can answer: which NBA/MLB jobs run, on what schedule, what env flags gate them, and what alerts indicate.
Owner decision needed:
- Confirm whether scheduler explanation is the first guided review subsystem.

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
