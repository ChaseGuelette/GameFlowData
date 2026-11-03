# NBA remote automation repair — November 2, 2026

Restore the existing remote job chain and its verified outputs. This is not authorization to replace it with a second linker or a manual runbook.

## Kick off immediately

- This chat: owns this incident, source repairs, independent review, educational wiki, bounded data verification and skill updates.
- Other chat: no work requested; protect the active NBA evidence/lifecycle/replay worktrees and their source/tests/plans.
- You: implementation requested; deployment/commit/push and exact schema or large historical-backfill operations remain separate review gates. Ordinary routine jobs should remain automated.
- Owner-launch clarification: prepare and validate code, then give Chase the commands for linker, sync, scraper, ingestion and derived/catch-up jobs. Do not launch writing smokes or register/start/reschedule OS tasks. Approval to resume bounded log inspection and staging the reviewed source snapshot was not data-job execution approval.
- Reading order: [pipeline wiki](../understanding/nba-production-data-pipeline.md), this contract, [existing incident](nba-reconciliation-incident-2026-11-02.md), [post-sync evidence](nba-post-sync-checks-2026-11-02.md).

## Scope and sequence

1. Daily Stats: establish the actual first failing child exception, improve non-secret diagnostic persistence, keep CDN-only ingestion, propagate internal source failures, and make rolling-feature catch-up follow completed source data rather than blindly selecting yesterday. Preserve shifted-history/eight-minute semantics. Verify real output, not only exit code.
2. Scheduler and linker: repair actual production callers to use bounded, resumable PK work through the existing linker. Cover an entire selected ingestion interval or explicitly report its remaining cursor; do not silently substitute a latest-N sample for complete interval coverage. Independent game/player/team fills, exact unique reference matches, null-only optimistic writes, fresh future schedule references, no fuzzy assignments/shared permanent staging/unbounded initial counts.
3. Ingestion: verify and prepare the existing `nba_game_schedule` schema/ingestion path, deploy-time/runtime wiring and freshness failures. The existing migration is the starting point, not permission to create raw-table indexes or claim mutable schedule snapshots are historical evidence.
4. Mirror sync: identify its actual host/schedule, finish scoped new-row catch-up, propagate canonical corrections when necessary, and verify parity. Reuse existing proven PK sync; no full truncate/reload. Check small-table schemas/keys before upserts and distinguish future-commence watermark traps from absence of data.
5. Historical readiness: resolve what can be demonstrated with bounded current/archive/source-clock evidence; use the approved cohort/policy if one exists. If no owner-approved temporal/cohort policy exists, prepare the exact decision rather than inventing one. Do not claim S1/S2 acceptance from code tests or a latest-N sample.

DFS-table errors, model/feature/calibration changes, new unit tests, broad historical scrapes, archive bulk copies and production raw-table indexes are excluded.

## Discovery and ownership

Status: scraper/Daily Stats/linker plus props-follow, bounded game-lines sync and opponent-history repairs are integrated into main's working tree, uncommitted and undeployed. The initial review blockers were corrected; final integration tests pass 139 existing cases without test/fixture changes. A subsequent local [advanced transport repair](../understanding/nba-advanced-scraper-connection-repair.md) addressed SOCKS/direct-requests failures with a pinned browser-compatible curl session and explicit direct routing. Chase authorized single-game smokes: game `0022600001` was fetched/saved by the real CLI, exit zero, with independent canonical readback tracked in that wiki. This is the only new data-job execution exception; no bulk/mirror/linker/derived job, migration, deployment or OS-task change was launched. Automatic support-table transport, schema/state initialization, deployment and historical policy remain gates. Owner commands: [retest handoff](nba-scraper-retest-handoff-2026-11-02.md).

Candidate source scope (narrowed per implementation handoff after discovery):

- `src/orchestration/daily_stats_job.py`
- `src/scrapers/nba_unified_scraper.py`
- `src/scrapers/nba_cdn_scraper.py` only if its existing source accounting/window API requires repair
- `src/processing/nba_linker_local.py` (preserve the already reviewed local bounded repair)
- `src/orchestration/lines_job.py`
- `src/orchestration/scheduler.py`
- `src/scrapers/nba_schedule.py` and the existing schedule migration only if a concrete compatibility defect is verified
- existing explicit sync entrypoints, after actual schedule/host discovery

Workers get disjoint file allowlists and a written executable handoff. A worker touching Daily Stats owns its linker-call wiring; a linker worker supplies the reviewed public invocation contract rather than concurrently editing that file. Preserve all existing test files and fixtures.

## Validation and gates

- Existing focused suites only, plus disposable bounded execution probes. No authored/edited unit tests.
- Ruff, compile, scoped whitespace/diff review and differential Pyright against the untouched baseline where relevant.
- DB reads through isolated read-only SQL runners with actual target identity, keyed access and short timeouts.
- Independently review source diffs and runtime evidence before accepting worker output.
- Record separate code, deployment, production-output, mirror-integrity and historical-readiness verdicts.
- Schema changes: exact migration, target and preflight before approval/execution; no broad locks/index creation on raw props.
- Deployment: review exact tracked/untracked source allowlist, obtain commit/push/deploy approval, then read back the actual deployed commit and real scheduled outputs.
- Large resource/backfill work: probe disk/DB/window and obtain the required owner execution decision before launch.

## Current baseline

- Main HEAD at discovery: `3aa73a5fbed0ebd7a3974f1e0f76793a58890c27`.
- Existing linker local repair: six existing tests and 45 guarded checks passed; differential Pyright has zero introduced errors. It is uncommitted/undeployed.
- Prior read-only latest-1,000 preview: no missing IDs/proposals; future identities remain unverified, not proof of historical correctness.
- Prior full props mirror: transferred and bounded-parity checked; later appends/canonical repairs are a separate catch-up.
- Source/derived/job freshness is being rechecked; old watermarks are evidence, not a claim about current production.
