---
title: Pending handoff — NBA Slice 8 native source architecture
type: handoff
status: pending-sync
owner: Chase
effective_date: 2026-11-03
tags: [handoff, nba, historical-sources]
---

# Pending handoff — NBA Slice 8 native source architecture

**Date**: 2026-11-03, morning EST. Final code check 09:44; wrap-up Git readback 09:46.

## Publication gap

Canonical brain is `gameflow-agent:/home/chase/GameFlowBrain`, not local WSL. Tailscale SSH requested an additional authentication check and timed out; no remote file write/commit/sync/graph backfill/push occurred. Preserve this draft locally. After authentication, acquire the existing remote sync lock, choose the next unused handoff number, promote this body, update the handoff hub and replace the superseded NBA execution-plan checkpoint, commit/sync/embed/graph and verify exact readback. Last MCP-visible canonical handoff was `handoffs/handoff-136`; do not assume the next number is free without remote Git inspection.

MCP pre-wrap health: 250 pages, full embedding coverage, zero missing embeddings and zero orphan pages. `stale_pages=171` is a pre-existing health metric, not evidence of missing embeddings. These figures do not include this unsynced draft. Engineering OS remains paused at read-only MVP 0; private Tailscale dashboard direction and owner promotion/schedule gates are unchanged.

## Owner direction — supersedes the earlier blocked-code approach

Chase owns data repair/coverage. Continue actual code architecture without using missing data as a reason to leave source readers unimplemented. Coordinator implements directly; delegate only explicitly requested localized fixes, not another writer/reviewer fan-out. Minimal working paths first, existing affected suites plus a short populated path check, no expensive scratch matrices. Give progress about every five minutes during active work. No long model jobs, broad backfills, DB/schema changes or publication inferred.

Confirmed domain choices: T-60; strict missing/ambiguous source, injury history and commence blocking; no diagnostic fallback; target minutes 5 and rolling contribution minutes 8. Exact cohort/split/family policy freeze remains open; timed-out forms are not approvals.

## Actual source location — protect uncommitted work

- Worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-slice8-consumers`.
- Branch: `repair/nba-slice8-consumers`.
- Base/HEAD: `b24a69f36dc7f7d51aef3a78381af209e6a9530a`.
- Slice 8 source/plans/docs are uncommitted, unmerged and unpushed. New source modules are untracked; plain git diff omits them. Do not delete/reset this worktree or infer integration from documentation.
- Earlier S1 was owner-authorized, committed as `9c0dd003` and merged locally as `b24a69f3`; no push/deployment was performed by this lane.
- Current main at wrap-up: `04cdb08a33133ee4b2482903d72edb689eb451b4`. Concurrent support-table sync commits (`af093150`, merge `bf740ad6`, CLI credential fix `cf9325d9`, documentation `04cdb08a`) are not Slice 8 integration. Main has separate position-repair/readiness changes, which were left untouched. Origin status was not newly verified.

## Delivered code

- `historical_loader.py`: shared historical assembler and source registration; normal requests now call real source acquisition instead of the unconditional unavailable-adapter gate. Caller-built evidence remains controlled-only.
- New `historical_sources.py`: `PostgresHistoricalSources`, `HistoricalSourceMetadata`, `HistoricalSourceDataError`. Reads raw target/basic/advanced/team/position/defense/market/injury tables, constructs existing typed evidence and reuses numerical transforms. Explicit metadata may be embedded per row or supplied by a sidecar; no DB migration is required by the file route.
- Date/training/range public FeatureStore APIs share the normal reader. Native output readiness is `source_integrated`, controlled readiness is `controlled_only`; both remain `decision_grade=False`.
- Injury weights are bulk as-of reads, including OUT players without target-game box scores. Defense season is joined from its own game/team source identity because the defense writer has no season column.
- `historical_replay.py` and narrow harness seams feed the same native reader's raw quotes, contexts and explicit routes into the accepted pure quote adapter for direct/prefetch mode. Context configuration is not mutated between dates. Exact pair/clock metadata survives edge merge, numeric crossings are guarded, query/cap failures propagate and ledger export preserves exact values.
- One delegated localized repair was released and incorporated: overlapping injury basic/advanced columns and wrong-target-season contribution snapshots now block; static season-type exclusion precedes target validation. No new review fan-out was started.
- Earlier pure adapter and controlled replay had independent acceptance. Parent native integration changed subsequent bytes: do not inherit those approvals as final native-source acceptance.

Key source files:
`src/models/features/nba/{historical_sources,historical_loader,requests,date_batch_loader,date_range_loader,training_loader,line_sources,injury_context}.py`, `src/models/feature_store.py`, `src/backtesting/nba/{historical_quote_adapter,historical_replay}.py`, `src/backtesting/backtest_harness.py`.

## Verification and explicit limits

- Final populated normal-path probe passed actual reader -> default FeatureStore -> training/date/range -> actual direct/prefetch harness. Only external DB reads and model predictions were controlled, not reader/assembler implementations. Included OUT contribution without a target box score and a precise missing-metadata blocked error.
- 135 affected existing feature/harness/backtest tests passed under network/dotenv guards (1.37 seconds; one existing warning). This was a selected suite, not the whole repository.
- Existing controlled harness check passed pair/ledger and prediction/edge parity, empirical probability behavior, metadata retention, sanitized query failures and protected sample/combo/selection/edge-math AST parity.
- Final source Ruff, py_compile and scoped git diff --check passed. Last short native-path check passed at 09:44 EST.
- No new unit tests/fixtures or large scratch framework. One new source-boundary probe reused the existing controlled fixture producer; disposable paths can expire.
- No live SQL execution, real historical coverage/authenticity acceptance, model training/replay on real artifacts, baseline launch, deployment or source repair was performed. Do not present mocked DB execution as actual PostgreSQL SQL/schema certification.

Disposable checks (native project venv at `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe`):
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8-native-path-probe.py`
- Existing `nba-s8-harness-acceptance-probe.py` in the same scratch directory.

## Next session — code first, owner data lane separate

1. Read this draft plus the native source contract and inspect exact current worktree/main status. Finish CLI/artifact-policy attachment to the working native APIs, then lifecycle threading, directly and with minimal validation. Do not reopen broad data audits or repeat controlled certification loops.
2. Resolve the remaining exact policy windows/family choices only where needed for a frozen executable configuration. Owner supplies/fixes source metadata and coverage; missing records remain explicit data errors, never healthy defaults.
3. Bounded real-data/lifecycle acceptance (8D) and frozen owner-launched baseline (8E) remain open. Code delivery is not source/model acceptance or training authorization.
4. Source commit/merge/push needs separate owner authorization. Integrate onto current main with scoped diffs; preserve concurrent support-table sync and position repair.
5. Promote this pending handoff to remote canonical GBrain once Tailscale authentication is restored. Do not silently write into the stale local brain.

## Read first

All project paths below are relative to the Slice 8 worktree:
- `docs/operations/nba-s8-native-source-contract.md` — exact API, tables, sidecar keys and required source fields.
- `docs/operations/nba-slice8-completion-status.md` — newest checkpoint supersedes older historical sections.
- `docs/operations/nba-s8-feature-consumer-evidence.md` and `docs/understanding/nba-s8-feature-consumers.md`.
- `.hermes/plans/nba-slice8c-replay-seam.md` — historical implementation scope; native bridge is a later owner-directed extension.
- Earlier decision/family proposals remain in sibling worktree `nba-slice8-completion/docs/operations/`.

Reusable workflow saved in the default Hermes profile: `gameflow-minimal-source-integration`, with `references/nba-native-sources.md`. Preserve its direct-implementation, owner-data, lean-verification and progress-update rules.
