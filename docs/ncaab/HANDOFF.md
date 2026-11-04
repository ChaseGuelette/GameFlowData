---
title: "NCAAB parallel design lane — NBA remains first"
type: handoff
domain: handoffs
status: completed
owner: Chase
effective_date: 2026-11-03
tags: [handoff, ncaab, design, parallel-worktree, nba-priority]
---

# NCAAB parallel design lane — NBA remains first

**Date**: 2026-11-03

Canonical summary published as `handoffs/handoff-139` in GameFlowBrain commit `a02ddb89f0795515cac390798e9cf41df7075044` on `master`; the pushed ref was verified. Exact handoff readback, hub backlink, keyword retrieval and the NBA-first roadmap text passed. Graph/timeline backfill created the new entry; all 1,030 chunks were embedded and orphan pages were zero at verification.

Broader GBrain doctor warnings remain for stale code indexes, resolver/cycle maintenance and subagent configuration. These were not repaired during this bounded markdown wrap-up. The full application design files remain local/uncommitted; publishing the canonical summary did not commit, push or deploy GameFlowData.

## Priority and ownership

**NBA remains the primary project priority.** NCAAB is a new, simultaneous design/worktree lane, not a replacement for the active NBA readiness, evidence and production work. Do not switch the default next session into NCAAB implementation, restart NBA work from this document, or take resources/ownership away from the NBA lane.

Physical Git state at wrap-up start: `C:\Users\Chase\Projects\GameFlowData`, branch `main`, checkpoint `28755c3d05646cd03105265fa5ff9ed55e6ab004`. The NCAAB design changes are uncommitted in that checkout. No dedicated NCAAB worktree is registered in its `git worktree list`; this session did not create or move a worktree. Treat NCAAB as the newly designated parallel lane and establish explicit physical isolation before implementation. Preserve all existing NBA worktrees and concurrent edits.

## Delivered

The repository now contains a complete design set under `docs/ncaab/`:

- `README.md`: summary, navigation, NBA-first priority and actual worktree state.
- `model-architecture-decision.md`: all discussed model proposals and the initial controlled experiment.
- `data-sources.md`: historical/live statistical sources and separate odds acquisition; KenPom excluded.
- `architecture-overview.md`: shared contracts, dependency boundaries, configuration defaults and rollout gates.
- Seven fully populated `layers/01-...` through `layers/07-...` documents covering ingestion, storage/linking, features, training/artifacts, prediction/decisions, evaluation/paper and orchestration/readiness.

The design names proposed files/classes/interfaces, 30 proposed table contracts, migration boundaries, implementation sequences and acceptance tests. These are specifications, not implemented modules or applied tables. The root documentation index links the set.

Initial direction: Python access to SportsDataverse ESPN histories and live ESPN feeds, a verified season-classification reference, and the existing The Odds API; paired independent/aware quantile-tree baselines; later Bayesian-state and roster/matchup neural challengers. The acquisition target is ending seasons 2022-2026 plus current 2027, not a claim of completed coverage. KenPom and Kaggle are outside the initial build.

## Verification and limitations

Documentation checks passed: all seven layer files populated; local links and shared-contract anchors resolved; table references matched the catalog; Python interface examples parsed; feature dimensions and illustrative payout math checked; scoped whitespace checks passed. These are design-consistency checks, not runtime tests, trained-model results, full-history certification or deployed acceptance.

Earlier research exercised small public historical files and local read-only live HTTP responses. It did not establish production-host availability or complete historical pregame injury/roster knowledge. The design explicitly separates strict observations from reconstructed historical inputs and uses one shared replay/paper prediction and settlement path.

No NCAAB runtime code, SQL migration, database write, paid odds backfill, training, scheduler enablement, deployment or real-money action was performed. No GameFlowData commit or push was requested/performed by this lane. NBA commits continued concurrently; the checkpoint above is an observed starting snapshot, not an assertion that main stopped advancing.

## Preserve concurrent NBA work

The current NBA owner/handoffs and application readiness documents remain the authority for NBA next actions. In particular, preserve completed Slice 8 work and do not reopen accepted coding merely because this NCAAB handoff is newer. Follow the newest actual NBA evidence if it has advanced beyond the canonical prior handoff.

At wrap-up start, `docs/operations/nba-props-linkage-recovery-2026-11-03.md` was an unrelated dirty NBA document. It was not staged, reverted or edited by this wrap-up.

## Next-session routing

1. Resume/prioritize the active NBA work and its current acceptance/production needs.
2. Keep NCAAB as a separate, simultaneous design lane. Review the saved docs without starting ingestion/training or modifying NBA code.
3. Before NCAAB implementation, preserve/commit the design only when requested and create/identify its isolated worktree with explicit ownership.
4. Supply remaining execution inputs: source/classification evidence, book allowlists and settlement rules, resource/API budgets, schema preflight, durable storage, deployment-host canary and paper-release approval.

NCAAB entry point: `GameFlowData/docs/ncaab/architecture-overview.md`. The layer documents define the future build order; they do not authorize that build or supersede NBA priority.
