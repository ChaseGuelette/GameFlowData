---
title: NBA Batch D observation primitives — branch delivery
type: handoff
domain: handoffs
status: completed
owner: Chase
effective_date: 2026-11-01
tags: [handoff, nba, observation, quotes]
---

# NBA Batch D / coding slice 4 handoff

## Kick off immediately

Batch D pure primitives are implemented and bounded checks pass; historical interpretation and production consumers remain untouched.

- You: branch commit/push and merge into main approved; production activation and historical policy decisions remain separate gates.
- Next coordinator: this chat owned Batch D / coding slice 4 in `nba-training-evaluation-slices.md`, not historical repair S4 or artifact slice 2. Continue from canonical `handoffs/handoff-129` and the merge commit on main.
- Canonical destination: `gameflow-agent:/home/chase/GameFlowBrain/Handoffs/handoff-129.md`, source `gameflow`. Brain commit `8143209` pushed; scoped sync/embed, exact readback, search, hub backlink and timeline verified; orphan count zero.
- Read the [decision/evidence packet](nba-batch-d-observation-primitives.md) and [educational wiki](../understanding/nba-rebuild.md).

## Delivery identity

Worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-observation-primitives`.
Branch: `repair/nba-observation-primitives`.
Base: `d4784584b089bd94c6b5f3ce2d87d6a3fc632969`.
Commit subject: `feat(nba): add observation and quote-selection primitives`.
Delivery commit: `b2fb86f359eb3b812b00badc868f1f57970a2f7a`; remote branch SHA matched local HEAD. The later merge commit on main carries documentation reconciliation without changing the delivered Python modules.

## Implemented and decisions

- New `src/models/features/nba/observation_policy.py`: explicit aware caller clocks, identified interpreter, typed rejections and provenance, UTC cutoff comparisons and result-consistency validation.
- New `src/models/features/nba/quote_selection.py`: same game/player/market/period/variant/book/line/source/snapshot pairing; exact duplicate collapse; conflict-before-filtering; stable content IDs; explicit ranking/tie status; complete raw occurrence ledger.
- Approved pairing contract preserves literal opaque identifiers and compatible decision contexts, never mixes snapshots or silently resolves contradictions. An incomplete newer snapshot does not create an implicit older fallback selection.
- No source-clock/cohort/injury policy, SQL, consumer wiring, model/calibration changes, dependencies or new unit tests. Main/S1/CLI worktrees were not edited by this lane.

## Actual validation

Coordinator reran all checks before delivery on November 1, 2026:

- 77 worker controlled assertions passed.
- Six coordinator adversarial cases passed after repairs.
- Independent-review findings repaired and their controlled reproductions rerun with corrected expectations: provenance-order/scalar-order determinism, empty-ID isolation, preserved price-validation reasons and duplicate rejection evidence passed.
- Ten existing NBA feature-store inventory tests passed; no test edits.
- Scoped Ruff, compile, Pyright and new-file whitespace checks passed. Static import/source isolation checks passed.

Evidence runner: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-batch-d/final_verify.py`.
Raw commands, stdout/stderr and source hashes: sibling `verification-results.json`.
Scratch evidence is disposable and may be pruned. It proves controlled-input mechanics, not historical-data correctness. Before commit, the exact worktree allowlist was the two modules and three approved docs; this wrap-up adds only this handoff.

## Open gates and documentation caution

Source interpretation, real-data validation and training/direct-prefetch replay wiring remain S2/S3/S4/Batch H work. Branch publication does not merge or activate consumers.

The wiki/coding plan were copied from main's uncommitted planning documents at slice start. Other chats continued editing main documentation. Later integration must merge only Batch D additions into newer versions, not overwrite current documents with these snapshots. Related pre-existing links may not resolve on the isolated branch until documentation integration.

Reusable procedure saved in the default Hermes profile: `gameflow-implementation-worker/references/nba-observation-quote-primitives.md`.

## Canonical GBrain resolution

The initial Tailscale wrong-account/SSH authentication blocker was resolved. Canonical handoff 129 is synced and retrievable, with 100% embedding coverage and zero orphan pages. Doctor still flags unrelated stale code indexes and automation configuration; those were not repaired or treated as a failure of this handoff. The owner then approved the merge into main; historical policy and real-data acceptance remain open.
