# NBA Slice 8 implementation and evidence

## Kick off immediately

Publication checkpoint: Chase explicitly authorized commit, merge and push. Slice 7 source `8e4c3b4` is merged into main as `83c121b`; partial Slice 8 source `1f16d2b` is merged as `041c013`. Earlier isolation/uncommitted wording below describes the implementation checkpoint, not current Git integration. The pending S1 and other-chat automation/reconciliation work remain excluded. No training, source-policy approval or real-data acceptance is inferred from publication.

This chat owns sequential historical integration in `repair/nba-historical-integration`, worktree `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-historical-integration`, based on `bf7a3e9`. Chase authorized the remaining code implementation, no unit tests and lean verification. Source and data acceptance remain distinct.

Other chat owns completed but uncommitted Slice 7 and active source reconciliation/automation. No predecessor source is copied or merged by this lane. Historical S1 remains separately owned; its pending patch touches the date-range loader and replay harness. Resolve overlap before changing those methods. No commits, push, merge, DB writes, archive access or long runs are authorized.

## Current result

Section 1 pure historical policy/context and observation adapters are implemented. Independent review found clock/readiness defects, then nested immutable-object, carryover ordering and exact-clock parsing defects. These were repaired; the parent also corrected an incompatible calendar-year season format to the actual literal NBA tokens (`22023` etc.) after checking the final Slice 7 validator. Parent's final short API check, scoped Ruff and compile pass. Independent review has not issued a final acceptance verdict on these final bytes; do not label them SPEC PASS / QUALITY APPROVED. No consumer is wired yet; this is not completed Slice 8B or completed Slice 8.

New files:

- `src/models/features/nba/historical_policy.py`
- `src/models/features/nba/historical_observation_adapters.py`

The market/bookmaker timestamp mixup, UTC elapsed/max-clock comparison, explicit slate fallback margin, typed authority definitions, required-family coverage and policy-derived context construction have been repaired. Final parent API output: `PASS: nested immutable object, keyed-family hash, literal NBA tokens, exact clock rejection; controlled values only`. The first two attempts to reproduce the final review findings failed because the parent used an incomplete cohort and then exposed the incompatible season validator; neither failed attempt is represented as a pass.

The code remains configurable only: controlled probe values are not approved project windows or timing policy. No policy YAML or source-authenticity assertion was created. Loader/replay/lifecycle implementation is paused at the owner decision and predecessor ownership gate below, not marked complete.

## Owner decision required before consumer wiring

The clarification form timed out; no answers were inferred. Chase must choose:

1. Initial operational decision rule: T-60, the proposed 18:30 ET/early-game rule, or retain comparison-only candidates.
2. Missing/unprovable sources: strict blocking versus a separately named diagnostic-only exception. No silent production defaults are permitted in decision-grade historical mode.
3. Use of exact fingerprinted read-only S1/Slice 7 snapshots in this isolated worktree versus waiting for their owner integration. No original predecessor worktree edits or implicit merge.

Actual cohort/split windows, per-family carryover and accepted source provenance also remain unresolved; code authorization does not approve them. No more generic scaffolding or validation frameworks should be added merely to hide these decisions.

## Existing-suite baseline

Parent ran the actual project venv against five existing feature suites from this isolated worktree with dotenv disabled, DB URLs removed, and engine/network creation guarded:

- `tests/test_nba_feature_store_inventory.py`
- `tests/test_nba_feature_contracts.py`
- `tests/test_nba_mode_loaders.py`
- `tests/test_nba_injury_context.py`
- `tests/test_nba_line_sources.py`

Result: 102 passed in 0.62 seconds, existing asyncio deprecation warning only. No test or fixture files changed. This is a compatibility baseline before consumer integration, not a test of new historical behavior or real data.

## Remaining sections and gates

1. Repair/review the pure policy/source adapters.
2. Define exact typed semantic policy fields and integrate shared historical features without changing the legacy production path.
3. Resolve S1 overlap before date-range/harness fail-closed changes.
4. Integrate one paired-quote path for direct and prefetched replay, preserving numerical owners.
5. Reconcile exact completed Slice 7 source into an authorized dependency base before lifecycle changes.
6. Add a bounded verifier using real owners; unresolved data, policies, windows and artifacts remain blocked, never success-shaped placeholders.
7. Owner approves policy values and bounded real-data execution; Chase launches any long baseline.

No new policy YAML, chosen decision time, retrospective cohort/split or real-data approval has been inferred from the general implementation authorization.

## Reading

- Main-checkout wiki: `docs/understanding/nba-historical-integration.md`
- Main-checkout policy packet: `docs/operations/nba-s8-source-policy-matrix-2026-11-02.md`
- Worktree section scope: `docs/operations/nba-s8-implementation-scope-2026-11-02.md`
