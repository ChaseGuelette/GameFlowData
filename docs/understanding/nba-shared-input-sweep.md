# NBA shared-input sweeps — coding slice 5

## Current state

Chase approved implementation using the available slice-3 plumbing on November 1, 2026. Work is complete and controlled-path verified in `repair/nba-shared-input-sweep`, at `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-shared-input-sweep`: independent **SPEC PASS / QUALITY APPROVED**. It remains uncommitted and unmerged; this is not historical/model certification.

The starting checkout is main `10d9c22` plus exact read-only copies of slice-3 `backtest_harness.py` and `bet_simulator.py`, and slice-2 `nba_artifacts.py`. The harness snapshot was refreshed after a slice-3 precision fix; all three dependencies match upstream at final acceptance. These are inherited prerequisites, not slice-5 edits. Main separately advanced to `79a7443` with slice 3 merged while this work ran; this lane did not perform that integration. S1 remains deferred and untouched.

## Why shared inputs matter

Comparing two settings is meaningful only if they see the same model draws and quote candidates. Otherwise a changed result can reflect fresh Monte Carlo noise, newly acquired rows or different source inputs rather than the setting being compared.

Slice 5 separates acquisition from evaluation:

1. Validate one exact model bundle and fixed acquisition settings.
2. Acquire features, quotes, outcomes and voids once; generate each eligible date's prediction/sample bundle once.
3. Reuse that bundle across cells. Each cell gets fresh betting state and invokes the corrected existing edge/selection/simulation methods.
4. Save cell configuration, candidate evidence, selected predictions, bets and metrics with the same observed input identity.

Flat stakes and raw/no-BL controls are the first comparison contract. Optional explicit global BL cells do not constitute a recommendation to tune every model. There is no Kelly optimization or automatic winning-model promotion.

## What is deliberately not included

- Persistent prediction caches, cross-run resume or a lifecycle manager.
- New probability/BL math, a second simulator, calibration correction or global conformal offsets.
- Changes to source SQL, clock/cohort policies or historical S1.
- Reusing the legacy sweep fast path's missing-sample fallback or omitted candidate exports.
- Training, a large sweep, DB mutation, customer publication or a merge.

## Evidence limits

A common input bundle proves comparisons used the same evidence; it does not prove the evidence was historically available at decision time. The first implementation remains explicitly diagnostic/not-decision-grade until historical integration and real-data gates pass. Source-clock or completeness information that is unavailable must stay unknown.

Controlled offline probes can prove call reuse, selected-side/flat behavior, invalid-sample rejection, isolation across cells, identity checks and failure-safe outputs. They cannot prove historical profit, quote cleanliness or production-model quality.

## Exact scope and next gate

See [slice 5 implementation/evidence record](../operations/nba-shared-input-sweep-slice5-2026-11-01.md) and the [eight-slice map](nba-training-evaluation-slices.md). Final coordinator checks passed 78 existing tests, 551 contract-repair checks, seven parity/failure groups including 24 guarded CLI cases, and 27 output-location safety checks. The independent review gates are closed. Reconciliation/commit/merge, artifact-validator integration and real-data execution remain separately authorized; do not overwrite newer main with old dependency files.
