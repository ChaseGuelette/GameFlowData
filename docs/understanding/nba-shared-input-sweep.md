# NBA shared-input sweeps — coding slice 5

## Current state

Chase approved implementation using the available slice-3 plumbing on November 1, 2026. Work is isolated in `repair/nba-shared-input-sweep`, at `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-shared-input-sweep`. Implementation/review is in progress, not accepted or merged.

The starting checkout is main `10d9c22` plus exact read-only copies of slice-3 `backtest_harness.py` and `bet_simulator.py`, and slice-2 `nba_artifacts.py`. These are inherited prerequisites, not completed source integration. Upstream source identities must be checked again before acceptance because slice 3's final repair/review is still active. S1 remains deferred and untouched.

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

See [slice 5 implementation plan](../operations/nba-shared-input-sweep-slice5-2026-11-01.md) and the [eight-slice map](nba-training-evaluation-slices.md). Finish implementation, independently review specification then quality, rerun actual checks and recheck upstream dependency hashes. Reconciliation/commit/merge and real-data execution remain separately authorized.
