# NBA replay correctness — coding slice 3

## Current state

Coding slice 3 is complete and controlled-path verified on `repair/nba-flat-replay`: **SPEC PASS / QUALITY APPROVED**, 34 existing scoped tests and all bounded API/CLI/combo/precision checks pass in the final coordinator rerun. Chase narrowly approved the three existing fixture repairs and immediate replay combo-call seam. Changes remain uncommitted and unintegrated; historical certification still requires stable sync and approved policy. Exact scope, source hashes, evidence and remaining integration gates belong in the [slice 3 decision record](../operations/nba-flat-replay-slice3-2026-11-01.md).

## Why this comes before a sweep

A sweep can repeat a mistake very efficiently. Before comparing settings, the simulator must place the direction the harness actually selected, size the stake the command actually requested, and refuse to turn missing model samples into believable-looking probabilities.

Three boundaries matter:

- Selection decides the side. Simulation must not quietly place the opposite side as well. A selected under that is disallowed does not authorize an over fallback.
- Sizing decides whether a real positive finite stake exists. Rejected stakes must not alter bankroll, create a bet or advance bet identity. A flat stake is a requested dollar amount, still subject to available bankroll; it is not a Kelly multiplier.
- Samples provide probability evidence. A missing, empty or nonfinite sample vector is unavailable model evidence, not 50% confidence, a Gaussian approximation or permission to bet the market prior. Preserve rejected candidate evidence with reasons.

Combo derivation is another validation boundary: components must align in length, and finite inputs do not guarantee a valid sum. Integer arithmetic can silently wrap into plausible-looking negative values; float arithmetic can overflow. Replay rejects these with component evidence rather than creating a phantom under bet or discarding the entire date. Shared model math is unchanged.

The existing valid-sample clipping and BL math are not being redesigned. Q10 behavior and the no-global-conformal-offset rule remain unchanged.

## What offline checks can prove

Controlled inputs can demonstrate chosen-direction preservation, flat sizing, rejection without state mutation, empirical-only calculation, safe BL sibling paths, and honest saved configuration. They cannot demonstrate real quote eligibility, historical source clocks, coverage or profitability.

Slice 3 does not wait for sync. Its historical acceptance does. It also does not absorb slices 1/2 or S1: those changes remain separately owned and integration-gated. In particular, this branch's baseline artifact/offset behavior is not promoted as safe canonical execution merely because the slice's simulator mechanics pass.

## Remaining coding sequence

1. Slices 1/2: finish their existing acceptance and integration gates; do not restart their implementation.
2. Slice 3: replay/sizing correctness — this worktree.
3. Slice 4: observation/quote primitives are merged; historical consumer integration remains gated.
4. Slice 5: shared-input sweeps against stable upstream contracts.
5. Slice 6: verify saved evidence and explicitly unavailable measurements.
6. Slice 7: orchestrate working commands with evidence-aware resume.
7. Slice 8: approved historical integration and real-data acceptance after stable sync and policy decisions.

The fuller eight-slice map remains owned in the coordinator checkout; do not copy its concurrent whole-file snapshot over later edits during integration.
