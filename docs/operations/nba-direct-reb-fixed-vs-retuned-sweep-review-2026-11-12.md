# Direct REB fixed versus retuned sweep review — November 12, 2026

## Kick off immediately

- **Chase:** both submitted sweep and analysis command pairs were correct and completed. Do not rerun them because of the garbled console line. The retuned/reselected model is a separate owner-running experiment; leave it undisturbed.
- **This chat:** retain the two direct artifacts as unsuccessful betting challengers in this discovery window; review the third result when available, then return to conditional-error improvement of existing minutes/rate models.
- **Other chat:** no new writer or replay needed for this review.

## Command and artifact verification

Verified exact sweeps `backtest_results/nba_direct_reb_sweep_nov2_v1` and `backtest_results/nba_direct_reb_pinball_sweep_nov2_v1`, plus each `_analysis` directory. Both manifests are complete with 20 planned/completed cells, 20 summary rows and 20 cell directories. The first uses the completed frozen direct benchmark; the second uses the completed pinball-retuned direct benchmark, with distinct model hashes. All analysis-manifest file hashes and source-sweep bindings pass. The saved-sweep verifier rechecked cell file hashes/rows, reconstructed wager outcomes/payoffs and reconciled betting/risk metrics with summaries. All ten control summary rows and all ten control wager ledgers are identical across the two runs.

Both use the same October21–November2 discovery window, REB-only selection, $2,000 bankroll, nominal $10 flat stakes, seed42, 5,000 samples, five original edge thresholds and raw/fixed-BL settings. No accidental all-stat run, wrong-model substitution or incomplete sweep explains the losses. Source/receipt consistency does not independently certify historical quote/data truth.

## Frozen 5% edge comparison

| Model | Raw bets | Raw profit | Raw ROI | Fixed-BL bets | Fixed-BL profit | Fixed-BL ROI |
|---|---:|---:|---:|---:|---:|---:|
| Original decomposition control |612|-461.46|-7.54%|140|36.27|2.59%|
| Frozen direct |543|-356.31|-6.56%|74|-92.95|-12.56%|
| Retuned direct |487|-459.69|-9.44%|39|-115.73|-29.68%|

The frozen direct model has one profitable cell out of ten: BL3%, +$34.76 on160 wagers (+2.17%). All five raw settings lose. Every retuned-direct cell loses. These cells are overlapping alternative policies, not independent experiments or additive profits. The control's best ROI at BL12% has only20 bets; it is not a promotion winner. Twelve scored dates make annualized Sharpe magnitudes unstable.

## Selection clues, not new policies

At raw5%, frozen-direct overs lose $318.48 on262 bets and unders lose $37.83 on281. Retuned-direct overs lose $358.39 on208 and unders lose $101.30 on279. Both directions are negative; this does not establish an under-only solution.

At fixed-BL5%, the frozen direct model retains52 wagers common with the decomposition control that lose $92.37, adds22 losing $0.58, and omits88 control-only wagers that earned $128.65. The retuned direct model retains28 common wagers losing $99.22, adds11 losing $16.51, and omits112 control-only wagers earning $135.50. Common wagers have identical stakes/payoffs. The observed BL deterioration comes largely from changed selection, particularly omission of profitable control wagers, not inconsistent settlement. These are descriptive discovery-window decompositions, not forecasts of which future bets to exclude.

## Decision

Neither direct model is a demonstrated betting upgrade. The retuned direct model improved overall pinball slightly but worsened probability Brier relative to the frozen direct model and performs poorly under these betting policies. The existing raw decomposition control is also weak; its small positive BL pocket is not production certification.

Let the already-running reselection candidate finish, evaluate it with the same frozen comparison, and do not expand the trial budget or optimize many new thresholds to rescue these results. Next existing-architecture work should investigate conditional probability errors at the selected rebound lines and the loss of useful under opportunities, then choose one learning/input intervention. Keep forecast quality, market-relative probability quality and selected-bet profitability separate.

Relevant prior lessons/invariants: cheap controlled experiments before architecture changes; empirical sample probabilities; no global offsets or blind Q10 correction; discovery is not untouched validation. This review used saved files only and launched no fit, sweep, SQL query, production change or promotion.
