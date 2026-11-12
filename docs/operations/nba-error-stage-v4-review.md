# NBA error-stage v4 review

## Verdict

The owner-run v4 diagnostic completed. Prioritize a bounded rate-model improvement experiment, with minutes held fixed, while testing whether the resulting probabilities remain useful after bet selection. This is a development recommendation, not proof that rates contain recoverable alpha or approval to tune, train or promote.

## Evidence and verification

### Retuning readiness follow-up

**Feature-parity implementation update:** The owner requested an opt-in experiment. `--tune --tuning-match-final-features` is now implemented and integrated, uncommitted; it closes the tuning/final-fit column mismatch when enabled. Without the flag, existing union-feature tuning remains. Main verification passed 48 existing offline tests, ordered-column controlled probes and the exact rate-only CLI dry-run. See [the delivery record](../../.hermes/plans/nba-tuning-feature-parity.md). Copula behavior is unchanged; the earlier mismatch discussion below documents the reason for the flag, not an unresolved implementation blocker. No real training was launched.

The pinball objective has since been delivered in the main working tree. Both tuner/pipeline hashes match the implementation review receipt; that receipt records 48 existing offline tests and controlled mechanism probes passing, not real-fit acceptance. The old PROPOSED label in the implementation plan is superseded for objective implementation by its delivery section. Training remains owner-launched.

Recommended experiment budget: 100 shared-hyperparameter trials per PTS/REB/AST rate model (300 Optuna trials total), no minutes retraining, no feature reselection. This is a proposed practical budget, not an empirically optimal trial count. Shared trials evaluate five quantiles; 300 trials can therefore involve up to 1,500 quantile fits before pruning, plus final fitting. Separate per-quantile mode would instead create 1,500 trials, so trial counts alone do not establish a fivefold fitting-cost difference.

Launch is not yet certified for the intended feature-faithful comparison:

- Partial training preserves minutes unless `--retrain-minutes` is passed and selected lists unless `--reselect-features` is passed (`train_pipeline.py:339-378`).
- Tuning still constructs each rate's union feature matrix (`train_pipeline.py:597-609`). The parent artifact's PTS/REB/AST unions contain 56/55/56 features, while its individual quantiles retain different subsets. Assists Q10 uses only 12 features, versus the 56-feature tuning union. Resolve this known mismatch so trial quantiles are evaluated with their corresponding final-fit lists before spending the full tuning budget; do not silently force all final quantiles onto the union.
- Partial training recomputes copula parameters from training data (`train_pipeline.py:383-387`, `1264-1284`). This does not prove they will change, but exact equality with the parent must be verified or explicit reuse implemented for a strict rates-only comparison.
- Dates are passed into temporal splitting; no claim of a complete nested/feature-selection-leakage audit is made. Existing selected lists are frozen for this experiment, not reselected inside every tuning fold.

No readiness repair, tuning, or source change was executed by this review. Coordinate the narrowly scoped feature-list fix and copula contract with the implementation owner before handing off a launch command.

- Output: `backtest_results/nba_game_market_free_error_stages_nov2_v4`.
- Model: `src/models/artifacts/nba_feature_ablations/game_market_free/nba_run_20261108_150945`.
- Reference: `backtest_results/nba_game_market_free_holdout_sweep_10_nov2_v1`.
- Window: October 21–November 2, 2026; already-used discovery data.
- Completion receipt: November 9, 2026 04:05:43 UTC. Manifest records `state=complete`, `decision_grade=false`, historical readiness unverified, source artifact unchanged, local read-only transactions and 15-second timeout.
- Summary records baseline and canonical RNG reproduction. All 1,579 eligible player-games are oracle-valid; no missing actual minutes, invalid stat actuals or nonpositive actual minutes.
- Independently verified all five output file hashes against the manifest. Recomputed all arm/stat Brier scores from `quote_probabilities.csv`; they match the summary within 1e-12.
- Output row counts: components 6,316; totals 18,948; quote probabilities 11,728. Quoted baseline opportunities: 2,932.
- This verifies saved-output consistency, not source provenance, whole historical readiness, independent statistical significance or production readiness.

## 1. Components beat simple lagged references

| Component | Model median MAE | L5 reference MAE | Relative MAE reduction |
|---|---:|---:|---:|
| Minutes | 4.2762 | 4.5503 | 6.0% |
| Points rate | 0.16821 | 0.19472 | 13.6% |
| Rebounds rate | 0.07841 | 0.08510 | 7.9% |
| Assists rate | 0.05573 | 0.06283 | 11.3% |

Rate errors are in stat units per minute; do not compare their magnitudes directly with minutes errors or across targets as if the scales were equal. The learned heads are not failing to beat their cheap point references. L5 is a point reference, not a competing distribution with a comparable five-quantile pinball score.

Maximum absolute component coverage gaps across the five quantiles are approximately 1.77 percentage points for minutes, 0.90 for points rate, 2.29 for rebounds rate and 1.65 for assists rate. Aggregate coverage is already reasonably close to nominal; this does not establish conditional accuracy or market-beating probabilities.

## 2. Rates dominate hindsight sensitivity, not proven reducible error

Relative reduction in final total-stat mean pinball, on the same valid cohort:

| Stat | Replace minutes with actual | Replace rate with actual |
|---|---:|---:|
| Points | 14.7% | 60.0% |
| Rebounds | 7.4% | 57.6% |
| Assists | 5.1% | 68.2% |

MAE and Brier comparisons agree with the rate-first triage. Baseline total MAE is 4.709 points, 1.941 rebounds and 1.390 assists. Both-actual arms reach essentially zero total loss and zero Brier, consistent with the sanity check.

The actual-rate intervention uses realized stat / realized minutes. It therefore reveals realized outcome noise as well as predictable rate variation. The large gain is not a forecast of attainable improvement and does not prove that the learned rate model is defective. Minutes and rates interact; these percentages are not additive shares of causal error.

## 3. Raw probabilities trail the market; selection amplifies the gap

Brier is squared probability error; lower is better. Each model/market pair below uses the same cohort.

| Lane / stage | N | Model Brier | Market Brier |
|---|---:|---:|---:|
| Raw, all quoted | 2,932 | 0.25396 | 0.24624 |
| Raw, best stat/side selected | 1,119 | 0.25855 | 0.24318 |
| Raw, after policy/cash | 944 | 0.26132 | 0.24285 |
| BL, all quoted | 2,932 | 0.24676 | 0.24624 |
| BL, best stat/side selected | 1,119 | 0.24544 | 0.24472 |
| BL, after policy/cash | 262 | 0.24660 | 0.24484 |

Raw model-minus-market loss widens from 0.00771 to 0.01537 to 0.01847. Each individual stat also has a larger raw model-minus-market gap in the final wager cohort than in all quoted opportunities. This is descriptive evidence consistent with selection amplification, not a randomized causal effect; cohorts change through selection.

BL brings probability losses much closer to the market on the shared all-quoted cohort. This supports market anchoring as a useful control here, not standalone model alpha. Raw and BL final wagers differ, so their final losses are not a same-bet treatment comparison.

BL final-wager detail: rebounds 130 rows, model 0.24190 vs market 0.24597; points 82 rows, 0.25419 vs 0.25174; assists 50 rows, 0.24639 vs 0.23059. Rebounds are the favorable descriptive pocket; assists are a caution. Neither justifies a new stat ban or promotion from this short discovery sample. No uncertainty intervals were computed. Brier is not ROI.

## Recommended next experiment; not yet authorized

1. Use the separately proposed NBA pinball-objective work as a bounded candidate-development intervention, coordinating with its owner. Its inspected plan remains PROPOSED: `.hermes/plans/2026-11-08_184843-nba-optuna-pinball-objective.md`.
2. For the first comparison, hold the existing minutes model fixed and tune rate heads using validation pinball within the training/development chronology. Average across quantiles within each target; do not pool unnormalized minutes and rate losses into one objective.
3. Preserve feature families, data policy, sampler/copula settings and fixed raw/BL evaluation settings. Do not simultaneously change selection thresholds or run another broad family search.
4. Require improvements in rate scores to be checked against combined probabilities and market-relative loss on common opportunities, plus fixed-policy flat-stake results. An improvement only in coverage, or only in component pinball, is insufficient to establish better betting signal.
5. Use the current window for development comparison, not objective optimization or independent confirmation. Freeze a promising challenger before later untouched validation.
6. Untuned control versus pinball-tuned challenger tests the whole tuning intervention. Proving the objective itself superior requires matched calibration-gap and pinball searches with equivalent budget, search space and folds; that additional experiment is not automatically authorized.

## Relevant prior lessons and ownership

Apply `lessons/cheap-baseline-before-architecture`, `lessons/q10-miscalibration-is-edge`, and `operations/critical-invariants`: preserve empirical probabilities and Q10 behavior, never add global conformal offsets, and require downstream evidence before promotion. Pinball tuning does not guarantee preservation of historical edge.

The other chat owns diagnostic repairs. This review changed no diagnostic/trainer/tuner source, queried no application DB, and launched no training or replay. Only the review and its restart-plan link are documentation changes. No new owner decision, model promotion or production authorization is implied.
