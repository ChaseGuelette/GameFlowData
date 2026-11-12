# NBA rate pinball run 20261109_220136 — analysis

## Verdict

Repair follow-up: Chase approved the fix; [parameter/early-stopping parity is now implemented and offline-verified in main](../../.hermes/plans/nba-tuning-parameter-parity.md), uncommitted. This does not change the artifact analyzed below. No replacement training or backtest has been launched.

The owner-run partial retrain completed and produced a manifest-verified artifact. Feature matching, frozen minutes and unchanged copula controls held. However, two pre-existing tuning-to-final-fit parameter mismatches were discovered: regularization parameters are discarded when converting the winning trial to the final config, and tuning removes early stopping while final training enables it. The artifact is a real exploratory candidate, but not a faithful refit of the Optuna winners; do not attribute its downstream behavior to the intended matched pinball experiment without this qualification.

No repair, retraining, backtest, DB query or promotion was performed in this analysis. The next recommended action is a narrowly scoped parameter-round-trip/early-stopping repair, followed by deciding what can be recovered from retained trial logs before scheduling another owner-run search. Preserve this artifact.

## Exact artifacts

- Parent: `src/models/artifacts/nba_feature_ablations/game_market_free/nba_run_20261108_150945`.
- Candidate: `src/models/artifacts/nba_rate_pinball_feature_match/nba_run_20261109_220136`.
- Candidate final directory exists; matching `_incomplete` directory is absent. Manifest completion and sanity pass are recorded.
- Verified all manifest-listed file hashes.

## What held constant

- `selected_features.json` and `copula_params.json` are byte-identical to the parent.
- Copula values remain PTS 0.3356, REB -0.0029, AST 0.2128.
- Minutes joblib bytes differ after reserialization, but all five underlying booster raw hashes, config, ordered features and calibration offsets match the parent. Its holdout calibration rows are exactly unchanged. Do not infer retraining from a changed container hash.
- Training and calibration population chronology/key-hash receipts exactly match the parent: 46,249 acquired training rows ending April 13, 2025, and 23,898 calibration rows spanning October 21, 2025–June 13, 2026. The supplied log reports 42,678 eligible rows for rate fits. Key parity does not certify feature-value equality or full historical source readiness.
- Candidate metadata records only PTS/REB/AST retraining, no minutes retraining, no reselection, pinball objective and feature-matching requested/effective true.
- Loaded saved rate suites retain the parent selected lists and matching estimator feature counts. XGBoost booster feature-name metadata is absent, so direct comparison of booster names to selected names is not meaningful; this is not evidence of a feature mismatch.

## Selected scores and holdout calibration

Saved inner-validation winning mean pinball:

| Rate | Best objective |
|---|---:|
| PTS | 0.0568076500 |
| REB | 0.0257911735 |
| AST | 0.0186899222 |

These are scores from the tuning fits, not verified scores of the saved final estimators. The parent has no matched inner-validation pinball baseline in these reports. Do not compare them with October/November v4 diagnostic losses, which use a different population/window.

Worst absolute component holdout coverage gaps, percentage points:

| Component | Parent | Candidate |
|---|---:|---:|
| Minutes | 2.816 | 2.816 |
| PTS rate | 1.589 | 1.541 |
| REB rate | 1.577 | 1.522 |
| AST rate | 1.331 | 1.779 |

Component calibration remains close to the parent. This neither proves better forecasts nor refutes pinball tuning: coverage is not the chosen selection metric.

Combined calibration worst gaps, percentage points:

| Stat | Parent | Candidate |
|---|---:|---:|
| PTS | 5.00 | 5.10 |
| REB | 5.00 | 5.05 |
| AST | 7.80 | 7.90 |

The combined warning is largely the same pattern as the control, not a new large degradation. It is based on the sampled calibration procedure, not a betting replay. `_evaluate_combined_calibration` passes the saved/computed copula into its predictor; the generic independent-sampling warning does not establish that this run ignored dependence. Generated global offsets are diagnostic-only in the manifest and must not be deployed or loaded into canonical comparison replay.

## Confirmed parameter-contract defects

### Regularization parameters lost

- `_sample_params` searches `reg_alpha` and `reg_lambda`; trial estimators receive them.
- `_params_to_config` at `src/models/hyperparameter_tuner.py:485-496` omits both.
- `QuantileModelConfig` at `src/models/quantile_trainer.py:24-58` has no fields for them and filters unknown keys.
- Final estimator construction at `src/models/quantile_trainer.py:166-178` does not forward them.
- Candidate `best_hyperparams.json` omits them; all saved rate estimators report those constructor options as unset (`None`), not the selected values.
- Concrete retained AST winner from the owner's log: `reg_alpha=4.379502032151772e-07`, `reg_lambda=0.002412593629533685`. Those selected values did not survive into final fitting.

### Early stopping differs between search and final fit

- Both trial-objective functions explicitly remove `early_stopping_rounds` before estimator construction (`hyperparameter_tuner.py:249`, `302`). That sampled setting cannot affect the trial score.
- Final training enables the selected patience from the config. The AST winner was scored with `n_estimators=756` and no early-stopping option; final AST Q10 recorded 515 boosted rounds with best iteration 475. Other final heads also stop/select earlier iterations.
- Optuna trial pruning is distinct from XGBoost early stopping; a pruned-trial log does not repair this mismatch.

Both defects are present in committed HEAD before the pinball and feature-matching patches; verified with `git show`. They were missed by the earlier parity review, which checked ordered feature columns but not the full effective estimator parameter contract.

## Recommendation / authorization boundary

Fix the mapping and stopping contract before treating another run as a matched pinball challenger. Verify actual estimator kwargs for trial and final fit, saved JSON/config round trips, and learned-tree selection policy with bounded controlled probes. Preserve legacy artifact compatibility and coordinate the implementation scope; this analysis does not authorize source changes. Recover full trial settings/logs where available, but do not promise a no-search refit suffices: making early stopping consistent can change trial ranking.

No ROI, market-relative probability improvement or promotion verdict can be established from the current training reports. Relevant prior lessons: `lessons/q10-miscalibration-is-edge`, `lessons/cheap-baseline-before-architecture`, and `operations/critical-invariants`. Keep global offsets disabled and use empirical sample probabilities in downstream comparisons.
