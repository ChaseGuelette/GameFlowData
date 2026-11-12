# Repaired NBA rate pinball run 20261110_092318

## Verdict

The repaired owner-run candidate completed and passes the artifact checks for the previously identified parameter-handoff defects. It is ready for a matched development replay against the original game-market-free control, not for promotion. Saved training reports do not yet establish improved forecast quality versus that control or improved betting results. No further retrain is indicated by these artifact checks alone.

## Identity and verification

- Candidate: `src/models/artifacts/nba_rate_pinball_feature_match/nba_run_20261110_092318`.
- Candidate manifest identity: `369330835ae493a78bc7bbe4d17548d2b63754cbe4c78ce61b43856cde278860`.
- Primary comparator: `src/models/artifacts/nba_feature_ablations/game_market_free/nba_run_20261108_150945`.
- Earlier pre-repair candidate `nba_run_20261109_220136` remains exploratory evidence, not the clean comparator for attributing tuning benefit.
- Final directory exists, `_incomplete` sibling is absent, completion and single-game sanity checks passed. All 13 manifest-listed candidate files match their hashes; control and previous-candidate manifests were also verified.
- Metadata records pinball, feature matching enabled, shared tuning and 100 trials configured per rate target.
- Loaded each saved rate suite read-only: all 15 rate estimators match their saved hyperparameter configurations for estimator budget, depth, learning rate, subsampling, column sampling, child weight, early-stopping patience, regularization and seed. Stored configs also match JSON including validation fraction. Saved feature lists/counts match the selected-feature evidence.
- Final early stopping is present and used: PTS Q10 has budget 539, best iteration 186, 260 boosted rounds; AST Q10 has budget 1,117, best iteration 869, 954 boosted rounds. Different final quantiles legitimately stop at different iterations.
- Rate component calibration offsets are all zero. Global combined offsets remain separate diagnostic files, not promotion instructions.

## Frozen comparison controls

Selected features and copula JSON are byte-identical to the original control. Copula remains PTS 0.3356, REB -0.0029, AST 0.2128.

Minutes container bytes differ after reserialization, but all five learned booster raw hashes, config, ordered features and calibration offsets match the control. Its coverage report is unchanged.

Training/calibration chronology and key-hash receipts match the control: fitting observations end April 13, 2025; calibration ends June 13, 2026. Recorded populations contain 46,249 acquired training rows and 23,898 calibration rows. Receipt equality is not certification that every historical source value or timestamp is correct. Historical provenance remains `source_verified=false`, `decision_grade=false`.

## Saved tuning results

| Rate | Winning inner-validation pinball | Selected reg_alpha | Selected reg_lambda | Patience |
|---|---:|---:|---:|---:|
| PTS | 0.0567874231 | 0.0000000551164 | 2.26739351 | 73 |
| REB | 0.0257975003 | 5.93353458 | 0.00185159 | 38 |
| AST | 0.0186861368 | 0.04049054 | 0.0000190079 | 84 |

These regularizers now exist in both JSON and the actual saved estimator constructor parameters. Unlike the previous run, they were not discarded. Different target losses have different units; do not use their absolute sizes to rank which stat is best. No matched inner-validation control loss is available in the saved reports. The earlier faulty-search scores also came from a different stopping procedure and are not an independent gain estimate. Do not compare these scores to the October/November diagnostic as if it were the same cohort.

## Calibration versus original control

Worst absolute component coverage gap, percentage points:

| Component | Control | Repaired candidate |
|---|---:|---:|
| Minutes | 2.82 | 2.82 |
| PTS rate | 1.59 | 1.76 |
| REB rate | 1.58 | 1.64 |
| AST rate | 1.33 | 1.98 |

Calibration remains close to nominal, with slightly larger worst gaps for all three rates. This is neither a pinball-success metric nor a reason to reject the candidate; the experiment optimized pinball rather than coverage gaps.

Worst combined minutes-times-rate coverage gap, percentage points:

| Stat | Control | Repaired candidate |
|---|---:|---:|
| PTS | 5.00 | 4.95 |
| REB | 5.00 | 4.95 |
| AST | 7.80 | 8.10 |

The combined pattern is broadly unchanged. AST Q10 coverage is 18.1% versus nominal 10%, compared with 17.8% for the control. The report is a sampled calibration check, not the full matched decision-time replay. No confidence intervals or significance claims were produced. Do not infer a breakthrough or a substantial deterioration from these small changes.

## Next action

Use this exact repaired candidate for a matched replay against the original control, preserving quote window, raw/fixed-BL settings, flat stake policy and sample settings. Compare common-opportunity probability scores/market-relative loss, selection behavior, and flat-stake profit/ROI/drawdown/volume. The previously used October 21–November 2 window remains discovery data; later untouched confirmation is a separate gate. Do not start another tuning/feature wave or apply global offsets based solely on these calibration reports.

Relevant prior lessons: `lessons/q10-miscalibration-is-edge`, `lessons/empirical-cdf-for-probabilities`, and `operations/critical-invariants`. No offsets, promotion, DB queries, fitting, prediction replay or backtest were executed during this read-only artifact analysis. Only this report and its restart-plan link were written.
