# NBA low-minute exclusion investigation

Date: 2026-11-12

## Conclusion

The five-minute acquisition filter does hide a small number of quoted, settled outcomes, but the current evidence does not justify removing it. The ten-minute rate and combined-calibration floor is supported by the scored holdout and should remain unchanged.

The smallest justified correction is measurement and policy consolidation, not a threshold change:

1. Keep the current five-minute acquisition threshold while collecting a larger scored cohort.
2. Keep the ten-minute rate-model, rate-calibration, and combined-calibration threshold.
3. Continue including 5–<10-minute rows in the minutes model; removing them would make the minutes model less representative of settled bets and is not supported by these results.
4. Replace the duplicated hard-coded ten-minute checks with one shared rate-eligibility definition so configuration, fitting, tuning, calibration, and diagnostics cannot drift apart.
5. Add explicit `<5`, `5–<10`, and `>=10` cohort counts and errors to future calibration/evaluation reports.

A future minutes-only challenger may test admitting positive `<5` outcomes, but only after a larger scored holdout establishes that these outcomes occur in the actual model-output/bet population and that including them improves downside-tail or probability accuracy. Rate eligibility should remain fixed during that comparison.

## Scope

Primary scored cohort:

- Model artifact: `8e1c8b65b0e0dfc0c87f83d35341fb45ba4d185c428120c877b2366518ee151d`
- Artifact: `src/models/artifacts/production`
- Evaluation cell: `cell_34ca124425d0feb618405f0969a7ca0c607083eb58716a9e94d1a5e107033efe`
- Window: 2026-10-21 through 2026-11-02
- Saved quote opportunities: 2,932
- Unique artifact-output player-games: 1,119
- Selected predictions: 1,119
- Funded bets: 300
- Database authority: local training database, queried read-only with a 15-second statement timeout

Broader prevalence cohorts use every retained player-specific T-60 PTS/REB/AST feature-line player-game in the artifact's training, calibration, and evaluation windows. This denominator is intentionally broader than the model-output cohort.

## What the filters actually do

These thresholds use the realized postgame minutes outcome. They are training/calibration filters, not live pregame betting rules.

| Stage | Effective outcome eligibility | Evidence |
|---|---:|---|
| Training-data acquisition | `actual_minutes >= 5` | `src/models/features/nba/training_loader.py:278-296`, `:557-566` |
| Minutes fitting, tuning, and feature selection | `actual_minutes > 0`, effectively `>=5` after acquisition | `src/models/quantile_trainer.py:425-435`; `src/models/train_pipeline.py:759-766`, `:839-847`, `:875-884` |
| Minutes holdout calibration | `actual_minutes > 0`, effectively `>=5` | `src/models/train_pipeline.py:938-947` |
| Rate-target creation | `actual_minutes >= FeatureConfig.min_minutes_for_rate`, currently 10 | `src/models/features/nba/training_loader.py:251-254`; `src/models/feature_store.py:52-58` |
| Rate fitting, tuning, and feature selection | `actual_minutes >= 10` | `src/models/quantile_trainer.py:492-508`; `src/models/train_pipeline.py:610-623`, `:791-802`, `:849-860`, `:886-897` |
| Rate holdout calibration | `actual_minutes >= 10` | `src/models/train_pipeline.py:949-960` |
| Combined minutes x rate calibration | `actual_minutes >= 10` | `src/models/train_pipeline.py:1093-1101` |
| Minutes/rate correlation diagnostic | `actual_minutes >= 10` | `src/models/train_pipeline.py:1236-1247` |

Therefore:

- `<5` rows are absent from all model fitting and calibration.
- `5–<10` rows train and calibrate the minutes model but not the rate models or combined distribution.
- `>=10` rows are used throughout.

The separate rolling-stat minimum-minutes invariant of 8 controls which prior appearances contribute to pregame rolling features. It is not the current-row outcome filter investigated here.

## How many outcomes are hidden?

### Broad quoted-player prevalence

These counts use unique player-games with at least one exact-chronology player-specific T-60 PTS/REB/AST feature line and a settled `player_game_stats` outcome.

| Artifact population | Settled quoted player-games | `<5` acquisition-excluded | `5–<10` rate/calibration-excluded | `>=10` retained |
|---|---:|---:|---:|---:|
| Training: 2023-10-24 to 2025-04-13 | 31,404 | 77 (0.245%) | 351 (1.118%) | 30,976 (98.637%) |
| Calibration: 2025-10-21 to 2026-06-13 | 17,972 | 119 (0.662%) | 239 (1.330%) | 17,614 (98.008%) |
| Evaluation source: 2026-10-21 to 2026-11-02 | 1,352 | 11 (0.814%) | 9 (0.666%) | 1,332 (98.521%) |

The five-minute acquisition threshold therefore does hide real quoted outcomes. Its share rose from 0.25% in the training seasons to 0.66% in calibration and 0.81% in the short evaluation source cohort. That trend is worth monitoring, but the counts remain small and do not establish that the outcomes are predictable from pregame information.

### Exact model-output cohort

The artifact-output cohort is narrower than the feature-line source cohort because additional feature/backtest eligibility steps occur before predictions are saved.

| Population | `<5` | `5–<10` | `>=10` |
|---|---:|---:|---:|
| Unique quoted model-output player-games | 0 | 8 (0.715%) | 1,111 (99.285%) |
| Stat quote opportunities | 0 | 20 (0.682%) | 2,912 (99.318%) |
| Selected predictions | 0 | 8 (0.715%) | 1,111 (99.285%) |
| Funded bets | 0 | 2 (0.667%) | 298 (99.333%) |

The broader evaluation source had 1,360 quoted player-games, while the artifact output had 1,119. The 241-player-game difference is a denominator/eligibility difference, not missing settlement in the scored output. Because no `<5` result reached the exact model-output cohort, this holdout cannot measure model error for the acquisition-excluded bucket.

## Error comparison

Errors below use all 2,932 settled quote opportunities in the exact model-output cohort. Actual PTS/REB/AST values were reconstructed from exact player/game `player_game_stats` rows and reconciled against all 1,119 saved selected-prediction actuals with zero discrepancies.

### Aggregate errors

| Metric | `5–<10` minutes, n=20 | `>=10` minutes, n=2,912 | Low-minus-retained interpretation |
|---|---:|---:|---|
| Median absolute error | 3.602 | 2.115 | Worse in low-minute rows |
| MAE | 4.916 | 3.112 | +1.804 |
| RMSE | 6.720 | 4.453 | Worse in low-minute rows |
| Bias: actual minus predicted median | -4.833 | +0.467 | Strong low-minute overprediction |
| Mean five-quantile pinball | 1.808 | 1.081 | Worse distribution loss |
| Brier, pushes excluded | 0.234 | 0.254 | No demonstrated disadvantage; very small sample |

The whole-date bootstrap MAE difference was +1.804 with a 95% interval of [+0.017, +4.173]. The Brier difference was -0.0199 with a 95% interval of [-0.0827, +0.0732]. Both use only 12 dates; the low-minute group contains 20 quote rows, so the intervals are underpowered.

### Per-stat errors

| Stat | Cohort | N | MAE | RMSE | Bias: actual minus median |
|---|---|---:|---:|---:|---:|
| PTS | `5–<10` | 8 | 7.695 | 9.092 | -7.695 |
| PTS | `>=10` | 1,107 | 5.076 | 6.475 | +0.624 |
| REB | `5–<10` | 8 | 4.111 | 5.421 | -3.943 |
| REB | `>=10` | 1,025 | 2.084 | 2.710 | +0.376 |
| AST | `5–<10` | 4 | 0.965 | 1.289 | -0.891 |
| AST | `>=10` | 780 | 1.675 | 2.211 | +0.361 |

PTS and REB account for most of the low-minute error. Every low-minute stat shows negative bias, meaning the model's median forecast was above the realized result.

### Quantile coverage

| Quantile | Target | `5–<10` empirical coverage | `>=10` empirical coverage |
|---|---:|---:|---:|
| Q10 | 10% | 50.0% | 9.1% |
| Q25 | 25% | 60.0% | 22.0% |
| Q50 | 50% | 90.0% | 48.2% |
| Q75 | 75% | 100.0% | 75.8% |
| Q90 | 90% | 100.0% | 91.7% |

The low-minute coverage pattern is consistent with forecasts being much too high for those outcomes. The retained population is substantially closer to nominal. The low-minute sample is too small to justify calibration changes, especially global offsets.

## Betting impact

| Cohort | Bets | Record | Hit rate | Flat-stake profit | ROI |
|---|---:|---:|---:|---:|---:|
| `5–<10` | 2 | 1-1 | 50.0% | -0.219 units | -10.94% |
| `>=10` | 298 | 166-132 | 55.70% | +12.484 units | +4.19% |
| Overall | 300 | 167-133 | 55.67% | +12.265 units | +4.09% |

Two bets cannot establish a betting-policy result. They only confirm that low-minute outcomes can reach the funded portfolio despite being excluded from rate fitting and combined calibration.

## Interpretation

### Five-minute acquisition threshold

The broad prevalence audit proves that `<5` quoted outcomes exist and are removed from training/calibration: 77 in training and 119 in calibration. However, no `<5` outcome appeared in the exact saved model-output cohort, so there is no matched error evidence showing that lowering the acquisition threshold would improve the distribution we actually scored or bet.

Lowering the acquisition threshold immediately would mix two changes:

- adding very short appearances to the minutes target;
- exposing unstable per-minute outcomes unless rate eligibility remains independently protected.

The current evidence is insufficient for the first change and argues against the second.

### Ten-minute rate/calibration threshold

The 5–<10-minute outcomes are rare, materially higher-error, and strongly overpredicted. Per-minute rates in this range are also structurally volatile. Keeping these rows out of rate fitting, rate calibration, combined calibration, and correlation estimation is the smallest evidence-supported policy.

This does not mean the settled outcomes should disappear from evaluation. They remain part of the betting distribution and should continue to be reported as a separate stress bucket.

### Minutes-model treatment

Rows from 5–<10 minutes already train the minutes model. Their poor holdout results do not justify removing them: exclusion would make the minutes model less representative of early-exit outcomes that can settle real bets. The current result instead identifies a conditional failure pocket to monitor and possibly model with pregame-available role/injury uncertainty in a separate experiment.

### Follow-up clarification: are low-minute players included in training?

The current answer depends on the realized-minute bucket:

- `<5 actual minutes`: the player-game row is excluded during acquisition and reaches none of the minutes or rate training/calibration paths.
- `5–<10 actual minutes`: the row trains and calibrates the minutes model, but is excluded from rate fitting, rate calibration, combined calibration, and minutes/rate correlation estimation.
- `>=10 actual minutes`: the row participates throughout the minutes and rate pipeline.

This means the minutes model is currently trained on minutes conditional on the player surviving at least five minutes, rather than the full settled-minutes distribution for all quoted players. That creates a plausible upward survivorship bias in the extreme downside tail. The measured prevalence is small, but nonzero: `<5` outcomes were 0.245% of the broader settled training cohort and 0.662% of the broader calibration cohort.

The bias is worth a controlled minutes-only test, but it is not yet evidence that the production threshold should be changed. Very short appearances often reflect injuries, ejections, or other events that were not predictable before tipoff. Including them can make the target population more honest while also adding irreducible noise.

### Do excluded low-minute players affect teammate training?

Excluding one player's `<5` target row does not remove the rest of that game. Other players' rows and their realized minutes and production remain in the training dataset. If a teammate received extra minutes or usage after the early exit, that outcome is still present in the teammate's training target.

However, inclusion of those teammate outcomes does not by itself teach the model why the redistribution occurred. For an unexpected in-game exit, no pregame feature can identify the event; the teammate increase appears largely as unexplained outcome variation. When a limitation or absence was known before the game, the explanatory path should be pregame injury, availability, starter/bench, rotation, projected-minutes, or usage-redistribution features.

Therefore, adding the low-minute player's own row would primarily correct the minutes target distribution for that player. It would not automatically create better teammate predictions or propagate an in-game opportunity change across the roster.

### Training takeaway

The evidence supports a clean separation of targets:

1. Minutes should ultimately be evaluated against every settled quoted outcome because minutes bets and downstream stat bets can settle after an early exit.
2. A minutes-only challenger should test admitting every positive-minute outcome, including `<5`, to measure whether removing survivorship bias improves downside-tail and final-stat probability accuracy.
3. Rate targets should retain the `>=10` floor because very short-appearance per-minute rates are unstable and were not supported by the error analysis.
4. Evaluation should never remove low-minute settled outcomes merely because they were excluded from fitting; they are part of the realized betting distribution.
5. Teammate redistribution should be addressed through information available before the game, not hindsight knowledge of the early exit.

## Smallest justified correction

No numerical threshold should change from this investigation alone.

The smallest durable code correction is to define rate eligibility once and consume it everywhere. `FeatureConfig.min_minutes_for_rate` is configurable in target construction, while tuning, feature selection, fitting, rate calibration, combined calibration, and correlation diagnostics repeat a hard-coded `10`. These paths currently agree, but they can silently diverge later.

A minimal follow-up should:

1. Introduce one shared rate-eligibility policy/helper with the current value of 10.
2. Route target creation, fitting, tuning, feature selection, rate calibration, combined calibration, and correlation analysis through it.
3. Add cohort accounting for `<5`, `5–<10`, and `>=10` to training and calibration receipts.
4. Make no model-population or production-policy change.

A later controlled experiment may compare the current minutes acquisition rule against a minutes-only `actual_minutes > 0` candidate while holding rate eligibility, features, hyperparameters, copula, sampler, BL policy, and evaluation window fixed. It should be rejected unless it improves minutes downside-tail loss and final-stat probability quality on a larger independent scored cohort without harming fixed-policy flat-stake results.

## Relevant prior lessons and invariants

- Preserve empirical sample probabilities; do not replace them with Gaussian CDF estimates.
- Never deploy global conformal offsets. The low-minute coverage gap is conditional and sparse, not evidence for a global correction.
- Q10 behavior is edge-bearing; any minutes-tail change must prove downstream betting value rather than optimizing a calibration chart.
- Use the cheapest controlled comparison before changing architecture or adding a new early-exit model.
- Large odds/source audits must remain keyed, bounded, and timeout-aware.

## Limitations

- The scored holdout is only 12 dates and was already used for model discovery; it is not independent confirmation.
- The `<5` error bucket is unobservable in the exact artifact-output cohort.
- The broader prevalence audit is conditional on retained T-60 feature-line coverage in the local training database, not canonical remote completeness.
- The broader evaluation source cohort and exact model-output cohort have different denominators. Their counts must not be combined as if every source quote reached model eligibility.
- Quote-opportunity errors weight each saved stat/bookmaker/line opportunity, not one row per player-game.
- Actuals were verified from local `player_game_stats`; full production settlement finality was not rerun.
- The two low-minute funded bets are descriptive only.

## Evidence receipts

- `reports/evidence/nba-low-minute-exclusion-analysis-2026-11-12.json`
  - SHA-256: `e60af8cd671dae4bfe1084532ec435d791c458895fe6de5da76d6dc4aaa97957`
- `reports/evidence/nba-low-minute-quoted-prevalence-2026-11-12.json`
  - SHA-256: `82e6e0438f6b3429bab9c947acb6aa58a81bf472f22e72242a97baa0c4824d8a`

Both database investigations used read-only local transactions, verified `transaction_read_only=on`, enforced a 15-second statement timeout, and performed no database or production changes.
