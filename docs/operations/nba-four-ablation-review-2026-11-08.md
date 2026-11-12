# NBA four-ablation review — November 8, 2026

## Kick off immediately

The clearest result is that player-prop market inputs are valuable to this model; removing them makes forecasts and betting selection worse. Short-form removal is a plausible simplification, not a demonstrated profitability breakthrough. No candidate is promotion-certified.

- This chat: completed the numerical analysis directly from saved artifacts/exports, including artifact controls, accounting, shared-cohort scoring and paired uncertainty. No subagent findings are required for the conclusions below.
- You: choose a scoped signal-development experiment after the priority clarification below. Later-window validation remains a separate confidence/promotion track; all long jobs remain owner-launched.
- Other chat: no additional analysis/writer needed. Preserve unrelated ingestion/linker/production work.
- Scope: analysis/evidence and restart-document reconciliation only; no training, replay, model deserialization, DB access, deployment or betting changes.

## Executive interpretation

1. Keep player-prop market inputs in the next serious baseline. Their removal is the strongest negative result: every fixed-BL threshold loses, probability accuracy worsens in each stat on a common scored cohort, and increased volume mostly adds losing bets.
2. Keep the game-line-free control as the safer comparison reference while historical game spread/total timing remains unverified. The original model performs better here, but that does not establish that the unverified source path is safe or that its advantage is leakage.
3. Carry no-short-form as a lean challenger. It removes 12 features per component union with almost unchanged predictive accuracy. Its +$34.07 BL improvement over the control is fragile, so prefer the complexity argument over a strong alpha claim.
4. Do not promote no-opponent-matchup as a general improvement. Its raw result worsens; the small BL improvement largely avoids somewhat worse losing bets. The family may be redundant or weakly used, but this does not prove matchup information is intrinsically useless.
5. The model remains heavily market-anchored. All raw settings lose across all five artifacts, while limited BL selections can profit. Adding feature families or retuning many settings is not yet supported by these experiments.

## What was actually compared

All new models exclude injuries and game spread/total features. Additional families are tested against the same game-line-free control, not independently against the original artifact.

| Label | Exact artifact directory suffix | Additional exclusion relative to control |
|---|---|---|
| Original | `no_injury_baseline/nba_run_20261108_122129` | Original reference retains game lines |
| Control C | `nba_feature_ablations/game_market_free/nba_run_20261108_150945` | None; game_market has 2 members |
| C minus short form | `nba_feature_ablations/game_market_short_form_free/nba_run_20261108_154528` | short_form: 12 members |
| C minus matchup | `nba_feature_ablations/game_market_opponent_matchup_free/nba_run_20261108_161911` | opponent_matchup: 14 members |
| C minus prop market | `nba_feature_ablations/game_market_prop_market_free/nba_run_20261108_171518` | prop_market: 4 members |

Artifact paths are relative to `src/models/artifacts/`. Sweep paths are recorded in the JSON evidence. The last three sweep directory names contain `95`, but their manifests and exports contain **10 cells each**, not 95. There are 40 new cells plus the original 95-cell reference; only the ten matching configurations per artifact are used in the main comparisons.

### Comparability checks

- Same fitting and calibration row-key populations: 46,249 fit rows / 2,452 games and 23,898 calibration rows / 1,278 games. Fit seasons `22023`/`22024`, calibration `22025`, null explicit cutoff.
- Exact fixed hyperparameter dictionaries, selector tolerance .005 and no-tuning settings match. All no-injury obligations hold.
- All requested exclusions are absent from every selected quantile list. Run-config, manifest and effective-list evidence agree. General exclusion counts are 2/14/16/6 for C/no-short/no-matchup/no-prop, independently of the shared injury exclusion.
- Same October 21–November 2 discovery window; 12 betting dates, October 22–November 2. Same $2,000 bankroll, nominal $10 stakes, seed 42, 5,000 samples, minimum average minutes 10, books and side policy.
- Same recorded actuals/voids digests, same acquisition counts, same predictor settings and dependency versions. Copula parameters are identical across all models; global offsets remain disabled.
- All 2,932 quoted base-stat opportunities have identical keys, prices and checked quote-time fields across models. There are no combo quotes. These inputs collapse to the same 1,119 selected player-games; each cell chooses one stat/side per player-game.
- All new summary receipts, all 135 cell bet/config/metrics receipts and all referenced model-file hashes were checked. Stakes/profits reconcile; saved outcomes/payoffs reproduce; checked selected quote timestamps never follow their selected decision times.

These are strong saved-evidence comparability checks, not full historical-source certification. Row-key equality does not prove that every fitting-feature value was identical; full fitting matrices are not retained in these exports. `historical_readiness=unverified` and `decision_grade=false` still apply.

## Primary fixed-setting results

The primary table uses raw edge .05 and the frozen BL edge .05 / tau .5 / z_max .5 / max_weight .5. No candidate receives its own newly optimized configuration.

| Model | Raw bets | Raw ROI | BL bets | BL profit | BL ROI | BL drawdown | Saved BL daily Sharpe |
|---|---:|---:|---:|---:|---:|---:|---:|
| Original | 972 | -5.91% | 271 | +$113.00 | +4.17% | 3.22% | 4.43 |
| Control C | 944 | -8.11% | 262 | +$65.15 | +2.49% | 4.74% | 2.62 |
| C minus short form | 944 | -7.00% | 258 | +$99.22 | +3.85% | 2.85% | 4.84 |
| C minus matchup | 929 | -9.23% | 242 | +$88.76 | +3.67% | 4.66% | 3.45 |
| C minus prop market | 995 | -7.87% | 587 | -$108.42 | -1.85% | 15.52% | -1.72 |

Sharpe is annualized from a very short daily series; its magnitude is not evidence of a durable Sharpe near four. These are exploratory policies on a reused selection window.

### Threshold stability

| Model | Raw ROI at edges 3 / 5 / 7 / 10 / 12% | Fixed-BL ROI at the same edges |
|---|---|---|
| Original | -4.48 / -5.91 / -6.13 / -6.28 / -7.26% | -2.59 / +4.17 / +2.98 / -3.14 / +2.33% |
| C | -7.59 / -8.11 / -8.52 / -5.00 / -5.72% | -3.15 / +2.49 / +1.17 / -11.07 / -0.90% |
| C minus short form | -5.37 / -7.00 / -6.63 / -4.85 / -7.03% | -2.03 / +3.85 / -1.83 / -8.27 / -7.50% |
| C minus matchup | -8.48 / -9.23 / -10.23 / -5.65 / -0.76% | -0.83 / +3.67 / -0.67 / -4.54 / +1.55% |
| C minus prop market | -7.29 / -7.87 / -7.30 / -6.39 / -3.47% | -3.62 / -1.85 / -2.14 / -2.80 / -5.46% |

No raw cell is profitable. Short-form removal improves raw ROI versus C at four of five thresholds, but its fixed-BL portfolio profits only at .05. Matchup removal improves BL ROI at four of five thresholds but raw ROI at only one; its positive BL .12 cell has only 36 bets. Do not declare a stable winner from those small high-edge cells.

## Why the family results differ

### Player-prop market inputs: retain

Removing four prop-line covariates changes the model much more than removing 12 short-form or 14 matchup covariates.

At the fixed BL setting, volume rises from 262 to 587 bets (2.24x), while profit falls by $173.57. The comparison retains 237 identical bets, drops 25 control-only bets that earned $52.53, and adds 350 candidate-only bets that lose $121.04 (-3.46% ROI). This is an increase in false-positive betting activity, not a newly found independent edge.

The raw .05 model's mean selected win probability rises from C's 60.52% to 66.86%, while its realized hit rate is only 49.05%. This is selection-conditional overconfidence in this window, not a whole-population calibration estimate.

On the identical 1,760-outcome scoring cohort described below, removing prop inputs raises PTS/REB/AST median absolute error from approximately 5.31/2.17/1.71 to 5.70/2.32/1.88. Probability-error deterioration is positive for every stat under the exploratory paired date bootstrap. Thus the negative conclusion has support beyond one profit cell.

This establishes practical value for the entire prop-input family in the current pipeline, not which individual prop or which component deserves it. Inputs are removed from minutes and all rate models together. Do not infer that `prop_line_threes` is useless simply because threes is not an output model, or that prop inputs should be removed only from one corresponding rate head.

Decision for the next baseline: retain prop inputs; shelf the all-prop-excluded candidate as a useful negative control. This does not authorize promotion or prove future profitability.

### Short-form: lean challenger, not proven new alpha

Distinct selected features across quantiles fall from C's 55/56/55/56 in minutes/PTS/REB/AST to 43/44/43/44. At the union level, the difference is exactly the 12 removed family members; no nonfamily additions/removals appear. Per-quantile selection and fitted weights can still change.

Median prediction errors and probability scores barely move. The strongest implication is that the current short-form representation may be redundant or weakly used. Saved metadata cannot distinguish genuine redundancy from poor/default-only source coverage, so this is not permission to retire the underlying source or declare recent form conceptually useless.

The BL profit gain is only $34.07. It retains 237 common bets, removes 25 that lost $33.85 and adds 21 that make just $0.22. It avoids bad marginal bets; it does not demonstrate a new productive candidate-only segment.

Stat contributions explain the gain: AST improves $57.88, PTS $1.28, and REB declines $25.08. The ablation is not uniformly better. Its advantage is negative in the first six betting dates and positive in the last six. Removing one influential date can reverse the BL profit advantage.

Decision: retain as the preferred lean challenger for a fixed later-window comparison. Keep C as the reference until that evidence exists.

### Opponent matchup: mixed; do not remove by default yet

Selected unions shrink to 41/42/42/42. Median/quantile/probability errors remain close to C, consistent with redundancy or limited incremental use, not proof that the feature concept is worthless.

Raw .05 ROI deteriorates from -8.11% to -9.23%. BL .05 profit improves by only $23.61. The comparison retains 207 identical bets, removes 55 that lost $50.01 and adds 35 that still lose $26.40. The improvement is weaker avoidance of losing exposure, not profitable new bets.

The first-half BL comparison loses $22.86 relative to C; the second half gains $46.47. Its advantage can also reverse when one date is omitted. Keep the family in the main reference; treat its exclusion as an interaction/simplification hypothesis rather than a proven upgrade.

### Game lines: retain the exclusion for timing discipline, not because it won

C loses $190.63 more than the original at raw .05 and $47.85 more at fixed BL .05. Removing spread/total did not improve these results, even though component calibration improved.

The ordinary historical game-line source remains the known timing caveat. A higher original ROI does not justify reinstating an unverified information path, and this comparison does not prove the original gain was leakage. Preserve the original as a reference and keep C as the common research control unless timing is separately resolved.

## Prediction quality on a fairer common cohort

The per-cell `predictions.csv` contains one selected stat per player-game, and changing a model or BL setting changes the selected stat. Directly comparing those files' aggregate calibration mixes prediction quality with cohort composition.

To reduce that problem, this analysis extracted every unique receipt-verified prediction export, recovered consistent actual labels, and scored all five models' saved candidate predictions on the **same 1,760 player/game/stat outcomes**: 629 PTS, 668 REB, 463 AST, over 90 game IDs and 12 dates. Quotes are identical across models. This is the available-label union, not every generated prediction; conclusions must remain conditional on this scored cohort.

Brier loss is the mean squared error of the predicted probability versus the binary over outcome; lower is better. It measures probability quality, not calibration alone.[4]

| Model | PTS median MAE | REB median MAE | AST median MAE | Overall over-probability Brier |
|---|---:|---:|---:|---:|
| Original | 5.3165 | 2.1717 | 1.7085 | 0.25722 |
| C | 5.3105 | 2.1675 | 1.7103 | 0.25723 |
| C minus short form | 5.3098 | 2.1709 | 1.7094 | 0.25715 |
| C minus matchup | 5.2999 | 2.1749 | 1.7141 | 0.25703 |
| C minus prop market | 5.7042 | 2.3205 | 1.8805 | 0.27056 |
| Same quoted market probability | — | — | — | 0.24510 |

This is not a market-only betting replay or a market median forecast comparison. It compares the saved paired-market probabilities with the model's raw probabilities on the same over outcomes.

The substantive differences are:

- Short-form and matchup exclusions have extremely small prediction-score effects; their intervals include no change.
- Prop exclusion increases Brier by 0.01333, with an exploratory date-block interval of +0.00915 to +0.01774. Per-stat Brier deterioration is also consistently positive under this diagnostic.
- The market probability beats every model's raw probability on this available-label cohort. This supports prioritizing incremental signal relative to the market rather than counting features or celebrating larger raw edges.
- All models' common-cohort medians undershoot actuals on average. These whole-suite ablations cannot identify whether that begins in minutes, rates or their combination; do not diagnose a particular component from aggregate betting P&L.

### Calibration is not the selection criterion

The no-prop model improves the worst minutes-component coverage gap from C's 2.82 pp to 1.66 pp and AST rate from 1.33 pp to 0.99 pp, yet its forecasts and betting selection worsen. That is a direct example of why component coverage alone is not a promotion gate.

Combined calibration remains sampled rather than full-population evidence. All five models retain the same fitted copula parameters and simulation settings. There is no evidence here that a new copula, survival model, global offsets or a blanket Q10 correction is the next intervention.

## Stat, side and shared-bet implications

Rebound unders remain the strongest consistent region, but the arms overlap heavily and are not independent confirmations.

| Model | BL PTS profit | BL REB profit | BL AST profit | BL under ROI | BL over ROI |
|---|---:|---:|---:|---:|---:|
| Original | +$37.55 | +$90.18 | -$14.73 | +10.54% | -10.52% |
| C | +$32.63 | +$97.10 | -$64.58 | +9.13% | -9.78% |
| C minus short form | +$33.91 | +$72.02 | -$6.71 | +9.65% | -6.45% |
| C minus matchup | +$37.03 | +$111.63 | -$59.90 | +6.12% | -1.21% |
| C minus prop market | -$154.62 | +$84.80 | -$38.60 | +3.84% | -15.12% |

All five portfolios share 174 identical bets, earning $133.85 (+7.69% ROI). Forty-eight common rebound-under bets earn $82.99. Those common bets explain much of the repeated favorable signal; the model variants often change the less successful margin of the portfolio. Do not turn the post-hoc five-model agreement slice into a certified ensemble/filter without independent testing.

AST remains weak in aggregate at the frozen setting, although short-form removal substantially reduces its loss. Overs are negative in every portfolio. These are reasons to report side/stat results separately and preregister any future restricted policy, not to silently deploy an under-only policy or discard an entire model head. Saved under-only subsets are not replays that can choose alternative bets after filtering.

The models have different fitted minutes and rate components. Do not splice the best-looking PTS/REB/AST components across artifacts without a coherent combined-artifact experiment and validation.

## Uncertainty and what is not established

Paired bootstrap uses the same resampled dates for both portfolios, 5,000 resamples and a fixed random seed. It preserves within-date dependence but has only 12 dates and does not correct repeated model/configuration selection.

| Candidate versus reference | BL profit difference | Exploratory paired date interval |
|---|---:|---:|
| C versus original | -$47.85 | -$158.29 to +$58.60 |
| No short form versus C | +$34.07 | -$104.54 to +$169.55 |
| No matchup versus C | +$23.61 | -$121.40 to +$168.05 |
| No prop market versus C | -$173.57 | -$482.56 to +$204.87 |

None establishes a reliable betting-profit improvement. Prop removal is rejected for the next candidate because its consistent predictive deterioration, volume expansion and fixed-grid losses agree—not because its profit interval alone proves a universal loss.

No CLV, new-window, forward-paper or live-money evidence was collected. No family decision is independently certified. Source availability/default variation was not audited from the live fitting matrix; do not interpret negligible effects as proof of absence of a basketball signal.

## Recommended next actions

### Priority clarification: signal development versus confirmation

Chase reaffirmed that the objective is stronger betting signal, not an open-ended series of minor confirmations. The numerical findings above are unchanged, but the earlier ordering overemphasized validation and simplification as the primary development work. The recommendations here are proposals, not approval for source changes, prediction jobs, tuning or training.

- **Existing-model comparison on new dates is validation.** It uses the already fitted weights and asks whether the apparent advantage generalizes. It does not improve either model or create new signal. Repeating the same dates/settings would provide essentially no new modeling information unless reproducing a failure. Validation matters before trusting a candidate, but should not make active development wait for more dates.
- **Combined short-form/matchup exclusion is an interaction/simplification experiment.** If joint removal hurts while individual removal barely matters, the families may substitute for each other; if it is neutral, a smaller representation is viable; if it helps, noise reduction may improve use of existing information. It adds no new source information. Given the near-identical individual forecasts, expected information value is modest rather than a likely major betting improvement. Keep it optional.
- **The higher-priority development question is why model disagreement with the market is insufficiently useful.** The common-cohort probability result does not prove there is no profitable subset. Separate conditional forecast error, minutes/rate composition and bet-selection amplification rather than treating weak aggregate ROI as a diagnosis of a particular feature or head.

Proposed bounded next work for this chat: identify one actionable bottleneck, then scope one controlled learning experiment. Separate minutes prediction, per-minute rate prediction and combined line probabilities on aligned cohorts, plus a targeted check that the implicated inputs contain real non-default variation. Existing exports mostly contain combined predictions, so component-level prediction export may be needed; it has not been produced by this review. The deliverable is a choice of one intervention with its expected effect, not a new broad readiness audit or more summary tables without a decision.

Possible outcomes have different consequences: a minutes-driven miss calls for a minutes experiment evaluated across all stats; a rate-driven miss calls for a stat-specific rate experiment with the shared minutes model fixed; reasonable components but poor combined probabilities call for a bounded predictor/composition comparison; a loss concentrated only after extreme-edge selection points toward selection-policy robustness. Default-heavy inputs would call for a narrowly evidenced consumer/source repair, not more family permutations. None of those causes is established by the completed ablations alone.

There is a concrete retuning concern: `src/models/hyperparameter_tuner.py:163-207,211-244` trains XGBoost with quantile loss but ranks Optuna trials by worst absolute coverage gap (or the single quantile gap). That outer objective is not pinball-loss minimization or betting-signal optimization. A targeted retune is a plausible development experiment, but not by blindly adding `--tune`: specify a proper predictive objective, matching quantile feature contract and fitting-only whole-date chronological validation/fold-local selection, then check downstream combined probabilities and fixed flat-stake betting behavior. Do not optimize the small discovery window's ROI or correct Q10/global offsets merely to improve a calibration number. Fixed incumbent parameters were the ablation comparison control, not a permanent ban on tuning.

If existing information cannot support improvement under a well-specified learning experiment, choose a new feature family tied to a measured failure. Do not infer that architecture or more data is automatically required. Keep prop inputs in the working baseline while investigating incremental value beyond the market.

Relevant prior lessons remain cheap controlled experiments before complex architecture, whole-family rather than selector-only evidence, and no automatic calibration/promotion changes.

### Parallel validation track: freeze the reference and lean challenger

Freeze C and no-short-form, retain prop inputs, keep the current .05 raw and .05/.5/.5/.5 BL comparisons, and evaluate a later accepted `22026` interval that has not been used to select settings. Keep raw threshold sensitivity visible but do not choose a new optimum for every model. This discovery window cannot serve as the final untouched test; `22025` was calibration data and is not independent for these artifacts.

### Optional low-cost interaction experiment

Run exactly one combined simplification hypothesis: game_market + short_form + opponent_matchup excluded, prop_market retained, same fixed parameters and injury lock. Compare with C and no-short-form. This tests whether the two apparently weak families substitute for each other; their individual removals must not be assumed additive. It is optional discovery work, not a promised improvement or automatically approved run.

Do not expand to more sources, a broad retuning matrix, automatic stat-head mixing or new distribution architecture merely because a simpler ablation did not win. A targeted, separately identified retune can proceed as development without waiting for the later validation window, once its objective and controlled scope are approved. Do not use global recalibration to compensate for weak selected-bet performance.

Practical disposition:

- Retain prop-market inputs in the working baseline; shelf the all-prop-excluded candidate.
- Keep C as the timing-conscious comparison reference.
- Keep no-short-form as a lean challenger, not a promoted winner.
- Keep matchup by default; optional joint-removal test can investigate redundancy.
- Keep minutes-role/schedule/injury expansion deferred rather than reflexively starting a second broad wave.

## Audit notes and evidence

The original 95-cell summary CSV no longer matches its stored byte hash. Its numeric differences from receipt-verified cell metrics are tiny (largest absolute difference across checked scalar fields about 3.72e-7). All original cell metrics/config/bet receipts and model-file hashes still match. Original-reference numbers here were reconstructed from those verified cell files, not trusted from the modified summary. No original artifact/export was rewritten.

The four new summary receipts match. Their core replay code fingerprints match each other; the final run has a newer Git commit identifier, but the recorded replay source file hashes are unchanged. Original replay source differences are the previously documented console/validation changes. Unknown source-policy fields remain unknown; they are not automatically a newly reproduced data failure.

Evidence files:

- [Full numerical review](evidence/nba-four-ablations-2026-11-08-analysis.json): exact inventory, recipe/feature checks, ledger hashes, common quote and outcome parity, fixed-cell slices, paired portfolios, bootstrap intervals and score cohort definitions.
- [Matched 50-cell table](evidence/nba-four-ablations-2026-11-08-matched-cells.csv): original matching ten cells plus all 40 new cells.
- [Common-cohort scores](evidence/nba-four-ablations-2026-11-08-common-cohort-scores.csv): per-stat and pooled probability/quantile/point diagnostics for the same saved-label cohort.

Relevant prior lessons: `lessons/feature-selector-is-not-an-ablation`, `lessons/correlated-feature-family-validation`, `lessons/cheap-baseline-before-architecture`, and `lessons/implausibly-profitable-backtests-are-methodology-red-flags`. Apply empirical CDF probabilities, no global conformal offsets, preservation of Q10 behavior, owner-launched long jobs and separate model-promotion gates.

Sources:
[4] https://scikit-learn.org/stable/modules/model_evaluation.html — Scikit-learn model evaluation: Brier score loss
