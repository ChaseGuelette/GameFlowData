# NBA no-injury baseline: modeling dossier

As of November 12, 2026. Source inspection anchor: application main `65611ff61d84733bed0ce01b250b639ab08ff7ed`.

## 1. Reference and evidence boundaries

Owner direction for this discussion: use `src/models/artifacts/no_injury_baseline/nba_run_20261108_122129` as the starting point for separate model-improvement analysis. This is an analysis reference, not a new training, promotion, betting or deployment authorization.

Manifest identity: `8e1c8b65b0e0dfc0c87f83d35341fb45ba4d185c428120c877b2366518ee151d`. All 13 manifest-listed file sizes and SHA-256 hashes were rechecked during this inspection and matched. Model binaries were not deserialized and no fits, replay or database queries were run.

The baseline excludes injury-derived model features, but retains both game spread/total and player-prop inputs. It is not market-independent. An Out-player eligibility check at serving time is separate from injury covariates in the learned model.

Do not confuse this artifact with:

- `nba_feature_ablations/game_market_free/nba_run_20261108_150945`: a separately refitted control without `line_spread` and `line_total`; it has 56 rather than 58 unique selected features.
- `nba_rate_pinball_feature_match/nba_run_20261110_092318`: a rate-only retune of that game-market-free control, not a retune of the chosen original baseline.
- Local `artifacts/production/`: its inspected run configuration is dated March 23, 2026 and is not the November baseline. `artifacts/production.policy.json` was absent locally at inspection. No live artifact identity was checked.

The current code includes subsequent tuner and paper-policy changes that did not retroactively modify the baseline. Artifact metadata describes what was saved; source describes how current paths work. Historical source provenance remains `source_verified=false` and `decision_grade=false`.

## 2. Model architecture and targets

The main architecture is XGBoost quantile regression with minutes-times-rate decomposition:

1. One minutes component predicts `actual_minutes`.
2. Three rate components predict `pts_per_min`, `reb_per_min`, and `ast_per_min`.
3. Each component has separate Q10, Q25, Q50, Q75 and Q90 regressors: 20 learned heads in the four-component suite.
4. Quantile curves become marginal sample distributions.
5. A Gaussian copula couples each stat's rate with shared simulated minutes.
6. Final stat draws are `minutes_draw * rate_draw`.
7. Empirical line probabilities are calculated from those draws.
8. BL optionally anchors probabilities to paired sportsbook prices.
9. A separate selection policy chooses wagers.

Each regressor uses `objective="reg:quantileerror"` and its own `quantile_alpha`. These are conditional quantiles of the outcome, not confidence intervals on a single estimated mean. Q50 is a conditional median, not necessarily the expected value. A product of component medians is not generally the median of the final product; the system obtains final quantiles from sampled products.

The active base-stat prediction scope is PTS/REB/AST. A trained threes component is not part of this saved baseline, although `prop_line_threes` and historical shooting features are inputs. Combo predictions are derived, not separately trained.

Sources: `src/models/quantile_trainer.py:24-45,163-192,389-508`; `src/models/monte_carlo.py:278-375`.

## 3. Training population, chronology and eligibility

Saved baseline populations:

- Fitting seasons: `22023`, `22024` (2023-24 and 2024-25 regular-season tokens).
- Acquired fitting population: 46,249 player-game rows across 2,452 games, October 24, 2023-April 13, 2025.
- Separate calibration season: `22025`.
- Acquired calibration population: 23,898 rows across 1,278 games, October 21, 2025-June 13, 2026.
- No explicit calibration end-date cutoff (`cal_end_date=null`).
- No sample weighting (`sample_weighting="none"`).
- Initial subsequent-season discovery replay: October 21-November 2, 2026. This window has already been used for development and policy selection, not untouched validation.

Acquired row counts are not the number fitted into every head. Current acquisition requires at least five actual minutes; rate training additionally requires at least ten minutes and a non-null rate. Minutes training checks positive minutes on the acquired population. Separately, rolling-feature policy excludes appearances below eight minutes from rolling statistical values while schedule counters still count appearances. The 5/8/10 thresholds serve different purposes and must not be conflated.

Current chronology guards validate literal season tokens, unique player-game identity, consistent game/date/season metadata, no training/calibration game overlap, and strict ordering of observed training before calibration dates. Dates, rather than individual player rows, define current inner splits when passed by the NBA caller. The 15% inner holdout is the trailing 15% of distinct dates, rounded up—not necessarily exactly 15% of rows. The final early-stopping holdout is not refitted into the booster after choosing the best iteration.

Current source is prior/as-of oriented: rolling features, historical position and quote cutoffs must be pregame; player history is retained across trades, while target-game team/opponent context changes. Historical feature truth is not established merely because these current consumer guards exist. Game spread/total timing remains a specific unresolved caveat for this chosen baseline.

Sources: baseline `run_config.json` and `artifact_manifest.json`; `src/models/features/nba/training_loader.py:46-101,195-255,333-338`; `src/models/nba_training_chronology.py:38-102`; `operations/critical-invariants`.

## 4. Exact feature scope

There are three different notions of a feature list:

1. Static feature-store contracts: 33 minutes, 31 PTS, 30 REB, 30 AST and 32 threes entries; 74 distinct names across those contracts.
2. Runtime numeric candidate columns after target/metadata exclusions and explicit family controls.
3. The actual saved, ordered list for each component and quantile.

The third is the authority for this artifact. Its four components use 58 distinct features overall. Most heads use a broad shared context, rather than only the narrow stat-specific static contract. All four player-prop lines can enter a minutes or rate head; a PTS head is not restricted to PTS features.

The exact per-quantile counts and complete feature membership inventory appear in Appendix A. All ordered per-quantile lists are preserved verbatim in the companion JSON snapshot.

Feature categories in the baseline include:

- Player workload/role: L3/L5/L15/season minutes, workload ratios, L5 minutes floor/dispersion and recent starts.
- Recent production: PTS/REB/AST/threes history, volatility and momentum ratios.
- Efficiency/opportunity: usage, true shooting, rebounding percentage, assist percentage, three-point attempts.
- Team/opponent environment: pace, shooting profile and defensive rating.
- Opponent-position matchup: rates/counts allowed for points-related offense, rebounds, assists and threes, including per-100-possession variants.
- Markets: game spread/total and PTS/REB/AST/threes prop lines.
- Schedule/location: rest, games in seven days and home status where selected.

No selected feature starts `team_out_` or `opp_out_`; `player_is_questionable` and `player_is_probable` are also absent. In the union, `player_starter_prob` and `is_back_to_back` are not selected, although starts, minutes and rest can represent related information.

Important semantics: the historical feature names `player_reb_l3_l15_ratio`, `player_ast_l3_l15_ratio` and `player_fg3m_l3_l15_ratio` actually divide L3 by L5. Only the PTS counterpart divides L3 by L15. Do not infer formulas from those names. Missingness handling is not uniform: absent rolling rows have explicit zero/league-style defaults; some prepared values retain NaN for XGBoost. This is not a learned cold-start or missingness model.

Sources: exact `selected_features.json`; `src/models/features/nba/contracts.py:10-250`; `src/models/features/nba/transforms.py:13-128`; `src/models/nba/feature_controls.py:8-43`.

## 5. How feature selection works

Full training performs feature selection before hyperparameter tuning and final fitting, using fitting-season data only.

For each component and each quantile:

1. Find numeric candidate columns, excluding actual outcomes, rate targets, IDs, dates and other protected metadata.
2. Apply requested feature/family exclusions and required inclusions. Injury exclusion removes its whole predicate-defined family, not just one injury flag.
3. Rank candidates with a fixed quantile XGBoost proxy: 100 trees, depth 3, learning rate .05, seed 42.
4. Calculate permutation importance, five repeats, using negative pinball scoring. The current ranker evaluates importance on the proxy's training partition from the last temporal fold—not its held-out partition.
5. Normally keep positive-importance candidates; if fewer than three are positive, retain the full ranking.
6. Test ranked prefixes of 5 through N features using three temporal folds and a second proxy (50 trees, depth 3, learning rate .1).
7. Find the prefix with minimum average quantile pinball loss.
8. Apply the relative tolerance, saved as .005 for this baseline: choose a larger prefix when its loss is within 0.5% of the best. The algorithm deliberately tends toward the largest acceptable prefix, not the smallest adequate feature set.

Feature selection and hyperparameter tuning optimize different proxy procedures. They are not a single nested search. The ranking is reused across feature-count folds; selected lists are then reused for later tuning/early-stopping validation. Consequently, current feature selection is not fully fold-local relative to those validation observations. This is an internal model-selection optimism risk, not proof of leakage into the separate 2026-27 discovery replay.

Six explicit ablation families exist: `game_market`, `prop_market`, `opponent_matchup`, `short_form`, `minutes_role`, `schedule`. Injury exclusion is separate and predicate-based. Selection/importance is not evidence that an entire correlated family adds betting value; that requires controlled inclusion/exclusion comparisons.

Sources: `src/processing/feature_selection.py:31-207,641-677`; `src/models/train_pipeline.py:203-212,709-719,833-865`; `src/models/nba/feature_controls.py:8-31,146-208`.

## 6. The baseline's saved hyperparameters

This baseline did not perform a new Optuna search. It used fixed incumbent hyperparameters. The game-market-free control has byte-identical `best_hyperparams.json`; later rate-retuned artifacts do not.

The table in Appendix B is generated directly from baseline JSON. Each component shares one parameter configuration across its five quantiles, but different feature lists and early-stopping best iterations can produce different effective boosters.

All components record validation fraction .15 and seed 42. Older saved configurations do not include `reg_alpha` or `reg_lambda`; the current backward-compatible defaults are 0 and 1. No claim of newly verified constructor parameters inside the baseline binaries is made; these binaries were only hashed.

Sources: baseline `best_hyperparams.json`; `src/models/quantile_trainer.py:24-62,170-192`.

## 7. How current hyperparameter tuning works

### Three separate objectives

- Estimator fitting: per-quantile XGBoost pinball/quantile error.
- Feature selection: proxy validation pinball loss for each quantile.
- Current NBA Optuna selection: validation pinball, explicitly set by full and partial NBA callers.

The shared tuner's default still says `calibration_gap` for compatible non-NBA/legacy callers. That is not the current NBA caller's effective objective. The old objective minimized worst absolute quantile coverage gap; current NBA shared trials minimize the equal-weight mean of the five quantiles' pinball losses for one component. This does not directly optimize final-stat line probability, Brier/log loss, EV or profit.

For residual r = actual - prediction, quantile pinball is `max(q*r, (q-1)*r)`. Scores have target units: minutes and different rates cannot be pooled or ranked by absolute loss without normalization.

### Search and validation

Optuna uses seeded TPE sampling, minimization and a median pruner. Shared mode samples one parameter set and fits the component's five quantiles. Pruning reports the running same-objective score as successive quantiles finish. Separate mode uses one study per quantile, with the requested number of trials per study.

The current pipeline default is tuning off, 50 trials if enabled. The tuner class's standalone default is 100. The completed November 10 challenger used 100 shared trials for each of PTS/REB/AST, freezing minutes and feature selection. That experiment budget is not a universal default and its parent was the game-market-free control.

Search ranges:

| Parameter | Range | Sampling |
|---|---|---|
| max_depth | 3-8 | integer |
| min_child_weight | 1-20 | integer |
| learning_rate | .01-.15 | logarithmic |
| n_estimators | 200-1500 | integer |
| subsample | .60-.95 | linear |
| colsample_bytree | .60-.95 | linear |
| reg_alpha | 1e-8-10 | logarithmic |
| reg_lambda | 1e-8-10 | logarithmic |
| early_stopping_rounds | 30-100 | integer |

### Feature and estimator parity

By default, tuning uses each component's union of selected features for every quantile. Final fitting uses each head's specific list. `--tuning-match-final-features` opts into making trial fit/eval/predict use those same ordered final lists. It does not reselect features or create a fully nested feature-selection procedure.

Regularization/early-stopping handoff was repaired after the earlier exploratory November 9 candidate: current shared-mode config conversion forwards sampled regularization, patience, seed and validation fraction to final training. Incompatible old persisted studies are rejected. The completed November 10 shared-mode artifact was subsequently reviewed for effective estimator parity.

A separate optional-path issue was identified in this inspection: per-quantile tuning writes nested configurations, while the final component trainers call a flat `QuantileModelConfig.from_dict`. An isolated execution of that actual AST-extracted class confirmed that a nested quantile mapping is silently converted to generic defaults. This is a source-level/per-conversion reproduction, not a full training reproduction; it does not affect this untuned baseline or the inspected shared-mode challenger. It is not repaired by this documentation task.

Do not infer that component studies contaminate each other merely because they have the same name: the normal NBA caller supplies no persistent storage. A controlled Optuna check confirmed two same-named `storage=None` studies have separate storage/trials. Explicit persistent-storage reuse would need separate component names and acceptance checks.

Sources: `src/models/hyperparameter_tuner.py:41-56,84-116,163-244,246-339,345-508`; `src/models/train_pipeline.py:568-640,730-824`; `src/models/quantile_trainer.py:51-62,396-403,460-467`.

## 8. Fitting, early stopping and calibration

Final component fitting uses its chronological inner train/validation split. Each quantile fits with its selected ordered columns, uses its configured early-stopping patience and the validation eval set, and saves its own booster. `n_estimators` is a maximum budget, not a promise that every head uses that many trees.

There are distinct calibration mechanisms:

1. Embedded component offsets: the trainer computes a quantile residual offset if its internal validation coverage gap exceeds .03. `predict_quantiles` applies saved component offsets and then a cumulative-maximum quantile repair. The numerical offsets embedded in this specific baseline's binaries were not deserialized here; JSON coverage reports do not prove they are all zero.
2. Separate-season component reports: coverage diagnostics on the calibration population.
3. Combined minutes-times-rate report: a sampled end-to-end diagnostic. Current code keeps rows with at least ten actual minutes and complete outcomes, caps at 2,000 rows, uses 1,000 draws and counts/skips prediction failures.
4. Global combined offsets: diagnostic files written alongside that report. They must not be applied in the reference research/paper prediction path. File presence is not evidence of use. No global conformal offset deployment is authorized.

Baseline separate-season maximum absolute component coverage gaps are minutes 3.97 percentage points, PTS rate 2.98, REB rate 1.89 and AST rate 1.40. Sampled combined maxima are PTS 5.2 pp, REB 5.4 pp and AST 7.6 pp. Combined AST Q10 coverage is 17.6%; this is not itself permission to correct the tail. These component and combined reports differ in population, draw count and quantity measured.

Normal component quantile predictions ARE repaired for monotonicity before reaching Monte Carlo: `np.maximum.accumulate` enforces Q10 <= Q25 <= Q50 <= Q75 <= Q90. The inverse-CDF helper does not independently repair arbitrary crossed inputs, but claiming the ordinary component path lacks repair would be incorrect.

Sources: `src/models/quantile_trainer.py:142-228,300-343`; `src/models/train_pipeline.py:1075-1218`; baseline calibration reports.

## 9. Quantile-to-distribution construction

Reference historical sweep settings, read from its saved manifest:

- 5,000 draws; `np.random.RandomState(42)`.
- Lower-tail multiplier 1.15; upper-tail multiplier 1.0.
- PTS/AST spread multiplier 1.0; REB spread multiplier 1.15.
- Blowout mixture disabled.
- Saved copula parameters enabled.
- Global combined offsets null.

The generic MonteCarloPredictor constructor defaults to 10,000 draws, so constructor defaults must not be substituted for actual reference settings.

The marginal inverse CDF uses five learned quantiles plus tail anchors at p=.001 and .999. Tail values are extrapolated using the Q10-Q25 and Q75-Q90 segment slopes, with the lower extrapolated value floored at zero. Values below .001 are snapped to zero. Uniform values are clipped to [.001,.999] and mapped by piecewise-linear interpolation.

Minutes and rates are nonnegative. Final products remain continuous: there is no Poisson/count draw, integer rounding or explicit multinomial/negative-binomial count likelihood. Zero mass is represented approximately by flat zero segments/snapping. No explicit game-minute upper cap is enforced by these sampling operations.

REB's 1.15 value scales sample deviations around the sample median: `median + 1.15*(sample-median)`, then floors at zero. It widens sample spread, not necessarily mathematical variance by exactly 15%, especially after clipping. This and the tail multipliers affect final probabilities without changing the boosters.

The RNG persists across player predictions. Deterministic row order matters; canonical sweeps freeze one set of samples and reuse it across cells, rather than regenerating outcomes for each BL/edge configuration.

Sources: saved `backtest_results/nba_no_injury_holdout_sweep_95_nov2_v1/sweep_manifest.json` predictor settings; `src/models/monte_carlo.py:74-105,148-214,535-557,718-817`.

## 10. How minutes and rate are coupled

The baseline's saved Spearman coefficients are:

- PTS: .3356
- REB: -.0029
- AST: .2128

They are pooled training-population correlations between actual minutes and observed per-minute rate, using at least ten actual minutes and valid values. Fewer than 50 valid observations would produce zero; saved coefficients are rounded to four decimals. These are not fitted correlations of out-of-sample forecast residuals.

At prediction:

- Draw one latent `z_minutes ~ Normal(0,1)` per simulation index.
- Convert its normal CDF to a uniform and map through the minutes inverse CDF.
- For each stat convert Spearman rho_s into latent-normal rho_g: `rho_g = 2*sin(pi*rho_s/6)`, clipped to [-.999,.999].
- Draw an independent normal vector for that rate and set `z_rate = rho_g*z_minutes + sqrt(1-rho_g^2)*epsilon`.
- Map its normal CDF through the rate inverse CDF.
- Multiply the paired minutes/rate draws.

Using a Gaussian CDF here only generates copula uniforms. The final prop probability is an empirical sample frequency, not a Gaussian approximation to stat totals.

All stats share the same simulated minutes array. There is no separately fitted residual dependence among PTS/REB/AST rates conditional on minutes. Combo samples are sums at matching simulation indices: PRA=PTS+REB+AST, PR=PTS+REB, PA=PTS+AST, RA=REB+AST. This carries common-minutes dependence, not an unrestricted joint basketball outcome model. The chosen paper recommendation policy is restricted to base PTS/REB/AST.

With nonempty saved copula parameters, legacy post-hoc minutes-bucket rate factors are bypassed; their presence in a settings dump does not mean both corrections run. If copula is absent, a legacy independent-plus-rate-adjustment path exists and is not the desired reference path.

Sources: baseline `copula_params.json`; `src/models/monte_carlo.py:233-276,304-359,860-974`.

## 11. Raw probabilities, edge and Black-Litterman blending

The base probability is `p_over = mean(samples > line)`. It is not computed from a normal total-stat CDF. Under convention is path-sensitive: `PropPrediction` and raw harness complement use <=, whereas BL computes `mean(samples < line)`. On ordinary positive half-point lines, equality has no relevant sample mass; integer/zero lines need explicit consistency analysis. Historical settlement separately refunds actual equality as a push. The sampling/probability stack is not an explicit three-outcome win/push/loss model.

### Market prior

Convert paired over/under American prices to decimal payouts D_over, D_under. Raw implied probabilities are 1/D. Multiplicative devigging gives:

`p_market_over = (1/D_over) / ((1/D_over)+(1/D_under))`

and the equivalent normalized under probability. This describes the implemented normalization only; do not rely on comments claiming every devig method is equivalent.

### Confidence and blend

`z = abs(mean(samples)-line) / std(samples)`

`confidence = min(z/z_max, 1)`

Empty or effectively zero-variance samples give confidence zero.

`w = min(tau*confidence, max_weight)`

`logit(p_BL) = logit(p_market) + w*(logit(p_model)-logit(p_market))`

`p_BL = sigmoid(logit(p_BL))`

Input model/market probabilities are clipped to [.01,.99] before logits. The same confidence is used for the two sides. The implementation is a BL-inspired scalar log-odds shrinker, not a full asset-allocation covariance model or learned ensemble. Its confidence is a distance-from-line heuristic, not a calibrated probability that the model is correct, a statistical confidence interval or estimated epistemic uncertainty.

Higher tau increases model influence until the cap; lower z_max saturates confidence sooner; max_weight caps influence. BL modifies probabilities and therefore candidate ranks/which wagers survive, not just stake sizes. It does not retrain or repair the underlying minutes/rate distribution.

Synthetic illustration, not actual model output: raw .70, market .50, confidence .50, tau .75 and cap .75 yield w=.375 and posterior approximately .57877.

`edge = decision_probability - devigged_market_probability`.

Thus the 7% policy threshold means seven probability points AFTER BL. It is neither a 7% expected payout return nor a 7% raw edge. A separate payout-aware expectation would use both win/loss probabilities and net payout. Raw versus BL thresholds of the same numeric value do not select equivalent opportunity sets.

Sources: `src/models/black_litterman.py:82-108,110-208`; `src/backtesting/backtest_harness.py:791-912`; `src/models/monte_carlo.py:44-71`.

## 12. Bet selection is a separate layer

### Chosen forward-paper direction

Current source policy defaults, separate from learned model weights:

- Base PTS/REB/AST only.
- BL tau .75, z_max .50, max_weight .75.
- Minimum post-BL probability-point edge .07.
- Inclusive American odds bounds [-200,+200].
- $10 flat stake; 5,000 draws; seed42.
- Choose the maximum-edge direction per candidate; OVER wins exact directional ties.
- Choose one maximum-edge base-stat opportunity per player/game.
- THEN reject OVER. Do not replace a rejected OVER with the next-best UNDER/stat.
- Apply the remaining edge/price/eligibility/provenance gates.

This is `best_then_under`, not 'find the best qualifying UNDER from all options'. The ordering can deliberately leave a player with no bet even if a lower-ranked UNDER would qualify.

NBA's old Q50-versus-L5 sanity recommendation filters were removed from the current shared recommendation loop. A similarly named legacy function remains because other callers still use it. It should not be presented as an active gate in this chosen current NBA policy. Out-player and pregame safeguards are separate and remain relevant.

### Historical replay differences

The general harness shops over and under quotes independently by maximum edge, selects the winning side per player/game/stat, and then one stat per player/game. Threshold, allowed directions and odds bounds are applied afterward without substitution. The current daily reader first chooses a lowest-vig paired quote per player/game/market before shared policy ranking. These are different quote-routing surfaces; matching BL/direction settings alone does not certify identical live/replay routing.

Historical sweeps can evaluate both directions; they are not inherently under-only. Flat stakes override Kelly in the saved reference sweeps, but a flat wager can be partially funded by remaining bankroll. Generic simulator Kelly capability is not active Kelly in this reference.

Also distinguish raw display/harness probabilities from BL inputs: the all-raw historical branch clips empirical over probability to [.05,.90], daily raw display to [.05,.95], while BL recomputes raw frequencies and uses its own [.01,.99] logit bounds. Therefore 'raw' is not an identical probability transformation in every consumer.

Sources: `src/models/nba_paper_policy.py:17-124`; `src/models/daily_runner.py:837-1050`; `src/backtesting/backtest_harness.py:576-644,857-912`; `src/backtesting/bet_simulator.py:187-269,388-493`.

## 13. Baseline-specific saved results

The original sweep covers October 21-November 2, 2026, with wagers on 12 dates. Starting bankroll $2,000, nominal $10 flat stakes, 5,000 draws, seed42, minimum pregame L5 average minutes10. It contains 95 alternative cells, not 95 independent experiments.

Three exact bet ledgers were rechecked against saved hashes/row counts and independently aggregated during this dossier. Values below are not freshly run predictions:

| Reference policy | Bets | Profit | ROI |
|---|---:|---:|---:|
| Raw edge .05 | 972 | -$574.61 | -5.91% |
| BL edge .05, tau/z/cap .5/.5/.5 | 271 | +$113.00 | +4.17% |
| BL edge .07, tau/z/cap .75/.5/.75, both sides | 300 | +$122.65 | +4.09% |
| UNDER subset of preceding 300 selected wagers | 209 | +$208.28 | +9.97% |

The 209-UNDER subset is 123 wins/86 losses, 58.85% hit rate. It is a post-hoc restriction of a previously selected portfolio, not independent forward-paper evidence or a newly executed direction-first policy. Replayed book/eligibility filters still matter.

At raw .05, average reported selected-win probability is 60.81%, but actual hit rate is 48.97%. This is a selected-bet discrepancy over one short discovery window; it is not a whole-population calibration estimate or proof that one feature causes the problem.

The retained full review found every tested raw threshold losing. BL mostly avoided bad raw selections and also changed stat/side choice. The strongest apparent BL contributions came from unders and rebounds. Small-window results, grid selection, date concentration and unresolved game-line feature timing prevent a long-run profitability claim.

Sources: exact receipt-bound cells in companion snapshot; `docs/operations/nba-first-sweep-review-2026-11-08.md:12-81`; `docs/operations/nba-baseline-paper-policy-2026-11-12.md:11-32`.

## 14. What later experiments do and do not tell us

- Family removals and repaired rate tuning used the game-market-free control, not this original baseline. Removing player-prop inputs worsened common-cohort forecasts in those completed comparisons; this supports retaining that information in that tested setting, not a universal proof.
- The November 10 shared pinball rate retune held minutes, selected features and saved copula fixed relative to its control. Its forecast changes were small/mixed; raw ROI improved but remained negative, while fixed-BL portfolio results worsened. It is not a demonstration that pinball beats a matched calibration-objective search, because that objective-only experiment was not performed.
- The full error-stage/oracle diagnostic also used the game-market-free control. Actual-rate substitution helped much more than actual-minute substitution, but actual rates contain realized outcome noise. Do not attribute those exact diagnostics to this no-injury baseline or call the oracle gains learnable alpha.
- A direct-total REB side benchmark tested final rebounds without minutes multiplication. It showed modest mixed distribution-score gains, still trailed market Brier, and did not establish a betting upgrade in completed fixed/tune-only sweeps. It is a comparison, not the active architecture or permission to replace it. A separately owned reselection run was recorded as ongoing; this dossier did not inspect its latest artifacts/process state.

Sources: `docs/operations/nba-four-ablation-review-2026-11-08.md`; `nba-pinball-retune-sweep-diagnostic-review.md`; `nba-error-stage-v4-review.md`; `nba-direct-reb-benchmark.md`; `nba-direct-reb-fixed-vs-retuned-sweep-review-2026-11-12.md` in the same operations directory.

## 15. Questions for the separate improvement analysis

These are interpretation boundaries and testable questions, not approved implementation or claims of diagnosed causality:

1. Does conditional line-probability error, rather than average component coverage, explain the chosen baseline's selected-bet overconfidence?
2. How much is due to conditional minutes/rate prediction versus five-quantile interpolation, tail extrapolation and the fixed REB spread multiplier?
3. Is one pooled actual-minutes/actual-rate copula coefficient appropriate for conditional forecast uncertainty, given cross-player role differences? This is a hypothesis, not a finding that the copula is wrong.
4. Does broad per-head selection add noisy/redundant variables? Training-partition importance and the larger-prefix tolerance deserve separation from true family contribution.
5. Does tuning a component's average pinball improve final line probabilities? These are different objectives; a sound optimization procedure need not add market-relative signal.
6. Are apparent margins driven by market-input reuse (prop/game covariates plus BL's market anchor), legitimate incremental basketball signal, quote routing, or selection effects?
7. Would explicit count/push/zero treatment materially change relevant probability errors? The current continuous product is not a discrete count model.
8. What survives a fixed later untouched window and forward-paper observation, rather than additional selection on October21-November2?

Keep distribution quality, market-relative probability quality, bet selection, and sizing separate in that analysis. Changing BL or banning a side can alter observed P&L without improving the learned forecast. No evidence here authorizes offsets, extra tuning budgets, new sources, a production switch or automatic betting.

## 16. Relevant prior lessons and invariants

- Preserve empirical sample probabilities; a Gaussian latent copula does not authorize Gaussian total-stat probabilities.
- Do not deploy global conformal offsets or blindly correct NBA Q10 behavior. Component embedded offsets, diagnostic combined offsets and BL are distinct mechanisms.
- Feature importance/selection is not a family ablation, especially for correlated features.
- Use cheap controlled comparisons before architectural escalation.
- Separate fitting, calibration, discovery, untouched confirmation and paper evidence.
- Exact artifacts/settings outrank old prose or generic defaults.

Canonical context consulted: `operations/hard-facts`, `operations/critical-invariants`, `models/nba-model`, `decisions/model-architecture-decisions`, `decisions/calibration-decisions`, lesson-tag inventory and relevant atomic lessons, `handoffs/handoff-154`. Newer actual application source/metadata resolves stale recommendations in those pages.

## 17. Companion data and verification scope

Companion JSON: `docs/operations/evidence/nba-no-injury-baseline-model-snapshot-2026-11-12.json`.

It contains the primary manifest, exact configurations, all ordered per-quantile feature lists, source/identity boundaries, reference predictor settings, calibration reports, selected ledger aggregates and comparison metadata. It can be handed to a separate analyst without reconstructing the artifact identity.

This is a source/metadata/saved-evidence dossier, not a fresh performance certification. No training, new replay, SQL query, artifact promotion or deployment occurred. Actual embedded baseline component offsets and estimator objects were not deserialized. Diagnostic and current-source behavior must not be mistaken for a new evaluation of historical source truth.

## Appendix A. Exact selected-feature counts and membership

| Component | Q10 | Q25 | Q50 | Q75 | Q90 | Union |
|---|---:|---:|---:|---:|---:|---:|
| minutes_features | 47 | 50 | 48 | 50 | 49 | 57 |
| pts_rate_features | 52 | 56 | 47 | 50 | 55 | 58 |
| reb_rate_features | 45 | 52 | 55 | 47 | 52 | 57 |
| ast_rate_features | 14 | 40 | 52 | 51 | 51 | 55 |

Overall union: 58 distinct names. Component union membership below does not imply membership in every quantile. Exact order and per-quantile inclusion are in the JSON snapshot.

| Feature | Minutes | PTS rate | REB rate | AST rate |
|---|:---:|:---:|:---:|:---:|
| `games_in_last_7_days` | yes | yes | yes | - |
| `is_home` | - | yes | - | - |
| `line_spread` | yes | yes | yes | yes |
| `line_total` | yes | yes | yes | yes |
| `opp_avg_def_rtg_l5` | yes | yes | yes | yes |
| `opp_avg_fg3_pct_l5` | yes | yes | yes | yes |
| `opp_avg_fg3a_l5` | yes | yes | yes | yes |
| `opp_avg_pace_l5` | yes | yes | yes | yes |
| `opp_pos_ast_allowed_l15` | yes | yes | yes | yes |
| `opp_pos_ast_allowed_l5` | yes | yes | yes | yes |
| `opp_pos_ast_per100_allowed_l5` | yes | yes | yes | yes |
| `opp_pos_off_rtg_allowed_l15` | yes | yes | yes | yes |
| `opp_pos_off_rtg_allowed_l5` | yes | yes | yes | yes |
| `opp_pos_reb_allowed_l15` | yes | yes | yes | yes |
| `opp_pos_reb_allowed_l5` | yes | yes | yes | yes |
| `opp_pos_reb_per100_allowed_l5` | yes | yes | yes | yes |
| `opp_pos_threes_allowed_l15` | yes | yes | yes | yes |
| `opp_pos_threes_allowed_l5` | yes | yes | yes | - |
| `opp_pos_threes_per100_allowed_l5` | yes | yes | yes | yes |
| `player_ast_l3_l15_ratio` | yes | yes | yes | yes |
| `player_avg_ast_l3` | yes | yes | yes | yes |
| `player_avg_ast_l5` | yes | yes | yes | yes |
| `player_avg_ast_pct_l5` | yes | yes | yes | yes |
| `player_avg_fg3a_l5` | yes | yes | yes | yes |
| `player_avg_fg3m_l3` | yes | yes | yes | yes |
| `player_avg_fg3m_l5` | yes | yes | yes | yes |
| `player_avg_min_l15` | yes | yes | yes | yes |
| `player_avg_min_l3` | yes | yes | yes | yes |
| `player_avg_min_l5` | yes | yes | yes | yes |
| `player_avg_min_szn` | yes | yes | yes | yes |
| `player_avg_pts_l15` | yes | yes | yes | yes |
| `player_avg_pts_l3` | yes | yes | yes | yes |
| `player_avg_pts_l5` | yes | yes | yes | yes |
| `player_avg_reb_l3` | yes | yes | yes | yes |
| `player_avg_reb_l5` | yes | yes | yes | yes |
| `player_avg_reb_pct_l5` | yes | yes | yes | yes |
| `player_avg_ts_pct_l15` | yes | yes | yes | yes |
| `player_avg_usg_pct_l5` | yes | yes | yes | yes |
| `player_fg3m_l3_l15_ratio` | yes | yes | yes | yes |
| `player_games_started_l5` | yes | yes | yes | yes |
| `player_min_floor_l5` | yes | yes | yes | yes |
| `player_min_l3_l15_ratio` | yes | yes | yes | yes |
| `player_min_l3_l5_ratio` | yes | yes | yes | yes |
| `player_min_std_l5` | yes | yes | yes | yes |
| `player_pts_l3_l15_ratio` | yes | yes | yes | yes |
| `player_reb_l3_l15_ratio` | yes | yes | yes | yes |
| `player_std_ast_l5` | yes | yes | yes | yes |
| `player_std_fg3m_l5` | yes | yes | yes | yes |
| `player_std_pts_l5` | yes | yes | yes | yes |
| `player_std_reb_l5` | yes | yes | yes | yes |
| `prop_line_ast` | yes | yes | yes | yes |
| `prop_line_pts` | yes | yes | yes | yes |
| `prop_line_reb` | yes | yes | yes | yes |
| `prop_line_threes` | yes | yes | yes | yes |
| `rest_days` | yes | yes | yes | yes |
| `team_avg_fg3_pct_l5` | yes | yes | yes | yes |
| `team_avg_fg3a_l5` | yes | yes | yes | yes |
| `team_avg_pace_l5` | yes | yes | yes | yes |

Selected counts within registered ablation families: game_market 2/2, prop_market 4/4, opponent_matchup 14/14, short_form 12/12, minutes_role 7/8, schedule 2/3. These six families do not partition every basketball feature; other core history/context remains.

## Appendix B. Baseline hyperparameter table

| Component | n_estimators | Depth | Learning rate | Row sample | Column sample | Min child weight | Patience |
|---|---:|---:|---:|---:|---:|---:|---:|
| minutes | 253 | 4 | 0.014336082 | 0.655331241 | 0.676446037 | 3 | 45 |
| pts | 292 | 3 | 0.022249840 | 0.905484568 | 0.930893346 | 5 | 55 |
| reb | 1319 | 3 | 0.010072985 | 0.613841394 | 0.635213484 | 5 | 33 |
| ast | 312 | 3 | 0.017592374 | 0.661329158 | 0.699792244 | 16 | 94 |

Displayed floats are rounded; companion JSON preserves the saved values. Every component uses seed42 and validation fraction.15.
