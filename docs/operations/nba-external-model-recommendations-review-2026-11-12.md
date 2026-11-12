# NBA external modeling recommendations — assessment

## Decision status

November 12, 2026. Assessment of Chase's supplied 198-line review of the original no-injury baseline. Most recommendations are defensible experiments; they are not established causes of losses or an approved implementation queue. No training, replay, DB operation, policy change or promotion was performed. Current source spot-checks and existing review documents were read; model/result artifacts were not independently reverified in this assessment.

Reference: `no_injury_baseline/nba_run_20261108_122129`. The [baseline dossier](../understanding/nba-no-injury-baseline-model-dossier-2026-11-12.md) describes this exact artifact. Direct-REB and detailed REB opportunity diagnostics instead use the game-market-free control; do not transfer their exact metrics to the original baseline.

## Strong recommendations to retain

### 1. Audit outcome-population coverage before changing its policy

Source confirms acquisition `pgs.min >= 5` (`src/models/features/nba/training_loader.py:557-566`) and combined calibration `actual_minutes >= 10` (`src/models/train_pipeline.py:1096-1105`). Rate selection/tuning/calibration also use the ten-minute restriction; minutes uses the acquired population. These differ from the eight-minute rolling-value policy and pregame replay eligibility.

Measure settled quoted outcomes excluded by each restriction, including early exits. Report full eligible and low-minute subgroup scores separately. The combined calibration report is not evidence for all settled outcomes. Rate-target stabilization may justify a different fitting population, but does not justify silently evaluating only that population. Do not automatically remove every threshold, assume every early exit settles under every venue's rules, or claim this proves the cause/direction of model error. Current replay exclusion of all low-minute outcomes has not been established by this audit.

### 2. Align decision probabilities and execution semantics

The dossier sections 11–12 record different raw clipping, under/equality treatment and book routing across replay/daily paths. Preserve immutable quotes, coherent over/under prices, explicit pushes and `best_then_under` ordering. Payout EV and devigged probability-point edge are different quantities. The supplied -110 example is numerically correct: 52% win probability yields about -0.7273% expected return with no pushes.

This is a contract/verification recommendation, not authorization for a broad harness rewrite or selection-policy change. A shared function alone is not proof of real replay/live parity.

### 3. Test a small learned market shrinker as a challenger

The current BL-inspired implementation already blends in log-odds space. The external formula primarily replaces distance-from-line confidence with a learned coefficient, not a new blending algebra. Compare market-only, raw, current fixed BL and one regularized learned blend using chronological out-of-sample predictions at identical lines and information times. Fit and choose the blend before the final evaluation block.

A near-zero learned weight means limited incremental value under the tested model/data/form, not proof that basketball features contain no signal. Better Brier/log loss is not by itself a betting improvement. Market shrinkage is distinct from changing the underlying minutes/rate forecast and from forbidden global conformal offsets.

#### Follow-up: one learned weight per stat

Chase asked about learning BL parameters; the following is a recommended bounded challenger, not implementation/run/policy approval. Keep the exact no-injury baseline, features, component models, copula and sample settings frozen.

- Fit `logit(p_final) = logit(p_market) + w_stat * (logit(p_model) - logit(p_market))`, with `w_stat` bounded to [0,1], no intercept and no extra reliability features initially. This learns market-relative shrinkage, not new basketball signal or guaranteed calibration. A bounded scalar optimizer per stat is sufficient; explicitly evaluate the endpoints 0 and 1. Optuna/neural machinery is unnecessary for the first comparison.
- Predeclare opportunity-weighted log loss as the fitting objective; report Brier, reliability curves and later policy results separately. Proper scores combine calibration and discrimination, so lower score alone must not be called improved calibration or profitability.
- Fit on chronological base-model-out-of-sample forecasts; predictions used to fit this layer are its training data. Base feature selection, tuning and any learned calibration must respect prediction-time provenance. A fixed earlier-trained base artifact can supply later historical forecasts; retraining every date is not inherently necessary. October 21-November 2, 2026 remains discovery data, not untouched evidence for choosing the blend.
- Use coherent same-line/time paired prices and all eligible opportunities, not only old selected bets. Prefer one predeclared reference quote per player/game/stat; if retaining several books/times/lines, define weighting so repeated observations of the same underlying outcome do not dominate. For an initial binary comparison use positive half-point lines; handle integer pushes and voids explicitly rather than silently labeling them losses. Do not restrict scoring by realized minutes merely because rate training used a ten-minute threshold.
- Compare market-only, unblended empirical probabilities, current fixed BL and learned per-stat weights on matched later observations. Use game/date-aware uncertainty and inspect weight stability. Preserve the current 7% post-blend threshold, best-then-under ordering, quote routing and flat stake in the first downstream comparison; changing them is a separate intervention. Market-only may correctly yield no bets under this threshold.
- The existing positive-parameter BL curve is `w(z)=min((tau/z_max)*z, tau, max_weight)`, so it has a slope and effective cap `min(tau,max_weight)`: the three knobs are not independently identifiable. Constant weights first test whether the distance-from-line confidence rule contributes. A later learned slope/cap would be a separate challenger, not an automatic expansion.
- The supplied numerical illustration is correct: model .65, market .52 and weight .20 produce .5468229 (about 54.68%), only 2.68 probability points above market. It would not clear the present 7-point gate. A learned shrinker reducing bad edges or finding near-zero weight is information, not a reason to lower thresholds on the test block.

Method references checked: scikit-learn probability-calibration guidance (disjoint fitting data and proper-score interpretation), SciPy bounded scalar minimization, and Cawley/Talbot on model-selection overfitting. No probability layer was fitted or installed by this follow-up.

### 4. Test sampler assumptions without changing boosters

Pooled actual-minutes/rate correlation is not conditional forecast-error dependence. Independence/current/residual-dependence comparisons are reasonable hypotheses, not evidence that the current copula is defective. The existing saved REB coefficient is already near zero, so eliminating that correlation alone is not an attractive explanation for REB errors. An independent comparator must also bypass the legacy no-copula rate adjustment; removing a file is not sufficient.

Inspect crossing frequency and materiality, interpolation/tail/spread effects and seed sensitivity one at a time. Monotone rearrangement is defensible, but cumulative-maximum repair is already implemented. At 5,000 iid draws and p=.5, the binomial Monte Carlo standard error is about 0.7071 percentage points; this is numerical error, not total model uncertainty.

### 5. Choose one opportunity/reliability family from conditional errors

Opportunity, role and exposure/reliability inputs are plausible candidates. NBA tracking provides potential assists and rebound chances, but public availability does not establish retained historical coverage, decision-time validity or incremental predictive value. Many suggested workload/usage/market proxies already exist. Audit available columns and non-default training coverage before acquiring sources or adding a bundle.

The [REB opportunity view](nba-reb-opportunity-view.md) and its concentrated analysis are complete, not pending. They expose recurring low-line-over probability errors in the game-market-free control, without identifying a causal role/injury feature. Use that evidence to select a specific input/modeling hypothesis rather than rebuilding diagnostics.

## Valid but lower priority, conditional or already partly completed

- **Training recency:** a recent-data challenger is reasonable, but the existing two fitting seasons / separate calibration season were deliberate owner policy, not an accidental stale-model bug. Move fitting/calibration/evaluation boundaries together. Change recency and population in separate comparisons before combining them, so their effects are attributable.
- **Direct count distribution:** a negative-binomial challenger remains a new experiment. Completed direct REB work fit five direct quantile heads, not a count likelihood. Its mixed forecast gains and unsuccessful betting sweeps neither certify nor disprove a count model. Discreteness supplies zero/push mass but does not guarantee accurate probabilities. XGBoostLSS supports count distributions; compatibility with this project's environment and consumers has not been tested here.
- **Feature selection:** training-partition permutation ranking and non-fold-local selection deserve attention. However, 58 is the baseline's selected-feature union, not the verified full candidate pool. A simpler shared feature set is an experiment, not an automatic improvement. Validate whole correlated families rather than interpreting selector pruning as an ablation.
- **Tuning:** current NBA callers already select pinball; shared-mode regularization/early-stopping handoff is repaired and a completed challenger reviewed. Do not repeat those repairs. The dossier separately records a nested per-quantile config conversion issue; avoid/fix that optional path before using it. Broader search ranges are proposals, not a reason to increase every study budget.
- **Neural models and opportunity simulators:** shelf as future comparisons. They are not the best next default response to these findings. Keep cheap controlled alternatives first.

## Corrections to the suggested experiment sequence

### Current status — remaining development focus is objectives 3 and 4

Chase has directed attention to the remaining sampler and feature-family objectives after the low-minute investigation and learned-blend build. This updates work ordering, not permission to implement or launch either remaining experiment.

- **1 — Investigation complete.** Read [the low-minute investigation](nba-low-minute-exclusion-investigation-2026-11-12.md). Keep numerical thresholds unchanged on present evidence; the exact scored output contains no `<5` outcomes, and its `5–<10` subset is small. Shared rate-eligibility consolidation and explicit cohort reporting are proposed follow-ups, not evidence that lowering a threshold helps or that every code follow-up is implemented. Do not repeat the audit by default.
- **2 — Tooling complete; empirical comparison remains separate.** [Learned market blending](nba-learned-market-blend.md) is built and engineering-verified. The intended original-baseline fit/later-window comparison has not been established by that delivery. Keep it as an evaluation gate, not an obstacle to developing objective 3 and not proof of improvement.
- **3 — Bounded runner delivered; owner experiment pending.** [Sampler comparison](nba-sampler-comparison.md) is integrated uncommitted and engineering-verified. It freezes component forecasts/source inputs and compares control, REB widening disabled, and the extra lower-tail multiplier disabled across paired seeds; raw and fixed ordinary BL are scored without refitting. Existing models/defaults remain unchanged. Main-checkout validation passed 92 guarded existing tests, Ruff, Pyright, populated public-CLI acceptance and the real-artifact launch dry-run. The full historical experiment has not run and no improvement is established. Alternative interpolation remains deferred; a denser learned quantile grid would require new heads/retraining outside this frozen-model comparison.
- **4 — Next signal-development slice.** Use the completed conditional-error analysis plus objective 3 results to select one specific opportunity/reliability hypothesis. Check existing features and historical/as-of variation before acquiring data or fitting a small controlled family. Do not treat already-present workload proxies as new features or add a large bundle by default.

### Ranked shortlist requested by Chase

Original shortlist below; current completion/ordering is recorded above. No new implementation or run authorization is implied.

1. **Audit low-minute settled-outcome coverage.** Reconcile acquisition, rate fitting, calibration reporting and replay eligibility against the quoted/settled population. Quantify exclusions and subgroup error before changing any threshold. Deliverable: whether the current reports omit a material failure population and the smallest justified correction.
2. **Benchmark one learned market-blending weight per stat.** Reuse chronological out-of-sample forecasts and compare market-only, raw, current fixed BL and a regularized learned blend at identical lines. Deliverable: whether model disagreement adds useful information beyond the market; evaluate untouched probability scores and flat-stake outcomes, not just fitted calibration.
3. **Isolate probability-construction effects with frozen component forecasts.** Examine five-quantile interpolation, tail extrapolation and REB spread widening one at a time; measure seed sensitivity. Treat conditional dependence separately, especially because the REB coefficient is already near zero. Deliverable: whether probability error can improve without refitting boosters.
4. **Test one error-guided opportunity/reliability feature family.** Use the completed REB conditional-error view to select a concrete pregame hypothesis, check existing fields and historical non-default/as-of coverage, then compare a small family with fixed model settings. Deliverable: incremental underlying forecast and line-probability improvement, not merely a change in which bets are selected.

Proceed to item 3, then item 4 for the remaining development work. Items 2 and 3 address how forecasts become decisions; item 4 targets learned basketball signal. Keep direct-count and neural architectures outside this shortlist and do not combine interventions in the first comparison.

Do not launch the full proposed menu or combine population correction, newer seasons, new likelihood and learned calibration in the first refit. Reuse the completed population investigation and pick one intervention based on which failure it can distinguish. Saved-forecast sampler/blend work can be separated from improving the learned component inputs.

October 21–November 2 remains development data. Chronological monthly/four-week blocks, game/date-clustered uncertainty and equal opportunity weighting are sound design suggestions, but block duration is not a universal sufficiency threshold. Out-of-sample predictions used to fit a calibrator are its training data, not its untouched test. Do not duplicate one outcome's weight across books/snapshots accidentally.

## Relevant prior lessons/invariants

Read `operations/hard-facts`, `operations/critical-invariants`, `decisions/model-architecture-decisions`, the lesson-tag inventory and relevant calibration/feature/architecture lessons. No global conformal offsets, no blind Q10 repair, empirical probabilities for the existing Monte Carlo path, family-level validation and cheap baselines before architecture changes remain in force. Historical calibration failures do not prove all learned probability calibration is invalid; a new challenger still needs its own downstream evidence. No settled owner policy is changed by this review.

## Evidence

- Baseline dossier sections 3, 4, 5, 7, 9–15: population, selected versus candidate features, optional tuner issue, sampler, BL and consumer drift.
- `nba-direct-reb-benchmark.md:9-23`: direct quantile model, matched predictive results and realized-minute evaluation distinction.
- `nba-direct-reb-fixed-vs-retuned-sweep-review-2026-11-12.md:9-35`: completed fixed/tune-only betting comparisons; neither is a demonstrated upgrade.
- `nba-reb-opportunity-view.md:48-56`: completed concentrated conditional-error analysis, not pending diagnostic construction.
- scikit-learn, Permutation feature importance: held-out scoring and correlated-feature limitations.
- Kull et al. (2017), Beta calibration, PMLR 54: calibration-map assumptions and small-sample limitations.
- Patton (2006), Modelling Asymmetric Exchange Rate Dependence: conditional copula framework.
- Chernozhukov et al. (2010), Quantile and Probability Curves Without Crossing, arXiv:0704.3649: monotone rearrangement.
- Cawley and Talbot (2010), On Over-fitting in Model Selection and Subsequent Selection Bias in Performance Evaluation, JMLR 11:2079–2107.
- Official XGBoostLSS distribution/API documentation; NBA Stats glossary. These validate method/data definitions, not GameFlow performance.
