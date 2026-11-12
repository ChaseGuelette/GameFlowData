# NBA predictive-quality direction — November 12, 2026

## Owner direction versus recommendations

Owner direction: improve baseline predictive quality, rather than repeat broad hyperparameter searches or merely confirm the existing weak model. Chase considers different models with similar predictive quality a likely explanation for the repaired pinball retune's modest results; this remains an interpretation, not a diagnosed error mechanism.

Chase subsequently approved implementing the isolated one-stat direct-total benchmark and receiving the command to launch it, then returning attention to the existing minutes-times-rate architecture. The [implementation plan](../../.hermes/plans/nba-direct-reb-benchmark-2026-11-12.md) owns its exact scope and validation. Agents implement and dry-run; Chase launches the full fit/evaluation. Architecture replacement, new source acquisition, production changes and promotion remain unapproved. Other recommendations below remain proposals.

## Established checkpoint

The [completed matched retune review](nba-pinball-retune-sweep-diagnostic-review.md) records small mixed rate-score changes, changed probabilities/wager sets, and no whole-suite upgrade. The original game-market-free control remains the methodological reference; the November 10 repaired candidate remains exploratory. Minutes, selected features and copula were held fixed. The candidate is not a fresh matched calibration-objective-versus-pinball-objective search.

The [v4 diagnosis](nba-error-stage-v4-review.md) shows rate-oracle sensitivity, but actual rate includes outcome noise. That sensitivity does not establish how much error can be learned away. October 21–November 2, 2026 is already-used discovery data. Historical source verification remains separate and unverified.

## Previously documented unfinished ideas

From [the family plan](nba-feature-family-ablation-direction-2026-11-08.md):

- Combined `short_form` + `opponent_matchup` exclusion: optional interaction/simplification experiment, not established new signal.
- Conditional `minutes_role` family ablation, preserving L5/L15 minutes anchors.
- Conditional `schedule` family ablation.
- Targeted forced inclusion after a concrete feature hypothesis; no specific new feature addition was selected.
- Later untouched validation of a frozen challenger: confidence/promotion evidence, not a mechanism for improving predictions.

The [previous-day injury proposal](nba-rapidapi-clock-validation-2026-11-08.md) remains a separate shelved option requiring source-date/coverage acceptance. Its potential forecast benefit is a hypothesis. Missing reports must remain unknown. No blanket timestamp mapping or broad rescrape is authorized.

The original family plan explicitly shelved new architecture and external sources. The benchmark below is a new, now owner-approved side experiment, not an authorization to replace the active architecture or acquire external sources.

## Recommended order for discussion

### 1. Small direct-total benchmark, not a pipeline replacement

Question: does predicting a final stat directly improve forecast quality relative to separately learning minutes and per-minute rates and composing them?

Proposed pilot: one stat, initially rebounds, using the existing quantile-model family against final rebound count. Rebounds are a tractable first pilot with a modest existing matched-score lead; selecting this stat is discovery, not independent evidence that it will win.

Retain current decomposition as the control and production architecture. Freeze fitting chronology, eligible evaluation cohort, available pregame information and evaluation settings. A direct target needs an explicit input contract; do not silently reuse a rate-only selected subset and then claim a definitive architecture comparison. Never use realized minutes or future outcomes as input features. Compare final-stat quantile loss first, then empirical over/under probability scores on common quotes; validate the direct model's sampling interface before replay. No expensive tuner grid or new model family is the initial proposal.

Implementation is integrated into main, uncommitted; independent review and offline verification passed. Chase completed the first real benchmark on November 12; [the runbook](nba-direct-reb-benchmark.md) records verified modest mixed forecast gains, not a clear winner. Chase approved a separate rebounds-only sweep and post-sweep analysis without retraining. The existing minutes-times-rate architecture remains the main development lane. A single fixed configuration cannot prove one architecture universally superior.

### 2. Error-guided input improvement

If retaining the current model is preferred, use existing diagnostic exports for a bounded error-group comparison to choose ONE pregame feature hypothesis. Candidate questions concern role stability, recent role changes and teammate availability, but their current representation and source variation must be checked before calling them missing features. Error concentration alone does not prove predictable signal.

The deliverable should be one learning intervention with a stated mechanism, not another broad diagnostic framework. Injury context is an existing proposal but has source acceptance overhead; do not reopen it automatically.

### 3. Optional cheap combination check

If the two existing artifacts make complementary mistakes, a predeclared equal-weight forecast combination is a possible low-cost experiment. This is a new proposal, not an approved ensemble implementation. First confirm saved outputs contain the necessary aligned predictions; averaging quantiles and mixing full sample distributions are different constructions. No weights should be optimized on the reused discovery window. Do not promise improvement over the better constituent.

## What not to prioritize

- Another full-suite trial-budget increase or objective bake-off without a specific unanswered mechanism.
- Exhausting remaining family-removal combinations solely because the old plan lists them.
- Betting-filter or BL changes presented as baseline predictive improvements.
- New architecture production migration before a cheap benchmark.
- Waiting for more test dates as the sole model-improvement strategy.

## Evaluation contract and prior lessons

For forecast development, compare final-stat pinball and raw probability quality on matched opportunities, with per-stat/per-quantile and relevant cohort breakdowns. Keep component scores diagnostic and fixed flat-stake results as downstream evidence. Later untouched confirmation and forward paper gates remain separate; a forecast improvement is not automatic promotion.

Apply `lessons/cheap-baseline-before-architecture`, `lessons/feature-selector-is-not-an-ablation`, and `operations/critical-invariants`: retain empirical sample probabilities, no global conformal offsets or blind Q10 correction, decision-time-safe inputs and owner-launched long jobs. No experiment was launched by this discussion.
