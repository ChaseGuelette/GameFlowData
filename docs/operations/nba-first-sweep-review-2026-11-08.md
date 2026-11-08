# First NBA no-injury sweep review — November 8, 2026

## Kick off immediately

The model is an executable exploratory baseline, but raw betting selection is unprofitable in this window. A small subtractive feature-family wave is the next useful training experiment; a larger BL search, broad retune or architecture replacement is not supported yet.

- This chat: completed saved-output analysis and source interpretation. Next proposed coding slice: narrow full-training family controls and lifecycle forwarding from the existing [ablation proposal](nba-feature-family-ablation-direction-2026-11-08.md). Implementation is not authorized by this report.
- You: approve the comparison scope and launch subsequent long training/replay jobs. Retain the original baseline and sweep unchanged.
- Other chat: no additional writer required. Preserve injury, Railway and existing sweep-console work.
- Read first: verdict/results here, next-runs section, then the family-control proposal. No new broad readiness audit or seasonal-sampling gate is proposed.

## Scope and verification

Source sweep: `backtest_results/nba_no_injury_holdout_sweep_95_nov2_v1`.
Exact model: `src/models/artifacts/no_injury_baseline/nba_run_20261108_122129`.

Requested October 21–November 2, 2026; bets exist on October 22–November 2, 12 dates. October 21 had 215 feature rows and zero players passing the current eligibility filter; it was not a no-games day. Fit populations end April 13, 2025 and calibration ends June 13, 2026, so this replay follows the recorded fit/calibration populations. The manifest still explicitly reports `historical_readiness=unverified` and `decision_grade=false`.

Fixed settings: $2,000 starting bankroll, nominal $10 flat stakes, 5,000 Monte Carlo samples, seed 42, minimum average minutes 10. Grid: edges .03/.05/.07/.10/.12; BL tau .25/.5/.75, z_max .5/.75/1, max_weight .5/.75, plus raw controls. No Kelly sizing or global combined-calibration offsets.

All 95 summary rows, 95 completed manifest entries and 95 cell directories agree. All 95 saved bet-file hashes matched receipts; counts, stakes and P&L reconciled to summary rows. Outcomes/payoffs also reproduced from the saved actual/line/side/price values. No checked quote timestamp followed the saved selected decision time, and no checked timing fields were missing. Recorded model-file hashes and sweep-summary receipt matched. These are saved-output consistency checks, not an independent DB/source audit or verification of historical feature chronology.

There are 53 distinct selected-bet portfolios, not 95 independent experiments. Thirty cells are profitable; 44 have at least 100 total bets, of which 26 are profitable. After deduplicating selected portfolios, 29 have at least 100 bets and 15 of those are profitable. Do not add P&L across configurations or treat repeated winners as independent confirmation.

Numeric evidence: [all-cell CSV](evidence/nba-first-sweep-2026-11-08-cells.csv) and [full analysis receipt](evidence/nba-first-sweep-2026-11-08-analysis.json).

## Results

No raw/no-BL threshold is profitable. Higher nominal edges did not rescue this model's raw selection.

| Raw edge | Bets | Profit | ROI | Max drawdown |
|---|---:|---:|---:|---:|
| 3% | 1,048 | -$469.79 | -4.48% | 37.26% |
| 5% | 972 | -$574.61 | -5.91% | 41.22% |
| 7% | 836 | -$512.29 | -6.13% | 33.40% |
| 10% | 603 | -$378.40 | -6.28% | 24.27% |
| 12% | 423 | -$306.99 | -7.26% | 22.32% |

The best ROI cell among configurations with at least 100 total bets is edge .05 / tau .5 / z_max .5 / max_weight .5: **271 bets, +$113.00, +4.17% ROI, 3.22% drawdown, saved annualized daily Sharpe 4.43**. Equivalent settings produce the same portfolio. Highest profit is edge .07 / tau .75 / z_max .5 / max_weight .75: 300 bets, +$122.65, +4.09% ROI, 4.55% drawdown. The headline maximum ROI, +6.62%, has only 53 bets; it is not the useful selection target.

These are promising discovery cells, not certified policies. A deterministic 5,000-resample date-block bootstrap of the 271-bet cell gives a conditional percentile ROI interval of approximately -6.61% to +14.49%. It does not adjust for choosing the winner from the grid and has only 12 observed date blocks. Removing the two highest-profit dates leaves 177 bets and -$17.91 (-1.01% ROI), a concentration diagnostic rather than an unbiased alternative estimate. Do not extrapolate the short-window annualized Sharpe.

### What carries the BL result

The profitable cell is driven by unders, particularly rebounds.

| Stat/side | Bets | Profit | ROI |
|---|---:|---:|---:|
| PTS under | 71 | +$63.25 | +8.91% |
| PTS over | 16 | -$25.70 | -16.06% |
| REB under | 75 | +$120.74 | +16.10% |
| REB over | 54 | -$30.56 | -5.66% |
| AST under | 43 | +$15.23 | +3.54% |
| AST over | 12 | -$29.97 | -24.97% |

All unders: 189 bets, +$199.23, +10.54% ROI. All overs: 82 bets, -$86.23, -10.52% ROI. These are post-hoc slices of selected bets, not a newly executed under-only policy that can choose alternative bets. REB is the strongest lead; PTS is mixed; AST aggregate profit is negative in all 44 configurations with at least 100 **total portfolio** bets. That is not a claim of 100 AST bets in every cell, nor 44 independent tests.

The two executable books actually represented are DraftKings and Pinnacle, despite the longer permitted-book list. In the 271-bet cell, DraftKings contributes 232 bets/+2.22% ROI and Pinnacle 39 bets/+15.78% ROI. Do not extrapolate all-book routing or treat the smaller Pinnacle slice as a validated venue policy.

### Why raw probability quality is the concern

At raw edge .05, average stated win probability is 60.81%, but observed hit rate is 48.97%: an 11.84-percentage-point gap on the selected bets. This is selection-conditional overconfidence in this short window, not a whole-population calibration estimate or proof that one feature family caused it.

Compared with raw .05, the BL candidate retains 259 identical bets (+$67.37), avoids 713 raw-only bets (-$641.98), and selects 12 new bets (+$45.63). BL is largely avoiding bad raw selections here. It also changes stat/side ranking; this is not merely resizing the same bets. Raw and BL edge thresholds are not semantically identical.

## Initial training interpretation

Training completion and a single-game sanity pass establish functional artifact creation, not betting quality. The provided sanity example is in the calibration season, not this new-season holdout.

Component calibration is reasonably close to its nominal levels: worst absolute gap is minutes 3.97 pp, PTS rate 2.98 pp, REB rate 1.89 pp, AST rate 1.40 pp. But good component coverage does not guarantee useful probabilities for selected sportsbook wagers.

Combined calibration is a sampled end-to-end diagnostic, not the full-season component report. The current implementation filters to at least 10 actual minutes and complete actuals across evaluated stats, caps at 2,000 rows, uses 1,000 draws and skips rows whose prediction fails. Saved combined PTS Q25 coverage is 19.8% rather than 25%; REB Q25 is 19.6%; AST Q10 is 17.6% rather than 10%. The report does not itself preserve success/failure counts, so the exact accepted sample size was not independently established here. These findings are diagnostics, not permission to apply the accompanying proposed global offsets.

The logged Pearson minutes/rate correlations (PTS about .308, AST about .173) do **not** show that the sweep sampled independently. Those warnings come from calibration data; fitted copula parameters use training-data Spearman correlation. The recorded sweep passes PTS .3356, REB -.0029 and AST .2128, and the predictor dispatches to its copula path when that nonempty parameter dictionary is supplied. Global combined offsets are explicitly null. The warning therefore does not justify a new copula, survival model or decomposition rewrite.

## Interpretation boundaries

- `predictions.csv` is the selected-stat population, not every generated prediction. Source selection keeps one highest-edge stat/side per player-game. Both raw and BL files contain 1,119 player-games, but only 873 identical player/game/stat keys overlap; shared-key quantiles and actuals are unchanged. Small cell-to-cell differences in reported quantile calibration reflect selected-stat composition, not BL retraining or repairing the distribution.
- The run generated 11,053 stat/sample bundles, including combinations. There were 2,932 quoted base-stat opportunities and no combo quotes. These collapse to 1,119 player-games after selection; no combo betting performance was tested. Quoted base-stat coverage is 61.90% of generated base-stat rows, not whole-source completeness. Keep missing-quote and opening-date exclusions visible in comparisons.
- Flat stakes are capped by remaining cash. Raw .03 and .05 each contain one partially funded bet ($7.93 and $9.74 respectively); this is not active Kelly or a percentage cap. Preserve the same capital/ordering rules across candidate comparisons.
- Historical game spread/total as-of semantics remain the concrete feature-source caveat documented in the ablation proposal. General manifest unknowns do not individually prove a new loader failure; do not restart completed harness/props work from those labels.
- Current sweep CLI/runner hashes differ from the executed receipt. Their scoped live Git diff is console/progress logging and safe acquisition-error detail; saved inputs/results remain the analysis authority. This review did not alter those files.

## Recommended next training runs

Run a small fixed-parameter subtractive wave, with a common reference and no tuning. The observed weakness motivates testing redundancy/noise and market dependence, but does not establish which family is responsible.

1. **C — game-line-free control:** same no-injury baseline recipe, excluding `line_spread` and `line_total` across all affected components. Retain the finished model as the original reference. Until that source timing is verified, use C for the following family comparisons.
2. **C minus short_form (12 members):** remove stat L3 means, momentum ratios and L5 stat volatility; preserve slower player history and minutes-role features. This is a hypothesis about noisy recent-form signals, not a diagnosis inferred from the sweep.
3. **C minus opponent_matchup (14 members):** remove the exact position/defense/shooting context family from the proposal; keep pace separate. Assess whether AST/PTS improve without sacrificing REB.
4. **C minus prop_market (4 members):** remove all `prop_line_pts/reb/ast/threes` covariates from every affected minutes/rate component. Keep actual sportsbook odds for replay. This answers market-input dependence; it does not remove market information from the execution-price evaluation.

The sequence refines the earlier discussion: short-form/matchup subtraction is prioritized for next-run discovery, while market exclusion remains a high-information comparison. It is a recommendation, not owner approval or a claim that any family is harmful. Minutes-role and schedule groups remain second wave; injuries, new external sources and new architecture remain shelved.

Freeze train `22023`/`22024`, calibration `22025`, exact incumbent hyperparameters, cohort/source snapshot, selector tolerance/policy and sample seed. Preserve injury exclusion. Use full-suite refitting for the first wave rather than the current partial `--force-features` shortcut. Save requested/resolved/effective feature sets and verify the actual artifact throughout replay.

Compare all models with the same five raw edge controls; use raw .05 as a primary fixed comparison. Also retain the current edge .05 / BL .5/.5/.5 cell as one fixed operational comparison. Do not re-optimize a 95-cell BL grid for every family and select each model's prettiest result. Require improved paired profit/risk/volume and stat/side behavior, not better calibration alone or a smaller losing bet set. Tune only a survivor later, as a separately identified challenger.

This October 21–November 2 interval is now a discovery/selection window. Reserve later accepted `22026` observations for independent confirmation. Replaying `22025` against this artifact would reuse calibration data, not supply an independent validation window. Generic NBA family controls/lifecycle forwarding still need the narrow implementation described in the proposal; no new model job was launched here.

## Relevant prior lessons and source routes

- GBrain `lessons/feature-selector-is-not-an-ablation`, `lessons/correlated-feature-family-validation`, `lessons/cheap-baseline-before-architecture`: compare explicit whole-family treatments at fixed settings; do not infer family value from selection or immediately replace architecture.
- `operations/critical-invariants`: empirical CDF, no global conformal offsets/Q10 correction, no automatic promotion, owner-launched long jobs.
- `src/models/train_pipeline.py:940-1024,1110-1199` — sampled combined calibration and Pearson/Spearman distinction.
- `src/models/monte_carlo.py:168-181,233-235,327-334` — copula precedence and latent correlation.
- `src/backtesting/nba/sweep_runner.py:522-568`; `src/backtesting/backtest_harness.py:563-644` — selected-stat metrics and one-best-bet policy.
- `src/backtesting/bet_simulator.py:187-199,324-325` — nominal flat stakes versus remaining cash.
- Exact sweep manifest, cell ledgers and model JSON reports above are the numeric authority. No DB query, model fitting, model deserialization or replay was performed by this review.
