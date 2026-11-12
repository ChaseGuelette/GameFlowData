# Repaired NBA pinball retune: completed sweep and diagnostic comparison

## Verdict

Both requested jobs completed, and the saved evidence verifies. The rate retune produced small, mixed forecast changes and some raw-loss reduction, not a demonstrated whole-suite betting upgrade. Keep the original game-market-free artifact as the comparison control; do not promote the candidate or launch another broad tuning search based on this window. Rebounds show the most consistent modest forecast improvement, but the fixed BL portfolio did not improve overall. The implementation fixes remain valid; a correct tuning method is not a guarantee of incremental predictive signal.

## Exact evidence

- Control sweep: `backtest_results/nba_game_market_free_holdout_sweep_10_nov2_v1`.
- Candidate sweep: `backtest_results/nba_rate_pinball_feature_match_holdout_sweep_10_nov2_v1`.
- Control diagnostic: `backtest_results/nba_game_market_free_error_stages_nov2_v4`.
- Candidate diagnostic: `backtest_results/nba_rate_pinball_feature_match_error_stages_nov2_v1`.
- Candidate artifact: `src/models/artifacts/nba_rate_pinball_feature_match/nba_run_20261110_092318`.
- Numeric evidence: [comparison JSON](evidence/nba-repaired-pinball-sweep-diagnostic-comparison.json).

## Completion and comparability

- Each sweep has exactly 10 planned cells, completed receipts, summary rows and cell directories, with 10 distinct wager portfolios. Verified all 51 export hashes per sweep and each receipt's CSV row count. Reconciled bet counts, total stakes/profits and ROI; reconstructed wager outcomes/payoffs from saved labels and odds. This is accounting verification, not independent source truth.
- Both diagnostics are complete; all five file receipts per diagnostic match. Each reproduced its own sweep baseline and canonical RNG state. All 1,579 eligible player-games are oracle-valid, with no missing/invalid outcomes.
- Fixed settings, predictor settings, dependency versions, observed date counts, actual/void digests and ordered-feature identity match. Independently matched all 11,053 exported candidate-edge quote keys/prices/timing fields. Only `quantile_trainer.py` differs in the recorded source-file hashes, consistent with the reviewed trainer repair; input identities appropriately differ with model predictions.
- Component keys/actuals/L5 references match. The quoted probability comparison uses exactly the same 2,932 opportunities, actual labels and paired-market probabilities. Raw Brier scores were independently recomputed from the exports.
- Window: October 21–November 2, 2026; 12 dates with scored predictions. Both jobs remain `decision_grade=false` and historical readiness unverified. This is already-used discovery data, not independent certification.

## Matched betting ladder

All cells use the same nominal $10 flat stake, $2,000 bankroll and, where enabled, fixed BL tau/z_max/max_weight 0.5/0.5/0.5. Raw controls are separate policies; do not sum grid-cell profits or interpret their edge thresholds as identical posterior/raw criteria.

| Edge | Control raw ROI | Candidate raw ROI | Control BL ROI | Candidate BL ROI | Candidate BL bets |
|---|---:|---:|---:|---:|---:|
| 3% | -7.59% | -6.33% | -3.15% | -1.34% | 392 |
| 5% | -8.11% | -5.80% | +2.49% | +0.42% | 223 |
| 7% | -8.52% | -4.93% | +1.17% | -1.59% | 141 |
| 10% | -5.00% | -5.29% | -11.07% | -4.12% | 65 |
| 12% | -5.72% | -8.88% | -0.90% | +7.29% | 32 |

At frozen edge 5%:

- Raw control: 944 bets, -$765.24, -8.11% ROI. Candidate: 954 bets, -$552.66, -5.80% ROI. Observed gain is $212.58, but the candidate remains losing. Saved drawdown falls from 48.77% to 38.87%.
- BL control: 262 bets, +$65.15, +2.49% ROI. Candidate: 223 bets, +$9.40, +0.42% ROI. Candidate loses $55.76 of the control's profit; drawdown is similar (4.74% vs 4.49%).
- Candidate BL 12% edge has +7.29% ROI, but only 32 bets and +$23.32. It is an underpowered post-grid maximum, not a deployment winner.
- Nominal flat staking can be partially funded by available cash: one raw 5% wager in each artifact is below $10. This is existing bankroll policy, not Kelly.

### Paired wager mechanism

At raw 5%, 808 common wagers account for most losses. The candidate-only 146 wagers earned $160.30; control-only 136 wagers lost $43.73. A small common-wager P&L difference comes from stake funding. This is a localized favorable discovery result, but not enough to make the full raw portfolio profitable.

At BL 5%, 206 common wagers earned $54.52 in both runs. The control-only 56 wagers earned $10.63; candidate-only 17 wagers lost $45.13. The net deterioration is concentrated in changed selection, not a different payoff on identical wagers.

Candidate BL 5% per-stat: PTS 81 bets, -$0.75; REB 109 bets, +$69.59; AST 33 bets, -$59.44. The corresponding control REB slice earned $97.10 on 130 bets, so improved rebound forecast scores did not automatically improve the selected BL rebound portfolio.

Candidate BL 5% unders earned $163.24 on 144 bets; overs lost $153.84 on 79 bets. This is a post-hoc decomposition, not an executed under-only selection policy or approval to change direction filters.

## Component and combined forecast quality

| Component | Control mean pinball | Candidate mean pinball | Relative change (negative better) |
|---|---:|---:|---:|
| minutes | 1.51512506 | 1.51512506 | +0.000% |
| pts_rate | 0.05827508 | 0.05845072 | +0.301% |
| reb_rate | 0.02735925 | 0.02722532 | -0.490% |
| ast_rate | 0.01932883 | 0.01933928 | +0.054% |

The changes are small: rebound-rate loss improves about half a percent; points-rate loss worsens about three-tenths of a percent; assists-rate loss is nearly unchanged/slightly worse. Minutes are identical, as intended. Combined total-stat pinball changes are PTS +0.039%, REB -0.565%, AST -0.246%. No large new predictive gain appeared.

## Probability quality on matched quotes

Lower Brier is better. Intervals are 5,000 paired resamples of whole dates (seed 42), weighted by the resampled opportunity counts.

| Stat | N | Control Brier | Candidate Brier | Market Brier | Candidate-control paired-date 95% interval |
|---|---:|---:|---:|---:|---|
| pts | 1115 | 0.256633 | 0.257188 | 0.249756 | [-0.000079, +0.001352] |
| reb | 1033 | 0.253469 | 0.252443 | 0.244925 | [-0.001939, -0.000205] |
| ast | 784 | 0.250791 | 0.250548 | 0.242984 | [-0.001733, +0.001120] |
| ALL | 2932 | 0.253956 | 0.253741 | 0.246243 | [-0.000993, +0.000585] |

Rebounds have the clearest small improvement: the exploratory per-stat interval is below zero. Aggregate and PTS/AST intervals include zero. This is only 12 previously used dates, without multiple-comparison correction; it does not establish independent robustness. Every candidate raw stat still trails its paired-market reference.

Average absolute raw probability movement is about 1.26 percentage points across the same opportunities. Small probability shifts can nonetheless move bets across thresholds and change the selected stat, explaining larger portfolio changes than the component-loss changes suggest.

## Selection and oracle interpretation

Candidate raw model-minus-market Brier disadvantage grows from 0.00750 on all quoted opportunities to 0.01342 after best-stat selection and 0.01543 on final wagers. Selection amplification is reduced relative to the control but persists. These are changing cohorts, not a randomized causal estimate.

Fixed BL brings all-quoted Brier close to market (0.24675 vs 0.24624), almost unchanged from the control. On final BL wagers its disadvantage is 0.00467, versus 0.00176 for the control's final portfolio; those portfolios differ and are not a same-bet intervention comparison.

All three oracle labels remain `investigate_rate`. Actual-rate substitutions reduce total pinball roughly 60%/57%/68% for PTS/REB/AST, far more than actual-minute substitutions. Actual rate contains realized outcome noise; this does not show that such gains are learnable or that another hyperparameter search will recover them. Do not interpret unchanged triage labels as failed execution.

## Uncertainty and next decision

Paired-date bootstrap intervals for candidate-minus-control profit at edge 5% are approximately [-$171, +$619] for raw and [-$226, +$72] for BL. Both cross zero. These exploratory intervals do not undo selection from earlier experiments, and short-window annualized Sharpe is not a stable long-run estimate.

The requested train/sweep/diagnostic experiment is finished. Do not rerun it for completeness or promote the whole candidate. Retain it as an exploratory challenger; the original remains the methodological control, not a newly certified live model. If further development is approved, the evidence supports a narrowly defined stat/selection investigation rather than more full-suite trials. Rebounds' small matched-score gain is a possible lead, while points/assists and over-selection remain weak; none authorizes splicing models or new betting filters automatically.

Relevant prior lessons/invariants: preserve empirical probabilities, no global conformal offsets or blind Q10 correction, cheap controlled experiments before architecture, and separate discovery from untouched confirmation. No training, DB query, new replay, production mutation or model promotion was performed during this saved-export analysis.
