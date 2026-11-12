# NBA direct-total REB benchmark

## Kick off immediately

- **You:** the first benchmark run is complete; do not repeat training. Rebounds-only sweep/analysis support is now approved and being implemented separately.
- **This chat:** preserve the completed benchmark and implement the separate saved-window direct/control betting comparison; then continue error-guided improvements to the existing minutes/rate architecture.
- **Other chat:** no additional benchmark writer needed. Production and paper-policy work are separate.

## Completed owner run — November 12, 2026

`backtest_results/nba_direct_reb_benchmark_nov2_v1` completed. Coordinator verified every manifest-listed file hash and independently recomputed both models' pinball and Brier scores from exports. The run has 1,579 matched player-games, 1,033 quotes and 12 scored dates. Direct mean pinball is 0.673762 versus control 0.680703 (about 1.02% lower); direct Brier is 0.251949 versus control 0.253469, still above paired-market 0.244925. Median MAE is slightly worse (1.950692 versus 1.940735), while RMSE is lower (2.527674 versus 2.569510). This is a modest mixed discovery result, not promotion evidence. The loader printed injury acquisition, but no injury features entered the saved 55-feature model. No hyperparameter tuning was run.

The training command below is retained as provenance, not a request to rerun against the occupied output directory. The separate sweep extension must not refit the model or overwrite these outputs.

Chase subsequently approved an optional pinball retune and independent optional feature reselection. [The opt-in plan](../../.hermes/plans/nba-direct-reb-retune-2026-11-12.md) is in implementation: tuning defaults to100 shared trials when enabled; reselection uses the parent's62 allowed pregame candidates on an inner-training partition, preserving injury/game-line exclusions. The default remains fixed55features/no tuning. New variants require separate output directories; their commands are not ready until verification passes.

### Exploratory paired-date uncertainty

A saved-export comparison used 5,000 paired resamples of whole scored dates (NumPy `default_rng(42)`), weighting each resample by its observation count. Candidate-minus-control mean pinball was -0.006941, with approximate 95% interval [-0.013701, -0.001424]; 9 of 12 dates improved. Brier delta was -0.001520, interval [-0.004697, +0.002003]; 7 of 12 dates improved. Quantile pinball improved at Q10/Q25/Q75/Q90 (about 1.65%/1.15%/1.03%/3.29%) and worsened at Q50 (about 0.51%). This supports a modest distribution-score lead within this reused discovery window; it does not establish independent robustness, a reliable probability advantage or betting profitability. Date clustering and prior experiment selection remain limitations; no new fit or replay was run for this calculation.

This is an owner-run side experiment. It fits five direct `actual_reb` quantile heads with the saved game-market-free REB hyperparameters and compares them with the existing minutes-times-rate control on exactly matched October–November 2026 (`season_id=22026`) rows and quote occurrences.

It does **not** tune, simulate bets, claim ROI, promote a model, or replace the production architecture. Calibration season `22025` is evaluation-only. Training alone applies `actual_minutes >= 10`; evaluation does not use realized minutes for eligibility. The equal-information feature set is the sorted union of all parent minutes and REB-rate head features, including `prop_line_reb`, while retaining the parent's exclusions of `line_spread`, `line_total`, and injury context.

## Owner launch (PowerShell)

Run from `C:\Users\Chase\Projects\GameFlowData` after reviewing the dry-run receipt. Choose a new output name; neither the final name nor its `_incomplete` sibling may already exist.

```powershell
& 'C:\Users\Chase\Projects\GameFlowData\venv\Scripts\python.exe' 'C:\Users\Chase\Projects\GameFlowData\scripts\benchmark_nba_direct_reb.py' --model-dir 'C:\Users\Chase\Projects\GameFlowData\src\models\artifacts\nba_feature_ablations\game_market_free\nba_run_20261108_150945' --sweep-dir 'C:\Users\Chase\Projects\GameFlowData\backtest_results\nba_game_market_free_holdout_sweep_10_nov2_v1' --diagnostic-dir 'C:\Users\Chase\Projects\GameFlowData\backtest_results\nba_game_market_free_error_stages_nov2_v4' --output-dir 'C:\Users\Chase\Projects\GameFlowData\backtest_results\nba_direct_reb_benchmark_nov2_v1' --local
```

Add `--dry-run` first for filesystem/config validation only. Dry-run creates no output, opens no database connection, loads no model binary, and performs no fit.

## Outputs and interpretation

A successful final directory contains:

- `reb_direct_total_model.joblib`
- `effective_config.json` and `fit_calibration_scores.json`
- `matched_totals.csv` with native direct quantiles, sampled direct quantiles, and matched control sampled quantiles
- `matched_quotes.csv` preserving quote occurrences and empirical `(samples > line).mean()` probabilities
- `summary.json`, `report.md`, and `benchmark_manifest.json`

Negative candidate-minus-control pinball/MAE/RMSE/Brier deltas are better predictive scores. They are not profitability evidence. The discovery window is previously consumed data, and the union feature design does not isolate architecture while holding per-head dimensionality identical.

## Offline validation

- `py_compile` and Ruff passed for both new Python files.
- The public CLI `--help` passed.
- The exact artifact/sweep/diagnostic `--dry-run` passed under the NBA offline guard; the guard reported zero connection, model-load, or model-fit attempts, and no output directory was created.
- Coordinator verification passed the populated synthetic fit/evaluation and full successful public executor under the same guard: 11 direct-fit rows, 5 calibration rows scored without a realized-minutes filter, 2 matched total rows, and 3 quote occurrences. It checked the `actual_reb` target, zeroed offsets, no minutes multiplication, canonical inverse sampling, empirical quote probability, all saved file hashes, report/export readback, complete manifest, final no-replace rename, failed-stage manifest, and the specified negative cases.
- Review found and corrected an outcome-cap mismatch: the bounded wide-outcome preflight now runs before fitting; the subsequent base/combo long-form outcome population is compared by player-game keys, not capped by expanded row count. The controlled full executor accepted 14 stat rows for 2 player-games with a cap of 2.
- Controlled receipt: `C:\Users\Chase\AppData\Local\hermes\cache\scratch\nba-direct-reb-worker\controlled-probe-receipt.json`.
- Independent specification recheck and code-quality review passed after the cap repair. Main-checkout verification repeated the exact-input dry-run, full controlled executor, Ruff and all 8 existing Monte Carlo integration tests. Coordinator type-only refinements then passed Pyright with 0 errors and 3 intentional private-helper reuse warnings, Ruff, and the controlled executor again. No new unit tests were added.

The implementation worker did not run historical SQL acquisition, a real XGBoost fit, or the full matched evaluation. Those remain owner-launched.
