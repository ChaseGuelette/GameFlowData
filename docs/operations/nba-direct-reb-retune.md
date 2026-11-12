# NBA direct-total REB opt-in retune

These owner-launched diagnostics preserve the completed fixed benchmark and the original game-market-free minutes-times-rate control. They do not promote a model or change production.

## Completed tuning-only run — November 12, 2026

`nba_direct_reb_pinball_benchmark_nov2_v1` completed. All manifest-listed hashes verified; coordinator recomputed pinball/Brier from exports and verified identical outcome/quote cohorts against the fixed direct benchmark. Do not rerun the occupied tuning-only output path below.

The 100-trial study has 61 completed and 39 pruned trials. First completed score0.697421; best score0.694276 at trial40 (about0.451% improvement over trial0, not over the fixed model). All61 completed scores are distinct; median0.695171. No best-so-far improvement occurred after trial40. Pruned partial-quantile scores were excluded from this comparison.

On the same discovery cohort, fixed-to-retuned direct-model mean pinball improves0.673762→0.668715 (about0.749%), median MAE1.950692→1.947526 and RMSE2.527674→2.522447, while quote Brier worsens0.251949→0.252216; market Brier remains0.244925. Training subset receipts match (42,678 player-games), and all learned component offsets recorded in both runs are zero. This is a small distribution-score improvement, not a probability or betting upgrade. The fixed artifact did not save an inner-validation pinball score, so trial0 is not a substitute for that missing same-fold comparator. Coverage lines and their train-minus-validation gaps are not the Optuna pinball objective. No new training, data acquisition or replay was launched for this saved-export check.

The tuning-only variant keeps the same 55 fixed inputs and fits `actual_reb` after 100 fresh shared-parameter Optuna trials using mean pinball loss. Training seasons are `22023` and `22024`; `22025` is calibration-only; evaluation remains October 21 through November 2, 2026.

```powershell
& 'C:\Users\Chase\Projects\GameFlowData\venv\Scripts\python.exe' -X utf8 'C:\Users\Chase\Projects\GameFlowData\scripts\benchmark_nba_direct_reb.py' --model-dir 'C:\Users\Chase\Projects\GameFlowData\src\models\artifacts\nba_feature_ablations\game_market_free\nba_run_20261108_150945' --sweep-dir 'C:\Users\Chase\Projects\GameFlowData\backtest_results\nba_game_market_free_holdout_sweep_10_nov2_v1' --diagnostic-dir 'C:\Users\Chase\Projects\GameFlowData\backtest_results\nba_game_market_free_error_stages_nov2_v4' --output-dir 'C:\Users\Chase\Projects\GameFlowData\backtest_results\nba_direct_reb_pinball_benchmark_nov2_v1' --local --tune --tuning-trials 100
```

The separate tune-plus-reselection variant first runs the existing selector independently for each quantile on only the inner-training partition of the parent's 62 allowed pregame candidates. It answers a different question and confounds a pure tuning-only comparison.

```powershell
& 'C:\Users\Chase\Projects\GameFlowData\venv\Scripts\python.exe' -X utf8 'C:\Users\Chase\Projects\GameFlowData\scripts\benchmark_nba_direct_reb.py' --model-dir 'C:\Users\Chase\Projects\GameFlowData\src\models\artifacts\nba_feature_ablations\game_market_free\nba_run_20261108_150945' --sweep-dir 'C:\Users\Chase\Projects\GameFlowData\backtest_results\nba_game_market_free_holdout_sweep_10_nov2_v1' --diagnostic-dir 'C:\Users\Chase\Projects\GameFlowData\backtest_results\nba_game_market_free_error_stages_nov2_v4' --output-dir 'C:\Users\Chase\Projects\GameFlowData\backtest_results\nba_direct_reb_pinball_reselect_benchmark_nov2_v1' --local --tune --tuning-trials 100 --reselect-features
```

Add `--dry-run` first. Dry-run performs filesystem/config validation only and leaves final tuned parameters or reselected features unresolved. A 100-trial run means 100 shared trials, each evaluating up to five quantile heads before pruning, followed by one final five-head fit.

## Delivery verification

Integrated into main, uncommitted. Independent quality review passed; its sole spec finding (dry-run reporting inherited feature identity as final during reselection) was corrected and verified in main: final count/identity are null until selection, with inherited values labeled separately. Both exact owner-command dry-runs, four-mode controlled real-Optuna/estimator-double and selector-boundary checks, 25 existing simulator/Monte Carlo tests, Ruff and compilation passed. Pyright has 0 errors and 3 existing private-helper reuse warnings. Full historical tuning/reselection was not run by an agent. Fixed benchmark outputs and shared tuner/trainer/selector source remain unchanged.