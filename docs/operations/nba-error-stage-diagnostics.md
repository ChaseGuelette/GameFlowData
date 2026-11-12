# NBA error-stage diagnostics

## Purpose and status

Identify which part of the existing minutes-times-rate pipeline deserves the next development experiment. This is diagnosis, not another training run or an automatic model-selection gate.

Implemented: `scripts/diagnose_nba_error_stages.py` and `src/diagnostics/nba_error_stages.py`. Existing training, tuner, model, sampler and replay source are unchanged. Populated controlled verification uses the real canonical acquisition/evaluation/sampler paths with synthetic boundary inputs; actual model/database execution remains the owner-run acceptance step.

## Run from the GameFlowData root in PowerShell

The command inherits October 21–November 2, 2026, seed 42, 5,000 samples, quotes/eligibility, copula parameters and fixed raw/BL .05 comparisons from the exact completed control sweep. It refuses a different model identity or changed predictor/dependency settings.

```powershell
.\venv\Scripts\python.exe -X utf8 scripts\diagnose_nba_error_stages.py --local --model-dir src\models\artifacts\nba_feature_ablations\game_market_free\nba_run_20261108_150945 --sweep-dir backtest_results\nba_game_market_free_holdout_sweep_10_nov2_v1 --output-dir backtest_results\nba_game_market_free_error_stages_nov2_v3
```

Append `--dry-run` for filesystem/config preflight only. Help and dry-run do not connect to a database, deserialize models, create a result directory or run predictions. The exact command above passed preflight; that is not a real-data execution claim.

Use a new output directory on reruns. Failed or interrupted runs retain an `_incomplete` directory and a manifest with the failing stage. Do not overwrite existing evidence or point output inside the model/reference sweep.

## Four diagnostic tests

1. **Component quality:** minutes and PTS/REB/AST rates are scored separately using Q50 MAE/RMSE/bias and quantile pinball/coverage. The cheap point references are the existing pregame L5 minutes and L5 stat/minutes ratios. These are descriptive baselines, not newly trained models.
2. **Oracle sensitivity:** canonical baseline versus actual minutes with learned rates, learned minutes with actual rates, and a both-actual sanity arm. All arms use aligned draws and the same valid outcomes. The existing predictor is reused with detached in-memory quantile adapters; the original pipeline/artifact is not rewritten.
3. **Combined probabilities:** total-stat distribution errors and empirical line-probability Brier are compared with the same paired-market probabilities. The baseline is first required to reproduce the stored reference's candidate predictions and raw/BL wagers.
4. **Selection amplification:** compare all quoted opportunities, one-best-stat-per-player-game selections, and the actual fixed-policy/cash-filtered simulated wagers. This separates cohort selection from a change in the underlying prediction. No thresholds or new ranker are fitted.

## Read the results

- `report.md`: short component table, oracle loss reductions and tentative next-stage suggestions.
- `summary.json`: per-component, per-stat, common-cohort and selection-stage metrics with valid counts and exclusions.
- `components.csv`: minutes/rate forecasts, actuals and lagged baselines.
- `totals.csv`: baseline/oracle total-stat forecasts, valid-cohort flags and explicit `oracle_non_executable` labels.
- `quote_probabilities.csv`: baseline/oracle empirical probabilities against the same lines and paired-market probabilities.
- `diagnostic_manifest.json`: exact model/reference identities, settings, source hashes, database-guard readbacks, output receipts and completion state.

Positive oracle reductions mean that perfect information about that component would improve the measured outcome on this cohort. A consistent minutes/rate suggestion prioritizes investigation; it is not proof that the corresponding learned head is the unique causal failure or that its oracle gain is attainable. If the metrics disagree, the result is explicitly `inconclusive_or_mixed`.

If component scores are weak against lagged baselines and the corresponding oracle helps, investigate that head. If components are reasonable but combined probabilities are poor, investigate composition/distribution behavior. If quality falls mainly after extreme-edge selection, investigate selection robustness. None of these findings automatically changes hyperparameters, feature families or betting rules.

## Boundaries and limitations

- Requires `--local`. Every data transaction enforces and reads back PostgreSQL READ ONLY and a 15-second statement timeout.
- Default feature caps: 200 games (`--max-games`) and 2,000 rows (`--max-player-games`). Separate outcome caps: 200 games (`--max-outcome-games`) and 10,000 rows (`--max-outcome-rows`); at most a 31-day window. The selected reference records 1,954 feature rows and 1,579 eligible player-games. Outcome reads include all non-DNP players in the date window, a larger population than feature eligibility, so outcome limits are independent of feature limits. Both row options are bounded to 1..10,000 and both game options to 1..200; exceeding a cap fails rather than silently samples. No automatic cap escalation/retry occurs.
- Actual minutes use the same `player_game_stats.min` source expression as ordinary training. Actuals are date-bounded and exclude explicitly marked DNP records, matching the current replay reader; no settlement/participation policy changes are made.
- Zero/invalid minutes make observed per-minute rates undefined. Such rows remain visible in baseline coverage but are excluded from the common oracle cohort, with counts reported. No zero-rate guess is substituted.
- Oracles use hindsight. They are never deployable models, betting signals, calibrated production artifacts or estimates of achievable profit. Oracle betting ROI is deliberately not reported. Minutes and rates are dependent, so the sensitivity comparison is not an additive causal decomposition.
- No global calibration offsets, Gaussian probability approximation, training, tuning, remote DB calls, migrations, rescraping, paper/live placement or promotion.
- This remains a previously used discovery window. Historical readiness and decision-grade status remain unverified/false.
- Fresh baseline/outcome/quote drift is a blocker to matched attribution, not permission to replace the reference silently. Failure messages preserve safe canonical stage/date context without printing connection credentials.

## Verification performed

- 18 existing calibration/Monte Carlo checks passed under the offline guard.
- One explicitly synthetic populated integration probe exercised canonical acquisition, raw/BL selection, all oracle arms, exact sample/RNG reproduction, actual/row-cap rejection, read-only callback failure, and complete diagnostic export/receipt readback. Four controlled player-games included one zero-minute case; no actual model or database was loaded.
- The exact completed control/reference passed CLI help and dry-run. Four actual saved-CSV normalization checks passed without DB/model execution.
- Scoped Ruff and syntax checks passed. Full real-data diagnosis was not launched by the agent.
- Row-cap repair: separate outcome/feature defaults passed the exact v2 CLI dry-run under the offline guard; controlled loader checks accepted a larger outcome population while retaining feature-cap, outcome-overflow, invalid-bound, empty and duplicate protections. The original v1 incomplete evidence is retained. Real local outcome-loader verification is separate from full diagnostic execution.
- Local outcome-loader verification returned 2,803 rows across 104 games, with no missing or duplicate player-game keys; read-only and 15-second timeout guards passed for both queries. The previous 2,000-row failure reproduced. This exposed the same population mismatch in the game cap, now separated too. The actual CLI game-guard branch accepts 104 outcome games under 200, rejects them under 100, and preserves the feature-game cap at 100. Final v2 dry-run and Ruff passed; full diagnostic remains owner-run. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_actual_loader_verification_20261109.json`.

### Feature-stage follow-up (supersedes the earlier 100-game default)

The real local feature path returned 1,954 rows across 104 games with zero duplicate/missing player-game keys and valid game IDs; `--max-games=100` alone caused the next failure. CLI and callable preflight now default to 200 feature games, within the existing hard maximum; the 2,000-feature-row cap remains unchanged. The CLI unwraps only safe `DiagnosticError` context from acquisition's `SweepFailure`, keeping arbitrary database errors redacted while preserving actionable cap/count messages in console and failure manifests. The earlier outcome-only verification was insufficient to establish feature-stage readiness.

Real feature acquisition ran exactly once under read-only/15-second guards, without predictions. Controlled checks verify the new default accepts a 1,954-row/104-game batch and rejects 201 games or 2,001 rows. Safe-error propagation/redaction checks, final v3 preflight and Ruff passed. No full diagnostic was launched. Local feature receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_feature_path_probe_20261109_receipt.json`.

### Oracle dtype repair

Reproduced `TypeError: Invalid value ... for dtype 'float32'` in `_CachedHead` with pandas 3.0.0: float64 actual-rate ratios could not be assigned into copied float32 model quantiles. Replacement arms now explicitly widen detached copies to float64; baseline dtype, original model heads and source frames are unchanged. A controlled two-date probe exercised all four arms with float32 heads, non-exact ratios and zero-minute exclusions through the real sampler and downstream component/total/probability scoring. Exact baseline samples/RNG, untouched source/baseline frames, masked-row preservation, offline guards and Ruff passed. The v4 CLI dry-run passed; full real-data diagnostic completion is not claimed.

Probe: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/check_nba_oracle_float32.py`. Use a fresh output basename `nba_game_market_free_error_stages_nov2_v4` for the next owner run.

Implementation scope and delivery receipt: `.hermes/plans/2026-11-08-nba-error-stage-diagnostics.md`.
