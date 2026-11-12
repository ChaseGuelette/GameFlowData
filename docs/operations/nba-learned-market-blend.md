# NBA learned per-stat market blend

## Different from ordinary Black-Litterman

Ordinary BL remains unchanged:

```text
confidence = min((abs(mean(samples) - line) / std(samples)) / z_max, 1)
weight = min(tau * confidence, max_weight)
p_final = sigmoid(logit(p_market) + weight * (logit(p_model) - logit(p_market)))
```

The new offline mode is `learned_market_v1`:

```text
weight = learned_weight_for_stat[stat]
p_final = sigmoid(logit(p_market) + weight * (logit(p_empirical) - logit(p_market)))
```

Each PTS/REB/AST weight is one frozen scalar in `[0,1]`. Inputs to the logits are clipped to `[.01,.99]`; the original empirical probability remains saved as evidence. There is no tau, z-score, maximum-weight cap, confidence multiplier, intercept, online update, or silent missing-stat fallback. On supported positive half-point lines, UNDER is `1 - OVER`.

## Supported workflow

This command is an offline saved-window research lane. It does not retrain/load a base model, query a database or API, generate forecasts for arbitrary dates, alter ordinary BL, support combo/integer-line markets, wire live inference, or claim compatibility with the generic NBA evidence CLI.

`prepare` receipt-checks a completed diagnostic and its exact bound sweep, retains baseline executable rows only, joins original empirical probabilities/outcomes to raw and one declared usual-BL all-candidate table, and writes one authoritative quote per player/game/stat before selection.

`fit` minimizes pooled binary log loss, then per-stat binary log loss plus `shrinkage_strength * (b - b_pool)^2`, using bounded SciPy optimization plus explicit endpoints. Reports are labeled in-sample fit evidence; ROI is not fitted.

`evaluate` freezes the artifact and compares `raw`, `usual_bl_reference`, `market_only`, and `learned_market_v1` on a strictly later matched cohort. It replaces probabilities/edges before calling `BacktestHarness._filter_best_bets`, then uses the existing `BetSimulator` and metrics calculator with a fresh simulator per treatment/threshold.

All output directories are exclusive and published from an incomplete temporary directory. `--dry-run` validates inputs/configuration without creating outputs, fitting coefficients, or simulating bets.

Source model identity is verified, not pinned to a development fixture. Any valid explicitly selected usual-BL comparator tuple is supported; its actual values are retained. A separately prepared later evaluation bundle may have different dates/input hashes while matching model, predictor settings, stats, quote-policy/probability semantics and comparator. Exact fit game IDs are stored to reject overlap across bundles. Consumed inputs are hashed and rechecked before fit/evaluation publication. Outputs cannot be inside the source model directory.

## Commands

Run from `C:\Users\Chase\Projects\GameFlowData`. Append `--dry-run` first. The examples use the available **game-market-free control**, not the intended original no-injury baseline. The dates and shrinkage below reproduce a small engineering-check design, not a recommended final experiment or untouched holdout. Use durable output paths outside Hermes scratch for owner experiments.

### Prepare

```powershell
.\venv\Scripts\python.exe scripts\nba_learned_market_blend.py prepare --diagnostic-dir backtest_results\nba_game_market_free_error_stages_nov2_v4 --sweep-dir backtest_results\nba_game_market_free_holdout_sweep_10_nov2_v1 --usual-bl-cell cell_a320710db5e257998a6b67bea32c3141e2a698d26c54a4454ee2dca37da1e0cb --stats pts reb ast --output-dir backtest_results\nba_learned_blend_example\prepared
```

Outputs: `opportunities.csv`, `preparation_manifest.json`, `preparation_report.md`.

### Fit

Choose an earlier whole-date range and freeze `--shrinkage-strength` before the owner run:

```powershell
.\venv\Scripts\python.exe scripts\nba_learned_market_blend.py fit --input-dir backtest_results\nba_learned_blend_example\prepared --fit-start 2026-10-22 --fit-end 2026-10-24 --shrinkage-strength 1.0 --output-dir backtest_results\nba_learned_blend_example\fit
```

Outputs: `learned_blend.json`, `fit_scores.csv`, `fit_report.md`, `fit_manifest.json`.

`--shrinkage-strength` is regularization toward the fitted pooled weight, not the final model/market blend weight. Zero disables the penalty; positive values pull stat weights toward the pooled fit. Freeze it before evaluation, not from later ROI.

### Evaluate

Evaluation must start strictly after both the artifact's declared and observed fit end dates and cannot overlap fit games:

```powershell
.\venv\Scripts\python.exe scripts\nba_learned_market_blend.py evaluate --input-dir backtest_results\nba_learned_blend_example\prepared --blend-artifact backtest_results\nba_learned_blend_example\fit\learned_blend.json --start 2026-10-25 --end 2026-10-25 --edge-thresholds 0.05 --starting-bankroll 2000 --flat-bet-size 10 --output-dir backtest_results\nba_learned_blend_example\evaluation
```

Optional direction restrictions use entries such as `--allowed-bets pts:under reb:over`. Canonical ranking happens first; disallowed winning directions are rejected without selecting a lower-ranked fallback.

Outputs: `all_candidates.csv`, `selected.csv`, `bets.csv`, `probability_scores.csv`, `cell_configs.json`, `metrics.json`, `evaluation_manifest.json`, `evaluation_report.md`.

## Evidence status and remaining gate

Coordinator bounded public-CLI acceptance used authentic control rows: 2,932 prepared opportunities, 691 fitting rows on October 22–24 and 209 later opportunities on October 25 across four treatments. Independent objective/formula/payoff checks, dry-run nonmutation, occupied-output rejection and cross-bundle game-overlap checks passed under the offline guard. A separate-window check used an explicitly labeled derivative of genuine later-date rows, not a new native acquisition. Existing BL/harness/legacy-sweep baseline tests passed 84 checks; final integration verification is recorded in the restart checkpoint.

The populated game-market-free diagnostic/sweep can exercise the implementation, but it is a different research control from the intended original no-injury owner baseline. Its use must remain labeled engineering acceptance/development evidence. A receipt-compatible diagnostic for the owner-selected original baseline, plus owner-frozen fit/evaluation dates, shrinkage strength, edge thresholds, sides, bankroll, and stake, remains the data/policy gate before the full study. Do not publish engineering-smoke weights as the experiment result.
