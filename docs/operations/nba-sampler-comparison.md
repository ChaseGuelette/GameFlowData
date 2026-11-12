# NBA sampler comparison runner

## Purpose and acceptance boundary

`compare_nba_samplers.py` is a bounded, local-only engineering diagnostic. It loads one existing manifested NBA artifact, acquires the source population once through the canonical sweep seams, freezes the component quantile forecasts, and compares exactly three Monte Carlo sampler settings across paired explicit seeds:

- `control`: current canonical predictor settings.
- `no_reb_widening`: only `variance_inflation["reb"] = 1.0`.
- `no_extra_lower_tail`: only `tail_adjustment["lower_tail_multiplier"] = 1.0`.

It does not fit, retrain, choose a seed, search thresholds, alter the copula, apply global conformal offsets, promote a model, write to a database, or claim a winner. Results remain `decision_grade: false` and `historical_readiness: unverified`.

The current executable source allowlist is `sweep_config.BOOKMAKERS`: 23 names and **no `fliff`**. The older original sweep manifest included `fliff`; this command does not weaken the current whitelist to reproduce that historical list. Omitting `--bookmakers` uses the full current 23-name list, and dry-run output records the complete effective list.

## Scientific and replay contracts

- Builds an ordinary canonical `SweepConfig` with one edge threshold, raw replay, and one fixed BL tuple. Existing plan/artifact validators remain unchanged. The acquisition seed must equal the first comparison seed, including direct API calls; parity checks cannot be skipped.
- Bootstraps and acquires once. Component heads are replaced instance-locally by recording proxies before acquisition, then frozen cache-only.
- A frozen request must match the complete prepared DataFrame content, order, columns, dtypes, index, and attrs through the existing typed content identity. A cache miss fails; it never falls through to a booster.
- Each seed/arm starts a fresh canonical `MonteCarloPredictor`. Within a seed, all arms consume identical random draws and must finish at identical `RandomState` state.
- The control at the first seed must exactly reproduce every canonical acquisition sample. Its raw and BL selected predictions, bets, candidate edges, and metrics must exactly reproduce `evaluate_cell`.
- Component forecasts are saved with date/player/game identity, feature and quantile hashes, quantile values, and cache receipts.
- All eligible quote rows are retained, including multiple books/lines. Probability scores are explicitly quote-row-weighted, not independent player-game-weighted.
- Raw scoring uses the empirical CDF `p(over) = (samples > line).mean()`. Under includes equality, consistent with canonical replay. Pushes are excluded from proper scores.
- Raw and fixed-BL all-quote outputs include Brier score and clipped log loss (`epsilon=1e-12`) with the devigged market comparator. Distribution summaries include median error, quantile coverage, and pinball loss.
- Canonical betting replay still uses the existing harness selection, line shopping, flat-stake simulator, actual/void settlement, and metrics. Fixed BL confidence may respond to changed samples; tau, z-max, max weight, edge threshold, stake, source, cohort, and sampling policy are not refit.

## Bounds and filesystem behavior

Defaults are hard caps, not truncation targets:

- 31 inclusive dates
- 5 unique seeds
- 20,000 samples per player/stat
- 10,000 eligible player-games
- 250,000 acquired quote rows
- 3,000,000,000 bounded component-sample workload units

The request ceiling is checked before runtime bootstrap. Observed player-game, quote-row, and workload bounds are checked immediately after the single acquisition and before the variant matrix. Over-limit requests fail closed; there is no unlimited mode.

`--local` and `--diagnostic-history` are required. Existing historical policy/source and acquisition-limit options are passed through the canonical `SweepConfig` contract when supplied.

Dry-run validates the exact artifact, metadata, source plan, bounds, effective bookmaker list, and destination collision without deserializing the model, opening database/network connections, loading dotenv credentials, or creating the output directory.

Normal execution creates only `<output>_incomplete` with exclusive creation. It renames non-replacing to the final directory only after output digests, cache call counts, parity checks, RNG checks, and current artifact/source-plan checks pass. Failure leaves an incomplete directory with a failed manifest and never publishes a success-shaped final directory. Existing final or incomplete destinations are rejected even when empty.

## Outputs

- `comparison_manifest.json`: canonical plan, artifact/source identity (including exact runner and CLI hashes rechecked at completion), arm settings, effective bounds, cohort/quote identity, cache receipts, parity and no-new-booster-call evidence, output hashes.
- `component_forecasts.csv`: frozen minutes/rate quantiles and identities.
- `all_quote_probabilities.csv`: every eligible quote row by arm/seed/raw-or-BL mode, actual, market comparator, and explicit score/exclusion status.
- `summary_scores.csv`: per-seed scores, seed mean/range aggregates, and paired same-seed deltas versus control.
- `selected_predictions.csv`: canonical selected prediction replay per arm/seed/mode.
- `bets.csv`: canonical simulator output per arm/seed/mode.
- `metrics.json`: canonical metrics per arm/seed/mode.
- `report.md`: concise engineering summary and limitations.

## Dry-run

Run from `C:\Users\Chase\Projects\GameFlowData`. PowerShell-safe single line:

```powershell
.\venv\Scripts\python.exe scripts\compare_nba_samplers.py --model-dir src\models\artifacts\no_injury_baseline\nba_run_20261108_122129 --start 2026-10-21 --end 2026-11-02 --stats pts reb ast --seeds 42 123 2026 --n-samples 5000 --min-minutes-avg 10 --starting-bankroll 2000 --flat-bet-size 10 --edge-threshold 0.05 --bl-tau 0.5 --bl-z-max 0.5 --bl-max-weight 0.5 --output-dir backtest_results\nba_no_injury_sampler_comparison_nov2_v1 --local --diagnostic-history --dry-run
```

## Owner launch command

This uses the original no-injury artifact and fixed example comparison settings. Engineering acceptance does not launch the full experiment:

```powershell
.\venv\Scripts\python.exe scripts\compare_nba_samplers.py --model-dir src\models\artifacts\no_injury_baseline\nba_run_20261108_122129 --start 2026-10-21 --end 2026-11-02 --stats pts reb ast --seeds 42 123 2026 --n-samples 5000 --min-minutes-avg 10 --starting-bankroll 2000 --flat-bet-size 10 --edge-threshold 0.05 --bl-tau 0.5 --bl-z-max 0.5 --bl-max-weight 0.5 --output-dir backtest_results\nba_no_injury_sampler_comparison_nov2_v1 --local --diagnostic-history
```

Seeds `42 123 2026` are explicit stability probes, not previously selected winning seeds. The `2026-10-21` through `2026-11-02` window is development evidence, not untouched confirmation.

## Engineering validation

The implementation acceptance boundary is:

1. Scoped Ruff and Pyright using the main environment/Python path.
2. The existing guarded 92-test offline suite; no tests are added or weakened.
3. Actual public CLI dry-run against the manifested artifact, confirming no output creation.
4. A short populated public-CLI execution using an injected controlled acquisition runtime and the real `MonteCarloPredictor`, all three arms, at least two seeds, canonical control sample/replay parity, same-seed RNG parity, manual empirical probability/payoff checks, parameter-only arm differences, occupied-output rejection, over-budget rejection, and no original head calls after freeze.

Controlled inputs or previously generated engineering exports are not historical performance evidence. No database/network/training call is part of engineering acceptance.

### Verified delivery — November 12, 2026

Integrated into the main working tree, uncommitted, after independent specification/quality approval. Main-checkout validation passed the 92 existing guarded tests, Ruff, and Pyright (zero errors/warnings; type-check categories remain enabled). The populated public CLI completed all three arms across two seeds with the real sampler and explicitly controlled heads/source inputs: 36 probability rows, exact canonical control sample/replay parity, paired RNG states and four original head calls before/after replay. Manual probability/Brier/payoff checks, direct-API seed mismatch rejection, implementation hashes, occupied-output and over-budget rejection passed. The exact launch configuration above passed artifact/destination dry-run without loading models, accessing the DB/network or creating output.

The full historical run remains owner-launched. This delivery does not establish sampler improvement or local source completeness for the full window. Existing production defaults, model artifacts, ordinary BL, and unrelated concurrent work were not changed.
