# MLB Pitcher K Refresh Lane

## Goal
Restore a clean pitcher_strikeouts baseline and restart disciplined ablation/iteration before any Kalshi promotion decision.

## Why this lane exists
- Pitcher K is behind batter_hits in process maturity.
- Existing production artifact is old (`src/models/mlb/artifacts/production/`, best hyperparams timestamp 2026-03-13).
- Current code already contains pitcher K ablation scaffolding, but the evidence needs to be re-run/frozen under the current quote-clean validation path.
- Historical copula/IP-rate decomposition underperformed; do not revive it as default. Use cheap baselines first.

## Relevant prior lessons/invariants
- Feature selector output is not an ablation; it is only a diagnostic.
- Correlated feature families need force-include / force-exclude family validation.
- Cheap baseline before architecture; avoid copula/survival unless a cheaper experiment proves the need.
- Promotion is ROI/CLV/ranking + calibration + volume gated, not NLL/calibration-only.
- Probabilities must use empirical MC CDF; never Gaussian CDF.
- Do not globally fix Q10 miscalibration; it may be edge-bearing.
- For pitcher K, under-only splits matter. Both-direction summaries can hide the actual edge.

## Current repo state found on 2026-06-07
Read-only inventory only; no model/backtest jobs launched.

### Code paths
- Training: `src/models/mlb/mlb_train_pipeline.py`
- Feature contract / allowlist / lockout: `src/models/mlb/features/contracts.py`
- Canonical validation harness: `src/backtesting/mlb/run_mlb_sweep.py`
- Production model loader: `src/models/mlb/mlb_model_suite.py`

### Existing training controls
`src/models/mlb/mlb_train_pipeline.py` already supports:
- `--ablation-variant none`
- `--ablation-variant static_no_l30`
- `--ablation-variant hook_only`
- `--ablation-variant ip_only`
- `--ablation-variant ip_hook`
- single hook variants: `hook_avg_ip_l30`, `hook_short_hook_l30`, `hook_deep_start_l30`

The training pipeline currently:
- trains from explicit `PITCHER_K_TRAINING_FEATURES`, not all numeric columns;
- locks out rejected Phase 3A lineup/contact/umpire fields via `PITCHER_K_PHASE3A_REJECTED_FEATURES`;
- requires Phase 3B features to be present before training;
- can force ablation feature groups into every K quantile for the hard-coded IP/hook variants.

### Existing artifacts
- Production: `src/models/mlb/artifacts/production/`
  - `pitcher_k_model.joblib`
  - `pitcher_k_feature_config.joblib`
  - `pitcher_k_best_hyperparams.json`
  - best hyperparams timestamp: `2026-03-13T20:02:03.919572`
- May validation artifacts:
  - `src/models/mlb/artifacts/validation_2025_static_no_l30/mlb_run_20260513_131137/`
  - `src/models/mlb/artifacts/validation_2025_hook_deep_start_l30/mlb_run_20260513_131302/`
- May IP/hook ablation artifacts:
  - `src/models/mlb/artifacts/ip_ablation_hook_only/mlb_run_20260513_121138/`
  - `src/models/mlb/artifacts/ip_ablation_ip_only/mlb_run_20260513_121143/`
  - `src/models/mlb/artifacts/ip_ablation_ip_hook/mlb_run_20260513_121153/`
  - `src/models/mlb/artifacts/ip_ablation_hook_deep_start_l30/mlb_run_20260513_130657/`

### Verified locally
- Syntax compile passed:
  - `src/models/mlb/mlb_train_pipeline.py`
  - `src/models/mlb/features/contracts.py`
  - `src/backtesting/mlb/run_mlb_sweep.py`
- `run_mlb_sweep.py --help` confirms current flags:
  - `--quote-clean`
  - `--quote-cutoff-time-et`
  - `--quote-decision-policy`
  - `--line-source`
  - `--book-routing-policy`
  - `--direction`

## Phase 0: freeze/restore baseline evidence
Do this before new feature work.

### 0.1 Baseline artifact choices
Compare these in order:
1. Current production artifact: `src/models/mlb/artifacts/production`
2. May static-no-L30 baseline: `src/models/mlb/artifacts/validation_2025_static_no_l30/mlb_run_20260513_131137`
3. May hook-deep-start baseline: `src/models/mlb/artifacts/validation_2025_hook_deep_start_l30/mlb_run_20260513_131302`

Decision-grade benchmark should use configs with `n_bets >= 100`.

### 0.2 Manual commands for Chase
Run from PowerShell at repo root: `cd C:\Users\Chase\Projects\GameFlowData`

Production artifact, raw under-only:
```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy preferred_book_first --model-dir src\models\mlb\artifacts\production --stats pitcher_strikeouts --direction under --start 2026-04-13 --end 2026-05-10 --tau none --edge 0.02 0.05 0.08 0.10 0.12 0.15 --flat 100 --output-dir backtest_results\mlb_pitcher_k_baseline\production_raw_under
```

Production artifact, focused BL under-only:
```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy preferred_book_first --model-dir src\models\mlb\artifacts\production --stats pitcher_strikeouts --direction under --start 2026-04-13 --end 2026-05-10 --tau 0.5 0.75 0.9 --edge 0.02 0.03 0.04 0.05 0.06 0.08 --z-max 0.25 0.5 --max-weight 0.50 0.65 0.80 --flat 100 --output-dir backtest_results\mlb_pitcher_k_baseline\production_bl_under
```

Repeat the same two commands with these `--model-dir` values:
- `src\models\mlb\artifacts\validation_2025_static_no_l30\mlb_run_20260513_131137`
- `src\models\mlb\artifacts\validation_2025_hook_deep_start_l30\mlb_run_20260513_131302`

Optional over-only diagnostic after under baseline is frozen:
```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy preferred_book_first --model-dir src\models\mlb\artifacts\production --stats pitcher_strikeouts --direction over --start 2026-04-13 --end 2026-05-10 --tau none --edge 0.02 0.05 0.08 0.10 0.12 0.15 --flat 100 --output-dir backtest_results\mlb_pitcher_k_baseline\production_raw_over
```

## Phase 1: retrain clean current baseline
Use this only after baseline replay is done or if existing artifacts fail to load.

Training command, no long architecture, current feature allowlist:
```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_train_pipeline.py --local --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --feature-tolerance 0.02 --ablation-variant none --output-dir src\models\mlb\artifacts\pitcher_k_refresh_baselines
```

Static/no L30 baseline:
```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_train_pipeline.py --local --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --feature-tolerance 0.02 --ablation-variant static_no_l30 --output-dir src\models\mlb\artifacts\pitcher_k_refresh_baselines
```

Hook-only/downside baseline:
```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_train_pipeline.py --local --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --feature-tolerance 0.02 --ablation-variant hook_deep_start_l30 --output-dir src\models\mlb\artifacts\pitcher_k_refresh_baselines
```

Do not use `--copula` for the first restoration pass.

## Phase 2: first ablation/iteration grid
Once Phase 0/1 gives a winner, run true feature-family comparisons:

1. `static_no_l30` — removes L30 team hook features.
2. `hook_only` — forces all L30 hook features.
3. `hook_deep_start_l30` — forces only deep-start rate.
4. `ip_only` — forces predicted IP feature-source family without hook features.
5. `ip_hook` — forces predicted IP family plus hook family.

Interpretation gates:
- Feature pickup: inspect `feature_manifest.json` and `ip_feature_source_metadata.json` if present.
- Calibration: hard fail if severe quantile gaps; do not reward cosmetic calibration if ROI worsens.
- Backtest: lead with under-only configs with `n_bets >= 100`.
- CLV/ranker: do not promote until CLV rank/order agrees; ROI alone is not enough for staking.

## Phase 3: code gap if we want batter_hits-style generic family ablation
Current pitcher K supports hard-coded IP/hook ablation variants, but not generic `--force-include-families` / `--force-exclude-families` like batter_hits.

If Phase 2 shows we need broader family iteration, implement a small pitcher feature-family registry and CLI:
- add pitcher family definitions: workload/leash, pitcher_stuff, opponent_contact, market, environment, inning_fatigue, phase3b_downside;
- add `--force-include-families`, `--force-exclude-families`, `--force-include-features`, `--force-exclude-features` to `mlb_train_pipeline.py`;
- persist forced/excluded families/features in run metadata;
- fail loud if force-included features are missing/non-numeric;
- keep Phase 3A rejected lineup/contact/umpire lockout unless explicitly included in a controlled experiment.

This is a code-change phase and should go through a scoped implementation-worker spec before editing.

## Immediate next checkpoint
After Chase runs the Phase 0 production/raw-under and production/BL-under commands, inspect:
- `backtest_results\mlb_pitcher_k_baseline\production_raw_under\sweep_summary.csv`
- `backtest_results\mlb_pitcher_k_baseline\production_bl_under\sweep_summary.csv`

Then decide whether to:
- keep production artifact as benchmark;
- promote a May validation artifact as the benchmark;
- retrain current baseline first;
- or implement generic family controls before more iteration.
