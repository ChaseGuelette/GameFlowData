# Handoff 002 — MLB Batter Pipeline Aligned with Distributional Model

> Part of [[Handoffs]]

**Date**: March 24, 2026 at 2:05 PM

## Summary

Redesigned the MLB batter training pipeline to properly evaluate what the NegBin distributional model actually outputs. Replaced quantile-based metrics (misleading for discrete distributions) with PMF/CDF-based calibration, swapped per-quantile pinball loss feature selection for NLL-based selection, and built an Optuna hyperparameter tuner specifically for the NegBin model. The pipeline is now ready for initial model training.

## What Was Done

### Code Created
- `src/models/negbin_tuner.py` — New Optuna hyperparameter tuner for NegBin models. Trains full 2-output XGBoostLSS per trial, optimizes validation NB NLL, returns `NegBinConfig`.

### Code Modified
- `src/processing/feature_selection.py` — Added 3 new methods to `ImprovedFeatureSelector`: `select_features_nll()`, `_rank_features_nll()`, `_optimize_count_nll()`. Uses Poisson proxy + NB NLL scorer.
- `src/models/mlb/mlb_batter_train_pipeline.py` — Replaced:
  - Step 3: `select_features_per_quantile()` → `select_features_nll()`
  - Steps 6-7: 40-min MC sampling loop → vectorized PMF/CDF calibration (~5 sec)
  - Step 8: Quantile sanity check → distributional parameter check (mu, alpha, P(over))
  - Added Step 4: Optional Optuna tuning via `_resolve_negbin_config()`
  - Added `_compute_negbin_calibration()` static method

### Brain Updated
- [[MLB-Model]] — Status upgraded, key files table expanded, architecture section added
- [[Model-Architecture-Decisions]] — Added decisions #16 (NLL feature selection), #17 (PMF calibration), #18 (NegBin Optuna tuner)
- [[Execution-Plan]] — Step 1.1 marked completed, Step 1.3 moved to in_progress

## Decisions Made

1. **NLL over pinball loss for feature selection**: Pinball loss is for quantile models. The NegBin model is distributional — NLL is its native loss function. Using a Poisson proxy keeps selection fast (~2 min) while scoring by the correct metric.

2. **PMF/CDF over MC sampling for calibration**: The old Q10 "gap of +33%" was a measurement artifact, not a model failure. Discrete distributions where 40% of outcomes are 0 will always have high Q10 coverage — it's structural. PMF-based metrics (NLL, bias, zero fraction, per-line P(over)) give actionable information.

3. **Separate tuner for NegBin**: The existing `QuantileHyperparameterTuner` optimizes calibration gaps across quantile levels — wrong objective entirely for a distributional model. The new tuner optimizes NB NLL directly with the real custom XGBoost objective.

## Blockers and Open Questions

- None blocking. Pipeline is ready for training.
- Open question: how many Optuna trials are needed for convergence? Start with 50-100, check if NLL plateaus.

## Recommended Next Steps

1. **Train total_bases model** (highest priority — already partially validated):
   ```
   python src/models/mlb/mlb_batter_train_pipeline.py --stat total_bases --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100
   ```
2. **Train hits model**:
   ```
   python src/models/mlb/mlb_batter_train_pipeline.py --stat hits --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100
   ```
3. Review calibration reports — check NLL, bias ratio near 1.0, per-line gaps
4. Lock best hyperparams from initial runs for future retrains
5. Move to rbis/runs stats after hits and total_bases are validated
6. Begin Step 1.6: backtesting with `run_mlb_sweep.py`

## Files to Read on Resume

- [[MLB-Model]] — Current state of MLB pipeline and architecture
- [[Model-Architecture-Decisions]] — Decisions #16-18 for rationale
- [[Execution-Plan]] — Phase 1 progress tracking
- `src/models/mlb/mlb_batter_train_pipeline.py` — The pipeline that's ready to run
- `src/models/negbin_tuner.py` — The new Optuna tuner
