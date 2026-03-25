# Handoff 004 — Binomial Model for MLB Batter Hits

> Part of [[Handoffs]]

**Date**: March 24, 2026 at 10:20 PM

## Summary
Built a complete Binomial model for MLB batter hits to replace the mismatched NegBin model. Hits data is underdispersed (variance < mean), making NegBin a poor fit. The binomial model treats hits as successes in at-bats with a custom XGBoost objective. Also built the Optuna hyperparameter tuner and verified all 5 batter stat trainers (hits, total_bases, rbis, runs, home_runs) are ready to run.

## What Was Done
- **`src/models/binomial_model.py`** (NEW) — BinomialModel class with custom XGBoost objective (logit link, at-bats via DMatrix weights), gradient `n*p - y`, hessian `n*p*(1-p)`. Closed-form probability via `binom.cdf`. Save/load artifact persistence.
- **`src/models/binomial_tuner.py`** (NEW) — Optuna hyperparameter tuner mirroring NegBinHyperparameterTuner. Searches max_depth, min_child_weight, learning_rate, n_estimators, subsample, colsample_bytree, early_stopping.
- **`src/models/mlb/mlb_batter_feature_store.py`** — Added `at_bats` to SQL queries (training + batch inference), added `projected_ab` derived feature from `batter_avg_ab_l5`.
- **`src/models/mlb/mlb_batter_train_pipeline.py`** — Added routing (hits->binomial, HR->binary, else->negbin), `_run_binomial_pipeline()`, `_resolve_binomial_config()`, `_compute_binomial_calibration()`.
- **`src/processing/feature_selection.py`** — Added `select_features_binomial_nll()`, `_rank_features_binomial_nll()`, `_optimize_count_binomial_nll()`.
- **`src/models/mlb/mlb_monte_carlo.py`** — Added `MLBBinomialPredictor` class for inference.
- **`src/models/mlb/mlb_model_suite.py`** — Updated `from_directory()` to check binomial first, fall back to NegBin.
- **`src/models/mlb/mlb_stat_config.py`** — Changed `batter_hits` model_type from `negbin` to `binomial`.

## Decisions Made
- **Binomial over NegBin for hits** — Hits are successes in at-bats (underdispersed). NegBin assumes overdispersion and was failing: constant alpha, -5.1% calibration gap.
- **Actual AB for training, projected AB for inference** — Avoids same-game leakage. `projected_ab` = `batter_avg_ab_l5` (rolling L5 average).
- **Closed-form probabilities** — `1 - binom.cdf(floor(line), n, p)` instead of MC simulation for probability calculations. MC samples still used for empirical CDF in edge calculator.
- **Poisson proxy for feature ranking** — Poisson mu approximates `n*p`, fast enough for feature screening. Binomial NLL used for final scoring.
- **CLI stat key is `runs` not `runs_scored`** — The mapping goes: CLI `runs` -> market key `batter_runs_scored` -> feature store stat `runs_scored`.

## Blockers and Open Questions
- No batter backtest harness exists yet (only pitcher). Will need one to validate batter model performance before going live.
- Training has not been run yet — all 5 training commands are ready but need to be executed.

## Recommended Next Steps
1. **Train all 5 batter models** — Run the training commands in [[MLB-Model]] (hits first to validate binomial, then TB, RBI, runs, HR). Start with `--tune --tuning-trials 50` to save time, increase if results look good.
2. **Build batter backtest harness** — Extend `mlb_backtest_harness.py` to support batter predictions so models can be validated.
3. **Backtest and compare** — Run backtests for each stat, compare calibration metrics and ROI.
4. **Stripe integration** — Phase 3 is the next product-level priority after MLB pipeline.

## Files to Read on Resume
- [[MLB-Model]] — Updated with binomial architecture, model routing table, and training commands
- [[Execution-Plan]] — Step 1.3 status updated
- `src/models/binomial_model.py` — Core binomial model implementation
- `src/models/mlb/mlb_batter_train_pipeline.py` — Training pipeline with routing logic
