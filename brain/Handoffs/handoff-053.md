> Part of [[Handoffs]]

**Date**: April 28, 2026 at 1:38 PM

## Summary

Major MLB model architecture overhaul session. Implemented three structural changes: (1) decomposed pitcher strikeouts into IP + K-rate sub-models joined by Gaussian copula (same pattern as NBA minutes x rate), (2) added a NegBin AB prediction model for compound Binomial hit sampling, and (3) wired 13 new features from existing but unused tables (pitcher inning-level fatigue + opposing bullpen workload). All code complete across 7 files (+1,011 lines), all passing syntax checks. Not yet retrained or deployed.

## What Was Done

- **Pitcher feature store** (`mlb_feature_store.py`): Added 6 inning-level fatigue features via LATERAL JOIN on `mlb_pitcher_inning_stats`. Added `actual_ip` as target column for copula training. New `_get_inning_fatigue_stats()` inference helper.
- **Batter feature store** (`mlb_batter_feature_store.py`): Added 7 features (4 bullpen workload from `mlb_bullpen_daily_status`, 3 opposing pitcher inning stats). New `_get_opposing_bullpen_stats()` helper. Wired bulk merge for opposing pitcher inning features in `enrich_with_matchup_features()`.
- **Matchup features** (`mlb_batter_matchup_features.py`): Added `get_opposing_pitcher_inning_stats()` (single-game) and `compute_opposing_pitcher_inning_bulk()` (training) functions.
- **Monte Carlo predictors** (`mlb_monte_carlo.py`): Added `MLBPitcherKCopulaPredictor` (IP x K-rate Gaussian copula, 141 lines) and `MLBCompoundBinomialPredictor` (AB NegBin + hit Binomial, 114 lines).
- **Pitcher training pipeline** (`mlb_train_pipeline.py`): Added `--copula` flag, IP/K-rate sub-model training, Spearman rho computation, copula artifact saving. Also trains single model for A/B comparison.
- **Batter training pipeline** (`mlb_batter_train_pipeline.py`): Added AB NegBin training step (Step 3b) before Binomial hit model in `_run_binomial_pipeline`.
- **Model suite** (`mlb_model_suite.py`): Copula pitcher K loading (preferred) with single-model fallback. Compound Binomial loading (preferred) with standard Binomial fallback.
- **Decisions doc**: Created `brain/Decisions/MLB-Model-Architecture-Overhaul-Apr28.md` with full rationale for all three architectural changes.

## Decisions Made

1. **Copula decomposition for pitcher K** — Same proven pattern as NBA (minutes x rate). IP model captures workload/fatigue, K-rate model captures stuff quality. Spearman rho computed on training data (filter IP >= 3 to exclude short relief). Old single model preserved as fallback.
2. **Compound Binomial for hits** — AB modeled as NegBin distribution (no exposure), feeds into Binomial(AB, p). Propagates AB uncertainty into hit tails. Falls back to point-estimate Binomial if no AB model artifacts.
3. **13 new features from existing tables** — `mlb_pitcher_inning_stats` (scraped, synced) and `mlb_bullpen_daily_status` (computed daily) were sitting unused. Now wired into both feature stores. Feature selection will determine which ones the models actually use.

## Blockers and Open Questions

- **Not yet retrained** — all code is local, no model artifacts exist for the new architecture yet
- **No index on `mlb_pitcher_inning_stats(player_id, is_starter, game_date)`** — the LATERAL JOINs will be slow without it. Need a CONCURRENT index migration before heavy training.
- **K-rate target handling is a hack** — both sub-models swap target columns to `actual_so` because `MLBPitcherKPipeline` hardcodes that name internally. Works but fragile.
- **`select_features_negbin_nll` may not exist** on `ImprovedFeatureSelector` — the AB training has a correlation-based fallback if it doesn't.
- **Duplicate `_get_opposing_bullpen_stats`** in batter feature store was cleaned up during session but verify no duplicates remain.

## Recommended Next Steps

1. **Create index** on `mlb_pitcher_inning_stats(player_id, is_starter, game_date DESC)` — CONCURRENTLY
2. **Sync local DB**: `python scripts/sync_local_db.py --tables mlb_pitcher_inning_stats mlb_bullpen_daily_status`
3. **Retrain pitcher K (copula)**: `python src/models/mlb/mlb_train_pipeline.py --local --train-seasons 2024 2025 --cal-season 2025 --cal-end-date 2025-07-01 --copula --tune --tuning-trials 100`
4. **Retrain batter hits (compound)**: `python src/models/mlb/mlb_batter_train_pipeline.py --local --stat hits --train-seasons 2024 2025 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100`
5. **Validate**: IP Q50 ~5-6 for aces, K-rate Q50 ~1.0-1.5 for K pitchers, AB predictions 2-5 range
6. **Backtest**: Compare copula vs single, compound vs standard on 2025 holdout
7. **Deploy**: If backtests pass, copy artifacts to production/ and deploy to Railway

## Files to Read on Resume

- [[MLB-Model-Architecture-Overhaul-Apr28]] — Full rationale and implementation details
- [[Execution-Plan]] — Check Phase 1 sub-steps for model retraining
- `src/models/mlb/mlb_monte_carlo.py` — The two new predictor classes
- `src/models/mlb/mlb_train_pipeline.py` — The copula training flow (search for `--copula`)
- `src/models/mlb/mlb_batter_train_pipeline.py` — The AB NegBin training step (search for `Step 3b`)
