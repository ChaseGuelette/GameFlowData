# Handoff 055

> Part of [[Handoffs]]

**Date**: May 1, 2026 at 9:28 AM

## Summary

Implemented the full MLB Feature Improvements plan (Batch 0 ablation + all 9 Batch 1 features) across 6 specs. All code paths wired (training, batch inference, single-game inference) for both pitcher K and batter hits models. New umpire scraper and DB table created. Ablation test revealed that the binomial training pipeline ignores feature lists — uses its own NLL-based selection.

## What Was Done

**Spec A — Ablation + Trivial Wiring** (GLM via OpenCode):
- Removed `batter_avg_hr_szn`, `batter_avg_r_l5`, `batter_avg_rbi_szn` from `BATTER_BASE_FEATURES`
- Added `batter_gb_pct_l10`, `batter_fb_pct_l10` to `BATTER_HITS_FEATURES`
- Added `pitcher_pitches_per_ip_l5` to `PITCHER_K_FEATURES`
- Files: `mlb_batter_feature_store.py`, `mlb_feature_store.py`

**Spec B — First-5-IP K Rate + Pitcher Interactions** (GLM via OpenCode):
- Added `pitcher_avg_k_first_5ip_l5` via inn_agg LATERAL JOINs (training, batch, inference)
- Created `_add_interaction_features()`: `pitcher_k_opp_k_interaction`, `pitcher_whiff_opp_whiff_interaction`
- Wired into `mlb_train_pipeline.py` after matchup enrichment
- Files: `mlb_feature_store.py`, `mlb_train_pipeline.py`

**Spec C — BABIP Against + Batter Interactions** (direct edit — OpenRouter credits exhausted):
- Added `opp_pitcher_babip_against_l5` to opposing starter bulk + single-game queries
- Created `_add_batter_interaction_features()`: `batter_babip_opp_babip_interaction`, `projected_ab_x_recent_form`
- Fixed BABIP default to 0.300 in `enrich_with_matchup_features()`
- Files: `mlb_batter_matchup_features.py`, `mlb_batter_feature_store.py`, `mlb_batter_train_pipeline.py`

**Spec D — Pitch Repertoire Diversity** (direct edit):
- Added `pitcher_fastball_pct_l5`, `pitcher_breaking_pct_l5`, `pitcher_offspeed_pct_l5` to statcast LATERAL JOINs
- Added `pitcher_num_pitch_types_l5` (count pitch types > 5% usage)
- File: `mlb_feature_store.py`

**Spec E — Opposing Lineup Features** (direct edit):
- Created `get_lineup_k_features()` and `compute_lineup_features_bulk()` in `mlb_matchup_features.py`
- Added `projected_lineup_k_pct` and `pct_opp_lineup_same_hand` to pitcher model
- IMPORTANT: Fixed column name mismatches (`game_pk` not `game_id`, `is_pitcher` not `is_starting`)
- Files: `mlb_matchup_features.py`, `mlb_feature_store.py`

**Spec F — Umpire Infrastructure** (direct edit):
- Applied DB migration `create_mlb_game_umpires` (PK on game_id+position, 2 indexes)
- Created `src/scrapers/mlb/mlb_umpire_scraper.py` (backfill support, dry-run, --local)
- Created `src/orchestration/mlb_umpire_scraper_job.py`
- Added `umpire_avg_k_per_game_l20` to both models (all 3 code paths each)
- Created `_get_umpire_features()` and `_compute_umpire_features_bulk()` in both feature stores
- Scheduler: 9:36 AM ET daily

**Verification**:
- PITCHER_K_FEATURES: 50 features (was ~39)
- BATTER_BASE_FEATURES: 77 features
- BATTER_HITS_FEATURES: 84 features
- No duplicates, all modules import cleanly, all methods verified

## Decisions Made

1. **Ablation doesn't affect training**: The binomial pipeline's `ImprovedFeatureSelector.select_features_binomial_nll()` picks from ALL DataFrame columns (line 228-231 of `mlb_batter_train_pipeline.py`), not from `BATTER_HITS_FEATURES`. Removing features from the feature list only affects inference. The selector independently kept `batter_avg_hr_szn` and `batter_avg_r_l5` (they do reduce NLL), and already excluded `batter_avg_rbi_szn`.

2. **Feature lists gate inference only**: `BATTER_HITS_FEATURES` and `PITCHER_K_FEATURES` are used for single-game inference column selection, not training. Training uses NLL-based selection from all available columns.

3. **Umpire bulk query design**: Uses window function approach (`ROW_NUMBER() OVER PARTITION BY game_id ORDER BY game_date DESC`) for efficient bulk computation rather than per-game LATERAL JOIN in SQL.

4. **OpenRouter credits exhausted during Spec C**: Fell back to direct implementation for Specs C, D, E, F. All completed successfully.

## Blockers and Open Questions

1. **Ablation test inconclusive**: The binomial pipeline ignores feature lists for training. Two options:
   - (A) Filter `candidates` in pipeline to intersect with feature list — forces ablation
   - (B) Trust the NLL selector — it's data-driven and arguably better than manual ablation
   - Needs user decision before proceeding

2. **Umpire data not backfilled**: `mlb_game_umpires` table exists but is empty. Must run backfill before training with umpire features: `python src/scrapers/mlb/mlb_umpire_scraper.py --backfill --start-date 2023-04-01 --local`

3. **New features may not survive selection**: Even after adding 11+ pitcher features and 7+ batter features, the NLL selector may not pick them. Need to retrain and check the selected feature set.

4. **Copula model still pending**: Previous session identified copula artifacts at `mlb_run_20260428_164726/` not yet promoted. Copula vs single-model sweep comparison still needed.

## Recommended Next Steps

1. **Backfill umpire data** — Run `python src/scrapers/mlb/mlb_umpire_scraper.py --backfill --start-date 2023-04-01 --local` to populate 2023-2025 historical data
2. **Decide on ablation approach** — Filter pipeline candidates to feature list (option A) or trust NLL selector (option B)
3. **Retrain both models** with all Batch 1 features:
   - Batter: `python src/models/mlb/mlb_batter_train_pipeline.py --local --stat hits --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100`
   - Pitcher: `python src/models/mlb/mlb_train_pipeline.py --local --stat pitcher_strikeouts --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100`
4. **Backtest sweep** on 2026 OOS window to measure aggregate improvement
5. **Copula sweep comparison** — compare copula vs single-model pitcher K performance

## Files to Read on Resume

- [[MLB-Feature-Improvements]] — the master plan for all batch 1 features
- `src/models/mlb/mlb_feature_store.py` — pitcher K feature store (50 features, all specs wired)
- `src/models/mlb/mlb_batter_feature_store.py` — batter feature store (84 hits features, all specs wired)
- `src/models/mlb/mlb_batter_train_pipeline.py` lines 222-242 — the NLL-based feature selection that ignores feature lists
- `src/scrapers/mlb/mlb_umpire_scraper.py` — new umpire scraper (needs backfill)
