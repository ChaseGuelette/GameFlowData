# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial project setup with Session-Driven Development
- Market neutralization diagnostic (A1) — regression + Brier score analysis on predictions.csv
- Comprehensive roadmap in ACTIONITEMS.md with Tracks A–E
- **Black-Litterman probability blending layer (A3)** — new module `src/models/black_litterman.py`
  - `BlackLittermanBlender` class with `BLConfig` dataclass
  - Log-odds space blending of model probabilities with devigged market prior
  - Per-prediction z-score confidence from MC distribution properties
  - Multiplicative devigging (equivalent to Shin's method for 2-outcome markets)
  - American-to-decimal odds conversion utility
  - 39 unit tests in `tests/test_black_litterman.py`
- `--bl-tau` CLI flag on `run_backtest.py` to enable BL blending (disabled by default)
- `posterior_prob` diagnostic field on `Bet` dataclass in `bet_simulator.py`
- BL diagnostic columns in predictions CSV: `model_over/under`, `market_over/under`, `confidence`, `posterior_over/under`
- **Prop line centering features (A4)** — per-stat player prop lines as rate model features
  - `prop_line_pts`, `prop_line_reb`, `prop_line_ast`, `prop_line_threes` added to `RATE_FEATURES_*` lists
  - LATERAL JOIN to `raw_player_props_combined` in all 4 feature store query paths
  - New `_get_player_prop_lines()` helper for single-player inference path
  - Database index `idx_props_player_game` on `(player_id, game_id)` for performance
- **B2/B3/B4: Rest, Trend, and Minutes Stability features** — 20 new model features
  - **B2 (Rest/Schedule):** `rest_days`, `is_back_to_back`, `games_in_last_7_days` added to `MINUTES_FEATURES` and all 4 `RATE_FEATURES_*` lists
  - **B3 (Short-Window Trends):** L3 rolling averages (`player_avg_{stat}_l3`), momentum ratios (`player_{stat}_l3_l15_ratio`), and L5 std deviations (`player_std_{stat}_l5`) added to `RATE_FEATURES_*` and `MINUTES_FEATURES`
  - **B4 (Minutes Stability):** `player_min_std_l5`, `player_min_floor_l5`, `player_games_started_l5` added to `MINUTES_FEATURES`
  - 14 new columns in `player_average_game_stats` table
  - New `calculate_b2_b3_b4_features()` in `populate_average_stats.py` with shift(1) no-leakage pattern
  - All 4 feature store query paths updated with consistent SQL
  - 4 new tests for B2/B3/B4 computation (no-leakage, std, rest_days, games_started)
- **B1: Injury/lineup context features** — 10 new features from `rapidapi_injuries` table
  - Teammate injuries: `team_out_count`, `team_out_min_sum`, `team_out_pts_sum`, `team_out_reb_sum`, `team_out_ast_sum`, `team_out_usg_sum`
  - Opponent injuries: `opp_out_count`, `opp_out_min_sum`
  - Player status: `player_is_questionable`, `player_is_probable`
  - SQL LATERAL JOINs in `feature_store.py` with temporal integrity (report_date ≤ game_date)
  - Added to all 4 `RATE_FEATURES_*` lists and `MINUTES_FEATURES`
- **Injury data pipeline** — RapidAPI historical backfill + fuzzy player linking
  - `src/scrapers/rapidapi_injury_backfill.py` — backfills injury data from 2021-present (88K+ rows)
  - `src/processing/link_injury_data.py` — 3-tier name matching cascade (manual CSV → exact → fuzzy SequenceMatcher)
  - `data/linker_data/player_mappings.csv` — 11 manual mappings for truncated API names (suffixes like "III", "Jr.")
  - Database cleanup: 142 garbage rows deleted, 99.3% of injury records fully linked
- **C0: Gaussian copula for minutes-rate correlation** — replaces legacy post-hoc adjustment
  - `MonteCarloPredictor` accepts `copula_params: dict[stat → Spearman ρ]`
  - `_predict_copula()`: shared z_minutes, per-stat correlated z_rate via Cholesky decomposition
  - Preserves both marginal distributions exactly while inducing correct rank dependency
  - `compute_copula_params_from_data()` and `load_copula_params()` utility functions
  - Training pipeline computes and saves `copula_params.json` as artifact
  - `run_backtest.py` and `run_daily.py` auto-load copula params from model artifacts
  - Falls back to legacy adjustment when copula params unavailable (backward compat)
- **Backtest dashboard** — expanded `visualize_results.py` from 163 to 925 lines
  - Self-contained HTML with Plotly charts (CDN) + vanilla JS for sorting/filtering
  - Sections: portfolio performance, metrics summary cards, enriched bet log, bookmaker line comparison
  - DB enrichment: resolves player_id/team_id/game_id to names/matchups via `player_game_stats` + `players` + `teams`
  - Graceful degradation for missing columns (bookmaker, posterior_prob) and missing data files

### Changed

- Updated ACTIONITEMS.md with corrected root cause diagnosis: model is catastrophically overconfident (Brier 0.2705), not market-correlated (R²=0.10)
- Promoted Black-Litterman blending (A3) to top priority based on diagnostic findings
- Reorganized priority matrix with A1 marked complete
- `backtest_harness.py`: Added `bl_blender` field and dual-path `_calculate_edges()` — BL path when blender is set, original path when None
- `run_backtest.py`: Added `--bl-tau` argument and BL blender construction
- `bet_simulator.py`: Added `posterior_prob` field to `Bet`, wired posterior storage in `evaluate_predictions()`
- Updated ARCHITECTURE.md with Stage E (Probability Blending), updated data flow diagram, backtesting CLI docs, and Known Issues section
- Marked A3 as implemented in ACTIONITEMS.md priority matrix
- **A2**: Removed `line_total` from `RATE_FEATURES_PTS` to eliminate market leakage (remains in `MINUTES_FEATURES`)
- **A4**: `feature_store.py` — added LATERAL JOINs and `prop_line_*` SELECT columns to `get_training_dataset()`, `get_features_for_date()`, `get_features_for_date_range()`; wired `_get_player_prop_lines()` into `get_player_game_features()`
- Marked A2 and A4 as implemented in ACTIONITEMS.md priority matrix
- Updated ARCHITECTURE.md Feature Store section with prop line centering documentation
- **B2/B3/B4**: `feature_store.py` — updated all 5 feature lists, all 3 bulk SQL queries, `_get_player_rolling_stats()`, and `get_player_game_features()` for new features
- **B2/B3/B4**: `populate_average_stats.py` — refactored `rolling_with_groupby()` to support `agg` parameter (std/min/sum), updated insert column list
- Updated ARCHITECTURE.md Feature Store section with B2/B3/B4 documentation
- **monte_carlo.py**: Added `copula_params` to `__init__`, new `_predict_copula()` method, `_build_extended_quantile_fn()`, `_map_uniforms_to_samples()` helpers, updated `predict_batch_for_date()` with copula branch
- **train_pipeline.py**: Added `_compute_copula_params()` step to training pipeline, imports `compute_copula_params_from_data`
- **run_backtest.py**: Auto-loads `copula_params.json` from model artifacts, passes to `MonteCarloPredictor`
- **run_daily.py**: Same copula auto-loading for daily inference pipeline
- Updated ARCHITECTURE.md with injury data, copula sampling, dashboard, and injury linker documentation
- Updated ACTIONITEMS.md: B1 marked done, C0 (copula) added, A6 (conditional rate modeling) added as future option

### Fixed

- **B2/B3/B4**: Fixed `AttributeError: Can only use .dt accessor with datetimelike values` in `calculate_b2_b3_b4_features()` — DB returns `date` objects, not `datetime64`. Added `pd.to_datetime()` conversion before date arithmetic in both `calculate_b2_b3_b4_features()` and `_count_games_in_window()`
- **Feature Store**: Fixed hardcoded zeros bug in `get_features_for_date_range()` — `rest_days` and `is_back_to_back` were being overwritten to 0 instead of using SQL-computed values
- **MCP Config**: Fixed `.mcp.json` RapidAPI server entry for Windows — changed `npx` to `cmd /c npx` wrapper pattern

### Removed

- Removed `_get_travel_and_rest_features()` from `FeatureStore` — rest features now pre-computed in DB via backfill script
- Removed `_get_travel_features_single()` from `FeatureStore` — same reason
- Removed `TEAM_LOCATIONS` dict and `_haversine()` static method from `feature_store.py` — no longer needed after travel feature removal
- Removed `numpy` import from `feature_store.py` — no longer used

### Changed
- Refactored project structure and moved files
- Updated test_backfill_league_priors.py
