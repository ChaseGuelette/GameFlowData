# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2026-02-09 Session 17] — Windows Task Scheduler Fixes & Incremental Stats

### Fixed

- **Batch script virtual environment path** — Changed `.venv` to `venv` in all 3 scripts:
  - `scripts/run_daily_stats.bat`
  - `scripts/run_lines.bat`
  - `scripts/run_inference.bat`
- **PYTHONPATH for subprocess imports** — Added `set PYTHONPATH=C:\Users\Chase\Projects\GameFlowData` to all batch scripts to fix `ModuleNotFoundError: No module named 'src'`
- **Log file permission conflicts** — Removed shell redirect (`>> logs\*.log 2>&1`) from batch scripts since Python's FileHandler handles logging
- **SQL syntax error in `update_player_position_history.py`** — Changed `:snap_date::DATE` to `CAST(:snap_date AS DATE)` to avoid SQLAlchemy parameter binding conflict with PostgreSQL cast syntax

### Added

- **`src/processing/populate_average_stats_incremental.py`** (~325 lines):
  - Lightweight daily version of rolling average calculation
  - Only processes players who played on target date (vs all players)
  - Fetches last 20 games per player (vs full history)
  - Uses UPSERT instead of TRUNCATE + reload
  - **Performance: 1.0s vs 1709s (28.5 min) — 1700x speedup**

### Changed

- **`src/orchestration/daily_stats_job.py`** — Step 6 now uses `populate_average_stats_incremental.py` instead of full recalculation

### Verified

- All 5 production scheduled tasks working:
  - `GameFlow-DailyStats` (9:00 AM)
  - `GameFlow-Lines-12PM` (12:00 PM)
  - `GameFlow-Lines-4PM` (4:00 PM)
  - `GameFlow-Lines-6PM` (6:00 PM)
  - `GameFlow-Inference` (6:30 PM)
- Full daily stats job completed successfully in 53 minutes total

---

## [2026-02-09 Session 16] — Windows Task Scheduler Automation

### Added

- **Windows Task Scheduler batch scripts** in `scripts/`:
  - `scripts/run_daily_stats.bat` — Wraps `daily_stats_job.py` for Task Scheduler
  - `scripts/run_lines.bat` — Wraps `lines_job.py` for Task Scheduler
  - `scripts/run_inference.bat` — Wraps `inference_job.py` for Task Scheduler
- **5 Windows Scheduled Tasks** for local deployment:
  - `GameFlow-DailyStats` — 9:00 AM daily
  - `GameFlow-Lines-12PM` — 12:00 PM daily
  - `GameFlow-Lines-4PM` — 4:00 PM daily
  - `GameFlow-Lines-6PM` — 6:00 PM daily
  - `GameFlow-Inference` — 6:30 PM daily

### Changed

- **ARCHITECTURE.md** — Added Windows Task Scheduler documentation in Orchestration section

### Analysis

- **Backtest sweep review (2026-02-08):** Analyzed 165 configurations from latest sweep
  - Best config: `tau=0.5, z_max=1.0, edge=0.15, kelly=0.125`
  - Results: $27,379 profit, 10.87% ROI, 57.6% hit rate, 1.21 Sharpe, 427 bets
  - PTS strongest: +17.5% ROI (241 bets)
  - BL blending now works after A3b fix (linear ramp confidence)
  - Edge > 0.20 bucket shows +20% ROI

---

## [2026-02-09 Session 15] — C4 THREES Truncated Negative Binomial Count Model

### Added

- **`TruncatedNegBinModel` class** in `src/models/truncated_negbin.py` (~500 lines):
  - Two-stage architecture: XGBoost regressors predict log(μ) and log(α) for Truncated Negative Binomial
  - Inverse CDF sampling produces integer counts directly (not continuous quantile interpolation)
  - mu/alpha parameterization: μ = mean, α = overdispersion (variance = μ + α×μ²)
  - `fit()`, `predict_params()`, `sample()`, `sample_single()` methods
  - `save()` / `load()` / `exists()` for persistence
  - Configuration via `TruncatedNegBinConfig` dataclass
- **`tests/test_truncated_negbin.py`** — 17 unit tests covering:
  - Model fitting convergence
  - Parameter prediction ranges
  - Integer sampling (all samples ≥ 1)
  - Batch sampling
  - Save/load roundtrip
  - Edge cases (zero values rejected, unfitted model raises)
- **`scripts/validate_threes_negbin.py`** — Phase 0 validation script:
  - Chi-squared goodness-of-fit test for truncated NegBin
  - Segment validation by shooter volume (high/moderate/low 3PA)
  - Results: All segments passed (WMAPE < 5%)
- **`_sample_threes_count()` method** in `src/models/monte_carlo.py`:
  - Bernoulli draw for zero vs positive (independent of copula)
  - Truncated NegBin sampling for positive samples
  - Integer output (0, 1, 2, 3, ...)
- **`_has_threes_count_model()` helper** in `src/models/monte_carlo.py`
- **`_train_threes_count_model()` method** in `src/models/quantile_trainer.py`:
  - Stage 1: XGBoost binary classifier + isotonic calibration for P(zero)
  - Stage 2: TruncatedNegBinModel on positive samples
  - Stores `threes_zero_classifier`, `threes_zero_calibrator`, `threes_count_model`, `threes_zero_feature_names`
- **`_calibrate_count_model()` method** in `src/models/train_pipeline.py`:
  - Zero prediction accuracy diagnostics
  - Quantile coverage evaluation via inverse CDF

### Changed

- **`src/models/quantile_trainer.py`:**
  - `train_rate_models()` — detects `stat == "threes"` and uses count model instead of quantile regression
  - `save_all()` — saves count model artifacts (classifier, calibrator, feature names, count model files)
  - `load_all()` — detects `model_type: "count"` in `threes_is_hurdle.json` and loads count model components
- **`src/models/monte_carlo.py`:**
  - `_predict_copula()` — routes threes through count model before copula processing
  - `predict_batch_for_date()` — handles count model for threes separately from copula stats
- **`src/models/train_pipeline.py`:**
  - `_evaluate_calibration()` — evaluates count model when present
  - `_evaluate_combined_calibration()` — includes threes when count model present

### Fixed

- **`AttributeError: feature_names_in_`** — XGBoost doesn't reliably expose feature names. Fixed by:
  - Storing `threes_zero_feature_names` explicitly during training
  - Saving as `threes_zero_feature_names.joblib`
  - Loading and using in calibration and inference paths

### Technical Details

**Why Truncated Negative Binomial:**
- Made threes are discrete integers (0, 1, 2, 3...) — quantile regression produces continuous values
- Overdispersion: variance ≈ 2.8 vs mean ≈ 2.1 — Poisson would underestimate variance
- Truncation at 0: we only model positive samples (zero classifier handles P(zero) separately)

**Sampling Strategy (inverse CDF, not rejection):**
```python
# Map u in (0,1) to truncated distribution
p_zero_nb = nbinom.pmf(0, n, p)
adjusted_u = u * (1 - p_zero_nb) + p_zero_nb
samples = nbinom.ppf(adjusted_u, n, p)  # integers >= 1
```

**Artifacts (C4 architecture):**
- `threes_zero_classifier.joblib` — XGBoost binary classifier
- `threes_zero_calibrator.joblib` — Isotonic regression for P(zero)
- `threes_zero_feature_names.joblib` — Feature names for zero classifier
- `truncated_negbin_meta.json` — Global mu/alpha, feature names
- `truncated_negbin_mu_model.joblib` — XGBoost regressor for log(μ)
- `truncated_negbin_alpha_model.joblib` — XGBoost regressor for log(α)
- `threes_is_hurdle.json` — Flag file with `model_type: "count"`

### Status

- All 523 tests pass
- Ready for retraining to activate C4 architecture
- Expected to fix the 25.6% Q10 calibration gap from C3

---

## [2026-02-07 Session 14] — Backtesting Data Fixes

### Fixed

- **Incomplete model directory selection** in `src/backtesting/run_sweep.py`:
  - `find_latest_model_dir()` now validates that `minutes_model.joblib` exists before selecting a directory
  - Logs warning when skipping incomplete training run directories
  - Prevents silent failures when an aborted training run leaves an empty artifact directory

- **Game ID format mismatch** affecting backtest line fetching:
  - **Root cause:** `raw_player_props_combined.game_id` stored as 8-digit (e.g., "22500589") vs `player_game_stats.game_id` as 10-digit (e.g., "0022500589")
  - **Fix (query):** Updated prefetch lines query in `src/backtesting/backtest_harness.py` to use `LPAD(rp.game_id, 10, '0') = gd.game_id`
  - **Fix (linker):** Added `.zfill(10)` in `src/processing/nba_linker_local.py` when storing game_ids in lookup dictionaries (3 locations)
  - **Impact:** Lines fetched increased from 33,962 to 191,908 (+465%). Bets increased from 889 to 2,251 (+153%)

- **Pre-existing test failure** in `tests/test_backtest_harness.py`:
  - Added missing `all_edges_df` parameter to `sample_result` fixture
  - `TestBacktestResult::test_to_csv` now passes

### Changed

- Updated `src/backtesting/backtest_harness.py` line 556: JOIN uses `LPAD()` for game_id compatibility
- Updated `src/backtesting/run_sweep.py` lines 560-592: `find_latest_model_dir()` includes validation
- Updated `src/processing/nba_linker_local.py` lines 355-357, 374-377, 916-919: game_id stored with leading zeros

---

## [2026-02-05 Session 13] — E6 Daily Pipeline Automation

### Added

- **Frequency-separated job scripts** for cron scheduling:
  - **`src/orchestration/daily_stats_job.py`** — Once-daily (6 AM ET) NBA game results + full processing pipeline
    - Steps: `nba_unified_scraper.py` → `nba_linker_local.py incremental` → `backfill_team_ids.py` → `update_player_position_history.py` → `update_league_position_averages.py` → `populate_average_stats.py` → `backfill_opponent_allowed.py`
    - Runtime: ~2-5 minutes
    - CLI: `--dry-run` to preview commands without executing
  - **`src/orchestration/lines_job.py`** — Multiple-times-daily (12 PM, 4 PM, 6 PM ET) props + injuries scraping
    - Steps: `daily_game_lines_scraper.py` → `daily_player_props_scraper.py` → `rapidapi_injury_backfill.py` (optional) → `link_injury_data.py` (optional) → `nba_linker_local.py incremental` (optional)
    - Runtime: ~30-90 seconds
    - CLI: `--date`, `--dry-run`, `--skip-injuries`, `--skip-linker`
  - **`src/orchestration/inference_job.py`** — Pre-game (6:30 PM ET) prediction generation
    - Loads model artifacts (auto-detects latest `run_*` directory)
    - Initializes Monte Carlo predictor with 10K samples + Gaussian copula
    - Stores to `daily_predictions` + `daily_prediction_samples` tables
    - Exports CSV backup to `predictions/` directory
    - Runtime: ~1-3 minutes
    - CLI: `--date`, `--dry-run`, `--model-dir`, `--stats`
- **`.session/specs/E6_daily_automation.md`** — Full specification document with:
  - Architecture diagram and timeline
  - Job descriptions and usage examples
  - Environment variable requirements
  - Cron configuration guide (ET → UTC conversion)
  - Monitoring and troubleshooting guide
- **`cron/gameflow_crontab.txt`** — Server cron schedule template with:
  - UTC times for all 5 daily jobs
  - Environment setup instructions
  - Log rotation job (weekly)
  - Documentation comments for manual runs and dry-run testing
- **`logs/` directory** — Job execution log directory with `.gitkeep`
- **`predictions/` directory** — Created by `inference_job.py` for CSV exports

### Changed

- Updated **ARCHITECTURE.md**:
  - Added frequency-separated job scripts table in Orchestration section
  - Added CLI documentation for new job scripts in Daily Workflow section
  - Updated directory structure with `logs/`, `cron/`, `predictions/` directories
  - Updated "Current state" section with E6 completion note
- Updated **ACTIONITEMS.md**:
  - E6 entry marked as implemented with full schedule and remaining Phase 2 work
  - Added Session 13 summary

### Analysis

- **Root cause of missing backtest bets after Jan 9:** The `game_id_map_staging` table lacks mappings for games after Jan 10 because the linker upload step never completed. The `props_game_updates.csv` file has mappings through Jan 23 but they weren't uploaded to the database. Fix: run `python src/processing/nba_linker_local.py upload`.

---

## [2026-02-05 Session 12] — Lightweight Incremental Linker

### Added

- **`link_incremental()` function** in `src/processing/nba_linker_local.py`:
  - Lightweight mode for daily automated linking without downloading full 25M+ row tables
  - Queries only unlinked records (`WHERE player_id IS NULL`)
  - Loads reference tables once (teams, players, team_game_stats)
  - Direct SQL updates via batched queries
  - Fuzzy player name matching with 0.80 threshold and last name bonus
  - Game matching via normalized team names and ±90 day fuzzy window
  - CLI options: `--batch-size` (default 50000), `--limit` (optional cap)
- **`normalize_player()` function** — Moved to module level for reuse across functions
- **Expanded `TEAM_NAME_ALIASES`** — 30 full team name → 3-letter abbreviation mappings (e.g., "Atlanta Hawks" → "ATL") for matching Odds API full names to NBA API abbreviations

### Changed

- **`src/orchestration/run_daily.py`:**
  - Fixed broken linker call (was missing command argument)
  - Now uses `incremental` command: `python src/processing/nba_linker_local.py incremental`

### Updated

- **`tests/test_nba_linker_local.py`:**
  - Updated `test_normalize_team_aliases` — Now expects 3-letter abbreviations instead of full team names

### Test Results

- Player match rate: 99.3% (4,963/5,000 records)
- Game match rate: 40.7% (2,037/5,000 records) — lower because many props are for future games
- Total unlinked records: ~2.8M combo market rows from backfill

---

## [2026-02-05 Session 11] — Fix BL Confidence + THREES Hurdle Model (A3b, C3)

### Added — THREES Hurdle Model (C3)

- **`HurdleQuantileModel` class** in `src/models/quantile_trainer.py`:
  - Two-stage architecture: Stage 1 binary classifier + Stage 2 quantile regression on positive samples
  - Isotonic regression calibration for P(zero) classifier
  - `predict_p_zero()`, `predict_quantiles()` methods with zero/positive combination
  - `_interpolate_positive_quantile()` for adjusted quantile mapping
  - `save()` / `load()` / `is_hurdle_model()` for persistence
- **`train_hurdle_model()` function** — Trains hurdle model with conformal recalibration on positive distribution
- **`_sample_hurdle()` and `_sample_hurdle_from_quantiles()` methods** in `src/models/monte_carlo.py`:
  - Bernoulli draw for zero vs positive (independent of copula)
  - Inverse CDF sampling for positive samples
  - Copula-correlated uniforms applied to positive branch only
- **`_calibrate_hurdle_model()` method** in `src/models/train_pipeline.py`:
  - Zero prediction accuracy diagnostics
  - Quantile coverage evaluation

### Changed

- **`PlayerPropsModelPipeline` class**:
  - Added `hurdle_models: dict[str, HurdleQuantileModel]` attribute
  - Modified `train_rate_models()` — uses `train_hurdle_model()` for THREES stat
  - Modified `save_all()` — saves hurdle model artifacts (classifier, calibrator, rate models, flag file)
  - Modified `load_all()` — detects and loads hurdle models via `threes_is_hurdle.json` flag
- **`MonteCarloPredictor._predict_copula()`** — Detects hurdle models and uses `_sample_hurdle()` instead of regular inverse CDF
- **`MonteCarloPredictor.predict_batch_for_date()`** — Handles hurdle models in batch prediction loop
- **`TrainingOrchestrator._evaluate_calibration()`** — Evaluates hurdle models separately
- **`TrainingOrchestrator._evaluate_combined_calibration()`** — Includes hurdle stats in combined eval

### Changed — BL Confidence (A3b)

- **`BLConfig` dataclass** in `src/models/black_litterman.py`:
  - Added `z_max: float = 1.0` parameter — z-score at which confidence saturates to 1.0
- **`compute_confidence()` method** — Replaced exponential formula with linear ramp:
  - Old: `confidence = 1 - exp(-0.5 * z²)` (near-zero for z < 0.5)
  - New: `confidence = min(z / z_max, 1.0)` (proportional for z < z_max)
  - Impact: At z=0.13 (typical 3% edge), confidence is now 0.13 vs 0.008 previously (16x improvement)

### Updated

- **`tests/test_black_litterman.py`:**
  - Updated `test_line_one_std_away` — z=1 now → confidence=1.0 (was 0.39)
  - Updated `test_line_two_std_away` — z=2 now → confidence=1.0 (was 0.86)
  - Updated `test_line_three_std_away` — z=3 now → confidence=1.0 (was 0.99)
  - Added `test_linear_confidence_at_half_z_max` — verifies z=0.5 → confidence=0.5
  - Added `test_custom_z_max` — verifies custom z_max=2.0 works correctly
  - Added `test_linear_ramp_proportional` — verifies linear relationship
  - Updated `test_default_config` and `test_custom_config` to include z_max assertions
  - All 42 tests pass
- **Test suite:** 518 of 523 tests pass (5 pre-existing failures unrelated to hurdle model)

---

## [2026-02-05 Session 10] — BL Sizing Parameter + Combo Markets Verification

### Added

- **`--bl-sizing-tau` CLI parameter** on `run_backtest.py` — Enables BL-blended probabilities for Kelly position sizing independently from edge detection
- **`bl_sizing_blender` field** on `BacktestHarness` — Separate blender instance for sizing calculations
- **`sizing_prob_over`/`sizing_prob_under` columns** in predictions output — BL-blended probabilities for position sizing
- **Spec files for next items:**
  - `.session/specs/A3b_BL_confidence_fix.md` — Linear ramp confidence function
  - `.session/specs/C3_THREES_hurdle_model.md` — Zero-inflated hurdle model for THREES

### Changed

- **`BetSimulator.place_bet()`** — Now accepts optional `sizing_prob` parameter for Kelly calculation (defaults to model probability if not provided)
- **`_calculate_edges()`** in `backtest_harness.py` — Computes sizing probabilities when `bl_sizing_blender` is set

### Verified

- **Combo markets scraping job (2026-01-31):** ~35K new prop lines successfully added to `raw_player_props_combined`:
  - `player_points_rebounds_assists` (12,013 rows, 82 players, 6 games)
  - `player_points_rebounds` (7,939 rows)
  - `player_points_assists` (5,758 rows)
  - `player_rebounds_assists` (5,107 rows)
  - `player_blocks_steals` (2,582 rows)
  - `player_field_goals` (2,376 rows)

### Analysis

- **Brier score improved:** 0.2705 → 0.2506 (model no longer catastrophically overconfident)
- **No-BL ROI:** +3.5% (profitable without BL blending)
- **BL confidence function issue persists:** Crushes sizing probs toward market, resulting in near-zero Kelly stakes

---

## [2026-02-04 Session 9] — Daily Injury Pipeline Fix + Paper Trading Infrastructure

### Added

- **Paper Trading Infrastructure (E5):**
  - **`src/paper_trading/paper_trader.py`** — Core `PaperTrader` class with:
    - `select_bets(game_date)` — Query daily_predictions, filter by edge threshold, calculate Kelly stakes
    - `place_bets(bets)` — UPSERT into paper_bets table
    - `resolve_bets(game_date)` — Fetch actuals from player_game_stats, update status/P&L
    - `get_pending_bets()`, `get_daily_summary()`, `get_bets_for_date()` — Dashboard query methods
  - **`src/paper_trading/place_bets.py`** — CLI script to place paper bets
    - `--dry-run` mode to preview without placing
    - `--edge-threshold`, `--kelly-fraction`, `--bankroll` parameters
    - Formatted table output with bet summary
  - **`src/paper_trading/resolve_bets.py`** — CLI script to resolve bets using actual results
    - `--dry-run` mode to preview resolution
    - Formatted resolution table with P&L summary
  - **DB migration:** `paper_bets` and `paper_trading_daily_log` tables
  - **Unit tests:** 20 tests in `tests/test_paper_trader.py` covering Kelly calculation, bet selection, resolution logic

### Changed

- **`src/orchestration/run_daily.py`** — Fixed `--scrape-injuries` flag to use RapidAPI instead of ESPN
  - Now calls `rapidapi_injury_backfill.py --start {date} --end {date}` to fetch injuries into `rapidapi_injuries` table
  - Then calls `link_injury_data.py` to populate `player_id` column via fuzzy name matching
  - Ensures consistency with feature store (`feature_store.py`) and daily runner (`daily_runner.py`) which both query `rapidapi_injuries`
  - Updated help text from "Scrape current injuries from ESPN" to "Scrape injuries from RapidAPI and link player IDs"

### Fixed

- **E4 (Daily injury pipeline)** — The `--scrape-injuries` flag was writing to `espn_injuries` table but all downstream components read from `rapidapi_injuries`. Daily injury data was effectively unused. Now both scraping and consumption use the same data source.

---

## [2026-01-31 Session 8] — Calibration Fixes, BL Sweep Analysis

### Added

- **Conformal recalibration** in `quantile_trainer.py` — post-training offset from validation residuals when coverage gap exceeds 3%
  - `RECALIBRATION_GAP_THRESHOLD = 0.03` class constant
  - `calibration_offsets: dict[float, float]` computed per quantile, applied at `predict_quantiles()` time
  - Persisted in model artifacts via `save()`/`load()`
- **Zero-snap handling** in `monte_carlo.py` — `ZERO_SNAP_THRESHOLD = 1e-3` snaps near-zero inverse CDF values to exactly 0
  - Applied in `_build_extended_quantile_fn()` for both copula and non-copula paths

### Changed

- `train_pipeline.py` — `_evaluate_combined_calibration()` now dynamically evaluates all trained rate models (`[s for s in ["pts", "reb", "ast", "threes"] if s in pipeline.rate_models]`) instead of hardcoded `["pts", "reb", "ast"]`
- `train_pipeline.py` — `_analyze_minutes_rate_correlation()` loop includes `"threes"` alongside `"pts"`, `"reb"`, `"ast"`
- `monte_carlo.py` — `_inverse_transform_sample()` refactored to use `_build_extended_quantile_fn()` instead of duplicating logic

### Analysis

- **BL parameter sweep (40 configs):** No-BL shows +3% ROI (600-873 bets, REB +7.9%). ALL BL configs produce 0-12 bets due to structural confidence function issue — `1 - exp(-0.5 * z²)` near-zero for realistic edges (z < 0.5)

---

## [2026-01-31] — Prediction Storage, Daily Runner Refactor, Scraper Resume

### Added

- **`src/models/prediction_store.py`** — Storage and retrieval module for daily MC predictions
  - `store_predictions()` — bulk upsert via `psycopg2.extras.execute_values` with `ON CONFLICT DO UPDATE`
  - `store_samples()` — gzip-compressed float64 numpy arrays stored as PostgreSQL BYTEA (~20-40KB per prediction)
  - `get_predictions()` — filtered retrieval by date/player/stat
  - `get_samples()` — decompress and return as np.ndarray
  - `get_player_id_by_name()` — fuzzy name lookup (case-insensitive LIKE)
- **`src/tools/query_player.py`** — CLI tool for querying stored daily predictions
  - Mode 1: Player + stat + line → compute over/under probability from MC samples + optional EV calculation
  - Mode 2: Player overview → all predictions for a player on a date
  - Mode 3: Top N edges → best absolute edges for a date with model vs market breakdown
- **DB migration: `create_daily_predictions_tables`** — two new tables:
  - `daily_predictions` — quantile predictions, edges, implied probabilities. UNIQUE on `(prediction_date, player_id, game_id, stat)`.
  - `daily_prediction_samples` — gzip-compressed MC sample arrays. UNIQUE on `(prediction_date, player_id, game_id, stat)`.
  - 3 indexes for query performance
- `--skip-storage` CLI flag on `run_daily.py` to skip DB persistence

### Changed

- **`daily_runner.py` — major refactor:**
  - `_get_games_for_date()` → NBA API ScoreboardV2 as primary, DB fallback for past dates
  - `_filter_injured_players()` → `rapidapi_injuries` with `player_id` integer matching (was `espn_injuries` with string name matching)
  - `_get_current_lines()` → `ROW_NUMBER() OVER (... ORDER BY snapshot_time DESC)` for latest snapshot per line
  - `_calculate_edges()` → MC samples empirical CDF with quantile interpolation fallback (was quantile-only)
  - `run_for_date()` → returns `(pd.DataFrame, dict[tuple, np.ndarray])` tuple instead of `pd.DataFrame`
  - New `_build_features_df()` and `_enrich_predictions()` helper methods
  - Uses `predict_batch_for_date()` (4 XGBoost calls) instead of per-player predict
- **`run_daily.py`** — wired `PredictionStore` for predictions + samples storage after inference
- **`player_prop_scraper.py`** — resume capability with market-aware progress file format
  - Progress file format: `{"markets": "...", "processed": [[ts, eid], ...]}` (was flat list)
  - Skip logic in main loop for already-processed events
  - Progress saving after each snapshot and on interrupt/error
  - `--no-resume` flag to start fresh

### Fixed

- **`test_daily_runner.py`** — updated all 7 failing tests for new return types, injury source, edge calc, and batch predict path. Added 4 new tests: NBA API primary, MC samples edge calc, quantile fallback, build_features_df, enrich_predictions.
- **`test_player_prop_scraper.py`** — updated 2 tests for new market-aware progress file format

---

## [2026-01-30] — Bug Fix Sweep, Parameter Sweep Tool, Scraper Improvements

### Added

- **ISSUES.md** — Comprehensive 28-issue audit of the core pipeline (12 fixed, 16 open)
- **`src/backtesting/run_sweep.py`** (778 lines) — Parameter sweep tool for BL tau, edge threshold, and Kelly fraction
  - Runs Phase 0-1 (DB fetch + MC predictions) once, replays edge calc + bet sim per config
  - Cartesian grid of `(tau, edge_threshold, kelly_fraction)` values
  - Per-config subdirectories with bets.csv, predictions.csv, metrics.json (compatible with `visualize_results.py`)
  - Comparison table with per-stat breakdown
- **`tests/test_run_sweep.py`** (651 lines) — Tests for sweep grid builder, shared phases, single-config execution, output formatting, and save logic
- 11 additional US2/us_ex bookmakers added to defaults: ballybet, betopenly, betparx, espnbet, fliff, hardrockbet, novig, polymarket, prophetx, rebet, windcreek
- **Scraper CLI improvements:**
  - `daily_player_props_scraper.py`: `--combos`, `--combos-only`, `--markets` flags for market selection; shared `CORE_MARKETS` and `COMBO_MARKETS` presets
  - `player_prop_scraper.py`: `--start-date`, `--end-date` date range filters; `--combos`, `--combos-only`, `--markets` flags; `--dry-run` credit estimation; argparse-based CLI

### Fixed

- **ISS-001** (CRITICAL): Minutes model now uses tuned hyperparams — `self.config` → `config` in `quantile_trainer.py:374`
- **ISS-002** (HIGH): `_run_date()` early-exit paths return `(None, pd.DataFrame())` instead of `None` — prevents `TypeError` unpacking
- **ISS-003** (HIGH): Non-BL edge path now uses multiplicative devigging in both `backtest_harness.py` and `daily_runner.py` — previously used vigged implied probabilities, understating edges by ~2-3%
- **ISS-004** (HIGH): Injury LATERAL JOIN split into two separate subqueries (game stats + advanced stats) — eliminates N×M cross-product and incorrect `ORDER BY` across tables. Applied to all 4 feature store query paths + single-player inference.
- **ISS-005** (HIGH): Training query filter `min > 0` → `min >= 5` — matches inference threshold, removes noisy low-minute samples
- **ISS-006** (HIGH): `early_stopping_rounds` now passed to `model.fit()` in `quantile_trainer.py` — previously configured but never applied
- **ISS-007** (MEDIUM): Combined calibration now evaluates the copula inference path — reordered `train_pipeline.py` steps so copula params are computed before combined calibration and passed to `MonteCarloPredictor`
- **ISS-008** (MEDIUM): `line_spread` now team-directional — negative for home (favored) team via `CASE WHEN matchup LIKE '%vs.%'` across all query paths; single-player path updated to apply sign from `is_home` context
- **ISS-009** (MEDIUM): COALESCE defaults changed from 0 to league averages — `avg_pace_l5=99.5`, `avg_def_rtg_l5=112.0`, `avg_fg3a_l5=34.0`, `avg_fg3_pct_l5=0.36`, `avg_usg_pct_l5=0.20`, `avg_ts_pct_l15=0.56`, etc. Applied to all bulk and single-player query paths.
- **ISS-011** (MEDIUM): Inference path advanced stats JOIN changed from exact `game_id` match to date-based LATERAL lookup (`game_date < :as_of_date ORDER BY game_date DESC LIMIT 1`) — matches bulk training/backtesting pattern
- **ISS-015** (MEDIUM): `_filter_best_bets` now selects best over and best under lines independently per (player, game, stat) — previously picked one row by max single-side edge, discarding valid opposite-side bets from other bookmakers
- **ISS-016** (MEDIUM): Combined calibration prediction failures tracked and logged as `WARNING` with count — previously swallowed at `DEBUG` level

### Changed

- `daily_runner.py`: `_get_current_lines()` now fetches all bookmakers and selects the sharpest (lowest-vig) line per player/game/market via booksum minimization; implied probabilities devigged via multiplicative normalization
- `backtest_harness.py`: `_run_date()` return type changed from `pd.DataFrame | None` to `tuple[pd.DataFrame | None, pd.DataFrame]`
- `train_pipeline.py`: Pipeline step ordering — copula params (5b) now computed before combined calibration (5c), correlation analysis moved to (5d)
- `player_prop_scraper.py`: Extended 2025-26 Regular season end date from 2026-01-23 to 2026-04-15

---

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
