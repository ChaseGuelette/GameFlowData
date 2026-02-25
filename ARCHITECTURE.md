# GameFlowData Architecture

This document describes the system architecture, design patterns, and data flows for **GameFlowData**, an NBA analytics and machine learning platform for player prop betting markets.

---

## High-Level Overview

GameFlowData is a data-intensive application that ingests raw NBA game statistics and sportsbook odds, normalizes and links them, trains advanced machine learning models (Quantile Regression + Monte Carlo), and evaluates performance via a rigorous time-travel backtesting harness.

### Core Goals
1.  **Precision Data:** Accurate, pace-adjusted, and opponent-specific metrics.
2.  **Probabilistic Modeling:** Predicting full probability distributions (not just means) to price derivatives.
3.  **Backtesting Rigor:** Preventing look-ahead bias to realistically simulate betting edges.

---

## Technology Stack

| Layer | Components | Purpose |
|-------|------------|---------|
| **Language** | Python 3.11+ | Core runtime |
| **Database** | PostgreSQL 15+ (Supabase) | Primary relational store |
| **ORM/Data** | SQLAlchemy 2.0, Pandas 3.0, Psycopg2 | Data access and manipulation |
| **ML Core** | XGBoost, Scikit-Learn, NumPy, SciPy | Quantile regression, isotonic calibration, statistics |
| **HPO** | Optuna | Bayesian hyperparameter optimization |
| **API** | FastAPI, Uvicorn, Pydantic | Web framework (future live pipeline) |
| **Data Sources** | nba_api, The Odds API, ESPN | NBA stats, sportsbook odds, injury reports |
| **Pipeline** | Custom Python orchestration | Training, inference, and backfill jobs |
| **Visualization** | Plotly | Backtest equity curves and diagnostic plots |
| **Image Gen** | Pillow | Social media pick card rendering |
| **Dashboard** | Next.js 16, TypeScript, Tailwind, Recharts | Web UI for predictions and paper trading |
| **Testing** | Pytest, Pytest-Cov | Unit and integration testing |
| **Linting/Types** | Ruff, Pyright | Code quality and static analysis |

---

## Directory Structure

```
GameFlowData/
├── src/                        # Main source code
│   ├── config/                 # Configuration (stat_config.py)
│   ├── db/                     # Database connection layer
│   ├── scrapers/               # Data ingestion (13 modules)
│   ├── processing/             # Data pipeline: linking, averages, backfill
│   ├── diagnostics/            # Database health monitoring tools
│   ├── models/                 # ML core: features, training, inference, storage
│   ├── backtesting/            # Historical replay and bet simulation
│   ├── tools/                  # CLI query tools
│   ├── orchestration/          # Daily workflow coordination
│   ├── paper_trading/          # Paper bet placement and resolution
│   └── social/                 # Social media image generation
├── dashboard/                  # Next.js web dashboard (TypeScript, Tailwind)
├── assets/fonts/               # Montserrat font files for image generation
├── tests/                      # Unit and integration tests (34 modules)
├── docs/                       # Component-level documentation
├── notebooks/                  # Jupyter notebooks for research
├── database/                   # Schema definitions (schema.sql)
├── data/linker_data/           # Local CSV cache for linking pipeline
├── backtest_results/           # Backtest output and analysis
├── logs/                       # Job execution logs (daily_stats, lines, inference)
├── cron/                       # Cron schedule templates for server deployment
├── predictions/                # Daily prediction CSV exports
├── pyproject.toml              # Project config, deps, ruff/pytest settings
├── requirements.txt            # Production ML/data dependencies
├── requirements-dev.txt        # Dev/test dependencies
├── pytest.ini                  # Test runner configuration
└── alembic.ini                 # Database migration configuration
```

---

## System Components

### 0. Configuration (`src/config/`)

**`stat_config.py`** — Per-stat configuration for betting parameters.
- `StatConfig` dataclass — per-stat settings (enabled, edge_threshold, bl_tau)
- `StatConfigSet` container — global defaults with per-stat overrides
- `parse_stat_param()` helper — parses CLI arguments like `"pts=0.10 reb=0.07"`
- Supports CLI format: `--edge-threshold pts=0.10 reb=0.07 ast=0.15` or global `--edge-threshold 0.05`
- Used by backtesting harness, bet simulator, and paper trading

### 1. Database Layer (`src/db/`)

**`client.py`** — Singleton SQLAlchemy engine with connection pooling.
- Pool size 10, max overflow 6, 5-minute connection recycle (optimized for parallel feature building).
- 5-minute statement timeout.
- pgBouncer compatible.
- **Lazy initialization:** Engine creation is deferred — module is safely importable without `DATABASE_URL` (e.g., in CI/test environments). `get_engine()` raises `ValueError` at call time if `DATABASE_URL` is missing.

### 2. Data Collection & "The Linker"

The system ingests data from two distinct worlds that don't natively share identifiers:
1.  **Official NBA Data:** (Via `nba_api`) Game stats, player bios, team box scores.
2.  **Sportsbook Data:** (Via The Odds API) Player props, game lines, futures.
3.  **Injury Data:** (Via RapidAPI) Historical backfill from 2021 and daily collection via RapidAPI NBA Injury Reports API. Data stored in `rapidapi_injuries` table with `player_id` linking via `link_injury_data.py`.

#### Scrapers (`src/scrapers/`)

| Module | Purpose |
|--------|---------|
| `nba_unified_scraper.py` | CLI tool for team game stats and player advanced metrics from NBA API |
| `daily_player_props_scraper.py` | Daily player prop lines from Odds API (regions: us, us2, us_ex, us_dfs) |
| `daily_game_lines_scraper.py` | Daily game spreads and totals |
| `live_odds_scraper.py` | Real-time odds from multiple sportsbooks |
| `espn_injury_scraper.py` | ESPN injury reports and player status |
| `nba_player_position.py` | Player position data |
| `injury_database.py` | Injury data management |
| `injury_scraper_job.py` | Scheduled injury updates |
| `update_league_position_averages.py` | League-wide position average baselines |
| `update_player_position_history.py` | Historical player position tracking |
| `player_prop_scraper.py` | Alternate player props source |
| `game_lines_scraper.py` | Historical game lines |
| `rapidapi_injury_backfill.py` | Injury data from RapidAPI — historical backfill (2021-present, 88K+ rows) and daily collection via `run_daily.py --scrape-injuries` |
| `play_type_scraper.py` | Team-level Synergy play type data (offensive/defensive frequency + efficiency) |

#### The NBA Linker (`src/processing/nba_linker_local.py`)

Serves as the bridge between NBA and sportsbook data:
- **Fuzzy Matching:** Matches variations of player names (e.g., "Luka Doncic" vs "Luka Dončić") and team names.
- **Team Normalization:** All team names normalized to 3-letter abbreviations (e.g., "Atlanta Hawks" → "ATL", "Los Angeles Lakers" → "LAL") for consistent matching between Odds API full names and NBA API abbreviations.
- **Date Alignment:** Handles timezone differences and scheduling quirks (e.g., ±90 day fuzzy windows for futures).
- **Staging Tables:** Data first lands in `raw_*_staging` tables before being linked to official `game_id` and `player_id`.
- **Manual Overrides:** `data/linker_data/player_mappings.csv` for edge cases.
- **Unmatched Output:** Writes `unmatched_*.csv` files for human review.
- **Commands:**
  - `download` — Pull full tables to local CSV (one-time bulk operation)
  - `process` — Match IDs locally using downloaded CSVs
  - `upload` — Push linked results back to database
  - `incremental` — **Lightweight daily mode**: queries only unlinked records (`WHERE player_id IS NULL`), matches against reference tables, updates directly via batched SQL. No CSV download. Used by `run_daily.py` for automated pipelines. Options: `--batch-size` (default 50000), `--limit` (optional cap on records to process).

### 3. Processing Pipeline (`src/processing/`)

| Module | Purpose |
|--------|---------|
| `populate_average_stats.py` | Computes L5, L15, season-to-date rolling averages for players and teams. Full recalculation for historical backfills. |
| `populate_average_stats_incremental.py` | Lightweight daily version — only processes players who played on target date. Uses batch UPSERT. Queries actual season game count for correct `games_szn`. Runtime: ~1s vs ~28min for full script. |
| `backfill_opponent_allowed.py` | Computes opponent defensive metrics by position → `team_allowed_by_position` table. Rolling windows use `.mean()` (per-game averages). |
| `backfill_opponent_allowed_incremental.py` | Lightweight daily version — only processes last 30 days with 15-day lookback buffer. Runs as Step 7 in `daily_stats_job.py`. |
| `backfill_league_priors.py` | Computes league-wide Bayesian priors → `league_priors_history` table. |
| `backfill_team_ids.py` | Validates and links team IDs across data sources. |
| `feature_selection.py` | `ImprovedFeatureSelector` — per-quantile feature selection with time-series aware 3-split CV and permutation importance. |
| `link_injury_data.py` | Links RapidAPI injury records to NBA player/team IDs via 3-tier cascade: manual CSV overrides → exact normalized match → SequenceMatcher fuzzy match (threshold 0.80, +0.15 last name bonus). 99.3% coverage. |

### 4. Diagnostics (`src/diagnostics/`)

Database health monitoring and model calibration analysis tools.

| Module | Purpose |
|--------|---------|
| `db_health_check.py` | Comprehensive database health check script with 8 validation categories |
| `calibration_per_stat.py` | Per-stat (PTS/REB/AST) calibration diagnostic with quantile coverage, bias, ECE, Brier score |

**`calibration_per_stat.py`** — Per-stat calibration diagnostic tool (C2):
- **Quantile coverage** (Q10–Q90): Is P(actual <= pred_q) ≈ q? Per stat and global.
- **Bias**: Mean predicted vs mean actual with relative percentage.
- **Interval sharpness**: 80% and 50% prediction interval widths.
- **Probability calibration**: Brier score and Expected Calibration Error (ECE, 10-bin).
- **Reliability curve data**: Per-bin (predicted_prob, actual_rate, count) for plotting.
- **Auto-diagnosis**: Flags stats exceeding configurable tolerances for coverage gap, bias, and ECE.
- **Two input paths**: Backtest CSV (`--csv`) or production DB (`--db` with `--start`/`--end`).
- **JSON export**: Structured report via `--output`.

**Usage:**
```bash
python -m src.diagnostics.calibration_per_stat --csv backtest_results/predictions.csv
python -m src.diagnostics.calibration_per_stat --db --start 2025-02-10 --end 2025-02-18
python -m src.diagnostics.calibration_per_stat --csv predictions.csv --output report.json --tolerance 0.05
```

**`db_health_check.py`** — Validates data integrity, freshness, and linkage across all tables:
- **Data Freshness** — Latest dates for key tables (player_game_stats, daily_predictions, injuries)
- **Game Data Completeness** — Games per date, player counts per game
- **Prop Linking Health** — NULL game_id/player_id/team_id rates
- **Aggregation Sync** — player_average_game_stats coverage vs player_game_stats
- **Injury Linking** — Injuries without player_id
- **Position History** — Active players with position data
- **Prediction Coverage** — Games with/without predictions
- **Foreign Key Integrity** — Soft FK validation

**Usage:**
```bash
python src/diagnostics/db_health_check.py              # Basic run
python src/diagnostics/db_health_check.py --days 14    # Check last 14 days
python src/diagnostics/db_health_check.py --verbose    # Detailed breakdowns
python src/diagnostics/db_health_check.py --json       # JSON output for automation
```

**Exit Codes:**
- `0` = All checks passed
- `1` = Warnings present
- `2` = Critical errors found

### 5. Feature Store (`src/models/feature_store.py`)

Centralized engine for converting raw stats into model-ready features.

**Key Capabilities:**
- **Vectorized SQL Generation:** Uses PostgreSQL `LATERAL JOIN`s to compute complex rolling windows (L5, L15, Season) for thousands of players instantly.
- **Time-Travel Safety:** Strictly enforces `game_date < target_date` inequalities to prevent data leakage.
- **Contextual Features:**
    - **Pace-Adjusted Opponent Defense:** e.g., "Opponent allows X threes per 100 possessions."
    - **Rest & Schedule (B2):** `rest_days`, `is_back_to_back`, `games_in_last_7_days` — pre-computed in `player_average_game_stats` from game date diffs.
    - **Short-Window Trends (B3):** L3 rolling averages (`player_avg_{stat}_l3`), momentum ratios (`player_{stat}_l3_l15_ratio`), and L5 standard deviations (`player_std_{stat}_l5`) for all stats.
    - **Minutes Stability (B4):** `player_min_std_l5`, `player_min_floor_l5`, `player_games_started_l5` — distinguishes locked-in starters from volatile rotation players.
    - **Injury Context (B1):** `team_out_count`, `team_out_min_sum`, `team_out_pts_sum`, `team_out_reb_sum`, `team_out_ast_sum`, `team_out_usg_sum` (teammate injuries), `opp_out_count`, `opp_out_min_sum` (opponent injuries), `player_is_questionable`, `player_is_probable` (player's own status). Computed via two separate SQL LATERAL JOINs (game stats + advanced stats) to `rapidapi_injuries` table with temporal integrity (report_date ≤ game_date).
    - **Betting Signals:** Implied totals and team-directional spreads as proxies for game script. `line_spread` is negative when the player's team is favored (home games with `matchup LIKE '%vs.%'`).
    - **Prop Line Centering:** Per-stat player prop lines (`prop_line_pts`, `prop_line_reb`, `prop_line_ast`, `prop_line_threes`) from `raw_player_props_combined`. Enables residual modeling — the model learns deviations from market expectation rather than absolute values.
- **League-Average Defaults:** Missing feature values default to league averages instead of 0 — `avg_pace_l5=99.5`, `avg_def_rtg_l5=112.0`, `avg_fg3a_l5=34.0`, `avg_fg3_pct_l5=0.36`, `avg_usg_pct_l5=0.20`, `avg_ts_pct_l15=0.56`, `avg_reb_pct_l5=0.10`, `avg_ast_pct_l5=0.15`, `off_rtg_allowed=112.0`. Prevents extreme outlier feature values for early-season games and rookies.
- **Train/Serve Consistency:** All 4 query paths (training, date, date_range, single-player) use identical SQL patterns — date-based LATERAL JOINs for rolling stats, matching thresholds (`min >= 5`), and consistent COALESCE defaults.

**API Methods:**
- `get_player_game_features()` — Single player-game feature vector.
- `get_features_for_date()` — All players for a given date.
- `get_features_for_date_range()` — Time-series dataset across date range.
- `get_training_dataset()` — Full training data for season(s).

**Feature Groups:**
- `RATE_FEATURES_PTS` / `_REB` / `_AST` — Per-stat rate model features. Each includes its corresponding `prop_line_*` centering feature plus B3 trend/variability features (`player_avg_{stat}_l3`, `player_{stat}_l3_l15_ratio`, `player_std_{stat}_l5`).
- `MINUTES_FEATURES` — Playing time prediction features (includes `line_spread`, `line_total`, B2 rest/schedule, B3 minutes L3 trend, B4 minutes stability).
- Configuration via `FeatureConfig` dataclass.

### 6. Machine Learning Pipeline (`src/models/`)

The modeling engine predicts the probability distribution of player stats.

#### Stage A: Quantile Regression (`quantile_trainer.py`)

- `PlayerPropsModelPipeline` class with `QuantileModelConfig` dataclass.
- Trains multiple **XGBoost** models for each target stat (Points, Rebounds, Assists).
- **Per-Quantile Optimization:** Each quantile (10th, 25th, 50th, 75th, 90th) selects its own optimal feature set.
    - *Example:* "Floor" (Q10) models might prioritize minutes played, while "Ceiling" (Q90) models prioritize usage rate and pace.
- **Isotonic Calibration:** Post-processing step to ensure monotonic predictions (`Q10 <= Q25 <= ...`).
- **Conformal Recalibration:** After training each quantile, computes validation residuals `(y_val - pred)`. If coverage gap exceeds 3%, applies a conformal offset `delta = np.quantile(residuals, q)` at prediction time. Offsets persisted in model artifacts.
- Default hyperparameters: `n_estimators=1000`, `max_depth=5`, `learning_rate=0.03`, `early_stopping_rounds=50`.
- **Archived (2026-02-10):** THREES model (3-pointers) archived to `archive/threes_model/` due to poor market coverage (50% missing lines) and insufficient betting volume. Scrapers still collect `player_threes` market data for future optionality.

#### Stage B: Hyperparameter Tuning (`hyperparameter_tuner.py`)

- `QuantileHyperparameterTuner` class using **Optuna**.
- Objective: minimize max calibration gap (not raw loss).
- Supports per-quantile or shared hyperparameters.
- Search space: depth, learning rate, L1/L2 regularization, subsample, colsample.

#### Stage C: Monte Carlo Simulation (`monte_carlo.py`)

- `MonteCarloPredictor` class with `PropPrediction` dataclass.
- Combines the outputs of:
    1.  **Minutes Model:** Predicts playing time distribution.
    2.  **Rate Model:** Predicts stats-per-minute distribution.
- Simulates 10,000+ outcomes per player to generate a final probability density function.
- **Gaussian Copula Sampling (C0):** Minutes and per-minute rates are correlated (PTS ρ=0.314, AST ρ=0.176). Rather than sampling independently and applying a post-hoc hack, the predictor uses a Gaussian copula:
    1. Shared latent normal `z_minutes ~ N(0,1)` across all stats
    2. Per-stat: `z_rate = ρ·z_minutes + √(1-ρ²)·z_independent`
    3. Transform to uniform via `Φ(z)`, map through marginal inverse CDFs
    4. This preserves both marginal distributions exactly while inducing the correct rank dependency
    - Copula parameters (Spearman ρ) are computed at training time and saved as `copula_params.json` artifact
    - Falls back to legacy post-hoc adjustment when copula params unavailable (backward compat)
- **Zero-Inflation Handling:** `_build_extended_quantile_fn()` snaps quantile values below `ZERO_SNAP_THRESHOLD` (1e-3) to exactly 0. Ensures MC samples in the zero-mass region of discrete distributions (e.g., threes_per_min) map to 0 instead of tiny positive interpolated values. Works in both copula and non-copula paths.
- **Combined Calibration Offsets (infrastructure, not deployed):** `_apply_combined_calibration()` applies per-stat per-quantile conformal offsets via piecewise-linear sample warping on the combined (minutes x rate) distribution. Loaded from `combined_calibration_offsets.json` via `load_combined_calibration_offsets()`. Backward-compatible no-op when file is absent. A/B backtest (Session 42) showed offsets improved calibration metrics but degraded betting ROI — offsets are NOT deployed to production. Code retained for future use if genuine calibration drift emerges.
- **Output:** Exact probabilities for any line (e.g., "Probability of 20+ points").
- **Betting utilities:** `prob_over(line)`, `prob_under(line)`, `expected_value_over/under(line, odds)`.

#### Stage D: Calibration (`calibration.py`)

- `CalibrationEvaluator` class with `CalibrationReport` dataclass.
- Checks: P(actual < predicted_q) ≈ q for each quantile.
- Tolerance-based validation: `CALIBRATION_TOLERANCE = 0.05`, `CALIBRATION_HARD_FAIL = 0.10`.

#### Stage E: Probability Blending (`black_litterman.py`)

Anchors the model's overconfident probability estimates to the market's well-calibrated prior using a log-odds Bayesian blend. Sits between Monte Carlo output and the bet simulator.

- `BlackLittermanBlender` class with `BLConfig` dataclass.
- **Prior:** Devigged sportsbook probability (vig removed via multiplicative normalization, equivalent to Shin's method for 2-outcome markets).
- **View:** Model's empirical P(over) from MC samples.
- **Confidence:** Per-prediction confidence using linear ramp based on z-score:
  ```
  z = |mean(samples) - line| / std(samples)
  confidence = min(z / z_max, 1.0)
  ```
  z=0 → confidence=0 (line at center, posterior ≈ market). z=z_max → confidence=1.0 (full model weight). Linear interpolation between.
- **Blending (log-odds space):**
  ```
  w = min(tau × confidence, max_weight)
  posterior_logit = market_logit + w × (model_logit - market_logit)
  posterior = sigmoid(posterior_logit)
  ```
- **Parameters:** `tau` (global scaling, 0.01–0.30, default 0.05), `max_weight` (hard cap, default 0.50), `z_max` (confidence saturation point, default 1.0), `min_prob`/`max_prob` (clamping to avoid log(0)).
- **Key property:** When tau=0 or confidence=0, posterior = market → no edge → no bet. Model influence scales with both global trust (tau) and per-prediction confidence.
- **Integration:** Wired into `_calculate_edges()` in `backtest_harness.py` via `--bl-tau` CLI flag. Disabled by default (backward compatible).

#### Training Orchestrator (`train_pipeline.py`)

- `TrainingOrchestrator` class — orchestrates full training workflow.
- Optional Optuna hyperparameter tuning.
- Feature selection integration.
- Calibration validation (individual + combined minutes×rate).
- Minutes-rate correlation analysis with Spearman rank correlations.
- Computes and saves Gaussian copula parameters (`copula_params.json`) for MC inference.
- Computes per-stat per-quantile conformal offsets during combined calibration and saves as `combined_calibration_offsets.json`.
- **`--calibrate-only` mode:** Loads an existing model, runs MC predictions on calibration data, computes combined offsets, and saves them to the model directory without retraining. Useful for post-hoc recalibration experiments.
- Model persistence via `joblib`.
- **Atomic rename pattern (added 2026-02-09):** Training creates `run_YYYYMMDD_HHMMSS_incomplete` directory initially, renamed to `run_YYYYMMDD_HHMMSS` only after all artifacts are saved. Prevents inference job from selecting incomplete models during training. Inference job filters out `_incomplete` directories when auto-selecting latest model.

#### Daily Runner (`daily_runner.py`)

- `DailyPredictionRunner` class — production inference pipeline.
- Workflow: get today's games (NBA API ScoreboardV2) → filter injured players (`rapidapi_injuries`) → build features → batch predict (4 XGBoost calls) → enrich with opponents → fetch prop lines → calculate edges → return `(predictions_df, samples_dict)`.
- **Game Discovery:** Primary source is `nba_api.stats.endpoints.ScoreboardV2` (works for scheduled/future games). Falls back to `team_game_stats` DB query for past dates when NBA API is unavailable.
- **Injury Filtering:** Queries `rapidapi_injuries` table by `player_id` (integer matching). Uses most recent `report_date` on or before target date. Filters players with `status = 'Out'`.
- **Batch Prediction:** Uses `predict_batch_for_date()` — 4 total XGBoost calls (1 minutes + 3 rates) for all players, instead of N per-player calls.
- **Parallel Feature Building (optimized 2026-02-13):** Uses `ThreadPoolExecutor` with 8 workers to parallelize feature store queries. Runtime reduced from ~65s to ~5s (13x faster). Connection pool increased to handle concurrent queries.
- **Sharpest-Book Selection:** Fetches lines from all bookmakers via `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY snapshot_time DESC)` to get only the latest snapshot, then selects the lowest-vig (smallest booksum) line per player/game/market. Applies multiplicative devigging to implied probabilities for edge calculation.
- **Optimized Prop Lines Query (2026-02-13):** Query searches both 8-digit and 10-digit game_id formats to avoid `LPAD()` function in WHERE clause, enabling index usage. With `idx_props_game_id` index on `raw_player_props_combined.game_id`, query reduced from ~137s to ~0.2s (685x faster).
- **Edge Calculation:** Uses MC samples empirical CDF (`(samples > line).mean()`) for probability estimation. Falls back to 5-point quantile interpolation when samples are unavailable.

#### Prediction Storage (`prediction_store.py`)

- `PredictionStore` class — stores and retrieves daily predictions and MC samples.
- **Predictions:** Upserted to `daily_predictions` table via `psycopg2.extras.execute_values` with `ON CONFLICT DO UPDATE`. Stores quantiles, edges, implied probabilities, and prop line info.
- **MC Samples:** Gzip-compressed `float64` numpy arrays stored as PostgreSQL `BYTEA` in `daily_prediction_samples` table (~20-40KB per prediction for 10K samples).
- **Retrieval:** `get_predictions()` for filtered queries, `get_samples()` for decompressing single arrays, `get_all_samples_for_date()` for bulk retrieval (returns `dict[(player_id, game_id, stat) -> np.ndarray]` used by edge refresh), `get_player_id_by_name()` for fuzzy name lookup.

#### Query Tool (`src/tools/query_player.py`)

CLI tool for querying stored daily predictions. Three modes:
1. **Line query:** Player + stat + line → compute over/under probability from stored MC samples + optional EV at given odds.
2. **Player overview:** All predictions for a player on a date.
3. **Top edges:** Top N predictions by absolute edge for a date.

#### Analysis & Diagnostics

| Module | Purpose |
|--------|---------|
| `analyze_calibration_drift.py` | Post-hoc calibration analysis. Detects drift in quantile coverage over time. |
| `analyze_minutes_bimodality.py` | Diagnostic for minutes distribution shape. Finding: bimodality is not real (closed). |

### 7. Backtesting Harness (`src/backtesting/`)

A simulation environment to validate betting strategies.

| Module | Purpose |
|--------|---------|
| `backtest_harness.py` | Core engine — day-by-day historical replay with blind predictions. `BacktestResult` dataclass. Integrates optional BL blending in `_calculate_edges()`. Supports per-stat configuration via `StatConfigSet`. |
| `bet_simulator.py` | `Bet` and `BetOutcome` classes. `BetSide` enum (OVER/UNDER). P&L tracking per bet. Stores BL `posterior_prob` diagnostic. Supports per-stat edge thresholds via `StatConfigSet`. |
| `performance_metrics.py` | `PerformanceMetrics` dataclass — ROI, hit rate, Sharpe ratio, drawdown, Brier score. |
| `run_backtest.py` | CLI entry point. Accepts date range, model paths, output directory. Supports per-stat edge thresholds and BL tau via `nargs="+"` format. |
| `run_sweep.py` | Parameter sweep tool — runs Phase 0-1 once, then sweeps `(tau, edge_threshold, kelly_fraction)` grid. Saves per-config subdirectories compatible with `visualize_results.py`. Supports `StatConfigSet` for per-stat configuration. |
| `visualize_results.py` | Self-contained HTML dashboard: bankroll growth chart, daily P&L bars, metrics summary cards, enriched bet log table (player names/teams from DB), other bookmaker lines comparison. Sortable/filterable via vanilla JS. |

**Key Capabilities:**
- **Historical Replay:** Iterates through past seasons day-by-day.
- **Blind Predictions:** Models only see data available *before* tip-off.
- **Betting Simulation:**
    - **Line Shopping:** Independently selects best over line and best under line across bookmakers per (player, game, stat), ensuring both sides are considered even when they come from different bookmakers.
    - **Kelly Criterion:** Sizes bets based on calculated edge and bankroll.
    - **ROI Analysis:** Tracks bankroll growth, drawdown, and win rates.
- **Edge Calculation:** `_calculate_edges()` method determines bet eligibility. Both paths use multiplicative devigging to remove bookmaker vig before computing edges:
    - **Default (BL disabled):** Raw empirical CDF → edge vs devigged implied probability.
    - **BL enabled (`--bl-tau`):** Devigged market prior + log-odds BL blending → edge = posterior_prob - devigged_market_prob. Adds diagnostic columns: `model_over/under`, `market_over/under`, `confidence`, `posterior_over/under`.
- **Parameter Sweep:** `run_sweep.py` enables efficient grid search across BL tau, edge threshold, and Kelly fraction values by caching all shared data (features, lines, actuals, MC predictions/samples) and replaying only edge calculation + bet simulation per configuration.

### 8. Query Tools (`src/tools/`)

| Module | Purpose |
|--------|---------|
| `query_player.py` | CLI tool for querying stored predictions. Modes: line probability, player overview, top edges. |

### 9. Orchestration (`src/orchestration/`)

**`run_daily.py`** — Full pipeline orchestrator (legacy). Triggers complete workflow: data scraping → linking → feature store → predictions → storage → CSV export. Supports `--skip-storage` to skip DB persistence. The `--scrape-injuries` flag fetches current injuries from RapidAPI into `rapidapi_injuries` and runs `link_injury_data.py` to populate `player_id` for feature generation and filtering.

**Frequency-Separated Job Scripts (E6 — added 2026-02-05, expanded 2026-02-19):**

| Script | Schedule | Purpose |
|--------|----------|---------|
| `daily_stats_job.py` | 9:00 AM ET (once) | NBA game results + full processing pipeline |
| `lines_job.py --live` | 12 PM, 4 PM ET | Full lines scrape (game lines + live props + injuries + linker) |
| `inference_job.py` | 12:15 PM, 4:15 PM ET | Full inference (MC predictions + edges + BL) |
| `lines_job.py --live --props-only` | 1, 2, 3, 4:30, 5, 5:30, 6, 6:30 PM ET | Props-only scrape (live props + linker) |
| `edge_refresh_job.py` | 2 min after each props-only | Recalculate edges from stored MC samples + fresh lines |

**`daily_stats_job.py`** — Once-daily stats scraping after previous night's games finalize. Steps: `nba_unified_scraper.py` → `nba_linker_local.py incremental` → `backfill_team_ids_incremental.py` → `update_player_position_history.py` → `update_league_position_averages.py` → `populate_average_stats_incremental.py` → `backfill_opponent_allowed_incremental.py` → **resolve ALL pending paper bets** (via `PaperTrader.resolve_all_pending()`). The bet resolution step finds all pending bets across multiple dates, checks if game stats are available, and resolves them automatically — enabling multi-day catchup. Supports `--dry-run` to preview commands and `--skip-resolution` to skip bet resolution. Resolution failures don't fail the job (stats are prioritized). Runtime: ~3-5 minutes (optimized from ~30 minutes via incremental scripts).

**Step resilience (2026-02-24):** Each step is marked critical or non-critical. Critical steps (CDN scrape, linker, rolling averages, opponent allowed) cause the job to abort on failure. Non-critical steps (team IDs backfill, position history, league averages) log a warning and continue to the next step. This ensures paper bet resolution always runs even when a non-critical step fails. Play type scraper (`play_type_scraper.py`) was removed from the daily pipeline because `stats.nba.com` blocks datacenter IPs (Railway, GitHub Actions) — can be re-added when the API becomes accessible.

**`lines_job.py`** — Multiple-times-daily props and injuries scraping. Two modes:
- **Full mode (`--live`):** `daily_game_lines_scraper.py` → `daily_player_props_scraper.py --live --target-table raw_player_props_combined` → `rapidapi_injury_backfill.py` → `link_injury_data.py` → `nba_linker_local.py incremental`. Used at 12 PM and 4 PM ET.
- **Props-only mode (`--live --props-only`):** `daily_player_props_scraper.py --live --target-table raw_player_props_combined` → `nba_linker_local.py incremental`. Skips game lines and injuries for fast intra-day refreshes. Used hourly/half-hourly between inference windows.
- Supports `--date`, `--dry-run`, `--skip-injuries`, `--skip-linker`, `--live`, `--props-only`. Runtime: ~30-90 seconds (full), ~15-30 seconds (props-only).

**`inference_job.py`** — Full prediction generation. Loads model artifacts (latest `run_*` directory), initializes Monte Carlo predictor with 10K samples and Gaussian copula, checks upstream data freshness (warns if rolling averages >2 days stale), generates predictions via `DailyPredictionRunner.run_for_date()`, stores to `daily_predictions` and `daily_prediction_samples` tables, **automatically places paper bets** on recommended predictions (via `PaperTrader.select_bets()` + `place_bets()`), sends Discord alert, and exports CSV backup. Runs twice daily (12:15 PM, 4:15 PM ET) to catch new player props. Supports `--date`, `--dry-run`, `--model-dir`, `--stats`, `--skip-bets`, `--skip-discord`. Runtime: ~1-3 minutes.

**`edge_refresh_job.py`** — Lightweight edge recalculation (~2-3 seconds). Loads stored predictions from `daily_predictions` and MC samples from `daily_prediction_samples` via `PredictionStore.get_all_samples_for_date()`, fetches fresh prop lines from `raw_player_props_combined`, recalculates edges (empirical CDF) and Black-Litterman recommendations, upserts updated predictions. Self-contained — does NOT instantiate model pipeline or feature store. Exits gracefully if no samples exist (inference hasn't run yet). Supports `--date`, `--dry-run`, `--stats`, `--skip-discord`. Runs after each intra-day props scrape.

**Cron Configuration:** See `cron/gameflow_crontab.txt` for Linux server deployment template with UTC times and environment setup instructions.

**Windows Task Scheduler:** For local Windows deployment, batch scripts in `scripts/` directory wrap each job:
- `scripts/run_daily_stats.bat` — Runs daily stats job
- `scripts/run_lines.bat` — Runs lines job
- `scripts/run_inference.bat` — Runs inference job

Scheduled tasks (GameFlow-DailyStats, GameFlow-Lines-12PM, GameFlow-Lines-4PM, GameFlow-Lines-6PM, GameFlow-Inference) execute these batch scripts at configured times. See `scripts/` directory for implementation. **Note:** Local tasks disabled as of 2026-02-14 in favor of Railway deployment.

**Railway Cloud Deployment (2026-02-14):** Production deployment uses Railway with APScheduler for job orchestration:
- `nixpacks.toml` — Nixpacks build config: Python venv with system-site-packages, explicit `LD_LIBRARY_PATH` for Nix-installed shared libraries (libz, libstdc++), zlib and stdenv.cc.cc.lib nixPkgs for numpy/scipy/xgboost C extensions
- `railway.toml` — Railway-specific build and deploy settings (nixpacks builder, restart policy)
- `src/orchestration/scheduler.py` — APScheduler-based scheduler runs 21 jobs on cron schedule (UTC times):
  - `daily_stats_job.py` — 9 AM ET (scrapes NBA game results)
  - `lines_job.py --live` — 12 PM, 4 PM ET (full live scrape: game lines + props + injuries + linker)
  - `inference_job.py` — 12:15 PM, 4:15 PM ET (full MC inference)
  - `lines_job.py --live --props-only` — 1, 2, 3, 4:30, 5, 5:30, 6, 6:30 PM ET (props-only scrape + linker)
  - `edge_refresh_job.py` — 2 min after each props-only run (recalculates edges from stored samples + fresh lines)
- **Discord job status alerts (2026-02-15):** Scheduler sends success/failure notifications to `#alerts` channel after each job completes. Includes job name, duration, metrics (when available), and error details for failures. Non-fatal — alert failures don't affect job execution.
- **Subprocess Python path (2026-02-18):** All orchestration job scripts use `sys.executable` instead of hardcoded `python` when spawning subprocesses, ensuring the venv Python (with all installed packages) is used consistently.
- Single always-on worker process handles all scheduled jobs
- Environment variables: `DATABASE_URL`, `ODDS_API_KEY`, `RAPIDAPI_KEY`, `DISCORD_CHANNEL_ALERTS`
- Model artifacts use "production folder" strategy: `src/models/artifacts/production/` is committed to git, `run_*/` directories are gitignored
- Promote models via `scripts/promote_model.py` — copies latest training run to production folder
- See `docs/railway_deployment.md` for full setup guide

### 10. Paper Trading (`src/paper_trading/`)

Paper bet placement, outcome resolution, and P&L tracking. Bet placement is automated via `inference_job.py` (runs after each prediction generation). Resolution is automated via `daily_stats_job.py` (runs each morning after games finalize). Manual CLI scripts also available for ad-hoc operations. Integrated with the Dashboard for visualization.

### 11. Dashboard (`dashboard/`)

Next.js web application for viewing daily predictions, analyzing player props, and tracking paper trading performance.

**Technology Stack:**
| Layer | Components |
|-------|------------|
| Framework | Next.js 16 with App Router |
| Language | TypeScript |
| Styling | Tailwind CSS |
| Data | Supabase (PostgreSQL) via `@supabase/ssr` |
| Charts | Recharts |
| Auth | Supabase Auth (email/password) |

**Directory Structure:**
```
dashboard/
├── src/
│   ├── app/                    # Next.js App Router pages
│   │   ├── (public)/           # Public routes (no auth required)
│   │   │   ├── page.tsx        # Landing page (free-beta + Discord CTA)
│   │   │   ├── picks/page.tsx  # Public picks teaser (3 real + blurred)
│   │   │   ├── pricing/page.tsx # $0/mo beta access card
│   │   │   ├── terms/page.tsx  # Terms of Service
│   │   │   └── privacy/page.tsx # Privacy Policy
│   │   ├── (auth)/             # Auth routes (redirect if logged in)
│   │   │   ├── login/page.tsx  # Login page
│   │   │   └── signup/page.tsx # Sign-up page
│   │   ├── (protected)/        # Auth-gated routes
│   │   │   ├── dashboard/page.tsx  # Main predictions dashboard
│   │   │   ├── dfs/page.tsx          # DFS Edge Finder — DFS vs model comparison
│   │   │   ├── history/page.tsx    # Bet history with filters
│   │   │   ├── performance/page.tsx # Performance metrics
│   │   │   ├── account/page.tsx    # Profile + community card
│   │   │   ├── stats/page.tsx        # Data Vault — heatmap stat tables
│   │   │   └── subscribe/page.tsx  # Redirects to /dashboard
│   │   ├── auth/callback/route.ts  # Auth callback for email confirmation
│   │   └── layout.tsx          # Root layout with dark theme
│   ├── components/
│   │   ├── landing/            # HeroSection, FeatureGrid
│   │   ├── layout/             # Navbar, PublicNavbar, Footer
│   │   ├── predictions/        # PropCard, PropGrid, FilterTabs, PlayOfTheDay
│   │   ├── dfs/                # DfsTable, DfsFilters — DFS edge comparison
│   │   ├── stats/              # HeatmapTable, StatTabs, CategoryTabs, WindowToggle, PositionFilter, OffDefToggle
│   │   ├── analysis/           # AnalysisModal, Last5Chart, QuantileSummary
│   │   ├── history/            # BetCard, BetList, HistoryFilters, HistorySummary
│   │   ├── performance/        # KPICard, BankrollChart, StatBreakdown
│   │   ├── subscription/       # PricingCard (dormant, for future Stripe)
│   │   └── shared/             # PlayerAvatar, Badge, BetSourceFilter components
│   ├── lib/
│   │   ├── supabase/           # Client, server, and middleware helpers
│   │   ├── constants.ts        # DISCORD_URL, TEAM_ABBREV shared map
│   │   ├── dfs-utils.ts        # Quantile interpolation, DFS EV calculations
│   │   ├── insights.ts         # Template-based insight generator
│   │   ├── sportsbook-availability.ts # US state → legal bookmaker mapping for line filtering
│   │   ├── stats/columns.ts    # Column definitions for Data Vault heatmap tables
│   │   ├── stats/pivotPlayTypes.ts # Client-side pivot for play type long→wide format
│   │   ├── subscription.ts     # Subscription types/utils (dormant)
│   │   └── utils.ts            # Formatting, edge tiers, headshot URLs
│   ├── types/
│   │   ├── predictions.ts      # TypeScript interfaces for predictions, bets, performance
│   │   ├── dfs.ts              # DFS line types, slip types, platform constants
│   │   ├── stats.ts            # Types for Data Vault (ColumnDef, StatRow, SortState)
│   │   └── subscription.ts     # Subscription type definitions (dormant)
│   └── middleware.ts           # Auth redirect for protected routes
├── .env.local                  # Supabase credentials (not committed)
└── next.config.ts              # NBA CDN image domains
```

**Key Features:**
- **Play of the Day:** Featured card at the top of the dashboard highlighting the model's highest-edge pick. Amber/gold visual treatment with trophy badge, large player avatar, star rating, and prominent edge display. Respects current filter settings (date, edge threshold, BL blending). Clicking "Analyze Pick" opens the analysis modal.
- **Predictions View (`/`):** Displays predictions filtered by stat type (pts/reb/ast), sorted by edge magnitude. Matchup filter allows viewing predictions for specific games (e.g., "LAL vs SAS"). Includes:
  - **Date Selector:** View predictions from any date in the last 30 days (uses `get_prediction_dates()` RPC function for efficient distinct query)
  - **Edge Threshold Filter:** Filter picks by minimum edge (All, ≥3%, ≥5%, ≥7%, ≥10%, ≥15%, ≥20%)
  - **Black-Litterman Blending Filter:** Optionally apply BL blending to edges (Off, τ=0.03, τ=0.05, τ=0.10, τ=0.15, τ=0.25). BL calculation implemented client-side using `calculateBLConfidence()` and `blendProbability()` utility functions.
- **Analysis Modal:** Click any prop card to see:
  - Last 5 games chart with performance history
  - Quantile distribution summary with visual bar
  - Sportsbook line shopping with actual edge calculations
  - Kelly bet sizing calculator with bankroll input and fraction selection
  - Model probabilities, market implied probabilities, and edge breakdown
- **State Selector:** Dropdown filter persisted to localStorage (`user_state`). Filters AnalysisModal sportsbook lines to only show bookmakers legal in the selected state. Offshore books (Pinnacle, Novig, ProphetX, Bovada) excluded from all states. Mapping in `sportsbook-availability.ts` covers ~26 legal sports betting states.
- **Line Shopping:** Shows all available bookmaker lines for each prop (filtered by state if set). For Over bets, lower lines are better; for Under bets, higher lines are better. Displays estimated probability and edge for each line. Lines are clickable — selecting a line recalculates the bet sizing section using that line's odds and model probability. Defaults to the best-edge line.
- **Kelly Sizing:** Bankroll persisted to localStorage. Preset Kelly fractions (Full, Half, Quarter, Eighth) or custom decimal input. Displays recommended bet size based on edge and odds from the selected sportsbook line.
- **History View (`/history`):** Shows past betting results with bet source filter (Model Picks/All Bets), status filters (All/Won/Lost/Push), summary stats bar, and individual bet cards with actual vs line comparison. Model Picks filter shows only bets with edge ≥9% (matching production model configuration). Displays bookmaker badge on each bet card showing which sportsbook had the sharpest line.
- **Performance View (`/performance`):** KPI cards (bankroll, P&L, ROI, win rate), bankroll over time chart (Recharts AreaChart), and performance breakdown by stat type. Includes bet source filter to view Model Picks performance separately from all bets. Model Picks view simulates what bankroll would be if only high-edge bets were taken.
- **Player Avatars:** NBA headshots from CDN with fallback to inline SVG placeholder.
- **Bankroll Tracking:** Navbar displays current paper trading bankroll from `paper_trading_daily_log`.
- **Auth Protection:** Middleware redirects unauthenticated users to `/login`.
- **Free Beta Model:** No paywall — all authenticated users have full access. Public `/picks` page shows 3 real picks via `get_public_picks()` RPC to drive signups. All CTAs point to sign-up and Discord. Stripe infrastructure preserved (dormant) for future activation at ~200 Discord members.
- **Data Vault (`/stats`):** Dense heatmap stat table with player, team, defense-vs-position, and play type breakdowns. Features percentile-based blue heatmap coloring (5-step gradient with inline legend), sortable columns, sticky name/position/team columns, window toggles (L5/L15/SZN), category tabs (Box Score/Shooting/Advanced/Consistency for players), position and team filters with info button explaining G/W/B groups, stat header tooltips, and player search. Reads from 3 database views (`player_stats_latest`, `team_stats_latest`, `defense_by_position_latest`) plus the `team_play_types` table (Synergy play type data) that join rolling average tables with player/team reference data. All filtering and sorting is client-side after initial parallel fetch.
- **DFS Edge Finder (`/dfs`):** Compares DFS platform lines (PrizePicks, Underdog, Pick6, Betr) against the model's true probabilities. For each DFS line, re-estimates model probability at the DFS-specific line (which may differ from the sharp sportsbook line) via quantile interpolation using `estimateOverProb`/`estimateUnderProb` from `dfs-utils.ts`. Computes EV against DFS break-even thresholds per slip type (UD 3/5-Pick, PP 5/6-Flex). Platform filter tabs, slip type selector, stat filter, +EV toggle, KPI summary cards, and sortable table. Data fetched via `get_dfs_lines` RPC function with partial index on 26M+ row table.
- **Route Groups:** `(public)` for landing/picks/pricing/legal, `(auth)` for login/signup (redirects if already logged in), `(protected)` for dashboard/history/performance/account/stats/dfs (requires auth).

**Data Sources:**
- `daily_predictions` table — prediction quantiles, edges, implied probabilities, bookmaker (sharpest line source)
- `players` table — player names for enrichment
- `player_game_stats` table — historical game performance for Last 5 chart
- `raw_player_props_combined` table — bookmaker lines for line shopping
- `paper_bets` table — individual bet records with status and P&L
- `paper_trading_daily_log` table — daily aggregated stats, bankroll tracking
- `player_stats_latest` view — Data Vault player tab (rolling averages + advanced stats)
- `team_stats_latest` view — Data Vault team tab (rolling team averages)
- `defense_by_position_latest` view — Data Vault defense tab (defense-vs-position stats)
- `team_play_types` table — Season-level Synergy play type data (30 teams x 11 play types x 2 groupings)
- SQL view definitions version-controlled in `sql/views/` (player_stats_latest.sql, team_stats_latest.sql, defense_by_position_latest.sql). All views use deterministic `DISTINCT ON` with `game_id DESC` tiebreaker.

**Run Commands:**
```bash
cd dashboard && npm run dev    # Development server at localhost:3000
cd dashboard && npm run build  # Production build
cd dashboard && npm run lint   # ESLint check
```

| Module | Purpose |
|--------|---------|
| `paper_trader.py` | Core `PaperTrader` class — bet selection, Kelly sizing, outcome resolution, daily log updates |
| `place_bets.py` | CLI to place paper bets from daily predictions |
| `resolve_bets.py` | CLI to resolve bets using actual game results |

**Database Tables:**
- `paper_bets` — Individual bet records with odds, edge, stake, status, P&L. Unique on `(game_date, player_id, stat_type, bet_direction)`.
- `paper_trading_daily_log` — Daily aggregated stats: wins/losses, total staked, P&L, cumulative bankroll. Unique on `game_date`.

**Execution Flow (Automated):**
```
1. Inference Job (12:15 PM, 4:15 PM ET):
   inference_job.py → predictions stored → PaperTrader.select_bets() → place_bets()
   └── Reads daily_predictions → Filters by edge/is_recommended → Writes to paper_bets

2. Daily Stats Job (9:00 AM ET, next day):
   daily_stats_job.py → stats scraped → PaperTrader.resolve_all_pending()
   └── Reads player_game_stats → Compares to lines → Updates paper_bets & daily_log
```

**Manual CLI (ad-hoc operations):**
```
python src/paper_trading/place_bets.py --date 2026-02-04      # Place bets manually
python src/paper_trading/resolve_bets.py --date 2026-02-04    # Resolve bets manually
python src/paper_trading/backfill_paper_bets.py --start X --end Y  # Backfill + resolve range
```

**Bet Selection Logic:**
- Query `daily_predictions` for target date (pts, reb, ast stats)
- Filter predictions where `over_edge` or `under_edge` exceeds threshold (default 5%)
- Choose direction with higher edge
- Calculate stake via fractional Kelly (default 12.5%)
- Cap at max 5% of bankroll per bet

**Resolution Logic:**
- Fetch actual stats from `player_game_stats`
- **DNP/0-minute void (2026-02-24):** Players with `did_not_play=True` OR `min=0` are voided (status=`cancelled`, pnl=0). Matches sportsbook behavior where bets on DNP players are refunded.
- Compare actual vs line → won/lost/push/cancelled
- Calculate P&L: won = stake × (decimal_odds - 1), lost = -stake
- Update `paper_trading_daily_log` with aggregates

### 12. Social Media Image Generator (`src/social/`)

CLI tool for generating branded pick images for Instagram, TikTok, and Discord marketing.

**Directory Structure:**
```
src/social/
├── __init__.py              # Package marker
├── theme.py                 # Colors, fonts, layout constants, drawing helpers
├── data_provider.py         # Sync DB queries for picks + results
├── card_renderer.py         # HeadshotCache + 3 renderer classes
└── generate_images.py       # CLI entry point (argparse + orchestration)

assets/fonts/
├── Montserrat-Bold.ttf      # Google Fonts (OFL license)
├── Montserrat-SemiBold.ttf
└── Montserrat-Medium.ttf
```

**Card Types:**
| Card | Description | Formats |
|------|-------------|---------|
| **Slate Card** | Top 3-5 picks on one image — main daily post | 1080x1080, 1080x1920 |
| **Pick Card** | Single player feature with headshot, stars, projection | 1080x1080, 1080x1920 |
| **Results Card** | Yesterday's outcomes with hit/miss, P&L, season stats | 1080x1080, 1080x1920 |

**Design:**
- Dark theme matching dashboard Tailwind (slate-950 backgrounds, green/yellow/slate edge tiers)
- Star ratings (1-5) using same formula as PropCard.tsx: `min(5, max(1, ceil(abs(edge) * 50)))`
- Confidence labels ("Strong Edge" / "High Confidence" / "Lean") — no exact percentages
- NBA player headshots from CDN with disk cache and placeholder fallback
- Stat badges color-coded (blue=PTS, teal=REB, purple=AST)

**Data Provider:** Sync SQLAlchemy queries via `get_engine()` — does NOT reuse async Discord services.

**CLI Usage:**
```bash
# Daily slate
python src/social/generate_images.py --date 2026-02-18 --type picks

# Both picks + yesterday's results
python src/social/generate_images.py --date 2026-02-18 --type both --individual

# Story format for IG stories / TikTok
python src/social/generate_images.py --date 2026-02-18 --type picks --format story

# Dry run (no DB, no image output)
python src/social/generate_images.py --date 2026-02-18 --type picks --dry-run
```

**Output:** Images saved to `output/social/` (gitignored). Headshots cached to `data/headshots/` (gitignored via `data/`).

### 13. Discord Bot (`src/discord_bot/`)

Interactive Discord bot for sending daily prediction alerts and responding to slash commands. Development plan at `docs/discord_bot_development.md`.

**Directory Structure:**
```
src/discord_bot/
├── __init__.py
├── bot.py                    # GameFlowBot class with slash commands
├── run_bot.py                # Entry point with graceful shutdown
├── alerts.py                 # REST API alert sender (no bot process needed)
├── commands/__init__.py
├── services/
│   ├── __init__.py
│   ├── predictions.py        # Query daily_predictions table
│   └── paper_trading.py      # Query paper_bets, paper_trading_daily_log
└── formatters/
    ├── __init__.py
    └── embeds.py             # Discord embed builders
```

**Slash Commands:**
- `/picks [stat] [min_edge]` — Today's top predictions (filter by pts/reb/ast, edge threshold)
- `/player <name>` — Predictions for a specific player (fuzzy match supported)
- `/bankroll` — Paper trading balance, daily P&L, total P&L
- `/performance [days]` — Win rate, ROI, total bets (last 30 days default)
- `/toppicks` — Top 5 high-edge picks (alert preview)

**Architecture:**
- **Discord.py 2.6+** with slash commands via `@bot.tree.command()`
- **Async services** — `asyncio.to_thread()` wraps synchronous SQLAlchemy queries
- **Rich embeds** — Color-coded formatters for picks, players, bankroll, performance
- **REST API alerts** — `alerts.py` sends messages without bot process (for inference job integration)
- **Graceful shutdown** — SIGINT/SIGTERM handling for Railway compatibility

**Automated Alerts:**
- **Prediction alerts:** Inference job triggers alert after predictions stored → `#predictions` channel
- **Job status alerts:** Scheduler sends success/failure notifications for all jobs (daily_stats, lines, inference) → `#alerts` channel
- **Daily P&L summary:** After bet resolution, sends daily P&L, win/loss record, and bankroll → `#performance` channel
- Uses `send_*_sync()` wrappers — works without bot process running via REST API
- Skip prediction alerts with `--skip-discord` flag on inference job

**Environment Variables:**
```
DISCORD_BOT_TOKEN=...
DISCORD_CHANNEL_PREDICTIONS=...
DISCORD_CHANNEL_ALERTS=...
DISCORD_CHANNEL_PERFORMANCE=...
```

**Hosting:**
- **Phase 1 (Local):** Windows Task Scheduler via `scripts/run_discord_bot.bat`
- **Phase 2 (Railway):** Add second service with `startCommand = "python src/discord_bot/run_bot.py"`

**Database Queries:** Uses `daily_predictions` for picks, `paper_trading_daily_log` for bankroll, `paper_bets` for performance stats. Queries use correct column names: `status` (not `result`), `stat_type` (not `stat`), `bankroll_after` (not `current_bankroll`).

---

## Database Schema Highlights

### Core Stats
- `player_game_stats`: Box scores (pts, reb, ast, min, etc.).
- `team_game_stats`: Team-level metrics (pace, efficiency, basic + advanced).
- `player_game_advanced_stats`: Derived metrics (usage%, TS%, PIE).

### Rolling Context (Pre-Computed)
- `player_average_game_stats`: L5/L15/Season player averages.
- `player_average_advanced_stats`: L5/L15/Season advanced metric averages.
- `team_average_game_stats`: Team rolling trends.
- `team_allowed_by_position`: **Critical defensive metric.** Tracks how well teams defend specific positions (e.g., "Celtics defense vs. Point Guards L15").

### Historical Priors
- `league_priors_history`: League-average baselines used for Bayesian shrinkage when player sample size is low (rookies/injuries).

### Injury Data
- `rapidapi_injuries`: Historical injury reports (88K+ rows, 2021-present). Columns: `player_name`, `player_id`, `team_id`, `status` (Out/Questionable/Probable/Day-To-Day), `report_date`. Linked to NBA IDs via `link_injury_data.py` (99.3% coverage).

### Betting Data
- `raw_game_lines_staging`: Spreads and totals.
- `raw_player_props_combined`: Player prop lines and odds (~25M rows). Includes:
  - **Core markets:** `player_points`, `player_rebounds`, `player_assists`, `player_threes`, `player_steals`, `player_blocks`, `player_turnovers`
  - **Combo markets (added 2026-01-31):** `player_points_rebounds_assists` (PRA), `player_points_rebounds`, `player_points_assists`, `player_rebounds_assists`, `player_blocks_steals`, `player_field_goals`

### Predictions
- `daily_predictions`: Stored daily prediction quantiles, edges, and implied probabilities. Unique on `(prediction_date, player_id, game_id, stat)`. Supports upsert for re-runs.
- `daily_prediction_samples`: Gzip-compressed MC sample arrays (10K float64 values per prediction, ~20-40KB). Unique on `(prediction_date, player_id, game_id, stat)`.

### Paper Trading
- `paper_bets`: Individual paper bet records with full context (odds, edge, stake, status, P&L). Unique on `(game_date, player_id, stat_type, bet_direction)`.
- `paper_trading_daily_log`: Daily aggregated P&L tracking. Unique on `game_date`. Tracks wins/losses, total staked, ROI, cumulative P&L, and running bankroll.

### Dashboard Views (Pre-Computed Joins)
- `player_stats_latest`: Latest per-player rolling stats — joins `player_average_game_stats` + `player_average_advanced_stats` + `players` + `player_position_history`. ~529 rows.
- `team_stats_latest`: Latest per-team rolling stats — joins `team_average_game_stats` + `teams`. 30 rows.
- `defense_by_position_latest`: Latest defense-vs-position — joins `team_allowed_by_position` + `teams`, filtered to G/W/B positions. 90 rows.
- `team_play_types`: Season-level Synergy play type frequency and efficiency. 660 rows (30 teams x 11 play types x offensive/defensive). Public read RLS.

### Reference
- `players`: Player reference data.
- `teams`: Team reference data.
- `player_position_history`: Position tracking over time.

---

## Data Flow Diagram

```mermaid
graph TD
    subgraph "External Sources"
        NBA[NBA API]
        Odds[Sportsbooks]
        ESPN[ESPN Injuries]
    end

    subgraph "Ingestion & Linking"
        RawStats[Raw Stats Tables]
        Staging[Staging Tables]
        Linker(NBA Linker Process)
    end

    subgraph "Processing & Features"
        AvgStats[Populate Averages]
        DefMetrics[Opponent Defense Metrics]
        Priors[League Priors]
        FeatStore(Feature Store Engine)
        FeatSelect(Feature Selector)
    end

    subgraph "Modeling"
        HPO[Hyperparameter Tuner]
        Trainer[Quantile Trainer]
        Calibrator[Calibration Evaluator]
        MC[Monte Carlo Sim]
        BL[Black-Litterman Blender]
    end

    subgraph "Execution"
        Backtest[Backtest Harness]
        BetSim[Bet Simulator]
        PerfMetrics[Performance Metrics]
        Daily[Daily Runner]
        PredStore[Prediction Store]
        QueryTool[Query Tool CLI]
        Viz[Visualize Results]
        DB[(PostgreSQL)]
    end

    NBA --> RawStats
    Odds --> Staging
    ESPN --> Staging
    RawStats --> AvgStats
    RawStats --> DefMetrics
    RawStats --> Priors
    Staging --> Linker
    Linker --> DB
    AvgStats --> DB
    DefMetrics --> DB
    Priors --> DB

    DB --> FeatStore
    FeatStore --> FeatSelect
    FeatSelect --> HPO
    HPO --> Trainer
    FeatSelect --> Trainer
    Trainer --> Calibrator
    Calibrator --> MC
    MC --> Daily
    MC --> BL
    BL --> Backtest
    MC --> Backtest
    Daily --> PredStore
    PredStore --> DB
    DB --> QueryTool
    Backtest --> BetSim
    BetSim --> PerfMetrics
    PerfMetrics --> Viz
```

---

## Entry Points & CLI

### Scrapers
```bash
python src/scrapers/nba_unified_scraper.py [--season YYYY-YY] [--season-type TYPE] [--skip-team] [--skip-advanced]
python src/scrapers/daily_player_props_scraper.py [--live|--date YYYY-MM-DD] [--combos|--combos-only|--markets M1 M2]
python src/scrapers/player_prop_scraper.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--combos|--combos-only] [--dry-run]
python src/scrapers/daily_game_lines_scraper.py
```

### Processing
```bash
python src/processing/nba_linker_local.py [download|process|upload|incremental]
python src/processing/nba_linker_local.py incremental [--batch-size 50000] [--limit N]  # Lightweight daily mode
python src/processing/populate_average_stats.py [--season YYYY-YY] [--table player]  # Full historical recalculation
python src/processing/populate_average_stats_incremental.py [--date YYYY-MM-DD]  # Lightweight daily update (~1s)
python src/processing/backfill_opponent_allowed.py
python src/processing/backfill_league_priors.py
```

### Training
```bash
python -m src.models.train_pipeline [--tune-hyperparams] [--tuning-trials N]
python -m src.models.hyperparameter_tuner [--n-trials 50] [--timeout 3600]
```

### Backtesting
```bash
python src/backtesting/run_backtest.py --start YYYY-MM-DD --end YYYY-MM-DD
python src/backtesting/run_backtest.py --start YYYY-MM-DD --end YYYY-MM-DD --bl-tau 0.05  # Enable BL blending
python src/backtesting/run_backtest.py --start YYYY-MM-DD --end YYYY-MM-DD --allowed-bets pts:under reb:over
python src/backtesting/visualize_results.py --results-dir backtest_results/

# Per-stat edge thresholds (different minimums per stat)
python src/backtesting/run_backtest.py --start YYYY-MM-DD --end YYYY-MM-DD \
    --edge-threshold pts=0.10 reb=0.07 ast=0.15

# Per-stat BL tau (use "none" to disable BL for a stat)
python src/backtesting/run_backtest.py --start YYYY-MM-DD --end YYYY-MM-DD \
    --bl-tau pts=0.05 reb=0.10 ast=none

# Mixed: global default + per-stat overrides
python src/backtesting/run_backtest.py --start YYYY-MM-DD --end YYYY-MM-DD \
    --edge-threshold 0.05 pts=0.10

# Parameter sweep (BL tau × edge threshold × Kelly fraction)
python src/backtesting/run_sweep.py \
    --start YYYY-MM-DD --end YYYY-MM-DD \
    --model-dir src/models/artifacts/run_YYYYMMDD_HHMMSS \
    --tau none 0.03 0.05 0.09 0.15 \
    --edge 0.03 0.05 0.07 \
    --kelly 0.10 0.125 0.15 \
    --n-samples 10000 --stats pts reb ast
```

### Daily Workflow
```bash
# Full pipeline (legacy)
python src/orchestration/run_daily.py [--date YYYY-MM-DD] [--skip-scraping] [--skip-processing] [--skip-inference] [--skip-storage] [--scrape-injuries]

# Frequency-separated jobs (E6)
python src/orchestration/daily_stats_job.py [--dry-run]                           # 9 AM ET - Stats + processing
python src/orchestration/lines_job.py --live [--dry-run]                          # 12/4 PM ET - Full live scrape
python src/orchestration/lines_job.py --live --props-only [--dry-run]             # Hourly/half-hourly - Props only
python src/orchestration/lines_job.py [--date YYYY-MM-DD] [--dry-run] [--skip-injuries] [--skip-linker]  # Historical mode
python src/orchestration/inference_job.py [--date YYYY-MM-DD] [--dry-run] [--model-dir PATH] [--stats pts reb ast] [--skip-bets] [--skip-discord]  # 12:15/4:15 PM ET
python src/orchestration/edge_refresh_job.py [--date YYYY-MM-DD] [--dry-run] [--stats pts reb ast] [--skip-discord]  # After each props scrape
```

The `--scrape-injuries` flag:
1. Fetches injuries for the target date from RapidAPI → `rapidapi_injuries` table
2. Runs `link_injury_data.py` to populate `player_id` column via fuzzy matching
3. Enables injury features in feature store and injury filtering in daily runner

### Query Predictions
```bash
# Probability of a player scoring over a line
python src/tools/query_player.py --player "Cade Cunningham" --stat pts --line 25.5

# Same query with EV at given odds
python src/tools/query_player.py --player "Cade Cunningham" --stat pts --line 25.5 --odds -110

# All predictions for a player
python src/tools/query_player.py --player "Cade Cunningham"

# Top edges for today
python src/tools/query_player.py --top 20

# Top edges for a specific date
python src/tools/query_player.py --date 2026-01-29 --top 10
```

### Social Media Images
```bash
# Daily slate card
python src/social/generate_images.py --date 2026-02-18 --type picks

# Results recap from yesterday
python src/social/generate_images.py --date 2026-02-17 --type results

# Both picks + results (results auto-fetched from yesterday)
python src/social/generate_images.py --date 2026-02-18 --type both

# Story format (1080x1920) + individual player cards
python src/social/generate_images.py --date 2026-02-18 --type picks --format story --individual

# Dry run (no DB, no images)
python src/social/generate_images.py --date 2026-02-18 --type picks --dry-run
```

### Paper Trading
```bash
# Place paper bets from predictions
python src/paper_trading/place_bets.py --date 2026-02-04

# Dry-run to see bets without placing
python src/paper_trading/place_bets.py --date 2026-02-04 --dry-run

# Custom edge threshold (global)
python src/paper_trading/place_bets.py --date 2026-02-04 --edge-threshold 0.08

# Per-stat edge thresholds (different minimums per stat)
python src/paper_trading/place_bets.py --date 2026-02-04 --edge-threshold pts=0.10 reb=0.07 ast=0.15

# Resolve bets after games complete
python src/paper_trading/resolve_bets.py --date 2026-02-04

# Dry-run resolution
python src/paper_trading/resolve_bets.py --date 2026-02-04 --dry-run
```

---

## Testing Architecture

**Framework:** Pytest with pytest-cov (60% coverage target).

**Test Organization:** 34 test modules in `tests/` mirroring `src/` structure. Each source module has a corresponding `test_*.py`.

**Test Categories (markers):**
- `unit` — Isolated logic tests with mocks.
- `integration` — Tests requiring database or external services.
- `slow` — Long-running tests (backtesting, full pipeline).

**Patterns:**
- Mock-based unit tests with proper fixtures for database interactions.
- Time-series aware validation (chronological ordering enforced).
- All scrapers tested with mocked HTTP responses.
- **CI-safe imports:** All source modules with env-var dependencies use lazy initialization — `sys.exit()`/`raise` deferred from module-level to `if __name__ == "__main__"` or function calls. Enables pytest collection without credentials.

**Configuration:** `pyproject.toml` contains pytest, coverage, and ruff settings. Line length 120, Python 3.11 target.

---

## Critical Invariants & Rules

1.  **Temporal Integrity:**
    - **Rule:** Feature generation must ONLY use data where `game_date < target_game_date`.
    - **Reason:** Prevents "look-ahead bias" where the model accidentally learns from the future (e.g., knowing a player played 40 minutes makes predicting points too easy).
    - **Implementation Note (fixed 2026-02-09):** Pre-computed rolling averages in `player_average_game_stats` use `shift(1)` during population, meaning the row for `game_date X` contains averages from games BEFORE X. Therefore, feature store queries use `<= game_date` (not `< game_date`) to get the correct pre-computed features for each game. The previous `<` logic caused an off-by-one error where models used stale features (one game behind).

2.  **Minutes Dependency:**
    - **Rule:** We model **Rate** (Stats per Minute) and **Minutes** separately.
    - **Reason:** Variance in NBA stats is heavily driven by playing time. Predicting them independently allows for more robust handling of blowouts or overtime.

3.  **Bayesian Fallback:**
    - **Rule:** If a player has < 5 games of history, blend their stats with `league_priors_history`.
    - **Reason:** Prevents wild outliers for rookies or players returning from long injuries.

4.  **Quantile Crossing:**
    - **Rule:** `Q10 <= Q25 <= Q50 <= Q75 <= Q90`.
    - **Reason:** Statistical necessity. If raw model output violates this (e.g., Q90 < Q75), isotonic regression is applied to force monotonicity.

5.  **Empirical CDF (not Gaussian):**
    - **Rule:** Probability of over/under is computed as `(samples > line).mean()`, never via Gaussian CDF.
    - **Reason:** Monte Carlo distributions are non-Gaussian. Gaussian CDF produces phantom edges (see ACTIONITEMS.md — Key Findings Archive).

---

## Known Issues & Active Research

See `ACTIONITEMS.md` for full details.

**Root finding (2026-01-28):** The model is catastrophically overconfident on probability estimates. Quantile calibration (Q10–Q90) is good, but translating MC distributions into betting probabilities via `(samples > line).mean()` amplifies small mean shifts into extreme P(over) values. Brier score 0.2705 (worse than naive 0.2500).

**Black-Litterman blending (A3) — Implemented (2026-01-28):** The BL blending layer is complete and integrated into the backtesting pipeline. Anchors model probabilities to the devigged market prior using log-odds space blending with per-prediction z-score confidence. Activated via `--bl-tau` flag (default: disabled). Needs validation backtest via `run_sweep.py` to find optimal tau, edge, and Kelly parameters.

**Bug fix sweep (2026-01-30 + 2026-02-19):** 30 of 43 total issues fixed from two comprehensive pipeline audits — see `ISSUES.md`. Session 43 added fixes for opponent-allowed rolling windows (.sum() → .mean()), backtesting odds timestamp cutoff, batch upserts, view tiebreakers, inference freshness checks, and Data Vault display bugs. 13 issues remain open (mostly low-priority/cosmetic).

**Calibration fixes (2026-01-31):** Applied conformal recalibration (quantile_trainer.py) — post-training offset from validation residuals closes coverage gaps > 3%. Zero-snap handling (monte_carlo.py) snaps values below 1e-3 in inverse CDF to exactly 0.

**BL parameter sweep results (2026-01-31):** Comprehensive sweep revealed:
- **No-BL configs are profitable:** +3% ROI, 600-873 bets across edge/Kelly combinations. REB is the strongest stat at +7.9% ROI.
- **ALL BL configs produce 0-12 bets:** The BL confidence formula `confidence = 1 - exp(-0.5 * z²)` produces near-zero values for realistic betting edges. For a 3% raw edge (P(over)=0.55), z~0.13, confidence~0.008. Combined with tau, `w = tau * confidence` is vanishingly small (~0.0008), crushing edges below any practical threshold.
- **Fixed (2026-02-05):** Replaced exponential confidence with linear ramp `confidence = min(z / z_max, 1.0)`. At z=0.13, confidence is now 0.13 instead of 0.008 — a 16x improvement in effective weight. BL should now produce meaningful bet counts.

**Active tracks:**
- **Track A** (Critical): Probability recalibration — A1–A4 all implemented. A3b (BL confidence fix) completed. A5 (residual classifier) pending evaluation. A6 (conditional rate modeling) added as future option.
- **Track B** (Complete): New signal sources — B1 (injury context, 10 features), B2 (rest/schedule), B3 (short-window trends), B4 (minutes stability) all implemented and included in latest training run.
- **Track C**: Calibration refinement — C0 (Gaussian copula) implemented and active. C1 (Q10 over-coverage) investigated through Sessions 40-42: surgical retrains, feature reselection, per-quantile tuning, and combined conformal recalibration all tested. **Conclusion:** AST Q10 combined gap is structural (~18% zero-assist rate sets coverage floor). A/B backtest confirmed offsets hurt betting ROI despite improving calibration metrics. C1 closed — not fixable without negative stat predictions. C2 (per-stat calibration diagnostic) implemented 2026-02-18 — `src/diagnostics/calibration_per_stat.py`. C3-C5 (THREES model experiments) archived 2026-02-10 due to poor market coverage.
- **Track D**: Deprioritized model items (pending recalibration).
- **Track E**: Go-live pipeline — no-BL path shows positive ROI (+3%). E4 (daily injury pipeline) and E5 (paper trading infra) complete. E6 (scheduling) pending.

**Prediction storage + query tool (2026-01-31):** Daily predictions and MC samples now persisted to PostgreSQL (`daily_predictions` + `daily_prediction_samples` tables). CLI query tool (`src/tools/query_player.py`) enables ad-hoc probability queries against stored distributions. Daily runner refactored: NBA API ScoreboardV2 for game discovery, `rapidapi_injuries` for injury filtering, MC samples for edge calculation, `ROW_NUMBER` snapshot ranking for line freshness.

**Current state (2026-02-10):** Models trained for PTS, REB, AST stats — latest complete artifact: `run_20260205_165808`. Daily inference pipeline fully wired to DB storage. BL confidence function fixed with linear ramp — now produces meaningful weights for realistic edges. **THREES model archived (2026-02-10):** All THREES-related code (C3 hurdle, C4 NegBin, C5 multiclass) moved to `archive/threes_model/` due to poor market coverage (50% missing lines) and insufficient betting volume (2 bets out of 78 in backtest). Scrapers still collect `player_threes` market data for future optionality. **Training safety pattern added** — Training creates `_incomplete` suffix directory, renamed atomically after all artifacts saved. Inference job filters out incomplete directories. Prevents race condition when training and inference overlap. **Incremental linker added:** Lightweight `incremental` command for daily automated linking without downloading full 25M+ row tables. Queries only unlinked records, matches against reference tables, updates directly via batched SQL. Integrated into `run_daily.py`. Test results: 99.3% player match rate, 40.7% game match rate (future games not yet in DB). **E6 Daily Pipeline Automation (2026-02-05):** Three frequency-separated job scripts created for cron scheduling — `daily_stats_job.py` (once daily), `lines_job.py` (multiple times daily), `inference_job.py` (pre-game). Cron template at `cron/gameflow_crontab.txt`. **Dashboard History & Performance Pages (2026-02-10):** Added `/history` and `/performance` routes with full UI components for viewing betting history and performance metrics. **Dashboard Analysis Modal Enhancements (2026-02-10):** Added sportsbook line shopping with proper Under bet EV calculation (higher lines = easier to hit), Kelly bet sizing calculator with localStorage-persisted bankroll and preset/custom fraction toggle, matchup-based game filter on main page. RLS policies added for `player_game_stats` and `raw_player_props_combined` tables to enable browser-side data access.

**Backtesting fixes (2026-02-07):**
1. **Incomplete model directory validation:** `find_latest_model_dir()` in `run_sweep.py` now skips incomplete training runs (directories without `minutes_model.joblib`). Prevents silent failures when an aborted training run is selected.
2. **Game ID format mismatch fix:** Backtest harness query now uses `LPAD(rp.game_id, 10, '0')` to handle both 8-digit and 10-digit game_id formats. The linker was storing game_ids without leading zeros (e.g., "22500589" vs "0022500589"), causing JOIN failures for ~99% of props data.
3. **Linker leading zeros preservation:** Added `.zfill(10)` when storing game_ids in lookup dictionaries to ensure consistent 10-digit format for future linker runs.

**Impact:** Lines fetched increased from 33,962 to 191,908. Bets increased from 889 to 2,251. Full date coverage restored (Jan 1-29 instead of just Jan 1-15).

**Feature store off-by-one fix (2026-02-09):**
1. **Off-by-one bug in LATERAL JOINs:** Feature store queries used `< game_date` to fetch pre-computed rolling averages, but `player_average_game_stats` uses `shift(1)` during population — meaning the row for game_date X already contains averages from games BEFORE X (not including X). The `<` logic caused queries to fetch the PREVIOUS game's row instead of the current game's row, resulting in stale features (one game behind).
2. **Fix applied:** Changed `< game_date` to `<= game_date` in 15 LATERAL JOINs across 3 feature store methods: `get_features_for_date()`, `get_features_for_date_range()`, and `_load_single_season_training()`. Added explanatory comments.
3. **Injury queries unchanged:** Queries that look up OTHER players' historical stats (e.g., teammates out with injuries) correctly use `<` since they're fetching past game data, not pre-computed rolling stats.
4. **Daily runner recency filter:** Added 30-day cutoff filter to `_get_players_for_games()` to exclude retired players (e.g., Shaquille O'Neal) from predictions.

**Impact:** Models will now train and predict with current-game features instead of stale one-game-behind features. Requires model retraining to benefit from fix.

**Per-stat configuration system (2026-02-10):** Added `src/config/stat_config.py` module enabling different edge thresholds and BL tau values for each stat type (pts, reb, ast). Backtesting showed REB performs best (+7.9% ROI) while AST is marginal — per-stat tuning allows tighter thresholds on weaker stats and looser on stronger ones.
1. **New module:** `StatConfig` dataclass for per-stat settings, `StatConfigSet` container with global fallbacks and CLI parsing via `from_cli_args()`.
2. **CLI format:** `--edge-threshold pts=0.10 reb=0.07 ast=0.15` for per-stat, or `--edge-threshold 0.05` for global (backward compatible).
3. **Files updated:** `bet_simulator.py`, `backtest_harness.py`, `run_backtest.py`, `run_sweep.py`, `paper_trader.py`, `place_bets.py` now accept `StatConfigSet`.
4. **Tests:** 30 new tests in `tests/test_stat_config.py`. All 570 tests pass.

**Dashboard insight features (G2/G3 — 2026-02-14):** Added 14 `feat_*` columns to `daily_predictions` table for template-based insights in the Analysis Modal. Features explain WHY the model made its prediction.
1. **Database migration (G2):** Added columns for B2 rest/schedule (`feat_rest_days`, `feat_is_back_to_back`, `feat_games_last_7d`), B1 injury context (`feat_team_out_count`, `feat_team_out_min_sum`, `feat_opp_out_count`, `feat_player_is_questionable`, `feat_player_is_probable`), B3 stat-specific trends (`feat_player_avg_stat_l3/l5/l15`, `feat_stat_l3_l15_ratio`, `feat_stat_std_l5`), and opponent abbreviation (`feat_opp_abbrev`).
2. **Prediction storage update (G3):** `daily_runner.py` now maps feature values from `features_df` to predictions via `_map_features_to_predictions()` method. Uses hardcoded `TEAM_ABBREV` map (same as dashboard) for opponent abbreviations.
3. **Insights generator:** New `dashboard/src/lib/insights.ts` generates template-based insights from feature values. Categories: rest, injury, trend, consistency, average. Sentiments are context-aware — considers bet direction (Over vs Under) when determining if a signal is positive/negative.
4. **Analysis Modal update:** Added "Model Context" section displaying insights with color-coded sentiments (green=positive, red=negative, neutral=gray).
5. **Historical backfill:** `src/tools/backfill_prediction_features.py` script populates feat_* columns for historical predictions without modifying prediction values. Supports `--date`, `--start/--end`, `--dry-run` flags.

**Dashboard Vercel deployment (2026-02-14):** Dashboard deployed to Vercel at `game-flow-data.vercel.app`. Configuration: root directory `dashboard`, environment variables `NEXT_PUBLIC_SUPABASE_URL` and `NEXT_PUBLIC_SUPABASE_ANON_KEY`. Vercel MCP available via `claude mcp add --transport http vercel https://mcp.vercel.com`.
