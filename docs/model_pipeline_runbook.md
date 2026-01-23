# Model Pipeline Runbook

Complete guide to training, backtesting, and running daily predictions.

---

## Pipeline Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  1. TRAIN       │ ──► │  2. BACKTEST    │ ──► │  3. DAILY RUN   │
│  train_pipeline │     │  run_backtest   │     │  run_daily      │
│                 │     │                 │     │                 │
│  Outputs:       │     │  Outputs:       │     │  Outputs:       │
│  - .joblib      │     │  - metrics.json │     │  - predictions  │
│  - calibration  │     │  - bets.csv     │     │    CSV with     │
│    report       │     │  - ROI / Sharpe │     │    edges        │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

---

## Prerequisites

1. `.env` file at project root with `DATABASE_URL=postgresql://...`
2. Database populated with scraped data (game stats, props, positions, averages)
3. Python packages: `xgboost`, `scikit-learn`, `pandas`, `numpy`, `sqlalchemy`, `joblib`, `python-dotenv`

---

## Step 1: Train Models

### File
```
src/models/train_pipeline.py
```

### Command
```bash
python src/models/train_pipeline.py --train-seasons 22022 22023 --cal-season 22024
```

### Arguments

| Arg | Default | Description |
|-----|---------|-------------|
| `--train-seasons` | `22022 22023` | NBA season IDs for training data |
| `--cal-season` | `22024` | NBA season ID for calibration holdout |

**Season ID format:** `2YYYY` where `YYYY` is the starting year. Examples:
- `22022` = 2022-23 season
- `22023` = 2023-24 season
- `22024` = 2024-25 season

### Logic Flow

```
1. Load Training Data
   └─ FeatureStore.get_training_dataset(train_seasons)
      ├─ Pulls player_game_stats, rolling averages, team stats, opponent defense
      ├─ Joins betting lines (spread/total)
      ├─ Calculates travel/rest features
      └─ Computes rate targets (pts_per_min, reb_per_min, etc.)

2. Load Calibration Data (Holdout)
   └─ Same query but for cal_season only

3. Feature Selection (Training Data ONLY)
   ├─ For each model (minutes, pts_rate, reb_rate, ast_rate, threes_rate):
   │   ├─ Rank features via Permutation Importance (XGBoost proxy model)
   │   └─ Optimize feature count via TimeSeriesSplit CV (avg pinball loss)
   └─ Output: dict of selected features per model

4. Train Models
   ├─ Minutes Model: QuantileModelSuite trained on actual_minutes
   │   └─ 5 XGBoost models (Q10, Q25, Q50, Q75, Q90)
   └─ Rate Models (x4): QuantileModelSuite per stat
       └─ Each: 5 XGBoost models predicting stat_per_min

5. Calibration Evaluation (Holdout Season)
   ├─ Predict quantiles on holdout data
   ├─ Check: P(actual <= predicted_Qxx) ≈ xx%
   ├─ FAIL if any gap > 10%
   └─ WARN if any gap > 5%

6. Sanity Check
   ├─ Pick random player-game from holdout
   ├─ Run full inference: FeatureStore → Pipeline → MonteCarloPredictor
   └─ Assert: predictions positive, quantiles monotonic, values reasonable

7. Save Artifacts
   └─ src/models/artifacts/run_YYYYMMDD_HHMMSS/
       ├─ minutes_model.joblib
       ├─ pts_rate_model.joblib
       ├─ reb_rate_model.joblib
       ├─ ast_rate_model.joblib
       ├─ threes_rate_model.joblib
       ├─ feature_config.joblib
       ├─ selected_features.json
       ├─ run_config.json
       └─ calibration_report.json
```

### What Good Calibration Looks Like

| Quantile | Target | Good (gap) | Acceptable | Fail |
|----------|--------|------------|------------|------|
| Q10 | 10% | <2% | <5% | >10% |
| Q25 | 25% | <2% | <5% | >10% |
| Q50 | 50% | <2% | <5% | >10% |
| Q75 | 75% | <2% | <5% | >10% |
| Q90 | 90% | <2% | <5% | >10% |

If Q50 actual coverage is 60% instead of 50%, the model is **underconfident** — it underestimates player stats, which means over bets look worse than they are.

---

## Step 2: Backtest

### File
```
src/backtesting/run_backtest.py
```

### Command
```bash
# Auto-finds latest model artifacts
python src/backtesting/run_backtest.py --start 2024-10-22 --end 2025-01-15

# Specify exact model run
python src/backtesting/run_backtest.py --model-dir src/models/artifacts/run_20250120_143022 --start 2024-10-22 --end 2025-01-15

# Custom settings
python src/backtesting/run_backtest.py --start 2024-11-01 --end 2024-12-31 --edge-threshold 0.07 --n-samples 10000
```

### Arguments

| Arg | Default | Description |
|-----|---------|-------------|
| `--start` | *required* | Start date (YYYY-MM-DD) |
| `--end` | *required* | End date (YYYY-MM-DD) |
| `--model-dir` | `src/models/artifacts` | Path to model artifacts (finds latest run_*) |
| `--output-dir` | `backtest_results/bt_<timestamp>` | Where to save results |
| `--n-samples` | `5000` | Monte Carlo samples per prediction |
| `--stats` | `pts reb ast` | Stats to predict and bet on |
| `--edge-threshold` | `0.05` | Minimum edge (5%) to place a simulated bet |
| `--bookmaker` | `pinnacle` | Bookmaker for line comparison |

### Logic Flow

```
1. Load Models
   └─ PlayerPropsModelPipeline.load_all(model_dir)

2. Get Game Dates in Range
   └─ Query distinct game dates from player_game_stats

3. For Each Game Date:
   ├─ Get all games and players who played that day
   ├─ For each player:
   │   ├─ FeatureStore.get_player_game_features(player, game, date)
   │   ├─ MonteCarloPredictor.predict() → distribution per stat
   │   └─ Store quantile predictions
   ├─ Fetch prop lines (from raw_player_props_combined)
   ├─ Calculate edges:
   │   ├─ P(over) from predicted distribution
   │   ├─ Implied prob from book odds
   │   └─ Edge = P(over) - implied_prob
   ├─ Place bets where |edge| > threshold
   └─ Resolve bets against actual outcomes

4. Calculate Performance Metrics
   ├─ ROI (total profit / total wagered)
   ├─ Win rate
   ├─ Sharpe ratio (annualized to 170 NBA game-days)
   ├─ Max drawdown (% of peak equity)
   ├─ Win/loss streaks
   └─ Profit by stat, by edge bucket

5. Save Results
   └─ output_dir/
       ├─ predictions.csv   (all predictions with features)
       ├─ bets.csv          (placed bets with outcomes)
       └─ metrics.json      (summary performance)
```

### Interpreting Results

| Metric | What It Means | Target |
|--------|---------------|--------|
| ROI | Profit per unit wagered | >3% is good, >7% is great |
| Win Rate | % of bets won | ~53-55% at -110 odds to profit |
| Sharpe | Risk-adjusted return | >1.0 is strong |
| Max Drawdown | Worst peak-to-trough loss | <20% of bankroll |

---

## Step 3: Daily Predictions

### File
```
src/orchestration/run_daily.py
```

### Command
```bash
# Full pipeline (scrape + process + predict)
python src/orchestration/run_daily.py --date 2025-01-23

# Predictions only (data already fresh)
python src/orchestration/run_daily.py --date 2025-01-23 --skip-scraping --skip-processing

# With optional scrapers
python src/orchestration/run_daily.py --date 2025-01-23 --scrape-injuries --scrape-daily-props
```

### Arguments

| Arg | Default | Description |
|-----|---------|-------------|
| `--date` | today | Target date (YYYY-MM-DD) |
| `--skip-scraping` | false | Skip all scraping |
| `--skip-processing` | false | Skip derived stats updates |
| `--skip-inference` | false | Skip predictions |
| `--model-dir` | `src/models/artifacts` | Path to model artifacts |
| `--scrape-live-odds` | false | Scrape current game lines → `raw_game_lines_live` |
| `--scrape-daily-props` | false | Scrape player props snapshot → `raw_player_props_combined` |
| `--scrape-live-props` | false | Scrape live player props → `raw_player_props_live` |
| `--scrape-injuries` | false | Scrape ESPN injury report |

### Logic Flow

```
1. SCRAPING (unless --skip-scraping)
   ├─ nba_unified_scraper.py        → Latest box scores to player_game_stats
   ├─ daily_game_lines_scraper.py   → Game odds to raw_game_lines_staging
   ├─ [opt] daily_player_props_scraper.py → Props to raw_player_props_combined
   ├─ [opt] live_odds_scraper.py    → Live odds to raw_game_lines_live
   └─ [opt] injury_scraper_job.py   → Injuries to espn_injuries

2. PROCESSING (unless --skip-processing)
   ├─ nba_linker_local.py           → Link API player IDs
   ├─ backfill_team_ids.py          → Fill team_id gaps
   ├─ update_player_position_history.py → Update position snapshots
   ├─ update_league_position_averages.py → League-wide position stats
   ├─ populate_average_stats.py     → Rolling averages (L5, L15)
   └─ backfill_opponent_allowed.py  → Opponent positional defense stats

3. INFERENCE (unless --skip-inference)
   ├─ Load latest model from model-dir
   ├─ Get today's games from team_game_stats
   ├─ Get active players (avg_min_l5 >= 10)
   ├─ Filter out injured players (if injury data available)
   ├─ For each player:
   │   ├─ FeatureStore.get_player_game_features()
   │   └─ MonteCarloPredictor.predict() (10,000 samples)
   ├─ Fetch current prop lines from raw_player_props_combined
   ├─ Calculate edges (model prob vs implied prob)
   └─ Save to predictions_YYYY-MM-DD.csv

4. OUTPUT: predictions_YYYY-MM-DD.csv
   Columns: player_name, stat, pred_mean, pred_median,
            pred_q10..q90, line, over_odds, under_odds,
            over_prob, under_prob, over_edge, under_edge
```

### Reading the Output

```python
import pandas as pd

preds = pd.read_csv("predictions_2025-01-23.csv")

# Filter for actionable bets (>5% edge)
over_bets = preds[preds["over_edge"] > 0.05].sort_values("over_edge", ascending=False)
under_bets = preds[preds["under_edge"] > 0.05].sort_values("under_edge", ascending=False)

print("=== OVER BETS ===")
print(over_bets[["player_name", "stat", "line", "over_edge", "over_odds"]].head(10))

print("\n=== UNDER BETS ===")
print(under_bets[["player_name", "stat", "line", "under_edge", "under_odds"]].head(10))
```

---

## Typical Workflow

```bash
# ONE TIME: Train models (takes 5-15 min depending on data size)
python src/models/train_pipeline.py --train-seasons 22022 22023 --cal-season 22024

# ONE TIME: Validate with backtest
python src/backtesting/run_backtest.py --start 2024-10-22 --end 2025-01-15

# DAILY: Generate predictions
python src/orchestration/run_daily.py --date 2025-01-23 --scrape-injuries --scrape-daily-props
```

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `DATABASE_URL not found` | Missing `.env` | Create `.env` with `DATABASE_URL=postgresql://user:pass@host:5432/db` |
| `Suspiciously few rows` | Training query returns <10k rows | Check that season data is scraped and processed |
| `Calibration failed: worst gap = X%` | Model is miscalibrated | Try different training seasons, check data quality |
| `No run_* directories found` | No trained model exists | Run `train_pipeline.py` first |
| `No games found for date` | No game data for that date | Verify scraping ran, check `team_game_stats` table |
| `FeatureStore returned None` | Missing player/position data | Run full processing pipeline, check `player_position_history` |

---

## File Reference

| File | Purpose | Entry Point |
|------|---------|-------------|
| `src/models/train_pipeline.py` | Orchestrates training | `python -m` or direct |
| `src/models/feature_store.py` | Feature engineering (training + inference) | Imported |
| `src/models/quantile_trainer.py` | XGBoost quantile models | Imported |
| `src/models/monte_carlo.py` | Samples distributions from quantiles | Imported |
| `src/models/calibration.py` | Calibration evaluation utilities | Imported |
| `src/models/daily_runner.py` | Daily prediction generation | Imported by run_daily |
| `src/processing/feature_selection.py` | Automated feature selection | Imported by train_pipeline |
| `src/backtesting/run_backtest.py` | Backtest entry point | `python -m` or direct |
| `src/backtesting/backtest_harness.py` | Backtest orchestration | Imported |
| `src/backtesting/bet_simulator.py` | Bet tracking and resolution | Imported |
| `src/backtesting/performance_metrics.py` | ROI, Sharpe, drawdown calculations | Imported |
| `src/orchestration/run_daily.py` | Daily pipeline (scrape + process + predict) | `python -m` or direct |
| `src/db/client.py` | Database connection singleton | Imported |
