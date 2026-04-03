# MLB Model

> Part of [[Models]]

## Status: 3 Production Models — TB, Runs, HR Dropped

3 MLB models promoted to production with per-stat optimal Black-Litterman configs. `batter_total_bases`, `batter_runs_scored`, and `batter_home_runs` all dropped after backtest sweeps showed no viable edge.

### Promoted Models (Production)

| Model | Type | BL Config | Edge Threshold | Backtest ROI | Notes |
|-------|------|-----------|----------------|-------------|-------|
| `pitcher_strikeouts` | Quantile | tau=0.9, z_max=0.25, mw=0.8 | 5% | +8.0% (645 bets, 58% win) | High volume, consistent |
| `batter_hits` | Binomial | tau=0.75, z_max=1.0, mw=0.8 | 8% | +33.2% (282 bets, 56% win) | Strongest ROI per bet |
| `batter_rbis` | NegBin | tau=0.9, z_max=0.25, mw=0.8 | 12% | +44.2% (137 bets, 62% win) | Highest ROI, most selective |

**Combined Backtest (Jul 1 - Sep 28, 2025)**: 1,064 bets, 57.8% hit rate, **+21.25% ROI**, $1.2M profit, 1.19 Sharpe, 20.5% max DD.

### Dropped Models

| Model | Type | Reason | Session |
|-------|------|--------|---------|
| `batter_home_runs` | Binary | No edge — max 8 bets/month, -12.3% ROI. Event too rare (~7% HR rate). | Session 19 |
| `batter_total_bases` | NegBin | 0/540 configs profitable in Sep 2025 sweep. Best ROI: 0.00%. | Session 24 |
| `batter_runs_scored` | NegBin | 3/540 configs profitable, all trivial ($211 profit). Only viable config: tau=0.05, edge=0.02. | Session 24 |

### Per-Stat Config Architecture (Session 24)

Unlike NBA (single global BL config), MLB uses **per-stat optimal configs** from backtest sweeps. This is centralized in `src/models/mlb/mlb_stat_config.py`:

```python
STAT_BL_CONFIGS = {
    "pitcher_strikeouts": BLConfig(tau=0.9, z_max=0.25, max_weight=0.80),
    "batter_hits":        BLConfig(tau=0.75, z_max=1.0, max_weight=0.80),
    "batter_rbis":        BLConfig(tau=0.9, z_max=0.25, max_weight=0.80),
}
```

The daily runner, paper trader, and backtest sweep all read from this config. The `--combined` flag on the sweep runs all 3 stats together with their individual optimal configs.

### Bugs Fixed (Session 10)
1. **Model naming mismatch**: `batter_runs_scored` mapped to wrong artifact filenames (was looking for `batter_runs_scored_*`, actual files are `batter_runs_*`)
2. **Missing prop lines**: Daily runner hardcoded `stat="hits"` so only `prop_line_batter_hits` was populated. Added bulk prop line fetch for all stats.
3. **`at_bats` defaulting to 0**: Models trained with actual game ABs, but inference had no pre-game value. Added `projected_ab` → `at_bats` proxy mapping.
4. **RLS blocking dashboard**: MLB table policies required expired subscription. Replaced with open-access `USING (true)`.

### Bugs Fixed (Session 13)
1. **Supavisor timeout stripping**: Supabase Supavisor pooler (transaction mode) strips `-c` startup parameters from connections. Engine's `statement_timeout=300000` via `connect_args.options` never took effect — role-level timeout (8s) killed all heavy queries. Fixed by adding explicit `SET statement_timeout` per-connection in 5 locations.
2. **Silent zero-prediction sweep**: `get_features_for_date()` timed out on every date, but try/except silently continued → 0 predictions across all 28 dates. Added timeout fix + improved error logging with exception type.
3. **`at_bats` data leakage (code fix)**: Renamed SQL column `at_bats` → `actual_at_bats` in both training + inference queries. NegBin trainer exclusion set now includes `actual_at_bats`. Daily runner's `at_bats` hack replaced with `projected_ab` fallback. Models must be retrained to pick up the change.

### What's Built
- Full data pipeline: boxscore scraper, Statcast scraper, FanGraphs scraper, props/lines scrapers, 15 database tables
- Local linker with checkpoint/resume — 96.8% linking coverage (21.97M/22.71M rows)
- Rolling averages: batting (L5/L10/L20/SZN) and pitching (L3/L5/SZN) with rate stats + Statcast averages
- Feature store: 31 features for pitcher K model across 6 data sources
- Quantile trainer: `MLBPitcherKPipeline` wrapping NBA's `QuantileModelSuite`
- Monte Carlo: `MLBMonteCarloPredictor` with integer rounding, no copula
- Training pipeline: `mlb_train_pipeline.py` — 10-step CLI orchestrator
- Backtest sweep: `run_mlb_sweep.py` with `--combined` mode for multi-stat backtesting
- Stat config: Per-stat BL configs and edge thresholds in `mlb_stat_config.py`
- **Batter pipeline complete**: NLL-based feature selection, PMF-based calibration, Optuna hyperparameter tuning
- NegBin model v2: XGBoostLSS distributional regression (jointly learns mu + alpha)
- **Binomial model**: Custom XGBoost objective for underdispersed data (hits in at-bats)
- **MLBModelSuite**: Unified container discovers/loads all model types from a single directory

### Model Type Routing
| Stat | Model | Rationale |
|------|-------|-----------|
| `pitcher_strikeouts` | Quantile | Semi-continuous, well-suited to quantile regression |
| `batter_hits` | **Binomial** | Underdispersed (var=0.77 < mean=0.82). Hits = successes in n at-bats |
| `batter_rbis` | NegBin | Overdispersed count data |

### Binomial Model Architecture (Session 4)
Hits data is **underdispersed** (variance < mean, ratio=0.93). NegBin assumes overdispersion and failed: constant alpha, -5.1% calibration gap at 0.5 line. Solution: Binomial(n, p) model.

- **Custom XGBoost objective**: logit link, at-bats via DMatrix weights
  - Gradient: `n*p - y` | Hessian: `n*p*(1-p)`
- **At-bats handling**: actual `ab` for training, `batter_avg_ab_l5` projected AB for inference (avoids leakage)
- **Closed-form probability**: `1 - binom.cdf(floor(line), n, p)` — no MC simulation needed
- **Feature selection**: Poisson proxy + binomial NLL scorer (dedicated `select_features_binomial_nll()`)
- **Hyperparameter tuning**: Optuna with binomial NLL objective (`BinomialHyperparameterTuner`)

### Backtest Results

**Combined (Jul 1 - Sep 28, 2025)** — per-stat optimal BL configs:
| Stat | ROI | Bets | Hit Rate |
|------|-----|------|----------|
| pitcher_strikeouts | +8.0% | 645 | 58% |
| batter_hits | +33.2% | 282 | 56% |
| batter_rbis | +44.2% | 137 | 62% |
| **TOTAL** | **+21.25%** | **1,064** | **57.8%** |

**Individual sweeps (Sep 1-28, 2025)**:
| Stat | Best Config | ROI | Bets | Hit Rate |
|------|-------------|-----|------|----------|
| pitcher_strikeouts | tau=0.9 z_max=0.25 mw=0.8 edge=0.05 | +10.88% | 220 | 60.9% |
| batter_hits | tau=0.75 z_max=1.0 mw=0.8 edge=0.08 | +36.63% | 78 | 60.3% |
| batter_rbis | tau=0.9 z_max=0.25 mw=0.8 edge=0.12 | +112.54% | 16 | 81.2% |

### Bugs Fixed (Session 15)
1. **pitcher_outs resolution mapping**: `MLB_STAT_RESOLUTION` in `mlb_paper_trader.py` mapped `"pitcher_outs"` to column `"outs"` — actual DB column is `"outs_recorded"`. Silent failures on pitcher_outs bet resolution.
2. **2026 game stats missing**: MLB schedule existed (2430 games) but all stuck at "Scheduled" — boxscores never scraped. Railway stats job was failing before it could update statuses. Backfilled locally: 77 games finalized, 946 bets resolved (467W/404L/75C).
3. **Railway averages timeout**: `mlb_populate_averages_incremental.py` used `pd.read_sql(query, engine)` without `SET statement_timeout`. Supavisor's 8s role-level timeout killed queries. Fix: explicit `SET statement_timeout = '120000'` (2 min) on both `fetch_batter_season_games()` and `fetch_pitcher_season_games()`.
4. **MLB Discord P&L not sending**: Added `_send_mlb_pnl_summary()` to `mlb_daily_stats_job.py` — queries `mlb_paper_trading_daily_log` and calls `send_pnl_summary_sync(sport="mlb")`.
5. **Paper bet placement disabled**: `--skip-bets` flag added to MLB inference in scheduler until leaky models retrained.

### Key Differences from NBA
- No minutes decomposition — stats predicted directly
- No copula — single stat per model
- Integer targets — strikeouts are whole numbers
- **Per-stat BL configs** (vs NBA's single global config)
- Higher, stat-specific edge thresholds (5-12% vs NBA's ~9%)

### Key Files
| File | Purpose |
|------|---------|
| `src/models/mlb/mlb_stat_config.py` | Per-stat BL configs and edge thresholds |
| `src/models/mlb/mlb_daily_runner.py` | Production prediction runner |
| `src/models/mlb/mlb_model_suite.py` | Unified model container |
| `src/models/mlb/mlb_monte_carlo.py` | MC predictors (quantile, negbin, binomial) |
| `src/models/mlb/mlb_batter_train_pipeline.py` | Batter training pipeline |
| `src/models/mlb/mlb_batter_feature_store.py` | Batter features |
| `src/models/mlb/mlb_quantile_trainer.py` | Pitcher K quantile model |
| `src/models/binomial_model.py` | Binomial model for hits |
| `src/models/negbin_model.py` | NegBin v2 for RBIs |
| `src/backtesting/mlb/run_mlb_sweep.py` | Backtest sweep with --combined mode |
| `src/paper_trading/mlb_paper_trader.py` | Paper trading |

### Training Commands
```bash
# Hits (binomial)
python src/models/mlb/mlb_batter_train_pipeline.py --stat hits --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100

# RBIs (negbin)
python src/models/mlb/mlb_batter_train_pipeline.py --stat rbis --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100
```

### Backtest Commands
```bash
# Combined backtest with per-stat optimal configs
python src/backtesting/mlb/run_mlb_sweep.py --local --start 2025-07-01 --end 2025-09-28 --combined

# Individual stat sweep
python src/backtesting/mlb/run_mlb_sweep.py --local --start 2025-09-01 --end 2025-09-28 --stats pitcher_strikeouts
```

#mlb #model #production
