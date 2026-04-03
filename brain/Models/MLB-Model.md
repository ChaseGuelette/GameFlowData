# MLB Model

> Part of [[Models]]

## Status: 5 Active Models — HR Dropped, 2 Need Retraining

5 MLB models active in production (`src/models/mlb/artifacts/production/`). `batter_home_runs` dropped entirely (no edge, too rare). Two models have `at_bats` feature leakage requiring retraining. Batter hits backtested with strong results.

### Trained Models (Production)
| Model | Type | Status | Notes |
|-------|------|--------|-------|
| `pitcher_strikeouts` | Quantile | OK | Backtested, best config: tau=0.9 z_max=0.75 mw=0.65 |
| `batter_hits` | Binomial | **BACKTESTED** | Best config: tau=0.75 z_max=1.0 mw=0.65 edge=0.08 (+36.3% ROI, 3.52 Sharpe, 314 bets) |
| `batter_total_bases` | NegBin | **RETRAINED** | Retrained clean. Alpha varies 0.87-1.30 (learns player-specific variance). Needs backtest sweep. |
| `batter_rbis` | NegBin | OK | No leakage, uses `batter_avg_ab_l5`/`batter_avg_pa_l5` |
| `batter_runs_scored` | NegBin | **RETRAINED** | Retrained clean. Minimal bias, excellent zero calibration, constant alpha=0.135. Needs backtest sweep. |
| `batter_home_runs` | Binary | **DROPPED** | No edge — backtest showed max 8 bets/month, -12.3% ROI. Too rare/binary for model to beat market. Remove from pipeline + dashboard. |

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
- Backtest sweep: `run_mlb_sweep.py` (fixed Session 83)
- Stat config: Quantile for pitcher K/outs (8% edge), Binomial for hits (10%), NegBin for TB/RBI/runs (10%), Binary for HR (10%)
- **Batter pipeline complete**: NLL-based feature selection, PMF-based calibration, Optuna hyperparameter tuning
- NegBin model v2: XGBoostLSS distributional regression (jointly learns mu + alpha)
- **Binomial model** (Session 4): Custom XGBoost objective for underdispersed data (hits in at-bats)
- **MLBModelSuite**: Unified container discovers/loads all model types from a single directory

### Model Type Routing (Session 4)
| Stat | Model | Rationale |
|------|-------|-----------|
| `pitcher_strikeouts` | Quantile | Semi-continuous, well-suited to quantile regression |
| `batter_hits` | **Binomial** | Underdispersed (var=0.77 < mean=0.82). Hits = successes in n at-bats |
| `batter_total_bases` | NegBin | Overdispersed count data |
| `batter_rbis` | NegBin | Overdispersed count data |
| `batter_runs_scored` | NegBin | Overdispersed count data |
| `batter_home_runs` | Binary | Rare event (~7% rate), yes/no prediction |

### Binomial Model Architecture (Session 4)
Hits data is **underdispersed** (variance < mean, ratio=0.93). NegBin assumes overdispersion and failed: constant alpha, -5.1% calibration gap at 0.5 line. Solution: Binomial(n, p) model.

- **Custom XGBoost objective**: logit link, at-bats via DMatrix weights
  - Gradient: `n*p - y` | Hessian: `n*p*(1-p)`
- **At-bats handling**: actual `ab` for training, `batter_avg_ab_l5` projected AB for inference (avoids leakage)
- **Closed-form probability**: `1 - binom.cdf(floor(line), n, p)` — no MC simulation needed
- **Feature selection**: Poisson proxy + binomial NLL scorer (dedicated `select_features_binomial_nll()`)
- **Hyperparameter tuning**: Optuna with binomial NLL objective (`BinomialHyperparameterTuner`)

### Backtest Results (Session 13)
**`batter_runs_scored`** — Sep 2025, 28 dates, 6,732 predictions:
| Config | ROI | Bets | Hit Rate | Sharpe |
|--------|-----|------|----------|--------|
| Best ROI: tau=0.9 z_max=0.25 mw=0.8 edge=0.10 | **+49.5%** | 309 | 72% | — |
| Best Sharpe: tau=0.5 z_max=0.25 edge=0.15 | +47.9% | 55 | 87% | 9.86 |
| Most volume: tau=0.9 z_max=0.25 mw=0.65 edge=0.02 | +30.9% | 1,785 | 61% | — |
| Sweet spot: tau=0.75 z_max=0.75 mw=0.65 edge=0.15 | +48.6% | 80 | 88% | — |

Note: These results may be inflated by `at_bats` leakage — the backtest also had access to actual ABs. Re-run after retraining.

**`batter_hits`** — Sep 2025, backtest sweep (Session 19):
| Config | ROI | Bets | Hit Rate | Sharpe | MaxDD |
|--------|-----|------|----------|--------|-------|
| **Best pick**: tau=0.75 z_max=1.0 mw=0.65 edge=0.08 | **+36.3%** | 314 | 63% | 3.52 | 22.3% |
| Best Sharpe: tau=0.5 z_max=1.0 mw=0.65 edge=0.08 | +32.3% | 303 | 63% | 3.66 | 21.0% |
| Highest ROI: tau=0.75 z_max=0.75 mw=0.65 edge=0.08 | +39.3% | 284 | 63% | 3.34 | — |
| Most volume: tau=0.9 z_max=1.0 mw=0.65 edge=0.04 | +25.5% | 715 | 59% | — | — |

**`batter_home_runs`** — Sep 2025, backtest sweep (Session 19): **DROPPED**
- Max 8 bets across entire month in any config
- Best ROI: -12.3% (with raw model, no BL)
- Event too rare (~7% HR rate) for model to find exploitable edge against market pricing
- Decision: remove from pipeline AND dashboard predictions entirely

### Bugs Fixed (Session 15)
1. **pitcher_outs resolution mapping**: `MLB_STAT_RESOLUTION` in `mlb_paper_trader.py` mapped `"pitcher_outs"` to column `"outs"` — actual DB column is `"outs_recorded"`. Silent failures on pitcher_outs bet resolution.
2. **2026 game stats missing**: MLB schedule existed (2430 games) but all stuck at "Scheduled" — boxscores never scraped. Railway stats job was failing before it could update statuses. Backfilled locally: 77 games finalized, 946 bets resolved (467W/404L/75C).
3. **Railway averages timeout**: `mlb_populate_averages_incremental.py` used `pd.read_sql(query, engine)` without `SET statement_timeout`. Supavisor's 8s role-level timeout killed queries. Fix: explicit `SET statement_timeout = '120000'` (2 min) on both `fetch_batter_season_games()` and `fetch_pitcher_season_games()`.
4. **MLB Discord P&L not sending**: Added `_send_mlb_pnl_summary()` to `mlb_daily_stats_job.py` — queries `mlb_paper_trading_daily_log` and calls `send_pnl_summary_sync(sport="mlb")`.
5. **Paper bet placement disabled**: `--skip-bets` flag added to MLB inference in scheduler until leaky models retrained.

### What's NOT Built
- `batter_total_bases` and `batter_runs_scored` retrained — need backtest sweeps to find best BL configs
- MLB inference runs once daily — no periodic line re-scrape + rerun like NBA
- `batter_home_runs` needs removal from pipeline code (daily runner, paper trader, dashboard predictions)
- Batter hits config (tau=0.75 z_max=1.0 mw=0.65 edge=0.08) needs promotion to production config

### Key Differences from NBA
- No minutes decomposition — stats predicted directly
- No copula — single stat per model
- Integer targets — strikeouts are whole numbers
- Higher edge thresholds (8-10% vs NBA's 5%)

### Key Files
| File | Purpose |
|------|---------|
| `src/models/mlb/mlb_feature_store.py` | 31 features, 6 data sources |
| `src/models/mlb/mlb_train_pipeline.py` | 10-step training CLI |
| `src/models/mlb/mlb_quantile_trainer.py` | XGBoost quantile for pitcher K |
| `src/models/mlb/mlb_monte_carlo.py` | MC predictors (quantile, negbin, binomial) |
| `src/models/mlb/mlb_batter_train_pipeline.py` | Batter pipeline (routes hits→binomial, TB/RBI/runs→negbin, HR→binary) |
| `src/models/mlb/mlb_batter_feature_store.py` | Batter features, prop line mapping, at_bats queries |
| `src/models/mlb/mlb_model_suite.py` | Unified model container (discovers all model types) |
| `src/models/mlb/mlb_binary_model.py` | Binary classifier for HR |
| `src/models/binomial_model.py` | Binomial model with custom XGBoost objective |
| `src/models/binomial_tuner.py` | Optuna tuner for binomial hyperparams |
| `src/models/negbin_model.py` | NegBin v2 XGBoostLSS distributional regression |
| `src/models/negbin_tuner.py` | Optuna NB NLL tuner for NegBin hyperparams |
| `src/processing/feature_selection.py` | Feature selection (quantile + NLL + binomial NLL methods) |
| `src/backtesting/mlb/run_mlb_sweep.py` | Parameter sweep harness |

### Training Commands
```bash
# Hits (binomial)
python src/models/mlb/mlb_batter_train_pipeline.py --stat hits --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100

# Total bases (negbin)
python src/models/mlb/mlb_batter_train_pipeline.py --stat total_bases --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100

# RBIs (negbin)
python src/models/mlb/mlb_batter_train_pipeline.py --stat rbis --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100

# Runs (negbin)
python src/models/mlb/mlb_batter_train_pipeline.py --stat runs --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100

# Home runs (binary)
python src/models/mlb/mlb_batter_train_pipeline.py --stat home_runs --train-seasons 2023 2024 --cal-season 2025 --cal-end-date 2025-07-01 --tune --tuning-trials 100
```

### Batter Pipeline Architecture (Session 2, updated Session 4)
The batter pipeline routes each stat to its appropriate model type:
1. **Hits → Binomial**: Feature selection via binomial NLL, BinomialModel training, binom.cdf calibration
2. **TB/RBI/Runs → NegBin**: Feature selection via NB NLL, NegBinModel training, PMF calibration
3. **HR → Binary**: Standard binary classification with Platt scaling
4. **Calibration**: Direct PMF/CDF metrics (NLL, bias, zero fraction, per-line P(over) calibration) ~5 sec
5. **Feature selection**: ~2 min. Tuning adds ~30-60 min when enabled.

#mlb #model #in-progress
