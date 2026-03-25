# MLB Model

> Part of [[Models]]

## Status: All 5 Batter Models Ready for Training

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

### What's NOT Built
- No trained batter models yet — pipeline ready, training not yet run
- No batter backtest harness (pitcher-only currently)

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
