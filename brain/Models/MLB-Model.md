# MLB Model

> Part of [[Models]]

## Status: Batter Pipeline Ready for Training

### What's Built
- Full data pipeline: boxscore scraper, Statcast scraper, FanGraphs scraper, props/lines scrapers, 15 database tables
- Local linker with checkpoint/resume — 96.8% linking coverage (21.97M/22.71M rows)
- Rolling averages: batting (L5/L10/L20/SZN) and pitching (L3/L5/SZN) with rate stats + Statcast averages
- Feature store: 31 features for pitcher K model across 6 data sources
- Quantile trainer: `MLBPitcherKPipeline` wrapping NBA's `QuantileModelSuite`
- Monte Carlo: `MLBMonteCarloPredictor` with integer rounding, no copula
- Training pipeline: `mlb_train_pipeline.py` — 10-step CLI orchestrator
- Backtest sweep: `run_mlb_sweep.py` (fixed Session 83)
- Stat config: Quantile for pitcher K/outs (8% edge), NegBin for batter counts (10%), Binary for HR (10%)
- **Batter pipeline complete**: NLL-based feature selection, PMF-based calibration, Optuna hyperparameter tuning
- NegBin model v2: XGBoostLSS distributional regression (jointly learns mu + alpha)

### What's NOT Built
- No trained models yet (need data backfills first)
- No MLB daily runner (inference pipeline)
- No MLB dashboard integration
- No MLB paper trading

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
| `src/models/mlb/mlb_monte_carlo.py` | MC predictor, integer rounding |
| `src/models/mlb/mlb_batter_train_pipeline.py` | Batter pipeline (NLL selection + PMF calibration) |
| `src/models/mlb/mlb_batter_feature_store.py` | Batter features, prop line mapping |
| `src/models/negbin_model.py` | NegBin v2 XGBoostLSS distributional regression |
| `src/models/negbin_tuner.py` | Optuna NB NLL tuner for NegBin hyperparams |
| `src/processing/feature_selection.py` | Feature selection (quantile + NLL methods) |
| `src/backtesting/mlb/run_mlb_sweep.py` | Parameter sweep harness |

### Batter Pipeline Architecture (Session 2)
The batter pipeline was redesigned to align with what the NegBin model actually outputs:
1. **Feature selection**: NLL-based (Poisson proxy + NB NLL scorer) instead of per-quantile pinball loss
2. **Calibration**: Direct PMF/CDF metrics (NLL, bias, zero fraction, per-line P(over) calibration) instead of 40-min MC sampling
3. **Hyperparameter tuning**: Optuna with NB NLL objective, searches depth/lr/subsample/etc.
4. Calibration runs in ~5 sec (was ~40 min). Feature selection ~2 min (was ~4 min). Tuning adds ~30-60 min when enabled.

#mlb #model #in-progress
