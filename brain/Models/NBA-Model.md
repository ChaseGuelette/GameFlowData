# NBA Model

> Part of [[Models]]

## Current Production Model
- **Run ID**: `nba_run_20260323_212931`
- **Trained on**: Seasons 22023 + 22024 + 22025 (3 full seasons)
- **Architecture**: XGBoost quantile regression (Q10/Q25/Q50/Q75/Q90) with per-quantile feature selection and isotonic calibration
- **Stats**: PTS, REB, AST + combo markets (PRA, PR, PA, RA)
- **Simulation**: 10,000 Gaussian copula Monte Carlo samples per player (PTS rho=0.314, AST rho=0.176)
- **Blending**: Black-Litterman log-odds with tau=0.09, linear ramp confidence via z-score (sportsbook). Kalshi uses tau=0.5, z_max=1.0 (same as Model Picks)
- **Calibration offsets**: NONE deployed (4x confirmed to hurt ROI)

## Latest Calibration Check (Apr 3, 2026 — 11 days old, HOLD)
| Metric | Value |
|--------|-------|
| 14-day ROI | +9.8% (65 bets, $23K PnL) |
| Win rate | 64.6% |
| AST UNDER ROI | +31.9% (21 bets, 81% win) |
| PTS UNDER ROI | -47.9% (11 bets, 36.4% win — flagged) |
| 15%+ edge bucket | 55.6% actual vs ~70% expected — flagged |
| Bias PTS/REB/AST | +0.5% / +1.4% / -6.7% (all improved vs Mar 31) |
| Next check | ~Apr 13 (model will be 21 days / 3-week limit) |

## Previous Backtest (Mar 18-23, 2026)
| Metric | Value |
|--------|-------|
| Overall hit rate | 63% |
| Overall ROI | 28.96% |
| PTS hit rate | 42.43% |
| REB hit rate | 34.14% |
| AST hit rate | -24.55% (13 bets, noise) |

## Recent Fix (Session 86)
PTS model had degraded to 44.8% win rate (30% last 7d), -$15K PnL over 14 days. Root cause: minutes x rate decomposition creating fake under edges for variable-minutes players.

**Fixes applied:**
- Q50 vs L5 sanity check — rejects under bets where pred_q50 is 30%+ below L5 avg
- MIN_MINUTES_FOR_STATS raised from 5 to 8
- Retrained with fresh data, locked hyperparams from previous production run

## Feature Store
66 unique features across 5 model lists (minutes, pts_rate, reb_rate, ast_rate, shared). Sources:
- Player rolling averages (L3/L5/L15/SZN)
- Opponent defense by position (per-100 possessions)
- Rest/schedule (B2): rest_days, is_back_to_back, games_last_7d
- Injury context (B1): 10 features from rapidapi_injuries
- Short-window trends (B3): L3 averages, momentum ratios, L5 std devs
- Minutes stability (B4): min_std_l5, min_floor_l5, starter_prob
- Betting signals: spread, total, prop lines (centering features)

Full catalog: `docs/nba_feature_catalog.md`

## Key Files
| File | Purpose |
|------|---------|
| `src/models/feature_store.py` | 66-feature store with 4 query paths |
| `src/models/train_pipeline.py` | Training orchestrator (XGBoost + Optuna) |
| `src/models/quantile_trainer.py` | Per-quantile XGBoost with isotonic calibration |
| `src/models/monte_carlo.py` | 10K-sample Gaussian copula MC predictor |
| `src/models/black_litterman.py` | Log-odds Bayesian market blending |
| `src/models/daily_runner.py` | Production inference pipeline |
| `src/models/prediction_store.py` | Prediction + sample storage (gzip BYTEA) |
| `src/models/artifacts/production/` | Live model artifacts |

## Archived
- **THREES model** (Session 24): Poor market coverage (50% missing lines), insufficient volume. Code in `archive/threes_model/`. Scrapers still collect data.

#nba #model #production
