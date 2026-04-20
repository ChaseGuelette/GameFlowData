# NBA Model

> Part of [[Models]]

## Current Production Models

### Regular Season (`production/`)
- **Run ID**: `nba_run_20260323_212931`
- **Trained on**: Seasons 22023 + 22024 + 22025 (3 full seasons)
- **Architecture**: XGBoost quantile regression (Q10/Q25/Q50/Q75/Q90) with per-quantile feature selection and isotonic calibration
- **Stats**: PTS, REB, AST + combo markets (PRA, PR, PA, RA)
- **Simulation**: 10,000 Gaussian copula Monte Carlo samples per player (PTS rho=0.314, AST rho=0.176)
- **BL Config (regular season)**: tau=0.5, z_max=1.0, max_weight=0.5, edge=0.09
- **Calibration offsets**: NONE deployed (4x confirmed to hurt ROI)

### Playoffs (`production_playoffs/`) — v2 (CURRENT)
- **Run ID**: `nba_run_20260419_153328`
- **Label**: Playoff v2 — added minutes trend ratios as features
- **Trained on**: Seasons 42019, 42020, 42021, 42022, 42023 (5 playoff seasons)
- **Calibrated on**: 42024
- **OOS backtest (42025)**: 327 raw bets → **277 filtered, 63.5% hit, +16.7% ROI, Sharpe 1.50**
- **BL Config (playoffs)**: tau=0.9, z_max=1.0, max_weight=0.8, edge=0.15
- **New features vs v1**: minutes trend ratios (added to `feature_store.py` in commit `0bbd0f4`)
- **Post-filter rules** — hardcoded in `daily_runner.py:1000-1013`, applied at recommendation time:
  - No REB OVER on line ≤ 2.5 (−12% structural drag) — `FILTER [REB_OVER_LOW]`
  - No AST OVER any line (−22% structural drag) — `FILTER [AST_OVER]`
  - Under-only performance: +14.3% ROI (unchanged vs v1)
- **Copula**: NOT a new module — Gaussian copula has always been in `monte_carlo.py` (`_predict_copula`). `copula_params.json` artifact saved per run.
- **Promoted**: Apr 20, 2026 (commit `76cf464`)
- **Status**: Model deployed but `NBA_TRADING_ENABLED=false` — trading paused after Apr 19 incident (21 bets / $233 in 16s from broken inference)
- **CDN game discovery**: Playoff game IDs (prefix `004`) already supported — no code change needed

### Playoffs (`production_playoffs/`) — v1 (PREVIOUS)
- **Run ID**: `nba_run_20260415_152608`
- **Trained on**: Seasons 42019, 42020, 42021, 42022, 42023 (5 playoff seasons)
- **Calibrated on**: 42024 (mild contamination — not training overlap)
- **OOS backtest (42025)**: **63.6% hit, +19.3% ROI, 272 bets, Sharpe 2.33**
- **BL Config**: tau=0.9, z_max=0.25, max_weight=0.8, edge=0.12
- **Deployed**: Apr 16, 2026 — superseded by v2 on Apr 20

## Latest Calibration Check (Apr 20, 2026 — v2 deployed, trading paused)
| Metric | Value |
|--------|-------|
| Model | `nba_run_20260419_153328` (playoff v2) |
| OOS backtest 42025 (filtered) | 277 bets, 63.5% hit, +16.7% ROI, Sharpe 1.50 |
| Under-only ROI | +14.3% |
| Live trading | PAUSED — `NBA_TRADING_ENABLED=false` |
| Reason | Apr 19 incident: 21 bets / $233 in 16s with 17–46% edges (broken inference, not model quality) |
| Next action | Investigate inference bug → validate model → re-enable trading |

### Apr 19 Incident — Root Cause (TBD)
The 17-46% edge values were NOT from the v2 model (which produces sensible backtest edges). They were from v1 being invoked during a transition. Investigation needed before re-enabling `NBA_TRADING_ENABLED`.

## Previous Calibration Check (Apr 3, 2026 — 11 days old at time)
| Metric | Value |
|--------|-------|
| 14-day ROI | +9.8% (65 bets, $23K PnL) |
| Win rate | 64.6% |
| AST UNDER ROI | +31.9% (21 bets, 81% win) |
| PTS UNDER ROI | -47.9% (11 bets, 36.4% win — flagged) |
| 15%+ edge bucket | 55.6% actual vs ~70% expected — flagged |
| Bias PTS/REB/AST | +0.5% / +1.4% / -6.7% (all improved vs Mar 31) |

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

## Paper Trader Fix (Session 25)

**Root cause**: `paper_trader.py` was re-blending raw MC samples with a conservative tau=0.5 blender AND applying independent sanity checks (L5_ABOVE_LINE_MARGIN=0.0, MAX_Q50_DIVERGENCE=0.30). Meanwhile `edge_refresh_job.py` at 4:15 PM overwrites `is_recommended` in DB *without* those sanity checks. This caused dashboard to show ~15 picks while paper trader bet on far fewer.

**Fix**: Paper trader now reads stored BL values (`bl_over_edge`, `bl_under_edge`, `bl_over_prob`, `bl_under_prob`) directly from `daily_predictions` table. Falls back to raw edges when BL columns are NULL.
- Removed: `_bl_blender`, `_load_samples_for_date()`, `MAX_Q50_DIVERGENCE`, `L5_ABOVE_LINE_MARGIN`, gzip/numpy imports
- Kept: `bl_tau`/`bl_z_max` dataclass fields for backward compat with `place_bets.py`
- Paper trader now uses same `is_recommended`-aligned logic as the dashboard

## Key Files
| File | Purpose |
|------|---------|
| `src/models/feature_store.py` | 66-feature store with 4 query paths |
| `src/models/train_pipeline.py` | Training orchestrator (XGBoost + Optuna) |
| `src/models/quantile_trainer.py` | Per-quantile XGBoost with isotonic calibration |
| `src/models/monte_carlo.py` | 10K-sample Gaussian copula MC predictor |
| `src/models/black_litterman.py` | Log-odds Bayesian market blending |
| `src/models/daily_runner.py` | Production inference pipeline (dual-mode: regular/playoffs via `NBA_PLAYOFF_MODE`) |
| `src/models/prediction_store.py` | Prediction + sample storage (gzip BYTEA) |
| `src/models/artifacts/production/` | Regular season model artifacts |
| `src/models/artifacts/production_playoffs/` | Playoff model artifacts |

## Archived
- **THREES model** (Session 24): Poor market coverage (50% missing lines), insufficient volume. Code in `archive/threes_model/`. Scrapers still collect data.

#nba #model #production
