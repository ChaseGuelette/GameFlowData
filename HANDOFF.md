# GameFlowData — Engineering Handoff Document

**Last updated:** 2026-03-24 (Session 87)
**Current production model:** `nba_run_20260323_212931`

---

## 1. Project Overview

GameFlowData is a sports analytics and machine learning platform that ingests NBA game statistics and sportsbook odds, trains XGBoost quantile regression models with Gaussian copula Monte Carlo simulation, and generates probability distributions for player prop bets (points, rebounds, assists, combo markets). The system runs a fully automated daily pipeline on Railway (scraping, processing, inference, paper trading) and serves predictions through a Next.js dashboard on Vercel with Supabase Auth, real-time edge calculation, DFS comparison tools, AI-powered pick analysis (Claude Haiku), and cross-device bet tracking. MLB (pitcher strikeouts, batter hits) and NCAAB (game spreads/totals) pipelines are partially built but not yet in production. The target audience is quantitative sports bettors and DFS players who want model-driven edges against sportsbook lines.

---

## 2. Current State by Domain

### NBA Models

**Status: Production, running daily, profitable**

- **Pipeline:** Minutes model + per-stat rate models (PTS, REB, AST) trained via `src/models/train_pipeline.py`. XGBoost quantile regression (Q10/Q25/Q50/Q75/Q90) with per-quantile feature selection, isotonic calibration, and optional Optuna hyperparameter tuning.
- **Feature Store:** 66 unique features (`src/models/feature_store.py`) across 5 model lists (minutes, pts_rate, reb_rate, ast_rate, shared). Sources: player rolling averages (L3/L5/L15/SZN), opponent defense by position, rest/schedule (B2), injury context (B1: 10 features from `rapidapi_injuries`), short-window trends (B3), minutes stability (B4: `player_starter_prob`), betting signals (spread, total, prop lines). Full catalog at `docs/nba_feature_catalog.md`.
- **Monte Carlo:** 10,000 samples per player via Gaussian copula (PTS rho=0.314, AST rho=0.176). Minutes and rate sampled jointly. Combo stats (PRA, PR, PA, RA) derived by element-wise summing base samples — no separate model needed.
- **Black-Litterman:** Log-odds blending of model probability with devigged market prior. Per-prediction z-score confidence. Production uses BL with tau=0.09 for bet selection.
- **Current Model:** `nba_run_20260323_212931` — trained on 22023+22024+22025 (3 seasons). Locked hyperparams from previous production run. No calibration offsets deployed.
- **Backtest (Mar 18-23):** 63% hit rate, 28.96% ROI. PTS 42.43%, REB 34.14%, AST -24.55% (only 13 AST bets, noise).
- **Recent fix (Session 86):** PTS model had degraded to 44.8% win rate (30% last 7d), -$15K PnL. Root cause: minutes x rate decomposition creating fake under edges for variable-minutes players. Fix: Q50 vs L5 sanity check (rejects under bets where pred_q50 is 30%+ below L5 avg), MIN_MINUTES_FOR_STATS raised 5->8.
- **Calibration:** Individual model calibration is excellent (all quantile gaps < 5%). AST Q10 combined gap (+7-10%) is structural (~18% of games have 0 assists). **Offsets are NEVER deployed** — 4 separate A/B backtests confirmed they hurt ROI.
- **THREES model:** Archived (Session 24, `archive/threes_model/`). Poor market coverage (50% missing lines), insufficient betting volume. Scrapers still collect `player_threes` data.

### MLB Models

**Status: Architecture built, not yet trained or in production**

- **What's built:**
  - Full data pipeline: boxscore scraper (`mlb_stats_scraper.py`), Statcast scraper (`mlb_statcast_scraper.py`), FanGraphs scraper (`mlb_fangraphs_scraper.py`), props/lines scrapers, 15 database tables
  - Local linker with checkpoint/resume (`mlb_linker_local.py`) — 96.8% linking coverage (21.97M/22.71M rows)
  - Rolling averages: batting (L5/L10/L20/SZN) and pitching (L3/L5/SZN) with rate stats and Statcast averages
  - Feature store: 31 features for pitcher K model (`src/models/mlb/mlb_feature_store.py`) across 6 data sources
  - Quantile trainer: `MLBPitcherKPipeline` wrapping NBA's `QuantileModelSuite` — direct SO prediction (no minutes decomposition)
  - Monte Carlo: `MLBMonteCarloPredictor` with integer rounding, no copula
  - Training pipeline: `src/models/mlb/mlb_train_pipeline.py` — 10-step CLI orchestrator
  - Backtest sweep: `src/backtesting/mlb/run_mlb_sweep.py` (fixed Session 83 — was producing 0 predictions due to 4 argument mismatches)
  - Stat config: Quantile for pitcher K/outs (8% edge), NegBin for batter counts (10%), Binary for HR (10%)
  - **MLB batter pipeline in progress:** `mlb_batter_train_pipeline.py` modified (current git changes), `negbin_model.py` for NegBin stat modeling
- **What's NOT built:**
  - No trained models yet (need data backfills first)
  - No MLB daily runner (inference pipeline)
  - No MLB dashboard integration
  - No MLB paper trading
- **Key differences from NBA:** No minutes decomposition, no copula, higher edge thresholds (8-10%), integer targets

### NCAAB Models

**Status: Code complete, database migrations NOT applied, no data backfilled**

- **What's built (Session 63):** Complete pipeline — 3 database migrations (009-011), 3 scrapers (CBBpy, Barttorvik, Odds API game lines), 4 processing modules (config, linker, rolling averages, Barttorvik linker), feature store (~30 game-level features), XGBoost spread+total models, time-travel backtester, 2 orchestration scripts, 34 tests passing
- **Key design:** Game-level only (no player props for college — regulatory). Features are team differentials (home - away). Barttorvik for adjusted efficiency (free KenPom alternative). LATERAL JOIN for point-in-time ratings. Neutral site handling critical for March Madness (363 D1 teams).
- **Blockers:** Migrations 009-011 not applied to Supabase. No historical data backfilled. `cbbpy` not in `requirements.txt`. NCAAB cron jobs removed from Railway scheduler (Session 65) because they were failing.

### Frontend / Dashboard

**Status: Feature-complete for NBA, deployed on Vercel (needs fresh deploy)**

- **Tech:** Next.js 16, TypeScript, Tailwind CSS v4, Supabase Auth, Recharts
- **URL:** `game-flow-data.vercel.app`
- **Route groups:** `(public)` (landing, picks teaser, pricing, legal), `(auth)` (login/signup), `(protected)` (dashboard, history, performance, account, stats, dfs, subscribe)
- **Key pages:**
  - **`/dashboard`** — PropCards with PlayOfTheDay, FilterTabs (PTS/REB/AST/Combos), date selector (30d), edge threshold filter, BL blending filter, multi-select sportsbook filter, direction filter (Both/Over/Under), live/pre-game toggle with real NBA scoreboard polling (30s), matchup filter
  - **`/dfs`** — DFS Edge Finder: 3 modes (Model/Market/Combined), 6 stats (pts/reb/ast/stl/blk/3pm), platform filters, slip type selector, +EV toggle
  - **`/history`** — My Bets + Model History tabs, status/direction filters, date range filter with presets, per-stat win rate cards, over/under breakdown, expandable bet context snapshots
  - **`/performance`** — My Bets + Props + DFS tabs, bankroll chart (user-configurable initial bankroll), stat breakdown, KPI cards
  - **`/stats`** — Data Vault: player/team/defense/play-type heatmap tables, L5/L15/SZN windows, percentile coloring, sortable
  - **Analysis Modal** — Click any PropCard: L5 chart, model context insights, AI Q&A chat (Claude Haiku, 20 questions/day, position-aware, enriched injuries), quantile distribution, line shopping by state, Kelly sizing, "Take Bet" button with confidence stars (1-5)
- **Auth:** Supabase email/password. Middleware redirects. RLS on all tables.
- **Cross-device sync:** `useUserBets` (optimistic UI + Supabase) and `useUserPreferences` (localStorage cache + Supabase DB)
- **Pending deploy:** AI Q&A, combo markets, DFS 6-stat, mobile optimization, security headers, error boundaries, auth on `/api/slate`. Needs `ANTHROPIC_API_KEY` env var on Vercel.
- **Free Beta Model:** No paywall. Public `/picks` shows 3 real picks. Stripe infra preserved dormant.

### Infrastructure

**Railway (Python backend):**
- Single always-on worker: `src/orchestration/scheduler.py` (APScheduler, `America/New_York` timezone)
- Build: Nixpacks (`nixpacks.toml`) — Python 3.11 venv, zlib, stdenv.cc for numpy/scipy/xgboost C extensions
- Start command: `/app/venv/bin/python src/orchestration/scheduler.py`
- Schedule:
  | Time (ET) | Job | Details |
  |-----------|-----|---------|
  | 11:00 AM | `daily_stats_job.py` | CDN scrape + rolling averages + resolve paper+user bets |
  | 11:30 AM | retry | Auto-retry if 11 AM failed |
  | 12:00/4:00 PM | `lines_job.py --live --parallel` | Full props + injuries, concurrent |
  | 12:15 PM | `inference_job.py` | Full MC inference + paper bets + Discord alert |
  | 4:15 PM | `inference_job.py --skip-bets` | Refresh predictions only |
  | */5 min 11AM-11PM | `lines_job.py --live --props-only` | Props-only scrape (~156/day, silent) |
  | */5 min +2 offset | `edge_refresh_job.py --skip-paper` | Edge recalc + drift detection (~156/day, silent) |
- Env vars: `DATABASE_URL`, `ODDS_API_KEY`, `RAPIDAPI_KEY`, `DISCORD_CHANNEL_ALERTS`
- Model artifacts: `src/models/artifacts/production/` committed to git

**Vercel (Dashboard):**
- Root directory: `dashboard/` (configured in root `vercel.json`)
- Env vars: `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`, `ANTHROPIC_API_KEY` (needed for AI Q&A)

**Supabase (Database):**
- PostgreSQL 15+. `postgres` role for Python backend (bypasses RLS). `authenticated` role for dashboard (8s statement_timeout).
- `raw_player_props_combined`: **67M+ rows** — NEVER create non-concurrent indexes via migration
- Key RPCs with 30s timeout override: `get_dfs_lines`, `get_game_commence_times`
- 21+ migrations applied via `database/migrations/`

**Local (Windows Task Scheduler):**
- Only `scripts/run_advanced_scraper.bat` runs locally — advanced stats require direct stats.nba.com access
- 9 AM ET, flags: `--no-proxy --skip-team --skip-traditional`

### Discord Bot

**Status: REST API alerts working; slash commands require running bot process (local Phase 1)**

- **Automated alerts (no bot process needed):** `#predictions` (after inference), `#alerts` (job success/failure), `#performance` (daily P&L)
- **Slash commands** (require bot): `/picks`, `/player`, `/bankroll`, `/performance`, `/toppicks`
- **Code:** `src/discord_bot/` — `bot.py`, `alerts.py`, services, formatters

### Data Pipeline

**Daily orchestration flow:**
```
11:00 AM  -> CDN boxscores -> linker -> rolling averages -> opponent defense -> resolve paper+user bets
12:00 PM  -> Full live props+injuries (parallel) -> linker
12:15 PM  -> Feature store -> XGBoost -> MC simulation -> edges -> BL -> paper bets -> Discord
Every 5m  -> Props-only scrape -> linker -> edge refresh (drift detection + selective re-inference)
4:00 PM   -> Full live props+injuries
4:15 PM   -> Refresh inference (skip paper trading)
```

**Key data sources:** NBA CDN (boxscores), nba_api (ScoreboardV2 for game discovery), The Odds API (props from us/us2/us_ex/us_dfs regions, game lines), RapidAPI (injuries, 88K+ rows 2021-present), stats.nba.com (advanced stats, local only)

**Database highlights:** `player_game_stats`, `player_average_game_stats` (L5/L15/SZN rolling), `team_allowed_by_position`, `raw_player_props_combined` (67M+ rows, append-only snapshots), `daily_predictions`, `daily_prediction_samples` (gzip-compressed 10K float64 arrays), `paper_bets`, `user_bets`, `rapidapi_injuries`, plus 15 MLB tables and 6 NCAAB tables.

---

## 3. Critical Invariants

These rules must NEVER be violated regardless of what domain you're working in:

1. **Temporal Integrity:** Feature generation uses ONLY data where `game_date < target_game_date`. Pre-computed rolling averages use `shift(1)` so feature store queries use `<= game_date` (not `<`). This is NOT a mistake — it was a deliberate fix in Session 17.

2. **NEVER deploy global conformal recalibration offsets.** 4x confirmed to hurt ROI (Sessions 42, 75, 78, plus retrain run_20260218). Better calibration numbers != better edges. The model's Q10 "miscalibration" IS the under-betting edge.

3. **NEVER put advanced stats scraping on Railway.** stats.nba.com blocks datacenter IPs. Always local only, never with a proxy.

4. **Railway daily_stats_job uses CDN only.** No stats.nba.com calls from Railway. The `--cdn-only` flag exists for this.

5. **NEVER run non-concurrent CREATE INDEX on `raw_player_props_combined`** (67M+ rows). Supabase `apply_migration` runs in a transaction so CONCURRENTLY won't work. Use Supabase dashboard SQL editor with extended timeout.

6. **Empirical CDF for probabilities, not Gaussian.** Always `(samples > line).mean()`, never `scipy.stats.norm.cdf()`. Gaussian CDF produces phantom edges on non-Gaussian MC distributions.

7. **Quantile monotonicity enforced:** Q10 <= Q25 <= Q50 <= Q75 <= Q90. Isotonic regression post-processing.

8. **MIN_MINUTES_FOR_STATS = 8.** Games < 8 minutes excluded from rolling stat averages. Schedule features (rest_days, games_last_7d) still count all games.

9. **Combo samples NEVER stored to DB.** Always derived on-the-fly from base stat samples (pts + reb + ast) to avoid consistency issues and save storage.

10. **Python backend uses `postgres` role** (bypasses RLS). Dashboard uses `authenticated` role (governed by RLS policies). `authenticated` has 8s `statement_timeout` — override with `ALTER FUNCTION SET statement_timeout = '30s'` for slow RPCs.

11. **Recalibration triggers (Session 75):** ROI < 8% over 14 days, any stat ECE > 0.06, model age > 3 weeks. Code thresholds: quantile gap 3%, ECE 0.03, edge gap 8pp, bias 4%.

12. **Full retrains are risky.** run_20260218 retrain significantly hurt model performance. Always validate with backtests. Lock hyperparams from production when retraining on fresh data.

---

## 4. Active Work & Blockers

### Last shipped (Session 87, Mar 24):
Verified PTS model fix: Q50 sanity check, MIN_MINUTES 5->8, retrained `nba_run_20260323_212931`. Backtest: 63% hit rate, 28.96% ROI. 719 tests passing, ruff clean.

### In progress (from git status):
- `src/models/mlb/mlb_batter_train_pipeline.py` — MLB batter model training
- `src/models/negbin_model.py` + `tests/test_negbin_model.py` — NegBin model for count-based MLB stats

### Blocked:
| Item | Blocker |
|------|---------|
| NCAAB activation | Migrations 009-011 not applied, no data backfilled, `cbbpy` not in requirements.txt |
| Stripe integration | Prioritization — has been TODO since Session 42 |
| Play type scraper on Railway | stats.nba.com datacenter IP ban |
| DFS query performance | `get_dfs_lines` takes 9-14s on 67M-row table |
| MLB model training | Needs averages backfill completion first |

### Outstanding deploys:
1. **Vercel** — AI Q&A, combo markets, DFS 6-stat, mobile optimization, security headers, error boundaries. Set `ANTHROPIC_API_KEY`.
2. **Railway** — Up-to-date as of Session 75b.

---

## 5. Key Decisions Log

| # | Decision | Why |
|---|----------|-----|
| 1 | **XGBoost quantile regression** over ordinal classifiers | Gives full probability distribution (Q10-Q90) needed for MC simulation; handles heteroskedastic NBA stats naturally |
| 2 | **Minutes x Rate decomposition** (not direct stat prediction) | Variance driven by playing time. Separate modeling handles blowouts, OT, injury exits. Copula preserves correlation. |
| 3 | **Gaussian copula** for minutes-rate correlation | Independent sampling produced correlated errors. Copula (PTS rho=0.314) preserves marginals while inducing correct rank dependency. |
| 4 | **Empirical CDF** over Gaussian CDF | MC distributions are non-Gaussian (skewed, zero-inflated). Gaussian CDF produces phantom edges at tails. |
| 5 | **Calibration offsets NEVER deployed** | 4 A/B tests showed offsets improve calibration but degrade ROI by 1.4-12.8pp. Under-prediction IS the edge — sportsbooks inflate lines due to public over-bias. Academic support: Hubacek 2022, Dmochowski 2023. |
| 6 | **Per-100 possessions** for opponent defense | Per-36 ignores game pace. A 110 poss/game team has fundamentally different stat distributions than 95. |
| 7 | **COVID seasons excluded** from training | Bubble/shortened seasons don't represent normal NBA. Training on 22023+22024+22025 (3 full seasons). |
| 8 | **Black-Litterman in log-odds space** | Additive blending in probability space can produce impossible values. Linear ramp confidence (not exponential — exponential crushed weights, producing 0-12 bets). |
| 9 | **Sharpest-book line selection** (lowest vig) | Ensures we beat the sharpest available line, not just the worst. Over/under sides evaluated independently (may come from different books). |
| 10 | **Combo stats derived, not modeled** | PRA = pts_samples + reb_samples + ast_samples element-wise. Correlations preserved via shared copula minutes draws. No storage needed. |
| 11 | **THREES model archived** | 50% missing lines, 2 bets out of 78 in backtest. Scrapers still collect data for future optionality. |
| 12 | **DFS market edge works without model** | Compares DFS lines against devigged sportsbook consensus — works for all 6 stats including those the model doesn't predict. |
| 13 | **`player_starter_prob` rejected** for production | A/B backtest: ROI -3.19pp, AST -6.24pp. Feature smooths calibration but reduces edge-finding. Remains in feature store for future experiments. |
| 14 | **Q50 vs L5 sanity check** | Prevents minutes x rate decomposition from creating fake under edges on variable-minutes players. 30% divergence threshold. Reduced PTS PnL loss from -$15K to -$386. |
| 15 | **5-minute refresh cadence** | Fuzzy cache reduced linker from 15s to <1s, enabling ~156 scrape+refresh cycles/day. Keeps edges fresh as lines move. |

---

## 6. Known Issues

### Bugs
- `test_finds_latest_run_directory` failing — expects `run_*` prefix but code now expects `nba_run_*`
- `idx_props_dfs_latest` unused index in DB — should be dropped
- `idx_props_dfs_commence` / `idx_props_sb_commence` may be invalid from failed creation

### Technical Debt
- `raw_player_props_combined` at **67M+ rows** — queries take 9-14s. Needs archiving or partitioning.
- In-memory rate limiting on `/api/ask` — won't work multi-instance. Needs Redis.
- No pagination on history/performance pages
- DFS/heatmap tables use horizontal scroll on mobile (should be card layouts)
- AI chat not persisted across modal close
- 13 open issues in ISSUES.md (mostly low priority/cosmetic)
- Old model backups: `production_old_20260210/`, `production_old_20260323/`
- No CI/CD — deploys are manual git push

### Calibration
- AST Q10 combined gap (+7-10%) is **structural** — ~18% zero-assist rate sets coverage floor. Not fixable. Minimal betting impact.
- PTS systematic under-prediction is **intentional** — this is where the edge lives.

### Infrastructure
- Advanced stats scraper depends on local PC being awake at 9 AM ET
- Vercel deploy is stale (many features committed but not deployed)
- Discord bot slash commands only work when bot process is running locally

---

## 7. What's Next (Priority Order)

1. **Deploy to Vercel** — AI Q&A, combo markets, DFS 6-stat, mobile, security. Set `ANTHROPIC_API_KEY`. Pure deployment, no code changes needed.

2. **Finish MLB batter pipeline** — Complete `negbin_model.py` and `mlb_batter_train_pipeline.py` (in progress). Then train pitcher K model, build MLB daily runner and backtesting harness.

3. **Stripe integration** — `/subscribe` + Checkout, `/account` + Customer Portal, webhook at `api/stripe/webhook/route.ts`, add `stripe_customer_id`/`stripe_subscription_id` to `user_subscriptions`. Deferred since Session 42.

4. **Monitor current NBA model** — `nba_run_20260323_212931` is new (Mar 23). Track ROI vs recalibration triggers (ROI < 8% over 14d, ECE > 0.06, age > 3 weeks).

5. **NCAAB activation** — Apply migrations 009-011, add `cbbpy` to requirements, backfill data, train models, validate backtest, re-add cron jobs.

6. **Database performance** — Archive old `raw_player_props_combined` rows. Optimize `get_dfs_lines` query. Drop `idx_props_dfs_latest`.

7. **Dashboard scaling** — Pagination (SCALING.md Tier 1). React Query caching (Tier 2). Composite indexes.

8. **Backtest combo validation** — Short backtest with combo stats (pra/pr/pa/ra) to validate end-to-end.

9. **Discord bot Phase 2** — Deploy as Railway second service for persistent slash commands.

10. **Future experiments** — Targeted single-stat, single-quantile adjustments only (never global). Copula rho sweep for PTS. PTS retrain with force-included matchup features (but research suggests under-prediction is where edge lives — proceed with caution).

---

## Appendix: Key File Paths

| Category | Path |
|----------|------|
| **NBA Feature Store** | `src/models/feature_store.py` |
| **NBA Training Pipeline** | `src/models/train_pipeline.py` |
| **NBA Daily Runner** | `src/models/daily_runner.py` |
| **Monte Carlo Predictor** | `src/models/monte_carlo.py` |
| **Black-Litterman** | `src/models/black_litterman.py` |
| **Prediction Storage** | `src/models/prediction_store.py` |
| **Combo Config** | `src/config/combo_config.py` |
| **Stat Config** | `src/config/stat_config.py` |
| **Backtest Harness** | `src/backtesting/backtest_harness.py` |
| **Backtest Sweep** | `src/backtesting/run_sweep.py` |
| **Paper Trader** | `src/paper_trading/paper_trader.py` |
| **Calibration Monitor** | `src/paper_trading/calibration_monitor.py` |
| **DFS Paper Trader** | `src/paper_trading/dfs_paper_trader.py` |
| **User Bet Resolver** | `src/paper_trading/user_bet_resolver.py` |
| **Daily Stats Job** | `src/orchestration/daily_stats_job.py` |
| **Lines Job** | `src/orchestration/lines_job.py` |
| **Inference Job** | `src/orchestration/inference_job.py` |
| **Edge Refresh Job** | `src/orchestration/edge_refresh_job.py` |
| **Railway Scheduler** | `src/orchestration/scheduler.py` |
| **NBA Linker** | `src/processing/nba_linker_local.py` |
| **Rolling Averages** | `src/processing/populate_average_stats.py` |
| **Rolling Averages (Incr)** | `src/processing/populate_average_stats_incremental.py` |
| **Opponent Defense** | `src/processing/backfill_opponent_allowed.py` |
| **MLB Feature Store** | `src/models/mlb/mlb_feature_store.py` |
| **MLB Training Pipeline** | `src/models/mlb/mlb_train_pipeline.py` |
| **MLB Batter Pipeline** | `src/models/mlb/mlb_batter_train_pipeline.py` |
| **NegBin Model** | `src/models/negbin_model.py` |
| **NCAAB Feature Store** | `src/models/ncaab_feature_store.py` |
| **NCAAB Trainer** | `src/models/ncaab_trainer.py` |
| **Production Artifacts** | `src/models/artifacts/production/` |
| **Dashboard Main** | `dashboard/src/app/(protected)/dashboard/page.tsx` |
| **Analysis Modal** | `dashboard/src/components/analysis/AnalysisModal.tsx` |
| **AI Q&A Route** | `dashboard/src/app/api/ask/route.ts` |
| **DFS Page** | `dashboard/src/app/(protected)/dfs/page.tsx` |
| **Data Vault** | `dashboard/src/app/(protected)/stats/page.tsx` |
| **Middleware** | `dashboard/src/middleware.ts` |
| **Types** | `dashboard/src/types/predictions.ts`, `dfs.ts`, `stats.ts` |
| **DB Migrations** | `database/migrations/` |
| **SQL Views** | `sql/views/` |
| **Architecture** | `ARCHITECTURE.md` |
| **Action Items** | `ACTIONITEMS.md` |
| **Changelog** | `CHANGELOG.md` |
| **Feature Catalogs** | `docs/nba_feature_catalog.md`, `docs/mlb_feature_catalog.md` |
| **Session Docs** | `docs/development_docs/2026-*_session*.md` |
