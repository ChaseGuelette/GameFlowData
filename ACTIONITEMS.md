# GameFlowData — Roadmap

## Session Summary (2026-02-13 — Session 28)

### What We Did

**Added "Play of the Day" featured card to dashboard.** Created a prominent hero card at the top of the predictions page highlighting the model's highest-edge pick.

**Key features:**
- Trophy badge header with amber/gold visual treatment
- Large player avatar (96x96), player name, team matchup, game time
- Stat badge + bet direction/line with star rating (1-5)
- Edge badge and model probability display
- "Analyze Pick" button opens analysis modal
- Responsive layout (stacked on mobile, horizontal on desktop)

**Filter integration:**
- Respects all current filters (date, edge threshold, BL blending, stat type, matchup)
- Uses `sortedPredictions[0]` — already filtered and sorted by max edge
- Disappears when no predictions available

**Files created:**
- `dashboard/src/components/predictions/PlayOfTheDay.tsx` — New featured card component (~85 lines)

**Files modified:**
- `dashboard/src/app/page.tsx` — Import and render PlayOfTheDay above PropGrid

**Tests:** 575 passed, 0 failures

### Next Step

1. **Paper trade** — Continue daily paper trading with automated pipeline
2. **Mobile responsiveness** — Test and refine dashboard on mobile devices
3. **Discord bot** — Follow development plan in `docs/discord_bot_development.md`

---

## Session Summary (2026-02-13 — Session 27)

### What We Did

**Massive inference job performance optimization.** Reduced total inference job runtime from ~180s to ~16s (10x faster) through two key optimizations:

**1. Parallel Feature Building:**
- Replaced sequential player-by-player feature store queries with `ThreadPoolExecutor` (8 workers)
- Increased connection pool from 5→10 and max_overflow from 2→6 in `src/db/client.py`
- Runtime: 65s → 4.8s (13x faster)

**2. Prop Lines Query Optimization:**
- Identified bottleneck: `raw_player_props_combined` table has 26.2M rows, `LPAD()` function in WHERE clause prevented index usage
- Modified query to search both 8-digit and 10-digit game_id formats without `LPAD()` in WHERE/PARTITION BY
- Created indexes via Supabase Dashboard: `idx_props_game_id`, `idx_props_game_market`, `idx_props_game_id_padded`
- Query runtime: 137s → 0.2s (685x faster)

**Reduced Odds API rate limiting.** Decreased sleep from 0.2s to 0.05s in scrapers (Odds API allows 30 req/s).

**Discord bot development planning.** Created comprehensive development plan for interactive Discord bot with slash commands (`/picks`, `/player`, `/bankroll`, `/performance`) and automated alerts after inference. Full plan at `docs/discord_bot_development.md`.

**Files modified:**
- `src/db/client.py` — Increased connection pool for parallel queries
- `src/models/daily_runner.py` — Parallel feature building + optimized lines query
- `src/scrapers/daily_player_props_scraper.py` — Reduced rate limiting
- `src/scrapers/game_lines_scraper.py` — Reduced rate limiting
- `docs/discord_bot_development.md` — New Discord bot development plan

**Tests:** 575 passed, 0 failures

### Next Step

1. **Discord bot implementation** — Follow development plan in `docs/discord_bot_development.md`
2. **Paper trade** — Continue daily paper trading with automated pipeline
3. **Mobile responsiveness** — Add responsive design to dashboard

---

## Session Summary (2026-02-13 — Session 26)

### What We Did

**Automated paper bet resolution.** Added `resolve_all_pending()` method to `PaperTrader` class that resolves ALL pending bets across multiple dates in a single call. Integrated into `daily_stats_job.py` as the final step — runs after stats are scraped and processed, automatically resolving any outstanding bets from previous days.

**Key features:**
- Multi-day catchup: finds all dates with pending bets, checks if game stats are available, resolves automatically
- Graceful failure: resolution errors don't fail the main stats job (stats are prioritized)
- CLI support: `resolve_bets.py --all-pending` for manual multi-day resolution
- Added `--skip-resolution` flag to daily_stats_job.py for debugging

**Dashboard date selector.** Added dropdown to view predictions from any date in the last 30 days:
- Uses `get_prediction_dates()` PostgreSQL RPC function for efficient distinct query (avoids Supabase 1000 row limit issue)
- Defaults to today, falls back to most recent date if today has no predictions
- Fixed timezone bug in `formatDate()` — was showing Feb 9 instead of Feb 10 due to UTC midnight interpretation

**Dashboard model parameter filters.** Added edge threshold and Black-Litterman blending dropdowns to filter predictions:
- **Edge threshold filter:** All, ≥3%, ≥5% (Rec), ≥7%, ≥10%, ≥15%, ≥20%
- **BL tau filter:** Off, τ=0.03, τ=0.05, τ=0.10 (Rec), τ=0.15, τ=0.25
- BL blending calculated client-side using `calculateBLConfidence()` and `blendProbability()` utility functions
- Removed hardcoded 3% edge filter from Supabase query — now filtered client-side based on user selection

**Files created/modified:**
- `src/paper_trading/paper_trader.py` — Added `resolve_all_pending()` method
- `src/paper_trading/resolve_bets.py` — Added `--all-pending` flag
- `src/orchestration/daily_stats_job.py` — Added `resolve_pending_bets()` as Step 8
- `dashboard/src/lib/utils.ts` — Fixed `formatDate()` timezone bug, added BL blending functions
- `dashboard/src/app/page.tsx` — Added date selector, edge/BL dropdowns, updated filtering logic
- `dashboard/src/types/predictions.ts` — Added `pred_mean`, `pred_std` fields

**Tests:** 575 passed, 0 failures (added 5 new tests for `resolve_all_pending()`)

### Next Step

1. **Paper trade** — Continue daily paper trading with automated resolution
2. **Mobile responsiveness** — Add responsive design for mobile viewing
3. **Health check integration** — Consider adding db_health_check to daily pipeline as monitoring step

---

## Session Summary (2026-02-13 — Session 25)

### What We Did

**Created comprehensive database health check script.** Built `src/diagnostics/db_health_check.py` with 8 validation categories to monitor data integrity, freshness, and linkage across all tables.

**Health check categories:**
1. **Data Freshness** — Latest dates for player_game_stats, raw_player_props_combined, rapidapi_injuries, daily_predictions
2. **Game Data Completeness** — Games per date, player counts per game (alerts if <20 players)
3. **Prop Linking Health** — NULL game_id/player_id/team_id rates (alerts if >10% unlinked)
4. **Aggregation Sync** — player_average_game_stats coverage vs player_game_stats
5. **Injury Linking** — Injuries without player_id (alerts if >20% unlinked)
6. **Position History** — Active players with position data
7. **Prediction Coverage** — Games with/without predictions, orphaned predictions
8. **Foreign Key Integrity** — Soft FK validation for player_id, team_id references

**Created incremental team_id backfill script.** Built `src/processing/backfill_team_ids_incremental.py` to process only recent data via staging_id threshold instead of all 26M+ rows.

**Updated daily_stats_job.py** to use incremental backfill (Step 3) instead of full table scan.

**Files created:**
- `src/diagnostics/__init__.py` — Package init
- `src/diagnostics/db_health_check.py` — Health check script (~580 lines)
- `src/processing/backfill_team_ids_incremental.py` — Incremental backfill (~160 lines)

**Files modified:**
- `src/orchestration/daily_stats_job.py` — Updated Step 3 to use incremental backfill

**Tests:** 575 passed, 0 failures

### Next Step

1. **Run inference** — Generate predictions for Feb 12-13
2. **Paper trade** — Continue daily paper trading
3. **Consider adding health check to daily pipeline** — Optional monitoring step

---

## Session Summary (2026-02-10 — Session 24)

### What We Did

**Enhanced Dashboard Analysis Modal with Line Shopping and Kelly Sizing.** Major improvements to the dashboard's analysis modal for better betting decision support.

**Key features added:**

1. **Sportsbook Line Shopping** — Shows all available bookmaker lines for each prop with:
   - Actual edge calculation using quantile-based probability estimation
   - Proper Under bet EV calculation (higher lines = easier to hit for unders)
   - Lines sorted by edge magnitude with "BEST" indicator
   - Bookmaker name formatting for cleaner display

2. **Kelly Bet Sizing Calculator** — Interactive bet sizing tool:
   - Bankroll input persisted to localStorage
   - Preset Kelly fractions (Full, Half, Quarter, Eighth) via dropdown
   - Toggle to switch to custom decimal input
   - Displays recommended bet size based on edge, odds, and Kelly fraction
   - Fixed leading zeros issue in bankroll input

3. **Matchup Filter** — Changed team filter from individual teams to matchup format:
   - "LAL vs SAS" instead of separate "LAL" and "SAS" options
   - More intuitive for filtering by game

4. **RLS Policies** — Added Supabase Row Level Security policies:
   - `player_game_stats` — for Last 5 games chart data
   - `raw_player_props_combined` — for sportsbook lines data
   - Enables browser-side access without auth issues

**Bug fixes:**
- Fixed Under bet probability estimation (was treating lower lines as better)
- Added NaN guards to EdgeBadge and QuantileSummary components
- Fixed useState lazy initialization pattern (removed useEffect setState warning)
- Removed unused variable in calculateKelly function

**Files modified:**
- `dashboard/src/components/analysis/AnalysisModal.tsx` — Major changes for line shopping, Kelly sizing
- `dashboard/src/app/page.tsx` — Matchup filter format
- `dashboard/src/components/shared/Badge.tsx` — NaN guards
- `dashboard/src/components/analysis/QuantileSummary.tsx` — NaN guards

**Tests:** 570 passed, 0 failures

### Next Step

1. **Paper trade** — Begin daily paper trading with optimized dashboard
2. **Mobile responsiveness** — Add responsive design for mobile viewing
3. **Date range selector** — Allow viewing historical predictions

---

## Session Summary (2026-02-10 — Session 23)

### What We Did

**Implemented per-stat configuration system.** Added ability to set different edge thresholds and Black-Litterman tau values for each stat type (pts, reb, ast). Backtesting showed REB performs best (+7.9% ROI) while AST is marginal — per-stat tuning allows tighter thresholds on weaker stats and looser on stronger ones.

**New files created:**
- `src/config/__init__.py` — Package init
- `src/config/stat_config.py` — Core dataclasses and CLI parsing (~230 lines)
- `tests/test_stat_config.py` — 30 unit tests

**Files modified:**
- `src/backtesting/bet_simulator.py` — Added `stat_config` parameter and `_get_edge_threshold(stat)` method
- `src/backtesting/backtest_harness.py` — Per-stat BL blenders and stat_config integration
- `src/backtesting/run_backtest.py` — CLI parsing with `nargs="+"` for per-stat values
- `src/backtesting/run_sweep.py` — StatConfigSet integration
- `src/paper_trading/paper_trader.py` — Per-stat edge thresholds
- `src/paper_trading/place_bets.py` — CLI parsing for per-stat edge thresholds
- `tests/test_run_backtest.py` — Fixed mock values for new CLI format

**CLI format:**
```bash
# Global (backward compatible)
--edge-threshold 0.05

# Per-stat
--edge-threshold pts=0.10 reb=0.07 ast=0.15

# Mixed (global default + overrides)
--edge-threshold 0.05 pts=0.10

# Per-stat BL tau (use "none" to disable)
--bl-tau pts=0.05 reb=0.10 ast=none
```

**Tests:** 570 passed, 0 failures

### Next Step

1. **Run backtest with per-stat tuning** — Test REB=0.07, PTS=0.10, AST=0.15 configuration
2. **Paper trade** — Begin daily paper trading with per-stat optimized thresholds
3. **Dashboard improvements** — Add date range selector, mobile responsiveness

---

## Session Summary (2026-02-10 — Session 22)

### What We Did

**Archived THREES (3-pointer) model.** After extensive development across C3 (hurdle), C4 (truncated NegBin), and C5 (multiclass PMF) approaches, the THREES model was archived due to poor market coverage and insufficient betting volume.

**Why archived:**
- 50% of THREES predictions had no odds available (sportsbooks don't offer 3PT props for many players)
- Only 2 bets out of 78 in backtesting came from THREES
- Extensive development time not justified by minimal betting opportunities

**Files archived to `archive/threes_model/`:**
- `threes_multiclass.py` — C5 multiclass PMF model (377 lines)
- `test_threes_multiclass.py` — Test suite (370 lines)
- `validate_threes_negbin.py` — C4 validation script (322 lines)
- `test_threes_global_params.py` — C4 diagnostic (154 lines)
- `test_threes_distribution.py` — C4 diagnostic (113 lines)
- `C4_threes_count_model.md` — Spec document (425 lines)

**Files modified:**
- `src/models/train_pipeline.py` — Removed THREES training, calibration, save/load
- `src/models/monte_carlo.py` — Removed THREES sampling, hurdle model logic
- `src/models/quantile_trainer.py` — Removed HurdleQuantileModel class, hurdle training
- `src/backtesting/backtest_harness.py` — Removed `player_threes` market mapping

**Preserved for future optionality:**
- Scrapers still collect `player_threes` market data (low cost)
- Feature columns remain in `feature_store.py`

**Tests:** 540 passed, 0 failures

### Next Step

1. **Paper trade** — Begin daily paper trading with PTS/REB/AST models
2. **Dashboard improvements** — Add date range selector, mobile responsiveness
3. **Monitor THREES market coverage** — If coverage improves, consider restoring from archive

---

## Session Summary (2026-02-10 — Session 21)

### What We Did

**Implemented C5 THREES Multiclass PMF Model.** Complete replacement for C3/C4 approaches. Instead of modeling continuous rates or count distributions, directly predicts a 9-class probability mass function (PMF) for made threes: P(threes=0), P(threes=1), ..., P(threes=8+).

**Why multiclass works better:**
- Discrete outcomes (0, 1, 2, ... made threes) are naturally categorical
- XGBoost multi:softprob directly outputs calibrated class probabilities
- No quantile-to-PMF conversion needed — model outputs ARE the distribution
- Categorical sampling produces integer counts directly

**Files created:**
- `src/models/threes_multiclass.py` — `ThreesMulticlassModel` class (~350 lines)
- `tests/test_threes_multiclass.py` — 25 unit tests

**Files modified:**
- `src/models/quantile_trainer.py` — imports and integration
- `src/models/monte_carlo.py` — `_sample_threes_multiclass()` for PMF-based sampling
- `src/models/train_pipeline.py` — `_calibrate_multiclass_model()` for evaluation

**Built Dashboard History & Performance Pages (G8 partial).** Added two new routes for viewing betting history and performance metrics:

**History Page (`/history`):**
- Status filter tabs (All/Won/Lost/Push)
- Summary stats bar (total bets, wins, losses, win rate, P&L)
- Individual bet cards showing player, stat, line, actual value, result, P&L
- Last 30 days of data from `paper_bets` table

**Performance Page (`/performance`):**
- KPI cards: Current Bankroll, Total P&L, Overall ROI, Win Rate
- Bankroll over time chart (Recharts AreaChart with green/red trend coloring)
- Performance by stat breakdown table (per-stat wins, losses, ROI)
- Data from `paper_trading_daily_log` and `paper_bets` tables

**Components created:**
- `dashboard/src/components/history/` — BetCard, BetList, HistoryFilters, HistorySummary
- `dashboard/src/components/performance/` — KPICard, BankrollChart, StatBreakdown

**Fixed auth callback route.** Added `dashboard/src/app/auth/callback/route.ts` to handle email confirmation redirects.

**Tests:** 570 passed, 0 failures

### Next Step

1. ~~**Retrain models** — Run training to activate C5 THREES multiclass model~~ *(Superseded — THREES archived)*
2. **Paper trade** — Begin daily paper trading with full pipeline
3. **Dashboard improvements** — Add date range selector, mobile responsiveness

---

## Session Summary (2026-02-09 — Session 20)

### What We Did

**Built Next.js Dashboard (G1-G4 partial).** Created web application for viewing daily predictions and analyzing player props. Previous session crashed due to invalid placeholder image file — recovered and completed setup.

**Tech Stack:**
- Next.js 16 with App Router, TypeScript, Tailwind CSS
- Supabase Auth (email/password) + SSR client
- Recharts for visualization
- Dark theme, desktop-first design

**Components created:**
- `Navbar` — Navigation with bankroll display from `paper_trading_daily_log`
- `FilterTabs` — Stat type filtering (All/PTS/REB/AST/THREES)
- `PropCard` / `PropGrid` — Prediction cards with edge badges
- `AnalysisModal` — Last 5 games chart + quantile summary
- `PlayerAvatar` — NBA headshots with inline SVG fallback
- `Badge` / `EdgeBadge` — Stat type and edge tier indicators
- Login page with email/password auth

**Data flow:**
- Main page fetches from `daily_predictions` + `players` tables
- Filters predictions by edge threshold (≥3%)
- Enriches with player names
- Auth middleware redirects unauthenticated users to `/login`

**Files created:**
- `dashboard/` — Complete Next.js project
- `dashboard/src/app/` — Page routes (home, login)
- `dashboard/src/components/` — React components
- `dashboard/src/lib/supabase/` — Client, server, middleware helpers
- `dashboard/src/types/predictions.ts` — TypeScript interfaces

**Fixed crash issue:** Previous instance wrote text to `placeholder-avatar.png` instead of image data, causing API errors. Replaced with inline SVG data URL.

**Tests:** 540 passed, 0 failures (coverage warning only — 50.32%)

### Next Step

1. **G5** — Complete analysis modal with feature-based insights
2. **G6** — Add "Lock of the Day" hero section
3. **Retrain models** — With off-by-one fix from Session 19
4. **Paper trade** — Begin daily paper trading

---

## Session Summary (2026-02-09 — Session 19)

### What We Did

**Fixed critical off-by-one bug in feature store LATERAL JOINs.** The feature store queries used `< game_date` to fetch pre-computed rolling averages, but `player_average_game_stats` uses `shift(1)` during population — meaning the row for `game_date X` already contains averages from games BEFORE X (not including X). The `<` logic caused queries to fetch the PREVIOUS game's row instead of the current game's row, resulting in models training and predicting with stale features (one game behind).

**Fix:** Changed `< game_date` to `<= game_date` in 15 LATERAL JOINs across 3 feature store methods:
- `get_features_for_date()` — backtesting
- `get_features_for_date_range()` — batch backtesting
- `_load_single_season_training()` — model training

Added explanatory comments to clarify why `<=` is safe (not data leakage).

**Injury queries unchanged:** Queries that look up OTHER players' historical stats (e.g., teammates out with injuries) correctly use `<` since they're fetching past game data, not pre-computed rolling stats.

**Fixed daily runner inference bug:** Added 30-day recency filter to `_get_players_for_games()` to exclude retired players (e.g., Shaquille O'Neal, Grant Hill) from predictions. Also added `target_date` parameter to method signature for proper cutoff calculation.

**Fixed test failures:** Updated 6 failing tests to match new method signatures and mock data structures.

**Files modified:**
- `src/models/feature_store.py` — Changed 15 LATERAL JOINs from `<` to `<=`
- `src/models/daily_runner.py` — Added `target_date` param, 30-day recency filter
- `tests/test_daily_runner.py` — Updated 2 tests for new signature
- `tests/test_feature_store.py` — Updated 4 tests with proper mock data

**Tests:** 540 passed, 0 failures

### Next Step

1. **Retrain models** — Critical: models must be retrained to benefit from the off-by-one fix
2. **Run backtest** — Verify calibration and ROI with current-game features
3. **Paper trade** — Begin paper trading with automated pipeline

---

## Session Summary (2026-02-09 — Session 18)

### What We Did

**Fixed C4 THREES truncated NegBin mu training target.** Identified and fixed critical bug causing 25.8% calibration gap at Q10 in the THREES count model.

**Bug:** The mu model was trained to predict `log(observed_count + 0.5)`, but observed values come from the **truncated** distribution (conditioned on X > 0), which has a higher mean than the underlying untruncated distribution. For truncated NegBin: E[X | X > 0] = μ / (1 - P(X=0)), so observed values are inflated by ~26%.

**Fix:** Applied truncation adjustment factor in `truncated_negbin.py`:
```python
# Before (wrong):
log_mu_target = np.log(y + 0.5)

# After (correct):
p_zero_global = nbinom.pmf(0, 1/alpha, ...)  # ~0.26
log_mu_target = np.log((y + 0.5) * (1 - p_zero_global))
```

This scales down training targets by ~26%, bringing predicted mu from ~2.5 to correct ~1.66.

**Added training safety pattern (atomic rename).** Prevents race condition where inference job at 6:30 PM could select an incomplete model directory if training is in progress.

**Implementation:**
- Training creates `run_YYYYMMDD_HHMMSS_incomplete` directory
- Renamed to `run_YYYYMMDD_HHMMSS` only after all artifacts saved
- Inference job filters out `_incomplete` directories when auto-selecting

**Files modified:**
- `src/models/truncated_negbin.py` — Truncation adjustment for mu training target
- `src/models/train_pipeline.py` — Atomic rename pattern
- `src/orchestration/inference_job.py` — Filter incomplete directories

**Tests:** 536 passed, 4 pre-existing failures in `test_feature_store.py` (mock issues)

### Next Step

1. **Retrain models** — Run training to verify C4 THREES calibration improvement
2. **Paper trade** — Begin paper trading with automated pipeline
3. **Fix pre-existing test failures** — 4 mock issues in `test_feature_store.py`

---

## Session Summary (2026-02-09 — Session 17)

### What We Did

**Fixed Windows Task Scheduler batch scripts and tested all scheduled tasks.** Multiple critical bugs prevented scheduled tasks from working correctly.

**Issues Fixed:**
1. **Virtual environment path mismatch** — Batch scripts referenced `.venv\Scripts\activate` but actual path is `venv\Scripts\activate`
2. **PYTHONPATH missing** — Subprocess calls failed with `ModuleNotFoundError: No module named 'src'`
3. **Log file permission conflict** — Python's FileHandler and shell redirect both trying to write to same log file
4. **SQL syntax error** — `:snap_date::DATE` in `update_player_position_history.py` conflicted with SQLAlchemy parameter binding

**Files modified:**
- `scripts/run_daily_stats.bat`, `scripts/run_lines.bat`, `scripts/run_inference.bat` — Fixed venv path, added PYTHONPATH, removed log redirect
- `src/scrapers/update_player_position_history.py` — Changed `:snap_date::DATE` to `CAST(:snap_date AS DATE)`
- `src/orchestration/daily_stats_job.py` — Updated to use incremental stats script

**Performance Optimization (Major):**
Created `src/processing/populate_average_stats_incremental.py` — lightweight daily version:
- Only processes players who played on target date (vs all players)
- Fetches last 20 games per player (vs full history)
- Uses UPSERT instead of TRUNCATE + reload
- **Result: 1.0s vs 1709s (28.5 min) — 1700x speedup**

**Verified working:**
All 5 production scheduled tasks tested and confirmed working via Windows Task Scheduler.

### Next Step

1. **Paper trade** — Begin paper trading with automated pipeline
2. **Retrain with C4** — Run training to activate truncated NegBin for THREES
3. **Investigate pre-existing test failures** — 4 failing tests in `test_feature_store.py` (mock issues)

---

## Session Summary (2026-02-09 — Session 16)

### What We Did

**Set up Windows Task Scheduler for daily pipeline automation.** Created batch scripts and scheduled tasks for local Windows deployment of the daily scraping and inference pipeline.

**Backtest Analysis:** Reviewed sweep results from 2026-02-08. Top performing config: `tau=0.5, z_max=1.0, edge=0.15, kelly=0.125` with $27,379 profit, 10.87% ROI, 57.6% hit rate, 1.21 Sharpe.

**Files created:**
- `scripts/run_daily_stats.bat` — Wraps daily_stats_job.py for Task Scheduler
- `scripts/run_lines.bat` — Wraps lines_job.py for Task Scheduler
- `scripts/run_inference.bat` — Wraps inference_job.py for Task Scheduler

**Windows Task Scheduler tasks created:**
| Task | Schedule | Script |
|------|----------|--------|
| GameFlow-DailyStats | 9:00 AM | run_daily_stats.bat |
| GameFlow-Lines-12PM | 12:00 PM | run_lines.bat |
| GameFlow-Lines-4PM | 4:00 PM | run_lines.bat |
| GameFlow-Lines-6PM | 6:00 PM | run_lines.bat |
| GameFlow-Inference | 6:30 PM | run_inference.bat |

**Key backtest insights:**
- BL blending (tau=0.5) now works after A3b fix — produces meaningful weights
- PTS strongest stat: +17.5% ROI, 241 bets
- REB solid: +1.9% ROI, 109 bets
- AST marginal: +3.2% ROI, 77 bets
- Edge > 0.20 bucket: +20% ROI (214 bets)

### Next Step

1. **Test scheduled tasks** — Run `scripts\run_lines.bat` manually to verify
2. **Paper trade** — Begin paper trading with automated pipeline
3. **Retrain with C4** — Run training to activate truncated NegBin for THREES

---

## Session Summary (2026-02-09 — Session 15)

### What We Did

**Implemented C4 THREES Truncated Negative Binomial Count Model.** Replaced the failed C3 hurdle+quantile regression approach with a proper count model for discrete integer outcomes.

**Why C3 failed:** Quantile regression produces continuous values (e.g., Q10=1.2) for discrete outcomes (made threes are 0, 1, 2, 3...). With ~35% zero mass and p_zero≈0.47, Q10 maps to the 5.7th percentile of the positive distribution, requiring extrapolation below the training range. Result: 25.6% calibration gap at Q10.

**C4 Solution:**
1. **Stage 1 (unchanged):** XGBoost binary classifier with isotonic calibration for P(zero)
2. **Stage 2 (new):** Truncated Negative Binomial model predicting μ (mean) and α (overdispersion)
   - Two XGBoost regressors predict log(μ) and log(α) for positivity
   - Inverse CDF sampling produces integer counts directly
   - Handles overdispersion (variance ≈ 2.8 vs mean ≈ 2.1)

**New files created:**
- `src/models/truncated_negbin.py` (~500 lines) — `TruncatedNegBinModel` class
- `tests/test_truncated_negbin.py` — 17 unit tests (all pass)
- `scripts/validate_threes_negbin.py` — Phase 0 validation (chi-squared test)

**Files modified:**
- `src/models/quantile_trainer.py` — Added `_train_threes_count_model()`, updated `save_all()`/`load_all()`
- `src/models/monte_carlo.py` — Added `_sample_threes_count()`, `_has_threes_count_model()`
- `src/models/train_pipeline.py` — Added `_calibrate_count_model()`, updated evaluation methods

**Fixed during session:**
- `AttributeError: feature_names_in_` — XGBoost doesn't reliably expose feature names. Fixed by storing `threes_zero_feature_names` explicitly.

**Test results:** All 523 tests pass.

### Next Step

1. **Retrain models** — Run training pipeline to generate C4 count model for THREES
2. **Run backtest with threes** — `--stats pts reb ast threes` to validate calibration improvement
3. **Validate Q10 gap** — Target: < ±5% gap (down from 25.6%)

---

## Session Summary (2026-02-07 — Session 14)

### What We Did

**Fixed critical backtesting data issues.** Investigated and resolved two bugs causing the backtest to miss 85%+ of available prop lines:

**Issue 1: Incomplete model directory selection**
- `find_latest_model_dir()` was auto-selecting `run_20260206_171812`, an incomplete training run with only `run_config.json` (no model files)
- Root cause: Training started but never completed, leaving empty artifact directory
- **Fix:** Updated `find_latest_model_dir()` in `run_sweep.py` to validate `minutes_model.joblib` exists before selecting a directory; logs warning when skipping incomplete runs

**Issue 2: Game ID format mismatch**
- `player_game_stats.game_id` uses 10-digit format: `0022500589`
- `raw_player_props_combined.game_id` was storing 8-digit format: `22500589` (missing leading zeros)
- The JOIN in `backtest_harness.py` failed for mismatched IDs
- **Fix (backtest):** Changed JOIN to use `LPAD(rp.game_id, 10, '0') = gd.game_id` to handle both formats
- **Fix (linker):** Added `.zfill(10)` when storing game_ids in lookup dictionaries to ensure proper format for future runs

**Impact:**
| Metric | Before Fix | After Fix |
|--------|------------|-----------|
| Lines chunk 1 (Jan 1-15) | 33,565 | 119,129 |
| Lines chunk 2 (Jan 16-29) | 397 | 72,779 |
| Total lines | 33,962 | 191,908 |
| Total bets (edge=0.05) | 889 | 2,251 |

**Files modified:**
- `src/backtesting/run_sweep.py` — Validation in `find_latest_model_dir()`
- `src/backtesting/backtest_harness.py` — LPAD in prefetch lines query
- `src/processing/nba_linker_local.py` — `.zfill(10)` in 3 locations for game_id storage
- `tests/test_backtest_harness.py` — Fixed pre-existing test failure (missing `all_edges_df`)

### Next Step

1. **Run full BL parameter sweep** — With corrected data, rerun sweep with higher tau values (0.3-1.0) to find optimal BL configuration
2. **Retrain models** — Run training pipeline to generate fresh hurdle model for THREES
3. **Paper trade** — Begin paper trading with automated pipeline

---

## Session Summary (2026-02-05 — Session 13)

### What We Did

**Implemented E6 Daily Pipeline Automation.** Created frequency-separated job scripts for cron scheduling, separating stats scraping (once daily) from lines/injuries (multiple times daily) and inference (pre-game).

**New files created:**

| File | Purpose |
|------|---------|
| `src/orchestration/daily_stats_job.py` | Once-daily NBA stats + processing (6 AM ET) |
| `src/orchestration/lines_job.py` | Multiple-times-daily props + injuries (12/4/6 PM ET) |
| `src/orchestration/inference_job.py` | Pre-game predictions (6:30 PM ET) |
| `.session/specs/E6_daily_automation.md` | Full specification document |
| `cron/gameflow_crontab.txt` | Server cron schedule template |
| `logs/` | Job execution log directory |

**`daily_stats_job.py` (6:00 AM ET):**
- Scrapes previous night's NBA game results via `nba_unified_scraper.py`
- Runs full processing pipeline: linker → team_ids → positions → averages → opponent stats
- Runtime: ~2-5 minutes

**`lines_job.py` (12 PM, 4 PM, 6 PM ET):**
- Scrapes game lines, player props, injuries
- Runs incremental linker for new props
- Options: `--date`, `--skip-injuries`, `--skip-linker`, `--dry-run`
- Runtime: ~30-90 seconds

**`inference_job.py` (6:30 PM ET):**
- Loads model artifacts (auto-detects latest `run_*` directory)
- Generates predictions via `DailyPredictionRunner` with 10K Monte Carlo samples
- Stores to `daily_predictions` + `daily_prediction_samples` tables
- Exports CSV backup to `predictions/` directory
- Options: `--date`, `--model-dir`, `--stats`, `--dry-run`
- Runtime: ~1-3 minutes

**Also investigated:** Root cause of missing backtest bets after Jan 9 — `game_id_map_staging` table lacks mappings for games after Jan 10 because linker upload step never completed.

### Next Step

1. **Run linker upload** — `python src/processing/nba_linker_local.py upload` to fix missing game mappings
2. **Retrain models** — Run training pipeline to generate hurdle model for THREES
3. **Deploy automation** — Set up cron jobs on server using `cron/gameflow_crontab.txt` template
4. **Paper trade** — Begin paper trading with automated pipeline

---

## Session Summary (2026-02-05 — Session 12)

### What We Did

**Implemented lightweight incremental linker.** Added `incremental` command to `nba_linker_local.py` for daily automated linking without downloading the full 25M+ row `raw_player_props_combined` table.

**Changes to `src/processing/nba_linker_local.py`:**
- Added `link_incremental()` function (~240 lines)
- Added `normalize_player()` at module level (was previously a local function)
- Expanded `TEAM_NAME_ALIASES` to map all team names to 3-letter abbreviations (e.g., "Atlanta Hawks" → "ATL")
- Added `--batch-size` and `--limit` CLI arguments
- Incremental mode: queries only unlinked records (`WHERE player_id IS NULL`), matches against reference tables, updates directly via batched SQL

**Changes to `src/orchestration/run_daily.py`:**
- Fixed broken linker call on line 114 (was missing command argument)
- Changed to use `incremental` command for automated daily pipeline

**Test updates:**
- Updated `test_normalize_team_aliases` to expect 3-letter abbreviations
- All 518 tests pass (5 pre-existing failures unrelated to this work)

**Test results:**
- Player match rate: 99.3% (4,963/5,000)
- Game match rate: 40.7% (2,037/5,000) — lower because many props are for future games not yet in DB
- Total unlinked: ~2.8M records

### Next Step

1. **Retrain models** — Run training pipeline to generate hurdle model for THREES
2. **Validate C3** — Check THREES calibration gaps < 5% on holdout
3. **Run full incremental linker** — Link all ~2.8M unlinked combo market records
4. **E6** — Automate daily scheduling

---

## Session Summary (2026-02-05 — Session 11)

### What We Did

**Fixed BL confidence function (A3b).** Replaced the exponential confidence formula with linear ramp to enable meaningful BL blending weights for realistic betting edges.

**Implemented THREES hurdle model (C3).** Two-stage architecture for zero-inflated THREES distribution that can correctly predict Q0.10 = 0 when ~35% of samples are exactly zero.

**Changes to `src/models/black_litterman.py`:**
- Added `z_max` parameter to `BLConfig` (default 1.0)
- Changed `compute_confidence()` from `1 - exp(-0.5 * z²)` to `min(z / z_max, 1.0)`
- Added `--z-max` to sweep parameter grid

**Changes to `src/models/quantile_trainer.py`:**
- Added `HurdleQuantileModel` dataclass with two-stage architecture
- Added `train_hurdle_model()` function for training hurdle models
- Modified `PlayerPropsModelPipeline.train_rate_models()` to use hurdle for THREES
- Updated `save_all()` and `load_all()` to handle hurdle model artifacts
- Hurdle model includes isotonic regression calibration for P(zero) classifier

**Changes to `src/models/monte_carlo.py`:**
- Added `_sample_hurdle()` and `_sample_hurdle_from_quantiles()` methods
- Modified `_predict_copula()` to detect and use hurdle models
- Modified `predict_batch_for_date()` to handle hurdle models
- Bernoulli draw (zero vs positive) is independent of copula; copula affects positive rate magnitude only

**Changes to `src/models/train_pipeline.py`:**
- Added `_calibrate_hurdle_model()` method with zero-accuracy diagnostics
- Updated `_evaluate_calibration()` to evaluate hurdle models
- Updated `_evaluate_combined_calibration()` to include hurdle stats

**Test updates:**
- All 42 BL tests pass
- 518 of 523 tests pass (5 pre-existing failures unrelated to hurdle model)

**Impact:** THREES Q0.10 should now correctly return 0 when P(zero) > 0.10, fixing the +20.4% calibration gap. Enables betting on 4th stat.

### Next Step

1. **Retrain models** — Run training pipeline to generate hurdle model for THREES
2. **Validate C3** — Check THREES calibration gaps < 5% on holdout
3. **E6** — Automate daily scheduling

---

## Session Summary (2026-02-05 — Session 10)

### What We Did

**Reviewed project status and added `--bl-sizing-tau` parameter.** Implemented BL-blended probabilities for Kelly position sizing (separate from edge detection):

- Added `--bl-sizing-tau` CLI parameter to `run_backtest.py`
- Added `bl_sizing_blender` field to `BacktestHarness`
- Modified `_calculate_edges()` to compute `sizing_prob_over`/`sizing_prob_under` columns
- Modified `BetSimulator.place_bet()` to accept optional `sizing_prob` for Kelly calculation
- Verified implementation with one-day backtest (same bet count, different stakes)

**Key finding:** Model is no longer catastrophically overconfident (Brier improved from 0.2705 to 0.2506). The no-BL approach shows +3.5% ROI. However, the BL confidence function issue persists — it crushes sizing probs toward market, resulting in near-zero Kelly stakes even with BL sizing enabled.

**Verified testing scripts are still valid:**
- `src/models/analyze_calibration_drift.py` — minutes-rate correlation, combined calibration
- `src/models/analyze_minutes_bimodality.py` — spread/blowout handling
- `src/backtesting/visualize_results.py` — HTML dashboard generation

**Created specs for next items:**
- `.session/specs/A3b_BL_confidence_fix.md` — Linear ramp confidence function
- `.session/specs/C3_THREES_hurdle_model.md` — Zero-inflated model for THREES

**Verified combo markets scraping job (2026-01-31):** Confirmed ~35K new prop lines successfully added to `raw_player_props_combined`:
- `player_points_rebounds_assists` (12K rows)
- `player_points_rebounds` (8K rows)
- `player_points_assists` (6K rows)
- `player_rebounds_assists` (5K rows)
- `player_blocks_steals` (3K rows)
- `player_field_goals` (2K rows)

### Next Step

1. **A3b** — Fix BL confidence function (linear ramp) — Quick win, enables BL for sizing/filtering
2. **C3** — Implement THREES hurdle model — Enables betting on 4th stat
3. **E6** — Automate daily scheduling

---

## Session Summary (2026-02-04 — Session 9 continued)

### What We Did

**Built paper trading infrastructure (E5).** Created standalone CLI scripts to convert stored `daily_predictions` into paper bets with bet selection, outcome resolution, and P&L tracking.

**Implementation:**
- **Database tables:** `paper_bets` (individual bets) and `paper_trading_daily_log` (daily aggregates)
- **`src/paper_trading/paper_trader.py`** — Core `PaperTrader` class with `select_bets()`, `place_bets()`, `resolve_bets()` methods
- **`src/paper_trading/place_bets.py`** — CLI to place paper bets (supports `--dry-run`, `--edge-threshold`, `--kelly-fraction`)
- **`src/paper_trading/resolve_bets.py`** — CLI to resolve bets using actual game results
- **Unit tests:** 20 tests covering Kelly calculation, bet selection logic, resolution, and defaults

**Design decisions:**
- Standalone scripts (not integrated into `run_daily.py`) for future lightweight dashboard
- Supports pts, reb, ast stats only
- SQL tables for dashboard display

**Earlier in session:** Fixed daily injury pipeline (E4) — `--scrape-injuries` now uses RapidAPI + linker.

### Next Step

With E4 and E5 complete:
1. Retrain models with calibration fixes (E1b)
2. Automate daily scheduling (E6)
3. Paper trade for 2-4 weeks (E7)

---

## Session Summary (2026-01-31 — Session 8)

### What We Did

**Calibration fixes for zero-inflated distributions.** Training showed THREES rate model Q0.10 with +20.4% calibration gap (coverage 0.352 vs target 0.10). Root cause: zero-inflated distribution where 35%+ of `threes_per_min` is exactly 0. Three fixes applied:
1. **Conformal recalibration** (`quantile_trainer.py`) — computes validation residual offset when coverage gap > 3%, applied at prediction time. Standard technique in probabilistic forecasting.
2. **Zero-snap handling** (`monte_carlo.py`) — snaps quantile values below 1e-3 to exactly 0 in `_build_extended_quantile_fn()`. Ensures MC samples in zero-mass region map to 0 instead of tiny positive interpolated values.
3. **Threes in combined calibration** (`train_pipeline.py`) — `_evaluate_combined_calibration()` now dynamically evaluates all trained rate models instead of hardcoded `["pts", "reb", "ast"]`.

**BL parameter sweep analysis.** Ran full BL parameter sweep (40 configs across tau × edge × kelly):
- **No-BL configs profitable:** +3% ROI, 600-873 bets. REB is strongest at +7.9% ROI.
- **All BL configs eliminated:** 0-12 bets across all tau values (0.01-0.30).
- **Root cause identified:** BL confidence formula `1 - exp(-0.5 * z²)` produces near-zero confidence for realistic edges. For P(over)=0.55, z~0.13, confidence~0.008, w=tau*0.008 → edge crushed from 3% to 0.006%.
- **Conclusion:** Model DOES find edges (visible in no-BL results). BL confidence function is structurally broken for this use case — it demands z > 1.0 for meaningful weight, but profitable edges have z < 0.5.

### Next Step

Retrain models with calibration fixes (conformal recalibration + zero-snap). Then either:
1. Proceed to paper trading with no-BL strategy (model shows +3% ROI, REB +7.9%)
2. Redesign BL confidence function (fixed-weight tau, or linear confidence ramp) and re-sweep

---

## Session Summary (2026-01-31 — Session 7)

### What We Did

**Prediction storage + query tool.** Built full prediction persistence pipeline:
- `src/models/prediction_store.py` — stores daily predictions and gzip-compressed MC samples to PostgreSQL
- DB migration: `daily_predictions` (quantiles + edges) and `daily_prediction_samples` (compressed bytea) tables
- `src/tools/query_player.py` — CLI tool for querying stored predictions (line probability, player overview, top edges)
- `src/orchestration/run_daily.py` — wired `PredictionStore` into daily pipeline with `--skip-storage` flag

**Daily runner audit + refactor.** Comprehensive audit found 4 issues; all fixed:
- **Game discovery:** Replaced `team_game_stats` query (only has post-game data) with NBA API ScoreboardV2 as primary source. DB query retained as fallback for past dates.
- **Injury filtering:** Switched from `espn_injuries` (string name matching) to `rapidapi_injuries` (integer `player_id` matching), consistent with feature store and backtest harness.
- **Edge calculation:** Switched from 5-point quantile interpolation to MC samples empirical CDF (`(samples > line).mean()`) with quantile fallback. Consistent with backtest harness.
- **Line freshness:** Added `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY snapshot_time DESC)` to `_get_current_lines()` to use only the latest snapshot per line.

**Scraper resume capability.** Completed resume capability in `player_prop_scraper.py`:
- Market-aware progress file format (`{"markets": "...", "processed": [...]}`)
- Skip logic in main loop for already-processed events
- Progress saving after each snapshot and on interrupt/error
- `--no-resume` flag to start fresh

### Next Step

Run comprehensive BL parameter sweep backtest via `run_sweep.py` on the out-of-sample period
(2025-10-22 to 2026-01-29) to find optimal `(tau, edge_threshold, kelly_fraction)` configuration
and evaluate whether the model + BL blending produces positive edge.

---

## Session Summary (2026-01-30)

### What We Did

**Comprehensive bug fix sweep.** Created `ISSUES.md` with 28-issue pipeline audit. Fixed 12 issues
in a single commit — 1 critical, 5 high, 6 medium:

| Fixed | Severity | Summary |
|-------|----------|---------|
| ISS-001 | CRITICAL | Minutes model now uses tuned hyperparams (`self.config` → `config`) |
| ISS-002 | HIGH | `_run_date()` returns proper tuple instead of `None` |
| ISS-003 | HIGH | Non-BL edge path now devigs implied probabilities (was understating edges ~2-3%) |
| ISS-004 | HIGH | Injury LATERAL JOIN split into 2 subqueries (eliminates cross-product) |
| ISS-005 | HIGH | Training `min > 0` → `min >= 5` (matches inference threshold) |
| ISS-006 | HIGH | `early_stopping_rounds` now actually passed to `model.fit()` |
| ISS-007 | MEDIUM | Copula params computed before combined calibration; passed to MC predictor |
| ISS-008 | MEDIUM | Spread now team-directional (negative = player's team favored) |
| ISS-009 | MEDIUM | COALESCE defaults changed from 0 to league averages across all paths |
| ISS-011 | MEDIUM | Inference advanced stats JOIN matches bulk LATERAL pattern |
| ISS-015 | MEDIUM | Line shopping selects best over and best under independently |
| ISS-016 | MEDIUM | Calibration prediction failures logged as WARNING with count |

16 issues remain open (mostly low-priority). See `ISSUES.md` for details.

**Built parameter sweep tool.** `src/backtesting/run_sweep.py` (778 lines) — runs Phase 0-1
(DB fetch + MC predictions) once, then sweeps the full cartesian grid of `(tau, edge_threshold,
kelly_fraction)` configurations. Per-config output directories compatible with `visualize_results.py`.
651 lines of tests in `tests/test_run_sweep.py`.

**Expanded bookmaker coverage.** Added 11 US2/us_ex bookmakers to default lists: ballybet,
betopenly, betparx, espnbet, fliff, hardrockbet, novig, polymarket, prophetx, rebet, windcreek.

**Improved scraper CLI.** Both `daily_player_props_scraper.py` and `player_prop_scraper.py`
now support `--combos`, `--combos-only`, `--markets` flags. Historical scraper adds `--start-date`,
`--end-date`, and `--dry-run` (credit estimation without scraping).

**Daily runner sharpest-book selection.** `daily_runner.py` now fetches all bookmakers and selects
the lowest-vig (smallest booksum) line per player/game/market. Implied probabilities devigged.

**Models retrained.** Latest complete artifact: `run_20260129_205540` (trained on 22023+22024,
calibrated on 22025 through 2026-01-01). Includes all bug fixes, new features, copula params,
and feature selection. Models have early stopping active, use `min >= 5` threshold, and include
all B1-B4 + A4 features.

### Next Step

Run comprehensive BL parameter sweep backtest via `run_sweep.py` on the out-of-sample period
(2025-10-22 to 2026-01-29) to find optimal `(tau, edge_threshold, kelly_fraction)` configuration
and evaluate whether the model + BL blending produces positive edge.

---

## Session Summary (2026-01-28)

### What We Learned

**Minutes bimodality is not real.** Ran `analyze_minutes_bimodality.py` across all spread
segments. Bimodality coefficient *decreases* from 0.419 (close games) to 0.354 (extreme
blowouts) — the opposite of the hypothesis. The model's minutes predictions are well-calibrated
across all segments via the `line_spread` feature. No mixture model needed. This item is closed.

**The "winning strategy" was a Gaussian artifact.** The previous +27.86% ROI on PTS under +
REB over was computed using a Gaussian CDF edge calculation (`stats.norm.sf(z_score)`) applied
to non-Gaussian Monte Carlo distributions. Session 1 (Jan 27) replaced this with the correct
empirical CDF (`(samples > line).mean()`). Re-running the filtered backtest with the fix:

| Segment | Old (Gaussian CDF) | New (Empirical CDF) |
|---------|--------------------|--------------------|
| PTS under | +12.86% ROI | -8.84% ROI |
| REB over | +17.99% ROI | -5.18% ROI |
| Combined | +27.86% ROI @ 0.15 | -8.15% ROI |

The empirical CDF implementation was verified line-by-line — it is correct. The model is
well-calibrated (all quantiles within OK band) but does not beat the market. The 49.2% hit rate
is below the ~52.4% breakeven at -110 vig.

**Root cause: the model is catastrophically overconfident, not market-correlated.**
The market neutralization diagnostic (A1) revealed a surprise — the model is NOT a market
clone. R² of `model_prob` regressed on `implied_prob` is only **0.104** (10% explained).
The real problem is probability miscalibration against prop lines:

| Metric | Market | Model | Naive (50%) |
|--------|--------|-------|-------------|
| Brier score | 0.2495 | **0.2705** | 0.2500 |
| Correlation w/ outcome | 0.079 | 0.046 | — |
| Residual signal | — | **0.022** | — |

The model is **worse than a coin flip** on Brier score. When it predicts 84% over → actual
is 49.1%. Its quantile calibration is good (Q10–Q90 all within OK bands) but its *probability
calibration against prop lines* is catastrophically wrong. The model's MC distributions are
reasonable in shape, but translating `(samples > line).mean()` into a betting probability
produces extreme overconfidence because the distribution centers near the line (as it should
for a well-calibrated model) and small shifts in mean create large swings in P(over).

The model's residuals vs the market have essentially zero predictive signal (r = 0.022).
This means the model currently adds no independent information beyond what the market already
prices. Per Hubacek et al., profitability requires independent signal the market hasn't priced.

### New Strategic Direction

The path forward has three pillars:

1. **Probability recalibration** — the model's raw P(over) is useless. Black-Litterman
   blending anchors to the market's well-calibrated prior and extracts whatever small
   independent signal the model has. This is the critical first step.
2. **Market decorrelation** — restructure how the model relates to the market (residual
   modeling, remove market leakage features) to increase independent signal.
3. **New signal sources** — add features the market prices imperfectly (injury context,
   rest/fatigue, short-window trends, lineup effects)

---

## Track A: Probability Recalibration & Market Decorrelation (Critical Path)

These items address the fundamental problem: the model's raw probabilities are catastrophically
overconfident and contain no independent signal beyond the market. Ordered by effort (easiest first).

- [x] **A1. Run post-hoc market neutralization diagnostic** *(DONE — 2026-01-28)*
  Regressed `model_prob` on `implied_prob`. R² = 0.104 — model is NOT a market clone.
  However, the model is catastrophically overconfident (Brier 0.2705 vs naive 0.2500).
  Model residuals have zero predictive signal (r = 0.022 with outcomes).
  **Conclusion: the problem is probability miscalibration, not market correlation.**
  Black-Litterman blending is the correct first fix — anchor to market prior.

- [x] **A2. Remove `line_total` from rate features** *(DONE — 2026-01-28)*
  `line_total` (Vegas game total) was in `RATE_FEATURES_PTS`. Removed it to eliminate market
  leakage. `line_total` remains in `MINUTES_FEATURES` (genuinely predicts playing time).
  `line_spread` remains in `MINUTES_FEATURES` only.
  **Retrained** in `run_20260129_205540` — `line_total` removed from PTS rate features (though feature selection may still select it for other stats/quantiles where it provides signal).

- [x] **A3. Implement Black-Litterman blending layer** *(IMPLEMENTED — 2026-01-28, STRUCTURAL ISSUE FOUND — 2026-01-31)*
  New module `src/models/black_litterman.py` between `MonteCarloPredictor` and `BetSimulator`.
  The A1 diagnostic proved this is the correct fix: the model's raw P(over) is useless
  (Brier 0.2705), but the market is well-calibrated (Brier 0.2495). BL anchors to the market
  prior and only deviates when the model shows high-confidence disagreement.

  **Implementation (completed):**
  - `BlackLittermanBlender` class with `BLConfig` dataclass in `src/models/black_litterman.py`
  - **Prior**: Devigged market probability via multiplicative normalization (equivalent to Shin's method for 2-outcome markets)
  - **View**: Model's empirical P(over) from MC samples
  - **Confidence**: Z-score based: `z = |mean - line| / std`, `confidence = 1 - exp(-0.5z²)`
  - **Blending**: Log-odds space (not linear probability) to handle boundary effects:
    `posterior_logit = market_logit + w × (model_logit - market_logit)` where `w = min(tau × confidence, max_weight)`
  - **Integration**: Wired into `_calculate_edges()` in `backtest_harness.py`. Enabled via `--bl-tau` CLI flag on `run_backtest.py`. Disabled by default (backward compatible).
  - **Diagnostics**: Extra columns in predictions CSV: `model_over/under`, `market_over/under`, `confidence`, `posterior_over/under`
  - **Tests**: 39 unit tests in `tests/test_black_litterman.py` (all passing)

  **Structural issue found (2026-01-31):** BL parameter sweep (40 configs) showed ALL BL configs produce 0-12 bets while no-BL shows 600-873 bets at +3% ROI. The confidence formula `1 - exp(-0.5 * z²)` is near-zero for realistic edges (z < 0.5). For a 3% edge: z~0.13, confidence~0.008, w~0.0008. The BL layer demands z > 1.0 for meaningful weight, but profitable edges exist in the z < 0.5 range. This is a design flaw, not a model quality issue.

  **Fix applied (2026-02-05):** Replaced exponential confidence with linear ramp.

- [x] **A3b. Fix BL confidence function** *(IMPLEMENTED — 2026-02-05)*
  Replaced exponential confidence formula with linear ramp in `black_litterman.py`.

  **Changes:**
  - Added `z_max` parameter to `BLConfig` (default 1.0)
  - Changed `compute_confidence()`: `confidence = min(z / z_max, 1.0)`
  - Updated 3 existing tests with new expected values
  - Added 4 new tests for linear ramp behavior

  **Result:** At z=0.13 (3% edge), confidence now equals 0.13 instead of 0.008.
  This is a 16x improvement in effective weight for realistic betting edges.

  **Next step:** Run backtest with `--bl-tau 0.10` to verify meaningful bet counts

- [x] **A4. Residual modeling (Option A — feature-based)** *(IMPLEMENTED — 2026-01-28)*
  Added per-stat prop lines (`prop_line_pts`, `prop_line_reb`, `prop_line_ast`, `prop_line_threes`)
  as centering features to all rate models. The model now sees market expectation and learns
  deviations rather than absolute values.

  **Implementation:**
  - LATERAL JOIN to `raw_player_props_combined` in all 4 feature store query paths
    (`get_training_dataset`, `get_features_for_date`, `get_features_for_date_range`, `get_player_game_features`)
  - `DISTINCT ON (market_key)` deduplication: picks most recent snapshot per stat from pinnacle/draftkings
  - New `_get_player_prop_lines()` helper for single-player inference path
  - Each `RATE_FEATURES_*` list now includes its corresponding `prop_line_*`
  - COALESCE to 0 for missing lines (consistent with `line_spread`/`line_total` pattern)
  - Database index `idx_props_player_game` created on `(player_id, game_id)` for query performance
  - **Retrained** in `run_20260129_205540` — prop line features active and selected across all models.

- [ ] **A5. Residual modeling (Option B — binary classifier)**
  Build a separate model that directly predicts P(over | features, line) trained on historical
  over/under outcomes. Architecturally cleaner for decorrelation but a bigger lift than Option A.
  Evaluate after A4 results are in.

- [ ] **A6. Conditional rate modeling (minutes as rate feature)**
  Instead of modeling minutes and rates independently and combining via copula, pass the
  sampled minutes value as a feature into the rate model at inference time. The MC loop would
  sample minutes first, then condition rate predictions on sampled minutes. This directly models
  the dependency rather than approximating it via copula. Consider if copula-based combined
  calibration still shows drift after retraining.

---

## Track B: New Signal Sources (Parallel — High Impact)

These add information the market may price imperfectly, especially for non-star players
where bookmaker attention is lower.

- [x] **B1. Injury/lineup context features** *(IMPLEMENTED — 2026-01-29)*
  Historical injury data acquired via RapidAPI (2021-present, 88K+ rows). Player name-to-ID
  linking via 3-tier cascade (manual CSV → exact normalized → SequenceMatcher fuzzy, threshold 0.80).
  99.3% of injury records fully linked. Garbage API entries cleaned (142 rows deleted).

  **Features added to all rate models and minutes model (10 total):**
  - `team_out_count` — players listed Out on player's team
  - `team_out_min_sum` — total recent minutes of Out teammates
  - `team_out_pts_sum`, `team_out_reb_sum`, `team_out_ast_sum`, `team_out_usg_sum` — production of Out teammates
  - `opp_out_count`, `opp_out_min_sum` — opponent injury context
  - `player_is_questionable`, `player_is_probable` — player's own injury status (binary)

  Computed via SQL LATERAL JOINs in `feature_store.py`. Pre-game temporal integrity enforced
  (uses report_date <= game_date). Manual mappings for truncated API names (suffixes like "III", "Jr.").
  **Retrained** in `run_20260129_205540` — injury features active and selected by feature selection.

- [x] **B2. Rest days / back-to-back features** *(IMPLEMENTED — 2026-01-29)*
  Schedule density features pre-computed in `player_average_game_stats` via `calculate_b2_b3_b4_features()`.
  Added to `MINUTES_FEATURES` and all 4 `RATE_FEATURES_*` lists: `rest_days`, `is_back_to_back`, `games_in_last_7_days`.
  DB columns: `rest_days`, `games_last_7d`. `is_back_to_back` derived in SQL (`CASE WHEN rest_days = 1`).
  All 4 feature store query paths updated. Defaults: rest=3, b2b=0, games_7d=2.

- [x] **B3. Short-window + trend features** *(IMPLEMENTED — 2026-01-29)*
  L3 rolling averages and L5 std deviations pre-computed in `player_average_game_stats`.

  Features added (13 total across all feature lists):
  - `player_avg_{stat}_l3` (5 stats) — last 3 games rolling average
  - `player_{stat}_l3_l15_ratio` (4 stats: pts/reb/ast/fg3m) — momentum ratio (>1.0 = trending up)
  - `player_std_{stat}_l5` / `player_min_std_l5` (5 stats) — L5 standard deviation (consistency signal)

  DB columns: `avg_{stat}_l3` (5), `std_{stat}_l5` (5). Momentum ratios computed in SQL from L3/L15 averages.
  Shift(1) no-leakage pattern ensures features only use prior games.

- [x] **B4. Minutes stability features** *(IMPLEMENTED — 2026-01-29)*
  Minutes stability features pre-computed in `player_average_game_stats`.

  Features added to `MINUTES_FEATURES`:
  - `player_min_std_l5` — minutes variance (shared with B3 std computation)
  - `player_min_floor_l5` — minimum minutes in last 5 games
  - `player_games_started_l5` — games with 20+ minutes in last 5 (starter proxy)

  DB columns: `min_floor_l5`, `games_started_l5`. Starter threshold = 20 minutes.

---

## Track C: Calibration Refinement (Parallel — Lower Priority)

- [x] **C0. Gaussian copula for minutes-rate correlation** *(IMPLEMENTED — 2026-01-29)*
  Replaced the legacy post-hoc correlation adjustment (hardcoded bucket-based rate factors) with
  proper Gaussian copula sampling. This preserves both marginal distributions exactly while
  capturing the empirical rank dependency between minutes and per-minute rates.

  **Problem:** PTS (ρ=0.314) and AST (ρ=0.176) show significant minutes-rate correlation.
  Independent sampling + post-hoc multiplicative adjustment distorted the rate distribution
  and was the likely root cause of the AST Q10 combined calibration gap (+9.7%).

  **Implementation:**
  - `MonteCarloPredictor` accepts `copula_params: dict[stat → Spearman ρ]`
  - Training pipeline computes Spearman rank correlations and saves `copula_params.json` as artifact
  - `_predict_copula()`: shared z_minutes ~ N(0,1), per-stat z_rate = ρ·z_min + √(1-ρ²)·z_indep
  - Uniform transform via Φ(z), then inverse CDF mapping through each marginal
  - Both `predict()` and `predict_batch_for_date()` support copula path
  - `run_backtest.py` and `run_daily.py` auto-load copula params from model artifacts
  - Falls back to legacy adjustment when `copula_params.json` not present (backward compat)
  - Helper: `compute_copula_params_from_data()`, `load_copula_params()`
  - If copula still shows combined calibration drift, see A6 (conditional rate modeling)

- [x] **C1. Investigate Q10 over-coverage** *(PARTIALLY ADDRESSED — 2026-01-31)*
  Training showed THREES rate model Q0.10 at 35.2% coverage (worst case). Root cause: zero-inflated
  distribution — 35%+ of `threes_per_min` samples are exactly 0, which XGBoost's `quantileerror`
  cannot learn. Combined THREES Q0.10 showed +20.4% gap on holdout data. Combined AST Q0.10 showed
  +10.4% gap from discrete spike at 0 assists.

  **Fixes applied:**
  - Conformal recalibration in `quantile_trainer.py` — closes gaps > 3% via validation residual offset
  - Zero-snap in `monte_carlo.py` — values < 1e-3 snapped to 0 in inverse CDF
  - Dynamic stat inclusion in `train_pipeline.py` — combined calibration now evaluates all trained rate models

  **Status:** Code changes applied. Models need retraining to incorporate conformal offsets. Zero-snap
  and combined eval fixes will take effect on next retrain.

- [ ] **C2. Per-stat calibration breakdown**
  Run `analyze_calibration_drift.py` with the current model to get per-stat quantile coverage.
  This informs whether rate_factors or tail adjustments need stat-specific tuning.

- [x] **C3-C5. THREES Model Experiments** *(ARCHIVED — 2026-02-10)*
  Multiple approaches attempted for modeling THREES (3-pointers):
  - **C3:** Hurdle + quantile regression — failed (25.6% calibration gap)
  - **C4:** Hurdle + truncated NegBin — implemented but superseded
  - **C5:** Multiclass PMF — implemented but not deployed

  **All THREES work archived** to `archive/threes_model/` due to:
  - 50% of predictions had no odds available (poor market coverage)
  - Only 2 bets out of 78 in backtesting came from THREES
  - Development time not justified by minimal betting opportunities

  **Preserved for future:**
  - Scrapers still collect `player_threes` market data
  - Feature columns remain in `feature_store.py`
  - Archive contains all code for restoration if market coverage improves

---

## Track D: Previous Model Improvement Items (Deprioritized)

These were the original Track B items. Most are superseded by the probability recalibration
findings — fixing per-stat biases won't help if the fundamental problem is overconfident
probabilities and zero independent signal. Revisit after Tracks A and B are complete.

- [ ] **D1. Investigate PTS upward bias** *(Superseded by A2)*
  PTS rate_factors going up to 1.30 may inflate upper tail. However, the market decorrelation
  work (Track A) should be done first. If removing `line_total` and adding residual modeling
  changes the prediction landscape, the rate_factors may need re-derivation anyway.
  - Compare predicted PTS distributions vs actuals (mean, median, skew)
  - Test dampening PTS rate_factors (e.g., cap at 1.15 instead of 1.30)

- [ ] **D2. Diagnose AST** *(Low Priority)*
  Lost money in both directions. Either poorly calibrated, weak features, or market is too
  efficient on assists. Revisit after decorrelation work.

- [ ] **D3. Investigate REB under** *(Low Effort)*
  REB under was -1.46% under old Gaussian CDF. May not be meaningful under empirical CDF.
  Re-evaluate after decorrelation work.

- [ ] **D4. Explore adding THREES** *(Unknown Value)*
  Model supports `threes` but wasn't in recent backtests. Revisit after core model improvements.

- [ ] **D5. Hurdle-specific hyperparameter tuning** *(Future — After C3 Validation)*
  Current hyperparameter tuning tunes THREES as regular quantile regression on ALL data (including zeros), then applies those hyperparams to the hurdle model. This is suboptimal because:
  1. The classifier is a binary classification problem (not quantile regression)
  2. The positive rate models train on filtered data (positive samples only)

  **To implement:** Modify `hyperparameter_tuner.py` to:
  1. Tune the zero classifier separately with binary objective (logloss)
  2. Tune the positive quantile models on filtered positive-only data

  **Priority:** Low — validate C3 hurdle model works first with transferred hyperparams. Only pursue if calibration gaps remain after hurdle model retraining.

- [ ] **D6. Add steals/blocks models** *(Future — After C3 Validation)*
  Steals and blocks have good historical data (1.7M+ rows each, May 2023+). However:
  - Severely zero-inflated (many players get 0 per game)
  - Would require hurdle model architecture (like THREES)
  - Very noisy/random events — harder to predict than volume stats

  **Priority:** Low — validate THREES hurdle model works first, then apply same architecture.

---

## Track E: Go-Live Pipeline (Blocked — Needs Edge First)

These items are blocked until the BL parameter sweep demonstrates positive ROI on the
out-of-sample period. Do not pursue E4+ until sweep results are in.

- [x] **E1. Retrain models** — *(DONE — 2026-01-30)* Retrained with all bug fixes and new features. Artifact: `run_20260129_205540`. Needs another retrain to incorporate calibration fixes from session 8.
- [x] **E2. Run BL parameter sweep** — *(DONE — 2026-01-31)* Ran `run_sweep.py` on OOS period (2025-10-22 to 2026-01-29). 40 configs: tau × edge × kelly.
- [x] **E3. Analyze sweep results** — *(DONE — 2026-01-31)* **Key finding:** No-BL is profitable (+3% ROI, REB +7.9%). BL confidence function is structurally broken — kills all edges. See A3 for details and fix options.
- [x] **E4. Fix daily injury pipeline** *(DONE — 2026-02-04)* — `run_daily.py --scrape-injuries` now calls `rapidapi_injury_backfill.py` (for target date) followed by `link_injury_data.py`. Both feature store and daily runner use `rapidapi_injuries` with `player_id` linking.
- [x] **E5. Paper trade infrastructure** *(DONE — 2026-02-04)* — Convert stored `daily_predictions` into paper bets:
  - `paper_bets` and `paper_trading_daily_log` tables for storage
  - `PaperTrader` class with bet selection (edge threshold + Kelly sizing), placement (UPSERT), and resolution
  - CLI scripts: `place_bets.py` (with `--dry-run`) and `resolve_bets.py`
  - 20 unit tests in `tests/test_paper_trader.py`
- [x] **E6. Daily Pipeline Automation** — *(DONE — 2026-02-09)*
  Separated jobs by frequency for scheduling. Spec: `.session/specs/E6_daily_automation.md`

  **Scripts created:**
  - `src/orchestration/daily_stats_job.py` — Once daily: NBA results + full processing pipeline
  - `src/orchestration/lines_job.py` — Multiple times daily: Props + injuries + linking
  - `src/orchestration/inference_job.py` — Once daily: Generate predictions before games

  **Scheduling implemented:**
  - **Linux:** `cron/gameflow_crontab.txt` template for server deployment
  - **Windows:** Batch scripts in `scripts/` + Task Scheduler tasks (GameFlow-*)

  **Windows Task Scheduler (local deployment):**
  - 9:00 AM: `GameFlow-DailyStats` — Scrape previous night's games
  - 12:00 PM: `GameFlow-Lines-12PM` — First props scrape
  - 4:00 PM: `GameFlow-Lines-4PM` — Second props scrape
  - 6:00 PM: `GameFlow-Lines-6PM` — Final props scrape
  - 6:30 PM: `GameFlow-Inference` — Generate predictions

  **Phase 2 (future):**
  - [ ] Email notifications on success/failure
  - [ ] Health check endpoints
- [ ] **E7. Paper trade** — Run live for 2-4 weeks, validate predictions vs outcomes
- [ ] **E8. Go live — minimum flat stakes**
- [ ] **E9. Scale to Kelly sizing**

---

## Track F: Market Expansion (Future — After Demonstrated Edge)

New market data scraped (2.6M rows, 2026-01-31) but not yet modeled. Expand when core
pts/reb/ast shows profitability.

- [ ] **F1. Backfill new markets further** — Currently scraped for recent window only. Extend `player_prop_scraper.py` run with `--markets player_field_goals player_frees_made player_frees_attempts player_blocks_steals` + combos back to 2024-10-22.
- [ ] **F2. Add FG/FT/BLK+STL rate features** — New `RATE_FEATURES_FG`, `RATE_FEATURES_FT`, `RATE_FEATURES_BLK_STL` in feature store. Need new `actual_*` target columns in training data.
- [ ] **F3. Train expanded models** — Add `fg`, `ft_made`, `ft_attempts`, `blk_stl` as stat targets. Train + calibrate.
- [ ] **F4. Add combo market edges** — PRA, P+R, P+A, R+A are sums of individual predictions. Compute from existing MC samples without additional models.
- [ ] **F5. DD/TD markets** — Binary outcomes, need separate classifier (not quantile regression). `player_double_double`, `player_triple_double`.

---

## Track G: Dashboard (In Progress)

Next.js dashboard for viewing predictions and paper trading results. Full spec: `.session/specs/dashboard_implementation.md`

**Tech Stack:** Next.js 16, TypeScript, Supabase SSR, Tailwind CSS, Recharts
**Location:** `dashboard/` folder in repo
**Design:** Desktop-first, dark theme

- [x] **G1. Project setup** — *(DONE — 2026-02-09)* Next.js 16 with TypeScript, Tailwind, Supabase SSR client, email/password auth
- [ ] **G2. Database migration** — Add `feat_*` columns to `daily_predictions` for insight generation
- [ ] **G3. Update prediction storage** — Modify `prediction_store.py` and `daily_runner.py` to save feature values
- [x] **G4. Home page MVP** — *(DONE — 2026-02-09)* Prop cards grid with filtering (All/PTS/REB/AST/THREES), edge sorting, player name enrichment
- [x] **G5. Analysis modal** — *(PARTIAL — 2026-02-09)* Last 5 games chart + quantile summary created. Template-based insights pending (needs G2/G3).
- [ ] **G6. Hero section** — "Lock of the Day" with top pick by edge
- [x] **G7. Player headshots** — *(DONE — 2026-02-09)* NBA CDN integration with inline SVG fallback
- [x] **G8. Paper trading views** — *(DONE — 2026-02-10)* History page and Performance page created:
  - `/history` — Bet history with status filters (All/Won/Lost/Push), summary bar, bet cards
  - `/performance` — KPI cards, bankroll chart, stat breakdown table
  - Components: BetCard, BetList, HistoryFilters, HistorySummary, KPICard, BankrollChart, StatBreakdown
  - Auth callback route for email confirmation added
- [ ] **G9. Vercel deployment** — Production deployment with environment variables

---

## Track H: Discord Bot (Planned)

Interactive Discord bot for daily prediction alerts and command-based queries. Full development plan at `docs/discord_bot_development.md`.

**Prerequisites (Manual Setup Required):**
- Create Discord server with channels (`#predictions`, `#alerts`, `#performance`)
- Create Discord application and bot at https://discord.com/developers/applications
- Generate bot token and channel IDs
- Add to `.env`: `DISCORD_BOT_TOKEN`, `DISCORD_CHANNEL_PREDICTIONS`, `DISCORD_CHANNEL_ALERTS`

**Implementation Items:**
- [ ] **H1. Bot foundation** — Entry point (`run_bot.py`), Discord.py setup, slash command registration
- [ ] **H2. Prediction service** — Query `daily_predictions` for today's picks, player predictions, top edges
- [ ] **H3. `/picks` command** — Get today's top predictions (filterable by stat type and min edge)
- [ ] **H4. `/player` command** — Get predictions for a specific player (fuzzy match supported)
- [ ] **H5. `/bankroll` command** — Show paper trading balance from `paper_trading_daily_log`
- [ ] **H6. `/performance` command** — Show model stats (win rate, ROI, total bets) from `paper_bets`
- [ ] **H7. Automated alerts** — Send top picks to Discord after inference job completes
- [ ] **H8. Bot hosting** — Windows Task Scheduler or Windows service for continuous running

**Files to Create:**
| File | Purpose |
|------|---------|
| `src/discord_bot/__init__.py` | Package init |
| `src/discord_bot/run_bot.py` | Entry point |
| `src/discord_bot/bot.py` | Bot class and command registration |
| `src/discord_bot/commands/picks.py` | `/picks` command |
| `src/discord_bot/commands/player.py` | `/player` command |
| `src/discord_bot/commands/bankroll.py` | `/bankroll` command |
| `src/discord_bot/commands/performance.py` | `/performance` command |
| `src/discord_bot/services/predictions.py` | Prediction queries |
| `src/discord_bot/services/paper_trading.py` | Paper trading queries |
| `src/discord_bot/formatters/embeds.py` | Discord embed builders |
| `src/discord_bot/alerts.py` | Alert sending functions |

---

## Priority Matrix

| Item | Effort | Expected Value | Notes |
|------|--------|----------------|-------|
| ~~A1 (Market neutralization diagnostic)~~ | ~~Trivial~~ | ~~Critical~~ | **DONE** — R²=0.10, Brier 0.2705, overconfidence not correlation |
| ~~A3 (Black-Litterman blending)~~ | ~~Medium~~ | ~~Critical~~ | **DONE** — Implemented in `black_litterman.py`, 39 tests passing. Needs validation backtest. |
| ~~A2 (Remove line_total)~~ | ~~Low~~ | ~~High~~ | **DONE** — Removed from `RATE_FEATURES_PTS`. Retrained. |
| ~~B2 (Rest/B2B features)~~ | ~~Low~~ | ~~Medium-High~~ | **DONE** — `rest_days`, `is_back_to_back`, `games_in_last_7_days`. Retrained. |
| ~~B3 (L3 + trend features)~~ | ~~Low~~ | ~~Medium~~ | **DONE** — 13 features (L3 avg, momentum ratios, L5 std). Retrained. |
| ~~B1 (Injury features)~~ | ~~Medium-High~~ | ~~High~~ | **DONE** — 10 injury features via LATERAL JOIN. 99.3% linked. Retrained. |
| ~~A4 (Residual modeling — features)~~ | ~~Medium~~ | ~~High~~ | **DONE** — Prop line centering in all 4 query paths. Retrained. |
| ~~B4 (Minutes stability)~~ | ~~Low~~ | ~~Medium~~ | **DONE** — `min_std_l5`, `min_floor_l5`, `games_started_l5`. Retrained. |
| ~~C0 (Gaussian copula)~~ | ~~Medium~~ | ~~Medium-High~~ | **DONE** — Replaces hardcoded rate factors with proper copula sampling. Retrained. |
| ~~E1 (Retrain)~~ | ~~Low~~ | ~~Critical~~ | **DONE** — `run_20260129_205540`. Needs re-retrain with calibration fixes. |
| ~~E2 (BL Sweep)~~ | ~~Medium~~ | ~~Critical~~ | **DONE** — No-BL profitable (+3% ROI). BL kills all edges (confidence function flaw). |
| ~~E3 (Analyze sweep)~~ | ~~Low~~ | ~~Critical~~ | **DONE** — REB +7.9%, model finds genuine edges without BL. |
| ~~A3b (Fix BL confidence)~~ | ~~Low~~ | ~~High~~ | **DONE** — Linear ramp confidence. 42 tests passing. |
| ~~C3-C5 (THREES models)~~ | ~~Various~~ | ~~High~~ | **ARCHIVED** — All THREES work moved to `archive/threes_model/` due to poor market coverage (50% missing lines). |
| E1b (Retrain with calibration fixes) | Low | Medium | Conformal recalibration + zero-snap need retraining to take effect |
| ~~E4 (Daily injury pipeline)~~ | ~~Medium~~ | ~~Critical~~ | **DONE** — `--scrape-injuries` now uses RapidAPI + linker |
| ~~E5 (Paper trade infra)~~ | ~~Medium~~ | ~~High~~ | **DONE** — `PaperTrader` class, CLI scripts, 20 tests |
| E6 (Scheduling) | Low | High | cron/Task Scheduler automation |
| ~~C1 (Q10 investigation)~~ | ~~Low~~ | ~~Low-Medium~~ | **PARTIALLY DONE** — Root cause identified (zero-inflation), conformal recalibration applied |
| A5 (Residual modeling — classifier) | High | High | Only if A4 isn't sufficient |
| A6 (Conditional rate modeling) | Medium-High | Medium-High | Only if copula combined calibration still drifts |
| D1-D4 (Old model items) | Various | Low until recalibrated | Revisit after Track A |
| F1-F5 (Market expansion) | Various | Medium | After demonstrated edge on core markets |
| ~~G1 (Project setup)~~ | ~~Medium~~ | ~~High~~ | **DONE** — Next.js 16, TypeScript, Tailwind, Supabase SSR |
| ~~G4 (Home page MVP)~~ | ~~Medium~~ | ~~High~~ | **DONE** — Prop cards, filtering, edge sorting |
| ~~G7 (Player headshots)~~ | ~~Low~~ | ~~Medium~~ | **DONE** — NBA CDN with SVG fallback |
| ~~G8 (Paper trading views)~~ | ~~Medium~~ | ~~High~~ | **DONE** — History page, Performance page with charts |
| G5-G6, G9 (Dashboard) | Medium | Mid-High | In progress. Spec: `.session/specs/dashboard_implementation.md` |
| H1-H8 (Discord Bot) | Medium | High | Full spec at `docs/discord_bot_development.md`. Requires manual Discord setup first. |

---

## Key Findings Archive

### Market Neutralization Diagnostic (Closed — 2026-01-28)

Ran full diagnostic on `backtest_results/bt_20260128_145106/predictions.csv` (15,090 rows).

**Key results:**
- R² (model_prob ~ implied_prob) = 0.104 → model is NOT a market clone
- Per-stat: PTS R²=0.041, REB R²=0.187
- Model Brier score: 0.2705 (worse than naive 0.2500)
- Market Brier score: 0.2495 (well-calibrated)
- Model correlation with outcome: 0.046
- Market correlation with outcome: 0.079 (market is better predictor)
- Model residual correlation with outcome: 0.022 (essentially zero independent signal)
- Probability calibration: model says 84% over → actual 49.1% (catastrophic overconfidence)
- Model avg P(over): 0.604 (should be ~0.50 for balanced lines)

**Conclusion:** The original hypothesis ("model is correlated with the market") was wrong.
The model is overconfident, not correlated. Good quantile calibration does NOT imply good
probability calibration against prop lines. The MC distribution centers near the line (correct
behavior), but `(samples > line).mean()` amplifies small mean shifts into extreme probabilities.
Black-Litterman blending is the correct fix — anchor to the market's well-calibrated prior.

### Minutes Bimodality (Closed — 2026-01-28)

No bimodality detected. BC decreases with spread (0.419 close → 0.354 extreme blowout).
Model handles blowouts via `line_spread` feature. No mixture model needed.

### Empirical CDF Verification (Closed — 2026-01-28)

Implementation verified correct. `(samples > line).mean()` is textbook empirical P(over).
Sample routing via `(player_id, game_id, stat)` tuple keys confirmed correct. Edge
calculation, odds conversion, and bet resolution all verified. The Gaussian CDF was the
source of phantom edges, not a bug in the empirical replacement.
