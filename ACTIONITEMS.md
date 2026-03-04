# GameFlowData — Roadmap

## Session Summary (2026-03-04 — Session 65)

### What We Did

**Removed NCAAB Cron Jobs from Scheduler (1 file)**

Removed 3 NCAAB cron job registrations (`ncaab_daily_stats`, `ncaab_lines_noon`, `ncaab_lines_4pm`), their wrapper functions (`run_ncaab_daily_stats`, `run_ncaab_lines`), and JOB_NAMES entries from `scheduler.py`. These jobs were failing on Railway because migrations 009-011 haven't been applied, no historical data has been backfilled, and `cbbpy` isn't in `requirements.txt`.

**Modified:** `src/orchestration/scheduler.py`

NCAAB jobs should be re-added once items 15-18 from the action items are completed (migrations applied, cbbpy added, data backfilled, models trained).

---

## Session Summary (2026-03-03 — Session 64)

### What We Did

**User Bet Tracking — Cross-Device Sync + Auto-Resolution (9 files)**

Built full-stack user bet tracking: database migration, React hooks, dashboard integration, backend auto-resolution, and My Bets tabs on History and Performance pages.

**New Files (4):**
- `database/migrations/012_user_bet_tracking.sql` — `user_profiles` + `user_bets` tables with RLS policies, indexes, `updated_at` trigger
- `dashboard/src/lib/hooks/useUserBets.ts` — Optimistic UI toggle + async Supabase upsert/delete, per-date fetching
- `dashboard/src/lib/hooks/useUserPreferences.ts` — localStorage-first + debounced DB sync for bankroll/kelly/state
- `src/paper_trading/user_bet_resolver.py` — `UserBetResolver` class mirroring `PaperTrader.resolve_bets()` for `user_bets` table

**Modified (5):**
- `dashboard/src/app/(protected)/dashboard/page.tsx` — Replaced localStorage with `useUserBets` + `useUserPreferences` hooks
- `dashboard/src/components/analysis/AnalysisModal.tsx` — Replaced 6 localStorage reads with `useUserPreferences` hook
- `src/orchestration/daily_stats_job.py` — Added `resolve_pending_user_bets()` step after paper bet resolution
- `dashboard/src/app/(protected)/history/page.tsx` — Added "My Bets" / "Model History" tab toggle
- `dashboard/src/app/(protected)/performance/page.tsx` — Added "My Bets" tab alongside Props / DFS

**Migration applied to Supabase:** Both tables created with RLS enabled.

### Remaining Action Items

1. **Deploy to Railway** — push changes so new scheduler, fuzzy cache, parallel execution, and 5-min cadence are active
2. **Deploy dashboard to Vercel** — user bet tracking features need frontend deployment
3. **Monitor cross-device sync** — verify checkmark state syncs between phone and laptop
4. **Monitor user bet auto-resolution** — verify bets resolve correctly after daily_stats_job runs
5. **Verify partial index creation state** — `idx_props_dfs_commence` and `idx_props_sb_commence` may be in invalid state
6. **MLB linker remaining gaps (3.2%)** — 231K missing game_id, 500K missing team_id, 3.4K unmatched players
7. **Run MLB averages backfill** — `mlb_populate_averages --table all` now that linking is at 96.8%
8. **Build MLB Statcast rolling averages** — `mlb_player_average_statcast_batting/pitching` tables after Statcast backfill finishes
9. **Monitor DFS paper trading P&L** — review after 1 week (~28 entries) via `--dfs` audit flag
10. **Train pitcher K model on 2024 data** — run end-to-end training pipeline, validate calibration
11. **Build MLB daily runner** — inference pipeline mirroring NBA `daily_runner.py`
12. **Build MLB backtesting harness** — historical replay for pitcher K predictions
13. **Stripe integration** — subscribe page, customer portal, webhook
14. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts
15. **Apply NCAAB migrations 009-011 to Supabase** — NCAAB tables
16. **Fix ncaab_teams UNIQUE constraint** — add UNIQUE(team_name) to migration 009
17. **Add `cbbpy` to requirements.txt** — NCAAB dependency
18. **Backfill NCAAB historical data** — CBBpy box scores, Barttorvik snapshots, game lines
19. **Train NCAAB spread + total models** — 2022-2024 training, 2025 backtest validation
20. **Add stake calculation to useUserBets** — currently records bets without stake; could use Kelly from preferences
21. **Re-add NCAAB cron jobs to scheduler** — removed in Session 65 (failing on Railway); re-add after items 15-18 are complete

---

## Session Summary (2026-03-03 — Session 63)

### What We Did

**NCAAB Game-Level Prediction Pipeline — Full Stack Implementation (~20 new files)**

Built the complete NCAA Men's Basketball prediction pipeline from scratch: database migrations, scrapers, processing, feature store, XGBoost models, backtester, orchestration, and tests.

**New Files (20):**
- `database/migrations/009_ncaab_foundation.sql` — Core tables (teams, schedule, box scores, game lines)
- `database/migrations/010_ncaab_barttorvik.sql` — Barttorvik ratings snapshot table
- `database/migrations/011_ncaab_averages.sql` — Team rolling averages table
- `src/scrapers/ncaab/ncaab_game_lines_scraper.py` — Odds API (sport key `basketball_ncaab`)
- `src/scrapers/ncaab/ncaab_cbbpy_scraper.py` — ESPN box scores via CBBpy
- `src/scrapers/ncaab/ncaab_barttorvik_scraper.py` — Free efficiency ratings CSV download
- `src/processing/ncaab/ncaab_config.py` — Rolling windows, stat lists, team aliases
- `src/processing/ncaab/ncaab_linker.py` — Game-level linking (Odds API → schedule)
- `src/processing/ncaab/ncaab_populate_averages.py` — Shift(1) rolling team averages
- `src/processing/ncaab/ncaab_barttorvik_linker.py` — Link Barttorvik names to teams
- `src/models/ncaab_feature_store.py` — ~30 game-level matchup features (team differentials)
- `src/models/ncaab_trainer.py` — XGBoost spread + total quantile models
- `src/models/ncaab_backtest.py` — Time-travel backtester (ATS, O/U tracking)
- `src/orchestration/ncaab_daily_stats_job.py` — Daily pipeline (CBBpy → averages → Barttorvik)
- `src/orchestration/ncaab_lines_job.py` — Game lines scrape + linker
- 4 test files (34 tests, all passing)

**Modified:** `src/orchestration/scheduler.py` (3 new NCAAB cron jobs with `month="11-12,1-4"` guard)

**Key Design Decisions:**
- Game-level only (no player props for college sports — regulatory)
- Features are team differentials (home - away) for efficiency, box scores, pace, context
- Barttorvik for adjusted efficiency (free alternative to KenPom)
- Point-in-time Barttorvik via LATERAL JOIN (`snapshot_date < game_date`)
- Reuses existing `QuantileModelSuite` — XGBoost quantile regression (Q10-Q90)
- Moneyline derived from spread distribution (fit normal to Q25/Q50/Q75)

### Remaining Action Items

1. **Deploy to Railway** — push changes so new scheduler, fuzzy cache, parallel execution, and 5-min cadence are active
2. **Monitor first few 5-min cycles** — verify fuzzy cache creates on first run, hits on subsequent runs
3. **Apply migration 007 to Supabase** — `database/migrations/007_job_executions.sql` (job_executions table)
4. **Deploy dashboard changes to Vercel** — batched sportsbook fetch + allGamesStarted UX (from Session 57)
5. **Verify partial index creation state** — `idx_props_dfs_commence` and `idx_props_sb_commence` may be in invalid state
6. **MLB linker remaining gaps (3.2%)** — 231K missing game_id, 500K missing team_id, 3.4K unmatched players
7. **Run MLB averages backfill** — `mlb_populate_averages --table all` now that linking is at 96.8%
8. **Build MLB Statcast rolling averages** — `mlb_player_average_statcast_batting/pitching` tables after Statcast backfill finishes
9. **Monitor DFS paper trading P&L** — review after 1 week (~28 entries) via `--dfs` audit flag
10. **Train pitcher K model on 2024 data** — run end-to-end training pipeline, validate calibration, evaluate backtesting performance
11. **Build MLB daily runner** — inference pipeline mirroring NBA `daily_runner.py` for production predictions
12. **Build MLB backtesting harness** — historical replay for pitcher K predictions
13. **Stripe integration** — subscribe page, customer portal, webhook
14. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts
15. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic
16. **Apply NCAAB migrations 009-011 to Supabase** — create ncaab_teams, ncaab_game_schedule, ncaab_team_box_scores, ncaab_raw_game_lines, ncaab_barttorvik_ratings, ncaab_team_rolling_averages tables
17. **Fix ncaab_teams UNIQUE constraint** — migration 009 has UNIQUE on espn_team_id but CBBpy scraper uses ON CONFLICT (team_name). Need to add UNIQUE(team_name) to migration.
18. **Add `cbbpy` to requirements.txt** — new dependency for NCAAB ESPN data scraping
19. **Populate NCAAB team aliases** — `ODDS_API_TEAM_ALIASES` and `BARTTORVIK_TO_ESPN` dicts need expansion after first scrape
20. **Verify CBBpy import path** — `cbbpy.mens_scraper` needs runtime verification
21. **Backfill NCAAB historical data** — CBBpy box scores (2022-2025), Barttorvik snapshots, Odds API historical game lines
22. **Train NCAAB spread + total models** — 2022-2024 training, 2025 backtest validation
23. **Build NCAAB dashboard section** — if models show promise

---

## Session Summary (2026-03-03 — Session 62)

### What We Did

**MLB Model Architecture — Feature Store + Pitcher K Quantile Model + Monte Carlo Sampler**

Built the complete model layer for MLB pitcher strikeout predictions (6 new files, 0 existing files modified):

**New Files:**
- `src/models/mlb/__init__.py` — Package init
- `src/models/mlb/mlb_stat_config.py` — MLB stat types and edge thresholds (quantile/negbin/binary, 8-10%)
- `src/processing/mlb/mlb_matchup_features.py` — Opposing team batting tendencies (L10 K rate + batting avg) via window functions, bulk computation for training
- `src/models/mlb/mlb_feature_store.py` — 28-feature pitcher K feature store with LATERAL JOIN SQL, training/inference/backtest modes, time-travel safe
- `src/models/mlb/mlb_quantile_trainer.py` — `MLBPitcherKPipeline` wrapping `QuantileModelSuite`, direct SO prediction (no minutes decomposition)
- `src/models/mlb/mlb_monte_carlo.py` — `MLBMonteCarloPredictor` with inverse CDF sampling, integer rounding, batch prediction, reuses `PropPrediction`

**Key Design Decisions:**
- No minutes-rate decomposition (MLB stats predicted directly)
- No copula (single stat, no correlation to model)
- Reuses QuantileModelSuite, QuantileModelConfig, PropPrediction from NBA code
- Higher edge thresholds (8-10% vs NBA 5%) due to higher MLB prop juice
- 28 features from 6 data sources (pitching avgs, Statcast, FanGraphs, park factors, opposing team batting, prop/game lines)

### Remaining Action Items

1. **Deploy to Railway** — push changes so new scheduler, fuzzy cache, parallel execution, and 5-min cadence are active
2. **Monitor first few 5-min cycles** — verify fuzzy cache creates on first run, hits on subsequent runs
3. **Apply migration 007 to Supabase** — `database/migrations/007_job_executions.sql` (job_executions table)
4. **Deploy dashboard changes to Vercel** — batched sportsbook fetch + allGamesStarted UX (from Session 57)
5. **Verify partial index creation state** — `idx_props_dfs_commence` and `idx_props_sb_commence` may be in invalid state
6. **MLB linker remaining gaps (3.2%)** — 231K missing game_id, 500K missing team_id, 3.4K unmatched players
7. **Run MLB averages backfill** — `mlb_populate_averages --table all` now that linking is at 96.8%
8. **Build MLB Statcast rolling averages** — `mlb_player_average_statcast_batting/pitching` tables after Statcast backfill finishes
9. **Monitor DFS paper trading P&L** — review after 1 week (~28 entries) via `--dfs` audit flag
10. **~~MLB model architecture~~** ✅ — Feature store, quantile trainer, and MC sampler built (Session 62)
11. **Train pitcher K model on 2024 data** — run end-to-end training pipeline, validate calibration, evaluate backtesting performance
12. **Build MLB daily runner** — inference pipeline mirroring NBA `daily_runner.py` for production predictions
13. **Build MLB backtesting harness** — historical replay for pitcher K predictions
14. **Stripe integration** — subscribe page, customer portal, webhook
15. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts
16. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-03-03 — Session 61)

### What We Did

**MLB Linker Deep Debug — Team Alias Fix + Re-link Pass → 62% to 96.8% Linking**

**Root Cause: ARI/OAK Team Abbreviation Mismatch (`mlb_config.py`):**
- `MLB_TEAM_ALIASES` mapped "Arizona Diamondbacks" → "ARI" and "Oakland Athletics" → "OAK", but the `mlb_teams` DB table uses "AZ" and "ATH"
- This caused EVERY game involving Arizona or Oakland to fail game matching (~3M+ affected rows)
- Fixed: "ARI" → "AZ", "OAK" → "ATH", plus all abbreviation pass-through entries

**New Re-link Pass (Sub-stage 5 in `mlb_linker_local.py`):**
- Added `process_player_props_relink()` function that runs after initial 4 sub-stages
- Finds rows with game_id + player_id set but team_id NULL
- Categorizes: wrong_pid_fixable, correct_pid_not_in_game, game_no_boxscore, name_not_found
- Fixes wrong player_ids where the correct player IS in the game's boxscore
- Resolves team_id from nearby games (within 30 days) for remaining rows
- Added corresponding upload stages with chunked retry

**Results:**
- Fully linked: 14,075,000 → 21,974,799 (+7.9M rows)
- Linked %: 62.0% → 96.8%
- Missing game_id: 7,111,625 → 231,687 (-96.7%)
- Missing team_id: 1,520,376 → 500,319 (-67.1%)

### Remaining Action Items

1. **Deploy to Railway** — push changes so new scheduler, fuzzy cache, parallel execution, and 5-min cadence are active
2. **Monitor first few 5-min cycles** — verify fuzzy cache creates on first run, hits on subsequent runs
3. **Apply migration 007 to Supabase** — `database/migrations/007_job_executions.sql` (job_executions table)
4. **Deploy dashboard changes to Vercel** — batched sportsbook fetch + allGamesStarted UX (from Session 57)
5. **Verify partial index creation state** — `idx_props_dfs_commence` and `idx_props_sb_commence` may be in invalid state
6. **MLB linker remaining gaps (3.2%)** — 231K missing game_id (46 unmatched games not in schedule), 500K missing team_id (player not in any nearby boxscore), 3.4K unmatched players
7. **Run MLB averages backfill** — `mlb_populate_averages --table all` now that linking is at 96.8%
8. **Build MLB Statcast rolling averages** — `mlb_player_average_statcast_batting/pitching` tables after Statcast backfill finishes
9. **Monitor DFS paper trading P&L** — review after 1 week (~28 entries) via `--dfs` audit flag
10. **MLB model architecture** — build feature store and training pipeline once processing layer is complete
11. **Stripe integration** — subscribe page, customer portal, webhook
12. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts
13. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-03-03 — Session 60)

### What We Did

**Faster Lines Pipeline — Fuzzy Cache + Parallel Steps + 5-Minute Refresh Cadence**

**Persistent Fuzzy Cache (`nba_linker_local.py`):**
- Added `_load_fuzzy_cache()` / `_save_fuzzy_cache()` — file-based cache at `linker_data/_fuzzy_cache.json` maps `{normalized_name: player_id_or_null}` with player count for invalidation
- Added `_resolve_fuzzy_names()` — batch SequenceMatcher on unique unmatched names (0.80 threshold, +0.15 last name bonus)
- Refactored player matching in both `link_incremental()` and `process_local()` from per-row `match_player()` to 3-step batch: manual `.map()` → exact `.map(player_lookup)` → fuzzy cache lookup
- First run builds cache, subsequent runs see 95%+ cache hits (~15s → <1s for linker step)

**Parallel Steps (`lines_job.py`):**
- Added `--parallel` flag: props path (game lines → props → linker) and injury path (scraper → linker) run concurrently via threads
- New `run_step_group()` and `run_parallel_groups()` helpers
- Without `--parallel`: identical sequential behavior (backward compatible)
- Full mode runtime: ~90s → ~45-55s

**5-Minute Refresh (`scheduler.py`):**
- Props-only cron: `*/10` → `*/5` (every 5 minutes, ~156 runs/day)
- Edge refresh cron: updated to match 5-minute cadence
- Noon/4pm full runs now use `--parallel` via `run_lines_full_parallel()`

### Remaining Action Items

1. **Deploy to Railway** — push changes so new scheduler, fuzzy cache, parallel execution, and 5-min cadence are active
2. **Monitor first few 5-min cycles** — verify fuzzy cache creates on first run, hits on subsequent runs
3. **Apply migration 007 to Supabase** — `database/migrations/007_job_executions.sql` (job_executions table)
4. **Deploy dashboard changes to Vercel** — batched sportsbook fetch + allGamesStarted UX (from Session 57)
5. **Verify partial index creation state** — `idx_props_dfs_commence` and `idx_props_sb_commence` may be in invalid state
6. **MLB linker backfill in progress** — run `mlb_linker_local all` for full offline pipeline, then re-run averages backfill
7. **Build MLB Statcast rolling averages** — `mlb_player_average_statcast_batting/pitching` tables after Statcast backfill finishes
8. **Monitor DFS paper trading P&L** — review after 1 week (~28 entries) via `--dfs` audit flag
9. **MLB model architecture** — build feature store and training pipeline once processing layer is complete
10. **Stripe integration** — subscribe page, customer portal, webhook
11. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts
12. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-03-02 — Session 59)

### What We Did

**MLB Local Linker with Checkpoint/Resume — offline CSV-based linking pipeline mirroring the NBA local linker.**

**New File: `src/processing/mlb/mlb_linker_local.py`**
- Download/process/upload workflow: downloads 6 tables to `mlb_linker_data/` CSVs, matches IDs locally in pandas, uploads via chunked temp tables
- Checkpoint/resume system (`_checkpoint.json`) tracks per-stage and per-chunk progress — interrupted runs pick up where they left off
- 4 processing sub-stages: game_lines, props_games (±1 day fuzzy), props_players (exact + fuzzy), props_teams (boxscore cross-ref)
- Upload retry/backoff (20 retries, 60s cap, `engine.dispose()` on error) survives laptop sleep/wake
- CLI: download, process, upload, all, status, init, reset with `--force` and `--batch-delay` flags
- Reuses matching functions from `mlb_linker.py` — no code duplication
- Player name diagnostics: `unmatched_players.csv` with fuzzy suggestions, `player_mappings.csv` for manual overrides

### Remaining Action Items

1. **Apply migration 007 to Supabase** — `database/migrations/007_job_executions.sql` (job_executions table)
2. **Deploy to Railway** — push changes so new scheduler, retries, and dependency gates are active in production
3. **Deploy dashboard changes to Vercel** — batched sportsbook fetch + allGamesStarted UX (from Session 57)
4. **Verify partial index creation state** — `idx_props_dfs_commence` and `idx_props_sb_commence` may be in invalid state
5. **MLB linker backfill in progress** — run `mlb_linker_local all` for full offline pipeline, then re-run averages backfill
6. **Build MLB Statcast rolling averages** — `mlb_player_average_statcast_batting/pitching` tables after Statcast backfill finishes
7. **Monitor DFS paper trading P&L** — review after 1 week (~28 entries) via `--dfs` audit flag
8. **MLB model architecture** — build feature store and training pipeline once processing layer is complete
9. **Stripe integration** — subscribe page, customer portal, webhook
10. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts
11. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-03-02 — Session 58)

### What We Did

**Pipeline Resilience Overhaul — made the pipeline self-healing, dependency-aware, and transparent about failures.**

**Job Status Tracking:**
- Added `JOB_STATUS` in-memory dict to `scheduler.py` — tracks every job's status, end time, and duration after execution.
- Added `record_job_execution()` — persists execution history to `job_executions` Supabase table (migration 007).
- Provides both fast in-memory dependency checks and persistent history for debugging.

**Per-Step Retries with Backoff:**
- Extended `run_command()` in `daily_stats_job.py` with `max_retries` and `retry_delay` params.
- Critical steps (scrape, linker, rolling averages, opponent stats) get 2 retries with exponential backoff (15s, 30s).
- Step 6 (rolling averages) timeout increased from 10m→20m (most common timeout culprit).
- Step 7 (opponent stats) timeout increased to 15m. Non-critical steps reduced to 5m.
- Global scheduler timeout increased from 30m→45m.

**Dependency Gate:**
- `check_dependency()` in scheduler verifies upstream jobs succeeded within configurable time window.
- `run_inference()` checks daily stats succeeded in last 8 hours before running.
- If stale: passes `--stale-warning` flag, sends Discord alert, but still runs inference (stale data > no data).

**Automatic 9:30 AM Retry:**
- New `run_daily_stats_retry()` at 14:30 UTC checks if 9 AM run succeeded, re-runs if not.
- Gives the system a second chance before inference at 12:15 PM.

**Stale Data Transparency:**
- Inference staleness check improved: changed from `days_stale > 2` to `latest_game_date < yesterday`.
- `--stale-warning` flag triggers stale-data Discord alert after successful prediction generation.
- Edge refresh warns via Discord if MC samples are >6 hours old.

### Remaining Action Items

1. **Apply migration 007 to Supabase** — `database/migrations/007_job_executions.sql` (job_executions table)
2. **Deploy to Railway** — push changes so new scheduler, retries, and dependency gates are active in production
3. **Deploy dashboard changes to Vercel** — batched sportsbook fetch + allGamesStarted UX (from Session 57)
4. **Verify partial index creation state** — `idx_props_dfs_commence` and `idx_props_sb_commence` may be in invalid state
5. **MLB linker backfill in progress** — re-run averages backfill after linker completes
6. **Build MLB Statcast rolling averages** — `mlb_player_average_statcast_batting/pitching` tables after Statcast backfill finishes
7. **Monitor DFS paper trading P&L** — review after 1 week (~28 entries) via `--dfs` audit flag
8. **MLB model architecture** — build feature store and training pipeline once processing layer is complete
9. **Stripe integration** — subscribe page, customer portal, webhook
10. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts
11. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-03-01 — Session 57)

### What We Did

**Fixed production DFS dashboard (no data showing), sportsbook RPC timeouts, and edge refresh 30-minute timeout.**

**DFS Dashboard Fix:**
- Applied migration 004 to production (RPCs no longer depend on `daily_predictions`).
- Fixed `get_dfs_lines` and `get_sportsbook_lines` RPCs: replaced `commence_time::date` cast (prevents index usage) with range conditions.
- Created `idx_props_commence_time` index on `raw_player_props_combined(commence_time)`.

**Sportsbook RPC Timeout Fix:**
- Old `get_sportsbook_lines(date)` timed out scanning millions of accumulated snapshot rows (8s Supabase PostgREST limit).
- Created new `get_sportsbook_lines_by_games(text[])` RPC (migration 005): accepts game_id array, 24h snapshot_time cutoff, pure SQL function. Returns in 0.3s for 3 games.
- Updated DFS dashboard page to two-step fetch: load DFS lines first, extract game_ids, then batch sportsbook calls (3 games per batch, parallel).

**Edge Refresh 30-Minute Timeout Fix:**
- `fetch_fresh_lines()` had no `snapshot_time` cutoff — scanned ALL historical snapshots. During evening games (22:22-22:52 UTC), query degraded past 30-minute timeout, causing 3 consecutive skipped runs.
- Added `AND snapshot_time > now() - interval '24 hours'` cutoff.

**DFS Paper Trader Query Fix:**
- Both `_fetch_dfs_lines()` and `_fetch_sportsbook_lines()` used slow `commence_time::date` cast and had no snapshot_time cutoff.
- Updated to range conditions + 24-hour cutoff, matching the RPC fix pattern.

**MLB Rolling Averages pgBouncer Fix:**
- `mlb_populate_averages.py` crashed with "lost synchronization with server" when fetching the entire batting stats table through pgBouncer.
- Added `_get_seasons()` helper; fetch/concat data season-by-season.

**Dashboard UX:**
- Added `allGamesStarted` detection — shows helpful message with "+ Live" button when Pre-Game filter hides all games.

### Remaining Action Items

1. **Deploy dashboard changes to Vercel** — batched sportsbook fetch + allGamesStarted UX (code committed, not yet deployed)
2. **Verify partial index creation state** — `idx_props_dfs_commence` and `idx_props_sb_commence` were started but stopped mid-creation; may be in invalid state
3. **MLB linker backfill in progress** — re-run averages backfill after linker completes
4. **Build MLB Statcast rolling averages** — `mlb_player_average_statcast_batting/pitching` tables after Statcast backfill finishes
5. **Monitor DFS paper trading P&L** — review after 1 week (~28 entries) via `--dfs` audit flag
6. **MLB model architecture** — build feature store and training pipeline once processing layer is complete
7. **Stripe integration** — subscribe page, customer portal, webhook
8. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts
9. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-02-28 — Session 51)

### What We Did

**Rewrote scheduler to 10-minute cadence + fixed paper bet resolution + added live game filtering.**

**Scheduler Rewrite (`scheduler.py`):**
- Replaced 21 hardcoded job definitions (hourly 1-3 PM, half-hourly 4:30-6:30 PM) with 2 APScheduler cron jobs covering 11 AM – 11 PM ET every 10 minutes (~78 runs/day each).
- Added `silent_on_success` flag to `run_job()` — high-frequency jobs only send Discord alerts on failure.
- Total job definitions reduced from 21 → 7.

**Continuous Bet Resolution (`edge_refresh_job.py`):**
- Edge refresh step 7b now calls `resolve_all_pending(exclude_today=True)` before placing new bets.
- Bets from previous days are resolved every 10 minutes instead of only at the daily stats job.
- Backfilled 14 missed historical bets across 5 dates; P&L corrected from $1,841.68 → $2,231.14.

**Live Game Filter (`paper_trader.py`):**
- Added `_get_started_game_ids()` — checks `commence_time` from `raw_player_props_combined` to identify in-progress games.
- `select_bets()` now skips games where `commence_time < now()`, preventing false edges from mid-game line comparisons.
- Added `exclude_today` parameter to `resolve_all_pending()` to prevent same-day false resolution.

**Diagnostic Tool (`audit_and_resolve.py`):**
- New script with `--audit`, `--resolve`, `--backfill`, `--dry-run` flags for inspecting and fixing paper bet state.

### Remaining Action Items

1. **Run MLB backfills** — boxscores (2022-2025), then FanGraphs (all seasons), then Statcast (2024-2025), then props/lines
2. **Stripe integration** — subscribe page, customer portal, webhook
3. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts (or find alternative data source)
4. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-02-26 — Session 50)

### What We Did

**Implemented MLB Statcast & FanGraphs advanced stats scrapers — Phase 2 of MLB data pipeline.**

**New Scrapers:**
- **`mlb_statcast_scraper.py`:** Daily Statcast scraper using `pybaseball.statcast()`. Fetches pitch-level data, aggregates per (batter/pitcher, game_date) into contact quality metrics (exit velo, barrel%, xBA, xwOBA), batted ball types, spray direction, plate discipline, pitch velocity/spin, and pitch mix. Uses `ON CONFLICT DO UPDATE` for retroactive corrections.
- **`mlb_fangraphs_scraper.py`:** Season-level FanGraphs stats (wRC+, FIP, WAR, etc.) with FanGraphs→MLBAM player ID crosswalk resolution.
- **`mlb_statcast_backfill.py`:** Bulk backfill orchestrator with progress file resume and tqdm progress.

**Database:** 3 new tables (`mlb_player_game_statcast_batting`, `mlb_player_game_statcast_pitching`, `mlb_player_season_advanced`) with proper FK constraints and indexes.

**Verified:** Single-day Statcast test (303 batting + 125 pitching rows), FanGraphs 2024 season test (485 batting + 579 pitching rows).

### Remaining Action Items

1. **Run MLB backfills** — boxscores (2022-2025), then FanGraphs (all seasons), then Statcast (2024-2025), then props/lines
2. **Stripe integration** — subscribe page, customer portal, webhook
3. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts (or find alternative data source)
4. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-02-26 — Session 49)

### What We Did

**Added Market Edge and Combined Edge modes to DFS Edge Finder + fixed dashboard fallback games display.**

**DFS Edge Modes:**
- **Market Edge mode:** Compares DFS lines against devigged sportsbook consensus probabilities. Created `get_sportsbook_lines` RPC function + `idx_props_sportsbook_lookup` performance index. Added `americanToImpliedProb`, `devig`, `computeVig`, `formatBookmaker` utilities to `dfs-utils.ts`. New types: `EdgeMode`, `SportsbookLine`, `MarketEdgePlatformLine`, `CombinedEdgePlatformLine`.
- **Combined Edge mode:** Highest-conviction tier — only shows picks where BOTH model AND market agree on direction with positive edge. Displayed edge = `min(model_edge, market_edge)`.
- **3-way toggle:** Segmented control in DfsFilters (Model Edge / Market Edge / Combined). DfsTable renders mode-specific column layouts. KPI cards adapt per mode.
- **Refactored AnalysisModal:** Replaced local `oddsToImpliedProb` and `formatBookmaker` with shared imports from `dfs-utils.ts`.

**Dashboard Fallback Games Fix:**
- Fixed games not displaying when predictions haven't been generated yet. Root cause: `get_games_for_date` RPC depended on odds scraper data + had UTC/ET timezone mismatch + 33-second query on 3.1M row table (PostgREST timeout).
- Created `/api/games` Next.js API route that fetches from NBA CDN schedule (always available, no scraper dependency). Maps tri-codes to full team names. 1-hour revalidation cache.
- Updated dashboard page to use the new API route instead of Supabase RPC.

### Remaining Action Items

1. **Stripe integration** — subscribe page, customer portal, webhook
2. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts (or find alternative data source)
3. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-02-25 — Session 48)

### What We Did

**Implemented DFS Edge Finder page — surfaces mispriced DFS props (PrizePicks, Underdog, Pick6, Betr) vs the model's true probabilities.**

**Changes:**
- **Scraper update:** Added `us_dfs` region to Odds API requests, pulling DFS platform lines into `raw_player_props_combined` alongside sportsbook lines.
- **Supabase RPC:** Created `get_dfs_lines(target_date)` function to efficiently query latest DFS lines per bookmaker/player/stat. Added partial index `idx_props_bookmaker_dfs` for performance on 26M+ row table.
- **New types/utils:** `types/dfs.ts` (DFS types, slip type break-even thresholds, platform names), `lib/dfs-utils.ts` (extracted quantile interpolation functions shared with AnalysisModal).
- **DFS page (`/dfs`):** Platform filter tabs, slip type selector (UD 3/5-Pick, PP 5/6-Flex), stat filter, +EV toggle, KPI summary cards, sortable comparison table. Joins predictions with DFS lines client-side and re-estimates model probability at each DFS-specific line via quantile interpolation.
- **Navbar:** Added "DFS" link between Props and History.
- **Refactored AnalysisModal:** Replaced inline `estimateUnderProb` with shared import from `dfs-utils.ts`.

### Remaining Action Items

1. **Stripe integration** — subscribe page, customer portal, webhook
2. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts (or find alternative data source)
3. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-02-24 — Session 46)

### What We Did

**Fixed 5-day Railway pipeline outage (Feb 19-24) — two independent bugs, recovery of missed predictions + paper bets, and pipeline resilience hardening.**

**Bug fixes:**
- `prediction_store.py` — Fixed `np.isfinite()` TypeError on mixed `None`/`float` columns (`bl_confidence` etc.) by adding `pd.to_numeric(errors="coerce")` before the `isfinite` check. Also fixed `NaT` timestamps being passed as string `'NaT'` to PostgreSQL.
- `daily_stats_job.py` — Removed `play_type_scraper.py` (Step 8) that was causing 30-minute timeout because `stats.nba.com` blocks datacenter IPs. Added critical/non-critical step resilience so non-critical failures don't kill the whole pipeline.
- `scheduler.py` — Fixed Discord step counter showing "1/7" by switching from `re.search()` (first match) to `re.findall()[-1]` (last match).

**Paper trading loop closure:**
- `inference_job.py` — Now automatically places paper bets after storing predictions (via `PaperTrader.select_bets()` + `place_bets()`). Added `--skip-bets` flag.
- `paper_trader.py` — Added DNP/0-minute player void logic in `resolve_bets()`. Players with `did_not_play=True` or `min=0` get bets voided (status=`cancelled`, pnl=0), matching sportsbook behavior.

**Recovery:**
- Ran inference for 5 missed dates (Feb 20-24), backfilled paper bets for Feb 20-23
- Results: 18W-1L-1C (1 voided DNP), +$1,035 on $1,424 staked
- Comprehensive audit confirmed data integrity — no future sight, all actuals match game stats

### Remaining Action Items

1. **Stripe integration** — subscribe page, customer portal, webhook
2. **Re-enable play type scraper** when `stats.nba.com` datacenter ban lifts (or find alternative data source)
3. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-02-19 — Session 45)

### What We Did

**Implemented frequent line scraping + edge refresh pipeline.** Lines now scraped hourly/half-hourly using live API (not historical snapshots). New lightweight `edge_refresh_job.py` recalculates edges from stored MC samples + fresh lines without re-running inference. Full schedule expanded from 5 jobs to 21 jobs.

**Key changes:**
- `daily_player_props_scraper.py` — Added `--target-table` arg so live scraping can write to `raw_player_props_combined`
- `lines_job.py` — Added `--live` (uses live API) and `--props-only` (skips game lines/injuries) flags
- `game_lines_scraper.py` + `live_odds_scraper.py` — Region coverage expanded from `us` to `us,us2,us_ex`
- `prediction_store.py` — Added `get_all_samples_for_date()` bulk sample retrieval method
- `edge_refresh_job.py` — **New file.** Self-contained job that loads stored predictions + MC samples, fetches fresh lines, recalculates edges + BL recommendations, upserts back to DB
- `scheduler.py` — Updated `run_job()` to accept `extra_args`, new schedule: 2 full inference windows (12:15 PM, 4:15 PM) + hourly/half-hourly edge refreshes (1-3 PM hourly, 4:30-6:30 PM every 30 min)

### Remaining Action Items

1. **Stripe integration** — subscribe page, customer portal, webhook
2. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-02-19 — Session 44)

### What We Did

**Implemented NBA Play Types feature — Monster.bet-style team play type analysis on the Data Vault page.**

Added a new "Play Types" tab to the existing Data Vault (`/stats`) page showing Synergy play type data for all 30 NBA teams. Features:
- **11 play types:** Isolation, Transition, PnR Ball Handler, PnR Roll Man, Post Up, Spot Up, Handoff, Cut, Off Screen, Off Rebound, Misc
- **Offense/Defense toggle:** View offensive play type usage or defensive play type matchup data
- **Frequency sub-tab:** Shows what % of possessions each team uses for each play type (POSS_PCT)
- **Efficiency sub-tab:** Shows points per possession (PPP) for each play type
- **Full heatmap integration:** Percentile-based blue gradient coloring, sortable columns

**Backend:**
- Database table `team_play_types` (660 rows) with public read RLS — already existed from prior work
- Python scraper `play_type_scraper.py` using NBA Synergy API — already existed from prior work
- Added scraper as Step 8 in `daily_stats_job.py` pipeline

**Dashboard (7 files):**
- New types: `PlayTypeCategory`, `PlayTypeGrouping` added to `stats.ts`
- 22 new column definitions (11 frequency + 11 efficiency) in `columns.ts`
- New `pivotPlayTypes.ts` utility — transforms long-format DB rows to wide-format team rows
- New `OffDefToggle.tsx` component matching existing WindowToggle pattern
- Updated `StatTabs.tsx` with Play Types tab
- Full integration in `stats/page.tsx` — state, data fetch, pivot memo, controls, sorting

### Remaining Action Items

1. **Stripe integration** — subscribe page, customer portal, webhook
2. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-02-19 — Session 43)

### What We Did

**Data Vault bug fixes + comprehensive pipeline audit. Fixed 12 issues across dashboard, processing scripts, backtesting, and database views.**

This session had two phases:

**Phase 1 — Data Vault fixes (6 issues):**
- ISS-029: `games_szn` in incremental averages showed max 19 instead of actual season count (LeBron had 36 games). Fixed by querying real season game count per player.
- ISS-030: TOV% displayed as 1029% — added `rawPct1` format type that skips the `*100` multiplication.
- ISS-033: Opponent-allowed rolling windows used `.sum()` instead of `.mean()` — totals columns showed cumulative sums, not per-game averages. Fixed in both full and incremental backfill scripts.
- ISS-034: Added hover tooltips to all stat column headers in Data Vault.
- ISS-035: Added heatmap color legend component above the table.
- ISS-036: Saved 3 Supabase view definitions to `sql/views/` for version control.

**Phase 2 — Deep pipeline audit + fixes (6 issues):**
- ISS-038: Backtesting odds query used date-level filter that could theoretically include post-inference lines. Changed to timestamp-level cutoff matching 6:30 PM ET production schedule. Functionally equivalent given scrape times (12/4/6 PM ET).
- ISS-039: Added data freshness check to inference_job.py — warns if rolling averages are >2 days stale.
- ISS-041: Converted row-by-row upserts to batch execution in incremental averages script (~30x faster for large batches).
- ISS-042: Added `game_id DESC` deterministic tiebreaker to all 3 DISTINCT ON views.
- ISS-043: Removed `created_at = NOW()` overwrite from opponent-allowed UPSERT statements.
- ISS-040: Skipped (combined calibration offsets in run_daily/run_sweep — not used in production).

**Audit methodology:** 4 parallel Explore agents scanned all processing, model, backtesting, and scraper code. 5 most critical findings validated with dedicated agents — caught 3 false positives before filing issues.

### Remaining Action Items

1. **Stripe integration** — subscribe page, customer portal, webhook
2. **13 open issues remain in ISSUES.md** — mostly low priority/cosmetic

---

## Session Summary (2026-02-19 — Session 42)

### What We Did

**Built combined conformal recalibration infrastructure, tested via A/B backtest, concluded offsets hurt betting performance. AST Q10 gap confirmed structural.**

**Code changes (4 files):**
- `src/models/monte_carlo.py` — Added `combined_calibration_offsets` param, `_apply_combined_calibration()` sample warping method (piecewise-linear interpolation through quantile anchor points), `load_combined_calibration_offsets()` helper. Applied in all 3 prediction paths (legacy, copula, batch).
- `src/models/train_pipeline.py` — Modified `_evaluate_combined_calibration()` to compute per-stat per-quantile conformal offsets (`residuals = actuals - predicted_q_values`, `offset = np.quantile(residuals, q)`). Saves `combined_calibration_offsets.json` artifact. Added `--calibrate-only` CLI mode that loads an existing model, computes offsets, and saves without retraining.
- `src/orchestration/inference_job.py` — Loads offsets alongside copula params, passes to MonteCarloPredictor.
- `src/backtesting/run_backtest.py` — Same integration pattern.

**Backtest A/B comparison** (Jan 15 – Feb 14, 2026, offsets computed on cal data through Jan 14):

| Metric | WITH offsets | WITHOUT offsets |
|--------|-------------|-----------------|
| Total bets | 455 | 409 |
| ROI | 6.01% | **7.44%** |
| Return on capital | 82.2% | **88.2%** |
| Sharpe | 0.742 | **0.891** |
| Max drawdown | 29.2% | **26.5%** |
| Calibration gap | **0.019** | 0.032 |
| PTS ROI | 9.0% | **13.7%** |
| REB ROI | -0.37% | **0.41%** |
| AST ROI | **10.1%** | 7.0% |

**Findings:**
1. **Offsets improved calibration metrics but degraded betting performance.** Better calibration numbers ≠ better edges.
2. **PTS/REB offsets were harmful.** The largest offsets (PTS Q50 +0.83, REB Q25 +0.30) shifted predictions away from where they were already performing well.
3. **AST Q10 offset was near-zero (-0.001)** — conformal recalibration can't fix zero-inflated distributions where the quantile is already at the floor (can't predict Q10 < 0 for a non-negative stat).
4. **AST Q10 gap is structural.** ~17-18% of player-games result in 0 assists, setting a floor on Q10 coverage. No amount of post-hoc adjustment can push coverage below the natural zero-rate.
5. **Differences between runs are NOT MC variance** — both use `random_state=42`. All differences are deterministic consequences of the offsets changing predicted distributions.

**Decision:** No offsets deployed to production. Code infrastructure retained (backward-compatible no-op when offsets file is absent). Offsets file removed from production artifacts.

### Closing the AST Q10 Investigation

This session concludes the multi-session AST Q10 calibration investigation (Sessions 40–42). Approaches tried:

| Approach | Result |
|----------|--------|
| Surgical retrain (no tuning) | Q10 +10.25% → +8.10%, Q50 regressed -0.30% → -3.50% |
| Surgical retrain (per-quantile Optuna) | Q10 → +7.60%, Q50 regressed further to -4.45% |
| Feature reselection | Identical features selected, no effect |
| Combined conformal recalibration | Better calibration numbers, worse betting ROI |

**Root cause:** The AST Q10 combined gap is the natural zero-rate of assists (~17-18%). For any model predicting Q10 ≥ 0, coverage must be ≥ P(actual = 0) ≈ 18%. This is a property of the data, not a model deficiency. Individual AST model calibration is excellent (all quantiles within 2%).

**Impact on betting:** Minimal. The Q10 gap affects the extreme lower tail. Bets are placed around the median where AST combined calibration is good (Q50 gap < 3%).

### Next Steps

1. **Stripe integration** — No longer deferred by calibration work
2. **Full retrain with extended calibration window** — When needed for seasonal drift, not for calibration fixes
3. **Monitor live betting performance** — The model is performing well as-is

---

## Session Summary (2026-02-19 — Session 41)

### What We Did

**Fixed CI pipeline — module-level `sys.exit()`/`raise ValueError` in 5 source files crashed pytest collection when env vars are missing.**

Six files fixed to use lazy initialization:
- `src/db/client.py` — Engine creation deferred; `get_engine()` raises at call time instead of import time
- `src/scrapers/daily_player_props_scraper.py` — Removed module-level `sys.exit`, moved to `if __name__ == "__main__"`
- `src/scrapers/daily_game_lines_scraper.py` — Same pattern
- `src/scrapers/game_lines_scraper.py` — Removed module-level `raise ValueError`, moved to `if __name__ == "__main__"`
- `src/scrapers/live_odds_scraper.py` — Same pattern
- `tests/test_db_client.py` — Updated test to check `get_engine()` raises instead of import crash

All 608 tests pass. Ruff clean.

### Next Steps

1. ~~**Investigate AST zero-inflation**~~ **Closed** — See Session 42. Structural, not fixable, minimal betting impact.
2. ~~**Full retrain with extended calibration window**~~ — PTS/REB combined Q25 drift not hurting betting performance per A/B backtest
3. **Stripe integration** — No longer deferred

---

## Session Summary (2026-02-19 — Session 40)

### What We Did

**Evaluated two AST-only surgical retrains using `run_partial()` with per-quantile hyperparameter tuning. Documented findings; no model promoted.**

Two runs were evaluated against the production model (`run_20260210_095220`):

| Run | ID | Features | Tuning |
|-----|----|----------|--------|
| Run 1 | `run_20260218_175622` | Inherited from base | None (base hyperparams) |
| Run 2 | `run_20260218_180752` | Reselected via Optuna | Per-quantile Optuna tuning |

**Code change:** Added `_resolve_hyperparams_partial()` to `train_pipeline.py` (+105 lines), enabling hyperparameter tuning for individual surgical retrains via `run_partial()`. Priority chain: explicit file > fresh Optuna tuning > base model's `best_hyperparams.json` > XGBoost defaults.

**Individual AST calibration** — All three models (production + both retrains) show good individual calibration, every quantile within 2%:

| Quantile | Production | Run 1 (no tune) | Run 2 (tuned) |
|----------|------------|------------------|---------------|
| Q10 | -0.59% | +1.44% | +1.44% |
| Q25 | +0.52% | +0.38% | +0.39% |
| Q50 | -0.96% | -1.42% | -1.40% |
| Q75 | -1.26% | -1.83% | -1.82% |
| Q90 | -0.70% | -1.64% | -1.64% |

**Combined AST calibration (minutes x rate)** — Q10 gap improved but still exceeds 5% tolerance:

| Quantile | Production | Run 1 (no tune) | Run 2 (tuned) |
|----------|------------|------------------|---------------|
| Q10 | **+10.25%** | **+8.10%** | **+7.60%** |
| Q25 | -1.15% | +0.05% | -1.15% |
| Q50 | -0.30% | -3.50% | -4.45% |
| Q75 | +1.60% | -0.85% | -1.80% |
| Q90 | +1.40% | +0.40% | +0.20% |

**Other stats (frozen, same across all runs) also show combined Q25 drift on the new calibration window:**
- PTS Q25 combined: -3.4% (prod) → -4.6% (Run 1) → -5.2% (Run 2)
- REB Q25 combined: -4.9% (prod) → -6.0% (Run 1) → -6.75% (Run 2)

**Decision:** No model promoted. The AST Q10 combined gap (zero-inflation issue) needs deeper investigation beyond surgical retrains. PTS and REB combined Q25 gaps also warrant attention on the next full retrain.

### Next Steps

1. **Investigate AST zero-inflation** — The combined Q10 gap is structural (many zero-assist games even at high minutes). Consider truncated/zero-inflated mixture models or per-quantile conformal recalibration improvements
2. **Full retrain with extended calibration window** — PTS and REB combined Q25 drift may be a calibration window artifact
3. **Stripe integration** — Deferred; model calibration takes priority

---

## Session Summary (2026-02-18 — Session 39)

### What We Did

**Built Data Vault page (`/stats`) — dense heatmap stat table for exploring player, team, and defensive stats.**

A Monster.bet-style data table that surfaces pre-computed rolling averages already in the database. No new pipeline work needed — all data comes from existing `player_average_game_stats`, `player_average_advanced_stats`, `team_average_game_stats`, and `team_allowed_by_position` tables.

**Database:**
- Created 3 Supabase views via migration: `player_stats_latest` (529 rows), `team_stats_latest` (30 rows), `defense_by_position_latest` (90 rows)
- Views use `DISTINCT ON` for efficient "latest row per entity" queries with JOINs to lookup tables

**Dashboard (8 new files):**
- `types/stats.ts` — TypeScript types (ColumnDef, StatRow, SortState, WindowSuffix, etc.)
- `lib/stats/columns.ts` — Column definitions for all categories with `{window}` template pattern
- `components/stats/StatTabs.tsx` — Players / Teams / Defense vs Position tab bar
- `components/stats/CategoryTabs.tsx` — Generic sub-category pill tabs
- `components/stats/WindowToggle.tsx` — L5 / L15 / SZN toggle
- `components/stats/PositionFilter.tsx` — All / Guards / Wings / Bigs position filter
- `components/stats/HeatmapTable.tsx` — Core table with sorting, sticky columns, 5-step percentile heatmap coloring
- `app/(protected)/stats/page.tsx` — Main Data Vault page wiring all components

**Modified:** Navbar.tsx — Added "Data Vault" link

**Key features:**
- 5-step blue heatmap gradient based on percentile rank (with `invertHeatmap` for negative stats like TOV, DRtg)
- All 3 data sources fetched in parallel on mount; filtering/sorting client-side
- Player tab: Box Score / Shooting / Advanced / Consistency categories + search + team dropdown + min GP + position filter
- Team tab: Offense / Defense / Overall categories
- Defense tab: Totals / Per 100 Poss categories with position selector
- Sticky name column + optional sticky position/team columns + sticky header
- Window toggle hidden on Consistency tab (all windowless columns)

**Tests:** 608 passed, 0 failures. Build succeeds.

### Next Step

1. ~~**Monitor Railway jobs** — Continuing from Session 38~~ **Done** — Jobs confirmed stable
2. **Stripe integration** — Deferred; lower priority than model calibration work

---

## Session Summary (2026-02-18 — Session 38)

### What We Did

**Fixed Railway deployment pipeline — jobs now run successfully on Railway.**

Three root causes found and fixed:
1. **Nix immutable filesystem:** `ensurepip` tried to write to read-only `/nix/store`. Fixed by using a Python venv with `--system-site-packages` instead.
2. **Hardcoded `python` in subprocess calls:** Job scripts (`lines_job.py`, `daily_stats_job.py`, `run_daily.py`) used bare `python` which resolved to system Nix Python (no packages). Fixed by replacing with `sys.executable` so subprocesses use the venv Python.
3. **Missing shared libraries at runtime:** numpy/scipy/xgboost C extensions need `libz.so.1` and `libstdc++.so.6`, but Nix garbage collector deleted them. Fixed by adding `zlib` and `stdenv.cc.cc.lib` to nixPkgs and setting `LD_LIBRARY_PATH=/root/.nix-profile/lib` in nixpacks.toml.

Also removed the temporary one-shot test job from `scheduler.py` after confirming all jobs pass.

**Files modified:**
- `nixpacks.toml` — Venv-based install, LD_LIBRARY_PATH, zlib + stdenv.cc.cc.lib
- `railway.toml` — Updated start command to use venv Python
- `src/orchestration/scheduler.py` — Removed temp test job
- `src/orchestration/lines_job.py` — `sys.executable` for all subprocess calls
- `src/orchestration/daily_stats_job.py` — `sys.executable` for all subprocess calls
- `src/orchestration/run_daily.py` — `sys.executable` for all subprocess calls

**Tests:** 608 passed, 0 failures.

### Next Step

1. **Monitor scheduled jobs** — Verify daily_stats (9 AM ET), lines (12/4/6 PM ET), and inference (6:30 PM ET) all run successfully over the next few days
2. **Fix watchPatterns** — Railway dashboard shows empty `watchPatterns: []` which disables auto-deploy on git push. Clear the field in Railway dashboard to restore auto-deploy.

---

## Session Summary (2026-02-18 — Session 37)

### What We Did

**Built social media pick image generator (`src/social/`).** CLI tool that auto-generates professional dark-themed images from daily predictions for Instagram/TikTok/Discord marketing. Three card types: daily slate (top 3-5 picks), individual player feature card, and results recap with hit/miss indicators. Uses Pillow, matches dashboard color scheme, shows confidence tiers (not exact percentages) to keep premium data behind the dashboard.

**New files:**
- `src/social/theme.py` — Color palette, font helpers, edge tiers, star rating formula, drawing utilities
- `src/social/data_provider.py` — 4 sync DB query functions (top picks, resolved bets, daily summary, performance stats)
- `src/social/card_renderer.py` — HeadshotCache + PickCardRenderer + SlateCardRenderer + ResultsCardRenderer
- `src/social/generate_images.py` — CLI entry point with argparse (--date, --type, --format, --individual, --dry-run)
- `tests/test_card_renderer.py` — 33 unit tests (theme utils, renderers, headshot cache)
- `assets/fonts/Montserrat-*.ttf` — 3 font weights from Google Fonts (OFL license)

**Modified files:**
- `requirements.txt` — Added `Pillow>=10.0.0`
- `.gitignore` — Added `output/social/`

**Tests:** 608 passed, 0 failures. New module coverage: theme.py 97%, card_renderer.py 96%.

### Next Step

1. **Run live test** — `python src/social/generate_images.py --date 2026-02-18 --type both` against production DB
2. **Visual review** — Check generated PNGs for layout quality
3. **Integrate into daily pipeline** — Add image generation step after inference job
4. **Post to social media** — Start daily Instagram/TikTok posting cadence
5. **Discord auto-post** — Wire image output to Discord #picks channel

---

## Session Summary (2026-02-18 — Session 35)

### What We Did

**Pivoted from paid subscription to free Discord funnel.** Removed paywall, opened RLS to all authenticated users, added public picks page, and replaced all paid messaging with free-beta + Discord CTAs.

**Database changes (2 Supabase migrations):**
- Dropped subscriber-only RLS policies on 4 prediction tables, replaced with `authenticated USING (true)`
- Created `get_public_picks(pick_limit)` RPC for anon/auth access to top recommended picks

**Dashboard changes:**
- Middleware: removed subscription check block, added `/picks` to public routes
- New `/picks` page: 3 real pick cards from RPC + 6 blurred teaser cards with sign-up/Discord CTAs
- Landing page: replaced "Simple Pricing" + PricingCard with "Free During Beta" + Discord CTA
- Pricing page: $0/mo "Beta Access" card with feature checklist
- Subscribe page: replaced with `redirect('/dashboard')` (catches stale bookmarks)
- Account page: removed subscription info, added "Free Beta" badge + Community/Discord card
- Hero section: "View Pricing" → "Join Discord" (external link)
- Public navbar: "Pricing" → "Picks" + "Discord" links, "Sign Up" → "Sign Up Free"
- Footer: added Discord link
- Terms/Privacy: updated subscription/Stripe references to "free during beta" language
- Created shared `constants.ts` with `DISCORD_URL` and `TEAM_ABBREV` map
- Dashboard page imports `TEAM_ABBREV` from constants (no duplication)

**Diagnostics:**
- Built `src/diagnostics/calibration_per_stat.py` — per-stat (PTS/REB/AST) calibration diagnostic with quantile coverage (Q10–Q90), bias, interval sharpness, Brier score, ECE, reliability curve data, and auto-diagnosis flags. Supports backtest CSV (`--csv`) or production DB (`--db`) input with JSON export (`--output`). Completes C2.

**What stays intact for future Stripe:**
- `user_subscriptions` table, `is_subscribed()` function, `subscription.ts` types/utils, `PricingCard.tsx` (dormant)

**Build:** `npm run build` passes cleanly

### Next Step

1. **Update Discord invite URL** — Replace placeholder `https://discord.gg/gameflow` in `constants.ts` with real invite
2. **Deploy to Vercel** — Push changes and verify all pages
3. **Create Discord server** — Set up channels for picks, discussion, alerts
4. **Social media strategy** — Start posting picks from `/picks` page on Twitter/X
5. **Flip Stripe on later** — Re-enable paid subscription when ~200 Discord members reached

---

## Session Summary (2026-02-15 — Session 34)

### What We Did

**Added Discord job status alerts and daily P&L summary notifications.**

**Job status alerts (all scheduled jobs):**
- Scheduler now sends Discord notifications after every job completes (daily_stats, lines, inference)
- Success alerts show job name, duration, and extracted metrics (when available)
- Failure alerts include error details for debugging
- Alerts go to `#alerts` channel via REST API (no bot process required)
- Non-fatal — alert failures logged but don't affect job execution

**Daily P&L summary (after bet resolution):**
- After `daily_stats_job.py` resolves pending bets, sends P&L summary to `#performance` channel
- Shows win/loss/push record, daily P&L, cumulative P&L, and current bankroll
- Uses green/red embed colors based on daily profit/loss

**Channel organization:**
- `#predictions` — Top picks after inference job (existing)
- `#alerts` — Job status notifications (new)
- `#performance` — Daily P&L summaries (new)

**Files modified:**
- `src/discord_bot/alerts.py` — Added `send_job_alert()`, `send_pnl_summary()` and sync wrappers (~200 lines added)
- `src/orchestration/scheduler.py` — Added job alert integration after each subprocess (~50 lines added)
- `src/orchestration/daily_stats_job.py` — Added P&L summary after bet resolution (~30 lines added)
- `requirements.txt` — Added `aiohttp>=3.9.0` for async HTTP
- `ARCHITECTURE.md` — Updated Discord Bot and Orchestration sections

**Tests:** 575 passed, 0 failures

### Next Step

1. **Verify alerts on Railway** — Next job run should trigger Discord notification
2. **Check environment variables** — Ensure `DISCORD_CHANNEL_ALERTS` and `DISCORD_CHANNEL_PERFORMANCE` are set on Railway
3. **Continue paid subscription plan** — Track I documented in `docs/paid_subscription_plan.md`

---

## Session Summary (2026-02-15 — Session 33)

### What We Did

**Railway deployment fixes, dashboard UI improvements, and bookmaker tracking.**

**Railway deployment:**
- Diagnosed NBA All-Star break (Feb 13-15) as reason for no predictions — not a system failure
- Fixed Railway build error ("No module named pip") by creating `nixpacks.toml` with explicit pip installation via ensurepip
- Verified APScheduler-based job scheduling is correctly configured for all cron jobs

**Dashboard improvements:**
- Enhanced navbar tab highlighting — active tab now has `bg-blue-600 text-white` styling with clear visual distinction from inactive tabs
- Added bookmaker tracking to bet history — displays which sportsbook had the sharpest line for each bet
- Fixed Feb 11 Model Picks bug — Supabase returns bigint inconsistently as number/string, fixed using `String()` for Map keys

**Backend changes:**
- Added `bookmaker` column to `daily_predictions` table via Supabase migration
- Updated `daily_runner.py` to preserve bookmaker column instead of dropping it
- Added bookmaker to `PREDICTION_COLS` in `prediction_store.py`

**Test fixes:**
- Fixed 5 failing tests caused by recent changes:
  - `test_get_current_lines_success` — updated assertion for bookmaker column presence
  - `test_select_bets_over/under_direction` — added `bl_tau=None` to disable BL blending
  - `test_default_edge_threshold/bankroll` — changed to test explicit parameter passing

**Files created:**
- `nixpacks.toml` — Nixpacks build configuration for Railway (~15 lines)

**Files modified:**
- `dashboard/src/components/layout/Navbar.tsx` — Active tab highlighting with `usePathname()`
- `dashboard/src/app/history/page.tsx` — Fetch and display bookmaker from daily_predictions
- `dashboard/src/components/history/BetCard.tsx` — Display bookmaker badge
- `dashboard/src/types/predictions.ts` — Added bookmaker to PaperBet interface
- `src/models/daily_runner.py` — Keep bookmaker column in sharpest-book selection
- `src/models/prediction_store.py` — Added bookmaker to PREDICTION_COLS
- `tests/test_daily_runner.py` — Updated bookmaker assertion
- `tests/test_paper_trader.py` — Fixed BL blending and default value tests

**Tests:** 575 passed, 0 failures

### Next Step

1. **Monitor Railway deployment** — Jobs should resume after NBA All-Star break (Feb 16)
2. **Paper trade** — Continue daily paper trading with bookmaker tracking
3. **Discord bot** — Continue following development plan

---

## Session Summary (2026-02-15 — Session 32)

### What We Did

**Implemented Discord bot (Track H complete).** Built interactive Discord bot with slash commands and automated alerts for daily predictions and paper trading status.

**Slash commands implemented:**
- `/picks` — Get today's top predictions (filterable by stat type and min edge)
- `/player <name>` — Get predictions for a specific player (fuzzy match supported)
- `/bankroll` — Show current paper trading balance and P&L
- `/performance` — Show model performance stats (win rate, ROI, total bets)
- `/toppicks` — Quick view of top 5 picks for daily alerts

**Architecture:**
- Discord.py 2.6+ with slash commands via `@bot.tree.command()`
- Async database queries using `asyncio.to_thread()` for SQLAlchemy
- Discord REST API for alerts (works without bot process running)
- Rich embeds for formatted Discord messages
- Graceful shutdown handling (SIGINT/SIGTERM) for Railway compatibility

**Alerts integration:**
- Automated alerts triggered after inference job completes
- Uses Discord REST API directly — no bot process required
- `--skip-discord` flag added to inference job for debugging

**Bug fixes during implementation:**
- Fixed `teams` table query — `team_name` instead of non-existent `abbreviation` column
- Fixed `paper_bets` table columns — `status` not `result`, `stat_type` not `stat`, `bet_direction` not `side`, `odds_at_bet` not `odds`
- Fixed `paper_trading_daily_log` columns — `bankroll_after` not `current_bankroll`, `total_pnl`/`cumulative_pnl` for P&L values

**Files created:**
- `src/discord_bot/__init__.py`, `commands/__init__.py`, `services/__init__.py`, `formatters/__init__.py` — Package init files
- `src/discord_bot/bot.py` — Main bot class with all slash commands (~250 lines)
- `src/discord_bot/run_bot.py` — Entry point with graceful shutdown (~65 lines)
- `src/discord_bot/services/predictions.py` — Prediction database queries (~225 lines)
- `src/discord_bot/services/paper_trading.py` — Paper trading database queries (~225 lines)
- `src/discord_bot/formatters/embeds.py` — Discord embed builders (~280 lines)
- `src/discord_bot/alerts.py` — REST API alert sender (~195 lines)
- `scripts/run_discord_bot.bat` — Windows Task Scheduler script (~40 lines)

**Files modified:**
- `.env` — Renamed `BOT_TOKEN` to `DISCORD_BOT_TOKEN`, added channel IDs
- `requirements.txt` — Added `discord.py>=2.3.0`
- `src/orchestration/inference_job.py` — Added Discord alert trigger with `--skip-discord` flag
- `ARCHITECTURE.md` — Updated Discord Bot section from "Planned" to "Implemented"

**Tests:** 571 passed, 4 pre-existing failures (paper trading config defaults, unrelated to Discord bot)

### Next Step

1. **Start the Discord bot** — Run `scripts\run_discord_bot.bat` or add to Task Scheduler
2. **Test slash commands** — Commands should sync within 1 minute of bot startup
3. **Verify alerts** — Run inference job to test automated alert delivery
4. **Paper trade** — Continue daily paper trading with Discord notifications

---

## Session Summary (2026-02-14 — Session 31)

### What We Did

**Fixed Vercel deployment and added Model Picks filtering to dashboard.** Two main accomplishments this session.

**Vercel deployment fix:**
- Root cause: `dashboard/package.json` and `dashboard/tsconfig.json` were never committed due to `*.json` in `.gitignore`
- Added exceptions to `.gitignore`: `!package.json`, `!package-lock.json`, `!vercel.json`, `!tsconfig.json`
- Committed missing config files with `baseUrl: "."` in tsconfig.json for `@/*` path alias resolution
- User disabled "Include files outside the root directory" in Vercel settings
- Dashboard now live and serving pages at `game-flow-data.vercel.app`

**Model Picks filtering (History & Performance pages):**
- Created `BetSourceFilter` component (`dashboard/src/components/shared/BetSourceFilter.tsx`)
- Toggle between "Model Picks" (edge ≥9%) and "All Bets" — defaults to Model Picks
- History page: Filters bet list and summary stats by bet source
- Performance page: Recalculates all KPIs (P&L, ROI, Win Rate) from filtered bets, simulates model-picks-only bankroll progression
- Allows viewing actual model performance separate from all placed bets

**Files created:**
- `dashboard/src/components/shared/BetSourceFilter.tsx` — Bet source toggle component (~45 lines)

**Files modified:**
- `.gitignore` — Added exceptions for JSON config files
- `dashboard/src/app/history/page.tsx` — Added bet source filter and filtering logic
- `dashboard/src/app/performance/page.tsx` — Added bet source filter with recalculated KPIs
- `ARCHITECTURE.md` — Updated dashboard documentation

**Tests:** 575 passed, 0 failures

### Next Step

1. **Paper trade** — Continue daily paper trading with Railway automation
2. **Monitor Vercel deployment** — Check analytics and error logs
3. **Discord bot** — Follow development plan in `docs/discord_bot_development.md`

---

## Session Summary (2026-02-14 — Session 30)

### What We Did

**Deployed to Railway cloud platform.** Migrated from local Windows Task Scheduler to Railway for production job scheduling. Single always-on worker runs APScheduler-based scheduler.

**Railway deployment setup:**
- Created `railway.toml` for build configuration (Nixpacks)
- Created `src/orchestration/scheduler.py` with APScheduler (5 cron jobs, UTC times)
- Added `apscheduler==3.10.4` to requirements.txt
- Changed `psycopg2` to `psycopg2-binary` (pre-compiled, no build deps)
- Set environment variables via Railway CLI: `DATABASE_URL`, `ODDS_API_KEY`, `RAPIDAPI_KEY`

**Production model workflow:**
- Created `src/models/artifacts/production/` folder (committed to git)
- Updated `.gitignore` to ignore `run_*/` but allow `production/`
- Created `scripts/promote_model.py` to copy training runs to production
- Updated `inference_job.py` to check `production/` folder before falling back to latest `run_*`

**Disabled local scheduled tasks:**
- All 5 Windows Task Scheduler tasks (GameFlow-*) disabled to avoid conflicts with Railway
- Created high-priority work items for monitoring Railway tasks and adding Discord notifications

**Documentation:**
- Created `docs/railway_deployment.md` — full deployment guide
- Created `docs/scalability.md` — architecture capacity analysis and scaling path
- Updated `ARCHITECTURE.md` with Railway deployment section

**Files created:**
- `railway.toml` — Railway service configuration
- `src/orchestration/scheduler.py` — APScheduler-based job scheduler (~145 lines)
- `scripts/promote_model.py` — Model promotion script (~100 lines)
- `docs/railway_deployment.md` — Deployment guide (~130 lines)
- `docs/scalability.md` — Scalability analysis (~65 lines)

**Tests:** 575 passed, 0 failures

### Next Step

1. **Monitor Railway scheduled tasks** — Check logs via `railway logs` after first scheduled runs
2. **Add Discord webhook notifications** — Work item `feature_add_discord_webhook_notificati` (high priority)
3. **Paper trade** — Continue daily paper trading with Railway automation

---

## Session Summary (2026-02-14 — Session 29)

### What We Did

**Implemented G2/G3 dashboard insight features.** Added 14 `feat_*` columns to `daily_predictions` table and template-based insight generation for the Analysis Modal. Features explain WHY the model made its prediction.

**Key features:**
- **Database migration (G2):** Added columns for B2 rest/schedule, B1 injury context, B3 stat-specific trends, and opponent abbreviation via Supabase migration
- **Prediction storage update (G3):** `daily_runner.py` now maps feature values from `features_df` to predictions via `_map_features_to_predictions()` method
- **Insights generator:** New `dashboard/src/lib/insights.ts` generates template-based insights from feature values. Categories: rest, injury, trend, consistency, average
- **Context-aware sentiments:** Insight sentiments consider bet direction — for Under bets, L5 avg below line is positive (green), not negative
- **Analysis Modal update:** Added "Model Context" section displaying insights with color-coded sentiments

**Created historical backfill script.** `src/tools/backfill_prediction_features.py` populates `feat_*` columns for old predictions without modifying prediction values. Successfully backfilled Feb 10-12 (1,338 predictions).

**Deployed dashboard to Vercel (G9).** Dashboard live at `game-flow-data.vercel.app`. Configuration: root directory `dashboard`, environment variables `NEXT_PUBLIC_SUPABASE_URL` and `NEXT_PUBLIC_SUPABASE_ANON_KEY`. Vercel MCP available via `claude mcp add --transport http vercel https://mcp.vercel.com`.

**Files created:**
- `dashboard/src/lib/insights.ts` — Template-based insight generator (~155 lines)
- `src/tools/backfill_prediction_features.py` — Historical feature backfill script (~315 lines)

**Files modified:**
- `src/models/prediction_store.py` — Added 14 feat_* columns to PREDICTION_COLS
- `src/models/daily_runner.py` — Added `_map_features_to_predictions()` and `_get_opponent_abbrevs()` methods, defensive column checks
- `dashboard/src/components/analysis/AnalysisModal.tsx` — Added "Model Context" insights section

**Tests:** 575 passed, 0 failures

### Next Step

1. **Monitor Vercel deployment** — Check analytics and error logs
2. **Paper trade** — Continue daily paper trading with automated pipeline
3. **Discord bot** — Follow development plan in `docs/discord_bot_development.md`

---

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

- [x] **C2. Per-stat calibration breakdown** *(Implemented — 2026-02-18)*
  Built `src/diagnostics/calibration_per_stat.py` — standalone diagnostic producing per-stat (PTS/REB/AST) calibration report. Metrics: quantile coverage (Q10–Q90), bias (relative %), interval sharpness (80%/50% widths), Brier score, ECE, reliability curve data. Auto-diagnosis flags stats exceeding tolerance. Reads from backtest CSV (`--csv`) or production DB (`--db`). JSON export via `--output`.

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
- [x] **G2. Database migration** — *(DONE — 2026-02-14)* Added `feat_*` columns to `daily_predictions` for insight generation
- [x] **G3. Update prediction storage** — *(DONE — 2026-02-14)* Modified `prediction_store.py` and `daily_runner.py` to save feature values
- [x] **G4. Home page MVP** — *(DONE — 2026-02-09)* Prop cards grid with filtering (All/PTS/REB/AST/THREES), edge sorting, player name enrichment
- [x] **G5. Analysis modal** — *(DONE — 2026-02-14)* Last 5 games chart + quantile summary + template-based insights ("Model Context" section)
- [x] **G6. Hero section** — *(DONE — 2026-02-13)* "Play of the Day" with top pick by edge, star rating, analyze button
- [x] **G7. Player headshots** — *(DONE — 2026-02-09)* NBA CDN integration with inline SVG fallback
- [x] **G8. Paper trading views** — *(DONE — 2026-02-10)* History page and Performance page created:
  - `/history` — Bet history with status filters (All/Won/Lost/Push), summary bar, bet cards
  - `/performance` — KPI cards, bankroll chart, stat breakdown table
  - Components: BetCard, BetList, HistoryFilters, HistorySummary, KPICard, BankrollChart, StatBreakdown
  - Auth callback route for email confirmation added
- [x] **G9. Vercel deployment** — *(DONE — 2026-02-14)* Production deployment at `game-flow-data.vercel.app` with environment variables

---

## Track H: Discord Bot (Implemented)

Interactive Discord bot for daily prediction alerts and command-based queries. Full development plan at `docs/discord_bot_development.md`.

**Configuration (in `.env`):**
- `DISCORD_BOT_TOKEN` — Bot authentication token
- `DISCORD_CHANNEL_PREDICTIONS` — Channel for prediction queries
- `DISCORD_CHANNEL_ALERTS` — Channel for automated daily alerts
- `DISCORD_CHANNEL_PERFORMANCE` — Channel for performance updates

**Implementation Items:**
- [x] **H1. Bot foundation** — *(DONE — 2026-02-15)* Entry point (`run_bot.py`), Discord.py 2.6+ setup, slash command registration via `@bot.tree.command()`
- [x] **H2. Prediction service** — *(DONE — 2026-02-15)* Query `daily_predictions` for today's picks, player predictions, top edges via `services/predictions.py`
- [x] **H3. `/picks` command** — *(DONE — 2026-02-15)* Get today's top predictions (filterable by stat type and min edge)
- [x] **H4. `/player` command** — *(DONE — 2026-02-15)* Get predictions for a specific player (fuzzy match supported)
- [x] **H5. `/bankroll` command** — *(DONE — 2026-02-15)* Show paper trading balance from `paper_trading_daily_log`
- [x] **H6. `/performance` command** — *(DONE — 2026-02-15)* Show model stats (win rate, ROI, total bets) from `paper_bets`
- [x] **H7. Automated alerts** — *(DONE — 2026-02-15)* Send top picks to Discord after inference job completes via REST API (no bot process needed)
- [x] **H8. Bot hosting** — *(DONE — 2026-02-15)* Windows batch script (`scripts/run_discord_bot.bat`) for Task Scheduler; Railway-ready architecture

**Files Created:**
| File | Purpose |
|------|---------|
| `src/discord_bot/__init__.py` | Package init |
| `src/discord_bot/run_bot.py` | Entry point with graceful shutdown |
| `src/discord_bot/bot.py` | Bot class with all slash commands |
| `src/discord_bot/services/predictions.py` | Prediction database queries |
| `src/discord_bot/services/paper_trading.py` | Paper trading database queries |
| `src/discord_bot/formatters/embeds.py` | Discord embed builders |
| `src/discord_bot/alerts.py` | REST API alert sender (works without bot running) |
| `scripts/run_discord_bot.bat` | Windows Task Scheduler script |

**Architecture:**
- Slash commands require bot process running continuously (use `scripts/run_discord_bot.bat`)
- Automated alerts use Discord REST API directly — triggered by inference job without bot process
- All database queries use `asyncio.to_thread()` for async wrapping of synchronous SQLAlchemy
- Railway-ready: env vars, no Windows-specific code, graceful SIGINT/SIGTERM handling

---

## Track I: Paid Subscriptions (Deferred — Free Beta Active)

Monetization via Stripe deferred until ~200 Discord members. Currently running free beta to build credibility via social/Discord funnel.

**Current state (Session 35):** Paywall removed, all authenticated users have full access. RLS opened to `authenticated USING (true)`. Public `/picks` page shows 3 free picks to drive signups. All paid messaging replaced with "Free During Beta" + Discord CTAs.

**What stays intact for future Stripe activation:**
- `user_subscriptions` table and `is_subscribed()` function (dormant)
- `subscription.ts` types/utils
- `PricingCard.tsx` component (dormant)
- Stripe integration plan at `docs/paid_subscription_plan.md`

**Completed items (infrastructure):**
- [x] **I1. Database schema** — `user_subscriptions` table exists
- [x] **I2. RLS policies** — Subscriber-only policies created then replaced with open auth policies (Session 35)
- [x] **I4. Middleware** — Subscription check code written then removed (Session 35)
- [x] **I5. Pricing page** — Now shows $0/mo beta card
- [x] **I6. Landing page** — Done with free-beta messaging
- [x] **I7. Legal pages** — Terms + Privacy done (updated for free beta)
- [x] **I8. Route restructuring** — (public), (auth), (protected) route groups done

**Deferred items (activate when ready for Stripe):**
- [ ] **I3. Stripe integration** — Checkout session API, webhook handler, customer portal
- [ ] **I9. Re-enable paywall** — Restore subscriber-only RLS policies, middleware subscription check
- [ ] **I10. Update pricing page** — Switch from $0 beta card to $19.99/mo Stripe checkout
- [ ] **I11. Update legal pages** — Restore Stripe/billing language in Terms and Privacy

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
| ~~H1-H8 (Discord Bot)~~ | ~~Medium~~ | ~~High~~ | **DONE** — All slash commands, alerts, and hosting implemented. |
| I1-I8 (Paid Subscriptions) | Medium-High | Critical | **DEFERRED** — Free beta active (Session 35). Stripe activation deferred until ~200 Discord members. Infrastructure in place. |

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
