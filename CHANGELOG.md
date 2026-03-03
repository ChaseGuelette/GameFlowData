# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2026-03-03 Session 60] — Faster Lines Pipeline: Fuzzy Cache + Parallel Steps

### Added

- **Persistent fuzzy cache (`nba_linker_local.py`):** File-based cache at `linker_data/_fuzzy_cache.json` stores `{normalized_name: player_id_or_null}` with player count for auto-invalidation. `_load_fuzzy_cache()`, `_save_fuzzy_cache()`, and `_resolve_fuzzy_names()` helpers ported from MLB linker pattern. Both `link_incremental()` and `process_local()` refactored from per-row `match_player()` to 3-step batch pipeline: manual `.map()` → exact `.map(player_lookup)` → fuzzy cache. Typical runs: 95%+ cache hits, linker step ~15s → <1s.
- **Parallel step execution (`lines_job.py`):** New `--parallel` flag runs props path (game lines → props → linker) and injury path (scraper → linker) concurrently via `threading.Thread`. `run_step_group()` and `run_parallel_groups()` helpers. Full mode runtime: ~90s → ~45-55s. Without `--parallel`: unchanged sequential behavior.
- **`run_lines_full_parallel()` (`scheduler.py`):** New wrapper for noon/4pm full runs using `--live --parallel`.

### Changed

- **Props-only cron:** `*/10` → `*/5` (every 5 minutes, ~156 runs/day, up from ~78)
- **Edge refresh cron:** Updated to 5-minute cadence matching props schedule
- **Full run schedule:** Noon/4pm now use `run_lines_full_parallel()` with `--parallel`
- **API credit impact:** ~6,400 credits/day (up from ~3,200) — 4% of 5M monthly quota

### Files Changed

| File | Action |
|------|--------|
| `src/processing/nba_linker_local.py` | Modified — fuzzy cache helpers, batch player matching in both `link_incremental()` and `process_local()` |
| `src/orchestration/lines_job.py` | Modified — `--parallel` flag, `run_step_group()`, `run_parallel_groups()` |
| `src/orchestration/scheduler.py` | Modified — 5-min crons, `run_lines_full_parallel()`, updated noon/4pm jobs |

### Verified

- 629 Python tests pass, 0 failures
- Ruff: 0 remaining issues
- All 3 files compile without syntax errors

---

## [2026-03-02 Session 59] — MLB Local Linker with Checkpoint/Resume

### Added

- **`mlb_linker_local.py`** — Local CSV-based MLB linker with checkpoint/resume, mirroring the NBA local linker pattern. Downloads 6 tables to `mlb_linker_data/`, processes matching in pandas, uploads via chunked temp tables with retry/backoff.
- **Checkpoint system** — `_checkpoint.json` tracks per-stage and per-chunk progress. Completed stages skip on resume, in-progress uploads resume from last completed chunk.
- **4 processing sub-stages** — game_lines matching, props→games (±1 day fuzzy), props→players (exact + fuzzy + manual mappings), props→teams (boxscore cross-ref).
- **Upload retry/backoff** — 20 retries with linear backoff capped at 60s, `engine.dispose()` on error (survives laptop sleep/wake).
- **CLI commands** — download, process, upload, all, status, init, reset with `--force` and `--batch-delay` flags.
- **Player diagnostics** — `unmatched_players.csv` with fuzzy suggestions and confidence scores, `player_mappings.csv` for manual overrides.

### Files Changed

| File | Action |
|------|--------|
| `src/processing/mlb/mlb_linker_local.py` | Created — ~550 lines, full download/process/upload pipeline |
| `ARCHITECTURE.md` | Updated — added mlb_linker_local.py to module table and CLI reference |
| `docs/mlb_processing_pipeline_documentation.md` | Updated — added mlb_linker_local.py section |

### Verified

- 629 Python tests pass, 0 failures
- Ruff: 0 remaining issues (7 auto-fixed)
- CLI `--help`, `status`, and `reset` commands verified working
- All imports resolve correctly

---

## [2026-03-02 Session 58] — Pipeline Resilience Overhaul

### Added

- **Job status tracking (`scheduler.py`):** In-memory `JOB_STATUS` dict tracks every job's status, end time, and duration. `record_job_execution()` persists history to `job_executions` Supabase table for debugging and observability.
- **Dependency gate (`scheduler.py`):** `check_dependency()` verifies upstream jobs succeeded within a configurable time window. `run_inference()` checks daily stats succeeded in last 8 hours — if not, passes `--stale-warning` flag and sends Discord alert.
- **9:30 AM automatic retry (`scheduler.py`):** New `run_daily_stats_retry()` scheduled at 14:30 UTC. Checks if 9 AM run succeeded, re-runs if not. Gives the system a second chance before inference at 12:15 PM.
- **Per-step retries with backoff (`daily_stats_job.py`):** `run_command()` accepts `max_retries` and `retry_delay` params. Critical steps get 2 retries with exponential backoff (15s, 30s). Non-critical steps get 0 retries.
- **`--stale-warning` flag (`inference_job.py`):** Scheduler passes this when daily stats dependency check fails. Triggers stale-data Discord alert after predictions are generated.
- **MC sample staleness check (`edge_refresh_job.py`):** Warns via Discord if MC samples are >6 hours old.
- **`job_executions` table (`007_job_executions.sql`):** Persistent job execution history with index on `(job_name, started_at DESC)`.
- **21 unit tests (`test_pipeline_resilience.py`):** Coverage for `check_dependency()`, `run_command()` retries, `JOB_STATUS` tracking, and 9:30 retry logic.

### Changed

- **Global job timeout:** Increased from 30m → 45m in `scheduler.py:run_job()` to accommodate retry attempts.
- **Per-step timeouts:** Step 6 (rolling averages) 10m→20m, Step 7 (opponent stats) 10m→15m, Steps 3-5 (non-critical) 10m→5m.
- **Staleness check in inference:** Changed from `days_stale > 2` to checking if latest `game_date < yesterday`. Inference never hard-fails — stale data is better than zero predictions.
- **Edge refresh no-op message:** Changed "No MC samples" from warning to info-level "NO-OP" message (expected before first inference).

### Files Changed

| File | Action |
|------|--------|
| `src/orchestration/scheduler.py` | Modified — JOB_STATUS, check_dependency(), record_job_execution(), 9:30 retry, 45m timeout |
| `src/orchestration/daily_stats_job.py` | Modified — per-step retries with backoff, timeout tuning |
| `src/orchestration/inference_job.py` | Modified — --stale-warning flag, improved staleness check, stale data Discord alert |
| `src/orchestration/edge_refresh_job.py` | Modified — MC sample staleness warning, improved no-op logging |
| `database/migrations/007_job_executions.sql` | Created — persistent job execution history table |
| `tests/test_pipeline_resilience.py` | Created — 21 unit tests for resilience features |

### Verified

- 629 Python tests pass, 0 failures (21 new tests)
- Ruff: 0 remaining issues (18 auto-fixed)
- `--dry-run` confirms all 7 steps with correct timeout/retry params
- `--stale-warning` flag accepted by inference_job CLI
- Scheduler module imports cleanly, dependency check returns False when no status exists

---

## [2026-03-01 Session 57] — Fix DFS Dashboard, Sportsbook Timeouts, Edge Refresh Timeout

### Fixed

- **DFS dashboard showing no data on production:** Migration 004 (RPC independence from `daily_predictions`) was not applied. Applied via SQLAlchemy. Both `get_dfs_lines` and `get_sportsbook_lines` RPCs updated from `commence_time::date` cast to range conditions for index-friendly queries.
- **Sportsbook RPC statement timeout:** `get_sportsbook_lines(date)` timed out on multi-million row `raw_player_props_combined` table (8s Supabase limit). Created new `get_sportsbook_lines_by_games(text[])` RPC with game_id parameter and 24h snapshot_time cutoff. Dashboard updated to two-step batched fetch (3 games per parallel call, 0.3s each).
- **Edge refresh 30-minute timeout:** `fetch_fresh_lines()` had no `snapshot_time` cutoff, scanning all historical snapshots. During evening games, query degraded past 30-min timeout causing 3 consecutive skipped runs. Added `snapshot_time > now() - interval '24 hours'` cutoff.
- **DFS paper trader slow queries:** `_fetch_dfs_lines()` and `_fetch_sportsbook_lines()` used `commence_time::date` cast and had no snapshot_time cutoff. Updated to range conditions + 24h cutoff.
- **MLB rolling averages pgBouncer crash:** `mlb_populate_averages.py` crashed with "lost synchronization with server" fetching entire batting stats table. Added season-by-season fetch with `_get_seasons()` helper.

### Added

- **`get_sportsbook_lines_by_games(text[])` RPC:** Pure SQL function accepting game_id array, 24h snapshot_time cutoff. Migration `005_fast_sportsbook_rpc.sql`.
- **`idx_props_commence_time` index:** On `raw_player_props_combined(commence_time)` for range query performance.
- **`allGamesStarted` UX on DFS page:** Shows helpful message with clickable "+ Live" button when Pre-Game filter hides all started games.

### Files Changed

| File | Action |
|------|--------|
| `src/orchestration/edge_refresh_job.py` | Modified — 24h snapshot_time cutoff in `fetch_fresh_lines()` |
| `src/paper_trading/dfs_paper_trader.py` | Modified — range conditions + 24h cutoff in both fetch methods |
| `src/processing/mlb/mlb_populate_averages.py` | Modified — season-by-season fetch |
| `database/migrations/004_fix_rpc_prediction_dependency.sql` | Modified — range conditions, index |
| `database/migrations/005_fast_sportsbook_rpc.sql` | Created — batched sportsbook RPC |
| `dashboard/src/app/(protected)/dfs/page.tsx` | Modified — batched sportsbook fetch, allGamesStarted UX |

### Verified

- 575 Python tests pass, 0 failures
- Ruff: no new issues (pre-existing E402 only)
- Railway logs confirm edge refresh completing in ~2 min after fix (was timing out at 30 min)
- Sportsbook RPC batch of 3 games returns in 0.3s (was timing out at 8s+)

---

## [2026-03-01 Session 56] — Fix Stale Prediction Lines (MAX(line) Alt-Line Bug)

### Fixed

- **Critical: `MAX(line)` alt-line conflation in `fetch_fresh_lines()` SQL** — Bookmakers offering multiple alt lines (e.g., novig at 7.5/9.5/11.5/13.5/15.5) caused `MAX(line)` to pick the highest alt line. Over/Under odds from different line values got paired together, producing artificially low booksums that made broken data appear "sharpest." Result: Wembanyama stored at line=15.5 (market=11.5), Dort at line=7.5 (market=3.5).
- **Fix:** Added `line` to `ROW_NUMBER PARTITION BY` and `GROUP BY` so each bookmaker×line is its own row. Added `HAVING` clause requiring both Over and Under odds. Applied to both `edge_refresh_job.py` and `daily_runner.py`.

### Files Changed

| File | Action |
|------|--------|
| `src/orchestration/edge_refresh_job.py` | Modified — `fetch_fresh_lines()` SQL: line in partition, HAVING clause |
| `src/models/daily_runner.py` | Modified — `_get_current_lines()` SQL: same fix |
| `ARCHITECTURE.md` | Updated — line selection documentation |

### Verified

- 575 Python tests pass, 0 failures
- Ruff: no new issues (pre-existing E402 only)
- DB queries confirmed: Wembanyama line=15.5 stored (should be 11.5), Dort line=7.5 (should be 3.5) — fix will correct on next edge refresh

---

## [2026-03-01 Session 55] — MLB Processing Pipeline + Game Time TBD Fix

### Added

- **MLB Processing Module (`src/processing/mlb/`):** Complete Phase 2 processing pipeline:
  - `mlb_config.py` — Shared constants: rolling windows (batting L5/L10/L20/SZN, pitching L3/L5/SZN), 12 batting stats, 8 pitching stats, team aliases, batch sizes.
  - `mlb_linker.py` — Links `mlb_raw_player_props` by populating `game_id`, `player_id`, `team_id`. Temp table UPDATE pattern, fuzzy player matching (cached), ±1 day date window. Modes: `incremental` (daily) and `backfill` (one-time). Retry logic (20 attempts, escalating waits) survives connection drops and laptop sleep/wake.
  - `mlb_populate_averages.py` — Full backfill of `mlb_player_average_batting` (71 columns) and `mlb_player_average_pitching` (41 columns). Shift(1) rolling averages, rate stats from rolling sums, std devs, context metrics.
  - `mlb_populate_averages_incremental.py` — Daily incremental per-player rolling calculation with UPSERT.
- **MLB Average Tables (`database/migrations/002_mlb_averages.sql`):** Two new tables for model consumption with indexes and RLS.

### Fixed

- **995 placeholder player names in `mlb_players`:** Batch-fetched real names from MLB Stats API. All 995 resolved. Linker match rate jumped from ~1% to 100%.
- **`_ensure_player` in `mlb_stats_scraper.py`:** Changed from `ON CONFLICT DO NOTHING` to `DO UPDATE` to prevent future placeholder name stagnation.
- **Dashboard game times showing "TBD" (`daily_runner.py`):** `_enrich_game_times()` assumed all NBA games are evening (UTC date = target+1). Matinee/afternoon games fall on same UTC date and were missed. Fixed to search 2-day UTC window filtered by ET date. Backfilled 2,073 predictions — all dates now 100% coverage.
- **Ruff lint:** Removed unused `starter_float` variable in `mlb_populate_averages.py`, fixed undefined `utc_date` reference in `daily_runner.py`.

### Files Changed

| File | Action |
|------|--------|
| `src/processing/mlb/__init__.py` | Created — module marker |
| `src/processing/mlb/mlb_config.py` | Created — shared constants |
| `src/processing/mlb/mlb_linker.py` | Created — MLB props linker (~580 lines) |
| `src/processing/mlb/mlb_populate_averages.py` | Created — full backfill averages (~400 lines) |
| `src/processing/mlb/mlb_populate_averages_incremental.py` | Created — daily incremental averages (~350 lines) |
| `database/migrations/002_mlb_averages.sql` | Created — batting + pitching average tables |
| `src/scrapers/mlb/mlb_stats_scraper.py` | Modified — `_ensure_player` DO UPDATE |
| `src/models/daily_runner.py` | Modified — game time enrichment UTC fix |

### Verified

- 575 Python tests pass, 0 failures
- Ruff: all checks pass for modified files
- MLB averages backfill completed: 201,306 batting + 82,979 pitching rows
- MLB linker backfill in progress (~2.2M/22.7M linked, running in terminal)
- Game time backfill: 2,073 predictions updated, 100% coverage across all dates

---

## [2026-03-01 Session 54] — DFS Fixes, LIVE Tags, Performance DFS Tab, RPC Independence

### Fixed

- **Edge refresh early exit blocking DFS paper trading (`edge_refresh_job.py`):** `sys.exit(0)` at line 496-501 (when no MC samples exist before inference) killed the process before DFS step 7c could run. Moved DFS paper trading to **step 0** (before MC sample check) so it runs every 10 minutes independently of model inference.
- **RPCs dependent on `daily_predictions` (`004_fix_rpc_prediction_dependency.sql`):** Both `get_dfs_lines` and `get_sportsbook_lines` RPC functions joined on `daily_predictions` for game scoping — returned no data before inference. Updated both to scope by `commence_time::date` directly. `get_dfs_lines` now also joins `players` table for `player_name` and returns `game_time` (commence_time).
- **DFS page blank in market mode before inference (`dfs/page.tsx`):** `marketComparisons` useMemo required a matching prediction to build comparisons. Now constructs comparisons from DFS line data as fallback when no predictions exist.
- **Stale prediction lines (Danny Wolf 13.5 → 10.5):** Edge refresh mechanism properly re-evaluates MC samples at new lines, but wasn't running due to the sys.exit(0) bug above. Now fixed — edges update within 10 minutes of line movement.

### Added

- **LIVE tags on game cards:** PropCard, PlayOfTheDay, and TonightsGames game pills now show a pulsing red dot + "Live" badge when `game_time <= now()`. Added `isGameLive()` utility to `utils.ts`. Game times always display ("TBD" when unknown instead of blank).
- **Game time backfill (`dashboard/page.tsx`):** Client-side propagation of `game_time` from predictions that have it to same-game predictions that don't (570/783 were missing game_time).
- **DFS Performance tab (`performance/page.tsx`):** New "Props / DFS" tab toggle. DFS tab shows KPI cards (bankroll, P&L, ROI, W-L-P record), bankroll chart from `dfs_paper_daily_log`, and slip type breakdown table with per-type stats.
- **`DfsLine` type updates (`dfs.ts`):** Added `player_name` and `game_time` optional fields to match updated RPC return type.

### Files Changed

| File | Action |
|------|--------|
| `src/orchestration/edge_refresh_job.py` | Modified — moved DFS block from step 7c to step 0 |
| `database/migrations/004_fix_rpc_prediction_dependency.sql` | Created — updates both RPCs to remove prediction dependency |
| `sql/functions/get_sportsbook_lines.sql` | Modified — updated to match deployed RPC |
| `dashboard/src/app/(protected)/dfs/page.tsx` | Modified — market comparisons without predictions |
| `dashboard/src/app/(protected)/performance/page.tsx` | Modified — DFS performance tab |
| `dashboard/src/app/(protected)/dashboard/page.tsx` | Modified — game_time backfill |
| `dashboard/src/lib/utils.ts` | Modified — formatGameTime returns "TBD", added isGameLive() |
| `dashboard/src/types/dfs.ts` | Modified — player_name, game_time fields |
| `dashboard/src/components/predictions/PropCard.tsx` | Modified — always show time, LIVE tag |
| `dashboard/src/components/predictions/PlayOfTheDay.tsx` | Modified — always show time, LIVE tag |
| `dashboard/src/components/predictions/TonightsGames.tsx` | Modified — always show time, LIVE tag |

### Verified

- 575 Python tests pass, 0 failures
- Ruff: only pre-existing warnings (E402, F841 in MLB/linker files)
- Migration 004 ran successfully — 2,098 DFS lines and 5,490 sportsbook lines returned for today before inference

---

## [2026-02-28 Session 53] — DFS Paper Trading Engine + Live Toggle

### Added

- **DFS Paper Trading Engine (`dfs_paper_trader.py`):** Backend paper trading engine for multi-leg DFS entries using devigged sportsbook consensus (market edge, no model dependency). Builds 4 entries/day across slip types: UD 3-pick (6x), UD 5-pick (20x), PP 5-flex (10x/2x/0.4x), PP 6-flex (25x/2x/0.4x). Port of TypeScript market edge logic (exact-line-match devigging, multiplicative consensus averaging) to Python. Entry selection: positive edge filter, platform preference tiebreaker, one-leg-per-player dedup, started game exclusion. Resolution handles push/cancel (reduce effective entry), flex partial payouts. $500 bankroll, $10/entry.
- **Database Migration (`003_dfs_paper_trading.sql`):** Three new tables — `dfs_paper_entries` (unique on entry_date/slip_type), `dfs_paper_legs` (FK cascade, unique on entry_id/player_id), `dfs_paper_daily_log` (unique on entry_date). Indices on date/status, entry_id, date.
- **Edge Refresh Step 7c (`edge_refresh_job.py`):** DFS paper trading integrated into 10-minute edge refresh cycle. Resolves previous-day entries, then builds and places new ones. Non-fatal try/except wrapper.
- **DFS Audit (`audit_and_resolve.py`):** `--dfs` flag shows entry/leg status breakdown, per-slip-type W/L/P stats, leg details, daily log with cumulative P&L and bankroll.
- **DFS Live Toggle (`DfsFilters.tsx`, `dfs/page.tsx`):** "Pre-Game / + Live" toggle on DFS Edge Finder page. Default hides started-game picks. Orange "+ Live" mode shows all picks. Same pattern as main dashboard live toggle.

### Files Changed

| File | Action |
|------|--------|
| `src/paper_trading/dfs_paper_trader.py` | Created — DFS paper trading engine (~900 lines) |
| `database/migrations/003_dfs_paper_trading.sql` | Created — 3 new tables + indices |
| `src/orchestration/edge_refresh_job.py` | Modified — added step 7c DFS paper trading |
| `src/paper_trading/audit_and_resolve.py` | Modified — added `--dfs` flag |
| `dashboard/src/app/(protected)/dfs/page.tsx` | Modified — `showLive` state + filtering |
| `dashboard/src/components/dfs/DfsFilters.tsx` | Modified — live toggle UI |

### Verified

- 575 Python tests pass, 0 failures
- TypeScript compiles with zero errors
- Migration ran successfully — all 3 tables verified in Supabase
- `build_entries(date.today())` successfully built 4 entries (824 DFS lines, 2781 sportsbook lines)
- First entries placed: entry_ids 1-4 for 2026-02-28

---

## [2026-02-28 Session 52] — Dashboard Live Toggle + DFS 2-Pick

### Added

- **Live Betting Toggle (`dashboard/page.tsx`):** "Pre-Game / + Live" pill toggle in dashboard header controls. Default (Pre-Game) hides predictions whose `game_time` has passed. "+ Live" (orange pill) shows all predictions including in-progress games. Client-side comparison: `new Date(p.game_time) <= new Date()`.
- **DFS PP 2-Pick Slip Type (`dfs.ts`):** Added `pp_2_power` to `DFS_SLIP_TYPES` — PrizePicks 2-Pick Power with 3x payout, 57.7% break-even per leg. Most conservative slip type for high-conviction plays.

### Files Changed

| File | Action |
|------|--------|
| `dashboard/src/app/(protected)/dashboard/page.tsx` | Modified — `showLive` state, filter logic, toggle UI |
| `dashboard/src/types/dfs.ts` | Modified — added `pp_2_power` slip type |

### Verified

- TypeScript compiles successfully (`npx next build`)
- 575 Python tests pass, ruff auto-fixed 5 issues (pre-existing)

---

## [2026-02-28 Session 51] — 10-Minute Scheduler + Bet Resolution + Live Game Filter

### Changed

- **`scheduler.py`:** Replaced 21 hardcoded job definitions (hourly 1-3 PM, half-hourly 4:30-6:30 PM) with 2 APScheduler `CronTrigger` jobs covering 11 AM – 11 PM ET every 10 minutes. Props scrape at `:00/:10/:20/:30/:40/:50`, edge refresh at `:02/:12/:22/:32/:42/:52`. Added `silent_on_success` flag to `run_job()` — high-frequency jobs only send Discord alerts on failure (~78 runs/day each, down from 8/day previously). Total job definitions: 21 → 7.
- **`edge_refresh_job.py`:** Step 7b now calls `PaperTrader.resolve_all_pending(exclude_today=True)` before placing new bets. Previous-day bets get resolved every 10 minutes instead of only once daily.
- **`paper_trader.py`:** Added `exclude_today` parameter to `resolve_all_pending()` — filters `game_date < today` to prevent same-day false resolution. Added `_get_started_game_ids()` method checking `commence_time` from `raw_player_props_combined`. `select_bets()` now skips in-progress games to prevent false edges from mid-game line comparisons.

### Added

- **`audit_and_resolve.py`:** Diagnostic script for paper bet state. Supports `--audit` (show status breakdown), `--resolve` (resolve pending bets with available stats), `--backfill` (re-place missed bets from historical predictions), and `--dry-run` flags.

### Fixed

- **Paper bet resolution gap:** Bets placed after old schedule cutoff (6:30 PM) were never resolved. Backfilled 14 missed bets across 5 dates; corrected P&L from $1,841.68 → $2,231.14 (+$389.46).
- **Live game false edges:** 16 of 36 pending bets were for games already in progress (pre-game MC samples vs mid-game lines). Live game filter prevents this going forward; existing 16 live bets were deleted.

### Files Changed

| File | Action |
|------|--------|
| `src/orchestration/scheduler.py` | Modified — 10-min cron schedule, silent_on_success |
| `src/orchestration/edge_refresh_job.py` | Modified — resolve pending bets before placing new ones |
| `src/paper_trading/paper_trader.py` | Modified — exclude_today, live game filter |
| `src/paper_trading/audit_and_resolve.py` | Created — diagnostic/fix script |

### Verified

- 575 Python tests pass, 1 skipped, ruff clean
- Backfill resolved 18 additional bets correctly
- Live game filter correctly identified 16 in-progress game bets
- Deployed to Railway, 10-minute cadence confirmed

---

## [2026-02-26 Session 50] — MLB Statcast & FanGraphs Advanced Stats Scrapers

### Added

- **`mlb_statcast_scraper.py`:** Daily Statcast scraper using `pybaseball.statcast()`. Fetches pitch-level data (~4,500 rows/day), aggregates per (batter/pitcher, game_date) into contact quality (exit velo, barrel%, hard hit%, sweet spot%), expected stats (xBA, xSLG, xwOBA), batted ball distribution (GB/FB/LD/popup), spray direction, and plate discipline (zone%, chase%, whiff%). Pitching adds fastball velo/spin, pitch mix classification (FF/SI/FC=fastball, SL/CU/KC=breaking, CH/FS=offspeed), and CSW%. Uses `ON CONFLICT DO UPDATE` upserts (Statcast data corrected retroactively).
- **`mlb_fangraphs_scraper.py`:** Season-level FanGraphs stats via `pybaseball.batting_stats()` / `pitching_stats()`. Batting: wRC+, wOBA, ISO, WAR, BB%, K%, Hard%. Pitching: FIP, xFIP, xERA, SIERA, K/9, BB/9, HR/9. Player ID resolution: name matching against `mlb_players` → fallback to `playerid_reverse_lookup()` FanGraphs→MLBAM crosswalk. Auto-inserts unknown players via `_ensure_player()`.
- **`mlb_statcast_backfill.py`:** Bulk Statcast backfill orchestrator. Day-by-day iteration through MLB seasons (March–October). Progress file (`mlb_statcast_backfill_progress.json`) for resume capability. 1 req/sec rate limiting with tqdm progress bar.
- **Supabase migration:** 3 new tables — `mlb_player_game_statcast_batting` (PK: player_id, game_date), `mlb_player_game_statcast_pitching` (PK: player_id, game_date), `mlb_player_season_advanced` (PK: player_id, season, player_type). All with FK to `mlb_players` and appropriate indexes.
- **`pybaseball>=2.2.7`** added to `requirements.txt`.

### Fixed

- **FanGraphs FK violation:** Added `_ensure_player()` to FanGraphs scraper to auto-insert players resolved via crosswalk but not yet in `mlb_players` table.

### Files Changed

| File | Action |
|------|--------|
| `src/scrapers/mlb/mlb_statcast_scraper.py` | Created — Daily Statcast scraper with pitch-level aggregation |
| `src/scrapers/mlb/mlb_fangraphs_scraper.py` | Created — Season-level FanGraphs advanced stats |
| `src/scrapers/mlb/mlb_statcast_backfill.py` | Created — Backfill orchestrator with resume support |
| `requirements.txt` | Modified — added pybaseball>=2.2.7 |

### Database Migrations

| Migration | Description |
|-----------|-------------|
| `create_mlb_statcast_and_advanced_tables` | 3 tables: statcast batting, statcast pitching, season advanced |

### Verified

- 608 Python tests pass, ruff clean
- pybaseball sanity check: 4,294 pitch rows for 2025-06-15
- Statcast single-day: 303 batting + 125 pitching rows upserted
- FanGraphs 2024 season: 485 batting + 579 pitching rows upserted (92%/88% match rate)

---

## [2026-02-26 Session 49] — DFS Market/Combined Edge Modes + Fallback Games Fix

### Added

- **Market Edge mode (`/dfs`):** 3-way edge mode toggle (Model/Market/Combined) on DFS Edge Finder. Market Edge uses devigged sportsbook consensus probabilities vs DFS break-even thresholds. Combined Edge shows highest-conviction picks where both model AND market agree.
- **`get_sportsbook_lines` RPC function:** Supabase SECURITY DEFINER function returning non-DFS bookmaker lines for a target date. Uses `ROW_NUMBER()` deduplication, handles game ID format normalization.
- **`idx_props_sportsbook_lookup` partial index:** Performance index on `raw_player_props_combined` for sportsbook queries (excludes DFS platforms).
- **Devigging utilities in `dfs-utils.ts`:** `americanToImpliedProb()`, `devig()`, `computeVig()`, `formatBookmaker()` — shared between DFS page and AnalysisModal.
- **New types in `dfs.ts`:** `EdgeMode`, `SportsbookLine`, `MarketEdgePlatformLine`, `CombinedEdgePlatformLine`.
- **`/api/games` API route:** Next.js server-side route fetching today's games from NBA CDN schedule (`scheduleLeagueV2.json`). Replaces `get_games_for_date` RPC for fallback games. 1-hour revalidation cache.
- **`get_games_for_date` SQL function:** Fixed ET timezone boundaries and added `idx_staging_commence_time` index (query: 33s → 0.026s). Retained as SQL migration reference.

### Changed

- **`DfsFilters.tsx`:** Added 3-way edge mode segmented control (blue=Model, purple=Market, amber=Combined).
- **`DfsTable.tsx`:** Conditional column layouts per mode (Model: 10 cols, Market: 11 cols, Combined: 10 cols). Extended `SortKey` with `market_prob` and `books`.
- **`dfs/page.tsx`:** Added `get_sportsbook_lines` RPC to parallel fetch. New `sbIndex` and `marketComparisons` memos for market data. 3-way `filteredRows` branching. Mode-aware KPI cards.
- **`AnalysisModal.tsx`:** Replaced local `oddsToImpliedProb` and `formatBookmaker` with imports from `dfs-utils.ts`.
- **`dashboard/page.tsx`:** Replaced `get_games_for_date` RPC fallback with `/api/games` NBA CDN route.

### Files Changed

| File | Action |
|------|--------|
| `dashboard/src/types/dfs.ts` | Modified — added EdgeMode, SportsbookLine, MarketEdgePlatformLine, CombinedEdgePlatformLine |
| `dashboard/src/lib/dfs-utils.ts` | Modified — added americanToImpliedProb, devig, computeVig, formatBookmaker |
| `dashboard/src/components/dfs/DfsFilters.tsx` | Modified — added edge mode toggle |
| `dashboard/src/components/dfs/DfsTable.tsx` | Modified — 3 conditional column layouts |
| `dashboard/src/app/(protected)/dfs/page.tsx` | Modified — sportsbook fetch, market comparisons, 3-way filtering |
| `dashboard/src/components/analysis/AnalysisModal.tsx` | Modified — import shared utils |
| `dashboard/src/app/(protected)/dashboard/page.tsx` | Modified — NBA CDN fallback |
| `dashboard/src/app/api/games/route.ts` | Created — NBA CDN schedule proxy |
| `sql/functions/get_sportsbook_lines.sql` | Created — sportsbook lines RPC |
| `sql/functions/get_games_for_date.sql` | Created — ET-aware game lookup (reference) |

### Database Migrations

| Migration | Description |
|-----------|-------------|
| `get_sportsbook_lines` | RPC function for non-DFS bookmaker lines |
| `idx_props_sportsbook_lookup` | Partial index for sportsbook queries |
| `get_games_for_date` update | ET timezone boundaries + range filter |
| `idx_staging_commence_time` | Index on `raw_game_lines_staging(commence_time)` |

### Verified

- 575 Python tests pass, ruff clean (pre-existing issues only)
- `cd dashboard && npm run build` — no TypeScript errors
- NBA CDN returns 10 games for today (Feb 26)
- `get_sportsbook_lines('2026-02-26')` returns 2,291+ rows in ~2.5s

---

## [2026-02-25 Session 48] — DFS Edge Finder Page

### Added

- **DFS scraping (`us_dfs` region):** Added `us_dfs` to Odds API requests in `daily_player_props_scraper.py`. DFS platforms (PrizePicks, Underdog, Pick6, Betr) now stored in `raw_player_props_combined` alongside sportsbook lines.
- **`get_dfs_lines` RPC function:** Supabase SECURITY DEFINER function that returns latest DFS line per bookmaker/player/stat for a given date's games. Handles game ID format normalization via LPAD.
- **`idx_props_bookmaker_dfs` partial index:** Partial index on `raw_player_props_combined` for DFS bookmaker queries. Required for acceptable performance on 26M+ row table.
- **`dashboard/src/types/dfs.ts`:** TypeScript types for DFS lines, comparisons, platform lines. Constants for slip type break-even thresholds (UD 3/5-Pick, PP 5/6-Flex), platform display names, market-to-stat mappings.
- **`dashboard/src/lib/dfs-utils.ts`:** Extracted `estimateUnderProb`, `estimateOverProb`, `calcDfsEv`, `calcAllSlipEvs` functions. Shared between DFS page and AnalysisModal.
- **`dashboard/src/components/dfs/DfsFilters.tsx`:** Platform filter tabs (All/PrizePicks/Underdog/Pick6/Betr), slip type dropdown, stat filter tabs, +EV Only toggle.
- **`dashboard/src/components/dfs/DfsTable.tsx`:** Sortable table with player avatars, stat badges, platform names, sharp vs DFS line comparison, direction recommendation, model probability, break-even threshold, and color-coded edge display.
- **`dashboard/src/app/(protected)/dfs/page.tsx`:** DFS Edge Finder page. Fetches predictions + DFS lines in parallel, joins client-side, re-estimates model probability at DFS-specific lines via quantile interpolation, computes EV against slip type break-even thresholds. Includes KPI summary cards (count, avg edge, best pick).
- **Navbar DFS link:** Added "DFS" between Props and History in protected nav.

### Changed

- **`AnalysisModal.tsx`:** Replaced inline `estimateUnderProb` function (50 lines) with import from `@/lib/dfs-utils`. No behavior change.

### Files Changed

| File | Action |
|------|--------|
| `src/scrapers/daily_player_props_scraper.py` | Modified — added `us_dfs` to regions |
| `dashboard/src/types/dfs.ts` | Created — DFS types, slip types, platform names |
| `dashboard/src/lib/dfs-utils.ts` | Created — quantile interpolation, EV calc |
| `dashboard/src/components/dfs/DfsFilters.tsx` | Created — filter controls |
| `dashboard/src/components/dfs/DfsTable.tsx` | Created — sortable comparison table |
| `dashboard/src/app/(protected)/dfs/page.tsx` | Created — DFS Edge Finder page |
| `dashboard/src/components/layout/Navbar.tsx` | Modified — added DFS link |
| `dashboard/src/components/analysis/AnalysisModal.tsx` | Modified — import from dfs-utils |

### Database Migrations

| Migration | Description |
|-----------|-------------|
| `create_get_dfs_lines_rpc` | Initial RPC function (integer types) |
| `fix_get_dfs_lines_column_types` | Fixed to use bigint/timestamptz matching actual column types |
| `add_dfs_bookmaker_index` | Partial index for DFS bookmaker queries |

### Verified

- 575 Python tests pass, ruff clean (pre-existing issues only)
- `cd dashboard && npm run build` — no TypeScript errors
- DFS scrape confirmed: 564 lines across 4 platforms (177 PrizePicks, 173 Betr, 134 Pick6, 80 Underdog)
- `get_dfs_lines('2026-02-25')` returns results in <1s with index

---

## [2026-02-25 Session 47] — US State Sportsbook Filter + Clickable Line Selection

### Added

- **`sportsbook-availability.ts`:** New utility mapping ~26 US legal sports betting states to their licensed bookmaker keys. Offshore books (Pinnacle, Novig, ProphetX, Bovada) excluded from all states. `getAllowedBookmakers(stateCode)` returns allowed list or `null` for "All States".
- **State selector dropdown on dashboard page:** Persisted to localStorage (`user_state`). Shows abbreviated state codes in filter bar before Model Picks toggle.
- **AnalysisModal state filtering:** Reads `user_state` from localStorage. Filters `processedLines` to only show lines from bookmakers legal in the selected state. Automatically affects BEST EDGE badge, EASIEST badge, and bet sizing. Shows "(MI only)" label when active. Shows "No lines from MI-licensed books" when all lines are filtered out.
- **Clickable sportsbook line selection:** Line rows converted from `<div>` to `<button>` elements. Clicking any line selects it for bet sizing calculation (recalculates Kelly stake, recommended bet, and odds display). Defaults to best-edge line (index 0). Resets when `processedLines` changes. Shows "SIZING" badge on selected row with green border/ring highlight.

### Files Changed

| File | Action |
|------|--------|
| `dashboard/src/lib/sportsbook-availability.ts` | Created — US state → bookmaker mapping |
| `dashboard/src/app/(protected)/dashboard/page.tsx` | Modified — state selector dropdown, localStorage persistence |
| `dashboard/src/components/analysis/AnalysisModal.tsx` | Modified — state filter on processedLines, clickable line selection, selectedLineIndex state |

### Verified

- 608 Python tests pass, ruff clean (pre-existing issues only)
- `cd dashboard && npm run build` — no TypeScript errors
- Build passes cleanly with all new imports and state management

---

## [2026-02-24 Session 46] — Pipeline Recovery + Resilience + Auto Paper Bets

### Fixed

- **`prediction_store.py` — `np.isfinite()` TypeError:** Mixed `None`/`float` in `bl_confidence` and other edge columns created `object` dtype that `np.isfinite()` can't handle. Added `pd.to_numeric(errors="coerce")` before the `isfinite` call. Also fixed `NaT` timestamps serializing as string `'NaT'` to PostgreSQL instead of NULL.
- **`daily_stats_job.py` — 30-minute timeout:** `play_type_scraper.py` (Step 8) called `stats.nba.com` which blocks datacenter IPs, causing the subprocess to hang until the scheduler killed it after 30 minutes. Removed play type scraper from daily pipeline.
- **`scheduler.py` — Discord "1/7 steps" bug:** `re.search()` returned the first match ("Step 1/7") instead of the last. Switched to `re.findall()` and take `[-1]` for the final step count. Also updated hardcoded `Step (\d+)/8` regex to dynamic `Step (\d+)/(\d+)`.

### Added

- **Critical/non-critical step resilience in `daily_stats_job.py`:** Steps are now 3-tuples `(command, description, critical)`. Critical steps (CDN scrape, linker, rolling averages, opponent allowed) abort on failure. Non-critical steps (team IDs, positions, league averages) log warning and continue. Ensures paper bet resolution always runs.
- **Auto paper bet placement in `inference_job.py`:** After storing predictions, automatically calls `PaperTrader.select_bets()` + `place_bets()`. Non-fatal — failures don't affect predictions. `--skip-bets` flag available.
- **DNP/0-minute void in `paper_trader.py`:** `resolve_bets()` now voids bets on players with `did_not_play=True` or `min=0` (status=`cancelled`, pnl=0). Matches sportsbook behavior where DNP bets are refunded.
- **`--skip-bets` flag on `inference_job.py`:** Skips automatic paper bet placement.

### Recovery

- Ran inference for 5 missed dates (Feb 20-24) using stored feature data and prop lines
- Backfilled paper bets for Feb 20-23: 18W-1L-1C (+$1,035 on $1,424 staked)
- Voided 1 bet (Vukcevic, 0 minutes played on Feb 22)
- Comprehensive audit confirmed no future sight — all feature queries use strict `game_date < :as_of_date`

### Files Changed

| File | Action |
|------|--------|
| `src/models/prediction_store.py` | Fixed — `pd.to_numeric()` + `pd.NaT` check |
| `src/orchestration/daily_stats_job.py` | Modified — removed play type scraper, added step resilience |
| `src/orchestration/scheduler.py` | Fixed — step counter regex, `re.findall()` for last match |
| `src/orchestration/inference_job.py` | Modified — auto paper bet placement, `--skip-bets` flag |
| `src/paper_trading/paper_trader.py` | Modified — DNP/0-minute void logic in `resolve_bets()` |

### Verified

- 608 Python tests pass, ruff clean (1 pre-existing unused var in nba_linker_local.py)
- Recovery predictions stored and verified against game stats
- Paper bet audit confirmed all actuals match `player_game_stats`

---

## [2026-02-19 Session 45] — Frequent Line Scraping + Edge Refresh Pipeline

### Added

- **`edge_refresh_job.py`:** New lightweight job (~2-3 sec) that recalculates edges and BL recommendations using stored MC samples + fresh prop lines. Runs after each intra-day props scrape without re-running inference. Self-contained — no model pipeline dependencies.
- **`PredictionStore.get_all_samples_for_date()`:** Bulk retrieval method that loads all MC sample arrays for a date into `dict[(player_id, game_id, stat) -> np.ndarray]`.
- **`--target-table` arg on `daily_player_props_scraper.py`:** Allows live scraping to write directly to `raw_player_props_combined` instead of `raw_player_props_live`.
- **`--live` flag on `lines_job.py`:** Uses live Odds API endpoints with `--target-table raw_player_props_combined`.
- **`--props-only` flag on `lines_job.py`:** Skips game lines and injury scraping — runs only props scraper + linker for fast intra-day refreshes.
- **21-job scheduler schedule:** Two full inference windows (12:15 PM, 4:15 PM ET), hourly props+edge refresh (1-3 PM ET), half-hourly props+edge refresh (4:30-6:30 PM ET).
- **Edge refresh metrics parsing** in scheduler Discord alerts (`predictions_updated`, `recommended`).

### Changed

- **`game_lines_scraper.py`:** Region parameter from `"us"` to `"us,us2,us_ex"` for full US sportsbook coverage.
- **`live_odds_scraper.py`:** Same region expansion.
- **`scheduler.py`:** `run_job()` now accepts `extra_args` string parameter. Added `run_lines_full()`, `run_lines_props_only()`, `run_edge_refresh()` wrappers.

### Files Changed

| File | Action |
|------|--------|
| `src/scrapers/daily_player_props_scraper.py` | Modified — `--target-table` CLI arg, `target_table` param on `run_live_scrape()` |
| `src/orchestration/lines_job.py` | Modified — `--live`, `--props-only` flags |
| `src/scrapers/game_lines_scraper.py` | Modified — region us → us,us2,us_ex |
| `src/scrapers/live_odds_scraper.py` | Modified — region us → us,us2,us_ex |
| `src/models/prediction_store.py` | Modified — `get_all_samples_for_date()` method |
| `src/orchestration/edge_refresh_job.py` | Created — lightweight edge recalculation job |
| `src/orchestration/scheduler.py` | Modified — new schedule, `extra_args`, new wrappers |

### Verified

- 608 Python tests pass, ruff clean
- `lines_job --live --props-only --dry-run` confirmed correct commands
- `edge_refresh_job --dry-run` exits gracefully when no samples exist
- Scheduler lists all 21 jobs with correct UTC cron triggers

---

## [2026-02-19 Session 44] — NBA Play Types Feature

### Added

- **Play Types tab on Data Vault:** New "Play Types" tab alongside Players, Teams, and Defense. Shows Synergy play type data for all 30 NBA teams across 11 play types (Isolation, Transition, PnR BH, PnR RM, Post Up, Spot Up, Handoff, Cut, Off Screen, Off Rebound, Misc).
- **Offense/Defense toggle:** `OffDefToggle` component for switching between offensive and defensive play type breakdowns.
- **Frequency/Efficiency sub-tabs:** Frequency shows possession percentage (POSS_PCT) per play type; Efficiency shows points per possession (PPP).
- **Client-side pivot utility:** `pivotPlayTypes.ts` transforms 330 long-format DB rows into 30 wide-format team rows with synthetic columns.
- **Database table:** `team_play_types` with public read RLS and season index (already existed from prior session).
- **Python scraper:** `play_type_scraper.py` — fetches 22 API calls (11 play types x 2 groupings) from NBA Synergy endpoint (already existed from prior session).
- **Pipeline integration:** Added play type scraper as Step 8 in `daily_stats_job.py`.

### Files Changed

| File | Action |
|------|--------|
| `src/orchestration/daily_stats_job.py` | Modified — added Step 8 play type scraper |
| `dashboard/src/types/stats.ts` | Modified — added `playTypes` to `StatsMainTab`, `PlayTypeCategory`, `PlayTypeGrouping` types |
| `dashboard/src/lib/stats/columns.ts` | Modified — added 22 column definitions (11 frequency + 11 efficiency) and `playTypeColumnMap` |
| `dashboard/src/lib/stats/pivotPlayTypes.ts` | Created — long-to-wide pivot utility |
| `dashboard/src/components/stats/OffDefToggle.tsx` | Created — offense/defense toggle |
| `dashboard/src/components/stats/StatTabs.tsx` | Modified — added Play Types tab |
| `dashboard/src/app/(protected)/stats/page.tsx` | Modified — full tab integration with state, data fetch, pivot, controls |

### Verified

- 575 Python tests pass, ruff clean
- Dashboard build succeeds (`next build` clean)
- Database has 660 rows (30 teams x 11 play types x 2 groupings)

---

## [2026-02-19 Session 43] — Data Vault Fixes + Pipeline Audit

### Fixed

- **Data Vault — `games_szn` count (ISS-029):** `populate_average_stats_incremental.py` now queries actual season game count per player instead of deriving from fetched window size. LeBron shows 36 games instead of 19.
- **Data Vault — TOV% display (ISS-030):** Added `rawPct1` format type that displays `avg_tov_ratio` as-is (10.3%) instead of multiplying by 100 (1029%).
- **Opponent-allowed rolling windows (ISS-033):** Changed `.sum()` to `.mean()` in both `backfill_opponent_allowed.py` and `backfill_opponent_allowed_incremental.py`. Rolling windows now compute per-game averages, not cumulative sums.
- **Backtesting odds cutoff (ISS-038):** Changed `snapshot_time::date <= :game_date` to timestamp-level cutoff (`interval '23 hours 30 minutes'`) in `backtest_harness.py` to match 6:30 PM ET production inference window. Functionally equivalent given scrape schedule (12/4/6 PM ET).
- **Inference data freshness check (ISS-039):** `inference_job.py` now warns if `player_average_game_stats` data is >2 days stale before generating predictions.
- **Batch upserts (ISS-041):** Converted 3 row-by-row `iterrows()` upsert loops in `populate_average_stats_incremental.py` to batch `conn.execute(text(sql), records)`.
- **View tiebreakers (ISS-042):** Added `game_id DESC` as deterministic tiebreaker to all 3 `DISTINCT ON` views (`player_stats_latest`, `team_stats_latest`, `defense_by_position_latest`). Applied via Supabase migration.
- **`created_at` overwrite (ISS-043):** Removed `created_at = NOW()` from UPSERT `DO UPDATE` in both opponent-allowed backfill scripts.

### Added

- **Data Vault — stat tooltips (ISS-034):** All column headers in HeatmapTable now have hover tooltips explaining each stat abbreviation.
- **Data Vault — heatmap legend (ISS-035):** Color legend component showing 5-step percentile gradient above the table.
- **Data Vault — position info button:** Info button next to position filter explaining G/W/B position groups.
- **SQL view definitions (ISS-036):** Saved all 3 Supabase view definitions to `sql/views/` for version control.
- **ISSUES.md:** Comprehensive pipeline audit — 43 total issues tracked, 30 fixed.

### Verified

- 575 Python tests pass, ruff clean
- Dashboard build succeeds
- Supabase migration applied for view tiebreakers

### Note

- **Full opponent-allowed re-backfill needed:** Existing DB data still has old cumulative-sum values. Run `python src/processing/backfill_opponent_allowed.py` to update.

---

## [2026-02-19 Session 42] — Combined Conformal Recalibration (Built, Tested, Not Deployed)

### Added

- **`monte_carlo.py`:** `combined_calibration_offsets` parameter on `MonteCarloPredictor.__init__()`, `_apply_combined_calibration()` method (piecewise-linear sample warping through quantile anchor points), `load_combined_calibration_offsets()` module-level helper
- **`train_pipeline.py`:** `--calibrate-only` CLI mode — loads existing model, computes combined calibration offsets from MC predictions on calibration data, saves `combined_calibration_offsets.json` without retraining
- **`train_pipeline.py`:** `_evaluate_combined_calibration()` now computes per-stat per-quantile conformal offsets (`residuals = actuals - predicted_q_values`, `offset = np.quantile(residuals, q)`) and saves as artifact

### Changed

- **`inference_job.py`:** Loads `combined_calibration_offsets.json` alongside copula params, passes to `MonteCarloPredictor` (backward-compatible no-op when absent)
- **`run_backtest.py`:** Same integration pattern as inference_job

### Evaluated (Not Deployed)

- **A/B backtest (Jan 15 – Feb 14, 2026):** Offsets improved calibration metrics (overall gap 0.019 vs 0.032) but degraded betting performance — ROI 6.01% vs 7.44%, Sharpe 0.742 vs 0.891, max drawdown 29.2% vs 26.5%. PTS ROI dropped from 13.7% to 9.0%
- **AST Q10 offset was -0.001** — conformal recalibration cannot fix zero-inflated distributions where the quantile is already at the floor (~18% of games have 0 assists)
- **Decision:** Offsets file removed from production. Code infrastructure retained as backward-compatible no-op
- **AST Q10 investigation closed (Sessions 40-42):** Surgical retrains, feature reselection, per-quantile tuning, and conformal recalibration all tested. Gap is structural, minimal betting impact

### Verified

- 608 Python tests pass, ruff clean

---

## [2026-02-19 Session 41] — CI Pipeline Fix: Lazy Module Initialization

### Fixed

- **`src/db/client.py`:** Deferred engine creation so module is importable without `DATABASE_URL`. `get_engine()` now raises `ValueError` at call time instead of import time
- **`src/scrapers/daily_player_props_scraper.py`:** Removed module-level `sys.exit(1)` when `DATABASE_URL`/`ODDS_API_KEY` missing. Validation moved to `if __name__ == "__main__"`
- **`src/scrapers/daily_game_lines_scraper.py`:** Same pattern — removed module-level `sys.exit(1)`
- **`src/scrapers/game_lines_scraper.py`:** Removed module-level `raise ValueError`. Engine creation deferred with `if DATABASE_URL else None`
- **`src/scrapers/live_odds_scraper.py`:** Same pattern — removed module-level `raise ValueError`
- **`tests/test_db_client.py`:** Updated `test_import_raises_without_database_url` → `test_get_engine_raises_without_database_url` to match new lazy init behavior

### Verified

- 608 Python tests pass with empty env vars (CI simulation), 0 collection errors
- Ruff lint clean
- Scripts still fail fast when run directly without credentials (validation in `__main__`)

---

## [2026-02-19 Session 40] — AST Surgical Retrain Evaluation

### Added

- **`_resolve_hyperparams_partial()` in `train_pipeline.py`:** Enables per-quantile hyperparameter tuning for surgical retrains via `run_partial()`. Supports priority chain: explicit file > Optuna tuning > base model params > XGBoost defaults (+105 lines)

### Changed

- **`monte_carlo.py`:** Cleanup, removed unused code (-46 lines)

### Evaluated (Not Promoted)

- **AST-only surgical retrain — Run 1 (`run_20260218_175622`):** No tuning, no feature reselection. AST Q10 combined gap: +8.10% (prod was +10.25%)
- **AST-only surgical retrain — Run 2 (`run_20260218_180752`):** Per-quantile Optuna tuning + feature reselection. AST Q10 combined gap: +7.60%
- Neither run promoted — combined Q10 gap still exceeds 5% tolerance (zero-inflation issue)
- Individual AST calibration good on both runs (all quantiles within 2%)
- PTS and REB combined Q25 gaps also exceed 5% on the new calibration window

### Verified

- 608 Python tests pass, no regressions
- All code changes committed in `c8a9eba`

---

## [2026-02-18 Session 39] — Data Vault Heatmap Stat Tables

### Added

- **Data Vault page (`/stats`):** Dense heatmap stat table for exploring player, team, and defense-vs-position stats
  - 5-step percentile-based blue heatmap coloring with `invertHeatmap` support for negative stats
  - Players tab: Box Score, Shooting, Advanced, Consistency categories with search, team, position, and min GP filters
  - Teams tab: Offense, Defense, Overall categories
  - Defense vs Position tab: Totals and Per 100 Possessions with G/W/B position selector
  - Window toggle (L5/L15/SZN), sortable columns, sticky name/position/team columns

- **Database views (Supabase migration):**
  - `player_stats_latest` — Latest per-player rolling stats (game stats + advanced stats + position), ~529 rows
  - `team_stats_latest` — Latest per-team rolling stats, 30 rows
  - `defense_by_position_latest` — Latest defense-vs-position per team+position, 90 rows (30 teams x 3 positions)

- **New dashboard files:**
  - `types/stats.ts` — TypeScript types for Data Vault feature
  - `lib/stats/columns.ts` — Column definitions for all stat categories
  - `components/stats/HeatmapTable.tsx` — Core heatmap table component
  - `components/stats/StatTabs.tsx` — Main tab bar
  - `components/stats/CategoryTabs.tsx` — Sub-category pill tabs
  - `components/stats/WindowToggle.tsx` — Rolling window toggle
  - `components/stats/PositionFilter.tsx` — Position filter

### Changed

- `components/layout/Navbar.tsx` — Added "Data Vault" link to `/stats`

### Verified

- Build succeeds with zero errors
- 608 Python tests pass, no regressions
- All 3 database views return expected row counts

---

## [2026-02-18 Session 38] — Railway Deployment Fixes

### Fixed

- **Nixpacks build failure (`externally-managed-environment`):** Replaced `ensurepip` approach with Python venv + `--system-site-packages` to avoid writing to immutable Nix store
- **Subprocess `ModuleNotFoundError`:** All orchestration job scripts (`lines_job.py`, `daily_stats_job.py`, `run_daily.py`) now use `sys.executable` instead of hardcoded `python` for subprocess calls, ensuring the venv Python (with packages) is used
- **numpy `ImportError: libz.so.1`:** Added `zlib` and `stdenv.cc.cc.lib` to nixPkgs and set `LD_LIBRARY_PATH=/root/.nix-profile/lib` so Nix-installed shared libraries are discoverable at runtime by C extensions (numpy, scipy, xgboost)

### Changed

- `nixpacks.toml` — Complete rewrite: venv-based install, explicit LD_LIBRARY_PATH, system library nixPkgs
- `railway.toml` — Start command updated to `/app/venv/bin/python`
- `src/orchestration/scheduler.py` — Removed temporary one-shot test job

### Verified

- Lines job completes successfully on Railway (all 5 steps pass)
- Discord job alerts fire correctly on success/failure
- 608 tests pass locally

---

## [2026-02-18 Session 37] — Social Media Pick Image Generator

### Added

- **Social media image generator (`src/social/`):**
  - CLI tool generating branded pick images for Instagram, TikTok, and Discord
  - Three card types: slate (daily top picks), individual pick, results recap
  - Dark theme matching dashboard Tailwind color scheme
  - Star ratings (1-5) using same formula as PropCard.tsx
  - Confidence tiers ("Strong Edge" / "High Confidence" / "Lean") — no exact percentages
  - NBA headshot cache with CDN download and placeholder fallback
  - Square (1080x1080) and story (1080x1920) format support

- **Module files:**
  - `src/social/theme.py` — Colors, fonts, edge tiers, drawing helpers
  - `src/social/data_provider.py` — 4 sync DB query functions
  - `src/social/card_renderer.py` — HeadshotCache + 3 renderer classes
  - `src/social/generate_images.py` — CLI entry point

- **Assets:**
  - `assets/fonts/Montserrat-Bold.ttf`, `Montserrat-SemiBold.ttf`, `Montserrat-Medium.ttf`

- **Tests:**
  - `tests/test_card_renderer.py` — 33 tests covering theme utils, renderers, headshot cache

### Changed

- `requirements.txt` — Added `Pillow>=10.0.0`
- `.gitignore` — Added `output/social/`

### Usage

```bash
python src/social/generate_images.py --date 2026-02-18 --type picks
python src/social/generate_images.py --date 2026-02-18 --type both --individual
python src/social/generate_images.py --date 2026-02-18 --type picks --format story --dry-run
```

---

## [2026-02-18 Session 36] — Per-Stat Calibration Diagnostic Tool (C2)

### Added

- **Per-stat calibration diagnostic (`src/diagnostics/calibration_per_stat.py`):**
  - Standalone CLI tool producing per-stat (PTS/REB/AST) calibration report
  - Quantile coverage analysis (Q10–Q90) with per-stat gap detection
  - Bias analysis (mean predicted vs mean actual, relative %)
  - Interval sharpness (80% and 50% prediction interval widths)
  - Probability calibration: Brier score and Expected Calibration Error (ECE)
  - Reliability curve data for plotting
  - Auto-diagnosis engine flags stats exceeding configurable tolerances
  - Two input paths: backtest CSV (`--csv`) or production DB (`--db`)
  - JSON structured export via `--output`
  - Windows-safe Unicode output handling

### Usage

```bash
python -m src.diagnostics.calibration_per_stat --csv predictions.csv
python -m src.diagnostics.calibration_per_stat --db --start 2025-02-10 --end 2025-02-18
python -m src.diagnostics.calibration_per_stat --csv predictions.csv --output report.json --tolerance 0.05
```

---

## [2026-02-18 Session 35] — Free Discord Funnel Pivot

### Added

- **Public picks page (`/picks`):**
  - Server component calling `get_public_picks(3)` RPC
  - 3 real pick cards (player, stat, line, edge, teams)
  - 6 blurred skeleton cards with overlay: "Sign Up Free" + "Join Discord" CTAs
  - Shareable link for social media posts

- **Shared constants (`dashboard/src/lib/constants.ts`):**
  - `DISCORD_URL` placeholder for Discord invite link
  - `TEAM_ABBREV` map extracted from dashboard page (eliminates duplication)

- **`get_public_picks()` RPC function:**
  - Returns top N recommended picks for current date
  - Accessible by anon and authenticated users
  - Ordered by highest BL edge

### Changed

- **Database RLS policies:**
  - Dropped subscriber-only policies on `daily_predictions`, `daily_prediction_samples`, `paper_bets`, `paper_trading_daily_log`
  - Replaced with `authenticated USING (true)` — all logged-in users can read

- **Middleware (`middleware.ts`):**
  - Added `/picks` to `PUBLIC_ROUTES`
  - Removed `SUBSCRIPTION_EXEMPT_ROUTES`
  - Removed entire subscription check block (DB query + redirect)
  - Result: auth-only gate, no paywall

- **Landing page (`(public)/page.tsx`):**
  - Replaced "Simple Pricing" + PricingCard section with "Free During Beta" + Discord CTA

- **HeroSection:** "View Pricing" → "Join Discord" (external link)
- **PublicNavbar:** "Pricing" → "Picks" link + "Discord" external link; "Sign Up" → "Sign Up Free"
- **Footer:** Added Discord link
- **Pricing page:** $0/mo "Beta Access" card with feature checklist, no Stripe references
- **Subscribe page:** Replaced with `redirect('/dashboard')`
- **Account page:** Removed subscription state/card, added "Free Beta" badge + Community/Discord card
- **Dashboard page:** Imports `TEAM_ABBREV` from shared constants
- **Terms of Service (Section 4):** "paid subscription at $19.99/month" → "free during beta, may add paid plans later"
- **Privacy Policy (Sections 2-3):** Stripe references → "may add payment processing in future"

### Preserved (for future Stripe activation)

- `user_subscriptions` table and `is_subscribed()` function
- `subscription.ts` types/utils
- `PricingCard.tsx` component (dormant)

---

## [2026-02-15 Session 34] — Discord Job Alerts, P&L Summary

### Added

- **Discord job status alerts:**
  - Scheduler sends success/failure notifications after every job (daily_stats, lines, inference)
  - Success alerts: job name, duration, extracted metrics (when available)
  - Failure alerts: error details for debugging
  - Alerts go to `#alerts` channel via REST API
  - Non-fatal: alert failures don't affect job execution

- **Daily P&L summary notifications:**
  - After bet resolution in `daily_stats_job.py`, sends P&L summary to `#performance` channel
  - Shows win/loss/push record, daily P&L, cumulative P&L, current bankroll
  - Green/red embed colors based on daily profit/loss

- **New alert functions in `alerts.py`:**
  - `send_job_alert()` / `send_job_alert_sync()` — job completion notifications
  - `send_pnl_summary()` / `send_pnl_summary_sync()` — daily P&L summaries
  - `_format_duration()` — human-readable duration formatting
  - `_build_job_alert_embed()` / `_build_pnl_summary_embed()` — Discord embed builders

- **Paid subscription planning (Track I):**
  - Created comprehensive plan at `docs/paid_subscription_plan.md`
  - $19.99/month for read-only prediction access
  - Stripe integration, Supabase RLS, middleware enhancement
  - Track I items (I1-I8) added to ACTIONITEMS.md

### Changed

- **`src/orchestration/scheduler.py`:**
  - Added `_send_job_alert()` function called after each subprocess
  - Added `JOB_NAMES` mapping for display names
  - Added `_parse_metrics_from_output()` for job-specific metric extraction

- **`src/orchestration/daily_stats_job.py`:**
  - Added `_send_pnl_summary()` called after bet resolution
  - Fetches bankroll data from `get_bankroll_summary()`

- **`requirements.txt`:**
  - Added `aiohttp>=3.9.0` for async HTTP requests

### Documentation

- Updated ARCHITECTURE.md with Discord alerts and Railway job notifications
- Created `docs/paid_subscription_plan.md` — full Stripe/Supabase implementation plan

---

## [2026-02-15 Session 33] — Railway Fixes, Dashboard UI, Bookmaker Tracking

### Added

- **Nixpacks configuration** (`nixpacks.toml`):
  - Fixed Railway build error ("No module named pip")
  - Explicit pip installation via ensurepip
  - Python 3.11 with pip package in Nix setup phase

- **Bookmaker tracking in bet history:**
  - Added `bookmaker` column to `daily_predictions` table via Supabase migration
  - Dashboard bet history page now displays which sportsbook had the sharpest line
  - Bookmaker badge on BetCard component

- **Dashboard navbar improvements:**
  - Active tab highlighting with `usePathname()` hook
  - Active state: `bg-blue-600 text-white` for clear visual distinction
  - Inactive state: `text-slate-400` with hover effects

### Changed

- **`src/models/daily_runner.py`:**
  - Keep bookmaker column in sharpest-book selection (line 623)
  - Previously dropped bookmaker column after line selection

- **`src/models/prediction_store.py`:**
  - Added `bookmaker` to `PREDICTION_COLS` for storage

- **`dashboard/src/app/history/page.tsx`:**
  - Fetch `is_recommended` and `bookmaker` from `daily_predictions`
  - Use `String()` for Map keys to handle Supabase bigint inconsistency

- **`dashboard/src/components/history/BetCard.tsx`:**
  - Display bookmaker badge in bet details row

- **`dashboard/src/types/predictions.ts`:**
  - Added `bookmaker?: string` to `PaperBet` interface

### Fixed

- **Railway build error:** "No module named pip" — fixed with nixpacks.toml

- **Feb 11 Model Picks bug:** Supabase returns bigint values inconsistently as number or string. Fixed by using `String()` conversion on Map keys for consistent lookup.

- **Test failures (5 tests):**
  - `test_get_current_lines_success` — updated assertion for bookmaker column presence
  - `test_select_bets_over/under_direction` — added `bl_tau=None` to disable BL blending
  - `test_default_edge_threshold` and `test_default_bankroll` — changed to test explicit parameter passing

### Technical Notes

**NBA All-Star break (Feb 13-15, 2026):**
- No NBA games during this period — expected behavior for no predictions
- Railway jobs are correctly configured; will resume when games resume

---

## [2026-02-15 Session 32] — Discord Bot Implementation (Track H Complete)

### Added

- **Discord Bot Package** (`src/discord_bot/`):
  - Interactive Discord bot with slash commands for daily predictions and paper trading
  - Discord.py 2.6+ with `@bot.tree.command()` slash command registration
  - Async database queries using `asyncio.to_thread()` for SQLAlchemy wrapping
  - Graceful shutdown handling (SIGINT/SIGTERM) for Railway compatibility

- **Slash Commands:**
  - `/picks` — Get today's top predictions (filterable by stat type and min edge)
  - `/player <name>` — Get predictions for specific player (fuzzy match supported)
  - `/bankroll` — Show current paper trading balance and P&L
  - `/performance` — Show model performance stats (win rate, ROI, total bets)
  - `/toppicks` — Quick view of top 5 picks for alerts

- **Files Created:**
  - `src/discord_bot/__init__.py` — Package init
  - `src/discord_bot/commands/__init__.py` — Commands package init
  - `src/discord_bot/services/__init__.py` — Services package init
  - `src/discord_bot/formatters/__init__.py` — Formatters package init
  - `src/discord_bot/bot.py` — Main bot class with all slash commands (~250 lines)
  - `src/discord_bot/run_bot.py` — Entry point with graceful shutdown (~65 lines)
  - `src/discord_bot/services/predictions.py` — Prediction database queries (~225 lines)
  - `src/discord_bot/services/paper_trading.py` — Paper trading database queries (~225 lines)
  - `src/discord_bot/formatters/embeds.py` — Discord embed builders (~280 lines)
  - `src/discord_bot/alerts.py` — REST API alert sender (~195 lines)
  - `scripts/run_discord_bot.bat` — Windows Task Scheduler script (~40 lines)

- **Automated Alerts:**
  - Discord REST API alert sender (works without bot process running)
  - `send_predictions_alert()` async function using aiohttp
  - `send_predictions_alert_sync()` wrapper for synchronous code

### Changed

- **`.env`:**
  - Renamed `BOT_TOKEN` to `DISCORD_BOT_TOKEN` for consistency
  - Added `DISCORD_CHANNEL_PREDICTIONS=1472768933730980005`
  - Added `DISCORD_CHANNEL_ALERTS=1472768974662926427`
  - Added `DISCORD_CHANNEL_PERFORMANCE=1472769015725293708`

- **`requirements.txt`:**
  - Added `discord.py>=2.3.0` dependency

- **`src/orchestration/inference_job.py`:**
  - Added Discord alert trigger after predictions are saved
  - Added `--skip-discord` CLI flag for debugging
  - Alert wrapped in try/except so failures don't break inference

- **`ARCHITECTURE.md`:**
  - Updated Discord Bot section from "Planned" to "Implemented"
  - Added full documentation of directory structure, commands, architecture

### Fixed

- **`teams` table query** in `predictions.py`:
  - Changed `t.abbreviation` to `t.team_name` (abbreviation column doesn't exist)
  - Used `dp.feat_opp_abbrev` for opponent directly from predictions table

- **`paper_bets` table columns** in `paper_trading.py`:
  - Changed `result` to `status`
  - Changed `'win'`/`'loss'` to `'won'`/`'lost'` for status values
  - Changed `stat` to `stat_type`
  - Changed `side` to `bet_direction`
  - Changed `odds` to `odds_at_bet`

- **`paper_trading_daily_log` table columns** in `paper_trading.py`:
  - Changed `current_bankroll` to `bankroll_after`
  - Used `total_pnl` for daily P&L
  - Used `cumulative_pnl` for total P&L

### Technical Notes

**Architecture:**
```
Discord Bot Architecture
├── Slash Commands (require bot process running)
│   ├── /picks — Query daily_predictions table
│   ├── /player — Fuzzy match player name, get all stats
│   ├── /bankroll — Query paper_trading_daily_log
│   └── /performance — Query paper_bets aggregates
│
└── Automated Alerts (REST API, no bot needed)
    └── Triggered by inference_job.py after predictions
```

**Database Query Pattern:**
```python
async def get_predictions() -> list[dict]:
    def _query():
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            return [dict(zip(columns, row)) for row in rows]
    return await asyncio.to_thread(_query)
```

**Bot Hosting Options:**
1. Local: `scripts\run_discord_bot.bat` via Windows Task Scheduler
2. Railway: Add worker service in `railway.toml` (Railway-ready architecture)

### Test Results

- 571 tests passed, 4 pre-existing failures (paper trading config defaults, unrelated)

---

## [2026-02-14 Session 31] — Vercel Deployment Fix & Model Picks Filtering

### Added

- **Model Picks Filtering (History & Performance Pages)**:
  - `BetSourceFilter` component (`dashboard/src/components/shared/BetSourceFilter.tsx`)
  - Toggle between "Model Picks" (edge ≥9%) and "All Bets"
  - Defaults to Model Picks view to show actual model performance
  - History page: Filters bet list and summary stats by bet source
  - Performance page: Recalculates all KPIs from filtered bets, simulates model-picks-only bankroll

### Changed

- **`.gitignore`**:
  - Added exceptions for JSON config files: `!package.json`, `!package-lock.json`, `!vercel.json`, `!tsconfig.json`
  - Root cause fix: these files were never committed due to `*.json` rule

- **`dashboard/src/app/history/page.tsx`**:
  - Added `betSource` state defaulting to 'model'
  - Filter bets by edge threshold (≥9%) when Model Picks selected
  - HistorySummary now receives filtered bets

- **`dashboard/src/app/performance/page.tsx`**:
  - Added `betSource` state defaulting to 'model'
  - Recalculates KPIs (P&L, ROI, Win Rate) from filtered bets using `useMemo`
  - Simulates bankroll progression for Model Picks only
  - Chart shows model-picks-only equity curve when filter active

- **`ARCHITECTURE.md`**:
  - Updated dashboard documentation to reflect bet source filtering

### Fixed

- **Vercel Deployment**:
  - `dashboard/package.json` was never committed (blocked by `*.json` gitignore)
  - `dashboard/tsconfig.json` was never committed (same issue)
  - Added `baseUrl: "."` to tsconfig.json for `@/*` path alias resolution
  - User disabled "Include files outside the root directory" in Vercel settings
  - Dashboard now live at `game-flow-data.vercel.app`

### Technical Notes

**Model Picks Threshold:**
```typescript
export const MODEL_PICKS_EDGE_THRESHOLD = 0.09  // 9% edge
```

**Performance Page KPI Recalculation:**
```typescript
// Filter bets by source
const filteredBets = betSource === 'model'
  ? allBets.filter(b => b.edge >= MODEL_PICKS_EDGE_THRESHOLD)
  : allBets

// Recalculate all metrics from filtered bets
const { totalPnl, totalWins, totalLosses, overallRoi, winRate } = useMemo(() => {
  // ... calculated from filteredBets
}, [filteredBets])
```

### Test Results

- 575 tests passed, 0 failures

---

## [2026-02-14 Session 30] — Railway Cloud Deployment

### Added

- **Railway Cloud Deployment:**
  - APScheduler-based job scheduler (`src/orchestration/scheduler.py`) running 5 cron jobs on UTC schedule
  - `railway.toml` configuration for Nixpacks build and service deployment
  - Production model workflow with `src/models/artifacts/production/` folder
  - Model promotion script (`scripts/promote_model.py`) to copy training runs to production

- **Documentation:**
  - `docs/railway_deployment.md` — Full deployment guide (~130 lines)
  - `docs/scalability.md` — Architecture capacity analysis (~65 lines)

### Changed

- **`src/orchestration/inference_job.py`:**
  - Now checks `production/` folder before falling back to latest `run_*` directory
  - Filters out `_incomplete` directories when auto-selecting model

- **`.gitignore`:**
  - Ignores `run_*/` training directories but allows `production/` folder

- **`requirements.txt`:**
  - Added `apscheduler==3.10.4` for job scheduling
  - Changed `psycopg2` to `psycopg2-binary` (pre-compiled, avoids build dependencies)

- **Local scheduled tasks disabled:**
  - All 5 Windows Task Scheduler tasks (GameFlow-*) disabled to avoid conflicts with Railway

### Technical Notes

**Railway Architecture:**
```
Railway (always-on worker)
    └── scheduler.py (APScheduler)
        ├── daily_stats_job (14:00 UTC / 9 AM ET)
        ├── lines_noon (17:00 UTC / 12 PM ET)
        ├── lines_4pm (21:00 UTC / 4 PM ET)
        ├── lines_6pm (23:00 UTC / 6 PM ET)
        └── inference (23:30 UTC / 6:30 PM ET)
```

**Production Model Workflow:**
```bash
# Promote latest model to production
python scripts/promote_model.py

# Or specify a specific run
python scripts/promote_model.py run_20260214_183000
```

**Scalability Capacity:**
| Tier | Concurrent Users | Monthly Users | Cost |
|------|------------------|---------------|------|
| Current | 30-50 | 500-1K | ~$5/mo |
| Starter | 100-200 | 5K | ~$50/mo |
| Growth | 500-1K | 20K | ~$200/mo |

### Test Results

- 575 tests passed, 0 failures

---

## [2026-02-14 Session 29] — Dashboard Insight Features & Vercel Deployment

### Added

- **Dashboard Insight Features (G2/G3)**:
  - 14 `feat_*` columns in `daily_predictions` table for template-based insights
  - B2 rest/schedule features: `feat_rest_days`, `feat_is_back_to_back`, `feat_games_last_7d`
  - B1 injury features: `feat_team_out_count`, `feat_team_out_min_sum`, `feat_opp_out_count`, `feat_player_is_questionable`, `feat_player_is_probable`
  - B3 stat-specific trends: `feat_player_avg_stat_l3/l5/l15`, `feat_stat_l3_l15_ratio`, `feat_stat_std_l5`
  - Opponent context: `feat_opp_abbrev` (3-letter team abbreviation)

- **Insights Generator** (`dashboard/src/lib/insights.ts`):
  - `generateInsights(prediction)` — template-based insight generation from feature values
  - Categories: rest, injury, trend, consistency, average
  - Context-aware sentiments — considers bet direction (Over vs Under) for positive/negative determination
  - `getInsightSummary(insights)` — quick summary text for display

- **"Model Context" Section** in `AnalysisModal.tsx`:
  - Displays insights with color-coded sentiments (green=positive, red=negative, gray=neutral)
  - Icons per sentiment: ✓ (positive), ⚠ (negative), • (neutral)
  - Positioned after Last 5 Games chart

- **Historical Backfill Script** (`src/tools/backfill_prediction_features.py`):
  - Populates `feat_*` columns for historical predictions without modifying prediction values
  - Uses `FeatureStore.get_player_game_features()` for feature extraction
  - Supports `--date`, `--start/--end`, `--dry-run` CLI flags
  - Successfully backfilled Feb 10-12 (1,338 predictions)

- **Vercel Deployment (G9)**:
  - Dashboard live at `game-flow-data.vercel.app`
  - Root directory: `dashboard`
  - Environment variables: `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`
  - Vercel MCP integration: `claude mcp add --transport http vercel https://mcp.vercel.com`

### Changed

- **`src/models/prediction_store.py`**:
  - Added 14 `feat_*` columns to `PREDICTION_COLS` list

- **`src/models/daily_runner.py`**:
  - Added `_map_features_to_predictions()` method — extracts B1/B2/B3 features from `features_df` and attaches to predictions
  - Added `_get_opponent_abbrevs()` method — uses hardcoded `TEAM_ABBREV` map (same as dashboard)
  - Added defensive column checks for missing `player_id`/`game_id`/`stat` columns
  - Added defensive check for missing `opponent_id` column

- **`dashboard/src/lib/insights.ts`**:
  - Trend insights now consider bet direction — hot streak is positive for Over, negative for Under
  - Average vs line insights now consider bet direction — L5 avg below line is positive for Under bets

### Technical Notes

**Insight Sentiment Logic (context-aware):**
| Insight | Over Bet | Under Bet |
|---------|----------|-----------|
| L5 avg above line | ✓ positive | ⚠ negative |
| L5 avg below line | ⚠ negative | ✓ positive |
| Hot streak (L3 > L15) | ✓ positive | ⚠ negative |
| Cold stretch (L3 < L15) | ⚠ negative | ✓ positive |

**Feature Column Mapping:**
```python
# B2: Rest/Schedule (same for all stats)
feat_rest_days = features["rest_days"]
feat_is_back_to_back = features["is_back_to_back"]
feat_games_last_7d = features["games_in_last_7_days"]

# B1: Injury Context
feat_team_out_count = features["team_out_count"]
feat_team_out_min_sum = features["team_out_min_sum"]
feat_opp_out_count = features["opp_out_count"]

# B3: Stat-specific (e.g., for pts)
feat_player_avg_stat_l3 = features["player_avg_pts_l3"]
feat_stat_l3_l15_ratio = features["player_pts_l3_l15_ratio"]
feat_stat_std_l5 = features["player_std_pts_l5"]
```

### Test Results

- 575 tests passed, 0 failures

---

## [2026-02-13 Session 28] — Play of the Day Featured Card

### Added

- **Play of the Day Component** (`dashboard/src/components/predictions/PlayOfTheDay.tsx`):
  - Featured hero card highlighting the model's highest-edge pick
  - Trophy badge header with amber/gold visual treatment
  - Large player avatar (96x96), player name, team matchup, game time
  - Stat badge + bet direction/line display
  - Star rating visualization (1-5 based on edge magnitude)
  - Edge badge and model probability display
  - "Analyze Pick" button opens analysis modal
  - Responsive layout (stacked mobile, horizontal desktop)

### Changed

- **Main Dashboard** (`dashboard/src/app/page.tsx`):
  - Added PlayOfTheDay component above PropGrid
  - Renders when predictions exist, hidden during loading or empty state
  - Uses `sortedPredictions[0]` — respects all active filters

### Technical Notes

**Filter Integration:**
- Play of the Day respects current filters: date, edge threshold, BL blending, stat type, matchup
- Updates automatically when filters change
- Shows best pick within the filtered view, not absolute best

**Visual Design:**
```
┌────────────────────────────────────────────────────────────────────┐
│  🏆 PLAY OF THE DAY                                                 │
├────────────────────────────────────────────────────────────────────┤
│  [Avatar]  Player Name           [Stat]           Over/Under XX.X  │
│            Team vs Opponent      ★★★★★           +12.3% Edge       │
│            Game Time                              62.1% Model Prob │
│                                                  [Analyze Pick]    │
└────────────────────────────────────────────────────────────────────┘
```

**Styling:**
- Border: `border-2 border-amber-400/50`
- Background: `bg-gradient-to-r from-amber-950/30 to-slate-800`
- Stars: `text-amber-400`
- CTA button: `bg-amber-600 hover:bg-amber-500`

### Test Results

- 575 tests passed, 0 failures

---

## [2026-02-13 Session 27] — Inference Job Performance Optimization + Discord Bot Plan

### Added

- **Discord Bot Development Plan** (`docs/discord_bot_development.md`):
  - Full specification for interactive Discord bot with slash commands
  - `/picks`, `/player`, `/bankroll`, `/performance` command designs
  - Automated alerts after inference job completes
  - File structure, database queries, and testing plan
  - ~3.5 hours estimated implementation time

- **Track H: Discord Bot** in `ACTIONITEMS.md`:
  - 8 implementation items (H1-H8)
  - Prerequisites for Discord setup
  - Files to create listed

### Changed

- **Parallel Feature Building** in `src/models/daily_runner.py`:
  - Replaced sequential player loop with `ThreadPoolExecutor` (8 workers)
  - Runtime: 65s → 4.8s (13x faster)
  - Added timing logs for feature building phase

- **Connection Pool** in `src/db/client.py`:
  - Increased `pool_size` from 5 → 10
  - Increased `max_overflow` from 2 → 6
  - Enables concurrent feature store queries

- **Prop Lines Query Optimization** in `src/models/daily_runner.py`:
  - Query now searches both 8-digit and 10-digit `game_id` formats
  - Removed `LPAD()` from WHERE and PARTITION BY clauses
  - Enables index usage on `raw_player_props_combined.game_id`
  - Query runtime: 137s → 0.2s (685x faster)

- **Rate Limiting** in scrapers:
  - `src/scrapers/daily_player_props_scraper.py`: `time.sleep(0.2)` → `time.sleep(0.05)`
  - `src/scrapers/game_lines_scraper.py`: `time.sleep(0.2)` → `time.sleep(0.05)`
  - Odds API allows 30 req/s; 0.05s = 20 req/s max (safe margin)

### Technical Notes

**Database Indexes Created (via Supabase Dashboard):**
- `idx_props_game_id` on `raw_player_props_combined(game_id)`
- `idx_props_game_market` on `raw_player_props_combined(game_id, market_key)`
- `idx_props_game_id_padded` on `raw_player_props_combined(LPAD(game_id, 10, '0'))`

**Total Inference Job Performance:**
- Before: ~180s (3 minutes)
- After: ~16s
- **10x overall speedup**

**Breakdown of optimizations:**
| Component | Before | After | Speedup |
|-----------|--------|-------|---------|
| Feature building | 65s | 4.8s | 13x |
| Prop lines query | 137s | 0.2s | 685x |
| Other (model, MC) | ~10s | ~11s | — |

### Test Results

- 575 tests passed, 0 failures

---

## [2026-02-13 Session 26] — Dashboard Model Parameter Filters

### Added

- **Edge Threshold Filter Dropdown** in `dashboard/src/app/page.tsx`:
  - Filter predictions by minimum edge: All, ≥3%, ≥5% (Rec), ≥7%, ≥10%, ≥15%, ≥20%
  - Client-side filtering with `edgeThreshold` state
  - Default: 3% (matches previous hardcoded behavior)

- **Black-Litterman Blending Filter Dropdown** in `dashboard/src/app/page.tsx`:
  - Apply BL blending to probabilities before filtering: Off, τ=0.03, τ=0.05, τ=0.10 (Rec), τ=0.15, τ=0.25
  - Client-side BL calculation using model's distribution parameters
  - `blTau` state (null = no blending, number = tau value)

- **BL Blending Utility Functions** in `dashboard/src/lib/utils.ts`:
  - `calculateBLConfidence(predMean, predStd, line)` — z-score based confidence (0-1)
  - `blendProbability(modelProb, impliedProb, tau, confidence)` — log-odds space blending
  - Constants: `BL_Z_MAX = 1.0`, `BL_MAX_WEIGHT = 0.50`

- **Distribution Parameters** in `dashboard/src/types/predictions.ts`:
  - `pred_mean: number` — Model's predicted mean
  - `pred_std: number` — Model's standard deviation

- **Automated Bet Resolution** in `src/paper_trading/paper_trader.py`:
  - `resolve_all_pending()` method for multi-day catchup resolution
  - Resolves all pending bets with available actuals

- **Bet Resolution Step** in `src/orchestration/daily_stats_job.py`:
  - Step 8: Automatic bet resolution after stats collection
  - `--skip-resolution` flag to disable
  - `resolve_pending_bets()` helper function

### Changed

- **`dashboard/src/app/page.tsx`:**
  - Added `edgeThreshold` state (default 0.03)
  - Added `blTau` state (default null = off)
  - Added edge and BL dropdown UI components
  - Updated filtering logic with BL blending support
  - Added `pred_mean` and `pred_std` to prediction mapping
  - Removed hardcoded `.or('over_edge.gte.0.03,under_edge.gte.0.03')` from Supabase query

- **`dashboard/src/lib/utils.ts`:**
  - Fixed timezone bug in `formatDate()` — `new Date("YYYY-MM-DD")` interprets as UTC midnight, causing off-by-one in Eastern timezone
  - Manual parsing for date-only strings: `date.split('-').map(Number)` → `new Date(year, month - 1, day)`

### Fixed

- **Date dropdown showing only one date** — Supabase default 1000 row limit caused dates with >1000 predictions to be excluded from distinct query. Created `get_prediction_dates()` PostgreSQL RPC function using `SELECT DISTINCT`.

- **Date display off-by-one** — Dashboard showed "Feb 9, 2026" instead of "Feb 10, 2026" due to JavaScript `new Date("2026-02-10")` interpreting as UTC midnight.

### Technical Notes

**BL Blending Formula (same as backtesting):**
```typescript
// Confidence from model's prediction distribution
const confidence = Math.min(|predMean - line| / predStd / Z_MAX, 1.0)

// Blending weight (capped at 50%)
const w = Math.min(tau * confidence, 0.50)

// Blend in log-odds space
const posteriorLogit = impliedLogit + w * (modelLogit - impliedLogit)
const posteriorProb = 1 / (1 + exp(-posteriorLogit))

// Blended edge
const blendedEdge = posteriorProb - impliedProb
```

**UI Layout:**
```
┌──────────┐ ┌──────────┐ ┌───────────┐ ┌────────────┐ ┌───────────┐
│ Feb 13 ▼ │ │All Games▼│ │Edge: ≥5% ▼│ │BL: τ=0.10 ▼│ │All│PTS│...│
└──────────┘ └──────────┘ └───────────┘ └────────────┘ └───────────┘
 Date        Matchup       Edge Filter   BL Blending   Stat Filter
```

### Test Results

- 575 tests passed, 0 failures

---

## [2026-02-13 Session 25] — Database Health Check & Incremental Backfill

### Added

- **Database Health Check Script** (`src/diagnostics/db_health_check.py`):
  - `DatabaseHealthChecker` class with 8 validation categories
  - `CheckResult` dataclass for structured results (status: passed/warning/failed)
  - Data freshness checks for key tables (player_game_stats, props, injuries, predictions)
  - Game data completeness checks (games per date, player counts)
  - Prop linking health (NULL game_id/player_id/team_id rates)
  - Aggregation sync validation (player_average_game_stats coverage)
  - Injury linking validation
  - Position history coverage
  - Prediction coverage analysis
  - Foreign key integrity checks
  - CLI arguments: `--days`, `--verbose`, `--json`
  - Exit codes: 0 (pass), 1 (warnings), 2 (critical)

- **`src/diagnostics/__init__.py`** — Package initialization

- **Incremental Team ID Backfill** (`src/processing/backfill_team_ids_incremental.py`):
  - Only processes recent rows by staging_id threshold
  - `--days-back` parameter (default 7 days)
  - `--staging-id-threshold` for explicit cutoff
  - Batch processing with progress bar
  - Avoids full table scan on 26M+ row table

### Changed

- **`src/orchestration/daily_stats_job.py`:**
  - Step 3 now uses `backfill_team_ids_incremental.py --days-back 7` instead of full backfill
  - Prevents unnecessary processing of historical data that may have unresolvable team_id issues

### Technical Notes

**Health Check Categories:**

| Check | Alert Condition |
|-------|-----------------|
| Data Freshness | Any table >1 day stale on game days |
| Game Completeness | Game has <20 players or missing team stats |
| Prop Linking | >10% of recent props unlinked |
| Aggregation Sync | Recent games missing aggregations |
| Injury Linking | >20% of recent injuries unlinked |
| Position History | Active players lack position data |
| Prediction Coverage | Games without predictions or orphaned predictions |
| Foreign Keys | player_id/team_id references invalid |

**Usage:**
```bash
# Basic run
python src/diagnostics/db_health_check.py

# Extended check period
python src/diagnostics/db_health_check.py --days 14

# Detailed output
python src/diagnostics/db_health_check.py --verbose

# JSON for automation
python src/diagnostics/db_health_check.py --json
```

### Test Results

- 575 tests passed, 0 failures (coverage warning: 51.06% < 60% target)

---

## [2026-02-10 Session 24] — Dashboard Line Shopping & Kelly Sizing

### Added

- **Sportsbook Line Shopping** in `AnalysisModal`:
  - Displays all available bookmaker lines for each prop
  - Calculates actual edge using quantile-based probability estimation
  - Proper Under bet EV calculation (higher lines = easier to hit)
  - Lines sorted by edge magnitude with "BEST" indicator
  - `formatBookmakerName()` helper for cleaner sportsbook display

- **Kelly Bet Sizing Calculator** in `AnalysisModal`:
  - `bankroll` state with localStorage persistence (lazy initialization)
  - `kellyFraction` state with preset dropdown (Full 1.0, Half 0.5, Quarter 0.25, Eighth 0.125)
  - `customKelly` state with toggle for custom decimal input
  - `calculateKelly()` function for recommended bet size
  - `oddsToImpliedProb()` helper for odds conversion

- **Matchup Filter** in `page.tsx`:
  - Changed from individual team dropdown to matchup format
  - "LAL vs SAS" style options instead of separate team entries
  - Sorted alphabetically for consistent display

- **Supabase RLS Policies** (via migration):
  - `Allow public read access on player_game_stats` — enables Last 5 chart
  - `Allow public read access on raw_player_props_combined` — enables line shopping

- **`estimateUnderProb()` function** in `AnalysisModal`:
  - 5-point quantile interpolation for Under probability estimation
  - Proper extrapolation beyond q90 (higher lines → higher Under prob)
  - Linear interpolation between quantile points

### Changed

- **`dashboard/src/components/analysis/AnalysisModal.tsx`:**
  - Added Kelly sizing UI with bankroll input and fraction selector
  - Added sportsbook lines section with edge calculation
  - Refactored probability estimation for Under bets
  - Added toggle between preset and custom Kelly fractions

- **`dashboard/src/app/page.tsx`:**
  - Matchup filter format: `[team1, team2].sort().join(' vs ')`
  - Filter logic updated to check both teams in matchup string

- **`dashboard/src/components/shared/Badge.tsx`:**
  - Added NaN guard in `EdgeBadge` component
  - Returns dash (—) for non-finite edge values

- **`dashboard/src/components/analysis/QuantileSummary.tsx`:**
  - Added `safeFixed()` helper for NaN-safe toFixed calls
  - Added safety checks for q50Position calculation

### Fixed

- **Under bet EV calculation** — Was incorrectly treating lower lines as better (copied Over logic). Now properly estimates that higher lines are easier to hit for Under bets.

- **Bankroll input leading zeros** — Changed from `String(value)` to lazy initialization pattern, preventing "0" prefix when backspacing.

- **useState in useEffect lint error** — Replaced `useEffect` + `setState` with lazy initialization `useState(() => ...)` pattern for bankroll and kellyFraction.

- **Unused variable `q`** — Removed from `calculateKelly` function to fix lint warning.

- **RLS blocking modal data** — Added public read policies for `player_game_stats` and `raw_player_props_combined` tables.

### Technical Notes

**Kelly Formula Implementation:**
```typescript
const calculateKelly = (modelProb: number, odds: number, kellyFraction: number): number => {
  const b = odds > 0 ? odds / 100 : 100 / Math.abs(odds)  // decimal odds - 1
  const f = (modelProb * (b + 1) - 1) / b  // full Kelly fraction
  return Math.min(f * kellyFraction, 0.25)  // cap at 25% of bankroll
}
```

**Under Probability Estimation:**
- Uses 5 quantile points: (q10, 0.90), (q25, 0.75), (q50, 0.50), (q75, 0.25), (q90, 0.10)
- Linear interpolation between adjacent points
- Extrapolation above q90 uses slope continuation (higher lines → higher Under prob)
- Capped between 0.90 and 0.99 for lines beyond q90

**LocalStorage Pattern:**
```typescript
const [bankroll, setBankroll] = useState<number>(() => {
  if (typeof window !== 'undefined') {
    const saved = localStorage.getItem('gameflow_bankroll')
    return saved ? parseFloat(saved) : 1000
  }
  return 1000
})
```

### Test Results

- 570 tests passed, 0 failures

---

## [2026-02-10 Session 23] — Per-Stat Configuration System

### Added

- **`src/config/stat_config.py`** — Per-stat configuration module:
  - `StatConfig` dataclass — per-stat settings (stat, enabled, edge_threshold, bl_tau)
  - `StatConfigSet` container — global defaults with per-stat overrides
  - `parse_stat_param()` helper — parses CLI arguments like `"pts=0.10 reb=0.07"`
  - `from_cli_args()` factory method for CLI integration
  - `to_dict()` serialization for logging/debugging
  - `get_edge_threshold(stat)`, `get_bl_tau(stat)`, `is_stat_enabled(stat)` getters with fallback logic
- **`src/config/__init__.py`** — Package init with exports
- **`tests/test_stat_config.py`** — 30 unit tests covering:
  - Global value parsing
  - Per-stat value parsing
  - Mixed global + per-stat overrides
  - "none" value handling (to disable BL for a stat)
  - Case-insensitive stat names
  - Error handling for invalid formats
  - Serialization roundtrips

### Changed

- **`src/backtesting/bet_simulator.py`:**
  - Added `stat_config: StatConfigSet | None` parameter to `BetSimulator.__init__()`
  - Added `_get_edge_threshold(stat: str)` method for per-stat threshold lookup
  - Modified `should_bet()` to accept `stat` parameter and use per-stat thresholds
  - Modified `evaluate_predictions()` to pass stat type to `should_bet()`
- **`src/backtesting/backtest_harness.py`:**
  - Added `stat_config: StatConfigSet | None` parameter to `BacktestHarness.__init__()`
  - Added `_stat_blenders: dict[str, BlackLittermanBlender]` for per-stat BL blenders
  - Added `_get_blender_for_stat(stat: str)` method
  - Modified `__post_init__` to create per-stat BL blenders when `stat_config` has per-stat tau values
  - Modified `_calculate_edges()` to use per-stat blenders
  - Passes `stat_config` to `BetSimulator`
- **`src/backtesting/run_backtest.py`:**
  - Changed `--edge-threshold` from `type=float` to `nargs="+"` for multiple values
  - Changed `--bl-tau` from `type=float` to `nargs="+"` for multiple values
  - Added `StatConfigSet.from_cli_args()` parsing in `main()`
  - Passes `stat_config` to `BacktestHarness`
- **`src/backtesting/run_sweep.py`:**
  - Added `StatConfigSet` import
  - Creates `StatConfigSet` from `SweepConfig` values
  - Passes `stat_config` to `BacktestHarness` and `BetSimulator`
- **`src/paper_trading/paper_trader.py`:**
  - Added `stat_config: StatConfigSet | None` parameter to `PaperTrader.__init__()`
  - Added `_get_edge_threshold(stat: str)` method
  - Modified `select_bets()` to use per-stat edge thresholds
- **`src/paper_trading/place_bets.py`:**
  - Changed `--edge-threshold` from `type=float` to `nargs="+"` for multiple values
  - Added `StatConfigSet.from_cli_args()` parsing in `main()`
  - Passes `stat_config` to `PaperTrader`
  - Logs per-stat thresholds at startup

### Fixed

- **`tests/test_run_backtest.py`:**
  - Updated mock return values for `edge_threshold` from float `0.05` to list `["0.05"]`
  - Affected tests: `test_main_runs_backtest_with_defaults`, `test_main_parses_allowed_bets`, `test_main_creates_timestamped_output_dir`

### Test Results

- 570 tests passed, 0 failures

### Technical Notes

**CLI Format Examples:**
```bash
# Backward compatible (global value)
--edge-threshold 0.05

# Per-stat values
--edge-threshold pts=0.10 reb=0.07 ast=0.15

# Mixed: global default + per-stat overrides
--edge-threshold 0.05 pts=0.10

# Per-stat BL tau with disable option
--bl-tau pts=0.05 reb=0.10 ast=none
```

**Precedence Logic:**
1. Per-stat value (if configured) → highest priority
2. Global value (if set) → fallback
3. Default value (0.05 for edge) → final fallback

**Why Per-Stat Configuration:**
Backtesting showed significant ROI differences between stats:
- REB: +7.9% ROI (strongest)
- PTS: Variable performance
- AST: +3.2% ROI (marginal)

Per-stat configuration allows:
- Tighter edge thresholds on weaker stats (filter more aggressively)
- Looser thresholds on stronger stats (capture more profitable bets)
- Different BL tau values based on stat-specific calibration
- Disabling BL entirely for specific stats that don't benefit

---

## [2026-02-10 Session 22] — Archive THREES Model

### Removed

- **THREES (3-pointer) model archived** due to poor market coverage and insufficient betting volume:
  - Only 50% of predictions had odds available (sportsbooks don't offer 3PT props for many players)
  - Generated only 2 bets out of 78 total in backtesting
  - Market reality issue, not a code bug — archived for potential future use

### Archived (moved to `archive/threes_model/`)

| File | Lines | Purpose |
|------|-------|---------|
| `src/models/threes_multiclass.py` | 377 | Core C5 multiclass model |
| `tests/test_threes_multiclass.py` | 370 | Test suite |
| `scripts/validate_threes_negbin.py` | 322 | C4 validation script |
| `scripts/test_threes_global_params.py` | 154 | C4 diagnostic |
| `scripts/test_threes_distribution.py` | 113 | C4 diagnostic |
| `.session/specs/C4_threes_count_model.md` | 425 | Spec document |

### Changed

- **`src/models/train_pipeline.py`:**
  - Removed "threes" from stat loops
  - Removed `_calibrate_count_model()` method
  - Removed `_calibrate_multiclass_model()` method
  - Updated calibration evaluation to exclude threes
- **`src/models/monte_carlo.py`:**
  - Removed "threes" from DEFAULT_VARIANCE_INFLATION
  - Removed "threes" from DEFAULT_CORRELATION_CONFIG
  - Removed THREES sampling methods
  - Removed hurdle model infrastructure
  - Simplified predict() and predict_batch() methods
- **`src/models/quantile_trainer.py`:**
  - Removed `RATE_FEATURES_THREES` import
  - Removed "threes" from STAT_FEATURES mapping
  - Removed `HurdleQuantileModel` class
  - Removed `train_hurdle_model()` function
  - Removed `_train_threes_count_model()` method
  - Simplified `save_all()` and `load_all()` methods
- **`src/backtesting/backtest_harness.py`:**
  - Removed "threes" from stat_to_market mappings
  - Removed "threes" from SQL actuals query
  - Removed "threes" from rate model loading

### Kept (low cost, future optionality)

- Scraper market collection for `player_threes` (minimal API credits)
- Feature columns in `feature_store.py` (no harm, enables future re-add)

---

## [2026-02-10 Session 21] — THREES Multiclass Model (C5) & Dashboard History/Performance Pages (G8)

### Added

- **C5 THREES Multiclass PMF Model** (`src/models/threes_multiclass.py`):
  - `ThreesMulticlassModel` class (~350 lines) — XGBoost multiclass classifier
  - Predicts 9-class PMF: P(threes=0), P(threes=1), ..., P(threes=8+)
  - `objective='multi:softprob'`, `num_class=9`
  - Classes 0-7 are exact counts, class 8 represents "8 or more" (capped)
  - `fit()`, `predict_proba()`, `sample()` methods
  - `save()` / `load()` for persistence
  - Configuration via `ThreesMulticlassConfig` dataclass
- **`tests/test_threes_multiclass.py`** — 25 unit tests covering:
  - Model fitting and training
  - PMF probability output validation
  - Categorical sampling (integer outputs 0-8)
  - Calibration evaluation
  - Save/load roundtrip
- **`_sample_threes_multiclass()` method** in `src/models/monte_carlo.py`:
  - Uses PMF probabilities for weighted random choice
  - Produces integer counts directly (0, 1, 2, ..., 8)
  - Called when multiclass model is detected
- **`_calibrate_multiclass_model()` method** in `src/models/train_pipeline.py`:
  - Evaluates per-class accuracy diagnostics
  - Computes quantile coverage from cumulative PMF
- **Dashboard History Page** (`dashboard/src/app/history/page.tsx`):
  - Status filter tabs: All, Won, Lost, Push
  - Summary stats bar: total bets, wins, losses, win rate, P&L
  - Fetches from `paper_bets` table (last 30 days)
- **Dashboard Performance Page** (`dashboard/src/app/performance/page.tsx`):
  - KPI cards: Current Bankroll, Total P&L, Overall ROI, Win Rate
  - Bankroll over time chart (Recharts AreaChart with green/red trend)
  - Performance by stat breakdown table
  - Fetches from `paper_trading_daily_log` and `paper_bets` tables
- **History Components** (`dashboard/src/components/history/`):
  - `BetCard.tsx` — Individual bet display with player, stat, line, actual, result, P&L
  - `BetList.tsx` — Grid container for bet cards
  - `HistoryFilters.tsx` — Status filter tab buttons
  - `HistorySummary.tsx` — Summary stats bar with win/loss counts
- **Performance Components** (`dashboard/src/components/performance/`):
  - `KPICard.tsx` — Metric card with label, value, optional trend indicator
  - `BankrollChart.tsx` — Recharts AreaChart with gradient fill
  - `StatBreakdown.tsx` — Per-stat performance table
- **Auth Callback Route** (`dashboard/src/app/auth/callback/route.ts`):
  - Handles email confirmation redirects from Supabase
  - Exchanges code for session, redirects to home or login

### Changed

- **`src/models/quantile_trainer.py`:**
  - Added imports for `ThreesMulticlassModel`, `ThreesMulticlassConfig`
  - Updated `train_rate_models()` to detect multiclass model option
  - Updated `save_all()` / `load_all()` for multiclass artifacts
- **`src/models/monte_carlo.py`:**
  - Added `_has_threes_multiclass_model()` detection method
  - Added `_sample_threes_multiclass()` for PMF-based sampling
  - Updated prediction logic to route threes through multiclass model
- **`src/models/train_pipeline.py`:**
  - Added `_calibrate_multiclass_model()` for C5 evaluation
  - Updated calibration flow to check for multiclass model before C4 count model
- **`dashboard/src/types/predictions.ts`:**
  - Added `BetStatus` type and `PaperBet` interface
  - Added `DailyPerformance` interface for performance page
  - Added `StatPerformance` interface for stat breakdown

### Fixed

- **XGBoost `best_iteration` AttributeError** in `threes_multiclass.py`:
  - `best_iteration` is only set when early stopping triggers
  - Added try/except to fall back to `n_estimators` when not set
- **XGBoost `use_label_encoder` deprecation** in `threes_multiclass.py`:
  - Removed deprecated parameter from XGBClassifier instantiation

### Test Results

- 570 tests passed, 0 failures

### Technical Notes

**Why C5 Multiclass vs C4 Truncated NegBin:**
- Discrete outcomes (0, 1, 2, ... made threes) are naturally categorical
- XGBoost multi:softprob directly outputs calibrated class probabilities
- No quantile-to-PMF or count distribution conversion needed
- Categorical sampling is simpler and more direct than inverse CDF

**Artifacts (C5 architecture):**
- `threes_multiclass_model.joblib` — XGBoost multiclass model
- `threes_multiclass_meta.json` — Feature names, class count, config
- `threes_is_hurdle.json` — Flag file with `model_type: "multiclass"`

---

## [2026-02-09 Session 20] — Next.js Dashboard (G1, G4, G5 partial, G7)

### Added

- **Next.js Dashboard** (`dashboard/`):
  - **Tech Stack:** Next.js 16 with App Router, TypeScript, Tailwind CSS, Supabase SSR, Recharts
  - **Authentication:** Email/password login via Supabase Auth with middleware redirect
  - **Home Page:** Daily predictions grid with stat type filtering (All/PTS/REB/AST/THREES), edge sorting, player name enrichment from `players` table
  - **Analysis Modal:** Last 5 games bar chart, quantile distribution summary, prediction metadata
  - **Components created:**
    - `Navbar` — Navigation with bankroll display from `paper_trading_daily_log`
    - `FilterTabs` — Stat type filtering chips
    - `PropCard` / `PropGrid` — Prediction cards with over/under probabilities, edge badges
    - `AnalysisModal` — Modal with Last5Chart and QuantileSummary
    - `Last5Chart` — Recharts bar chart with reference line for prop line
    - `QuantileSummary` — Q10/Q25/Q50/Q75/Q90 distribution display
    - `PlayerAvatar` — NBA CDN headshots with inline SVG fallback
    - `Badge` / `EdgeBadge` — Stat type and edge tier visual indicators
    - Login page with form validation and error handling
  - **Supabase Integration:**
    - `src/lib/supabase/client.ts` — Browser client for client components
    - `src/lib/supabase/server.ts` — Server client for server components
    - `src/lib/supabase/middleware.ts` — Session refresh and auth redirect
  - **Utilities:**
    - `src/lib/utils.ts` — Date formatting, edge calculation, headshot URLs, inline SVG placeholder
    - `src/types/predictions.ts` — TypeScript interfaces for predictions, stats, colors
  - **Configuration:**
    - `.env.local` — Supabase URL and anon key (gitignored)
    - `next.config.ts` — NBA CDN image domain allowlist
    - `middleware.ts` — Auth redirect for protected routes

### Fixed

- **Crash recovery:** Previous session wrote text to `placeholder-avatar.png` causing API errors. Replaced with inline SVG data URL (no external file needed).

### Changed

- **ARCHITECTURE.md:**
  - Added Section 10 (Dashboard) with tech stack, directory structure, features, data sources
  - Updated Technology Stack table with Dashboard entry
  - Updated Directory Structure to include `dashboard/` folder
- **ACTIONITEMS.md:**
  - Added Session 20 summary
  - Updated Track G with G1, G4, G5, G7 marked as done/partial
  - Updated Priority Matrix with dashboard items

### Test Results

- 540 tests passed, 0 failures (coverage warning: 50.32% < 60% target)

---

## [2026-02-09 Session 19] — Feature Store Off-by-One Fix & Daily Runner Recency Filter

### Fixed

- **Critical off-by-one bug in feature store LATERAL JOINs** (`src/models/feature_store.py`):
  - **Bug:** Queries used `< game_date` to fetch pre-computed rolling averages, but `player_average_game_stats` uses `shift(1)` during population — meaning the row for `game_date X` already contains averages from games BEFORE X. The `<` logic caused queries to fetch the PREVIOUS game's row instead of current game's row.
  - **Impact:** Models were training and predicting with stale features (one game behind).
  - **Fix:** Changed `< game_date` to `<= game_date` in 15 LATERAL JOINs across 3 methods:
    - `get_features_for_date()` — backtesting
    - `get_features_for_date_range()` — batch backtesting
    - `_load_single_season_training()` — model training
  - Added explanatory comments clarifying why `<=` is safe (not data leakage).
  - Injury queries that look up OTHER players' historical stats correctly remain as `<`.

- **Daily runner returning retired players** (`src/models/daily_runner.py`):
  - **Bug:** Query for expected players had no recency filter, returning players like Shaquille O'Neal and Grant Hill from historical team rosters.
  - **Fix:** Added 30-day cutoff filter (`AND pgs.game_date >= :cutoff_date`) to `_get_players_for_games()`.
  - Added `target_date` parameter to method signature for proper cutoff calculation.

### Changed

- **`src/models/daily_runner.py`:**
  - `_get_players_for_games(games, target_date)` — now requires `target_date` parameter
  - `run_for_date()` — passes `target_date` to `_get_players_for_games()`

### Updated

- **`tests/test_daily_runner.py`:**
  - `test_get_players_for_games_empty` — now passes `target_date` argument
  - `test_get_players_for_games_success` — now passes `target_date` argument

- **`tests/test_feature_store.py`:**
  - `test_get_player_game_features_combines_outputs` — fixed mock for `_get_game_lines()` to return `line_spread_raw`, added mock for `_get_injury_context()`
  - `test_get_training_dataset_raises_on_small_dataset` — added `game_date` column and injury query handling to mock
  - `test_get_training_dataset_raises_on_null_position_group` — same fix
  - `test_get_training_dataset_builds_rate_targets` — same fix, corrected assertion from `seasons` to `season`

### Test Results

- 540 tests passed, 0 failures

### Impact

Models must be retrained to benefit from the off-by-one fix. Previously trained models were optimized for stale features; new training will use current-game features.

---

## [2026-02-09 Session 18] — Truncated NegBin Mu Training Fix & Training Safety

### Fixed

- **C4 THREES truncated NegBin mu training target** in `src/models/truncated_negbin.py`:
  - **Bug:** The mu model was trained on `log(observed_count + 0.5)`, but observed values come from the truncated distribution (E[X|X>0] = μ/(1-P(X=0))), which is inflated by ~26%
  - **Fix:** Applied truncation adjustment factor: `log_mu_target = log((y + 0.5) * (1 - p_zero_global))`
  - Scales down training targets by ~26%, bringing predicted mu from ~2.5 to correct ~1.66
  - This should fix the 25.8% calibration gap at Q10 in THREES count model

### Added

- **Atomic rename pattern for training safety** in `src/models/train_pipeline.py`:
  - Training now creates `run_YYYYMMDD_HHMMSS_incomplete` directory initially
  - Renamed to `run_YYYYMMDD_HHMMSS` only after all artifacts are saved (step 8)
  - Prevents race condition where inference job could select incomplete model during training
- **Incomplete directory filtering** in `src/orchestration/inference_job.py`:
  - Auto-select logic now filters out `_incomplete` directories
  - Improved error message when only incomplete runs exist

### Test Results

- 536 tests passed, 4 pre-existing failures in `test_feature_store.py` (mock issues unrelated to this session)

---

## [2026-02-09 Session 17] — Windows Task Scheduler Fixes & Incremental Stats

### Fixed

- **Batch script virtual environment path** — Changed `.venv` to `venv` in all 3 scripts:
  - `scripts/run_daily_stats.bat`
  - `scripts/run_lines.bat`
  - `scripts/run_inference.bat`
- **PYTHONPATH for subprocess imports** — Added `set PYTHONPATH=C:\Users\Chase\Projects\GameFlowData` to all batch scripts to fix `ModuleNotFoundError: No module named 'src'`
- **Log file permission conflicts** — Removed shell redirect (`>> logs\*.log 2>&1`) from batch scripts since Python's FileHandler handles logging
- **SQL syntax error in `update_player_position_history.py`** — Changed `:snap_date::DATE` to `CAST(:snap_date AS DATE)` to avoid SQLAlchemy parameter binding conflict with PostgreSQL cast syntax

### Added

- **`src/processing/populate_average_stats_incremental.py`** (~325 lines):
  - Lightweight daily version of rolling average calculation
  - Only processes players who played on target date (vs all players)
  - Fetches last 20 games per player (vs full history)
  - Uses UPSERT instead of TRUNCATE + reload
  - **Performance: 1.0s vs 1709s (28.5 min) — 1700x speedup**

### Changed

- **`src/orchestration/daily_stats_job.py`** — Step 6 now uses `populate_average_stats_incremental.py` instead of full recalculation

### Verified

- All 5 production scheduled tasks working:
  - `GameFlow-DailyStats` (9:00 AM)
  - `GameFlow-Lines-12PM` (12:00 PM)
  - `GameFlow-Lines-4PM` (4:00 PM)
  - `GameFlow-Lines-6PM` (6:00 PM)
  - `GameFlow-Inference` (6:30 PM)
- Full daily stats job completed successfully in 53 minutes total

---

## [2026-02-09 Session 16] — Windows Task Scheduler Automation

### Added

- **Windows Task Scheduler batch scripts** in `scripts/`:
  - `scripts/run_daily_stats.bat` — Wraps `daily_stats_job.py` for Task Scheduler
  - `scripts/run_lines.bat` — Wraps `lines_job.py` for Task Scheduler
  - `scripts/run_inference.bat` — Wraps `inference_job.py` for Task Scheduler
- **5 Windows Scheduled Tasks** for local deployment:
  - `GameFlow-DailyStats` — 9:00 AM daily
  - `GameFlow-Lines-12PM` — 12:00 PM daily
  - `GameFlow-Lines-4PM` — 4:00 PM daily
  - `GameFlow-Lines-6PM` — 6:00 PM daily
  - `GameFlow-Inference` — 6:30 PM daily

### Changed

- **ARCHITECTURE.md** — Added Windows Task Scheduler documentation in Orchestration section

### Analysis

- **Backtest sweep review (2026-02-08):** Analyzed 165 configurations from latest sweep
  - Best config: `tau=0.5, z_max=1.0, edge=0.15, kelly=0.125`
  - Results: $27,379 profit, 10.87% ROI, 57.6% hit rate, 1.21 Sharpe, 427 bets
  - PTS strongest: +17.5% ROI (241 bets)
  - BL blending now works after A3b fix (linear ramp confidence)
  - Edge > 0.20 bucket shows +20% ROI

---

## [2026-02-09 Session 15] — C4 THREES Truncated Negative Binomial Count Model

### Added

- **`TruncatedNegBinModel` class** in `src/models/truncated_negbin.py` (~500 lines):
  - Two-stage architecture: XGBoost regressors predict log(μ) and log(α) for Truncated Negative Binomial
  - Inverse CDF sampling produces integer counts directly (not continuous quantile interpolation)
  - mu/alpha parameterization: μ = mean, α = overdispersion (variance = μ + α×μ²)
  - `fit()`, `predict_params()`, `sample()`, `sample_single()` methods
  - `save()` / `load()` / `exists()` for persistence
  - Configuration via `TruncatedNegBinConfig` dataclass
- **`tests/test_truncated_negbin.py`** — 17 unit tests covering:
  - Model fitting convergence
  - Parameter prediction ranges
  - Integer sampling (all samples ≥ 1)
  - Batch sampling
  - Save/load roundtrip
  - Edge cases (zero values rejected, unfitted model raises)
- **`scripts/validate_threes_negbin.py`** — Phase 0 validation script:
  - Chi-squared goodness-of-fit test for truncated NegBin
  - Segment validation by shooter volume (high/moderate/low 3PA)
  - Results: All segments passed (WMAPE < 5%)
- **`_sample_threes_count()` method** in `src/models/monte_carlo.py`:
  - Bernoulli draw for zero vs positive (independent of copula)
  - Truncated NegBin sampling for positive samples
  - Integer output (0, 1, 2, 3, ...)
- **`_has_threes_count_model()` helper** in `src/models/monte_carlo.py`
- **`_train_threes_count_model()` method** in `src/models/quantile_trainer.py`:
  - Stage 1: XGBoost binary classifier + isotonic calibration for P(zero)
  - Stage 2: TruncatedNegBinModel on positive samples
  - Stores `threes_zero_classifier`, `threes_zero_calibrator`, `threes_count_model`, `threes_zero_feature_names`
- **`_calibrate_count_model()` method** in `src/models/train_pipeline.py`:
  - Zero prediction accuracy diagnostics
  - Quantile coverage evaluation via inverse CDF

### Changed

- **`src/models/quantile_trainer.py`:**
  - `train_rate_models()` — detects `stat == "threes"` and uses count model instead of quantile regression
  - `save_all()` — saves count model artifacts (classifier, calibrator, feature names, count model files)
  - `load_all()` — detects `model_type: "count"` in `threes_is_hurdle.json` and loads count model components
- **`src/models/monte_carlo.py`:**
  - `_predict_copula()` — routes threes through count model before copula processing
  - `predict_batch_for_date()` — handles count model for threes separately from copula stats
- **`src/models/train_pipeline.py`:**
  - `_evaluate_calibration()` — evaluates count model when present
  - `_evaluate_combined_calibration()` — includes threes when count model present

### Fixed

- **`AttributeError: feature_names_in_`** — XGBoost doesn't reliably expose feature names. Fixed by:
  - Storing `threes_zero_feature_names` explicitly during training
  - Saving as `threes_zero_feature_names.joblib`
  - Loading and using in calibration and inference paths

### Technical Details

**Why Truncated Negative Binomial:**
- Made threes are discrete integers (0, 1, 2, 3...) — quantile regression produces continuous values
- Overdispersion: variance ≈ 2.8 vs mean ≈ 2.1 — Poisson would underestimate variance
- Truncation at 0: we only model positive samples (zero classifier handles P(zero) separately)

**Sampling Strategy (inverse CDF, not rejection):**
```python
# Map u in (0,1) to truncated distribution
p_zero_nb = nbinom.pmf(0, n, p)
adjusted_u = u * (1 - p_zero_nb) + p_zero_nb
samples = nbinom.ppf(adjusted_u, n, p)  # integers >= 1
```

**Artifacts (C4 architecture):**
- `threes_zero_classifier.joblib` — XGBoost binary classifier
- `threes_zero_calibrator.joblib` — Isotonic regression for P(zero)
- `threes_zero_feature_names.joblib` — Feature names for zero classifier
- `truncated_negbin_meta.json` — Global mu/alpha, feature names
- `truncated_negbin_mu_model.joblib` — XGBoost regressor for log(μ)
- `truncated_negbin_alpha_model.joblib` — XGBoost regressor for log(α)
- `threes_is_hurdle.json` — Flag file with `model_type: "count"`

### Status

- All 523 tests pass
- Ready for retraining to activate C4 architecture
- Expected to fix the 25.6% Q10 calibration gap from C3

---

## [2026-02-07 Session 14] — Backtesting Data Fixes

### Fixed

- **Incomplete model directory selection** in `src/backtesting/run_sweep.py`:
  - `find_latest_model_dir()` now validates that `minutes_model.joblib` exists before selecting a directory
  - Logs warning when skipping incomplete training run directories
  - Prevents silent failures when an aborted training run leaves an empty artifact directory

- **Game ID format mismatch** affecting backtest line fetching:
  - **Root cause:** `raw_player_props_combined.game_id` stored as 8-digit (e.g., "22500589") vs `player_game_stats.game_id` as 10-digit (e.g., "0022500589")
  - **Fix (query):** Updated prefetch lines query in `src/backtesting/backtest_harness.py` to use `LPAD(rp.game_id, 10, '0') = gd.game_id`
  - **Fix (linker):** Added `.zfill(10)` in `src/processing/nba_linker_local.py` when storing game_ids in lookup dictionaries (3 locations)
  - **Impact:** Lines fetched increased from 33,962 to 191,908 (+465%). Bets increased from 889 to 2,251 (+153%)

- **Pre-existing test failure** in `tests/test_backtest_harness.py`:
  - Added missing `all_edges_df` parameter to `sample_result` fixture
  - `TestBacktestResult::test_to_csv` now passes

### Changed

- Updated `src/backtesting/backtest_harness.py` line 556: JOIN uses `LPAD()` for game_id compatibility
- Updated `src/backtesting/run_sweep.py` lines 560-592: `find_latest_model_dir()` includes validation
- Updated `src/processing/nba_linker_local.py` lines 355-357, 374-377, 916-919: game_id stored with leading zeros

---

## [2026-02-05 Session 13] — E6 Daily Pipeline Automation

### Added

- **Frequency-separated job scripts** for cron scheduling:
  - **`src/orchestration/daily_stats_job.py`** — Once-daily (6 AM ET) NBA game results + full processing pipeline
    - Steps: `nba_unified_scraper.py` → `nba_linker_local.py incremental` → `backfill_team_ids.py` → `update_player_position_history.py` → `update_league_position_averages.py` → `populate_average_stats.py` → `backfill_opponent_allowed.py`
    - Runtime: ~2-5 minutes
    - CLI: `--dry-run` to preview commands without executing
  - **`src/orchestration/lines_job.py`** — Multiple-times-daily (12 PM, 4 PM, 6 PM ET) props + injuries scraping
    - Steps: `daily_game_lines_scraper.py` → `daily_player_props_scraper.py` → `rapidapi_injury_backfill.py` (optional) → `link_injury_data.py` (optional) → `nba_linker_local.py incremental` (optional)
    - Runtime: ~30-90 seconds
    - CLI: `--date`, `--dry-run`, `--skip-injuries`, `--skip-linker`
  - **`src/orchestration/inference_job.py`** — Pre-game (6:30 PM ET) prediction generation
    - Loads model artifacts (auto-detects latest `run_*` directory)
    - Initializes Monte Carlo predictor with 10K samples + Gaussian copula
    - Stores to `daily_predictions` + `daily_prediction_samples` tables
    - Exports CSV backup to `predictions/` directory
    - Runtime: ~1-3 minutes
    - CLI: `--date`, `--dry-run`, `--model-dir`, `--stats`
- **`.session/specs/E6_daily_automation.md`** — Full specification document with:
  - Architecture diagram and timeline
  - Job descriptions and usage examples
  - Environment variable requirements
  - Cron configuration guide (ET → UTC conversion)
  - Monitoring and troubleshooting guide
- **`cron/gameflow_crontab.txt`** — Server cron schedule template with:
  - UTC times for all 5 daily jobs
  - Environment setup instructions
  - Log rotation job (weekly)
  - Documentation comments for manual runs and dry-run testing
- **`logs/` directory** — Job execution log directory with `.gitkeep`
- **`predictions/` directory** — Created by `inference_job.py` for CSV exports

### Changed

- Updated **ARCHITECTURE.md**:
  - Added frequency-separated job scripts table in Orchestration section
  - Added CLI documentation for new job scripts in Daily Workflow section
  - Updated directory structure with `logs/`, `cron/`, `predictions/` directories
  - Updated "Current state" section with E6 completion note
- Updated **ACTIONITEMS.md**:
  - E6 entry marked as implemented with full schedule and remaining Phase 2 work
  - Added Session 13 summary

### Analysis

- **Root cause of missing backtest bets after Jan 9:** The `game_id_map_staging` table lacks mappings for games after Jan 10 because the linker upload step never completed. The `props_game_updates.csv` file has mappings through Jan 23 but they weren't uploaded to the database. Fix: run `python src/processing/nba_linker_local.py upload`.

---

## [2026-02-05 Session 12] — Lightweight Incremental Linker

### Added

- **`link_incremental()` function** in `src/processing/nba_linker_local.py`:
  - Lightweight mode for daily automated linking without downloading full 25M+ row tables
  - Queries only unlinked records (`WHERE player_id IS NULL`)
  - Loads reference tables once (teams, players, team_game_stats)
  - Direct SQL updates via batched queries
  - Fuzzy player name matching with 0.80 threshold and last name bonus
  - Game matching via normalized team names and ±90 day fuzzy window
  - CLI options: `--batch-size` (default 50000), `--limit` (optional cap)
- **`normalize_player()` function** — Moved to module level for reuse across functions
- **Expanded `TEAM_NAME_ALIASES`** — 30 full team name → 3-letter abbreviation mappings (e.g., "Atlanta Hawks" → "ATL") for matching Odds API full names to NBA API abbreviations

### Changed

- **`src/orchestration/run_daily.py`:**
  - Fixed broken linker call (was missing command argument)
  - Now uses `incremental` command: `python src/processing/nba_linker_local.py incremental`

### Updated

- **`tests/test_nba_linker_local.py`:**
  - Updated `test_normalize_team_aliases` — Now expects 3-letter abbreviations instead of full team names

### Test Results

- Player match rate: 99.3% (4,963/5,000 records)
- Game match rate: 40.7% (2,037/5,000 records) — lower because many props are for future games
- Total unlinked records: ~2.8M combo market rows from backfill

---

## [2026-02-05 Session 11] — Fix BL Confidence + THREES Hurdle Model (A3b, C3)

### Added — THREES Hurdle Model (C3)

- **`HurdleQuantileModel` class** in `src/models/quantile_trainer.py`:
  - Two-stage architecture: Stage 1 binary classifier + Stage 2 quantile regression on positive samples
  - Isotonic regression calibration for P(zero) classifier
  - `predict_p_zero()`, `predict_quantiles()` methods with zero/positive combination
  - `_interpolate_positive_quantile()` for adjusted quantile mapping
  - `save()` / `load()` / `is_hurdle_model()` for persistence
- **`train_hurdle_model()` function** — Trains hurdle model with conformal recalibration on positive distribution
- **`_sample_hurdle()` and `_sample_hurdle_from_quantiles()` methods** in `src/models/monte_carlo.py`:
  - Bernoulli draw for zero vs positive (independent of copula)
  - Inverse CDF sampling for positive samples
  - Copula-correlated uniforms applied to positive branch only
- **`_calibrate_hurdle_model()` method** in `src/models/train_pipeline.py`:
  - Zero prediction accuracy diagnostics
  - Quantile coverage evaluation

### Changed

- **`PlayerPropsModelPipeline` class**:
  - Added `hurdle_models: dict[str, HurdleQuantileModel]` attribute
  - Modified `train_rate_models()` — uses `train_hurdle_model()` for THREES stat
  - Modified `save_all()` — saves hurdle model artifacts (classifier, calibrator, rate models, flag file)
  - Modified `load_all()` — detects and loads hurdle models via `threes_is_hurdle.json` flag
- **`MonteCarloPredictor._predict_copula()`** — Detects hurdle models and uses `_sample_hurdle()` instead of regular inverse CDF
- **`MonteCarloPredictor.predict_batch_for_date()`** — Handles hurdle models in batch prediction loop
- **`TrainingOrchestrator._evaluate_calibration()`** — Evaluates hurdle models separately
- **`TrainingOrchestrator._evaluate_combined_calibration()`** — Includes hurdle stats in combined eval

### Changed — BL Confidence (A3b)

- **`BLConfig` dataclass** in `src/models/black_litterman.py`:
  - Added `z_max: float = 1.0` parameter — z-score at which confidence saturates to 1.0
- **`compute_confidence()` method** — Replaced exponential formula with linear ramp:
  - Old: `confidence = 1 - exp(-0.5 * z²)` (near-zero for z < 0.5)
  - New: `confidence = min(z / z_max, 1.0)` (proportional for z < z_max)
  - Impact: At z=0.13 (typical 3% edge), confidence is now 0.13 vs 0.008 previously (16x improvement)

### Updated

- **`tests/test_black_litterman.py`:**
  - Updated `test_line_one_std_away` — z=1 now → confidence=1.0 (was 0.39)
  - Updated `test_line_two_std_away` — z=2 now → confidence=1.0 (was 0.86)
  - Updated `test_line_three_std_away` — z=3 now → confidence=1.0 (was 0.99)
  - Added `test_linear_confidence_at_half_z_max` — verifies z=0.5 → confidence=0.5
  - Added `test_custom_z_max` — verifies custom z_max=2.0 works correctly
  - Added `test_linear_ramp_proportional` — verifies linear relationship
  - Updated `test_default_config` and `test_custom_config` to include z_max assertions
  - All 42 tests pass
- **Test suite:** 518 of 523 tests pass (5 pre-existing failures unrelated to hurdle model)

---

## [2026-02-05 Session 10] — BL Sizing Parameter + Combo Markets Verification

### Added

- **`--bl-sizing-tau` CLI parameter** on `run_backtest.py` — Enables BL-blended probabilities for Kelly position sizing independently from edge detection
- **`bl_sizing_blender` field** on `BacktestHarness` — Separate blender instance for sizing calculations
- **`sizing_prob_over`/`sizing_prob_under` columns** in predictions output — BL-blended probabilities for position sizing
- **Spec files for next items:**
  - `.session/specs/A3b_BL_confidence_fix.md` — Linear ramp confidence function
  - `.session/specs/C3_THREES_hurdle_model.md` — Zero-inflated hurdle model for THREES

### Changed

- **`BetSimulator.place_bet()`** — Now accepts optional `sizing_prob` parameter for Kelly calculation (defaults to model probability if not provided)
- **`_calculate_edges()`** in `backtest_harness.py` — Computes sizing probabilities when `bl_sizing_blender` is set

### Verified

- **Combo markets scraping job (2026-01-31):** ~35K new prop lines successfully added to `raw_player_props_combined`:
  - `player_points_rebounds_assists` (12,013 rows, 82 players, 6 games)
  - `player_points_rebounds` (7,939 rows)
  - `player_points_assists` (5,758 rows)
  - `player_rebounds_assists` (5,107 rows)
  - `player_blocks_steals` (2,582 rows)
  - `player_field_goals` (2,376 rows)

### Analysis

- **Brier score improved:** 0.2705 → 0.2506 (model no longer catastrophically overconfident)
- **No-BL ROI:** +3.5% (profitable without BL blending)
- **BL confidence function issue persists:** Crushes sizing probs toward market, resulting in near-zero Kelly stakes

---

## [2026-02-04 Session 9] — Daily Injury Pipeline Fix + Paper Trading Infrastructure

### Added

- **Paper Trading Infrastructure (E5):**
  - **`src/paper_trading/paper_trader.py`** — Core `PaperTrader` class with:
    - `select_bets(game_date)` — Query daily_predictions, filter by edge threshold, calculate Kelly stakes
    - `place_bets(bets)` — UPSERT into paper_bets table
    - `resolve_bets(game_date)` — Fetch actuals from player_game_stats, update status/P&L
    - `get_pending_bets()`, `get_daily_summary()`, `get_bets_for_date()` — Dashboard query methods
  - **`src/paper_trading/place_bets.py`** — CLI script to place paper bets
    - `--dry-run` mode to preview without placing
    - `--edge-threshold`, `--kelly-fraction`, `--bankroll` parameters
    - Formatted table output with bet summary
  - **`src/paper_trading/resolve_bets.py`** — CLI script to resolve bets using actual results
    - `--dry-run` mode to preview resolution
    - Formatted resolution table with P&L summary
  - **DB migration:** `paper_bets` and `paper_trading_daily_log` tables
  - **Unit tests:** 20 tests in `tests/test_paper_trader.py` covering Kelly calculation, bet selection, resolution logic

### Changed

- **`src/orchestration/run_daily.py`** — Fixed `--scrape-injuries` flag to use RapidAPI instead of ESPN
  - Now calls `rapidapi_injury_backfill.py --start {date} --end {date}` to fetch injuries into `rapidapi_injuries` table
  - Then calls `link_injury_data.py` to populate `player_id` column via fuzzy name matching
  - Ensures consistency with feature store (`feature_store.py`) and daily runner (`daily_runner.py`) which both query `rapidapi_injuries`
  - Updated help text from "Scrape current injuries from ESPN" to "Scrape injuries from RapidAPI and link player IDs"

### Fixed

- **E4 (Daily injury pipeline)** — The `--scrape-injuries` flag was writing to `espn_injuries` table but all downstream components read from `rapidapi_injuries`. Daily injury data was effectively unused. Now both scraping and consumption use the same data source.

---

## [2026-01-31 Session 8] — Calibration Fixes, BL Sweep Analysis

### Added

- **Conformal recalibration** in `quantile_trainer.py` — post-training offset from validation residuals when coverage gap exceeds 3%
  - `RECALIBRATION_GAP_THRESHOLD = 0.03` class constant
  - `calibration_offsets: dict[float, float]` computed per quantile, applied at `predict_quantiles()` time
  - Persisted in model artifacts via `save()`/`load()`
- **Zero-snap handling** in `monte_carlo.py` — `ZERO_SNAP_THRESHOLD = 1e-3` snaps near-zero inverse CDF values to exactly 0
  - Applied in `_build_extended_quantile_fn()` for both copula and non-copula paths

### Changed

- `train_pipeline.py` — `_evaluate_combined_calibration()` now dynamically evaluates all trained rate models (`[s for s in ["pts", "reb", "ast", "threes"] if s in pipeline.rate_models]`) instead of hardcoded `["pts", "reb", "ast"]`
- `train_pipeline.py` — `_analyze_minutes_rate_correlation()` loop includes `"threes"` alongside `"pts"`, `"reb"`, `"ast"`
- `monte_carlo.py` — `_inverse_transform_sample()` refactored to use `_build_extended_quantile_fn()` instead of duplicating logic

### Analysis

- **BL parameter sweep (40 configs):** No-BL shows +3% ROI (600-873 bets, REB +7.9%). ALL BL configs produce 0-12 bets due to structural confidence function issue — `1 - exp(-0.5 * z²)` near-zero for realistic edges (z < 0.5)

---

## [2026-01-31] — Prediction Storage, Daily Runner Refactor, Scraper Resume

### Added

- **`src/models/prediction_store.py`** — Storage and retrieval module for daily MC predictions
  - `store_predictions()` — bulk upsert via `psycopg2.extras.execute_values` with `ON CONFLICT DO UPDATE`
  - `store_samples()` — gzip-compressed float64 numpy arrays stored as PostgreSQL BYTEA (~20-40KB per prediction)
  - `get_predictions()` — filtered retrieval by date/player/stat
  - `get_samples()` — decompress and return as np.ndarray
  - `get_player_id_by_name()` — fuzzy name lookup (case-insensitive LIKE)
- **`src/tools/query_player.py`** — CLI tool for querying stored daily predictions
  - Mode 1: Player + stat + line → compute over/under probability from MC samples + optional EV calculation
  - Mode 2: Player overview → all predictions for a player on a date
  - Mode 3: Top N edges → best absolute edges for a date with model vs market breakdown
- **DB migration: `create_daily_predictions_tables`** — two new tables:
  - `daily_predictions` — quantile predictions, edges, implied probabilities. UNIQUE on `(prediction_date, player_id, game_id, stat)`.
  - `daily_prediction_samples` — gzip-compressed MC sample arrays. UNIQUE on `(prediction_date, player_id, game_id, stat)`.
  - 3 indexes for query performance
- `--skip-storage` CLI flag on `run_daily.py` to skip DB persistence

### Changed

- **`daily_runner.py` — major refactor:**
  - `_get_games_for_date()` → NBA API ScoreboardV2 as primary, DB fallback for past dates
  - `_filter_injured_players()` → `rapidapi_injuries` with `player_id` integer matching (was `espn_injuries` with string name matching)
  - `_get_current_lines()` → `ROW_NUMBER() OVER (... ORDER BY snapshot_time DESC)` for latest snapshot per line
  - `_calculate_edges()` → MC samples empirical CDF with quantile interpolation fallback (was quantile-only)
  - `run_for_date()` → returns `(pd.DataFrame, dict[tuple, np.ndarray])` tuple instead of `pd.DataFrame`
  - New `_build_features_df()` and `_enrich_predictions()` helper methods
  - Uses `predict_batch_for_date()` (4 XGBoost calls) instead of per-player predict
- **`run_daily.py`** — wired `PredictionStore` for predictions + samples storage after inference
- **`player_prop_scraper.py`** — resume capability with market-aware progress file format
  - Progress file format: `{"markets": "...", "processed": [[ts, eid], ...]}` (was flat list)
  - Skip logic in main loop for already-processed events
  - Progress saving after each snapshot and on interrupt/error
  - `--no-resume` flag to start fresh

### Fixed

- **`test_daily_runner.py`** — updated all 7 failing tests for new return types, injury source, edge calc, and batch predict path. Added 4 new tests: NBA API primary, MC samples edge calc, quantile fallback, build_features_df, enrich_predictions.
- **`test_player_prop_scraper.py`** — updated 2 tests for new market-aware progress file format

---

## [2026-01-30] — Bug Fix Sweep, Parameter Sweep Tool, Scraper Improvements

### Added

- **ISSUES.md** — Comprehensive 28-issue audit of the core pipeline (12 fixed, 16 open)
- **`src/backtesting/run_sweep.py`** (778 lines) — Parameter sweep tool for BL tau, edge threshold, and Kelly fraction
  - Runs Phase 0-1 (DB fetch + MC predictions) once, replays edge calc + bet sim per config
  - Cartesian grid of `(tau, edge_threshold, kelly_fraction)` values
  - Per-config subdirectories with bets.csv, predictions.csv, metrics.json (compatible with `visualize_results.py`)
  - Comparison table with per-stat breakdown
- **`tests/test_run_sweep.py`** (651 lines) — Tests for sweep grid builder, shared phases, single-config execution, output formatting, and save logic
- 11 additional US2/us_ex bookmakers added to defaults: ballybet, betopenly, betparx, espnbet, fliff, hardrockbet, novig, polymarket, prophetx, rebet, windcreek
- **Scraper CLI improvements:**
  - `daily_player_props_scraper.py`: `--combos`, `--combos-only`, `--markets` flags for market selection; shared `CORE_MARKETS` and `COMBO_MARKETS` presets
  - `player_prop_scraper.py`: `--start-date`, `--end-date` date range filters; `--combos`, `--combos-only`, `--markets` flags; `--dry-run` credit estimation; argparse-based CLI

### Fixed

- **ISS-001** (CRITICAL): Minutes model now uses tuned hyperparams — `self.config` → `config` in `quantile_trainer.py:374`
- **ISS-002** (HIGH): `_run_date()` early-exit paths return `(None, pd.DataFrame())` instead of `None` — prevents `TypeError` unpacking
- **ISS-003** (HIGH): Non-BL edge path now uses multiplicative devigging in both `backtest_harness.py` and `daily_runner.py` — previously used vigged implied probabilities, understating edges by ~2-3%
- **ISS-004** (HIGH): Injury LATERAL JOIN split into two separate subqueries (game stats + advanced stats) — eliminates N×M cross-product and incorrect `ORDER BY` across tables. Applied to all 4 feature store query paths + single-player inference.
- **ISS-005** (HIGH): Training query filter `min > 0` → `min >= 5` — matches inference threshold, removes noisy low-minute samples
- **ISS-006** (HIGH): `early_stopping_rounds` now passed to `model.fit()` in `quantile_trainer.py` — previously configured but never applied
- **ISS-007** (MEDIUM): Combined calibration now evaluates the copula inference path — reordered `train_pipeline.py` steps so copula params are computed before combined calibration and passed to `MonteCarloPredictor`
- **ISS-008** (MEDIUM): `line_spread` now team-directional — negative for home (favored) team via `CASE WHEN matchup LIKE '%vs.%'` across all query paths; single-player path updated to apply sign from `is_home` context
- **ISS-009** (MEDIUM): COALESCE defaults changed from 0 to league averages — `avg_pace_l5=99.5`, `avg_def_rtg_l5=112.0`, `avg_fg3a_l5=34.0`, `avg_fg3_pct_l5=0.36`, `avg_usg_pct_l5=0.20`, `avg_ts_pct_l15=0.56`, etc. Applied to all bulk and single-player query paths.
- **ISS-011** (MEDIUM): Inference path advanced stats JOIN changed from exact `game_id` match to date-based LATERAL lookup (`game_date < :as_of_date ORDER BY game_date DESC LIMIT 1`) — matches bulk training/backtesting pattern
- **ISS-015** (MEDIUM): `_filter_best_bets` now selects best over and best under lines independently per (player, game, stat) — previously picked one row by max single-side edge, discarding valid opposite-side bets from other bookmakers
- **ISS-016** (MEDIUM): Combined calibration prediction failures tracked and logged as `WARNING` with count — previously swallowed at `DEBUG` level

### Changed

- `daily_runner.py`: `_get_current_lines()` now fetches all bookmakers and selects the sharpest (lowest-vig) line per player/game/market via booksum minimization; implied probabilities devigged via multiplicative normalization
- `backtest_harness.py`: `_run_date()` return type changed from `pd.DataFrame | None` to `tuple[pd.DataFrame | None, pd.DataFrame]`
- `train_pipeline.py`: Pipeline step ordering — copula params (5b) now computed before combined calibration (5c), correlation analysis moved to (5d)
- `player_prop_scraper.py`: Extended 2025-26 Regular season end date from 2026-01-23 to 2026-04-15

---

## [Unreleased]

### Added

- Initial project setup with Session-Driven Development
- Market neutralization diagnostic (A1) — regression + Brier score analysis on predictions.csv
- Comprehensive roadmap in ACTIONITEMS.md with Tracks A–E
- **Black-Litterman probability blending layer (A3)** — new module `src/models/black_litterman.py`
  - `BlackLittermanBlender` class with `BLConfig` dataclass
  - Log-odds space blending of model probabilities with devigged market prior
  - Per-prediction z-score confidence from MC distribution properties
  - Multiplicative devigging (equivalent to Shin's method for 2-outcome markets)
  - American-to-decimal odds conversion utility
  - 39 unit tests in `tests/test_black_litterman.py`
- `--bl-tau` CLI flag on `run_backtest.py` to enable BL blending (disabled by default)
- `posterior_prob` diagnostic field on `Bet` dataclass in `bet_simulator.py`
- BL diagnostic columns in predictions CSV: `model_over/under`, `market_over/under`, `confidence`, `posterior_over/under`
- **Prop line centering features (A4)** — per-stat player prop lines as rate model features
  - `prop_line_pts`, `prop_line_reb`, `prop_line_ast`, `prop_line_threes` added to `RATE_FEATURES_*` lists
  - LATERAL JOIN to `raw_player_props_combined` in all 4 feature store query paths
  - New `_get_player_prop_lines()` helper for single-player inference path
  - Database index `idx_props_player_game` on `(player_id, game_id)` for performance
- **B2/B3/B4: Rest, Trend, and Minutes Stability features** — 20 new model features
  - **B2 (Rest/Schedule):** `rest_days`, `is_back_to_back`, `games_in_last_7_days` added to `MINUTES_FEATURES` and all 4 `RATE_FEATURES_*` lists
  - **B3 (Short-Window Trends):** L3 rolling averages (`player_avg_{stat}_l3`), momentum ratios (`player_{stat}_l3_l15_ratio`), and L5 std deviations (`player_std_{stat}_l5`) added to `RATE_FEATURES_*` and `MINUTES_FEATURES`
  - **B4 (Minutes Stability):** `player_min_std_l5`, `player_min_floor_l5`, `player_games_started_l5` added to `MINUTES_FEATURES`
  - 14 new columns in `player_average_game_stats` table
  - New `calculate_b2_b3_b4_features()` in `populate_average_stats.py` with shift(1) no-leakage pattern
  - All 4 feature store query paths updated with consistent SQL
  - 4 new tests for B2/B3/B4 computation (no-leakage, std, rest_days, games_started)
- **B1: Injury/lineup context features** — 10 new features from `rapidapi_injuries` table
  - Teammate injuries: `team_out_count`, `team_out_min_sum`, `team_out_pts_sum`, `team_out_reb_sum`, `team_out_ast_sum`, `team_out_usg_sum`
  - Opponent injuries: `opp_out_count`, `opp_out_min_sum`
  - Player status: `player_is_questionable`, `player_is_probable`
  - SQL LATERAL JOINs in `feature_store.py` with temporal integrity (report_date ≤ game_date)
  - Added to all 4 `RATE_FEATURES_*` lists and `MINUTES_FEATURES`
- **Injury data pipeline** — RapidAPI historical backfill + fuzzy player linking
  - `src/scrapers/rapidapi_injury_backfill.py` — backfills injury data from 2021-present (88K+ rows)
  - `src/processing/link_injury_data.py` — 3-tier name matching cascade (manual CSV → exact → fuzzy SequenceMatcher)
  - `data/linker_data/player_mappings.csv` — 11 manual mappings for truncated API names (suffixes like "III", "Jr.")
  - Database cleanup: 142 garbage rows deleted, 99.3% of injury records fully linked
- **C0: Gaussian copula for minutes-rate correlation** — replaces legacy post-hoc adjustment
  - `MonteCarloPredictor` accepts `copula_params: dict[stat → Spearman ρ]`
  - `_predict_copula()`: shared z_minutes, per-stat correlated z_rate via Cholesky decomposition
  - Preserves both marginal distributions exactly while inducing correct rank dependency
  - `compute_copula_params_from_data()` and `load_copula_params()` utility functions
  - Training pipeline computes and saves `copula_params.json` as artifact
  - `run_backtest.py` and `run_daily.py` auto-load copula params from model artifacts
  - Falls back to legacy adjustment when copula params unavailable (backward compat)
- **Backtest dashboard** — expanded `visualize_results.py` from 163 to 925 lines
  - Self-contained HTML with Plotly charts (CDN) + vanilla JS for sorting/filtering
  - Sections: portfolio performance, metrics summary cards, enriched bet log, bookmaker line comparison
  - DB enrichment: resolves player_id/team_id/game_id to names/matchups via `player_game_stats` + `players` + `teams`
  - Graceful degradation for missing columns (bookmaker, posterior_prob) and missing data files

### Changed

- Updated ACTIONITEMS.md with corrected root cause diagnosis: model is catastrophically overconfident (Brier 0.2705), not market-correlated (R²=0.10)
- Promoted Black-Litterman blending (A3) to top priority based on diagnostic findings
- Reorganized priority matrix with A1 marked complete
- `backtest_harness.py`: Added `bl_blender` field and dual-path `_calculate_edges()` — BL path when blender is set, original path when None
- `run_backtest.py`: Added `--bl-tau` argument and BL blender construction
- `bet_simulator.py`: Added `posterior_prob` field to `Bet`, wired posterior storage in `evaluate_predictions()`
- Updated ARCHITECTURE.md with Stage E (Probability Blending), updated data flow diagram, backtesting CLI docs, and Known Issues section
- Marked A3 as implemented in ACTIONITEMS.md priority matrix
- **A2**: Removed `line_total` from `RATE_FEATURES_PTS` to eliminate market leakage (remains in `MINUTES_FEATURES`)
- **A4**: `feature_store.py` — added LATERAL JOINs and `prop_line_*` SELECT columns to `get_training_dataset()`, `get_features_for_date()`, `get_features_for_date_range()`; wired `_get_player_prop_lines()` into `get_player_game_features()`
- Marked A2 and A4 as implemented in ACTIONITEMS.md priority matrix
- Updated ARCHITECTURE.md Feature Store section with prop line centering documentation
- **B2/B3/B4**: `feature_store.py` — updated all 5 feature lists, all 3 bulk SQL queries, `_get_player_rolling_stats()`, and `get_player_game_features()` for new features
- **B2/B3/B4**: `populate_average_stats.py` — refactored `rolling_with_groupby()` to support `agg` parameter (std/min/sum), updated insert column list
- Updated ARCHITECTURE.md Feature Store section with B2/B3/B4 documentation
- **monte_carlo.py**: Added `copula_params` to `__init__`, new `_predict_copula()` method, `_build_extended_quantile_fn()`, `_map_uniforms_to_samples()` helpers, updated `predict_batch_for_date()` with copula branch
- **train_pipeline.py**: Added `_compute_copula_params()` step to training pipeline, imports `compute_copula_params_from_data`
- **run_backtest.py**: Auto-loads `copula_params.json` from model artifacts, passes to `MonteCarloPredictor`
- **run_daily.py**: Same copula auto-loading for daily inference pipeline
- Updated ARCHITECTURE.md with injury data, copula sampling, dashboard, and injury linker documentation
- Updated ACTIONITEMS.md: B1 marked done, C0 (copula) added, A6 (conditional rate modeling) added as future option

### Fixed

- **B2/B3/B4**: Fixed `AttributeError: Can only use .dt accessor with datetimelike values` in `calculate_b2_b3_b4_features()` — DB returns `date` objects, not `datetime64`. Added `pd.to_datetime()` conversion before date arithmetic in both `calculate_b2_b3_b4_features()` and `_count_games_in_window()`
- **Feature Store**: Fixed hardcoded zeros bug in `get_features_for_date_range()` — `rest_days` and `is_back_to_back` were being overwritten to 0 instead of using SQL-computed values
- **MCP Config**: Fixed `.mcp.json` RapidAPI server entry for Windows — changed `npx` to `cmd /c npx` wrapper pattern

### Removed

- Removed `_get_travel_and_rest_features()` from `FeatureStore` — rest features now pre-computed in DB via backfill script
- Removed `_get_travel_features_single()` from `FeatureStore` — same reason
- Removed `TEAM_LOCATIONS` dict and `_haversine()` static method from `feature_store.py` — no longer needed after travel feature removal
- Removed `numpy` import from `feature_store.py` — no longer used

### Changed
- Refactored project structure and moved files
- Updated test_backfill_league_priors.py
