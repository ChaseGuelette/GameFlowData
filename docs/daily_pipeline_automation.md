# Daily Pipeline Automation (E6)

Documentation for the frequency-separated job scripts that automate the GameFlowData daily pipeline.

## Overview

The daily pipeline is split into four jobs based on execution frequency:

| Job | Schedule (ET) | Purpose | Runtime |
|-----|---------------|---------|---------|
| `daily_stats_job.py` | 11:00 AM | NBA game results + processing | ~3-5 min |
| `daily_stats_job.py` (retry) | 11:30 AM | Auto-retry if 11 AM run failed | ~3-5 min |
| `lines_job.py --live --parallel` | 12 PM, 4 PM | Full live scrape with parallel props + injury paths | ~45-55 sec |
| `inference_job.py` | 12:15 PM, 4:15 PM | Full MC inference + edge calculation (checks daily stats dependency). 4:15 PM run uses `--skip-bets` to avoid paper trading hang during game hours. | ~16 sec |
| `lines_job.py --live --props-only` | Every 5 min, 11 AM – 11 PM | Props-only live scrape + linker | ~25-30 sec |
| `edge_refresh_job.py` | Every 5 min (+2 min offset), 11 AM – 11 PM | Recalculate edges (--skip-paper on cron runs) | ~2-3 min |

**Note:** Inference job optimized from ~3 min to ~16 sec in Session 27 via parallel feature building and prop lines query optimization.

**API Budget:** 5M credits/month. Current schedule uses ~6,400/day (~200K/month) — 4% of quota.

**Discord Alerts:** High-frequency jobs (props-only, edge refresh) only send Discord alerts on failure (`silent_on_success`). Full scrapes, inference, and daily stats always alert.

## Pipeline Timeline

```
11:00 AM   daily_stats_job.py
           ├─ nba_unified_scraper.py (NBA game results)       [10m timeout, 2 retries]
           ├─ nba_linker_local.py incremental                 [10m timeout, 2 retries]
           ├─ backfill_team_ids.py                            [5m timeout, no retries]
           ├─ update_player_position_history.py                [5m timeout, no retries]
           ├─ update_league_position_averages.py               [5m timeout, no retries]
           ├─ populate_average_stats_incremental.py            [20m timeout, 2 retries]
           ├─ backfill_opponent_allowed_incremental.py         [15m timeout, 2 retries]
           ├─ resolve ALL pending paper bets
           └─ resolve ALL pending user bets (from dashboard checkmark)

11:30 AM   daily_stats_retry (auto-retry if 11 AM failed)
           └─ Checks JOB_STATUS["daily_stats_job.py"], re-runs if not "success"

12:00 PM   lines_job.py --live --parallel (full scrape, parallel)
           Group A (props):    ├─ daily_game_lines_scraper.py
                               ├─ daily_player_props_scraper.py --live --target-table raw_player_props_combined
                               └─ nba_linker_local.py incremental
           Group B (injuries): ├─ rapidapi_injury_backfill.py
              (concurrent)     └─ link_injury_data.py

12:15 PM   inference_job.py (FULL MC inference)
           ├─ Scheduler checks daily_stats dependency (8h window)
           │   └─ If stale: passes --stale-warning, sends Discord alert
           ├─ Load model artifacts
           ├─ Check upstream data freshness (latest game_date < yesterday?)
           ├─ DailyPredictionRunner.run_for_date()
           ├─ Store predictions + MC samples to DB
           ├─ Place paper bets (PaperTrader.select_bets + place_bets)
           ├─ Export predictions CSV
           └─ Send Discord alert (+ stale data warning if applicable)

11 AM -    lines_job.py --live --props-only (every 5 min, :00/:05/:10/.../:55)
11 PM      edge_refresh_job.py (every 5 min, :02/:07/:12/.../:57)
           ├─ Resolve pending bets from previous days (exclude_today=True)
           ├─ Recalculate edges from stored MC samples + fresh lines
           └─ Place/update paper bets for today (skips live games)
           Discord alerts: ONLY on failure (silent_on_success)

4:00 PM    lines_job.py --live --parallel (full scrape — catches new player props)

4:15 PM    inference_job.py --skip-bets (MC inference — skip paper trading, bets already placed at noon)

7:00 PM    Games typically start
```

---

## daily_stats_job.py

**Location:** `src/orchestration/daily_stats_job.py`

**Purpose:** Scrape NBA game results from the previous night and run the full processing pipeline to update derived stats.

**Schedule:** Once daily, 11:00 AM ET (after previous night's games are final, moved from 9 AM to ensure all games are posted)

### Usage

```bash
# Normal run
python src/orchestration/daily_stats_job.py

# Dry run (show commands without executing)
python src/orchestration/daily_stats_job.py --dry-run
```

### CLI Arguments

| Argument | Description |
|----------|-------------|
| `--dry-run` | Show what would be executed without running |

### Pipeline Steps

| Step | Script | Critical | Timeout | Retries |
|------|--------|----------|---------|---------|
| 1 | `nba_unified_scraper.py` — Fetch latest game box scores | Yes | 10m | 2 |
| 2 | `nba_linker_local.py incremental` — Link unmatched props | Yes | 10m | 2 |
| 3 | `backfill_team_ids.py` — Fill missing team IDs | No | 5m | 0 |
| 4 | `update_player_position_history.py` — Update positions | No | 5m | 0 |
| 5 | `update_league_position_averages.py` — League averages | No | 5m | 0 |
| 6 | `populate_average_stats_incremental.py` — Rolling averages | Yes | **20m** | **2** |
| 7 | `backfill_opponent_allowed_incremental.py` — Opponent stats | Yes | **15m** | **2** |

**Retries use exponential backoff:** 15s → 30s → 60s between attempts. Step 6 is the most common timeout culprit — its timeout was doubled from 10m→20m in Session 58.

**Note:** `play_type_scraper.py` was removed (Session 46) because `stats.nba.com` blocks datacenter IPs. Non-critical step failures log a warning and continue; critical step failures abort the pipeline.

### Logs

Output is written to `logs/daily_stats.log`.

---

## lines_job.py

**Location:** `src/orchestration/lines_job.py`

**Purpose:** Scrape latest player prop lines and injury updates. Supports full scrapes and lightweight props-only refreshes.

**Schedule:**
- **Full scrape (`--live --parallel`):** 12 PM, 4 PM ET (props + injuries run concurrently)
- **Props-only (`--live --props-only`):** Every 5 minutes, 11 AM – 11 PM ET (silent on success)
- **Historical mode (no `--live`):** For backfills (uses historical API snapshots)

### Usage

```bash
# Live full scrape with parallel execution
python src/orchestration/lines_job.py --live --parallel

# Live full scrape (sequential, backward compatible)
python src/orchestration/lines_job.py --live

# Live props-only (fast — props + linker only)
python src/orchestration/lines_job.py --live --props-only

# Historical scrape for specific date
python src/orchestration/lines_job.py --date 2026-02-05

# Dry run
python src/orchestration/lines_job.py --live --parallel --dry-run
```

### CLI Arguments

| Argument | Description |
|----------|-------------|
| `--date YYYY-MM-DD` | Target date (defaults to today) |
| `--live` | Use live API endpoints; writes props to `raw_player_props_combined` |
| `--props-only` | Skip game lines and injuries (only props + linker) |
| `--parallel` | Run props path and injury path concurrently (full mode only) |
| `--dry-run` | Show what would be executed without running |
| `--skip-injuries` | Skip injury scraping (faster execution) |
| `--skip-linker` | Skip incremental linker (if already run today) |

### Pipeline Steps (Full Mode)

When `--parallel` is set, Group A and Group B run concurrently:

**Group A (props path — serial):**
1. `daily_game_lines_scraper.py` - Fetch game lines from Odds API (skipped in `--props-only`)
2. `daily_player_props_scraper.py --live --target-table raw_player_props_combined` - Fetch live player props
3. `nba_linker_local.py incremental` - Link new props to player/game IDs

**Group B (injury path — serial, concurrent with Group A):**
4. `rapidapi_injury_backfill.py` - Fetch injury updates (skipped in `--props-only`)
5. `link_injury_data.py` - Link injury player names to IDs (skipped in `--props-only`)

Without `--parallel`, all steps run sequentially in order 1→2→4→5→3.

### Logs

Output is written to `logs/lines.log`.

---

## edge_refresh_job.py

**Location:** `src/orchestration/edge_refresh_job.py`

**Purpose:** Lightweight edge recalculation using stored MC samples and fresh prop lines. Does NOT re-run inference — no model loading, no feature engineering, no MC sampling.

**Schedule:** Every 5 minutes (+2 min offset from props scrape), 11 AM – 11 PM ET (~156 runs/day, silent on success)

### Usage

```bash
# Normal run
python src/orchestration/edge_refresh_job.py

# Dry run (compute but don't upsert)
python src/orchestration/edge_refresh_job.py --dry-run

# Specific date
python src/orchestration/edge_refresh_job.py --date 2026-02-05

# Specific stats
python src/orchestration/edge_refresh_job.py --stats pts reb
```

### CLI Arguments

| Argument | Description |
|----------|-------------|
| `--date YYYY-MM-DD` | Target date (defaults to today) |
| `--dry-run` | Compute edges but don't upsert to database |
| `--stats STAT [STAT ...]` | Stats to refresh (defaults to `pts reb ast`) |
| `--skip-discord` | Skip Discord alert |
| `--skip-paper` | Skip paper trading step (bet selection + placement). Used by 5-min cron runs to avoid timeouts |

### Pipeline Steps

1. Load stored MC samples via `PredictionStore.get_all_samples_for_date()` — returns `dict[(player_id, game_id, stat) -> np.ndarray]`
2. Load stored predictions from `daily_predictions` for target date
3. Get unique game_ids from predictions
4. Fetch fresh prop lines from `raw_player_props_combined` (sharpest book per player/game/market, 24h snapshot_time cutoff)
5. Recalculate over/under probabilities from MC samples (empirical CDF)
6. Compute implied probabilities from odds (multiplicative devigging)
7. Recalculate raw edges (model prob - implied prob)
8. Recalculate Black-Litterman blended probabilities and recommendations
9. Upsert updated predictions to `daily_predictions`
10. Export CSV backup
11. **Resolve pending paper bets** from previous days (`exclude_today=True`) — prevents same-day false resolution
12. **Place/update paper bets** for today's recommended predictions — skips games already in progress (checks `commence_time`)

### Key Design Decisions

- **Self-contained:** Does NOT instantiate `DailyPredictionRunner` or load model/feature pipeline. Only uses `PredictionStore`, `BlackLittermanBlender`, and raw SQL queries.
- **Graceful exit:** If no MC samples exist for the target date (inference hasn't run yet), exits with info-level "NO-OP" message and code 0 (expected before first inference of the day).
- **MC sample staleness (2026-03-02):** If MC samples are >6 hours old, logs a warning and sends a Discord alert so operators know edge calculations use stale inference data.
- **Feature preservation:** Loads existing predictions and only updates line/edge/BL columns. All `feat_*` columns and quantile predictions are preserved.
- **Line preservation (2026-03-05 fix):** When fresh lines aren't available for a prediction (props no longer on the API), old line/odds/bookmaker values are preserved via `fillna()` fallback instead of being nulled out by the LEFT merge.
- **Skip paper trading (2026-03-05):** `--skip-paper` flag skips paper bet selection and placement. The 5-minute silent cron runs use this to avoid 45-minute timeouts caused by loading MC samples and running BL blending for every prediction during game hours.
- **Line selection (2026-03-01 fix):** `fetch_fresh_lines()` partitions by `(player_id, game_id, market_key, bookmaker, line, outcome_label)` — including `line` in the partition ensures alt lines from the same bookmaker are separate rows. A `HAVING` clause requires both Over and Under odds. The sharpest-book selection (lowest booksum) naturally picks primary lines with matched odds. Previously, `MAX(line)` conflated alt lines, causing mismatched odds and stale line selection.

### Logs

Output is written to `logs/edge_refresh.log`.

---

## inference_job.py

**Location:** `src/orchestration/inference_job.py`

**Purpose:** Generate predictions for today's games using the latest model artifacts and prop lines.

**Schedule:** Twice daily, 12:15 PM and 4:15 PM ET (after full lines scrapes)

### Usage

```bash
# Normal run
python src/orchestration/inference_job.py

# Specific date
python src/orchestration/inference_job.py --date 2026-02-05

# Specific model
python src/orchestration/inference_job.py --model-dir src/models/artifacts/run_20260131_112534

# Dry run (generate predictions but don't store to DB)
python src/orchestration/inference_job.py --dry-run

# Custom stats
python src/orchestration/inference_job.py --stats pts reb
```

### CLI Arguments

| Argument | Description |
|----------|-------------|
| `--date YYYY-MM-DD` | Target date (defaults to today) |
| `--dry-run` | Generate predictions but don't store to database |
| `--model-dir PATH` | Path to model artifacts (defaults to `src/models/artifacts`) |
| `--stats STAT [STAT ...]` | Stats to predict (defaults to `pts reb ast`) |
| `--skip-bets` | Skip automatic paper bet placement |
| `--skip-discord` | Skip sending Discord alert |
| `--stale-warning` | Flag that upstream daily stats may be stale (set by scheduler dependency check) |

### Pipeline Steps

1. Load model artifacts (auto-detects latest `run_*` directory, or `production/` folder)
2. Initialize FeatureStore and MonteCarloPredictor (10,000 samples)
3. Load Gaussian copula params for correlated sampling
4. Load combined calibration offsets (if `combined_calibration_offsets.json` exists — currently not deployed)
5. Check upstream data freshness — warns if latest `game_date` in `player_average_game_stats` is before yesterday (changed from >2 days threshold in Session 58). Sets `data_stale` flag but **does not hard-fail** — stale data is better than zero predictions.
6. Run `DailyPredictionRunner.run_for_date()`
   - **Parallel feature building** (8 workers, ~5s) — queries feature store concurrently
   - **Optimized prop lines query** (~0.2s) — searches both 8/10-digit game_id formats
7. Store predictions to `daily_predictions` table
8. Store MC samples to `daily_prediction_samples` table
9. **Place paper bets** on recommended predictions via `PaperTrader.select_bets()` + `place_bets()` (non-fatal)
10. Send Discord alert (non-fatal)
11. Export CSV backup to `predictions/predictions_YYYY-MM-DD.csv`

### Performance

| Component | Before | After | Speedup |
|-----------|--------|-------|---------|
| Feature building | 65s | 4.8s | 13x |
| Prop lines query | 137s | 0.2s | 685x |
| Other (model, MC) | ~10s | ~11s | — |
| **Total** | ~180s | ~16s | **10x** |

### Logs

Output is written to `logs/inference.log`.

---

## Environment Variables

Required in `.env`:

```bash
DATABASE_URL=postgresql://user:pass@host:port/db
ODDS_API_KEY=<your-odds-api-key>
RAPIDAPI_KEY=<your-rapidapi-key>
```

---

## Scheduling Configuration

### Option 1: Windows Task Scheduler (Local)

For local Windows deployment, batch scripts in `scripts/` wrap each job:

**Batch Scripts:**
- `scripts/run_daily_stats.bat` — Runs daily_stats_job.py
- `scripts/run_lines.bat` — Runs lines_job.py
- `scripts/run_inference.bat` — Runs inference_job.py

**Create Scheduled Tasks:**

```cmd
:: Daily Stats - 9:00 AM
schtasks /create /tn "GameFlow-DailyStats" /tr "C:\Users\Chase\Projects\GameFlowData\scripts\run_daily_stats.bat" /sc daily /st 09:00 /f

:: Lines Job - 12:00 PM
schtasks /create /tn "GameFlow-Lines-12PM" /tr "C:\Users\Chase\Projects\GameFlowData\scripts\run_lines.bat" /sc daily /st 12:00 /f

:: Lines Job - 4:00 PM
schtasks /create /tn "GameFlow-Lines-4PM" /tr "C:\Users\Chase\Projects\GameFlowData\scripts\run_lines.bat" /sc daily /st 16:00 /f

:: Lines Job - 6:00 PM
schtasks /create /tn "GameFlow-Lines-6PM" /tr "C:\Users\Chase\Projects\GameFlowData\scripts\run_lines.bat" /sc daily /st 18:00 /f

:: Inference Job - 6:30 PM
schtasks /create /tn "GameFlow-Inference" /tr "C:\Users\Chase\Projects\GameFlowData\scripts\run_inference.bat" /sc daily /st 18:30 /f
```

**Manage Tasks:**

```cmd
:: List all GameFlow tasks
schtasks /query /fo TABLE | findstr GameFlow

:: Run a task manually
schtasks /run /tn "GameFlow-Lines-12PM"

:: Disable all tasks (off-season)
schtasks /change /tn "GameFlow-DailyStats" /disable
schtasks /change /tn "GameFlow-Lines-12PM" /disable
schtasks /change /tn "GameFlow-Lines-4PM" /disable
schtasks /change /tn "GameFlow-Lines-6PM" /disable
schtasks /change /tn "GameFlow-Inference" /disable

:: Re-enable all tasks
schtasks /change /tn "GameFlow-DailyStats" /enable
schtasks /change /tn "GameFlow-Lines-12PM" /enable
schtasks /change /tn "GameFlow-Lines-4PM" /enable
schtasks /change /tn "GameFlow-Lines-6PM" /enable
schtasks /change /tn "GameFlow-Inference" /enable

:: Delete a task
schtasks /delete /tn "GameFlow-DailyStats" /f
```

**Note:** Windows tasks only run if PC is on and user is logged in. Missed tasks do NOT run retroactively.

### Option 2: Railway Cloud (Recommended)

**Current production deployment.** Single always-on worker running APScheduler.

See [Railway Deployment](railway_deployment.md) for full setup guide.

**Key benefits:**
- Always-on (no missed runs)
- Automatic restarts on failure
- Centralized logging via `railway logs`
- ~$5/month cost

**Note:** Local Windows tasks have been disabled to avoid conflicts with Railway.

### Option 3: Linux Cron (Server)

For self-hosted server deployment, use the template at `cron/gameflow_crontab.txt`.

**Example (UTC times for EST):**

```cron
# 9:00 AM ET (14:00 UTC during EST)
0 14 * * * cd /path/to/GameFlowData && python src/orchestration/daily_stats_job.py >> logs/daily_stats.log 2>&1

# 12:00 PM ET (17:00 UTC during EST)
0 17 * * * cd /path/to/GameFlowData && python src/orchestration/lines_job.py >> logs/lines.log 2>&1

# 4:00 PM ET (21:00 UTC during EST)
0 21 * * * cd /path/to/GameFlowData && python src/orchestration/lines_job.py >> logs/lines.log 2>&1

# 6:00 PM ET (23:00 UTC during EST)
0 23 * * * cd /path/to/GameFlowData && python src/orchestration/lines_job.py >> logs/lines.log 2>&1

# 6:30 PM ET (23:30 UTC during EST)
30 23 * * * cd /path/to/GameFlowData && python src/orchestration/inference_job.py >> logs/inference.log 2>&1
```

**Note:** NBA API (`nba_api`) may be blocked from datacenter IPs. Consider local Windows deployment or residential proxy for cloud servers.

---

## Pipeline Resilience (Session 58)

### Job Status Tracking

The scheduler maintains an in-memory `JOB_STATUS` dict that tracks every job's status, end time, and duration. After each `run_job()` call, status is recorded both in-memory (for fast dependency checks) and to the `job_executions` Supabase table (for persistent history and debugging).

```sql
-- Query recent job executions
SELECT job_name, status, started_at, duration_seconds, error_message
FROM job_executions
ORDER BY started_at DESC
LIMIT 20;
```

### Dependency Gate

Before running inference, the scheduler calls `check_dependency("daily_stats_job.py", max_age_hours=8)`. If the daily stats job hasn't succeeded in the last 8 hours:

1. Logs a WARNING with the specific upstream failure
2. Sends a Discord alert: "Daily stats job has not succeeded today"
3. **Still runs inference** with `--stale-warning` flag
4. Inference runs normally but flags predictions as potentially stale

### Automatic Retry

If the 9 AM daily stats job fails, a retry fires automatically at 9:30 AM ET. The retry checks `JOB_STATUS` — if the 9 AM run already succeeded, it's a no-op.

### Failure Cascade (What Happens When 9 AM Fails)

```
9:00 AM  - daily_stats_job FAILS (timeout, network error, etc.)
9:30 AM  - daily_stats_retry fires, re-runs daily_stats_job
           ├─ If retry SUCCEEDS: pipeline continues normally
           └─ If retry also FAILS:
12:15 PM - inference checks dependency → STALE
           ├─ Sends Discord alert: "stale rolling averages"
           ├─ Passes --stale-warning to inference_job
           └─ Inference RUNS with slightly stale data (L5 has 4/5 overlap)
           All downstream jobs continue normally
```

---

## Monitoring

### Log Monitoring

```bash
# Watch logs in real-time
tail -f logs/daily_stats.log
tail -f logs/lines.log
tail -f logs/inference.log

# Check for errors
grep -i "error\|failed" logs/*.log
```

### Database Health Check

```sql
-- Check latest predictions
SELECT prediction_date, COUNT(*)
FROM daily_predictions
GROUP BY prediction_date
ORDER BY prediction_date DESC
LIMIT 7;

-- Check latest props
SELECT snapshot_time::date, COUNT(*)
FROM raw_player_props_combined
WHERE snapshot_time > NOW() - INTERVAL '7 days'
GROUP BY snapshot_time::date
ORDER BY 1 DESC;
```

---

## Troubleshooting

### Job fails with "No games found"
- Check if it's an off-day (no NBA games)
- Verify NBA API is accessible
- Check `team_game_stats` table for recent data

### Props not linking
- Run full linker: `python src/processing/nba_linker_local.py all`
- Check `linker_data/unmatched_players.csv` for manual mappings needed
- Verify `game_id_map_staging` has recent dates

### Predictions empty
- Ensure lines_job ran successfully first
- Check `raw_player_props_combined` has today's game_ids populated
- Verify model artifacts exist in `src/models/artifacts/`

### API rate limits
- Odds API: Check remaining credits at https://the-odds-api.com
- NBA API: Built-in rate limiting (0.6s between calls)
- RapidAPI: Check subscription limits

---

## Related Documentation

- [Railway Deployment](railway_deployment.md) - Cloud deployment guide (current production)
- [Scalability](scalability.md) - Architecture capacity and scaling path
- [Model Pipeline Runbook](model_pipeline_runbook.md) - Full training and inference guide
- [NBA Linker Local](nba_linker_local_documentation.md) - ID matching and linking
- [Feature Store](feature_store_documentation.md) - Feature generation
- [Spec Document](.session/specs/E6_daily_automation.md) - Full specification
