# Daily Pipeline Automation (E6)

Documentation for the frequency-separated job scripts that automate the GameFlowData daily pipeline.

## Overview

The daily pipeline is split into four jobs based on execution frequency:

| Job | Schedule (ET) | Purpose | Runtime |
|-----|---------------|---------|---------|
| `daily_stats_job.py` | 9:00 AM | NBA game results + processing | ~3-5 min |
| `lines_job.py --live` | 12 PM, 4 PM | Full live scrape (game lines + props + injuries + linker) | ~30-90 sec |
| `inference_job.py` | 12:15 PM, 4:15 PM | Full MC inference + edge calculation | ~16 sec |
| `lines_job.py --live --props-only` | 1-3 PM hourly, 4:30-6:30 PM half-hourly | Props-only live scrape + linker | ~15-30 sec |
| `edge_refresh_job.py` | 2 min after each props-only | Recalculate edges from stored samples + fresh lines | ~2-3 sec |

**Note:** Inference job optimized from ~3 min to ~16 sec in Session 27 via parallel feature building and prop lines query optimization.

**API Budget:** 5M credits/month. New schedule uses ~3,250/month (previously ~750/month) — negligible impact.

## Pipeline Timeline

```
9:00 AM    daily_stats_job.py
           ├─ nba_unified_scraper.py (NBA game results)
           ├─ nba_linker_local.py incremental
           ├─ backfill_team_ids.py
           ├─ update_player_position_history.py
           ├─ update_league_position_averages.py
           ├─ populate_average_stats_incremental.py (lightweight, ~1s)
           ├─ backfill_opponent_allowed_incremental.py --days-back 2
           └─ resolve ALL pending paper bets

12:00 PM   lines_job.py --live (full scrape)
           ├─ daily_game_lines_scraper.py
           ├─ daily_player_props_scraper.py --live --target-table raw_player_props_combined
           ├─ rapidapi_injury_backfill.py
           ├─ link_injury_data.py
           └─ nba_linker_local.py incremental

12:15 PM   inference_job.py (FULL MC inference)
           ├─ Load model artifacts
           ├─ Check upstream data freshness
           ├─ DailyPredictionRunner.run_for_date()
           ├─ Store predictions + MC samples to DB
           ├─ Place paper bets (PaperTrader.select_bets + place_bets)
           ├─ Export predictions CSV
           └─ Send Discord alert

1:00 PM    lines_job.py --live --props-only → edge_refresh_job.py (1:02 PM)
2:00 PM    lines_job.py --live --props-only → edge_refresh_job.py (2:02 PM)
3:00 PM    lines_job.py --live --props-only → edge_refresh_job.py (3:02 PM)

4:00 PM    lines_job.py --live (full scrape — catches new player props)

4:15 PM    inference_job.py (FULL MC inference — second window)

4:30 PM    lines_job.py --live --props-only → edge_refresh_job.py (4:32 PM)
5:00 PM    lines_job.py --live --props-only → edge_refresh_job.py (5:02 PM)
5:30 PM    lines_job.py --live --props-only → edge_refresh_job.py (5:32 PM)
6:00 PM    lines_job.py --live --props-only → edge_refresh_job.py (6:02 PM)
6:30 PM    lines_job.py --live --props-only → edge_refresh_job.py (6:32 PM, final)

7:00 PM    Games typically start
```

---

## daily_stats_job.py

**Location:** `src/orchestration/daily_stats_job.py`

**Purpose:** Scrape NBA game results from the previous night and run the full processing pipeline to update derived stats.

**Schedule:** Once daily, 9:00 AM ET (after previous night's games are final)

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

1. `nba_unified_scraper.py` - Fetch latest game box scores from NBA API **(critical)**
2. `nba_linker_local.py incremental` - Link unmatched props to player/game IDs **(critical)**
3. `backfill_team_ids.py` - Fill missing team IDs in staging tables *(non-critical)*
4. `update_player_position_history.py` - Update position snapshots *(non-critical)*
5. `update_league_position_averages.py` - Update league averages by position *(non-critical)*
6. `populate_average_stats_incremental.py` - Compute rolling averages for today's players only (~1s vs ~28min for full) **(critical)**
7. `backfill_opponent_allowed_incremental.py --days-back 2` - Update opponent-adjusted defensive stats **(critical)**

**Note:** `play_type_scraper.py` was removed (Session 46) because `stats.nba.com` blocks datacenter IPs. Non-critical step failures log a warning and continue; critical step failures abort the pipeline.

### Logs

Output is written to `logs/daily_stats.log`.

---

## lines_job.py

**Location:** `src/orchestration/lines_job.py`

**Purpose:** Scrape latest player prop lines and injury updates. Supports full scrapes and lightweight props-only refreshes.

**Schedule:**
- **Full scrape (`--live`):** 12 PM, 4 PM ET
- **Props-only (`--live --props-only`):** 1, 2, 3, 4:30, 5, 5:30, 6, 6:30 PM ET
- **Historical mode (no `--live`):** For backfills (uses historical API snapshots)

### Usage

```bash
# Live full scrape (game lines + props + injuries + linker)
python src/orchestration/lines_job.py --live

# Live props-only (fast — props + linker only)
python src/orchestration/lines_job.py --live --props-only

# Historical scrape for specific date
python src/orchestration/lines_job.py --date 2026-02-05

# Skip injuries (faster)
python src/orchestration/lines_job.py --live --skip-injuries

# Dry run
python src/orchestration/lines_job.py --live --props-only --dry-run
```

### CLI Arguments

| Argument | Description |
|----------|-------------|
| `--date YYYY-MM-DD` | Target date (defaults to today) |
| `--live` | Use live API endpoints; writes props to `raw_player_props_combined` |
| `--props-only` | Skip game lines and injuries (only props + linker) |
| `--dry-run` | Show what would be executed without running |
| `--skip-injuries` | Skip injury scraping (faster execution) |
| `--skip-linker` | Skip incremental linker (if already run today) |

### Pipeline Steps (Full Mode)

1. `daily_game_lines_scraper.py` - Fetch game lines from Odds API (skipped in `--props-only`)
2. `daily_player_props_scraper.py --live --target-table raw_player_props_combined` - Fetch live player props
3. `rapidapi_injury_backfill.py` - Fetch injury updates (skipped in `--props-only`)
4. `link_injury_data.py` - Link injury player names to IDs (skipped in `--props-only`)
5. `nba_linker_local.py incremental` - Link new props to player/game IDs

### Logs

Output is written to `logs/lines.log`.

---

## edge_refresh_job.py

**Location:** `src/orchestration/edge_refresh_job.py`

**Purpose:** Lightweight edge recalculation using stored MC samples and fresh prop lines. Does NOT re-run inference — no model loading, no feature engineering, no MC sampling.

**Schedule:** 2 minutes after each props-only scrape (8 times daily)

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

### Pipeline Steps

1. Load stored MC samples via `PredictionStore.get_all_samples_for_date()` — returns `dict[(player_id, game_id, stat) -> np.ndarray]`
2. Load stored predictions from `daily_predictions` for target date
3. Get unique game_ids from predictions
4. Fetch fresh prop lines from `raw_player_props_combined` (sharpest book per player/game/market)
5. Recalculate over/under probabilities from MC samples (empirical CDF)
6. Compute implied probabilities from odds (multiplicative devigging)
7. Recalculate raw edges (model prob - implied prob)
8. Recalculate Black-Litterman blended probabilities and recommendations
9. Upsert updated predictions to `daily_predictions`
10. Export CSV backup

### Key Design Decisions

- **Self-contained:** Does NOT instantiate `DailyPredictionRunner` or load model/feature pipeline. Only uses `PredictionStore`, `BlackLittermanBlender`, and raw SQL queries.
- **Graceful exit:** If no MC samples exist for the target date (inference hasn't run yet), exits with a warning and code 0.
- **Feature preservation:** Loads existing predictions and only updates line/edge/BL columns. All `feat_*` columns and quantile predictions are preserved.

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

### Pipeline Steps

1. Load model artifacts (auto-detects latest `run_*` directory, or `production/` folder)
2. Initialize FeatureStore and MonteCarloPredictor (10,000 samples)
3. Load Gaussian copula params for correlated sampling
4. Load combined calibration offsets (if `combined_calibration_offsets.json` exists — currently not deployed)
5. Check upstream data freshness (warns if rolling averages >2 days stale)
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
