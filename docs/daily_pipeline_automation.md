# Daily Pipeline Automation (E6)

Documentation for the frequency-separated job scripts that automate the GameFlowData daily pipeline.

## Overview

The daily pipeline is split into three jobs based on execution frequency:

| Job | Schedule (ET) | Purpose | Runtime |
|-----|---------------|---------|---------|
| `daily_stats_job.py` | 9:00 AM | NBA game results + processing | ~5-10 min |
| `lines_job.py` | 12 PM, 4 PM, 6 PM | Props + injuries + linking | ~30-90 sec |
| `inference_job.py` | 6:30 PM | Generate predictions | ~1-3 min |

## Pipeline Timeline

```
9:00 AM   daily_stats_job.py
          ├─ nba_unified_scraper.py (NBA game results)
          ├─ nba_linker_local.py incremental
          ├─ backfill_team_ids.py
          ├─ update_player_position_history.py
          ├─ update_league_position_averages.py
          ├─ populate_average_stats_incremental.py (lightweight, ~1s)
          └─ backfill_opponent_allowed.py

12:00 PM  lines_job.py (first run)
          ├─ daily_game_lines_scraper.py
          ├─ daily_player_props_scraper.py
          ├─ rapidapi_injury_backfill.py
          ├─ link_injury_data.py
          └─ nba_linker_local.py incremental

4:00 PM   lines_job.py (second run)

6:00 PM   lines_job.py (final run)

6:30 PM   inference_job.py
          ├─ Load model artifacts
          ├─ DailyPredictionRunner.run_for_date()
          ├─ Store to daily_predictions table
          └─ Export predictions CSV

7:00 PM   Games typically start
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

1. `nba_unified_scraper.py` - Fetch latest game box scores from NBA API
2. `nba_linker_local.py incremental` - Link unmatched props to player/game IDs
3. `backfill_team_ids.py` - Fill missing team IDs in staging tables
4. `update_player_position_history.py` - Update position snapshots
5. `update_league_position_averages.py` - Update league averages by position
6. `populate_average_stats_incremental.py` - Compute rolling averages for today's players only (~1s vs ~28min for full)
7. `backfill_opponent_allowed.py` - Update opponent-adjusted defensive stats

### Logs

Output is written to `logs/daily_stats.log`.

---

## lines_job.py

**Location:** `src/orchestration/lines_job.py`

**Purpose:** Scrape latest player prop lines and injury updates. Run multiple times daily to capture line movement.

**Schedule:** Multiple times daily (12 PM, 4 PM, 6 PM ET)

### Usage

```bash
# Normal run for today
python src/orchestration/lines_job.py

# Specific date
python src/orchestration/lines_job.py --date 2026-02-05

# Skip injuries (faster)
python src/orchestration/lines_job.py --skip-injuries

# Skip linker (if already run)
python src/orchestration/lines_job.py --skip-linker

# Dry run
python src/orchestration/lines_job.py --dry-run
```

### CLI Arguments

| Argument | Description |
|----------|-------------|
| `--date YYYY-MM-DD` | Target date (defaults to today) |
| `--dry-run` | Show what would be executed without running |
| `--skip-injuries` | Skip injury scraping (faster execution) |
| `--skip-linker` | Skip incremental linker (if already run today) |

### Pipeline Steps

1. `daily_game_lines_scraper.py` - Fetch game lines (spreads, totals) from Odds API
2. `daily_player_props_scraper.py` - Fetch player props from Odds API
3. `rapidapi_injury_backfill.py` - Fetch injury updates from RapidAPI (optional)
4. `link_injury_data.py` - Link injury player names to IDs (optional)
5. `nba_linker_local.py incremental` - Link new props to player/game IDs (optional)

### Logs

Output is written to `logs/lines.log`.

---

## inference_job.py

**Location:** `src/orchestration/inference_job.py`

**Purpose:** Generate predictions for today's games using the latest model artifacts and prop lines.

**Schedule:** Once daily, 6:30 PM ET (after final lines_job, before games start)

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

### Pipeline Steps

1. Load model artifacts (auto-detects latest `run_*` directory)
2. Initialize FeatureStore and MonteCarloPredictor (10,000 samples)
3. Load Gaussian copula params for correlated sampling
4. Run `DailyPredictionRunner.run_for_date()`
5. Store predictions to `daily_predictions` table
6. Store MC samples to `daily_prediction_samples` table
7. Export CSV backup to `predictions/predictions_YYYY-MM-DD.csv`

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

### Option 2: Linux Cron (Server)

For server deployment, use the template at `cron/gameflow_crontab.txt`.

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

- [Model Pipeline Runbook](model_pipeline_runbook.md) - Full training and inference guide
- [NBA Linker Local](nba_linker_local_documentation.md) - ID matching and linking
- [Feature Store](feature_store_documentation.md) - Feature generation
- [Spec Document](.session/specs/E6_daily_automation.md) - Full specification
