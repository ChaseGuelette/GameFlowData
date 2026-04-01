# Scheduling

> Part of [[Pipeline]]

## Railway APScheduler (`src/orchestration/scheduler.py`)

Single always-on worker process. All times ET, DST-aware via `BlockingScheduler(timezone="America/New_York")`.

### NBA Job Schedule
| Time (ET) | Job | Key Flags |
|-----------|-----|-----------|
| 11:00 AM | `daily_stats_job.py` | CDN-only scrape |
| 11:30 AM | `daily_stats_job.py` (retry) | Only if 11 AM failed |
| 12:00 PM | `lines_job.py` | `--live --parallel` |
| 12:15 PM | `inference_job.py` | Full inference + paper bets |
| 4:00 PM | `lines_job.py` | `--live --parallel` |
| 4:15 PM | `inference_job.py` | `--skip-bets` (refresh only) |
| */5 min, 11AM-11PM | `lines_job.py` | `--live --props-only` (silent) |
| */5 min +2, 11AM-11PM | `edge_refresh_job.py` | `--skip-paper` (silent) |
| */10 min, 11AM-11PM | `kalshi_refresh_job.py` | `--sport nba` (silent) |

### MLB Job Schedule
| Time (ET) | Job | Key Flags |
|-----------|-----|-----------|
| 10:00 AM | `mlb_daily_stats_job.py` | Scrape boxscores, Statcast, linker, rolling averages, resolve bets, Discord P&L |
| 10:30 AM | `mlb_daily_stats_job.py` (retry) | Only if 10 AM failed |
| 1:30 PM | `mlb_inference_job.py` | **`--skip-bets`** (disabled until leaky models retrained) |
| 6:30 PM | `mlb_inference_job.py` | **`--skip-bets`** (disabled until leaky models retrained) |
| */5 min, 11AM-11PM | `mlb_lines_job.py` | `--live --props-only` (silent) |
| */10 min, 11AM-11PM | `kalshi_refresh_job.py` | `--sport mlb` (silent) |

### Job Status Tracking
- `JOB_STATUS` in-memory dict tracks status, end time, duration
- `record_job_execution()` writes to `job_executions` Supabase table
- `check_dependency()` gates downstream jobs (e.g., inference checks daily stats)

### Retry & Resilience
- 11:30 AM auto-retry if 11 AM daily stats failed
- Critical steps abort on failure, non-critical continue
- Per-step retries with exponential backoff (2 retries, 15s base)
- Step 6 timeout: 20m, Step 7: 15m, Steps 3-5: 5m
- Global scheduler timeout: 45m

### Silent Alerts
5-minute jobs use `silent_on_success=True` — Discord alerts only on failure. Full scrape and inference alert on success.

## Local Windows Task Scheduler
Only `scripts/run_advanced_scraper.bat` runs locally:
- 9 AM ET
- Flags: `--no-proxy --skip-team --skip-traditional`
- NEVER run on Railway, NEVER use a proxy

#scheduling #railway #infrastructure
