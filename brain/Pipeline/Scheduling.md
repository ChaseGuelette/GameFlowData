# Scheduling

> Part of [[Pipeline]]

## Railway APScheduler (`src/orchestration/scheduler.py`)

Single always-on worker process. All times ET, DST-aware via `BlockingScheduler(timezone="America/New_York")`.

### NBA Job Schedule
| Time (ET) | Job | Key Flags |
|-----------|-----|-----------|
| 10:00 AM | `lines_job.py` | `--live --props-only` early scrape (shifted from 11:00 AM — Apr 25 2026) |
| 10:15 AM | `inference_job.py` | Early inference + paper bets (shifted from 11:15 AM — Apr 25 2026) |
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
| 9:00 AM | `mlb_daily_stats_job.py` | Scrape boxscores, Statcast, linker, rolling averages, resolve bets, Discord P&L (shifted from 10:00 AM — Apr 25 2026) |
| 9:20 AM | `mlb_daily_stats_job.py` (retry) | Only if 9 AM failed (shifted from 10:30 AM — Apr 25 2026) |
| 9:25 AM | `mlb_weather_job.py` | Weather fetch (shifted from 10:40 AM — Apr 25 2026) |
| 9:30 AM | `mlb_props_scrape_job.py` | `--extended` props scrape (shifted from 10:45 AM — Apr 25 2026) |
| 9:35 AM | `mlb_lineup_scrape_job.py` | Lineup scrape (shifted from 10:50 AM — Apr 25 2026) |
| 9:50 AM | `mlb_inference_job.py` | Early inference + paper bets (shifted from 11:00 AM — Apr 25 2026) |
| 12:00 PM | `mlb_inference_job.py` | Full noon inference |
| 4:00 PM | `mlb_inference_job.py` | Afternoon inference |
| */5 min, 11AM-11PM | `mlb_lines_job.py` | `--live --props-only` (silent) |
| */10 min, 11AM-11PM | `kalshi_refresh_job.py` | `--sport mlb` (silent) |

> **Note (Apr 25 2026)**: MLB and NBA early windows shifted ~1 hour earlier to gain more market exposure time. MLB stats now fires at 9:00 AM (was 10:00 AM); NBA early props at 10:00 AM (was 11:00 AM). Tradeoff: early MLB inference at 9:50 AM has fewer confirmed lineups (model defaults `lineup_position=0` for unconfirmed). Noon and 4 PM full windows are unchanged.

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
