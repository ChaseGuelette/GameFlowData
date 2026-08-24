# Daily pipeline automation

The production scheduler is `src/orchestration/scheduler.py`. It runs as one APScheduler worker in the `America/New_York` timezone. The code is the schedule authority; this page is an operator summary and must be updated whenever scheduler registrations change.

## Safety boundary

- Railway NBA result collection invokes `src/scrapers/nba_unified_scraper.py --cdn-only` from `daily_stats_job.py`.
- Never add `stats.nba.com` calls to Railway jobs.
- Long backfills, training, and sweeps are not scheduler jobs; Chase launches them after a dry-run/preflight.
- Scheduled model work uses the three retained MLB production stats: `pitcher_strikeouts`, `batter_hits`, and `batter_rbis`.
- Job execution history is best-effort persisted to `job_executions`; a persistence failure does not convert a failed job into success.

## NBA jobs

| Time (ET) | Job |
|---|---|
| 3:00 AM | Archive old combined prop rows |
| 9:00 AM | Daily stats/results and processing |
| 9:30 AM | Daily-stats retry when needed; resolve user paper bets |
| 10:00 AM | Props-only scrape |
| 10:15 AM | Early inference |
| 12:01 PM | Full lines scrape when `NBA_FULL_LINES_ENABLED` is enabled |
| 12:15 PM | Inference |
| 4:01 PM | Full parallel lines scrape when enabled |
| 4:15 PM | Inference with paper-bet placement skipped |
| Every 5 minutes, 9 AM–11 PM | Props-only scrape at `:00/:05/...`; edge refresh at `:02/:07/...` |

`lines_job.py` is protected against overlapping scheduler launches. The parked NBA linker failure remains visible as a deferred failure; do not reinterpret it as healthy.

## MLB jobs

| Time (ET) | Job |
|---|---|
| 2:20 AM | Dense CLV snapshot job; internally environment-gated |
| 9:00 AM | Active roster and daily stats |
| 9:20 AM | Daily-stats retry |
| 9:25 AM | Weather forecast |
| 9:30 AM | Props-only scrape |
| 9:35 / 9:36 AM | Lineup and umpire collection |
| 9:50 AM | Early inference |
| 10:05 AM | Roster retry |
| 12:00 / 12:05 / 12:15 PM | Full lines, lineup, inference |
| 12:45 / 1:00 / 1:30 PM | Lineup, props-only, inference |
| 2:30 / 4:30 PM | Edge refresh |
| 5:00 / 6:00 / 6:10 / 6:30 PM | Full lines, props-only, lineup, inference |
| Every 10 minutes, 10 AM–11 PM | Approximately T-30 pre-commence prop snapshot capture |

## Entry points

- NBA: `daily_stats_job.py`, `lines_job.py`, `inference_job.py`, `edge_refresh_job.py`.
- MLB: `mlb_daily_stats_job.py`, `mlb_lines_job.py`, `mlb_inference_job.py`, `mlb_edge_refresh_job.py`, lineup/roster/weather/umpire jobs, and `mlb_dense_clv_job.py`.
- Maintenance: `archive_old_props_job.py`, `resolve_user_paper_bets.py`.

All paths above are under `src/orchestration/`.

## Local dry checks

These commands inspect behavior without starting the persistent scheduler or performing provider/DB writes:

```powershell
Set-Location 'C:\Users\Chase\Projects\GameFlowData'
.\venv\Scripts\python.exe -m compileall -q src\orchestration
.\venv\Scripts\python.exe src\orchestration\daily_stats_job.py --dry-run
.\venv\Scripts\python.exe src\orchestration\lines_job.py --live --parallel --dry-run
```

Do not run `scheduler.py` casually from a development shell: it starts the blocking production schedule. Use Railway logs/status for deployed behavior.

## Monitoring

- Scheduler startup logs enumerate every registered job and trigger.
- Job completion/failure alerts use `src/discord_bot/alerts.py` when Discord is configured.
- High-frequency refresh jobs are silent on success and alert on failure.
- Inference checks recent daily-stats success; stale upstream data is warned, not represented as fresh.

See [`railway_deployment.md`](railway_deployment.md) for deployment and rollback boundaries.
