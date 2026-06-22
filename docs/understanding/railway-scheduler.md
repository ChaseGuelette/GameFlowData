# Railway Scheduler

This page explains the production scheduler subsystem from current code. It is a human mental model, not a replacement for checking Railway logs, `job_executions`, or live environment variables.

Evidence used:
- `railway.toml` starts one Railway worker with `/app/venv/bin/python src/orchestration/scheduler.py`.
- `nixpacks.toml` builds the Railway Python venv and sets `LD_LIBRARY_PATH` for native libraries.
- `src/orchestration/scheduler.py` defines the APScheduler loop, job wrappers, env gates, alerts, dependency checks, and schedule.
- `tests/test_pipeline_resilience.py` covers dependency checks, retry behavior, timeout status, deferred NBA lines failure tagging, and `lines_job.py` overlap skipping.
- `docs/railway_deployment.md` and `docs/daily_pipeline_automation.md` are useful historical docs, but their job counts/times are stale against current scheduler code.

## One-sentence model

Railway runs one always-on Python process; APScheduler inside `src/orchestration/scheduler.py` launches many short-lived `src/orchestration/*_job.py` subprocesses on ET schedules, records each run to `job_executions`, and sends Discord alerts unless a job is configured as silent-on-success.

## Runtime boundary

```text
Railway service
  -> start command: /app/venv/bin/python src/orchestration/scheduler.py
  -> BlockingScheduler(timezone="America/New_York")
  -> wrapper function, e.g. run_mlb_inference()
  -> run_job(script_name, extra_args, silent_on_success, timeout)
  -> subprocess: sys.executable src/orchestration/<script_name> <args>
  -> Supabase writes: job outputs + job_executions + domain tables
  -> Discord alerts: success/failure depending on silent_on_success
```

Important implication: most business logic is not inside the scheduler. The scheduler chooses when/how to call job entrypoints. The job scripts own scraping, inference, linking, trading, and settlement behavior.

## Core mechanisms

### Schedule source of truth

Current production schedule lives in `src/orchestration/scheduler.py`, not `railway.toml` cron entries. `railway.toml` starts the single always-on scheduler process.

### Timezone

The scheduler uses `BlockingScheduler(timezone="America/New_York")` and ET `CronTrigger`s. That means DST is handled by APScheduler/pytz rather than by manually converting to UTC cron strings.

### `run_job()`

`run_job()` is the shared launcher:
- builds a command using `sys.executable`, so Railway subprocesses use the same venv as the scheduler;
- captures stdout/stderr;
- applies a timeout;
- logs final output lines on success;
- sends Discord alerts unless success is silent;
- updates in-memory `JOB_STATUS`;
- records persistent history in Supabase `job_executions`.

### `JOB_STATUS` and `job_executions`

`JOB_STATUS` is in-memory and fast, but disappears on redeploy/restart. `check_dependency()` first checks `JOB_STATUS`, then falls back to the `job_executions` table so dependency gates can survive process restarts.

### Dependency gates are warning gates, not hard stops

NBA inference checks whether `daily_stats_job.py` succeeded within 8 hours. If not, it sends a stale-data warning and runs inference with `--stale-warning`.

MLB inference checks whether `mlb_daily_stats_job.py` succeeded within 8 hours. Current code logs the stale-data warning but still runs MLB inference.

### Retry jobs

The scheduler has explicit retry wrappers for selected jobs:
- NBA daily stats retry at 9:30 AM ET if the 9:00 AM run did not succeed.
- MLB daily stats retry at 9:20 AM ET if the 9:00 AM run did not succeed.
- MLB roster scraper retry at 10:05 AM ET if the 9:00 AM roster scrape did not succeed.

### Overlap protection

Only `lines_job.py` is currently in `LOCKABLE_JOB_SCRIPTS`. If another NBA lines subprocess is still active, a second scheduler-triggered NBA lines run is skipped and persisted as `status='skipped'` rather than launching concurrently.

This matters because props-only jobs run every 5 minutes and full NBA lines jobs can also run near those windows.

### Alerts

Most high-value scheduled jobs alert on success and failure. High-frequency jobs usually set `silent_on_success=True`, so Discord only sees failures. This keeps noisy jobs observable without spamming normal success messages.

### Deferred NBA lines failure tagging

`lines_job.py` failures are tagged as NBA-deferred in alert/status text. This does not make the job pass; it keeps known parked NBA lines failures distinguishable from fresh shared-infra regressions.

## Environment gates and defaults

| Gate | Default | What it controls | Notes |
|---|---:|---|---|
| `NBA_FULL_LINES_ENABLED` | true | Adds/removes noon and 4:01 PM full NBA lines jobs | Does not disable props-only NBA refresh, NBA inference, edge refresh, or Kalshi NBA refresh. |
| `NBA_PLAYOFF_MODE` | false | Adds playoff model-dir args to NBA inference | Seasonal model selection only. |
| `MLB_DENSE_CLV_ENABLED` | false in job script | Dense MLB CLV snapshot capture | Scheduler schedules the wrapper unconditionally; the job itself is env-gated. |
| `ARB_SCANNER_ENABLED` | false | Adds/removes arb scan jobs | Arb lane is parked by default. |
| `ARB_SCRAPING_ENABLED` | false | Enables non-sports Polymarket scrape and controls scrape behavior for MLB arb scan | Large scrape is default-off. |
| `ARB_ALERTS_ENABLED` | false | Allows arb Discord alerts | Otherwise arb scan passes `--skip-discord`. |
| `ARB_PAPER_TRADING_ENABLED` | false | Allows arb paper trading | Otherwise arb scan passes `--skip-paper`. |
| `KALSHI_LIVE_TRADING_ENABLED` | false in job scripts | Live Kalshi execution/repricing/cancellation behavior | Scheduler still polls executor jobs; job scripts exit gracefully when disabled. |

## Current schedule map

All times are ET.

### Maintenance and shared jobs

| Time | Scheduler id | Wrapper | Effect |
|---|---|---|---|
| 2:20 AM | `mlb_dense_clv_snapshots` | `run_mlb_dense_clv_snapshots` | Bounded/resume-aware MLB dense CLV capture; job-level env gate. |
| 3:00 AM | `archive_old_props` | `run_archive_old_props` | Archive old `raw_player_props_combined` rows; silent on success. |
| 9:30 AM | `user_paper_bet_resolution` | `run_user_paper_bet_resolution` | Resolve pending user paper bets. |

### NBA jobs

| Time | Scheduler id | Wrapper | Effect |
|---|---|---|---|
| 9:00 AM | `daily_stats` | `run_daily_stats` | NBA daily stats job. Railway-safe invariant: CDN-only, no `stats.nba.com`. |
| 9:30 AM | `daily_stats_retry` | `run_daily_stats_retry` | Retry only if 9:00 AM daily stats did not succeed. |
| 10:00 AM | `lines_props_10am` | `run_lines_props_only` | Early NBA props-only scrape before 10:15 inference. |
| 10:15 AM | `inference_1015am` | `run_inference` | Early NBA inference. |
| 12:01 PM | `lines_noon_full` | `run_lines_full` | Full NBA lines scrape; only added when `NBA_FULL_LINES_ENABLED=true`. |
| 12:15 PM | `inference_noon` | `run_inference` | Full NBA inference. |
| every 5 min 9 AM-11 PM | `props_every_5` | `run_lines_props_only_silent` | NBA props-only refresh; silent on success. |
| every 5 min 9 AM-11 PM, offset +2 min | `edge_refresh_every_5` | `run_edge_refresh_silent` | NBA edge refresh with `--skip-paper`; silent on success. |
| 4:01 PM | `lines_4pm_full` | `run_lines_full_parallel` | Full parallel NBA lines scrape; only added when `NBA_FULL_LINES_ENABLED=true`. |
| 4:15 PM | `inference_4pm` | `run_inference(skip_bets=True)` | NBA inference without automatic paper bet placement. |

### MLB jobs

| Time | Scheduler id | Wrapper | Effect |
|---|---|---|---|
| 9:00 AM | `mlb_roster_scraper` | `run_mlb_roster_scraper` | MLB active roster / IL tracking. |
| 9:00 AM | `mlb_daily_stats` | `run_mlb_daily_stats` | MLB daily stats. |
| 9:20 AM | `mlb_daily_stats_retry` | `run_mlb_daily_stats_retry` | Retry if daily stats failed/missing. |
| 9:25 AM | `mlb_weather_forecast` | `run_mlb_weather_forecast` | Weather forecast before inference windows. |
| 9:30 AM | `mlb_lines_props_930am` | `run_mlb_lines_props_only` | Early props-only scrape. |
| 9:35 AM | `mlb_lineup_scraper_935am` | `run_mlb_lineup_scraper` | Early lineup scrape. |
| 9:36 AM | `mlb_umpire_scraper_936am` | `run_mlb_umpire_scraper` | Umpire assignments. |
| 9:50 AM | `mlb_inference_950am` | `run_mlb_inference` | Early MLB inference. |
| 10:05 AM | `mlb_roster_scraper_retry` | `run_mlb_roster_scraper_retry` | Retry if roster scrape failed/missing. |
| 12:00 PM | `mlb_lines_full_noon` | `run_mlb_lines_full` | Full MLB lines scrape. |
| 12:05 PM | `mlb_lineup_scraper_1205pm` | `run_mlb_lineup_scraper` | Midday lineup scrape. |
| 12:15 PM | `mlb_inference_noon` | `run_mlb_inference` | Noon MLB inference. |
| 12:45 PM | `mlb_lineup_scraper_1pm` | `run_mlb_lineup_scraper` | Afternoon lineup confirmation. |
| 1:00 PM | `mlb_lines_props_1pm` | `run_mlb_lines_props_only` | Props-only refresh before afternoon games. |
| 1:30 PM | `mlb_inference_1pm` | `run_mlb_inference` | Afternoon/evening MLB inference. |
| 2:30 PM | `mlb_edge_refresh_230pm` | `run_mlb_edge_refresh` | MLB edge refresh. |
| 4:30 PM | `mlb_edge_refresh_430pm` | `run_mlb_edge_refresh` | MLB edge refresh before evening games. |
| 5:00 PM | `mlb_lines_full_5pm` | `run_mlb_lines_full` | Full lines scrape for evening props. |
| 6:00 PM | `mlb_lines_props_6pm` | `run_mlb_lines_props_only` | Props-only refresh before evening games. |
| 6:10 PM | `mlb_lineup_scraper_6pm` | `run_mlb_lineup_scraper` | Evening lineup confirmation. |
| 6:30 PM | `mlb_inference_6pm` | `run_mlb_inference` | Evening MLB inference. |
| every 10 min 10 AM-11 PM | `mlb_pregame_30min_props` | `run_mlb_pregame_30min_props` | Captures close-ish props around commence_time -30m; script filters by game time. |

### Kalshi jobs

| Time | Scheduler id | Wrapper | Effect |
|---|---|---|---|
| 9:15 AM | `kalshi_live_resolution` | `run_kalshi_live_resolution` | Resolve yesterday's NBA and MLB Kalshi bets. |
| 10:00 AM | `kalshi_daily_summary` | `run_kalshi_daily_summary` | Daily P&L/summary. |
| every 10 min 9 AM-11 PM on :00 | `kalshi_refresh_mlb` | `run_kalshi_refresh_mlb` | MLB market refresh first for exposure priority. |
| every 10 min 9 AM-11 PM on :02 | `kalshi_refresh_nba` | `run_kalshi_refresh` | NBA market refresh after MLB. |
| every 10 min 9 AM-11 PM | `kalshi_nonsports_refresh` | `run_kalshi_nonsports_refresh` | Non-sports Kalshi market refresh. |
| every 2 min 9 AM-11 PM | `kalshi_execute_approved` | `run_kalshi_execute_approved` | Execute dashboard-approved trades if live trading is enabled in job script. |
| every 4 min 9 AM-11 PM | `kalshi_reprice_stale` | `run_kalshi_reprice_stale` | Reprice stale resting orders if live trading is enabled in job script. |
| every 5 min 9 AM-11 PM | `kalshi_pending_fills` | `run_kalshi_pending_fills` | Poll pending fills. |
| every 5 min 9 AM-11 PM | `kalshi_stale_fills` | `run_kalshi_stale_fills` | Detect pending orders whose games started and enqueue cancellation review. |
| every 4 min 9 AM-11 PM | `kalshi_execute_cancellations` | `run_kalshi_execute_cancellations` | Execute human-approved cancellations. |

### Arbitrage jobs

These are default-off by env gates.

| Time | Scheduler id | Wrapper | Gate | Effect |
|---|---|---|---|---|
| every 10 min 12:05 PM-11:05 PM | `arb_scan_mlb` | `run_arb_scan_mlb` | `ARB_SCANNER_ENABLED=true` | MLB sport arb scan; scrape/alerts/paper each have additional gates. |
| 9 AM and 5 PM | `nonsports_scrape` | `run_nonsports_scrape` | `ARB_SCRAPING_ENABLED=true` | Slow all-category Polymarket scrape. |
| every 30 min 9 AM-11 PM | `arb_scan_all_categories` | `run_arb_scan_all_categories` | `ARB_SCANNER_ENABLED=true` | Non-sports scan using existing scraped Polymarket data. |

## What can go wrong

- A missing required env var can let the scheduler start but cause specific jobs to fail.
- A source/provider issue can show up as a job failure, stale inference warning, missing rows, or no dashboard picks.
- `NBA_FULL_LINES_ENABLED=false` can be misunderstood as “all NBA paused”; it is not.
- High-frequency jobs can hide problems if Chase only watches Discord success messages; use Railway logs and `job_executions` for full history.
- Historical docs can be stale because the scheduler schedule changes faster than the docs.

## Audit findings from this review

- The current scheduler has two NBA props-only triggers at 10:00 AM ET: `lines_props_10am` and the generic `props_every_5` loop. Because `lines_job.py` is lockable, one duplicate run should be skipped rather than overlap, but this can create noisy `job_executions` rows and makes the 10 AM schedule harder to explain.
- The older `docs/railway_deployment.md` and `docs/daily_pipeline_automation.md` schedule sections are stale against current code. They still contain useful deployment/resilience context, but schedule truth should come from this page plus `src/orchestration/scheduler.py` until those docs are refreshed or delegated.

## How Chase can verify later

- Scheduler process/config: `railway.toml`, `nixpacks.toml`, Railway service start logs.
- Current schedule: `src/orchestration/scheduler.py`, especially `scheduler.add_job(...)` blocks.
- Recent runtime behavior: Railway logs and `job_executions` rows.
- Unit-level safety behavior: `tests/test_pipeline_resilience.py`.

## What to remember

The scheduler is an orchestration router, not the model or scraper itself. When debugging, first ask: did the scheduler launch the right job, with the right args and env gates, at the right time? Only then debug the job script's domain behavior.
