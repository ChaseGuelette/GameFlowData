# Daily Flow

> Part of [[Pipeline]]

## Daily Orchestration Timeline (ET)

```
11:00 AM  -> CDN boxscores -> linker -> rolling averages -> opponent defense -> resolve paper+user bets
11:30 AM  -> Auto-retry if 11 AM failed
12:00 PM  -> Full live props+injuries (parallel) -> linker
12:15 PM  -> Feature store -> XGBoost -> MC simulation -> edges -> BL -> paper bets -> Discord
Every 5m  -> Props-only scrape -> linker -> edge refresh (drift detection + selective re-inference)
4:00 PM   -> Full live props+injuries (parallel)
4:15 PM   -> Refresh inference (skip paper trading)
```

## Job Details

### 11:00 AM — Daily Stats (`daily_stats_job.py`)
CDN-only scrape (NEVER stats.nba.com from Railway). Steps:
1. `nba_unified_scraper.py` (CDN boxscores)
2. `nba_linker_local.py incremental`
3. `backfill_team_ids_incremental.py`
4. `update_player_position_history.py`
5. `update_league_position_averages.py`
6. `populate_average_stats_incremental.py`
7. `backfill_opponent_allowed_incremental.py`
8. Resolve ALL pending paper + user bets

Critical steps abort on failure. Non-critical steps log warning and continue. Runtime: ~3-5 minutes.

### 12:00/4:00 PM — Lines (`lines_job.py --live --parallel`)
Full props + injuries scraped concurrently via threads. Runtime: ~45-55 seconds.

### 12:15/4:15 PM — Inference (`inference_job.py`)
Full MC inference with 10K samples + Gaussian copula. 4:15 PM run uses `--skip-bets`. Runtime: ~1-3 minutes.

### Every 5 min — Props Refresh (`lines_job.py --live --props-only`)
Lightweight props-only scrape. ~156 runs/day. Silent Discord. Runtime: ~25-30 seconds.

### Every 5 min (+2 offset) — Edge Refresh (`edge_refresh_job.py`)
Recalculates edges from stored MC samples + fresh lines. Drift detection triggers selective re-inference for stale predictions. ~156 runs/day. Silent Discord. Runtime: ~2-3 minutes.

## Dependency Chain
- Inference checks daily stats succeeded (last 8 hours)
- If stale, inference still runs but with `--stale-warning` flag
- Stale data produces a separate Discord alert

#pipeline #daily #orchestration
