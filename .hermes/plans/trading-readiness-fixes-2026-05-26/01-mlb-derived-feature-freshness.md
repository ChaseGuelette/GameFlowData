# Fix plan 01 — MLB derived feature data is stale

## Verdict

Production source data is current, but multiple derived/model-input tables are stale. Fix this before trusting MLB inference or Kalshi model_prob linkage.

## Evidence from remote production DB

SELECT-only SQL-runner audit on `DATABASE_URL`, statement timeout 15s:

| table | rows | freshest date |
|---|---:|---|
| `mlb_player_game_stats_batting` | 218,171 | `2026-05-25` |
| `mlb_player_game_stats_pitching` | 89,792 | `2026-05-25` |
| `mlb_game_lineups` | 145,614 | `2026-05-25` |
| `mlb_player_average_batting` | 13,124 | `2026-05-12` |
| `mlb_player_average_pitching` | 88,189 | `2026-05-11` |
| `mlb_bullpen_daily_status` | 11,056 | `2026-05-11` |
| `mlb_active_roster` | 7,789 | max `scraped_at = 2026-04-25 13:00:56+00` |
| `mlb_game_schedule` | 12,150 | max scheduled `game_date = 2026-09-27` |

Interpretation:

- External/source scrapes are not the primary blocker.
- The rolling average derivation path stopped around May 11-12.
- The bullpen workload derivation is not wired into `mlb_daily_stats_job.py`.
- The roster scraper is scheduled but stale, so it likely has a runtime/API/schema/job issue or has not actually run successfully since April 25.

## Code findings

Relevant files:

- `src/orchestration/mlb_daily_stats_job.py`
  - Steps 4 and 5 call `src/processing/mlb/mlb_populate_averages_incremental.py --type batting` and `--type pitching`.
  - It does not call `src/scrapers/mlb/mlb_bullpen_workload_scraper.py`.
- `src/processing/mlb/mlb_populate_averages_incremental.py`
  - Defaults to `date.today()` if `--date` is omitted.
  - If there is no game data for that target date, it logs “No game data found for target date. Nothing to update.” and returns success.
  - This can produce a false-green job if the job runs before same-day source rows exist; it should probably update the latest completed/stat date or yesterday, not today blindly.
- `src/scrapers/mlb/mlb_bullpen_workload_scraper.py`
  - Has a safe derived-data path: `--date`, `--yesterday`, `--backfill --start-date --end-date`.
  - Uses only existing `mlb_player_game_stats_pitching`; no external API.
- `src/orchestration/mlb_roster_scraper_job.py`
  - Scheduled at 9 AM ET via `src/orchestration/scheduler.py`.
  - Uses `MLBRosterScraper.scrape_date(date.today())` and writes `mlb_active_roster`.

## Likely root causes

1. Rolling averages are using the wrong target date in production.
   - The source stats are for completed games through `2026-05-25`.
   - The incremental average job without `--date` uses `date.today()`.
   - If today's games have not completed, there are no source rows for today, so the job can no-op while still letting the wrapper mark success.

2. Bullpen workload is not included in the daily stats orchestration.
   - The derivation script exists but no scheduler/daily job call was found.

3. Active roster has a separate stale path.
   - Roster job exists and is scheduled, but remote data max `scraped_at` is one month old.
   - Need Railway/log/API confirmation once Railway auth is restored.


## Implementation status

Implemented 2026-05-26:

- Added `scripts/refresh_mlb_derived_features.py` as a safe-by-default catch-up helper for stale MLB derived inputs.
- Patched `src/orchestration/mlb_daily_stats_job.py` so rolling averages target the latest completed source-stat date instead of blindly using today's date.
- Added bullpen workload derivation to the critical MLB daily stats path after pitching stats are available.
- Added a stale-output guard so `mlb_player_average_batting`, `mlb_player_average_pitching`, and `mlb_bullpen_daily_status` must be current through the completed source date or the job exits non-zero.
- Hardened `src/orchestration/mlb_roster_scraper_job.py` with a minimum stored-row count guard.
- Added a scheduler retry path for the roster scraper after the morning run.

Validation run:

- `./venv/Scripts/python.exe -m ruff check scripts/refresh_mlb_derived_features.py src/orchestration/mlb_daily_stats_job.py src/orchestration/mlb_roster_scraper_job.py` — passed.
- `./venv/Scripts/python.exe -m py_compile scripts/refresh_mlb_derived_features.py src/orchestration/mlb_daily_stats_job.py src/orchestration/mlb_roster_scraper_job.py` — passed.

Remaining operational follow-up:

- Run the remote catch-up only after approval, then verify production max dates with `scripts/validate_mlb_db_state.py --remote`.
- Railway/log confirmation for the roster path still belongs to production certification, not this code hotfix.

## Fix proposal

### Phase A — no-code verification / one-off catch-up plan

Ask Chase before running writes. If approved, use remote-first canonical production DB and bounded date ranges:

1. Verify the latest completed source date:
   - expected: `2026-05-25`.
2. Run rolling averages for each missing date from `2026-05-12`/`2026-05-13` through latest source date.
3. Run bullpen workload for each missing date from `2026-05-12` through latest source date.
4. Run roster scraper for current date or latest operational date.
5. Verify dates/counts again, then sync local mirror only after remote is fixed.

Suggested user-run commands after approval, from PowerShell repo root:

```powershell
.\venv\Scripts\python.exe scripts\validate_mlb_db_state.py --remote
```

Catch-up now has a safe-by-default helper. First dry-run the planned remote refresh window:

```powershell
.\venv\Scripts\python.exe scripts\refresh_mlb_derived_features.py --refresh-roster
```

Then run the bounded remote write after approval:

```powershell
.\venv\Scripts\python.exe scripts\refresh_mlb_derived_features.py --execute --refresh-roster
```

For a one-day smoke instead of the full window:

```powershell
.\venv\Scripts\python.exe scripts\refresh_mlb_derived_features.py --execute --start-date 2026-05-25 --end-date 2026-05-25
```

### Phase B — durable code fix

1. `mlb_daily_stats_job.py` now derives the latest completed source date after source scrape/linker completes:
   - computes `LEAST(MAX(game_date))` from batting and pitching source stats;
   - passes `--date <latest_completed_source_date>` to both batting and pitching average jobs.
2. Bullpen workload derivation is now a critical derived step after pitching stats are available:
   - `python -m src.scrapers.mlb.mlb_bullpen_workload_scraper --date <latest_completed_source_date>`.
3. `mlb_daily_stats_job.py` now has a stale-output guard:
   - after derived steps, asserts max dates for `mlb_player_average_batting`, `mlb_player_average_pitching`, and `mlb_bullpen_daily_status` are at least the completed source date.
   - if stale, exits non-zero; the wrapper should not mark success.
4. `mlb_roster_scraper_job.py` is hardened:
   - fails non-zero if stored count is below `--min-roster-entries` (default `600`);
   - this makes scheduler/Discord retry/alerts fire for empty or partial roster scrapes.
5. `scheduler.py` now includes an automatic 10:05 AM ET retry cron for the roster scraper if the 9 AM run failed or under-counted.

## Verification

Run after fix/catch-up:

```powershell
.\venv\Scripts\python.exe scripts\validate_mlb_db_state.py --remote
```

Expected:

- `mlb_player_average_batting.max_date >= latest source game_date`
- `mlb_player_average_pitching.max_date >= latest source game_date`
- `mlb_bullpen_daily_status.max_date >= latest source game_date`
- `mlb_active_roster.scraped_at` current/recent and row count roughly active-roster scale

## Non-goals

- Do not full-truncate/reload remote derived tables unless explicitly approved.
- Do not run remote-to-local sync before production remote is fixed.
- Do not treat scheduler job success as enough; verify table max dates.
