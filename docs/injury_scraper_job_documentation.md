# Injury Scraper Job Documentation

> **⚠️ DEPRECATED:** This ESPN-based scraper is no longer used in the daily pipeline.
> The `--scrape-injuries` flag in `run_daily.py` now uses `rapidapi_injury_backfill.py`
> instead, which writes to `rapidapi_injuries` (the table used by feature store and daily runner).
> See `rapidapi_injury_backfill.py` for current injury scraping.

## Overview
Orchestrates the ESPN injury scraping workflow with logging, persistence, change detection,
and optional notifications. **Note:** This writes to `espn_injuries` table, which is NOT
used by the ML pipeline (feature store, daily runner, backtest harness all use `rapidapi_injuries`).

## Inputs and Dependencies
- Scraper: `ESPNInjuryScraper`
- Database: `InjuryDatabase`
- Change detection: `InjuryChangeDetector`
- Environment variables: `LOG_DIR`, `ALERT_EMAIL`

## Output
- Logs under `LOG_DIR`
- JSON job results file per run
- Optional alert messages via `send_alert`
- Data in `espn_injuries` table (not used by ML pipeline)

## Key Steps
1. Load previous injuries (for change detection).
2. Scrape current injuries from ESPN.
3. Store current injuries in Supabase.
4. Detect changes and record counts.
5. Compute summary stats and persist results.

## Usage
```bash
# Direct usage (writes to espn_injuries - NOT recommended for ML pipeline)
python src/scrapers/injury_scraper_job.py

# Recommended: Use RapidAPI scraper via daily orchestrator
python src/orchestration/run_daily.py --scrape-injuries
```

## Notes
- If no injuries are scraped, the run is marked failed and an alert is sent.
- `send_alert` is a placeholder for an email service integration.

## Related Documentation
- [Documentation Index](index.md)
- [Injury Database](injury_database_documentation.md)
