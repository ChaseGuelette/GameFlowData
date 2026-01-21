# Injury Scraper Job Documentation

## Overview
Orchestrates the ESPN injury scraping workflow with logging, persistence, change detection,
and optional notifications.

## Inputs and Dependencies
- Scraper: `ESPNInjuryScraper`
- Database: `InjuryDatabase`
- Change detection: `InjuryChangeDetector`
- Environment variables: `LOG_DIR`, `ALERT_EMAIL`

## Output
- Logs under `LOG_DIR`
- JSON job results file per run
- Optional alert messages via `send_alert`

## Key Steps
1. Load previous injuries (for change detection).
2. Scrape current injuries from ESPN.
3. Store current injuries in Supabase.
4. Detect changes and record counts.
5. Compute summary stats and persist results.

## Usage
```bash
python src/scrapers/injury_scraper_job.py
```

## Notes
- If no injuries are scraped, the run is marked failed and an alert is sent.
- `send_alert` is a placeholder for an email service integration.

## Related Documentation
- [Documentation Index](index.md)
- [Injury Database](injury_database_documentation.md)
