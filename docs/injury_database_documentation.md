# Injury Database Documentation

## Overview
Wraps Supabase access for the ESPN injury scraper. Handles insertion, change detection,
querying recent snapshots, and cleanup.

## Inputs and Dependencies
- Environment variables: `SUPABASE_URL`, `SUPABASE_KEY`
- Table: `espn_injuries`
- Data model: `InjuryRecord` from `espn_injury_scraper`

## Output
Persists injury records and provides query helpers for downstream jobs.

## Key Methods
- `store_injuries`: Inserts new records and detects changes for existing injuries.
- `_has_changed`: Checks meaningful fields (status, return date, comments).
- `get_latest_injuries`: Returns the most recent scrape snapshot.
- `get_injuries_for_date`: Returns records within a specific day window.
- `get_active_injuries_for_team`: Filters active statuses for a team.
- `get_injury_history`: Pulls player history for a rolling window.
- `get_scrape_stats`: Calculates scrape frequency and counts.
- `cleanup_old_data`: Deletes records older than the retention window.

## Usage
```python
from injury_database import InjuryDatabase

db = InjuryDatabase()
latest = db.get_latest_injuries()
```

## Notes
- Deduplication uses `(espn_injury_id, scrape_timestamp)`.
- Changes are stored as new rows rather than updates.

## Related Documentation
- [Documentation Index](index.md)
- [Injury Scraper Job](injury_scraper_job_documentation.md)
