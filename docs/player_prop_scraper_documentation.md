# Player Prop Scraper Documentation

## Overview
Scrapes historical NBA player prop markets from The Odds API and writes raw snapshots into
`raw_player_props_staging_v2`. It captures multiple daily snapshots to support opening and
closing line analysis.

## Inputs and Dependencies
- Environment variables: `DATABASE_URL`, `ODDS_API_KEY`
- API endpoints:
  - `/v4/historical/sports/basketball_nba/events`
  - `/v4/historical/sports/basketball_nba/events/{eventId}/odds`
- Local progress file: `scrape_progress.json` (stores processed IDs)

## Output
Writes rows to `raw_player_props_staging_v2` with the following key fields:
- `api_game_id`, `api_player_name`, `bookmaker`, `market_key`, `outcome_label`
- `line`, `odds_american`, `commence_time`, `home_team`, `away_team`
- `snapshot_time`, `market_last_update`, `bookmaker_last_update`, `bookmaker_name`

## Key Logic
- `get_events_for_date` fetches event IDs for a snapshot timestamp with retry handling.
- `scrape_event_props` fetches player props for target markets and handles 422/429/401 cases.
- `parse_and_store` inserts every valid snapshot row; skips outcomes missing player names.
- `generate_snapshot_timestamps` builds the seasonal snapshot schedule and skips Jul-Sep.

## Usage
```bash
python src/scrapers/player_prop_scraper.py
```

## Notes
- Rate limiting is enforced with sleep calls and exponential backoff on 429 responses.
- The local progress file enables resumable runs.

## Related Documentation
- [Documentation Index](index.md)
- [NBA Linker Local](nba_linker_local_documentation.md)
- [Game Lines Scraper](game_lines_scraper_documentation.md)
