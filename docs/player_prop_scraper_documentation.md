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

## Market Selection
- `CORE_MARKETS`: player_points, player_rebounds, player_assists, player_threes, player_blocks, player_steals, player_turnovers
- `COMBO_MARKETS`: player_points_rebounds_assists, player_points_rebounds, player_points_assists, player_rebounds_assists, player_double_double, player_triple_double

## Resume Capability
- Progress file format: `{"markets": "<sorted_markets_key>", "processed": [[ts, eid], ...]}`
- Market-aware: different market sets use independent progress tracking. If the markets key differs from the stored file, progress is discarded and scraping starts fresh.
- Events already in `processed_ids` are skipped in the main loop.
- Progress is saved after each snapshot and on interrupt/error for reliable resumption.
- `--no-resume` flag deletes the progress file before starting.

## Usage
```bash
# Full backfill with core markets (default)
python src/scrapers/player_prop_scraper.py

# Core + combo markets
python src/scrapers/player_prop_scraper.py --combos

# Only combo markets
python src/scrapers/player_prop_scraper.py --combos-only

# Specific markets
python src/scrapers/player_prop_scraper.py --markets player_double_double player_triple_double

# Date range + dry run (credit estimation)
python src/scrapers/player_prop_scraper.py --start-date 2025-01-01 --end-date 2025-06-30 --dry-run

# Resume from where you left off (default behavior)
python src/scrapers/player_prop_scraper.py --combos

# Start fresh (ignore progress file)
python src/scrapers/player_prop_scraper.py --combos --no-resume
```

## Notes
- Rate limiting is enforced with sleep calls and exponential backoff on 429 responses.
- 401 with "OUT_OF_USAGE_CREDITS" triggers save_progress and raises an exception.
- Credit cost: ~10 credits per market per event per region call.

## Related Documentation
- [Documentation Index](index.md)
- [NBA Linker Local](nba_linker_local_documentation.md)
- [Game Lines Scraper](game_lines_scraper_documentation.md)
