# Game Lines Scraper Documentation

## Overview
Scrapes historical NBA game lines (moneyline, spreads, totals) from The Odds API and writes
snapshots into `raw_game_lines_staging`.

## Inputs and Dependencies
- Environment variables: `DATABASE_URL`, `ODDS_API_KEY`
- API endpoint: `/v4/historical/sports/basketball_nba/odds`

## Output
Writes rows to `raw_game_lines_staging` with:
- `api_game_id`, `bookmaker`, `market_key`, `outcome_label`
- `line`, `odds_american`, `commence_time`, `home_team`, `away_team`
- `snapshot_time`, `market_last_update`, `bookmaker_last_update`, `bookmaker_name`

## Key Logic
- `get_bulk_odds` retries on 429 and returns credit usage from response headers.
- `parse_and_store` flattens bookmakers/markets/outcomes into row tuples.
- `generate_historical_schedule` builds two daily snapshots per season.

## Usage
```bash
python src/scrapers/game_lines_scraper.py
```

## Notes
- The schedule starts with the 2019-20 bubble due to API availability.
- Rate limiting is handled via sleep and retry loops.

## Related Documentation
- [Documentation Index](index.md)
- [NBA Linker Local](nba_linker_local_documentation.md)
- [Feature Store](feature_store_documentation.md)
