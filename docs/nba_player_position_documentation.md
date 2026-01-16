# NBA Player Position Documentation

## Overview
Fetches current NBA player position data via `nba_api` and updates the `players` table with
`primary_position` and a normalized `position_group`.

## Inputs and Dependencies
- `nba_api.stats.endpoints.commonplayerinfo`
- Database access from `nba_unified_scraper.get_engine`
- Rate limiting via `nba_unified_scraper.rate_limit_delay`

## Output
Updates `public.players`:
- `primary_position`
- `position_group`

## Key Logic
- `get_position_group` maps raw positions to:
  - Guard -> `Guard`
  - Forward -> `Forward`
  - Center/Forward-Center -> `Big`
  - Unknown -> `Other`
- Only players missing `primary_position` are processed.

## Usage
```bash
python src/scrapers/nba_player_position.py
```

## Notes
- The script sleeps on errors to avoid rapid retry loops.
- Rate limiting is delegated to the unified scraper helper.

## Related Documentation
- [Documentation Index](index.md)
- [Player Position History](player_position_history.md)
- [Update Player Position History](update_player_position_history_documentation.md)
