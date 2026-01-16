# NBA Linker Local Documentation

## Overview
Downloads raw tables to CSV, performs local ID matching in pandas, and uploads the matched
results back to the database. Includes fuzzy date matching for player props to handle bad
commence times from Odds API.

## Inputs and Dependencies
- Database tables:
  - `teams`, `players`, `team_game_stats`, `player_game_stats`
  - `raw_game_lines_staging`, `raw_player_props_combined`
- Local data directory: `linker_data/`

## Output Files
Written under `linker_data/`:
- `game_lines_updates.csv`
- `props_game_updates.csv`
- `props_player_updates.csv`
- `props_full_updates.csv`
- `unmatched_game_lines.csv`
- `unmatched_games.csv`
- `unmatched_players.csv`
- `player_mappings.csv` (manual mapping input)

## Key Logic
- Team name normalization uses `TEAM_NAME_ALIASES`.
- Fuzzy date matching searches within `FUZZY_DATE_WINDOW_DAYS` (default 90).
- Player matching uses manual mappings first, then normalized name lookup.
- Team ID backfill uses `(player_id, game_id)` from `player_game_stats`.

## Usage
```bash
python src/processing/nba_linker_local.py download
python src/processing/nba_linker_local.py process
python src/processing/nba_linker_local.py upload
python src/processing/nba_linker_local.py init
python src/processing/nba_linker_local.py all
```

## Notes
- `init` creates an empty `player_mappings.csv` template.
- Upload uses temp tables and chunked updates for safety.

## Related Documentation
- [Documentation Index](index.md)
- [Game Lines Scraper](game_lines_scraper_documentation.md)
- [Player Prop Scraper](player_prop_scraper_documentation.md)
