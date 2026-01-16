# Update League Position Averages Documentation

## Overview
Calculates and upserts league-wide averages by position group for each season in
`league_position_averages`. This table holds full-season averages (not monthly snapshots).

## Inputs and Dependencies
- Database tables: `player_game_stats`, `team_game_stats`,
  `player_game_advanced_stats`, `player_position_history`
- Uses `player_position_history` to determine the most recent position group before each game.

## Output
Upserts into `league_position_averages` with per-100 possession metrics and totals.

## Key Logic
- `normalize_season_id` accepts `2024-25`, `22024`, or `2024` and returns DB format.
- SQL aggregates stats per 100 possessions and counts total games and possessions.
- Upsert via `ON CONFLICT (season_id, position_group)` keeps data current.

## Usage
```bash
python src/scrapers/update_league_position_averages.py
python src/scrapers/update_league_position_averages.py --season 2024-25
python src/scrapers/update_league_position_averages.py --season 22024
```

## Notes
- Filters out rows with `pgs.min <= 0` or `adv.possessions <= 0`.
- Uses the most recent position snapshot prior to game date.

## Related Documentation
- [Documentation Index](index.md)
- [Player Position History](player_position_history.md)
- [Update Player Position History](update_player_position_history_documentation.md)
