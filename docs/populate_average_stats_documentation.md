# Populate Average Stats Documentation

## Overview
Computes rolling averages for player and team stats (L5, L15, season-to-date) and writes
results into:
- `player_average_game_stats`
- `player_average_advanced_stats`
- `team_average_game_stats`

## Inputs and Dependencies
- Player tables: `player_game_stats`, `player_game_advanced_stats`
- Team table: `team_game_stats`
- Uses shift(1) to ensure no data leakage from the current game.

## Output
Each table includes:
- `game_number`, `games_l5`, `games_l15`, `games_szn`
- Rolling averages for basic and advanced metrics (prefixed with `avg_`)

## Key Logic
- `calculate_games_in_window` counts prior games in each window.
- `rolling_with_groupby` computes group-safe rolling means.
- Insert functions truncate and bulk insert in batches (`BATCH_SIZE`).

## Usage
```bash
python src/processing/populate_average_stats.py
python src/processing/populate_average_stats.py --season 2024-25
python src/processing/populate_average_stats.py --table player
python src/processing/populate_average_stats.py --table player_advanced
python src/processing/populate_average_stats.py --table team
```

## Notes
- The first game of a season will have null averages (no prior history).
- Advanced stat column names are mapped via `PLAYER_ADVANCED_MAPPING`.

## Related Documentation
- [Documentation Index](index.md)
- [Feature Store](feature_store_documentation.md)
