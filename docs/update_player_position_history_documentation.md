# Update Player Position History Documentation

## Overview
Updates `player_position_history` with periodic snapshots of player role/position based on
the previous year of games. Supports single-date updates and full backfills.

## Inputs and Dependencies
- Database tables: `player_game_advanced_stats`, `player_game_stats`
- Uses `adv.position` values from NBA stats feeds.

## Output
Upserts into `player_position_history` with:
- `primary_position`, `position_group`, `position_confidence`, `total_games_in_window`

## Key Logic
- Snapshot window is `snapshot_date - 1 year` through `snapshot_date`.
- Position tie-breaker order: `C`, `C-F`, `F-C`, `F`, `G-F`, `F-G`, else.
- Position groups map to:
  - `G` -> Guard
  - `G-F`, `F-G`, `F` -> Wing
  - others -> Big

## Usage
```bash
python src/scrapers/update_player_position_history.py
python src/scrapers/update_player_position_history.py --date 2025-01-15
python src/scrapers/update_player_position_history.py --backfill
```

## Notes
- Backfill generates snapshots for Oct 1, Dec 25, Feb 15, and Apr 15.
- Upserts on `(player_id, snapshot_date)` to keep history current.

## Related Documentation
- [Documentation Index](index.md)
- [Player Position History](player_position_history.md)
- [Feature Store](feature_store_documentation.md)
