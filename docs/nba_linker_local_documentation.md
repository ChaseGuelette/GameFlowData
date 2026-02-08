# NBA Linker Local Documentation

## Overview
Downloads raw tables to CSV, performs local ID matching in pandas, and uploads the matched
results back to the database. Includes fuzzy date matching for player props to handle bad
commence times from Odds API.

**Two modes are available:**
1. **Bulk mode** (`download` → `process` → `upload`): Downloads full tables to CSV for one-time linking operations
2. **Incremental mode** (`incremental`): Lightweight daily linking without downloading — queries only unlinked records

## Inputs and Dependencies
- Database tables:
  - `teams`, `players`, `team_game_stats`, `player_game_stats`
  - `raw_game_lines_staging`, `raw_player_props_combined`
- Local data directory: `linker_data/` (bulk mode only)

## Output Files (Bulk Mode)
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
- **Team name normalization:** Uses `TEAM_NAME_ALIASES` to convert all team names to 3-letter abbreviations (e.g., "Atlanta Hawks" → "ATL", "Los Angeles Lakers" → "LAL"). This enables matching between Odds API (full names) and NBA API (abbreviations).
- **Fuzzy date matching:** Searches within `FUZZY_DATE_WINDOW_DAYS` (default 90) to handle incorrect commence times.
- **Player matching:** Manual mappings → exact normalized match → SequenceMatcher fuzzy (0.80 threshold, +0.15 last name bonus).
- **Team ID backfill:** Uses `(player_id, game_id)` from `player_game_stats`.
- **Game ID format:** All game_ids are stored as 10-digit strings with leading zeros (e.g., "0022500589") using `.zfill(10)` to ensure compatibility with `player_game_stats.game_id` format.

## Usage

### Bulk Mode (One-Time Operations)
```bash
python src/processing/nba_linker_local.py download   # Download tables to CSV
python src/processing/nba_linker_local.py process    # Match IDs locally
python src/processing/nba_linker_local.py upload     # Push results back
python src/processing/nba_linker_local.py init       # Create empty player_mappings.csv
python src/processing/nba_linker_local.py all        # Run all steps
```

### Incremental Mode (Daily Automation)
```bash
# Link unlinked records without downloading tables
python src/processing/nba_linker_local.py incremental

# With options
python src/processing/nba_linker_local.py incremental --batch-size 50000  # Records per batch
python src/processing/nba_linker_local.py incremental --limit 10000       # Max records to process
```

**Incremental mode:**
- No CSV download (avoids 25M+ row table downloads)
- Queries only `WHERE player_id IS NULL`
- Loads reference tables once (teams, players, team_game_stats)
- Updates directly via batched SQL
- Used by `run_daily.py` for automated pipelines
- Test results: 99.3% player match rate, 40.7% game match rate

## Notes
- `init` creates an empty `player_mappings.csv` template.
- Upload uses temp tables and chunked updates for safety.
- Incremental mode is designed for daily automated runs (via `run_daily.py`).
- Game match rate is lower in incremental mode because props for future games haven't been played yet.

## Related Documentation
- [Documentation Index](index.md)
- [Game Lines Scraper](game_lines_scraper_documentation.md)
- [Player Prop Scraper](player_prop_scraper_documentation.md)
