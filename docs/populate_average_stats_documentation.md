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
- B2/B3/B4 features (see below)

## Key Logic
- `calculate_games_in_window` counts prior games in each window.
- `rolling_with_groupby` computes group-safe rolling means (supports `agg` parameter: `"mean"`, `"std"`, `"min"`, `"sum"`).
- `calculate_b2_b3_b4_features(df)` computes 14 new columns using shift(1) no-leakage pattern.
- `_count_games_in_window(df, group_cols, days)` counts calendar-window prior games.
- Insert functions truncate and bulk insert in batches (`BATCH_SIZE`).

## B2/B3/B4 Features (14 columns)

### B2: Rest & Schedule
- `rest_days` — days since player's last game, clipped [0,7], default 3 for first game
- `games_last_7d` — calendar-window count of games in prior 7 days

### B3: Short-Window Trends
- `avg_{min,pts,reb,ast,fg3m}_l3` — L3 rolling average (5 columns)
- `std_{min,pts,reb,ast,fg3m}_l5` — L5 rolling standard deviation (5 columns)

### B4: Minutes Stability
- `min_floor_l5` — minimum minutes in last 5 games
- `games_started_l5` — count of games with 20+ minutes in last 5 (starter proxy, threshold: `STARTER_MINUTES_THRESHOLD = 20`)

All computed with `shift(1)` to ensure no data leakage from the current game.

## Usage
```bash
# Full recalculation (historical backfills)
python src/processing/populate_average_stats.py
python src/processing/populate_average_stats.py --season 2024-25
python src/processing/populate_average_stats.py --table player
python src/processing/populate_average_stats.py --table player_advanced
python src/processing/populate_average_stats.py --table team
python src/processing/populate_average_stats.py --table player --from-year 2021

# Incremental update (daily cron jobs) — see populate_average_stats_incremental.py
python src/processing/populate_average_stats_incremental.py                    # Today's games
python src/processing/populate_average_stats_incremental.py --date 2026-02-09  # Specific date
```

## Incremental Version

For daily cron jobs, use `populate_average_stats_incremental.py` instead of the full script:

**Key optimizations:**
- Only processes players who played on the target date (vs all ~4000 players)
- Fetches last 20 games per player (vs full history)
- Uses batch UPSERT (`conn.execute(text(sql), records)`) instead of TRUNCATE + reload (Session 43: converted from row-by-row `iterrows()`)
- Queries actual season game count per player for correct `games_szn` (Session 43: fixed off-by-one where `games_szn` was capped at 19 due to `LOOKBACK_GAMES = 20`)
- **Performance: ~1 second vs ~28 minutes (1700x speedup)**

The incremental version computes the same rolling averages (L5, L15, season-to-date) and B2/B3/B4 features, but only for players who had games on the target date. Results are upserted into `player_average_game_stats` using `ON CONFLICT (player_id, game_id) DO UPDATE`.

## Notes
- The first game of a season will have null averages (no prior history).
- Advanced stat column names are mapped via `PLAYER_ADVANCED_MAPPING`.
- `game_date` from the DB arrives as Python `date` objects — converted to `datetime64` via `pd.to_datetime()` before date arithmetic.

## Related Documentation
- [Documentation Index](index.md)
- [Feature Store](feature_store_documentation.md)
