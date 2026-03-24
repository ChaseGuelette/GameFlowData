# Supabase Database

> Part of [[Infrastructure]]

## Overview
PostgreSQL 15+ hosted on Supabase. Two roles: `postgres` (Python backend, bypasses RLS) and `authenticated` (dashboard users, 8s statement_timeout).

## Key Tables

### NBA Core
| Table | Size | Purpose |
|-------|------|---------|
| `player_game_stats` | Large | Per-game player box scores |
| `player_average_game_stats` | Large | L5/L15/SZN rolling averages |
| `team_allowed_by_position` | Medium | Opponent defense metrics |
| `raw_player_props_combined` | **67M+ rows** | Append-only prop line snapshots |
| `daily_predictions` | Medium | Model predictions with edges |
| `daily_prediction_samples` | Large | Gzip-compressed MC sample arrays (BYTEA) |
| `paper_bets` | Medium | Automated paper trading bets |
| `user_bets` | Small | User-placed bets with cross-device sync |
| `rapidapi_injuries` | Medium | Injury data (88K+ rows, 2021-present) |

### MLB (15 tables)
`mlb_game_schedule`, `mlb_player_game_batting`, `mlb_player_game_pitching`, `mlb_raw_player_props`, `mlb_player_average_batting`, `mlb_player_average_pitching`, etc.

### NCAAB (6 tables)
Migrations 009-011 NOT applied yet.

## Performance Critical

### `raw_player_props_combined` (67M+ rows)
- NEVER run non-concurrent CREATE INDEX via migration
- Supabase `apply_migration` runs in a transaction, so CONCURRENTLY won't work
- All queries MUST include `snapshot_time` cutoffs (24-hour window)
- To add indexes: use Supabase dashboard SQL editor with longer timeout
- Consider archiving old data (primary performance improvement)

### Statement Timeout
- `authenticated` role: 8s default
- Override for slow RPCs: `ALTER FUNCTION SET statement_timeout = '30s'`
- `get_dfs_lines` and `get_game_commence_times` have 30s override

## RPCs
- `get_dfs_lines` — DFS line comparison (9-14s query time)
- `get_game_commence_times` — Game start times with LATERAL LIMIT 1
- `get_sportsbook_lines_by_games` — All markets (migration 020 removed market filter)
- `is_subscribed(uuid)` — Subscription check for RLS

## Migrations
21+ migrations applied via `database/migrations/`. Migrations 009-011 (NCAAB) pending.

## Views
- `player_stats_latest` — JOINs game stats + advanced stats
- `team_stats_latest` — Team rolling averages
- `defense_by_position_latest` — Opponent defense heatmap
- SQL definitions tracked in `sql/views/`

#supabase #database #infrastructure
