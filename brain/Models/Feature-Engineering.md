# Feature Engineering

> Part of [[Models]]

## NBA Feature Store Architecture

The feature store (`src/models/feature_store.py`) is the central engine for converting raw stats into model-ready features. It serves all 4 query paths with identical SQL patterns to prevent train/serve skew.

### Query Paths (must stay in sync)
1. `get_training_dataset()` — Full training data for season(s)
2. `get_features_for_date()` — All players for a given date
3. `get_features_for_date_range()` — Time-series dataset across date range
4. `get_player_game_features()` — Single player-game feature vector

### Feature Groups (66 total)
| Group | Count | Key Features |
|-------|-------|-------------|
| Minutes | ~15 | min rolling avgs, rest_days, B2B, games_last_7d, spread, total, starter_prob |
| PTS Rate | ~15 | pts rolling avgs, opp defense, prop_line_pts, L3 trends, std devs |
| REB Rate | ~15 | reb rolling avgs, opp defense, prop_line_reb, L3 trends, std devs |
| AST Rate | ~15 | ast rolling avgs, opp defense, prop_line_ast, L3 trends, std devs |
| Shared | ~6 | Injury context (B1): team/opp out counts, minutes, stats sums |

### Feature Bundles
- **B1 (Injury)**: 10 features from `rapidapi_injuries` — teammate and opponent injuries
- **B2 (Schedule)**: rest_days, is_back_to_back, games_last_7d
- **B3 (Trends)**: L3 rolling averages, L3/L5 momentum ratios, L5 std devs
- **B4 (Minutes Stability)**: min_std_l5, min_floor_l5, games_started_l5, starter_prob

### Design Principles
- **Time-travel safe**: Strict `game_date < target_date` inequalities prevent data leakage
- **League-average defaults**: Missing values default to league averages (not 0) — e.g., pace=99.5, def_rtg=112.0
- **Per-100 possessions**: Opponent defense normalized for pace differences
- **Prop line centering**: Model learns deviations from market expectation, not absolute values
- **LATERAL JOINs**: PostgreSQL lateral joins for efficient rolling window computation

### MLB Feature Store
31 features for pitcher K model across 6 data sources. See `src/models/mlb/mlb_feature_store.py`.

#features #model #architecture
