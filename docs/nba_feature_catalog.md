# NBA Feature Catalog — Player Props Models

Complete reference for all features used across the NBA prediction models. Features are defined in `src/models/feature_store.py`.

**Model architecture:** Minutes model predicts playing time; Rate models predict per-minute stat rates. Final prediction = Minutes x Rate (via Monte Carlo).

---

## Model → Feature List Mapping

| Model | Target | Feature List | Unique Features |
|-------|--------|-------------|-----------------|
| Minutes | `actual_minutes` | `MINUTES_FEATURES` | 25 |
| Points Rate | `pts_per_min` | `RATE_FEATURES_PTS` | 24 |
| Rebounds Rate | `reb_per_min` | `RATE_FEATURES_REB` | 23 |
| Assists Rate | `ast_per_min` | `RATE_FEATURES_AST` | 23 |
| Threes Rate | `fg3m_per_min` | `RATE_FEATURES_THREES` | 26 |

**Total entries across all lists: 121** (many shared — injury/rest features appear in every list)
**Total unique features: 66**

---

## Feature Reference

### Player Performance — Rolling Averages

Source: `player_average_game_stats` via LATERAL JOIN (`game_date <= current`, shift(1) safe)

| Feature | Window | Models | Signal |
|---------|--------|--------|--------|
| `player_avg_min_l3` | L3 | MIN | Very recent minutes trend |
| `player_avg_min_l5` | L5 | MIN | Stable minutes baseline |
| `player_avg_min_l15` | L15 | MIN | Longer-term minutes baseline |
| `player_avg_pts_l3` | L3 | PTS | Recent scoring form |
| `player_avg_pts_l5` | L5 | PTS | Stable scoring baseline |
| `player_avg_pts_l15` | L15 | PTS | Longer-term scoring level |
| `player_avg_reb_l3` | L3 | REB | Recent rebounding form |
| `player_avg_reb_l5` | L5 | REB | Stable rebounding baseline |
| `player_avg_ast_l3` | L3 | AST | Recent assist form |
| `player_avg_ast_l5` | L5 | AST | Stable assist baseline |
| `player_avg_fg3m_l3` | L3 | 3PT | Recent three-point form |
| `player_avg_fg3m_l5` | L5 | 3PT | Stable three-point baseline |
| `player_avg_fg3a_l5` | L5 | 3PT | Three-point attempt volume |

### Player Performance — Variability

Source: `player_average_game_stats`

| Feature | Window | Models | Signal |
|---------|--------|--------|--------|
| `player_min_std_l5` | L5 | MIN | Minutes consistency |
| `player_std_pts_l5` | L5 | PTS | Scoring consistency |
| `player_std_reb_l5` | L5 | REB | Rebounding consistency |
| `player_std_ast_l5` | L5 | AST | Assist consistency |
| `player_std_fg3m_l5` | L5 | 3PT | Three-point consistency |

### Player Performance — Momentum Ratios

Source: Derived in SQL from `player_average_game_stats`

| Feature | Formula | Models | Signal |
|---------|---------|--------|--------|
| `player_pts_l3_l15_ratio` | `avg_pts_l3 / avg_pts_l15` | PTS | Scoring momentum (uses L15 denom) |
| `player_reb_l3_l15_ratio` | `avg_reb_l3 / avg_reb_l5` | REB | Rebound momentum (ISS-017: actually L3/L5) |
| `player_ast_l3_l15_ratio` | `avg_ast_l3 / avg_ast_l5` | AST | Assist momentum (ISS-017: actually L3/L5) |
| `player_fg3m_l3_l15_ratio` | `avg_fg3m_l3 / avg_fg3m_l5` | 3PT | Three-point momentum (ISS-017: actually L3/L5) |

> **ISS-017 note:** REB/AST/3PT momentum ratios are named `_l3_l15_ratio` but actually compute L3/L5. Only PTS uses the true L3/L15 ratio. Names kept for model artifact compatibility.

### Player Advanced Stats

Source: `player_average_advanced_stats` via LATERAL JOIN

| Feature | Window | Models | Signal |
|---------|--------|--------|--------|
| `player_avg_usg_pct_l5` | L5 | MIN, PTS, AST | Usage rate — offensive involvement |
| `player_avg_ts_pct_l15` | L15 | PTS | True shooting efficiency |
| `player_avg_reb_pct_l5` | L5 | REB | Rebound rate (% of available rebounds) |
| `player_avg_ast_pct_l5` | L5 | AST | Assist rate (% of team FG assisted) |

### Minutes Stability

Source: `player_average_game_stats`

| Feature | Window | Models | Signal |
|---------|--------|--------|--------|
| `player_min_floor_l5` | L5 | MIN | Minimum minutes in last 5 (floor games) |
| `player_games_started_l5` | L5 | MIN | Games with 20+ min in last 5 (starter consistency) |

### Team Context

Source: `team_average_game_stats` via LATERAL JOIN (own team + opponent team)

| Feature | Window | Models | Signal |
|---------|--------|--------|--------|
| `team_avg_pace_l5` | L5 | MIN, PTS, REB, AST, 3PT | Team pace (possessions per 48) |
| `opp_avg_pace_l5` | L5 | MIN, REB | Opponent pace |
| `opp_avg_def_rtg_l5` | L5 | PTS | Opponent defensive rating |
| `team_avg_fg3a_l5` | L5 | 3PT | Team three-point attempt volume |
| `team_avg_fg3_pct_l5` | L5 | 3PT | Team three-point shooting percentage |
| `opp_avg_fg3a_l5` | L5 | 3PT | Opponent three-point attempt volume |
| `opp_avg_fg3_pct_l5` | L5 | 3PT | Opponent three-point shooting percentage |

### Opponent Defense vs Position

Source: `team_allowed_by_position` (joined via opponent_id + position_group)

| Feature | Window | Models | Signal |
|---------|--------|--------|--------|
| `opp_pos_off_rtg_allowed_l5` | L5 | PTS | Off rating allowed vs player's position |
| `opp_pos_off_rtg_allowed_l15` | L15 | PTS | Off rating allowed vs position (stable) |
| `opp_pos_reb_allowed_l5` | L5 | REB | Rebounds allowed vs position (raw) |
| `opp_pos_reb_per100_allowed_l5` | L5 | REB | Rebounds per 100 allowed vs position |
| `opp_pos_reb_allowed_l15` | L15 | REB | Rebounds allowed vs position (stable) |
| `opp_pos_ast_allowed_l5` | L5 | AST | Assists allowed vs position (raw) |
| `opp_pos_ast_per100_allowed_l5` | L5 | AST | Assists per 100 allowed vs position |
| `opp_pos_ast_allowed_l15` | L15 | AST | Assists allowed vs position (stable) |
| `opp_pos_threes_allowed_l5` | L5 | 3PT | Threes allowed vs position (raw) |
| `opp_pos_threes_per100_allowed_l5` | L5 | 3PT | Threes per 100 allowed vs position |
| `opp_pos_threes_allowed_l15` | L15 | 3PT | Threes allowed vs position (stable) |

### Rest & Schedule

Source: `player_average_game_stats`

| Feature | Window | Models | Signal |
|---------|--------|--------|--------|
| `rest_days` | Current | ALL | Days since last game (capped 0-7, default 3) |
| `is_back_to_back` | Current | ALL | Back-to-back flag (rest_days = 1) |
| `games_in_last_7_days` | 7-day window | ALL | Schedule density |

### Game Context

| Feature | Source | Models | Signal |
|---------|--------|--------|--------|
| `is_home` | `player_game_stats.matchup` (LIKE '%vs.%') | ALL | Home court advantage |
| `line_spread` | `raw_game_lines_staging` (pinnacle/DK) | MIN | Spread — blowout risk proxy |
| `line_total` | `raw_game_lines_staging` (pinnacle/DK) | MIN | Game total — pace environment |

### Betting Signal — Prop Lines

Source: `raw_player_props_combined` (latest snapshot, pinnacle/DK, `DISTINCT ON market_key`)

| Feature | Market Key | Models | Signal |
|---------|-----------|--------|--------|
| `prop_line_pts` | `player_points` | PTS | Market expectation — centering feature |
| `prop_line_reb` | `player_rebounds` | REB | Market expectation — centering feature |
| `prop_line_ast` | `player_assists` | AST | Market expectation — centering feature |
| `prop_line_threes` | `player_threes` | 3PT | Market expectation — centering feature |

### Injury Context — Team

Source: `rapidapi_injuries` (status = 'Out') cross-referenced with `player_average_game_stats` for missing players' L5 stats

| Feature | Models | Signal |
|---------|--------|--------|
| `team_out_count` | ALL | Number of teammates listed Out |
| `team_out_min_sum` | ALL | Sum of Out teammates' avg minutes (L5) |
| `team_out_pts_sum` | ALL | Sum of Out teammates' avg points (L5) |
| `team_out_reb_sum` | ALL | Sum of Out teammates' avg rebounds (L5) |
| `team_out_ast_sum` | ALL | Sum of Out teammates' avg assists (L5) |
| `team_out_usg_sum` | ALL | Sum of Out teammates' avg usage% (L5) |
| `opp_out_count` | ALL | Number of opponent players listed Out |
| `opp_out_min_sum` | ALL | Sum of Out opponents' avg minutes (L5) |

### Injury Context — Player Status

Source: `rapidapi_injuries`

| Feature | Models | Signal |
|---------|--------|--------|
| `player_is_questionable` | MIN only | Player listed as Questionable |
| `player_is_probable` | MIN only | Player listed as Probable |

> Note: Despite existing docs claiming these are in all RATE_FEATURES lists, they are only in MINUTES_FEATURES.

---

## Source Tables Summary

| Table | Features | Description |
|-------|----------|-------------|
| `player_average_game_stats` | ~20 | Pre-computed rolling player averages (shift(1) safe) |
| `player_average_advanced_stats` | 4 | Advanced rate stats (USG%, TS%, REB%, AST%) |
| `team_average_game_stats` | 7 | Team-level rolling stats (pace, def rating, FG3) |
| `team_allowed_by_position` | 11 | Positional defensive matchup stats |
| `raw_game_lines_staging` | 2 | Sportsbook game lines (spread, total) |
| `raw_player_props_combined` | 4 | Sportsbook player prop lines |
| `rapidapi_injuries` | 10 | Injury reports (Out/Questionable/Probable) |
| `player_game_stats` | 1 | Game metadata (home/away derivation) |
| `player_position_history` | 0 (SQL logic) | Position group for defense-vs-position joins |

## Key Design Notes

1. **Time-Travel Safety**: All LATERAL JOINs use `<= game_date`. Rolling averages tables use `shift(1)` during population — the row for game_date X contains stats from games before X.

2. **Bayesian Fallback**: Missing opponent defense or position data falls back to league priors from `league_priors_history`.

3. **Injury linkage**: `rapidapi_injuries` linked to NBA player IDs via `link_injury_data.py` (3-tier cascade: manual CSV, exact match, fuzzy match).

4. **Prop line semantics**: 0 means "no line available" (real lines are always > 0), so the model can learn the absence.

5. **Deprecated features**: `travel_dist`, `opp_rest_days`, `opp_travel_dist`, `opp_is_back_to_back` are hardcoded to 0 in queries for backward compatibility but carry no signal.
