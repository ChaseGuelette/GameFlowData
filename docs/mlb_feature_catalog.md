# MLB Feature Catalog — Pitcher Strikeout Model

Complete reference for all 31 features in `PITCHER_K_FEATURES` (`src/models/mlb/mlb_feature_store.py`).

**Model target:** `actual_so` (pitcher strikeouts in a single game start)

---

## Feature Reference Table

### Pitcher Rolling Averages

Source: `mlb_player_average_pitching` via LATERAL JOIN (`game_date <= current`)

| # | Feature | Column | Window | Signal |
|---|---------|--------|--------|--------|
| 1 | `pitcher_avg_so_l3` | `avg_so_l3` | L3 | Recent strikeout volume |
| 2 | `pitcher_avg_so_l5` | `avg_so_l5` | L5 | Stable strikeout baseline |
| 3 | `pitcher_avg_so_szn` | `avg_so_szn` | Season | Season-level K ability |
| 4 | `pitcher_avg_k_per_9_l5` | `avg_k_per_9_l5` | L5 | K rate normalized by innings |
| 5 | `pitcher_avg_ip_l3` | `avg_ip_l3` | L3 | Recent pitch depth |
| 6 | `pitcher_avg_ip_l5` | `avg_ip_l5` | L5 | Stable pitch depth |
| 7 | `pitcher_avg_ip_szn` | `avg_ip_szn` | Season | Season workload depth |
| 8 | `pitcher_avg_pitches_thrown_l3` | `avg_pitches_thrown_l3` | L3 | Recent pitch volume |
| 9 | `pitcher_avg_bb_l5` | `avg_bb_l5` | L5 | Walk tendency (command proxy) |
| 10 | `pitcher_std_so_l3` | `std_so_l3` | L3 | K consistency/volatility |

### Pitcher Context

Source: `mlb_player_average_pitching` via LATERAL JOIN

| # | Feature | Column | Window | Signal |
|---|---------|--------|--------|--------|
| 11 | `pitcher_days_rest` | `days_rest` | Latest | Recovery; capped at 14, default 5 |
| 12 | `pitcher_pitch_count_last_start` | `pitch_count_last_start` | Latest | Fatigue from prior start |
| 13 | `pitcher_starts_szn` | `starts_szn` | Season | Season experience/workload |

### Pitcher Statcast

Source: `mlb_player_average_statcast_pitching` via LATERAL JOIN (`game_date <= current`)

| # | Feature | Column | Window | Signal |
|---|---------|--------|--------|--------|
| 14 | `pitcher_avg_whiff_pct_l5` | `avg_whiff_pct_l5` | L5 | Swing-and-miss rate |
| 15 | `pitcher_avg_csw_pct_l5` | `avg_csw_pct_l5` | L5 | Called strikes + whiffs (closest to SwStr%) |
| 16 | `pitcher_avg_chase_pct_l5` | `avg_chase_pct_l5` | L5 | How often batters chase out of zone |
| 17 | `pitcher_avg_zone_pct_l5` | `avg_zone_pct_l5` | L5 | Strike zone command |
| 18 | `pitcher_avg_fastball_velo_l5` | `avg_avg_fastball_velo_l5` | L5 | Fastball velocity (stuff indicator) |
| 19 | `pitcher_std_whiff_pct_l3` | `std_whiff_pct_l3` | L3 | Whiff consistency/volatility |

### FanGraphs Season-Level

Source: `mlb_player_season_advanced` (joined on `player_id`, `season`, `player_type = 'pitcher'`)

| # | Feature | Column | Window | Signal |
|---|---------|--------|--------|--------|
| 20 | `pitcher_fip_szn` | `fip` | Season | Fielding-independent pitching quality |
| 21 | `pitcher_k_pct_szn` | `k_pct` | Season | Season K% — core strikeout rate |

### Opposing Team Batting Context

Source: `mlb_player_game_stats_batting` + `mlb_player_game_statcast_batting` (team-level aggregation via `mlb_matchup_features.py`)

| # | Feature | Computation | Window | Signal |
|---|---------|-------------|--------|--------|
| 22 | `opp_team_avg_so_l10` | `AVG(SUM(so) per game)` | L10 | Raw team K volume (confounded by opponent quality) |
| 23 | `opp_team_avg_batting_avg_l10` | `SUM(h) / SUM(ab)` | L10 | Team contact ability |
| 24 | `opp_team_k_pct_l10` | `SUM(so) / SUM(pa)` | L10 | **Team K rate** — normalized by PA, not confounded |
| 25 | `opp_team_whiff_pct_l10` | Swing-weighted `SUM(whiff% * swings) / SUM(swings)` | L10 | **Team swing-and-miss tendency** from Statcast |

Load method: `enrich_with_matchup_features()` calls `compute_matchup_features_bulk()`. Window uses `ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING` for time-travel safety.

### Game Context

| # | Feature | Source Table | Signal |
|---|---------|-------------|--------|
| 26 | `park_so_factor` | `mlb_park_factors` (venue_id + season) | Venue K adjustment (1.0 = neutral) |
| 27 | `is_home` | `mlb_game_schedule` (derived CASE) | Home/away flag |
| 28 | `line_total` | `mlb_raw_game_lines` (totals market, pinnacle/DK) | Game total — pace/environment proxy |

### Betting Signal

| # | Feature | Source Table | Signal |
|---|---------|-------------|--------|
| 29 | `prop_line_pitcher_strikeouts` | `mlb_raw_player_props` (latest snapshot, pinnacle/DK) | Market K expectation — centering feature |

### Derived Features

Computed in Python (`_add_derived_features()`) after SQL load.

| # | Feature | Formula | Signal |
|---|---------|---------|--------|
| 30 | `pitcher_est_bf_l5` | `3 * pitcher_avg_ip_l5 + pitcher_avg_h_allowed_l5 + pitcher_avg_bb_l5` | **Estimated batters faced** — K opportunity volume |
| 31 | `pitcher_so_l3_l5_ratio` | `pitcher_avg_so_l3 / pitcher_avg_so_l5` (default 1.0) | Momentum — >1.0 means trending up |

Note: `pitcher_avg_h_allowed_l5` is loaded from SQL (`mlb_player_average_pitching.avg_h_allowed_l5`) but is NOT in PITCHER_K_FEATURES — it exists only to support the `pitcher_est_bf_l5` derivation.

---

## Source Tables Summary

| Table | Features | Description |
|-------|----------|-------------|
| `mlb_player_average_pitching` | 13 + 1 support | Pre-computed rolling averages (shift(1) for time safety) |
| `mlb_player_average_statcast_pitching` | 6 | Statcast plate discipline metrics |
| `mlb_player_season_advanced` | 2 | FanGraphs season-level advanced stats |
| `mlb_player_game_stats_batting` | 4 (via matchup) | Team-level batting aggregation |
| `mlb_player_game_statcast_batting` | 1 (via matchup) | Team-level Statcast batting aggregation |
| `mlb_park_factors` | 1 | Venue-level K adjustment |
| `mlb_game_schedule` | 1 | Game metadata (home/away) |
| `mlb_raw_game_lines` | 1 | Sportsbook game total lines |
| `mlb_raw_player_props` | 1 | Sportsbook pitcher K prop lines |

## Key Design Notes

1. **Time-Travel Safety**: All LATERAL JOINs use `game_date <= current`. Rolling averages in `mlb_player_average_pitching` and `mlb_player_average_statcast_pitching` use `shift(1)` during population, so the row for game_date X contains stats from games before X.

2. **Matchup features require explicit call**: `get_training_dataset()` does NOT include opp_team features. Must call `enrich_with_matchup_features()` separately.

3. **Null handling**: All SQL features use `COALESCE(..., 0)` or `COALESCE(..., 1.0)` for park factor. Matchup features default to 0 via `fillna(0)`. Derived features handle division-by-zero explicitly.

4. **No minutes decomposition**: Unlike NBA (minutes x rate), MLB predicts strikeouts directly.
