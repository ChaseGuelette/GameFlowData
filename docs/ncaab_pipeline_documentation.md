# NCAAB Game-Level Prediction Pipeline

## Overview

NCAA Men's Basketball prediction pipeline for game-level spread, moneyline, and total predictions. Unlike NBA/MLB (player props), NCAAB is game-level only — college player props are not available due to regulatory restrictions.

## Architecture

```
Data Sources             Scrapers                  Processing              Models
-----------             --------                  ----------              ------
CBBpy (ESPN)  ------>  ncaab_cbbpy_scraper   -->  ncaab_populate_averages
                                                                          \
Barttorvik    ------>  ncaab_barttorvik_scraper -> ncaab_barttorvik_linker --> ncaab_feature_store --> ncaab_trainer
                                                                          /
Odds API      ------>  ncaab_game_lines_scraper -> ncaab_linker          --> ncaab_backtest
```

## Data Sources

### CBBpy (ESPN Box Scores)
- **Package:** `cbbpy` (`pip install cbbpy`)
- **Data:** Player-level box scores, game schedules, team rosters
- **Aggregation:** Player stats aggregated to team level at ingest time
- **Key Stats:** Points, FG/FGA, 3P/3PA, FT/FTA, OREB, DREB, AST, STL, BLK, TOV
- **Computed:** Possessions (`FGA - OREB + TOV + 0.44 * FTA`), Four Factors (eFG%, TOV%, ORB%, FT Rate)

### Barttorvik (Adjusted Efficiency)
- **Source:** `barttorvik.com/{season}_team_results.csv`
- **Auth:** None required (free, public data)
- **Update Frequency:** Every 15 minutes in-season
- **Key Metrics:** AdjOE, AdjDE, AdjEM, AdjTempo, Barthag, Four Factors (offense + defense)
- **Point-in-Time:** Stored with `snapshot_date` for temporal integrity. Feature store uses `snapshot_date < game_date` via LATERAL JOIN.

### The Odds API (Game Lines)
- **Sport Key:** `basketball_ncaab`
- **Markets:** h2h (moneyline), spreads, totals
- **Snapshot Hours:** 18, 21, 0 UTC (1 PM, 4 PM, 7 PM ET)
- **Note:** No player props available for college sports (regulatory)

## Database Tables

### Migration 009 — Core Foundation
| Table | Purpose |
|-------|---------|
| `ncaab_teams` | 363 D1 programs. PK: `team_id` (SERIAL). UNIQUE: `espn_team_id`. |
| `ncaab_game_schedule` | One row per game. PK: `game_id` (BIGINT, ESPN game ID). Includes `neutral_site`, `season_type` (regular/conference_tournament/tournament). |
| `ncaab_team_box_scores` | Team-level aggregated stats. UNIQUE: `(game_id, team_id)`. Raw stats + computed possessions + Four Factors + opponent Four Factors. |
| `ncaab_raw_game_lines` | Odds API ingest table. Same schema as `mlb_raw_game_lines` with linked columns: `game_id`, `home_team_id`, `away_team_id`. |

### Migration 010 — Barttorvik Ratings
| Table | Purpose |
|-------|---------|
| `ncaab_barttorvik_ratings` | Point-in-time efficiency snapshots. UNIQUE: `(team_name, season, snapshot_date)`. Stores AdjOE/AdjDE/AdjEM/AdjTempo/Barthag, Four Factors (offense + defense), ranks. |

### Migration 011 — Rolling Averages
| Table | Purpose |
|-------|---------|
| `ncaab_team_rolling_averages` | Pre-game team features. UNIQUE: `(team_id, game_id)`. L5/L10/L20/SZN windows for all stats. Includes `rest_days`, `games_last_7d`. |

## Scrapers

### `ncaab_game_lines_scraper.py`
Direct port of `mlb_daily_game_lines_scraper.py`.
- `NCAABGameLineScraper(api_key, engine)` class
- `get_live_odds()` — Current game lines
- `get_historical_odds(date)` — Historical game lines
- `parse_and_store(response, snapshot_time)` — Parse API response, batch insert via `execute_values`
- Table: `ncaab_raw_game_lines`

### `ncaab_cbbpy_scraper.py`
- `NCAABCBBpyScraper(engine)` class
- `scrape_schedule(season)` — Upserts teams and schedule from ESPN
- `_scrape_single_boxscore(game_id)` — Aggregates player stats to team level
- `backfill_boxscores(season)` — Fills in missing box scores
- `_aggregate_team_stats()` — Flexible column mapping (CBBpy headers vary)
- Four Factors computed: `efg_pct = (fgm + 0.5 * fg3m) / fga`, `tov_pct = tov / (fga + 0.44 * fta + tov)`, etc.

### `ncaab_barttorvik_scraper.py`
- `BarttorviKScraper(engine)` class
- `fetch_csv(season)` — HTTP GET with retry (3 attempts, exponential backoff)
- `normalize_columns(df)` — Maps CSV headers to DB schema via `COLUMN_MAP` dict
- `store_snapshot(df, season, snapshot_date)` — Batch insert with ON CONFLICT DO NOTHING
- `backfill_seasons(start, end)` — One snapshot per historical season (April 15th)

## Processing

### `ncaab_config.py`
```python
ROLLING_WINDOWS = {"l5": 5, "l10": 10, "l20": 20, "szn": None}
TEAM_BOX_STATS = ["pts", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
                  "oreb", "dreb", "ast", "stl", "blk", "tov", "poss_est",
                  "efg_pct", "ft_rate"]
TEAM_OPP_STATS = ["opp_efg_pct", "opp_tov_pct", "opp_orb_pct", "opp_ft_rate"]
```

### `ncaab_linker.py`
Game-level only (simpler than NBA/MLB player linkers):
- `normalize_team(name, aliases)` — Case-insensitive alias lookup
- `build_schedule_lookup(engine)` — Dict keyed by `(home_normalized, away_normalized)`
- `fuzzy_match_team(name, lookup)` — SequenceMatcher with threshold 0.72
- `match_batch(df, schedule_lookup, team_lookup, aliases)` — Returns DataFrame with matched game_id
- `apply_updates(engine, updates_df)` — Temp table UPDATE pattern

### `ncaab_populate_averages.py`
- Groups by `(team_id, season)`, sorts by `game_date`
- `shift(1)` on all stats, then rolling mean at each window
- Computes: `game_number`, `games_l5/l10/l20/szn`, `std_pts_l5`, `std_poss_l5`, `rest_days`, `games_last_7d`
- Full backfill: TRUNCATE + reload
- Incremental: DELETE stale teams + re-insert

### `ncaab_barttorvik_linker.py`
3-step matching:
1. Manual `BARTTORVIK_TO_ESPN` mapping (handles known mismatches like UConn→Connecticut)
2. Direct name match against `ncaab_teams.team_name`
3. Fuzzy match (SequenceMatcher >= 0.72) for remaining unmatched

## Feature Store

### Feature Categories (~30 features)

| Category | Features | Source |
|----------|----------|--------|
| Efficiency differentials | `diff_adj_em`, `diff_adj_oe`, `diff_adj_de`, `diff_barthag` | Barttorvik (LATERAL JOIN) |
| Pace | `avg_tempo_combined`, `diff_avg_poss_l5` | Barttorvik + rolling averages |
| Rolling box score diffs | `diff_avg_pts_l5/l10`, `diff_avg_efg_pct_l5`, `diff_avg_tov_pct_l5`, `diff_avg_orb_pct_l5`, `diff_avg_ft_rate_l5` | Rolling averages |
| Defensive diffs | `diff_avg_opp_efg_pct_l5`, `diff_avg_opp_tov_pct_l5` | Opponent Four Factors |
| Raw values (totals model) | `home/away_avg_pts_l5`, `home/away_bart_adj_oe/de` | Both teams raw |
| Context | `rest_differential`, `is_neutral_site`, `is_tournament` | Schedule |
| Market | `line_spread`, `line_total` | Game lines |

### Temporal Integrity
- Rolling averages use `shift(1)` — the row for game_date X contains averages from games BEFORE X
- Barttorvik ratings use LATERAL JOIN: `WHERE snapshot_date < game_date ORDER BY snapshot_date DESC LIMIT 1`
- No future data leakage possible

## Models

### `ncaab_trainer.py`
Two XGBoost quantile models:
1. **Spread model:** target = `home_margin` (home_score - away_score), features = `SPREAD_FEATURES`
2. **Total model:** target = `total_score` (home + away), features = `TOTAL_FEATURES`

**XGBoost Config (game-level tuning):**
- `max_depth=4` (shallower than player models — fewer rows, less overfitting risk)
- `n_estimators=800`, `learning_rate=0.04`, `min_child_weight=5`
- Quantiles: (0.10, 0.25, 0.50, 0.75, 0.90)
- ~5,000 training rows per season

**Moneyline derived from spread:** Fit normal distribution to Q25/Q50/Q75 percentiles, compute `P(home_margin > 0)`.

### `ncaab_backtest.py`
- `NCAABBacktestHarness(engine, model_dir)` class
- Iterates dates chronologically, generates predictions using only pre-game data
- Tracks: ATS record, O/U record, spread MAE, total MAE, edge distribution
- Outputs results CSV for analysis

## Orchestration

### `ncaab_daily_stats_job.py`
**Schedule:** 14:05 UTC / 9:05 AM ET (November through April only)

Steps:
1. CBBpy scrape (new final games)
2. Rolling averages incremental update
3. Barttorvik snapshot download
4. Barttorvik team linker

### `ncaab_lines_job.py`
**Schedule:** 17:30 UTC (12:30 PM ET) and 21:30 UTC (4:30 PM ET), November through April only

Steps:
1. Game lines scrape (live)
2. Game lines linker (incremental)

### Scheduler Integration
3 new cron jobs in `scheduler.py` with `month="11-12,1-4"` guard:
```python
scheduler.add_job(run_ncaab_daily_stats, "cron", hour=14, minute=5, month="11-12,1-4")
scheduler.add_job(run_ncaab_lines, "cron", hour=17, minute=30, month="11-12,1-4")
scheduler.add_job(run_ncaab_lines, "cron", hour=21, minute=30, month="11-12,1-4")
```

## Testing

4 test modules, 34 tests:
- `test_ncaab_game_lines_scraper.py` — 5 tests (live/historical/empty responses, sport key, batch insert)
- `test_ncaab_barttorvik_scraper.py` — 6 tests (normalize, adj_em, four factors, dedup, fetch CSV)
- `test_ncaab_linker.py` — 9 tests (normalize, closest game, fuzzy match, batch matching)
- `test_ncaab_feature_store.py` — 10 tests (differentials, feature lists, no duplicates)

## Known Limitations / TODOs

1. **Team aliases incomplete:** `ODDS_API_TEAM_ALIASES` and partial `BARTTORVIK_TO_ESPN` need expansion after first scrape
2. **CBBpy import path:** `cbbpy.mens_scraper` needs runtime verification
3. **ncaab_teams UNIQUE constraint:** Migration 009 has UNIQUE on `espn_team_id` but CBBpy scraper uses `ON CONFLICT (team_name)` — needs fix
4. **Historical Barttorvik limitation:** Historical seasons get one end-of-season snapshot (slight optimism vs true point-in-time)
5. **Mid-major coverage gaps:** Early-season games against small programs may lack Odds API lines
6. **`cbbpy` not in requirements.txt yet** — needs to be added before deployment
