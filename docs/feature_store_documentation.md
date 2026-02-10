# Feature Store Technical Documentation

## 1. System Overview
The **Feature Store** (`features/feature_store.py`) is the centralized engine for transforming raw NBA data into machine-learning-ready feature vectors. It is designed to solve the "Train-Serve Skew" problem by ensuring that the logic used to generate historical training data is mathematically identical to the logic used for live inference.

### Core Capabilities
1.  **Historical Training (Bulk Mode):** Generates datasets of 150k+ rows efficiently using **Vectorized SQL (Lateral Joins)**.
2.  **Live Inference (Online Mode):** Generates features for a single game/player in real-time with strict temporal safety.
3.  **Bayesian Shrinkage:** Automatically fills missing data (rookies, early season) with league-average priors.

---

## 2. Architecture & Design Philosophy

### 2.1 The "Time Travel" Guarantee
The most critical requirement of this module is **Temporal Integrity**.
* **Rule:** When generating features for a game on `2023-11-15`, the system effectively "travels back in time." It is strictly forbidden from accessing any data created on or after the game's tip-off.
* **Implementation:** All database queries use strict inequality comparisons (`date < game_date`) or snapshot lookups. This prevents "Look-Ahead Bias" (e.g., using a player's end-of-season average to predict their Game 1 performance).

### 2.2 Vectorized SQL (The N+1 Solution)
Naive feature stores loop through rows in Python, executing ~6 queries per row. For 5 years of data (~150k rows), this results in ~900k DB calls.
* **Our Solution:** We use Postgres **LATERAL JOINS** in `get_training_dataset`.
* **Benefit:** This pushes the iteration logic into the database engine, generating the entire training set in **one single SQL query** (seconds vs. hours).

### 2.3 Strict Schema Alignment
To prevent silent bugs where the model trains on one feature (e.g., `reb_allowed`) but inference provides another (e.g., `reb_per100_allowed`), this store enforces strict alias matching.
* **Source:** Uses `team_allowed_by_position` table for defensive metrics.
* **Normalization:** All defensive stats are strictly mapped to `_per100` columns to ensure pace-neutrality.

---

## 3. Key Feature Groups

### 3.1 Player Role (Position) Snapshots
[cite_start]Instead of using a static position (e.g., "LeBron is a Point Guard"), we use **Time-Travel Snapshots** from `player_position_history`[cite: 30].
* **Logic:** Finds the role classification (`G`, `W`, or `B`) active *strictly before* the game date.
* **Why:** Prevents using a player's future role change (e.g., shifting to Center in the playoffs) to predict regular season games.

### 3.2 Opponent Defense vs. Position
Captures how well the specific opponent defends the player's specific role.
* **Source:** `team_allowed_by_position`
* **Optimization:** Uses a single lookup for metrics like `opp_pos_reb_per100_allowed_l5`.
* **Robustness:** If an opponent has no history (rare), it falls back to the **League Prior** for that position.

### 3.3 League Priors (Bayesian Shrinkage)
[cite_start]Fetches the league-average stats (`league_priors_history`) for the player's position at that specific point in the season[cite: 1].
* **Usage:** Used as a fallback when data is missing or sample size is too small (e.g., a rookie's first game).
* **Example:** If a player has `NULL` shooting stats, the model receives `league_off_rtg` (approx 110.0) instead of a crash or a zero.

### 3.4 Betting Market Context
Fetches lines from `raw_game_lines_staging`.
* **Optimization:** Uses a conditional aggregation query to scan the table only once per game, filtering strictly for sharp books (`pinnacle`, `draftkings`).
* **Game Lines:** `line_spread` and `line_total` — game-level betting signals (spread, total). Used in `MINUTES_FEATURES`.

### 3.5 Prop Line Centering Features
Per-stat player prop lines from `raw_player_props_combined`, enabling residual modeling — the model learns deviations from market expectation rather than absolute values.

* **Features:** `prop_line_pts`, `prop_line_reb`, `prop_line_ast`, `prop_line_threes`
* **Source:** `raw_player_props_combined` table, filtered to pinnacle/draftkings bookmakers
* **Deduplication:** `DISTINCT ON (market_key)` ordered by `snapshot_time DESC` picks the most recent pre-game snapshot per stat
* **Missing data:** COALESCE to 0 (consistent with `line_spread`/`line_total` pattern). Real prop lines are always > 0, so the model can learn that 0 means "no line available."
* **Query paths:** LATERAL JOIN added to all 4 feature store methods (`get_training_dataset`, `get_features_for_date`, `get_features_for_date_range`). Single-player path uses `_get_player_prop_lines()` helper.
* **Feature assignment:** Each `RATE_FEATURES_*` list includes only its corresponding `prop_line_*` (e.g., `RATE_FEATURES_PTS` includes `prop_line_pts` but not `prop_line_reb`).
* **Database index:** `idx_props_player_game` on `(player_id, game_id)` for query performance.

### 3.6 Rest & Schedule Features (B2)
Pre-computed in `player_average_game_stats` from game date diffs. Added to `MINUTES_FEATURES`:
- `rest_days` — days since player's last game (clipped [0,7], default 3)
- `is_back_to_back` — binary flag (derived from `rest_days = 1` in SQL)
- `games_in_last_7_days` — calendar-window count of prior games (default 2)

### 3.7 Short-Window Trend Features (B3)
Pre-computed in `player_average_game_stats` with `shift(1)` no-leakage pattern. Distributed across all feature lists:
- **L3 rolling averages** (5): `player_avg_{min,pts,reb,ast,fg3m}_l3` — last 3 games, captures very recent form
- **Momentum ratios** (4): `player_{pts,reb,ast,fg3m}_l3_l15_ratio` — L3/L15 ratio (>1.0 = trending up, default 1.0)
- **L5 standard deviations** (5): `player_std_{pts,reb,ast,fg3m}_l5` + `player_min_std_l5` — consistency/variance signal

Momentum ratios are computed in SQL as `CASE WHEN l15 > 0 THEN l3/l15 ELSE 1.0 END` to avoid division by zero.

### 3.8 Minutes Stability Features (B4)
Pre-computed in `player_average_game_stats`. Added to `MINUTES_FEATURES`:
- `player_min_std_l5` — minutes variance over last 5 games (shared with B3 std)
- `player_min_floor_l5` — minimum minutes in last 5 games (floor games indicator)
- `player_games_started_l5` — games with 20+ minutes in last 5 (starter consistency proxy)

### 3.9 Injury/Lineup Context Features (B1)
Injury-driven features from `rapidapi_injuries` table, capturing how teammate and opponent absences affect a player's expected production.

**Teammate injuries (6 features):**
- `team_out_count` — number of teammates listed as "Out"
- `team_out_min_sum` — sum of season-avg minutes for missing teammates
- `team_out_pts_sum` / `team_out_reb_sum` / `team_out_ast_sum` — stats vacated by absent teammates
- `team_out_usg_sum` — usage rate vacated (opportunity proxy)

**Opponent injuries (2 features):**
- `opp_out_count` — number of opposing players listed as "Out"
- `opp_out_min_sum` — sum of season-avg minutes for missing opponents

**Player status (2 features):**
- `player_is_questionable` — binary flag if player's own status is "Questionable"
- `player_is_probable` — binary flag if player's own status is "Probable"

**Implementation:**
- SQL LATERAL JOINs in `feature_store.py` with temporal integrity (`report_date <= game_date`)
- Injury data linked to NBA player IDs via `link_injury_data.py` (3-tier cascade: manual CSV → exact match → fuzzy SequenceMatcher)
- Added to all 4 `RATE_FEATURES_*` lists and `MINUTES_FEATURES`
- COALESCE to 0 for all injury features (no injury data = no injuries reported)

---

## 4. Derived Features & Formulas

The store calculates several "Interaction Features" in Python after fetching raw stats.

| Feature | Formula | Purpose |
| :--- | :--- | :--- |
| **`expected_pace`** | `(Team_Pace * Opp_Pace) / League_Pace` | Dean Oliver's formula for estimating possessions. |
| **`line_spread_abs`** | `abs(spread)` | Proxy for "Blowout Risk" (affects minutes played). |
| **`player_usg_trend`** | `usg_l5 - usg_l15` | Detects recent changes in offensive role. |
| **`player_pts_per100`** | `(pts / min) * (48 / pace) * 100` | Normalizes scoring for minutes and game speed. **Includes safety check for low minutes.** |

---

## 5. API Usage

### 5.1 Training (Historical Data)
Use this to build your XGBoost training set.

```python
from features.feature_store import FeatureStore

fs = FeatureStore(engine)
df_train = fs.get_training_dataset(seasons=['22021', '22022', '22023'])

# Validation (Built-in)
# The method automatically asserts data volume and quality.
print(df_train.describe())
```

### 5.2 Inference Live
Use this inside your prediction pipeline.

```python
from datetime import date

# Fetch features for LeBron James (player_id=2544) vs Warriors
features = fs.get_player_game_features(
    player_id=2544,
    game_id='0022300123',
    as_of_date=date(2023, 11, 15)
)

# Returns a dictionary ready for xgb.DMatrix
# {
#   'player_avg_pts_l5': 25.4,
#   'opp_pos_def_rtg_l5': 112.3,
#   ...
# }
```

## Maintenance and Troubleshooting

### Critical Constraints
**Schema Dependencies:** This store relies on `player_position_history` and `league_priors_history` being populated. If you re-run migrations, you must re-run the backfills for these tables.

**Date Logic (Updated 2026-02-09):** The LATERAL JOINs for pre-computed rolling averages use `<= game_date` because `player_average_game_stats` uses `shift(1)` during population — meaning the row for `game_date X` already contains averages from games BEFORE X (not including X). Using `<=` gets the correct pre-computed features for each game. This is NOT data leakage because:
1. The rolling average computation in `populate_average_stats.py` applies `shift(1)` before saving
2. The row labeled `game_date X` contains averages from games [X-N, X-1], computed before game X was played
3. Using `<` instead of `<=` would fetch the PREVIOUS game's row (stale features, one game behind)

**Exception - Injury queries:** Queries that look up OTHER players' historical stats (e.g., `team_out_pts_sum` from teammates' past performances) correctly use `<` since they're fetching actual past game data, not pre-computed rolling stats.

### Common Errors
**ValueError: Suspiciously few rows:** Triggers if `get_training_dataset` returns < 10,000 rows. Check if your seasons list matches the IDs in your database (e.g., '22023' vs '2023-24').

**KeyError in Model:** Ensure that any new feature added to `get_training_dataset` (SQL) is also added to `get_player_game_features` (Dictionary).



## Related Documentation
- [Documentation Index](index.md)
- [League Priors History](league_priors_history.md)
- [Player Position History](player_position_history.md)
- [Team Allowed By Position](team_allowed_by_position.md)
- [Populate Average Stats](populate_average_stats_documentation.md)
- [Game Lines Scraper](game_lines_scraper_documentation.md)
