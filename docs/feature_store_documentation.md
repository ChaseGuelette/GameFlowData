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
[cite_start]Fetches lines from `raw_game_lines_staging`[cite: 33].
* **Optimization:** Uses a conditional aggregation query to scan the table only once per game, filtering strictly for sharp books (`pinnacle`, `draftkings`).

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
Critical Constraints
Schema Dependencies: This store relies on player_position_history and league_priors_history being populated. If you re-run migrations, you must re-run the backfills for these tables.

Date Logic: Never change < game_date to <= game_date. This will introduce leakage (using the game's own stats to predict itself).

Common Errors
ValueError: Suspiciously few rows: Triggers if get_training_dataset returns < 10,000 rows. Check if your seasons list matches the IDs in your database (e.g., '22023' vs '2023-24').

KeyError in Model: Ensure that any new feature added to get_training_dataset (SQL) is also added to get_player_game_features (Dictionary).


