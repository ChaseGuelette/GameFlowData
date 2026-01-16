# GameFlowData Architecture

This document describes the architecture, patterns, and design decisions for the GameFlowData project - an NBA analytics and machine learning pipeline for player prop predictions.

---

## Technology Stack

### Core Languages & Runtime
- **Python 3.11+** - Primary language for all components
- **PostgreSQL** - Database (hosted on Supabase)

### Data Layer
| Package | Version | Purpose |
|---------|---------|---------|
| `sqlalchemy` | 2.0.37 | Database ORM and query building |
| `psycopg2-binary` | 2.9.10 | PostgreSQL driver |
| `pandas` | - | Data manipulation and analysis |
| `numpy` | - | Numerical operations |

### Machine Learning
| Package | Version | Purpose |
|---------|---------|---------|
| `xgboost` | - | Quantile regression models |
| `scikit-learn` | - | Isotonic regression, model utilities |
| `joblib` | - | Model serialization |

### Data Collection
| Package | Purpose |
|---------|---------|
| `nba_api` | Official NBA Stats API wrapper |
| `requests` | HTTP client for API calls |

### Development & Quality
| Package | Version | Purpose |
|---------|---------|---------|
| `pytest` | 8.3.4 | Testing framework |
| `pytest-cov` | 6.0.0 | Coverage reporting |
| `ruff` | 0.9.2 | Linting and formatting |
| `pyright` | 1.1.407 | Static type checking |

---

## Project Structure

```
GameFlowData/
├── src/
│   ├── db/
│   │   └── client.py          # Database connection singleton
│   ├── models/
│   │   ├── feature_store.py   # Feature engineering engine
│   │   ├── quantile_trainer.py # XGBoost quantile models
│   │   ├── monte_carlo.py     # Probability distribution generation
│   │   ├── daily_runner.py    # Production prediction pipeline
│   │   └── calibration.py     # Model calibration utilities
│   ├── processing/
│   │   ├── backfill_*.py      # Historical data backfill scripts
│   │   ├── player_name_mapper.py
│   │   └── populate_average_stats.py
│   └── scrapers/
│       ├── nba_unified_scraper.py  # Main data collection script
│       ├── espn_injury_scraper.py
│       ├── update_*.py        # Incremental update scripts
│       └── nba_player_position.py
├── docs/
│   └── *.md                   # Feature documentation
├── notebooks/                 # Jupyter notebooks for exploration
├── tests/                     # Test suite
└── pyproject.toml            # Project configuration
```

---

## Database Schema

### Core Game Data
| Table | Rows | Description |
|-------|------|-------------|
| `team_game_stats` | ~40K | Team box scores with advanced metrics |
| `player_game_stats` | ~430K | Individual player game statistics |
| `player_game_advanced_stats` | - | Player advanced metrics (usage, efficiency) |

### Rolling Averages (Pre-computed)
| Table | Description |
|-------|-------------|
| `player_average_game_stats` | Player L5/L15/Season averages |
| `player_average_advanced_stats` | Player advanced stat averages |
| `team_average_game_stats` | Team rolling averages |

### Contextual Data
| Table | Rows | Description |
|-------|------|-------------|
| `player_position_history` | ~17K | Time-series position snapshots |
| `team_allowed_by_position` | ~49K | Defensive stats by opponent position |
| `league_priors_history` | ~120 | League baseline stats by position |
| `league_position_averages` | 21 | Current season league averages |

### Betting Data
| Table | Rows | Description |
|-------|------|-------------|
| `raw_game_lines_staging` | ~1.2M | Game spreads and totals |
| `raw_player_props_combined` | ~12M | Player prop lines from sportsbooks |

### Reference Tables
| Table | Description |
|-------|-------------|
| `players` | Player registry with position info |
| `teams` | Team registry |
| `team_aliases` | Team name variations for matching |
| `game_id_map` | External to NBA game ID mapping |

---

## Architectural Patterns

### 1. Database Connection Singleton

The database connection is managed as a module-level singleton in `src/db/client.py`:

```python
# Singleton pattern - engine created once at import time
engine = create_engine(DATABASE_URL)

def get_engine():
    return engine
```

All modules import `get_engine()` to share the same connection pool.

### 2. Feature Store Pattern

The `FeatureStore` class (`src/models/feature_store.py`) centralizes all feature engineering:

**Key Capabilities:**
- **Training Mode**: Generates 150K+ row datasets via vectorized SQL
- **Inference Mode**: Real-time single-player feature generation
- **Train-Serve Consistency**: Identical logic for both modes prevents skew

**Critical Design Principle - Temporal Integrity:**
```
All queries use strict inequality: date < game_date
Never: date <= game_date (would leak future data)
```

### 3. Vectorized SQL with LATERAL JOINS

Training data generation uses PostgreSQL LATERAL JOINs to push iteration into the database:

```sql
-- Example: Get most recent position for each player-game
LEFT JOIN LATERAL (
    SELECT position_group
    FROM player_position_history ph
    WHERE ph.player_id = pgs.player_id
      AND ph.snapshot_date < pgs.game_date
    ORDER BY ph.snapshot_date DESC LIMIT 1
) pos ON TRUE
```

**Benefit**: Single query replaces ~900K individual queries (hours → seconds).

### 4. Bayesian Shrinkage (League Priors)

Missing or sparse data is filled with league-average priors:

```python
# If player has no history, use league averages for their position
if result is None:
    return {
        'league_off_rtg': 110.0,
        'league_reb_per100': 15.0,
        'league_threes_per100': 3.0
    }
```

This prevents model crashes for rookies or early-season predictions.

### 5. Rolling Window Conventions

All averages follow a consistent naming pattern:
- `*_l5` - Last 5 games
- `*_l15` - Last 15 games
- `*_szn` - Season-to-date

---

## ML Pipeline Architecture

### Prediction Flow

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Feature Store  │────▶│ Quantile Models  │────▶│ Monte Carlo     │
│                 │     │ (Minutes + Rate) │     │ Simulator       │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        │                        │                        │
        ▼                        ▼                        ▼
   Player context          Q10, Q25, Q50,          Full probability
   Team context            Q75, Q90 for           distribution for
   Opponent defense        minutes & rate          final stat
```

### Quantile Regression Models

Each stat prediction uses **two XGBoost quantile models**:
1. **Minutes Model** - Predicts playing time distribution
2. **Rate Model** - Predicts stat-per-minute distribution

**Output**: 5 quantiles (10th, 25th, 50th, 75th, 90th percentile)

### Monte Carlo Simulation

Final predictions combine minutes and rate distributions:

```python
# Sample from quantile distributions
minutes_samples = sample_from_quantiles(minutes_model.predict(X))
rate_samples = sample_from_quantiles(rate_model.predict(X))

# Combine: stat = minutes × rate
stat_samples = minutes_samples * rate_samples

# Extract final quantiles from combined distribution
```

**Output**: Full probability distribution with over/under probabilities.

---

## Data Pipeline

### Collection Flow

```
NBA API ──▶ nba_unified_scraper.py ──▶ team_game_stats
                                   ──▶ player_game_stats
                                   ──▶ player_game_advanced_stats

Sportsbook APIs ──▶ game_lines_staging
                ──▶ player_props_combined
```

### Processing Flow

```
Raw Stats ──▶ populate_average_stats.py ──▶ *_average_*_stats tables
          ──▶ backfill_league_priors.py ──▶ league_priors_history
          ──▶ backfill_opponent_allowed.py ──▶ team_allowed_by_position
          ──▶ update_player_position_history.py ──▶ player_position_history
```

### Rate Limiting

API calls implement respectful rate limiting:
```python
SHORT_DELAY_MIN = 0.6   # seconds between calls
SHORT_DELAY_MAX = 1.5
LONG_PAUSE_EVERY = 100  # games before long pause
LONG_PAUSE_MIN = 30     # seconds for long pause
BAN_COOLDOWN = 600      # 10 min if rate limited
```

---

## Key Feature Groups

### Player Features
| Feature | Source | Description |
|---------|--------|-------------|
| `player_avg_min_l5/l15` | `player_average_game_stats` | Recent minutes |
| `player_avg_pts_l5/l15` | `player_average_game_stats` | Recent scoring |
| `player_avg_usg_pct_l5` | `player_average_advanced_stats` | Usage rate |
| `player_avg_ts_pct_l15` | `player_average_advanced_stats` | True shooting |

### Team Context
| Feature | Source | Description |
|---------|--------|-------------|
| `team_avg_pace_l5` | `team_average_game_stats` | Team pace |
| `team_avg_off_rtg_l5` | `team_average_game_stats` | Offensive rating |

### Opponent Context
| Feature | Source | Description |
|---------|--------|-------------|
| `opp_avg_def_rtg_l5` | `team_average_game_stats` | Opponent defense |
| `opp_pos_off_rtg_allowed_l5` | `team_allowed_by_position` | Position-specific defense |
| `opp_pos_reb_per100_allowed_l5` | `team_allowed_by_position` | Rebounds allowed |

### Betting Context
| Feature | Source | Description |
|---------|--------|-------------|
| `line_spread` | `raw_game_lines_staging` | Game spread (blowout proxy) |
| `line_total` | `raw_game_lines_staging` | Over/under (pace proxy) |

### Derived Features
| Feature | Formula | Purpose |
|---------|---------|---------|
| `expected_pace` | `(team_pace × opp_pace) / league_pace` | Dean Oliver's formula |
| `line_spread_abs` | `abs(spread)` | Blowout risk indicator |
| `player_usg_trend` | `usg_l5 - usg_l15` | Role change detection |
| `player_pts_per100_l5` | `(pts/min) × (48/pace) × 100` | Pace-normalized scoring |

---

## Conventions

### Database Naming
- All column names are `snake_case`
- Foreign keys follow pattern: `fk_{source_table}_{column}`
- Rolling averages: `avg_{stat}_{window}` (e.g., `avg_pts_l5`)

### Code Style
- Line length: 100 characters (Ruff)
- Quote style: Double quotes
- Import order: Standard → Third-party → Local (isort)

### Testing
- Target coverage: 60% (Tier 1 Essential)
- Test files: `test_*.py` or `*_test.py`
- Async mode: Auto (pytest-asyncio)

---

## Data Coverage

| Season | Status |
|--------|--------|
| 2022-23 | Historical |
| 2023-24 | Historical |
| 2024-25 | Active (current season) |

---

## Critical Invariants

1. **Never use `<= game_date`** in feature queries - always use `< game_date` to prevent data leakage

2. **Position snapshots are time-travel safe** - always use the most recent snapshot *before* the game date

3. **League priors are required** - `player_position_history` and `league_priors_history` must be populated before training

4. **Quantiles must be monotonic** - isotonic regression enforces `q10 <= q25 <= q50 <= q75 <= q90`

5. **Rate calculations require minimum minutes** - only calculate rates for players with 10+ minutes to avoid noise
