# MLB Player Prop Prediction Expansion Plan

## Context

GameFlowData currently predicts NBA player props (PTS/REB/AST) end-to-end: scrapers, feature store, XGBoost quantile regression, Monte Carlo sampling, paper trading, dashboard, Discord, and social media. The MLB season starts **March 25, 2026** (~1 month away). This plan adds MLB player prop prediction capability by replicating the NBA architecture with MLB-specific adaptations.

**Key challenge:** MLB batting stats are fundamentally different from NBA stats — very discrete, low-count distributions (hits: 0-4, HR: 0-1) that don't suit quantile regression. Pitcher strikeouts (mean ~5-6) are the closest NBA analog and the safest launch target.

---

## Data Sources

### MLB Stats API (Primary — Game-by-Game Stats)
- **URL:** `statsapi.mlb.com` — Free, no auth required
- **Python wrapper:** `MLB-StatsAPI` by toddrob99 (`pip install MLB-StatsAPI`)
- **Key endpoints:** `schedule(start_date, end_date)` for game IDs, `boxscore_data(gamePk)` for per-player per-game batting + pitching stats
- **Rate limits:** None published — generous, 1 req/sec is conservative
- **Coverage:** Full historical boxscores going back decades

### pybaseball (Supplementary — Advanced/Statcast Features)
- **Install:** `pip install pybaseball`
- **Provides:** Statcast data (exit velocity, launch angle, barrel%), FanGraphs advanced stats (wOBA, FIP, wRC+, xBA, xERA)
- **Statcast data:** Available from 2015 onward (pitch-level)
- **Use for:** Season-level advanced features, not game-by-game scraping

### The Odds API (Props + Game Lines)
- **Same API as NBA** — sport key changes from `basketball_nba` to `baseball_mlb`
- **Historical props:** Available from May 2023
- **Game lines:** h2h, spreads, totals (same as NBA)

### MLB Player Prop Markets Available (The Odds API)

**Batter props:**
| Market Key | Typical Lines | Volume |
|-----------|---------------|--------|
| `batter_home_runs` | O/U 0.5 (binary) | Highest engagement |
| `batter_hits` | O/U 0.5, 1.5 | Highest volume |
| `batter_total_bases` | O/U 0.5, 1.5, 2.5 | High volume |
| `batter_rbis` | O/U 0.5 | Medium |
| `batter_runs_scored` | O/U 0.5 | Medium |
| `batter_hits_runs_rbis` | O/U 1.5, 2.5, 3.5 | Medium |
| `batter_stolen_bases` | O/U 0.5 | Low-Medium |
| `batter_strikeouts` | O/U 0.5, 1.5 | Medium |
| `batter_walks` | O/U 0.5 | Low |
| `batter_singles` | O/U 0.5 | Low |
| `batter_doubles` | O/U 0.5 | Low |

**Pitcher props:**
| Market Key | Typical Lines | Volume |
|-----------|---------------|--------|
| `pitcher_strikeouts` | O/U 4.5, 5.5, 6.5, 7.5 | **Highest — most balanced/liquid** |
| `pitcher_earned_runs` | O/U 1.5, 2.5, 3.5 | Medium |
| `pitcher_hits_allowed` | O/U 4.5, 5.5, 6.5 | Medium |
| `pitcher_outs` | O/U 15.5, 16.5, 17.5 | Medium |
| `pitcher_walks` | O/U 1.5, 2.5 | Low |

---

## MLB vs NBA — Critical Modeling Differences

### Stat Distribution Comparison
| | NBA PTS | NBA REB | NBA AST | MLB Hits | MLB HR | MLB Pitcher K |
|--|---------|---------|---------|----------|--------|---------------|
| Mean | ~20 | ~6 | ~4 | ~1.0 | ~0.1 | ~5-6 |
| Range | 0-60+ | 0-20+ | 0-15+ | 0-4 | 0-3 | 2-14 |
| Shape | Bell-curve | Slightly skewed | Right-skewed | **Very discrete** | **Nearly binary** | Semi-continuous |
| P(zero) | <5% | <5% | ~10% | **~30%** | **~90%** | <5% |
| Quantile regression? | Yes | Yes | Yes | **No** | **No** | **Yes** |

### Features Unique to MLB (No NBA Equivalent)
- **Platoon splits** — Batter performance swings 15-30% based on pitcher handedness (LHP vs RHP)
- **Park factors** — Coors Field inflates all offense by 10-20%, Oracle Park suppresses HR. NBA is indoors, controlled
- **Weather** — Wind direction/speed, temperature affect ball flight. No NBA equivalent
- **Lineup position** — Batting 1st vs 8th affects plate appearances and RBI opportunities
- **Opposing starter quality** — The specific starting pitcher directly affects all batter props
- **Days rest / pitch count** — Pitchers start every 5 days, fatigue is critical
- **Umpire tendencies** — Strike zone size affects K and BB rates

### Market Structure Differences
- MLB prop juice is **higher** than NBA, especially on low-line batter props (0.5 lines carry 15-25 cents of vig)
- Many MLB batting props are **effectively binary** (HR yes/no, SB yes/no)
- Pitcher strikeouts have the **tightest juice** in MLB — closest to NBA-style O/U
- Props shift when starting pitchers are confirmed (~2-3 hours before first pitch)
- MLB season: 162 games/team (2x NBA), ~15 games/day, March-September

---

## Phase 1: Foundation — Database + Scrapers + Historical Backfill (Week 1)

### 1.1 Database Schema Migration

**Create ~14 new MLB tables (all prefixed `mlb_`):**

#### `mlb_teams`
```sql
CREATE TABLE mlb_teams (
    team_id INTEGER PRIMARY KEY,  -- MLB Stats API team ID
    team_name TEXT NOT NULL,
    team_abbreviation TEXT NOT NULL,
    league TEXT,  -- "AL" or "NL"
    division TEXT  -- "East", "Central", "West"
);
```

#### `mlb_players`
```sql
CREATE TABLE mlb_players (
    player_id INTEGER PRIMARY KEY,  -- MLB Stats API player ID
    player_name TEXT NOT NULL,
    primary_position TEXT,  -- "P", "C", "1B", "2B", "SS", "3B", "LF", "CF", "RF", "DH"
    bats TEXT,  -- "L", "R", "S" (switch)
    throws TEXT,  -- "L", "R"
    active BOOLEAN DEFAULT TRUE
);
```

#### `mlb_game_schedule`
```sql
CREATE TABLE mlb_game_schedule (
    game_id INTEGER PRIMARY KEY,  -- MLB Stats API gamePk
    game_date DATE NOT NULL,
    season INTEGER NOT NULL,
    game_type TEXT NOT NULL DEFAULT 'R',  -- "R" regular, "P" postseason, "S" spring
    home_team_id INTEGER REFERENCES mlb_teams(team_id),
    away_team_id INTEGER REFERENCES mlb_teams(team_id),
    venue_id INTEGER,
    venue_name TEXT,
    home_score INTEGER,
    away_score INTEGER,
    status TEXT,  -- "Final", "Scheduled", "In Progress", "Postponed"
    game_time_utc TIMESTAMPTZ
);
CREATE INDEX idx_mlb_schedule_date ON mlb_game_schedule(game_date);
CREATE INDEX idx_mlb_schedule_season ON mlb_game_schedule(season);
```

#### `mlb_player_game_stats_batting`
```sql
CREATE TABLE mlb_player_game_stats_batting (
    player_id INTEGER NOT NULL REFERENCES mlb_players(player_id),
    game_id INTEGER NOT NULL REFERENCES mlb_game_schedule(game_id),
    game_date DATE NOT NULL,
    season INTEGER NOT NULL,
    team_id INTEGER NOT NULL REFERENCES mlb_teams(team_id),
    lineup_position INTEGER,  -- 1-9, NULL if pinch hitter only
    is_starter BOOLEAN NOT NULL DEFAULT FALSE,
    pa INTEGER DEFAULT 0,  -- plate appearances
    ab INTEGER DEFAULT 0,
    r INTEGER DEFAULT 0,
    h INTEGER DEFAULT 0,
    doubles INTEGER DEFAULT 0,
    triples INTEGER DEFAULT 0,
    hr INTEGER DEFAULT 0,
    rbi INTEGER DEFAULT 0,
    bb INTEGER DEFAULT 0,
    so INTEGER DEFAULT 0,
    sb INTEGER DEFAULT 0,
    cs INTEGER DEFAULT 0,
    hbp INTEGER DEFAULT 0,
    sf INTEGER DEFAULT 0,
    sac INTEGER DEFAULT 0,
    tb INTEGER DEFAULT 0,
    avg NUMERIC,
    obp NUMERIC,
    slg NUMERIC,
    ops NUMERIC,
    did_not_play BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, game_id)
);
CREATE INDEX idx_mlb_batting_date ON mlb_player_game_stats_batting(game_date);
CREATE INDEX idx_mlb_batting_season ON mlb_player_game_stats_batting(season);
CREATE INDEX idx_mlb_batting_player_date ON mlb_player_game_stats_batting(player_id, game_date);
```

#### `mlb_player_game_stats_pitching`
```sql
CREATE TABLE mlb_player_game_stats_pitching (
    player_id INTEGER NOT NULL REFERENCES mlb_players(player_id),
    game_id INTEGER NOT NULL REFERENCES mlb_game_schedule(game_id),
    game_date DATE NOT NULL,
    season INTEGER NOT NULL,
    team_id INTEGER NOT NULL REFERENCES mlb_teams(team_id),
    is_starter BOOLEAN NOT NULL DEFAULT FALSE,
    is_winner BOOLEAN,
    is_loser BOOLEAN,
    is_save BOOLEAN,
    ip NUMERIC,  -- Innings pitched decimal (6.333 for 6.1 IP)
    outs_recorded INTEGER,  -- ip * 3
    h_allowed INTEGER DEFAULT 0,
    r_allowed INTEGER DEFAULT 0,
    er INTEGER DEFAULT 0,
    bb INTEGER DEFAULT 0,
    so INTEGER DEFAULT 0,
    hr_allowed INTEGER DEFAULT 0,
    pitches_thrown INTEGER,
    strikes INTEGER,
    era NUMERIC,
    whip NUMERIC,
    did_not_play BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, game_id)
);
CREATE INDEX idx_mlb_pitching_date ON mlb_player_game_stats_pitching(game_date);
CREATE INDEX idx_mlb_pitching_player_date ON mlb_player_game_stats_pitching(player_id, game_date);
CREATE INDEX idx_mlb_pitching_starter ON mlb_player_game_stats_pitching(is_starter, game_date);
```

#### `mlb_player_average_batting`
```sql
CREATE TABLE mlb_player_average_batting (
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    game_date DATE NOT NULL,
    team_id INTEGER NOT NULL,
    game_number INTEGER,  -- nth game this season for this player
    -- Rolling windows: L5, L10, L20, SZN
    -- For each of: pa, ab, h, hr, rbi, r, bb, so, sb, tb, doubles, triples
    avg_h_l5 NUMERIC, avg_h_l10 NUMERIC, avg_h_l20 NUMERIC, avg_h_szn NUMERIC,
    avg_hr_l5 NUMERIC, avg_hr_l10 NUMERIC, avg_hr_l20 NUMERIC, avg_hr_szn NUMERIC,
    avg_rbi_l5 NUMERIC, avg_rbi_l10 NUMERIC, avg_rbi_l20 NUMERIC, avg_rbi_szn NUMERIC,
    avg_r_l5 NUMERIC, avg_r_l10 NUMERIC, avg_r_l20 NUMERIC, avg_r_szn NUMERIC,
    avg_bb_l5 NUMERIC, avg_bb_l10 NUMERIC, avg_bb_l20 NUMERIC, avg_bb_szn NUMERIC,
    avg_so_l5 NUMERIC, avg_so_l10 NUMERIC, avg_so_l20 NUMERIC, avg_so_szn NUMERIC,
    avg_sb_l5 NUMERIC, avg_sb_l10 NUMERIC, avg_sb_l20 NUMERIC, avg_sb_szn NUMERIC,
    avg_tb_l5 NUMERIC, avg_tb_l10 NUMERIC, avg_tb_l20 NUMERIC, avg_tb_szn NUMERIC,
    avg_pa_l5 NUMERIC, avg_pa_l10 NUMERIC, avg_pa_l20 NUMERIC, avg_pa_szn NUMERIC,
    avg_ab_l5 NUMERIC, avg_ab_l10 NUMERIC, avg_ab_l20 NUMERIC, avg_ab_szn NUMERIC,
    avg_doubles_l5 NUMERIC, avg_doubles_l10 NUMERIC, avg_doubles_l20 NUMERIC, avg_doubles_szn NUMERIC,
    avg_triples_l5 NUMERIC, avg_triples_l10 NUMERIC, avg_triples_l20 NUMERIC, avg_triples_szn NUMERIC,
    -- Standard deviations (for consistency features)
    std_h_l5 NUMERIC, std_hr_l5 NUMERIC, std_tb_l5 NUMERIC, std_rbi_l5 NUMERIC,
    std_r_l5 NUMERIC, std_so_l5 NUMERIC, std_sb_l5 NUMERIC,
    -- Rate stats
    avg_batting_avg_l10 NUMERIC, avg_obp_l10 NUMERIC, avg_slg_l10 NUMERIC, avg_ops_l10 NUMERIC,
    -- Schedule context
    rest_days SMALLINT,
    games_last_7d SMALLINT,
    PRIMARY KEY (player_id, game_id)
);
```

#### `mlb_player_average_pitching`
```sql
CREATE TABLE mlb_player_average_pitching (
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    game_date DATE NOT NULL,
    team_id INTEGER NOT NULL,
    -- Rolling windows: L3, L5, SZN (pitchers start every ~5 days)
    avg_so_l3 NUMERIC, avg_so_l5 NUMERIC, avg_so_szn NUMERIC,
    avg_ip_l3 NUMERIC, avg_ip_l5 NUMERIC, avg_ip_szn NUMERIC,
    avg_er_l3 NUMERIC, avg_er_l5 NUMERIC, avg_er_szn NUMERIC,
    avg_h_allowed_l3 NUMERIC, avg_h_allowed_l5 NUMERIC, avg_h_allowed_szn NUMERIC,
    avg_bb_l3 NUMERIC, avg_bb_l5 NUMERIC, avg_bb_szn NUMERIC,
    avg_hr_allowed_l3 NUMERIC, avg_hr_allowed_l5 NUMERIC, avg_hr_allowed_szn NUMERIC,
    avg_pitches_thrown_l3 NUMERIC, avg_pitches_thrown_l5 NUMERIC, avg_pitches_thrown_szn NUMERIC,
    avg_outs_recorded_l3 NUMERIC, avg_outs_recorded_l5 NUMERIC, avg_outs_recorded_szn NUMERIC,
    -- Derived rate stats
    avg_era_l5 NUMERIC, avg_whip_l5 NUMERIC,
    avg_k_per_9_l5 NUMERIC, avg_bb_per_9_l5 NUMERIC,
    -- Standard deviations
    std_so_l3 NUMERIC, std_er_l3 NUMERIC,
    -- Rest context
    days_rest SMALLINT,
    pitch_count_last_start INTEGER,
    starts_l3 SMALLINT, starts_l5 SMALLINT, starts_szn SMALLINT,
    PRIMARY KEY (player_id, game_id)
);
```

#### `mlb_team_average_stats`
```sql
CREATE TABLE mlb_team_average_stats (
    team_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    game_date DATE NOT NULL,
    -- Offensive rolling averages
    avg_runs_l5 NUMERIC, avg_runs_l10 NUMERIC, avg_runs_szn NUMERIC,
    avg_hits_l5 NUMERIC, avg_hits_l10 NUMERIC, avg_hits_szn NUMERIC,
    avg_hr_l5 NUMERIC, avg_hr_l10 NUMERIC, avg_hr_szn NUMERIC,
    avg_so_l5 NUMERIC, avg_so_l10 NUMERIC, avg_so_szn NUMERIC,
    -- Pitching rolling averages
    avg_era_l5 NUMERIC, avg_whip_l5 NUMERIC,
    avg_runs_allowed_l5 NUMERIC, avg_runs_allowed_l10 NUMERIC,
    PRIMARY KEY (team_id, game_id)
);
```

#### `mlb_park_factors`
```sql
CREATE TABLE mlb_park_factors (
    venue_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    venue_name TEXT,
    runs_factor NUMERIC DEFAULT 1.0,  -- >1.0 = hitter-friendly
    hr_factor NUMERIC DEFAULT 1.0,
    hits_factor NUMERIC DEFAULT 1.0,
    so_factor NUMERIC DEFAULT 1.0,
    PRIMARY KEY (venue_id, season)
);
```

#### `mlb_raw_player_props`
```sql
CREATE TABLE mlb_raw_player_props (
    staging_id BIGSERIAL PRIMARY KEY,
    api_game_id TEXT,
    api_player_name TEXT,
    bookmaker TEXT,
    market_key TEXT,
    outcome_label TEXT,
    line NUMERIC,
    odds_american INTEGER,
    commence_time TIMESTAMPTZ,
    home_team TEXT,
    away_team TEXT,
    snapshot_time TIMESTAMPTZ,
    market_last_update TIMESTAMPTZ,
    bookmaker_last_update TIMESTAMPTZ,
    bookmaker_name TEXT,
    -- Linked fields (filled by mlb_linker)
    game_id INTEGER,
    player_id INTEGER,
    team_id INTEGER,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_mlb_props_game_id ON mlb_raw_player_props(game_id);
CREATE INDEX idx_mlb_props_player_id ON mlb_raw_player_props(player_id);
CREATE INDEX idx_mlb_props_snapshot ON mlb_raw_player_props(snapshot_time);
CREATE INDEX idx_mlb_props_unlinked ON mlb_raw_player_props(game_id) WHERE game_id IS NULL;
```

#### `mlb_raw_game_lines`
```sql
CREATE TABLE mlb_raw_game_lines (
    staging_id BIGSERIAL PRIMARY KEY,
    api_game_id TEXT,
    bookmaker TEXT,
    market_key TEXT,
    outcome_label TEXT,
    line NUMERIC,
    odds_american INTEGER,
    commence_time TIMESTAMPTZ,
    home_team TEXT,
    away_team TEXT,
    snapshot_time TIMESTAMPTZ,
    mlb_game_id INTEGER,
    mlb_home_team_id INTEGER,
    mlb_away_team_id INTEGER,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### `mlb_daily_predictions` + `mlb_daily_prediction_samples` + `mlb_paper_bets` + `mlb_paper_trading_daily_log`
Same schema structure as NBA equivalents, using MLB game/player ID types (integer instead of text for game_id).

**Design decision:** Separate tables rather than adding a `sport` column. Avoids migration risk on existing 27M-row NBA props table, keeps NBA queries fast, allows MLB-specific columns.

### 1.2 MLB Stats API Scraper

**Create:** `src/scrapers/mlb/mlb_stats_scraper.py`
**Pattern:** `src/scrapers/nba_unified_scraper.py`

```python
import statsapi  # pip install MLB-StatsAPI

class MLBStatsScraper:
    def __init__(self, engine):
        self.engine = engine

    def scrape_schedule(self, start_date: str, end_date: str) -> int:
        """Scrape game schedule into mlb_game_schedule.
        Uses statsapi.schedule(start_date, end_date)
        Returns number of games inserted."""

    def scrape_boxscores(self, game_ids: list[int]) -> tuple[int, int]:
        """Scrape box scores for given games.
        Uses statsapi.boxscore_data(gamePk)
        Inserts into mlb_player_game_stats_batting AND mlb_player_game_stats_pitching.
        Also auto-inserts new players into mlb_players.
        Returns (processed, failed)."""

    def scrape_season(self, season: int) -> None:
        """Full season backfill: schedule + all box scores."""
```

**Parsing notes:**
- IP "6.1" = 6 innings + 1 out → convert to 6.333 decimal: `int(ip) + (ip % 1) * 10 / 3`
- Filter `game_type = 'R'` (exclude spring training)
- Rate limiting: 1 req/sec (conservative, MLB API is generous)
- Dedup pattern: check existing game_ids before scraping

### 1.3 Historical Backfill Script

**Create:** `src/scrapers/mlb/mlb_backfill.py`
**Scope:** 2022-2025 seasons, ~9,720 games total, ~291K player-game rows
**Runtime:** ~2-4 hours per season at 1 req/sec — run overnight

### 1.4 Odds API MLB Props Scraper

**Create:**
- `src/scrapers/mlb/mlb_player_props_scraper.py` (historical backfill)
- `src/scrapers/mlb/mlb_daily_player_props_scraper.py` (daily live)
- `src/scrapers/mlb/mlb_daily_game_lines_scraper.py` (game lines)

**Pattern:** `src/scrapers/daily_player_props_scraper.py`
**Only change:** Sport key `basketball_nba` → `baseball_mlb`, market keys updated

**Phase 1 priority markets:**
```python
MLB_CORE_MARKETS = [
    "pitcher_strikeouts",     # Most liquid, most predictable
    "batter_hits",            # Most liquid batter market
    "batter_total_bases",     # Good volume, decent range
    "batter_home_runs",       # Popular but binary/plus-money
    "pitcher_outs",           # Semi-continuous
    "batter_rbis",            # Decent volume
    "batter_runs_scored",     # Decent volume
]
```

### 1.5 MLB Linker

**Create:** `src/processing/mlb/mlb_linker.py`
**Pattern:** `src/processing/nba_linker_local.py` (incremental linking mode)

**Simpler than NBA linker:**
- MLB game IDs are stable integers → game matching uses team names + exact date (no ±90 day fuzzy window needed)
- Player name matching: reuse same `normalize_player()` fuzzy approach (SequenceMatcher ratio > 0.80)

**Needs:** `MLB_TEAM_NAME_ALIASES` dict for all 30 teams + variations:
```python
MLB_TEAM_NAME_ALIASES = {
    "New York Yankees": "NYY", "NY Yankees": "NYY",
    "New York Mets": "NYM", "NY Mets": "NYM",
    "Los Angeles Dodgers": "LAD", "LA Dodgers": "LAD",
    "Los Angeles Angels": "LAA", "LA Angels": "LAA",
    "Chicago Cubs": "CHC", "Chi Cubs": "CHC",
    "Chicago White Sox": "CWS", "Chi White Sox": "CWS",
    # ... all 30 teams
}
```

### 1.6 Reference Data

**Create:** `src/scrapers/mlb/mlb_reference.py`
- Seed `mlb_teams` (30 teams from MLB Stats API)
- Seed `mlb_park_factors` (30 venues × 4 seasons from FanGraphs data)

---

## Phase 2: Processing + Features (Week 2)

### 2.1 Rolling Average Computation

**Create:**
- `src/processing/mlb/mlb_config.py` — Window sizes, stat lists
- `src/processing/mlb/mlb_populate_averages.py` — Full backfill
- `src/processing/mlb/mlb_populate_averages_incremental.py` — Daily incremental

**Pattern:** `src/processing/populate_average_stats_incremental.py`

**MLB-specific windows:**
```python
# Batters: more games = wider windows make sense
BATTER_WINDOWS = {"l5": 5, "l10": 10, "l20": 20, "szn": None}

# Pitchers: start every 5 days, so L3 ≈ 15 days of starts
PITCHER_WINDOWS = {"l3": 3, "l5": 5, "szn": None}

BATTING_STATS = ["pa", "ab", "h", "hr", "rbi", "r", "bb", "so", "sb", "tb", "doubles", "triples"]
PITCHING_STATS = ["ip", "so", "er", "h_allowed", "bb", "hr_allowed", "pitches_thrown", "outs_recorded"]
```

Same `shift(1)` time-travel safety as NBA — row for game X contains averages from games BEFORE game X.

### 2.2 Pitcher vs Batter Matchup Features

**Create:** `src/processing/mlb/mlb_matchup_features.py`
**No NBA equivalent — new and critical for MLB.**

For each game, identify the opposing starting pitcher and compute:
- `opp_starter_k_per_9_l5` — Opposing pitcher's recent strikeout rate
- `opp_starter_whip_l5` — Opposing pitcher's recent WHIP
- `opp_starter_era_l5` — Opposing pitcher's recent ERA
- `opp_starter_handedness` — "L" or "R" (binary feature for platoon effects)

### 2.3 Platoon Split Computation

**Integrated into:** `src/processing/mlb/mlb_populate_averages.py`

Split batter rolling averages by opposing pitcher handedness:
- `avg_h_vs_lhp_l10`, `avg_h_vs_rhp_l10`
- `avg_so_vs_lhp_l10`, `avg_so_vs_rhp_l10`
- `avg_hr_vs_lhp_l20`, `avg_hr_vs_rhp_l20`

Requires joining each game to the opposing starter's `throws` field from `mlb_players`.

### 2.4 MLB Feature Store

**Create:** `src/models/mlb/mlb_feature_store.py`
**Pattern:** `src/models/feature_store.py` (SQL lateral join approach)
**This is the largest single file.**

**Feature sets per prop type:**

#### Pitcher Strikeouts Features (~15 features)
```python
FEATURES_PITCHER_SO = [
    "pitcher_avg_so_l3", "pitcher_avg_so_l5", "pitcher_avg_so_szn",
    "pitcher_avg_k_per_9_l5",
    "pitcher_avg_ip_l3", "pitcher_avg_ip_l5",
    "pitcher_days_rest",
    "pitcher_pitch_count_last_start",
    "pitcher_std_so_l3",
    "opp_team_avg_so_l10",       # Opposing team's strikeout tendency as batters
    "opp_team_avg_batting_avg_l10",
    "park_so_factor",
    "prop_line_pitcher_strikeouts",
    "is_home",
    "pitcher_handedness",         # L=0, R=1
]
```

#### Batter Hits Features (~18 features)
```python
FEATURES_BATTER_HITS = [
    "batter_avg_h_l5", "batter_avg_h_l10", "batter_avg_h_l20", "batter_avg_h_szn",
    "batter_avg_pa_l5",
    "batter_avg_batting_avg_l10",
    "batter_h_vs_hand_l20",       # Platoon split
    "batter_std_h_l5",
    "batter_h_l5_l20_ratio",      # Trend (momentum indicator)
    "opp_starter_whip_l5",
    "opp_starter_k_per_9_l5",
    "opp_starter_handedness",
    "park_hits_factor",
    "lineup_position",
    "prop_line_batter_hits",
    "is_home",
    "rest_days",
    "games_last_7d",
]
```

#### Batter Total Bases Features (~16 features)
```python
FEATURES_BATTER_TB = [
    "batter_avg_tb_l5", "batter_avg_tb_l10", "batter_avg_tb_l20",
    "batter_avg_slg_l10",
    "batter_avg_hr_l10",
    "batter_tb_vs_hand_l20",
    "batter_std_tb_l5",
    "opp_starter_era_l5",
    "opp_starter_hr_allowed_per_9_l5",
    "opp_starter_handedness",
    "park_hr_factor",
    "park_runs_factor",
    "lineup_position",
    "prop_line_batter_total_bases",
    "is_home",
    "rest_days",
]
```

#### Batter Home Run Features (~12 features)
```python
FEATURES_BATTER_HR = [
    "batter_avg_hr_l10", "batter_avg_hr_l20", "batter_avg_hr_szn",
    "batter_avg_slg_l10",
    "batter_hr_vs_hand_l20",
    "opp_starter_hr_allowed_per_9_l5",
    "opp_starter_handedness",
    "park_hr_factor",
    "lineup_position",
    "prop_line_batter_home_runs",
    "is_home",
    "rest_days",
]
```

Same interface as NBA: `get_player_game_features()` for inference, `get_training_dataset()` for training.

---

## Phase 3: Model Training + Backtesting (Week 2-3)

### 3.1 Model Architecture — The Critical Decision

**MLB stat distributions require different model types than NBA:**

| Prop | Distribution | Model | Why | Existing Code Reference |
|------|-------------|-------|-----|------------------------|
| **Pitcher K** | Semi-continuous (mean ~5-6, range 2-14) | XGBoost Quantile Regression | Sufficient range for meaningful quantiles | `src/models/quantile_trainer.py` — reuse directly |
| **Pitcher Outs** | Semi-continuous (mean ~17, range 3-27) | XGBoost Quantile Regression | Good range | Same |
| **Batter Hits** | Discrete count (mean ~1, range 0-4) | Negative Binomial | Quantiles collapse with only 5 values | `src/models/truncated_negbin.py` — adapt |
| **Batter TB** | Discrete count (mean ~1.5, range 0-8) | Negative Binomial | Count data, overdispersed | Same |
| **Batter HR** | Nearly binary (90% zeros) | XGBoost Binary Classifier | Just predict P(HR>=1) | New, simple `binary:logistic` |
| **Batter RBI** | Zero-heavy count (mean ~0.7) | Negative Binomial | High zero rate | Same NegBin |
| **Batter Runs** | Zero-heavy count (mean ~0.6) | Negative Binomial | High zero rate | Same NegBin |

**Why not quantile regression for batting?** With only 5 possible values (0-4 hits), quantiles collapse: Q10=Q25=0, Q50=1, Q75=1, Q90=2. No resolution between quantiles. NegBin parameterized by (mu, alpha) produces proper count distributions.

**Files to create:**
- `src/models/mlb/mlb_quantile_trainer.py` — Thin wrapper for pitcher K/outs
- `src/models/mlb/mlb_negbin_trainer.py` — Standard (not truncated) NegBin for batter counting stats. Adapted from `src/models/truncated_negbin.py` which already implements mu/alpha parameterization with XGBoost
- `src/models/mlb/mlb_binary_trainer.py` — XGBoost classifier for HR/SB

**Binary classifier (new, simple):**
```python
class BinaryPropModel:
    """XGBoost classifier for P(stat >= 1)."""
    def __init__(self):
        self.model = xgb.XGBClassifier(
            objective="binary:logistic",
            n_estimators=500,
            max_depth=4,
            learning_rate=0.03,
            eval_metric="logloss",
        )

    def train(self, X, y):
        # y = (actual >= 1).astype(int)
        self.model.fit(X, y, eval_set=[(X_val, y_val)], early_stopping_rounds=50)

    def predict_proba(self, X) -> np.ndarray:
        return self.model.predict_proba(X)[:, 1]
```

### 3.2 Monte Carlo Adaptation

**Create:** `src/models/mlb/mlb_monte_carlo.py`
**Pattern:** `src/models/monte_carlo.py`

**Key difference from NBA:** No minutes-rate decomposition. MLB samples directly:
- **Pitcher K:** Sample from quantile distribution (interpolate between quantile predictions, like NBA PTS but without minutes step)
- **Batter Hits/TB/RBI:** Sample from NegBin(mu, alpha) → integer counts
- **Batter HR:** Sample from Bernoulli(p) where p = model P(HR>=1)

```python
def sample_negbin(mu, alpha, n_samples=10000):
    """Sample from NegBin(mu, alpha) distribution."""
    n = 1.0 / alpha
    p = n / (n + mu)
    return np.random.negative_binomial(n, p, size=n_samples)

def sample_binary(prob_ge_1, n_samples=10000):
    """For binary props (HR, SB). P(over 0.5) = P(>=1)."""
    return np.random.binomial(1, prob_ge_1, size=n_samples)
```

The `PropPrediction` dataclass (prob_over, prob_under, expected_value) from `monte_carlo.py` is sport-agnostic and reusable as-is.

### 3.3 Training Pipeline

**Create:** `src/models/mlb/mlb_train_pipeline.py`
**Pattern:** `src/models/train_pipeline.py`

```python
class MLBTrainingOrchestrator:
    def run(self, train_seasons=[2022, 2023, 2024], cal_season=2025):
        # 1. Load features from MLB feature store
        # 2. Train pitcher K quantile model (Q10/Q25/Q50/Q75/Q90)
        # 3. Train pitcher outs quantile model
        # 4. Train batter hits NegBin model (mu, alpha)
        # 5. Train batter TB NegBin model
        # 6. Train batter HR binary classifier
        # 7. (Optional) Train batter RBI, runs NegBin models
        # 8. Calibrate on 2025 holdout
        # 9. Save artifacts to src/models/mlb/artifacts/run_{timestamp}/
```

**Artifacts directory:** `src/models/mlb/artifacts/`

### 3.4 Backtesting

**Create:** `src/backtesting/mlb/mlb_backtest_harness.py`
**Pattern:** `src/backtesting/backtest_harness.py`

Walk through 2025 season date-by-date, generate blind predictions, compare to actuals. Same Kelly criterion bet sizing.

**Key calibration targets:**
- Pitcher K quantile model: all quantile gaps within 3%
- Batter hits NegBin: predicted P(over 0.5) and P(over 1.5) match actual rates within 3%
- Batter HR binary: AUC > 0.65, calibration curve roughly diagonal
- Overall backtest ROI target: >3% on pitcher K props

---

## Phase 4: Production Pipeline (Week 3-4)

### 4.1 Orchestration Jobs

**Create:**
- `src/orchestration/mlb_daily_stats_job.py` — 9 AM ET: scrape yesterday's MLB results, update rolling averages, resolve paper bets
- `src/orchestration/mlb_lines_job.py` — Multiple times: scrape live props + game lines, run linker
- `src/orchestration/mlb_inference_job.py` — After lines: load model, generate predictions, store, place paper bets
- `src/orchestration/mlb_edge_refresh_job.py` — After props-only: recalculate edges from stored MC samples + fresh lines

**MLB schedule (different from NBA):**
- MLB games: 1:05 PM, 4:05 PM, 7:05 PM ET (spread throughout day)
- NBA games: mostly 7-10:30 PM ET

**Proposed MLB job schedule:**
| Time (ET) | Job | Purpose |
|-----------|-----|---------|
| 9:00 AM | `mlb_daily_stats_job` | Yesterday's results, rolling averages, bet resolution |
| 10:00 AM | `mlb_lines_job --live` | Full scrape (game lines + props + injuries) |
| 10:15 AM | `mlb_inference_job` | Full inference for day's games |
| 11:30 AM | `mlb_lines_job --live --props-only` + edge refresh | Props refresh |
| 12:30 PM | Props-only + edge refresh | Before early (1 PM) games |
| 3:00 PM | `mlb_lines_job --live` | Full scrape |
| 3:15 PM | `mlb_inference_job` | Re-inference with updated lines |
| 4:30 PM | Props-only + edge refresh | Before afternoon games |
| 5:30 PM | Props-only + edge refresh | Before evening games |
| 6:30 PM | Props-only + edge refresh | Final refresh |

### 4.2 Scheduler Update

**Modify:** `src/orchestration/scheduler.py`
Add MLB cron jobs alongside existing NBA ones. No conflicts — MLB and NBA seasons overlap (April-June is both NBA playoffs and MLB regular season).

### 4.3 Paper Trading

**Create:** `src/paper_trading/mlb_paper_trader.py`
Adapt existing `PaperTrader` pattern for MLB tables and stat types. Resolution reads from `mlb_player_game_stats_batting`/`pitching`.

**MLB-specific considerations:**
- Higher juice → need larger edges (8-10% threshold vs NBA's 5%)
- Binary props (HR) have different Kelly sizing than O/U props
- DNP detection: check `did_not_play` flag, void bets for scratched players

---

## Phase 5: Frontend + Integrations (Post-Launch)

### 5.1 Dashboard — Sport Toggle
- Add NBA/MLB toggle to predictions page header
- PropCard component works generically if fed correct data shape
- API routes need MLB query paths (`/api/mlb-slate/route.ts`)

### 5.2 Discord — MLB Commands
- `/mlb-picks` — Today's top MLB predictions
- `/mlb-player <name>` — Player-specific predictions
- `/mlb-bankroll` — MLB paper trading balance

### 5.3 Social Media — MLB Cards
- Adapt card templates with baseball-themed styling
- Same generator pattern, different data queries

---

## Complete New File List

```
# Scrapers
src/scrapers/mlb/__init__.py
src/scrapers/mlb/mlb_stats_scraper.py
src/scrapers/mlb/mlb_backfill.py
src/scrapers/mlb/mlb_player_props_scraper.py
src/scrapers/mlb/mlb_daily_player_props_scraper.py
src/scrapers/mlb/mlb_daily_game_lines_scraper.py
src/scrapers/mlb/mlb_reference.py

# Processing
src/processing/mlb/__init__.py
src/processing/mlb/mlb_config.py
src/processing/mlb/mlb_linker.py
src/processing/mlb/mlb_populate_averages.py
src/processing/mlb/mlb_populate_averages_incremental.py
src/processing/mlb/mlb_matchup_features.py

# Models
src/models/mlb/__init__.py
src/models/mlb/mlb_feature_store.py
src/models/mlb/mlb_quantile_trainer.py
src/models/mlb/mlb_negbin_trainer.py
src/models/mlb/mlb_binary_trainer.py
src/models/mlb/mlb_monte_carlo.py
src/models/mlb/mlb_train_pipeline.py
src/models/mlb/mlb_prediction_store.py
src/models/mlb/artifacts/                    (directory)

# Orchestration
src/orchestration/mlb_daily_stats_job.py
src/orchestration/mlb_lines_job.py
src/orchestration/mlb_inference_job.py
src/orchestration/mlb_edge_refresh_job.py

# Paper Trading
src/paper_trading/mlb_paper_trader.py

# Backtesting
src/backtesting/mlb/__init__.py
src/backtesting/mlb/mlb_backtest_harness.py

# Tests
tests/test_mlb_stats_scraper.py
tests/test_mlb_linker.py
tests/test_mlb_feature_store.py
tests/test_mlb_monte_carlo.py
```

**Files to modify:** `src/orchestration/scheduler.py` (add MLB cron jobs)

---

## Key Reference Files (NBA patterns to replicate)

| Pattern | NBA File |
|---------|----------|
| Stats scraper | `src/scrapers/nba_unified_scraper.py` |
| Props scraper | `src/scrapers/daily_player_props_scraper.py` |
| Linker | `src/processing/nba_linker_local.py` |
| Rolling averages | `src/processing/populate_average_stats_incremental.py` |
| Feature store | `src/models/feature_store.py` |
| Quantile training | `src/models/quantile_trainer.py` |
| NegBin model | `src/models/truncated_negbin.py` |
| Monte Carlo | `src/models/monte_carlo.py` |
| Training pipeline | `src/models/train_pipeline.py` |
| Prediction store | `src/models/prediction_store.py` |
| Daily stats job | `src/orchestration/daily_stats_job.py` |
| Lines job | `src/orchestration/lines_job.py` |
| Inference job | `src/orchestration/inference_job.py` |
| Edge refresh | `src/orchestration/edge_refresh_job.py` |
| Paper trader | `src/paper_trading/paper_trader.py` |
| Backtesting | `src/backtesting/backtest_harness.py` |

---

## Timeline

| Week | Phase | Deliverables |
|------|-------|-------------|
| **Week 1** (Feb 24 - Mar 2) | Foundation | DB migration, MLB scraper, historical backfill running, Odds API scraper, linker, reference data |
| **Week 2** (Mar 3 - Mar 9) | Processing + Initial Models | Rolling averages, matchup features, platoon splits, feature store, begin model training |
| **Week 3** (Mar 10 - Mar 16) | Models + Backtesting | All model trainers, Monte Carlo, training pipeline, backtesting on 2025, model tuning |
| **Week 4** (Mar 17 - Mar 24) | Production Pipeline | Orchestration jobs, scheduler, paper trading, end-to-end testing, Railway deployment |
| **Mar 25** | **MLB Opening Day** | **System live** |
| **April+** | Frontend | Dashboard MLB tab, Discord commands, social media cards |

---

## Build Status (as of 2026-03-01)

### Completed (Phase 1 — Foundation)

| Component | File | Status |
|-----------|------|--------|
| DB schema (14 tables) | Supabase migration | Done |
| MLB Stats API scraper | `src/scrapers/mlb/mlb_stats_scraper.py` | Done |
| Boxscore backfill | `src/scrapers/mlb/mlb_backfill.py` | Done (2022-2025) |
| Statcast scraper | `src/scrapers/mlb/mlb_statcast_scraper.py` | Done |
| Statcast backfill | `src/scrapers/mlb/mlb_statcast_backfill.py` | **In progress** — 2022 done, 2023 ~65% done, 2024-2025 pending |
| FanGraphs scraper | `src/scrapers/mlb/mlb_fangraphs_scraper.py` | Done |
| Odds API props scraper | `src/scrapers/mlb/mlb_player_props_scraper.py` | Done |
| Daily props scraper | `src/scrapers/mlb/mlb_daily_player_props_scraper.py` | Done |
| Game lines scraper | `src/scrapers/mlb/mlb_daily_game_lines_scraper.py` | Done |
| Reference data | `src/scrapers/mlb/mlb_reference.py` | Done |
| MLB linker | `src/processing/mlb/mlb_linker.py` | Done |
| Rolling average config | `src/processing/mlb/mlb_config.py` | Done |
| Rolling average population | `src/processing/mlb/mlb_populate_averages.py` | Done |
| Incremental averages | `src/processing/mlb/mlb_populate_averages_incremental.py` | Done |

### Not Started (Phase 2-4)

| Component | Priority | Notes |
|-----------|----------|-------|
| Matchup features | Phase 2 | `mlb_matchup_features.py` — pitcher vs batter context |
| Platoon splits | Phase 2 | Integrate into rolling averages (vs LHP / vs RHP) |
| Feature store | Phase 2 | `src/models/mlb/mlb_feature_store.py` — largest single file |
| Pitcher K quantile trainer | Phase 3 | Reuse NBA pattern directly |
| Batter hits NegBin trainer | Phase 3 | Fresh implementation (see decisions below) |
| Batter HR binary classifier | Phase 3 | Simple XGBoost binary:logistic |
| Monte Carlo (pitcher) | Phase 3 | Adapt from NBA MC |
| Training pipeline | Phase 3 | Orchestrates all model types |
| Backtesting harness | Phase 3 | Walk-forward on 2025 season |
| Orchestration jobs | Phase 4 | Daily stats, lines, inference, edge refresh |
| Paper trader | Phase 4 | Adapt from NBA paper_trader.py |
| Scheduler integration | Phase 4 | Add MLB cron jobs to scheduler.py |

### Backfills Still Needed

1. **Statcast backfill** — currently running, ~6-7 hours remaining (2023 Sep onward + 2024-2025)
2. **FanGraphs seasons** — need to run for all seasons (2022-2025)
3. **Rolling averages population** — run `mlb_populate_averages.py` for batting + pitching after boxscores are complete
4. **Historical props backfill** — run `mlb_player_props_scraper.py` for 2023-2025 (Odds API historical)
5. **Linker** — run `mlb_linker.py` after props are loaded

---

## Modeling Decisions (confirmed 2026-03-01)

### Decision 1: Three model types, not one

| Prop Type | Model | Why | Output |
|-----------|-------|-----|--------|
| **Pitcher K, Pitcher Outs** | XGBoost Quantile Regression | Semi-continuous, range 2-14+. Same as NBA PTS/REB/AST. | Q10/Q25/Q50/Q75/Q90 → MC sampling → P(over/under) |
| **Batter Hits, TB, RBI, Runs** | Negative Binomial (mu, alpha) | Discrete counts (0-4), quantiles collapse. NegBin handles overdispersion (variance > mean). | Analytical P(X >= k) from NegBin CDF |
| **Batter HR, SB** | XGBoost Binary Classifier | ~90% zeros, effectively yes/no. Line is always 0.5. | Direct P(at least 1) |

All three model types produce the same output: **P(over line)** and **P(under line)**. Everything downstream (edge calculation, paper trading, dashboard, DFS) is model-agnostic.

### Decision 2: Fresh NegBin implementation, do NOT reuse threes code

The archived `truncated_negbin.py` from the THREES model experiment failed for multiple reasons:
- THREES had 50% missing prop lines and only produced 2 bets in backtesting
- The truncation logic (zero-truncated NegBin) added complexity that MLB hits don't need
- The hurdle model experiments (C3/C4/C5) were all dead ends

**Action:** Write `src/models/mlb/mlb_negbin_trainer.py` from scratch. Standard (non-truncated) NegBin. XGBoost predicts mu and alpha, `scipy.stats.nbinom` computes P(over line) analytically. No Monte Carlo needed for count stats.

### Decision 3: Build order — pitcher K first, batting stats second

1. **Pitcher K** (quantile regression) — known pattern, most liquid market, tightest juice. Ship first, validate edge.
2. **Batter Hits** (NegBin) — highest volume batter market, proves the NegBin approach works.
3. **Batter HR** (binary classifier) — add once hits pipeline is validated.
4. **TB, RBI, Runs** (NegBin) — same NegBin pattern as hits, quick to replicate.

**Rationale:** If we're not ready by March 25, pitcher K alone is a viable launch product. Batting stats can be added iteratively without disrupting the pipeline.

### Decision 4: No minutes-rate decomposition for MLB

NBA uses a two-stage model: minutes model → rate model → Monte Carlo combines them. This handles the bimodality of starter-vs-DNP.

MLB doesn't have this problem:
- Batters get 3-5 plate appearances per game (consistent)
- Starting pitchers pitch until pulled (5-7 innings typically)
- DNPs are handled by lineup confirmation, not modeled

MLB models predict the stat directly, not a rate.

---

## Next Steps (Phase 2 — after backfills complete)

### Step 1: Run remaining backfills
- Wait for Statcast backfill to finish (~6-7 hours)
- Run FanGraphs backfill for all seasons (2022-2025)
- Run rolling averages population (`mlb_populate_averages.py`) for batting + pitching
- Run historical props backfill from Odds API (2023-2025)
- Run linker on historical props

### Step 2: Build feature store (`src/models/mlb/mlb_feature_store.py`)
- SQL lateral joins pulling from `mlb_player_average_batting`, `mlb_player_average_pitching`, `mlb_team_average_stats`, `mlb_park_factors`
- Separate feature sets per prop type (pitcher K ~15 features, batter hits ~18 features)
- Same interface as NBA: `get_player_game_features()` for inference, `get_training_dataset()` for training

### Step 3: Build matchup features (`src/processing/mlb/mlb_matchup_features.py`)
- Opposing starter identification per game
- `opp_starter_k_per_9_l5`, `opp_starter_whip_l5`, `opp_starter_era_l5`, `opp_starter_handedness`
- Platoon splits: batter averages vs LHP and vs RHP separately

### Step 4: Build pitcher K model (quantile regression)
- `src/models/mlb/mlb_quantile_trainer.py` — thin wrapper around existing `quantile_trainer.py` pattern
- `src/models/mlb/mlb_monte_carlo.py` — sample from quantile distribution (no minutes step)
- Train on 2022-2024, calibrate on 2025

### Step 5: Build batter hits model (NegBin)
- `src/models/mlb/mlb_negbin_trainer.py` — fresh implementation
- XGBoost regressor predicts mu (mean) and alpha (dispersion)
- `scipy.stats.nbinom.sf(k-1, n, p)` for P(over k) — no MC sampling needed
- Train on 2022-2024, validate on 2025

### Step 6: Build batter HR model (binary classifier)
- `src/models/mlb/mlb_binary_trainer.py` — XGBoost binary:logistic
- Target: `(actual_hr >= 1).astype(int)`
- Output: P(HR >= 1) directly

### Step 7: Training pipeline + backtesting
- `src/models/mlb/mlb_train_pipeline.py` — orchestrates all model types
- `src/backtesting/mlb/mlb_backtest_harness.py` — walk-forward on 2025 season
- Calibration targets: pitcher K quantile gaps < 3%, NegBin P(over) within 3% of actual rates, HR AUC > 0.65

### Step 8: Production pipeline (Phase 4)
- Orchestration jobs, scheduler integration, paper trading, Railway deployment

---

## Risk Mitigation

1. **If models aren't ready by March 25:** Launch with pitcher strikeouts only (most like NBA, uses existing quantile regression). Add batting props iteratively.
2. **Odds API budget:** Core markets only initially (7 markets). Expand after validating edge.
3. **Spring training data:** Filter `game_type = 'R'` everywhere — spring training stats are meaningless for modeling.
4. **Early-season cold start:** L10/L20 windows will naturally pull from late 2025 season. Rolling average code handles cross-season lookback.
5. **Higher juice on MLB props:** Need larger edges to overcome vig. Edge thresholds of 8-10% vs NBA's 5%.
6. **NBA-MLB overlap (April-June):** Both systems run in parallel. Scheduler has separate cron entries, no conflicts. API budget needs to account for both sports.
