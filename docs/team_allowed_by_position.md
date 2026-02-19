# Team Allowed By Position - Technical Documentation

## 1. Overview
The `team_allowed_by_position` table is a **Feature Store** designed for NBA betting models. It aggregates defensive performance metrics (Points, Rebounds, 3PM, etc.) allowed by a team to specific position groups (Guard, Wing, Big).

**Key Features:**
* **Granularity:** Aggregated by Position Group (G, W, B) rather than team-level totals.
* **Pace Adjustment:** All metrics are available as "Per 100 Possessions" rates to normalize for pace variance.
* **Rolling Windows:** Includes L5 (Last 5), L15, and Season-to-Date windows to capture streaks vs. baseline.
* **Leakage Prevention:** Uses `shift(1)` logic to ensure stats reflect data *entering* a game, not *including* the game itself.

---

## 2. Database Schema

```sql
CREATE TABLE IF NOT EXISTS team_allowed_by_position (
    team_id BIGINT NOT NULL,
    game_id TEXT NOT NULL,
    game_date DATE NOT NULL,
    position_group TEXT NOT NULL, -- 'G', 'W', 'B'
    
    -- Context (Denominators)
    games_l5 SMALLINT,
    games_l15 SMALLINT,
    games_szn SMALLINT,
    poss_faced_l5 NUMERIC(8,2),
    poss_faced_l15 NUMERIC(8,2),
    poss_faced_szn NUMERIC(10,2),

    -- Raw Totals (Numerators)
    pts_allowed_l5 NUMERIC(8,2), pts_allowed_l15 NUMERIC(8,2), pts_allowed_szn NUMERIC(10,2),
    reb_allowed_l5 NUMERIC(8,2), reb_allowed_l15 NUMERIC(8,2), reb_allowed_szn NUMERIC(10,2),
    ast_allowed_l5 NUMERIC(8,2), ast_allowed_l15 NUMERIC(8,2), ast_allowed_szn NUMERIC(10,2),
    stl_allowed_l5 NUMERIC(8,2), stl_allowed_l15 NUMERIC(8,2), stl_allowed_szn NUMERIC(10,2),
    blk_allowed_l5 NUMERIC(8,2), blk_allowed_l15 NUMERIC(8,2), blk_allowed_szn NUMERIC(10,2),
    threes_allowed_l5 NUMERIC(8,2), threes_allowed_l15 NUMERIC(8,2), threes_allowed_szn NUMERIC(10,2),
    tov_forced_l5 NUMERIC(8,2), tov_forced_l15 NUMERIC(8,2), tov_forced_szn NUMERIC(10,2),

    -- Pace Adjusted Rates (Per 100 Possessions)
    off_rtg_allowed_l5 NUMERIC(6,2), off_rtg_allowed_l15 NUMERIC(6,2), off_rtg_allowed_szn NUMERIC(6,2),
    reb_per100_allowed_l5 NUMERIC(6,2), reb_per100_allowed_l15 NUMERIC(6,2), reb_per100_allowed_szn NUMERIC(6,2),
    ast_per100_allowed_l5 NUMERIC(6,2), ast_per100_allowed_l15 NUMERIC(6,2), ast_per100_allowed_szn NUMERIC(6,2),
    stl_per100_allowed_l5 NUMERIC(6,2), stl_per100_allowed_l15 NUMERIC(6,2), stl_per100_allowed_szn NUMERIC(6,2),
    blk_per100_allowed_l5 NUMERIC(6,2), blk_per100_allowed_l15 NUMERIC(6,2), blk_per100_allowed_szn NUMERIC(6,2),
    threes_per100_allowed_l5 NUMERIC(6,2), threes_per100_allowed_l15 NUMERIC(6,2), threes_per100_allowed_szn NUMERIC(6,2),
    tov_per100_forced_l5 NUMERIC(6,2), tov_per100_forced_l15 NUMERIC(6,2), tov_per100_forced_szn NUMERIC(6,2),

    created_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (team_id, game_id, position_group),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_team_allowed_lookup 
ON team_allowed_by_position(team_id, position_group, game_date DESC);
```

## Backfill Procedure

**Full backfill:** `python src/processing/backfill_opponent_allowed.py` — Processes all seasons. Rolling windows use `.mean()` (per-game averages, fixed in Session 43). Requires TRUNCATE if re-running.

**Incremental backfill:** `python src/processing/backfill_opponent_allowed_incremental.py --days-back 30` — Lightweight daily version, processes last 30 days with 15-day lookback buffer for L15 window calculations. Runs automatically as Step 7 in `daily_stats_job.py`. Uses UPSERT to avoid duplicates.

## Maintenance Script

File: `backfill_opponent_allowed_incremental.py` — Schedule: Runs daily at 9 AM ET via `daily_stats_job.py`. Logic: Fetches games from the last 30+15 days, computes rolling metrics, filters to target date range, upserts into `team_allowed_by_position`.

### Legacy Maintenance Script (Superseded)

File: update_opponent_allowed.py — Original maintenance approach. Superseded by the incremental backfill script above.

```python
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from backfill_opponent_allowed import fetch_raw_allowed_stats, compute_rolling_metrics, batch_insert_to_db

def update_team_allowed_recent(engine):
    # 1. Find last update
    with engine.connect() as conn:
        last_date = conn.execute(text("SELECT MAX(game_date) FROM team_allowed_by_position")).scalar()
    
    if not last_date:
        print("Table empty. Run backfill first.")
        return

    print(f"Last processed: {last_date}")
    
    # 2. Find active seasons
    with engine.connect() as conn:
        seasons = [row[0] for row in conn.execute(text("SELECT DISTINCT season_id FROM team_game_stats WHERE team_game_date::DATE > :d"), {'d': last_date})]
    
    if not seasons:
        print("No new games found.")
        return

    # 3. Buffer fetch (60 days lookback)
    lookback = last_date - pd.Timedelta(days=60)
    query = text(f"""
        SELECT 
            tgs.team_id, tgs.game_id, tgs.team_game_date::DATE as game_date, tgs.season_id,
            COALESCE(ph.position_group, 'U') as position_group,
            COALESCE(SUM(pgs.pts), 0) as pts, COALESCE(SUM(pgs.reb), 0) as reb, COALESCE(SUM(pgs.ast), 0) as ast,
            COALESCE(SUM(pgs.fg3m), 0) as threes, COALESCE(SUM(pgs.stl), 0) as stl, COALESCE(SUM(pgs.blk), 0) as blk,
            COALESCE(SUM(pgs.tov), 0) as tov, COALESCE(SUM(adv.possessions), 0) as poss_faced
        FROM team_game_stats tgs
        JOIN player_game_stats pgs ON tgs.game_id = pgs.game_id AND tgs.team_id != pgs.team_id AND pgs.min > 0
        JOIN player_game_advanced_stats adv ON pgs.game_id = adv.game_id AND pgs.player_id = adv.player_id
        LEFT JOIN LATERAL (
            SELECT position_group FROM player_position_history ph
            WHERE ph.player_id = pgs.player_id AND ph.snapshot_date < tgs.team_game_date::DATE
            ORDER BY ph.snapshot_date DESC LIMIT 1
        ) ph ON TRUE
        WHERE tgs.season_id IN :seasons AND tgs.team_game_date::DATE >= :lookback
        GROUP BY tgs.team_id, tgs.game_id, tgs.team_game_date, tgs.season_id, ph.position_group
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'seasons': tuple(seasons), 'lookback': lookback})

    # 4. Calculate & Filter
    df_processed = compute_rolling_metrics(df)
    df_new = df_processed[df_processed['game_date'] > last_date]
    
    # 5. Insert
    if not df_new.empty:
        print(f"Inserting {len(df_new)} new rows...")
        batch_insert_to_db(engine, df_new)
    else:
        print("No valid new rows generated.")

if __name__ == "__main__":
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))
    update_team_allowed_recent(engine)
```

## Usage in betting models

Example 1: Betting Analysis
Question: "How do the Lakers defend Guards recently compared to their season average?"

```sql

SELECT 
    t.team_name,
    tab.game_date,
    -- Compare Recency (L5) vs Baseline (Season)
    tab.off_rtg_allowed_l5 AS def_rating_l5,
    tab.off_rtg_allowed_szn AS def_rating_szn,
    (tab.off_rtg_allowed_l5 - tab.off_rtg_allowed_szn) AS slump_metric,
    -- 3-Point Defense (Critical for Props)
    tab.threes_per100_allowed_l5,
    tab.threes_per100_allowed_szn
FROM team_allowed_by_position tab
JOIN teams t ON tab.team_id = t.team_id
WHERE t.team_name = 'Lakers' 
  AND tab.position_group = 'G'
ORDER BY tab.game_date DESC
LIMIT 5;
```

Example 2: Training Data Extraction
Strictly joins to the Opponent's defensive stats entering the game.

```sql 
SELECT 
    -- The Target (What actually happened)
    pgs.player_id,
    pgs.pts AS actual_points,
    
    -- The Feature (Opponent's defense vs this position)
    def.off_rtg_allowed_l5 AS opp_def_rating_l5,
    def.threes_per100_allowed_szn AS opp_3pt_defense_szn,
    
    -- Context
    ph.position_group
FROM player_game_stats pgs
JOIN team_game_stats tgs ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
-- 1. Get Player Position
LEFT JOIN LATERAL (
    SELECT position_group FROM player_position_history ph 
    WHERE ph.player_id = pgs.player_id AND ph.snapshot_date < pgs.game_date::DATE 
    ORDER BY ph.snapshot_date DESC LIMIT 1
) ph ON TRUE
-- 2. Join Opponent Defense
JOIN team_allowed_by_position def 
    ON def.team_id = tgs.opponent_id         -- The Team Defending
    AND def.game_id = tgs.game_id            -- The Specific Game
    AND def.position_group = ph.position_group; -- The Position they are defending
```


## Related Documentation
- [Documentation Index](index.md)
- [Feature Store](feature_store_documentation.md)
- [Player Position History](player_position_history.md)
