# League Priors History - Technical Documentation

## 1. Overview
The `league_priors_history` table stores **monthly snapshots** of league-wide betting stats (Points, Rebounds, Fouls, etc. per 100 Possessions).

**Purpose:**
* **Shrinkage:** Provides a stable baseline to "shrink" team stats towards early in the season when sample sizes are small.
* **Leakage Prevention:** By storing snapshots (e.g., "League Averages as of Nov 1st"), we prevent the model from "peeking" at future scoring trends (like the post-All-Star break scoring boom) when predicting early-season games.

---

## 2. Database Schema

**Action Required:** Run this SQL to create the table.

```sql
DROP TABLE IF EXISTS league_priors_history;

CREATE TABLE league_priors_history (
    -- Primary Key: Season + Position + Snapshot Date
    season_id TEXT NOT NULL,
    position_group TEXT NOT NULL, -- 'G', 'W', 'B'
    snapshot_date DATE NOT NULL,  -- The "As Of" Date
    
    -- League Averages (Per 100 Possessions)
    league_off_rtg NUMERIC(6,2),
    league_reb_per100 NUMERIC(6,2),
    league_ast_per100 NUMERIC(6,2),
    league_stl_per100 NUMERIC(6,2),
    league_blk_per100 NUMERIC(6,2),
    league_threes_per100 NUMERIC(6,2),
    league_tov_per100 NUMERIC(6,2),
    
    -- Sharp Stats
    league_fta_per100 NUMERIC(6,2),
    league_oreb_per100 NUMERIC(6,2),
    league_pf_per100 NUMERIC(6,2),
    
    -- Context
    total_possessions NUMERIC(12,2),
    total_games INT,
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (season_id, position_group, snapshot_date)
);

-- Index for fast "Time Travel" joins in the model
CREATE INDEX IF NOT EXISTS idx_league_priors_lookup 
ON league_priors_history(season_id, snapshot_date DESC);
```

## Backfill Script 

File: backfill_league_priors.py Logic:

Iterates through the 1st of every month during the NBA season (Nov–April).

Calculates league aggregates using only data available prior to that date.

Auto-detects the season_id from the database (no manual mapping required).

Includes Sharp Stats (FTA, OREB, Fouls).


## Usage in Model Training 

When constructing your training dataset, use this Lateral Join pattern. It finds the most recent league snapshot that existed before the game started.

```sql 
SELECT 
    game.game_date,
    game.team_id,
    
    -- The League Baseline (To be used for Shrinkage)
    priors.league_off_rtg as league_avg_pts_100,
    priors.league_threes_per100 as league_avg_3pm_100,
    priors.league_pf_per100 as league_avg_fouls_100
    
FROM games game
LEFT JOIN LATERAL (
    SELECT *
    FROM league_priors_history priors
    WHERE priors.season_id = game.season_id  -- Match the Season
      AND priors.snapshot_date < game.game_date -- "Time Travel" check
    ORDER BY priors.snapshot_date DESC
    LIMIT 1
) priors ON TRUE;
```









## Related Documentation
- [Documentation Index](index.md)
- [Feature Store](feature_store_documentation.md)
