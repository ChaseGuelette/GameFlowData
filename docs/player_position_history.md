# Player Position History Table Documentation

## 1. Overview & Purpose

The `player_position_history` table is a **Periodic Snapshot Fact Table** designed to track the evolution of NBA player roles over time.

**Critical Goal:** Prevent **Look-Ahead Bias (Data Leakage)** in historical backtesting for the betting model.

By snapshotting player roles at fixed intervals (4 times per year), we ensure that a backtest run on a game in *December 2021* uses only the role information available *as of December 2021*, rather than the player's eventual role at the end of the season.

---

## 2. Schema Definition

The table uses a **Composite Primary Key** (`player_id` + `snapshot_date`) to allow multiple historical records per player.

| Column | Type | Description |
|------|------|-------------|
| **`player_id`** | `BIGINT` | Foreign Key to `players`. Part of PK. |
| **`snapshot_date`** | `DATE` | The effective date of the classification. Part of PK. |
| `team_id` | `BIGINT` | The player's team at the time of the snapshot. |
| `season_id` | `TEXT` | Season context (e.g., `2023-24`). |
| `primary_position` | `TEXT` | Raw position code (e.g., `C-F`, `G`). |
| **`position_group`** | `TEXT` | Simplified betting role: **G** (Guard), **W** (Wing), **B** (Big). |
| `position_confidence` | `NUMERIC` | Percentage of games played at this position in the lookback window. |
| `total_games_in_window` | `INT` | Sample size used for this classification. |

### DDL Script

```sql
CREATE TABLE public.player_position_history (
    player_id BIGINT NOT NULL,
    team_id BIGINT NOT NULL,
    snapshot_date DATE NOT NULL,
    season_id TEXT,
    primary_position TEXT,
    position_group TEXT,
    position_confidence NUMERIC(4,3),
    total_games_in_window INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT pk_player_position_history
        PRIMARY KEY (player_id, snapshot_date),

    CONSTRAINT fk_pos_hist_player
        FOREIGN KEY (player_id)
        REFERENCES public.players(player_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    CONSTRAINT fk_pos_hist_team
        FOREIGN KEY (team_id)
        REFERENCES public.teams(team_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pos_hist_lookup
    ON public.player_position_history (player_id, snapshot_date DESC);
```

## Classifcation Logic 
For every snapshot, games are analyzed from:
(snapshot_date - 1 year) → snapshot_date

## Grouping Logic

Raw positions are mapped into three betting groups.

Raw Position	Group	Logic
G	G	Primary ball handlers and point-of-attack defenders
G-F, F-G, F	W	6'5"+ perimeter players (SG / SF archetypes)
F-C, C-F, C	B	Rim protectors and primary rebounders


## Tie-Breaker Hierarchy 
If a player splits time evenly between positions, the model defaults to the larger defensive role.

Precedence Order:
Center > Forward > Guard

Rationale:
In betting, it is safer to assume a hybrid player anchors a defense than to assume perimeter responsibilities.

## Initial Setup 
Ran once to populate snapshots to 2018

```sql
INSERT INTO public.player_position_history (
    player_id,
    team_id,
    snapshot_date,
    season_id,
    primary_position,
    position_group,
    position_confidence,
    total_games_in_window
)
WITH RECURSIVE snapshot_dates AS (
    SELECT d::date AS snap_date
    FROM generate_series(2018, 2026) AS year_num,
    LATERAL (
        VALUES
            (MAKE_DATE(year_num, 10, 1)),
            (MAKE_DATE(year_num, 12, 25)),
            (MAKE_DATE(year_num + 1, 2, 15)),
            (MAKE_DATE(year_num + 1, 4, 15))
    ) AS v(d)
    WHERE d <= CURRENT_DATE
),
player_data_joined AS (
    SELECT
        adv.player_id,
        adv.team_id,
        adv.position,
        box.game_date::DATE AS game_date,
        box.season_id
    FROM public.player_game_advanced_stats adv
    JOIN public.player_game_stats box
        ON adv.game_id = box.game_id
       AND adv.player_id = box.player_id
    WHERE adv.position IS NOT NULL
      AND (adv.did_not_play = FALSE OR adv.did_not_play IS NULL)
),
player_windows AS (
    SELECT
        sd.snap_date,
        p.player_id,
        p.team_id,
        p.season_id,
        p.position,
        p.game_date
    FROM snapshot_dates sd
    JOIN player_data_joined p
      ON p.game_date BETWEEN (sd.snap_date - INTERVAL '1 year')
                         AND sd.snap_date
),
latest_teams AS (
    SELECT DISTINCT ON (player_id, snap_date)
        player_id,
        snap_date,
        team_id,
        season_id
    FROM player_windows
    ORDER BY player_id, snap_date, game_date DESC
),
position_counts AS (
    SELECT
        snap_date,
        player_id,
        position,
        COUNT(*) AS games_at_pos
    FROM player_windows
    GROUP BY snap_date, player_id, position
),
ranked_positions AS (
    SELECT
        pc.snap_date,
        pc.player_id,
        lt.team_id,
        lt.season_id,
        pc.position,
        pc.games_at_pos,
        SUM(pc.games_at_pos)
            OVER (PARTITION BY pc.player_id, pc.snap_date) AS total_games,
        ROW_NUMBER() OVER (
            PARTITION BY pc.player_id, pc.snap_date
            ORDER BY
                pc.games_at_pos DESC,
                CASE pc.position
                    WHEN 'C'   THEN 1
                    WHEN 'C-F' THEN 2
                    WHEN 'F-C' THEN 3
                    WHEN 'F'   THEN 4
                    WHEN 'G-F' THEN 5
                    WHEN 'F-G' THEN 6
                    ELSE 7
                END
        ) AS rn
    FROM position_counts pc
    JOIN latest_teams lt
      ON pc.player_id = lt.player_id
     AND pc.snap_date = lt.snap_date
)
SELECT
    player_id,
    team_id,
    snap_date,
    season_id,
    position,
    CASE
        WHEN position = 'G' THEN 'G'
        WHEN position IN ('G-F', 'F-G', 'F') THEN 'W'
        ELSE 'B'
    END,
    games_at_pos::NUMERIC / NULLIF(total_games, 0),
    total_games
FROM ranked_positions
WHERE rn = 1;
```

## Maintenance Schedule

This table does not update live.
It must be refreshed four times per year via a scheduled job (cron):

October 1st — Pre-season baseline

December 25th — Early season / Christmas rotations

February 15th — Post-trade deadline / All-Star reset

April 15th — Final pre-playoff roles

```sql
INSERT INTO public.player_position_history (
    player_id,
    team_id,
    snapshot_date,
    season_id,
    primary_position,
    position_group,
    position_confidence,
    total_games_in_window
)
WITH recent_window AS (
    SELECT
        adv.player_id,
        adv.team_id,
        adv.position,
        box.game_date::DATE AS game_date,
        box.season_id
    FROM public.player_game_advanced_stats adv
    JOIN public.player_game_stats box
        ON adv.game_id = box.game_id
       AND adv.player_id = box.player_id
    WHERE box.game_date::DATE BETWEEN (CURRENT_DATE - INTERVAL '1 year')
                                  AND CURRENT_DATE
      AND adv.position IS NOT NULL
      AND (adv.did_not_play = FALSE OR adv.did_not_play IS NULL)
),
latest_team_info AS (
    SELECT DISTINCT ON (player_id)
        player_id,
        team_id,
        season_id
    FROM recent_window
    ORDER BY player_id, game_date DESC
),
position_counts AS (
    SELECT
        player_id,
        position,
        COUNT(*) AS games_at_pos
    FROM recent_window
    GROUP BY player_id, position
),
ranked_positions AS (
    SELECT
        pc.player_id,
        lt.team_id,
        lt.season_id,
        pc.position,
        pc.games_at_pos,
        SUM(pc.games_at_pos)
            OVER (PARTITION BY pc.player_id) AS total_games,
        ROW_NUMBER() OVER (
            PARTITION BY pc.player_id
            ORDER BY
                pc.games_at_pos DESC,
                CASE pc.position
                    WHEN 'C'   THEN 1
                    WHEN 'C-F' THEN 2
                    WHEN 'F-C' THEN 3
                    WHEN 'F'   THEN 4
                    WHEN 'G-F' THEN 5
                    WHEN 'F-G' THEN 6
                    ELSE 7
                END
        ) AS rn
    FROM position_counts pc
    JOIN latest_team_info lt
      ON pc.player_id = lt.player_id
)
SELECT
    player_id,
    team_id,
    CURRENT_DATE,
    season_id,
    position,
    CASE
        WHEN position = 'G' THEN 'G'
        WHEN position IN ('G-F', 'F-G', 'F') THEN 'W'
        ELSE 'B'
    END,
    games_at_pos::NUMERIC / NULLIF(total_games, 0),
    total_games
FROM ranked_positions
WHERE rn = 1
ON CONFLICT (player_id, snapshot_date)
DO UPDATE SET
    position_group = EXCLUDED.position_group,
    position_confidence = EXCLUDED.position_confidence,
    team_id = EXCLUDED.team_id,
    updated_at = NOW();
```


## Usage in backtesting

When training ML models on historical games, always join to the most recent snapshot strictly before the game date.

```sql
SELECT
    g.game_date,
    g.player_id,
    g.pts,
    ph.position_group AS role_at_game_time,
    ph.position_confidence
FROM player_game_stats g
LEFT JOIN LATERAL (
    SELECT
        position_group,
        position_confidence
    FROM player_position_history ph
    WHERE ph.player_id = g.player_id
      AND ph.snapshot_date < g.game_date::DATE
    ORDER BY ph.snapshot_date DESC
    LIMIT 1
) ph ON TRUE;
```











## Related Documentation
- [Documentation Index](index.md)
- [Update Player Position History](update_player_position_history_documentation.md)
- [Feature Store](feature_store_documentation.md)
- [Team Allowed By Position](team_allowed_by_position.md)
