# NBA basic catch-up SQL diagnosis — 2026-11-07

## Scope and guards

- Target: canonical remote `DATABASE_URL`; direct psycopg2 connection loaded from repository env.
- Read-only diagnosis only. The underlying SELECT was not executed; `EXPLAIN` used no `ANALYZE`.
- Explicit DB execute calls: **5 exactly**.
  1. `BEGIN TRANSACTION READ ONLY`
  2. `SET LOCAL statement_timeout = '15s'`
  3. guard/index metadata SELECT
  4. `EXPLAIN (FORMAT JSON)`
  5. `ROLLBACK`
- Guard readback in call 3: `transaction_read_only = on`; `statement_timeout = 15s`.

## Exact index definitions returned

### `player_average_game_stats`

- `CREATE INDEX idx_pags_player_date ON public.player_average_game_stats USING btree (player_id, game_date DESC)`
- `CREATE INDEX idx_player_avg_date ON public.player_average_game_stats USING btree (game_date)`
- `CREATE INDEX idx_player_avg_season ON public.player_average_game_stats USING btree (player_id, season_id)`
- `CREATE INDEX idx_player_avg_team ON public.player_average_game_stats USING btree (team_id, game_date)`
- `CREATE UNIQUE INDEX pk_player_avg_game_stats ON public.player_average_game_stats USING btree (player_id, game_id)`

### `player_game_stats`

- `CREATE INDEX idx_player_game_date ON public.player_game_stats USING btree (game_date)`
- `CREATE INDEX idx_player_game_lookup ON public.player_game_stats USING btree (player_id, game_id)`
- `CREATE INDEX idx_player_game_season ON public.player_game_stats USING btree (season_id)`
- `CREATE UNIQUE INDEX pk_player_game_stats ON public.player_game_stats USING btree (player_id, game_id)`

### `team_average_game_stats`

- `CREATE INDEX idx_tags_team_date ON public.team_average_game_stats USING btree (team_id, game_date DESC)`
- `CREATE INDEX idx_team_avg_date ON public.team_average_game_stats USING btree (game_date)`
- `CREATE INDEX idx_team_avg_season ON public.team_average_game_stats USING btree (team_id, season_id)`
- `CREATE UNIQUE INDEX pk_team_avg_game_stats ON public.team_average_game_stats USING btree (team_id, game_id)`

### `team_game_stats`

- `CREATE INDEX idx_team_game_date ON public.team_game_stats USING btree (game_date)`
- `CREATE INDEX idx_team_game_season ON public.team_game_stats USING btree (season_id)`
- `CREATE INDEX idx_team_game_stats_opponent ON public.team_game_stats USING btree (opponent_id)`
- `CREATE UNIQUE INDEX pk_team_game_stats ON public.team_game_stats USING btree (team_id, game_id)`

## Explained SQL

Call 4 was `EXPLAIN (FORMAT JSON)` of the exact source SELECT supplied for `discover_basic_catchup`, with the normal CLI literal `LIMIT 14`. No bind fallback was needed.

```sql
WITH latest_season AS (
    SELECT season_id
    FROM player_game_stats
    WHERE game_id LIKE ANY (ARRAY['002%', '004%', '005%'])
      AND game_date::date < CURRENT_DATE
    ORDER BY game_date DESC
    LIMIT 1
), missing_dates AS (
    SELECT DISTINCT pgs.game_date::date AS game_date
    FROM player_game_stats pgs
    JOIN latest_season ls ON ls.season_id = pgs.season_id
    LEFT JOIN player_average_game_stats pags
      ON pags.player_id = pgs.player_id AND pags.game_id = pgs.game_id
    WHERE pgs.game_date::date < CURRENT_DATE
      AND (pgs.did_not_play = false OR pgs.did_not_play IS NULL)
      AND pags.player_id IS NULL
    UNION
    SELECT DISTINCT tgs.game_date::date AS game_date
    FROM team_game_stats tgs
    JOIN latest_season ls ON ls.season_id = tgs.season_id
    LEFT JOIN team_average_game_stats tags
      ON tags.team_id = tgs.team_id AND tags.game_id = tgs.game_id
    WHERE tgs.game_date::date < CURRENT_DATE
      AND tags.team_id IS NULL
    UNION
    SELECT DISTINCT tgs.game_date::date AS game_date
    FROM team_game_stats tgs
    JOIN latest_season ls ON ls.season_id = tgs.season_id
    WHERE tgs.game_date::date < CURRENT_DATE
      AND NOT EXISTS (
          SELECT 1 FROM player_game_stats pgs
          WHERE pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
      )
    UNION
    SELECT DISTINCT pgs.game_date::date AS game_date
    FROM player_game_stats pgs
    JOIN latest_season ls ON ls.season_id = pgs.season_id
    WHERE pgs.game_date::date < CURRENT_DATE
      AND NOT EXISTS (
          SELECT 1 FROM team_game_stats tgs
          WHERE tgs.game_id = pgs.game_id AND tgs.team_id = pgs.team_id
      )
)
SELECT (SELECT season_id FROM latest_season), game_date,
       COUNT(*) OVER () AS total_missing_dates
FROM missing_dates
ORDER BY game_date ASC
LIMIT 14;
```

## Compact plan evidence

All figures are planner estimates because `ANALYZE` was prohibited.

- Root: `Limit` → `Sort` → `WindowAgg` → hashed `Aggregate` over an `Append`; total estimated cost **81705.39**, with **1,276** deduplicated date rows before the final `LIMIT 14`. The window count and ascending order require processing the complete missing-date result before the limit can return.
- `latest_season`: backward `Index Scan` on `idx_player_game_date`; index condition `game_date < CURRENT_DATE`, while the `game_id LIKE ANY (...)` test remains a filter. Estimated one-row limited cost **0.52**; underlying scan estimate is **431,393** candidate rows/cost **41181.38**, but the backward scan can stop after the first qualifying row.
- Missing player averages: season-scoped `Bitmap Index Scan` on `idx_player_game_season` → `Bitmap Heap Scan` of about **15,021** player rows, then nested-loop anti join using index-only probes of `pk_player_avg_game_stats (player_id, game_id)`. Branch cost **20473.89**. The average-table lookup index is used correctly.
- Missing team averages: `Index Scan` on `idx_team_game_season` over about **1,396** team rows, then index-only anti probes of `pk_team_avg_game_stats (team_id, game_id)`. Branch cost **990.95**. The average-table lookup index is used correctly.
- Team games lacking player rows: season-scoped outer `idx_team_game_season` scan (~**1,396** rows), but the anti join builds a hash from a full `Seq Scan` of `player_game_stats` (~**444,448** rows; scan cost **32595.48**). Branch cost **45269.29**, the largest branch. None of the player indexes begins with the required `(game_id, team_id)` lookup shape, and the inner subquery is not season-scoped, so no existing selective season index is used there.
- Player rows lacking team games: season-scoped outer `idx_player_game_season` bitmap scan (~**15,326** rows), then hash anti join against all estimated **40,472** `team_game_stats` index entries via `pk_team_game_stats`; branch cost **14876.22**. The primary-key index is scanned in full rather than used as repeated keyed probes.
- Each branch performs its own `Sort`/`Unique`, followed by a global hashed aggregate for the `UNION`; this is secondary overhead, not the dominant estimate.

## Bottleneck verdict

The plan supports a query-shape bottleneck in the two cross-table `NOT EXISTS` branches, especially the team-to-player check. Although the outer rows are already restricted to `latest_season`, the inner anti-join inputs are not. PostgreSQL therefore plans to hash the entire player table (estimated 444,448 rows) and the full team primary-key index (estimated 40,472 rows). The dominant team-to-player branch contributes about **45,269** of the root's **81,705** estimated cost. The average-stat anti joins already use their composite primary-key indexes and are not the primary defect.

## Minimal query-shape fix supported by this plan

Scope each `NOT EXISTS` inner relation to the same latest season already established by the outer branch:

```sql
-- Team row missing player rows
AND NOT EXISTS (
    SELECT 1
    FROM player_game_stats pgs
    WHERE pgs.season_id = ls.season_id
      AND pgs.game_id = tgs.game_id
      AND pgs.team_id = tgs.team_id
)

-- Player row missing team row
AND NOT EXISTS (
    SELECT 1
    FROM team_game_stats tgs
    WHERE tgs.season_id = ls.season_id
      AND tgs.game_id = pgs.game_id
      AND tgs.team_id = pgs.team_id
)
```

This is a query-shape change only: it gives the planner access to the existing season indexes and avoids asking the anti joins to consider unrelated seasons. No DDL is proposed. A later implementation should preserve the intended season semantics and be verified separately; this receipt does not repair or execute the daily job.

## Daily-job relevance

`discover_basic_catchup` is the discovery gate for the real daily stats workflow. Reducing the two unscoped anti-join inputs targets the plan node delaying that gate, so the next engineering step is a narrow source change followed by an actual daily-job-path retest—not another broad database audit.
