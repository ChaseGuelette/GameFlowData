"""
Update Player Position History Table

This script updates the player_position_history table with a new snapshot.
Run this 4x per year at key NBA dates:
  - October 1st (pre-season baseline)
  - December 25th (early season/Christmas)
  - February 15th (post-trade deadline)
  - April 15th (pre-playoff roles)

Usage:
  python update_player_position_history.py                    # Uses current date
  python update_player_position_history.py --date 2025-01-15  # Specific snapshot date
  python update_player_position_history.py --backfill         # Backfill all historical snapshots
"""

import argparse
import logging
from datetime import datetime, date
from sqlalchemy import text
from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def update_snapshot(engine, snapshot_date: date):
    """
    Insert or update player position history for a specific snapshot date.

    Analyzes games from (snapshot_date - 1 year) to snapshot_date.
    Uses position hierarchy tie-breaker: Center > Forward > Guard.
    """
    logger.info(f"Updating player_position_history for snapshot: {snapshot_date}")

    query = text("""
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
            WHERE box.game_date::DATE BETWEEN (:snap_date::DATE - INTERVAL '1 year')
                                          AND :snap_date::DATE
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
            :snap_date::DATE,
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
            primary_position = EXCLUDED.primary_position,
            season_id = EXCLUDED.season_id,
            total_games_in_window = EXCLUDED.total_games_in_window;
    """)

    with engine.begin() as conn:
        result = conn.execute(query, {'snap_date': snapshot_date.isoformat()})
        logger.info(f"Snapshot {snapshot_date} updated successfully")


def backfill_all_snapshots(engine):
    """
    Backfill all historical snapshots from 2018 to current year.

    Generates snapshots for 4 key dates per season:
      - October 1st, December 25th, February 15th, April 15th
    """
    logger.info("Starting full backfill of player_position_history...")

    query = text("""
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
        WITH snapshot_dates AS (
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
        WHERE rn = 1
        ON CONFLICT (player_id, snapshot_date)
        DO UPDATE SET
            position_group = EXCLUDED.position_group,
            position_confidence = EXCLUDED.position_confidence,
            team_id = EXCLUDED.team_id,
            primary_position = EXCLUDED.primary_position,
            season_id = EXCLUDED.season_id,
            total_games_in_window = EXCLUDED.total_games_in_window;
    """)

    with engine.begin() as conn:
        conn.execute(query)
        logger.info("Full backfill completed successfully")


def main():
    parser = argparse.ArgumentParser(description='Update player position history snapshots')
    parser.add_argument('--date', type=str, help='Specific snapshot date (YYYY-MM-DD). Defaults to today.')
    parser.add_argument('--backfill', action='store_true', help='Backfill all historical snapshots (2018-present)')
    args = parser.parse_args()

    engine = get_engine()

    if args.backfill:
        backfill_all_snapshots(engine)
    else:
        if args.date:
            snapshot_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        else:
            snapshot_date = date.today()

        update_snapshot(engine, snapshot_date)

    # Show current snapshot count
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as cnt, COUNT(DISTINCT snapshot_date) as dates FROM player_position_history"))
        row = result.fetchone()
        logger.info(f"Total records: {row[0]:,} across {row[1]} snapshot dates")


if __name__ == "__main__":
    main()
