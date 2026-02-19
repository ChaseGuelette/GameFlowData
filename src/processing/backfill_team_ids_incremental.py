"""
Incremental Backfill team_id in raw_player_props_combined.

Only processes recent rows (by staging_id threshold) to avoid
re-processing historical data that may have unresolvable team_id issues.

Uses player's most recent team from player_game_stats and matches it to
the home/away team in team_game_stats for that game.

Run: python src/processing/backfill_team_ids_incremental.py [--days-back 7]
"""

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from tqdm import tqdm

from src.db.client import get_engine

BATCH_SIZE = 5000


def main():
    parser = argparse.ArgumentParser(description="Incremental team_id backfill")
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="Only process rows from the last N days (default: 7)",
    )
    parser.add_argument(
        "--staging-id-threshold",
        type=int,
        default=None,
        help="Only process rows with staging_id > this value (overrides --days-back)",
    )
    args = parser.parse_args()

    engine = get_engine()

    # Determine the staging_id threshold
    if args.staging_id_threshold:
        min_staging_id = args.staging_id_threshold
        print(f"Using explicit staging_id threshold: {min_staging_id:,}")
    else:
        # Find staging_id threshold based on days-back
        print(f"Finding staging_id threshold for last {args.days_back} days...")
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                SELECT COALESCE(MIN(staging_id), 0) as threshold
                FROM raw_player_props_combined
                WHERE snapshot_time >= CURRENT_DATE - :days_back
            """),
                {"days_back": args.days_back},
            ).fetchone()
            min_staging_id = result[0] if result[0] else 0

        if min_staging_id == 0:
            print(f"No data found in the last {args.days_back} days. Exiting.")
            return

        print(f"Staging ID threshold: {min_staging_id:,}")

    # Count rows to backfill
    print("Counting rows to backfill...")
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT COUNT(*) FROM raw_player_props_combined
            WHERE team_id IS NULL
              AND player_id IS NOT NULL
              AND staging_id >= :min_staging_id
        """),
            {"min_staging_id": min_staging_id},
        ).fetchone()
        total_to_fix = result[0]

    print(f"Found {total_to_fix:,} rows missing team_id (recent data only)")

    if total_to_fix == 0:
        print("Nothing to backfill!")
        return

    fixed = 0
    pbar = tqdm(total=total_to_fix, desc="Backfilling team_ids")

    while True:
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                WITH to_update AS (
                    SELECT p.staging_id
                    FROM raw_player_props_combined p
                    WHERE p.team_id IS NULL
                      AND p.player_id IS NOT NULL
                      AND p.staging_id >= :min_staging_id
                    LIMIT :batch_size
                ),
                player_recent_teams AS (
                    SELECT DISTINCT ON (player_id)
                        player_id, team_id as recent_team_id
                    FROM player_game_stats
                    ORDER BY player_id, game_date DESC, game_id DESC
                ),
                derived AS (
                    SELECT
                        p.staging_id,
                        CASE
                            WHEN prt.recent_team_id = t_home.team_id THEN t_home.team_id
                            WHEN prt.recent_team_id = t_away.team_id THEN t_away.team_id
                            ELSE NULL
                        END as derived_team_id
                    FROM raw_player_props_combined p
                    JOIN to_update tu ON p.staging_id = tu.staging_id
                    LEFT JOIN player_recent_teams prt ON p.player_id = prt.player_id
                    LEFT JOIN team_game_stats t_home ON p.game_id = t_home.game_id
                        AND t_home.team_matchup LIKE '%vs.%'
                    LEFT JOIN team_game_stats t_away ON p.game_id = t_away.game_id
                        AND t_away.team_matchup LIKE '%@%'
                )
                UPDATE raw_player_props_combined p
                SET team_id = d.derived_team_id
                FROM derived d
                WHERE p.staging_id = d.staging_id AND d.derived_team_id IS NOT NULL
                RETURNING p.staging_id
            """),
                {"batch_size": BATCH_SIZE, "min_staging_id": min_staging_id},
            )

            rows_updated = len(result.fetchall())

        if rows_updated == 0:
            # Check if there are still rows to process
            with engine.connect() as conn:
                remaining = conn.execute(
                    text("""
                    SELECT COUNT(*) FROM raw_player_props_combined
                    WHERE team_id IS NULL
                      AND player_id IS NOT NULL
                      AND staging_id >= :min_staging_id
                """),
                    {"min_staging_id": min_staging_id},
                ).fetchone()[0]

            if remaining > 0:
                print(f"\n{remaining:,} rows could not be backfilled (player's team not in game)")
            break

        fixed += rows_updated
        pbar.update(rows_updated)

    pbar.close()
    print(f"\nDone! Backfilled {fixed:,} rows.")


if __name__ == "__main__":
    main()
