"""
Backfill team_id in raw_player_props_combined.

Uses player's most recent team from player_game_stats and matches it to
the home/away team in team_game_stats for that game.

Run: python backfill_team_ids.py
"""

from sqlalchemy import text
from tqdm import tqdm
from src.db.client import get_engine

BATCH_SIZE = 5000

def main():
    engine = get_engine()

    print("Counting rows to backfill...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM raw_player_props_combined
            WHERE team_id IS NULL AND player_id IS NOT NULL
        """)).fetchone()
        total_to_fix = result[0]

    print(f"Found {total_to_fix:,} rows missing team_id (with player_id)")

    if total_to_fix == 0:
        print("Nothing to backfill!")
        return

    fixed = 0
    pbar = tqdm(total=total_to_fix, desc="Backfilling team_ids")

    while True:
        with engine.begin() as conn:
            # The backfill logic:
            # 1. Find player's most recent team from player_game_stats
            # 2. Get home/away team_ids from team_game_stats for the game
            # 3. If player's recent team matches home or away, use that team_id
            result = conn.execute(text("""
                WITH to_update AS (
                    SELECT p.staging_id
                    FROM raw_player_props_combined p
                    WHERE p.team_id IS NULL AND p.player_id IS NOT NULL
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
            """), {"batch_size": BATCH_SIZE})

            rows_updated = len(result.fetchall())

        if rows_updated == 0:
            # Check if there are still rows to process (might have NULL derived_team_id)
            with engine.connect() as conn:
                remaining = conn.execute(text("""
                    SELECT COUNT(*) FROM raw_player_props_combined
                    WHERE team_id IS NULL AND player_id IS NOT NULL
                """)).fetchone()[0]

            if remaining > 0:
                print(f"\n{remaining:,} rows could not be backfilled (player's team not in game)")
            break

        fixed += rows_updated
        pbar.update(rows_updated)

    pbar.close()
    print(f"\nDone! Backfilled {fixed:,} rows.")

    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(team_id) as has_team_id,
                COUNT(*) - COUNT(team_id) as missing_team_id
            FROM raw_player_props_combined
            WHERE player_id IS NOT NULL
        """)).fetchone()
        print(f"\nRows with player_id: {result[0]:,}")
        print(f"  Has team_id: {result[1]:,}")
        print(f"  Missing team_id: {result[2]:,}")

if __name__ == "__main__":
    main()
