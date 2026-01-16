"""
Fix wrong game_ids in raw_player_props_combined.

The original linker sometimes matched props to the wrong game (same date,
different opponent). This script re-matches using home_team + away_team +
fuzzy date (±3 days) to find the correct game_id.

Run: python fix_wrong_game_ids.py
"""

from sqlalchemy import text
from tqdm import tqdm

from src.db.client import get_engine

BATCH_SIZE = 1000


def main():
    engine = get_engine()

    print("Finding props with wrong game_ids...")

    # Find distinct wrong game_id + correct game_id mappings
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            WITH unmatched_props AS (
                SELECT DISTINCT
                    p.game_id as wrong_game_id,
                    p.home_team,
                    p.away_team,
                    DATE(p.commence_time) as props_date
                FROM raw_player_props_combined p
                WHERE p.team_id IS NULL AND p.player_id IS NOT NULL
            ),
            correct_games AS (
                SELECT
                    t_home.game_id,
                    t_home.team_name as home_team,
                    t_home.game_date,
                    t_away.team_name as away_team
                FROM team_game_stats t_home
                JOIN team_game_stats t_away ON t_home.game_id = t_away.game_id
                    AND t_away.team_matchup LIKE '%@%'
                WHERE t_home.team_matchup LIKE '%vs.%'
            ),
            matches AS (
                SELECT DISTINCT ON (u.wrong_game_id, u.home_team, u.away_team)
                    u.wrong_game_id,
                    u.home_team,
                    u.away_team,
                    u.props_date,
                    c.game_id as correct_game_id,
                    c.game_date as correct_date,
                    ABS(u.props_date - c.game_date) as date_diff
                FROM unmatched_props u
                JOIN correct_games c
                    ON u.home_team = c.home_team
                    AND u.away_team = c.away_team
                    AND c.game_date BETWEEN u.props_date - INTERVAL '3 days' AND u.props_date + INTERVAL '3 days'
                WHERE u.wrong_game_id != c.game_id  -- Only where game_id is actually wrong
                ORDER BY u.wrong_game_id, u.home_team, u.away_team, ABS(u.props_date - c.game_date)
            )
            SELECT wrong_game_id, correct_game_id, home_team, away_team, date_diff
            FROM matches
        """)
        )

        mappings = result.fetchall()

    print(f"Found {len(mappings)} wrong game_id mappings to fix")

    if not mappings:
        print("Nothing to fix!")
        return

    # Show sample mappings
    print("\nSample mappings:")
    for m in mappings[:5]:
        print(
            f"  {m.wrong_game_id} -> {m.correct_game_id} ({m.home_team} vs {m.away_team}, {m.date_diff} days off)"
        )

    # Apply fixes
    fixed_rows = 0
    for mapping in tqdm(mappings, desc="Fixing game_ids"):
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                UPDATE raw_player_props_combined
                SET game_id = :correct_game_id
                WHERE game_id = :wrong_game_id
                  AND home_team = :home_team
                  AND away_team = :away_team
                  AND team_id IS NULL
            """),
                {
                    "correct_game_id": mapping.correct_game_id,
                    "wrong_game_id": mapping.wrong_game_id,
                    "home_team": mapping.home_team,
                    "away_team": mapping.away_team,
                },
            )
            fixed_rows += result.rowcount

    print(f"\nDone! Fixed {fixed_rows:,} rows with corrected game_ids.")
    print("\nNow re-run backfill_team_ids.py to fill in the team_ids.")


if __name__ == "__main__":
    main()
