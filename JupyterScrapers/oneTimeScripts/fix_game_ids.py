"""
Fix game_id leading zeros in raw_player_props_combined.

Run: python fix_game_ids.py

This script updates game_ids that are missing leading zeros (8 chars → 10 chars).
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tqdm import tqdm

BATCH_SIZE = 5000  # Rows per batch

def main():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in .env")

    engine = create_engine(database_url)

    print("Counting rows to fix...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM raw_player_props_combined WHERE LENGTH(game_id) = 8
        """)).fetchone()
        total_to_fix = result[0]

    print(f"Found {total_to_fix:,} rows with 8-char game_ids (need leading zeros)")

    if total_to_fix == 0:
        print("Nothing to fix!")
        return

    fixed = 0
    pbar = tqdm(total=total_to_fix, desc="Fixing game_ids")

    while True:
        with engine.begin() as conn:
            # Find and fix a batch
            result = conn.execute(text("""
                WITH to_update AS (
                    SELECT staging_id
                    FROM raw_player_props_combined
                    WHERE LENGTH(game_id) = 8
                    LIMIT :batch_size
                )
                UPDATE raw_player_props_combined p
                SET game_id = LPAD(p.game_id, 10, '0')
                FROM to_update t
                WHERE p.staging_id = t.staging_id
                RETURNING p.staging_id
            """), {"batch_size": BATCH_SIZE})

            rows_updated = result.rowcount

        if rows_updated == 0:
            break

        fixed += rows_updated
        pbar.update(rows_updated)

    pbar.close()
    print(f"\nDone! Fixed {fixed:,} rows.")

    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT LENGTH(game_id) as len, COUNT(*) as cnt
            FROM raw_player_props_combined
            GROUP BY LENGTH(game_id)
        """)).fetchall()
        print("\nGame ID lengths after fix:")
        for row in result:
            print(f"  {row[0]} chars: {row[1]:,} rows")

if __name__ == "__main__":
    main()
