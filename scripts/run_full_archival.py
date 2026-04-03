#!/usr/bin/env python3
"""
One-time script to archive all rows older than N days from
raw_player_props_combined into raw_player_props_archive.

Runs in a loop until no more rows to move. Prints progress every batch.
Automatically retries on connection errors with fresh connections.

Usage:
    python run_full_archival.py              # Archive rows older than 7 days
    python run_full_archival.py --days 14    # Archive rows older than 14 days
"""

import argparse
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

from sqlalchemy import create_engine, text  # noqa: E402

DATABASE_URL = os.environ["DATABASE_URL"]
BATCH_SIZE = 50_000
MAX_RETRIES = 5

ARCHIVE_SQL = text("""
    WITH to_archive AS (
        SELECT staging_id FROM raw_player_props_combined
        WHERE snapshot_time < now() - make_interval(days => :retention_days)
        ORDER BY staging_id
        LIMIT :batch_size
    ),
    moved AS (
        INSERT INTO raw_player_props_archive
        SELECT r.*, now() FROM raw_player_props_combined r
        JOIN to_archive t ON r.staging_id = t.staging_id
        RETURNING staging_id
    )
    DELETE FROM raw_player_props_combined
    WHERE staging_id IN (SELECT staging_id FROM moved)
""")

parser = argparse.ArgumentParser(description="Archive old player props rows")
parser.add_argument("--days", type=int, default=7, help="Retention window in days (default: 7)")
args = parser.parse_args()

retention_days = args.days
total_moved = 0
batch_num = 0
consecutive_errors = 0
start = time.time()

print(f"Starting full archival: rows older than {retention_days} days (batch_size={BATCH_SIZE:,})")
print(f"{'='*60}")

while True:
    batch_num += 1
    batch_start = time.time()

    # Create a fresh engine for each batch to avoid stale connections
    engine = create_engine(
        DATABASE_URL,
        connect_args={"connect_timeout": 30},
        pool_pre_ping=True,
        pool_recycle=300,
    )

    try:
        with engine.begin() as conn:
            conn.execute(text("SET statement_timeout = '120s'"))
            result = conn.execute(ARCHIVE_SQL, {"batch_size": BATCH_SIZE, "retention_days": retention_days})
            moved = result.rowcount
        consecutive_errors = 0
    except Exception as e:
        consecutive_errors += 1
        engine.dispose()
        if consecutive_errors >= MAX_RETRIES:
            print(f"\n{MAX_RETRIES} consecutive errors. Stopping.")
            print(f"Last error: {e}")
            break
        wait = min(30, 5 * consecutive_errors)
        print(f"  Error on batch {batch_num}: {type(e).__name__}. Retrying in {wait}s... ({consecutive_errors}/{MAX_RETRIES})")
        sys.stdout.flush()
        time.sleep(wait)
        continue
    finally:
        engine.dispose()

    batch_dur = time.time() - batch_start
    total_moved += moved
    elapsed = time.time() - start
    rate = total_moved / elapsed if elapsed > 0 else 0

    if moved == 0:
        print("\nDone! No more rows to archive.")
        break

    print(f"Batch {batch_num}: {moved:,} rows in {batch_dur:.1f}s | Total: {total_moved:,} | Rate: {rate:,.0f} rows/s | Elapsed: {elapsed:.0f}s")
    sys.stdout.flush()

elapsed = time.time() - start
print(f"{'='*60}")
print(f"Archival complete: {total_moved:,} rows moved in {elapsed:.0f}s ({elapsed/60:.1f} min)")
