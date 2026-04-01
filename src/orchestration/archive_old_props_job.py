#!/usr/bin/env python3
"""
Archive Old Props Job
=====================
Moves rows older than 7 days from raw_player_props_combined
to raw_player_props_archive in batches.

Runs daily at 3 AM ET via the scheduler. Each run moves up to
MAX_ROWS_PER_RUN rows to keep execution time bounded.

Usage:
    python src/orchestration/archive_old_props_job.py
    python src/orchestration/archive_old_props_job.py --batch-size 100000
    python src/orchestration/archive_old_props_job.py --retention-days 60
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ArchiveOldPropsJob")

BATCH_SIZE = 50_000
MAX_ROWS_PER_RUN = 500_000
DEFAULT_RETENTION_DAYS = 7


def archive_batch(engine, retention_days: int, batch_size: int) -> int:
    """Move one batch of old rows to archive. Returns number of rows moved."""
    from sqlalchemy import text

    with engine.begin() as conn:
        result = conn.execute(
            text("""
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
            """),
            {"retention_days": retention_days, "batch_size": batch_size},
        )
        return result.rowcount


def main():
    parser = argparse.ArgumentParser(description="Archive old player props rows")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Rows per batch (default: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=MAX_ROWS_PER_RUN,
        help=f"Max rows to move per run (default: {MAX_ROWS_PER_RUN})",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Keep rows newer than N days (default: {DEFAULT_RETENTION_DAYS})",
    )
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("Archive Old Props Job")
    logger.info(f"  Retention: {args.retention_days} days")
    logger.info(f"  Batch size: {args.batch_size:,}")
    logger.info(f"  Max rows per run: {args.max_rows:,}")
    logger.info("=" * 50)

    start_time = time.time()

    from src.db.client import get_engine
    engine = get_engine()

    total_moved = 0
    batch_num = 0

    while total_moved < args.max_rows:
        batch_num += 1
        batch_start = time.time()
        moved = archive_batch(engine, args.retention_days, args.batch_size)
        batch_duration = time.time() - batch_start

        if moved == 0:
            logger.info(f"No more rows to archive (batch {batch_num})")
            break

        total_moved += moved
        logger.info(
            f"Batch {batch_num}: archived {moved:,} rows in {batch_duration:.1f}s "
            f"(total: {total_moved:,})"
        )

    duration = time.time() - start_time
    logger.info("-" * 50)
    logger.info(f"Archive complete: {total_moved:,} rows moved in {duration:.1f}s")
    logger.info("-" * 50)

    # Emit metrics for scheduler parsing
    print(f"Archived {total_moved} rows")
    print(f"Step {batch_num}/{batch_num}")

    sys.exit(0)


if __name__ == "__main__":
    main()
