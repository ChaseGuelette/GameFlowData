#!/usr/bin/env python3
"""Prune expired NBA inference quotes after compact T-60 materialization."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("PruneNBAInferenceProps")

DEFAULT_RETENTION_DAYS = 21
DEFAULT_BATCH_SIZE = 25_000
MAX_BATCH_SIZE = 100_000
DEFAULT_MAX_ROWS = 500_000
DEFAULT_MAX_RUNTIME_SECONDS = 1_500
ADVISORY_LOCK_KEY = 7_114_220_261_121
CORE_MARKETS = ("player_points", "player_rebounds", "player_assists", "player_threes")

PRUNE_SQL = """
WITH candidates AS MATERIALIZED (
    SELECT p.staging_id
    FROM public.nba_player_props_inference AS p
    WHERE p.commence_time < now() - make_interval(days => :retention_days)
      AND (
          p.market_key <> ALL(CAST(:core_markets AS text[]))
          OR EXISTS (
              SELECT 1
              FROM public.nba_player_prop_feature_lines AS f
              WHERE f.game_id = p.game_id
                AND f.player_id = p.player_id
                AND f.market_key = p.market_key
                AND f.commence_time = p.commence_time
                AND f.selection_policy_version = 't60_v1'
          )
      )
    ORDER BY p.commence_time, p.staging_id
    LIMIT :batch_size
    FOR UPDATE OF p SKIP LOCKED
), deleted AS (
    DELETE FROM public.nba_player_props_inference AS p
    USING candidates AS c
    WHERE p.staging_id = c.staging_id
    RETURNING p.market_key
)
SELECT
    count(*)::bigint AS deleted,
    count(*) FILTER (WHERE market_key = ANY(CAST(:core_markets AS text[])))::bigint AS deleted_core,
    count(*) FILTER (WHERE market_key <> ALL(CAST(:core_markets AS text[])))::bigint AS deleted_non_core
FROM deleted
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    parser.add_argument("--max-runtime-seconds", type=int, default=DEFAULT_MAX_RUNTIME_SECONDS)
    parser.add_argument("--statement-timeout-ms", type=int, default=120_000)
    parser.add_argument("--lock-timeout-ms", type=int, default=5_000)
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.retention_days < 1:
        parser.error("--retention-days must be positive")
    if not 1 <= args.batch_size <= MAX_BATCH_SIZE:
        parser.error(f"--batch-size must be between 1 and {MAX_BATCH_SIZE}")
    if args.max_rows < 1 or args.max_runtime_seconds < 1:
        parser.error("--max-rows and --max-runtime-seconds must be positive")
    if args.statement_timeout_ms < 1 or args.lock_timeout_ms < 1:
        parser.error("timeouts must be positive")


def prune_batch(engine, args: argparse.Namespace, batch_size: int) -> tuple[int, int, int, str, str]:
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(
            text("SELECT set_config('statement_timeout', :timeout, true)"),
            {"timeout": f"{args.statement_timeout_ms}ms"},
        )
        conn.execute(
            text("SELECT set_config('lock_timeout', :timeout, true)"),
            {"timeout": f"{args.lock_timeout_ms}ms"},
        )
        statement_timeout = conn.execute(text("SHOW statement_timeout")).scalar_one()
        lock_timeout = conn.execute(text("SHOW lock_timeout")).scalar_one()
        row = conn.execute(
            text(PRUNE_SQL),
            {
                "retention_days": args.retention_days,
                "batch_size": batch_size,
                "core_markets": list(CORE_MARKETS),
            },
        ).one()
    return int(row.deleted), int(row.deleted_core), int(row.deleted_non_core), statement_timeout, lock_timeout


def run(args: argparse.Namespace) -> int:
    from sqlalchemy import text

    from src.db.client import get_engine

    engine = get_engine()
    lock_conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    acquired = False
    started = time.monotonic()
    totals = {"deleted": 0, "deleted_core": 0, "deleted_non_core": 0, "batches": 0}
    try:
        acquired = bool(lock_conn.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": ADVISORY_LOCK_KEY}).scalar_one())
        if not acquired:
            logger.warning("Another inference-pruning run holds the advisory lock; exiting without changes")
            print("Pruned 0 rows")
            print("Step 0/0")
            return 0

        while totals["deleted"] < args.max_rows:
            elapsed = time.monotonic() - started
            if elapsed >= args.max_runtime_seconds:
                logger.info("Maximum runtime reached before starting another batch")
                break
            batch_limit = min(args.batch_size, args.max_rows - totals["deleted"])
            deleted, core, non_core, statement_timeout, lock_timeout = prune_batch(engine, args, batch_limit)
            totals["batches"] += 1
            totals["deleted"] += deleted
            totals["deleted_core"] += core
            totals["deleted_non_core"] += non_core
            logger.info(
                "Batch %d: deleted=%d core_materialized=%d non_core=%d total=%d statement_timeout=%s lock_timeout=%s",
                totals["batches"], deleted, core, non_core, totals["deleted"], statement_timeout, lock_timeout,
            )
            if deleted < batch_limit:
                break

        duration = time.monotonic() - started
        logger.info(
            "Prune complete: deleted=%d core_materialized=%d non_core=%d batches=%d duration=%.1fs",
            totals["deleted"], totals["deleted_core"], totals["deleted_non_core"], totals["batches"], duration,
        )
        print(f"Pruned {totals['deleted']} rows")
        print(f"Core materialized pruned: {totals['deleted_core']}")
        print(f"Non-core pruned: {totals['deleted_non_core']}")
        print(f"Step {totals['batches']}/{totals['batches']}")
        return 0
    finally:
        if acquired:
            lock_conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": ADVISORY_LOCK_KEY})
        lock_conn.close()
        engine.dispose()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)
    logger.info(
        "NBA inference pruning: retention=%d days batch=%d max_rows=%d max_runtime=%ds",
        args.retention_days, args.batch_size, args.max_rows, args.max_runtime_seconds,
    )
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
