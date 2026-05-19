#!/usr/bin/env python3
"""
MLB Lines Job - Player Props & Game Lines Scraping
===================================================
Run multiple times daily before MLB games start.

This job:
1. Scrapes MLB game lines (moneyline, spreads, totals) from Odds API
2. Scrapes MLB player props from Odds API
3. Runs incremental linker to match new props to games/players

Mirrors src/orchestration/lines_job.py (NBA) with MLB-specific scrapers and linker.

Usage:
    python src/orchestration/mlb_lines_job.py --live
    python src/orchestration/mlb_lines_job.py --live --props-only
    python src/orchestration/mlb_lines_job.py --live --parallel
    python src/orchestration/mlb_lines_job.py --dry-run

Examples:
    # Full live scrape (game lines + props + linker)
    python src/orchestration/mlb_lines_job.py --live

    # Props only (fast — props + linker, skip game lines)
    python src/orchestration/mlb_lines_job.py --live --props-only

    # Full live scrape with parallel execution
    python src/orchestration/mlb_lines_job.py --live --parallel

    # Dry run
    python src/orchestration/mlb_lines_job.py --live --dry-run
"""

import argparse
import logging
import shlex
import subprocess
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Configure logging
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "mlb_lines.log"),
    ],
)
logger = logging.getLogger("MLBLinesJob")

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_command(command: str, description: str, dry_run: bool = False) -> bool:
    """Run a shell command and return success status."""
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}STARTING: {description}")

    if dry_run:
        logger.info(f"  Command: {command}")
        return True

    start_time = time.time()
    try:
        cmd_args = shlex.split(command)
        result = subprocess.run(
            cmd_args,
            check=True,
            capture_output=True,
            text=True,
            shell=False,
            cwd=PROJECT_ROOT,
        )
        elapsed = time.time() - start_time
        logger.info(f"COMPLETED: {description} ({elapsed:.1f}s)")
        if result.stdout:
            logger.debug(f"  Output: ...{result.stdout[-200:]}")
        return True
    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        logger.error(f"FAILED: {description} ({elapsed:.1f}s)")
        logger.error(f"  Exit code: {e.returncode}")
        logger.error(f"  Stderr: {e.stderr[:500] if e.stderr else 'No error output'}")
        if e.stdout:
            logger.error(f"  Stdout: {e.stdout[:500]}")
        return False
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"FAILED: {description} ({elapsed:.1f}s)")
        logger.error(f"  Exception: {e}")
        return False


def run_step_group(steps: list[tuple[str, str]], dry_run: bool = False) -> bool:
    """Run a list of (command, description) steps serially."""
    success = True
    for command, description in steps:
        if not run_command(command, description, dry_run):
            success = False
            logger.error(f"Job failed at step: {description}")
    return success


def run_parallel_groups(groups: list[list[tuple[str, str]]], dry_run: bool = False) -> bool:
    """Run step groups concurrently via threads."""
    results = [None] * len(groups)

    def _worker(idx, steps):
        results[idx] = run_step_group(steps, dry_run)

    threads = []
    for i, group in enumerate(groups):
        t = threading.Thread(target=_worker, args=(i, group), name=f"mlb-group-{i}")
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return all(results)


def get_table_max_id(table_name: str, dry_run: bool = False) -> int:
    """Return current MAX(id) for a table, used to bound dense-linker passes."""
    if dry_run:
        return 0
    from sqlalchemy import text

    from src.db.client import get_engine

    with get_engine().connect() as conn:
        return int(conn.execute(text(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}")).scalar() or 0)  # nosec


def run_dense_clv_linkers(start_id: int, dry_run: bool = False) -> bool:
    """Link newly inserted dense CLV rows only, using bounded id windows."""
    steps = [
        (
            f"{sys.executable} scripts/link_mlb_clv_snapshots.py --execute --max-batches 20 --batch-size 10000 --start-id {start_id} --only-games --second-pass-games --skip-report",
            "Dense CLV game linker for new rows",
        ),
        (
            f"{sys.executable} scripts/link_mlb_clv_snapshots.py --execute --max-batches 20 --batch-size 10000 --start-id {start_id} --only-players --skip-report",
            "Dense CLV player linker for new rows",
        ),
    ]
    return run_step_group(steps, dry_run)


def main():
    parser = argparse.ArgumentParser(
        description="MLB Lines Job - Player Props & Game Lines Scraping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date", type=str, default=str(date.today()),
        help="Target date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be executed without running")
    parser.add_argument("--live", action="store_true", help="Use live API endpoints")
    parser.add_argument("--props-only", action="store_true", help="Only scrape props + run linker (skip game lines)")
    parser.add_argument("--parallel", action="store_true", help="Run game lines and props concurrently")
    parser.add_argument("--skip-linker", action="store_true", help="Skip incremental linker")
    parser.add_argument("--extended", action="store_true", help="Include extended prop markets")
    parser.add_argument("--pregame-minutes", type=int, default=None, help="Live props only: scrape games about N minutes before commence_time")
    parser.add_argument("--pregame-tolerance-minutes", type=int, default=5, help="Tolerance around --pregame-minutes")
    parser.add_argument(
        "--dense-clv-close",
        action="store_true",
        help="Live props only: write pregame close snapshots to mlb_player_props_clv_snapshots and link new rows.",
    )
    args = parser.parse_args()

    python = sys.executable
    start_time = time.time()

    logger.info("=" * 60)
    logger.info(f"MLB LINES JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Mode: {'live' if args.live else 'historical'} | Props-only: {args.props_only} | Parallel: {args.parallel}")
    logger.info("=" * 60)

    if args.live:
        dense_start_id = 0
        if args.dense_clv_close:
            dense_start_id = get_table_max_id("mlb_player_props_clv_snapshots", args.dry_run)
            logger.info("Dense CLV close mode: existing max id=%d", dense_start_id)

        # Build props command
        props_cmd = f"{python} -m src.scrapers.mlb.mlb_daily_player_props_scraper --live"
        if args.extended:
            props_cmd += " --extended"
        if args.pregame_minutes is not None:
            props_cmd += f" --pregame-minutes {args.pregame_minutes} --pregame-tolerance-minutes {args.pregame_tolerance_minutes}"
        if args.dense_clv_close:
            props_cmd += " --target-table mlb_player_props_clv_snapshots"

        # Game lines command
        game_lines_cmd = f"{python} -m src.scrapers.mlb.mlb_daily_game_lines_scraper --live"

        # Linker command
        linker_cmd = f"{python} -m src.processing.mlb.mlb_linker incremental"

        if args.props_only:
            # Props + linker only
            steps = [
                (props_cmd, "MLB player props scrape (live)"),
            ]
            run_step_group(steps, args.dry_run)
            if args.dense_clv_close and not args.skip_linker:
                run_dense_clv_linkers(dense_start_id, args.dry_run)
            elif not args.skip_linker:
                run_command(linker_cmd, "MLB incremental linker", args.dry_run)

        elif args.parallel:
            # Game lines and props run concurrently, then linker
            group_lines = [(game_lines_cmd, "MLB game lines scrape (live)")]
            group_props = [(props_cmd, "MLB player props scrape (live)")]

            logger.info("Running game lines and props in parallel...")
            run_parallel_groups([group_lines, group_props], args.dry_run)

            # Linker after both complete
            if args.dense_clv_close and not args.skip_linker:
                run_dense_clv_linkers(dense_start_id, args.dry_run)
            elif not args.skip_linker:
                run_command(linker_cmd, "MLB incremental linker", args.dry_run)
        else:
            # Sequential: game lines -> props -> linker
            steps = [
                (game_lines_cmd, "MLB game lines scrape (live)"),
                (props_cmd, "MLB player props scrape (live)"),
            ]
            run_step_group(steps, args.dry_run)
            if args.dense_clv_close and not args.skip_linker:
                run_dense_clv_linkers(dense_start_id, args.dry_run)
            elif not args.skip_linker:
                run_command(linker_cmd, "MLB incremental linker", args.dry_run)
    else:
        # Historical mode
        target_date = args.date
        steps = [
            (f"{python} -m src.scrapers.mlb.mlb_daily_game_lines_scraper --date {target_date}",
             f"MLB game lines scrape ({target_date})"),
            (f"{python} -m src.scrapers.mlb.mlb_daily_player_props_scraper --date {target_date}",
             f"MLB player props scrape ({target_date})"),
        ]
        if not args.skip_linker:
            steps.append((
                f"{python} -m src.processing.mlb.mlb_linker incremental",
                "MLB incremental linker",
            ))
        run_step_group(steps, args.dry_run)

    elapsed = time.time() - start_time
    logger.info(f"MLB LINES JOB COMPLETE: {elapsed:.1f}s total")


if __name__ == "__main__":
    main()
