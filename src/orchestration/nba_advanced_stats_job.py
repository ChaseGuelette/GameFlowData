#!/usr/bin/env python3
"""Bounded NBA advanced producer and strict derived-chain owner, local or opted-in cloud."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.orchestration.daily_stats_job import (  # noqa: E402
    count_missing_advanced_pairs,
    latest_completed_source_date,
)
from src.orchestration.scheduler import record_job_execution  # noqa: E402
from src.scrapers.nba_unified_scraper import (  # noqa: E402
    current_nba_season_label,
    season_label_to_id,
)

JOB_NAME = "nba_advanced_stats_job.py"
RAILWAY_ENV_MARKERS = ("RAILWAY_ENVIRONMENT", "RAILWAY_SERVICE_ID", "RAILWAY_PROJECT_ID")
ADVANCED_LOCK_KEY = 741926081


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run bounded NBA advanced source and strict derived chain")
    parser.add_argument("--dry-run", action="store_true", help="Print the ordered plan without DB/network/subprocess writes")
    parser.add_argument("--advanced-limit", type=int, default=120)
    parser.add_argument("--request-timeout", type=int, default=30)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--http-client", choices=("curl", "requests"), default="curl")
    parser.add_argument("--railway", action="store_true", help="Explicitly opt into the bounded Railway producer")
    parser.add_argument("--skip-basic", action="store_true", help="Skip initial CDN fetch after a successful basic job")
    parser.add_argument("--max-requests", type=int, help="Stats HTTP budget (default: game limit times attempt limit)")
    parser.add_argument("--source-timeout", type=int, default=3600, help="Maximum seconds per source child")
    parser.add_argument("--derived-timeout", type=int, default=2700, help="Maximum seconds for the derived chain")
    proxy = parser.add_mutually_exclusive_group()
    proxy.add_argument("--use-proxy", action="store_true")
    proxy.add_argument("--no-proxy", action="store_true")
    args = parser.parse_args()
    if args.advanced_limit <= 0:
        parser.error("--advanced-limit must be positive")
    if not 1 <= args.request_timeout <= 120:
        parser.error("--request-timeout must be 1..120")
    if not 1 <= args.max_retries <= 5:
        parser.error("--max-retries must be 1..5")
    if args.max_requests is None:
        args.max_requests = args.advanced_limit * args.max_retries
    if args.max_requests <= 0 or args.source_timeout <= 0 or args.derived_timeout <= 0:
        parser.error("request and runtime budgets must be positive")
    if args.railway and (
        args.advanced_limit > 30 or args.max_requests > 60 or args.max_retries > 2
        or args.request_timeout > 30 or args.source_timeout > 900 or args.derived_timeout > 1800
        or args.use_proxy
    ):
        parser.error("Railway caps: 30 games, 60 requests, 2 attempts, 30s/request, 900s source, 1800s derived, no proxy")
    return args


def child_commands(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    python = sys.executable
    proxy_flag = "--use-proxy" if args.use_proxy else "--no-proxy"
    commands = [
        (
            "CDN basic source ingestion",
            [python, "src/scrapers/nba_unified_scraper.py", "--cdn-only"],
        ),
        (
            "bounded advanced source ingestion",
            [
                python,
                "src/scrapers/nba_unified_scraper.py",
                "--season",
                "current",
                "--advanced-limit",
                str(args.advanced_limit),
                "--request-timeout",
                str(args.request_timeout),
                "--max-retries",
                str(args.max_retries),
                "--max-requests",
                str(args.max_requests),
                "--http-client",
                args.http_client,
                proxy_flag,
                "--skip-team",
                "--skip-traditional",
            ],
        ),
        (
            "strict full derived chain and completeness verification",
            [
                python,
                "src/orchestration/daily_stats_job.py",
                "--skip-resolution",
                "--skip-linker",
            ],
        ),
    ]
    return commands[1:] if args.skip_basic else commands


def run_child(command: list[str], timeout: int) -> None:
    """Bound a whole child tree, including the derived job's subprocesses."""
    process = subprocess.Popen(
        command, cwd=PROJECT_ROOT, env={**os.environ, "PYTHONUTF8": "1"},
        start_new_session=os.name != "nt",
    )
    try:
        returncode = process.wait(timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], check=False, timeout=15)
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.wait(timeout=15)
        raise
    if returncode:
        raise subprocess.CalledProcessError(returncode, command)


def main() -> None:
    args = parse_args()
    started_at = datetime.now(UTC)
    started = time.time()

    commands = child_commands(args)
    if args.dry_run:
        print("DRY RUN: no DB, network, subprocess, or job-history writes")
        for index, (description, command) in enumerate(commands, start=1):
            print(f"{index}. {description}: {subprocess.list2cmdline(command)}")
        print(f"Budgets: source={args.source_timeout}s, derived={args.derived_timeout}s, HTTP={args.max_requests}")
        print("Execution holds a shared local/cloud advisory lock; busy means no work")
        print("Verify latest completed source target has zero missing raw advanced pairs")
        print("Only then record success with metrics.through_date")
        return

    on_railway = any(os.getenv(name) for name in RAILWAY_ENV_MARKERS)
    railway_enabled = os.getenv("NBA_ADVANCED_STATS_ENABLED", "false").lower() in {"true", "1", "yes", "on"}
    if on_railway and not (args.railway and railway_enabled):
        message = "Railway advanced producer requires --railway and NBA_ADVANCED_STATS_ENABLED=true"
        record_job_execution(JOB_NAME, started_at, "failed", time.time() - started, message)
        raise SystemExit(message)

    stage = "startup"
    lock_connection = None
    lock_acquired = False
    try:
        from sqlalchemy import text

        from src.db.client import get_engine

        engine = get_engine()
        # Session-level lock survives commits without an idle transaction. Both
        # local and cloud wrappers use this key against the canonical database.
        lock_connection = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        lock_acquired = bool(lock_connection.execute(
            text("SELECT pg_try_advisory_lock(:key)"), {"key": ADVANCED_LOCK_KEY}
        ).scalar_one())
        if not lock_acquired:
            raise RuntimeError("another local/cloud advanced producer holds the shared lock; no work started")
        for stage, command in commands:
            print(f"STARTING: {stage}", flush=True)
            timeout = args.derived_timeout if "strict full derived" in stage else args.source_timeout
            run_child(command, timeout)
            print(f"COMPLETED: {stage}", flush=True)

        stage = "post-run advanced coverage verification"
        season_id = season_label_to_id(current_nba_season_label())
        through_date = latest_completed_source_date(engine, season_id)
        if through_date is None:
            raise RuntimeError("no completed NBA source target date exists after strict full chain")
        missing = count_missing_advanced_pairs(engine, season_id, through_date)
        if missing:
            raise RuntimeError(f"{missing} raw advanced player/team pairs remain through {through_date}")

        duration = time.time() - started
        metrics = {
            "through_date": through_date.isoformat(),
            "season_id": season_id,
            "missing_advanced_pairs": 0,
            "advanced_limit": args.advanced_limit,
            "max_requests": args.max_requests,
            "producer_host": "railway" if on_railway else "local",
        }
        record_job_execution(JOB_NAME, started_at, "success", duration, metrics=metrics)
        print(f"NBA ADVANCED PIPELINE VERIFIED THROUGH {through_date}", flush=True)
    except Exception as exc:
        duration = time.time() - started
        message = f"{stage} failed: {type(exc).__name__}: {exc}"
        record_job_execution(JOB_NAME, started_at, "failed", duration, error_message=message)
        print(message, file=sys.stderr, flush=True)
        raise SystemExit(1) from exc
    finally:
        if lock_connection is not None:
            try:
                if lock_acquired:
                    lock_connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": ADVANCED_LOCK_KEY})
            finally:
                lock_connection.close()


if __name__ == "__main__":
    main()
