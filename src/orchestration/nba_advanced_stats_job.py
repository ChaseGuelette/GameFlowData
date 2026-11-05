#!/usr/bin/env python3
"""Local-only NBA advanced source producer and strict derived-chain owner."""

from __future__ import annotations

import argparse
import os
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local NBA advanced source and strict derived chain")
    parser.add_argument("--dry-run", action="store_true", help="Print the ordered plan without DB/network/subprocess writes")
    parser.add_argument("--advanced-limit", type=int, default=120)
    parser.add_argument("--request-timeout", type=int, default=30)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--http-client", choices=("curl", "requests"), default="curl")
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
    return args


def child_commands(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    python = sys.executable
    proxy_flag = "--use-proxy" if args.use_proxy else "--no-proxy"
    return [
        (
            "CDN basic source ingestion",
            [python, "src/scrapers/nba_unified_scraper.py", "--cdn-only"],
        ),
        (
            "bounded local advanced source ingestion",
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


def main() -> None:
    args = parse_args()
    started_at = datetime.now(UTC)
    started = time.time()

    commands = child_commands(args)
    if args.dry_run:
        print("DRY RUN: no DB, network, subprocess, or job-history writes")
        for index, (description, command) in enumerate(commands, start=1):
            print(f"{index}. {description}: {subprocess.list2cmdline(command)}")
        print("4. verify latest completed source target has zero missing raw advanced pairs")
        print("5. only then record success with metrics.through_date")
        return

    if any(os.getenv(name) for name in RAILWAY_ENV_MARKERS):
        message = "local advanced producer is forbidden on Railway"
        record_job_execution(JOB_NAME, started_at, "failed", time.time() - started, message)
        raise SystemExit(message)

    stage = "startup"
    try:
        for stage, command in commands:
            print(f"STARTING: {stage}", flush=True)
            subprocess.run(command, cwd=PROJECT_ROOT, check=True, env={**os.environ, "PYTHONUTF8": "1"})
            print(f"COMPLETED: {stage}", flush=True)

        stage = "post-run advanced coverage verification"
        from src.db.client import get_engine

        season_id = season_label_to_id(current_nba_season_label())
        engine = get_engine()
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
        }
        record_job_execution(JOB_NAME, started_at, "success", duration, metrics=metrics)
        print(f"NBA ADVANCED PIPELINE VERIFIED THROUGH {through_date}", flush=True)
    except Exception as exc:
        duration = time.time() - started
        message = f"{stage} failed: {type(exc).__name__}: {exc}"
        record_job_execution(JOB_NAME, started_at, "failed", duration, error_message=message)
        print(message, file=sys.stderr, flush=True)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
