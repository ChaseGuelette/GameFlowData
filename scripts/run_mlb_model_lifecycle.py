#!/usr/bin/env python3
"""Run a profile-driven MLB model lifecycle from one YAML file."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.models.mlb.lifecycle.runner import STAGES, LifecycleRunner  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train, quote-clean backtest, CLV-audit, rank, and classify an MLB model."
    )
    parser.add_argument("--config", required=True, help="Lifecycle YAML file")
    parser.add_argument("--dry-run", action="store_true", help="Resolve and print commands without executing stages")
    parser.add_argument("--status", action="store_true", help="Read existing stage status without executing")
    parser.add_argument("--from-stage", choices=STAGES, default=None)
    parser.add_argument("--force-stage", choices=STAGES, default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    runner = LifecycleRunner(
        args.config,
        dry_run=args.dry_run,
        from_stage=args.from_stage,
        force_stage=args.force_stage,
    )
    result = runner.status() if args.status else runner.run()
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
