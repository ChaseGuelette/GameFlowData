"""NBA lifecycle CLI. Help and status never import runtime model/DB modules."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import NoReturn

# Support the documented direct script invocation without installing the repo.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models.nba.lifecycle import STAGES  # noqa: E402


class LifecycleParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        self.exit(2, json.dumps({"state": "invalid_request", "reason": "invalid_cli_arguments"}, sort_keys=True) + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = LifecycleParser(description="Report-only NBA lifecycle; no historical or live authorization.", allow_abbrev=False)
    parser.add_argument("--config", help="Strict lifecycle YAML; relative paths resolve against repository root")
    parser.add_argument("--dry-run", action="store_true", help="Write only isolated planning files; execute no children")
    parser.add_argument("--from-stage", choices=STAGES, help="Resume only after verifying every predecessor")
    parser.add_argument("--force-stage", choices=STAGES, help="Invalidate selected/downstream state; preserve child evidence")
    parser.add_argument("--status", action="store_true", help="Read only recorded lifecycle-owned files, not live evidence")
    parser.add_argument("--run-root", help="Existing lifecycle directory; allowed only with --status")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    tokens = list(sys.argv[1:] if argv is None else argv)
    options = [token.split("=", 1)[0] for token in tokens if token.startswith("--")]
    if len(options) != len(set(options)):
        parser.error("duplicate options")
    args = parser.parse_args(tokens)
    if args.status:
        if not args.run_root or args.config or args.dry_run or args.from_stage or args.force_stage:
            parser.error("status mode conflicts")
        from src.models.nba.lifecycle.runner import status

        result, code = status(args.run_root)
    else:
        if not args.config or args.run_root or args.dry_run and (args.from_stage or args.force_stage):
            parser.error("configuration mode conflicts")
        from src.models.nba.lifecycle.config import load_config

        try:
            resolved = load_config(args.config)
        except Exception:
            print(json.dumps({"state": "invalid_config", "reason": "configuration_validation_failed"}, sort_keys=True))
            return 2
        from src.models.nba.lifecycle.runner import LifecycleError, run_lifecycle

        try:
            result, code = run_lifecycle(resolved, dry_run_only=args.dry_run, from_stage=args.from_stage,
                                         force_stage=args.force_stage)
        except LifecycleError as exc:
            result = {"state": "blocked" if exc.status in ("blocked", "unsupported") else "failed", "reason": exc.reason}
            code = 2 if exc.status in ("blocked", "unsupported") else 1
        except Exception:
            result, code = {"state": "failed", "reason": "lifecycle_execution_failed"}, 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
