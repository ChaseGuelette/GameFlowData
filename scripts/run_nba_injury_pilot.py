"""Diagnostic evidence utility only; not the production injury ingestion job.

Use run_nba_injury_recovery.py or rapidapi_injury_backfill.py for DB ingestion.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.scrapers.nba_injury_pilot import audit, capture, replay, request_plan  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode")
    for mode in ("plan", "capture"):
        command = modes.add_parser(mode)
        command.add_argument("--request-manifest", required=True)
        command.add_argument("--max-requests", type=int, default=3)
        if mode == "capture":
            command.add_argument("--output-dir", required=True)
            command.add_argument("--execute", action="store_true", help="Make the explicitly listed API requests")
    for mode in ("replay", "audit"):
        command = modes.add_parser(mode)
        command.add_argument("--input-manifest", action="append", required=True, help="Repeat to combine captures")
        command.add_argument("--output-dir", required=True, help="New directory; existing bundles are not overwritten")
        if mode == "replay":
            command.add_argument("--target-dates", nargs="+")
        else:
            command.add_argument("--target-manifest", required=True)
    args = parser.parse_args(argv)
    if not args.mode:
        parser.print_help()
        return 0
    try:
        if args.mode == "plan" or (args.mode == "capture" and not args.execute):
            result = request_plan(args.request_manifest, args.max_requests)
            result["dry_run"] = True
        elif args.mode == "capture":
            result = capture(args.request_manifest, args.output_dir, args.max_requests)
        elif args.mode == "replay":
            result, _ = replay(args.input_manifest, args.output_dir, args.target_dates)
        else:
            result = audit(args.input_manifest, args.target_manifest, args.output_dir)
        print(json.dumps(result, indent=2))
        return 2 if result.get("audit_status") == "SOURCE_BLOCKED" else 0
    except (ValueError, OSError, KeyError, TypeError, RuntimeError) as exc:
        print(f"Pilot failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
