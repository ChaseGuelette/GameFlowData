#!/usr/bin/env python3
"""Dry-run or persist bounded NBA injury game-start reconciliation."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.processing.nba_injury_game_timing import (  # noqa: E402
    MAX_GAME_IDS,
    GameTimingRequest,
    normalize_requests,
    persist_game_timings,
    resolve_game_timings,
    select_injury_linked_games,
)


def _date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; expected YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; expected YYYY-MM-DD")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile authoritative NBA game starts for injury cutoffs (dry-run by default)"
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--game-id", action="append", help="Exact competitive NBA game ID; repeatable")
    selection.add_argument("--start-date", type=_date, help="First injury-linked game date, inclusive")
    parser.add_argument("--game-date", action="append", type=_date, help="Date matching each --game-id")
    parser.add_argument("--end-date", type=_date, help="Last injury-linked game date, inclusive")
    parser.add_argument("--max-games", type=int, default=MAX_GAME_IDS, help=f"Range cap (1-{MAX_GAME_IDS})")
    parser.add_argument("--execute", action="store_true", help="Persist changes; default is read-only dry-run")
    parser.add_argument("--local", action="store_true", help="Use get_engine(local=True)")
    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if not 1 <= args.max_games <= MAX_GAME_IDS:
        parser.error(f"--max-games must be between 1 and {MAX_GAME_IDS}")
    if args.game_id:
        if args.end_date is not None:
            parser.error("--end-date cannot be used with --game-id")
        if not args.game_date or len(args.game_date) != len(args.game_id):
            parser.error("repeat --game-date exactly once for every --game-id")
        if len(args.game_id) > MAX_GAME_IDS:
            parser.error(f"at most {MAX_GAME_IDS} --game-id values are allowed")
    else:
        if args.game_date:
            parser.error("--game-date requires --game-id")
        if args.end_date is None:
            parser.error("--end-date is required with --start-date")
        if args.start_date > args.end_date:
            parser.error("--start-date must be on or before --end-date")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_args(parser, args)

    requests = None
    if args.game_id:
        try:
            requests = normalize_requests(
                GameTimingRequest(game_id, game_date)
                for game_id, game_date in zip(args.game_id, args.game_date, strict=True)
            )
        except ValueError as exc:
            parser.error(str(exc))

    from src.db.client import get_engine

    engine = get_engine(local=args.local)
    if requests is None:
        requests = select_injury_linked_games(
            engine, args.start_date, args.end_date, max_games=args.max_games
        )

    receipts = resolve_game_timings(engine, requests)
    changed = persist_game_timings(engine, receipts) if args.execute else 0
    counts = {
        "requested": len(receipts),
        "resolved": sum(receipt.status == "resolved" for receipt in receipts),
        "unknown": sum(receipt.status == "unknown" for receipt in receipts),
        "changed": changed,
    }
    output = {
        "mode": "execute" if args.execute else "dry-run",
        "route": "local" if args.local else "remote",
        "counts": counts,
        "games": [receipt.as_json() for receipt in receipts],
    }
    print(json.dumps(output, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
