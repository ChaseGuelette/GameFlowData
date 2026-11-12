#!/usr/bin/env python
"""Public CLI for the bounded NBA sampler comparison."""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtesting.nba.run_nba_sweep import StrictParser  # noqa: E402
from src.backtesting.nba.sampler_comparison import (  # noqa: E402
    ComparisonFailure,
    make_request,
    prepare_comparison,
    run_comparison,
)
from src.backtesting.nba.sweep_config import BOOKMAKERS  # noqa: E402
from src.models.features.nba.acquisition import add_acquisition_arguments, limits_from_values  # noqa: E402


def _date(token: str) -> date:
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", token) is None:
        raise argparse.ArgumentTypeError("Dates must be exact YYYY-MM-DD values")
    try:
        return date.fromisoformat(token)
    except ValueError:
        raise argparse.ArgumentTypeError("Dates must be exact YYYY-MM-DD values") from None


def _integer(token: str) -> int:
    if re.fullmatch(r"0|[1-9][0-9]*", token) is None:
        raise argparse.ArgumentTypeError("Expected a nonnegative decimal integer")
    return int(token)


def _number(token: str) -> float:
    try:
        if token != token.strip():
            raise ValueError
        value = float(token)
        if not math_isfinite(value):
            raise ValueError
        return value
    except ValueError:
        raise argparse.ArgumentTypeError("Expected a finite literal real number") from None


def math_isfinite(value: float) -> bool:
    return value == value and value not in (float("inf"), float("-inf"))


def build_parser() -> argparse.ArgumentParser:
    parser = StrictParser(
        description="Bounded local NBA sampler comparison; canonical acquisition, frozen component forecasts",
        allow_abbrev=False,
    )
    parser.add_argument("--model-dir", required=True)
    parser.add_argument("--start", type=_date, required=True)
    parser.add_argument("--end", type=_date, required=True)
    parser.add_argument("--stats", nargs="+", required=True)
    parser.add_argument(
        "--bookmakers",
        nargs="+",
        default=BOOKMAKERS,
        help="Current canonical 23-book executable allowlist by default (fliff is intentionally excluded)",
    )
    parser.add_argument("--seeds", nargs="+", type=_integer, required=True)
    parser.add_argument("--n-samples", type=_integer, required=True)
    parser.add_argument("--min-minutes-avg", type=_number, required=True)
    parser.add_argument("--starting-bankroll", type=_number, required=True)
    parser.add_argument("--flat-bet-size", type=_number, required=True)
    parser.add_argument("--allowed-bets", nargs="+", help="Exact stat:over/stat:under tokens")
    parser.add_argument("--edge-threshold", type=_number, required=True)
    parser.add_argument("--bl-tau", type=_number, required=True)
    parser.add_argument("--bl-z-max", type=_number, required=True)
    parser.add_argument("--bl-max-weight", type=_number, required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--diagnostic-history", action="store_true")
    parser.add_argument("--historical-policy")
    parser.add_argument("--historical-source-metadata")
    add_acquisition_arguments(parser)
    parser.add_argument("--max-days", type=_integer, default=31)
    parser.add_argument("--max-seeds", type=_integer, default=5)
    parser.add_argument("--max-samples", type=_integer, default=20_000)
    parser.add_argument("--max-player-games", type=_integer, default=10_000)
    parser.add_argument("--max-quote-rows", type=_integer, default=250_000)
    parser.add_argument("--max-total-samples", type=_integer, default=3_000_000_000)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def request_from_args(args: argparse.Namespace):
    allowed = None
    if args.allowed_bets is not None:
        allowed = tuple(tuple(token.split(":")) for token in args.allowed_bets)
    return make_request(
        model_dir=args.model_dir,
        start=args.start,
        end=args.end,
        stats=tuple(args.stats),
        bookmakers=tuple(args.bookmakers),
        seeds=tuple(args.seeds),
        n_samples=args.n_samples,
        min_minutes_avg=args.min_minutes_avg,
        starting_bankroll=args.starting_bankroll,
        flat_bet_size=args.flat_bet_size,
        edge_threshold=args.edge_threshold,
        bl_tau=args.bl_tau,
        bl_z_max=args.bl_z_max,
        bl_max_weight=args.bl_max_weight,
        output_dir=args.output_dir,
        local=args.local,
        diagnostic_history=args.diagnostic_history,
        allowed_bets=allowed,
        historical_policy=args.historical_policy,
        historical_source_metadata=args.historical_source_metadata,
        acquisition_limits=limits_from_values(vars(args), required=args.historical_policy is not None),
        max_days=args.max_days,
        max_seeds=args.max_seeds,
        max_samples=args.max_samples,
        max_player_games=args.max_player_games,
        max_quote_rows=args.max_quote_rows,
        max_total_samples=args.max_total_samples,
    )


def main(argv: list[str] | None = None, *, runtime_factory: Callable | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        request = request_from_args(args)
        prepared = prepare_comparison(request)
    except (ValueError, OSError, ImportError) as exc:
        parser.error(str(exc))
    if args.dry_run:
        print(json.dumps(prepared, indent=2, sort_keys=True, allow_nan=False))
        return 0
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logging.getLogger().setLevel(logging.INFO)
    try:
        output = run_comparison(request, runtime_factory=runtime_factory, plan=prepared["canonical_plan"])
    except (ComparisonFailure, OSError, ValueError) as exc:
        print(str(exc) if isinstance(exc, ComparisonFailure) else f"Sampler comparison failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps({"output_dir": str(output), "state": "complete", "decision_grade": False}, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
