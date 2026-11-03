"""Importable NBA sweep CLI. Invoke with python -m src.backtesting.nba.run_nba_sweep.

All fixed acquisition/stake controls are explicit. Help, invalid requests and
--dry-run never bootstrap runtime objects or create output directories.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date

from src.backtesting.nba.sweep_config import SweepConfig, build_plan
from src.models.features.nba.acquisition import add_acquisition_arguments, limits_from_values


class StrictParser(argparse.ArgumentParser):
    def parse_args(self, args=None, namespace=None):
        tokens = list(sys.argv[1:] if args is None else args)
        seen = set()
        for token in tokens:
            if token.startswith("--"):
                option = token.split("=", 1)[0]
                if option in seen:
                    self.error(f"Repeated option is not allowed: {option}")
                seen.add(option)
        return super().parse_args(tokens, namespace)


def _date(token: str) -> date:
    try:
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", token) is None:
            raise ValueError
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
        return float(token)
    except ValueError:
        raise argparse.ArgumentTypeError("Expected a literal real number") from None


def build_parser() -> argparse.ArgumentParser:
    parser = StrictParser(description="Diagnostic NBA shared-input sweep; flat stakes; no historical certification",
                          allow_abbrev=False)
    parser.add_argument("--model-dir", required=True, help="Exact manifested artifact directory; no discovery/legacy")
    parser.add_argument("--start", type=_date, required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--end", type=_date, required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--stats", nargs="+", required=True,
                        help="Ordered unique model components: pts reb ast; eligible combos are derived/quoted too")
    parser.add_argument("--bookmakers", nargs="+", required=True, help="Ordered unique exact bookmaker tokens")
    parser.add_argument("--n-samples", type=_integer, required=True)
    parser.add_argument("--seed", type=_integer, required=True, help="RandomState seed, 0..4294967295")
    parser.add_argument("--min-minutes-avg", type=_number, required=True)
    parser.add_argument("--starting-bankroll", type=_number, required=True)
    parser.add_argument("--flat-bet-size", type=_number, required=True)
    parser.add_argument("--allowed-bets", nargs="+", help="Exact stat:over/stat:under tokens; default all eligible sides")
    parser.add_argument("--edge-thresholds", nargs="+", type=_number, required=True)
    parser.add_argument("--bl-taus", nargs="+", type=_number, default=(), help="Optional global BL grid; raw always included")
    parser.add_argument("--bl-z-max", nargs="+", type=_number, default=(1.0,))
    parser.add_argument("--bl-max-weight", nargs="+", type=_number, default=(0.5,))
    parser.add_argument("--max-cells", type=_integer, default=64)
    parser.add_argument("--output-dir", required=True, help="New final run directory; collisions rejected, not overwritten")
    parser.add_argument("--local", action="store_true")
    parser.add_argument('--historical-policy', help='Explicit historical policy JSON; requires --local')
    parser.add_argument('--historical-source-metadata', help='Optional historical metadata sidecar JSON')
    add_acquisition_arguments(parser)
    parser.add_argument("--diagnostic-history", action="store_true", help="Required for execution; never grants decision-grade status")
    parser.add_argument("--dry-run", action="store_true", help="Strict filesystem validation/plan only; no model deserialization")
    return parser


def config_from_args(args: argparse.Namespace) -> SweepConfig:
    pairs = None
    if args.allowed_bets is not None:
        pairs = tuple(tuple(token.split(":")) for token in args.allowed_bets)
    return SweepConfig(
        model_dir=args.model_dir, start=args.start, end=args.end, stats=tuple(args.stats),
        bookmakers=tuple(args.bookmakers), n_samples=args.n_samples, seed=args.seed,
        min_minutes_avg=args.min_minutes_avg, starting_bankroll=args.starting_bankroll,
        flat_bet_size=args.flat_bet_size, edge_thresholds=tuple(args.edge_thresholds),
        output_dir=args.output_dir, allowed_bets=pairs, bl_taus=tuple(args.bl_taus),
        bl_z_max=tuple(args.bl_z_max), bl_max_weight=tuple(args.bl_max_weight),
        max_cells=args.max_cells, local=args.local, diagnostic_history=args.diagnostic_history,
        historical_policy=getattr(args, 'historical_policy', None),
        historical_source_metadata=getattr(args, 'historical_source_metadata', None),
        acquisition_limits=limits_from_values(
            vars(args), required=getattr(args, 'historical_policy', None) is not None,
        ),
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = config_from_args(args)
        if not args.dry_run and not config.diagnostic_history:
            raise ValueError("Execution requires --diagnostic-history; historical_readiness stays unverified")
        plan = build_plan(config)
    except (ValueError, OSError, ImportError) as exc:
        parser.error(str(exc))
    if args.dry_run:
        print(json.dumps(plan, indent=2, sort_keys=True, allow_nan=False))
        return 0
    # No replay CLI import: its eager DB bootstrap and legacy discovery/combined
    # offsets must never be reachable from this command.
    from src.backtesting.nba.sweep_runner import SweepFailure, run_sweep

    try:
        output = run_sweep(config, plan=plan)
    except (SweepFailure, OSError, ValueError) as exc:
        # Runner exceptions are sanitized. Do not print arbitrary provider errors.
        print(str(exc) if isinstance(exc, SweepFailure) else f"Sweep failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps({"output_dir": str(output), "state": "complete",
                      "historical_readiness": "unverified", "decision_grade": False}, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
