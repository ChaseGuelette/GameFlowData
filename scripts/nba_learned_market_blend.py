#!/usr/bin/env python3
"""CLI for the offline learned per-stat market blend research lane."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.backtesting.nba.learned_market_blend import (  # noqa: E402
    EvaluateRequest,
    FitRequest,
    LearnedMarketBlendWorkflowError,
    PrepareRequest,
    evaluate_saved_inputs,
    fit_saved_inputs,
    prepare_saved_inputs,
)


def _allowed_bet(value: str) -> tuple[str, str]:
    pieces = value.split(":")
    if len(pieces) != 2 or pieces[1] not in ("over", "under"):
        raise argparse.ArgumentTypeError("allowed bet must be STAT:over or STAT:under")
    return pieces[0], pieces[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare, fit, and evaluate frozen learned per-stat market blending from saved NBA evidence."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="Receipt-check diagnostic/sweep files and prepare opportunities")
    prepare.add_argument("--diagnostic-dir", type=Path, required=True)
    prepare.add_argument("--sweep-dir", type=Path, required=True)
    prepare.add_argument("--usual-bl-cell", required=True)
    prepare.add_argument("--stats", nargs="+", required=True, choices=("pts", "reb", "ast"))
    prepare.add_argument("--output-dir", type=Path, required=True)
    prepare.add_argument("--dry-run", action="store_true")

    fit = subparsers.add_parser("fit", help="Fit pooled and shrunk per-stat scalar weights")
    fit.add_argument("--input-dir", type=Path, required=True)
    fit.add_argument("--fit-start", required=True)
    fit.add_argument("--fit-end", required=True)
    fit.add_argument("--shrinkage-strength", type=float, required=True)
    fit.add_argument("--output-dir", type=Path, required=True)
    fit.add_argument("--dry-run", action="store_true")

    evaluate = subparsers.add_parser("evaluate", help="Replay frozen weights through canonical selection/simulation")
    evaluate.add_argument("--input-dir", type=Path, required=True)
    evaluate.add_argument("--blend-artifact", type=Path, required=True)
    evaluate.add_argument("--start", required=True)
    evaluate.add_argument("--end", required=True)
    evaluate.add_argument("--edge-thresholds", nargs="+", type=float, required=True)
    evaluate.add_argument("--starting-bankroll", type=float, required=True)
    evaluate.add_argument("--flat-bet-size", type=float, required=True)
    evaluate.add_argument("--output-dir", type=Path, required=True)
    evaluate.add_argument("--allowed-bets", nargs="+", type=_allowed_bet)
    evaluate.add_argument("--dry-run", action="store_true")
    return parser


def _display(result: dict | Path) -> None:
    if isinstance(result, Path):
        print(result)
    else:
        print(json.dumps(result, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare_saved_inputs(
                PrepareRequest(
                    diagnostic_dir=args.diagnostic_dir,
                    sweep_dir=args.sweep_dir,
                    usual_bl_cell=args.usual_bl_cell,
                    stats=tuple(args.stats),
                    output_dir=args.output_dir,
                ),
                dry_run=args.dry_run,
            )
        elif args.command == "fit":
            result = fit_saved_inputs(
                FitRequest(
                    input_dir=args.input_dir,
                    fit_start=args.fit_start,
                    fit_end=args.fit_end,
                    shrinkage_strength=args.shrinkage_strength,
                    output_dir=args.output_dir,
                ),
                dry_run=args.dry_run,
            )
        else:
            result = evaluate_saved_inputs(
                EvaluateRequest(
                    input_dir=args.input_dir,
                    blend_artifact=args.blend_artifact,
                    start=args.start,
                    end=args.end,
                    edge_thresholds=tuple(args.edge_thresholds),
                    starting_bankroll=args.starting_bankroll,
                    flat_bet_size=args.flat_bet_size,
                    output_dir=args.output_dir,
                    allowed_bets=tuple(args.allowed_bets) if args.allowed_bets else None,
                ),
                dry_run=args.dry_run,
            )
        _display(result)
        return 0
    except (LearnedMarketBlendWorkflowError, ValueError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
