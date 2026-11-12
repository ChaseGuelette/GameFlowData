"""CLI for the offline direct-total REB saved-window sweep."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtesting.nba.direct_reb_sweep import SweepRequest, dry_run_summary, prepare_sweep, run_sweep


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Run the receipt-verified direct-total REB saved-window sweep")
    result.add_argument("--benchmark-dir", type=Path, required=True)
    result.add_argument("--output-dir", type=Path, required=True)
    result.add_argument("--reference-sweep-dir", type=Path)
    result.add_argument("--models", nargs="+", default=["direct", "control"], choices=["direct", "control"])
    result.add_argument("--dry-run", action="store_true")
    return result


def main() -> None:
    args = parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    prepared = prepare_sweep(SweepRequest(
        benchmark_dir=args.benchmark_dir,
        output_dir=args.output_dir,
        reference_sweep_dir=args.reference_sweep_dir,
        models=tuple(args.models),
    ))
    if args.dry_run:
        print(json.dumps(dry_run_summary(prepared), indent=2, sort_keys=True))
        return
    print(run_sweep(prepared))


if __name__ == "__main__":
    main()
