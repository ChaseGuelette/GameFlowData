"""CLI for receipt-verified direct-total REB sweep analysis."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtesting.nba.direct_reb_sweep import analyze_sweep


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Analyze a completed direct-total REB saved-window sweep")
    result.add_argument("--sweep-dir", type=Path, required=True)
    result.add_argument("--output-dir", type=Path, required=True)
    return result


def main() -> None:
    args = parser().parse_args()
    print(analyze_sweep(args.sweep_dir, args.output_dir))


if __name__ == "__main__":
    main()
