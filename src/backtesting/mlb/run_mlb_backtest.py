"""
Run an MLB backtest over a date range using trained model artifacts.

Usage:
    python src/backtesting/mlb/run_mlb_backtest.py \
        --start 2024-04-01 --end 2024-09-30 \
        --model-dir src/models/mlb/artifacts/run_pitcher_k_YYYYMMDD \
        --stats pitcher_strikeouts \
        --edge-threshold 0.08
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.backtesting.mlb.mlb_backtest_harness import MLBBacktestHarness
from src.db.client import get_engine
from src.models.black_litterman import BlackLittermanBlender, BLConfig
from src.models.mlb.mlb_feature_store import MLBFeatureStore
from src.models.mlb.mlb_monte_carlo import MLBMonteCarloPredictor
from src.models.mlb.mlb_quantile_trainer import MLBPitcherKPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBBacktestRunner")


def parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{s}'. Use YYYY-MM-DD")


def find_latest_model_dir(base_dir: str) -> Path:
    base = Path(base_dir)
    if not base.exists():
        raise FileNotFoundError(f"Artifacts directory not found: {base}")

    # Check if base_dir itself contains model files
    if (base / "pitcher_k_model.joblib").exists():
        return base

    # Check production symlink
    if (base / "production" / "pitcher_k_model.joblib").exists():
        return base / "production"

    # Fall back to latest run_* directory
    runs = sorted([
        d for d in base.iterdir()
        if d.is_dir() and d.name.startswith("run_")
        and not d.name.endswith("_incomplete")
    ])
    if not runs:
        raise FileNotFoundError(f"No run_* directories found in {base}")

    return runs[-1]


def main():
    parser = argparse.ArgumentParser(description="Run MLB Backtest")
    parser.add_argument("--start", type=parse_date, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=parse_date, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--model-dir", type=str, default="src/models/mlb/artifacts",
        help="Path to MLB model artifacts",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory (default: backtest_results/mlb/<timestamp>)",
    )
    parser.add_argument("--n-samples", type=int, default=5000, help="Monte Carlo samples")
    parser.add_argument(
        "--stats", nargs="+", default=["pitcher_strikeouts"],
        help="Stats to predict",
    )
    parser.add_argument(
        "--edge-threshold", type=float, default=0.08,
        help="Minimum edge to place bet",
    )
    parser.add_argument("--kelly", type=float, default=0.125, help="Kelly fraction")
    parser.add_argument("--bankroll", type=float, default=10000.0, help="Starting bankroll")
    parser.add_argument("--bl-tau", type=float, default=None, help="Black-Litterman tau (None = no BL)")

    args = parser.parse_args()

    # Find model artifacts
    model_path = find_latest_model_dir(args.model_dir)
    logger.info(f"Using MLB model artifacts: {model_path}")

    # Initialize components
    engine = get_engine()
    pitcher_feature_store = MLBFeatureStore(engine)

    logger.info("Loading MLB pitcher K pipeline...")
    pipeline = MLBPitcherKPipeline.load(str(model_path))

    logger.info(f"Initializing MLB MC predictor ({args.n_samples} samples)...")
    predictor = MLBMonteCarloPredictor(pipeline, n_samples=args.n_samples)

    # BL blender
    bl_blender = None
    if args.bl_tau is not None:
        bl_blender = BlackLittermanBlender(BLConfig(tau=args.bl_tau))
        logger.info(f"BL blending enabled: tau={args.bl_tau}")

    # Create harness
    harness = MLBBacktestHarness(
        engine=engine,
        pitcher_feature_store=pitcher_feature_store,
        pitcher_k_pipeline=pipeline,
        pitcher_k_predictor=predictor,
        edge_threshold=args.edge_threshold,
        starting_bankroll=args.bankroll,
        kelly_fraction=args.kelly,
        stats=args.stats,
        bl_blender=bl_blender,
    )

    # Run backtest
    logger.info(f"Running MLB backtest: {args.start} to {args.end}")
    result = harness.run(args.start, args.end)

    # Output results
    output_dir = args.output_dir or f"backtest_results/mlb/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    result.to_csv(output_dir)

    # Print summary
    logger.info("=" * 60)
    logger.info(f"MLB Backtest Results: {args.start} to {args.end}")
    logger.info(f"  Total predictions: {len(result.predictions_df)}")
    logger.info(f"  Total bets: {len(result.bets_df)}")
    if hasattr(result.metrics, 'total_pnl'):
        logger.info(f"  Total P&L: ${result.metrics.total_pnl:,.2f}")
    if hasattr(result.metrics, 'roi_pct'):
        logger.info(f"  ROI: {result.metrics.roi_pct:.1f}%")
    logger.info(f"  Output: {output_dir}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
