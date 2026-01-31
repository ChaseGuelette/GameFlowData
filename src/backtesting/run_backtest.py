"""
Run a backtest over a date range using trained model artifacts.

Usage:
    python src/backtesting/run_backtest.py --model-dir src/models/artifacts/run_XXXXX --start 2024-10-22 --end 2025-01-15
    python src/backtesting/run_backtest.py --start 2024-11-01 --end 2024-12-31  # auto-finds latest run
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.backtesting.backtest_harness import BacktestHarness
from src.db.client import get_engine
from src.models.feature_store import FeatureStore
from src.models.monte_carlo import MonteCarloPredictor, load_copula_params
from src.models.quantile_trainer import PlayerPropsModelPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BacktestRunner")


def find_latest_model_dir(base_dir: str) -> Path:
    """Find the most recent run_* directory in the artifacts folder."""
    base = Path(base_dir)
    if not base.exists():
        raise FileNotFoundError(f"Artifacts directory not found: {base}")

    # Check if base_dir itself contains model files
    if (base / "minutes_model.joblib").exists():
        return base

    # Otherwise look for run_* subdirectories
    runs = sorted([d for d in base.iterdir() if d.is_dir() and d.name.startswith("run_")])
    if not runs:
        raise FileNotFoundError(f"No run_* directories found in {base}")

    return runs[-1]


def main():
    parser = argparse.ArgumentParser(description="Run Backtest on Historical Data")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--model-dir", type=str, default="src/models/artifacts", help="Path to model artifacts")
    parser.add_argument(
        "--output-dir", type=str, default=None, help="Output directory (default: backtest_results/<timestamp>)"
    )
    parser.add_argument(
        "--n-samples", type=int, default=5000, help="Monte Carlo samples (more = slower but more precise)"
    )
    parser.add_argument("--stats", nargs="+", default=["pts", "reb", "ast"], help="Stats to predict")
    parser.add_argument("--edge-threshold", type=float, default=0.05, help="Minimum edge to place bet")
    parser.add_argument("--starting-bankroll", type=float, default=10000.0, help="Starting bankroll amount")
    parser.add_argument(
        "--kelly-fraction", type=float, default=0.125, help="Kelly Criterion fraction (e.g., 0.125 for 1/8th)"
    )
    parser.add_argument(
        "--bookmakers",
        nargs="+",
        default=[
            # US market (original)
            "draftkings", "fanduel", "betmgm", "betrivers", "bovada",
            "williamhill_us", "betonlineag", "unibet_us", "mybookieag",
            "pointsbetus", "fanatics", "barstool", "wynnbet",
            # US2 / us_ex markets
            "ballybet", "betopenly", "betparx", "espnbet", "fliff",
            "hardrockbet", "novig", "polymarket", "prophetx", "rebet",
            "windcreek",
        ],
        help="List of bookmakers to shop lines from (default: all available)",
    )
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument(
        "--allowed-bets",
        nargs="+",
        default=None,
        help="Stat:side pairs to allow (e.g., pts:under reb:over). If omitted, all combos are allowed.",
    )
    parser.add_argument(
        "--bl-tau",
        type=float,
        default=None,
        help="Black-Litterman tau parameter. Enables BL blending when set (e.g., 0.05=conservative). None=disabled.",
    )

    args = parser.parse_args()

    # Parse allowed_bets from "stat:side" strings to list of tuples
    allowed_bets = None
    if args.allowed_bets:
        allowed_bets = []
        for pair in args.allowed_bets:
            parts = pair.lower().split(":")
            if len(parts) != 2 or parts[1] not in ("over", "under"):
                parser.error(f"Invalid --allowed-bets value '{pair}'. Use format stat:side (e.g., pts:under)")
            allowed_bets.append((parts[0], parts[1]))

    # Resolve model directory
    model_path = find_latest_model_dir(args.model_dir)
    logger.info(f"Using model artifacts from: {model_path}")

    # Setup output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("backtest_results") / f"bt_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize components
    engine = get_engine()
    feature_store = FeatureStore(engine)

    logger.info("Loading model pipeline...")
    pipeline = PlayerPropsModelPipeline.load_all(str(model_path), feature_store)

    # Load copula parameters if available in model artifacts
    copula_params = load_copula_params(str(model_path))
    if copula_params:
        logger.info(f"Loaded Gaussian copula params: { {k: f'{v:.4f}' for k, v in copula_params.items()} }")
    else:
        logger.info("No copula_params.json found, using legacy correlation adjustment")

    logger.info(f"Initializing Monte Carlo predictor (n_samples={args.n_samples})...")
    predictor = MonteCarloPredictor(pipeline, n_samples=args.n_samples, copula_params=copula_params)

    # Configure Black-Litterman blender (optional)
    bl_blender = None
    if args.bl_tau is not None:
        from src.models.black_litterman import BlackLittermanBlender, BLConfig

        bl_blender = BlackLittermanBlender(BLConfig(tau=args.bl_tau))
        logger.info(f"Black-Litterman blending enabled (tau={args.bl_tau})")

    # Configure and run harness
    harness = BacktestHarness(
        engine=engine,
        feature_store=feature_store,
        model_pipeline=pipeline,
        predictor=predictor,
        stats=args.stats,
        edge_threshold=args.edge_threshold,
        starting_bankroll=args.starting_bankroll,
        kelly_fraction=args.kelly_fraction,
        bookmakers=args.bookmakers,
        allowed_bets=allowed_bets,
        bl_blender=bl_blender,
    )

    if allowed_bets:
        logger.info(f"Allowed bets filter: {[f'{s}:{side}' for s, side in allowed_bets]}")
    logger.info(f"Running backtest from {args.start} to {args.end}...")
    result = harness.run(start_date=args.start, end_date=args.end, max_workers=args.workers)

    # Save results
    result.to_csv(str(output_dir))

    # Print summary
    metrics = result.metrics
    logger.info("=" * 60)
    logger.info("BACKTEST RESULTS")
    logger.info("=" * 60)
    logger.info(f"Total bets: {metrics.total_bets}")
    logger.info(f"Win rate: {metrics.hit_rate:.1%}")
    logger.info(f"ROI: {metrics.roi:.2%}")
    logger.info(f"Total profit: ${metrics.total_profit:,.2f}")
    logger.info(f"Sharpe ratio: {metrics.sharpe_ratio:.3f}")
    logger.info(f"Max drawdown: {metrics.max_drawdown:.2%}")
    logger.info(f"Results saved to: {output_dir}")


if __name__ == "__main__":
    main()
