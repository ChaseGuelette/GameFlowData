"""
Run a backtest over a date range using trained model artifacts.

Usage:
    python src/backtesting/run_backtest.py --model-dir src/models/artifacts/run_XXXXX --start 2024-10-22 --end 2025-01-15
    python src/backtesting/run_backtest.py --start 2024-11-01 --end 2024-12-31  # auto-finds latest run

    # Per-stat edge thresholds
    python src/backtesting/run_backtest.py --start 2024-11-01 --end 2024-12-31 --edge-threshold pts=0.10 reb=0.07 ast=0.15

    # Per-stat BL tau (use "none" to disable BL for a stat)
    python src/backtesting/run_backtest.py --start 2024-11-01 --end 2024-12-31 --bl-tau pts=0.05 reb=0.10 ast=none
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.backtesting.backtest_harness import BacktestHarness
from src.config.stat_config import StatConfigSet
from src.db.client import get_engine
from src.models.feature_store import FeatureStore
from src.models.monte_carlo import MonteCarloPredictor, load_copula_params
from src.models.nba_artifacts import ArtifactInspection, resolve_artifact_for_execution
from src.models.quantile_trainer import PlayerPropsModelPipeline
from src.models.training_preflight import (
    SUPPORTED_MODEL_STATS,
    validate_finite_number,
    validate_output_path,
    validate_positive_int,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BacktestRunner")


def parse_date(s: str) -> date:
    """Parse date string in YYYY-MM-DD format for argparse."""
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{s}'. Use YYYY-MM-DD")


def find_latest_model_dir(base_dir: str) -> Path:
    """Find the most recent run_* directory in the artifacts folder."""
    base = Path(base_dir)
    if not base.exists():
        raise FileNotFoundError(f"Artifacts directory not found: {base}")

    # Check if base_dir itself contains model files
    if (base / "minutes_model.joblib").exists():
        return base

    # Otherwise look for nba_run_* subdirectories
    runs = sorted([d for d in base.iterdir() if d.is_dir() and d.name.startswith("nba_run_")])
    if not runs:
        raise FileNotFoundError(f"No nba_run_* directories found in {base}")

    return runs[-1]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Backtest on Historical Data")
    parser.add_argument("--start", type=parse_date, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=parse_date, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--model-dir", type=str, default="src/models/artifacts", help="Path to model artifacts")
    parser.add_argument(
        "--output-dir", type=str, default=None, help="Output directory (default: backtest_results/<timestamp>)"
    )
    parser.add_argument(
        "--n-samples", type=int, default=5000, help="Monte Carlo samples (more = slower but more precise)"
    )
    parser.add_argument("--stats", nargs="+", default=["pts", "reb", "ast"], help="Stats to predict")
    parser.add_argument(
        "--edge-threshold",
        nargs="+",
        default=["0.05"],
        help="Minimum edge to place bet. Global (0.05) or per-stat (pts=0.10 reb=0.07)",
    )
    parser.add_argument("--starting-bankroll", type=float, default=10000.0, help="Starting bankroll amount")
    parser.add_argument(
        "--kelly-fraction", type=float, default=0.125, help="Kelly Criterion fraction (e.g., 0.125 for 1/8th)"
    )
    parser.add_argument(
        "--max-bet-pct",
        type=float,
        default=None,
        help="Maximum bet size as %% of bankroll (e.g., 0.025 = 2.5%%). Caps Kelly sizing. Default: no cap.",
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
            "hardrockbet", "novig", "prophetx", "rebet",
            "windcreek",
        ],
        help="List of bookmakers to shop lines from (default: all available)",
    )
    # --workers removed (ISS-026): harness always runs sequentially
    parser.add_argument(
        "--allowed-bets",
        nargs="+",
        default=None,
        help="Stat:side pairs to allow (e.g., pts:under reb:over). If omitted, all combos are allowed.",
    )
    parser.add_argument(
        "--bl-tau",
        nargs="+",
        default=None,
        help="Black-Litterman tau. Global (0.05) or per-stat (pts=0.05 reb=0.10 ast=none). None=disabled.",
    )
    parser.add_argument(
        "--bl-sizing-tau",
        type=float,
        default=None,
        help="Black-Litterman tau for Kelly sizing only (edge detection uses raw model probs). E.g., 0.10",
    )

    parser.add_argument("--local", action="store_true",
                        help="Use local Postgres (LOCAL_DATABASE_URL) instead of Supabase")

    parser.add_argument(
        "--dry-run",
        "--preflight",
        dest="dry_run",
        action="store_true",
        help="Validate filesystem/config inputs and print a JSON execution plan without replaying",
    )
    parser.add_argument(
        "--allow-legacy-artifacts",
        action="store_true",
        help="Permit complete pre-manifest bundles as legacy/unverified artifacts",
    )
    return parser


def _namespace_value(args: argparse.Namespace, name: str, default=None):
    """Read an optional CLI field without inventing a value for omitted fields."""
    return vars(args).get(name, default)


def _resolve_complete_model_dir(
    base_dir: str,
    stats: list[str],
    *,
    allow_legacy: bool = False,
) -> ArtifactInspection:
    """Resolve the newest valid execution artifact without changing discovery behavior."""
    return resolve_artifact_for_execution(base_dir, stats, allow_legacy=allow_legacy)


def _validate_backtest_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> dict:
    try:
        if args.start > args.end:
            raise ValueError("--start must be on or before --end")
        unsupported = sorted(set(args.stats) - set(SUPPORTED_MODEL_STATS))
        if unsupported:
            raise ValueError(f"Unsupported --stats values: {', '.join(unsupported)}")
        if len(set(args.stats)) != len(args.stats):
            raise ValueError("--stats must not contain duplicates")

        validate_positive_int(args.n_samples, "--n-samples")
        validate_finite_number(args.starting_bankroll, "--starting-bankroll", minimum=0.0, strict_minimum=True)
        validate_finite_number(args.kelly_fraction, "--kelly-fraction", minimum=0.0, maximum=1.0, strict_minimum=True)
        if args.max_bet_pct is not None:
            validate_finite_number(args.max_bet_pct, "--max-bet-pct", minimum=0.0, maximum=1.0, strict_minimum=True)
        if args.bl_sizing_tau is not None:
            validate_finite_number(args.bl_sizing_tau, "--bl-sizing-tau", minimum=0.0)

        stat_config = StatConfigSet.from_cli_args(
            edge_values=args.edge_threshold,
            tau_values=args.bl_tau,
            stats=args.stats,
        )
        configured_stats = set(stat_config.configs)
        unknown_config_stats = sorted(configured_stats - set(args.stats))
        if unknown_config_stats:
            raise ValueError(
                "Per-stat edge/tau configuration must be a subset of --stats: "
                + ", ".join(unknown_config_stats)
            )
        validate_finite_number(
            stat_config.global_edge_threshold,
            "global edge threshold",
            minimum=0.0,
            maximum=1.0,
        )
        if stat_config.global_bl_tau is not None:
            validate_finite_number(stat_config.global_bl_tau, "global BL tau", minimum=0.0)
        for stat in args.stats:
            validate_finite_number(
                stat_config.get_edge_threshold(stat),
                f"{stat} edge threshold",
                minimum=0.0,
                maximum=1.0,
            )
            tau = stat_config.get_bl_tau(stat)
            if tau is not None:
                validate_finite_number(tau, f"{stat} BL tau", minimum=0.0)

        allowed_bets = None
        if args.allowed_bets:
            allowed_bets = []
            for pair in args.allowed_bets:
                parts = pair.lower().split(":")
                if len(parts) != 2 or parts[1] not in ("over", "under"):
                    raise ValueError(f"Invalid --allowed-bets value '{pair}'. Use format stat:side (e.g., pts:under)")
                if parts[0] not in args.stats:
                    raise ValueError(f"Allowed bet stat '{parts[0]}' must be included in --stats")
                allowed_bets.append((parts[0], parts[1]))

        artifact = _resolve_complete_model_dir(
            args.model_dir,
            args.stats,
            allow_legacy=bool(_namespace_value(args, "allow_legacy_artifacts", False)),
        )
        model_path = artifact.path
        if args.output_dir:
            output_dir = validate_output_path(args.output_dir)
            planned_output_name = output_dir.name
        else:
            output_root = validate_output_path("backtest_results")
            output_dir = output_root / "nba_bt_<runtime_timestamp>"
            planned_output_name = "nba_bt_<runtime_timestamp>"
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    return {
        "stat_config": stat_config,
        "allowed_bets": allowed_bets,
        "model_path": model_path,
        "artifact": artifact,
        "output_dir": output_dir,
        "planned_output_name": planned_output_name,
    }


def _backtest_plan(args: argparse.Namespace, validated: dict) -> dict:
    stat_config = validated["stat_config"]
    effective = {
        stat: {
            "edge_threshold": stat_config.get_edge_threshold(stat),
            "bl_tau": stat_config.get_bl_tau(stat),
        }
        for stat in args.stats
    }
    return {
        "command": "nba-historical-replay",
        "status": "preflight_only",
        "mode": "historical_replay",
        "execution": False,
        "quote_clean_readiness": {"checked": False, "status": "not_checked"},
        "database_target": "LOCAL" if _namespace_value(args, "local", False) else "REMOTE",
        "date_range": {"start": str(args.start), "end": str(args.end)},
        "stats": args.stats,
        "n_samples": args.n_samples,
        "model_directory": str(validated["model_path"].resolve(strict=False)),
        "artifact_identity": validated["artifact"].to_plan_dict(),
        "bankroll": {
            "starting": args.starting_bankroll,
            "kelly_fraction": args.kelly_fraction,
            "max_bet_pct": args.max_bet_pct,
        },
        "configuration": {
            "effective_by_stat": effective,
            "bl_sizing_tau": args.bl_sizing_tau,
            "allowed_bets": (
                None if validated["allowed_bets"] is None
                else [f"{stat}:{side}" for stat, side in validated["allowed_bets"]]
            ),
            "bookmakers": args.bookmakers,
        },
        "output": {
            "path": str(validated["output_dir"].resolve(strict=False)),
            "planned_directory_name": validated["planned_output_name"],
            "created": False,
        },
    }


def main(argv=None):
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    validated = _validate_backtest_args(parser, args)
    stat_config = validated["stat_config"]
    allowed_bets = validated["allowed_bets"]
    model_path = validated["model_path"]
    if _namespace_value(args, "dry_run", False):
        print(json.dumps(_backtest_plan(args, validated), indent=2, sort_keys=True))
        return 0

    logger.info(f"Using model artifacts from: {model_path}")
    if validated["artifact"].state == "legacy_unverified":
        logger.warning("Legacy/unverified artifact %s; file hashes only, not decision-grade", validated["artifact"].identity)

    # Setup output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("backtest_results") / f"nba_bt_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize components
    use_local = bool(_namespace_value(args, "local", False))
    engine = get_engine(local=use_local)
    if use_local:
        logger.info("Using LOCAL database")
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
    predictor = MonteCarloPredictor(
        pipeline, n_samples=args.n_samples, copula_params=copula_params,
    )

    # Log per-stat configuration
    logger.info(f"Stat config: global_edge={stat_config.global_edge_threshold}")
    for stat in args.stats:
        edge = stat_config.get_edge_threshold(stat)
        tau = stat_config.get_bl_tau(stat)
        tau_str = f"{tau}" if tau is not None else "disabled"
        logger.info(f"  {stat}: edge={edge}, bl_tau={tau_str}")

    # bl_blender is no longer needed as a single instance - per-stat blenders are created in harness
    bl_blender = None

    # Configure Black-Litterman blender for sizing only (optional)
    bl_sizing_blender = None
    if args.bl_sizing_tau is not None:
        from src.models.black_litterman import BlackLittermanBlender, BLConfig

        bl_sizing_blender = BlackLittermanBlender(BLConfig(tau=args.bl_sizing_tau))
        logger.info(f"Black-Litterman sizing enabled (tau={args.bl_sizing_tau})")

    # Configure and run harness
    harness = BacktestHarness(
        engine=engine,
        feature_store=feature_store,
        model_pipeline=pipeline,
        predictor=predictor,
        stats=args.stats,
        edge_threshold=stat_config.global_edge_threshold,
        starting_bankroll=args.starting_bankroll,
        kelly_fraction=args.kelly_fraction,
        max_bet_pct=args.max_bet_pct,
        bookmakers=args.bookmakers,
        allowed_bets=allowed_bets,
        bl_blender=bl_blender,
        bl_sizing_blender=bl_sizing_blender,
        stat_config=stat_config,
    )

    if args.max_bet_pct:
        logger.info(f"Max bet cap: {args.max_bet_pct:.1%} of bankroll")
    if allowed_bets:
        logger.info(f"Allowed bets filter: {[f'{s}:{side}' for s, side in allowed_bets]}")
    logger.info(f"Running backtest from {args.start} to {args.end}...")
    result = harness.run(start_date=args.start, end_date=args.end)

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

    return 0


if __name__ == "__main__":
    main()
