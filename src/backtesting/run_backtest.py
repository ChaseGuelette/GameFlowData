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
import math
import sys
from dataclasses import asdict
from datetime import date, datetime
from numbers import Real
from pathlib import Path
from typing import cast

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.backtesting.backtest_harness import BacktestHarness
from src.backtesting.nba.historical_execution import (
    load_attachment,
    make_sources,
    recheck_attachment,
    validate_artifact_window,
    validate_window,
)
from src.config.combo_config import COMBO_DEFINITIONS
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


def _positive_dollars(value: str | float) -> float:
    """Validate flat controls during parsing, before any execution side effects."""
    if isinstance(value, bool) or not isinstance(value, str | Real):
        raise argparse.ArgumentTypeError("--flat-bet-size must be finite and strictly positive dollars")
    try:
        amount = float(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("--flat-bet-size must be finite and strictly positive dollars") from exc
    if not math.isfinite(amount) or amount <= 0:
        raise argparse.ArgumentTypeError("--flat-bet-size must be finite and strictly positive dollars")
    return amount


def _artifact_cli_identity(artifact: ArtifactInspection) -> dict:
    """Report inspected byte identity without certifying historical model quality."""
    return {
        **artifact.to_plan_dict(),
        "resolved_model_dir": str(artifact.path.resolve()),
        "identity_status": artifact.state,
        "manifest_validation": (
            "validated" if artifact.state == "verified_manifest" else "not_present_explicit_legacy_opt_in"
        ),
        "model_file_digests": {
            name: digest for name, digest in artifact.file_hashes.items() if name.endswith(".joblib")
        },
        "historical_readiness": "unverified_not_decision_grade",
    }


def _effective_cli_config(harness: BacktestHarness, artifact: ArtifactInspection, requested_stats: list[str]) -> dict:
    """Describe actual CLI execution and the artifact accepted by preflight.

    Direct harness.run() enrichment is outside this CLI integration. Validated
    artifact bytes establish identity/completion, not historical model quality.
    """
    simulator = harness._simulator
    predictor = cast(MonteCarloPredictor, harness.predictor)
    flat = simulator.flat_bet_size is not None
    # Inherited calculation routing computes optional sizing posteriors only
    # when edge BL is off, even for a non-BL stat in a mixed run.
    edge_bl_enabled = harness.bl_blender is not None or any(
        harness._get_blender_for_stat(stat) is not None for stat in harness.stats
    )
    requested = set(harness.stats)
    derivable_combos = {
        stat for stat, definition in COMBO_DEFINITIONS.items()
        if set(definition["components"]).issubset(requested)
    }
    effective_stats = requested | derivable_combos
    per_stat = {}
    for stat in sorted(effective_stats):
        blender = harness._get_blender_for_stat(stat)
        per_stat[stat] = {
            "edge_threshold": simulator._get_edge_threshold(stat),
            "bl_config": asdict(blender.config) if blender is not None else None,
        }
    evidence = {
        "config_evidence_scope": "cli_export_only",
        "flat_bet_size": simulator.flat_bet_size,
        "sizing": {
            "mode": "flat" if flat else "kelly",
            "flat_bet_size": simulator.flat_bet_size,
            "bankroll_limited": True,
            "kelly_fraction": None if flat else simulator.kelly_fraction,
            "max_bet_pct": None if flat else simulator.max_bet_pct,
            "bl_sizing_applies": not flat and simulator.use_bl_for_sizing and not edge_bl_enabled,
            "bl_sizing_status": (
                "disabled" if not simulator.use_bl_for_sizing else
                "not_applicable_flat" if flat else
                "ignored_when_edge_bl_enabled_inherited_routing" if edge_bl_enabled else "enabled"
            ),
        },
        "requested_stats": sorted(requested_stats),
        "effective_stat_config": per_stat,
        "effective_stat_config_scope": "requested_and_derived_combo_stats",
        "effective_allowed_bets": (
            sorted(f"{stat}:{side}" for stat, side in simulator.allowed_bets)
            if simulator.allowed_bets is not None else None
        ),
        "effective_bl_sizing_config": (
            asdict(harness.bl_sizing_blender.config) if harness.bl_sizing_blender is not None else None
        ),
        "predictor_settings": {
            "n_samples": predictor.n_samples,
            "use_correlated_sampling": predictor.use_correlated_sampling,
            "correlation_config": predictor.correlation_config,
            "variance_inflation": predictor.variance_inflation,
            "tail_adjustment": predictor.tail_adjustment,
            "blowout_config": predictor.blowout_config,
            "copula_params": predictor.copula_params,
            "combined_calibration_offsets": predictor.combined_calibration_offsets,
        },
        "artifact_identity": _artifact_cli_identity(artifact),
    }
    # Deterministic nested mappings/lists with real JSON types, not stringified mocks.
    return json.loads(json.dumps(evidence, sort_keys=True, allow_nan=False))


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
        "--flat-bet-size", type=_positive_dollars, default=None,
        help="Fixed finite positive dollar stake, capped by remaining bankroll; overrides Kelly sizing and cap.",
    )
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
    parser.add_argument('--historical-policy', help='Explicit historical policy JSON; requires --local')
    parser.add_argument('--historical-source-metadata', help='Optional historical source-metadata JSON')

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
    # Also validate programmatic callers supplying a namespace rather than parsing.
    if args.flat_bet_size is not None:
        try:
            args.flat_bet_size = _positive_dollars(args.flat_bet_size)
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))

    try:
        if args.start > args.end:
            raise ValueError("--start must be on or before --end")
        historical = load_attachment(_namespace_value(args, 'historical_policy'),
                                     _namespace_value(args, 'historical_source_metadata'),
                                     local=_namespace_value(args, 'local', False) is True)
        validate_window(historical, args.start, args.end)
        if historical and (_namespace_value(args, 'allow_legacy_artifacts', False)
                           or args.flat_bet_size is None):
            raise ValueError('Historical replay requires nonlegacy artifacts and explicit flat stakes')
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
        if historical:
            validate_artifact_window(artifact, args.start)
            if Path(args.model_dir).resolve() != artifact.path.resolve():
                raise ValueError('Historical replay requires the exact artifact directory, not discovery')
        if args.output_dir:
            output_dir = validate_output_path(args.output_dir)
            planned_output_name = output_dir.name
        else:
            output_root = validate_output_path("backtest_results")
            output_dir = output_root / "nba_bt_<runtime_timestamp>"
            planned_output_name = "nba_bt_<runtime_timestamp>"
        if historical:
            if output_dir.resolve(strict=False).is_relative_to(model_path.resolve()):
                raise ValueError('Historical output must be outside the artifact directory')
            for item in (historical['policy'], historical['source_metadata']):
                if item['path'] and Path(item['path']).is_relative_to(output_dir.resolve(strict=False)):
                    raise ValueError('Historical input collides with replay output')
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    return {
        "stat_config": stat_config,
        "allowed_bets": allowed_bets,
        "model_path": model_path,
        "artifact": artifact,
        "output_dir": output_dir,
        "planned_output_name": planned_output_name,
        "historical": historical,
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
        "historical_attachment": validated['historical'],
        "database_target": "LOCAL" if _namespace_value(args, "local", False) else "REMOTE",
        "date_range": {"start": str(args.start), "end": str(args.end)},
        "stats": args.stats,
        "n_samples": args.n_samples,
        "model_directory": str(validated["model_path"].resolve(strict=False)),
        "artifact_identity": _artifact_cli_identity(validated["artifact"]),
        "bankroll": {
            "starting": args.starting_bankroll,
            "sizing_mode": "flat" if args.flat_bet_size is not None else "kelly",
            "flat_bet_size": args.flat_bet_size,
            "kelly_fraction": None if args.flat_bet_size is not None else args.kelly_fraction,
            "max_bet_pct": None if args.flat_bet_size is not None else args.max_bet_pct,
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
    historical = validated['historical']
    policy = recheck_attachment(historical)
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
    feature_store = (FeatureStore(engine, historical_sources=make_sources(engine, historical))
                     if historical else FeatureStore(engine))

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
        flat_bet_size=args.flat_bet_size,
        bookmakers=args.bookmakers,
        allowed_bets=allowed_bets,
        bl_blender=bl_blender,
        bl_sizing_blender=bl_sizing_blender,
        stat_config=stat_config,
        historical_policy=policy,
    )

    if args.max_bet_pct:
        logger.info(f"Max bet cap: {args.max_bet_pct:.1%} of bankroll")
    if allowed_bets:
        logger.info(f"Allowed bets filter: {[f'{s}:{side}' for s, side in allowed_bets]}")
    logger.info(f"Running backtest from {args.start} to {args.end}...")
    result = harness.run(start_date=args.start, end_date=args.end)
    if historical:
        recheck_attachment(historical)
        current = _resolve_complete_model_dir(str(model_path), args.stats, allow_legacy=False)
        if current.to_plan_dict() != validated['artifact'].to_plan_dict():
            raise ValueError('Historical artifact changed during replay')
        result.config['historical_attachment'] = historical

    # Preserve legacy config keys. This enrichment does not change harness.run().
    result.config.update(_effective_cli_config(harness, validated["artifact"], args.stats))

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
