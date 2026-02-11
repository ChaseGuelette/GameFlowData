"""
Backtest parameter sweep: runs Phase 0-1 once, then sweeps
(tau, edge_threshold, kelly_fraction) configs through Phase 1.5-2-metrics.

Avoids redundant DB queries and XGBoost calls by caching all shared data
(features, lines, actuals, voids, MC predictions/samples) and replaying
only the edge calculation + bet simulation per configuration.

Usage:
    python src/backtesting/run_sweep.py --start 2026-01-01 --end 2026-01-29
    python src/backtesting/run_sweep.py --start 2026-01-01 --end 2026-01-29 \
        --tau none 0.03 0.05 0.09 0.15 0.25
    python src/backtesting/run_sweep.py --start 2026-01-01 --end 2026-01-29 \
        --tau none 0.05 0.10 --edge 0.03 0.05 0.07 --kelly 0.10 0.125 0.15
"""

import argparse
import itertools
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.backtesting.backtest_harness import BacktestHarness, BacktestResult
from src.backtesting.bet_simulator import BetSimulator
from src.backtesting.performance_metrics import MetricsCalculator, PerformanceMetrics
from src.config.stat_config import StatConfigSet
from src.db.client import get_engine
from src.models.black_litterman import BLConfig, BlackLittermanBlender
from src.models.feature_store import FeatureStore
from src.models.monte_carlo import MonteCarloPredictor, load_copula_params
from src.models.quantile_trainer import PlayerPropsModelPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BacktestSweep")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SweepConfig:
    """One point in the parameter sweep grid."""

    tau: float | None  # None = no BL blending (baseline)
    edge_threshold: float
    kelly_fraction: float
    z_max: float = 1.0  # BL confidence saturation point

    @property
    def label(self) -> str:
        if self.tau is None:
            return f"no_BL | edge={self.edge_threshold} | kelly={self.kelly_fraction}"
        return f"tau={self.tau} z_max={self.z_max} | edge={self.edge_threshold} | kelly={self.kelly_fraction}"

    def to_dict(self) -> dict:
        return {
            "tau": self.tau,
            "z_max": self.z_max,
            "edge_threshold": self.edge_threshold,
            "kelly_fraction": self.kelly_fraction,
        }


@dataclass
class SweepResult:
    """Results for a single sweep configuration."""

    config: SweepConfig
    metrics: PerformanceMetrics
    bets_df: pd.DataFrame
    predictions_df: pd.DataFrame
    all_edges_df: pd.DataFrame  # Pre-line-shopping edges across all bookmakers
    elapsed_seconds: float


# ---------------------------------------------------------------------------
# Grid builder
# ---------------------------------------------------------------------------

def build_sweep_grid(
    tau_values: list[float | None],
    edge_thresholds: list[float],
    kelly_fractions: list[float],
    z_max_values: list[float] | None = None,
) -> list[SweepConfig]:
    """Generate Cartesian product of parameter values.

    Note: z_max only applies when tau is not None. For no-BL configs,
    z_max is ignored but we still include one config per (edge, kelly) pair.
    """
    if z_max_values is None:
        z_max_values = [1.0]

    configs = []
    for tau, edge, kelly, z_max in itertools.product(tau_values, edge_thresholds, kelly_fractions, z_max_values):
        # For no-BL, z_max doesn't matter, so only add one config per (edge, kelly)
        if tau is None and z_max != z_max_values[0]:
            continue
        configs.append(SweepConfig(tau=tau, edge_threshold=edge, kelly_fraction=kelly, z_max=z_max))
    return configs


# ---------------------------------------------------------------------------
# Phase 0 + 1: shared data loading and prediction generation
# ---------------------------------------------------------------------------

def run_shared_phases(
    harness: BacktestHarness,
    feature_store: FeatureStore,
    predictor: MonteCarloPredictor,
    start_date: date,
    end_date: date,
    stats: list[str],
    min_minutes_avg: int = 10,
) -> tuple[
    list[date],
    dict[date, pd.DataFrame],
    pd.DataFrame,
    pd.DataFrame,
    dict[date, pd.DataFrame],
    dict[date, dict],
]:
    """Execute Phase 0 (DB fetch) and Phase 1 (MC predictions) once.

    Returns:
        game_dates: Sorted list of dates with games.
        prefetched_lines: dict[date, DataFrame] of betting lines.
        actuals_df: DataFrame of actual outcomes (long format).
        voids_df: DataFrame of DNPs.
        date_predictions: dict[date, DataFrame] of raw predictions (pre-edge-calc).
        date_samples: dict[date, dict[(player_id, game_id, stat) -> ndarray]].
    """
    # Phase 0: Prefetch all data from DB
    logger.info("Phase 0: Fetching game dates...")
    game_dates = harness._get_game_dates(start_date, end_date)
    logger.info(f"  Found {len(game_dates)} dates with games")

    logger.info("Phase 0: Prefetching features...")
    prefetched_features = feature_store.get_features_for_date_range(start_date, end_date)
    logger.info(f"  Prefetched features for {len(prefetched_features)} dates")

    logger.info("Phase 0: Prefetching lines...")
    prefetched_lines = harness._prefetch_all_lines(start_date, end_date)
    logger.info(f"  Prefetched lines for {len(prefetched_lines)} dates")

    logger.info("Phase 0: Fetching actuals and voids...")
    actuals_df = harness._get_actuals(start_date, end_date)
    voids_df = harness._get_voids(start_date, end_date)
    logger.info(f"  Actuals: {len(actuals_df)} rows, Voids: {len(voids_df)} rows")

    # Phase 1: Generate MC predictions for all dates (reusable across configs)
    logger.info("Phase 1: Generating predictions...")
    date_predictions: dict[date, pd.DataFrame] = {}
    date_samples: dict[date, dict] = {}
    total_predictions = 0

    for i, game_date in enumerate(game_dates):
        features_df = prefetched_features.get(game_date)
        if features_df is None or features_df.empty:
            continue

        # Filter for minimum minutes
        if "player_avg_min_l5" in features_df.columns:
            features_df = features_df[features_df["player_avg_min_l5"] >= min_minutes_avg]
        if features_df.empty:
            continue

        # Sort for deterministic RNG ordering
        features_df = features_df.sort_values(["player_id", "game_id"]).reset_index(drop=True)

        try:
            predictions_list, prediction_samples = predictor.predict_batch_for_date(features_df, stats=stats)
        except Exception as e:
            logger.error(f"  Error predicting {game_date}: {e}")
            continue

        for pred in predictions_list:
            pred["game_date"] = game_date

        if predictions_list:
            date_predictions[game_date] = pd.DataFrame(predictions_list)
            date_samples[game_date] = prediction_samples
            total_predictions += len(predictions_list)

        if (i + 1) % 5 == 0 or (i + 1) == len(game_dates):
            logger.info(f"  Phase 1: {i + 1}/{len(game_dates)} dates processed")

    logger.info(f"Phase 1 complete: {total_predictions} predictions across {len(date_predictions)} dates")
    return game_dates, prefetched_lines, actuals_df, voids_df, date_predictions, date_samples


# ---------------------------------------------------------------------------
# Per-config sweep execution (Phase 1.5 + 2 + metrics)
# ---------------------------------------------------------------------------

def run_single_config(
    config: SweepConfig,
    engine: object,
    feature_store: FeatureStore,
    model_pipeline: object,
    predictor: MonteCarloPredictor,
    game_dates: list[date],
    prefetched_lines: dict[date, pd.DataFrame],
    actuals_df: pd.DataFrame,
    voids_df: pd.DataFrame,
    date_predictions: dict[date, pd.DataFrame],
    date_samples: dict[date, dict],
    stats: list[str],
    starting_bankroll: float,
    bookmakers: list[str],
    allowed_bets: list[tuple[str, str]] | None,
    start_date: date,
    end_date: date,
    max_bet_pct: float | None = None,
) -> SweepResult:
    """Run edge calculation + simulation + metrics for one sweep config."""
    t0 = time.time()

    # Create StatConfigSet from sweep config (global values for all stats)
    stat_config = StatConfigSet(
        global_edge_threshold=config.edge_threshold,
        global_bl_tau=config.tau,
    )

    # Create BL blender for this config (used as fallback, but per-stat blenders are in harness)
    bl_blender = None
    if config.tau is not None:
        bl_blender = BlackLittermanBlender(BLConfig(tau=config.tau, z_max=config.z_max))

    # Create a lightweight harness to reuse _calculate_edges and _filter_best_bets
    config_harness = BacktestHarness(
        engine=engine,
        feature_store=feature_store,
        model_pipeline=model_pipeline,
        predictor=predictor,
        edge_threshold=config.edge_threshold,
        starting_bankroll=starting_bankroll,
        kelly_fraction=config.kelly_fraction,
        max_bet_pct=max_bet_pct,
        bookmakers=bookmakers,
        stats=stats,
        allowed_bets=allowed_bets,
        bl_blender=bl_blender,
        stat_config=stat_config,
    )

    # Phase 1.5: Calculate edges for each date with this config's blender
    all_predictions = []
    all_bookmaker_edges = []
    for game_date in game_dates:
        raw_preds = date_predictions.get(game_date)
        if raw_preds is None or raw_preds.empty:
            continue

        # CRITICAL: copy to avoid mutating cached data
        preds_df = raw_preds.copy()
        samples = date_samples.get(game_date, {})
        lines_df = prefetched_lines.get(game_date, pd.DataFrame())

        if len(lines_df) > 0:
            preds_df = config_harness._calculate_edges(preds_df, lines_df, samples)
            # Snapshot all bookmaker edges BEFORE line shopping
            all_bookmaker_edges.append(preds_df.copy())
            preds_df = config_harness._filter_best_bets(preds_df)

        all_predictions.append(preds_df)

    if all_predictions:
        predictions_df = pd.concat(all_predictions, ignore_index=True)
        predictions_df = predictions_df.sort_values(["game_date", "game_id", "player_id"])
    else:
        predictions_df = pd.DataFrame()

    if all_bookmaker_edges:
        all_edges_df = pd.concat(all_bookmaker_edges, ignore_index=True)
        all_edges_df = all_edges_df.sort_values(["game_date", "player_id", "stat", "bookmaker"])
    else:
        all_edges_df = pd.DataFrame()

    # Phase 2: Simulate bets with fresh simulator
    simulator = BetSimulator(
        edge_threshold=config.edge_threshold,
        starting_bankroll=starting_bankroll,
        kelly_fraction=config.kelly_fraction,
        max_bet_pct=max_bet_pct,
        allowed_bets=set(allowed_bets) if allowed_bets else None,
        stat_config=stat_config,
    )

    if not predictions_df.empty:
        sorted_dates = sorted(predictions_df["game_date"].unique())
        for sim_date in sorted_dates:
            if len(voids_df) > 0:
                simulator.resolve_voids(voids_df)
            if len(actuals_df) > 0:
                simulator.resolve_bets(actuals_df)
            day_preds = predictions_df[predictions_df["game_date"] == sim_date]
            simulator.evaluate_predictions(day_preds, sim_date)

    # Final resolution
    if len(voids_df) > 0:
        simulator.resolve_voids(voids_df)
    if len(actuals_df) > 0:
        simulator.resolve_bets(actuals_df)

    bets_df = simulator.to_dataframe()

    # Merge actuals into predictions
    if len(predictions_df) > 0 and len(actuals_df) > 0:
        predictions_df = config_harness._merge_actuals(predictions_df, actuals_df)

    # Calculate metrics
    metrics = MetricsCalculator().calculate(predictions_df, bets_df, starting_bankroll=starting_bankroll)

    elapsed = time.time() - t0

    return SweepResult(
        config=config,
        metrics=metrics,
        bets_df=bets_df,
        predictions_df=predictions_df,
        all_edges_df=all_edges_df,
        elapsed_seconds=elapsed,
    )


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def print_comparison_table(
    results: list[SweepResult],
    start_date: date,
    end_date: date,
    phase01_time: float,
    total_predictions: int,
    total_dates: int,
    starting_bankroll: float = 10000.0,
) -> None:
    """Print a formatted comparison table to console."""
    header = (
        f"\n{'=' * 100}\n"
        f"BACKTEST SWEEP  ({start_date} to {end_date})\n"
        f"Phase 0-1: {total_dates} dates, {total_predictions} predictions ({phase01_time:.1f}s)\n"
        f"Starting bankroll: ${starting_bankroll:,.0f}\n"
        f"{'=' * 100}\n"
    )
    print(header)

    # Column headers
    fmt = "{:>3}  {:<40} {:>5} {:>7} {:>8} {:>9} {:>7} {:>6} {:>6}"
    print(fmt.format("#", "Config", "Bets", "HitRate", "ROI", "Profit", "Sharpe", "MaxDD", "Time"))
    print(fmt.format("---", "-" * 40, "-----", "-------", "--------", "---------", "-------", "------", "------"))

    for i, r in enumerate(results, 1):
        m = r.metrics
        roi_str = f"{m.roi:+.2%}" if m.roi != 0 else "0.00%"
        profit_str = f"${m.total_profit:+,.0f}" if m.total_profit != 0 else "$0"
        print(fmt.format(
            i,
            r.config.label,
            m.total_bets,
            f"{m.hit_rate:.1%}",
            roi_str,
            profit_str,
            f"{m.sharpe_ratio:.2f}",
            f"{m.max_drawdown:.1%}",
            f"{r.elapsed_seconds:.1f}s",
        ))

    # Print per-stat breakdown for each config
    print(f"\n{'─' * 100}")
    print("PER-STAT BREAKDOWN")
    print(f"{'─' * 100}")
    stat_fmt = "{:>3}  {:<40} {:>12} {:>12} {:>12}"
    stat_labels = []
    for r in results:
        if r.metrics.by_stat:
            stat_labels = list(r.metrics.by_stat.keys())
            break

    if stat_labels:
        header_parts = [f"{s.upper():>12}" for s in stat_labels]
        print(f"{'#':>3}  {'Config':<40} " + " ".join(header_parts))
        print(f"{'---':>3}  {'-' * 40} " + " ".join(["-" * 12] * len(stat_labels)))

        for i, r in enumerate(results, 1):
            parts = []
            for s in stat_labels:
                stat_data = r.metrics.by_stat.get(s, {})
                roi = stat_data.get("roi", 0)
                bets = stat_data.get("bets", 0)
                parts.append(f"{roi:+.1%}({bets})")
            print(f"{i:>3}  {r.config.label:<40} " + " ".join(f"{p:>12}" for p in parts))

    # Summary
    print(f"\n{'=' * 100}")
    if results:
        best_roi = max(results, key=lambda r: r.metrics.roi)
        best_sharpe = max(results, key=lambda r: r.metrics.sharpe_ratio)
        most_bets = max(results, key=lambda r: r.metrics.total_bets)

        best_roi_idx = results.index(best_roi) + 1
        best_sharpe_idx = results.index(best_sharpe) + 1
        most_bets_idx = results.index(most_bets) + 1

        print(f"Best ROI:    #{best_roi_idx} ({best_roi.config.label}) = {best_roi.metrics.roi:+.2%}")
        print(f"Best Sharpe: #{best_sharpe_idx} ({best_sharpe.config.label}) = {best_sharpe.metrics.sharpe_ratio:.2f}")
        print(f"Most bets:   #{most_bets_idx} ({most_bets.config.label}) = {most_bets.metrics.total_bets}")
    print(f"{'=' * 100}\n")


def _config_dir_name(idx: int, config: SweepConfig) -> str:
    """Generate a filesystem-safe directory name for a sweep config."""
    if config.tau is None:
        return f"config_{idx:02d}_no_BL_edge{config.edge_threshold}_kelly{config.kelly_fraction}"
    return f"config_{idx:02d}_tau{config.tau}_zmax{config.z_max}_edge{config.edge_threshold}_kelly{config.kelly_fraction}"


def save_results(
    results: list[SweepResult],
    output_dir: Path,
    start_date: date,
    end_date: date,
    phase01_time: float,
    total_predictions: int,
    total_dates: int,
    sweep_grid_size: int,
    starting_bankroll: float = 10000.0,
    bookmakers: list[str] | None = None,
    stats: list[str] | None = None,
    allowed_bets: list[tuple[str, str]] | None = None,
    max_bet_pct: float | None = None,
) -> None:
    """Save sweep results to JSON, CSV, and per-config subdirectories.

    Each config gets a subdirectory with bets.csv, predictions.csv,
    metrics.json, and all_bookmaker_edges.csv — compatible with
    visualize_results.py.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    total_sweep_time = sum(r.elapsed_seconds for r in results) + phase01_time

    # JSON with full metrics per config
    json_output = {
        "sweep_metadata": {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "game_dates": total_dates,
            "total_predictions": total_predictions,
            "total_configs": sweep_grid_size,
            "phase01_time_seconds": round(phase01_time, 1),
            "total_sweep_time_seconds": round(total_sweep_time, 1),
        },
        "results": [],
    }

    csv_rows = []
    config_dirs = []

    for i, r in enumerate(results, 1):
        m = r.metrics

        json_output["results"].append({
            "config": r.config.to_dict(),
            "metrics": m.to_dict(),
            "elapsed_seconds": round(r.elapsed_seconds, 2),
        })

        row = {
            "tau": r.config.tau,
            "z_max": r.config.z_max,
            "edge_threshold": r.config.edge_threshold,
            "kelly_fraction": r.config.kelly_fraction,
            "total_bets": m.total_bets,
            "wins": m.wins,
            "losses": m.losses,
            "pushes": m.pushes,
            "hit_rate": round(m.hit_rate, 4),
            "roi": round(m.roi, 4),
            "return_on_capital": round(m.return_on_capital, 4),
            "total_profit": round(m.total_profit, 2),
            "total_staked": round(m.total_staked, 2),
            "sharpe_ratio": round(m.sharpe_ratio, 3),
            "max_drawdown": round(m.max_drawdown, 4),
            "elapsed_seconds": round(r.elapsed_seconds, 2),
        }

        # Add per-stat ROI columns
        for stat, stat_data in m.by_stat.items():
            row[f"{stat}_bets"] = stat_data.get("bets", 0)
            row[f"{stat}_roi"] = round(stat_data.get("roi", 0), 4)
            row[f"{stat}_hit_rate"] = round(stat_data.get("hit_rate", 0), 4)

        csv_rows.append(row)

        # ── Per-config subdirectory (visualize_results.py compatible) ──
        dir_name = _config_dir_name(i, r.config)
        config_dir = output_dir / dir_name
        config_dir.mkdir(parents=True, exist_ok=True)
        config_dirs.append(dir_name)

        # bets.csv
        if not r.bets_df.empty:
            r.bets_df.to_csv(config_dir / "bets.csv", index=False)
        else:
            pd.DataFrame().to_csv(config_dir / "bets.csv", index=False)

        # predictions.csv
        if not r.predictions_df.empty:
            r.predictions_df.to_csv(config_dir / "predictions.csv", index=False)

        # all_bookmaker_edges.csv
        if not r.all_edges_df.empty:
            r.all_edges_df.to_csv(config_dir / "all_bookmaker_edges.csv", index=False)

        # metrics.json — matches BacktestResult.to_csv format
        metrics_output = m.to_dict()
        metrics_output["config"] = {
            "edge_threshold": r.config.edge_threshold,
            "starting_bankroll": starting_bankroll,
            "kelly_fraction": r.config.kelly_fraction,
            "max_bet_pct": max_bet_pct,
            "bl_tau": r.config.tau,
            "bl_z_max": r.config.z_max,
            "bookmakers": bookmakers or [],
            "stats": stats or [],
            "allowed_bets": (
                [f"{s}:{side}" for s, side in allowed_bets] if allowed_bets else None
            ),
        }
        with open(config_dir / "metrics.json", "w") as f:
            json.dump(metrics_output, f, indent=2, default=str)

    # Write sweep-level JSON
    json_path = output_dir / "sweep_results.json"
    with open(json_path, "w") as f:
        json.dump(json_output, f, indent=2, default=str)
    logger.info(f"Saved detailed results to {json_path}")

    # Write CSV summary
    csv_path = output_dir / "sweep_summary.csv"
    pd.DataFrame(csv_rows).to_csv(csv_path, index=False)
    logger.info(f"Saved summary CSV to {csv_path}")

    # Log per-config directories
    for dir_name in config_dirs:
        logger.info(f"  Config dir: {output_dir / dir_name}")
    logger.info(
        f"Per-config dashboards: run  "
        f"python src/backtesting/visualize_results.py --results-dir <config_dir>"
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def find_latest_model_dir(base_dir: str) -> Path:
    """Find the most recent COMPLETE run_* directory in the artifacts folder.

    A directory is considered complete if it contains minutes_model.joblib.
    Incomplete training runs (config only) are skipped.
    """
    base = Path(base_dir)
    if not base.exists():
        raise FileNotFoundError(f"Artifacts directory not found: {base}")

    # Check if base_dir itself is a model directory
    if (base / "minutes_model.joblib").exists():
        return base

    # Find all run_* directories, sorted newest first
    runs = sorted(
        [d for d in base.iterdir() if d.is_dir() and d.name.startswith("run_")],
        reverse=True,
    )

    if not runs:
        raise FileNotFoundError(f"No run_* directories found in {base}")

    # Find first complete run (has minutes_model.joblib)
    for run_dir in runs:
        if (run_dir / "minutes_model.joblib").exists():
            return run_dir
        logger.warning(f"Skipping incomplete model directory: {run_dir.name}")

    raise FileNotFoundError(
        f"No complete model artifacts found in {base}. "
        f"Checked {len(runs)} directories but none contained minutes_model.joblib."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Backtest Parameter Sweep — load data once, sweep configs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # BL tau sweep (default)
  python src/backtesting/run_sweep.py --start 2026-01-01 --end 2026-01-29

  # Custom tau + edge sweep
  python src/backtesting/run_sweep.py --start 2026-01-01 --end 2026-01-29 \\
      --tau none 0.03 0.05 0.10 --edge 0.03 0.05 0.07

  # Sweep z_max (confidence sensitivity)
  python src/backtesting/run_sweep.py --start 2026-01-01 --end 2026-01-29 \\
      --tau 0.10 --z-max 0.5 1.0 2.0 --edge 0.05

  # Full grid sweep
  python src/backtesting/run_sweep.py --start 2026-01-01 --end 2026-01-29 \\
      --tau none 0.05 0.10 --edge 0.03 0.05 0.08 --kelly 0.10 0.125 0.15
        """,
    )

    # Required
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")

    # Sweep grid parameters
    parser.add_argument(
        "--tau", type=str, nargs="+",
        default=["none", "0.03", "0.05", "0.09", "0.15", "0.25"],
        help="BL tau values to sweep. Use 'none' for no-BL baseline. (default: none 0.03 0.05 0.09 0.15 0.25)",
    )
    parser.add_argument(
        "--edge", type=float, nargs="+",
        default=[0.05],
        help="Edge threshold values to sweep (default: 0.05)",
    )
    parser.add_argument(
        "--kelly", type=float, nargs="+",
        default=[0.125],
        help="Kelly fraction values to sweep (default: 0.125)",
    )
    parser.add_argument(
        "--z-max", type=float, nargs="+",
        default=[1.0],
        help="BL z_max values to sweep (confidence saturation point). Lower=more aggressive. (default: 1.0)",
    )

    # Model / data config (mirrors run_backtest.py)
    parser.add_argument("--model-dir", type=str, default="src/models/artifacts", help="Path to model artifacts")
    parser.add_argument("--n-samples", type=int, default=5000, help="Monte Carlo samples")
    parser.add_argument("--stats", nargs="+", default=["pts", "reb", "ast"], help="Stats to predict")
    parser.add_argument("--starting-bankroll", type=float, default=10000.0, help="Starting bankroll")
    parser.add_argument(
        "--max-bet-pct",
        type=float,
        default=None,
        help="Maximum bet size as %% of bankroll (e.g., 0.025 = 2.5%%). Caps Kelly sizing. Default: no cap.",
    )
    parser.add_argument(
        "--bookmakers", nargs="+",
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
        help="Bookmakers to shop lines from",
    )
    parser.add_argument(
        "--allowed-bets", nargs="+", default=None,
        help="Stat:side pairs to allow (e.g., pts:under reb:over)",
    )
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory")

    args = parser.parse_args()

    # Parse tau values ("none" -> None, else float)
    tau_values: list[float | None] = []
    for v in args.tau:
        if v.lower() == "none":
            tau_values.append(None)
        else:
            try:
                tau_values.append(float(v))
            except ValueError:
                parser.error(f"Invalid --tau value '{v}'. Use 'none' or a float (e.g., 0.05).")

    # Parse allowed_bets
    allowed_bets = None
    if args.allowed_bets:
        allowed_bets = []
        for pair in args.allowed_bets:
            parts = pair.lower().split(":")
            if len(parts) != 2 or parts[1] not in ("over", "under"):
                parser.error(f"Invalid --allowed-bets value '{pair}'. Use format stat:side (e.g., pts:under)")
            allowed_bets.append((parts[0], parts[1]))

    # Parse dates
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    # Build sweep grid
    configs = build_sweep_grid(tau_values, args.edge, args.kelly, args.z_max)
    logger.info(f"Sweep grid: {len(configs)} configurations")
    for i, c in enumerate(configs, 1):
        logger.info(f"  Config {i}: {c.label}")

    # Output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("backtest_results") / f"sweep_{timestamp}"

    # Initialize shared components
    engine = get_engine()
    feature_store = FeatureStore(engine)

    model_path = find_latest_model_dir(args.model_dir)
    logger.info(f"Using model artifacts from: {model_path}")

    pipeline = PlayerPropsModelPipeline.load_all(str(model_path), feature_store)

    copula_params = load_copula_params(str(model_path))
    if copula_params:
        logger.info(f"Loaded Gaussian copula params: { {k: f'{v:.4f}' for k, v in copula_params.items()} }")
    else:
        logger.info("No copula_params.json found, using legacy correlation adjustment")

    predictor = MonteCarloPredictor(pipeline, n_samples=args.n_samples, copula_params=copula_params)

    # Create a loader harness for shared data fetching
    loader_harness = BacktestHarness(
        engine=engine,
        feature_store=feature_store,
        model_pipeline=pipeline,
        predictor=predictor,
        stats=args.stats,
        bookmakers=args.bookmakers,
    )

    # Phase 0 + 1: Load data and generate predictions (once)
    logger.info("=" * 60)
    logger.info("PHASE 0-1: Loading shared data and generating predictions...")
    logger.info("=" * 60)
    t_shared = time.time()

    game_dates, prefetched_lines, actuals_df, voids_df, date_predictions, date_samples = run_shared_phases(
        harness=loader_harness,
        feature_store=feature_store,
        predictor=predictor,
        start_date=start_date,
        end_date=end_date,
        stats=args.stats,
    )

    phase01_time = time.time() - t_shared
    total_predictions = sum(len(df) for df in date_predictions.values())
    logger.info(f"Phase 0-1 complete in {phase01_time:.1f}s")

    # Sweep loop
    logger.info("=" * 60)
    logger.info(f"SWEEP: Running {len(configs)} configurations...")
    logger.info("=" * 60)

    results: list[SweepResult] = []
    for i, config in enumerate(configs, 1):
        logger.info(f"Config {i}/{len(configs)}: {config.label}")

        result = run_single_config(
            config=config,
            engine=engine,
            feature_store=feature_store,
            model_pipeline=pipeline,
            predictor=predictor,
            game_dates=game_dates,
            prefetched_lines=prefetched_lines,
            actuals_df=actuals_df,
            voids_df=voids_df,
            date_predictions=date_predictions,
            date_samples=date_samples,
            stats=args.stats,
            starting_bankroll=args.starting_bankroll,
            bookmakers=args.bookmakers,
            allowed_bets=allowed_bets,
            start_date=start_date,
            end_date=end_date,
            max_bet_pct=args.max_bet_pct,
        )

        results.append(result)
        m = result.metrics
        logger.info(
            f"  -> {m.total_bets} bets, "
            f"HitRate={m.hit_rate:.1%}, "
            f"ROI={m.roi:+.2%}, "
            f"Sharpe={m.sharpe_ratio:.2f} "
            f"({result.elapsed_seconds:.1f}s)"
        )

    # Output
    print_comparison_table(
        results,
        start_date=start_date,
        end_date=end_date,
        phase01_time=phase01_time,
        total_predictions=total_predictions,
        total_dates=len(date_predictions),
        starting_bankroll=args.starting_bankroll,
    )

    save_results(
        results,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        phase01_time=phase01_time,
        total_predictions=total_predictions,
        total_dates=len(date_predictions),
        sweep_grid_size=len(configs),
        starting_bankroll=args.starting_bankroll,
        bookmakers=args.bookmakers,
        stats=args.stats,
        allowed_bets=allowed_bets,
        max_bet_pct=args.max_bet_pct,
    )

    total_time = phase01_time + sum(r.elapsed_seconds for r in results)
    logger.info(f"Total sweep time: {total_time:.1f}s (Phase 0-1: {phase01_time:.1f}s, Configs: {total_time - phase01_time:.1f}s)")
    logger.info(f"Results saved to: {output_dir}")


if __name__ == "__main__":
    main()
