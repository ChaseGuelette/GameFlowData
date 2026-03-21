"""
MLB Backtest Parameter Sweep.

Runs Phase 0-1 (data loading + MC predictions) once, then sweeps
(tau, edge_threshold, kelly_fraction) configs through edge calculation
+ bet simulation + metrics. Avoids redundant DB queries and XGBoost calls.

Usage:
    python src/backtesting/mlb/run_mlb_sweep.py --start 2025-07-01 --end 2025-09-28

    python src/backtesting/mlb/run_mlb_sweep.py --start 2025-07-01 --end 2025-09-28 \
        --tau none 0.03 0.05 0.10 0.25

    python src/backtesting/mlb/run_mlb_sweep.py --start 2025-07-01 --end 2025-09-28 \
        --tau none 0.05 0.10 --edge 0.05 0.08 0.10 --kelly 0.10 0.125 0.15
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
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.backtesting.bet_simulator import BetSimulator
from src.backtesting.mlb.mlb_backtest_harness import STAT_ACTUALS
from src.backtesting.performance_metrics import MetricsCalculator, PerformanceMetrics
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
logger = logging.getLogger("MLBBacktestSweep")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SweepConfig:
    """One point in the parameter sweep grid."""

    tau: float | None  # None = no BL blending (baseline)
    edge_threshold: float
    kelly_fraction: float
    z_max: float = 1.0

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
    if z_max_values is None:
        z_max_values = [1.0]

    configs = []
    for tau, edge, kelly, z_max in itertools.product(tau_values, edge_thresholds, kelly_fractions, z_max_values):
        if tau is None and z_max != z_max_values[0]:
            continue
        configs.append(SweepConfig(tau=tau, edge_threshold=edge, kelly_fraction=kelly, z_max=z_max))
    return configs


# ---------------------------------------------------------------------------
# Shared data structures for cached predictions
# ---------------------------------------------------------------------------

@dataclass
class DatePrediction:
    """Cached prediction for one pitcher on one date (pre-edge)."""

    game_date: date
    player_id: int
    game_id: int
    team_id: int
    opponent_id: int
    stat: str
    model_type: str
    pred_mean: float
    pred_median: float
    pred_q10: float
    pred_q25: float
    pred_q50: float
    pred_q75: float
    pred_q90: float
    samples: np.ndarray  # MC samples, used for edge calc per config


# ---------------------------------------------------------------------------
# Phase 0 + 1: Shared data loading and prediction generation
# ---------------------------------------------------------------------------

def run_shared_phases(
    engine,
    feature_store: MLBFeatureStore,
    predictor: MLBMonteCarloPredictor,
    start_date: date,
    end_date: date,
    stats: list[str],
    bookmakers: list[str],
) -> tuple[
    list[date],
    dict[date, list[DatePrediction]],
    dict[date, pd.DataFrame],
    dict[date, dict[tuple[int, str], float]],
]:
    """Execute Phase 0 (DB fetch) and Phase 1 (MC predictions) once.

    Returns:
        game_dates: sorted dates with games
        date_predictions: dict[date, list[DatePrediction]] - cached raw predictions
        date_lines: dict[date, DataFrame] - prop lines per date
        date_actuals: dict[date, dict[(player_id, stat) -> float]] - actuals
    """
    # Phase 0a: Get game dates
    logger.info("Phase 0: Fetching game dates...")
    query = text("""
        SELECT DISTINCT game_date
        FROM mlb_game_schedule
        WHERE game_date BETWEEN :start_date AND :end_date
          AND status != 'Cancelled'
        ORDER BY game_date
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"start_date": start_date, "end_date": end_date})
        game_dates = [row[0] for row in result]
    logger.info(f"  Found {len(game_dates)} dates with games")

    # Phase 0b: Prefetch all actuals
    logger.info("Phase 0: Fetching actuals...")
    date_actuals: dict[date, dict[tuple[int, str], float]] = {}
    for stat, (table, column) in STAT_ACTUALS.items():
        if stat not in stats:
            continue
        q = text(f"""
            SELECT game_date, player_id, {column} as actual_value
            FROM {table}
            WHERE game_date BETWEEN :start_date AND :end_date
              AND did_not_play IS NOT TRUE
              AND {column} IS NOT NULL
        """)
        with engine.connect() as conn:
            for row in conn.execute(q, {"start_date": start_date, "end_date": end_date}):
                gd = row[0]
                if gd not in date_actuals:
                    date_actuals[gd] = {}
                date_actuals[gd][(int(row[1]), stat)] = float(row[2])
    total_actuals = sum(len(v) for v in date_actuals.values())
    logger.info(f"  Prefetched {total_actuals} actuals across {len(date_actuals)} dates")

    # Phase 1: Generate predictions date-by-date
    logger.info("Phase 1: Generating predictions...")
    date_predictions: dict[date, list[DatePrediction]] = {}
    date_lines: dict[date, pd.DataFrame] = {}
    total_preds = 0

    for i, game_date in enumerate(game_dates):
        try:
            preds, lines = _process_date_shared(
                engine, feature_store, predictor, game_date, stats, bookmakers,
            )
            if preds:
                date_predictions[game_date] = preds
                total_preds += len(preds)
            if lines is not None and not lines.empty:
                date_lines[game_date] = lines
        except Exception as e:
            logger.error(f"  Error processing {game_date}: {e}")
            continue

        if (i + 1) % 10 == 0 or (i + 1) == len(game_dates):
            logger.info(f"  Phase 1: {i + 1}/{len(game_dates)} dates, {total_preds} predictions so far")

    logger.info(f"Phase 1 complete: {total_preds} predictions across {len(date_predictions)} dates")
    return game_dates, date_predictions, date_lines, date_actuals


def _process_date_shared(
    engine,
    feature_store: MLBFeatureStore,
    predictor: MLBMonteCarloPredictor,
    game_date: date,
    stats: list[str],
    bookmakers: list[str],
) -> tuple[list[DatePrediction], pd.DataFrame | None]:
    """Generate predictions + fetch lines for a single date."""
    # Get games
    query = text("""
        SELECT s.game_id, s.home_team_id, s.away_team_id,
               s.probable_pitcher_home_id, s.probable_pitcher_away_id
        FROM mlb_game_schedule s
        WHERE s.game_date = :game_date
          AND s.status != 'Cancelled'
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"game_date": game_date})
        games = [dict(row._mapping) for row in result]

    if not games:
        return [], None

    # Extract pitchers
    pitchers = []
    for game in games:
        for side, opp_side, is_home in [("home", "away", True), ("away", "home", False)]:
            pitcher_id = game.get(f"probable_pitcher_{side}_id")
            if pitcher_id:
                pitchers.append({
                    "player_id": int(pitcher_id),
                    "game_id": int(game["game_id"]),
                    "team_id": game[f"{side}_team_id"],
                    "opponent_id": game[f"{opp_side}_team_id"],
                    "is_home": is_home,
                })

    # Generate predictions
    predictions = []
    pitcher_stats = [s for s in stats if s.startswith("pitcher_")]

    if pitcher_stats and pitchers:
        for pitcher in pitchers:
            try:
                features = feature_store.get_player_game_features(
                    player_id=pitcher["player_id"],
                    game_id=pitcher["game_id"],
                    as_of_date=game_date,
                    team_id=pitcher["team_id"],
                    opponent_id=pitcher["opponent_id"],
                    is_home=pitcher["is_home"],
                )
                if features is None:
                    continue

                pred = predictor.predict(
                    player_id=pitcher["player_id"],
                    game_id=pitcher["game_id"],
                    features=features,
                )

                predictions.append(DatePrediction(
                    game_date=game_date,
                    player_id=pred.player_id,
                    game_id=int(pred.game_id),
                    team_id=pitcher["team_id"],
                    opponent_id=pitcher["opponent_id"],
                    stat=pred.stat,
                    model_type="quantile",
                    pred_mean=pred.mean,
                    pred_median=pred.median,
                    pred_q10=pred.q10,
                    pred_q25=pred.q25,
                    pred_q50=pred.q50,
                    pred_q75=pred.q75,
                    pred_q90=pred.q90,
                    samples=pred.samples,
                ))
            except Exception as e:
                logger.debug(f"Error predicting pitcher {pitcher['player_id']}: {e}")

    # Fetch lines for all players on this date
    game_ids = [g["game_id"] for g in games]
    market_keys = [s for s in stats if s in STAT_ACTUALS]
    lines_df = _fetch_lines_for_date(engine, game_ids, market_keys, bookmakers)

    return predictions, lines_df


def _fetch_lines_for_date(
    engine, game_ids: list[int], market_keys: list[str], bookmakers: list[str],
) -> pd.DataFrame:
    """Fetch all prop lines for a set of games."""
    if not game_ids or not market_keys:
        return pd.DataFrame()

    # Build parameterized query
    game_id_placeholders = ", ".join(f":gid_{i}" for i in range(len(game_ids)))
    market_placeholders = ", ".join(f":mk_{i}" for i in range(len(market_keys)))
    book_placeholders = ", ".join(f":bk_{i}" for i in range(len(bookmakers)))

    params = {}
    for i, gid in enumerate(game_ids):
        params[f"gid_{i}"] = gid
    for i, mk in enumerate(market_keys):
        params[f"mk_{i}"] = mk
    for i, bk in enumerate(bookmakers):
        params[f"bk_{i}"] = bk

    query = text(f"""
        WITH ranked AS (
            SELECT
                player_id, game_id, bookmaker, market_key, line,
                MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
                MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
            FROM mlb_raw_player_props
            WHERE game_id IN ({game_id_placeholders})
              AND market_key IN ({market_placeholders})
              AND bookmaker IN ({book_placeholders})
              AND player_id IS NOT NULL
            GROUP BY player_id, game_id, bookmaker, market_key, line
            HAVING MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) IS NOT NULL
               AND MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) IS NOT NULL
        )
        SELECT * FROM ranked
    """)

    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


# ---------------------------------------------------------------------------
# Edge calculation helpers
# ---------------------------------------------------------------------------

def _odds_to_prob(odds: float) -> float:
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def _select_sharpest_line(lines: pd.DataFrame, player_id: int, market_key: str) -> dict | None:
    """Find the lowest-vig line for a player/market from all bookmakers."""
    mask = (lines["player_id"] == player_id) & (lines["market_key"] == market_key)
    player_lines = lines[mask]

    if player_lines.empty:
        return None

    best_line = None
    best_booksum = 999.0

    for _, row in player_lines.iterrows():
        over_odds = row["over_odds"]
        under_odds = row["under_odds"]
        if pd.isna(over_odds) or pd.isna(under_odds):
            continue

        booksum = _odds_to_prob(over_odds) + _odds_to_prob(under_odds)
        if booksum < best_booksum:
            best_booksum = booksum
            best_line = {
                "line": row["line"],
                "over_odds": over_odds,
                "under_odds": under_odds,
                "bookmaker": row["bookmaker"],
            }

    return best_line


def compute_edges_for_config(
    predictions: list[DatePrediction],
    lines_df: pd.DataFrame | None,
    bl_blender: BlackLittermanBlender | None,
    actuals: dict[tuple[int, str], float] | None,
) -> list[dict]:
    """Calculate edges for predictions using a specific BL config."""
    results = []

    for pred in predictions:
        row = {
            "game_date": pred.game_date,
            "player_id": pred.player_id,
            "game_id": pred.game_id,
            "team_id": pred.team_id,
            "opponent_id": pred.opponent_id,
            "stat": pred.stat,
            "model_type": pred.model_type,
            "pred_mean": pred.pred_mean,
            "pred_median": pred.pred_median,
            "pred_q10": pred.pred_q10,
            "pred_q25": pred.pred_q25,
            "pred_q50": pred.pred_q50,
            "pred_q75": pred.pred_q75,
            "pred_q90": pred.pred_q90,
        }

        # Find best line
        line_info = None
        if lines_df is not None and not lines_df.empty:
            line_info = _select_sharpest_line(lines_df, pred.player_id, pred.stat)

        if line_info is None:
            results.append(row)
            continue

        line_val = line_info["line"]
        over_odds = line_info["over_odds"]
        under_odds = line_info["under_odds"]
        samples = pred.samples

        # Empirical CDF from MC samples
        over_prob = float((samples > line_val).mean())
        over_prob = min(max(over_prob, 0.05), 0.95)
        under_prob = 1 - over_prob

        # Devig
        raw_over = _odds_to_prob(over_odds)
        raw_under = _odds_to_prob(under_odds)
        booksum = raw_over + raw_under
        implied_over = raw_over / booksum
        implied_under = raw_under / booksum

        row["line"] = line_val
        row["over_odds"] = over_odds
        row["under_odds"] = under_odds
        row["bookmaker"] = line_info["bookmaker"]
        row["over_prob"] = over_prob
        row["under_prob"] = under_prob
        row["implied_over"] = implied_over
        row["implied_under"] = implied_under
        row["over_edge"] = over_prob - implied_over
        row["under_edge"] = under_prob - implied_under

        # BL blending
        if bl_blender is not None:
            bl_result = bl_blender.blend_prediction(
                samples=samples,
                line=line_val,
                over_odds=over_odds,
                under_odds=under_odds,
            )
            row["bl_over_prob"] = bl_result["posterior_over"]
            row["bl_under_prob"] = bl_result["posterior_under"]
            row["bl_over_edge"] = bl_result["posterior_over"] - implied_over
            row["bl_under_edge"] = bl_result["posterior_under"] - implied_under

        # Attach actual
        if actuals:
            actual = actuals.get((pred.player_id, pred.stat))
            if actual is not None:
                row["actual"] = actual

        results.append(row)

    return results


# ---------------------------------------------------------------------------
# Per-config sweep execution
# ---------------------------------------------------------------------------

def run_single_config(
    config: SweepConfig,
    game_dates: list[date],
    date_predictions: dict[date, list[DatePrediction]],
    date_lines: dict[date, pd.DataFrame],
    date_actuals: dict[date, dict[tuple[int, str], float]],
    starting_bankroll: float,
    max_bet_pct: float | None = None,
    flat_bet_size: float | None = None,
) -> SweepResult:
    """Run edge calculation + bet simulation + metrics for one config."""
    t0 = time.time()

    # Create BL blender for this config
    bl_blender = None
    if config.tau is not None:
        bl_blender = BlackLittermanBlender(BLConfig(tau=config.tau, z_max=config.z_max))

    # Create simulator
    simulator = BetSimulator(
        edge_threshold=config.edge_threshold,
        starting_bankroll=starting_bankroll,
        kelly_fraction=config.kelly_fraction,
        max_bet_pct=max_bet_pct,
        flat_bet_size=flat_bet_size,
    )

    all_prediction_rows = []

    for game_date in game_dates:
        preds = date_predictions.get(game_date)
        if not preds:
            continue

        lines = date_lines.get(game_date)
        actuals = date_actuals.get(game_date, {})

        # Compute edges with this config's BL blender
        day_rows = compute_edges_for_config(preds, lines, bl_blender, actuals)
        if not day_rows:
            continue

        day_df = pd.DataFrame(day_rows)
        all_prediction_rows.append(day_df)

        # Feed to simulator
        simulator.evaluate_predictions(day_df, game_date)

    # Resolve bets with actuals
    if all_prediction_rows:
        predictions_df = pd.concat(all_prediction_rows, ignore_index=True)
    else:
        predictions_df = pd.DataFrame()

    bets_df = simulator.to_dataframe()
    metrics = MetricsCalculator().calculate(predictions_df, bets_df, starting_bankroll=starting_bankroll)

    elapsed = time.time() - t0

    return SweepResult(
        config=config,
        metrics=metrics,
        bets_df=bets_df,
        predictions_df=predictions_df,
        elapsed_seconds=elapsed,
    )


# ---------------------------------------------------------------------------
# Output
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
    header = (
        f"\n{'=' * 100}\n"
        f"MLB BACKTEST SWEEP  ({start_date} to {end_date})\n"
        f"Phase 0-1: {total_dates} dates, {total_predictions} predictions ({phase01_time:.1f}s)\n"
        f"Starting bankroll: ${starting_bankroll:,.0f}\n"
        f"{'=' * 100}\n"
    )
    print(header)

    fmt = "{:>3}  {:<45} {:>5} {:>7} {:>8} {:>9} {:>7} {:>6}"
    print(fmt.format("#", "Config", "Bets", "HitRate", "ROI", "Profit", "Sharpe", "Time"))
    print(fmt.format("---", "-" * 45, "-----", "-------", "--------", "---------", "-------", "------"))

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
            f"{r.elapsed_seconds:.1f}s",
        ))

    print(f"\n{'=' * 100}")
    if results:
        best_roi = max(results, key=lambda r: r.metrics.roi)
        best_sharpe = max(results, key=lambda r: r.metrics.sharpe_ratio)
        best_roi_idx = results.index(best_roi) + 1
        best_sharpe_idx = results.index(best_sharpe) + 1
        print(f"Best ROI:    #{best_roi_idx} ({best_roi.config.label}) = {best_roi.metrics.roi:+.2%}")
        print(f"Best Sharpe: #{best_sharpe_idx} ({best_sharpe.config.label}) = {best_sharpe.metrics.sharpe_ratio:.2f}")
    print(f"{'=' * 100}\n")


def save_results(
    results: list[SweepResult],
    output_dir: Path,
    start_date: date,
    end_date: date,
    phase01_time: float,
    total_predictions: int,
    total_dates: int,
    starting_bankroll: float = 10000.0,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    json_output = {
        "sweep_metadata": {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "game_dates": total_dates,
            "total_predictions": total_predictions,
            "total_configs": len(results),
            "phase01_time_seconds": round(phase01_time, 1),
        },
        "results": [],
    }

    csv_rows = []

    for i, r in enumerate(results, 1):
        m = r.metrics

        json_output["results"].append({
            "config": r.config.to_dict(),
            "metrics": m.to_dict(),
            "elapsed_seconds": round(r.elapsed_seconds, 2),
        })

        csv_rows.append({
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
            "total_profit": round(m.total_profit, 2),
            "total_staked": round(m.total_staked, 2),
            "sharpe_ratio": round(m.sharpe_ratio, 3),
            "max_drawdown": round(m.max_drawdown, 4),
            "elapsed_seconds": round(r.elapsed_seconds, 2),
        })

        # Per-config subdirectory
        tau_label = "no_BL" if r.config.tau is None else f"tau{r.config.tau}"
        dir_name = f"config_{i:02d}_{tau_label}_edge{r.config.edge_threshold}_kelly{r.config.kelly_fraction}"
        config_dir = output_dir / dir_name
        config_dir.mkdir(parents=True, exist_ok=True)

        if not r.bets_df.empty:
            r.bets_df.to_csv(config_dir / "bets.csv", index=False)
        if not r.predictions_df.empty:
            r.predictions_df.to_csv(config_dir / "predictions.csv", index=False)

        metrics_output = m.to_dict()
        metrics_output["config"] = r.config.to_dict()
        with open(config_dir / "metrics.json", "w") as f:
            json.dump(metrics_output, f, indent=2, default=str)

    with open(output_dir / "sweep_results.json", "w") as f:
        json.dump(json_output, f, indent=2, default=str)

    pd.DataFrame(csv_rows).to_csv(output_dir / "sweep_summary.csv", index=False)
    logger.info(f"Results saved to {output_dir}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def find_latest_model_dir(base_dir: str) -> Path:
    base = Path(base_dir)
    if not base.exists():
        raise FileNotFoundError(f"Artifacts directory not found: {base}")

    if (base / "pitcher_k_model.joblib").exists():
        return base
    if (base / "production" / "pitcher_k_model.joblib").exists():
        return base / "production"

    runs = sorted([
        d for d in base.iterdir()
        if d.is_dir() and d.name.startswith("mlb_run_") and not d.name.endswith("_incomplete")
    ])
    if not runs:
        raise FileNotFoundError(f"No mlb_run_* directories found in {base}")
    return runs[-1]


def main():
    parser = argparse.ArgumentParser(
        description="MLB Backtest Parameter Sweep",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")

    # Sweep grid
    parser.add_argument(
        "--tau", type=str, nargs="+",
        default=["none", "0.03", "0.05", "0.10", "0.25"],
        help="BL tau values. Use 'none' for no-BL baseline.",
    )
    parser.add_argument("--edge", type=float, nargs="+", default=[0.05, 0.08, 0.10])
    parser.add_argument("--kelly", type=float, nargs="+", default=[0.125])
    parser.add_argument("--z-max", type=float, nargs="+", default=[1.0])

    # Model / data
    parser.add_argument("--model-dir", type=str, default="src/models/mlb/artifacts")
    parser.add_argument("--n-samples", type=int, default=5000, help="Monte Carlo samples")
    parser.add_argument("--stats", nargs="+", default=["pitcher_strikeouts"])
    parser.add_argument("--starting-bankroll", type=float, default=10000.0)
    parser.add_argument("--max-bet-pct", type=float, default=None)
    parser.add_argument("--flat-bet", type=float, default=None)
    parser.add_argument(
        "--bookmakers", nargs="+",
        default=[
            "draftkings", "fanduel", "betmgm", "betrivers", "bovada",
            "williamhill_us", "betonlineag", "fanatics",
        ],
    )
    parser.add_argument("--output-dir", type=str, default=None)

    args = parser.parse_args()

    # Parse tau values
    tau_values: list[float | None] = []
    for v in args.tau:
        if v.lower() == "none":
            tau_values.append(None)
        else:
            tau_values.append(float(v))

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    configs = build_sweep_grid(tau_values, args.edge, args.kelly, args.z_max)
    logger.info(f"Sweep grid: {len(configs)} configurations")

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("backtest_results") / f"mlb_sweep_{timestamp}"

    # Initialize
    engine = get_engine()
    feature_store = MLBFeatureStore(engine)

    model_path = find_latest_model_dir(args.model_dir)
    logger.info(f"Using model: {model_path}")

    pipeline = MLBPitcherKPipeline.load(str(model_path))
    predictor = MLBMonteCarloPredictor(pipeline, n_samples=args.n_samples)

    # Phase 0 + 1
    logger.info("=" * 60)
    logger.info("PHASE 0-1: Loading data and generating predictions...")
    logger.info("=" * 60)
    t_shared = time.time()

    game_dates, date_predictions, date_lines, date_actuals = run_shared_phases(
        engine=engine,
        feature_store=feature_store,
        predictor=predictor,
        start_date=start_date,
        end_date=end_date,
        stats=args.stats,
        bookmakers=args.bookmakers,
    )

    phase01_time = time.time() - t_shared
    total_predictions = sum(len(preds) for preds in date_predictions.values())

    # Sweep
    logger.info("=" * 60)
    logger.info(f"SWEEP: Running {len(configs)} configurations...")
    logger.info("=" * 60)

    results: list[SweepResult] = []
    for i, config in enumerate(configs, 1):
        logger.info(f"Config {i}/{len(configs)}: {config.label}")

        result = run_single_config(
            config=config,
            game_dates=game_dates,
            date_predictions=date_predictions,
            date_lines=date_lines,
            date_actuals=date_actuals,
            starting_bankroll=args.starting_bankroll,
            max_bet_pct=args.max_bet_pct,
            flat_bet_size=args.flat_bet,
        )

        results.append(result)
        m = result.metrics
        logger.info(
            f"  -> {m.total_bets} bets, HitRate={m.hit_rate:.1%}, "
            f"ROI={m.roi:+.2%}, Sharpe={m.sharpe_ratio:.2f} ({result.elapsed_seconds:.1f}s)"
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
        starting_bankroll=args.starting_bankroll,
    )

    total_time = phase01_time + sum(r.elapsed_seconds for r in results)
    logger.info(f"Total: {total_time:.1f}s (Phase 0-1: {phase01_time:.1f}s, Sweep: {total_time - phase01_time:.1f}s)")


if __name__ == "__main__":
    main()
