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
from src.models.mlb.mlb_batter_feature_store import MLBBatterFeatureStore
from src.models.mlb.mlb_feature_store import MLBFeatureStore
from src.models.mlb.mlb_model_suite import MLBModelSuite

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBBacktestSweep")

# Mapping from sweep stat key → batter feature store short stat name
BATTER_STAT_FS_MAP: dict[str, str] = {
    "batter_hits": "hits",
    "batter_total_bases": "total_bases",
    "batter_rbis": "rbis",
    "batter_runs_scored": "runs",
}


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
    max_weight: float = 0.50

    @property
    def label(self) -> str:
        if self.tau is None:
            return f"no_BL | edge={self.edge_threshold} | kelly={self.kelly_fraction}"
        mw = f" mw={self.max_weight}" if self.max_weight != 0.50 else ""
        return f"tau={self.tau} z_max={self.z_max}{mw} | edge={self.edge_threshold} | kelly={self.kelly_fraction}"

    def to_dict(self) -> dict:
        return {
            "tau": self.tau,
            "z_max": self.z_max,
            "max_weight": self.max_weight,
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
    max_weight_values: list[float] | None = None,
) -> list[SweepConfig]:
    if z_max_values is None:
        z_max_values = [1.0]
    if max_weight_values is None:
        max_weight_values = [0.50]

    configs = []
    for tau, edge, kelly, z_max, mw in itertools.product(
        tau_values, edge_thresholds, kelly_fractions, z_max_values, max_weight_values,
    ):
        if tau is None and (z_max != z_max_values[0] or mw != max_weight_values[0]):
            continue
        configs.append(SweepConfig(
            tau=tau, edge_threshold=edge, kelly_fraction=kelly, z_max=z_max, max_weight=mw,
        ))
    return configs


# ---------------------------------------------------------------------------
# Shared data structures for cached predictions
# ---------------------------------------------------------------------------

@dataclass
class DatePrediction:
    """Cached prediction for one player on one date (pre-edge)."""

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
    pitcher_feature_store: MLBFeatureStore,
    batter_feature_store: MLBBatterFeatureStore | None,
    suite: MLBModelSuite,
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

    # Phase 0c: Precompute matchup features once per season (avoids re-querying per date)
    matchup_cache: dict[int, tuple[pd.DataFrame, pd.DataFrame]] | None = None
    if batter_feature_store is not None:
        from src.processing.mlb.mlb_batter_matchup_features import (
            compute_opposing_starter_bulk,
            compute_platoon_splits_bulk,
        )

        seasons = {gd.year for gd in game_dates}
        matchup_cache = {}
        for season in seasons:
            logger.info(f"  Precomputing matchup features for season {season}...")
            t_mc = time.time()
            opp_df = compute_opposing_starter_bulk(engine, season)
            logger.info(f"    Opposing starter features: {len(opp_df)} rows ({time.time() - t_mc:.1f}s)")
            t_mc = time.time()
            plat_df = compute_platoon_splits_bulk(engine, season)
            logger.info(f"    Platoon splits: {len(plat_df)} rows ({time.time() - t_mc:.1f}s)")
            matchup_cache[season] = (opp_df, plat_df)

    # Phase 1: Generate predictions date-by-date
    logger.info("Phase 1: Generating predictions...")
    date_predictions: dict[date, list[DatePrediction]] = {}
    date_lines: dict[date, pd.DataFrame] = {}
    total_preds = 0

    for i, game_date in enumerate(game_dates):
        try:
            preds, lines = _process_date_shared(
                engine, pitcher_feature_store, batter_feature_store, suite,
                game_date, stats, bookmakers, matchup_cache=matchup_cache,
            )
            if preds:
                date_predictions[game_date] = preds
                total_preds += len(preds)
            if lines is not None and not lines.empty:
                date_lines[game_date] = lines
        except Exception as e:
            logger.error(f"  Error processing {game_date}: {type(e).__name__}: {e}")
            continue

        if (i + 1) % 10 == 0 or (i + 1) == len(game_dates):
            logger.info(f"  Phase 1: {i + 1}/{len(game_dates)} dates, {total_preds} predictions so far")

    logger.info(f"Phase 1 complete: {total_preds} predictions across {len(date_predictions)} dates")
    return game_dates, date_predictions, date_lines, date_actuals


def _process_date_shared(
    engine,
    pitcher_feature_store: MLBFeatureStore,
    batter_feature_store: MLBBatterFeatureStore | None,
    suite: MLBModelSuite,
    game_date: date,
    stats: list[str],
    bookmakers: list[str],
    matchup_cache: dict[int, tuple[pd.DataFrame, pd.DataFrame]] | None = None,
) -> tuple[list[DatePrediction], pd.DataFrame | None]:
    """Generate predictions + fetch lines for a single date."""
    # Get games
    query = text("""
        SELECT s.game_id, s.home_team_id, s.away_team_id,
               s.probable_pitcher_home_id, s.probable_pitcher_away_id,
               s.venue_id, s.season
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
                    "venue_id": game.get("venue_id", 0),
                    "season": game.get("season", game_date.year),
                })

    # Generate predictions
    predictions = []

    # --- Pitcher predictions (routed through suite) ---
    pitcher_stats = [s for s in stats if s.startswith("pitcher_") and suite.has_stat(s)]

    if pitcher_stats and pitchers:
        for pitcher in pitchers:
            try:
                features = pitcher_feature_store.get_player_game_features(
                    player_id=pitcher["player_id"],
                    game_id=pitcher["game_id"],
                    game_date=str(game_date),
                    team_id=pitcher["team_id"],
                    opp_team_id=pitcher["opponent_id"],
                    venue_id=pitcher.get("venue_id", 0),
                    season=pitcher["season"],
                    is_home=pitcher["is_home"],
                )
                if features is None:
                    continue

                for p_stat in pitcher_stats:
                    pred = suite.predict(
                        p_stat,
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
                logger.warning(f"Error predicting pitcher {pitcher['player_id']}: {e}")

    # --- Batter predictions (batch features per stat per date) ---
    batter_stats = [s for s in stats if s.startswith("batter_") and suite.has_stat(s)]

    if batter_stats and batter_feature_store is not None:
        for batter_stat in batter_stats:
            short_stat = BATTER_STAT_FS_MAP.get(batter_stat)
            if short_stat is None:
                logger.warning(f"No feature-store mapping for {batter_stat}, skipping")
                continue

            try:
                features_df = batter_feature_store.get_features_for_date(
                    str(game_date), stat=short_stat,
                    matchup_cache=matchup_cache,
                )
            except Exception as e:
                logger.warning(f"Error loading batter features for {batter_stat} on {game_date}: {e}")
                continue

            if features_df.empty:
                continue

            for _, row in features_df.iterrows():
                try:
                    player_id = int(row["player_id"])
                    game_id_val = int(row["game_id"])
                    features = row.to_dict()

                    pred = suite.predict(batter_stat, player_id, game_id_val, features)

                    predictions.append(DatePrediction(
                        game_date=game_date,
                        player_id=pred.player_id,
                        game_id=int(pred.game_id),
                        team_id=int(row.get("team_id", 0)),
                        opponent_id=int(row.get("opp_team_id", 0)),
                        stat=batter_stat,
                        model_type=suite.get_model_type(batter_stat),
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
                    logger.warning(
                        f"Error predicting {batter_stat} for player {row.get('player_id', '?')}: {e}"
                    )

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
        conn.execute(text("SET statement_timeout = '300000'"))  # 5 min
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
    bl_blender: BlackLittermanBlender | dict[str, BlackLittermanBlender] | None,
    actuals: dict[tuple[int, str], float] | None,
) -> list[dict]:
    """Calculate edges for predictions using a specific BL config.

    bl_blender can be:
    - None: no BL blending (baseline)
    - BlackLittermanBlender: single blender for all stats
    - dict[str, BlackLittermanBlender]: per-stat blenders
    """
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

        # Resolve blender for this stat
        stat_blender = None
        if isinstance(bl_blender, dict):
            stat_blender = bl_blender.get(pred.stat)
        elif bl_blender is not None:
            stat_blender = bl_blender

        # BL blending — overwrite probs so edges and bet decisions use posteriors
        if stat_blender is not None:
            bl_result = stat_blender.blend_prediction(
                samples=samples,
                line=line_val,
                over_odds=over_odds,
                under_odds=under_odds,
            )
            over_prob = bl_result["posterior_over"]
            under_prob = bl_result["posterior_under"]

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

        if stat_blender is not None:
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
    allowed_bets: set[tuple[str, str]] | None = None,
) -> SweepResult:
    """Run edge calculation + bet simulation + metrics for one config."""
    t0 = time.time()

    # Create BL blender for this config
    bl_blender = None
    if config.tau is not None:
        bl_blender = BlackLittermanBlender(BLConfig(tau=config.tau, z_max=config.z_max, max_weight=config.max_weight))

    # Create simulator
    simulator = BetSimulator(
        edge_threshold=config.edge_threshold,
        starting_bankroll=starting_bankroll,
        kelly_fraction=config.kelly_fraction,
        max_bet_pct=max_bet_pct,
        flat_bet_size=flat_bet_size,
        allowed_bets=allowed_bets,
    )

    all_prediction_rows = []
    all_actuals_rows: list[dict] = []

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

        # Collect actuals for resolution
        for row in day_rows:
            if row.get("actual") is not None:
                all_actuals_rows.append({
                    "player_id": row["player_id"],
                    "game_id": row["game_id"],
                    "stat": row["stat"],
                    "actual_value": row["actual"],
                })

        # Resolve pending bets from previous days before placing new ones
        if all_actuals_rows:
            simulator.resolve_bets(pd.DataFrame(all_actuals_rows))

        # Feed to simulator (place new bets)
        simulator.evaluate_predictions(day_df, game_date)

    # Final resolution for last day's bets
    if all_actuals_rows:
        simulator.resolve_bets(pd.DataFrame(all_actuals_rows))

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


def run_combined_config(
    stat_bl_configs: dict[str, BLConfig],
    stat_edge_thresholds: dict[str, float],
    game_dates: list[date],
    date_predictions: dict[date, list[DatePrediction]],
    date_lines: dict[date, pd.DataFrame],
    date_actuals: dict[date, dict[tuple[int, str], float]],
    starting_bankroll: float,
    kelly_fraction: float = 0.125,
    max_bet_pct: float | None = None,
    flat_bet_size: float | None = None,
    allowed_bets: set[tuple[str, str]] | None = None,
) -> SweepResult:
    """Run a combined backtest using per-stat BL configs and edge thresholds.

    This is the 'promotion validation' mode — each stat uses its own optimal
    BL and edge config from individual sweeps.
    """
    from src.config.stat_config import StatConfig, StatConfigSet

    t0 = time.time()

    # Build per-stat blenders
    stat_blenders: dict[str, BlackLittermanBlender] = {}
    for stat_key, bl_cfg in stat_bl_configs.items():
        stat_blenders[stat_key] = BlackLittermanBlender(config=bl_cfg)

    # Build StatConfigSet for per-stat edge thresholds
    min_edge = min(stat_edge_thresholds.values()) if stat_edge_thresholds else 0.05
    stat_config_set = StatConfigSet(global_edge_threshold=min_edge)
    for stat_key, threshold in stat_edge_thresholds.items():
        stat_config_set.configs[stat_key] = StatConfig(stat=stat_key, edge_threshold=threshold)

    # Create simulator with per-stat edge thresholds
    simulator = BetSimulator(
        edge_threshold=min_edge,  # fallback
        starting_bankroll=starting_bankroll,
        kelly_fraction=kelly_fraction,
        max_bet_pct=max_bet_pct,
        flat_bet_size=flat_bet_size,
        stat_config=stat_config_set,
        allowed_bets=allowed_bets,
    )

    all_prediction_rows = []
    all_actuals_rows: list[dict] = []

    for game_date in game_dates:
        preds = date_predictions.get(game_date)
        if not preds:
            continue

        lines = date_lines.get(game_date)
        actuals = date_actuals.get(game_date, {})

        day_rows = compute_edges_for_config(preds, lines, stat_blenders, actuals)
        if not day_rows:
            continue

        day_df = pd.DataFrame(day_rows)
        all_prediction_rows.append(day_df)

        for row in day_rows:
            if row.get("actual") is not None:
                all_actuals_rows.append({
                    "player_id": row["player_id"],
                    "game_id": row["game_id"],
                    "stat": row["stat"],
                    "actual_value": row["actual"],
                })

        if all_actuals_rows:
            simulator.resolve_bets(pd.DataFrame(all_actuals_rows))
        simulator.evaluate_predictions(day_df, game_date)

    if all_actuals_rows:
        simulator.resolve_bets(pd.DataFrame(all_actuals_rows))

    if all_prediction_rows:
        predictions_df = pd.concat(all_prediction_rows, ignore_index=True)
    else:
        predictions_df = pd.DataFrame()

    bets_df = simulator.to_dataframe()
    metrics = MetricsCalculator().calculate(predictions_df, bets_df, starting_bankroll=starting_bankroll)

    # Build a representative SweepConfig for labeling
    label_config = SweepConfig(
        tau=None, edge_threshold=min_edge, kelly_fraction=kelly_fraction,
    )

    elapsed = time.time() - t0

    return SweepResult(
        config=label_config,
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
        f"\n{'=' * 120}\n"
        f"MLB BACKTEST SWEEP  ({start_date} to {end_date})\n"
        f"Phase 0-1: {total_dates} dates, {total_predictions} predictions ({phase01_time:.1f}s)\n"
        f"Starting bankroll: ${starting_bankroll:,.0f}\n"
        f"{'=' * 120}\n"
    )
    print(header)

    fmt = "{:>3}  {:<40} {:>5} {:>7} {:>8} {:>9} {:>10} {:>7} {:>7} {:>6}"
    print(fmt.format(
        "#", "Config", "Bets", "HitRate", "ROI", "Profit",
        "Staked", "Sharpe", "MaxDD", "Time",
    ))
    print(fmt.format(
        "---", "-" * 40, "-----", "-------", "--------", "---------",
        "----------", "-------", "-------", "------",
    ))

    for i, r in enumerate(results, 1):
        m = r.metrics
        roi_str = f"{m.roi:+.2%}" if m.roi != 0 else "0.00%"
        profit_str = f"${m.total_profit:+,.0f}" if m.total_profit != 0 else "$0"
        staked_str = f"${m.total_staked:,.0f}" if m.total_staked != 0 else "$0"
        print(fmt.format(
            i,
            r.config.label,
            m.total_bets,
            f"{m.hit_rate:.1%}",
            roi_str,
            profit_str,
            staked_str,
            f"{m.sharpe_ratio:.2f}",
            f"{m.max_drawdown:.1%}",
            f"{r.elapsed_seconds:.1f}s",
        ))

    # Per-stat breakdown
    print(f"\n{'─' * 120}")
    print("PER-STAT BREAKDOWN")
    print(f"{'─' * 120}")
    stat_labels = []
    for r in results:
        if r.metrics.by_stat:
            stat_labels = list(r.metrics.by_stat.keys())
            break

    if stat_labels:
        header_parts = [f"{s.upper():>16}" for s in stat_labels]
        print(f"{'#':>3}  {'Config':<40} " + " ".join(header_parts))
        print(f"{'---':>3}  {'-' * 40} " + " ".join(["-" * 16] * len(stat_labels)))

        for i, r in enumerate(results, 1):
            parts = []
            for s in stat_labels:
                stat_data = r.metrics.by_stat.get(s, {})
                roi = stat_data.get("roi", 0)
                bets = stat_data.get("bets", 0)
                hit = stat_data.get("hit_rate", 0)
                parts.append(f"{roi:+.1%}({bets},{hit:.0%})")
            print(f"{i:>3}  {r.config.label:<40} " + " ".join(f"{p:>16}" for p in parts))

    # Summary
    print(f"\n{'=' * 120}")
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
    print(f"{'=' * 120}\n")


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

        row = {
            "tau": r.config.tau,
            "z_max": r.config.z_max,
            "max_weight": r.config.max_weight,
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
            "win_streak": m.win_streak,
            "loss_streak": m.loss_streak,
            "elapsed_seconds": round(r.elapsed_seconds, 2),
        }

        # Add per-stat columns
        for stat, stat_data in m.by_stat.items():
            row[f"{stat}_bets"] = stat_data.get("bets", 0)
            row[f"{stat}_roi"] = round(stat_data.get("roi", 0), 4)
            row[f"{stat}_hit_rate"] = round(stat_data.get("hit_rate", 0), 4)

        csv_rows.append(row)

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
    """Find the best model directory for the suite.

    Priority:
    1. production/ subdirectory (unified suite location)
    2. Model files directly in base_dir (legacy)
    3. Latest mlb_run_* directory
    """
    base = Path(base_dir)
    if not base.exists():
        raise FileNotFoundError(f"Artifacts directory not found: {base}")

    # 1. Check production/ first (where unified suite lives)
    prod = base / "production"
    if prod.exists() and prod.is_dir():
        return prod

    # 2. Legacy: model files directly in base_dir
    if (base / "pitcher_k_model.joblib").exists():
        return base

    # 3. Scan mlb_run_* directories, pick latest
    runs = sorted([
        d for d in base.iterdir()
        if d.is_dir() and d.name.startswith("mlb_run_") and not d.name.endswith("_incomplete")
    ])
    if not runs:
        raise FileNotFoundError(f"No model directories found in {base}")
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
    parser.add_argument("--max-weight", type=float, nargs="+", default=[0.50],
                        help="BL max blending weight (0.50=default, higher=more model influence)")

    # Model / data
    parser.add_argument("--model-dir", type=str, default="src/models/mlb/artifacts")
    parser.add_argument("--n-samples", type=int, default=5000, help="Monte Carlo samples")
    parser.add_argument("--stats", nargs="+",
                        default=["pitcher_strikeouts", "batter_hits", "batter_rbis"])
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
    parser.add_argument("--local", action="store_true",
                        help="Use local Postgres (LOCAL_DATABASE_URL) instead of Supabase")
    parser.add_argument("--combined", action="store_true",
                        help="Run combined backtest using per-stat optimal BL configs from mlb_stat_config.py. "
                             "Ignores --tau, --edge, --z-max, --max-weight when set.")
    parser.add_argument("--direction", choices=["over", "under", "both"], default="both",
                        help="Restrict bet direction for all stats (default: both). "
                             "In --combined mode, per-stat allowed_directions from mlb_stat_config.py "
                             "are also applied on top of this filter.")

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

    # Build allowed_bets set from --direction + --stats flags
    if args.direction == "both":
        cli_allowed_bets: set[tuple[str, str]] | None = None
    else:
        cli_allowed_bets = {(stat, args.direction) for stat in args.stats}

    configs = build_sweep_grid(tau_values, args.edge, args.kelly, args.z_max, args.max_weight)
    logger.info(f"Sweep grid: {len(configs)} configurations")

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("backtest_results") / f"mlb_sweep_{timestamp}"

    # Initialize
    engine = get_engine(local=args.local)
    if args.local:
        logger.info("Using LOCAL database")
    pitcher_feature_store = MLBFeatureStore(engine)

    model_path = find_latest_model_dir(args.model_dir)
    logger.info(f"Using model directory: {model_path}")

    suite = MLBModelSuite.from_directory(model_path, n_samples=args.n_samples)
    logger.info(f"Suite loaded stats: {suite.available_stats}")

    # Only create batter feature store if we have batter stats to predict
    has_batter_stats = any(s.startswith("batter_") and suite.has_stat(s) for s in args.stats)
    batter_feature_store = MLBBatterFeatureStore(engine) if has_batter_stats else None

    # Phase 0 + 1
    logger.info("=" * 60)
    logger.info("PHASE 0-1: Loading data and generating predictions...")
    logger.info("=" * 60)
    t_shared = time.time()

    game_dates, date_predictions, date_lines, date_actuals = run_shared_phases(
        engine=engine,
        pitcher_feature_store=pitcher_feature_store,
        batter_feature_store=batter_feature_store,
        suite=suite,
        start_date=start_date,
        end_date=end_date,
        stats=args.stats,
        bookmakers=args.bookmakers,
    )

    phase01_time = time.time() - t_shared
    total_predictions = sum(len(preds) for preds in date_predictions.values())

    results: list[SweepResult] = []

    if args.combined:
        # Combined mode: use per-stat optimal configs from mlb_stat_config.py
        from src.models.mlb.mlb_stat_config import DEFAULT_BL_CONFIG, MLB_STATS, STAT_BL_CONFIGS

        logger.info("=" * 60)
        logger.info("COMBINED MODE: Using per-stat optimal BL configs")
        for stat_key, bl_cfg in STAT_BL_CONFIGS.items():
            if stat_key in args.stats:
                edge = MLB_STATS.get(stat_key, {}).get("edge_threshold", 0.08)
                dirs = MLB_STATS.get(stat_key, {}).get("allowed_directions", ["over", "under"])
                logger.info(f"  {stat_key}: tau={bl_cfg.tau}, z_max={bl_cfg.z_max}, mw={bl_cfg.max_weight}, edge={edge}, dirs={dirs}")
        logger.info("=" * 60)

        # Build per-stat BL configs and edge thresholds for requested stats
        stat_bl = {s: STAT_BL_CONFIGS.get(s, DEFAULT_BL_CONFIG) for s in args.stats}
        stat_edges = {s: MLB_STATS.get(s, {}).get("edge_threshold", 0.08) for s in args.stats}

        # Build allowed_bets: intersect CLI direction filter with per-stat allowed_directions from config
        config_pairs: set[tuple[str, str]] = set()
        for stat in args.stats:
            per_stat_dirs = MLB_STATS.get(stat, {}).get("allowed_directions")
            dirs = per_stat_dirs if per_stat_dirs else ["over", "under"]
            for d in dirs:
                config_pairs.add((stat, d))
        if cli_allowed_bets is not None:
            combined_allowed_bets: set[tuple[str, str]] | None = config_pairs & cli_allowed_bets
        else:
            all_pairs = {(s, d) for s in args.stats for d in ("over", "under")}
            combined_allowed_bets = config_pairs if config_pairs != all_pairs else None

        result = run_combined_config(
            stat_bl_configs=stat_bl,
            stat_edge_thresholds=stat_edges,
            game_dates=game_dates,
            date_predictions=date_predictions,
            date_lines=date_lines,
            date_actuals=date_actuals,
            starting_bankroll=args.starting_bankroll,
            kelly_fraction=args.kelly[0],
            max_bet_pct=args.max_bet_pct,
            flat_bet_size=args.flat_bet,
            allowed_bets=combined_allowed_bets,
        )

        results.append(result)
        m = result.metrics
        logger.info(
            f"  -> {m.total_bets} bets, HitRate={m.hit_rate:.1%}, "
            f"ROI={m.roi:+.2%}, Profit=${m.total_profit:+,.0f}, "
            f"Sharpe={m.sharpe_ratio:.2f}, MaxDD={m.max_drawdown:.1%} ({result.elapsed_seconds:.1f}s)"
        )
    else:
        # Standard sweep mode
        logger.info("=" * 60)
        logger.info(f"SWEEP: Running {len(configs)} configurations...")
        logger.info("=" * 60)

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
                allowed_bets=cli_allowed_bets,
            )

            results.append(result)
            m = result.metrics
            logger.info(
                f"  -> {m.total_bets} bets, HitRate={m.hit_rate:.1%}, "
                f"ROI={m.roi:+.2%}, Profit=${m.total_profit:+,.0f}, "
                f"Sharpe={m.sharpe_ratio:.2f}, MaxDD={m.max_drawdown:.1%} ({result.elapsed_seconds:.1f}s)"
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
