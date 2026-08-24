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

from src.backtesting.backtest_harness import BacktestHarness
from src.backtesting.bet_simulator import BetSimulator
from src.backtesting.performance_metrics import MetricsCalculator, PerformanceMetrics
from src.config.combo_config import MARKET_TO_STAT
from src.config.stat_config import StatConfigSet
from src.db.client import get_engine
from src.models.black_litterman import BlackLittermanBlender, BLConfig
from src.models.feature_store import FeatureStore
from src.models.monte_carlo import MonteCarloPredictor, load_combined_calibration_offsets, load_copula_params
from src.models.quantile_trainer import PlayerPropsModelPipeline

_MIN_PROB = 1e-6
_MAX_PROB = 1.0 - 1e-6

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
    max_weight: float = 0.50  # Hard cap on BL blending weight

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
    max_weight_values: list[float] | None = None,
) -> list[SweepConfig]:
    """Generate Cartesian product of parameter values.

    Note: z_max and max_weight only apply when tau is not None. For no-BL
    configs, they are ignored and only one config per (edge, kelly) is added.
    """
    if z_max_values is None:
        z_max_values = [1.0]
    if max_weight_values is None:
        max_weight_values = [0.50]

    configs = []
    for tau, edge, kelly, z_max, mw in itertools.product(
        tau_values, edge_thresholds, kelly_fractions, z_max_values, max_weight_values,
    ):
        # For no-BL, BL params don't matter — only add one config per (edge, kelly)
        if tau is None and (z_max != z_max_values[0] or mw != max_weight_values[0]):
            continue
        configs.append(SweepConfig(
            tau=tau, edge_threshold=edge, kelly_fraction=kelly, z_max=z_max, max_weight=mw,
        ))
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
# Phase 0b: pre-compute base probabilities (runs once, reused across configs)
# ---------------------------------------------------------------------------

def precompute_base_probabilities(
    game_dates: list[date],
    date_predictions: dict[date, pd.DataFrame],
    date_samples: dict[date, dict],
    prefetched_lines: dict[date, pd.DataFrame],
) -> pd.DataFrame:
    """Compute model & market probabilities for every (player, game, stat, line, bookie) row.

    Called once after Phase 1. Per-config sweep only needs vectorized BL math on this
    DataFrame — no harness instantiation, no iterrows per config.

    Columns added:
        model_over, model_under  — empirical CDF from MC samples (constant across configs)
        market_over, market_under — devigged market probs (constant across configs)
        model_logit, market_logit — log-odds (constant across configs)
        z_raw                    — |mean - line| / std before z_max clipping (constant)
    """
    all_frames = []

    for game_date in game_dates:
        raw_preds = date_predictions.get(game_date)
        lines_df = prefetched_lines.get(game_date)
        samples_dict = date_samples.get(game_date, {})

        if raw_preds is None or raw_preds.empty or lines_df is None or lines_df.empty:
            continue

        # Merge predictions × lines (identical to _calculate_edges logic)
        ld = lines_df.copy()
        ld["stat"] = ld["market_key"].map(MARKET_TO_STAT)
        line_cols = ["player_id", "game_id", "stat", "line", "over_odds", "under_odds"]
        if "bookmaker" in ld.columns:
            line_cols.append("bookmaker")
        merged = raw_preds.merge(ld[line_cols], on=["player_id", "game_id", "stat"], how="left")
        merged = merged.dropna(subset=["line", "over_odds", "under_odds"]).reset_index(drop=True)

        if merged.empty:
            continue

        merged["game_date"] = game_date

        # Model probs + raw z-score — samples lookup requires per-row access (runs ONCE total)
        model_overs = np.zeros(len(merged))
        z_raws = np.zeros(len(merged))
        for i, row in enumerate(merged.itertuples(index=False)):
            key = (row.player_id, row.game_id, row.stat)
            samples = samples_dict.get(key)
            if samples is not None and len(samples) > 0:
                model_overs[i] = float((samples > row.line).mean())
                std = float(np.std(samples))
                z_raws[i] = abs(float(np.mean(samples)) - row.line) / std if std > 1e-6 else 0.0
            else:
                model_overs[i] = 0.5

        merged["model_over"] = model_overs
        merged["model_under"] = 1.0 - model_overs
        merged["z_raw"] = z_raws

        # Vectorized devig
        over_odds = merged["over_odds"].values.astype(float)
        under_odds = merged["under_odds"].values.astype(float)
        dec_over = np.where(over_odds > 0, 1.0 + over_odds / 100.0, 1.0 - 100.0 / over_odds)
        dec_under = np.where(under_odds > 0, 1.0 + under_odds / 100.0, 1.0 - 100.0 / under_odds)
        raw_over = 1.0 / np.clip(dec_over, 1.01, None)
        raw_under = 1.0 / np.clip(dec_under, 1.01, None)
        booksum = np.clip(raw_over + raw_under, 1e-9, None)
        market_over = np.clip(raw_over / booksum, _MIN_PROB, _MAX_PROB)
        market_under = np.clip(raw_under / booksum, _MIN_PROB, _MAX_PROB)
        merged["market_over"] = market_over
        merged["market_under"] = market_under

        # Pre-compute log-odds for BL blend formula
        mo_clipped = np.clip(model_overs, _MIN_PROB, _MAX_PROB)
        merged["model_logit"] = np.log(mo_clipped / (1.0 - mo_clipped))
        merged["market_logit"] = np.log(market_over / (1.0 - market_over))

        all_frames.append(merged)

    if not all_frames:
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Fast per-config execution using pre-computed base probabilities
# ---------------------------------------------------------------------------

def _filter_best_bets_fast(predictions_df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized line-shopping + bet-dedup. Replaces BacktestHarness._filter_best_bets
    in the sweep hot-path (no iterrows).

    Stage 1: For each (player, game, stat), pick the bookmaker with best over edge
             AND independently the one with best under edge; compare to choose side.
    Stage 2: One bet per (player, game) — keep highest edge stat.
    """
    if predictions_df.empty:
        return predictions_df

    dedup_key = ["player_id", "game_id", "stat"]
    df = predictions_df.reset_index(drop=True)

    # Stage 1: idxmax per group — O(n log n), fully vectorized
    best_over_idx = df.groupby(dedup_key)["over_edge"].idxmax()
    best_under_idx = df.groupby(dedup_key)["under_edge"].idxmax()

    best_over_rows = df.loc[best_over_idx.values].reset_index(drop=True)
    best_under_rows = df.loc[best_under_idx.values].reset_index(drop=True)

    use_over = best_over_rows["over_edge"].values >= best_under_rows["under_edge"].values
    selected_idxs = np.where(use_over, best_over_idx.values, best_under_idx.values)

    result = df.loc[selected_idxs].copy().reset_index(drop=True)
    result["max_edge"] = np.maximum(
        best_over_rows["over_edge"].values,
        best_under_rows["under_edge"].values,
    )

    # Stage 2: one bet per (player, game)
    result = result.sort_values("max_edge", ascending=False).drop_duplicates(
        subset=["player_id", "game_id"], keep="first"
    )
    return result


def run_single_config_fast(
    config: SweepConfig,
    precomputed_df: pd.DataFrame,
    game_dates: list[date],
    actuals_lookup: dict,
    void_keys: set,
    starting_bankroll: float,
    allowed_bets: list[tuple[str, str]] | None,
    max_bet_pct: float | None = None,
    flat_bet_size: float | None = None,
) -> SweepResult:
    """Fast per-config sweep using pre-computed base probabilities.

    Replaces run_single_config() in the sweep loop. Per-config work is:
      - Vectorized numpy BL math (no iterrows, no harness instantiation)
      - _filter_best_bets per date (~90 rows/date — negligible)
      - BetSimulator sequential by date (required for bankroll tracking)

    ~10–50x faster than run_single_config() on large sweeps.
    """
    t0 = time.time()

    if precomputed_df.empty:
        empty_metrics = MetricsCalculator().calculate(
            pd.DataFrame(), pd.DataFrame(), starting_bankroll=starting_bankroll
        )
        return SweepResult(
            config=config, metrics=empty_metrics,
            bets_df=pd.DataFrame(), predictions_df=pd.DataFrame(),
            all_edges_df=pd.DataFrame(), elapsed_seconds=time.time() - t0,
        )

    # ── Vectorized BL computation (replaces iterrows in _calculate_edges) ──
    z_raw = precomputed_df["z_raw"].values
    model_logit = precomputed_df["model_logit"].values
    market_logit = precomputed_df["market_logit"].values
    model_over = precomputed_df["model_over"].values
    market_over = precomputed_df["market_over"].values
    market_under = precomputed_df["market_under"].values

    if config.tau is None:
        posterior_over = model_over.copy()
    else:
        confidence = np.minimum(z_raw / config.z_max, 1.0)
        w = np.minimum(config.tau * confidence, config.max_weight)
        posterior_logit = market_logit + w * (model_logit - market_logit)
        posterior_over = 1.0 / (1.0 + np.exp(-posterior_logit))

    posterior_under = 1.0 - posterior_over

    # Build working DataFrame with only the columns needed by filter + simulator
    keep_cols = [c for c in [
        "game_date", "player_id", "game_id", "stat", "line", "over_odds", "under_odds", "bookmaker",
    ] if c in precomputed_df.columns]
    preds = precomputed_df[keep_cols].copy()
    preds["over_prob"]       = posterior_over
    preds["under_prob"]      = posterior_under
    preds["implied_over"]    = market_over
    preds["implied_under"]   = market_under
    preds["over_edge"]       = posterior_over - market_over
    preds["under_edge"]      = posterior_under - market_under
    preds["posterior_over"]  = posterior_over
    preds["posterior_under"] = posterior_under

    # ── Simulation loop (sequential for bankroll tracking) ─────────────────
    stat_config = StatConfigSet(
        global_edge_threshold=config.edge_threshold,
        global_bl_tau=config.tau,
    )
    simulator = BetSimulator(
        edge_threshold=config.edge_threshold,
        starting_bankroll=starting_bankroll,
        kelly_fraction=config.kelly_fraction,
        max_bet_pct=max_bet_pct,
        flat_bet_size=flat_bet_size,
        allowed_bets=set(allowed_bets) if allowed_bets else None,
        stat_config=stat_config,
    )

    all_date_preds: list[pd.DataFrame] = []
    for game_date in game_dates:
        day_df = preds[preds["game_date"] == game_date]
        if day_df.empty:
            continue

        day_filtered = _filter_best_bets_fast(day_df)
        if day_filtered.empty:
            continue

        all_date_preds.append(day_filtered)

        # Use pre-built lookups — no iterrows per date
        if void_keys:
            simulator._resolve_voids_from_keys(void_keys)
        if actuals_lookup:
            simulator._resolve_bets_from_lookup(actuals_lookup)
        simulator.evaluate_predictions(day_filtered, game_date)

    # Final resolution
    if void_keys:
        simulator._resolve_voids_from_keys(void_keys)
    if actuals_lookup:
        simulator._resolve_bets_from_lookup(actuals_lookup)

    bets_df = simulator.to_dataframe()
    predictions_df = (
        pd.concat(all_date_preds, ignore_index=True) if all_date_preds else pd.DataFrame()
    )
    metrics = MetricsCalculator().calculate(predictions_df, bets_df, starting_bankroll=starting_bankroll)

    return SweepResult(
        config=config, metrics=metrics,
        bets_df=bets_df, predictions_df=predictions_df,
        all_edges_df=pd.DataFrame(),   # skipped in sweep mode for speed
        elapsed_seconds=time.time() - t0,
    )


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
    flat_bet_size: float | None = None,
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
        bl_blender = BlackLittermanBlender(BLConfig(tau=config.tau, z_max=config.z_max, max_weight=config.max_weight))

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
        flat_bet_size=flat_bet_size,
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
        flat_bet_size=flat_bet_size,
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
        "Per-config dashboards: run  "
        "python src/backtesting/visualize_results.py --results-dir <config_dir>"
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

    # Find all nba_run_* directories, sorted newest first
    runs = sorted(
        [d for d in base.iterdir() if d.is_dir() and d.name.startswith("nba_run_")],
        reverse=True,
    )

    if not runs:
        raise FileNotFoundError(f"No nba_run_* directories found in {base}")

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
    parser.add_argument(
        "--max-weight", type=float, nargs="+",
        default=[0.50],
        help="BL max blending weight values to sweep. Hard cap on model influence. (default: 0.50)",
    )
    parser.add_argument(
        "--direction", choices=["over", "under", "both"], default="both",
        help="Restrict bet direction for all stats (default: both). "
             "Shorthand for --allowed-bets pts:dir reb:dir ast:dir.",
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
        "--flat-bet",
        type=float,
        default=None,
        help="Fixed dollar amount per bet (overrides Kelly sizing). E.g., --flat-bet 100 for $100/bet.",
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
            "hardrockbet", "novig", "prophetx", "rebet",
            "windcreek",
        ],
        help="Bookmakers to shop lines from",
    )
    parser.add_argument(
        "--allowed-bets", nargs="+", default=None,
        help="Stat:side pairs to allow (e.g., pts:under reb:over)",
    )
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory")
    parser.add_argument("--local", action="store_true",
                        help="Use local Postgres (LOCAL_DATABASE_URL) instead of Supabase")

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
    # --direction is a shorthand that expands to per-stat pairs; --allowed-bets overrides it
    allowed_bets = None
    if args.allowed_bets:
        allowed_bets = []
        for pair in args.allowed_bets:
            parts = pair.lower().split(":")
            if len(parts) != 2 or parts[1] not in ("over", "under"):
                parser.error(f"Invalid --allowed-bets value '{pair}'. Use format stat:side (e.g., pts:under)")
            allowed_bets.append((parts[0], parts[1]))
    elif args.direction != "both":
        allowed_bets = [(stat, args.direction) for stat in args.stats]

    # Parse dates
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    # Build sweep grid
    configs = build_sweep_grid(tau_values, args.edge, args.kelly, args.z_max, args.max_weight)
    logger.info(f"Sweep grid: {len(configs)} configurations")
    for i, c in enumerate(configs, 1):
        logger.info(f"  Config {i}: {c.label}")

    # Output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("backtest_results") / f"nba_sweep_{timestamp}"

    # Initialize shared components
    engine = get_engine(local=args.local)
    if args.local:
        logger.info("Using LOCAL database")
    feature_store = FeatureStore(engine)

    model_path = find_latest_model_dir(args.model_dir)
    logger.info(f"Using model artifacts from: {model_path}")

    pipeline = PlayerPropsModelPipeline.load_all(str(model_path), feature_store)

    copula_params = load_copula_params(str(model_path))
    if copula_params:
        logger.info(f"Loaded Gaussian copula params: { {k: f'{v:.4f}' for k, v in copula_params.items()} }")
    else:
        logger.info("No copula_params.json found, using legacy correlation adjustment")

    combined_cal_offsets = load_combined_calibration_offsets(str(model_path))
    if combined_cal_offsets:
        logger.info(f"Loaded combined calibration offsets for: {list(combined_cal_offsets.keys())}")

    predictor = MonteCarloPredictor(
        pipeline, n_samples=args.n_samples, copula_params=copula_params,
        combined_calibration_offsets=combined_cal_offsets,
    )

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

    # Phase 0b: pre-compute base probabilities + lookup dicts once (reused across all configs)
    logger.info("Phase 0b: Precomputing base probabilities and lookup tables...")
    t0b = time.time()
    precomputed_df = precompute_base_probabilities(
        game_dates, date_predictions, date_samples, prefetched_lines
    )
    # Pre-build actuals/voids lookups once — avoids rebuilding per date per config
    actuals_lookup: dict = {}
    if len(actuals_df) > 0:
        actuals_lookup = dict(zip(
            zip(actuals_df["player_id"], actuals_df["game_id"], actuals_df["stat"]),
            actuals_df["actual_value"],
        ))
    void_keys: set = set()
    if len(voids_df) > 0:
        void_keys = set(zip(voids_df["player_id"], voids_df["game_id"]))
    logger.info(
        f"  {len(precomputed_df):,} rows precomputed, "
        f"{len(actuals_lookup):,} actuals, {len(void_keys):,} void keys  "
        f"({time.time() - t0b:.1f}s)"
    )

    # Sweep loop
    logger.info("=" * 60)
    logger.info(f"SWEEP: Running {len(configs)} configurations...")
    logger.info("=" * 60)

    results: list[SweepResult] = []
    for i, config in enumerate(configs, 1):
        logger.info(f"Config {i}/{len(configs)}: {config.label}")

        result = run_single_config_fast(
            config=config,
            precomputed_df=precomputed_df,
            game_dates=game_dates,
            actuals_lookup=actuals_lookup,
            void_keys=void_keys,
            starting_bankroll=args.starting_bankroll,
            allowed_bets=allowed_bets,
            max_bet_pct=args.max_bet_pct,
            flat_bet_size=args.flat_bet,
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
