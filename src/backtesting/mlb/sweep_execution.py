"""Per-config execution helpers for the MLB backtest sweep.

This module owns sweep-config execution and bet-simulator/metrics orchestration.
It deliberately does not load DB data, build predictions, fetch prop lines, or
serialize results.
"""

from __future__ import annotations

import time
from datetime import date

import pandas as pd

from src.backtesting.bet_simulator import BetSimulator
from src.backtesting.mlb.edge_engine import build_config_edge_frame, compute_edges_for_config
from src.backtesting.mlb.prediction_cache import DatePrediction
from src.backtesting.mlb.sweep_config import SweepConfig
from src.backtesting.mlb.sweep_results import SweepResult
from src.backtesting.performance_metrics import MetricsCalculator
from src.models.black_litterman import BLConfig, BlackLittermanBlender


def run_single_config_fast_mlb(
    config: SweepConfig,
    precomputed_df: pd.DataFrame,
    game_dates: list[date],
    starting_bankroll: float,
    allowed_bets: set[tuple[str, str]] | None = None,
    max_bet_pct: float | None = None,
    flat_bet_size: float | None = None,
) -> SweepResult:
    """Vectorized config evaluation using precomputed base probabilities."""
    t0 = time.time()

    if precomputed_df.empty:
        empty_metrics = MetricsCalculator().calculate(pd.DataFrame(), pd.DataFrame(), starting_bankroll=starting_bankroll)
        return SweepResult(
            config=config,
            metrics=empty_metrics,
            bets_df=pd.DataFrame(),
            predictions_df=pd.DataFrame(),
            elapsed_seconds=time.time() - t0,
        )

    df = build_config_edge_frame(config, precomputed_df)

    actuals_mask = df["actual"].notna()
    actuals_sub = df.loc[actuals_mask, ["player_id", "game_id", "stat", "actual"]]
    actuals_lookup: dict = dict(zip(
        zip(actuals_sub["player_id"], actuals_sub["game_id"], actuals_sub["stat"]),
        actuals_sub["actual"],
    ))

    simulator = BetSimulator(
        edge_threshold=config.edge_threshold,
        starting_bankroll=starting_bankroll,
        kelly_fraction=config.kelly_fraction,
        max_bet_pct=max_bet_pct,
        flat_bet_size=flat_bet_size,
        allowed_bets=allowed_bets,
    )

    all_pred_dfs = []
    for gd in game_dates:
        day_df = df[df["game_date"] == gd]
        if day_df.empty:
            continue
        simulator.evaluate_predictions(day_df, gd)
        simulator._resolve_bets_from_lookup(actuals_lookup)
        all_pred_dfs.append(day_df)

    predictions_df = pd.concat(all_pred_dfs, ignore_index=True) if all_pred_dfs else pd.DataFrame()
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

    bl_blender = None
    if config.tau is not None:
        bl_blender = BlackLittermanBlender(BLConfig(tau=config.tau, z_max=config.z_max, max_weight=config.max_weight))

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

        day_rows = compute_edges_for_config(preds, lines, bl_blender, actuals)
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

    predictions_df = pd.concat(all_prediction_rows, ignore_index=True) if all_prediction_rows else pd.DataFrame()
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
    """Run a combined backtest using per-stat BL configs and edge thresholds."""
    from src.config.stat_config import StatConfig, StatConfigSet

    t0 = time.time()

    stat_blenders: dict[str, BlackLittermanBlender | None] = {}
    for stat_key, bl_cfg in stat_bl_configs.items():
        stat_blenders[stat_key] = BlackLittermanBlender(config=bl_cfg) if bl_cfg is not None else None

    min_edge = min(stat_edge_thresholds.values()) if stat_edge_thresholds else 0.05
    stat_config_set = StatConfigSet(global_edge_threshold=min_edge)
    for stat_key, threshold in stat_edge_thresholds.items():
        stat_config_set.configs[stat_key] = StatConfig(stat=stat_key, edge_threshold=threshold)

    simulator = BetSimulator(
        edge_threshold=min_edge,
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

    predictions_df = pd.concat(all_prediction_rows, ignore_index=True) if all_prediction_rows else pd.DataFrame()
    bets_df = simulator.to_dataframe()
    metrics = MetricsCalculator().calculate(predictions_df, bets_df, starting_bankroll=starting_bankroll)

    label_config = SweepConfig(
        tau=None,
        edge_threshold=min_edge,
        kelly_fraction=kelly_fraction,
        flat_bet_size=flat_bet_size,
    )

    elapsed = time.time() - t0
    return SweepResult(
        config=label_config,
        metrics=metrics,
        bets_df=bets_df,
        predictions_df=predictions_df,
        elapsed_seconds=elapsed,
    )
