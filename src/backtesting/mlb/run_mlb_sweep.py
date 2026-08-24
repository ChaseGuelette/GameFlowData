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

import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.backtesting.mlb.backtest_data_loader import (
    fetch_actuals_by_date,
    fetch_game_dates,
    fetch_games_for_date,
)
from src.backtesting.mlb.edge_engine import precompute_mlb_base_probs
from src.backtesting.mlb.matchup_cache import build_matchup_cache
from src.backtesting.mlb.mlb_backtest_harness import STAT_ACTUALS
from src.backtesting.mlb.prediction_cache import DatePrediction, build_predictions_for_date
from src.backtesting.mlb.promotion_contracts import build_promotion_contract_metadata
from src.backtesting.mlb.quote_clean_line_service import fetch_lines_for_date
from src.backtesting.mlb.quote_decision_policy import build_fixed_cutoff_ts
from src.backtesting.mlb.sweep_bootstrap import initialize_sweep_runtime
from src.backtesting.mlb.sweep_config import (
    build_arg_parser,
    parse_sweep_cli_config,
)
from src.backtesting.mlb.sweep_execution import (
    run_combined_config,
    run_single_config_fast_mlb,
)
from src.backtesting.mlb.sweep_results import SweepResult, print_comparison_table, save_results
from src.models.mlb.mlb_batter_feature_store import MLBBatterFeatureStore
from src.models.mlb.mlb_feature_store import MLBFeatureStore
from src.models.mlb.mlb_model_suite import MLBModelSuite

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBBacktestSweep")

# Bookmakers excluded from edge calculation — mirrors mlb_daily_runner._EXCLUDED_BOOKMAKERS.
# novig: low-vig sharp book (user cannot bet there)
# betonlineag: offshore book (user cannot bet there)
# DFS platforms: use DFS-specific pricing, not real sportsbook odds
EXCLUDED_BOOKMAKERS: tuple[str, ...] = (
    "novig",
    "betonlineag",
    "dabble_us_dfs",
    "betr_us_dfs",
    "pick6",
    "prizepicks",
    "underdog",
)


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
    quote_clean_cutoff_time_et: str | None = None,
    quote_decision_policy: str = "fixed_et",
    quote_relative_minutes: int = 60,
    line_source: str = "mlb_raw_player_props",
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
    game_dates = fetch_game_dates(engine, start_date, end_date)
    logger.info(f"  Found {len(game_dates)} dates with games")

    # Phase 0b: Prefetch all actuals
    logger.info("Phase 0: Fetching actuals...")
    date_actuals = fetch_actuals_by_date(engine, start_date, end_date, stats)
    total_actuals = sum(len(v) for v in date_actuals.values())
    logger.info(f"  Prefetched {total_actuals} actuals across {len(date_actuals)} dates")

    # Phase 0c: Precompute matchup features once per season (avoids re-querying per date)
    matchup_cache = build_matchup_cache(
        engine=engine,
        game_dates=game_dates,
        enabled=batter_feature_store is not None,
    )

    # Phase 1: Generate predictions date-by-date
    logger.info("Phase 1: Generating predictions...")
    date_predictions: dict[date, list[DatePrediction]] = {}
    date_lines: dict[date, pd.DataFrame] = {}
    total_preds = 0

    for i, game_date in enumerate(game_dates):
        try:
            preds, lines = _process_date_shared(
                engine, pitcher_feature_store, batter_feature_store, suite,
                game_date, stats, matchup_cache=matchup_cache,
                quote_clean_cutoff_time_et=quote_clean_cutoff_time_et,
                quote_decision_policy=quote_decision_policy,
                quote_relative_minutes=quote_relative_minutes,
                line_source=line_source,
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
    matchup_cache: dict[int, tuple[pd.DataFrame, pd.DataFrame]] | None = None,
    quote_clean_cutoff_time_et: str | None = None,
    quote_decision_policy: str = "fixed_et",
    quote_relative_minutes: int = 60,
    line_source: str = "mlb_raw_player_props",
) -> tuple[list[DatePrediction], pd.DataFrame | None]:
    """Generate predictions + fetch lines for a single date."""
    # Get games
    games = fetch_games_for_date(engine, game_date)

    if not games:
        return [], None

    quote_clean_cutoff_ts = None
    if quote_clean_cutoff_time_et is not None:
        quote_clean_cutoff_ts = build_fixed_cutoff_ts(game_date, quote_clean_cutoff_time_et)

    predictions = build_predictions_for_date(
        pitcher_feature_store=pitcher_feature_store,
        batter_feature_store=batter_feature_store,
        suite=suite,
        game_date=game_date,
        games=games,
        stats=stats,
        matchup_cache=matchup_cache,
        as_of_time=quote_clean_cutoff_ts,
    )

    # Fetch lines for all players on this date
    market_keys = [s for s in stats if s in STAT_ACTUALS]
    lines_df = fetch_lines_for_date(
        engine,
        games,
        market_keys,
        quote_clean_cutoff_ts=quote_clean_cutoff_ts,
        quote_clean_cutoff_time_et=quote_clean_cutoff_time_et,
        quote_decision_policy=quote_decision_policy,
        quote_relative_minutes=quote_relative_minutes,
        line_source=line_source,
    )

    return predictions, lines_df


# ---------------------------------------------------------------------------
# Fast path: precompute once, vectorized BL per config
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    cli_config = parse_sweep_cli_config(args)

    start_date = cli_config.start_date
    end_date = cli_config.end_date
    cli_allowed_bets = cli_config.cli_allowed_bets
    configs = cli_config.sweep_grid
    logger.info(f"Sweep grid: {len(configs)} configurations")

    if cli_config.output_dir:
        output_dir = cli_config.output_dir
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("backtest_results") / f"mlb_sweep_{timestamp}"

    # Initialize
    runtime = initialize_sweep_runtime(cli_config, log=logger)
    engine = runtime.engine
    pitcher_feature_store = runtime.pitcher_feature_store
    batter_feature_store = runtime.batter_feature_store
    suite = runtime.suite

    # Phase 0 + 1
    logger.info("=" * 60)
    logger.info("PHASE 0-1: Loading data and generating predictions...")
    logger.info("=" * 60)
    t_shared = time.time()

    logger.info(f"Excluding bookmakers: {list(EXCLUDED_BOOKMAKERS)}")
    if cli_config.quote_clean.enabled:
        logger.info(
            "Quote-clean line mode enabled: latest snapshots <= %s ET, then lowest-vig production line selection",
            cli_config.quote_clean.cutoff_time_et,
        )
    else:
        logger.warning(
            "Legacy line mode enabled: aggregates odds across all snapshots; results are not promotion-grade."
        )
    game_dates, date_predictions, date_lines, date_actuals = run_shared_phases(
        engine=engine,
        pitcher_feature_store=pitcher_feature_store,
        batter_feature_store=batter_feature_store,
        suite=suite,
        start_date=start_date,
        end_date=end_date,
        stats=cli_config.stats,
        quote_clean_cutoff_time_et=cli_config.quote_clean.cutoff_time_et if cli_config.quote_clean.enabled else None,
        quote_decision_policy=cli_config.quote_clean.decision_policy if cli_config.quote_clean.enabled else "fixed_et",
        quote_relative_minutes=cli_config.quote_clean.relative_minutes,
        line_source=cli_config.quote_clean.line_source if cli_config.quote_clean.enabled else "mlb_raw_player_props",
    )

    phase01_time = time.time() - t_shared
    total_predictions = sum(len(preds) for preds in date_predictions.values())

    # Phase 0b: Precompute base probabilities + lookup tables once (used by fast sweep path)
    logger.info("Phase 0b: Precomputing base probabilities...")
    t_pre = time.time()
    precomputed_df = precompute_mlb_base_probs(game_dates, date_predictions, date_lines, date_actuals)
    logger.info(f"  {len(precomputed_df):,} rows precomputed in {time.time() - t_pre:.1f}s")

    results: list[SweepResult] = []

    if cli_config.combined:
        # Combined mode: use per-stat optimal configs from mlb_stat_config.py
        from src.models.mlb.mlb_stat_config import DEFAULT_BL_CONFIG, MLB_STATS, STAT_BL_CONFIGS

        logger.info("=" * 60)
        logger.info("COMBINED MODE: Using per-stat optimal BL configs")
        for stat_key, bl_cfg in STAT_BL_CONFIGS.items():
            if stat_key in cli_config.stats:
                edge = MLB_STATS.get(stat_key, {}).get("edge_threshold", 0.08)
                dirs = MLB_STATS.get(stat_key, {}).get("allowed_directions", ["over", "under"])
                if bl_cfg is not None:
                    logger.info(f"  {stat_key}: tau={bl_cfg.tau}, z_max={bl_cfg.z_max}, mw={bl_cfg.max_weight}, edge={edge}, dirs={dirs}")
                else:
                    logger.info(f"  {stat_key}: BL=None (raw model), edge={edge}, dirs={dirs}")
        logger.info("=" * 60)

        # Build per-stat BL configs and edge thresholds for requested stats
        stat_bl = {s: STAT_BL_CONFIGS.get(s, DEFAULT_BL_CONFIG) for s in cli_config.stats}
        stat_edges = {s: MLB_STATS.get(s, {}).get("edge_threshold", 0.08) for s in cli_config.stats}

        # Build allowed_bets: intersect CLI direction filter with per-stat allowed_directions from config
        config_pairs: set[tuple[str, str]] = set()
        for stat in cli_config.stats:
            per_stat_dirs = MLB_STATS.get(stat, {}).get("allowed_directions")
            dirs = per_stat_dirs if per_stat_dirs else ["over", "under"]
            for d in dirs:
                config_pairs.add((stat, d))
        if cli_allowed_bets is not None:
            combined_allowed_bets: set[tuple[str, str]] | None = config_pairs & cli_allowed_bets
        else:
            all_pairs = {(s, d) for s in cli_config.stats for d in ("over", "under")}
            combined_allowed_bets = config_pairs if config_pairs != all_pairs else None

        result = run_combined_config(
            stat_bl_configs=stat_bl,
            stat_edge_thresholds=stat_edges,
            game_dates=game_dates,
            date_predictions=date_predictions,
            date_lines=date_lines,
            date_actuals=date_actuals,
            starting_bankroll=cli_config.starting_bankroll,
            kelly_fraction=cli_config.kelly_fractions[0],
            max_bet_pct=cli_config.max_bet_pct,
            flat_bet_size=cli_config.flat_bet,
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

            result = run_single_config_fast_mlb(
                config=config,
                precomputed_df=precomputed_df,
                game_dates=game_dates,
                starting_bankroll=cli_config.starting_bankroll,
                max_bet_pct=cli_config.max_bet_pct,
                flat_bet_size=cli_config.flat_bet,
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
        starting_bankroll=cli_config.starting_bankroll,
    )

    save_results(
        results,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        phase01_time=phase01_time,
        total_predictions=total_predictions,
        total_dates=len(date_predictions),
        starting_bankroll=cli_config.starting_bankroll,
        promotion_metadata=build_promotion_contract_metadata(
            cli_config.quote_clean,
            dense_clv_linked_coverage_audit_note=cli_config.dense_clv_linked_coverage_audit_note,
        ),
    )

    total_time = phase01_time + sum(r.elapsed_seconds for r in results)
    logger.info(f"Total: {total_time:.1f}s (Phase 0-1: {phase01_time:.1f}s, Sweep: {total_time - phase01_time:.1f}s)")


if __name__ == "__main__":
    main()
