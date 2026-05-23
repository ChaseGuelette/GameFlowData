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

from src.backtesting.bet_simulator import BetSimulator
from src.backtesting.mlb.mlb_backtest_harness import STAT_ACTUALS
from src.backtesting.mlb.backtest_data_loader import (
    fetch_actuals_by_date,
    fetch_game_dates,
    fetch_games_for_date,
)
from src.backtesting.mlb.edge_engine import (
    build_config_edge_frame,
    compute_edges_for_config,
    odds_to_prob,
    precompute_mlb_base_probs,
    select_sharpest_line,
)
from src.backtesting.mlb.matchup_cache import build_matchup_cache
from src.backtesting.mlb.prediction_cache import DatePrediction, build_predictions_for_date
from src.backtesting.mlb.quote_decision_policy import (
    build_fixed_cutoff_ts,
    build_slate_decision_ts,
    decision_time_for_game,
)
from src.backtesting.mlb.quote_clean_line_service import fetch_lines_for_date
from src.backtesting.mlb.sweep_results import SweepResult, print_comparison_table, save_results
from src.backtesting.mlb.sweep_config import (
    SweepConfig,
    build_arg_parser,
    build_sweep_grid,
    parse_sweep_cli_config,
)
from src.backtesting.performance_metrics import MetricsCalculator
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

# Stats whose sportsbook market_key in mlb_raw_player_props differs from the internal stat name
STAT_TO_MARKET_KEY: dict[str, str] = {
    "batter_hrr": "batter_hits_runs_rbis",
}

_MIN_PROB: float = 1e-6
_MAX_PROB: float = 1.0 - 1e-6

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
        quote_clean_cutoff_ts = _build_quote_clean_cutoff_ts(game_date, quote_clean_cutoff_time_et)

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
    lines_df = _fetch_lines_for_date(
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


def _build_quote_clean_cutoff_ts(game_date: date, cutoff_time_et: str) -> datetime:
    """Compatibility wrapper for migrated quote decision policy helper."""
    return build_fixed_cutoff_ts(game_date, cutoff_time_et)


def _build_slate_decision_ts(commence_ts: datetime, fallback_relative_minutes: int = 60) -> datetime:
    """Compatibility wrapper for migrated quote decision policy helper."""
    return build_slate_decision_ts(commence_ts, fallback_relative_minutes=fallback_relative_minutes)


def _game_decision_time(
    game: dict,
    *,
    policy: str,
    fixed_cutoff_ts: datetime | None,
    relative_minutes: int,
) -> datetime | None:
    """Compatibility wrapper for migrated quote decision policy helper."""
    return decision_time_for_game(
        game,
        policy=policy,
        fixed_cutoff_ts=fixed_cutoff_ts,
        relative_minutes=relative_minutes,
    )


def _fetch_lines_for_date(
    engine,
    games: list[dict] | None = None,
    market_keys: list[str] | None = None,
    quote_clean_cutoff_ts: datetime | None = None,
    quote_clean_cutoff_time_et: str | None = None,
    quote_decision_policy: str = "fixed_et",
    quote_relative_minutes: int = 60,
    line_source: str = "mlb_raw_player_props",
    *,
    game_ids: list[int] | None = None,
) -> pd.DataFrame:
    """Compatibility wrapper for migrated quote-clean line service."""
    return fetch_lines_for_date(
        engine,
        games=games,
        market_keys=market_keys,
        quote_clean_cutoff_ts=quote_clean_cutoff_ts,
        quote_clean_cutoff_time_et=quote_clean_cutoff_time_et,
        quote_decision_policy=quote_decision_policy,
        quote_relative_minutes=quote_relative_minutes,
        line_source=line_source,
        game_ids=game_ids,
    )


# ---------------------------------------------------------------------------
# Edge calculation compatibility wrappers
# ---------------------------------------------------------------------------

def _odds_to_prob(odds: float) -> float:
    """Compatibility wrapper for migrated edge helper."""
    return odds_to_prob(odds)


def _select_sharpest_line(
    lines: pd.DataFrame,
    player_id: int,
    game_id: int,
    market_key: str,
) -> dict | None:
    """Compatibility wrapper for migrated edge helper."""
    return select_sharpest_line(lines, player_id, game_id, market_key)


# ---------------------------------------------------------------------------
# Fast path: precompute once, vectorized BL per config
# ---------------------------------------------------------------------------


def run_single_config_fast_mlb(
    config: SweepConfig,
    precomputed_df: pd.DataFrame,
    game_dates: list[date],
    starting_bankroll: float,
    allowed_bets: set[tuple[str, str]] | None = None,
    max_bet_pct: float | None = None,
    flat_bet_size: float | None = None,
) -> SweepResult:
    """Vectorized config evaluation using precomputed base probabilities.

    Replaces run_single_config: skips iterrows, rebuilds no lookup dicts per-config.
    """
    t0 = time.time()

    if precomputed_df.empty:
        empty_metrics = MetricsCalculator().calculate(pd.DataFrame(), pd.DataFrame(), starting_bankroll=starting_bankroll)
        return SweepResult(
            config=config, metrics=empty_metrics,
            bets_df=pd.DataFrame(), predictions_df=pd.DataFrame(),
            elapsed_seconds=time.time() - t0,
        )

    df = build_config_edge_frame(config, precomputed_df)

    # Build actuals lookup once — passed to _resolve_bets_from_lookup each day
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
        config=config, metrics=metrics,
        bets_df=bets_df, predictions_df=predictions_df,
        elapsed_seconds=elapsed,
    )


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

    # Build per-stat blenders (None means raw model, no BL)
    stat_blenders: dict[str, BlackLittermanBlender | None] = {}
    for stat_key, bl_cfg in stat_bl_configs.items():
        stat_blenders[stat_key] = BlackLittermanBlender(config=bl_cfg) if bl_cfg is not None else None

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

    # 2. Model files directly in base_dir (run dir passed directly, or legacy layout)
    if (base / "pitcher_k_model.joblib").exists():
        return base
    if any(base.glob("*_binomial_booster.json")):
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
    engine = get_engine(local=cli_config.local)
    if cli_config.local:
        logger.info("Using LOCAL database")
    pitcher_feature_store = MLBFeatureStore(engine)

    model_path = find_latest_model_dir(cli_config.model_dir)
    logger.info(f"Using model directory: {model_path}")

    suite = MLBModelSuite.from_directory(model_path, n_samples=cli_config.n_samples)
    logger.info(f"Suite loaded stats: {suite.available_stats}")

    # Only create batter feature store if we have batter stats to predict
    has_batter_stats = any(s.startswith("batter_") and suite.has_stat(s) for s in cli_config.stats)
    batter_feature_store = MLBBatterFeatureStore(engine) if has_batter_stats else None

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
    )

    total_time = phase01_time + sum(r.elapsed_seconds for r in results)
    logger.info(f"Total: {total_time:.1f}s (Phase 0-1: {phase01_time:.1f}s, Sweep: {total_time - phase01_time:.1f}s)")


if __name__ == "__main__":
    main()
