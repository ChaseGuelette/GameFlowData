#!/usr/bin/env python3
"""
Edge Refresh Job - Lightweight Edge Recalculation
==================================================
Recalculates edges and BL recommendations using stored MC samples + fresh prop lines.
Does NOT re-run inference (no model loading, no feature engineering, no MC sampling).

Designed to run after each intra-day props scrape so users see updated edges as lines move.

Usage:
    python src/orchestration/edge_refresh_job.py [--date YYYY-MM-DD] [--dry-run]

Examples:
    # Refresh edges for today
    python src/orchestration/edge_refresh_job.py

    # Dry run (compute but don't upsert)
    python src/orchestration/edge_refresh_job.py --dry-run
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402
from sqlalchemy import bindparam, create_engine, text  # noqa: E402

from src.config.combo_config import MARKET_TO_STAT, STAT_TO_MARKET  # noqa: E402
from src.models.black_litterman import BlackLittermanBlender, BLConfig  # noqa: E402
from src.models.daily_runner import should_skip_recommendation  # noqa: E402
from src.models.monte_carlo import derive_combo_samples  # noqa: E402
from src.models.prediction_store import PredictionStore  # noqa: E402

# Drift detection: re-infer when prop_line moves >= threshold from inference time
DRIFT_THRESHOLD = 1.0  # points

# Module-level cache for lazy-loaded model pipeline + predictor
_cached_pipeline = None
_cached_predictor = None

# Configure logging
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "edge_refresh.log"),
    ],
)
logger = logging.getLogger("EdgeRefreshJob")

# BL constants — must match daily_runner.py exactly
# Regular season: tau=0.5, z_max=1.0, mw=0.5, edge=0.09
# Playoffs:       tau=0.9, z_max=1.0, mw=0.8, edge=0.15
_PLAYOFF_MODE = os.getenv("NBA_PLAYOFF_MODE", "").lower() in ("true", "1", "yes")

DEFAULT_BL_TAU = 0.9 if _PLAYOFF_MODE else 0.5
DEFAULT_BL_Z_MAX = 1.0 if _PLAYOFF_MODE else 1.0
DEFAULT_BL_MAX_WEIGHT = 0.8 if _PLAYOFF_MODE else 0.5
DEFAULT_BL_EDGE_THRESHOLD = 0.15 if _PLAYOFF_MODE else 0.09

# Sanity check thresholds (must match daily_runner.py)
MAX_Q50_DIVERGENCE = 0.30
L5_ABOVE_LINE_MARGIN = 0.0

# STAT_TO_MARKET and MARKET_TO_STAT imported from src.config.combo_config


def _odds_to_prob(odds):
    """Convert American odds to implied probability."""
    if pd.isna(odds):
        return None
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def fetch_fresh_lines(engine, game_ids: list[str], stats: list[str]) -> pd.DataFrame:
    """Fetch sharpest prop lines from raw_player_props_combined.

    Replicates the query from DailyPredictionRunner._get_current_lines()
    without needing to instantiate the full runner.
    """
    markets = [STAT_TO_MARKET[s] for s in stats if s in STAT_TO_MARKET]
    if not game_ids or not markets:
        return pd.DataFrame()

    # Search both 8-digit and 10-digit game_ids (mixed formats in DB)
    game_ids_10digit = [g.zfill(10) for g in game_ids]
    game_ids_8digit = [g.lstrip("0") for g in game_ids]
    all_game_ids = list(set(game_ids_10digit + game_ids_8digit))

    query = text("""
        WITH ranked_lines AS (
            SELECT
                player_id,
                game_id,
                bookmaker,
                market_key,
                line,
                outcome_label,
                odds_american,
                snapshot_time,
                ROW_NUMBER() OVER (
                    PARTITION BY player_id, game_id, market_key, bookmaker, line, outcome_label
                    ORDER BY snapshot_time DESC
                ) as rn
            FROM raw_player_props_combined
            WHERE game_id IN :game_ids
              AND market_key IN :markets
              AND player_id IS NOT NULL
              AND snapshot_time > now() - interval '24 hours'
        )
        SELECT
            player_id,
            LPAD(game_id, 10, '0') as game_id,
            bookmaker,
            market_key,
            line,
            MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
            MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
        FROM ranked_lines
        WHERE rn = 1
        GROUP BY player_id, game_id, bookmaker, market_key, line
        HAVING MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) IS NOT NULL
           AND MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) IS NOT NULL
    """).bindparams(
        bindparam("game_ids", expanding=True),
        bindparam("markets", expanding=True),
    )

    with engine.connect() as conn:
        all_lines = pd.read_sql(query, conn, params={"game_ids": all_game_ids, "markets": list(markets)})

    if all_lines.empty:
        return all_lines

    # Select sharpest book (lowest vig) per player/game/market
    all_lines["_raw_over"] = all_lines["over_odds"].apply(_odds_to_prob)
    all_lines["_raw_under"] = all_lines["under_odds"].apply(_odds_to_prob)
    all_lines["_booksum"] = all_lines["_raw_over"] + all_lines["_raw_under"]
    all_lines = all_lines.dropna(subset=["_booksum"])

    idx = all_lines.groupby(["player_id", "game_id", "market_key"])["_booksum"].idxmin()
    best_lines = all_lines.loc[idx].drop(columns=["_raw_over", "_raw_under", "_booksum"])

    # Map market_key to stat
    best_lines["stat"] = best_lines["market_key"].map(MARKET_TO_STAT)

    return best_lines.reset_index(drop=True)


def detect_stale_predictions(
    predictions_df: pd.DataFrame,
    fresh_lines_df: pd.DataFrame,
) -> list[tuple[int, str, str]]:
    """Detect predictions where prop_line has drifted significantly from inference time.

    Compares the inference-time prop_line (baked into MC samples) against the
    current fresh line. Returns keys for predictions needing re-inference.

    Args:
        predictions_df: Stored predictions with prop_line column.
        fresh_lines_df: Fresh lines from fetch_fresh_lines() with line column.

    Returns:
        List of (player_id, game_id, stat) tuples where drift >= DRIFT_THRESHOLD.
    """
    # Guard: if prop_line column doesn't exist, nothing to detect
    if "prop_line" not in predictions_df.columns:
        return []

    # Only check base stats that have prop_line stored (not combos)
    base_preds = predictions_df[
        predictions_df["stat"].isin(["pts", "reb", "ast"])
        & predictions_df["prop_line"].notna()
    ].copy()

    if base_preds.empty:
        return []

    fresh_subset = fresh_lines_df[["player_id", "game_id", "stat", "line"]].rename(
        columns={"line": "fresh_line"}
    )
    merged = base_preds.merge(
        fresh_subset,
        on=["player_id", "game_id", "stat"],
        how="inner",
    )

    merged["drift"] = (merged["fresh_line"] - merged["prop_line"]).abs()
    stale = merged[merged["drift"] >= DRIFT_THRESHOLD]

    if not stale.empty:
        logger.info(
            f"Drift detection: {len(stale)} predictions exceed {DRIFT_THRESHOLD}-pt threshold "
            f"(max drift: {stale['drift'].max():.1f} pts)"
        )
        for _, row in stale.iterrows():
            logger.info(
                f"  {row.get('player_name', row['player_id'])} {row['stat']}: "
                f"prop_line={row['prop_line']:.1f} → fresh={row['fresh_line']:.1f} "
                f"(drift={row['drift']:.1f})"
            )

    return [
        (int(row[0]), str(row[1]), str(row[2]))
        for row in stale[["player_id", "game_id", "stat"]].itertuples(index=False, name=None)
    ]


def _get_cached_pipeline_and_predictor():
    """Lazy-load and cache the model pipeline + MC predictor.

    Loads once per process lifetime (~2-3s), then reused for all subsequent
    re-inference calls. Returns (pipeline, predictor).
    """
    global _cached_pipeline, _cached_predictor

    if _cached_pipeline is not None and _cached_predictor is not None:
        return _cached_pipeline, _cached_predictor

    from src.models.monte_carlo import (
        MonteCarloPredictor,
        load_combined_calibration_offsets,
        load_copula_params,
    )
    from src.models.quantile_trainer import PlayerPropsModelPipeline

    artifacts_path = PROJECT_ROOT / "src" / "models" / "artifacts"

    if (artifacts_path / "production" / "minutes_model.joblib").exists():
        model_path = artifacts_path / "production"
    elif (artifacts_path / "minutes_model.joblib").exists():
        model_path = artifacts_path
    else:
        runs = sorted([
            d for d in artifacts_path.iterdir()
            if d.is_dir()
            and d.name.startswith("nba_run_")
            and not d.name.endswith("_incomplete")
        ])
        if not runs:
            raise FileNotFoundError(f"No model found in {artifacts_path}")
        model_path = runs[-1]

    logger.info(f"Loading model pipeline for re-inference: {model_path.name}")
    t0 = time.perf_counter()

    # feature_store=None is fine — only used for training, not prediction
    pipeline = PlayerPropsModelPipeline.load_all(str(model_path), None)

    copula_params = load_copula_params(str(model_path))
    cal_offsets = load_combined_calibration_offsets(str(model_path))
    predictor = MonteCarloPredictor(
        pipeline,
        n_samples=10000,
        copula_params=copula_params,
        combined_calibration_offsets=cal_offsets,
    )

    logger.info(f"Pipeline loaded in {time.perf_counter() - t0:.1f}s")

    _cached_pipeline = pipeline
    _cached_predictor = predictor
    return pipeline, predictor


def _get_home_team_ids(engine, target_date: date) -> set[int]:
    """Get home team IDs for today's games from odds staging data.

    Returns set of home_team_id values.
    """
    from datetime import timedelta

    utc_start = target_date
    utc_end = target_date + timedelta(days=2)

    query = text("""
        WITH game_times AS (
            SELECT DISTINCT ON (home_team, away_team)
                home_team, away_team
            FROM raw_game_lines_staging
            WHERE commence_time >= CAST(:utc_start AS date)
              AND commence_time < CAST(:utc_end AS date)
              AND CAST(commence_time AT TIME ZONE 'US/Eastern' AS date) = CAST(:target_date AS date)
            ORDER BY home_team, away_team, snapshot_time DESC
        )
        SELECT
            t_home.team_id as home_team_id,
            t_away.team_id as away_team_id
        FROM game_times gt
        JOIN teams t_home ON t_home.team_name = gt.home_team
            OR (t_home.team_name = 'LA Clippers' AND gt.home_team = 'Los Angeles Clippers')
        JOIN teams t_away ON t_away.team_name = gt.away_team
            OR (t_away.team_name = 'LA Clippers' AND gt.away_team = 'Los Angeles Clippers')
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {
                "utc_start": utc_start,
                "utc_end": utc_end,
                "target_date": target_date,
            })
            # Build set of home team IDs
            home_teams = set()
            for row in result:
                home_teams.add(int(row[0]))
            return home_teams
    except Exception as e:
        logger.warning(f"Could not determine home teams: {e}")
        return set()


def reinfer_stale_players(
    stale_keys: list[tuple[int, str, str]],
    samples_dict: dict[tuple, np.ndarray],
    predictions_df: pd.DataFrame,
    engine,
    target_date: date,
    store: PredictionStore,
) -> tuple[dict[tuple, np.ndarray], pd.DataFrame]:
    """Re-run inference for players with stale prop_lines.

    Builds fresh features (picking up current prop_lines), runs XGBoost
    quantile prediction + MC sampling, and replaces stale entries in
    samples_dict and predictions_df.

    Args:
        stale_keys: List of (player_id, game_id, stat) needing re-inference.
        samples_dict: Current MC samples dict (modified in-place).
        predictions_df: Current predictions DataFrame.
        engine: SQLAlchemy engine.
        target_date: Target prediction date.
        store: PredictionStore for DB upserts.

    Returns:
        Updated (samples_dict, predictions_df).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from src.models.feature_store import FeatureStore

    t0 = time.perf_counter()

    # 1. Get unique (player_id, game_id) pairs and stale stats
    player_games = list({(pid, gid) for pid, gid, _ in stale_keys})
    stale_stats = list({stat for _, _, stat in stale_keys})

    logger.info(
        f"Re-inferring {len(stale_keys)} stale predictions "
        f"({len(player_games)} players, stats: {stale_stats})"
    )

    # 2. Build player info from existing predictions
    home_teams = _get_home_team_ids(engine, target_date)

    players = []
    seen = set()
    for pid, gid in player_games:
        if (pid, gid) in seen:
            continue
        seen.add((pid, gid))

        row = predictions_df[
            (predictions_df["player_id"] == pid)
            & (predictions_df["game_id"] == gid)
        ].iloc[0]

        team_id = row.get("team_id")
        opponent_id = row.get("opponent_id")
        is_home = int(team_id) in home_teams if team_id and home_teams else None

        players.append({
            "player_id": pid,
            "player_name": row.get("player_name"),
            "game_id": gid,
            "team_id": team_id,
            "opponent_id": opponent_id,
            "is_home": is_home,
            "game_time": row.get("game_time"),
        })

    # 3. Build features (parallel, same as daily_runner._build_features_df)
    feature_store = FeatureStore(engine)

    def fetch_single(player: dict) -> pd.DataFrame | None:
        try:
            features = feature_store.get_player_game_features(
                player_id=player["player_id"],
                game_id=player["game_id"],
                as_of_date=target_date,
                team_id=player.get("team_id"),
                opponent_id=player.get("opponent_id"),
                is_home=player.get("is_home"),
            )
            if features is not None:
                row_df = pd.DataFrame([features]) if isinstance(features, dict) else features
                row_df["player_id"] = player["player_id"]
                row_df["player_name"] = player["player_name"]
                row_df["game_id"] = player["game_id"]
                row_df["team_id"] = player["team_id"]
                row_df["game_time"] = player.get("game_time")
                return row_df
        except Exception as e:
            logger.error(f"Re-inference feature build failed for player {player['player_id']}: {e}")
        return None

    all_features = []
    max_workers = min(8, len(players)) if players else 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_single, p): p for p in players}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                all_features.append(result)

    if not all_features:
        logger.warning("Re-inference: could not build features for any stale player")
        return samples_dict, predictions_df

    features_df = pd.concat(all_features, ignore_index=True)
    logger.info(f"Built features for {len(features_df)} players in {time.perf_counter() - t0:.1f}s")

    # 4. Run batch prediction (XGBoost + MC sampling)
    _, predictor = _get_cached_pipeline_and_predictor()
    new_preds_list, new_samples = predictor.predict_batch_for_date(features_df, stats=stale_stats)

    if not new_samples:
        logger.warning("Re-inference produced no samples")
        return samples_dict, predictions_df

    logger.info(f"Re-inferred {len(new_samples)} sample arrays")

    # 5. Replace stale entries in samples_dict
    for key, samp in new_samples.items():
        samples_dict[key] = samp

    # 6. Update predictions_df with new quantile values + fresh prop_line
    new_preds_df = pd.DataFrame(new_preds_list)
    quantile_cols = [
        "pred_mean", "pred_std", "pred_median",
        "pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90",
    ]

    for _, new_row in new_preds_df.iterrows():
        mask = (
            (predictions_df["player_id"] == new_row["player_id"])
            & (predictions_df["game_id"] == new_row["game_id"])
            & (predictions_df["stat"] == new_row["stat"])
        )
        if mask.any():
            for col in quantile_cols:
                if col in new_row:
                    predictions_df.loc[mask, col] = new_row[col]

            # Update prop_line from fresh features
            feat_row = features_df[features_df["player_id"] == new_row["player_id"]]
            if not feat_row.empty:
                prop_line_col = f"prop_line_{new_row['stat']}"
                pl = feat_row.iloc[0].get(prop_line_col)
                if pl is not None and pl != 0:
                    predictions_df.loc[mask, "prop_line"] = float(pl)

    # 7. Re-derive combo samples for affected players
    combo_preds, combo_samps = derive_combo_samples(samples_dict, [])
    samples_dict.update(combo_samps)

    # Update combo prediction quantiles in predictions_df
    for pred in combo_preds:
        mask = (
            (predictions_df["player_id"] == pred["player_id"])
            & (predictions_df["game_id"] == pred["game_id"])
            & (predictions_df["stat"] == pred["stat"])
        )
        if mask.any():
            for col in quantile_cols:
                if col in pred:
                    predictions_df.loc[mask, col] = pred[col]

    if combo_samps:
        logger.info(f"Re-derived {len(combo_samps)} combo sample arrays")

    # 8. Persist updated samples + predictions to DB
    store.store_samples(new_samples, target_date)
    if combo_samps:
        store.store_samples(combo_samps, target_date)

    elapsed = time.perf_counter() - t0
    logger.info(f"Re-inference complete: {len(new_samples)} predictions refreshed in {elapsed:.1f}s")

    return samples_dict, predictions_df


def recalculate_edges(
    predictions_df: pd.DataFrame,
    lines_df: pd.DataFrame,
    samples_dict: dict[tuple, np.ndarray],
) -> pd.DataFrame:
    """Recalculate edges and BL recommendations with fresh lines.

    Merges fresh lines onto existing predictions, recomputes over/under
    probabilities from stored MC samples, deviggs odds, and recalculates
    raw edges + BL blended edges. Preserves all other columns (quantiles, features).
    """
    df = predictions_df.copy()

    # Preserve old line columns so we can fall back when fresh lines are missing
    line_preserve_cols = ["line", "over_odds", "under_odds", "bookmaker"]
    old_lines = {}
    for c in line_preserve_cols:
        if c in df.columns:
            old_lines[c] = df[c].copy()

    # Drop old line/edge/BL columns — we'll recalculate them
    edge_cols = [
        "line", "over_odds", "under_odds", "bookmaker",
        "over_prob", "under_prob", "implied_over", "implied_under",
        "over_edge", "under_edge",
        "bl_over_prob", "bl_under_prob", "bl_over_edge", "bl_under_edge",
        "bl_confidence", "is_recommended",
    ]
    df = df.drop(columns=[c for c in edge_cols if c in df.columns], errors="ignore")

    # Merge fresh lines
    merge_cols = ["player_id", "game_id", "stat"]
    line_cols = ["player_id", "game_id", "stat", "line", "over_odds", "under_odds", "bookmaker"]
    available_line_cols = [c for c in line_cols if c in lines_df.columns]
    df = df.merge(lines_df[available_line_cols], on=merge_cols, how="left")

    # Fill missing lines with previous values (preserves old predictions
    # when fresh lines aren't available, e.g. after games finish)
    for c, old_series in old_lines.items():
        if c in df.columns:
            df[c] = df[c].fillna(old_series)

    # Calculate over_prob from MC samples
    def estimate_over_prob(row):
        if pd.isna(row.get("line")):
            return None
        line = row["line"]
        key = (row["player_id"], row["game_id"], row["stat"])
        samples = samples_dict.get(key)
        if samples is not None and len(samples) > 0:
            prob_over = float((samples > line).mean())
            return min(max(prob_over, 0.05), 0.95)
        # Fallback: quantile interpolation
        values = [
            row.get("pred_q10"), row.get("pred_q25"), row.get("pred_q50"),
            row.get("pred_q75"), row.get("pred_q90"),
        ]
        if any(pd.isna(v) for v in values):
            return None
        if line <= values[0]:
            return 0.95
        elif line >= values[-1]:
            return 0.05
        else:
            prob_under = np.interp(line, values, [0.10, 0.25, 0.50, 0.75, 0.90])
            return 1 - prob_under

    df["over_prob"] = df.apply(estimate_over_prob, axis=1)
    df["under_prob"] = 1 - df["over_prob"]

    # Implied probabilities (multiplicative devigging)
    raw_over = df["over_odds"].apply(_odds_to_prob)
    raw_under = df["under_odds"].apply(_odds_to_prob)
    booksum = raw_over + raw_under
    df["implied_over"] = raw_over / booksum
    df["implied_under"] = raw_under / booksum

    # Raw edges
    df["over_edge"] = df["over_prob"] - df["implied_over"]
    df["under_edge"] = df["under_prob"] - df["implied_under"]

    # --- Black-Litterman blending ---
    df["bl_over_prob"] = None
    df["bl_under_prob"] = None
    df["bl_over_edge"] = None
    df["bl_under_edge"] = None
    df["bl_confidence"] = None
    df["is_recommended"] = False

    bl_config = BLConfig(tau=DEFAULT_BL_TAU, z_max=DEFAULT_BL_Z_MAX, max_weight=DEFAULT_BL_MAX_WEIGHT)
    blender = BlackLittermanBlender(config=bl_config)

    bl_computed = 0
    recommended_count = 0

    for idx, row in df.iterrows():
        if pd.isna(row.get("line")) or pd.isna(row.get("over_odds")) or pd.isna(row.get("under_odds")):
            continue

        key = (row["player_id"], row["game_id"], row["stat"])
        samples = samples_dict.get(key)
        if samples is None or len(samples) == 0:
            continue

        bl_result = blender.blend_prediction(
            samples=samples,
            line=row["line"],
            over_odds=row["over_odds"],
            under_odds=row["under_odds"],
        )

        df.at[idx, "bl_over_prob"] = bl_result["posterior_over"]
        df.at[idx, "bl_under_prob"] = bl_result["posterior_under"]
        df.at[idx, "bl_confidence"] = bl_result["confidence"]

        implied_over = row.get("implied_over")
        implied_under = row.get("implied_under")

        if pd.notna(implied_over) and pd.notna(implied_under):
            bl_over_edge = bl_result["posterior_over"] - implied_over
            bl_under_edge = bl_result["posterior_under"] - implied_under

            df.at[idx, "bl_over_edge"] = bl_over_edge
            df.at[idx, "bl_under_edge"] = bl_under_edge

            max_bl_edge = max(bl_over_edge, bl_under_edge)
            if max_bl_edge >= DEFAULT_BL_EDGE_THRESHOLD:
                rec_direction = "under" if bl_under_edge > bl_over_edge else "over"
                stat = row.get("stat")
                line_val = row.get("line")
                feat_l5 = row.get("feat_player_avg_stat_l5")
                pred_q50 = row.get("pred_q50")

                skip, reason = should_skip_recommendation(
                    stat=stat,
                    direction=rec_direction,
                    line=line_val if pd.notna(line_val) else None,
                    feat_l5=feat_l5 if pd.notna(feat_l5) else None,
                    pred_q50=pred_q50 if pd.notna(pred_q50) else None,
                )
                if skip:
                    logger.debug(
                        f"SKIP {row.get('player_name', '?')} {stat} {rec_direction}: {reason}"
                    )
                else:
                    df.at[idx, "is_recommended"] = True
                    recommended_count += 1

        bl_computed += 1

    logger.info(
        f"BL blending: {bl_computed} computed, "
        f"{recommended_count} recommended (edge >= {DEFAULT_BL_EDGE_THRESHOLD*100:.0f}%)"
    )

    return df


def refresh_injuries(engine, target_date: date) -> None:
    """Scrape today's injury data from RapidAPI and link player IDs.

    Fetches the latest injury report, upserts into rapidapi_injuries,
    and runs the injury linker to ensure new entries get player_id mapped.
    Non-fatal: errors are logged but don't stop the edge refresh.
    """
    try:
        api_key = os.getenv("RAPIDAPI_KEY")
        if not api_key:
            logger.warning("RAPIDAPI_KEY not set — skipping injury refresh")
            return

        from src.scrapers.rapidapi_injury_backfill import (
            RapidAPIInjuryClient,
            store_records,
        )

        client = RapidAPIInjuryClient(api_key=api_key, delay=0.5)
        records = client.fetch_date(target_date)

        if records is None:
            logger.warning(f"Injury fetch failed for {target_date}")
            return

        if records:
            inserted = store_records(engine, target_date, records)
            logger.info(f"Injury refresh: {len(records)} reports, {inserted} new rows")
        else:
            logger.info(f"Injury refresh: no reports for {target_date}")

        # Run the linker to map player names → player_id for any new entries
        linker_cmd = [sys.executable, str(PROJECT_ROOT / "src" / "processing" / "link_injury_data.py")]
        result = subprocess.run(
            linker_cmd, cwd=str(PROJECT_ROOT),
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode == 0:
            logger.info("Injury linker completed successfully")
        else:
            logger.warning(f"Injury linker failed (non-fatal): {result.stderr[-300:]}")

    except Exception as e:
        logger.warning(f"Injury refresh failed (non-fatal): {e}")


def get_out_player_ids(engine, target_date: date) -> set[int]:
    """Get player IDs whose most recent injury status (last 7 days) is 'Out'.

    Two-pass approach:
      1. Primary: match by player_id for linked injuries.
      2. Fallback: join unlinked injuries (player_id IS NULL) against the players
         table by name to catch the ~0.7% the linker couldn't resolve.
    """
    cutoff_date = target_date - timedelta(days=7)

    with engine.connect() as conn:
        # Pass 1: linked injuries (player_id IS NOT NULL)
        query = text("""
            WITH recent_injuries AS (
                SELECT
                    player_id,
                    status,
                    ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY report_date DESC) as rn
                FROM rapidapi_injuries
                WHERE report_date >= :cutoff_date
                  AND report_date <= :target_date
                  AND player_id IS NOT NULL
            )
            SELECT DISTINCT player_id
            FROM recent_injuries
            WHERE rn = 1 AND status = 'Out'
        """)
        result = conn.execute(query, {"target_date": target_date, "cutoff_date": cutoff_date})
        out_ids = {row[0] for row in result}

        # Pass 2: unlinked injuries matched by name against the players table
        name_query = text("""
            WITH recent_unlinked AS (
                SELECT
                    player,
                    status,
                    ROW_NUMBER() OVER (
                        PARTITION BY LOWER(TRIM(player))
                        ORDER BY report_date DESC
                    ) as rn
                FROM rapidapi_injuries
                WHERE report_date >= :cutoff_date
                  AND report_date <= :target_date
                  AND player_id IS NULL
            )
            SELECT DISTINCT p.player_id
            FROM recent_unlinked ru
            JOIN players p ON LOWER(TRIM(p.player_name)) = LOWER(TRIM(ru.player))
            WHERE ru.rn = 1 AND ru.status = 'Out'
        """)
        result = conn.execute(name_query, {"target_date": target_date, "cutoff_date": cutoff_date})
        name_matched = {row[0] for row in result}

        if name_matched:
            logger.info(f"Found {len(name_matched)} additional Out players via name matching")
            out_ids.update(name_matched)

        return out_ids


def filter_out_players(
    engine,
    predictions: pd.DataFrame,
    samples_dict: dict,
    target_date: date,
) -> tuple[pd.DataFrame, dict]:
    """Remove predictions for players currently marked as 'Out' and delete them from DB.

    Returns filtered predictions DataFrame and samples dict.
    """
    out_ids = get_out_player_ids(engine, target_date)
    if not out_ids:
        logger.info("No 'Out' players found in injury reports")
        return predictions, samples_dict

    # Find predictions to remove
    out_mask = predictions["player_id"].isin(out_ids)
    n_removed = out_mask.sum()

    if n_removed == 0:
        logger.info(f"{len(out_ids)} players marked Out, none had active predictions")
        return predictions, samples_dict

    # Log which players are being removed
    removed_players = predictions.loc[out_mask, ["player_id", "player_name", "stat"]].drop_duplicates()
    for _, row in removed_players.iterrows():
        logger.info(f"  Removing prediction: {row.get('player_name', row['player_id'])} ({row['stat']})")

    # Filter predictions DataFrame
    filtered = predictions[~out_mask].reset_index(drop=True)

    # Filter samples dict
    filtered_samples = {
        k: v for k, v in samples_dict.items()
        if k[0] not in out_ids  # key is (player_id, game_id, stat)
    }

    # Delete from database so dashboard stops showing them
    delete_query = text("""
        DELETE FROM daily_predictions
        WHERE prediction_date = :prediction_date
          AND player_id = ANY(:player_ids)
    """)
    player_id_list = list(out_ids)
    with engine.begin() as conn:
        result = conn.execute(delete_query, {
            "prediction_date": target_date,
            "player_ids": player_id_list,
        })
        logger.info(
            f"Removed {n_removed} predictions for {len(out_ids)} Out players "
            f"({result.rowcount} rows deleted from DB)"
        )

    return filtered, filtered_samples


def backfill_game_times(engine, predictions: pd.DataFrame, target_date: date) -> pd.DataFrame:
    """Backfill NULL game_time values from raw_game_lines_staging.

    The inference job enriches game_time once, but if odds data hasn't been
    scraped yet at that point, predictions get permanent NULL game_time.
    This function re-checks the staging table on each edge refresh so that
    game_time is populated as soon as odds data becomes available.

    Non-fatal: returns predictions unchanged on error.
    """
    if "game_time" not in predictions.columns:
        return predictions

    missing_mask = predictions["game_time"].isna()
    n_missing = missing_mask.sum()
    if n_missing == 0:
        return predictions

    logger.info(f"Found {n_missing} predictions with missing game_time — attempting backfill")

    try:
        from datetime import timedelta

        utc_start = target_date
        utc_end = target_date + timedelta(days=2)

        query = text("""
            WITH game_times AS (
                SELECT DISTINCT ON (home_team, away_team)
                    home_team,
                    away_team,
                    commence_time
                FROM raw_game_lines_staging
                WHERE commence_time >= CAST(:utc_start AS date)
                  AND commence_time < CAST(:utc_end AS date)
                  AND CAST(commence_time AT TIME ZONE 'US/Eastern' AS date) = CAST(:target_date AS date)
                ORDER BY home_team, away_team, snapshot_time DESC
            )
            SELECT
                t_home.team_id as home_team_id,
                t_away.team_id as away_team_id,
                gt.commence_time
            FROM game_times gt
            JOIN teams t_home ON t_home.team_name = gt.home_team
                OR (t_home.team_name = 'LA Clippers' AND gt.home_team = 'Los Angeles Clippers')
            JOIN teams t_away ON t_away.team_name = gt.away_team
                OR (t_away.team_name = 'LA Clippers' AND gt.away_team = 'Los Angeles Clippers')
        """)

        with engine.connect() as conn:
            result = conn.execute(query, {
                "utc_start": utc_start,
                "utc_end": utc_end,
                "target_date": target_date,
            })
            # Build lookup: (home_team_id, away_team_id) → commence_time
            time_lookup = {(row[0], row[1]): row[2] for row in result}

        if not time_lookup:
            logger.info("No game times in staging table, trying raw_player_props_combined...")
            # Fallback: get times directly by game_id from props table
            missing_game_ids = list(predictions.loc[missing_mask, "game_id"].unique())
            if missing_game_ids:
                props_query = text("""
                    SELECT g.gid as game_id, t.commence_time
                    FROM unnest(:game_ids) AS g(gid)
                    CROSS JOIN LATERAL (
                        SELECT rp.commence_time
                        FROM raw_player_props_combined rp
                        WHERE rp.game_id = g.gid
                          AND rp.commence_time IS NOT NULL
                          AND rp.snapshot_time > NOW() - interval '48 hours'
                        LIMIT 1
                    ) t
                """)
                with engine.connect() as conn:
                    props_result = conn.execute(props_query, {"game_ids": missing_game_ids})
                    props_time_map = {row[0]: row[1] for row in props_result}

                if props_time_map:
                    filled = 0
                    for idx in predictions.index[missing_mask]:
                        gid = predictions.at[idx, "game_id"]
                        ct = props_time_map.get(gid)
                        if ct is not None:
                            predictions.at[idx, "game_time"] = ct
                            filled += 1
                    logger.info(f"Backfilled game_time for {filled}/{n_missing} predictions from props table")
                    return predictions

            logger.info("No game times found in any source for backfill")
            return predictions

        # Map game_id → (team_id, opponent_id) from predictions themselves.
        # Each prediction has team_id and opponent_id; we need to figure out
        # which is home/away. Try both orderings against the lookup.
        game_team_map: dict[str, tuple[int, int]] = {}
        for _, row in predictions[["game_id", "team_id", "opponent_id"]].drop_duplicates("game_id").iterrows():
            game_team_map[row["game_id"]] = (row["team_id"], row["opponent_id"])

        game_time_map: dict[str, object] = {}
        for game_id, (tid, oid) in game_team_map.items():
            # Try team as home, opponent as away
            ct = time_lookup.get((tid, oid))
            if ct is None:
                # Try opponent as home, team as away
                ct = time_lookup.get((oid, tid))
            if ct is not None:
                game_time_map[game_id] = ct

        if not game_time_map:
            logger.info("Staging table had times but none matched prediction games")
            return predictions

        # Apply backfill only to rows with missing game_time
        filled = 0
        for idx in predictions.index[missing_mask]:
            gid = predictions.at[idx, "game_id"]
            ct = game_time_map.get(gid)
            if ct is not None:
                predictions.at[idx, "game_time"] = ct
                filled += 1

        logger.info(f"Backfilled game_time for {filled}/{n_missing} predictions")
        return predictions

    except Exception as e:
        logger.warning(f"game_time backfill failed (non-fatal): {e}")
        return predictions


def main():
    parser = argparse.ArgumentParser(
        description="Edge Refresh Job - Recalculate edges with fresh lines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date",
        type=str,
        default=str(date.today()),
        help="Target date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute edges but don't upsert to database",
    )
    parser.add_argument(
        "--stats",
        type=str,
        nargs="+",
        default=["pts", "reb", "ast"],
        help="Stats to refresh (default: pts reb ast)",
    )
    parser.add_argument(
        "--skip-paper",
        action="store_true",
        help="Skip paper trading step (bet selection + placement)",
    )
    parser.add_argument(
        "--skip-discord",
        action="store_true",
        help="Skip Discord alert",
    )
    parser.add_argument(
        "--skip-reinference",
        action="store_true",
        help="Skip re-inference of stale predictions (drift detection only logs, no re-run)",
    )
    args = parser.parse_args()

    start_time = time.time()
    target_date = date.fromisoformat(args.date)

    logger.info("=" * 60)
    logger.info(f"EDGE REFRESH JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target Date: {target_date} | Stats: {args.stats}")
    logger.info(
        f"BL Config: tau={DEFAULT_BL_TAU}, z_max={DEFAULT_BL_Z_MAX}, "
        f"mw={DEFAULT_BL_MAX_WEIGHT}, edge={DEFAULT_BL_EDGE_THRESHOLD} "
        f"({'PLAYOFF' if _PLAYOFF_MODE else 'regular season'})"
    )
    logger.info("=" * 60)

    try:
        load_dotenv()
        DATABASE_URL = os.getenv("DATABASE_URL")
        if not DATABASE_URL:
            logger.error("Missing DATABASE_URL")
            sys.exit(1)

        engine = create_engine(DATABASE_URL)
        store = PredictionStore(engine)

        # 0. DFS paper trading (market-edge — runs independently of model inference)
        if not args.dry_run:
            try:
                from src.paper_trading.dfs_paper_trader import DfsPaperTrader

                dfs_trader = DfsPaperTrader()

                logger.info("Resolving pending DFS entries from previous days...")
                dfs_res = dfs_trader.resolve_all_pending(exclude_today=True)
                if dfs_res["total_resolved"] > 0:
                    logger.info(
                        f"Resolved {dfs_res['total_resolved']} DFS entries across "
                        f"{dfs_res['dates_processed']} days "
                        f"({dfs_res['total_won']}W {dfs_res['total_lost']}L "
                        f"{dfs_res['total_partial']}P)"
                    )

                logger.info("Building DFS entries for today...")
                entries = dfs_trader.build_entries(target_date)
                if entries:
                    count = dfs_trader.place_entries(entries)
                    logger.info(f"Placed {count} DFS entries for {target_date}")
                else:
                    logger.info("No DFS entries meet edge criteria")
            except Exception as e:
                logger.warning(f"DFS paper trading step failed: {e} (non-fatal)")

        # 1. Load stored MC samples
        logger.info("Loading stored MC samples...")
        samples_dict = store.get_all_samples_for_date(target_date)

        if not samples_dict:
            logger.info(
                f"EDGE REFRESH NO-OP: No MC samples found for {target_date}. "
                "Inference must run before edge refresh can recalculate. "
                "This is expected before the first inference run of the day."
            )
            sys.exit(0)

        # Check MC sample staleness (warn if >6 hours old)
        try:
            with engine.connect() as conn:
                sample_age = conn.execute(text(
                    "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) / 3600.0 "
                    "FROM mc_samples WHERE prediction_date = :target_date"
                ), {"target_date": target_date}).scalar()

                if sample_age is not None and sample_age > 6:
                    logger.warning(
                        f"MC samples are {sample_age:.1f} hours old — "
                        "edge calculations may be based on stale inference."
                    )
                    # Send Discord warning for stale MC samples
                    try:
                        if os.getenv("DISCORD_BOT_TOKEN"):
                            from src.discord_bot.alerts import send_job_alert_sync
                            send_job_alert_sync(
                                job_name="Edge Refresh (Stale Samples Warning)",
                                success=True,
                                duration_seconds=0,
                                error_message=(
                                    f"MC samples are {sample_age:.1f} hours old. "
                                    "Edge recalculations use stale inference data."
                                ),
                            )
                    except Exception as e:
                        logger.warning(f"Stale samples Discord alert failed: {e} (non-fatal)")
                elif sample_age is not None:
                    logger.info(f"MC samples are {sample_age:.1f} hours old")
        except Exception as e:
            logger.warning(f"Could not check MC sample age: {e} (non-fatal)")

        # 2. Load stored predictions
        logger.info("Loading stored predictions...")
        predictions = store.get_predictions(target_date)

        if predictions.empty:
            logger.warning(f"No predictions found for {target_date}. Exiting gracefully.")
            sys.exit(0)

        # 2a. Derive combo samples from stored base stat samples
        combo_preds, combo_samps = derive_combo_samples(samples_dict, [])
        samples_dict.update(combo_samps)
        if combo_samps:
            logger.info(f"Derived {len(combo_samps)} combo sample arrays from base stats")

        logger.info(f"Loaded {len(predictions)} predictions, {len(samples_dict)} sample arrays")

        # 2b. Backfill missing game_time from staging table
        predictions = backfill_game_times(engine, predictions, target_date)

        # 3. Refresh injury data and filter out players now marked as 'Out'
        if not args.dry_run:
            logger.info("Refreshing injury data...")
            refresh_injuries(engine, target_date)

        logger.info("Checking for Out players...")
        predictions, samples_dict = filter_out_players(
            engine, predictions, samples_dict, target_date
        )

        if predictions.empty:
            logger.warning("All predictions filtered (all players Out?). Exiting.")
            sys.exit(0)

        # 4. Get unique game_ids from predictions
        game_ids = predictions["game_id"].astype(str).unique().tolist()

        # 5. Fetch fresh lines
        logger.info("Fetching fresh prop lines...")
        t0 = time.perf_counter()
        fresh_lines = fetch_fresh_lines(engine, game_ids, args.stats)
        logger.info(f"Fetched {len(fresh_lines)} fresh lines in {time.perf_counter() - t0:.1f}s")

        if fresh_lines.empty:
            logger.warning("No fresh lines found. Exiting gracefully.")
            sys.exit(0)

        # 5a. Detect prop_line drift and re-infer stale players
        if not args.skip_reinference and "prop_line" in predictions.columns:
            stale_keys = detect_stale_predictions(predictions, fresh_lines)

            if stale_keys:
                logger.info(
                    f"Detected {len(stale_keys)} stale predictions — "
                    "triggering selective re-inference..."
                )
                if not args.dry_run:
                    samples_dict, predictions = reinfer_stale_players(
                        stale_keys, samples_dict, predictions,
                        engine, target_date, store,
                    )
                else:
                    logger.info("[DRY RUN] Skipping re-inference")
            else:
                logger.info("Drift detection: all predictions within threshold")
        elif args.skip_reinference:
            logger.info("Re-inference skipped (--skip-reinference)")

        # 6. Recalculate edges + BL
        logger.info("Recalculating edges and BL recommendations...")
        updated = recalculate_edges(predictions, fresh_lines, samples_dict)

        # Count how many edges changed
        has_edge = updated["over_edge"].notna().sum()
        has_rec = updated["is_recommended"].sum()
        logger.info(f"Updated: {has_edge} predictions with edges, {has_rec} recommended")

        # 7. Upsert to database
        if not args.dry_run:
            logger.info("Upserting updated predictions...")
            store.store_predictions(updated, target_date)
        else:
            logger.info("[DRY RUN] Skipping database upsert")

        # 7b. Resolve past pending bets, then place new bets
        if not args.dry_run and not args.skip_paper:
            try:
                from src.paper_trading.paper_trader import PaperTrader

                trader = PaperTrader()

                # Resolve any pending bets from PREVIOUS days (exclude_today=True
                # so we never falsely resolve today's games that haven't finished)
                logger.info("Resolving pending bets from previous days...")
                res = trader.resolve_all_pending(exclude_today=True)
                if res["total_resolved"] > 0:
                    logger.info(
                        f"Resolved {res['total_resolved']} bets across {res['dates_processed']} days "
                        f"({res['total_won']}W {res['total_lost']}L {res['total_push']}P)"
                    )

                # Place / update paper bets for today's predictions
                logger.info("Placing paper bets on recommended predictions...")
                bets = trader.select_bets(target_date)
                if bets:
                    count = trader.place_bets(bets)
                    logger.info(f"Placed {count} paper bets for {target_date}")
                else:
                    logger.info("No predictions meet edge threshold for paper bets")
            except Exception as e:
                logger.warning(f"Paper trading step failed: {e} (non-fatal)")

        # 8. Export CSV backup
        output_dir = Path("predictions")
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / f"predictions_{target_date}.csv"
        updated.to_csv(output_file, index=False)
        logger.info(f"Exported CSV: {output_file}")

        # 9. Discord alert (reuse predictions alert)
        if not args.dry_run and not args.skip_discord:
            try:
                if os.getenv("DISCORD_BOT_TOKEN"):
                    from src.discord_bot.alerts import send_predictions_alert_sync
                    logger.info("Sending Discord alert...")
                    success = send_predictions_alert_sync(updated, target_date)
                    if success:
                        logger.info("Discord alert sent successfully")
                    else:
                        logger.warning("Discord alert failed (non-fatal)")
                else:
                    logger.debug("Discord not configured, skipping alert")
            except Exception as e:
                logger.warning(f"Discord alert failed: {e} (non-fatal)")

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"EDGE REFRESH JOB COMPLETED SUCCESSFULLY ({elapsed:.1f}s)")
        logger.info(f"  Predictions updated: {has_edge}")
        logger.info(f"  Recommended picks: {has_rec}")
        logger.info("=" * 60)

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error("=" * 60)
        logger.error(f"EDGE REFRESH JOB FAILED ({elapsed:.1f}s)")
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
