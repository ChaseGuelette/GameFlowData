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

from dotenv import load_dotenv
from sqlalchemy import bindparam, create_engine, text

from src.models.black_litterman import BlackLittermanBlender, BLConfig
from src.models.prediction_store import PredictionStore

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

# BL constants (must match daily_runner.py)
DEFAULT_BL_TAU = 0.5
DEFAULT_BL_Z_MAX = 1.0
DEFAULT_BL_EDGE_THRESHOLD = 0.09

STAT_TO_MARKET = {
    "pts": "player_points",
    "reb": "player_rebounds",
    "ast": "player_assists",
}

MARKET_TO_STAT = {v: k for k, v in STAT_TO_MARKET.items()}


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
                    PARTITION BY player_id, game_id, market_key, bookmaker, outcome_label
                    ORDER BY snapshot_time DESC
                ) as rn
            FROM raw_player_props_combined
            WHERE game_id IN :game_ids
              AND market_key IN :markets
              AND player_id IS NOT NULL
        )
        SELECT
            player_id,
            LPAD(game_id, 10, '0') as game_id,
            bookmaker,
            market_key,
            MAX(line) as line,
            MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
            MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
        FROM ranked_lines
        WHERE rn = 1
        GROUP BY player_id, game_id, bookmaker, market_key
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

    bl_config = BLConfig(tau=DEFAULT_BL_TAU, z_max=DEFAULT_BL_Z_MAX)
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
        "--skip-discord",
        action="store_true",
        help="Skip Discord alert",
    )
    args = parser.parse_args()

    start_time = time.time()
    target_date = date.fromisoformat(args.date)

    logger.info("=" * 60)
    logger.info(f"EDGE REFRESH JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target Date: {target_date} | Stats: {args.stats}")
    logger.info("=" * 60)

    try:
        load_dotenv()
        DATABASE_URL = os.getenv("DATABASE_URL")
        if not DATABASE_URL:
            logger.error("Missing DATABASE_URL")
            sys.exit(1)

        engine = create_engine(DATABASE_URL)
        store = PredictionStore(engine)

        # 1. Load stored MC samples
        logger.info("Loading stored MC samples...")
        samples_dict = store.get_all_samples_for_date(target_date)

        if not samples_dict:
            logger.warning(
                f"No MC samples found for {target_date}. "
                "Inference must run before edge refresh. Exiting gracefully."
            )
            sys.exit(0)

        # 2. Load stored predictions
        logger.info("Loading stored predictions...")
        predictions = store.get_predictions(target_date)

        if predictions.empty:
            logger.warning(f"No predictions found for {target_date}. Exiting gracefully.")
            sys.exit(0)

        logger.info(f"Loaded {len(predictions)} predictions, {len(samples_dict)} sample arrays")

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
        if not args.dry_run:
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
