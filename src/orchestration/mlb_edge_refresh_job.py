#!/usr/bin/env python3
"""
MLB Edge Refresh Job - Lightweight BL Re-blending
===================================================
Re-runs Black-Litterman blending with stored MC samples + fresh prop lines
so is_recommended stays current as lines move between inference runs.

Does NOT load MLB model suite or run inference — samples-only re-blending.

Usage:
    python src/orchestration/mlb_edge_refresh_job.py [--date YYYY-MM-DD] [--dry-run] [--skip-discord]
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

import argparse
import logging
import time
from datetime import date, datetime

import pandas as pd
from sqlalchemy import text

from src.discord_bot.alerts import send_predictions_alert_sync

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "mlb_edge_refresh.log"),
    ],
)
logger = logging.getLogger("MLBEdgeRefreshJob")


def odds_to_prob(odds):
    if pd.isna(odds):
        return None
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


def main():
    parser = argparse.ArgumentParser(
        description="MLB Edge Refresh Job - Re-blend BL with stored samples + fresh lines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date", type=str, default=str(date.today()),
        help="Target date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute but don't write to DB")
    parser.add_argument("--skip-discord", action="store_true", help="Suppress Discord alert")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    start_time = time.time()
    logger.info("=" * 60)
    logger.info(f"MLB EDGE REFRESH JOB START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target Date: {target_date}")
    logger.info("=" * 60)

    try:
        from src.db.client import get_engine
        from src.models.black_litterman import BlackLittermanBlender
        from src.models.daily_runner import should_skip_recommendation
        from src.models.mlb.mlb_prediction_store import MLBPredictionStore
        from src.models.mlb.mlb_stat_config import MLB_STATS, STAT_BL_CONFIGS

        engine = get_engine()
        store = MLBPredictionStore(engine)

        # Step 1 — Load stored predictions
        logger.info("Loading stored MLB predictions...")
        preds_df = store.get_predictions(target_date)
        if preds_df.empty:
            logger.info(f"No MLB predictions found for {target_date}. Exiting.")
            return
        logger.info(f"Loaded {len(preds_df)} predictions")

        # Step 2 — Load stored MC samples
        logger.info("Loading stored MC samples...")
        samples_dict = store.get_all_samples_for_date(target_date)
        if not samples_dict:
            logger.info(f"No MC samples found for {target_date}. Exiting.")
            return
        logger.info(f"Loaded {len(samples_dict)} sample arrays")

        # Step 3 — Fetch fresh prop lines
        logger.info("Fetching fresh prop lines...")
        fresh_query = text("""
            WITH ranked_lines AS (
                SELECT
                    player_id,
                    bookmaker,
                    market_key,
                    line,
                    outcome_label,
                    odds_american,
                    snapshot_time,
                    ROW_NUMBER() OVER (
                        PARTITION BY player_id, market_key, bookmaker, line, outcome_label
                        ORDER BY snapshot_time DESC
                    ) AS rn
                FROM mlb_raw_player_props
                WHERE game_date = :target_date
                  AND player_id IS NOT NULL
            )
            SELECT
                player_id,
                market_key AS stat,
                line,
                bookmaker,
                MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) AS over_odds,
                MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) AS under_odds
            FROM ranked_lines
            WHERE rn = 1
            GROUP BY player_id, market_key, line, bookmaker
            HAVING MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) IS NOT NULL
               AND MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) IS NOT NULL
        """)

        with engine.connect() as conn:
            result = conn.execute(fresh_query.bindparams(target_date=target_date))
            fresh_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        fresh_lines = {}
        for _, row in fresh_df.iterrows():
            key = (int(row["player_id"]), row["stat"])
            fresh_lines[key] = {
                "line": row["line"],
                "over_odds": row["over_odds"],
                "under_odds": row["under_odds"],
                "bookmaker": row.get("bookmaker"),
            }

        logger.info(f"Found fresh lines for {len(fresh_lines)} player-market combos")

        # Step 4 — Merge fresh lines onto predictions
        fresh_count = 0
        for idx, row in preds_df.iterrows():
            key = (int(row["player_id"]), row["stat"])
            if key in fresh_lines:
                fl = fresh_lines[key]
                preds_df.at[idx, "line"] = fl["line"]
                preds_df.at[idx, "over_odds"] = fl["over_odds"]
                preds_df.at[idx, "under_odds"] = fl["under_odds"]
                if "bookmaker" in preds_df.columns:
                    preds_df.at[idx, "bookmaker"] = fl["bookmaker"]
                fresh_count += 1

        logger.info(f"Fresh lines applied to {fresh_count} predictions")

        # Step 5 — Re-run BL blending
        logger.info("Re-running BL blending...")

        stat_blenders: dict[str, BlackLittermanBlender | None] = {}
        for stat_key, bl_cfg in STAT_BL_CONFIGS.items():
            stat_blenders[stat_key] = BlackLittermanBlender(config=bl_cfg) if bl_cfg is not None else None

        bl_computed = 0
        recommended_count = 0
        no_samples_keys = set()

        for idx, row in preds_df.iterrows():
            if pd.isna(row.get("line")) or pd.isna(row.get("over_odds")) or pd.isna(row.get("under_odds")):
                preds_df.at[idx, "is_recommended"] = False
                continue

            key = (int(row["player_id"]), int(row["game_id"]), row["stat"])
            samples = samples_dict.get(key)

            if samples is None or len(samples) == 0:
                no_samples_keys.add(key)
                preds_df.at[idx, "is_recommended"] = False
                continue

            stat = row["stat"]
            blender_cfg = STAT_BL_CONFIGS.get(stat)
            line = float(row["line"])
            over_odds = float(row["over_odds"])
            under_odds = float(row["under_odds"])

            if blender_cfg is None:
                posterior_over = float((samples > line).mean())
                posterior_under = 1.0 - posterior_over
                bl_confidence = 1.0
            else:
                blender = BlackLittermanBlender(config=blender_cfg)
                bl_result = blender.blend_prediction(
                    samples=samples, line=line, over_odds=over_odds, under_odds=under_odds
                )
                posterior_over = bl_result["posterior_over"]
                posterior_under = bl_result["posterior_under"]
                bl_confidence = bl_result["confidence"]

            preds_df.at[idx, "bl_over_prob"] = posterior_over
            preds_df.at[idx, "bl_under_prob"] = posterior_under
            preds_df.at[idx, "bl_confidence"] = bl_confidence

            raw_over = odds_to_prob(over_odds)
            raw_under = odds_to_prob(under_odds)
            if raw_over is None or raw_under is None:
                preds_df.at[idx, "is_recommended"] = False
                continue

            booksum = raw_over + raw_under
            implied_over = raw_over / booksum
            implied_under = raw_under / booksum

            bl_over_edge = posterior_over - implied_over
            bl_under_edge = posterior_under - implied_under

            preds_df.at[idx, "bl_over_edge"] = bl_over_edge
            preds_df.at[idx, "bl_under_edge"] = bl_under_edge

            stat_config = MLB_STATS.get(stat, {})
            edge_threshold = stat_config.get("edge_threshold", 0.08)
            allowed_dirs = stat_config.get("allowed_directions")

            if bl_over_edge >= bl_under_edge:
                best_dir, best_edge = "over", bl_over_edge
            else:
                best_dir, best_edge = "under", bl_under_edge

            if allowed_dirs and best_dir not in allowed_dirs:
                other_dir = "under" if best_dir == "over" else "over"
                other_edge = bl_under_edge if best_dir == "over" else bl_over_edge
                if other_dir in allowed_dirs:
                    best_dir, best_edge = other_dir, other_edge
                else:
                    best_edge = -1

            if best_edge >= edge_threshold:
                line_val = float(row["line"]) if pd.notna(row.get("line")) else None
                feat_l5 = row.get("feat_player_avg_stat_l5")

                skip, reason = should_skip_recommendation(
                    stat=stat,
                    direction=best_dir,
                    line=line_val,
                    feat_l5=float(feat_l5) if pd.notna(feat_l5) else None,
                )

                if not skip and best_dir == "over" and line_val is not None and line_val <= 0.5:
                    feat_l5_val = float(feat_l5) if pd.notna(feat_l5) else None
                    if feat_l5_val is None or feat_l5_val <= 0.1:
                        skip = True
                        reason = f"FILTER [MLB_COLD_OVER]: {stat} over {line_val} with L5={feat_l5_val}"

                if skip:
                    logger.debug(f"SKIP {row.get('player_name', '?')} {stat} {best_dir}: {reason}")
                    preds_df.at[idx, "is_recommended"] = False
                else:
                    preds_df.at[idx, "is_recommended"] = True
                    recommended_count += 1
            else:
                preds_df.at[idx, "is_recommended"] = False

            bl_computed += 1

        # Step 6 — Mark predictions with no samples as not recommended
        for idx, row in preds_df.iterrows():
            key = (int(row["player_id"]), int(row["game_id"]), row["stat"])
            if key in no_samples_keys:
                preds_df.at[idx, "is_recommended"] = False

        logger.info(f"BL blending: {bl_computed} computed, {recommended_count} recommended")

        # Upsert
        upsert_cols = [
            "prediction_date", "player_id", "game_id", "stat", "line",
            "over_odds", "under_odds", "bookmaker",
            "bl_over_prob", "bl_under_prob", "bl_over_edge", "bl_under_edge",
            "bl_confidence", "is_recommended",
        ]

        updated_df = preds_df[upsert_cols].copy()

        if not args.dry_run:
            logger.info("Upserting updated predictions...")
            store.store_predictions(updated_df, target_date)

            recommended = preds_df[preds_df["is_recommended"]]
            if not recommended.empty:
                try:
                    send_predictions_alert_sync(recommended, target_date, sport="mlb")
                except Exception as exc:
                    logger.warning(f"Discord alert failed (non-fatal): {exc}")
        else:
            logger.info("[DRY RUN] Skipping database upsert")

        # Step 7 — Log summary
        by_stat = preds_df[preds_df["is_recommended"]].groupby("stat").size().to_dict()
        stat_summary = " ".join(f"{k}={v}" for k, v in sorted(by_stat.items()))

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"MLB EDGE REFRESH COMPLETE: {bl_computed} recomputed, {recommended_count} recommended")
        logger.info(f"  Fresh lines: {fresh_count} players updated")
        logger.info(f"  by stat: {stat_summary}")
        logger.info(f"  Elapsed: {elapsed:.1f}s")
        logger.info("=" * 60)

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error("=" * 60)
        logger.error(f"MLB EDGE REFRESH JOB FAILED ({elapsed:.1f}s)")
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
