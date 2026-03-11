"""
NCAAB Backtesting
==================
Time-travel backtester for NCAAB spread/total predictions.

Iterates historical dates, generates predictions using only data
available as of that date, and computes betting metrics.

Usage:
    python -m src.models.ncaab_backtest --model-dir src/models/artifacts/ncaab/run_YYYYMMDD --season 2025
"""

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.models.ncaab_feature_store import (
    NCAABFeatureStore,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


class NCAABBacktestHarness:
    def __init__(self, engine, model_dir: str):
        self.engine = engine
        self.feature_store = NCAABFeatureStore(engine)
        self.model_dir = Path(model_dir)

        # Load models
        self.spread_model = joblib.load(self.model_dir / "spread_model.joblib")
        self.total_model = joblib.load(self.model_dir / "total_model.joblib")

        with open(self.model_dir / "feature_config.json") as f:
            self.feature_config = json.load(f)

    def run(self, start_date: date, end_date: date,
            spread_threshold: float = 0.55,
            total_threshold: float = 0.55) -> dict:
        """Run backtest over a date range.

        For each date:
        1. Get features for scheduled games
        2. Generate spread/total predictions
        3. Compare to actual results

        Returns summary dict with betting metrics.
        """
        # Get all completed games in range
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT game_date
                FROM ncaab_game_schedule
                WHERE game_date BETWEEN :start AND :end
                  AND is_final = TRUE
                ORDER BY game_date
            """), {"start": start_date, "end": end_date})
            dates = [row[0] for row in result]

        logger.info(f"Backtesting {len(dates)} dates from {start_date} to {end_date}")

        all_results = []

        for game_date in dates:
            df = self.feature_store.get_training_dataset([game_date.year])
            day_df = df[df["game_date"] == pd.Timestamp(game_date)]

            if day_df.empty:
                continue

            # Available features
            spread_feats = [f for f in self.feature_config["spread_features"] if f in day_df.columns]
            total_feats = [f for f in self.feature_config["total_features"] if f in day_df.columns]

            X_spread = day_df[spread_feats].fillna(0)
            X_total = day_df[total_feats].fillna(0)

            # Predictions (median)
            spread_pred = self.spread_model.predict(X_spread, quantile=0.50)
            total_pred = self.total_model.predict(X_total, quantile=0.50)

            for i, (_, row) in enumerate(day_df.iterrows()):
                actual_margin = row["home_margin"]
                actual_total = row["total_score"]
                line_spread = row.get("line_spread", 0)
                line_total = row.get("line_total", 0)

                pred_margin = spread_pred[i]
                pred_total = total_pred[i]

                # ATS: did we correctly predict which side covers?
                # Model says home covers if pred_margin > line_spread
                model_covers_home = pred_margin > line_spread
                actual_covers_home = actual_margin > line_spread

                # O/U: model says over if pred_total > line_total
                model_over = pred_total > line_total
                actual_over = actual_total > line_total

                all_results.append({
                    "game_id": row["game_id"],
                    "game_date": game_date,
                    "actual_margin": actual_margin,
                    "actual_total": actual_total,
                    "pred_margin": pred_margin,
                    "pred_total": pred_total,
                    "line_spread": line_spread,
                    "line_total": line_total,
                    "ats_correct": model_covers_home == actual_covers_home,
                    "ou_correct": model_over == actual_over,
                    "spread_edge": abs(pred_margin - line_spread),
                    "total_edge": abs(pred_total - line_total),
                })

        results_df = pd.DataFrame(all_results)

        if results_df.empty:
            logger.warning("No results — check data availability.")
            return {}

        # Summary
        summary = {
            "total_games": len(results_df),
            "ats_record": f"{results_df['ats_correct'].sum()}-{(~results_df['ats_correct']).sum()}",
            "ats_pct": results_df["ats_correct"].mean(),
            "ou_record": f"{results_df['ou_correct'].sum()}-{(~results_df['ou_correct']).sum()}",
            "ou_pct": results_df["ou_correct"].mean(),
            "avg_spread_edge": results_df["spread_edge"].mean(),
            "avg_total_edge": results_df["total_edge"].mean(),
            "spread_mae": (results_df["actual_margin"] - results_df["pred_margin"]).abs().mean(),
            "total_mae": (results_df["actual_total"] - results_df["pred_total"]).abs().mean(),
        }

        logger.info("=== BACKTEST RESULTS ===")
        logger.info(f"Games:       {summary['total_games']}")
        logger.info(f"ATS:         {summary['ats_record']} ({summary['ats_pct']:.1%})")
        logger.info(f"O/U:         {summary['ou_record']} ({summary['ou_pct']:.1%})")
        logger.info(f"Spread MAE:  {summary['spread_mae']:.2f} pts")
        logger.info(f"Total MAE:   {summary['total_mae']:.2f} pts")
        logger.info(f"Avg Edge:    spread={summary['avg_spread_edge']:.2f}, total={summary['avg_total_edge']:.2f}")

        # Save results
        results_df.to_csv(self.model_dir / "backtest_results.csv", index=False)
        logger.info(f"Results saved to {self.model_dir / 'backtest_results.csv'}")

        return summary


if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("Missing DATABASE_URL.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="NCAAB Backtest")
    parser.add_argument("--model-dir", type=str, required=True, help="Path to model artifacts")
    parser.add_argument("--season", type=int, help="Season to backtest (e.g., 2025)")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    engine = create_engine(DATABASE_URL)

    if args.season:
        # Convention: NCAAB season 2025 = Nov 2024 to Apr 2025
        start = date(args.season - 1, 11, 1)
        end = date(args.season, 4, 15)
    elif args.start_date and args.end_date:
        from datetime import datetime as dt
        start = dt.strptime(args.start_date, "%Y-%m-%d").date()
        end = dt.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        logger.error("Specify --season or --start-date/--end-date")
        sys.exit(1)

    harness = NCAABBacktestHarness(engine, args.model_dir)
    harness.run(start, end)
