"""
Storage and retrieval for daily MC predictions.

Stores quantile predictions + gzip-compressed MC samples to PostgreSQL,
enabling the query_player CLI tool to answer arbitrary line questions
against the stored distributions.
"""

import gzip
import logging
from datetime import date

import numpy as np
import pandas as pd
from psycopg2 import extras

logger = logging.getLogger(__name__)

# Columns in daily_predictions that come from the predictions DataFrame
PREDICTION_COLS = [
    "prediction_date", "player_id", "player_name", "game_id", "team_id",
    "opponent_id", "stat", "pred_mean", "pred_std", "pred_median",
    "pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90",
    "line", "over_odds", "under_odds", "over_prob", "under_prob",
    "implied_over", "implied_under", "over_edge", "under_edge",
]


class PredictionStore:
    def __init__(self, engine):
        self.engine = engine

    def store_predictions(self, predictions_df: pd.DataFrame, prediction_date: date):
        """Upsert prediction rows into daily_predictions.

        Args:
            predictions_df: DataFrame from DailyPredictionRunner.run_for_date()
            prediction_date: The date these predictions are for
        """
        if predictions_df.empty:
            return

        df = predictions_df.copy()
        df["prediction_date"] = prediction_date

        # Ensure all expected columns exist (NULL for missing edge columns)
        for col in PREDICTION_COLS:
            if col not in df.columns:
                df[col] = None

        rows = []
        for _, row in df.iterrows():
            rows.append(tuple(row[col] for col in PREDICTION_COLS))

        col_list = ", ".join(PREDICTION_COLS)
        update_cols = [c for c in PREDICTION_COLS if c not in ("prediction_date", "player_id", "game_id", "stat")]
        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        query = f"""
            INSERT INTO daily_predictions ({col_list})
            VALUES %s
            ON CONFLICT (prediction_date, player_id, game_id, stat)
            DO UPDATE SET {update_clause}
        """

        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, rows)
            conn.commit()
            logger.info(f"Stored {len(rows)} predictions for {prediction_date}")
        finally:
            conn.close()

    def store_samples(
        self,
        samples_dict: dict[tuple, np.ndarray],
        prediction_date: date,
    ):
        """Store MC samples as gzip-compressed bytea.

        Args:
            samples_dict: {(player_id, game_id, stat): np.ndarray} from batch predict
            prediction_date: The date these predictions are for
        """
        if not samples_dict:
            return

        rows = []
        for (player_id, game_id, stat), samples in samples_dict.items():
            compressed = gzip.compress(samples.astype(np.float64).tobytes())
            rows.append((
                prediction_date,
                int(player_id),
                str(game_id),
                stat,
                len(samples),
                compressed,
            ))

        query = """
            INSERT INTO daily_prediction_samples
                (prediction_date, player_id, game_id, stat, n_samples, samples_gz)
            VALUES %s
            ON CONFLICT (prediction_date, player_id, game_id, stat)
            DO UPDATE SET n_samples = EXCLUDED.n_samples,
                          samples_gz = EXCLUDED.samples_gz,
                          created_at = NOW()
        """

        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, rows)
            conn.commit()
            logger.info(f"Stored {len(rows)} sample arrays for {prediction_date}")
        finally:
            conn.close()

    def get_predictions(
        self,
        prediction_date: date,
        player_id: int | None = None,
        stat: str | None = None,
    ) -> pd.DataFrame:
        """Retrieve stored predictions with optional filters."""
        conditions = ["prediction_date = :prediction_date"]
        params: dict = {"prediction_date": prediction_date}

        if player_id is not None:
            conditions.append("player_id = :player_id")
            params["player_id"] = player_id
        if stat is not None:
            conditions.append("stat = :stat")
            params["stat"] = stat

        where = " AND ".join(conditions)
        query = f"SELECT * FROM daily_predictions WHERE {where} ORDER BY player_name, stat"

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params=params)

    def get_samples(
        self,
        prediction_date: date,
        player_id: int,
        game_id: str,
        stat: str,
    ) -> np.ndarray | None:
        """Retrieve and decompress MC samples for a specific prediction."""
        query = """
            SELECT n_samples, samples_gz
            FROM daily_prediction_samples
            WHERE prediction_date = :prediction_date
              AND player_id = :player_id
              AND game_id = :game_id
              AND stat = :stat
        """
        params = {
            "prediction_date": prediction_date,
            "player_id": player_id,
            "game_id": game_id,
            "stat": stat,
        }

        with self.engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text(query), params).fetchone()

        if result is None:
            return None

        n_samples = result[0]
        blob = result[1]

        # Handle memoryview (psycopg2 returns bytea as memoryview)
        if isinstance(blob, memoryview):
            blob = bytes(blob)

        return np.frombuffer(gzip.decompress(blob), dtype=np.float64, count=n_samples)

    def get_player_id_by_name(self, name: str) -> int | None:
        """Fuzzy lookup: find player_id by name (case-insensitive LIKE)."""
        query = """
            SELECT player_id, player_name
            FROM players
            WHERE LOWER(player_name) LIKE :pattern
            ORDER BY player_name
            LIMIT 5
        """
        pattern = f"%{name.lower()}%"

        with self.engine.connect() as conn:
            from sqlalchemy import text
            rows = conn.execute(text(query), {"pattern": pattern}).fetchall()

        if not rows:
            return None
        if len(rows) == 1:
            return rows[0][0]

        # Multiple matches — return exact match if one exists, else first
        for row in rows:
            if row[1].lower() == name.lower():
                return row[0]
        return rows[0][0]
