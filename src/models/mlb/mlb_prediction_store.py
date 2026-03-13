"""
Storage and retrieval for MLB daily predictions.

Stores quantile/NegBin/binary predictions + gzip-compressed MC samples
to PostgreSQL, mirroring the NBA PredictionStore pattern but with
MLB-specific table names, column lists, and integer game_id.
"""

import gzip
import logging
from datetime import date

import numpy as np
import pandas as pd
from psycopg2 import extras

logger = logging.getLogger(__name__)

# Columns in mlb_daily_predictions
MLB_PREDICTION_COLS = [
    "prediction_date", "player_id", "player_name", "game_id", "team_id",
    "opponent_id", "stat", "model_type", "pred_mean", "pred_std", "pred_median",
    "pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90",
    "pred_prob",  # for binary stats
    "line", "over_odds", "under_odds", "over_prob", "under_prob",
    "implied_over", "implied_under", "over_edge", "under_edge", "game_time",
    "bookmaker",
    # Feature columns for dashboard insights
    "feat_days_rest", "feat_lineup_position", "feat_park_factor",
    "feat_player_avg_stat_l5", "feat_player_avg_stat_szn", "feat_opp_abbrev",
    # Black-Litterman blended values
    "bl_over_prob", "bl_under_prob", "bl_over_edge", "bl_under_edge",
    "bl_confidence", "is_recommended",
]


class MLBPredictionStore:
    def __init__(self, engine):
        self.engine = engine

    def store_predictions(self, predictions_df: pd.DataFrame, prediction_date: date):
        """Upsert prediction rows into mlb_daily_predictions."""
        if predictions_df.empty:
            return

        df = predictions_df.copy()
        df["prediction_date"] = prediction_date

        # Ensure all expected columns exist (NULL for missing)
        for col in MLB_PREDICTION_COLS:
            if col not in df.columns:
                df[col] = None

        # Replace NaN/inf with None so PostgreSQL stores NULL
        edge_and_line_cols = [
            "line", "over_odds", "under_odds", "over_prob", "under_prob",
            "implied_over", "implied_under", "over_edge", "under_edge",
            "bl_over_prob", "bl_under_prob", "bl_over_edge", "bl_under_edge",
            "bl_confidence", "pred_prob",
        ]
        for col in edge_and_line_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].where(pd.notna(df[col]) & np.isfinite(df[col]), other=None)

        rows = []
        for _, row in df.iterrows():
            rows.append(tuple(
                None if (isinstance(v, float) and (np.isnan(v) or np.isinf(v))) or v is pd.NaT else v
                for v in (row[col] for col in MLB_PREDICTION_COLS)
            ))

        col_list = ", ".join(MLB_PREDICTION_COLS)
        update_cols = [c for c in MLB_PREDICTION_COLS if c not in ("prediction_date", "player_id", "game_id", "stat")]
        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        query = f"""
            INSERT INTO mlb_daily_predictions ({col_list})
            VALUES %s
            ON CONFLICT (prediction_date, player_id, game_id, stat)
            DO UPDATE SET {update_clause}
        """

        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, rows)
            conn.commit()
            logger.info(f"Stored {len(rows)} MLB predictions for {prediction_date}")
        finally:
            conn.close()

    def store_samples(
        self,
        samples_dict: dict[tuple, np.ndarray],
        prediction_date: date,
    ):
        """Store MC samples as gzip-compressed bytea."""
        if not samples_dict:
            return

        rows = []
        for (player_id, game_id, stat), samples in samples_dict.items():
            compressed = gzip.compress(samples.astype(np.float64).tobytes())
            rows.append((
                prediction_date,
                int(player_id),
                int(game_id),  # MLB game_id is integer
                stat,
                len(samples),
                compressed,
            ))

        query = """
            INSERT INTO mlb_daily_prediction_samples
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
            logger.info(f"Stored {len(rows)} MLB sample arrays for {prediction_date}")
        finally:
            conn.close()

    def get_predictions(
        self,
        prediction_date: date,
        player_id: int | None = None,
        stat: str | None = None,
    ) -> pd.DataFrame:
        """Retrieve stored predictions with optional filters."""
        from sqlalchemy import text

        conditions = ["prediction_date = :prediction_date"]
        params: dict = {"prediction_date": prediction_date}

        if player_id is not None:
            conditions.append("player_id = :player_id")
            params["player_id"] = player_id
        if stat is not None:
            conditions.append("stat = :stat")
            params["stat"] = stat

        where = " AND ".join(conditions)
        query = f"SELECT * FROM mlb_daily_predictions WHERE {where} ORDER BY player_name, stat"

        with self.engine.connect() as conn:
            return pd.read_sql(text(query), conn, params=params)

    def get_samples(
        self,
        prediction_date: date,
        player_id: int,
        game_id: int,
        stat: str,
    ) -> np.ndarray | None:
        """Retrieve and decompress MC samples for a specific prediction."""
        query = """
            SELECT n_samples, samples_gz
            FROM mlb_daily_prediction_samples
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

        if isinstance(blob, memoryview):
            blob = bytes(blob)

        return np.frombuffer(gzip.decompress(blob), dtype=np.float64, count=n_samples)

    def get_all_samples_for_date(self, prediction_date: date) -> dict[tuple, np.ndarray]:
        """Retrieve all MC samples for a date as a samples_dict.

        Returns:
            dict[(player_id, game_id, stat) -> np.ndarray]
        """
        query = """
            SELECT player_id, game_id, stat, n_samples, samples_gz
            FROM mlb_daily_prediction_samples
            WHERE prediction_date = :prediction_date
        """
        from sqlalchemy import text

        samples_dict: dict[tuple, np.ndarray] = {}

        with self.engine.connect() as conn:
            rows = conn.execute(text(query), {"prediction_date": prediction_date}).fetchall()

        for row in rows:
            player_id, game_id, stat, n_samples, blob = row
            if isinstance(blob, memoryview):
                blob = bytes(blob)
            samples = np.frombuffer(gzip.decompress(blob), dtype=np.float64, count=n_samples)
            samples_dict[(int(player_id), int(game_id), stat)] = samples

        logger.info(f"Loaded {len(samples_dict)} MLB sample arrays for {prediction_date}")
        return samples_dict

    def get_player_id_by_name(self, name: str) -> int | None:
        """Fuzzy lookup: find player_id by name from mlb_players."""
        query = """
            SELECT player_id, player_name
            FROM mlb_players
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

        for row in rows:
            if row[1].lower() == name.lower():
                return row[0]
        return rows[0][0]
