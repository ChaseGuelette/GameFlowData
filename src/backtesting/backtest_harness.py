"""
Backtesting harness for evaluating prediction models on historical data.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import scipy.stats as stats
from sqlalchemy import bindparam, text

from src.backtesting.bet_simulator import BetSimulator
from src.backtesting.performance_metrics import MetricsCalculator, PerformanceMetrics

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """Container for backtest results."""

    predictions_df: pd.DataFrame
    bets_df: pd.DataFrame
    metrics: PerformanceMetrics
    start_date: date
    end_date: date
    config: dict

    def to_csv(self, output_dir: str) -> None:
        """Export results to CSV files."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        self.predictions_df.to_csv(output_path / "predictions.csv", index=False)
        self.bets_df.to_csv(output_path / "bets.csv", index=False)

        # Save metrics as JSON (include config for visualization)
        metrics_output = self.metrics.to_dict()
        metrics_output["config"] = self.config
        with open(output_path / "metrics.json", "w") as f:
            json.dump(metrics_output, f, indent=2, default=str)


@dataclass
class BacktestHarness:
    """
    Main backtesting orchestrator.

    Iterates through historical dates, generates time-travel safe predictions,
    simulates betting, and calculates performance metrics.
    """

    engine: object  # SQLAlchemy engine
    feature_store: object  # FeatureStore instance
    model_pipeline: object  # Loaded model pipeline
    predictor: object  # MonteCarloPredictor instance

    # Configuration
    edge_threshold: float = 0.05
    starting_bankroll: float = 10000.0
    kelly_fraction: float = 0.125
    bookmakers: list[str] = field(default_factory=lambda: ["pinnacle"])
    stats: list[str] = field(default_factory=lambda: ["pts", "reb", "ast"])
    min_minutes_avg: int = 10

    # Internal state
    _simulator: BetSimulator = field(init=False)
    _metrics_calc: MetricsCalculator = field(init=False)

    def __post_init__(self):
        self._simulator = BetSimulator(
            edge_threshold=self.edge_threshold,
            starting_bankroll=self.starting_bankroll,
            kelly_fraction=self.kelly_fraction,
        )
        self._metrics_calc = MetricsCalculator()

    def run(
        self,
        start_date: str | date,
        end_date: str | date,
        progress_callback: callable | None = None,
        max_workers: int = 4,
    ) -> BacktestResult:
        """
        Run backtest over date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            progress_callback: Optional callback(current_date, total_dates) for progress
            max_workers: Number of threads for parallel prediction generation

        Returns:
            BacktestResult with predictions, bets, and metrics
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        logger.info(f"Starting backtest from {start_date} to {end_date} (Sequential Execution)")

        # Get all dates with games in range
        game_dates = self._get_game_dates(start_date, end_date)
        logger.info(f"Found {len(game_dates)} dates with games")

        all_predictions = []
        completed_count = 0

        # Phase 1: Sequential Batch Prediction Generation
        logger.info("Phase 1: Generating predictions (Sequential Batch)...")

        for game_date in game_dates:
            completed_count += 1
            
            if progress_callback:
                progress_callback(game_date, len(game_dates))

            if completed_count % 5 == 0 or completed_count == len(game_dates):
                logger.info(f"Processed {completed_count}/{len(game_dates)} dates")

            try:
                # Run prediction for this date (using efficient batch fetch)
                date_preds = self._run_date(game_date)
                if date_preds is not None and len(date_preds) > 0:
                    all_predictions.append(date_preds)
            except Exception as e:
                logger.error(f"Error processing {game_date}: {e}")

        # Combine all predictions
        if all_predictions:
            predictions_df = pd.concat(all_predictions, ignore_index=True)
            # Sort by date to ensure deterministic simulation order
            predictions_df = predictions_df.sort_values(["game_date", "game_id", "player_id"])
        else:
            predictions_df = pd.DataFrame()

        logger.info(f"Total predictions generated: {len(predictions_df)}")

        # Phase 2: Sequential Simulation
        logger.info("Phase 2: Simulating bets...")

        # Get actuals upfront so we can resolve bets daily
        actuals_df = self._get_actuals(start_date, end_date)
        
        # Get voids (DNPs) upfront
        voids_df = self._get_voids(start_date, end_date)

        # Iterate through unique dates in the predictions
        if not predictions_df.empty:
            sorted_dates = sorted(predictions_df["game_date"].unique())
            for sim_date in sorted_dates:
                # 1. Resolve voids (refund DNPs) from previous days/current day
                if len(voids_df) > 0:
                    self._simulator.resolve_voids(voids_df)

                # 2. Resolve pending bets from previous days (updates bankroll)
                if len(actuals_df) > 0:
                    self._simulator.resolve_bets(actuals_df)

                # 3. Get preds for this date
                day_preds = predictions_df[predictions_df["game_date"] == sim_date]

                # 4. Evaluate and place bets (updates simulator state using new bankroll)
                self._simulator.evaluate_predictions(day_preds, sim_date)

        # Final resolution for any remaining bets
        if len(voids_df) > 0:
            voided = self._simulator.resolve_voids(voids_df)
            if voided > 0:
                logger.info(f"Final resolution: {voided} voided bets")
                
        if len(actuals_df) > 0:
            resolved = self._simulator.resolve_bets(actuals_df)
            logger.info(f"Final resolution: {resolved} resolved bets")

        # Get bets DataFrame
        bets_df = self._simulator.to_dataframe()
        logger.info(f"Total bets: {len(bets_df)}")

        # Add actuals to predictions
        if len(predictions_df) > 0 and len(actuals_df) > 0:
            predictions_df = self._merge_actuals(predictions_df, actuals_df)

        # Calculate metrics
        metrics = self._metrics_calc.calculate(predictions_df, bets_df, starting_bankroll=self.starting_bankroll)

        logger.info(f"\n{metrics}")

        return BacktestResult(
            predictions_df=predictions_df,
            bets_df=bets_df,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
            config={
                "edge_threshold": self.edge_threshold,
                "starting_bankroll": self.starting_bankroll,
                "kelly_fraction": self.kelly_fraction,
                "bookmakers": self.bookmakers,
                "stats": self.stats,
            },
        )

    def _get_game_dates(self, start_date: date, end_date: date) -> list[date]:
        """Get all dates with games in the range."""
        query = """
            SELECT DISTINCT game_date
            FROM player_game_stats
            WHERE game_date >= :start_date
              AND game_date <= :end_date
            ORDER BY game_date
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"start_date": start_date, "end_date": end_date})
            return [row[0] for row in result]

    def _run_date(self, game_date: date) -> pd.DataFrame | None:
        """Run predictions for a single date using efficient batch processing."""
        # Batch fetch all features for this date (One Query Rule them all)
        features_df = self.feature_store.get_features_for_date(game_date)

        if features_df is None or features_df.empty:
            logger.debug(f"No features returned for {game_date}")
            return None

        pre_filter_count = len(features_df)

        # Filter for min minutes using rolling average (forward-looking safe)
        if "player_avg_min_l5" in features_df.columns:
            features_df = features_df[features_df["player_avg_min_l5"] >= self.min_minutes_avg]

        if features_df.empty:
            logger.debug(f"All {pre_filter_count} players filtered out for {game_date} (min_minutes_avg={self.min_minutes_avg})")
            return None

        all_predictions = []
        prediction_samples = {}

        # Convert to records for iteration
        player_records = features_df.to_dict("records")

        for player_row in player_records:
            try:
                # Features are just the row itself
                features = player_row

                # Generate predictions
                preds = self.predictor.predict(
                    player_id=player_row["player_id"],
                    game_id=player_row["game_id"],
                    features=features,
                    stats=self.stats,
                )

                for stat, pred in preds.items():
                    all_predictions.append(
                        {
                            "player_id": player_row["player_id"],
                            "player_name": player_row.get("player_name"),
                            "game_id": player_row["game_id"],
                            "game_date": game_date,
                            "team_id": player_row.get("team_id"),
                            "stat": stat,
                            "pred_mean": pred.mean,
                            "pred_std": pred.samples.std(),
                            "pred_median": pred.median,
                            "pred_q10": pred.q10,
                            "pred_q25": pred.q25,
                            "pred_q50": pred.q50,
                            "pred_q75": pred.q75,
                            "pred_q90": pred.q90,
                        }
                    )
                    # Store samples separately to avoid DataFrame memory bloat
                    prediction_samples[(player_row["player_id"], player_row["game_id"], stat)] = pred.samples

            except Exception as e:
                logger.warning(f"Error predicting for player {player_row.get('player_id')}: {e}")
                continue

        logger.info(f"  {game_date}: {len(player_records)} players -> {len(all_predictions)} predictions")

        if not all_predictions:
            return None

        predictions_df = pd.DataFrame(all_predictions)

        # Get unique game IDs for fetching lines
        game_ids = features_df["game_id"].unique().tolist()

        # Get prop lines and calculate edges
        lines_df = self._get_lines_for_date(game_date, game_ids)
        if len(lines_df) > 0:
            predictions_df = self._calculate_edges(predictions_df, lines_df, prediction_samples)

            # Filter to best line per player/stat (Line Shopping)
            predictions_df = self._filter_best_bets(predictions_df)

        return predictions_df

    def _filter_best_bets(self, predictions_df: pd.DataFrame) -> pd.DataFrame:
        """Select single best betting opportunity per player/game (One Bet Per Player)."""
        if predictions_df.empty:
            return predictions_df

        # Calculate max potential edge (over or under)
        predictions_df["max_edge"] = predictions_df[["over_edge", "under_edge"]].max(axis=1)

        # Sort by max edge descending
        predictions_df = predictions_df.sort_values("max_edge", ascending=False)

        # Deduplicate to keep best line per player (Limit Correlation Risk)
        # Modified: Subset now excludes "stat" to ensure only ONE bet per player per game
        predictions_df = predictions_df.drop_duplicates(subset=["player_id", "game_id"], keep="first")

        return predictions_df

    def _get_games_for_date(self, game_date: date) -> list[dict]:
        """Get all games for a date."""
        query = """
            SELECT DISTINCT game_id
            FROM player_game_stats
            WHERE game_date = :game_date
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"game_date": game_date})
            return [{"game_id": row[0]} for row in result]

    def _get_players_for_date(self, game_date: date) -> list[dict]:
        """Get players who played on this date (retrospective)."""
        query = """
            SELECT pgs.player_id,
                   pgs.game_id,
                   pgs.team_id,
                   p.player_name
            FROM player_game_stats pgs
            JOIN players p ON pgs.player_id = p.player_id
            WHERE pgs.game_date = :game_date
              AND pgs.min >= :min_minutes
              AND pgs.did_not_play IS NOT TRUE
        """

        with self.engine.connect() as conn:
            result = conn.execute(
                text(query),
                {"game_date": game_date, "min_minutes": self.min_minutes_avg},
            )
            return [dict(row._mapping) for row in result]

    def _get_lines_for_date(self, game_date: date, game_ids: list[str]) -> pd.DataFrame:
        """Get prop lines for games on a date."""
        if not game_ids:
            return pd.DataFrame()

        stat_to_market = {
            "pts": "player_points",
            "reb": "player_rebounds",
            "ast": "player_assists",
        }
        markets = [stat_to_market[s] for s in self.stats if s in stat_to_market]

        # Get lines from snapshot closest to but before game time
        # For simplicity, use latest snapshot on or before game date
        query = text("""
            WITH ranked_lines AS (
                SELECT
                    player_id,
                    game_id,
                    market_key,
                    line,
                    outcome_label,
                    odds_american,
                    snapshot_time,
                    ROW_NUMBER() OVER (
                        PARTITION BY player_id, game_id, market_key, line
                        ORDER BY snapshot_time DESC
                    ) as rn
                FROM raw_player_props_combined
                WHERE game_id IN :game_ids
                  AND market_key IN :markets
                  AND bookmaker IN :bookmakers
                  AND snapshot_time::date <= :game_date
            )
            SELECT
                player_id,
                game_id,
                market_key,
                line,
                MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
                MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
            FROM ranked_lines
            WHERE rn = 1
            GROUP BY player_id, game_id, market_key, line
        """).bindparams(
            bindparam("game_ids", expanding=True),
            bindparam("markets", expanding=True),
            bindparam("bookmakers", expanding=True),
        )

        try:
            with self.engine.connect() as conn:
                return pd.read_sql(
                    query,
                    conn,
                    params={
                        "game_ids": list(game_ids),
                        "markets": list(markets),
                        "bookmakers": list(self.bookmakers),
                        "game_date": game_date,
                    },
                )
        except Exception as e:
            logger.warning(f"Error fetching lines for {game_date}: {e}")
            return pd.DataFrame()

    def _calculate_edges(self, predictions_df: pd.DataFrame, lines_df: pd.DataFrame, prediction_samples: dict) -> pd.DataFrame:
        """Add edge calculations to predictions using empirical CDF from Monte Carlo samples."""
        market_to_stat = {
            "player_points": "pts",
            "player_rebounds": "reb",
            "player_assists": "ast",
        }
        lines_df = lines_df.copy()
        lines_df["stat"] = lines_df["market_key"].map(market_to_stat)

        # Merge predictions with lines
        merged = predictions_df.merge(
            lines_df[["player_id", "game_id", "stat", "line", "over_odds", "under_odds"]],
            on=["player_id", "game_id", "stat"],
            how="left",
        )

        # Calculate model probability of over using empirical CDF from Monte Carlo samples
        def estimate_over_prob(row):
            if pd.isna(row.get("line")):
                return None

            line = row["line"]
            # Look up samples from the dictionary using the composite key
            samples = prediction_samples.get((row["player_id"], row["game_id"], row["stat"]))

            # Primary: empirical CDF from Monte Carlo samples
            if samples is not None and hasattr(samples, "__len__") and len(samples) > 0:
                prob_over = float((samples > line).mean())
            else:
                # Fallback: Gaussian approximation (no samples available)
                mean_pred = row.get("pred_mean")
                std_dev = row.get("pred_std")

                if pd.isna(mean_pred) or pd.isna(std_dev) or std_dev == 0:
                    return 0.5

                z_score = (line - mean_pred) / std_dev
                prob_over = stats.norm.sf(z_score)

            # Apply sanity caps (min 5%, max 90%)
            # This prevents extreme Kelly bets on outliers
            return min(max(prob_over, 0.05), 0.90)

        merged["over_prob"] = merged.apply(estimate_over_prob, axis=1)
        merged["under_prob"] = 1 - merged["over_prob"]

        # Convert odds to implied probability
        def odds_to_prob(odds):
            if pd.isna(odds):
                return None
            if odds > 0:
                return 100 / (odds + 100)
            else:
                return abs(odds) / (abs(odds) + 100)

        merged["implied_over"] = merged["over_odds"].apply(odds_to_prob)
        merged["implied_under"] = merged["under_odds"].apply(odds_to_prob)

        # Calculate edges
        merged["over_edge"] = merged["over_prob"] - merged["implied_over"]
        merged["under_edge"] = merged["under_prob"] - merged["implied_under"]

        return merged

    def _get_actuals(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Get actual outcomes for all games in range."""
        query = """
            SELECT
                player_id,
                game_id,
                pts,
                reb,
                ast,
                fg3m as threes
            FROM player_game_stats
            WHERE game_date >= :start_date
              AND game_date <= :end_date
              AND did_not_play IS NOT TRUE
        """

        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"start_date": start_date, "end_date": end_date})

        # Melt to long format
        if df.empty:
            return pd.DataFrame(columns=["player_id", "game_id", "stat", "actual_value"])

        melted = df.melt(
            id_vars=["player_id", "game_id"],
            value_vars=["pts", "reb", "ast", "threes"],
            var_name="stat",
            value_name="actual_value",
        )

        return melted

    def _get_voids(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Get players who did not play (voids) for all games in range."""
        query = """
            SELECT
                player_id,
                game_id
            FROM player_game_stats
            WHERE game_date >= :start_date
              AND game_date <= :end_date
              AND did_not_play IS TRUE
        """

        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"start_date": start_date, "end_date": end_date})
            
        return df

    def _merge_actuals(self, predictions_df: pd.DataFrame, actuals_df: pd.DataFrame) -> pd.DataFrame:
        """Merge actual outcomes into predictions."""
        merged = predictions_df.merge(
            actuals_df[["player_id", "game_id", "stat", "actual_value"]],
            on=["player_id", "game_id", "stat"],
            how="left",
        )
        merged = merged.rename(columns={"actual_value": "actual"})
        return merged


def load_model_pipeline(model_path: str) -> dict:
    """Load model artifacts from a training run."""
    path = Path(model_path)

    pipeline = {
        "minutes_model": joblib.load(path / "minutes_model.joblib"),
        "rate_models": {},
        "minutes_features": None,
        "rate_features": None,
    }

    # Load rate models
    for stat in ["pts", "reb", "ast", "threes"]:
        model_file = path / f"{stat}_rate_model.joblib"
        if model_file.exists():
            pipeline["rate_models"][stat] = joblib.load(model_file)

    # Load feature config
    feature_config = joblib.load(path / "feature_config.joblib")
    pipeline["minutes_features"] = feature_config.get("minutes_features", [])
    pipeline["rate_features"] = feature_config.get("rate_features", [])

    return pipeline
