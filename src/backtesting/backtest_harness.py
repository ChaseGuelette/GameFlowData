"""
Backtesting harness for evaluating prediction models on historical data.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import joblib
import pandas as pd
import scipy.stats as stats
from sqlalchemy import bindparam, text

from src.backtesting.bet_simulator import BetSimulator
from src.backtesting.performance_metrics import MetricsCalculator, PerformanceMetrics
from src.models.black_litterman import BlackLittermanBlender, BLConfig

if TYPE_CHECKING:
    from src.config.stat_config import StatConfigSet

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """Container for backtest results."""

    predictions_df: pd.DataFrame
    bets_df: pd.DataFrame
    all_edges_df: pd.DataFrame  # Pre-line-shopping edges across all bookmakers
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

        # Save all bookmaker edges (pre-line-shopping) for analysis
        if not self.all_edges_df.empty:
            self.all_edges_df.to_csv(output_path / "all_bookmaker_edges.csv", index=False)

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
    max_bet_pct: float | None = None  # Cap bet size as % of bankroll (e.g., 0.025 = 2.5%)
    flat_bet_size: float | None = None  # Fixed dollar amount per bet (overrides Kelly)
    bookmakers: list[str] = field(default_factory=lambda: [
        # US market (original)
        "draftkings", "fanduel", "betmgm", "betrivers", "bovada",
        "williamhill_us", "betonlineag", "unibet_us", "mybookieag",
        "pointsbetus", "fanatics", "barstool", "wynnbet",
        # US2 / us_ex markets
        "ballybet", "betopenly", "betparx", "espnbet", "fliff",
        "hardrockbet", "novig", "polymarket", "prophetx", "rebet",
        "windcreek",
    ])
    stats: list[str] = field(default_factory=lambda: ["pts", "reb", "ast"])
    min_minutes_avg: int = 10
    allowed_bets: list[tuple[str, str]] | None = None  # e.g., [("pts", "under"), ("reb", "over")]
    bl_blender: BlackLittermanBlender | None = None  # Black-Litterman probability blender (for edge filtering)
    bl_sizing_blender: BlackLittermanBlender | None = None  # BL for Kelly sizing only (edge detection uses raw probs)
    stat_config: StatConfigSet | None = None  # Per-stat edge thresholds and BL tau values

    # Internal state
    _simulator: BetSimulator = field(init=False)
    _metrics_calc: MetricsCalculator = field(init=False)
    _stat_blenders: dict[str, BlackLittermanBlender | None] = field(init=False)

    def __post_init__(self):
        self._simulator = BetSimulator(
            edge_threshold=self.edge_threshold,
            starting_bankroll=self.starting_bankroll,
            kelly_fraction=self.kelly_fraction,
            max_bet_pct=self.max_bet_pct,
            flat_bet_size=self.flat_bet_size,
            allowed_bets=set(self.allowed_bets) if self.allowed_bets else None,
            use_bl_for_sizing=self.bl_sizing_blender is not None,
            stat_config=self.stat_config,
        )
        self._metrics_calc = MetricsCalculator()

        # Create per-stat BL blenders based on stat_config
        self._stat_blenders = {}
        if self.stat_config is not None:
            for stat in self.stats:
                tau = self.stat_config.get_bl_tau(stat)
                if tau is not None:
                    self._stat_blenders[stat] = BlackLittermanBlender(BLConfig(tau=tau))
                else:
                    self._stat_blenders[stat] = None
        elif self.bl_blender is not None:
            # Fallback: use same blender for all stats
            for stat in self.stats:
                self._stat_blenders[stat] = self.bl_blender

    def _get_blender_for_stat(self, stat: str) -> BlackLittermanBlender | None:
        """Get BL blender for a specific stat, or None if BL is disabled for that stat."""
        if self._stat_blenders:
            return self._stat_blenders.get(stat)
        return self.bl_blender

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

        # Phase 0: Prefetch all data (features + lines) to minimize DB round trips
        logger.info("Phase 0: Prefetching all features and lines...")
        prefetched_features = self.feature_store.get_features_for_date_range(start_date, end_date)
        prefetched_lines = self._prefetch_all_lines(start_date, end_date)
        logger.info(
            f"Prefetched features for {len(prefetched_features)} dates, lines for {len(prefetched_lines)} dates"
        )

        all_predictions = []
        all_bookmaker_edges = []
        completed_count = 0

        # Phase 1: Generate predictions (zero DB calls per date)
        logger.info("Phase 1: Generating predictions (Sequential Batch)...")

        for game_date in game_dates:
            completed_count += 1

            if progress_callback:
                progress_callback(game_date, len(game_dates))

            if completed_count % 5 == 0 or completed_count == len(game_dates):
                logger.info(f"Processed {completed_count}/{len(game_dates)} dates")

            try:
                date_preds, date_all_edges = self._run_date(game_date, prefetched_features, prefetched_lines)
                if date_preds is not None and len(date_preds) > 0:
                    all_predictions.append(date_preds)
                if date_all_edges is not None and len(date_all_edges) > 0:
                    all_bookmaker_edges.append(date_all_edges)
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

        # Combine all bookmaker edges (pre-line-shopping data)
        if all_bookmaker_edges:
            all_edges_df = pd.concat(all_bookmaker_edges, ignore_index=True)
            all_edges_df = all_edges_df.sort_values(["game_date", "player_id", "stat", "bookmaker"])
            logger.info(f"Total bookmaker edge rows (pre-line-shop): {len(all_edges_df)}")
        else:
            all_edges_df = pd.DataFrame()

        # Calculate metrics
        metrics = self._metrics_calc.calculate(predictions_df, bets_df, starting_bankroll=self.starting_bankroll)

        logger.info(f"\n{metrics}")

        return BacktestResult(
            predictions_df=predictions_df,
            bets_df=bets_df,
            all_edges_df=all_edges_df,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
            config={
                "edge_threshold": self.edge_threshold,
                "starting_bankroll": self.starting_bankroll,
                "kelly_fraction": self.kelly_fraction,
                "max_bet_pct": self.max_bet_pct,
                "bookmakers": self.bookmakers,
                "stats": self.stats,
                "allowed_bets": [f"{s}:{side}" for s, side in self.allowed_bets] if self.allowed_bets else None,
                "bl_tau": self.bl_blender.config.tau if self.bl_blender else None,
                "bl_sizing_tau": self.bl_sizing_blender.config.tau if self.bl_sizing_blender else None,
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

    def _run_date(
        self,
        game_date: date,
        prefetched_features: dict | None = None,
        prefetched_lines: dict | None = None,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame]:
        """Run predictions for a single date using efficient batch processing."""
        # Get features (from prefetch dict or single-date DB query)
        if prefetched_features is not None:
            features_df = prefetched_features.get(game_date)
            if features_df is None:
                features_df = pd.DataFrame()
        else:
            features_df = self.feature_store.get_features_for_date(game_date)

        if features_df is None or features_df.empty:
            logger.debug(f"No features returned for {game_date}")
            return None, pd.DataFrame()

        pre_filter_count = len(features_df)

        # Filter for min minutes using rolling average (forward-looking safe)
        if "player_avg_min_l5" in features_df.columns:
            features_df = features_df[features_df["player_avg_min_l5"] >= self.min_minutes_avg]

        if features_df.empty:
            logger.debug(
                f"All {pre_filter_count} players filtered out for {game_date} (min_minutes_avg={self.min_minutes_avg})"
            )
            return None, pd.DataFrame()

        # Sort for deterministic RNG ordering across query strategies
        features_df = features_df.sort_values(["player_id", "game_id"]).reset_index(drop=True)

        # Batch predict all players at once (4 XGBoost calls instead of ~N*4)
        try:
            predictions_list, prediction_samples = self.predictor.predict_batch_for_date(features_df, stats=self.stats)
        except Exception as e:
            logger.error(f"Error in batch prediction for {game_date}: {e}")
            return None, pd.DataFrame()

        # Add game_date to each prediction
        for pred in predictions_list:
            pred["game_date"] = game_date

        logger.info(f"  {game_date}: {len(features_df)} players -> {len(predictions_list)} predictions")

        if not predictions_list:
            return None, pd.DataFrame()

        predictions_df = pd.DataFrame(predictions_list)

        # Get lines (from prefetch dict or single-date DB query)
        if prefetched_lines is not None:
            lines_df = prefetched_lines.get(game_date, pd.DataFrame())
        else:
            game_ids = features_df["game_id"].unique().tolist()
            lines_df = self._get_lines_for_date(game_date, game_ids)

        all_edges_df = pd.DataFrame()
        if len(lines_df) > 0:
            predictions_df = self._calculate_edges(predictions_df, lines_df, prediction_samples)

            # Snapshot all bookmaker edges before filtering
            all_edges_df = predictions_df.copy()

            # Filter to best line per player/stat (Line Shopping)
            predictions_df = self._filter_best_bets(predictions_df)

        return predictions_df, all_edges_df

    def _filter_best_bets(self, predictions_df: pd.DataFrame) -> pd.DataFrame:
        """Line-shop across bookmakers, then select single best bet per player/game.

        Stage 1 (Line Shopping): For each (player, game, stat), independently select
        the best over line and best under line. This ensures a good over from Bookmaker A
        and a good under from Bookmaker B are both considered.

        Stage 2 (Bet Dedup): For each (player, game), keep only the single best stat.
        This limits correlation risk to one bet per player per game.
        """
        if predictions_df.empty:
            return predictions_df

        dedup_key = ["player_id", "game_id", "stat"]

        # Stage 1: Line Shopping — best over and best under independently per player/game/stat
        best_over = (
            predictions_df.sort_values("over_edge", ascending=False)
            .drop_duplicates(subset=dedup_key, keep="first")[dedup_key + ["over_edge"]]
            .rename(columns={"over_edge": "best_over_edge"})
        )
        best_under = (
            predictions_df.sort_values("under_edge", ascending=False)
            .drop_duplicates(subset=dedup_key, keep="first")[dedup_key + ["under_edge"]]
            .rename(columns={"under_edge": "best_under_edge"})
        )

        # Merge best edges back, then pick the winning side's full row
        best_edges = best_over.merge(best_under, on=dedup_key)
        best_edges["best_side"] = (best_edges["best_over_edge"] >= best_edges["best_under_edge"]).map(
            {True: "over", False: "under"}
        )
        best_edges["max_edge"] = best_edges[["best_over_edge", "best_under_edge"]].max(axis=1)

        # For each (player, game, stat), pick the row that provided the winning side's edge
        rows = []
        for _, edge_row in best_edges.iterrows():
            pid, gid, stat = edge_row["player_id"], edge_row["game_id"], edge_row["stat"]
            mask = (
                (predictions_df["player_id"] == pid)
                & (predictions_df["game_id"] == gid)
                & (predictions_df["stat"] == stat)
            )
            candidates = predictions_df[mask]
            if edge_row["best_side"] == "over":
                best = candidates.sort_values("over_edge", ascending=False).iloc[0]
            else:
                best = candidates.sort_values("under_edge", ascending=False).iloc[0]
            rows.append(best)

        if not rows:
            return predictions_df.iloc[0:0]

        predictions_df = pd.DataFrame(rows)
        predictions_df["max_edge"] = predictions_df[["over_edge", "under_edge"]].max(axis=1)

        # Stage 2: Bet Dedup — one bet per player per game
        predictions_df = predictions_df.sort_values("max_edge", ascending=False)
        predictions_df = predictions_df.drop_duplicates(
            subset=["player_id", "game_id"], keep="first"
        )

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

        # Get lines from snapshot closest to but before inference cutoff
        # Production inference runs at 6:30 PM ET (~23:30 UTC EST / 22:30 UTC EDT)
        # Use 23:30 UTC as cutoff to match production behavior
        query = text("""
            WITH ranked_lines AS (
                SELECT
                    player_id,
                    game_id,
                    market_key,
                    line,
                    bookmaker,
                    outcome_label,
                    odds_american,
                    snapshot_time,
                    ROW_NUMBER() OVER (
                        PARTITION BY player_id, game_id, market_key, line, bookmaker, outcome_label
                        ORDER BY snapshot_time DESC
                    ) as rn
                FROM raw_player_props_combined
                WHERE game_id IN :game_ids
                  AND market_key IN :markets
                  AND bookmaker IN :bookmakers
                  AND snapshot_time < :game_date::timestamp + interval '23 hours 30 minutes'
            )
            SELECT
                player_id,
                game_id,
                market_key,
                line,
                bookmaker,
                MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
                MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
            FROM ranked_lines
            WHERE rn = 1
            GROUP BY player_id, game_id, market_key, line, bookmaker
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

    def _prefetch_all_lines(self, start_date: date, end_date: date, chunk_size: int = 3) -> dict[date, pd.DataFrame]:
        """Prefetch all prop lines for a date range in chunked queries.

        Returns dict[date, DataFrame] with the same columns as _get_lines_for_date.
        """
        stat_to_market = {
            "pts": "player_points",
            "reb": "player_rebounds",
            "ast": "player_assists",
        }
        markets = [stat_to_market[s] for s in self.stats if s in stat_to_market]

        # Get all game dates in range first
        date_query = text("""
            SELECT DISTINCT game_date
            FROM player_game_stats
            WHERE game_date >= :start_date AND game_date <= :end_date
            ORDER BY game_date
        """)
        with self.engine.connect() as conn:
            date_result = conn.execute(date_query, {"start_date": start_date, "end_date": end_date})
            all_dates = [row[0] for row in date_result]

        if not all_dates:
            logger.warning("No game dates found for lines prefetch")
            return {}

        # Chunk dates to keep individual queries manageable
        chunks = [all_dates[i : i + chunk_size] for i in range(0, len(all_dates), chunk_size)]
        logger.info(
            f"Prefetching lines in {len(chunks)} chunks (chunk_size={chunk_size}) for {len(all_dates)} dates..."
        )

        query = text("""
            WITH game_dates AS (
                SELECT DISTINCT game_id, game_date
                FROM player_game_stats
                WHERE game_date >= :chunk_start AND game_date <= :chunk_end
            ),
            ranked_lines AS (
                SELECT
                    rp.player_id,
                    gd.game_id,
                    gd.game_date,
                    rp.market_key,
                    rp.line,
                    rp.bookmaker,
                    rp.outcome_label,
                    rp.odds_american,
                    ROW_NUMBER() OVER (
                        PARTITION BY rp.player_id, gd.game_id, rp.market_key, rp.line, rp.bookmaker, rp.outcome_label
                        ORDER BY rp.snapshot_time DESC
                    ) as rn
                FROM raw_player_props_combined rp
                JOIN game_dates gd ON LPAD(rp.game_id, 10, '0') = gd.game_id
                WHERE rp.market_key IN :markets
                  AND rp.bookmaker IN :bookmakers
                  AND rp.snapshot_time < gd.game_date::timestamp + interval '23 hours 30 minutes'
            )
            SELECT
                player_id,
                game_id,
                game_date,
                market_key,
                line,
                bookmaker,
                MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
                MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
            FROM ranked_lines
            WHERE rn = 1
            GROUP BY player_id, game_id, game_date, market_key, line, bookmaker
        """).bindparams(
            bindparam("markets", expanding=True),
            bindparam("bookmakers", expanding=True),
        )

        result = {}
        total_lines = 0

        for ci, chunk_dates in enumerate(chunks):
            chunk_start = chunk_dates[0]
            chunk_end = chunk_dates[-1]

            try:
                with self.engine.connect() as conn:
                    chunk_lines = pd.read_sql(
                        query,
                        conn,
                        params={
                            "chunk_start": chunk_start,
                            "chunk_end": chunk_end,
                            "markets": list(markets),
                            "bookmakers": list(self.bookmakers),
                        },
                    )

                if not chunk_lines.empty:
                    for gd, group_df in chunk_lines.groupby("game_date"):
                        if hasattr(gd, "date"):
                            gd = gd.date()
                        result[gd] = group_df.drop(columns=["game_date"]).reset_index(drop=True)
                    total_lines += len(chunk_lines)

                logger.info(
                    f"  Lines chunk {ci + 1}/{len(chunks)}: "
                    f"{chunk_start} to {chunk_end} "
                    f"({len(chunk_dates)} dates) -> {len(chunk_lines)} rows"
                )

            except Exception:
                logger.warning(f"Lines chunk {ci + 1} failed ({chunk_start} to {chunk_end}), retrying 1-date-at-a-time...")
                for single_date in chunk_dates:
                    try:
                        with self.engine.connect() as conn:
                            single_lines = pd.read_sql(
                                query,
                                conn,
                                params={
                                    "chunk_start": single_date,
                                    "chunk_end": single_date,
                                    "markets": list(markets),
                                    "bookmakers": list(self.bookmakers),
                                },
                            )
                        if not single_lines.empty:
                            for gd, group_df in single_lines.groupby("game_date"):
                                if hasattr(gd, "date"):
                                    gd = gd.date()
                                result[gd] = group_df.drop(columns=["game_date"]).reset_index(drop=True)
                            total_lines += len(single_lines)
                            logger.info(f"    Retry {single_date}: {len(single_lines)} rows")
                    except Exception as retry_e:
                        logger.error(f"    Retry {single_date} also failed: {retry_e}")

        logger.info(f"Prefetched lines for {len(result)} dates ({total_lines} total line entries)")
        return result

    def _calculate_edges(
        self, predictions_df: pd.DataFrame, lines_df: pd.DataFrame, prediction_samples: dict
    ) -> pd.DataFrame:
        """Add edge calculations to predictions using empirical CDF from Monte Carlo samples.

        When a Black-Litterman blender is configured, probabilities are blended with
        the devigged market prior in log-odds space. Otherwise, raw empirical CDF
        probabilities are used (original behavior).
        """
        market_to_stat = {
            "player_points": "pts",
            "player_rebounds": "reb",
            "player_assists": "ast",
        }
        lines_df = lines_df.copy()
        lines_df["stat"] = lines_df["market_key"].map(market_to_stat)

        # Merge predictions with lines (one-to-many: each prediction gets one row per bookmaker/line)
        line_cols = ["player_id", "game_id", "stat", "line", "over_odds", "under_odds"]
        if "bookmaker" in lines_df.columns:
            line_cols.append("bookmaker")
        merged = predictions_df.merge(
            lines_df[line_cols],
            on=["player_id", "game_id", "stat"],
            how="left",
        )

        # Convert odds to implied probability (raw, with vig — used as baseline when BL is off)
        def odds_to_prob(odds):
            if pd.isna(odds):
                return None
            if odds > 0:
                return 100 / (odds + 100)
            else:
                return abs(odds) / (abs(odds) + 100)

        # Check if any stat has BL enabled
        has_any_bl = self.bl_blender is not None or any(
            self._stat_blenders.get(s) is not None for s in self.stats
        )

        if has_any_bl:
            # ── Black-Litterman path (per-stat or global) ──
            # Uses devigged market probs, per-prediction confidence, and log-odds blending.
            bl_results = []
            for _, row in merged.iterrows():
                if pd.isna(row.get("line")) or pd.isna(row.get("over_odds")) or pd.isna(row.get("under_odds")):
                    bl_results.append({
                        "model_over": None, "model_under": None,
                        "market_over": None, "market_under": None,
                        "confidence": None,
                        "posterior_over": None, "posterior_under": None,
                        "bl_enabled": False,
                    })
                    continue

                stat = row["stat"]
                blender = self._get_blender_for_stat(stat)
                samples = prediction_samples.get((row["player_id"], row["game_id"], stat))

                if blender is not None and samples is not None and hasattr(samples, "__len__") and len(samples) > 0:
                    result = blender.blend_prediction(
                        samples=samples,
                        line=row["line"],
                        over_odds=int(row["over_odds"]),
                        under_odds=int(row["under_odds"]),
                    )
                    result["bl_enabled"] = True
                elif blender is not None:
                    # BL enabled but no samples — fall back to market prior (no bet will be placed)
                    try:
                        mkt_over, mkt_under = blender.devig(
                            int(row["over_odds"]), int(row["under_odds"])
                        )
                    except Exception:
                        mkt_over, mkt_under = 0.5, 0.5

                    result = {
                        "model_over": 0.5, "model_under": 0.5,
                        "market_over": mkt_over, "market_under": mkt_under,
                        "confidence": 0.0,
                        "posterior_over": mkt_over, "posterior_under": mkt_under,
                        "bl_enabled": True,
                    }
                else:
                    # No BL for this stat — use raw empirical CDF
                    if samples is not None and hasattr(samples, "__len__") and len(samples) > 0:
                        over_prob = float((samples > row["line"]).mean())
                    else:
                        over_prob = 0.5

                    result = {
                        "model_over": over_prob, "model_under": 1.0 - over_prob,
                        "market_over": None, "market_under": None,
                        "confidence": None,
                        "posterior_over": over_prob, "posterior_under": 1.0 - over_prob,
                        "bl_enabled": False,
                    }

                bl_results.append(result)

            bl_df = pd.DataFrame(bl_results)
            for col in bl_df.columns:
                merged[col] = bl_df[col].values

            # Use posterior probs for betting decisions
            merged["over_prob"] = merged["posterior_over"]
            merged["under_prob"] = merged["posterior_under"]

            # For rows with BL enabled, use devigged market probs as baseline for edge
            # For rows without BL, calculate implied probs from raw odds
            raw_over = merged["over_odds"].apply(odds_to_prob)
            raw_under = merged["under_odds"].apply(odds_to_prob)
            booksum = raw_over + raw_under
            # Fill in implied probs: use market probs if BL was applied, else devigged raw odds
            merged["implied_over"] = merged.apply(
                lambda r: r["market_over"] if r.get("bl_enabled") and pd.notna(r.get("market_over"))
                else (raw_over[r.name] / booksum[r.name] if pd.notna(booksum[r.name]) and booksum[r.name] > 0 else None),
                axis=1
            )
            merged["implied_under"] = merged.apply(
                lambda r: r["market_under"] if r.get("bl_enabled") and pd.notna(r.get("market_under"))
                else (raw_under[r.name] / booksum[r.name] if pd.notna(booksum[r.name]) and booksum[r.name] > 0 else None),
                axis=1
            )

        else:
            # ── Original path (no BL) ──
            def estimate_over_prob(row):
                if pd.isna(row.get("line")):
                    return None

                line = row["line"]
                samples = prediction_samples.get((row["player_id"], row["game_id"], row["stat"]))

                if samples is not None and hasattr(samples, "__len__") and len(samples) > 0:
                    prob_over = float((samples > line).mean())
                else:
                    mean_pred = row.get("pred_mean")
                    std_dev = row.get("pred_std")

                    if pd.isna(mean_pred) or pd.isna(std_dev) or std_dev == 0:
                        return 0.5

                    z_score = (line - mean_pred) / std_dev
                    prob_over = stats.norm.sf(z_score)

                return min(max(prob_over, 0.05), 0.90)

            merged["over_prob"] = merged.apply(estimate_over_prob, axis=1)
            merged["under_prob"] = 1 - merged["over_prob"]

            raw_over = merged["over_odds"].apply(odds_to_prob)
            raw_under = merged["under_odds"].apply(odds_to_prob)
            booksum = raw_over + raw_under
            # Multiplicative devigging: divide each raw prob by the booksum
            merged["implied_over"] = raw_over / booksum
            merged["implied_under"] = raw_under / booksum

            # ── BL Sizing path (optional) ──
            # When bl_sizing_blender is set, compute BL posteriors for Kelly sizing only
            # Edge detection still uses raw model probs (over_prob/under_prob)
            if self.bl_sizing_blender is not None:
                sizing_results = []
                for _, row in merged.iterrows():
                    if pd.isna(row.get("line")) or pd.isna(row.get("over_odds")) or pd.isna(row.get("under_odds")):
                        sizing_results.append({
                            "sizing_prob_over": None,
                            "sizing_prob_under": None,
                        })
                        continue

                    samples = prediction_samples.get((row["player_id"], row["game_id"], row["stat"]))

                    if samples is not None and hasattr(samples, "__len__") and len(samples) > 0:
                        result = self.bl_sizing_blender.blend_prediction(
                            samples=samples,
                            line=row["line"],
                            over_odds=int(row["over_odds"]),
                            under_odds=int(row["under_odds"]),
                        )
                        sizing_results.append({
                            "sizing_prob_over": result["posterior_over"],
                            "sizing_prob_under": result["posterior_under"],
                        })
                    else:
                        # No samples — fall back to raw model probs
                        sizing_results.append({
                            "sizing_prob_over": row["over_prob"],
                            "sizing_prob_under": row["under_prob"],
                        })

                sizing_df = pd.DataFrame(sizing_results)
                merged["sizing_prob_over"] = sizing_df["sizing_prob_over"].values
                merged["sizing_prob_under"] = sizing_df["sizing_prob_under"].values

        # Calculate edges (works for both paths)
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
                ast
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
            value_vars=["pts", "reb", "ast"],
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
    for stat in ["pts", "reb", "ast"]:
        model_file = path / f"{stat}_rate_model.joblib"
        if model_file.exists():
            pipeline["rate_models"][stat] = joblib.load(model_file)

    # Load feature config
    feature_config = joblib.load(path / "feature_config.joblib")
    pipeline["minutes_features"] = feature_config.get("minutes_features", [])
    pipeline["rate_features"] = feature_config.get("rate_features", [])

    return pipeline
