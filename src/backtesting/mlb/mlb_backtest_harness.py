"""
MLB Backtesting Harness.

Day-by-day replay of pitcher K (quantile) and batter (NegBin/binary) models
on historical MLB data. Fetches features, generates predictions, calculates
edges against prop lines, simulates bets, and computes performance metrics.

Adapted from src/backtesting/backtest_harness.py with MLB-specific:
- Multiple model types (quantile, negbin, binary) per stat
- Resolution against mlb_player_game_stats_pitching / _batting
- MLB game_id as integer
- Game schedule from mlb_game_schedule

Reuses sport-agnostic: BetSimulator, MetricsCalculator, BlackLittermanBlender

ARCHITECTURE NOTE: `run_mlb_sweep.py --quote-clean` is the only canonical
production-validation entry point. This harness is retained for single-config
legacy/debug runs only; outputs from this harness must not be used as promotion
evidence. It must share line-selection code with the sweep path; do not add raw
odds SQL here.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.backtesting.bet_simulator import BetSimulator
from src.backtesting.mlb.line_selection import fetch_lines_at_decision_time
from src.backtesting.performance_metrics import MetricsCalculator, PerformanceMetrics
from src.models.black_litterman import BlackLittermanBlender
from src.models.mlb.features.pitcher_inference_loader import PitcherInferenceLoader
from src.models.mlb.features.requests import PlayerGameFeatureRequest

logger = logging.getLogger(__name__)

# Stat -> (table, column) for fetching actuals.
# For compound stats, column is a SQL expression used as `{column} as actual_value`.
STAT_ACTUALS: dict[str, tuple[str, str]] = {
    "pitcher_strikeouts": ("mlb_player_game_stats_pitching", "so"),
    "pitcher_outs":       ("mlb_player_game_stats_pitching", "outs"),
    "batter_hits":        ("mlb_player_game_stats_batting",  "h"),
    "batter_total_bases": ("mlb_player_game_stats_batting",  "tb"),
    "batter_rbis":        ("mlb_player_game_stats_batting",  "rbi"),
    "batter_runs_scored": ("mlb_player_game_stats_batting",  "r"),
    "batter_hrr":         ("mlb_player_game_stats_batting",  "h + r + rbi"),
}


@dataclass
class MLBBacktestResult:
    """Container for MLB backtest results."""

    predictions_df: pd.DataFrame
    bets_df: pd.DataFrame
    all_edges_df: pd.DataFrame
    metrics: PerformanceMetrics
    start_date: date
    end_date: date
    config: dict

    def to_csv(self, output_dir: str) -> None:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        self.predictions_df.to_csv(output_path / "predictions.csv", index=False)
        self.bets_df.to_csv(output_path / "bets.csv", index=False)

        if not self.all_edges_df.empty:
            self.all_edges_df.to_csv(output_path / "all_bookmaker_edges.csv", index=False)

        metrics_output = self.metrics.to_dict()
        metrics_output["config"] = self.config
        with open(output_path / "metrics.json", "w") as f:
            json.dump(metrics_output, f, indent=2, default=str)


@dataclass
class MLBBacktestHarness:
    """
    MLB backtesting orchestrator.

    Iterates through historical MLB game dates, generates time-travel safe
    predictions, simulates betting, and calculates performance metrics.
    """

    engine: object
    pitcher_feature_store: object = None
    batter_feature_store: object = None
    pitcher_k_pipeline: object = None
    pitcher_k_predictor: object = None

    # Configuration
    edge_threshold: float = 0.08
    starting_bankroll: float = 10000.0
    kelly_fraction: float = 0.125
    max_bet_pct: float | None = None
    flat_bet_size: float | None = None
    bookmakers: list[str] = field(default_factory=lambda: [
        "draftkings", "fanduel", "betmgm", "betrivers", "bovada",
        "williamhill_us", "betonlineag", "fanatics",
    ])
    stats: list[str] = field(default_factory=lambda: ["pitcher_strikeouts"])
    bl_blender: BlackLittermanBlender | None = None

    _simulator: BetSimulator = field(init=False)
    _metrics_calc: MetricsCalculator = field(init=False)

    def __post_init__(self):
        logger.warning(
            "MLBBacktestHarness is legacy/debug-only. Use run_mlb_sweep.py --quote-clean "
            "for promotion-grade MLB backtest evidence."
        )
        self._simulator = BetSimulator(
            edge_threshold=self.edge_threshold,
            starting_bankroll=self.starting_bankroll,
            kelly_fraction=self.kelly_fraction,
            max_bet_pct=self.max_bet_pct,
            flat_bet_size=self.flat_bet_size,
        )
        self._metrics_calc = MetricsCalculator()

    def run(
        self,
        start_date: str | date,
        end_date: str | date,
        progress_callback=None,
    ) -> MLBBacktestResult:
        """Run backtest over MLB date range."""
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        logger.info(f"Starting MLB backtest from {start_date} to {end_date}")

        game_dates = self._get_game_dates(start_date, end_date)
        logger.info(f"Found {len(game_dates)} MLB dates with games")

        all_predictions = []
        all_bookmaker_edges = []
        completed_count = 0

        for game_date in game_dates:
            try:
                day_preds, day_edges = self._process_date(game_date)
                if day_preds:
                    all_predictions.extend(day_preds)
                if day_edges:
                    all_bookmaker_edges.extend(day_edges)
            except Exception as e:
                logger.error(f"Error processing {game_date}: {e}")
                continue

            completed_count += 1
            if progress_callback and completed_count % 10 == 0:
                progress_callback(completed_count, len(game_dates))

        if not all_predictions:
            logger.warning("No predictions generated in backtest period")
            empty_df = pd.DataFrame()
            empty_metrics = PerformanceMetrics(
                total_bets=0, wins=0, losses=0, pushes=0,
                total_staked=0.0, total_profit=0.0, roi=0.0,
                return_on_capital=0.0, hit_rate=0.0, sharpe_ratio=0.0,
                max_drawdown=0.0, win_streak=0, loss_streak=0,
                calibration_results=[], overall_calibration_gap=0.0,
                by_stat={}, by_edge_bucket={},
            )
            return MLBBacktestResult(
                predictions_df=empty_df,
                bets_df=empty_df,
                all_edges_df=empty_df,
                metrics=empty_metrics,
                start_date=start_date,
                end_date=end_date,
                config=self._get_config(),
            )

        predictions_df = pd.DataFrame(all_predictions)
        predictions_df = predictions_df.sort_values(["game_date", "game_id", "player_id"])
        all_edges_df = pd.DataFrame(all_bookmaker_edges) if all_bookmaker_edges else pd.DataFrame()

        logger.info(f"Total predictions generated: {len(predictions_df)}")

        # Phase 2: Sequential betting simulation (day-by-day for bankroll management)
        logger.info("Phase 2: Simulating bets...")

        # Build actuals DataFrame for resolution
        actuals_rows = []
        for pred in all_predictions:
            if "actual" in pred and pred["actual"] is not None:
                actuals_rows.append({
                    "player_id": pred["player_id"],
                    "game_id": pred["game_id"],
                    "stat": pred["stat"],
                    "actual_value": pred["actual"],
                })
        actuals_df = pd.DataFrame(actuals_rows) if actuals_rows else pd.DataFrame()

        # Day-by-day simulation
        sorted_dates = sorted(predictions_df["game_date"].unique())
        for sim_date in sorted_dates:
            # Resolve pending bets from previous days
            if len(actuals_df) > 0:
                self._simulator.resolve_bets(actuals_df)

            # Get and evaluate predictions for this date
            day_preds = predictions_df[predictions_df["game_date"] == sim_date]
            self._simulator.evaluate_predictions(day_preds, sim_date)

        # Final resolution
        if len(actuals_df) > 0:
            resolved = self._simulator.resolve_bets(actuals_df)
            logger.info(f"Final resolution: {resolved} resolved bets")

        bets_df = self._simulator.to_dataframe()

        # Merge actuals into predictions_df
        if len(predictions_df) > 0 and len(actuals_df) > 0:
            actuals_merge = actuals_df.rename(columns={"actual_value": "actual"})
            predictions_df = predictions_df.drop(columns=["actual"], errors="ignore")
            predictions_df = predictions_df.merge(
                actuals_merge[["player_id", "game_id", "stat", "actual"]],
                on=["player_id", "game_id", "stat"],
                how="left",
            )

        # Calculate metrics
        metrics = self._metrics_calc.calculate(
            predictions_df, bets_df, starting_bankroll=self.starting_bankroll
        )

        logger.info(
            f"MLB Backtest complete: {len(predictions_df)} predictions, "
            f"{len(bets_df)} bets"
        )

        return MLBBacktestResult(
            predictions_df=predictions_df,
            bets_df=bets_df,
            all_edges_df=all_edges_df,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
            config=self._get_config(),
        )

    def _get_game_dates(self, start_date: date, end_date: date) -> list[date]:
        """Get all dates with MLB games in range from schedule."""
        query = text("""
            SELECT DISTINCT game_date
            FROM mlb_game_schedule
            WHERE game_date BETWEEN :start_date AND :end_date
              AND status != 'Cancelled'
            ORDER BY game_date
        """)

        with self.engine.connect() as conn:
            result = conn.execute(
                query, {"start_date": start_date, "end_date": end_date}
            )
            return [row[0] for row in result]

    def _process_date(self, game_date: date) -> tuple[list[dict], list[dict]]:
        """Process a single game date: features → predictions → edges."""
        predictions = []
        all_edges = []

        # Get games for date
        games = self._get_games(game_date)
        if not games:
            logger.info(f"  {game_date}: no games found in schedule")
            return predictions, all_edges

        # Pitcher K predictions
        pitcher_stats = [s for s in self.stats if s.startswith("pitcher_")]
        if pitcher_stats and self.pitcher_k_predictor is not None:
            pitchers = self._get_pitchers(games)
            logger.info(f"  {game_date}: {len(games)} games, {len(pitchers)} probable pitchers")
            for pitcher in pitchers:
                try:
                    pred = self._predict_pitcher(pitcher, game_date)
                    if pred is None:
                        logger.debug(f"  {game_date}: no features for pitcher {pitcher['player_id']}")
                        continue
                    # Fetch lines and calculate edges
                    lines = self._get_lines_for_player(
                        pitcher["player_id"], pitcher["game_id"],
                        "pitcher_strikeouts", game_date
                    )
                    if not lines:
                        logger.debug(f"  {game_date}: no lines for pitcher {pitcher['player_id']}")
                        continue
                    pred = self._add_edges(pred, lines)
                    predictions.append(pred)
                    all_edges.extend(lines)
                except Exception as e:
                    logger.warning(f"  {game_date}: error predicting pitcher {pitcher['player_id']}: {e}")

        # Fetch actuals for all predictions on this date
        if predictions:
            actuals = self._get_actuals(game_date)
            for pred in predictions:
                key = (pred["player_id"], pred["stat"])
                actual = actuals.get(key)
                if actual is not None:
                    pred["actual"] = actual

        return predictions, all_edges

    def _get_games(self, game_date: date) -> list[dict]:
        """Get games from schedule, joining teams for abbreviations."""
        query = text("""
            SELECT s.game_id, s.home_team_id, s.away_team_id,
                   s.probable_pitcher_home_id, s.probable_pitcher_away_id,
                   s.venue_id,
                   ht.team_abbreviation AS home_team_abbrev,
                   at.team_abbreviation AS away_team_abbrev
            FROM mlb_game_schedule s
            LEFT JOIN mlb_teams ht ON ht.team_id = s.home_team_id
            LEFT JOIN mlb_teams at ON at.team_id = s.away_team_id
            WHERE s.game_date = :game_date
              AND s.status != 'Cancelled'
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"game_date": game_date})
            return [dict(row._mapping) for row in result]

    def _get_pitchers(self, games: list[dict]) -> list[dict]:
        """Extract probable starters from games."""
        pitchers = []
        for game in games:
            for side, opp_side, is_home in [
                ("home", "away", True), ("away", "home", False)
            ]:
                pitcher_id = game.get(f"probable_pitcher_{side}_id")
                if pitcher_id:
                    pitchers.append({
                        "player_id": int(pitcher_id),
                        "game_id": int(game["game_id"]),
                        "team_id": game[f"{side}_team_id"],
                        "opponent_id": game[f"{opp_side}_team_id"],
                        "venue_id": game.get("venue_id") or 0,
                        "is_home": is_home,
                    })
        return pitchers

    def _predict_pitcher(self, pitcher: dict, game_date: date) -> dict | None:
        """Generate prediction for a single pitcher."""
        request = PlayerGameFeatureRequest(
            player_id=pitcher["player_id"],
            game_id=pitcher["game_id"],
            game_date=str(game_date),
            team_id=pitcher["team_id"],
            opp_team_id=pitcher["opponent_id"],
            venue_id=pitcher.get("venue_id", 0),
            season=game_date.year,
            is_home=pitcher["is_home"],
        )
        features = PitcherInferenceLoader(self.pitcher_feature_store).load_player_game(request)

        if features is None:
            return None

        pred = self.pitcher_k_predictor.predict(
            player_id=pitcher["player_id"],
            game_id=pitcher["game_id"],
            features=features,
        )

        return {
            "game_date": game_date,
            "player_id": pred.player_id,
            "game_id": int(pred.game_id),
            "team_id": pitcher["team_id"],
            "opponent_id": pitcher["opponent_id"],
            "stat": pred.stat,
            "model_type": "quantile",
            "pred_mean": pred.mean,
            "pred_median": pred.median,
            "pred_q10": pred.q10,
            "pred_q25": pred.q25,
            "pred_q50": pred.q50,
            "pred_q75": pred.q75,
            "pred_q90": pred.q90,
            "samples": pred.samples,
        }

    def _get_lines_for_player(
        self,
        player_id: int,
        game_id: int,
        market_key: str,
        game_date: date | None = None,
        as_of_time: datetime | None = None,
    ) -> list[dict]:
        """Get quote-clean prop lines for a player/game/market.

        This legacy single-config harness intentionally routes through the same
        shared line-selection helper as ``run_mlb_sweep.py``. If ``as_of_time``
        is omitted we allow the explicit legacy latest-without-as-of mode, but
        post-commence rows are still excluded by the helper.
        """
        lines = fetch_lines_at_decision_time(
            self.engine,
            game_ids=[game_id],
            market_keys=[market_key],
            as_of_time=as_of_time,
            allow_latest_without_as_of=as_of_time is None,
            bookmakers=self.bookmakers,
        )
        if lines.empty:
            return []
        player_lines = lines[lines["player_id"].astype("Int64") == int(player_id)]
        return player_lines.to_dict("records")

    def _add_edges(self, pred: dict, lines: list[dict]) -> dict:
        """Calculate edges using MC samples and best line."""
        samples = pred.get("samples")
        if samples is None or len(samples) == 0:
            return pred

        # Find sharpest line (lowest vig)
        best_line = None
        best_booksum = 999

        for line_info in lines:
            over_odds = line_info["over_odds"]
            under_odds = line_info["under_odds"]
            if over_odds is None or under_odds is None:
                continue

            def _odds_to_prob(odds):
                if odds > 0:
                    return 100 / (odds + 100)
                else:
                    return abs(odds) / (abs(odds) + 100)

            booksum = _odds_to_prob(over_odds) + _odds_to_prob(under_odds)
            if booksum < best_booksum:
                best_booksum = booksum
                best_line = line_info

        if best_line is None:
            return pred

        line_val = best_line["line"]
        over_odds = best_line["over_odds"]
        under_odds = best_line["under_odds"]

        # Empirical CDF
        over_prob = float((samples > line_val).mean())
        over_prob = min(max(over_prob, 0.05), 0.95)
        under_prob = 1 - over_prob

        # Devig
        def _odds_to_prob(odds):
            if odds > 0:
                return 100 / (odds + 100)
            else:
                return abs(odds) / (abs(odds) + 100)

        raw_over = _odds_to_prob(over_odds)
        raw_under = _odds_to_prob(under_odds)
        booksum = raw_over + raw_under
        implied_over = raw_over / booksum
        implied_under = raw_under / booksum

        # BL blending — overwrite probs so edges and bet decisions use posteriors
        if self.bl_blender is not None:
            bl_result = self.bl_blender.blend_prediction(
                samples=samples,
                line=line_val,
                over_odds=over_odds,
                under_odds=under_odds,
            )
            bl_over_prob = bl_result["posterior_over"]
            bl_under_prob = bl_result["posterior_under"]
            pred["bl_over_prob"] = bl_over_prob
            pred["bl_under_prob"] = bl_under_prob
            pred["bl_over_edge"] = bl_over_prob - implied_over
            pred["bl_under_edge"] = bl_under_prob - implied_under
            over_prob = bl_over_prob
            under_prob = bl_under_prob

        pred["line"] = line_val
        pred["over_odds"] = over_odds
        pred["under_odds"] = under_odds
        pred["bookmaker"] = best_line["bookmaker"]
        pred["over_prob"] = over_prob
        pred["under_prob"] = under_prob
        pred["implied_over"] = implied_over
        pred["implied_under"] = implied_under
        pred["over_edge"] = over_prob - implied_over
        pred["under_edge"] = under_prob - implied_under

        return pred

    def _get_actuals(self, game_date: date) -> dict[tuple[int, str], float]:
        """Fetch actual stat values for all players on a date."""
        actuals: dict[tuple[int, str], float] = {}

        for stat, (table, column) in STAT_ACTUALS.items():
            if stat not in self.stats:
                continue

            query = text(f"""
                SELECT player_id, {column} as actual_value
                FROM {table}
                WHERE game_date = :game_date
                  AND did_not_play IS NOT TRUE
            """)

            with self.engine.connect() as conn:
                result = conn.execute(query, {"game_date": game_date})
                for row in result:
                    if row[1] is not None:
                        actuals[(int(row[0]), stat)] = float(row[1])

        return actuals

    def _get_config(self) -> dict:
        return {
            "edge_threshold": self.edge_threshold,
            "starting_bankroll": self.starting_bankroll,
            "kelly_fraction": self.kelly_fraction,
            "max_bet_pct": self.max_bet_pct,
            "stats": self.stats,
            "bookmakers": self.bookmakers,
            "bl_enabled": self.bl_blender is not None,
        }
