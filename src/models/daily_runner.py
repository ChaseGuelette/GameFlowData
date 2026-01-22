# production/daily_runner.py

import logging
from datetime import date, datetime

import numpy as np
import pandas as pd
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DailyPredictionRunner:
    """
    Production pipeline for daily predictions.
    """

    def __init__(self, engine, feature_store, model_pipeline, predictor):
        self.engine = engine
        self.feature_store = feature_store
        self.pipeline = model_pipeline
        self.predictor = predictor

    def run_for_date(self, target_date: date, stats: list[str] = None) -> pd.DataFrame:
        """
        Generate predictions for all players in games on target_date.
        """
        stats = stats or ["pts", "reb", "ast"]

        logger.info(f"Running predictions for {target_date}")

        # 1. Get today's games and players
        games = self._get_games_for_date(target_date)
        logger.info(f"Found {len(games)} games")

        # 2. Get all players expected to play
        players = self._get_players_for_games(games)
        logger.info(f"Found {len(players)} players (pre-injury filter)")
        
        # Filter injured players
        players = self._filter_injured_players(players, target_date)
        logger.info(f"Found {len(players)} players (post-injury filter)")

        # 3. Generate predictions
        all_predictions = []

        for player in players:
            try:
                features = self.feature_store.get_player_game_features(
                    player_id=player["player_id"], game_id=player["game_id"], as_of_date=target_date
                )

                if features is None:
                    continue

                preds = self.predictor.predict(
                    player_id=player["player_id"],
                    game_id=player["game_id"],
                    features=features,
                    stats=stats,
                )

                for stat, pred in preds.items():
                    all_predictions.append(
                        {
                            "player_id": player["player_id"],
                            "player_name": player["player_name"],
                            "game_id": player["game_id"],
                            "team_id": player["team_id"],
                            "opponent_id": player["opponent_id"],
                            "stat": stat,
                            "pred_mean": pred.mean,
                            "pred_median": pred.median,
                            "pred_q10": pred.q10,
                            "pred_q25": pred.q25,
                            "pred_q50": pred.q50,
                            "pred_q75": pred.q75,
                            "pred_q90": pred.q90,
                            "prediction_time": datetime.now(),
                        }
                    )

            except Exception as e:
                logger.error(f"Error predicting for player {player['player_id']}: {e}")
                continue

        predictions_df = pd.DataFrame(all_predictions)
        logger.info(f"Generated {len(predictions_df)} predictions")

        # 4. Get current prop lines
        lines_df = self._get_current_lines(games, stats)

        # 5. Calculate edges
        if len(lines_df) > 0:
            predictions_df = self._calculate_edges(predictions_df, lines_df)
        else:
            logger.warning("No prop lines found for today's games. Skipping edge calculation.")

        return predictions_df

    def _get_games_for_date(self, target_date: date) -> list[dict]:
        """Get all games scheduled for target date."""
        query = """
            SELECT DISTINCT game_id,
                   MAX(CASE WHEN team_matchup LIKE '%vs.%' THEN team_id END) as home_team_id,
                   MAX(CASE WHEN team_matchup LIKE '%@%' THEN team_id END) as away_team_id
            FROM team_game_stats
            WHERE team_game_date::date = :target_date
            GROUP BY game_id
        """

        with self.engine.connect() as conn:
            result = conn.execute(query, {"target_date": target_date})
            return [dict(r) for r in result]

    def _get_players_for_games(self, games: list[dict]) -> list[dict]:
        """Get expected players for games (based on recent activity)."""
        [g["game_id"] for g in games]

        # Get players who played recently for teams in these games
        query = """
            WITH game_teams AS (
                SELECT game_id, home_team_id, away_team_id
                FROM (VALUES {}) AS t(game_id, home_team_id, away_team_id)
            ),
            recent_players AS (
                SELECT DISTINCT ON (pgs.player_id)
                    pgs.player_id,
                    p.player_name,
                    pgs.team_id,
                    pgs.avg_min_l5
                FROM player_average_game_stats pgs
                JOIN players p ON pgs.player_id = p.player_id
                WHERE pgs.team_id IN (
                    SELECT home_team_id FROM game_teams
                    UNION
                    SELECT away_team_id FROM game_teams
                )
                AND pgs.avg_min_l5 >= 10  -- Only players averaging 10+ minutes
                ORDER BY pgs.player_id, pgs.game_date DESC
            )
            SELECT rp.*, gt.game_id,
                   CASE WHEN rp.team_id = gt.home_team_id THEN gt.away_team_id
                        ELSE gt.home_team_id END as opponent_id
            FROM recent_players rp
            JOIN game_teams gt ON rp.team_id IN (gt.home_team_id, gt.away_team_id)
        """

        # This is simplified - actual implementation would properly construct the VALUES clause
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return [dict(r) for r in result]

    def _filter_injured_players(self, players: list[dict], target_date: date) -> list[dict]:
        """Remove players listed as 'Out' in injury report."""
        try:
            # Get latest injury scrape for date
            start_ts = datetime.combine(target_date, datetime.min.time())
            end_ts = datetime.combine(target_date, datetime.max.time())
            
            # Find latest scrape timestamp in range
            ts_query = "SELECT MAX(scrape_timestamp) FROM espn_injuries WHERE scrape_timestamp >= :start AND scrape_timestamp <= :end"
            
            with self.engine.connect() as conn:
                latest_ts = conn.execute(text(ts_query), {"start": start_ts, "end": end_ts}).scalar()
                
                if not latest_ts:
                    # Fallback to most recent ever if today has no report yet? 
                    # Safer to just look back 24h
                    # For now, just log and return all
                    logger.warning(f"No injury report found for {target_date}")
                    return players

                # Get OUT players
                # Status is often "Out", "Out for season", "Out indefinitely"
                inj_query = """
                    SELECT player_name 
                    FROM espn_injuries 
                    WHERE scrape_timestamp = :ts 
                    AND (lower(status) LIKE '%out%' OR lower(status) = 'doubtful')
                """
                result = conn.execute(text(inj_query), {"ts": latest_ts})
                out_names = {row[0].lower().strip() for row in result}
                
            # Filter
            active_players = []
            for p in players:
                # Normalize name
                p_name = p["player_name"].lower().strip()
                # Simple check - could improve with fuzzy match
                if p_name in out_names:
                    continue
                # Handle "Jr.", "III" differences if strict match fails?
                # For now exact match on lower/strip is decent baseline
                active_players.append(p)
                
            return active_players

        except Exception as e:
            logger.error(f"Error filtering injuries: {e}")
            return players

    def _get_current_lines(self, games: list[dict], stats: list[str]) -> pd.DataFrame:
        """Fetch current prop lines from database."""
        game_ids = [g["game_id"] for g in games]

        stat_to_market = {
            "pts": "player_points",
            "reb": "player_rebounds",
            "ast": "player_assists",
        }

        markets = [stat_to_market[s] for s in stats if s in stat_to_market]

        query = """
            SELECT
                player_id,
                game_id,
                market_key,
                line,
                MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
                MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
            FROM raw_player_props_combined
            WHERE game_id IN :game_ids
              AND market_key IN :markets
              AND bookmaker = 'pinnacle'
            GROUP BY player_id, game_id, market_key, line
        """

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"game_ids": tuple(game_ids), "markets": tuple(markets)})

    def _calculate_edges(self, predictions_df: pd.DataFrame, lines_df: pd.DataFrame) -> pd.DataFrame:
        """Add edge calculations to predictions."""

        # Map market_key back to stat
        market_to_stat = {
            "player_points": "pts",
            "player_rebounds": "reb",
            "player_assists": "ast",
        }
        lines_df["stat"] = lines_df["market_key"].map(market_to_stat)

        # Merge
        merged = predictions_df.merge(
            lines_df[["player_id", "game_id", "stat", "line", "over_odds", "under_odds"]],
            on=["player_id", "game_id", "stat"],
            how="left",
        )

        # Calculate probabilities and edges
        def estimate_over_prob(row):
            if pd.isna(row["line"]):
                return None

            quantiles = [0.10, 0.25, 0.50, 0.75, 0.90]
            values = [
                row["pred_q10"],
                row["pred_q25"],
                row["pred_q50"],
                row["pred_q75"],
                row["pred_q90"],
            ]

            if row["line"] <= values[0]:
                return 0.95
            elif row["line"] >= values[-1]:
                return 0.05
            else:
                prob_under = np.interp(row["line"], values, quantiles)
                return 1 - prob_under

        merged["over_prob"] = merged.apply(estimate_over_prob, axis=1)
        merged["under_prob"] = 1 - merged["over_prob"]

        # Implied probabilities
        def odds_to_prob(odds):
            if pd.isna(odds):
                return None
            if odds > 0:
                return 100 / (odds + 100)
            else:
                return abs(odds) / (abs(odds) + 100)

        merged["implied_over"] = merged["over_odds"].apply(odds_to_prob)
        merged["implied_under"] = merged["under_odds"].apply(odds_to_prob)

        # Edges
        merged["over_edge"] = merged["over_prob"] - merged["implied_over"]
        merged["under_edge"] = merged["under_prob"] - merged["implied_under"]

        return merged
