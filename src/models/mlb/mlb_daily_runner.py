"""
MLB Daily Prediction Runner.

Production pipeline for generating daily MLB predictions across pitcher K,
batter NegBin, and batter binary model types. Discovers games from
mlb_game_schedule, builds features, generates predictions, fetches prop lines,
calculates edges, and applies BL blending.

Adapted from src/models/daily_runner.py (NBA) with MLB-specific differences:
- No minutes × rate decomposition — all stats predicted directly
- Multiple model types dispatched per stat (quantile, negbin, binary)
- Game discovery from DB table (mlb_game_schedule), not NBA API/CDN
- Pitcher identification from probable_pitcher fields in schedule
- Integer game_id throughout
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

from src.models.black_litterman import BlackLittermanBlender, BLConfig
from src.models.mlb.mlb_stat_config import MLB_STATS

logger = logging.getLogger(__name__)

# Optimal BL config (same approach as NBA)
DEFAULT_BL_TAU = 0.5
DEFAULT_BL_Z_MAX = 1.0
DEFAULT_BL_EDGE_THRESHOLD = 0.08


class MLBDailyPredictionRunner:
    """
    Production pipeline for daily MLB predictions.

    Accepts an MLBModelSuite for unified model access across pitcher quantile,
    batter NegBin, and batter binary model types.
    """

    def __init__(
        self,
        engine,
        suite=None,
        pitcher_feature_store=None,
        batter_feature_store=None,
        # Legacy args — kept for backward compatibility
        pitcher_k_pipeline=None,
        pitcher_k_predictor=None,
        batter_models: dict[str, Any] | None = None,
    ):
        self.engine = engine
        self.pitcher_feature_store = pitcher_feature_store
        self.batter_feature_store = batter_feature_store

        # If suite is provided, use it for all model access
        self.suite = suite

        # Legacy fallback: wrap old-style args
        if suite is not None:
            self.pitcher_k_predictor = suite.get_predictor("pitcher_strikeouts")
        else:
            self.pitcher_k_predictor = pitcher_k_predictor

        self.pitcher_k_pipeline = pitcher_k_pipeline
        self.batter_models = batter_models or {}

    def run_for_date(
        self, target_date: date, stats: list[str] | None = None
    ) -> tuple[pd.DataFrame, dict[tuple, np.ndarray]]:
        """
        Generate predictions for all players in MLB games on target_date.

        Returns:
            (predictions_df, samples_dict) where samples_dict maps
            (player_id, game_id, stat) -> MC samples array
        """
        stats = stats or list(MLB_STATS.keys())

        logger.info(f"Running MLB predictions for {target_date}, stats={stats}")

        # 1. Get today's games from schedule
        games = self._get_games_for_date(target_date)
        logger.info(f"Found {len(games)} MLB games")

        if not games:
            return pd.DataFrame(), {}

        all_predictions = []
        all_samples: dict[tuple, np.ndarray] = {}

        # 2. Pitcher K predictions
        pitcher_stats = [s for s in stats if s.startswith("pitcher_")]
        if pitcher_stats and self.pitcher_k_predictor is not None:
            pitchers = self._get_pitchers_for_games(games)
            logger.info(f"Found {len(pitchers)} probable starters")

            if pitchers:
                pitcher_preds, pitcher_samples = self._run_pitcher_predictions(
                    pitchers, target_date, pitcher_stats
                )
                all_predictions.extend(pitcher_preds)
                all_samples.update(pitcher_samples)

        # 3. Batter predictions
        batter_stats = [s for s in stats if s.startswith("batter_")]
        has_batter_models = (
            (self.suite is not None and self.suite.batter_stats)
            or self.batter_models
        )
        if batter_stats and has_batter_models:
            batters = self._get_batters_for_games(games, target_date)
            logger.info(f"Found {len(batters)} active batters")

            if batters:
                batter_preds, batter_samples = self._run_batter_predictions(
                    batters, target_date, batter_stats
                )
                all_predictions.extend(batter_preds)
                all_samples.update(batter_samples)

        if not all_predictions:
            logger.warning("No MLB predictions generated")
            return pd.DataFrame(), {}

        predictions_df = pd.DataFrame(all_predictions)
        logger.info(f"Generated {len(predictions_df)} MLB predictions")

        # 4. Fetch prop lines
        lines_df = self._get_current_lines(games, stats)

        # 5. Calculate edges
        if len(lines_df) > 0:
            predictions_df = self._calculate_edges(predictions_df, lines_df, all_samples)
        else:
            logger.warning("No MLB prop lines found. Skipping edge calculation.")

        # 6. Compute BL-blended recommendations
        predictions_df = self._compute_bl_recommendations(predictions_df, all_samples)

        # 7. Map feature values to predictions
        predictions_df = self._map_features_to_predictions(predictions_df, games)

        return predictions_df, all_samples

    def _get_games_for_date(self, target_date: date) -> list[dict]:
        """Get MLB games from mlb_game_schedule, joining teams for abbreviations."""
        query = text("""
            SELECT
                s.game_id,
                s.home_team_id,
                s.away_team_id,
                s.probable_pitcher_home_id,
                s.probable_pitcher_away_id,
                s.venue_id,
                s.season,
                s.game_time_utc,
                ht.team_abbreviation AS home_team_abbrev,
                at.team_abbreviation AS away_team_abbrev
            FROM mlb_game_schedule s
            LEFT JOIN mlb_teams ht ON ht.team_id = s.home_team_id
            LEFT JOIN mlb_teams at ON at.team_id = s.away_team_id
            WHERE s.game_date = :target_date
              AND s.status != 'Cancelled'
            ORDER BY s.game_time_utc
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"target_date": target_date})
            games = [dict(row._mapping) for row in result]

        return games

    def _get_pitchers_for_games(self, games: list[dict]) -> list[dict]:
        """Extract probable starters from game schedule."""
        pitchers = []
        for game in games:
            game_id = game["game_id"]
            game_time = game.get("game_time_utc")

            # Home pitcher
            home_pitcher_id = game.get("probable_pitcher_home_id")
            if home_pitcher_id:
                pitchers.append({
                    "player_id": int(home_pitcher_id),
                    "game_id": int(game_id),
                    "team_id": game["home_team_id"],
                    "opponent_id": game["away_team_id"],
                    "is_home": True,
                    "game_time": game_time,
                    "opp_abbrev": game.get("away_team_abbrev"),
                })

            # Away pitcher
            away_pitcher_id = game.get("probable_pitcher_away_id")
            if away_pitcher_id:
                pitchers.append({
                    "player_id": int(away_pitcher_id),
                    "game_id": int(game_id),
                    "team_id": game["away_team_id"],
                    "opponent_id": game["home_team_id"],
                    "is_home": False,
                    "game_time": game_time,
                    "opp_abbrev": game.get("home_team_abbrev"),
                })

        return pitchers

    def _get_batters_for_games(
        self, games: list[dict], target_date: date
    ) -> list[dict]:
        """Get active batters for today's games (based on recent activity)."""
        team_ids = set()
        for g in games:
            if g.get("home_team_id"):
                team_ids.add(g["home_team_id"])
            if g.get("away_team_id"):
                team_ids.add(g["away_team_id"])

        if not team_ids:
            return []

        # Get batters with recent activity (games_szn >= 3)
        query = text("""
            SELECT DISTINCT ON (player_id)
                player_id,
                team_id,
                games_szn
            FROM mlb_player_average_batting
            WHERE team_id IN :team_ids
              AND games_szn >= 3
            ORDER BY player_id, game_date DESC
        """).bindparams(bindparam("team_ids", expanding=True))

        with self.engine.connect() as conn:
            result = conn.execute(query, {"team_ids": list(team_ids)})
            batters_raw = [dict(row._mapping) for row in result]

        # Map batters to games (enriched with venue_id, opp_pitcher_id, season)
        team_game_map: dict[int, dict] = {}
        for g in games:
            home_id = g.get("home_team_id")
            away_id = g.get("away_team_id")
            if home_id and away_id:
                team_game_map[home_id] = {
                    "game_id": g["game_id"],
                    "opponent_id": away_id,
                    "is_home": True,
                    "game_time": g.get("game_time_utc"),
                    "opp_abbrev": g.get("away_team_abbrev"),
                    "venue_id": g.get("venue_id"),
                    "season": g.get("season"),
                    "opp_pitcher_id": g.get("probable_pitcher_away_id"),
                }
                team_game_map[away_id] = {
                    "game_id": g["game_id"],
                    "opponent_id": home_id,
                    "is_home": False,
                    "game_time": g.get("game_time_utc"),
                    "opp_abbrev": g.get("home_team_abbrev"),
                    "venue_id": g.get("venue_id"),
                    "season": g.get("season"),
                    "opp_pitcher_id": g.get("probable_pitcher_home_id"),
                }

        # Look up player names
        player_ids = [b["player_id"] for b in batters_raw]
        if player_ids:
            name_query = text("""
                SELECT player_id, player_name FROM mlb_players WHERE player_id IN :pids
            """).bindparams(bindparam("pids", expanding=True))
            with self.engine.connect() as conn:
                name_rows = conn.execute(name_query, {"pids": player_ids}).fetchall()
            name_map = {row[0]: row[1] for row in name_rows}
        else:
            name_map = {}

        batters = []
        for b in batters_raw:
            mapping = team_game_map.get(b["team_id"])
            if mapping:
                batters.append({
                    "player_id": b["player_id"],
                    "player_name": name_map.get(b["player_id"], "Unknown"),
                    "game_id": mapping["game_id"],
                    "team_id": b["team_id"],
                    "opponent_id": mapping["opponent_id"],
                    "is_home": mapping["is_home"],
                    "game_time": mapping.get("game_time"),
                    "opp_abbrev": mapping.get("opp_abbrev"),
                    "venue_id": mapping.get("venue_id"),
                    "season": mapping.get("season"),
                    "opp_pitcher_id": mapping.get("opp_pitcher_id"),
                })

        return batters

    def _run_pitcher_predictions(
        self,
        pitchers: list[dict],
        target_date: date,
        stats: list[str],
    ) -> tuple[list[dict], dict[tuple, np.ndarray]]:
        """Generate pitcher K predictions using MC predictor."""
        start_time = time.perf_counter()
        predictions = []
        samples_dict: dict[tuple, np.ndarray] = {}

        # Look up pitcher names
        pitcher_ids = [p["player_id"] for p in pitchers]
        name_query = text("""
            SELECT player_id, player_name FROM mlb_players WHERE player_id IN :pids
        """).bindparams(bindparam("pids", expanding=True))
        with self.engine.connect() as conn:
            name_rows = conn.execute(name_query, {"pids": pitcher_ids}).fetchall()
        name_map = {row[0]: row[1] for row in name_rows}

        # Build features in parallel
        def fetch_pitcher_features(pitcher: dict) -> tuple[dict, dict | None]:
            try:
                features = self.pitcher_feature_store.get_player_game_features(
                    player_id=pitcher["player_id"],
                    game_id=pitcher["game_id"],
                    as_of_date=target_date,
                    team_id=pitcher.get("team_id"),
                    opponent_id=pitcher.get("opponent_id"),
                    is_home=pitcher.get("is_home"),
                )
                return pitcher, features
            except Exception as e:
                logger.error(f"Error building features for pitcher {pitcher['player_id']}: {e}")
                return pitcher, None

        pitcher_features = []
        max_workers = min(8, len(pitchers)) if pitchers else 1

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_pitcher_features, p): p for p in pitchers}
            for future in as_completed(futures):
                pitcher, features = future.result()
                if features is not None:
                    pitcher_features.append((pitcher, features))

        # Batch predict
        if pitcher_features and "pitcher_strikeouts" in stats:
            player_games = [
                (p["player_id"], p["game_id"], f)
                for p, f in pitcher_features
            ]
            preds = self.pitcher_k_predictor.predict_batch(player_games)

            for pred, (pitcher, features) in zip(preds, pitcher_features):
                predictions.append({
                    "player_id": pred.player_id,
                    "player_name": name_map.get(pred.player_id, "Unknown"),
                    "game_id": int(pred.game_id),
                    "team_id": pitcher["team_id"],
                    "opponent_id": pitcher["opponent_id"],
                    "stat": pred.stat,
                    "model_type": "quantile",
                    "pred_mean": pred.mean,
                    "pred_std": float(np.std(pred.samples)) if pred.samples is not None else None,
                    "pred_median": pred.median,
                    "pred_q10": pred.q10,
                    "pred_q25": pred.q25,
                    "pred_q50": pred.q50,
                    "pred_q75": pred.q75,
                    "pred_q90": pred.q90,
                    "pred_prob": None,
                    "game_time": pitcher.get("game_time"),
                })
                if pred.samples is not None:
                    samples_dict[(pred.player_id, int(pred.game_id), pred.stat)] = pred.samples

        elapsed = time.perf_counter() - start_time
        logger.info(
            f"Pitcher predictions: {len(predictions)} in {elapsed:.1f}s "
            f"({len(pitcher_features)} pitchers with features)"
        )

        return predictions, samples_dict

    def _run_batter_predictions(
        self,
        batters: list[dict],
        target_date: date,
        stats: list[str],
    ) -> tuple[list[dict], dict[tuple, np.ndarray]]:
        """Generate batter predictions via NegBin/binary dispatch through suite."""
        start_time = time.perf_counter()
        predictions = []
        samples_dict: dict[tuple, np.ndarray] = {}

        # Determine which batter stats we can actually predict
        if self.suite is not None:
            available_batter = [s for s in stats if self.suite.has_stat(s)]
        else:
            available_batter = []

        if not available_batter:
            logger.info("No batter models loaded — skipping batter predictions")
            return predictions, samples_dict

        if self.batter_feature_store is None:
            logger.warning("No batter feature store — skipping batter predictions")
            return predictions, samples_dict

        logger.info(
            "Building batter features for %d batters, stats=%s",
            len(batters), available_batter,
        )

        # Build features in parallel (one call per batter, covers all stats)
        def fetch_batter_features(batter: dict) -> tuple[dict, dict | None]:
            try:
                features = self.batter_feature_store.get_player_game_features(
                    player_id=batter["player_id"],
                    game_id=batter["game_id"],
                    game_date=str(target_date),
                    team_id=batter["team_id"],
                    opp_team_id=batter["opponent_id"],
                    venue_id=batter.get("venue_id") or 0,
                    season=batter.get("season") or target_date.year,
                    is_home=batter["is_home"],
                    opp_pitcher_id=batter.get("opp_pitcher_id"),
                    lineup_pos=0,  # Lineup data not available pre-game
                    stat="hits",   # Base features (stat-specific prop line added per-stat)
                )
                return batter, features
            except Exception as e:
                logger.error(
                    "Error building features for batter %s: %s",
                    batter["player_id"], e,
                )
                return batter, None

        batter_features = []
        max_workers = min(8, len(batters)) if batters else 1

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_batter_features, b): b for b in batters}
            for future in as_completed(futures):
                batter, features = future.result()
                if features is not None:
                    batter_features.append((batter, features))

        logger.info(
            "Batter features built for %d/%d batters", len(batter_features), len(batters)
        )

        # Bulk-fetch all batter prop lines so each stat gets its correct line.
        # The base features were built with stat="hits", so only prop_line_batter_hits
        # is populated. We need prop lines for every market we'll predict.
        prop_line_lookup = self._bulk_fetch_batter_prop_lines(batter_features, available_batter)

        # Mapping from suite stat key → prop line market key
        stat_to_market = {
            "batter_hits": "batter_hits",
            "batter_total_bases": "batter_total_bases",
            "batter_rbis": "batter_rbis",
            "batter_runs_scored": "batter_runs_scored",
            "batter_home_runs": "batter_home_runs",
        }

        # For each available batter stat, build player_games and batch predict
        for stat in available_batter:
            model_type = self.suite.get_model_type(stat)
            market_key = stat_to_market.get(stat, stat)
            prop_line_col = f"prop_line_{market_key}"

            player_games = []
            batter_info = []
            for batter, base_features in batter_features:
                # Copy features so stat-specific overrides don't leak between stats
                feat = dict(base_features)

                # Inject the correct prop line for this stat
                feat[prop_line_col] = prop_line_lookup.get(
                    (batter["player_id"], batter["game_id"], market_key), 0.0
                )

                # Map projected_ab → at_bats for models trained with at_bats feature
                # (e.g. batter_total_bases, batter_runs). projected_ab = max(avg_ab_l5, 1.0)
                feat["at_bats"] = feat.get("projected_ab", feat.get("batter_avg_ab_l5", 3.5))

                player_games.append((batter["player_id"], batter["game_id"], feat))
                batter_info.append(batter)

            if not player_games:
                continue

            try:
                preds = self.suite.predict_batch(stat, player_games)
            except Exception as e:
                logger.error("Error predicting %s: %s", stat, e)
                continue

            for pred, batter in zip(preds, batter_info):
                pred_prob = pred.mean if model_type == "binary" else None

                predictions.append({
                    "player_id": pred.player_id,
                    "player_name": batter.get("player_name", "Unknown"),
                    "game_id": int(pred.game_id),
                    "team_id": batter["team_id"],
                    "opponent_id": batter["opponent_id"],
                    "stat": pred.stat,
                    "model_type": model_type,
                    "pred_mean": pred.mean,
                    "pred_std": float(np.std(pred.samples)) if pred.samples is not None else None,
                    "pred_median": pred.median,
                    "pred_q10": pred.q10,
                    "pred_q25": pred.q25,
                    "pred_q50": pred.q50,
                    "pred_q75": pred.q75,
                    "pred_q90": pred.q90,
                    "pred_prob": pred_prob,
                    "game_time": batter.get("game_time"),
                })
                if pred.samples is not None:
                    samples_dict[(pred.player_id, int(pred.game_id), pred.stat)] = pred.samples

        elapsed = time.perf_counter() - start_time
        logger.info(
            "Batter predictions: %d in %.1fs (%d batters × %d stats)",
            len(predictions), elapsed, len(batter_features), len(available_batter),
        )

        return predictions, samples_dict

    def _bulk_fetch_batter_prop_lines(
        self,
        batter_features: list[tuple[dict, dict]],
        available_stats: list[str],
    ) -> dict[tuple[int, int, str], float]:
        """Fetch all batter prop lines in one query for all (player, game, market_key) combos.

        Returns dict: (player_id, game_id, market_key) → line_value
        """
        stat_to_market = {
            "batter_hits": "batter_hits",
            "batter_total_bases": "batter_total_bases",
            "batter_rbis": "batter_rbis",
            "batter_runs_scored": "batter_runs_scored",
            "batter_home_runs": "batter_home_runs",
        }
        market_keys = [stat_to_market[s] for s in available_stats if s in stat_to_market]

        if not batter_features or not market_keys:
            return {}

        # Collect unique (player_id, game_id) pairs
        player_game_pairs = list({
            (b["player_id"], b["game_id"]) for b, _ in batter_features
        })

        # Build parameterized query
        pg_placeholders = ", ".join(
            f"(:pid_{i}, :gid_{i})" for i in range(len(player_game_pairs))
        )
        mk_placeholders = ", ".join(f":mk_{i}" for i in range(len(market_keys)))

        params: dict = {}
        for i, (pid, gid) in enumerate(player_game_pairs):
            params[f"pid_{i}"] = pid
            params[f"gid_{i}"] = gid
        for i, mk in enumerate(market_keys):
            params[f"mk_{i}"] = mk

        query = text(f"""
            SELECT DISTINCT ON (player_id, game_id, market_key)
                player_id, game_id, market_key, line
            FROM mlb_raw_player_props
            WHERE (player_id, game_id) IN ({pg_placeholders})
              AND market_key IN ({mk_placeholders})
              AND bookmaker IN ('pinnacle', 'draftkings')
            ORDER BY player_id, game_id, market_key, snapshot_time DESC NULLS LAST
        """)

        lookup: dict[tuple[int, int, str], float] = {}
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, params)
                for row in result:
                    lookup[(int(row.player_id), int(row.game_id), row.market_key)] = float(row.line)
            logger.info("Bulk-fetched %d batter prop lines", len(lookup))
        except Exception as e:
            logger.error("Error bulk-fetching batter prop lines: %s", e)

        return lookup

    def _get_current_lines(
        self, games: list[dict], stats: list[str]
    ) -> pd.DataFrame:
        """Fetch the most recent MLB prop lines, selecting sharpest per player/market."""
        game_ids = [g["game_id"] for g in games]

        if not game_ids:
            return pd.DataFrame()

        stat_to_market = {
            "pitcher_strikeouts": "pitcher_strikeouts",
            "pitcher_outs": "pitcher_outs",
            "batter_hits": "batter_hits",
            "batter_total_bases": "batter_total_bases",
            "batter_rbis": "batter_rbis",
            "batter_runs_scored": "batter_runs_scored",
            "batter_home_runs": "batter_home_runs",
        }

        markets = [stat_to_market[s] for s in stats if s in stat_to_market]
        if not markets:
            return pd.DataFrame()

        start_time = time.perf_counter()

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
                FROM mlb_raw_player_props
                WHERE game_id IN :game_ids
                  AND market_key IN :markets
                  AND player_id IS NOT NULL
            )
            SELECT
                player_id,
                game_id,
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

        with self.engine.connect() as conn:
            all_lines = pd.read_sql(
                query, conn, params={"game_ids": game_ids, "markets": markets}
            )

        elapsed = time.perf_counter() - start_time
        logger.info(f"Fetched {len(all_lines)} MLB prop lines in {elapsed:.1f}s")

        if all_lines.empty:
            return all_lines

        # Select sharpest book per player/game/market (lowest vig)
        def _odds_to_prob(odds):
            if pd.isna(odds):
                return None
            if odds > 0:
                return 100 / (odds + 100)
            else:
                return abs(odds) / (abs(odds) + 100)

        all_lines["_raw_over"] = all_lines["over_odds"].apply(_odds_to_prob)
        all_lines["_raw_under"] = all_lines["under_odds"].apply(_odds_to_prob)
        all_lines["_booksum"] = all_lines["_raw_over"] + all_lines["_raw_under"]

        all_lines = all_lines.dropna(subset=["_booksum"])

        idx = all_lines.groupby(["player_id", "game_id", "market_key"])["_booksum"].idxmin()
        best_lines = all_lines.loc[idx].drop(columns=["_raw_over", "_raw_under", "_booksum"])

        return best_lines.reset_index(drop=True)

    def _calculate_edges(
        self,
        predictions_df: pd.DataFrame,
        lines_df: pd.DataFrame,
        samples_dict: dict[tuple, np.ndarray] | None = None,
    ) -> pd.DataFrame:
        """Add edge calculations to predictions using MC samples."""
        # MLB market_key matches stat directly
        lines_df = lines_df.copy()
        lines_df["stat"] = lines_df["market_key"]

        merge_cols = ["player_id", "game_id", "stat", "line", "over_odds", "under_odds"]
        if "bookmaker" in lines_df.columns:
            merge_cols.append("bookmaker")

        merged = predictions_df.merge(
            lines_df[merge_cols],
            on=["player_id", "game_id", "stat"],
            how="left",
        )

        def estimate_over_prob(row):
            if pd.isna(row.get("line")):
                return None

            line = row["line"]

            # Primary: empirical CDF from MC samples
            if samples_dict:
                key = (row["player_id"], int(row["game_id"]), row["stat"])
                samples = samples_dict.get(key)
                if samples is not None and len(samples) > 0:
                    prob_over = float((samples > line).mean())
                    return min(max(prob_over, 0.05), 0.95)

            # Fallback: quantile interpolation
            q_cols = ["pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90"]
            values = [row.get(c) for c in q_cols]
            if any(v is None or pd.isna(v) for v in values):
                return None

            values = [float(v) for v in values]
            if line <= values[0]:
                return 0.95
            elif line >= values[-1]:
                return 0.05
            else:
                prob_under = np.interp(line, values, [0.10, 0.25, 0.50, 0.75, 0.90])
                return 1 - prob_under

        merged["over_prob"] = merged.apply(estimate_over_prob, axis=1)
        merged["under_prob"] = 1 - merged["over_prob"].where(merged["over_prob"].notna())

        # Implied probabilities (multiplicative devigging)
        def odds_to_prob(odds):
            if pd.isna(odds):
                return None
            if odds > 0:
                return 100 / (odds + 100)
            else:
                return abs(odds) / (abs(odds) + 100)

        raw_over = merged["over_odds"].apply(odds_to_prob)
        raw_under = merged["under_odds"].apply(odds_to_prob)
        booksum = raw_over + raw_under
        merged["implied_over"] = raw_over / booksum
        merged["implied_under"] = raw_under / booksum

        merged["over_edge"] = merged["over_prob"] - merged["implied_over"]
        merged["under_edge"] = merged["under_prob"] - merged["implied_under"]

        return merged

    def _compute_bl_recommendations(
        self,
        predictions_df: pd.DataFrame,
        samples_dict: dict[tuple, np.ndarray] | None = None,
    ) -> pd.DataFrame:
        """Compute Black-Litterman blended probabilities and mark recommended picks."""
        predictions_df["bl_over_prob"] = None
        predictions_df["bl_under_prob"] = None
        predictions_df["bl_over_edge"] = None
        predictions_df["bl_under_edge"] = None
        predictions_df["bl_confidence"] = None
        predictions_df["is_recommended"] = False

        if samples_dict is None or not samples_dict:
            logger.warning("No MC samples for BL blending. Skipping.")
            return predictions_df

        bl_config = BLConfig(tau=DEFAULT_BL_TAU, z_max=DEFAULT_BL_Z_MAX)
        blender = BlackLittermanBlender(config=bl_config)

        bl_computed = 0
        recommended_count = 0

        for idx, row in predictions_df.iterrows():
            if pd.isna(row.get("line")) or pd.isna(row.get("over_odds")) or pd.isna(row.get("under_odds")):
                continue

            key = (row["player_id"], int(row["game_id"]), row["stat"])
            samples = samples_dict.get(key)

            if samples is None or len(samples) == 0:
                continue

            bl_result = blender.blend_prediction(
                samples=samples,
                line=row["line"],
                over_odds=row["over_odds"],
                under_odds=row["under_odds"],
            )

            predictions_df.at[idx, "bl_over_prob"] = bl_result["posterior_over"]
            predictions_df.at[idx, "bl_under_prob"] = bl_result["posterior_under"]
            predictions_df.at[idx, "bl_confidence"] = bl_result["confidence"]

            implied_over = row.get("implied_over")
            implied_under = row.get("implied_under")

            if pd.notna(implied_over) and pd.notna(implied_under):
                bl_over_edge = bl_result["posterior_over"] - implied_over
                bl_under_edge = bl_result["posterior_under"] - implied_under

                predictions_df.at[idx, "bl_over_edge"] = bl_over_edge
                predictions_df.at[idx, "bl_under_edge"] = bl_under_edge

                max_bl_edge = max(bl_over_edge, bl_under_edge)
                if max_bl_edge >= DEFAULT_BL_EDGE_THRESHOLD:
                    predictions_df.at[idx, "is_recommended"] = True
                    recommended_count += 1

            bl_computed += 1

        logger.info(
            f"MLB BL blending: {bl_computed} computed, "
            f"{recommended_count} recommended (edge >= {DEFAULT_BL_EDGE_THRESHOLD*100:.0f}%)"
        )

        return predictions_df

    def _map_features_to_predictions(
        self,
        predictions_df: pd.DataFrame,
        games: list[dict],
    ) -> pd.DataFrame:
        """Map feature values to predictions for dashboard insights."""
        # Build opponent abbreviation lookup from games
        game_opp_abbrev: dict[tuple[int, int], str] = {}
        for g in games:
            gid = g["game_id"]
            home_id = g.get("home_team_id")
            away_id = g.get("away_team_id")
            if home_id and away_id:
                game_opp_abbrev[(gid, home_id)] = g.get("away_team_abbrev", "")
                game_opp_abbrev[(gid, away_id)] = g.get("home_team_abbrev", "")

        # Initialize feat columns
        feat_cols = [
            "feat_days_rest", "feat_lineup_position", "feat_park_factor",
            "feat_player_avg_stat_l5", "feat_player_avg_stat_szn", "feat_opp_abbrev",
        ]
        for col in feat_cols:
            if col not in predictions_df.columns:
                predictions_df[col] = None

        # Populate opp_abbrev from games data
        for idx, row in predictions_df.iterrows():
            game_id = int(row["game_id"])
            team_id = row.get("team_id")
            if team_id:
                abbrev = game_opp_abbrev.get((game_id, int(team_id)))
                if abbrev:
                    predictions_df.at[idx, "feat_opp_abbrev"] = abbrev

        return predictions_df
