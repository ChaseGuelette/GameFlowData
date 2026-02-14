# production/daily_runner.py

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

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

    def run_for_date(
        self, target_date: date, stats: list[str] = None
    ) -> tuple[pd.DataFrame, dict[tuple, np.ndarray]]:
        """
        Generate predictions for all players in games on target_date.

        Uses the efficient batch prediction path (4 XGBoost calls total)
        instead of per-player calls.

        Returns:
            (predictions_df, samples_dict) where samples_dict maps
            (player_id, game_id, stat) -> MC samples array
        """
        stats = stats or ["pts", "reb", "ast"]

        logger.info(f"Running predictions for {target_date}")

        # 1. Get today's games and players
        games = self._get_games_for_date(target_date)
        logger.info(f"Found {len(games)} games")

        # 2. Get all players expected to play
        players = self._get_players_for_games(games, target_date)
        logger.info(f"Found {len(players)} players (pre-injury filter)")

        # Filter injured players
        players = self._filter_injured_players(players, target_date)
        logger.info(f"Found {len(players)} players (post-injury filter)")

        if not players:
            return pd.DataFrame(), {}

        # 3. Build features DataFrame (batch)
        features_df = self._build_features_df(players, target_date)

        if features_df.empty:
            logger.warning("No features could be built for any player.")
            return pd.DataFrame(), {}

        logger.info(f"Built features for {len(features_df)} players")

        # 4. Batch predict (4 XGBoost calls total, not N)
        predictions_list, samples_dict = self.predictor.predict_batch_for_date(
            features_df, stats=stats
        )
        predictions_df = pd.DataFrame(predictions_list)

        if predictions_df.empty:
            return pd.DataFrame(), {}

        # 5. Enrich with opponent_id (batch predict doesn't include it)
        predictions_df = self._enrich_predictions(predictions_df, players)

        logger.info(f"Generated {len(predictions_df)} predictions")

        # 6. Get current prop lines
        lines_df = self._get_current_lines(games, stats)

        # 7. Calculate edges (using MC samples for accurate probability estimates)
        if len(lines_df) > 0:
            predictions_df = self._calculate_edges(predictions_df, lines_df, samples_dict)
        else:
            logger.warning("No prop lines found for today's games. Skipping edge calculation.")

        return predictions_df, samples_dict

    def _build_features_df(self, players: list[dict], target_date: date) -> pd.DataFrame:
        """Build features DataFrame for all players using parallel execution.

        Uses ThreadPoolExecutor to fetch features concurrently, reducing
        wall-clock time from O(n * query_time) to O(n * query_time / workers).
        """
        start_time = time.perf_counter()

        def fetch_single(player: dict) -> pd.DataFrame | None:
            """Thread-safe feature fetch for a single player."""
            try:
                features = self.feature_store.get_player_game_features(
                    player_id=player["player_id"],
                    game_id=player["game_id"],
                    as_of_date=target_date,
                    team_id=player.get("team_id"),
                    opponent_id=player.get("opponent_id"),
                    is_home=player.get("is_home"),
                )
                if features is not None:
                    row_df = pd.DataFrame([features]) if isinstance(features, dict) else features
                    row_df["player_id"] = player["player_id"]
                    row_df["player_name"] = player["player_name"]
                    row_df["game_id"] = player["game_id"]
                    row_df["team_id"] = player["team_id"]
                    row_df["game_time"] = player.get("game_time")
                    return row_df
            except Exception as e:
                logger.error(f"Error building features for player {player['player_id']}: {e}")
            return None

        all_features = []
        max_workers = min(8, len(players)) if players else 1

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_single, p): p for p in players}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    all_features.append(result)

        elapsed = time.perf_counter() - start_time
        logger.info(f"Built features for {len(all_features)} players in {elapsed:.1f}s ({max_workers} workers)")

        if all_features:
            return pd.concat(all_features, ignore_index=True)
        return pd.DataFrame()

    def _enrich_predictions(self, predictions_df: pd.DataFrame, players: list[dict]) -> pd.DataFrame:
        """Add opponent_id to predictions from the players list."""
        player_lookup = {
            (p["player_id"], p["game_id"]): p.get("opponent_id")
            for p in players
        }
        predictions_df["opponent_id"] = predictions_df.apply(
            lambda r: player_lookup.get((r["player_id"], r["game_id"])), axis=1
        )
        return predictions_df

    def _get_games_for_date(self, target_date: date) -> list[dict]:
        """Get all games for target date via NBA API ScoreboardV2.

        Falls back to team_game_stats DB query for past dates if the
        NBA API is unavailable.
        """
        # Primary: NBA API ScoreboardV2 (works for scheduled/future games)
        games = self._get_games_from_nba_api(target_date)
        if games:
            # Enrich with game times from odds API (NBA API doesn't provide scheduled start times)
            games = self._enrich_game_times(games, target_date)
            return games

        # Fallback: DB query (only works for past dates with box score data)
        logger.warning("NBA API unavailable, falling back to team_game_stats query")
        return self._get_games_from_db(target_date)

    def _get_games_from_nba_api(self, target_date: date) -> list[dict]:
        """Fetch today's games from NBA API ScoreboardV2."""
        try:
            from nba_api.stats.endpoints import scoreboardv2

            scoreboard = scoreboardv2.ScoreboardV2(
                game_date=target_date.strftime("%Y-%m-%d"),
                timeout=15,
            )
            # Respect NBA API rate limits
            time.sleep(0.6)

            game_header = scoreboard.get_data_frames()[0]

            if game_header.empty:
                logger.info(f"No games found via NBA API for {target_date}")
                return []

            games = []
            for _, row in game_header.iterrows():
                games.append({
                    "game_id": row["GAME_ID"],
                    "home_team_id": int(row["HOME_TEAM_ID"]),
                    "away_team_id": int(row["VISITOR_TEAM_ID"]),
                    "game_time": None,  # Will be enriched from odds API
                })

            logger.info(f"Found {len(games)} games via NBA API ScoreboardV2")
            return games

        except Exception as e:
            logger.warning(f"NBA API ScoreboardV2 failed: {e}")
            return []

    def _get_games_from_db(self, target_date: date) -> list[dict]:
        """Fallback: get games from team_game_stats (only works for past dates)."""
        query = text("""
            SELECT DISTINCT game_id,
                   MAX(CASE WHEN team_matchup LIKE '%vs.%' THEN team_id END) as home_team_id,
                   MAX(CASE WHEN team_matchup LIKE '%@%' THEN team_id END) as away_team_id
            FROM team_game_stats
            WHERE team_game_date::date = :target_date
            GROUP BY game_id
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"target_date": target_date})
            return [dict(row._mapping) for row in result]

    def _enrich_game_times(self, games: list[dict], target_date: date) -> list[dict]:
        """Add game_time to games from odds API (raw_game_lines_staging).

        The NBA API ScoreboardV2 doesn't provide scheduled start times,
        so we get them from the odds API instead.
        """
        from datetime import timedelta

        # Games for target_date evening are stored with next day's UTC date
        # (e.g., 7:30 PM ET on Feb 10 = 00:30 UTC on Feb 11)
        utc_date = target_date + timedelta(days=1)

        query = text("""
            WITH game_times AS (
                SELECT DISTINCT ON (home_team, away_team)
                    home_team,
                    away_team,
                    commence_time
                FROM raw_game_lines_staging
                WHERE commence_time::date = :utc_date
                ORDER BY home_team, away_team, snapshot_time DESC
            )
            SELECT
                t_home.team_id as home_team_id,
                t_away.team_id as away_team_id,
                gt.commence_time
            FROM game_times gt
            JOIN teams t_home ON t_home.team_name = gt.home_team
                OR (t_home.team_name = 'LA Clippers' AND gt.home_team = 'Los Angeles Clippers')
            JOIN teams t_away ON t_away.team_name = gt.away_team
                OR (t_away.team_name = 'LA Clippers' AND gt.away_team = 'Los Angeles Clippers')
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"utc_date": utc_date})
                time_lookup = {
                    (row[0], row[1]): row[2]
                    for row in result
                }

            # Log if no times found in database
            if not time_lookup:
                logger.warning(
                    f"No game times found in raw_game_lines_staging for UTC date {utc_date}. "
                    "Ensure lines_job ran before inference."
                )
                return games

            # Enrich games with times
            enriched_count = 0
            for game in games:
                home_id = game.get("home_team_id")
                away_id = game.get("away_team_id")
                game_time = time_lookup.get((home_id, away_id))
                if game_time:
                    game["game_time"] = game_time
                    enriched_count += 1
                    logger.debug(f"Game {game['game_id']}: {game_time}")

            # Log enrichment results with visibility into partial failures
            if enriched_count == len(games):
                logger.info(f"Enriched {enriched_count} games with start times")
            elif enriched_count > 0:
                logger.warning(
                    f"Partial game time enrichment: {enriched_count}/{len(games)} games. "
                    "Some games may display without times in dashboard."
                )
            else:
                logger.error(
                    f"Failed to enrich ANY game times (0/{len(games)} games). "
                    "Dashboard will not display game times. Check team name matching."
                )

            return games

        except Exception as e:
            logger.error(
                f"Game time enrichment failed with exception: {e}. "
                "Dashboard will not display game times for these predictions."
            )
            return games

    def _get_players_for_games(self, games: list[dict], target_date: date) -> list[dict]:
        """Get expected players for games (based on recent activity)."""
        if not games:
            return []

        # Collect all team IDs from today's games
        team_ids = set()
        for g in games:
            if g.get("home_team_id"):
                team_ids.add(g["home_team_id"])
            if g.get("away_team_id"):
                team_ids.add(g["away_team_id"])

        if not team_ids:
            return []

        # Get recent active players for these teams
        # Filter to players who have played in the last 30 days to exclude retired players
        query = text("""
            SELECT DISTINCT ON (pgs.player_id)
                pgs.player_id,
                p.player_name,
                pgs.team_id,
                pgs.avg_min_l5
            FROM player_average_game_stats pgs
            JOIN players p ON pgs.player_id = p.player_id
            WHERE pgs.team_id IN :team_ids
              AND pgs.avg_min_l5 >= 10
              AND pgs.game_date >= :cutoff_date
            ORDER BY pgs.player_id, pgs.game_date DESC
        """).bindparams(bindparam("team_ids", expanding=True))

        # Calculate cutoff date (30 days before target to exclude retired players)
        cutoff_date = target_date - timedelta(days=30)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"team_ids": list(team_ids), "cutoff_date": cutoff_date})
            players = [dict(row._mapping) for row in result]

        # Map each player to their game and opponent
        # Build team -> game mapping
        team_game_map = {}  # team_id -> (game_id, opponent_id, is_home, game_time)
        for g in games:
            home = g.get("home_team_id")
            away = g.get("away_team_id")
            game_time = g.get("game_time")
            if home and away:
                team_game_map[home] = {"game_id": g["game_id"], "opponent_id": away, "is_home": True, "game_time": game_time}
                team_game_map[away] = {"game_id": g["game_id"], "opponent_id": home, "is_home": False, "game_time": game_time}

        result_players = []
        for p in players:
            mapping = team_game_map.get(p["team_id"])
            if mapping:
                p["game_id"] = mapping["game_id"]
                p["opponent_id"] = mapping["opponent_id"]
                p["is_home"] = mapping["is_home"]
                p["game_time"] = mapping.get("game_time")
                result_players.append(p)

        return result_players

    def _filter_injured_players(self, players: list[dict], target_date: date) -> list[dict]:
        """Remove players listed as 'Out' using rapidapi_injuries (player_id matching).

        Gets each player's MOST RECENT status from the last 7 days, filtering out
        players whose latest status is 'Out'. This handles cases where a player
        is marked Out on day N but not re-listed on day N+1.

        Matches by player_id (integer) for reliable filtering, consistent
        with the feature_store and backtest harness injury queries.
        """
        try:
            # Get players whose most recent status (in last 7 days) is 'Out'
            # This fixes the bug where players marked Out yesterday but not
            # re-listed today would slip through
            query = text("""
                WITH recent_injuries AS (
                    SELECT
                        player_id,
                        status,
                        report_date,
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

            # Look back 7 days for injury reports
            cutoff_date = target_date - timedelta(days=7)

            with self.engine.connect() as conn:
                result = conn.execute(query, {"target_date": target_date, "cutoff_date": cutoff_date})
                out_player_ids = {row[0] for row in result}

            if not out_player_ids:
                logger.info("No 'Out' players found in recent injury reports.")
                return players

            active_players = [p for p in players if p["player_id"] not in out_player_ids]
            n_filtered = len(players) - len(active_players)
            if n_filtered > 0:
                logger.info(f"Filtered {n_filtered} injured players (Out)")

            return active_players

        except Exception as e:
            logger.error(f"Error filtering injuries: {e}")
            return players

    def _get_current_lines(self, games: list[dict], stats: list[str]) -> pd.DataFrame:
        """Fetch the most recent prop lines, then select the sharpest
        (lowest-vig) line per player/game/market for edge calculation.

        Uses ROW_NUMBER over snapshot_time to get only the latest snapshot
        per player/game/market/bookmaker/line, consistent with the backtest harness.
        """
        game_ids = [g["game_id"] for g in games]

        if not game_ids:
            return pd.DataFrame()

        stat_to_market = {
            "pts": "player_points",
            "reb": "player_rebounds",
            "ast": "player_assists",
        }

        markets = [stat_to_market[s] for s in stats if s in stat_to_market]

        # Create both 10-digit and 8-digit versions of game_ids to search
        # raw_player_props_combined has mixed formats (some 8-digit, some 10-digit)
        # Searching both avoids LPAD() in WHERE clause which prevents index usage on 26M rows
        game_ids_10digit = [g.zfill(10) for g in game_ids]  # Ensure 10-digit
        game_ids_8digit = [g.lstrip('0') for g in game_ids]  # Strip leading zeros
        all_game_ids = list(set(game_ids_10digit + game_ids_8digit))

        start_time = time.perf_counter()
        # Query uses both 8-digit and 10-digit game_ids to match mixed data formats
        # After index creation on game_id, this should be very fast
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
                line,
                MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
                MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
            FROM ranked_lines
            WHERE rn = 1
            GROUP BY player_id, game_id, bookmaker, market_key, line
        """).bindparams(
            bindparam("game_ids", expanding=True),
            bindparam("markets", expanding=True),
        )

        with self.engine.connect() as conn:
            all_lines = pd.read_sql(query, conn, params={"game_ids": all_game_ids, "markets": list(markets)})

        elapsed = time.perf_counter() - start_time
        logger.info(f"Fetched {len(all_lines)} prop lines in {elapsed:.1f}s")

        if all_lines.empty:
            return all_lines

        # Select the sharpest book per player/game/market (lowest booksum = lowest vig)
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

        # Drop rows missing either side
        all_lines = all_lines.dropna(subset=["_booksum"])

        # Keep the row with the lowest booksum (sharpest) per player/game/market
        idx = all_lines.groupby(["player_id", "game_id", "market_key"])["_booksum"].idxmin()
        best_lines = all_lines.loc[idx].drop(columns=["_raw_over", "_raw_under", "_booksum", "bookmaker"])

        return best_lines.reset_index(drop=True)

    def _calculate_edges(
        self,
        predictions_df: pd.DataFrame,
        lines_df: pd.DataFrame,
        samples_dict: dict[tuple, np.ndarray] | None = None,
    ) -> pd.DataFrame:
        """Add edge calculations to predictions.

        Uses MC samples (empirical CDF) for probability estimation when available,
        consistent with the backtest harness. Falls back to quantile interpolation
        if samples are missing for a given prediction.
        """
        # Map market_key back to stat
        market_to_stat = {
            "player_points": "pts",
            "player_rebounds": "reb",
            "player_assists": "ast",
        }
        lines_df = lines_df.copy()
        lines_df["stat"] = lines_df["market_key"].map(market_to_stat)

        # Merge
        merged = predictions_df.merge(
            lines_df[["player_id", "game_id", "stat", "line", "over_odds", "under_odds"]],
            on=["player_id", "game_id", "stat"],
            how="left",
        )

        # Calculate probabilities using MC samples (empirical CDF)
        def estimate_over_prob(row):
            if pd.isna(row.get("line")):
                return None

            line = row["line"]

            # Primary: empirical CDF from MC samples (10k samples → accurate)
            if samples_dict:
                key = (row["player_id"], row["game_id"], row["stat"])
                samples = samples_dict.get(key)
                if samples is not None and len(samples) > 0:
                    prob_over = float((samples > line).mean())
                    return min(max(prob_over, 0.05), 0.95)

            # Fallback: quantile interpolation (coarser, 5 points)
            values = [
                row["pred_q10"], row["pred_q25"], row["pred_q50"],
                row["pred_q75"], row["pred_q90"],
            ]
            if line <= values[0]:
                return 0.95
            elif line >= values[-1]:
                return 0.05
            else:
                prob_under = np.interp(line, values, [0.10, 0.25, 0.50, 0.75, 0.90])
                return 1 - prob_under

        merged["over_prob"] = merged.apply(estimate_over_prob, axis=1)
        merged["under_prob"] = 1 - merged["over_prob"]

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

        # Edges
        merged["over_edge"] = merged["over_prob"] - merged["implied_over"]
        merged["under_edge"] = merged["under_prob"] - merged["implied_under"]

        return merged
