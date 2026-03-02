# production/daily_runner.py

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

from src.models.black_litterman import BlackLittermanBlender, BLConfig

logger = logging.getLogger(__name__)

# Optimal BL config from backtest sweep (61.5% hit rate, 7.72% ROI)
DEFAULT_BL_TAU = 0.5
DEFAULT_BL_Z_MAX = 1.0
DEFAULT_BL_EDGE_THRESHOLD = 0.09


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

        # 8. Compute BL-blended recommendations ("Model Picks")
        predictions_df = self._compute_bl_recommendations(predictions_df, samples_dict)

        # 9. Map feature values to predictions for dashboard insights
        predictions_df = self._map_features_to_predictions(predictions_df, features_df)

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

    def _map_features_to_predictions(
        self, predictions_df: pd.DataFrame, features_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Map feature values to predictions for dashboard insights.

        Extracts B1 (injury), B2 (rest/schedule), and B3 (trend) features from the
        features DataFrame and attaches them to predictions for display in the
        dashboard's Analysis Modal.
        """
        if features_df.empty:
            return predictions_df

        # Ensure required columns exist for mapping
        required_cols = ["player_id", "game_id", "stat"]
        if not all(col in predictions_df.columns for col in required_cols):
            logger.warning("predictions_df missing required columns for feature mapping")
            return predictions_df

        # Create lookup from (player_id, game_id) to feature row
        # Use first occurrence if duplicates exist
        features_df = features_df.drop_duplicates(subset=["player_id", "game_id"], keep="first")
        feature_lookup = features_df.set_index(["player_id", "game_id"])

        # Get opponent abbreviations from database
        opp_abbrev_map = self._get_opponent_abbrevs(predictions_df)

        # Initialize feat_* columns with None
        feat_cols = [
            "feat_rest_days", "feat_is_back_to_back", "feat_games_last_7d",
            "feat_team_out_count", "feat_team_out_min_sum", "feat_opp_out_count",
            "feat_player_is_questionable", "feat_player_is_probable",
            "feat_player_avg_stat_l3", "feat_player_avg_stat_l5", "feat_player_avg_stat_l15",
            "feat_stat_l3_l15_ratio", "feat_stat_std_l5", "feat_opp_abbrev",
        ]
        for col in feat_cols:
            predictions_df[col] = None

        # Map features to each prediction row
        for idx, row in predictions_df.iterrows():
            key = (row["player_id"], row["game_id"])
            if key not in feature_lookup.index:
                continue

            feat = feature_lookup.loc[key]
            stat = row["stat"]

            # B2: Rest/Schedule (same for all stats)
            predictions_df.at[idx, "feat_rest_days"] = int(feat.get("rest_days", 0) or 0)
            predictions_df.at[idx, "feat_is_back_to_back"] = bool(feat.get("is_back_to_back", 0))
            predictions_df.at[idx, "feat_games_last_7d"] = int(feat.get("games_in_last_7_days", 0) or 0)

            # B1: Injury Context
            predictions_df.at[idx, "feat_team_out_count"] = int(feat.get("team_out_count", 0) or 0)
            predictions_df.at[idx, "feat_team_out_min_sum"] = float(feat.get("team_out_min_sum", 0) or 0)
            predictions_df.at[idx, "feat_opp_out_count"] = int(feat.get("opp_out_count", 0) or 0)
            predictions_df.at[idx, "feat_player_is_questionable"] = bool(feat.get("player_is_questionable", 0))
            predictions_df.at[idx, "feat_player_is_probable"] = bool(feat.get("player_is_probable", 0))

            # B3: Stat-specific averages/trends
            if stat in ("pts", "reb", "ast"):
                s = stat
                avg_l3 = feat.get(f"player_avg_{s}_l3")
                avg_l5 = feat.get(f"player_avg_{s}_l5")
                avg_l15 = feat.get(f"player_avg_{s}_l15")
                ratio = feat.get(f"player_{s}_l3_l15_ratio")
                std = feat.get(f"player_std_{s}_l5")

                predictions_df.at[idx, "feat_player_avg_stat_l3"] = float(avg_l3) if avg_l3 is not None else None
                predictions_df.at[idx, "feat_player_avg_stat_l5"] = float(avg_l5) if avg_l5 is not None else None
                predictions_df.at[idx, "feat_player_avg_stat_l15"] = float(avg_l15) if avg_l15 is not None else None
                predictions_df.at[idx, "feat_stat_l3_l15_ratio"] = float(ratio) if ratio is not None else None
                predictions_df.at[idx, "feat_stat_std_l5"] = float(std) if std is not None else None

            # Opponent abbreviation
            opp_id = row.get("opponent_id")
            if opp_id and opp_id in opp_abbrev_map:
                predictions_df.at[idx, "feat_opp_abbrev"] = opp_abbrev_map[opp_id]

        logger.info(f"Mapped insight features for {len(predictions_df)} predictions")
        return predictions_df

    def _get_opponent_abbrevs(self, predictions_df: pd.DataFrame) -> dict[int, str]:
        """Get team abbreviations for opponent_ids using hardcoded map."""
        # NBA team ID to abbreviation map (same as dashboard)
        team_abbrev = {
            1610612737: 'ATL', 1610612738: 'BOS', 1610612751: 'BKN',
            1610612766: 'CHA', 1610612741: 'CHI', 1610612739: 'CLE',
            1610612742: 'DAL', 1610612743: 'DEN', 1610612765: 'DET',
            1610612744: 'GSW', 1610612745: 'HOU', 1610612754: 'IND',
            1610612746: 'LAC', 1610612747: 'LAL', 1610612763: 'MEM',
            1610612748: 'MIA', 1610612749: 'MIL', 1610612750: 'MIN',
            1610612740: 'NOP', 1610612752: 'NYK', 1610612760: 'OKC',
            1610612753: 'ORL', 1610612755: 'PHI', 1610612756: 'PHX',
            1610612757: 'POR', 1610612758: 'SAC', 1610612759: 'SAS',
            1610612761: 'TOR', 1610612762: 'UTA', 1610612764: 'WAS',
        }

        if "opponent_id" not in predictions_df.columns:
            return {}
        opp_ids = predictions_df["opponent_id"].dropna().unique().tolist()
        return {int(tid): team_abbrev.get(int(tid), 'UNK') for tid in opp_ids if tid}

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

        # Fallback 1: CDN schedule (works for scheduled/future games)
        logger.warning("NBA API unavailable, trying CDN schedule...")
        games = self._get_games_from_cdn(target_date)
        if games:
            games = self._enrich_game_times(games, target_date)
            return games

        # Fallback 2: DB query (only works for past dates with box score data)
        logger.warning("CDN schedule also failed, falling back to team_game_stats query")
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

    def _get_games_from_cdn(self, target_date: date) -> list[dict]:
        """Fallback: get games from cdn.nba.com schedule (works when stats.nba.com is blocked)."""
        try:
            import requests

            # NBA team ID mapping (abbreviation -> nba team_id)
            TEAM_ABBREV_TO_ID = {
                "ATL": 1610612737, "BOS": 1610612738, "BKN": 1610612751, "CHA": 1610612766,
                "CHI": 1610612741, "CLE": 1610612739, "DAL": 1610612742, "DEN": 1610612743,
                "DET": 1610612765, "GSW": 1610612744, "HOU": 1610612745, "IND": 1610612754,
                "LAC": 1610612746, "LAL": 1610612747, "MEM": 1610612763, "MIA": 1610612748,
                "MIL": 1610612749, "MIN": 1610612750, "NOP": 1610612740, "NYK": 1610612752,
                "OKC": 1610612760, "ORL": 1610612753, "PHI": 1610612755, "PHX": 1610612756,
                "POR": 1610612757, "SAC": 1610612758, "SAS": 1610612759, "TOR": 1610612761,
                "UTA": 1610612762, "WAS": 1610612764,
            }

            url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            target_str = target_date.strftime("%m/%d/%Y 00:00:00")
            games = []
            for game_date_entry in data.get("leagueSchedule", {}).get("gameDates", []):
                if game_date_entry.get("gameDate") == target_str:
                    for game in game_date_entry.get("games", []):
                        if game.get("weekNumber", 0) > 0:  # Regular season only
                            home_abbrev = game["homeTeam"]["teamTricode"]
                            away_abbrev = game["awayTeam"]["teamTricode"]
                            games.append({
                                "game_id": game["gameId"],
                                "home_team_id": TEAM_ABBREV_TO_ID.get(home_abbrev),
                                "away_team_id": TEAM_ABBREV_TO_ID.get(away_abbrev),
                                "game_time": None,
                            })
                    break

            if games:
                logger.info(f"Found {len(games)} games via CDN schedule")
            return games

        except Exception as e:
            logger.warning(f"CDN schedule failed: {e}")
            return []

    def _get_games_from_db(self, target_date: date) -> list[dict]:
        """Fallback: get games from team_game_stats (only works for past dates)."""
        query = text("""
            SELECT DISTINCT game_id,
                   MAX(CASE WHEN team_matchup LIKE '%vs.%' THEN team_id END) as home_team_id,
                   MAX(CASE WHEN team_matchup LIKE '%@%' THEN team_id END) as away_team_id
            FROM team_game_stats
            WHERE game_date = :target_date
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

        # Games on target_date span two UTC dates:
        #   - Matinee games (noon-6 PM ET) fall on the same UTC date
        #   - Evening games (7 PM+ ET) roll into the next UTC date
        # Search both to catch all games.
        utc_start = target_date
        utc_end = target_date + timedelta(days=2)

        query = text("""
            WITH game_times AS (
                SELECT DISTINCT ON (home_team, away_team)
                    home_team,
                    away_team,
                    commence_time
                FROM raw_game_lines_staging
                WHERE commence_time >= CAST(:utc_start AS date)
                  AND commence_time < CAST(:utc_end AS date)
                  AND CAST(commence_time AT TIME ZONE 'US/Eastern' AS date) = CAST(:target_date AS date)
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
                result = conn.execute(query, {
                    "utc_start": utc_start,
                    "utc_end": utc_end,
                    "target_date": target_date,
                })
                time_lookup = {
                    (row[0], row[1]): row[2]
                    for row in result
                }

            # Log if no times found in database
            if not time_lookup:
                logger.warning(
                    f"No game times found in raw_game_lines_staging for date {target_date}. "
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
        """Remove players listed as 'Out' using rapidapi_injuries.

        Two-pass filtering:
          1. Primary: match by player_id (integer) for linked injuries.
          2. Fallback: match by player name for unlinked injuries (player_id IS NULL)
             to catch the ~0.7% of injuries the linker couldn't resolve.

        Gets each player's MOST RECENT status from the last 7 days, filtering out
        players whose latest status is 'Out'. This handles cases where a player
        is marked Out on day N but not re-listed on day N+1.
        """
        try:
            # Look back 7 days for injury reports
            cutoff_date = target_date - timedelta(days=7)

            with self.engine.connect() as conn:
                # Pass 1: Get Out players by player_id (linked injuries)
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
                result = conn.execute(query, {"target_date": target_date, "cutoff_date": cutoff_date})
                out_player_ids = {row[0] for row in result}

                # Pass 2: Get Out player names from unlinked injuries (player_id IS NULL)
                name_query = text("""
                    WITH recent_unlinked AS (
                        SELECT
                            player,
                            status,
                            ROW_NUMBER() OVER (
                                PARTITION BY LOWER(TRIM(player))
                                ORDER BY report_date DESC
                            ) as rn
                        FROM rapidapi_injuries
                        WHERE report_date >= :cutoff_date
                          AND report_date <= :target_date
                          AND player_id IS NULL
                    )
                    SELECT DISTINCT LOWER(TRIM(player)) as out_name
                    FROM recent_unlinked
                    WHERE rn = 1 AND status = 'Out'
                """)
                result = conn.execute(name_query, {"target_date": target_date, "cutoff_date": cutoff_date})
                out_names = {row[0] for row in result}

            # Match unlinked Out names against our players list
            if out_names:
                name_matched = 0
                for p in players:
                    name = p.get("player_name", "")
                    if name and name.lower().strip() in out_names and p["player_id"] not in out_player_ids:
                        out_player_ids.add(p["player_id"])
                        name_matched += 1
                        logger.info(f"  Name-matched Out player: {name} (ID: {p['player_id']})")
                if name_matched:
                    logger.info(f"Found {name_matched} additional Out players via name matching")

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
                        PARTITION BY player_id, game_id, market_key, bookmaker, outcome_label
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
                MAX(line) as line,
                MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) as over_odds,
                MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) as under_odds
            FROM ranked_lines
            WHERE rn = 1
            GROUP BY player_id, game_id, bookmaker, market_key
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
        best_lines = all_lines.loc[idx].drop(columns=["_raw_over", "_raw_under", "_booksum"])

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

        # Merge (include bookmaker for tracking which book had best line)
        merge_cols = ["player_id", "game_id", "stat", "line", "over_odds", "under_odds"]
        if "bookmaker" in lines_df.columns:
            merge_cols.append("bookmaker")
        merged = predictions_df.merge(
            lines_df[merge_cols],
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

    def _compute_bl_recommendations(
        self,
        predictions_df: pd.DataFrame,
        samples_dict: dict[tuple, np.ndarray] | None = None,
    ) -> pd.DataFrame:
        """Compute Black-Litterman blended probabilities and mark recommended picks.

        Uses the optimal BL config from backtest sweeps (tau=0.5, z_max=1.0)
        to blend model probabilities with market priors. Predictions with
        BL edge >= 9% are marked as recommended ("Model Picks").

        Args:
            predictions_df: DataFrame with raw model predictions and edges.
            samples_dict: MC samples for empirical probability estimation.

        Returns:
            DataFrame with added columns: bl_over_prob, bl_under_prob,
            bl_over_edge, bl_under_edge, bl_confidence, is_recommended.
        """
        # Initialize BL columns
        predictions_df["bl_over_prob"] = None
        predictions_df["bl_under_prob"] = None
        predictions_df["bl_over_edge"] = None
        predictions_df["bl_under_edge"] = None
        predictions_df["bl_confidence"] = None
        predictions_df["is_recommended"] = False

        if samples_dict is None or not samples_dict:
            logger.warning("No MC samples available for BL blending. Skipping BL computation.")
            return predictions_df

        # Initialize BL blender with optimal config
        bl_config = BLConfig(tau=DEFAULT_BL_TAU, z_max=DEFAULT_BL_Z_MAX)
        blender = BlackLittermanBlender(config=bl_config)

        bl_computed = 0
        recommended_count = 0

        for idx, row in predictions_df.iterrows():
            # Skip rows without lines or odds
            if pd.isna(row.get("line")) or pd.isna(row.get("over_odds")) or pd.isna(row.get("under_odds")):
                continue

            # Get MC samples for this prediction
            key = (row["player_id"], row["game_id"], row["stat"])
            samples = samples_dict.get(key)

            if samples is None or len(samples) == 0:
                continue

            # Compute BL-blended prediction
            bl_result = blender.blend_prediction(
                samples=samples,
                line=row["line"],
                over_odds=row["over_odds"],
                under_odds=row["under_odds"],
            )

            # Store BL values
            predictions_df.at[idx, "bl_over_prob"] = bl_result["posterior_over"]
            predictions_df.at[idx, "bl_under_prob"] = bl_result["posterior_under"]
            predictions_df.at[idx, "bl_confidence"] = bl_result["confidence"]

            # Compute BL edges (posterior - implied market)
            implied_over = row.get("implied_over")
            implied_under = row.get("implied_under")

            if pd.notna(implied_over) and pd.notna(implied_under):
                bl_over_edge = bl_result["posterior_over"] - implied_over
                bl_under_edge = bl_result["posterior_under"] - implied_under

                predictions_df.at[idx, "bl_over_edge"] = bl_over_edge
                predictions_df.at[idx, "bl_under_edge"] = bl_under_edge

                # Mark as recommended if max BL edge meets threshold
                max_bl_edge = max(bl_over_edge, bl_under_edge)
                if max_bl_edge >= DEFAULT_BL_EDGE_THRESHOLD:
                    predictions_df.at[idx, "is_recommended"] = True
                    recommended_count += 1

            bl_computed += 1

        logger.info(
            f"Computed BL blending for {bl_computed} predictions, "
            f"{recommended_count} marked as recommended (edge >= {DEFAULT_BL_EDGE_THRESHOLD*100:.0f}%)"
        )

        return predictions_df
