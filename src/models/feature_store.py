import logging
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

logger = logging.getLogger(__name__)

# ID: (Latitude, Longitude)
# These are approximate stadium locations
TEAM_LOCATIONS = {
    1610612737: (33.7573, -84.3963),  # ATL (State Farm Arena)
    1610612738: (42.3662, -71.0621),  # BOS (TD Garden)
    1610612751: (40.6826, -73.9754),  # BKN (Barclays Center)
    1610612766: (35.2251, -80.8392),  # CHA (Spectrum Center)
    1610612741: (41.8807, -87.6742),  # CHI (United Center)
    1610612739: (41.4965, -81.6881),  # CLE (Rocket Mortgage FieldHouse)
    1610612742: (32.7905, -96.8103),  # DAL (American Airlines Center)
    1610612743: (39.7487, -105.0076),  # DEN (Ball Arena)
    1610612765: (42.3411, -83.0553),  # DET (Little Caesars Arena)
    1610612744: (37.7680, -122.3877),  # GSW (Chase Center)
    1610612745: (29.7508, -95.3621),  # HOU (Toyota Center)
    1610612754: (39.7640, -86.1555),  # IND (Gainbridge Fieldhouse)
    1610612746: (33.9425, -118.4081),  # LAC (Intuit Dome - broadly LA area)
    1610612747: (34.0430, -118.2673),  # LAL (Crypto.com Arena)
    1610612763: (35.1382, -90.0505),  # MEM (FedExForum)
    1610612748: (25.7814, -80.1870),  # MIA (Kaseya Center)
    1610612749: (43.0451, -87.9172),  # MIL (Fiserv Forum)
    1610612750: (44.9795, -93.2761),  # MIN (Target Center)
    1610612740: (29.9490, -90.0821),  # NOP (Smoothie King Center)
    1610612752: (40.7505, -73.9934),  # NYK (Madison Square Garden)
    1610612760: (35.4634, -97.5151),  # OKC (Paycom Center)
    1610612753: (28.5392, -81.3839),  # ORL (Kia Center)
    1610612755: (39.9012, -75.1720),  # PHI (Wells Fargo Center)
    1610612756: (33.4457, -112.0712),  # PHX (Footprint Center)
    1610612757: (45.5316, -122.6668),  # POR (Moda Center)
    1610612758: (38.5802, -121.4997),  # SAC (Golden 1 Center)
    1610612759: (29.4270, -98.4375),  # SAS (Frost Bank Center)
    1610612761: (43.6435, -79.3791),  # TOR (Scotiabank Arena)
    1610612762: (40.7683, -111.9011),  # UTA (Delta Center)
    1610612764: (38.8982, -77.0209),  # WAS (Capital One Arena)
}


@dataclass
class FeatureConfig:
    """Configuration for feature engineering."""

    min_minutes_for_rate: int = 10
    min_games_l5: int = 3
    excluded_seasons: tuple[str, ...] = ("22019", "22020")


# Centralized feature definitions for consistency between training and inference
MINUTES_FEATURES = [
    "player_avg_min_l5",
    "player_avg_min_l15",
    "player_avg_usg_pct_l5",
    "team_avg_pace_l5",
    "opp_avg_pace_l5",
    "line_spread",
    "line_total",
    "is_home",
]

RATE_FEATURES_PTS = [
    "player_avg_pts_l5",
    "player_avg_pts_l15",
    "player_avg_usg_pct_l5",
    "player_avg_ts_pct_l15",
    "team_avg_pace_l5",
    "opp_avg_def_rtg_l5",
    "opp_pos_off_rtg_allowed_l5",
    "opp_pos_off_rtg_allowed_l15",
    "is_home",
]

RATE_FEATURES_REB = [
    "player_avg_reb_l5",
    "player_avg_reb_pct_l5",
    "opp_pos_reb_allowed_l5",
    "opp_pos_reb_per100_allowed_l5",
    "opp_pos_reb_allowed_l15",
    "team_avg_pace_l5",
    "opp_avg_pace_l5",
    "is_home",
]

RATE_FEATURES_AST = [
    "player_avg_ast_l5",
    "player_avg_ast_pct_l5",
    "player_avg_usg_pct_l5",
    "opp_pos_ast_allowed_l5",
    "opp_pos_ast_per100_allowed_l5",
    "opp_pos_ast_allowed_l15",
    "team_avg_pace_l5",
    "is_home",
]

RATE_FEATURES_THREES = [
    "player_avg_fg3m_l5",
    "player_avg_fg3a_l5",
    "opp_pos_threes_allowed_l5",
    "opp_pos_threes_per100_allowed_l5",
    "opp_pos_threes_allowed_l15",
    "team_avg_fg3a_l5",
    "team_avg_fg3_pct_l5",
    "opp_avg_fg3a_l5",
    "opp_avg_fg3_pct_l5",
    "team_avg_pace_l5",
    "is_home",
]


class FeatureStore:
    """
    Central feature engineering class.

    Streamlined feature set:
    - No derived per100 features (use raw stats)
    - Fill missing with 0 (not fake league averages)
    - Opponent stats derived via join on opponent_id
    """

    def __init__(self, engine, config: FeatureConfig | None = None):
        self.engine = engine
        self.config = config or FeatureConfig()

    @staticmethod
    def _haversine(lat1, lon1, lat2, lon2):
        """Vectorized Haversine distance calculation."""
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        c = 2 * np.arcsin(np.sqrt(a))
        return c * 3956  # Miles

    def _get_travel_and_rest_features(self, seasons: list[str]) -> pd.DataFrame:
        """
        Calculate rest days and travel distance for all games in the specified seasons.
        Returns a DataFrame with columns: [game_id, team_id, rest_days, travel_dist, is_back_to_back]
        """
        with self.engine.connect() as conn:
            # Get a simple schedule: Who played where and when?
            query = text("""
                SELECT
                    team_id, game_id, game_date, matchup as team_matchup, season_id,
                    CASE WHEN matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home
                FROM player_game_stats
                WHERE season_id IN :seasons
                GROUP BY team_id, game_id, game_date, matchup, season_id
                ORDER BY team_id, game_date
            """).bindparams(bindparam("seasons", expanding=True))
            df = pd.read_sql(query, conn, params={"seasons": list(seasons)})

        if df.empty:
            return pd.DataFrame(columns=["game_id", "team_id", "rest_days", "travel_dist", "is_back_to_back"])

        # 1. Setup Coordinates
        df["game_date"] = pd.to_datetime(df["game_date"])

        # Get My Location
        team_lat = {k: v[0] for k, v in TEAM_LOCATIONS.items()}
        team_lon = {k: v[1] for k, v in TEAM_LOCATIONS.items()}

        df["my_lat"] = df["team_id"].map(team_lat)
        df["my_lon"] = df["team_id"].map(team_lon)

        # Get Opponent Location (If I am away, I am at opp location. If home, at mine)
        # Self-join to get opponent info (rows where same game_id, diff team_id)
        # We need opponent's team_id to look up their stadium.
        # Since we grouped by team_id above, we have one row per team-game.
        # We can self-merge on game_id.
        df_opp = df[["game_id", "team_id", "my_lat", "my_lon"]].rename(
            columns={"team_id": "opp_id", "my_lat": "opp_lat", "my_lon": "opp_lon"}
        )
        df = df.merge(df_opp, on="game_id")
        df = df[df["team_id"] != df["opp_id"]].copy()  # Remove self-matches

        # Determine Game Location
        # If Home: Location = My Stadium
        # If Away: Location = Opponent Stadium
        df["loc_lat"] = np.where(df["is_home"] == 1, df["my_lat"], df["opp_lat"])
        df["loc_lon"] = np.where(df["is_home"] == 1, df["my_lon"], df["opp_lon"])

        # 2. Sort by Team sequence
        df = df.sort_values(["team_id", "game_date"])

        # 3. Calculate Lags (Previous Game info)
        df["prev_date"] = df.groupby("team_id")["game_date"].shift(1)
        df["prev_lat"] = df.groupby("team_id")["loc_lat"].shift(1)
        df["prev_lon"] = df.groupby("team_id")["loc_lon"].shift(1)

        # Fill First Game of Season (Assume Rest=7, Dist=0)
        # Ideally we'd look at previous season, but for now we assume fresh start.
        df["prev_date"] = df["prev_date"].fillna(df["game_date"] - pd.Timedelta(days=7))
        df["prev_lat"] = df["prev_lat"].fillna(df["my_lat"])  # Start at home
        df["prev_lon"] = df["prev_lon"].fillna(df["my_lon"])

        # 4. Compute Metrics
        df["rest_days"] = (df["game_date"] - df["prev_date"]).dt.days
        # Clip rest days to max 7 to avoid outliers from all-star breaks/season start
        df["rest_days"] = df["rest_days"].clip(0, 7)

        df["travel_dist"] = self._haversine(df["prev_lat"], df["prev_lon"], df["loc_lat"], df["loc_lon"])
        df["is_back_to_back"] = (df["rest_days"] == 1).astype(int)

        return df[["game_id", "team_id", "rest_days", "travel_dist", "is_back_to_back"]]

    def get_player_game_features(self, player_id: int, game_id: str, as_of_date: date) -> dict | None:
        """
        Get all features for a single player-game (Inference Mode).
        Strictly uses data available BEFORE the game starts.
        """
        with self.engine.connect() as conn:
            # 1. Game & Position Context
            ctx = self._get_context_snapshots(conn, game_id, player_id, as_of_date)
            if ctx is None:
                return None

            # 2. Player Stats
            player_stats = self._get_player_rolling_stats(conn, player_id, as_of_date)

            # 3. Team & Opponent Stats
            team_stats = self._get_team_rolling_stats(conn, ctx["team_id"], as_of_date, is_opponent=False)
            opp_stats = self._get_team_rolling_stats(conn, ctx["opponent_id"], as_of_date, is_opponent=True)

            # 4. Opponent Defense vs Position
            opp_pos_stats = self._get_opponent_positional_stats(
                conn, ctx["opponent_id"], ctx["position_group"], as_of_date
            )

            # 5. Betting Lines
            game_lines = self._get_game_lines(conn, game_id)

            # 6. Travel & Rest Features
            team_travel = self._get_travel_features_single(
                conn, ctx["team_id"], as_of_date, ctx["is_home"] == 1, ctx["opponent_id"]
            )
            # For opponent: logic is reversed. If I am home, opponent is away (at my stadium).
            # If I am away (at opp stadium), opponent is home.
            opp_travel_raw = self._get_travel_features_single(
                conn, ctx["opponent_id"], as_of_date, ctx["is_home"] == 0, ctx["team_id"]
            )
            opp_travel = {
                "opp_rest_days": opp_travel_raw["rest_days"],
                "opp_travel_dist": opp_travel_raw["travel_dist"],
                "opp_is_back_to_back": opp_travel_raw["is_back_to_back"],
            }

            return {
                "player_id": player_id,
                "game_id": game_id,
                "game_date": as_of_date,
                **ctx,
                **player_stats,
                **team_stats,
                **opp_stats,
                **opp_pos_stats,
                **game_lines,
                **team_travel,
                **opp_travel,
            }

    def _get_travel_features_single(self, conn, team_id, game_date, is_home, opponent_id):
        """
        Calculate travel/rest features for a single team-game context.
        """
        # 1. Get current game location
        # If home, my lat/lon. If away, opp lat/lon.
        my_lat, my_lon = TEAM_LOCATIONS.get(team_id, (0, 0))
        opp_lat, opp_lon = TEAM_LOCATIONS.get(opponent_id, (0, 0))

        curr_lat = my_lat if is_home else opp_lat
        curr_lon = my_lon if is_home else opp_lon

        # 2. Get previous game info
        query = text("""
            SELECT game_date, team_matchup, opponent_id
            FROM team_game_stats
            WHERE team_id = :team_id AND game_date < :game_date
            ORDER BY game_date DESC LIMIT 1
        """)
        prev_game = conn.execute(query, {"team_id": team_id, "game_date": game_date}).fetchone()

        if not prev_game:
            # First game of season or no history
            return {"rest_days": 7, "travel_dist": 0, "is_back_to_back": 0}

        prev_date = prev_game.game_date
        prev_is_home = "vs." in prev_game.team_matchup
        prev_opp_id = prev_game.opponent_id

        prev_opp_lat, prev_opp_lon = TEAM_LOCATIONS.get(prev_opp_id, (0, 0))

        prev_lat = my_lat if prev_is_home else prev_opp_lat
        prev_lon = my_lon if prev_is_home else prev_opp_lon

        # 3. Calculate metrics
        rest_days = (game_date - prev_date).days
        rest_days = min(rest_days, 7)

        dist = self._haversine(prev_lat, prev_lon, curr_lat, curr_lon)

        return {"rest_days": rest_days, "travel_dist": dist, "is_back_to_back": 1 if rest_days == 1 else 0}

    def get_features_for_date(self, game_date: date) -> pd.DataFrame:
        """
        Efficiently fetch features for ALL players on a specific date in one query.
        Used for backtesting to prevent N+1 query exhaustion.
        """
        query = text("""
            SELECT
                -- Identifiers
                pgs.game_id, pgs.player_id, pgs.game_date::date, pgs.season_id,
                pgs.team_id, tgs.opponent_id,
                p.player_name,

                -- Position
                pos.position_group,

                -- Player Box Score Averages
                COALESCE(p_avg.avg_min_l5, 0) as player_avg_min_l5,
                COALESCE(p_avg.avg_min_l15, 0) as player_avg_min_l15,
                COALESCE(p_avg.avg_pts_l5, 0) as player_avg_pts_l5,
                COALESCE(p_avg.avg_pts_l15, 0) as player_avg_pts_l15,
                COALESCE(p_avg.avg_reb_l5, 0) as player_avg_reb_l5,
                COALESCE(p_avg.avg_ast_l5, 0) as player_avg_ast_l5,
                COALESCE(p_avg.avg_fg3m_l5, 0) as player_avg_fg3m_l5,
                COALESCE(p_avg.avg_fg3a_l5, 0) as player_avg_fg3a_l5,

                -- Player Advanced Averages
                COALESCE(pa_avg.avg_usg_pct_l5, 0) as player_avg_usg_pct_l5,
                COALESCE(pa_avg.avg_ts_pct_l15, 0) as player_avg_ts_pct_l15,
                COALESCE(pa_avg.avg_reb_pct_l5, 0) as player_avg_reb_pct_l5,
                COALESCE(pa_avg.avg_ast_pct_l5, 0) as player_avg_ast_pct_l5,

                -- Team Context
                COALESCE(t_avg.avg_pace_l5, 0) as team_avg_pace_l5,
                COALESCE(t_avg.avg_fg3a_l5, 0) as team_avg_fg3a_l5,
                COALESCE(t_avg.avg_fg3_pct_l5, 0) as team_avg_fg3_pct_l5,

                -- Opponent Context (via opponent_id join)
                COALESCE(opp_avg.avg_def_rtg_l5, 0) as opp_avg_def_rtg_l5,
                COALESCE(opp_avg.avg_pace_l5, 0) as opp_avg_pace_l5,
                COALESCE(opp_avg.avg_fg3a_l5, 0) as opp_avg_fg3a_l5,
                COALESCE(opp_avg.avg_fg3_pct_l5, 0) as opp_avg_fg3_pct_l5,

                -- Opponent Defense vs Position (L5 raw)
                COALESCE(opp_def.off_rtg_allowed_l5, 0) as opp_pos_off_rtg_allowed_l5,
                COALESCE(opp_def.reb_allowed_l5, 0) as opp_pos_reb_allowed_l5,
                COALESCE(opp_def.ast_allowed_l5, 0) as opp_pos_ast_allowed_l5,
                COALESCE(opp_def.threes_allowed_l5, 0) as opp_pos_threes_allowed_l5,

                -- Opponent Defense vs Position (L5 pace-adjusted per 100 possessions)
                COALESCE(opp_def.threes_per100_allowed_l5, 0) as opp_pos_threes_per100_allowed_l5,
                COALESCE(opp_def.reb_per100_allowed_l5, 0) as opp_pos_reb_per100_allowed_l5,
                COALESCE(opp_def.ast_per100_allowed_l5, 0) as opp_pos_ast_per100_allowed_l5,

                -- Opponent Defense vs Position (L15 raw)
                COALESCE(opp_def.off_rtg_allowed_l15, 0) as opp_pos_off_rtg_allowed_l15,
                COALESCE(opp_def.reb_allowed_l15, 0) as opp_pos_reb_allowed_l15,
                COALESCE(opp_def.ast_allowed_l15, 0) as opp_pos_ast_allowed_l15,
                COALESCE(opp_def.threes_allowed_l15, 0) as opp_pos_threes_allowed_l15,

                -- Game Lines
                COALESCE(lines.spread, 0) as line_spread,
                COALESCE(lines.total, 0) as line_total,

                -- Game Context
                CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home

            FROM player_game_stats pgs
            JOIN players p ON pgs.player_id = p.player_id
            JOIN team_game_stats tgs
                ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id

            -- Position
            LEFT JOIN LATERAL (
                SELECT position_group
                FROM player_position_history ph
                WHERE ph.player_id = pgs.player_id
                  AND ph.snapshot_date < pgs.game_date
                ORDER BY ph.snapshot_date DESC LIMIT 1
            ) pos ON TRUE

            -- Player Box Score Averages
            LEFT JOIN LATERAL (
                SELECT avg_min_l5, avg_min_l15, avg_pts_l5, avg_pts_l15,
                       avg_reb_l5, avg_ast_l5, avg_fg3m_l5, avg_fg3a_l5
                FROM player_average_game_stats pags
                WHERE pags.player_id = pgs.player_id
                  AND pags.game_date < pgs.game_date
                ORDER BY pags.game_date DESC LIMIT 1
            ) p_avg ON TRUE

            -- Player Advanced Stats
            LEFT JOIN LATERAL (
                SELECT avg_usg_pct_l5, avg_ts_pct_l15, avg_reb_pct_l5, avg_ast_pct_l5
                FROM player_average_advanced_stats paas
                WHERE paas.player_id = pgs.player_id
                  AND paas.game_date < pgs.game_date
                ORDER BY paas.game_date DESC LIMIT 1
            ) pa_avg ON TRUE

            -- Team Rolling Stats
            LEFT JOIN LATERAL (
                SELECT avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = pgs.team_id
                  AND tags.game_date < pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) t_avg ON TRUE

            -- Opponent Stats (via opponent_id)
            LEFT JOIN LATERAL (
                SELECT avg_def_rtg_l5, avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = tgs.opponent_id
                  AND tags.game_date < pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) opp_avg ON TRUE

            -- Opponent Defense vs Position
            LEFT JOIN LATERAL (
                SELECT
                    off_rtg_allowed_l5, reb_allowed_l5, ast_allowed_l5, threes_allowed_l5,
                    threes_per100_allowed_l5, reb_per100_allowed_l5, ast_per100_allowed_l5,
                    off_rtg_allowed_l15, reb_allowed_l15, ast_allowed_l15, threes_allowed_l15
                FROM team_allowed_by_position tabp
                WHERE tabp.team_id = tgs.opponent_id
                  AND tabp.position_group = pos.position_group
                  AND tabp.game_date < pgs.game_date
                ORDER BY tabp.game_date DESC LIMIT 1
            ) opp_def ON TRUE

            -- Betting Lines
            LEFT JOIN LATERAL (
                SELECT
                    MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                    MAX(CASE WHEN market_key = 'totals' THEN line END) as total
                FROM raw_game_lines_staging
                WHERE nba_game_id = pgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            WHERE pgs.game_date = :game_date
              AND pgs.min >= 5
              AND pos.position_group IS NOT NULL
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date})

        if df.empty:
            return df

        # Fill missing columns that training data has but this query might miss if strict
        # (Though we selected them all)

        # Default Travel & Rest to 0 for now to avoid N+1 complexities in this hotfix.
        cols = [
            "rest_days",
            "travel_dist",
            "is_back_to_back",
            "opp_rest_days",
            "opp_travel_dist",
            "opp_is_back_to_back",
        ]
        for col in cols:
            df[col] = 0.0

        return df

    def _get_game_dates_in_range(self, start_date: date, end_date: date) -> list[date]:
        """Get all distinct game dates in a range."""
        query = text("""
            SELECT DISTINCT game_date
            FROM player_game_stats
            WHERE game_date >= :start_date AND game_date <= :end_date
            ORDER BY game_date
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query, {"start_date": start_date, "end_date": end_date})
            return [row[0] for row in result]

    def get_features_for_date_range(
        self,
        start_date: date,
        end_date: date,
        chunk_size: int = 25,
    ) -> dict[date, pd.DataFrame]:
        """
        Prefetch features for all players across a date range in chunked queries.
        Same LATERAL JOIN query as get_features_for_date() but batched by date chunks
        to avoid Supabase statement timeouts.

        Returns:
            Dict mapping game_date -> DataFrame of features for that date.
        """
        all_dates = self._get_game_dates_in_range(start_date, end_date)
        if not all_dates:
            return {}

        # Chunk dates to keep individual queries under timeout
        chunks = [all_dates[i : i + chunk_size] for i in range(0, len(all_dates), chunk_size)]
        logger.info(f"Fetching features in {len(chunks)} chunks (chunk_size={chunk_size})")

        chunk_dfs = []
        for chunk_idx, chunk_dates in enumerate(chunks):
            chunk_start = chunk_dates[0]
            chunk_end = chunk_dates[-1]

            query = text("""
                SELECT
                    pgs.game_id, pgs.player_id, pgs.game_date::date as game_date, pgs.season_id,
                    pgs.team_id, tgs.opponent_id,
                    p.player_name,
                    pos.position_group,
                    COALESCE(p_avg.avg_min_l5, 0) as player_avg_min_l5,
                    COALESCE(p_avg.avg_min_l15, 0) as player_avg_min_l15,
                    COALESCE(p_avg.avg_pts_l5, 0) as player_avg_pts_l5,
                    COALESCE(p_avg.avg_pts_l15, 0) as player_avg_pts_l15,
                    COALESCE(p_avg.avg_reb_l5, 0) as player_avg_reb_l5,
                    COALESCE(p_avg.avg_ast_l5, 0) as player_avg_ast_l5,
                    COALESCE(p_avg.avg_fg3m_l5, 0) as player_avg_fg3m_l5,
                    COALESCE(p_avg.avg_fg3a_l5, 0) as player_avg_fg3a_l5,
                    COALESCE(pa_avg.avg_usg_pct_l5, 0) as player_avg_usg_pct_l5,
                    COALESCE(pa_avg.avg_ts_pct_l15, 0) as player_avg_ts_pct_l15,
                    COALESCE(pa_avg.avg_reb_pct_l5, 0) as player_avg_reb_pct_l5,
                    COALESCE(pa_avg.avg_ast_pct_l5, 0) as player_avg_ast_pct_l5,
                    COALESCE(t_avg.avg_pace_l5, 0) as team_avg_pace_l5,
                    COALESCE(t_avg.avg_fg3a_l5, 0) as team_avg_fg3a_l5,
                    COALESCE(t_avg.avg_fg3_pct_l5, 0) as team_avg_fg3_pct_l5,
                    COALESCE(opp_avg.avg_def_rtg_l5, 0) as opp_avg_def_rtg_l5,
                    COALESCE(opp_avg.avg_pace_l5, 0) as opp_avg_pace_l5,
                    COALESCE(opp_avg.avg_fg3a_l5, 0) as opp_avg_fg3a_l5,
                    COALESCE(opp_avg.avg_fg3_pct_l5, 0) as opp_avg_fg3_pct_l5,
                    COALESCE(opp_def.off_rtg_allowed_l5, 0) as opp_pos_off_rtg_allowed_l5,
                    COALESCE(opp_def.reb_allowed_l5, 0) as opp_pos_reb_allowed_l5,
                    COALESCE(opp_def.ast_allowed_l5, 0) as opp_pos_ast_allowed_l5,
                    COALESCE(opp_def.threes_allowed_l5, 0) as opp_pos_threes_allowed_l5,
                    COALESCE(opp_def.threes_per100_allowed_l5, 0) as opp_pos_threes_per100_allowed_l5,
                    COALESCE(opp_def.reb_per100_allowed_l5, 0) as opp_pos_reb_per100_allowed_l5,
                    COALESCE(opp_def.ast_per100_allowed_l5, 0) as opp_pos_ast_per100_allowed_l5,
                    COALESCE(opp_def.off_rtg_allowed_l15, 0) as opp_pos_off_rtg_allowed_l15,
                    COALESCE(opp_def.reb_allowed_l15, 0) as opp_pos_reb_allowed_l15,
                    COALESCE(opp_def.ast_allowed_l15, 0) as opp_pos_ast_allowed_l15,
                    COALESCE(opp_def.threes_allowed_l15, 0) as opp_pos_threes_allowed_l15,
                    COALESCE(lines.spread, 0) as line_spread,
                    COALESCE(lines.total, 0) as line_total,
                    CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home
                FROM player_game_stats pgs
                JOIN players p ON pgs.player_id = p.player_id
                JOIN team_game_stats tgs
                    ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
                LEFT JOIN LATERAL (
                    SELECT position_group
                    FROM player_position_history ph
                    WHERE ph.player_id = pgs.player_id
                      AND ph.snapshot_date < pgs.game_date
                    ORDER BY ph.snapshot_date DESC LIMIT 1
                ) pos ON TRUE
                LEFT JOIN LATERAL (
                    SELECT avg_min_l5, avg_min_l15, avg_pts_l5, avg_pts_l15,
                           avg_reb_l5, avg_ast_l5, avg_fg3m_l5, avg_fg3a_l5
                    FROM player_average_game_stats pags
                    WHERE pags.player_id = pgs.player_id
                      AND pags.game_date < pgs.game_date
                    ORDER BY pags.game_date DESC LIMIT 1
                ) p_avg ON TRUE
                LEFT JOIN LATERAL (
                    SELECT avg_usg_pct_l5, avg_ts_pct_l15, avg_reb_pct_l5, avg_ast_pct_l5
                    FROM player_average_advanced_stats paas
                    WHERE paas.player_id = pgs.player_id
                      AND paas.game_date < pgs.game_date
                    ORDER BY paas.game_date DESC LIMIT 1
                ) pa_avg ON TRUE
                LEFT JOIN LATERAL (
                    SELECT avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                    FROM team_average_game_stats tags
                    WHERE tags.team_id = pgs.team_id
                      AND tags.game_date < pgs.game_date
                    ORDER BY tags.game_date DESC LIMIT 1
                ) t_avg ON TRUE
                LEFT JOIN LATERAL (
                    SELECT avg_def_rtg_l5, avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                    FROM team_average_game_stats tags
                    WHERE tags.team_id = tgs.opponent_id
                      AND tags.game_date < pgs.game_date
                    ORDER BY tags.game_date DESC LIMIT 1
                ) opp_avg ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        off_rtg_allowed_l5, reb_allowed_l5, ast_allowed_l5, threes_allowed_l5,
                        threes_per100_allowed_l5, reb_per100_allowed_l5, ast_per100_allowed_l5,
                        off_rtg_allowed_l15, reb_allowed_l15, ast_allowed_l15, threes_allowed_l15
                    FROM team_allowed_by_position tabp
                    WHERE tabp.team_id = tgs.opponent_id
                      AND tabp.position_group = pos.position_group
                      AND tabp.game_date < pgs.game_date
                    ORDER BY tabp.game_date DESC LIMIT 1
                ) opp_def ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                        MAX(CASE WHEN market_key = 'totals' THEN line END) as total
                    FROM raw_game_lines_staging
                    WHERE nba_game_id = pgs.game_id
                      AND bookmaker IN ('pinnacle', 'draftkings')
                ) lines ON TRUE
                WHERE pgs.game_date >= :chunk_start
                  AND pgs.game_date <= :chunk_end
                  AND pgs.min >= 5
                  AND pos.position_group IS NOT NULL
            """)

            try:
                with self.engine.connect() as conn:
                    chunk_df = pd.read_sql(
                        query,
                        conn,
                        params={"chunk_start": chunk_start, "chunk_end": chunk_end},
                    )
                logger.info(
                    f"  Feature chunk {chunk_idx + 1}/{len(chunks)}: "
                    f"{chunk_start} to {chunk_end} ({len(chunk_dates)} dates) -> {len(chunk_df)} rows"
                )
                if not chunk_df.empty:
                    chunk_dfs.append(chunk_df)
            except Exception as e:
                logger.error(f"  Feature chunk {chunk_idx + 1}/{len(chunks)} failed: {e}")
                continue

        if not chunk_dfs:
            return {}

        all_results = pd.concat(chunk_dfs, ignore_index=True)

        # Add hardcoded rest/travel columns (same as get_features_for_date)
        for col in [
            "rest_days",
            "travel_dist",
            "is_back_to_back",
            "opp_rest_days",
            "opp_travel_dist",
            "opp_is_back_to_back",
        ]:
            all_results[col] = 0.0

        # Ensure game_date is a proper date type for groupby
        if hasattr(all_results["game_date"].iloc[0], "date"):
            all_results["game_date"] = all_results["game_date"].apply(lambda x: x.date() if hasattr(x, "date") else x)

        return {game_date: group_df.reset_index(drop=True) for game_date, group_df in all_results.groupby("game_date")}

    def get_training_dataset(self, seasons: list[str]) -> pd.DataFrame:
        """
        Build complete training dataset.
        Streamlined feature set - no derived per100 features.
        """
        print(f"Generating training data for seasons: {seasons}")

        query = text("""
            SELECT
                -- Identifiers
                pgs.game_id, pgs.player_id, pgs.game_date::date, pgs.season_id,
                pgs.team_id, tgs.opponent_id,

                -- Target Variables
                pgs.min as actual_minutes,
                pgs.pts as actual_pts,
                pgs.reb as actual_reb,
                pgs.ast as actual_ast,
                pgs.fg3m as actual_threes,

                -- Position
                pos.position_group,

                -- Player Box Score Averages
                COALESCE(p_avg.avg_min_l5, 0) as player_avg_min_l5,
                COALESCE(p_avg.avg_min_l15, 0) as player_avg_min_l15,
                COALESCE(p_avg.avg_pts_l5, 0) as player_avg_pts_l5,
                COALESCE(p_avg.avg_pts_l15, 0) as player_avg_pts_l15,
                COALESCE(p_avg.avg_reb_l5, 0) as player_avg_reb_l5,
                COALESCE(p_avg.avg_ast_l5, 0) as player_avg_ast_l5,
                COALESCE(p_avg.avg_fg3m_l5, 0) as player_avg_fg3m_l5,
                COALESCE(p_avg.avg_fg3a_l5, 0) as player_avg_fg3a_l5,

                -- Player Advanced Averages
                COALESCE(pa_avg.avg_usg_pct_l5, 0) as player_avg_usg_pct_l5,
                COALESCE(pa_avg.avg_ts_pct_l15, 0) as player_avg_ts_pct_l15,
                COALESCE(pa_avg.avg_reb_pct_l5, 0) as player_avg_reb_pct_l5,
                COALESCE(pa_avg.avg_ast_pct_l5, 0) as player_avg_ast_pct_l5,

                -- Team Context
                COALESCE(t_avg.avg_pace_l5, 0) as team_avg_pace_l5,
                COALESCE(t_avg.avg_fg3a_l5, 0) as team_avg_fg3a_l5,
                COALESCE(t_avg.avg_fg3_pct_l5, 0) as team_avg_fg3_pct_l5,

                -- Opponent Context (via opponent_id join)
                COALESCE(opp_avg.avg_def_rtg_l5, 0) as opp_avg_def_rtg_l5,
                COALESCE(opp_avg.avg_pace_l5, 0) as opp_avg_pace_l5,
                COALESCE(opp_avg.avg_fg3a_l5, 0) as opp_avg_fg3a_l5,
                COALESCE(opp_avg.avg_fg3_pct_l5, 0) as opp_avg_fg3_pct_l5,

                -- Opponent Defense vs Position (L5 raw)
                COALESCE(opp_def.off_rtg_allowed_l5, 0) as opp_pos_off_rtg_allowed_l5,
                COALESCE(opp_def.reb_allowed_l5, 0) as opp_pos_reb_allowed_l5,
                COALESCE(opp_def.ast_allowed_l5, 0) as opp_pos_ast_allowed_l5,
                COALESCE(opp_def.threes_allowed_l5, 0) as opp_pos_threes_allowed_l5,

                -- Opponent Defense vs Position (L5 pace-adjusted per 100 possessions)
                COALESCE(opp_def.threes_per100_allowed_l5, 0) as opp_pos_threes_per100_allowed_l5,
                COALESCE(opp_def.reb_per100_allowed_l5, 0) as opp_pos_reb_per100_allowed_l5,
                COALESCE(opp_def.ast_per100_allowed_l5, 0) as opp_pos_ast_per100_allowed_l5,

                -- Opponent Defense vs Position (L15 raw)
                COALESCE(opp_def.off_rtg_allowed_l15, 0) as opp_pos_off_rtg_allowed_l15,
                COALESCE(opp_def.reb_allowed_l15, 0) as opp_pos_reb_allowed_l15,
                COALESCE(opp_def.ast_allowed_l15, 0) as opp_pos_ast_allowed_l15,
                COALESCE(opp_def.threes_allowed_l15, 0) as opp_pos_threes_allowed_l15,

                -- Game Lines
                COALESCE(lines.spread, 0) as line_spread,
                COALESCE(lines.total, 0) as line_total,

                -- Game Context
                CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home

            FROM player_game_stats pgs
            JOIN team_game_stats tgs
                ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id

            -- Position
            LEFT JOIN LATERAL (
                SELECT position_group
                FROM player_position_history ph
                WHERE ph.player_id = pgs.player_id
                  AND ph.snapshot_date < pgs.game_date
                ORDER BY ph.snapshot_date DESC LIMIT 1
            ) pos ON TRUE

            -- Player Box Score Averages
            LEFT JOIN LATERAL (
                SELECT avg_min_l5, avg_min_l15, avg_pts_l5, avg_pts_l15,
                       avg_reb_l5, avg_ast_l5, avg_fg3m_l5, avg_fg3a_l5
                FROM player_average_game_stats pags
                WHERE pags.player_id = pgs.player_id
                  AND pags.game_date < pgs.game_date
                ORDER BY pags.game_date DESC LIMIT 1
            ) p_avg ON TRUE

            -- Player Advanced Stats
            LEFT JOIN LATERAL (
                SELECT avg_usg_pct_l5, avg_ts_pct_l15, avg_reb_pct_l5, avg_ast_pct_l5
                FROM player_average_advanced_stats paas
                WHERE paas.player_id = pgs.player_id
                  AND paas.game_date < pgs.game_date
                ORDER BY paas.game_date DESC LIMIT 1
            ) pa_avg ON TRUE

            -- Team Rolling Stats
            LEFT JOIN LATERAL (
                SELECT avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = pgs.team_id
                  AND tags.game_date < pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) t_avg ON TRUE

            -- Opponent Stats (via opponent_id)
            LEFT JOIN LATERAL (
                SELECT avg_def_rtg_l5, avg_pace_l5, avg_fg3a_l5, avg_fg3_pct_l5
                FROM team_average_game_stats tags
                WHERE tags.team_id = tgs.opponent_id
                  AND tags.game_date < pgs.game_date
                ORDER BY tags.game_date DESC LIMIT 1
            ) opp_avg ON TRUE

            -- Opponent Defense vs Position
            LEFT JOIN LATERAL (
                SELECT
                    off_rtg_allowed_l5, reb_allowed_l5, ast_allowed_l5, threes_allowed_l5,
                    threes_per100_allowed_l5, reb_per100_allowed_l5, ast_per100_allowed_l5,
                    off_rtg_allowed_l15, reb_allowed_l15, ast_allowed_l15, threes_allowed_l15
                FROM team_allowed_by_position tabp
                WHERE tabp.team_id = tgs.opponent_id
                  AND tabp.position_group = pos.position_group
                  AND tabp.game_date < pgs.game_date
                ORDER BY tabp.game_date DESC LIMIT 1
            ) opp_def ON TRUE

            -- Betting Lines
            LEFT JOIN LATERAL (
                SELECT
                    MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                    MAX(CASE WHEN market_key = 'totals' THEN line END) as total
                FROM raw_game_lines_staging
                WHERE nba_game_id = pgs.game_id
                  AND bookmaker IN ('pinnacle', 'draftkings')
            ) lines ON TRUE

            WHERE pgs.season_id IN :seasons
              AND pgs.season_id NOT IN :excluded
              AND pgs.min > 0
              AND pos.position_group IS NOT NULL
        """).bindparams(
            bindparam("seasons", expanding=True),
            bindparam("excluded", expanding=True),
        )

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={"seasons": list(seasons), "excluded": list(self.config.excluded_seasons)},
            )

        # Calculate Travel & Rest
        travel_df = self._get_travel_and_rest_features(seasons)

        if not travel_df.empty:
            # Merge Team's rest/travel
            df = df.merge(
                travel_df,
                on=["game_id", "team_id"],
                how="left",
            )
            df[["rest_days", "travel_dist", "is_back_to_back"]] = df[
                ["rest_days", "travel_dist", "is_back_to_back"]
            ].fillna(0)

            # Merge Opponent's rest/travel
            opp_travel = travel_df.rename(
                columns={
                    "team_id": "opponent_id",
                    "rest_days": "opp_rest_days",
                    "travel_dist": "opp_travel_dist",
                    "is_back_to_back": "opp_is_back_to_back",
                }
            )
            df = df.merge(
                opp_travel,
                on=["game_id", "opponent_id"],
                how="left",
            )
            df[["opp_rest_days", "opp_travel_dist", "opp_is_back_to_back"]] = df[
                ["opp_rest_days", "opp_travel_dist", "opp_is_back_to_back"]
            ].fillna(0)
        else:
            print("Warning: No travel data found for selected seasons.")
            for col in [
                "rest_days",
                "travel_dist",
                "is_back_to_back",
                "opp_rest_days",
                "opp_travel_dist",
                "opp_is_back_to_back",
            ]:
                df[col] = 0

        # Validation
        print(f"Loaded {len(df):,} rows.")
        if len(df) < 10000:
            raise ValueError(f"Suspiciously few rows: {len(df)}. Check query/season_ids.")

        if not df["position_group"].notna().all():
            raise ValueError("CRITICAL: Position Group has NULLs. Filter logic failed.")

        # Rate targets for training
        mask = df["actual_minutes"] >= self.config.min_minutes_for_rate
        for stat in ["pts", "reb", "ast", "threes"]:
            df.loc[mask, f"{stat}_per_min"] = df.loc[mask, f"actual_{stat}"] / df.loc[mask, "actual_minutes"]

        return df

    def _get_context_snapshots(self, conn, game_id, player_id, as_of_date):
        query = text("""
            SELECT
                pgs.team_id, pgs.season_id, tgs.opponent_id,
                CASE WHEN pgs.matchup LIKE '%vs.%' THEN 1 ELSE 0 END as is_home,
                (SELECT position_group FROM player_position_history ph
                 WHERE ph.player_id = :player_id AND ph.snapshot_date < :as_of_date
                 ORDER BY ph.snapshot_date DESC LIMIT 1) as position_group
            FROM player_game_stats pgs
            JOIN team_game_stats tgs ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
            WHERE pgs.game_id = :game_id AND pgs.player_id = :player_id
        """)
        result = conn.execute(query, {"game_id": game_id, "player_id": player_id, "as_of_date": as_of_date}).fetchone()
        return dict(result._mapping) if result and result.position_group else None

    def _get_player_rolling_stats(self, conn, player_id, as_of_date):
        query = text("""
            SELECT
                pags.avg_min_l5, pags.avg_min_l15,
                pags.avg_pts_l5, pags.avg_pts_l15,
                pags.avg_reb_l5, pags.avg_ast_l5,
                pags.avg_fg3m_l5, pags.avg_fg3a_l5,
                paas.avg_usg_pct_l5, paas.avg_ts_pct_l15,
                paas.avg_reb_pct_l5, paas.avg_ast_pct_l5
            FROM player_average_game_stats pags
            LEFT JOIN player_average_advanced_stats paas
                ON pags.player_id = paas.player_id AND pags.game_id = paas.game_id
            WHERE pags.player_id = :player_id AND pags.game_date < :as_of_date
            ORDER BY pags.game_date DESC LIMIT 1
        """)
        result = conn.execute(query, {"player_id": player_id, "as_of_date": as_of_date}).fetchone()

        if result is None:
            return {
                "player_avg_min_l5": 0,
                "player_avg_min_l15": 0,
                "player_avg_pts_l5": 0,
                "player_avg_pts_l15": 0,
                "player_avg_reb_l5": 0,
                "player_avg_ast_l5": 0,
                "player_avg_fg3m_l5": 0,
                "player_avg_fg3a_l5": 0,
                "player_avg_usg_pct_l5": 0,
                "player_avg_ts_pct_l15": 0,
                "player_avg_reb_pct_l5": 0,
                "player_avg_ast_pct_l5": 0,
            }
        return {f"player_{k}": v or 0 for k, v in result._mapping.items()}

    def _get_team_rolling_stats(self, conn, team_id, as_of_date, is_opponent=False):
        prefix = "opp" if is_opponent else "team"
        query = text("""
            SELECT avg_pace_l5, avg_def_rtg_l5, avg_fg3a_l5, avg_fg3_pct_l5
            FROM team_average_game_stats
            WHERE team_id = :team_id AND game_date < :as_of_date
            ORDER BY game_date DESC LIMIT 1
        """)
        result = conn.execute(query, {"team_id": team_id, "as_of_date": as_of_date}).fetchone()

        if result is None:
            return {
                f"{prefix}_avg_pace_l5": 0,
                f"{prefix}_avg_def_rtg_l5": 0,
                f"{prefix}_avg_fg3a_l5": 0,
                f"{prefix}_avg_fg3_pct_l5": 0,
            }
        return {f"{prefix}_{k}": v or 0 for k, v in result._mapping.items()}

    def _get_opponent_positional_stats(self, conn, opponent_id, position_group, as_of_date):
        """Fetch opponent's positional defense stats. Returns 0 if not found."""
        query = text("""
            SELECT
                off_rtg_allowed_l5, reb_allowed_l5, ast_allowed_l5, threes_allowed_l5,
                threes_per100_allowed_l5, reb_per100_allowed_l5, ast_per100_allowed_l5,
                off_rtg_allowed_l15, reb_allowed_l15, ast_allowed_l15, threes_allowed_l15
            FROM team_allowed_by_position
            WHERE team_id = :opponent_id
              AND position_group = :position_group
              AND game_date < :as_of_date
            ORDER BY game_date DESC LIMIT 1
        """)
        result = conn.execute(
            query,
            {"opponent_id": opponent_id, "position_group": position_group, "as_of_date": as_of_date},
        ).fetchone()

        if result is None:
            return {
                "opp_pos_off_rtg_allowed_l5": 0,
                "opp_pos_reb_allowed_l5": 0,
                "opp_pos_ast_allowed_l5": 0,
                "opp_pos_threes_allowed_l5": 0,
                "opp_pos_threes_per100_allowed_l5": 0,
                "opp_pos_reb_per100_allowed_l5": 0,
                "opp_pos_ast_per100_allowed_l5": 0,
                "opp_pos_off_rtg_allowed_l15": 0,
                "opp_pos_reb_allowed_l15": 0,
                "opp_pos_ast_allowed_l15": 0,
                "opp_pos_threes_allowed_l15": 0,
            }
        return {f"opp_pos_{k}": v or 0 for k, v in result._mapping.items()}

    def _get_game_lines(self, conn, game_id):
        """Fetch spread/total from betting lines. Returns 0 if not found."""
        query = text("""
            SELECT
                MAX(CASE WHEN market_key = 'spreads' THEN line END) as spread,
                MAX(CASE WHEN market_key = 'totals' THEN line END) as total
            FROM raw_game_lines_staging
            WHERE nba_game_id = :game_id
              AND bookmaker IN ('pinnacle', 'draftkings')
        """)
        result = conn.execute(query, {"game_id": game_id}).fetchone()

        return {
            "line_spread": result.spread if result and result.spread else 0,
            "line_total": result.total if result and result.total else 0,
        }
