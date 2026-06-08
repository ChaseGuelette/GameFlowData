from dataclasses import dataclass
from datetime import date

import pandas as pd

from src.models.features.nba.contracts import (
    MINUTES_FEATURES,
    RATE_FEATURES_AST,
    RATE_FEATURES_PTS,
    RATE_FEATURES_REB,
    RATE_FEATURES_THREES,
)
from src.models.features.nba.context_sources import (
    get_context_snapshots,
    get_player_position,
)
from src.models.features.nba.injury_context import (
    get_injury_context,
    load_injury_features_bulk,
    load_player_injury_status_bulk,
)
from src.models.features.nba.line_sources import (
    get_game_lines,
    get_player_prop_lines,
)
from src.models.features.nba.opponent_sources import get_opponent_positional_stats
from src.models.features.nba.player_sources import get_player_rolling_stats
from src.models.features.nba.requests import (
    DateFeatureRequest,
    DateRangeFeatureRequest,
    PlayerGameFeatureRequest,
    TrainingFeatureRequest,
)
from src.models.features.nba.team_sources import get_team_rolling_stats
from src.models.features.nba.date_batch_loader import DateBatchFeatureLoader
from src.models.features.nba.date_range_loader import DateRangeFeatureLoader
from src.models.features.nba.inference_loader import InferenceFeatureLoader
from src.models.features.nba.training_loader import TrainingFeatureLoader

@dataclass
class FeatureConfig:
    """Configuration for feature engineering."""

    min_minutes_for_rate: int = 10
    min_games_l5: int = 3
    excluded_seasons: tuple[str, ...] = ("22019", "22020")



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

    def get_player_game_features(
        self,
        player_id: int,
        game_id: str,
        as_of_date: date,
        team_id: int | None = None,
        opponent_id: int | None = None,
        is_home: bool | None = None,
    ) -> dict | None:
        """
        Get all features for a single player-game (Inference Mode).
        Strictly uses data available BEFORE the game starts.

        For scheduled games (not yet played), pass team_id, opponent_id, is_home
        to avoid querying player_game_stats which won't have the game yet.
        """
        request = PlayerGameFeatureRequest(
            player_id=player_id,
            game_id=game_id,
            as_of_date=as_of_date,
            team_id=team_id,
            opponent_id=opponent_id,
            is_home=is_home,
        )
        return InferenceFeatureLoader(self).load(request)

    def get_features_for_date(self, game_date: date) -> pd.DataFrame:
        """Efficiently fetch features for ALL players on a specific date."""
        request = DateFeatureRequest(game_date=game_date)
        return DateBatchFeatureLoader(self).load(request)

    def _get_game_dates_in_range(self, start_date: date, end_date: date) -> list[date]:
        """Get all distinct game dates in a range."""
        request = DateRangeFeatureRequest(start_date=start_date, end_date=end_date)
        loader = DateRangeFeatureLoader(self)
        return loader._get_game_dates_in_range(request.start_date, request.end_date)

    def get_features_for_date_range(
        self,
        start_date: date,
        end_date: date,
        chunk_size: int = 25,
    ) -> dict[date, pd.DataFrame]:
        """Fetch features for all players across an inclusive date range."""
        request = DateRangeFeatureRequest(start_date=start_date, end_date=end_date)
        return DateRangeFeatureLoader(self).load(request, chunk_size=chunk_size)

    def get_training_dataset(self, seasons: list[str]) -> pd.DataFrame:
        """Build complete training dataset."""
        request = TrainingFeatureRequest(seasons=seasons)
        return TrainingFeatureLoader(self).load(request)

    def _load_single_season_training(self, season: str) -> pd.DataFrame:
        """Load training features for a single season."""
        return TrainingFeatureLoader(self).load_single_season(season)

    def _load_injury_features_bulk(self, game_dates: list) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Pre-aggregate injury features for all (team, date) pairs in one pass.
        Returns:
            - team_agg: DataFrame keyed by (nba_team_id, report_date) with team injury aggregations.
            - per_player: DataFrame with per-injured-player rows including position_group
              for position-matched aggregation downstream.
        """
        return load_injury_features_bulk(self.engine, game_dates)

    def _load_player_injury_status_bulk(self, game_dates: list) -> pd.DataFrame:
        """Load player injury statuses for all (player, date) pairs."""
        return load_player_injury_status_bulk(self.engine, game_dates)

    def _get_player_position(self, conn, player_id: int, as_of_date: date) -> str | None:
        """Get player's position group from position history."""
        return get_player_position(conn, player_id, as_of_date)

    def _get_context_snapshots(self, conn, game_id, player_id, as_of_date):
        return get_context_snapshots(conn, game_id, player_id, as_of_date)

    def _get_player_rolling_stats(self, conn, player_id, as_of_date):
        return get_player_rolling_stats(conn, player_id, as_of_date)

    def _get_team_rolling_stats(self, conn, team_id, as_of_date, is_opponent=False):
        return get_team_rolling_stats(conn, team_id, as_of_date, is_opponent=is_opponent)

    def _get_opponent_positional_stats(self, conn, opponent_id, position_group, as_of_date):
        """Fetch opponent's positional defense stats. Returns 0 if not found."""
        return get_opponent_positional_stats(conn, opponent_id, position_group, as_of_date)

    def _get_game_lines(self, conn, game_id, as_of_date=None):
        """Fetch spread/total from betting lines using only as-of, pregame quotes."""
        return get_game_lines(conn, game_id, as_of_date=as_of_date)

    def _get_player_prop_lines(self, conn, player_id, game_id, as_of_date=None):
        """Fetch per-stat prop lines using only as-of, pregame quotes. Returns 0 if not found."""
        return get_player_prop_lines(conn, player_id, game_id, as_of_date=as_of_date)

    def _get_injury_context(self, conn, player_id, team_id, opponent_id, game_date,
                            player_position_group: str | None = None):
        """Fetch injury context: team/opponent OUT player aggregates + player injury status.

        Args:
            player_position_group: G/W/B position group of the player being predicted.
                If provided, also computes position-matched injury features.
        """
        return get_injury_context(
            conn,
            player_id,
            team_id,
            opponent_id,
            game_date,
            player_position_group=player_position_group,
        )
