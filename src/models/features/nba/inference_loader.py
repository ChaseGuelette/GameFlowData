"""NBA single player-game inference loader."""

from __future__ import annotations

from src.models.features.nba.requests import PlayerGameFeatureRequest

DEPRECATED_COMPAT_FEATURES = {
    "travel_dist": 0,
    "opp_rest_days": 0,
    "opp_travel_dist": 0,
    "opp_is_back_to_back": 0,
}


class InferenceFeatureLoader:
    """Load all features for a single NBA player-game inference request."""

    def __init__(self, feature_store):
        self.feature_store = feature_store

    def load(self, request: PlayerGameFeatureRequest) -> dict | None:
        """Load one player-game feature dict using the FeatureStore compatibility helpers."""
        store = self.feature_store
        with store.engine.connect() as conn:
            ctx = self._load_context(conn, request)
            if ctx is None:
                return None

            player_stats = store._get_player_rolling_stats(conn, request.player_id, request.as_of_date)
            team_stats = store._get_team_rolling_stats(conn, ctx["team_id"], request.as_of_date, is_opponent=False)
            opp_stats = store._get_team_rolling_stats(conn, ctx["opponent_id"], request.as_of_date, is_opponent=True)
            opp_pos_stats = store._get_opponent_positional_stats(
                conn,
                ctx["opponent_id"],
                ctx["position_group"],
                request.as_of_date,
            )
            game_lines = store._get_game_lines(conn, request.game_id)
            prop_lines = store._get_player_prop_lines(conn, request.player_id, request.game_id)
            injury_context = store._get_injury_context(
                conn,
                request.player_id,
                ctx["team_id"],
                ctx["opponent_id"],
                request.as_of_date,
                player_position_group=ctx.get("position_group"),
            )

            raw_spread = game_lines.pop("line_spread_raw", 0)
            game_lines["line_spread"] = -raw_spread if ctx.get("is_home") else raw_spread

            return {
                "player_id": request.player_id,
                "game_id": request.game_id,
                "game_date": request.as_of_date,
                **ctx,
                **player_stats,
                **team_stats,
                **opp_stats,
                **opp_pos_stats,
                **game_lines,
                **prop_lines,
                **injury_context,
                **DEPRECATED_COMPAT_FEATURES,
            }

    def _load_context(self, conn, request: PlayerGameFeatureRequest) -> dict | None:
        if request.is_scheduled_context:
            position_group = self.feature_store._get_player_position(
                conn,
                request.player_id,
                request.as_of_date,
            )
            if position_group is None:
                return None
            return {
                "team_id": request.team_id,
                "opponent_id": request.opponent_id,
                "is_home": request.is_home if request.is_home is not None else True,
                "position_group": position_group,
                "season_id": "22025",
            }

        return self.feature_store._get_context_snapshots(
            conn,
            request.game_id,
            request.player_id,
            request.as_of_date,
        )
