"""Compatibility loader shells for batter inference feature requests."""

from __future__ import annotations

from dataclasses import dataclass

from src.models.mlb.features.requests import DateFeatureRequest, PlayerGameFeatureRequest


@dataclass(frozen=True)
class BatterInferenceLoader:
    feature_store: object

    def load_date(self, request: DateFeatureRequest, *, stat: str = "hits"):
        return self.feature_store.get_features_for_date(request.game_date, stat=stat, as_of_time=request.as_of_time)

    def load_player_game(self, request: PlayerGameFeatureRequest, *, stat: str = "hits", opp_pitcher_id: int | None = None, lineup_pos: int | None = None):
        return self.feature_store.get_player_game_features(
            request.player_id,
            request.game_id,
            request.game_date,
            request.team_id,
            request.opp_team_id,
            request.venue_id,
            request.season,
            bool(request.is_home),
            opp_pitcher_id=opp_pitcher_id,
            lineup_pos=lineup_pos,
            stat=stat,
            as_of_time=request.as_of_time,
        )
