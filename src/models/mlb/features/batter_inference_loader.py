"""Compatibility loader shells for batter inference feature requests."""

from __future__ import annotations

from dataclasses import dataclass

from src.models.mlb.features.requests import DateFeatureRequest, PlayerGameFeatureRequest


@dataclass(frozen=True)
class BatterInferenceLoader:
    feature_store: object

    def load_date(self, request: DateFeatureRequest, *, stat: str = "hits", matchup_cache=None):
        return self.feature_store.get_features_for_date(
            request.game_date,
            stat=stat,
            matchup_cache=matchup_cache,
            as_of_time=request.as_of_time,
        )

    def load_player_game(self, request: PlayerGameFeatureRequest, *, stat: str = "hits", opp_pitcher_id: int | None = None, lineup_pos: int | None = None):
        return self.feature_store.get_player_game_features(
            player_id=request.player_id,
            game_id=request.game_id,
            game_date=request.game_date,
            team_id=request.team_id,
            opp_team_id=request.opp_team_id,
            venue_id=request.venue_id,
            season=request.season,
            is_home=bool(request.is_home),
            opp_pitcher_id=opp_pitcher_id,
            lineup_pos=lineup_pos,
            stat=stat,
            as_of_time=request.as_of_time,
        )
