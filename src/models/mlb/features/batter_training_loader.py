"""Compatibility loader shells for batter training feature requests."""

from __future__ import annotations

from dataclasses import dataclass

from src.models.mlb.features.requests import TrainingFeatureRequest


@dataclass(frozen=True)
class BatterTrainingLoader:
    feature_store: object

    def load(self, request: TrainingFeatureRequest):
        return self.feature_store.get_training_dataset(list(request.seasons), stat=request.stat, as_of_time=request.as_of_time)
