"""Compatibility loader shells for pitcher training feature requests."""

from __future__ import annotations

from dataclasses import dataclass

from src.models.mlb.features.requests import TrainingFeatureRequest


@dataclass(frozen=True)
class PitcherTrainingLoader:
    feature_store: object

    def load(self, request: TrainingFeatureRequest):
        return self.feature_store.get_training_dataset(list(request.seasons))
