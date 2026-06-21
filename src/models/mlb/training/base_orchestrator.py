"""Shared lifecycle scaffold for MLB training orchestrators.

This module intentionally owns only artifact lifecycle and common JSON writer
plumbing. Stat-specific data loading, feature selection, model objectives,
calibration, and promotion gates stay in the concrete pitcher/batter
orchestrators.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from src.models.mlb.training.artifacts import (
    build_run_directory,
    finalize_incomplete_run_directory,
    write_calibration_report,
    write_feature_experiment_metadata,
    write_feature_manifest,
    write_model_manifest,
    write_run_config,
    write_training_metadata,
)


class BaseMLBTrainingOrchestrator:
    """Shared artifact lifecycle for MLB stat-suite training orchestrators."""

    def __init__(
        self,
        *,
        base_artifacts_dir: str | Path,
        artifact_prefix: str,
        timestamp: datetime | None = None,
        artifact_suffix: str | None = None,
    ) -> None:
        self.timestamp = timestamp or datetime.now()
        self.run_dir, self._final_run_dir_name = build_run_directory(
            base_artifacts_dir,
            prefix=artifact_prefix,
            timestamp=self.timestamp,
            suffix=artifact_suffix,
        )

    def _finalize_run_directory(self) -> Path:
        """Finalize the current `_incomplete` artifact directory."""
        final_dir = finalize_incomplete_run_directory(self.run_dir, self._final_run_dir_name)
        self.run_dir = final_dir
        return final_dir

    def _write_run_config(self, config: dict[str, Any]) -> Path:
        return write_run_config(self.run_dir, config)

    def _write_feature_manifest(self, selected_features: dict[Any, Any]) -> Path:
        return write_feature_manifest(self.run_dir, selected_features)

    def _write_feature_experiment_metadata(self, metadata: dict[str, Any]) -> Path:
        return write_feature_experiment_metadata(self.run_dir, metadata)

    def _write_calibration_report(self, reports: dict[str, Any]) -> Path:
        return write_calibration_report(self.run_dir, reports)

    def _write_training_metadata(self, metadata: dict[str, Any]) -> Path:
        return write_training_metadata(self.run_dir, metadata)

    def _write_model_manifest(
        self,
        *,
        stat_key: str,
        model_type: str,
        profile_name: str,
        git_hash: str | None,
        artifact_files: list[str] | tuple[str, ...],
        compatibility_loader: str = "src.models.mlb.mlb_model_suite.MLBModelSuite",
    ) -> Path:
        return write_model_manifest(
            self.run_dir,
            stat_key=stat_key,
            model_type=model_type,
            profile_name=profile_name,
            created_at=self.timestamp.isoformat(),
            git_hash=git_hash,
            artifact_files=artifact_files,
            compatibility_loader=compatibility_loader,
        )
