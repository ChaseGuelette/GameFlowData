"""Shared MLB training helpers for stat-suite migration slices."""

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
from src.models.mlb.training.base_orchestrator import BaseMLBTrainingOrchestrator
from src.models.mlb.training.feature_controls import (
    FeatureControlSpec,
    expand_feature_families,
    merge_required_and_selected_features,
    normalize_cli_names,
    normalize_feature_names,
    resolve_feature_controls,
)
from src.models.mlb.training.profiles import (
    PITCHER_K_FORCE_FEATURE_FAMILIES,
    MLBTrainingProfile,
    get_training_profile,
    list_training_profiles,
)

__all__ = [
    "BaseMLBTrainingOrchestrator",
    "FeatureControlSpec",
    "MLBTrainingProfile",
    "PITCHER_K_FORCE_FEATURE_FAMILIES",
    "build_run_directory",
    "expand_feature_families",
    "finalize_incomplete_run_directory",
    "get_training_profile",
    "list_training_profiles",
    "merge_required_and_selected_features",
    "normalize_cli_names",
    "normalize_feature_names",
    "resolve_feature_controls",
    "write_calibration_report",
    "write_feature_experiment_metadata",
    "write_feature_manifest",
    "write_model_manifest",
    "write_run_config",
    "write_training_metadata",
]
