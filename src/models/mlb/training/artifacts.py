"""Shared artifact lifecycle and JSON writers for MLB training pipelines."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def build_run_directory(
    base_artifacts_dir: str | Path,
    *,
    prefix: str,
    timestamp: datetime | None = None,
    suffix: str | None = None,
) -> tuple[Path, str]:
    """Create an `_incomplete` run directory and return it plus final name."""
    timestamp = timestamp or datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    parts = [prefix, timestamp_str]
    if suffix:
        parts.append(str(suffix).strip("_"))
    final_name = "_".join(part for part in parts if part)
    run_dir = Path(base_artifacts_dir) / f"{final_name}_incomplete"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir, final_name


def finalize_incomplete_run_directory(run_dir: str | Path, final_name: str) -> Path:
    """Rename an `_incomplete` run directory to its final artifact name."""
    source = Path(run_dir)
    final_dir = source.parent / final_name
    source.rename(final_dir)
    return final_dir


def _json_ready_key(key: Any) -> str:
    return str(key)


def _write_json(run_dir: str | Path, filename: str, payload: Any) -> Path:
    path = Path(run_dir) / filename
    with open(path, "w") as f:
        json.dump(payload, f, indent=4)
    return path


def stringify_mapping_keys(mapping: dict[Any, Any]) -> dict[str, Any]:
    """Return a shallow copy with JSON-stable string keys."""
    return {_json_ready_key(key): value for key, value in mapping.items()}


def write_run_config(run_dir: str | Path, config: dict[str, Any]) -> Path:
    return _write_json(run_dir, "run_config.json", config)


def write_feature_manifest(run_dir: str | Path, selected_features: dict[Any, Any]) -> Path:
    return _write_json(run_dir, "feature_manifest.json", stringify_mapping_keys(selected_features))


def write_feature_experiment_metadata(run_dir: str | Path, metadata: dict[str, Any]) -> Path:
    return _write_json(run_dir, "feature_experiment_metadata.json", metadata)


def write_calibration_report(run_dir: str | Path, reports: dict[str, Any]) -> Path:
    return _write_json(run_dir, "calibration_report_combined.json", reports)


def write_training_metadata(run_dir: str | Path, metadata: dict[str, Any]) -> Path:
    return _write_json(run_dir, "training_metadata.json", metadata)


def write_model_manifest(
    run_dir: str | Path,
    *,
    stat_key: str,
    model_type: str,
    profile_name: str,
    created_at: str,
    git_hash: str | None,
    artifact_files: list[str] | tuple[str, ...],
    compatibility_loader: str,
    schema_version: int = 1,
    feature_manifest_file: str = "feature_manifest.json",
    calibration_report_file: str = "calibration_report_combined.json",
    training_metadata_file: str = "training_metadata.json",
) -> Path:
    manifest = {
        "schema_version": schema_version,
        "stat_key": stat_key,
        "model_type": model_type,
        "profile_name": profile_name,
        "created_at": created_at,
        "git_hash": git_hash,
        "artifact_files": list(artifact_files),
        "feature_manifest_file": feature_manifest_file,
        "calibration_report_file": calibration_report_file,
        "training_metadata_file": training_metadata_file,
        "compatibility_loader": compatibility_loader,
    }
    return _write_json(run_dir, "model_manifest.json", manifest)
