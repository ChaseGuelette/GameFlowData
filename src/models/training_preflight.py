"""Filesystem-only validation helpers for NBA training and replay CLIs."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

SUPPORTED_MODEL_STATS = ("pts", "reb", "ast")
_MODEL_CONFIG_KEYS = {
    "quantiles",
    "n_estimators",
    "max_depth",
    "learning_rate",
    "subsample",
    "colsample_bytree",
    "min_child_weight",
    "early_stopping_rounds",
    "val_fraction",
    "random_state",
}
_INTEGER_CONFIG_KEYS = {
    "n_estimators",
    "max_depth",
    "min_child_weight",
    "early_stopping_rounds",
    "random_state",
}
_POSITIVE_CONFIG_KEYS = {
    "n_estimators",
    "max_depth",
    "learning_rate",
    "subsample",
    "colsample_bytree",
    "min_child_weight",
    "early_stopping_rounds",
    "val_fraction",
}


def absolute_path(value: str | Path) -> Path:
    """Resolve a user path from the current working directory without creating it."""
    return Path(value).expanduser().resolve(strict=False)


def validate_output_path(value: str | Path) -> Path:
    """Reject observably unusable output paths without probing or creating them."""
    path = absolute_path(value)
    if path.exists() and not path.is_dir():
        raise ValueError(f"Output target exists and is not a directory: {path}")

    parent = path if path.exists() else path.parent
    while not parent.exists() and parent != parent.parent:
        parent = parent.parent
    if not parent.is_dir():
        raise ValueError(f"Nearest existing output parent is not a directory: {parent}")
    if not os.access(parent, os.W_OK):
        raise ValueError(f"Nearest existing output parent is not currently writable: {parent}")
    return path


def validate_existing_directory(value: str | Path, label: str) -> Path:
    path = absolute_path(value)
    if not path.exists():
        raise ValueError(f"{label} does not exist: {path}")
    if not path.is_dir():
        raise ValueError(f"{label} must be a directory: {path}")
    return path


def validate_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def validate_finite_number(
    value: Any,
    label: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    strict_minimum: bool = False,
) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{label} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{label} must be finite")
    if minimum is not None and (number <= minimum if strict_minimum else number < minimum):
        qualifier = "greater than" if strict_minimum else "at least"
        raise ValueError(f"{label} must be {qualifier} {minimum}")
    if maximum is not None and number > maximum:
        raise ValueError(f"{label} must be at most {maximum}")
    return number


def _validate_model_config(config: Any, label: str) -> None:
    if not isinstance(config, dict):
        raise ValueError(f"{label} must be a JSON object")
    unknown = sorted(set(config) - _MODEL_CONFIG_KEYS)
    if unknown:
        raise ValueError(f"{label} has unsupported keys: {', '.join(unknown)}")

    for key, value in config.items():
        item_label = f"{label}.{key}"
        if key == "quantiles":
            if not isinstance(value, list | tuple) or not value:
                raise ValueError(f"{item_label} must be a non-empty numeric list")
            for index, quantile in enumerate(value):
                validate_finite_number(
                    quantile,
                    f"{item_label}[{index}]",
                    minimum=0.0,
                    maximum=1.0,
                    strict_minimum=True,
                )
                if quantile >= 1.0:
                    raise ValueError(f"{item_label}[{index}] must be less than 1")
            if len(set(value)) != len(value):
                raise ValueError(f"{item_label} must not contain duplicates")
            continue
        if key in _INTEGER_CONFIG_KEYS:
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{item_label} must be an integer")
        validate_finite_number(
            value,
            item_label,
            minimum=0.0 if key in _POSITIVE_CONFIG_KEYS else None,
            strict_minimum=key in _POSITIVE_CONFIG_KEYS,
        )
        if key in {"subsample", "colsample_bytree"} and value > 1.0:
            raise ValueError(f"{item_label} must be at most 1")
        if key == "val_fraction" and value >= 1.0:
            raise ValueError(f"{item_label} must be less than 1")


def validate_hyperparams_file(value: str | Path) -> tuple[Path, dict[str, Any]]:
    path = absolute_path(value)
    if not path.exists() or not path.is_file():
        raise ValueError(f"Hyperparameter JSON file not found: {path}")
    try:
        with path.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Hyperparameter JSON is not readable valid JSON: {path}") from exc
    if not isinstance(data, dict):
        raise ValueError("Hyperparameter JSON root must be an object")

    allowed_models = {"minutes", *SUPPORTED_MODEL_STATS}
    unknown_models = sorted(set(data) - allowed_models)
    if unknown_models:
        raise ValueError(f"Hyperparameter JSON has unsupported model keys: {', '.join(unknown_models)}")
    for model, config in data.items():
        if not isinstance(config, dict):
            raise ValueError(f"Hyperparameters for {model} must be a JSON object")
        _validate_model_config(config, model)
    return path, data


def validate_replay_model_dir(
    value: str | Path,
    stats: list[str],
    *,
    allow_legacy: bool = False,
) -> Path:
    """Validate a replay bundle without deserializing it."""
    from src.models.nba_artifacts import validate_artifact_bundle

    return validate_artifact_bundle(value, stats, allow_legacy=allow_legacy).path
