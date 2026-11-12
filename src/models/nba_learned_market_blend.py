"""Pure numerical helpers for frozen per-stat market blending.

Ordinary BL remains confidence dependent::

    weight = min(tau * confidence, max_weight)

This module implements the separate ``learned_market_v1`` policy::

    weight = learned_weight_for_stat[stat]
    p = sigmoid(logit(market) + weight * (logit(empirical) - logit(market)))

There is no tau, z-score, confidence, cap, intercept, or online update here.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
from scipy.optimize import minimize_scalar

MODE = "learned_market_v1"
SCHEMA_VERSION = 1
SUPPORTED_STATS = ("pts", "reb", "ast")
CLIP_MIN = 0.01
CLIP_MAX = 0.99


class LearnedMarketBlendError(ValueError):
    """Raised when learned-blend inputs or artifacts are invalid."""


@dataclass(frozen=True)
class WeightFit:
    """One bounded scalar optimization result."""

    weight: float
    objective: float
    optimizer_success: bool
    optimizer_message: str
    candidates: tuple[tuple[float, float], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "weight": self.weight,
            "objective": self.objective,
            "optimizer_success": self.optimizer_success,
            "optimizer_message": self.optimizer_message,
            "candidates": [
                {"weight": weight, "objective": objective}
                for weight, objective in self.candidates
            ],
        }


def canonical_json(value: Any) -> str:
    """Return stable compact JSON used for content identities."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def content_identity(value: Any) -> str:
    """Hash a JSON-compatible value deterministically."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _probabilities(values: Any, name: str) -> np.ndarray:
    result = np.asarray(values, dtype=float)
    if result.ndim == 0:
        result = result.reshape(1)
    if not np.isfinite(result).all() or ((result < 0.0) | (result > 1.0)).any():
        raise LearnedMarketBlendError(f"{name} must contain finite probabilities in [0, 1]")
    return result


def _weight(value: Any, name: str = "weight") -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise LearnedMarketBlendError(f"{name} must be a finite scalar in [0, 1]") from exc
    if not math.isfinite(result) or not 0.0 <= result <= 1.0:
        raise LearnedMarketBlendError(f"{name} must be a finite scalar in [0, 1]")
    return result


def learned_market_probability(
    empirical_probability: Any,
    market_probability: Any,
    weight: float,
    *,
    clip_min: float = CLIP_MIN,
    clip_max: float = CLIP_MAX,
) -> float | np.ndarray:
    """Blend empirical and market probabilities with one frozen scalar.

    Inputs are clipped only for the logit calculation. Scalar inputs produce a
    scalar; array-like inputs produce a NumPy array. No ordinary-BL parameters
    are accepted by this API.
    """

    empirical = _probabilities(empirical_probability, "empirical_probability")
    market = _probabilities(market_probability, "market_probability")
    try:
        empirical, market = np.broadcast_arrays(empirical, market)
    except ValueError as exc:
        raise LearnedMarketBlendError("Empirical and market probabilities are not broadcast-compatible") from exc
    blend_weight = _weight(weight)
    if not (0.0 < clip_min < clip_max < 1.0):
        raise LearnedMarketBlendError("Probability clipping bounds must satisfy 0 < min < max < 1")
    empirical_clipped = np.clip(empirical, clip_min, clip_max)
    market_clipped = np.clip(market, clip_min, clip_max)
    market_logit = np.log(market_clipped) - np.log1p(-market_clipped)
    empirical_logit = np.log(empirical_clipped) - np.log1p(-empirical_clipped)
    eta = market_logit + blend_weight * (empirical_logit - market_logit)
    result = 1.0 / (1.0 + np.exp(-eta))
    if np.asarray(empirical_probability).ndim == 0 and np.asarray(market_probability).ndim == 0:
        return float(result[0])
    return result


def learned_market_probability_for_stat(
    empirical_probability: Any,
    market_probability: Any,
    stat: str,
    weights: Mapping[str, float],
) -> float | np.ndarray:
    """Apply the validated frozen weight for ``stat``; missing stats fail."""

    if stat not in SUPPORTED_STATS:
        raise LearnedMarketBlendError(f"Unsupported stat {stat!r}; expected one of {SUPPORTED_STATS}")
    if stat not in weights:
        raise LearnedMarketBlendError(f"No learned weight is available for requested stat {stat!r}")
    return learned_market_probability(empirical_probability, market_probability, _weight(weights[stat], stat))


def binary_log_loss_from_eta(eta: np.ndarray, outcomes: np.ndarray) -> float:
    """Mean binary log loss using the numerically stable specified formula."""

    values = np.logaddexp(0.0, eta) - outcomes * eta
    result = float(np.mean(values))
    if not math.isfinite(result):
        raise LearnedMarketBlendError("Log-loss objective is nonfinite")
    return result


def probability_scores(probabilities: Any, outcomes: Any) -> dict[str, float | int]:
    """Return binary Brier score and log loss for a finite matched population."""

    probs = _probabilities(probabilities, "probabilities")
    labels = np.asarray(outcomes, dtype=float)
    if labels.ndim == 0:
        labels = labels.reshape(1)
    if probs.shape != labels.shape or not np.isfinite(labels).all() or not np.isin(labels, [0.0, 1.0]).all():
        raise LearnedMarketBlendError("Outcomes must be finite binary labels matching probabilities")
    clipped = np.clip(probs, CLIP_MIN, CLIP_MAX)
    eta = np.log(clipped) - np.log1p(-clipped)
    return {
        "rows": int(len(probs)),
        "brier": float(np.mean((probs - labels) ** 2)),
        "log_loss": binary_log_loss_from_eta(eta, labels),
    }


def _objective_parts(empirical: Any, market: Any, outcomes: Any) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    empirical_values = _probabilities(empirical, "empirical_probability")
    market_values = _probabilities(market, "market_probability")
    labels = np.asarray(outcomes, dtype=float)
    if labels.ndim == 0:
        labels = labels.reshape(1)
    if empirical_values.shape != market_values.shape or empirical_values.shape != labels.shape or not len(labels):
        raise LearnedMarketBlendError("Fit arrays must be nonempty and have identical shapes")
    if not np.isfinite(labels).all() or not np.isin(labels, [0.0, 1.0]).all():
        raise LearnedMarketBlendError("Fit outcomes must be binary")
    market_clipped = np.clip(market_values, CLIP_MIN, CLIP_MAX)
    empirical_clipped = np.clip(empirical_values, CLIP_MIN, CLIP_MAX)
    market_eta = np.log(market_clipped) - np.log1p(-market_clipped)
    delta = (
        np.log(empirical_clipped)
        - np.log1p(-empirical_clipped)
        - market_eta
    )
    return market_eta, delta, labels


def fit_bounded_weight(
    empirical: Any,
    market: Any,
    outcomes: Any,
    *,
    pooled_weight: float | None = None,
    shrinkage_strength: float = 0.0,
    tie_tolerance: float = 1e-12,
) -> WeightFit:
    """Fit one scalar in [0, 1], explicitly considering both endpoints.

    Equal/flat objective values within ``tie_tolerance`` resolve to the smaller
    model weight. A failed or nonfinite optimizer result is rejected.
    """

    market_eta, delta, labels = _objective_parts(empirical, market, outcomes)
    try:
        shrinkage = float(shrinkage_strength)
    except (TypeError, ValueError) as exc:
        raise LearnedMarketBlendError("shrinkage_strength must be finite and nonnegative") from exc
    if not math.isfinite(shrinkage) or shrinkage < 0.0:
        raise LearnedMarketBlendError("shrinkage_strength must be finite and nonnegative")
    if pooled_weight is None:
        if shrinkage != 0.0:
            raise LearnedMarketBlendError("A pooled weight is required when shrinkage is nonzero")
        pool = 0.0
    else:
        pool = _weight(pooled_weight, "pooled_weight")

    def objective(candidate: float) -> float:
        candidate = _weight(candidate)
        value = binary_log_loss_from_eta(market_eta + candidate * delta, labels)
        if shrinkage:
            value += shrinkage * (candidate - pool) ** 2
        if not math.isfinite(value):
            raise LearnedMarketBlendError("Weight objective is nonfinite")
        return float(value)

    optimized = cast(Any, minimize_scalar(objective, bounds=(0.0, 1.0), method="bounded"))
    if not bool(optimized.success) or not math.isfinite(float(optimized.fun)):
        raise LearnedMarketBlendError(f"Bounded weight optimizer failed: {optimized.message}")
    candidates: list[tuple[float, float]] = [(0.0, objective(0.0)), (1.0, objective(1.0))]
    if math.isfinite(float(optimized.x)):
        point = min(1.0, max(0.0, float(optimized.x)))
        candidates.append((point, objective(point)))
    candidates.sort(key=lambda item: item[0])
    best_value = min(value for _, value in candidates)
    eligible = [item for item in candidates if item[1] <= best_value + tie_tolerance]
    best_weight, objective_value = min(eligible, key=lambda item: item[0])
    if not math.isfinite(best_weight) or not math.isfinite(objective_value):
        raise LearnedMarketBlendError("Weight optimization did not produce a finite solution")
    return WeightFit(
        weight=float(best_weight),
        objective=float(objective_value),
        optimizer_success=bool(optimized.success),
        optimizer_message=str(optimized.message),
        candidates=tuple((float(weight), float(value)) for weight, value in candidates),
    )


def fit_per_stat_weights(
    stats: Sequence[str],
    empirical: Any,
    market: Any,
    outcomes: Any,
    requested_stats: Sequence[str],
    shrinkage_strength: float,
) -> dict[str, Any]:
    """Fit pooled then regularized requested-stat weights."""

    requested = tuple(requested_stats)
    if not requested or len(requested) != len(set(requested)) or any(stat not in SUPPORTED_STATS for stat in requested):
        raise LearnedMarketBlendError(f"requested_stats must be unique choices from {SUPPORTED_STATS}")
    stat_values = np.asarray(stats, dtype=object)
    if not set(stat_values.tolist()) <= set(requested):
        raise LearnedMarketBlendError("Fit rows include unrequested or unsupported stats")
    empirical_values = _probabilities(empirical, "empirical_probability")
    market_values = _probabilities(market, "market_probability")
    label_values = np.asarray(outcomes, dtype=float)
    if not (stat_values.shape == empirical_values.shape == market_values.shape == label_values.shape):
        raise LearnedMarketBlendError("Stat and fit arrays must have identical shapes")
    pooled = fit_bounded_weight(empirical_values, market_values, label_values)
    fits: dict[str, WeightFit] = {}
    counts: dict[str, int] = {}
    for stat in requested:
        mask = stat_values == stat
        count = int(mask.sum())
        counts[stat] = count
        if count == 0:
            raise LearnedMarketBlendError(f"Requested stat {stat!r} has no usable fit rows")
        classes = np.unique(label_values[mask])
        if len(classes) != 2:
            raise LearnedMarketBlendError(
                f"Requested stat {stat!r} has one-class fit outcomes {classes.tolist()} across {count} rows"
            )
        fits[stat] = fit_bounded_weight(
            empirical_values[mask],
            market_values[mask],
            label_values[mask],
            pooled_weight=pooled.weight,
            shrinkage_strength=shrinkage_strength,
        )
    return {
        "mode": MODE,
        "pooled": pooled,
        "per_stat": fits,
        "weights": {stat: fits[stat].weight for stat in requested},
        "counts": counts,
    }


def finalize_artifact(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and add a content-derived immutable artifact identity."""

    artifact = dict(payload)
    artifact.pop("artifact_id", None)
    if artifact.get("schema_version") != SCHEMA_VERSION or artifact.get("mode") != MODE:
        raise LearnedMarketBlendError("Learned blend artifact schema/mode is invalid")
    if artifact.get("bl_config") is not None:
        raise LearnedMarketBlendError("learned_market_v1 requires bl_config=null")
    requested = artifact.get("requested_stats")
    weights = artifact.get("weights")
    if not isinstance(requested, list) or not requested or not isinstance(weights, dict):
        raise LearnedMarketBlendError("Artifact requested_stats/weights are missing")
    if set(requested) != set(weights) or len(requested) != len(set(requested)):
        raise LearnedMarketBlendError("Artifact stats and weights disagree")
    for stat in requested:
        if stat not in SUPPORTED_STATS:
            raise LearnedMarketBlendError(f"Unsupported artifact stat {stat!r}")
        _weight(weights[stat], f"weights[{stat}]")
    clipping = artifact.get("probability_clipping")
    if clipping != {"min": CLIP_MIN, "max": CLIP_MAX}:
        raise LearnedMarketBlendError("Artifact probability clipping contract is invalid")
    artifact["artifact_id"] = content_identity(artifact)
    return artifact


def validate_artifact(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate an artifact and its content identity."""

    artifact = dict(value)
    expected = artifact.pop("artifact_id", None)
    validated = finalize_artifact(artifact)
    if not isinstance(expected, str) or expected != validated["artifact_id"]:
        raise LearnedMarketBlendError("Learned blend artifact identity mismatch")
    return validated


def load_artifact(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LearnedMarketBlendError(f"Cannot read learned blend artifact: {path}") from exc
    if not isinstance(value, dict):
        raise LearnedMarketBlendError("Learned blend artifact must be a JSON object")
    return validate_artifact(value)


def write_artifact(path: Path, artifact: Mapping[str, Any]) -> None:
    """Write a validated artifact to a new path only."""

    validated = validate_artifact(artifact) if "artifact_id" in artifact else finalize_artifact(artifact)
    if path.exists():
        raise FileExistsError(f"Refusing to replace existing artifact: {path}")
    with path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(validated, indent=2, sort_keys=True, allow_nan=False) + "\n")
