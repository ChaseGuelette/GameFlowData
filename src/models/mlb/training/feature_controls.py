"""Feature-control helpers shared by future MLB training entrypoints."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping

from src.models.mlb.training.profiles import MLBTrainingProfile

_NUMERIC_DTYPES = {"float64", "float32", "int64", "int32"}
_STRUCTURAL_EXCLUDED_FEATURES = {
    "game_id",
    "player_id",
    "game_date",
    "season",
    "team_id",
    "opp_team_id",
    "actual",
    "actual_so",
    "actual_ip",
    "actual_at_bats",
    "player_name",
}


@dataclass(frozen=True)
class FeatureControlSpec:
    force_include_families: tuple[str, ...] = ()
    force_exclude_families: tuple[str, ...] = ()
    force_include_features: tuple[str, ...] = ()
    force_exclude_features: tuple[str, ...] = ()


def normalize_cli_names(names: list[str] | tuple[str, ...] | None) -> list[str]:
    """Normalize comma/repeated CLI names while preserving order."""
    if not names:
        return []

    normalized: list[str] = []
    for raw in names:
        for part in str(raw).split(","):
            name = part.strip().lower().replace("-", "_")
            if name and name not in normalized:
                normalized.append(name)
    return normalized


def normalize_feature_names(names: list[str] | tuple[str, ...] | None) -> list[str]:
    """Normalize comma/repeated exact feature names while preserving spelling."""
    if not names:
        return []

    normalized: list[str] = []
    for raw in names:
        for part in str(raw).split(","):
            name = part.strip()
            if name and name not in normalized:
                normalized.append(name)
    return normalized


def expand_feature_families(profile: MLBTrainingProfile, families: list[str] | tuple[str, ...]) -> list[str]:
    """Expand profile family names to de-duplicated features in registry order."""
    normalized = normalize_cli_names(families)
    unknown = [family for family in normalized if family not in profile.feature_families]
    if unknown:
        valid = ", ".join(sorted(profile.feature_families))
        raise ValueError(
            f"Unknown feature family/families for {profile.stat_key}: {unknown}. Valid: {valid}"
        )

    expanded: list[str] = []
    for family in normalized:
        for feature in profile.feature_families[family]:
            if feature not in expanded:
                expanded.append(feature)
    return expanded


def merge_required_and_selected_features(required: list[str], selected: list[str]) -> list[str]:
    """Put forced/required features first, preserving order and de-duping."""
    merged: list[str] = []
    for feature in [*required, *selected]:
        if feature not in merged:
            merged.append(feature)
    return merged


def _dtype_items(dtypes: Mapping[str, object] | object) -> list[tuple[str, str]]:
    if isinstance(dtypes, Mapping):
        return [(str(column), str(dtype)) for column, dtype in dtypes.items()]
    if hasattr(dtypes, "items"):
        return [(str(column), str(dtype)) for column, dtype in dtypes.items()]
    return []


def _numeric_candidates(
    dtypes: Mapping[str, object] | object,
    *,
    extra_excluded: set[str] | None = None,
) -> list[str]:
    excluded = set(_STRUCTURAL_EXCLUDED_FEATURES)
    if extra_excluded:
        excluded.update(extra_excluded)
    candidates: list[str] = []
    for column, dtype in _dtype_items(dtypes):
        if column in excluded:
            continue
        if dtype in _NUMERIC_DTYPES:
            candidates.append(column)
    return candidates


def _available_numeric_features(dtypes: Mapping[str, object] | object) -> set[str]:
    return {column for column, dtype in _dtype_items(dtypes) if dtype in _NUMERIC_DTYPES}


def resolve_feature_controls(
    profile: MLBTrainingProfile,
    dtypes: Mapping[str, object] | object,
    spec: FeatureControlSpec | None = None,
    *,
    extra_excluded: set[str] | None = None,
    apply_forced_includes: bool = True,
) -> tuple[list[str], list[str]]:
    """Resolve selector candidates and forced-required features for a profile.

    Family includes/excludes are expanded through the profile registry. Family
    features that are not present in the current dataframe are ignored so shared
    profiles can be used across narrower test fixtures or optional feature
    sources. Exact forced features fail loud if missing.
    """
    spec = spec or FeatureControlSpec()
    include_families = tuple(normalize_cli_names(spec.force_include_families))
    exclude_families = tuple(normalize_cli_names(spec.force_exclude_families))
    include_features = tuple(normalize_feature_names(spec.force_include_features))
    exclude_features = tuple(normalize_feature_names(spec.force_exclude_features))

    include_from_families = expand_feature_families(profile, include_families)
    exclude_from_families = expand_feature_families(profile, exclude_families)
    forced_includes = merge_required_and_selected_features(include_from_families, list(include_features))
    forced_excludes = merge_required_and_selected_features(exclude_from_families, list(exclude_features))

    conflicts = sorted(set(forced_includes).intersection(forced_excludes))
    if conflicts:
        raise ValueError(f"Feature(s) both included and excluded for {profile.stat_key}: {conflicts}")

    available_numeric = _available_numeric_features(dtypes)
    missing_exact = [feature for feature in include_features if feature not in available_numeric]
    if missing_exact:
        raise ValueError(f"Forced feature(s) missing or non-numeric for {profile.stat_key}: {missing_exact}")

    excluded = set(profile.locked_out_features).union(forced_excludes)
    if extra_excluded:
        excluded.update(extra_excluded)

    candidates = [feature for feature in _numeric_candidates(dtypes) if feature not in excluded]

    required: list[str] = []
    if apply_forced_includes:
        for feature in forced_includes:
            if feature in excluded:
                continue
            if feature in available_numeric and feature not in required:
                required.append(feature)
        candidates = [feature for feature in candidates if feature not in required]

    return candidates, required
