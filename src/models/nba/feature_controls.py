"""NBA-local feature-family controls and artifact conformance helpers."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

REGISTRY_VERSION = 1
FEATURE_FAMILIES: dict[str, tuple[str, ...]] = {
    "game_market": ("line_spread", "line_total"),
    "prop_market": ("prop_line_pts", "prop_line_reb", "prop_line_ast", "prop_line_threes"),
    "opponent_matchup": (
        "opp_avg_def_rtg_l5", "opp_avg_fg3a_l5", "opp_avg_fg3_pct_l5",
        "opp_pos_off_rtg_allowed_l5", "opp_pos_off_rtg_allowed_l15",
        "opp_pos_reb_allowed_l5", "opp_pos_reb_per100_allowed_l5", "opp_pos_reb_allowed_l15",
        "opp_pos_ast_allowed_l5", "opp_pos_ast_per100_allowed_l5", "opp_pos_ast_allowed_l15",
        "opp_pos_threes_allowed_l5", "opp_pos_threes_per100_allowed_l5", "opp_pos_threes_allowed_l15",
    ),
    "short_form": (
        "player_avg_pts_l3", "player_avg_reb_l3", "player_avg_ast_l3", "player_avg_fg3m_l3",
        "player_pts_l3_l15_ratio", "player_reb_l3_l15_ratio", "player_ast_l3_l15_ratio",
        "player_fg3m_l3_l15_ratio", "player_std_pts_l5", "player_std_reb_l5",
        "player_std_ast_l5", "player_std_fg3m_l5",
    ),
    "minutes_role": (
        "player_avg_min_l3", "player_min_l3_l5_ratio", "player_min_l3_l15_ratio",
        "player_avg_min_szn", "player_min_std_l5", "player_min_floor_l5",
        "player_games_started_l5", "player_starter_prob",
    ),
    "schedule": ("rest_days", "is_back_to_back", "games_in_last_7_days"),
}

_INJURY_EXACT = {"player_is_questionable", "player_is_probable"}
_PROTECTED_EXACT = {
    "player_id", "game_id", "game_date", "player_name", "team", "opponent", "position",
    "position_group", "commence_time_utc", "injury_cutoff_utc",
    "actual_minutes", "actual_pts", "actual_reb", "actual_ast", "actual_threes",
    "pts_per_min", "reb_per_min", "ast_per_min", "threes_per_min",
}


def is_injury_feature(feature: str) -> bool:
    return feature.startswith(("team_out_", "opp_out_")) or feature in _INJURY_EXACT


def is_protected_feature(feature: str) -> bool:
    return (
        feature in _PROTECTED_EXACT
        or feature.startswith("actual_")
        or feature.endswith(("_id", "_date", "_name"))
        or feature in {"season", "season_id", "start_time", "commence_time"}
    )


def _tokens(values: Iterable[str] | None, label: str) -> tuple[str, ...]:
    result = tuple(values or ())
    if any(type(value) is not str or not value or value != value.strip() for value in result):
        raise ValueError(f"{label} must contain exact nonempty identifiers")
    if len(set(result)) != len(result):
        raise ValueError(f"{label} must not contain duplicates")
    return result


@dataclass(frozen=True)
class FeatureControlSpec:
    include_feature_families: tuple[str, ...] = ()
    exclude_feature_families: tuple[str, ...] = ()
    include_features: tuple[str, ...] = ()
    exclude_features: tuple[str, ...] = ()
    exclude_injury_features: bool = False

    @classmethod
    def create(
        cls, *, include_feature_families: Iterable[str] | None = None,
        exclude_feature_families: Iterable[str] | None = None,
        include_features: Iterable[str] | None = None, exclude_features: Iterable[str] | None = None,
        exclude_injury_features: bool = False,
    ) -> FeatureControlSpec:
        spec = cls(
            _tokens(include_feature_families, "include_feature_families"),
            _tokens(exclude_feature_families, "exclude_feature_families"),
            _tokens(include_features, "include_features"),
            _tokens(exclude_features, "exclude_features"),
            bool(exclude_injury_features),
        )
        spec.validate()
        return spec

    def validate(self) -> None:
        unknown = sorted((set(self.include_feature_families) | set(self.exclude_feature_families)) - set(FEATURE_FAMILIES))
        if unknown:
            raise ValueError(f"Unknown NBA feature families: {', '.join(unknown)}")
        if set(self.include_feature_families) & set(self.exclude_feature_families):
            raise ValueError("Feature families cannot be included and excluded together")
        protected = sorted(
            feature for feature in (*self.include_features, *self.exclude_features)
            if is_protected_feature(feature)
        )
        if protected:
            raise ValueError(f"Protected features cannot be requested: {', '.join(protected)}")
        if self.exclude_injury_features:
            injury = sorted(feature for feature in self.include_features if is_injury_feature(feature))
            if injury:
                raise ValueError(f"Injury features cannot be included while injury exclusion is active: {', '.join(injury)}")
        included, excluded = self.resolved_sides()
        overlap = sorted(set(included) & set(excluded))
        if overlap:
            raise ValueError(f"Feature include/exclude controls conflict: {', '.join(overlap)}")

    @property
    def active(self) -> bool:
        return bool(self.include_feature_families or self.exclude_feature_families or self.include_features or self.exclude_features)

    def resolved_sides(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        def expand(families: tuple[str, ...], exact: tuple[str, ...]) -> tuple[str, ...]:
            result: list[str] = []
            for family in families:
                for feature in FEATURE_FAMILIES[family]:
                    if feature not in result:
                        result.append(feature)
            for feature in exact:
                if feature not in result:
                    result.append(feature)
            return tuple(result)
        return expand(self.include_feature_families, self.include_features), expand(
            self.exclude_feature_families, self.exclude_features
        )

    def requested_receipt(self, *, availability: str = "unresolved_until_data_load") -> dict[str, Any]:
        included, excluded = self.resolved_sides()
        return {
            "registry_version": REGISTRY_VERSION,
            "requested": {
                "include_feature_families": list(self.include_feature_families),
                "exclude_feature_families": list(self.exclude_feature_families),
                "include_features": list(self.include_features),
                "exclude_features": list(self.exclude_features),
                "exclude_injury_features": self.exclude_injury_features,
            },
            "resolved": {"required": list(included), "excluded": list(excluded)},
            "availability": availability,
            "components": {},
        }


def resolve_component(
    spec: FeatureControlSpec, candidates: Iterable[str], numeric_columns: Iterable[str], *, component: str,
) -> tuple[list[str], dict[str, Any]]:
    original = list(candidates)
    if not original:
        raise ValueError(f"{component} has no numeric feature candidates")
    if len(set(original)) != len(original):
        raise ValueError(f"{component} candidate features contain duplicates")
    numeric = set(numeric_columns)
    required, excluded = spec.resolved_sides()
    requested = set(required) | set(excluded)
    protected = sorted(feature for feature in requested if is_protected_feature(feature))
    if protected:
        raise ValueError(f"{component} requested protected features: {', '.join(protected)}")
    missing = sorted(requested - set(original))
    nonnumeric = sorted(requested - numeric)
    if missing or nonnumeric:
        details = []
        if missing:
            details.append("missing=" + ",".join(missing))
        if nonnumeric:
            details.append("non_numeric=" + ",".join(nonnumeric))
        raise ValueError(f"{component} requested feature availability failed ({'; '.join(details)})")
    injury_excluded = [feature for feature in original if spec.exclude_injury_features and is_injury_feature(feature)]
    effective_excluded = set(excluded) | set(injury_excluded)
    filtered = [feature for feature in original if feature not in effective_excluded]
    if not filtered:
        raise ValueError(f"{component} has no candidates after feature controls")
    receipt = {
        "candidates": original,
        "required": list(required),
        "excluded": list(excluded),
        "injury_excluded": injury_excluded,
        "effective_candidates": filtered,
        "effective": {},
    }
    return filtered, receipt


def enforce_selected(selected: Mapping[Any, list[str]], receipt: dict[str, Any], *, component: str) -> dict[Any, list[str]]:
    if not isinstance(selected, Mapping) or not selected:
        raise ValueError(f"{component} selector returned no quantile feature lists")
    required = receipt["required"]
    forbidden = set(receipt["excluded"]) | set(receipt["injury_excluded"])
    allowed = set(receipt["effective_candidates"])
    result: dict[Any, list[str]] = {}
    effective: dict[str, list[str]] = {}
    for quantile, values in selected.items():
        if not isinstance(values, list):
            raise ValueError(f"{component} selector returned a malformed feature list")
        ordered = [feature for feature in values if feature not in forbidden]
        unknown = sorted(set(ordered) - allowed)
        if unknown:
            raise ValueError(f"{component} selector returned non-candidate features: {', '.join(unknown)}")
        for feature in required:
            if feature not in ordered:
                ordered.append(feature)
        if not ordered or len(set(ordered)) != len(ordered):
            raise ValueError(f"{component} effective feature list is malformed")
        result[quantile] = ordered
        effective[str(quantile)] = list(ordered)
    receipt["effective"] = effective
    return result


def validate_feature_control_evidence(
    controls: Any, effective_features: Mapping[str, Any], *, allow_legacy_default: bool = True,
) -> dict[str, Any]:
    if controls is None:
        if allow_legacy_default:
            return FeatureControlSpec.create().requested_receipt(availability="legacy_default")
        raise ValueError("Feature-control evidence is missing")
    if not isinstance(controls, dict) or set(controls) != {
        "registry_version", "requested", "resolved", "availability", "components"
    }:
        raise ValueError("Feature-control evidence is malformed")
    requested = controls["requested"]
    if not isinstance(requested, dict):
        raise ValueError("Feature-control requested settings are malformed")
    spec = FeatureControlSpec.create(
        include_feature_families=requested.get("include_feature_families"),
        exclude_feature_families=requested.get("exclude_feature_families"),
        include_features=requested.get("include_features"), exclude_features=requested.get("exclude_features"),
        exclude_injury_features=requested.get("exclude_injury_features", False),
    )
    expected = spec.requested_receipt(availability=controls["availability"])
    if controls["registry_version"] != REGISTRY_VERSION or controls["resolved"] != expected["resolved"]:
        raise ValueError("Feature-control registry resolution is inconsistent")
    components = controls["components"]
    if not isinstance(components, dict):
        raise ValueError("Feature-control component evidence is malformed")
    if spec.active or spec.exclude_injury_features:
        if controls["availability"] != "resolved_from_training_data":
            raise ValueError("Nondefault feature controls lack resolved data evidence")
        if set(components) != set(effective_features):
            raise ValueError("Feature-control component evidence is incomplete")
    for component, selected in effective_features.items():
        quantiles = selected if isinstance(selected, dict) else {"flat": selected}
        evidence = components.get(component)
        if evidence is None:
            if spec.active or spec.exclude_injury_features:
                raise ValueError(f"Missing feature-control evidence for {component}")
            continue
        if not isinstance(evidence, dict) or set(evidence) != {
            "candidates", "required", "excluded", "injury_excluded", "effective_candidates", "effective"
        }:
            raise ValueError(f"Feature-control component evidence is malformed for {component}")
        candidates = evidence["candidates"]
        if (not isinstance(candidates, list) or not candidates or len(candidates) != len(set(candidates))
                or evidence["required"] != controls["resolved"]["required"]
                or evidence["excluded"] != controls["resolved"]["excluded"]):
            raise ValueError(f"Feature-control resolution differs for {component}")
        if not set(evidence["excluded"]).issubset(candidates):
            raise ValueError(f"Excluded feature evidence is unavailable for {component}")
        expected_injury = [feature for feature in candidates if spec.exclude_injury_features and is_injury_feature(feature)]
        forbidden = set(evidence["excluded"]) | set(expected_injury)
        expected_candidates = [feature for feature in candidates if feature not in forbidden]
        if evidence["injury_excluded"] != expected_injury or evidence["effective_candidates"] != expected_candidates:
            raise ValueError(f"Feature-control candidate evidence differs for {component}")
        if not set(evidence["required"]).issubset(expected_candidates):
            raise ValueError(f"Required feature evidence is unavailable for {component}")
        if evidence.get("effective") != {str(k): v for k, v in quantiles.items()}:
            raise ValueError(f"Feature-control effective evidence differs for {component}")
        required = set(evidence["required"])
        for values in quantiles.values():
            if not required.issubset(values) or forbidden.intersection(values):
                raise ValueError(f"Effective features violate controls for {component}")
            if spec.exclude_injury_features and any(is_injury_feature(value) for value in values):
                raise ValueError(f"Injury features violate controls for {component}")
    return controls
