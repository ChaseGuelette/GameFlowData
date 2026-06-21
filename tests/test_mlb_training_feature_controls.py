from __future__ import annotations

import pytest

from src.models.mlb.training.feature_controls import (
    FeatureControlSpec,
    expand_feature_families,
    merge_required_and_selected_features,
    normalize_cli_names,
    resolve_feature_controls,
)
from src.models.mlb.training.profiles import get_training_profile


def test_normalize_cli_names_accepts_commas_hyphens_and_dedupes() -> None:
    assert normalize_cli_names(["contact-quality,matchup_pitcher", "contact_quality"]) == [
        "contact_quality",
        "matchup_pitcher",
    ]


def test_expand_feature_families_uses_profile_registry_order() -> None:
    profile = get_training_profile("batter_hits")

    expanded = expand_feature_families(profile, ["market", "environment"])

    assert expanded[:2] == ["prop_line_batter_hits", "line_total"]
    assert "park_hits_factor" in expanded
    assert len(expanded) == len(set(expanded))


def test_unknown_family_fails_loudly_with_profile_name() -> None:
    profile = get_training_profile("pitcher_strikeouts")

    with pytest.raises(ValueError, match="Unknown feature family.*pitcher_strikeouts"):
        expand_feature_families(profile, ["made_up_family"])


def test_resolve_feature_controls_removes_excluded_and_returns_required_features() -> None:
    profile = get_training_profile("batter_hits")
    spec = FeatureControlSpec(
        force_include_families=("market",),
        force_exclude_families=("contact_quality",),
        force_include_features=("batter_avg_h_l5",),
        force_exclude_features=("batter_barrel_pct_l10",),
    )
    dtypes = {
        "actual": "int64",
        "batter_avg_h_l5": "float64",
        "batter_avg_h_l10": "float64",
        "prop_line_batter_hits": "float64",
        "line_total": "float64",
        "batter_xba_l10": "float64",
        "batter_barrel_pct_l10": "float64",
    }

    candidates, required = resolve_feature_controls(profile, dtypes, spec)

    assert "batter_avg_h_l10" in candidates
    assert "batter_avg_h_l5" not in candidates
    assert "prop_line_batter_hits" not in candidates
    assert "line_total" not in candidates
    assert "batter_barrel_pct_l10" not in candidates
    assert required == ["prop_line_batter_hits", "line_total", "batter_avg_h_l5"]


def test_resolve_feature_controls_detects_conflicts_and_missing_features() -> None:
    profile = get_training_profile("batter_hits")

    with pytest.raises(ValueError, match="both included and excluded"):
        resolve_feature_controls(
            profile,
            {"actual": "int64", "batter_xba_l10": "float64"},
            FeatureControlSpec(
                force_include_features=("batter_xba_l10",),
                force_exclude_features=("batter_xba_l10",),
            ),
        )

    with pytest.raises(ValueError, match="Forced feature.*missing"):
        resolve_feature_controls(
            profile,
            {"actual": "int64", "batter_avg_h_l5": "float64"},
            FeatureControlSpec(force_include_features=("missing_feature",)),
        )


def test_pitcher_profile_family_controls_exclude_locked_out_features_by_default() -> None:
    profile = get_training_profile("pitcher_strikeouts")
    spec = FeatureControlSpec(force_include_families=("workload_leash",))
    dtypes = {
        "actual_so": "int64",
        "pitcher_avg_so_l5": "float64",
        "pitcher_avg_ip_l5": "float64",
        "pitcher_min_ip_l5": "float64",
        "projected_lineup_k_pct": "float64",
    }

    candidates, required = resolve_feature_controls(profile, dtypes, spec)

    assert "pitcher_avg_so_l5" in candidates
    assert "projected_lineup_k_pct" not in candidates
    assert required == ["pitcher_avg_ip_l5", "pitcher_min_ip_l5"]


def test_merge_required_and_selected_features_preserves_order_and_dedupes() -> None:
    assert merge_required_and_selected_features(["b", "a"], ["a", "c"]) == ["b", "a", "c"]
