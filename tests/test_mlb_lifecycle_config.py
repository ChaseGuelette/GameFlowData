from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from src.models.mlb.lifecycle.config import LifecycleConfig, resolve_lifecycle_config
from src.models.mlb.training.profiles import get_training_profile, list_training_profiles

ROOT = Path(__file__).resolve().parents[1]
HITS_CONFIG = ROOT / "configs/mlb/batter_hits/platoon_contact_independent.yaml"
HITS_END_TO_END_CONFIG = ROOT / "configs/mlb/batter_hits/platoon_contact_end_to_end.yaml"
PITCHER_CONFIG = ROOT / "configs/mlb/pitcher_strikeouts/baseline_independent.yaml"
RBI_CONFIG = ROOT / "configs/mlb/batter_rbis/baseline_independent.yaml"
EXAMPLE_DIR = ROOT / "configs/mlb/examples"
RESUME_EXAMPLE = EXAMPLE_DIR / "resume_existing.yaml"
START_FROM_SCRATCH_EXAMPLE = EXAMPLE_DIR / "start_from_scratch.yaml"
FEATURE_FAMILIES_GUIDE = EXAMPLE_DIR / "FEATURE_FAMILIES.md"


def _raw(path: Path = HITS_CONFIG) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_sample_configs_resolve_three_profiles() -> None:
    assert resolve_lifecycle_config(HITS_CONFIG).profile_stat == "batter_hits"
    assert resolve_lifecycle_config(PITCHER_CONFIG).profile_stat == "pitcher_strikeouts"
    assert resolve_lifecycle_config(RBI_CONFIG).profile_stat == "batter_rbis"


def test_example_configs_cover_resume_and_start_from_scratch_modes() -> None:
    resume_raw = _raw(RESUME_EXAMPLE)
    resume = resolve_lifecycle_config(RESUME_EXAMPLE)
    assert resume.profile_stat == "batter_hits"
    assert resume_raw["model"]["artifact_dir"]
    assert resume_raw["model"]["sweep_dir"]
    assert resume_raw["model"]["sweep_artifact_identity_sha256"]

    scratch_raw = _raw(START_FROM_SCRATCH_EXAMPLE)
    scratch = resolve_lifecycle_config(START_FROM_SCRATCH_EXAMPLE)
    assert scratch.profile_stat == "batter_hits"
    assert scratch.purpose == "discovery"
    assert scratch_raw["model"].get("artifact_dir") is None
    assert scratch_raw["model"].get("sweep_dir") is None
    assert scratch_raw["model"].get("sweep_artifact_identity_sha256") is None


def test_feature_family_guide_lists_every_registered_profile_family_and_feature() -> None:
    guide = FEATURE_FAMILIES_GUIDE.read_text(encoding="utf-8")
    for profile_name in list_training_profiles():
        profile = get_training_profile(profile_name)
        assert f"## `{profile_name}`" in guide
        for family_name, features in profile.feature_families.items():
            assert f"### `{family_name}`" in guide
            for feature in features:
                assert f"`{feature}`" in guide


def test_platoon_contact_expands_to_two_families_and_twenty_features() -> None:
    resolved = resolve_lifecycle_config(HITS_CONFIG)
    assert resolved.feature_controls.requested_families == ["platoon", "contact_quality"]
    assert len(resolved.feature_controls.family_features["platoon"]) == 3
    assert len(resolved.feature_controls.family_features["contact_quality"]) == 17
    assert resolved.feature_controls.resolved_feature_count == 20


def test_batter_hits_end_to_end_config_is_no_attach_full_lifecycle() -> None:
    raw = _raw(HITS_END_TO_END_CONFIG)
    assert raw["model"].get("artifact_dir") is None
    assert raw["model"].get("sweep_dir") is None
    assert raw["model"].get("sweep_artifact_identity_sha256") is None

    resolved = resolve_lifecycle_config(HITS_END_TO_END_CONFIG)

    assert resolved.profile_stat == "batter_hits"
    assert resolved.purpose == "finalist_certification"
    assert resolved.model.base == "no_prop_line"
    assert resolved.feature_controls.requested_families == ["platoon", "contact_quality"]
    assert resolved.training.seasons == [2024, 2025]
    assert str(resolved.training.calibration_end) == "2026-04-12"
    assert str(resolved.evaluation.start) == "2026-05-18"
    assert str(resolved.evaluation.end) == "2026-06-21"
    assert resolved.quotes.clean is True
    assert resolved.quotes.line_source == "mlb_player_props_clv_snapshots"
    assert resolved.quotes.decision_policy == "slate_or_tminus"
    assert resolved.quotes.relative_minutes == 60
    assert resolved.quotes.routing == "preferred_book_first"
    assert resolved.audit.mode == "full"
    assert resolved.audit.minimum_bets == 100
    assert resolved.audit.bootstrap_samples == 1000
    assert resolved.evaluation.flat_bet == 100
    assert resolved.evaluation.kelly_values == [0.0]
    assert resolved.decision.require_independent_window is True


def test_audit_selection_policy_is_purpose_gated_and_explicit_for_certification() -> None:
    discovery = _raw(START_FROM_SCRATCH_EXAMPLE)
    discovery["audit"]["selection"] = {
        "policy": "risk_filtered_top_n",
        "max_configs": 3,
        "include_no_bl_control": True,
        "rank_by": "sharpe_ratio",
        "configs": [],
    }
    assert LifecycleConfig.model_validate(discovery).audit.selection.policy == "risk_filtered_top_n"

    certification = _raw(HITS_CONFIG)
    certification["audit"].pop("selection", None)
    with pytest.raises(ValidationError, match="requires audit.selection.policy='explicit'"):
        LifecycleConfig.model_validate(certification)

    discovery["audit"]["selection"]["policy"] = "explicit"
    discovery["audit"]["selection"]["configs"] = []
    with pytest.raises(ValidationError, match="explicit selectors must be nonempty"):
        LifecycleConfig.model_validate(discovery)


def test_explicit_audit_selectors_reject_duplicates_and_over_cap() -> None:
    raw = _raw(HITS_CONFIG)
    selector = {
        "tau": None,
        "z_max": 0.25,
        "max_weight": 0.5,
        "edge_threshold": 0.12,
        "kelly_fraction": 0.0,
    }
    raw["audit"]["selection"] = {
        "policy": "explicit",
        "max_configs": 1,
        "configs": [selector, deepcopy(selector)],
    }
    with pytest.raises(ValidationError, match="Duplicate explicit audit selector"):
        LifecycleConfig.model_validate(raw)

    raw["audit"]["selection"]["configs"][1]["edge_threshold"] = 0.15
    with pytest.raises(ValidationError, match="cannot exceed max_configs"):
        LifecycleConfig.model_validate(raw)


def test_unknown_profile_and_family_fail_loudly() -> None:
    raw = _raw()
    raw["experiment"]["profile"] = "unknown_stat"
    with pytest.raises(ValidationError, match="Unknown MLB training profile"):
        LifecycleConfig.model_validate(raw)

    raw = _raw()
    raw["model"]["feature_controls"]["families"] = ["not_a_family"]
    with pytest.raises(ValueError, match="Unknown feature family"):
        resolve_lifecycle_config(LifecycleConfig.model_validate(raw))


def test_temporal_and_quote_clean_promotion_guards() -> None:
    raw = _raw()
    raw["evaluation"]["start"] = raw["training"]["calibration_end"]
    with pytest.raises(ValueError, match="must be after"):
        resolve_lifecycle_config(LifecycleConfig.model_validate(raw))

    raw = _raw()
    raw["quotes"]["clean"] = False
    with pytest.raises(ValueError, match="quotes.clean=true"):
        resolve_lifecycle_config(LifecycleConfig.model_validate(raw))


def test_incomplete_artifact_is_rejected() -> None:
    raw = deepcopy(_raw())
    raw["model"]["artifact_dir"] = "tmp/model_incomplete"
    with pytest.raises(ValueError, match="incomplete artifact"):
        resolve_lifecycle_config(LifecycleConfig.model_validate(raw))


def test_variant_is_profile_specific_and_validated_before_execution() -> None:
    batter = _raw()
    batter["model"]["variant"] = "hook_only"
    with pytest.raises(ValidationError, match="variant must be null for batter"):
        LifecycleConfig.model_validate(batter)

    pitcher = _raw(PITCHER_CONFIG)
    pitcher["model"]["variant"] = "not_registered"
    with pytest.raises(ValidationError, match="Unsupported pitcher_strikeouts variant"):
        LifecycleConfig.model_validate(pitcher)
