from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from src.models.mlb.lifecycle.config import LifecycleConfig, resolve_lifecycle_config

ROOT = Path(__file__).resolve().parents[1]
HITS_CONFIG = ROOT / "configs/mlb/batter_hits/platoon_contact_independent.yaml"
PITCHER_CONFIG = ROOT / "configs/mlb/pitcher_strikeouts/baseline_independent.yaml"
RBI_CONFIG = ROOT / "configs/mlb/batter_rbis/baseline_independent.yaml"


def _raw(path: Path = HITS_CONFIG) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_sample_configs_resolve_three_profiles() -> None:
    assert resolve_lifecycle_config(HITS_CONFIG).profile_stat == "batter_hits"
    assert resolve_lifecycle_config(PITCHER_CONFIG).profile_stat == "pitcher_strikeouts"
    assert resolve_lifecycle_config(RBI_CONFIG).profile_stat == "batter_rbis"


def test_platoon_contact_expands_to_two_families_and_twenty_features() -> None:
    resolved = resolve_lifecycle_config(HITS_CONFIG)
    assert resolved.feature_controls.requested_families == ["platoon", "contact_quality"]
    assert len(resolved.feature_controls.family_features["platoon"]) == 3
    assert len(resolved.feature_controls.family_features["contact_quality"]) == 17
    assert resolved.feature_controls.resolved_feature_count == 20


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
