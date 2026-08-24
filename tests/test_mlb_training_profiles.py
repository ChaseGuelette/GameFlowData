from __future__ import annotations

import pytest

from src.models.mlb.features.contracts import (
    BATTER_FORCE_FEATURE_FAMILIES,
    PITCHER_K_PHASE3A_REJECTED_FEATURES,
)
from src.models.mlb.training.profiles import (
    MLBTrainingProfile,
    get_training_profile,
    list_training_profiles,
)


def test_batter_hits_profile_matches_current_runner_and_training_contracts() -> None:
    profile = get_training_profile("batter_hits")

    assert isinstance(profile, MLBTrainingProfile)
    assert profile.stat_key == "batter_hits"
    assert profile.display_name == "Batter Hits"
    assert profile.train_entrypoint_kind == "batter"
    assert profile.model_type == "binomial"
    assert profile.train_short_stat == "hits"
    assert profile.target_columns == ("actual", "actual_at_bats")
    assert profile.prop_line_feature == "prop_line_batter_hits"
    assert profile.default_direction == "both"
    assert profile.artifact_prefix == "mlb_run_batter_hits"
    assert profile.default_quote_policy == "slate_or_tminus"
    assert profile.default_line_source == "mlb_player_props_clv_snapshots"
    assert profile.default_book_routing_policy == "preferred_book_first"
    assert profile.min_decision_grade_bets == 100
    assert profile.feature_families["market"] == BATTER_FORCE_FEATURE_FAMILIES["market"]


def test_pitcher_strikeouts_profile_locks_rejected_phase3a_features() -> None:
    profile = get_training_profile("pitcher_strikeouts")

    assert profile.stat_key == "pitcher_strikeouts"
    assert profile.display_name == "Pitcher Strikeouts"
    assert profile.train_entrypoint_kind == "pitcher_quantile"
    assert profile.model_type == "quantile"
    assert profile.train_short_stat is None
    assert profile.target_columns == ("actual_so", "actual_ip")
    assert profile.prop_line_feature == "prop_line_pitcher_strikeouts"
    assert profile.default_direction == "under"
    assert profile.artifact_prefix == "mlb_run"
    assert profile.model_artifact_names == ("pitcher_k_model.joblib", "pitcher_k_feature_config.joblib")
    assert set(profile.locked_out_features) == PITCHER_K_PHASE3A_REJECTED_FEATURES
    assert "workload_leash" in profile.feature_families
    assert "team_hook" in profile.feature_families
    assert "opponent_contact" in profile.feature_families
    assert "projected_lineup_k_pct" in profile.locked_out_features


def test_batter_rbis_reuses_generic_batter_profile_contract() -> None:
    profile = get_training_profile("batter_rbis")

    assert profile.train_entrypoint_kind == "batter"
    assert profile.train_short_stat == "rbis"
    assert profile.model_type == "negative_binomial"
    assert profile.model_artifact_names == (
        "batter_rbis_xgblss_booster.json",
        "batter_rbis_negbin_meta.json",
    )
    assert get_training_profile("rbis") is profile


def test_profile_lookup_normalizes_aliases_and_lists_initial_profiles() -> None:
    assert get_training_profile("pitcher-k").stat_key == "pitcher_strikeouts"
    assert get_training_profile("pitcher_k").stat_key == "pitcher_strikeouts"
    assert get_training_profile("hits").stat_key == "batter_hits"
    assert list_training_profiles() == ["batter_hits", "batter_rbis", "pitcher_strikeouts"]


def test_unknown_profile_fails_loudly() -> None:
    with pytest.raises(ValueError, match="Unknown MLB training profile"):
        get_training_profile("unsupported_stat")
