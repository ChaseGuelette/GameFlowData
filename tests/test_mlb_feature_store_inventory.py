"""Inventory guards for MLB feature-store boundary migration."""

from __future__ import annotations

import inspect
from pathlib import Path

from src.models.mlb.features.legacy_batter_feature_store import (
    MLBBatterFeatureStore as LegacyMLBBatterFeatureStore,
)
from src.models.mlb.features.legacy_pitcher_feature_store import (
    MLBFeatureStore as LegacyMLBFeatureStore,
)
from src.models.mlb.mlb_batter_feature_store import MLBBatterFeatureStore
from src.models.mlb.mlb_feature_store import MLBFeatureStore

ROOT = Path(__file__).resolve().parents[1]


def _non_comment_loc(path: Path) -> int:
    return sum(1 for line in path.read_text().splitlines() if line.strip() and not line.lstrip().startswith("#"))


def test_legacy_feature_store_public_methods_remain_available():
    for cls in (MLBFeatureStore, MLBBatterFeatureStore):
        for method in ("get_training_dataset", "get_player_game_features", "get_features_for_date", "_get_prop_line"):
            assert hasattr(cls, method), f"{cls.__name__}.{method} missing"


def test_feature_boundary_modules_exist():
    features_dir = ROOT / "src" / "models" / "mlb" / "features"
    for name in [
        "__init__.py",
        "contracts.py",
        "transforms.py",
        "temporal_contracts.py",
        "prop_line_feature_source.py",
    ]:
        assert (features_dir / name).exists()


def test_prop_line_single_row_wrappers_delegate_to_feature_source():
    pitcher_src = inspect.getsource(MLBFeatureStore._get_prop_line)
    batter_src = inspect.getsource(MLBBatterFeatureStore._get_prop_line)
    assert "fetch_single_prop_line" in pitcher_src
    assert "fetch_single_prop_line" in batter_src
    assert "SELECT line" not in pitcher_src
    assert "SELECT line" not in batter_src


def test_prop_line_lateral_sql_is_owned_by_feature_source():
    pitcher_src = inspect.getsource(LegacyMLBFeatureStore)
    batter_src = inspect.getsource(LegacyMLBBatterFeatureStore)
    facade_pitcher_src = inspect.getsource(MLBFeatureStore)
    facade_batter_src = inspect.getsource(MLBBatterFeatureStore)
    assert "build_lateral_prop_line_join" in pitcher_src
    assert "build_lateral_prop_line_join" in batter_src
    assert "FROM mlb_raw_player_props" not in pitcher_src
    assert "FROM mlb_raw_player_props" not in batter_src
    assert "FROM mlb_raw_player_props" not in facade_pitcher_src
    assert "FROM mlb_raw_player_props" not in facade_batter_src


def test_facade_files_are_thin_compatibility_adapters():
    assert _non_comment_loc(ROOT / "src" / "models" / "mlb" / "mlb_feature_store.py") < 600
    assert _non_comment_loc(ROOT / "src" / "models" / "mlb" / "mlb_batter_feature_store.py") < 600


def test_facades_do_not_define_large_source_specific_helpers():
    forbidden_helpers = {
        "_load_single_season_training",
        "_get_pitcher_rolling_stats",
        "_get_pitcher_ip_context_features",
        "_get_team_starter_leash_features",
        "_get_statcast_stats",
        "_get_fangraphs_stats",
        "_get_park_factor",
        "_get_game_weather",
        "_get_team_bullpen_stats",
        "_get_game_total",
        "_get_inning_fatigue_stats",
        "_get_umpire_features",
        "_compute_umpire_features_bulk",
        "_get_batter_rolling_stats",
        "_get_batter_statcast_stats",
        "_get_batter_fangraphs_stats",
        "_get_park_factors",
        "_get_batter_handedness",
        "_get_platoon_features",
        "_get_opposing_bullpen_stats",
    }
    assert forbidden_helpers.isdisjoint(MLBFeatureStore.__dict__)
    assert forbidden_helpers.isdisjoint(MLBBatterFeatureStore.__dict__)


def test_facades_delegate_to_legacy_implementations_not_inline_sql_owners():
    assert MLBFeatureStore.__mro__[1].__module__.endswith("legacy_pitcher_feature_store")
    assert MLBBatterFeatureStore.__mro__[1].__module__.endswith("legacy_batter_feature_store")
