"""Inventory guards for MLB feature-store boundary migration."""

from __future__ import annotations

import inspect
from pathlib import Path

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
    pitcher_src = inspect.getsource(MLBFeatureStore)
    batter_src = inspect.getsource(MLBBatterFeatureStore)
    assert "build_lateral_prop_line_join" in pitcher_src
    assert "build_lateral_prop_line_join" in batter_src
    assert "FROM mlb_raw_player_props" not in pitcher_src
    assert "FROM mlb_raw_player_props" not in batter_src


def test_facade_shrink_thresholds_documented_not_enforced_yet():
    # Full source/facade thinning is a later Lane 02 safety boundary. Keep this
    # guard visible so progress can tighten it when phases 4-9 are complete.
    assert _non_comment_loc(ROOT / "src" / "models" / "mlb" / "mlb_feature_store.py") > 600
    assert _non_comment_loc(ROOT / "src" / "models" / "mlb" / "mlb_batter_feature_store.py") > 600
