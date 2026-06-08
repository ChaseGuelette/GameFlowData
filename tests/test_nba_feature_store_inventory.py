"""Inventory guards for NBA FeatureStore boundary migration."""

from __future__ import annotations

import inspect
from pathlib import Path

from src.models.feature_store import FeatureStore

ROOT = Path(__file__).resolve().parents[1]


def _non_comment_loc(path: Path) -> int:
    return sum(1 for line in path.read_text().splitlines() if line.strip() and not line.lstrip().startswith("#"))


def test_nba_feature_store_public_methods_remain_available():
    for method in (
        "get_player_game_features",
        "get_features_for_date",
        "get_features_for_date_range",
        "get_training_dataset",
        "_get_game_lines",
        "_get_player_prop_lines",
        "_get_injury_context",
        "_get_player_position",
        "_get_context_snapshots",
        "_get_player_rolling_stats",
        "_get_team_rolling_stats",
        "_get_opponent_positional_stats",
    ):
        assert hasattr(FeatureStore, method), f"FeatureStore.{method} missing"


def test_nba_feature_boundary_modules_exist_for_contract_slice():
    features_dir = ROOT / "src" / "models" / "features" / "nba"
    assert (features_dir / "__init__.py").exists()
    assert (features_dir / "contracts.py").exists()
    assert (features_dir / "transforms.py").exists()
    assert (features_dir / "line_sources.py").exists()
    assert (features_dir / "injury_context.py").exists()
    assert (features_dir / "context_sources.py").exists()
    assert (features_dir / "player_sources.py").exists()
    assert (features_dir / "team_sources.py").exists()
    assert (features_dir / "opponent_sources.py").exists()
    assert (features_dir / "requests.py").exists()
    assert (features_dir / "inference_loader.py").exists()
    assert (features_dir / "date_batch_loader.py").exists()
    assert (features_dir / "date_range_loader.py").exists()
    assert (features_dir / "training_loader.py").exists()


def test_nba_feature_store_facade_stays_thin_after_mode_loader_extraction():
    assert _non_comment_loc(ROOT / "src" / "models" / "feature_store.py") < 250


def test_nba_feature_store_still_owns_source_helpers_until_later_phases():
    # This is an inventory note, not the final target. Later phases should flip
    # remaining source helpers into anti-regrowth assertions as each owner module lands.
    source = inspect.getsource(FeatureStore)
    assert "def _get_injury_context" in source
    assert "def _get_player_prop_lines" in source


def test_nba_player_game_facade_delegates_to_inference_loader():
    source = inspect.getsource(FeatureStore.get_player_game_features)
    assert "InferenceFeatureLoader" in source
    assert "PlayerGameFeatureRequest" in source
    assert "line_spread_raw" not in source
    assert "travel_dist" not in source


def test_nba_date_range_training_facades_delegate_to_mode_loaders():
    checks = (
        (FeatureStore.get_features_for_date, "DateBatchFeatureLoader", "DateFeatureRequest"),
        (FeatureStore.get_features_for_date_range, "DateRangeFeatureLoader", "DateRangeFeatureRequest"),
        (FeatureStore.get_training_dataset, "TrainingFeatureLoader", "TrainingFeatureRequest"),
        (FeatureStore._load_single_season_training, "TrainingFeatureLoader", "load_single_season"),
    )
    for method, loader_name, request_name in checks:
        source = inspect.getsource(method)
        assert loader_name in source
        assert request_name in source


def test_nba_batch_and_training_sql_moved_out_of_feature_store_facade():
    forbidden_sql_tables = (
        "player_average_game_stats",
        "team_average_game_stats",
        "team_allowed_by_position",
        "raw_game_lines_staging",
        "raw_player_props_combined",
    )
    facade_methods = (
        FeatureStore.get_features_for_date,
        FeatureStore._get_game_dates_in_range,
        FeatureStore.get_features_for_date_range,
        FeatureStore.get_training_dataset,
        FeatureStore._load_single_season_training,
    )
    for method in facade_methods:
        source = inspect.getsource(method)
        for table in forbidden_sql_tables:
            assert table not in source


def test_nba_context_player_team_opponent_sql_moved_out_of_feature_store_facade():
    checks = (
        (FeatureStore._get_player_position, "player_position_history", "get_player_position"),
        (FeatureStore._get_context_snapshots, "player_game_stats", "get_context_snapshots"),
        (FeatureStore._get_player_rolling_stats, "player_average_game_stats", "get_player_rolling_stats"),
        (FeatureStore._get_team_rolling_stats, "team_average_game_stats", "get_team_rolling_stats"),
        (FeatureStore._get_opponent_positional_stats, "team_allowed_by_position", "get_opponent_positional_stats"),
    )
    for method, raw_table, delegate in checks:
        source = inspect.getsource(method)
        assert raw_table not in source
        assert delegate in source


def test_nba_injury_context_sql_moved_out_of_feature_store_facade():
    source = inspect.getsource(FeatureStore._get_injury_context)
    assert "rapidapi_injuries" not in source
    assert "get_injury_context" in source

    source = inspect.getsource(FeatureStore._load_injury_features_bulk)
    assert "rapidapi_injuries" not in source
    assert "load_injury_features_bulk" in source

    source = inspect.getsource(FeatureStore._load_player_injury_status_bulk)
    assert "rapidapi_injuries" not in source
    assert "load_player_injury_status_bulk" in source


def test_nba_line_source_sql_moved_out_of_feature_store_facade():
    source = inspect.getsource(FeatureStore._get_player_prop_lines)
    assert "raw_player_props_combined" not in source
    assert "get_player_prop_lines" in source

    source = inspect.getsource(FeatureStore._get_game_lines)
    assert "raw_game_lines_staging" not in source
    assert "get_game_lines" in source
