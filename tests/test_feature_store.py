"""
Tests for feature_store module.
"""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, Mock

import numpy as np
import pandas as pd
import pytest

# Add src to path for imports
SRC_PATH = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_PATH))

from models.feature_store import (
    MINUTES_FEATURES,
    RATE_FEATURES_AST,
    RATE_FEATURES_PTS,
    RATE_FEATURES_REB,
    RATE_FEATURES_THREES,
    FeatureConfig,
    FeatureStore,
)


class FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping
        for key, value in mapping.items():
            setattr(self, key, value)


class FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


def test_feature_config_defaults():
    cfg = FeatureConfig()

    assert cfg.min_minutes_for_rate == 10
    assert cfg.min_games_l5 == 3
    assert cfg.excluded_seasons == ("22019", "22020")


def test_feature_lists_defined():
    """Ensure centralized feature lists are defined."""
    assert len(MINUTES_FEATURES) > 0
    assert "player_avg_min_l5" in MINUTES_FEATURES
    assert "team_avg_pace_l5" in MINUTES_FEATURES

    assert len(RATE_FEATURES_PTS) > 0
    assert "player_avg_pts_l5" in RATE_FEATURES_PTS

    assert len(RATE_FEATURES_REB) > 0
    assert "player_avg_reb_l5" in RATE_FEATURES_REB

    assert len(RATE_FEATURES_AST) > 0
    assert "player_avg_ast_l5" in RATE_FEATURES_AST

    assert len(RATE_FEATURES_THREES) > 0
    assert "player_avg_fg3m_l5" in RATE_FEATURES_THREES


def test_get_context_snapshots_returns_none_without_position_group():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow(
        {
            "team_id": 1,
            "season_id": "22024",
            "opponent_id": 2,
            "is_home": 1,
            "position_group": None,
        }
    )
    conn.execute.return_value = FakeResult(row)

    result = store._get_context_snapshots(conn, "g1", 10, date(2025, 1, 1))

    assert result is None


def test_get_context_snapshots_returns_dict():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow(
        {
            "team_id": 1,
            "season_id": "22024",
            "opponent_id": 2,
            "is_home": 1,
            "position_group": "G",
        }
    )
    conn.execute.return_value = FakeResult(row)

    result = store._get_context_snapshots(conn, "g1", 10, date(2025, 1, 1))

    assert result["team_id"] == 1
    assert result["season_id"] == "22024"
    assert result["position_group"] == "G"


def test_get_player_rolling_stats_fallback():
    """When no data found, return 0 for all stats."""
    store = FeatureStore(engine=Mock())
    conn = Mock()
    conn.execute.return_value = FakeResult(None)

    result = store._get_player_rolling_stats(conn, 10, date(2025, 1, 1))

    assert result["player_avg_min_l5"] == 0
    assert result["player_avg_usg_pct_l5"] == 0
    assert result["player_avg_reb_l5"] == 0
    assert result["player_avg_ast_l5"] == 0


def test_get_player_rolling_stats_maps_keys():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow(
        {
            "avg_min_l5": 12,
            "avg_min_l15": 18,
            "avg_pts_l5": 10,
            "avg_pts_l15": 14,
            "avg_reb_l5": 5,
            "avg_ast_l5": 3,
            "avg_fg3m_l5": 2,
            "avg_fg3a_l5": 5,
            "avg_usg_pct_l5": 0.2,
            "avg_ts_pct_l15": 0.55,
            "avg_reb_pct_l5": 0.1,
            "avg_ast_pct_l5": 0.15,
        }
    )
    conn.execute.return_value = FakeResult(row)

    result = store._get_player_rolling_stats(conn, 10, date(2025, 1, 1))

    assert result["player_avg_min_l5"] == 12
    assert result["player_avg_ts_pct_l15"] == 0.55
    assert result["player_avg_reb_l5"] == 5


def test_get_team_rolling_stats_fallback():
    """When no data found, return 0 for all stats."""
    store = FeatureStore(engine=Mock())
    conn = Mock()
    conn.execute.return_value = FakeResult(None)

    result = store._get_team_rolling_stats(conn, 1, date(2025, 1, 1), False)

    assert result["team_avg_pace_l5"] == 0
    assert result["team_avg_def_rtg_l5"] == 0


def test_get_team_rolling_stats_mapping_for_opponent():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow({"avg_pace_l5": 101, "avg_def_rtg_l5": 108})
    conn.execute.return_value = FakeResult(row)

    result = store._get_team_rolling_stats(conn, 2, date(2025, 1, 1), True)

    assert result["opp_avg_pace_l5"] == 101
    assert result["opp_avg_def_rtg_l5"] == 108


def test_get_opponent_positional_stats_fallback():
    """When no data found, return 0 for all stats."""
    store = FeatureStore(engine=Mock())
    conn = Mock()
    conn.execute.return_value = FakeResult(None)

    result = store._get_opponent_positional_stats(conn, 2, "G", date(2025, 1, 1))

    assert result["opp_pos_off_rtg_allowed_l5"] == 0
    assert result["opp_pos_reb_allowed_l5"] == 0
    assert result["opp_pos_ast_allowed_l5"] == 0
    assert result["opp_pos_threes_allowed_l5"] == 0


def test_get_opponent_positional_stats_mapping():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow(
        {
            "off_rtg_allowed_l5": 108,
            "reb_allowed_l5": 45.5,
            "ast_allowed_l5": 25.0,
            "threes_allowed_l5": 12.0,
        }
    )
    conn.execute.return_value = FakeResult(row)

    result = store._get_opponent_positional_stats(conn, 2, "G", date(2025, 1, 1))

    assert result["opp_pos_off_rtg_allowed_l5"] == 108
    assert result["opp_pos_reb_allowed_l5"] == 45.5


def test_get_game_lines_defaults():
    """When no data found, return 0 for all lines."""
    store = FeatureStore(engine=Mock())
    conn = Mock()
    conn.execute.return_value = FakeResult(None)

    result = store._get_game_lines(conn, "g1")

    assert result["line_spread"] == 0
    assert result["line_total"] == 0


def test_get_game_lines_uses_db_values():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow({"spread": -4.0, "total": 219.5})
    conn.execute.return_value = FakeResult(row)

    result = store._get_game_lines(conn, "g1")

    assert result["line_spread"] == -4.0
    assert result["line_total"] == 219.5


def test_get_player_game_features_returns_none_when_context_missing():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    store = FeatureStore(engine=engine)
    store._get_context_snapshots = Mock(return_value=None)
    store._get_player_rolling_stats = Mock()

    result = store.get_player_game_features(1, "g1", date(2025, 1, 1))

    assert result is None
    store._get_player_rolling_stats.assert_not_called()


def test_get_player_game_features_combines_outputs():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    store = FeatureStore(engine=engine)
    store._get_context_snapshots = Mock(
        return_value={"season_id": "22024", "position_group": "G", "team_id": 1, "opponent_id": 2, "is_home": 1}
    )
    store._get_player_rolling_stats = Mock(return_value={"player_avg_min_l5": 20})
    store._get_team_rolling_stats = Mock(side_effect=[{"team_avg_pace_l5": 100}, {"opp_avg_pace_l5": 98}])
    store._get_opponent_positional_stats = Mock(return_value={"opp_pos_off_rtg_allowed_l5": 110})
    store._get_game_lines = Mock(return_value={"line_spread": -6, "line_total": 225})
    store._get_travel_features_single = Mock(return_value={"rest_days": 2, "travel_dist": 100, "is_back_to_back": 0})

    result = store.get_player_game_features(1, "g1", date(2025, 1, 1))

    assert result["player_id"] == 1
    assert result["game_id"] == "g1"
    assert result["season_id"] == "22024"
    assert result["player_avg_min_l5"] == 20
    assert result["line_spread"] == -6
    assert result["rest_days"] == 2


def test_get_training_dataset_raises_on_small_dataset():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm
    store = FeatureStore(engine=engine)
    store._get_travel_and_rest_features = Mock(return_value=pd.DataFrame())

    def fake_read_sql(query, conn, params=None):
        return pd.DataFrame({"position_group": ["G"]})

    pd_read_sql = pd.read_sql
    pd.read_sql = fake_read_sql
    try:
        with pytest.raises(ValueError, match="Suspiciously few rows"):
            store.get_training_dataset(["22024"])
    finally:
        pd.read_sql = pd_read_sql


def test_get_training_dataset_raises_on_null_position_group():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm
    store = FeatureStore(engine=engine)
    store._get_travel_and_rest_features = Mock(return_value=pd.DataFrame())

    def fake_read_sql(query, conn, params=None):
        rows = 10000
        data = {
            "position_group": ["G"] * rows,
            "actual_minutes": np.full(rows, 20.0),
            "actual_pts": np.full(rows, 18.0),
            "actual_reb": np.full(rows, 5.0),
            "actual_ast": np.full(rows, 7.0),
            "actual_threes": np.full(rows, 2.0),
        }
        data["position_group"][0] = None
        return pd.DataFrame(data)

    pd_read_sql = pd.read_sql
    pd.read_sql = fake_read_sql
    try:
        with pytest.raises(ValueError, match="Position Group has NULLs"):
            store.get_training_dataset(["22024"])
    finally:
        pd.read_sql = pd_read_sql


def test_get_training_dataset_builds_rate_targets():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    store = FeatureStore(engine=engine)
    store._get_travel_and_rest_features = Mock(return_value=pd.DataFrame())
    captured = {}

    def fake_read_sql(query, _conn, params=None):
        captured["params"] = params
        rows = 10000
        data = {
            "position_group": ["G"] * rows,
            "actual_minutes": np.full(rows, 20.0),
            "actual_pts": np.full(rows, 18.0),
            "actual_reb": np.full(rows, 5.0),
            "actual_ast": np.full(rows, 7.0),
            "actual_threes": np.full(rows, 2.0),
            "game_id": ["g1"] * rows,
            "team_id": [1] * rows,
            "opponent_id": [2] * rows,
        }
        # One row with low minutes (should not have rate targets)
        data["actual_minutes"][0] = 5.0
        return pd.DataFrame(data)

    pd_read_sql = pd.read_sql
    pd.read_sql = fake_read_sql
    try:
        df = store.get_training_dataset(["22024"])
    finally:
        pd.read_sql = pd_read_sql

    assert captured["params"]["seasons"] == ["22024"]
    assert captured["params"]["excluded"] == list(store.config.excluded_seasons)

    # Rate targets should be calculated for rows with >= 10 minutes
    assert "pts_per_min" in df.columns
    assert pd.isna(df["pts_per_min"].iloc[0])  # Low minutes row
    assert df["pts_per_min"].iloc[1] == pytest.approx(0.9, rel=1e-6)  # 18/20
