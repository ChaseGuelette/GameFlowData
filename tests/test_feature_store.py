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

from models.feature_store import FeatureConfig, FeatureStore


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


def test_get_league_priors_fallback():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    conn.execute.return_value = FakeResult(None)

    result = store._get_league_priors(conn, "22024", "G", date(2025, 1, 1))

    assert result["league_off_rtg"] == 110.0
    assert result["league_reb_per100"] == 15.0
    assert result["league_threes_per100"] == 3.0
    assert result["league_pace"] == 99.0


def test_get_player_rolling_stats_fallback():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    conn.execute.return_value = FakeResult(None)

    result = store._get_player_rolling_stats(conn, 10, date(2025, 1, 1), {})

    assert result["player_avg_min_l5"] == 0
    assert result["player_avg_usg_pct_l5"] == 0.15


def test_get_player_rolling_stats_maps_keys():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow(
        {
            "avg_min_l5": 12,
            "avg_min_l15": 18,
            "avg_pts_l5": 10,
            "avg_pts_l15": 14,
            "avg_usg_pct_l5": 0.2,
            "avg_usg_pct_l15": 0.19,
            "avg_ts_pct_l15": 0.55,
        }
    )
    conn.execute.return_value = FakeResult(row)

    result = store._get_player_rolling_stats(conn, 10, date(2025, 1, 1), {})

    assert result["player_avg_min_l5"] == 12
    assert result["player_avg_ts_pct_l15"] == 0.55


def test_get_team_rolling_stats_fallback():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    conn.execute.return_value = FakeResult(None)
    priors = {"league_pace": 99.0, "league_off_rtg": 110.0}

    result = store._get_team_rolling_stats(conn, 1, date(2025, 1, 1), priors, False)

    assert result["team_avg_pace_l5"] == 99.0
    assert result["team_avg_off_rtg_l5"] == 110.0
    assert result["team_avg_def_rtg_l5"] == 110.0


def test_get_team_rolling_stats_mapping_for_opponent():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow({"avg_pace_l5": 101, "avg_off_rtg_l5": 112, "avg_def_rtg_l5": 108})
    conn.execute.return_value = FakeResult(row)

    result = store._get_team_rolling_stats(conn, 2, date(2025, 1, 1), {}, True)

    assert result["opp_avg_pace_l5"] == 101
    assert result["opp_avg_def_rtg_l5"] == 108


def test_get_opponent_positional_stats_fallback():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    conn.execute.return_value = FakeResult(None)
    priors = {"league_off_rtg": 110.0, "league_reb_per100": 15.0, "league_threes_per100": 3.0}

    result = store._get_opponent_positional_stats(conn, 2, "G", date(2025, 1, 1), priors)

    assert result["opp_pos_off_rtg_allowed_l5"] == 110.0
    assert result["opp_pos_reb_per100_allowed_l5"] == 15.0
    assert result["opp_pos_threes_per100_allowed_l5"] == 3.0


def test_get_opponent_positional_stats_mapping():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow(
        {
            "off_rtg_allowed_l5": 108,
            "reb_per100_allowed_l5": 14.5,
            "threes_per100_allowed_l5": 3.2,
        }
    )
    conn.execute.return_value = FakeResult(row)

    result = store._get_opponent_positional_stats(conn, 2, "G", date(2025, 1, 1), {})

    assert result["opp_pos_off_rtg_allowed_l5"] == 108
    assert result["opp_pos_reb_per100_allowed_l5"] == 14.5


def test_get_game_lines_defaults():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    conn.execute.return_value = FakeResult(None)

    result = store._get_game_lines(conn, "g1")

    assert result["line_spread"] == -5.5
    assert result["line_total"] == 225.0


def test_get_game_lines_uses_db_values():
    store = FeatureStore(engine=Mock())
    conn = Mock()
    row = FakeRow({"spread": -4.0, "total": 219.5})
    conn.execute.return_value = FakeResult(row)

    result = store._get_game_lines(conn, "g1")

    assert result["line_spread"] == -4.0
    assert result["line_total"] == 219.5


def test_calculate_derived_features_handles_low_minutes():
    store = FeatureStore(engine=Mock())

    p_stats = {"player_avg_pts_l5": 10, "player_avg_min_l5": 4, "player_avg_usg_pct_l5": 0.2}
    t_stats = {"team_avg_pace_l5": 100}
    o_stats = {"opp_avg_pace_l5": 98}
    priors = {"league_pace": 99.0}
    lines = {"line_spread": -6}

    derived = store._calculate_derived_features(p_stats, t_stats, o_stats, lines, priors)

    assert derived["line_spread_abs"] == 6
    assert derived["player_pts_per100_l5"] == 0.0


def test_calculate_derived_features_expected_pace_and_trend():
    store = FeatureStore(engine=Mock())

    p_stats = {
        "player_avg_pts_l5": 20,
        "player_avg_min_l5": 30,
        "player_avg_usg_pct_l5": 0.25,
        "player_avg_usg_pct_l15": 0.2,
    }
    t_stats = {"team_avg_pace_l5": 100}
    o_stats = {"opp_avg_pace_l5": 98}
    priors = {"league_pace": 99.0}
    lines = {"line_spread": -6}

    derived = store._calculate_derived_features(p_stats, t_stats, o_stats, lines, priors)

    assert derived["expected_pace"] == pytest.approx(100 * 98 / 99, rel=1e-6)
    assert derived["player_usg_trend"] == pytest.approx(0.05, rel=1e-6)
    assert derived["player_pts_per100_l5"] == pytest.approx(32.0, rel=1e-6)


def test_get_player_game_features_returns_none_when_context_missing():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    store = FeatureStore(engine=engine)
    store._get_context_snapshots = Mock(return_value=None)
    store._get_league_priors = Mock()

    result = store.get_player_game_features(1, "g1", date(2025, 1, 1))

    assert result is None
    store._get_league_priors.assert_not_called()


def test_get_player_game_features_combines_outputs():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    store = FeatureStore(engine=engine)
    store._get_context_snapshots = Mock(
        return_value={"season_id": "22024", "position_group": "G", "team_id": 1, "opponent_id": 2}
    )
    store._get_league_priors = Mock(return_value={"league_pace": 99.0})
    store._get_player_rolling_stats = Mock(return_value={"player_avg_min_l5": 20})
    store._get_team_rolling_stats = Mock(side_effect=[{"team_avg_pace_l5": 100}, {"opp_avg_pace_l5": 98}])
    store._get_opponent_positional_stats = Mock(return_value={"opp_pos_off_rtg_allowed_l5": 110})
    store._get_game_lines = Mock(return_value={"line_spread": -6, "line_total": 225})
    store._get_rest_travel = Mock(return_value={"rest_days": 1, "is_back_to_back": 0})
    store._calculate_derived_features = Mock(return_value={"expected_pace": 99})

    result = store.get_player_game_features(1, "g1", date(2025, 1, 1))

    assert result["player_id"] == 1
    assert result["game_id"] == "g1"
    assert result["season_id"] == "22024"
    assert result["expected_pace"] == 99


def test_get_training_dataset_raises_on_small_dataset():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm
    store = FeatureStore(engine=engine)

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

    def fake_read_sql(query, conn, params=None):
        rows = 10000
        data = {
            "position_group": ["G"] * rows,
            "team_avg_pace_l5": np.full(rows, 100.0),
            "opp_avg_pace_l5": np.full(rows, 98.0),
            "league_avg_pace": np.full(rows, 99.0),
            "line_spread": np.full(rows, -6.0),
            "player_avg_usg_pct_l5": np.full(rows, 0.25),
            "player_avg_usg_pct_l15": np.full(rows, 0.2),
            "player_avg_pts_l5": np.full(rows, 20.0),
            "player_avg_min_l5": np.full(rows, 30.0),
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


def test_get_training_dataset_builds_features():
    engine = Mock()
    conn = Mock()
    connect_cm = MagicMock()
    connect_cm.__enter__.return_value = conn
    connect_cm.__exit__.return_value = False
    engine.connect.return_value = connect_cm

    store = FeatureStore(engine=engine)
    captured = {}

    def fake_read_sql(query, _conn, params=None):
        captured["params"] = params
        rows = 10000
        data = {
            "position_group": ["G"] * rows,
            "team_avg_pace_l5": np.full(rows, 100.0),
            "opp_avg_pace_l5": np.full(rows, 98.0),
            "league_avg_pace": np.full(rows, 99.0),
            "line_spread": np.full(rows, -6.0),
            "player_avg_usg_pct_l5": np.full(rows, 0.25),
            "player_avg_usg_pct_l15": np.full(rows, 0.2),
            "player_avg_pts_l5": np.full(rows, 20.0),
            "player_avg_min_l5": np.full(rows, 30.0),
            "actual_minutes": np.full(rows, 20.0),
            "actual_pts": np.full(rows, 18.0),
            "actual_reb": np.full(rows, 5.0),
            "actual_ast": np.full(rows, 7.0),
            "actual_threes": np.full(rows, 2.0),
        }
        data["player_avg_min_l5"][0] = 4.0
        return pd.DataFrame(data)

    pd_read_sql = pd.read_sql
    pd.read_sql = fake_read_sql
    try:
        df = store.get_training_dataset(["22024"])
    finally:
        pd.read_sql = pd_read_sql

    assert captured["params"]["seasons"] == ("22024",)
    assert captured["params"]["excluded"] == store.config.excluded_seasons

    assert "expected_pace" in df.columns
    assert df["line_spread_abs"].iloc[0] == 6.0
    assert df["player_usg_trend"].iloc[0] == pytest.approx(0.05, rel=1e-6)
    assert pd.isna(df["player_pts_per100_l5"].iloc[0])
    assert df["player_pts_per100_l5"].iloc[1] == pytest.approx(32.0, rel=1e-6)
    assert df["pts_per_min"].iloc[0] == pytest.approx(0.9, rel=1e-6)
