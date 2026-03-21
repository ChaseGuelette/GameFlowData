"""
Tests for populate_average_stats module.
"""

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

# Add src to path for imports
SRC_PATH = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_PATH))


@pytest.fixture
def module(monkeypatch):
    mock_client = Mock()
    monkeypatch.setitem(sys.modules, "src.db.client", mock_client)
    monkeypatch.setitem(sys.modules, "src.db", Mock())
    if "processing.populate_average_stats" in sys.modules:
        del sys.modules["processing.populate_average_stats"]
    return importlib.import_module("processing.populate_average_stats")


def test_calculate_games_in_window_counts(module):
    df = pd.DataFrame(
        {
            "player_id": [1, 1, 1],
            "season_id": ["2024", "2024", "2024"],
            "game_date": [
                pd.Timestamp("2024-10-03"),
                pd.Timestamp("2024-10-01"),
                pd.Timestamp("2024-10-02"),
            ],
        }
    )

    result = module.calculate_games_in_window(df, ["player_id", "season_id"])

    assert list(result["game_number"]) == [1, 2, 3]
    assert list(result["games_l5"]) == [0, 1, 2]
    assert list(result["games_l15"]) == [0, 1, 2]
    assert list(result["games_szn"]) == [0, 1, 2]


def test_calculate_rolling_averages_no_leakage(module):
    df = pd.DataFrame(
        {
            "player_id": [1, 1, 1],
            "season_id": ["2024", "2024", "2024"],
            "game_date": [
                pd.Timestamp("2024-10-01"),
                pd.Timestamp("2024-10-02"),
                pd.Timestamp("2024-10-03"),
            ],
            "pts": [10, 20, 30],
        }
    )

    result = module.calculate_rolling_averages(df, ["player_id", "season_id"], ["pts"])

    assert pd.isna(result["avg_pts_l5"].iloc[0])
    assert result["avg_pts_l5"].iloc[1] == 10
    assert result["avg_pts_l5"].iloc[2] == 15
    assert result["avg_pts_szn"].iloc[2] == 15


def test_rolling_with_groupby_respects_groups(module):
    series = pd.Series([1, 2, 10, 20])
    groups = pd.Series(["A", "A", "B", "B"])

    rolled = module.rolling_with_groupby(series, groups, window=2)

    assert list(rolled) == [1.0, 1.5, 10.0, 15.0]


def test_calculate_player_basic_averages_outputs_columns(module):
    df = pd.DataFrame(
        {
            "player_id": [1, 1],
            "season_id": ["2024", "2024"],
            "game_date": [pd.Timestamp("2024-10-01"), pd.Timestamp("2024-10-02")],
            "team_id": [10, 10],
            "min": [30, 32],
            "pts": [10, 20],
        }
    )

    result = module.calculate_player_basic_averages(df)

    assert "avg_min_l5" in result.columns
    assert "avg_pts_l5" in result.columns
    assert pd.isna(result["avg_pts_l5"].iloc[0])
    assert result["avg_pts_l5"].iloc[1] == 10


def test_calculate_player_advanced_averages_outputs_columns(module):
    df = pd.DataFrame(
        {
            "player_id": [1, 1],
            "season_id": ["2024", "2024"],
            "game_date": [pd.Timestamp("2024-10-01"), pd.Timestamp("2024-10-02")],
            "team_id": [10, 10],
            "offensive_rating": [100, 110],
            "pace": [98, 101],
        }
    )

    result = module.calculate_player_advanced_averages(df)

    assert "avg_off_rtg_l5" in result.columns
    assert pd.isna(result["avg_off_rtg_l5"].iloc[0])
    assert result["avg_off_rtg_l5"].iloc[1] == 100


def test_calculate_team_averages_outputs_columns(module):
    df = pd.DataFrame(
        {
            "team_id": [1, 1],
            "season_id": ["2024", "2024"],
            "game_date": [pd.Timestamp("2024-10-01"), pd.Timestamp("2024-10-02")],
            "team_pts": [100, 105],
            "offensive_rating": [110, 112],
        }
    )

    result = module.calculate_team_averages(df)

    assert "avg_pts_l5" in result.columns
    assert "avg_off_rtg_l5" in result.columns
    assert pd.isna(result["avg_pts_l5"].iloc[0])
    assert result["avg_pts_l5"].iloc[1] == 100


def test_fetch_player_game_stats_season_filter(module, monkeypatch):
    captured = {}

    def fake_read_sql(query, engine):
        captured["query"] = query
        return pd.DataFrame()

    monkeypatch.setattr(module.pd, "read_sql", fake_read_sql)

    module.fetch_player_game_stats(Mock(), season_id="2024-25")

    assert "season_id = '2024-25'" in captured["query"]


def test_insert_player_basic_averages_truncates_and_batches(module, monkeypatch):
    module.BATCH_SIZE = 1
    df = pd.DataFrame(
        {
            "player_id": [1, 1],
            "game_id": ["g1", "g2"],
            "season_id": ["2024", "2024"],
            "game_date": [pd.Timestamp("2024-10-01"), pd.Timestamp("2024-10-02")],
            "team_id": [10, 10],
            "game_number": [1, 2],
            "games_l5": [0, 1],
            "games_l15": [0, 1],
            "games_szn": [0, 1],
            "avg_min_l5": [1.23456, 2.34567],
            "avg_min_l15": [1.23456, 2.34567],
            "avg_min_szn": [1.23456, 2.34567],
        }
    )

    conn = Mock()
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine = Mock()
    engine.begin.return_value = begin_cm

    captured_batches = []

    def fake_to_sql(self, name, conn, **kwargs):
        captured_batches.append(self.copy())

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=False)

    module.insert_player_basic_averages(engine, df)

    assert any("DELETE FROM player_average_game_stats" in str(call[0][0]) for call in conn.execute.call_args_list)
    assert len(captured_batches) == 2
    assert captured_batches[0]["avg_min_l5"].iloc[0] == 1.2346


def test_insert_player_advanced_averages_inserts(module, monkeypatch):
    module.BATCH_SIZE = 2
    df = pd.DataFrame(
        {
            "player_id": [1],
            "game_id": ["g1"],
            "season_id": ["2024"],
            "game_date": [pd.Timestamp("2024-10-01")],
            "team_id": [10],
            "game_number": [1],
            "games_l5": [0],
            "games_l15": [0],
            "games_szn": [0],
            "avg_off_rtg_l5": [101.12345],
        }
    )

    conn = Mock()
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine = Mock()
    engine.begin.return_value = begin_cm

    to_sql_calls = []

    def fake_to_sql(self, name, conn, **kwargs):
        to_sql_calls.append(name)

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=False)

    module.insert_player_advanced_averages(engine, df)

    assert "player_average_advanced_stats" in to_sql_calls
    assert any(
        "DELETE FROM player_average_advanced_stats" in str(call[0][0]) for call in conn.execute.call_args_list
    )


def test_insert_team_averages_inserts(module, monkeypatch):
    module.BATCH_SIZE = 2
    df = pd.DataFrame(
        {
            "team_id": [1],
            "game_id": ["g1"],
            "season_id": ["2024"],
            "game_date": [pd.Timestamp("2024-10-01")],
            "game_number": [1],
            "games_l5": [0],
            "games_l15": [0],
            "games_szn": [0],
            "avg_pts_l5": [99.9999],
        }
    )

    conn = Mock()
    begin_cm = MagicMock()
    begin_cm.__enter__.return_value = conn
    begin_cm.__exit__.return_value = False
    engine = Mock()
    engine.begin.return_value = begin_cm

    to_sql_calls = []

    def fake_to_sql(self, name, conn, **kwargs):
        to_sql_calls.append(name)

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=False)

    module.insert_team_averages(engine, df)

    assert "team_average_game_stats" in to_sql_calls
    assert any("DELETE FROM team_average_game_stats" in str(call[0][0]) for call in conn.execute.call_args_list)


def test_main_player_only_calls_player_flow(module, monkeypatch):
    engine = Mock()
    monkeypatch.setattr(module, "get_engine", Mock(return_value=engine))

    player_df = pd.DataFrame({"player_id": [1]})
    fetch_basic = Mock(return_value=player_df)
    calc_basic = Mock(return_value=player_df)
    calc_b2_b3_b4 = Mock(return_value=player_df)
    monkeypatch.setattr(module, "fetch_player_game_stats", fetch_basic)
    monkeypatch.setattr(module, "calculate_player_basic_averages", calc_basic)
    monkeypatch.setattr(module, "calculate_b2_b3_b4_features", calc_b2_b3_b4)
    insert_basic = Mock()
    monkeypatch.setattr(module, "insert_player_basic_averages", insert_basic)

    fetch_adv = Mock()
    calc_adv = Mock()
    insert_adv = Mock()
    fetch_team = Mock()
    calc_team = Mock()
    insert_team = Mock()

    monkeypatch.setattr(module, "fetch_player_advanced_stats", fetch_adv)
    monkeypatch.setattr(module, "calculate_player_advanced_averages", calc_adv)
    monkeypatch.setattr(module, "insert_player_advanced_averages", insert_adv)
    monkeypatch.setattr(module, "fetch_team_game_stats", fetch_team)
    monkeypatch.setattr(module, "calculate_team_averages", calc_team)
    monkeypatch.setattr(module, "insert_team_averages", insert_team)

    monkeypatch.setattr(sys, "argv", ["prog", "--table", "player", "--season", "2024-25"])

    module.main()

    fetch_basic.assert_called_once_with(engine, "2024-25", None)
    calc_basic.assert_called_once_with(player_df)
    calc_b2_b3_b4.assert_called_once_with(player_df)
    insert_basic.assert_called_once()
    fetch_adv.assert_not_called()
    fetch_team.assert_not_called()


@pytest.fixture
def incremental_module(monkeypatch):
    mock_client = Mock()
    monkeypatch.setitem(sys.modules, "src.db.client", mock_client)
    monkeypatch.setitem(sys.modules, "src.db", Mock())
    if "processing.populate_average_stats_incremental" in sys.modules:
        del sys.modules["processing.populate_average_stats_incremental"]
    return importlib.import_module("processing.populate_average_stats_incremental")


def test_low_minutes_games_excluded_from_rolling(module):
    """A 1-min injury exit game should be excluded from stat rolling averages
    but schedule features (rest_days) should still count it."""
    df = pd.DataFrame(
        {
            "player_id": [1] * 7,
            "season_id": ["2024"] * 7,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04",
                 "2024-10-05", "2024-10-06", "2024-10-07"]
            ),
            "min": [30, 32, 28, 1, 30, 34, 29],   # game 3 (idx=3) is 1-min injury exit
            "pts": [20, 22, 18, 0, 24, 26, 20],
            "reb": [8, 9, 7, 0, 8, 10, 6],
            "ast": [5, 4, 6, 0, 5, 7, 3],
            "fg3m": [2, 3, 1, 0, 2, 4, 1],
        }
    )

    df = module.calculate_player_basic_averages(df)
    df = module.calculate_b2_b3_b4_features(df)

    # Row 6 (game 7): L5 window covers games 1-5 (indices 1-5)
    # Without filter: avg_reb_l5 includes 0 from 1-min game → (9+7+0+8+10)/5 = 6.8
    # With filter: avg_reb_l5 excludes 1-min game → (9+7+8+10)/4 = 8.5
    row6 = df.iloc[6]
    assert row6["avg_reb_l5"] == pytest.approx(8.5, abs=0.01), \
        f"Expected 8.5 but got {row6['avg_reb_l5']} — low-minutes game not excluded"

    # min_floor_l5 should exclude the 1-min game
    # Without filter: min(32, 28, 1, 30, 34) = 1.0
    # With filter: min(32, 28, 30, 34) = 28.0
    assert row6["min_floor_l5"] == pytest.approx(28.0, abs=0.01), \
        f"Expected 28.0 but got {row6['min_floor_l5']} — 1-min game not excluded from min_floor"

    # rest_days should still count the 1-min game (schedule feature)
    # Game 7 (Oct 7) - Game 6 (Oct 6) = 1 day
    assert row6["rest_days"] == 1


def test_all_low_minutes_produces_nan(module):
    """When all prior games are < 5 min, rolling averages should be NaN."""
    df = pd.DataFrame(
        {
            "player_id": [1] * 4,
            "season_id": ["2024"] * 4,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04"]
            ),
            "min": [2, 3, 4, 30],   # first 3 games all < 5 min
            "pts": [1, 2, 1, 20],
            "reb": [0, 1, 0, 8],
            "ast": [0, 0, 1, 5],
            "fg3m": [0, 0, 0, 3],
        }
    )

    df = module.calculate_player_basic_averages(df)

    # Row 3 (game 4): all prior games were < 5 min, so all masked → NaN
    assert pd.isna(df["avg_reb_l5"].iloc[3]), \
        f"Expected NaN but got {df['avg_reb_l5'].iloc[3]} — low-minutes games should produce NaN"


def test_low_minutes_excluded_incremental(incremental_module):
    """Test that the incremental pipeline also excludes low-minutes games."""
    player_df = pd.DataFrame(
        {
            "player_id": [1] * 7,
            "game_id": [f"g{i}" for i in range(7)],
            "season_id": ["2024"] * 7,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04",
                 "2024-10-05", "2024-10-06", "2024-10-07"]
            ),
            "team_id": [10] * 7,
            "min": [30, 32, 28, 1, 30, 34, 29],
            "pts": [20, 22, 18, 0, 24, 26, 20],
            "reb": [8, 9, 7, 0, 8, 10, 6],
            "ast": [5, 4, 6, 0, 5, 7, 3],
            "fg3m": [2, 3, 1, 0, 2, 4, 1],
            "fgm": [8] * 7, "fga": [16] * 7, "fg_pct": [0.5] * 7,
            "fg3a": [5] * 7, "fg3_pct": [0.4] * 7,
            "ftm": [4] * 7, "fta": [5] * 7, "ft_pct": [0.8] * 7,
            "oreb": [2] * 7, "dreb": [6] * 7,
            "stl": [1] * 7, "blk": [1] * 7, "tov": [2] * 7,
            "pf": [3] * 7, "plus_minus": [5] * 7,
        }
    )

    result = incremental_module.calculate_basic_rolling_for_player(player_df)

    row6 = result.iloc[6]
    assert row6["avg_reb_l5"] == pytest.approx(8.5, abs=0.01)
    assert row6["min_floor_l5"] == pytest.approx(28.0, abs=0.01)
    assert row6["rest_days"] == 1


def test_calculate_b2_b3_b4_features_l3_no_leakage(module):
    """L3 averages should use shift(1), so row 0 has NaN."""
    df = pd.DataFrame(
        {
            "player_id": [1, 1, 1, 1, 1],
            "season_id": ["2024"] * 5,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04", "2024-10-05"]
            ),
            "min": [30, 32, 28, 35, 25],
            "pts": [10, 20, 30, 40, 50],
            "reb": [5, 6, 7, 8, 9],
            "ast": [3, 4, 5, 6, 7],
            "fg3m": [1, 2, 3, 4, 5],
        }
    )

    result = module.calculate_b2_b3_b4_features(df)

    # Row 0: no prior games → NaN
    assert pd.isna(result["avg_pts_l3"].iloc[0])
    # Row 1: only game 0 is prior → avg(10) = 10
    assert result["avg_pts_l3"].iloc[1] == 10.0
    # Row 3: prior games are 0,1,2 → last 3 = avg(10,20,30) = 20
    assert result["avg_pts_l3"].iloc[3] == 20.0


def test_calculate_b2_b3_b4_features_std_constant(module):
    """Constant data should produce std ≈ 0."""
    df = pd.DataFrame(
        {
            "player_id": [1] * 7,
            "season_id": ["2024"] * 7,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04",
                 "2024-10-05", "2024-10-06", "2024-10-07"]
            ),
            "min": [30] * 7,
            "pts": [20] * 7,
            "reb": [5] * 7,
            "ast": [3] * 7,
            "fg3m": [2] * 7,
        }
    )

    result = module.calculate_b2_b3_b4_features(df)

    # After enough games, std of constant values = 0
    last_row = result.iloc[-1]
    assert last_row["std_pts_l5"] == 0.0
    assert last_row["std_min_l5"] == 0.0


def test_calculate_b2_b3_b4_features_rest_days(module):
    """Rest days should be computed from game_date diffs."""
    df = pd.DataFrame(
        {
            "player_id": [1, 1, 1],
            "season_id": ["2024"] * 3,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-05"]
            ),
            "min": [30, 32, 28],
            "pts": [10, 20, 30],
            "reb": [5, 6, 7],
            "ast": [3, 4, 5],
            "fg3m": [1, 2, 3],
        }
    )

    result = module.calculate_b2_b3_b4_features(df)

    # Row 0: first game → default rest_days = 3
    assert result["rest_days"].iloc[0] == 3
    # Row 1: back-to-back (1 day gap)
    assert result["rest_days"].iloc[1] == 1
    # Row 2: 3-day gap
    assert result["rest_days"].iloc[2] == 3


def test_calculate_b2_b3_b4_features_games_started(module):
    """Games started should count games with min >= 20."""
    df = pd.DataFrame(
        {
            "player_id": [1] * 6,
            "season_id": ["2024"] * 6,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-03",
                 "2024-10-04", "2024-10-05", "2024-10-06"]
            ),
            "min": [25, 10, 30, 22, 15, 28],  # started: Y, N, Y, Y, N, Y
            "pts": [10, 5, 20, 15, 8, 18],
            "reb": [5, 2, 7, 6, 3, 8],
            "ast": [3, 1, 5, 4, 2, 6],
            "fg3m": [1, 0, 3, 2, 1, 4],
        }
    )

    result = module.calculate_b2_b3_b4_features(df)

    # Row 5 (last): prior games 0-4, last 5 started = [Y, N, Y, Y, N] = 3
    assert result["games_started_l5"].iloc[5] == 3


def test_games_started_uses_actual_started_column(module):
    """When 'started' column exists, games_started_l5 should use it."""
    df = pd.DataFrame(
        {
            "player_id": [1] * 6,
            "season_id": ["2024"] * 6,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-03",
                 "2024-10-04", "2024-10-05", "2024-10-06"]
            ),
            # Minutes proxy would say: Y(25), N(10), Y(30), N(15), N(15), Y(28)
            "min": [25, 10, 30, 15, 15, 28],
            # Actual starter data: N, Y, Y, Y, Y, N  (different from proxy)
            "started": [False, True, True, True, True, False],
            "pts": [10, 5, 20, 15, 8, 18],
            "reb": [5, 2, 7, 6, 3, 8],
            "ast": [3, 1, 5, 4, 2, 6],
            "fg3m": [1, 0, 3, 2, 1, 4],
        }
    )

    result = module.calculate_b2_b3_b4_features(df)

    # Row 5 (last): prior games 0-4 actual started = [N, Y, Y, Y, Y] = 4
    # With proxy it would be [Y, N, Y, N, N] = 2
    assert result["games_started_l5"].iloc[5] == 4, \
        f"Expected 4 from actual started data but got {result['games_started_l5'].iloc[5]}"


def test_games_started_fallback_when_started_null(module):
    """When 'started' column is all NULL, should fall back to minutes proxy."""
    df = pd.DataFrame(
        {
            "player_id": [1] * 6,
            "season_id": ["2024"] * 6,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-03",
                 "2024-10-04", "2024-10-05", "2024-10-06"]
            ),
            "min": [25, 10, 30, 22, 15, 28],
            "started": [None, None, None, None, None, None],
            "pts": [10, 5, 20, 15, 8, 18],
            "reb": [5, 2, 7, 6, 3, 8],
            "ast": [3, 1, 5, 4, 2, 6],
            "fg3m": [1, 0, 3, 2, 1, 4],
        }
    )

    result = module.calculate_b2_b3_b4_features(df)

    # Same as proxy-only: Y, N, Y, Y, N → 3
    assert result["games_started_l5"].iloc[5] == 3


def test_games_started_mixed_null_and_actual(module):
    """Mixed data: use actual where available, proxy for NULL."""
    df = pd.DataFrame(
        {
            "player_id": [1] * 6,
            "season_id": ["2024"] * 6,
            "game_date": pd.to_datetime(
                ["2024-10-01", "2024-10-02", "2024-10-03",
                 "2024-10-04", "2024-10-05", "2024-10-06"]
            ),
            # Minutes: 25(Y), 10(N), 30(Y), 15(N), 25(Y), 28(Y)
            "min": [25, 10, 30, 15, 25, 28],
            # Actual: True, None(→proxy N), True, None(→proxy N), True, None(→proxy Y)
            "started": [True, None, True, None, True, None],
            "pts": [10, 5, 20, 15, 8, 18],
            "reb": [5, 2, 7, 6, 3, 8],
            "ast": [3, 1, 5, 4, 2, 6],
            "fg3m": [1, 0, 3, 2, 1, 4],
        }
    )

    result = module.calculate_b2_b3_b4_features(df)

    # Row 5 (last): prior games 0-4 = [True, proxy(10→N), True, proxy(15→N), True] = 3
    assert result["games_started_l5"].iloc[5] == 3
