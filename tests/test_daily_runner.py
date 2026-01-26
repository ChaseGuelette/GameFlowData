import logging
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.models.daily_runner import DailyPredictionRunner


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    # Mock connection context manager
    connection = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    return engine


@pytest.fixture
def mock_feature_store():
    return MagicMock()


@pytest.fixture
def mock_pipeline():
    return MagicMock()


@pytest.fixture
def mock_predictor():
    return MagicMock()


@pytest.fixture
def runner(mock_engine, mock_feature_store, mock_pipeline, mock_predictor):
    return DailyPredictionRunner(
        engine=mock_engine,
        feature_store=mock_feature_store,
        model_pipeline=mock_pipeline,
        predictor=mock_predictor,
    )


def test_init(runner, mock_engine, mock_feature_store, mock_pipeline, mock_predictor):
    assert runner.engine == mock_engine
    assert runner.feature_store == mock_feature_store
    assert runner.pipeline == mock_pipeline
    assert runner.predictor == mock_predictor


def test_get_games_for_date(runner, mock_engine):
    target_date = date(2023, 10, 25)

    # Mock DB result
    mock_result = MagicMock()
    # row._mapping is used in code
    row1 = MagicMock()
    row1._mapping = {"game_id": "g1", "home_team_id": 1, "away_team_id": 2}
    mock_result.__iter__.return_value = [row1]

    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = mock_result

    games = runner._get_games_for_date(target_date)

    assert len(games) == 1
    assert games[0]["game_id"] == "g1"

    # Verify query execution
    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    # call_args is (args, kwargs).
    # The call is conn.execute(query, {"target_date": target_date})
    # So args[0] is query, args[1] is the params dict.
    assert "SELECT DISTINCT game_id" in str(call_args[0][0])
    assert call_args[0][1]["target_date"] == target_date


def test_get_players_for_games_empty(runner):
    assert runner._get_players_for_games([]) == []


def test_get_players_for_games_success(runner, mock_engine):
    games = [{"game_id": "g1", "home_team_id": 1, "away_team_id": 2}]

    # Mock DB result
    mock_result = MagicMock()
    row1 = MagicMock()
    row1._mapping = {"player_id": 101, "player_name": "Player A", "team_id": 1, "avg_min_l5": 30.0}
    row2 = MagicMock()
    row2._mapping = {"player_id": 102, "player_name": "Player B", "team_id": 2, "avg_min_l5": 25.0}
    mock_result.__iter__.return_value = [row1, row2]

    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = mock_result

    players = runner._get_players_for_games(games)

    assert len(players) == 2

    p1 = next(p for p in players if p["player_id"] == 101)
    assert p1["game_id"] == "g1"
    assert p1["opponent_id"] == 2

    p2 = next(p for p in players if p["player_id"] == 102)
    assert p2["game_id"] == "g1"
    assert p2["opponent_id"] == 1

    conn.execute.assert_called_once()


def test_filter_injured_players_no_report(runner, mock_engine, caplog):
    target_date = date(2023, 10, 25)
    players = [{"player_id": 1, "player_name": "Test Player"}]

    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.scalar.return_value = None  # No timestamp found

    with caplog.at_level(logging.WARNING):
        result = runner._filter_injured_players(players, target_date)

    assert len(result) == 1
    assert "No injury report found" in caplog.text


def test_filter_injured_players_success(runner, mock_engine):
    target_date = date(2023, 10, 25)
    players = [
        {"player_id": 1, "player_name": "Healthy Player"},
        {"player_id": 2, "player_name": "Injured Player"},
    ]

    conn = mock_engine.connect.return_value.__enter__.return_value

    # First query gets timestamp
    conn.execute.return_value.scalar.return_value = datetime(2023, 10, 25, 12, 0, 0)

    # Second query gets injured players
    # Mocking result of execute for injured list
    mock_inj_result = MagicMock()
    mock_inj_result.__iter__.return_value = [("Injured Player",)]

    # We need side_effect because execute is called twice with different returns
    def execute_side_effect(query, params=None):
        if "MAX(scrape_timestamp)" in str(query):
            mock_scalar = MagicMock()
            mock_scalar.scalar.return_value = datetime(2023, 10, 25, 12, 0, 0)
            return mock_scalar
        else:
            return mock_inj_result

    conn.execute.side_effect = execute_side_effect

    result = runner._filter_injured_players(players, target_date)

    assert len(result) == 1
    assert result[0]["player_name"] == "Healthy Player"


def test_get_current_lines_empty(runner):
    assert runner._get_current_lines([], ["pts"]).empty


def test_get_current_lines_success(runner, mock_engine):
    games = [{"game_id": "g1"}]
    stats = ["pts"]

    # Mock pd.read_sql
    with patch("src.models.daily_runner.pd.read_sql") as mock_read_sql:
        mock_read_sql.return_value = pd.DataFrame(
            {
                "player_id": [1],
                "game_id": ["g1"],
                "market_key": ["player_points"],
                "line": [20.5],
                "over_odds": [-110],
                "under_odds": [-110],
            }
        )

        df = runner._get_current_lines(games, stats)

        assert len(df) == 1
        assert df.iloc[0]["line"] == 20.5
        mock_read_sql.assert_called_once()


def test_calculate_edges(runner):
    preds_df = pd.DataFrame(
        {
            "player_id": [1],
            "game_id": ["g1"],
            "stat": ["pts"],
            "pred_q10": [10],
            "pred_q25": [15],
            "pred_q50": [20],
            "pred_q75": [25],
            "pred_q90": [30],
        }
    )

    lines_df = pd.DataFrame(
        {
            "player_id": [1],
            "game_id": ["g1"],
            "market_key": ["player_points"],  # Maps to pts
            "line": [20.0],
            "over_odds": [-110],  # ~52.38% implied
            "under_odds": [-110],
        }
    )

    result = runner._calculate_edges(preds_df, lines_df)

    assert "over_prob" in result.columns
    assert "over_edge" in result.columns

    # line 20.0 matches q50 exactly -> 50% probability
    # Implied prob of -110 is ~52.4%
    # Edge should be 0.50 - 0.524 = -0.024
    assert result.iloc[0]["over_prob"] == 0.5
    assert result.iloc[0]["over_edge"] < 0


def test_run_for_date_flow(runner):
    target_date = date(2023, 10, 25)

    # Mock helper methods using side_effect or return_value on the runner instance is tricky if we don't patch class methods.
    # But since we have the instance 'runner', we can mock its methods if we attach mocks to the instance?
    # No, python methods are bound. Easier to patch the methods on the class or use partial mocking.
    # However, since we tested helpers individually, let's mock them to test the flow.

    with (
        patch.object(runner, "_get_games_for_date") as mock_get_games,
        patch.object(runner, "_get_players_for_games") as mock_get_players,
        patch.object(runner, "_filter_injured_players") as mock_filter_injuries,
        patch.object(runner, "_get_current_lines") as mock_get_lines,
        patch.object(runner, "_calculate_edges") as mock_calc_edges,
    ):
        mock_get_games.return_value = [{"game_id": "g1"}]
        mock_get_players.return_value = [{"player_id": 1}]
        mock_filter_injuries.return_value = [
            {"player_id": 1, "game_id": "g1", "player_name": "P1", "team_id": 10, "opponent_id": 20}
        ]

        # Mock feature store
        runner.feature_store.get_player_game_features.return_value = pd.DataFrame()

        # Mock predictor
        mock_pred_result = MagicMock()
        mock_pred_result.mean = 20
        mock_pred_result.median = 20
        mock_pred_result.q10 = 10
        mock_pred_result.q25 = 15
        mock_pred_result.q50 = 20
        mock_pred_result.q75 = 25
        mock_pred_result.q90 = 30

        runner.predictor.predict.return_value = {"pts": mock_pred_result}

        # Mock lines and edges
        mock_get_lines.return_value = pd.DataFrame({"dummy": [1]})
        mock_calc_edges.return_value = pd.DataFrame({"final": [1]})

        result = runner.run_for_date(target_date, stats=["pts"])

        assert len(result) == 1
        assert result.iloc[0]["final"] == 1

        mock_get_games.assert_called_once_with(target_date)
        mock_get_players.assert_called_once()
        mock_filter_injuries.assert_called_once()
        runner.feature_store.get_player_game_features.assert_called_once()
        runner.predictor.predict.assert_called_once()
        mock_get_lines.assert_called_once()
        mock_calc_edges.assert_called_once()


def test_run_for_date_no_features(runner, caplog):
    # Test skipping when features are None
    with (
        patch.object(runner, "_get_games_for_date", return_value=[{"game_id": "g1"}]),
        patch.object(runner, "_get_players_for_games", return_value=[{"player_id": 1}]),
        patch.object(runner, "_filter_injured_players", return_value=[{"player_id": 1, "game_id": "g1"}]),
        patch.object(runner, "_get_current_lines", return_value=pd.DataFrame()),
    ):
        runner.feature_store.get_player_game_features.return_value = None

        result = runner.run_for_date(date(2023, 10, 25))

        assert result.empty
        # Should skip prediction call
        runner.predictor.predict.assert_not_called()


def test_run_for_date_prediction_error(runner, caplog):
    # Test handling prediction exception
    with (
        patch.object(runner, "_get_games_for_date", return_value=[{"game_id": "g1"}]),
        patch.object(runner, "_get_players_for_games", return_value=[{"player_id": 1}]),
        patch.object(runner, "_filter_injured_players", return_value=[{"player_id": 1, "game_id": "g1"}]),
        patch.object(runner, "_get_current_lines", return_value=pd.DataFrame()),
    ):
        runner.feature_store.get_player_game_features.return_value = pd.DataFrame()
        runner.predictor.predict.side_effect = Exception("Predict Error")

        result = runner.run_for_date(date(2023, 10, 25))

        assert result.empty
        assert "Error predicting for player 1" in caplog.text
