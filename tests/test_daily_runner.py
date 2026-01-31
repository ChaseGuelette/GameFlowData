import logging
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import numpy as np
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
    """Test _get_games_for_date falls back to DB when NBA API returns empty."""
    target_date = date(2023, 10, 25)

    # Mock DB result
    mock_result = MagicMock()
    row1 = MagicMock()
    row1._mapping = {"game_id": "g1", "home_team_id": 1, "away_team_id": 2}
    mock_result.__iter__.return_value = [row1]

    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = mock_result

    # Patch NBA API to return empty (triggers DB fallback)
    with patch.object(runner, "_get_games_from_nba_api", return_value=[]):
        games = runner._get_games_for_date(target_date)

    assert len(games) == 1
    assert games[0]["game_id"] == "g1"

    # Verify DB query execution
    conn.execute.assert_called_once()
    call_args = conn.execute.call_args
    assert "SELECT DISTINCT game_id" in str(call_args[0][0])
    assert call_args[0][1]["target_date"] == target_date


def test_get_games_for_date_nba_api_primary(runner):
    """Test _get_games_for_date uses NBA API when available."""
    target_date = date(2023, 10, 25)
    api_games = [{"game_id": "g1", "home_team_id": 1, "away_team_id": 2}]

    with patch.object(runner, "_get_games_from_nba_api", return_value=api_games):
        games = runner._get_games_for_date(target_date)

    assert len(games) == 1
    assert games[0]["game_id"] == "g1"


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
    """No 'Out' players in injury report → all players returned."""
    target_date = date(2023, 10, 25)
    players = [{"player_id": 1, "player_name": "Test Player"}]

    conn = mock_engine.connect.return_value.__enter__.return_value

    # Single query returns no rows (no 'Out' player_ids)
    mock_result = MagicMock()
    mock_result.__iter__.return_value = []
    conn.execute.return_value = mock_result

    with caplog.at_level(logging.INFO):
        result = runner._filter_injured_players(players, target_date)

    assert len(result) == 1
    assert "No 'Out' players found" in caplog.text


def test_filter_injured_players_success(runner, mock_engine):
    """Player with matching player_id in 'Out' list is filtered."""
    target_date = date(2023, 10, 25)
    players = [
        {"player_id": 1, "player_name": "Healthy Player"},
        {"player_id": 2, "player_name": "Injured Player"},
    ]

    conn = mock_engine.connect.return_value.__enter__.return_value

    # Query returns player_id=2 as 'Out'
    mock_result = MagicMock()
    row1 = MagicMock()
    row1.__getitem__ = lambda self, idx: 2  # row[0] returns player_id 2
    mock_result.__iter__.return_value = [row1]
    conn.execute.return_value = mock_result

    result = runner._filter_injured_players(players, target_date)

    assert len(result) == 1
    assert result[0]["player_name"] == "Healthy Player"


def test_get_current_lines_empty(runner):
    assert runner._get_current_lines([], ["pts"]).empty


def test_get_current_lines_success(runner, mock_engine):
    games = [{"game_id": "g1"}]
    stats = ["pts"]

    # Mock pd.read_sql — new query returns bookmaker column (dropped during sharpest-book selection)
    with patch("src.models.daily_runner.pd.read_sql") as mock_read_sql:
        mock_read_sql.return_value = pd.DataFrame(
            {
                "player_id": [1],
                "game_id": ["g1"],
                "bookmaker": ["fanduel"],
                "market_key": ["player_points"],
                "line": [20.5],
                "over_odds": [-110],
                "under_odds": [-110],
            }
        )

        df = runner._get_current_lines(games, stats)

        assert len(df) == 1
        assert df.iloc[0]["line"] == 20.5
        # bookmaker column is dropped after sharpest-book selection
        assert "bookmaker" not in df.columns
        mock_read_sql.assert_called_once()


def test_calculate_edges_with_samples(runner):
    """Test edge calculation using MC samples (primary path)."""
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
            "market_key": ["player_points"],
            "line": [20.0],
            "over_odds": [-110],
            "under_odds": [-110],
        }
    )

    # Create MC samples: ~50% over 20.0
    rng = np.random.default_rng(42)
    samples = rng.normal(20.0, 5.0, size=10000)
    samples_dict = {(1, "g1", "pts"): samples}

    result = runner._calculate_edges(preds_df, lines_df, samples_dict=samples_dict)

    assert "over_prob" in result.columns
    assert "over_edge" in result.columns

    # With mean=20, line=20, prob_over should be ~0.50
    assert abs(result.iloc[0]["over_prob"] - 0.5) < 0.05
    # Implied prob of -110 is ~52.4%, so edge should be negative
    assert result.iloc[0]["over_edge"] < 0


def test_calculate_edges_quantile_fallback(runner):
    """Test edge calculation falls back to quantile interpolation when no samples."""
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
            "market_key": ["player_points"],
            "line": [20.0],
            "over_odds": [-110],
            "under_odds": [-110],
        }
    )

    # No samples_dict → quantile interpolation fallback
    result = runner._calculate_edges(preds_df, lines_df, samples_dict=None)

    assert "over_prob" in result.columns
    # line 20.0 matches q50 exactly → 50% probability via interpolation
    assert result.iloc[0]["over_prob"] == 0.5


def test_run_for_date_flow(runner):
    """Test full run_for_date flow returns (DataFrame, dict) tuple."""
    target_date = date(2023, 10, 25)

    with (
        patch.object(runner, "_get_games_for_date") as mock_get_games,
        patch.object(runner, "_get_players_for_games") as mock_get_players,
        patch.object(runner, "_filter_injured_players") as mock_filter_injuries,
        patch.object(runner, "_build_features_df") as mock_build_features,
        patch.object(runner, "_get_current_lines") as mock_get_lines,
        patch.object(runner, "_calculate_edges") as mock_calc_edges,
        patch.object(runner, "_enrich_predictions") as mock_enrich,
    ):
        mock_get_games.return_value = [{"game_id": "g1"}]
        mock_get_players.return_value = [{"player_id": 1}]
        mock_filter_injuries.return_value = [
            {"player_id": 1, "game_id": "g1", "player_name": "P1", "team_id": 10, "opponent_id": 20}
        ]

        # Mock feature building
        mock_build_features.return_value = pd.DataFrame(
            {"player_id": [1], "game_id": ["g1"], "feature_a": [1.0]}
        )

        # Mock batch predictor — returns (predictions_list, samples_dict)
        pred_row = {
            "player_id": 1, "game_id": "g1", "stat": "pts",
            "pred_mean": 20.0, "pred_median": 20.0,
            "pred_q10": 10, "pred_q25": 15, "pred_q50": 20, "pred_q75": 25, "pred_q90": 30,
        }
        samples = np.random.default_rng(42).normal(20.0, 5.0, size=100)
        runner.predictor.predict_batch_for_date.return_value = (
            [pred_row],
            {(1, "g1", "pts"): samples},
        )

        # Mock enrich and edge calc
        enriched_df = pd.DataFrame([pred_row])
        mock_enrich.return_value = enriched_df
        mock_get_lines.return_value = pd.DataFrame({"dummy": [1]})
        mock_calc_edges.return_value = pd.DataFrame({"final": [1]})

        result_df, result_samples = runner.run_for_date(target_date, stats=["pts"])

        assert len(result_df) == 1
        assert result_df.iloc[0]["final"] == 1
        assert isinstance(result_samples, dict)

        mock_get_games.assert_called_once_with(target_date)
        mock_get_players.assert_called_once()
        mock_filter_injuries.assert_called_once()
        mock_build_features.assert_called_once()
        runner.predictor.predict_batch_for_date.assert_called_once()
        mock_get_lines.assert_called_once()
        mock_calc_edges.assert_called_once()


def test_run_for_date_no_features(runner, caplog):
    """Test returns empty tuple when features can't be built."""
    with (
        patch.object(runner, "_get_games_for_date", return_value=[{"game_id": "g1"}]),
        patch.object(runner, "_get_players_for_games", return_value=[{"player_id": 1}]),
        patch.object(runner, "_filter_injured_players", return_value=[
            {"player_id": 1, "game_id": "g1", "player_name": "P1", "team_id": 10}
        ]),
        patch.object(runner, "_build_features_df", return_value=pd.DataFrame()),
    ):
        result_df, result_samples = runner.run_for_date(date(2023, 10, 25))

        assert result_df.empty
        assert result_samples == {}
        # Batch predictor should not be called
        runner.predictor.predict_batch_for_date.assert_not_called()


def test_run_for_date_no_players(runner, caplog):
    """Test returns empty tuple when no players after injury filter."""
    with (
        patch.object(runner, "_get_games_for_date", return_value=[{"game_id": "g1"}]),
        patch.object(runner, "_get_players_for_games", return_value=[{"player_id": 1}]),
        patch.object(runner, "_filter_injured_players", return_value=[]),
    ):
        result_df, result_samples = runner.run_for_date(date(2023, 10, 25))

        assert result_df.empty
        assert result_samples == {}


def test_build_features_df(runner):
    """Test _build_features_df aggregates features for multiple players."""
    players = [
        {"player_id": 1, "player_name": "P1", "game_id": "g1", "team_id": 10},
        {"player_id": 2, "player_name": "P2", "game_id": "g1", "team_id": 20},
    ]
    target_date = date(2023, 10, 25)

    runner.feature_store.get_player_game_features.side_effect = [
        {"feature_a": 1.0, "feature_b": 2.0},
        {"feature_a": 3.0, "feature_b": 4.0},
    ]

    result = runner._build_features_df(players, target_date)

    assert len(result) == 2
    assert list(result["player_id"]) == [1, 2]
    assert list(result["feature_a"]) == [1.0, 3.0]


def test_enrich_predictions(runner):
    """Test _enrich_predictions adds opponent_id from players list."""
    predictions_df = pd.DataFrame({
        "player_id": [1, 2],
        "game_id": ["g1", "g1"],
        "stat": ["pts", "pts"],
    })
    players = [
        {"player_id": 1, "game_id": "g1", "opponent_id": 20},
        {"player_id": 2, "game_id": "g1", "opponent_id": 10},
    ]

    result = runner._enrich_predictions(predictions_df, players)

    assert list(result["opponent_id"]) == [20, 10]
