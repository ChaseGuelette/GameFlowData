import logging
from datetime import UTC, date, datetime
from types import SimpleNamespace
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


def _schedule_row(*, fresh: bool = True):
    return SimpleNamespace(
        _mapping={
            "game_id": "0022600001",
            "home_team_id": 1610612738,
            "away_team_id": 1610612752,
            "game_time": datetime(2026, 10, 20, 23, 30, tzinfo=UTC),
            "is_fresh": fresh,
        }
    )


def test_get_games_for_date_prioritizes_fresh_schedule_db(runner, mock_engine):
    target_date = date(2026, 10, 20)
    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = [_schedule_row()]

    with patch.object(runner, "_get_games_from_cdn") as cdn, patch.object(
        runner, "_get_games_from_nba_api"
    ) as nba_api, patch.object(runner, "_enrich_game_times") as enrich:
        games = runner._get_games_for_date(target_date)

    assert games == [
        {
            "game_id": "0022600001",
            "home_team_id": 1610612738,
            "away_team_id": 1610612752,
            "game_time": datetime(2026, 10, 20, 23, 30, tzinfo=UTC),
        }
    ]
    cdn.assert_not_called()
    nba_api.assert_not_called()
    enrich.assert_not_called()
    query, params = conn.execute.call_args.args
    query_text = str(query)
    assert "nba_game_schedule" in query_text
    assert "observed_at" in query_text
    assert "game_type IN" in query_text
    assert "source_updated_at" in query_text
    assert params == {"target_date": target_date, "max_age_minutes": 48 * 60}


def test_today_schedule_requires_recent_ingest(runner, mock_engine):
    from zoneinfo import ZoneInfo

    target_date = datetime.now(ZoneInfo("America/New_York")).date()
    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = [_schedule_row()]
    runner._get_games_from_schedule_db(target_date)
    assert conn.execute.call_args.args[1]["max_age_minutes"] == 90


def test_get_games_for_date_stale_schedule_falls_back_to_cdn(runner, mock_engine):
    target_date = date(2026, 10, 20)
    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = [_schedule_row(fresh=False)]
    cdn_games = [{"game_id": "0022600002", "home_team_id": 1, "away_team_id": 2, "game_time": None}]

    with patch.object(runner, "_get_games_from_cdn", return_value=cdn_games) as cdn, patch.object(
        runner, "_get_games_from_nba_api"
    ) as nba_api, patch.object(runner, "_enrich_game_times") as enrich:
        games = runner._get_games_for_date(target_date)

    assert games == cdn_games
    cdn.assert_called_once_with(target_date)
    nba_api.assert_not_called()
    enrich.assert_not_called()


def test_get_games_for_date_missing_table_falls_back_to_cdn(runner, mock_engine):
    target_date = date(2026, 10, 20)
    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.side_effect = RuntimeError("relation does not exist")
    cdn_games = [{"game_id": "0022600002", "home_team_id": 1, "away_team_id": 2, "game_time": None}]

    with patch.object(runner, "_get_games_from_cdn", return_value=cdn_games):
        assert runner._get_games_for_date(target_date) == cdn_games


def test_get_games_for_date_missing_schedule_date_checks_cdn(runner, mock_engine):
    target_date = date(2026, 10, 21)
    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = []
    cdn_games = [{"game_id": "0022600002", "home_team_id": 1, "away_team_id": 2, "game_time": datetime(2026, 10, 21, tzinfo=UTC)}]

    with patch.object(runner, "_get_games_from_cdn", return_value=cdn_games) as cdn:
        assert runner._get_games_for_date(target_date) == cdn_games

    cdn.assert_called_once_with(target_date)


def test_get_games_for_date_partially_stale_schedule_uses_complete_cdn(runner, mock_engine):
    target_date = date(2026, 10, 20)
    conn = mock_engine.connect.return_value.__enter__.return_value
    stale = _schedule_row(fresh=False)
    stale._mapping["game_id"] = "0022600002"
    conn.execute.return_value = [_schedule_row(), stale]
    cdn_games = [{"game_id": "0022600003", "home_team_id": 1, "away_team_id": 2, "game_time": datetime(2026, 10, 20, tzinfo=UTC)}]

    with patch.object(runner, "_get_games_from_cdn", return_value=cdn_games) as cdn:
        assert runner._get_games_for_date(target_date) == cdn_games

    cdn.assert_called_once_with(target_date)


def test_get_games_for_date_tbd_schedule_checks_cdn_for_team_updates(runner, mock_engine):
    target_date = date(2026, 10, 20)
    conn = mock_engine.connect.return_value.__enter__.return_value
    tbd = _schedule_row()
    tbd._mapping["home_team_id"] = None
    conn.execute.return_value = [tbd]
    cdn_games = [{"game_id": "0022600001", "home_team_id": 1, "away_team_id": 2, "game_time": datetime(2026, 10, 20, tzinfo=UTC)}]

    with patch.object(runner, "_get_games_from_cdn", return_value=cdn_games):
        assert runner._get_games_for_date(target_date) == cdn_games


def test_get_games_for_date_fails_closed_when_db_and_cdn_unavailable(runner, mock_engine, caplog):
    target_date = date(2026, 10, 20)
    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.side_effect = RuntimeError("DB unavailable")

    with patch.object(runner, "_get_games_from_cdn", return_value=[]), caplog.at_level(logging.WARNING):
        assert runner._get_games_for_date(target_date) == []

    assert "No valid NBA schedule games available" in caplog.text


def test_get_players_for_games_empty(runner):
    assert runner._get_players_for_games([], date(2023, 10, 25)) == []


def test_get_players_for_games_success(runner, mock_engine):
    game_time = datetime(2026, 10, 20, 23, 30, tzinfo=UTC)
    games = [{"game_id": "g1", "home_team_id": 1, "away_team_id": 2, "game_time": game_time}]
    target_date = date(2023, 10, 25)

    # Mock DB result
    mock_result = MagicMock()
    row1 = MagicMock()
    row1._mapping = {"player_id": 101, "player_name": "Player A", "team_id": 1, "avg_min_l5": 30.0}
    row2 = MagicMock()
    row2._mapping = {"player_id": 102, "player_name": "Player B", "team_id": 2, "avg_min_l5": 25.0}
    mock_result.__iter__.return_value = [row1, row2]

    conn = mock_engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = mock_result

    players = runner._get_players_for_games(games, target_date)

    assert len(players) == 2

    p1 = next(p for p in players if p["player_id"] == 101)
    assert p1["game_id"] == "g1"
    assert p1["opponent_id"] == 2
    assert p1["game_time"] == game_time

    p2 = next(p for p in players if p["player_id"] == 102)
    assert p2["game_id"] == "g1"
    assert p2["opponent_id"] == 1
    assert p2["game_time"] == game_time

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
        # bookmaker column is now kept for tracking purposes
        assert "bookmaker" in df.columns
        assert df.iloc[0]["bookmaker"] == "fanduel"
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
    result_sorted = result.sort_values("player_id").reset_index(drop=True)
    assert list(result_sorted["player_id"]) == [1, 2]
    assert list(result_sorted["feature_a"]) == [1.0, 3.0]


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
