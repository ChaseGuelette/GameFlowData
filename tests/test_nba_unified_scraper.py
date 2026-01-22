"""
Tests for nba_unified_scraper module.
"""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd

# Add src to path for imports
SRC_PATH = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_PATH))

# Mock nba_api before importing the scraper
mock_nba_api = MagicMock()
sys.modules["nba_api"] = mock_nba_api
sys.modules["nba_api.stats"] = mock_nba_api.stats
sys.modules["nba_api.stats.endpoints"] = mock_nba_api.stats.endpoints

from src.scrapers import nba_unified_scraper

# =============================================================================
# Helper Function Tests
# =============================================================================


def test_normalize_for_postgres():
    df = pd.DataFrame({"Player Name": ["LeBron"], "Points Scored": [30]})
    normalized = nba_unified_scraper.normalize_for_postgres(df)
    assert "player_name" in normalized.columns
    assert "points_scored" in normalized.columns
    assert normalized.columns[0] == "player_name"


def test_camel_to_snake():
    assert nba_unified_scraper.camel_to_snake("firstName") == "first_name"
    assert nba_unified_scraper.camel_to_snake("TeamID") == "team_id"
    assert nba_unified_scraper.camel_to_snake("PlayerID") == "player_id"
    assert nba_unified_scraper.camel_to_snake("simple") == "simple"


def test_parse_minutes():
    assert nba_unified_scraper.parse_minutes(30.5) == 30.5
    assert nba_unified_scraper.parse_minutes("30:30") == 30.5
    assert nba_unified_scraper.parse_minutes("PT30M30.00S") == 30.5
    assert nba_unified_scraper.parse_minutes(None) == 0.0
    assert nba_unified_scraper.parse_minutes("") == 0.0
    assert nba_unified_scraper.parse_minutes("invalid") == 0.0


def test_determine_dnp():
    # Playing
    assert nba_unified_scraper.determine_dnp(pd.Series({"minutes": 10, "comment": ""})) is False
    assert nba_unified_scraper.determine_dnp(pd.Series({"minutes": 10, "comment": None})) is False

    # Not Playing
    assert nba_unified_scraper.determine_dnp(pd.Series({"minutes": 0, "comment": "DNP"})) is True
    assert nba_unified_scraper.determine_dnp(pd.Series({"minutes": 0, "comment": ""})) is True
    assert nba_unified_scraper.determine_dnp(pd.Series({"minutes": None, "comment": "Coach's Decision"})) is True


def test_extract_opponent_id():
    abbrev_map = {"LAL": 1, "BOS": 2}

    # Home game vs BOS
    assert nba_unified_scraper.extract_opponent_id("LAL vs. BOS", 1, abbrev_map) == 2

    # Away game @ BOS
    assert nba_unified_scraper.extract_opponent_id("LAL @ BOS", 1, abbrev_map) == 2

    # Invalid formats
    assert nba_unified_scraper.extract_opponent_id("Invalid Matchup", 1, abbrev_map) is None
    assert nba_unified_scraper.extract_opponent_id(None, 1, abbrev_map) is None


# =============================================================================
# Team Game Stats Tests
# =============================================================================


@patch("scrapers.nba_unified_scraper.inspect")
@patch("pandas.read_sql")
def test_get_existing_game_ids(mock_read_sql, mock_inspect):
    engine = Mock()
    mock_inspector = Mock()
    mock_inspect.return_value = mock_inspector

    # Case 1: Table doesn't exist
    mock_inspector.get_table_names.return_value = []
    assert nba_unified_scraper.get_existing_game_ids(engine) == set()

    # Case 2: Table exists
    mock_inspector.get_table_names.return_value = ["team_game_stats"]
    mock_read_sql.return_value = pd.DataFrame({"game_id": ["001", "002"]})
    assert nba_unified_scraper.get_existing_game_ids(engine) == {"001", "002"}


@patch("scrapers.nba_unified_scraper.leaguegamefinder.LeagueGameFinder")
@patch("scrapers.nba_unified_scraper.get_existing_game_ids")
@patch("pandas.DataFrame.to_sql")
def test_scrape_team_game_stats(mock_to_sql, mock_existing_ids, mock_game_finder):
    engine = Mock()
    mock_existing_ids.return_value = {"001"}

    # Setup mock dataframe from API
    mock_games = pd.DataFrame(
        {
            "GAME_ID": ["001", "002"],
            "TEAM_ID": [1, 1],
            "MATCHUP": ["LAL vs. BOS", "LAL @ BOS"],
            "GAME_DATE": ["2024-01-01", "2024-01-03"],
            "WL": ["W", "L"],
            "MIN": [240, 240],
            "PTS": [100, 90],
        }
    )

    mock_finder_instance = mock_game_finder.return_value
    mock_finder_instance.get_data_frames.return_value = [mock_games]

    # Test execution
    added_count = nba_unified_scraper.scrape_team_game_stats(engine, ["2024-25"])

    # Should only process game 002 (001 exists)
    assert added_count == 1

    # Verify to_sql called
    mock_to_sql.assert_called_once()


# =============================================================================
# Advanced Stats Tests
# =============================================================================


def test_transform_player_df():
    input_df = pd.DataFrame(
        {"gameId": ["001"], "personId": [123], "teamId": [1], "firstName": ["LeBron"], "minutes": ["35:00"]}
    )

    result = nba_unified_scraper.transform_player_df(input_df)

    assert "player_id" in result.columns
    assert "first_name" in result.columns
    assert "minutes" in result.columns
    assert result.iloc[0]["minutes"] == 35.0
    assert "created_at" in result.columns


def test_transform_team_adv_df():
    input_df = pd.DataFrame({"gameId": ["001"], "teamId": [1], "offensiveRating": [110.5]})

    result = nba_unified_scraper.transform_team_adv_df(input_df)

    assert "game_id" in result.columns
    assert "offensive_rating" in result.columns


@patch("scrapers.nba_unified_scraper.boxscoreadvancedv3.BoxScoreAdvancedV3")
def test_scrape_single_game_advanced(mock_boxscore):
    mock_instance = mock_boxscore.return_value
    mock_instance.get_data_frames.return_value = [
        pd.DataFrame({"personId": [1], "gameId": ["001"]}),  # Players
        pd.DataFrame({"teamId": [10], "gameId": ["001"]}),  # Teams
    ]

    player_df, team_df = nba_unified_scraper.scrape_single_game_advanced("001")

    assert not player_df.empty
    assert "player_id" in player_df.columns
    assert not team_df.empty
    assert "team_id" in team_df.columns


# =============================================================================
# Traditional Stats Tests
# =============================================================================


def test_transform_player_traditional_df():
    input_df = pd.DataFrame({"personId": [123], "firstName": ["Test"], "minutes": ["30:00"], "comment": ""})

    result = nba_unified_scraper.transform_player_traditional_df(input_df)

    assert "player_id" in result.columns
    assert "first_name" in result.columns
    assert "min" in result.columns  # Renamed from minutes
    assert result.iloc[0]["min"] == 30
    assert not result.iloc[0]["did_not_play"]


@patch("scrapers.nba_unified_scraper.pd.read_sql")
@patch("scrapers.nba_unified_scraper.boxscoretraditionalv3.BoxScoreTraditionalV3")
@patch("scrapers.nba_unified_scraper.ensure_players_exist")
@patch("pandas.DataFrame.to_sql")
def test_scrape_traditional_stats(mock_to_sql, mock_ensure_players, mock_boxscore, mock_read_sql):
    engine = Mock()

    # Mock finding missing games
    mock_read_sql.return_value = pd.DataFrame(
        {
            "game_id": ["001"],
            "team_id": [1],
            "season_id": ["22024"],
            "game_date": [date(2024, 1, 1)],
            "matchup": ["LAL vs BOS"],
            "wl": ["W"],
        }
    )

    # Mock API response
    mock_instance = mock_boxscore.return_value
    mock_instance.player_stats.get_data_frame.return_value = pd.DataFrame(
        {"personId": [123], "teamId": [1], "gameId": ["001"], "minutes": ["30:00"], "points": [20], "turnovers": [2]}
    )

    processed, failed = nba_unified_scraper.scrape_traditional_stats(engine, limit=1)

    assert processed == 1
    assert failed == 0
