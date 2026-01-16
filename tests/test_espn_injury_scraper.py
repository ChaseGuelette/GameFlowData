"""
Tests for ESPN Injury Scraper module

Tests cover:
- InjuryRecord dataclass and to_dict conversion
- ESPNInjuryScraper rate limiting, retry logic, and parsing
- InjuryChangeDetector change detection logic
"""

import sys
import time
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "scrapers"))

from espn_injury_scraper import (
    ESPNInjuryScraper,
    InjuryChangeDetector,
    InjuryRecord,
    InjuryStatus,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def sample_injury_record():
    """Create a sample InjuryRecord for testing"""
    return InjuryRecord(
        espn_injury_id="12345",
        espn_player_id="67890",
        player_name="LeBron James",
        team_id="13",
        team_name="Los Angeles Lakers",
        status="out",
        injury_type="knee",
        injury_location="left",
        injury_side="left",
        injury_detail="sprain",
        return_date="2025-01-20",
        date_reported="2025-01-15",
        short_comment="Expected to miss 5 games",
        long_comment="Suffered injury in practice",
        fantasy_status="OUT",
        scrape_timestamp="2025-01-15T12:00:00Z",
    )


@pytest.fixture
def sample_espn_api_response():
    """Create a sample ESPN API response for testing parse_injuries"""
    return {
        "injuries": [
            {
                "id": "13",
                "displayName": "Los Angeles Lakers",
                "injuries": [
                    {
                        "id": "12345",
                        "athlete": {
                            "id": "67890",
                            "displayName": "LeBron James",
                        },
                        "type": {
                            "description": "out",
                        },
                        "details": {
                            "type": "knee",
                            "location": "left",
                            "side": "left",
                            "detail": "sprain",
                            "returnDate": "2025-01-20",
                            "fantasyStatus": {
                                "description": "OUT",
                            },
                        },
                        "date": "2025-01-15",
                        "shortComment": "Expected to miss 5 games",
                        "longComment": "Suffered injury in practice",
                    },
                    {
                        "id": "12346",
                        "athlete": {
                            "id": "67891",
                            "displayName": "Anthony Davis",
                        },
                        "type": {
                            "description": "questionable",
                        },
                        "details": {
                            "type": "ankle",
                            "location": "right",
                            "side": "right",
                            "detail": "soreness",
                            "returnDate": None,
                            "fantasyStatus": {
                                "description": "GTD",
                            },
                        },
                        "date": "2025-01-14",
                        "shortComment": "Game-time decision",
                        "longComment": None,
                    },
                ],
            },
            {
                "id": "1",
                "displayName": "Boston Celtics",
                "injuries": [
                    {
                        "id": "22345",
                        "athlete": {
                            "id": "77890",
                            "displayName": "Jaylen Brown",
                        },
                        "type": {
                            "description": "probable",
                        },
                        "details": {
                            "type": "back",
                            "location": "lower",
                            "side": None,
                            "detail": "tightness",
                            "returnDate": None,
                            "fantasyStatus": None,
                        },
                        "date": "2025-01-15",
                        "shortComment": "Probable to play",
                        "longComment": None,
                    },
                ],
            },
        ]
    }


@pytest.fixture
def scraper():
    """Create a scraper instance with short intervals for testing"""
    return ESPNInjuryScraper(min_request_interval=0.1, max_retries=3, timeout=5)


@pytest.fixture
def change_detector():
    """Create a change detector instance"""
    return InjuryChangeDetector()


# =============================================================================
# InjuryRecord Tests
# =============================================================================


class TestInjuryRecord:
    """Tests for InjuryRecord dataclass"""

    def test_to_dict_returns_all_fields(self, sample_injury_record):
        """Test that to_dict returns all expected fields"""
        result = sample_injury_record.to_dict()

        expected_fields = [
            "espn_injury_id",
            "espn_player_id",
            "player_name",
            "team_id",
            "team_name",
            "status",
            "injury_type",
            "injury_location",
            "injury_side",
            "injury_detail",
            "return_date",
            "date_reported",
            "short_comment",
            "long_comment",
            "fantasy_status",
            "scrape_timestamp",
        ]

        for field in expected_fields:
            assert field in result, f"Missing field: {field}"

    def test_to_dict_values_match_input(self, sample_injury_record):
        """Test that to_dict values match the original record"""
        result = sample_injury_record.to_dict()

        assert result["espn_injury_id"] == "12345"
        assert result["espn_player_id"] == "67890"
        assert result["player_name"] == "LeBron James"
        assert result["team_id"] == "13"
        assert result["team_name"] == "Los Angeles Lakers"
        assert result["status"] == "out"
        assert result["injury_type"] == "knee"
        assert result["injury_location"] == "left"
        assert result["injury_side"] == "left"
        assert result["injury_detail"] == "sprain"
        assert result["return_date"] == "2025-01-20"
        assert result["date_reported"] == "2025-01-15"
        assert result["short_comment"] == "Expected to miss 5 games"
        assert result["long_comment"] == "Suffered injury in practice"
        assert result["fantasy_status"] == "OUT"
        assert result["scrape_timestamp"] == "2025-01-15T12:00:00Z"

    def test_to_dict_with_none_values(self):
        """Test that to_dict handles None values correctly"""
        record = InjuryRecord(
            espn_injury_id="99999",
            espn_player_id="88888",
            player_name="Test Player",
            team_id="1",
            team_name="Test Team",
            status="questionable",
            injury_type=None,
            injury_location=None,
            injury_side=None,
            injury_detail=None,
            return_date=None,
            date_reported="2025-01-15",
            short_comment=None,
            long_comment=None,
            fantasy_status=None,
            scrape_timestamp="2025-01-15T12:00:00Z",
        )

        result = record.to_dict()

        assert result["injury_type"] is None
        assert result["injury_location"] is None
        assert result["injury_side"] is None
        assert result["injury_detail"] is None
        assert result["return_date"] is None
        assert result["short_comment"] is None
        assert result["long_comment"] is None
        assert result["fantasy_status"] is None


# =============================================================================
# InjuryStatus Enum Tests
# =============================================================================


class TestInjuryStatus:
    """Tests for InjuryStatus enum"""

    def test_injury_status_values(self):
        """Test that all injury statuses have correct values"""
        assert InjuryStatus.OUT.value == "out"
        assert InjuryStatus.DOUBTFUL.value == "doubtful"
        assert InjuryStatus.QUESTIONABLE.value == "questionable"
        assert InjuryStatus.PROBABLE.value == "probable"
        assert InjuryStatus.DAY_TO_DAY.value == "day-to-day"
        assert InjuryStatus.ACTIVE.value == "active"

    def test_injury_status_count(self):
        """Test that we have the expected number of statuses"""
        assert len(InjuryStatus) == 6


# =============================================================================
# ESPNInjuryScraper Tests
# =============================================================================


class TestESPNInjuryScraperInit:
    """Tests for ESPNInjuryScraper initialization"""

    def test_default_initialization(self):
        """Test scraper initializes with default values"""
        scraper = ESPNInjuryScraper()

        assert scraper.min_request_interval == 2.0
        assert scraper.max_retries == 3
        assert scraper.timeout == 15
        assert scraper.last_request_time is None

    def test_custom_initialization(self):
        """Test scraper initializes with custom values"""
        scraper = ESPNInjuryScraper(
            min_request_interval=5.0,
            max_retries=5,
            timeout=30,
        )

        assert scraper.min_request_interval == 5.0
        assert scraper.max_retries == 5
        assert scraper.timeout == 30


class TestESPNInjuryScraperRateLimit:
    """Tests for ESPNInjuryScraper._rate_limit()"""

    def test_rate_limit_first_request_no_delay(self, scraper):
        """Test that first request has no delay"""
        start = time.time()
        scraper._rate_limit()
        elapsed = time.time() - start

        # First request should have no delay (less than 50ms overhead)
        assert elapsed < 0.05

    def test_rate_limit_enforces_minimum_interval(self, scraper):
        """Test that rate limit enforces minimum interval between requests"""
        # Make first request
        scraper._rate_limit()

        # Make second request immediately
        start = time.time()
        scraper._rate_limit()
        elapsed = time.time() - start

        # Should have waited at least min_request_interval (0.1s for test)
        assert elapsed >= scraper.min_request_interval - 0.01  # Allow small margin

    def test_rate_limit_no_delay_if_enough_time_passed(self, scraper):
        """Test that no delay if enough time has passed since last request"""
        # Make first request
        scraper._rate_limit()

        # Wait longer than interval
        time.sleep(scraper.min_request_interval + 0.05)

        # Make second request
        start = time.time()
        scraper._rate_limit()
        elapsed = time.time() - start

        # Should have minimal delay since we already waited
        assert elapsed < 0.05


class TestESPNInjuryScraperRetryWithBackoff:
    """Tests for ESPNInjuryScraper._retry_with_backoff()"""

    def test_retry_success_first_attempt(self, scraper):
        """Test successful execution on first attempt"""
        mock_func = Mock(return_value="success")

        result = scraper._retry_with_backoff(mock_func)

        assert result == "success"
        assert mock_func.call_count == 1

    def test_retry_success_after_failures(self, scraper):
        """Test successful execution after initial failures"""
        mock_func = Mock(
            side_effect=[
                requests.RequestException("fail"),
                requests.RequestException("fail"),
                "success",
            ]
        )

        result = scraper._retry_with_backoff(mock_func)

        assert result == "success"
        assert mock_func.call_count == 3

    def test_retry_all_attempts_fail(self, scraper):
        """Test that None is returned after all retries fail"""
        mock_func = Mock(side_effect=requests.RequestException("always fail"))

        result = scraper._retry_with_backoff(mock_func)

        assert result is None
        assert mock_func.call_count == scraper.max_retries

    def test_retry_passes_arguments(self, scraper):
        """Test that arguments are passed to the function"""
        mock_func = Mock(return_value="result")

        result = scraper._retry_with_backoff(mock_func, "arg1", kwarg1="value1")

        assert result == "result"
        mock_func.assert_called_once_with("arg1", kwarg1="value1")


class TestESPNInjuryScraperParseInjuries:
    """Tests for ESPNInjuryScraper.parse_injuries()"""

    def test_parse_injuries_returns_list(self, scraper, sample_espn_api_response):
        """Test that parse_injuries returns a list of InjuryRecord"""
        result = scraper.parse_injuries(sample_espn_api_response)

        assert isinstance(result, list)
        assert len(result) == 3  # 2 Lakers + 1 Celtics
        assert all(isinstance(r, InjuryRecord) for r in result)

    def test_parse_injuries_extracts_correct_data(self, scraper, sample_espn_api_response):
        """Test that parsed data matches input"""
        result = scraper.parse_injuries(sample_espn_api_response)

        # Check first injury (LeBron James)
        lebron = next(r for r in result if r.player_name == "LeBron James")
        assert lebron.espn_injury_id == "12345"
        assert lebron.espn_player_id == "67890"
        assert lebron.team_id == "13"
        assert lebron.team_name == "Los Angeles Lakers"
        assert lebron.status == "out"
        assert lebron.injury_type == "knee"
        assert lebron.injury_location == "left"
        assert lebron.short_comment == "Expected to miss 5 games"
        assert lebron.fantasy_status == "OUT"

        # Check second injury (Anthony Davis)
        ad = next(r for r in result if r.player_name == "Anthony Davis")
        assert ad.espn_injury_id == "12346"
        assert ad.status == "questionable"
        assert ad.injury_type == "ankle"
        assert ad.return_date is None

        # Check third injury (Jaylen Brown)
        jb = next(r for r in result if r.player_name == "Jaylen Brown")
        assert jb.team_name == "Boston Celtics"
        assert jb.status == "probable"
        assert jb.fantasy_status is None  # fantasyStatus was None in response

    def test_parse_injuries_empty_response(self, scraper):
        """Test parse_injuries with empty response"""
        result = scraper.parse_injuries({})
        assert result == []

    def test_parse_injuries_none_response(self, scraper):
        """Test parse_injuries with None response"""
        result = scraper.parse_injuries(None)
        assert result == []

    def test_parse_injuries_missing_injuries_key(self, scraper):
        """Test parse_injuries when injuries key is missing"""
        result = scraper.parse_injuries({"other_data": "value"})
        assert result == []

    def test_parse_injuries_empty_injuries_list(self, scraper):
        """Test parse_injuries with empty injuries list"""
        result = scraper.parse_injuries({"injuries": []})
        assert result == []

    def test_parse_injuries_sets_scrape_timestamp(self, scraper, sample_espn_api_response):
        """Test that all records get the same scrape timestamp"""
        result = scraper.parse_injuries(sample_espn_api_response)

        # All should have the same timestamp
        timestamps = [r.scrape_timestamp for r in result]
        assert len(set(timestamps)) == 1  # All same timestamp


class TestESPNInjuryScraperGetActiveInjuryIds:
    """Tests for ESPNInjuryScraper.get_active_injury_ids()"""

    def test_get_active_injury_ids_returns_set(self, scraper, sample_injury_record):
        """Test that get_active_injury_ids returns a set"""
        injuries = [sample_injury_record]
        result = scraper.get_active_injury_ids(injuries)

        assert isinstance(result, set)

    def test_get_active_injury_ids_extracts_correct_ids(self, scraper):
        """Test that correct injury IDs are extracted"""
        injuries = [
            InjuryRecord(
                espn_injury_id="111",
                espn_player_id="1",
                player_name="Player 1",
                team_id="1",
                team_name="Team A",
                status="out",
                injury_type=None,
                injury_location=None,
                injury_side=None,
                injury_detail=None,
                return_date=None,
                date_reported="2025-01-15",
                short_comment=None,
                long_comment=None,
                fantasy_status=None,
                scrape_timestamp="2025-01-15T12:00:00Z",
            ),
            InjuryRecord(
                espn_injury_id="222",
                espn_player_id="2",
                player_name="Player 2",
                team_id="1",
                team_name="Team A",
                status="questionable",
                injury_type=None,
                injury_location=None,
                injury_side=None,
                injury_detail=None,
                return_date=None,
                date_reported="2025-01-15",
                short_comment=None,
                long_comment=None,
                fantasy_status=None,
                scrape_timestamp="2025-01-15T12:00:00Z",
            ),
            InjuryRecord(
                espn_injury_id="333",
                espn_player_id="3",
                player_name="Player 3",
                team_id="2",
                team_name="Team B",
                status="probable",
                injury_type=None,
                injury_location=None,
                injury_side=None,
                injury_detail=None,
                return_date=None,
                date_reported="2025-01-15",
                short_comment=None,
                long_comment=None,
                fantasy_status=None,
                scrape_timestamp="2025-01-15T12:00:00Z",
            ),
        ]

        result = scraper.get_active_injury_ids(injuries)

        assert result == {"111", "222", "333"}

    def test_get_active_injury_ids_empty_list(self, scraper):
        """Test get_active_injury_ids with empty list"""
        result = scraper.get_active_injury_ids([])
        assert result == set()


# =============================================================================
# InjuryChangeDetector Tests
# =============================================================================


def create_injury_record(
    injury_id: str,
    player_name: str,
    status: str = "out",
    injury_type: str = None,
    return_date: str = None,
) -> InjuryRecord:
    """Helper to create InjuryRecord for testing"""
    return InjuryRecord(
        espn_injury_id=injury_id,
        espn_player_id=f"player_{injury_id}",
        player_name=player_name,
        team_id="1",
        team_name="Test Team",
        status=status,
        injury_type=injury_type,
        injury_location=None,
        injury_side=None,
        injury_detail=None,
        return_date=return_date,
        date_reported="2025-01-15",
        short_comment=None,
        long_comment=None,
        fantasy_status=None,
        scrape_timestamp="2025-01-15T12:00:00Z",
    )


class TestInjuryChangeDetector:
    """Tests for InjuryChangeDetector.detect_changes()"""

    def test_detect_changes_returns_dict_with_required_keys(self, change_detector):
        """Test that detect_changes returns dict with new, updated, resolved keys"""
        result = change_detector.detect_changes([], [])

        assert isinstance(result, dict)
        assert "new" in result
        assert "updated" in result
        assert "resolved" in result

    def test_detect_changes_new_injuries(self, change_detector):
        """Test detection of new injuries"""
        previous = []
        current = [
            create_injury_record("1", "Player A"),
            create_injury_record("2", "Player B"),
        ]

        result = change_detector.detect_changes(previous, current)

        assert len(result["new"]) == 2
        assert len(result["updated"]) == 0
        assert len(result["resolved"]) == 0

        new_ids = {inj.espn_injury_id for inj in result["new"]}
        assert new_ids == {"1", "2"}

    def test_detect_changes_resolved_injuries(self, change_detector):
        """Test detection of resolved injuries"""
        previous = [
            create_injury_record("1", "Player A"),
            create_injury_record("2", "Player B"),
        ]
        current = []

        result = change_detector.detect_changes(previous, current)

        assert len(result["new"]) == 0
        assert len(result["updated"]) == 0
        assert len(result["resolved"]) == 2

        resolved_ids = {inj.espn_injury_id for inj in result["resolved"]}
        assert resolved_ids == {"1", "2"}

    def test_detect_changes_status_update(self, change_detector):
        """Test detection of status changes"""
        previous = [create_injury_record("1", "Player A", status="out")]
        current = [create_injury_record("1", "Player A", status="questionable")]

        result = change_detector.detect_changes(previous, current)

        assert len(result["new"]) == 0
        assert len(result["updated"]) == 1
        assert len(result["resolved"]) == 0
        assert result["updated"][0].espn_injury_id == "1"
        assert result["updated"][0].status == "questionable"

    def test_detect_changes_return_date_update(self, change_detector):
        """Test detection of return date changes"""
        previous = [create_injury_record("1", "Player A", return_date="2025-01-20")]
        current = [create_injury_record("1", "Player A", return_date="2025-01-25")]

        result = change_detector.detect_changes(previous, current)

        assert len(result["updated"]) == 1
        assert result["updated"][0].return_date == "2025-01-25"

    def test_detect_changes_injury_type_update(self, change_detector):
        """Test detection of injury type changes"""
        previous = [create_injury_record("1", "Player A", injury_type="knee")]
        current = [create_injury_record("1", "Player A", injury_type="ankle")]

        result = change_detector.detect_changes(previous, current)

        assert len(result["updated"]) == 1
        assert result["updated"][0].injury_type == "ankle"

    def test_detect_changes_no_meaningful_change(self, change_detector):
        """Test that unchanged injuries are not flagged as updated"""
        injury = create_injury_record("1", "Player A", status="out", injury_type="knee")
        previous = [injury]
        # Create identical injury
        current = [create_injury_record("1", "Player A", status="out", injury_type="knee")]

        result = change_detector.detect_changes(previous, current)

        assert len(result["new"]) == 0
        assert len(result["updated"]) == 0
        assert len(result["resolved"]) == 0

    def test_detect_changes_mixed_scenario(self, change_detector):
        """Test complex scenario with new, updated, and resolved injuries"""
        previous = [
            create_injury_record("1", "Player A", status="out"),  # Will be updated
            create_injury_record("2", "Player B", status="out"),  # Will be resolved
            create_injury_record("3", "Player C", status="questionable"),  # Unchanged
        ]
        current = [
            create_injury_record("1", "Player A", status="questionable"),  # Status changed
            create_injury_record("3", "Player C", status="questionable"),  # Unchanged
            create_injury_record("4", "Player D", status="out"),  # New
        ]

        result = change_detector.detect_changes(previous, current)

        # Check new
        assert len(result["new"]) == 1
        assert result["new"][0].espn_injury_id == "4"

        # Check updated
        assert len(result["updated"]) == 1
        assert result["updated"][0].espn_injury_id == "1"

        # Check resolved
        assert len(result["resolved"]) == 1
        assert result["resolved"][0].espn_injury_id == "2"

    def test_detect_changes_empty_both(self, change_detector):
        """Test with empty previous and current lists"""
        result = change_detector.detect_changes([], [])

        assert result["new"] == []
        assert result["updated"] == []
        assert result["resolved"] == []


# =============================================================================
# Integration-style Tests (without network)
# =============================================================================


class TestESPNInjuryScraperIntegration:
    """Integration-style tests combining multiple methods"""

    def test_parse_and_get_ids_workflow(self, scraper, sample_espn_api_response):
        """Test typical workflow: parse then extract IDs"""
        injuries = scraper.parse_injuries(sample_espn_api_response)
        ids = scraper.get_active_injury_ids(injuries)

        assert len(ids) == 3
        assert "12345" in ids  # LeBron
        assert "12346" in ids  # Anthony Davis
        assert "22345" in ids  # Jaylen Brown

    def test_change_detection_with_parsed_data(
        self, scraper, change_detector, sample_espn_api_response
    ):
        """Test change detection with realistic parsed data"""
        # Parse initial injuries
        initial_injuries = scraper.parse_injuries(sample_espn_api_response)

        # Simulate modified response (LeBron recovered, new injury added)
        modified_response = {
            "injuries": [
                {
                    "id": "13",
                    "displayName": "Los Angeles Lakers",
                    "injuries": [
                        # LeBron removed (recovered)
                        {
                            "id": "12346",
                            "athlete": {
                                "id": "67891",
                                "displayName": "Anthony Davis",
                            },
                            "type": {
                                "description": "probable",  # Changed from questionable
                            },
                            "details": {
                                "type": "ankle",
                                "location": "right",
                                "side": "right",
                                "detail": "soreness",
                                "returnDate": None,
                                "fantasyStatus": {
                                    "description": "GTD",
                                },
                            },
                            "date": "2025-01-14",
                            "shortComment": "Upgraded to probable",
                            "longComment": None,
                        },
                    ],
                },
                {
                    "id": "1",
                    "displayName": "Boston Celtics",
                    "injuries": [
                        {
                            "id": "22345",
                            "athlete": {
                                "id": "77890",
                                "displayName": "Jaylen Brown",
                            },
                            "type": {
                                "description": "probable",
                            },
                            "details": {
                                "type": "back",
                                "location": "lower",
                                "side": None,
                                "detail": "tightness",
                                "returnDate": None,
                                "fantasyStatus": None,
                            },
                            "date": "2025-01-15",
                            "shortComment": "Probable to play",
                            "longComment": None,
                        },
                        {
                            "id": "22346",  # New injury
                            "athlete": {
                                "id": "77891",
                                "displayName": "Jayson Tatum",
                            },
                            "type": {
                                "description": "questionable",
                            },
                            "details": {
                                "type": "wrist",
                                "location": "right",
                                "side": "right",
                                "detail": "soreness",
                                "returnDate": None,
                                "fantasyStatus": None,
                            },
                            "date": "2025-01-16",
                            "shortComment": "New injury",
                            "longComment": None,
                        },
                    ],
                },
            ]
        }

        updated_injuries = scraper.parse_injuries(modified_response)

        changes = change_detector.detect_changes(initial_injuries, updated_injuries)

        # LeBron should be resolved (removed from list)
        resolved_names = [inj.player_name for inj in changes["resolved"]]
        assert "LeBron James" in resolved_names

        # Tatum should be new
        new_names = [inj.player_name for inj in changes["new"]]
        assert "Jayson Tatum" in new_names

        # Anthony Davis should be updated (status changed)
        updated_names = [inj.player_name for inj in changes["updated"]]
        assert "Anthony Davis" in updated_names
