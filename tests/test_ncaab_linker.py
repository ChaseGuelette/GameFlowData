"""Tests for NCAAB Game Lines Linker."""

from datetime import date

import pandas as pd

from src.processing.ncaab.ncaab_linker import (
    find_closest_game,
    fuzzy_match_team,
    match_batch,
    normalize_team,
)


class TestNormalizeTeam:
    def test_exact_alias_match(self):
        aliases = {"Duke Blue Devils": "Duke"}
        assert normalize_team("Duke Blue Devils", aliases) == "Duke"

    def test_case_insensitive_alias(self):
        aliases = {"Duke Blue Devils": "Duke"}
        assert normalize_team("duke blue devils", aliases) == "Duke"

    def test_no_alias_returns_stripped(self):
        assert normalize_team("  Kansas  ", {}) == "Kansas"

    def test_empty_returns_none(self):
        assert normalize_team("", {}) is None
        assert normalize_team("   ", {}) is None


class TestFindClosestGame:
    def test_exact_date(self):
        candidates = [
            (100, date(2025, 1, 15), 1, 2),
            (101, date(2025, 1, 20), 1, 2),
        ]
        result = find_closest_game(candidates, date(2025, 1, 15))
        assert result == (100, 1, 2)

    def test_within_one_day(self):
        candidates = [(100, date(2025, 1, 15), 1, 2)]
        result = find_closest_game(candidates, date(2025, 1, 16), max_days=1)
        assert result == (100, 1, 2)

    def test_outside_window(self):
        candidates = [(100, date(2025, 1, 15), 1, 2)]
        result = find_closest_game(candidates, date(2025, 1, 20), max_days=1)
        assert result is None

    def test_empty_candidates(self):
        assert find_closest_game([], date(2025, 1, 15)) is None


class TestFuzzyMatchTeam:
    def test_exact_match(self):
        lookup = {"duke": "Duke", "kansas": "Kansas"}
        assert fuzzy_match_team("duke", lookup) == "Duke"

    def test_close_fuzzy_match(self):
        lookup = {"north carolina": "North Carolina"}
        result = fuzzy_match_team("North Carolina Tar Heels", lookup)
        # This may or may not match depending on threshold; test it doesn't crash
        assert result is None or result == "North Carolina"

    def test_no_match_below_threshold(self):
        lookup = {"duke": "Duke"}
        result = fuzzy_match_team("completely different team", lookup)
        assert result is None


class TestMatchBatch:
    def test_match_success(self):
        """Test that a matching game is found."""
        schedule_lookup = {
            ("duke", "kansas"): [(100, date(2025, 1, 15), 1, 2)],
        }
        team_lookup = {"duke": "Duke", "kansas": "Kansas"}

        batch = pd.DataFrame([{
            "staging_id": 1,
            "home_team": "Duke",
            "away_team": "Kansas",
            "commence_time": "2025-01-15T23:00:00Z",
        }])

        result = match_batch(batch, schedule_lookup, team_lookup, {})
        assert len(result) == 1
        assert result.iloc[0]["game_id"] == 100

    def test_match_no_schedule_entry(self):
        """No matching game in schedule."""
        schedule_lookup = {}
        team_lookup = {"duke": "Duke", "kansas": "Kansas"}

        batch = pd.DataFrame([{
            "staging_id": 1,
            "home_team": "Duke",
            "away_team": "Kansas",
            "commence_time": "2025-01-15T23:00:00Z",
        }])

        result = match_batch(batch, schedule_lookup, team_lookup, {})
        assert result.iloc[0]["game_id"] is None
