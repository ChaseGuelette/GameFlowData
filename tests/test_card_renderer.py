"""Tests for social media card renderer and theme utilities."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.social.theme import (
    get_best_side,
    get_confidence_label,
    get_edge_color,
    get_edge_tier,
    get_star_count,
    GREEN,
    YELLOW,
    SLATE,
    create_placeholder_headshot,
)


# ---------------------------------------------------------------------------
# Theme utility tests
# ---------------------------------------------------------------------------


class TestGetEdgeTier:
    def test_high_edge(self):
        assert get_edge_tier(0.07) == "high"
        assert get_edge_tier(0.10) == "high"
        assert get_edge_tier(0.15) == "high"

    def test_medium_edge(self):
        assert get_edge_tier(0.05) == "medium"
        assert get_edge_tier(0.06) == "medium"
        assert get_edge_tier(0.069) == "medium"

    def test_low_edge(self):
        assert get_edge_tier(0.04) == "low"
        assert get_edge_tier(0.01) == "low"
        assert get_edge_tier(0.0) == "low"

    def test_negative_edge_uses_abs(self):
        assert get_edge_tier(-0.08) == "high"
        assert get_edge_tier(-0.05) == "medium"
        assert get_edge_tier(-0.02) == "low"

    def test_none_edge(self):
        assert get_edge_tier(None) == "low"

    def test_nan_edge(self):
        assert get_edge_tier(float("nan")) == "low"


class TestGetEdgeColor:
    def test_returns_correct_colors(self):
        assert get_edge_color(0.10) == GREEN
        assert get_edge_color(0.06) == YELLOW
        assert get_edge_color(0.02) == SLATE


class TestGetConfidenceLabel:
    def test_labels(self):
        assert get_confidence_label(0.10) == "Strong Edge"
        assert get_confidence_label(0.06) == "High Confidence"
        assert get_confidence_label(0.02) == "Lean"


class TestGetStarCount:
    """Star formula: min(5, max(1, ceil(abs(edge) * 50)))"""

    def test_minimum_one_star(self):
        assert get_star_count(0.0) == 1
        assert get_star_count(0.005) == 1

    def test_edge_cases(self):
        # ceil(0.02 * 50) = ceil(1.0) = 1
        assert get_star_count(0.02) == 1
        # ceil(0.03 * 50) = ceil(1.5) = 2
        assert get_star_count(0.03) == 2
        # ceil(0.05 * 50) = ceil(2.5) = 3
        assert get_star_count(0.05) == 3
        # ceil(0.07 * 50) = ceil(3.5) = 4
        assert get_star_count(0.07) == 4
        # ceil(0.09 * 50) = ceil(4.5) = 5
        assert get_star_count(0.09) == 5

    def test_maximum_five_stars(self):
        assert get_star_count(0.20) == 5
        assert get_star_count(1.0) == 5

    def test_negative_edge(self):
        assert get_star_count(-0.07) == 4

    def test_none_edge(self):
        assert get_star_count(None) == 1


class TestGetBestSide:
    def test_over_wins(self):
        pick = {"over_edge": 0.08, "under_edge": 0.03}
        direction, edge = get_best_side(pick)
        assert direction == "OVER"
        assert edge == 0.08

    def test_under_wins(self):
        pick = {"over_edge": 0.02, "under_edge": 0.09}
        direction, edge = get_best_side(pick)
        assert direction == "UNDER"
        assert edge == 0.09

    def test_tie_goes_to_over(self):
        pick = {"over_edge": 0.05, "under_edge": 0.05}
        direction, edge = get_best_side(pick)
        assert direction == "OVER"

    def test_missing_edges(self):
        pick = {}
        direction, edge = get_best_side(pick)
        assert direction == "OVER"
        assert edge == 0

    def test_none_edges(self):
        pick = {"over_edge": None, "under_edge": None}
        direction, edge = get_best_side(pick)
        assert direction == "OVER"
        assert edge == 0


class TestPlaceholderHeadshot:
    def test_creates_rgba_image(self):
        img = create_placeholder_headshot(100)
        assert img.mode == "RGBA"
        assert img.size == (100, 100)

    def test_default_size(self):
        img = create_placeholder_headshot()
        assert img.size == (200, 200)


# ---------------------------------------------------------------------------
# Card renderer tests (image dimensions, no DB needed)
# ---------------------------------------------------------------------------

SAMPLE_PICK = {
    "player_id": 201566,
    "player_name": "Russell Westbrook",
    "stat": "pts",
    "line": 25.5,
    "over_edge": 0.08,
    "under_edge": 0.02,
    "pred_q50": 28.3,
    "pred_q10": 18.1,
    "pred_q90": 38.5,
    "game_time": "2026-02-18T00:00:00Z",
    "feat_opp_abbrev": "LAL",
}

SAMPLE_BET = {
    "player_id": 201566,
    "player_name": "Russell Westbrook",
    "stat_type": "pts",
    "line": 25.5,
    "bet_direction": "over",
    "status": "won",
    "pnl": 15.50,
    "actual_value": 31,
}

SAMPLE_SUMMARY = {
    "bets_won": 5,
    "bets_lost": 2,
    "bets_push": 0,
    "total_pnl": 47.30,
    "total_bets": 7,
}


def _mock_headshot_cache():
    """Create a HeadshotCache that returns placeholders without HTTP."""
    cache = MagicMock()
    cache.get.return_value = create_placeholder_headshot(200)
    return cache


class TestPickCardRenderer:
    def test_square_dimensions(self):
        from src.social.card_renderer import PickCardRenderer

        renderer = PickCardRenderer(headshot_cache=_mock_headshot_cache())
        img = renderer.render(SAMPLE_PICK, date(2026, 2, 18), fmt="square")
        assert img.size == (1080, 1080)

    def test_story_dimensions(self):
        from src.social.card_renderer import PickCardRenderer

        renderer = PickCardRenderer(headshot_cache=_mock_headshot_cache())
        img = renderer.render(SAMPLE_PICK, date(2026, 2, 18), fmt="story")
        assert img.size == (1080, 1920)

    def test_rgb_mode(self):
        from src.social.card_renderer import PickCardRenderer

        renderer = PickCardRenderer(headshot_cache=_mock_headshot_cache())
        img = renderer.render(SAMPLE_PICK, date(2026, 2, 18))
        assert img.mode == "RGB"


class TestSlateCardRenderer:
    def test_square_dimensions(self):
        from src.social.card_renderer import SlateCardRenderer

        renderer = SlateCardRenderer(headshot_cache=_mock_headshot_cache())
        img = renderer.render([SAMPLE_PICK] * 5, date(2026, 2, 18), fmt="square")
        assert img.size == (1080, 1080)

    def test_story_dimensions(self):
        from src.social.card_renderer import SlateCardRenderer

        renderer = SlateCardRenderer(headshot_cache=_mock_headshot_cache())
        img = renderer.render([SAMPLE_PICK] * 3, date(2026, 2, 18), fmt="story")
        assert img.size == (1080, 1920)

    def test_limits_to_max_rows(self):
        from src.social.card_renderer import SlateCardRenderer

        renderer = SlateCardRenderer(headshot_cache=_mock_headshot_cache())
        # Pass 10 picks, square limits to 5
        img = renderer.render([SAMPLE_PICK] * 10, date(2026, 2, 18), fmt="square")
        assert img.size == (1080, 1080)

    def test_empty_picks(self):
        from src.social.card_renderer import SlateCardRenderer

        renderer = SlateCardRenderer(headshot_cache=_mock_headshot_cache())
        img = renderer.render([], date(2026, 2, 18))
        assert img.size == (1080, 1080)


class TestResultsCardRenderer:
    def test_square_dimensions(self):
        from src.social.card_renderer import ResultsCardRenderer

        renderer = ResultsCardRenderer()
        img = renderer.render(
            [SAMPLE_BET] * 5, SAMPLE_SUMMARY, date(2026, 2, 17)
        )
        assert img.size == (1080, 1080)

    def test_story_dimensions(self):
        from src.social.card_renderer import ResultsCardRenderer

        renderer = ResultsCardRenderer()
        img = renderer.render(
            [SAMPLE_BET] * 3, SAMPLE_SUMMARY, date(2026, 2, 17), fmt="story"
        )
        assert img.size == (1080, 1920)

    def test_no_bets(self):
        from src.social.card_renderer import ResultsCardRenderer

        renderer = ResultsCardRenderer()
        img = renderer.render([], None, date(2026, 2, 17))
        assert img.size == (1080, 1080)

    def test_with_performance_footer(self):
        from src.social.card_renderer import ResultsCardRenderer

        perf = {"win_rate": 0.582, "roi": 0.031, "total_bets": 150}
        renderer = ResultsCardRenderer()
        img = renderer.render(
            [SAMPLE_BET], SAMPLE_SUMMARY, date(2026, 2, 17), performance=perf
        )
        assert img.size == (1080, 1080)


# ---------------------------------------------------------------------------
# HeadshotCache tests (mocked HTTP)
# ---------------------------------------------------------------------------

class TestHeadshotCache:
    def test_returns_placeholder_on_failure(self, tmp_path):
        from src.social.card_renderer import HeadshotCache

        cache = HeadshotCache(cache_dir=tmp_path)
        with patch("src.social.card_renderer.requests.get") as mock_get:
            mock_get.side_effect = Exception("network error")
            img = cache.get(999999)
        assert img.mode == "RGBA"
        assert img.size == (200, 200)

    def test_caches_to_disk(self, tmp_path):
        from src.social.card_renderer import HeadshotCache

        # Create a fake PNG in memory
        from PIL import Image
        import io

        fake = Image.new("RGBA", (100, 100), (255, 0, 0, 255))
        buf = io.BytesIO()
        fake.save(buf, "PNG")
        png_bytes = buf.getvalue()

        cache = HeadshotCache(cache_dir=tmp_path)
        with patch("src.social.card_renderer.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.content = png_bytes
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            img = cache.get(12345)

        assert img.mode == "RGBA"
        assert (tmp_path / "12345.png").exists()

        # Second call should use cache (no HTTP)
        with patch("src.social.card_renderer.requests.get") as mock_get2:
            img2 = cache.get(12345)
            mock_get2.assert_not_called()
