"""NBA game-identity season resolution at season and game-type boundaries."""

from datetime import date

import pytest

from src.models.features.nba.season import is_competitive_nba_game_id, resolve_nba_season


@pytest.mark.parametrize(
    ("game_id", "game_date", "season_id", "label"),
    [
        ("0041900406", date(2020, 10, 11), "22019", "2019-20"),
        ("0022500001", date(2025, 10, 21), "22025", "2025-26"),
        ("0022501234", date(2026, 1, 1), "22025", "2025-26"),
        ("0052500001", date(2026, 4, 15), "22025", "2025-26"),
        ("0042500001", date(2026, 6, 1), "22025", "2025-26"),
        ("0022600001", date(2026, 10, 20), "22026", "2026-27"),
        ("0022600002", date(2027, 1, 1), "22026", "2026-27"),
        ("0029900001", date(2100, 1, 1), "22099", "2099-00"),
    ],
)
def test_season_comes_from_game_id_and_target_game_date(game_id, game_date, season_id, label):
    season = resolve_nba_season(game_id, game_date)
    assert season.season_id == season_id
    assert season.label == label


@pytest.mark.parametrize("game_id", ["0012600001", "0032600001", "", "g1", "00226", "00226000x1"])
def test_noncompetitive_or_invalid_game_id_is_excluded(game_id):
    assert not is_competitive_nba_game_id(game_id)
    with pytest.raises(ValueError, match="game ID"):
        resolve_nba_season(game_id, date(2026, 10, 20))


@pytest.mark.parametrize(
    ("game_id", "game_date"),
    [
        ("0022500001", date(2026, 10, 20)),
        ("0022600001", date(2025, 10, 20)),
        ("0022600001", date(2026, 1, 20)),
    ],
)
def test_season_mismatch_fails_closed(game_id, game_date):
    with pytest.raises(ValueError, match="season"):
        resolve_nba_season(game_id, game_date)
