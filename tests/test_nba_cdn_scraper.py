"""NBA CDN season labeling from individual scheduled games."""

import pytest

from src.scrapers.nba_cdn_scraper import get_games_by_date, transform_player_stats, transform_team_stats


def _boxscore(game_id):
    def team(team_id, code, city, score, player_id):
        return {
            "teamId": team_id,
            "teamTricode": code,
            "teamCity": city,
            "teamName": "Team",
            "score": score,
            "statistics": {},
            "players": [{"personId": player_id, "played": 1, "status": "ACTIVE", "statistics": {}}],
        }

    return {
        "gameId": game_id,
        "homeTeam": team(1, "BOS", "Boston", 110, 10),
        "awayTeam": team(2, "NYK", "New York", 100, 20),
    }


@pytest.mark.parametrize(
    ("game_id", "game_date", "season_id", "label"),
    [
        ("0022500001", "2025-10-21", "22025", "2025-26"),
        ("0042500001", "2026-05-01", "22025", "2025-26"),
        ("0052500001", "2026-04-15", "22025", "2025-26"),
        ("0022600001", "2026-10-20", "22026", "2026-27"),
    ],
)
def test_cdn_rows_use_each_game_season(game_id, game_date, season_id, label):
    boxscore = _boxscore(game_id)
    teams = transform_team_stats(boxscore, game_date)
    players = transform_player_stats(boxscore, game_date)

    assert len(teams) == len(players) == 2
    assert {row["season_id"] for row in teams + players} == {season_id}
    assert {row["season"] for row in teams} == {label}


def test_cdn_transform_rejects_mismatched_game_season():
    with pytest.raises(ValueError, match="season"):
        transform_team_stats(_boxscore("0022500001"), "2026-10-20")
    with pytest.raises(ValueError, match="season"):
        transform_player_stats(_boxscore("0022500001"), "2026-10-20")


def test_cdn_schedule_includes_final_competitive_games_only():
    schedule = {
        "leagueSchedule": {
            "gameDates": [{
                "gameDate": "10/20/2026 00:00:00",
                "games": [
                    {"gameId": game_id, "gameStatus": status}
                    for game_id, status in [
                        ("0012600001", 3),
                        ("0022600001", 3),
                        ("0032600001", 3),
                        ("0042600001", 3),
                        ("0052600001", 3),
                        ("0022600002", 1),
                    ]
                ],
            }]
        }
    }
    games = get_games_by_date(schedule)
    assert [game["gameId"] for game in games["2026-10-20"]] == [
        "0022600001", "0042600001", "0052600001"
    ]
