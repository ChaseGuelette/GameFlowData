#!/usr/bin/env python3
"""
NBA CDN Fallback Scraper
========================
Uses cdn.nba.com endpoints (which power nba.com itself) to fetch game data
when stats.nba.com is blocked or rate-limited.

Populates:
- team_game_stats: Basic team stats per game
- player_game_stats: Traditional player box scores

Does NOT populate (requires stats.nba.com):
- player_game_advanced_stats: Advanced player metrics
- team_game_stats advanced columns: offensive_rating, pace, etc.

Usage:
    python nba_cdn_scraper.py                          # Scrape all missing games
    python nba_cdn_scraper.py --date 2026-02-19        # Scrape specific date
    python nba_cdn_scraper.py --date-range 2026-02-13 2026-02-19  # Date range
"""

import argparse
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text

from src.db.client import get_engine

# =============================================================================
# Configuration
# =============================================================================

CDN_BASE = "https://cdn.nba.com/static/json"
SCHEDULE_URL = f"{CDN_BASE}/staticData/scheduleLeagueV2.json"
BOXSCORE_URL = f"{CDN_BASE}/liveData/boxscore/boxscore_{{game_id}}.json"

CDN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
}

DEFAULT_SEASON_ID = "22025"
DEFAULT_SEASON = "2025-26"
REQUEST_DELAY = 0.5  # seconds between CDN requests (be polite)

# NBA team abbreviation -> team_id mapping
TEAM_ABBREV_MAP = {
    "ATL": 1610612737, "BOS": 1610612738, "BKN": 1610612751, "CHA": 1610612766,
    "CHI": 1610612741, "CLE": 1610612739, "DAL": 1610612742, "DEN": 1610612743,
    "DET": 1610612765, "GSW": 1610612744, "HOU": 1610612745, "IND": 1610612754,
    "LAC": 1610612746, "LAL": 1610612747, "MEM": 1610612763, "MIA": 1610612748,
    "MIL": 1610612749, "MIN": 1610612750, "NOP": 1610612740, "NYK": 1610612752,
    "OKC": 1610612760, "ORL": 1610612753, "PHI": 1610612755, "PHX": 1610612756,
    "POR": 1610612757, "SAC": 1610612758, "SAS": 1610612759, "TOR": 1610612761,
    "UTA": 1610612762, "WAS": 1610612764,
}

# Reverse map: team_id -> abbreviation
TEAM_ID_TO_ABBREV = {v: k for k, v in TEAM_ABBREV_MAP.items()}


# =============================================================================
# CDN Data Fetching
# =============================================================================


def fetch_schedule() -> dict:
    """Fetch the full NBA schedule from CDN."""
    resp = requests.get(SCHEDULE_URL, headers=CDN_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_boxscore(game_id: str) -> dict | None:
    """Fetch box score for a single game from CDN. Returns None on failure."""
    url = BOXSCORE_URL.format(game_id=game_id)
    try:
        resp = requests.get(url, headers=CDN_HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        print(f"    CDN returned {resp.status_code} for {game_id}")
        return None
    except Exception as e:
        print(f"    CDN error for {game_id}: {e}")
        return None


def get_games_by_date(schedule: dict) -> dict[str, list[dict]]:
    """
    Parse schedule into {date_str: [game_dicts]} for all competitive games.
    Includes regular season (002), Play-In tournament (005), and Playoffs (004).
    Excludes pre-season (001) and All-Star (003).
    date_str format: 'YYYY-MM-DD'
    """
    # Game ID prefixes we care about
    INCLUDED_PREFIXES = ("002", "004", "005")

    games_by_date = {}
    dates = schedule.get("leagueSchedule", {}).get("gameDates", [])

    for d in dates:
        raw_date = d.get("gameDate", "")
        # Format: "02/19/2026 00:00:00" -> "2026-02-19"
        try:
            dt = datetime.strptime(raw_date.split(" ")[0], "%m/%d/%Y")
            date_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

        # Include regular season (002), playoffs (004), play-in (005); skip pre-season/all-star
        final_games = [
            g for g in d.get("games", [])
            if g.get("gameId", "").startswith(INCLUDED_PREFIXES)
            and g.get("gameStatus", 0) == 3  # Final only
        ]

        if final_games:
            games_by_date[date_str] = final_games

    return games_by_date


# =============================================================================
# Minutes Parsing
# =============================================================================


def parse_iso_minutes(minutes_str) -> float:
    """Parse ISO 8601 duration (PT30M41.80S) to float minutes."""
    if not minutes_str or minutes_str == "PT00M00.00S":
        return 0.0

    if isinstance(minutes_str, int | float):
        return float(minutes_str)

    minutes_str = str(minutes_str)

    match = re.match(r"PT(\d+)M([\d.]+)S", minutes_str)
    if match:
        mins = int(match.group(1))
        secs = float(match.group(2))
        return round(mins + secs / 60, 3)

    return 0.0


# =============================================================================
# Data Transformation: CDN -> DB Schema
# =============================================================================


def build_matchup(team_tricode: str, opp_tricode: str, is_home: bool) -> str:
    """Build matchup string like 'CHA vs. HOU' (home) or 'HOU @ CHA' (away)."""
    if is_home:
        return f"{team_tricode} vs. {opp_tricode}"
    else:
        return f"{team_tricode} @ {opp_tricode}"


def determine_wl(team_score: int, opp_score: int) -> str:
    """Determine W/L from scores."""
    return "W" if team_score > opp_score else "L"


def transform_team_stats(game: dict, game_date: str) -> list[dict]:
    """Transform CDN box score into team_game_stats rows (one per team)."""
    home = game["homeTeam"]
    away = game["awayTeam"]
    game_id = game["gameId"]

    rows = []
    for team_data, opp_data, is_home in [(home, away, True), (away, home, False)]:
        stats = team_data.get("statistics", {})
        team_tricode = team_data["teamTricode"]
        opp_tricode = opp_data["teamTricode"]

        row = {
            "season_id": DEFAULT_SEASON_ID,
            "team_id": team_data["teamId"],
            "team_abbreviation": team_tricode,
            "team_name": f"{team_data['teamCity']} {team_data['teamName']}",
            "game_id": game_id,
            "game_date": game_date,
            "team_matchup": build_matchup(team_tricode, opp_tricode, is_home),
            "team_wl": determine_wl(team_data["score"], opp_data["score"]),
            "team_min": round(parse_iso_minutes(stats.get("minutes", "PT240M00.00S"))),
            "team_pts": stats.get("points", team_data["score"]),
            "team_fgm": stats.get("fieldGoalsMade", 0),
            "team_fga": stats.get("fieldGoalsAttempted", 0),
            "team_fg_pct": stats.get("fieldGoalsPercentage", 0.0),
            "team_fg3m": stats.get("threePointersMade", 0),
            "team_fg3a": stats.get("threePointersAttempted", 0),
            "team_fg3_pct": stats.get("threePointersPercentage", 0.0),
            "team_ftm": stats.get("freeThrowsMade", 0),
            "team_fta": stats.get("freeThrowsAttempted", 0),
            "team_ft_pct": stats.get("freeThrowsPercentage", 0.0),
            "team_oreb": stats.get("reboundsOffensive", 0),
            "team_dreb": stats.get("reboundsDefensive", 0),
            "team_reb": stats.get("reboundsTotal", 0),
            "team_ast": stats.get("assists", 0),
            "team_stl": stats.get("steals", 0),
            "team_blk": stats.get("blocks", 0),
            "team_tov": stats.get("turnoversTotal", stats.get("turnovers", 0)),
            "team_pf": stats.get("foulsPersonal", 0),
            "team_plus_minus": team_data["score"] - opp_data["score"],
            "season": DEFAULT_SEASON,
            "opponent_id": opp_data["teamId"],
        }
        rows.append(row)

    return rows


def transform_player_stats(game: dict, game_date: str) -> list[dict]:
    """Transform CDN box score into player_game_stats rows."""
    home = game["homeTeam"]
    away = game["awayTeam"]
    game_id = game["gameId"]

    rows = []
    for team_data, opp_data, is_home in [(home, away, True), (away, home, False)]:
        team_tricode = team_data["teamTricode"]
        opp_tricode = opp_data["teamTricode"]
        matchup = build_matchup(team_tricode, opp_tricode, is_home)
        wl = determine_wl(team_data["score"], opp_data["score"])

        for player in team_data.get("players", []):
            stats = player.get("statistics", {})
            minutes_float = parse_iso_minutes(stats.get("minutes", "PT00M00.00S"))

            # Determine DNP: inactive players or 0 minutes
            played = player.get("played", 0)
            status = player.get("status", "")
            is_dnp = played == 0 or status == "INACTIVE"

            # Comment for DNP players
            comment = None
            if status == "INACTIVE":
                comment = "DNP - Inactive"
            elif played == 0 and status == "ACTIVE":
                comment = "DNP - Coach's Decision"

            row = {
                "season_id": DEFAULT_SEASON_ID,
                "player_id": player["personId"],
                "game_id": game_id,
                "game_date": game_date,
                "matchup": matchup,
                "wl": wl,
                "min": round(minutes_float),
                "fgm": stats.get("fieldGoalsMade", 0),
                "fga": stats.get("fieldGoalsAttempted", 0),
                "fg_pct": stats.get("fieldGoalsPercentage", 0.0),
                "fg3m": stats.get("threePointersMade", 0),
                "fg3a": stats.get("threePointersAttempted", 0),
                "fg3_pct": stats.get("threePointersPercentage", 0.0),
                "ftm": stats.get("freeThrowsMade", 0),
                "fta": stats.get("freeThrowsAttempted", 0),
                "ft_pct": stats.get("freeThrowsPercentage", 0.0),
                "oreb": stats.get("reboundsOffensive", 0),
                "dreb": stats.get("reboundsDefensive", 0),
                "reb": stats.get("reboundsTotal", 0),
                "ast": stats.get("assists", 0),
                "stl": stats.get("steals", 0),
                "blk": stats.get("blocks", 0),
                "tov": stats.get("turnovers", 0),
                "pf": stats.get("foulsPersonal", 0),
                "pts": stats.get("points", 0),
                "plus_minus": int(stats.get("plusMinusPoints", 0)) if stats.get("plusMinusPoints") is not None else 0,
                "video_available": 1,
                "team_id": team_data["teamId"],
                "comment": comment,
                "did_not_play": is_dnp,
                "started": player.get("starter") == "1",
            }
            rows.append(row)

    return rows


# =============================================================================
# Database Operations
# =============================================================================


def get_existing_game_ids(engine) -> set[str]:
    """Get game IDs already in both team_game_stats and player_game_stats."""
    team_ids = set()
    player_ids = set()

    try:
        result = pd.read_sql("SELECT DISTINCT game_id FROM team_game_stats", engine)
        team_ids = set(result["game_id"].unique())
    except Exception:
        pass

    try:
        result = pd.read_sql("SELECT DISTINCT game_id FROM player_game_stats", engine)
        player_ids = set(result["game_id"].unique())
    except Exception:
        pass

    # A game is "complete" only if it's in both tables
    return team_ids & player_ids


def ensure_players_exist(engine, player_rows: list[dict], boxscore_game: dict):
    """Ensure all players exist in the players table."""
    # Build player_id -> name mapping from the CDN box score
    player_map = {}
    for team_key in ["homeTeam", "awayTeam"]:
        for p in boxscore_game.get(team_key, {}).get("players", []):
            pid = p["personId"]
            name = f"{p.get('firstName', '')} {p.get('familyName', '')}".strip()
            player_map[pid] = name

    player_ids = list(player_map.keys())
    if not player_ids:
        return

    # Check which exist
    placeholders = ",".join([f":id{i}" for i in range(len(player_ids))])
    params = {f"id{i}": pid for i, pid in enumerate(player_ids)}

    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT player_id FROM players WHERE player_id IN ({placeholders})"),  # nosec
            params,
        )
        existing_ids = {row[0] for row in result}

    missing = {pid: name for pid, name in player_map.items() if pid not in existing_ids}

    if missing:
        print(f"    Adding {len(missing)} new player(s) to players table...")
        with engine.begin() as conn:
            for pid, name in missing.items():
                conn.execute(
                    text("INSERT INTO players (player_id, player_name) VALUES (:id, :name) ON CONFLICT (player_id) DO NOTHING"),
                    {"id": pid, "name": name},
                )


def insert_team_stats(engine, rows: list[dict]) -> int:
    """Insert team stats rows, skipping duplicates."""
    if not rows:
        return 0

    df = pd.DataFrame(rows)
    try:
        df.to_sql("team_game_stats", engine, if_exists="append", index=False)
        return len(df)
    except Exception as e:
        if "duplicate key" in str(e) or "UniqueViolation" in str(e):
            print("    (Team stats already exist, skipping)")
            return 0
        raise


def insert_player_stats(engine, rows: list[dict]) -> int:
    """Insert player stats rows, skipping duplicates."""
    if not rows:
        return 0

    df = pd.DataFrame(rows)
    try:
        df.to_sql("player_game_stats", engine, if_exists="append", index=False)
        return len(df)
    except Exception as e:
        if "duplicate key" in str(e) or "UniqueViolation" in str(e):
            print("    (Player stats already exist, skipping)")
            return 0
        raise


# =============================================================================
# Main Scraping Logic
# =============================================================================


def scrape_games_cdn(
    engine,
    target_dates: list[str] | None = None,
    scrape_all_missing: bool = False,
) -> tuple[int, int]:
    """
    Scrape game data from CDN for specified dates or all missing games.

    Args:
        engine: Database engine
        target_dates: List of date strings (YYYY-MM-DD) to scrape
        scrape_all_missing: If True, scrape all games not yet in DB

    Returns:
        (games_scraped, games_failed)
    """
    print("\n" + "=" * 60)
    print("NBA CDN FALLBACK SCRAPER")
    print("=" * 60)

    # 1. Fetch schedule
    print("\nFetching NBA schedule from CDN...")
    schedule = fetch_schedule()
    games_by_date = get_games_by_date(schedule)
    print(f"Found {sum(len(g) for g in games_by_date.values())} games (regular season + play-in + playoffs) across {len(games_by_date)} dates")

    # 2. Determine which games to scrape
    existing_ids = get_existing_game_ids(engine)
    print(f"Found {len(existing_ids)} complete games already in database")

    games_to_scrape = []  # list of (date_str, game_dict)

    if target_dates:
        for date_str in target_dates:
            if date_str in games_by_date:
                for g in games_by_date[date_str]:
                    if g["gameId"] not in existing_ids:
                        games_to_scrape.append((date_str, g))
                    else:
                        print(f"  Skipping {g['gameId']} ({date_str}) - already in DB")
            else:
                print(f"  No games found for {date_str}")
    elif scrape_all_missing:
        for date_str in sorted(games_by_date.keys()):
            for g in games_by_date[date_str]:
                if g["gameId"] not in existing_ids:
                    games_to_scrape.append((date_str, g))

    if not games_to_scrape:
        print("\nNo new games to scrape!")
        return 0, 0

    print(f"\nWill scrape {len(games_to_scrape)} games")

    # Also check which games are in team_game_stats but missing from player_game_stats
    try:
        result = pd.read_sql("SELECT DISTINCT game_id FROM team_game_stats", engine)
        team_only_ids = set(result["game_id"].unique())
    except Exception:
        team_only_ids = set()

    try:
        result = pd.read_sql("SELECT DISTINCT game_id FROM player_game_stats", engine)
        player_ids = set(result["game_id"].unique())
    except Exception:
        player_ids = set()

    # 3. Scrape each game
    games_scraped = 0
    games_failed = 0

    for i, (date_str, game_info) in enumerate(games_to_scrape, 1):
        game_id = game_info["gameId"]
        home_tri = game_info.get("homeTeam", {}).get("teamTricode", "???")
        away_tri = game_info.get("awayTeam", {}).get("teamTricode", "???")

        print(f"\n[{i}/{len(games_to_scrape)}] {game_id}: {away_tri} @ {home_tri} ({date_str})")

        # Fetch box score
        boxscore = fetch_boxscore(game_id)
        if not boxscore:
            print("    FAILED - could not fetch box score")
            games_failed += 1
            continue

        game = boxscore["game"]

        # Verify game is final
        if game.get("gameStatus") != 3:
            print(f"    Skipping - game status is {game.get('gameStatus')} (not final)")
            continue

        try:
            # Ensure players exist
            ensure_players_exist(engine, [], game)

            # Insert team stats (skip if already in team_game_stats)
            if game_id not in team_only_ids:
                team_rows = transform_team_stats(game, date_str)
                team_count = insert_team_stats(engine, team_rows)
                print(f"    Team stats: {team_count} rows")
            else:
                print("    Team stats: already exist")

            # Insert player stats (skip if already in player_game_stats)
            if game_id not in player_ids:
                player_rows = transform_player_stats(game, date_str)
                player_count = insert_player_stats(engine, player_rows)
                print(f"    Player stats: {player_count} rows")
            else:
                print("    Player stats: already exist")

            games_scraped += 1

        except Exception as e:
            print(f"    ERROR: {e}")
            games_failed += 1

        # Rate limiting
        if i < len(games_to_scrape):
            time.sleep(REQUEST_DELAY)

    print(f"\n{'=' * 60}")
    print(f"CDN Scraper complete: {games_scraped} scraped, {games_failed} failed")
    print(f"{'=' * 60}")

    return games_scraped, games_failed


# =============================================================================
# Entry Point
# =============================================================================


def main():
    parser = argparse.ArgumentParser(description="NBA CDN Fallback Scraper")
    parser.add_argument(
        "--date",
        type=str,
        help="Scrape a specific date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Scrape a date range (YYYY-MM-DD YYYY-MM-DD)",
    )
    parser.add_argument(
        "--all-missing",
        action="store_true",
        help="Scrape all missing games for the season",
    )

    args = parser.parse_args()

    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    engine = get_engine()
    print("Database connection established")

    target_dates = None

    if args.date:
        target_dates = [args.date]
    elif args.date_range:
        start = datetime.strptime(args.date_range[0], "%Y-%m-%d")
        end = datetime.strptime(args.date_range[1], "%Y-%m-%d")
        target_dates = []
        current = start
        while current <= end:
            target_dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    elif args.all_missing:
        pass  # Will use scrape_all_missing=True
    else:
        # Default: scrape yesterday
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        target_dates = [yesterday]
        print(f"No date specified, defaulting to yesterday: {yesterday}")

    scraped, failed = scrape_games_cdn(
        engine,
        target_dates=target_dates,
        scrape_all_missing=args.all_missing,
    )

    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
