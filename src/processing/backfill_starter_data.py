"""
Backfill starter data from CDN boxscores.

Fetches the CDN boxscore JSON for historical games and extracts the `starter`
field ("1"/"0") for each player. Updates the `started` boolean column in
`player_game_stats`.

For games where CDN data is unavailable, falls back to the minutes proxy
(min >= 20 = starter).

Usage:
    python -m src.processing.backfill_starter_data
    python -m src.processing.backfill_starter_data --season 22025
    python -m src.processing.backfill_starter_data --dry-run
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import requests
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CDN_BASE = "https://cdn.nba.com/static/json"
BOXSCORE_URL = f"{CDN_BASE}/liveData/boxscore/boxscore_{{game_id}}.json"
CDN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
}

STARTER_MINUTES_THRESHOLD = 20
RATE_LIMIT_SECONDS = 0.5


def get_games_needing_backfill(engine, season_id: str | None = None) -> list[str]:
    """Get game IDs where any player has started IS NULL."""
    query = """
        SELECT DISTINCT game_id
        FROM player_game_stats
        WHERE started IS NULL
    """
    if season_id:
        query += f" AND season_id = '{season_id}'"
    query += " ORDER BY game_id"

    with engine.connect() as conn:
        result = conn.execute(text(query))
        game_ids = [row[0] for row in result]
    return game_ids


def fetch_boxscore(game_id: str) -> dict | None:
    """Fetch box score for a single game from CDN."""
    url = BOXSCORE_URL.format(game_id=game_id)
    try:
        resp = requests.get(url, headers=CDN_HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception:
        return None


def extract_starter_data(boxscore: dict) -> dict[int, bool]:
    """Extract player_id -> started mapping from CDN boxscore JSON."""
    game = boxscore.get("game", {})
    starter_map = {}

    for team_key in ["homeTeam", "awayTeam"]:
        for player in game.get(team_key, {}).get("players", []):
            player_id = player.get("personId")
            if player_id:
                starter_map[player_id] = player.get("starter") == "1"

    return starter_map


def backfill_game(engine, game_id: str, starter_map: dict[int, bool]) -> int:
    """Update started column for all players in a game. Returns rows updated."""
    if not starter_map:
        return 0

    updated = 0
    with engine.begin() as conn:
        for player_id, started in starter_map.items():
            result = conn.execute(
                text("""
                    UPDATE player_game_stats
                    SET started = :started
                    WHERE game_id = :game_id AND player_id = :player_id AND started IS NULL
                """),
                {"started": started, "game_id": game_id, "player_id": player_id},
            )
            updated += result.rowcount
    return updated


def fallback_backfill(engine, season_id: str | None = None) -> int:
    """For games still missing starter data, use minutes proxy."""
    query = """
        UPDATE player_game_stats
        SET started = (min >= :threshold)
        WHERE started IS NULL
    """
    if season_id:
        query += f" AND season_id = '{season_id}'"

    with engine.begin() as conn:
        result = conn.execute(text(query), {"threshold": STARTER_MINUTES_THRESHOLD})
        return result.rowcount


def main():
    parser = argparse.ArgumentParser(description="Backfill starter data from CDN boxscores")
    parser.add_argument("--season", type=str, default=None, help="Season ID to backfill (e.g. 22025)")
    parser.add_argument("--dry-run", action="store_true", help="Count games but don't update")
    args = parser.parse_args()

    engine = get_engine()

    game_ids = get_games_needing_backfill(engine, args.season)
    logger.info(f"Found {len(game_ids)} games needing starter backfill")

    if args.dry_run:
        logger.info("Dry run — no updates applied")
        return

    cdn_success = 0
    cdn_fail = 0
    total_updated = 0

    for i, game_id in enumerate(game_ids):
        boxscore = fetch_boxscore(game_id)

        if boxscore:
            starter_map = extract_starter_data(boxscore)
            updated = backfill_game(engine, game_id, starter_map)
            total_updated += updated
            cdn_success += 1
        else:
            cdn_fail += 1

        if (i + 1) % 100 == 0:
            logger.info(
                f"Progress: {i + 1}/{len(game_ids)} games "
                f"(CDN: {cdn_success} ok, {cdn_fail} failed, {total_updated} rows updated)"
            )

        time.sleep(RATE_LIMIT_SECONDS)

    logger.info(
        f"CDN backfill complete: {cdn_success} games from CDN, {cdn_fail} CDN failures, "
        f"{total_updated} rows updated"
    )

    # Fallback for games without CDN data
    fallback_count = fallback_backfill(engine, args.season)
    logger.info(f"Fallback (minutes proxy): {fallback_count} additional rows updated")

    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM player_game_stats WHERE started IS NULL"))
        remaining = result.scalar()
    logger.info(f"Remaining NULL started values: {remaining}")


if __name__ == "__main__":
    main()
