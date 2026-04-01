"""
MLB Stats Scraper
=================
Scrapes game schedules and boxscores from the MLB Stats API (statsapi.mlb.com).
Populates: mlb_game_schedule, mlb_player_game_stats_batting, mlb_player_game_stats_pitching, mlb_players, mlb_teams.

Uses the raw API directly (not the statsapi wrapper) for full stat field coverage.

Usage:
    python -m src.scrapers.mlb.mlb_stats_scraper --season 2025
    python -m src.scrapers.mlb.mlb_stats_scraper --season 2025 --schedule-only
    python -m src.scrapers.mlb.mlb_stats_scraper --season 2025 --boxscores-only --limit 50
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBStatsScraper")

BASE_URL = "https://statsapi.mlb.com/api/v1"
RATE_LIMIT_DELAY = 0.3  # seconds between requests (conservative)


class MLBStatsScraper:
    def __init__(self, engine):
        self.engine = engine
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "GameFlowData/1.0"})

    # ------------------------------------------------------------------
    # Schedule
    # ------------------------------------------------------------------

    def scrape_schedule(self, season: int, game_type: str = "R") -> int:
        """Scrape game schedule into mlb_game_schedule.

        Args:
            season: MLB season year (e.g. 2025)
            game_type: 'R' regular, 'P' postseason, 'S' spring training

        Returns:
            Number of new games inserted.
        """
        logger.info(f"Fetching {season} schedule (game_type={game_type})...")

        # MLB regular season typically runs late March through early October
        start_date = f"{season}-02-20"  # Spring training starts early
        end_date = f"{season}-11-15"  # Covers World Series

        url = f"{BASE_URL}/schedule"
        params = {
            "sportId": 1,
            "startDate": start_date,
            "endDate": end_date,
            "gameType": game_type,
            "hydrate": "venue,probablePitcher",
        }

        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Get existing game IDs to track inserts vs updates
        existing = self._get_existing_game_ids()

        games_inserted = 0
        games_updated = 0
        with self.engine.begin() as conn:
            for date_entry in data.get("dates", []):
                for game in date_entry.get("games", []):
                    game_pk = game["gamePk"]

                    status = game.get("status", {}).get("detailedState", "Unknown")
                    home = game.get("teams", {}).get("home", {})
                    away = game.get("teams", {}).get("away", {})
                    venue = game.get("venue", {})

                    # Extract probable pitcher IDs
                    home_pitcher = home.get("probablePitcher", {})
                    away_pitcher = away.get("probablePitcher", {})

                    conn.execute(
                        text("""
                            INSERT INTO mlb_game_schedule
                                (game_id, game_date, season, game_type,
                                 home_team_id, away_team_id, venue_id, venue_name,
                                 home_score, away_score, status, game_time_utc,
                                 probable_pitcher_home_id, probable_pitcher_away_id)
                            VALUES
                                (:game_id, :game_date, :season, :game_type,
                                 :home_team_id, :away_team_id, :venue_id, :venue_name,
                                 :home_score, :away_score, :status, :game_time_utc,
                                 :probable_pitcher_home_id, :probable_pitcher_away_id)
                            ON CONFLICT (game_id) DO UPDATE SET
                                home_score = EXCLUDED.home_score,
                                away_score = EXCLUDED.away_score,
                                status = EXCLUDED.status,
                                probable_pitcher_home_id = COALESCE(EXCLUDED.probable_pitcher_home_id, mlb_game_schedule.probable_pitcher_home_id),
                                probable_pitcher_away_id = COALESCE(EXCLUDED.probable_pitcher_away_id, mlb_game_schedule.probable_pitcher_away_id)
                        """),
                        {
                            "game_id": game_pk,
                            "game_date": game.get("officialDate"),
                            "season": season,
                            "game_type": game.get("gameType", game_type),
                            "home_team_id": home.get("team", {}).get("id"),
                            "away_team_id": away.get("team", {}).get("id"),
                            "venue_id": venue.get("id"),
                            "venue_name": venue.get("name"),
                            "home_score": home.get("score"),
                            "away_score": away.get("score"),
                            "status": status,
                            "game_time_utc": game.get("gameDate"),
                            "probable_pitcher_home_id": home_pitcher.get("id"),
                            "probable_pitcher_away_id": away_pitcher.get("id"),
                        },
                    )
                    if game_pk in existing:
                        games_updated += 1
                    else:
                        games_inserted += 1

        logger.info(f"Schedule: {games_inserted} inserted, {games_updated} updated for {season} season.")
        return games_inserted

    # ------------------------------------------------------------------
    # Boxscores
    # ------------------------------------------------------------------

    def scrape_boxscores(self, game_ids: list[int] | None = None, limit: int | None = None) -> tuple[int, int]:
        """Scrape boxscores for games.

        If game_ids is None, finds all Final games missing boxscore data.
        Auto-inserts new players into mlb_players.

        Returns:
            (processed, failed) counts.
        """
        if game_ids is None:
            game_ids = self._get_games_missing_boxscores()

        if limit:
            game_ids = game_ids[:limit]

        total = len(game_ids)
        if total == 0:
            logger.info("No games need boxscore scraping.")
            return 0, 0

        logger.info(f"Scraping boxscores for {total} games...")

        processed = 0
        failed = 0

        for i, game_id in enumerate(game_ids, 1):
            try:
                logger.info(f"[{i}/{total}] Scraping boxscore for game {game_id}...")
                self._scrape_single_boxscore(game_id)
                processed += 1
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    logger.warning(f"  Game {game_id} not found (404), skipping.")
                else:
                    logger.error(f"  HTTP error for game {game_id}: {e}")
                failed += 1
            except Exception as e:
                logger.error(f"  Error for game {game_id}: {e}")
                failed += 1

            time.sleep(RATE_LIMIT_DELAY)

            # Long pause every 200 games
            if i % 200 == 0:
                logger.info(f"  Pausing 5s after {i} games...")
                time.sleep(5)

        logger.info(f"Boxscores complete: {processed} processed, {failed} failed.")
        return processed, failed

    def _scrape_single_boxscore(self, game_id: int):
        """Fetch and store boxscore for a single game."""
        url = f"{BASE_URL}/game/{game_id}/boxscore"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        teams = data.get("teams", {})

        with self.engine.begin() as conn:
            for side in ("home", "away"):
                team_data = teams.get(side, {})
                team_info = team_data.get("team", {})
                team_id = team_info.get("id")
                if not team_id:
                    continue

                players_dict = team_data.get("players", {})
                batting_order = team_data.get("battingOrder", [])
                pitcher_ids = team_data.get("pitchers", [])

                # Build lineup position lookup: player_id -> position (1-9)
                lineup_map = {}
                for idx, pid in enumerate(batting_order):
                    lineup_map[pid] = idx + 1

                for player_key, player_data in players_dict.items():
                    person = player_data.get("person", {})
                    player_id = person.get("id")
                    if not player_id:
                        continue

                    player_name = person.get("fullName", "Unknown")
                    position = player_data.get("position", {})
                    position_abbrev = position.get("abbreviation", "")

                    # Extract handedness from boxscore player data
                    bat_side = person.get("batSide", {}).get("code")
                    pitch_hand = person.get("pitchHand", {}).get("code")

                    # Ensure player exists
                    self._ensure_player(conn, player_id, player_name, position_abbrev,
                                        bats=bat_side, throws=pitch_hand)

                    stats = player_data.get("stats", {})

                    # Get game metadata
                    game_meta = self._get_game_meta(conn, game_id)
                    if not game_meta:
                        continue
                    game_date = game_meta["game_date"]
                    season = game_meta["season"]

                    # Batting stats
                    batting = stats.get("batting", {})
                    if batting and batting.get("plateAppearances") is not None:
                        pa = _int(batting.get("plateAppearances", 0))
                        did_not_bat = pa == 0 and player_id not in batting_order
                        if not did_not_bat or player_id in batting_order:
                            self._insert_batting(
                                conn, player_id, game_id, game_date, season,
                                team_id, batting, lineup_map.get(player_id),
                                player_id in batting_order,
                            )

                    # Pitching stats
                    pitching = stats.get("pitching", {})
                    if pitching and pitching.get("inningsPitched") is not None:
                        ip_str = pitching.get("inningsPitched", "0")
                        if ip_str and ip_str != "0" and float(ip_str.replace(".", "", 1).lstrip("0") or "0") > 0 or player_id in pitcher_ids:
                            self._insert_pitching(
                                conn, player_id, game_id, game_date, season,
                                team_id, pitching, player_id == pitcher_ids[0] if pitcher_ids else False,
                            )

    def _insert_batting(self, conn, player_id, game_id, game_date, season,
                        team_id, batting, lineup_position, is_starter):
        """Insert a single player's batting stats."""
        ab = _int(batting.get("atBats", 0))
        h = _int(batting.get("hits", 0))
        doubles = _int(batting.get("doubles", 0))
        triples = _int(batting.get("triples", 0))
        hr = _int(batting.get("homeRuns", 0))

        # Compute total bases if not provided
        tb = _int(batting.get("totalBases", 0))
        if tb == 0 and h > 0:
            singles = h - doubles - triples - hr
            tb = singles + 2 * doubles + 3 * triples + 4 * hr

        pa = _int(batting.get("plateAppearances", 0))

        conn.execute(
            text("""
                INSERT INTO mlb_player_game_stats_batting
                    (player_id, game_id, game_date, season, team_id,
                     lineup_position, is_starter, pa, ab, r, h, doubles, triples, hr,
                     rbi, bb, so, sb, cs, hbp, sf, sac, tb, did_not_play)
                VALUES
                    (:player_id, :game_id, :game_date, :season, :team_id,
                     :lineup_position, :is_starter, :pa, :ab, :r, :h, :doubles, :triples, :hr,
                     :rbi, :bb, :so, :sb, :cs, :hbp, :sf, :sac, :tb, :did_not_play)
                ON CONFLICT (player_id, game_id) DO NOTHING
            """),
            {
                "player_id": player_id,
                "game_id": game_id,
                "game_date": game_date,
                "season": season,
                "team_id": team_id,
                "lineup_position": lineup_position,
                "is_starter": is_starter,
                "pa": pa,
                "ab": ab,
                "r": _int(batting.get("runs", 0)),
                "h": h,
                "doubles": doubles,
                "triples": triples,
                "hr": hr,
                "rbi": _int(batting.get("rbi", 0)),
                "bb": _int(batting.get("baseOnBalls", 0)),
                "so": _int(batting.get("strikeOuts", 0)),
                "sb": _int(batting.get("stolenBases", 0)),
                "cs": _int(batting.get("caughtStealing", 0)),
                "hbp": _int(batting.get("hitByPitch", 0)),
                "sf": _int(batting.get("sacFlies", 0)),
                "sac": _int(batting.get("sacBunts", 0)),
                "tb": tb,
                "did_not_play": pa == 0 and not is_starter,
            },
        )

    def _insert_pitching(self, conn, player_id, game_id, game_date, season,
                         team_id, pitching, is_starter):
        """Insert a single player's pitching stats."""
        ip_str = pitching.get("inningsPitched", "0")
        ip_decimal = _parse_ip(ip_str)
        outs = _int(pitching.get("outs", 0))
        if outs == 0 and ip_decimal > 0:
            outs = round(ip_decimal * 3)

        note = pitching.get("note", "")
        is_winner = "(W" in note if note else False
        is_loser = "(L" in note if note else False
        is_save = "(S" in note if note else False

        conn.execute(
            text("""
                INSERT INTO mlb_player_game_stats_pitching
                    (player_id, game_id, game_date, season, team_id,
                     is_starter, is_winner, is_loser, is_save,
                     ip, outs_recorded, h_allowed, r_allowed, er, bb, so,
                     hr_allowed, pitches_thrown, strikes, did_not_play)
                VALUES
                    (:player_id, :game_id, :game_date, :season, :team_id,
                     :is_starter, :is_winner, :is_loser, :is_save,
                     :ip, :outs_recorded, :h_allowed, :r_allowed, :er, :bb, :so,
                     :hr_allowed, :pitches_thrown, :strikes, :did_not_play)
                ON CONFLICT (player_id, game_id) DO NOTHING
            """),
            {
                "player_id": player_id,
                "game_id": game_id,
                "game_date": game_date,
                "season": season,
                "team_id": team_id,
                "is_starter": is_starter,
                "is_winner": is_winner,
                "is_loser": is_loser,
                "is_save": is_save,
                "ip": ip_decimal,
                "outs_recorded": outs,
                "h_allowed": _int(pitching.get("hits", 0)),
                "r_allowed": _int(pitching.get("runs", 0)),
                "er": _int(pitching.get("earnedRuns", 0)),
                "bb": _int(pitching.get("baseOnBalls", 0)),
                "so": _int(pitching.get("strikeOuts", 0)),
                "hr_allowed": _int(pitching.get("homeRuns", 0)),
                "pitches_thrown": _int(pitching.get("pitchesThrown")) or _int(pitching.get("numberOfPitches")),
                "strikes": _int(pitching.get("strikes", 0)),
                "did_not_play": False,
            },
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_player(self, conn, player_id: int, player_name: str, position: str = "",
                        bats: str | None = None, throws: str | None = None):
        """Insert or update player in mlb_players (including handedness)."""
        conn.execute(
            text("""
                INSERT INTO mlb_players (player_id, player_name, primary_position, bats, throws)
                VALUES (:pid, :name, :pos, :bats, :throws)
                ON CONFLICT (player_id) DO UPDATE
                    SET player_name = EXCLUDED.player_name,
                        primary_position = COALESCE(EXCLUDED.primary_position, mlb_players.primary_position),
                        bats = COALESCE(EXCLUDED.bats, mlb_players.bats),
                        throws = COALESCE(EXCLUDED.throws, mlb_players.throws)
            """),
            {
                "pid": player_id,
                "name": player_name,
                "pos": position or None,
                "bats": bats if bats in ("L", "R", "S") else None,
                "throws": throws if throws in ("L", "R") else None,
            },
        )

    def _get_existing_game_ids(self) -> set[int]:
        """Get all game IDs already in mlb_game_schedule."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT game_id FROM mlb_game_schedule"))
            return {row[0] for row in result}

    def _get_games_missing_boxscores(self) -> list[int]:
        """Find Final games that don't have batting stats yet."""
        sql = """
            SELECT s.game_id
            FROM mlb_game_schedule s
            LEFT JOIN mlb_player_game_stats_batting b ON s.game_id = b.game_id
            WHERE s.status = 'Final'
              AND s.game_type = 'R'
              AND b.game_id IS NULL
            ORDER BY s.game_date ASC
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            return [row[0] for row in result]

    def _get_game_meta(self, conn, game_id: int) -> dict | None:
        """Get game_date and season for a game."""
        result = conn.execute(
            text("SELECT game_date, season FROM mlb_game_schedule WHERE game_id = :gid"),
            {"gid": game_id},
        )
        row = result.fetchone()
        if row:
            return {"game_date": row[0], "season": row[1]}
        return None

    def update_scores(self, season: int) -> int:
        """Update scores and statuses for games in a season."""
        logger.info(f"Updating scores for {season} season...")

        start_date = f"{season}-03-01"
        end_date = f"{season}-11-15"

        url = f"{BASE_URL}/schedule"
        params = {
            "sportId": 1,
            "startDate": start_date,
            "endDate": end_date,
            "gameType": "R",
        }

        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        updated = 0
        with self.engine.begin() as conn:
            for date_entry in data.get("dates", []):
                for game in date_entry.get("games", []):
                    home = game.get("teams", {}).get("home", {})
                    away = game.get("teams", {}).get("away", {})
                    status = game.get("status", {}).get("detailedState", "")

                    if status != "Final":
                        continue

                    result = conn.execute(
                        text("""
                            UPDATE mlb_game_schedule
                            SET home_score = :hs, away_score = :as_, status = :status
                            WHERE game_id = :gid AND (home_score IS NULL OR status != 'Final')
                        """),
                        {
                            "gid": game["gamePk"],
                            "hs": home.get("score"),
                            "as_": away.get("score"),
                            "status": status,
                        },
                    )
                    updated += result.rowcount

        logger.info(f"Updated scores for {updated} games.")
        return updated


# ------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------

def _int(val) -> int:
    """Safely convert to int, returning 0 for None/empty."""
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _parse_ip(ip_str: str) -> float:
    """Parse innings pitched string to decimal.

    MLB format: "6.1" means 6 innings + 1 out = 6.333
    """
    if not ip_str:
        return 0.0
    try:
        parts = str(ip_str).split(".")
        innings = int(parts[0])
        if len(parts) > 1:
            outs = int(parts[1])
            return round(innings + outs / 3, 4)
        return float(innings)
    except (ValueError, TypeError):
        return 0.0


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="MLB Stats Scraper")
    parser.add_argument("--season", type=int, default=2025, help="Season year (default: 2025)")
    parser.add_argument("--game-type", type=str, default="R", choices=["R", "P", "S"],
                        help="Game type: R=regular, P=postseason, S=spring (default: R)")
    parser.add_argument("--schedule-only", action="store_true", help="Only scrape schedule")
    parser.add_argument("--boxscores-only", action="store_true", help="Only scrape boxscores")
    parser.add_argument("--update-scores", action="store_true", help="Update scores for existing games")
    parser.add_argument("--limit", type=int, default=None, help="Limit boxscores to N games")

    args = parser.parse_args()

    engine = get_engine()
    scraper = MLBStatsScraper(engine)

    print("=" * 60)
    print("MLB STATS SCRAPER")
    print("=" * 60)
    print(f"Season: {args.season}")
    print(f"Game Type: {args.game_type}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.update_scores:
        scraper.update_scores(args.season)
        return

    if not args.boxscores_only:
        new_games = scraper.scrape_schedule(args.season, args.game_type)
        print(f"Schedule: {new_games} new games added")

    if not args.schedule_only:
        processed, failed = scraper.scrape_boxscores(limit=args.limit)
        print(f"Boxscores: {processed} processed, {failed} failed")

    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
