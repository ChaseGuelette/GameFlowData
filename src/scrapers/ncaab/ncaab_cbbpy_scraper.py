"""
NCAAB CBBpy Box Score & Schedule Scraper
==========================================
Uses CBBpy to pull ESPN box scores and schedules into:
  - ncaab_game_schedule
  - ncaab_teams
  - ncaab_team_box_scores

CBBpy returns player-level box scores; this scraper aggregates them
to team level and computes Four Factors + possession estimates.

Usage:
    python -m src.scrapers.ncaab.ncaab_cbbpy_scraper --season 2025
    python -m src.scrapers.ncaab.ncaab_cbbpy_scraper --season 2025 --schedule-only
    python -m src.scrapers.ncaab.ncaab_cbbpy_scraper --date 2025-01-15
    python -m src.scrapers.ncaab.ncaab_cbbpy_scraper --backfill --season 2025
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).resolve().parents[3]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("NCAABCBBpy")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Delay between ESPN requests (be polite)
REQUEST_DELAY = 0.3


def _estimate_possessions(fga, oreb, tov, fta):
    """Standard NCAA possession estimate: FGA - OREB + TOV + 0.44 * FTA."""
    return fga - oreb + tov + 0.44 * fta


def _safe_div(num, den):
    """Safe division returning None on zero denominator."""
    if den is None or den == 0:
        return None
    return num / den


class NCAABCBBpyScraper:
    def __init__(self, engine):
        self.engine = engine
        # Lazy import to avoid import errors when cbbpy not installed
        import cbbpy.mens_scraper as cbb
        self.cbb = cbb

    def scrape_schedule(self, season: int) -> int:
        """Scrape full season schedule from ESPN via CBBpy.

        Args:
            season: Season end year (e.g., 2025 for 2024-25 season)

        Returns:
            Count of new games inserted.
        """
        logger.info(f"Scraping schedule for {season-1}-{season} season...")

        try:
            info_df, box_df, pbp_df = self.cbb.get_games_season(season=season, info=True, box=False, pbp=False)
        except Exception as e:
            logger.error(f"CBBpy get_games_season failed: {e}")
            return 0

        if info_df is None or info_df.empty:
            logger.warning("No schedule data returned.")
            return 0

        logger.info(f"Got {len(info_df)} games from CBBpy")

        # Upsert teams first
        team_map = self._upsert_teams(info_df, season)

        # Upsert schedule
        new_count = self._upsert_schedule(info_df, team_map, season)
        logger.info(f"Inserted/updated {new_count} games in schedule.")
        return new_count

    def scrape_date(self, date_str: str) -> int:
        """Scrape all games for a specific date.

        Returns:
            Count of box scores stored.
        """
        logger.info(f"Scraping games for {date_str}...")

        try:
            game_ids = self.cbb.get_game_ids(date_str)
        except Exception as e:
            logger.error(f"CBBpy get_game_ids failed: {e}")
            return 0

        if not game_ids:
            logger.info("No games found for this date.")
            return 0

        logger.info(f"Found {len(game_ids)} games")
        return self._scrape_boxscores(game_ids)

    def backfill_boxscores(self, season: int, delay: float = REQUEST_DELAY) -> int:
        """Backfill box scores for all completed games missing from DB.

        Args:
            season: Season end year
            delay: Seconds between requests

        Returns:
            Total box scores stored.
        """
        # Get games in schedule that are final but missing from box scores
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT gs.game_id
                FROM ncaab_game_schedule gs
                WHERE gs.season = :season AND gs.is_final = TRUE
                  AND NOT EXISTS (
                    SELECT 1 FROM ncaab_team_box_scores bs
                    WHERE bs.game_id = gs.game_id
                  )
                ORDER BY gs.game_date
            """), {"season": season})
            missing_ids = [row[0] for row in result]

        if not missing_ids:
            logger.info("No missing box scores.")
            return 0

        logger.info(f"Backfilling {len(missing_ids)} games...")
        return self._scrape_boxscores(missing_ids, delay=delay)

    def _scrape_boxscores(self, game_ids: list, delay: float = REQUEST_DELAY) -> int:
        """Scrape and store box scores for a list of game IDs."""
        stored = 0
        errors = 0

        for i, game_id in enumerate(game_ids):
            try:
                success = self._scrape_single_boxscore(int(game_id))
                if success:
                    stored += 1
                if (i + 1) % 50 == 0:
                    logger.info(f"  Progress: {i+1}/{len(game_ids)} (stored: {stored}, errors: {errors})")
            except Exception as e:
                errors += 1
                logger.warning(f"  Error on game {game_id}: {e}")

            time.sleep(delay)

        logger.info(f"Boxscore scrape complete. Stored: {stored}, Errors: {errors}")
        return stored

    def _scrape_single_boxscore(self, game_id: int) -> bool:
        """Scrape a single game's box score and store team-level aggregates."""
        try:
            info_df, box_df, _ = self.cbb.get_game(game_id, info=True, box=True, pbp=False)
        except Exception as e:
            logger.warning(f"CBBpy get_game({game_id}) failed: {e}")
            return False

        if box_df is None or box_df.empty:
            return False
        if info_df is None or info_df.empty:
            return False

        # Extract game info
        game_info = info_df.iloc[0]
        home_team = str(game_info.get("home_team", ""))

        # Ensure game exists in schedule
        self._ensure_game_in_schedule(game_id, game_info)

        # Aggregate player box scores to team level
        # CBBpy box_df has a 'team' column identifying which team each player belongs to
        team_col = "team" if "team" in box_df.columns else "TEAM"
        teams_in_box = box_df[team_col].unique()

        if len(teams_in_box) < 2:
            logger.warning(f"Game {game_id}: fewer than 2 teams in boxscore")
            return False

        # Determine which team is home vs away
        rows_to_insert = []
        team_stats = {}

        for team_name in teams_in_box:
            team_df = box_df[box_df[team_col] == team_name]
            stats = self._aggregate_team_stats(team_df)
            team_stats[team_name] = stats

        # Match teams to home/away
        for team_name, stats in team_stats.items():
            is_home = self._fuzzy_team_match(team_name, home_team)
            opp_name = [t for t in team_stats if t != team_name]
            opp_stats = team_stats[opp_name[0]] if opp_name else {}

            # Compute derived stats
            poss = _estimate_possessions(
                stats.get("fga", 0) or 0,
                stats.get("oreb", 0) or 0,
                stats.get("tov", 0) or 0,
                stats.get("fta", 0) or 0,
            )
            opp_poss = _estimate_possessions(
                opp_stats.get("fga", 0) or 0,
                opp_stats.get("oreb", 0) or 0,
                opp_stats.get("tov", 0) or 0,
                opp_stats.get("fta", 0) or 0,
            )

            fga = stats.get("fga", 0) or 0
            fg3m = stats.get("fg3m", 0) or 0
            fgm = stats.get("fgm", 0) or 0
            efg_pct = _safe_div(fgm + 0.5 * fg3m, fga) if fga > 0 else None
            tov_pct = _safe_div(stats.get("tov", 0), poss) if poss > 0 else None
            oreb_val = stats.get("oreb", 0) or 0
            opp_dreb = opp_stats.get("dreb", 0) or 0
            orb_pct = _safe_div(oreb_val, oreb_val + opp_dreb) if (oreb_val + opp_dreb) > 0 else None
            ft_rate = _safe_div(stats.get("fta", 0), fga) if fga > 0 else None

            # Opponent Four Factors
            opp_fga = opp_stats.get("fga", 0) or 0
            opp_fg3m = opp_stats.get("fg3m", 0) or 0
            opp_fgm = opp_stats.get("fgm", 0) or 0
            opp_efg = _safe_div(opp_fgm + 0.5 * opp_fg3m, opp_fga) if opp_fga > 0 else None
            opp_tov_pct = _safe_div(opp_stats.get("tov", 0), opp_poss) if opp_poss > 0 else None
            opp_oreb = opp_stats.get("oreb", 0) or 0
            dreb_val = stats.get("dreb", 0) or 0
            opp_drb_pct = _safe_div(opp_oreb, opp_oreb + dreb_val) if (opp_oreb + dreb_val) > 0 else None
            opp_ft_rate_val = _safe_div(opp_stats.get("fta", 0), opp_fga) if opp_fga > 0 else None

            rows_to_insert.append({
                "game_id": game_id,
                "team_name": team_name,
                "is_home": is_home,
                "pts": stats.get("pts", 0),
                "fgm": fgm, "fga": fga,
                "fg3m": fg3m, "fg3a": stats.get("fg3a", 0),
                "ftm": stats.get("ftm", 0), "fta": stats.get("fta", 0),
                "oreb": oreb_val, "dreb": dreb_val,
                "reb": stats.get("reb", 0),
                "ast": stats.get("ast", 0),
                "stl": stats.get("stl", 0),
                "blk": stats.get("blk", 0),
                "tov": stats.get("tov", 0),
                "pf": stats.get("pf", 0),
                "poss_est": round(poss, 2),
                "efg_pct": round(efg_pct, 4) if efg_pct is not None else None,
                "tov_pct": round(tov_pct, 4) if tov_pct is not None else None,
                "orb_pct": round(orb_pct, 4) if orb_pct is not None else None,
                "ft_rate": round(ft_rate, 4) if ft_rate is not None else None,
                "opp_pts": opp_stats.get("pts", 0),
                "opp_poss_est": round(opp_poss, 2),
                "opp_efg_pct": round(opp_efg, 4) if opp_efg is not None else None,
                "opp_tov_pct": round(opp_tov_pct, 4) if opp_tov_pct is not None else None,
                "opp_drb_pct": round(opp_drb_pct, 4) if opp_drb_pct is not None else None,
                "opp_ft_rate": round(opp_ft_rate_val, 4) if opp_ft_rate_val is not None else None,
            })

        # Insert into DB
        self._insert_box_scores(rows_to_insert, game_info)
        return True

    def _aggregate_team_stats(self, team_df: pd.DataFrame) -> dict:
        """Aggregate player-level stats to team totals."""
        # CBBpy column names (may vary slightly)
        col_map = {
            "pts": ["PTS", "pts", "points"],
            "fgm": ["FGM", "fgm", "field_goals_made"],
            "fga": ["FGA", "fga", "field_goals_attempted"],
            "fg3m": ["3PM", "fg3m", "three_point_field_goals_made", "3PT_M"],
            "fg3a": ["3PA", "fg3a", "three_point_field_goals_attempted", "3PT_A"],
            "ftm": ["FTM", "ftm", "free_throws_made"],
            "fta": ["FTA", "fta", "free_throws_attempted"],
            "oreb": ["OREB", "oreb", "offensive_rebounds", "OR"],
            "dreb": ["DREB", "dreb", "defensive_rebounds", "DR"],
            "reb": ["REB", "reb", "rebounds", "TOT"],
            "ast": ["AST", "ast", "assists"],
            "stl": ["STL", "stl", "steals"],
            "blk": ["BLK", "blk", "blocks"],
            "tov": ["TO", "tov", "turnovers"],
            "pf": ["PF", "pf", "personal_fouls"],
        }

        result = {}
        for stat, candidates in col_map.items():
            for col_name in candidates:
                if col_name in team_df.columns:
                    result[stat] = pd.to_numeric(team_df[col_name], errors="coerce").sum()
                    break
            if stat not in result:
                result[stat] = 0

        return result

    def _fuzzy_team_match(self, team_name: str, target: str) -> bool:
        """Check if team_name matches target using simple substring matching."""
        t1 = team_name.lower().strip()
        t2 = target.lower().strip()
        return t1 == t2 or t1 in t2 or t2 in t1

    def _upsert_teams(self, info_df: pd.DataFrame, season: int) -> dict:
        """Extract unique teams and upsert into ncaab_teams.

        Returns:
            {team_name: team_id} mapping
        """
        home_teams = info_df["home_team"].dropna().unique().tolist() if "home_team" in info_df.columns else []
        away_teams = info_df["away_team"].dropna().unique().tolist() if "away_team" in info_df.columns else []
        all_teams = list(set(home_teams + away_teams))

        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                for team_name in all_teams:
                    if not team_name or not team_name.strip():
                        continue
                    cur.execute("""
                        INSERT INTO ncaab_teams (team_name)
                        VALUES (%s)
                        ON CONFLICT (team_name) DO NOTHING
                    """, (team_name.strip(),))
            conn.commit()
        finally:
            conn.close()

        # Build lookup
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT team_name, team_id FROM ncaab_teams"))
            return {row[0]: row[1] for row in result}

    def _upsert_schedule(self, info_df: pd.DataFrame, team_map: dict, season: int) -> int:
        """Upsert game schedule rows."""
        inserted = 0
        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                for _, row in info_df.iterrows():
                    game_id = row.get("game_id")
                    if game_id is None:
                        continue

                    home_team = str(row.get("home_team", "")).strip()
                    away_team = str(row.get("away_team", "")).strip()
                    home_team_id = team_map.get(home_team)
                    away_team_id = team_map.get(away_team)

                    home_score = row.get("home_score")
                    away_score = row.get("away_score")
                    is_final = home_score is not None and away_score is not None

                    game_date = None
                    game_day = row.get("game_day") or row.get("date")
                    if game_day is not None:
                        try:
                            if isinstance(game_day, str):
                                game_date = datetime.strptime(game_day, "%Y-%m-%d").date()
                            else:
                                game_date = pd.Timestamp(game_day).date()
                        except (ValueError, TypeError):
                            pass

                    neutral = bool(row.get("neutral_site", False)) if "neutral_site" in row.index else False

                    # Determine season type
                    season_type = "regular"
                    if game_date and game_date.month == 3 and game_date.day >= 15:
                        season_type = "tournament"
                    elif game_date and game_date.month == 4:
                        season_type = "tournament"

                    cur.execute("""
                        INSERT INTO ncaab_game_schedule
                        (game_id, game_date, season, season_type,
                         home_team_id, away_team_id,
                         home_score, away_score, is_final, neutral_site)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (game_id) DO UPDATE SET
                            home_score = EXCLUDED.home_score,
                            away_score = EXCLUDED.away_score,
                            is_final = EXCLUDED.is_final
                    """, (
                        int(game_id), game_date, season, season_type,
                        home_team_id, away_team_id,
                        int(home_score) if home_score is not None else None,
                        int(away_score) if away_score is not None else None,
                        is_final, neutral,
                    ))
                    inserted += 1

            conn.commit()
        finally:
            conn.close()
        return inserted

    def _ensure_game_in_schedule(self, game_id: int, game_info) -> None:
        """Make sure this game exists in the schedule table."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM ncaab_game_schedule WHERE game_id = :gid"),
                {"gid": game_id},
            )
            if result.fetchone():
                return

        # Insert basic record
        home_team = str(game_info.get("home_team", ""))
        away_team = str(game_info.get("away_team", ""))

        # Ensure teams exist
        team_map = self._upsert_teams(
            pd.DataFrame([{"home_team": home_team, "away_team": away_team}]),
            season=datetime.utcnow().year,
        )

        game_date = None
        game_day = game_info.get("game_day") or game_info.get("date")
        if game_day is not None:
            try:
                if isinstance(game_day, str):
                    game_date = datetime.strptime(game_day, "%Y-%m-%d").date()
                else:
                    game_date = pd.Timestamp(game_day).date()
            except (ValueError, TypeError):
                pass

        season = game_date.year if game_date and game_date.month >= 9 else (game_date.year if game_date else datetime.utcnow().year)

        home_score = game_info.get("home_score")
        away_score = game_info.get("away_score")

        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ncaab_game_schedule
                    (game_id, game_date, season, home_team_id, away_team_id,
                     home_score, away_score, is_final)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id) DO NOTHING
                """, (
                    game_id, game_date, season,
                    team_map.get(home_team), team_map.get(away_team),
                    int(home_score) if home_score is not None else None,
                    int(away_score) if away_score is not None else None,
                    home_score is not None,
                ))
            conn.commit()
        finally:
            conn.close()

    def _insert_box_scores(self, rows: list[dict], game_info) -> None:
        """Insert team box score rows into ncaab_team_box_scores."""
        game_date = None
        game_day = game_info.get("game_day") or game_info.get("date")
        if game_day is not None:
            try:
                if isinstance(game_day, str):
                    game_date = datetime.strptime(game_day, "%Y-%m-%d").date()
                else:
                    game_date = pd.Timestamp(game_day).date()
            except (ValueError, TypeError):
                pass

        season = game_date.year if game_date and game_date.month >= 9 else (game_date.year if game_date else datetime.utcnow().year)

        # Resolve team_ids
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT team_name, team_id FROM ncaab_teams"))
            team_map = {r[0]: r[1] for r in result}

        db_conn = self.engine.raw_connection()
        try:
            with db_conn.cursor() as cur:
                for row in rows:
                    team_id = team_map.get(row["team_name"])
                    if team_id is None:
                        logger.warning(f"Unknown team: {row['team_name']}")
                        continue

                    cur.execute("""
                        INSERT INTO ncaab_team_box_scores
                        (game_id, team_id, game_date, season, is_home,
                         pts, fgm, fga, fg3m, fg3a, ftm, fta,
                         oreb, dreb, reb, ast, stl, blk, tov, pf,
                         poss_est, efg_pct, tov_pct, orb_pct, ft_rate,
                         opp_pts, opp_poss_est, opp_efg_pct, opp_tov_pct,
                         opp_drb_pct, opp_ft_rate)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (game_id, team_id) DO NOTHING
                    """, (
                        row["game_id"], team_id, game_date, season, row["is_home"],
                        row["pts"], row["fgm"], row["fga"], row["fg3m"], row["fg3a"],
                        row["ftm"], row["fta"], row["oreb"], row["dreb"], row["reb"],
                        row["ast"], row["stl"], row["blk"], row["tov"], row["pf"],
                        row["poss_est"], row["efg_pct"], row["tov_pct"],
                        row["orb_pct"], row["ft_rate"],
                        row["opp_pts"], row["opp_poss_est"], row["opp_efg_pct"],
                        row["opp_tov_pct"], row["opp_drb_pct"], row["opp_ft_rate"],
                    ))
            db_conn.commit()
        finally:
            db_conn.close()


if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("Missing DATABASE_URL.")
        sys.exit(1)

    engine = create_engine(DATABASE_URL)
    scraper = NCAABCBBpyScraper(engine)

    parser = argparse.ArgumentParser(description="NCAAB CBBpy Scraper")
    parser.add_argument("--season", type=int, help="Season end year (e.g., 2025)")
    parser.add_argument("--schedule-only", action="store_true", help="Only scrape schedule, skip box scores")
    parser.add_argument("--date", type=str, help="Scrape box scores for date (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="Backfill missing box scores for season")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY, help="Delay between requests (seconds)")

    args = parser.parse_args()

    if args.date:
        scraper.scrape_date(args.date)
    elif args.season:
        scraper.scrape_schedule(args.season)
        if not args.schedule_only:
            if args.backfill:
                scraper.backfill_boxscores(args.season, delay=args.delay)
    else:
        logger.error("Specify --season or --date")
        sys.exit(1)
