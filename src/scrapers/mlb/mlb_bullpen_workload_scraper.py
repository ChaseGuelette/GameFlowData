"""
MLB Bullpen Workload Scraper
=============================
Derives daily bullpen workload status per team from existing
mlb_player_game_stats_pitching data. No external API calls needed.

Computes rolling reliever usage (IP, pitches, appearances) over 3/5/7 day
windows for each team, plus availability signals (rest days, closer status).

Usage:
    python -m src.scrapers.mlb.mlb_bullpen_workload_scraper --date 2026-04-26
    python -m src.scrapers.mlb.mlb_bullpen_workload_scraper --yesterday
    python -m src.scrapers.mlb.mlb_bullpen_workload_scraper --backfill --start-date 2026-04-01 --end-date 2026-04-26
    python -m src.scrapers.mlb.mlb_bullpen_workload_scraper --seasons 2024 2025
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBBullpenWorkload")

SEASON_RANGES = {
    2022: ("2022-04-07", "2022-10-05"),
    2023: ("2023-03-30", "2023-10-01"),
    2024: ("2024-03-20", "2024-09-29"),
    2025: ("2025-03-27", "2025-09-28"),
    2026: ("2026-03-26", "2026-09-27"),
}


class MLBBullpenWorkloadScraper:
    def __init__(self, engine):
        self.engine = engine

    def compute_day(self, date_str: str) -> int:
        """Compute bullpen workload for all teams with games on this date.

        Uses reliever data from the PREVIOUS 7 days (not including today,
        since today's game hasn't happened yet at inference time).
        """
        season = int(date_str[:4])

        # Get all teams that have a game on this date
        teams_sql = text("""
            SELECT DISTINCT team_id
            FROM mlb_player_game_stats_pitching
            WHERE game_date = :game_date
              AND is_starter = false
              AND did_not_play = false
            UNION
            SELECT DISTINCT team_id
            FROM mlb_player_game_stats_pitching
            WHERE game_date = :game_date
              AND is_starter = true
              AND did_not_play = false
        """)

        with self.engine.connect() as conn:
            teams_df = pd.read_sql(teams_sql, conn, params={"game_date": date_str})

        if teams_df.empty:
            # No games on this date — try getting teams from schedule
            # Fall back: compute for all teams that had recent activity
            teams_sql_fallback = text("""
                SELECT DISTINCT team_id
                FROM mlb_player_game_stats_pitching
                WHERE game_date BETWEEN :start AND :end
                  AND is_starter = false
            """)
            start = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
            with self.engine.connect() as conn:
                teams_df = pd.read_sql(teams_sql_fallback, conn, params={"start": start, "end": date_str})

        if teams_df.empty:
            logger.info(f"No team data found near {date_str}.")
            return 0

        team_ids = teams_df["team_id"].tolist()

        # Pull reliever stats for the 7-day window BEFORE this date
        window_start = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

        reliever_sql = text("""
            SELECT
                player_id, team_id, game_date, ip, pitches_thrown, er,
                so, bb, h_allowed, hr_allowed, is_save, is_starter
            FROM mlb_player_game_stats_pitching
            WHERE team_id = ANY(:team_ids)
              AND game_date >= :window_start
              AND game_date < :game_date
              AND is_starter = false
              AND did_not_play = false
        """)

        with self.engine.connect() as conn:
            rel_df = pd.read_sql(
                reliever_sql, conn,
                params={
                    "team_ids": team_ids,
                    "window_start": window_start,
                    "game_date": date_str,
                },
            )

        rows = []
        target_date = datetime.strptime(date_str, "%Y-%m-%d")

        for team_id in team_ids:
            team_data = rel_df[rel_df["team_id"] == team_id].copy()

            if team_data.empty:
                # No reliever usage in last 7 days — fully rested bullpen
                rows.append(self._empty_row(team_id, date_str, season))
                continue

            team_data["game_date"] = pd.to_datetime(team_data["game_date"])
            team_data["days_ago"] = (target_date - team_data["game_date"]).dt.days

            # Window filters
            last_3d = team_data[team_data["days_ago"] <= 3]
            last_5d = team_data[team_data["days_ago"] <= 5]
            last_7d = team_data  # already filtered to 7 days

            # Aggregate workload
            bp_ip_3 = float(last_3d["ip"].sum()) if not last_3d.empty else 0.0
            bp_ip_5 = float(last_5d["ip"].sum()) if not last_5d.empty else 0.0
            bp_ip_7 = float(last_7d["ip"].sum()) if not last_7d.empty else 0.0

            bp_pitches_3 = int(last_3d["pitches_thrown"].fillna(0).sum())
            bp_pitches_5 = int(last_5d["pitches_thrown"].fillna(0).sum())
            bp_pitches_7 = int(last_7d["pitches_thrown"].fillna(0).sum())

            bp_app_3 = len(last_3d)
            bp_app_5 = len(last_5d)
            bp_app_7 = len(last_7d)

            # Bullpen quality (ERA/WHIP/K9 over last 7 days)
            total_ip_7 = float(last_7d["ip"].sum())
            total_er_7 = float(last_7d["er"].fillna(0).sum())
            total_h_7 = float(last_7d["h_allowed"].fillna(0).sum())
            total_bb_7 = float(last_7d["bb"].fillna(0).sum())
            total_so_7 = float(last_7d["so"].fillna(0).sum())

            bp_era_7 = (total_er_7 / total_ip_7 * 9) if total_ip_7 > 0 else None
            bp_whip_7 = ((total_h_7 + total_bb_7) / total_ip_7) if total_ip_7 > 0 else None
            bp_k9_7 = (total_so_7 / total_ip_7 * 9) if total_ip_7 > 0 else None

            # Per-reliever availability
            reliever_last_game = team_data.groupby("player_id")["game_date"].max()
            reliever_rest_days = (target_date - reliever_last_game).dt.days

            zero_rest = int((reliever_rest_days == 1).sum())  # pitched yesterday
            one_day_rest = int((reliever_rest_days == 2).sum())  # pitched day before
            available = int((reliever_rest_days >= 2).sum())  # 2+ days rest

            # Closer detection: pitcher with most saves in last 7 days
            closer_id = None
            closer_days_rest = None
            closer_pitches_3d = 0
            save_counts = last_7d[last_7d["is_save"]].groupby("player_id").size()
            if not save_counts.empty:
                closer_id = save_counts.idxmax()
                closer_games = team_data[team_data["player_id"] == closer_id]
                if not closer_games.empty:
                    last_closer_game = closer_games["game_date"].max()
                    closer_days_rest = int((target_date - last_closer_game).days)
                    closer_3d = closer_games[closer_games["days_ago"] <= 3]
                    closer_pitches_3d = int(closer_3d["pitches_thrown"].fillna(0).sum())

            # High-leverage relievers: pitchers who appeared in close games (rough proxy)
            # Use top 3 by appearances in last 7 days as "high leverage"
            top_relievers = last_7d.groupby("player_id").size().nlargest(3).index
            hl_rested = 0
            for pid in top_relievers:
                pid_rest = reliever_rest_days.get(pid, 99)
                if pid_rest >= 2:
                    hl_rested += 1

            rows.append({
                "team_id": team_id,
                "game_date": date_str,
                "season": season,
                "bullpen_ip_last_3d": round(bp_ip_3, 1),
                "bullpen_ip_last_5d": round(bp_ip_5, 1),
                "bullpen_ip_last_7d": round(bp_ip_7, 1),
                "bullpen_pitches_last_3d": bp_pitches_3,
                "bullpen_pitches_last_5d": bp_pitches_5,
                "bullpen_pitches_last_7d": bp_pitches_7,
                "bullpen_appearances_last_3d": bp_app_3,
                "bullpen_appearances_last_5d": bp_app_5,
                "bullpen_appearances_last_7d": bp_app_7,
                "bullpen_era_last_7d": round(bp_era_7, 2) if bp_era_7 is not None else None,
                "bullpen_whip_last_7d": round(bp_whip_7, 2) if bp_whip_7 is not None else None,
                "bullpen_k_per_9_last_7d": round(bp_k9_7, 2) if bp_k9_7 is not None else None,
                "relievers_on_zero_rest": zero_rest,
                "relievers_on_one_day_rest": one_day_rest,
                "relievers_available": available,
                "closer_days_rest": closer_days_rest,
                "closer_pitches_last_3d": closer_pitches_3d,
                "high_leverage_relievers_rested": hl_rested,
            })

        if not rows:
            return 0

        # Upsert
        upsert_sql = text("""
            INSERT INTO mlb_bullpen_daily_status (
                team_id, game_date, season,
                bullpen_ip_last_3d, bullpen_ip_last_5d, bullpen_ip_last_7d,
                bullpen_pitches_last_3d, bullpen_pitches_last_5d, bullpen_pitches_last_7d,
                bullpen_appearances_last_3d, bullpen_appearances_last_5d, bullpen_appearances_last_7d,
                bullpen_era_last_7d, bullpen_whip_last_7d, bullpen_k_per_9_last_7d,
                relievers_on_zero_rest, relievers_on_one_day_rest, relievers_available,
                closer_days_rest, closer_pitches_last_3d, high_leverage_relievers_rested
            ) VALUES (
                :team_id, :game_date, :season,
                :bullpen_ip_last_3d, :bullpen_ip_last_5d, :bullpen_ip_last_7d,
                :bullpen_pitches_last_3d, :bullpen_pitches_last_5d, :bullpen_pitches_last_7d,
                :bullpen_appearances_last_3d, :bullpen_appearances_last_5d, :bullpen_appearances_last_7d,
                :bullpen_era_last_7d, :bullpen_whip_last_7d, :bullpen_k_per_9_last_7d,
                :relievers_on_zero_rest, :relievers_on_one_day_rest, :relievers_available,
                :closer_days_rest, :closer_pitches_last_3d, :high_leverage_relievers_rested
            )
            ON CONFLICT (team_id, game_date) DO UPDATE SET
                bullpen_ip_last_3d = EXCLUDED.bullpen_ip_last_3d,
                bullpen_ip_last_5d = EXCLUDED.bullpen_ip_last_5d,
                bullpen_ip_last_7d = EXCLUDED.bullpen_ip_last_7d,
                bullpen_pitches_last_3d = EXCLUDED.bullpen_pitches_last_3d,
                bullpen_pitches_last_5d = EXCLUDED.bullpen_pitches_last_5d,
                bullpen_pitches_last_7d = EXCLUDED.bullpen_pitches_last_7d,
                bullpen_appearances_last_3d = EXCLUDED.bullpen_appearances_last_3d,
                bullpen_appearances_last_5d = EXCLUDED.bullpen_appearances_last_5d,
                bullpen_appearances_last_7d = EXCLUDED.bullpen_appearances_last_7d,
                bullpen_era_last_7d = EXCLUDED.bullpen_era_last_7d,
                bullpen_whip_last_7d = EXCLUDED.bullpen_whip_last_7d,
                bullpen_k_per_9_last_7d = EXCLUDED.bullpen_k_per_9_last_7d,
                relievers_on_zero_rest = EXCLUDED.relievers_on_zero_rest,
                relievers_on_one_day_rest = EXCLUDED.relievers_on_one_day_rest,
                relievers_available = EXCLUDED.relievers_available,
                closer_days_rest = EXCLUDED.closer_days_rest,
                closer_pitches_last_3d = EXCLUDED.closer_pitches_last_3d,
                high_leverage_relievers_rested = EXCLUDED.high_leverage_relievers_rested,
                inserted_at = NOW()
        """)

        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(upsert_sql, row)

        logger.info(f"  Upserted {len(rows)} team bullpen rows for {date_str}.")
        return len(rows)

    def _empty_row(self, team_id: int, date_str: str, season: int) -> dict:
        return {
            "team_id": team_id, "game_date": date_str, "season": season,
            "bullpen_ip_last_3d": 0, "bullpen_ip_last_5d": 0, "bullpen_ip_last_7d": 0,
            "bullpen_pitches_last_3d": 0, "bullpen_pitches_last_5d": 0, "bullpen_pitches_last_7d": 0,
            "bullpen_appearances_last_3d": 0, "bullpen_appearances_last_5d": 0, "bullpen_appearances_last_7d": 0,
            "bullpen_era_last_7d": None, "bullpen_whip_last_7d": None, "bullpen_k_per_9_last_7d": None,
            "relievers_on_zero_rest": 0, "relievers_on_one_day_rest": 0, "relievers_available": 0,
            "closer_days_rest": None, "closer_pitches_last_3d": 0, "high_leverage_relievers_rested": 0,
        }


def get_date_range(seasons: list[int] | None = None,
                   start_date: str | None = None,
                   end_date: str | None = None) -> list[str]:
    dates = []
    if start_date and end_date:
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    elif seasons:
        for season in sorted(seasons):
            if season not in SEASON_RANGES:
                logger.warning(f"Unknown season {season}, skipping.")
                continue
            s, e = SEASON_RANGES[season]
            current = datetime.strptime(s, "%Y-%m-%d")
            end = datetime.strptime(e, "%Y-%m-%d")
            while current <= end:
                dates.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
    return dates


def main():
    parser = argparse.ArgumentParser(description="MLB Bullpen Workload Derivation")
    parser.add_argument("--date", type=str, help="Compute for specific date (YYYY-MM-DD)")
    parser.add_argument("--yesterday", action="store_true", help="Compute for yesterday")
    parser.add_argument("--backfill", action="store_true", help="Backfill date range")
    parser.add_argument("--start-date", type=str, help="Backfill start date")
    parser.add_argument("--end-date", type=str, help="Backfill end date")
    parser.add_argument("--seasons", nargs="+", type=int, help="Seasons to backfill")
    parser.add_argument("--local", action="store_true", help="Use local Postgres")

    args = parser.parse_args()
    engine = get_engine(local=args.local)
    scraper = MLBBullpenWorkloadScraper(engine)

    if args.date:
        n = scraper.compute_day(args.date)
        logger.info(f"Done: {n} team rows for {args.date}")

    elif args.yesterday:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        n = scraper.compute_day(date_str)
        logger.info(f"Done: {n} team rows for {date_str}")

    elif args.backfill or args.seasons:
        dates = get_date_range(
            seasons=args.seasons,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        if not dates:
            logger.error("No dates to process.")
            return

        logger.info(f"Backfill: {len(dates)} dates to process")
        total = 0
        errors = 0
        for i, date_str in enumerate(dates, 1):
            try:
                n = scraper.compute_day(date_str)
                total += n
                if i % 50 == 0:
                    logger.info(f"  Progress: {i}/{len(dates)} dates, {total} total rows")
            except Exception as e:
                logger.error(f"Error on {date_str}: {e}")
                errors += 1

        logger.info(f"Backfill complete: {total} rows across {len(dates)} dates ({errors} errors)")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
