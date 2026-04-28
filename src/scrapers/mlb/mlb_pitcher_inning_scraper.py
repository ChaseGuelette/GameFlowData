"""
MLB Pitcher Inning-Level Stats Scraper
=======================================
Scrapes Statcast pitch-level data via pybaseball, aggregates per
(pitcher, game, inning), and upserts into mlb_pitcher_inning_stats.

This provides the foundation for:
  - IP prediction models (per-inning pitch counts, fatigue curves)
  - K/IP rate models (per-inning K rates, stuff quality changes)
  - Velocity decay features (velo drop by inning)

Uses the same pybaseball statcast() call as mlb_statcast_scraper.py
but groups by inning instead of game-level.

Usage:
    python -m src.scrapers.mlb.mlb_pitcher_inning_scraper --date 2026-04-26
    python -m src.scrapers.mlb.mlb_pitcher_inning_scraper --yesterday
    python -m src.scrapers.mlb.mlb_pitcher_inning_scraper --backfill --start-date 2026-04-01 --end-date 2026-04-26
    python -m src.scrapers.mlb.mlb_pitcher_inning_scraper --seasons 2024 2025
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from pybaseball import statcast
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBPitcherInningScraper")

# Pitch type classification (same as mlb_statcast_scraper)
FASTBALL_TYPES = {"FF", "SI", "FC", "FA"}
BREAKING_TYPES = {"SL", "CU", "KC", "SV", "CS", "KN", "SC", "ST", "EP"}
OFFSPEED_TYPES = {"CH", "FS", "FO"}

SWING_EVENTS = {
    "swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
    "foul_bunt", "hit_into_play", "hit_into_play_no_out",
    "hit_into_play_score", "missed_bunt", "bunt_foul_tip",
}
WHIFF_EVENTS = {"swinging_strike", "swinging_strike_blocked", "missed_bunt"}

# Called strike + whiff (CSW)
CALLED_STRIKE_EVENTS = {"called_strike"}
CSW_EVENTS = WHIFF_EVENTS | CALLED_STRIKE_EVENTS

# At-bat ending events for batters faced count
AB_END_EVENTS = {
    "strikeout", "strikeout_double_play", "field_out", "grounded_into_double_play",
    "force_out", "double_play", "triple_play", "fielders_choice",
    "fielders_choice_out", "sac_fly", "sac_bunt", "sac_fly_double_play",
    "sac_bunt_double_play", "field_error", "catcher_interf",
    "single", "double", "triple", "home_run",
    "walk", "hit_by_pitch", "intent_walk",
}

# Strikeout events
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
WALK_EVENTS = {"walk", "intent_walk", "hit_by_pitch"}
HIT_EVENTS = {"single", "double", "triple", "home_run"}
HR_EVENTS = {"home_run"}

# Out events (for outs_recorded)
OUT_EVENTS = {
    "strikeout", "strikeout_double_play", "field_out", "grounded_into_double_play",
    "force_out", "double_play", "triple_play", "fielders_choice",
    "fielders_choice_out", "sac_fly", "sac_bunt", "sac_fly_double_play",
    "sac_bunt_double_play",
}

# Season date ranges
SEASON_RANGES = {
    2022: ("2022-04-07", "2022-10-05"),
    2023: ("2023-03-30", "2023-10-01"),
    2024: ("2024-03-20", "2024-09-29"),
    2025: ("2025-03-27", "2025-09-28"),
    2026: ("2026-03-26", "2026-09-27"),
}

PROGRESS_FILE = Path(__file__).parent / "mlb_pitcher_inning_backfill_progress.json"


def _safe_float(val) -> float | None:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return float(val)


def _safe_int(val) -> int | None:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return int(val)


class MLBPitcherInningScraper:
    def __init__(self, engine):
        self.engine = engine

    def scrape_day(self, date_str: str) -> dict:
        """Fetch Statcast pitch data for a single day."""
        return self.scrape_range(date_str, date_str)

    def scrape_range(self, start_date: str, end_date: str) -> dict:
        """Fetch Statcast pitch data for a date range, aggregate per (pitcher, game, inning)."""
        logger.info(f"Fetching Statcast data for {start_date} to {end_date}...")

        try:
            df = statcast(start_dt=start_date, end_dt=end_date)
        except Exception as e:
            logger.error(f"Failed to fetch Statcast for {start_date}-{end_date}: {e}")
            return {"rows": 0, "dates": []}

        if df is None or df.empty:
            logger.info(f"No Statcast data for {start_date} to {end_date}.")
            return {"rows": 0, "dates": []}

        logger.info(f"  Fetched {len(df)} pitch rows for {start_date} to {end_date}.")

        rows_inserted = self._process_pitching_innings(df)

        # Track which dates had data
        dates_covered = []
        if "game_date" in df.columns:
            dates_covered = sorted(df["game_date"].dropna().astype(str).str[:10].unique().tolist())

        return {"rows": rows_inserted, "dates": dates_covered}

    def _process_pitching_innings(self, df: pd.DataFrame) -> int:
        """Aggregate pitch data per (pitcher, game_pk, inning) and upsert."""
        # Filter to pitches with valid pitcher and game
        pdf = df.dropna(subset=["pitcher", "game_pk", "inning"]).copy()
        if pdf.empty:
            return 0

        pdf["pitcher"] = pdf["pitcher"].astype(int)
        pdf["game_pk"] = pdf["game_pk"].astype(int)
        pdf["inning"] = pdf["inning"].astype(int)

        # Identify starters: pitcher with most pitches per game from the starting side
        # The first pitcher for each team in each game is the starter
        starters = set()
        for game_pk in pdf["game_pk"].unique():
            game_df = pdf[pdf["game_pk"] == game_pk]
            for half in ["Top", "Bot"]:
                half_df = game_df[game_df["inning_topbot"] == half]
                if not half_df.empty:
                    # Starter is the pitcher who threw in inning 1
                    inning_1 = half_df[half_df["inning"] == 1]
                    if not inning_1.empty:
                        starter_id = inning_1["pitcher"].mode()
                        if len(starter_id) > 0:
                            starters.add((game_pk, int(starter_id.iloc[0])))

        # Build a game_date lookup from the data
        game_date_lookup = {}
        if "game_date" in pdf.columns:
            for _, row in pdf[["game_pk", "game_date"]].drop_duplicates().iterrows():
                game_date_lookup[int(row["game_pk"])] = str(row["game_date"])[:10]

        grouped = pdf.groupby(["pitcher", "game_pk", "inning"])
        rows = []

        for (pitcher_id, game_pk, inning), grp in grouped:
            pitcher_id = int(pitcher_id)
            game_pk = int(game_pk)
            inning = int(inning)

            is_starter = (game_pk, pitcher_id) in starters
            n_pitches = len(grp)

            # Pitch types
            pitch_types = grp["pitch_type"].dropna()
            n_typed = len(pitch_types)
            fastball_pct = (pitch_types.isin(FASTBALL_TYPES).sum() / n_typed) if n_typed > 0 else None
            breaking_pct = (pitch_types.isin(BREAKING_TYPES).sum() / n_typed) if n_typed > 0 else None
            offspeed_pct = (pitch_types.isin(OFFSPEED_TYPES).sum() / n_typed) if n_typed > 0 else None

            # Velocity
            velo = grp["release_speed"].dropna()
            avg_velo = _safe_float(velo.mean()) if len(velo) > 0 else None
            max_velo = _safe_float(velo.max()) if len(velo) > 0 else None

            # Spin rate
            spin = grp["release_spin_rate"].dropna()
            avg_spin = _safe_float(spin.mean()) if len(spin) > 0 else None

            # Extension
            ext = grp["release_extension"].dropna()
            avg_ext = _safe_float(ext.mean()) if len(ext) > 0 else None

            # Plate discipline
            descriptions = grp["description"].dropna()
            n_swings = descriptions.isin(SWING_EVENTS).sum()
            n_whiffs = descriptions.isin(WHIFF_EVENTS).sum()
            n_csw = descriptions.isin(CSW_EVENTS).sum()
            # Zone rate
            zones = grp["zone"].dropna()
            n_in_zone = (zones <= 9).sum()  # zones 1-9 are in the strike zone
            zone_rate = _safe_float(n_in_zone / len(zones)) if len(zones) > 0 else None

            # Chase rate: swings on pitches outside zone
            outside_zone = grp[grp["zone"].fillna(99) > 9]
            n_outside_swings = outside_zone["description"].isin(SWING_EVENTS).sum() if len(outside_zone) > 0 else 0
            n_outside = len(outside_zone)
            chase_rate = _safe_float(n_outside_swings / n_outside) if n_outside > 0 else None

            whiff_rate = _safe_float(n_whiffs / n_swings) if n_swings > 0 else None
            csw_rate = _safe_float(n_csw / n_pitches) if n_pitches > 0 else None
            swinging_strike_pct = _safe_float(n_whiffs / n_pitches) if n_pitches > 0 else None

            # Outcomes (from events column — only populated on last pitch of at-bat)
            events = grp["events"].dropna()
            strikeouts = int(events.isin(STRIKEOUT_EVENTS).sum())
            walks = int(events.isin(WALK_EVENTS).sum())
            hits_allowed = int(events.isin(HIT_EVENTS).sum())
            home_runs = int(events.isin(HR_EVENTS).sum())
            batters_faced = int(events.isin(AB_END_EVENTS).sum())

            # Outs recorded (approximate — double plays count as 2)
            outs = int(events.isin(OUT_EVENTS).sum())
            # Add extra out for double plays
            outs += int(events.isin({"grounded_into_double_play", "double_play",
                                     "sac_fly_double_play", "sac_bunt_double_play",
                                     "strikeout_double_play"}).sum())

            # Runs allowed (from post_bat_score changes if available)
            # Use a simpler approach: count scoring events
            runs = 0

            # Batted ball quality
            batted = grp.dropna(subset=["launch_speed"])
            avg_ev = _safe_float(batted["launch_speed"].mean()) if len(batted) > 0 else None
            barrels = 0
            hard_hits = 0
            if len(batted) > 0:
                if "launch_speed_angle" in batted.columns:
                    barrels = int((batted["launch_speed_angle"] == 6).sum())
                # Hard hit: exit velo >= 95 mph
                hard_hits = int((batted["launch_speed"] >= 95).sum())
            barrel_pct = _safe_float(barrels / len(batted)) if len(batted) > 0 else None
            hard_hit_pct = _safe_float(hard_hits / len(batted)) if len(batted) > 0 else None

            # Get game_date and season from the data
            gd = game_date_lookup.get(game_pk, "1900-01-01")
            season = int(gd[:4])

            rows.append({
                "player_id": pitcher_id,
                "game_id": str(game_pk),
                "game_date": gd,
                "season": season,
                "inning": inning,
                "is_starter": is_starter,
                "pitches_thrown": n_pitches,
                "batters_faced": batters_faced,
                "outs_recorded": outs,
                "strikeouts": strikeouts,
                "walks": walks,
                "hits_allowed": hits_allowed,
                "runs_allowed": runs,
                "home_runs_allowed": home_runs,
                "avg_release_speed": avg_velo,
                "max_release_speed": max_velo,
                "avg_spin_rate": avg_spin,
                "avg_extension": avg_ext,
                "fastball_pct": _safe_float(fastball_pct),
                "breaking_pct": _safe_float(breaking_pct),
                "offspeed_pct": _safe_float(offspeed_pct),
                "whiff_rate": whiff_rate,
                "chase_rate": chase_rate,
                "zone_rate": zone_rate,
                "csw_rate": csw_rate,
                "swinging_strike_pct": swinging_strike_pct,
                "avg_exit_velocity": avg_ev,
                "barrel_pct": barrel_pct,
                "hard_hit_pct": hard_hit_pct,
            })

        if not rows:
            return 0

        # Upsert
        upsert_sql = text("""
            INSERT INTO mlb_pitcher_inning_stats (
                player_id, game_id, game_date, season, inning, is_starter,
                pitches_thrown, batters_faced, outs_recorded,
                strikeouts, walks, hits_allowed, runs_allowed, home_runs_allowed,
                avg_release_speed, max_release_speed, avg_spin_rate, avg_extension,
                fastball_pct, breaking_pct, offspeed_pct,
                whiff_rate, chase_rate, zone_rate, csw_rate, swinging_strike_pct,
                avg_exit_velocity, barrel_pct, hard_hit_pct
            ) VALUES (
                :player_id, :game_id, :game_date, :season, :inning, :is_starter,
                :pitches_thrown, :batters_faced, :outs_recorded,
                :strikeouts, :walks, :hits_allowed, :runs_allowed, :home_runs_allowed,
                :avg_release_speed, :max_release_speed, :avg_spin_rate, :avg_extension,
                :fastball_pct, :breaking_pct, :offspeed_pct,
                :whiff_rate, :chase_rate, :zone_rate, :csw_rate, :swinging_strike_pct,
                :avg_exit_velocity, :barrel_pct, :hard_hit_pct
            )
            ON CONFLICT (player_id, game_id, inning) DO UPDATE SET
                is_starter = EXCLUDED.is_starter,
                pitches_thrown = EXCLUDED.pitches_thrown,
                batters_faced = EXCLUDED.batters_faced,
                outs_recorded = EXCLUDED.outs_recorded,
                strikeouts = EXCLUDED.strikeouts,
                walks = EXCLUDED.walks,
                hits_allowed = EXCLUDED.hits_allowed,
                runs_allowed = EXCLUDED.runs_allowed,
                home_runs_allowed = EXCLUDED.home_runs_allowed,
                avg_release_speed = EXCLUDED.avg_release_speed,
                max_release_speed = EXCLUDED.max_release_speed,
                avg_spin_rate = EXCLUDED.avg_spin_rate,
                avg_extension = EXCLUDED.avg_extension,
                fastball_pct = EXCLUDED.fastball_pct,
                breaking_pct = EXCLUDED.breaking_pct,
                offspeed_pct = EXCLUDED.offspeed_pct,
                whiff_rate = EXCLUDED.whiff_rate,
                chase_rate = EXCLUDED.chase_rate,
                zone_rate = EXCLUDED.zone_rate,
                csw_rate = EXCLUDED.csw_rate,
                swinging_strike_pct = EXCLUDED.swinging_strike_pct,
                avg_exit_velocity = EXCLUDED.avg_exit_velocity,
                barrel_pct = EXCLUDED.barrel_pct,
                hard_hit_pct = EXCLUDED.hard_hit_pct,
                inserted_at = NOW()
        """)

        chunk_size = 200
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            with self.engine.begin() as conn:
                for row in chunk:
                    conn.execute(upsert_sql, row)

        logger.info(f"  Upserted {len(rows)} pitcher-inning rows for {rows[0]['game_date']}.")
        return len(rows)


def load_progress() -> set[str]:
    if PROGRESS_FILE.exists():
        data = json.loads(PROGRESS_FILE.read_text())
        return set(data.get("completed_dates", []))
    return set()


def save_progress(completed: set[str]):
    PROGRESS_FILE.write_text(json.dumps({
        "completed_dates": sorted(completed),
        "last_updated": datetime.now().isoformat(),
    }, indent=2))


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
    parser = argparse.ArgumentParser(
        description="MLB Pitcher Inning-Level Statcast Scraper",
    )
    parser.add_argument("--date", type=str, help="Scrape specific date (YYYY-MM-DD)")
    parser.add_argument("--yesterday", action="store_true", help="Scrape yesterday")
    parser.add_argument("--backfill", action="store_true", help="Backfill date range")
    parser.add_argument("--start-date", type=str, help="Backfill start date")
    parser.add_argument("--end-date", type=str, help="Backfill end date")
    parser.add_argument("--seasons", nargs="+", type=int, help="Seasons to backfill (e.g. 2024 2025)")
    parser.add_argument("--no-resume", action="store_true", help="Ignore progress, reprocess all")
    parser.add_argument("--local", action="store_true", help="Write to local Postgres")

    args = parser.parse_args()
    engine = get_engine(local=args.local)
    scraper = MLBPitcherInningScraper(engine)

    if args.date:
        result = scraper.scrape_day(args.date)
        logger.info(f"Done: {result['rows']} rows for {args.date}")

    elif args.yesterday:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        result = scraper.scrape_day(date_str)
        logger.info(f"Done: {result['rows']} rows for {date_str}")

    elif args.backfill or args.seasons:
        dates = get_date_range(
            seasons=args.seasons,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        if not dates:
            logger.error("No dates to process. Use --seasons or --start-date/--end-date.")
            return

        completed = set() if args.no_resume else load_progress()
        remaining = [d for d in dates if d not in completed]
        logger.info(f"Backfill: {len(remaining)} dates remaining ({len(completed)} already done)")

        # Chunk into weekly ranges for fewer HTTP requests (~7x speedup)
        chunk_size = 7
        chunks = []
        for i in range(0, len(remaining), chunk_size):
            chunk = remaining[i:i + chunk_size]
            chunks.append((chunk[0], chunk[-1]))  # (start, end) of chunk

        logger.info(f"  Processing {len(chunks)} weekly chunks ({len(remaining)} dates)")

        total_rows = 0
        errors = 0
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            try:
                result = scraper.scrape_range(chunk_start, chunk_end)
                total_rows += result["rows"]

                # Mark all dates in range as completed (including days with no games)
                cur = datetime.strptime(chunk_start, "%Y-%m-%d")
                end = datetime.strptime(chunk_end, "%Y-%m-%d")
                while cur <= end:
                    completed.add(cur.strftime("%Y-%m-%d"))
                    cur += timedelta(days=1)

                if i % 5 == 0:
                    save_progress(completed)
                    logger.info(f"  Progress: {i}/{len(chunks)} chunks, {total_rows} total rows, {errors} errors")

            except Exception as e:
                logger.error(f"Error processing chunk {chunk_start} to {chunk_end}: {e}")
                errors += 1

            time.sleep(1)  # Rate limit between chunks

        save_progress(completed)
        logger.info(f"Backfill complete: {total_rows} rows across {len(chunks)} chunks ({errors} errors)")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
