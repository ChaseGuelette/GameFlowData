"""
MLB Statcast Scraper
====================
Scrapes daily Statcast pitch-level data from Baseball Savant via pybaseball,
aggregates per (player, game_date), and upserts into:
  - mlb_player_game_statcast_batting
  - mlb_player_game_statcast_pitching

Uses day-level queries (more efficient than per-player).
Statcast data may be corrected retroactively, so we use DO UPDATE upserts.

Usage:
    python -m src.scrapers.mlb.mlb_statcast_scraper --date 2025-06-15
    python -m src.scrapers.mlb.mlb_statcast_scraper --yesterday
    python -m src.scrapers.mlb.mlb_statcast_scraper --backfill --start-date 2025-04-01 --end-date 2025-04-07
"""

import argparse
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
logger = logging.getLogger("MLBStatcastScraper")

# Pitch type classification
FASTBALL_TYPES = {"FF", "SI", "FC", "FA"}
BREAKING_TYPES = {"SL", "CU", "KC", "SV", "CS", "KN", "SC", "ST", "EP"}
OFFSPEED_TYPES = {"CH", "FS", "FO"}

# Batted ball types from launch angle
# Ground ball: < 10, Line drive: 10-25, Fly ball: 25-50, Popup: > 50
BB_TYPE_MAP = {"ground_ball": "gb", "line_drive": "ld", "fly_ball": "fb", "popup": "popup"}

# Swing events (for whiff calculation)
SWING_EVENTS = {
    "swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
    "foul_bunt", "hit_into_play", "hit_into_play_no_out",
    "hit_into_play_score", "missed_bunt", "bunt_foul_tip",
}
WHIFF_EVENTS = {"swinging_strike", "swinging_strike_blocked", "missed_bunt"}


class MLBStatcastScraper:
    def __init__(self, engine):
        self.engine = engine

    def scrape_day(self, date_str: str) -> dict:
        """Fetch Statcast data for a single day, aggregate, and upsert.

        Args:
            date_str: Date in YYYY-MM-DD format.

        Returns:
            Dict with counts: batting_rows, pitching_rows.
        """
        logger.info(f"Fetching Statcast data for {date_str}...")

        try:
            df = statcast(start_dt=date_str, end_dt=date_str)
        except Exception as e:
            logger.error(f"Failed to fetch Statcast for {date_str}: {e}")
            return {"batting_rows": 0, "pitching_rows": 0}

        if df is None or df.empty:
            logger.info(f"No Statcast data for {date_str}.")
            return {"batting_rows": 0, "pitching_rows": 0}

        logger.info(f"  Fetched {len(df)} pitch rows for {date_str}.")

        # Aggregate and insert
        batting_count = self._process_batting(df, date_str)
        pitching_count = self._process_pitching(df, date_str)

        logger.info(f"  Upserted {batting_count} batting, {pitching_count} pitching rows.")
        return {"batting_rows": batting_count, "pitching_rows": pitching_count}

    def _process_batting(self, df: pd.DataFrame, date_str: str) -> int:
        """Aggregate batting stats from pitch-level data and upsert."""
        agg = self._aggregate_batting(df)
        if agg.empty:
            return 0

        count = 0
        with self.engine.begin() as conn:
            for _, row in agg.iterrows():
                player_id = int(row["batter"])
                self._ensure_player(conn, player_id)
                conn.execute(
                    text("""
                        INSERT INTO mlb_player_game_statcast_batting
                            (player_id, game_date,
                             avg_exit_velocity, max_exit_velocity, avg_launch_angle,
                             barrel_pct, hard_hit_pct, sweet_spot_pct,
                             xba, xslg, xwoba, woba,
                             gb_pct, fb_pct, ld_pct, popup_pct,
                             pull_pct, center_pct, oppo_pct,
                             zone_pct, chase_pct, whiff_pct,
                             batted_balls, barrels, hard_hits,
                             total_pitches_seen, total_swings)
                        VALUES
                            (:player_id, :game_date,
                             :avg_exit_velocity, :max_exit_velocity, :avg_launch_angle,
                             :barrel_pct, :hard_hit_pct, :sweet_spot_pct,
                             :xba, :xslg, :xwoba, :woba,
                             :gb_pct, :fb_pct, :ld_pct, :popup_pct,
                             :pull_pct, :center_pct, :oppo_pct,
                             :zone_pct, :chase_pct, :whiff_pct,
                             :batted_balls, :barrels, :hard_hits,
                             :total_pitches_seen, :total_swings)
                        ON CONFLICT (player_id, game_date) DO UPDATE SET
                             avg_exit_velocity = EXCLUDED.avg_exit_velocity,
                             max_exit_velocity = EXCLUDED.max_exit_velocity,
                             avg_launch_angle = EXCLUDED.avg_launch_angle,
                             barrel_pct = EXCLUDED.barrel_pct,
                             hard_hit_pct = EXCLUDED.hard_hit_pct,
                             sweet_spot_pct = EXCLUDED.sweet_spot_pct,
                             xba = EXCLUDED.xba,
                             xslg = EXCLUDED.xslg,
                             xwoba = EXCLUDED.xwoba,
                             woba = EXCLUDED.woba,
                             gb_pct = EXCLUDED.gb_pct,
                             fb_pct = EXCLUDED.fb_pct,
                             ld_pct = EXCLUDED.ld_pct,
                             popup_pct = EXCLUDED.popup_pct,
                             pull_pct = EXCLUDED.pull_pct,
                             center_pct = EXCLUDED.center_pct,
                             oppo_pct = EXCLUDED.oppo_pct,
                             zone_pct = EXCLUDED.zone_pct,
                             chase_pct = EXCLUDED.chase_pct,
                             whiff_pct = EXCLUDED.whiff_pct,
                             batted_balls = EXCLUDED.batted_balls,
                             barrels = EXCLUDED.barrels,
                             hard_hits = EXCLUDED.hard_hits,
                             total_pitches_seen = EXCLUDED.total_pitches_seen,
                             total_swings = EXCLUDED.total_swings
                    """),
                    {
                        "player_id": player_id,
                        "game_date": date_str,
                        "avg_exit_velocity": _safe_float(row.get("avg_exit_velocity")),
                        "max_exit_velocity": _safe_float(row.get("max_exit_velocity")),
                        "avg_launch_angle": _safe_float(row.get("avg_launch_angle")),
                        "barrel_pct": _safe_float(row.get("barrel_pct")),
                        "hard_hit_pct": _safe_float(row.get("hard_hit_pct")),
                        "sweet_spot_pct": _safe_float(row.get("sweet_spot_pct")),
                        "xba": _safe_float(row.get("xba")),
                        "xslg": _safe_float(row.get("xslg")),
                        "xwoba": _safe_float(row.get("xwoba")),
                        "woba": _safe_float(row.get("woba")),
                        "gb_pct": _safe_float(row.get("gb_pct")),
                        "fb_pct": _safe_float(row.get("fb_pct")),
                        "ld_pct": _safe_float(row.get("ld_pct")),
                        "popup_pct": _safe_float(row.get("popup_pct")),
                        "pull_pct": _safe_float(row.get("pull_pct")),
                        "center_pct": _safe_float(row.get("center_pct")),
                        "oppo_pct": _safe_float(row.get("oppo_pct")),
                        "zone_pct": _safe_float(row.get("zone_pct")),
                        "chase_pct": _safe_float(row.get("chase_pct")),
                        "whiff_pct": _safe_float(row.get("whiff_pct")),
                        "batted_balls": _safe_int(row.get("batted_balls")),
                        "barrels": _safe_int(row.get("barrels")),
                        "hard_hits": _safe_int(row.get("hard_hits")),
                        "total_pitches_seen": _safe_int(row.get("total_pitches_seen")),
                        "total_swings": _safe_int(row.get("total_swings")),
                    },
                )
                count += 1

        return count

    def _process_pitching(self, df: pd.DataFrame, date_str: str) -> int:
        """Aggregate pitching stats from pitch-level data and upsert."""
        agg = self._aggregate_pitching(df)
        if agg.empty:
            return 0

        count = 0
        with self.engine.begin() as conn:
            for _, row in agg.iterrows():
                player_id = int(row["pitcher"])
                self._ensure_player(conn, player_id)
                conn.execute(
                    text("""
                        INSERT INTO mlb_player_game_statcast_pitching
                            (player_id, game_date,
                             avg_exit_velocity_against, max_exit_velocity_against,
                             barrel_pct_against, hard_hit_pct_against,
                             xba_against, xslg_against, xwoba_against, xera,
                             avg_fastball_velo, max_fastball_velo,
                             avg_fastball_spin, avg_breaking_spin,
                             fastball_pct, breaking_pct, offspeed_pct,
                             gb_pct, fb_pct, ld_pct, popup_pct,
                             zone_pct, chase_pct, whiff_pct, csw_pct,
                             batted_balls_against, barrels_against,
                             total_pitches, total_swings_against)
                        VALUES
                            (:player_id, :game_date,
                             :avg_exit_velocity_against, :max_exit_velocity_against,
                             :barrel_pct_against, :hard_hit_pct_against,
                             :xba_against, :xslg_against, :xwoba_against, :xera,
                             :avg_fastball_velo, :max_fastball_velo,
                             :avg_fastball_spin, :avg_breaking_spin,
                             :fastball_pct, :breaking_pct, :offspeed_pct,
                             :gb_pct, :fb_pct, :ld_pct, :popup_pct,
                             :zone_pct, :chase_pct, :whiff_pct, :csw_pct,
                             :batted_balls_against, :barrels_against,
                             :total_pitches, :total_swings_against)
                        ON CONFLICT (player_id, game_date) DO UPDATE SET
                             avg_exit_velocity_against = EXCLUDED.avg_exit_velocity_against,
                             max_exit_velocity_against = EXCLUDED.max_exit_velocity_against,
                             barrel_pct_against = EXCLUDED.barrel_pct_against,
                             hard_hit_pct_against = EXCLUDED.hard_hit_pct_against,
                             xba_against = EXCLUDED.xba_against,
                             xslg_against = EXCLUDED.xslg_against,
                             xwoba_against = EXCLUDED.xwoba_against,
                             xera = EXCLUDED.xera,
                             avg_fastball_velo = EXCLUDED.avg_fastball_velo,
                             max_fastball_velo = EXCLUDED.max_fastball_velo,
                             avg_fastball_spin = EXCLUDED.avg_fastball_spin,
                             avg_breaking_spin = EXCLUDED.avg_breaking_spin,
                             fastball_pct = EXCLUDED.fastball_pct,
                             breaking_pct = EXCLUDED.breaking_pct,
                             offspeed_pct = EXCLUDED.offspeed_pct,
                             gb_pct = EXCLUDED.gb_pct,
                             fb_pct = EXCLUDED.fb_pct,
                             ld_pct = EXCLUDED.ld_pct,
                             popup_pct = EXCLUDED.popup_pct,
                             zone_pct = EXCLUDED.zone_pct,
                             chase_pct = EXCLUDED.chase_pct,
                             whiff_pct = EXCLUDED.whiff_pct,
                             csw_pct = EXCLUDED.csw_pct,
                             batted_balls_against = EXCLUDED.batted_balls_against,
                             barrels_against = EXCLUDED.barrels_against,
                             total_pitches = EXCLUDED.total_pitches,
                             total_swings_against = EXCLUDED.total_swings_against
                    """),
                    {
                        "player_id": player_id,
                        "game_date": date_str,
                        "avg_exit_velocity_against": _safe_float(row.get("avg_exit_velocity_against")),
                        "max_exit_velocity_against": _safe_float(row.get("max_exit_velocity_against")),
                        "barrel_pct_against": _safe_float(row.get("barrel_pct_against")),
                        "hard_hit_pct_against": _safe_float(row.get("hard_hit_pct_against")),
                        "xba_against": _safe_float(row.get("xba_against")),
                        "xslg_against": _safe_float(row.get("xslg_against")),
                        "xwoba_against": _safe_float(row.get("xwoba_against")),
                        "xera": _safe_float(row.get("xera")),
                        "avg_fastball_velo": _safe_float(row.get("avg_fastball_velo")),
                        "max_fastball_velo": _safe_float(row.get("max_fastball_velo")),
                        "avg_fastball_spin": _safe_float(row.get("avg_fastball_spin")),
                        "avg_breaking_spin": _safe_float(row.get("avg_breaking_spin")),
                        "fastball_pct": _safe_float(row.get("fastball_pct")),
                        "breaking_pct": _safe_float(row.get("breaking_pct")),
                        "offspeed_pct": _safe_float(row.get("offspeed_pct")),
                        "gb_pct": _safe_float(row.get("gb_pct")),
                        "fb_pct": _safe_float(row.get("fb_pct")),
                        "ld_pct": _safe_float(row.get("ld_pct")),
                        "popup_pct": _safe_float(row.get("popup_pct")),
                        "zone_pct": _safe_float(row.get("zone_pct")),
                        "chase_pct": _safe_float(row.get("chase_pct")),
                        "whiff_pct": _safe_float(row.get("whiff_pct")),
                        "csw_pct": _safe_float(row.get("csw_pct")),
                        "batted_balls_against": _safe_int(row.get("batted_balls_against")),
                        "barrels_against": _safe_int(row.get("barrels_against")),
                        "total_pitches": _safe_int(row.get("total_pitches")),
                        "total_swings_against": _safe_int(row.get("total_swings_against")),
                    },
                )
                count += 1

        return count

    # ------------------------------------------------------------------
    # Aggregation logic
    # ------------------------------------------------------------------

    def _aggregate_batting(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate pitch-level data into per-batter-game rows."""
        if "batter" not in df.columns:
            return pd.DataFrame()

        results = []
        for (batter, game_date), group in df.groupby(["batter", "game_date"]):
            row = {"batter": batter, "game_date": game_date}

            # Batted balls: type == 'X' means ball in play
            batted = group[group["type"] == "X"] if "type" in group.columns else pd.DataFrame()

            # Total pitches and swings
            row["total_pitches_seen"] = len(group)
            swings = group[group["description"].isin(SWING_EVENTS)] if "description" in group.columns else pd.DataFrame()
            row["total_swings"] = len(swings)

            # Exit velocity & launch angle (batted balls only)
            if not batted.empty and "launch_speed" in batted.columns:
                ev = batted["launch_speed"].dropna()
                row["avg_exit_velocity"] = round(ev.mean(), 1) if not ev.empty else None
                row["max_exit_velocity"] = round(ev.max(), 1) if not ev.empty else None
            else:
                row["avg_exit_velocity"] = None
                row["max_exit_velocity"] = None

            if not batted.empty and "launch_angle" in batted.columns:
                la = batted["launch_angle"].dropna()
                row["avg_launch_angle"] = round(la.mean(), 1) if not la.empty else None
            else:
                row["avg_launch_angle"] = None

            bb_count = len(batted)
            row["batted_balls"] = bb_count

            # Barrel & hard hit
            if bb_count > 0:
                if "barrel" in batted.columns:
                    barrel_count = int(batted["barrel"].sum())
                    row["barrels"] = barrel_count
                    row["barrel_pct"] = round(barrel_count / bb_count, 4)
                else:
                    row["barrels"] = 0
                    row["barrel_pct"] = None

                if "launch_speed" in batted.columns:
                    hard = batted["launch_speed"].dropna()
                    hard_count = int((hard >= 95).sum())
                    row["hard_hits"] = hard_count
                    row["hard_hit_pct"] = round(hard_count / bb_count, 4)
                else:
                    row["hard_hits"] = 0
                    row["hard_hit_pct"] = None

                # Sweet spot: launch angle 8-32 degrees
                if "launch_angle" in batted.columns:
                    la = batted["launch_angle"].dropna()
                    sweet = int(((la >= 8) & (la <= 32)).sum())
                    row["sweet_spot_pct"] = round(sweet / bb_count, 4)
                else:
                    row["sweet_spot_pct"] = None
            else:
                row["barrels"] = 0
                row["hard_hits"] = 0
                row["barrel_pct"] = None
                row["hard_hit_pct"] = None
                row["sweet_spot_pct"] = None

            # Expected stats (on batted balls)
            if not batted.empty:
                row["xba"] = _col_mean(batted, "estimated_ba_using_speedangle")
                row["xslg"] = _col_mean(batted, "estimated_slg_using_speedangle") if "estimated_slg_using_speedangle" in batted.columns else None
                row["xwoba"] = _col_mean(batted, "estimated_woba_using_speedangle")
                row["woba"] = _col_mean(batted, "woba_value")
            else:
                row["xba"] = None
                row["xslg"] = None
                row["xwoba"] = None
                row["woba"] = None

            # Batted ball type distribution
            if bb_count > 0 and "bb_type" in batted.columns:
                bb_types = batted["bb_type"].value_counts()
                row["gb_pct"] = round(bb_types.get("ground_ball", 0) / bb_count, 4)
                row["fb_pct"] = round(bb_types.get("fly_ball", 0) / bb_count, 4)
                row["ld_pct"] = round(bb_types.get("line_drive", 0) / bb_count, 4)
                row["popup_pct"] = round(bb_types.get("popup", 0) / bb_count, 4)
            else:
                row["gb_pct"] = None
                row["fb_pct"] = None
                row["ld_pct"] = None
                row["popup_pct"] = None

            # Spray direction
            if bb_count > 0 and "hc_x" in batted.columns:
                hc = batted["hc_x"].dropna()
                if not hc.empty:
                    # Baseball Savant: hc_x 125.42 is center. Pull depends on batter hand
                    # Simplified: < 100 = pull for RHB, > 150 = pull for RHB
                    # Use a simple L/C/R split: < 95 = pull, 95-155 = center, > 155 = oppo (RHB)
                    # Since we don't have stand info per pitch reliably, use absolute spray
                    pull = int((hc < 100).sum())
                    center = int(((hc >= 100) & (hc <= 150)).sum())
                    oppo = int((hc > 150).sum())
                    total_spray = pull + center + oppo
                    if total_spray > 0:
                        row["pull_pct"] = round(pull / total_spray, 4)
                        row["center_pct"] = round(center / total_spray, 4)
                        row["oppo_pct"] = round(oppo / total_spray, 4)
                    else:
                        row["pull_pct"] = None
                        row["center_pct"] = None
                        row["oppo_pct"] = None
                else:
                    row["pull_pct"] = None
                    row["center_pct"] = None
                    row["oppo_pct"] = None
            else:
                row["pull_pct"] = None
                row["center_pct"] = None
                row["oppo_pct"] = None

            # Plate discipline
            total_pitches = len(group)
            if total_pitches > 0 and "zone" in group.columns:
                zones = group["zone"].dropna()
                in_zone = int((zones.between(1, 9)).sum())
                out_zone = int((zones > 9).sum())
                row["zone_pct"] = round(in_zone / total_pitches, 4) if total_pitches > 0 else None

                # Chase: swings at pitches outside zone
                if out_zone > 0 and "description" in group.columns:
                    outside = group[group["zone"].between(11, 14) | (group["zone"] > 9)]
                    outside_swings = outside[outside["description"].isin(SWING_EVENTS)]
                    row["chase_pct"] = round(len(outside_swings) / out_zone, 4)
                else:
                    row["chase_pct"] = None
            else:
                row["zone_pct"] = None
                row["chase_pct"] = None

            # Whiff rate
            if len(swings) > 0 and "description" in group.columns:
                whiffs = group[group["description"].isin(WHIFF_EVENTS)]
                row["whiff_pct"] = round(len(whiffs) / len(swings), 4)
            else:
                row["whiff_pct"] = None

            results.append(row)

        return pd.DataFrame(results)

    def _aggregate_pitching(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate pitch-level data into per-pitcher-game rows."""
        if "pitcher" not in df.columns:
            return pd.DataFrame()

        results = []
        for (pitcher, game_date), group in df.groupby(["pitcher", "game_date"]):
            row = {"pitcher": pitcher, "game_date": game_date}

            total_pitches = len(group)
            row["total_pitches"] = total_pitches

            # Batted balls against
            batted = group[group["type"] == "X"] if "type" in group.columns else pd.DataFrame()
            bb_count = len(batted)
            row["batted_balls_against"] = bb_count

            # Swings against
            swings = group[group["description"].isin(SWING_EVENTS)] if "description" in group.columns else pd.DataFrame()
            row["total_swings_against"] = len(swings)

            # Contact quality against
            if not batted.empty and "launch_speed" in batted.columns:
                ev = batted["launch_speed"].dropna()
                row["avg_exit_velocity_against"] = round(ev.mean(), 1) if not ev.empty else None
                row["max_exit_velocity_against"] = round(ev.max(), 1) if not ev.empty else None
            else:
                row["avg_exit_velocity_against"] = None
                row["max_exit_velocity_against"] = None

            if bb_count > 0:
                if "barrel" in batted.columns:
                    barrel_count = int(batted["barrel"].sum())
                    row["barrels_against"] = barrel_count
                    row["barrel_pct_against"] = round(barrel_count / bb_count, 4)
                else:
                    row["barrels_against"] = 0
                    row["barrel_pct_against"] = None

                if "launch_speed" in batted.columns:
                    hard = batted["launch_speed"].dropna()
                    row["hard_hit_pct_against"] = round(int((hard >= 95).sum()) / bb_count, 4)
                else:
                    row["hard_hit_pct_against"] = None
            else:
                row["barrels_against"] = 0
                row["barrel_pct_against"] = None
                row["hard_hit_pct_against"] = None

            # Expected stats against
            if not batted.empty:
                row["xba_against"] = _col_mean(batted, "estimated_ba_using_speedangle")
                row["xslg_against"] = _col_mean(batted, "estimated_slg_using_speedangle") if "estimated_slg_using_speedangle" in batted.columns else None
                row["xwoba_against"] = _col_mean(batted, "estimated_woba_using_speedangle")
            else:
                row["xba_against"] = None
                row["xslg_against"] = None
                row["xwoba_against"] = None

            # xERA: approximate from xwOBA against (simplified)
            # xERA ≈ ((xwOBA - 0.310) / 1.157) * 9 + 3.10 (rough conversion)
            xwoba_against = row.get("xwoba_against")
            if xwoba_against is not None:
                row["xera"] = round(((xwoba_against - 0.310) / 1.157) * 9 + 3.10, 2)
            else:
                row["xera"] = None

            # Velocity & spin (fastballs)
            if "pitch_type" in group.columns:
                fastballs = group[group["pitch_type"].isin(FASTBALL_TYPES)]
                breaking = group[group["pitch_type"].isin(BREAKING_TYPES)]
                offspeed = group[group["pitch_type"].isin(OFFSPEED_TYPES)]

                if not fastballs.empty and "release_speed" in fastballs.columns:
                    velo = fastballs["release_speed"].dropna()
                    row["avg_fastball_velo"] = round(velo.mean(), 1) if not velo.empty else None
                    row["max_fastball_velo"] = round(velo.max(), 1) if not velo.empty else None
                else:
                    row["avg_fastball_velo"] = None
                    row["max_fastball_velo"] = None

                if not fastballs.empty and "release_spin_rate" in fastballs.columns:
                    spin = fastballs["release_spin_rate"].dropna()
                    row["avg_fastball_spin"] = round(spin.mean(), 0) if not spin.empty else None
                else:
                    row["avg_fastball_spin"] = None

                if not breaking.empty and "release_spin_rate" in breaking.columns:
                    spin = breaking["release_spin_rate"].dropna()
                    row["avg_breaking_spin"] = round(spin.mean(), 0) if not spin.empty else None
                else:
                    row["avg_breaking_spin"] = None

                # Pitch mix
                if total_pitches > 0:
                    row["fastball_pct"] = round(len(fastballs) / total_pitches, 4)
                    row["breaking_pct"] = round(len(breaking) / total_pitches, 4)
                    row["offspeed_pct"] = round(len(offspeed) / total_pitches, 4)
                else:
                    row["fastball_pct"] = None
                    row["breaking_pct"] = None
                    row["offspeed_pct"] = None
            else:
                row["avg_fastball_velo"] = None
                row["max_fastball_velo"] = None
                row["avg_fastball_spin"] = None
                row["avg_breaking_spin"] = None
                row["fastball_pct"] = None
                row["breaking_pct"] = None
                row["offspeed_pct"] = None

            # Batted ball distribution
            if bb_count > 0 and "bb_type" in batted.columns:
                bb_types = batted["bb_type"].value_counts()
                row["gb_pct"] = round(bb_types.get("ground_ball", 0) / bb_count, 4)
                row["fb_pct"] = round(bb_types.get("fly_ball", 0) / bb_count, 4)
                row["ld_pct"] = round(bb_types.get("line_drive", 0) / bb_count, 4)
                row["popup_pct"] = round(bb_types.get("popup", 0) / bb_count, 4)
            else:
                row["gb_pct"] = None
                row["fb_pct"] = None
                row["ld_pct"] = None
                row["popup_pct"] = None

            # Plate discipline (from pitcher perspective)
            if total_pitches > 0 and "zone" in group.columns:
                zones = group["zone"].dropna()
                in_zone = int((zones.between(1, 9)).sum())
                out_zone = int((zones > 9).sum())
                row["zone_pct"] = round(in_zone / total_pitches, 4)

                if out_zone > 0 and "description" in group.columns:
                    outside = group[group["zone"].between(11, 14) | (group["zone"] > 9)]
                    outside_swings = outside[outside["description"].isin(SWING_EVENTS)]
                    row["chase_pct"] = round(len(outside_swings) / out_zone, 4)
                else:
                    row["chase_pct"] = None
            else:
                row["zone_pct"] = None
                row["chase_pct"] = None

            # Whiff rate
            if len(swings) > 0 and "description" in group.columns:
                whiffs = group[group["description"].isin(WHIFF_EVENTS)]
                row["whiff_pct"] = round(len(whiffs) / len(swings), 4)
            else:
                row["whiff_pct"] = None

            # CSW% (called strikes + whiffs / total pitches)
            if total_pitches > 0 and "description" in group.columns:
                called_strikes = group[group["description"] == "called_strike"]
                whiffs = group[group["description"].isin(WHIFF_EVENTS)]
                row["csw_pct"] = round((len(called_strikes) + len(whiffs)) / total_pitches, 4)
            else:
                row["csw_pct"] = None

            results.append(row)

        return pd.DataFrame(results)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_player(self, conn, player_id: int):
        """Insert player stub if not in mlb_players (name updated by boxscore scraper)."""
        conn.execute(
            text("""
                INSERT INTO mlb_players (player_id, player_name)
                VALUES (:pid, :name)
                ON CONFLICT (player_id) DO NOTHING
            """),
            {"pid": player_id, "name": f"Player {player_id}"},
        )


# ------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------

def _safe_float(val) -> float | None:
    """Convert to float, returning None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        if np.isnan(f):
            return None
        return round(f, 4)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    """Convert to int, returning None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        if np.isnan(f):
            return None
        return int(f)
    except (ValueError, TypeError):
        return None


def _col_mean(df: pd.DataFrame, col: str) -> float | None:
    """Safely compute mean of a column."""
    if col not in df.columns:
        return None
    vals = df[col].dropna()
    if vals.empty:
        return None
    return round(float(vals.mean()), 4)


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="MLB Statcast Scraper")
    parser.add_argument("--date", type=str, help="Scrape specific date (YYYY-MM-DD)")
    parser.add_argument("--yesterday", action="store_true", help="Scrape yesterday's games")
    parser.add_argument("--backfill", action="store_true", help="Backfill date range")
    parser.add_argument("--start-date", type=str, help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Backfill end date (YYYY-MM-DD)")

    args = parser.parse_args()

    engine = get_engine()
    scraper = MLBStatcastScraper(engine)

    print("=" * 60)
    print("MLB STATCAST SCRAPER")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.yesterday:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        result = scraper.scrape_day(date_str)
        print(f"  {date_str}: {result['batting_rows']} batting, {result['pitching_rows']} pitching")

    elif args.backfill:
        if not args.start_date or not args.end_date:
            print("ERROR: --backfill requires --start-date and --end-date")
            sys.exit(1)

        start = datetime.strptime(args.start_date, "%Y-%m-%d")
        end = datetime.strptime(args.end_date, "%Y-%m-%d")
        current = start
        total_batting = 0
        total_pitching = 0

        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            result = scraper.scrape_day(date_str)
            total_batting += result["batting_rows"]
            total_pitching += result["pitching_rows"]
            print(f"  {date_str}: {result['batting_rows']} batting, {result['pitching_rows']} pitching")
            current += timedelta(days=1)
            time.sleep(1)  # Rate limit per pybaseball recommendations

        print(f"\nBackfill totals: {total_batting} batting, {total_pitching} pitching rows")

    elif args.date:
        result = scraper.scrape_day(args.date)
        print(f"  {args.date}: {result['batting_rows']} batting, {result['pitching_rows']} pitching")

    else:
        print("ERROR: Specify --date, --yesterday, or --backfill")
        sys.exit(1)

    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
