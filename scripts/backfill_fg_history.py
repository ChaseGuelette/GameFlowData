"""Backfill mlb_player_season_advanced_history for past seasons.

The FanGraphs season-leaderboard API (pybaseball.pitching_stats / batting_stats)
does NOT accept date filters — it returns end-of-season values. We cannot
reconstruct point-in-time FanGraphs metrics from it.

Strategy: use pybaseball.{pitching,batting}_stats_range (Baseball Reference)
which DOES accept date ranges. BR has raw component stats (IP, BB, SO, HR, BF,
H, AB, etc.) from which we derive the metrics the feature stores care about:

    pitcher:
      k_per_9   = SO9 (BR direct)
      bb_per_9  = BB / IP * 9
      hr_per_9  = HR / IP * 9
      k_pct     = SO / BF
      bb_pct    = BB / BF
      babip     = BAbip (BR direct)
      era       = ERA (BR direct)
      ip        = IP (BR direct)
      fip       = ((13*HR) + (3*BB) - (2*SO)) / IP + 3.10 (FIP constant ~2024-25)
      gb_pct    = GB/FB ratio mapped to a percentage (approx)
      NULL: xfip, xera, siera, lob_pct, war  (FanGraphs-only)

    batter:
      avg, obp, slg, ops, pa  (BR direct, "BA" -> "avg")
      k_pct     = SO / PA
      bb_pct    = BB / PA
      iso       = SLG - AVG
      babip     = (H - HR) / (AB - SO - HR + SF)
      NULL: wrc_plus, woba, hard_pct, war  (FanGraphs-only)

mlbID column from BR == our player_id (verified). No name matching needed.

Going forward, the daily FanGraphs scraper (mlb_fangraphs_scraper.py) writes
FULL FanGraphs columns including the FG-only fields. So 2026+ rows are richer
than these BR-derived backfill rows.

Usage:
    venv/Scripts/python.exe scripts/backfill_fg_history.py --local --seasons 2022 2023 2024 2025
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.db.client import get_engine  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_fg_history")

# Roughly captures regular-season + early postseason for MLB.
SEASON_START_MONTHDAY = (3, 20)  # March 20
SEASON_END_MONTHDAY = (10, 5)    # October 5
TRADE_DEADLINE = (7, 31)         # July 31 (snapshot day-of)

# FIP constants by season (approximate league-avg cFIP). Sourced from FanGraphs
# year-end constants for these years. Off by a few hundredths but close enough
# for our use as a model feature.
FIP_CONSTANT = {
    2022: 3.09,
    2023: 3.13,
    2024: 3.13,
    2025: 3.13,
    2026: 3.13,  # placeholder until 2026 final cFIP is published
}


def get_snapshot_dates(season: int) -> list[date]:
    """Mondays in season + trade-deadline date."""
    start = date(season, *SEASON_START_MONTHDAY)
    end = date(season, *SEASON_END_MONTHDAY)
    dates: list[date] = []
    d = start
    while d <= end:
        if d.weekday() == 0:  # Monday
            dates.append(d)
        d += timedelta(days=1)
    deadline = date(season, *TRADE_DEADLINE)
    if deadline not in dates:
        dates.append(deadline)
    dates.sort()
    return dates


def _safe_float(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        if np.isnan(f) or np.isinf(f):
            return None
        return round(f, 4)
    except (ValueError, TypeError):
        return None


def _safe_int(v) -> int | None:
    if v is None:
        return None
    try:
        f = float(v)
        if np.isnan(f):
            return None
        return int(f)
    except (ValueError, TypeError):
        return None


def derive_pitcher_row(row: pd.Series, season: int) -> dict:
    """Map a Baseball-Reference pitcher_stats_range row to history-table fields."""
    ip = _safe_float(row.get("IP")) or 0.0
    bf = _safe_float(row.get("BF")) or 0.0
    so = _safe_float(row.get("SO")) or 0.0
    bb = _safe_float(row.get("BB")) or 0.0
    hr = _safe_float(row.get("HR")) or 0.0
    so9 = _safe_float(row.get("SO9"))

    k_pct = round(so / bf, 4) if bf > 0 else None
    bb_pct = round(bb / bf, 4) if bf > 0 else None
    bb_per_9 = round(bb / ip * 9, 4) if ip > 0 else None
    hr_per_9 = round(hr / ip * 9, 4) if ip > 0 else None
    fip = None
    if ip > 0:
        fip_const = FIP_CONSTANT.get(season, 3.13)
        fip = round(((13 * hr) + (3 * bb) - (2 * so)) / ip + fip_const, 4)
    # BR has GB/FB ratio. Convert to GB%: GB / (GB + FB + LD + PU) — approximate.
    gb_ratio = _safe_float(row.get("GB/FB"))
    ld = _safe_float(row.get("LD")) or 0.0
    pu = _safe_float(row.get("PU")) or 0.0
    gb_pct = None
    if gb_ratio is not None and gb_ratio > 0:
        # GB / (GB + FB) = gb_ratio / (gb_ratio + 1). Then scale down by LD+PU share.
        gb_share_of_gb_fb = gb_ratio / (gb_ratio + 1.0)
        gb_share_overall = gb_share_of_gb_fb * (1.0 - ld - pu)
        gb_pct = round(gb_share_overall, 4)

    return {
        "ip": _safe_float(row.get("IP")),
        "era": _safe_float(row.get("ERA")),
        "babip": _safe_float(row.get("BAbip")),
        "k_per_9": so9,
        "k_pct": k_pct,
        "bb_per_9": bb_per_9,
        "bb_pct": bb_pct,
        "hr_per_9": hr_per_9,
        "fip": fip,
        "gb_pct": gb_pct,
        # FG-only — leave NULL for BR backfill
        "war": None,
        "xfip": None,
        "xera": None,
        "siera": None,
        "lob_pct": None,
        "fangraphs_id": None,
    }


def derive_batter_row(row: pd.Series) -> dict:
    """Map a Baseball-Reference batter_stats_range row to history-table fields."""
    pa = _safe_float(row.get("PA")) or 0.0
    ab = _safe_float(row.get("AB")) or 0.0
    h = _safe_float(row.get("H")) or 0.0
    hr = _safe_float(row.get("HR")) or 0.0
    so = _safe_float(row.get("SO")) or 0.0
    bb = _safe_float(row.get("BB")) or 0.0
    sf = _safe_float(row.get("SF")) or 0.0
    avg = _safe_float(row.get("BA"))
    obp = _safe_float(row.get("OBP"))
    slg = _safe_float(row.get("SLG"))
    ops = _safe_float(row.get("OPS"))

    k_pct = round(so / pa, 4) if pa > 0 else None
    bb_pct = round(bb / pa, 4) if pa > 0 else None
    iso = round(slg - avg, 4) if (slg is not None and avg is not None) else None
    babip = None
    denom = ab - so - hr + sf
    if denom > 0:
        babip = round((h - hr) / denom, 4)

    return {
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "pa": _safe_int(pa),
        "k_pct": k_pct,
        "bb_pct": bb_pct,
        "iso": iso,
        "babip": babip,
        # FG-only — leave NULL for BR backfill
        "war": None,
        "wrc_plus": None,
        "woba": None,
        "hard_pct": None,
        "fangraphs_id": None,
    }


def insert_pitcher_rows(engine, season: int, as_of_date: date, df: pd.DataFrame) -> int:
    n = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            mlb_id = _safe_int(row.get("mlbID"))
            if not mlb_id:
                continue
            lev = str(row.get("Lev") or "")
            if not lev.startswith("Maj"):
                continue
            derived = derive_pitcher_row(row, season)
            params = {
                "player_id": mlb_id,
                "season": season,
                "as_of_date": as_of_date,
                **derived,
            }
            conn.execute(
                text("""
                    INSERT INTO mlb_player_season_advanced_history
                        (player_id, season, player_type, as_of_date,
                         war, babip, fip, xfip, xera, siera, era,
                         lob_pct, gb_pct, k_per_9, k_pct, bb_per_9, bb_pct,
                         hr_per_9, ip, fangraphs_id)
                    VALUES
                        (:player_id, :season, 'pitcher', :as_of_date,
                         :war, :babip, :fip, :xfip, :xera, :siera, :era,
                         :lob_pct, :gb_pct, :k_per_9, :k_pct, :bb_per_9, :bb_pct,
                         :hr_per_9, :ip, :fangraphs_id)
                    ON CONFLICT (player_id, season, player_type, as_of_date) DO NOTHING
                """),
                params,
            )
            n += 1
    return n


def insert_batter_rows(engine, season: int, as_of_date: date, df: pd.DataFrame) -> int:
    n = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            mlb_id = _safe_int(row.get("mlbID"))
            if not mlb_id:
                continue
            lev = str(row.get("Lev") or "")
            if not lev.startswith("Maj"):
                continue
            derived = derive_batter_row(row)
            params = {
                "player_id": mlb_id,
                "season": season,
                "as_of_date": as_of_date,
                **derived,
            }
            conn.execute(
                text("""
                    INSERT INTO mlb_player_season_advanced_history
                        (player_id, season, player_type, as_of_date,
                         war, babip, wrc_plus, woba, iso,
                         bb_pct, k_pct, hard_pct,
                         avg, obp, slg, ops, pa, fangraphs_id)
                    VALUES
                        (:player_id, :season, 'batter', :as_of_date,
                         :war, :babip, :wrc_plus, :woba, :iso,
                         :bb_pct, :k_pct, :hard_pct,
                         :avg, :obp, :slg, :ops, :pa, :fangraphs_id)
                    ON CONFLICT (player_id, season, player_type, as_of_date) DO NOTHING
                """),
                params,
            )
            n += 1
    return n


def run(seasons: list[int], local: bool, sleep_s: float) -> None:
    if local:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
        from sqlalchemy import create_engine
        url = os.getenv("LOCAL_DATABASE_URL")
        if not url:
            raise RuntimeError("LOCAL_DATABASE_URL not set in .env")
        engine = create_engine(url)
    else:
        engine = get_engine()

    from pybaseball import pitching_stats_range, batting_stats_range  # type: ignore

    for season in seasons:
        season_start = date(season, *SEASON_START_MONTHDAY)
        snapshots = get_snapshot_dates(season)
        logger.info(
            "Season %d: %d snapshot dates (%s to %s)",
            season, len(snapshots), snapshots[0], snapshots[-1],
        )
        for snap in snapshots:
            start_s = season_start.strftime("%Y-%m-%d")
            end_s = snap.strftime("%Y-%m-%d")

            # Pitchers
            try:
                p_df = pitching_stats_range(start_s, end_s)
            except Exception as exc:
                logger.warning("pitching_stats_range failed for %s..%s: %s", start_s, end_s, exc)
                p_df = pd.DataFrame()
            time.sleep(sleep_s)

            # Batters
            try:
                b_df = batting_stats_range(start_s, end_s)
            except Exception as exc:
                logger.warning("batting_stats_range failed for %s..%s: %s", start_s, end_s, exc)
                b_df = pd.DataFrame()
            time.sleep(sleep_s)

            n_p = insert_pitcher_rows(engine, season, snap, p_df) if not p_df.empty else 0
            n_b = insert_batter_rows(engine, season, snap, b_df) if not b_df.empty else 0
            logger.info(
                "  %s: %d pitchers, %d batters inserted",
                snap, n_p, n_b,
            )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", type=int, nargs="+", default=[2022, 2023, 2024, 2025])
    ap.add_argument("--local", action="store_true", help="Use LOCAL_DATABASE_URL")
    ap.add_argument("--sleep", type=float, default=1.5, help="Sleep seconds between BR calls")
    args = ap.parse_args()
    run(args.seasons, args.local, args.sleep)


if __name__ == "__main__":
    main()
