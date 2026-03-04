"""
NCAAB Barttorvik Ratings Scraper
===================================
Downloads bulk CSV from barttorvik.com and stores snapshots into
ncaab_barttorvik_ratings for point-in-time adjusted efficiency metrics.

CSV URL pattern:
    https://barttorvik.com/{season}_team_results.csv

Updated every ~15 minutes during the season. No API key required.

Usage:
    python -m src.scrapers.ncaab.ncaab_barttorvik_scraper --season 2025
    python -m src.scrapers.ncaab.ncaab_barttorvik_scraper --season 2025 --date 2025-01-15
    python -m src.scrapers.ncaab.ncaab_barttorvik_scraper --backfill 2022 2023 2024 2025
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine

sys.path.append(str(Path(__file__).resolve().parents[3]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("NCAABBarttorvik")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

BASE_URL = "https://barttorvik.com/{year}_team_results.csv"

# Flexible column mapping: our DB column -> list of possible CSV header names
# Barttorvik's headers change slightly between seasons
COLUMN_MAP = {
    "team_name":    ["Team", "team", "TEAM"],
    "adj_oe":       ["AdjOE", "OE", "Adj. OE", "ADJOE"],
    "adj_de":       ["AdjDE", "DE", "Adj. DE", "ADJDE"],
    "adj_tempo":    ["AdjT", "AdjTempo", "T", "ADJT"],
    "barthag":      ["Barthag", "barthag", "BARTHAG"],
    "efg_pct":      ["eFG%", "EFG%", "eFG_pct", "EFG"],
    "opp_efg_pct":  ["EFG%D", "eFG%D", "OppEFG", "EFGD"],
    "tov_pct":      ["TOR", "TOV%", "TO%"],
    "opp_tov_pct":  ["TORD", "OppTOV%", "OppTO%"],
    "orb_pct":      ["ORB", "OR%", "ORB%"],
    "opp_orb_pct":  ["DRB", "OppOR%", "OPRB"],
    "ft_rate":      ["FTR", "FT Rate", "FT_RATE"],
    "opp_ft_rate":  ["FTRD", "OppFTR", "OPP_FTR"],
    "adj_oe_rank":  ["OE Rank", "OE_Rank", "OERK"],
    "adj_de_rank":  ["DE Rank", "DE_Rank", "DERK"],
    "adj_em_rank":  ["Rk", "Rank", "RK"],
    "games_played": ["G", "Games", "GP"],
}


class BarttorviKScraper:
    def __init__(self, engine):
        self.engine = engine
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (GameFlowData Analytics)",
        })

    def fetch_csv(self, season: int) -> pd.DataFrame | None:
        """Download bulk CSV from barttorvik.com.

        Args:
            season: Season end year (e.g., 2025 for 2024-25)

        Returns:
            Parsed DataFrame or None on failure.
        """
        url = BASE_URL.format(year=season)
        logger.info(f"Fetching {url}")

        for attempt in range(3):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    df = pd.read_csv(StringIO(response.text))
                    logger.info(f"Parsed {len(df)} teams from CSV")
                    return df
                elif response.status_code == 404:
                    logger.error(f"Season {season} CSV not found (404)")
                    return None
                else:
                    logger.warning(f"HTTP {response.status_code}, retrying...")
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.warning(f"Fetch error: {e}")
                time.sleep(2 * (attempt + 1))

        logger.error("Failed to fetch CSV after 3 attempts")
        return None

    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map Barttorvik CSV columns to our schema names.

        Handles year-to-year header variations via COLUMN_MAP.
        """
        result = {}

        for our_col, candidates in COLUMN_MAP.items():
            matched = False
            for csv_col in candidates:
                if csv_col in df.columns:
                    result[our_col] = df[csv_col]
                    matched = True
                    break
            if not matched:
                result[our_col] = None

        out = pd.DataFrame(result)

        # Compute adj_em if not present (AdjOE - AdjDE)
        if "adj_em" not in out.columns or out.get("adj_em") is None:
            # Try to find it in the original CSV
            for col_name in ["AdjEM", "AdjNEM", "EM", "NEM"]:
                if col_name in df.columns:
                    out["adj_em"] = pd.to_numeric(df[col_name], errors="coerce")
                    break
            else:
                # Compute from components
                if out["adj_oe"] is not None and out["adj_de"] is not None:
                    out["adj_em"] = pd.to_numeric(out["adj_oe"], errors="coerce") - pd.to_numeric(out["adj_de"], errors="coerce")
                else:
                    out["adj_em"] = None

        # Convert numeric columns
        numeric_cols = [
            "adj_oe", "adj_de", "adj_em", "adj_tempo", "barthag",
            "efg_pct", "opp_efg_pct", "tov_pct", "opp_tov_pct",
            "orb_pct", "opp_orb_pct", "ft_rate", "opp_ft_rate",
            "adj_oe_rank", "adj_de_rank", "adj_em_rank", "games_played",
        ]
        for col in numeric_cols:
            if col in out.columns and out[col] is not None:
                out[col] = pd.to_numeric(out[col], errors="coerce")

        # Drop rows with no team name
        if "team_name" in out.columns:
            out = out.dropna(subset=["team_name"])
            out["team_name"] = out["team_name"].astype(str).str.strip()

        return out

    def store_snapshot(self, df: pd.DataFrame, season: int,
                       snapshot_date: date | None = None) -> int:
        """Batch-insert into ncaab_barttorvik_ratings.

        Uses ON CONFLICT (team_name, season, snapshot_date) DO NOTHING
        to prevent duplicate snapshots.

        Returns:
            Rows inserted.
        """
        if snapshot_date is None:
            snapshot_date = date.today()

        rows = []
        for _, row in df.iterrows():
            rows.append((
                row.get("team_name"),
                season,
                snapshot_date,
                row.get("adj_oe"),
                row.get("adj_de"),
                row.get("adj_em"),
                row.get("adj_tempo"),
                row.get("barthag"),
                row.get("efg_pct"),
                row.get("tov_pct"),
                row.get("orb_pct"),
                row.get("ft_rate"),
                row.get("opp_efg_pct"),
                row.get("opp_tov_pct"),
                row.get("opp_orb_pct"),
                row.get("opp_ft_rate"),
                row.get("adj_oe_rank"),
                row.get("adj_de_rank"),
                row.get("adj_em_rank"),
                row.get("games_played"),
            ))

        if not rows:
            return 0

        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO ncaab_barttorvik_ratings
                    (team_name, season, snapshot_date,
                     adj_oe, adj_de, adj_em, adj_tempo, barthag,
                     efg_pct, tov_pct, orb_pct, ft_rate,
                     opp_efg_pct, opp_tov_pct, opp_orb_pct, opp_ft_rate,
                     adj_oe_rank, adj_de_rank, adj_em_rank, games_played)
                    VALUES %s
                    ON CONFLICT (team_name, season, snapshot_date) DO NOTHING
                """
                extras.execute_values(cur, query, rows)
            conn.commit()
        finally:
            conn.close()

        logger.info(f"Stored {len(rows)} ratings for season {season}, snapshot {snapshot_date}")
        return len(rows)

    def scrape_season(self, season: int, snapshot_date: date | None = None) -> int:
        """Full pipeline: fetch CSV -> normalize -> store.

        Returns:
            Rows stored.
        """
        df = self.fetch_csv(season)
        if df is None:
            return 0

        normalized = self.normalize_columns(df)
        logger.info(f"Normalized {len(normalized)} teams with columns: {list(normalized.columns)}")

        return self.store_snapshot(normalized, season, snapshot_date)

    def backfill_seasons(self, seasons: list[int]) -> None:
        """Download and store one snapshot per historical season."""
        for season in seasons:
            logger.info(f"--- Backfilling season {season} ---")
            # Use a representative end-of-season date for historical snapshots
            snap_date = date(season, 4, 10)  # Post-tournament
            count = self.scrape_season(season, snapshot_date=snap_date)
            logger.info(f"Season {season}: {count} teams stored")
            time.sleep(1)


if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("Missing DATABASE_URL.")
        sys.exit(1)

    engine = create_engine(DATABASE_URL)
    scraper = BarttorviKScraper(engine)

    parser = argparse.ArgumentParser(description="NCAAB Barttorvik Ratings Scraper")
    parser.add_argument("--season", type=int, help="Season end year (e.g., 2025)")
    parser.add_argument("--date", type=str, help="Snapshot date override (YYYY-MM-DD)")
    parser.add_argument("--backfill", nargs="+", type=int, help="Backfill multiple seasons")

    args = parser.parse_args()

    if args.backfill:
        scraper.backfill_seasons(args.backfill)
    elif args.season:
        snap_date = None
        if args.date:
            snap_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        scraper.scrape_season(args.season, snapshot_date=snap_date)
    else:
        logger.error("Specify --season or --backfill")
        sys.exit(1)
