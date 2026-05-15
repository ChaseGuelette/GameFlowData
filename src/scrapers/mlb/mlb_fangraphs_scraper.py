"""
MLB FanGraphs Advanced Stats Scraper
=====================================
Scrapes season-level advanced stats from FanGraphs via pybaseball.
Populates: mlb_player_season_advanced

Maps FanGraphs player names to our mlb_players.player_id using name matching,
with fallback to pybaseball.playerid_lookup() for MLBAM crosswalk.

Usage:
    python -m src.scrapers.mlb.mlb_fangraphs_scraper --season 2025
    python -m src.scrapers.mlb.mlb_fangraphs_scraper --season 2025 --pitching-only
    python -m src.scrapers.mlb.mlb_fangraphs_scraper --season 2025 --batting-only
    python -m src.scrapers.mlb.mlb_fangraphs_scraper --all-seasons
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
from pybaseball import batting_stats, pitching_stats, playerid_reverse_lookup
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBFanGraphsScraper")

DEFAULT_SEASONS = [2022, 2023, 2024, 2025]
# Minimum PA/IP to qualify (FanGraphs defaults)
MIN_PA = 50
MIN_IP = 10


class MLBFanGraphsScraper:
    def __init__(self, engine):
        self.engine = engine
        self._player_cache = {}  # name -> player_id
        self._fg_to_mlbam_cache = {}  # fangraphs_id -> mlbam_id
        self._load_player_cache()

    def _load_player_cache(self):
        """Load existing players into name -> player_id cache."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT player_id, player_name FROM mlb_players"))
            for row in result:
                self._player_cache[row[1].lower()] = row[0]
        logger.info(f"Loaded {len(self._player_cache)} players into cache.")

    def scrape_season_batting(self, season: int, as_of_date: date | None = None) -> int:
        """Fetch and upsert FanGraphs batting stats for a season.

        Writes to legacy mlb_player_season_advanced (UPSERT, latest wins) AND to
        mlb_player_season_advanced_history (INSERT with as_of_date, idempotent
        per snapshot date). The history table is the safe one — feature stores
        join with `as_of_date < game_date` to avoid the season-only leak.

        Args:
            season: Season year (e.g. 2025).
            as_of_date: Snapshot date for the history row. Defaults to yesterday
                so the snapshot represents stats through end of yesterday and is
                safe to use for any game on today or later (strict `<` semantics).

        Returns:
            Number of rows upserted.
        """
        if as_of_date is None:
            as_of_date = date.today() - timedelta(days=1)
        logger.info(f"Fetching FanGraphs batting stats for {season} (as_of {as_of_date})...")

        try:
            df = batting_stats(season, qual=MIN_PA)
        except Exception as e:
            logger.error(f"Failed to fetch batting stats for {season}: {e}")
            return 0

        if df is None or df.empty:
            logger.info(f"No batting stats for {season}.")
            return 0

        logger.info(f"  Fetched {len(df)} qualified batters for {season}.")

        count = 0
        unmatched = []

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                name = str(row.get("Name", ""))
                fg_id = row.get("IDfg")
                player_id = self._resolve_player_id(name, fg_id)

                if player_id is None:
                    unmatched.append(name)
                    continue

                self._ensure_player(conn, player_id, name)
                params = {
                    "player_id": player_id,
                    "season": season,
                    "as_of_date": as_of_date,
                    "war": _safe_float(row.get("WAR")),
                    "babip": _safe_float(row.get("BABIP")),
                    "wrc_plus": _safe_float(row.get("wRC+")),
                    "woba": _safe_float(row.get("wOBA")),
                    "iso": _safe_float(row.get("ISO")),
                    "bb_pct": _pct_to_float(row.get("BB%")),
                    "k_pct": _pct_to_float(row.get("K%")),
                    "hard_pct": _pct_to_float(row.get("Hard%")),
                    "avg": _safe_float(row.get("AVG")),
                    "obp": _safe_float(row.get("OBP")),
                    "slg": _safe_float(row.get("SLG")),
                    "ops": _safe_float(row.get("OPS")),
                    "pa": _safe_int(row.get("PA")),
                    "fangraphs_id": _safe_int(fg_id),
                }
                conn.execute(
                    text("""
                        INSERT INTO mlb_player_season_advanced
                            (player_id, season, player_type,
                             war, babip, wrc_plus, woba, iso,
                             bb_pct, k_pct, hard_pct,
                             avg, obp, slg, ops, pa, fangraphs_id)
                        VALUES
                            (:player_id, :season, 'batter',
                             :war, :babip, :wrc_plus, :woba, :iso,
                             :bb_pct, :k_pct, :hard_pct,
                             :avg, :obp, :slg, :ops, :pa, :fangraphs_id)
                        ON CONFLICT (player_id, season, player_type) DO UPDATE SET
                             war = EXCLUDED.war,
                             babip = EXCLUDED.babip,
                             wrc_plus = EXCLUDED.wrc_plus,
                             woba = EXCLUDED.woba,
                             iso = EXCLUDED.iso,
                             bb_pct = EXCLUDED.bb_pct,
                             k_pct = EXCLUDED.k_pct,
                             hard_pct = EXCLUDED.hard_pct,
                             avg = EXCLUDED.avg,
                             obp = EXCLUDED.obp,
                             slg = EXCLUDED.slg,
                             ops = EXCLUDED.ops,
                             pa = EXCLUDED.pa,
                             fangraphs_id = EXCLUDED.fangraphs_id
                    """),
                    params,
                )
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
                count += 1

        if unmatched:
            logger.warning(f"  Could not match {len(unmatched)} batters: {unmatched[:10]}{'...' if len(unmatched) > 10 else ''}")

        logger.info(f"  Upserted {count} batting rows for {season}.")
        return count

    def scrape_season_pitching(self, season: int, as_of_date: date | None = None) -> int:
        """Fetch and upsert FanGraphs pitching stats for a season.

        Writes to legacy mlb_player_season_advanced (UPSERT, latest wins) AND to
        mlb_player_season_advanced_history (INSERT with as_of_date). See
        scrape_season_batting for the as_of_date semantics.

        Returns:
            Number of rows upserted.
        """
        if as_of_date is None:
            as_of_date = date.today() - timedelta(days=1)
        logger.info(f"Fetching FanGraphs pitching stats for {season} (as_of {as_of_date})...")

        try:
            df = pitching_stats(season, qual=MIN_IP)
        except Exception as e:
            logger.error(f"Failed to fetch pitching stats for {season}: {e}")
            return 0

        if df is None or df.empty:
            logger.info(f"No pitching stats for {season}.")
            return 0

        logger.info(f"  Fetched {len(df)} qualified pitchers for {season}.")

        count = 0
        unmatched = []

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                name = str(row.get("Name", ""))
                fg_id = row.get("IDfg")
                player_id = self._resolve_player_id(name, fg_id)

                if player_id is None:
                    unmatched.append(name)
                    continue

                self._ensure_player(conn, player_id, name)
                params = {
                    "player_id": player_id,
                    "season": season,
                    "as_of_date": as_of_date,
                    "war": _safe_float(row.get("WAR")),
                    "babip": _safe_float(row.get("BABIP")),
                    "fip": _safe_float(row.get("FIP")),
                    "xfip": _safe_float(row.get("xFIP")),
                    "xera": _safe_float(row.get("xERA")),
                    "siera": _safe_float(row.get("SIERA")),
                    "era": _safe_float(row.get("ERA")),
                    "lob_pct": _pct_to_float(row.get("LOB%")),
                    "gb_pct": _pct_to_float(row.get("GB%")),
                    "k_per_9": _safe_float(row.get("K/9")),
                    "bb_per_9": _safe_float(row.get("BB/9")),
                    "hr_per_9": _safe_float(row.get("HR/9")),
                    "ip": _safe_float(row.get("IP")),
                    "fangraphs_id": _safe_int(fg_id),
                    "k_pct": _pct_to_float(row.get("K%")),
                    "bb_pct": _pct_to_float(row.get("BB%")),
                }
                conn.execute(
                    text("""
                        INSERT INTO mlb_player_season_advanced
                            (player_id, season, player_type,
                             war, babip, fip, xfip, xera, siera, era,
                             lob_pct, gb_pct, k_per_9, bb_per_9, hr_per_9,
                             ip, fangraphs_id)
                        VALUES
                            (:player_id, :season, 'pitcher',
                             :war, :babip, :fip, :xfip, :xera, :siera, :era,
                             :lob_pct, :gb_pct, :k_per_9, :bb_per_9, :hr_per_9,
                             :ip, :fangraphs_id)
                        ON CONFLICT (player_id, season, player_type) DO UPDATE SET
                             war = EXCLUDED.war,
                             babip = EXCLUDED.babip,
                             fip = EXCLUDED.fip,
                             xfip = EXCLUDED.xfip,
                             xera = EXCLUDED.xera,
                             siera = EXCLUDED.siera,
                             era = EXCLUDED.era,
                             lob_pct = EXCLUDED.lob_pct,
                             gb_pct = EXCLUDED.gb_pct,
                             k_per_9 = EXCLUDED.k_per_9,
                             bb_per_9 = EXCLUDED.bb_per_9,
                             hr_per_9 = EXCLUDED.hr_per_9,
                             ip = EXCLUDED.ip,
                             fangraphs_id = EXCLUDED.fangraphs_id
                    """),
                    params,
                )
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
                count += 1

        if unmatched:
            logger.warning(f"  Could not match {len(unmatched)} pitchers: {unmatched[:10]}{'...' if len(unmatched) > 10 else ''}")

        logger.info(f"  Upserted {count} pitching rows for {season}.")
        return count

    # ------------------------------------------------------------------
    # Player ID resolution
    # ------------------------------------------------------------------

    def _resolve_player_id(self, name: str, fg_id) -> int | None:
        """Map a FanGraphs player to our mlb_players.player_id.

        Strategy:
        1. Direct name match against our players table
        2. FanGraphs ID -> MLBAM ID crosswalk via pybaseball
        """
        # Try direct name match
        name_lower = name.strip().lower()
        if name_lower in self._player_cache:
            return self._player_cache[name_lower]

        # Try with common name variations (Jr., Sr., etc.)
        for suffix in [" jr.", " sr.", " ii", " iii", " iv"]:
            without = name_lower.replace(suffix, "").strip()
            if without in self._player_cache:
                self._player_cache[name_lower] = self._player_cache[without]
                return self._player_cache[without]
            with_suffix = name_lower + suffix if suffix not in name_lower else name_lower.replace(suffix, "")
            if with_suffix in self._player_cache:
                self._player_cache[name_lower] = self._player_cache[with_suffix]
                return self._player_cache[with_suffix]

        # Try FanGraphs ID -> MLBAM crosswalk
        fg_id_int = _safe_int(fg_id)
        if fg_id_int is not None and fg_id_int not in self._fg_to_mlbam_cache:
            try:
                lookup = playerid_reverse_lookup([fg_id_int], key_type="fangraphs")
                if not lookup.empty and "key_mlbam" in lookup.columns:
                    mlbam_id = int(lookup.iloc[0]["key_mlbam"])
                    if mlbam_id > 0:
                        self._fg_to_mlbam_cache[fg_id_int] = mlbam_id
                        self._player_cache[name_lower] = mlbam_id
                        return mlbam_id
            except Exception:
                pass  # Crosswalk may not have every player

        if fg_id_int in self._fg_to_mlbam_cache:
            mlbam_id = self._fg_to_mlbam_cache[fg_id_int]
            self._player_cache[name_lower] = mlbam_id
            return mlbam_id

        return None

    def _ensure_player(self, conn, player_id: int, player_name: str):
        """Insert player stub if not in mlb_players."""
        conn.execute(
            text("""
                INSERT INTO mlb_players (player_id, player_name)
                VALUES (:pid, :name)
                ON CONFLICT (player_id) DO NOTHING
            """),
            {"pid": player_id, "name": player_name},
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


def _pct_to_float(val) -> float | None:
    """Convert percentage value to decimal float.

    Handles both '23.5%' string format and 0.235 float format.
    """
    if val is None:
        return None
    try:
        if isinstance(val, str):
            val = val.replace("%", "").replace(" ", "")
            if not val:
                return None
            return round(float(val) / 100, 4)
        f = float(val)
        if np.isnan(f):
            return None
        # If value > 1, assume it's a percentage (e.g. 23.5 -> 0.235)
        if f > 1:
            return round(f / 100, 4)
        return round(f, 4)
    except (ValueError, TypeError):
        return None


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="MLB FanGraphs Advanced Stats Scraper")
    parser.add_argument("--season", type=int, help="Season year (e.g. 2025)")
    parser.add_argument("--all-seasons", action="store_true",
                        help=f"Scrape all seasons ({DEFAULT_SEASONS})")
    parser.add_argument("--batting-only", action="store_true", help="Only scrape batting stats")
    parser.add_argument("--pitching-only", action="store_true", help="Only scrape pitching stats")
    parser.add_argument(
        "--as-of-date",
        type=str,
        default=None,
        help="YYYY-MM-DD snapshot date for the history table row. Defaults to yesterday.",
    )

    args = parser.parse_args()

    as_of_date = None
    if args.as_of_date:
        as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()

    if not args.season and not args.all_seasons:
        print("ERROR: Specify --season YYYY or --all-seasons")
        sys.exit(1)

    engine = get_engine()
    scraper = MLBFanGraphsScraper(engine)

    seasons = DEFAULT_SEASONS if args.all_seasons else [args.season]

    print("=" * 60)
    print("MLB FANGRAPHS ADVANCED STATS SCRAPER")
    print("=" * 60)
    print(f"Seasons: {seasons}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    total_batting = 0
    total_pitching = 0

    for season in seasons:
        print(f"\n--- Season {season} ---")

        if not args.pitching_only:
            batting = scraper.scrape_season_batting(season, as_of_date=as_of_date)
            total_batting += batting
            print(f"  Batting: {batting} rows")

        if not args.batting_only:
            pitching = scraper.scrape_season_pitching(season, as_of_date=as_of_date)
            total_pitching += pitching
            print(f"  Pitching: {pitching} rows")

    print(f"\nTotals: {total_batting} batting, {total_pitching} pitching rows")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
